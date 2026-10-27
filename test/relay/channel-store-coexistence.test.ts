import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { runtimeContractVersion } from "@axisosme/axis-contracts/runtime";
import type { Task } from "@axisosme/axis-contracts/runtime";

import { SqliteChannelConversationStore } from "../../src/router/channel-conversation.js";
import { channelInstructionDigest, linearChannelConversationScope } from "../../src/router/channel-conversation-types.js";
import { linearTenantRef } from "../../src/router/channel-identity.js";
import { SqliteChannelRunChainStore } from "../../src/router/channel-run-chain.js";
import { normalizedSchemaSql } from "../../src/shared/schema-ledger.js";
import { GOLDEN_STORE_SCHEMAS } from "../fixtures/schema-ledger-goldens.js";
import { temporaryDatabase } from "../helpers.js";

const scope = linearChannelConversationScope({ organizationId: "organization_134", agentSessionId: "session_165" });

function stores(filename: string) {
  let clock = 0;
  const now = () => new Date(Date.UTC(2026, 8, 6, 0, 0, clock++));
  let decisions = 0;
  const conversations = new SqliteChannelConversationStore({ filename, now });
  const chain = new SqliteChannelRunChainStore({ conversations, filename, now, id: () => `decision_${++decisions}` });
  return { conversations, chain, close() { chain.close(); conversations.close(); } };
}

function seed(store: ReturnType<typeof stores>): void {
  const conversationId = store.conversations.openOrResume({ scope }).conversation.conversationId;
  const tenant = linearTenantRef(scope.organizationId);
  const task: Task = {
    contractVersion: runtimeContractVersion,
    taskId: "task_1",
    correlationId: "correlation_1",
    sourceRef: { system: "linear", organizationId: scope.organizationId, issueId: "issue_165", agentSessionId: conversationId },
  };
  store.chain.reserve({ scope, conversationId, tenant, commandId: "cmd_start", kind: "start", instructionDigest: channelInstructionDigest("@axis do it") });
  store.chain.settle({ commandId: "cmd_start", outcome: { commandId: "cmd_start", taskId: "task_1", runId: "run_1", status: "accepted" } });
  store.chain.recordRunState({ conversationId, task, runId: "run_1", state: "running" });
  store.chain.reserve({ scope, conversationId, tenant, commandId: "cmd_continue", kind: "continue", instructionDigest: channelInstructionDigest("@axis again") });
  store.chain.settle({ commandId: "cmd_continue", outcome: { commandId: "cmd_continue", taskId: "task_1", runId: "run_1", status: "interrupt_requested" } });
  store.chain.recordRunState({ conversationId, task, runId: "run_1", state: "waiting_for_input" });
  store.chain.raiseWaitingDecision({ runId: "run_1", elicitationDigest: channelInstructionDigest("approach A or B?") });
}

test("channel-conversation and channel-run-chain share one database file, and each reopens it alongside the other's objects", (context) => {
  const shared = temporaryDatabase(); context.after(shared.cleanup);

  const built = stores(shared.filename);
  seed(built);
  assert.equal(built.conversations.ready(), true);
  assert.equal(built.chain.ready(), true);
  built.close();

  const database = new DatabaseSync(shared.filename);
  const names = (database.prepare("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").all() as { name: string }[]).map((row) => row.name);
  database.close();
  const registry = names.filter((name) => name.startsWith("channel_conversation"));
  const runChain = names.filter((name) => name.startsWith("channel_run_chain"));
  assert.ok(registry.length > 1 && runChain.length > 1);
  assert.deepEqual(names.filter((name) => !registry.includes(name) && !runChain.includes(name)), []);

  const reopened = stores(shared.filename); context.after(() => reopened.close());
  assert.equal(reopened.conversations.ready(), true);
  assert.equal(reopened.chain.ready(), true);
});

test("channel-conversation and channel-run-chain refuse a database whose ledger is not at the current version", (context) => {
  const file = temporaryDatabase(); context.after(file.cleanup);
  stores(file.filename).close();
  const database = new DatabaseSync(file.filename);
  database.exec("DELETE FROM channel_conversation_schema; INSERT INTO channel_conversation_schema(version, applied_at) VALUES (987654321, '2026-01-01T00:00:00.000Z');");
  database.exec("DELETE FROM channel_run_chain_schema; INSERT INTO channel_run_chain_schema(version, applied_at) VALUES (987654321, '2026-01-01T00:00:00.000Z');");
  database.close();
  assert.throws(() => new SqliteChannelConversationStore({ filename: file.filename }), { code: "schema_version_unsupported" });
  const registry = temporaryDatabase(); context.after(registry.cleanup);
  const conversations = new SqliteChannelConversationStore({ filename: registry.filename });
  context.after(() => conversations.close());
  assert.throws(() => new SqliteChannelRunChainStore({ conversations, filename: file.filename }), { code: "schema_version_unsupported" });
});

for (const label of ["channel-conversation", "channel-run-chain"] as const) {
  test(`${label}: a fresh database matches the current golden DDL`, (context) => {
    const file = temporaryDatabase(); context.after(file.cleanup);
    stores(file.filename).close();
    const golden = GOLDEN_STORE_SCHEMAS[label]!;
    const prefix = label === "channel-conversation" ? "channel_conversation" : "channel_run_chain";
    const database = new DatabaseSync(file.filename);
    const actual = (database.prepare("SELECT type, name, sql FROM sqlite_master WHERE name LIKE ? AND name NOT LIKE 'sqlite_%'").all(`${prefix}%`) as { type: string; name: string; sql: string }[])
      .map((row) => ({ key: `${row.type}:${row.name}`, sql: row.sql, fingerprint: normalizedSchemaSql(row.sql) }))
      .sort((a, b) => a.key.localeCompare(b.key));
    const versions = (database.prepare(`SELECT version FROM ${golden.ledgerTable}`).all() as { version: number }[]).map((row) => row.version);
    database.close();
    const recorded = golden.swept.map((object) => ({ key: object.key, sql: object.ddl, fingerprint: object.fingerprint })).sort((a, b) => a.key.localeCompare(b.key));
    assert.deepEqual(actual, recorded);
    assert.deepEqual(versions, [golden.version]);
  });
}

test("run chains reject unsupported Task versions and persist only the current Runtime version", (context) => {
  const file = temporaryDatabase(); context.after(file.cleanup);
  const store = stores(file.filename); context.after(() => store.close());
  seed(store);
  const database = new DatabaseSync(file.filename); context.after(() => database.close());
  assert.deepEqual(database.prepare("SELECT runtime_contract_version FROM channel_run_chain_runtime_versions").all().map((row) => row.runtime_contract_version), [runtimeContractVersion]);
  const task = {
    contractVersion: "fictional-contract", taskId: "task_1", correlationId: "correlation_1",
    sourceRef: { system: "linear", organizationId: scope.organizationId, issueId: "issue_165", agentSessionId: scope.agentSessionId },
  };
  assert.throws(() => store.chain.recordRunState({ conversationId: scope.agentSessionId, task: task as Task, runId: "run_1", state: "waiting_for_input" }), TypeError);
  assert.throws(() => database.prepare("INSERT INTO channel_run_chain_runtime_versions(conversation_id, runtime_contract_version) VALUES ('unsupported-conversation', ?)").run(task.contractVersion), /CHECK constraint failed/u);
});
