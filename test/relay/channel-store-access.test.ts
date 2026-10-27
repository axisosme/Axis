import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { SqliteChannelConversationStore } from "../../src/router/channel-conversation.js";
import { SqliteChannelRunChainStore } from "../../src/router/channel-run-chain.js";
import { digestFixture, linearScopeFixture, linearTenantFixture } from "../support/channel-conversation-fixtures.js";
import { SCAN_PRAGMA, recordStatements, schemaBounded } from "../support/statement-recorder.js";
import { schemaObjects, tamper } from "../support/sqlite-tamper.js";

function fullCheckOnlyStatements(store: { verifyIntegrity(): void; readyForTraffic(): boolean }): readonly string[] {
  const full = recordStatements(() => store.verifyIntegrity()).statements;
  const cheap = recordStatements(() => store.readyForTraffic());
  assert.equal(cheap.value, true);
  assert.ok(cheap.statements.length > 0, "readiness must actually check the store");
  assert.ok(cheap.statements.every(schemaBounded), "readiness must be bounded by schema size");
  const expensive = full.filter(sql => !cheap.statements.includes(sql));
  assert.ok(expensive.some(sql => SCAN_PRAGMA.test(sql)), "the full check must include whole-database scans");
  assert.ok(expensive.some(sql => /\bJOIN\b|\bGROUP BY\b/iu.test(sql)), "the full check must include data invariant sweeps");
  return expensive;
}

test("ingress store lookups avoid full checks and refuse after integrity failure", context => {
  const directory = mkdtempSync(join(tmpdir(), "axis-channel-store-access-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const conversations = new SqliteChannelConversationStore({ filename: join(directory, "conversations.sqlite") });
  context.after(() => conversations.close());
  const runChainFile = join(directory, "run-chain.sqlite");
  const runChain = new SqliteChannelRunChainStore({ conversations, filename: runChainFile });
  context.after(() => runChain.close());

  const conversationId = conversations.openOrResume({ scope: linearScopeFixture }).conversation.conversationId;
  assert.equal(conversationId, linearScopeFixture.agentSessionId);
  conversations.recordDelivery({ deliveryId: "delivery-1" });
  runChain.reserve({
    scope: linearScopeFixture, tenant: linearTenantFixture, conversationId,
    commandId: "command-start-1", kind: "start", instructionDigest: digestFixture("start instruction"),
  });

  const conversationExpensive = fullCheckOnlyStatements(conversations);
  const runChainExpensive = fullCheckOnlyStatements(runChain);
  const delivery = recordStatements(() => {
    assert.equal(conversations.hasDelivery({ deliveryId: "delivery-1" }), true);
    assert.equal(conversations.hasDelivery({ deliveryId: "unknown-delivery" }), false);
  });
  assert.ok(delivery.statements.length > 0, "delivery probes must issue statements");
  assert.deepEqual(delivery.statements.filter(sql => SCAN_PRAGMA.test(sql)), [], "hasDelivery must issue no whole-database scans");
  assert.deepEqual(delivery.statements.filter(sql => conversationExpensive.includes(sql)), [], "hasDelivery must issue no full-check-only statements");

  const initialStart = recordStatements(() => {
    assert.equal(runChain.initialStartReservation({ conversationId })?.commandId, "command-start-1");
    assert.equal(runChain.initialStartReservation({ conversationId: "unknown-session" }), null);
  });
  assert.ok(initialStart.statements.length > 0, "reservation lookups must issue statements");
  assert.deepEqual(initialStart.statements.filter(sql => SCAN_PRAGMA.test(sql)), [], "initialStartReservation must issue no whole-database scans");
  assert.deepEqual(initialStart.statements.filter(sql => runChainExpensive.includes(sql)), [], "initialStartReservation must issue no full-check-only statements");

  const schemaBefore = schemaObjects(runChainFile);
  tamper(runChainFile, "INSERT INTO channel_run_chain_runtime_versions(conversation_id, runtime_contract_version) VALUES ('orphan-session', '1.0.0')");
  assert.deepEqual(schemaObjects(runChainFile), schemaBefore, "the data damage must be invisible to a schema sweep");
  assert.equal(runChain.readyForTraffic(), true, "schema readiness does not detect data damage before a full check");
  assert.equal(runChain.initialStartReservation({ conversationId })?.commandId, "command-start-1");

  assert.throws(() => runChain.verifyIntegrity(), { code: "persisted_state_conflict" });
  assert.equal(runChain.readyForTraffic(), false, "the full check must latch the integrity failure");
  assert.throws(() => runChain.initialStartReservation({ conversationId }), { code: "persisted_state_conflict" });
  assert.throws(() => runChain.initialStartReservation({ conversationId: "unknown-session" }), { code: "persisted_state_conflict" });
});
