import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { Router, RouterWorkerDispatchError } from "../../src/router/router.js";
import { linearTaskIdentity } from "../../src/router/task-identity.js";
import { nodeRoutingSnapshotFixture, workerSelectionFixture } from "../support/node-routing-fixtures.js";

function schema(filename: string): readonly unknown[] {
  const database = new DatabaseSync(filename, { readOnly: true });
  try { return database.prepare("SELECT type, name, sql FROM sqlite_master ORDER BY type, name").all(); }
  finally { database.close(); }
}

test("Router creates the current schema and reopens it alongside another store", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-router-schema-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const filename = join(directory, "router.sqlite");
  const database = new DatabaseSync(filename);
  database.exec("CREATE TABLE unrelated_store (id TEXT PRIMARY KEY)");
  database.close();
  new Router({ filename }).close();
  const before = schema(filename);
  new Router({ filename }).close();
  assert.deepEqual(schema(filename), before);
  const read = new DatabaseSync(filename, { readOnly: true });
  try {
    assert.equal(read.prepare("SELECT version FROM router_node_schema").get()?.version, 1);
    assert.deepEqual(read.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'router_*' ORDER BY name").all().map((row) => row.name), [
      "router_node_command_state", "router_node_conflicts", "router_node_events", "router_node_run_execution",
      "router_node_runs", "router_node_schema", "router_receipts", "router_runs", "router_tasks",
    ]);
  } finally { read.close(); }
});

test("Router refuses a database with Router objects but no schema ledger without changing it", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-router-ledger-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const filename = join(directory, "router.sqlite");
  const database = new DatabaseSync(filename);
  database.exec("CREATE TABLE router_unrecognized_tasks (task_id TEXT PRIMARY KEY)");
  database.close();
  const before = schema(filename);
  assert.throws(() => new Router({ filename }), { code: "axis.router.recovery_required" });
  assert.deepEqual(schema(filename), before);
});

test("Router refuses every unrecognized schema version without creating or altering objects", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-router-version-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  for (const version of [987654321, -987654321]) {
    const filename = join(directory, `router-${version}.sqlite`);
    new Router({ filename }).close();
    const database = new DatabaseSync(filename);
    database.prepare("UPDATE router_node_schema SET version=?").run(version);
    database.close();
    const before = schema(filename);
    assert.throws(() => new Router({ filename }), { code: "axis.router.recovery_required" });
    assert.deepEqual(schema(filename), before);
    const read = new DatabaseSync(filename, { readOnly: true });
    try { assert.equal(read.prepare("SELECT version FROM router_node_schema").get()?.version, version); }
    finally { read.close(); }
  }
});

test("Router refuses missing core tables without recreating them", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-router-core-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  for (const table of ["router_tasks", "router_runs", "router_receipts"]) {
    const filename = join(directory, `${table}.sqlite`);
    new Router({ filename }).close();
    const database = new DatabaseSync(filename);
    database.exec(`DROP TABLE ${table}`);
    database.close();
    const before = schema(filename);
    assert.throws(() => new Router({ filename }), { code: "axis.router.recovery_required" });
    assert.deepEqual(schema(filename), before);
  }
});

test("a current ledger cannot authorize partial or unknown Router objects", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-router-objects-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  for (const variant of ["ledger", "partial", "extra"]) {
    const filename = join(directory, `${variant}.sqlite`);
    if (variant === "extra") new Router({ filename }).close();
    const database = new DatabaseSync(filename);
    if (variant !== "extra") {
      database.exec("CREATE TABLE router_node_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO router_node_schema VALUES (1,'2026-10-27T00:00:00.000Z')");
    }
    if (variant !== "ledger") database.exec("CREATE TABLE router_unrecognized_tasks (task_id TEXT PRIMARY KEY)");
    database.close();
    const before = schema(filename);
    assert.throws(() => new Router({ filename }), { code: "axis.router.recovery_required" });
    assert.deepEqual(schema(filename), before);
  }
});

test("Run target evidence retains the frozen Node and explicit pending execution after a lost response", async (context) => {
  let runId: string | undefined;
  const target = nodeRoutingSnapshotFixture();
  const router = new Router({ nodeRouting: {
    bindingResolver: {
      resolveExecutionTarget() { throw new Error("Only Node targets are resolved."); },
      resolveNodeTarget() { return target; },
    },
    port: {
      async dispatch(request) {
        if (request.command.command !== "start") throw new Error("A start command is required.");
        runId = request.command.start.identity.run.runId;
        throw new RouterWorkerDispatchError("axis.worker.transport_unknown", "ambiguous");
      },
      async readReceipt() { throw new Error("No receipt is available."); },
    },
  } });
  context.after(() => router.close());
  const task = { organizationId: "tenant-1", teamId: "team-1", issueId: "issue-1", agentSessionId: "session-1" };
  await assert.rejects(router.startNode({ ...task, commandId: "command-1", instruction: "Do the work.", workerSelection: workerSelectionFixture() }), { code: "axis.router.recovery_required" });
  assert.ok(runId);
  const retainedRunId = runId;
  assert.deepEqual(router.readRunTarget({ ...task, runId }), {
    contractVersion: "1.0.0", runId, target,
    execution: { status: "pending", workerId: null, executorId: null },
  });
  assert.throws(() => router.readRunTarget({ ...task, runId: "missing-run" }), { code: "axis.router.not_found" });
  assert.throws(() => router.readRunTarget({ ...task, teamId: "other-team", runId: retainedRunId }), { code: "axis.router.authority_mismatch" });
});

test("Router task identity diagnostics expose Linear columns and refuse persisted identity drift", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-router-identity-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const filename = join(directory, "router.sqlite");
  new Router({ filename }).close();
  const database = new DatabaseSync(filename);
  context.after(() => database.close());
  const sourceRefJson = '{"agentSessionId":"session-1","issueId":"issue-1","system":"linear"}';
  database.prepare(`INSERT INTO router_tasks(task_id, organization_id, issue_id, agent_session_id, linear_team_id,
    attempt_id, correlation_id, operation_id, task_json, source_system, tenant_id, source_ref_json, routing_scope_id)
    VALUES ('task-1', 'tenant-1', 'issue-1', 'session-1', 'team-1', 'attempt-1', 'correlation-1', 'operation-1', '{}',
    'linear', 'tenant-1', ?, 'team-1')`).run(sourceRefJson);
  const router = new Router({ filename });
  context.after(() => router.close());
  const identity = linearTaskIdentity({ organizationId: "tenant-1", issueId: "issue-1", agentSessionId: "session-1", teamId: "team-1" });
  const persisted = {
    contractVersion: "1.0.0", taskId: "task-1", identity,
    linearColumns: { organizationId: "tenant-1", issueId: "issue-1", agentSessionId: "session-1", linearTeamId: "team-1" },
  };
  assert.deepEqual(router.readTaskIdentity("task-1"), persisted);
  assert.deepEqual(router.resolveTaskIdentity(identity), persisted);
  const verified = { contractVersion: "1.0.0", rows: 1, linearRows: 1, linearRoundTrips: 1 };
  assert.deepEqual(router.verifyTaskIdentity(), verified);
  for (const [column, value] of [["organization_id", "tenant-1"], ["issue_id", "issue-1"], ["agent_session_id", "session-1"], ["linear_team_id", "team-1"]] as const) {
    database.prepare(`UPDATE router_tasks SET ${column} = ?`).run("different-identity");
    assert.throws(() => router.verifyTaskIdentity(), { code: "axis.router.recovery_required" });
    assert.throws(() => new Router({ filename }), { code: "axis.router.recovery_required" });
    assert.equal(database.prepare(`SELECT ${column} AS value FROM router_tasks`).get()?.value, "different-identity");
    database.prepare(`UPDATE router_tasks SET ${column} = ?`).run(value);
    assert.deepEqual(router.verifyTaskIdentity(), verified);
  }
});
