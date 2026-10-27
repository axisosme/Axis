import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { assertLifecycleDatabaseDrained } from "../../src/lifecycle/macos-adapter.js";
import { HostStateError } from "../../src/lifecycle/host-state.js";
import { SqliteWorkerCommandJournal } from "../../src/worker/command.js";
import { temporaryDatabase } from "../helpers.js";

const occurredAt = "2026-01-01T00:00:00.000Z";

function withDatabase<T>(filename: string, read: (database: DatabaseSync) => T): T {
  const database = new DatabaseSync(filename);
  try { return read(database); } finally { database.close(); }
}

function snapshot(filename: string): unknown {
  return withDatabase(filename, database => {
    const objects = database.prepare("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name").all();
    const tables = database.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all();
    return { objects, rows: tables.map(({ name }) => [name, database.prepare(`SELECT * FROM "${String(name).replaceAll('"', '""')}"`).all()]) };
  });
}

test("Worker creates one current schema record and reopens command and observation evidence unchanged", context => {
  const file = temporaryDatabase(); context.after(file.cleanup);
  let journal = new SqliteWorkerCommandJournal(file.filename);
  assert.deepEqual(withDatabase(file.filename, database => database.prepare("SELECT version FROM worker_schema").all()).map(row => row.version), [1]);
  const command = { commandId: "command-one", intentJson: "{}", intentDigest: "intent-one", occurredAt };
  assert.equal(journal.beginCommand(command).kind, "claimed");
  journal.reserveRun({ runId: "run-one", intentJson: "{}", intentDigest: "intent-one", occurredAt });
  journal.recordObservational({ runId: "run-one", cause: "worker.recovery_unprovable", occurredAt });
  journal.close();
  const before = snapshot(file.filename);
  journal = new SqliteWorkerCommandJournal(file.filename);
  assert.equal(journal.beginCommand(command).kind, "existing");
  assert.equal(journal.readRun("run-one")?.state, "observational");
  assert.equal(journal.readObservationalCause("run-one"), "worker.recovery_unprovable");
  journal.close();
  journal = new SqliteWorkerCommandJournal(file.filename, { readOnly: true });
  assert.equal(journal.readCommand("command-one")?.intentDigest, "intent-one");
  assert.equal(journal.readObservationalCause("run-one"), "worker.recovery_unprovable");
  journal.close();
  assert.deepEqual(snapshot(file.filename), before);
});

test("Worker preserves unrelated objects and lifecycle inspection accepts its drained current schema", context => {
  const file = temporaryDatabase(); context.after(file.cleanup);
  withDatabase(file.filename, database => database.exec("CREATE TABLE unrelated_rows(id TEXT); CREATE INDEX unrelated_index ON unrelated_rows(id)"));
  new SqliteWorkerCommandJournal(file.filename).close();
  const before = snapshot(file.filename);
  new SqliteWorkerCommandJournal(file.filename).close();
  assertLifecycleDatabaseDrained(file.filename);
  assert.deepEqual(snapshot(file.filename), before);
});

const faults = {
  unsupported: "UPDATE worker_schema SET version=987654321",
  extra: "INSERT INTO worker_schema VALUES (987654321, '2026-01-01T00:00:00.000Z')",
  empty: "DELETE FROM worker_schema",
  timestamp: "UPDATE worker_schema SET applied_at='fictional-instant'",
  "missing-record-table": "DROP TABLE worker_schema",
  partial: "DROP TABLE worker_waiting_runs",
  altered: "ALTER TABLE worker_runs ADD COLUMN fictional_column TEXT",
  "foreign-trigger": "CREATE TRIGGER sqliteXguard BEFORE INSERT ON worker_runs BEGIN SELECT RAISE(ABORT, 'fictional guard'); END",
  "foreign-index": "CREATE INDEX sqliteXguard ON worker_runs(run_id)",
} as const;

for (const [fault, sql] of Object.entries(faults)) {
  test(`Worker refuses ${fault} storage on writable, read-only and lifecycle paths without changing it`, context => {
    const file = temporaryDatabase(); context.after(file.cleanup);
    new SqliteWorkerCommandJournal(file.filename).close();
    withDatabase(file.filename, database => database.exec(sql));
    const before = snapshot(file.filename);
    for (const readOnly of [false, true]) {
      assert.throws(() => new SqliteWorkerCommandJournal(file.filename, { readOnly }), { code: "axis.worker.recovery_required" });
      assert.deepEqual(snapshot(file.filename), before);
    }
    assert.throws(() => assertLifecycleDatabaseDrained(file.filename), HostStateError);
    assert.deepEqual(snapshot(file.filename), before);
  });
}

test("a read-only Worker open refuses an empty database without creating schema", context => {
  const file = temporaryDatabase(); context.after(file.cleanup);
  withDatabase(file.filename, () => {});
  assert.throws(() => new SqliteWorkerCommandJournal(file.filename, { readOnly: true }), { code: "axis.worker.recovery_required" });
  assert.deepEqual(withDatabase(file.filename, database => database.prepare("SELECT name FROM sqlite_master").all()), []);
});
