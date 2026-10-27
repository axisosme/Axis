import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { HostStateError } from "../../../src/lifecycle/host-state.js";
import { assertLifecycleDatabaseDrained } from "../../../src/lifecycle/macos-adapter.js";
import { Router } from "../../../src/router/router.js";

test("lifecycle drain accepts idle Router storage and blocks running work", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-router-drain-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const filename = join(directory, "router.sqlite");
  new Router({ filename }).close();
  assert.doesNotThrow(() => assertLifecycleDatabaseDrained(filename));
  const database = new DatabaseSync(filename);
  database.prepare("INSERT INTO router_runs(run_id,task_id,attempt_json,run_json,binding_json,start_json,state) VALUES (?,?,?,?,?,?,'running')")
    .run("run-1", "task-1", "{}", "{}", "{}", "{}");
  database.close();
  const before = readFileSync(filename);
  assert.throws(() => assertLifecycleDatabaseDrained(filename), HostStateError);
  assert.deepEqual(readFileSync(filename), before);
});

test("lifecycle drain refuses unrecognized Router schemas without changing storage", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-router-drain-schema-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  for (const variant of ["unknown", "partial", "version", "extra"]) {
    const filename = join(directory, `${variant}.sqlite`);
    if (variant === "version" || variant === "extra") new Router({ filename }).close();
    const database = new DatabaseSync(filename);
    if (variant === "partial") {
      database.exec("CREATE TABLE router_node_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO router_node_schema VALUES (1,'2026-10-27T00:00:00.000Z')");
    } else if (variant === "version") {
      database.exec("UPDATE router_node_schema SET version=987654321");
    } else {
      database.exec("CREATE TABLE router_unrecognized_runs(state TEXT); INSERT INTO router_unrecognized_runs VALUES ('running')");
    }
    database.close();
    const before = readFileSync(filename);
    assert.throws(() => assertLifecycleDatabaseDrained(filename), HostStateError);
    assert.deepEqual(readFileSync(filename), before);
  }
});

test("lifecycle drain leaves stores outside the Router namespace to their own checks", (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-other-store-drain-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const filename = join(directory, "store.sqlite");
  const database = new DatabaseSync(filename);
  database.exec("CREATE TABLE unrelated_store(id TEXT PRIMARY KEY)");
  database.close();
  assert.doesNotThrow(() => assertLifecycleDatabaseDrained(filename));
});
