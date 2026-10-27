import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { SqliteNodeJournal } from "../../src/node/journal.js";

const storageUnavailable = (error: unknown): boolean => (error as { code?: string }).code === "axis.node.storage_unavailable";

function schemaObjects(filename: string): readonly string[] {
  const database = new DatabaseSync(filename, { readOnly: true });
  try { return (database.prepare("SELECT type || ':' || name AS object FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name").all() as { object: string }[]).map((row) => row.object); }
  finally { database.close(); }
}

test("an empty file becomes the current journal, which reopens and inspects as idle", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "axis-node-journal-schema-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const filename = join(directory, "node.sqlite");
  (await SqliteNodeJournal.open(filename)).close();
  (await SqliteNodeJournal.recover(filename)).close();
  assert.deepEqual(schemaObjects(filename), ["index:routes_first_source_session", "table:commands", "table:event_outbox", "table:metadata", "table:routes"]);
  assert.deepEqual(await SqliteNodeJournal.inspectMaintenance(filename), {
    mode: "resume",
    blockerCounts: { reservedCommands: 0, dispatchingCommands: 0, ambiguousCommands: 0, pendingNodeEvents: 0, quarantinedNodeEvents: 0 },
  });
  assert.deepEqual(await SqliteNodeJournal.inspectAmbiguousCommands(filename), { ambiguous: [], resolved: [] });
  const database = new DatabaseSync(filename, { readOnly: true });
  try { assert.equal(database.prepare("SELECT value FROM metadata WHERE key='schema_version'").get()?.value, "1"); }
  finally { database.close(); }
});

test("a journal under any other schema is refused by every opener and left unchanged", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "axis-node-journal-schema-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const filename = join(directory, "node.sqlite");
  (await SqliteNodeJournal.open(filename)).close();
  const database = new DatabaseSync(filename);
  database.exec("UPDATE metadata SET value='fictional-schema' WHERE key='schema_version'");
  database.close();
  const before = schemaObjects(filename);
  await assert.rejects(SqliteNodeJournal.open(filename), storageUnavailable);
  await assert.rejects(SqliteNodeJournal.recover(filename), storageUnavailable);
  await assert.rejects(SqliteNodeJournal.inspectMaintenance(filename), storageUnavailable);
  await assert.rejects(SqliteNodeJournal.inspectAmbiguousCommands(filename), storageUnavailable);
  assert.deepEqual(schemaObjects(filename), before);
  const read = new DatabaseSync(filename, { readOnly: true });
  try { assert.equal(read.prepare("SELECT value FROM metadata WHERE key='schema_version'").get()?.value, "fictional-schema"); }
  finally { read.close(); }
});


for (const kind of ["trigger", "index", "table"] as const) {
  test(`a sqliteXguard ${kind} cannot hide from journal initialization or validation`, async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "axis-node-journal-schema-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const filename = join(directory, "node.sqlite");
    if (kind !== "table") (await SqliteNodeJournal.open(filename)).close();
    const database = new DatabaseSync(filename);
    database.exec(kind === "trigger"
      ? "CREATE TRIGGER sqliteXguard BEFORE UPDATE ON metadata BEGIN SELECT RAISE(ABORT, 'unexpected guard'); END"
      : kind === "index"
        ? "CREATE INDEX sqliteXguard ON commands(command_id)"
        : "CREATE TABLE sqliteXguard(id TEXT)");
    database.close();
    const before = schemaObjects(filename);
    assert.ok(before.includes(`${kind}:sqliteXguard`));
    await assert.rejects(SqliteNodeJournal.open(filename), storageUnavailable);
    await assert.rejects(SqliteNodeJournal.recover(filename), storageUnavailable);
    await assert.rejects(SqliteNodeJournal.inspectMaintenance(filename), storageUnavailable);
    await assert.rejects(SqliteNodeJournal.inspectAmbiguousCommands(filename), storageUnavailable);
    assert.deepEqual(schemaObjects(filename), before);
  });
}
