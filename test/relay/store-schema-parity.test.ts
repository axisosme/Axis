import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { join, relative } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { SqliteChannelOutboxStore, CHANNEL_OUTBOX_STATES, type PersistedChannelOutboxState } from "../../src/relay/channel-outbox.js";
import { LinearInstallationStore } from "../../src/relay/linear/installation.js";
import { SqliteLinearInbox } from "../../src/relay/linear/inbox.js";
import { SqliteLinearLedgers } from "../../src/relay/linear/ledgers.js";
import { SqliteChannelConversationStore } from "../../src/router/channel-conversation.js";
import { SqliteChannelRunChainStore } from "../../src/router/channel-run-chain.js";
import { Router } from "../../src/router/router.js";
import { SCHEMA_IF_NOT_EXISTS_ELIDED_AFTER, normalizedSchemaSql } from "../../src/shared/schema-ledger.js";
import { type ChannelOutboxState } from "../../src/router/channel-conversation-types.js";
import { GOLDEN_STORE_SCHEMAS } from "../fixtures/schema-ledger-goldens.js";
import { temporaryDatabase } from "../helpers.js";

/**
 * Check store schema versions, fresh-create/reopen parity, independent Router golden
 * DDL and the shared schema normalizer.
 */

type SqlRow = Record<string, unknown>;

function withDatabase<T>(filename: string, read: (database: DatabaseSync) => T): T {
  const database = new DatabaseSync(filename);
  try { return read(database); } finally { database.close(); }
}

interface PersistedObject { readonly key: string; readonly sql: string }

/** Every object the file holds, verbatim, except SQLite's own internal objects. */
const persistedObjects = (filename: string): PersistedObject[] =>
  withDatabase(filename, (database) => (database.prepare(
    "SELECT type, name, sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' AND sql IS NOT NULL",
  ).all() as SqlRow[])
    .map((row) => ({ key: `${String(row.type)}:${String(row.name)}`, sql: String(row.sql) }))
    .sort((a, b) => a.key.localeCompare(b.key)));

const ledgerVersions = (filename: string, table: string): number[] =>
  withDatabase(filename, (database) => (database.prepare(`SELECT version FROM ${table} ORDER BY version`).all() as SqlRow[]).map((row) => Number(row.version)));

/**
 * Two databases hold the same objects, the same fingerprints and the same
 * persisted bytes, and no definition carries a quoted identifier.
 * `expectedKeys` is checked against the fresh database so two empty sweeps
 * cannot agree.
 */
function assertReopenedMatchesFresh(input: {
  readonly label: string;
  readonly reopened: string;
  readonly fresh: string;
  readonly expectedKeys: readonly string[];
}): void {
  const reopenedObjects = persistedObjects(input.reopened);
  const freshObjects = persistedObjects(input.fresh);
  assert.deepEqual(freshObjects.map((object) => object.key), [...input.expectedKeys], `${input.label}: fresh object set`);
  assert.deepEqual(reopenedObjects.map((object) => object.key), freshObjects.map((object) => object.key), `${input.label}: object set`);
  assert.deepEqual(
    reopenedObjects.map((object) => ({ key: object.key, fingerprint: normalizedSchemaSql(object.sql) })),
    freshObjects.map((object) => ({ key: object.key, fingerprint: normalizedSchemaSql(object.sql) })),
    `${input.label}: fingerprints`,
  );
  assert.deepEqual(reopenedObjects, freshObjects, `${input.label}: persisted bytes`);
  assert.deepEqual(reopenedObjects.filter((object) => object.sql.includes('"')), [], `${input.label}: quoted identifiers`);
}

/**
 * Check the installation schema version and fresh-create/reopen parity directly so a reopen cannot
 * silently alter its schema.
 */
const LINEAR_INSTALLATION_KEYS = Object.freeze([
  "table:linear_channel_schema",
  "table:linear_installation_audit",
  "table:linear_installation_teams",
  "table:linear_installations",
  "table:linear_oauth_states",
]);

/**
 * The object keys a store's golden entry records, in `persistedObjects` order.
 *
 * Derived rather than transcribed. A second hand-written key list would be a
 * copy of the golden that cannot disagree with the store it describes, and the
 * hand-written lists above exist only for stores that have no golden at all.
 */
const goldenKeys = (label: string): readonly string[] => {
  const golden = GOLDEN_STORE_SCHEMAS[label];
  assert.ok(golden !== undefined, `${label}: no golden schema is recorded, so its key list cannot be derived`);
  return Object.freeze([...golden.swept, ...golden.outsideSweep]
    .map((object) => object.key)
    .sort((a, b) => a.localeCompare(b)));
};

/** A fresh database holds exactly the DDL and fingerprints the golden records. */
function assertFreshMatchesGolden(label: string, filename: string): void {
  const golden = GOLDEN_STORE_SCHEMAS[label];
  assert.ok(golden !== undefined, `${label}: no golden schema is recorded`);

  const recorded = [...golden.swept, ...golden.outsideSweep]
    .map((object) => ({ key: object.key, sql: object.ddl, fingerprint: object.fingerprint }))
    .sort((a, b) => a.key.localeCompare(b.key));
  const actual = persistedObjects(filename)
    .map((object) => ({ key: object.key, sql: object.sql, fingerprint: normalizedSchemaSql(object.sql) }));

  assert.ok(recorded.length > 0, `${label}: the golden records no objects, so the comparison below would pass vacuously`);
  assert.deepEqual(actual, recorded,
    `${label}: a freshly created database must hold exactly the DDL and fingerprints the golden records for the current schema`);
}

/**
 * The installation and team-binding stores: a fresh database records exactly version 1, a reopen
 * adds no ledger row and matches a fresh create, and the Router stores match
 * their golden DDL.
 */
for (const store of [
  {
    label: "linear-installation",
    ledger: "linear_channel_schema",
    open: (filename: string) => new LinearInstallationStore(filename),
    keys: LINEAR_INSTALLATION_KEYS,
    golden: false,
  },
  // Team bindings carry a golden entry for the stronger DDL comparison.
  {
    label: "team-bindings",
    ledger: "team_binding_schema",
    open: (filename: string) => new SqliteTeamBindingStore({ filename }),
    keys: goldenKeys("team-bindings"),
    golden: true,
  },
] as const) {
  test(`${store.label} is schema version 1 and a reopen matches a fresh create`, (context) => {
    const reopenedDb = temporaryDatabase(); context.after(reopenedDb.cleanup);
    const freshDb = temporaryDatabase(); context.after(freshDb.cleanup);

    store.open(reopenedDb.filename).close();
    assert.deepEqual(ledgerVersions(reopenedDb.filename, store.ledger), [1],
      `${store.label}: a fresh database must record exactly version 1`);

    const reopened = store.open(reopenedDb.filename); context.after(() => reopened.close());
    assert.deepEqual(ledgerVersions(reopenedDb.filename, store.ledger), [1],
      `${store.label}: reopening must not add a ledger row`);

    const fresh = store.open(freshDb.filename); context.after(() => fresh.close());

    assertReopenedMatchesFresh({
      label: `${store.label} reopen`,
      reopened: reopenedDb.filename,
      fresh: freshDb.filename,
      expectedKeys: store.keys,
    });

    if (store.golden) assertFreshMatchesGolden(store.label, freshDb.filename);
  });

  for (const fault of ["unsupported", "extra", "empty", "timestamp", "missing-table"] as const) {
    test(`${store.label} refuses the ${fault} schema fault without repairing it`, (context) => {
      const file = temporaryDatabase(); context.after(file.cleanup);
      store.open(file.filename).close();
      withDatabase(file.filename, (database) => {
        if (fault === "unsupported") database.exec(`UPDATE ${store.ledger} SET version = 987654321`);
        if (fault === "extra") database.exec(`INSERT INTO ${store.ledger} VALUES (987654321, '2026-01-01T00:00:00.000Z')`);
        if (fault === "empty") database.exec(`DELETE FROM ${store.ledger}`);
        if (fault === "timestamp") database.exec(`UPDATE ${store.ledger} SET applied_at = 'fictional-instant'`);
        if (fault === "missing-table") database.exec(`DROP TABLE ${store.ledger}`);
      });
      const before = persistedObjects(file.filename);
      const rows = fault === "missing-table" ? null : withDatabase(file.filename, (database) => database.prepare(`SELECT * FROM ${store.ledger}`).all());
      assert.throws(() => store.open(file.filename), { code: store.label === "linear-installation" ? "axis.relay.schema_version_unsupported" : "persisted_state_conflict" });
      assert.deepEqual(persistedObjects(file.filename), before);
      if (rows !== null) assert.deepEqual(withDatabase(file.filename, (database) => database.prepare(`SELECT * FROM ${store.ledger}`).all()), rows);
    });
  }
}

type MutuallyAssignable<Left, Right> = [Left] extends [Right] ? ([Right] extends [Left] ? true : false) : false;
type AssertTrue<Holds extends true> = Holds;
type ChannelOutboxStateIsWhatIsPersisted = AssertTrue<MutuallyAssignable<ChannelOutboxState, PersistedChannelOutboxState>>;

test("the neutral ChannelOutboxState union and the persisted state vocabulary stay mutually assignable", () => {
  // The assertion is the type alias above: `MutuallyAssignable`'s constraints
  // are checked wherever it is instantiated, so a drift in either direction
  // fails `npm run lint` whether or not this case ever runs. What is left for
  // runtime is the anti-vacuity half -- an empty tuple would make the union
  // side of that pin unfalsifiable.
  assert.ok(CHANNEL_OUTBOX_STATES.length > 0,
    "the persisted vocabulary must be non-empty, or the compile-time pin above holds against nothing");
  assert.equal(new Set(CHANNEL_OUTBOX_STATES).size, CHANNEL_OUTBOX_STATES.length,
    "and must not repeat a state, which would let a member be dropped from the union without changing its length");
});

/**
 * Schema normalization elides IF NOT EXISTS only for TABLE, TRIGGER and INDEX. SQLite
 * omits that clause from persisted DDL, so matching normalized definitions must remain
 * compatible with stored schemas.
 *
 * The source scan separately establishes that current fingerprinted definitions do not
 * spell the clause. Adding it can preserve database compatibility while invalidating
 * the claim that elision is inert. Read the module-private definition blocks from
 * source to check that premise.
 */
test("the IF NOT EXISTS elision list is exactly TABLE/TRIGGER/INDEX, and no fingerprinted definition spells the clause", () => {
  assert.deepEqual(
    [...SCHEMA_IF_NOT_EXISTS_ELIDED_AFTER],
    ["TABLE", "TRIGGER", "INDEX"],
    "adding or dropping a keyword shifts the fingerprint of any definition spelling that form, so it is a decision to argue for in a diff",
  );

  const sourceRoot = fileURLToPath(new URL("../../src/", import.meta.url));
  const sources: string[] = [];
  const walk = (directory: string): void => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile() && path.endsWith(".ts")) sources.push(path);
    }
  };
  walk(sourceRoot);

  const offenders: string[] = [];
  const scanned: string[] = [];
  let characters = 0;
  for (const path of sources) {
    const text = readFileSync(path, "utf8");
    const open = text.indexOf("const schemaDefinitions = Object.freeze({");
    if (open === -1) continue;
    const close = text.indexOf("\n});", open);
    assert.notEqual(close, -1, `${relative(sourceRoot, path)}'s schemaDefinitions must terminate`);
    const block = text.slice(open, close);
    characters += block.length;
    scanned.push(relative(sourceRoot, path).replaceAll("\\", "/"));
    if (/IF\s+NOT\s+EXISTS/iu.test(block)) offenders.push(relative(sourceRoot, path));
  }
  assert.deepEqual(offenders, []);

  // Not vacuous, in the two ways it could be: a walk that matched no file, and
  // slices that came back empty, both satisfy the assertion above having read
  // nothing. The stores with fingerprinted schemas must be among the
  // ones actually scanned, and the blocks must be the real ones.
  for (const expected of ["relay/linear/inbox.ts", "relay/channel-outbox.ts", "router/channel-conversation.ts", "router/channel-run-chain.ts"]) {
    assert.ok(scanned.includes(expected), `${expected} declares a fingerprinted schema and must have been scanned; scanned ${scanned.join(", ")}`);
  }
  assert.ok(characters > 20_000, `expected the real definition blocks; scanned ${characters} characters`);
});


for (const store of [
  { label: "installation", table: "linear_installations", column: "organization_id", refusal: "axis.relay.schema_version_unsupported", open: (filename: string) => new LinearInstallationStore(filename) },
  { label: "team bindings", table: "team_binding_revisions", column: "organization_id", refusal: "persisted_state_conflict", open: (filename: string) => new SqliteTeamBindingStore({ filename }) },
  { label: "inbox", table: "linear_inbox_deliveries", column: "delivery_id", refusal: "linear.ingress.storage_unavailable", open: (filename: string) => new SqliteLinearInbox({ filename }) },
  { label: "ledgers", table: "linear_ledger_sessions", column: "agent_session_id", refusal: "linear.ingress.storage_unavailable", open: (filename: string) => new SqliteLinearLedgers({ filename }) },
  { label: "outbox", table: "channel_message_outbox", column: "id", refusal: "persisted_state_conflict", open: (filename: string) => new SqliteChannelOutboxStore(filename) },
  { label: "conversations", table: "channel_conversations", column: "conversation_id", refusal: "persisted_state_conflict", open: (filename: string) => new SqliteChannelConversationStore({ filename }) },
  { label: "run chain", table: "channel_run_chain_conversations", column: "conversation_id", refusal: "persisted_state_conflict", open: (filename: string) => {
    const conversations = new SqliteChannelConversationStore();
    try {
      const chain = new SqliteChannelRunChainStore({ conversations, filename });
      return { close() { chain.close(); conversations.close(); } };
    } catch (error) { conversations.close(); throw error; }
  } },
  { label: "Router", table: "router_tasks", column: "task_id", refusal: "axis.router.recovery_required", open: (filename: string) => new Router({ filename }) },
] as const) {
  for (const kind of ["trigger", "index"] as const) {
    test(`${store.label} refuses a sqliteXguard ${kind} attached to an owned table and preserves unrelated objects`, (context) => {
      const file = temporaryDatabase(); context.after(file.cleanup);
      store.open(file.filename).close();
      withDatabase(file.filename, (database) => database.exec(`
        CREATE TABLE unrelated_rows(id TEXT);
        CREATE INDEX unrelated_index ON unrelated_rows(id);
        CREATE TRIGGER unrelated_gate BEFORE INSERT ON unrelated_rows BEGIN SELECT RAISE(ABORT, 'unrelated guard'); END;
      `));
      store.open(file.filename).close();
      withDatabase(file.filename, (database) => database.exec(kind === "trigger"
        ? `CREATE TRIGGER sqliteXguard BEFORE INSERT ON ${store.table} BEGIN SELECT RAISE(ABORT, 'unexpected guard'); END`
        : `CREATE INDEX sqliteXguard ON ${store.table}(${store.column})`));
      const before = persistedObjects(file.filename);
      assert.throws(() => store.open(file.filename), { code: store.refusal });
      assert.deepEqual(persistedObjects(file.filename), before);
    });
  }
}
