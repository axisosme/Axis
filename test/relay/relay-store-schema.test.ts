import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { SqliteChannelOutboxStore } from "../../src/relay/channel-outbox.js";
import { SqliteLinearInbox } from "../../src/relay/linear/inbox.js";
import { LINEAR_LEDGERS_SCHEMA_VERSION, SqliteLinearLedgers } from "../../src/relay/linear/ledgers.js";
import { normalizedSchemaSql } from "../../src/shared/schema-ledger.js";
import { GOLDEN_STORE_SCHEMAS } from "../fixtures/schema-ledger-goldens.js";
import { temporaryDatabase } from "../helpers.js";

interface RelayStore {
  readonly label: string;
  readonly ledgerTable: string;
  readonly version: number;
  readonly open: (filename: string) => { close(): void };
  readonly refusal: Record<string, unknown>;
}

const STORES: readonly RelayStore[] = [
  {
    label: "channel-outbox",
    ledgerTable: "channel_outbox_schema",
    version: GOLDEN_STORE_SCHEMAS["channel-outbox"]!.version,
    open: (filename) => new SqliteChannelOutboxStore(filename),
    refusal: { code: "schema_version_unsupported" },
  },
  {
    label: "linear-inbox",
    ledgerTable: "linear_inbox_schema",
    version: GOLDEN_STORE_SCHEMAS["linear-inbox"]!.version,
    open: (filename) => new SqliteLinearInbox({ filename }),
    refusal: { code: "axis.relay.schema_version_unsupported" },
  },
  {
    label: "linear-ledgers",
    ledgerTable: "linear_ledger_schema",
    version: LINEAR_LEDGERS_SCHEMA_VERSION,
    open: (filename) => new SqliteLinearLedgers({ filename }),
    refusal: { code: "axis.relay.schema_version_unsupported" },
  },
];

function ledgerVersions(filename: string, table: string): number[] {
  const database = new DatabaseSync(filename);
  try {
    return (database.prepare(`SELECT version FROM ${table} ORDER BY version`).all() as { version: number }[]).map((row) => Number(row.version));
  } finally {
    database.close();
  }
}

for (const store of STORES) {
  test(`${store.label}: an empty database gets the current schema and one ledger row, and a reopen keeps it`, (context) => {
    const file = temporaryDatabase(); context.after(file.cleanup);
    store.open(file.filename).close();
    store.open(file.filename).close();
    assert.deepEqual(ledgerVersions(file.filename, store.ledgerTable), [store.version]);
  });

  for (const recorded of [987654321, 123456789]) {
    test(`${store.label}: a database whose ledger is at version ${recorded} is refused`, (context) => {
      const file = temporaryDatabase(); context.after(file.cleanup);
      store.open(file.filename).close();
      const database = new DatabaseSync(file.filename);
      database.prepare(`UPDATE ${store.ledgerTable} SET version = ?`).run(recorded);
      database.close();
      assert.throws(() => store.open(file.filename), store.refusal);
    });
  }
}

for (const label of ["channel-outbox", "linear-inbox"] as const) {
  test(`${label}: a fresh database matches the current golden DDL`, (context) => {
    const file = temporaryDatabase(); context.after(file.cleanup);
    STORES.find((store) => store.label === label)!.open(file.filename).close();
    const golden = GOLDEN_STORE_SCHEMAS[label]!;
    const database = new DatabaseSync(file.filename);
    const actual = (database.prepare("SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL").all() as { type: string; name: string; sql: string }[])
      .map((row) => ({ key: `${row.type}:${row.name}`, sql: row.sql, fingerprint: normalizedSchemaSql(row.sql) }))
      .sort((a, b) => a.key.localeCompare(b.key));
    database.close();
    const recorded = [...golden.swept, ...golden.outsideSweep]
      .map((object) => ({ key: object.key, sql: object.ddl, fingerprint: object.fingerprint }))
      .sort((a, b) => a.key.localeCompare(b.key));
    assert.deepEqual(actual, recorded);
  });
}
