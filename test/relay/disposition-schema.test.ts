import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { LinearInboxGiveUpDispositions, SqliteLinearInbox } from "../../src/relay/linear/inbox.js";
import { SqliteChannelConversationStore } from "../../src/router/channel-conversation.js";
import { ChannelRedispatchDispositions, SqliteChannelRunChainStore } from "../../src/router/channel-run-chain.js";
import { temporaryDatabase } from "../helpers.js";

for (const kind of ["inbox", "run-chain"] as const) {
  for (const fault of ["unsupported", "missing-record", "empty-record", "timestamp", "extra-record", "foreign-index"] as const) {
    test(`${kind} disposition reads refuse ${fault} storage on an already open reader`, context => {
      const file = temporaryDatabase(); context.after(file.cleanup);
      if (kind === "inbox") new SqliteLinearInbox({ filename: file.filename }).close();
      else {
        const conversations = new SqliteChannelConversationStore();
        try { new SqliteChannelRunChainStore({ filename: file.filename, conversations }).close(); }
        finally { conversations.close(); }
      }
      const reader = kind === "inbox" ? new LinearInboxGiveUpDispositions(file.filename) : new ChannelRedispatchDispositions(file.filename);
      context.after(() => reader.close());
      assert.deepEqual(reader.list(), []);
      const database = new DatabaseSync(file.filename);
      const ledger = kind === "inbox" ? "linear_inbox_schema" : "channel_run_chain_schema";
      try {
        if (fault === "unsupported") database.exec(`UPDATE ${ledger} SET version=987654321`);
        if (fault === "missing-record") database.exec(`DROP TABLE ${ledger}`);
        if (fault === "empty-record") database.exec(`DELETE FROM ${ledger}`);
        if (fault === "timestamp") database.exec(`UPDATE ${ledger} SET applied_at='fictional-instant'`);
        if (fault === "extra-record") database.exec(`INSERT INTO ${ledger} VALUES (987654321, '2026-01-01T00:00:00.000Z')`);
        if (fault === "foreign-index") database.exec(`CREATE INDEX sqliteXguard ON ${ledger}(version)`);
        const before = database.prepare("SELECT type, name, sql FROM sqlite_master ORDER BY name").all();
        const refusal = { code: kind === "inbox" ? "linear.ingress.storage_unavailable" : "persisted_state_conflict" };
        assert.throws(() => reader.list(), refusal);
        if (reader instanceof LinearInboxGiveUpDispositions) assert.throws(() => reader.recorded("delivery-one"), refusal);
        assert.deepEqual(database.prepare("SELECT type, name, sql FROM sqlite_master ORDER BY name").all(), before);
      } finally { database.close(); }
    });
  }
}
