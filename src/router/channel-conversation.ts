import type { DatabaseSync } from "node:sqlite";

import {
  CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
  CHANNEL_WORKER_ROUTING_ADMISSION_VERSION,
  ChannelSessionInputError,
  ChannelSessionRefusalError,
  channelConversationScope,
  channelConversationScopeKey,
  channelDigest,
  channelMessageUniquenessDomain,
  linearChannelConversationScope,
} from "./channel-conversation-types.js";
import type {
  ChannelConversation,
  ChannelConversationRegistryPort,
  ChannelConversationResolution,
  ChannelConversationScope,
  ChannelDigest,
  ChannelInboundDeliveryReceipt,
  ChannelMessageInvocation,
  ChannelMessageInvocationReceipt,
} from "./channel-conversation-types.js";
import { durableStoreInput, frozen, openDurableStoreConnection, type DurableStoreConnection } from "../shared/durable-store.js";
import { schemaFingerprintTable, schemaLedgerAgrees, schemaLedgerVersion, schemaObjectsMatch } from "../shared/schema-ledger.js";

/**
 * This store's own persistence failures. Refusals in the contract's vocabulary
 * are thrown as `ChannelSessionRefusalError`.
 */
export type ChannelConversationStoreErrorCode = "schema_version_unsupported" | "persisted_state_conflict";

const errorMessages: Readonly<Record<ChannelConversationStoreErrorCode, string>> = {
  schema_version_unsupported: "Channel conversation database is not at the schema version this build opens.",
  persisted_state_conflict: "Channel conversation persisted state conflict.",
};

export class ChannelConversationStoreError extends Error {
  readonly code: ChannelConversationStoreErrorCode;
  constructor(code: unknown) {
    const normalized: ChannelConversationStoreErrorCode = typeof code === "string" && Object.hasOwn(errorMessages, code) ? code as ChannelConversationStoreErrorCode : "persisted_state_conflict";
    super(errorMessages[normalized]);
    this.code = normalized;
    this.name = "ChannelConversationStoreError";
  }
}
class ChannelConversationInputError extends TypeError { constructor() { super("Channel conversation input is malformed."); } }
function malformed(): never { throw new ChannelConversationInputError(); }
/**
 * Runs a contract validator and converts its `ChannelSessionInputError` into
 * this store's `TypeError`, so malformed input is always a `TypeError`.
 */
function validated<T>(derive: () => T): T {
  try { return derive(); }
  catch (error) { if (error instanceof ChannelSessionInputError) malformed(); throw error; }
}

export interface SqliteChannelConversationStoreOptions {
  readonly filename?: string;
  readonly now?: () => Date;

  readonly busyTimeoutMs?: number;
}

type Row = Record<string, unknown>;

const schemaDefinitions = Object.freeze({
  "table:channel_conversation_schema": "CREATE TABLE channel_conversation_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
  "table:channel_conversations": "CREATE TABLE channel_conversations(sequence INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id TEXT NOT NULL UNIQUE, scope_system TEXT NOT NULL CHECK(scope_system IN ('linear')), scope_domain TEXT NOT NULL, scope_selector TEXT NOT NULL, contract_version TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('active', 'closed')), created_at TEXT NOT NULL, closed_at TEXT, CHECK(status = 'active' AND closed_at IS NULL OR status = 'closed' AND closed_at IS NOT NULL))",
  "table:channel_conversation_current": "CREATE TABLE channel_conversation_current(scope_system TEXT NOT NULL, scope_domain TEXT NOT NULL, scope_selector TEXT NOT NULL, conversation_id TEXT NOT NULL UNIQUE, PRIMARY KEY(scope_system, scope_domain, scope_selector), FOREIGN KEY(conversation_id) REFERENCES channel_conversations(conversation_id))",
  "table:channel_conversation_messages": "CREATE TABLE channel_conversation_messages(scope_system TEXT NOT NULL, scope_domain TEXT NOT NULL, scope_selector TEXT NOT NULL, message_id TEXT NOT NULL, conversation_id TEXT NOT NULL, contract_version TEXT NOT NULL, instruction_digest TEXT NOT NULL, accepted_at TEXT NOT NULL, PRIMARY KEY(scope_system, scope_domain, message_id), FOREIGN KEY(conversation_id) REFERENCES channel_conversations(conversation_id))",
  "table:channel_conversation_deliveries": "CREATE TABLE channel_conversation_deliveries(delivery_id TEXT PRIMARY KEY, received_at TEXT NOT NULL)",
  "table:channel_conversation_worker_routing_admissions": "CREATE TABLE channel_conversation_worker_routing_admissions(conversation_id TEXT PRIMARY KEY, contract_version TEXT NOT NULL, admitted_at TEXT NOT NULL, FOREIGN KEY(conversation_id) REFERENCES channel_conversations(conversation_id))",
  "trigger:channel_conversation_messages_immutable_update": "CREATE TRIGGER channel_conversation_messages_immutable_update BEFORE UPDATE ON channel_conversation_messages BEGIN SELECT RAISE(ABORT, 'channel conversation messages are immutable'); END",
  "trigger:channel_conversation_messages_immutable_delete": "CREATE TRIGGER channel_conversation_messages_immutable_delete BEFORE DELETE ON channel_conversation_messages BEGIN SELECT RAISE(ABORT, 'channel conversation messages are immutable'); END",
  "trigger:channel_conversation_deliveries_immutable_update": "CREATE TRIGGER channel_conversation_deliveries_immutable_update BEFORE UPDATE ON channel_conversation_deliveries BEGIN SELECT RAISE(ABORT, 'channel inbound deliveries are immutable'); END",
  "trigger:channel_conversation_deliveries_immutable_delete": "CREATE TRIGGER channel_conversation_deliveries_immutable_delete BEFORE DELETE ON channel_conversation_deliveries BEGIN SELECT RAISE(ABORT, 'channel inbound deliveries are immutable'); END",
  "trigger:channel_conversation_worker_routing_admissions_immutable_update": "CREATE TRIGGER channel_conversation_worker_routing_admissions_immutable_update BEFORE UPDATE ON channel_conversation_worker_routing_admissions BEGIN SELECT RAISE(ABORT, 'channel conversation worker routing admissions are immutable'); END",
  "trigger:channel_conversation_worker_routing_admissions_immutable_delete": "CREATE TRIGGER channel_conversation_worker_routing_admissions_immutable_delete BEFORE DELETE ON channel_conversation_worker_routing_admissions BEGIN SELECT RAISE(ABORT, 'channel conversation worker routing admissions are immutable'); END",
});

const schemaFingerprints = schemaFingerprintTable(schemaDefinitions);

/** The schema version this build creates and opens. */
export const CHANNEL_CONVERSATION_SCHEMA_VERSION = 1;

const { plainData, opaque } = durableStoreInput(ChannelConversationInputError);

/** The persisted, normalized form of a `ChannelConversationScope` -- see this module's header. */
interface ScopeColumns {
  readonly system: "linear";
  /** The domain a provider `messageId` is unique within. */
  readonly domain: string;
  /** The conversation selector within that domain. */
  readonly selector: string;
}

/**
 * Validates a scope through the contract's constructors and splits it into the
 * three persisted columns, asserting the split still agrees with
 * `channelConversationScopeKey` and `channelMessageUniquenessDomain`.
 */
function scopeColumns(value: unknown): ScopeColumns {
  const normalized = validated(() => channelConversationScope(value));
  const columns: ScopeColumns = { system: "linear", domain: normalized.organizationId, selector: normalized.agentSessionId };
  if (channelConversationScopeKey(normalized) !== `${columns.system}:${columns.domain}:${columns.selector}`
    || channelMessageUniquenessDomain(normalized) !== `${columns.system}:${columns.domain}`) throw new ChannelConversationStoreError("persisted_state_conflict");
  return columns;
}

/** Rebuilds a scope from its persisted columns, re-validating through the contract's constructors. */
function scopeFromColumns(system: unknown, domain: unknown, selector: unknown): ChannelConversationScope {
  if (system === "linear") return validated(() => linearChannelConversationScope({ organizationId: opaque(domain), agentSessionId: opaque(selector) }));
  return malformed();
}
function options(value: unknown): { readonly filename?: string; readonly now?: () => Date; readonly busyTimeoutMs?: number } {
  const data = plainData(value);
  const keys = Object.keys(data), allowed = ["filename", "now", "busyTimeoutMs"];
  if (keys.some((key) => !allowed.includes(key))) malformed();
  if (data.filename !== undefined && typeof data.filename !== "string") malformed();
  if (data.now !== undefined && typeof data.now !== "function") malformed();
  if (data.busyTimeoutMs !== undefined && (!Number.isSafeInteger(data.busyTimeoutMs) || (data.busyTimeoutMs as number) < 0)) malformed();
  return data as { readonly filename?: string; readonly now?: () => Date; readonly busyTimeoutMs?: number };
}

/** The `sha256:`-prefixed digest form this store persists, validated by the contract's `channelDigest`. */
function instructionDigestValue(value: unknown): ChannelDigest {
  return validated(() => channelDigest(value));
}

export class SqliteChannelConversationStore implements ChannelConversationRegistryPort {
  readonly #db: DatabaseSync;
  readonly #now: () => Date;
  readonly #connection: DurableStoreConnection;
  /**
   * Whether the most recent full invariant check failed. `#assertUsable` reads it on
   * every public method, so a store condemned by `verifyIntegrity()` refuses traffic
   * and `readyForTraffic()` agrees with the operations. Written only by
   * `#assertInvariants`; it clears when a later full check passes.
   */
  #invariantFault = false;

  constructor(input: SqliteChannelConversationStoreOptions = {}) {
    const normalized = options(input);
    this.#connection = openDurableStoreConnection(normalized, {
      conflict: () => new ChannelConversationStoreError("persisted_state_conflict"),
      preserve: (error) => error instanceof ChannelConversationStoreError || error instanceof ChannelSessionRefusalError,
    });
    this.#db = this.#connection.db;
    this.#now = normalized.now ?? (() => new Date());
    this.#connection.initialize(() => { this.#initializeSchema(); this.#assertInvariants(); });
  }

  close(): void { this.#connection.close(); }
  /**
   * The full health check: both whole-database pragmas, the schema sweep and the
   * data invariants. Behind `GET /health/diagnostics`; readiness calls
   * `readyForTraffic()`.
   */
  ready(): boolean {
    try { this.verifyIntegrity(); return true; } catch { return false; }
  }

  /**
   * The full check `ready()` reports, thrown instead, as
   * `ChannelConversationStoreError("persisted_state_conflict")`. Run by the
   * constructor and by both observe ticks on `../relay/integrity-cadence.ts`'s
   * cadence.
   */
  verifyIntegrity(): void { this.#assertOpen(); this.#assertInvariants(); }

  /**
   * Whether this store can serve traffic, at a cost independent of its size: the
   * schema sweep and the fault latch. The data invariants are also
   * size-proportional and stay on the diagnostics path.
   */
  readyForTraffic(): boolean {
    // The same code as every public method's guard, so the probe and the operations
    // cannot disagree. The latch covers damage that leaves the schema intact.
    try { this.#assertUsable(); return true; } catch { return false; }
  }

  openOrResume(input: { readonly scope: ChannelConversationScope; readonly admitWorkerRouting?: true }): ChannelConversationResolution {
    const data = plainData(input);
    if (!Object.hasOwn(data, "scope") || Object.keys(data).some((key) => !["scope", "admitWorkerRouting"].includes(key))
      || (data.admitWorkerRouting !== undefined && data.admitWorkerRouting !== true)) malformed();
    const columns = scopeColumns(data.scope);
    const admitWorkerRouting = data.admitWorkerRouting === true;
    this.#assertUsable();
    return this.#transaction(() => {
      const existing = this.#currentRow(columns);
      if (existing !== null && existing.status === "active") return frozen({ conversation: existing, opened: false });
      // A closed Linear conversation's successor would reuse the same `agentSessionId`;
      // refused explicitly rather than as a UNIQUE violation.
      if (existing !== null && columns.system === "linear") throw new ChannelSessionRefusalError("conversation_closed");
      const conversationId = columns.selector;
      const createdAt = this.#now().toISOString();
      this.#db.prepare("INSERT INTO channel_conversations(conversation_id, scope_system, scope_domain, scope_selector, contract_version, status, created_at, closed_at) VALUES (?, ?, ?, ?, ?, 'active', ?, NULL)")
        .run(conversationId, columns.system, columns.domain, columns.selector, CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, createdAt);
      if (admitWorkerRouting) {
        this.#db.prepare("INSERT INTO channel_conversation_worker_routing_admissions(conversation_id, contract_version, admitted_at) VALUES (?, ?, ?)")
          .run(conversationId, CHANNEL_WORKER_ROUTING_ADMISSION_VERSION, createdAt);
      }
      this.#db.prepare(`INSERT INTO channel_conversation_current(scope_system, scope_domain, scope_selector, conversation_id) VALUES (?, ?, ?, ?)
        ON CONFLICT(scope_system, scope_domain, scope_selector) DO UPDATE SET conversation_id = excluded.conversation_id`)
        .run(columns.system, columns.domain, columns.selector, conversationId);
      const conversation = this.#conversation(conversationId);
      if (conversation === null) throw new ChannelConversationStoreError("persisted_state_conflict");
      return frozen({ conversation, opened: true });
    });
  }

  /** The conversation the pointer currently selects, or null when the issue has never been mentioned. */
  current(input: { readonly scope: ChannelConversationScope }): ChannelConversation | null {
    const data = plainData(input, ["scope"]);
    const columns = scopeColumns(data.scope);
    this.#assertUsable();
    return this.#readSnapshot(() => this.#currentRow(columns));
  }

  /**
   * One conversation by its own id, or null. Needs no scope, so a caller holding
   * only a `conversationId` can reach `scope.issueNumber`.
   */
  conversation(conversationId: string): ChannelConversation | null {
    const id = opaque(conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => this.#conversation(id));
  }

  /**
   * Every conversation ever opened for a scope, oldest first. Ordered by the
   * monotonic `sequence`, never `created_at`; `afterSequence` pages forward past
   * the cap.
   */
  history(input: { readonly scope: ChannelConversationScope; readonly afterSequence?: number; readonly limit?: number }): readonly ChannelConversation[] {
    const data = plainData(input), keys = Object.keys(data);
    if (keys.some((key) => !["scope", "afterSequence", "limit"].includes(key)) || !Object.hasOwn(data, "scope")) malformed();
    const columns = scopeColumns(data.scope);
    const limit: number = data.limit === undefined ? 100 : (Number.isSafeInteger(data.limit) ? data.limit as number : malformed());
    if (limit < 1 || limit > 100) malformed();
    const afterSequence: number = data.afterSequence === undefined ? 0 : (Number.isSafeInteger(data.afterSequence) && (data.afterSequence as number) >= 0 ? data.afterSequence as number : malformed());
    this.#assertUsable();
    return frozen((this.#db.prepare(`SELECT c.*, a.contract_version AS worker_routing_contract_version, a.admitted_at AS worker_routing_admitted_at
      FROM channel_conversations c LEFT JOIN channel_conversation_worker_routing_admissions a ON a.conversation_id = c.conversation_id
      WHERE c.scope_system = ? AND c.scope_domain = ? AND c.scope_selector = ? AND c.sequence > ? ORDER BY c.sequence ASC LIMIT ?`)
      .all(columns.system, columns.domain, columns.selector, afterSequence, limit) as Row[]).map((row) => this.#row(row)));
  }

  /**
   * Closes a conversation explicitly; a terminal Run never closes one. The pointer
   * stays so `history` is complete, and the next `openOrResume` opens a successor.
   */
  closeConversation(input: { readonly conversationId: string }): ChannelConversation {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#transaction(() => {
      const existing = this.#conversation(conversationId);
      if (existing === null) throw new ChannelSessionRefusalError("unknown_conversation");
      if (existing.status === "closed") throw new ChannelSessionRefusalError("conversation_closed");
      this.#db.prepare("UPDATE channel_conversations SET status = 'closed', closed_at = ? WHERE conversation_id = ?").run(this.#now().toISOString(), conversationId);
      const closed = this.#conversation(conversationId);
      if (closed === null) throw new ChannelConversationStoreError("persisted_state_conflict");
      return closed;
    });
  }

  /**
   * Binds an instruction-bearing message to a conversation, first write wins. The
   * same message and instruction replays with `replayed: true`; a different
   * instruction throws `message_rebound` and changes nothing.
   */
  recordMessageInvocation(input: { readonly scope: ChannelConversationScope; readonly messageId: string; readonly conversationId: string; readonly instructionDigest: ChannelDigest }): ChannelMessageInvocationReceipt {
    const data = plainData(input, ["scope", "messageId", "conversationId", "instructionDigest"]);
    const columns = scopeColumns(data.scope);
    const messageId = opaque(data.messageId), conversationId = opaque(data.conversationId);
    const digestValue = instructionDigestValue(data.instructionDigest);
    this.#assertUsable();
    return this.#transaction(() => {
      const existing = this.#invocation(columns.system, columns.domain, messageId);
      if (existing !== null) {
        if (existing.instructionDigest !== digestValue || existing.conversationId !== conversationId) throw new ChannelSessionRefusalError("message_rebound");
        return frozen({ invocation: existing, replayed: true });
      }
      const conversation = this.#conversation(conversationId);
      if (conversation === null) throw new ChannelSessionRefusalError("unknown_conversation");
      const conversationColumns = scopeColumns(conversation.scope);
      if (conversationColumns.system !== columns.system || conversationColumns.domain !== columns.domain) throw new ChannelSessionRefusalError("conversation_scope_mismatch");
      const acceptedAt = this.#now().toISOString();
      this.#db.prepare("INSERT INTO channel_conversation_messages(scope_system, scope_domain, scope_selector, message_id, conversation_id, contract_version, instruction_digest, accepted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        .run(columns.system, columns.domain, columns.selector, messageId, conversationId, CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, digestValue, acceptedAt);
      const inserted = this.#invocation(columns.system, columns.domain, messageId);
      if (inserted === null) throw new ChannelConversationStoreError("persisted_state_conflict");
      return frozen({ invocation: inserted, replayed: false });
    });
  }

  /** The invocation a comment is already bound to, or null. */
  messageInvocation(input: { readonly scope: ChannelConversationScope; readonly messageId: string }): ChannelMessageInvocation | null {
    const data = plainData(input, ["scope", "messageId"]);
    const columns = scopeColumns(data.scope), messageId = opaque(data.messageId);
    this.#assertUsable();
    return this.#readSnapshot(() => this.#invocation(columns.system, columns.domain, messageId));
  }

  /**
   * The conversation a `conversationId` names, or null. The redispatch trigger needs
   * its full scope to re-`reserve`.
   */
  byConversationId(input: { readonly conversationId: string }): ChannelConversation | null {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => this.#conversation(conversationId));
  }

  messageInvocationsByConversation(input: { readonly conversationId: string }): readonly ChannelMessageInvocation[] {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => frozen((this.#db.prepare("SELECT * FROM channel_conversation_messages WHERE conversation_id = ? ORDER BY accepted_at ASC").all(conversationId) as Row[]).map((row) => this.#invocationRow(row))));
  }

  /** Read-only duplicate probe used before an optional provider lookup. */
  hasDelivery(input: { readonly deliveryId: string }): boolean {
    const data = plainData(input, ["deliveryId"]);
    const deliveryId = opaque(data.deliveryId);
    this.#assertUsable();
    return this.#readSnapshot(() => this.#db.prepare("SELECT 1 FROM channel_conversation_deliveries WHERE delivery_id = ?").get(deliveryId) !== undefined);
  }

  /**
   * Records a webhook delivery id, reporting whether it was already seen. Delivery
   * identity is separate from conversation identity.
   */
  recordDelivery(input: { readonly deliveryId: string }): ChannelInboundDeliveryReceipt {
    const data = plainData(input, ["deliveryId"]);
    const deliveryId = opaque(data.deliveryId);
    this.#assertUsable();
    return this.#transaction(() => {
      const existing = this.#db.prepare("SELECT received_at FROM channel_conversation_deliveries WHERE delivery_id = ?").get(deliveryId) as Row | undefined;
      if (existing !== undefined) return frozen({ deliveryId, receivedAt: String(existing.received_at), duplicate: true });
      const receivedAt = this.#now().toISOString();
      this.#db.prepare("INSERT INTO channel_conversation_deliveries(delivery_id, received_at) VALUES (?, ?)").run(deliveryId, receivedAt);
      return frozen({ deliveryId, receivedAt, duplicate: false });
    });
  }

  /** Creates the current schema in an empty database; opens only a database already at the current version. */
  #initializeSchema(): void {
    const present = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE name LIKE 'channel_conversation%' LIMIT 1").get();
    if (present === undefined) {
      this.#transaction(() => {
        for (const definition of Object.values(schemaDefinitions)) this.#db.exec(definition);
        this.#db.prepare("INSERT INTO channel_conversation_schema(version, applied_at) VALUES (?, ?)").run(CHANNEL_CONVERSATION_SCHEMA_VERSION, this.#now().toISOString());
      });
      return;
    }
    const ledger = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'channel_conversation_schema'").get();
    if (ledger === undefined) throw new ChannelConversationStoreError("persisted_state_conflict");
    const version = schemaLedgerVersion(this.#db.prepare("SELECT version FROM channel_conversation_schema").all() as Row[]);
    if (version === null) throw new ChannelConversationStoreError("persisted_state_conflict");
    if (version !== CHANNEL_CONVERSATION_SCHEMA_VERSION) throw new ChannelConversationStoreError("schema_version_unsupported");
  }

  #currentRow(columns: ScopeColumns): ChannelConversation | null {
    const row = this.#db.prepare(`SELECT c.*, a.contract_version AS worker_routing_contract_version, a.admitted_at AS worker_routing_admitted_at
      FROM channel_conversation_current p JOIN channel_conversations c ON c.conversation_id = p.conversation_id
      LEFT JOIN channel_conversation_worker_routing_admissions a ON a.conversation_id = c.conversation_id
      WHERE p.scope_system = ? AND p.scope_domain = ? AND p.scope_selector = ?`).get(columns.system, columns.domain, columns.selector) as Row | undefined;
    return row === undefined ? null : this.#row(row);
  }
  #conversation(conversationId: string): ChannelConversation | null {
    const row = this.#db.prepare(`SELECT c.*, a.contract_version AS worker_routing_contract_version, a.admitted_at AS worker_routing_admitted_at
      FROM channel_conversations c LEFT JOIN channel_conversation_worker_routing_admissions a ON a.conversation_id = c.conversation_id
      WHERE c.conversation_id = ?`).get(conversationId) as Row | undefined;
    return row === undefined ? null : this.#row(row);
  }
  #invocation(system: string, domain: string, messageId: string): ChannelMessageInvocation | null {
    const row = this.#db.prepare("SELECT * FROM channel_conversation_messages WHERE scope_system = ? AND scope_domain = ? AND message_id = ?").get(system, domain, messageId) as Row | undefined;
    return row === undefined ? null : this.#invocationRow(row);
  }
  #invocationRow(row: Row): ChannelMessageInvocation {
    try {
      if (row.contract_version !== CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION || typeof row.accepted_at !== "string" || new Date(row.accepted_at).toISOString() !== row.accepted_at) throw new Error();
      return frozen({
        contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
        scope: scopeFromColumns(row.scope_system, row.scope_domain, row.scope_selector),
        messageId: opaque(row.message_id), conversationId: opaque(row.conversation_id),
        instructionDigest: instructionDigestValue(row.instruction_digest),
        acceptedAt: row.accepted_at,
      });
    } catch { throw new ChannelConversationStoreError("persisted_state_conflict"); }
  }
  #row(row: Row): ChannelConversation {
    try {
      const status = row.status;
      if (row.contract_version !== CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION || (status !== "active" && status !== "closed") || typeof row.created_at !== "string" || new Date(row.created_at).toISOString() !== row.created_at) throw new Error();
      if (status === "active" ? row.closed_at !== null : !(typeof row.closed_at === "string" && new Date(row.closed_at).toISOString() === row.closed_at)) throw new Error();
      if (!Number.isSafeInteger(row.sequence) || (row.sequence as number) < 1) throw new Error();
      const admitted = row.worker_routing_contract_version === null && row.worker_routing_admitted_at === null
        ? undefined
        : row.worker_routing_contract_version === CHANNEL_WORKER_ROUTING_ADMISSION_VERSION
          && typeof row.worker_routing_admitted_at === "string"
          && new Date(row.worker_routing_admitted_at).toISOString() === row.worker_routing_admitted_at
          ? { contractVersion: CHANNEL_WORKER_ROUTING_ADMISSION_VERSION, admittedAt: row.worker_routing_admitted_at }
          : (() => { throw new Error(); })();
      return frozen({
        contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
        sequence: row.sequence as number,
        scope: scopeFromColumns(row.scope_system, row.scope_domain, row.scope_selector),
        conversationId: opaque(row.conversation_id),
        status,
        createdAt: row.created_at,
        ...(status === "closed" ? { closedAt: row.closed_at as string } : {}),
        ...(admitted === undefined ? {} : { workerRoutingAdmission: admitted }),
      });
    } catch { throw new ChannelConversationStoreError("persisted_state_conflict"); }
  }
  #assertOpen(): void { this.#connection.assertOpen(); }

  /**
   * The guard every public method runs: the handle is open, the last full check did
   * not fail, and the schema is still exactly this build's. See
   * `./channel-run-chain.ts`'s `#assertUsable` for the full-check cadence and
   * detection window, which this store shares.
   */
  #assertUsable(): void {
    this.#assertOpen();
    if (this.#invariantFault) throw new ChannelConversationStoreError("persisted_state_conflict");
    this.#readSnapshot(() => this.#assertSchemaInvariants());
  }
  #assertInvariants(): void {
    try { this.#readSnapshot(() => this.#assertInvariantsUnsafe()); }
    catch (error) {
      // Latched before the rethrow, on every failure.
      this.#invariantFault = true;
      if (error instanceof ChannelConversationStoreError || error instanceof ChannelSessionRefusalError) throw error;
      throw new ChannelConversationStoreError("persisted_state_conflict");
    }
    this.#invariantFault = false;
  }
  #assertInvariantsUnsafe(): void {
    this.#assertScanInvariants();
    this.#assertSchemaInvariants();
    this.#assertDataInvariants();
  }

  /**
   * The two whole-database scans, proportional to the file size: run at
   * construction and on the diagnostics path, never by `readyForTraffic()`.
   */
  #assertScanInvariants(): void {
    const integrity = this.#db.prepare("PRAGMA integrity_check").get() as { integrity_check?: string } | undefined;
    if (integrity?.integrity_check !== "ok") throw new ChannelConversationStoreError("persisted_state_conflict");
    if (this.#db.prepare("PRAGMA foreign_key_check").all().length !== 0) throw new ChannelConversationStoreError("persisted_state_conflict");
  }

  /** The schema objects and the version ledger; bounded by object count, and the whole of `readyForTraffic()`. */
  #assertSchemaInvariants(): void {
    const objects = this.#db.prepare("SELECT type, name, sql FROM sqlite_master WHERE (name LIKE 'channel_conversation%' OR tbl_name LIKE 'channel_conversation%') AND name NOT GLOB 'sqlite_*'").all() as Row[];
    if (!schemaObjectsMatch(objects, schemaFingerprints)) throw new ChannelConversationStoreError("persisted_state_conflict");
    const ledger = this.#db.prepare("SELECT version, applied_at FROM channel_conversation_schema").all() as Row[];
    if (!schemaLedgerAgrees(ledger, CHANNEL_CONVERSATION_SCHEMA_VERSION)) throw new ChannelConversationStoreError("persisted_state_conflict");
  }

  /**
   * The cross-table consistency checks; scans and joins, so they stay off
   * `readyForTraffic()` with the pragmas.
   */
  #assertDataInvariants(): void {
    // A pointer must select a conversation of the same scope; a foreign key alone would allow another's.
    const mismatched = this.#db.prepare(`SELECT 1 FROM channel_conversation_current p JOIN channel_conversations c ON c.conversation_id = p.conversation_id
      WHERE c.scope_system != p.scope_system OR c.scope_domain != p.scope_domain OR c.scope_selector != p.scope_selector LIMIT 1`).get();
    if (mismatched !== undefined) throw new ChannelConversationStoreError("persisted_state_conflict");
    // At most one active conversation per scope. The pointer's PRIMARY KEY only makes
    // the pointer unique.
    const multipleActive = this.#db.prepare(`SELECT 1 FROM channel_conversations WHERE status = 'active'
      GROUP BY scope_system, scope_domain, scope_selector HAVING COUNT(*) > 1 LIMIT 1`).get();
    if (multipleActive !== undefined) throw new ChannelConversationStoreError("persisted_state_conflict");
    // Every conversation's issue must still have a pointer row: the two are
    // written in one transaction, so a missing pointer means external damage.
    const orphaned = this.#db.prepare(`SELECT 1 FROM channel_conversations c WHERE NOT EXISTS
      (SELECT 1 FROM channel_conversation_current p WHERE p.scope_system = c.scope_system AND p.scope_domain = c.scope_domain AND p.scope_selector = c.scope_selector) LIMIT 1`).get();
    if (orphaned !== undefined) throw new ChannelConversationStoreError("persisted_state_conflict");
    // A comment invocation must belong to its conversation's repository.
    const crossRepository = this.#db.prepare(`SELECT 1 FROM channel_conversation_messages i JOIN channel_conversations c ON c.conversation_id = i.conversation_id
      WHERE c.scope_system != i.scope_system OR c.scope_domain != i.scope_domain LIMIT 1`).get();
    if (crossRepository !== undefined) throw new ChannelConversationStoreError("persisted_state_conflict");
    for (const admission of this.#db.prepare("SELECT contract_version, admitted_at FROM channel_conversation_worker_routing_admissions").all() as Row[]) {
      if (admission.contract_version !== CHANNEL_WORKER_ROUTING_ADMISSION_VERSION
        || typeof admission.admitted_at !== "string" || new Date(admission.admitted_at).toISOString() !== admission.admitted_at) {
        throw new ChannelConversationStoreError("persisted_state_conflict");
      }
    }
  }
  #transaction<T>(operation: () => T): T {
    this.#assertOpen();
    if (this.#connection.inTransaction) return operation();
    try { this.#db.exec("BEGIN IMMEDIATE"); }
    catch { throw new ChannelConversationStoreError("persisted_state_conflict"); }
    this.#connection.inTransaction = true;
    try {
      const result = operation();
      try { this.#db.exec("COMMIT"); } catch { try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ } throw new ChannelConversationStoreError("persisted_state_conflict"); }
      return result;
    } catch (error) {
      try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ }
      if (error instanceof ChannelConversationStoreError || error instanceof ChannelSessionRefusalError) throw error;
      if (error instanceof ChannelConversationInputError) throw error;
      throw new ChannelConversationStoreError("persisted_state_conflict");
    } finally { this.#connection.inTransaction = false; }
  }
  /** Multi-statement reads share one snapshot under a concurrent writer. */
  #readSnapshot<T>(operation: () => T): T { return this.#connection.readSnapshot(operation); }
}
