import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import {
  ChannelSessionRefusalError,
  channelDeliveryTarget,
  channelDigest,
  channelInstructionDigest,
  type ChannelDeliveryTarget,
  type ChannelDigest,
  type ChannelMessageContent,
  type ChannelOutboxPort,
  type ChannelOutboxRecord,
  type ChannelOutboxState,
  type ChannelRenderedMessage,
} from "../router/channel-conversation-types.js";
import { elicitationDigest, type StructuredElicitation } from "../shared/elicitation.js";
import { safeNormalizedSchemaSql, schemaFingerprintTable, schemaLedgerAgrees, schemaLedgerVersion, schemaObjectsMatch } from "../shared/schema-ledger.js";
import { RETRY_BUDGET_MAX_ATTEMPTS, retryBackoffDelayMs } from "../shared/retry-budget.js";

/**
 * The channel-neutral outbox: a gapless per-conversation `axisSequence`
 * projection, idempotent enqueue, claim/lease/backoff delivery, ambiguous-row
 * reconciliation, and the immutable waiting-decision ledger.
 */

export type ChannelOutboxErrorCode = "schema_version_unsupported" | "persisted_state_conflict";

/** This store's own persistence failures. Port-level refusals use `ChannelSessionRefusalError`. */
export class ChannelOutboxError extends Error {
  readonly code: ChannelOutboxErrorCode;
  constructor(code: ChannelOutboxErrorCode) {
    super(`channel outbox storage failed: ${code}`);
    this.name = "ChannelOutboxError";
    this.code = code;
  }
}

/** Exponential backoff capped at 60s. The curve lives in `../shared/retry-budget.ts`. */
export const channelBackoffDelayMs = retryBackoffDelayMs;

/**
 * The give-up ceiling for a retryable failure: past it the row becomes
 * `confirmed_failure`. With 1-based attempts this is nine gaps of 2..60s,
 * about five minutes. Which failures are capped is channel knowledge.
 */
export const CHANNEL_MAX_RETRYABLE_ATTEMPTS = RETRY_BUDGET_MAX_ATTEMPTS;

/** The state vocabulary the current `CHECK` constraint enforces, checked by `assertPersistedStatesMatchSchema`. */
export const CHANNEL_OUTBOX_STATES = Object.freeze(["pending", "delivering", "delivered", "confirmed_failure", "ambiguous", "conflict"] as const);

/** The persisted state vocabulary as a type. Mutually assignable with `ChannelOutboxState` by construction. */
export type PersistedChannelOutboxState = (typeof CHANNEL_OUTBOX_STATES)[number];

/** The safe code for a lease that expired before any disposition, in the row's own channel's vocabulary. */
export function channelLeaseExpiredSafeCode(target: ChannelDeliveryTarget): string {
  return `${target.system}.provider.lease_expired`;
}

/**
 * The exact JSON bytes a content payload is stored and digested as. Key order
 * is preserved, not sorted: reconcilers re-derive the digest from the
 * provider's own canonical field order, so each renderer owes that order.
 */
export function channelContentJson(content: ChannelMessageContent): string {
  return JSON.stringify(content);
}

export function channelContentDigest(content: ChannelMessageContent): ChannelDigest {
  return channelInstructionDigest(channelContentJson(content));
}

// --------------------------------------------------------------------------
// Schema
// --------------------------------------------------------------------------

/**
 * The current schema, one canonical `CREATE` per object. `target` is one JSON
 * column holding a closed provider-discriminated union, re-validated through
 * `channelDeliveryTarget` on every read.
 */
const schemaDefinitions = Object.freeze({
  "table:channel_outbox_schema": "CREATE TABLE channel_outbox_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
  "table:channel_conversation_cursors": "CREATE TABLE channel_conversation_cursors(conversation_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL, target TEXT NOT NULL, updated_at TEXT NOT NULL)",
  "table:channel_message_outbox": "CREATE TABLE channel_message_outbox(id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, target TEXT NOT NULL, source_event_id TEXT NOT NULL, axis_sequence INTEGER NOT NULL, content TEXT NOT NULL, content_summary TEXT NOT NULL, content_digest TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN ('pending', 'delivering', 'delivered', 'confirmed_failure', 'ambiguous', 'conflict')), attempt_count INTEGER NOT NULL DEFAULT 0, available_at TEXT NOT NULL, lease_owner TEXT, lease_expires_at TEXT, provider_error_code TEXT, provider_message_id TEXT, created_at TEXT NOT NULL, delivered_at TEXT, reconcile_attempted_at TEXT, UNIQUE (conversation_id, source_event_id, content_digest))",
  "table:channel_message_delivery_attempts": "CREATE TABLE channel_message_delivery_attempts(attempt_id TEXT PRIMARY KEY, outbox_id TEXT NOT NULL REFERENCES channel_message_outbox(id), disposition TEXT NOT NULL CHECK (disposition IN ('confirmed', 'rejected', 'ambiguous', 'conflict')), safe_code TEXT, occurred_at TEXT NOT NULL)",
  "table:channel_waiting_decision_ledger": "CREATE TABLE channel_waiting_decision_ledger(decision_id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, source_event_id TEXT NOT NULL UNIQUE, task_id TEXT NOT NULL, attempt_id TEXT NOT NULL, run_id TEXT NOT NULL UNIQUE, axis_sequence INTEGER NOT NULL, elicitation_digest TEXT NOT NULL, elicitation_json TEXT NOT NULL, outbox_id TEXT NOT NULL UNIQUE REFERENCES channel_message_outbox(id), created_at TEXT NOT NULL)",
  "trigger:channel_waiting_decision_ledger_immutable_update": "CREATE TRIGGER channel_waiting_decision_ledger_immutable_update BEFORE UPDATE ON channel_waiting_decision_ledger BEGIN SELECT RAISE(ABORT, 'channel waiting decision ledger is immutable'); END",
  "trigger:channel_waiting_decision_ledger_immutable_delete": "CREATE TRIGGER channel_waiting_decision_ledger_immutable_delete BEFORE DELETE ON channel_waiting_decision_ledger BEGIN SELECT RAISE(ABORT, 'channel waiting decision ledger is immutable'); END",
  "index:channel_message_outbox_claim": "CREATE INDEX channel_message_outbox_claim ON channel_message_outbox(state, available_at)",
});

const schemaFingerprints = schemaFingerprintTable(schemaDefinitions);


/** The schema version this build writes and reads. Any other version is refused. */
const CHANNEL_OUTBOX_SCHEMA_VERSION = 1;

interface Row { readonly [column: string]: unknown }

/** Every object this store owns. The `_` is escaped because `LIKE` treats it as a wildcard. */
const CHANNEL_OBJECT_SWEEP_SQL = "SELECT type, name, sql FROM sqlite_master WHERE (name LIKE 'channel\\_%' ESCAPE '\\' OR tbl_name LIKE 'channel\\_%' ESCAPE '\\') AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'";

// --------------------------------------------------------------------------
// Store
// --------------------------------------------------------------------------

/**
 * How one outbox row's id is chosen. Injectable because a provider can
 * constrain it: Linear reuses the Worker receipt's `activityId`.
 * `rendered` is absent for a direct message.
 */
export type ChannelOutboxMessageIdSource = (input: {
  readonly conversationId: string;
  readonly sourceEventId: string;
  readonly rendered?: ChannelRenderedMessage;
}) => string;

export interface SqliteChannelOutboxStoreOptions {
  readonly now?: () => Date;
  readonly busyTimeoutMs?: number;
  /** See `ChannelOutboxMessageIdSource`. Defaults to `randomUUID`. */
  readonly messageId?: ChannelOutboxMessageIdSource;
  /** Runs the two whole-database scans at construction, so a damaged database refuses to open. Defaults to `false`. */
  readonly scanAtOpen?: boolean;
  /**
   * Test-only seam. Called inside each schema classification's read
   * transaction right after the `channel_%` sweep (see `#classifySchema`).
   */
  readonly afterSchemaObjectSweep?: (sweep: { readonly channelObjectsFound: boolean }) => void;
}

/** One outbox row's persisted summary, for an operator view or a log line that must not re-parse channel-shaped JSON. */
export interface ChannelOutboxMessageSummary {
  readonly state: ChannelOutboxState;
  readonly summary: string;
  readonly attemptCount: number;
  readonly providerMessageId: string | null;
  readonly safeCode: string | null;
}

/** The channel-neutral outbox, backed by one SQLite file per channel deployment. */
export class SqliteChannelOutboxStore implements ChannelOutboxPort {
  readonly #db: DatabaseSync;
  readonly #now: () => Date;
  readonly #messageId: ChannelOutboxMessageIdSource;
  readonly #afterSchemaObjectSweep: SqliteChannelOutboxStoreOptions["afterSchemaObjectSweep"];
  #closed = false;

  constructor(filename: string, options: SqliteChannelOutboxStoreOptions = {}) {
    this.#db = new DatabaseSync(filename);
    this.#now = options.now ?? (() => new Date());
    this.#messageId = options.messageId ?? (() => randomUUID());
    this.#afterSchemaObjectSweep = options.afterSchemaObjectSweep;
    try {
      this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;");
      this.#db.exec(`PRAGMA busy_timeout=${options.busyTimeoutMs ?? 5_000};`);
      this.#initializeSchema();
      // Opt-in; see `scanAtOpen`.
      if (options.scanAtOpen === true) this.#assertScanInvariants();
      this.#assertSchemaInvariants();
    } catch (error) {
      // A failed construction must not leak an open connection.
      try { this.#db.close(); } catch { /* preserve the original construction failure */ }
      this.#closed = true;
      throw error;
    }
  }

  close(): void {
    if (!this.#closed) { this.#db.close(); this.#closed = true; }
  }

  /** The full health check: the schema sweep plus both whole-database scans. Never throws. */
  ready(): boolean {
    if (this.#closed) return false;
    try {
      this.#assertScanInvariants();
      this.#assertSchemaInvariants();
      return true;
    } catch { return false; }
  }

  /** The two whole-database scans, shared by the constructor and `ready()`. Never run per call. */
  #assertScanInvariants(): void {
    const row = this.#db.prepare("PRAGMA integrity_check").get() as { integrity_check?: string } | undefined;
    if (row?.integrity_check !== "ok") throw new ChannelOutboxError("persisted_state_conflict");
    if (this.#db.prepare("PRAGMA foreign_key_check").all().length !== 0) throw new ChannelOutboxError("persisted_state_conflict");
  }

  /** `ready()` without its two whole-database scans, so its cost does not grow with the data. Never throws. */
  readyForTraffic(): boolean {
    if (this.#closed) return false;
    try { this.#assertSchemaInvariants(); return true; } catch { return false; }
  }

  /** 0 for a conversation never projected before -- matches `axisSequence` starting at 1. */
  cursor(conversationId: string): number {
    this.#assertUsable();
    return this.#cursorUnsafe(conversationId);
  }

  /**
   * Atomically advances the conversation cursor and enqueues one row per
   * rendered event, plus a waiting-decision row where the rendering carries
   * one. Returns `false` on a cursor mismatch so the caller can reload.
   * Sequences must be gapless (`outbox_not_gapless`); a `null` rendering still
   * advances the cursor and creates no row.
   */
  recordProjectedBatch(input: {
    readonly conversationId: string;
    readonly target: ChannelDeliveryTarget;
    readonly expectedCursor: number;
    readonly events: readonly {
      readonly axisSequence: number;
      readonly sourceEventId: string;
      readonly rendered: ChannelRenderedMessage | null;
    }[];
  }): boolean {
    this.#assertUsable();
    // Re-validated before it becomes durable.
    const target = JSON.stringify(channelDeliveryTarget(input.target));
    return this.#transaction(() => {
      const current = this.#cursorUnsafe(input.conversationId);
      if (current !== input.expectedCursor) return false;
      const now = this.#instant();
      this.#db.prepare(`
        INSERT INTO channel_conversation_cursors(conversation_id, cursor, target, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(conversation_id) DO UPDATE SET cursor = excluded.cursor, target = excluded.target, updated_at = excluded.updated_at
      `).run(input.conversationId, input.expectedCursor, target, now);

      const insert = this.#db.prepare(`
        INSERT OR IGNORE INTO channel_message_outbox(
          id, conversation_id, target, source_event_id, axis_sequence, content, content_summary, content_digest, state, available_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
      `);
      let nextCursor = input.expectedCursor;
      for (const event of input.events) {
        if (event.axisSequence !== nextCursor + 1) throw new ChannelSessionRefusalError("outbox_not_gapless");
        nextCursor = event.axisSequence;
        if (event.rendered === null) continue;
        const digest = channelContentDigest(event.rendered.content);
        insert.run(
          this.#messageId({ conversationId: input.conversationId, sourceEventId: event.sourceEventId, rendered: event.rendered }),
          input.conversationId, target, event.sourceEventId, event.axisSequence,
          channelContentJson(event.rendered.content), event.rendered.summary, digest, now, now,
        );
        // Re-read: `INSERT OR IGNORE` absorbs a replay, and the ledger must
        // reference the row that actually exists.
        const row = this.#db.prepare(`
          SELECT id FROM channel_message_outbox WHERE conversation_id = ? AND source_event_id = ? AND content_digest = ?
        `).get(input.conversationId, event.sourceEventId, digest) as { id: string };
        if (event.rendered.waiting !== undefined) {
          const elicitation = event.rendered.waiting.elicitation;
          this.#db.prepare(`
            INSERT INTO channel_waiting_decision_ledger(decision_id, conversation_id, source_event_id, task_id, attempt_id, run_id, axis_sequence, elicitation_digest, elicitation_json, outbox_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `).run(
            elicitation.decisionId, input.conversationId, event.sourceEventId, event.rendered.waiting.taskId, event.rendered.waiting.attemptId,
            event.rendered.waiting.runId, event.axisSequence, elicitationDigest(elicitation), JSON.stringify(elicitation), row.id, now,
          );
        }
      }
      this.#db.prepare("UPDATE channel_conversation_cursors SET cursor = ?, updated_at = ? WHERE conversation_id = ?").run(nextCursor, now, input.conversationId);
      return true;
    });
  }

  /**
   * Enqueues a message that is not a Router-event projection, such as a
   * command outcome. Idempotent on `(conversation_id, source_event_id)` only:
   * its content may differ on redelivery, and the first content wins.
   * `axisSequence` is `MAX + 1` for the conversation.
   */
  recordDirectMessage(input: {
    readonly conversationId: string;
    readonly target: ChannelDeliveryTarget;
    readonly sourceEventId: string;
    readonly content: ChannelMessageContent;
  }): { readonly outboxId: string; readonly created: boolean } {
    this.#assertUsable();
    const target = JSON.stringify(channelDeliveryTarget(input.target));
    return this.#transaction(() => {
      const existing = this.#db.prepare(`
        SELECT id FROM channel_message_outbox WHERE conversation_id = ? AND source_event_id = ?
      `).get(input.conversationId, input.sourceEventId) as { id: string } | undefined;
      if (existing !== undefined) return Object.freeze({ outboxId: existing.id, created: false });
      const now = this.#instant();
      const nextSequence = ((this.#db.prepare(`
        SELECT COALESCE(MAX(axis_sequence), 0) AS latest FROM channel_message_outbox WHERE conversation_id = ?
      `).get(input.conversationId) as { latest: number }).latest) + 1;
      const outboxId = this.#messageId({ conversationId: input.conversationId, sourceEventId: input.sourceEventId });
      this.#db.prepare(`
        INSERT INTO channel_message_outbox(
          id, conversation_id, target, source_event_id, axis_sequence, content, content_summary, content_digest, state, available_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
      `).run(
        outboxId, input.conversationId, target, input.sourceEventId, nextSequence,
        channelContentJson(input.content), channelDirectSummary(input.content), channelContentDigest(input.content), now, now,
      );
      return Object.freeze({ outboxId, created: true });
    });
  }

  /**
   * Claims the next due message under a lease, or `null`. An expired
   * `delivering` lease becomes `ambiguous`, never `pending`, because its holder
   * may already have posted. Ordered by `axis_sequence`, since a batch shares
   * one timestamp.
   */
  claim(owner: string, leaseMs = 30_000): ChannelOutboxRecord | null {
    this.#assertUsable();
    return this.#transaction(() => {
      const now = this.#instant();
      // Reaped row by row rather than in one UPDATE, because the safe code is
      // the row's own channel's -- see `channelLeaseExpiredSafeCode`.
      const expired = this.#db.prepare(`
        SELECT id, target FROM channel_message_outbox WHERE state = 'delivering' AND lease_expires_at < ?
      `).all(now) as unknown as Row[];
      const expire = this.#db.prepare(`
        UPDATE channel_message_outbox
        SET state = 'ambiguous', provider_error_code = ?, lease_owner = NULL, lease_expires_at = NULL
        WHERE id = ? AND state = 'delivering'
      `);
      const attempt = this.#db.prepare(`
        INSERT INTO channel_message_delivery_attempts(attempt_id, outbox_id, disposition, safe_code, occurred_at)
        VALUES (?, ?, 'ambiguous', ?, ?)
      `);
      for (const row of expired) {
        let safeCode: string;
        try { safeCode = channelLeaseExpiredSafeCode(channelDeliveryTarget(JSON.parse(String(row.target)))); }
        catch { throw new ChannelOutboxError("persisted_state_conflict"); }
        expire.run(safeCode, String(row.id));
        attempt.run(randomUUID(), String(row.id), safeCode, now);
      }

      const row = this.#db.prepare(`
        SELECT id, conversation_id, target, source_event_id, content, content_digest, attempt_count, available_at, created_at
        FROM channel_message_outbox
        WHERE state = 'pending' AND available_at <= ?
        ORDER BY conversation_id ASC, axis_sequence ASC LIMIT 1
      `).get(now) as unknown as Row | undefined;
      if (row === undefined) return null;
      const expires = new Date(this.#now().getTime() + leaseMs).toISOString();
      const changed = this.#db.prepare(`
        UPDATE channel_message_outbox SET state = 'delivering', lease_owner = ?, lease_expires_at = ?, attempt_count = attempt_count + 1
        WHERE id = ? AND state = 'pending'
      `).run(owner, expires, String(row.id)).changes;
      if (changed !== 1) return null;
      return this.#record(row, "delivering", Number(row.attempt_count) + 1);
    });
  }

  /** Terminally dispositions a claimed message, and appends the attempt to the delivery-attempt ledger. */
  recordDisposition(outboxId: string, input: {
    readonly disposition: "confirmed" | "rejected" | "ambiguous" | "conflict";
    readonly safeCode?: string;
    readonly providerMessageId?: string;
  }): void {
    this.#assertUsable();
    this.#transaction(() => {
      const state: ChannelOutboxState = input.disposition === "confirmed" ? "delivered"
        : input.disposition === "rejected" ? "confirmed_failure"
          : input.disposition;
      const result = this.#db.prepare(`
        UPDATE channel_message_outbox SET state = ?, provider_error_code = ?, provider_message_id = ?, delivered_at = ?,
          lease_owner = NULL, lease_expires_at = NULL
        WHERE id = ? AND state IN ('delivering', 'ambiguous')
      `).run(
        state, input.safeCode ?? null, input.providerMessageId ?? null,
        state === "delivered" ? this.#instant() : null, outboxId,
      );
      if (result.changes !== 1) throw new ChannelSessionRefusalError("outbox_not_deliverable");
      this.#db.prepare(`
        INSERT INTO channel_message_delivery_attempts(attempt_id, outbox_id, disposition, safe_code, occurred_at)
        VALUES (?, ?, ?, ?, ?)
      `).run(randomUUID(), outboxId, input.disposition, input.safeCode ?? null, this.#instant());
    });
  }

  /** Returns a claimed message to `pending`, available again after `delayMs`. */
  retry(outboxId: string, safeCode: string, delayMs: number): void {
    this.#assertUsable();
    const availableAt = new Date(this.#now().getTime() + delayMs).toISOString();
    this.#transaction(() => {
      const changed = this.#db.prepare(`
        UPDATE channel_message_outbox SET state = 'pending', provider_error_code = ?, available_at = ?,
          lease_owner = NULL, lease_expires_at = NULL
        WHERE id = ? AND state = 'delivering'
      `).run(safeCode, availableAt, outboxId).changes;
      if (changed !== 1) throw new ChannelSessionRefusalError("outbox_not_retryable");
      this.#db.prepare(`
        INSERT INTO channel_message_delivery_attempts(attempt_id, outbox_id, disposition, safe_code, occurred_at)
        VALUES (?, ?, 'rejected', ?, ?)
      `).run(randomUUID(), outboxId, safeCode, this.#instant());
    });
  }

  /**
   * Every `ambiguous` message, least-recently-reconciliation-attempted first,
   * so an unresolvable row cannot starve the rest.
   */
  ambiguousMessages(): readonly ChannelOutboxRecord[] {
    this.#assertUsable();
    return Object.freeze((this.#db.prepare(`
      SELECT id, conversation_id, target, source_event_id, content, content_digest, attempt_count, available_at, created_at
      FROM channel_message_outbox WHERE state = 'ambiguous'
      ORDER BY COALESCE(reconcile_attempted_at, '') ASC, created_at ASC, axis_sequence ASC
    `).all() as unknown as Row[]).map((row) => this.#record(row, "ambiguous", Number(row.attempt_count))));
  }

  /** Records an unresolved reconciliation attempt, so `ambiguousMessages()` rotates past the row. No-op if it is no longer ambiguous. */
  touchReconciliationAttempt(outboxId: string): void {
    this.#assertUsable();
    this.#db.prepare("UPDATE channel_message_outbox SET reconcile_attempted_at = ? WHERE id = ? AND state = 'ambiguous'").run(this.#instant(), outboxId);
  }

  /** The current state of one row, for tests and diagnostics -- `null` if it does not exist. */
  outboxState(outboxId: string): ChannelOutboxState | null {
    this.#assertUsable();
    const row = this.#db.prepare("SELECT state FROM channel_message_outbox WHERE id = ?").get(outboxId) as { state: ChannelOutboxState } | undefined;
    return row?.state ?? null;
  }

  /**
   * Whether this store has ever enqueued `messageId`, as a row id or a
   * recorded `provider_message_id`; backs the Linear feedback-loop gate. No
   * state filter: an `ambiguous` row may already be live at the provider.
   */
  ownsMessageId(messageId: string): boolean {
    this.#assertUsable();
    const row = this.#db.prepare(
      "SELECT 1 FROM channel_message_outbox WHERE id = ? OR provider_message_id = ? LIMIT 1",
    ).get(messageId, messageId);
    return row !== undefined;
  }

  /** One row's persisted `content_summary`, for views that must not re-parse channel JSON. */
  messageSummary(outboxId: string): ChannelOutboxMessageSummary | null {
    this.#assertUsable();
    const row = this.#db.prepare(`
      SELECT state, content_summary, attempt_count, provider_message_id, provider_error_code
      FROM channel_message_outbox WHERE id = ?
    `).get(outboxId) as Row | undefined;
    if (row === undefined) return null;
    return Object.freeze({
      state: String(row.state) as ChannelOutboxState,
      summary: String(row.content_summary),
      attemptCount: Number(row.attempt_count),
      providerMessageId: row.provider_message_id === null ? null : String(row.provider_message_id),
      safeCode: row.provider_error_code === null ? null : String(row.provider_error_code),
    });
  }

  /**
   * Every waiting decision ever raised on one conversation, newest first,
   * including unanswered cancelled ones. `run_id` is the key that joins these
   * rows to `channel_run_chain_decisions`.
   */
  waitingDecisionsForConversation(conversationId: string): readonly { readonly decisionId: string; readonly runId: string; readonly elicitation: StructuredElicitation }[] {
    this.#assertUsable();
    const rows = this.#db.prepare(`
      SELECT decision_id, run_id, elicitation_json FROM channel_waiting_decision_ledger
      WHERE conversation_id = ? ORDER BY axis_sequence DESC, decision_id DESC
    `).all(conversationId) as Row[];
    return Object.freeze(rows.map((row) => Object.freeze({
      decisionId: String(row.decision_id),
      runId: String(row.run_id),
      elicitation: JSON.parse(String(row.elicitation_json)) as StructuredElicitation,
    })));
  }

  /** One recorded waiting decision, or `null`. */
  waitingDecision(decisionId: string): { readonly conversationId: string; readonly elicitation: StructuredElicitation; readonly outboxId: string } | null {
    this.#assertUsable();
    const row = this.#db.prepare(`
      SELECT conversation_id, elicitation_json, outbox_id FROM channel_waiting_decision_ledger WHERE decision_id = ?
    `).get(decisionId) as Row | undefined;
    return row === undefined ? null : Object.freeze({
      conversationId: String(row.conversation_id),
      elicitation: JSON.parse(String(row.elicitation_json)) as StructuredElicitation,
      outboxId: String(row.outbox_id),
    });
  }

  /** The decision an outbox row's Activity offers, or `null`. */
  waitingDecisionForOutbox(outboxId: string): { readonly decisionId: string; readonly elicitation: StructuredElicitation } | null {
    this.#assertUsable();
    const row = this.#db.prepare(`
      SELECT decision_id, elicitation_json FROM channel_waiting_decision_ledger WHERE outbox_id = ?
    `).get(outboxId) as Row | undefined;
    return row === undefined ? null : Object.freeze({
      decisionId: String(row.decision_id),
      elicitation: JSON.parse(String(row.elicitation_json)) as StructuredElicitation,
    });
  }

  #cursorUnsafe(conversationId: string): number {
    const row = this.#db.prepare("SELECT cursor FROM channel_conversation_cursors WHERE conversation_id = ?").get(conversationId) as { cursor: number } | undefined;
    return row?.cursor ?? 0;
  }

  /** Rebuilds a record from a row, re-validating target and digest; a tampered column is refused. */
  #record(row: Row, state: ChannelOutboxState, attemptCount: number): ChannelOutboxRecord {
    try {
      return Object.freeze({
        id: String(row.id),
        conversationId: String(row.conversation_id),
        target: channelDeliveryTarget(JSON.parse(String(row.target))),
        sourceEventId: String(row.source_event_id),
        content: Object.freeze(JSON.parse(String(row.content)) as ChannelMessageContent),
        contentDigest: channelDigest(row.content_digest),
        state,
        attemptCount,
        availableAt: String(row.available_at),
        createdAt: String(row.created_at),
      });
    } catch { throw new ChannelOutboxError("persisted_state_conflict"); }
  }

  #assertUsable(): void {
    if (this.#closed) throw new ChannelOutboxError("persisted_state_conflict");
    this.#assertSchemaInvariants();
  }

  /** The cheap per-call check: fingerprint sweep plus schema ledger, without the whole-database scans. */
  #assertSchemaInvariants(): void {
    const objects = this.#db.prepare(CHANNEL_OBJECT_SWEEP_SQL).all() as Row[];
    // Non-throwing normalizer, so an unreadable `sql` fails as `ChannelOutboxError`.
    if (!schemaObjectsMatch(objects, schemaFingerprints, safeNormalizedSchemaSql)) {
      throw new ChannelOutboxError("persisted_state_conflict");
    }
    const ledger = this.#db.prepare("SELECT version, applied_at FROM channel_outbox_schema").all() as Row[];
    if (!schemaLedgerAgrees(ledger, CHANNEL_OUTBOX_SCHEMA_VERSION)) {
      throw new ChannelOutboxError("persisted_state_conflict");
    }
  }

  /** Creates the schema on a database with no outbox objects, and refuses one at any other version. */
  #initializeSchema(): void {
    let version = this.#classifySchema();
    if (version === null) {
      // Re-checked under the write lock: two first constructions can race.
      const bootstrapped = this.#transaction(() => {
        if (this.#tableExists("channel_outbox_schema")) return false;
        for (const definition of Object.values(schemaDefinitions)) this.#db.exec(definition);
        this.#recordSchemaVersion(CHANNEL_OUTBOX_SCHEMA_VERSION);
        return true;
      });
      if (bootstrapped) return;
      version = this.#classifySchema();
      if (version === null) throw new ChannelOutboxError("persisted_state_conflict");
    }
    if (version !== CHANNEL_OUTBOX_SCHEMA_VERSION) throw new ChannelOutboxError("schema_version_unsupported");
  }

  /**
   * `#recordedSchemaVersion` under one `BEGIN DEFERRED` snapshot, so a
   * concurrent bootstrap is never seen half-done. Committed before
   * `#initializeSchema` writes, since a promoted stale snapshot fails with
   * `SQLITE_BUSY_SNAPSHOT`.
   */
  #classifySchema(): number | null {
    this.#db.exec("BEGIN DEFERRED");
    let version: number | null;
    try {
      version = this.#recordedSchemaVersion();
    } catch (error) {
      this.#db.exec("ROLLBACK");
      throw error;
    }
    this.#db.exec("COMMIT");
    return version;
  }

  /** The single version in the ledger, or `null` with no outbox objects. */
  #recordedSchemaVersion(): number | null {
    const found = this.#db.prepare(`${CHANNEL_OBJECT_SWEEP_SQL} LIMIT 1`).get();
    this.#afterSchemaObjectSweep?.({ channelObjectsFound: found !== undefined });
    if (found === undefined) return null;
    // Objects without a ledger cannot be classified.
    if (!this.#tableExists("channel_outbox_schema")) throw new ChannelOutboxError("persisted_state_conflict");
    const version = schemaLedgerVersion(this.#db.prepare("SELECT version FROM channel_outbox_schema").all() as Row[]);
    if (version === null) throw new ChannelOutboxError("persisted_state_conflict");
    return version;
  }

  #tableExists(name: string): boolean {
    return this.#db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name) !== undefined;
  }

  #recordSchemaVersion(version: number): void {
    this.#db.prepare("INSERT INTO channel_outbox_schema(version, applied_at) VALUES (?, ?)").run(version, this.#instant());
  }

  #instant(): string {
    return this.#now().toISOString();
  }

  #transaction<T>(operation: () => T): T {
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.#db.exec("COMMIT");
      return result;
    } catch (error) {
      this.#db.exec("ROLLBACK");
      throw error;
    }
  }
}

/** The summary for a direct message: its `body` when it has one, else its JSON. */
function channelDirectSummary(content: ChannelMessageContent): string {
  const body = (content as { readonly body?: unknown }).body;
  return typeof body === "string" ? body : channelContentJson(content);
}

/**
 * Proves `CHANNEL_OUTBOX_STATES` and the `channel_message_outbox` `CHECK`
 * name the same states in the same order. Runs at module load.
 */
function assertPersistedStatesMatchSchema(): void {
  const clause = /state TEXT NOT NULL CHECK \(state IN \(([^)]*)\)\)/u.exec(schemaDefinitions["table:channel_message_outbox"]);
  const declared = clause?.[1]?.split(",").map((entry) => entry.trim().replace(/^'|'$/gu, ""));
  if (declared === undefined || declared.length !== CHANNEL_OUTBOX_STATES.length || declared.some((state, index) => state !== CHANNEL_OUTBOX_STATES[index])) {
    throw new Error("channel-outbox.ts: CHANNEL_OUTBOX_STATES and the channel_message_outbox CHECK constraint disagree.");
  }
}

assertPersistedStatesMatchSchema();
