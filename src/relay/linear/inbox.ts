import { DatabaseSync } from "node:sqlite";

import { RelayError } from "../errors.js";
import { schemaFingerprintTable, schemaLedgerAgrees, schemaLedgerVersion, schemaObjectsMatch } from "../../shared/schema-ledger.js";
import { RETRY_BUDGET_EXHAUSTED_CODE, RETRY_BUDGET_MAX_ATTEMPTS, isRetryBudget, retryVerdict } from "../../shared/retry-budget.js";

/**
 * Persistent, encrypted-at-rest Linear webhook inbox.
 *
 * `/webhooks/linear` verifies, parses, persists the encrypted body here, answers 200, and processes
 * asynchronously. This store keeps one row per `Linear-Delivery` with the raw body only as ciphertext,
 * decides replay versus conflict, quarantines unsupported shapes, and hands out leases so two processes
 * never take the same delivery. It does not parse payloads, authorize, or talk to Linear.
 *
 * A second arrival under an existing delivery id is a replay only when `webhookId`, `eventName` and
 * `organizationId` match and either `bodyDigest` or `semanticDigest` (the body without `webhookTimestamp`,
 * which is how Linear's retries differ) matches. Anything else is `linear.ingress.delivery_conflict`
 * (409). A replay never rewrites the row, so the first payload stays authoritative.
 */

export const LINEAR_INBOX_CONTRACT_VERSION = "1.0.0" as const;

/** The failure code of a delivery quarantined as a shape Axis does not understand. Operator runbooks key on it. */
export const LINEAR_UNSUPPORTED_SHAPE_CODE = "linear.ingress.unsupported_shape";

/** The default lease a claim holds, in milliseconds. */
export const LINEAR_INBOX_LEASE_MS = 30_000;

/**
 * The failure code of a delivery that spent its retry budget, shared with `../../shared/retry-budget.ts`.
 * Only inbox give-ups carry it; the redispatch marker records its give-up as a `quarantined_at` stamp.
 */
export const LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE = RETRY_BUDGET_EXHAUSTED_CODE;

export type LinearInboxState = "accepted" | "processing" | "completed" | "quarantined" | "failed";
export type LinearInboxTerminalState = Extract<LinearInboxState, "completed" | "quarantined" | "failed">;
/** The append-only event log's vocabulary; mirrors the `event_type` CHECK below. */
export type LinearInboxEventType = "accepted" | LinearInboxTerminalState | "claimed" | "reclaimed";

export interface LinearInboxAcceptInput {
  /** `Linear-Delivery`. The primary key; a v4 UUID by the time ingress calls this. */
  readonly deliveryId: string;
  readonly webhookId: string;
  /** `Linear-Event`. Stored as supplied and never compared to the parsed body type. */
  readonly eventName: string;
  readonly bodyDigest: string;
  readonly semanticDigest: string;
  readonly organizationId: string | null;
  /** The raw body, already encrypted by the caller. Plaintext must never reach this module. */
  readonly payloadCiphertext: string;
  /**
   * Non-null exactly when the parser did not recognize the envelope. Such a delivery is persisted
   * directly in `quarantined`, in the transaction that accepts it.
   */
  readonly unsupportedShape: { readonly type: string; readonly action: string } | null;
  /**
   * The delivery's `AgentSession` id, or null for an envelope without one (`PermissionChange`, `OAuthApp`,
   * `Unsupported`). `claim` orders one session's deliveries FIFO by it.
   */
  readonly sessionKey: string | null;
  /** True for a stop (`agentActivity.signal === "stop"`), which `claim` lets jump ahead of its session's queue. */
  readonly preempts: boolean;
}

export interface LinearInboxAcceptance {
  readonly duplicate: boolean;
  readonly state: LinearInboxState;
}

export interface ClaimedLinearDelivery {
  readonly deliveryId: string;
  readonly webhookId: string;
  readonly eventName: string;
  readonly organizationId: string | null;
  readonly payloadCiphertext: string;
  readonly attempt: number;
  readonly leaseOwner: string;
  readonly leaseExpiresAt: string;
}

/**
 * What `claim` hands back: an attempt on the delivery, or the obligation to quarantine it because its
 * retry budget is spent.
 *
 * The ceiling is enforced here because `claim` is the only way to obtain an attempt, so even a caller
 * that crashes and is recycled by lease expiry cannot loop. The obligation is leased like an attempt and
 * the row stays `processing` until `quarantine` is called, so an ignored or abandoned obligation comes
 * back on the next claim. Quarantine ends the attempt, never the row: ciphertext and event log are kept.
 */
export type LinearInboxClaim =
  /** Work to do. `delivery.attempt` counts this attempt, and `remaining` is how many are left after it. */
  | { readonly disposition: "attempt"; readonly delivery: ClaimedLinearDelivery; readonly remaining: number }
  /**
   * No attempt is left. The caller must produce a user-visible result and then call
   * `quarantine(deliveryId, owner, LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE)`. `delivery.attempt` is the
   * count already spent; being told to give up is not itself an attempt.
   */
  | { readonly disposition: "quarantine"; readonly delivery: ClaimedLinearDelivery; readonly attempts: number; readonly maxAttempts: number };

/** The operator listing's page ceiling, matching the other `axis` listings' `--limit <1..100>`. */
export const LINEAR_INBOX_GIVE_UP_PAGE_LIMIT = 100;

/**
 * One recorded inbox give-up, and the operator acknowledgement that released its hold on host upgrade, if
 * any. Identity and disposition only: the table has no payload column. `acknowledgement` is null or
 * complete.
 */
export interface LinearInboxGiveUpDisposition {
  readonly sequence: number;
  readonly deliveryId: string;
  readonly attempts: number;
  readonly failureCode: typeof LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE;
  readonly givenUpAt: string;
  readonly acknowledgement: LinearInboxGiveUpAcknowledgement | null;
}

/** Who released the upgrade gate, when, and under what authority. `reason` is optional and persisted verbatim. */
export interface LinearInboxGiveUpAcknowledgement {
  readonly acknowledgedAt: string;
  readonly operatorRef: string;
  readonly authorizationRef: string;
  readonly source: string;
  readonly requestRef: string;
  readonly reason: string | null;
}

/** What `acknowledgeGiveUp` was told to record. Every field but `reason` is required, because this action opens an upgrade gate. */
export interface LinearInboxGiveUpAcknowledgementInput {
  readonly deliveryId: string;
  readonly operatorRef: string;
  readonly authorizationRef: string;
  readonly source: string;
  readonly requestRef: string;
  readonly reason?: string;
}

/**
 * `applied` is false when an earlier call already wrote the acknowledgement, so a retried command is
 * idempotent. `disposition.acknowledgement` is then the original one, which the schema refuses to overwrite.
 */
export interface LinearInboxGiveUpAcknowledgementOutcome {
  readonly applied: boolean;
  readonly disposition: LinearInboxGiveUpDisposition;
}

export interface LinearInboxDeliverySnapshot {
  readonly deliveryId: string;
  readonly webhookId: string;
  readonly eventName: string;
  readonly organizationId: string | null;
  readonly state: LinearInboxState;
  readonly attempt: number;
  readonly failureCode: string | null;
  readonly leaseOwner: string | null;
  readonly leaseExpiresAt: string | null;
}

/** The acceptance half, kept separate so the HTTP ingress cannot reach a processing method. */
export interface LinearInboxWriter {
  accept(input: LinearInboxAcceptInput): LinearInboxAcceptance;
}

/** The surface the processing loop consumes: acceptance plus the claim and terminal lifecycle. */
export interface LinearInboxPort extends LinearInboxWriter {
  /**
   * Takes the oldest claimable delivery under a `leaseMs` lease, or returns null. Claimable means
   * `accepted`, or `processing` with an expired lease, so a delivery survives a crash mid-process. A
   * delivery that has spent its retry budget comes back as a quarantine obligation (`LinearInboxClaim`).
   */
  claim(owner: string, leaseMs?: number): LinearInboxClaim | null;
  complete(deliveryId: string, owner: string): void;
  quarantine(deliveryId: string, owner: string, failureCode: string): void;
  fail(deliveryId: string, owner: string, failureCode: string): void;
  snapshot(deliveryId: string): LinearInboxDeliverySnapshot | null;
  /**
   * The non-stop deliveries of `sessionKey` that are `accepted` (or `processing` with an expired lease)
   * and arrived before the stop `deliveryId`. Later arrivals are left to run after the stop.
   *
   * Read-only and separate from `supersedeQueued` so the caller can announce each superseded delivery
   * before terminating it.
   */
  queuedAheadForSession(sessionKey: string, deliveryId: string): readonly string[];
  /**
   * Quarantines, in one transaction, every listed delivery still `accepted` or `processing` with an
   * expired lease. A delivery whose state already moved on is skipped, not refused. Returns the ids
   * actually superseded.
   */
  supersedeQueued(deliveryIds: readonly string[], owner: string, failureCode: string): readonly string[];
  ready(): boolean;
  close(): void;
}

export interface SqliteLinearInboxOptions {
  /** Omitted for an in-memory database; composition supplies the configured channel database. */
  readonly filename?: string;
  readonly now?: () => Date;
  readonly busyTimeoutMs?: number;
  /**
   * Attempts a delivery gets before `claim` hands out the quarantine obligation instead. Defaults to
   * `RETRY_BUDGET_MAX_ATTEMPTS`.
   */
  readonly maxAttempts?: number;
}

function conflict(): RelayError {
  return new RelayError("linear.ingress.delivery_conflict", "The delivery identity conflicts with a prior payload.", 409);
}

function unavailable(message: string): RelayError {
  return new RelayError("linear.ingress.storage_unavailable", message, 503, true);
}

/**
 * Several guards share `linear.ingress.storage_unavailable`; these messages tell a caller which one fired,
 * so they are contract.
 */
export const LINEAR_INBOX_UNLEDGERED_MESSAGE = "The Linear inbox database holds tables with no schema ledger.";
export const LINEAR_INBOX_SCHEMA_DRIFT_MESSAGE = "The Linear inbox schema does not match this build.";

function invalidTransition(): RelayError {
  return new RelayError("axis.relay.invalid_transition", "The delivery lease is no longer active.", 409);
}

/**
 * SQL fragment: `earlier` arrived before `later` (`accepted_at`, then `delivery_id` as the tie-break).
 * `queuedAheadForSession` uses this alone, because a stop supersedes only what arrived before it.
 */
function arrivedBefore(earlier: string, later: string): string {
  return `(${earlier}.accepted_at < ${later}.accepted_at OR (${earlier}.accepted_at = ${later}.accepted_at AND ${earlier}.delivery_id < ${later}.delivery_id))`;
}

/**
 * SQL fragment: `winner` outranks `loser` in one session's claim order. A `preempts` row (a stop) always
 * wins; otherwise arrival order decides. Both arguments are table aliases chosen by the caller, never
 * row data, so interpolation is safe.
 */
function outranks(winner: string, loser: string): string {
  return `(${winner}.preempts > ${loser}.preempts OR (${winner}.preempts = ${loser}.preempts AND ${arrivedBefore(winner, loser)}))`;
}

/**
 * Bounds attacker-influenced strings recorded off the wire (an unrecognized envelope's `type`/`action`,
 * the `Linear-Event` header) so they cannot grow the database without limit.
 */
function boundedShapeText(value: string): string {
  return value.length <= 128 ? value : `${value.slice(0, 125)}...`;
}

const schemaDefinitions = Object.freeze({
  "table:linear_inbox_schema": "CREATE TABLE linear_inbox_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
  "table:linear_inbox_deliveries": "CREATE TABLE linear_inbox_deliveries(delivery_id TEXT PRIMARY KEY, webhook_id TEXT NOT NULL, event_name TEXT NOT NULL, body_digest TEXT NOT NULL, semantic_digest TEXT NOT NULL, organization_id TEXT, payload_ciphertext TEXT NOT NULL, contract_version TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('accepted', 'processing', 'completed', 'quarantined', 'failed')), attempt_count INTEGER NOT NULL, lease_owner TEXT, lease_expires_at TEXT, failure_code TEXT, accepted_at TEXT NOT NULL, completed_at TEXT, session_key TEXT, preempts INTEGER NOT NULL DEFAULT 0, CHECK((state = 'processing') = (lease_owner IS NOT NULL)), CHECK((state = 'processing') = (lease_expires_at IS NOT NULL)), CHECK((state IN ('completed', 'quarantined', 'failed')) = (completed_at IS NOT NULL)))",
  "table:linear_inbox_events": "CREATE TABLE linear_inbox_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL, event_type TEXT NOT NULL CHECK(event_type IN ('accepted', 'quarantined', 'claimed', 'reclaimed', 'completed', 'failed')), detail_json TEXT, recorded_at TEXT NOT NULL, FOREIGN KEY(delivery_id) REFERENCES linear_inbox_deliveries(delivery_id))",
  // Where an inbox give-up is recorded; `linear_inbox_deliveries.state` is where it is enforced. A
  // separate table so an operator acknowledgement can clear the give-up's hold on host upgrade without
  // replacing the `quarantined` state, and so the record does not depend on the delivery row (hence no
  // foreign key and no payload column). The acknowledgement columns are all-or-nothing by CHECK, except
  // the optional `acknowledged_reason`.
  "table:linear_inbox_give_up_dispositions": "CREATE TABLE linear_inbox_give_up_dispositions(sequence INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL UNIQUE, attempts INTEGER NOT NULL, failure_code TEXT NOT NULL, given_up_at TEXT NOT NULL, acknowledged_at TEXT, acknowledged_by TEXT, acknowledged_authority TEXT, acknowledged_source TEXT, acknowledged_request TEXT, acknowledged_reason TEXT, CHECK(attempts > 0), CHECK((acknowledged_at IS NULL) = (acknowledged_by IS NULL)), CHECK((acknowledged_at IS NULL) = (acknowledged_authority IS NULL)), CHECK((acknowledged_at IS NULL) = (acknowledged_source IS NULL)), CHECK((acknowledged_at IS NULL) = (acknowledged_request IS NULL)), CHECK(acknowledged_reason IS NULL OR acknowledged_at IS NOT NULL))",
  "index:linear_inbox_deliveries_claimable": "CREATE INDEX linear_inbox_deliveries_claimable ON linear_inbox_deliveries(state, accepted_at)",
  // Serves `claim`'s per-session outrank guard and `queuedAheadForSession`.
  "index:linear_inbox_deliveries_session_order": "CREATE INDEX linear_inbox_deliveries_session_order ON linear_inbox_deliveries(session_key, state, preempts, accepted_at, delivery_id)",
  "trigger:linear_inbox_events_immutable_update": "CREATE TRIGGER linear_inbox_events_immutable_update BEFORE UPDATE ON linear_inbox_events BEGIN SELECT RAISE(ABORT, 'linear inbox events are immutable'); END",
  "trigger:linear_inbox_events_immutable_delete": "CREATE TRIGGER linear_inbox_events_immutable_delete BEFORE DELETE ON linear_inbox_events BEGIN SELECT RAISE(ABORT, 'linear inbox events are immutable'); END",
  // A disposition row is written at most twice: by `quarantine`, then once by `acknowledgeGiveUp`. Its
  // identity never changes, and an acknowledged row cannot be updated again.
  "trigger:linear_inbox_give_up_dispositions_immutable_identity": "CREATE TRIGGER linear_inbox_give_up_dispositions_immutable_identity BEFORE UPDATE ON linear_inbox_give_up_dispositions WHEN OLD.sequence IS NOT NEW.sequence OR OLD.delivery_id IS NOT NEW.delivery_id OR OLD.attempts IS NOT NEW.attempts OR OLD.failure_code IS NOT NEW.failure_code OR OLD.given_up_at IS NOT NEW.given_up_at OR OLD.acknowledged_at IS NOT NULL BEGIN SELECT RAISE(ABORT, 'linear inbox give-up disposition identity is immutable and its acknowledgement is one-shot'); END",
  "trigger:linear_inbox_give_up_dispositions_immutable_delete": "CREATE TRIGGER linear_inbox_give_up_dispositions_immutable_delete BEFORE DELETE ON linear_inbox_give_up_dispositions BEGIN SELECT RAISE(ABORT, 'linear inbox give-up dispositions are append-only'); END",
});

const schemaFingerprints = schemaFingerprintTable(schemaDefinitions);

function assertCurrentSchema(database: DatabaseSync): void {
  try {
    const objects = database.prepare("SELECT type, name, sql FROM sqlite_master WHERE (name LIKE 'linear_inbox%' OR tbl_name LIKE 'linear_inbox%') AND name NOT GLOB 'sqlite_*'").all() as Row[];
    // The throwing normalizer is safe here: every caller converts or catches its errors.
    if (!schemaObjectsMatch(objects, schemaFingerprints)) throw unavailable(LINEAR_INBOX_SCHEMA_DRIFT_MESSAGE);
    // Single-current-version check; see `schemaLedgerAgrees`.
    const ledger = database.prepare("SELECT version, applied_at FROM linear_inbox_schema").all() as Row[];
    if (!schemaLedgerAgrees(ledger, LINEAR_INBOX_SCHEMA_VERSION)) throw unavailable("The Linear inbox schema ledger disagrees with its objects.");
  } catch (error) {
    if (error instanceof RelayError) throw error;
    throw unavailable(LINEAR_INBOX_SCHEMA_DRIFT_MESSAGE);
  }
}

const LINEAR_INBOX_SCHEMA_VERSION = 1;

type Row = Record<string, unknown>;

/** The disposition columns, shared by every disposition reader. None of them is on `linear_inbox_deliveries`. */
const GIVE_UP_DISPOSITION_COLUMNS = "sequence, delivery_id, attempts, failure_code, given_up_at, acknowledged_at, acknowledged_by, acknowledged_authority, acknowledged_source, acknowledged_request, acknowledged_reason";

export const LINEAR_INBOX_GIVE_UP_CODE_MESSAGE = "The Linear inbox give-up disposition holds a failure code this build never writes.";
export const LINEAR_INBOX_GIVE_UP_WITHOUT_ATTEMPT_MESSAGE = "The Linear inbox cannot record a give-up on a delivery with no attempts.";
export const LINEAR_INBOX_ACKNOWLEDGEMENT_REASON_MESSAGE = "The Linear inbox give-up acknowledgement reason must be printable text of at most 512 characters.";
export const LINEAR_INBOX_GIVE_UP_QUERY_MESSAGE = "The Linear inbox give-up query is invalid.";
export const LINEAR_INBOX_NOT_A_CHANNEL_DATABASE_MESSAGE = "The file is not a Linear channel database.";

/** How long an operator's acknowledgement reason may be. Far past any real one, and bounded because the column is append-only. */
export const LINEAR_INBOX_ACKNOWLEDGEMENT_REASON_LIMIT = 512;
export const LINEAR_INBOX_ACKNOWLEDGEMENT_AUDIT_MESSAGE = "The Linear inbox give-up acknowledgement requires an opaque operator, authority, source and request reference.";

/** The opaque-reference shape `mutationAudit` (`../../axis-cli.ts`) enforces, restated because the store is the durable boundary. */
const LINEAR_INBOX_AUDIT_REF = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;

/**
 * Validates the operator reason in the store, not only at the CLI, because the column is append-only.
 * Refused rather than truncated: a silently cut audit reason is worse than an error. Control characters
 * are refused because the text is read back in a terminal.
 */
function acknowledgementReason(value: string | undefined): string | null {
  if (value === undefined) return null;
  // eslint-disable-next-line no-control-regex
  if (value.length === 0 || value.length > LINEAR_INBOX_ACKNOWLEDGEMENT_REASON_LIMIT || /[\u0000-\u001F\u007F]/u.test(value)) {
    throw new RelayError("linear.ingress.storage_unavailable", LINEAR_INBOX_ACKNOWLEDGEMENT_REASON_MESSAGE, 400, false);
  }
  return value;
}

/**
 * Narrows a persisted give-up code to the only one this build writes. Not retryable: a bad row reads the
 * same on every attempt.
 */
function attemptsExhaustedCode(value: unknown): typeof LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE {
  if (value !== LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE) {
    throw new RelayError("linear.ingress.storage_unavailable", LINEAR_INBOX_GIVE_UP_CODE_MESSAGE, 503, false);
  }
  return LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE;
}

/** The one projection from a disposition row, so both readers report the same shape and neither can widen it alone. */
function giveUpDisposition(row: Row): LinearInboxGiveUpDisposition {
  const acknowledgedAt = row.acknowledged_at;
  return Object.freeze({
    sequence: Number(row.sequence),
    deliveryId: String(row.delivery_id),
    attempts: Number(row.attempts),
    failureCode: attemptsExhaustedCode(row.failure_code),
    givenUpAt: String(row.given_up_at),
    acknowledgement: acknowledgedAt === null || acknowledgedAt === undefined ? null : Object.freeze({
      acknowledgedAt: String(acknowledgedAt),
      operatorRef: String(row.acknowledged_by),
      authorizationRef: String(row.acknowledged_authority),
      source: String(row.acknowledged_source),
      requestRef: String(row.acknowledged_request),
      reason: row.acknowledged_reason === null || row.acknowledged_reason === undefined ? null : String(row.acknowledged_reason),
    }),
  });
}


export class SqliteLinearInbox implements LinearInboxPort {
  readonly #db: DatabaseSync;
  readonly #now: () => Date;
  readonly #maxAttempts: number;
  #closed = false;
  #inTransaction = false;

  constructor(options: SqliteLinearInboxOptions = {}) {
    const busyTimeoutMs = options.busyTimeoutMs ?? 5_000;
    // Validated before it is interpolated into SQL.
    if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0) throw unavailable("The Linear inbox busy timeout must be a non-negative integer.");
    const maxAttempts = options.maxAttempts ?? RETRY_BUDGET_MAX_ATTEMPTS;
    // Refused rather than clamped: a ceiling of 0 would quarantine every delivery on its first claim.
    if (!isRetryBudget(maxAttempts)) throw unavailable("The Linear inbox attempt ceiling must be a positive integer.");
    this.#maxAttempts = maxAttempts;
    this.#now = options.now ?? (() => new Date());
    this.#db = new DatabaseSync(options.filename ?? ":memory:");
    try {
      this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;");
      this.#db.exec(`PRAGMA busy_timeout=${busyTimeoutMs};`);
      this.#initializeSchema();
      this.#assertInvariants();
    } catch (error) {
      try { this.#db.close(); this.#closed = true; } catch { /* preserve the construction failure */ }
      throw error;
    }
  }

  close(): void {
    if (!this.#closed) { this.#db.close(); this.#closed = true; }
  }

  /**
   * The full health check: the schema sweep plus the whole-database `integrity_check` and
   * `foreign_key_check` scans. Used by the operator diagnostics path, not `/health/ready`, because its
   * cost grows with the data. Readiness uses `readyForTraffic()`.
   */
  ready(): boolean {
    try { this.#assertOpen(); this.#assertInvariants(); return true; } catch { return false; }
  }

  /**
   * Whether this store can serve traffic, at a cost independent of its data size: the `sqlite_master`
   * fingerprint sweep plus the schema ledger. Catches a closed handle, dropped or edited objects, a
   * disagreeing ledger and page-1 damage, but not corruption confined to data pages or orphaned
   * references, which `ready()` covers.
   */
  readyForTraffic(): boolean {
    try { this.#assertOpen(); this.#assertSchemaInvariants(); return true; } catch { return false; }
  }

  /**
   * Persists one delivery, or recognizes it as a replay of one already held.
   *
   * An unsupported delivery is inserted directly in `quarantined` in this transaction, so it is never
   * claimable and can never produce a command.
   */
  accept(input: LinearInboxAcceptInput): LinearInboxAcceptance {
    this.#assertOpen();
    return this.#transaction(() => {
      const existing = this.#db.prepare(
        `SELECT delivery_id, webhook_id, event_name, body_digest, semantic_digest, organization_id, state
         FROM linear_inbox_deliveries WHERE delivery_id = ?`,
      ).get(input.deliveryId) as Row | undefined;
      if (existing !== undefined) {
        // Exact byte replay, or Linear's own retry of the same logical event
        // with only `webhookTimestamp` moved. Anything else -- a changed
        // subscription, event name, organization, or body -- is a different
        // event wearing an already-used delivery id.
        const exactBody = existing.body_digest === input.bodyDigest;
        const semanticRetry = existing.semantic_digest === input.semanticDigest;
        if (
          existing.webhook_id !== input.webhookId
          || existing.event_name !== input.eventName
          || existing.organization_id !== input.organizationId
          || (!exactBody && !semanticRetry)
        ) throw conflict();
        // Deliberately no UPDATE: the first payload stays authoritative, so a
        // replay can never swap the ciphertext a claim is about to decrypt.
        return { duplicate: true, state: this.#stateOf(existing) };
      }
      const unsupported = input.unsupportedShape !== null;
      const state: LinearInboxState = unsupported ? "quarantined" : "accepted";
      const instant = this.#instant();
      this.#db.prepare(`
        INSERT INTO linear_inbox_deliveries(
          delivery_id, webhook_id, event_name, body_digest, semantic_digest, organization_id,
          payload_ciphertext, contract_version, state, attempt_count, lease_owner, lease_expires_at,
          failure_code, accepted_at, completed_at, session_key, preempts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?, ?, ?)
      `).run(
        input.deliveryId,
        input.webhookId,
        // The unsigned `Linear-Event` header is bounded like every other off-the-wire string.
        boundedShapeText(input.eventName),
        input.bodyDigest,
        input.semanticDigest,
        input.organizationId,
        input.payloadCiphertext,
        LINEAR_INBOX_CONTRACT_VERSION,
        state,
        unsupported ? LINEAR_UNSUPPORTED_SHAPE_CODE : null,
        instant,
        unsupported ? instant : null,
        input.sessionKey,
        input.preempts ? 1 : 0,
      );
      this.#record(input.deliveryId, "accepted", { event: boundedShapeText(input.eventName) });
      if (input.unsupportedShape !== null) {
        this.#record(input.deliveryId, "quarantined", {
          code: LINEAR_UNSUPPORTED_SHAPE_CODE,
          type: boundedShapeText(input.unsupportedShape.type),
          action: boundedShapeText(input.unsupportedShape.action),
        });
      }
      return { duplicate: false, state };
    });
  }

  claim(owner: string, leaseMs = LINEAR_INBOX_LEASE_MS): LinearInboxClaim | null {
    this.#assertOpen();
    if (owner.length === 0) throw unavailable("A delivery lease requires an owner.");
    // Leases are compared as ISO-8601 strings, which order correctly only while `toISOString()` stays
    // fixed-width (years 0-9999), so a lease is capped at a century.
    if (!Number.isSafeInteger(leaseMs) || leaseMs < 0 || leaseMs > 3_155_760_000_000) {
      throw unavailable("The Linear inbox lease duration is out of range.");
    }
    return this.#transaction(() => {
      const now = this.#instant();
      // Per-session FIFO, enforced on the SELECT: `d` is claimable and no unterminated sibling `e` in its
      // session outranks it (a stop outranks a non-stop; otherwise earlier arrival wins). `NOT EXISTS`,
      // never `NOT IN`, which fails open on a NULL. A NULL `session_key` neither blocks nor is blocked.
      // The UPDATE needs no copy of this guard: both run in one `BEGIN IMMEDIATE` transaction. Across
      // sessions the oldest claimable row still wins.
      const row = this.#db.prepare(`
        SELECT d.delivery_id AS delivery_id, d.webhook_id AS webhook_id, d.event_name AS event_name,
               d.organization_id AS organization_id, d.payload_ciphertext AS payload_ciphertext,
               d.attempt_count AS attempt_count, d.state AS state
        FROM linear_inbox_deliveries d
        WHERE (d.state = 'accepted' OR (d.state = 'processing' AND d.lease_expires_at < ?))
          AND NOT EXISTS (
            SELECT 1 FROM linear_inbox_deliveries e
            WHERE e.session_key IS NOT NULL AND d.session_key IS NOT NULL AND e.session_key = d.session_key
              AND e.state IN ('accepted', 'processing')
              AND ${outranks("e", "d")}
          )
        ORDER BY d.accepted_at ASC, d.delivery_id ASC LIMIT 1
      `).get(now) as Row | undefined;
      if (row === undefined) return null;
      const deliveryId = String(row.delivery_id);
      const reclaimed = row.state === "processing";
      const expiresAt = new Date(this.#now().getTime() + leaseMs).toISOString();
      // Every claim spends an attempt, including a reclaim after a crashed worker, so a poison-pill
      // delivery cannot live forever. The cost: a delivery in flight across enough unrelated restarts is
      // also quarantined (retained, and reported to the user as a give-up).
      const verdict = retryVerdict(Number(row.attempt_count), this.#maxAttempts);
      // The WHERE repeats the selection predicate as a second guard. A quarantine hand-back takes the
      // same owner-scoped lease as an attempt but does not increment the count.
      const changed = this.#db.prepare(`
        UPDATE linear_inbox_deliveries
        SET state = 'processing', lease_owner = ?, lease_expires_at = ?, attempt_count = attempt_count + ?
        WHERE delivery_id = ? AND (state = 'accepted' OR (state = 'processing' AND lease_expires_at < ?))
      `).run(owner, expiresAt, verdict.disposition === "attempt" ? 1 : 0, deliveryId, now).changes;
      if (changed !== 1) return null;
      // Recorded as `claimed`/`reclaimed`: the lease really was taken, and the later `quarantined` event
      // carries the give-up.
      this.#record(deliveryId, reclaimed ? "reclaimed" : "claimed", { owner: boundedShapeText(owner) });
      const delivery: ClaimedLinearDelivery = Object.freeze({
        deliveryId,
        webhookId: String(row.webhook_id),
        eventName: String(row.event_name),
        organizationId: row.organization_id === null ? null : String(row.organization_id),
        payloadCiphertext: String(row.payload_ciphertext),
        attempt: Number(row.attempt_count) + (verdict.disposition === "attempt" ? 1 : 0),
        leaseOwner: owner,
        leaseExpiresAt: expiresAt,
      });
      return verdict.disposition === "attempt"
        ? Object.freeze({ disposition: "attempt" as const, delivery, remaining: verdict.remaining })
        : Object.freeze({ disposition: "quarantine" as const, delivery, attempts: verdict.attempts, maxAttempts: verdict.maxAttempts });
    });
  }

  /**
   * See `LinearInboxPort#queuedAheadForSession`. Uses `arrivedBefore`, not `outranks`, because
   * `outranks` would also match siblings that arrived after the stop. Another stop is excluded: `claim`
   * never lets it ahead of an earlier stop, so it is handled in its own turn.
   */
  queuedAheadForSession(sessionKey: string, deliveryId: string): readonly string[] {
    this.#assertOpen();
    const now = this.#instant();
    const rows = this.#db.prepare(`
      SELECT candidate.delivery_id AS delivery_id
      FROM linear_inbox_deliveries candidate, linear_inbox_deliveries stopRow
      WHERE stopRow.delivery_id = ? AND stopRow.session_key = ?
        AND candidate.session_key = stopRow.session_key
        AND candidate.delivery_id != stopRow.delivery_id
        AND candidate.preempts = 0
        AND (candidate.state = 'accepted' OR (candidate.state = 'processing' AND candidate.lease_expires_at < ?))
        AND ${arrivedBefore("candidate", "stopRow")}
      ORDER BY candidate.accepted_at ASC, candidate.delivery_id ASC
    `).all(deliveryId, sessionKey, now) as Row[];
    return Object.freeze(rows.map((row) => String(row.delivery_id)));
  }

  /**
   * See `LinearInboxPort#supersedeQueued`. Not `#finish` per row, because `#finish` requires the lease
   * owner and a queued row has usually never been claimed. `owner` is recorded on each row's event.
   */
  supersedeQueued(deliveryIds: readonly string[], owner: string, failureCode: string): readonly string[] {
    this.#assertOpen();
    if (owner.length === 0) throw unavailable("A superseded delivery requires an owner.");
    if (deliveryIds.length === 0) return Object.freeze([]);
    return this.#transaction(() => {
      const now = this.#instant();
      const superseded: string[] = [];
      for (const deliveryId of deliveryIds) {
        const changed = this.#db.prepare(`
          UPDATE linear_inbox_deliveries
          SET state = 'quarantined', failure_code = ?, completed_at = ?, lease_owner = NULL, lease_expires_at = NULL
          WHERE delivery_id = ? AND (state = 'accepted' OR (state = 'processing' AND lease_expires_at < ?))
        `).run(failureCode, now, deliveryId, now).changes;
        // A row that moved on (claimed by a live owner, or terminal) is skipped.
        if (changed === 1) {
          this.#record(deliveryId, "quarantined", { code: boundedShapeText(failureCode), reason: boundedShapeText(`superseded by ${owner}`) });
          superseded.push(deliveryId);
        }
      }
      return Object.freeze(superseded);
    });
  }

  complete(deliveryId: string, owner: string): void {
    this.#finish(deliveryId, owner, "completed", null);
  }

  /**
   * Terminal-quarantines a delivery under its lease. For the retry-exhaustion code it also records the
   * give-up disposition in the same transaction, with `given_up_at` read from the row's `completed_at`.
   * Other codes (unsupported shapes, channel refusals) are not give-ups and write no disposition, the
   * same line `assertLifecycleDatabaseDrained` draws.
   */
  quarantine(deliveryId: string, owner: string, failureCode: string): void {
    // Before the transaction, so a closed store refuses with the generic storage message.
    this.#assertOpen();
    this.#transaction(() => {
      this.#finish(deliveryId, owner, "quarantined", failureCode);
      if (failureCode !== LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE) return;
      const row = this.#db.prepare("SELECT attempt_count, completed_at FROM linear_inbox_deliveries WHERE delivery_id = ?").get(deliveryId) as Row | undefined;
      const attempts = row === undefined ? 0 : Number(row.attempt_count);
      // A give-up at zero attempts means a caller quarantined an unattempted delivery as exhausted. Refused
      // rather than recorded.
      if (!Number.isSafeInteger(attempts) || attempts < 1) {
        throw new RelayError("linear.ingress.storage_unavailable", LINEAR_INBOX_GIVE_UP_WITHOUT_ATTEMPT_MESSAGE, 503, false);
      }
      this.#db.prepare("INSERT INTO linear_inbox_give_up_dispositions(delivery_id, attempts, failure_code, given_up_at) VALUES (?, ?, ?, ?)")
        .run(deliveryId, attempts, LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE, String(row?.completed_at));
    });
  }

  /**
   * Records an operator acknowledgement of one give-up, releasing its hold on host upgrade without
   * touching the delivery row. Idempotent: a second call reports `applied: false` with the original
   * acknowledgement, which the schema's one-shot trigger protects. Not on `LinearInboxPort`, because it is
   * an operator action rather than a processing step.
   *
   * Returns null when the delivery has no recorded give-up.
   */
  acknowledgeGiveUp(input: LinearInboxGiveUpAcknowledgementInput): LinearInboxGiveUpAcknowledgementOutcome | null {
    this.#assertOpen();
    // Validated before the transaction, and here rather than only in the CLI: the row is append-only, so
    // an empty operator or authority would be permanent.
    for (const [field, value] of [["operatorRef", input.operatorRef], ["authorizationRef", input.authorizationRef], ["source", input.source], ["requestRef", input.requestRef]] as const) {
      if (typeof value !== "string" || !LINEAR_INBOX_AUDIT_REF.test(value)) {
        throw new RelayError("linear.ingress.storage_unavailable", `${LINEAR_INBOX_ACKNOWLEDGEMENT_AUDIT_MESSAGE} (${field})`, 400, false);
      }
    }
    const reason = acknowledgementReason(input.reason);
    return this.#transaction(() => {
      const before = this.#db.prepare(`SELECT ${GIVE_UP_DISPOSITION_COLUMNS} FROM linear_inbox_give_up_dispositions WHERE delivery_id = ?`).get(input.deliveryId) as Row | undefined;
      if (before === undefined) return null;
      const recorded = giveUpDisposition(before);
      if (recorded.acknowledgement !== null) return Object.freeze({ applied: false, disposition: recorded });
      const changed = this.#db.prepare(`
        UPDATE linear_inbox_give_up_dispositions
        SET acknowledged_at = ?, acknowledged_by = ?, acknowledged_authority = ?, acknowledged_source = ?, acknowledged_request = ?, acknowledged_reason = ?
        WHERE delivery_id = ? AND acknowledged_at IS NULL
      `).run(this.#instant(), input.operatorRef, input.authorizationRef, input.source, input.requestRef, reason, input.deliveryId).changes;
      if (changed !== 1) throw unavailable("The Linear inbox give-up disposition could not be acknowledged.");
      // Read back through the shared projection, so the caller sees what the database now holds.
      const after = this.#db.prepare(`SELECT ${GIVE_UP_DISPOSITION_COLUMNS} FROM linear_inbox_give_up_dispositions WHERE delivery_id = ?`).get(input.deliveryId) as Row | undefined;
      if (after === undefined) throw unavailable("The Linear inbox give-up disposition could not be acknowledged.");
      return Object.freeze({ applied: true, disposition: giveUpDisposition(after) });
    });
  }

  fail(deliveryId: string, owner: string, failureCode: string): void {
    this.#finish(deliveryId, owner, "failed", failureCode);
  }

  snapshot(deliveryId: string): LinearInboxDeliverySnapshot | null {
    this.#assertOpen();
    const row = this.#db.prepare(`
      SELECT delivery_id, webhook_id, event_name, organization_id, state, attempt_count, failure_code, lease_owner, lease_expires_at
      FROM linear_inbox_deliveries WHERE delivery_id = ?
    `).get(deliveryId) as Row | undefined;
    if (row === undefined) return null;
    return Object.freeze({
      deliveryId: String(row.delivery_id),
      webhookId: String(row.webhook_id),
      eventName: String(row.event_name),
      organizationId: row.organization_id === null ? null : String(row.organization_id),
      state: this.#stateOf(row),
      attempt: Number(row.attempt_count),
      failureCode: row.failure_code === null ? null : String(row.failure_code),
      leaseOwner: row.lease_owner === null ? null : String(row.lease_owner),
      leaseExpiresAt: row.lease_expires_at === null ? null : String(row.lease_expires_at),
    });
  }

  /**
   * A terminal transition requires the lease owner, not just `processing`: otherwise an owner whose lease
   * expired could write its outcome onto another owner's reclaimed delivery. The loser gets
   * `axis.relay.invalid_transition`.
   */
  #finish(deliveryId: string, owner: string, state: LinearInboxTerminalState, failureCode: string | null): void {
    this.#assertOpen();
    // The state change and its event share a transaction, so a delivery is never terminal without its event.
    this.#transaction(() => {
    const changed = this.#db.prepare(`
      UPDATE linear_inbox_deliveries
      SET state = ?, failure_code = ?, completed_at = ?, lease_owner = NULL, lease_expires_at = NULL
      WHERE delivery_id = ? AND state = 'processing' AND lease_owner = ?
    `).run(state, failureCode, this.#instant(), deliveryId, owner).changes;
    if (changed !== 1) throw invalidTransition();
    this.#record(deliveryId, state, failureCode === null ? null : { code: boundedShapeText(failureCode) });
    });
  }

  #record(deliveryId: string, eventType: LinearInboxEventType, detail: Readonly<Record<string, string>> | null): void {
    this.#db.prepare("INSERT INTO linear_inbox_events(delivery_id, event_type, detail_json, recorded_at) VALUES (?, ?, ?, ?)")
      .run(deliveryId, eventType, detail === null ? null : JSON.stringify(detail), this.#instant());
  }

  #stateOf(row: Row): LinearInboxState {
    const state = row.state;
    if (state !== "accepted" && state !== "processing" && state !== "completed" && state !== "quarantined" && state !== "failed") {
      // Not retryable: a corrupt state reads the same on every attempt.
      throw new RelayError("linear.ingress.storage_unavailable", "The webhook could not be accepted durably.", 503, false);
    }
    return state;
  }

  #instant(): string {
    return this.#now().toISOString();
  }

  /** Creates the schema on a database with no Linear inbox objects, and refuses one at any other version. */
  #initializeSchema(): void {
    const version = this.#recordedSchemaVersion();
    if (version === null) {
      this.#transaction(() => {
        for (const definition of Object.values(schemaDefinitions)) this.#db.exec(definition);
        this.#recordSchemaVersion(LINEAR_INBOX_SCHEMA_VERSION);
      });
      return;
    }
    if (version !== LINEAR_INBOX_SCHEMA_VERSION) {
      throw new RelayError("axis.relay.schema_version_unsupported", "The Linear inbox database does not match the current schema version.", 500);
    }
  }

  /** The recorded schema version, or null when this database holds no Linear inbox objects at all. */
  #recordedSchemaVersion(): number | null {
    const existing = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE name LIKE 'linear_inbox%' LIMIT 1").get();
    if (existing === undefined) return null;
    const ledger = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'linear_inbox_schema'").get();
    // Objects without a ledger are refused rather than given a guessed version.
    if (ledger === undefined) throw unavailable(LINEAR_INBOX_UNLEDGERED_MESSAGE);
    const version = schemaLedgerVersion(this.#db.prepare("SELECT version FROM linear_inbox_schema").all() as Row[]);
    if (version === null) throw unavailable("The Linear inbox database's schema ledger is unreadable.");
    return version;
  }

  #recordSchemaVersion(version: number): void {
    this.#db.prepare("INSERT INTO linear_inbox_schema(version, applied_at) VALUES (?, ?)").run(version, this.#instant());
  }

  /**
   * Uses the generic storage message: `safeError` puts a `RelayError`'s message into the webhook response
   * body, so it must not name internal state.
   */
  #assertOpen(): void {
    if (this.#closed) throw unavailable("The webhook could not be accepted durably.");
  }

  #assertInvariants(): void {
    // Converts any non-`RelayError` driver failure, so no SQLite message escapes.
    try { this.#assertInvariantsUnsafe(); }
    catch (error) {
      if (error instanceof RelayError) throw error;
      throw unavailable("The Linear inbox database could not be verified.");
    }
  }

  #assertInvariantsUnsafe(): void {
    this.#assertScanInvariants();
    this.#assertSchemaInvariants();
  }

  /** The two whole-database scans. Run at construction and on the diagnostics path, never on `/health/ready`. */
  #assertScanInvariants(): void {
    const integrity = this.#db.prepare("PRAGMA integrity_check").get() as { integrity_check?: string } | undefined;
    if (integrity?.integrity_check !== "ok") throw unavailable("The Linear inbox database failed its integrity check.");
    if (this.#db.prepare("PRAGMA foreign_key_check").all().length !== 0) throw unavailable("The Linear inbox database holds an orphaned reference.");
  }

  /**
   * The schema half: `sqlite_master` plus the schema ledger. Both are
   * bounded by the number of schema objects, not by the rows they describe,
   * which is what makes `readyForTraffic()` safe to serve unauthenticated.
   */
  #assertSchemaInvariants(): void {
    assertCurrentSchema(this.#db);
  }

  /**
   * `BEGIN IMMEDIATE` takes the write lock up front, so two connections cannot both claim a delivery.
   * Every failure, including `SQLITE_BUSY` at begin, surfaces as a `RelayError`.
   */
  #transaction<T>(operation: () => T): T {
    if (this.#inTransaction) return operation();
    try {
      this.#db.exec("BEGIN IMMEDIATE");
    } catch {
      throw unavailable("The Linear inbox could not begin a transaction.");
    }
    this.#inTransaction = true;
    try {
      const result = operation();
      this.#db.exec("COMMIT");
      return result;
    } catch (error) {
      try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ }
      if (error instanceof RelayError) throw error;
      throw unavailable("The Linear inbox could not complete a durable write.");
    } finally {
      this.#inTransaction = false;
    }
  }
}

/**
 * The operator's read-only view of recorded inbox give-ups.
 *
 * A separate class opened `{ readOnly: true }`, so a listing can never write the database. It
 * reads only the disposition table and never joins `linear_inbox_deliveries`, so it cannot return a
 * payload.
 */
export class LinearInboxGiveUpDispositions {
  readonly #db: DatabaseSync;
  #closed = false;

  constructor(filename: string) {
    // Wrapped so a missing or non-database file surfaces as a `RelayError`.
    try { this.#db = new DatabaseSync(filename, { readOnly: true }); }
    catch { throw new RelayError("linear.ingress.storage_unavailable", LINEAR_INBOX_NOT_A_CHANNEL_DATABASE_MESSAGE, 400, false); }
  }

  /**
   * Verifies the complete current schema before reading disposition evidence.
   */
  #assertLinearChannelDatabase(): void {
    assertCurrentSchema(this.#db);
  }

  /**
   * Recorded give-ups, oldest first. `afterSequence` is exclusive and `limit` is bounded; out-of-range
   * input is refused rather than clamped. Paging on `sequence` never skips or repeats a row.
   */
  list(input: { readonly afterSequence?: number; readonly limit?: number } = {}): readonly LinearInboxGiveUpDisposition[] {
    this.#assertOpen();
    // Unknown keys are refused, matching `ChannelRedispatchDispositions`.
    const unknown = Object.keys(input).filter((key) => !["afterSequence", "limit"].includes(key));
    if (unknown.length > 0) throw new RelayError("linear.ingress.storage_unavailable", LINEAR_INBOX_GIVE_UP_QUERY_MESSAGE, 400, false);
    const afterSequence = input.afterSequence ?? 0;
    const limit = input.limit ?? LINEAR_INBOX_GIVE_UP_PAGE_LIMIT;
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0) throw new RelayError("linear.ingress.storage_unavailable", LINEAR_INBOX_GIVE_UP_QUERY_MESSAGE, 400, false);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > LINEAR_INBOX_GIVE_UP_PAGE_LIMIT) throw new RelayError("linear.ingress.storage_unavailable", LINEAR_INBOX_GIVE_UP_QUERY_MESSAGE, 400, false);
    this.#assertLinearChannelDatabase();
    return Object.freeze((this.#db.prepare(`
      SELECT ${GIVE_UP_DISPOSITION_COLUMNS} FROM linear_inbox_give_up_dispositions WHERE sequence > ? ORDER BY sequence LIMIT ?
    `).all(afterSequence, limit) as Row[]).map(giveUpDisposition));
  }

  /**
   * The recorded give-up for one delivery, or null. Lets a mutation check for something to release
   * without opening `SqliteLinearInbox`, whose constructor would initialize the schema even when the
   * release then fails.
   */
  recorded(deliveryId: string): LinearInboxGiveUpDisposition | null {
    this.#assertOpen();
    this.#assertLinearChannelDatabase();
    const row = this.#db.prepare(`SELECT ${GIVE_UP_DISPOSITION_COLUMNS} FROM linear_inbox_give_up_dispositions WHERE delivery_id = ?`).get(deliveryId) as Row | undefined;
    return row === undefined ? null : giveUpDisposition(row);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#db.close();
  }

  #assertOpen(): void {
    if (this.#closed) throw unavailable("The Linear inbox give-up listing is closed.");
  }
}

/**
 * An in-memory inbox for tests: the real `SqliteLinearInbox` against `:memory:`, so every schema-enforced
 * rule behaves as in production. Each call returns an isolated database; close it when the test ends. It
 * cannot exercise a second connection or durability across `close()`.
 */
export function createMemoryLinearInbox(options: { readonly now?: () => Date; readonly maxAttempts?: number } = {}): LinearInboxPort {
  return new SqliteLinearInbox({
    ...(options.now === undefined ? {} : { now: options.now }),
    // Forwarded so a test can reach the give-up path in a few attempts.
    ...(options.maxAttempts === undefined ? {} : { maxAttempts: options.maxAttempts }),
  });
}
