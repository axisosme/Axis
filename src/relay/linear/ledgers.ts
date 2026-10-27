import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import { selectElicitationReply, validateCanonicalLinearUserUrl, type StructuredElicitation } from "../../shared/elicitation.js";
import { schemaFingerprintTable, schemaLedgerAgrees, schemaLedgerVersion, schemaObjectsMatch } from "../../shared/schema-ledger.js";

import { RelayError } from "../errors.js";
import type { VerifiedLinearOwner } from "./authorization.js";
import type { LinearDecisionClaim, LinearDecisionLedger } from "./instruction.js";
import type { LinearChannelTaskRefLookup, LinearRedispatchSourceLookup } from "./redispatch.js";

/**
 * Linear's own durable ledgers. They share the Linear channel database with
 * `./inbox.ts` under the `linear_ledger_` prefix, so the two schema
 * fingerprints stay disjoint.
 *
 * - `linear_ledger_sessions`: AgentSession identity (`issueId`, `teamId`,
 *   creator) and the comment thread root every comment gate compares against.
 * - `linear_ledger_session_inputs`: each turn's dispatched input as
 *   ciphertext, with its Activity/Comment and the frozen owner.
 * - `linear_ledger_startups`: one admission per `created` event, so an exact
 *   replay returns the first outcome and a rejected startup stays rejected.
 * - `linear_ledger_comment_invocations`: one issue Comment to the command it
 *   produced, with its thread root and prompt Activity.
 * - `linear_ledger_decision_outcomes`: how each waiting decision ended and by
 *   whom. The run chain owns whether a decision is open and the outbox owns
 *   what it said.
 * - `linear_ledger_events`: the `feedback_ignored` and `retryable` audit
 *   events, which are not delivery state transitions and so are not
 *   `linear_inbox_events`.
 *
 * Every write runs under `BEGIN IMMEDIATE` and every table has update/delete
 * triggers that abort: a ledger whose rows can be rewritten cannot answer "was
 * this already dispatched". The one exception is
 * `linear_ledger_sessions.root_comment_id`, which is write-once: a session
 * opened by a direct `created` event may gain its root from a later
 * comment-origin turn, and it cannot be re-pointed after that.
 */

/** The contract version stamped on every row this module writes. */
export const LINEAR_LEDGERS_CONTRACT_VERSION = "1.0.0" as const;

/**
 * The two audit events `linear_inbox_events` cannot carry. Frozen as a table so
 * a test can `deepEqual` the persisted strings dashboards match on.
 */
export const LINEAR_LEDGER_EVENT_TYPES = Object.freeze({
  /** Axis observed its own Activity. The delivery is quarantined; no command exists. */
  feedbackIgnored: "feedback_ignored",
  /** Processing failed in a way that may still succeed. The lease is left to expire. */
  retryable: "retryable",
} as const);

export type LinearLedgerEventType = (typeof LINEAR_LEDGER_EVENT_TYPES)[keyof typeof LINEAR_LEDGER_EVENT_TYPES];

/** A total map, so a union member added without being admitted is a `tsc` error. */
const ledgerEventTypeAdmitted: Readonly<Record<LinearLedgerEventType, true>> = Object.freeze({
  feedback_ignored: true,
  retryable: true,
});

/** Detail payloads are bounded exactly like `./inbox.ts` bounds its own. */
const DETAIL_TEXT_LIMIT = 128;

function boundedDetailText(value: string): string {
  return value.length <= DETAIL_TEXT_LIMIT ? value : `${value.slice(0, DETAIL_TEXT_LIMIT - 3)}...`;
}

function unavailable(message: string): RelayError {
  return new RelayError("linear.ingress.storage_unavailable", message, 503, true);
}

function conflict(message: string): RelayError {
  return new RelayError("linear.ingress.logical_event_conflict", message, 409);
}

function invalid(message: string): RelayError {
  return new RelayError("axis.relay.invalid_transition", message, 409);
}

/** Distinct messages for guards sharing `linear.ingress.storage_unavailable`, so a test can tell them apart. */
export const LINEAR_LEDGERS_UNLEDGERED_MESSAGE = "The Linear ledger database holds tables with no schema ledger.";
export const LINEAR_LEDGERS_SCHEMA_DRIFT_MESSAGE = "The Linear ledger schema does not match this build.";
export const LINEAR_LEDGERS_LEDGER_MESSAGE = "The Linear ledger schema ledger disagrees with its objects.";

// ---------------------------------------------------------------------------
// Logical event identity
// ---------------------------------------------------------------------------

/** The four shapes a Linear logical event key can take. Closed on purpose. */
export type LinearLogicalEventOrigin =
  | { readonly kind: "session"; readonly action: "created"; readonly agentSessionId: string }
  | { readonly kind: "session"; readonly action: "prompted"; readonly agentSessionId: string; readonly activityId: string }
  | { readonly kind: "comment"; readonly action: "created"; readonly sourceCommentId: string }
  | { readonly kind: "comment"; readonly action: "prompted"; readonly sourceCommentId: string; readonly activityId: string };

/**
 * The command idempotency key. These JSON strings are contract: a redelivery
 * under a different `Linear-Delivery` id must land on the same command, and
 * changing one re-dispatches every in-flight command once. A comment-origin
 * event keys on the Comment, because a prompt-first start and its delayed
 * `created` event describe the same act through different sessions' fields.
 */
export function linearLogicalEventKey(origin: LinearLogicalEventOrigin): string {
  if (origin.kind === "comment") {
    return origin.action === "created"
      ? JSON.stringify(["Comment", "create", origin.sourceCommentId])
      : JSON.stringify(["Comment", "prompted", origin.sourceCommentId, origin.activityId]);
  }
  return origin.action === "created"
    ? JSON.stringify(["AgentSessionEvent", "created", origin.agentSessionId])
    : JSON.stringify(["AgentSessionEvent", "prompted", origin.agentSessionId, origin.activityId]);
}

/**
 * The deterministic `commandId` for one logical event. The run chain recognizes
 * a repeat by `commandId` plus intent digest, so a redelivery lands on the
 * existing reservation instead of reserving a second command.
 */
export function linearChannelCommandId(logicalEventKey: string): string {
  return `lnc-${createHash("sha256").update(logicalEventKey, "utf8").digest("hex").slice(0, 40)}`;
}

/**
 * The startup intent digest over a `created` event's identity fields, so a
 * replay that disagrees about any of them is a conflict rather than a repeat.
 */
export function linearStartupIntentDigest(input: {
  readonly organizationId: string;
  readonly appUserId: string;
  readonly agentSession: {
    readonly id: string;
    readonly organizationId: string;
    readonly appUserId: string;
    readonly issueId: string;
    readonly issueTeamId: string;
    readonly creatorId: string;
    readonly status: string;
    readonly commentId?: string | null;
    readonly sourceCommentId?: string | null;
  };
}): string {
  return canonicalDigest({
    version: 1,
    type: "AgentSessionEvent",
    action: "created",
    organizationId: input.organizationId,
    appUserId: input.appUserId,
    agentSession: {
      id: input.agentSession.id,
      organizationId: input.agentSession.organizationId,
      appUserId: input.agentSession.appUserId,
      issueId: input.agentSession.issueId,
      issueTeamId: input.agentSession.issueTeamId,
      creatorId: input.agentSession.creatorId,
      status: input.agentSession.status,
      commentId: input.agentSession.commentId ?? null,
      sourceCommentId: input.agentSession.sourceCommentId ?? null,
    },
  });
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  const object = value as Readonly<Record<string, unknown>>;
  return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`).join(",")}}`;
}

/** `sha256:`-prefixed, matching `../security.ts`'s `sha256`; the prefix is part of the stored form. */
function canonicalDigest(value: unknown): string {
  return `sha256:${createHash("sha256").update(canonicalJson(value), "utf8").digest("hex")}`;
}

// ---------------------------------------------------------------------------
// Row shapes
// ---------------------------------------------------------------------------

export interface LinearSessionRecord {
  readonly agentSessionId: string;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly issueId: string;
  readonly teamId: string;
  readonly creatorId: string;
  /** The captured comment thread root, or null for a session opened directly. */
  readonly rootCommentId: string | null;
}

export interface LinearSessionInputRecord {
  readonly sourceEventId: string;
  readonly agentSessionId: string;
  readonly commandId: string | null;
  readonly activityId: string | null;
  readonly sourceCommentId: string | null;
  readonly inputDigest: string;
  readonly turnOrdinal: number;
  readonly owner: VerifiedLinearOwner;
}

export type LinearStartupDisposition = "accepted" | "rejected";

export interface LinearStartupRecord {
  readonly agentSessionId: string;
  readonly logicalEventKey: string;
  readonly startupIntentDigest: string;
  readonly disposition: LinearStartupDisposition;
  /** Non-null exactly when `disposition` is `accepted`. */
  readonly commandId: string | null;
  readonly inputSourceEventId: string | null;
  readonly failureStage: string | null;
  readonly failureReason: string | null;
}

export interface LinearCommentInvocationRecord {
  readonly sourceCommentId: string;
  readonly rootCommentId: string;
  readonly agentSessionId: string;
  readonly activityId: string | null;
  readonly deliveryId: string;
  readonly logicalEventKey: string;
  readonly disposition: "accepted" | "rejected";
  /** Non-null exactly when `disposition` is `accepted`. */
  readonly commandId: string | null;
  readonly instructionDigest: string;
  readonly safeCode: string | null;
  readonly responseSourceEventId: string | null;
}

export interface LinearLedgerEvent {
  readonly sequence: number;
  readonly deliveryId: string;
  readonly eventType: LinearLedgerEventType;
  readonly detail: Readonly<Record<string, string>> | null;
  readonly recordedAt: string;
}

/**
 * How a waiting decision ended. `resolved`: the user picked a raised option.
 * `closed`: a stop. `superseded`: the user replied in prose, the question went
 * to the model to interpret, and the successor Run replaced the parked one.
 *
 * `superseded` is not an answer (`resolvedDecisionByActivity` ignores it), but
 * `interpretedDecisionByActivity` reads it to rebuild the identical dispatch on
 * an exact webhook replay; without it the run chain would refuse the replay
 * `command_rebound`.
 */
export type LinearDecisionOutcomeKind = "resolved" | "closed" | "superseded";

export interface LinearDecisionOutcomeRecord {
  readonly decisionId: string;
  readonly agentSessionId: string;
  readonly outcome: LinearDecisionOutcomeKind;
  readonly sourceEventId: string;
  readonly activityId: string;
  readonly ownerUserId: string;
  readonly selectedOptionId: string | null;
  readonly selectedLabel: string | null;
}

/**
 * The session-scoped owner read. A waiting decision's owner comes from
 * `linearDecisionOwner` instead; see `sessionOwner`.
 */
export interface LinearSessionOwnerLookup {
  sessionOwner(agentSessionId: string): VerifiedLinearOwner | null;
}

export interface SqliteLinearLedgersOptions {
  /** Omitted for an in-process database. */
  readonly filename?: string;
  readonly now?: () => Date;
  readonly busyTimeoutMs?: number;
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const schemaDefinitions = Object.freeze({
  "table:linear_ledger_schema": "CREATE TABLE linear_ledger_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
  "table:linear_ledger_sessions": "CREATE TABLE linear_ledger_sessions(agent_session_id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, app_user_id TEXT NOT NULL, issue_id TEXT NOT NULL, team_id TEXT NOT NULL, creator_id TEXT NOT NULL, root_comment_id TEXT, contract_version TEXT NOT NULL, created_at TEXT NOT NULL)",
  "table:linear_ledger_session_inputs": "CREATE TABLE linear_ledger_session_inputs(source_event_id TEXT PRIMARY KEY, agent_session_id TEXT NOT NULL, command_id TEXT UNIQUE, activity_id TEXT, source_comment_id TEXT, input_digest TEXT NOT NULL, input_ciphertext TEXT NOT NULL, turn_ordinal INTEGER NOT NULL, owner_user_id TEXT NOT NULL, owner_user_url TEXT NOT NULL, contract_version TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(agent_session_id, turn_ordinal), CHECK(turn_ordinal >= 1), FOREIGN KEY(agent_session_id) REFERENCES linear_ledger_sessions(agent_session_id))",
  "table:linear_ledger_startups": "CREATE TABLE linear_ledger_startups(agent_session_id TEXT PRIMARY KEY, logical_event_key TEXT NOT NULL UNIQUE, startup_intent_digest TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN ('accepted', 'rejected')), command_id TEXT, input_source_event_id TEXT, failure_stage TEXT, failure_reason TEXT, contract_version TEXT NOT NULL, created_at TEXT NOT NULL, CHECK((disposition = 'accepted') = (command_id IS NOT NULL)), CHECK((disposition = 'accepted') = (input_source_event_id IS NOT NULL)), CHECK((disposition = 'rejected') = (failure_stage IS NOT NULL)), CHECK((disposition = 'rejected') = (failure_reason IS NOT NULL)))",
  "table:linear_ledger_comment_invocations": "CREATE TABLE linear_ledger_comment_invocations(source_comment_id TEXT PRIMARY KEY, root_comment_id TEXT NOT NULL, agent_session_id TEXT NOT NULL, activity_id TEXT, delivery_id TEXT NOT NULL, logical_event_key TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN ('accepted', 'rejected')), command_id TEXT, instruction_digest TEXT NOT NULL, safe_code TEXT, response_source_event_id TEXT, contract_version TEXT NOT NULL, created_at TEXT NOT NULL, CHECK((disposition = 'accepted') = (command_id IS NOT NULL)), CHECK((disposition = 'rejected') = (safe_code IS NOT NULL)))",
  "table:linear_ledger_decision_outcomes": "CREATE TABLE linear_ledger_decision_outcomes(decision_id TEXT PRIMARY KEY, agent_session_id TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN ('resolved', 'closed', 'superseded')), source_event_id TEXT NOT NULL, activity_id TEXT NOT NULL, owner_user_id TEXT NOT NULL, selected_option_id TEXT, selected_label TEXT, contract_version TEXT NOT NULL, recorded_at TEXT NOT NULL, CHECK((outcome = 'resolved') = (selected_option_id IS NOT NULL)), CHECK((outcome = 'resolved') = (selected_label IS NOT NULL)), FOREIGN KEY(agent_session_id) REFERENCES linear_ledger_sessions(agent_session_id))",
  "table:linear_ledger_events": "CREATE TABLE linear_ledger_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL, event_type TEXT NOT NULL CHECK(event_type IN ('feedback_ignored', 'retryable')), detail_json TEXT, recorded_at TEXT NOT NULL)",
  "index:linear_ledger_session_inputs_session": "CREATE INDEX linear_ledger_session_inputs_session ON linear_ledger_session_inputs(agent_session_id, turn_ordinal)",
  "index:linear_ledger_comment_invocations_session": "CREATE INDEX linear_ledger_comment_invocations_session ON linear_ledger_comment_invocations(agent_session_id, disposition)",
  "index:linear_ledger_decision_outcomes_session": "CREATE INDEX linear_ledger_decision_outcomes_session ON linear_ledger_decision_outcomes(agent_session_id, recorded_at)",
  "index:linear_ledger_events_delivery": "CREATE INDEX linear_ledger_events_delivery ON linear_ledger_events(delivery_id, sequence)",
  "trigger:linear_ledger_session_inputs_immutable_update": "CREATE TRIGGER linear_ledger_session_inputs_immutable_update BEFORE UPDATE ON linear_ledger_session_inputs BEGIN SELECT RAISE(ABORT, 'linear ledger session inputs are immutable'); END",
  "trigger:linear_ledger_session_inputs_immutable_delete": "CREATE TRIGGER linear_ledger_session_inputs_immutable_delete BEFORE DELETE ON linear_ledger_session_inputs BEGIN SELECT RAISE(ABORT, 'linear ledger session inputs are immutable'); END",
  "trigger:linear_ledger_startups_immutable_update": "CREATE TRIGGER linear_ledger_startups_immutable_update BEFORE UPDATE ON linear_ledger_startups BEGIN SELECT RAISE(ABORT, 'linear ledger startups are immutable'); END",
  "trigger:linear_ledger_startups_immutable_delete": "CREATE TRIGGER linear_ledger_startups_immutable_delete BEFORE DELETE ON linear_ledger_startups BEGIN SELECT RAISE(ABORT, 'linear ledger startups are immutable'); END",
  "trigger:linear_ledger_comment_invocations_immutable_update": "CREATE TRIGGER linear_ledger_comment_invocations_immutable_update BEFORE UPDATE ON linear_ledger_comment_invocations BEGIN SELECT RAISE(ABORT, 'linear ledger comment invocations are immutable'); END",
  "trigger:linear_ledger_comment_invocations_immutable_delete": "CREATE TRIGGER linear_ledger_comment_invocations_immutable_delete BEFORE DELETE ON linear_ledger_comment_invocations BEGIN SELECT RAISE(ABORT, 'linear ledger comment invocations are immutable'); END",
  "trigger:linear_ledger_sessions_immutable_update": "CREATE TRIGGER linear_ledger_sessions_immutable_update BEFORE UPDATE ON linear_ledger_sessions WHEN OLD.root_comment_id IS NOT NULL OR NEW.agent_session_id IS NOT OLD.agent_session_id OR NEW.organization_id IS NOT OLD.organization_id OR NEW.app_user_id IS NOT OLD.app_user_id OR NEW.issue_id IS NOT OLD.issue_id OR NEW.team_id IS NOT OLD.team_id OR NEW.creator_id IS NOT OLD.creator_id OR NEW.contract_version IS NOT OLD.contract_version OR NEW.created_at IS NOT OLD.created_at BEGIN SELECT RAISE(ABORT, 'linear ledger session identity is immutable'); END",
  "trigger:linear_ledger_sessions_immutable_delete": "CREATE TRIGGER linear_ledger_sessions_immutable_delete BEFORE DELETE ON linear_ledger_sessions BEGIN SELECT RAISE(ABORT, 'linear ledger sessions are immutable'); END",
  "trigger:linear_ledger_decision_outcomes_immutable_update": "CREATE TRIGGER linear_ledger_decision_outcomes_immutable_update BEFORE UPDATE ON linear_ledger_decision_outcomes BEGIN SELECT RAISE(ABORT, 'linear ledger decision outcomes are immutable'); END",
  "trigger:linear_ledger_decision_outcomes_immutable_delete": "CREATE TRIGGER linear_ledger_decision_outcomes_immutable_delete BEFORE DELETE ON linear_ledger_decision_outcomes BEGIN SELECT RAISE(ABORT, 'linear ledger decision outcomes are immutable'); END",
  "trigger:linear_ledger_events_immutable_update": "CREATE TRIGGER linear_ledger_events_immutable_update BEFORE UPDATE ON linear_ledger_events BEGIN SELECT RAISE(ABORT, 'linear ledger events are immutable'); END",
  "trigger:linear_ledger_events_immutable_delete": "CREATE TRIGGER linear_ledger_events_immutable_delete BEFORE DELETE ON linear_ledger_events BEGIN SELECT RAISE(ABORT, 'linear ledger events are immutable'); END",
});

const schemaFingerprints = schemaFingerprintTable(schemaDefinitions);

/** The current schema version; exported so a test can pin the real constant. */
export const LINEAR_LEDGERS_SCHEMA_VERSION = 1;

type Row = Record<string, unknown>;

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

export class SqliteLinearLedgers implements LinearSessionOwnerLookup {
  readonly #db: DatabaseSync;
  readonly #now: () => Date;
  #closed = false;
  #inTransaction = false;

  constructor(options: SqliteLinearLedgersOptions = {}) {
    const busyTimeoutMs = options.busyTimeoutMs ?? 5_000;
    if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0) throw unavailable("The Linear ledger busy timeout must be a non-negative integer.");
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
   * The full health check: the schema sweep plus `integrity_check` and
   * `foreign_key_check`. Backs `GET /health/diagnostics`; readiness calls
   * `readyForTraffic()`.
   */
  ready(): boolean {
    try { this.#assertOpen(); this.#assertInvariants(); return true; } catch { return false; }
  }

  /**
   * Whether this store can serve traffic, at a cost bounded by the schema
   * rather than the rows. Scoped to the `linear_ledger%` prefix: the inbox and
   * installation tables in the same file are checked by their own stores.
   */
  readyForTraffic(): boolean {
    try { this.#assertOpen(); this.#assertSchemaInvariants(); return true; } catch { return false; }
  }

  // -- session identity -----------------------------------------------------

  /**
   * Binds one AgentSession's durable identity, or verifies the one already
   * held. Every field is compared on a repeat, so a re-teamed issue or a
   * different creator fails closed. `rootCommentId` is write-once: it may
   * arrive on a later comment-origin turn and cannot change after that.
   */
  bindSession(input: {
    readonly agentSessionId: string;
    readonly organizationId: string;
    readonly appUserId: string;
    readonly issueId: string;
    readonly teamId: string;
    readonly creatorId: string;
    readonly rootCommentId: string | null;
  }): { readonly created: boolean } {
    this.#assertOpen();
    return this.#transaction(() => {
      const existing = this.#db.prepare("SELECT * FROM linear_ledger_sessions WHERE agent_session_id = ?").get(input.agentSessionId) as Row | undefined;
      if (existing !== undefined) {
        if (
          existing.organization_id !== input.organizationId
          || existing.app_user_id !== input.appUserId
          || existing.issue_id !== input.issueId
          || existing.team_id !== input.teamId
          || existing.creator_id !== input.creatorId
          || (existing.root_comment_id !== null && input.rootCommentId !== null && existing.root_comment_id !== input.rootCommentId)
        ) throw conflict("The AgentSession identity conflicts with retained state.");
        if (existing.root_comment_id === null && input.rootCommentId !== null) {
          this.#db.prepare("UPDATE linear_ledger_sessions SET root_comment_id = ? WHERE agent_session_id = ? AND root_comment_id IS NULL")
            .run(input.rootCommentId, input.agentSessionId);
        }
        return { created: false };
      }
      this.#db.prepare(`
        INSERT INTO linear_ledger_sessions(
          agent_session_id, organization_id, app_user_id, issue_id, team_id, creator_id,
          root_comment_id, contract_version, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        input.agentSessionId, input.organizationId, input.appUserId, input.issueId,
        input.teamId, input.creatorId, input.rootCommentId,
        LINEAR_LEDGERS_CONTRACT_VERSION, this.#instant(),
      );
      return { created: true };
    });
  }

  session(agentSessionId: string): LinearSessionRecord | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT * FROM linear_ledger_sessions WHERE agent_session_id = ?").get(agentSessionId) as Row | undefined;
    return row === undefined ? null : Object.freeze({
      agentSessionId: String(row.agent_session_id),
      organizationId: String(row.organization_id),
      appUserId: String(row.app_user_id),
      issueId: String(row.issue_id),
      teamId: String(row.team_id),
      creatorId: String(row.creator_id),
      rootCommentId: row.root_comment_id === null ? null : String(row.root_comment_id),
    });
  }

  // -- session inputs -------------------------------------------------------

  /**
   * Records one turn's verified execution input, or recognizes the identical
   * one already held.
   *
   * A replay matches on `sourceEventId` or `commandId`. Linear retries a
   * logical event under a fresh `Linear-Delivery`, which `./inbox.ts` treats as
   * a new delivery carrying the same deterministic `commandId`; matching on the
   * delivery alone would hit `command_id`'s UNIQUE constraint and fail as a
   * retryable storage error forever. A replay returns the original turn
   * ordinal. A match whose identity disagrees is a conflict.
   */
  recordSessionInput(input: {
    readonly sourceEventId: string;
    readonly agentSessionId: string;
    readonly activityId: string | null;
    readonly sourceCommentId: string | null;
    readonly inputDigest: string;
    readonly inputCiphertext: string;
    readonly owner: VerifiedLinearOwner;
    /**
     * The command this input feeds, and the second replay key. A waiting stop
     * supplies one too: its run-chain reservation id. Lets
     * `linearRedispatchSourceLookup` find the input a command read.
     */
    readonly commandId: string | null;
  }): {
    readonly turnOrdinal: number;
    readonly created: boolean;
    /**
     * The ciphertext retained for this logical event: the caller's on first
     * record, the original on a replay. Not compared, because encryption is
     * randomized; dispatching it keeps a command's bytes the ones first recorded.
     */
    readonly inputCiphertext: string;
  } {
    const ownerUrl = validateCanonicalLinearUserUrl(input.owner.userUrl);
    if (input.owner.userId.trim().length === 0 || ownerUrl === null) throw invalid("The session input owner is invalid.");
    this.#assertOpen();
    return this.#transaction(() => {
      const columns = "source_event_id, agent_session_id, command_id, activity_id, source_comment_id, input_digest, input_ciphertext, turn_ordinal, owner_user_id, owner_user_url";
      const byDelivery = this.#db.prepare(`SELECT ${columns} FROM linear_ledger_session_inputs WHERE source_event_id = ?`)
        .get(input.sourceEventId) as Row | undefined;
      // Looked up separately so two different rows for the two keys is a conflict.
      const byCommand = input.commandId === null ? undefined
        : this.#db.prepare(`SELECT ${columns} FROM linear_ledger_session_inputs WHERE command_id = ?`)
          .get(input.commandId) as Row | undefined;
      if (byDelivery !== undefined && byCommand !== undefined && byDelivery.source_event_id !== byCommand.source_event_id) {
        throw conflict("The session input's delivery and command already name two different turns.");
      }
      const existing = byDelivery ?? byCommand;
      if (existing !== undefined) {
        // `source_event_id` is not compared: a semantic replay differs only there.
        if (
          existing.agent_session_id !== input.agentSessionId
          || existing.command_id !== input.commandId
          || existing.activity_id !== input.activityId
          || existing.source_comment_id !== input.sourceCommentId
          || existing.input_digest !== input.inputDigest
          || existing.owner_user_id !== input.owner.userId
          || existing.owner_user_url !== ownerUrl
        ) throw conflict("The session input conflicts with the one already recorded for this logical event.");
        return { turnOrdinal: Number(existing.turn_ordinal), created: false, inputCiphertext: String(existing.input_ciphertext) };
      }
      const ordinal = this.#nextTurnOrdinal(input.agentSessionId);
      this.#db.prepare(`
        INSERT INTO linear_ledger_session_inputs(
          source_event_id, agent_session_id, command_id, activity_id, source_comment_id, input_digest,
          input_ciphertext, turn_ordinal, owner_user_id, owner_user_url, contract_version, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        input.sourceEventId, input.agentSessionId, input.commandId, input.activityId, input.sourceCommentId,
        input.inputDigest, input.inputCiphertext, ordinal, input.owner.userId, ownerUrl,
        LINEAR_LEDGERS_CONTRACT_VERSION, this.#instant(),
      );
      return { turnOrdinal: ordinal, created: true, inputCiphertext: input.inputCiphertext };
    });
  }

  /** The input one command carried, or null. Keyed on the command, for redispatch. */
  sessionInputByCommand(commandId: string): LinearSessionInputRecord | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT * FROM linear_ledger_session_inputs WHERE command_id = ?").get(commandId) as Row | undefined;
    return row === undefined ? null : this.#sessionInputRow(row);
  }

  sessionInput(sourceEventId: string): LinearSessionInputRecord | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT * FROM linear_ledger_session_inputs WHERE source_event_id = ?").get(sourceEventId) as Row | undefined;
    return row === undefined ? null : this.#sessionInputRow(row);
  }

  /** The ciphertext of one turn's input, for a caller that holds the key. */
  inputCiphertext(sourceEventId: string): string | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT input_ciphertext FROM linear_ledger_session_inputs WHERE source_event_id = ?").get(sourceEventId) as Row | undefined;
    return row === undefined ? null : String(row.input_ciphertext);
  }

  /** The highest turn ordinal recorded for a session, or 0 before its first input. */
  turnOrdinal(agentSessionId: string): number {
    this.#assertOpen();
    return this.#highestTurnOrdinal(agentSessionId);
  }

  /**
   * The owner frozen on this session's newest input ("who last acted"). Not a
   * waiting decision's owner, which differs once two humans share a session;
   * see `linearDecisionOwner`.
   */
  sessionOwner(agentSessionId: string): VerifiedLinearOwner | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT owner_user_id, owner_user_url FROM linear_ledger_session_inputs WHERE agent_session_id = ? ORDER BY turn_ordinal DESC LIMIT 1")
      .get(agentSessionId) as Row | undefined;
    return row === undefined ? null : this.#ownerOf(row);
  }

  /** The owner frozen on the input a specific command carried. */
  commandOwner(commandId: string): VerifiedLinearOwner | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT owner_user_id, owner_user_url FROM linear_ledger_session_inputs WHERE command_id = ?")
      .get(commandId) as Row | undefined;
    return row === undefined ? null : this.#ownerOf(row);
  }

  #ownerOf(row: Row): VerifiedLinearOwner {
    const url = validateCanonicalLinearUserUrl(row.owner_user_url);
    if (url === null) throw invalid("The retained session owner URL is not canonical.");
    return Object.freeze({ userId: String(row.owner_user_id), userUrl: url });
  }

  // -- startup admission ----------------------------------------------------

  startup(agentSessionId: string): LinearStartupRecord | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT * FROM linear_ledger_startups WHERE agent_session_id = ?").get(agentSessionId) as Row | undefined;
    return row === undefined ? null : this.#startupRow(row);
  }

  /**
   * Admits one `created` event, or returns the outcome the first identical one
   * produced. A row matching on `agentSessionId` or `logicalEventKey` is a
   * repeat only when every retained field agrees; otherwise it is a conflict.
   * `commandId` is the caller's deterministic id, shared with the run chain.
   */
  recordStartupAcceptance(input: {
    readonly agentSessionId: string;
    readonly logicalEventKey: string;
    readonly startupIntentDigest: string;
    readonly commandId: string;
    readonly inputSourceEventId: string;
  }): { readonly commandId: string; readonly created: boolean } {
    this.#assertOpen();
    return this.#transaction(() => {
      const retained = this.#retainedStartup(input.agentSessionId, input.logicalEventKey);
      if (retained !== null) {
        if (
          retained.agentSessionId !== input.agentSessionId
          || retained.logicalEventKey !== input.logicalEventKey
          || retained.startupIntentDigest !== input.startupIntentDigest
          || retained.disposition !== "accepted"
          || retained.commandId === null
        ) throw conflict("The created event conflicts with retained startup state.");
        return { commandId: retained.commandId, created: false };
      }
      this.#db.prepare(`
        INSERT INTO linear_ledger_startups(
          agent_session_id, logical_event_key, startup_intent_digest, disposition,
          command_id, input_source_event_id, failure_stage, failure_reason, contract_version, created_at
        ) VALUES (?, ?, ?, 'accepted', ?, ?, NULL, NULL, ?, ?)
      `).run(
        input.agentSessionId, input.logicalEventKey, input.startupIntentDigest,
        input.commandId, input.inputSourceEventId, LINEAR_LEDGERS_CONTRACT_VERSION, this.#instant(),
      );
      return { commandId: input.commandId, created: true };
    });
  }

  /**
   * Retains a definite startup failure so the same `created` event cannot later
   * be admitted. A later prompt on the session keys on its own logical event
   * and can still start work.
   */
  recordStartupRejection(input: {
    readonly agentSessionId: string;
    readonly logicalEventKey: string;
    readonly startupIntentDigest: string;
    readonly failureStage: string;
    readonly failureReason: string;
  }): { readonly created: boolean } {
    this.#assertOpen();
    return this.#transaction(() => {
      const retained = this.#retainedStartup(input.agentSessionId, input.logicalEventKey);
      if (retained !== null) {
        if (
          retained.agentSessionId !== input.agentSessionId
          || retained.logicalEventKey !== input.logicalEventKey
          || retained.startupIntentDigest !== input.startupIntentDigest
          || retained.disposition !== "rejected"
          || retained.failureStage !== input.failureStage
          || retained.failureReason !== input.failureReason
        ) throw conflict("The created event conflicts with retained startup state.");
        return { created: false };
      }
      this.#db.prepare(`
        INSERT INTO linear_ledger_startups(
          agent_session_id, logical_event_key, startup_intent_digest, disposition,
          command_id, input_source_event_id, failure_stage, failure_reason, contract_version, created_at
        ) VALUES (?, ?, ?, 'rejected', NULL, NULL, ?, ?, ?, ?)
      `).run(
        input.agentSessionId, input.logicalEventKey, input.startupIntentDigest,
        input.failureStage, input.failureReason, LINEAR_LEDGERS_CONTRACT_VERSION, this.#instant(),
      );
      return { created: true };
    });
  }

  // -- comment invocations --------------------------------------------------

  commentInvocation(sourceCommentId: string): LinearCommentInvocationRecord | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT * FROM linear_ledger_comment_invocations WHERE source_comment_id = ?").get(sourceCommentId) as Row | undefined;
    return row === undefined ? null : this.#commentInvocationRow(row);
  }

  /**
   * Binds one issue Comment to the command it produced. A repeat is a replay
   * only when every identity field agrees, including the thread root and prompt
   * Activity; anything else is a refused rebind.
   */
  recordCommentInvocation(input: {
    readonly sourceCommentId: string;
    readonly rootCommentId: string;
    readonly agentSessionId: string;
    readonly activityId: string | null;
    readonly deliveryId: string;
    readonly logicalEventKey: string;
    readonly commandId: string;
    readonly instructionDigest: string;
    readonly responseSourceEventId: string;
  }): { readonly replayed: boolean } {
    this.#assertOpen();
    return this.#transaction(() => {
      const existing = this.#db.prepare("SELECT * FROM linear_ledger_comment_invocations WHERE source_comment_id = ?").get(input.sourceCommentId) as Row | undefined;
      if (existing !== undefined) {
        const retained = this.#commentInvocationRow(existing);
        if (
          retained.rootCommentId !== input.rootCommentId
          || retained.agentSessionId !== input.agentSessionId
          || retained.activityId !== input.activityId
          || retained.logicalEventKey !== input.logicalEventKey
          || retained.instructionDigest !== input.instructionDigest
          || retained.disposition !== "accepted"
          || retained.commandId !== input.commandId
          || retained.responseSourceEventId !== input.responseSourceEventId
        ) throw conflict("The comment identity conflicts with a prior invocation.");
        return { replayed: true };
      }
      this.#db.prepare(`
        INSERT INTO linear_ledger_comment_invocations(
          source_comment_id, root_comment_id, agent_session_id, activity_id, delivery_id,
          logical_event_key, disposition, command_id, instruction_digest, safe_code,
          response_source_event_id, contract_version, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?, ?, NULL, ?, ?, ?)
      `).run(
        input.sourceCommentId, input.rootCommentId, input.agentSessionId, input.activityId,
        input.deliveryId, input.logicalEventKey, input.commandId, input.instructionDigest,
        input.responseSourceEventId, LINEAR_LEDGERS_CONTRACT_VERSION, this.#instant(),
      );
      return { replayed: false };
    });
  }

  /**
   * Retains a refused comment invocation together with the fixed body that was
   * answered, so the same Comment cannot be retried into a dispatch and the
   * refusal is answered exactly once.
   */
  recordCommentRejection(input: {
    readonly sourceCommentId: string;
    readonly rootCommentId: string;
    readonly agentSessionId: string;
    readonly activityId: string | null;
    readonly deliveryId: string;
    readonly logicalEventKey: string;
    readonly instructionDigest: string;
    readonly safeCode: string;
    readonly responseSourceEventId: string;
  }): { readonly created: boolean } {
    this.#assertOpen();
    return this.#transaction(() => {
      const existing = this.#db.prepare("SELECT * FROM linear_ledger_comment_invocations WHERE source_comment_id = ?").get(input.sourceCommentId) as Row | undefined;
      if (existing !== undefined) {
        const retained = this.#commentInvocationRow(existing);
        if (
          retained.rootCommentId !== input.rootCommentId
          || retained.agentSessionId !== input.agentSessionId
          || retained.activityId !== input.activityId
          || retained.logicalEventKey !== input.logicalEventKey
          || retained.instructionDigest !== input.instructionDigest
          || retained.disposition !== "rejected"
          || retained.safeCode !== input.safeCode
        ) throw conflict("The comment identity conflicts with a prior invocation.");
        return { created: false };
      }
      this.#db.prepare(`
        INSERT INTO linear_ledger_comment_invocations(
          source_comment_id, root_comment_id, agent_session_id, activity_id, delivery_id,
          logical_event_key, disposition, command_id, instruction_digest, safe_code,
          response_source_event_id, contract_version, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'rejected', NULL, ?, ?, ?, ?, ?)
      `).run(
        input.sourceCommentId, input.rootCommentId, input.agentSessionId, input.activityId,
        input.deliveryId, input.logicalEventKey, input.instructionDigest, input.safeCode,
        input.responseSourceEventId, LINEAR_LEDGERS_CONTRACT_VERSION, this.#instant(),
      );
      return { created: true };
    });
  }

  // -- waiting decisions ----------------------------------------------------

  decisionOutcome(decisionId: string): LinearDecisionOutcomeRecord | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT * FROM linear_ledger_decision_outcomes WHERE decision_id = ?").get(decisionId) as Row | undefined;
    return row === undefined ? null : this.#decisionOutcomeRow(row);
  }

  /**
   * The decision this session resolved from this exact Activity, if any. Keyed
   * on the Activity, not the reply text, so an exact webhook replay is
   * idempotent.
   */
  resolvedDecisionByActivity(agentSessionId: string, activityId: string): LinearDecisionOutcomeRecord | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT * FROM linear_ledger_decision_outcomes WHERE agent_session_id = ? AND activity_id = ? AND outcome = 'resolved' LIMIT 1")
      .get(agentSessionId, activityId) as Row | undefined;
    return row === undefined ? null : this.#decisionOutcomeRow(row);
  }

  /**
   * The decision a free-text interpretation from this exact Activity was bound
   * to. An exact replay uses it to rebuild the identical interpretation
   * envelope; re-dispatching the raw body would be refused `command_rebound`.
   */
  interpretedDecisionByActivity(agentSessionId: string, activityId: string): LinearDecisionOutcomeRecord | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT * FROM linear_ledger_decision_outcomes WHERE agent_session_id = ? AND activity_id = ? AND outcome = 'superseded' LIMIT 1")
      .get(agentSessionId, activityId) as Row | undefined;
    return row === undefined ? null : this.#decisionOutcomeRow(row);
  }

  /**
   * The decision this input answered or was interpreted against, or null.
   * Derived from the outcome row's `source_event_id`. Feeds
   * `linearRedispatchSourceLookup`: neither kind of input dispatched its
   * Activity body, so neither is redispatchable. `closed` rows are excluded
   * because a stop never queues a redispatch marker.
   */
  decisionBoundToInput(sourceEventId: string): string | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT decision_id FROM linear_ledger_decision_outcomes WHERE source_event_id = ? AND outcome IN ('resolved', 'superseded')")
      .get(sourceEventId) as Row | undefined;
    return row === undefined ? null : String(row.decision_id);
  }

  /**
   * The Comment a delivery was invoked through, or null for a non-comment
   * delivery. A replayed decision reply must arrive through the same channel
   * as the original answer.
   */
  commentSourceForDelivery(deliveryId: string, agentSessionId: string): string | null {
    this.#assertOpen();
    const row = this.#db.prepare("SELECT source_comment_id FROM linear_ledger_comment_invocations WHERE delivery_id = ? AND agent_session_id = ? AND disposition = 'accepted'")
      .get(deliveryId, agentSessionId) as Row | undefined;
    return row === undefined ? null : String(row.source_comment_id);
  }

  /**
   * Records how one waiting decision ended, one row per decision. The
   * identical outcome is a replay; a different one is a conflict.
   */
  recordDecisionOutcome(input: {
    readonly decisionId: string;
    readonly agentSessionId: string;
    readonly outcome: LinearDecisionOutcomeKind;
    readonly sourceEventId: string;
    readonly activityId: string;
    readonly ownerUserId: string;
    readonly selected: { readonly optionId: string; readonly label: string } | null;
  }): { readonly created: boolean } {
    if ((input.outcome === "resolved") !== (input.selected !== null)) {
      throw invalid("A resolved decision requires a selected option and every other outcome forbids one.");
    }
    this.#assertOpen();
    return this.#transaction(() => {
      const existing = this.#db.prepare("SELECT * FROM linear_ledger_decision_outcomes WHERE decision_id = ?").get(input.decisionId) as Row | undefined;
      if (existing !== undefined) {
        const retained = this.#decisionOutcomeRow(existing);
        if (
          retained.agentSessionId !== input.agentSessionId
          || retained.outcome !== input.outcome
          || retained.sourceEventId !== input.sourceEventId
          || retained.activityId !== input.activityId
          || retained.ownerUserId !== input.ownerUserId
          || retained.selectedOptionId !== (input.selected?.optionId ?? null)
          || retained.selectedLabel !== (input.selected?.label ?? null)
        ) throw conflict("The decision outcome conflicts with the one already recorded.");
        return { created: false };
      }
      this.#db.prepare(`
        INSERT INTO linear_ledger_decision_outcomes(
          decision_id, agent_session_id, outcome, source_event_id, activity_id, owner_user_id,
          selected_option_id, selected_label, contract_version, recorded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        input.decisionId, input.agentSessionId, input.outcome, input.sourceEventId, input.activityId,
        input.ownerUserId, input.selected?.optionId ?? null, input.selected?.label ?? null,
        LINEAR_LEDGERS_CONTRACT_VERSION, this.#instant(),
      );
      return { created: true };
    });
  }

  // -- audit ----------------------------------------------------------------

  /**
   * Appends a `feedback_ignored` or `retryable` audit event. Detail values are
   * bounded because they come straight off an untrusted delivery.
   */
  recordChannelEvent(deliveryId: string, eventType: LinearLedgerEventType, detail: Readonly<Record<string, string>> | null = null): void {
    if (ledgerEventTypeAdmitted[eventType] !== true) throw invalid("The Linear ledger event type is not admitted.");
    this.#assertOpen();
    const bounded = detail === null ? null : Object.fromEntries(Object.entries(detail).map(([key, value]) => [key, boundedDetailText(value)]));
    this.#transaction(() => {
      this.#db.prepare("INSERT INTO linear_ledger_events(delivery_id, event_type, detail_json, recorded_at) VALUES (?, ?, ?, ?)")
        .run(deliveryId, eventType, bounded === null ? null : JSON.stringify(bounded), this.#instant());
    });
  }

  /** Every recorded event, oldest first, optionally narrowed to one delivery. */
  channelEvents(deliveryId?: string): readonly LinearLedgerEvent[] {
    this.#assertOpen();
    const rows = deliveryId === undefined
      ? this.#db.prepare("SELECT * FROM linear_ledger_events ORDER BY sequence ASC").all() as Row[]
      : this.#db.prepare("SELECT * FROM linear_ledger_events WHERE delivery_id = ? ORDER BY sequence ASC").all(deliveryId) as Row[];
    return Object.freeze(rows.map((row) => Object.freeze({
      sequence: Number(row.sequence),
      deliveryId: String(row.delivery_id),
      eventType: String(row.event_type) as LinearLedgerEventType,
      detail: row.detail_json === null ? null : Object.freeze(JSON.parse(String(row.detail_json)) as Record<string, string>),
      recordedAt: String(row.recorded_at),
    })));
  }

  // -- internals ------------------------------------------------------------

  #sessionInputRow(row: Row): LinearSessionInputRecord {
    const url = validateCanonicalLinearUserUrl(row.owner_user_url);
    if (url === null) throw invalid("The retained session owner URL is not canonical.");
    return Object.freeze({
      sourceEventId: String(row.source_event_id),
      agentSessionId: String(row.agent_session_id),
      commandId: row.command_id === null ? null : String(row.command_id),
      activityId: row.activity_id === null ? null : String(row.activity_id),
      sourceCommentId: row.source_comment_id === null ? null : String(row.source_comment_id),
      inputDigest: String(row.input_digest),
      turnOrdinal: Number(row.turn_ordinal),
      owner: Object.freeze({ userId: String(row.owner_user_id), userUrl: url }),
    });
  }

  #startupRow(row: Row): LinearStartupRecord {
    return Object.freeze({
      agentSessionId: String(row.agent_session_id),
      logicalEventKey: String(row.logical_event_key),
      startupIntentDigest: String(row.startup_intent_digest),
      disposition: String(row.disposition) as LinearStartupDisposition,
      commandId: row.command_id === null ? null : String(row.command_id),
      inputSourceEventId: row.input_source_event_id === null ? null : String(row.input_source_event_id),
      failureStage: row.failure_stage === null ? null : String(row.failure_stage),
      failureReason: row.failure_reason === null ? null : String(row.failure_reason),
    });
  }

  #commentInvocationRow(row: Row): LinearCommentInvocationRecord {
    return Object.freeze({
      sourceCommentId: String(row.source_comment_id),
      rootCommentId: String(row.root_comment_id),
      agentSessionId: String(row.agent_session_id),
      activityId: row.activity_id === null ? null : String(row.activity_id),
      deliveryId: String(row.delivery_id),
      logicalEventKey: String(row.logical_event_key),
      disposition: String(row.disposition) as "accepted" | "rejected",
      commandId: row.command_id === null ? null : String(row.command_id),
      instructionDigest: String(row.instruction_digest),
      safeCode: row.safe_code === null ? null : String(row.safe_code),
      responseSourceEventId: row.response_source_event_id === null ? null : String(row.response_source_event_id),
    });
  }

  #decisionOutcomeRow(row: Row): LinearDecisionOutcomeRecord {
    return Object.freeze({
      decisionId: String(row.decision_id),
      agentSessionId: String(row.agent_session_id),
      outcome: String(row.outcome) as LinearDecisionOutcomeKind,
      sourceEventId: String(row.source_event_id),
      activityId: String(row.activity_id),
      ownerUserId: String(row.owner_user_id),
      selectedOptionId: row.selected_option_id === null ? null : String(row.selected_option_id),
      selectedLabel: row.selected_label === null ? null : String(row.selected_label),
    });
  }

  #retainedStartup(agentSessionId: string, logicalEventKey: string): LinearStartupRecord | null {
    const row = this.#db.prepare("SELECT * FROM linear_ledger_startups WHERE agent_session_id = ? OR logical_event_key = ?")
      .get(agentSessionId, logicalEventKey) as Row | undefined;
    return row === undefined ? null : this.#startupRow(row);
  }

  #highestTurnOrdinal(agentSessionId: string): number {
    const row = this.#db.prepare("SELECT COALESCE(MAX(turn_ordinal), 0) AS latest FROM linear_ledger_session_inputs WHERE agent_session_id = ?")
      .get(agentSessionId) as { latest: number };
    return Number(row.latest);
  }

  #nextTurnOrdinal(agentSessionId: string): number {
    return this.#highestTurnOrdinal(agentSessionId) + 1;
  }

  #instant(): string {
    return this.#now().toISOString();
  }

  #assertOpen(): void {
    if (this.#closed) throw unavailable("The Linear ledger database is closed.");
  }

  /** Creates the schema on a database with no Linear ledger objects, and refuses one at any other version. */
  #initializeSchema(): void {
    const version = this.#recordedSchemaVersion();
    if (version === null) {
      this.#transaction(() => {
        for (const definition of Object.values(schemaDefinitions)) this.#db.exec(definition);
        this.#recordSchemaVersion(LINEAR_LEDGERS_SCHEMA_VERSION);
      });
      return;
    }
    if (version !== LINEAR_LEDGERS_SCHEMA_VERSION) {
      throw new RelayError("axis.relay.schema_version_unsupported", "The Linear ledger database does not match the current schema version.", 500);
    }
  }

  /**
   * The schema version this database's ledger records, or `null` when it holds none
   * of this module's tables (a fresh file, or one with only `./inbox.ts`'s
   * tables). Tables without a ledger are refused rather than adopted.
   */
  #recordedSchemaVersion(): number | null {
    const tables = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE name LIKE 'linear_ledger%' LIMIT 1").get();
    if (tables === undefined) return null;
    const ledger = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'linear_ledger_schema'").get();
    if (ledger === undefined) throw unavailable(LINEAR_LEDGERS_UNLEDGERED_MESSAGE);
    const version = schemaLedgerVersion(this.#db.prepare("SELECT version FROM linear_ledger_schema").all() as Row[]);
    if (version === null) throw unavailable("The Linear ledger database's schema ledger is unreadable.");
    return version;
  }

  #recordSchemaVersion(version: number): void {
    this.#db.prepare("INSERT INTO linear_ledger_schema(version, applied_at) VALUES (?, ?)").run(version, this.#instant());
  }

  #transaction<T>(operation: () => T): T {
    if (this.#inTransaction) return operation();
    try {
      this.#db.exec("BEGIN IMMEDIATE");
    } catch {
      throw unavailable("The Linear ledger could not begin a transaction.");
    }
    this.#inTransaction = true;
    try {
      const result = operation();
      this.#db.exec("COMMIT");
      return result;
    } catch (error) {
      try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ }
      if (error instanceof RelayError) throw error;
      throw unavailable("The Linear ledger could not complete a durable write.");
    } finally {
      this.#inTransaction = false;
    }
  }

  #assertInvariants(): void {
    try { this.#assertInvariantsUnsafe(); }
    catch (error) {
      if (error instanceof RelayError) throw error;
      throw unavailable("The Linear ledger database could not be verified.");
    }
  }

  #assertInvariantsUnsafe(): void {
    this.#assertScanInvariants();
    this.#assertSchemaInvariants();
  }

  /** The whole-database scans: run at construction and on diagnostics, never on readiness. */
  #assertScanInvariants(): void {
    const integrity = this.#db.prepare("PRAGMA integrity_check").get() as { integrity_check?: string } | undefined;
    if (integrity?.integrity_check !== "ok") throw unavailable("The Linear ledger database failed its integrity check.");
    if (this.#db.prepare("PRAGMA foreign_key_check").all().length !== 0) throw unavailable("The Linear ledger database holds an orphaned reference.");
  }

  /**
   * The schema half: `sqlite_master` plus the schema ledger, both bounded
   * by the number of schema objects rather than by the rows they describe.
   */
  #assertSchemaInvariants(): void {
    // Scoped to this module's prefix: the shared file also holds `./inbox.ts`'s tables.
    const objects = this.#db.prepare("SELECT type, name, sql FROM sqlite_master WHERE (name LIKE 'linear_ledger%' OR tbl_name LIKE 'linear_ledger%') AND name NOT GLOB 'sqlite_*'").all() as Row[];
    if (!schemaObjectsMatch(objects, schemaFingerprints)) throw unavailable(LINEAR_LEDGERS_SCHEMA_DRIFT_MESSAGE);
    // Single-current-version check; see `schemaLedgerAgrees`.
    const ledger = this.#db.prepare("SELECT version, applied_at FROM linear_ledger_schema").all() as Row[];
    if (!schemaLedgerAgrees(ledger, LINEAR_LEDGERS_SCHEMA_VERSION)) throw unavailable(LINEAR_LEDGERS_LEDGER_MESSAGE);
  }
}

/**
 * The neutral run chain read this module needs: which decision, if any, the
 * conversation is currently waiting on. Declared structurally so nothing here
 * imports a store it could write through.
 */
export interface LinearOpenDecisionLookup {
  openDecision(input: { readonly conversationId: string }): { readonly decisionId: string; readonly runId: string } | null;
  /** The conversation's current Run; its `commandId` keys the input whose owner was frozen. */
  currentRun(input: { readonly conversationId: string }): { readonly runId: string; readonly commandId: string } | null;
}

/** One elicitation as the neutral outbox recorded it when the Run raised it. */
export interface LinearRaisedDecision {
  /** The elicitation's own content-derived id, which is what this module keys outcomes on. */
  readonly decisionId: string;
  /** The Run that raised it. The only key that joins to the run chain's decision row. */
  readonly runId: string;
  readonly elicitation: StructuredElicitation;
}

/** The neutral outbox read: every decision ever raised on a conversation. */
export interface LinearElicitationLookup {
  waitingDecisionsForConversation(conversationId: string): readonly LinearRaisedDecision[];
}

/**
 * The elicitation a given Run raised, or null. Joined on `runId`: the run chain
 * and the outbox mint decision ids independently, so a `decisionId` lookup
 * would silently find nothing.
 */
export function linearRaisedDecisionForRun(outbox: LinearElicitationLookup, agentSessionId: string, runId: string): LinearRaisedDecision | null {
  return outbox.waitingDecisionsForConversation(agentSessionId).find((raised) => raised.runId === runId) ?? null;
}

/**
 * The `LinearDecisionLedger` `./instruction.ts` consumes, joined from three
 * stores: the run chain (which decision is open), the outbox (what it said)
 * and this ledger (how it ended, and who owns it). For Linear,
 * `conversationId === agentSessionId`.
 *
 * `matchesReservedDecisionOptionLabel` reads every decision ever raised, from
 * the outbox's append-only rows, so a decision cancelled before it was
 * answered keeps its labels reserved and a late `Yes` is refused as a stale
 * reply rather than dispatched as work.
 */
export function linearDecisionLedger(ports: {
  readonly ledger: SqliteLinearLedgers;
  readonly runChain: LinearOpenDecisionLookup;
  readonly outbox: LinearElicitationLookup;
}): LinearDecisionLedger {
  const openClaim = (agentSessionId: string): LinearDecisionClaim | null => {
    const open = ports.runChain.openDecision({ conversationId: agentSessionId });
    if (open === null) return null;
    const raised = linearRaisedDecisionForRun(ports.outbox, agentSessionId, open.runId);
    if (raised === null) return null;
    // A decision this ledger already finished is not open, whatever the run
    // chain says: the two stores commit separately.
    if (ports.ledger.decisionOutcome(raised.decisionId) !== null) return null;
    const owner = linearDecisionOwner(ports, agentSessionId, open.runId);
    if (owner === null) return null;
    return Object.freeze({ decisionId: raised.decisionId, ownerUserId: owner.userId, ownerUserUrl: owner.userUrl, elicitation: raised.elicitation });
  };
  return Object.freeze({
    decisionClaimForReply(agentSessionId: string, activityId: string, sourceCommentId: string | null, replyValue: string): LinearDecisionClaim | null {
      const pending = openClaim(agentSessionId);
      if (pending !== null && selectElicitationReply(pending.elicitation, replyValue) !== null) return pending;
      const resolved = ports.ledger.resolvedDecisionByActivity(agentSessionId, activityId);
      if (resolved === null) return null;
      // The replay must arrive through the same channel the original answer
      // did; see `commentSourceForDelivery`.
      if (ports.ledger.commentSourceForDelivery(resolved.sourceEventId, agentSessionId) !== sourceCommentId) return null;
      const elicitation = ports.outbox.waitingDecisionsForConversation(agentSessionId)
        .find((raised) => raised.decisionId === resolved.decisionId)?.elicitation ?? null;
      if (elicitation === null) return null;
      const option = selectElicitationReply(elicitation, replyValue);
      if (option === null || option.optionId !== resolved.selectedOptionId) return null;
      // Judged against the owner of the input that originally answered, not the
      // session's current owner.
      const owner = ports.ledger.sessionInput(resolved.sourceEventId)?.owner ?? null;
      if (owner === null) return null;
      return Object.freeze({ decisionId: resolved.decisionId, ownerUserId: owner.userId, ownerUserUrl: owner.userUrl, elicitation });
    },
    pendingDecision(agentSessionId: string): LinearDecisionClaim | null {
      return openClaim(agentSessionId);
    },
    /**
     * The decision a free-text reply from this Activity is interpreted against,
     * open or replayed. The open arm is `openClaim`; the replay arm mirrors
     * `decisionClaimForReply`'s, with the same channel and original-owner checks.
     */
    interpretationClaim(agentSessionId: string, activityId: string, sourceCommentId: string | null): LinearDecisionClaim | null {
      // The replay arm goes first. By the time a redelivery arrives the agent may
      // have raised a re-ask, and binding the old body to that new open question
      // would be refused `command_rebound`. Prose matches any question, so unlike
      // a token reply nothing else tells the two apart.
      const interpreted = ports.ledger.interpretedDecisionByActivity(agentSessionId, activityId);
      if (interpreted !== null) {
        if (ports.ledger.commentSourceForDelivery(interpreted.sourceEventId, agentSessionId) !== sourceCommentId) return null;
        const elicitation = ports.outbox.waitingDecisionsForConversation(agentSessionId)
          .find((raised) => raised.decisionId === interpreted.decisionId)?.elicitation ?? null;
        if (elicitation === null) return null;
        const owner = ports.ledger.sessionInput(interpreted.sourceEventId)?.owner ?? null;
        if (owner === null) return null;
        return Object.freeze({ decisionId: interpreted.decisionId, ownerUserId: owner.userId, ownerUserUrl: owner.userUrl, elicitation });
      }
      // A question with an Other option is never interpreted: its prose is a
      // custom answer. Using `customAnswerForPendingDecision`'s predicate keeps
      // the two paths disjoint regardless of call-site order.
      const pending = openClaim(agentSessionId);
      return pending === null || pending.elicitation.other !== undefined ? null : pending;
    },
    matchesReservedDecisionOptionLabel(agentSessionId: string, reply: string): boolean {
      for (const raised of ports.outbox.waitingDecisionsForConversation(agentSessionId)) {
        if (raised.elicitation.options.some((option) => option.label === reply)) return true;
        if (raised.elicitation.other !== undefined && reply === "Other") return true;
      }
      return false;
    },
  });
}

/**
 * The owner of one waiting decision: the human frozen on the input whose
 * command created the Run the decision was raised on. Never the session's
 * current owner: if A's Run parks on a question and B then prompts, only A may
 * answer it.
 *
 * `currentRun` gives the Run's immutable `commandId`, which keys the session
 * input whose owner was frozen. Returns null when any link is missing, so the
 * decision is unanswerable.
 */
export function linearDecisionOwner(
  ports: { readonly ledger: SqliteLinearLedgers; readonly runChain: LinearOpenDecisionLookup },
  agentSessionId: string,
  runId: string,
): VerifiedLinearOwner | null {
  const run = ports.runChain.currentRun({ conversationId: agentSessionId });
  // Asserted rather than assumed: an owner from a different Run is exactly the defect.
  if (run === null || run.runId !== runId) return null;
  return ports.ledger.commandOwner(run.commandId);
}

/**
 * The `LinearDecisionOwnerLookup` `./activity.ts` declares, resolved through
 * the open decision's Run so the rendered question addresses the human the
 * reply side will accept.
 */
export function linearDecisionOwnerLookup(ports: {
  readonly ledger: SqliteLinearLedgers;
  readonly runChain: LinearOpenDecisionLookup;
}): { decisionOwner(agentSessionId: string): VerifiedLinearOwner | null } {
  return Object.freeze({
    decisionOwner(agentSessionId: string) {
      const open = ports.runChain.openDecision({ conversationId: agentSessionId });
      return open === null ? null : linearDecisionOwner(ports, agentSessionId, open.runId);
    },
  });
}

/**
 * `LinearChannelTaskRefLookup` from the session table, which retains the
 * `issueId` and `teamId` the run chain's conversation scope lacks. Null for a
 * conversation never bound here, which the observe side records as a failure.
 */
export function linearChannelTaskRefLookup(ledger: SqliteLinearLedgers): LinearChannelTaskRefLookup {
  return Object.freeze({
    taskRef(input: { readonly conversationId: string }) {
      const session = ledger.session(input.conversationId);
      return session === null ? null : Object.freeze({
        organizationId: session.organizationId,
        issueId: session.issueId,
        teamId: session.teamId,
        agentSessionId: session.agentSessionId,
      });
    },
  });
}

/**
 * `LinearRedispatchSourceLookup` from the session input the interrupted command
 * carried, keyed on the marker's own `commandId`.
 *
 * Returns identity, not content: the redispatch trigger re-fetches the Activity
 * and digests it itself, which is what gives `instruction_changed` meaning.
 * `expectedDecisionId` is non-null for a decision-bound input, which is not
 * redispatchable. Null for a command with no input, from another conversation,
 * or with no Activity.
 */
export function linearRedispatchSourceLookup(ledger: SqliteLinearLedgers): LinearRedispatchSourceLookup {
  return Object.freeze({
    redispatchSource(input: { readonly conversationId: string; readonly commandId: string }) {
      const record = ledger.sessionInputByCommand(input.commandId);
      if (record === null || record.agentSessionId !== input.conversationId || record.activityId === null) return null;
      // Every input has a session binding, so null here is defensive.
      const session = ledger.session(record.agentSessionId);
      if (session === null) return null;
      return Object.freeze({
        agentSessionId: record.agentSessionId,
        activityId: record.activityId,
        ownerUserId: record.owner.userId,
        sourceCommentId: record.sourceCommentId,
        expectedDecisionId: ledger.decisionBoundToInput(record.sourceEventId),
        appUserId: session.appUserId,
        issueId: session.issueId,
        rootCommentId: session.rootCommentId,
      });
    },
  });
}
