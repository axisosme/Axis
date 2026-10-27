import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import { validateRuntimeTransition } from "@axisosme/axis-contracts/runtime";
import type { RuntimeState, RuntimeTaskContinuationContext } from "@axisosme/axis-contracts/runtime";

import type { ChannelCommandKind, ChannelCommandOutcome, ChannelCommandPort } from "./channel-command.js";
import { channelReconcileQuery } from "./channel-command.js";
import { RETRY_BUDGET_EXHAUSTED_CODE, RETRY_BUDGET_MAX_ATTEMPTS, isRetryBudget, retryBackoffDelayMs, retryVerdict } from "../shared/retry-budget.js";
import {
  CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
  CHANNEL_RUN_SETTLED_STATES,
  CHANNEL_RUN_TERMINAL_STATES,
  ChannelSessionInputError,
  ChannelSessionRefusalError,
  channelConversationScope,
  channelMessageUniquenessDomain,
  channelDigest,
  channelRedispatchEligible,
  isChannelRuntimeContractVersion,
} from "./channel-conversation-types.js";
import type {
  ChannelConversation,
  ChannelRuntimeContractVersion,
  ChannelSessionRefusalCode,
  ChannelConversationScope,
  ChannelDigest,
  ChannelRedispatchMarker,
  ChannelRunChainEntry,
  ChannelRunChainPort,
  ChannelRunChainReconcileOutcome,
  ChannelRunChainReservation,
  ChannelRunChainReservationReceipt,
  ChannelRunChainReservationStatus,
  ChannelRuntimeTask,
  ChannelWaitingDecision,
  ChannelWaitingDecisionReceipt,
  ChannelWaitingDecisionStatus,
} from "./channel-conversation-types.js";
import type { System, TenantRef } from "./channel-identity.js";
import { canonical, durableStoreInput, frozen, isObject, openDurableStoreConnection, type DurableStoreConnection } from "../shared/durable-store.js";
import { schemaFingerprintTable, schemaLedgerAgrees, schemaLedgerVersion, schemaObjectsMatch } from "../shared/schema-ledger.js";
import { validateWorkerSelection } from "../shared/worker-selection.js";
import type { WorkerSelection } from "../shared/worker-selection.js";

/**
 * The channel-neutral Run chain behind `ChannelRunChainPort`: within one
 * conversation, which Runs happened, in what order, which one is current, which
 * waiting decision is open, and which command may move that state now.
 * `conversationId` is the join key.
 *
 * Router owns execution; this store owns association. Run facts arrive from
 * outside (ids from a dispatch outcome, `RuntimeState` from a lifecycle event),
 * and `recordRunState` defers to the contract's `validateRuntimeTransition`.
 *
 * A new instruction while a Run executes:
 *
 * - `reserve` for a `continue` on a non-terminal Run records `interruptRunId`
 *   and no Run row.
 * - `settle` with `interrupt_requested` records a redispatch marker carrying the
 *   command identity and instruction digest, never the instruction text.
 * - `reconcile` reports an unsettled reservation as `unknown`; with
 *   `unconfirmedAfterMs` it is settled `rejected` with
 *   `axis.channel.outcome_unconfirmed` once that window passes.
 * - `reconcileStale` / `reconcileNow` sweep reservations stranded by a crash.
 *
 * Conversations are keyed by the same `(scope_system, scope_domain,
 * scope_selector)` triple as `./channel-conversation.ts`. Tables use the
 * `channel_run_chain_` prefix so both stores can share one database file.
 */

/**
 * This store's own persistence failures. Refusals in the contract's vocabulary
 * are thrown as `ChannelSessionRefusalError`.
 */
export type ChannelRunChainStoreErrorCode = "schema_version_unsupported" | "persisted_state_conflict";

const errorMessages: Readonly<Record<ChannelRunChainStoreErrorCode, string>> = {
  schema_version_unsupported: "Channel run chain database is not at the schema version this build opens.",
  persisted_state_conflict: "Channel run chain persisted state conflict.",
};

export class ChannelRunChainStoreError extends Error {
  readonly code: ChannelRunChainStoreErrorCode;
  constructor(code: unknown) {
    const normalized: ChannelRunChainStoreErrorCode = typeof code === "string" && Object.hasOwn(errorMessages, code) ? code as ChannelRunChainStoreErrorCode : "persisted_state_conflict";
    super(errorMessages[normalized]);
    this.code = normalized;
    this.name = "ChannelRunChainStoreError";
  }
}
class ChannelRunChainInputError extends TypeError { constructor() { super("Channel run chain input is malformed."); } }
function malformed(): never { throw new ChannelRunChainInputError(); }
/**
 * Runs a contract validator and converts its `ChannelSessionInputError` into
 * this store's `TypeError`, so malformed input is always a `TypeError`.
 */
function validated<T>(derive: () => T): T {
  try { return derive(); }
  catch (error) { if (error instanceof ChannelSessionInputError) malformed(); throw error; }
}

/** The reservation statuses the row parser accepts. */
const reservationStatuses: readonly ChannelRunChainReservationStatus[] = Object.freeze(["reserved", "accepted", "interrupt_requested", "steered", "rejected", "resolved_locally"]);

/** The part of the conversation registry this store reads. */
export interface ChannelConversationLookup {
  current(input: { readonly scope: ChannelConversationScope }): ChannelConversation | null;
}

export interface ChannelConversationDomain {
  readonly system: System;

  readonly domain: string;
}

/** One conversation `openRunConversations` reports as still needing observation. */
export interface ChannelRunChainOpenConversation {
  readonly conversationId: string;
  readonly scope: ChannelConversationDomain;
  readonly tenant: TenantRef;
  readonly taskId: string;
  readonly currentRunId: string;
}

/** One conversation this store has ever bound, for a caller sweeping all of them. */
export interface ChannelRunChainBoundConversation {
  readonly conversationId: string;
  readonly scope: ChannelConversationDomain;
  readonly tenant: TenantRef;
}

/** The locally held state of one bound conversation. */
export interface ChannelRunChainConversationState {
  readonly scope: ChannelConversationDomain;
  readonly tenant: TenantRef;

  readonly sourceIssueId: string | null;
  readonly taskId: string | null;
  readonly correlationId: string | null;
  readonly currentRunId: string | null;
  readonly pendingCommandId: string | null;
}

/** The Runtime `SourceRef` narrowed to what this store compares it against. */
interface ChannelTaskSource {
  readonly system: System;

  readonly domain: string;

  readonly conversationRef: string;

  readonly issueId: string;
}

/** What `reconcileStale` or `reconcileNow` did with one stranded reservation. */
export type ChannelRunChainSweepDisposition =
  /** Router had accepted the interrupt; the reservation is settled and its redispatch marker restored. */
  | "interrupt_requested"
  /** Router handed the instruction to the running Run; settled `steered`, with no marker and no Run. */
  | "steered"
  /** Router never accepted the command; the reservation is settled `rejected` and the slot released. Never replayed. */
  | "absent"
  /** Router's retained accepted lineage settled the reservation. */
  | "accepted"
  /** Accepted, but this adapter could not resolve retained lineage. Left untouched. */
  | "accepted_unresolved"
  /** Router cannot say what happened. Left untouched: not settled, not cleared, not replayed. */
  | "unknown"
  /**
   * Router still cannot say what happened past the unconfirmed window.
   * Settled `rejected` with `axis.channel.outcome_unconfirmed` and the slot
   * released. Never replayed.
   */
  | "unconfirmed"
  /**
   * Router answered, but settling with it was refused. The reservation is left
   * as it was and `code` carries the refusal; the rest of the sweep still runs.
   */
  | "settle_refused";

export interface ChannelRunChainSweepEntry {
  readonly commandId: string;
  readonly disposition: ChannelRunChainSweepDisposition;
  /** Present only for `settle_refused`: the refusal `settle` raised. */
  readonly code?: ChannelSessionRefusalCode;
}

export interface ChannelRunChainSweepResult {
  /** How many stale reservations this sweep looked at, including ones it left alone. */
  readonly examined: number;
  readonly entries: readonly ChannelRunChainSweepEntry[];
}

/**
 * How long a reservation must sit in `reserved` before a sweep may touch it.
 * Far longer than any single Router dispatch, so the sweeper never touches a
 * command that is being submitted.
 */
export const DEFAULT_STALE_RESERVATION_MS = 300_000;
/**
 * How long a reservation may sit `reserved` and answer `unknown` before a sweep
 * settles it terminal. Thirty minutes, far longer than the Node outbox retry
 * ceiling, so a live command is never mistaken for a stranded one. Overridable
 * per composition (`LinearChannelOptions.unconfirmedReservationMs`) and per
 * `reconcileStale({ unconfirmedAfterMs })` call; no environment variable reads it.
 */
export const DEFAULT_UNCONFIRMED_RESERVATION_MS = 1_800_000;
/** One sweep's page size: bounds the port calls and `settle` transactions one sweep performs. */
const DEFAULT_SWEEP_LIMIT = 20;
/**
 * The `safeCode` a confirmed-absent dispatch settles with, mirroring Linear's
 * own `axis.remote.confirmed_absent` under the neutral namespace.
 */
const CONFIRMED_ABSENT_SAFE_CODE = "axis.channel.confirmed_absent";
/**
 * The `safeCode` a reservation settles with when the unconfirmed window passes
 * with Router still answering `unknown`. It asserts nothing about whether the
 * command ran, only that Axis stopped waiting.
 */
export const OUTCOME_UNCONFIRMED_SAFE_CODE = "axis.channel.outcome_unconfirmed";
/** How far `holdRedispatchAttempt` spaces the next attempt at a redispatch a draining Node refused. */
const REDISPATCH_DRAIN_HOLD_MS = 60_000;

export interface SqliteChannelRunChainStoreOptions {
  readonly conversations: ChannelConversationLookup;
  readonly filename?: string;
  readonly now?: () => Date;
  readonly id?: () => string;
  readonly busyTimeoutMs?: number;
  /**
   * How many attempts one queued redispatch gets before `claimRedispatch` hands
   * out the obligation to quarantine it. Defaults to `RETRY_BUDGET_MAX_ATTEMPTS`.
   */
  readonly maxAttempts?: number;
}

type Row = Record<string, unknown>;
/**
 * The contract's terminal Run states, including `waiting_for_input`. Governs
 * whether a continuation may chain a successor; the redispatch condition is
 * stricter, see `pendingRedispatch`.
 */
const terminalStates: readonly RuntimeState[] = CHANNEL_RUN_TERMINAL_STATES;
/** A conversation that is genuinely idle: nothing executing and nothing awaiting an answer. */
const settledStates: readonly RuntimeState[] = CHANNEL_RUN_SETTLED_STATES;

/** Every continuation-context shape `continuationContext` may return, one per admitted contract version. */
export type ChannelContinuationContext = RuntimeTaskContinuationContext;

/** The operator listing's page ceiling, matching every `axis` listing's `--limit <1..100>`. */
export const CHANNEL_DISPOSITION_PAGE_LIMIT = 100;

/**
 * One recorded redispatch give-up: identity and disposition only, no instruction
 * body or digest. `failureCode` is the shared retry policy's own code.
 */
export interface ChannelRedispatchDisposition {
  readonly sequence: number;
  readonly conversationId: string;
  readonly commandId: string;
  readonly attempts: number;
  readonly failureCode: typeof RETRY_BUDGET_EXHAUSTED_CODE;
  readonly givenUpAt: string;
}

/** Narrows a persisted give-up code to the only one this build writes, refusing anything else as damaged state. */
function retryBudgetExhaustedCode(value: unknown): typeof RETRY_BUDGET_EXHAUSTED_CODE {
  if (value !== RETRY_BUDGET_EXHAUSTED_CODE) throw new ChannelRunChainStoreError("persisted_state_conflict");
  return RETRY_BUDGET_EXHAUSTED_CODE;
}

/**
 * What `claimRedispatch` hands back: an attempt on the queued redispatch, or the
 * obligation to give up on it. Both carry the marker, so the caller can address
 * the user-visible result and discharge the obligation against the exact command.
 */
export type ChannelRedispatchClaim =
  /** This attempt is within budget. `attempt` is 1-based and counts this one; `remaining` is what is left after it. */
  | { readonly disposition: "attempt"; readonly marker: ChannelRedispatchMarker; readonly attempt: number; readonly remaining: number }
  /**
   * The budget is spent. No further attempt will be handed out; the caller must
   * produce a user-visible result and then call `quarantineRedispatch`. Until
   * it does, this same obligation is reissued.
   */
  | { readonly disposition: "quarantine"; readonly marker: ChannelRedispatchMarker; readonly attempts: number; readonly maxAttempts: number };

/**
 * What one queued redispatch has spent. `quarantinedAt` non-null means Axis has
 * stopped attempting it; the marker is retained.
 */
export interface ChannelRedispatchRetryState {
  readonly commandId: string;
  readonly attemptCount: number;
  /** When the next attempt becomes available, or null before the first one. */
  readonly nextAttemptAt: string | null;
  /** When Axis gave up, or null while it is still attempting. */
  readonly quarantinedAt: string | null;
}

const schemaDefinitions = Object.freeze({
  // The CHECK list is the persisted half of `CHANNEL_RUNTIME_CONTRACT_VERSIONS`; the two must agree.
  "table:channel_run_chain_runtime_versions": "CREATE TABLE channel_run_chain_runtime_versions(conversation_id TEXT PRIMARY KEY, runtime_contract_version TEXT NOT NULL CHECK(runtime_contract_version = '1.0.0'), FOREIGN KEY(conversation_id) REFERENCES channel_run_chain_conversations(conversation_id))",
  "trigger:channel_run_chain_runtime_versions_immutable_update": "CREATE TRIGGER channel_run_chain_runtime_versions_immutable_update BEFORE UPDATE ON channel_run_chain_runtime_versions BEGIN SELECT RAISE(ABORT, 'channel runtime version is immutable'); END",
  "trigger:channel_run_chain_runtime_versions_immutable_delete": "CREATE TRIGGER channel_run_chain_runtime_versions_immutable_delete BEFORE DELETE ON channel_run_chain_runtime_versions BEGIN SELECT RAISE(ABORT, 'channel runtime version is immutable'); END",
  "table:channel_run_chain_schema": "CREATE TABLE channel_run_chain_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
  "table:channel_run_chain_conversations": "CREATE TABLE channel_run_chain_conversations(conversation_id TEXT PRIMARY KEY, scope_system TEXT NOT NULL CHECK(scope_system IN ('linear')), scope_domain TEXT NOT NULL, tenant_id TEXT NOT NULL, source_issue_id TEXT, task_id TEXT, correlation_id TEXT, current_run_id TEXT, pending_command_id TEXT UNIQUE, contract_version TEXT NOT NULL, created_at TEXT NOT NULL, CHECK(correlation_id IS NULL OR task_id IS NOT NULL))",
  // `worker_selection_json` carries a start's policy selection; only a start
  // carries one.
  "table:channel_run_chain_reservations": "CREATE TABLE channel_run_chain_reservations(sequence INTEGER PRIMARY KEY AUTOINCREMENT, command_id TEXT NOT NULL UNIQUE, conversation_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('start', 'continue', 'cancel')), intent_digest TEXT NOT NULL, instruction_digest TEXT, worker_selection_json TEXT, prior_run_id TEXT, interrupt_run_id TEXT, decision_id TEXT, status TEXT NOT NULL CHECK(status IN ('reserved', 'accepted', 'interrupt_requested', 'steered', 'rejected', 'resolved_locally')), run_id TEXT, task_id TEXT, safe_code TEXT, contract_version TEXT NOT NULL, reserved_at TEXT NOT NULL, settled_at TEXT, FOREIGN KEY(conversation_id) REFERENCES channel_run_chain_conversations(conversation_id), CHECK(worker_selection_json IS NULL OR kind = 'start'), CHECK(status = 'reserved' AND run_id IS NULL AND task_id IS NULL AND safe_code IS NULL AND settled_at IS NULL OR status IN ('accepted', 'interrupt_requested', 'steered') AND run_id IS NOT NULL AND task_id IS NOT NULL AND safe_code IS NULL AND settled_at IS NOT NULL OR status = 'rejected' AND run_id IS NULL AND task_id IS NULL AND safe_code IS NOT NULL AND settled_at IS NOT NULL OR status = 'resolved_locally' AND run_id IS NULL AND task_id IS NULL AND safe_code IS NULL AND settled_at IS NOT NULL))",
  "table:channel_run_chain_runs": "CREATE TABLE channel_run_chain_runs(sequence INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL UNIQUE, conversation_id TEXT NOT NULL, prior_run_id TEXT UNIQUE, task_id TEXT NOT NULL, command_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL CHECK(state IN ('pending', 'running', 'waiting_for_input', 'succeeded', 'failed')), contract_version TEXT NOT NULL, started_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY(conversation_id) REFERENCES channel_run_chain_conversations(conversation_id), FOREIGN KEY(prior_run_id) REFERENCES channel_run_chain_runs(run_id), FOREIGN KEY(command_id) REFERENCES channel_run_chain_reservations(command_id))",
  "table:channel_run_chain_decisions": "CREATE TABLE channel_run_chain_decisions(sequence INTEGER PRIMARY KEY AUTOINCREMENT, decision_id TEXT NOT NULL UNIQUE, conversation_id TEXT NOT NULL, run_id TEXT NOT NULL UNIQUE, elicitation_digest TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('waiting', 'claimed', 'resolved', 'cancelled')), claimed_by_command_id TEXT, contract_version TEXT NOT NULL, raised_at TEXT NOT NULL, settled_at TEXT, FOREIGN KEY(conversation_id) REFERENCES channel_run_chain_conversations(conversation_id), FOREIGN KEY(run_id) REFERENCES channel_run_chain_runs(run_id), CHECK(status = 'waiting' AND claimed_by_command_id IS NULL AND settled_at IS NULL OR status = 'claimed' AND claimed_by_command_id IS NOT NULL AND settled_at IS NULL OR status IN ('resolved', 'cancelled') AND settled_at IS NOT NULL))",
  // `attempt_count` is the budget `../shared/retry-budget.ts` bounds, the same
  // notion as the Linear inbox's `attempt_count`. `next_attempt_at` spaces the
  // attempts. `quarantined_at` stops a spent marker being attempted without
  // deleting it. The CHECK makes "quarantined without an attempt" unrepresentable.
  "table:channel_run_chain_redispatch": "CREATE TABLE channel_run_chain_redispatch(conversation_id TEXT PRIMARY KEY, command_id TEXT NOT NULL UNIQUE, interrupted_run_id TEXT NOT NULL, instruction_digest TEXT NOT NULL, contract_version TEXT NOT NULL, requested_at TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT, quarantined_at TEXT, FOREIGN KEY(conversation_id) REFERENCES channel_run_chain_conversations(conversation_id), FOREIGN KEY(interrupted_run_id) REFERENCES channel_run_chain_runs(run_id), FOREIGN KEY(command_id) REFERENCES channel_run_chain_reservations(command_id), CHECK(quarantined_at IS NULL OR attempt_count > 0))",
  // Where a redispatch give-up is recorded. Separate from the marker because the
  // marker is deleted by a stop, a cancel, a superseding instruction, or an edited
  // comment, and the record must outlive it; `ChannelRedispatchDispositions` reads
  // this table alone and never joins back.
  //
  // Append-only. No acknowledgement columns: a redispatch give-up does not block
  // drain. `failure_code` is written from and validated against
  // `RETRY_BUDGET_EXHAUSTED_CODE`. The listing pages exclusively on `sequence`.
  "table:channel_run_chain_redispatch_dispositions": "CREATE TABLE channel_run_chain_redispatch_dispositions(sequence INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id TEXT NOT NULL, command_id TEXT NOT NULL UNIQUE, attempts INTEGER NOT NULL, failure_code TEXT NOT NULL, given_up_at TEXT NOT NULL, CHECK(attempts > 0))",
  "trigger:channel_run_chain_redispatch_dispositions_immutable_update": "CREATE TRIGGER channel_run_chain_redispatch_dispositions_immutable_update BEFORE UPDATE ON channel_run_chain_redispatch_dispositions BEGIN SELECT RAISE(ABORT, 'channel run chain redispatch dispositions are append-only'); END",
  "trigger:channel_run_chain_redispatch_dispositions_immutable_delete": "CREATE TRIGGER channel_run_chain_redispatch_dispositions_immutable_delete BEFORE DELETE ON channel_run_chain_redispatch_dispositions BEGIN SELECT RAISE(ABORT, 'channel run chain redispatch dispositions are append-only'); END",
  // SQLite does not index a foreign key. `#waitLocallyCancelled` and the
  // invariant sweep's slot check both look up a conversation's reservations by
  // status.
  "index:channel_run_chain_reservations_conversation": "CREATE INDEX channel_run_chain_reservations_conversation ON channel_run_chain_reservations(conversation_id, status)",
  "index:channel_run_chain_runs_conversation": "CREATE INDEX channel_run_chain_runs_conversation ON channel_run_chain_runs(conversation_id, sequence)",
  "index:channel_run_chain_decisions_conversation": "CREATE INDEX channel_run_chain_decisions_conversation ON channel_run_chain_decisions(conversation_id, status)",
  // `worker_selection_json` is part of a start's immutable routing intent.
  "trigger:channel_run_chain_reservations_immutable_identity": "CREATE TRIGGER channel_run_chain_reservations_immutable_identity BEFORE UPDATE ON channel_run_chain_reservations WHEN OLD.sequence IS NOT NEW.sequence OR OLD.command_id IS NOT NEW.command_id OR OLD.conversation_id IS NOT NEW.conversation_id OR OLD.kind IS NOT NEW.kind OR OLD.intent_digest IS NOT NEW.intent_digest OR OLD.instruction_digest IS NOT NEW.instruction_digest OR OLD.worker_selection_json IS NOT NEW.worker_selection_json OR OLD.prior_run_id IS NOT NEW.prior_run_id OR OLD.interrupt_run_id IS NOT NEW.interrupt_run_id OR OLD.reserved_at IS NOT NEW.reserved_at BEGIN SELECT RAISE(ABORT, 'channel run chain reservation intent is immutable'); END",
  "trigger:channel_run_chain_reservations_immutable_delete": "CREATE TRIGGER channel_run_chain_reservations_immutable_delete BEFORE DELETE ON channel_run_chain_reservations BEGIN SELECT RAISE(ABORT, 'channel run chain reservations are append-only'); END",
  "trigger:channel_run_chain_runs_immutable_identity": "CREATE TRIGGER channel_run_chain_runs_immutable_identity BEFORE UPDATE ON channel_run_chain_runs WHEN OLD.sequence IS NOT NEW.sequence OR OLD.run_id IS NOT NEW.run_id OR OLD.conversation_id IS NOT NEW.conversation_id OR OLD.prior_run_id IS NOT NEW.prior_run_id OR OLD.task_id IS NOT NEW.task_id OR OLD.command_id IS NOT NEW.command_id OR OLD.started_at IS NOT NEW.started_at BEGIN SELECT RAISE(ABORT, 'channel run chain run identity is immutable'); END",
  "trigger:channel_run_chain_runs_immutable_delete": "CREATE TRIGGER channel_run_chain_runs_immutable_delete BEFORE DELETE ON channel_run_chain_runs BEGIN SELECT RAISE(ABORT, 'channel run chain runs are append-only'); END",
  "trigger:channel_run_chain_decisions_immutable_identity": "CREATE TRIGGER channel_run_chain_decisions_immutable_identity BEFORE UPDATE ON channel_run_chain_decisions WHEN OLD.sequence IS NOT NEW.sequence OR OLD.decision_id IS NOT NEW.decision_id OR OLD.conversation_id IS NOT NEW.conversation_id OR OLD.run_id IS NOT NEW.run_id OR OLD.elicitation_digest IS NOT NEW.elicitation_digest OR OLD.raised_at IS NOT NEW.raised_at BEGIN SELECT RAISE(ABORT, 'channel waiting decision identity is immutable'); END",
  "trigger:channel_run_chain_decisions_immutable_delete": "CREATE TRIGGER channel_run_chain_decisions_immutable_delete BEFORE DELETE ON channel_run_chain_decisions BEGIN SELECT RAISE(ABORT, 'channel waiting decisions are append-only'); END",
});

const schemaFingerprints = schemaFingerprintTable(schemaDefinitions);

function assertCurrentSchema(database: DatabaseSync): void {
  try {
    const objects = database.prepare("SELECT type, name, sql FROM sqlite_master WHERE (name LIKE 'channel_run_chain%' OR tbl_name LIKE 'channel_run_chain%') AND name NOT GLOB 'sqlite_*'").all() as Row[];
    if (!schemaObjectsMatch(objects, schemaFingerprints)) throw new ChannelRunChainStoreError("persisted_state_conflict");
    const ledger = database.prepare("SELECT version, applied_at FROM channel_run_chain_schema").all() as Row[];
    if (!schemaLedgerAgrees(ledger, CHANNEL_RUN_CHAIN_SCHEMA_VERSION)) throw new ChannelRunChainStoreError("persisted_state_conflict");
  } catch { throw new ChannelRunChainStoreError("persisted_state_conflict"); }
}

/** The schema version this build creates and opens. */
export const CHANNEL_RUN_CHAIN_SCHEMA_VERSION = 1;

const { plainData, opaque } = durableStoreInput(ChannelRunChainInputError);
function optionalOpaque(value: unknown): string | undefined { return value === undefined ? undefined : opaque(value); }
/**
 * Validates a scope through the contract's constructor and projects the domain
 * this store persists, pinned to `channelMessageUniquenessDomain`.
 */
function conversationDomain(value: unknown): ChannelConversationDomain {
  const normalized = validated(() => channelConversationScope(value));
  const domain = normalized.organizationId;
  if (channelMessageUniquenessDomain(normalized) !== `${normalized.system}:${domain}`) throw new ChannelRunChainStoreError("persisted_state_conflict");
  return Object.freeze({ system: normalized.system, domain });
}

/** Re-derives a `TenantRef` rather than trusting the caller's structurally-typed object. */
function tenantRef(value: unknown): TenantRef {
  const data = plainData(value, ["system", "tenantId"]);
  if (data.system !== "linear") malformed();
  return Object.freeze({ system: data.system, tenantId: opaque(data.tenantId) });
}

/** The `sha256:`-prefixed digest form this store persists, validated by the contract's `channelDigest`. */
function digestValue(value: unknown): ChannelDigest {
  return validated(() => channelDigest(value));
}
/** Mirrors the write path's own check, so a damaged value fails closed like every other column. */
function safeCodeValue(value: unknown): string {
  if (typeof value !== "string" || value.trim().length === 0) malformed();
  return value;
}
function commandKind(value: unknown): ChannelCommandKind {
  if (value !== "start" && value !== "continue" && value !== "cancel") malformed();
  return value;
}
function workerSelection(value: unknown): WorkerSelection {
  try { return validateWorkerSelection(value); }
  catch { return malformed(); }
}
/**
 * A stored policy selection, read back. An unparseable value is damaged state,
 * not "no selection".
 */
function parsedWorkerSelectionJson(value: unknown): unknown {
  if (typeof value !== "string") return malformed();
  try { return JSON.parse(value); }
  catch { return malformed(); }
}
function runState(value: unknown): RuntimeState {
  if (value !== "pending" && value !== "running" && value !== "waiting_for_input" && value !== "succeeded" && value !== "failed") malformed();
  return value;
}
function options(value: unknown): SqliteChannelRunChainStoreOptions {
  const data = plainData(value);
  const keys = Object.keys(data), allowed = ["conversations", "filename", "now", "id", "busyTimeoutMs", "maxAttempts"];
  if (keys.some((key) => !allowed.includes(key)) || !isObject(data.conversations) || typeof (data.conversations as { current?: unknown }).current !== "function") malformed();
  if (data.filename !== undefined && typeof data.filename !== "string") malformed();
  if (data.now !== undefined && typeof data.now !== "function") malformed();
  if (data.id !== undefined && typeof data.id !== "function") malformed();
  if (data.busyTimeoutMs !== undefined && (!Number.isSafeInteger(data.busyTimeoutMs) || (data.busyTimeoutMs as number) < 0)) malformed();
  // Refused rather than clamped: a ceiling of zero would quarantine every redispatch on its first claim.
  if (data.maxAttempts !== undefined && (typeof data.maxAttempts !== "number" || !isRetryBudget(data.maxAttempts))) malformed();
  return data as unknown as SqliteChannelRunChainStoreOptions;
}

/**
 * The closed intent a `commandId` is bound to. Presenting the same `commandId`
 * with anything else is `command_rebound`; the first invocation stays authoritative.
 */
export function channelRunChainIntentDigest(input: { readonly kind: ChannelCommandKind; readonly conversationId: string; readonly tenant: TenantRef; readonly commandId: string; readonly instructionDigest?: ChannelDigest; readonly workerSelection?: WorkerSelection }): ChannelDigest {
  const data = plainData(input);
  if (Object.keys(data).some((key) => !["kind", "conversationId", "tenant", "commandId", "instructionDigest", "workerSelection"].includes(key))) malformed();
  const tenant = tenantRef(data.tenant);
  const kind = commandKind(data.kind);
  const selectedWorker = data.workerSelection === undefined ? undefined : workerSelection(data.workerSelection);
  if (selectedWorker !== undefined && kind !== "start") malformed();
  // The hash input is the persisted `intent_digest` shape: these five keys, with
  // `tenantId` as a bare string. Changing it would make every stored reservation
  // refuse its own replay with `command_rebound`.
  const intent = {
    kind, conversationId: opaque(data.conversationId), tenantId: tenant.tenantId, commandId: opaque(data.commandId),
    ...(data.instructionDigest === undefined ? {} : { instructionDigest: digestValue(data.instructionDigest) }),
    ...(selectedWorker === undefined ? {} : { workerSelection: selectedWorker }),
  };
  return `sha256:${createHash("sha256").update(canonical(intent), "utf8").digest("hex")}`;
}

export class SqliteChannelRunChainStore implements ChannelRunChainPort {
  readonly #db: DatabaseSync;
  readonly #conversations: ChannelConversationLookup;
  readonly #now: () => Date;
  readonly #id: () => string;
  readonly #maxAttempts: number;
  readonly #connection: DurableStoreConnection;
  /**
   * Whether the most recent full invariant check failed. `#assertUsable` reads it
   * on every public method, so a store condemned by `verifyIntegrity()` refuses
   * traffic, and `readyForTraffic()` agrees with the operations it predicts.
   * Written only by `#assertInvariants`; it clears when a later full check passes.
   */
  #invariantFault = false;

  constructor(input: SqliteChannelRunChainStoreOptions) {
    const normalized = options(input);
    this.#connection = openDurableStoreConnection(normalized, {
      conflict: () => new ChannelRunChainStoreError("persisted_state_conflict"),
      preserve: (error) => error instanceof ChannelRunChainStoreError || error instanceof ChannelSessionRefusalError,
    });
    this.#db = this.#connection.db;
    this.#conversations = normalized.conversations;
    this.#now = normalized.now ?? (() => new Date());
    this.#id = normalized.id ?? (() => createHash("sha256").update(`${Date.now()}:${Math.random()}`).digest("hex").slice(0, 32));
    this.#maxAttempts = normalized.maxAttempts ?? RETRY_BUDGET_MAX_ATTEMPTS;
    this.#connection.initialize(() => { this.#initializeSchema(); this.#assertInvariants(); });
  }

  close(): void { this.#connection.close(); }
  /**
   * The full health check: both whole-database pragmas, the schema sweep and the
   * cross-table drift queries. Behind `GET /health/diagnostics`; readiness calls
   * `readyForTraffic()`.
   */
  ready(): boolean {
    try { this.verifyIntegrity(); return true; } catch { return false; }
  }

  /**
   * The full check `ready()` reports, thrown instead. Both observe ticks call it at
   * a tick boundary, and it is the entry point for a pre-upgrade or operator check.
   * A damaged run chain fails with `ChannelRunChainStoreError("persisted_state_conflict")`.
   */
  verifyIntegrity(): void { this.#assertOpen(); this.#assertInvariants(); }

  /**
   * Whether this store can serve traffic, at a cost independent of its size: the
   * schema sweep and the fault latch, without the pragmas or data invariants.
   */
  readyForTraffic(): boolean {
    // The same code as every public method's guard, so the probe and the operations
    // cannot disagree. The latch covers damage that leaves the schema intact.
    try { this.#assertUsable(); return true; } catch { return false; }
  }

  /**
   * Reserves the conversation's single command slot and records what this command
   * may do before anything is dispatched. Runs under `BEGIN IMMEDIATE` on a UNIQUE
   * `pending_command_id`, so a concurrent `continue` and `cancel` cannot both
   * proceed; the second is refused `reservation_in_flight`.
   *
   * - `start` requires an empty chain.
   * - `continue` onto a terminal current Run (including `waiting_for_input`)
   *   chains a successor and carries `priorRunId`.
   * - `continue` onto an executing Run carries `interruptRunId` and creates nothing.
   * - `cancel` requires a Run that has not succeeded or failed, or a queued
   *   instruction to suppress.
   *
   * A `continue` on a waiting Run also claims that Run's open decision, atomically
   * with the slot.
   */
  reserve(input: { readonly scope: ChannelConversationScope; readonly conversationId: string; readonly tenant: TenantRef; readonly commandId: string; readonly kind: ChannelCommandKind; readonly instructionDigest?: ChannelDigest; readonly priorRunId?: string; readonly workerSelection?: WorkerSelection }): ChannelRunChainReservationReceipt {
    const data = plainData(input);
    if (Object.keys(data).some((key) => !["scope", "conversationId", "tenant", "commandId", "kind", "instructionDigest", "priorRunId", "workerSelection"].includes(key))
      || !["scope", "conversationId", "tenant", "commandId", "kind"].every((key) => Object.hasOwn(data, key))) malformed();
    const normalizedScope = validated(() => channelConversationScope(data.scope)), conversationId = opaque(data.conversationId), tenant = tenantRef(data.tenant), commandId = opaque(data.commandId), kind = commandKind(data.kind);
    const assertedPriorRunId = optionalOpaque(data.priorRunId);
    const selectedWorker = data.workerSelection === undefined ? undefined : workerSelection(data.workerSelection);
    // A cancel carries no instruction and no continuation assertion.
    if (kind === "cancel" ? data.instructionDigest !== undefined || assertedPriorRunId !== undefined : data.instructionDigest === undefined) malformed();
    if (kind === "start" && assertedPriorRunId !== undefined) malformed();
    if (kind !== "start" && selectedWorker !== undefined) malformed();
    const instructionDigest = data.instructionDigest === undefined ? undefined : digestValue(data.instructionDigest);
    this.#assertUsable();
    return this.#transaction(() => {
      const firstStartRow = kind === "start"
        ? this.#db.prepare("SELECT * FROM channel_run_chain_reservations WHERE conversation_id = ? AND kind = 'start' ORDER BY sequence ASC LIMIT 1").get(conversationId) as Row | undefined
        : undefined;
      const firstStart = firstStartRow === undefined ? undefined : this.#reservationRow(firstStartRow);
      // A first start recorded without a selection has none to inherit; the new start's own selection wins.
      const effectiveWorker = firstStart === undefined || firstStart.commandId === commandId || firstStart.workerSelection === undefined ? selectedWorker : firstStart.workerSelection;
      const intentDigest = channelRunChainIntentDigest({ kind, conversationId, tenant, commandId, ...(instructionDigest === undefined ? {} : { instructionDigest }), ...(effectiveWorker === undefined ? {} : { workerSelection: effectiveWorker }) });
      const replay = this.#reservation(commandId);
      if (replay !== null) {
        if (replay.intentDigest !== intentDigest || replay.conversationId !== conversationId) throw new ChannelSessionRefusalError("command_rebound");
        return frozen({ reservation: replay, replayed: true });
      }
      const conversation = this.#bindConversation(normalizedScope, conversationId, tenant);
      const current = conversation.currentRunId === null ? null : this.#runById(conversation.currentRunId);
      // A conversation whose earlier start or continue was settled
      // `axis.channel.outcome_unconfirmed` accepts no further instruction, since it
      // might duplicate a turn that already ran. The exception is a stop of a Run that
      // is still executing; a parked `waiting_for_input` Run does not qualify. Checked
      // before the slot and per-kind logic.
      if (this.#sessionUnconfirmed(conversationId)
        && !(kind === "cancel" && current !== null && !terminalStates.includes(current.state)))
        throw new ChannelSessionRefusalError("session_unconfirmed");
      if (conversation.pendingCommandId !== null) throw new ChannelSessionRefusalError("reservation_in_flight");
      let priorRunId: string | undefined, interruptRunId: string | undefined, decisionId: string | undefined;
      if (kind === "start") { if (current !== null) throw new ChannelSessionRefusalError("run_state_conflict"); }
      else if (current === null) throw new ChannelSessionRefusalError("run_state_conflict");
      else if (kind === "continue") {
        if (terminalStates.includes(current.state)) {
          // A wait this store already cancelled locally stays `waiting_for_input` forever;
          // an ordinary comment here would chain a new Run onto a conversation the user
          // was told had stopped, so it is refused before any reservation is created.
          if (current.state === "waiting_for_input" && this.#waitLocallyCancelled(conversationId, current.runId)) throw new ChannelSessionRefusalError("wait_cancelled");
          priorRunId = current.runId;
          if (current.state === "waiting_for_input") decisionId = this.#claimDecision(current.runId, commandId);
        } else interruptRunId = current.runId;
      }
      else {
        // A cancel targets the executing Run when there is one; otherwise it is admitted
        // only to suppress a queued redispatch.
        if (!settledStates.includes(current.state)) interruptRunId = current.runId;
        else if (this.#db.prepare("SELECT 1 FROM channel_run_chain_redispatch WHERE conversation_id = ?").get(conversationId) === undefined) throw new ChannelSessionRefusalError("run_state_conflict");
        // An explicit stop suppresses the queued redispatch here, atomically with the
        // stop intent and before any dispatch. At settle time the interrupted Run's
        // terminal could already have made the marker eligible, and a rejected cancel
        // never reaches `#cancelPending`.
        this.#db.prepare("DELETE FROM channel_run_chain_redispatch WHERE conversation_id = ?").run(conversationId);
      }
      // The caller may assert the predecessor it believes it is continuing from; a stale assertion is refused.
      if (assertedPriorRunId !== undefined && assertedPriorRunId !== priorRunId) throw new ChannelSessionRefusalError("prior_run_mismatch");
      const reservedAt = this.#now().toISOString();
      this.#db.prepare(`INSERT INTO channel_run_chain_reservations(command_id, conversation_id, kind, intent_digest, instruction_digest, worker_selection_json, prior_run_id, interrupt_run_id, decision_id, status, run_id, task_id, safe_code, contract_version, reserved_at, settled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'reserved', NULL, NULL, NULL, ?, ?, NULL)`)
        .run(commandId, conversationId, kind, intentDigest, instructionDigest ?? null,
          // `canonical` keeps the stored text key-order stable for replay comparison and the intent digest.
          effectiveWorker === undefined ? null : canonical(effectiveWorker),
          priorRunId ?? null, interruptRunId ?? null, decisionId ?? null, CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, reservedAt);
      this.#db.prepare("UPDATE channel_run_chain_conversations SET pending_command_id = ? WHERE conversation_id = ? AND pending_command_id IS NULL").run(commandId, conversationId);
      const reservation = this.#reservation(commandId);
      if (reservation === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
      return frozen({ reservation, replayed: false });
    });
  }

  /**
   * Settles a reservation with the outcome a Router dispatch returned, applies the
   * transition that outcome authorizes, and releases the slot. Re-presenting the
   * identical outcome is an idempotent replay; a different outcome for a settled
   * reservation is refused.
   *
   * There is no `release` API. A reservation whose settle is refused stays
   * `reserved`, keeps the slot, and reconciles as `unknown` until an accepted
   * outcome arrives or, for a caller that passes `unconfirmedAfterMs`, the
   * unconfirmed window settles it terminal.
   */
  settle(input: { readonly commandId: string; readonly outcome: ChannelCommandOutcome }): ChannelRunChainReservation {
    const data = plainData(input, ["commandId", "outcome"]);
    const commandId = opaque(data.commandId);
    const outcome = this.#outcome(data.outcome);
    if (outcome.commandId !== commandId) malformed();
    this.#assertUsable();
    return this.#transaction(() => {
      const reservation = this.#reservation(commandId);
      if (reservation === null) throw new ChannelSessionRefusalError("unknown_reservation");
      if (reservation.status !== "reserved") {
        const settled = outcome.status === "rejected"
          ? reservation.status === "rejected" && reservation.safeCode === outcome.safeCode
          : reservation.status === outcome.status && reservation.runId === outcome.runId && reservation.taskId === outcome.taskId;
        if (!settled) throw new ChannelSessionRefusalError("reservation_settled");
        return reservation;
      }
      const settledAt = this.#now().toISOString();
      if (outcome.status === "rejected") {
        // Nothing executed, so a claimed decision returns to `waiting`. After
        // `OUTCOME_UNCONFIRMED_SAFE_CODE` it is cancelled instead, since a second answer
        // could duplicate a turn that may have run.
        if (reservation.decisionId !== undefined) {
          if (outcome.safeCode === OUTCOME_UNCONFIRMED_SAFE_CODE)
            this.#db.prepare("UPDATE channel_run_chain_decisions SET status = 'cancelled', claimed_by_command_id = NULL, settled_at = ? WHERE decision_id = ? AND status = 'claimed'").run(settledAt, reservation.decisionId);
          else
            this.#db.prepare("UPDATE channel_run_chain_decisions SET status = 'waiting', claimed_by_command_id = NULL WHERE decision_id = ? AND status = 'claimed'").run(reservation.decisionId);
        }
        this.#db.prepare("UPDATE channel_run_chain_reservations SET status = 'rejected', safe_code = ?, settled_at = ? WHERE command_id = ?").run(outcome.safeCode, settledAt, commandId);
      } else {
        this.#bindTask(reservation.conversationId, outcome.taskId);
        if (outcome.status === "interrupt_requested") {
          // A suppression-only cancel reserved no Run; every other reservation must name the Run it reserved.
          if (reservation.interruptRunId !== undefined && reservation.interruptRunId !== outcome.runId) throw new ChannelSessionRefusalError("run_state_conflict");
          if (reservation.kind === "continue") {
            // A continue always reserved an instruction digest; a missing one is damaged state.
            if (reservation.instructionDigest === undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
            // Release the claim as the `rejected` branch does: this instruction has not run
            // either, and comes back later as an ordinary `continue`. A no-op for an ordinary
            // interrupt, which never claims a decision.
            if (reservation.decisionId !== undefined) this.#db.prepare("UPDATE channel_run_chain_decisions SET status = 'waiting', claimed_by_command_id = NULL WHERE decision_id = ? AND status = 'claimed'").run(reservation.decisionId);
            // A newer instruction supersedes an older un-redispatched one, so the marker
            // names the latest interrupt. The conflict path resets the retry columns because
            // the row now holds a different instruction.
            this.#db.prepare(`INSERT INTO channel_run_chain_redispatch(conversation_id, command_id, interrupted_run_id, instruction_digest, contract_version, requested_at) VALUES (?, ?, ?, ?, ?, ?)
              ON CONFLICT(conversation_id) DO UPDATE SET command_id = excluded.command_id, interrupted_run_id = excluded.interrupted_run_id, instruction_digest = excluded.instruction_digest, requested_at = excluded.requested_at,
                attempt_count = 0, next_attempt_at = NULL, quarantined_at = NULL`)
              .run(reservation.conversationId, commandId, outcome.runId, reservation.instructionDigest, CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, settledAt);
          } else this.#cancelPending(reservation.conversationId, settledAt);
        } else if (outcome.status === "steered") {
          // Router handed this instruction to the Run this continue reserved as an
          // interrupt, and the Run took it into its running turn. Nothing runs and nothing
          // is queued: no redispatch marker, an existing one is left alone, and no Run row.
          // A steer only answers an interrupting continue.
          if (reservation.kind !== "continue" || reservation.interruptRunId === undefined || reservation.interruptRunId !== outcome.runId) throw new ChannelSessionRefusalError("run_state_conflict");
          // Released as the `interrupt_requested` branch releases it; a no-op for the same reason.
          if (reservation.decisionId !== undefined) this.#db.prepare("UPDATE channel_run_chain_decisions SET status = 'waiting', claimed_by_command_id = NULL WHERE decision_id = ? AND status = 'claimed'").run(reservation.decisionId);
        } else if (reservation.kind === "cancel") this.#cancelPending(reservation.conversationId, settledAt);
        else {
          if (this.#runById(outcome.runId) !== null) throw new ChannelSessionRefusalError("run_state_conflict");
          // An `accepted` outcome on an interrupt reservation means Router saw the prior Run
          // terminal first and created a successor; it still chains onto the reserved Run.
          const priorRunId = reservation.priorRunId ?? reservation.interruptRunId ?? null;
          // The interrupted Run's open elicitation is superseded; settle it here so the
          // conversation never holds two open decisions.
          if (reservation.interruptRunId !== undefined) this.#db.prepare("UPDATE channel_run_chain_decisions SET status = 'cancelled', claimed_by_command_id = NULL, settled_at = ? WHERE run_id = ? AND status IN ('waiting', 'claimed')").run(settledAt, reservation.interruptRunId);
          this.#db.prepare("INSERT INTO channel_run_chain_runs(run_id, conversation_id, prior_run_id, task_id, command_id, state, contract_version, started_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)")
            .run(outcome.runId, reservation.conversationId, priorRunId, outcome.taskId, commandId, CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, settledAt, settledAt);
          this.#db.prepare("UPDATE channel_run_chain_conversations SET current_run_id = ? WHERE conversation_id = ?").run(outcome.runId, reservation.conversationId);
          // A decision reply does not supersede a queued redispatch: it is the interrupted
          // Run's wind-down asking a question, and the queued instruction waits through
          // every round trip. A plain accepted continue does supersede it. The
          // discriminator is the chained Run's state, not whether a decision row was
          // claimed, because the waiting state and its elicitation arrive separately.
          const chained = reservation.priorRunId === undefined ? null : this.#runById(reservation.priorRunId);
          if (chained?.state !== "waiting_for_input") this.#db.prepare("DELETE FROM channel_run_chain_redispatch WHERE conversation_id = ?").run(reservation.conversationId);
          if (reservation.decisionId !== undefined) this.#resolveDecision(reservation.decisionId, commandId, settledAt);
        }
        this.#db.prepare("UPDATE channel_run_chain_reservations SET status = ?, run_id = ?, task_id = ?, settled_at = ? WHERE command_id = ?").run(outcome.status, outcome.runId, outcome.taskId, settledAt, commandId);
      }
      this.#db.prepare("UPDATE channel_run_chain_conversations SET pending_command_id = NULL WHERE conversation_id = ? AND pending_command_id = ?").run(reservation.conversationId, commandId);
      const settled = this.#reservation(commandId);
      if (settled === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
      return settled;
    });
  }

  /**
   * Settles a `cancel` that was never dispatched to Router: a cancel on a waiting
   * conversation has no Run to interrupt. `resolved_locally` is the only honest
   * status, since every `ChannelCommandOutcome` asserts something about Router.
   *
   * The caller must still `reserve` first. That slot is the only mutual exclusion
   * between this cancel and a concurrent continue on the same conversation.
   *
   * Recording the resolution, closing the decision and suppressing any queued
   * redispatch commit in one transaction. The Run stays `waiting_for_input`;
   * `recordRunState` is its only writer. A question that arrives after the cancel
   * is refused by `raiseWaitingDecision`.
   */
  resolveLocally(input: { readonly commandId: string }): ChannelRunChainReservation {
    const data = plainData(input, ["commandId"]);
    const commandId = opaque(data.commandId);
    this.#assertUsable();
    return this.#transaction(() => {
      const reservation = this.#reservation(commandId);
      if (reservation === null) throw new ChannelSessionRefusalError("unknown_reservation");
      // Only a cancel: an undispatched start or continue was abandoned, not resolved.
      if (reservation.kind !== "cancel") throw new ChannelSessionRefusalError("run_state_conflict");
      if (reservation.status !== "reserved") {
        // Idempotent for a crash retry; a conflict for anything else, matching `settle`.
        if (reservation.status !== "resolved_locally") throw new ChannelSessionRefusalError("reservation_settled");
        return reservation;
      }
      const settledAt = this.#now().toISOString();
      this.#db.prepare("UPDATE channel_run_chain_reservations SET status = 'resolved_locally', settled_at = ? WHERE command_id = ?").run(settledAt, commandId);
      this.#cancelPending(reservation.conversationId, settledAt);
      this.#db.prepare("UPDATE channel_run_chain_conversations SET pending_command_id = NULL WHERE conversation_id = ? AND pending_command_id = ?").run(reservation.conversationId, commandId);
      const settled = this.#reservation(commandId);
      if (settled === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
      return settled;
    });
  }

  /**
   * The outcomes `Router#reconcileCommand` returns, plus `rejected`. An unsettled
   * reservation reports `unknown`, never `accepted`.
   */
  reconcile(input: { readonly commandId: string }): ChannelRunChainReconcileOutcome {
    const data = plainData(input, ["commandId"]);
    const commandId = opaque(data.commandId);
    this.#assertUsable();
    return this.#readSnapshot(() => {
      const reservation = this.#reservation(commandId);
      return reservation === null ? "absent" : reservation.status === "reserved" ? "unknown" : reservation.status;
    });
  }

  /**
   * Sweeps reservations stranded in `reserved` and converges each one through
   * `ChannelCommandPort#reconcile` and its lineage readers.
   *
   * - `accepted` with lineage, `interrupt_requested`, `steered` -> settled through
   *   `settle`, restoring a redispatch marker the crashed caller never wrote.
   * - `absent` -> settled `rejected` with `axis.channel.confirmed_absent`; never replayed.
   * - `unknown` -> left as it is, unless the caller passed `unconfirmedAfterMs`
   *   and that window has passed, in which case it is settled `rejected` with
   *   `axis.channel.outcome_unconfirmed`.
   * - accepted without lineage -> `accepted_unresolved`, untouched.
   *
   * A reservation is stale only once `reserved_at` is at least `staleAfterMs` old,
   * because `reserved` is also the live in-flight state.
   *
   * `retain` lets a caller keep a commandId whose dispatching delivery is still
   * live: it is skipped before any Router read and counted in `examined`.
   *
   * Every stale reservation in the database is reconciled through the one
   * `dispatch` port, which assumes one channel per run-chain file.
   */
  async reconcileStale(input: { readonly dispatch: ChannelCommandPort; readonly staleAfterMs?: number; readonly unconfirmedAfterMs?: number; readonly limit?: number; readonly retain?: (commandId: string) => boolean }): Promise<ChannelRunChainSweepResult> {
    const data = plainData(input);
    if (Object.keys(data).some((key) => !["dispatch", "staleAfterMs", "unconfirmedAfterMs", "limit", "retain"].includes(key))
      || !isObject(data.dispatch) || typeof (data.dispatch as { reconcile?: unknown }).reconcile !== "function"
      || typeof (data.dispatch as { reconcileInterrupted?: unknown }).reconcileInterrupted !== "function"
      || (data.retain !== undefined && typeof data.retain !== "function")) malformed();
    // A non-finite window would silently sweep nothing, so it is refused.
    const staleAfterMs: number = data.staleAfterMs === undefined ? DEFAULT_STALE_RESERVATION_MS
      : (Number.isSafeInteger(data.staleAfterMs) && (data.staleAfterMs as number) >= 0 ? data.staleAfterMs as number : malformed());
    // Same checks as `staleAfterMs`, and never narrower than it. Undefined when the
    // caller does not opt into the unconfirmed sweep.
    const unconfirmedAfterMs: number | undefined = data.unconfirmedAfterMs === undefined ? undefined
      : (Number.isSafeInteger(data.unconfirmedAfterMs) && (data.unconfirmedAfterMs as number) >= staleAfterMs ? data.unconfirmedAfterMs as number : malformed());
    const limit: number = data.limit === undefined ? DEFAULT_SWEEP_LIMIT
      : (Number.isSafeInteger(data.limit) && (data.limit as number) >= 1 && (data.limit as number) <= 100 ? data.limit as number : malformed());
    const dispatch = data.dispatch as unknown as ChannelCommandPort;
    // Optional: undefined treats every candidate as ownerless.
    const retain = data.retain as ((commandId: string) => boolean) | undefined;
    this.#assertUsable();
    const cutoff = new Date(this.#now().getTime() - staleAfterMs).toISOString();
    const unconfirmedCutoff = unconfirmedAfterMs === undefined ? undefined : new Date(this.#now().getTime() - unconfirmedAfterMs).toISOString();
    // Candidates are read in one snapshot and settled one at a time, so no write
    // lock is held across a port call.
    const stale = this.#readSnapshot(() => (this.#db.prepare(
      "SELECT command_id FROM channel_run_chain_reservations WHERE status = 'reserved' AND reserved_at <= ? ORDER BY sequence ASC LIMIT ?",
    ).all(cutoff, limit) as Row[]).map((row) => opaque(row.command_id)));
    const entries: ChannelRunChainSweepEntry[] = [];
    for (const commandId of stale) {
      // Retained: not asked of Router, not settled, not in `entries`, but counted in `examined`.
      if (retain?.(commandId) === true) continue;
      const entry = this.#reconcileThrough(commandId, dispatch, false, unconfirmedCutoff);
      if (entry !== null) entries.push(entry);
    }
    return frozen({ examined: stale.length, entries: Object.freeze(entries) });
  }

  /**
   * Converges one reservation through the same mapping as `reconcileStale`, with no
   * staleness window. Called by `../relay/linear/channel.ts` after its own `submit`
   * of this `commandId` threw.
   *
   * Never re-submits: `accepted` settles from Router's retained lineage, and
   * `unknown` is left for the sweeper. `null` means the reservation is no longer
   * `unknown` to this store.
   *
   * Ownership replaces the window. Router is called in-process and claims the
   * command synchronously before its first await, so once the caller's submit has
   * rejected Router either holds the command or never will, and no other
   * dispatcher submits that `commandId`. Never call this about a command the
   * caller did not submit itself; a networked Router port or two Relay processes
   * on one file would need a window here.
   */
  async reconcileNow(input: { readonly commandId: string; readonly dispatch: ChannelCommandPort }): Promise<ChannelRunChainSweepEntry | null> {
    const data = plainData(input, ["commandId", "dispatch"]);
    if (!isObject(data.dispatch)
      || typeof (data.dispatch as { reconcile?: unknown }).reconcile !== "function"
      || typeof (data.dispatch as { reconcileInterrupted?: unknown }).reconcileInterrupted !== "function") malformed();
    const commandId = opaque(data.commandId);
    this.#assertUsable();
    return this.#reconcileThrough(commandId, data.dispatch as unknown as ChannelCommandPort);
  }

  /**
   * Settles held reservations whose outcome Router can already state with lineage,
   * before the stale window passes. Only `accepted` with lineage and
   * `interrupt_requested`/`steered` with lineage are acted on; `absent`, `unknown`
   * and answers without lineage are left for `reconcileStale`.
   *
   * Candidates are `reserved_at > now - staleAfterMs`, the complement of
   * `reconcileStale`, read from the conversations' UNIQUE command slot (zero rows
   * when idle) and joined to their reservation by `command_id`. The `CROSS JOIN`
   * pins that loop order.
   */
  async reconcileSettledEarly(input: { readonly dispatch: ChannelCommandPort; readonly staleAfterMs?: number; readonly limit?: number }): Promise<ChannelRunChainSweepResult> {
    const data = plainData(input);
    if (Object.keys(data).some((key) => !["dispatch", "staleAfterMs", "limit"].includes(key))
      || !isObject(data.dispatch) || typeof (data.dispatch as { reconcile?: unknown }).reconcile !== "function"
      || typeof (data.dispatch as { reconcileInterrupted?: unknown }).reconcileInterrupted !== "function") malformed();
    const limit: number = data.limit === undefined ? DEFAULT_SWEEP_LIMIT
      : (Number.isSafeInteger(data.limit) && (data.limit as number) >= 1 && (data.limit as number) <= 100 ? data.limit as number : malformed());
    const staleAfterMs: number = data.staleAfterMs === undefined ? DEFAULT_STALE_RESERVATION_MS
      : (Number.isSafeInteger(data.staleAfterMs) && (data.staleAfterMs as number) >= 0 ? data.staleAfterMs as number : malformed());
    const dispatch = data.dispatch as unknown as ChannelCommandPort;
    this.#assertUsable();
    const cutoff = new Date(this.#now().getTime() - staleAfterMs).toISOString();
    const held = this.#readSnapshot(() => (this.#db.prepare(
      "SELECT c.pending_command_id FROM channel_run_chain_conversations c CROSS JOIN channel_run_chain_reservations r WHERE r.command_id = c.pending_command_id AND c.pending_command_id IS NOT NULL AND r.reserved_at > ? LIMIT ?",
    ).all(cutoff, limit) as Row[]).map((row) => opaque(row.pending_command_id)));
    const entries: ChannelRunChainSweepEntry[] = [];
    for (const commandId of held) {
      const entry = this.#reconcileThrough(commandId, dispatch, true);
      if (entry !== null) entries.push(entry);
    }
    return frozen({ examined: held.length, entries: Object.freeze(entries) });
  }

  /**
   * One reservation, one Router answer, one settled outcome: the only mapping from
   * `ChannelReconcileOutcome` to a reservation state. `null` means the reservation
   * is no longer `unknown`; it is re-read because an earlier entry or a live
   * settle may have moved it.
   *
   * `definiteOnly` acts only on an answer with lineage. `unconfirmedCutoff` is
   * consulted only for `unknown` and only when `definiteOnly` is false.
   */
  #reconcileThrough(commandId: string, dispatch: ChannelCommandPort, definiteOnly = false, unconfirmedCutoff?: string): ChannelRunChainSweepEntry | null {
    if (this.reconcile({ commandId }) !== "unknown") return null;
    const status = dispatch.reconcile(channelReconcileQuery(commandId));
    if (status === "unknown") {
      // Still `unknown`. If the reservation has sat past the unconfirmed window, this
      // is where it becomes terminal.
      if (!definiteOnly && unconfirmedCutoff !== undefined) {
        const reservation = this.#reservation(commandId);
        if (reservation !== null && reservation.reservedAt <= unconfirmedCutoff)
          return this.#sweepSettle(commandId, { commandId, status: "rejected", safeCode: OUTCOME_UNCONFIRMED_SAFE_CODE }, "unconfirmed");
      }
      return definiteOnly ? null : Object.freeze({ commandId, disposition: "unknown" as const });
    }
    if (status === "accepted") {
      const retained = dispatch.reconcileAccepted?.(channelReconcileQuery(commandId)) ?? null;
      if (retained === null) return definiteOnly ? null : Object.freeze({ commandId, disposition: "accepted_unresolved" as const });
      return this.#sweepSettle(commandId, { commandId, status: "accepted", taskId: retained.taskId, runId: retained.runId }, "accepted");
    }
    if (status === "absent") {
      // Never inside the window: a young reservation may be a live submit Router has not seen yet.
      if (definiteOnly) return null;
      return this.#sweepSettle(commandId, { commandId, status: "rejected", safeCode: CONFIRMED_ABSENT_SAFE_CODE }, "absent");
    }
    if (status === "steered") {
      // Same contract as the interrupt below: lineage from the same durable row, or nothing acted on.
      const steered = dispatch.reconcileSteered?.(channelReconcileQuery(commandId)) ?? null;
      if (steered === null) return definiteOnly ? null : Object.freeze({ commandId, disposition: "unknown" as const });
      return this.#sweepSettle(commandId, { commandId, status: "steered", taskId: steered.taskId, runId: steered.runId }, "steered");
    }
    const retained = dispatch.reconcileInterrupted(channelReconcileQuery(commandId));
    // `interrupt_requested` without lineage is a port that breaks its own contract: reported, never guessed at.
    if (retained === null) return definiteOnly ? null : Object.freeze({ commandId, disposition: "unknown" as const });
    return this.#sweepSettle(commandId, { commandId, status: "interrupt_requested", taskId: retained.taskId, runId: retained.runId }, "interrupt_requested");
  }

  /**
   * Settles one swept command, containing a `ChannelSessionRefusalError` to that
   * command so one refusing reservation cannot block the rest of the sweep on
   * every tick. A `ChannelRunChainStoreError` is damage and propagates.
   */
  #sweepSettle(commandId: string, outcome: ChannelCommandOutcome, disposition: ChannelRunChainSweepDisposition): ChannelRunChainSweepEntry {
    try {
      this.settle({ commandId, outcome });
      return Object.freeze({ commandId, disposition });
    } catch (error) {
      if (!(error instanceof ChannelSessionRefusalError)) throw error;
      return Object.freeze({ commandId, disposition: "settle_refused" as const, code: error.code });
    }
  }

  /** The reservation a command is bound to, or null. */
  reservation(input: { readonly commandId: string }): ChannelRunChainReservation | null {
    const data = plainData(input, ["commandId"]);
    const commandId = opaque(data.commandId);
    this.#assertUsable();
    return this.#readSnapshot(() => this.#reservation(commandId));
  }

  /** Whether this conversation is guarded by the `session_unconfirmed` refusal `reserve` applies. */
  sessionUnconfirmed(input: { readonly conversationId: string }): boolean {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => this.#sessionUnconfirmed(conversationId));
  }

  /** The immutable first start intent, including a rejected attempt. */
  initialStartReservation(input: { readonly conversationId: string }): ChannelRunChainReservation | null {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => {
      const row = this.#db.prepare("SELECT * FROM channel_run_chain_reservations WHERE conversation_id = ? AND kind = 'start' ORDER BY sequence ASC LIMIT 1").get(conversationId) as Row | undefined;
      return row === undefined ? null : this.#reservationRow(row);
    });
  }

  /**
   * Conversations whose current Run has not reached `succeeded`/`failed`, which an
   * observe tick polls Router for. A `waiting_for_input` Run is still observed.
   */
  openRunConversations(): readonly ChannelRunChainOpenConversation[] {
    this.#assertUsable();
    return this.#readSnapshot(() => frozen((this.#db.prepare(`SELECT c.conversation_id, c.scope_system, c.scope_domain, c.tenant_id, c.task_id, c.current_run_id FROM channel_run_chain_conversations c
      JOIN channel_run_chain_runs r ON r.run_id = c.current_run_id WHERE c.current_run_id IS NOT NULL AND r.state NOT IN ('succeeded', 'failed')`).all() as Row[])
      .map((row) => ({ conversationId: opaque(row.conversation_id), scope: this.#domainOf(row), tenant: this.#tenantOf(row), taskId: opaque(row.task_id as string), currentRunId: opaque(row.current_run_id as string) }))));
  }

  /**
   * Every conversation this store has ever bound, with no Run-state filter: a
   * settled conversation can still be reopened by a later comment.
   */
  allConversations(): readonly ChannelRunChainBoundConversation[] {
    this.#assertUsable();
    return this.#readSnapshot(() => frozen((this.#db.prepare(`SELECT conversation_id, scope_system, scope_domain, tenant_id FROM channel_run_chain_conversations`).all() as Row[])
      .map((row) => ({ conversationId: opaque(row.conversation_id), scope: this.#domainOf(row), tenant: this.#tenantOf(row) }))));
  }

  /**
   * Projects a Router-owned Run state change into the chain. The Task must name
   * this conversation's domain and conversation and match its bound
   * `taskId`/`correlationId`. Transition legality defers to the contract's
   * `validateRuntimeTransition`; re-presenting the current state is an idempotent
   * replay.
   */
  recordRunState(input: { readonly conversationId: string; readonly task: ChannelRuntimeTask; readonly runId: string; readonly state: RuntimeState }): ChannelRunChainEntry {
    const data = plainData(input, ["conversationId", "task", "runId", "state"]);
    const conversationId = opaque(data.conversationId), runId = opaque(data.runId), state = runState(data.state);
    const task = this.#task(data.task);
    this.#assertUsable();
    return this.#transaction(() => {
      const conversation = this.#conversation(conversationId);
      if (conversation === null) throw new ChannelSessionRefusalError("unknown_conversation");
      if (task.source.system !== conversation.scope.system || task.source.domain !== conversation.scope.domain || task.source.conversationRef !== conversationId) throw new ChannelSessionRefusalError("task_mismatch");
      const run = this.#runById(runId);
      if (run === null || run.conversationId !== conversationId) throw new ChannelSessionRefusalError("unknown_run");
      if (conversation.taskId !== task.taskId || run.taskId !== task.taskId) throw new ChannelSessionRefusalError("task_mismatch");
      if (conversation.correlationId === null) this.#db.prepare("UPDATE channel_run_chain_conversations SET correlation_id = ? WHERE conversation_id = ? AND correlation_id IS NULL").run(task.correlationId, conversationId);
      else if (conversation.correlationId !== task.correlationId) throw new ChannelSessionRefusalError("task_mismatch");
      // Bound once from the first Task that carries it, then immutable, like `correlation_id`.
      if (conversation.sourceIssueId === null) this.#db.prepare("UPDATE channel_run_chain_conversations SET source_issue_id = ? WHERE conversation_id = ? AND source_issue_id IS NULL").run(task.source.issueId, conversationId);
      else if (conversation.sourceIssueId !== task.source.issueId) throw new ChannelSessionRefusalError("task_mismatch");
      const version = this.#runtimeVersion(conversationId);
      if (version === null) this.#db.prepare("INSERT INTO channel_run_chain_runtime_versions(conversation_id, runtime_contract_version) VALUES (?, ?)").run(conversationId, task.contractVersion);
      else if (version !== task.contractVersion) throw new ChannelSessionRefusalError("task_mismatch");
      if (run.state === state) return run;
      if (!validateRuntimeTransition(run.state, state).valid) throw new ChannelSessionRefusalError("run_state_conflict");
      this.#db.prepare("UPDATE channel_run_chain_runs SET state = ?, updated_at = ? WHERE run_id = ?").run(state, this.#now().toISOString(), runId);
      const updated = this.#runById(runId);
      if (updated === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
      return updated;
    });
  }

  /**
   * Records the elicitation a waiting Run is blocked on and mints the decision
   * identity. Only the digest is stored. The Run must be the conversation's
   * current one, so a late elicitation for a superseded Run cannot open a second
   * decision.
   */
  raiseWaitingDecision(input: { readonly runId: string; readonly elicitationDigest: string }): ChannelWaitingDecisionReceipt {
    const data = plainData(input, ["runId", "elicitationDigest"]);
    const runId = opaque(data.runId), elicitationDigest = digestValue(data.elicitationDigest);
    this.#assertUsable();
    return this.#transaction(() => {
      const run = this.#runById(runId);
      if (run === null) throw new ChannelSessionRefusalError("unknown_run");
      if (run.state !== "waiting_for_input") throw new ChannelSessionRefusalError("run_state_conflict");
      const conversation = this.#conversation(run.conversationId);
      if (conversation === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
      if (conversation.currentRunId !== runId) throw new ChannelSessionRefusalError("run_superseded");
      const existing = this.#decisionByRun(runId);
      if (existing !== null) {
        if (existing.elicitationDigest !== elicitationDigest) throw new ChannelSessionRefusalError("decision_rebound");
        return frozen({ decision: existing, raised: false });
      }
      // The wait may have been cancelled before its question arrived. A
      // `resolved_locally` cancel naming this Run in its immutable `interrupt_run_id`
      // is a durable record that the wait is over. Only a locally honoured cancel
      // refuses: a dispatched cancel is a request, and the Run may still ask a
      // question before honouring it.
      if (this.#waitLocallyCancelled(run.conversationId, runId)) throw new ChannelSessionRefusalError("decision_cancelled");
      const decisionId = opaque(this.#id());
      this.#db.prepare("INSERT INTO channel_run_chain_decisions(decision_id, conversation_id, run_id, elicitation_digest, status, claimed_by_command_id, contract_version, raised_at, settled_at) VALUES (?, ?, ?, ?, 'waiting', NULL, ?, ?, NULL)")
        .run(decisionId, run.conversationId, runId, elicitationDigest, CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, this.#now().toISOString());
      const decision = this.#decisionByRun(runId);
      if (decision === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
      return frozen({ decision, raised: true });
    });
  }

  /** The conversation's decision that is still awaiting an answer, or null. */
  openDecision(input: { readonly conversationId: string }): ChannelWaitingDecision | null {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => {
      // Scoped to the current Run and ordered, so a superseded Run's decision is never
      // returned.
      const conversation = this.#conversation(conversationId);
      if (conversation === null || conversation.currentRunId === null) return null;
      const row = this.#db.prepare("SELECT * FROM channel_run_chain_decisions WHERE conversation_id = ? AND run_id = ? AND status IN ('waiting', 'claimed') ORDER BY sequence ASC LIMIT 1").get(conversationId, conversation.currentRunId) as Row | undefined;
      return row === undefined ? null : this.#decisionRow(row);
    });
  }

  /** The Run the conversation is currently on, terminal or not, or null before the first one. */
  currentRun(input: { readonly conversationId: string }): ChannelRunChainEntry | null {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => {
      const conversation = this.#conversation(conversationId);
      return conversation === null || conversation.currentRunId === null ? null : this.#runById(conversation.currentRunId);
    });
  }

  /**
   * Every Run of a conversation, oldest first, ordered by the monotonic
   * `sequence`, never by timestamp. `afterSequence` pages forward past the cap.
   */
  runChain(input: { readonly conversationId: string; readonly afterSequence?: number; readonly limit?: number }): readonly ChannelRunChainEntry[] {
    const data = plainData(input);
    if (Object.keys(data).some((key) => !["conversationId", "afterSequence", "limit"].includes(key)) || !Object.hasOwn(data, "conversationId")) malformed();
    const conversationId = opaque(data.conversationId);
    const limit: number = data.limit === undefined ? 100 : (Number.isSafeInteger(data.limit) ? data.limit as number : malformed());
    if (limit < 1 || limit > 100) malformed();
    const afterSequence: number = data.afterSequence === undefined ? 0 : (Number.isSafeInteger(data.afterSequence) && (data.afterSequence as number) >= 0 ? data.afterSequence as number : malformed());
    this.#assertUsable();
    return frozen((this.#db.prepare("SELECT * FROM channel_run_chain_runs WHERE conversation_id = ? AND sequence > ? ORDER BY sequence ASC LIMIT ?")
      .all(conversationId, afterSequence, limit) as Row[]).map((row) => this.#runRow(row)));
  }

  /**
   * The continuation context `decideRuntimeTaskSubmit` requires, built from this
   * store's own state. `priorRun.task` is rebuilt from the bound identity, never
   * echoed from the caller, so the contract's `sameTask` check is meaningful.
   * Without a bound `correlationId`, `priorRun` is omitted while `hasPriorRuns`
   * stays true, and the contract refuses any `priorRunId`.
   */
  continuationContext(input: { readonly conversationId: string }): ChannelContinuationContext {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => {
      const conversation = this.#conversation(conversationId);
      if (conversation === null) throw new ChannelSessionRefusalError("unknown_conversation");
      const current = conversation.currentRunId === null ? null : this.#runById(conversation.currentRunId);
      const hasPriorRuns = this.#db.prepare("SELECT 1 FROM channel_run_chain_runs WHERE conversation_id = ? LIMIT 1").get(conversationId) !== undefined;
      if (current === null || conversation.taskId === null || conversation.correlationId === null) return frozen({ hasPriorRuns });
      const contractVersion = this.#runtimeVersion(conversationId);
      if (contractVersion === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
      // A Linear conversation without a bound `sourceIssueId` cannot produce a
      // `LinearSourceRef`, so it reports `hasPriorRuns` with no `priorRun` until its
      // first lifecycle event.
      if (conversation.sourceIssueId === null) return frozen({ hasPriorRuns });
      const task: ChannelRuntimeTask = {
        taskId: conversation.taskId, correlationId: conversation.correlationId, contractVersion,
        sourceRef: { system: "linear", organizationId: conversation.scope.domain, issueId: conversation.sourceIssueId, agentSessionId: conversationId },
      };
      return frozen({ hasPriorRuns, priorRun: { runId: current.runId, state: current.state, task } }) as ChannelContinuationContext;
    });
  }

  /**
   * The interrupted instruction still awaiting redispatch, or null. A read only: it
   * spends no budget, and reports a given-up marker like a live one.
   *
   * `eligible` requires the current Run to be `succeeded`/`failed`, never
   * `waiting_for_input`: redispatching into an open elicitation would let an
   * instruction written before the question answer it. The terminal need not
   * belong to the interrupted Run.
   */
  pendingRedispatch(input: { readonly conversationId: string }): ChannelRedispatchMarker | null {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => {
      const row = this.#redispatchRow(conversationId);
      return row === null ? null : this.#redispatchMarker(conversationId, row);
    });
  }

  /**
   * Takes one attempt on this conversation's queued redispatch, or hands back the
   * obligation to give up on it. This is the only way to obtain an attempt, so a
   * caller cannot loop past the ceiling.
   *
   * Returns nothing, without spending an attempt, when there is no marker, the
   * current Run has not settled (`channelRedispatchEligible`), or the next attempt
   * is not due yet.
   *
   * Past the ceiling it returns the same `quarantine` until the caller calls
   * `quarantineRedispatch`. Nothing here deletes the marker.
   */
  claimRedispatch(input: { readonly conversationId: string }): ChannelRedispatchClaim | null {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#transaction(() => {
      const row = this.#redispatchRow(conversationId);
      if (row === null) return null;
      const marker = this.#redispatchMarker(conversationId, row);
      const state = this.#redispatchRetryState(row);
      // Already given up on, and the user already told.
      if (state.quarantinedAt !== null) return null;
      if (!marker.eligible) return null;
      const now = this.#now();
      // Compared as ISO-8601 strings, which is chronological because every stamp is
      // `toISOString()` output; `#redispatchRetryState` rejects any other form.
      if (state.nextAttemptAt !== null && state.nextAttemptAt > now.toISOString()) return null;
      const verdict = retryVerdict(state.attemptCount, this.#maxAttempts);
      if (verdict.disposition === "quarantine") {
        // Spaced but not incremented: being told to give up is not an attempt. The
        // spacing still moves, so the obligation is not reissued every tick.
        this.#db.prepare("UPDATE channel_run_chain_redispatch SET next_attempt_at = ? WHERE conversation_id = ? AND command_id = ? AND quarantined_at IS NULL")
          .run(new Date(now.getTime() + retryBackoffDelayMs(state.attemptCount)).toISOString(), conversationId, marker.commandId);
        return frozen({ disposition: "quarantine" as const, marker, attempts: verdict.attempts, maxAttempts: verdict.maxAttempts });
      }
      this.#db.prepare("UPDATE channel_run_chain_redispatch SET attempt_count = ?, next_attempt_at = ? WHERE conversation_id = ? AND command_id = ?")
        .run(verdict.attempt, new Date(now.getTime() + retryBackoffDelayMs(verdict.attempt)).toISOString(), conversationId, marker.commandId);
      return frozen({ disposition: "attempt" as const, marker, attempt: verdict.attempt, remaining: verdict.remaining });
    });
  }

  /**
   * Discharges a `quarantine` obligation: the marker stops being attempted and is
   * retained. Guarded by `conversationId`, the exact `commandId`, and
   * `quarantined_at IS NULL`, so a superseding instruction never inherits the
   * give-up and the first stamp wins. A marker matching none of these is already
   * handled.
   *
   * It does not tell the user; each channel announces before calling this.
   */
  quarantineRedispatch(input: { readonly conversationId: string; readonly commandId: string }): boolean {
    const data = plainData(input, ["conversationId", "commandId"]);
    const conversationId = opaque(data.conversationId), commandId = opaque(data.commandId);
    this.#assertUsable();
    return this.#transaction(() => {
      const givenUpAt = this.#now().toISOString();
      const row = this.#db.prepare("SELECT attempt_count FROM channel_run_chain_redispatch WHERE conversation_id = ? AND command_id = ? AND quarantined_at IS NULL AND attempt_count > 0").get(conversationId, commandId) as Row | undefined;
      if (row === undefined) return false;
      const attempts = Number(row.attempt_count);
      if (!Number.isSafeInteger(attempts) || attempts < 1) throw new ChannelRunChainStoreError("persisted_state_conflict");
      const changed = this.#db.prepare("UPDATE channel_run_chain_redispatch SET quarantined_at = ? WHERE conversation_id = ? AND command_id = ? AND quarantined_at IS NULL AND attempt_count > 0")
        .run(givenUpAt, conversationId, commandId).changes;
      if (changed !== 1) return false;
      // The stamp and the disposition are written in one transaction. The stamp keeps
      // `claimRedispatch` quiet; the disposition is the operator's record and outlives
      // the marker.
      this.#db.prepare("INSERT INTO channel_run_chain_redispatch_dispositions(conversation_id, command_id, attempts, failure_code, given_up_at) VALUES (?, ?, ?, ?, ?)")
        .run(conversationId, commandId, attempts, RETRY_BUDGET_EXHAUSTED_CODE, givenUpAt);
      return true;
    });
  }

  /**
   * Un-charges an attempt `claimRedispatch` handed out, for a failure class the
   * ceiling does not apply to, such as a credential or token-mint failure.
   * `next_attempt_at` is left as set, so the class cannot become a per-tick loop.
   * Guarded on the exact `commandId`, `attempt_count > 0` and
   * `quarantined_at IS NULL`. Returns whether it un-charged.
   */
  refundRedispatchAttempt(input: { readonly conversationId: string; readonly commandId: string }): boolean {
    const data = plainData(input, ["conversationId", "commandId"]);
    const conversationId = opaque(data.conversationId), commandId = opaque(data.commandId);
    this.#assertUsable();
    return this.#transaction(() => this.#db.prepare("UPDATE channel_run_chain_redispatch SET attempt_count = attempt_count - 1 WHERE conversation_id = ? AND command_id = ? AND quarantined_at IS NULL AND attempt_count > 0")
      .run(conversationId, commandId).changes === 1);
  }

  /**
   * Un-charges the attempt and spaces the next one a fixed minute out, for an
   * attempt Router answered with `axis.router.node_draining`. Same guards as
   * `refundRedispatchAttempt`. Returns whether it held.
   */
  holdRedispatchAttempt(input: { readonly conversationId: string; readonly commandId: string }): boolean {
    const data = plainData(input, ["conversationId", "commandId"]);
    const conversationId = opaque(data.conversationId), commandId = opaque(data.commandId);
    this.#assertUsable();
    return this.#transaction(() => this.#db.prepare("UPDATE channel_run_chain_redispatch SET attempt_count = attempt_count - 1, next_attempt_at = ? WHERE conversation_id = ? AND command_id = ? AND quarantined_at IS NULL AND attempt_count > 0")
      .run(new Date(this.#now().getTime() + REDISPATCH_DRAIN_HOLD_MS).toISOString(), conversationId, commandId).changes === 1);
  }

  /**
   * What this conversation's queued redispatch has spent, or null when there is
   * none. `quarantinedAt` distinguishes a given-up marker from a live one. No CLI
   * or HTTP surface exposes it.
   */
  redispatchAttempts(input: { readonly conversationId: string }): ChannelRedispatchRetryState | null {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => {
      const row = this.#redispatchRow(conversationId);
      if (row === null) return null;
      // Validated through the same reader the claim path uses.
      this.#redispatchMarker(conversationId, row);
      return frozen({ commandId: opaque(row.command_id), ...this.#redispatchRetryState(row) });
    });
  }

  #redispatchRow(conversationId: string): Row | null {
    const row = this.#db.prepare("SELECT * FROM channel_run_chain_redispatch WHERE conversation_id = ?").get(conversationId) as Row | undefined;
    return row ?? null;
  }

  /** The retry columns, read back. A stamp that does not round-trip through `toISOString()` is damaged state. */
  #redispatchRetryState(row: Row): { readonly attemptCount: number; readonly nextAttemptAt: string | null; readonly quarantinedAt: string | null } {
    const attemptCount = Number(row.attempt_count);
    if (!Number.isSafeInteger(attemptCount) || attemptCount < 0) throw new ChannelRunChainStoreError("persisted_state_conflict");
    const stamp = (value: unknown): string | null => {
      if (value === null || value === undefined) return null;
      if (typeof value !== "string" || new Date(value).toISOString() !== value) throw new ChannelRunChainStoreError("persisted_state_conflict");
      return value;
    };
    return { attemptCount, nextAttemptAt: stamp(row.next_attempt_at), quarantinedAt: stamp(row.quarantined_at) };
  }

  /** One marker row as the neutral contract describes it, shared by `pendingRedispatch` and `claimRedispatch`. */
  #redispatchMarker(conversationId: string, row: Row): ChannelRedispatchMarker {
    try {
      const conversation = this.#conversation(conversationId);
      const interruptedRunId = opaque(row.interrupted_run_id), interrupted = this.#runById(interruptedRunId);
      if (conversation === null || interrupted === null || row.contract_version !== CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION || typeof row.requested_at !== "string" || new Date(row.requested_at).toISOString() !== row.requested_at) throw new Error();
      const current = conversation.currentRunId === null ? null : this.#runById(conversation.currentRunId);
      return frozen({
        contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, conversationId: opaque(row.conversation_id), commandId: opaque(row.command_id),
        interruptedRunId, instructionDigest: digestValue(row.instruction_digest),
        // `channelRedispatchEligible` is the contract's authority for this field; the
        // wider `terminalStates` would report a waiting conversation as ready.
        eligible: channelRedispatchEligible(current === null ? null : current.state), requestedAt: row.requested_at,
      });
    } catch (error) { if (error instanceof ChannelRunChainStoreError || error instanceof ChannelSessionRefusalError) throw error; throw new ChannelRunChainStoreError("persisted_state_conflict"); }
  }

  /**
   * Clears a redispatch marker without any Router interaction, when the queued
   * comment was edited or deleted and must never fire. Guarded by `conversationId`
   * and the exact `commandId`, so a newer marker is never clobbered; a marker that
   * no longer matches is already handled.
   */
  rejectRedispatch(input: { readonly conversationId: string; readonly commandId: string }): void {
    const data = plainData(input, ["conversationId", "commandId"]);
    const conversationId = opaque(data.conversationId), commandId = opaque(data.commandId);
    this.#assertUsable();
    this.#transaction(() => {
      this.#db.prepare("DELETE FROM channel_run_chain_redispatch WHERE conversation_id = ? AND command_id = ?").run(conversationId, commandId);
    });
  }

  /**
   * This store's binding for a `conversationId`, or null. The redispatch trigger
   * reads the bound tenant from it to re-`reserve`.
   */
  conversation(input: { readonly conversationId: string }): ChannelRunChainConversationState | null {
    const data = plainData(input, ["conversationId"]);
    const conversationId = opaque(data.conversationId);
    this.#assertUsable();
    return this.#readSnapshot(() => this.#conversation(conversationId));
  }

  /** Creates the current schema in an empty database; opens only a database already at the current version. */
  #initializeSchema(): void {
    const present = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE name LIKE 'channel_run_chain%' LIMIT 1").get();
    if (present === undefined) {
      this.#transaction(() => {
        for (const definition of Object.values(schemaDefinitions)) this.#db.exec(definition);
        this.#db.prepare("INSERT INTO channel_run_chain_schema(version, applied_at) VALUES (?, ?)").run(CHANNEL_RUN_CHAIN_SCHEMA_VERSION, this.#now().toISOString());
      });
      return;
    }
    const ledger = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'channel_run_chain_schema'").get();
    if (ledger === undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    const version = schemaLedgerVersion(this.#db.prepare("SELECT version FROM channel_run_chain_schema").all() as Row[]);
    if (version === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
    if (version !== CHANNEL_RUN_CHAIN_SCHEMA_VERSION) throw new ChannelRunChainStoreError("schema_version_unsupported");
  }

  /**
   * Resolves the conversation through the registry pointer and binds it locally on
   * first use. A `conversationId` other than the one the pointer selects is
   * refused, so a command is never retargeted at a successor conversation.
   */
  #bindConversation(scopeValue: ChannelConversationScope, conversationId: string, tenant: TenantRef): ChannelRunChainConversationState {
    const domain = conversationDomain(scopeValue);
    // Tenancy is provider-scoped: a scope and tenant naming different systems are refused.
    if (tenant.system !== domain.system) throw new ChannelSessionRefusalError("tenant_mismatch");
    const pointer = this.#conversations.current({ scope: scopeValue });
    // The pointer's own scope is re-checked, since `#conversations` is a structural
    // port; the rebind path below checks the same.
    if (pointer === null || pointer.conversationId !== conversationId) throw new ChannelSessionRefusalError("unknown_conversation");
    const pointerDomain = conversationDomain(pointer.scope);
    if (pointerDomain.system !== domain.system || pointerDomain.domain !== domain.domain) throw new ChannelSessionRefusalError("unknown_conversation");
    if (pointer.status !== "active") throw new ChannelSessionRefusalError("conversation_closed");
    const existing = this.#conversation(conversationId);
    if (existing !== null) {
      if (existing.tenant.tenantId !== tenant.tenantId || existing.tenant.system !== tenant.system) throw new ChannelSessionRefusalError("tenant_mismatch");
      if (existing.scope.system !== domain.system || existing.scope.domain !== domain.domain) throw new ChannelSessionRefusalError("unknown_conversation");
      return existing;
    }
    this.#db.prepare("INSERT INTO channel_run_chain_conversations(conversation_id, scope_system, scope_domain, tenant_id, source_issue_id, task_id, correlation_id, current_run_id, pending_command_id, contract_version, created_at) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, ?)")
      .run(conversationId, domain.system, domain.domain, tenant.tenantId, CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, this.#now().toISOString());
    const created = this.#conversation(conversationId);
    if (created === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
    return created;
  }
  #bindTask(conversationId: string, taskId: string): void {
    const conversation = this.#conversation(conversationId);
    if (conversation === null) throw new ChannelSessionRefusalError("unknown_conversation");
    if (conversation.taskId === null) this.#db.prepare("UPDATE channel_run_chain_conversations SET task_id = ? WHERE conversation_id = ? AND task_id IS NULL").run(taskId, conversationId);
    else if (conversation.taskId !== taskId) throw new ChannelSessionRefusalError("task_mismatch");
  }
  /**
   * A decision is answerable once. A `resolved` or `cancelled` one is not re-claimed
   * and the reply proceeds as a follow-up. `claimed` here is damaged state, since
   * the slot was just observed free.
   */
  #claimDecision(runId: string, commandId: string): string | undefined {
    const decision = this.#decisionByRun(runId);
    if (decision === null || decision.status === "resolved" || decision.status === "cancelled") return undefined;
    if (decision.status !== "waiting") throw new ChannelRunChainStoreError("persisted_state_conflict");
    this.#db.prepare("UPDATE channel_run_chain_decisions SET status = 'claimed', claimed_by_command_id = ? WHERE decision_id = ? AND status = 'waiting'").run(commandId, decision.decisionId);
    return decision.decisionId;
  }
  /**
   * Whether `runId` was cancelled while waiting: a `resolved_locally` cancel naming
   * it as `interrupt_run_id`. Shared by `raiseWaitingDecision` and `reserve`'s
   * continue branch.
   *
   * Scoped per Run, not per conversation. If a cancelled conversation could ever
   * chain a successor Run, a conversation-scoped check would silence every later
   * question in it.
   */
  #waitLocallyCancelled(conversationId: string, runId: string): boolean {
    return this.#db.prepare(`SELECT 1 FROM channel_run_chain_reservations
      WHERE conversation_id = ? AND status = 'resolved_locally' AND kind = 'cancel' AND interrupt_run_id = ? LIMIT 1`).get(conversationId, runId) !== undefined;
  }
  #resolveDecision(decisionId: string, commandId: string, settledAt: string): void {
    const changes = this.#db.prepare("UPDATE channel_run_chain_decisions SET status = 'resolved', settled_at = ? WHERE decision_id = ? AND status = 'claimed' AND claimed_by_command_id = ?").run(settledAt, decisionId, commandId);
    // The claim was taken in the slot this settle releases; anything else means the row moved.
    if (Number(changes.changes) !== 1) throw new ChannelRunChainStoreError("persisted_state_conflict");
  }
  /** A cancel ends whatever the conversation was waiting on and abandons any un-redispatched instruction. */
  #cancelPending(conversationId: string, settledAt: string): void {
    this.#db.prepare("UPDATE channel_run_chain_decisions SET status = 'cancelled', claimed_by_command_id = NULL, settled_at = ? WHERE conversation_id = ? AND status IN ('waiting', 'claimed')").run(settledAt, conversationId);
    this.#db.prepare("DELETE FROM channel_run_chain_redispatch WHERE conversation_id = ?").run(conversationId);
  }
  /** Closed validation of the neutral outcome; the two shapes share no optional fields. */
  #outcome(value: unknown): ChannelCommandOutcome {
    const data = plainData(value);
    const keys = Object.keys(data).sort().join(",");
    if (data.status === "rejected") {
      if (keys !== "commandId,safeCode,status" || typeof data.safeCode !== "string" || data.safeCode.trim().length === 0) malformed();
      return { commandId: opaque(data.commandId), status: "rejected", safeCode: data.safeCode };
    }
    if (keys !== "commandId,runId,status,taskId" || (data.status !== "accepted" && data.status !== "interrupt_requested" && data.status !== "steered")) malformed();
    return { commandId: opaque(data.commandId), status: data.status, runId: opaque(data.runId), taskId: opaque(data.taskId) };
  }
  /**
   * This conversation's pinned runtime contract version, or `null` before the first
   * Task reaches `recordRunState`. Pinned once and never drifts. A persisted value
   * outside `isChannelRuntimeContractVersion` is `persisted_state_conflict`.
   */
  #runtimeVersion(conversationId: string): ChannelRuntimeContractVersion | null {
    const row = this.#db.prepare("SELECT runtime_contract_version FROM channel_run_chain_runtime_versions WHERE conversation_id = ?").get(conversationId);
    if (row === undefined) return null;
    if (!isChannelRuntimeContractVersion(row.runtime_contract_version)) throw new ChannelRunChainStoreError("persisted_state_conflict");
    return row.runtime_contract_version;
  }

  /**
   * Closed validation of the Runtime v1 Task. `SourceRef` is a
   * provider-discriminated union; an unknown `system` or a member with the wrong
   * key set is malformed.
   *
   * `agentSessionId` is required for a Linear Task: it is the conversation's
   * identity, so a Linear Task without it cannot be bound to a conversation.
   *
   * The accepted versions are `isChannelRuntimeContractVersion`, derived from the
   * `ChannelRuntimeTask` type.
   */
  #task(value: unknown): { readonly contractVersion: ChannelRuntimeContractVersion; readonly taskId: string; readonly correlationId: string; readonly source: ChannelTaskSource } {
    const data = plainData(value, ["contractVersion", "taskId", "correlationId", "sourceRef"]);
    if (!isChannelRuntimeContractVersion(data.contractVersion)) malformed();
    const source = plainData(data.sourceRef);
    const keys = Object.keys(source).sort().join(",");
    const parsed: ChannelTaskSource = source.system === "linear" && keys === "agentSessionId,issueId,organizationId,system"
        ? { system: "linear", domain: opaque(source.organizationId), conversationRef: opaque(source.agentSessionId), issueId: opaque(source.issueId) }
        : malformed();
    return frozen({ contractVersion: data.contractVersion, taskId: opaque(data.taskId), correlationId: opaque(data.correlationId), source: parsed });
  }
  /** The persisted scope domain of one conversation row. */
  #domainOf(row: Row): ChannelConversationDomain {
    if (row.scope_system !== "linear") throw new ChannelRunChainStoreError("persisted_state_conflict");
    return frozen({ system: row.scope_system, domain: opaque(row.scope_domain) });
  }

  /**
   * The tenant of one conversation row. `tenant.system` is `scope_system`;
   * `#bindConversation` keeps the two equal.
   */
  #tenantOf(row: Row): TenantRef {
    if (row.scope_system !== "linear") throw new ChannelRunChainStoreError("persisted_state_conflict");
    return frozen({ system: row.scope_system, tenantId: opaque(row.tenant_id) });
  }

  #conversation(conversationId: string): ChannelRunChainConversationState | null {
    const row = this.#db.prepare("SELECT * FROM channel_run_chain_conversations WHERE conversation_id = ?").get(conversationId) as Row | undefined;
    if (row === undefined) return null;
    try {
      if (row.contract_version !== CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION || typeof row.created_at !== "string" || new Date(row.created_at).toISOString() !== row.created_at) throw new Error();
      if (row.scope_system !== "linear") throw new Error();
      return frozen({
        scope: { system: row.scope_system, domain: opaque(row.scope_domain) },
        tenant: { system: row.scope_system, tenantId: opaque(row.tenant_id) },
        sourceIssueId: row.source_issue_id === null ? null : opaque(row.source_issue_id),
        taskId: row.task_id === null ? null : opaque(row.task_id), correlationId: row.correlation_id === null ? null : opaque(row.correlation_id),
        currentRunId: row.current_run_id === null ? null : opaque(row.current_run_id), pendingCommandId: row.pending_command_id === null ? null : opaque(row.pending_command_id),
      });
    } catch { throw new ChannelRunChainStoreError("persisted_state_conflict"); }
  }
  /**
   * Whether this conversation's latest `start`/`continue` reservation settled
   * `rejected` with `axis.channel.outcome_unconfirmed`. One indexed read by
   * `sequence`. Cancels are excluded: an unconfirmed stop asserts nothing about
   * whether its Run is running, so it does not guard the conversation.
   */
  #sessionUnconfirmed(conversationId: string): boolean {
    const row = this.#db.prepare(
      "SELECT status, safe_code FROM channel_run_chain_reservations WHERE conversation_id = ? AND kind IN ('start', 'continue') ORDER BY sequence DESC LIMIT 1",
    ).get(conversationId) as Row | undefined;
    return row !== undefined && row.status === "rejected" && row.safe_code === OUTCOME_UNCONFIRMED_SAFE_CODE;
  }
  #reservation(commandId: string): ChannelRunChainReservation | null {
    const row = this.#db.prepare("SELECT * FROM channel_run_chain_reservations WHERE command_id = ?").get(commandId) as Row | undefined;
    return row === undefined ? null : this.#reservationRow(row);
  }
  #reservationRow(row: Row): ChannelRunChainReservation {
    try {
      const status = row.status;
      if (row.contract_version !== CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION || typeof status !== "string" || !reservationStatuses.includes(status as ChannelRunChainReservationStatus)) throw new Error();
      if (typeof row.reserved_at !== "string" || new Date(row.reserved_at).toISOString() !== row.reserved_at) throw new Error();
      if (status === "reserved" ? row.settled_at !== null : !(typeof row.settled_at === "string" && new Date(row.settled_at).toISOString() === row.settled_at)) throw new Error();
      if (!Number.isSafeInteger(row.sequence) || (row.sequence as number) < 1) throw new Error();
      // A start has its policy selection here or none.
      const selectedWorker = (row.worker_selection_json ?? null) === null
        ? undefined
        : workerSelection(parsedWorkerSelectionJson(row.worker_selection_json));
      if (selectedWorker !== undefined && row.kind !== "start") throw new Error();
      return frozen({
        contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, sequence: row.sequence as number, conversationId: opaque(row.conversation_id), commandId: opaque(row.command_id),
        kind: commandKind(row.kind), intentDigest: digestValue(row.intent_digest),
        ...(row.instruction_digest === null ? {} : { instructionDigest: digestValue(row.instruction_digest) }),
        ...(selectedWorker === undefined ? {} : { workerSelection: selectedWorker }),
        ...(row.prior_run_id === null ? {} : { priorRunId: opaque(row.prior_run_id) }),
        ...(row.interrupt_run_id === null ? {} : { interruptRunId: opaque(row.interrupt_run_id) }),
        ...(row.decision_id === null ? {} : { decisionId: opaque(row.decision_id) }),
        status: status as ChannelRunChainReservationStatus,
        ...(row.run_id === null ? {} : { runId: opaque(row.run_id) }),
        ...(row.task_id === null ? {} : { taskId: opaque(row.task_id) }),
        ...(row.safe_code === null ? {} : { safeCode: safeCodeValue(row.safe_code) }),
        reservedAt: row.reserved_at,
        ...(row.settled_at === null ? {} : { settledAt: row.settled_at as string }),
      });
    } catch { throw new ChannelRunChainStoreError("persisted_state_conflict"); }
  }
  #runById(runId: string): ChannelRunChainEntry | null {
    const row = this.#db.prepare("SELECT * FROM channel_run_chain_runs WHERE run_id = ?").get(runId) as Row | undefined;
    return row === undefined ? null : this.#runRow(row);
  }
  #runRow(row: Row): ChannelRunChainEntry {
    try {
      if (row.contract_version !== CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION || typeof row.started_at !== "string" || new Date(row.started_at).toISOString() !== row.started_at) throw new Error();
      if (typeof row.updated_at !== "string" || new Date(row.updated_at).toISOString() !== row.updated_at) throw new Error();
      if (!Number.isSafeInteger(row.sequence) || (row.sequence as number) < 1) throw new Error();
      return frozen({
        contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, sequence: row.sequence as number, conversationId: opaque(row.conversation_id), runId: opaque(row.run_id),
        ...(row.prior_run_id === null ? {} : { priorRunId: opaque(row.prior_run_id) }),
        taskId: opaque(row.task_id), commandId: opaque(row.command_id), state: runState(row.state), startedAt: row.started_at, updatedAt: row.updated_at,
      });
    } catch { throw new ChannelRunChainStoreError("persisted_state_conflict"); }
  }
  #decisionByRun(runId: string): ChannelWaitingDecision | null {
    const row = this.#db.prepare("SELECT * FROM channel_run_chain_decisions WHERE run_id = ?").get(runId) as Row | undefined;
    return row === undefined ? null : this.#decisionRow(row);
  }
  #decisionRow(row: Row): ChannelWaitingDecision {
    try {
      const status = row.status;
      if (row.contract_version !== CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION || typeof status !== "string" || !["waiting", "claimed", "resolved", "cancelled"].includes(status)) throw new Error();
      if (typeof row.raised_at !== "string" || new Date(row.raised_at).toISOString() !== row.raised_at) throw new Error();
      if (row.settled_at !== null && !(typeof row.settled_at === "string" && new Date(row.settled_at).toISOString() === row.settled_at)) throw new Error();
      return frozen({
        contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, conversationId: opaque(row.conversation_id), runId: opaque(row.run_id), decisionId: opaque(row.decision_id),
        elicitationDigest: digestValue(row.elicitation_digest), status: status as ChannelWaitingDecisionStatus,
        ...(row.claimed_by_command_id === null ? {} : { claimedByCommandId: opaque(row.claimed_by_command_id) }),
        raisedAt: row.raised_at,
        ...(row.settled_at === null ? {} : { settledAt: row.settled_at as string }),
      });
    } catch { throw new ChannelRunChainStoreError("persisted_state_conflict"); }
  }
  #assertOpen(): void { this.#connection.assertOpen(); }

  /**
   * The guard every public method runs: the handle is open, the last full check
   * did not fail, and the schema is still exactly this build's. `readyForTraffic()`
   * is this method in a try/catch. `./channel-conversation.ts` has the same shape.
   *
   * The full check (`verifyIntegrity()`) runs at construction, on
   * `../relay/integrity-cadence.ts`'s cadence from the observe ticks, and from
   * `/health/diagnostics`. Refusal is per call, through `#invariantFault`. Data
   * damage between two full checks goes unnoticed for up to
   * `max(integrity interval, tick interval)` plus one tick; schema damage is
   * caught on the next call. Every long-lived construction is in a channel
   * composition whose loops always start, and the constructor's full check covers
   * the moment before the first tick.
   */
  #assertUsable(): void {
    this.#assertOpen();
    if (this.#invariantFault) throw new ChannelRunChainStoreError("persisted_state_conflict");
    this.#readSnapshot(() => this.#assertSchemaInvariants());
  }
  #assertInvariants(): void {
    try { this.#readSnapshot(() => this.#assertInvariantsUnsafe()); }
    catch (error) {
      // Latched before the rethrow, on every failure.
      this.#invariantFault = true;
      if (error instanceof ChannelRunChainStoreError || error instanceof ChannelSessionRefusalError) throw error;
      throw new ChannelRunChainStoreError("persisted_state_conflict");
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
    if (integrity?.integrity_check !== "ok") throw new ChannelRunChainStoreError("persisted_state_conflict");
    if (this.#db.prepare("PRAGMA foreign_key_check").all().length !== 0) throw new ChannelRunChainStoreError("persisted_state_conflict");
  }

  /** The schema objects and the version ledger; bounded by object count, and the whole of `readyForTraffic()`. */
  #assertSchemaInvariants(): void {
    assertCurrentSchema(this.#db);
  }

  /**
   * The cross-table drift queries; joins and grouped scans, so they stay off
   * `readyForTraffic()` with the pragmas.
   */
  #assertDataInvariants(): void {
    const versionDrift = this.#db.prepare(`SELECT 1 FROM channel_run_chain_conversations c LEFT JOIN channel_run_chain_runtime_versions v ON v.conversation_id = c.conversation_id WHERE (c.correlation_id IS NULL) != (v.runtime_contract_version IS NULL) LIMIT 1`).get();
    if (versionDrift !== undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    // The command slot and the reservation table must agree: a cleared slot with a
    // `reserved` reservation lets a second command dispatch, and a slot left set
    // wedges the conversation. A grouped join driven by
    // `channel_run_chain_reservations_conversation`.
    const slotDrift = this.#db.prepare(`SELECT 1 FROM channel_run_chain_conversations c
      LEFT JOIN channel_run_chain_reservations r ON r.conversation_id = c.conversation_id AND r.status = 'reserved'
      GROUP BY c.conversation_id, c.pending_command_id
      HAVING COUNT(r.command_id) != (CASE WHEN c.pending_command_id IS NULL THEN 0 ELSE 1 END)
        OR COUNT(CASE WHEN r.command_id = c.pending_command_id THEN 1 END) != (CASE WHEN c.pending_command_id IS NULL THEN 0 ELSE 1 END) LIMIT 1`).get();
    if (slotDrift !== undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    // The current-Run pointer must select a Run of the same conversation --
    // a foreign key alone would allow it to point at another conversation's.
    const pointerDrift = this.#db.prepare(`SELECT 1 FROM channel_run_chain_conversations c JOIN channel_run_chain_runs r ON r.run_id = c.current_run_id
      WHERE r.conversation_id != c.conversation_id LIMIT 1`).get();
    if (pointerDrift !== undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    // A Run may only chain onto an earlier predecessor in its own
    // conversation; `prior_run_id`'s UNIQUE constraint keeps the chain
    // linear, since a fork would give one conversation two competing
    // continuations of the same Run.
    const chainDrift = this.#db.prepare(`SELECT 1 FROM channel_run_chain_runs r JOIN channel_run_chain_runs p ON p.run_id = r.prior_run_id
      WHERE p.conversation_id != r.conversation_id OR p.sequence >= r.sequence LIMIT 1`).get();
    if (chainDrift !== undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    // A decision belongs to the conversation of the Run that raised it, and
    // at most one per conversation may be awaiting an answer.
    const decisionDrift = this.#db.prepare(`SELECT 1 FROM channel_run_chain_decisions d JOIN channel_run_chain_runs r ON r.run_id = d.run_id
      WHERE d.conversation_id != r.conversation_id LIMIT 1`).get();
    if (decisionDrift !== undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    const multipleOpenDecisions = this.#db.prepare(`SELECT 1 FROM channel_run_chain_decisions WHERE status IN ('waiting', 'claimed')
      GROUP BY conversation_id HAVING COUNT(*) > 1 LIMIT 1`).get();
    if (multipleOpenDecisions !== undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    // A Run row must have been created by a settled, accepted reservation
    // of its own conversation; anything else means the chain was written
    // around the reservation slot that exists to serialize it.
    const runProvenanceDrift = this.#db.prepare(`SELECT 1 FROM channel_run_chain_runs r JOIN channel_run_chain_reservations v ON v.command_id = r.command_id
      WHERE v.conversation_id != r.conversation_id OR v.status != 'accepted' OR v.run_id != r.run_id LIMIT 1`).get();
    if (runProvenanceDrift !== undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
  }
  #transaction<T>(operation: () => T): T {
    this.#assertOpen();
    if (this.#connection.inTransaction) return operation();
    try { this.#db.exec("BEGIN IMMEDIATE"); }
    catch { throw new ChannelRunChainStoreError("persisted_state_conflict"); }
    this.#connection.inTransaction = true;
    try {
      const result = operation();
      try { this.#db.exec("COMMIT"); } catch { try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ } throw new ChannelRunChainStoreError("persisted_state_conflict"); }
      return result;
    } catch (error) {
      try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ }
      if (error instanceof ChannelRunChainStoreError || error instanceof ChannelSessionRefusalError) throw error;
      if (error instanceof ChannelRunChainInputError) throw error;
      throw new ChannelRunChainStoreError("persisted_state_conflict");
    } finally { this.#connection.inTransaction = false; }
  }
  /** Multi-statement reads share one snapshot. */
  #readSnapshot<T>(operation: () => T): T { return this.#connection.readSnapshot(operation); }
}

/**
 * The operator's read-only view of recorded redispatch give-ups. Opens the file
 * `{ readOnly: true }`, because constructing the store would create a schema.
 * Reads the disposition table alone, so a deleted marker never hides a give-up,
 * and the table has no payload column, so no instruction body can be returned.
 */
export class ChannelRedispatchDispositions {
  readonly #db: DatabaseSync;
  #closed = false;

  constructor(filename: string) {
    // Wrapped so this class keeps the module's error contract: `node:sqlite` throws a
    // bare `Error` for a missing or non-database file.
    try { this.#db = new DatabaseSync(filename, { readOnly: true }); }
    catch { throw new ChannelRunChainStoreError("persisted_state_conflict"); }
  }

  /** Refuses a file that is not a run-chain database. */
  #assertRunChainDatabase(): void {
    assertCurrentSchema(this.#db);
  }

  /**
   * Recorded redispatch give-ups, oldest first, after the exclusive
   * `afterSequence`, at most `limit`. Out-of-range input is refused, not clamped.
   */
  list(input: { readonly afterSequence?: number; readonly limit?: number } = {}): readonly ChannelRedispatchDisposition[] {
    const data = plainData(input);
    if (Object.keys(data).some((key) => !["afterSequence", "limit"].includes(key))) malformed();
    const afterSequence = data.afterSequence === undefined ? 0 : data.afterSequence;
    const limit = data.limit === undefined ? CHANNEL_DISPOSITION_PAGE_LIMIT : data.limit;
    if (typeof afterSequence !== "number" || !Number.isSafeInteger(afterSequence) || afterSequence < 0) malformed();
    if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > CHANNEL_DISPOSITION_PAGE_LIMIT) malformed();
    // A listing on a closed handle is a caller bug: `persisted_state_conflict`.
    if (this.#closed) throw new ChannelRunChainStoreError("persisted_state_conflict");
    this.#assertRunChainDatabase();
    return frozen((this.#db.prepare(`
      SELECT sequence, conversation_id, command_id, attempts, failure_code, given_up_at
      FROM channel_run_chain_redispatch_dispositions WHERE sequence > ? ORDER BY sequence LIMIT ?
    `).all(afterSequence, limit) as Row[]).map((row) => frozen({
      sequence: Number(row.sequence),
      conversationId: opaque(row.conversation_id),
      commandId: opaque(row.command_id),
      attempts: Number(row.attempts),
      // Validated against `RETRY_BUDGET_EXHAUSTED_CODE`; any other code is damaged state.
      failureCode: retryBudgetExhaustedCode(row.failure_code),
      givenUpAt: String(row.given_up_at),
    })));
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#db.close();
  }
}
