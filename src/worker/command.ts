import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
  agentActivityContractVersion,
  agentActivityFallbackResponseBody,
  validateAgentActivitySourceEvent,
  validateAgentActivityResponseReceipt,
  validateRunAgentResponseDelivery,
} from "@axisosme/axis-contracts/agent-activity";
import type { AgentActivityResponseReceipt, RunAgentResponseDelivery } from "@axisosme/axis-contracts/agent-activity";
import { runtimeSourceSessionIdentity, validateRuntimeBoundaryMessage } from "../shared/runtime-boundary.js";
import type {
  RunAcceptedEnvelope,
  RunFailedEnvelope,
  RunProgressedEnvelope,
  RunStartEnvelope,
  RunSucceededEnvelope,
  RuntimeError,
  SourceRef,
} from "../shared/runtime-boundary.js";

import {
  CODEX_EXECUTOR_CONTRACT_VERSION,
  executorError,
  ExecutorLifecycleError,
  executorLifecycleReason,
  normalizeExecutorCommentary,
  selectExecutorRegistration,
  type ExecutorCapability,
  type ExecutorContinueResult,
  type ExecutorLifecycleEvent,
  type ExecutorLiveBinding,
  type ExecutorErrorCode,
  type ExecutorRecoveryRequest,
  type ExecutorRecoveryResult,
  type ExecutorRegistration,
  type ExecutorStartResult,
  type ExecutorTerminalResponse,
} from "../shared/worker-executor.js";
import { validateStructuredElicitation } from "../shared/elicitation.js";
import { RouterEventRefusedError, WORKER_COMMAND_VERSION, WORKER_WAITING_EXTENSION_VERSION, isWorkerOpaqueIdentifier, type RouterRunEventSink, type RouterRunLifecycleEnvelope, type WorkerCommand, type WorkerCommandSteer, type WorkerIngressCommand, type WorkerIngressResponse, type WorkerCommandContractVersion, type WorkerExecutionTarget, type WorkerCommandResponse, type WorkerExecutionIdentity, type WorkerRunConfiguration, type WorkerRunWaitingForInputEnvelope, type WorkerVersionedReference, type WorkerWorkspaceConfiguration } from "../shared/router-worker.js";
import { assertWorkerSchema, initializeWorkerSchema } from "./schema.js";
import { normalizeWorkerWorkspaceConfigurations, resolveWorkerWorkspace, workerWorkspaceKey, type WorkerWorkspaceResolutionOutcome, type WorkerWorkspaceResolutionReason, type WorkerWorkspaceSystem } from "./workspace-policy.js";

export type WorkerCommandErrorCode =
  | "axis.worker.malformed"
  | "axis.worker.policy_forbidden"
  | "axis.worker.identity_mismatch"
  | "axis.worker.intent_conflict"
  | "axis.worker.recovery_required"
  | "axis.worker.operation_unsupported"
  | "axis.worker.adapter_invalid"
  | "axis.worker.launch_ambiguous"
  | "axis.worker.runtime_missing"
  | "axis.worker.runtime_incompatible"
  | "axis.worker.authentication_required"
  | "axis.worker.probe_timeout"
  | "axis.worker.capability_unavailable"
  | "axis.worker.workspace_unavailable"
  | "axis.worker.launch_failed"
  | "axis.worker.continuation_unresumable";

const safeMessages: Readonly<Record<WorkerCommandErrorCode, string>> = {
  "axis.worker.malformed": "The Worker command is malformed.",
  "axis.worker.policy_forbidden": "The Worker command is not permitted by policy.",
  "axis.worker.identity_mismatch": "The Worker's configured identity does not match the command's execution target.",
  "axis.worker.intent_conflict": "The Worker command ID is already bound to different intent.",
  "axis.worker.recovery_required": "The Worker retained a command that cannot be resumed.",
  "axis.worker.operation_unsupported": "The executor operation is not supported.",
  "axis.worker.adapter_invalid": "The executor adapter returned an invalid lifecycle.",
  "axis.worker.launch_ambiguous": "The executor command outcome is ambiguous.",
  "axis.worker.runtime_missing": "The configured executor runtime is unavailable.",
  "axis.worker.runtime_incompatible": "The configured executor runtime is incompatible.",
  "axis.worker.authentication_required": "The configured executor runtime requires authentication.",
  "axis.worker.probe_timeout": "The executor preflight probe did not respond in time.",
  "axis.worker.capability_unavailable": "The configured executor capability is unavailable.",
  "axis.worker.workspace_unavailable": "No trusted Worker workspace is available for this run.",
  "axis.worker.launch_failed": "The executor launch failed before execution began.",
  "axis.worker.continuation_unresumable": "The prior native session for this continuation cannot be resumed.",
};

export class WorkerCommandError extends Error {
  constructor(readonly code: WorkerCommandErrorCode, readonly statusCode: 400 | 403 | 409 | 502 | 503) {
    super(safeMessages[code]);
    this.name = "WorkerCommandError";
  }
}

export interface WorkerCommandOptions {
  readonly configuration: WorkerRunConfiguration;
  readonly registrations: readonly ExecutorRegistration[];
  readonly eventSink: RouterRunEventSink;
  readonly journal?: WorkerCommandJournal;
  readonly now?: () => Date;
  readonly messageId?: () => string;
  /** Deterministic commentary scheduling seam. Production uses one unref'd timeout per pending Run. */
  readonly scheduleCommentary?: (callback: () => void, delayMs: number) => unknown;
  readonly cancelCommentarySchedule?: (handle: unknown) => void;
  /** Deterministic test seam that may delay, but never replaces, terminal Runtime v1 validation. */
  readonly beforeTerminalValidation?: () => Promise<void>;
}

/**
 * Why a Run was isolated as `observational`, one label per reason Worker itself can
 * produce. Executor-produced reasons arrive on the thrown `ExecutorLifecycleError`
 * and retain the adapter's fixed reason label, so the prefix says which layer decided. `observational` is a permanent fail-closed
 * isolation. These strings are persisted and read by operators: fixed labels only,
 * never text built from executor output.
 */
const WORKER_OBSERVATIONAL_CAUSES = {
  /** The executor's lifecycle iterator ended without ever producing a terminal event. */
  lifecycleEnded: "worker.lifecycle_ended",
  /** An event that is not a valid lifecycle event for this binding/sequence. */
  invalidLifecycleEvent: "worker.invalid_lifecycle_event",
  /** The registration refused to project a `progressed` event. */
  invalidProgressProjection: "worker.invalid_progress_projection",
  /** Building the `run.progressed` envelope for a generic progress fact failed. */
  progressEnvelopeFailed: "worker.progress_envelope_failed",
  /** The registration refused to project a `waiting_for_input` event. */
  invalidWaitingProjection: "worker.invalid_waiting_projection",
  /** The registration refused to project a `succeeded` event. */
  invalidSucceededProjection: "worker.invalid_succeeded_projection",
  /** The registration refused to project a `failed`/`cancelled` event. */
  invalidFailedProjection: "worker.invalid_failed_projection",
  /** A lifecycle outcome this Worker has no branch for. */
  unexpectedLifecycleOutcome: "worker.unexpected_lifecycle_outcome",
  /** The terminal envelope could not be journaled. */
  terminalNotJournaled: "worker.terminal_not_journaled",
  /** `start`/`continue` on the adapter threw. */
  launchThrew: "worker.launch_threw",
  /** The adapter returned a failure it classifies as ambiguous. */
  launchFailureAmbiguous: "worker.launch_failure_ambiguous",
  /** The adapter returned a result that does not match the request, or an invalid binding. */
  launchResultUnusable: "worker.launch_result_unusable",
  /** The native binding could not be journaled. */
  bindingNotJournaled: "worker.binding_not_journaled",
  /** `observe()` did not produce a valid `started` event as its first event. */
  observeStartInvalid: "worker.observe_start_invalid",
  /** `run.accepted` could not be journaled. */
  acceptedNotJournaled: "worker.accepted_not_journaled",
  /** A restart with no `recover` registered. */
  recoveryUnsupported: "worker.recovery_unsupported",
  /** The retained Run's own start envelope could not be rebuilt. */
  recoveryStartUnreadable: "worker.recovery_start_unreadable",
  /** The adapter's recovery result does not match the Run it was asked about. */
  recoveryResultUnusable: "worker.recovery_result_unusable",
  /** The adapter claimed the Run is still active but handed back no usable observer. */
  recoveryObserverUnusable: "worker.recovery_observer_unusable",
  /** The adapter's recovered terminal could not be validated or projected. */
  recoveryTerminalUnusable: "worker.recovery_terminal_unusable",
  /** Recovery threw. */
  recoveryThrew: "worker.recovery_threw",
  /** A recovered terminal was projected but could not be journaled. */
  recoveredTerminalNotJournaled: "worker.recovered_terminal_not_journaled",
  /** A retry of a quarantine whose earlier write did not prove durable. */
  recoveryQuarantineRetry: "worker.recovery_quarantine_retry",
  /** Reached only by an error carrying no label: something threw that no branch above names. */
  unclassified: "worker.unclassified_supervise_error",
} as const;

/**
 * Any key whose label is spelled the same as another key's; `never` when every label
 * is distinct.
 */
type DuplicateWorkerObservationalCause = {
  [K in keyof typeof WORKER_OBSERVATIONAL_CAUSES]: (typeof WORKER_OBSERVATIONAL_CAUSES)[K] extends (typeof WORKER_OBSERVATIONAL_CAUSES)[Exclude<keyof typeof WORKER_OBSERVATIONAL_CAUSES, K>] ? K : never;
}[keyof typeof WORKER_OBSERVATIONAL_CAUSES];

/**
 * A cause this Worker names itself: one of the table's values. Collapses to `never`
 * if two keys share a label, so no call site compiles.
 */
type WorkerObservationalCause = [DuplicateWorkerObservationalCause] extends [never]
  ? (typeof WORKER_OBSERVATIONAL_CAUSES)[keyof typeof WORKER_OBSERVATIONAL_CAUSES]
  : never;

/**
 * The label a thrown error carries, or `fallback` when it carries none. The fallback
 * is its own label, so a cause never claims a mechanism it did not observe. Returns
 * `string` because an executor's label is authored outside this module.
 */
function observationalCause(error: unknown, fallback: WorkerObservationalCause = WORKER_OBSERVATIONAL_CAUSES.unclassified): string {
  return executorLifecycleReason(error) ?? fallback;
}

/**
 * One reason the contract refused a projected agent response, as
 * `@axisosme/axis-contracts` produced it. None of these fields can carry answer
 * text: `instancePath` is a JSON pointer, `keyword` and `schemaId` name the rule,
 * and `safeMessage` is one of the contract's fixed sentences, so the record is safe
 * to persist. `code` is `string` because the row may be read by a different build.
 */
export interface WorkerResponseProjectionDiagnostic {
  readonly code: string;
  readonly instancePath: string;
  readonly schemaId: string | null;
  readonly keyword: string | null;
  readonly safeMessage: string;
}
/** Why one Run's agent response was replaced by the fixed fallback body. */
export interface WorkerResponseProjectionRefusal {
  readonly runId: string;
  readonly sourceEventId: string;
  readonly diagnostics: readonly WorkerResponseProjectionDiagnostic[];
  readonly occurredAt: string;
}

export interface WorkerCommandJournal {
  close(): void;
  coordinatorKey(): string;
  beginCommand(input: { readonly commandId: string; readonly intentJson: string; readonly intentDigest: string; readonly occurredAt: string }): WorkerCommandJournalClaim;
  readCommand(commandId: string): WorkerCommandRecord | undefined;
  /**
   * Records a command's response. `recordAccepted` already commits the response of an
   * accepted start or continue, so a byte-identical response for an already-recorded
   * command is a no-op; any other response for it throws.
   */
  recordCommandResponse(input: { readonly commandId: string; readonly response: WorkerIngressResponse; readonly occurredAt: string }): void;
  reserveRun(input: { readonly runId: string; readonly intentJson: string; readonly intentDigest: string; readonly occurredAt: string }): WorkerCommandRunClaim;
  readRun(runId: string): WorkerCommandRunRecord | undefined;
  /** Every retained record that may have crossed native admission and is not terminal. */
  listRecoverableRuns(): readonly WorkerCommandRunRecord[];
  /**
   * Commits the command's response in the same transaction as the Run's acceptance
   * and its `run.accepted` outbox row, so an accepted Run always has a recorded
   * response for a read-only query.
   */
  recordAccepted(input: { readonly runId: string; readonly accepted: RunAcceptedEnvelope; readonly commandId: string; readonly response: WorkerCommandResponse; readonly occurredAt: string }): void;
  recordProgress(input: { readonly runId: string; readonly progressed: RunProgressedEnvelope; readonly occurredAt: string }): void;
  recordBinding(input: { readonly runId: string; readonly binding: unknown; readonly occurredAt: string }): void;
  recordTerminal(input: { readonly runId: string; readonly terminal: RunSucceededEnvelope | RunAgentResponseDelivery | WorkerRunWaitingForInputEnvelope | RunFailedEnvelope; readonly occurredAt: string }): void;
  /** Isolates the Run and records why, in the same transaction. `cause` is required. */
  recordObservational(input: { readonly runId: string; readonly cause: string; readonly occurredAt: string }): void;
  /** The cause recorded beside an isolated Run, or `undefined`. */
  readObservationalCause(runId: string): string | undefined;
  /**
   * Records why a projected agent response was refused, so the fixed fallback body a
   * user reads can be traced to the rule that fired. Required, so every journal
   * implementation covers this path.
   */
  recordResponseProjectionRefusal(input: WorkerResponseProjectionRefusal): void;
  /**
   * The refusal recorded beside a Run, or `undefined` when its response projected
   * or the stored row cannot be read as diagnostics.
   */
  readResponseProjectionRefusal(runId: string): WorkerResponseProjectionRefusal | undefined;
  recordWorkspaceResolutionAudit(input: Omit<WorkerWorkspaceResolutionAudit, "contractVersion" | "sequence">): WorkerWorkspaceResolutionAudit;
  listWorkspaceResolutionAudits(input?: { readonly afterSequence?: number; readonly limit?: number }): readonly WorkerWorkspaceResolutionAudit[];
  listUndeliveredEvents(): readonly WorkerCommandOutboxEvent[];
  markEventDelivered(input: { readonly messageId: string; readonly occurredAt: string }): void;
  /**
   * Gives up on one persisted row this build can never parse, and says so. Required,
   * so every journal implementation covers this path.
   */
  quarantineEvent(input: { readonly messageId: string; readonly runId: string; readonly code: WorkerOutboxQuarantineCode; readonly quarantinedAt: string; readonly substitute: RunFailedEnvelope | null }): boolean;
  /**
   * Whether the row this build gave up on was the Run's own `run.accepted`, so Router
   * may hold no first event for a Run this Worker started.
   */
  runAcceptedQuarantined(runId: string): boolean;
  /** The operator-facing count. Reads the sidecar alone, so it still reports a disposition whose outbox row an operator has since deleted. */
  listQuarantineDispositions(input?: { readonly afterSequence?: number; readonly limit?: number }): readonly WorkerOutboxQuarantineDisposition[];
  /**
   * The retention proof. Joins the retained payload, so a disposition whose row was
   * deleted is absent; `listQuarantineDispositions` counts those.
   */
  listQuarantinedEvents(): readonly WorkerQuarantinedOutboxEvent[];
}

export const WORKER_WORKSPACE_RESOLUTION_AUDIT_VERSION = "1.0.0" as const;
export interface WorkerWorkspaceResolutionAudit {
  readonly contractVersion: typeof WORKER_WORKSPACE_RESOLUTION_AUDIT_VERSION;
  readonly sequence: number;
  readonly commandId: string;
  readonly taskId: string;
  readonly runId: string;

  readonly system: WorkerWorkspaceSystem;

  readonly organizationId: string;
  readonly nodeId: string;
  readonly workspacePolicyRef: WorkerVersionedReference | null;
  readonly outcome: WorkerWorkspaceResolutionOutcome;
  readonly reason: WorkerWorkspaceResolutionReason;
  readonly occurredAt: string;
}

export interface WorkerCommandRecord {
  readonly commandId: string;
  readonly intentJson: string;
  readonly intentDigest: string;
  readonly response: WorkerIngressResponse | undefined;
}

export interface WorkerCommandRunRecord {
  readonly runId: string;
  readonly intentJson: string;
  readonly intentDigest: string;
  readonly state: "reserved" | "binding" | "accepted" | "running" | "waiting_for_input" | "succeeded" | "failed" | "observational";
  /** Undefined only while a durable reservation exists before the provider returns a truthful binding. */
  readonly binding: unknown | undefined;
  readonly accepted: RunAcceptedEnvelope | undefined;
  readonly terminal: RunSucceededEnvelope | RunAgentResponseDelivery | WorkerRunWaitingForInputEnvelope | RunFailedEnvelope | undefined;
}

/** A redacted, immutable Router receipt waiting for idempotent delivery. */
export interface WorkerCommandOutboxEvent {
  readonly runId: string;
  readonly messageId: string;
  readonly event: RouterRunLifecycleEnvelope;
}

/**
 * The disposition of a persisted row whose processing failed, distinct from the
 * failure itself. `unprocessable`: this build reading this row will never
 * succeed, because the cause is inside the row (a surface this build does not
 * publish, a shape its closed validator rejects), so retrying stops and a visible
 * terminating action is taken. A transient cause outside the row is retried. The
 * disposition is recorded beside the row and `event_json` is never touched.
 */
export type WorkerOutboxQuarantineCode = typeof WORKER_OUTBOX_UNPROCESSABLE_CODE | typeof WORKER_OUTBOX_REFUSED_CODE;

/** Identity and disposition with no payload: what an operator needs to count the debt, and all this surface is allowed to print. */
export interface WorkerOutboxQuarantineDisposition {
  readonly sequence: number;
  readonly runId: string;
  readonly messageId: string;
  readonly code: WorkerOutboxQuarantineCode;
  readonly quarantinedAt: string;
}

/**
 * A retained undeliverable row plus its disposition, with the payload so a reader
 * can prove it was not rewritten. A row whose `event_json` is not readable as an
 * envelope carries the placeholder in `event`; read `worker_outbox` for its
 * bytes.
 */
export interface WorkerQuarantinedOutboxEvent {
  readonly runId: string;
  readonly messageId: string;
  readonly code: WorkerOutboxQuarantineCode;
  readonly quarantinedAt: string;
  readonly event: RouterRunLifecycleEnvelope;
}

export interface WorkerPendingCommentary { readonly runId: string; readonly dueAtMs: number; readonly messageId: string; readonly safeMessage: string; }
export type WorkerCommentaryJournalResult =
  | { readonly kind: "ignored" }
  | { readonly kind: "enqueued" }
  | { readonly kind: "pending"; readonly dueAtMs: number };

interface WorkerCommentaryJournalExtension {
  recordCommentary(input: { readonly runId: string; readonly digest: string; readonly progressed: RunProgressedEnvelope; readonly occurredAt: string; readonly occurredAtMs: number }): WorkerCommentaryJournalResult;
  flushPendingCommentary(input: { readonly runId: string; readonly occurredAt: string; readonly occurredAtMs: number }): WorkerCommentaryJournalResult;
  listPendingCommentary(): readonly WorkerPendingCommentary[];
  discardPendingCommentary(input: { readonly runId: string; readonly occurredAt: string }): void;
}

interface WorkerRecoveryJournalExtension { listRecoverableRuns(): readonly WorkerCommandRunRecord[]; }

export type WorkerCommandJournalClaim =
  | { readonly kind: "claimed"; readonly record: WorkerCommandRecord }
  | { readonly kind: "existing"; readonly record: WorkerCommandRecord }
  | { readonly kind: "intent_conflict"; readonly record: WorkerCommandRecord };
export type WorkerCommandRunClaim =
  | { readonly kind: "claimed"; readonly record: WorkerCommandRunRecord }
  | { readonly kind: "existing"; readonly record: WorkerCommandRunRecord }
  | { readonly kind: "intent_conflict"; readonly record: WorkerCommandRunRecord };

type JsonRecord = Readonly<Record<string, unknown>>;
const COMMENTARY_WINDOW_MS = 30_000;
/** Private startup bound; a dedicated referenced timer keeps recovery alive. */
const RECOVERY_TIMEOUT_MS = 30_000;
function record(value: unknown): JsonRecord | null { return typeof value === "object" && value !== null && !Array.isArray(value) ? value as JsonRecord : null; }
function text(value: unknown): string | null { return typeof value === "string" && value.trim().length > 0 ? value : null; }
function exactKeys(value: JsonRecord, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort(); const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}
function digest(value: string): string { return createHash("sha256").update(value).digest("hex"); }
function validWorkspaceResolutionPair(outcome: WorkerWorkspaceResolutionOutcome, reason: WorkerWorkspaceResolutionReason): boolean {
  return (outcome === "mapped" && reason === "catalog_match")
    || (outcome === "default_root" && reason === "no_ref")
    || (outcome === "fallback_root" && (reason === "catalog_miss" || reason === "mapped_path_unavailable"))
    || (outcome === "unavailable" && (reason === "root_unavailable" || reason === "catalog_miss" || reason === "mapped_path_unavailable"));
}
function validWorkspaceAuditInput(input: Omit<WorkerWorkspaceResolutionAudit, "contractVersion" | "sequence">): boolean {
  return [input.commandId, input.taskId, input.runId, input.organizationId, input.nodeId].every(isWorkerOpaqueIdentifier)
    && input.system === "linear"
    && (input.workspacePolicyRef === null || (isWorkerOpaqueIdentifier(input.workspacePolicyRef.ref) && isWorkerOpaqueIdentifier(input.workspacePolicyRef.version)))
    && validWorkspaceResolutionPair(input.outcome, input.reason)
    && ((input.workspacePolicyRef === null && ((input.outcome === "default_root" && input.reason === "no_ref") || (input.outcome === "unavailable" && input.reason === "root_unavailable")))
      || (input.workspacePolicyRef !== null && !((input.outcome === "default_root" && input.reason === "no_ref") || (input.outcome === "unavailable" && input.reason === "root_unavailable"))))
    && new Date(input.occurredAt).toISOString() === input.occurredAt;
}

function workspaceSourceKey(sourceRef: SourceRef): { readonly system: WorkerWorkspaceSystem; readonly id: string } {
  // Defence in depth: both versions require `organizationId` on their Linear member.
  // An unknown provider fails closed instead of being keyed as `linear` with
  // `"undefined"`.
  if (typeof (sourceRef as { readonly organizationId?: unknown }).organizationId !== "string") fail("axis.worker.policy_forbidden", 403);
  return { system: "linear", id: sourceRef.organizationId };
}
function deterministicActivityId(value: string): string { const hex = digest(value); return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`; }
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  const candidate = record(value);
  if (candidate !== null) return `{${Object.keys(candidate).sort().map((key) => `${JSON.stringify(key)}:${canonical(candidate[key])}`).join(",")}}`;
  return JSON.stringify(value);
}
function runtimeEnvelope(event: RouterRunLifecycleEnvelope): RunAcceptedEnvelope | RunProgressedEnvelope | RunSucceededEnvelope | WorkerRunWaitingForInputEnvelope | RunFailedEnvelope {
  return "deliveryType" in event ? event.envelope : event;
}
function lifecycleMessageId(event: RouterRunLifecycleEnvelope): string { return runtimeEnvelope(event).messageId; }
function fail(code: WorkerCommandErrorCode, statusCode: 400 | 403 | 409 | 502 | 503): never { throw new WorkerCommandError(code, statusCode); }

/**
 * Raised only where a closed published validator has decided a persisted row is
 * not a member of a surface this build publishes. An unexpected throw from a
 * validator is not this: it stays transient and retries.
 */
class UnprocessableOutboxEvent extends Error {}

/**
 * Why no substitute could be built: the announcement is ready, this Run can never
 * produce one, this Run has not finished and will produce its own, or the attempt
 * failed for a reason outside the Run and must be retried.
 */
type SubstituteOutcome =
  | { readonly kind: "announce"; readonly envelope: RunFailedEnvelope }
  | { readonly kind: "unannounceable" }
  | { readonly kind: "unsettled" }
  | { readonly kind: "transient" };

/** This build could not read the row. `WorkerOutboxQuarantineCode` is `typeof` these, so a code cannot be added by restating a literal. */
export const WORKER_OUTBOX_UNPROCESSABLE_CODE = "axis.worker.outbox_unprocessable" as const;
/** This build read the row, and the Router refused it permanently. Kept distinct from the code above because the two have different causes and different operator answers: the first may become deliverable under another build, the second never will under any. */
export const WORKER_OUTBOX_REFUSED_CODE = "axis.worker.outbox_refused" as const;

/**
 * True only for a terminal this build minted to announce its own give-up.
 *
 * A Router 409 (`axis.router.conflict`) is either a permanent ordering refusal or
 * a rewritten `axis.router.not_found` from a Router whose database is not ready,
 * and the wire cannot tell them apart. So a persisted Run event is never given up
 * on for a 409; only an announcement is, which loses at most the announcement.
 *
 * Recognised from the row itself: `#unprocessableTerminal` is the only producer of
 * a `run.failed` with this code.
 */
function ownAnnouncement(event: RouterRunLifecycleEnvelope): boolean {
  if (!("envelopeType" in event) || event.envelopeType !== "run.failed") return false;
  return record((event as RunFailedEnvelope).error)?.code === WORKER_OUTBOX_UNPROCESSABLE_CODE;
}

function outboxQuarantineCode(value: unknown): WorkerOutboxQuarantineCode {
  if (value !== WORKER_OUTBOX_UNPROCESSABLE_CODE && value !== WORKER_OUTBOX_REFUSED_CODE) throw new Error("invalid outbox quarantine code");
  return value;
}

/**
 * The value a persisted outbox row decodes to when its bytes are not readable as
 * an envelope. It has no `deliveryType` or `envelopeType`, so
 * `#decodeUndeliveredEvent` routes it to `validateRuntimeBoundaryMessage`, which
 * returns invalid, and the row takes the ordinary unprocessable branch with its
 * own `messageId` and `runId` (both read from their own columns). Frozen.
 */
const UNREADABLE_OUTBOX_PAYLOAD = Object.freeze({}) as unknown as RouterRunLifecycleEnvelope;

/**
 * `JSON.parse` for one row, never the batch, so one row's bytes cannot stall the
 * outbox. A parse that succeeds into a non-object is unreadable too: only a plain
 * object is passed through.
 */
function outboxPayload(eventJson: string): RouterRunLifecycleEnvelope {
  let parsed: unknown;
  try { parsed = JSON.parse(eventJson); } catch { return UNREADABLE_OUTBOX_PAYLOAD; }
  return record(parsed) === null ? UNREADABLE_OUTBOX_PAYLOAD : parsed as RouterRunLifecycleEnvelope;
}

/**
 * The stored refusal diagnostics, or `null` when the row is not that shape. Every
 * dereferenced field is checked; `code` stays `string`.
 */
function responseProjectionDiagnostics(diagnosticsJson: string): readonly WorkerResponseProjectionDiagnostic[] | null {
  let parsed: unknown;
  try { parsed = JSON.parse(diagnosticsJson); } catch { return null; }
  if (!Array.isArray(parsed)) return null;
  const diagnostics: WorkerResponseProjectionDiagnostic[] = [];
  for (const entry of parsed) {
    const candidate = record(entry);
    if (candidate === null || typeof candidate.code !== "string" || typeof candidate.instancePath !== "string" || typeof candidate.safeMessage !== "string") return null;
    if (!(candidate.schemaId === null || typeof candidate.schemaId === "string") || !(candidate.keyword === null || typeof candidate.keyword === "string")) return null;
    diagnostics.push({ code: candidate.code, instancePath: candidate.instancePath, schemaId: candidate.schemaId, keyword: candidate.keyword, safeMessage: candidate.safeMessage });
  }
  return diagnostics;
}

export class SqliteWorkerCommandJournal implements WorkerCommandJournal, WorkerCommentaryJournalExtension {
  readonly #db: DatabaseSync;
  readonly #key: string;
  constructor(filename: string, options: { readonly readOnly?: boolean } = {}) {
    this.#db = options.readOnly === true ? new DatabaseSync(filename, { readOnly: true }) : new DatabaseSync(filename); this.#key = filename === ":memory:" ? `memory:${Math.random()}` : resolve(filename);
    try {
      if (options.readOnly === true) assertWorkerSchema(this.#db);
      else {
        initializeWorkerSchema(this.#db);
        this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
      }
    } catch {
      this.#db.close();
      throw new WorkerCommandError("axis.worker.recovery_required", 503);
    }
  }
  close(): void { this.#db.close(); }
  coordinatorKey(): string { return this.#key; }
  beginCommand(input: { readonly commandId: string; readonly intentJson: string; readonly intentDigest: string; readonly occurredAt: string }): WorkerCommandJournalClaim {
    return this.#transaction(() => {
      const current = this.readCommand(input.commandId);
      if (current !== undefined) return current.intentJson === input.intentJson && current.intentDigest === input.intentDigest ? { kind: "existing", record: current } : { kind: "intent_conflict", record: current };
      this.#db.prepare("INSERT INTO worker_commands VALUES (?, ?, ?, NULL, ?, ?)").run(input.commandId, input.intentJson, input.intentDigest, input.occurredAt, input.occurredAt);
      const inserted = this.readCommand(input.commandId); if (inserted === undefined) throw new Error("command journal lost claim"); return { kind: "claimed", record: inserted };
    });
  }
  readCommand(commandId: string): WorkerCommandRecord | undefined {
    const row = this.#db.prepare("SELECT * FROM worker_commands WHERE command_id = ?").get(commandId) as { command_id: string; intent_json: string; intent_digest: string; response_json: string | null } | undefined;
    return row === undefined ? undefined : { commandId: row.command_id, intentJson: row.intent_json, intentDigest: row.intent_digest, response: row.response_json === null ? undefined : JSON.parse(row.response_json) as WorkerCommandResponse };
  }
  /** Idempotent for the byte-identical response `recordAccepted` already committed; any other response throws. */
  recordCommandResponse(input: { readonly commandId: string; readonly response: WorkerIngressResponse; readonly occurredAt: string }): void {
    this.#transaction(() => {
      const responseJson = JSON.stringify(input.response);
      const current = this.#db.prepare("SELECT response_json FROM worker_commands WHERE command_id = ?").get(input.commandId) as { response_json: string | null } | undefined;
      if (current !== undefined && current.response_json === responseJson) return;
      this.#update("UPDATE worker_commands SET response_json = ?, updated_at = ? WHERE command_id = ? AND response_json IS NULL", responseJson, input.occurredAt, input.commandId);
    });
  }
  reserveRun(input: { readonly runId: string; readonly intentJson: string; readonly intentDigest: string; readonly occurredAt: string }): WorkerCommandRunClaim {
    return this.#transaction(() => {
      const current = this.readRun(input.runId);
      if (current !== undefined) return current.intentJson === input.intentJson && current.intentDigest === input.intentDigest ? { kind: "existing", record: current } : { kind: "intent_conflict", record: current };
      this.#db.prepare("INSERT INTO worker_runs VALUES (?, ?, ?, 'reserved', NULL, NULL, NULL, ?, ?)").run(input.runId, input.intentJson, input.intentDigest, input.occurredAt, input.occurredAt);
      const inserted = this.readRun(input.runId); if (inserted === undefined) throw new Error("run journal lost claim"); return { kind: "claimed", record: inserted };
    });
  }
  readRun(runId: string): WorkerCommandRunRecord | undefined {
    const row = this.#db.prepare("SELECT * FROM worker_runs WHERE run_id = ?").get(runId) as { run_id: string; intent_json: string; intent_digest: string; state: WorkerCommandRunRecord["state"]; binding_json: string | null; accepted_json: string | null; terminal_json: string | null } | undefined;
    if (row === undefined) return undefined;
    const waiting = this.#db.prepare("SELECT terminal_json FROM worker_waiting_runs WHERE run_id = ?").get(runId) as { terminal_json: string } | undefined;
    return { runId: row.run_id, intentJson: row.intent_json, intentDigest: row.intent_digest, state: waiting === undefined ? row.state : "waiting_for_input", binding: row.binding_json === null ? undefined : JSON.parse(row.binding_json) as unknown, accepted: row.accepted_json === null ? undefined : JSON.parse(row.accepted_json) as RunAcceptedEnvelope, terminal: waiting === undefined ? (row.terminal_json === null ? undefined : JSON.parse(row.terminal_json) as RunSucceededEnvelope | RunAgentResponseDelivery | RunFailedEnvelope) : JSON.parse(waiting.terminal_json) as WorkerRunWaitingForInputEnvelope };
  }
  listRecoverableRuns(): readonly WorkerCommandRunRecord[] {
    const rows = this.#db.prepare("SELECT run_id FROM worker_runs WHERE state IN ('binding','accepted','running') ORDER BY created_at ASC, run_id ASC").all() as unknown as readonly { run_id: string }[];
    return rows.map(({ run_id }) => this.readRun(run_id)).filter((value): value is WorkerCommandRunRecord => value !== undefined);
  }
  recordAccepted(input: { readonly runId: string; readonly accepted: RunAcceptedEnvelope; readonly commandId: string; readonly response: WorkerCommandResponse; readonly occurredAt: string }): void {
    this.#transaction(() => {
      this.#update("UPDATE worker_runs SET state = 'accepted', accepted_json = ?, updated_at = ? WHERE run_id = ? AND state = 'binding'", JSON.stringify(input.accepted), input.occurredAt, input.runId);
      this.#enqueue(input.runId, input.accepted, input.occurredAt);
      this.#update("UPDATE worker_commands SET response_json = ?, updated_at = ? WHERE command_id = ? AND response_json IS NULL", JSON.stringify(input.response), input.occurredAt, input.commandId);
    });
  }
  recordProgress(input: { readonly runId: string; readonly progressed: RunProgressedEnvelope; readonly occurredAt: string }): void {
    this.#transaction(() => {
      this.#update("UPDATE worker_runs SET state = 'running', updated_at = ? WHERE run_id = ? AND state IN ('accepted','running')", input.occurredAt, input.runId);
      this.#enqueue(input.runId, input.progressed, input.occurredAt);
    });
  }
  recordCommentary(input: { readonly runId: string; readonly digest: string; readonly progressed: RunProgressedEnvelope; readonly occurredAt: string; readonly occurredAtMs: number }): WorkerCommentaryJournalResult {
    return this.#transaction(() => {
      if (!this.#commentaryEnvelopeIsSafe(input.progressed, input.runId, input.digest)) throw new Error("invalid commentary envelope");
      const run = this.#db.prepare("SELECT state FROM worker_runs WHERE run_id = ?").get(input.runId) as { state: WorkerCommandRunRecord["state"] } | undefined;
      if (run === undefined || (run.state !== "accepted" && run.state !== "running")) throw new Error("invalid journal transition");
      const state = this.#commentaryState(input.runId);
      const duplicate = this.#db.prepare("SELECT 1 AS present FROM worker_commentary_digests WHERE run_id = ? AND commentary_digest = ?").get(input.runId, input.digest);
      if (duplicate !== undefined) return { kind: "ignored" };
      if (state === undefined) {
        this.#db.prepare("INSERT INTO worker_commentary_digests(run_id, commentary_digest, created_at) VALUES (?, ?, ?)").run(input.runId, input.digest, input.occurredAt);
        this.#db.prepare("UPDATE worker_runs SET state = 'running', updated_at = ? WHERE run_id = ? AND state IN ('accepted','running')").run(input.occurredAt, input.runId);
        this.#enqueue(input.runId, input.progressed, input.occurredAt);
        this.#db.prepare("INSERT INTO worker_commentary_state(run_id, emitted_count, window_started_at_ms, pending_digest, pending_event_json, pending_due_at_ms, updated_at) VALUES (?, 1, ?, NULL, NULL, NULL, ?)").run(input.runId, input.occurredAtMs, input.occurredAt);
        return { kind: "enqueued" };
      }
      const dueAtMs = state.window_started_at_ms + COMMENTARY_WINDOW_MS;
      if (state.pending_event_json === null && input.occurredAtMs >= dueAtMs) {
        this.#db.prepare("INSERT INTO worker_commentary_digests(run_id, commentary_digest, created_at) VALUES (?, ?, ?)").run(input.runId, input.digest, input.occurredAt);
        this.#db.prepare("UPDATE worker_runs SET state = 'running', updated_at = ? WHERE run_id = ? AND state IN ('accepted','running')").run(input.occurredAt, input.runId);
        this.#enqueue(input.runId, input.progressed, input.occurredAt);
        this.#db.prepare("UPDATE worker_commentary_state SET window_started_at_ms = ?, updated_at = ? WHERE run_id = ?").run(input.occurredAtMs, input.occurredAt, input.runId);
        return { kind: "enqueued" };
      }
      if (state.pending_digest !== null) this.#db.prepare("DELETE FROM worker_commentary_digests WHERE run_id = ? AND commentary_digest = ?").run(input.runId, state.pending_digest);
      this.#db.prepare("INSERT INTO worker_commentary_digests(run_id, commentary_digest, created_at) VALUES (?, ?, ?)").run(input.runId, input.digest, input.occurredAt);
      this.#db.prepare("UPDATE worker_commentary_state SET pending_digest = ?, pending_event_json = ?, pending_due_at_ms = ?, updated_at = ? WHERE run_id = ?").run(input.digest, JSON.stringify(input.progressed), dueAtMs, input.occurredAt, input.runId);
      return { kind: "pending", dueAtMs };
    });
  }
  flushPendingCommentary(input: { readonly runId: string; readonly occurredAt: string; readonly occurredAtMs: number }): WorkerCommentaryJournalResult {
    return this.#transaction(() => {
      const run = this.#db.prepare("SELECT state FROM worker_runs WHERE run_id = ?").get(input.runId) as { state: WorkerCommandRunRecord["state"] } | undefined;
      const state = this.#commentaryState(input.runId);
      if (run === undefined || state === undefined || state.pending_event_json === null || state.pending_due_at_ms === null || (run.state !== "accepted" && run.state !== "running")) return { kind: "ignored" };
      if (input.occurredAtMs < state.pending_due_at_ms) return { kind: "pending", dueAtMs: state.pending_due_at_ms };
      const event = JSON.parse(state.pending_event_json) as RunProgressedEnvelope;
      if (state.pending_digest === null || !this.#commentaryEnvelopeIsSafe(event, input.runId, state.pending_digest)) throw new Error("invalid commentary envelope");
      this.#db.prepare("UPDATE worker_runs SET state = 'running', updated_at = ? WHERE run_id = ? AND state IN ('accepted','running')").run(input.occurredAt, input.runId);
      this.#enqueue(input.runId, event, input.occurredAt);
      this.#db.prepare("UPDATE worker_commentary_state SET window_started_at_ms = ?, pending_digest = NULL, pending_event_json = NULL, pending_due_at_ms = NULL, updated_at = ? WHERE run_id = ?").run(input.occurredAtMs, input.occurredAt, input.runId);
      return { kind: "enqueued" };
    });
  }
  listPendingCommentary(): readonly WorkerPendingCommentary[] {
    return (this.#db.prepare("SELECT run_id, pending_digest, pending_due_at_ms, pending_event_json FROM worker_commentary_state WHERE pending_event_json IS NOT NULL ORDER BY pending_due_at_ms, run_id").all() as { run_id: string; pending_digest: string; pending_due_at_ms: number; pending_event_json: string }[]).map((row) => { const event = JSON.parse(row.pending_event_json) as RunProgressedEnvelope; if (!this.#commentaryEnvelopeIsSafe(event, row.run_id, row.pending_digest)) throw new Error("invalid commentary envelope"); return { runId: row.run_id, dueAtMs: row.pending_due_at_ms, messageId: event.messageId, safeMessage: event.progress.safeMessage as string }; });
  }
  discardPendingCommentary(input: { readonly runId: string; readonly occurredAt: string }): void {
    this.#db.prepare("DELETE FROM worker_commentary_state WHERE run_id = ?").run(input.runId);
  }
  recordBinding(input: { readonly runId: string; readonly binding: unknown; readonly occurredAt: string }): void { this.#update("UPDATE worker_runs SET state = 'binding', binding_json = ?, updated_at = ? WHERE run_id = ? AND state = 'reserved'", JSON.stringify(input.binding), input.occurredAt, input.runId); }
  recordTerminal(input: { readonly runId: string; readonly terminal: RunSucceededEnvelope | RunAgentResponseDelivery | WorkerRunWaitingForInputEnvelope | RunFailedEnvelope; readonly occurredAt: string }): void {
    this.#transaction(() => {
      this.#db.prepare("DELETE FROM worker_commentary_state WHERE run_id = ?").run(input.runId);
      if (runtimeEnvelope(input.terminal).envelopeType === "run.waiting_for_input") {
        this.#update("UPDATE worker_runs SET state = 'failed', terminal_json = ?, updated_at = ? WHERE run_id = ? AND state IN ('accepted','running')", JSON.stringify(input.terminal), input.occurredAt, input.runId);
        this.#db.prepare("INSERT INTO worker_waiting_runs(run_id, terminal_json, created_at) VALUES (?, ?, ?)").run(input.runId, JSON.stringify(input.terminal), input.occurredAt);
      } else {
        this.#update("UPDATE worker_runs SET state = ?, terminal_json = ?, updated_at = ? WHERE run_id = ? AND state IN ('accepted','running')", runtimeEnvelope(input.terminal).envelopeType === "run.succeeded" ? "succeeded" : "failed", JSON.stringify(input.terminal), input.occurredAt, input.runId);
      }
      this.#enqueue(input.runId, input.terminal, input.occurredAt);
    });
  }
  /**
   * A separate table from `worker_runs`, written in the same transaction as the
   * state change. `DO NOTHING` on conflict: the cause worth keeping is the one that
   * isolated the Run.
   */
  recordObservational(input: { readonly runId: string; readonly cause: string; readonly occurredAt: string }): void {
    this.#transaction(() => {
      this.#update("UPDATE worker_runs SET state = 'observational', updated_at = ? WHERE run_id = ? AND state IN ('reserved','binding','accepted','running')", input.occurredAt, input.runId);
      this.#db.prepare("INSERT INTO worker_run_observations(run_id, cause, occurred_at) VALUES (?, ?, ?) ON CONFLICT(run_id) DO NOTHING").run(input.runId, input.cause, input.occurredAt);
    });
  }
  readObservationalCause(runId: string): string | undefined {
    const row = this.#db.prepare("SELECT cause FROM worker_run_observations WHERE run_id = ?").get(runId) as { cause: string } | undefined;
    return row?.cause;
  }
  /**
   * A separate table, written outside the terminal's transaction: the refusal is
   * true the moment the projection is refused, whether or not the terminal is then
   * journaled. `DO NOTHING` on conflict, because recovery can re-project the same
   * succeeded event.
   */
  recordResponseProjectionRefusal(input: WorkerResponseProjectionRefusal): void {
    this.#db.prepare("INSERT INTO worker_response_projection_refusals(run_id, source_event_id, diagnostics_json, occurred_at) VALUES (?, ?, ?, ?) ON CONFLICT(run_id) DO NOTHING")
      .run(input.runId, input.sourceEventId, JSON.stringify(input.diagnostics), input.occurredAt);
  }
  /** Absent rows and unreadable diagnostic payloads read as no refusal. */
  readResponseProjectionRefusal(runId: string): WorkerResponseProjectionRefusal | undefined {
    const row = this.#db.prepare("SELECT source_event_id, diagnostics_json, occurred_at FROM worker_response_projection_refusals WHERE run_id = ?").get(runId) as { source_event_id: string; diagnostics_json: string; occurred_at: string } | undefined;
    if (row === undefined) return undefined;
    const diagnostics = responseProjectionDiagnostics(row.diagnostics_json);
    return diagnostics === null ? undefined : { runId, sourceEventId: row.source_event_id, diagnostics, occurredAt: row.occurred_at };
  }
  recordWorkspaceResolutionAudit(input: Omit<WorkerWorkspaceResolutionAudit, "contractVersion" | "sequence">): WorkerWorkspaceResolutionAudit {
    if (!validWorkspaceAuditInput(input)) throw new Error("workspace resolution audit is invalid");
    return this.#transaction(() => {
      const current = this.#workspaceResolutionAudit(input.commandId);
      if (current !== undefined) {
        const { contractVersion: _contractVersion, sequence: _sequence, ...recorded } = current;
        if (canonical(recorded) !== canonical(input)) throw new Error("workspace resolution audit conflict");
        return current;
      }
      this.#db.prepare("INSERT INTO worker_workspace_resolution_audits(contract_version, command_id, task_id, run_id, system, organization_id, node_id, workspace_policy_ref_json, outcome, reason, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .run(WORKER_WORKSPACE_RESOLUTION_AUDIT_VERSION, input.commandId, input.taskId, input.runId, input.system, input.organizationId, input.nodeId, input.workspacePolicyRef === null ? null : canonical(input.workspacePolicyRef), input.outcome, input.reason, input.occurredAt);
      const inserted = this.#workspaceResolutionAudit(input.commandId);
      if (inserted === undefined) throw new Error("workspace resolution audit lost");
      return inserted;
    });
  }
  listWorkspaceResolutionAudits(input: { readonly afterSequence?: number; readonly limit?: number } = {}): readonly WorkerWorkspaceResolutionAudit[] {
    const afterSequence = input.afterSequence ?? 0, limit = input.limit ?? 100;
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new Error("workspace resolution audit query is invalid");
    return (this.#db.prepare("SELECT * FROM worker_workspace_resolution_audits WHERE sequence > ? ORDER BY sequence LIMIT ?").all(afterSequence, limit) as Record<string, unknown>[]).map((row) => this.#workspaceResolutionAuditFromRow(row));
  }
  /**
   * Undelivered and not quarantined: excluding quarantined rows is what stops their
   * retry, and lets the extra delivery pass a quarantine requests terminate. The
   * drain readers (`../node/maintenance.js` `pendingWorkerOutbox`, `DRAIN_CHECKS` in
   * `../lifecycle/macos-adapter.js`) test `delivered_at IS NULL`, which a quarantined
   * row still satisfies: quarantine is counted operational debt, not a drained row.
   */
  listUndeliveredEvents(): readonly WorkerCommandOutboxEvent[] {
    return (this.#db.prepare("SELECT o.message_id, o.run_id, o.event_json FROM worker_outbox o LEFT JOIN worker_outbox_quarantine q ON q.message_id = o.message_id WHERE o.delivered_at IS NULL AND q.message_id IS NULL ORDER BY o.created_at, o.rowid").all() as { message_id: string; run_id: string; event_json: string }[]).map((row) => ({ runId: row.run_id, messageId: row.message_id, event: outboxPayload(row.event_json) }));
  }
  markEventDelivered(input: { readonly messageId: string; readonly occurredAt: string }): void { this.#update("UPDATE worker_outbox SET delivered_at = ? WHERE message_id = ? AND delivered_at IS NULL", input.occurredAt, input.messageId); }
  /**
   * Records the disposition beside the row and, in the same transaction, enqueues the
   * substitute terminal only if this give-up leaves the Run silent: nothing else
   * undelivered and not quarantined for it. Returns whether it announced. So two
   * unreadable rows produce one Activity, and an unreadable row followed by a
   * readable terminal produces none of its own.
   *
   * The disposition is a sidecar table because `#enqueue` inserts positionally into
   * `worker_outbox`.
   */
  quarantineEvent(input: { readonly messageId: string; readonly runId: string; readonly code: WorkerOutboxQuarantineCode; readonly quarantinedAt: string; readonly substitute: RunFailedEnvelope | null }): boolean {
    return this.#transaction(() => {
      const existing = this.#db.prepare("SELECT code FROM worker_outbox_quarantine WHERE message_id = ?").get(input.messageId) as { code?: unknown } | undefined;
      // Already given up on. Re-announcing is as wrong as never announcing.
      if (existing !== undefined) { if (outboxQuarantineCode(existing.code) !== input.code) throw new Error("outbox quarantine conflict"); return false; }
      const row = this.#db.prepare("SELECT run_id FROM worker_outbox WHERE message_id = ? AND delivered_at IS NULL").get(input.messageId) as { run_id?: unknown } | undefined;
      if (row === undefined || row.run_id !== input.runId) throw new Error("invalid outbox quarantine target");
      this.#db.prepare("INSERT INTO worker_outbox_quarantine(message_id, run_id, code, quarantined_at) VALUES (?, ?, ?, ?)").run(input.messageId, input.runId, outboxQuarantineCode(input.code), input.quarantinedAt);
      if (input.substitute === null) return false;
      const remaining = this.#db.prepare("SELECT COUNT(*) AS count FROM worker_outbox o LEFT JOIN worker_outbox_quarantine q ON q.message_id = o.message_id WHERE o.run_id = ? AND o.delivered_at IS NULL AND q.message_id IS NULL").get(input.runId) as { count: number };
      if (Number(remaining.count) !== 0) return false;
      this.#enqueue(input.runId, input.substitute, input.quarantinedAt);
      return true;
    });
  }
  /**
   * Keyed by identity: the retained `run.accepted`'s `messageId` is the primary key of
   * the outbox row `recordAccepted` enqueued. `accepted_json` is read without
   * validation, so a Run whose accepted this build cannot validate still answers
   * true, and the repair then declines when `#recoveryStart` returns null. False on
   * any read failure.
   */
  runAcceptedQuarantined(runId: string): boolean {
    try {
      const accepted = this.readRun(runId)?.accepted;
      const messageId = record(accepted)?.messageId;
      if (typeof messageId !== "string") return false;
      return this.#db.prepare("SELECT 1 AS present FROM worker_outbox_quarantine q JOIN worker_outbox o ON o.message_id = q.message_id WHERE q.run_id = ? AND q.message_id = ? AND o.delivered_at IS NULL").get(runId, messageId) !== undefined;
    } catch { return false; }
  }
  /**
   * Bounded, payload-free, and read from the sidecar alone, so a disposition whose
   * row was deleted is still counted.
   */
  listQuarantineDispositions(input: { readonly afterSequence?: number; readonly limit?: number } = {}): readonly WorkerOutboxQuarantineDisposition[] {
    const afterSequence = input.afterSequence ?? 0, limit = input.limit ?? 100;
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new Error("outbox quarantine query is invalid");
    return (this.#db.prepare("SELECT sequence, message_id, run_id, code, quarantined_at FROM worker_outbox_quarantine WHERE sequence > ? ORDER BY sequence LIMIT ?").all(afterSequence, limit) as { sequence: number; message_id: string; run_id: string; code: string; quarantined_at: string }[])
      .map((row) => ({ sequence: row.sequence, runId: row.run_id, messageId: row.message_id, code: outboxQuarantineCode(row.code), quarantinedAt: row.quarantined_at }));
  }
  /**
   * Joins back to the retained payload, so a reader can prove `event_json` was never
   * rewritten. A disposition whose row was deleted is absent; an unparseable row
   * reports the placeholder in `event`.
   */
  listQuarantinedEvents(): readonly WorkerQuarantinedOutboxEvent[] {
    return (this.#db.prepare("SELECT q.message_id, q.run_id, q.code, q.quarantined_at, o.event_json FROM worker_outbox_quarantine q JOIN worker_outbox o ON o.message_id = q.message_id ORDER BY q.quarantined_at, q.rowid").all() as { message_id: string; run_id: string; code: string; quarantined_at: string; event_json: string }[])
      .map((row) => ({ runId: row.run_id, messageId: row.message_id, code: outboxQuarantineCode(row.code), quarantinedAt: row.quarantined_at, event: outboxPayload(row.event_json) }));
  }
  #enqueue(runId: string, event: RouterRunLifecycleEnvelope, occurredAt: string): void {
    const row = this.#db.prepare("SELECT COALESCE(MAX(ordinal), 0) AS ordinal FROM worker_outbox WHERE run_id = ?").get(runId) as { ordinal: number } | undefined;
    this.#db.prepare("INSERT INTO worker_outbox VALUES (?, ?, ?, ?, ?, NULL)").run(lifecycleMessageId(event), runId, Number(row?.ordinal ?? 0) + 1, JSON.stringify(event), occurredAt);
  }
  #commentaryState(runId: string): { readonly window_started_at_ms: number; readonly pending_digest: string | null; readonly pending_event_json: string | null; readonly pending_due_at_ms: number | null } | undefined {
    return this.#db.prepare("SELECT window_started_at_ms, pending_digest, pending_event_json, pending_due_at_ms FROM worker_commentary_state WHERE run_id = ?").get(runId) as { window_started_at_ms: number; pending_digest: string | null; pending_event_json: string | null; pending_due_at_ms: number | null } | undefined;
  }
  #workspaceResolutionAudit(commandId: string): WorkerWorkspaceResolutionAudit | undefined {
    const row = this.#db.prepare("SELECT * FROM worker_workspace_resolution_audits WHERE command_id = ?").get(commandId) as Record<string, unknown> | undefined;
    return row === undefined ? undefined : this.#workspaceResolutionAuditFromRow(row);
  }
  #workspaceResolutionAuditFromRow(row: Record<string, unknown>): WorkerWorkspaceResolutionAudit {
    const system = row.system;
    if (row.contract_version !== WORKER_WORKSPACE_RESOLUTION_AUDIT_VERSION || system !== "linear" || !Number.isSafeInteger(row.sequence) || (row.sequence as number) < 1 || !isWorkerOpaqueIdentifier(row.command_id) || !isWorkerOpaqueIdentifier(row.task_id) || !isWorkerOpaqueIdentifier(row.run_id) || !isWorkerOpaqueIdentifier(row.organization_id) || !isWorkerOpaqueIdentifier(row.node_id) || !["mapped", "default_root", "fallback_root", "unavailable"].includes(String(row.outcome)) || !["catalog_match", "no_ref", "catalog_miss", "mapped_path_unavailable", "root_unavailable"].includes(String(row.reason)) || text(row.occurred_at) === null || new Date(String(row.occurred_at)).toISOString() !== row.occurred_at || !validWorkspaceResolutionPair(row.outcome as WorkerWorkspaceResolutionOutcome, row.reason as WorkerWorkspaceResolutionReason)) throw new Error("workspace resolution audit is invalid");
    let workspacePolicyRef: WorkerVersionedReference | null = null;
    if (row.workspace_policy_ref_json !== null) {
      const parsed = record(JSON.parse(String(row.workspace_policy_ref_json)));
      if (parsed === null || !exactKeys(parsed, ["ref", "version"]) || !isWorkerOpaqueIdentifier(parsed.ref) || !isWorkerOpaqueIdentifier(parsed.version) || canonical(parsed) !== row.workspace_policy_ref_json) throw new Error("workspace resolution audit is invalid");
      workspacePolicyRef = { ref: parsed.ref as string, version: parsed.version as string };
    }
    const audit = { commandId: row.command_id as string, taskId: row.task_id as string, runId: row.run_id as string, system: system as WorkerWorkspaceSystem, organizationId: row.organization_id as string, nodeId: row.node_id as string, workspacePolicyRef, outcome: row.outcome as WorkerWorkspaceResolutionOutcome, reason: row.reason as WorkerWorkspaceResolutionReason, occurredAt: row.occurred_at as string };
    if (!validWorkspaceAuditInput(audit)) throw new Error("workspace resolution audit is invalid");
    return Object.freeze({ contractVersion: WORKER_WORKSPACE_RESOLUTION_AUDIT_VERSION, sequence: row.sequence as number, ...audit, workspacePolicyRef: workspacePolicyRef === null ? null : Object.freeze(workspacePolicyRef) });
  }
  #commentaryEnvelopeIsSafe(event: unknown, runId: string, commentaryDigest: string): event is RunProgressedEnvelope {
    const candidate = record(event); const run = record(candidate?.run); const progress = record(candidate?.progress);
    return candidate?.envelopeType === "run.progressed" && text(candidate.messageId) !== null && run?.runId === runId && typeof progress?.safeMessage === "string" && normalizeExecutorCommentary(progress.safeMessage) === progress.safeMessage && digest(progress.safeMessage) === commentaryDigest;
  }
  #update(sql: string, ...values: string[]): void { if ((this.#db.prepare(sql).run(...values) as { changes: number }).changes !== 1) throw new Error("invalid journal transition"); }
  #transaction<T>(operation: () => T): T { this.#db.exec("BEGIN IMMEDIATE"); try { const value = operation(); this.#db.exec("COMMIT"); return value; } catch (error) { this.#db.exec("ROLLBACK"); throw error; } }
}

interface Configuration { readonly bearerToken: string; readonly workerId: string; readonly runtimeKind: string; readonly executorId: string; readonly nodeId: string; readonly runtimeProfile: WorkerVersionedReference; readonly methodRef: WorkerRunConfiguration["methodRef"]; readonly workspaces: ReadonlyMap<string, WorkerWorkspaceConfiguration>; }
function configuration(input: WorkerRunConfiguration): Configuration {
  let workspaces: ReadonlyMap<string, WorkerWorkspaceConfiguration>;
  try { workspaces = normalizeWorkerWorkspaceConfigurations(input.workspaces); } catch { throw new Error("Worker configuration is invalid."); }
  const profile = input.runtimeProfile === undefined ? { ref: input.executorId, version: "v1" } : record(input.runtimeProfile);
  if (workspaces.size === 0 || text(input.bearerToken) === null || text(input.workerId) === null || text(input.runtimeKind) === null || text(input.executorId) === null || text(input.nodeId) === null || profile === null || !exactKeys(profile, ["ref", "version"]) || text(profile.ref) === null || text(profile.version) === null || input.methodRef === null || typeof input.methodRef !== "object") throw new Error("Worker configuration is invalid.");
  return { bearerToken: input.bearerToken, workerId: input.workerId, runtimeKind: input.runtimeKind, executorId: input.executorId, nodeId: input.nodeId, runtimeProfile: { ref: profile.ref as string, version: profile.version as string }, methodRef: input.methodRef, workspaces };
}

function commentaryJournal(journal: WorkerCommandJournal): WorkerCommentaryJournalExtension | null {
  const candidate = journal as WorkerCommandJournal & Partial<WorkerCommentaryJournalExtension>;
  return typeof candidate.recordCommentary === "function" && typeof candidate.flushPendingCommentary === "function" && typeof candidate.listPendingCommentary === "function" && typeof candidate.discardPendingCommentary === "function" ? candidate as WorkerCommentaryJournalExtension : null;
}

type WorkerStartCommand = Extract<WorkerCommand, { readonly command: "start" | "continue" }>;
type WorkerCancelCommand = Extract<WorkerCommand, { readonly command: "cancel" }>;
function commandWorkspacePolicyRef(command: WorkerStartCommand): WorkerVersionedReference | null {
  return command.workspacePolicyRef;
}

export class WorkerCommandService {
  readonly #configuration: Configuration; readonly #registration: ExecutorRegistration; readonly #sink: RouterRunEventSink; readonly #journal: WorkerCommandJournal; readonly #commentaryJournal: WorkerCommentaryJournalExtension | null; readonly #now: () => Date; readonly #messageId: () => string; readonly #schedule: (callback: () => void, delayMs: number) => unknown; readonly #cancelSchedule: (handle: unknown) => void; readonly #beforeTerminalValidation: () => Promise<void>;
  readonly #inFlight = new Map<string, Promise<WorkerIngressResponse>>();
  readonly #activeRuns = new Map<string, ExecutorLiveBinding>();
  /** Process-local proof that this service observed an owned provider binding; never reconstructed after restart. */
  readonly #observedBindings = new Map<string, ExecutorLiveBinding>();
  readonly #commentaryTimers = new Map<string, { readonly dueAtMs: number; readonly handle: unknown }>();
  /**
   * Runs whose missing first event this process has already replaced at Router. Only
   * bounds wasted emissions; it is not consulted after a refusal, since a 409 may be
   * transient. A restart drops it and the repair is re-derived.
   */
  readonly #replacedRunAccepted = new Set<string>();
  /**
   * True only until the startup recovery pass has run once; gates all commands until
   * durable active state is enumerated. A semantic recovery failure is quarantined
   * as observational and excluded; only a failed quarantine write or readback stays
   * global. A quarantined Run answers `axis.worker.recovery_required` for later
   * `continue`/`cancel`.
   */
  #recoveryPending: boolean;
  readonly #recoveredRuns = new Set<string>();
  readonly #recoveryPersistenceFailures = new Set<string>();
  #recoveryInFlight: Promise<void> | undefined;
  #deliveryInProgress = false;
  #deliveryRequested = false;
  #deliveryRetryTimer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;
  constructor(options: WorkerCommandOptions) {
    this.#configuration = configuration(options.configuration); this.#registration = selectExecutorRegistration(this.#configuration, options.registrations); this.#sink = options.eventSink; this.#journal = options.journal ?? new SqliteWorkerCommandJournal(":memory:"); this.#commentaryJournal = commentaryJournal(this.#journal); this.#now = options.now ?? (() => new Date()); this.#messageId = options.messageId ?? randomUUID; this.#beforeTerminalValidation = options.beforeTerminalValidation ?? (() => Promise.resolve()); const recovery = this.#journal as unknown as Partial<WorkerRecoveryJournalExtension>; this.#recoveryPending = typeof recovery.listRecoverableRuns === "function" && recovery.listRecoverableRuns().length > 0;
    this.#schedule = options.scheduleCommentary ?? ((callback, delayMs) => { const handle = setTimeout(callback, delayMs); handle.unref?.(); return handle; });
    this.#cancelSchedule = options.cancelCommentarySchedule ?? ((handle) => clearTimeout(handle as ReturnType<typeof setTimeout>));
    for (const pending of this.#commentaryJournal?.listPendingCommentary() ?? []) this.#schedulePendingCommentary(pending.runId, pending.dueAtMs);
  }
  authenticate(candidate: string): boolean { return timingSafeEqual(createHash("sha256").update(this.#configuration.bearerToken).digest(), createHash("sha256").update(candidate).digest()); }
  /**
   * Reconnect only records whose persisted binding and accepted envelope can be
   * proved by the configured provider. This never calls start or continue.
   */
  async recoverActiveRuns(): Promise<void> {
    if (this.#recoveryInFlight !== undefined) return this.#recoveryInFlight;
    const operation = this.#recoverActiveRuns(); this.#recoveryInFlight = operation;
    try { await operation; } finally { this.#recoveryInFlight = undefined; }
  }
  /**
   * The response this Worker recorded for `commandId`, exactly as dispatched, or
   * `undefined`. One primary-key read; it never begins, reserves or executes.
   */
  readRecordedResponse(commandId: string): WorkerIngressResponse | undefined {
    if (!isWorkerOpaqueIdentifier(commandId)) fail("axis.worker.malformed", 400);
    let record: WorkerCommandRecord | undefined;
    try { record = this.#journal.readCommand(commandId); } catch { fail("axis.worker.recovery_required", 503); }
    return record?.response;
  }
  /**
   * A `WorkerCommand` is never answered `steered` (only a `steer` is), so the narrower
   * overload is exact.
   */
  async execute(candidate: WorkerCommand, expectedContractVersion?: WorkerCommandContractVersion): Promise<{ readonly response: WorkerCommandResponse; readonly replayed: boolean }>;
  async execute(candidate: unknown, expectedContractVersion?: WorkerCommandContractVersion): Promise<{ readonly response: WorkerIngressResponse; readonly replayed: boolean }>;
  async execute(candidate: unknown, expectedContractVersion?: WorkerCommandContractVersion): Promise<{ readonly response: WorkerIngressResponse; readonly replayed: boolean }> {
    const command = await parseCommand(candidate, expectedContractVersion); const intentJson = commandIntent(command); const intentDigest = digest(intentJson);
    // A durable response, a same-process execution, and an intent conflict are
    // all safe to resolve while startup recovery is still enumerating old Runs.
    // `beginCommand` below remains the authority after the gate for races.
    let existing: WorkerCommandRecord | undefined;
    try { existing = this.#journal.readCommand(command.commandId); } catch { fail("axis.worker.recovery_required", 503); }
    if (existing !== undefined) return this.#existingCommand(existing, command.commandId, intentJson, intentDigest);
    if (this.#recoveryPending) fail("axis.worker.recovery_required", 503);
    let claim: WorkerCommandJournalClaim; try { claim = this.#journal.beginCommand({ commandId: command.commandId, intentJson, intentDigest, occurredAt: this.#instant() }); } catch { fail("axis.worker.recovery_required", 503); }
    if (claim.kind === "intent_conflict") fail("axis.worker.intent_conflict", 409);
    if (claim.kind === "existing") return this.#existingCommand(claim.record, command.commandId, intentJson, intentDigest);
    const result = command.command === "cancel" ? this.#cancel(command) : command.command === "steer" ? this.#steer(command) : this.#start(command);
    this.#inFlight.set(command.commandId, result);
    try { const response = await result; this.#journal.recordCommandResponse({ commandId: command.commandId, response, occurredAt: this.#instant() }); return { response, replayed: false }; } catch (error) { throw error; } finally { this.#inFlight.delete(command.commandId); }
  }
  async #start(command: WorkerStartCommand): Promise<WorkerCommandResponse> {
    const validated = await this.#validateStart(command.start, command.executionTarget);
    const workspacePolicyRef = commandWorkspacePolicyRef(command);
    const resolution = await resolveWorkerWorkspace(validated.workspace, workspacePolicyRef);
    try {
      this.#journal.recordWorkspaceResolutionAudit({ commandId: command.commandId, taskId: validated.start.identity.task.taskId, runId: validated.start.identity.run.runId, system: workspaceSourceKey(validated.start.identity.task.sourceRef).system, organizationId: workspaceSourceKey(validated.start.identity.task.sourceRef).id, nodeId: this.#configuration.nodeId, workspacePolicyRef, outcome: resolution.outcome, reason: resolution.reason, occurredAt: this.#instant() });
    } catch { fail("axis.worker.recovery_required", 503); }
    if (resolution.workingDirectory === null) fail("axis.worker.workspace_unavailable", 503);
    const workspaceRoot = resolution.workingDirectory;
    this.#require("observe"); this.#require(command.command);
    let continuationBinding: ExecutorLiveBinding | undefined;
    // The refusals below that resolve the prior binding answer
    // `continuation_unresumable`: the two state checks, the drift check, the
    // no-proof-path `else`, `reattestTerminal`'s `proof === null` exit (the provider
    // was reached and declined), and `#readRun`'s miss. `reattestTerminal`'s other exit
    // (proof missing, malformed or mismatched) is `capability_unavailable`. None of
    // these reserves a Run or invokes the executor, which is what lets the code be
    // `definite`. `recovery_required` stays ambiguous.
    //
    // After a restart `#observedBindings` is empty, so every prior Run (`succeeded`,
    // `failed`, or parked on `waiting_for_input`) is continued only through
    // `reattestTerminal`. Only Codex parks a Run on `waiting_for_input`.
    if (command.command === "continue") {
      const prior = this.#readRun(command.continuation.priorRunId, "axis.worker.continuation_unresumable"); const observed = this.#observedBindings.get(prior.runId);
      if ((prior.state !== "waiting_for_input" && prior.state !== "succeeded" && prior.state !== "failed") || prior.binding === undefined) fail("axis.worker.continuation_unresumable", 503);
      if (observed !== undefined) {
        if (!this.#sameBinding(observed, prior.binding)) fail("axis.worker.continuation_unresumable", 503);
        continuationBinding = observed;
      } else if ((prior.state === "succeeded" || prior.state === "failed" || prior.state === "waiting_for_input") && this.#validBinding(prior.binding) && this.#registration.reattestTerminal !== undefined) {
        const expectedOutcome = prior.state;
        let proof;
        try { proof = await this.#registration.reattestTerminal({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: command.commandId, binding: prior.binding, expectedOutcome }); } catch { fail("axis.worker.capability_unavailable", 503); }
        if (proof === null) fail("axis.worker.continuation_unresumable", 503);
        if (proof === undefined || proof.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || proof.requestId !== command.commandId || proof.status !== "attested" || !this.#sameBinding(proof.binding, prior.binding)) fail("axis.worker.capability_unavailable", 503);
        continuationBinding = prior.binding;
      } else fail("axis.worker.continuation_unresumable", 503);
    }
    const runId = command.start.identity.run.runId;
    const runIntent = canonical({ identity: command.start.identity, operation: command.command, instructionDigest: digest(command.instruction), workspacePolicyRef, ...(command.command === "continue" ? { continuation: command.continuation } : {}) }); let claim: WorkerCommandRunClaim;
    try { claim = this.#journal.reserveRun({ runId, intentJson: runIntent, intentDigest: digest(runIntent), occurredAt: this.#instant() }); } catch { fail("axis.worker.recovery_required", 503); }
    if (claim.kind === "intent_conflict") fail("axis.worker.intent_conflict", 409);
    if (claim.kind === "existing") fail("axis.worker.recovery_required", 503);
    let result: ExecutorStartResult | ExecutorContinueResult;
    try {
      // A successor Run needs its own execution context, exactly as a first Run does.
      const linearExecution = Object.freeze({
        taskId: validated.start.identity.task.taskId, attemptId: validated.start.identity.attempt.attemptId,
        runId, organizationId: validated.start.identity.task.sourceRef.organizationId,
      });
      result = command.command === "start"
        ? await this.#registration.adapter.start!({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: command.commandId, workingDirectory: workspaceRoot, instruction: command.instruction, linearExecution })
        : await this.#registration.adapter.continue!({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: command.commandId, workingDirectory: workspaceRoot, instruction: command.instruction, binding: continuationBinding!, linearExecution });
    } catch {
      this.#observational(runId, WORKER_OBSERVATIONAL_CAUSES.launchThrew); fail("axis.worker.launch_ambiguous", 503);
    }
    const failure = this.#exactLaunchFailure(result, command.command, command.commandId, continuationBinding);
    if (failure !== null) {
      if (failure.observational) this.#observational(runId, WORKER_OBSERVATIONAL_CAUSES.launchFailureAmbiguous);
      fail(failure.code, failure.statusCode);
    }
    if (!this.#matchesOperationResult(result, command.command, command.commandId) || result.status !== "accepted" || !this.#validBinding(result.binding)) { this.#observational(runId, WORKER_OBSERVATIONAL_CAUSES.launchResultUnusable); fail("axis.worker.launch_ambiguous", 503); }
    const nativeBinding = result.binding;
    try { this.#journal.recordBinding({ runId, binding: nativeBinding, occurredAt: this.#instant() }); } catch { this.#observational(runId, WORKER_OBSERVATIONAL_CAUSES.bindingNotJournaled); fail("axis.worker.launch_ambiguous", 503); }
    let iterator: AsyncIterator<ExecutorLifecycleEvent>; let sequence: number;
    try { iterator = this.#registration.adapter.observe!(nativeBinding)[Symbol.asyncIterator](); const first = await iterator.next(); if (first.done || !this.#validLifecycleEvent(first.value, nativeBinding, 0) || first.value.outcome !== "started") throw new Error("invalid lifecycle"); sequence = first.value.sequence; } catch (error) { this.#observational(runId, observationalCause(error, WORKER_OBSERVATIONAL_CAUSES.observeStartInvalid)); fail("axis.worker.launch_ambiguous", 503); }
    const accepted = await this.#accepted(command.start);
    const response: WorkerCommandResponse = { contractVersion: command.contractVersion, commandId: command.commandId, status: "accepted", identity: this.#identity(), runAccepted: accepted };
    try { this.#journal.recordAccepted({ runId, accepted, commandId: command.commandId, response, occurredAt: this.#instant() }); } catch { this.#observational(runId, WORKER_OBSERVATIONAL_CAUSES.acceptedNotJournaled); fail("axis.worker.launch_ambiguous", 503); }
    this.#observedBindings.set(runId, nativeBinding);
    this.#activeRuns.set(runId, nativeBinding);
    void this.#supervise(runId, command.start, nativeBinding, iterator, sequence);
    this.#requestOutboxDelivery();
    return response;
  }
  async #cancel(command: WorkerCancelCommand | WorkerCommandSteer): Promise<WorkerCommandResponse> {
    this.#require("cancel");
    const ended = this.#endedRun(command.target.runId);
    if (ended !== undefined) return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: ended.runId };
    const { run, active } = this.#interruptibleRun(command.target.runId);
    const result = await this.#registration.adapter.cancel!({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: command.commandId, binding: active });
    if (!this.#matchesOperationResult(result, "cancel", command.commandId) || result.status !== "interrupt_requested" || !this.#validBinding(result.binding) || !this.#sameBinding(result.binding, active)) fail("axis.worker.launch_ambiguous", 503);
    return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: run.runId };
  }
  /**
   * Hands the instruction to the running turn when the executor declares `steer`.
   * Anything that cannot take it answers `interrupt_requested`, so the caller falls
   * back to interrupt-then-continue: an executor without `steer` takes `#cancel`'s
   * path, and a turn already finishing (`executor.steer_too_late`) is answered
   * without interrupting anything.
   */
  async #steer(command: WorkerCommandSteer): Promise<WorkerIngressResponse> {
    const steer = this.#registration.adapter.steer;
    if (!this.#registration.supportedOperations.includes("steer") || typeof steer !== "function") return this.#cancel(command);
    // A steer that lands after its Run ended is answered like `executor.steer_too_late`,
    // without asking the adapter. See `#endedRun`.
    const ended = this.#endedRun(command.target.runId);
    if (ended !== undefined) return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: ended.runId };
    const { run, active } = this.#interruptibleRun(command.target.runId);
    const result: unknown = await steer.call(this.#registration.adapter, { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: command.commandId, binding: active, instruction: command.instruction });
    if (!this.#matchesOperationResult(result, "steer", command.commandId)) fail("axis.worker.launch_ambiguous", 503);
    const candidate = record(result);
    if (candidate?.status === "steered" && this.#validBinding(candidate.binding) && this.#sameBinding(candidate.binding, active)) return { contractVersion: command.contractVersion, commandId: command.commandId, status: "steered", runId: run.runId };
    if (candidate?.status === "failed" && record(candidate.error)?.code === "executor.steer_too_late") return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: run.runId };
    fail("axis.worker.launch_ambiguous", 503);
  }
  /**
   * The Run a `cancel` or `steer` targets when it has already ended (`succeeded`,
   * `failed`, or parked on `waiting_for_input`) with a valid binding. Nothing is
   * running, so the executor is not asked and the command is answered
   * `interrupt_requested`. Safe for a waiting Run: the run chain queues the
   * interrupting continue behind `channelRedispatchEligible`, which a waiting
   * conversation does not pass. `undefined` leaves every other state to
   * `#interruptibleRun`.
   */
  #endedRun(runId: string): WorkerCommandRunRecord | undefined {
    const run = this.#readRun(runId); const observed = this.#observedBindings.get(run.runId);
    if (run.state !== "succeeded" && run.state !== "failed" && run.state !== "waiting_for_input") return undefined;
    return run.binding !== undefined && this.#validBinding(run.binding) && (observed === undefined || this.#sameBinding(observed, run.binding)) ? run : undefined;
  }
  /** The Run a `cancel` or `steer` acts on: `accepted`/`running`, supervised by this process, with the binding it recorded. */
  #interruptibleRun(runId: string): { readonly run: WorkerCommandRunRecord; readonly active: ExecutorLiveBinding } {
    const run = this.#readRun(runId); const active = this.#activeRuns.get(run.runId); if ((run.state !== "accepted" && run.state !== "running") || run.binding === undefined || active === undefined) fail("axis.worker.recovery_required", 503);
    if (!this.#sameBinding(active, run.binding)) fail("axis.worker.recovery_required", 503);
    return { run, active };
  }
  async #supervise(runId: string, start: RunStartEnvelope, nativeBinding: ExecutorLiveBinding, iterator: AsyncIterator<ExecutorLifecycleEvent>, sequence: number, recovered = false): Promise<void> {
    try { for (;;) { const next = await iterator.next(); if (next.done) throw new ExecutorLifecycleError(WORKER_OBSERVATIONAL_CAUSES.lifecycleEnded, "lifecycle ended"); const event = next.value; if (!this.#validLifecycleEvent(event, nativeBinding, sequence)) throw new ExecutorLifecycleError(WORKER_OBSERVATIONAL_CAUSES.invalidLifecycleEvent, "invalid lifecycle"); sequence = event.sequence; if (event.outcome === "progressed") { const projection = this.#project(event, runId); if (projection.kind !== "progress") throw new ExecutorLifecycleError(WORKER_OBSERVATIONAL_CAUSES.invalidProgressProjection, "invalid lifecycle projection"); let progressed: RunProgressedEnvelope; try { progressed = await this.#progressed(start, projection); } catch (error) { if (event.progress.kind === "commentary") continue; throw new ExecutorLifecycleError(WORKER_OBSERVATIONAL_CAUSES.progressEnvelopeFailed, "progress envelope failed", { cause: error }); } if (event.progress.kind === "generic") this.#journal.recordProgress({ runId, progressed, occurredAt: this.#instant() }); else { if (this.#commentaryJournal === null) continue; const now = this.#now(); const result = this.#commentaryJournal.recordCommentary({ runId, digest: digest(event.progress.safeMessage), progressed, occurredAt: now.toISOString(), occurredAtMs: now.getTime() }); if (result.kind === "pending") this.#schedulePendingCommentary(runId, result.dueAtMs); if (result.kind === "ignored") continue; } this.#requestOutboxDelivery(); continue; }
      if (event.outcome === "waiting_for_input") { this.#sealCommentary(runId); const projection = this.#project(event, runId); if (projection.kind !== "waiting_for_input") throw new ExecutorLifecycleError(WORKER_OBSERVATIONAL_CAUSES.invalidWaitingProjection, "invalid lifecycle projection"); await this.#terminal(runId, await this.#waiting(start, projection.elicitation), recovered); return; }
      if (event.outcome === "succeeded") { this.#sealCommentary(runId); const projection = this.#project(event, runId); if (projection.kind !== "succeeded") throw new ExecutorLifecycleError(WORKER_OBSERVATIONAL_CAUSES.invalidSucceededProjection, "invalid lifecycle projection"); await this.#terminal(runId, await this.#successTerminal(start, projection.resultRef, projection.response), recovered); return; }
      if (event.outcome === "failed" || event.outcome === "cancelled") { this.#sealCommentary(runId); const projection = this.#project(event, runId); if (projection.kind !== "failed") throw new ExecutorLifecycleError(WORKER_OBSERVATIONAL_CAUSES.invalidFailedProjection, "invalid lifecycle projection"); await this.#terminal(runId, await this.#failed(start, { code: projection.code, safeMessage: projection.safeMessage }), recovered); return; }
      throw new ExecutorLifecycleError(WORKER_OBSERVATIONAL_CAUSES.unexpectedLifecycleOutcome, "unexpected lifecycle"); } }
    // The isolation carries the label the error carries.
    catch (error) { const cause = observationalCause(error); if (recovered) this.#quarantineRecoveredObserverFailure(runId, cause); else this.#observational(runId, cause); } finally { this.#activeRuns.delete(runId); } }
  async #terminal(runId: string, terminal: RunSucceededEnvelope | RunAgentResponseDelivery | WorkerRunWaitingForInputEnvelope | RunFailedEnvelope, recovered = false): Promise<void> { try { this.#journal.recordTerminal({ runId, terminal, occurredAt: this.#instant() }); this.#requestOutboxDelivery(); } catch (error) { if (recovered) throw error; this.#observational(runId, WORKER_OBSERVATIONAL_CAUSES.terminalNotJournaled); } }
  async #validateStart(startCandidate: unknown, executionTarget: WorkerExecutionTarget): Promise<{ readonly start: RunStartEnvelope; readonly workspace: WorkerWorkspaceConfiguration }> { const validation = await validateRuntimeBoundaryMessage(startCandidate); if (!validation.valid || validation.value?.envelopeType !== "run.start") fail("axis.worker.malformed", 400); const start = validation.value as RunStartEnvelope; const identity = start.identity; if (start.correlationId !== identity.task.correlationId || start.correlationId !== identity.attempt.correlationId || start.correlationId !== identity.run.correlationId || start.correlationId !== identity.sessionBinding.correlationId) fail("axis.worker.policy_forbidden", 403); if (!this.#matchesExecutionTarget(executionTarget)) fail("axis.worker.identity_mismatch", 409); const source = workspaceSourceKey(identity.task.sourceRef); const workspace = this.#configuration.workspaces.get(workerWorkspaceKey(source.system, source.id)); if (workspace === undefined) fail("axis.worker.policy_forbidden", 403); return { start, workspace }; }
  /**
   * Every emitted lifecycle envelope echoes the `run.start`'s contract version: Router
   * requires `accepted.contractVersion === start.contractVersion`, and each schema
   * pins its own `contractVersion`.
   */
  async #accepted(start: RunStartEnvelope): Promise<RunAcceptedEnvelope> { const run = { ...start.identity.run, state: "running" as const }; return this.#validated({ contractVersion: start.contractVersion, messageId: this.#messageId(), correlationId: start.correlationId, operationId: start.operationId, causationMessageId: start.messageId, occurredAt: this.#now().toISOString(), producer: "worker", consumer: "router", envelopeType: "run.accepted", task: start.identity.task, attempt: start.identity.attempt, run }); }
  async #progressed(start: RunStartEnvelope, projection: { readonly completed: number; readonly total: number; readonly safeMessage: string }): Promise<RunProgressedEnvelope> { const run = { ...start.identity.run, state: "running" as const }; return this.#validated({ contractVersion: start.contractVersion, messageId: this.#messageId(), correlationId: start.correlationId, operationId: start.operationId, causationMessageId: start.messageId, occurredAt: this.#now().toISOString(), producer: "worker", consumer: "router", envelopeType: "run.progressed", task: start.identity.task, attempt: start.identity.attempt, run, progress: { completed: projection.completed, total: projection.total, safeMessage: projection.safeMessage } }); }
  /**
   * The terminal a successful Run emits, which is never "none". A success whose
   * terminal cannot be projected is reported as `run.failed` with the safe code,
   * since `#supervise`'s catch would otherwise record `observational` and emit
   * nothing. Only a `WorkerCommandError` (a refused projection) is converted;
   * anything else propagates. Used on both the `#supervise` and the recovery path.
   */
  async #successTerminal(start: RunStartEnvelope, resultRef: string, response: ExecutorTerminalResponse | undefined): Promise<RunSucceededEnvelope | RunAgentResponseDelivery | RunFailedEnvelope> {
    try {
      return await this.#succeeded(start, resultRef, response);
    } catch (error) {
      if (!(error instanceof WorkerCommandError)) throw error;
      return await this.#failed(start, { code: error.code, safeMessage: safeMessages[error.code] });
    }
  }
  async #succeeded(start: RunStartEnvelope, resultRef: string, response: ExecutorTerminalResponse | undefined): Promise<RunSucceededEnvelope | RunAgentResponseDelivery> {
    const run = { ...start.identity.run, state: "succeeded" as const, resultRef };
    await this.#beforeTerminalValidation();
    const envelope = await this.#validated({ contractVersion: start.contractVersion, messageId: this.#messageId(), correlationId: start.correlationId, operationId: start.operationId, causationMessageId: start.messageId, occurredAt: this.#now().toISOString(), producer: "worker", consumer: "router", envelopeType: "run.succeeded", task: start.identity.task, attempt: start.identity.attempt, run, resultRef });
    const { sessionId: agentSessionId } = runtimeSourceSessionIdentity(start.identity.task.sourceRef);
    if (response === undefined || agentSessionId === undefined) return envelope;
    const activityId = deterministicActivityId(`${start.identity.task.taskId}\u0000${start.identity.run.runId}\u0000${response.sourceEventId}`);
    const projected: AgentActivityResponseReceipt = {
      contractVersion: agentActivityContractVersion,
      activityId,
      taskId: start.identity.task.taskId,
      attemptId: start.identity.attempt.attemptId,
      runId: start.identity.run.runId,
      // The receipt's provider-discriminated `conversation`; the guard above returned for every non-Linear source.
      conversation: { system: "linear", agentSessionId },
      sourceEventId: response.sourceEventId,
      operationId: start.operationId,
      content: { type: "response", body: response.body },
      projectionStatus: response.projectionStatus,
      resultRef,
      truncated: response.truncated,
      truncations: [...response.truncations],
    };
    // Keep the refusal reasons. The fallback body still says nothing about why; the
    // diagnostics go to the Worker journal only.
    const refusal = await this.#responseReceiptRefusal(projected, response.sourceEventId);
    const selected = refusal.length === 0
      ? projected
      : { ...projected, content: { type: "response" as const, body: agentActivityFallbackResponseBody }, projectionStatus: "fallback" as const, truncated: false, truncations: [] };
    if (refusal.length > 0) this.#recordResponseProjectionRefusal(start.identity.run.runId, response.sourceEventId, refusal);
    const delivery = { contractVersion: agentActivityContractVersion, deliveryType: "run_agent_response" as const, envelope, agentActivity: selected };
    const validation = await validateRunAgentResponseDelivery(delivery);
    // The surface exists and the delivery is still invalid: a malformed projection.
    // `#successTerminal` turns this into `run.failed`.
    if (!validation.valid || validation.value === null) fail("axis.worker.adapter_invalid", 502);
    return validation.value;
  }
  async #waiting(start: RunStartEnvelope, elicitationValue: unknown): Promise<WorkerRunWaitingForInputEnvelope> {
    const elicitation = validateStructuredElicitation(elicitationValue);
    if (elicitation === null) fail("axis.worker.adapter_invalid", 502);
    await this.#beforeTerminalValidation();
    return {
      contractVersion: WORKER_WAITING_EXTENSION_VERSION,
      messageId: this.#messageId(),
      correlationId: start.correlationId,
      operationId: start.operationId,
      causationMessageId: start.messageId,
      occurredAt: this.#instant(),
      producer: "worker",
      consumer: "router",
      envelopeType: "run.waiting_for_input",
      task: start.identity.task,
      attempt: start.identity.attempt,
      run: { ...start.identity.run, state: "waiting_for_input" },
      elicitation,
    };
  }
  async #failed(start: RunStartEnvelope, error: RuntimeError): Promise<RunFailedEnvelope> { const run = { ...start.identity.run, state: "failed" as const, error }; await this.#beforeTerminalValidation(); return this.#validated({ contractVersion: start.contractVersion, messageId: this.#messageId(), correlationId: start.correlationId, operationId: start.operationId, causationMessageId: start.messageId, occurredAt: this.#now().toISOString(), producer: "worker", consumer: "router", envelopeType: "run.failed", task: start.identity.task, attempt: start.identity.attempt, run, error }); }
  async #validated<T extends RunAcceptedEnvelope | RunProgressedEnvelope | RunSucceededEnvelope | RunFailedEnvelope>(event: T): Promise<T> { const validation = await validateRuntimeBoundaryMessage(event); if (!validation.valid || validation.value?.envelopeType !== event.envelopeType) fail("axis.worker.adapter_invalid", 502); return validation.value as T; }
  /**
   * Every reason this receipt may not be delivered, empty when it may. A receipt the
   * contract refuses is not also checked for its `sourceEventId`.
   */
  async #responseReceiptRefusal(receipt: AgentActivityResponseReceipt, sourceEventId: string): Promise<readonly WorkerResponseProjectionDiagnostic[]> {
    const validation = await validateAgentActivityResponseReceipt(receipt);
    const diagnostics = validation.valid ? validateAgentActivitySourceEvent(receipt, sourceEventId).diagnostics : validation.diagnostics;
    // Copied field by field, so a contract diagnostic carrying more than these five
    // fields can never write answer text into the journal.
    return diagnostics.map(({ code, instancePath, schemaId, keyword, safeMessage }) => ({ code, instancePath, schemaId, keyword, safeMessage }));
  }
  /** Swallowed: an observation about a terminal must never stop the terminal. */
  #recordResponseProjectionRefusal(runId: string, sourceEventId: string, diagnostics: readonly WorkerResponseProjectionDiagnostic[]): void {
    try { this.#journal.recordResponseProjectionRefusal({ runId, sourceEventId, diagnostics, occurredAt: this.#instant() }); } catch {}
  }
  async #emit<T extends RouterRunLifecycleEnvelope>(event: T): Promise<void> { await this.#sink.emit(event); }
  /**
   * `missing` defaults to `axis.worker.recovery_required`, which `#cancel` uses. The
   * continuation binding resolution passes `continuation_unresumable`: it is raised
   * before `reserveRun`.
   */
  #readRun(runId: string, missing: WorkerCommandErrorCode = "axis.worker.recovery_required"): WorkerCommandRunRecord { try { const run = this.#journal.readRun(runId); if (run === undefined) fail(missing, 503); return run; } catch (error) { if (error instanceof WorkerCommandError) throw error; fail(missing, 503); } }
  async #existingCommand(record: WorkerCommandRecord, commandId: string, intentJson: string, intentDigest: string): Promise<{ readonly response: WorkerIngressResponse; readonly replayed: boolean }> {
    if (record.intentJson !== intentJson || record.intentDigest !== intentDigest) fail("axis.worker.intent_conflict", 409);
    if (record.response !== undefined) { this.#requestOutboxDelivery(); return { response: record.response, replayed: true }; }
    const active = this.#inFlight.get(commandId);
    if (active !== undefined) return { response: await active, replayed: true };
    fail("axis.worker.recovery_required", 503);
  }
  #require(operation: ExecutorCapability): void { if (!this.#registration.supportedOperations.includes(operation)) fail("axis.worker.operation_unsupported", 503); }
  #matchesOperationResult(value: unknown, operation: "start" | "continue" | "cancel" | "steer", requestId: string): boolean {
    const candidate = record(value);
    return candidate?.contractVersion === CODEX_EXECUTOR_CONTRACT_VERSION && candidate.requestId === requestId && candidate.operation === operation;
  }
  /**
   * A definite launch refusal, for either operation: a successor whose credential
   * Relay refuses never reaches a native process, so it is neither ambiguous nor
   * observational.
   */
  #exactLaunchFailure(value: unknown, operation: "start" | "continue", expectedRequestId: string, continuationBinding?: ExecutorLiveBinding): { readonly code: WorkerCommandErrorCode; readonly statusCode: 502 | 503; readonly observational: boolean } | null {
    const candidate = record(value);
    if (candidate === null || !exactKeys(candidate, ["contractVersion", "requestId", "operation", "status", "binding", "error"]) || candidate.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || candidate.requestId !== expectedRequestId || candidate.operation !== operation) return null;
    // `invalid_request` is as exact a refusal as `failed`: the adapter rejected the request without acting.
    if (candidate.status !== "failed" && candidate.status !== "invalid_request") return null;
    // A failed continue may hand back the very binding it was given; that is
    // still an exact refusal. A different binding is a
    // successor that may exist natively, which stays ambiguous.
    if (candidate.binding !== null && !(continuationBinding !== undefined && this.#sameBinding(candidate.binding, continuationBinding))) return null;
    const nativeError = record(candidate.error);
    if (nativeError === null || !exactKeys(nativeError, ["contractVersion", "code", "operation", "category", "retryable", "safeMessage"]) || nativeError.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || nativeError.operation !== operation) return null;
    const mappings: readonly [ExecutorErrorCode, WorkerCommandErrorCode, 502 | 503, boolean][] = [
      ["executor.start_rejected", "axis.worker.launch_failed", 502, true],
      ["executor.runtime_missing", "axis.worker.runtime_missing", 503, false],
      ["executor.runtime_incompatible", "axis.worker.runtime_incompatible", 503, false],
      ["executor.authentication_required", "axis.worker.authentication_required", 503, false],
      ["executor.probe_timeout", "axis.worker.probe_timeout", 503, false],
      ["executor.capability_unavailable", "axis.worker.capability_unavailable", 503, false],
      ["executor.launch_failed", "axis.worker.launch_failed", 503, false],
      ["executor.launch_ambiguous", "axis.worker.launch_ambiguous", 503, true],
      // `start_failed` and `continue_failed` stay ambiguous: Codex wraps its whole
      // `startTurn`/`continueTurn` call in one catch, so a native turn may already
      // exist.
      ["executor.start_failed", "axis.worker.launch_ambiguous", 503, true],
      ["executor.continue_failed", "axis.worker.launch_ambiguous", 503, true],
      // Raised by the adapter before it acts, so the Run is not observational. Still
      // ambiguous to the user: `axis.worker.launch_failed` is ambiguous in
      // `workerFailureTuples`.
      ["executor.invalid_request", "axis.worker.launch_failed", 502, false],
      ["executor.idempotency_conflict", "axis.worker.launch_failed", 502, false],
    ];
    for (const [executorCode, workerCode, statusCode, observational] of mappings) {
      const expected = executorError(operation, executorCode);
      if (nativeError.code === expected.code && nativeError.category === expected.category && nativeError.retryable === expected.retryable && nativeError.safeMessage === expected.safeMessage) return { code: workerCode, statusCode, observational };
    }
    return null;
  }
  #validLifecycleEvent(value: unknown, nativeBinding: ExecutorLiveBinding, previousSequence: number): value is ExecutorLifecycleEvent {
    const candidate = record(value);
    if (candidate?.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || !this.#sameBinding(candidate.binding, nativeBinding) || !Number.isSafeInteger(candidate.sequence) || (candidate.sequence as number) <= previousSequence) return false;
    if (candidate.outcome === "progressed") {
      if (!exactKeys(candidate, ["contractVersion", "binding", "sequence", "outcome", "progress"])) return false;
      const progress = record(candidate.progress);
      return progress !== null && ((exactKeys(progress, ["kind"]) && progress.kind === "generic") || (exactKeys(progress, ["kind", "safeMessage"]) && progress.kind === "commentary" && normalizeExecutorCommentary(progress.safeMessage) === progress.safeMessage));
    }
    if (candidate.outcome === "succeeded") {
      if (exactKeys(candidate, ["contractVersion", "binding", "sequence", "outcome"])) return true;
      if (!exactKeys(candidate, ["contractVersion", "binding", "sequence", "outcome", "response"])) return false;
      const response = record(candidate.response);
      const truncations = response?.truncations;
      return response !== null
        && exactKeys(response, ["sourceEventId", "projectionStatus", "body", "truncated", "truncations"])
        && text(response.sourceEventId) !== null
        && (response.projectionStatus === "projected" || response.projectionStatus === "fallback")
        && text(response.body) !== null
        && typeof response.truncated === "boolean"
        && Array.isArray(truncations);
    }
    if (candidate.outcome === "waiting_for_input") return exactKeys(candidate, ["contractVersion", "binding", "sequence", "outcome", "elicitation"]) && validateStructuredElicitation(candidate.elicitation) !== null;
    if (["started", "cancelled"].includes(candidate.outcome as string)) return exactKeys(candidate, ["contractVersion", "binding", "sequence", "outcome"]);
    if (candidate.outcome !== "failed") return false;
    if (exactKeys(candidate, ["contractVersion", "binding", "sequence", "outcome"])) return true;
    if (!exactKeys(candidate, ["contractVersion", "binding", "sequence", "outcome", "failureKind"])) return false;
    // Each kind is accepted from exactly the runtime that can emit it.
    if (nativeBinding.runtimeKind === "codex") return candidate.failureKind === "model_capacity";
    if (nativeBinding.runtimeKind === "claude-code") return candidate.failureKind === "permission_denied";
    return false;
  }
  #validBinding(value: unknown): value is ExecutorLiveBinding { const candidate = record(value); if (candidate?.runtimeKind !== this.#registration.runtimeKind) return false; try { return this.#registration.validateBinding(value); } catch { return false; } }
  #sameBinding(left: unknown, right: unknown): boolean { if (!this.#validBinding(left) || !this.#validBinding(right)) return false; try { return this.#registration.bindingsEqual(left, right); } catch { return false; } }
  /**
   * Whether this command was reserved for this Worker's identity: nodeId, workerId,
   * executorKind, runtimeProfile and the session's executor id, against the
   * configuration read once at construction. A mismatch is a definite refusal
   * before `reserveRun`.
   */
  #matchesExecutionTarget(executionTarget: WorkerExecutionTarget): boolean {
    return canonical(executionTarget) === canonical({ identity: this.#identity(), sessionBindingExecutorId: this.#configuration.executorId });
  }
  #identity(): WorkerExecutionIdentity { return { nodeId: this.#configuration.nodeId, workerId: this.#configuration.workerId, executorKind: this.#configuration.runtimeKind, runtimeProfile: { ...this.#configuration.runtimeProfile } }; }
  #project(event: Exclude<ExecutorLifecycleEvent, { readonly outcome: "started" }>, runId: string) { return this.#registration.projectLifecycle({ event, runId }); }
  // Seals commentary like the terminal branches, inside the guard: during shutdown and
  // restart recovery the journal may be closed. `cause` is `string` because two call
  // sites pass an executor-authored label.
  #observational(runId: string, cause: string): void { try { this.#sealCommentary(runId); this.#journal.recordObservational({ runId, cause, occurredAt: this.#instant() }); } catch {} }
  #instant(): string { return this.#now().toISOString(); }

  /** Replay immutable lifecycle envelopes after a Router outage or Worker restart; never relaunch Codex. */
  async replayUndeliveredEvents(): Promise<void> { await this.#flushOutbox(); }
  close(): void { this.#closed = true; if (this.#deliveryRetryTimer !== undefined) clearTimeout(this.#deliveryRetryTimer); for (const runId of [...this.#commentaryTimers.keys()]) this.#cancelCommentaryTimer(runId); }
  async #recoverActiveRuns(): Promise<void> {
    const recovery = this.#journal as unknown as Partial<WorkerRecoveryJournalExtension>;
    if (typeof recovery.listRecoverableRuns !== "function") { this.#recoveryPending = false; return; }
    let complete = true;
    let retained: readonly WorkerCommandRunRecord[];
    try { retained = recovery.listRecoverableRuns(); } catch { this.#recoveryPending = true; return; }
    for (const runId of [...this.#recoveryPersistenceFailures]) {
      // If the prior write committed but its readback failed, prove that exact
      // observational state before attempting a second transition. Otherwise
      // retry the strict record-then-readback quarantine once.
      if (this.#recoveredQuarantineIsDurable(runId) || this.#quarantineRecoveredRun(runId, WORKER_OBSERVATIONAL_CAUSES.recoveryQuarantineRetry)) this.#recoveryPersistenceFailures.delete(runId);
      else complete = false;
    }
    for (const run of retained) {
      // A retained binding gets exactly one recovery proof; a reattached active run is
      // skipped on later calls.
      const quarantine = (cause: WorkerObservationalCause): void => {
        if (this.#quarantineRecoveredRun(run.runId, cause)) this.#recoveryPersistenceFailures.delete(run.runId);
        else { this.#recoveryPersistenceFailures.add(run.runId); complete = false; }
      };
      if (this.#recoveredRuns.has(run.runId)) {
        if (this.#recoveryPersistenceFailures.has(run.runId)) complete = false;
        continue;
      }
      this.#recoveredRuns.add(run.runId);
      if (this.#registration.recover === undefined) { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryUnsupported); continue; }
      let start: RunStartEnvelope | null;
      try { start = await this.#recoveryStart(run); } catch { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryStartUnreadable); continue; }
      if (start === null || run.binding === undefined || !this.#validBinding(run.binding)) { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryStartUnreadable); continue; }
      const requestId = `recovery-${run.runId}`;
      const recovered = await this.#recover({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId, runId: run.runId, binding: run.binding });
      try {
        const candidate = record(recovered);
        if (candidate === null || candidate.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || candidate.requestId !== requestId || candidate.runId !== run.runId || !this.#validBinding(candidate.binding) || !this.#sameBinding(candidate.binding, run.binding)) { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryResultUnusable); continue; }
        if (candidate.status === "active") {
          if (!exactKeys(candidate, ["contractVersion", "requestId", "runId", "status", "binding", "observe"])) { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryResultUnusable); continue; }
          const observer = this.#recoveryObserver(candidate.observe);
          if (observer === null) { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryObserverUnusable); continue; }
          const iterator = observer[Symbol.asyncIterator]();
          if (typeof iterator.next !== "function") { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryObserverUnusable); continue; }
          this.#observedBindings.set(run.runId, run.binding); this.#activeRuns.set(run.runId, run.binding);
          void this.#supervise(run.runId, start, run.binding, iterator, 0, true);
          continue;
        }
        if (candidate.status !== "terminal" || !exactKeys(candidate, ["contractVersion", "requestId", "runId", "status", "binding", "terminal"])) { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryResultUnusable); continue; }
        const terminal = this.#recoveryTerminalSnapshot(candidate.terminal);
        if (!this.#validRecoveryTerminal(terminal, run.binding) || !(await this.#recordRecoveredTerminal(run.runId, start, run.binding, terminal))) quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryTerminalUnusable);
      } catch { quarantine(WORKER_OBSERVATIONAL_CAUSES.recoveryThrew); }
    }
    // Only a recovery persistence failure stays global; every semantic or provider failure is isolated.
    this.#recoveryPending = !complete || this.#recoveryPersistenceFailures.size > 0;
    if (complete) this.#requestOutboxDelivery();
  }
  async #recover(input: ExecutorRecoveryRequest): Promise<ExecutorRecoveryResult | null> {
    const recover = this.#registration.recover;
    if (recover === undefined) return null;
    let handle: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<null>((resolve) => { handle = setTimeout(() => resolve(null), RECOVERY_TIMEOUT_MS); });
    try { return await Promise.race([recover(input), timeout]); } catch { return null; } finally { if (handle !== undefined) clearTimeout(handle); }
  }
  #quarantineRecoveredRun(runId: string, cause: string): boolean {
    try { this.#journal.recordObservational({ runId, cause, occurredAt: this.#instant() }); return this.#recoveredQuarantineIsDurable(runId); } catch { return false; }
  }
  #quarantineRecoveredObserverFailure(runId: string, cause: string): void {
    if (this.#quarantineRecoveredRun(runId, cause)) this.#recoveryPersistenceFailures.delete(runId);
    else { this.#recoveryPersistenceFailures.add(runId); this.#recoveryPending = true; }
  }
  #recoveredQuarantineIsDurable(runId: string): boolean { try { return this.#journal.readRun(runId)?.state === "observational"; } catch { return false; } }
  #recoveryObserver(value: unknown): AsyncIterable<ExecutorLifecycleEvent> | null {
    if (value === null || typeof value !== "object") return null;
    const iterator = (value as { readonly [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator];
    return typeof iterator === "function" ? value as AsyncIterable<ExecutorLifecycleEvent> : null;
  }
  #recoveryTerminalSnapshot(value: unknown): unknown {
    try { return structuredClone(value); } catch { return null; }
  }
  #validRecoveryTerminal(value: unknown, nativeBinding: ExecutorLiveBinding): value is Exclude<ExecutorLifecycleEvent, { readonly outcome: "started" | "progressed" }> {
    if (!this.#validLifecycleEvent(value, nativeBinding, 0)) return false;
    return value.outcome !== "started" && value.outcome !== "progressed";
  }
  async #recordRecoveredTerminal(runId: string, start: RunStartEnvelope, binding: ExecutorLiveBinding, event: Exclude<ExecutorLifecycleEvent, { readonly outcome: "started" | "progressed" }>): Promise<boolean> {
    try {
      const projection = this.#project(event, runId);
      // `#successTerminal`, so a refused projection still produces a terminal instead of
      // an observational Run with nothing emitted.
      const terminal = event.outcome === "succeeded" && projection.kind === "succeeded"
        ? await this.#successTerminal(start, projection.resultRef, projection.response)
        : event.outcome === "waiting_for_input" && projection.kind === "waiting_for_input"
          ? await this.#waiting(start, projection.elicitation)
          : (event.outcome === "failed" || event.outcome === "cancelled") && projection.kind === "failed"
            ? await this.#failed(start, { code: projection.code, safeMessage: projection.safeMessage })
            : null;
      if (terminal === null) throw new Error("invalid recovery terminal");
      this.#journal.recordTerminal({ runId, terminal, occurredAt: this.#instant() });
      if (event.outcome === "waiting_for_input") this.#observedBindings.set(runId, binding);
      this.#requestOutboxDelivery();
      return true;
    } catch { return this.#quarantineRecoveredRun(runId, WORKER_OBSERVATIONAL_CAUSES.recoveredTerminalNotJournaled); }
  }
  async #recoveryStart(run: WorkerCommandRunRecord): Promise<RunStartEnvelope | null> {
    if (run.accepted === undefined || run.accepted.run.runId !== run.runId) return null;
    const validation = await validateRuntimeBoundaryMessage(run.accepted);
    if (!validation.valid || validation.value?.envelopeType !== "run.accepted") return null;
    const accepted = validation.value as RunAcceptedEnvelope;
    const identity = {
      contractVersion: accepted.contractVersion,
      task: accepted.task,
      attempt: accepted.attempt,
      run: { ...accepted.run, state: "pending" as const },
      sessionBinding: { contractVersion: accepted.run.contractVersion, sessionBindingId: accepted.run.sessionBindingId, taskId: accepted.task.taskId, attemptId: accepted.attempt.attemptId, runId: accepted.run.runId, correlationId: accepted.correlationId, workerId: this.#configuration.workerId, executorId: this.#configuration.executorId },
    };
    const reconstructed = { contractVersion: accepted.contractVersion, messageId: accepted.causationMessageId, correlationId: accepted.correlationId, operationId: accepted.operationId, causationMessageId: accepted.causationMessageId, occurredAt: accepted.occurredAt, producer: "router", consumer: "worker", envelopeType: "run.start" as const, identity };
    const reconstructedValidation = await validateRuntimeBoundaryMessage(reconstructed);
    return reconstructedValidation.valid && reconstructedValidation.value?.envelopeType === "run.start" ? reconstructedValidation.value as RunStartEnvelope : null;
  }
  #schedulePendingCommentary(runId: string, dueAtMs: number): void {
    if (this.#closed || this.#commentaryJournal === null) return;
    const journal = this.#commentaryJournal;
    const current = this.#commentaryTimers.get(runId);
    if (current?.dueAtMs === dueAtMs) return;
    if (current !== undefined) this.#cancelSchedule(current.handle);
    const handle = this.#schedule(() => {
      this.#commentaryTimers.delete(runId);
      if (this.#closed) return;
      try {
        const now = this.#now();
        const result = journal.flushPendingCommentary({ runId, occurredAt: now.toISOString(), occurredAtMs: now.getTime() });
        if (result.kind === "pending") this.#schedulePendingCommentary(runId, result.dueAtMs);
        if (result.kind === "enqueued") this.#requestOutboxDelivery();
      } catch {}
    }, Math.max(0, dueAtMs - this.#now().getTime()));
    this.#commentaryTimers.set(runId, { dueAtMs, handle });
  }
  #sealCommentary(runId: string): void { this.#cancelCommentaryTimer(runId); this.#commentaryJournal?.discardPendingCommentary({ runId, occurredAt: this.#instant() }); }
  #cancelCommentaryTimer(runId: string): void { const timer = this.#commentaryTimers.get(runId); if (timer === undefined) return; this.#commentaryTimers.delete(runId); this.#cancelSchedule(timer.handle); }
  #requestOutboxDelivery(): void { if (this.#closed) return; this.#deliveryRequested = true; if (!this.#deliveryInProgress) void this.#flushOutbox(); }
  /**
   * Reads one persisted row through the closed validator that owns its surface; a
   * decided rejection raises `UnprocessableOutboxEvent`. No fallback reader: an
   * unreadable row stays unreadable.
   */
  async #decodeUndeliveredEvent(pending: WorkerCommandOutboxEvent): Promise<RouterRunLifecycleEnvelope> {
    if ("deliveryType" in pending.event) {
      // `deliveryType` selects the validator, matching `validateRouterWorkerLifecycleEvent`.
      const validated = await validateRunAgentResponseDelivery(pending.event);
      if (!validated.valid || validated.value === null) throw new UnprocessableOutboxEvent();
      return validated.value as RouterRunLifecycleEnvelope;
    }
    if (pending.event.envelopeType === "run.waiting_for_input") {
      if (pending.event.contractVersion !== WORKER_WAITING_EXTENSION_VERSION || validateStructuredElicitation(pending.event.elicitation) === null) throw new UnprocessableOutboxEvent();
      return pending.event;
    }
    const validated = await validateRuntimeBoundaryMessage(pending.event);
    if (!validated.valid || validated.value === null || !["run.accepted", "run.progressed", "run.succeeded", "run.failed"].includes(validated.value.envelopeType)) throw new UnprocessableOutboxEvent();
    return validated.value as RouterRunLifecycleEnvelope;
  }
  /**
   * Stops retrying a row this build can never parse. Returns false when the give-up
   * could not be made durable, which the caller retries. In one journal transaction:
   * the disposition is recorded beside the untouched row, and a substitute terminal
   * this build can validate is enqueued so the Run ends with exactly one final
   * Activity. Once a substitute is projected the Run is terminal at Router, so the
   * retained row is evidence, not a recovery path.
   */
  async #quarantineUndeliverableEvent(pending: WorkerCommandOutboxEvent): Promise<boolean> {
    const outcome = await this.#unprocessableTerminal(pending.runId);
    // A transient failure building the announcement keeps the row retrying.
    if (outcome.kind === "transient") return false;
    // `unsettled` quarantines but announces nothing: the Run is still running and its
    // own terminal is on the way.
    let announced: boolean;
    try {
      announced = this.#journal.quarantineEvent({ messageId: pending.messageId, runId: pending.runId, code: WORKER_OUTBOX_UNPROCESSABLE_CODE, quarantinedAt: this.#instant(), substitute: outcome.kind === "announce" ? outcome.envelope : null });
    } catch { return false; }
    // Enqueued inside the transaction above; ask the loop for one more pass.
    if (announced) this.#requestOutboxDelivery();
    return true;
  }
  /**
   * Stops retrying an announcement the Router refused; see `ownAnnouncement`. No
   * substitute here, so a give-up can never mint another. The Run ends silent:
   * nothing is lost if the refusal was permanent, the announcement is if it was
   * transient.
   */
  #quarantineRefusedEvent(pending: WorkerCommandOutboxEvent): boolean {
    try { this.#journal.quarantineEvent({ messageId: pending.messageId, runId: pending.runId, code: WORKER_OUTBOX_REFUSED_CODE, quarantinedAt: this.#instant(), substitute: null }); }
    catch { return false; }
    return true;
  }
  /**
   * The substitute the user sees when a give-up leaves the Run silent: `run.failed`
   * with a safe code, built from the Run's retained `run.accepted` through the
   * restart reconstruction. Side-effect free. A failure even if the row was a
   * success, since the response body was never delivered. `worker_runs` is left
   * as it stands. When the Run's own `run.accepted` is unreadable the result is
   * `unannounceable`.
   */
  async #unprocessableTerminal(runId: string): Promise<SubstituteOutcome> {
    let run: WorkerCommandRunRecord | undefined;
    // Reading the Run can fail transiently (`SQLITE_BUSY`, a locked journal).
    try { run = this.#journal.readRun(runId); } catch { return { kind: "transient" }; }
    if (run === undefined) return { kind: "unannounceable" };
    // The Run has not finished, so it has not gone silent: its own terminal will come.
    // A substitute here would be a false terminal, after which Router refuses the
    // Run's real one. Finished means a recorded terminal or `observational`.
    if (run.terminal === undefined && run.state !== "observational") return { kind: "unsettled" };
    let start: RunStartEnvelope | null;
    try { start = await this.#recoveryStart(run); } catch { return { kind: "transient" }; }
    if (start === null) return { kind: "unannounceable" };
    try {
      return { kind: "announce", envelope: await this.#failed(start, { code: WORKER_OUTBOX_UNPROCESSABLE_CODE, safeMessage: "This Worker build cannot read the persisted Router delivery for this Run. The record is retained; no response was delivered." }) };
    } catch (error) {
      // A `WorkerCommandError` is `#validated` refusing this Run's own retained identity,
      // a permanent property of stored bytes. Anything else is transient.
      return error instanceof WorkerCommandError ? { kind: "unannounceable" } : { kind: "transient" };
    }
  }
  /**
   * Restores the Run's first event when this Worker quarantined its own `run.accepted`.
   * Otherwise every readable row behind it is refused by Router's `#assertOrder` as
   * a first event, permanently.
   *
   * Router's state then selects the outcome: with no first event the replacement is
   * accepted and the Run's own terminal follows; with one already held the
   * replacement is refused and the queued row is accepted; with
   * `axis.router.not_found` everything keeps retrying.
   *
   * Emitted, never enqueued: it is re-derivable from durable state, idempotent at
   * Router (a second `run.accepted` is refused once `last_sequence > 0`), and
   * re-minted on the next pass. Built through `#recoveryStart` + `#accepted` from the
   * Run's own retained and re-validated `accepted_json`. Declines when that cannot
   * be rebuilt. Every failure is swallowed; the row's own delivery classifies it.
   */
  async #replaceQuarantinedRunAccepted(runId: string): Promise<void> {
    if (this.#replacedRunAccepted.has(runId)) return;
    // One `try`: every failure here has the same response (decline and let the row's
    // delivery classify it), and nothing may escape into `#flushOutbox`'s outer catch.
    try {
      if (!this.#journal.runAcceptedQuarantined(runId)) return;
      const run = this.#journal.readRun(runId);
      if (run === undefined) return;
      const start = await this.#recoveryStart(run);
      if (start === null) return;
      await this.#emit(await this.#accepted(start));
    } catch { return; }
    this.#replacedRunAccepted.add(runId);
  }
  async #flushOutbox(): Promise<void> {
    if (this.#deliveryInProgress || this.#closed) return;
    this.#deliveryInProgress = true;
    let retry = false;
    try {
      do {
        this.#deliveryRequested = false;
        const blockedRuns = new Set<string>();
        const repairedRuns = new Set<string>();
        for (const pending of this.#journal.listUndeliveredEvents()) {
          if (blockedRuns.has(pending.runId)) continue;
          // Two try blocks: reading the row decides "never parseable", delivering it decides
          // "not right now".
          let event: RouterRunLifecycleEnvelope;
          try {
            event = await this.#decodeUndeliveredEvent(pending);
          } catch (error) {
            if (error instanceof UnprocessableOutboxEvent && await this.#quarantineUndeliverableEvent(pending)) continue;
            blockedRuns.add(pending.runId);
            retry = true;
            continue;
          }
          // Restore a missing first event before Router is asked to accept anything behind it,
          // at most once per Run per pass.
          if (!repairedRuns.has(pending.runId)) { repairedRuns.add(pending.runId); await this.#replaceQuarantinedRunAccepted(pending.runId); }
          try {
            await this.#emit(event);
            this.#journal.markEventDelivered({ messageId: pending.messageId, occurredAt: this.#instant() });
          } catch (error) {
            if (error instanceof RouterEventRefusedError && ownAnnouncement(event) && this.#quarantineRefusedEvent(pending)) continue;
            blockedRuns.add(pending.runId);
            retry = true;
          }
        }
      } while (this.#deliveryRequested && !this.#closed);
    } catch {
      // Only failures outside any row reach here (the SELECT failing, `SQLITE_BUSY`,
      // storage briefly gone), so retry.
      retry = true;
    } finally {
      this.#deliveryInProgress = false;
      if (retry && !this.#closed && this.#deliveryRetryTimer === undefined) {
        this.#deliveryRetryTimer = setTimeout(() => { this.#deliveryRetryTimer = undefined; this.#requestOutboxDelivery(); }, 250);
        this.#deliveryRetryTimer.unref?.();
      }
    }
  }
}

async function parseCommand(candidate: unknown, expectedContractVersion?: WorkerCommandContractVersion): Promise<WorkerIngressCommand> {
  const root = record(candidate);
  if (root === null || root.contractVersion !== WORKER_COMMAND_VERSION || (expectedContractVersion !== undefined && root.contractVersion !== expectedContractVersion) || !isWorkerOpaqueIdentifier(root.commandId) || typeof root.command !== "string") fail("axis.worker.malformed", 400);
  const contractVersion = root.contractVersion;
  if (root.command === "cancel") {
    if (!exactKeys(root, ["contractVersion", "commandId", "command", "target"])) fail("axis.worker.malformed", 400);
    const target = record(root.target), runId = target?.runId;
    if (target === null || !exactKeys(target, ["runId"]) || !isWorkerOpaqueIdentifier(runId)) fail("axis.worker.malformed", 400);
    return { contractVersion, commandId: root.commandId as string, command: "cancel", target: { runId } };
  }
  if (root.command === "steer") {
    if (!exactKeys(root, ["contractVersion", "commandId", "command", "target", "instruction"]) || text(root.instruction) === null) fail("axis.worker.malformed", 400);
    const target = record(root.target), runId = target?.runId;
    if (target === null || !exactKeys(target, ["runId"]) || !isWorkerOpaqueIdentifier(runId)) fail("axis.worker.malformed", 400);
    return { contractVersion, commandId: root.commandId as string, command: "steer", target: { runId }, instruction: root.instruction as string };
  }
  if (root.command !== "start" && root.command !== "continue") fail("axis.worker.malformed", 400);
  const expected = root.command === "start"
    ? ["contractVersion", "commandId", "command", "start", "instruction", "workspacePolicyRef", "executionTarget"]
    : ["contractVersion", "commandId", "command", "start", "instruction", "continuation", "workspacePolicyRef", "executionTarget"];
  if (!exactKeys(root, expected) || text(root.instruction) === null) fail("axis.worker.malformed", 400);
  const validation = await validateRuntimeBoundaryMessage(root.start);
  if (!validation.valid || validation.value?.envelopeType !== "run.start") fail("axis.worker.malformed", 400);
  const start = validation.value as RunStartEnvelope;
  const executionTarget = parseExecutionTarget(root.executionTarget);
  // The Node builds both from one reservation, so a session binding naming a different
  // Worker or executor than the execution target is malformed.
  if (start.identity.sessionBinding.workerId !== executionTarget.identity.workerId
    || start.identity.sessionBinding.executorId !== executionTarget.sessionBindingExecutorId) fail("axis.worker.malformed", 400);
  let workspacePolicyRef: WorkerVersionedReference | null = null;
  if (root.workspacePolicyRef !== null) {
    const candidateRef = record(root.workspacePolicyRef);
    if (candidateRef === null || !exactKeys(candidateRef, ["ref", "version"]) || !isWorkerOpaqueIdentifier(candidateRef.ref) || !isWorkerOpaqueIdentifier(candidateRef.version)) fail("axis.worker.malformed", 400);
    workspacePolicyRef = { ref: candidateRef.ref, version: candidateRef.version };
  }
  if (root.command === "start") {
    return { contractVersion, commandId: root.commandId as string, command: "start", start, instruction: root.instruction as string, workspacePolicyRef, executionTarget };
  }
  const continuation = record(root.continuation), priorRunId = continuation?.priorRunId;
  if (continuation === null || !exactKeys(continuation, ["priorRunId"]) || !isWorkerOpaqueIdentifier(priorRunId)) fail("axis.worker.malformed", 400);
  return { contractVersion, commandId: root.commandId as string, command: "continue", start, instruction: root.instruction as string, continuation: { priorRunId }, workspacePolicyRef, executionTarget };
}

function parseExecutionTarget(value: unknown): WorkerExecutionTarget {
  const target = record(value), identity = record(target?.identity), runtimeProfile = record(identity?.runtimeProfile);
  if (target === null || identity === null || runtimeProfile === null
    || !exactKeys(target, ["identity", "sessionBindingExecutorId"])
    || !exactKeys(identity, ["nodeId", "workerId", "executorKind", "runtimeProfile"])
    || !exactKeys(runtimeProfile, ["ref", "version"])
    || ![identity.nodeId, identity.workerId, identity.executorKind, runtimeProfile.ref, runtimeProfile.version, target.sessionBindingExecutorId].every(isWorkerOpaqueIdentifier)) fail("axis.worker.malformed", 400);
  return {
    identity: { nodeId: identity.nodeId as string, workerId: identity.workerId as string, executorKind: identity.executorKind as string, runtimeProfile: { ref: runtimeProfile.ref as string, version: runtimeProfile.version as string } },
    sessionBindingExecutorId: target.sessionBindingExecutorId as string,
  };
}

function commandIntent(command: WorkerIngressCommand): string {
  if (command.command === "cancel") return canonical(command);
  const { instruction, ...safe } = command;
  return canonical({ ...safe, instructionDigest: digest(instruction) });
}
