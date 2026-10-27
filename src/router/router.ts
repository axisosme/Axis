import { createHash, randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import { runtimeContractVersion as runtimeContractVersion } from "@axisosme/axis-contracts/runtime";
import { agentActivityContractVersion } from "@axisosme/axis-contracts/agent-activity";
import type { RunAgentResponseDelivery, TaskAgentResponseDelivery } from "@axisosme/axis-contracts/agent-activity";

import { RouterHttpTransportError, WORKER_WAITING_EXTENSION_VERSION, validateRouterWorkerLifecycleEvent, type RouterRunLifecycleEnvelope, type WorkerExecutionIdentity, type WorkerRunWaitingForInputEnvelope, type WorkerVersionedReference } from "../shared/router-worker.js";
import { runtimeBoundaryVersion, runtimeSourceSessionIdentity } from "../shared/runtime-boundary.js";
import type {
  Attempt, Run, RunAcceptedEnvelope, RunFailedEnvelope, RunProgressedEnvelope, RunStartEnvelope, RunSucceededEnvelope,
  SessionBinding, Task, TaskAcceptedEnvelope, TaskFailedEnvelope, TaskProgressedEnvelope, TaskSucceededEnvelope,
} from "../shared/runtime-boundary.js";
import { validateStructuredElicitation, type StructuredElicitation } from "../shared/elicitation.js";
import type { ExecutionProfile, TeamBindingResolution } from "./team-bindings.js";
import { ChannelBindingError, type ChannelBindingIdentity, type ChannelBindingResolver } from "./binding-resolver.js";
import { ROUTER_TASK_IDENTITY_SCHEMA, RouterTaskIdentityError, assertRouterTaskIdentity, encodeRouterTaskIdentity, linearTaskIdentity, readRouterTaskIdentity, resolveRouterTaskIdentity, verifyRouterTaskIdentity, type RouterNeutralTaskIdentity, type RouterPersistedTaskIdentity, type RouterTaskIdentityColumns, type RouterTaskIdentityVerification } from "./task-identity.js";
import { routerNodeBindingDigest, validateRouterNodeRunTargetEvidence, validateRouterNodeTargetSnapshot, type RouterNodeRunTargetEvidence, type RouterNodeTargetSnapshot } from "../shared/node-routing-projection.js";
import { NODE_COMMAND_CONTRACT_VERSION, NODE_PENDING_COMMAND_LIMIT_BYTES, canonicalNodeCommandIntent, nodeCommandIntentDigest, validateNodeAuthenticatedContext, validateNodeCommandRequest, validateNodePriorRoute, validateNodeReceipt, nodeReceiptExpectationIntent, nodeReceiptExpectation, validateNodeReceiptExpectation, type NodeExpectedCommand, type NodeReceiptExpectation, validateNodeReservationEvidence, type NodeAuthenticatedContext, type NodeCommandRequest, type NodePendingRunStartEnvelope, type NodePriorRoute, type NodeReceiptQuery, type NodeReceipt, type NodeReservationEvidence, type NodeSessionBinding } from "../shared/router-node-commands.js";
import { NODE_OUTER_BODY_LIMIT_BYTES, type NodeErrorCode } from "../shared/router-node.js";

import { validateWorkerSelection, type WorkerSelection } from "../shared/worker-selection.js";
import type { RouterNodePort, RouterNodePortResult } from "./node-port.js";

export const ROUTER_VERSION = "1.0.0" as const;

export type RouterErrorCode = "axis.router.malformed" | "axis.router.intent_conflict" | "axis.router.conflict" | "axis.router.not_found" | "axis.router.recovery_required" | "axis.router.protocol_violation" | "axis.router.worker_rejected" | "axis.router.worker_identity_changed" | "axis.router.node_worker_unavailable" | "axis.router.binding_unconfigured" | "axis.router.binding_disabled" | "axis.router.binding_reference_missing" | "axis.router.binding_stale" | "axis.router.binding_incompatible" | "axis.router.endpoint_unknown" | "axis.router.authority_mismatch" | "axis.router.selection_conflict" | "axis.router.continuation_unresumable" | "axis.router.node_draining";
const messages: Readonly<Record<RouterErrorCode, string>> = {
  "axis.router.malformed": "The Router command is malformed.",
  "axis.router.intent_conflict": "The Router command ID is already bound to different intent.",
  "axis.router.conflict": "The Router command conflicts with durable task state.",
  "axis.router.not_found": "The Router task or Run was not found.",
  "axis.router.recovery_required": "The Router retained a command that cannot be resumed.",
  "axis.router.protocol_violation": "The Worker event does not match the durable Runtime identity.",
  "axis.router.worker_rejected": "The Worker did not accept the Router command.",
  "axis.router.continuation_unresumable": "The Worker could not resume the prior native session for this continuation.",
  // The Worker refused before reserving anything: its configured identity no longer
  // matches the Node inventory entry this session was reserved on. Operator action:
  // align the Worker's configuration with its inventory entry.
  "axis.router.worker_identity_changed": "The Worker's configured identity no longer matches the Node inventory.",
  // The routed Node deterministically carries no Worker able to take the command.
  // Distinct from `worker_rejected` (uncertain) and the `binding_*` codes (no route).
  "axis.router.node_worker_unavailable": "The routed Node has no available Worker.",
  "axis.router.binding_unconfigured": "No active execution binding is configured for this Linear Team.",
  "axis.router.binding_disabled": "The execution binding for this Linear Team is disabled.",
  "axis.router.binding_reference_missing": "The execution binding references unavailable configuration.",
  "axis.router.binding_stale": "The execution binding is stale.",
  "axis.router.binding_incompatible": "The execution binding is incompatible with Router.",
  "axis.router.endpoint_unknown": "The execution binding does not resolve to one configured endpoint.",
  "axis.router.authority_mismatch": "The captured Linear Team does not match the existing Task.",
  // The source named more than one executor. Its own code so each channel can tell
  // the user to fix the labels rather than Router state.
  "axis.router.selection_conflict": "The command names more than one executor.",
  // The routed Node is draining and refused before recording or sending anything;
  // the answer is to wait for the operator's resume.
  "axis.router.node_draining": "The routed Node is draining and accepted no new work.",
};
/**
 * The definite Node refusals Router answers a start dispatch with, and the Router
 * code for each. These carry an operator action: deploy a Worker, restore the
 * route, fix the labels, wait for the resume.
 *
 * The other definite codes (`malformed`, `intent_conflict`, `binding_conflict`,
 * `body_too_large`) are Router/Node protocol faults and keep the recovery path.
 */
const nodeRefusalSafeCodes: Partial<Record<NodeErrorCode, RouterErrorCode>> = Object.freeze({
  "axis.node.worker_unavailable": "axis.router.node_worker_unavailable",
  "axis.node.target_unknown": "axis.router.endpoint_unknown",
  // Refused before the Node writes or sends anything, so nothing ran and the start can be answered.
  "axis.node.selection_conflict": "axis.router.selection_conflict",
  // Refused before the Node writes anything, so nothing ran and the start can be answered.
  "axis.node.draining": "axis.router.node_draining",
});
/**
 * The definite Node refusals Router also answers for a `continue` or `cancel`,
 * with the same Router code `nodeRefusalSafeCodes` gives a start. Both are raised
 * before the Node records or sends anything: the retained route names a Worker no
 * longer in the inventory, or the Node is draining (a replayed in-flight command
 * is still `recovery_required`; see `drainExempt` in `src/node/journal.ts`).
 *
 * The answer is paired with retaining nothing (`#forgetRefusedNodeFollowUp`), so
 * `reconcileCommand` reads `absent` and the channel's sweep can settle it.
 */
const nodeFollowUpRefusalCodes: ReadonlySet<NodeErrorCode> = new Set<NodeErrorCode>(["axis.node.worker_unavailable", "axis.node.draining"]);
/**
 * Definite Worker refusals a rejected Node receipt carries that Router names with
 * their own code; every other rejection is `axis.router.worker_rejected`.
 */
const workerRejectionSafeCodes: Readonly<Record<string, RouterErrorCode>> = Object.freeze({
  "axis.worker.identity_mismatch": "axis.router.worker_identity_changed",
  // The Worker proved, before reserving the Run, that it cannot resume the prior
  // native session. Named so the channel can point at the session rather than
  // Router state.
  "axis.worker.continuation_unresumable": "axis.router.continuation_unresumable",
});
export class RouterError extends Error { constructor(readonly code: RouterErrorCode, options?: ErrorOptions) { super(messages[code], options); this.name = "RouterError"; } }
/**
 * Marks a selection or size failure before admitting a new command. Never thrown
 * across Router's boundary: `#nodeCommand` answers it as a rejected result.
 */
class NodeCommandAdmissionRejected extends Error { constructor(readonly code: RouterErrorCode, options?: ErrorOptions) { super(messages[code], options); this.name = "NodeCommandAdmissionRejected"; } }
/** Internal Router transport classification; never serialized as a Worker contract value. */
/**
 * Why a Router->Node exchange failed at the transport, as a closed category: no
 * address, message or credential. `timeout` is the port's own deadline.
 */
export type RouterNodeTransportFailure = "ECONNRESET" | "UND_ERR_SOCKET" | "EPIPE" | "timeout" | "other";

export class RouterWorkerDispatchError extends Error {
  constructor(
    readonly safeCode: string,
    readonly disposition: "definite" | "ambiguous",
    /** Set when the Node transport failed, so a `recovery_required` still carries its cause category. */
    readonly transportFailure?: RouterNodeTransportFailure,
  ) {
    super("The private Worker transport returned a classified startup failure.");
    this.name = "RouterWorkerDispatchError";
  }
}
function fail(code: RouterErrorCode): never { throw new RouterError(code); }

export interface RouterTeamBindingResolver { resolve(input: { readonly scope: { readonly organizationId: string; readonly linearTeamId: string } }): TeamBindingResolution; }
/** Explicit operator fallback for a known endpoint. Exact Team bindings always take precedence. */
export interface RouterDefaultTeamBinding { readonly profile: ExecutionProfile; readonly excludedTeamIds?: readonly string[]; }
export interface RouterNodeRoutingOptions {
  readonly bindingResolver: ChannelBindingResolver;
  readonly port: RouterNodePort;
  /** Trusted preparation after Router validates the exact predecessor route, before Node dispatch. */
  readonly beforeDispatch?: (request: NodeCommandRequest, prior: NodePriorRoute | undefined) => void;
  /**
   * (`router.nativeSteer`) A follow-up that reaches a still-running Run is sent
   * as a Node `steer` instead of a `cancel`. Read only when a command row is claimed;
   * the row's retained intent carries the choice after that.
   */
  readonly nativeSteer?: boolean;
}
export interface RouterOptions { readonly filename?: string; readonly nodeRouting?: RouterNodeRoutingOptions; readonly now?: () => Date; readonly id?: () => string; }
/** Linear's task reference, unchanged: no `system` discriminator, matching every row Router has ever written. */
export interface LinearRouterTaskRef { readonly organizationId: string; readonly issueId: string; readonly teamId: string; readonly agentSessionId: string; }

export type RouterTaskRef = LinearRouterTaskRef;

export type RouterStartInput = RouterTaskRef & { readonly commandId: string; readonly instruction: string; };
export type RouterNodeStartInput = RouterStartInput & { readonly workerSelection: WorkerSelection };
export type RouterContinueInput = RouterTaskRef & { readonly commandId: string; readonly instruction: string; };
export type RouterCancelInput = RouterTaskRef & { readonly commandId: string; };
/** Exact trusted Task scope plus one durable Run identity for target evidence. */
/** Linear-only, matching `readRunTarget`'s Team-scoped comparison. */
export type RouterReadRunTargetInput = LinearRouterTaskRef & { readonly runId: string; };
export type RouterCommandResult =
  | { readonly commandId: string; readonly taskId: string; readonly runId: string; readonly status: "accepted" | "interrupt_requested" | "steered" }
  | { readonly commandId: string; readonly status: "rejected"; readonly safeCode: RouterErrorCode; };
type AcceptedRouterCommandResult = { readonly commandId: string; readonly taskId: string; readonly runId: string; readonly status: "accepted" };
type WrappedRunAgentResponseDelivery = RunAgentResponseDelivery & { readonly envelopeType?: never; readonly messageId?: never; readonly run?: never; readonly task?: never; readonly attempt?: never; readonly progress?: never; readonly resultRef?: never; readonly error?: never };
type WrappedTaskAgentResponseDelivery = TaskAgentResponseDelivery & { readonly envelopeType?: never; readonly messageId?: never; readonly run?: never; readonly task?: never; readonly progress?: never; readonly resultRef?: never; readonly error?: never };

type RuntimeRunEvent = RunAcceptedEnvelope | RunProgressedEnvelope | RunSucceededEnvelope | RunFailedEnvelope;

type RouterWaitingEvent = Omit<WorkerRunWaitingForInputEnvelope, "task" | "attempt"> & { readonly task: Task; readonly attempt: Attempt };
export type RouterRunEvent = RuntimeRunEvent | WrappedRunAgentResponseDelivery | RouterWaitingEvent;
export interface RouterTaskWaitingForInputEnvelope {
  readonly contractVersion: typeof WORKER_WAITING_EXTENSION_VERSION; readonly messageId: string; readonly correlationId: string; readonly operationId: string; readonly causationMessageId: string; readonly occurredAt: string;
  readonly producer: "router"; readonly consumer: "relay"; readonly envelopeType: "task.waiting_for_input"; readonly task: Task;
  readonly run: { readonly runId: string; readonly state: "waiting_for_input" }; readonly attemptId: string; readonly elicitation: StructuredElicitation;
}
export type RouterTaskEvent = TaskAcceptedEnvelope | TaskProgressedEnvelope | TaskSucceededEnvelope | WrappedTaskAgentResponseDelivery | RouterTaskWaitingForInputEnvelope | TaskFailedEnvelope;
export interface RouterEventReceipt { readonly taskId: string; readonly runId: string; readonly cursor: number; readonly sourceMessageId: string; readonly relayEnvelope: RouterTaskEvent; readonly replayed: boolean; }
export interface RouterListedEvent { readonly cursor: number; readonly envelope: RouterTaskEvent; }
export type RouterNodeEventOrigin =
  | { readonly authentication: "authenticated"; readonly node: NodeAuthenticatedContext; readonly identity: WorkerExecutionIdentity }
  | { readonly authentication: "unavailable" };
export type RouterNodeEventIngestResult =
  | { readonly status: "buffered"; readonly sourceMessageId: string; readonly replayed: boolean }
  | { readonly status: "projected"; readonly sourceMessageId: string; readonly receipt: RouterEventReceipt; readonly replayed: boolean }
  | { readonly status: "quarantined"; readonly sourceMessageId: string; readonly safeCode: "axis.node.target_forbidden" | "axis.node.binding_conflict"; readonly replayed: boolean };

type CommandKind = "start" | "continue" | "cancel";
type RunState = "pending" | "running" | "waiting_for_input" | "succeeded" | "failed";
const retryNodeReceipt = Symbol("retry-node-receipt");
type Row = Record<string, unknown>;
function text(value: unknown): string | undefined { return typeof value === "string" && value.trim().length > 0 ? value : undefined; }
function canonical(value: unknown): string { if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`; if (value !== null && typeof value === "object") { const object = value as Record<string, unknown>; return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonical(object[key])}`).join(",")}}`; } return JSON.stringify(value); }
function digest(value: unknown): string { return createHash("sha256").update(canonical(value)).digest("hex"); }
function prefixedDigest(value: unknown): `sha256:${string}` { return `sha256:${digest(value)}`; }
function same(left: unknown, right: unknown): boolean { return canonical(left) === canonical(right); }
function parse<T>(value: unknown): T { return JSON.parse(String(value)) as T; }
function nodeRouterIntent(kind: CommandKind, normalized: RouterTaskRef & { readonly commandId: string }, input: RouterNodeStartInput | RouterContinueInput | RouterCancelInput, request: NodeCommandRequest): Readonly<Record<string, unknown>> {
  const caller = kind === "cancel"
    ? { kind, ...normalized }
    : { kind, ...normalized, instructionSha256: `sha256:${createHash("sha256").update((input as RouterStartInput).instruction).digest("hex")}` };
  return { caller, nodeCommand: canonicalNodeCommandIntent(request), nodeCommandIntentDigest: nodeCommandIntentDigest(request) };
}
function nodeEventIdentity(value: WorkerExecutionIdentity): WorkerExecutionIdentity {
  try { return Object.freeze({ nodeId: identifier(value.nodeId), workerId: identifier(value.workerId), executorKind: identifier(value.executorKind), runtimeProfile: versioned(value.runtimeProfile) }); }
  catch { fail("axis.router.protocol_violation"); }
}
function runEnvelope(event: Exclude<RouterRunEvent, WorkerRunWaitingForInputEnvelope>): RuntimeRunEvent { return "deliveryType" in event ? event.envelope : event; }

function isWaitingRunEvent(event: RouterRunEvent): event is RouterWaitingEvent {
  return !("deliveryType" in event) && event.envelopeType === "run.waiting_for_input";
}
/** The runtime envelope any Run-level event carries, unwrapping a delivery if the event is one. Generic across all four shapes `RouterRunEvent` admits. */
function runEventEnvelope(event: RouterRunEvent): RuntimeRunEvent | RouterWaitingEvent {
  return "deliveryType" in event ? event.envelope : event;
}
/**
 * Narrows a validated Worker lifecycle event to the current Runtime version.
 * `#assertIdentity` then compares it with the Task's persisted identity.
 */
function routerRuntimeEvent(event: RouterRunLifecycleEnvelope): RouterRunEvent {
  const version = "deliveryType" in event
    ? event.envelope.contractVersion
    : event.envelopeType === "run.waiting_for_input" ? event.task.contractVersion : event.contractVersion;
  // Defence in depth: the upstream validators already reject an unrecognized
  // version on all three branches. Kept so the door refuses before any Run lookup.
  if (runtimeBoundaryVersion(version) === null) throw new RouterHttpTransportError();
  return event as RouterRunEvent;
}
export function routerTaskEnvelope(event: RouterTaskEvent): TaskAcceptedEnvelope | TaskProgressedEnvelope | TaskSucceededEnvelope | RouterTaskWaitingForInputEnvelope | TaskFailedEnvelope { return "deliveryType" in event ? event.envelope : event; }

function canonicalSql(value: string | null): string { if (value === null) fail("axis.router.recovery_required"); return value.replace(/\s+/gu, " ").replace(/\s*([(),])/gu, "$1").trim(); }

const ROUTER_SCHEMA_VERSION = 1;
const nodeRoutingSchema = Object.freeze({
  "table:router_node_schema": "CREATE TABLE router_node_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
  "table:router_node_runs": "CREATE TABLE router_node_runs(run_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, prior_run_id TEXT, attempt_json TEXT NOT NULL, run_json TEXT NOT NULL, pending_binding_json TEXT NOT NULL, start_json TEXT NOT NULL, created_at TEXT NOT NULL)",
  "table:router_node_command_state": "CREATE TABLE router_node_command_state(command_id TEXT PRIMARY KEY, node_command_id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL CHECK(kind IN ('start','continue','cancel')), task_id TEXT NOT NULL, run_id TEXT NOT NULL, intent_json TEXT NOT NULL, intent_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','dispatching','receipt_pending','accepted','rejected')), reservation_json TEXT, receipt_digest TEXT, result_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
  "table:router_node_run_execution": "CREATE TABLE router_node_run_execution(run_id TEXT PRIMARY KEY, reservation_json TEXT NOT NULL, session_binding_json TEXT NOT NULL, receipt_digest TEXT NOT NULL, recorded_at TEXT NOT NULL)",
  "table:router_node_events": "CREATE TABLE router_node_events(source_message_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, event_json TEXT NOT NULL, event_digest TEXT NOT NULL, recorded_at TEXT NOT NULL, origin_json TEXT, origin_digest TEXT, state TEXT NOT NULL CHECK(state IN ('buffered','projected','quarantined')), safe_code TEXT, projected_cursor INTEGER, CHECK((state='buffered' AND safe_code IS NULL AND projected_cursor IS NULL) OR (state='projected' AND safe_code IS NULL AND projected_cursor IS NOT NULL) OR (state='quarantined' AND safe_code IS NOT NULL AND projected_cursor IS NULL)))",
  "table:router_node_conflicts": "CREATE TABLE router_node_conflicts(conflict_id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('receipt','event')), task_id TEXT NOT NULL, run_id TEXT NOT NULL, command_id TEXT, source_message_id TEXT, observed_digest TEXT NOT NULL, safe_code TEXT NOT NULL CHECK(safe_code IN ('axis.node.target_forbidden','axis.node.binding_conflict')), recorded_at TEXT NOT NULL)",
  "trigger:router_node_runs_immutable_update": "CREATE TRIGGER router_node_runs_immutable_update BEFORE UPDATE ON router_node_runs BEGIN SELECT RAISE(ABORT,'node run identity is immutable'); END",
  "trigger:router_node_runs_immutable_delete": "CREATE TRIGGER router_node_runs_immutable_delete BEFORE DELETE ON router_node_runs BEGIN SELECT RAISE(ABORT,'node run identity is immutable'); END",
  "trigger:router_node_command_state_one_way": "CREATE TRIGGER router_node_command_state_one_way BEFORE UPDATE ON router_node_command_state WHEN NEW.command_id != OLD.command_id OR NEW.node_command_id != OLD.node_command_id OR NEW.kind != OLD.kind OR NEW.task_id != OLD.task_id OR NEW.run_id != OLD.run_id OR NEW.intent_json != OLD.intent_json OR NEW.intent_digest != OLD.intent_digest OR NOT ((OLD.state='prepared' AND NEW.state='dispatching') OR (OLD.state IN ('dispatching','receipt_pending') AND NEW.state IN ('receipt_pending','accepted','rejected'))) BEGIN SELECT RAISE(ABORT,'node command state transition is invalid'); END",
  "trigger:router_node_run_execution_immutable_update": "CREATE TRIGGER router_node_run_execution_immutable_update BEFORE UPDATE ON router_node_run_execution BEGIN SELECT RAISE(ABORT,'node execution evidence is immutable'); END",
  "trigger:router_node_run_execution_immutable_delete": "CREATE TRIGGER router_node_run_execution_immutable_delete BEFORE DELETE ON router_node_run_execution BEGIN SELECT RAISE(ABORT,'node execution evidence is immutable'); END",
  "trigger:router_node_events_one_way": "CREATE TRIGGER router_node_events_one_way BEFORE UPDATE ON router_node_events WHEN NEW.source_message_id != OLD.source_message_id OR NEW.run_id != OLD.run_id OR NEW.event_json != OLD.event_json OR NEW.event_digest != OLD.event_digest OR NEW.recorded_at != OLD.recorded_at OR NEW.origin_json IS NOT OLD.origin_json OR NEW.origin_digest IS NOT OLD.origin_digest OR OLD.state != 'buffered' OR NEW.state NOT IN ('projected','quarantined') BEGIN SELECT RAISE(ABORT,'node event state transition is invalid'); END",
  "trigger:router_node_events_immutable_delete": "CREATE TRIGGER router_node_events_immutable_delete BEFORE DELETE ON router_node_events BEGIN SELECT RAISE(ABORT,'node events are immutable'); END",
  "trigger:router_node_conflicts_immutable_update": "CREATE TRIGGER router_node_conflicts_immutable_update BEFORE UPDATE ON router_node_conflicts BEGIN SELECT RAISE(ABORT,'node conflicts are immutable'); END",
  "trigger:router_node_conflicts_immutable_delete": "CREATE TRIGGER router_node_conflicts_immutable_delete BEFORE DELETE ON router_node_conflicts BEGIN SELECT RAISE(ABORT,'node conflicts are immutable'); END",
});
const routerSchema = Object.freeze({
  ...ROUTER_TASK_IDENTITY_SCHEMA,
  "table:router_runs": "CREATE TABLE router_runs(run_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, prior_run_id TEXT, attempt_json TEXT NOT NULL, run_json TEXT NOT NULL, binding_json TEXT NOT NULL, start_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','failed')), last_sequence INTEGER NOT NULL DEFAULT 0, terminal_message_id TEXT, waiting_for_input INTEGER NOT NULL DEFAULT 0 CHECK(waiting_for_input IN (0,1)))",
  "table:router_receipts": "CREATE TABLE router_receipts(source_message_id TEXT PRIMARY KEY, source_digest TEXT NOT NULL, task_id TEXT NOT NULL, run_id TEXT NOT NULL, run_sequence INTEGER NOT NULL, cursor INTEGER NOT NULL, source_json TEXT NOT NULL, relay_json TEXT NOT NULL, recorded_at TEXT NOT NULL, UNIQUE(run_id, run_sequence), UNIQUE(task_id, cursor))",
  ...nodeRoutingSchema,
});
const routerSchemaFingerprints = Object.freeze(Object.fromEntries(Object.entries(routerSchema).map(([name, sql]) => [name, canonicalSql(sql)])));

/** Package-private, read-only validation of the complete Router schema. */
export function assertRouterSchema(database: DatabaseSync): void {
  try {
    const objects = database.prepare("SELECT type,name,sql FROM sqlite_master WHERE (name GLOB 'router_*' OR tbl_name GLOB 'router_*') AND name NOT GLOB 'sqlite_*'").all() as { type: string; name: string; sql: string | null }[];
    if (objects.length !== Object.keys(routerSchemaFingerprints).length || objects.some((object) => routerSchemaFingerprints[`${object.type}:${object.name}`] !== canonicalSql(object.sql))) fail("axis.router.recovery_required");
    const ledger = database.prepare("SELECT version,applied_at FROM router_node_schema").all() as Row[];
    if (ledger.length !== 1 || ledger[0]?.version !== ROUTER_SCHEMA_VERSION || typeof ledger[0]?.applied_at !== "string" || instant(ledger[0].applied_at) !== ledger[0].applied_at) fail("axis.router.recovery_required");
  } catch { fail("axis.router.recovery_required"); }
}

/** Closed evidence from one persisted Node target snapshot. */
export type RouterRunTargetEvidence = RouterNodeRunTargetEvidence;
interface TaskRow { readonly taskId: string; readonly attemptId: string; readonly correlationId: string; readonly operationId: string; readonly task: Task; readonly teamId: string | null; readonly scopeId: string | null; readonly target: RouterNodeTargetSnapshot | null; readonly currentRunId: string | null; }
interface RunRow { readonly runId: string; readonly taskId: string; readonly attempt: Attempt; readonly run: Run; readonly binding: SessionBinding; readonly start: RunStartEnvelope; readonly state: RunState; readonly lastSequence: number; readonly terminalMessageId: string | null; }
/** A Task's `current_run_id` as a claim in this call found it, before the claim moved it. */
interface NodeClaimedPointer { readonly taskId: string; readonly currentRunId: string | null }
interface NodeRunRow { readonly runId: string; readonly taskId: string; readonly priorRunId: string | null; readonly attempt: Attempt; readonly run: Run; readonly pendingBinding: NodePendingRunStartEnvelope["identity"]["sessionBinding"]; readonly start: NodePendingRunStartEnvelope; readonly createdAt: string; }
type NodeCommandState = "prepared" | "dispatching" | "receipt_pending" | "accepted" | "rejected";
interface NodeCommandRow {
  readonly commandId: string; readonly nodeCommandId: string; readonly kind: CommandKind; readonly taskId: string; readonly runId: string;
  readonly intent: Readonly<Record<string, unknown>>; readonly intentDigest: string; readonly state: NodeCommandState;
  readonly reservation: NodeReservationEvidence | null; readonly receiptDigest: string | null; readonly result: RouterCommandResult | null;
  readonly createdAt: string; readonly updatedAt: string;
  /** Set only on the row `#claimNodeCommand` is building, before its intent exists; see `#interruptCommand`. */
  readonly interrupt?: "cancel" | "steer";
}
interface NodeExecutionRow { readonly reservation: NodeReservationEvidence; readonly sessionBinding: NodeSessionBinding; readonly receiptDigest: string; readonly recordedAt: string; }
interface NodeEventRow { readonly sourceMessageId: string; readonly runId: string; readonly event: unknown; readonly eventDigest: string; readonly recordedAt: string; readonly origin: WorkerExecutionIdentity | null; readonly originDigest: string | null; readonly state: "buffered" | "projected" | "quarantined"; readonly safeCode: "axis.node.target_forbidden" | "axis.node.binding_conflict" | null; readonly projectedCursor: number | null; }

/** Package-private seam, absent from `src/index.ts`. */
export interface RouterNodeTaskTargetOptions {
  readonly database: DatabaseSync;
  readonly bindingResolver: ChannelBindingResolver;
  readonly id?: () => string;
}
export interface RouterNodeTaskTargetInput {
  readonly taskRef: RouterTaskRef;
  readonly workerSelection: WorkerSelection;
}
export interface RouterNodeTaskTargetResult {
  readonly created: boolean;
  readonly taskId: string;
  readonly attemptId: string;
  readonly correlationId: string;
  readonly operationId: string;
  readonly task: Task;
  readonly target: RouterNodeTargetSnapshot;
}

/**
 * Router. It freezes one trusted target before creating an executable Task, then
 * derives every Run's Runtime v1 SessionBinding from that immutable snapshot.
 * Native bindings and caller instructions never enter Router durable state.
 */
export class Router {
  readonly #db: DatabaseSync; readonly #nodeRouting: RouterNodeRoutingOptions | undefined; readonly #now: () => Date; readonly #id: () => string;
  readonly #nodeInFlight = new Map<string, Promise<RouterCommandResult>>();
  constructor(options: RouterOptions) {
    this.#db = new DatabaseSync(options.filename ?? ":memory:"); this.#nodeRouting = options.nodeRouting; this.#now = options.now ?? (() => new Date()); this.#id = options.id ?? randomUUID;
    try {
      const existing = this.#db.prepare("SELECT name FROM sqlite_master WHERE name GLOB 'router_*'").all();
      if (existing.length !== 0) assertRouterSchema(this.#db);
      this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
      if (existing.length === 0) this.#createSchema();
      this.#assertNodeRouting();
      assertRouterTaskIdentity(this.#db);
    } catch (error) {
      try { this.#db.close(); } catch { /* construction failed closed */ }
      if (error instanceof RouterError) throw error;
      // `cause` carries the schema or identity failure behind the generic code.
      throw new RouterError("axis.router.recovery_required", { cause: error });
    }
  }
  close(): void { this.#db.close(); }

  async startNode(input: RouterNodeStartInput): Promise<RouterCommandResult> { return this.#nodeCommand("start", input); }
  async continueNode(input: RouterContinueInput): Promise<RouterCommandResult> { return this.#nodeCommand("continue", input); }
  async cancelNode(input: RouterCancelInput): Promise<RouterCommandResult> { return this.#nodeCommand("cancel", input); }
  /** Read-only command reconciliation. It never dispatches or retries a retained command. */
  reconcileCommand(commandId: string): "accepted" | "interrupt_requested" | "steered" | "absent" | "unknown" {
    if (text(commandId) === undefined) fail("axis.router.malformed");
    const result = this.#retainedCommandResult(commandId);
    if (result === undefined) return "absent";
    if (result === null) return "unknown";
    return result.status === "accepted" || result.status === "interrupt_requested" || result.status === "steered" ? result.status : "unknown";
  }

  /** Read-only accepted lineage, retained for crash recovery without command replay. */
  reconcileAcceptedCommand(commandId: string): { readonly taskId: string; readonly runId: string } | null {
    if (text(commandId) === undefined) fail("axis.router.malformed");
    const result = this.#retainedCommandResult(commandId);
    if (result?.status !== "accepted" || text(result.taskId) === undefined || text(result.runId) === undefined) return null;
    return { taskId: result.taskId, runId: result.runId };
  }

  /**
   * Companion to `reconcileCommand` for `interrupt_requested`: the Task/Run identity
   * retained with that result, so Relay can restore an interrupted continue's
   * redispatch marker. Null unless the retained result for this commandId is
   * `interrupt_requested`.
   */
  reconcileInterruptedCommand(commandId: string): { readonly taskId: string; readonly runId: string } | null {
    if (text(commandId) === undefined) fail("axis.router.malformed");
    const result = this.#retainedCommandResult(commandId);
    if (result === undefined || result === null) return null;
    if (result.status !== "interrupt_requested") return null;
    return { taskId: result.taskId, runId: result.runId };
  }

  /**
   * The `steered` counterpart of `reconcileInterruptedCommand`. Null unless the
   * retained result for this commandId is `steered`.
   */
  reconcileSteeredCommand(commandId: string): { readonly taskId: string; readonly runId: string } | null {
    if (text(commandId) === undefined) fail("axis.router.malformed");
    const result = this.#retainedCommandResult(commandId);
    if (result === undefined || result === null || result.status !== "steered") return null;
    return { taskId: result.taskId, runId: result.runId };
  }

  /** Live admitted execution facts only. Business approval is separately owned by Relay. */
  readLinearExecution(runId: string): { taskId: string; attemptId: string; runId: string; organizationId: string; sourceTeamId: string } | null {
    const node = this.#nodeRun(runId), core = this.#run(runId), run = node ?? core;
    if (run === undefined || (core !== undefined && core.state !== "pending" && core.state !== "running")
      || (node !== undefined && this.#nodeRunHasRejectedCommand(runId))) return null;
    const task = this.#taskById(run.taskId);
    if (task === undefined || task.currentRunId !== runId || task.teamId === null || task.task.sourceRef.system !== "linear") return null;
    return { taskId: task.taskId, attemptId: run.attempt.attemptId, runId,
      organizationId: task.task.sourceRef.organizationId, sourceTeamId: task.teamId };
  }

  /** Reads a retained command result without advancing its lifecycle. */
  #retainedCommandResult(commandId: string): RouterCommandResult | null | undefined {
    const node = this.#nodeCommandRow(commandId);
    return node === undefined ? undefined : node.result;
  }

  /**
   * Ingests one Node-delivered Run event. A buffered event is evidence the executor
   * ran even when the dispatch response was lost, so the door re-reads the Node's
   * receipt (never re-dispatches) and reports what the event became.
   */
  async ingestNodeRunEvent(input: { readonly envelope: RouterRunLifecycleEnvelope; readonly recordedAt: string; readonly origin: RouterNodeEventOrigin }): Promise<RouterNodeEventIngestResult> {
    const result = await this.#ingestNodeRunEvent(input);
    if (result.status !== "buffered") return result;
    const buffered = this.#nodeEvent(result.sourceMessageId);
    if (buffered === undefined || await this.#convergeStrandedNodeRun(buffered.runId) === null) return result;
    const current = this.#nodeEvent(result.sourceMessageId), nodeRun = current === undefined ? undefined : this.#nodeRun(current.runId);
    if (current === undefined || nodeRun === undefined) fail("axis.router.recovery_required");
    if (current.state === "projected") return { status: "projected", sourceMessageId: current.sourceMessageId, receipt: { ...this.#nodeProjectedReceipt(current, nodeRun), replayed: result.replayed }, replayed: result.replayed };
    if (current.state === "quarantined") return { status: "quarantined", sourceMessageId: current.sourceMessageId, safeCode: current.safeCode!, replayed: result.replayed };
    return result;
  }

  async #ingestNodeRunEvent(input: { readonly envelope: RouterRunLifecycleEnvelope; readonly recordedAt: string; readonly origin: RouterNodeEventOrigin }): Promise<RouterNodeEventIngestResult> {
    const recordedAt = instant(input.recordedAt);
    let event: RouterRunEvent;
    try { event = routerRuntimeEvent(await validateRouterWorkerLifecycleEvent(input.envelope)); }
    catch (error) { if (error instanceof RouterHttpTransportError) fail("axis.router.protocol_violation"); throw error; }
    const envelope = runEventEnvelope(event), sourceDigest = prefixedDigest(event);
    let origin: WorkerExecutionIdentity | null = null, authenticatedNode: NodeAuthenticatedContext | null = null;
    if (input.origin.authentication === "authenticated") {
      origin = nodeEventIdentity(input.origin.identity);
      try { authenticatedNode = validateNodeAuthenticatedContext(input.origin.node); }
      catch { fail("axis.router.protocol_violation"); }
    }
    const originJson = origin === null ? null : canonical(origin), originDigest = origin === null ? null : prefixedDigest(origin);
    const nodeRun = this.#nodeRun(envelope.run.runId); if (nodeRun === undefined) fail("axis.router.not_found");
    const task = this.#taskById(nodeRun.taskId); if (task === undefined || task.target?.contractVersion !== "1.0.0-node") fail("axis.router.recovery_required");
    if (authenticatedNode !== null
      && (!same(authenticatedNode.nodeRef, task.target.nodeRef) || authenticatedNode.nodeId !== origin?.nodeId)) fail("axis.router.conflict");
    const existing = this.#nodeEvent(envelope.messageId);
    if (existing !== undefined) {
      if (existing.eventDigest !== sourceDigest || existing.originDigest !== originDigest || (existing.origin === null ? origin !== null : !same(existing.origin, origin))) fail("axis.router.intent_conflict");
      const existingRun = this.#nodeRun(existing.runId); if (existingRun === undefined) fail("axis.router.recovery_required");
      if (existing.state === "projected") return { status: "projected", sourceMessageId: envelope.messageId, receipt: { ...this.#nodeProjectedReceipt(existing, existingRun), replayed: true }, replayed: true };
      if (existing.state === "quarantined") return { status: "quarantined", sourceMessageId: envelope.messageId, safeCode: existing.safeCode!, replayed: true };
      return this.#transaction(() => {
        const current = this.#nodeEvent(envelope.messageId); if (current === undefined) fail("axis.router.recovery_required");
        if (current.state === "projected") return { status: "projected", sourceMessageId: envelope.messageId, receipt: { ...this.#nodeProjectedReceipt(current, existingRun), replayed: true }, replayed: true } as const;
        if (current.state === "quarantined") return { status: "quarantined", sourceMessageId: envelope.messageId, safeCode: current.safeCode!, replayed: true } as const;
        const execution = this.#nodeExecution(existingRun.runId);
        if (execution === undefined) return { status: "buffered", sourceMessageId: envelope.messageId, replayed: true } as const;
        const task = this.#taskById(existingRun.taskId); if (task === undefined || task.target?.contractVersion !== "1.0.0-node") fail("axis.router.recovery_required");
        return this.#classifyBufferedNodeEvent(current, event, task, existingRun, execution, origin, true);
      });
    }
    return this.#transaction(() => {
      const execution = this.#nodeExecution(nodeRun.runId);
      const currentReservation = execution?.reservation ?? this.#nodeReservationForRun(nodeRun.runId);
      if (origin === null || (currentReservation !== null && !same(origin, currentReservation.target.identity))) {
        const safeCode = "axis.node.target_forbidden" as const;
        this.#insertNodeEvent(nodeRun, event, sourceDigest, recordedAt, originJson, originDigest, "quarantined", safeCode, null);
        this.#recordNodeConflict("event", task.taskId, nodeRun.runId, null, envelope.messageId, sourceDigest, safeCode);
        return { status: "quarantined", sourceMessageId: envelope.messageId, safeCode, replayed: false } as const;
      }
      if (execution === undefined && this.#nodeRunHasRejectedCommand(nodeRun.runId)) {
        const safeCode = "axis.node.binding_conflict" as const;
        this.#insertNodeEvent(nodeRun, event, sourceDigest, recordedAt, originJson, originDigest, "quarantined", safeCode, null);
        this.#recordNodeConflict("event", task.taskId, nodeRun.runId, null, envelope.messageId, sourceDigest, safeCode);
        return { status: "quarantined", sourceMessageId: envelope.messageId, safeCode, replayed: false } as const;
      }
      this.#insertNodeEvent(nodeRun, event, sourceDigest, recordedAt, originJson, originDigest, "buffered", null, null);
      if (execution === undefined) return { status: "buffered", sourceMessageId: envelope.messageId, replayed: false } as const;
      return this.#classifyBufferedNodeEvent(this.#nodeEvent(envelope.messageId)!, event, task, nodeRun, execution, origin, false);
    });
  }

  listTaskEvents(input: RouterTaskRef & { readonly afterCursor?: number; readonly limit?: number }): readonly RouterListedEvent[] {
    const task = this.#task(input); if (task === undefined) return []; const after = input.afterCursor ?? 0; const limit = input.limit ?? 100;
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 100) fail("axis.router.malformed");
    return (this.#db.prepare("SELECT cursor, relay_json FROM router_receipts WHERE task_id = ? AND cursor > ? ORDER BY cursor LIMIT ?").all(task.taskId, after, limit) as Row[]).map((row) => ({ cursor: Number(row.cursor), envelope: parse<RouterTaskEvent>(row.relay_json) }));
  }

  /**
   * Reads only the immutable target persisted for a trusted Task/Run pair.
   * It neither resolves current bindings nor reaches an endpoint or Worker.
   */
  readRunTarget(input: RouterReadRunTargetInput): RouterRunTargetEvidence {
    const normalized = readRunTargetInput(input); const task = this.#taskForRunTargetEvidence(normalized);
    if (task === undefined) fail("axis.router.not_found");
    if (task.target === null) fail("axis.router.recovery_required");
    const run = this.#nodeRun(normalized.runId); if (run === undefined || run.taskId !== task.taskId) fail("axis.router.not_found");
    const execution = this.#nodeExecution(run.runId);
    return validateRouterNodeRunTargetEvidence({
      contractVersion: "1.0.0", runId: run.runId, target: task.target,
      execution: execution === undefined
        ? { status: "pending", workerId: null, executorId: null }
        : { status: "recorded", workerId: execution.sessionBinding.workerId, executorId: execution.sessionBinding.executorId },
    });
  }

  /**
   * Reads the provider-neutral identity persisted for one Task, plus its Linear
   * identity columns. Resolves no binding, reaches no endpoint, creates nothing.
   */
  readTaskIdentity(taskId: string): RouterPersistedTaskIdentity | null {
    if (text(taskId) === undefined) fail("axis.router.malformed");
    try { return readRouterTaskIdentity(this.#db, taskId); } catch (error) { fail(taskIdentityError(error)); }
  }

  resolveTaskIdentity(identity: RouterNeutralTaskIdentity): RouterPersistedTaskIdentity | null {
    try { return resolveRouterTaskIdentity(this.#db, identity); } catch (error) { fail(taskIdentityError(error)); }
  }

  /**
   * Re-runs the task identity verification on demand. One statement, so it takes no
   * write lock on a live Router.
   */
  verifyTaskIdentity(): RouterTaskIdentityVerification {
    try { return verifyRouterTaskIdentity(this.#db); } catch (error) { fail(taskIdentityError(error)); }
  }

  async #nodeCommand(kind: CommandKind, input: RouterNodeStartInput | RouterContinueInput | RouterCancelInput): Promise<RouterCommandResult> {
    if (this.#nodeRouting === undefined) fail("axis.router.recovery_required");
    const normalized = commandInput(kind, input);
    let selection: WorkerSelection | undefined;
    if (kind === "start") {
      try { selection = validateWorkerSelection((input as RouterNodeStartInput).workerSelection); } catch { fail("axis.router.malformed"); }
    }
    const active = this.#nodeInFlight.get(normalized.commandId); if (active !== undefined) return active;
    let prepared: { readonly row: NodeCommandRow; readonly request: NodeCommandRequest; readonly result: RouterCommandResult | null };
    // The Task's `current_run_id` as the claim below found it, recorded only when the
    // claim happens in this call. `#forgetRefusedNodeFollowUp` restores this value.
    let claimed: NodeClaimedPointer | undefined;
    try {
      prepared = this.#transaction(() => {
        let row = this.#nodeCommandRow(normalized.commandId);
        if (row === undefined) { const fresh = this.#claimNodeCommand(kind, normalized, input, selection); row = fresh.row; claimed = fresh.claimed; }
        const request = this.#nodeRequest(row, input);
        const intent = nodeRouterIntent(kind, normalized, input, request);
        if (row.intentDigest !== prefixedDigest(intent) || !same(row.intent, intent)) fail("axis.router.intent_conflict");
        if (row.state === "accepted" || row.state === "rejected") {
          if (row.result === null) fail("axis.router.recovery_required");
          return { row, request, result: row.result };
        }
        return { row, request, result: null };
      });
    } catch (error) {
      // Admission failures return a definite rejection after the claim transaction
      // rolls back. Other malformed input and identity errors still throw.
      //
      // The claim writes have rolled back and Node was never contacted, so
      // `reconcileCommand` reads `absent` for the rejected command.
      if (!(error instanceof NodeCommandAdmissionRejected)) throw error;
      return { commandId: normalized.commandId, status: "rejected", safeCode: error.code };
    }
    if (prepared.result !== null) return prepared.result;
    const work = this.#dispatchNodeCommand(prepared.row, prepared.request, claimed);
    this.#nodeInFlight.set(normalized.commandId, work);
    let failure: unknown;
    try { return await work; } catch (error) { failure = error; } finally { this.#nodeInFlight.delete(normalized.commandId); }
    // An event that reached the door while this dispatch was in flight was buffered;
    // its evidence is checked again now that the row is `receipt_pending`.
    const converged = await this.#convergeStrandedNodeCommand(normalized.commandId);
    if (converged !== null) return converged;
    throw failure;
  }

  #claimNodeCommand(kind: CommandKind, normalized: RouterTaskRef & { readonly commandId: string }, input: RouterNodeStartInput | RouterContinueInput | RouterCancelInput, selection: WorkerSelection | undefined): { readonly row: NodeCommandRow; readonly claimed: NodeClaimedPointer } {
    if (this.#nodeRouting === undefined) fail("axis.router.recovery_required");
    let task = this.#task(normalized);
    if (task === undefined) {
      if (kind !== "start" || selection === undefined) fail("axis.router.not_found");
      try { freezeRouterNodeTaskTargetInTransaction({ database: this.#db, bindingResolver: this.#nodeRouting.bindingResolver, id: this.#id }, { taskRef: normalized, workerSelection: selection }); }
      catch (error) { if (error instanceof RouterError && selectionError(error.code)) throw new NodeCommandAdmissionRejected(error.code, { cause: error }); throw error; }
      task = this.#task(normalized);
    }
    if (task === undefined || task.target?.contractVersion !== "1.0.0-node") fail("axis.router.recovery_required");
    if (kind === "start" && (selection === undefined || !same(selection, task.target.workerSelection))) fail("axis.router.conflict");
    let nodeRun: NodeRunRow;
    let nodeCommandId = normalized.commandId;
    const claimed: NodeClaimedPointer = { taskId: task.taskId, currentRunId: task.currentRunId };
    if (kind === "start") {
      if (this.#latestNodeRun(task.taskId) !== undefined) fail("axis.router.conflict");
      nodeRun = this.#createNodeRun(task, null, normalized.commandId);
    } else {
      const currentId = task.currentRunId;
      const prior = currentId === null ? this.#latestNodeRun(task.taskId) : this.#nodeRun(currentId);
      if (prior === undefined) fail("axis.router.conflict");
      const core = this.#run(prior.runId);
      const active = core === undefined || core.state === "pending" || core.state === "running";
      if (kind === "continue" && !active) nodeRun = this.#createNodeRun(task, prior.runId, normalized.commandId);
      else {
        if (this.#nodeExecution(prior.runId) === undefined) fail("axis.router.recovery_required");
        nodeRun = prior;
        if (kind === "continue") nodeCommandId = this.#id();
      }
    }
    // The one place `nativeSteer` is read. The choice is rendered into the intent below and fixed there.
    const interrupt = kind === "continue" && nodeCommandId !== normalized.commandId ? (this.#nodeRouting.nativeSteer === true ? "steer" as const : "cancel" as const) : undefined;
    const provisional: NodeCommandRow = {
      commandId: normalized.commandId, nodeCommandId, kind, taskId: task.taskId, runId: nodeRun.runId,
      intent: {}, intentDigest: "", state: "prepared", reservation: null, receiptDigest: null, result: null,
      createdAt: this.#instant(), updatedAt: this.#instant(),
      ...(interrupt === undefined ? {} : { interrupt }),
    };
    const request = this.#nodeRequest(provisional, input);
    if (Buffer.byteLength(JSON.stringify(request.command), "utf8") > NODE_PENDING_COMMAND_LIMIT_BYTES
      || Buffer.byteLength(JSON.stringify(request), "utf8") > NODE_OUTER_BODY_LIMIT_BYTES) {
      throw new NodeCommandAdmissionRejected("axis.router.malformed");
    }
    const intent = nodeRouterIntent(kind, normalized, input, request), intentDigest = prefixedDigest(intent), createdAt = this.#instant();
    this.#db.prepare("INSERT INTO router_node_command_state(command_id,node_command_id,kind,task_id,run_id,intent_json,intent_digest,state,reservation_json,receipt_digest,result_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,'prepared',NULL,NULL,NULL,?,?)")
      .run(normalized.commandId, nodeCommandId, kind, task.taskId, nodeRun.runId, canonical(intent), intentDigest, createdAt, createdAt);
    return { row: { ...provisional, intent, intentDigest, createdAt, updatedAt: createdAt }, claimed };
  }

  /**
   * The command fields Router derives from durable state: everything a Node request
   * carries except the caller's instruction. `#nodeRequest` adds the instruction;
   * `#nodeReceiptExpectation` validates the rest for a re-read.
   */
  #nodeCommandBasis(row: NodeCommandRow): { readonly nodeRef: WorkerVersionedReference; readonly selection: WorkerSelection; readonly command: NodeExpectedCommand } {
    const task = this.#taskById(row.taskId), nodeRun = this.#nodeRun(row.runId);
    if (task === undefined || task.target?.contractVersion !== "1.0.0-node" || nodeRun === undefined) fail("axis.router.recovery_required");
    const common = { nodeRef: task.target.nodeRef, selection: task.target.workerSelection } as const;
    if (row.kind === "start") {
      return { ...common, command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: row.nodeCommandId, command: "start", start: nodeRun.start, workspacePolicyRef: task.target.workspacePolicyRef } };
    }
    const interrupt = row.kind === "continue" && row.nodeCommandId !== row.commandId;
    if (row.kind === "cancel" || (interrupt && this.#interruptCommand(row) === "cancel")) {
      return { ...common, command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: row.nodeCommandId, command: "cancel", target: { runId: nodeRun.runId } } };
    }
    if (interrupt) return { ...common, command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: row.nodeCommandId, command: "steer", target: { runId: nodeRun.runId } } };
    if (nodeRun.priorRunId === null) fail("axis.router.recovery_required");
    return { ...common, command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: row.nodeCommandId, command: "continue", start: nodeRun.start, continuation: { priorRunId: nodeRun.priorRunId }, workspacePolicyRef: task.target.workspacePolicyRef } };
  }

  /**
   * Whether an interrupting `continue` row is sent as `cancel` or `steer`. A row being
   * claimed carries the choice from `nativeSteer`; a row read back answers from its
   * retained intent, never from the live flag.
   */
  #interruptCommand(row: NodeCommandRow): "cancel" | "steer" {
    if (row.interrupt !== undefined) return row.interrupt;
    const retained = (row.intent.nodeCommand as { readonly command?: { readonly command?: unknown } } | undefined)?.command?.command;
    if (retained === "cancel" || retained === "steer") return retained;
    fail("axis.router.recovery_required");
  }

  #nodeRequest(row: NodeCommandRow, input: RouterNodeStartInput | RouterContinueInput | RouterCancelInput): NodeCommandRequest {
    const basis = this.#nodeCommandBasis(row), command = basis.command;
    const common = { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: basis.nodeRef, target: { selection: basis.selection } } as const;
    if (command.command === "cancel") return { ...common, command };
    const instruction = (input as RouterNodeStartInput | RouterContinueInput).instruction;
    // A steer carries the caller's instruction to the running Run, and nothing else.
    if (command.command === "steer") return { ...common, command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: command.commandId, command: "steer", target: command.target, instruction } };
    if (command.command === "continue") {
      return { ...common, command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: command.commandId, command: "continue", start: command.start, instruction, continuation: command.continuation, workspacePolicyRef: command.workspacePolicyRef } };
    }
    return { ...common, command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: command.commandId, command: "start", start: command.start, instruction, workspacePolicyRef: command.workspacePolicyRef } };
  }

  /**
   * What a receipt re-read is validated against, rebuilt from durable state. It has
   * no instruction and cannot be dispatched, and it must equal the redacted intent
   * retained at claim time, so a re-read only validates the dispatched command.
   */
  async #nodeReceiptExpectation(row: NodeCommandRow): Promise<{ readonly expectation: NodeReceiptExpectation; readonly prior: NodePriorRoute | undefined }> {
    const basis = this.#nodeCommandBasis(row);
    const prior = this.#nodePriorRouteFor(basis.command);
    let expectation: NodeReceiptExpectation;
    try { expectation = await validateNodeReceiptExpectation({ expectation: "node-receipt", ...basis }, prior); } catch { fail("axis.router.recovery_required"); }
    const retained = (row.intent.nodeCommand as { readonly command?: { readonly instructionSha256?: unknown } } | undefined)?.command?.instructionSha256;
    const instructionSha256 = expectation.command.command === "cancel" ? null : retained;
    if ((instructionSha256 !== null && typeof instructionSha256 !== "string")
      || !same(nodeReceiptExpectationIntent(expectation, instructionSha256), row.intent.nodeCommand)) fail("axis.router.recovery_required");
    return { expectation, prior };
  }

  /** The command whose receipt would let this Run's buffered events project, if one is stranded. */
  async #convergeStrandedNodeRun(runId: string): Promise<RouterCommandResult | null> {
    const row = this.#db.prepare("SELECT command_id FROM router_node_command_state WHERE run_id=? AND state='receipt_pending' AND kind IN ('start','continue') AND node_command_id=command_id ORDER BY rowid LIMIT 1").get(runId) as Row | undefined;
    return row === undefined ? null : this.#convergeStrandedNodeCommand(String(row.command_id));
  }

  /**
   * Converges a command whose dispatch response was lost, from the receipt the Node
   * holds. Acts only on a start or plain continue left `receipt_pending` with no
   * execution and at least one buffered event, and only reads the receipt. Applied
   * through `#settleNodeResponse` -> `#applyNodeReceipt`, registered in
   * `#nodeInFlight` first so concurrent callers join it. Returns null when there was
   * nothing to converge, the command is in flight, or the re-read failed.
   */
  async #convergeStrandedNodeCommand(commandId: string): Promise<RouterCommandResult | null> {
    if (this.#nodeRouting === undefined || this.#nodeInFlight.has(commandId)) return null;
    let row: NodeCommandRow | undefined;
    try {
      row = this.#nodeCommandRow(commandId);
      if (row === undefined || row.state !== "receipt_pending" || (row.kind !== "start" && row.kind !== "continue") || row.nodeCommandId !== row.commandId
        || this.#nodeExecution(row.runId) !== undefined || this.#nodeBufferedEvents(row.runId).length === 0) return null;
    } catch { return null; }
    const work = this.#rereadStrandedNodeReceipt(row);
    this.#nodeInFlight.set(commandId, work);
    try { return await work; } catch { return null; } finally { if (this.#nodeInFlight.get(commandId) === work) this.#nodeInFlight.delete(commandId); }
  }
  async #rereadStrandedNodeReceipt(row: NodeCommandRow): Promise<RouterCommandResult> {
    const { expectation, prior } = await this.#nodeReceiptExpectation(row);
    return this.#rereadNodeReceipt(row, expectation, prior);
  }

  async #dispatchNodeCommand(row: NodeCommandRow, requestValue: NodeCommandRequest, claimed?: NodeClaimedPointer): Promise<RouterCommandResult> {
    if (this.#nodeRouting === undefined) fail("axis.router.recovery_required");
    const prior = this.#nodePriorRouteFor(requestValue.command);
    let request: NodeCommandRequest, expectation: NodeReceiptExpectation;
    try {
      request = await validateNodeCommandRequest(requestValue, prior);
      expectation = await nodeReceiptExpectation(request, prior);
    } catch { fail("axis.router.recovery_required"); }
    // An exact re-presentation of a command already past `prepared` is a receipt query, and needs
    // nothing from the request but what the expectation already carries.
    if (row.state !== "prepared") return this.#rereadNodeReceipt(row, expectation, prior);
    const changed = this.#db.prepare("UPDATE router_node_command_state SET state='dispatching',updated_at=? WHERE command_id=? AND state='prepared'").run(this.#instant(), row.commandId);
    if (changed.changes !== 1) fail("axis.router.recovery_required");
    let response: RouterNodePortResult;
    try {
      this.#nodeRouting.beforeDispatch?.(request, prior);
      response = await this.#nodeRouting.port.dispatch(request);
    } catch (error) {
      // A definite transport classification on a dispatch means the Node refused before
      // accepting: nothing ran, so the caller gets an answer instead of an unknown
      // outcome. Only for a dispatch (a refused receipt query says nothing), only for
      // the mapped codes, and only for a `start`, or a follow-up in
      // `nodeFollowUpRefusalCodes` together with `#forgetRefusedNodeFollowUp`.
      const definite = error instanceof RouterWorkerDispatchError && error.disposition === "definite" ? error.safeCode as NodeErrorCode : undefined;
      const safeCode = definite === undefined ? undefined : nodeRefusalSafeCodes[definite];
      if (safeCode !== undefined && definite !== undefined && row.kind !== "start" && nodeFollowUpRefusalCodes.has(definite)) {
        const forgotten = this.#forgetRefusedNodeFollowUp(row, claimed);
        if (forgotten) return { commandId: row.commandId, status: "rejected", safeCode };
      }
      const refusal = row.kind === "start" ? safeCode : undefined;
      // The row stays `receipt_pending`: a terminal `rejected` row needs reservation and
      // receipt evidence a pre-receipt refusal does not have. Only the answer changes.
      this.#markNodeReceiptPending(row);
      if (refusal !== undefined) return { commandId: row.commandId, status: "rejected", safeCode: refusal };
      // The transport failure and its category stay on the error chain.
      throw new RouterError("axis.router.recovery_required", { cause: error });
    }
    return this.#settleNodeResponse(row, expectation, prior, response, 202, claimed);
  }

  /** A receipt query for a command already past `prepared`. It holds an expectation, which cannot be dispatched. */
  async #rereadNodeReceipt(row: NodeCommandRow, expectation: NodeReceiptExpectation, prior: NodePriorRoute | undefined): Promise<RouterCommandResult> {
    if (this.#nodeRouting === undefined) fail("axis.router.recovery_required");
    let response: RouterNodePortResult;
    try {
      response = await this.#nodeRouting.port.readReceipt({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: expectation.nodeRef, commandId: expectation.command.commandId });
    } catch (error) {
      this.#markNodeReceiptPending(row);
      throw new RouterError("axis.router.recovery_required", { cause: error });
    }
    return this.#settleNodeResponse(row, expectation, prior, response, 200);
  }

  #markNodeReceiptPending(row: NodeCommandRow): void {
    this.#db.prepare("UPDATE router_node_command_state SET state='receipt_pending',updated_at=? WHERE command_id=? AND state IN ('dispatching','receipt_pending')").run(this.#instant(), row.commandId);
  }

  /**
   * Retains nothing for a follow-up the Node refused before recording or sending it,
   * so `reconcileCommand` reads `absent` and the channel can settle. Returns false,
   * changing nothing, unless every precondition still holds.
   *
   * A `continue` of a settled Run already created an immutable Node Run and made it
   * current. The Task's pointer is moved back to the prior Run; the orphan has no
   * command, execution or event and is never current again.
   *
   * Preconditions, checked in one transaction: the row is `dispatching` with no
   * result, receipt or reservation; for a new-Run `continue`, that Run has no
   * execution and no event, is still current, and names its prior Run.
   */
  #forgetRefusedNodeFollowUp(row: NodeCommandRow, claimed: NodeClaimedPointer | undefined): boolean {
    try {
      return this.#transaction(() => {
        const current = this.#nodeCommandRow(row.commandId);
        if (current === undefined || current.state !== "dispatching" || current.result !== null || current.receiptDigest !== null || current.reservation !== null
          || current.kind !== row.kind || current.runId !== row.runId || current.nodeCommandId !== row.nodeCommandId) return false;
        const createdRun = current.kind === "continue" && current.nodeCommandId === current.commandId;
        if (createdRun && !this.#restoreClaimedPointerFromRefusedSuccessor(current, claimed)) return false;
        const removed = this.#db.prepare("DELETE FROM router_node_command_state WHERE command_id=? AND state='dispatching' AND result_json IS NULL AND receipt_digest IS NULL AND reservation_json IS NULL").run(current.commandId);
        if (removed.changes !== 1) fail("axis.router.recovery_required");
        return true;
      });
    } catch { return false; }
  }

  /**
   * For a follow-up `continue` that created a new Node Run and was refused before it
   * executed, by a pre-dispatch refusal or a definite Worker rejection: restores the
   * Task's pointer to exactly the value this continue's claim recorded. Returns
   * false, changing nothing, unless the command is a `continue` that created its
   * Run, the claim was made in this call, and the Run names its prior Run, has no
   * execution or event, and is still current. Runs inside the caller's transaction.
   *
   * `#latestNodeRun` skips such a Run. It reads as `pending` with no Worker in
   * `readRunTarget`, has no events, and any event for it is quarantined as
   * `binding_conflict`.
   */
  #restoreClaimedPointerFromRefusedSuccessor(command: NodeCommandRow, claimed: NodeClaimedPointer | undefined): boolean {
    if (command.kind !== "continue" || command.nodeCommandId !== command.commandId) return false;
    if (claimed === undefined || claimed.taskId !== command.taskId) return false;
    const nodeRun = this.#nodeRun(command.runId), task = this.#taskById(command.taskId);
    if (nodeRun === undefined || nodeRun.priorRunId === null || task === undefined || task.currentRunId !== command.runId || this.#nodeExecution(command.runId) !== undefined) return false;
    const events = this.#db.prepare("SELECT COUNT(*) AS count FROM router_node_events WHERE run_id=?").get(command.runId) as { count?: unknown } | undefined;
    if (Number(events?.count) !== 0) return false;
    const moved = this.#db.prepare("UPDATE router_tasks SET current_run_id=? WHERE task_id=? AND current_run_id=?").run(claimed.currentRunId, command.taskId, command.runId);
    if (moved.changes !== 1) fail("axis.router.recovery_required");
    return true;
  }

  /** The one path from a Node answer to durable state, whichever call obtained it. */
  async #settleNodeResponse(row: NodeCommandRow, expectation: NodeReceiptExpectation, prior: NodePriorRoute | undefined, response: RouterNodePortResult, expectedStatus: 200 | 202, claimed?: NodeClaimedPointer): Promise<RouterCommandResult> {
    if (response.status !== expectedStatus) {
      this.#retainNodeReceiptConflict(row, response.body);
      fail("axis.router.recovery_required");
    }
    let receipt: NodeReceipt;
    try {
      receipt = await validateNodeReceipt(response.body, { nodeRef: expectation.nodeRef, commandId: expectation.command.commandId }, { authenticatedNode: response.authenticatedNode, expectation, ...(prior === undefined ? {} : { priorRoute: prior }) });
    } catch {
      this.#retainNodeReceiptConflict(row, response.body);
      fail("axis.router.recovery_required");
    }
    return this.#applyNodeReceipt(row, expectation, receipt, prior, claimed);
  }

  async #applyNodeReceipt(row: NodeCommandRow, expectation: NodeReceiptExpectation, receipt: NodeReceipt, prior: NodePriorRoute | undefined, claimed?: NodeClaimedPointer): Promise<RouterCommandResult> {
    try {
      const receiptDigest = prefixedDigest(receipt);
      if (receipt.status === "accepted") return await this.#acceptNodeReceipt(row, expectation, receipt, receiptDigest, prior);
      if (receipt.status === "rejected") {
        const result: RouterCommandResult = { commandId: row.commandId, status: "rejected", safeCode: workerRejectionSafeCodes[receipt.failure.code] ?? "axis.router.worker_rejected" };
        const applied = this.#transaction<RouterCommandResult | null>(() => {
          const current = this.#currentNodeCommand(row);
          const terminal = this.#nodeTerminalReceipt(current, "rejected", receiptDigest, receipt.reservation, result);
          if (terminal !== undefined) return terminal;
          if (current.reservation !== null && !same(current.reservation, receipt.reservation)) {
            this.#recordNodeConflict("receipt", row.taskId, row.runId, row.commandId, null, receiptDigest, "axis.node.binding_conflict");
            return null;
          }
          const changed = this.#db.prepare("UPDATE router_node_command_state SET state='rejected',reservation_json=?,receipt_digest=?,result_json=?,updated_at=? WHERE command_id=? AND state=? AND result_json IS NULL AND receipt_digest IS ? AND reservation_json IS ?")
            .run(canonical(receipt.reservation), receiptDigest, canonical(result), this.#instant(), row.commandId, current.state, current.receiptDigest, current.reservation === null ? null : canonical(current.reservation));
          if (changed.changes !== 1) fail("axis.router.recovery_required");
          // A refused new-Run continue gives the Task back its claimed pointer; the rejected row stays.
          this.#restoreClaimedPointerFromRefusedSuccessor(current, claimed);
          for (const event of this.#nodeBufferedEvents(row.runId)) this.#quarantineBufferedNodeEvent(event, row.taskId, "axis.node.binding_conflict");
          return result;
        });
        if (applied === null) fail("axis.router.recovery_required");
        return applied;
      }
      const reservation = receipt.status === "not_found" ? null : receipt.reservation;
      const applied = this.#transaction<boolean>(() => {
        const current = this.#currentNodeCommand(row);
        if (current.state === "accepted" || current.state === "rejected"
          || (current.reservation !== null && (reservation === null || !same(current.reservation, reservation)))) {
          this.#recordNodeConflict("receipt", row.taskId, row.runId, row.commandId, null, receiptDigest, "axis.node.binding_conflict");
          return false;
        }
        const retainedReservation = current.reservation ?? reservation;
        const changed = this.#db.prepare("UPDATE router_node_command_state SET state='receipt_pending',reservation_json=?,receipt_digest=?,updated_at=? WHERE command_id=? AND state=? AND result_json IS NULL AND receipt_digest IS ? AND reservation_json IS ?")
          .run(retainedReservation === null ? null : canonical(retainedReservation), receiptDigest, this.#instant(), row.commandId, current.state, current.receiptDigest, current.reservation === null ? null : canonical(current.reservation));
        if (changed.changes !== 1) fail("axis.router.recovery_required");
        return true;
      });
      if (!applied) fail("axis.router.recovery_required");
      fail("axis.router.recovery_required");
    } catch (error) {
      if (error instanceof RouterError) throw error;
      fail("axis.router.recovery_required");
    }
  }

  async #acceptNodeReceipt(row: NodeCommandRow, expectation: NodeReceiptExpectation, receipt: Extract<NodeReceipt, { readonly status: "accepted" }>, receiptDigest: string, prior: NodePriorRoute | undefined): Promise<RouterCommandResult> {
    // A steer and a cancel both act on the prior route's Run: neither mints a Run or
    // writes execution evidence, and both answer with the prior route's binding.
    const onPriorRun = expectation.command.command === "cancel" || expectation.command.command === "steer";
    const result: RouterCommandResult = { commandId: row.commandId, taskId: row.taskId, runId: row.runId, status: receipt.result.status };
    for (;;) {
      const buffered = this.#nodeBufferedEvents(row.runId);
      const validated: { row: NodeEventRow; event: RouterRunEvent; origin: WorkerExecutionIdentity | null }[] = [];
      for (const item of buffered) {
        try {
          if (item.eventDigest !== prefixedDigest(item.event) || (item.origin === null ? item.originDigest !== null : item.originDigest !== prefixedDigest(item.origin))) throw new Error();
          validated.push({ row: item, event: routerRuntimeEvent(await validateRouterWorkerLifecycleEvent(item.event as RouterRunLifecycleEnvelope)), origin: item.origin });
        } catch { fail("axis.router.recovery_required"); }
      }
      const applied = this.#transaction<RouterCommandResult | null | typeof retryNodeReceipt>(() => {
        const current = this.#currentNodeCommand(row);
        const reservation = { workerKey: receipt.evidence.workerKey, target: receipt.evidence.target };
        const terminal = this.#nodeTerminalReceipt(current, "accepted", receiptDigest, reservation, result);
        if (terminal !== undefined) {
          if (terminal !== null) {
            const retained = this.#nodeExecution(row.runId);
            if (retained === undefined || this.#run(row.runId) === undefined || !same(retained.reservation, reservation)
              || !same(retained.sessionBinding, receipt.evidence.sessionBinding)
              || (!onPriorRun && retained.receiptDigest !== receiptDigest)) fail("axis.router.recovery_required");
          }
          return terminal;
        }
        if (!same(this.#nodeBufferedEvents(row.runId), buffered)) return retryNodeReceipt;
        const existing = this.#nodeExecution(row.runId);
        if (!onPriorRun) {
          if ((current.reservation !== null && !same(current.reservation, reservation)) || (existing !== undefined && (!same(existing.reservation, reservation) || !same(existing.sessionBinding, receipt.evidence.sessionBinding) || existing.receiptDigest !== receiptDigest))) {
            this.#recordNodeConflict("receipt", row.taskId, row.runId, row.commandId, null, receiptDigest, "axis.node.binding_conflict");
            return null;
          }
          if (existing === undefined) {
            const nodeRun = this.#nodeRun(row.runId); if (nodeRun === undefined) fail("axis.router.recovery_required");
            this.#db.prepare("INSERT INTO router_node_run_execution(run_id,reservation_json,session_binding_json,receipt_digest,recorded_at) VALUES (?,?,?,?,?)")
              .run(row.runId, canonical(reservation), canonical(receipt.evidence.sessionBinding), receiptDigest, this.#instant());
            const completeStart: RunStartEnvelope = { ...nodeRun.start, identity: { ...nodeRun.start.identity, sessionBinding: receipt.evidence.sessionBinding } } as RunStartEnvelope;
            this.#db.prepare("INSERT INTO router_runs(run_id,task_id,prior_run_id,attempt_json,run_json,binding_json,start_json,state,last_sequence,terminal_message_id,waiting_for_input) VALUES (?,?,?,?,?,?,?,'pending',0,NULL,0)")
              .run(nodeRun.runId, nodeRun.taskId, nodeRun.priorRunId, canonical(nodeRun.attempt), canonical(nodeRun.run), canonical(receipt.evidence.sessionBinding), canonical(completeStart));
          }
        } else if (prior === undefined || !same(prior.sessionBinding, receipt.evidence.sessionBinding)) fail("axis.router.recovery_required");
        const changed = this.#db.prepare("UPDATE router_node_command_state SET state='accepted',reservation_json=?,receipt_digest=?,result_json=?,updated_at=? WHERE command_id=? AND state=? AND result_json IS NULL AND receipt_digest IS ? AND reservation_json IS ?")
          .run(canonical(reservation), receiptDigest, canonical(result), this.#instant(), row.commandId, current.state, current.receiptDigest, current.reservation === null ? null : canonical(current.reservation));
        if (changed.changes !== 1) fail("axis.router.recovery_required");
        const execution = this.#nodeExecution(row.runId);
        if (execution !== undefined) {
          const nodeRun = this.#nodeRun(row.runId), task = this.#taskById(row.taskId); if (nodeRun === undefined || task === undefined) fail("axis.router.recovery_required");
          for (const item of validated) {
            this.#classifyBufferedNodeEvent(item.row, item.event, task, nodeRun, execution, item.origin, false);
          }
        }
        return result;
      });
      if (applied === retryNodeReceipt) continue;
      if (applied === null) fail("axis.router.recovery_required");
      return applied;
    }
  }

  #task(input: RouterTaskRef): TaskRow | undefined {
    const task = this.#taskByScope(input);
    if (task !== undefined && task.scopeId !== (input.teamId)) fail("axis.router.authority_mismatch");
    return task;
  }
  /**
   * Null-scope/null-target rows can prove no target, but never authorize a recorded
   * one. Gates on `routing_scope_id`, the same authority `#task` uses. Linear-only,
   * since `readRunTarget` is Linear-shaped.
   */
  #taskForRunTargetEvidence(input: LinearRouterTaskRef): Pick<TaskRow, "taskId" | "teamId"> & { readonly target: RouterNodeTargetSnapshot | null } | undefined {
    const row = this.#db.prepare("SELECT task_id, routing_scope_id, target_snapshot_json FROM router_tasks WHERE organization_id = ? AND issue_id = ? AND agent_session_id = ?").get(input.organizationId, input.issueId, input.agentSessionId) as Row | undefined;
    if (row === undefined) return undefined;
    let target: RouterNodeTargetSnapshot | null = null;
    if (row.target_snapshot_json !== null && row.target_snapshot_json !== undefined) {
      try { target = targetSnapshot(parse(row.target_snapshot_json)); } catch { fail("axis.router.recovery_required"); }
    }
    const task = { taskId: String(row.task_id), teamId: row.routing_scope_id === null || row.routing_scope_id === undefined ? null : String(row.routing_scope_id), target };
    if (task !== undefined && task.teamId !== input.teamId && (task.teamId !== null || task.target !== null)) fail("axis.router.authority_mismatch");
    return task;
  }
  /**
   * Finds a Task by its neutral identity, one keyed read for every channel. An
   * unencodable identity resolves to no Task rather than throwing, because
   * `listTaskEvents` passes its caller's ref through unnormalized.
   */
  #taskByScope(input: RouterTaskRef): TaskRow | undefined {
    let identity; try { identity = encodeRouterTaskIdentity(linearTaskIdentity(input)); } catch { return undefined; }
    return this.#taskFromRow(this.#db.prepare("SELECT * FROM router_tasks WHERE source_system = ? AND tenant_id = ? AND source_ref_json = ?").get(identity.sourceSystem, identity.tenantId, identity.sourceRefJson) as Row | undefined);
  }
  #taskById(taskId: string): TaskRow | undefined { return this.#taskFromRow(this.#db.prepare("SELECT * FROM router_tasks WHERE task_id = ?").get(taskId) as Row | undefined); }
  #taskFromRow(row: Row | undefined): TaskRow | undefined {
    if (row === undefined) return undefined;
    const rawTarget = row.target_snapshot_json;
    let target: RouterNodeTargetSnapshot | null = null;
    if (rawTarget !== null && rawTarget !== undefined) {
      try { target = targetSnapshot(parse(rawTarget)); } catch { fail("axis.router.recovery_required"); }
    }
    return {
      taskId: String(row.task_id), attemptId: String(row.attempt_id), correlationId: String(row.correlation_id), operationId: String(row.operation_id), task: parse<Task>(row.task_json),
      teamId: row.linear_team_id === null || row.linear_team_id === undefined ? null : String(row.linear_team_id),
      scopeId: row.routing_scope_id === null || row.routing_scope_id === undefined ? null : String(row.routing_scope_id),
      target,
      currentRunId: row.current_run_id === null ? null : String(row.current_run_id),
    };
  }
  #run(runId: string): RunRow | undefined { const row = this.#db.prepare("SELECT * FROM router_runs WHERE run_id = ?").get(runId) as Row | undefined; return row === undefined ? undefined : { runId: String(row.run_id), taskId: String(row.task_id), attempt: parse<Attempt>(row.attempt_json), run: parse<Run>(row.run_json), binding: parse<SessionBinding>(row.binding_json), start: parse<RunStartEnvelope>(row.start_json), state: Number(row.waiting_for_input) === 1 ? "waiting_for_input" : String(row.state) as RunState, lastSequence: Number(row.last_sequence), terminalMessageId: row.terminal_message_id === null ? null : String(row.terminal_message_id) }; }
  #createNodeRun(task: TaskRow, priorRunId: string | null, causationMessageId: string): NodeRunRow {
    if (task.target?.contractVersion !== "1.0.0-node") fail("axis.router.recovery_required");
    const version = task.task.contractVersion, runId = this.#id(), sessionBindingId = this.#id();
    const attempt: Attempt = { contractVersion: version, attemptId: task.attemptId, taskId: task.taskId, correlationId: task.correlationId };
    const run: Run = { contractVersion: version, runId, taskId: task.taskId, attemptId: task.attemptId, sessionBindingId, correlationId: task.correlationId, state: "pending" };
    const pendingBinding = { contractVersion: version, sessionBindingId, taskId: task.taskId, attemptId: task.attemptId, runId, correlationId: task.correlationId } as NodePendingRunStartEnvelope["identity"]["sessionBinding"];
    const start: NodePendingRunStartEnvelope = { contractVersion: version, messageId: this.#id(), correlationId: task.correlationId, operationId: task.operationId, causationMessageId, occurredAt: this.#instant(), producer: "router", consumer: "worker", envelopeType: "run.start", identity: { contractVersion: version, task: task.task, attempt, run, sessionBinding: pendingBinding } };
    const createdAt = this.#instant();
    this.#db.prepare("INSERT INTO router_node_runs(run_id,task_id,prior_run_id,attempt_json,run_json,pending_binding_json,start_json,created_at) VALUES (?,?,?,?,?,?,?,?)")
      .run(runId, task.taskId, priorRunId, canonical(attempt), canonical(run), canonical(pendingBinding), canonical(start), createdAt);
    this.#db.prepare("UPDATE router_tasks SET current_run_id=? WHERE task_id=?").run(runId, task.taskId);
    return { runId, taskId: task.taskId, priorRunId, attempt, run, pendingBinding, start, createdAt };
  }
  #nodeRun(runId: string): NodeRunRow | undefined {
    const row = this.#db.prepare("SELECT * FROM router_node_runs WHERE run_id=?").get(runId) as Row | undefined;
    if (row === undefined) return undefined;
    return { runId: String(row.run_id), taskId: String(row.task_id), priorRunId: row.prior_run_id === null ? null : String(row.prior_run_id), attempt: parse(row.attempt_json), run: parse(row.run_json), pendingBinding: parse(row.pending_binding_json), start: parse(row.start_json), createdAt: String(row.created_at) };
  }
  /**
   * The Task's newest Node Run that a command still names or that executed. A Run
   * whose only command was refused and forgotten has neither, and is skipped.
   */
  // Excludes an orphan with no command, and a successor Run whose every command was
  // refused and that never executed. A first Run is never excluded.
  #latestNodeRun(taskId: string): NodeRunRow | undefined { const row = this.#db.prepare("SELECT r.run_id FROM router_node_runs r WHERE r.task_id=? AND (EXISTS (SELECT 1 FROM router_node_command_state c WHERE c.run_id=r.run_id) OR EXISTS (SELECT 1 FROM router_node_run_execution e WHERE e.run_id=r.run_id)) AND (r.prior_run_id IS NULL OR EXISTS (SELECT 1 FROM router_node_run_execution e WHERE e.run_id=r.run_id) OR EXISTS (SELECT 1 FROM router_node_command_state c WHERE c.run_id=r.run_id AND c.state<>'rejected')) ORDER BY r.rowid DESC LIMIT 1").get(taskId) as Row | undefined; return row === undefined ? undefined : this.#nodeRun(String(row.run_id)); }
  #nodeExecution(runId: string): NodeExecutionRow | undefined {
    const row = this.#db.prepare("SELECT * FROM router_node_run_execution WHERE run_id=?").get(runId) as Row | undefined;
    if (row === undefined) return undefined;
    try {
      return { reservation: validateNodeReservationEvidence(parse(row.reservation_json)), sessionBinding: validateNodePriorRoute(this.#nodePriorRoute(runId, parse(row.reservation_json), parse(row.session_binding_json))).sessionBinding, receiptDigest: String(row.receipt_digest), recordedAt: instant(String(row.recorded_at)) };
    } catch { fail("axis.router.recovery_required"); }
  }
  #nodePriorRoute(runId: string, reservationValue?: unknown, bindingValue?: unknown): NodePriorRoute {
    const nodeRun = this.#nodeRun(runId), task = nodeRun === undefined ? undefined : this.#taskById(nodeRun.taskId);
    if (nodeRun === undefined || task === undefined || task.target?.contractVersion !== "1.0.0-node") fail("axis.router.recovery_required");
    const evidenceRow = reservationValue === undefined || bindingValue === undefined
      ? this.#db.prepare("SELECT reservation_json,session_binding_json FROM router_node_run_execution WHERE run_id=?").get(runId) as Row | undefined
      : undefined;
    const reservation = validateNodeReservationEvidence(reservationValue ?? parse(evidenceRow?.reservation_json));
    const sessionBinding = (bindingValue ?? parse(evidenceRow?.session_binding_json)) as NodeSessionBinding;
    const source = runtimeSourceSessionIdentity(task.task.sourceRef);
    return validateNodePriorRoute({ nodeRef: task.target.nodeRef, requestedSelection: task.target.workerSelection, reservation, sessionBinding, sourceSystem: source.system, sourceSessionId: source.sessionId, sourceRef: task.task.sourceRef, workspacePolicyRef: task.target.workspacePolicyRef });
  }
  #nodePriorRouteFor(command: NodeExpectedCommand): NodePriorRoute | undefined {
    if (command.command === "start") return undefined;
    // A cancel and a steer both act on the Run they target.
    return this.#nodePriorRoute(command.command === "continue" ? command.continuation.priorRunId : command.target.runId);
  }
  #nodeCoreRun(nodeRun: NodeRunRow, execution: NodeExecutionRow): RunRow {
    const core = this.#run(nodeRun.runId);
    if (core !== undefined) return core;
    const start = { ...nodeRun.start, identity: { ...nodeRun.start.identity, sessionBinding: execution.sessionBinding } } as RunStartEnvelope;
    return { runId: nodeRun.runId, taskId: nodeRun.taskId, attempt: nodeRun.attempt, run: nodeRun.run, binding: execution.sessionBinding as SessionBinding, start, state: "pending", lastSequence: 0, terminalMessageId: null };
  }
  #nextCursor(taskId: string): number { const row = this.#db.prepare("SELECT cursor FROM router_tasks WHERE task_id = ?").get(taskId) as Row | undefined; if (row === undefined) fail("axis.router.not_found"); const cursor = Number(row.cursor) + 1; this.#db.prepare("UPDATE router_tasks SET cursor = ? WHERE task_id = ?").run(cursor, taskId); return cursor; }
  #nodeCommandRow(commandId: string): NodeCommandRow | undefined {
    const row = this.#db.prepare("SELECT * FROM router_node_command_state WHERE command_id=?").get(commandId) as Row | undefined;
    if (row === undefined) return undefined;
    try {
      const intent = parse<Readonly<Record<string, unknown>>>(row.intent_json);
      const result = row.result_json === null ? null : parse<RouterCommandResult>(row.result_json);
      const reservation = row.reservation_json === null ? null : validateNodeReservationEvidence(parse(row.reservation_json));
      const state = row.state as NodeCommandState;
      const terminal = state === "accepted" || state === "rejected";
      if (!["prepared", "dispatching", "receipt_pending", "accepted", "rejected"].includes(state)
        || canonical(intent) !== row.intent_json || prefixedDigest(intent) !== row.intent_digest
        || typeof row.command_id !== "string" || !providerNeutralIdPattern.test(row.command_id)
        || typeof row.node_command_id !== "string" || !providerNeutralIdPattern.test(row.node_command_id)
        || typeof row.task_id !== "string" || !providerNeutralIdPattern.test(row.task_id)
        || typeof row.run_id !== "string" || !providerNeutralIdPattern.test(row.run_id)
        || typeof row.created_at !== "string" || instant(row.created_at) !== row.created_at
        || typeof row.updated_at !== "string" || instant(row.updated_at) !== row.updated_at
        || (row.receipt_digest !== null && (typeof row.receipt_digest !== "string" || !/^sha256:[a-f0-9]{64}$/u.test(row.receipt_digest)))
        || terminal !== (result !== null) || (result !== null && canonical(result) !== row.result_json)
        || (terminal && (reservation === null || row.receipt_digest === null))
        || (result !== null && (result.commandId !== row.command_id || (result.status !== "rejected" && (result.taskId !== row.task_id || result.runId !== row.run_id))))) fail("axis.router.recovery_required");
      return { commandId: String(row.command_id), nodeCommandId: String(row.node_command_id), kind: row.kind as CommandKind, taskId: String(row.task_id), runId: String(row.run_id), intent, intentDigest: String(row.intent_digest), state, reservation, receiptDigest: row.receipt_digest === null ? null : String(row.receipt_digest), result, createdAt: String(row.created_at), updatedAt: String(row.updated_at) };
    } catch (error) { if (error instanceof RouterError) throw error; fail("axis.router.recovery_required"); }
  }
  #currentNodeCommand(expected: NodeCommandRow): NodeCommandRow {
    const current = this.#nodeCommandRow(expected.commandId);
    if (current === undefined || current.nodeCommandId !== expected.nodeCommandId || current.kind !== expected.kind
      || current.taskId !== expected.taskId || current.runId !== expected.runId || current.intentDigest !== expected.intentDigest
      || !same(current.intent, expected.intent) || current.createdAt !== expected.createdAt) fail("axis.router.recovery_required");
    return current;
  }
  #nodeTerminalReceipt(
    current: NodeCommandRow,
    expectedState: Extract<NodeCommandState, "accepted" | "rejected">,
    receiptDigest: string,
    reservation: NodeReservationEvidence,
    result: RouterCommandResult,
  ): RouterCommandResult | null | undefined {
    if (current.state !== "accepted" && current.state !== "rejected") return undefined;
    if (current.state === expectedState && current.receiptDigest === receiptDigest
      && current.reservation !== null && same(current.reservation, reservation)
      && current.result !== null && same(current.result, result)) return current.result;
    this.#recordNodeConflict("receipt", current.taskId, current.runId, current.commandId, null, receiptDigest, "axis.node.binding_conflict");
    return null;
  }
  #retainNodeReceiptConflict(expected: NodeCommandRow, observed: unknown): void {
    try {
      const observedDigest = prefixedDigest(observed);
      this.#transaction(() => {
        const current = this.#currentNodeCommand(expected);
        if (current.state !== "accepted" && current.state !== "rejected") {
          const changed = this.#db.prepare("UPDATE router_node_command_state SET state='receipt_pending',updated_at=? WHERE command_id=? AND state=? AND result_json IS ? AND receipt_digest IS ? AND reservation_json IS ?")
            .run(this.#instant(), current.commandId, current.state, current.result === null ? null : canonical(current.result), current.receiptDigest, current.reservation === null ? null : canonical(current.reservation));
          if (changed.changes !== 1) fail("axis.router.recovery_required");
        }
        this.#recordNodeConflict("receipt", current.taskId, current.runId, current.commandId, null, observedDigest, "axis.node.binding_conflict");
      });
    } catch (error) {
      if (error instanceof RouterError) throw error;
      fail("axis.router.recovery_required");
    }
  }
  #nodeReservationForRun(runId: string): NodeReservationEvidence | null {
    const row = this.#db.prepare("SELECT reservation_json FROM router_node_command_state WHERE run_id=? AND reservation_json IS NOT NULL ORDER BY rowid DESC LIMIT 1").get(runId) as Row | undefined;
    if (row === undefined) return null;
    try { return validateNodeReservationEvidence(parse(row.reservation_json)); } catch { fail("axis.router.recovery_required"); }
  }
  #nodeRunHasRejectedCommand(runId: string): boolean {
    const row = this.#db.prepare("SELECT command_id FROM router_node_command_state WHERE run_id=? AND state='rejected' ORDER BY rowid LIMIT 1").get(runId) as Row | undefined;
    if (row === undefined) return false;
    const command = this.#nodeCommandRow(String(row.command_id));
    if (command === undefined || command.runId !== runId || command.state !== "rejected") fail("axis.router.recovery_required");
    return true;
  }
  #nodeEvent(messageId: string): NodeEventRow | undefined {
    const row = this.#db.prepare("SELECT * FROM router_node_events WHERE source_message_id=?").get(messageId) as Row | undefined;
    if (row === undefined) return undefined;
    return { sourceMessageId: String(row.source_message_id), runId: String(row.run_id), event: parse(row.event_json), eventDigest: String(row.event_digest), recordedAt: String(row.recorded_at), origin: row.origin_json === null ? null : parse(row.origin_json), originDigest: row.origin_digest === null ? null : String(row.origin_digest), state: row.state as NodeEventRow["state"], safeCode: row.safe_code as NodeEventRow["safeCode"], projectedCursor: row.projected_cursor === null ? null : Number(row.projected_cursor) };
  }
  #nodeBufferedEvents(runId: string): NodeEventRow[] { return (this.#db.prepare("SELECT source_message_id FROM router_node_events WHERE run_id=? AND state='buffered' ORDER BY rowid").all(runId) as Row[]).map(row => this.#nodeEvent(String(row.source_message_id))!); }
  #insertNodeEvent(nodeRun: NodeRunRow, event: RouterRunEvent, eventDigest: string, recordedAt: string, originJson: string | null, originDigest: string | null, state: NodeEventRow["state"], safeCode: NodeEventRow["safeCode"], projectedCursor: number | null): void {
    const envelope = runEventEnvelope(event);
    this.#db.prepare("INSERT INTO router_node_events(source_message_id,run_id,event_json,event_digest,recorded_at,origin_json,origin_digest,state,safe_code,projected_cursor) VALUES (?,?,?,?,?,?,?,?,?,?)")
      .run(envelope.messageId, nodeRun.runId, canonical(event), eventDigest, recordedAt, originJson, originDigest, state, safeCode, projectedCursor);
  }
  #recordNodeConflict(kind: "receipt" | "event", taskId: string, runId: string, commandId: string | null, messageId: string | null, observedDigest: string, safeCode: "axis.node.target_forbidden" | "axis.node.binding_conflict"): void {
    this.#db.prepare("INSERT INTO router_node_conflicts(conflict_id,kind,task_id,run_id,command_id,source_message_id,observed_digest,safe_code,recorded_at) VALUES (?,?,?,?,?,?,?,?,?)")
      .run(this.#id(), kind, taskId, runId, commandId, messageId, observedDigest, safeCode, this.#instant());
  }
  #quarantineBufferedNodeEvent(event: NodeEventRow, taskId: string, safeCode: "axis.node.target_forbidden" | "axis.node.binding_conflict"): void {
    const changed = this.#db.prepare("UPDATE router_node_events SET state='quarantined',safe_code=? WHERE source_message_id=? AND state='buffered'").run(safeCode, event.sourceMessageId);
    if (changed.changes !== 1) fail("axis.router.recovery_required");
    this.#recordNodeConflict("event", taskId, event.runId, null, event.sourceMessageId, event.eventDigest, safeCode);
  }
  #classifyBufferedNodeEvent(
    stored: NodeEventRow,
    event: RouterRunEvent,
    task: TaskRow,
    nodeRun: NodeRunRow,
    execution: NodeExecutionRow,
    origin: WorkerExecutionIdentity | null,
    replayed: boolean,
  ): RouterNodeEventIngestResult {
    if (origin === null || !same(origin, execution.reservation.target.identity)) {
      this.#quarantineBufferedNodeEvent(stored, task.taskId, "axis.node.target_forbidden");
      return { status: "quarantined", sourceMessageId: stored.sourceMessageId, safeCode: "axis.node.target_forbidden", replayed };
    }
    const core = this.#nodeCoreRun(nodeRun, execution);
    try { this.#assertProjectableNodeEvent(event, task, core, execution, origin); }
    catch {
      this.#quarantineBufferedNodeEvent(stored, task.taskId, "axis.node.binding_conflict");
      return { status: "quarantined", sourceMessageId: stored.sourceMessageId, safeCode: "axis.node.binding_conflict", replayed };
    }
    const receipt = this.#projectRunEvent(event, stored.recordedAt);
    const changed = this.#db.prepare("UPDATE router_node_events SET state='projected',projected_cursor=? WHERE source_message_id=? AND state='buffered'").run(receipt.cursor, stored.sourceMessageId);
    if (changed.changes !== 1) fail("axis.router.recovery_required");
    return { status: "projected", sourceMessageId: stored.sourceMessageId, receipt, replayed };
  }
  #nodeProjectedReceipt(event: NodeEventRow, nodeRun: NodeRunRow): Omit<RouterEventReceipt, "replayed"> {
    const row = this.#db.prepare("SELECT * FROM router_receipts WHERE source_message_id=?").get(event.sourceMessageId) as Row | undefined;
    if (row === undefined || event.state !== "projected" || event.projectedCursor === null
      || row.source_message_id !== event.sourceMessageId || row.task_id !== nodeRun.taskId || row.run_id !== event.runId
      || row.cursor !== event.projectedCursor || row.source_digest !== event.eventDigest.slice("sha256:".length)
      || row.source_json !== canonical(event.event) || row.recorded_at !== event.recordedAt
      || typeof row.run_sequence !== "number" || !Number.isSafeInteger(row.run_sequence) || row.run_sequence < 1
      || typeof row.cursor !== "number" || !Number.isSafeInteger(row.cursor) || row.cursor < 1
      || typeof row.relay_json !== "string") fail("axis.router.recovery_required");
    const sequencePosition = this.#db.prepare("SELECT COUNT(*) AS count FROM router_receipts WHERE run_id=? AND run_sequence<=?").get(event.runId, row.run_sequence) as { count?: number };
    if (sequencePosition.count !== row.run_sequence) fail("axis.router.recovery_required");
    let relayEnvelope: RouterTaskEvent;
    try {
      relayEnvelope = parse<RouterTaskEvent>(row.relay_json);
      const envelope = routerTaskEnvelope(relayEnvelope);
      if (canonical(relayEnvelope) !== row.relay_json || envelope.occurredAt !== row.recorded_at
        || canonical(this.#relay(event.event as RouterRunEvent, envelope.messageId, event.recordedAt)) !== row.relay_json) fail("axis.router.recovery_required");
    } catch (error) { if (error instanceof RouterError) throw error; fail("axis.router.recovery_required"); }
    return { taskId: nodeRun.taskId, runId: event.runId, cursor: event.projectedCursor, sourceMessageId: event.sourceMessageId, relayEnvelope };
  }
  #assertProjectableNodeEvent(event: RouterRunEvent, task: TaskRow, run: RunRow, execution: NodeExecutionRow, origin: WorkerExecutionIdentity): void {
    if (!same(origin, execution.reservation.target.identity) || run.binding.workerId !== origin.workerId || run.binding.executorId !== execution.reservation.target.sessionBindingExecutorId) fail("axis.router.protocol_violation");
    const envelope = runEventEnvelope(event);
    if (isWaitingRunEvent(event)) this.#assertWaitingIdentity(event, task, run);
    else { this.#assertIdentity(envelope as RuntimeRunEvent, task, run); this.#assertOrder(envelope as RuntimeRunEvent, run); }
  }
  #projectRunEvent(event: RouterRunEvent, recordedAt: string): RouterEventReceipt {
    const envelope = runEventEnvelope(event), sourceDigest = digest(event);
    const replay = this.#receipt(envelope.messageId); if (replay !== undefined) { if (replay.sourceDigest !== sourceDigest) fail("axis.router.intent_conflict"); return { ...replay.receipt, replayed: true }; }
    const run = this.#run(envelope.run.runId); if (run === undefined) fail("axis.router.not_found"); const task = this.#taskById(run.taskId); if (task === undefined) fail("axis.router.not_found");
    if (isWaitingRunEvent(event)) this.#assertWaitingIdentity(event, task, run);
    else { this.#assertIdentity(envelope as RuntimeRunEvent, task, run); this.#assertOrder(envelope as RuntimeRunEvent, run); }
    const cursor = this.#nextCursor(task.taskId), relay = this.#relay(event, this.#id(), recordedAt);
    this.#db.prepare("INSERT INTO router_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").run(envelope.messageId, sourceDigest, task.taskId, run.runId, run.lastSequence + 1, cursor, canonical(event), canonical(relay), recordedAt);
    const nextState: RunState = envelope.envelopeType === "run.waiting_for_input" ? "waiting_for_input" : envelope.envelopeType === "run.succeeded" ? "succeeded" : envelope.envelopeType === "run.failed" ? "failed" : "running";
    const terminal = nextState === "waiting_for_input" || nextState === "succeeded" || nextState === "failed" ? envelope.messageId : null;
    this.#db.prepare("UPDATE router_runs SET state=?,waiting_for_input=?,last_sequence=?,terminal_message_id=? WHERE run_id=?").run(nextState === "waiting_for_input" ? "failed" : nextState, nextState === "waiting_for_input" ? 1 : 0, run.lastSequence + 1, terminal, run.runId);
    if (terminal !== null) this.#db.prepare("UPDATE router_tasks SET current_run_id=NULL WHERE task_id=? AND current_run_id=?").run(task.taskId, run.runId);
    return { taskId: task.taskId, runId: run.runId, cursor, sourceMessageId: envelope.messageId, relayEnvelope: relay, replayed: false };
  }
  #receipt(messageId: string): { readonly sourceDigest: string; readonly receipt: Omit<RouterEventReceipt, "replayed"> } | undefined { const row = this.#db.prepare("SELECT * FROM router_receipts WHERE source_message_id = ?").get(messageId) as Row | undefined; return row === undefined ? undefined : { sourceDigest: String(row.source_digest), receipt: { taskId: String(row.task_id), runId: String(row.run_id), cursor: Number(row.cursor), sourceMessageId: messageId, relayEnvelope: parse<RouterTaskEvent>(row.relay_json) } }; }
  // Compares against the Task's own persisted `contractVersion`: each Task's events
  // are accepted at the version it was minted at, and any other version fails closed.
  #assertIdentity(event: RuntimeRunEvent, task: TaskRow, run: RunRow): void { if (event.contractVersion !== task.task.contractVersion || event.operationId !== task.operationId || event.correlationId !== task.correlationId || !same(event.task, task.task) || !same(event.attempt, run.attempt) || event.run.runId !== run.runId || event.run.taskId !== run.run.taskId || event.run.attemptId !== run.run.attemptId || event.run.sessionBindingId !== run.run.sessionBindingId || event.run.correlationId !== run.run.correlationId) fail("axis.router.protocol_violation"); }
  #assertOrder(event: RuntimeRunEvent, run: RunRow): void { if (run.state === "waiting_for_input" || run.state === "succeeded" || run.state === "failed") fail("axis.router.conflict"); if (run.lastSequence === 0 && event.envelopeType !== "run.accepted") fail("axis.router.conflict"); if (run.lastSequence > 0 && event.envelopeType === "run.accepted") fail("axis.router.conflict"); if ((event.envelopeType === "run.accepted" || event.envelopeType === "run.progressed") && event.run.state !== "running") fail("axis.router.protocol_violation"); if (event.envelopeType === "run.succeeded" && (event.run.state !== "succeeded" || event.run.resultRef !== event.resultRef)) fail("axis.router.protocol_violation"); if (event.envelopeType === "run.failed" && (event.run.state !== "failed" || !same(event.run.error, event.error))) fail("axis.router.protocol_violation"); }
  #assertWaitingIdentity(event: WorkerRunWaitingForInputEnvelope, task: TaskRow, run: RunRow): void {
    const elicitation = validateStructuredElicitation(event.elicitation);
    const occurredAt = typeof event.occurredAt === "string" && !Number.isNaN(Date.parse(event.occurredAt)) ? new Date(event.occurredAt).toISOString() : null;
    if (
      !plainData(event)
      || Object.keys(event).sort().join(",") !== "attempt,causationMessageId,consumer,contractVersion,correlationId,elicitation,envelopeType,messageId,occurredAt,operationId,producer,run,task"
      || !plainData(event.run)
      || Object.keys(event.run).sort().join(",") !== "attemptId,contractVersion,correlationId,runId,sessionBindingId,state,taskId"
      || run.state !== "running"
      || run.lastSequence < 1
      || event.contractVersion !== WORKER_WAITING_EXTENSION_VERSION
      || typeof event.messageId !== "string"
      || !providerNeutralIdPattern.test(event.messageId)
      || event.causationMessageId !== run.start.messageId
      || occurredAt !== event.occurredAt
      || event.producer !== "worker"
      || event.consumer !== "router"
      || event.operationId !== task.operationId
      || event.correlationId !== task.correlationId
      || elicitation === null
      || !same(elicitation, event.elicitation)
      || !same(event.task, task.task)
      || !same(event.attempt, run.attempt)
      || event.run.state !== "waiting_for_input"
      || event.run.contractVersion !== run.run.contractVersion
      || event.run.runId !== run.runId
      || event.run.taskId !== run.run.taskId
      || event.run.attemptId !== run.run.attemptId
      || event.run.sessionBindingId !== run.run.sessionBindingId
      || event.run.correlationId !== run.run.correlationId
    ) fail("axis.router.protocol_violation");
  }
  #relay(source: RouterRunEvent, messageId: string, occurredAt: string): RouterTaskEvent {
    if (isWaitingRunEvent(source)) {
      return { contractVersion: WORKER_WAITING_EXTENSION_VERSION, messageId, correlationId: source.correlationId, operationId: source.operationId, causationMessageId: source.messageId, occurredAt, producer: "router", consumer: "relay", envelopeType: "task.waiting_for_input", task: source.task, run: { runId: source.run.runId, state: "waiting_for_input" }, attemptId: source.attempt.attemptId, elicitation: source.elicitation };
    }
    const event = runEnvelope(source);
    const common = { contractVersion: event.contractVersion, messageId, correlationId: event.correlationId, operationId: event.operationId, causationMessageId: event.messageId, occurredAt, producer: "router" as const, consumer: "relay" as const, task: event.task };
    switch (event.envelopeType) {
      case "run.accepted": return { ...common, envelopeType: "task.accepted", run: { runId: event.run.runId, state: event.run.state } };
      case "run.progressed": return { ...common, envelopeType: "task.progressed", run: { runId: event.run.runId, state: event.run.state }, progress: event.progress };
      case "run.succeeded": {
        const envelope: TaskSucceededEnvelope = { ...common, envelopeType: "task.succeeded", run: { runId: event.run.runId, state: event.run.state, resultRef: event.resultRef }, resultRef: event.resultRef };
        // Response deliveries wrap the current Runtime success envelope with the
        // validated AgentActivity. A Run without a projected response returns
        // the plain success envelope.
        return "deliveryType" in source
          ? { contractVersion: agentActivityContractVersion, deliveryType: "task_agent_response", envelope: envelope as TaskAgentResponseDelivery["envelope"], agentActivity: source.agentActivity }
          : envelope;
      }
      case "run.failed": return { ...common, envelopeType: "task.failed", run: { runId: event.run.runId, state: event.run.state, error: event.error }, error: event.error };
    }
  }
  #createSchema(): void {
    try {
      this.#db.exec("BEGIN IMMEDIATE");
      try {
        this.#db.exec(Object.values(routerSchema).join(";"));
        this.#db.prepare("INSERT INTO router_node_schema(version,applied_at) VALUES (?,?)").run(ROUTER_SCHEMA_VERSION, this.#instant());
        this.#db.exec("COMMIT");
      } catch (error) { try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ } throw error; }
    } catch (error) { if (error instanceof RouterError && error.code === "axis.router.recovery_required") throw error; fail("axis.router.recovery_required"); }
  }
  #assertNodeRouting(): void {
    try {
      const integrity = this.#db.prepare("PRAGMA integrity_check").get() as { integrity_check?: string };
      if (integrity.integrity_check !== "ok") fail("axis.router.recovery_required");
      assertRouterSchema(this.#db);

      const nodeRuns = this.#db.prepare("SELECT * FROM router_node_runs ORDER BY run_id").all() as Row[];
      for (const row of nodeRuns) {
        const runId = String(row.run_id), taskId = String(row.task_id), priorRunId = row.prior_run_id === null ? null : String(row.prior_run_id);
        if (!providerNeutralIdPattern.test(runId) || !providerNeutralIdPattern.test(taskId) || (priorRunId !== null && !providerNeutralIdPattern.test(priorRunId)) || typeof row.created_at !== "string" || instant(row.created_at) !== row.created_at) fail("axis.router.recovery_required");
        const attempt = parse<Attempt>(row.attempt_json), run = parse<Run>(row.run_json), pending = parse<NodePendingRunStartEnvelope["identity"]["sessionBinding"]>(row.pending_binding_json), start = parse<NodePendingRunStartEnvelope>(row.start_json);
        if (canonical(attempt) !== row.attempt_json || canonical(run) !== row.run_json || canonical(pending) !== row.pending_binding_json || canonical(start) !== row.start_json || run.runId !== runId || run.taskId !== taskId || attempt.taskId !== taskId || pending.runId !== runId || pending.taskId !== taskId || start.identity.run.runId !== runId || start.identity.task.taskId !== taskId || !same(start.identity.attempt, attempt) || !same(start.identity.run, run) || !same(start.identity.sessionBinding, pending) || "priorRunId" in run) fail("axis.router.recovery_required");
        const task = this.#taskById(taskId); if (task === undefined || task.target?.contractVersion !== "1.0.0-node" || !same(task.task, start.identity.task)) fail("axis.router.recovery_required");
        if (priorRunId !== null) { const prior = this.#nodeRun(priorRunId); if (prior === undefined || prior.taskId !== taskId) fail("axis.router.recovery_required"); }
      }

      const commands = this.#db.prepare("SELECT command_id,task_id,run_id FROM router_node_command_state ORDER BY command_id").all() as Row[];
      for (const row of commands) {
        const commandId = String(row.command_id), command = this.#nodeCommandRow(commandId), run = this.#nodeRun(String(row.run_id));
        if (!providerNeutralIdPattern.test(commandId) || command === undefined || run === undefined || run.taskId !== String(row.task_id) || command.taskId !== run.taskId || command.runId !== run.runId) fail("axis.router.recovery_required");
      }

      const executions = this.#db.prepare("SELECT run_id,reservation_json,session_binding_json,receipt_digest,recorded_at FROM router_node_run_execution ORDER BY run_id").all() as Row[];
      for (const row of executions) {
        const runId = String(row.run_id), execution = this.#nodeExecution(runId), run = this.#nodeRun(runId);
        if (execution === undefined || run === undefined || canonical(execution.reservation) !== row.reservation_json || canonical(execution.sessionBinding) !== row.session_binding_json || !/^sha256:[a-f0-9]{64}$/u.test(execution.receiptDigest) || typeof row.recorded_at !== "string" || execution.recordedAt !== row.recorded_at) fail("axis.router.recovery_required");
      }

      const events = this.#db.prepare("SELECT * FROM router_node_events ORDER BY source_message_id").all() as Row[];
      for (const row of events) {
        const event = this.#nodeEvent(String(row.source_message_id));
        const nodeRun = event === undefined ? undefined : this.#nodeRun(event.runId);
        if (event === undefined || !providerNeutralIdPattern.test(event.sourceMessageId) || nodeRun === undefined || canonical(event.event) !== row.event_json || prefixedDigest(event.event) !== event.eventDigest || typeof event.recordedAt !== "string" || instant(event.recordedAt) !== event.recordedAt || (event.origin === null ? event.originDigest !== null || row.origin_json !== null : canonical(event.origin) !== row.origin_json || event.originDigest !== prefixedDigest(event.origin))) fail("axis.router.recovery_required");
        if ((event.state === "buffered" && (event.safeCode !== null || event.projectedCursor !== null)) || (event.state === "projected" && (event.safeCode !== null || event.projectedCursor === null)) || (event.state === "quarantined" && (event.safeCode === null || event.projectedCursor !== null))) fail("axis.router.recovery_required");
        if (event.state === "projected") this.#nodeProjectedReceipt(event, nodeRun);
      }

      const conflicts = this.#db.prepare("SELECT * FROM router_node_conflicts ORDER BY conflict_id").all() as Row[];
      for (const row of conflicts) {
        if (typeof row.conflict_id !== "string" || !providerNeutralIdPattern.test(row.conflict_id) || (row.kind !== "receipt" && row.kind !== "event") || typeof row.task_id !== "string" || typeof row.run_id !== "string" || this.#nodeRun(row.run_id) === undefined || typeof row.observed_digest !== "string" || !/^sha256:[a-f0-9]{64}$/u.test(row.observed_digest) || (row.safe_code !== "axis.node.target_forbidden" && row.safe_code !== "axis.node.binding_conflict") || typeof row.recorded_at !== "string" || instant(row.recorded_at) !== row.recorded_at) fail("axis.router.recovery_required");
      }
    } catch (error) { if (error instanceof RouterError && error.code === "axis.router.recovery_required") throw error; fail("axis.router.recovery_required"); }
  }
  #instant(): string { return this.#now().toISOString(); }
  #transaction<T>(action: () => T): T { this.#db.exec("BEGIN IMMEDIATE"); try { const value = action(); this.#db.exec("COMMIT"); return value; } catch (error) { this.#db.exec("ROLLBACK"); throw error; } }
}

/**
 * The Node route's Task freeze. Absent from the package root. Owns one immediate
 * transaction: an existing neutral Task is read without consulting current
 * bindings, and a missing Task resolves one `1.0.0-node` target and inserts only
 * the Task row.
 */
function freezeRouterNodeTaskTargetInTransaction(options: RouterNodeTaskTargetOptions, input: RouterNodeTaskTargetInput): RouterNodeTaskTargetResult {
  const database = options.database;
  let identity: RouterNeutralTaskIdentity;
  let columns: RouterTaskIdentityColumns;
  try {
    identity = linearTaskIdentity(input.taskRef);
    columns = encodeRouterTaskIdentity(identity);
  } catch (error) {
    throw new RouterError("axis.router.malformed", { cause: error });
  }
  if (identity.scopeId === null) fail("axis.router.malformed");
  const expectedScope = { system: identity.system, tenantId: identity.tenantId, scopeId: identity.scopeId } as const;
  const id = options.id ?? randomUUID;
  try {
    const row = database.prepare("SELECT task_id, attempt_id, correlation_id, operation_id, task_json, target_snapshot_json, routing_scope_id FROM router_tasks WHERE source_system = ? AND tenant_id = ? AND source_ref_json = ?")
      .get(columns.sourceSystem, columns.tenantId, columns.sourceRefJson) as Row | undefined;
    if (row !== undefined) {
      if (row.routing_scope_id !== identity.scopeId || row.target_snapshot_json === null || row.target_snapshot_json === undefined) fail("axis.router.recovery_required");
      let target: RouterNodeTargetSnapshot;
      let task: Task;
      try {
        target = targetSnapshot(parse(row.target_snapshot_json));
        task = parse<Task>(row.task_json);
      } catch (error) {
        if (error instanceof RouterError) throw error;
        throw new RouterError("axis.router.recovery_required", { cause: error });
      }
      const taskId = text(row.task_id), attemptId = text(row.attempt_id), correlationId = text(row.correlation_id), operationId = text(row.operation_id);
      if (taskId === undefined || attemptId === undefined || correlationId === undefined || operationId === undefined || !same(target.scope, expectedScope)) fail("axis.router.recovery_required");
      const expectedTask = reopenedRouterTask(input.taskRef, taskId, correlationId, task.contractVersion);
      if (expectedTask === null || !same(task, expectedTask)) fail("axis.router.recovery_required");
      return deeplyFrozen({ created: false, taskId, attemptId, correlationId, operationId, task, target });
    }

    let target: RouterNodeTargetSnapshot;
    try {
      target = validateRouterNodeTargetSnapshot(options.bindingResolver.resolveNodeTarget(linearBindingIdentity(input.taskRef), input.workerSelection));
    } catch (error) {
      if (error instanceof ChannelBindingError) throw new RouterError(channelBindingRouterError(error), { cause: error });
      throw new RouterError("axis.router.malformed", { cause: error });
    }
    if (!same(target.scope, expectedScope)) fail("axis.router.authority_mismatch");
    const { bindingDigest, ...targetFields } = target;
    if (routerNodeBindingDigest(targetFields) !== bindingDigest) fail("axis.router.recovery_required");
    const taskId = id(), attemptId = id(), correlationId = id(), operationId = id();
    const task = routerTask(input.taskRef, taskId, correlationId);
    database.prepare("INSERT INTO router_tasks(task_id, organization_id, issue_id, agent_session_id, linear_team_id, attempt_id, correlation_id, operation_id, task_json, target_snapshot_json, current_run_id, cursor, source_system, tenant_id, source_ref_json, routing_scope_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?, ?, ?, ?)")
      .run(
        taskId,
        input.taskRef.organizationId,
        input.taskRef.issueId,
        input.taskRef.agentSessionId,
        input.taskRef.teamId,
        attemptId, correlationId, operationId, JSON.stringify(task), JSON.stringify(target),
        columns.sourceSystem, columns.tenantId, columns.sourceRefJson, columns.routingScopeId,
      );
    return deeplyFrozen({ created: true, taskId, attemptId, correlationId, operationId, task, target });
  } catch (error) {
    if (error instanceof RouterError) throw error;
    throw new RouterError("axis.router.recovery_required", { cause: error });
  }
}

export function freezeRouterNodeTaskTarget(options: RouterNodeTaskTargetOptions, input: RouterNodeTaskTargetInput): RouterNodeTaskTargetResult {
  let transactionOpen = false;
  try {
    options.database.exec("BEGIN IMMEDIATE");
    transactionOpen = true;
    const result = freezeRouterNodeTaskTargetInTransaction(options, input);
    options.database.exec("COMMIT");
    transactionOpen = false;
    return result;
  } catch (error) {
    if (transactionOpen) try { options.database.exec("ROLLBACK"); } catch { /* preserve the original failure */ }
    if (error instanceof RouterError) throw error;
    throw new RouterError("axis.router.recovery_required", { cause: error });
  }
}

/**
 * The channel identity the neutral resolver gets for a Linear request. Plain frozen
 * values rather than `linearTenantRef`/`linearRoutingScope`, whose
 * `opaqueIdentifier` is narrower than the `Router#text` every stored Linear
 * identifier was admitted by.
 */
function linearBindingIdentity(input: LinearRouterTaskRef): ChannelBindingIdentity {
  return Object.freeze({ tenant: Object.freeze({ system: "linear" as const, tenantId: input.organizationId }), scope: Object.freeze({ system: "linear" as const, scopeId: input.teamId }) });
}

/** The one Task constructor. */
function routerTask(input: RouterTaskRef, taskId: string, correlationId: string): Task {
  return { contractVersion: runtimeContractVersion, taskId, correlationId, sourceRef: { system: "linear", organizationId: input.organizationId, issueId: input.issueId, agentSessionId: input.agentSessionId } };
}

/** Reconstructs one persisted Task at its own version. */
function reopenedRouterTask(input: RouterTaskRef, taskId: string, correlationId: string, contractVersion: Task["contractVersion"]): Task | null {
  return contractVersion === runtimeContractVersion ? routerTask(input, taskId, correlationId) : null;
}

/**
 * Translates the resolver's vocabulary into Router's. `resolver_unconfigured` is
 * `binding_unconfigured`, and `identity_mismatch` is `authority_mismatch`.
 * `malformed` stays `axis.router.malformed`, which is not a `selectionError`, so a
 * malformed bound profile throws rather than becoming a stored rejection.
 */
function channelBindingRouterError(error: unknown): RouterErrorCode {
  switch (error instanceof ChannelBindingError ? error.code : "incompatible") {
    case "unconfigured": case "resolver_unconfigured": return "axis.router.binding_unconfigured";
    case "disabled": return "axis.router.binding_disabled";
    case "reference_missing": case "binding_reference_missing": return "axis.router.binding_reference_missing";
    case "stale": return "axis.router.binding_stale";
    case "identity_mismatch": return "axis.router.authority_mismatch";
    case "endpoint_unknown": return "axis.router.endpoint_unknown";
    case "malformed": return "axis.router.malformed";
    default: return "axis.router.binding_incompatible";
  }
}

function commandInput(kind: CommandKind, input: RouterStartInput | RouterContinueInput | RouterCancelInput): RouterTaskRef & { readonly commandId: string } {
  const common = { commandId: text(input.commandId), organizationId: text(input.organizationId), issueId: text(input.issueId), teamId: text(input.teamId), agentSessionId: text(input.agentSessionId) };
  if (Object.values(common).some((value) => value === undefined) || (kind !== "cancel" && text((input as RouterStartInput).instruction) === undefined)) fail("axis.router.malformed");
  return common as LinearRouterTaskRef & { readonly commandId: string };
}
function readRunTargetInput(input: unknown): RouterReadRunTargetInput {
  try {
    if (input === null || typeof input !== "object" || Array.isArray(input) || Object.getPrototypeOf(input) !== Object.prototype || Object.getOwnPropertySymbols(input).length !== 0) fail("axis.router.malformed");
    const data = input as Record<string, unknown>, keys = Object.getOwnPropertyNames(data).sort(), expected = ["agentSessionId", "issueId", "organizationId", "runId", "teamId"];
    const descriptors = Object.getOwnPropertyDescriptors(data);
    if (keys.length !== expected.length || keys.some((key, index) => key !== expected[index]) || Object.values(descriptors).some((descriptor) => !descriptor.enumerable || descriptor.get !== undefined || descriptor.set !== undefined)) fail("axis.router.malformed");
    const value = Object.fromEntries(keys.map((key) => [key, descriptors[key]?.value]));
    const normalized = { organizationId: text(value.organizationId), issueId: text(value.issueId), teamId: text(value.teamId), agentSessionId: text(value.agentSessionId), runId: text(value.runId) };
    if (Object.values(normalized).some((entry) => entry === undefined)) fail("axis.router.malformed");
    return normalized as RouterReadRunTargetInput;
  } catch (error) { if (error instanceof RouterError) throw error; fail("axis.router.malformed"); }
}
function deeplyFrozen<T>(value: T): T { if (value !== null && typeof value === "object") { for (const child of Object.values(value as Record<string, unknown>)) deeplyFrozen(child); Object.freeze(value); } return value; }
function plainData(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype && Object.getOwnPropertySymbols(value).length === 0; }
const providerNeutralIdPattern = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
function identifier(value: unknown): string { const normalized = text(value); if (normalized === undefined || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(normalized)) fail("axis.router.malformed"); return normalized; }
function versioned(value: unknown): WorkerVersionedReference { const candidate = value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined; if (candidate === undefined || Object.keys(candidate).sort().join(",") !== "ref,version") fail("axis.router.malformed"); return { ref: identifier(candidate.ref), version: identifier(candidate.version) }; }
function targetSnapshot(value: unknown): RouterNodeTargetSnapshot {
  try {
    const target = validateRouterNodeTargetSnapshot(value);
    const { bindingDigest, ...fields } = target;
    if (routerNodeBindingDigest(fields) !== bindingDigest) fail("axis.router.recovery_required");
    return target;
  } catch { fail("axis.router.recovery_required"); }
}

function selectionError(code: RouterErrorCode): boolean { return ["axis.router.binding_unconfigured", "axis.router.binding_disabled", "axis.router.binding_reference_missing", "axis.router.binding_stale", "axis.router.binding_incompatible", "axis.router.endpoint_unknown", "axis.router.authority_mismatch"].includes(code); }
/**
 * Keeps the task-identity store's vocabulary distinguishable: an unpersistable
 * caller identity is a bad argument; an undecodable row or unrecognized schema is
 * state Router cannot resume from.
 */
function taskIdentityError(error: unknown): RouterErrorCode {
  return error instanceof RouterTaskIdentityError && error.code === "malformed" ? "axis.router.malformed" : "axis.router.recovery_required";
}
function instant(value: string): string { if (Number.isNaN(Date.parse(value))) fail("axis.router.malformed"); return new Date(value).toISOString(); }
