import { isDeepStrictEqual } from "node:util";
import type {
  MethodRef,
  RunAcceptedEnvelope,
  RunFailedEnvelope,
  RunProgressedEnvelope,
  Run,
  RunStartEnvelope,
  RunSucceededEnvelope,
  Task,
  Attempt,
} from "./runtime-boundary.js";
import { validateRunAgentResponseDelivery, type RunAgentResponseDelivery } from "@axisosme/axis-contracts/agent-activity";
import { runtimeBoundaryVersion, validateRuntimeBoundaryMessage } from "./runtime-boundary.js";
import type { StructuredElicitation } from "./elicitation.js";
import { validateStructuredElicitation } from "./elicitation.js";
import type { RouterErrorCode } from "../router/router.js";

type WrappedRunAgentResponseDelivery = RunAgentResponseDelivery & {
  readonly envelopeType?: never;
  readonly messageId?: never;
  readonly run?: never;
  readonly task?: never;
  readonly attempt?: never;
  readonly progress?: never;
  readonly resultRef?: never;
  readonly error?: never;
};

export type RouterRunLifecycleEnvelope =
  | RunAcceptedEnvelope
  | RunProgressedEnvelope
  | RunSucceededEnvelope
  | WrappedRunAgentResponseDelivery
  | WorkerRunWaitingForInputEnvelope

  | RunFailedEnvelope;

export interface WorkerRunWaitingForInputEnvelope {
  readonly contractVersion: typeof WORKER_WAITING_EXTENSION_VERSION;
  readonly messageId: string;
  readonly correlationId: string;
  readonly operationId: string;
  readonly causationMessageId: string;
  readonly occurredAt: string;
  readonly producer: "worker";
  readonly consumer: "router";
  readonly envelopeType: "run.waiting_for_input";
  readonly task: Task;
  readonly attempt: Attempt;
  readonly run: {
    readonly contractVersion: Run["contractVersion"];
    readonly runId: string;
    readonly taskId: string;
    readonly attemptId: string;
    readonly sessionBindingId: string;
    readonly correlationId: string;
    readonly state: "waiting_for_input";
  };
  readonly elicitation: StructuredElicitation;
}

export interface RouterRunEventSink {
  emit(event: RouterRunLifecycleEnvelope): Promise<void>;
}

export interface WorkerRunConfiguration {
  readonly bearerToken: string;
  readonly workerId: string;
  readonly runtimeKind: string;
  readonly executorId: string;
  readonly nodeId: string;
  /** Closed operator identity used by Router target verification; never a native binding. */
  readonly runtimeProfile?: WorkerVersionedReference;
  readonly methodRef: MethodRef;
  readonly workspaces: readonly WorkerWorkspaceConfiguration[];
}

/** A closed versioned reference. Values are opaque IDs, never URLs, paths, or credentials. */
export interface WorkerVersionedReference { readonly ref: string; readonly version: string; }
export interface WorkerWorkspaceCatalogEntry {
  readonly workspacePolicyRef: WorkerVersionedReference;
  readonly workspaceRoot: string;
}

export interface LinearWorkerWorkspaceConfiguration {
  readonly organizationId: string;
  readonly workspaceRoot: string;
  readonly workspaceCatalog?: readonly WorkerWorkspaceCatalogEntry[];
}

export type WorkerWorkspaceConfiguration = LinearWorkerWorkspaceConfiguration;

/**
 * The Worker process's configured execution identity. Router compares every
 * field before treating an endpoint response as proof for a frozen Task target.
 */
export interface WorkerExecutionIdentity {
  readonly nodeId: string;
  readonly workerId: string;
  readonly executorKind: string;
  readonly runtimeProfile: WorkerVersionedReference;
}

/**
 * Every Router, Node and Worker speaks exactly this version; any other command is
 * `axis.worker.malformed` (400), definite. Start and continue carry `executionTarget`.
 */
export const WORKER_COMMAND_VERSION = "1.0.0" as const;
/**
 * The version of Linear's bare `run.waiting_for_input` Axis extension envelope (Worker -> Node ->
 * Router, and Router's `task.waiting_for_input` projection to Relay). It versions independently of
 * `WORKER_COMMAND_VERSION`.
 */
export const WORKER_WAITING_EXTENSION_VERSION = "1.0.0" as const;
export const WORKER_OPAQUE_IDENTIFIER_MAX_LENGTH = 128;
const WORKER_OPAQUE_IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;

/** Shared package-private grammar for identifiers carried by Worker commands. */
export function isWorkerOpaqueIdentifier(value: unknown): value is string {
  return typeof value === "string" && value.length <= WORKER_OPAQUE_IDENTIFIER_MAX_LENGTH && WORKER_OPAQUE_IDENTIFIER_PATTERN.test(value);
}

export const workerFailureTuples: Readonly<Record<string, { readonly statuses: readonly number[]; readonly message: string; readonly disposition: "definite" | "ambiguous" }>> = Object.freeze({
  "axis.worker.malformed": { statuses: [400], message: "The Worker run request is malformed.", disposition: "definite" },
  "axis.worker.unauthorized": { statuses: [401], message: "Worker authorization is required.", disposition: "definite" },
  "axis.worker.policy_forbidden": { statuses: [403], message: "The Worker run is not permitted by policy.", disposition: "definite" },
  "axis.worker.identity_mismatch": { statuses: [409], message: "The Worker's configured identity does not match the command's execution target.", disposition: "definite" },
  "axis.worker.method_not_allowed": { statuses: [405], message: "The HTTP method is not allowed.", disposition: "definite" },
  "axis.worker.body_too_large": { statuses: [413], message: "The Worker run body exceeds the accepted limit.", disposition: "definite" },
  "axis.worker.unsupported_media": { statuses: [415], message: "The Worker run media type is unsupported.", disposition: "definite" },
  "axis.worker.adapter_invalid": { statuses: [502], message: "The executor adapter returned an invalid acceptance.", disposition: "ambiguous" },
  "axis.worker.launch_failed": { statuses: [502, 503], message: "The executor launch failed before execution began.", disposition: "ambiguous" },
  "axis.worker.launch_ambiguous": { statuses: [503], message: "The executor launch outcome is ambiguous.", disposition: "ambiguous" },
  "axis.worker.intent_conflict": { statuses: [409], message: "The Worker run ID is already bound to different start intent.", disposition: "definite" },
  "axis.worker.recovery_required": { statuses: [503], message: "The Worker retained an observational run that cannot be relaunched.", disposition: "ambiguous" },
  "axis.worker.operation_unsupported": { statuses: [503], message: "The executor operation is not supported.", disposition: "definite" },
  "axis.worker.runtime_missing": { statuses: [503], message: "The configured executor runtime is unavailable.", disposition: "definite" },
  "axis.worker.runtime_incompatible": { statuses: [503], message: "The configured executor runtime is incompatible.", disposition: "definite" },
  "axis.worker.authentication_required": { statuses: [503], message: "The configured executor runtime requires authentication.", disposition: "definite" },
  "axis.worker.probe_timeout": { statuses: [503], message: "The executor preflight probe did not respond in time.", disposition: "definite" },
  "axis.worker.capability_unavailable": { statuses: [503], message: "The configured executor capability is unavailable.", disposition: "definite" },
  "axis.worker.workspace_unavailable": { statuses: [503], message: "No trusted Worker workspace is available for this run.", disposition: "definite" },
  // Definite: Worker raises it only from continuation binding resolution in `#start`, before
  // `reserveRun`, so no Run was reserved and nothing ran.
  "axis.worker.continuation_unresumable": { statuses: [503], message: "The prior native session for this continuation cannot be resumed.", disposition: "definite" },
});

/**
 * The identity the dispatching Node reserved this command for: its inventory entry's execution
 * identity and the executor id bound into the start's session binding. The Worker compares it with
 * its own configuration before reserving the Run.
 */
export interface WorkerExecutionTarget {
  readonly identity: WorkerExecutionIdentity;
  readonly sessionBindingExecutorId: string;
}

export interface WorkerCommandStart {
  readonly contractVersion: typeof WORKER_COMMAND_VERSION;
  readonly commandId: string;
  readonly command: "start";
  readonly start: RunStartEnvelope;
  readonly instruction: string;
  readonly workspacePolicyRef: WorkerVersionedReference | null;
  readonly executionTarget: WorkerExecutionTarget;
}

export interface WorkerCommandContinue {
  readonly contractVersion: typeof WORKER_COMMAND_VERSION;
  readonly commandId: string;
  readonly command: "continue";
  readonly start: RunStartEnvelope;
  readonly instruction: string;
  readonly continuation: { readonly priorRunId: string };
  readonly workspacePolicyRef: WorkerVersionedReference | null;
  readonly executionTarget: WorkerExecutionTarget;
}

export interface WorkerCommandCancel {
  readonly contractVersion: typeof WORKER_COMMAND_VERSION;
  readonly commandId: string;
  readonly command: "cancel";
  readonly target: { readonly runId: string };
}

/**
 * Hand a further instruction to a Run's running turn without interrupting it. A Worker whose
 * executor cannot take it (no `steer` capability, or `executor.steer_too_late`) answers
 * `interrupt_requested`, so the caller falls back to interrupt-then-continue. Like `cancel`, it names
 * an existing Run by `target.runId`, carries no `start` or `executionTarget`, and mints no Run.
 */
export interface WorkerCommandSteer {
  readonly contractVersion: typeof WORKER_COMMAND_VERSION;
  readonly commandId: string;
  readonly command: "steer";
  readonly target: { readonly runId: string };
  readonly instruction: string;
}

export type WorkerCommand = WorkerCommandStart | WorkerCommandContinue | WorkerCommandCancel | WorkerCommandSteer;
/**
 * What `validateRouterWorkerResponse` checks a response against. The instruction is absent because
 * nothing about a response depends on it.
 */
export type WorkerResponseExpectation =
  | Pick<WorkerCommandStart, "contractVersion" | "commandId" | "command" | "start">
  | Pick<WorkerCommandContinue, "contractVersion" | "commandId" | "command" | "start">
  | Pick<WorkerCommandCancel, "contractVersion" | "commandId" | "command" | "target">
  | Pick<WorkerCommandSteer, "contractVersion" | "commandId" | "command" | "target">;
export type WorkerCommandContractVersion = typeof WORKER_COMMAND_VERSION;
/** `steered` answers only a `steer`; `interrupt_requested` answers a `cancel`, or a `steer` that fell back. */
export type WorkerCommandResponse =
  | { readonly contractVersion: WorkerCommandContractVersion; readonly commandId: string; readonly status: "accepted"; readonly identity: WorkerExecutionIdentity; readonly runAccepted: RunAcceptedEnvelope }
  | { readonly contractVersion: WorkerCommandContractVersion; readonly commandId: string; readonly status: "interrupt_requested"; readonly runId: string }
  | { readonly contractVersion: WorkerCommandContractVersion; readonly commandId: string; readonly status: "steered"; readonly runId: string };

/** Worker-ingress names for the shared unions. */
export type WorkerIngressCommand = WorkerCommand;
export type WorkerIngressResponse = WorkerCommandResponse;
export type WorkerSteerResponseExpectation = Extract<WorkerResponseExpectation, { readonly command: "steer" }>;
export type WorkerSteeredResponse = Extract<WorkerCommandResponse, { readonly status: "steered" }>;
/** What a `steer` may be answered with: `steered`, or the `interrupt_requested` fallback. */
export type WorkerSteerResponse = Extract<WorkerCommandResponse, { readonly status: "steered" | "interrupt_requested" }>;

export const ROUTER_WORKER_TRANSPORT_BODY_LIMIT_BYTES = 64 * 1024;

export class RouterHttpTransportError extends Error {
  constructor() {
    super("The private Router transport did not accept the request.");
    this.name = "RouterHttpTransportError";
  }
}

/**
 * The Router's two wire codes (HTTP 409) for a Run event it refuses:
 *
 * - `axis.router.conflict`: the Run is already terminal or the event is out of order.
 * - `axis.router.intent_conflict`: the same `messageId` was accepted with a different digest; a
 *   byte-identical redelivery is a 200 replay.
 *
 * `../router/http.js` also maps `axis.router.not_found` onto `axis.router.conflict`, which a Router
 * against a restored or not-yet-mounted database can produce for Runs it did dispatch. So this type
 * reports only that the Router refused the event; `../worker/command.js` keeps retrying every
 * persisted Run event. Every other non-2xx, including 502 `axis.router.protocol_violation`, stays a
 * `RouterHttpTransportError`.
 */
const ROUTER_EVENT_REFUSAL_CODES = Object.freeze(["axis.router.conflict", "axis.router.intent_conflict"] as const satisfies readonly RouterErrorCode[]);
export type RouterEventRefusalCode = typeof ROUTER_EVENT_REFUSAL_CODES[number];

/** A Run event the Router refused permanently. Distinct from `RouterHttpTransportError` so a caller can decline to retry it. */
export class RouterEventRefusedError extends Error {
  readonly code: RouterEventRefusalCode;
  constructor(code: RouterEventRefusalCode) {
    super("The Router refused this Run event permanently.");
    this.name = "RouterEventRefusedError";
    this.code = code;
  }
}

/**
 * Returns a code only when the response proves a refusal, and `null` otherwise: reading a transient
 * failure as permanent would abandon a deliverable event, while the reverse costs only a retry.
 */
export async function readRouterEventRefusal(response: Response): Promise<RouterEventRefusalCode | null> {
  if (response.status !== 409) return null;
  let body: unknown;
  try { body = await readRouterWorkerTransportResponse(response); } catch { return null; }
  const code = record(record(body)?.error)?.code;
  return ROUTER_EVENT_REFUSAL_CODES.find((candidate) => candidate === code) ?? null;
}

class StrictTransportJson {
  #index = 0;
  readonly #text: string;
  constructor(text: string) { this.#text = text; }
  parse(): unknown { this.#space(); this.#value(); this.#space(); if (this.#index !== this.#text.length) throw new RouterHttpTransportError(); return JSON.parse(this.#text) as unknown; }
  #value(): void { const value = this.#text[this.#index]; if (value === "{") return this.#object(); if (value === "[") return this.#array(); if (value === "\"") return void this.#string(); if (this.#text.startsWith("true", this.#index)) return void (this.#index += 4); if (this.#text.startsWith("false", this.#index)) return void (this.#index += 5); if (this.#text.startsWith("null", this.#index)) return void (this.#index += 4); this.#number(); }
  #object(): void { this.#index += 1; this.#space(); const keys = new Set<string>(); if (this.#text[this.#index] === "}") return void (this.#index += 1); for (;;) { this.#space(); if (this.#text[this.#index] !== "\"") throw new RouterHttpTransportError(); const key = this.#string(); if (keys.has(key)) throw new RouterHttpTransportError(); keys.add(key); this.#space(); if (this.#text[this.#index] !== ":") throw new RouterHttpTransportError(); this.#index += 1; this.#space(); this.#value(); this.#space(); if (this.#text[this.#index] === "}") return void (this.#index += 1); if (this.#text[this.#index] !== ",") throw new RouterHttpTransportError(); this.#index += 1; } }
  #array(): void { this.#index += 1; this.#space(); if (this.#text[this.#index] === "]") return void (this.#index += 1); for (;;) { this.#value(); this.#space(); if (this.#text[this.#index] === "]") return void (this.#index += 1); if (this.#text[this.#index] !== ",") throw new RouterHttpTransportError(); this.#index += 1; this.#space(); } }
  #string(): string { const start = this.#index; this.#index += 1; while (this.#index < this.#text.length) { const value = this.#text[this.#index] ?? ""; if (value === "\"") { this.#index += 1; return JSON.parse(this.#text.slice(start, this.#index)) as string; } if (value === "\\") { this.#index += 1; const escape = this.#text[this.#index]; if (escape === "u") { if (!/^[0-9a-f]{4}$/iu.test(this.#text.slice(this.#index + 1, this.#index + 5))) throw new RouterHttpTransportError(); this.#index += 5; continue; } if (escape === undefined || !"\"\\/bfnrt".includes(escape)) throw new RouterHttpTransportError(); this.#index += 1; continue; } if (value < " ") throw new RouterHttpTransportError(); this.#index += 1; } throw new RouterHttpTransportError(); }
  #number(): void { const match = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/u.exec(this.#text.slice(this.#index)); if (match?.[0] === undefined) throw new RouterHttpTransportError(); this.#index += match[0].length; }
  #space(): void { while (/\s/u.test(this.#text[this.#index] ?? "")) this.#index += 1; }
}

/** Strict UTF-8 JSON syntax seam; callers own their byte and media limits. */
export function parseStrictRouterWorkerJson(buffer: Uint8Array): unknown {
  try { return new StrictTransportJson(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(buffer)).parse(); }
  catch { throw new RouterHttpTransportError(); }
}

export async function readRouterWorkerTransportResponse(response: Response): Promise<unknown> {
  const contentType = (response.headers.get("content-type") ?? "").trim().toLowerCase();
  if (!/^application\/json(?:\s*;\s*charset=utf-8)?$/u.test(contentType) || response.headers.has("content-encoding")) throw new RouterHttpTransportError();
  const length = Number(response.headers.get("content-length") ?? "0");
  if (!Number.isSafeInteger(length) || length < 0 || length > ROUTER_WORKER_TRANSPORT_BODY_LIMIT_BYTES) throw new RouterHttpTransportError();
  const buffer = Buffer.from(await response.arrayBuffer());
  if (buffer.length > ROUTER_WORKER_TRANSPORT_BODY_LIMIT_BYTES) throw new RouterHttpTransportError();
  return parseStrictRouterWorkerJson(buffer);
}

export function routerWorkerEndpoint(base: URL | string, path: string): URL {
  return new URL(path, base instanceof URL ? base : new URL(base));
}

function record(value: unknown): Readonly<Record<string, unknown>> | undefined { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Readonly<Record<string, unknown>> : undefined; }
function exact(value: Readonly<Record<string, unknown>>, keys: readonly string[]): boolean { const actual = Object.keys(value).sort(), expected = [...keys].sort(); return actual.length === expected.length && actual.every((key, index) => key === expected[index]); }

/** Shared closed response validation for Router's in-process and HTTP Worker ports. */
export function validWorkerExecutionIdentity(value: unknown): value is WorkerExecutionIdentity {
  const identity = record(value), runtimeProfile = record(identity?.runtimeProfile);
  return identity !== undefined && runtimeProfile !== undefined
    && exact(identity, ["nodeId", "workerId", "executorKind", "runtimeProfile"])
    && exact(runtimeProfile, ["ref", "version"])
    && [identity.nodeId, identity.workerId, identity.executorKind, runtimeProfile.ref, runtimeProfile.version].every((item) => typeof item === "string" && item.trim().length > 0);
}

export async function validateRouterWorkerResponse(value: unknown, command: WorkerSteerResponseExpectation): Promise<WorkerSteerResponse>;
export async function validateRouterWorkerResponse(value: unknown, command: WorkerResponseExpectation): Promise<WorkerCommandResponse>;
export async function validateRouterWorkerResponse(value: unknown, command: WorkerResponseExpectation): Promise<WorkerCommandResponse> {
  const candidate = record(value);
  if (candidate === undefined || candidate.contractVersion !== command.contractVersion || candidate.commandId !== command.commandId || typeof candidate.status !== "string") throw new RouterHttpTransportError();
  if (candidate.status === "accepted" && (command.command === "start" || command.command === "continue") && exact(candidate, ["contractVersion", "commandId", "status", "identity", "runAccepted"]) && validWorkerExecutionIdentity(candidate.identity) && record(candidate.runAccepted) !== undefined) {
    const validated = await validateRuntimeBoundaryMessage(candidate.runAccepted);
    if (validated.valid && validated.value?.envelopeType === "run.accepted") {
      const accepted = validated.value, start = command.start;
      if (accepted.contractVersion === start.contractVersion
        && accepted.correlationId === start.correlationId
        && accepted.operationId === start.operationId
        && accepted.causationMessageId === start.messageId
        && isDeepStrictEqual(accepted.task, start.identity.task)
        && isDeepStrictEqual(accepted.attempt, start.identity.attempt)
        && isDeepStrictEqual(accepted.run, { ...start.identity.run, state: "running" })) return candidate as WorkerCommandResponse;
    }
  }
  // A `steer` may be answered `interrupt_requested` too: that is its fallback, under the same rule as `cancel`.
  if (candidate.status === "interrupt_requested" && (command.command === "cancel" || command.command === "steer") && candidate.runId === command.target.runId && exact(candidate, ["contractVersion", "commandId", "status", "runId"])) return candidate as WorkerCommandResponse;
  if (candidate.status === "steered" && command.command === "steer" && candidate.runId === command.target.runId && exact(candidate, ["contractVersion", "commandId", "status", "runId"])) return candidate as WorkerCommandResponse;
  throw new RouterHttpTransportError();
}

/** Shared lifecycle validation preserves Router's direct-event protocol boundary. */
export async function validateRouterWorkerLifecycleEvent(event: unknown): Promise<RouterRunLifecycleEnvelope> {
  const candidate = record(event); if (candidate === undefined) throw new RouterHttpTransportError();
  if (candidate.envelopeType === "run.waiting_for_input") {
    if (!exact(candidate, ["contractVersion", "messageId", "correlationId", "operationId", "causationMessageId", "occurredAt", "producer", "consumer", "envelopeType", "task", "attempt", "run", "elicitation"]) || candidate.contractVersion !== WORKER_WAITING_EXTENSION_VERSION || candidate.producer !== "worker" || candidate.consumer !== "router" || record(candidate.task) === undefined || record(candidate.attempt) === undefined || record(candidate.run) === undefined || validateStructuredElicitation(candidate.elicitation) === null) throw new RouterHttpTransportError();
    const run = record(candidate.run);
    if (run === undefined || !exact(run, ["contractVersion", "runId", "taskId", "attemptId", "sessionBindingId", "correlationId", "state"]) || run.state !== "waiting_for_input") throw new RouterHttpTransportError();
    // Waiting is an Axis extension. Project only its two discriminants onto a Runtime accepted envelope
    // to validate all shared fields and relations. The projected `contractVersion` comes from the nested
    // `task`, since each schema pins it at every level; `runtimeBoundaryVersion` rejects an unknown one.
    const projectedVersion = runtimeBoundaryVersion(record(candidate.task)?.contractVersion);
    if (projectedVersion === null) throw new RouterHttpTransportError();
    const { elicitation: _elicitation, ...runtime } = candidate;
    const validated = await validateRuntimeBoundaryMessage({ ...runtime, contractVersion: projectedVersion, envelopeType: "run.accepted", run: { ...run, state: "running" } });
    if (!validated.valid || validated.value?.envelopeType !== "run.accepted") throw new RouterHttpTransportError();
    return candidate as unknown as WorkerRunWaitingForInputEnvelope;
  }
  if ("deliveryType" in candidate) {
    const validated = await validateRunAgentResponseDelivery(event);
    if (validated.valid && validated.value !== null) return validated.value as RouterRunLifecycleEnvelope;
    throw new RouterHttpTransportError();
  }
  const validated = await validateRuntimeBoundaryMessage(event);
  if (validated.valid && validated.value !== null && ["run.accepted", "run.progressed", "run.succeeded", "run.failed"].includes(validated.value.envelopeType)) return validated.value as RouterRunLifecycleEnvelope;
  throw new RouterHttpTransportError();
}

export async function validRouterWorkerStartEnvelope(value: unknown): Promise<boolean> {
  const validated = await validateRuntimeBoundaryMessage(value);
  return validated.valid && validated.value?.envelopeType === "run.start";
}
