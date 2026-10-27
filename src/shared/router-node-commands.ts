import { createHash } from "node:crypto";

import { NODE_NESTED_BODY_LIMIT_BYTES, NODE_OUTER_BODY_LIMIT_BYTES, NodeContractError, nodeError, nodeKnownWorkerFailure, validateNodeKnownWorkerFailure, validateNodeTarget, type NodeErrorCode, type NodeKnownWorkerFailure, type NodeTarget, type NodeWorkerCommandPort } from "./router-node.js";
import {
  WORKER_COMMAND_VERSION,
  isWorkerOpaqueIdentifier,
  validateRouterWorkerResponse,
  type WorkerCommand,
  type WorkerCommandResponse,
  type WorkerResponseExpectation,
  type WorkerVersionedReference,
} from "./router-worker.js";
import {
  runtimeBoundaryVersion,
  runtimeSourceSessionIdentity,
  validateRuntimeBoundaryMessage,
  type RunStartEnvelope,
  type RuntimeSourceSystem,
  type SessionBinding,
  type SourceRef,
} from "./runtime-boundary.js";
import { validateWorkerSelection, type WorkerSelection } from "./worker-selection.js";


/**
 * What a Worker says it recorded for one command. `response` is untrusted until
 * `validateNodeCommandResponse` holds it to the command's expectation.
 */
export type NodeWorkerRecordedCommand =
  | { readonly status: "recorded"; readonly response: unknown }
  | { readonly status: "not_recorded" };

/**
 * The command Node's Worker port. `readCommand` is required, not optional: it is how an `ambiguous` receipt
 * converges, and a harness that does not provide it would leave that path untested while staying green.
 */
export interface NodeWorkerDispatchPort extends NodeWorkerCommandPort {
  /**
   * Read-only on the Worker: it returns a recorded response and can never cause execution. `signal` aborts the
   * read when the Node shuts down; an aborted read is an unknown answer, never a recorded or absent one.
   */
  readCommand(target: NodeTarget, commandId: string, signal?: AbortSignal): Promise<NodeWorkerRecordedCommand>;
}

/**
 * Whether this inventory can take a command for a target. A closed union so every reason a Worker
 * cannot take a command is one more member each caller must handle.
 */
export type NodeWorkerAdmission = { readonly kind: "available" } | { readonly kind: "absent" };

/**
 * The command Node's Worker inventory: the one collaborator that both dispatches and answers whether it
 * can take a command for a target, so both answers come from the same inventory. It also reads
 * recorded commands.
 */
export interface NodeWorkerInventoryPort extends NodeWorkerDispatchPort {
  admission(target: NodeTarget): NodeWorkerAdmission;
}

/** The Node command contract version. */
export const NODE_COMMAND_CONTRACT_VERSION = "1.0.0" as const;
/**
 * Room completion needs inside the nested limit: 30 bytes of member syntax for the two
 * session-binding ids (at most 128 ASCII bytes each), plus six opaque ids and 148 bytes of member
 * syntax for `executionTarget` (pinned in node-contract against the serialized shape).
 */
export const NODE_PENDING_COMMAND_LIMIT_BYTES = NODE_NESTED_BODY_LIMIT_BYTES - (30 + 2 * 128) - (148 + 6 * 128);

type R = Readonly<Record<string, unknown>>;
type PendingStartIdentity = Omit<RunStartEnvelope["identity"], "sessionBinding"> & {
  readonly sessionBinding: NodePendingSessionBinding;
};

/** Native executor session handles do not cross this Node routing boundary. */
export type NodeSessionBinding = Omit<SessionBinding, "nativeSessionRef"> & {
  readonly nativeSessionRef?: never;
};
export type NodePendingSessionBinding = Omit<NodeSessionBinding, "workerId" | "executorId"> & {
  readonly workerId?: never;
  readonly executorId?: never;
};
export type NodePendingRunStartEnvelope = Omit<RunStartEnvelope, "identity"> & {
  readonly identity: PendingStartIdentity;
};

export interface NodeCommandStart {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly commandId: string;
  readonly command: "start";
  readonly start: NodePendingRunStartEnvelope;
  readonly instruction: string;
  readonly workspacePolicyRef: WorkerVersionedReference | null;
}
export interface NodeCommandContinue {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly commandId: string;
  readonly command: "continue";
  readonly start: NodePendingRunStartEnvelope;
  readonly instruction: string;
  readonly continuation: { readonly priorRunId: string };
  readonly workspacePolicyRef: WorkerVersionedReference | null;
}
export interface NodeCommandCancel {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly commandId: string;
  readonly command: "cancel";
  readonly target: { readonly runId: string };
}
/**
 * Forwards a soft steer to the Worker running `target.runId`. Shaped like `cancel`, plus the
 * instruction the running turn is handed.
 */
export interface NodeCommandSteer {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly commandId: string;
  readonly command: "steer";
  readonly target: { readonly runId: string };
  readonly instruction: string;
}
export type NodeCommand = NodeCommandStart | NodeCommandContinue | NodeCommandCancel | NodeCommandSteer;
export interface NodeCommandRequest {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly nodeRef: WorkerVersionedReference;
  readonly target: { readonly selection: WorkerSelection };
  readonly command: NodeCommand;
}
/** A Node command with the caller's instruction removed: what Node answers about it are judged by. */
export type NodeExpectedCommand =
  | Omit<NodeCommandStart, "instruction">
  | Omit<NodeCommandContinue, "instruction">
  | NodeCommandCancel
  | Omit<NodeCommandSteer, "instruction">;
/**
 * What a Node's answer about one command is validated against: exactly the fields
 * `validateNodeReceipt` and `validateNodeCommandResponse` read. It carries no instruction and is
 * not a `NodeCommandRequest`, so it cannot be dispatched. A caller holding a full request derives
 * one (`nodeReceiptExpectation`); Router rebuilds one from durable state
 * (`validateNodeReceiptExpectation`).
 */
export interface NodeReceiptExpectation {
  readonly expectation: "node-receipt";
  readonly nodeRef: WorkerVersionedReference;
  readonly selection: WorkerSelection;
  readonly command: NodeExpectedCommand;
}

/** Immutable routing reservation. It is not evidence that a Worker executed. */
export interface NodeReservationEvidence {
  readonly workerKey: string;
  readonly target: NodeTarget;
}
/** Available only after a correlated Worker response is accepted. */
export interface NodeExecutionEvidence extends NodeReservationEvidence {
  readonly sessionBinding: NodeSessionBinding;
}
export interface NodePriorRoute {
  readonly nodeRef: WorkerVersionedReference;
  readonly requestedSelection: WorkerSelection;
  readonly reservation: NodeReservationEvidence;
  readonly sessionBinding: NodeSessionBinding;
  readonly sourceSystem: RuntimeSourceSystem;
  readonly sourceSessionId: string;
  readonly sourceRef: SourceRef;
  readonly workspacePolicyRef: WorkerVersionedReference | null;
}
/** Trusted only because the caller obtained it from the authenticated Node transport. */
export interface NodeAuthenticatedContext {
  readonly nodeRef: WorkerVersionedReference;
  readonly nodeId: string;
}
export interface NodeCommandResponse {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly nodeRef: WorkerVersionedReference;
  readonly commandId: string;
  readonly status: "accepted";
  readonly evidence: NodeExecutionEvidence;
  readonly result: WorkerCommandResponse;
}
export interface NodeReceiptQuery {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly nodeRef: WorkerVersionedReference;
  readonly commandId: string;
}
type ReceiptIdentity = {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly nodeRef: WorkerVersionedReference;
  readonly commandId: string;
};
export type NodeReceipt =
  | NodeCommandResponse
  | (ReceiptIdentity & { readonly status: "rejected"; readonly reservation: NodeReservationEvidence; readonly failure: NodeKnownWorkerFailure })
  | (ReceiptIdentity & { readonly status: "reserved" | "dispatching" | "ambiguous"; readonly reservation: NodeReservationEvidence })
  | (ReceiptIdentity & { readonly status: "not_found" });
export interface NodeReceiptValidationContext {
  readonly authenticatedNode: NodeAuthenticatedContext;
  readonly expectation: NodeReceiptExpectation;
  readonly priorRoute?: NodePriorRoute;
}
export interface NodeCommandError {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly error: { readonly code: NodeErrorCode; readonly safeMessage: string };
}

function fail(code: NodeErrorCode = "axis.node.malformed"): never { throw new NodeContractError(code); }
function object(value: unknown): R {
  if (value === null || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) fail();
  return value as R;
}
function closed(value: unknown, keys: readonly string[]): R {
  const item = object(value);
  if (Object.keys(item).length !== keys.length || keys.some((key) => !Object.hasOwn(item, key))) fail();
  return item;
}
function id(value: unknown): string {
  if (!isWorkerOpaqueIdentifier(value)) fail();
  return value;
}
function reference(value: unknown): WorkerVersionedReference {
  const item = closed(value, ["ref", "version"]);
  return Object.freeze({ ref: id(item.ref), version: id(item.version) });
}
function optionalReference(value: unknown): WorkerVersionedReference | null {
  return value === null ? null : reference(value);
}
function same(left: unknown, right: unknown): boolean { return canonical(left) === canonical(right); }
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const item = value as R;
    return `{${Object.keys(item).sort().map((key) => `${JSON.stringify(key)}:${canonical(item[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
function size(value: unknown, limit: number): void {
  try { if (Buffer.byteLength(JSON.stringify(value), "utf8") > limit) fail("axis.node.body_too_large"); }
  catch (error) { if (error instanceof NodeContractError) throw error; fail(); }
}
function freeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}
function selection(value: unknown): WorkerSelection {
  try { return validateWorkerSelection(value); }
  catch { fail(); }
}
function boundedTarget(value: unknown): NodeTarget {
  const target = validateNodeTarget(value);
  for (const value of [
    target.endpointRef.ref, target.endpointRef.version, target.identity.nodeId,
    target.identity.workerId, target.identity.executorKind, target.identity.runtimeProfile.ref,
    target.identity.runtimeProfile.version, target.sessionBindingExecutorId,
  ]) id(value);
  return freeze(structuredClone(target));
}
export function validateNodeAuthenticatedContext(value: unknown): NodeAuthenticatedContext {
  const item = closed(value, ["nodeRef", "nodeId"]);
  return freeze({ nodeRef: reference(item.nodeRef), nodeId: id(item.nodeId) });
}
function validateSessionBinding(value: unknown): NodeSessionBinding {
  const item = closed(value, ["contractVersion", "sessionBindingId", "taskId", "attemptId", "runId", "correlationId", "workerId", "executorId"]);
  if (runtimeBoundaryVersion(item.contractVersion) === null) fail();
  for (const key of ["sessionBindingId", "taskId", "attemptId", "runId", "correlationId", "workerId", "executorId"] as const) id(item[key]);
  return freeze(structuredClone(item)) as unknown as NodeSessionBinding;
}

const PENDING_WORKER_SENTINEL = "axis-validation-only-worker";
const PENDING_EXECUTOR_SENTINEL = "axis-validation-only-executor";
async function pendingStart(value: unknown): Promise<NodePendingRunStartEnvelope> {
  const original = structuredClone(object(value));
  const identity = object(original.identity);
  const pendingBinding = closed(identity.sessionBinding, ["contractVersion", "sessionBindingId", "taskId", "attemptId", "runId", "correlationId"]);
  const validationOnly = structuredClone(original) as Record<string, unknown>;
  validationOnly.identity = {
    ...identity,
    sessionBinding: { ...pendingBinding, workerId: PENDING_WORKER_SENTINEL, executorId: PENDING_EXECUTOR_SENTINEL },
  };
  const validated = await validateRuntimeBoundaryMessage(validationOnly);
  if (!validated.valid || validated.value?.envelopeType !== "run.start") fail();
  return freeze(original) as unknown as NodePendingRunStartEnvelope;
}

/**
 * One validator for a Node command's fields, with or without its instruction. A request requires
 * one; an expectation must NOT carry one, so its closed key set rejects a smuggled instruction.
 */
async function commandFields(value: unknown, withInstruction: boolean): Promise<{ readonly expected: NodeExpectedCommand; readonly instruction: string | null }> {
  size(value, NODE_NESTED_BODY_LIMIT_BYTES);
  const item = object(value);
  if (item.contractVersion !== NODE_COMMAND_CONTRACT_VERSION) fail();
  const commandId = id(item.commandId);
  if (item.command === "cancel") {
    closed(item, ["contractVersion", "commandId", "command", "target"]);
    return { expected: freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId, command: "cancel", target: { runId: id(closed(item.target, ["runId"]).runId) } }), instruction: null };
  }
  if (item.command === "steer") {
    closed(item, withInstruction ? ["contractVersion", "commandId", "command", "target", "instruction"] : ["contractVersion", "commandId", "command", "target"]);
    size(item, NODE_PENDING_COMMAND_LIMIT_BYTES);
    if (withInstruction && (typeof item.instruction !== "string" || !item.instruction.trim())) fail();
    const target = { runId: id(closed(item.target, ["runId"]).runId) };
    return { expected: freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId, command: "steer", target }), instruction: withInstruction ? item.instruction as string : null };
  }
  const keys = withInstruction
    ? ["contractVersion", "commandId", "command", "start", "instruction", "workspacePolicyRef"]
    : ["contractVersion", "commandId", "command", "start", "workspacePolicyRef"];
  if (item.command === "continue") keys.push("continuation");
  else if (item.command !== "start") fail();
  closed(item, keys);
  size(item, NODE_PENDING_COMMAND_LIMIT_BYTES);
  if (withInstruction && (typeof item.instruction !== "string" || !item.instruction.trim())) fail();
  const start = await pendingStart(item.start);
  const workspacePolicyRef = optionalReference(item.workspacePolicyRef);
  const instruction = withInstruction ? item.instruction as string : null;
  if (item.command === "continue") {
    const continuation = closed(item.continuation, ["priorRunId"]);
    return { expected: freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId, command: "continue", start, continuation: { priorRunId: id(continuation.priorRunId) }, workspacePolicyRef }), instruction };
  }
  return { expected: freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId, command: "start", start, workspacePolicyRef }), instruction };
}

async function command(value: unknown): Promise<NodeCommand> {
  const { expected, instruction } = await commandFields(value, true);
  if (expected.command === "cancel") return expected;
  if (instruction === null) fail();
  if (expected.command === "steer") {
    return freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: expected.commandId, command: "steer", target: expected.target, instruction });
  }
  if (expected.command === "continue") {
    return freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: expected.commandId, command: "continue", start: expected.start, instruction, continuation: expected.continuation, workspacePolicyRef: expected.workspacePolicyRef });
  }
  return freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: expected.commandId, command: "start", start: expected.start, instruction, workspacePolicyRef: expected.workspacePolicyRef });
}

/** The instruction-free half of a command, in the field order a request carries. */
function expectationOf(nodeRef: WorkerVersionedReference, selection: WorkerSelection, command: NodeCommand | NodeExpectedCommand): NodeReceiptExpectation {
  const expected: NodeExpectedCommand = command.command === "cancel"
    ? { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: command.commandId, command: "cancel", target: { runId: command.target.runId } }
    : command.command === "steer"
      ? { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: command.commandId, command: "steer", target: { runId: command.target.runId } }
      : command.command === "continue"
      ? { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: command.commandId, command: "continue", start: command.start, continuation: { priorRunId: command.continuation.priorRunId }, workspacePolicyRef: command.workspacePolicyRef }
      : { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: command.commandId, command: "start", start: command.start, workspacePolicyRef: command.workspacePolicyRef };
  return freeze({ expectation: "node-receipt", nodeRef, selection, command: expected });
}

export function nodeCommandError(code: NodeErrorCode): NodeCommandError {
  const error = nodeError(code).error;
  return freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, error });
}
export function validateNodeCommandError(value: unknown): NodeCommandError {
  const item = closed(value, ["contractVersion", "error"]), error = closed(item.error, ["code", "safeMessage"]);
  if (item.contractVersion !== NODE_COMMAND_CONTRACT_VERSION || typeof error.code !== "string") fail();
  const result = nodeCommandError(error.code as NodeErrorCode);
  if (!same(item, result)) fail();
  return result;
}

export function validateNodeReservationEvidence(value: unknown): NodeReservationEvidence {
  const item = closed(value, ["workerKey", "target"]);
  return freeze({ workerKey: id(item.workerKey), target: boundedTarget(item.target) });
}

function sourceReference(value: unknown): SourceRef {
  const item = object(value);
  if (item.system === "linear") {
    closed(item, ["system", "organizationId", "issueId", "agentSessionId"]);
    return { system: "linear", organizationId: id(item.organizationId), issueId: id(item.issueId), agentSessionId: id(item.agentSessionId) };
  }
  fail();
}

export function validateNodePriorRoute(value: unknown): NodePriorRoute {
  const item = closed(value, ["nodeRef", "requestedSelection", "reservation", "sessionBinding", "sourceSystem", "sourceSessionId", "sourceRef", "workspacePolicyRef"]);
  const reservation = validateNodeReservationEvidence(item.reservation);
  const requestedSelection = selection(item.requestedSelection);
  const sessionBinding = validateSessionBinding(item.sessionBinding);
  const sourceRef = sourceReference(item.sourceRef);
  const source = runtimeSourceSessionIdentity(sourceRef);
  const sourceSessionId = id(item.sourceSessionId);
  if (item.sourceSystem !== "linear") fail();
  if (sessionBinding.workerId !== reservation.target.identity.workerId
    || sessionBinding.executorId !== reservation.target.sessionBindingExecutorId
    || source.system !== item.sourceSystem || source.sessionId !== sourceSessionId
    ) fail("axis.node.binding_conflict");
  return freeze({
    nodeRef: reference(item.nodeRef), requestedSelection, reservation, sessionBinding,
    sourceSystem: item.sourceSystem, sourceSessionId, sourceRef,
    workspacePolicyRef: optionalReference(item.workspacePolicyRef),
  });
}

/** Validates and clones only caller-controlled request bytes; it never requires or reads durable predecessor state. */
export async function validateNodeCommandCandidate(value: unknown): Promise<NodeCommandRequest> {
  size(value, NODE_OUTER_BODY_LIMIT_BYTES);
  const item = closed(value, ["contractVersion", "nodeRef", "target", "command"]);
  if (item.contractVersion !== NODE_COMMAND_CONTRACT_VERSION) fail();
  const target = closed(item.target, ["selection"]);
  const request = freeze({
    contractVersion: NODE_COMMAND_CONTRACT_VERSION,
    nodeRef: reference(item.nodeRef),
    target: freeze({ selection: selection(target.selection) }),
    command: await command(item.command),
  });
  // Only a start or continue carries a Run start, and so a source session; cancel and steer carry neither.
  if (request.command.command === "start" || request.command.command === "continue") {
    const source = runtimeSourceSessionIdentity(request.command.start.identity.task.sourceRef);
    id(source.sessionId);
  }
  return request;
}

export async function validateNodeCommandRequest(value: unknown, priorRoute?: NodePriorRoute | null): Promise<NodeCommandRequest> {
  const request = await validateNodeCommandCandidate(value);
  assertExpectedPrior(request.nodeRef, request.target.selection, request.command, priorRoute);
  return request;
}

/** How a non-start command must relate to the route it continues, cancels or steers. Shared by requests and expectations. */
function assertExpectedPrior(nodeRef: WorkerVersionedReference, requestedSelection: WorkerSelection, command: NodeExpectedCommand, priorRoute: NodePriorRoute | null | undefined): void {
  if (command.command === "start") return;
  if (priorRoute == null) fail("axis.node.recovery_required");
  const prior = validateNodePriorRoute(priorRoute);
  if (!same(nodeRef, prior.nodeRef) || !same(requestedSelection, prior.requestedSelection)) fail("axis.node.binding_conflict");
  const expectedPriorRunId = command.command === "continue" ? command.continuation.priorRunId : command.target.runId;
  if (expectedPriorRunId !== prior.sessionBinding.runId) fail("axis.node.binding_conflict");
  if (command.command === "continue") {
    const source = runtimeSourceSessionIdentity(command.start.identity.task.sourceRef);
    const run = command.start.identity.run;
    if ((run.priorRunId !== undefined && run.priorRunId !== expectedPriorRunId)
      || run.runId === prior.sessionBinding.runId
      || command.start.identity.task.taskId !== prior.sessionBinding.taskId
      || command.start.identity.task.correlationId !== prior.sessionBinding.correlationId
      || command.start.identity.task.contractVersion !== prior.sessionBinding.contractVersion
      || !same(command.start.identity.task.sourceRef, prior.sourceRef)
      || source.system !== prior.sourceSystem || source.sessionId !== prior.sourceSessionId
      || !same(command.workspacePolicyRef, prior.workspacePolicyRef)) fail("axis.node.binding_conflict");
  }
}

/** The expectation a full request stands for, validating the request exactly as a dispatch would. */
export async function nodeReceiptExpectation(requestValue: NodeCommandRequest, priorRoute?: NodePriorRoute | null): Promise<NodeReceiptExpectation> {
  const request = await validateNodeCommandRequest(requestValue, priorRoute);
  return expectationOf(request.nodeRef, request.target.selection, request.command);
}

/** Validates an expectation, including one rebuilt from state that never held the instruction. */
export async function validateNodeReceiptExpectation(value: unknown, priorRoute?: NodePriorRoute | null): Promise<NodeReceiptExpectation> {
  const item = closed(value, ["expectation", "nodeRef", "selection", "command"]);
  if (item.expectation !== "node-receipt") fail();
  const { expected } = await commandFields(item.command, false);
  const nodeRef = reference(item.nodeRef), requestedSelection = selection(item.selection);
  if (expected.command === "start" || expected.command === "continue") id(runtimeSourceSessionIdentity(expected.start.identity.task.sourceRef).sessionId);
  assertExpectedPrior(nodeRef, requestedSelection, expected, priorRoute);
  return expectationOf(nodeRef, requestedSelection, expected);
}

/**
 * Completes pending identity only from the selected immutable reservation. The request and prior
 * route are validated by `validateNodeCommandRequest`; the command's shape and version are built
 * here; the pending-command limit leaves room for the two ids completion adds; and those ids come
 * from a reservation `boundedTarget` validated with `isWorkerOpaqueIdentifier`, the constraint every
 * accepted Runtime version's `providerNeutralId` has.
 */
export async function completeNodeWorkerCommand(
  value: NodeCommandRequest,
  resolved: NodeReservationEvidence,
  priorRoute?: NodePriorRoute | null,
): Promise<WorkerCommand> {
  const prior = priorRoute == null ? undefined : validateNodePriorRoute(priorRoute);
  const request = await validateNodeCommandRequest(value, prior);
  const reservation = validateNodeReservationEvidence(resolved);
  if (request.command.command !== "start") {
    if (prior === undefined || !same(prior.reservation, reservation)) fail("axis.node.binding_conflict");
  }
  let workerCommand: WorkerCommand;
  // The reservation's identity travels with the command, so a different Worker refuses it.
  const executionTarget = { identity: reservation.target.identity, sessionBindingExecutorId: reservation.target.sessionBindingExecutorId };
  if (request.command.command === "cancel") {
    workerCommand = { ...request.command, contractVersion: WORKER_COMMAND_VERSION };
  } else if (request.command.command === "steer") {
    // Like cancel: no `executionTarget` -- the Worker acts only on a Run it is already supervising.
    workerCommand = { contractVersion: WORKER_COMMAND_VERSION, commandId: request.command.commandId, command: "steer", target: { runId: request.command.target.runId }, instruction: request.command.instruction };
  } else {
    const completeStart = {
      ...request.command.start,
      identity: {
        ...request.command.start.identity,
        sessionBinding: {
          ...request.command.start.identity.sessionBinding,
          workerId: reservation.target.identity.workerId,
          executorId: reservation.target.sessionBindingExecutorId,
        },
      },
    } as RunStartEnvelope;
    workerCommand = { ...request.command, contractVersion: WORKER_COMMAND_VERSION, start: completeStart, executionTarget };
  }
  return freeze(structuredClone(workerCommand));
}

function validateExecutionEvidence(value: unknown): NodeExecutionEvidence {
  const item = closed(value, ["workerKey", "target", "sessionBinding"]);
  const reservation = validateNodeReservationEvidence({ workerKey: item.workerKey, target: item.target });
  const sessionBinding = validateSessionBinding(item.sessionBinding);
  if (sessionBinding.workerId !== reservation.target.identity.workerId
    || sessionBinding.executorId !== reservation.target.sessionBindingExecutorId) fail("axis.node.binding_conflict");
  return freeze({ ...reservation, sessionBinding });
}

function validateAuthenticatedReservation(expectation: NodeReceiptExpectation, reservation: NodeReservationEvidence, context: NodeAuthenticatedContext): void {
  if (!same(expectation.nodeRef, context.nodeRef) || reservation.target.identity.nodeId !== context.nodeId
    ) fail("axis.node.binding_conflict");
}

/**
 * The Worker command a response must correlate with, completed from the evidence reservation.
 * Not `completeNodeWorkerCommand`, which needs an instruction. The registration and
 * session-binding ids are built from this reservation; the source session, prior lineage, requested
 * key and start schema are checked before this runs; the prior route's reservation is checked here.
 */
function expectedWorkerResponse(expectation: NodeReceiptExpectation, reservation: NodeReservationEvidence, prior: NodePriorRoute | undefined): WorkerResponseExpectation {
  if (expectation.command.command !== "start" && (prior === undefined || !same(prior.reservation, reservation))) fail("axis.node.binding_conflict");
  const command = expectation.command;
  if (command.command === "cancel") return freeze({ contractVersion: WORKER_COMMAND_VERSION, commandId: command.commandId, command: "cancel", target: { runId: command.target.runId } });
  if (command.command === "steer") return freeze({ contractVersion: WORKER_COMMAND_VERSION, commandId: command.commandId, command: "steer", target: { runId: command.target.runId } });
  const start = {
    ...command.start,
    identity: {
      ...command.start.identity,
      sessionBinding: {
        ...command.start.identity.sessionBinding,
        workerId: reservation.target.identity.workerId,
        executorId: reservation.target.sessionBindingExecutorId,
      },
    },
  } as RunStartEnvelope;
  return freeze({ contractVersion: WORKER_COMMAND_VERSION, commandId: command.commandId, command: command.command, start });
}

export async function validateNodeCommandResponse(
  value: unknown,
  expectationValue: NodeReceiptExpectation,
  authenticatedValue: NodeAuthenticatedContext,
  priorRoute?: NodePriorRoute | null,
): Promise<NodeCommandResponse> {
  size(value, NODE_OUTER_BODY_LIMIT_BYTES);
  const prior = priorRoute == null ? undefined : validateNodePriorRoute(priorRoute);
  const expectation = await validateNodeReceiptExpectation(expectationValue, prior);
  const authenticated = validateNodeAuthenticatedContext(authenticatedValue);
  const item = closed(value, ["contractVersion", "nodeRef", "commandId", "status", "evidence", "result"]);
  if (item.contractVersion !== NODE_COMMAND_CONTRACT_VERSION || item.status !== "accepted"
    || !same(reference(item.nodeRef), expectation.nodeRef) || item.commandId !== expectation.command.commandId) fail();
  const evidence = validateExecutionEvidence(item.evidence);
  validateAuthenticatedReservation(expectation, evidence, authenticated);
  const completed = expectedWorkerResponse(expectation, { workerKey: evidence.workerKey, target: evidence.target }, prior);
  // Cancel and steer act on the prior route's Run, so they answer with its session binding.
  if (completed.command === "cancel" || completed.command === "steer") {
    if (prior === undefined || !same(evidence.sessionBinding, prior.sessionBinding)) fail("axis.node.binding_conflict");
  } else if (!same(evidence.sessionBinding, completed.start.identity.sessionBinding)) fail("axis.node.binding_conflict");
  size(item.result, NODE_NESTED_BODY_LIMIT_BYTES);
  let result: WorkerCommandResponse;
  try { result = await validateRouterWorkerResponse(item.result, completed); }
  catch { fail("axis.node.dispatch_ambiguous"); }
  if (result.status === "accepted" && !same(result.identity, evidence.target.identity)) fail("axis.node.dispatch_ambiguous");
  return freeze({
    contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: expectation.nodeRef,
    commandId: expectation.command.commandId, status: "accepted", evidence, result,
  });
}

export function validateNodeReceiptQuery(value: unknown): NodeReceiptQuery {
  const item = closed(value, ["contractVersion", "nodeRef", "commandId"]);
  if (item.contractVersion !== NODE_COMMAND_CONTRACT_VERSION) fail();
  return freeze({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: reference(item.nodeRef), commandId: id(item.commandId) });
}

const validatedReceipts = new WeakSet<object>();
function retainValidatedReceipt<T extends NodeReceipt>(receipt: T): T {
  freeze(receipt); validatedReceipts.add(receipt); return receipt;
}
export async function validateNodeReceipt(
  value: unknown,
  queryValue: Omit<NodeReceiptQuery, "contractVersion">,
  context?: NodeReceiptValidationContext,
): Promise<NodeReceipt> {
  size(value, NODE_OUTER_BODY_LIMIT_BYTES);
  const query = validateNodeReceiptQuery({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, ...queryValue });
  const item = object(value);
  if (item.contractVersion !== NODE_COMMAND_CONTRACT_VERSION || !same(reference(item.nodeRef), query.nodeRef) || item.commandId !== query.commandId) fail();
  const common: ReceiptIdentity = { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: query.nodeRef, commandId: query.commandId };
  const commonKeys = ["contractVersion", "nodeRef", "commandId", "status"];
  if (item.status === "not_found") {
    closed(item, commonKeys);
    return retainValidatedReceipt({ ...common, status: "not_found" });
  }
  if (context === undefined) fail("axis.node.recovery_required");
  const prior = context.priorRoute === undefined ? undefined : validateNodePriorRoute(context.priorRoute);
  const expectation = await validateNodeReceiptExpectation(context.expectation, prior);
  const authenticated = validateNodeAuthenticatedContext(context.authenticatedNode);
  if (expectation.command.commandId !== query.commandId || !same(expectation.nodeRef, query.nodeRef)) fail("axis.node.binding_conflict");
  if (item.status === "accepted") {
    return retainValidatedReceipt(await validateNodeCommandResponse(item, expectation, authenticated, prior));
  }
  const reservation = validateNodeReservationEvidence(item.reservation);
  validateAuthenticatedReservation(expectation, reservation, authenticated);
  if (expectation.command.command !== "start" && (prior === undefined || !same(reservation, prior.reservation))) fail("axis.node.binding_conflict");
  if (item.status === "reserved" || item.status === "dispatching" || item.status === "ambiguous") {
    closed(item, [...commonKeys, "reservation"]);
    return retainValidatedReceipt({ ...common, status: item.status, reservation });
  }
  if (item.status === "rejected") {
    closed(item, [...commonKeys, "reservation", "failure"]);
    return retainValidatedReceipt({ ...common, status: "rejected", reservation, failure: validateNodeKnownWorkerFailure(item.failure) });
  }
  fail();
}

/** Validated pre-acceptance receipts and buffered lifecycle events establish no execution identity. */
export function nodeReceiptExecutionEvidence(receipt: NodeReceipt): NodeExecutionEvidence | null {
  if (!validatedReceipts.has(receipt)) fail();
  return receipt.status === "accepted" ? receipt.evidence : null;
}

export function canonicalNodeCommandIntent(request: NodeCommandRequest): R {
  const instructionSha256 = request.command.command === "cancel" ? null : `sha256:${createHash("sha256").update(request.command.instruction).digest("hex")}`;
  return nodeReceiptExpectationIntent(expectationOf(request.nodeRef, request.target.selection, request.command), instructionSha256);
}
/**
 * The redacted intent an expectation stands for, given the digest of the instruction it no longer
 * carries. `canonicalNodeCommandIntent` is this applied to a full request.
 */
export function nodeReceiptExpectationIntent(expectation: NodeReceiptExpectation, instructionSha256: string | null): R {
  const command = expectation.command.command === "cancel" ? expectation.command : { ...expectation.command, instructionSha256 };
  return freeze(structuredClone({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: expectation.nodeRef, target: { selection: expectation.selection }, command }));
}
export function nodeCommandIntentDigest(request: NodeCommandRequest): `sha256:${string}` {
  return `sha256:${createHash("sha256").update("axis.node.command.intent.v1\0").update(canonical(canonicalNodeCommandIntent(request))).digest("hex")}`;
}
/** A journal slot is keyed by commandId: only an exact canonical request is a replay. */
export function nodeCommandReplayDisposition(previous: NodeCommandRequest, candidate: NodeCommandRequest): "replay" | "conflict" {
  return previous.command.commandId === candidate.command.commandId
    && same(canonicalNodeCommandIntent(previous), canonicalNodeCommandIntent(candidate)) ? "replay" : "conflict";
}

