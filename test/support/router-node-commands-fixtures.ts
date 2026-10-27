import { buildNodeAcceptedEvent, buildWorkerBoundStartRequest, buildNodeWaitingEvent, nodeFixtureRef, nodeFixtureTarget } from "./router-node-fixtures.js";
import { NODE_COMMAND_CONTRACT_VERSION, NODE_PENDING_COMMAND_LIMIT_BYTES, completeNodeWorkerCommand, type NodeAuthenticatedContext, type NodeCommandRequest, type NodeExecutionEvidence, type NodePriorRoute, type NodeReceipt, type NodeReservationEvidence } from "../../src/shared/router-node-commands.js";
import { nodeKnownWorkerFailure, type NodeErrorCode } from "../../src/shared/router-node.js";

import { runtimeSourceSessionIdentity, type RunAcceptedEnvelope, type RunStartEnvelope, type RuntimeBoundaryVersion } from "../../src/shared/runtime-boundary.js";
import { WORKER_COMMAND_VERSION, type RouterRunLifecycleEnvelope, type WorkerCommand, type WorkerCommandResponse, type WorkerExecutionIdentity } from "../../src/shared/router-worker.js";
import type { WorkerSelection } from "../../src/shared/worker-selection.js";
import { workerSelectionFixture } from "./node-routing-fixtures.js";
import type { SqliteNodeJournal } from "../../src/node/journal.js";

export const nodeReservation: NodeReservationEvidence = {
  workerKey: "codex-primary",
  target: nodeFixtureTarget,
};
export const nodeAuthenticatedContext: NodeAuthenticatedContext = {
  nodeRef: nodeFixtureRef,
  nodeId: nodeFixtureTarget.identity.nodeId,
};
/** Fixed oracle for the execution identity established by the base command fixture. */
export const nodeExpectedExecutionEvidence: NodeExecutionEvidence = {
  workerKey: "codex-primary",
  target: {
    endpointRef: { ref: "endpoint-1", version: "v1" },
    identity: {
      nodeId: "physical-node-1",
      workerId: "worker-1",
      executorKind: "codex",
      runtimeProfile: { ref: "profile-1", version: "v1" },
    },
    sessionBindingExecutorId: "executor-1",
  },
  sessionBinding: {
    contractVersion: "1.0.0",
    sessionBindingId: "binding-1",
    taskId: "task-1",
    attemptId: "attempt-1",
    runId: "run-1",
    correlationId: "c1",
    workerId: "worker-1",
    executorId: "executor-1",
  },
};

export type NodeTimingCommandDisposition = "none" | "dispatch" | "reconcile-receipt";
export type NodeTimingEventOrigin =
  | { readonly authentication: "authenticated"; readonly identity: WorkerExecutionIdentity }
  | { readonly authentication: "unavailable" };
export type NodeTimingObservation =
  | {
    readonly kind: "command";
    readonly delivery: "initial" | "retry" | "new-start";
    readonly request: NodeCommandRequest;
    readonly reservation: NodeReservationEvidence;
  }
  | {
    readonly kind: "event";
    readonly event: RouterRunLifecycleEnvelope;
    /** Identity independently authenticated by the event ingress, never inferred from the envelope or its receipt. */
    readonly origin: NodeTimingEventOrigin;
  }
  | {
    readonly kind: "receipt";
    readonly delivery: "command-response" | "receipt-query" | "late";
    readonly receipt: unknown;
  };
export interface NodeTimingExpectedState {
  readonly reservation: NodeReservationEvidence | null;
  readonly executionEvidence: NodeExecutionEvidence | null;
  readonly bufferedEventIds: readonly string[];
  readonly projectableEventIds: readonly string[];
  readonly quarantinedEventIds: readonly string[];
  readonly commandDisposition: NodeTimingCommandDisposition;
  readonly replayDisposition: "replay" | "conflict" | null;
  readonly eventIdentityAvailability: "available" | "unavailable" | null;
  readonly eventOriginDisposition: "authenticated" | "unavailable" | "mismatch" | null;
  readonly conflictCode: NodeErrorCode | null;
  readonly dispatchCount: number;
  readonly receiptQueryCount: number;
  readonly conflictCount: number;
}
export interface NodeTimingStep {
  readonly label: string;
  readonly observation: NodeTimingObservation;
  readonly expected: NodeTimingExpectedState;
}
export interface NodeTimingVector {
  readonly id:
    | "early-event-then-correlated-acceptance"
    | "lost-acceptance-same-command-reconciliation"
    | "conflicting-late-receipt-and-event"
    | "replay-versus-source-session-binding-conflict";
  readonly steps: readonly NodeTimingStep[];
}

function pending(start: RunStartEnvelope) {
  const { workerId: _workerId, executorId: _executorId, nativeSessionRef: _native, ...sessionBinding } = start.identity.sessionBinding;
  return { ...start, identity: { ...start.identity, sessionBinding } };
}

export async function buildNodeStartRequest(selection: WorkerSelection = workerSelectionFixture()): Promise<NodeCommandRequest> {
  const base = await buildWorkerBoundStartRequest();
  return {
    contractVersion: NODE_COMMAND_CONTRACT_VERSION,
    nodeRef: base.nodeRef,
    target: { selection },
    command: {
      contractVersion: NODE_COMMAND_CONTRACT_VERSION,
      commandId: base.command.commandId,
      command: "start",
      start: pending(base.command.start),
      instruction: base.command.instruction,
      workspacePolicyRef: base.command.workspacePolicyRef,
    },
  };
}

/** Linear start that permits an AgentActivity response delivery from the Worker. */
export async function buildNodeLinearStartRequest(selection: WorkerSelection = workerSelectionFixture()): Promise<NodeCommandRequest> {
  const base = await buildWorkerBoundStartRequest("command--linear-1");
  return {
    contractVersion: NODE_COMMAND_CONTRACT_VERSION,
    nodeRef: base.nodeRef,
    target: { selection },
    command: {
      contractVersion: NODE_COMMAND_CONTRACT_VERSION,
      commandId: base.command.commandId,
      command: "start",
      start: pending(base.command.start),
      instruction: base.command.instruction,
      workspacePolicyRef: base.command.workspacePolicyRef,
    },
  };
}


export async function buildNodePriorRoute(selection: WorkerSelection = workerSelectionFixture()): Promise<NodePriorRoute> {
  const base = await buildWorkerBoundStartRequest();
  const source = runtimeSourceSessionIdentity(base.command.start.identity.task.sourceRef);
  if (source.sessionId === undefined) throw new Error("The Node command fixture requires a source session.");
  const { nativeSessionRef: _native, ...sessionBinding } = base.command.start.identity.sessionBinding;
  return {
    nodeRef: nodeFixtureRef,
    requestedSelection: selection,
    reservation: nodeReservation,
    sessionBinding: {
      ...sessionBinding,
      sessionBindingId: "binding-prior",
      runId: "run-prior",
    },
    sourceSystem: source.system,
    sourceSessionId: source.sessionId,
    sourceRef: base.command.start.identity.task.sourceRef,
    workspacePolicyRef: base.command.workspacePolicyRef,
  };
}

export async function buildNodeContinuationRequest(selection: WorkerSelection = workerSelectionFixture()): Promise<NodeCommandRequest> {
  const base = await buildNodeStartRequest(selection);
  if (base.command.command !== "start") throw new Error("The Node command start fixture is invalid.");
  return {
    ...base,
    command: {
      ...base.command,
      commandId: "command-continue",
      command: "continue",
      continuation: { priorRunId: "run-prior" },
    },
  };
}

export interface NodeContinuationVector {
  readonly id: string;
  readonly request: NodeCommandRequest;
  readonly priorRoute: NodePriorRoute;
  readonly expected: { readonly command: WorkerCommand } | { readonly error: NodeErrorCode };
}

/** Fixed version/predecessor oracles; expected commands never call completion. */
export async function buildNodeContinuationVectors(): Promise<readonly NodeContinuationVector[]> {
  const base = await buildNodeContinuationRequest();
  const basePrior = await buildNodePriorRoute();
  if (base.command.command !== "continue") throw new Error("Expected continuation fixture.");
  const original = base.command;
  const cases: readonly [RuntimeBoundaryVersion, string | null, NodeErrorCode | null][] = [
    ["1.0.0", null, null],
    ["1.0.0", "run-prior", null],
    ["1.0.0", "run-unrelated", "axis.node.binding_conflict"],
  ];
  return cases.map(([contractVersion, priorRunId, error]) => {
    const graph = original.start.identity;
    const identity = {
      ...graph, contractVersion,
      task: { ...graph.task, contractVersion },
      attempt: { ...graph.attempt, contractVersion },
      run: { ...graph.run, contractVersion, ...(priorRunId === null ? {} : { priorRunId }) },
      sessionBinding: { ...graph.sessionBinding, contractVersion },
    };
    const start = { ...original.start, contractVersion, identity };
    const request: NodeCommandRequest = { ...base, command: { ...original, start } };
    const priorRoute = { ...basePrior, sessionBinding: { ...basePrior.sessionBinding, contractVersion } };
    const expectedCommand: WorkerCommand = {
      contractVersion: WORKER_COMMAND_VERSION, commandId: "command-continue", command: "continue",
      start: { ...start, identity: { ...identity, sessionBinding: { ...identity.sessionBinding, workerId: "worker-1", executorId: "executor-1" } } },
      instruction: original.instruction,
      continuation: { priorRunId: "run-prior" },
      workspacePolicyRef: original.workspacePolicyRef,
      executionTarget: { identity: nodeReservation.target.identity, sessionBindingExecutorId: nodeReservation.target.sessionBindingExecutorId },
    };
    return { id: `${contractVersion}/${priorRunId ?? "absent"}`, request, priorRoute, expected: error === null ? { command: expectedCommand } : { error } };
  });
}

/** Internally valid graphs that must still retain the prior Task's identity. */
export async function buildNodeContinuationIdentityVectors(): Promise<readonly NodeContinuationVector[]> {
  const controls = (await buildNodeContinuationVectors()).filter((vector) => vector.id.endsWith("/absent"));
  return controls.flatMap((control) => {
    if (control.request.command.command !== "continue") throw new Error("Expected continuation fixture.");
    const command = control.request.command;
    const graph = command.start.identity;
    const changedCorrelation = "correlation-changed";
    const variants = [
      { id: "correlation", fields: { correlationId: changedCorrelation }, envelope: { correlationId: changedCorrelation } },
    ] as const;
    return [
      { ...control, id: `${control.id}/identity-matching` },
      ...variants.map(({ id, fields, envelope }): NodeContinuationVector => ({
        id: `${control.id}/identity-${id}-drift`,
        request: {
          ...control.request,
          command: {
            ...command,
            start: {
              ...command.start, ...envelope,
              identity: {
                ...graph,
                task: { ...graph.task, ...fields },
                attempt: { ...graph.attempt, ...fields },
                run: { ...graph.run, ...fields },
                sessionBinding: { ...graph.sessionBinding, ...fields },
              },
            },
          },
        },
        priorRoute: control.priorRoute,
        expected: { error: "axis.node.binding_conflict" },
      })),
    ];
  });
}

export interface NodeCompletionBoundaryVector {
  readonly id: string;
  readonly request: NodeCommandRequest;
  readonly priorRoute: NodePriorRoute | null;
  readonly reservation: NodeReservationEvidence;
  readonly pendingBytes: number;
  readonly expected: { readonly command: WorkerCommand; readonly completedBytes: number } | { readonly error: NodeErrorCode };
}

/** Full Task-source equality is stronger than equality of the session lookup key. */
export async function buildNodeContinuationSourceVectors(): Promise<readonly NodeContinuationVector[]> {
  const controls = (await buildNodeContinuationVectors()).filter((vector) => vector.id.endsWith("/absent"));
  return controls.flatMap((control) => {
    if (control.request.command.command !== "continue") throw new Error("Expected continuation fixture.");
    const command = control.request.command;
    const graph = command.start.identity;
    const fields = ["organizationId", "issueId"];
    return [
      { ...control, id: `${control.id}/source-matching` },
      ...fields.map((field): NodeContinuationVector => ({
        ...control, id: `${control.id}/source-${field}-drift`,
        request: {
          ...control.request,
          command: {
            ...command,
            start: { ...command.start, identity: { ...graph, task: { ...graph.task, sourceRef: { ...graph.task.sourceRef, [field]: "different-source" } } } },
          },
        },
        expected: { error: "axis.node.binding_conflict" },
      })),
    ];
  });
}

/**
 * Independent byte oracles reserve the largest legal execution identity: two
 * session-binding IDs and six executionTarget IDs. The executionTarget JSON member
 * syntax uses 148 bytes. With each ID at its 128-byte maximum, a command at the pending
 * limit completes to exactly the nested limit.
 */
export async function buildNodeCompletionBoundaryVectors(): Promise<readonly NodeCompletionBoundaryVector[]> {
  const result: NodeCompletionBoundaryVector[] = [];
  for (const operation of ["start", "continue"] as const) {
    const base = operation === "start" ? await buildNodeStartRequest() : await buildNodeContinuationRequest();
    if (base.command.command === "cancel" || base.command.command === "steer") throw new Error("Expected pending fixture.");
    for (const identityLength of ["ordinary", "maximum"] as const) {
      const maximum = identityLength === "maximum";
      const workerId = maximum ? "w".repeat(128) : "worker-1";
      const executorId = maximum ? "e".repeat(128) : "executor-1";
      const identity: WorkerExecutionIdentity = maximum
        ? { nodeId: "n".repeat(128), workerId, executorKind: "k".repeat(128), runtimeProfile: { ref: "r".repeat(128), version: "v".repeat(128) } }
        : { ...nodeReservation.target.identity, workerId };
      const reservation = {
        ...nodeReservation,
        target: { ...nodeReservation.target, identity, sessionBindingExecutorId: executorId },
      };
      const prior = operation === "start" ? null : await buildNodePriorRoute();
      const priorRoute = prior === null ? null : {
        ...prior, reservation,
        sessionBinding: { ...prior.sessionBinding, workerId, executorId },
      };
      for (const [encoding, pendingBytes] of [["ascii", NODE_PENDING_COMMAND_LIMIT_BYTES], ["utf8", NODE_PENDING_COMMAND_LIMIT_BYTES], ["utf8", NODE_PENDING_COMMAND_LIMIT_BYTES + 1]] as const) {
        const empty = { ...base.command, instruction: "" };
        const paddingBytes = pendingBytes - Buffer.byteLength(JSON.stringify(empty));
        const chunk = encoding === "ascii" ? "x" : "é💡";
        const chunkBytes = Buffer.byteLength(chunk);
        const instruction = chunk.repeat(Math.floor(paddingBytes / chunkBytes)) + "x".repeat(paddingBytes % chunkBytes);
        const command = { ...base.command, instruction };
        const request = { ...base, command };
        const completed: WorkerCommand = {
          ...command, contractVersion: WORKER_COMMAND_VERSION,
          executionTarget: { identity, sessionBindingExecutorId: executorId },
          start: {
            ...command.start,
            identity: {
              ...command.start.identity,
              sessionBinding: { ...command.start.identity.sessionBinding, workerId, executorId },
            },
          },
        };
        result.push({
          id: `${operation}/${identityLength}/${encoding}/${pendingBytes}`,
          request, priorRoute, reservation, pendingBytes,
          expected: pendingBytes === NODE_PENDING_COMMAND_LIMIT_BYTES
            ? { command: completed, completedBytes: pendingBytes + 30 + workerId.length + executorId.length
              + 148 + identity.nodeId.length + identity.workerId.length + identity.executorKind.length + identity.runtimeProfile.ref.length + identity.runtimeProfile.version.length + executorId.length }
            : { error: "axis.node.body_too_large" },
        });
      }
    }
  }
  return result;
}

export async function buildNodeCancellationRequest(selection: WorkerSelection = workerSelectionFixture()): Promise<NodeCommandRequest> {
  const base = await buildNodeStartRequest(selection);
  return {
    ...base,
    command: {
      contractVersion: NODE_COMMAND_CONTRACT_VERSION,
      commandId: "command-cancel",
      command: "cancel",
      target: { runId: "run-prior" },
    },
  };
}

/** A start distinct from `base` in every durable identity a second first route in one journal needs. */
export function distinctNodeStart(base: NodeCommandRequest, suffix: string, sourceSessionId?: string): NodeCommandRequest {
  if (base.command.command !== "start") throw new Error("start fixture required");
  const sourceRef = base.command.start.identity.task.sourceRef;
  const changedSourceRef = sourceSessionId === undefined ? sourceRef : sourceRef.system === "linear"
    ? { ...sourceRef, agentSessionId: sourceSessionId }
    : { ...sourceRef, conversationId: sourceSessionId };
  const runId = `run-${suffix}`, sessionBindingId = `binding-${suffix}`;
  return {
    ...base,
    command: {
      ...base.command,
      commandId: `command-${suffix}`,
      start: {
        ...base.command.start,
        messageId: `message-${suffix}`,
        operationId: `operation-${suffix}`,
        identity: {
          ...base.command.start.identity,
          task: { ...base.command.start.identity.task, sourceRef: changedSourceRef },
          run: { ...base.command.start.identity.run, runId, sessionBindingId },
          sessionBinding: { ...base.command.start.identity.sessionBinding, runId, sessionBindingId },
        },
      },
    },
  };
}

/**
 * Drives a start through the real command journal to an accepted receipt -- the only state whose Run the
 * journal admits events for -- and returns the Worker start its events are built from.
 */
export async function acceptNodeStart(journal: SqliteNodeJournal, request: NodeCommandRequest, recordedAt: string): Promise<RunStartEnvelope> {
  await journal.claimCommandAndRoute({ request, resolveNewSessionReservation: () => nodeReservation, recordedAt });
  await journal.recordReceipt({ request, authenticatedNode: nodeAuthenticatedContext, receipt: buildNodeReceipt(request, "dispatching"), recordedAt });
  await journal.recordReceipt({ request, authenticatedNode: nodeAuthenticatedContext, receipt: await buildNodeAcceptedResponse(request), recordedAt });
  const command = await buildNodeCompletedCommand(request);
  if (command.command === "cancel" || command.command === "steer") throw new Error("start fixture required");
  return command.start;
}

export async function buildNodeCompletedCommand(request: NodeCommandRequest, priorRoute?: NodePriorRoute): Promise<WorkerCommand> {
  return completeNodeWorkerCommand(request, nodeReservation, priorRoute);
}

export function buildNodeWorkerResult(workerCommand: WorkerCommand): WorkerCommandResponse {
  if (workerCommand.command === "cancel") {
    return { contractVersion: workerCommand.contractVersion, commandId: workerCommand.commandId, status: "interrupt_requested", runId: workerCommand.target.runId };
  }
  if (workerCommand.command === "steer") {
    return { contractVersion: workerCommand.contractVersion, commandId: workerCommand.commandId, status: "steered", runId: workerCommand.target.runId };
  }
  return {
    contractVersion: workerCommand.contractVersion,
    commandId: workerCommand.commandId,
    status: "accepted",
    identity: nodeFixtureTarget.identity,
    runAccepted: buildNodeAcceptedEvent(workerCommand.start),
  };
}

export async function buildNodeExecutionEvidence(request: NodeCommandRequest, priorRoute?: NodePriorRoute): Promise<NodeExecutionEvidence> {
  const workerCommand = await buildNodeCompletedCommand(request, priorRoute);
  const { nativeSessionRef, ...sessionBinding } = workerCommand.command === "cancel" || workerCommand.command === "steer" ? priorRoute!.sessionBinding : workerCommand.start.identity.sessionBinding;
  if (nativeSessionRef !== undefined) throw new Error("Node routing evidence excludes native session handles.");
  return {
    ...nodeReservation,
    sessionBinding,
  };
}

export async function buildNodeAcceptedResponse(request: NodeCommandRequest, priorRoute?: NodePriorRoute) {
  const workerCommand = await buildNodeCompletedCommand(request, priorRoute);
  return {
    contractVersion: NODE_COMMAND_CONTRACT_VERSION,
    nodeRef: request.nodeRef,
    commandId: request.command.commandId,
    status: "accepted",
    evidence: await buildNodeExecutionEvidence(request, priorRoute),
    result: buildNodeWorkerResult(workerCommand),
  } as const;
}

export function buildNodeReceipt(
  request: NodeCommandRequest,
  status: "reserved" | "dispatching" | "ambiguous" | "rejected",
): NodeReceipt {
  const common = {
    contractVersion: NODE_COMMAND_CONTRACT_VERSION,
    nodeRef: request.nodeRef,
    commandId: request.command.commandId,
    reservation: nodeReservation,
  } as const;
  return status === "rejected"
    ? { ...common, status, failure: nodeKnownWorkerFailure("axis.worker.policy_forbidden") }
    : { ...common, status };
}

function timingExpected(
  overrides: Partial<NodeTimingExpectedState> = {},
): NodeTimingExpectedState {
  return {
    reservation: null,
    executionEvidence: null,
    bufferedEventIds: [],
    projectableEventIds: [],
    quarantinedEventIds: [],
    commandDisposition: "none",
    replayDisposition: null,
    eventIdentityAvailability: null,
    eventOriginDisposition: null,
    conflictCode: null,
    dispatchCount: 0,
    receiptQueryCount: 0,
    conflictCount: 0,
    ...overrides,
  };
}

async function buildTimingEvent(
  request: NodeCommandRequest,
  messageId: string,
): Promise<RunAcceptedEnvelope> {
  const command = await completeNodeWorkerCommand(request, nodeReservation);
  if (command.command === "cancel" || command.command === "steer") throw new Error("A timing event requires a start envelope.");
  return { ...buildNodeAcceptedEvent(command.start), messageId };
}

async function buildTimingWaitingEvent(
  request: NodeCommandRequest,
  messageId: string,
): Promise<RouterRunLifecycleEnvelope> {
  const command = await completeNodeWorkerCommand(request, nodeReservation);
  if (command.command === "cancel" || command.command === "steer") throw new Error("A timing waiting event requires a start envelope.");
  return { ...buildNodeWaitingEvent(command.start), messageId };
}

function buildDistinctStartForSameSource(
  request: NodeCommandRequest,
  suffix: string,
): NodeCommandRequest {
  if (request.command.command !== "start") throw new Error("A distinct timing start requires a start request.");
  const bindingId = `binding-${suffix}`;
  const runId = `run-${suffix}`;
  return {
    ...request,
    command: {
      ...request.command,
      commandId: `command-${suffix}`,
      instruction: `private instruction ${suffix}`,
      start: {
        ...request.command.start,
        messageId: `message-${suffix}`,
        identity: {
          ...request.command.start.identity,
          run: { ...request.command.start.identity.run, runId, sessionBindingId: bindingId },
          sessionBinding: { ...request.command.start.identity.sessionBinding, runId, sessionBindingId: bindingId },
        },
      },
    },
  };
}

async function buildConflictingAcceptedReceipt(request: NodeCommandRequest): Promise<unknown> {
  const accepted = await buildNodeAcceptedResponse(request);
  if (accepted.result.status !== "accepted") throw new Error("The accepted timing fixture is invalid.");
  const target = {
    ...accepted.evidence.target,
    identity: { ...accepted.evidence.target.identity, workerId: "worker-conflicting" },
  };
  return {
    ...accepted,
    evidence: {
      ...accepted.evidence,
      target,
      sessionBinding: { ...accepted.evidence.sessionBinding, workerId: target.identity.workerId },
    },
    result: { ...accepted.result, identity: target.identity },
  };
}

/**
 * Importable deterministic acceptance vectors for future Node/Router runtime
 * tickets. Expected snapshots are fixture-owned oracle data; consumers must
 * not derive them from their state-machine outputs.
 */
export async function buildNodeTimingVectors(): Promise<readonly NodeTimingVector[]> {
  const base = await buildNodeStartRequest();
  const accepted = await buildNodeAcceptedResponse(base);
  const earlyEventId = "timing-early-event";
  const missingOriginEventId = "timing-missing-origin-event";
  const wrongOriginEventId = "timing-wrong-origin-event";
  const waitingBeforeReceiptEventId = "timing-waiting-before-receipt";
  const waitingAfterReceiptEventId = "timing-waiting-after-receipt";
  const wrongOperationEventId = "timing-wrong-operation";
  const conflictingEventId = "timing-conflicting-event";
  const earlyEvent = await buildTimingEvent(base, earlyEventId);
  const wrongOperationEvent = { ...earlyEvent, messageId: wrongOperationEventId, operationId: "operation-unrelated" };
  const missingOriginEvent = { ...earlyEvent, messageId: missingOriginEventId };
  const wrongOriginEvent = { ...earlyEvent, messageId: wrongOriginEventId };
  const waitingBeforeReceiptEvent = await buildTimingWaitingEvent(base, waitingBeforeReceiptEventId);
  const waitingAfterReceiptEvent = await buildTimingWaitingEvent(base, waitingAfterReceiptEventId);
  const authenticatedEventOrigin: NodeTimingEventOrigin = { authentication: "authenticated", identity: nodeFixtureTarget.identity };
  const wrongEventOrigin: NodeTimingEventOrigin = {
    authentication: "authenticated",
    identity: { ...nodeFixtureTarget.identity, workerId: "worker-wrong-origin" },
  };
  const foreignBindingEventId = "timing-foreign-binding-event";
  const foreignBindingEvent = { ...earlyEvent, messageId: foreignBindingEventId, run: { ...earlyEvent.run, sessionBindingId: "binding-foreign" } };
  const conflictingStart = buildDistinctStartForSameSource(base, "conflicting");
  const conflictingEvent = await buildTimingEvent(conflictingStart, conflictingEventId);
  const conflictingReceipt = await buildConflictingAcceptedReceipt(base);
  const differentStart = buildDistinctStartForSameSource(base, "different-start");

  const dispatched = timingExpected({
    reservation: nodeReservation,
    commandDisposition: "dispatch",
    dispatchCount: 1,
  });
  const acceptedState = timingExpected({
    reservation: nodeReservation,
    executionEvidence: nodeExpectedExecutionEvidence,
    dispatchCount: 1,
  });

  return [
    {
      id: "early-event-then-correlated-acceptance",
      steps: [
        {
          label: "reserve and dispatch the first start",
          observation: { kind: "command", delivery: "initial", request: base, reservation: nodeReservation },
          expected: dispatched,
        },
        {
          label: "reject matching lineage when authenticated event origin is unavailable",
          observation: { kind: "event", event: missingOriginEvent, origin: { authentication: "unavailable" } },
          expected: timingExpected({
            reservation: nodeReservation,
            quarantinedEventIds: [missingOriginEventId],
            eventIdentityAvailability: "unavailable",
            eventOriginDisposition: "unavailable",
            conflictCode: "axis.node.target_forbidden",
            dispatchCount: 1,
            conflictCount: 1,
          }),
        },
        {
          label: "reject matching lineage from the wrong authenticated event origin",
          observation: { kind: "event", event: wrongOriginEvent, origin: wrongEventOrigin },
          expected: timingExpected({
            reservation: nodeReservation,
            quarantinedEventIds: [missingOriginEventId, wrongOriginEventId],
            eventIdentityAvailability: "unavailable",
            eventOriginDisposition: "mismatch",
            conflictCode: "axis.node.target_forbidden",
            dispatchCount: 1,
            conflictCount: 2,
          }),
        },
        {
          label: "buffer the early event without execution identity",
          observation: { kind: "event", event: earlyEvent, origin: authenticatedEventOrigin },
          expected: timingExpected({
            reservation: nodeReservation,
            bufferedEventIds: [earlyEventId],
            quarantinedEventIds: [missingOriginEventId, wrongOriginEventId],
            eventIdentityAvailability: "unavailable",
            eventOriginDisposition: "authenticated",
            dispatchCount: 1,
            conflictCount: 2,
          }),
        },
        {
          label: "buffer a supported waiting event with its authenticated origin",
          observation: { kind: "event", event: waitingBeforeReceiptEvent, origin: authenticatedEventOrigin },
          expected: timingExpected({
            reservation: nodeReservation,
            bufferedEventIds: [earlyEventId, waitingBeforeReceiptEventId],
            quarantinedEventIds: [missingOriginEventId, wrongOriginEventId],
            eventIdentityAvailability: "unavailable",
            eventOriginDisposition: "authenticated",
            dispatchCount: 1,
            conflictCount: 2,
          }),
        },
        {
          label: "buffer a same-Run event with a conflicting binding until evidence arrives",
          observation: { kind: "event", event: foreignBindingEvent, origin: authenticatedEventOrigin },
          expected: timingExpected({
            reservation: nodeReservation,
            bufferedEventIds: [earlyEventId, waitingBeforeReceiptEventId, foreignBindingEventId],
            quarantinedEventIds: [missingOriginEventId, wrongOriginEventId],
            eventIdentityAvailability: "unavailable",
            eventOriginDisposition: "authenticated",
            dispatchCount: 1,
            conflictCount: 2,
          }),
        },
        {
          label: "correlated acceptance records evidence and releases the event",
          observation: { kind: "receipt", delivery: "command-response", receipt: accepted },
          expected: timingExpected({
            reservation: nodeReservation,
            executionEvidence: nodeExpectedExecutionEvidence,
            projectableEventIds: [earlyEventId, waitingBeforeReceiptEventId],
            quarantinedEventIds: [missingOriginEventId, wrongOriginEventId, foreignBindingEventId],
            conflictCode: "axis.node.binding_conflict",
            conflictCount: 3,
            dispatchCount: 1,
          }),
        },
        {
          label: "project a supported waiting event observed after the receipt",
          observation: { kind: "event", event: waitingAfterReceiptEvent, origin: authenticatedEventOrigin },
          expected: timingExpected({
            reservation: nodeReservation,
            executionEvidence: nodeExpectedExecutionEvidence,
            projectableEventIds: [earlyEventId, waitingBeforeReceiptEventId, waitingAfterReceiptEventId],
            quarantinedEventIds: [missingOriginEventId, wrongOriginEventId, foreignBindingEventId],
            eventIdentityAvailability: "unavailable",
            eventOriginDisposition: "authenticated",
            conflictCount: 3,
            dispatchCount: 1,
          }),
        },
        {
          label: "reject an authenticated event belonging to a different operation",
          observation: { kind: "event", event: wrongOperationEvent, origin: authenticatedEventOrigin },
          expected: timingExpected({
            reservation: nodeReservation,
            executionEvidence: nodeExpectedExecutionEvidence,
            projectableEventIds: [earlyEventId, waitingBeforeReceiptEventId, waitingAfterReceiptEventId],
            quarantinedEventIds: [missingOriginEventId, wrongOriginEventId, foreignBindingEventId, wrongOperationEventId],
            eventIdentityAvailability: "unavailable",
            eventOriginDisposition: "authenticated",
            conflictCode: "axis.node.binding_conflict",
            conflictCount: 4,
            dispatchCount: 1,
          }),
        },
      ],
    },
    {
      id: "lost-acceptance-same-command-reconciliation",
      steps: [
        {
          label: "reserve and dispatch before the response is lost",
          observation: { kind: "command", delivery: "initial", request: base, reservation: nodeReservation },
          expected: dispatched,
        },
        {
          label: "replay the exact command by querying its receipt",
          observation: { kind: "command", delivery: "retry", request: structuredClone(base), reservation: nodeReservation },
          expected: timingExpected({
            reservation: nodeReservation,
            commandDisposition: "reconcile-receipt",
            replayDisposition: "replay",
            dispatchCount: 1,
            receiptQueryCount: 1,
          }),
        },
        {
          label: "reject recovered identity that contradicts the retained reservation",
          observation: { kind: "receipt", delivery: "receipt-query", receipt: conflictingReceipt },
          expected: timingExpected({
            reservation: nodeReservation,
            conflictCode: "axis.node.binding_conflict",
            dispatchCount: 1,
            receiptQueryCount: 1,
            conflictCount: 1,
          }),
        },
        {
          label: "recover the accepted receipt without another dispatch",
          observation: { kind: "receipt", delivery: "receipt-query", receipt: accepted },
          expected: timingExpected({
            reservation: nodeReservation,
            executionEvidence: nodeExpectedExecutionEvidence,
            dispatchCount: 1,
            receiptQueryCount: 1,
            conflictCount: 1,
          }),
        },
      ],
    },
    {
      id: "conflicting-late-receipt-and-event",
      steps: [
        {
          label: "reserve and dispatch the first start",
          observation: { kind: "command", delivery: "initial", request: base, reservation: nodeReservation },
          expected: dispatched,
        },
        {
          label: "establish the correlated execution identity",
          observation: { kind: "receipt", delivery: "command-response", receipt: accepted },
          expected: acceptedState,
        },
        {
          label: "reject a late receipt with a different Worker identity",
          observation: { kind: "receipt", delivery: "late", receipt: conflictingReceipt },
          expected: timingExpected({
            reservation: nodeReservation,
            executionEvidence: nodeExpectedExecutionEvidence,
            conflictCode: "axis.node.binding_conflict",
            dispatchCount: 1,
            conflictCount: 1,
          }),
        },
        {
          label: "quarantine a late event for a different Run identity",
          observation: { kind: "event", event: conflictingEvent, origin: authenticatedEventOrigin },
          expected: timingExpected({
            reservation: nodeReservation,
            executionEvidence: nodeExpectedExecutionEvidence,
            quarantinedEventIds: [conflictingEventId],
            eventIdentityAvailability: "unavailable",
            eventOriginDisposition: "authenticated",
            conflictCode: "axis.node.binding_conflict",
            dispatchCount: 1,
            conflictCount: 2,
          }),
        },
      ],
    },
    {
      id: "replay-versus-source-session-binding-conflict",
      steps: [
        {
          label: "reserve and dispatch the first start",
          observation: { kind: "command", delivery: "initial", request: base, reservation: nodeReservation },
          expected: dispatched,
        },
        {
          label: "reconcile an exact commandId replay",
          observation: { kind: "command", delivery: "retry", request: structuredClone(base), reservation: nodeReservation },
          expected: timingExpected({
            reservation: nodeReservation,
            commandDisposition: "reconcile-receipt",
            replayDisposition: "replay",
            dispatchCount: 1,
            receiptQueryCount: 1,
          }),
        },
        {
          label: "reject a different start for the same source session",
          observation: { kind: "command", delivery: "new-start", request: differentStart, reservation: nodeReservation },
          expected: timingExpected({
            reservation: nodeReservation,
            replayDisposition: "conflict",
            conflictCode: "axis.node.binding_conflict",
            dispatchCount: 1,
            receiptQueryCount: 1,
            conflictCount: 1,
          }),
        },
      ],
    },
  ];
}

/**
 * readCommand for a Worker double whose scenario does not converge ambiguous receipts.
 * Fail any call rather than inventing convergence evidence the test did not set up.
 */
export async function unexpectedWorkerCommandRead(): Promise<never> {
  throw new Error("unexpected Worker command read");
}
