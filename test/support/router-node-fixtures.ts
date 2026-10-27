import { agentActivityContractVersion, type RunAgentResponseDelivery } from "@axisosme/axis-contracts/agent-activity";
import {
  validateRuntimeBoundaryMessage,
  type RunAcceptedEnvelope,
  type RunFailedEnvelope,
  type RunProgressedEnvelope,
  type RunStartEnvelope,
  type RunSucceededEnvelope,
} from "../../src/shared/runtime-boundary.js";

import { deterministicElicitationReplyValue, type StructuredElicitation } from "../../src/shared/elicitation.js";
import { WORKER_WAITING_EXTENSION_VERSION } from "../../src/shared/router-worker.js";
import { runtimeSourceSessionIdentity } from "../../src/shared/runtime-boundary.js";

export const nodeFixtureRef = { ref: "logical-node-route-1", version: "v1" } as const;
export const nodeFixtureId = "physical-node-1";
export const nodeFixtureTarget = {
  endpointRef: { ref: "endpoint-1", version: "v1" },
  identity: {
    nodeId: nodeFixtureId,
    workerId: "worker-1",
    executorKind: "codex",
    runtimeProfile: { ref: "profile-1", version: "v1" },
  },
  sessionBindingExecutorId: "executor-1",
} as const;

const nodeFixtureEventIds = {
  accepted: "worker-accepted",
  progressed: "worker-progressed",
  succeeded: "worker-succeeded",
  failed: "worker-failed",
  waiting: "worker-waiting-for-input",
  activityDelivery: "worker-activity-delivery",
} as const;

const fixtureContractVersion = "1.0.0" as const;
const nodeFixtureStart = {
  contractVersion: fixtureContractVersion, messageId: "m-run-start", correlationId: "c1", causationMessageId: "m-submit", occurredAt: "2026-08-25T00:00:01Z",
  producer: "router", consumer: "worker", envelopeType: "run.start", operationId: "operation-1",
  identity: {
    contractVersion: fixtureContractVersion,
    task: { contractVersion: fixtureContractVersion, taskId: "task-1", correlationId: "c1", sourceRef: { system: "linear", organizationId: "org-1", issueId: "issue-1", agentSessionId: "session-1" } },
    attempt: { contractVersion: fixtureContractVersion, attemptId: "attempt-1", taskId: "task-1", correlationId: "c1" },
    run: { contractVersion: fixtureContractVersion, runId: "run-1", taskId: "task-1", attemptId: "attempt-1", sessionBindingId: "binding-1", correlationId: "c1", state: "pending" },
    sessionBinding: { contractVersion: fixtureContractVersion, sessionBindingId: "binding-1", taskId: "task-1", attemptId: "attempt-1", runId: "run-1", correlationId: "c1", workerId: "worker-1", executorId: "executor-1" },
  },
} as const;

/** A Linear start at , the version `Router#createTask` mints. */
export async function buildWorkerBoundStartRequest(commandId = "command-1") {
  const validated = await validateRuntimeBoundaryMessage(nodeFixtureStart);
  if (!validated.valid || validated.value?.envelopeType !== "run.start") throw new Error("The Runtime start fixture is invalid.");
  const start = validated.value;
  return {
    nodeRef: nodeFixtureRef,
    command: {
      commandId,
      command: "start",
      start,
      instruction: "private instruction",
      workspacePolicyRef: null,
    },
  } as const;
}


export function buildNodeAcceptedEvent(start: RunStartEnvelope): RunAcceptedEnvelope {
  return {
    contractVersion: start.contractVersion,
    messageId: nodeFixtureEventIds.accepted,
    correlationId: start.correlationId,
    operationId: start.operationId,
    causationMessageId: start.messageId,
    occurredAt: "2026-08-26T00:00:01.000Z",
    producer: "worker",
    consumer: "router",
    envelopeType: "run.accepted",
    task: start.identity.task,
    attempt: start.identity.attempt,
    run: { ...start.identity.run, state: "running" },
  };
}

export function buildNodeProgressedEvent(start: RunStartEnvelope): RunProgressedEnvelope {
  return { ...buildNodeAcceptedEvent(start), messageId: nodeFixtureEventIds.progressed, occurredAt: "2026-08-26T00:00:02.000Z", envelopeType: "run.progressed", progress: { completed: 1, total: 1, safeMessage: "Working." } };
}

export function buildNodeSucceededEvent(start: RunStartEnvelope): RunSucceededEnvelope {
  const resultRef = `axis-evidence://${start.identity.run.runId}/result`;
  return { ...buildNodeAcceptedEvent(start), messageId: nodeFixtureEventIds.succeeded, occurredAt: "2026-08-26T00:00:03.000Z", envelopeType: "run.succeeded", run: { ...start.identity.run, state: "succeeded", resultRef }, resultRef };
}

export function buildNodeFailedEvent(start: RunStartEnvelope): RunFailedEnvelope {
  const error = { code: "execution_cancelled", safeMessage: "Execution was cancelled." };
  return { ...buildNodeAcceptedEvent(start), messageId: nodeFixtureEventIds.failed, occurredAt: "2026-08-26T00:00:03.000Z", envelopeType: "run.failed", run: { ...start.identity.run, state: "failed", error }, error };
}

export function buildNodeActivityDelivery(start: RunStartEnvelope): RunAgentResponseDelivery {
  // The start must be : the response delivery schema references the succeeded envelope.
  const envelope = { ...buildNodeSucceededEvent(start), messageId: nodeFixtureEventIds.activityDelivery, occurredAt: "2026-08-26T00:00:04.000Z" } as RunAgentResponseDelivery["envelope"];
  return {
    contractVersion: agentActivityContractVersion,
    deliveryType: "run_agent_response",
    envelope,
    agentActivity: {
      contractVersion: agentActivityContractVersion,
      activityId: "activity-1",
      taskId: start.identity.task.taskId,
      attemptId: start.identity.attempt.attemptId,
      runId: start.identity.run.runId,
      conversation: { system: "linear", agentSessionId: runtimeSourceSessionIdentity(start.identity.task.sourceRef).sessionId! },
      sourceEventId: "source-1",
      operationId: start.operationId,
      content: { type: "response", body: "Completed." },
      projectionStatus: "projected",
      resultRef: envelope.resultRef,
      truncated: false,
      truncations: [],
    },
  };
}

export function buildNodeWaitingEvent(start: RunStartEnvelope) {
  const decisionId = `axis-decision-${"a".repeat(48)}`;
  const optionId = `axis-option-${"c".repeat(48)}`;
  const otherId = `axis-option-${"d".repeat(48)}`;
  const elicitation: StructuredElicitation = {
    contractVersion: "1.0.0-fixed-choice",
    decisionId,
    sourceEventId: `axis-elicitation-${"b".repeat(48)}`,
    recommendedOptionId: optionId,
    header: "Choose mode",
    question: "Which mode?",
    options: [
      { optionId, replyValue: deterministicElicitationReplyValue(decisionId, optionId), label: "Fast", description: "Use fast mode." },
      { optionId: otherId, replyValue: deterministicElicitationReplyValue(decisionId, otherId), label: "Thorough", description: "Use thorough mode." },
    ],
  };
  return {
    ...buildNodeAcceptedEvent(start),
    messageId: nodeFixtureEventIds.waiting,
    occurredAt: "2026-08-26T00:00:03.000Z",
    contractVersion: WORKER_WAITING_EXTENSION_VERSION,
    envelopeType: "run.waiting_for_input",
    run: { ...start.identity.run, state: "waiting_for_input" },
    elicitation,
  } as const;
}
