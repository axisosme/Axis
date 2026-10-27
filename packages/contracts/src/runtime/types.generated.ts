// Generated from schemas/runtime/ by scripts/generate-runtime-types.mjs. Do not edit.

export const runtimeContractVersion = "1.0.0" as const;
export const runtimeSchemaFingerprint = "sha256:ecff7f5872e92c1a80202ff80a45c9cd537b31f9ca7ba7a7e16dff63213850e9" as const;
export type RuntimeContractVersion = typeof runtimeContractVersion;

export type ProviderNeutralId = string;
export type SourceRef = LinearSourceRef;
export interface LinearSourceRef {
  "system": "linear";
  "organizationId": ProviderNeutralId;
  "issueId": ProviderNeutralId;
  "agentSessionId"?: ProviderNeutralId;
}
export type RuntimeState = "pending" | "running" | "succeeded" | "failed" | "waiting_for_input";
export interface NativeSessionRef {
  "runtimeKind": ProviderNeutralId;
  "sessionId": ProviderNeutralId;
}
export interface RuntimeError {
  "code": ProviderNeutralId;
  "safeMessage": string;
}
export type ResultRef = string;
export type BoundaryComponent = "relay" | "router" | "worker" | "codex-adapter";
export interface MethodRef {
  "version": ProviderNeutralId;
  "method": ProviderNeutralId;
}
export interface Progress {
  "completed": number;
  "total"?: number;
  "safeMessage"?: string;
}
export interface RunProjection {
  "runId": ProviderNeutralId;
  "state": RuntimeState;
  "error"?: RuntimeError;
  "resultRef"?: ResultRef;
  "elicitation"?: StructuredElicitation;
}
export interface ElicitationOption {
  "optionId": ProviderNeutralId;
  "replyValue": string;
  "label": string;
  "description": string;
}
export interface ElicitationOther {
  "optionId": ProviderNeutralId;
  "replyValue": string;
}
export interface StructuredElicitation {
  "decisionId": ProviderNeutralId;
  "sourceEventId": ProviderNeutralId;
  "recommendedOptionId": ProviderNeutralId;
  "header": string;
  "question": string;
  "options": Array<ElicitationOption>;
  "other"?: ElicitationOther;
}

export interface Task {
  "contractVersion": RuntimeContractVersion;
  "taskId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "sourceRef": SourceRef;
}
export interface Attempt {
  "contractVersion": RuntimeContractVersion;
  "attemptId": ProviderNeutralId;
  "taskId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
}
export interface Run {
  "contractVersion": RuntimeContractVersion;
  "runId": ProviderNeutralId;
  "taskId": ProviderNeutralId;
  "attemptId": ProviderNeutralId;
  "sessionBindingId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "state": RuntimeState;
  "error"?: RuntimeError;
  "resultRef"?: ResultRef;
  "elicitation"?: StructuredElicitation;
  "priorRunId"?: ProviderNeutralId;
}
export interface SessionBinding {
  "contractVersion": RuntimeContractVersion;
  "sessionBindingId": ProviderNeutralId;
  "taskId": ProviderNeutralId;
  "attemptId": ProviderNeutralId;
  "runId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "workerId": ProviderNeutralId;
  "executorId": ProviderNeutralId;
  "nativeSessionRef"?: NativeSessionRef;
}
export interface RuntimeIdentityGraph {
  "contractVersion": RuntimeContractVersion;
  "task": Task;
  "attempt": Attempt;
  "run": Run;
  "sessionBinding": SessionBinding;
}
export interface RuntimeStartEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "occurredAt": string;
  "startIdempotencyKey": ProviderNeutralId;
  "identity": RuntimeIdentityGraph;
}
export interface TaskSubmitEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "occurredAt": string;
  "submitIdempotencyKey": ProviderNeutralId;
  "producer": "relay";
  "consumer": "router";
  "envelopeType": "task.submit";
  "task": Task;
  "priorRunId"?: ProviderNeutralId;
}
export interface RunStartEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "router";
  "consumer": "worker";
  "envelopeType": "run.start";
  "identity": RuntimeIdentityGraph;
}
export interface ExecutorStartEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "worker";
  "consumer": "codex-adapter";
  "envelopeType": "executor.start";
  "identity": RuntimeIdentityGraph;
  "capability": "codex.execute";
  "methodRef"?: MethodRef;
}
export interface ExecutorAcceptedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "codex-adapter";
  "consumer": "worker";
  "envelopeType": "executor.accepted";
  "identity": RuntimeIdentityGraph;
}
export interface RunAcceptedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "worker";
  "consumer": "router";
  "envelopeType": "run.accepted";
  "task": Task;
  "attempt": Attempt;
  "run": Run;
}
export interface TaskAcceptedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "router";
  "consumer": "relay";
  "envelopeType": "task.accepted";
  "task": Task;
  "run": RunProjection;
}
export interface ExecutorProgressedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "codex-adapter";
  "consumer": "worker";
  "envelopeType": "executor.progressed";
  "identity": RuntimeIdentityGraph;
  "progress": Progress;
}
export interface RunProgressedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "worker";
  "consumer": "router";
  "envelopeType": "run.progressed";
  "task": Task;
  "attempt": Attempt;
  "run": Run;
  "progress": Progress;
}
export interface TaskProgressedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "router";
  "consumer": "relay";
  "envelopeType": "task.progressed";
  "task": Task;
  "run": RunProjection;
  "progress": Progress;
}
export interface ExecutorSucceededEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "codex-adapter";
  "consumer": "worker";
  "envelopeType": "executor.succeeded";
  "identity": RuntimeIdentityGraph;
  "resultRef": ResultRef;
}
export interface RunSucceededEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "worker";
  "consumer": "router";
  "envelopeType": "run.succeeded";
  "task": Task;
  "attempt": Attempt;
  "run": Run;
  "resultRef": ResultRef;
}
export interface TaskSucceededEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "router";
  "consumer": "relay";
  "envelopeType": "task.succeeded";
  "task": Task;
  "run": RunProjection;
  "resultRef": ResultRef;
}
export interface ExecutorFailedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "codex-adapter";
  "consumer": "worker";
  "envelopeType": "executor.failed";
  "identity": RuntimeIdentityGraph;
  "error": RuntimeError;
}
export interface RunFailedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "worker";
  "consumer": "router";
  "envelopeType": "run.failed";
  "task": Task;
  "attempt": Attempt;
  "run": Run;
  "error": RuntimeError;
}
export interface TaskFailedEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "router";
  "consumer": "relay";
  "envelopeType": "task.failed";
  "task": Task;
  "run": RunProjection;
  "error": RuntimeError;
}
export type RuntimeBoundaryMessage = TaskSubmitEnvelope | RunStartEnvelope | ExecutorStartEnvelope | ExecutorAcceptedEnvelope | RunAcceptedEnvelope | TaskAcceptedEnvelope | ExecutorProgressedEnvelope | RunProgressedEnvelope | TaskProgressedEnvelope | ExecutorSucceededEnvelope | RunSucceededEnvelope | TaskSucceededEnvelope | ExecutorFailedEnvelope | RunFailedEnvelope | TaskFailedEnvelope | ExecutorWaitingForInputEnvelope | RunWaitingForInputEnvelope | TaskWaitingForInputEnvelope;
export interface ExecutorWaitingForInputEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "codex-adapter";
  "consumer": "worker";
  "envelopeType": "executor.waiting_for_input";
  "identity": RuntimeIdentityGraph;
  "elicitation": StructuredElicitation;
}
export interface RunWaitingForInputEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "worker";
  "consumer": "router";
  "envelopeType": "run.waiting_for_input";
  "task": Task;
  "attempt": Attempt;
  "run": Run;
  "elicitation": StructuredElicitation;
}
export interface TaskWaitingForInputEnvelope {
  "contractVersion": RuntimeContractVersion;
  "messageId": ProviderNeutralId;
  "correlationId": ProviderNeutralId;
  "operationId": ProviderNeutralId;
  "causationMessageId": ProviderNeutralId;
  "occurredAt": string;
  "producer": "router";
  "consumer": "relay";
  "envelopeType": "task.waiting_for_input";
  "task": Task;
  "run": RunProjection;
  "elicitation": StructuredElicitation;
}
