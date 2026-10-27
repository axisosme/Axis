import type { ErrorObject } from "ajv";
import { createRuntimeSchemaRegistry, runtimeSchemaIds, runtimeValidatorFor, type RuntimeSchemaName } from "./registry.js";
import {
  runtimeContractVersion,
  type Attempt,
  type Run,
  type RuntimeIdentityGraph,
  type RuntimeStartEnvelope,
  type RuntimeBoundaryMessage,
  type RunStartEnvelope,
  type TaskSubmitEnvelope,
  type RuntimeState,
  type SessionBinding,
  type SourceRef,
  type Task,
  type StructuredElicitation,
} from "./types.generated.js";
import { memoizedRegistry } from "../registry-cache.js";

/** every validation in this module shares one compiled registry; see `registry-cache.ts`. */
const sharedRegistry = memoizedRegistry(createRuntimeSchemaRegistry);

const maxDiagnostics = 32;
const unsafeContent = /(?:\b(?:api[_-]?key|authorization|bearer|cookie|password|secret|token)\b|(?:^|[\s=:(\["'])~?\/|[A-Za-z]:\\|\bfile:|\bsk_(?:live|test)_[A-Za-z0-9]{16,}\b|\bghp_[A-Za-z0-9]{20,}\b|\bgithub_pat_[A-Za-z0-9_]{20,}\b|\bAKIA[0-9A-Z]{16}\b|\bxox[a-zA-Z]-[A-Za-z0-9-]{10,}\b|\bglpat-[A-Za-z0-9_-]{16,}\b|\b[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----)/i;

// waiting_for_input is terminal. Continuation submits an ordinary new Run under the same Task; it never mutates the terminal Run.
const transitions: Record<RuntimeState, readonly RuntimeState[]> = {
  pending: ["running"],
  running: ["succeeded", "failed", "waiting_for_input"],
  succeeded: [],
  failed: [],
  waiting_for_input: [],
};

export type RuntimeDiagnosticCode =
  | "schema_violation"
  | "identity_mismatch"
  | "illegal_state_transition"
  | "idempotency_violation"
  | "unsafe_error"
  | "unsupported_operation"
  | "causation_violation"
  | "ordering_violation"
  | "projection_violation";

export interface RuntimeDiagnostic {
  code: RuntimeDiagnosticCode;
  instancePath: string;
  schemaId: string | null;
  keyword: string | null;
  safeMessage: string;
}

export interface RuntimeValidationResult<T> {
  valid: boolean;
  value: T | null;
  diagnostics: RuntimeDiagnostic[];
}

export type RuntimeDuplicateStartResult = "distinct" | "duplicate" | "conflict";

export interface RuntimeDuplicateStartComparison {
  result: RuntimeDuplicateStartResult;
  diagnostics: RuntimeDiagnostic[];
}

function sortDiagnostics(diagnostics: RuntimeDiagnostic[]): RuntimeDiagnostic[] {
  return [...diagnostics]
    .sort((left, right) => left.instancePath.localeCompare(right.instancePath) || left.code.localeCompare(right.code) || left.safeMessage.localeCompare(right.safeMessage))
    .slice(0, maxDiagnostics);
}

function runtimeDiagnostic(
  code: RuntimeDiagnosticCode,
  safeMessage: string,
  instancePath: string,
  options: Partial<Pick<RuntimeDiagnostic, "schemaId" | "keyword">> = {},
): RuntimeDiagnostic {
  return { code, instancePath, schemaId: options.schemaId ?? null, keyword: options.keyword ?? null, safeMessage };
}

function schemaDiagnostics(errors: ErrorObject[] | null | undefined, schemaId: string): RuntimeDiagnostic[] {
  return [...(errors ?? [])]
    .sort((left, right) => left.instancePath.localeCompare(right.instancePath) || left.keyword.localeCompare(right.keyword))
    .slice(0, maxDiagnostics)
    .map((error) => runtimeDiagnostic("schema_violation", "The runtime document does not match the closed runtime schema.", error.instancePath, {
      schemaId,
      keyword: error.keyword,
    }));
}

function identityMismatch(safeMessage: string, instancePath: string): RuntimeDiagnostic {
  return runtimeDiagnostic("identity_mismatch", safeMessage, instancePath);
}

export function structuredElicitationDiagnostics(elicitation: StructuredElicitation, path = ""): RuntimeDiagnostic[] {
  const diagnostics: RuntimeDiagnostic[] = [];
  const optionIds = new Set<string>();
  const replyValues = new Set<string>();
  for (const [index, option] of elicitation.options.entries()) {
    if (optionIds.has(option.optionId)) diagnostics.push(identityMismatch("Every explicit elicitation option must have a unique optionId.", `${path}/options/${index}/optionId`));
    if (replyValues.has(option.replyValue)) diagnostics.push(identityMismatch("Every explicit elicitation option must have a unique replyValue.", `${path}/options/${index}/replyValue`));
    optionIds.add(option.optionId);
    replyValues.add(option.replyValue);
  }
  if (elicitation.recommendedOptionId !== elicitation.options[0]?.optionId) {
    diagnostics.push(identityMismatch("The recommended option must be the first explicit elicitation option.", `${path}/recommendedOptionId`));
  }
  if (elicitation.other) {
    if (optionIds.has(elicitation.other.optionId)) diagnostics.push(identityMismatch("The Other capability must have an optionId distinct from every explicit option.", `${path}/other/optionId`));
    if (replyValues.has(elicitation.other.replyValue)) diagnostics.push(identityMismatch("The Other capability must have a replyValue distinct from every explicit option.", `${path}/other/replyValue`));
    if (elicitation.recommendedOptionId === elicitation.other.optionId) diagnostics.push(identityMismatch("The Other capability cannot be recommended.", `${path}/recommendedOptionId`));
  }
  return diagnostics;
}

function runDiagnostics(run: Run, path = ""): RuntimeDiagnostic[] {
  const diagnostics: RuntimeDiagnostic[] = [];
  if (run.state === "succeeded" && (run.error || run.elicitation)) {
    diagnostics.push(identityMismatch("A succeeded run cannot carry an error or elicitation.", path));
  }
  if (run.state === "failed" && (!run.error || run.resultRef || run.elicitation)) {
    diagnostics.push(identityMismatch("A failed run requires an error and cannot carry a result reference or elicitation.", path));
  }
  if (run.state === "waiting_for_input" && (!run.elicitation || run.resultRef || run.error)) {
    diagnostics.push(identityMismatch("A waiting-for-input run requires a structured elicitation and cannot carry a result reference or error.", path));
  }
  if ((run.state === "pending" || run.state === "running") && (run.error || run.resultRef || run.elicitation)) {
    diagnostics.push(identityMismatch("A nonterminal run cannot carry a result reference, error, or elicitation.", path));
  }
  if (run.error && unsafeContent.test(run.error.safeMessage)) {
    diagnostics.push(runtimeDiagnostic("unsafe_error", "A runtime error message must not contain a filesystem path or secret-like value.", `${path}/error/safeMessage`));
  }
  if (run.priorRunId !== undefined && run.priorRunId === run.runId) {
    diagnostics.push(identityMismatch("A run cannot name itself as its own predecessor.", `${path}/priorRunId`));
  }
  if (run.elicitation) diagnostics.push(...structuredElicitationDiagnostics(run.elicitation, `${path}/elicitation`));
  return diagnostics;
}

function graphDiagnostics(graph: RuntimeIdentityGraph, envelopeCorrelationId?: string): RuntimeDiagnostic[] {
  const diagnostics: RuntimeDiagnostic[] = [];
  const { task, attempt, run, sessionBinding } = graph;
  const members = [graph.contractVersion, task.contractVersion, attempt.contractVersion, run.contractVersion, sessionBinding.contractVersion];
  if (members.some((version) => version !== runtimeContractVersion)) {
    diagnostics.push(identityMismatch("Every identity graph member must use the exact runtime contract version.", ""));
  }
  if (attempt.taskId !== task.taskId) diagnostics.push(identityMismatch("The attempt must reference the graph task.", "/attempt/taskId"));
  if (run.taskId !== task.taskId) diagnostics.push(identityMismatch("The run must reference the graph task.", "/run/taskId"));
  if (run.attemptId !== attempt.attemptId) diagnostics.push(identityMismatch("The run must reference the graph attempt.", "/run/attemptId"));
  if (run.sessionBindingId !== sessionBinding.sessionBindingId) diagnostics.push(identityMismatch("The run must reference the graph session binding.", "/run/sessionBindingId"));
  if (sessionBinding.taskId !== task.taskId) diagnostics.push(identityMismatch("The session binding must reference the graph task.", "/sessionBinding/taskId"));
  if (sessionBinding.attemptId !== attempt.attemptId) diagnostics.push(identityMismatch("The session binding must reference the graph attempt.", "/sessionBinding/attemptId"));
  if (sessionBinding.runId !== run.runId) diagnostics.push(identityMismatch("The session binding must reference the graph run.", "/sessionBinding/runId"));
  const correlations = [task.correlationId, attempt.correlationId, run.correlationId, sessionBinding.correlationId];
  if (new Set(correlations).size !== 1 || (envelopeCorrelationId !== undefined && correlations[0] !== envelopeCorrelationId)) {
    diagnostics.push(identityMismatch("Every runtime member must use the graph correlation identifier.", ""));
  }
  diagnostics.push(...runDiagnostics(run, "/run"));
  return diagnostics;
}

export async function validateRuntimeSchema<T>(name: RuntimeSchemaName, value: unknown): Promise<RuntimeValidationResult<T>> {
  const registry = await sharedRegistry();
  const validate = runtimeValidatorFor(registry, name);
  const diagnostics = validate(value) ? [] : schemaDiagnostics(validate.errors, runtimeSchemaIds[name]);
  const sorted = sortDiagnostics(diagnostics);
  return { valid: sorted.length === 0, value: sorted.length === 0 ? value as T : null, diagnostics: sorted };
}

export async function validateTask(value: unknown): Promise<RuntimeValidationResult<Task>> {
  return validateRuntimeSchema<Task>("task", value);
}

export async function validateAttempt(value: unknown): Promise<RuntimeValidationResult<Attempt>> {
  return validateRuntimeSchema<Attempt>("attempt", value);
}

export async function validateRun(value: unknown): Promise<RuntimeValidationResult<Run>> {
  const schemaResult = await validateRuntimeSchema<Run>("run", value);
  if (!schemaResult.value) return schemaResult;
  const diagnostics = sortDiagnostics(runDiagnostics(schemaResult.value));
  return { valid: diagnostics.length === 0, value: diagnostics.length === 0 ? schemaResult.value : null, diagnostics };
}

export async function validateSessionBinding(value: unknown): Promise<RuntimeValidationResult<SessionBinding>> {
  return validateRuntimeSchema<SessionBinding>("session-binding", value);
}

export async function validateIdentityGraph(value: unknown): Promise<RuntimeValidationResult<RuntimeIdentityGraph>> {
  const schemaResult = await validateRuntimeSchema<RuntimeIdentityGraph>("runtime-identity-graph", value);
  if (!schemaResult.value) return schemaResult;
  const diagnostics = sortDiagnostics(graphDiagnostics(schemaResult.value));
  return { valid: diagnostics.length === 0, value: diagnostics.length === 0 ? schemaResult.value : null, diagnostics };
}

export const validateRuntimeIdentityGraph = validateIdentityGraph;

export async function validateRuntimeStartEnvelope(value: unknown): Promise<RuntimeValidationResult<RuntimeStartEnvelope>> {
  const schemaResult = await validateRuntimeSchema<RuntimeStartEnvelope>("runtime-start-envelope", value);
  if (!schemaResult.value) return schemaResult;
  const diagnostics = graphDiagnostics(schemaResult.value.identity, schemaResult.value.correlationId);
  if (schemaResult.value.identity.run.state !== "pending" || schemaResult.value.identity.run.resultRef || schemaResult.value.identity.run.error) {
    diagnostics.push(identityMismatch("A start envelope requires a pending run without a result reference or error.", "/identity/run"));
  }
  const sorted = sortDiagnostics(diagnostics.map((item) => ({ ...item, instancePath: item.instancePath ? `/identity${item.instancePath}` : "/identity" })));
  return { valid: sorted.length === 0, value: sorted.length === 0 ? schemaResult.value : null, diagnostics: sorted };
}

export function validateRuntimeTransition(from: RuntimeState, to: RuntimeState): RuntimeValidationResult<{ from: RuntimeState; to: RuntimeState }> {
  const diagnostics = transitions[from].includes(to) ? [] : [runtimeDiagnostic("illegal_state_transition", "The runtime state transition is not allowed.", "/state")];
  return { valid: diagnostics.length === 0, value: diagnostics.length === 0 ? { from, to } : null, diagnostics };
}

export function compareRuntimeStarts(left: RuntimeStartEnvelope, right: RuntimeStartEnvelope): RuntimeDuplicateStartComparison {
  if (left.startIdempotencyKey !== right.startIdempotencyKey) return { result: "distinct", diagnostics: [] };
  const sameTarget = left.operationId === right.operationId &&
    left.identity.task.taskId === right.identity.task.taskId &&
    left.identity.run.runId === right.identity.run.runId;
  if (sameTarget) return { result: "duplicate", diagnostics: [] };
  return {
    result: "conflict",
    diagnostics: [runtimeDiagnostic("idempotency_violation", "A start idempotency key cannot target a different operation, task, or run.", "/startIdempotencyKey")],
  };
}

// Same-key equality includes priorRunId, including presence and absence. A changed predecessor under a
// reused key is a content conflict.
export function compareRuntimeTaskSubmits(left: TaskSubmitEnvelope, right: TaskSubmitEnvelope): RuntimeDuplicateStartComparison {
  if (left.submitIdempotencyKey !== right.submitIdempotencyKey) return { result: "distinct", diagnostics: [] };
  if (sameTask(left.task, right.task) && left.correlationId === right.correlationId &&
    left.operationId === right.operationId && left.priorRunId === right.priorRunId) {
    return { result: "duplicate", diagnostics: [] };
  }
  return {
    result: "conflict",
    diagnostics: [runtimeDiagnostic("idempotency_violation", "A submit idempotency key cannot target a different operation, task identity, correlation, or predecessor Run.", "/submitIdempotencyKey")],
  };
}

export interface RuntimeTaskSubmitExisting {
  originalSubmit: TaskSubmitEnvelope;
  originalRunStart: RunStartEnvelope;
}

// Continuation admission uses caller-supplied durable evidence for predecessor
// state and Task-family membership. The caller checks for forks in the same
// atomic Task-scoped operation that resolves `priorRun`; Router#claim performs
// both lookups inside one SQLite transaction.
export interface RuntimeTaskContinuationContext {
  hasPriorRuns: boolean;
  priorRun?: { runId: string; state: RuntimeState; task: Task };
}

export type RuntimeTaskSubmitDecision = "new" | "duplicate" | "conflict" | "continuation_conflict";

export interface RuntimeTaskSubmitDecisionResult {
  decision: RuntimeTaskSubmitDecision;
  emitRunStart: boolean;
  identity: RuntimeIdentityGraph | null;
  diagnostics: RuntimeDiagnostic[];
}

function taskSubmitConflict(): RuntimeTaskSubmitDecisionResult {
  return {
    decision: "conflict",
    emitRunStart: false,
    identity: null,
    diagnostics: [runtimeDiagnostic("idempotency_violation", "A duplicate task submit must retain the original operation, task identity, and coherent RunStart graph.", "/submitIdempotencyKey")],
  };
}

function continuationConflict(safeMessage: string, instancePath: string): RuntimeTaskSubmitDecisionResult {
  return {
    decision: "continuation_conflict",
    emitRunStart: false,
    identity: null,
    diagnostics: [runtimeDiagnostic("idempotency_violation", safeMessage, instancePath)],
  };
}

const terminalRunStates: readonly RuntimeState[] = ["succeeded", "failed", "waiting_for_input"];

// For a new submission, validate predecessor ownership and terminal state and reject a first-Run bypass.
// Same-key replays use their retained immutable identity.
function checkContinuationAdmission(incoming: TaskSubmitEnvelope, continuation: RuntimeTaskContinuationContext): RuntimeTaskSubmitDecisionResult | null {
  if (incoming.priorRunId !== undefined) {
    const priorRun = continuation.priorRun;
    if (!priorRun || priorRun.runId !== incoming.priorRunId) {
      return continuationConflict("A continuation submit's priorRunId must reference a resolved predecessor Run.", "/priorRunId");
    }
    if (!terminalRunStates.includes(priorRun.state)) {
      return continuationConflict("A continuation submit's predecessor Run must be terminal.", "/priorRunId");
    }
    if (!sameTask(priorRun.task, incoming.task)) {
      return continuationConflict("A continuation submit's predecessor Run must belong to the same Task.", "/priorRunId");
    }
  } else if (continuation.hasPriorRuns) {
    return continuationConflict("A Task with existing Run history requires priorRunId on every subsequent submit.", "/priorRunId");
  }
  return null;
}

export async function decideRuntimeTaskSubmit(
  incoming: TaskSubmitEnvelope,
  existing: RuntimeTaskSubmitExisting | null,
  continuation: RuntimeTaskContinuationContext,
): Promise<RuntimeTaskSubmitDecisionResult> {
  try {
    const validatedIncoming = await validateRuntimeBoundaryMessage(incoming);
    if (!validatedIncoming.value || validatedIncoming.value.envelopeType !== "task.submit") return taskSubmitConflict();

    if (existing !== null) {
      if (typeof existing !== "object") return taskSubmitConflict();
      const { originalSubmit, originalRunStart } = existing;
      const [submitted, started] = await Promise.all([
        validateRuntimeBoundaryMessage(originalSubmit),
        validateRuntimeBoundaryMessage(originalRunStart),
      ]);
      if (!submitted.value || !started.value || submitted.value.envelopeType !== "task.submit" || started.value.envelopeType !== "run.start") return taskSubmitConflict();
      if (started.value.causationMessageId !== submitted.value.messageId || started.value.operationId !== submitted.value.operationId ||
        started.value.correlationId !== submitted.value.correlationId || !sameTask(submitted.value.task, started.value.identity.task) ||
        started.value.identity.task.correlationId !== submitted.value.correlationId ||
        // The retained submit/Run pair must agree on priorRunId, including presence and absence, before it
        // can establish a duplicate identity.
        submitted.value.priorRunId !== started.value.identity.run.priorRunId) return taskSubmitConflict();

      const comparison = compareRuntimeTaskSubmits(validatedIncoming.value, submitted.value);
      // "duplicate" is an ordinary retry of the FIRST submission that
      // already produced a Run -- resolved here, before any continuation
      // admission check, so it is never misclassified as
      // continuation_conflict. "conflict" (including a same-key retry with
      // a changed priorRunId, which compareRuntimeTaskSubmits already
      // checks) is likewise resolved here, not treated as a continuation
      // provenance question.
      if (comparison.result === "duplicate") return { decision: "duplicate", emitRunStart: false, identity: started.value.identity, diagnostics: [] };
      if (comparison.result === "conflict") return taskSubmitConflict();
      // comparison.result === "distinct": the caller's key lookup found a
      // record, but it turns out to target a different submission (a
      // different submitIdempotencyKey than this one) -- treat exactly
      // like existing === null below: a genuinely new submission subject
      // to continuation admission.
    }

    const admissionConflict = checkContinuationAdmission(validatedIncoming.value, continuation);
    if (admissionConflict) return admissionConflict;
    return { decision: "new", emitRunStart: true, identity: null, diagnostics: [] };
  } catch {
    return taskSubmitConflict();
  }
}

export type RuntimeUnsupportedOperation =
  | "continue" | "cancel" | "approval" | "reconcile" | "observe" | "discovery" | "retry" | "resume" | "alternate-executor";

const unsupportedOperations: readonly RuntimeUnsupportedOperation[] = [
  "continue", "cancel", "approval", "reconcile", "observe", "discovery", "retry", "resume", "alternate-executor",
];

export function validateRuntimeOperation(operation: string): RuntimeValidationResult<never> {
  const knownUnsupported = unsupportedOperations.includes(operation as RuntimeUnsupportedOperation);
  return {
    valid: false,
    value: null,
    diagnostics: [runtimeDiagnostic("unsupported_operation", knownUnsupported
      ? "The requested runtime operation is explicitly outside this boundary contract."
      : "The requested runtime operation is not defined by this boundary contract.", "/operation")],
  };
}

function messageTask(message: RuntimeBoundaryMessage): Task {
  return "identity" in message ? message.identity.task : message.task;
}

function messageRun(message: Exclude<RuntimeBoundaryMessage, TaskSubmitEnvelope>): Run | { runId: string; state: RuntimeState; error?: { code: string; safeMessage: string }; resultRef?: string; elicitation?: StructuredElicitation } {
  return "identity" in message ? message.identity.run : message.run;
}

function messageRunId(message: RuntimeBoundaryMessage): string | undefined {
  if (message.envelopeType === "task.submit") return undefined;
  return messageRun(message).runId;
}

function sameSourceRef(left: SourceRef, right: SourceRef): boolean {
  return left.system === right.system && left.organizationId === right.organizationId && left.issueId === right.issueId && left.agentSessionId === right.agentSessionId;
}

function sameTask(left: Task, right: Task): boolean {
  return left.contractVersion === right.contractVersion && left.taskId === right.taskId && left.correlationId === right.correlationId &&
    sameSourceRef(left.sourceRef, right.sourceRef);
}

function sameAttempt(left: Attempt, right: Attempt): boolean {
  return left.contractVersion === right.contractVersion && left.attemptId === right.attemptId && left.taskId === right.taskId && left.correlationId === right.correlationId;
}

// priorRunId is part of immutable Run identity alongside runId, taskId, attemptId, and sessionBindingId.
function sameRunIdentity(left: Run, right: Run): boolean {
  return left.contractVersion === right.contractVersion && left.runId === right.runId && left.taskId === right.taskId &&
    left.attemptId === right.attemptId && left.sessionBindingId === right.sessionBindingId && left.correlationId === right.correlationId &&
    left.priorRunId === right.priorRunId;
}

function sameBindingCore(left: SessionBinding, right: SessionBinding): boolean {
  return left.contractVersion === right.contractVersion && left.sessionBindingId === right.sessionBindingId && left.taskId === right.taskId &&
    left.attemptId === right.attemptId && left.runId === right.runId && left.correlationId === right.correlationId &&
    left.workerId === right.workerId && left.executorId === right.executorId;
}

function sameNativeSession(left: NonNullable<SessionBinding["nativeSessionRef"]>, right: NonNullable<SessionBinding["nativeSessionRef"]>): boolean {
  return left.runtimeKind === right.runtimeKind && left.sessionId === right.sessionId;
}

// Compare complete elicitation content; decisionId identifies the decision but does not establish its question and options.
export function sameElicitationContent(left: StructuredElicitation, right: StructuredElicitation): boolean {
  if (left.decisionId !== right.decisionId || left.sourceEventId !== right.sourceEventId ||
    left.recommendedOptionId !== right.recommendedOptionId || left.header !== right.header || left.question !== right.question ||
    left.options.length !== right.options.length || left.other?.optionId !== right.other?.optionId ||
    left.other?.replyValue !== right.other?.replyValue) return false;
  return left.options.every((option, index) => {
    const other = right.options[index];
    return other !== undefined && option.optionId === other.optionId && option.replyValue === other.replyValue &&
      option.label === other.label && option.description === other.description;
  });
}

function sameRunObservation(
  left: { state: RuntimeState; error?: { code: string; safeMessage: string }; resultRef?: string; elicitation?: StructuredElicitation },
  right: { state: RuntimeState; error?: { code: string; safeMessage: string }; resultRef?: string; elicitation?: StructuredElicitation },
): boolean {
  if (left.state !== right.state || left.resultRef !== right.resultRef || left.error?.code !== right.error?.code || left.error?.safeMessage !== right.error?.safeMessage) return false;
  if (left.elicitation === undefined || right.elicitation === undefined) return left.elicitation === right.elicitation;
  return sameElicitationContent(left.elicitation, right.elicitation);
}

function boundaryDiagnostics(message: RuntimeBoundaryMessage): RuntimeDiagnostic[] {
  const diagnostics: RuntimeDiagnostic[] = [];
  const task = messageTask(message);
  if (task.correlationId !== message.correlationId) diagnostics.push(identityMismatch("The boundary message must use the Task correlation identifier.", "/correlationId"));
  if (message.envelopeType === "task.submit") return diagnostics;
  const run = messageRun(message);
  if ("identity" in message) {
    diagnostics.push(...graphDiagnostics(message.identity, message.correlationId).map((item) => ({ ...item, instancePath: item.instancePath ? `/identity${item.instancePath}` : "/identity" })));
  } else {
    if ("attempt" in message && message.attempt.taskId !== task.taskId) diagnostics.push(identityMismatch("The projected attempt must reference the message task.", "/attempt/taskId"));
    if ("attempt" in message && message.attempt.correlationId !== message.correlationId) diagnostics.push(identityMismatch("The projected attempt must use the message correlation identifier.", "/attempt/correlationId"));
    if ("attempt" in message && "attemptId" in run && run.attemptId !== message.attempt.attemptId) diagnostics.push(identityMismatch("The projected run must reference the projected attempt.", "/run/attemptId"));
    if ("taskId" in run && run.taskId !== task.taskId) diagnostics.push(identityMismatch("The projected run must reference the message task.", "/run/taskId"));
    if ("correlationId" in run && run.correlationId !== message.correlationId) diagnostics.push(identityMismatch("The projected run must use the message correlation identifier.", "/run/correlationId"));
    diagnostics.push(...runDiagnostics(run as Run, "/run"));
  }
  if ("error" in message && message.error && unsafeContent.test(message.error.safeMessage)) diagnostics.push(runtimeDiagnostic("unsafe_error", "A runtime error message must not contain a filesystem path or secret-like value.", "/error/safeMessage"));
  if ("progress" in message) {
    if (message.progress.total !== undefined && message.progress.completed > message.progress.total) diagnostics.push(runtimeDiagnostic("projection_violation", "Progress completed cannot exceed its declared total.", "/progress/completed"));
    if (message.progress.safeMessage && unsafeContent.test(message.progress.safeMessage)) diagnostics.push(runtimeDiagnostic("unsafe_error", "A progress message must not contain a filesystem path or secret-like value.", "/progress/safeMessage"));
  }
  const state = run.state;
  if ((message.envelopeType === "run.start" || message.envelopeType === "executor.start") && (state !== "pending" || run.error || run.resultRef || run.elicitation)) {
    diagnostics.push(runtimeDiagnostic("projection_violation", "A boundary start message requires a pending run without a result reference, error, or elicitation.", "/run"));
  }
  if (message.envelopeType.endsWith(".succeeded") && (state !== "succeeded" || !run.resultRef)) diagnostics.push(runtimeDiagnostic("projection_violation", "A succeeded boundary message requires a succeeded run with a result reference.", "/run"));
  if (message.envelopeType.endsWith(".failed") && (state !== "failed" || !run.error)) diagnostics.push(runtimeDiagnostic("projection_violation", "A failed boundary message requires a failed run with a safe error.", "/run"));
  if (message.envelopeType.endsWith(".waiting_for_input") && (state !== "waiting_for_input" || !run.elicitation)) diagnostics.push(runtimeDiagnostic("projection_violation", "A waiting-for-input boundary message requires a waiting-for-input run with a structured elicitation.", "/run"));
  if ((message.envelopeType.endsWith(".accepted") || message.envelopeType.endsWith(".progressed")) && !["pending", "running"].includes(state)) diagnostics.push(runtimeDiagnostic("projection_violation", "An accepted or progressed boundary message cannot project a terminal run.", "/run/state"));
  if ("resultRef" in message && message.envelopeType.endsWith(".succeeded") && message.resultRef !== run.resultRef) diagnostics.push(runtimeDiagnostic("projection_violation", "A succeeded boundary result reference must match the projected run.", "/resultRef"));
  if ("error" in message && message.envelopeType.endsWith(".failed") && (message.error.code !== run.error?.code || message.error.safeMessage !== run.error?.safeMessage)) diagnostics.push(runtimeDiagnostic("projection_violation", "A failed boundary error must match the projected run.", "/error"));
  if ("elicitation" in message && message.envelopeType.endsWith(".waiting_for_input") && (run.elicitation === undefined || !sameElicitationContent(message.elicitation, run.elicitation))) diagnostics.push(runtimeDiagnostic("projection_violation", "A waiting-for-input boundary elicitation must match the projected run.", "/elicitation"));
  if ("elicitation" in message && message.envelopeType.endsWith(".waiting_for_input")) diagnostics.push(...structuredElicitationDiagnostics(message.elicitation, "/elicitation"));
  return diagnostics;
}

export async function validateRuntimeBoundaryMessage(value: unknown): Promise<RuntimeValidationResult<RuntimeBoundaryMessage>> {
  const schemaResult = await validateRuntimeSchema<RuntimeBoundaryMessage>("runtime-boundary-message", value);
  if (!schemaResult.value) return schemaResult;
  const diagnostics = sortDiagnostics(boundaryDiagnostics(schemaResult.value));
  return { valid: diagnostics.length === 0, value: diagnostics.length === 0 ? schemaResult.value : null, diagnostics };
}

// Executor, Run, and Task waiting envelopes mirror the causal shape of
// their .failed counterparts exactly (same predecessor set at each hop).
const causationTypes: Record<RuntimeBoundaryMessage["envelopeType"], readonly RuntimeBoundaryMessage["envelopeType"][]> = {
  "task.submit": [],
  "run.start": ["task.submit"],
  "executor.start": ["run.start"],
  "executor.accepted": ["executor.start"],
  "run.accepted": ["executor.accepted"],
  "task.accepted": ["run.accepted"],
  "executor.progressed": ["executor.accepted", "executor.progressed"],
  "run.progressed": ["executor.progressed"],
  "task.progressed": ["run.progressed"],
  "executor.succeeded": ["executor.accepted", "executor.progressed"],
  "run.succeeded": ["executor.succeeded"],
  "task.succeeded": ["run.succeeded"],
  "executor.failed": ["executor.accepted", "executor.progressed"],
  "run.failed": ["executor.failed"],
  "task.failed": ["run.failed", "task.submit"],
  "executor.waiting_for_input": ["executor.accepted", "executor.progressed"],
  "run.waiting_for_input": ["executor.waiting_for_input"],
  "task.waiting_for_input": ["run.waiting_for_input"],
};

// Terminal outcomes (succeeded/failed/waiting_for_input) share phase 3 at
// each boundary.
const phaseForType: Record<RuntimeBoundaryMessage["envelopeType"], number> = {
  "task.submit": 0, "run.start": 0, "executor.start": 0,
  "executor.accepted": 1, "run.accepted": 1, "task.accepted": 1,
  "executor.progressed": 2, "run.progressed": 2, "task.progressed": 2,
  "executor.succeeded": 3, "run.succeeded": 3, "task.succeeded": 3,
  "executor.failed": 3, "run.failed": 3, "task.failed": 3,
  "executor.waiting_for_input": 3, "run.waiting_for_input": 3, "task.waiting_for_input": 3,
};

interface RunTraceState {
  runIdentity?: Run;
  attemptIdentity?: Attempt;
  bindingIdentity?: SessionBinding;
  phaseByBoundary: Map<string, number>;
}

// Trace-wide Task, operation, and correlation identity is stable. Each Run
// has independent identity, binding, and phase progression.
export async function validateRuntimeBoundaryTrace(messages: unknown): Promise<RuntimeValidationResult<readonly RuntimeBoundaryMessage[]>> {
  if (!Array.isArray(messages)) {
    const diagnostics = [runtimeDiagnostic("schema_violation", "A runtime boundary trace must be an array of closed boundary messages.", "")];
    return { valid: false, value: null, diagnostics };
  }
  const diagnostics: RuntimeDiagnostic[] = [];
  const validated: RuntimeBoundaryMessage[] = [];
  for (const [index, message] of messages.entries()) {
    try {
      const result = await validateRuntimeBoundaryMessage(message);
      if (result.value) {
        try {
          validated.push(structuredClone(result.value));
        } catch {
          diagnostics.push(runtimeDiagnostic("schema_violation", "The runtime boundary message could not be safely validated.", `/${index}`));
        }
      } else diagnostics.push(...result.diagnostics.map((item) => ({ ...item, instancePath: item.instancePath ? `/${index}${item.instancePath}` : `/${index}` })));
    } catch {
      diagnostics.push(runtimeDiagnostic("schema_violation", "The runtime boundary message could not be safely validated.", `/${index}`));
    }
  }
  const schemaDiagnostics = sortDiagnostics(diagnostics);
  if (schemaDiagnostics.length > 0) return { valid: false, value: null, diagnostics: schemaDiagnostics };
  const semanticDiagnostics: RuntimeDiagnostic[] = [];
  const byId = new Map<string, RuntimeBoundaryMessage>();
  const runStates = new Map<string, RunTraceState>();
  const runByRunId = new Map<string, Run>();
  let taskIdentity: Task | undefined;
  let nativeSession: SessionBinding["nativeSessionRef"] | undefined;
  let nativeSessionIntroduced = false;
  let operationId: string | undefined;
  for (const [index, message] of validated.entries()) {
    const path = `/${index}`;
    if (byId.has(message.messageId)) semanticDiagnostics.push(runtimeDiagnostic("ordering_violation", "Every boundary hop must have a new message identifier.", `${path}/messageId`));
    const task = messageTask(message);
    const run = message.envelopeType === "task.submit" ? undefined : messageRun(message);
    const runId = messageRunId(message);
    if (operationId === undefined) operationId = message.operationId;
    else if (message.operationId !== operationId) semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary trace must retain one operation identifier.", `${path}/operationId`));
    if (!taskIdentity) taskIdentity = task;
    else if (!sameTask(taskIdentity, task) || message.correlationId !== taskIdentity.correlationId) semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary trace must retain the exact Task identity and correlation.", path));

    const runState = runId !== undefined ? runStates.get(runId) ?? { phaseByBoundary: new Map<string, number>() } : undefined;
    if (runState && !runStates.has(runId as string)) runStates.set(runId as string, runState);

    if (message.envelopeType !== "task.submit" && runState) {
      if (!runState.runIdentity) runState.runIdentity = run as Run;
      else if (!run || (("taskId" in run) ? !sameRunIdentity(runState.runIdentity, run as Run) : run.runId !== runState.runIdentity.runId)) {
        semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary trace must retain immutable Run identity within one Run.", `${path}/run`));
      }
      if ("taskId" in (run as Run)) runByRunId.set((run as Run).runId, run as Run);
    }
    if ("attempt" in message) {
      if (!runState) {
        semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary message projecting an Attempt must also project a Run.", `${path}/attempt`));
      } else {
        if (!runState.attemptIdentity) runState.attemptIdentity = message.attempt;
        else if (!sameAttempt(runState.attemptIdentity, message.attempt)) semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary trace must retain immutable Attempt identity within one Run.", `${path}/attempt`));
      }
    }
    if ("identity" in message) {
      const binding = message.identity.sessionBinding;
      if (runState) {
        if (!runState.attemptIdentity) runState.attemptIdentity = message.identity.attempt;
        else if (!sameAttempt(runState.attemptIdentity, message.identity.attempt)) semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary trace must retain immutable Attempt identity within one Run.", `${path}/identity/attempt`));
        if (!runState.bindingIdentity) runState.bindingIdentity = binding;
        else if (!sameBindingCore(runState.bindingIdentity, binding)) semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary trace must retain immutable SessionBinding identity within one Run.", `${path}/identity/sessionBinding`));
      }
      if (binding.nativeSessionRef) {
        if (nativeSessionIntroduced && (!nativeSession || !sameNativeSession(nativeSession, binding.nativeSessionRef))) semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A native session reference may be introduced once but cannot change.", `${path}/identity/sessionBinding/nativeSessionRef`));
        nativeSession = binding.nativeSessionRef;
        nativeSessionIntroduced = true;
      } else if (nativeSessionIntroduced) {
        semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A native session reference cannot disappear after it is introduced.", `${path}/identity/sessionBinding/nativeSessionRef`));
      }
    }

    // Continuation-chain admission invariants, checked once per
    // Run at its run.start / task.submit boundary (message carrying priorRunId).
    if (message.envelopeType === "task.submit" && message.priorRunId !== undefined) {
      const priorRun = runByRunId.get(message.priorRunId);
      if (!priorRun) semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A continuation submit's priorRunId must reference a Run already observed in this trace.", `${path}/priorRunId`));
      else {
        if (!terminalRunStates.includes(priorRun.state)) semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A continuation submit's predecessor Run must be terminal.", `${path}/priorRunId`));
        if (priorRun.taskId !== task.taskId) semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A continuation submit's predecessor Run must belong to the same Task.", `${path}/priorRunId`));
      }
    }
    if (message.envelopeType === "run.start") {
      // Compare predecessor identity unconditionally, including presence and absence, between the Run and its causing submit.
      const causingSubmit = byId.get(message.causationMessageId);
      if (causingSubmit && causingSubmit.envelopeType === "task.submit" && causingSubmit.priorRunId !== message.identity.run.priorRunId) {
        semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A continuation Run's priorRunId must match its causing task.submit's priorRunId.", `${path}/identity/run/priorRunId`));
      }
      // Self-link (a Run naming itself as its own predecessor) is already
      // rejected one layer down, by validateRun's schema-level
      // priorRunId !== runId check -- no second check needed here.
    }

    const expected = causationTypes[message.envelopeType];
    if (expected.length === 0) {
      if ("causationMessageId" in message) semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A task submit is the root boundary message and cannot have a cause.", `${path}/causationMessageId`));
    } else if (message.envelopeType !== "task.submit") {
      const cause = byId.get(message.causationMessageId);
      if (!cause || !expected.includes(cause.envelopeType)) semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A boundary message must causally follow its permitted preceding hop.", `${path}/causationMessageId`));
      else {
        const causedTask = messageTask(cause);
        if (message.operationId !== cause.operationId) semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A boundary hop must preserve the operation identifier from its cause.", `${path}/operationId`));
        if (!sameTask(task, causedTask)) semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary hop must preserve the exact Task projection from its cause.", `${path}/task`));
        const causedRun = cause.envelopeType === "task.submit" ? undefined : messageRun(cause);
        const lifecycleAdvance = message.envelopeType === "executor.progressed" || message.envelopeType === "executor.succeeded" || message.envelopeType === "executor.failed" || message.envelopeType === "executor.waiting_for_input";
        // Every causal hop belongs to the same Run, independently of whether the hop advances lifecycle state.
        if (run && causedRun && run.runId !== causedRun.runId) {
          semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A boundary hop's cause must belong to the same Run.", `${path}/causationMessageId`));
        } else if (run && causedRun && !lifecycleAdvance && !sameRunObservation(run, causedRun)) {
          semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A non-lifecycle boundary hop must preserve the Run state and outcome projection.", `${path}/run`));
        }
        if ((message.envelopeType === "run.progressed" || message.envelopeType === "task.progressed") && "progress" in message && "progress" in cause &&
          (message.progress.completed !== cause.progress.completed || message.progress.total !== cause.progress.total || message.progress.safeMessage !== cause.progress.safeMessage)) {
          semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A propagated progress projection must match its causing boundary message.", `${path}/progress`));
        }
        if ((message.envelopeType === "run.succeeded" || message.envelopeType === "task.succeeded") && "resultRef" in message && "resultRef" in cause && message.resultRef !== cause.resultRef) {
          semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A propagated result reference must match its causing boundary message.", `${path}/resultRef`));
        }
        if ((message.envelopeType === "run.failed" || message.envelopeType === "task.failed") && "error" in message && "error" in cause &&
          (message.error.code !== cause.error.code || message.error.safeMessage !== cause.error.safeMessage)) {
          semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A propagated error must match its causing boundary message.", `${path}/error`));
        }
        if ((message.envelopeType === "run.waiting_for_input" || message.envelopeType === "task.waiting_for_input") && "elicitation" in message && "elicitation" in cause &&
          !sameElicitationContent(message.elicitation, cause.elicitation)) {
          semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A propagated elicitation must match its causing boundary message.", `${path}/elicitation`));
        }
        if (message.envelopeType === "task.failed" && cause.envelopeType === "task.submit") {
          if (message.error.code !== "configured_worker_unavailable") semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "Only configured-worker unavailability may fail directly from TaskSubmit.", `${path}/error/code`));
          // Scoped to this submit's own causal fan-out, not the whole trace,
          // since a continuation's later task.submit may also bypass
          // execution and fail directly while earlier Runs in the same
          // trace carry many more messages.
          const otherConsequences = validated.filter((candidate) => "causationMessageId" in candidate && candidate.causationMessageId === cause.messageId && candidate.messageId !== message.messageId);
          if (otherConsequences.length > 0) semanticDiagnostics.push(runtimeDiagnostic("causation_violation", "A direct TaskSubmit failure must be the only consequence of its causing submit.", path));
        }
      }
    }
    const boundary = `${message.producer}->${message.consumer}`;
    const phase = phaseForType[message.envelopeType];
    const boundaryState = runState ?? { phaseByBoundary: new Map<string, number>() };
    const priorPhase = boundaryState.phaseByBoundary.get(boundary) ?? 0;
    if (phase < priorPhase || (priorPhase === 3 && phase === 3)) semanticDiagnostics.push(runtimeDiagnostic("ordering_violation", "Messages on the same boundary within one Run must progress from accepted through progress to one terminal result.", path));
    boundaryState.phaseByBoundary.set(boundary, Math.max(priorPhase, phase));
    if (run && run.runId.length === 0) semanticDiagnostics.push(runtimeDiagnostic("projection_violation", "A boundary message must project a run identifier.", `${path}/run/runId`));
    byId.set(message.messageId, message);
  }
  const sorted = sortDiagnostics(semanticDiagnostics);
  return { valid: sorted.length === 0, value: sorted.length === 0 ? validated : null, diagnostics: sorted };
}
