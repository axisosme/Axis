import { readFile } from "node:fs/promises";
import {
  decideRuntimeTaskSubmit,
  validateRuntimeBoundaryMessage,
  validateRuntimeBoundaryTrace,
  validateRuntimeSchema,
} from "./validate.js";
import { runtimeSchemaNames, type RuntimeSchemaName } from "./registry.js";
import type { Run, RuntimeBoundaryMessage, RuntimeIdentityGraph, RunStartEnvelope, SourceRef, TaskSubmitEnvelope } from "./types.generated.js";
import { fromRuntimePackageRoot } from "../runtime-paths.js";

const traceVersion = "1.0.0" as const;
const contractVersion = "1.0.0" as const;
const suite = "runtime-v1-golden" as const;

export const goldenRules = [
  "RUNTIME-IDENTITY-LINEAGE",
  "RUNTIME-CORRELATION",
  "RUNTIME-LEGAL-STATE-ORDER",
  "RUNTIME-EXACTLY-ONE-TERMINAL-RESULT",
  "RUNTIME-START-IDEMPOTENCY",
  "RUNTIME-CONTINUATION-CHAIN",
] as const;

type GoldenRule = (typeof goldenRules)[number];
type GoldenTraceId = "success" | "failure" | "duplicate-submit" | "continuation" | "cancellation" | "continue-from-cancelled";
type GoldenOutcome = "succeeded" | "failed" | "duplicate";

interface GoldenTrace {
  traceVersion: typeof traceVersion;
  contractVersion: typeof contractVersion;
  traceId: GoldenTraceId;
  expectedOutcome: GoldenOutcome;
  messages: RuntimeBoundaryMessage[];
}

interface RecordingDouble {
  sent: string[];
  received: string[];
  send(message: RuntimeBoundaryMessage): void;
  receive(message: RuntimeBoundaryMessage): void;
}

class RecordingRelay implements RecordingDouble {
  sent: string[] = [];
  received: string[] = [];
  send(message: RuntimeBoundaryMessage): void { this.sent.push(message.messageId); }
  receive(message: RuntimeBoundaryMessage): void { this.received.push(message.messageId); }
}

class RecordingRouter implements RecordingDouble {
  sent: string[] = [];
  received: string[] = [];
  send(message: RuntimeBoundaryMessage): void { this.sent.push(message.messageId); }
  receive(message: RuntimeBoundaryMessage): void { this.received.push(message.messageId); }
}

class RecordingWorker implements RecordingDouble {
  sent: string[] = [];
  received: string[] = [];
  send(message: RuntimeBoundaryMessage): void { this.sent.push(message.messageId); }
  receive(message: RuntimeBoundaryMessage): void { this.received.push(message.messageId); }
}

class RecordingCodexAdapter implements RecordingDouble {
  sent: string[] = [];
  received: string[] = [];
  send(message: RuntimeBoundaryMessage): void { this.sent.push(message.messageId); }
  receive(message: RuntimeBoundaryMessage): void { this.received.push(message.messageId); }
}

function replay(messages: readonly RuntimeBoundaryMessage[]): boolean {
  const doubles: Record<RuntimeBoundaryMessage["producer"], RecordingDouble> = {
    relay: new RecordingRelay(),
    router: new RecordingRouter(),
    worker: new RecordingWorker(),
    "codex-adapter": new RecordingCodexAdapter(),
  };
  for (const message of messages) {
    doubles[message.producer].send(message);
    doubles[message.consumer].receive(message);
  }
  return Object.values(doubles).reduce((count, double) => count + double.sent.length + double.received.length, 0) === messages.length * 2;
}

function messageTask(message: RuntimeBoundaryMessage) {
  return "identity" in message ? message.identity.task : message.task;
}

function messageRun(message: RuntimeBoundaryMessage): Run | { runId: string; state: Run["state"]; error?: { code: string; safeMessage: string }; resultRef?: string; elicitation?: Run["elicitation"] } | undefined {
  if (message.envelopeType === "task.submit") return undefined;
  return "identity" in message ? message.identity.run : message.run;
}

function messageRunId(message: RuntimeBoundaryMessage): string | undefined {
  return messageRun(message)?.runId;
}

function sameSourceRefFields(left: SourceRef, right: SourceRef): boolean {
  return left.system === right.system && left.organizationId === right.organizationId && left.issueId === right.issueId && left.agentSessionId === right.agentSessionId;
}

// A continuation trace contains multiple Runs under one Task, with identity and lifecycle checks scoped to each Run.
function lineageFailure(messages: readonly RuntimeBoundaryMessage[]): boolean {
  const first = messages[0];
  if (!first) return true;
  const task = messageTask(first);
  const perRun = new Map<string, { attemptId?: string; bindingId?: string }>();
  for (const message of messages) {
    const currentTask = messageTask(message);
    if (currentTask.taskId !== task.taskId || !sameSourceRefFields(currentTask.sourceRef, task.sourceRef)) return true;
    const run = messageRun(message);
    const runId = messageRunId(message);
    if (run && runId !== undefined) {
      if ("taskId" in run && run.taskId !== task.taskId) return true;
      const state = perRun.get(runId) ?? {};
      if (!perRun.has(runId)) perRun.set(runId, state);
      if ("attemptId" in run) {
        if (state.attemptId !== undefined && run.attemptId !== state.attemptId) return true;
        state.attemptId ??= run.attemptId;
      }
    }
    if ("attempt" in message && runId !== undefined) {
      if (message.attempt.taskId !== task.taskId) return true;
      const state = perRun.get(runId) ?? {};
      if (!perRun.has(runId)) perRun.set(runId, state);
      if (state.attemptId !== undefined && message.attempt.attemptId !== state.attemptId) return true;
      state.attemptId ??= message.attempt.attemptId;
    }
    if ("identity" in message) {
      const binding = message.identity.sessionBinding;
      const identityRunId = message.identity.run.runId;
      if (binding.taskId !== task.taskId || binding.runId !== identityRunId) return true;
      const state = perRun.get(identityRunId) ?? {};
      if (!perRun.has(identityRunId)) perRun.set(identityRunId, state);
      if (state.attemptId !== undefined && binding.attemptId !== state.attemptId) return true;
      state.attemptId ??= binding.attemptId;
      if (state.bindingId !== undefined && binding.sessionBindingId !== state.bindingId) return true;
      state.bindingId ??= binding.sessionBindingId;
    }
  }
  return false;
}

function correlationFailure(messages: readonly RuntimeBoundaryMessage[]): boolean {
  const correlationId = messages[0]?.correlationId;
  if (!correlationId) return true;
  return messages.some((message) => {
    if (message.correlationId !== correlationId || messageTask(message).correlationId !== correlationId) return true;
    if ("attempt" in message && message.attempt.correlationId !== correlationId) return true;
    const run = messageRun(message);
    if (run && "correlationId" in run && run.correlationId !== correlationId) return true;
    if ("identity" in message) {
      const identity = message.identity;
      return identity.contractVersion !== contractVersion || identity.task.correlationId !== correlationId ||
        identity.attempt.correlationId !== correlationId || identity.run.correlationId !== correlationId ||
        identity.sessionBinding.correlationId !== correlationId;
    }
    return false;
  });
}

function stateOrderFailure(messages: readonly RuntimeBoundaryMessage[]): boolean {
  const perRunOrder = new Map<string, number>();
  for (const message of messages) {
    const run = messageRun(message);
    const runId = messageRunId(message);
    if (!run || runId === undefined) continue;
    const order = run.state === "pending" ? 0 : run.state === "running" ? 1 : 2;
    const priorOrder = perRunOrder.get(runId) ?? 0;
    if (order < priorOrder) return true;
    perRunOrder.set(runId, order);
  }
  return false;
}

function terminalCountFailure(trace: GoldenTrace): boolean {
  // Each Run with lifecycle progress beyond run.start must have exactly one terminal result. Check the
  // overall outcome on the last Run without a successor. A Run with only run.start is exempt because
  // continuation-chain validation owns abandoned forks.
  const isTerminalType = (envelopeType: RuntimeBoundaryMessage["envelopeType"]): envelopeType is "task.succeeded" | "task.failed" | "task.waiting_for_input" =>
    envelopeType === "task.succeeded" || envelopeType === "task.failed" || envelopeType === "task.waiting_for_input";
  const isTerminalMessage = (message: RuntimeBoundaryMessage): message is Extract<RuntimeBoundaryMessage, { envelopeType: "task.succeeded" | "task.failed" | "task.waiting_for_input" }> =>
    isTerminalType(message.envelopeType);
  if (trace.expectedOutcome === "duplicate") {
    return trace.messages.some(isTerminalMessage);
  }
  const runIdsWithLifecycleProgress = new Set<string>();
  for (const message of trace.messages) {
    if (message.envelopeType === "task.submit" || message.envelopeType === "run.start") continue;
    const runId = messageRunId(message);
    if (runId !== undefined) runIdsWithLifecycleProgress.add(runId);
  }
  if (runIdsWithLifecycleProgress.size === 0) return true;
  const terminalsByRunId = new Map<string, Extract<RuntimeBoundaryMessage, { envelopeType: "task.succeeded" | "task.failed" | "task.waiting_for_input" }>[]>();
  for (const message of trace.messages) {
    if (!isTerminalMessage(message)) continue;
    const runId = messageRunId(message);
    if (runId === undefined) continue;
    const existing = terminalsByRunId.get(runId) ?? [];
    existing.push(message);
    terminalsByRunId.set(runId, existing);
  }
  // Every Run that has progressed beyond run.start must have EXACTLY one
  // terminal of its own -- zero (a Run that never resolved despite
  // progressing) or more than one (a duplicate/replayed terminal for the
  // same Run) are both defects.
  for (const runId of runIdsWithLifecycleProgress) {
    if ((terminalsByRunId.get(runId)?.length ?? 0) !== 1) return true;
  }
  const terminals = [...terminalsByRunId.values()].flat();
  if (terminals.length === 0) return true;
  const final = trace.messages[trace.messages.length - 1];
  if (!final || !isTerminalMessage(final) || final.envelopeType !== `task.${trace.expectedOutcome}`) return true;
  const continuedRunIds = new Set(
    trace.messages
      .filter((message): message is TaskSubmitEnvelope => message.envelopeType === "task.submit" && message.priorRunId !== undefined)
      .map((message) => message.priorRunId!),
  );
  for (const terminal of terminals) {
    if (terminal.messageId === final.messageId) continue;
    const terminalRunId = messageRunId(terminal);
    if (terminalRunId === undefined || !continuedRunIds.has(terminalRunId)) return true;
  }
  return false;
}

async function startIdempotencyFailure(trace: GoldenTrace): Promise<boolean> {
  if (trace.expectedOutcome !== "duplicate") return false;
  const submits = trace.messages.filter((message): message is TaskSubmitEnvelope => message.envelopeType === "task.submit");
  const runStarts = trace.messages.filter((message): message is RunStartEnvelope => message.envelopeType === "run.start");
  const codexStarts = trace.messages.filter((message) => message.envelopeType === "executor.start" &&
    message.producer === "worker" && message.consumer === "codex-adapter");
  const codexStartCount = codexStarts.length;
  if (submits.length !== 2 || runStarts.length !== 1 || codexStartCount !== 1) return true;
  const originalRunStart = runStarts[0]!;
  const originalSubmit = submits.find((message) => message.messageId === originalRunStart.causationMessageId);
  const duplicateSubmit = submits.find((message) => message.messageId !== originalSubmit?.messageId);
  const codexStart = codexStarts[0]!;
  if (!originalSubmit || !duplicateSubmit || codexStart.causationMessageId !== originalRunStart.messageId ||
    !sameIdentityGraph(originalRunStart.identity, codexStart.identity)) return true;
  const decision = await decideRuntimeTaskSubmit(duplicateSubmit, {
    originalSubmit,
    originalRunStart,
  }, { hasPriorRuns: false });
  return decision.decision !== "duplicate" || decision.emitRunStart || !decision.identity ||
    !sameIdentityGraph(decision.identity, originalRunStart.identity);
}

// A predecessor may have only one successor across the entire corpus, including successors in separate independently valid traces.
function collectPriorRunClaims(messages: readonly RuntimeBoundaryMessage[]): Map<string, Set<string>> {
  const priorRunIdClaims = new Map<string, Set<string>>();
  for (const message of messages) {
    if (message.envelopeType === "task.submit" && message.priorRunId !== undefined) {
      const runStart = messages.find((candidate) => "causationMessageId" in candidate && candidate.causationMessageId === message.messageId && candidate.envelopeType === "run.start");
      const claimantRunId = runStart && "identity" in runStart ? runStart.identity.run.runId : undefined;
      if (claimantRunId) {
        const claimants = priorRunIdClaims.get(message.priorRunId) ?? new Set<string>();
        claimants.add(claimantRunId);
        priorRunIdClaims.set(message.priorRunId, claimants);
      }
    }
  }
  return priorRunIdClaims;
}

function continuationChainFailure(traceMessages: readonly RuntimeBoundaryMessage[], corpusMessages: readonly RuntimeBoundaryMessage[] = traceMessages): boolean {
  const corpusClaims = collectPriorRunClaims(corpusMessages);
  const traceClaimedPriorRunIds = new Set(
    traceMessages.filter((message): message is TaskSubmitEnvelope => message.envelopeType === "task.submit" && message.priorRunId !== undefined).map((message) => message.priorRunId!),
  );
  for (const priorRunId of traceClaimedPriorRunIds) {
    if ((corpusClaims.get(priorRunId)?.size ?? 0) > 1) return true;
  }
  return false;
}

function sameIdentityGraph(left: RuntimeIdentityGraph, right: RuntimeIdentityGraph): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

async function failedRules(trace: GoldenTrace, corpusMessages: readonly RuntimeBoundaryMessage[] = trace.messages): Promise<GoldenRule[]> {
  const failed: GoldenRule[] = [];
  if (lineageFailure(trace.messages)) failed.push("RUNTIME-IDENTITY-LINEAGE");
  if (correlationFailure(trace.messages)) failed.push("RUNTIME-CORRELATION");
  if (stateOrderFailure(trace.messages)) failed.push("RUNTIME-LEGAL-STATE-ORDER");
  if (terminalCountFailure(trace)) failed.push("RUNTIME-EXACTLY-ONE-TERMINAL-RESULT");
  if (await startIdempotencyFailure(trace)) failed.push("RUNTIME-START-IDEMPOTENCY");
  if (continuationChainFailure(trace.messages, corpusMessages)) failed.push("RUNTIME-CONTINUATION-CHAIN");
  return failed;
}

async function readTrace(traceId: GoldenTraceId): Promise<GoldenTrace> {
  const raw = JSON.parse(await readFile(fromRuntimePackageRoot("conformance", "runtime", "traces", `${traceId}.json`), "utf8")) as GoldenTrace;
  if (raw.traceVersion !== traceVersion || raw.contractVersion !== contractVersion || raw.traceId !== traceId || !Array.isArray(raw.messages)) {
    throw new Error("A bundled golden trace has an invalid deterministic envelope.");
  }
  return raw;
}

function schemaDocuments(traces: readonly GoldenTrace[]): Record<RuntimeSchemaName, unknown> {
  const messages = traces.flatMap((trace) => trace.messages);
  const byType = new Map(messages.map((message) => [message.envelopeType, message]));
  const runStart = byType.get("run.start") as RunStartEnvelope | undefined;
  const identity = runStart?.identity;
  if (!identity) throw new Error("The golden traces must include a RunStart identity graph.");
  const envelopeNames: Record<RuntimeBoundaryMessage["envelopeType"], RuntimeSchemaName> = {
    "task.submit": "task-submit-envelope", "run.start": "run-start-envelope", "executor.start": "executor-start-envelope",
    "executor.accepted": "executor-accepted-envelope", "run.accepted": "run-accepted-envelope", "task.accepted": "task-accepted-envelope",
    "executor.progressed": "executor-progressed-envelope", "run.progressed": "run-progressed-envelope", "task.progressed": "task-progressed-envelope",
    "executor.succeeded": "executor-succeeded-envelope", "run.succeeded": "run-succeeded-envelope", "task.succeeded": "task-succeeded-envelope",
    "executor.failed": "executor-failed-envelope", "run.failed": "run-failed-envelope", "task.failed": "task-failed-envelope",
    "executor.waiting_for_input": "executor-waiting-for-input-envelope", "run.waiting_for_input": "run-waiting-for-input-envelope", "task.waiting_for_input": "task-waiting-for-input-envelope",
  };
  const documents: Partial<Record<RuntimeSchemaName, unknown>> = {
    common: identity.task,
    task: identity.task,
    attempt: identity.attempt,
    run: identity.run,
    "session-binding": identity.sessionBinding,
    "runtime-identity-graph": identity,
    "runtime-start-envelope": {
      contractVersion,
      messageId: "golden-runtime-start",
      correlationId: identity.task.correlationId,
      operationId: runStart.operationId,
      occurredAt: runStart.occurredAt,
      startIdempotencyKey: "golden-runtime-start",
      identity,
    },
    "runtime-boundary-message": messages[0],
  };
  for (const [type, name] of Object.entries(envelopeNames) as [RuntimeBoundaryMessage["envelopeType"], RuntimeSchemaName][]) {
    documents[name] = byType.get(type);
  }
  if (runtimeSchemaNames.some((name) => documents[name] === undefined)) throw new Error("The golden traces do not cover every runtime schema.");
  return documents as Record<RuntimeSchemaName, unknown>;
}

function mutate(trace: GoldenTrace, mutationId: string): GoldenTrace {
  const result = structuredClone(trace);
  if (mutationId === "broken-identity") {
    (result.messages[1] as RunStartEnvelope).identity.task.taskId = "other-task";
  } else if (mutationId === "broken-source-identity") {
    const runStart = result.messages.find((item) => item.envelopeType === "run.start") as RunStartEnvelope | undefined;
    if (!runStart) throw new Error("The continuation trace lacks RunStart.");
    runStart.identity.task.sourceRef.organizationId = "other-organization";
  } else if (mutationId === "illegal-state-order") {
    const message = result.messages.find((item) => item.envelopeType === "run.progressed");
    if (!message || !("run" in message)) throw new Error("The success trace lacks RunProgressed.");
    message.run.state = "pending";
  } else if (mutationId === "wrong-terminal-count") {
    const terminal = result.messages.find((item) => item.envelopeType === "task.succeeded");
    if (!terminal) throw new Error("The success trace lacks TaskSucceeded.");
    const duplicate = structuredClone(terminal);
    duplicate.messageId = "success-task-succeeded-duplicate";
    result.messages.push(duplicate);
  } else if (mutationId === "missing-waiting-terminal") {
    // Removing a waiting Run's terminal hop must violate the per-Run terminal-result rule while leaving other Runs intact.
    const waitingTerminal = result.messages.find((item) => item.envelopeType === "task.waiting_for_input");
    if (!waitingTerminal) throw new Error("The continuation trace lacks its first Run's task.waiting_for_input terminal.");
    const index = result.messages.findIndex((item) => item.messageId === waitingTerminal.messageId);
    result.messages.splice(index, 1);
  } else if (mutationId === "broken-correlation") {
    (result.messages[1] as RunStartEnvelope).correlationId = "other-correlation";
  } else if (mutationId === "duplicate-start-conflict") {
    const duplicate = result.messages.find((message) => message.envelopeType === "task.submit" && message.messageId !== "d-submit") as TaskSubmitEnvelope | undefined;
    if (!duplicate) throw new Error("The duplicate trace lacks its duplicate TaskSubmit.");
    duplicate.operationId = "other-operation";
  } else if (mutationId === "continuation-fork") {
    // Two continuation submits both claim the same priorRunId -- clone the
    // second Run's whole message set with a new runId/attemptId/binding/
    // messageIds but leave priorRunId pointing at the SAME predecessor.
    const secondSubmit = result.messages.find((message) => message.envelopeType === "task.submit" && message.priorRunId !== undefined) as TaskSubmitEnvelope | undefined;
    if (!secondSubmit) throw new Error("The continuation trace lacks its continuation TaskSubmit.");
    const forkSubmit = structuredClone(secondSubmit);
    forkSubmit.messageId = `${secondSubmit.messageId}-fork`;
    forkSubmit.submitIdempotencyKey = `${secondSubmit.submitIdempotencyKey}-fork`;
    const secondRunStart = result.messages.find((message) => "causationMessageId" in message && message.causationMessageId === secondSubmit.messageId && message.envelopeType === "run.start") as RunStartEnvelope | undefined;
    if (!secondRunStart) throw new Error("The continuation trace lacks its continuation RunStart.");
    const forkRunStart = structuredClone(secondRunStart);
    forkRunStart.messageId = `${secondRunStart.messageId}-fork`;
    forkRunStart.causationMessageId = forkSubmit.messageId;
    forkRunStart.identity.run.runId = `${secondRunStart.identity.run.runId}-fork`;
    forkRunStart.identity.run.attemptId = `${secondRunStart.identity.attempt.attemptId}-fork`;
    forkRunStart.identity.run.sessionBindingId = `${secondRunStart.identity.sessionBinding.sessionBindingId}-fork`;
    forkRunStart.identity.attempt.attemptId = `${secondRunStart.identity.attempt.attemptId}-fork`;
    forkRunStart.identity.sessionBinding.sessionBindingId = `${secondRunStart.identity.sessionBinding.sessionBindingId}-fork`;
    forkRunStart.identity.sessionBinding.runId = forkRunStart.identity.run.runId;
    forkRunStart.identity.sessionBinding.attemptId = forkRunStart.identity.attempt.attemptId;
    // Insert before the trace's final terminal message so this mutation
    // exercises ONLY RUNTIME-CONTINUATION-CHAIN, not also
    // RUNTIME-EXACTLY-ONE-TERMINAL-RESULT via an incidental "last message is
    // no longer the terminal" side effect of appending at the end.
    result.messages.splice(result.messages.length - 1, 0, forkSubmit, forkRunStart);
  } else if (mutationId === "self-link") {
    // self-link case: a Run names itself as its own
    // predecessor. This is caught one layer down, by validateRun's
    // schema-level priorRunId !== runId check, on the single message
    // itself -- so this mutation is asserted differently from the others
    // (see runSelfLinkCase below), never routed through failedRules.
    const runStart = result.messages.find((message) => message.envelopeType === "run.start") as RunStartEnvelope | undefined;
    if (!runStart) throw new Error("The trace lacks RunStart.");
    runStart.identity.run.priorRunId = runStart.identity.run.runId;
  } else if (mutationId === "submit-run-mismatch") {
    // task.submit/Run priorRunId mismatch case: the
    // submit that caused a Run carries a DIFFERENT priorRunId than the Run
    // itself projects. The causing-submit check enforces their agreement.
    // Deliberately propagate the mismatched value to every full-Run
    // projection sharing this runId (run.start/run.accepted/run.succeeded
    // all carry a complete Run, unlike task.accepted/task.succeeded's
    // trimmed RunProjection, which has no priorRunId field at all) so the
    // mutation exercises ONLY the submit-vs-Run mismatch this case names,
    // not also an incidental in-Run immutability violation from touching
    // just one of several Run projections.
    const submit = result.messages.find((message) => message.envelopeType === "task.submit" && message.priorRunId !== undefined) as TaskSubmitEnvelope | undefined;
    if (!submit) throw new Error("The trace lacks a continuation TaskSubmit.");
    const runStart = result.messages.find((message) => "causationMessageId" in message && message.causationMessageId === submit.messageId && message.envelopeType === "run.start") as RunStartEnvelope | undefined;
    if (!runStart) throw new Error("The trace lacks the continuation submit's RunStart.");
    const mismatchedPriorRunId = `${submit.priorRunId}-mismatched`;
    const mutatedRunId = runStart.identity.run.runId;
    runStart.identity.run.priorRunId = mismatchedPriorRunId;
    for (const message of result.messages) {
      if (message === runStart) continue;
      const run = "identity" in message ? message.identity.run : ("run" in message ? message.run : undefined);
      if (run && "priorRunId" in run && run.runId === mutatedRunId) run.priorRunId = mismatchedPriorRunId;
    }
  } else {
    throw new Error("The golden mutation is not defined.");
  }
  return result;
}

// admission-invariant cases that assert rejection at
// decideRuntimeTaskSubmit/compareRuntimeTaskSubmits directly (not via a
// trace mutation), since these are properties of the ADMISSION DECISION
// given caller-supplied continuation context, not of a trace that's already
// been admitted. Each is built from the bundled continuation fixture's own
// identifiers so it exercises the real fixture's shape, not an invented one.
interface AdmissionCaseResult { id: string; expectedDecision: string; status: "passed" | "failed"; actualDecision: string }

async function runAdmissionInvariantCases(continuationTrace: GoldenTrace): Promise<AdmissionCaseResult[]> {
  const submit = continuationTrace.messages.find((message): message is TaskSubmitEnvelope => message.envelopeType === "task.submit" && message.priorRunId !== undefined);
  const runStart = continuationTrace.messages.find((message): message is RunStartEnvelope => message.envelopeType === "run.start" && message.identity.run.priorRunId !== undefined);
  if (!submit || !runStart) throw new Error("The continuation trace lacks a priorRunId-bearing submit/RunStart pair.");
  const predecessorRunId = submit.priorRunId!;
  const predecessorTask = runStart.identity.task;
  const otherTask = { ...predecessorTask, taskId: `${predecessorTask.taskId}-other`, correlationId: `${predecessorTask.correlationId}-other` };
  const otherTenantSourceRef: SourceRef = { ...predecessorTask.sourceRef, organizationId: `${predecessorTask.sourceRef.organizationId}-other` };
  const otherTenantTask = { ...predecessorTask, sourceRef: otherTenantSourceRef };

  const cases: { id: string; expectedDecision: string; run: () => Promise<{ decision: string }> }[] = [
    {
      id: "cross-task-predecessor",
      expectedDecision: "continuation_conflict",
      run: () => decideRuntimeTaskSubmit(submit, null, { hasPriorRuns: true, priorRun: { runId: predecessorRunId, state: "succeeded", task: otherTask } }),
    },
    {
      id: "cross-tenant-predecessor",
      expectedDecision: "continuation_conflict",
      run: () => decideRuntimeTaskSubmit(submit, null, { hasPriorRuns: true, priorRun: { runId: predecessorRunId, state: "succeeded", task: otherTenantTask } }),
    },
    {
      id: "non-terminal-predecessor",
      expectedDecision: "continuation_conflict",
      run: () => decideRuntimeTaskSubmit(submit, null, { hasPriorRuns: true, priorRun: { runId: predecessorRunId, state: "running", task: predecessorTask } }),
    },
    {
      id: "first-run-bypass",
      expectedDecision: "continuation_conflict",
      run: () => {
        const { priorRunId: _drop, ...withoutPriorRunId } = submit;
        return decideRuntimeTaskSubmit(withoutPriorRunId as TaskSubmitEnvelope, null, { hasPriorRuns: true });
      },
    },
    {
      id: "same-key-retry-changed-priorrunid",
      expectedDecision: "conflict",
      run: () => decideRuntimeTaskSubmit(
        { ...submit, priorRunId: `${predecessorRunId}-different` },
        { originalSubmit: submit, originalRunStart: runStart },
        { hasPriorRuns: true, priorRun: { runId: predecessorRunId, state: "succeeded", task: predecessorTask } },
      ),
    },
    {
      id: "legitimate-continuation-retry",
      expectedDecision: "duplicate",
      run: () => decideRuntimeTaskSubmit(
        submit,
        { originalSubmit: submit, originalRunStart: runStart },
        { hasPriorRuns: true, priorRun: { runId: predecessorRunId, state: "succeeded", task: predecessorTask } },
      ),
    },
  ];
  return Promise.all(cases.map(async ({ id, expectedDecision, run }) => {
    const { decision } = await run();
    return { id, expectedDecision, actualDecision: decision, status: decision === expectedDecision ? "passed" as const : "failed" as const };
  }));
}

export interface RuntimeV1GoldenReport {
  reportVersion: "1.0.0";
  contractVersion: "1.0.0";
  suite: "runtime-v1-golden";
  status: "passed" | "failed";
  schemaCoverage: { expected: number; validated: number; status: "passed" | "failed" };
  traces: { id: GoldenTraceId; status: "passed" | "failed"; messageCount: number; outcome: GoldenOutcome; terminal: { messageId: string; contractVersion: typeof contractVersion; rule: "RUNTIME-EXACTLY-ONE-TERMINAL-RESULT"; safeErrorCode: string | null } | null }[];
  mutations: { id: string; rule: GoldenRule; status: "rejected" | "unexpected" }[];
  admissionCases: AdmissionCaseResult[];
  summary: { tracesPassed: number; mutationsRejected: number; admissionCasesPassed: number; rules: readonly GoldenRule[] };
}

export async function runRuntimeV1Golden(): Promise<RuntimeV1GoldenReport> {
  const traces = await Promise.all((["success", "failure", "duplicate-submit", "continuation", "cancellation", "continue-from-cancelled"] as const).map(readTrace));
  const documents = schemaDocuments(traces);
  const schemaResults = await Promise.all(runtimeSchemaNames.map(async (name) => (await validateRuntimeSchema(name, documents[name])).valid));
  // Validate each trace's continuation chain against all bundled messages to detect forks across separate traces.
  const fullCorpus = traces.flatMap((trace) => trace.messages);
  const traceResults = await Promise.all(traces.map(async (trace) => {
    const messagesValid = (await Promise.all(trace.messages.map(async (message) => (await validateRuntimeBoundaryMessage(message)).valid))).every(Boolean);
    const traceValid = (await validateRuntimeBoundaryTrace(trace.messages)).valid;
    const rules = await failedRules(trace, fullCorpus);
    const terminalMessage = trace.messages.find((message) => message.envelopeType === "task.succeeded" || message.envelopeType === "task.failed");
    const terminal = terminalMessage ? {
      messageId: terminalMessage.messageId,
      contractVersion: terminalMessage.contractVersion,
      rule: "RUNTIME-EXACTLY-ONE-TERMINAL-RESULT" as const,
      safeErrorCode: terminalMessage.envelopeType === "task.failed" ? terminalMessage.error.code : null,
    } : null;
    return { id: trace.traceId, status: messagesValid && traceValid && rules.length === 0 && replay(trace.messages) ? "passed" as const : "failed" as const, messageCount: trace.messages.length, outcome: trace.expectedOutcome, terminal };
  }));
  const mutationDefinitions: readonly [string, GoldenRule, GoldenTraceId][] = [
    ["broken-identity", "RUNTIME-IDENTITY-LINEAGE", "success"],
    ["broken-source-identity", "RUNTIME-IDENTITY-LINEAGE", "continuation"],
    ["illegal-state-order", "RUNTIME-LEGAL-STATE-ORDER", "success"],
    ["wrong-terminal-count", "RUNTIME-EXACTLY-ONE-TERMINAL-RESULT", "success"],
    ["missing-waiting-terminal", "RUNTIME-EXACTLY-ONE-TERMINAL-RESULT", "continuation"],
    ["broken-correlation", "RUNTIME-CORRELATION", "success"],
    ["duplicate-start-conflict", "RUNTIME-START-IDEMPOTENCY", "duplicate-submit"],
    ["continuation-fork", "RUNTIME-CONTINUATION-CHAIN", "continuation"],
  ];
  // Evaluate a mutated trace alongside the other original traces. Including its unmutated copy would
  // duplicate identities and obscure the invariant under test.
  const mutations = await Promise.all(mutationDefinitions.map(async ([id, rule, traceId]) => {
    const originalTrace = traces.find((trace) => trace.traceId === traceId)!;
    const mutatedTrace = mutate(originalTrace, id);
    const restOfCorpus = traces.filter((trace) => trace.traceId !== traceId).flatMap((trace) => trace.messages);
    const failed = await failedRules(mutatedTrace, [...restOfCorpus, ...mutatedTrace.messages]);
    return { id, rule, status: failed.length === 1 && failed[0] === rule ? "rejected" as const : "unexpected" as const };
  }));
  // self-link case: caught one layer down, by
  // validateRun's schema-level priorRunId !== runId check on the single
  // run.start message itself -- so it is asserted via
  // validateRuntimeBoundaryMessage directly, not via failedRules (which
  // never even runs once schema validation on any message has failed).
  const selfLinkTrace = mutate(traces.find((trace) => trace.traceId === "continuation")!, "self-link");
  const selfLinkRunStart = selfLinkTrace.messages.find((message) => message.envelopeType === "run.start")!;
  const selfLinkResult = await validateRuntimeBoundaryMessage(selfLinkRunStart);
  const selfLinkMutation = {
    id: "self-link",
    rule: "RUNTIME-CONTINUATION-CHAIN" as GoldenRule,
    status: (!selfLinkResult.valid && selfLinkResult.diagnostics.some((diagnostic) => diagnostic.code === "identity_mismatch")) ? "rejected" as const : "unexpected" as const,
  };
  // task.submit/Run priorRunId mismatch case: this is
  // a per-trace CAUSATION check, owned by validateRuntimeBoundaryTrace
  // not one of golden.ts's own failedRules -- asserted
  // the same way as self-link, directly against the validator, not routed
  // through the RUNTIME-CONTINUATION-CHAIN mutation pipeline above.
  const submitRunMismatchTrace = mutate(traces.find((trace) => trace.traceId === "continuation")!, "submit-run-mismatch");
  const submitRunMismatchResult = await validateRuntimeBoundaryTrace(submitRunMismatchTrace.messages);
  const submitRunMismatchMutation = {
    id: "submit-run-mismatch",
    rule: "RUNTIME-CONTINUATION-CHAIN" as GoldenRule,
    status: (!submitRunMismatchResult.valid && submitRunMismatchResult.diagnostics.length === 1 &&
      submitRunMismatchResult.diagnostics[0]?.code === "causation_violation") ? "rejected" as const : "unexpected" as const,
  };
  // A separate, internally valid trace claiming the same predecessor must fail corpus-wide continuation-chain validation.
  const continuationTrace = traces.find((trace) => trace.traceId === "continuation")!;
  const realSecondSubmit = continuationTrace.messages.find((message): message is TaskSubmitEnvelope => message.envelopeType === "task.submit" && message.priorRunId !== undefined)!;
  const realSecondRunStart = continuationTrace.messages.find((message): message is RunStartEnvelope => message.envelopeType === "run.start" && message.causationMessageId === realSecondSubmit.messageId)!;
  const crossTraceForkSubmit = structuredClone(realSecondSubmit);
  crossTraceForkSubmit.messageId = "cross-trace-fork-submit";
  crossTraceForkSubmit.submitIdempotencyKey = "cross-trace-fork-submit-key";
  const crossTraceForkRunStart = structuredClone(realSecondRunStart);
  crossTraceForkRunStart.messageId = "cross-trace-fork-run-start";
  crossTraceForkRunStart.causationMessageId = crossTraceForkSubmit.messageId;
  crossTraceForkRunStart.identity.run.runId = "cross-trace-fork-run";
  crossTraceForkRunStart.identity.run.attemptId = "cross-trace-fork-attempt";
  crossTraceForkRunStart.identity.run.sessionBindingId = "cross-trace-fork-binding";
  crossTraceForkRunStart.identity.attempt.attemptId = "cross-trace-fork-attempt";
  crossTraceForkRunStart.identity.sessionBinding.sessionBindingId = "cross-trace-fork-binding";
  crossTraceForkRunStart.identity.sessionBinding.runId = "cross-trace-fork-run";
  crossTraceForkRunStart.identity.sessionBinding.attemptId = "cross-trace-fork-attempt";
  const syntheticSecondTraceMessages = [crossTraceForkSubmit, crossTraceForkRunStart];
  const crossTraceForkCorpus = [...continuationTrace.messages, ...syntheticSecondTraceMessages];
  const crossTraceForkOnContinuationTrace = continuationChainFailure(continuationTrace.messages, crossTraceForkCorpus);
  const crossTraceForkOnSyntheticTrace = continuationChainFailure(syntheticSecondTraceMessages, crossTraceForkCorpus);
  // Sanity check: neither trace's OWN messages alone (i.e. the exact
  // single-trace corpus every other check above already covers) trip this
  // rule -- proving the failure genuinely requires the cross-trace corpus,
  // not merely a bug that happens to also fire standalone.
  const crossTraceForkFalsePositiveCheck = continuationChainFailure(continuationTrace.messages, continuationTrace.messages) ||
    continuationChainFailure(syntheticSecondTraceMessages, syntheticSecondTraceMessages);
  const crossTraceForkMutation = {
    id: "cross-trace-fork",
    rule: "RUNTIME-CONTINUATION-CHAIN" as GoldenRule,
    status: (crossTraceForkOnContinuationTrace && crossTraceForkOnSyntheticTrace && !crossTraceForkFalsePositiveCheck) ? "rejected" as const : "unexpected" as const,
  };
  const allMutations = [...mutations, selfLinkMutation, submitRunMismatchMutation, crossTraceForkMutation];
  const admissionCases = await runAdmissionInvariantCases(continuationTrace);
  const validated = schemaResults.filter(Boolean).length;
  const tracesPassed = traceResults.filter((trace) => trace.status === "passed").length;
  const mutationsRejected = allMutations.filter((mutation) => mutation.status === "rejected").length;
  const admissionCasesPassed = admissionCases.filter((admissionCase) => admissionCase.status === "passed").length;
  return {
    reportVersion: "1.0.0",
    contractVersion,
    suite,
    status: validated === runtimeSchemaNames.length && tracesPassed === traces.length && mutationsRejected === allMutations.length &&
      admissionCasesPassed === admissionCases.length ? "passed" : "failed",
    schemaCoverage: { expected: runtimeSchemaNames.length, validated, status: validated === runtimeSchemaNames.length ? "passed" : "failed" },
    traces: traceResults,
    mutations: allMutations,
    admissionCases,
    summary: { tracesPassed, mutationsRejected, admissionCasesPassed, rules: goldenRules },
  };
}
