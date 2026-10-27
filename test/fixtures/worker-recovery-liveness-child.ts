import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { RunStartEnvelope } from "../../src/shared/runtime-boundary.js";
import { WORKER_COMMAND_VERSION } from "../../src/shared/router-worker.js";
import { executionTargetFor } from "../support/worker-execution-target.js";

import {
  CODEX_EXECUTOR_CONTRACT_VERSION,
  SqliteWorkerCommandJournal,
  WorkerCommandService,
  type CodexLiveBinding,
  type Executor,
  type ExecutorRegistration,
} from "../../src/index.js";

const binding: CodexLiveBinding = { runtimeKind: "codex", threadId: "child-thread", turnId: "child-turn" };
const runId = "run-child-recovery";
const start: RunStartEnvelope = {
  contractVersion: "1.0.0", messageId: "router-child-start", correlationId: "correlation-child", operationId: "operation-child",
  causationMessageId: "router-child-submit", occurredAt: "2026-09-03T00:00:00.000Z", producer: "router", consumer: "worker", envelopeType: "run.start",
  identity: {
    contractVersion: "1.0.0",
    task: { contractVersion: "1.0.0", taskId: "task-child", correlationId: "correlation-child", sourceRef: { system: "linear", organizationId: "organization-child", issueId: "issue-child" } },
    attempt: { contractVersion: "1.0.0", attemptId: "attempt-child", taskId: "task-child", correlationId: "correlation-child" },
    run: { contractVersion: "1.0.0", runId, taskId: "task-child", attemptId: "attempt-child", sessionBindingId: "binding-child", correlationId: "correlation-child", state: "pending" },
    sessionBinding: { contractVersion: "1.0.0", sessionBindingId: "binding-child", taskId: "task-child", attemptId: "attempt-child", runId, correlationId: "correlation-child", workerId: "worker-child", executorId: "executor-child" },
  },
};

const adapter: Executor<CodexLiveBinding> = {
  contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
  async start(input) { return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: input.requestId, operation: "start" as const, status: "accepted" as const, binding }; },
  async *observe(nativeBinding) { yield { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding: nativeBinding, sequence: 1, outcome: "started" as const }; await new Promise<never>(() => {}); },
  async recover() { return await new Promise<never>(() => {}); },
};
const registration: ExecutorRegistration<CodexLiveBinding> = {
  runtimeKind: "codex", executorId: "executor-child", methodRef: { version: "v1", method: "codex.execute" }, supportedOperations: ["start", "observe"], adapter,
  validateBinding: (value): value is CodexLiveBinding => typeof value === "object" && value !== null && (value as { runtimeKind?: unknown }).runtimeKind === "codex" && typeof (value as { threadId?: unknown }).threadId === "string" && typeof (value as { turnId?: unknown }).turnId === "string",
  bindingsEqual: (left, right) => left.threadId === right.threadId && left.turnId === right.turnId,
  methodRefEquals: (left, right) => left.version === right.version && left.method === right.method,
  projectLifecycle: () => ({ kind: "failed", code: "child_unexpected", safeMessage: "Child observer ended unexpectedly." }),
  recover: (input) => adapter.recover!(input),
};
const options = { configuration: { bearerToken: "child-token", workerId: "worker-child", runtimeKind: "codex", executorId: "executor-child", nodeId: "node-child", methodRef: { version: "v1", method: "codex.execute" }, workspaces: [{ organizationId: "organization-child", workspaceRoot: tmpdir() }] }, registrations: [registration], eventSink: { async emit(): Promise<void> {} } } as const;
const directory = await mkdtemp(join(tmpdir(), "axis-worker-recovery-child-"));
const filename = join(directory, "worker.sqlite");
const seedJournal = new SqliteWorkerCommandJournal(filename);
const seed = new WorkerCommandService({ ...options, journal: seedJournal });
const originalSetTimeout = globalThis.setTimeout;

try {
  await seed.execute({ contractVersion: WORKER_COMMAND_VERSION, executionTarget: executionTargetFor(options.configuration), commandId: "command-child-start", command: "start", start, instruction: "child recovery fixture", workspacePolicyRef: null });
  seed.close(); seedJournal.close();

  const restartedJournal = new SqliteWorkerCommandJournal(filename);
  const restarted = new WorkerCommandService({ ...options, journal: restartedJournal });
  globalThis.setTimeout = ((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => originalSetTimeout(callback, delay === 30_000 ? 10 : delay, ...args)) as unknown as typeof globalThis.setTimeout;
  await restarted.recoverActiveRuns();
  process.stdout.write("RECOVERY_COMPLETED\n");
  restarted.close(); restartedJournal.close();
} finally {
  globalThis.setTimeout = originalSetTimeout;
  await rm(directory, { recursive: true, force: true });
}
