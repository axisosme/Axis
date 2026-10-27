import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import test from "node:test";

import { agentActivityFallbackResponseBody } from "@axisosme/axis-contracts/agent-activity";

import { SqliteWorkerCommandJournal, WorkerCommandService, type RouterRunEventSink, type RouterRunLifecycleEnvelope } from "../../src/index.js";
import { ClaudeCodeExecutor, claudeCodeExecutorRegistration, type ClaudeCodeSpawnOptions } from "../../src/executor/claude-code.js";
import { ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY } from "../../src/shared/router-relay-event.js";
import { WORKER_COMMAND_VERSION } from "../../src/shared/router-worker.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION, type ClaudeCodeLiveBinding, type ExecutorLifecycleEvent } from "../../src/shared/worker-executor.js";
import { buildWorkerBoundStartRequest } from "../support/router-node-fixtures.js";
import { executionTargetFor } from "../support/worker-execution-target.js";

// Capture the Claude result line's final answer, normalize and truncate it, then carry
// it through the succeeded lifecycle event and Worker projection. Assert both
// executor-level output and the delivered run_agent_response so a fallback cannot
// conceal a missing answer.

const SESSION_ID = "33333333-3333-4333-8333-333333333333";
const HELP = [
  "  -p Native option",
  "  --session-id Native option",
  "  --resume Native option",
  "  --output-format Native option",
  '  --input-format <format>  Input format (only works with --print): "text" (default), or "stream-json" (realtime streaming input) (choices: "text", "stream-json")',
  "  --verbose Native option",
  "  --include-partial-messages Native option",
  "  --replay-user-messages Native option",
  "  --permission-mode <mode> Permission mode",
  '    (choices: "acceptEdits", "bypassPermissions", "dontAsk", "plan")',
].join("\n");

interface FakeSession {
  readonly stdout: PassThrough;
  sendMessage(message: Readonly<Record<string, unknown>>): void;
  exit(code: number | null, signal: NodeJS.Signals | null): void;
  /**
   * UUID of the first user line written to this session's stdin. It identifies the
   * adapter's own instruction for command_lifecycle reports; undefined until that line
   * arrives.
   */
  instructionUuid(): string | undefined;
}

function runtime(): { readonly spawnProcess: (file: string, args: readonly string[], options: ClaudeCodeSpawnOptions) => ChildProcess; readonly sessions: FakeSession[] } {
  const sessions: FakeSession[] = [];
  const spawnProcess = (_file: string, args: readonly string[], _options: ClaudeCodeSpawnOptions): ChildProcess => {
    const emitter = new EventEmitter();
    const task = args.includes("-p");
    const stdin = task ? new PassThrough() : null;
    const stdout = new PassThrough();
    const child = Object.assign(emitter, { stdin, stdout, stderr: null, kill: () => true }) as unknown as ChildProcess;
    queueMicrotask(() => emitter.emit("spawn"));
    if (args[0] === "--help") { queueMicrotask(() => { stdout.end(`${HELP}\n`); emitter.emit("close", 0, null); }); return child; }
    if (args[0] === "auth") { queueMicrotask(() => { stdout.end(); emitter.emit("close", 0, null); }); return child; }
    assert.ok(stdin !== null);
    let capturedUuid: string | undefined;
    const fake: FakeSession = {
      stdout,
      sendMessage: (message) => stdout.write(`${JSON.stringify(message)}\n`),
      exit: (code, signal) => { emitter.emit("exit", code, signal); emitter.emit("close", code, signal); },
      instructionUuid: () => capturedUuid,
    };
    let pending = "";
    stdin.setEncoding("utf8");
    stdin.on("data", (chunk: string) => {
      pending += chunk;
      for (let newline = pending.indexOf("\n"); newline >= 0; newline = pending.indexOf("\n")) {
        const line = JSON.parse(pending.slice(0, newline)) as Readonly<Record<string, unknown>>;
        pending = pending.slice(newline + 1);
        if (line.type === "user" && capturedUuid === undefined && typeof line.uuid === "string") capturedUuid = line.uuid;
      }
    });
    stdin.once("data", () => queueMicrotask(() => fake.sendMessage({ type: "system", subtype: "init", session_id: SESSION_ID })));
    stdin.resume();
    sessions.push(fake);
    return child;
  };
  return { spawnProcess, sessions };
}

async function runToSucceeded(executor: ClaudeCodeExecutor, fake: ReturnType<typeof runtime>, requestId: string, resultMessage: Readonly<Record<string, unknown>>): Promise<{
  readonly registration: ReturnType<typeof claudeCodeExecutorRegistration>;
  readonly events: readonly ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[];
}> {
  const registration = claudeCodeExecutorRegistration({ executorId: "claude-1", adapter: executor });
  const accepted = await executor.start({
    contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
    requestId,
    workingDirectory: "/tmp/axis-claude-workspace",
    instruction: "Do the thing.",
  });
  assert.equal(accepted.status, "accepted");
  assert.ok(accepted.status === "accepted");
  const binding = accepted.binding as ClaudeCodeLiveBinding;
  const session = fake.sessions.at(-1);
  assert.ok(session);

  const collected: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
  const consumption = (async () => { for await (const event of executor.observe(binding)) collected.push(event); })();

  session.sendMessage({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID, ...resultMessage });
  session.exit(0, null);
  await consumption;

  return { registration, events: collected };
}

test("a success result carries the answer text as a projected response, through the lifecycle event and projectLifecycle", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  try {
    const { registration, events } = await runToSucceeded(executor, fake, "terminal-response-success", {
      result: "TERMINAL-RESPONSE-MARKER: the task is done.",
    });
    const succeeded = events.find((event) => event.outcome === "succeeded");
    assert.ok(succeeded, "expected a succeeded lifecycle event");
    assert.ok(succeeded.outcome === "succeeded");
    assert.ok(succeeded.response !== undefined, "expected the succeeded event to carry a response");
    assert.equal(succeeded.response.projectionStatus, "projected");
    assert.equal(succeeded.response.body, "TERMINAL-RESPONSE-MARKER: the task is done.");
    assert.equal(succeeded.response.truncated, false);
    assert.deepEqual(succeeded.response.truncations, []);
    assert.equal(typeof succeeded.response.sourceEventId, "string");
    assert.ok(succeeded.response.sourceEventId.length > 0);

    const projected = registration.projectLifecycle({ event: succeeded, runId: "run-1" });
    assert.equal(projected.kind, "succeeded");
    assert.ok(projected.kind === "succeeded");
    assert.deepEqual(projected.response, succeeded.response);
  } finally {
    executor.close();
  }
});

test("a workspace path in the answer is redacted by the shared normalizer before it becomes the response body", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  try {
    const { events } = await runToSucceeded(executor, fake, "terminal-response-redaction", {
      result: "Wrote the file to /tmp/axis-claude-workspace/out/report.md and finished.",
    });
    const succeeded = events.find((event) => event.outcome === "succeeded");
    assert.ok(succeeded);
    assert.ok(succeeded.outcome === "succeeded");
    assert.ok(succeeded.response !== undefined);
    assert.equal(succeeded.response.projectionStatus, "projected");
    assert.equal(succeeded.response.body, "Wrote the file to out/report.md and finished.");
    assert.ok(!succeeded.response.body.includes("/tmp/axis-claude-workspace"), "the raw workspace path must not survive into the projected body");
  } finally {
    executor.close();
  }
});

test("a success result with a blank/missing answer falls back to the fixed fallback response, not an omitted one", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  try {
    const { events } = await runToSucceeded(executor, fake, "terminal-response-blank", { result: "   " });
    const succeeded = events.find((event) => event.outcome === "succeeded");
    assert.ok(succeeded);
    assert.ok(succeeded.outcome === "succeeded");
    assert.ok(succeeded.response !== undefined, "even a blank answer must still carry a response, or Worker treats it as no receipt to send at all");
    assert.equal(succeeded.response.projectionStatus, "fallback");
    assert.equal(succeeded.response.body, agentActivityFallbackResponseBody);
    assert.equal(succeeded.response.truncated, false);
    assert.deepEqual(succeeded.response.truncations, []);
  } finally {
    executor.close();
  }
});

test("a success result missing the `result` field entirely also falls back, rather than throwing or omitting the response", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  const registration = claudeCodeExecutorRegistration({ executorId: "claude-1", adapter: executor });
  try {
    const accepted = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "terminal-response-absent",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Do the thing.",
    });
    assert.equal(accepted.status, "accepted");
    assert.ok(accepted.status === "accepted");
    const binding = accepted.binding as ClaudeCodeLiveBinding;
    const session = fake.sessions[0];
    assert.ok(session);
    const collected: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
    const consumption = (async () => { for await (const event of executor.observe(binding)) collected.push(event); })();
    session.sendMessage({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    session.exit(0, null);
    await consumption;

    const succeeded = collected.find((event) => event.outcome === "succeeded");
    assert.ok(succeeded);
    assert.ok(succeeded.outcome === "succeeded");
    assert.ok(succeeded.response !== undefined);
    assert.equal(succeeded.response.projectionStatus, "fallback");
    assert.equal(succeeded.response.body, agentActivityFallbackResponseBody);

    const projected = registration.projectLifecycle({ event: succeeded, runId: "run-1" });
    assert.equal(projected.kind, "succeeded");
    assert.ok(projected.kind === "succeeded");
    assert.deepEqual(projected.response, succeeded.response);
  } finally {
    executor.close();
  }
});

test("two continuations of the same native session (shared sessionId) get distinct response sourceEventIds", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  try {
    const first = await runToSucceeded(executor, fake, "terminal-response-turn-1", { result: "First turn's answer." });
    const firstSucceeded = first.events.find((event) => event.outcome === "succeeded");
    assert.ok(firstSucceeded);
    assert.ok(firstSucceeded.outcome === "succeeded");
    assert.ok(firstSucceeded.response !== undefined);

    const continued = await executor.continue({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "terminal-response-turn-2",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Continue the thing.",
      binding: { runtimeKind: "claude-code", sessionId: SESSION_ID },
    });
    assert.equal(continued.status, "accepted");
    assert.ok(continued.status === "accepted");
    const secondBinding = continued.binding as ClaudeCodeLiveBinding;
    const secondSession = fake.sessions.at(-1);
    assert.ok(secondSession);
    const secondCollected: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
    const secondConsumption = (async () => { for await (const event of executor.observe(secondBinding)) secondCollected.push(event); })();
    // Same rendered text as the first turn: if `sourceEventId` collided across
    // Runs (hashing `sessionId` alone), the Relay
    // outbox's `UNIQUE(conversation_id, source_event_id, content_digest)`
    // guard would silently absorb this second, later, genuinely distinct
    // Run's response as a duplicate of the first.
    secondSession.sendMessage({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID, result: "First turn's answer." });
    secondSession.exit(0, null);
    await secondConsumption;

    const secondSucceeded = secondCollected.find((event) => event.outcome === "succeeded");
    assert.ok(secondSucceeded);
    assert.ok(secondSucceeded.outcome === "succeeded");
    assert.ok(secondSucceeded.response !== undefined);
    assert.notEqual(secondSucceeded.response.sourceEventId, firstSucceeded.response.sourceEventId, "each Run against the same native sessionId must get its own sourceEventId");
  } finally {
    executor.close();
  }
});

test("a denied/failed run carries no response", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  const registration = claudeCodeExecutorRegistration({ executorId: "claude-1", adapter: executor });
  try {
    const accepted = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "terminal-response-failed",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Do the thing.",
    });
    assert.equal(accepted.status, "accepted");
    assert.ok(accepted.status === "accepted");
    const binding = accepted.binding as ClaudeCodeLiveBinding;
    const session = fake.sessions[0];
    assert.ok(session);
    const collected: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
    const consumption = (async () => { for await (const event of executor.observe(binding)) collected.push(event); })();
    session.sendMessage({ type: "result", subtype: "error_during_execution", is_error: true, session_id: SESSION_ID, result: "should never be published" });
    session.exit(0, null);
    await consumption;

    const failed = collected.find((event) => event.outcome === "failed");
    assert.ok(failed, "expected a failed lifecycle event");
    assert.ok(failed.outcome === "failed");
    assert.equal((failed as unknown as { response?: unknown }).response, undefined, "a failed run must never carry a response field");

    const projected = registration.projectLifecycle({ event: failed, runId: "run-1" });
    assert.equal(projected.kind, "failed");
    assert.ok(!("response" in projected), "the failed projection must not carry a response field");
  } finally {
    executor.close();
  }
});

/**
 * Drive ClaudeCodeExecutor through the real WorkerCommandService and SQLite journal
 * with a Linear start permitting response delivery. Verify the delivered
 * run_agent_response contains the answer, in addition to the executor-level response
 * checks.
 */
test("a Claude success reaches the delivered run_agent_response envelope, not the unreported-success fallback", async (context) => {
  const marker = "WORKER-TERMINAL-RESPONSE-MARKER: the task is done end to end.";
  const request = await buildWorkerBoundStartRequest("command-worker-projection-1");
  const start = request.command.start;

  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  const configuration = {
    bearerToken: "worker-claude-projection-token",
    workerId: "worker-1",
    runtimeKind: "claude-code",
    executorId: "executor-1",
    nodeId: "node-claude-projection-1",
    methodRef: { version: "v1", method: "claude-code.execute" },
    // Matches `buildWorkerBoundStartRequest`'s `sourceRef.organizationId`.
    workspaces: [{ organizationId: "org-1", workspaceRoot: tmpdir() }],
  } as const;

  class CollectingSink implements RouterRunEventSink {
    readonly events: RouterRunLifecycleEnvelope[] = [];
    async emit(event: RouterRunLifecycleEnvelope): Promise<void> { this.events.push(event); }
  }

  const directory = mkdtempSync(join(tmpdir(), "axis-terminal-response-worker-projection-"));
  const journal = new SqliteWorkerCommandJournal(join(directory, "worker.sqlite"));
  const sink = new CollectingSink();
  const service = new WorkerCommandService({
    configuration,
    registrations: [claudeCodeExecutorRegistration({ executorId: configuration.executorId, adapter: executor })],
    eventSink: sink,
    journal,
  });
  context.after(() => { service.close(); executor.close(); journal.close(); rmSync(directory, { recursive: true, force: true }); });

  await service.execute({
    contractVersion: WORKER_COMMAND_VERSION,
    executionTarget: executionTargetFor(configuration),
    commandId: "command-worker-projection-1",
    command: "start",
    start,
    instruction: "Do the thing end to end.",
    workspacePolicyRef: null,
  });

  const session = fake.sessions.at(-1);
  assert.ok(session, "the Claude Code adapter must have spawned a task process for the start");
  session.sendMessage({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID, result: marker });
  session.exit(0, null);

  const delivered = (): Extract<RouterRunLifecycleEnvelope, { readonly deliveryType: "run_agent_response" }> | undefined =>
    sink.events.find((event): event is Extract<RouterRunLifecycleEnvelope, { readonly deliveryType: "run_agent_response" }> =>
      "deliveryType" in event && event.deliveryType === "run_agent_response");
  const deadline = Date.now() + 10_000;
  while (delivered() === undefined && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));
  const activity = delivered()?.agentActivity;
  assert.ok(activity !== undefined, "expected the Worker to deliver a run_agent_response Activity to Router");
  assert.equal(activity.projectionStatus, "projected");
  assert.equal(activity.content.body, marker);
  assert.notEqual(activity.content.body, ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY);
  assert.notEqual(activity.content.body, agentActivityFallbackResponseBody);
});

/**
 * Drive result(success), the instruction's command_lifecycle completed report and a
 * same-session SessionStart hook_response through the real executor, Worker service and
 * journal. The Run must reach run.succeeded with its answer rather than become
 * observational.
 */
function postResultHooksHarness(context: { after: (fn: () => void) => void }, runSuffix: string) {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  const configuration = {
    bearerToken: `worker-claude-post-result-hooks-token-${runSuffix}`,
    workerId: "worker-1",
    runtimeKind: "claude-code",
    executorId: "executor-1",
    nodeId: `node-claude-post-result-hooks-${runSuffix}`,
    methodRef: { version: "v1", method: "claude-code.execute" },
    // Match the Linear fixture's sourceRef.organizationId.
    workspaces: [{ organizationId: "org-1", workspaceRoot: tmpdir() }],
  } as const;

  class CollectingSink implements RouterRunEventSink {
    readonly events: RouterRunLifecycleEnvelope[] = [];
    async emit(event: RouterRunLifecycleEnvelope): Promise<void> { this.events.push(event); }
  }

  const directory = mkdtempSync(join(tmpdir(), `axis-post-result-hooks-worker-${runSuffix}-`));
  const journal = new SqliteWorkerCommandJournal(join(directory, "worker.sqlite"));
  const sink = new CollectingSink();
  const service = new WorkerCommandService({
    configuration,
    registrations: [claudeCodeExecutorRegistration({ executorId: configuration.executorId, adapter: executor })],
    eventSink: sink,
    journal,
  });
  context.after(() => { service.close(); executor.close(); journal.close(); rmSync(directory, { recursive: true, force: true }); });
  return { fake, executor, configuration, journal, sink, service };
}

async function postResultHooksStart(harness: ReturnType<typeof postResultHooksHarness>, runSuffix: string) {
  const request = await buildWorkerBoundStartRequest(`command-${runSuffix}`);
  const start = request.command.start;
  await harness.service.execute({
    contractVersion: WORKER_COMMAND_VERSION,
    executionTarget: executionTargetFor(harness.configuration),
    commandId: `command-${runSuffix}`,
    command: "start",
    start,
    instruction: "Do the thing end to end.",
    workspacePolicyRef: null,
  });
  const session = harness.fake.sessions.at(-1);
  assert.ok(session, "the Claude Code adapter must have spawned a task process for the start");
  const deadline = Date.now() + 10_000;
  while (session.instructionUuid() === undefined && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));
  const uuid = session.instructionUuid();
  assert.ok(uuid !== undefined, "expected to capture this session's own instruction uuid");
  return { runId: start.identity.run.runId, session, instructionUuid: uuid };
}

test("a same-session SessionStart hook after result and command completion preserves one successful Run answer", async (context) => {
  const marker = "WORKER-POST-RESULT-MARKER: the task is done end to end.";
  const harness = postResultHooksHarness(context, "post-result-hooks-projection-1");
  const { runId, session, instructionUuid } = await postResultHooksStart(harness, "worker-post-result-hooks-projection-1");

  session.sendMessage({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID, result: marker });
  session.sendMessage({ type: "command_lifecycle", command_uuid: instructionUuid, state: "completed", uuid: "11111111-0000-4000-8000-0000000000b1", session_id: SESSION_ID });
  session.sendMessage({ type: "system", subtype: "hook_response", hook_id: "hook-1", hook_name: "SessionStart:startup", hook_event: "SessionStart", uuid: "hook-uuid-1", session_id: SESSION_ID });
  session.exit(0, null);

  const delivered = (): Extract<RouterRunLifecycleEnvelope, { readonly deliveryType: "run_agent_response" }> | undefined =>
    harness.sink.events.find((event): event is Extract<RouterRunLifecycleEnvelope, { readonly deliveryType: "run_agent_response" }> =>
      "deliveryType" in event && event.deliveryType === "run_agent_response");
  const deadline = Date.now() + 10_000;
  while (delivered() === undefined && harness.journal.readRun(runId)?.state !== "observational" && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.journal.readRun(runId)?.state, "succeeded", "the Run must reach run.succeeded, never observational");
  assert.equal(harness.journal.readObservationalCause(runId), undefined);
  const activity = delivered()?.agentActivity;
  assert.ok(activity !== undefined, "expected the Worker to deliver a run_agent_response Activity to Router");
  assert.equal(activity.projectionStatus, "projected");
  assert.equal(activity.content.body, marker);
});

test("a foreign-session hook after result isolates the Run with unexpected_post_result_message", async (context) => {
  const harness = postResultHooksHarness(context, "post-result-hooks-other-session-1");
  const { runId, session, instructionUuid } = await postResultHooksStart(harness, "worker-post-result-hooks-other-session-1");

  session.sendMessage({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
  session.sendMessage({ type: "command_lifecycle", command_uuid: instructionUuid, state: "completed", uuid: "11111111-0000-4000-8000-0000000000b2", session_id: SESSION_ID });
  // A foreign session id, not this Run's own -- the exact shape a fake or misdirected hook would
  // carry; unlike the same-session hook above, this must still fail closed.
  session.sendMessage({ type: "system", subtype: "hook_response", hook_id: "hook-1", hook_name: "SessionStart:startup", hook_event: "SessionStart", uuid: "hook-uuid-2", session_id: "99999999-9999-4999-8999-999999999999" });

  const deadline = Date.now() + 10_000;
  while (harness.journal.readRun(runId)?.state !== "observational" && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));

  assert.equal(harness.journal.readRun(runId)?.state, "observational");
  assert.equal(harness.journal.readObservationalCause(runId), "claude-code.unexpected_post_result_message");
});
