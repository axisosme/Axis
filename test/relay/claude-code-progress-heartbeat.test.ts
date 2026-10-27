import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import test from "node:test";

import { ClaudeCodeExecutor, claudeCodeExecutorRegistration, type ClaudeCodeSpawnOptions } from "../../src/executor/claude-code.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION, type ClaudeCodeLiveBinding, type ExecutorLifecycleEvent } from "../../src/shared/worker-executor.js";

// A running Claude session emits recurring safe progress heartbeats during silence.
// Heartbeat text must survive lifecycle projection and vary between emissions so the
// Worker's commentary digest does not permanently deduplicate every heartbeat after the
// first.

const SESSION_ID = "22222222-2222-4222-8222-222222222222";
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
    const fake: FakeSession = {
      stdout,
      sendMessage: (message) => stdout.write(`${JSON.stringify(message)}\n`),
      exit: (code, signal) => { emitter.emit("exit", code, signal); emitter.emit("close", code, signal); },
    };
    // Emit init on the first stream-json stdin line while the turn's input remains
    // open.
    stdin.once("data", () => queueMicrotask(() => fake.sendMessage({ type: "system", subtype: "init", session_id: SESSION_ID })));
    stdin.resume();
    sessions.push(fake);
    return child;
  };
  return { spawnProcess, sessions };
}

/** A step-counting clock, decoupled from wall time: every call advances by exactly `step`. */
function controlledClock(step: number): () => number {
  let calls = 0;
  return () => {
    const value = calls * step;
    calls += 1;
    return value;
  };
}

function heartbeats(events: readonly ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[]): readonly Extract<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>, { readonly outcome: "progressed" }>[] {
  return events.filter((event): event is Extract<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>, { readonly outcome: "progressed" }> => event.outcome === "progressed" && event.progress.kind === "commentary");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitUntil(predicate: () => boolean, timeoutMs = 2_000, stepMs = 5): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return;
    if (Date.now() > deadline) throw new Error("Timed out waiting for condition.");
    await sleep(stepMs);
  }
}

test("a silently running session emits distinct, increasing-elapsed heartbeat commentary (and survives the dedup trap)", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
    progressHeartbeatIntervalMs: 25,
    now: controlledClock(25),
  });
  const registration = claudeCodeExecutorRegistration({ executorId: "claude-1", adapter: executor });
  try {
    const accepted = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "heartbeat-silence",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Run the long silent task.",
    });
    assert.equal(accepted.status, "accepted");
    assert.ok(accepted.status === "accepted");
    const binding = accepted.binding as ClaudeCodeLiveBinding;

    const collected: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
    for await (const event of executor.observe(binding)) {
      collected.push(event);
      if (heartbeats(collected).length >= 3) break;
    }

    const beats = heartbeats(collected);
    assert.ok(beats.length >= 3, "expected at least three heartbeat commentary events");
    const projected = beats.map((event) => registration.projectLifecycle({ event, runId: "run-1" }));
    const messages = projected.map((entry) => { assert.equal(entry.kind, "progress"); return entry.kind === "progress" ? entry.safeMessage : ""; });

    // recordCommentary deduplicates each run_id/commentary_digest pair. Require
    // distinct heartbeat bodies so recurring timer events remain observable downstream.
    assert.equal(new Set(messages).size, messages.length, "heartbeat messages must be pairwise distinct");

    const minutes = messages.map((message) => Number(/about (\d+) minutes elapsed/u.exec(message)?.[1]));
    assert.ok(minutes.every((value) => Number.isFinite(value)), `expected every message to carry an elapsed-minutes token: ${JSON.stringify(messages)}`);
    for (let index = 1; index < minutes.length; index += 1) assert.ok(minutes[index]! > minutes[index - 1]!, `expected increasing elapsed minutes, got ${JSON.stringify(minutes)}`);
  } finally {
    executor.close();
  }
});

test("a real accepted message mid-window resets the heartbeat window", async () => {
  const fake = runtime();
  // Real wall-clock timing, not a controlled clock (this test's whole point is
  // that the real scheduled timer gets replaced). A wider interval buys a wider
  // absolute margin around both boundaries below, since real setTimeout jitter
  // is roughly constant in absolute terms, not proportional to the interval.
  const intervalMs = 200;
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
    progressHeartbeatIntervalMs: intervalMs,
  });
  try {
    const accepted = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "heartbeat-reset",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Run the task with a mid-window message.",
    });
    assert.equal(accepted.status, "accepted");
    assert.ok(accepted.status === "accepted");
    const binding = accepted.binding as ClaudeCodeLiveBinding;
    const session = fake.sessions[0];
    assert.ok(session);

    const collected: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
    void (async () => { for await (const event of executor.observe(binding)) collected.push(event); })();

    // Real progress at t=60ms: this must push the next heartbeat out to
    // ~260ms after session start, not let the original ~200ms schedule fire.
    const resetAtMs = 60;
    await sleep(resetAtMs);
    session.sendMessage({ type: "assistant", message: { content: [] } });

    // t=230ms: 30ms past the ORIGINAL boundary (200ms) -- proving that boundary
    // did not fire -- and 30ms before the reset boundary (60+200=260ms). Both
    // margins are comfortably above ordinary event-loop/timer jitter.
    await sleep(230 - resetAtMs);
    assert.equal(heartbeats(collected).length, 0, "the window must have been reset by the real message, not have fired on the original schedule");

    // Past the reset window (measured from the reset point): exactly one heartbeat.
    await waitUntil(() => heartbeats(collected).length >= 1, 2_000);
    assert.equal(heartbeats(collected).length, 1);
  } finally {
    executor.close();
  }
});

test("no further heartbeat is produced once the session leaves running", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
    progressHeartbeatIntervalMs: 25,
  });
  try {
    const accepted = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "heartbeat-terminal",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Run the task that then completes.",
    });
    assert.equal(accepted.status, "accepted");
    assert.ok(accepted.status === "accepted");
    const binding = accepted.binding as ClaudeCodeLiveBinding;
    const session = fake.sessions[0];
    assert.ok(session);

    const collected: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
    const consumption = (async () => { for await (const event of executor.observe(binding)) collected.push(event); })();

    await waitUntil(() => heartbeats(collected).length >= 1, 2_000);
    const countAtExit = heartbeats(collected).length;

    session.sendMessage({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID, result: "done" });
    session.exit(0, null);
    await consumption;

    assert.ok(collected.some((event) => event.outcome === "succeeded"), "expected the session to reach a terminal succeeded event");

    // Give several more windows' worth of real time: nothing should be added.
    await sleep(25 * 5);
    assert.equal(heartbeats(collected).length, countAtExit, "no heartbeat may be produced after the session left running");
  } finally {
    executor.close();
  }
});

test("projectLifecycle carries the heartbeat's commentary safeMessage through, and keeps the fixed string for generic progress", () => {
  const executor = new ClaudeCodeExecutor({ command: "/opt/claude/bin/claude" });
  const registration = claudeCodeExecutorRegistration({ executorId: "claude-1", adapter: executor });
  const binding: ClaudeCodeLiveBinding = { runtimeKind: "claude-code", sessionId: SESSION_ID };

  const commentary = registration.projectLifecycle({
    event: { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding, sequence: 1, outcome: "progressed", progress: { kind: "commentary", safeMessage: "Claude Code is still running; about 5 minutes elapsed." } },
    runId: "run-1",
  });
  assert.deepEqual(commentary, { kind: "progress", completed: 1, total: 1, safeMessage: "Claude Code is still running; about 5 minutes elapsed." });

  const generic = registration.projectLifecycle({
    event: { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding, sequence: 2, outcome: "progressed", progress: { kind: "generic" } },
    runId: "run-1",
  });
  assert.deepEqual(generic, { kind: "progress", completed: 1, total: 1, safeMessage: "Claude Code session is progressing." });
});
