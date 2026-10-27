import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import test from "node:test";

import { ClaudeCodeExecutor, claudeCodeExecutorRegistration, type ClaudeCodeSpawnOptions } from "../../src/executor/claude-code.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION, type ClaudeCodeLiveBinding, type ExecutorLifecycleEvent } from "../../src/shared/worker-executor.js";

// Claude cancel writes a control_request interrupt on the turn's open stdin. The fake
// returns a matching successful control_response, interrupted-turn user messages and an
// error_during_execution result, then exits 1 without a signal once stdin closes.

const SESSION_ID = "55555555-5555-4555-8555-555555555555";
const OTHER_SESSION_ID = "66666666-6666-4666-8666-666666666666";
const WORKSPACE = "/tmp/axis-claude-workspace";
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

type Json = Readonly<Record<string, unknown>>;
type InterruptAnswer = "success" | "silent" | "unknown-id";

interface Turn {
  readonly args: readonly string[];
  readonly controlRequests: Json[];
  /** Writes one stdout line as the native CLI. */
  write(message: Json): void;
}

function harness(answer: InterruptAnswer, controlResponseTimeoutMs = 10_000) {
  const turns: Turn[] = [];
  const spawnProcess = (_file: string, args: readonly string[], _options: ClaudeCodeSpawnOptions): ChildProcess => {
    const emitter = new EventEmitter();
    const stdin = args.includes("-p") ? new PassThrough() : null;
    const stdout = new PassThrough();
    const child = Object.assign(emitter, { stdin, stdout, stderr: null, kill: () => true }) as unknown as ChildProcess;
    queueMicrotask(() => emitter.emit("spawn"));
    if (args[0] === "--help") { queueMicrotask(() => { stdout.end(`${HELP}\n`); emitter.emit("close", 0, null); }); return child; }
    if (args[0] === "auth") { queueMicrotask(() => { stdout.end(); emitter.emit("close", 0, null); }); return child; }
    assert.ok(stdin !== null);
    const turn: Turn = { args, controlRequests: [], write: (message) => { stdout.write(`${JSON.stringify(message)}\n`); } };
    turns.push(turn);
    let interrupted = false;
    let pending = "";
    stdin.setEncoding("utf8");
    stdin.on("data", (chunk: string) => {
      pending += chunk;
      for (let newline = pending.indexOf("\n"); newline >= 0; newline = pending.indexOf("\n")) {
        const line = JSON.parse(pending.slice(0, newline)) as Json;
        pending = pending.slice(newline + 1);
        if (line.type === "user") queueMicrotask(() => {
          turn.write({ type: "system", subtype: "init", session_id: SESSION_ID });
          turn.write({ type: "assistant", session_id: SESSION_ID });
        });
        if (line.type === "control_request") {
          turn.controlRequests.push(line);
          if (answer === "silent") continue;
          interrupted = true;
          const requestId = answer === "success" ? line.request_id : "77777777-7777-4777-8777-777777777777";
          queueMicrotask(() => {
            turn.write({ type: "control_response", response: { subtype: "success", request_id: requestId, response: { still_queued: [] } } });
            turn.write({ type: "user", session_id: SESSION_ID });
            turn.write({ type: "user", session_id: SESSION_ID });
            turn.write({ type: "result", subtype: "error_during_execution", is_error: true, session_id: SESSION_ID });
          });
        }
      }
    });
    // The real CLI exits 1 once stdin closes after an interrupted turn, 0 after
    // an ordinary one.
    stdin.on("end", () => queueMicrotask(() => {
      const code = interrupted ? 1 : 0;
      stdout.end();
      emitter.emit("exit", code, null);
      emitter.emit("close", code, null);
    }));
    return child;
  };
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
    controlResponseTimeoutMs,
  });
  return { executor, turns };
}

/** Drains one turn's lifecycle; `ambiguous` records the adapter refusing to report a terminal. */
function collect(executor: ClaudeCodeExecutor, binding: ClaudeCodeLiveBinding): { readonly events: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[]; readonly done: Promise<"terminal" | "ambiguous"> } {
  const events: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
  const done = (async () => {
    try { for await (const event of executor.observe(binding)) events.push(event); return "terminal" as const; }
    catch { return "ambiguous" as const; }
  })();
  return { events, done };
}

async function started(executor: ClaudeCodeExecutor, requestId = "start-1"): Promise<ClaudeCodeLiveBinding> {
  const result = await executor.start({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId, workingDirectory: WORKSPACE, instruction: "Count slowly." });
  assert.equal(result.status, "accepted");
  return result.binding;
}

/** Waits until the first `assistant` line has been consumed, so the session is `running` with progress seen. */
async function progressed(events: readonly ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[]): Promise<void> {
  for (let i = 0; i < 100 && !events.some((event) => event.outcome === "progressed"); i += 1) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(events.some((event) => event.outcome === "progressed"));
}

const registration = (executor: ClaudeCodeExecutor) => claudeCodeExecutorRegistration({ executorId: "claude-test", adapter: executor });

test("cancel interrupts the running turn, which ends cancelled and projects as execution_cancelled; continue then resumes the session", async () => {
  const { executor, turns } = harness("success");
  try {
    assert.deepEqual(registration(executor).supportedOperations, ["start", "continue", "cancel", "steer", "observe"]);
    const binding = await started(executor);
    const first = collect(executor, binding);
    await progressed(first.events);

    const result = await executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-1", binding });
    assert.deepEqual(result, { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-1", operation: "cancel", status: "interrupt_requested", binding });
    assert.equal(turns[0]!.controlRequests.length, 1);
    const request = turns[0]!.controlRequests[0]!;
    assert.deepEqual(Object.keys(request).sort(), ["request", "request_id", "type"]);
    assert.deepEqual(request.request, { subtype: "interrupt" });

    assert.equal(await first.done, "terminal");
    assert.deepEqual(first.events.map((event) => event.outcome), ["started", "progressed", "cancelled"]);
    const terminal = first.events.at(-1)!;
    assert.ok(terminal.outcome === "cancelled");
    assert.deepEqual(registration(executor).projectLifecycle({ event: terminal, runId: "run-1" }), { kind: "failed", code: "execution_cancelled", safeMessage: "Execution was cancelled." });

    const continued = await executor.continue({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "continue-1", workingDirectory: WORKSPACE, instruction: "Carry on.", binding });
    assert.equal(continued.status, "accepted");
    assert.deepEqual(turns[1]!.args.slice(0, 3), ["-p", "--resume", SESSION_ID]);
    const second = collect(executor, binding);
    await progressed(second.events);
    turns[1]!.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    assert.equal(await second.done, "terminal");
    assert.equal(second.events.at(-1)!.outcome, "succeeded");
  } finally {
    executor.close();
  }
});

test("an interrupt the CLI never answers fails the cancel without making the session ambiguous", async () => {
  const { executor, turns } = harness("silent", 20);
  try {
    const binding = await started(executor);
    const lifecycle = collect(executor, binding);
    await progressed(lifecycle.events);
    const result = await executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-silent", binding });
    assert.equal(result.status, "failed");
    assert.equal(result.status === "failed" && result.error.code, "executor.interrupt_failed");
    assert.deepEqual(result.binding, binding);
    // The turn is still live: its own result still finalizes it.
    turns[0]!.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    assert.equal(await lifecycle.done, "terminal");
    assert.equal(lifecycle.events.at(-1)!.outcome, "succeeded");
  } finally {
    executor.close();
  }
});

test("cancel is idempotent per request ID and refuses the same ID for another binding", async () => {
  const { executor, turns } = harness("success");
  try {
    const binding = await started(executor);
    const lifecycle = collect(executor, binding);
    await progressed(lifecycle.events);
    const first = executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-dup", binding });
    const again = executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-dup", binding: { runtimeKind: "claude-code", sessionId: SESSION_ID } });
    assert.equal(again, first);
    assert.equal((await first).status, "interrupt_requested");
    assert.equal(turns[0]!.controlRequests.length, 1);
    const conflict = await executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-dup", binding: { runtimeKind: "claude-code", sessionId: OTHER_SESSION_ID } });
    assert.equal(conflict.status === "failed" && conflict.error.code, "executor.idempotency_conflict");
    const invalid = await executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-bad", binding: { runtimeKind: "claude-code", sessionId: "not-a-uuid" } });
    assert.equal(invalid.status, "invalid_request");
    assert.equal(invalid.status === "invalid_request" && invalid.error.code, "executor.invalid_request");
    assert.equal(await lifecycle.done, "terminal");
  } finally {
    executor.close();
  }
});

test("a control_response for a request this adapter did not send makes the session ambiguous", async () => {
  const { executor } = harness("unknown-id");
  try {
    const binding = await started(executor);
    const lifecycle = collect(executor, binding);
    await progressed(lifecycle.events);
    const result = await executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-unknown", binding });
    // The session settled ambiguous before any matching answer, which fails the pending interrupt.
    assert.equal(result.status === "failed" && result.error.code, "executor.interrupt_failed");
    assert.equal(await lifecycle.done, "ambiguous");
    assert.equal(lifecycle.events.some((event) => event.outcome === "cancelled"), false);
  } finally {
    executor.close();
  }
});
