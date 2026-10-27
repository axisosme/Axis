import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import test from "node:test";

import { ClaudeCodeExecutor, claudeCodeExecutorRegistration, type ClaudeCodeSpawnOptions } from "../../src/executor/claude-code.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION, executorLifecycleReason, selectExecutorRegistration, type ClaudeCodeLiveBinding, type ExecutorLifecycleEvent, type ExecutorRegistration } from "../../src/shared/worker-executor.js";

// Claude steer writes a UUID-tagged user message on the turn's open stdin. The fake
// models command_lifecycle queued/started/completed, isReplay echoes and steer
// consumption either inside a turn or at a subsequent turn boundary. A queued steer can
// survive an interrupt result until the process receives a signal.

const SESSION_ID = "88888888-8888-4888-8888-888888888888";
const OTHER_SESSION_ID = "99999999-9999-4999-8999-999999999999";
const FOREIGN_UUID = "77777777-7777-4777-8777-777777777777";
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

interface Turn {
  readonly args: readonly string[];
  /** Every stdin line the adapter wrote, parsed. */
  readonly written: Json[];
  /** Signals the adapter sent this child. */
  readonly kills: string[];
  stdinEnded: boolean;
  /** Writes one stdout line as the native CLI. */
  write(message: Json): void;
  /** Writes raw stdout bytes (for a partial line). */
  raw(text: string): void;
}

interface HarnessOptions {
  /** The real CLI exits once stdin closes after an ordinary turn; a CLI still holding a queued steer does not. */
  readonly exitOnStdinEnd?: boolean;
  /** Emit `command_lifecycle` for the instruction before `init`, as 2.1.281 does for a uuid-tagged message. */
  readonly lifecycleBeforeInit?: "own" | "foreign" | "none";
  /** What the fake answers a `control_request` interrupt with. */
  readonly stillQueued?: "pending" | "none";
}

function harness(options: HarnessOptions = {}) {
  const exitOnStdinEnd = options.exitOnStdinEnd ?? true;
  const lifecycleBeforeInit = options.lifecycleBeforeInit ?? "own";
  const turns: Turn[] = [];
  const spawnProcess = (_file: string, args: readonly string[], _options: ClaudeCodeSpawnOptions): ChildProcess => {
    const emitter = new EventEmitter();
    const stdin = args.includes("-p") ? new PassThrough() : null;
    const stdout = new PassThrough();
    let exited = false;
    const exit = (code: number | null, signal: string | null): void => {
      if (exited) return;
      exited = true;
      stdout.end();
      emitter.emit("exit", code, signal);
      emitter.emit("close", code, signal);
    };
    const kills: string[] = [];
    // Records the signal, then exits with it a little later, so lines the test
    // writes in between still reach the adapter before the exit does.
    const kill = (signal: string = "SIGTERM"): boolean => { kills.push(signal); setTimeout(() => exit(null, signal), 50); return true; };
    const child = Object.assign(emitter, { stdin, stdout, stderr: null, kill }) as unknown as ChildProcess;
    queueMicrotask(() => emitter.emit("spawn"));
    if (args[0] === "--help") { queueMicrotask(() => { stdout.end(`${HELP}\n`); emitter.emit("close", 0, null); }); return child; }
    if (args[0] === "auth") { queueMicrotask(() => { stdout.end(); emitter.emit("close", 0, null); }); return child; }
    assert.ok(stdin !== null);
    const turn: Turn = {
      args, written: [], kills, stdinEnded: false,
      write: (message) => { stdout.write(`${JSON.stringify(message)}\n`); },
      raw: (text) => { stdout.write(text); },
    };
    turns.push(turn);
    let interrupted = false;
    let pending = "";
    stdin.setEncoding("utf8");
    stdin.on("data", (chunk: string) => {
      pending += chunk;
      for (let newline = pending.indexOf("\n"); newline >= 0; newline = pending.indexOf("\n")) {
        const line = JSON.parse(pending.slice(0, newline)) as Json;
        pending = pending.slice(newline + 1);
        turn.written.push(line);
        if (line.type === "user" && turn.written.filter((entry) => entry.type === "user").length === 1) queueMicrotask(() => {
          if (lifecycleBeforeInit !== "none") {
            const commandUuid = lifecycleBeforeInit === "own" ? line.uuid : FOREIGN_UUID;
            turn.write({ type: "command_lifecycle", command_uuid: commandUuid, state: "queued", uuid: "11111111-0000-4000-8000-000000000001", session_id: SESSION_ID });
            turn.write({ type: "command_lifecycle", command_uuid: commandUuid, state: "started", uuid: "11111111-0000-4000-8000-000000000002", session_id: SESSION_ID });
          }
          turn.write({ type: "system", subtype: "init", session_id: SESSION_ID });
          turn.write({ type: "user", isReplay: true, uuid: line.uuid, session_id: SESSION_ID });
          turn.write({ type: "assistant", session_id: SESSION_ID });
        });
        if (line.type === "control_request") {
          interrupted = true;
          const queued = options.stillQueued === "pending"
            ? turn.written.filter((entry) => entry.type === "user").slice(1).map((entry) => entry.uuid)
            : [];
          queueMicrotask(() => turn.write({ type: "control_response", response: { subtype: "success", request_id: line.request_id, response: { still_queued: queued } } }));
        }
      }
    });
    stdin.on("end", () => {
      turn.stdinEnded = true;
      if (exitOnStdinEnd) queueMicrotask(() => exit(interrupted ? 1 : 0, null));
    });
    return child;
  };
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
    resultKillGraceMs: 50,
  });
  return { executor, turns };
}

function collect(executor: ClaudeCodeExecutor, binding: ClaudeCodeLiveBinding): { readonly events: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[]; readonly done: Promise<"terminal" | "ambiguous"> } {
  const events: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
  const done = (async () => {
    try { for await (const event of executor.observe(binding)) events.push(event); return "terminal" as const; }
    catch { return "ambiguous" as const; }
  })();
  return { events, done };
}

/**
 * Collect lifecycle outcomes while retaining the ExecutorLifecycleError ambiguity
 * reason, allowing tests to distinguish specific labelled ambiguity paths.
 */
function collectReasoned(executor: ClaudeCodeExecutor, binding: ClaudeCodeLiveBinding): { readonly events: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[]; readonly done: Promise<{ readonly kind: "terminal" } | { readonly kind: "ambiguous"; readonly reason: string | undefined }> } {
  const events: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
  const done = (async () => {
    try { for await (const event of executor.observe(binding)) events.push(event); return { kind: "terminal" as const }; }
    catch (error) { return { kind: "ambiguous" as const, reason: executorLifecycleReason(error) }; }
  })();
  return { events, done };
}

async function until(predicate: () => boolean, label: string): Promise<void> {
  for (let i = 0; i < 200 && !predicate(); i += 1) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(predicate(), label);
}

/** A few turns of the event loop, for asserting that something did NOT happen. */
async function settle(): Promise<void> {
  for (let i = 0; i < 20; i += 1) await new Promise((resolve) => setImmediate(resolve));
}

async function running(executor: ClaudeCodeExecutor) {
  const result = await executor.start({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "start-1", workingDirectory: WORKSPACE, instruction: "Run the long task." });
  assert.equal(result.status, "accepted");
  const binding = result.binding;
  const lifecycle = collect(executor, binding);
  await until(() => lifecycle.events.some((event) => event.outcome === "progressed"), "the turn never progressed");
  return { binding, lifecycle };
}

/** Start a session whose lifecycle collector preserves labelled ambiguity reasons. */
async function runningReasoned(executor: ClaudeCodeExecutor) {
  const result = await executor.start({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "start-reasoned-1", workingDirectory: WORKSPACE, instruction: "Run the long task." });
  assert.equal(result.status, "accepted");
  const binding = result.binding;
  const lifecycle = collectReasoned(executor, binding);
  await until(() => lifecycle.events.some((event) => event.outcome === "progressed"), "the turn never progressed");
  return { binding, lifecycle };
}

function steer(executor: ClaudeCodeExecutor, binding: ClaudeCodeLiveBinding, requestId: string, instruction = "End your reply with the word PINEAPPLE.") {
  return executor.steer({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId, binding, instruction });
}

const userLines = (turn: Turn) => turn.written.filter((line) => line.type === "user");
const outcomes = (events: readonly ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[]) => events.map((event) => event.outcome);
const terminals = (events: readonly ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[]) => events.filter((event) => event.outcome === "succeeded" || event.outcome === "failed" || event.outcome === "cancelled");

test("a steer consumed at a tool boundary joins the running turn, which ends with its one result as succeeded", async () => {
  const { executor, turns } = harness();
  try {
    const registration = claudeCodeExecutorRegistration({ executorId: "claude-test", adapter: executor });
    assert.deepEqual(registration.supportedOperations, ["start", "continue", "cancel", "steer", "observe"]);
    assert.equal(selectExecutorRegistration({ runtimeKind: "claude-code", executorId: "claude-test", methodRef: { version: "v1", method: "claude-code.execute" } }, [registration as unknown as ExecutorRegistration]).adapter, executor);
    const { binding, lifecycle } = await running(executor);
    const turn = turns[0]!;
    assert.ok(turn.args.includes("--replay-user-messages"));

    const result = await steer(executor, binding, "steer-1");
    assert.deepEqual(result, { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "steer-1", operation: "steer", status: "steered", binding });
    const [instructionLine, steerLine] = userLines(turn);
    assert.ok(instructionLine !== undefined && steerLine !== undefined);
    // Every user message carries this adapter's own uuid, each a fresh one.
    for (const line of [instructionLine, steerLine]) {
      assert.deepEqual(Object.keys(line).sort(), ["message", "type", "uuid"]);
      assert.match(String(line.uuid), /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
    }
    assert.notEqual(steerLine.uuid, instructionLine.uuid);
    assert.notEqual(steerLine.uuid, SESSION_ID);
    // The steer carries the instruction alone, like a continuation.
    assert.deepEqual(steerLine.message, { role: "user", content: "End your reply with the word PINEAPPLE." });

    // Consumed after the running tool's result, inside the same turn.
    turn.write({ type: "user", session_id: SESSION_ID });
    turn.write({ type: "user", isReplay: true, uuid: steerLine.uuid, session_id: SESSION_ID });
    turn.write({ type: "assistant", session_id: SESSION_ID });
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    turn.write({ type: "command_lifecycle", command_uuid: steerLine.uuid, state: "completed", uuid: "11111111-0000-4000-8000-000000000003", session_id: SESSION_ID });
    turn.write({ type: "command_lifecycle", command_uuid: instructionLine.uuid, state: "completed", uuid: "11111111-0000-4000-8000-000000000004", session_id: SESSION_ID });

    assert.equal(await lifecycle.done, "terminal");
    assert.deepEqual(outcomes(lifecycle.events), ["started", "progressed", "succeeded"]);
    assert.deepEqual(turn.kills, []);
  } finally {
    executor.close();
  }
});

test("a steer that misses the turn's final text keeps the Run open through the further turn it opens, which ends it once", async () => {
  const { executor, turns } = harness();
  try {
    const { binding, lifecycle } = await running(executor);
    const turn = turns[0]!;
    assert.equal((await steer(executor, binding, "steer-gen")).status, "steered");
    const steerUuid = userLines(turn)[1]!.uuid;

    // The turn finishes without having read the steer.
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    await settle();
    assert.deepEqual(terminals(lifecycle.events), []);
    assert.equal(turn.stdinEnded, false, "stdin must stay open for the turn the pending steer opens");

    // The same process opens a further turn for it.
    turn.write({ type: "command_lifecycle", command_uuid: steerUuid, state: "started", uuid: "11111111-0000-4000-8000-000000000005", session_id: SESSION_ID });
    turn.write({ type: "system", subtype: "init", session_id: SESSION_ID });
    turn.write({ type: "user", isReplay: true, uuid: steerUuid, session_id: SESSION_ID });
    turn.write({ type: "assistant", session_id: SESSION_ID });
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    turn.write({ type: "command_lifecycle", command_uuid: steerUuid, state: "completed", uuid: "11111111-0000-4000-8000-000000000006", session_id: SESSION_ID });

    assert.equal(await lifecycle.done, "terminal");
    assert.deepEqual(outcomes(lifecycle.events), ["started", "progressed", "succeeded"]);
    assert.equal(terminals(lifecycle.events).length, 1);
    assert.equal(turn.stdinEnded, true);
  } finally {
    executor.close();
  }
});

test("a steer after the turn's result is too late and leaves the turn to succeed", async () => {
  const { executor, turns } = harness();
  try {
    const { binding, lifecycle } = await running(executor);
    const turn = turns[0]!;
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    await until(() => turn.stdinEnded, "stdin never closed after the result");
    const late = await steer(executor, binding, "steer-late");
    assert.equal(late.status, "failed");
    assert.equal(late.status === "failed" && late.error.code, "executor.steer_too_late");
    assert.deepEqual(late.binding, binding);
    assert.equal(userLines(turn).length, 1, "a refused steer writes nothing");
    assert.equal(await lifecycle.done, "terminal");
    assert.equal(lifecycle.events.at(-1)!.outcome, "succeeded");
  } finally {
    executor.close();
  }
});

test("a steer after a cancel is refused as too late: stop means everything stops", async () => {
  const { executor, turns } = harness();
  try {
    const { binding, lifecycle } = await running(executor);
    const turn = turns[0]!;
    assert.equal((await executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-1", binding })).status, "interrupt_requested");
    const refused = await steer(executor, binding, "steer-after-cancel");
    assert.equal(refused.status === "failed" && refused.error.code, "executor.steer_too_late");
    assert.equal(userLines(turn).length, 1);
    turn.write({ type: "result", subtype: "error_during_execution", is_error: true, session_id: SESSION_ID });
    assert.equal(await lifecycle.done, "terminal");
    assert.equal(lifecycle.events.at(-1)!.outcome, "cancelled");
  } finally {
    executor.close();
  }
});

test("a cancel that meets a queued steer stops the process after the interrupted result, discarding the turn that would follow", async () => {
  const { executor, turns } = harness({ exitOnStdinEnd: false, stillQueued: "pending" });
  try {
    const { binding, lifecycle } = await running(executor);
    const turn = turns[0]!;
    assert.equal((await steer(executor, binding, "steer-queued")).status, "steered");
    const [instructionLine, steerLine] = userLines(turn);
    const cancel = await executor.cancel({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "cancel-queued", binding });
    assert.equal(cancel.status, "interrupt_requested");

    turn.write({ type: "user", session_id: SESSION_ID });
    turn.write({ type: "result", subtype: "error_during_execution", is_error: true, session_id: SESSION_ID });
    // Measured on 2.1.281: the process keeps writing until the signal lands --
    // `command_lifecycle` at once, then a new `init`, the queued steer's echo.
    turn.write({ type: "command_lifecycle", command_uuid: instructionLine!.uuid, state: "completed", uuid: "11111111-0000-4000-8000-000000000007", session_id: SESSION_ID });
    turn.write({ type: "command_lifecycle", command_uuid: steerLine!.uuid, state: "started", uuid: "11111111-0000-4000-8000-000000000008", session_id: SESSION_ID });
    await settle();
    turn.write({ type: "system", subtype: "init", session_id: SESSION_ID });
    turn.write({ type: "user", isReplay: true, uuid: steerLine!.uuid, session_id: SESSION_ID });
    turn.raw('{"type":"assistant","session_id":');

    assert.equal(await lifecycle.done, "terminal");
    assert.deepEqual(outcomes(lifecycle.events), ["started", "progressed", "cancelled"]);
    assert.deepEqual(turn.kills, ["SIGTERM"], "the interrupted result starts the reap at once");
    assert.equal(turn.stdinEnded, true);
    assert.deepEqual(claudeCodeExecutorRegistration({ executorId: "claude-test", adapter: executor }).projectLifecycle({ event: lifecycle.events.at(-1) as Exclude<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>, { readonly outcome: "started" }>, runId: "run-1" }), { kind: "failed", code: "execution_cancelled", safeMessage: "Execution was cancelled." });
  } finally {
    executor.close();
  }
});

test("a second init with no steer pending is still ambiguous", async () => {
  const { executor, turns } = harness();
  try {
    const { lifecycle } = await running(executor);
    turns[0]!.write({ type: "system", subtype: "init", session_id: SESSION_ID });
    assert.equal(await lifecycle.done, "ambiguous");
    assert.deepEqual(terminals(lifecycle.events), []);
  } finally {
    executor.close();
  }
});

test("steer is idempotent per request ID, instruction and binding, and refuses what it cannot deliver", async () => {
  const { executor, turns } = harness();
  try {
    const { binding, lifecycle } = await running(executor);
    const turn = turns[0]!;
    const first = steer(executor, binding, "steer-dup");
    const again = steer(executor, { runtimeKind: "claude-code", sessionId: SESSION_ID }, "steer-dup");
    assert.equal(again, first);
    assert.equal((await first).status, "steered");
    assert.equal(userLines(turn).length, 2, "one request ID writes one steer");

    const otherInstruction = await steer(executor, binding, "steer-dup", "Something else.");
    assert.equal(otherInstruction.status === "failed" && otherInstruction.error.code, "executor.idempotency_conflict");
    const otherBinding = await steer(executor, { runtimeKind: "claude-code", sessionId: OTHER_SESSION_ID }, "steer-dup");
    assert.equal(otherBinding.status === "failed" && otherBinding.error.code, "executor.idempotency_conflict");
    const noSession = await steer(executor, { runtimeKind: "claude-code", sessionId: OTHER_SESSION_ID }, "steer-nosession");
    assert.equal(noSession.status === "failed" && noSession.error.code, "executor.capability_unavailable");
    for (const invalid of [
      { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "steer-blank", binding, instruction: "  " },
      { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: " ", binding, instruction: "Go." },
      { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "steer-bad-binding", binding: { runtimeKind: "claude-code", sessionId: "not-a-uuid" }, instruction: "Go." },
      { contractVersion: "0.0.0", requestId: "steer-bad-version", binding, instruction: "Go." },
    ]) {
      const result = await executor.steer(invalid as Parameters<ClaudeCodeExecutor["steer"]>[0]);
      assert.equal(result.status, "invalid_request");
      assert.equal(result.status === "invalid_request" && result.error.code, "executor.invalid_request");
    }
    assert.equal(userLines(turn).length, 2);

    turn.write({ type: "user", isReplay: true, uuid: userLines(turn)[1]!.uuid, session_id: SESSION_ID });
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    assert.equal(await lifecycle.done, "terminal");
    assert.equal(lifecycle.events.at(-1)!.outcome, "succeeded");
  } finally {
    executor.close();
  }
});

test("command_lifecycle for this session's own messages is tolerated before init and after the result; a foreign one before init is not", async () => {
  const own = harness({ lifecycleBeforeInit: "own" });
  try {
    const { lifecycle } = await running(own.executor);
    const turn = own.turns[0]!;
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    turn.write({ type: "command_lifecycle", command_uuid: userLines(turn)[0]!.uuid, state: "completed", uuid: "11111111-0000-4000-8000-000000000009", session_id: SESSION_ID });
    assert.equal(await lifecycle.done, "terminal");
    assert.deepEqual(outcomes(lifecycle.events), ["started", "progressed", "succeeded"]);
  } finally {
    own.executor.close();
  }

  const foreign = harness({ lifecycleBeforeInit: "foreign" });
  try {
    const result = await foreign.executor.start({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "start-foreign", workingDirectory: WORKSPACE, instruction: "Run." });
    assert.equal(result.status === "failed" && result.error.code, "executor.launch_ambiguous");
  } finally {
    foreign.executor.close();
  }
});

// A same-session SessionStart hook may arrive after result and this instruction's
// command_lifecycle completed report. Tolerate the three SessionStart subtypes while
// awaiting exit; foreign-session hooks, other hook events and unexplained messages
// remain distinctly labelled ambiguity cases.
for (const subtype of ["hook_started", "hook_progress", "hook_response"]) {
  test(`a same-session SessionStart ${subtype} after the result and the own command_lifecycle completed is tolerated, and the run still succeeds with its answer`, async () => {
    const { executor, turns } = harness();
    try {
      const { lifecycle } = await running(executor);
      const turn = turns[0]!;
      turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID, result: "POST-RESULT-MARKER: done." });
      turn.write({ type: "command_lifecycle", command_uuid: userLines(turn)[0]!.uuid, state: "completed", uuid: "11111111-0000-4000-8000-0000000000a1", session_id: SESSION_ID });
      turn.write({ type: "system", subtype, hook_id: "hook-1", hook_name: "SessionStart:startup", hook_event: "SessionStart", uuid: "hook-uuid-1", session_id: SESSION_ID });
      assert.equal(await lifecycle.done, "terminal");
      assert.deepEqual(outcomes(lifecycle.events), ["started", "progressed", "succeeded"]);
      const succeeded = lifecycle.events.at(-1);
      assert.ok(succeeded && succeeded.outcome === "succeeded");
      assert.equal(succeeded.response?.body, "POST-RESULT-MARKER: done.");
    } finally {
      executor.close();
    }
  });
}

test("a SessionStart hook for another session after the result stays ambiguous, labelled distinguishably from other post-result ambiguity", async () => {
  const { executor, turns } = harness();
  try {
    const { lifecycle } = await runningReasoned(executor);
    const turn = turns[0]!;
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    turn.write({ type: "command_lifecycle", command_uuid: userLines(turn)[0]!.uuid, state: "completed", uuid: "11111111-0000-4000-8000-0000000000a2", session_id: SESSION_ID });
    turn.write({ type: "system", subtype: "hook_response", hook_id: "hook-1", hook_name: "SessionStart:startup", hook_event: "SessionStart", uuid: "hook-uuid-2", session_id: OTHER_SESSION_ID });
    const done = await lifecycle.done;
    assert.deepEqual(done, { kind: "ambiguous", reason: "claude-code.unexpected_post_result_message" });
  } finally {
    executor.close();
  }
});

test("a non-SessionStart hook_event after the result stays ambiguous, labelled distinguishably", async () => {
  const { executor, turns } = harness();
  try {
    const { lifecycle } = await runningReasoned(executor);
    const turn = turns[0]!;
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    turn.write({ type: "command_lifecycle", command_uuid: userLines(turn)[0]!.uuid, state: "completed", uuid: "11111111-0000-4000-8000-0000000000a3", session_id: SESSION_ID });
    turn.write({ type: "system", subtype: "hook_response", hook_id: "hook-1", hook_name: "PreToolUse", hook_event: "PreToolUse", uuid: "hook-uuid-3", session_id: SESSION_ID });
    const done = await lifecycle.done;
    assert.deepEqual(done, { kind: "ambiguous", reason: "claude-code.unexpected_post_result_message" });
  } finally {
    executor.close();
  }
});

test("any other unexplained line after the result still stays ambiguous, labelled distinguishably", async () => {
  const { executor, turns } = harness();
  try {
    const { lifecycle } = await runningReasoned(executor);
    const turn = turns[0]!;
    turn.write({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID });
    turn.write({ type: "command_lifecycle", command_uuid: userLines(turn)[0]!.uuid, state: "completed", uuid: "11111111-0000-4000-8000-0000000000a4", session_id: SESSION_ID });
    turn.write({ type: "assistant", session_id: SESSION_ID });
    const done = await lifecycle.done;
    assert.deepEqual(done, { kind: "ambiguous", reason: "claude-code.unexpected_post_result_message" });
  } finally {
    executor.close();
  }
});

test("a registration that lists steer without implementing it is invalid", () => {
  const { executor } = harness();
  const registration = claudeCodeExecutorRegistration({ executorId: "claude-test", adapter: executor });
  const withoutSteer = { ...registration, adapter: { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, start: executor.start.bind(executor), continue: executor.continue.bind(executor), cancel: executor.cancel.bind(executor), observe: executor.observe.bind(executor) } };
  assert.throws(() => selectExecutorRegistration({ runtimeKind: "claude-code", executorId: "claude-test", methodRef: { version: "v1", method: "claude-code.execute" } }, [withoutSteer as unknown as ExecutorRegistration]), /registration is invalid/u);
  executor.close();
});
