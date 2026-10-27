import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import test from "node:test";

import { ClaudeCodeExecutor, type ClaudeCodeSpawnOptions } from "../../src/executor/claude-code.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION, type ClaudeCodeLiveBinding, type ExecutorLifecycleEvent } from "../../src/shared/worker-executor.js";

const SESSION_ID = "11111111-1111-4111-8111-111111111111";
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

interface SpawnCall {
  readonly args: readonly string[];
  readonly options: ClaudeCodeSpawnOptions;
}

function runtime() {
  const calls: SpawnCall[] = [];
  const spawnProcess = (_file: string, args: readonly string[], options: ClaudeCodeSpawnOptions): ChildProcess => {
    const emitter = new EventEmitter();
    const task = args.includes("-p");
    const stdin = task ? new PassThrough() : null;
    const stdout = new PassThrough();
    const child = Object.assign(emitter, { stdin, stdout, stderr: null, kill: () => true }) as unknown as ChildProcess;
    calls.push({ args: [...args], options });
    queueMicrotask(() => emitter.emit("spawn"));
    if (args[0] === "--help") queueMicrotask(() => { stdout.end(`${HELP}\n`); emitter.emit("close", 0, null); });
    else if (args[0] === "auth") queueMicrotask(() => { stdout.end(); emitter.emit("close", 0, null); });
    else {
      assert.ok(stdin !== null);
      // Emit init on the first stream-json stdin line while the turn's input remains
      // open.
      stdin.once("data", () => queueMicrotask(() => {
        stdout.write(`${JSON.stringify({ type: "system", subtype: "init", session_id: SESSION_ID })}\n`);
        stdout.write(`${JSON.stringify({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID, result: "done" })}\n`);
        stdout.end();
        emitter.emit("close", 0, null);
      }));
      stdin.resume();
    }
    return child;
  };
  return { calls, spawnProcess };
}

test("Claude inherits the service environment, loads native customizations, and bypasses permission prompts", async () => {
  const fake = runtime();
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin", CUSTOM_AGENT_ENV: "available" },
  });
  try {
    const accepted = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "full-access",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Run the task.",
    });
    assert.equal(accepted.status, "accepted");
    const launch = fake.calls.find((call) => call.args.includes("-p"));
    assert.ok(launch);
    assert.equal(launch.args.includes("--safe-mode"), false);
    assert.equal(launch.args[launch.args.indexOf("--permission-mode") + 1], "bypassPermissions");
    assert.equal(launch.options.env.PATH, "/operator/bin");
    assert.equal(launch.options.env.CUSTOM_AGENT_ENV, "available");
  } finally {
    executor.close();
  }
});

// Launch with streaming input and keep stdin open until the result arrives so interrupt
// and steer messages can be written during the turn.
test("streaming input: argv carries --input-format stream-json, stdin stays open until the result, and the turn still reaches succeeded", async () => {
  const launches: (readonly string[])[] = [];
  const stdins: PassThrough[] = [];
  const resultSenders: (() => void)[] = [];
  const spawnProcess = (_file: string, args: readonly string[], _options: ClaudeCodeSpawnOptions): ChildProcess => {
    const emitter = new EventEmitter();
    const task = args.includes("-p");
    const taskStdin = task ? new PassThrough() : null;
    const stdout = new PassThrough();
    const child = Object.assign(emitter, { stdin: taskStdin, stdout, stderr: null, kill: () => true }) as unknown as ChildProcess;
    queueMicrotask(() => emitter.emit("spawn"));
    if (args[0] === "--help") { queueMicrotask(() => { stdout.end(`${HELP}\n`); emitter.emit("close", 0, null); }); return child; }
    if (args[0] === "auth") { queueMicrotask(() => { stdout.end(); emitter.emit("close", 0, null); }); return child; }
    assert.ok(taskStdin !== null);
    launches.push([...args]);
    stdins.push(taskStdin);
    taskStdin.once("data", () => queueMicrotask(() => stdout.write(`${JSON.stringify({ type: "system", subtype: "init", session_id: SESSION_ID })}\n`)));
    taskStdin.resume();
    resultSenders.push(() => {
      stdout.write(`${JSON.stringify({ type: "result", subtype: "success", is_error: false, session_id: SESSION_ID, result: "done" })}\n`);
      queueMicrotask(() => { emitter.emit("exit", 0, null); emitter.emit("close", 0, null); });
    });
    return child;
  };
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
  try {
    const accepted = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "streaming-input",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Run the task.",
    });
    assert.equal(accepted.status, "accepted");
    assert.ok(accepted.status === "accepted");
    const launch = launches.find((args) => args.includes("-p"));
    assert.ok(launch);
    assert.equal(launch[launch.indexOf("--output-format") + 1], "stream-json");
    assert.equal(launch[launch.indexOf("--input-format") + 1], "stream-json");
    const stdin = stdins[0];
    assert.ok(stdin);
    assert.equal(stdin.writableEnded, false, "stdin must still be open after init, before the result");

    const binding = accepted.binding as ClaudeCodeLiveBinding;
    const collected: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
    const consumption = (async () => { for await (const event of executor.observe(binding)) collected.push(event); })();

    const sendResult = resultSenders[0];
    assert.ok(sendResult);
    sendResult();
    await consumption;

    assert.equal(stdin.writableEnded, true, "stdin must be closed once the result has arrived");
    assert.ok(collected.some((event) => event.outcome === "succeeded"), "expected the turn to reach succeeded");
  } finally {
    executor.close();
  }
});
