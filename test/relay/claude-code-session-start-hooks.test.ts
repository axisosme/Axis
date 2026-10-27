import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import test from "node:test";

import { ClaudeCodeExecutor, type ClaudeCodeSpawnOptions } from "../../src/executor/claude-code.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION } from "../../src/shared/worker-executor.js";

// SessionStart hook_started, hook_progress and hook_response messages may precede init.
// The adapter must tolerate those startup messages without classifying a valid launch
// as ambiguous.

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

/** Shapes copied from the real 2.1.280 stream (payload fields trimmed). */
function hook(subtype: string, overrides: Readonly<Record<string, unknown>> = {}): Readonly<Record<string, unknown>> {
  return { type: "system", subtype, hook_id: "hook-1", hook_name: "SessionStart:startup", hook_event: "SessionStart", uuid: "u", session_id: SESSION_ID, ...overrides };
}

function executorEmitting(preamble: readonly Readonly<Record<string, unknown>>[]): ClaudeCodeExecutor {
  const spawnProcess = (_file: string, args: readonly string[], _options: ClaudeCodeSpawnOptions): ChildProcess => {
    const emitter = new EventEmitter();
    const stdin = args.includes("-p") ? new PassThrough() : null;
    const stdout = new PassThrough();
    const child = Object.assign(emitter, { stdin, stdout, stderr: null, kill: () => true }) as unknown as ChildProcess;
    queueMicrotask(() => emitter.emit("spawn"));
    if (args[0] === "--help") { queueMicrotask(() => { stdout.end(`${HELP}\n`); emitter.emit("close", 0, null); }); return child; }
    if (args[0] === "auth") { queueMicrotask(() => { stdout.end(); emitter.emit("close", 0, null); }); return child; }
    assert.ok(stdin !== null);
    // Emit init on the first stream-json stdin line while the turn's input remains
    // open.
    stdin.once("data", () => queueMicrotask(() => {
      for (const message of [...preamble, { type: "system", subtype: "init", session_id: SESSION_ID }]) stdout.write(`${JSON.stringify(message)}\n`);
    }));
    stdin.resume();
    return child;
  };
  return new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
  });
}

async function startWith(preamble: readonly Readonly<Record<string, unknown>>[]): Promise<string> {
  const executor = executorEmitting(preamble);
  try {
    const result = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "session-start-hooks",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: "Reply with who you are.",
    });
    return result.status === "accepted" ? "accepted" : result.error.code;
  } finally {
    executor.close();
  }
}

test("SessionStart hook lifecycle messages ahead of init are skipped and the launch is accepted", async () => {
  assert.equal(await startWith([hook("hook_started"), hook("hook_started"), hook("hook_response"), hook("hook_progress"), hook("hook_response")]), "accepted");
});

test("anything else ahead of init still fails closed", async () => {
  assert.equal(await startWith([hook("hook_started", { session_id: "44444444-4444-4444-8444-444444444444" })]), "executor.launch_ambiguous");
  assert.equal(await startWith([hook("hook_started", { hook_event: "PreToolUse" })]), "executor.launch_ambiguous");
  assert.equal(await startWith([{ type: "system", subtype: "status", session_id: SESSION_ID }]), "executor.launch_ambiguous");
});
