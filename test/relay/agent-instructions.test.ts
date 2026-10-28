import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import test from "node:test";

import { ClaudeCodeExecutor, type ClaudeCodeSpawnOptions } from "../../src/executor/claude-code.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION } from "../../src/shared/worker-executor.js";

import { PackageLocalCodexAppServerTransport } from "../../src/executor/codex.js";
import {
  DEFAULT_AGENT_INSTRUCTIONS,
  DEFAULT_SESSION_RESPONSE_GUIDANCE,
  axisAgentGuidanceSections,
  axisTaskInput,
  resolveAgentInstructions,
} from "../../src/executor/response-guidance.js";
import { parseLinearWebhook } from "../../src/relay/linear/payload.js";
import { composeLinearDispatchInstruction } from "../../src/relay/linear/instruction.js";
import { currentFixture } from "../helpers.js";
import { fakeAppServer } from "../support/codex-app-server.js";

const AVAILABLE = { LINEAR_EXECUTION_STATUS: "available", LINEAR_EXECUTION_EXPIRES_AT: "2026-09-17T00:00:00.000Z" } as const;
const UNAVAILABLE = { LINEAR_EXECUTION_STATUS: "disabled" } as const;

/** How many times `needle` occurs in `haystack`. `includes` cannot see a second copy. */
function occurrences(haystack: string, needle: string): number {
  return haystack.split(needle).length - 1;
}

test("an agent that configures nothing sends the Axis defaults", () => {
  assert.deepEqual(resolveAgentInstructions(), DEFAULT_AGENT_INSTRUCTIONS);
  assert.deepEqual(resolveAgentInstructions({}), DEFAULT_AGENT_INSTRUCTIONS);
  assert.deepEqual(axisAgentGuidanceSections(undefined, DEFAULT_AGENT_INSTRUCTIONS), [DEFAULT_SESSION_RESPONSE_GUIDANCE]);
});

test("a configured session response replaces its default", () => {
  const resolved = resolveAgentInstructions({ sessionResponse: "Return the outcome in this session." });
  assert.deepEqual(axisAgentGuidanceSections(undefined, resolved), ["Return the outcome in this session."]);
});

test("a session response configured to null is not sent", () => {
  const resolved = resolveAgentInstructions({ sessionResponse: null });
  assert.deepEqual(axisAgentGuidanceSections(undefined, resolved), []);
  assert.equal(axisTaskInput("do the thing", "start", undefined, resolved), "Task:\ndo the thing");
});

test("an agent configuration cannot reach the Linear execution paragraph", () => {
  // The paragraph is derived from the environment the child receives. If a
  // configured section could carry it, a deployment could describe credentials
  // it does not hold, which is the failure the unavailable branch exists for.
  const resolved = resolveAgentInstructions({ sessionResponse: null });
  const sections = axisAgentGuidanceSections(AVAILABLE, resolved);
  // Anchored to the facts the paragraph exists to state, not to a second call
  // of the function under test: comparing production output with production
  // output would hold however that text changed, including if it emptied.
  assert.equal(sections.length, 1);
  const linear = sections[0] ?? "";
  for (const fact of [
    "Axis Linear execution clients are available for this Run through Relay.",
    "LINEAR_EXECUTION_SDK_MODULE",
    "LINEAR_EXECUTION_EXPIRES_AT",
    "workspace_mismatch",
    "Never print or persist execution credentials",
  ]) {
    assert.ok(linear.includes(fact), `the Linear paragraph must still state ${fact}`);
  }
});

test("an executor that injects no Linear environment claims no Linear access", () => {
  // A Run with no injected environment must not claim Axis client access.
  const preamble = axisTaskInput("do the thing", "start", undefined, DEFAULT_AGENT_INSTRUCTIONS);
  assert.ok(!preamble.includes("Axis Linear execution clients"));
  assert.ok(!preamble.includes("updating issue status"));
  assert.ok(!preamble.includes("Linear CLI"));
});

test("an available Run is never also told what to do when its clients are unavailable", () => {
  const available = axisAgentGuidanceSections(AVAILABLE, DEFAULT_AGENT_INSTRUCTIONS).join("\n\n");
  assert.ok(!available.includes("are unavailable because"));
  assert.ok(!available.includes("Do not describe the clients as missing"));

  const unavailable = axisAgentGuidanceSections(UNAVAILABLE, DEFAULT_AGENT_INSTRUCTIONS).join("\n\n");
  assert.ok(unavailable.includes("this Worker is not configured for Axis Linear execution access"));
  assert.ok(!unavailable.includes("LINEAR_EXECUTION_SDK_MODULE"));
});

test("each rule reaches the agent exactly once", () => {
  // Keep each shared guidance rule stated once across the shared, Linear and Codex
  // transport sections so independent copies cannot drift.
  const preamble = axisTaskInput("do the thing", "start", AVAILABLE, DEFAULT_AGENT_INSTRUCTIONS);
  for (const rule of [
    "Agent Activity",
    "never claim an operation succeeded",
    "resolve the exact parent",
    "within the task's scope, including posting comments",
    "Report it exactly",
  ]) {
    assert.equal(occurrences(preamble, rule), 1, `"${rule}" should appear exactly once`);
  }
});

test("the Linear paragraph gives a default reply-destination rule and forbids write-to-confirm", () => {
  // Completed Linear work defaults to one top-level conclusion Comment. Verification
  // rereads that write rather than posting another copy, and the session response
  // reports only what was verified.
  const sections = axisAgentGuidanceSections(AVAILABLE, DEFAULT_AGENT_INSTRUCTIONS);
  const linear = sections.at(-1) ?? "";
  for (const rule of [
    "When this turn reaches a definite conclusion, post it as a top-level Comment",
    "do not post a top-level Comment -- reply in the controlling AgentSession thread only",
    "is never a substitute for a top-level Comment or the reverse",
    "never by posting a second one to check",
  ]) {
    assert.ok(linear.includes(rule), `the Linear paragraph must state: ${rule}`);
  }
});

test("the default Comment is itself the request, and a failure stays in the thread", () => {
  // Run instructions authorize the default conclusion Comment for completed work.
  // Failures and blockers stay in the session response rather than becoming ticket
  // conclusions.
  const sections = axisAgentGuidanceSections(AVAILABLE, DEFAULT_AGENT_INSTRUCTIONS);
  const linear = sections.at(-1) ?? "";
  for (const rule of [
    "This default is the request for that one Comment",
    "do not skip it because a skill or tool permits writes only on explicit user request",
    "a failure, a blocker, or otherwise not yet definite, do not post a top-level Comment",
  ]) {
    assert.ok(linear.includes(rule), `the Linear paragraph must state: ${rule}`);
  }
});

test("a continuation sends the instruction alone", () => {
  assert.equal(axisTaskInput("second turn", "continue", AVAILABLE, DEFAULT_AGENT_INSTRUCTIONS), "second turn");
  assert.ok(axisTaskInput("first turn", "start", AVAILABLE, DEFAULT_AGENT_INSTRUCTIONS).includes(DEFAULT_SESSION_RESPONSE_GUIDANCE));
});

/**
 * The Codex adapter is the assembly that carried the duplication: its transport
 * block restated the shared rules in its own words. These drive the real
 * transport and read the `developerInstructions` it actually sends, rather than
 * recomposing the sections here and asserting against that.
 */
async function codexDeveloperInstructions(options: {
  readonly executionEnvironment?: Record<string, string>;
  readonly instructions?: { readonly sessionResponse?: string | null };
  readonly inherited?: string;
  readonly task?: string;
  readonly onTurnInput?: (input: unknown) => void;
}): Promise<string> {
  const server = fakeAppServer({
    initialize: { userAgent: "codex-cli/0.165.0", codexHome: "/tmp/axis-codex-home" },
    "config/read": { config: options.inherited === undefined ? {} : { developer_instructions: options.inherited } },
  });
  const transport = new PackageLocalCodexAppServerTransport({
    command: "/package/codex.js",
    spawnProcess: () => server.child,
    ...(options.executionEnvironment === undefined ? {} : { executionEnvironment: options.executionEnvironment }),
    ...(options.instructions === undefined ? {} : { instructions: options.instructions }),
  });
  try {
    await transport.startTurn({ workingDirectory: "/tmp/axis-codex-managed-workspace", instruction: options.task ?? "Read the issue." });
    options.onTurnInput?.(server.messages.find((message) => message.method === "turn/start")?.params);
    const threadStart = server.messages.find((message) => message.method === "thread/start");
    assert.ok(threadStart, "expected a thread/start request");
    const sent = (threadStart.params as Record<string, unknown>).developerInstructions;
    assert.equal(typeof sent, "string");
    return sent as string;
  } finally {
    transport.close();
  }
}

test("a Codex thread states each rule exactly once", async () => {
  const sent = await codexDeveloperInstructions({ executionEnvironment: { ...AVAILABLE } });
  // Present at all: the shared sections really are sent by this adapter.
  assert.ok(sent.includes(DEFAULT_SESSION_RESPONSE_GUIDANCE));
  assert.ok(sent.includes("Axis session transport context:"));
  assert.ok(sent.includes("Axis profile-routing policy:"));
  for (const rule of [
    "Agent Activity",
    "never claim an operation succeeded",
    "resolve the exact parent",
    "Codex response channel",
    "Report it exactly",
  ]) {
    assert.equal(occurrences(sent, rule), 1, `"${rule}" should appear exactly once`);
  }
  // And an available Run is not also handed the unavailable branch.
  assert.ok(!sent.includes("are unavailable because"));
});

test("a Codex thread carries this agent's configuration, after the operator's own text", async () => {
  const sent = await codexDeveloperInstructions({
    executionEnvironment: { ...AVAILABLE },
    instructions: { sessionResponse: "Return the outcome in this session." },
    inherited: "Operator instructions.",
  });
  assert.ok(sent.startsWith("Operator instructions.\n\n"));
  assert.ok(sent.includes("Return the outcome in this session."));
  assert.ok(!sent.includes(DEFAULT_SESSION_RESPONSE_GUIDANCE));
});

const CLAUDE_SESSION_ID = "11111111-1111-4111-8111-111111111111";
const CLAUDE_HELP = [
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

/**
 * What the Claude child actually reads on stdin. Plumbing an option through
 * four Worker runtimes is only real if it survives to the spawned process, so
 * this reads the bytes rather than the executor's inputs.
 */
async function claudeStartTurnInput(instructions?: { readonly sessionResponse?: string | null }, task = "Run the task."): Promise<string> {
  let written = "";
  const spawnProcess = (_file: string, args: readonly string[], _options: ClaudeCodeSpawnOptions): ChildProcess => {
    const emitter = new EventEmitter();
    const task = args.includes("-p");
    const stdin = task ? new PassThrough() : null;
    const stdout = new PassThrough();
    const child = Object.assign(emitter, { stdin, stdout, stderr: null, kill: () => true }) as unknown as ChildProcess;
    queueMicrotask(() => emitter.emit("spawn"));
    if (args[0] === "--help") queueMicrotask(() => { stdout.end(`${CLAUDE_HELP}\n`); emitter.emit("close", 0, null); });
    else if (args[0] === "auth") queueMicrotask(() => { stdout.end(); emitter.emit("close", 0, null); });
    else {
      assert.ok(stdin !== null);
      // React to the first stream-json stdin line while the turn's stdin remains open,
      // matching the CLI input protocol.
      stdin.once("data", (chunk: Buffer) => {
        written += chunk.toString("utf8");
        queueMicrotask(() => {
          stdout.write(`${JSON.stringify({ type: "system", subtype: "init", session_id: CLAUDE_SESSION_ID })}\n`);
          stdout.write(`${JSON.stringify({ type: "result", subtype: "success", is_error: false, session_id: CLAUDE_SESSION_ID, result: "done" })}\n`);
          stdout.end();
          emitter.emit("close", 0, null);
        });
      });
    }
    return child;
  };
  const executor = new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess,
    randomUuid: () => CLAUDE_SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
    ...(instructions === undefined ? {} : { instructions }),
  });
  try {
    const accepted = await executor.start({
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: "agent-instructions",
      workingDirectory: "/tmp/axis-claude-workspace",
      instruction: task,
    });
    assert.equal(accepted.status, "accepted");
    return written;
  } finally {
    executor.close();
  }
}

/**
 * Read instruction assertions from the stream-json user message's message.content
 * field.
 */
function streamedUserMessage(written: string): { readonly type: unknown; readonly role: unknown; readonly content: string } {
  const newline = written.indexOf("\n");
  const line = newline < 0 ? written : written.slice(0, newline);
  const parsed = JSON.parse(line) as { readonly type: unknown; readonly message: { readonly role: unknown; readonly content: unknown } };
  assert.equal(typeof parsed.message.content, "string");
  return { type: parsed.type, role: parsed.message.role, content: parsed.message.content as string };
}

test("a Claude agent's configuration reaches the spawned child", async () => {
  const withDefaults = streamedUserMessage(await claudeStartTurnInput());
  assert.equal(withDefaults.type, "user");
  assert.equal(withDefaults.role, "user");
  assert.ok(withDefaults.content.includes(DEFAULT_SESSION_RESPONSE_GUIDANCE));
  assert.ok(withDefaults.content.endsWith("Task:\nRun the task."));

  const configured = streamedUserMessage(await claudeStartTurnInput({ sessionResponse: "Return the outcome in this session." }));
  assert.equal(configured.type, "user");
  assert.equal(configured.role, "user");
  assert.ok(configured.content.includes("Return the outcome in this session."));
  assert.ok(!configured.content.includes(DEFAULT_SESSION_RESPONSE_GUIDANCE));
});

for (const withGuidance of [true, false]) {
  test(`created webhook ${withGuidance ? "guidance" : "without guidance"} reaches both native executors`, async () => {
    const payload = currentFixture("agent-session-created.json", withGuidance ? {} : { guidance: undefined });
    const event = parseLinearWebhook(Buffer.from(JSON.stringify(payload)));
    assert.equal(event.type, "AgentSessionEvent");
    if (event.type !== "AgentSessionEvent") assert.fail("expected an AgentSession event");
    const instruction = composeLinearDispatchInstruction("Read the issue.", {
      appUserId: event.appUserId,
      issue: { id: event.agentSession.issueId, identifier: "AX", title: "Instruction test" },
      sourceCommentId: null,
      rootCommentId: null,
      guidance: event.guidance,
    });
    let codexInput: unknown;
    await codexDeveloperInstructions({ task: instruction, onTurnInput: (input) => { codexInput = input; } });
    const codex = JSON.stringify(codexInput);
    const claude = streamedUserMessage(await claudeStartTurnInput(undefined, instruction)).content;
    for (const nativeInput of [codex, claude]) {
      assert.equal(occurrences(nativeInput, "<linear-agent-guidance>"), withGuidance ? 1 : 0);
      assert.equal(occurrences(nativeInput, "Use the captured Team execution profile."), withGuidance ? 1 : 0);
      assert.ok(nativeInput.includes("<user-command>"));
    }
  });
}
