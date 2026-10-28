import assert from "node:assert/strict";
import test from "node:test";

import { classifyCodexTurnFailure, PackageLocalCodexAppServerTransport } from "../../src/executor/codex.js";
import { fakeAppServer } from "../support/codex-app-server.js";

const WORKSPACE = "/tmp/axis-codex-managed-workspace";
const TURN_POLICY = {
  approvalPolicy: "never",
  sandboxPolicy: { type: "dangerFullAccess" },
} as const;

function methodMessage(messages: readonly Record<string, unknown>[], method: string, ordinal = 0): Record<string, unknown> {
  const matching = messages.filter((message) => message.method === method);
  const found = matching[ordinal];
  assert.ok(found, `expected ${method} request ${ordinal + 1}`);
  return found;
}

test("Axis Codex turns run without approvals or an Axis sandbox", async () => {
  const server = fakeAppServer({
    initialize: { userAgent: "codex-cli/0.165.0", codexHome: "/tmp/axis-codex-home" },
    "config/read": { config: {} },
  });
  const transport = new PackageLocalCodexAppServerTransport({
    command: "/package/codex.js",
    spawnProcess: (file, args) => {
      server.spawnCalls.push({ file, args });
      return server.child;
    },
  });

  try {
    await transport.startTurn({ workingDirectory: WORKSPACE, instruction: "Read the issue." });

    const threadStart = methodMessage(server.messages, "thread/start");
    const threadStartParams = threadStart.params as Record<string, unknown>;
    assert.equal(typeof threadStartParams.developerInstructions, "string");
    const { developerInstructions: _developerInstructions, ...threadPolicy } = threadStartParams;
    assert.deepEqual(threadPolicy, {
      cwd: WORKSPACE,
      approvalPolicy: "never",
      sandbox: "danger-full-access",
    });
    const turnStart = methodMessage(server.messages, "turn/start");
    assert.deepEqual(turnStart.params, {
      threadId: "thread-native-1",
      cwd: WORKSPACE,
      ...TURN_POLICY,
      input: [{ type: "text", text: "Read the issue.", text_elements: [] }],
    });
  } finally {
    transport.close();
  }
});

test("successor processes restore the same no-approval full-access Codex policy before continuing", async () => {
  const server = fakeAppServer({
    initialize: { userAgent: "codex-cli/0.165.0", codexHome: "/tmp/axis-codex-home" },
    "config/read": { config: {} },
    "thread/resume": { thread: { id: "thread-native-1" } },
  });
  const transport = new PackageLocalCodexAppServerTransport({
    command: "/package/codex.js",
    resumeBeforeContinue: true,
    spawnProcess: () => server.child,
  });

  try {
    await transport.continueTurn({
      workingDirectory: WORKSPACE,
      instruction: "Read it again.",
      binding: { runtimeKind: "codex", threadId: "thread-native-1", turnId: "turn-native-1" },
    });

    const resume = methodMessage(server.messages, "thread/resume");
    assert.equal((resume.params as Record<string, unknown>).approvalPolicy, "never");
    assert.equal((resume.params as Record<string, unknown>).sandbox, "danger-full-access");
    assert.deepEqual(methodMessage(server.messages, "turn/start").params, {
      threadId: "thread-native-1",
      cwd: WORKSPACE,
      ...TURN_POLICY,
      input: [{ type: "text", text: "Read it again.", text_elements: [] }],
    });
  } finally {
    transport.close();
  }
});

test("a failed turn's TurnError classifies serverOverloaded as model capacity, with or without its optional members", () => {
  const failed = (error: Record<string, unknown>) => ({ status: "failed", error });
  assert.equal(classifyCodexTurnFailure(failed({ message: "busy", codexErrorInfo: "serverOverloaded", additionalDetails: null, misalignment: null })), "model_capacity");
  assert.equal(classifyCodexTurnFailure(failed({ message: "busy", codexErrorInfo: "serverOverloaded" })), "model_capacity");
  assert.equal(classifyCodexTurnFailure(failed({ message: "blocked", codexErrorInfo: "serverOverloaded", misalignment: { explanation: "x" } })), "model_capacity");
  assert.equal(classifyCodexTurnFailure(failed({ message: "other", codexErrorInfo: "other", additionalDetails: null, misalignment: null })), null);
  assert.equal(classifyCodexTurnFailure(failed({ message: "busy", codexErrorInfo: "serverOverloaded", unexpected: true })), null);
  assert.equal(classifyCodexTurnFailure(failed({ message: "busy", codexErrorInfo: "serverOverloaded", misalignment: "text" })), null);
  assert.equal(classifyCodexTurnFailure(failed({ codexErrorInfo: "serverOverloaded" })), null);
  assert.equal(classifyCodexTurnFailure({ status: "completed", error: { message: "busy", codexErrorInfo: "serverOverloaded" } }), null);
});
