import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { packageLocalCodexExecutable } from "../../src/index.js";
import * as rootSurface from "../../src/index.js";
import { ClaudeCodeExecutor, claudeCodeExecutorRegistration } from "../../src/executor/claude-code.js";
import { CodexExecutor, codexExecutorRegistration } from "../../src/executor/codex.js";
import { selectExecutorRegistration } from "../../src/shared/worker-executor.js";
import { canonicalClaudeExecutable } from "../../src/worker/claude-runtime.js";

test("Worker resolves the Codex executable only from its installed package", () => {
  assert.equal(packageLocalCodexExecutable((() => "/packages/codex/package.json") as unknown as NodeRequire["resolve"]), "/packages/codex/bin/codex.js");
});

test("Claude Worker requires one absolute canonical executable and retains the neutral Worker configuration", async () => {
  let canonicalizations = 0;
  const executable = await canonicalClaudeExecutable("/opt/claude/bin/claude-link", async (path) => { canonicalizations += 1; assert.equal(path, "/opt/claude/bin/claude-link"); return "/opt/claude/bin/claude"; });
  assert.equal(canonicalizations, 1);
  assert.equal(executable, "/opt/claude/bin/claude");
  await assert.rejects(() => canonicalClaudeExecutable("claude"), /Claude executable is invalid/u);
});

test("Codex and Claude remain separate closed registrations with no provider selection at the package root", () => {
  const codex = new CodexExecutor({ command: "/package/codex", transport: {
    async startTurn() { throw new Error("unused"); }, async continueTurn() { throw new Error("unused"); }, async interruptTurn() {}, async *observeTurn() {},
  } });
  const claude = new ClaudeCodeExecutor({ command: "/opt/claude/bin/claude", spawnProcess: () => { throw new Error("unused"); } });
  const registrations = [codexExecutorRegistration({ executorId: "codex-1", adapter: codex }), claudeCodeExecutorRegistration({ executorId: "claude-1", adapter: claude })];
  assert.equal(selectExecutorRegistration({ runtimeKind: "codex", executorId: "codex-1", methodRef: { version: "v1", method: "codex.execute" } }, registrations).adapter, codex);
  assert.equal(selectExecutorRegistration({ runtimeKind: "claude-code", executorId: "claude-1", methodRef: { version: "v1", method: "claude-code.execute" } }, registrations).adapter, claude);
  assert.deepEqual(registrations.map((registration) => registration.supportedOperations), [["start", "continue", "cancel", "observe"], ["start", "continue", "cancel", "steer", "observe"]]);
  for (const name of ["ClaudeCodeExecutor", "claudeCodeExecutorRegistration", "startClaudeWorkerFromEnvironment", "claudeWorkerRuntimeConfig", "CLAUDE_CODE_VERSION"]) assert.equal(name in rootSurface, false);
  for (const name of ["startCodexWorker", "startClaudeWorker", "startNode", "startRelay", "composeLinearChannel"]) assert.equal(typeof rootSurface[name as keyof typeof rootSurface], "function");
  codex.close(); claude.close();
});

test("production composition uses Node routing and excludes configured secrets", async () => {
  const [relay, worker, claudeWorker, claudeAdapter] = await Promise.all([readFile(new URL("../../src/bin/relay.ts", import.meta.url), "utf8"), readFile(new URL("../../src/worker/runtime.ts", import.meta.url), "utf8"), readFile(new URL("../../src/worker/claude-runtime.ts", import.meta.url), "utf8"), readFile(new URL("../../src/executor/claude-code.ts", import.meta.url), "utf8")]);
  assert.equal(`${relay}\n${worker}\n${claudeWorker}`.includes("router-secret"), false);
  assert.equal(`${relay}\n${worker}\n${claudeWorker}`.includes("threadId"), false);
  assert.equal(/--model|--fallback|--continue|--plugin/u.test(claudeAdapter), false);
  const relayRuntime = await readFile(new URL("../../src/relay/runtime.ts", import.meta.url), "utf8");
  // Require node-routing-composition.ts to construct the command transport, and relay.ts to
  // build its Router through that composition. A class name appearing only in a comment
  // would not establish this wiring.
  const composition = await readFile(new URL("../../src/relay/node-routing-composition.ts", import.meta.url), "utf8");
  assert.match(composition, /new HttpRouterNodePort\(/u);
  assert.match(relayRuntime, /composeNodeRouter\(/u);
  assert.match(relay, /startRelay\(/u);
  assert.equal(/new Router\(/u.test(relay), false, "relay.ts must not construct a Router outside the Node composition");
});
