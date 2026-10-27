/**
 * Persist continuable Claude sessions with their working directories beside the Worker
 * database. A restarted executor reads that store for reattestTerminal and continue.
 * The fake uses streaming input closed at the result; a missing resume transcript
 * returns one error result for the same session without init, then exits 1.
 */
import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { PassThrough } from "node:stream";
import test from "node:test";

import { ClaudeCodeExecutor, claudeCodeExecutorRegistration, type ClaudeCodeSpawnOptions } from "../../src/executor/claude-code.js";
import { SqliteWorkerCommandJournal, WorkerCommandError, WorkerCommandService, type RouterRunEventSink, type RouterRunLifecycleEnvelope, type WorkerCommandOptions } from "../../src/index.js";
import { channelBindingResolver } from "../../src/router/binding-resolver.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { Router, RouterError, routerTaskEnvelope, type LinearRouterTaskRef, type RouterNodeEventIngestResult } from "../../src/router/router.js";
import type { WorkerCommand, WorkerCommandResponse } from "../../src/shared/router-worker.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION, type ClaudeCodeLiveBinding, type ExecutorLifecycleEvent } from "../../src/shared/worker-executor.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint } from "../support/node-dispatch-double.js";
import { workerSelectionFixture } from "../support/node-routing-fixtures.js";

const SESSION_ID = "88888888-8888-4888-8888-888888888888";
const OTHER_SESSION_ID = "99999999-9999-4999-8999-999999999999";
const HELP = [
  "  -p Native option",
  "  --session-id Native option",
  "  --resume Native option",
  "  --output-format Native option",
  '  --input-format <format>  Input format (choices: "text", "stream-json")',
  "  --verbose Native option",
  "  --include-partial-messages Native option",
  "  --replay-user-messages Native option",
  "  --permission-mode <mode> Permission mode",
  '    (choices: "acceptEdits", "bypassPermissions", "dontAsk", "plan")',
].join("\n");

type Json = Readonly<Record<string, unknown>>;

/** `transcript: "missing"` makes every `--resume` answer as a session whose transcript is gone. */
function fakeClaude(options: { readonly transcript?: "present" | "missing" } = {}) {
  const turns: (readonly string[])[] = [];
  const spawnProcess = (_file: string, args: readonly string[], _options: ClaudeCodeSpawnOptions): ChildProcess => {
    const emitter = new EventEmitter();
    const stdin = args.includes("-p") ? new PassThrough() : null;
    const stdout = new PassThrough();
    const child = Object.assign(emitter, { stdin, stdout, stderr: null, kill: () => true }) as unknown as ChildProcess;
    queueMicrotask(() => emitter.emit("spawn"));
    if (args[0] === "--help") { queueMicrotask(() => { stdout.end(`${HELP}\n`); emitter.emit("close", 0, null); }); return child; }
    if (args[0] === "auth") { queueMicrotask(() => { stdout.end(); emitter.emit("close", 0, null); }); return child; }
    assert.ok(stdin !== null);
    turns.push(args);
    const sessionId = args[args.indexOf(args.includes("--resume") ? "--resume" : "--session-id") + 1]!;
    const write = (message: Json): void => { stdout.write(`${JSON.stringify(message)}\n`); };
    if (args.includes("--resume") && options.transcript === "missing") {
      queueMicrotask(() => {
        write({ type: "result", subtype: "error_during_execution", is_error: true, num_turns: 0, session_id: sessionId, permission_denials: [], errors: [`No conversation found with session ID: ${sessionId}`] });
        stdout.end();
        emitter.emit("exit", 1, null);
        emitter.emit("close", 1, null);
      });
      return child;
    }
    let pending = "";
    stdin.setEncoding("utf8");
    stdin.on("data", (chunk: string) => {
      pending += chunk;
      for (let newline = pending.indexOf("\n"); newline >= 0; newline = pending.indexOf("\n")) {
        const line = JSON.parse(pending.slice(0, newline)) as Json;
        pending = pending.slice(newline + 1);
        if (line.type === "user") queueMicrotask(() => {
          write({ type: "system", subtype: "init", session_id: sessionId });
          write({ type: "assistant", session_id: sessionId });
          write({ type: "result", subtype: "success", is_error: false, session_id: sessionId, permission_denials: [] });
        });
      }
    });
    stdin.on("end", () => queueMicrotask(() => { stdout.end(); emitter.emit("exit", 0, null); emitter.emit("close", 0, null); }));
    return child;
  };
  return { turns, spawnProcess };
}

function executorFor(fake: ReturnType<typeof fakeClaude>, nativeSessionStorePath: string | undefined): ClaudeCodeExecutor {
  return new ClaudeCodeExecutor({
    command: "/opt/claude/bin/claude",
    spawnProcess: fake.spawnProcess,
    randomUuid: () => SESSION_ID,
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
    ...(nativeSessionStorePath === undefined ? {} : { nativeSessionStorePath }),
  });
}

async function withStore(run: (storePath: string, directory: string) => Promise<void>): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), "axis-claude-restart-"));
  // Retries: a continued turn's own best-effort store write can still be in flight.
  try { await run(join(directory, "worker.sqlite.claude-native-sessions.json"), directory); } finally { await rm(directory, { recursive: true, force: true, maxRetries: 10 }); }
}

/** The store write is best-effort and asynchronous; wait for it the way a restarted Worker would find it. */
async function storeNames(storePath: string, sessionId: string): Promise<Json> {
  for (let i = 0; i < 200; i += 1) {
    try {
      const parsed = JSON.parse(await readFile(storePath, "utf8")) as Json;
      if (parsed[sessionId] !== undefined) return parsed;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail(`the store never recorded ${sessionId}`);
}

async function drain(executor: ClaudeCodeExecutor, binding: ClaudeCodeLiveBinding): Promise<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[]> {
  const events: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
  for await (const event of executor.observe(binding)) events.push(event);
  return events;
}

/** Executor A runs one start to a consumed `succeeded`, is dropped, and its store is left behind. */
async function priorTurn(storePath: string, workingDirectory: string): Promise<ClaudeCodeLiveBinding> {
  const executor = executorFor(fakeClaude(), storePath);
  const started = await executor.start({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "start-1", workingDirectory, instruction: "Remember a word." });
  assert.equal(started.status, "accepted");
  assert.deepEqual((await drain(executor, started.binding)).map((event) => event.outcome), ["started", "progressed", "succeeded"]);
  await storeNames(storePath, started.binding.sessionId);
  executor.close();
  return started.binding;
}

function reattest(executor: ClaudeCodeExecutor, binding: ClaudeCodeLiveBinding) {
  return executor.reattestTerminal({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "reattest-1", binding, expectedOutcome: "succeeded" });
}

function continueTurn(executor: ClaudeCodeExecutor, binding: ClaudeCodeLiveBinding, workingDirectory: string) {
  return executor.continue({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "continue-1", workingDirectory, instruction: "Which word?", binding });
}

test("a restarted executor attests and resumes a session the store recorded, in the same working directory", async () => {
  await withStore(async (storePath, directory) => {
    const binding = await priorTurn(storePath, directory);
    assert.deepEqual(JSON.parse(await readFile(storePath, "utf8")), { [SESSION_ID]: { workingDirectory: directory } });
    assert.equal((await stat(storePath)).mode & 0o777, 0o600);

    const fake = fakeClaude();
    const restarted = executorFor(fake, storePath);
    assert.deepEqual(await reattest(restarted, binding), { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "reattest-1", status: "attested", binding });
    assert.equal(fake.turns.length, 0, "reattestTerminal never spawns");
    const continued = await continueTurn(restarted, binding, directory);
    assert.equal(continued.status, "accepted");
    assert.deepEqual(continued.binding, binding);
    assert.equal(fake.turns.length, 1);
    assert.deepEqual(fake.turns[0]!.slice(0, 3), ["-p", "--resume", SESSION_ID]);
    assert.deepEqual((await drain(restarted, binding)).map((event) => event.outcome), ["started", "progressed", "succeeded"]);
    restarted.close();
  });
});

test("a restarted executor refuses to resume a recorded session from another working directory", async () => {
  await withStore(async (storePath, directory) => {
    const binding = await priorTurn(storePath, directory);
    const fake = fakeClaude();
    const restarted = executorFor(fake, storePath);
    const continued = await continueTurn(restarted, binding, join(directory, "elsewhere"));
    assert.equal(continued.status, "failed");
    if (continued.status !== "failed") throw new Error("unreachable");
    assert.equal(continued.error.code, "executor.capability_unavailable");
    assert.equal(fake.turns.length, 0);
    restarted.close();
  });
});

test("no record, or a corrupt store, is not attested and not resumed", async () => {
  await withStore(async (storePath, directory) => {
    const binding: ClaudeCodeLiveBinding = { runtimeKind: "claude-code", sessionId: SESSION_ID };
    for (const content of [JSON.stringify({ [OTHER_SESSION_ID]: { workingDirectory: directory } }), "{not json", null]) {
      if (content === null) await rm(storePath, { force: true }); else await writeFile(storePath, content);
      const fake = fakeClaude();
      const restarted = executorFor(fake, storePath);
      assert.equal(await reattest(restarted, binding), null);
      const continued = await continueTurn(restarted, binding, directory);
      assert.equal(continued.status, "failed");
      if (continued.status !== "failed") throw new Error("unreachable");
      assert.equal(continued.error.code, "executor.capability_unavailable");
      assert.equal(fake.turns.length, 0);
      await assert.rejects(restarted.reattestTerminal({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: "reattest-1", binding: { runtimeKind: "claude-code", sessionId: "not-a-uuid" }, expectedOutcome: "succeeded" }), "an invalid request rejects rather than answering null");
      restarted.close();
    }
  });
});

test("a resume whose transcript is gone is a definite launch_failed, not launch_ambiguous", async () => {
  await withStore(async (storePath, directory) => {
    const binding = await priorTurn(storePath, directory);
    const fake = fakeClaude({ transcript: "missing" });
    const restarted = executorFor(fake, storePath);
    assert.equal((await reattest(restarted, binding))?.status, "attested");
    const continued = await continueTurn(restarted, binding, directory);
    assert.equal(fake.turns.length, 1);
    assert.equal(continued.status, "failed");
    if (continued.status !== "failed") throw new Error("unreachable");
    assert.equal(continued.error.code, "executor.launch_failed");
    restarted.close();
  });
});


// Exercise the real WorkerCommandService, SqliteWorkerCommandJournal and Router
// in-process through NodeDispatchDouble. Only the Claude CLI is faked.
const organizationId = "organization-claude-restart-1";
const teamId = "team-claude-restart-1";
const endpointRef = { ref: "endpoint-claude-restart-1", version: "v1" } as const;
const nodeRef = { ref: "node-logical-claude-restart-1", version: "v1" } as const;
const identity = { nodeId: "node-claude-restart-1", workerId: "worker-claude-restart-1", executorKind: "claude-code", runtimeProfile: { ref: "executor-claude-restart-1", version: "v1" } } as const;
const linearRef: LinearRouterTaskRef = { organizationId, issueId: "issue-claude-restart-1", teamId, agentSessionId: "session-claude-restart-1" };

class RouterNodeIngestSink implements RouterRunEventSink {
  readonly results: RouterNodeEventIngestResult[] = [];
  #clockMs = Date.parse("2026-09-24T00:00:01.000Z");
  constructor(private readonly router: () => Router, private readonly port: () => NodeDispatchDouble) {}
  async emit(event: RouterRunLifecycleEnvelope): Promise<void> {
    const recordedAt = new Date(this.#clockMs).toISOString();
    this.#clockMs += 1_000;
    this.results.push(await this.router().ingestNodeRunEvent({ envelope: event, recordedAt, origin: this.port().eventOrigin }));
  }
}

/** Router's Task lifecycle, unwrapping response deliveries and excluding `task.progressed`. */
async function settled(router: Router, count: number): Promise<readonly string[]> {
  const deadline = Date.now() + 10_000;
  const kinds = () => router.listTaskEvents(linearRef).map(({ envelope }) => routerTaskEnvelope(envelope).envelopeType).filter((kind) => kind !== "task.progressed");
  while (kinds().length < count && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(kinds().length >= count, `expected at least ${count} Router events, saw ${JSON.stringify(kinds())}`);
  return kinds();
}

test("a restarted Claude Worker accepts a continuation of a Run it no longer holds, and resumes the same session", async (context) => {
  await withStore(async (storePath, directory) => {
    const journal = new SqliteWorkerCommandJournal(":memory:");
    const teamBindings = new SqliteTeamBindingStore({});
    teamBindings.configure({ scope: { organizationId, linearTeamId: teamId }, expectedRevision: 0, profile: { ...identity, endpointRef }, audit: { operatorRef: "operator", source: "test", requestRef: "request", authorizationRef: "authorization" } });
    let router!: Router, port!: NodeDispatchDouble, service!: WorkerCommandService, executor!: ClaudeCodeExecutor;
    let fake = fakeClaude();
    const sink = new RouterNodeIngestSink(() => router, () => port);
    let id = 0;
    const configuration = (adapter: ClaudeCodeExecutor): WorkerCommandOptions => ({
      configuration: {
        bearerToken: "worker-claude-restart-token", workerId: identity.workerId, runtimeKind: "claude-code", executorId: identity.runtimeProfile.ref, nodeId: identity.nodeId,
        methodRef: { version: "v1", method: "claude-code.execute" }, workspaces: [{ organizationId, workspaceRoot: directory }],
      },
      registrations: [claudeCodeExecutorRegistration({ executorId: identity.runtimeProfile.ref, adapter })],
      eventSink: sink,
      journal,
      now: () => new Date("2026-09-24T00:00:01.000Z"),
      messageId: () => `worker-claude-restart-${++id}`,
    });
    executor = executorFor(fake, storePath);
    service = new WorkerCommandService(configuration(executor));
    port = new NodeDispatchDouble({
      worker: { async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> { return (await service.execute(command)).response; } },
      nodeRef,
      reservation: nodeReservationFromEndpoint({ endpointRef, identity, sessionBindingExecutorId: identity.runtimeProfile.ref }, "worker-key-claude-restart-1"),
    });
    router = new Router({
      nodeRouting: { bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef, nodeRef }] }), port },
      now: () => new Date("2026-09-24T00:00:02.000Z"),
    });
    context.after(() => { service.close(); executor.close(); journal.close(); router.close(); teamBindings.close(); });

    const started = await router.startNode({ ...linearRef, commandId: "command-claude-restart-start", instruction: "Remember a word.", workerSelection: workerSelectionFixture() });
    assert.equal(started.status, "accepted");
    assert.deepEqual(await settled(router, 2), ["task.accepted", "task.succeeded"]);
    await storeNames(storePath, SESSION_ID);

    // The restart: a new executor and a new Worker service on the same journal
    // and store, so `#observedBindings` and `#sessions` are both empty.
    service.close();
    executor.close();
    fake = fakeClaude();
    executor = executorFor(fake, storePath);
    service = new WorkerCommandService(configuration(executor));

    try {
      const continued = await router.continueNode({ ...linearRef, commandId: "command-claude-restart-continue", instruction: "Which word?" });
      assert.equal(continued.status, "accepted");
    } catch (error) {
      assert.fail(`the restarted Worker refused the continuation: ${error instanceof RouterError && error.cause instanceof WorkerCommandError ? error.cause.code : String(error)}`);
    }
    assert.deepEqual(await settled(router, 4), ["task.accepted", "task.succeeded", "task.accepted", "task.succeeded"]);
    assert.equal(fake.turns.length, 1);
    assert.deepEqual(fake.turns[0]!.slice(0, 3), ["-p", "--resume", SESSION_ID]);
    assert.deepEqual(sink.results.filter((result) => result.status === "quarantined"), []);
  });
});
