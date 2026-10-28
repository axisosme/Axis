/**
 * Continue a Codex conversation across a Worker restart after either waiting_for_input
 * or success. Before reserving a Run, the restarted Worker proves the durable thread
 * through the executor's non-archived thread/list transport; the successor resumes that
 * same thread.
 *
 * Use the real Worker service, SQLite journal, Router and Codex registration through
 * NodeDispatchDouble. Credentials are issued in-process, and fake app-server processes
 * share one thread store.
 */
import assert from "node:assert/strict";
import { type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import test from "node:test";

import { codexExecutorRegistration } from "../../src/executor/codex.js";
import { SqliteWorkerCommandJournal, WorkerCommandError, WorkerCommandService, type RouterRunEventSink, type RouterRunLifecycleEnvelope, type WorkerCommandOptions } from "../../src/index.js";
import { channelBindingResolver } from "../../src/router/binding-resolver.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { Router, RouterError, routerTaskEnvelope, type LinearRouterTaskRef, type RouterNodeEventIngestResult } from "../../src/router/router.js";
import type { WorkerCommand, WorkerCommandResponse } from "../../src/shared/router-worker.js";
import { LinearCodexExecutor } from "../../src/worker/linear-codex-executor.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint } from "../support/node-dispatch-double.js";
import { workerSelectionFixture } from "../support/node-routing-fixtures.js";

type Json = Readonly<Record<string, unknown>>;

const THREAD_ID = "thread-codex-restart-1";
const SIBLING_THREAD_ID = "thread-codex-restart-sibling";
/** The query `threadResumable` sends; every field is a codex 0.165.0 `thread/list` parameter. */
const LIST_QUERY = { archived: false, modelProviders: [], sourceKinds: ["cli", "vscode", "exec", "appServer", "unknown"], limit: 100 };

/**
 * `list`: whether the thread store still lists the thread (on the second page, after a sibling),
 * or answers `thread/list` with a JSON-RPC error. `turn`: how the next turn ends.
 */
function fakeCodexHome() {
  const state: { list: "listed" | "absent" | "error"; turn: "succeed" | "wait" } = { list: "listed", turn: "succeed" };
  const processes: Json[][] = [];
  let turns = 0;
  const spawnProcess = (): ChildProcess => {
    const stdin = new PassThrough();
    const stdout = new PassThrough();
    const child = Object.assign(new EventEmitter(), { stdin, stdout, stderr: null, kill: () => true }) as unknown as ChildProcess;
    const messages: Json[] = [];
    processes.push(messages);
    const write = (message: Json): void => { stdout.write(`${JSON.stringify(message)}\n`); };
    let buffer = "";
    stdin.setEncoding("utf8");
    stdin.on("data", (chunk: string) => {
      buffer += chunk;
      for (let newline = buffer.indexOf("\n"); newline >= 0; newline = buffer.indexOf("\n")) {
        const message = JSON.parse(buffer.slice(0, newline)) as Json;
        buffer = buffer.slice(newline + 1);
        messages.push(message);
        if (typeof message.id !== "number" || typeof message.method !== "string") continue;
        const params = (message.params ?? {}) as Json;
        const reply = (result: unknown): void => queueMicrotask(() => write({ id: message.id, result }));
        if (message.method === "thread/start") reply({ thread: { id: THREAD_ID } });
        else if (message.method === "thread/resume") reply({ thread: { id: params.threadId } });
        else if (message.method === "thread/list") {
          if (state.list === "error") queueMicrotask(() => write({ id: message.id, error: { code: -32603, message: "thread store unavailable" } }));
          else if (params.cursor === undefined) reply({ data: [{ id: SIBLING_THREAD_ID }], nextCursor: "cursor-2" });
          else reply({ data: state.list === "listed" ? [{ id: THREAD_ID }] : [], nextCursor: null });
        } else if (message.method === "turn/start") {
          const threadId = params.threadId as string, turnId = `turn-codex-restart-${++turns}`, mode = state.turn;
          reply({ turn: { id: turnId } });
          setImmediate(() => {
            write({ method: "turn/started", params: { threadId, turn: { id: turnId, status: "inProgress" } } });
            if (mode === "succeed") write({ method: "turn/completed", params: { threadId, turn: { id: turnId, status: "completed" } } });
            else write({ id: `elicitation-${turnId}`, method: "item/tool/requestUserInput", params: { threadId, turnId, itemId: "item-codex-restart-1", isBlocking: true, autoResolutionMs: null, questions: [{ id: "mode", header: "Choose mode", question: "Which mode?", isOther: false, isSecret: false, options: [{ label: "Fast", description: "Use fast mode." }, { label: "Thorough", description: "Use thorough mode." }] }] } });
          });
        } else if (message.method === "turn/interrupt") {
          reply({});
          setImmediate(() => write({ method: "turn/completed", params: { threadId: params.threadId, turn: { id: params.turnId, status: "interrupted" } } }));
        } else reply({});
      }
    });
    return child;
  };
  /** Every request a process spawned at or after `from` sent, other than the handshake. */
  const nativeRequests = (from: number): Json[] => processes.slice(from).flat()
    .filter((message) => typeof message.id === "number" && typeof message.method === "string" && !["initialize", "config/read"].includes(message.method));
  return { state, processes, spawnProcess, nativeRequests };
}

function executorFor(home: ReturnType<typeof fakeCodexHome>): LinearCodexExecutor {
  return new LinearCodexExecutor({
    command: "/package/codex.js",
    environment: { HOME: "/home/operator", PATH: "/operator/bin" },
    linear: {
      relay: "http://127.0.0.1:1",
      client: {
        issue: async (execution) => ({
          contractVersion: "1.0.0", requestId: `credential-${execution.runId}`, status: "issued", execution,
          credentialId: `credential-${execution.runId}`, credential: `axlea1_${"a".repeat(43)}`,
          expiresAt: new Date(Date.now() + 60_000).toISOString(),
        }),
        revoke: async () => undefined,
      },
    },
    spawnProcess: home.spawnProcess,
  });
}

const organizationId = "organization-codex-restart-1";
const teamId = "team-codex-restart-1";
const endpointRef = { ref: "endpoint-codex-restart-1", version: "v1" } as const;
const nodeRef = { ref: "node-logical-codex-restart-1", version: "v1" } as const;
const identity = { nodeId: "node-codex-restart-1", workerId: "worker-codex-restart-1", executorKind: "codex", runtimeProfile: { ref: "executor-codex-restart-1", version: "v1" } } as const;

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
async function settled(router: Router, ref: LinearRouterTaskRef, count: number): Promise<readonly string[]> {
  const deadline = Date.now() + 10_000;
  const kinds = () => router.listTaskEvents(ref).map(({ envelope }) => routerTaskEnvelope(envelope).envelopeType).filter((kind) => kind !== "task.progressed");
  while (kinds().length < count && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(kinds().length >= count, `expected at least ${count} Router events, saw ${JSON.stringify(kinds())}`);
  return kinds();
}

/**
 * Runs one start whose turn ends as `prior`, then restarts the Worker: a new executor and a new
 * `WorkerCommandService` on the same journal and thread store, so `#observedBindings` and every
 * app-server process are gone. Returns the index of the first process spawned after the restart.
 */
async function priorRunThenRestart(context: test.TestContext, prior: "succeed" | "wait") {
  const directory = await mkdtemp(join(tmpdir(), "axis-codex-restart-"));
  const home = fakeCodexHome();
  const journal = new SqliteWorkerCommandJournal(":memory:");
  const teamBindings = new SqliteTeamBindingStore({});
  teamBindings.configure({ scope: { organizationId, linearTeamId: teamId }, expectedRevision: 0, profile: { ...identity, endpointRef }, audit: { operatorRef: "operator", source: "test", requestRef: "request", authorizationRef: "authorization" } });
  const ref: LinearRouterTaskRef = { organizationId, issueId: `issue-codex-restart-${prior}`, teamId, agentSessionId: `session-codex-restart-${prior}` };
  let router!: Router, port!: NodeDispatchDouble, service!: WorkerCommandService, executor!: LinearCodexExecutor;
  const sink = new RouterNodeIngestSink(() => router, () => port);
  let id = 0;
  const configuration = (adapter: LinearCodexExecutor): WorkerCommandOptions => ({
    configuration: {
      bearerToken: "worker-codex-restart-token", workerId: identity.workerId, runtimeKind: "codex", executorId: identity.runtimeProfile.ref, nodeId: identity.nodeId,
      methodRef: { version: "v1", method: "codex.execute" }, workspaces: [{ organizationId, workspaceRoot: directory }],
    },
    registrations: [codexExecutorRegistration({ executorId: identity.runtimeProfile.ref, adapter })],
    eventSink: sink,
    journal,
    now: () => new Date("2026-09-24T00:00:01.000Z"),
    messageId: () => `worker-codex-restart-${++id}`,
  });
  executor = executorFor(home);
  service = new WorkerCommandService(configuration(executor));
  port = new NodeDispatchDouble({
    worker: { async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> { return (await service.execute(command)).response; } },
    nodeRef,
    reservation: nodeReservationFromEndpoint({ endpointRef, identity, sessionBindingExecutorId: identity.runtimeProfile.ref }, "worker-key-codex-restart-1"),
  });
  router = new Router({
    nodeRouting: { bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef, nodeRef }] }), port },
    now: () => new Date("2026-09-24T00:00:02.000Z"),
  });
  context.after(async () => { service.close(); executor.close(); journal.close(); router.close(); teamBindings.close(); await rm(directory, { recursive: true, force: true }); });

  home.state.turn = prior;
  const started = await router.startNode({ ...ref, commandId: `command-codex-restart-${prior}-start`, instruction: "Remember a word.", workerSelection: workerSelectionFixture() });
  assert.equal(started.status, "accepted");
  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", prior === "wait" ? "task.waiting_for_input" : "task.succeeded"]);

  service.close();
  executor.close();
  const restartedFrom = home.processes.length;
  executor = executorFor(home);
  service = new WorkerCommandService(configuration(executor));
  home.state.turn = "succeed";
  return { home, router, sink, ref, restartedFrom };
}

for (const prior of ["wait", "succeed"] as const) {
  test(`a restarted Worker continues a Codex Run that ${prior === "wait" ? "parked on waiting_for_input" : "succeeded"} by resuming the same thread`, async (context) => {
    const { home, router, sink, ref, restartedFrom } = await priorRunThenRestart(context, prior);
    try {
      const continued = await router.continueNode({ ...ref, commandId: `command-codex-restart-${prior}-continue`, instruction: "Which word?" });
      assert.equal(continued.status, "accepted");
    } catch (error) {
      assert.fail(`the restarted Worker refused the continuation: ${error instanceof RouterError && error.cause instanceof WorkerCommandError ? error.cause.code : String(error)}`);
    }
    assert.deepEqual((await settled(router, ref, 4)).slice(2), ["task.accepted", "task.succeeded"]);
    const native = home.nativeRequests(restartedFrom);
    assert.deepEqual(native.map((message) => message.method), ["thread/list", "thread/list", "thread/unsubscribe", "thread/resume", "turn/start"]);
    assert.deepEqual(native.slice(0, 2).map((message) => message.params), [LIST_QUERY, { ...LIST_QUERY, cursor: "cursor-2" }]);
    assert.deepEqual(native.slice(2).map((message) => (message.params as Json).threadId), [THREAD_ID, THREAD_ID, THREAD_ID]);
    assert.deepEqual(sink.results.filter((result) => result.status === "quarantined"), []);
  });
}

test("a thread the app-server no longer lists is continuation_unresumable, and nothing is resumed or started", async (context) => {
  const { home, router, ref, restartedFrom } = await priorRunThenRestart(context, "wait");
  home.state.list = "absent";
  await assert.rejects(router.continueNode({ ...ref, commandId: "command-codex-restart-absent-continue", instruction: "Which word?" }), (error: unknown) => {
    assert.ok(error instanceof RouterError && error.cause instanceof WorkerCommandError);
    assert.equal(error.cause.code, "axis.worker.continuation_unresumable");
    return true;
  });
  assert.deepEqual(home.nativeRequests(restartedFrom).map((message) => message.method), ["thread/list", "thread/list"]);
  assert.equal(home.processes.length, restartedFrom + 1, "only the base transport's app-server was spawned");
});

test("a thread/list error is capability_unavailable, and nothing is resumed or started", async (context) => {
  const { home, router, ref, restartedFrom } = await priorRunThenRestart(context, "wait");
  home.state.list = "error";
  await assert.rejects(router.continueNode({ ...ref, commandId: "command-codex-restart-error-continue", instruction: "Which word?" }), (error: unknown) => {
    assert.ok(error instanceof RouterError && error.cause instanceof WorkerCommandError);
    assert.equal(error.cause.code, "axis.worker.capability_unavailable");
    return true;
  });
  assert.deepEqual(home.nativeRequests(restartedFrom).map((message) => message.method), ["thread/list"]);
  assert.equal(home.processes.length, restartedFrom + 1, "only the base transport's app-server was spawned");
});
