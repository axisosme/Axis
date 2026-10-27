import { nodeConfigurationFixture } from "../support/node-configuration.js";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  CODEX_EXECUTOR_CONTRACT_VERSION,
  SqliteWorkerCommandJournal,
  WorkerCommandService,
  startWorkerHttpService,
  type CodexLiveBinding,
  type Executor,
  type ExecutorCancelRequest,
  type ExecutorContinueRequest,
  type ExecutorLifecycleEvent,
  type ExecutorRegistration,
  type ExecutorStartRequest,
} from "../../src/index.js";
import { RouterEventDelivery } from "../../src/node/router-event-delivery.js";
import { startNode } from "../../src/node/runtime.js";
import { HttpNodeWorkerClient } from "../../src/node/worker-client.js";
import { startRouterHttpServiceWithNodeEvents } from "../../src/router/http.js";
import type { Router } from "../../src/router/router.js";
import { nodeEventId } from "../../src/shared/router-node.js";
import type { NodeCommandRequest, NodeWorkerDispatchPort } from "../../src/shared/router-node-commands.js";
import type { RouterRunLifecycleEnvelope } from "../../src/shared/router-worker.js";
import { RouterHttpEventSink } from "../../src/worker/router-event-sink.js";
import { nodeFixtureId, nodeFixtureRef, nodeFixtureTarget } from "../support/router-node-fixtures.js";
import { buildNodeStartRequest, nodeAuthenticatedContext, nodeReservation } from "../support/router-node-commands-fixtures.js";

/**
 * Exercise ambiguous receipt convergence through a production Worker, real Node command
 * runtime and Router's Node event ingress. Inject faults at transport, event
 * destination or executor boundaries without reordering Worker behavior or modifying
 * Node state.
 */

async function listen(server: Server): Promise<URL> {
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("test server did not bind");
  return new URL(`http://127.0.0.1:${address.port}`);
}

async function close(server: Server): Promise<void> {
  if (!server.listening) return;
  server.closeAllConnections();
  await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
}

async function availablePort(): Promise<number> {
  const server = createServer(), url = await listen(server);
  await close(server);
  return Number(url.port);
}

async function eventually(label: string, predicate: () => boolean | Promise<boolean>, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(`Node convergence condition did not settle: ${label}`);
}

const pause = (milliseconds: number) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds));

async function readBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return Buffer.concat(chunks);
}

/** Executes nothing: a start is accepted with one live binding that reports `started` and then stays running. */
class HeldExecutor implements Executor<CodexLiveBinding> {
  readonly contractVersion = CODEX_EXECUTOR_CONTRACT_VERSION;
  #closed = false;
  readonly #waiters: (() => void)[] = [];
  readonly #counts: { starts: number };
  readonly #throwOnStart: boolean;
  constructor(counts: { starts: number }, options: { readonly throwOnStart: boolean }) { this.#counts = counts; this.#throwOnStart = options.throwOnStart; }
  async start(request: ExecutorStartRequest) {
    this.#counts.starts += 1;
    if (this.#throwOnStart) throw new Error("Synthetic executor launch failure");
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "start", status: "accepted", binding: this.#binding() } as const;
  }
  async continue(request: ExecutorContinueRequest<CodexLiveBinding>) {
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "continue", status: "accepted", binding: this.#binding() } as const;
  }
  async cancel(request: ExecutorCancelRequest<CodexLiveBinding>) {
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "cancel", status: "interrupt_requested", binding: request.binding } as const;
  }
  async *observe(binding: CodexLiveBinding): AsyncIterableIterator<ExecutorLifecycleEvent<CodexLiveBinding>> {
    yield { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding, sequence: 1, outcome: "started" };
    while (!this.#closed) await new Promise<void>((resolve) => this.#waiters.push(resolve));
  }
  close(): void { this.#closed = true; for (const wake of this.#waiters.splice(0)) wake(); }
  #binding(): CodexLiveBinding { return { runtimeKind: "codex", threadId: `thread-convergence-${this.#counts.starts}`, turnId: `turn-convergence-${this.#counts.starts}` }; }
}

function registration(executor: HeldExecutor): ExecutorRegistration<CodexLiveBinding> {
  return {
    runtimeKind: "codex",
    executorId: nodeFixtureTarget.sessionBindingExecutorId,
    methodRef: { version: "v1", method: "codex.execute" },
    supportedOperations: ["start", "continue", "cancel", "observe"],
    adapter: executor,
    validateBinding: (value): value is CodexLiveBinding => typeof value === "object" && value !== null
      && (value as { runtimeKind?: unknown }).runtimeKind === "codex"
      && typeof (value as { threadId?: unknown }).threadId === "string" && (value as { threadId: string }).threadId.length > 0
      && typeof (value as { turnId?: unknown }).turnId === "string" && (value as { turnId: string }).turnId.length > 0,
    bindingsEqual: (left, right) => left.threadId === right.threadId && left.turnId === right.turnId,
    methodRefEquals: (left, right) => left.version === right.version && left.method === right.method,
    projectLifecycle: () => ({ kind: "progress", completed: 1, total: 1, safeMessage: "Synthetic held run." }),
  };
}

interface ProxyControl {
  /** The Worker the proxy currently forwards to; a restarted Worker listens on a new port. */
  workerUrl: URL;
  /** Dispatches (POST) that reached the proxy, whether or not they were forwarded. */
  dispatches: number;
  /** Read-only command records (GET) forwarded to the Worker. */
  reads: number;
  /** Read-only command records (GET) that reached the proxy, counted on arrival, before any hold. */
  readArrivals: number;
  /** Upcoming dispatch responses to drop after the Worker produced (and recorded) them. */
  loseNextDispatchResponses: number;
  /** Upcoming dispatches to drop before they reach the Worker at all. */
  dropNextDispatchRequests: number;
  /** When set, each read-only command record is rewritten before the Node sees it. */
  tamperRead: ((body: Record<string, unknown>) => Record<string, unknown>) | undefined;
  /** When set, read-only command records wait for it before being forwarded. */
  holdReads: Promise<void> | undefined;
}

async function lossyWorkerProxy(control: ProxyControl): Promise<Server> {
  const proxy = createServer((request: IncomingMessage, response: ServerResponse) => {
    void (async () => {
      const body = await readBody(request);
      const isDispatch = request.method === "POST" && request.url === "/v1/worker/commands";
      const isRead = request.method === "GET" && request.url?.startsWith("/v1/worker/commands/") === true;
      if (isDispatch) {
        control.dispatches += 1;
        if (control.dropNextDispatchRequests > 0) { control.dropNextDispatchRequests -= 1; request.socket.destroy(); return; }
      }
      if (isRead) { control.readArrivals += 1; if (control.holdReads !== undefined) await control.holdReads; control.reads += 1; }
      const upstream = await fetch(new URL(request.url ?? "/", control.workerUrl), {
        method: request.method ?? "GET",
        headers: Object.fromEntries(Object.entries(request.headers).filter(([name]) => name === "authorization" || name === "content-type")) as Record<string, string>,
        ...(request.method === "GET" || request.method === "HEAD" ? {} : { body: new Uint8Array(body) }),
      });
      let text = await upstream.text();
      if (isDispatch && control.loseNextDispatchResponses > 0) {
        control.loseNextDispatchResponses -= 1;
        request.socket.destroy();
        return;
      }
      if (isRead && control.tamperRead !== undefined && upstream.status === 200) text = JSON.stringify(control.tamperRead(JSON.parse(text) as Record<string, unknown>));
      response.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") ?? "application/json" });
      response.end(text);
    })().catch(() => response.destroy());
  });
  await listen(proxy);
  return proxy;
}

const secondTarget = {
  endpointRef: { ref: "endpoint-2", version: "v1" },
  identity: { ...nodeFixtureTarget.identity, workerId: "worker-2" },
  sessionBindingExecutorId: "executor-2",
} as const;

function nodeEnvironment(input: { readonly database: string; readonly workerUrl: URL; readonly routerUrl: URL; readonly port: number; readonly eventPort: number; readonly secondWorker: boolean }) {
  const workers: import("../support/node-configuration.js").FixtureWorker[] = [{
    workerKey: nodeReservation.workerKey,
    target: nodeFixtureTarget,
    workerEndpoint: input.workerUrl.origin,
    commandToken: "worker-command-token",
    eventToken: "worker-event-token",
    readinessMode: "codex",
  }];
  if (input.secondWorker) workers.push({ workerKey: "codex-secondary", target: secondTarget, workerEndpoint: input.workerUrl.origin, commandToken: "worker-2-command-token", eventToken: "worker-2-event-token", readinessMode: "codex" });
  return {
    database: input.database,
    nodeId: nodeFixtureId,
    nodeRef: nodeFixtureRef.ref,
    nodeVersion: nodeFixtureRef.version,
    token: "node-token",
    host: "127.0.0.1",
    port: input.port,
    eventHost: "127.0.0.1",
    eventPort: input.eventPort,
    routerEndpoint: input.routerUrl.origin,
    // The Node composition validates its Node-event credential before opening stores.
    // Supply the same bearer used by the injected Router event delivery.
    routerEventToken: "node-router-token",
    defaultWorkerKey: nodeReservation.workerKey,
    eventRetryInitialDelayMs: 100,
    eventRetryMaxDelayMs: 1000,
    eventMaxConcurrentRuns: 2,
    workers: workers,
  } as const;
}

async function postJson(url: URL, token: string, value: unknown): Promise<{ readonly status: number; readonly body: Record<string, unknown> }> {
  const response = await fetch(url, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify(value) });
  return { status: response.status, body: await response.json() as Record<string, unknown> };
}

interface HarnessOptions {
  /** Deliver the Worker's lifecycle events to the Node (default). Off: they go to a closed port and never arrive. */
  readonly workerEventsReachNode?: boolean;
  readonly throwOnStart?: boolean;
  /** The Linear organization the Worker has a workspace for. The start fixture's is `org-1`. */
  readonly workspaceOrganizationId?: string;
  readonly secondWorker?: boolean;
  /** Wraps the Node's real Worker client, e.g. to model a read that has already left and cannot be aborted. */
  readonly wrapWorkerClient?: (client: NodeWorkerDispatchPort) => NodeWorkerDispatchPort;
}

async function convergenceHarness(context: { after(fn: () => unknown): void }, options: HarnessOptions = {}) {
  const directory = await mkdtemp(join(tmpdir(), "axis-convergence-e2e-"));
  const cleanup: (() => unknown)[] = [() => rm(directory, { recursive: true, force: true })];
  context.after(async () => { for (const release of cleanup.reverse()) await release(); });

  const routerEvents: RouterRunLifecycleEnvelope[] = [];
  const router = {
    async ingestNodeRunEvent(input: { readonly envelope: RouterRunLifecycleEnvelope }) {
      routerEvents.push(input.envelope);
      return { status: "projected", sourceMessageId: nodeEventId(input.envelope), receipt: {}, replayed: false } as const;
    },
  } as unknown as Router;
  const routerService = await startRouterHttpServiceWithNodeEvents({
    router,
    nodeEventBindings: [{ bearerToken: "node-router-token", authenticatedNode: nodeAuthenticatedContext, allowedOrigins: [nodeFixtureTarget.identity, secondTarget.identity] }],
  });
  cleanup.push(() => routerService.close());

  const nodePort = await availablePort(), eventPort = await availablePort();
  const workspaceRoot = join(directory, "workspace");
  await mkdir(workspaceRoot, { recursive: true });
  const workerDatabase = join(directory, "worker.sqlite");
  const counts = { starts: 0 };
  const eventEndpoint = options.workerEventsReachNode === false ? "http://127.0.0.1:9" : `http://127.0.0.1:${eventPort}`;

  let workerJournal = new SqliteWorkerCommandJournal(workerDatabase);
  let executor = new HeldExecutor(counts, { throwOnStart: options.throwOnStart === true });
  const createWorker = () => new WorkerCommandService({
    configuration: {
      bearerToken: "worker-command-token",
      workerId: nodeFixtureTarget.identity.workerId,
      runtimeKind: nodeFixtureTarget.identity.executorKind,
      executorId: nodeFixtureTarget.sessionBindingExecutorId,
      nodeId: nodeFixtureTarget.identity.nodeId,
      runtimeProfile: nodeFixtureTarget.identity.runtimeProfile,
      methodRef: { version: "v1", method: "codex.execute" },
      workspaces: [{ organizationId: options.workspaceOrganizationId ?? "org-1", workspaceRoot }],
    },
    registrations: [registration(executor)],
    eventSink: new RouterHttpEventSink({ endpoint: eventEndpoint, bearerToken: "worker-event-token" }),
    journal: workerJournal,
  });
  let worker = createWorker();
  let workerHttp = await startWorkerHttpService({ commandService: worker });
  const stopWorker = async () => { worker.close(); executor.close(); await workerHttp.close(); workerJournal.close(); };
  cleanup.push(() => stopWorker());

  const control: ProxyControl = { workerUrl: workerHttp.url, dispatches: 0, reads: 0, readArrivals: 0, loseNextDispatchResponses: 0, dropNextDispatchRequests: 0, tamperRead: undefined, holdReads: undefined };
  const proxy = await lossyWorkerProxy(control);
  cleanup.push(() => close(proxy));
  const proxyAddress = proxy.address();
  if (proxyAddress === null || typeof proxyAddress === "string") throw new Error("proxy did not bind");
  const proxyUrl = new URL(`http://127.0.0.1:${proxyAddress.port}`);

  const database = join(directory, "node.sqlite");
  const factories = {
    createWorkerClient: (clientOptions: ConstructorParameters<typeof HttpNodeWorkerClient>[0]) => {
      const client = new HttpNodeWorkerClient(clientOptions);
      return options.wrapWorkerClient === undefined ? client : options.wrapWorkerClient(client);
    },
    createRouterEventDelivery: (deliveryOptions: Omit<ConstructorParameters<typeof RouterEventDelivery>[0], "bearerToken">) => new RouterEventDelivery({ ...deliveryOptions, bearerToken: "node-router-token" }),
  };
  const environment = nodeEnvironment({ database, workerUrl: proxyUrl, routerUrl: routerService.url, port: nodePort, eventPort, secondWorker: options.secondWorker === true });
  let runtime = await startNode(nodeConfigurationFixture(environment), factories);
  cleanup.push(() => runtime.close());

  return {
    counts, control, routerEvents,
    get runtime() { return runtime; },
    get workerJournal() { return workerJournal; },
    async restartNode() { await runtime.close(); runtime = await startNode(nodeConfigurationFixture(environment), factories); },
    /** Restart the Worker exactly as its production composition starts: recover, replay, then listen. */
    async restartWorker() {
      await stopWorker();
      workerJournal = new SqliteWorkerCommandJournal(workerDatabase);
      executor = new HeldExecutor(counts, { throwOnStart: options.throwOnStart === true });
      worker = createWorker();
      await worker.recoverActiveRuns();
      await worker.replayUndeliveredEvents();
      workerHttp = await startWorkerHttpService({ commandService: worker });
      control.workerUrl = workerHttp.url;
    },
    database,
    dispatch: (request: unknown) => postJson(new URL("/v1/node/commands", runtime.service.url), "node-token", request),
    postEvent: (token: string, envelope: unknown) => postJson(new URL("/v1/router/run-events", runtime.service.eventUrl), token, { envelope, recordedAt: new Date().toISOString() }),
    routerSawAccepted: () => routerEvents.some((event) => ("deliveryType" in event ? event.envelope : event).envelopeType === "run.accepted"),
    recordedResponse: (commandId: string) => workerJournal.readCommand(commandId)?.response,
  };
}

test("a start whose Worker response was lost converges to accepted with no user action and executes once", async (context) => {
  const harness = await convergenceHarness(context);
  const request = await buildNodeStartRequest();
  harness.control.loseNextDispatchResponses = 1;

  const first = await harness.dispatch(request);
  assert.equal(first.status, 202, JSON.stringify(first.body));
  assert.equal(first.body.status, "ambiguous", "the lost response leaves the receipt ambiguous");
  assert.equal(harness.control.dispatches, 1);

  // No further input from anyone: the Worker's own retried run.accepted is refused, which triggers the read.
  await eventually("the Run's run.accepted reaches the Router door", () => harness.routerSawAccepted());
  assert.ok(harness.control.reads >= 1, "convergence read the Worker's recorded response");

  const replay = await harness.dispatch(request);
  assert.equal(replay.body.status, "accepted", JSON.stringify(replay.body));
  assert.equal(harness.control.dispatches, 1, "the Node never re-dispatched");
  assert.equal(harness.counts.starts, 1, "the Worker executed the start exactly once");
  assert.deepEqual((replay.body as { readonly result?: unknown }).result, harness.recordedResponse(request.command.commandId), "the accepted receipt carries the Worker's own recorded response");
});

test("a Worker restarted before convergence still answers from its recorded response, and never executes again", async (context) => {
  const harness = await convergenceHarness(context);
  const request = await buildNodeStartRequest();
  let releaseReads!: () => void;
  harness.control.holdReads = new Promise<void>((resolve) => { releaseReads = resolve; });
  harness.control.loseNextDispatchResponses = 1;

  assert.equal((await harness.dispatch(request)).body.status, "ambiguous");
  await harness.restartWorker();
  harness.control.holdReads = undefined;
  releaseReads();

  await eventually("the restarted Worker's replayed run.accepted reaches the Router door", () => harness.routerSawAccepted());
  const replay = await harness.dispatch(request);
  assert.equal(replay.body.status, "accepted", JSON.stringify(replay.body));
  assert.deepEqual((replay.body as { readonly result?: unknown }).result, harness.recordedResponse(request.command.commandId));
  assert.equal(harness.control.dispatches, 1);
  assert.equal(harness.counts.starts, 1, "neither Worker process executed the start again");
});

test("when no Run event ever arrives, the Node's open-time sweep converges the receipt", async (context) => {
  const harness = await convergenceHarness(context, { workerEventsReachNode: false });
  const request = await buildNodeStartRequest();
  harness.control.loseNextDispatchResponses = 1;

  assert.equal((await harness.dispatch(request)).body.status, "ambiguous");
  await pause(500);
  assert.equal(harness.control.reads, 0, "with no refused event there is no event-door trigger");
  assert.equal((await harness.dispatch(request)).body.status, "ambiguous");

  await harness.restartNode();
  await eventually("the open-time sweep read the Worker's recorded response", () => harness.control.reads >= 1);
  await eventually("the receipt converged", async () => (await harness.dispatch(request)).body.status === "accepted");
  assert.equal(harness.control.dispatches, 1);
  assert.equal(harness.counts.starts, 1);
});

test("a dispatch the Worker never received stays ambiguous: the Worker has nothing recorded", async (context) => {
  const harness = await convergenceHarness(context);
  const request = await buildNodeStartRequest();
  harness.control.dropNextDispatchRequests = 1;

  assert.equal((await harness.dispatch(request)).body.status, "ambiguous");
  await harness.restartNode();
  await eventually("the open-time sweep asked the Worker", () => harness.control.reads >= 1);
  await pause(300);
  assert.equal((await harness.dispatch(request)).body.status, "ambiguous", "not_recorded never becomes accepted");
  assert.equal(harness.recordedResponse(request.command.commandId), undefined);
  assert.equal(harness.counts.starts, 0);
  assert.equal(harness.control.dispatches, 1, "an ambiguous command is never re-dispatched to find out");
  assert.equal(harness.routerSawAccepted(), false);
});

test("a Worker that failed before recording a response leaves the receipt ambiguous", async (context) => {
  const harness = await convergenceHarness(context, { throwOnStart: true });
  const request = await buildNodeStartRequest();

  assert.equal((await harness.dispatch(request)).body.status, "ambiguous", "launch_ambiguous is an ambiguous Worker failure");
  assert.equal(harness.counts.starts, 1);
  assert.equal(harness.recordedResponse(request.command.commandId), undefined);
  await harness.restartNode();
  await eventually("the open-time sweep asked the Worker", () => harness.control.reads >= 1);
  await pause(300);
  assert.equal((await harness.dispatch(request)).body.status, "ambiguous");
  assert.equal(harness.routerSawAccepted(), false);
  assert.equal(harness.counts.starts, 1, "no second launch");
});

test("a recorded response that does not match the command is never accepted", async (context) => {
  const harness = await convergenceHarness(context);
  const request = await buildNodeStartRequest();
  harness.control.tamperRead = (body) => {
    const response = body.response as Record<string, unknown> | undefined;
    const identity = response?.identity as Record<string, unknown> | undefined;
    return response === undefined || identity === undefined ? body : { ...body, response: { ...response, identity: { ...identity, workerId: "worker-forged" } } };
  };
  harness.control.loseNextDispatchResponses = 1;

  assert.equal((await harness.dispatch(request)).body.status, "ambiguous");
  await eventually("the Node read the (tampered) record", () => harness.control.reads >= 1);
  await pause(1_500);
  assert.equal((await harness.dispatch(request)).body.status, "ambiguous", "an invalid response keeps the receipt ambiguous");
  assert.equal(harness.routerSawAccepted(), false);

  // Control: the same trigger converges once the read is honest again.
  harness.control.tamperRead = undefined;
  await eventually("the honest record converges the receipt", () => harness.routerSawAccepted());
  assert.equal((await harness.dispatch(request)).body.status, "accepted");
  assert.equal(harness.counts.starts, 1);
});

test("a definite Worker refusal is rejected and is never read as a convergence candidate", async (context) => {
  const harness = await convergenceHarness(context, { workspaceOrganizationId: "some-other-organization" });
  const request = await buildNodeStartRequest();

  assert.equal((await harness.dispatch(request)).body.status, "rejected");
  await harness.restartNode();
  await pause(500);
  assert.equal(harness.control.reads, 0, "only ambiguous receipts are swept");
  assert.equal((await harness.dispatch(request)).body.status, "rejected");
  assert.equal(harness.counts.starts, 0);
});

test("an event sent with another Worker's credential never causes a read; the owner's does", async (context) => {
  const harness = await convergenceHarness(context, { workerEventsReachNode: false, secondWorker: true });
  const request = await buildNodeStartRequest();
  harness.control.loseNextDispatchResponses = 1;

  assert.equal((await harness.dispatch(request)).body.status, "ambiguous");
  const recorded = harness.recordedResponse(request.command.commandId);
  if (recorded === undefined || recorded.status !== "accepted") assert.fail("the Worker recorded an accepted response");

  const foreign = await harness.postEvent("worker-2-event-token", recorded.runAccepted);
  assert.equal(foreign.status, 404, JSON.stringify(foreign.body));
  await pause(500);
  assert.equal(harness.control.reads, 0, "a Worker that does not own the route cannot trigger a read against it");

  const owner = await harness.postEvent("worker-event-token", recorded.runAccepted);
  assert.equal(owner.status, 404, "the refusal itself stands");
  await eventually("the owner's refused event triggered the read", () => harness.control.reads >= 1);
  await eventually("the receipt converged", async () => (await harness.dispatch(request)).body.status === "accepted");
});

test("a burst of refused events for one Run makes exactly one Worker read", async (context) => {
  const harness = await convergenceHarness(context, { workerEventsReachNode: false });
  const request = await buildNodeStartRequest();
  harness.control.loseNextDispatchResponses = 1;

  assert.equal((await harness.dispatch(request)).body.status, "ambiguous");
  const recorded = harness.recordedResponse(request.command.commandId);
  if (recorded === undefined || recorded.status !== "accepted") assert.fail("the Worker recorded an accepted response");

  let releaseReads!: () => void;
  harness.control.holdReads = new Promise<void>((resolve) => { releaseReads = resolve; });
  const refusals = await Promise.all(Array.from({ length: 8 }, () => harness.postEvent("worker-event-token", recorded.runAccepted)));
  assert.deepEqual(refusals.map(({ status }) => status), Array(8).fill(404));
  harness.control.holdReads = undefined;
  releaseReads();

  await eventually("the receipt converged", async () => (await harness.dispatch(request)).body.status === "accepted");
  assert.equal((await harness.postEvent("worker-event-token", recorded.runAccepted)).status, 202, "after convergence the event is admitted");
  await pause(300);
  assert.equal(harness.control.reads, 1, "concurrent triggers shared one read");
});

/** A second start for a distinct Run and conversation, so several receipts can be ambiguous at once. */
function distinctStart(base: NodeCommandRequest, suffix: string): NodeCommandRequest {
  if (base.command.command !== "start") throw new Error("start fixture required");
  const start = base.command.start, sourceRef = start.identity.task.sourceRef;
  const changedSourceRef = sourceRef.system === "linear" ? { ...sourceRef, agentSessionId: `session-${suffix}` } : { ...sourceRef, conversationId: `session-${suffix}` };
  const runId = `run-${suffix}`, sessionBindingId = `binding-${suffix}`;
  return {
    ...base,
    command: {
      ...base.command,
      commandId: `command-${suffix}`,
      start: { ...start, messageId: `message-${suffix}`, operationId: `operation-${suffix}`, identity: { ...start.identity, task: { ...start.identity.task, sourceRef: changedSourceRef }, run: { ...start.identity.run, runId, sessionBindingId }, sessionBinding: { ...start.identity.sessionBinding, runId, sessionBindingId } } },
    },
  } as NodeCommandRequest;
}

function commandStates(database: string): Record<string, string> {
  const reader = new DatabaseSync(database, { readOnly: true });
  try {
    return Object.fromEntries((reader.prepare("SELECT command_id, state FROM commands ORDER BY command_id").all() as { command_id: string; state: string }[]).map((row) => [row.command_id, row.state]));
  } finally { reader.close(); }
}

test("shutdown is bounded: close() aborts the stuck Worker reads it waits for, and a restart converges them", { timeout: 90_000 }, async (context) => {
  const harness = await convergenceHarness(context, { workerEventsReachNode: false });
  const base = await buildNodeStartRequest();
  const requests = ["held-a", "held-b"].map((suffix) => distinctStart(base, suffix));
  harness.control.loseNextDispatchResponses = requests.length;
  for (const request of requests) assert.equal((await harness.dispatch(request)).body.status, "ambiguous");

  // k = 2 Worker reads leave the Node and never come back: the Worker is reachable but does not answer.
  let releaseReads!: () => void;
  harness.control.holdReads = new Promise<void>((resolve) => { releaseReads = resolve; });
  for (const request of requests) {
    const response = harness.recordedResponse(request.command.commandId);
    if (response === undefined || response.status !== "accepted") assert.fail("the Worker recorded an accepted response");
    assert.equal((await harness.postEvent("worker-event-token", response.runAccepted)).status, 404);
  }
  await eventually("both stuck reads reached the Worker side", () => harness.control.readArrivals === 2);

  const startedAt = Date.now();
  await harness.runtime.close();
  const elapsedMs = Date.now() - startedAt;
  // The dispatch timeout here is the production default, 30 s: without the abort close() waits for it.
  assert.ok(elapsedMs < 5_000, `close() must not wait for a Worker dispatch timeout; it took ${elapsedMs} ms`);
  await assert.rejects(fetch(new URL("/v1/node/readiness", harness.runtime.service.url)), "the command listener is closed");
  await assert.rejects(fetch(new URL("/v1/router/run-events", harness.runtime.service.eventUrl), { method: "POST" }), "the event listener is closed");
  assert.deepEqual(Object.values(commandStates(harness.database)), ["ambiguous", "ambiguous"], "aborted reads leave both receipts ambiguous");

  // The journal was released cleanly: the same Node reopens it, and the open-time sweep converges both.
  harness.control.holdReads = undefined;
  releaseReads();
  await harness.restartNode();
  await eventually("the restart's sweep converged both receipts", () => Object.values(commandStates(harness.database)).every((state) => state === "accepted"));
  assert.equal(harness.control.dispatches, requests.length, "nothing was re-dispatched");
  assert.equal(harness.counts.starts, requests.length, "each start executed once");
});

test("shutdown waits for work already started, and once it began neither the event door nor the sweep starts another Worker read", { timeout: 90_000 }, async (context) => {
  // Reads that have already left and cannot be aborted: this client ignores the shutdown signal, so any read
  // started after shutdown began would reach the proxy and be counted.
  const harness = await convergenceHarness(context, {
    workerEventsReachNode: false,
    wrapWorkerClient: (client) => ({ dispatch: (target, command) => client.dispatch(target, command), readCommand: (target, commandId) => client.readCommand(target, commandId) }),
  });
  const base = await buildNodeStartRequest();
  const [first, ...rest] = ["door-a", "late-b", "late-c"].map((suffix) => distinctStart(base, suffix));
  const requests = [first!, ...rest];
  harness.control.loseNextDispatchResponses = requests.length;
  for (const request of requests) assert.equal((await harness.dispatch(request)).body.status, "ambiguous");
  const accepted = (request: NodeCommandRequest) => {
    const response = harness.recordedResponse(request.command.commandId);
    if (response === undefined || response.status !== "accepted") assert.fail("the Worker recorded an accepted response");
    return response.runAccepted;
  };
  const hold = () => { let release!: () => void; harness.control.holdReads = new Promise<void>((resolve) => { release = resolve; }); return () => { harness.control.holdReads = undefined; release(); }; };

  // Event door. A's read is in flight when close() begins.
  let release = hold();
  assert.equal((await harness.postEvent("worker-event-token", accepted(first!))).status, 404);
  await eventually("A's read reached the Worker side", () => harness.control.readArrivals === 1);
  let closed = false;
  let closing = harness.runtime.close().then(() => { closed = true; });
  await pause(300);
  assert.equal(closed, false, "close() waits for the convergence already in flight");
  // The listener is still open while close() waits, so this trigger really reaches the door ...
  assert.equal((await harness.postEvent("worker-event-token", accepted(requests[1]!))).status, 404, "the event door still answered during shutdown");
  await pause(300);
  // ... and starts no read.
  assert.equal(harness.control.readArrivals, 1, "a trigger for another command during shutdown starts no Worker read");
  release();
  await closing;
  assert.deepEqual(commandStates(harness.database), { [first!.command.commandId]: "accepted", [requests[1]!.command.commandId]: "ambiguous", [requests[2]!.command.commandId]: "ambiguous" },
    "the in-flight convergence committed before the journal closed; the late trigger converged nothing");

  // Sweep. The reopened Node's sweep reads one of B and C; close() begins while that read is held.
  release = hold();
  await harness.restartNode();
  await eventually("the sweep's first read reached the Worker side", () => harness.control.readArrivals === 2);
  closed = false;
  closing = harness.runtime.close().then(() => { closed = true; });
  await pause(300);
  assert.equal(closed, false, "close() waits for the sweep's read in flight");
  release();
  await closing;
  await pause(300);
  assert.equal(harness.control.readArrivals, 2, "the sweep starts no further read once shutdown began");
  const late = requests.slice(1).map((request) => commandStates(harness.database)[request.command.commandId]).sort();
  assert.deepEqual(late, ["accepted", "ambiguous"], "exactly the swept command converged; the other waits for the next open");
});
