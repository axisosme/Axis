import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { NodeCommandService } from "../../src/node/command.js";
import { NodeEventIngress, nodeEventIngressHandler } from "../../src/node/event-ingress.js";
import { SqliteNodeJournal } from "../../src/node/journal.js";
import { HttpNodeWorkerClient, NodeWorkerClientDispatcher } from "../../src/node/worker-client.js";
import {
  NODE_CONTRACT_VERSION,
  nodeMaintenanceStatus,
  type NodeTarget,
} from "../../src/shared/router-node.js";
import type { NodeRuntimeWorkerConfig } from "../../src/node/runtime-config.js";
import type { RouterRunLifecycleEnvelope, WorkerCommand, WorkerCommandResponse, WorkerVersionedReference } from "../../src/shared/router-worker.js";
import { NODE_COMMAND_CONTRACT_VERSION, type NodeCommandRequest } from "../../src/shared/router-node-commands.js";
import { buildNodeAcceptedEvent } from "../support/router-node-fixtures.js";
import { nodeAuthenticatedContext, nodeReservation } from "../support/router-node-commands-fixtures.js";

export const faultInstant = "2026-09-07T00:00:00.000Z";

/** The Node and Worker a fault ingress authenticates events for. */
export interface FaultEventRoute {
  readonly nodeRef: WorkerVersionedReference;
  readonly target: NodeTarget;
}

type Deferred<T> = {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((accept, decline) => { resolve = accept; reject = decline; });
  return { promise, resolve, reject };
}

function send(response: ServerResponse, status: number, value: unknown): void {
  const body = JSON.stringify(value);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
  });
  response.end(body);
}

async function requestBody(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function listen(server: Server): Promise<URL> {
  await new Promise<void>((resolve, reject) => {
    const failed = (error: Error) => { server.off("listening", ready); reject(error); };
    const ready = () => { server.off("error", failed); resolve(); };
    server.once("error", failed);
    server.once("listening", ready);
    server.listen(0, "127.0.0.1");
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("fault listener did not bind");
  return new URL(`http://127.0.0.1:${address.port}`);
}

async function closeServer(server: Server): Promise<void> {
  if (!server.listening) return;
  await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
}

export async function availablePort(): Promise<number> {
  const server = createServer();
  const url = await listen(server);
  await closeServer(server);
  return Number(url.port);
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

function workerResponse(identity: NodeTarget["identity"], command: WorkerCommand): WorkerCommandResponse {
  if (command.command === "cancel") {
    return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: command.target.runId };
  }
  if (command.command === "steer") {
    return { contractVersion: command.contractVersion, commandId: command.commandId, status: "steered", runId: command.target.runId };
  }
  return {
    contractVersion: command.contractVersion,
    commandId: command.commandId,
    status: "accepted",
    identity,
    runAccepted: buildNodeAcceptedEvent(command.start),
  };
}

export type FaultWorkerMode = "respond" | "hold-before-accept" | "hold-after-accept";

export interface FaultWorker {
  readonly url: URL;
  readonly requestObserved: Promise<void>;
  readonly accepted: Promise<void>;
  release(): void;
  counts(): { readonly requests: number; readonly launches: number };
  close(): Promise<void>;
}

export async function startFaultWorker(directory: string, request: FaultEventRoute, mode: FaultWorkerMode): Promise<FaultWorker> {
  const database = new DatabaseSync(join(directory, "worker.sqlite"));
  database.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE worker_counts(singleton INTEGER PRIMARY KEY CHECK(singleton=1), requests INTEGER NOT NULL); INSERT INTO worker_counts VALUES (1,0); CREATE TABLE worker_launches(run_id TEXT PRIMARY KEY, command_id TEXT NOT NULL UNIQUE, response_json TEXT NOT NULL);");
  const observed = deferred<void>(), accepted = deferred<void>(), release = deferred<void>();
  const active = new Set<Promise<void>>();
  const server = createServer((incoming, response) => {
    const operation = (async () => {
      if (incoming.method !== "POST" || incoming.url !== "/v1/worker/commands" || incoming.headers.authorization !== "Bearer worker-token") {
        send(response, 401, { error: { code: "axis.worker.unauthorized", message: "Worker authorization is required." } });
        return;
      }
      const command = await requestBody(incoming) as WorkerCommand;
      database.exec("BEGIN IMMEDIATE");
      try {
        database.prepare("UPDATE worker_counts SET requests=requests+1 WHERE singleton=1").run();
        database.exec("COMMIT");
      } catch (error) {
        database.exec("ROLLBACK");
        throw error;
      }
      observed.resolve();
      if (mode === "hold-before-accept") await release.promise;
      const runId = command.command === "cancel" || command.command === "steer" ? command.target.runId : command.start.identity.run.runId;
      const prior = database.prepare("SELECT response_json FROM worker_launches WHERE run_id=?").get(runId) as { response_json: string } | undefined;
      const result = prior === undefined ? workerResponse(request.target.identity, command) : JSON.parse(prior.response_json) as WorkerCommandResponse;
      if (prior === undefined) database.prepare("INSERT INTO worker_launches VALUES (?,?,?)").run(runId, command.commandId, JSON.stringify(result));
      accepted.resolve();
      if (mode === "hold-after-accept") await release.promise;
      if (!response.destroyed) send(response, prior === undefined ? 202 : 200, result);
    })().catch(() => { response.destroy(); });
    active.add(operation);
    void operation.finally(() => active.delete(operation)).catch(() => undefined);
  });
  const url = await listen(server);
  return {
    url,
    requestObserved: observed.promise,
    accepted: accepted.promise,
    release: () => release.resolve(),
    counts() {
      const requests = database.prepare("SELECT requests FROM worker_counts WHERE singleton=1").get() as { requests: number };
      const launches = database.prepare("SELECT COUNT(*) AS count FROM worker_launches").get() as { count: number };
      return { requests: requests.requests, launches: launches.count };
    },
    async close() {
      release.resolve();
      server.closeAllConnections();
      await Promise.allSettled([...active]);
      await closeServer(server);
      database.close();
    },
  };
}

export interface TemporaryFaultState {
  readonly directory: string;
  readonly nodeDatabase: string;
  cleanup(): Promise<void>;
}

export async function temporaryFaultState(): Promise<TemporaryFaultState> {
  const directory = await mkdtemp(join(tmpdir(), "axis-node-faults-"));
  return {
    directory,
    nodeDatabase: join(directory, "node.sqlite"),
    cleanup: () => rm(directory, { recursive: true, force: true }),
  };
}

function workerConfiguration(request: FaultEventRoute, workerUrl: string): NodeRuntimeWorkerConfig {
  return deepFreeze({
    nodeRef: request.nodeRef,
    nodeId: request.target.identity.nodeId,
    target: request.target,
    workerKey: "fault-worker",
    workerEndpoint: workerUrl,
    commandToken: "worker-token",
    eventToken: "event-token-1",
    readinessMode: "codex",
  });
}

/** The route a command default start is reserved on: the fixture reservation's Worker. */
export function faultRoute(request: NodeCommandRequest): FaultEventRoute {
  return { nodeRef: request.nodeRef, target: nodeReservation.target };
}

export async function executeCommand(journal: SqliteNodeJournal, request: NodeCommandRequest, workerUrl: string) {
  const route = faultRoute(request);
  const service = new NodeCommandService({
    authenticatedNode: nodeAuthenticatedContext,
    journal,
    resolveNewSessionReservation: () => nodeReservation,
    // As the runtime composes it: the inventory dispatcher over the real HTTP Worker client.
    worker: new NodeWorkerClientDispatcher([{ target: route.target, client: new HttpNodeWorkerClient({ worker: workerConfiguration(route, workerUrl), workerDispatchTimeoutMs: 120_000 }) }]),
    now: () => new Date(faultInstant),
  });
  return service.executeWithDisposition(request);
}

export type NodeChildMode = "pre-claim" | "reserved" | "dispatching" | "completed" | "execute";

export function startNodeChild(input: {
  readonly mode: NodeChildMode;
  readonly database: string;
  readonly request: NodeCommandRequest;
  readonly workerUrl?: string;
}): ChildProcess {
  const code = `import { runNodeFaultChild } from ${JSON.stringify(import.meta.url)}; await runNodeFaultChild();`;
  return spawn(process.execPath, ["--import=tsx", "--input-type=module", "--eval", code], {
    cwd: process.cwd(),
    env: { ...process.env, AXIS_NODE_FAULT_CHILD: JSON.stringify(input) },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

export async function runNodeFaultChild(): Promise<void> {
  const value = process.env.AXIS_NODE_FAULT_CHILD;
  if (value === undefined) throw new Error("missing fault child input");
  const input = JSON.parse(value) as { readonly mode: NodeChildMode; readonly database: string; readonly request: NodeCommandRequest; readonly workerUrl?: string };
  const journal = await SqliteNodeJournal.open(input.database, { busyTimeoutMs: 0 });
  if (input.workerUrl === undefined) throw new Error("fault child requires worker URL");
  const cutline = input.mode;
  const hold = async (state: Exclude<NodeChildMode, "execute">): Promise<never> => {
    process.stdout.write(`${JSON.stringify({ type: "cutline", state })}\n`);
    return new Promise<never>(() => setInterval(() => undefined, 1_000));
  };
  const serviceJournal = cutline === "execute" ? journal : new Proxy(journal, {
    get(target, property) {
      if (property === "claimCommandAndRoute") {
        return async (claimInput: Parameters<SqliteNodeJournal["claimCommandAndRoute"]>[0]) => {
          if (cutline === "pre-claim") return hold("pre-claim");
          const result = await target.claimCommandAndRoute(claimInput);
          if (cutline === "reserved") return hold("reserved");
          return result;
        };
      }
      if (property === "recordReceipt") {
        return async (receiptInput: Parameters<SqliteNodeJournal["recordReceipt"]>[0]) => {
          const result = await target.recordReceipt(receiptInput);
          if (cutline === "dispatching" && receiptInput.receipt.status === "dispatching") return hold("dispatching");
          if (cutline === "completed" && receiptInput.receipt.status === "accepted") return hold("completed");
          return result;
        };
      }
      const member = Reflect.get(target, property, target) as unknown;
      return typeof member === "function" ? member.bind(target) : member;
    },
  });
  const result = await executeCommand(serviceJournal, input.request, input.workerUrl);
  process.stdout.write(`${JSON.stringify({ type: "result", result })}\n`);
  journal.close();
}

export async function childMessage(child: ChildProcess): Promise<Readonly<Record<string, unknown>>> {
  const output = child.stdout;
  if (output === null) throw new Error("fault child stdout is unavailable");
  return new Promise((resolve, reject) => {
    let buffered = "", errors = "";
    const onData = (chunk: Buffer) => {
      buffered += chunk.toString("utf8");
      const newline = buffered.indexOf("\n");
      if (newline === -1) return;
      cleanup();
      try { resolve(JSON.parse(buffered.slice(0, newline)) as Readonly<Record<string, unknown>>); }
      catch (error) { reject(error); }
    };
    const onErrorData = (chunk: Buffer) => { errors += chunk.toString("utf8"); };
    const onError = (error: Error) => { cleanup(); reject(error); };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(new Error(`fault child exited before evidence: code=${String(code)} signal=${String(signal)} ${errors}`));
    };
    const cleanup = () => {
      output.off("data", onData);
      child.stderr?.off("data", onErrorData);
      child.off("error", onError);
      child.off("exit", onExit);
    };
    output.on("data", onData);
    child.stderr?.on("data", onErrorData);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}

export async function childExit(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => child.once("exit", () => resolve()));
}

export async function crashChild(child: ChildProcess): Promise<void> {
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  await childExit(child);
}

export function startEventIngressChild(database: string, request: FaultEventRoute, failEventPrecommit = false): ChildProcess {
  const code = `import { runEventIngressFaultChild } from ${JSON.stringify(import.meta.url)}; await runEventIngressFaultChild();`;
  return spawn(process.execPath, ["--import=tsx", "--input-type=module", "--eval", code], {
    cwd: process.cwd(),
    env: { ...process.env, AXIS_NODE_EVENT_FAULT_CHILD: JSON.stringify({ database, request, failEventPrecommit }) },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

export async function runEventIngressFaultChild(): Promise<void> {
  const value = process.env.AXIS_NODE_EVENT_FAULT_CHILD;
  if (value === undefined) throw new Error("missing event fault child input");
  const input = JSON.parse(value) as { readonly database: string; readonly request: FaultEventRoute; readonly failEventPrecommit: boolean };
  const journal = await SqliteNodeJournal.open(input.database, { busyTimeoutMs: 0 });
  const ingressJournal = input.failEventPrecommit ? new Proxy(journal, {
    get(target, property) {
      if (property === "recordEvent") {
        return async (recordInput: Parameters<SqliteNodeJournal["recordEvent"]>[0]) => {
          const sourceEvent = "deliveryType" in recordInput.event ? recordInput.event.envelope : recordInput.event;
          const rejected = { ...sourceEvent, task: { ...sourceEvent.task, taskId: "injected-storage-mismatch" } } as RouterRunLifecycleEnvelope;
          try { await target.recordEvent({ ...recordInput, event: rejected }); }
          catch { throw new Error("injected precommit storage failure"); }
          throw new Error("injected precommit storage failure did not reject");
        };
      }
      const member = Reflect.get(target, property, target) as unknown;
      return typeof member === "function" ? member.bind(target) : member;
    },
  }) : journal;
  const ingress = await startEventIngress(ingressJournal, input.request);
  process.stdout.write(`${JSON.stringify({ type: "ready", url: ingress.url.origin })}\n`);
}

function eventWorker(request: FaultEventRoute, eventToken: string, workerId = request.target.identity.workerId): NodeRuntimeWorkerConfig {
  const identity = { ...request.target.identity, workerId };
  return {
    nodeRef: request.nodeRef,
    nodeId: identity.nodeId,
    target: { ...request.target, identity },
    workerKey: `key-${workerId}`,
    workerEndpoint: "http://127.0.0.1:4101",
    commandToken: `command-${eventToken}`,
    eventToken,
    readinessMode: "codex",
  };
}

export async function startEventIngress(journal: SqliteNodeJournal, request: FaultEventRoute): Promise<{ readonly url: URL; close(): Promise<void> }> {
  const ingress = new NodeEventIngress({ journal });
  const maintenance = {
    async status() {
      return nodeMaintenanceStatus({
        contractVersion: NODE_CONTRACT_VERSION,
        nodeRef: request.nodeRef,
        mode: "resume",
        idle: false,
        blockerCounts: { reservedCommands: 1, dispatchingCommands: 0, ambiguousCommands: 0, pendingNodeEvents: 0, quarantinedNodeEvents: 0, activeWorkerRuns: 0, pendingWorkerOutbox: 0, unknownWorkerEvidence: 0 },
      });
    },
    async setMode() { return this.status(); },
  };
  const server = createServer(nodeEventIngressHandler({
    configuration: { token: "node-token", workers: [eventWorker(request, "event-token-1"), eventWorker(request, "event-token-2", "worker-2")] },
    ingress,
    maintenance,
  }));
  const url = await listen(server);
  return { url, close: () => closeServer(server) };
}

export async function startDropResponseProxy(target: URL): Promise<{ readonly url: URL; readonly firstForwarded: Promise<void>; close(): Promise<void> }> {
  const forwarded = deferred<void>();
  let drop = true;
  const active = new Set<Promise<void>>();
  const server = createServer((request, response) => {
    const operation = (async () => {
      const body = await requestBody(request);
      const inner = await fetch(new URL(request.url ?? "/", target), {
        method: request.method ?? "POST",
        headers: { authorization: request.headers.authorization ?? "", "content-type": request.headers["content-type"] ?? "application/json" },
        body: JSON.stringify(body),
      });
      const bytes = Buffer.from(await inner.arrayBuffer());
      if (drop) {
        drop = false;
        forwarded.resolve();
        response.destroy();
        return;
      }
      response.writeHead(inner.status, { "content-type": inner.headers.get("content-type") ?? "application/json", "content-length": bytes.byteLength });
      response.end(bytes);
    })().catch((error) => { forwarded.reject(error); response.destroy(); });
    active.add(operation);
    void operation.finally(() => active.delete(operation)).catch(() => undefined);
  });
  const url = await listen(server);
  return {
    url,
    firstForwarded: forwarded.promise,
    async close() {
      server.closeAllConnections();
      await Promise.allSettled([...active]);
      await closeServer(server);
    },
  };
}

export class ManualClock {
  #value = Date.parse(faultInstant);
  now = (): string => new Date(this.#value).toISOString();
  advance(milliseconds: number): void { this.#value += milliseconds; }
}

export class ManualScheduler {
  #next = 0;
  readonly #timers = new Map<number, () => void>();
  schedule = (callback: () => void): number => { const id = ++this.#next; this.#timers.set(id, callback); return id; };
  cancel = (value: unknown): void => { if (typeof value === "number") this.#timers.delete(value); };
  fire(): void {
    const item = this.#timers.entries().next().value as [number, () => void] | undefined;
    if (item === undefined) throw new Error("no scheduled retry");
    this.#timers.delete(item[0]);
    item[1]();
  }
  get size(): number { return this.#timers.size; }
}

export async function eventually(predicate: () => Promise<boolean>, message: string): Promise<void> {
  const maxAttempts = 500;
  for (let attempt = 0; ; attempt++) {
    if (await predicate()) return;
    if (attempt >= maxAttempts) throw new Error(message);
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
}

function source(event: RouterRunLifecycleEnvelope) {
  return "deliveryType" in event ? event.envelope : event;
}

/** The command Router receipt, built from the posted JSON: it is not a validated event, so no `nodeEvent*` helper applies. */
function routerReceipt(event: RouterRunLifecycleEnvelope, nodeRef: WorkerVersionedReference, replayed: boolean) {
  return { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef, sourceMessageId: source(event).messageId, runId: source(event).run.runId, status: "projected", replayed };
}

export type RouterFaultMode = "success" | "drop-after-commit" | "intent-conflict";

export interface DurableRouter {
  readonly url: URL;
  queue(eventId: string, ...modes: RouterFaultMode[]): void;
  calls(): readonly string[];
  acceptedCount(eventId: string): number;
  close(): Promise<void>;
}

export async function startDurableRouter(directory: string): Promise<DurableRouter> {
  const database = new DatabaseSync(join(directory, "router.sqlite"));
  database.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE accepted(event_id TEXT PRIMARY KEY, recorded_at TEXT NOT NULL); CREATE TABLE calls(sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL);");
  const modes = new Map<string, RouterFaultMode[]>();
  const server = createServer((request, response) => {
    void (async () => {
      if (request.method !== "POST" || request.url !== "/v1/router/node-run-events" || request.headers.authorization !== "Bearer router-token") {
        send(response, 401, {});
        return;
      }
      const value = await requestBody(request) as { readonly envelope: RouterRunLifecycleEnvelope; readonly recordedAt: string; readonly nodeRef: WorkerVersionedReference };
      const eventId = source(value.envelope).messageId;
      database.prepare("INSERT INTO calls(event_id) VALUES (?)").run(eventId);
      const mode = modes.get(eventId)?.shift() ?? "success";
      if (mode === "intent-conflict") {
        send(response, 409, { error: { code: "axis.router.intent_conflict", message: "The Router event conflicts with an existing receipt." } });
        return;
      }
      const replayed = database.prepare("SELECT 1 FROM accepted WHERE event_id=?").get(eventId) !== undefined;
      if (!replayed) database.prepare("INSERT INTO accepted VALUES (?,?)").run(eventId, value.recordedAt);
      if (mode === "drop-after-commit") {
        response.destroy();
        return;
      }
      send(response, replayed ? 200 : 202, routerReceipt(value.envelope, value.nodeRef, replayed));
    })().catch(() => response.destroy());
  });
  const url = await listen(server);
  return {
    url,
    queue(eventId, ...queued) { modes.set(eventId, [...queued]); },
    calls() {
      return (database.prepare("SELECT event_id FROM calls ORDER BY sequence").all() as { event_id: string }[]).map(({ event_id }) => event_id);
    },
    acceptedCount(eventId) {
      return Number((database.prepare("SELECT COUNT(*) AS count FROM accepted WHERE event_id=?").get(eventId) as { count: number }).count);
    },
    async close() { await closeServer(server); database.close(); },
  };
}
