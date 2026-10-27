import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHmac, randomBytes, randomUUID } from "node:crypto";
import { access, chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { DatabaseSync } from "node:sqlite";

import {
  CODEX_EXECUTOR_CONTRACT_VERSION,
  deterministicElicitationReplyValue,
  EnvelopeCipher,
  Router,
  RouterHttpEventSink,
  SqliteTeamBindingStore,
  SqliteWorkerCommandJournal,
  WorkerCommandService,
  startWorkerHttpService,
  type CodexLiveBinding,
  type Executor,
  type ExecutorCancelRequest,
  type ExecutorCancelResult,
  type ExecutorContinueRequest,
  type ExecutorContinueResult,
  type ExecutorLifecycleEvent,
  type ExecutorRegistration,
  type ExecutorStartRequest,
  type ExecutorStartResult,
} from "../../src/index.js";
import { stringify } from "smol-toml";
import { writeConfigurationFixture } from "../support/configuration-fixture.js";
import { managedConfigurationReference } from "../../src/lifecycle/configuration.js";

import { makeState } from "../../src/lifecycle/transaction.js";
import { composeNodeRouter } from "../../src/relay/node-routing-composition.js";
import type { RelayRuntimeConfig } from "../../src/relay/runtime-config.js";
import { startRouterHttpServiceWithNodeEvents } from "../../src/router/http.js";
import type { LinearIngressLoopTickResult } from "../../src/relay/linear/composition.js";
import type { LinearChannelDatabasePaths } from "../../src/relay/linear/runtime-config.js";
import { nodeMaintenanceStatus, type NodeMaintenanceStatus, type NodeTarget } from "../../src/shared/router-node.js";
import { FakeLinearProvider, MemoryVault, currentFixture, masterKey, promptActivity } from "../helpers.js";
import { awaitWithinAttempts, realAttemptSleep } from "./attempt-budget.js";
import {
  installTestLinearInstallation,
  linearChannelDatabases,
  linearChannelSnapshot,
  startTestLinearChannel,
  type TestLinearChannel,
} from "./linear-channel.js";

const scope = { organizationId: "organization-fixture", linearTeamId: "team-fixture" } as const;
/**
 * The two Linear identities the shipped fixtures name: the installed App User
 * (what a structured `@Axis` mention node must carry in `attrs.id`) and the
 * human who creates and prompts a session. Both are already pinned inside
 * `test/fixtures/linear/*.json`; naming them here lets the comment-origin
 * helper below build a Comment that agrees with the envelope it accompanies.
 */
const APP_USER = "app-user-fixture";
const HUMAN_USER = "human-user-fixture";
const ready = {
  status: "ready",
  modelAvailability: "not_checked",
  profiles: {
    explorer: { availability: "available", attempt: "not_attempted" },
    architect: { availability: "available", attempt: "not_attempted" },
    reviewer: { availability: "available", attempt: "not_attempted" },
    worker: { availability: "available", attempt: "not_attempted" },
  },
  degradedReasons: [],
} as const;

export type MockWorkerSlot = "a" | "b";

interface WorkerChildInput {
  readonly workerId: string;
  readonly executorId: string;
  readonly nodeId: string;
  readonly commandToken: string;
  readonly eventToken: string;
  readonly eventEndpoint: string;
  readonly database: string;
  readonly workspaceRoot: string;
  readonly nativeSentinel: string;
}

interface WorkerControlRequest {
  readonly requestId: number;
  readonly action: "stats" | "emit";
  readonly ordinal?: number;
  readonly outcome?: "progressed" | "waiting_for_input" | "succeeded" | "cancelled";
}

export interface WorkerStats {
  readonly starts: number;
  readonly continues: number;
  readonly cancels: number;
  readonly bindings: number;
}

interface ControlledStream {
  readonly binding: CodexLiveBinding;
  readonly events: ExecutorLifecycleEvent<CodexLiveBinding>[];
  readonly waiters: (() => void)[];
  sequence: number;
  closed: boolean;
}

class ControlledExecutor implements Executor<CodexLiveBinding> {
  readonly contractVersion = CODEX_EXECUTOR_CONTRACT_VERSION;
  starts = 0;
  continues = 0;
  cancels = 0;
  readonly #workerId: string;
  readonly #nativeSentinel: string;
  readonly #streams: ControlledStream[] = [];

  constructor(workerId: string, nativeSentinel: string) {
    this.#workerId = workerId;
    this.#nativeSentinel = nativeSentinel;
  }

  async start(request: ExecutorStartRequest): Promise<ExecutorStartResult<CodexLiveBinding>> {
    this.starts += 1;
    const binding = this.#binding(this.#streams.length + 1);
    const stream = this.#stream(binding);
    this.#push(stream, "started");
    return {
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: request.requestId,
      operation: "start",
      status: "accepted",
      binding,
    };
  }

  async continue(request: ExecutorContinueRequest<CodexLiveBinding>): Promise<ExecutorContinueResult<CodexLiveBinding>> {
    this.continues += 1;
    const binding = this.#binding(this.#streams.length + 1);
    const stream = this.#stream(binding);
    this.#push(stream, "started");
    return {
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: request.requestId,
      operation: "continue",
      status: "accepted",
      binding,
    };
  }

  async cancel(request: ExecutorCancelRequest<CodexLiveBinding>): Promise<ExecutorCancelResult<CodexLiveBinding>> {
    this.cancels += 1;
    return {
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      requestId: request.requestId,
      operation: "cancel",
      status: "interrupt_requested",
      binding: request.binding,
    };
  }

  async *observe(binding: CodexLiveBinding): AsyncIterableIterator<ExecutorLifecycleEvent<CodexLiveBinding>> {
    const stream = this.#streams.find((candidate) => sameBinding(candidate.binding, binding));
    if (stream === undefined) return;
    for (;;) {
      const event = stream.events.shift();
      if (event !== undefined) {
        yield event;
        continue;
      }
      if (stream.closed) return;
      await new Promise<void>((resolve) => stream.waiters.push(resolve));
    }
  }

  emit(ordinal: number, outcome: Exclude<WorkerControlRequest["outcome"], undefined>): void {
    const stream = this.#streams[ordinal - 1];
    if (stream === undefined || stream.closed) throw new Error("unknown mock binding");
    this.#push(stream, outcome);
    if (outcome !== "progressed") stream.closed = true;
  }

  stats(): WorkerStats {
    return { starts: this.starts, continues: this.continues, cancels: this.cancels, bindings: this.#streams.length };
  }

  close(): void {
    for (const stream of this.#streams) {
      stream.closed = true;
      for (const wake of stream.waiters.splice(0)) wake();
    }
  }

  #binding(ordinal: number): CodexLiveBinding {
    return {
      runtimeKind: "codex",
      threadId: `native-${this.#nativeSentinel}-${this.#workerId}-${ordinal}`,
      turnId: `turn-${this.#nativeSentinel}-${this.#workerId}-${ordinal}`,
    };
  }

  #stream(binding: CodexLiveBinding): ControlledStream {
    const stream: ControlledStream = { binding, events: [], waiters: [], sequence: 0, closed: false };
    this.#streams.push(stream);
    return stream;
  }

  #push(stream: ControlledStream, outcome: "started" | Exclude<WorkerControlRequest["outcome"], undefined>): void {
    const common = {
      contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
      binding: stream.binding,
      sequence: ++stream.sequence,
    } as const;
    const event: ExecutorLifecycleEvent<CodexLiveBinding> = outcome === "progressed"
      ? { ...common, outcome, progress: { kind: "generic" } }
      : outcome === "waiting_for_input"
        ? {
          ...common,
          outcome,
          elicitation: {
            contractVersion: "1.0.0-fixed-choice",
            decisionId: `axis-decision-${"a".repeat(48)}`,
            sourceEventId: `axis-elicitation-${"b".repeat(48)}`,
            recommendedOptionId: `axis-option-${"c".repeat(48)}`,
            header: "Mock decision",
            question: "Continue the deterministic scenario?",
            options: [
              {
                optionId: `axis-option-${"c".repeat(48)}`,
                replyValue: deterministicElicitationReplyValue(`axis-decision-${"a".repeat(48)}`, `axis-option-${"c".repeat(48)}`),
                label: "Continue",
                description: "Continue the mock-only acceptance scenario.",
              },
              {
                optionId: `axis-option-${"e".repeat(48)}`,
                replyValue: deterministicElicitationReplyValue(`axis-decision-${"a".repeat(48)}`, `axis-option-${"e".repeat(48)}`),
                label: "Stop",
                description: "Stop the mock-only acceptance scenario.",
              },
            ],
          },
        }
        : { ...common, outcome };
    stream.events.push(event);
    for (const wake of stream.waiters.splice(0)) wake();
  }
}

function sameBinding(left: CodexLiveBinding, right: CodexLiveBinding): boolean {
  return left.runtimeKind === right.runtimeKind && left.threadId === right.threadId && left.turnId === right.turnId;
}

function registration(executorId: string, executor: ControlledExecutor): ExecutorRegistration<CodexLiveBinding> {
  return {
    runtimeKind: "codex",
    executorId,
    methodRef: { version: "v1", method: "codex.execute" },
    supportedOperations: ["start", "continue", "cancel", "observe"],
    adapter: executor,
    validateBinding(value): value is CodexLiveBinding {
      const candidate = value as Partial<CodexLiveBinding> | null;
      return candidate !== null
        && candidate.runtimeKind === "codex"
        && typeof candidate.threadId === "string"
        && candidate.threadId.length > 0
        && typeof candidate.turnId === "string"
        && candidate.turnId.length > 0;
    },
    bindingsEqual: sameBinding,
    methodRefEquals: (left, right) => left.version === right.version && left.method === right.method,
    projectLifecycle: ({ event, runId }) => event.outcome === "progressed"
      ? { kind: "progress", completed: 1, total: 2, safeMessage: "Mock execution progressed." }
      : event.outcome === "waiting_for_input"
        ? { kind: "waiting_for_input", elicitation: event.elicitation }
        : event.outcome === "succeeded"
          // Provide a terminal response body so success exercises run_agent_response
          // delivery through Linear. The mock answer contains no native identity, local
          // path or runtime secret, matching the harness's public-projection
          // assertions.
          ? { kind: "succeeded", resultRef: `axis-evidence://${runId}/mock-terminal`, response: { sourceEventId: `mock-final-${runId}`, projectionStatus: "projected" as const, body: "Mock execution completed.", truncated: false, truncations: [] } }
          : event.outcome === "cancelled"
            ? { kind: "failed", code: "execution_cancelled", safeMessage: "Execution was cancelled." }
            : { kind: "failed", code: "mock_execution_failed", safeMessage: "Mock execution failed." },
  };
}

function sendControl(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

/** Child-only entrypoint. It composes the real Worker service around a controlled native adapter. */
export async function runNodeMockWorkerChild(): Promise<void> {
  const raw = process.env.AXIS_NODE_MOCK_WORKER_INPUT;
  if (raw === undefined) throw new Error("missing worker child input");
  const input = JSON.parse(raw) as WorkerChildInput;
  const executor = new ControlledExecutor(input.workerId, input.nativeSentinel);
  const journal = new SqliteWorkerCommandJournal(input.database);
  // Preserve production WorkerCommandService ordering: run.accepted delivery is
  // requested before dispatch returns. The Node may receive this event before accepting
  // its dispatch receipt, so the harness must exercise that race.
  const commandService = new WorkerCommandService({
    configuration: {
      bearerToken: input.commandToken,
      workerId: input.workerId,
      runtimeKind: "codex",
      executorId: input.executorId,
      nodeId: input.nodeId,
      runtimeProfile: { ref: `runtime-${input.workerId}`, version: "v1" },
      methodRef: { version: "v1", method: "codex.execute" },
      workspaces: [{ organizationId: "organization-fixture", workspaceRoot: input.workspaceRoot }],
    },
    registrations: [registration(input.executorId, executor)],
    eventSink: new RouterHttpEventSink({ endpoint: input.eventEndpoint, bearerToken: input.eventToken }),
    journal,
  });
  const http = await startWorkerHttpService({ commandService, readiness: () => ready });
  sendControl({ type: "ready", url: http.url.origin });

  let closing = false;
  const close = async (): Promise<void> => {
    if (closing) return;
    closing = true;
    commandService.close();
    executor.close();
    await http.close();
    journal.close();
  };
  const shutdown = () => { void close().then(() => process.exit(0), () => process.exit(1)); };
  process.once("SIGTERM", shutdown);
  process.once("SIGINT", shutdown);

  const lines = createInterface({ input: process.stdin });
  lines.on("line", (line) => {
    void (async () => {
      let requestId = 0;
      try {
        const request = JSON.parse(line) as WorkerControlRequest;
        requestId = request.requestId;
        if (request.action === "stats") {
          sendControl({ type: "response", requestId, ok: true, value: executor.stats() });
          return;
        }
        if (request.action !== "emit" || request.ordinal === undefined || request.outcome === undefined) throw new Error("malformed control request");
        executor.emit(request.ordinal, request.outcome);
        sendControl({ type: "response", requestId, ok: true, value: executor.stats() });
      } catch {
        sendControl({ type: "response", requestId, ok: false, code: "mock_worker_control_failed" });
      }
    })();
  });
}

class WorkerProcess {
  readonly child: ChildProcessWithoutNullStreams;
  readonly output: string[] = [];
  readonly errors: string[] = [];
  readonly #pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();
  #nextRequest = 0;
  #ready: Promise<URL>;
  #stopped = false;

  constructor(input: WorkerChildInput) {
    const code = `import { runNodeMockWorkerChild } from ${JSON.stringify(import.meta.url)}; await runNodeMockWorkerChild();`;
    this.child = spawn(process.execPath, ["--import=tsx", "--input-type=module", "--eval", code], {
      cwd: process.cwd(),
      env: { ...process.env, AXIS_NODE_MOCK_WORKER_INPUT: JSON.stringify(input) },
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.child.stderr.on("data", (chunk: Buffer) => this.errors.push(chunk.toString("utf8")));
    this.#ready = new Promise<URL>((resolve, reject) => {
      const lines = createInterface({ input: this.child.stdout });
      lines.on("line", (line) => {
        this.output.push(line);
        try {
          const value = JSON.parse(line) as {
            readonly type?: string;
            readonly url?: string;
            readonly requestId?: number;
            readonly ok?: boolean;
            readonly value?: unknown;
          };
          if (value.type === "ready" && typeof value.url === "string") {
            resolve(new URL(value.url));
            return;
          }
          if (value.type === "response" && typeof value.requestId === "number") {
            const pending = this.#pending.get(value.requestId);
            if (pending === undefined) return;
            this.#pending.delete(value.requestId);
            if (value.ok === true) pending.resolve(value.value);
            else pending.reject(new Error("mock worker control failed"));
          }
        } catch {
          reject(new Error("mock worker emitted malformed control output"));
        }
      });
      this.child.once("error", () => reject(new Error("mock worker process failed to start")));
      this.child.once("exit", () => {
        if (!this.#stopped) reject(new Error("mock worker exited before readiness"));
        for (const pending of this.#pending.values()) pending.reject(new Error("mock worker exited during control request"));
        this.#pending.clear();
      });
    });
  }

  ready(): Promise<URL> { return this.#ready; }

  async stats(): Promise<WorkerStats> {
    return await this.#request({ action: "stats" }) as WorkerStats;
  }

  async emit(ordinal: number, outcome: Exclude<WorkerControlRequest["outcome"], undefined>): Promise<WorkerStats> {
    return await this.#request({ action: "emit", ordinal, outcome }) as WorkerStats;
  }

  async stop(signal: NodeJS.Signals = "SIGTERM"): Promise<void> {
    if (this.#stopped) return;
    this.#stopped = true;
    if (this.child.exitCode === null && this.child.signalCode === null) this.child.kill(signal);
    if (!await childExitWithin(this.child, 5_000) && this.child.exitCode === null && this.child.signalCode === null) {
      this.child.kill("SIGKILL");
      await childExit(this.child);
    }
  }

  async #request(input: Omit<WorkerControlRequest, "requestId">): Promise<unknown> {
    if (this.#stopped) throw new Error("mock worker is stopped");
    const requestId = ++this.#nextRequest;
    const result = new Promise<unknown>((resolve, reject) => this.#pending.set(requestId, { resolve, reject }));
    this.child.stdin.write(`${JSON.stringify({ ...input, requestId })}\n`);
    return result;
  }
}

interface NodeProcess {
  readonly child: ChildProcessWithoutNullStreams;
  readonly output: string[];
  readonly errors: string[];
  readonly ready: Promise<void>;
}

function startNodeProcess(filename: string): NodeProcess {
  const child = spawn(process.execPath, ["--import=tsx", "src/bin/node.ts", "--config", filename, "--node", "mock-node"], {
    cwd: process.cwd(),
    env: process.env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  const output: string[] = [], errors: string[] = [];
  child.stderr.on("data", (chunk: Buffer) => errors.push(chunk.toString("utf8")));
  const readyPromise = new Promise<void>((resolve, reject) => {
    const lines = createInterface({ input: child.stdout });
    lines.on("line", (line) => { output.push(line); if (line === "Axis Node ready") resolve(); });
    child.once("error", () => reject(new Error("Node process failed to start")));
    child.once("exit", () => reject(new Error("Node process exited before readiness")));
  });
  return { child, output, errors, ready: readyPromise };
}

async function childExit(child: ChildProcessWithoutNullStreams): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => child.once("exit", () => resolve()));
}

async function childExitWithin(child: ChildProcessWithoutNullStreams, timeoutMs: number): Promise<boolean> {
  if (child.exitCode !== null || child.signalCode !== null) return true;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      childExit(child).then(() => true),
      new Promise<boolean>((resolve) => { timer = setTimeout(() => resolve(false), timeoutMs); }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function stopNode(node: NodeProcess, signal: NodeJS.Signals): Promise<void> {
  if (node.child.exitCode === null && node.child.signalCode === null) node.child.kill(signal);
  if (!await childExitWithin(node.child, 5_000) && node.child.exitCode === null && node.child.signalCode === null) {
    node.child.kill("SIGKILL");
    await childExit(node.child);
  }
}

async function listen(server: Server): Promise<URL> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("mock E2E listener did not bind");
  return new URL(`http://127.0.0.1:${address.port}`);
}

async function closeServer(server: Server): Promise<void> {
  if (!server.listening) return;
  server.closeAllConnections();
  await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
}

async function availablePort(): Promise<number> {
  const server = createServer();
  const url = await listen(server);
  await closeServer(server);
  return Number(url.port);
}

async function body(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return Buffer.concat(chunks);
}

function send(response: ServerResponse, status: number, headers: Headers, payload: Buffer): void {
  response.writeHead(status, {
    "content-type": headers.get("content-type") ?? "application/json; charset=utf-8",
    "content-length": String(payload.length),
    "cache-control": "no-store",
  });
  response.end(payload);
}

class ReceiptLossProxy {
  readonly server: Server;
  readonly url: Promise<URL>;
  forwarded = 0;
  dropped = 0;
  replayed = 0;
  #dropNext = false;
  #dropWaiters: (() => void)[] = [];

  constructor(upstream: URL) {
    this.server = createServer((request, response) => {
      void (async () => {
        const payload = await body(request);
        const upstreamResponse = await fetch(new URL(request.url ?? "/", upstream), {
          method: request.method ?? "POST",
          headers: {
            authorization: String(request.headers.authorization ?? ""),
            "content-type": String(request.headers["content-type"] ?? "application/json"),
          },
          body: payload.toString("utf8"),
        });
        const upstreamBody = Buffer.from(await upstreamResponse.arrayBuffer());
        this.forwarded += 1;
        if (upstreamResponse.status === 200) this.replayed += 1;
        if (this.#dropNext) {
          this.#dropNext = false;
          this.dropped += 1;
          for (const wake of this.#dropWaiters.splice(0)) wake();
          response.destroy();
          return;
        }
        send(response, upstreamResponse.status, upstreamResponse.headers, upstreamBody);
      })().catch(() => response.destroy());
    });
    this.url = listen(this.server);
  }

  arm(): void {
    if (this.#dropNext) throw new Error("receipt loss proxy is already armed");
    this.#dropNext = true;
  }

  async waitForDrop(): Promise<void> {
    if (this.dropped > 0) return;
    await new Promise<void>((resolve) => this.#dropWaiters.push(resolve));
  }

  close(): Promise<void> { return closeServer(this.server); }
}

function target(slot: MockWorkerSlot): NodeTarget {
  return {
    endpointRef: { ref: `mock-worker-endpoint-${slot}`, version: "v1" },
    identity: {
      nodeId: "mock-node",
      workerId: `mock-worker-${slot}`,
      executorKind: "codex",
      runtimeProfile: { ref: `runtime-mock-worker-${slot}`, version: "v1" },
    },
    sessionBindingExecutorId: `mock-executor-${slot}`,
  };
}

async function writeWorkerHostState(input: {
  readonly root: string;
  readonly slot: MockWorkerSlot;
  readonly configurationFile: string;
}): Promise<string> {
  const hostRoot = join(input.root, `host-${input.slot}`);
  await mkdir(hostRoot, { recursive: true, mode: 0o700 });
  const filename = join(hostRoot, "axis.toml");
  await writeFile(filename, await readFile(input.configurationFile), { mode: 0o600 });
  const configuration = managedConfigurationReference(filename, "worker", "mock-node", target(input.slot).identity.workerId);
  const state = makeState({
    request: {
      role: "worker",
      workerRuntime: "codex",
      configuration,
      serviceLabel: `com.axis.mock-worker-${input.slot}`,
    },
    managedPlist: join(hostRoot, `worker-${input.slot}.plist`),
    release: {
      packagePath: join(hostRoot, "release", "axis"),
      version: "0.14.9",
      manifestDigest: `sha256:${(input.slot === "a" ? "a" : "b").repeat(64)}`,
    },
    revision: 1,
  });
  await writeFile(join(hostRoot, "axis.host.v1.json"), JSON.stringify(state), { mode: 0o600 });
  await chmod(join(hostRoot, "axis.host.v1.json"), 0o600);
  return hostRoot;
}

function bindingProfile(slot: MockWorkerSlot) {
  const selected = target(slot);
  return {
    ...selected.identity,
    endpointRef: selected.endpointRef,
    workspacePolicyRef: { ref: `workspace-mock-worker-${slot}`, version: "v1" },
  };
}

export interface DatabaseSnapshot {
  /**
   * Linear channel state read across the four databases composeLinearChannel opens. The
   * snapshot counts are defined by ./linear-channel.ts.
   */
  readonly linear: {
    readonly accepted: number;
    readonly processing: number;
    readonly unsettledCommands: number;
    readonly outbox: number;
    readonly unsettledOutbox: number;
  };
  readonly router: {
    readonly commands: number;
    readonly unsettledCommands: number;
    readonly runs: number;
    readonly activeRuns: number;
    readonly receipts: number;
  };
  readonly node: {
    readonly commands: number;
    readonly ambiguousCommands: number;
    readonly routes: number;
    readonly events: number;
    readonly pendingEvents: number;
    readonly failedEventAttempts: number;
    readonly deliveredEvents: number;
  };
  readonly workers: Readonly<Record<MockWorkerSlot, {
    readonly commands: number;
    readonly runs: number;
    readonly activeRuns: number;
    readonly outbox: number;
    readonly pendingOutbox: number;
  }>>;
}

function row<T>(database: DatabaseSync, sql: string): T {
  return database.prepare(sql).get() as T;
}

function withDatabase<T>(filename: string, read: (database: DatabaseSync) => T): T {
  const database = new DatabaseSync(filename, { readOnly: true });
  try {
    database.exec("PRAGMA busy_timeout=5000; PRAGMA query_only=ON;");
    return read(database);
  } finally {
    database.close();
  }
}

export function databaseIntegrity(filename: string): string {
  return withDatabase(filename, (database) => String(row<{ readonly integrity_check: string }>(database, "PRAGMA integrity_check").integrity_check));
}

export interface GuardedRoutingResult {
  readonly changed: boolean;
  /** How many guarded routing changes have been applied so far. */
  readonly changes: number;
  readonly maintenance: NodeMaintenanceStatus;
}

/**
 * The prompt signals Linear's parser accepts on a `prompted` AgentSession
 * event. `stop` has its own harness method because it is the one signal that
 * does not carry work forward; the other three are ordinary continuation input
 * and differ only in what the Linear UI did to produce them.
 */
export type LinearPromptSignal = "continue" | "auth" | "select";

export interface NodeMockE2EHarness {
  readonly root: string;
  readonly paths: {
    /** All four Linear channel databases, so an integrity sweep covers every one of them. */
    readonly linear: LinearChannelDatabasePaths;
    readonly router: string;
    readonly node: string;
    readonly workers: Readonly<Record<MockWorkerSlot, string>>;
  };
  readonly targets: Readonly<Record<MockWorkerSlot, NodeTarget>>;
  readonly provider: FakeLinearProvider;
  readonly workers: Readonly<Record<MockWorkerSlot, WorkerProcess>>;
  readonly proxy: ReceiptLossProxy;
  readonly tokens: readonly string[];
  readonly nativeSentinel: string;
  readonly childOutput: string[];
  postCreated(suffix: string, replayDeliveryId?: string): Promise<{ readonly deliveryId: string; readonly duplicate: boolean }>;
  postPrompt(suffix: string, body: string, signal?: LinearPromptSignal): Promise<void>;
  postStop(suffix: string): Promise<void>;
  /**
   * The second of Linear's two normal ways to open an AgentSession: a human
   * writes a comment carrying a structured `@Axis` mention, and Linear sends a
   * `created` event naming that comment. The Relay reads the exact Comment
   * through the provider, proves the mention, and builds the start instruction
   * from the comment -- not from the envelope's `promptContext`.
   *
   * Registers the matching exact Comment and exact Issue before posting, for
   * the same reason `postCreated` registers an Issue: each mock delivery owns
   * its own identities, and a shared fixture fallback would let a wrong read
   * still look like a right one.
   */
  postCommentCreated(suffix: string, instruction?: string): Promise<{ readonly deliveryId: string; readonly duplicate: boolean }>;
  /**
   * A mention REPLY under the thread `postCommentCreated` opened: the ordinary
   * way a human takes a second turn in a comment-started session.
   *
   * It is a materially different shape from `postPrompt`, not a variation of
   * it. The envelope names both the reply comment (`agentActivity.sourceCommentId`)
   * and the captured thread root (`agentSession.commentId`), and the Relay
   * checks the reply's `parentId` against that root before it will continue --
   * so a direct prompt and a comment prompt do not share a provenance path.
   */
  postCommentPrompt(suffix: string, instruction: string): Promise<{ readonly deliveryId: string; readonly duplicate: boolean }>;
  /**
   * Any signed Linear envelope over the real HTTP entry.
   *
   * `postCreated`/`postPrompt`/`postStop` cover the AgentSession envelopes that
   * carry work. This is how the rest of Linear's catalog reaches the same real
   * ingress: the lifecycle events that have no session to answer on
   * (`PermissionChange`, `OAuthApp.revoked`), the Developer Preview shapes the
   * parser reports as `Unsupported`, and the official data-change envelopes.
   * The body is signed and posted exactly as the others are, so a caller
   * asserting "no dispatch" is asserting it about a delivery Axis genuinely
   * accepted and verified, not one it never saw.
   */
  postWebhook(payload: Record<string, unknown>, options?: { readonly deliveryId?: string; readonly event?: string }): Promise<{ readonly deliveryId: string; readonly duplicate: boolean }>;
  /**
   * One ingress, observe and outbox tick round. Return the ingress result so callers
   * can distinguish a processed refusal from a delivery that has not been processed.
   * Callers repeat rounds when they need a drain.
   */
  pump(): Promise<LinearIngressLoopTickResult>;
  maintenance(mode?: "drain" | "resume"): Promise<NodeMaintenanceStatus>;
  readiness(): Promise<Readonly<Record<string, unknown>>>;
  /**
   * Drain the Node and require both Node and upstream channel settlement before
   * restarting with the requested default Worker key. Worker selection belongs to the
   * Node.
   */
  guardedDefaultWorker(slot: MockWorkerSlot): Promise<GuardedRoutingResult>;
  snapshot(): DatabaseSnapshot;
  currentRouting(): { readonly changes: number; readonly defaultWorkerKey: string };
  restartNode(signal?: NodeJS.Signals): Promise<void>;
  stopNodeForInspection(): Promise<void>;
  close(): Promise<void>;
}

export async function createNodeMockE2EHarness(): Promise<NodeMockE2EHarness> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "axis-node-mock-e2e-")));
  const paths = {
    linear: linearChannelDatabases(root),
    router: join(root, "router.sqlite"),
    node: join(root, "node-mock-node.sqlite"),
    workers: { a: join(root, "worker-mock-worker-a.sqlite"), b: join(root, "worker-mock-worker-b.sqlite") },
  } as const;
  const tokenSet = {
    signingSecret: randomBytes(24).toString("hex"),
    operatorToken: randomBytes(24).toString("hex"),
    nodeToken: randomBytes(32).toString("base64"),
    workerAToken: randomBytes(32).toString("base64"),
    workerAEventToken: randomBytes(32).toString("base64"),
    workerBToken: randomBytes(32).toString("base64"),
    workerBEventToken: randomBytes(32).toString("base64"),
    nodeEventToken: randomBytes(32).toString("base64"),
  } as const;
  const { signingSecret, operatorToken, nodeToken, workerAToken, workerAEventToken, workerBToken, workerBEventToken, nodeEventToken } = tokenSet;
  const allTokens = Object.values(tokenSet);
  const nativeSentinel = randomBytes(18).toString("hex");
  const nodePort = await availablePort(), nodeEventPort = await availablePort();
  const nodeUrl = new URL(`http://127.0.0.1:${nodePort}`), nodeEventUrl = new URL(`http://127.0.0.1:${nodeEventPort}`);
  const workers: Readonly<Record<MockWorkerSlot, WorkerProcess>> = {
    a: new WorkerProcess({
      workerId: target("a").identity.workerId,
      executorId: target("a").sessionBindingExecutorId,
      nodeId: target("a").identity.nodeId,
      commandToken: workerAToken,
      eventToken: workerAEventToken,
      eventEndpoint: nodeEventUrl.origin,
      database: paths.workers.a,
      workspaceRoot: root,
      nativeSentinel,
    }),
    b: new WorkerProcess({
      workerId: target("b").identity.workerId,
      executorId: target("b").sessionBindingExecutorId,
      nodeId: target("b").identity.nodeId,
      commandToken: workerBToken,
      eventToken: workerBEventToken,
      eventEndpoint: nodeEventUrl.origin,
      database: paths.workers.b,
      workspaceRoot: root,
      nativeSentinel,
    }),
  };
  let linear: TestLinearChannel | undefined;
  let bindings: SqliteTeamBindingStore | undefined;
  let router: Router | undefined;
  let routerHttp: Awaited<ReturnType<typeof startRouterHttpServiceWithNodeEvents>> | undefined;
  let proxy: ReceiptLossProxy | undefined;
  let node: NodeProcess | undefined;
  let stoppedNodeSnapshot: DatabaseSnapshot["node"] = {
    commands: -1,
    ambiguousCommands: -1,
    routes: -1,
    events: -1,
    pendingEvents: -1,
    failedEventAttempts: -1,
    deliveredEvents: -1,
  };
  let closed = false;
  const childOutput: string[] = [];

  try {
    const workerUrls = { a: await workers.a.ready(), b: await workers.b.ready() } as const;


    bindings = new SqliteTeamBindingStore({ filename: paths.router });
    bindings.configure({
      scope,
      expectedRevision: 0,
      profile: bindingProfile("a"),
      audit: { operatorRef: "mock-operator", source: "test", requestRef: "initial-binding", authorizationRef: "mock-authorization" },
    });
    // Build the Node command route through the production Router composition.
    const mockNodeRef = { ref: "mock-node", version: "v1" } as const;
    const runtime: RelayRuntimeConfig = {
      routerDatabase: paths.router, routerHost: "127.0.0.1", routerPort: 0,
      nodeEndpoints: [{ nodeRef: mockNodeRef, nodeId: "mock-node", endpoint: nodeUrl.origin, bearerToken: nodeToken }],
      nodeTargets: (["a", "b"] as const).map((slot) => ({
        endpointRef: target(slot).endpointRef, nodeRef: mockNodeRef,
        identity: target(slot).identity, sessionBindingExecutorId: target(slot).sessionBindingExecutorId,
      })),
      defaultExcludedTeamIds: [],
    };
    router = composeNodeRouter({ runtime }, { teamBindings: bindings });
    routerHttp = await startRouterHttpServiceWithNodeEvents({
      router,
      nodeEventBindings: [{
        bearerToken: nodeEventToken,
        authenticatedNode: { nodeRef: mockNodeRef, nodeId: "mock-node" },
        allowedOrigins: [target("a").identity, target("b").identity],
      }],
    });
    proxy = new ReceiptLossProxy(routerHttp.url);
    const proxyUrl = await proxy.url;
    const hostRoots = { a: join(root, "host-a"), b: join(root, "host-b") };
    const nodeDocument = {
      schemaVersion: 1, dataDir: root,
      router: { port: Number(proxyUrl.port), url: proxyUrl.origin },
      nodes: [{ id: "mock-node", port: nodePort, eventPort: nodeEventPort, defaultWorker: "mock-worker-a",
        workerDispatchTimeoutMs: 2000, routerEventTimeoutMs: 2000, workerReadinessTimeoutMs: 500,
        eventRetryInitialDelayMs: 5000, eventRetryMaxDelayMs: 5000, eventMaxConcurrentRuns: 2,
        workers: (["a", "b"] as const).map(slot => ({
          id: target(slot).identity.workerId, kind: "codex", port: Number(workerUrls[slot].port), labels: [],
          endpointRef: target(slot).endpointRef.ref, executorId: target(slot).sessionBindingExecutorId,
          runtimeProfile: target(slot).identity.runtimeProfile, hostRoot: hostRoots[slot],
          workspaces: [{ organizationId: scope.organizationId, workspaceRoot: root }],
        })),
      }],
    };
    const deployment = await writeConfigurationFixture(root, nodeDocument);
    for (const [name, token] of Object.entries({
      "node-mock-node-command": nodeToken, "node-mock-node-router-events": nodeEventToken,
      "worker-mock-worker-a-command": workerAToken, "worker-mock-worker-a-events": workerAEventToken,
      "worker-mock-worker-b-command": workerBToken, "worker-mock-worker-b-events": workerBEventToken,
    })) await writeFile(join(root, "secrets", `${name}.key`), `${token}\n`, { mode: 0o600 });
    await writeWorkerHostState({ root, slot: "a", configurationFile: deployment.filename });
    await writeWorkerHostState({ root, slot: "b", configurationFile: deployment.filename });
    let defaultWorkerKey = "mock-worker-a";
    let routingChanges = 0;
    node = startNodeProcess(deployment.filename);
    await node.ready;
    childOutput.push(...node.output, ...node.errors);

    const provider = new FakeLinearProvider();
    const vault = new MemoryVault();
    // Send signed webhooks through the real startRelayHttpService entry backed by
    // composeLinearChannel. pump() drives the three production ticks to exercise
    // Node/Worker multiplexing, routing changes, affinity and receipt-loss recovery.
    linear = await startTestLinearChannel({
      databases: paths.linear,
      router,
      // Use the channel's Node command port for default routing.
      provider,
      vault,
      cipher: new EnvelopeCipher(masterKey()),
      signingSecret,
      operatorToken,
      teamIds: [scope.linearTeamId],
    });
    await installTestLinearInstallation({
      channel: linear.channel,
      vault,
      organizationId: scope.organizationId,
      appUserId: "app-user-fixture",
      teamIds: [scope.linearTeamId],
    });
    const channel = linear;

    const postPayload = async (payload: Record<string, unknown>, deliveryId: string = randomUUID(), event?: string) => {
      const accepted = await channel.post(payload, { deliveryId, ...(event === undefined ? {} : { event }) });
      if (accepted.status !== 200) throw new Error(`mock Linear webhook POST answered ${accepted.status}`);
      const body = accepted.body as { readonly accepted?: boolean; readonly duplicate?: boolean };
      if (body.accepted !== true) throw new Error("mock Linear webhook POST was not accepted");
      return { duplicate: body.duplicate === true };
    };

    const payloadFor = (fixtureName: string, suffix: string): Record<string, unknown> => {
      const payload = currentFixture(fixtureName);
      const agentSession = payload.agentSession as Record<string, unknown>;
      const issue = agentSession.issue as Record<string, unknown>;
      payload.agentSession = { ...agentSession, id: `agent-session-${suffix}`, issue: { ...issue, id: `issue-${suffix}` } };
      return payload;
    };
    const createdPayloads = new Map<string, Record<string, unknown>>();
    /** Per-session reply counter, so each mention reply owns a distinct source Comment. */
    const commentReplies = new Map<string, number>();

    const inspectStoppedNode = (): DatabaseSnapshot["node"] => withDatabase(paths.node, (database) => ({
      commands: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM commands").count,
      ambiguousCommands: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM commands WHERE state='ambiguous'").count,
      routes: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM routes").count,
      events: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM event_outbox").count,
      pendingEvents: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM event_outbox WHERE state='pending'").count,
      failedEventAttempts: row<{ count: number }>(database, "SELECT COALESCE(SUM(delivery_attempt_count),0) AS count FROM event_outbox").count,
      deliveredEvents: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM event_outbox WHERE state='delivered'").count,
    }));
    const snapshot = (): DatabaseSnapshot => {
      const channelState = linearChannelSnapshot(paths.linear);
      const linearState = {
        accepted: channelState.acceptedDeliveries,
        processing: channelState.processingDeliveries,
        // Count outstanding commands once. pendingConversationCommands mirrors
        // unsettledCommands: the run-chain CHECK constraint, integrity sweep and
        // transactional writes enforce agreement. Summing them would count each pending
        // command twice.
        unsettledCommands: channelState.unsettledCommands,
        outbox: channelState.outbox,
        unsettledOutbox: channelState.unsettledOutbox,
      };
      const routerState = withDatabase(paths.router, (database) => ({
        // Read command state from the Node command journal.
        commands: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM router_node_command_state").count,
        unsettledCommands: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM router_node_command_state WHERE state NOT IN ('accepted','rejected')").count,
        runs: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM router_node_runs").count,
        activeRuns: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM router_runs WHERE state IN ('pending','running')").count,
        receipts: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM router_receipts").count,
      }));
      const workerState = Object.fromEntries((["a", "b"] as const).map((slot) => [slot, withDatabase(paths.workers[slot], (database) => ({
        commands: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM worker_commands").count,
        runs: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM worker_runs").count,
        activeRuns: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM worker_runs WHERE state IN ('reserved','binding','accepted','running','observational')").count,
        outbox: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM worker_outbox").count,
        pendingOutbox: row<{ count: number }>(database, "SELECT COUNT(*) AS count FROM worker_outbox WHERE delivered_at IS NULL").count,
      }))])) as unknown as DatabaseSnapshot["workers"];
      return { linear: linearState, router: routerState, node: stoppedNodeSnapshot, workers: workerState };
    };

    const requestNode = async (path: string, options: RequestInit = {}): Promise<Response> => fetch(new URL(path, path.includes("maintenance") ? nodeEventUrl : nodeUrl), {
      ...options,
      headers: { authorization: `Bearer ${nodeToken}`, ...(options.headers ?? {}) },
    });

    const maintenance = async (mode?: "drain" | "resume"): Promise<NodeMaintenanceStatus> => {
      const response = await requestNode("/v1/node/maintenance", mode === undefined ? {} : {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ contractVersion: "1.0.0", mode }),
      });
      if (response.status !== 200) throw new Error("mock Node maintenance request failed");
      // Through the validator every real consumer uses (`axis upgrade` among them), never a bare cast.
      return nodeMaintenanceStatus(await response.json());
    };

    const guardedDefaultWorker = async (slot: MockWorkerSlot): Promise<GuardedRoutingResult> => {
      const maintenanceState = await maintenance("drain");
      const state = snapshot();
      const settled = state.linear.accepted === 0
        && state.linear.processing === 0
        && state.linear.unsettledCommands === 0
        && state.linear.unsettledOutbox === 0
        && state.router.unsettledCommands === 0
        && state.router.activeRuns === 0;
      if (!maintenanceState.idle || !settled) return { changed: false, changes: routingChanges, maintenance: maintenanceState };
      if (node === undefined) throw new Error("mock Node is unavailable");
      await stopNode(node, "SIGTERM");
      childOutput.push(...node.output, ...node.errors);
      defaultWorkerKey = `mock-worker-${slot}`;
      await writeFile(deployment.filename, stringify({ ...nodeDocument, nodes: [{ ...nodeDocument.nodes[0], defaultWorker: defaultWorkerKey }] }), { mode: 0o600 });
      node = startNodeProcess(deployment.filename);
      await node.ready;
      routingChanges += 1;
      return { changed: true, changes: routingChanges, maintenance: maintenanceState };
    };

    const harness: NodeMockE2EHarness = {
      root,
      paths,
      targets: { a: target("a"), b: target("b") },
      provider,
      workers,
      proxy,
      tokens: allTokens,
      nativeSentinel,
      childOutput,
      async postCreated(suffix, replayDeliveryId) {
        const deliveryId = replayDeliveryId ?? randomUUID();
        const payload = createdPayloads.get(deliveryId) ?? payloadFor("agent-session-created.json", suffix);
        createdPayloads.set(deliveryId, payload);
        // Created-session instructions require an authenticated exact Issue read.
        // Register each delivery's matching immutable Issue snapshot so an incorrect
        // identity cannot pass through a shared fallback.
        const issueId = ((payload.agentSession as Record<string, unknown>).issue as { readonly id: string }).id;
        provider.exactIssues.set(issueId, { ...provider.exactIssue, id: issueId });
        const result = await postPayload(payload, deliveryId);
        return { deliveryId, duplicate: result.duplicate };
      },
      async postPrompt(suffix, content, signal = "continue") {
        const activityId = `mock-prompt-${suffix}-${randomUUID()}`;
        provider.exactActivity = { agentSessionId: `agent-session-${suffix}`, activity: promptActivity(activityId, content) };
        const payload = payloadFor("agent-session-prompted.json", suffix);
        // The signal rides on the ENVELOPE only. `auth` and `select` describe
        // what the Linear UI did to produce the turn; the executable input
        // stays the exact provider Activity above, which is why the same
        // `provider.exactActivity` serves all three.
        payload.agentActivity = { ...(payload.agentActivity as Record<string, unknown>), id: activityId, body: content, signal };
        await postPayload(payload);
      },
      async postStop(suffix) { await postPayload(payloadFor("agent-session-stop.json", suffix)); },
      async postCommentCreated(suffix, instruction = "ship it") {
        const commentId = `comment-${suffix}`;
        const payload = payloadFor("agent-session-created.json", suffix);
        const agentSession = payload.agentSession as Record<string, unknown>;
        const issueId = (agentSession.issue as { readonly id: string }).id;
        // Both fields, matching what Linear sends for a mention-opened session:
        // `sourceCommentId` is the comment that invoked Axis and `commentId` is
        // the captured thread root. For a top-level mention they are the same
        // comment, and the Relay's provenance check requires exactly that.
        payload.agentSession = { ...agentSession, sourceCommentId: commentId, commentId };
        provider.exactIssues.set(issueId, { ...provider.exactIssue, id: issueId });
        provider.exactComment = {
          id: commentId,
          parentId: null,
          url: `https://linear.app/acme/issue/DEMO-9#${commentId}`,
          body: `@axis ${instruction}`,
          // The authority is `attrs.id`, not the label: a structured mention
          // node naming the installed App User is what authorizes the comment.
          bodyData: JSON.stringify({ type: "doc", content: [{ type: "paragraph", content: [
            { type: "suggestion_userMentions", attrs: { id: APP_USER, label: "axis" } },
            { type: "text", text: ` ${instruction}` },
          ] }] }),
          issueId,
          teamId: scope.linearTeamId,
          user: { id: HUMAN_USER, active: true, app: false, url: `https://linear.app/acme/profiles/${HUMAN_USER}` },
          botAuthored: false,
          member: { id: HUMAN_USER, active: true, app: false },
          membershipComplete: true,
        };
        const deliveryId = randomUUID();
        const result = await postPayload(payload, deliveryId);
        return { deliveryId, duplicate: result.duplicate };
      },
      async postCommentPrompt(suffix, instruction) {
        const rootCommentId = `comment-${suffix}`;
        const replyCommentId = `${rootCommentId}-reply-${commentReplies.get(suffix) ?? 1}`;
        commentReplies.set(suffix, (commentReplies.get(suffix) ?? 1) + 1);
        const activityId = `mock-comment-prompt-${replyCommentId}`;
        // The rendered Activity body as Linear sends it: the mention markup
        // followed by the instruction. The Relay strips the mention and the
        // REMAINDER is the executable input, which is why the instruction is
        // recoverable from the ledger digest without the markup.
        const body = `<user id="${APP_USER}" notify>axis</user> ${instruction}`;
        provider.exactComment = {
          id: replyCommentId,
          parentId: rootCommentId,
          url: `https://linear.app/acme/issue/DEMO-9#${replyCommentId}`,
          body,
          bodyData: JSON.stringify({ type: "doc", content: [{ type: "paragraph", content: [
            { type: "suggestion_userMentions", attrs: { id: APP_USER, label: "axis" } },
            { type: "text", text: ` ${instruction}` },
          ] }] }),
          issueId: `issue-${suffix}`,
          teamId: scope.linearTeamId,
          user: { id: HUMAN_USER, active: true, app: false, url: `https://linear.app/acme/profiles/${HUMAN_USER}` },
          botAuthored: false,
          member: { id: HUMAN_USER, active: true, app: false },
          membershipComplete: true,
        };
        provider.exactActivity = {
          agentSessionId: `agent-session-${suffix}`,
          activity: { ...promptActivity(activityId, body), sourceCommentId: replyCommentId },
        };
        const payload = payloadFor("agent-session-prompted.json", suffix);
        payload.agentSession = { ...(payload.agentSession as Record<string, unknown>), commentId: rootCommentId };
        payload.agentActivity = {
          ...(payload.agentActivity as Record<string, unknown>),
          id: activityId, body, signal: "continue", sourceCommentId: replyCommentId,
        };
        const deliveryId = randomUUID();
        const result = await postPayload(payload, deliveryId);
        return { deliveryId, duplicate: result.duplicate };
      },
      async postWebhook(payload, options = {}) {
        const deliveryId = options.deliveryId ?? randomUUID();
        const result = await postPayload(payload, deliveryId, options.event);
        return { deliveryId, duplicate: result.duplicate };
      },
      async pump() {
        const ingress = await channel.channel.ticks.ingress();
        await channel.channel.ticks.observe();
        await channel.channel.ticks.outbox();
        return ingress;
      },
      maintenance,
      async readiness() {
        const response = await requestNode("/v1/node/readiness");
        if (response.status !== 200) throw new Error("mock Node readiness request failed");
        return await response.json() as Readonly<Record<string, unknown>>;
      },
      guardedDefaultWorker,
      snapshot,
      currentRouting() {
        return { changes: routingChanges, defaultWorkerKey };
      },
      async restartNode(signal = "SIGKILL") {
        if (node === undefined) throw new Error("mock Node is unavailable");
        await stopNode(node, signal);
        childOutput.push(...node.output, ...node.errors);
        stoppedNodeSnapshot = inspectStoppedNode();
        node = startNodeProcess(deployment.filename);
        await node.ready;
        childOutput.push(...node.output, ...node.errors);
      },
      async stopNodeForInspection() {
        if (node === undefined) throw new Error("mock Node is unavailable");
        await stopNode(node, "SIGTERM");
        childOutput.push(...node.output, ...node.errors);
        stoppedNodeSnapshot = inspectStoppedNode();
      },
      async close() {
        if (closed) return;
        closed = true;
        const failures: unknown[] = [];
        const attempt = async (operation: () => Promise<void> | void) => {
          try { await operation(); } catch (error) { failures.push(error); }
        };
        if (node !== undefined) await attempt(() => stopNode(node!, "SIGTERM"));
        childOutput.push(...(node?.output ?? []), ...(node?.errors ?? []));
        await attempt(() => workers.a.stop());
        await attempt(() => workers.b.stop());
        childOutput.push(...workers.a.output, ...workers.a.errors, ...workers.b.output, ...workers.b.errors);
        await attempt(() => proxy!.close());
        await attempt(() => routerHttp!.close());
        // Close the Linear listener and its stores before the shared Router, then
        // remove the topology. TestLinearChannel.close() closes its listener and stores
        // together, while the harness owns the Router separately.
        await attempt(() => linear!.close());
        await attempt(() => router!.close());
        await attempt(() => bindings!.close());
        await attempt(() => rm(root, { recursive: true, force: true }));
        if (failures.length !== 0) throw new AggregateError(failures, "mock E2E cleanup failed");
      },
    };
    return harness;
  } catch (error) {
    await Promise.allSettled([
      node === undefined ? Promise.resolve() : stopNode(node, "SIGKILL"),
      workers.a.stop("SIGKILL"),
      workers.b.stop("SIGKILL"),
      proxy?.close() ?? Promise.resolve(),
      routerHttp?.close() ?? Promise.resolve(),
      linear?.close() ?? Promise.resolve(),
    ]);
    try { router?.close(); } catch { /* preserve setup failure */ }
    try { bindings?.close(); } catch { /* preserve setup failure */ }
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

/**
 * A phase receives 320 attempts separated by 25ms pauses. Counting attempts bounds
 * stalled child-process work without making scheduling delays consume the phase's
 * budget.
 */
const PHASE_ATTEMPTS = 320;
const PHASE_ATTEMPT_INTERVAL_MS = 25;

export async function waitForPhase(phase: string, predicate: () => boolean | Promise<boolean>, maxAttempts = PHASE_ATTEMPTS): Promise<void> {
  await awaitWithinAttempts({
    settled: predicate,
    maxAttempts,
    intervalMs: PHASE_ATTEMPT_INTERVAL_MS,
    sleep: realAttemptSleep,
    describe: () => `mock E2E phase did not settle: ${phase}`,
  });
}

export async function pathExists(path: string): Promise<boolean> {
  try { await access(path); return true; } catch { return false; }
}
