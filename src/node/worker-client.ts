import {
  nodeKnownWorkerFailure,
  type NodeKnownWorkerFailure,
  type NodeTarget,
  type NodeWorkerCommandPort,
} from "../shared/router-node.js";
import {
  readRouterWorkerTransportResponse,
  validateRouterWorkerResponse,
  workerFailureTuples,
  type WorkerCommand,
  type WorkerCommandResponse,
} from "../shared/router-worker.js";
import type { NodeWorkerAdmission, NodeWorkerDispatchPort, NodeWorkerInventoryPort, NodeWorkerRecordedCommand } from "../shared/router-node-commands.js";
import type { NodeRuntimeWorkerConfig } from "./runtime-config.js";

export interface HttpNodeWorkerClientOptions {
  readonly worker: NodeRuntimeWorkerConfig;
  readonly workerDispatchTimeoutMs: number;
  readonly fetch?: typeof fetch;
}

export interface NodeWorkerClientDispatcherEntry {
  readonly target: NodeTarget;
  readonly client: NodeWorkerDispatchPort;
}

/** A fixed, redacted classification for the one private Node-to-Worker attempt. */
export class NodeWorkerDispatchError extends Error {
  constructor(readonly failure: NodeKnownWorkerFailure | null) {
    super("The private Node Worker dispatch did not produce a trusted response.");
    this.name = "NodeWorkerDispatchError";
  }
}

/**
 * A command read for a Worker target this Node no longer holds in its inventory. No Worker was
 * contacted, and none can be: the command cannot converge from here, so asking again is pointless.
 */
export class NodeWorkerTargetAbsentError extends NodeWorkerDispatchError {
  constructor() {
    super(null);
    this.name = "NodeWorkerTargetAbsentError";
  }
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const item = value as Readonly<Record<string, unknown>>;
    return `{${Object.keys(item).sort().map((key) => `${JSON.stringify(key)}:${canonical(item[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function deeplyFrozen(value: unknown): boolean {
  if (value === null || typeof value !== "object" || !Object.isFrozen(value)) return value === null || typeof value !== "object";
  return Object.values(value).every(deeplyFrozen);
}

function numericLoopbackOrigin(value: string): URL | null {
  try {
    const parsed = new URL(value);
    const host = parsed.hostname === "[::1]" ? "::1" : parsed.hostname;
    const ipv4 = host.split(".");
    const loopback = host === "::1" || (ipv4.length === 4 && ipv4[0] === "127"
      && ipv4.every((piece) => /^(?:0|[1-9][0-9]{0,2})$/u.test(piece) && Number(piece) <= 255));
    if (parsed.protocol !== "http:" || !loopback || parsed.username.length !== 0 || parsed.password.length !== 0
      || parsed.pathname !== "/" || parsed.search.length !== 0 || parsed.hash.length !== 0 || parsed.origin !== value) return null;
    return parsed;
  } catch { return null; }
}

function record(value: unknown): Readonly<Record<string, unknown>> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype
    ? value as Readonly<Record<string, unknown>> : null;
}

function exact(value: Readonly<Record<string, unknown>>, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort(), expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

async function classifiedFailure(response: Response): Promise<NodeWorkerDispatchError> {
  try {
    const value = record(await readRouterWorkerTransportResponse(response));
    const error = record(value?.error);
    if (value !== null && error !== null && exact(value, ["error"]) && exact(error, ["code", "message"])
      && typeof error.code === "string" && typeof error.message === "string") {
      const tuple = workerFailureTuples[error.code];
      if (tuple !== undefined && tuple.statuses.includes(response.status) && error.message === tuple.message) {
        return new NodeWorkerDispatchError(nodeKnownWorkerFailure(error.code));
      }
    }
  } catch { /* All malformed or unreadable failure responses are unknown. */ }
  return new NodeWorkerDispatchError(null);
}

export class HttpNodeWorkerClient implements NodeWorkerDispatchPort {
  readonly #worker: NodeRuntimeWorkerConfig;
  readonly #endpoint: URL;
  readonly #origin: URL;
  readonly #timeoutMs: number;
  readonly #fetch: typeof fetch;

  constructor(options: HttpNodeWorkerClientOptions) {
    const endpoint = numericLoopbackOrigin(options.worker.workerEndpoint);
    if (!deeplyFrozen(options.worker) || endpoint === null || options.worker.commandToken.trim().length === 0
      || !Number.isSafeInteger(options.workerDispatchTimeoutMs) || options.workerDispatchTimeoutMs < 1_000 || options.workerDispatchTimeoutMs > 120_000) {
      throw new NodeWorkerDispatchError(null);
    }
    this.#worker = options.worker;
    this.#origin = endpoint;
    this.#endpoint = new URL("/v1/worker/commands", endpoint);
    this.#timeoutMs = options.workerDispatchTimeoutMs;
    this.#fetch = options.fetch ?? fetch;
  }

  /**
   * Asks the Worker what it recorded for one command. The route is read-only on the Worker, so
   * this can never cause execution. Anything but a closed, correlated answer is unknown and throws;
   * the response itself is not trusted here -- the caller validates it against its own expectation.
   * `signal` aborts the read (the Node is shutting down); an aborted read is unknown like any other failure.
   */
  async readCommand(target: NodeTarget, commandId: string, signal?: AbortSignal): Promise<NodeWorkerRecordedCommand> {
    if (canonical(target) !== canonical(this.#worker.target)) throw new NodeWorkerDispatchError(null);
    let response: Response;
    try {
      response = await this.#fetch(new URL(`/v1/worker/commands/${encodeURIComponent(commandId)}`, this.#origin), {
        method: "GET",
        headers: { authorization: `Bearer ${this.#worker.commandToken}` },
        redirect: "error",
        signal: signal === undefined ? AbortSignal.timeout(this.#timeoutMs) : AbortSignal.any([AbortSignal.timeout(this.#timeoutMs), signal]),
      });
    } catch { throw new NodeWorkerDispatchError(null); }
    if (response.status !== 200) throw new NodeWorkerDispatchError(null);
    let value: Readonly<Record<string, unknown>> | null;
    try { value = record(await readRouterWorkerTransportResponse(response)); } catch { throw new NodeWorkerDispatchError(null); }
    if (value === null || value.commandId !== commandId) throw new NodeWorkerDispatchError(null);
    if (value.status === "not_recorded" && exact(value, ["commandId", "status"])) return { status: "not_recorded" };
    if (value.status === "recorded" && exact(value, ["commandId", "status", "response"])) return { status: "recorded", response: value.response };
    throw new NodeWorkerDispatchError(null);
  }

  async dispatch(target: NodeTarget, command: WorkerCommand): Promise<WorkerCommandResponse> {
    if (canonical(target) !== canonical(this.#worker.target)) throw new NodeWorkerDispatchError(null);
    let response: Response;
    try {
      response = await this.#fetch(this.#endpoint, {
        method: "POST",
        headers: { authorization: `Bearer ${this.#worker.commandToken}`, "content-type": "application/json" },
        body: JSON.stringify(command),
        redirect: "error",
        signal: AbortSignal.timeout(this.#timeoutMs),
      });
    } catch { throw new NodeWorkerDispatchError(null); }
    if (response.status !== 200 && response.status !== 202) throw await classifiedFailure(response);
    try { return await validateRouterWorkerResponse(await readRouterWorkerTransportResponse(response), command); }
    catch { throw new NodeWorkerDispatchError(null); }
  }
}

/** Package-private exact-target dispatcher for an injected command Worker-client inventory. */
export class NodeWorkerClientDispatcher implements NodeWorkerInventoryPort {
  readonly #clients = new Map<string, NodeWorkerClientDispatcherEntry>();

  constructor(entries: readonly NodeWorkerClientDispatcherEntry[]) {
    for (const entry of entries) {
      const key = `${entry.target.endpointRef.ref}\u0000${entry.target.endpointRef.version}`;
      if (this.#clients.has(key)) throw new NodeWorkerDispatchError(null);
      this.#clients.set(key, { target: structuredClone(entry.target), client: entry.client });
    }
    // An empty client inventory is allowed. A Node with no Worker
    // deployed still starts, serves readiness and associates with Router.
    //
    // For a `start`, the refusal happens before this dispatcher is reached:
    // `selectNodeInventoryWorker` throws `NodeWorkerInventoryEmptyError`, which
    // `NodeCommandService` reports as `axis.node.worker_unavailable`. A
    // `continue` or `cancel` resolves its target from the retained route instead.
    // A Node whose inventory no longer names a route's Worker still starts, and
    // refuses such a follow-up through `admission` below, before the
    // Node records or sends it. `SqliteNodeJournal.retainedSessionsWithoutWorker`
    // counts those sessions through the same `admission`, for operators only.
  }

  /**
   * Whether a command for `target` can be handed to a Worker in this inventory at all,
   * answered from the same lookup `dispatch` uses so the two can never disagree.
   *
   * A `start` never needs it -- its reservation is chosen from the inventory -- but a `continue` or
   * `cancel` carries the reservation retained when its session started, and the Worker it names may
   * since have left the inventory. `NodeCommandService` asks before recording anything, so that
   * case is a decided refusal instead of a dispatch `dispatch` would reject and the service would
   * have to record as ambiguous.
   */
  admission(target: NodeTarget): NodeWorkerAdmission {
    return this.#entry(target) === undefined ? NODE_WORKER_ABSENT : NODE_WORKER_AVAILABLE;
  }

  dispatch(target: NodeTarget, command: WorkerCommand): Promise<WorkerCommandResponse> {
    const entry = this.#entry(target);
    if (entry === undefined) return Promise.reject(new NodeWorkerDispatchError(null));
    return entry.client.dispatch(target, command) as Promise<WorkerCommandResponse>;
  }

  readCommand(target: NodeTarget, commandId: string, signal?: AbortSignal): Promise<NodeWorkerRecordedCommand> {
    const entry = this.#entry(target);
    // A target outside the inventory is refused before any Worker is contacted, and distinctly, so the
    // caller stops asking instead of retrying a read that no Worker can answer. The same `#entry` lookup as
    // `admission` and `dispatch`, so all three agree on what the inventory holds.
    if (entry === undefined) return Promise.reject(new NodeWorkerTargetAbsentError());
    return entry.client.readCommand(target, commandId, signal);
  }

  #entry(target: NodeTarget): NodeWorkerClientDispatcherEntry | undefined {
    const entry = this.#clients.get(`${target.endpointRef.ref}\u0000${target.endpointRef.version}`);
    return entry === undefined || canonical(entry.target) !== canonical(target) ? undefined : entry;
  }
}

const NODE_WORKER_AVAILABLE: NodeWorkerAdmission = Object.freeze({ kind: "available" });
const NODE_WORKER_ABSENT: NodeWorkerAdmission = Object.freeze({ kind: "absent" });
