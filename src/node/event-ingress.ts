import type { IncomingMessage, RequestListener, ServerResponse } from "node:http";

import {
  NodeContractError,
  nodeEventRunId,
  validateNodeMaintenanceRequest,
  validateNodeEvent,
} from "../shared/router-node.js";
import type { NodeJournalEventRecord, NodeAcceptedRunRoute } from "./journal.js";
import type { NodeMaintenanceServicePort } from "./maintenance.js";
import type { NodeRuntimeConfig, NodeRuntimeWorkerConfig } from "./runtime-config.js";
import type { RouterRunLifecycleEnvelope, WorkerExecutionIdentity } from "../shared/router-worker.js";
import {
  enforceNoNodeBody,
  failNode,
  matchRawNodeRoute,
  rawHeaderValues,
  readNodeJsonBody,
  secureDigest,
  secureDigestEqual,
  secureEqual,
  sendNodeError,
  sendNodeFailure,
  sendNodeJson,
} from "./http-transport.js";

export interface NodeEventIngressJournal {
  readRunRoute(runId: string): Promise<NodeAcceptedRunRoute | null>;
  recordEvent(input: { readonly event: RouterRunLifecycleEnvelope; readonly recordedAt: string }): Promise<{ readonly replayed: boolean; readonly record: NodeJournalEventRecord }>;
}

export interface NodeEventIngressInput {
  /** Already-authenticated, complete execution identity; HTTP authentication is outside this seam. */
  readonly identity: WorkerExecutionIdentity;
  readonly body: unknown;
}

export interface NodeEventIngressReceipt {
  readonly eventId: string;
  readonly replayed: boolean;
  readonly recordedAt: string;
}

export interface NodeEventIngressHandlerOptions {
  readonly configuration: Pick<NodeRuntimeConfig, "token" | "workers">;
  readonly ingress: NodeEventIngress;
  readonly maintenance: NodeMaintenanceServicePort;
  /** Package-private lifecycle hook used by the owning service to await admitted durable work. */
  readonly track?: (operation: Promise<void>) => void;
}

type RecordValue = Readonly<Record<string, unknown>>;

function fail(code: NodeContractError["code"] = "axis.node.malformed"): never {
  return failNode(code);
}

function closed(value: unknown, keys: readonly string[]): RecordValue {
  if (value === null || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) fail();
  const item = value as RecordValue;
  if (Object.keys(item).length !== keys.length || keys.some((key) => !Object.hasOwn(item, key))) fail();
  return item;
}

function identifier(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(value)) fail();
  return value;
}

function identity(value: unknown): WorkerExecutionIdentity {
  const item = closed(value, ["nodeId", "workerId", "executorKind", "runtimeProfile"]);
  const runtimeProfile = closed(item.runtimeProfile, ["ref", "version"]);
  return {
    nodeId: identifier(item.nodeId),
    workerId: identifier(item.workerId),
    executorKind: identifier(item.executorKind),
    runtimeProfile: { ref: identifier(runtimeProfile.ref), version: identifier(runtimeProfile.version) },
  };
}

function recordedAt(value: unknown): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value)) || new Date(value).toISOString() !== value) fail();
  return value;
}

function sameIdentity(left: WorkerExecutionIdentity, right: WorkerExecutionIdentity): boolean {
  return left.nodeId === right.nodeId
    && left.workerId === right.workerId
    && left.executorKind === right.executorKind
    && left.runtimeProfile.ref === right.runtimeProfile.ref
    && left.runtimeProfile.version === right.runtimeProfile.version;
}

function copyIdentity(value: WorkerExecutionIdentity): WorkerExecutionIdentity {
  return {
    nodeId: value.nodeId,
    workerId: value.workerId,
    executorKind: value.executorKind,
    runtimeProfile: { ref: value.runtimeProfile.ref, version: value.runtimeProfile.version },
  };
}

/** Token-only private registry. It retains only digest-to-identity bindings, never endpoint or token material. */
export class NodeEventTokenRegistry {
  readonly #bindings: readonly { readonly digest: Buffer; readonly identity: WorkerExecutionIdentity }[];

  constructor(workers: readonly NodeRuntimeWorkerConfig[]) {
    const byDigest = new Map<string, { readonly digest: Buffer; readonly identity: WorkerExecutionIdentity }>();
    for (const worker of workers) {
      const digest = secureDigest(worker.eventToken), key = digest.toString("hex"), identity = copyIdentity(worker.target.identity);
      const existing = byDigest.get(key);
      if (existing !== undefined && !sameIdentity(existing.identity, identity)) throw new NodeContractError("axis.node.binding_conflict");
      if (existing === undefined) byDigest.set(key, { digest, identity });
    }
    this.#bindings = [...byDigest.values()];
  }

  authenticate(token: string): WorkerExecutionIdentity | null {
    const candidate = secureDigest(token);
    let authenticated: WorkerExecutionIdentity | null = null;
    for (const binding of this.#bindings) {
      if (secureDigestEqual(candidate, binding.digest)) authenticated = binding.identity;
    }
    return authenticated === null ? null : copyIdentity(authenticated);
  }
}

async function durable<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    if (error instanceof NodeContractError) throw error;
    fail("axis.node.storage_unavailable");
  }
}

/** Package-private pure ingress seam. It deliberately owns neither authentication nor HTTP transport. */
export class NodeEventIngress {
  readonly #journal: NodeEventIngressJournal;
  readonly #afterRecord: () => Promise<void> | void;
  readonly #onUnroutableRun: (input: { readonly runId: string; readonly identity: WorkerExecutionIdentity }) => void;

  constructor(input: {
    readonly journal: NodeEventIngressJournal;
    readonly afterRecord?: () => Promise<void> | void;
    /**
     * Told about every event refused because its Run has no deliverable route, with the
     * authenticated identity that sent it. It must not block or fail the refusal: the event is still
     * refused, and the Worker retries it. The command runtime uses it to converge an `ambiguous` receipt.
     */
    readonly onUnroutableRun?: (input: { readonly runId: string; readonly identity: WorkerExecutionIdentity }) => void;
  }) {
    this.#journal = input.journal;
    this.#afterRecord = input.afterRecord ?? (() => undefined);
    this.#onUnroutableRun = input.onUnroutableRun ?? (() => undefined);
  }

  async ingest(input: NodeEventIngressInput): Promise<NodeEventIngressReceipt> {
    const body = closed(input.body, ["envelope", "recordedAt"]);
    const authenticatedIdentity = identity(input.identity);
    const event = await validateNodeEvent(body.envelope);
    const durableRecordedAt = recordedAt(body.recordedAt);
    const route = await durable(() => this.#journal.readRunRoute(nodeEventRunId(event)));
    if (route === null) {
      // `readRunRoute` resolves only accepted command routes. The Worker sink treats only a 409 carrying a
      // Router refusal code as permanent, so it keeps retrying this event.
      try { this.#onUnroutableRun({ runId: nodeEventRunId(event), identity: authenticatedIdentity }); } catch { /* The refusal never depends on the hook. */ }
      fail("axis.node.target_unknown");
    }
    if (!sameIdentity(authenticatedIdentity, route.target.identity)) fail("axis.node.target_forbidden");
    const receipt = await durable(() => this.#journal.recordEvent({ event, recordedAt: durableRecordedAt }));
    await this.#afterRecord();
    return { eventId: receipt.record.eventId, replayed: receipt.replayed, recordedAt: receipt.record.recordedAt };
  }
}

function authenticatedIdentity(request: IncomingMessage, registry: NodeEventTokenRegistry): WorkerExecutionIdentity {
  const values = rawHeaderValues(request, "authorization"), prefix = "Bearer ";
  if (values.length !== 1) fail("axis.node.unauthorized");
  const authorization = values[0];
  if (authorization === undefined || !authorization.startsWith(prefix) || authorization.length === prefix.length) fail("axis.node.unauthorized");
  const identity = registry.authenticate(authorization.slice(prefix.length));
  if (identity === null) fail("axis.node.unauthorized");
  return identity;
}

function authenticateNode(request: IncomingMessage, token: string): void {
  const values = rawHeaderValues(request, "authorization"), candidate = values.length === 1 ? values[0] ?? "" : "";
  if (values.length !== 1 || !secureEqual(candidate, `Bearer ${token}`)) fail("axis.node.unauthorized");
}

/** A mountable private RequestListener only. It deliberately creates no server or production listener. */
export function nodeEventIngressHandler(options: NodeEventIngressHandlerOptions): RequestListener {
  const registry = new NodeEventTokenRegistry(options.configuration.workers);
  return (request, response) => {
    const operation = handle(request, response).catch((error: unknown) => {
      if (!response.headersSent) sendNodeFailure(response, error);
      else response.destroy();
    });
    options.track?.(operation);
    void operation;
  };

  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const target = matchRawNodeRoute(request, ["/v1/node/maintenance", "/v1/router/run-events"] as const);
    if (target.route === "/v1/node/maintenance") {
      const expectedMethod = request.method === "GET" || request.method === "POST" ? request.method : undefined;
      if (expectedMethod === undefined) {
        response.setHeader("allow", "GET, POST");
        sendNodeError(response, "axis.node.malformed", 405);
        return;
      }
      authenticateNode(request, options.configuration.token);
      if (target.query !== "") fail();
      if (expectedMethod === "GET") {
        await enforceNoNodeBody(request);
        sendNodeJson(response, 200, await options.maintenance.status());
        return;
      }
      const parsed = await readNodeJsonBody(request);
      const maintenance = validateNodeMaintenanceRequest(parsed);
      sendNodeJson(response, 200, await options.maintenance.setMode(maintenance.mode));
      return;
    }
    if (target.route !== "/v1/router/run-events") {
      sendNodeError(response, "axis.node.target_unknown");
      return;
    }
    if (request.method !== "POST") {
      response.setHeader("allow", "POST");
      sendNodeError(response, "axis.node.malformed", 405);
      return;
    }
    const identity = authenticatedIdentity(request, registry);
    if (target.query !== "") fail();
    const parsed = await readNodeJsonBody(request);
    const receipt = await options.ingress.ingest({ identity, body: parsed });
    sendNodeJson(response, receipt.replayed ? 200 : 202, receipt);
  }
}
