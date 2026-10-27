import { ROUTER_WORKER_TRANSPORT_BODY_LIMIT_BYTES, parseStrictRouterWorkerJson, routerWorkerEndpoint, validWorkerExecutionIdentity, type RouterRunLifecycleEnvelope, type WorkerExecutionIdentity, type WorkerVersionedReference } from "../shared/router-worker.js";
import { nodeEventId, nodeEventRunId, validateNodeEvent } from "../shared/router-node.js";
import { NODE_COMMAND_CONTRACT_VERSION, validateNodeAuthenticatedContext } from "../shared/router-node-commands.js";

const defaultTimeoutMs = 10_000;
const failureMessage = "Router event delivery failed.";

export type RouterEventDeliveryFailureKind = "delivery_unavailable" | "protocol_conflict";

/** Safe classification only. It intentionally retains no provider response, endpoint, or transport details. */
export class RouterEventDeliveryFailure extends Error {
  constructor(readonly kind: RouterEventDeliveryFailureKind) { super(failureMessage); this.name = "RouterEventDeliveryFailure"; }
}

/**
 * The Node's one Router event delivery. The accepted-route authority is required: every
 * event the pump delivers was recorded against an accepted command route that names both.
 */
export interface RouterEventDeliveryPort {
  deliver(input: {
    readonly event: RouterRunLifecycleEnvelope;
    readonly recordedAt: string;
    readonly signal?: AbortSignal;
    /** Recovered from the accepted durable route. */
    readonly nodeRef: WorkerVersionedReference;
    /** Recovered from the accepted durable route. */
    readonly origin: WorkerExecutionIdentity;
  }): Promise<RouterEventDeliveryReceipt>;
}

export interface RouterEventDeliveryOptions {
  readonly endpoint: URL | string;
  readonly bearerToken: string;
  readonly timeoutMs?: number;
  readonly fetch?: typeof fetch;
}

export interface RouterEventDeliveryReceipt {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly nodeRef: WorkerVersionedReference;
  readonly sourceMessageId: string;
  readonly runId: string;
  readonly status: "buffered" | "projected";
  readonly replayed: boolean;
}

type RecordValue = Readonly<Record<string, unknown>>;

function record(value: unknown): RecordValue | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype ? value as RecordValue : null;
}
function exact(value: RecordValue, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort(), expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}
function same(left: unknown, right: unknown): boolean { return canonical(left) === canonical(right); }
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const item = value as Record<string, unknown>;
    return `{${Object.keys(item).sort().map((key) => `${JSON.stringify(key)}:${canonical(item[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

class ResponseBodyUnavailable extends Error {}

async function responseBody(response: Response, signal: AbortSignal): Promise<unknown> {
  const contentType = (response.headers.get("content-type") ?? "").trim().toLowerCase();
  const declaredLength = Number(response.headers.get("content-length") ?? "0");
  if (!/^application\/json(?:\s*;\s*charset=utf-8)?$/u.test(contentType) || response.headers.has("content-encoding")
    || !Number.isSafeInteger(declaredLength) || declaredLength < 0 || declaredLength > ROUTER_WORKER_TRANSPORT_BODY_LIMIT_BYTES) throw new RouterEventDeliveryFailure("protocol_conflict");
  const reader = response.body?.getReader();
  if (reader === undefined) throw new RouterEventDeliveryFailure("protocol_conflict");
  const abort = () => { void reader.cancel().catch(() => undefined); };
  signal.addEventListener("abort", abort, { once: true });
  try {
    const chunks: Uint8Array[] = [];
    let length = 0;
    for (;;) {
      if (signal.aborted) throw new ResponseBodyUnavailable();
      let result: ReadableStreamReadResult<Uint8Array>;
      try { result = await reader.read(); } catch { throw new ResponseBodyUnavailable(); }
      if (signal.aborted) throw new ResponseBodyUnavailable();
      if (result.done) break;
      length += result.value.byteLength;
      if (length > ROUTER_WORKER_TRANSPORT_BODY_LIMIT_BYTES) throw new RouterEventDeliveryFailure("protocol_conflict");
      chunks.push(result.value);
    }
    const body = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
    try { return parseStrictRouterWorkerJson(body); }
    catch { throw new RouterEventDeliveryFailure("protocol_conflict"); }
  } finally { signal.removeEventListener("abort", abort); }
}

function routerProtocolConflict(value: unknown): boolean {
  const body = record(value), error = body === null ? null : record(body.error);
  return body !== null && error !== null && exact(body, ["error"]) && exact(error, ["code", "message"])
    && error.code === "axis.router.protocol_violation" && error.message === "The Worker event protocol is invalid.";
}

async function isRouterProtocolConflict(response: Response, signal: AbortSignal): Promise<boolean> {
  try { return routerProtocolConflict(await responseBody(response, signal)); }
  catch { return false; }
}

function receipt(
  value: unknown,
  input: { readonly nodeRef: WorkerVersionedReference; readonly event: RouterRunLifecycleEnvelope },
  status: number,
): RouterEventDeliveryReceipt {
  const item = record(value);
  if (item === null || !exact(item, ["contractVersion", "nodeRef", "sourceMessageId", "runId", "status", "replayed"])
    || item.contractVersion !== NODE_COMMAND_CONTRACT_VERSION || !same(item.nodeRef, input.nodeRef)
    || item.sourceMessageId !== nodeEventId(input.event) || item.runId !== nodeEventRunId(input.event)
    || (item.status !== "buffered" && item.status !== "projected") || item.replayed !== (status === 200)) {
    throw new RouterEventDeliveryFailure("protocol_conflict");
  }
  return item as unknown as RouterEventDeliveryReceipt;
}

/** Package-private Node-to-Router event delivery (Router's `/v1/router/node-run-events`). It makes exactly one bounded request per call. */
export class RouterEventDelivery implements RouterEventDeliveryPort {
  readonly #endpoint: URL;
  readonly #token: string;
  readonly #timeoutMs: number;
  readonly #fetch: typeof fetch;

  constructor(options: RouterEventDeliveryOptions) {
    this.#endpoint = routerWorkerEndpoint(options.endpoint, "/v1/router/node-run-events");
    this.#token = options.bearerToken;
    this.#timeoutMs = options.timeoutMs ?? defaultTimeoutMs;
    this.#fetch = options.fetch ?? fetch;
    if (this.#token.length === 0 || !Number.isSafeInteger(this.#timeoutMs) || this.#timeoutMs < 1 || this.#timeoutMs > 60_000) throw new Error(failureMessage);
  }

  async deliver(input: {
    readonly event: RouterRunLifecycleEnvelope;
    readonly recordedAt: string;
    readonly signal?: AbortSignal;
    readonly nodeRef: WorkerVersionedReference;
    readonly origin: WorkerExecutionIdentity;
  }): Promise<RouterEventDeliveryReceipt> {
    const event = await validateNodeEvent(input.event);
    // The type makes both present; a caller outside the type system still cannot deliver without a valid origin.
    if (!validWorkerExecutionIdentity(input.origin)) {
      throw new RouterEventDeliveryFailure("protocol_conflict");
    }
    try { validateNodeAuthenticatedContext({ nodeRef: input.nodeRef, nodeId: input.origin.nodeId }); }
    catch { throw new RouterEventDeliveryFailure("protocol_conflict"); }

    const controller = new AbortController();
    const abort = () => controller.abort();
    input.signal?.addEventListener("abort", abort, { once: true });
    if (input.signal?.aborted) controller.abort();
    const timer = setTimeout(() => controller.abort(), this.#timeoutMs);
    try {
      const response = await this.#fetch(this.#endpoint, {
        method: "POST",
        redirect: "manual",
        signal: controller.signal,
        headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" },
        body: JSON.stringify({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: input.nodeRef, origin: input.origin, envelope: event, recordedAt: input.recordedAt }),
      });
      if (response.status === 502) {
        if (await isRouterProtocolConflict(response, controller.signal)) throw new RouterEventDeliveryFailure("protocol_conflict");
        throw new RouterEventDeliveryFailure("delivery_unavailable");
      }
      if (response.status !== 200 && response.status !== 202) {
        if (response.status === 408 || response.status === 425 || response.status === 429 || response.status >= 500) {
          throw new RouterEventDeliveryFailure("delivery_unavailable");
        }
        throw new RouterEventDeliveryFailure("protocol_conflict");
      }
      const validatedReceipt = receipt(await responseBody(response, controller.signal), { nodeRef: input.nodeRef, event }, response.status);
      if (controller.signal.aborted) throw new ResponseBodyUnavailable();
      return validatedReceipt;
    } catch (error) {
      if (error instanceof RouterEventDeliveryFailure) throw error;
      if (error instanceof ResponseBodyUnavailable || controller.signal.aborted) throw new RouterEventDeliveryFailure("delivery_unavailable");
      throw new RouterEventDeliveryFailure("delivery_unavailable");
    } finally {
      clearTimeout(timer);
      input.signal?.removeEventListener("abort", abort);
    }
  }
}
