import { NODE_DEFINITE_ERROR_CODES, NODE_ERROR_STATUS, NODE_NESTED_BODY_LIMIT_BYTES, NODE_OUTER_BODY_LIMIT_BYTES } from "../shared/router-node.js";
import { parseStrictRouterWorkerJson, routerWorkerEndpoint, isWorkerOpaqueIdentifier } from "../shared/router-worker.js";
import { RouterWorkerDispatchError, type RouterNodeTransportFailure } from "./router.js";
import type {
  NodeAuthenticatedContext,
  NodeCommandRequest,
  NodeReceiptQuery,
} from "../shared/router-node-commands.js";
import { NODE_COMMAND_CONTRACT_VERSION, validateNodeCommandError, validateNodeReceiptQuery } from "../shared/router-node-commands.js";


const defaultTimeoutMs = 30_000;
const minimumTimeoutMs = 1_000;
const maximumTimeoutMs = 120_000;

/**
 * Package-private transport seam. Authentication is transport-owned; Router still
 * validates the returned body against the exact request or query.
 */
export interface RouterNodePortResult {
  /** Transport-owned new/replay disposition; Router requires 202 on dispatch and 200 on query. */
  readonly status: 200 | 202;
  readonly body: unknown;
  readonly authenticatedNode: NodeAuthenticatedContext;
}

export interface RouterNodePort {
  dispatch(request: NodeCommandRequest): Promise<RouterNodePortResult>;
  readReceipt(query: NodeReceiptQuery): Promise<RouterNodePortResult>;
}

export interface HttpRouterNodePortOptions {
  readonly endpoint: URL | string;
  readonly bearerToken: string;
  readonly authenticatedNode: NodeAuthenticatedContext;
  readonly timeoutMs?: number;
  readonly fetch?: typeof fetch;
}

function ambiguous(transportFailure?: RouterNodeTransportFailure): RouterWorkerDispatchError {
  return new RouterWorkerDispatchError("axis.worker.transport_unknown", "ambiguous", transportFailure);
}

/**
 * Connection-level failures before any response headers, typically a pooled
 * keep-alive connection whose Node just closed or restarted. undici reports them
 * as `TypeError("fetch failed")` with the socket error as `cause`.
 */
const retryableConnectionFailures: ReadonlySet<string> = new Set(["ECONNRESET", "UND_ERR_SOCKET", "EPIPE"]);

function connectionFailure(error: unknown): "ECONNRESET" | "UND_ERR_SOCKET" | "EPIPE" | undefined {
  if (!(error instanceof TypeError)) return undefined;
  const code = (error.cause as { readonly code?: unknown } | undefined)?.code;
  return typeof code === "string" && retryableConnectionFailures.has(code) ? code as "ECONNRESET" | "UND_ERR_SOCKET" | "EPIPE" : undefined;
}

function transportFailure(error: unknown, signal: AbortSignal): RouterNodeTransportFailure {
  if (signal.aborted || (error instanceof DOMException && (error.name === "TimeoutError" || error.name === "AbortError"))) return "timeout";
  return connectionFailure(error) ?? "other";
}

async function responseBody(response: Response, signal: AbortSignal, limit = NODE_OUTER_BODY_LIMIT_BYTES): Promise<unknown> {
  const contentType = (response.headers.get("content-type") ?? "").trim().toLowerCase();
  const declaredLength = Number(response.headers.get("content-length") ?? "0");
  if (!/^application\/json(?:\s*;\s*charset=utf-8)?$/u.test(contentType)
    || response.headers.has("content-encoding")
    || !Number.isSafeInteger(declaredLength)
    || declaredLength < 0
    || declaredLength > limit) throw ambiguous();

  const reader = response.body?.getReader();
  if (reader === undefined) throw ambiguous();
  const abort = () => { void reader.cancel().catch(() => undefined); };
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  try {
    const chunks: Uint8Array[] = [];
    let length = 0;
    for (;;) {
      if (signal.aborted) throw ambiguous();
      const result = await reader.read();
      if (signal.aborted) throw ambiguous();
      if (result.done) break;
      length += result.value.byteLength;
      if (length > limit) {
        void reader.cancel().catch(() => undefined);
        throw ambiguous();
      }
      chunks.push(result.value);
    }
    const body = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
    try { return parseStrictRouterWorkerJson(body); }
    catch { throw ambiguous(); }
  } finally { signal.removeEventListener("abort", abort); }
}

/**
 * Classifies a Node-tier failure from its `NodeCommandError` envelope. A definite
 * verdict requires a strict `NodeCommandError` body, a code on
 * `NODE_DEFINITE_ERROR_CODES`, and exactly the HTTP status that code carries.
 * Anything else (including every 5xx but `axis.node.draining`) is ambiguous.
 *
 * This says only that the Node did not accept the request. `Router#dispatchNodeCommand`
 * decides what it means: definite on a dispatch means nothing ran; definite on a
 * receipt query proves nothing about the command.
 */
async function classifiedFailure(response: Response, signal: AbortSignal): Promise<never> {
  try {
    const body = validateNodeCommandError(await responseBody(response, signal, NODE_OUTER_BODY_LIMIT_BYTES));
    if (NODE_DEFINITE_ERROR_CODES.includes(body.error.code) && NODE_ERROR_STATUS[body.error.code] === response.status) {
      throw new RouterWorkerDispatchError(body.error.code, "definite");
    }
  } catch (error) {
    if (error instanceof RouterWorkerDispatchError && error.disposition === "definite") throw error;
  }
  throw ambiguous();
}

/** Package-private command transport. Router owns contextual receipt validation and durable replay policy. */
export class HttpRouterNodePort implements RouterNodePort {
  readonly #endpoint: URL;
  readonly #token: string;
  readonly #authenticatedNode: NodeAuthenticatedContext;
  readonly #timeoutMs: number;
  readonly #fetch: typeof fetch;

  constructor(options: HttpRouterNodePortOptions) {
    const timeoutMs = options.timeoutMs ?? defaultTimeoutMs;
    if (typeof options.bearerToken !== "string"
      || options.bearerToken.trim().length === 0
      || !Number.isSafeInteger(timeoutMs)
      || timeoutMs < minimumTimeoutMs
      || timeoutMs > maximumTimeoutMs
      || options.authenticatedNode === null
      || typeof options.authenticatedNode !== "object"
      || Array.isArray(options.authenticatedNode)
      || Object.getPrototypeOf(options.authenticatedNode) !== Object.prototype
      || Object.keys(options.authenticatedNode).length !== 2
      || !Object.hasOwn(options.authenticatedNode, "nodeRef")
      || !Object.hasOwn(options.authenticatedNode, "nodeId")
      || !isWorkerOpaqueIdentifier(options.authenticatedNode.nodeId)) throw ambiguous();
    try {
      const endpoint = new URL(options.endpoint);
      if (endpoint.protocol !== "http:" && endpoint.protocol !== "https:") throw ambiguous();
      this.#endpoint = endpoint;
      const nodeRef = validateNodeReceiptQuery({
        contractVersion: NODE_COMMAND_CONTRACT_VERSION,
        nodeRef: options.authenticatedNode.nodeRef,
        commandId: "router-node-port",
      }).nodeRef;
      this.#authenticatedNode = Object.freeze({ nodeRef, nodeId: options.authenticatedNode.nodeId });
    } catch { throw ambiguous(); }
    this.#token = options.bearerToken;
    this.#timeoutMs = timeoutMs;
    this.#fetch = options.fetch ?? fetch;
  }

  async #post(path: "/v1/node/commands" | "/v1/node/command-receipts", value: NodeCommandRequest | NodeReceiptQuery, expectedStatus: 200 | 202): Promise<RouterNodePortResult> {
    let serialized: string;
    try {
      serialized = JSON.stringify(value);
      if (Buffer.byteLength(serialized, "utf8") > NODE_OUTER_BODY_LIMIT_BYTES
        || ("command" in value && Buffer.byteLength(JSON.stringify(value.command), "utf8") > NODE_NESTED_BODY_LIMIT_BYTES)) throw ambiguous();
    } catch { throw ambiguous(); }
    const signal = AbortSignal.timeout(this.#timeoutMs);
    const url = routerWorkerEndpoint(this.#endpoint, path);
    const init: RequestInit = {
      method: "POST",
      headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" },
      body: serialized,
      redirect: "error",
      signal,
    };
    let response: Response;
    try {
      response = await this.#fetch(url, init);
    } catch (error) {
      // A connection-level failure before any response headers is retried once, as the
      // identical POST on the same deadline; nothing else is retried. The Node
      // deduplicates by `commandId` and intent digest, so a retry of a request whose
      // response was lost answers the same receipt.
      if (connectionFailure(error) === undefined || signal.aborted) throw ambiguous(transportFailure(error, signal));
      try {
        response = await this.#fetch(url, init);
      } catch (retryError) {
        throw ambiguous(transportFailure(retryError, signal));
      }
    }
    if (response.redirected) throw ambiguous();
    if (response.status !== expectedStatus) return classifiedFailure(response, signal);
    return {
      status: expectedStatus,
      body: await responseBody(response, signal, NODE_OUTER_BODY_LIMIT_BYTES),
      authenticatedNode: this.#authenticatedNode,
    };
  }

  async dispatch(request: NodeCommandRequest): Promise<RouterNodePortResult> {
    try {
      const identity = validateNodeReceiptQuery({
        contractVersion: request.contractVersion,
        nodeRef: request.nodeRef,
        commandId: request.command.commandId,
      });
      if (identity.nodeRef.ref !== this.#authenticatedNode.nodeRef.ref
        || identity.nodeRef.version !== this.#authenticatedNode.nodeRef.version) throw ambiguous();
    } catch { throw ambiguous(); }
    return this.#post("/v1/node/commands", request, 202);
  }

  async readReceipt(queryValue: NodeReceiptQuery): Promise<RouterNodePortResult> {
    let query: NodeReceiptQuery;
    try { query = validateNodeReceiptQuery(queryValue); } catch { throw ambiguous(); }
    if (query.nodeRef.ref !== this.#authenticatedNode.nodeRef.ref || query.nodeRef.version !== this.#authenticatedNode.nodeRef.version) throw ambiguous();
    return this.#post("/v1/node/command-receipts", query, 200);
  }
}
