import { createHmac, randomBytes, randomUUID } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { getOperationAST, Kind, parse } from "graphql";

import type { LinearCliContext } from "./cli-access.js";
import type { LinearExecutionAccessContext } from "./execution-access-contract.js";

export const LINEAR_API_PATH = "/api/linear/graphql";
export const LINEAR_API_UPSTREAM = "https://api.linear.app/graphql";

type JsonObject = Record<string, unknown>;
type Operation = "query" | "mutation";
type Outcome = "not_dispatched" | "dispatched" | "response_received" | "graphql_errors" | "partial" | "upstream_error" | "outcome_unknown";

/** Private operator audit, never a public Activity or a request/body log. */
export type LinearApiContext = LinearExecutionAccessContext | LinearCliContext;
export type LinearApiAccessResult<T> = { readonly authorized: true; readonly value: T } | { readonly authorized: false; readonly status: number; readonly error: { readonly code: string; readonly message: string } };
export interface LinearApiAccess {
  verify(credential: unknown): LinearApiAccessResult<LinearApiContext>;
  withAccess<T>(credential: unknown, operation: (token: string, context: LinearApiContext) => Promise<T>): Promise<LinearApiAccessResult<T>>;
}

export interface LinearApiAudit {
  readonly requestId: string;
  readonly timestamp: string;
  readonly phase: "dispatch" | "complete";
  readonly target: typeof LINEAR_API_UPSTREAM;
  /** Process-keyed fingerprint of the exact forwarded envelope; no query or variables. */
  readonly requestDigest: string | null;
  readonly operation: Operation | null;
  readonly caller: LinearApiContext | null;
  readonly outcome: Outcome;
  readonly status: number | null;
  readonly upstreamStatus: number | null;
  readonly code: string | null;
}

export interface LinearApiProxyOptions {
  /** `null` means no trusted execution authority is configured, and every request is refused. */
  readonly access: LinearApiAccess | null;
  readonly audit: (event: LinearApiAudit) => void;
  readonly fetch?: typeof fetch;
  readonly timeoutMs?: number;
  readonly maxRequestBytes?: number;
  readonly maxResponseBytes?: number;
}

interface Reply {
  readonly status: number;
  readonly body: JsonObject;
  readonly outcome: Outcome;
  readonly code: string | null;
  readonly upstreamStatus: number | null;
  readonly headers?: Record<string, string>;
}

class ProxyFailure extends Error {
  constructor(readonly code: string, readonly status: number) { super("Linear API request could not be completed."); }
}

function object(value: unknown): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function requestEnvelope(raw: Buffer): { body: string; operation: Operation } {
  try {
    const value: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
    if (!object(value) || Object.keys(value).some((key) => !["query", "variables", "operationName"].includes(key))
      || typeof value.query !== "string" || value.query.trim().length === 0
      || (value.variables !== undefined && value.variables !== null && !object(value.variables))
      || (value.operationName !== undefined && value.operationName !== null && typeof value.operationName !== "string")) throw new Error();
    // Syntax/operation selection only. Linear owns its schema, permissions and resource validation.
    const document = parse(value.query, { noLocation: true, maxTokens: 20_000 });
    const names = new Set<string | undefined>();
    for (const definition of document.definitions) {
      if (definition.kind === Kind.FRAGMENT_DEFINITION) continue;
      if (definition.kind !== Kind.OPERATION_DEFINITION || names.has(definition.name?.value)) throw new Error();
      names.add(definition.name?.value);
    }
    if (names.size > 1 && names.has(undefined)) throw new Error();
    const operation = getOperationAST(document, value.operationName as string | null | undefined);
    if (operation == null || operation.operation === "subscription") throw new Error();
    return { body: JSON.stringify(value), operation: operation.operation };
  } catch {
    throw new ProxyFailure("linear.proxy.invalid_request", 400);
  }
}

function graphQlEnvelope(value: unknown): value is JsonObject {
  if (!object(value)) return false;
  const data = Object.hasOwn(value, "data");
  const errors = Object.hasOwn(value, "errors");
  return (data || errors) && (!data || value.data === null || object(value.data))
    && (!errors || (Array.isArray(value.errors) && value.errors.length > 0
      && value.errors.every((error) => object(error) && typeof error.message === "string")));
}

/** Only documented quota information; no cookies, redirects, auth or upstream diagnostics. */
function rateHeaders(headers: Headers, secrets: readonly string[]): Record<string, string> {
  const result: Record<string, string> = {};
  for (const name of ["x-complexity", "x-ratelimit-requests-limit", "x-ratelimit-requests-remaining", "x-ratelimit-requests-reset",
    "x-ratelimit-endpoint-requests-limit", "x-ratelimit-endpoint-requests-remaining", "x-ratelimit-endpoint-requests-reset",
    "x-ratelimit-complexity-limit", "x-ratelimit-complexity-remaining", "x-ratelimit-complexity-reset", "retry-after", "x-ratelimit-endpoint-name"]) {
    const value = headers.get(name);
    if (value === null || value.length > 128 || secrets.some((secret) => value.includes(secret))) continue;
    if (/^\d+(?:\.\d+)?$/.test(value)
      || (name === "retry-after" && /^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(value))
      || (name === "x-ratelimit-endpoint-name" && /^[_A-Za-z][_0-9A-Za-z]{0,127}$/.test(value))) result[name] = value;
  }
  return result;
}

function localReply(code: string, status: number, requestId: string, outcome: Outcome = "not_dispatched",
  upstreamStatus: number | null = null, upstream?: JsonObject): Reply {
  const message = outcome === "outcome_unknown"
    ? "The write outcome is unknown. Reconcile the original request before any retry."
    : "Linear API request could not be completed.";
  return {
    status, outcome, code, upstreamStatus,
    body: { ...upstream, errors: [
      ...(Array.isArray(upstream?.errors) ? upstream.errors : []),
      { message, extensions: { code, requestId, outcome, ...(upstreamStatus === null ? {} : { upstreamStatus }) } },
    ] },
  };
}

function abortable<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(new ProxyFailure("linear.proxy.timeout", 504));
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    // Both handlers remain attached even after a timeout, containing late failures.
    void work.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
  });
}

function readRequest(request: IncomingMessage, maxBytes: number, signal: AbortSignal): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    const cleanup = () => {
      request.off("data", data); request.off("end", end); request.off("error", error);
      signal.removeEventListener("abort", abort);
    };
    const fail = (failure: ProxyFailure) => { cleanup(); request.resume(); reject(failure); };
    const data = (chunk: Buffer) => {
      size += chunk.length;
      if (size > maxBytes) fail(new ProxyFailure("linear.proxy.body_too_large", 413));
      else chunks.push(chunk);
    };
    const end = () => { cleanup(); resolve(Buffer.concat(chunks)); };
    const error = () => fail(new ProxyFailure("linear.proxy.invalid_request", 400));
    const abort = () => fail(new ProxyFailure("linear.proxy.timeout", 504));
    request.on("data", data); request.once("end", end); request.once("error", error);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

async function readResponse(response: Response, maxBytes: number): Promise<JsonObject> {
  const type = response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase();
  if (!["application/json", "application/graphql-response+json"].includes(type ?? "") || response.body === null) {
    void response.body?.cancel().catch(() => {});
    throw new ProxyFailure("linear.proxy.invalid_response", 502);
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.length;
      if (size > maxBytes) throw new ProxyFailure("linear.proxy.response_too_large", 502);
      chunks.push(chunk.value);
    }
    const body: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks)));
    if (!graphQlEnvelope(body)) throw new ProxyFailure("linear.proxy.invalid_response", 502);
    return body;
  } catch (error) {
    void reader.cancel().catch(() => {});
    throw error instanceof ProxyFailure ? error : new ProxyFailure("linear.proxy.invalid_response", 502);
  } finally { reader.releaseLock(); }
}

function positive(value: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) throw new Error("Invalid Linear API limit.");
  return value;
}

/** One upstream send per request. No SDK retries, redirect following, outbox or mutation replay. */
export class LinearApiProxy {
  readonly #options: LinearApiProxyOptions;
  readonly #fetch: typeof fetch;
  readonly #timeout: number;
  readonly #maxRequest: number;
  readonly #maxResponse: number;
  readonly #digestKey = randomBytes(32);
  #inflight = 0;

  constructor(options: LinearApiProxyOptions) {
    this.#options = options;
    this.#fetch = options.fetch ?? fetch;
    this.#timeout = positive(options.timeoutMs ?? 30_000, 120_000);
    this.#maxRequest = positive(options.maxRequestBytes ?? 1_048_576, 1_048_576);
    this.#maxResponse = positive(options.maxResponseBytes ?? 8_388_608, 8_388_608);
  }

  async handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const requestId = randomUUID();
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.#timeout);
    timeout.unref();
    const disconnect = () => { if (!response.writableFinished) controller.abort(); };
    request.once("aborted", disconnect);
    response.once("close", disconnect);
    let caller: LinearApiContext | null = null;
    let requestDigest: string | null = null;
    let operation: Operation | null = null;
    let dispatched = false;
    let upstreamStatus: number | null = null;
    let upstreamHeaders: Record<string, string> = {};
    let admitted = false;
    const audit = (phase: LinearApiAudit["phase"], reply: Pick<Reply, "outcome" | "code"> & { status: number | null }) => {
      this.#options.audit(Object.freeze({ requestId, timestamp: new Date().toISOString(), phase,
        target: LINEAR_API_UPSTREAM, requestDigest, operation, caller, outcome: reply.outcome,
        status: reply.status, upstreamStatus, code: reply.code }));
    };
    const uncertain = (code: string, status: number, body?: JsonObject): Reply => dispatched && operation === "mutation"
      ? localReply("linear.proxy.outcome_unknown", 502, requestId, "outcome_unknown", upstreamStatus, body)
      : localReply(code, status, requestId, dispatched ? "upstream_error" : "not_dispatched", upstreamStatus);
    let reply: Reply;
    try {
      const access = this.#options.access;
      if (access === null) throw new ProxyFailure("linear.proxy.execution_access_unavailable", 503);
      // CLI API keys use a bare header; SDKs commonly use Bearer. Both resolve only explicitly configured Axis authority.
      const authorization = request.headers.authorization ?? "";
      const credential = authorization.replace(/^Bearer /i, "");
      const verified = access.verify(credential);
      if (!verified.authorized) {
        reply = localReply(verified.error.code, verified.status, requestId);
      } else {
        caller = verified.value;
        if (this.#inflight >= 64) throw new ProxyFailure("linear.proxy.capacity", 503);
        this.#inflight++; admitted = true;
        if (request.method !== "POST") throw new ProxyFailure("linear.proxy.method_not_allowed", 405);
        if (new URL(request.url ?? "/", "http://relay.invalid").search !== "") throw new ProxyFailure("linear.proxy.invalid_request", 400);
        if (!/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(request.headers["content-type"] ?? "")
          || (request.headers["content-encoding"] !== undefined && request.headers["content-encoding"] !== "identity")) {
          throw new ProxyFailure("linear.proxy.unsupported_media_type", 415);
        }
        const raw = await readRequest(request, this.#maxRequest, controller.signal);
        const envelope = requestEnvelope(raw);
        operation = envelope.operation;
        requestDigest = createHmac("sha256", this.#digestKey).update(envelope.body).digest("hex");
        const authorized = await abortable(access.withAccess(credential, async (token, context) => {
          // A late OAuth/viewer completion cannot dispatch after timeout or client disconnect.
          if (controller.signal.aborted) return uncertain("linear.proxy.timeout", 504);
          caller = context;
          try { audit("dispatch", { outcome: "dispatched", status: null, code: null }); }
          catch { return localReply("linear.proxy.audit_unavailable", 503, requestId); }
          if (controller.signal.aborted) return uncertain("linear.proxy.timeout", 504);
          dispatched = true;
          try {
            const upstream = await this.#fetch(LINEAR_API_UPSTREAM, {
              method: "POST", redirect: "error", signal: controller.signal,
              headers: { authorization: `Bearer ${token}`, "content-type": "application/json", accept: "application/json" },
              body: envelope.body,
            });
            upstreamStatus = upstream.status;
            const headers = rateHeaders(upstream.headers, [credential, token]);
            upstreamHeaders = headers;
            const body = await readResponse(upstream, this.#maxResponse);
            // Mask only the exact two credentials if an upstream error echoes them. Never log this response.
            let serialized = JSON.stringify(body);
            for (const secret of [credential, token]) serialized = serialized.split(JSON.stringify(secret).slice(1, -1)).join("[redacted]");
            const safeBody = JSON.parse(serialized) as JsonObject;
            if (upstream.status >= 500 && operation === "mutation") return { ...uncertain("linear.proxy.upstream_error", 502, safeBody), headers };
            if (upstream.status < 200 || (upstream.status >= 300 && upstream.status < 400)) return uncertain("linear.proxy.invalid_response", 502);
            const errors = Array.isArray(safeBody.errors) && safeBody.errors.length > 0;
            const outcome: Outcome = errors ? (safeBody.data == null ? "graphql_errors" : "partial")
              : upstream.ok ? "response_received" : "upstream_error";
            return { status: upstream.status, body: safeBody, outcome, code: null, upstreamStatus, headers };
          } catch (error) {
            return uncertain(error instanceof ProxyFailure ? error.code : "linear.proxy.upstream_unavailable", 502);
          }
        }), controller.signal);
        reply = authorized.authorized ? authorized.value : localReply(authorized.error.code, authorized.status, requestId);
      }
    } catch (error) {
      reply = uncertain(error instanceof ProxyFailure ? error.code : "linear.proxy.unavailable", error instanceof ProxyFailure ? error.status : 503);
    } finally {
      clearTimeout(timeout);
      request.off("aborted", disconnect); response.off("close", disconnect);
      if (admitted) this.#inflight--;
    }
    reply = { ...reply, headers: { ...upstreamHeaders, ...reply.headers } };
    let auditStatus = "recorded";
    try { audit("complete", reply); } catch { auditStatus = "unavailable"; }
    if (response.destroyed) return;
    const encoded = JSON.stringify(reply.body);
    response.writeHead(reply.status, {
      ...reply.headers, "content-type": "application/json; charset=utf-8", "content-length": Buffer.byteLength(encoded),
      "cache-control": "no-store", "x-axis-request-id": requestId, "x-axis-linear-outcome": reply.outcome,
      "x-axis-linear-audit": auditStatus, ...(reply.status === 405 ? { allow: "POST" } : {}),
      ...(!request.complete ? { connection: "close" } : {}),
    });
    response.end(encoded);
  }
}
