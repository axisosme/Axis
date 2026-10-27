import { createHash, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import { nodeEventRunId, validateNodeEvent, NODE_OUTER_BODY_LIMIT_BYTES } from "../shared/router-node.js";
import { NODE_COMMAND_CONTRACT_VERSION, validateNodeAuthenticatedContext, type NodeAuthenticatedContext } from "../shared/router-node-commands.js";

import { RouterHttpTransportError, isWorkerOpaqueIdentifier, type RouterRunLifecycleEnvelope, type WorkerExecutionIdentity } from "../shared/router-worker.js";
import { Router, RouterError } from "./router.js";

export const ROUTER_HTTP_BODY_LIMIT_BYTES = 64 * 1024;
type HttpErrorCode = "axis.router.malformed" | "axis.router.unauthorized" | "axis.router.body_too_large" | "axis.router.unsupported_media" | "axis.router.intent_conflict" | "axis.router.conflict" | "axis.router.protocol_violation" | "axis.router.recovery_required" | "axis.router.worker_rejected";
const messages: Readonly<Record<HttpErrorCode, string>> = {
  "axis.router.malformed": "The Router event request is malformed.", "axis.router.unauthorized": "Router authorization is required.", "axis.router.body_too_large": "The Router event body exceeds the accepted limit.", "axis.router.unsupported_media": "The Router event media type is unsupported.", "axis.router.intent_conflict": "The Router event conflicts with an existing receipt.", "axis.router.conflict": "The Router event conflicts with durable state.", "axis.router.protocol_violation": "The Worker event protocol is invalid.", "axis.router.recovery_required": "The Router command requires safe recovery.", "axis.router.worker_rejected": "The Worker command was not accepted.",
};
class RouterHttpError extends Error { constructor(readonly code: HttpErrorCode, readonly status: 400 | 401 | 409 | 413 | 415 | 502 | 503) { super(messages[code]); } }
function failure(code: HttpErrorCode, status: RouterHttpError["status"]): never { throw new RouterHttpError(code, status); }

export interface RouterHttpService { readonly server: Server; readonly url: URL; close(): Promise<void>; }
export interface RouterHttpOptions { readonly router: Router; readonly host?: string; readonly port?: number; }

/** Package-private transport seam, absent from `src/index.ts`. */
export const ROUTER_NODE_EVENT_HTTP_PATH = "/v1/router/node-run-events" as const;
export interface RouterNodeEventHttpBinding {
  readonly bearerToken: string;
  readonly authenticatedNode: NodeAuthenticatedContext;
  readonly allowedOrigins: readonly WorkerExecutionIdentity[];
}
export interface RouterNodeEventHttpRequest {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly nodeRef: NodeAuthenticatedContext["nodeRef"];
  readonly origin: WorkerExecutionIdentity;
  readonly envelope: RouterRunLifecycleEnvelope;
  readonly recordedAt: string;
}
export interface RouterNodeEventHttpResponse {
  readonly contractVersion: typeof NODE_COMMAND_CONTRACT_VERSION;
  readonly nodeRef: NodeAuthenticatedContext["nodeRef"];
  readonly sourceMessageId: string;
  readonly runId: string;
  readonly status: "buffered" | "projected";
  readonly replayed: boolean;
}
export interface RouterNodeEventHttpOptions extends RouterHttpOptions { readonly nodeEventBindings: readonly RouterNodeEventHttpBinding[]; }

function send(response: ServerResponse, status: number, body: unknown): void { const encoded = JSON.stringify(body); response.writeHead(status, { "content-type": "application/json; charset=utf-8", "content-length": Buffer.byteLength(encoded), "cache-control": "no-store" }); response.end(encoded); }
function headers(request: IncomingMessage, name: string): readonly string[] { const values: string[] = []; for (let index = 0; index < request.rawHeaders.length; index += 2) if (request.rawHeaders[index]?.toLowerCase() === name) values.push(request.rawHeaders[index + 1] ?? ""); return values; }
function media(request: IncomingMessage): void { const values = headers(request, "content-type"); const normalized = (values[0] ?? "").trim().toLowerCase(); if (values.length !== 1 || !/^application\/json(?:\s*;\s*charset=utf-8)?$/u.test(normalized) || headers(request, "content-encoding").length !== 0) failure("axis.router.unsupported_media", 415); }
async function body(request: IncomingMessage, limit = ROUTER_HTTP_BODY_LIMIT_BYTES): Promise<Buffer> { const chunks: Buffer[] = []; let size = 0; for await (const item of request) { const chunk = Buffer.isBuffer(item) ? item : Buffer.from(item); size += chunk.length; if (size > limit) failure("axis.router.body_too_large", 413); chunks.push(chunk); } return Buffer.concat(chunks); }

class StrictJson {
  #index = 0; readonly #text: string; constructor(text: string) { this.#text = text; }
  parse(): unknown { this.#space(); this.#value(); this.#space(); if (this.#index !== this.#text.length) this.#invalid(); return JSON.parse(this.#text) as unknown; }
  #value(): void { const value = this.#text[this.#index]; if (value === "{") return this.#object(); if (value === "[") return this.#array(); if (value === "\"") return void this.#string(); if (this.#text.startsWith("true", this.#index)) return void (this.#index += 4); if (this.#text.startsWith("false", this.#index)) return void (this.#index += 5); if (this.#text.startsWith("null", this.#index)) return void (this.#index += 4); this.#number(); }
  #object(): void { this.#index += 1; this.#space(); const keys = new Set<string>(); if (this.#text[this.#index] === "}") return void (this.#index += 1); for (;;) { this.#space(); if (this.#text[this.#index] !== "\"") this.#invalid(); const key = this.#string(); if (keys.has(key)) this.#invalid(); keys.add(key); this.#space(); if (this.#text[this.#index] !== ":") this.#invalid(); this.#index += 1; this.#space(); this.#value(); this.#space(); if (this.#text[this.#index] === "}") return void (this.#index += 1); if (this.#text[this.#index] !== ",") this.#invalid(); this.#index += 1; } }
  #array(): void { this.#index += 1; this.#space(); if (this.#text[this.#index] === "]") return void (this.#index += 1); for (;;) { this.#value(); this.#space(); if (this.#text[this.#index] === "]") return void (this.#index += 1); if (this.#text[this.#index] !== ",") this.#invalid(); this.#index += 1; this.#space(); } }
  #string(): string { const start = this.#index; this.#index += 1; while (this.#index < this.#text.length) { const value = this.#text[this.#index] ?? ""; if (value === "\"") { this.#index += 1; return JSON.parse(this.#text.slice(start, this.#index)) as string; } if (value === "\\") { this.#index += 1; const escape = this.#text[this.#index]; if (escape === "u") { if (!/^[0-9a-f]{4}$/iu.test(this.#text.slice(this.#index + 1, this.#index + 5))) this.#invalid(); this.#index += 5; continue; } if (escape === undefined || !"\"\\/bfnrt".includes(escape)) this.#invalid(); this.#index += 1; continue; } if (value < " ") this.#invalid(); this.#index += 1; } this.#invalid(); }
  #number(): void { const match = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/u.exec(this.#text.slice(this.#index)); if (match?.[0] === undefined) this.#invalid(); this.#index += match[0].length; }
  #space(): void { while (/\s/u.test(this.#text[this.#index] ?? "")) this.#index += 1; }
  #invalid(): never { failure("axis.router.malformed", 400); }
}
function json(buffer: Buffer): unknown { try { return new StrictJson(new TextDecoder("utf-8", { fatal: true }).decode(buffer)).parse(); } catch (error) { if (error instanceof RouterHttpError) throw error; failure("axis.router.malformed", 400); } }
function asRecord(value: unknown): Readonly<Record<string, unknown>> | undefined { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Readonly<Record<string, unknown>> : undefined; }
function exact(value: Readonly<Record<string, unknown>>, keys: readonly string[]): boolean { const actual = Object.keys(value).sort(), expected = [...keys].sort(); return actual.length === expected.length && actual.every((key, index) => key === expected[index]); }

function nodeOrigin(value: unknown): WorkerExecutionIdentity | undefined {
  const identity = asRecord(value), runtimeProfile = asRecord(identity?.runtimeProfile);
  if (identity === undefined || runtimeProfile === undefined
    || !exact(identity, ["nodeId", "workerId", "executorKind", "runtimeProfile"])
    || !exact(runtimeProfile, ["ref", "version"])
    || ![identity.nodeId, identity.workerId, identity.executorKind, runtimeProfile.ref, runtimeProfile.version].every(isWorkerOpaqueIdentifier)) return undefined;
  return Object.freeze({
    nodeId: identity.nodeId as string,
    workerId: identity.workerId as string,
    executorKind: identity.executorKind as string,
    runtimeProfile: Object.freeze({ ref: runtimeProfile.ref as string, version: runtimeProfile.version as string }),
  });
}
function sameNode(left: NodeAuthenticatedContext, right: NodeAuthenticatedContext): boolean { return left.nodeId === right.nodeId && left.nodeRef.ref === right.nodeRef.ref && left.nodeRef.version === right.nodeRef.version; }
function sameOrigin(left: WorkerExecutionIdentity, right: WorkerExecutionIdentity): boolean { return left.nodeId === right.nodeId && left.workerId === right.workerId && left.executorKind === right.executorKind && left.runtimeProfile.ref === right.runtimeProfile.ref && left.runtimeProfile.version === right.runtimeProfile.version; }

interface NormalizedNodeEventBinding { readonly digest: Buffer; readonly authenticatedNode: NodeAuthenticatedContext; readonly allowedOrigins: readonly WorkerExecutionIdentity[]; }
class RouterNodeEventBindingRegistry {
  readonly #bindings: readonly NormalizedNodeEventBinding[];
  constructor(bindings: readonly RouterNodeEventHttpBinding[]) {
    if (bindings.length === 0) throw new Error("Router Node event bindings are required.");
    const seenTokens = new Set<string>();
    this.#bindings = bindings.map((binding): NormalizedNodeEventBinding => {
      if (typeof binding.bearerToken !== "string" || binding.bearerToken.trim().length === 0) throw new Error("Router Node event binding is invalid.");
      const digest = createHash("sha256").update(binding.bearerToken).digest(), tokenKey = digest.toString("hex");
      if (seenTokens.has(tokenKey)) throw new Error("Router Node event binding is invalid.");
      seenTokens.add(tokenKey);
      let authenticatedNode: NodeAuthenticatedContext;
      try { authenticatedNode = validateNodeAuthenticatedContext(binding.authenticatedNode); }
      catch { throw new Error("Router Node event binding is invalid."); }
      // An empty list is a Node with no Worker deployed; the membership check below then
      // denies every origin.
      if (!Array.isArray(binding.allowedOrigins)) throw new Error("Router Node event binding is invalid.");
      const origins: WorkerExecutionIdentity[] = [];
      for (const value of binding.allowedOrigins) {
        const origin = nodeOrigin(value);
        if (origin === undefined || origin.nodeId !== authenticatedNode.nodeId || origins.some((candidate) => sameOrigin(candidate, origin))) throw new Error("Router Node event binding is invalid.");
        origins.push(origin);
      }
      return Object.freeze({ digest, authenticatedNode, allowedOrigins: Object.freeze(origins) });
    });
  }
  authenticate(request: IncomingMessage): NormalizedNodeEventBinding {
    const values = headers(request, "authorization"), value = values.length === 1 ? values[0] : undefined, prefix = "Bearer ";
    if (value === undefined || !value.startsWith(prefix) || value.length === prefix.length) failure("axis.router.unauthorized", 401);
    const digest = createHash("sha256").update(value.slice(prefix.length)).digest();
    let authenticated: NormalizedNodeEventBinding | undefined;
    for (const binding of this.#bindings) if (timingSafeEqual(binding.digest, digest)) authenticated = binding;
    if (authenticated === undefined) failure("axis.router.unauthorized", 401);
    return authenticated;
  }
}

/**
 * The Router's listener: its only route is the Node event door, and each Node event
 * binding's token is the only credential it accepts.
 */
export async function startRouterHttpServiceWithNodeEvents(options: RouterNodeEventHttpOptions): Promise<RouterHttpService> {
  return start(options, new RouterNodeEventBindingRegistry(options.nodeEventBindings));
}

/** The Node event door is the only route; any other path is a 404. */
async function start(options: RouterHttpOptions, nodeEvents: RouterNodeEventBindingRegistry): Promise<RouterHttpService> {
  const server = createServer((request, response) => { void handle(request, response).catch((error: unknown) => { if (!response.headersSent) { const mapped = map(error); send(response, mapped.status, { error: { code: mapped.code, message: messages[mapped.code] } }); } else response.destroy(); }); });
  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const rawTarget = request.url ?? "/", queryStart = rawTarget.indexOf("?"), rawPath = queryStart === -1 ? rawTarget : rawTarget.slice(0, queryStart);
    const url = new URL(rawTarget, "http://router.invalid");
    if (rawPath !== ROUTER_NODE_EVENT_HTTP_PATH) { send(response, 404, { error: { code: "axis.router.malformed", message: messages["axis.router.malformed"] } }); return; }
    if (request.method !== "POST") { response.setHeader("allow", "POST"); send(response, 405, { error: { code: "axis.router.malformed", message: "Only POST is allowed." } }); return; }
    const binding = nodeEvents.authenticate(request);
    if (url.search !== "") failure("axis.router.malformed", 400);
    media(request);
    const input = asRecord(json(await body(request, NODE_OUTER_BODY_LIMIT_BYTES)));
    if (input === undefined || !exact(input, ["contractVersion", "nodeRef", "origin", "envelope", "recordedAt"])
      || input.contractVersion !== NODE_COMMAND_CONTRACT_VERSION || typeof input.recordedAt !== "string") failure("axis.router.malformed", 400);
    let claimedNode: NodeAuthenticatedContext;
    try { claimedNode = validateNodeAuthenticatedContext({ nodeRef: input.nodeRef, nodeId: binding.authenticatedNode.nodeId }); }
    catch { failure("axis.router.malformed", 400); }
    const origin = nodeOrigin(input.origin);
    if (origin === undefined) failure("axis.router.malformed", 400);
    if (!sameNode(claimedNode, binding.authenticatedNode) || !binding.allowedOrigins.some((candidate) => sameOrigin(candidate, origin))) failure("axis.router.conflict", 409);
    let envelope: RouterRunLifecycleEnvelope;
    try { envelope = await validateNodeEvent(input.envelope); }
    catch { failure("axis.router.protocol_violation", 502); }
    const receipt = await options.router.ingestNodeRunEvent({ envelope, recordedAt: input.recordedAt, origin: { authentication: "authenticated", node: binding.authenticatedNode, identity: origin } });
    if (receipt.status === "quarantined") failure("axis.router.conflict", 409);
    const result: RouterNodeEventHttpResponse = {
      contractVersion: NODE_COMMAND_CONTRACT_VERSION,
      nodeRef: binding.authenticatedNode.nodeRef,
      sourceMessageId: receipt.sourceMessageId,
      runId: nodeEventRunId(envelope),
      status: receipt.status,
      replayed: receipt.replayed,
    };
    send(response, receipt.replayed ? 200 : 202, result);
  }
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(options.port ?? 0, options.host ?? "127.0.0.1", () => { server.off("error", reject); resolve(); }); });
  const address = server.address(); if (address === null || typeof address === "string") throw new Error("Router listener address is unavailable."); const url = new URL(`http://${address.address.includes(":") ? `[${address.address}]` : address.address}:${address.port}`);
  return { server, url, close: async () => new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error))) };
}

function map(error: unknown): { readonly code: HttpErrorCode; readonly status: number } { if (error instanceof RouterHttpError) return { code: error.code, status: error.status }; if (error instanceof RouterError) { switch (error.code) { case "axis.router.malformed": return { code: error.code, status: 400 }; case "axis.router.intent_conflict": case "axis.router.conflict": case "axis.router.not_found": return { code: error.code === "axis.router.not_found" ? "axis.router.conflict" : error.code, status: 409 }; case "axis.router.protocol_violation": return { code: error.code, status: 502 }; case "axis.router.recovery_required": case "axis.router.worker_rejected": return { code: error.code, status: 503 }; } } return { code: "axis.router.recovery_required", status: 503 }; }
