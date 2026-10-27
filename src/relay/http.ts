import { timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import { RelayError, safeError } from "./errors.js";
import type { LinearChannelDiagnostics } from "./linear/composition.js";
import { LINEAR_API_PATH, type LinearApiProxy } from "./linear/api-proxy.js";
import { ingestLinearWebhook, type LinearIngressOptions } from "./linear/ingress.js";
import type { LinearHeaders } from "./types.js";
import { linearExecutionCredentialPath } from "@axisosme/axis-contracts/linear-execution";
import type { LinearExecutionCredentialEndpoint } from "./linear/execution-runtime.js";

export interface RelayHttpOptions {
  readonly linearExecution?: LinearExecutionCredentialEndpoint;
  /** The Linear webhook ingress. Required: this process exists to run Linear. */
  readonly linear: LinearIngressOptions;
  /**
   * Answers the unauthenticated `GET /health/ready` per request, so it must
   * cost the same on an empty and a full database. Anything whose cost grows
   * with the data belongs in `diagnostics`.
   */
  readonly ready: () => boolean | Promise<boolean>;
  /**
   * Answers `GET /health/diagnostics`: whole-database integrity scans, called
   * only after the operator token is checked.
   */
  readonly diagnostics: () => LinearChannelDiagnostics;
  readonly oauth: {
    begin(returnTo?: string): string;
    callback(code: string, state: string): Promise<string>;
  };
  readonly operatorToken: string;
  readonly host?: string;
  readonly port?: number;
  readonly maxBodyBytes?: number;
  /** Omitted when disabled; the proxy owns its execution auth and bounded HTTP protocol. */
  readonly linearApi?: LinearApiProxy;
}

export interface RelayHttpService {
  readonly server: Server;
  readonly url: URL;
  close(): Promise<void>;
}

function secureEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}

function sendJson(response: ServerResponse, status: number, body: Readonly<Record<string, unknown>>): void {
  const encoded = JSON.stringify(body);
  response.writeHead(status, { "content-type": "application/json; charset=utf-8", "content-length": Buffer.byteLength(encoded) });
  response.end(encoded);
}

async function readBody(
  request: IncomingMessage,
  maxBytes: number,
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > maxBytes) throw new RelayError("linear.ingress.body_too_large", "The webhook body exceeds the accepted limit.", 413);
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

function header(
  request: IncomingMessage,
  name: string,
): string {
  const value = request.headers[name.toLowerCase()];
  if (typeof value !== "string" || value.length === 0) throw new RelayError("linear.ingress.malformed", `Missing ${name} header.`, 400);
  return value;
}

export async function startRelayHttpService(options: RelayHttpOptions): Promise<RelayHttpService> {
  const maxBodyBytes = options.maxBodyBytes ?? 1024 * 1024;
  const server = createServer((request, response) => {
    void handle(request, response).catch((error: unknown) => {
      const failure = safeError(error);
      const status = error instanceof RelayError ? error.statusCode : 500;
      if (!response.headersSent) sendJson(response, status, { error: { code: failure.code, message: failure.safeMessage } });
      else response.destroy();
    });
  });

  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const url = new URL(request.url ?? "/", "http://relay.invalid");
    if (url.pathname === linearExecutionCredentialPath && options.linearExecution !== undefined) {
      await options.linearExecution.handle(request, response); return;
    }
    if (url.pathname === LINEAR_API_PATH && options.linearApi !== undefined) {
      await options.linearApi.handle(request, response);
      return;
    }
    if (request.method === "GET" && url.pathname === "/health/live") {
      sendJson(response, 200, { status: "live" });
      return;
    }
    if (request.method === "GET" && url.pathname === "/health/ready") {
      const ready = await options.ready();
      sendJson(response, ready ? 200 : 503, { status: ready ? "ready" : "not_ready" });
      return;
    }
    // The token is checked before `options.diagnostics()` runs: the scans are
    // whole-database reads an anonymous caller must never trigger.
    if (request.method === "GET" && url.pathname === "/health/diagnostics") {
      const authorization = request.headers.authorization ?? "";
      if (!secureEqual(authorization, `Bearer ${options.operatorToken}`)) {
        sendJson(response, 401, { error: { code: "linear.auth.operator_required", message: "Operator authorization is required." } });
        return;
      }
      const report = options.diagnostics();
      sendJson(response, report.healthy ? 200 : 503, {
        status: report.healthy ? "healthy" : "degraded",
        vault: report.vault,
        stores: report.stores,
      });
      return;
    }
    if (request.method === "GET" && url.pathname === "/oauth/linear/install") {
      const authorization = request.headers.authorization ?? "";
      if (!secureEqual(authorization, `Bearer ${options.operatorToken}`)) {
        sendJson(response, 401, { error: { code: "linear.auth.operator_required", message: "Operator authorization is required." } });
        return;
      }
      const location = options.oauth.begin(url.searchParams.get("return_to") ?? "/");
      response.writeHead(302, { location, "cache-control": "no-store" });
      response.end();
      return;
    }
    if (request.method === "GET" && url.pathname === "/oauth/linear/callback") {
      const returnTo = await options.oauth.callback(url.searchParams.get("code") ?? "", url.searchParams.get("state") ?? "");
      response.writeHead(303, { location: returnTo, "cache-control": "no-store" });
      response.end();
      return;
    }
    if (request.method === "POST" && url.pathname === "/webhooks/linear") {
      // The only body-size limit; `readBody` refuses mid-stream, before the
      // signature is computed.
      const rawBody = await readBody(request, maxBodyBytes);
      const headers: LinearHeaders = {
        deliveryId: header(request, "Linear-Delivery"),
        event: header(request, "Linear-Event"),
        signature: header(request, "Linear-Signature"),
        timestamp: header(request, "Linear-Timestamp"),
      };
      // Verifies and persists to the encrypted inbox, then answers; rejections
      // throw a `RelayError`. Only the ingress loop drains the inbox: nudging
      // it from here would bypass `scheduledLoop`'s in-flight guard.
      const accepted = ingestLinearWebhook(options.linear, { headers, rawBody });
      sendJson(response, 200, { accepted: accepted.accepted, duplicate: accepted.duplicate });
      return;
    }
    sendJson(response, 404, { error: { code: "axis.relay.not_found", message: "Route not found." } });
  }

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port ?? 0, options.host ?? "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("The relay listener address is unavailable.");
  const url = new URL(`http://${address.address.includes(":") ? `[${address.address}]` : address.address}:${address.port}`);
  return {
    server,
    url,
    close: async () => {
      await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
    },
  };
}
