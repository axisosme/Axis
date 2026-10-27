import { createHash, timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

import {
  NODE_ERROR_STATUS,
  NODE_OUTER_BODY_LIMIT_BYTES,
  NodeContractError,
  nodeError,
  validateNodeTransportBody,
} from "../shared/router-node.js";

export function failNode(code: NodeContractError["code"] = "axis.node.malformed"): never {
  throw new NodeContractError(code);
}

/** Split the untouched HTTP request target once and match only an exact raw path. */
export function matchRawNodeRoute<Route extends string>(request: IncomingMessage, routes: readonly Route[]): {
  readonly route: Route | undefined;
  readonly query: string;
} {
  const target = request.url ?? "/";
  const separator = target.indexOf("?");
  const path = separator === -1 ? target : target.slice(0, separator);
  return {
    route: routes.find((candidate) => candidate === path),
    query: separator === -1 ? "" : target.slice(separator + 1),
  };
}

export function rawHeaderValues(request: IncomingMessage, name: string): readonly string[] {
  const values: string[] = [];
  for (let index = 0; index < request.rawHeaders.length; index += 2) {
    if (request.rawHeaders[index]?.toLowerCase() === name) values.push(request.rawHeaders[index + 1] ?? "");
  }
  return values;
}

export function secureDigest(value: string): Buffer {
  return createHash("sha256").update(value).digest();
}

export function secureDigestEqual(left: Buffer, right: Buffer): boolean {
  return timingSafeEqual(left, right);
}

export function secureEqual(left: string, right: string): boolean {
  return secureDigestEqual(secureDigest(left), secureDigest(right));
}

function jsonMedia(request: IncomingMessage): { readonly contentType: string } {
  const values = rawHeaderValues(request, "content-type");
  const contentEncoding = rawHeaderValues(request, "content-encoding");
  const contentType = values[0] ?? "";
  if (values.length !== 1 || contentEncoding.length !== 0
    || !/^application\/json(?:\s*;\s*charset=utf-8)?$/iu.test(contentType.trim())) {
    failNode("axis.node.unsupported_media");
  }
  return { contentType };
}

async function outerBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  let oversized = false;
  for await (const item of request.iterator({ destroyOnReturn: false })) {
    const chunk = Buffer.isBuffer(item) ? item : Buffer.from(item);
    bytes += chunk.byteLength;
    if (bytes > NODE_OUTER_BODY_LIMIT_BYTES) oversized = true;
    else chunks.push(chunk);
  }
  if (oversized) failNode("axis.node.body_too_large");
  return Buffer.concat(chunks);
}

export async function readNodeJsonBody(request: IncomingMessage): Promise<unknown> {
  const media = jsonMedia(request);
  return validateNodeTransportBody(await outerBody(request), media);
}

export async function enforceNoNodeBody(request: IncomingMessage): Promise<void> {
  if (rawHeaderValues(request, "content-type").length !== 0 || rawHeaderValues(request, "content-encoding").length !== 0) failNode();
  for await (const item of request) {
    if ((Buffer.isBuffer(item) ? item.byteLength : Buffer.byteLength(item)) !== 0) failNode();
  }
}

export function sendNodeJson(response: ServerResponse, status: number, value: unknown): void {
  const encoded = JSON.stringify(value);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(encoded),
    "cache-control": "no-store",
  });
  response.end(encoded);
}

export function sendNodeError(
  response: ServerResponse,
  code: NodeContractError["code"],
  status = NODE_ERROR_STATUS[code],
): void {
  sendNodeJson(response, status, nodeError(code));
}

export function sendNodeFailure(response: ServerResponse, error: unknown): void {
  sendNodeError(response, error instanceof NodeContractError ? error.code : "axis.node.storage_unavailable");
}

export function numericLoopback(host: string): boolean {
  if (host === "::1") return true;
  const pieces = host.split(".");
  return pieces.length === 4 && pieces[0] === "127"
    && pieces.every((piece) => /^(?:0|[1-9][0-9]{0,2})$/u.test(piece) && Number(piece) <= 255);
}
