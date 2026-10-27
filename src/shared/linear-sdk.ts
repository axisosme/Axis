import { LinearSdk } from "@linear/sdk";
import { getOperationAST, parse } from "graphql";
import { linearJsonBody, linearRelayUrl } from "./linear-execution-client.js";

export interface AxisLinearSdkOptions {
  readonly endpoint: string;
  readonly credential: string;
  readonly expiresAt?: string;
  readonly fetch?: typeof fetch;
  readonly timeoutMs?: number;
}

/** No query, variables, credential or native fetch exception is retained in diagnostics. */
export class AxisLinearRequestError extends Error {
  constructor(readonly code: string, readonly outcome: "rejected" | "partial" | "unknown", readonly status: number | null,
    readonly requestId: string | null, readonly data?: unknown, readonly errors?: readonly unknown[],
    readonly retryAfter: string | null = null) {
    super(outcome === "unknown" ? "Linear write outcome is unknown. Inspect prior effects before another write."
      : outcome === "partial" ? "Linear returned partial results. Inspect data and errors before another write." : `Linear request failed (${code}).`);
    this.name = "AxisLinearRequestError";
  }
}

function object(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

/** Official generated models retain their request callback, including lazy relationship queries. */
export class AxisLinearSdk extends LinearSdk {
  constructor(options: AxisLinearSdkOptions) {
    let endpoint: URL;
    try {
      endpoint = new URL(options.endpoint);
      const origin = linearRelayUrl(endpoint.origin);
      if (endpoint.href !== new URL("/api/linear/graphql", origin).href || !/^axlea1_[A-Za-z0-9_-]{43}$/u.test(options.credential)) throw new Error();
    } catch { throw new AxisLinearRequestError("linear.execution.invalid_request", "rejected", null, null); }
    const timeout = options.timeoutMs ?? 30_000;
    if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 120_000) throw new AxisLinearRequestError("linear.execution.invalid_request", "rejected", null, null);
    const expires = options.expiresAt === undefined ? undefined : Date.parse(options.expiresAt);
    if (expires !== undefined && !Number.isSafeInteger(expires)) throw new AxisLinearRequestError("linear.execution.invalid_request", "rejected", null, null);
    const request = async <Result, Variables extends Record<string, unknown>>(query: string, variables?: Variables): Promise<Result> => {
      let mutation: boolean;
      try {
        const operation = getOperationAST(parse(query));
        if (operation == null || operation.operation === "subscription") throw new Error();
        mutation = operation.operation === "mutation";
      } catch { throw new AxisLinearRequestError("linear.proxy.invalid_request", "rejected", null, null); }
      if (expires !== undefined && Date.now() >= expires) throw new AxisLinearRequestError("linear.execution.expired", "rejected", null, null);
      const signal = AbortSignal.timeout(timeout);
      let response: Response; let body: Record<string, unknown> | null;
      try {
        response = await (options.fetch ?? fetch)(endpoint, {
          method: "POST", headers: { authorization: `Bearer ${options.credential}`, "content-type": "application/json" },
          body: JSON.stringify({ query, ...(variables === undefined ? {} : { variables }) }), redirect: "error", signal,
        });
        body = object(await linearJsonBody(response, signal, 8 * 1024 * 1024));
        if (body === null || (!Object.hasOwn(body, "data") && !Array.isArray(body.errors))) throw new Error();
      } catch { throw new AxisLinearRequestError("linear.proxy.transport_error", mutation ? "unknown" : "rejected", null, null); }
      const errors = Array.isArray(body.errors) ? body.errors : [];
      if (!response.ok || errors.length > 0 || body.data === null || body.data === undefined) {
        const extension = object(object(errors[0])?.extensions);
        const rawCode = extension?.code;
        const code = typeof rawCode === "string" && /^linear\.(?:execution|proxy)\.[a-z_]{1,48}$/u.test(rawCode) ? rawCode : "linear.proxy.graphql_error";
        const rawRequestId = response.headers.get("x-axis-request-id");
        const requestId = rawRequestId !== null && /^[A-Za-z0-9._:-]{1,128}$/u.test(rawRequestId) ? rawRequestId : null;
        const unknown = mutation && (code === "linear.proxy.outcome_unknown" || response.headers.get("x-axis-linear-outcome") === "outcome_unknown");
        const partial = errors.length > 0 && body.data !== null && body.data !== undefined;
        const retryAfter = response.headers.get("retry-after");
        throw new AxisLinearRequestError(code, unknown ? "unknown" : partial ? "partial" : "rejected", response.status, requestId, body.data, errors,
          retryAfter !== null && /^\d{1,12}$/u.test(retryAfter) ? retryAfter : null);
      }
      return body.data as Result;
    };
    super(request);
  }
}

/** Reads only the injected execution variables; there is no personal SDK/CLI configuration lookup. */
export function axisLinearSdkFromEnvironment(env: Readonly<Record<string, string | undefined>> = process.env): AxisLinearSdk {
  if (env.LINEAR_EXECUTION_STATUS !== "available" || !env.LINEAR_GRAPHQL_ENDPOINT || !env.LINEAR_API_KEY) {
    throw new AxisLinearRequestError("linear.execution.unavailable", "rejected", null, null);
  }
  return new AxisLinearSdk({ endpoint: env.LINEAR_GRAPHQL_ENDPOINT, credential: env.LINEAR_API_KEY,
    ...(env.LINEAR_EXECUTION_EXPIRES_AT === undefined ? {} : { expiresAt: env.LINEAR_EXECUTION_EXPIRES_AT }) });
}
