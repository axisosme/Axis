import { randomUUID } from "node:crypto";
import {
  linearExecutionBodyLimitBytes, linearExecutionContractVersion, linearExecutionCredentialPath,
  validateLinearExecutionCredentialRequest, validateLinearExecutionCredentialResponse,
  type LinearExecutionIdentity, type LinearExecutionCredentialIssued,
} from "@axisosme/axis-contracts/linear-execution";

export class LinearExecutionClientError extends Error {
  constructor(readonly code: string = "linear.execution.unavailable") {
    super(`Linear execution unavailable (${code}).`); this.name = "LinearExecutionClientError";
  }
}

/** Configuration is operator-owned. Never derive an address from prompts or task payloads. */
export function linearRelayUrl(value: string | URL): URL {
  try {
    const url = new URL(value);
    if (url.username || url.password || url.search || url.hash || url.pathname !== "/"
      || (url.protocol !== "https:" && !(url.protocol === "http:" && ["127.0.0.1", "[::1]"].includes(url.hostname)))) throw new Error();
    return url;
  } catch { throw new LinearExecutionClientError("linear.execution.invalid_request"); }
}

/** Bounded body read for both credential and GraphQL clients. Cancels on overflow/timeout. */
export async function linearJsonBody(response: Response, signal: AbortSignal, limit: number): Promise<unknown> {
  if (response.redirected || !/^application\/json(?:\s*;.*)?$/iu.test(response.headers.get("content-type") ?? "")) throw new Error();
  const reader = response.body?.getReader();
  if (reader === undefined) throw new Error();
  const cancel = () => { void reader.cancel().catch(() => undefined); };
  signal.addEventListener("abort", cancel, { once: true });
  try {
    const chunks: Uint8Array[] = []; let length = 0;
    for (;;) {
      if (signal.aborted) throw new Error();
      const result = await reader.read();
      if (signal.aborted) throw new Error();
      if (result.done) break;
      length += result.value.length;
      if (length > limit) { cancel(); throw new Error(); }
      chunks.push(result.value);
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
  } finally { signal.removeEventListener("abort", cancel); reader.releaseLock(); }
}

export interface LinearExecutionClientOptions {
  readonly relay: URL | string;
  /** Dedicated Worker receiver authentication. Never inherited by an executor. */
  readonly receiverToken: string;
  readonly fetch?: typeof fetch;
  readonly now?: () => number;
}

export class LinearExecutionClient {
  readonly #options: LinearExecutionClientOptions;
  readonly #relay: URL;
  constructor(options: LinearExecutionClientOptions) {
    this.#relay = linearRelayUrl(options.relay);
    if (!options.receiverToken || /\s/u.test(options.receiverToken)) throw new LinearExecutionClientError();
    this.#options = options;
  }
  async issue(execution: LinearExecutionIdentity): Promise<LinearExecutionCredentialIssued> {
    const result = await this.#request("issue", execution);
    if (result.status !== "issued") throw new LinearExecutionClientError();
    return result;
  }
  async revoke(execution: LinearExecutionIdentity): Promise<void> { await this.#request("revoke", execution); }
  async #request(operation: "issue" | "revoke", execution: LinearExecutionIdentity) {
    try {
      const request = validateLinearExecutionCredentialRequest({ contractVersion: linearExecutionContractVersion, requestId: randomUUID(), operation, execution });
      const signal = AbortSignal.timeout(10_000);
      const response = await (this.#options.fetch ?? fetch)(new URL(linearExecutionCredentialPath, this.#relay), {
        method: "POST", headers: { authorization: `Bearer ${this.#options.receiverToken}`, "content-type": "application/json" },
        body: JSON.stringify(request), redirect: "error", signal,
      });
      const result = validateLinearExecutionCredentialResponse(await linearJsonBody(response, signal, linearExecutionBodyLimitBytes), request, (this.#options.now ?? Date.now)());
      if (result.status === "denied") throw new LinearExecutionClientError(result.code);
      if (response.status !== 200) throw new LinearExecutionClientError();
      return result;
    } catch (error) { throw error instanceof LinearExecutionClientError ? error : new LinearExecutionClientError(); }
  }
}

/**
 * Why this Run has no Linear execution access. `disabled` means the Worker was
 * never configured for it; `unavailable` means the Worker is configured but
 * this Run carries no Linear authority. Credential expiry and authorization
 * refusal are decided by Relay per call, not at process start, and reach the
 * agent as the Relay refusal code.
 */
export type LinearExecutionUnavailableReason = "disabled" | "unavailable";

/** Nonempty key overrides personal CLI configuration even while the feature is disabled. */
export function unavailableLinearEnvironment(reason: LinearExecutionUnavailableReason): Readonly<Record<string, string>> {
  return Object.freeze({ LINEAR_API_KEY: "axis-linear-execution-unavailable", LINEAR_GRAPHQL_ENDPOINT: "http://127.0.0.1:1/axis-linear-unavailable", LINEAR_EXECUTION_STATUS: reason });
}

/** Copy, never mutate process.env. Relay transport/OAuth secrets do not reach native children. */
export function linearExecutionEnvironment(base: Readonly<Record<string, string | undefined>>, relay?: string | URL, issued?: LinearExecutionCredentialIssued, reason: LinearExecutionUnavailableReason = "unavailable"): Record<string, string> {
  // Every value written below is a string: the copy skips undefined entries and
  // both assignments are string-valued. Saying so in the type spares each caller
  // a narrowing step that could only ever be a no-op.
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(base)) {
    if (!/^AXIS(?:_)/u.test(key) && !/^LINEAR_/u.test(key) && key !== "NODE_AUTH_TOKEN" && value !== undefined) env[key] = value;
  }
  Object.assign(env, unavailableLinearEnvironment(reason));
  if (relay !== undefined && issued !== undefined) {
    // The full response binding is checked by the client before this projection.
    if (!/^axlea1_[A-Za-z0-9_-]{43}$/u.test(issued.credential) || Date.parse(issued.expiresAt) <= Date.now()) throw new LinearExecutionClientError();
    Object.assign(env, { LINEAR_API_KEY: issued.credential, LINEAR_GRAPHQL_ENDPOINT: new URL("/api/linear/graphql", linearRelayUrl(relay)).toString(), LINEAR_EXECUTION_STATUS: "available", LINEAR_EXECUTION_EXPIRES_AT: issued.expiresAt,
      LINEAR_EXECUTION_SDK_MODULE: new URL("./linear-sdk.js", import.meta.url).href });
  }
  return env;
}
