import { LinearCliAccess, type LinearCliGrant } from "./cli-access.js";
import type { LinearChannel } from "./composition.js";
import { LinearExecutionAccess } from "./execution-access.js";
import type { LinearExecutionAuthorityPort } from "./execution-access-contract.js";
import { LinearApiProxy, type LinearApiAudit } from "./api-proxy.js";
import type { LinearProviderPort } from "./provider.js";

export interface LinearApiRuntimeConfig { readonly timeoutMs: number; readonly cliGrants?: readonly LinearCliGrant[]; }

export interface LinearApiRuntimeOptions {
  readonly channel: Pick<LinearChannel, "installations" | "oauth">;
  readonly provider: Pick<LinearProviderPort, "resolveViewer">;
  /** Current, independent business grants. `null` refuses all access. */
  readonly executions: LinearExecutionAuthorityPort | null;
  readonly audit: (event: LinearApiAudit) => void;
  readonly fetch?: typeof fetch;
}

export interface LinearApiRuntime {
  readonly proxy: LinearApiProxy;
  /** Trusted local issuer only. Never place this object or its results in a public response/log. */
  readonly access: LinearExecutionAccess | null;
  close(): void;
}

export function composeLinearApiRuntime(config: LinearApiRuntimeConfig | null, options: LinearApiRuntimeOptions): LinearApiRuntime | null {
  if (config === null) return null;
  const access = options.executions === null ? null : new LinearExecutionAccess({
    executions: options.executions, installations: options.channel.installations,
    credentials: options.channel.oauth, provider: options.provider,
  });
  const cli = config.cliGrants === undefined ? null : new LinearCliAccess({ grants: config.cliGrants,
    installations: options.channel.installations, credentials: options.channel.oauth, provider: options.provider });
  const combined = cli === null ? access : {
    verify: (credential: unknown) => typeof credential === "string" && credential.startsWith("axlcli1_") ? cli.verify(credential)
      : access?.verify(credential) ?? cli.verify(credential),
    withAccess: <T>(credential: unknown, operation: (token: string, context: import("./api-proxy.js").LinearApiContext) => Promise<T>) =>
      typeof credential === "string" && credential.startsWith("axlcli1_") ? cli.withAccess(credential, operation)
        : access?.withAccess(credential, operation) ?? cli.withAccess(credential, operation),
  };
  const proxy = new LinearApiProxy({ access: combined, audit: options.audit, timeoutMs: config.timeoutMs,
    ...(options.fetch === undefined ? {} : { fetch: options.fetch }) });
  return { proxy, access, close: () => { access?.close(); cli?.close(); } };
}
