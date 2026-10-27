import { createHash } from "node:crypto";
import type { LinearExecutionInstallationPort } from "./installation.js";
import type { LinearCredentialAccess } from "../oauth.js";
import type { LinearProviderPort } from "./provider.js";
import type { LinearApiAccess, LinearApiAccessResult } from "./api-proxy.js";

export interface LinearCliGrant {
  readonly credentialId: string;
  readonly credentialHash: string;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly expiresAt: string;
}
export interface LinearCliContext {
  readonly kind: "cli";
  readonly credentialId: string;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly expiresAt: string;
}
function denied(code: string, status: 401 | 403 | 503 = 403): LinearApiAccessResult<never> {
  return { authorized: false, status, error: { code: `linear.cli.${code}`, message: "Axis Linear CLI access was refused." } };
}

/** Installation-scoped CLI authentication, independent of Worker, Agent and Run. */
export class LinearCliAccess implements LinearApiAccess {
  readonly #grants: readonly LinearCliGrant[];
  #closed = false;
  constructor(readonly options: {
    readonly grants: readonly LinearCliGrant[];
    readonly installations: LinearExecutionInstallationPort;
    readonly credentials: Pick<LinearCredentialAccess, "ready" | "accessToken">;
    readonly provider: Pick<LinearProviderPort, "resolveViewer">;
    readonly now?: () => number;
  }) { this.#grants = structuredClone(options.grants); }
  verify(credential: unknown): LinearApiAccessResult<LinearCliContext> {
    if (this.#closed) return denied("unavailable", 503);
    if (typeof credential !== "string" || !/^axlcli1_[A-Za-z0-9_-]{43}$/u.test(credential)) return denied("invalid_credential", 401);
    const hash = createHash("sha256").update(credential).digest("hex");
    const grant = this.#grants.find(entry => entry.credentialHash === hash);
    if (grant === undefined) return denied("invalid_credential", 401);
    if (Date.parse(grant.expiresAt) <= (this.options.now ?? Date.now)()) return denied("expired", 401);
    const installation = this.options.installations.executionAuthority(grant.organizationId, grant.appUserId, "");
    // CLI consent covers the installation workspace. No source Team or Run is invented.
    if (installation === null) return denied("installation_denied");
    return { authorized: true, value: { kind: "cli", credentialId: grant.credentialId, organizationId: grant.organizationId, appUserId: grant.appUserId, expiresAt: grant.expiresAt } };
  }
  async withAccess<T>(credential: unknown, operation: (token: string, context: import("./api-proxy.js").LinearApiContext) => Promise<T>): Promise<LinearApiAccessResult<T>> {
    const checked = this.verify(credential);
    if (!checked.authorized) return checked;
    const context = checked.value;
    try {
      const read = () => this.options.installations.executionAuthority(context.organizationId, context.appUserId, "");
      const installation = read();
      if (installation === null || !this.options.credentials.ready) return denied("unavailable", 503);
      const token = await this.options.credentials.accessToken(installation.credentialRef);
      if (typeof token !== "string" || token.length === 0) return denied("unavailable", 503);
      const viewer = await this.options.provider.resolveViewer(token);
      const current = read(), verified = this.verify(credential);
      if (!verified.authorized) return verified;
      if (current === null || current.revision !== installation.revision || current.credentialRef !== installation.credentialRef
        || viewer.organizationId !== context.organizationId || viewer.appUserId !== context.appUserId) return denied("authorization_changed");
      return { authorized: true, value: await operation(token, context) as T };
    } catch { return denied("unavailable", 503); }
  }
  close(): void { this.#closed = true; }
}
