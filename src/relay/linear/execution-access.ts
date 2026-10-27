import { createHash, randomBytes, randomUUID } from "node:crypto";

import { authorizeLinearAuthority } from "./authorization.js";
import {
  LINEAR_EXECUTION_ACCESS_VERSION, LINEAR_EXECUTION_DEFAULT_TTL_MS, LINEAR_EXECUTION_MAX_TTL_MS,
  type LinearExecutionAccessAudit, type LinearExecutionAccessCode, type LinearExecutionAccessContext,
  type LinearExecutionAccessFailure, type LinearExecutionAccessResult, type LinearExecutionAuthorityPort,
  type LinearExecutionCredential, type LinearExecutionGrant,
} from "./execution-access-contract.js";
import type { LinearExecutionInstallationPort } from "./installation.js";
import type { LinearProviderPort } from "./provider.js";
import type { LinearCredentialAccess } from "../oauth.js";

interface CredentialRecord {
  readonly context: LinearExecutionAccessContext;
  readonly expiresAtMs: number;
  readonly issuedAtMs: number;
  readonly credentialRef: string;
  refusal?: LinearExecutionAccessCode;
}

export interface LinearExecutionAccessOptions {
  readonly executions: LinearExecutionAuthorityPort;
  readonly installations: LinearExecutionInstallationPort;
  readonly credentials: Pick<LinearCredentialAccess, "ready" | "accessToken">;
  readonly provider: Pick<LinearProviderPort, "resolveViewer">;
  readonly now?: () => Date;
  readonly maxCredentials?: number;
  readonly audit?: (event: LinearExecutionAccessAudit) => void;
}

const tokenPattern = /^axlea1_[A-Za-z0-9_-]{43}$/;
const idPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$(?![\s\S])/;
const grantIds = ["executionId", "taskId", "attemptId", "runId", "organizationId", "appUserId", "sourceTeamId"] as const;
const grantKeys = [...grantIds, "installationRevision", "state", "scope"];

function grant(value: LinearExecutionGrant | null, executionId: string): LinearExecutionGrant | null {
  if (value === null || typeof value !== "object" || Object.keys(value).length !== grantKeys.length
    || Object.keys(value).some((key) => !grantKeys.includes(key))
    || grantIds.some((key) => typeof value[key] !== "string" || !idPattern.test(value[key]))
    || value.executionId !== executionId || value.state !== "active" || value.scope !== "installation-workspace"
    || !Number.isSafeInteger(value.installationRevision) || value.installationRevision < 1) return null;
  return Object.freeze({
    executionId: value.executionId, taskId: value.taskId, attemptId: value.attemptId, runId: value.runId,
    organizationId: value.organizationId, appUserId: value.appUserId, sourceTeamId: value.sourceTeamId,
    installationRevision: value.installationRevision, state: value.state, scope: value.scope,
  });
}

function failure(code: LinearExecutionAccessCode): LinearExecutionAccessFailure {
  const status = code === "linear.execution.invalid_request" ? 400
    : code === "linear.execution.unavailable" || code === "linear.execution.capacity" ? 503
    : code === "linear.execution.invalid_credential" || code === "linear.execution.expired" || code === "linear.execution.revoked" ? 401 : 403;
  return { authorized: false, status, error: { code, message: "Linear execution access was refused." } };
}

function digest(credential: string): string {
  return createHash("sha256").update(credential).digest("hex");
}

/**
 * Relay-only owner of execution bearer credentials. Only hashes and safe binding state are retained,
 * and restart or disposal invalidates every bearer. Do not share across Relay replicas.
 */
export class LinearExecutionAccess {
  readonly #options: LinearExecutionAccessOptions;
  readonly #records = new Map<string, CredentialRecord>();
  readonly #revokedExecutions = new Set<string>();
  readonly #now: () => Date;
  readonly #capacity: number;
  #closed = false;

  constructor(options: LinearExecutionAccessOptions) {
    this.#options = options;
    this.#now = options.now ?? (() => new Date());
    this.#capacity = options.maxCredentials ?? 10_000;
    if (!Number.isSafeInteger(this.#capacity) || this.#capacity < 1 || this.#capacity > 100_000) {
      throw new Error("Invalid Linear execution access capacity.");
    }
  }

  /** Trusted issuer only. It supplies no installation selector or client claims. */
  issue(executionId: string, ttlMs = LINEAR_EXECUTION_DEFAULT_TTL_MS): LinearExecutionAccessResult<LinearExecutionCredential> {
    try {
      if (this.#closed) return this.#deny("issue", "linear.execution.unavailable");
      if (typeof executionId !== "string" || !idPattern.test(executionId)
        || !Number.isSafeInteger(ttlMs) || ttlMs < 1 || ttlMs > LINEAR_EXECUTION_MAX_TTL_MS) {
        return this.#deny("issue", "linear.execution.invalid_request");
      }
      if (this.#revokedExecutions.has(executionId)) return this.#deny("issue", "linear.execution.revoked");
      const execution = grant(this.#options.executions.resolve(executionId), executionId);
      if (execution === null) return this.#deny("issue", "linear.execution.execution_denied");
      const installation = this.#options.installations.executionAuthority(execution.organizationId, execution.appUserId, execution.sourceTeamId);
      if (authorizeLinearAuthority(installation).disposition !== "authorized" || installation === null) {
        return this.#deny("issue", "linear.execution.installation_denied");
      }
      if (installation.revision !== execution.installationRevision) return this.#deny("issue", "linear.execution.authorization_changed");
      const now = this.#instant();
      for (const [key, record] of this.#records) if (record.expiresAtMs <= now) this.#records.delete(key);
      if (this.#records.size + this.#revokedExecutions.size >= this.#capacity) return this.#deny("issue", "linear.execution.capacity");
      const credential = `axlea1_${randomBytes(32).toString("base64url")}`;
      const context: LinearExecutionAccessContext = Object.freeze({
        contractVersion: LINEAR_EXECUTION_ACCESS_VERSION, credentialId: randomUUID(), execution,
        expiresAt: new Date(now + ttlMs).toISOString(),
      });
      this.#records.set(digest(credential), { context, issuedAtMs: now, expiresAtMs: now + ttlMs, credentialRef: installation.credentialRef });
      this.#audit("issue", "authorized", context.credentialId);
      return { authorized: true, value: { credential, context } };
    } catch {
      return this.#deny("issue", "linear.execution.unavailable");
    }
  }

  /** Expected workspace can only narrow authority; installation always comes from the bearer binding. */
  verify(credential: unknown, expectedOrganizationId?: string): LinearExecutionAccessResult<LinearExecutionAccessContext> {
    const checked = this.#check(credential, expectedOrganizationId);
    if (!checked.authorized) return this.#deny("verify", checked.error.code);
    this.#audit("verify", "authorized", checked.value.context.credentialId);
    return { authorized: true, value: checked.value.context };
  }

  /**
   * Only a trusted Relay operation may receive the upstream token. No client
   * callback or context is accepted. Rechecks after OAuth/identity awaits close
   * revocation races before handoff. Once operation begins it is in flight and
   * cannot be undone; this method never retries it. Do not serialize its token.
   */
  async withAccess<T>(credential: unknown, operation: (accessToken: string, context: LinearExecutionAccessContext) => Promise<T>,
    expectedOrganizationId?: string): Promise<LinearExecutionAccessResult<T>> {
    let checked = this.#check(credential, expectedOrganizationId);
    if (!checked.authorized) return this.#deny("use", checked.error.code);
    const record = checked.value;
    try {
      if (!this.#options.credentials.ready) return this.#deny("use", "linear.execution.unavailable", record);
      const token = await this.#options.credentials.accessToken(record.credentialRef);
      if (typeof token !== "string" || token.length === 0) return this.#deny("use", "linear.execution.unavailable", record);
      checked = this.#check(credential, expectedOrganizationId);
      if (!checked.authorized) return this.#deny("use", checked.error.code, record);
      const viewer = await this.#options.provider.resolveViewer(token);
      if (viewer.organizationId !== record.context.execution.organizationId || viewer.appUserId !== record.context.execution.appUserId) {
        record.refusal = "linear.execution.authorization_changed";
        return this.#deny("use", record.refusal, record);
      }
      checked = this.#check(credential, expectedOrganizationId);
      if (!checked.authorized) return this.#deny("use", checked.error.code, record);
      // No callbacks/awaits between final authority check and trusted operation.
      const value = await operation(token, record.context);
      this.#audit("use", "authorized", record.context.credentialId);
      return { authorized: true, value };
    } catch {
      // The operation may already have taken effect, so it is never retried here.
      return this.#deny("use", "linear.execution.unavailable", record);
    }
  }

  /** Idempotent management operation on a non-secret ID, never on a bearer. */
  revoke(credentialId: string): void {
    for (const record of this.#records.values()) {
      if (record.context.credentialId === credentialId) record.refusal = "linear.execution.revoked";
    }
    this.#audit("revoke", "linear.execution.revoked", null);
  }

  /**
   * Permanently closes issuance for an execution known to this owner. Unknown
   * IDs are inert. The trusted authority must retain closure across restarts.
   */
  revokeExecution(executionId: string): void {
    for (const record of this.#records.values()) {
      if (record.context.execution.executionId === executionId) {
        record.refusal = "linear.execution.revoked";
        this.#revokedExecutions.add(executionId);
      }
    }
    this.#audit("revoke", "linear.execution.revoked", null);
  }

  close(): void {
    this.#closed = true;
    this.#records.clear();
    this.#revokedExecutions.clear();
  }

  #check(credential: unknown, expectedOrganizationId?: string): LinearExecutionAccessResult<CredentialRecord> {
    try {
      if (typeof credential !== "string" || !tokenPattern.test(credential)) return failure("linear.execution.invalid_credential");
      const record = this.#records.get(digest(credential));
      if (record === undefined) return failure("linear.execution.invalid_credential");
      if (record.refusal !== undefined) return failure(record.refusal);
      const now = this.#instant();
      if (now < record.issuedAtMs || now >= record.expiresAtMs) {
        record.refusal = "linear.execution.expired";
        return failure(record.refusal);
      }
      const execution = record.context.execution;
      if (expectedOrganizationId !== undefined && expectedOrganizationId !== execution.organizationId) return failure("linear.execution.workspace_mismatch");
      const current = grant(this.#options.executions.resolve(execution.executionId), execution.executionId);
      if (current === null || JSON.stringify(current) !== JSON.stringify(execution)) {
        record.refusal = "linear.execution.execution_denied";
        return failure(record.refusal);
      }
      const installation = this.#options.installations.executionAuthority(execution.organizationId, execution.appUserId, execution.sourceTeamId);
      if (installation === null || authorizeLinearAuthority(installation).disposition !== "authorized") {
        record.refusal = "linear.execution.installation_denied";
        return failure(record.refusal);
      }
      if (installation.revision !== execution.installationRevision || installation.credentialRef !== record.credentialRef) {
        record.refusal = "linear.execution.authorization_changed";
        return failure(record.refusal);
      }
      return { authorized: true, value: record };
    } catch {
      return failure("linear.execution.unavailable");
    }
  }

  #instant(): number {
    const now = this.#now().getTime();
    if (!Number.isSafeInteger(now)) throw new Error("Invalid Linear execution access clock.");
    return now;
  }

  #deny(action: LinearExecutionAccessAudit["action"], code: LinearExecutionAccessCode, record?: CredentialRecord): LinearExecutionAccessFailure {
    this.#audit(action, code, record?.context.credentialId ?? null);
    return failure(code);
  }

  #audit(action: LinearExecutionAccessAudit["action"], outcome: LinearExecutionAccessAudit["outcome"], credentialId: string | null): void {
    try { this.#options.audit?.(Object.freeze({ action, outcome, credentialId })); } catch { /* Never expose logger diagnostics or change authority. */ }
  }
}
