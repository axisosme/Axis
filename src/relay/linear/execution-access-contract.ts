/** Relay-private TypeScript contract for Linear execution access. Not a Runtime v1 wire extension. */
export const LINEAR_EXECUTION_ACCESS_VERSION = "1.0.0";
export const LINEAR_EXECUTION_DEFAULT_TTL_MS = 5 * 60_000;
export const LINEAR_EXECUTION_MAX_TTL_MS = 15 * 60_000;

/**
 * Supplied only by trusted Relay composition from admitted, immutable execution
 * evidence. Node authentication, request fields, and prompts cannot implement
 * this authority. Inactive execution IDs must never become active again.
 */
export interface LinearExecutionGrant {
  readonly executionId: string;
  readonly taskId: string;
  readonly attemptId: string;
  readonly runId: string;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly sourceTeamId: string;
  /** Captured when this execution's business access is approved, not on a client request. */
  readonly installationRevision: number;
  readonly state: "active" | "inactive";
  /** Explicit consent to all resources visible to this installation in its workspace. */
  readonly scope: "installation-workspace";
}

export interface LinearExecutionAuthorityPort {
  resolve(executionId: string): LinearExecutionGrant | null;
}

/** Safe internal projection. Contains no bearer, vault reference, or OAuth token. */
export interface LinearExecutionAccessContext {
  readonly contractVersion: typeof LINEAR_EXECUTION_ACCESS_VERSION;
  readonly credentialId: string;
  readonly execution: LinearExecutionGrant;
  readonly expiresAt: string;
}

export type LinearExecutionAccessCode =
  | "linear.execution.invalid_credential"
  | "linear.execution.expired"
  | "linear.execution.revoked"
  | "linear.execution.execution_denied"
  | "linear.execution.installation_denied"
  | "linear.execution.authorization_changed"
  | "linear.execution.workspace_mismatch"
  | "linear.execution.invalid_request"
  | "linear.execution.capacity"
  | "linear.execution.unavailable";

export interface LinearExecutionAccessFailure {
  readonly authorized: false;
  readonly status: 400 | 401 | 403 | 503;
  readonly error: {
    readonly code: LinearExecutionAccessCode;
    readonly message: string;
  };
}

export type LinearExecutionAccessResult<T> =
  | { readonly authorized: true; readonly value: T }
  | LinearExecutionAccessFailure;

/** Secret-bearing issuance result: deliver once over a protected execution-specific channel. */
export interface LinearExecutionCredential {
  readonly credential: string;
  readonly context: LinearExecutionAccessContext;
}

/** Closed, safe logging surface; never attach the request/result or caught exception. */
export interface LinearExecutionAccessAudit {
  readonly action: "issue" | "verify" | "use" | "revoke";
  readonly outcome: "authorized" | LinearExecutionAccessCode;
  readonly credentialId: string | null;
}
