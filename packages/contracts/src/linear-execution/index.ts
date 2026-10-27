import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import type { Ajv2020 as AjvType, ValidateFunction } from "ajv/dist/2020.js";
import { fromRuntimePackageRoot } from "../runtime-paths.js";

/** Independent credential transport. Runtime and Agent Activity versions do not change. */
export const linearExecutionContractVersion = "1.0.0" as const;
export const linearExecutionCredentialPath = "/api/linear/execution-credentials" as const;
export const linearExecutionMaxCredentialLifetimeMs = 15 * 60_000;
export const linearExecutionBodyLimitBytes = 8 * 1024;

/** Correlation constraints, never proof of a business grant or receiver identity. */
export interface LinearExecutionIdentity {
  readonly taskId: string;
  readonly attemptId: string;
  readonly runId: string;
  readonly organizationId: string;
}

export interface LinearExecutionCredentialRequest {
  readonly contractVersion: typeof linearExecutionContractVersion;
  readonly requestId: string;
  readonly operation: "issue" | "revoke";
  readonly execution: LinearExecutionIdentity;
}

export interface LinearExecutionCredentialIssued {
  readonly contractVersion: typeof linearExecutionContractVersion;
  readonly requestId: string;
  readonly status: "issued";
  readonly execution: LinearExecutionIdentity;
  readonly credentialId: string;
  /** Secret bearer. Transport only; never log, journal, or include in instructions. */
  readonly credential: string;
  readonly expiresAt: string;
}

export const linearExecutionDenialCodes = [
  "linear.execution.invalid_request", "linear.execution.invalid_credential",
  "linear.execution.expired", "linear.execution.revoked", "linear.execution.execution_denied",
  "linear.execution.installation_denied", "linear.execution.authorization_changed",
  "linear.execution.workspace_mismatch", "linear.execution.capacity", "linear.execution.unavailable",
] as const;
export type LinearExecutionDenialCode = typeof linearExecutionDenialCodes[number];
export type LinearExecutionCredentialResponse = LinearExecutionCredentialIssued | {
  readonly contractVersion: typeof linearExecutionContractVersion;
  readonly requestId: string;
  readonly status: "revoked";
  readonly execution: LinearExecutionIdentity;
} | {
  readonly contractVersion: typeof linearExecutionContractVersion;
  readonly requestId: string;
  readonly status: "denied";
  readonly code: LinearExecutionDenialCode;
};

export class LinearExecutionContractError extends Error {
  constructor() { super("Linear execution credential transport is invalid."); this.name = "LinearExecutionContractError"; }
}

let validators: { request: ValidateFunction; response: ValidateFunction } | undefined;
function schemas(): NonNullable<typeof validators> {
  if (validators !== undefined) return validators;
  const require = createRequire(import.meta.url);
  const Ajv = (require("ajv/dist/2020") as { default: new (options: Record<string, unknown>) => AjvType }).default;
  const addFormats = (require("ajv-formats") as { default: (ajv: AjvType) => void }).default;
  const ajv = new Ajv({ strict: true, allErrors: false, ownProperties: true, coerceTypes: false, removeAdditional: false, useDefaults: false });
  addFormats(ajv);
  const schema = JSON.parse(readFileSync(fromRuntimePackageRoot("schemas", "linear-execution", "linear-execution-credentials.schema.json"), "utf8")) as { $id: string };
  ajv.addSchema(schema);
  validators = { request: ajv.compile({ $ref: `${schema.$id}#/$defs/request` }), response: ajv.compile({ $ref: `${schema.$id}#/$defs/response` }) };
  return validators;
}

function plain(value: unknown): boolean {
  if (value === null || typeof value !== "object") return true;
  if (Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) return false;
  return Object.values(value).every(plain);
}
function check(value: unknown, kind: "request" | "response"): void {
  try {
    if (!plain(value) || Buffer.byteLength(JSON.stringify(value), "utf8") > linearExecutionBodyLimitBytes || !schemas()[kind](value)) throw new LinearExecutionContractError();
  } catch { throw new LinearExecutionContractError(); }
}
function detached<T>(value: T): T {
  const copy = JSON.parse(JSON.stringify(value)) as T;
  for (const entry of Object.values(copy as Record<string, unknown>)) if (entry !== null && typeof entry === "object") Object.freeze(entry);
  return Object.freeze(copy);
}

export function linearExecutionIdentitiesEqual(left: LinearExecutionIdentity, right: LinearExecutionIdentity): boolean {
  return left.taskId === right.taskId && left.attemptId === right.attemptId && left.runId === right.runId && left.organizationId === right.organizationId;
}

export function validateLinearExecutionCredentialRequest(value: unknown): LinearExecutionCredentialRequest {
  check(value, "request");
  return detached(value as LinearExecutionCredentialRequest);
}

/** Bind responses to the exact request and current clock before exposing a bearer to a child. */
export function validateLinearExecutionCredentialResponse(value: unknown, requestValue: LinearExecutionCredentialRequest, nowMs: number): LinearExecutionCredentialResponse {
  const request = validateLinearExecutionCredentialRequest(requestValue);
  check(value, "response");
  const response = value as LinearExecutionCredentialResponse;
  if (!Number.isSafeInteger(nowMs) || response.requestId !== request.requestId) throw new LinearExecutionContractError();
  if (response.status !== "denied") {
    if (!linearExecutionIdentitiesEqual(response.execution, request.execution)
      || (request.operation === "issue" ? response.status !== "issued" : response.status !== "revoked")) throw new LinearExecutionContractError();
    if (response.status === "issued") {
      const expires = Date.parse(response.expiresAt);
      if (!Number.isSafeInteger(expires) || new Date(expires).toISOString() !== response.expiresAt
        || expires <= nowMs || expires - nowMs > linearExecutionMaxCredentialLifetimeMs) throw new LinearExecutionContractError();
    }
  }
  return detached(response);
}
