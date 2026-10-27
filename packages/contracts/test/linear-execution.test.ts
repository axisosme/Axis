import assert from "node:assert/strict";
import { test } from "node:test";
import {
  LinearExecutionContractError, linearExecutionContractVersion,
  validateLinearExecutionCredentialRequest, validateLinearExecutionCredentialResponse,
} from "../src/linear-execution/index.js";

const now = Date.parse("2026-09-11T00:00:00.000Z");
const request = { contractVersion: linearExecutionContractVersion, requestId: "request-1", operation: "issue", execution: { taskId: "task-1", attemptId: "attempt-1", runId: "run-1", organizationId: "workspace-1" } } as const;
const response = { contractVersion: linearExecutionContractVersion, requestId: request.requestId, status: "issued", execution: request.execution, credentialId: "credential-1", credential: `axlea1_${"a".repeat(43)}`, expiresAt: "2026-09-11T00:05:00.000Z" } as const;

test("issue response is detached, immutable and bound to all execution identifiers", () => {
  const validated = validateLinearExecutionCredentialResponse(response, request, now);
  assert.deepEqual(validated, response);
  assert.notEqual(validated, response);
  assert.ok(Object.isFrozen(validated));
  assert.ok(validated.status === "issued" && Object.isFrozen(validated.execution));
  for (const name of ["taskId", "attemptId", "runId", "organizationId"] as const) {
    assert.throws(() => validateLinearExecutionCredentialResponse({ ...response, execution: { ...response.execution, [name]: "other" } }, request, now), LinearExecutionContractError);
  }
  assert.throws(() => validateLinearExecutionCredentialResponse({ ...response, requestId: "other" }, request, now), LinearExecutionContractError);
});

test("transport refuses extensions, personal tokens, malformed identity and unbounded input", () => {
  for (const value of [null, {}, { ...request, token: "secret" }, { ...request, requestId: "x".repeat(129) }, { ...request, operation: "grant" }, { ...request, execution: { ...request.execution, installationId: "other" } }, { ...request, execution: { ...request.execution, runId: "https://example.com/secret" } }]) {
    assert.throws(() => validateLinearExecutionCredentialRequest(value), { name: "LinearExecutionContractError", message: "Linear execution credential transport is invalid." });
  }
  for (const credential of ["", "lin_api_personal", "Bearer axlea1_invalid", `axlea1_${"a".repeat(42)}`]) {
    assert.throws(() => validateLinearExecutionCredentialResponse({ ...response, credential }, request, now), LinearExecutionContractError);
  }
  assert.throws(() => validateLinearExecutionCredentialResponse({ ...response, oauthToken: "secret" }, request, now), LinearExecutionContractError);
});

test("expiry is exclusive and bounded, and only canonical UTC timestamps cross the boundary", () => {
  for (const expiresAt of ["2026-09-10T23:59:59.999Z", "2026-09-11T00:00:00.000Z", "2026-09-11T00:15:00.001Z", "2026-09-11T08:05:00.000+08:00", "invalid"]) {
    assert.throws(() => validateLinearExecutionCredentialResponse({ ...response, expiresAt }, request, now), LinearExecutionContractError);
  }
  assert.equal(validateLinearExecutionCredentialResponse({ ...response, expiresAt: "2026-09-11T00:15:00.000Z" }, request, now).status, "issued");
  assert.throws(() => validateLinearExecutionCredentialResponse(response, request, NaN), LinearExecutionContractError);
});

test("revocation and closed denial cannot be mistaken for issuance", () => {
  const revoke = { ...request, operation: "revoke" } as const;
  const revoked = { contractVersion: linearExecutionContractVersion, requestId: request.requestId, status: "revoked", execution: request.execution } as const;
  assert.equal(validateLinearExecutionCredentialResponse(revoked, revoke, now).status, "revoked");
  assert.throws(() => validateLinearExecutionCredentialResponse(response, revoke, now), LinearExecutionContractError);
  assert.throws(() => validateLinearExecutionCredentialResponse(revoked, request, now), LinearExecutionContractError);
  const denied = { contractVersion: linearExecutionContractVersion, requestId: request.requestId, status: "denied", code: "linear.execution.execution_denied" } as const;
  assert.deepEqual(validateLinearExecutionCredentialResponse(denied, request, now), denied);
  assert.throws(() => validateLinearExecutionCredentialResponse({ ...denied, message: "secret" }, request, now), LinearExecutionContractError);
  assert.throws(() => validateLinearExecutionCredentialResponse({ ...denied, code: "retry_now" }, request, now), LinearExecutionContractError);
});

test("credential exchange accepts only the current protocol discriminator", () => {
  assert.equal(linearExecutionContractVersion, "1.0.0");
  assert.throws(() => validateLinearExecutionCredentialRequest({ ...request, contractVersion: "unsupported-contract" }), LinearExecutionContractError);
  assert.throws(() => validateLinearExecutionCredentialResponse({ ...response, contractVersion: "unsupported-contract" }, request, now), LinearExecutionContractError);
});
