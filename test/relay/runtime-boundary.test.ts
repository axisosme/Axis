import assert from "node:assert/strict";
import test from "node:test";

import { RUNTIME_BOUNDARY_VERSIONS, runtimeBoundaryVersion, validateRuntimeBoundaryMessage } from "../../src/shared/runtime-boundary.js";
import { buildWorkerBoundStartRequest } from "../support/router-node-fixtures.js";

test("Runtime boundaries accept only the current version throughout the identity graph", async () => {
  const { command: { start } } = await buildWorkerBoundStartRequest();
  assert.deepEqual(RUNTIME_BOUNDARY_VERSIONS, ["1.0.0"]);
  assert.equal(runtimeBoundaryVersion(start.contractVersion), start.contractVersion);
  assert.equal((await validateRuntimeBoundaryMessage(start)).valid, true);
  for (const contractVersion of ["fictional-contract", null, undefined]) {
    assert.equal(runtimeBoundaryVersion(contractVersion), null);
    assert.equal((await validateRuntimeBoundaryMessage({ ...start, contractVersion })).valid, false);
    assert.equal((await validateRuntimeBoundaryMessage({ ...start, identity: { ...start.identity, contractVersion } })).valid, false);
    for (const member of ["task", "attempt", "run", "sessionBinding"] as const) {
      assert.equal((await validateRuntimeBoundaryMessage({ ...start, identity: { ...start.identity, [member]: { ...start.identity[member], contractVersion } } })).valid, false);
    }
  }
});
