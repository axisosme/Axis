import assert from "node:assert/strict";
import test from "node:test";

import { LinearExecutionAuthority } from "../../src/relay/linear/execution-runtime.js";
import { LinearInstallationStore } from "../../src/relay/linear/installation.js";
import { linearExecutionEnvironment } from "../../src/shared/linear-execution-client.js";
import type { RouterNodePort } from "../../src/router/node-port.js";
import type { NodeCommandRequest } from "../../src/shared/router-node-commands.js";
import {
  buildNodeCancellationRequest,
  buildNodeContinuationRequest,
  buildNodePriorRoute,
  buildNodeReceipt,
  nodeAuthenticatedContext,
  nodeReservation,
} from "../support/router-node-commands-fixtures.js";

test("native children preserve package settings while excluding host and registry credentials", () => {
  const base = { PATH: "/usr/bin", LANG: "en_US.UTF-8", AXIS_WORKER_TOKEN: "worker-secret", LINEAR_API_KEY: "personal-secret", NODE_AUTH_TOKEN: "registry-secret", NODE_AUTH_SETTINGS: "node-settings", NPM_CONFIG_REGISTRY: "https://registry.npmjs.org/", UNSET: undefined };
  assert.deepEqual(linearExecutionEnvironment(base), {
    PATH: "/usr/bin", LANG: "en_US.UTF-8", LINEAR_API_KEY: "axis-linear-execution-unavailable",
    NODE_AUTH_SETTINGS: "node-settings", NPM_CONFIG_REGISTRY: "https://registry.npmjs.org/",
    LINEAR_GRAPHQL_ENDPOINT: "http://127.0.0.1:1/axis-linear-unavailable", LINEAR_EXECUTION_STATUS: "unavailable",
  });
  assert.equal(base.LINEAR_API_KEY, "personal-secret");
});

function execution(request: NodeCommandRequest) {
  if (request.command.command === "cancel" || request.command.command === "steer") throw new Error("A Run start identity is required.");
  const identity = request.command.start.identity;
  if (identity.task.sourceRef.system !== "linear") throw new Error("A Linear source is required.");
  return {
    taskId: identity.task.taskId,
    attemptId: identity.attempt.attemptId,
    runId: identity.run.runId,
    organizationId: identity.task.sourceRef.organizationId,
  };
}

function authorityFor(t: test.TestContext, current: Map<string, ReturnType<typeof execution> & { readonly sourceTeamId: string }>) {
  const installations = new LinearInstallationStore(":memory:");
  installations.install({ organizationId: "org-1", appUserId: "app-1", credentialRef: "vault-1", teamIds: ["team-1"], canAccessAllPublicTeams: false });
  const authority = new LinearExecutionAuthority({
    config: {
      grants: [{ organizationId: "org-1", appUserId: "app-1", scope: "installation-workspace" }],
      receivers: [{ bearerToken: `receiver-${"x".repeat(32)}`, identity: nodeReservation.target.identity }],
    },
    installations,
    router: { readLinearExecution: runId => current.get(runId) ?? null },
  });
  t.after(() => { authority.close(); installations.close(); });
  return authority;
}

test("Node command continuation prepares a fresh Linear execution grant for the successor Run", async (t) => {
  const request = await buildNodeContinuationRequest();
  const priorRoute = await buildNodePriorRoute();
  const successor = execution(request);
  const prior = { ...successor, runId: request.command.command === "continue" ? request.command.continuation.priorRunId : "unreachable" };
  const current = new Map([
    [prior.runId, { ...prior, sourceTeamId: "team-1" }],
    [successor.runId, { ...successor, sourceTeamId: "team-1" }],
  ]);
  const authority = authorityFor(t, current);
  authority.prepare(prior.runId, async () => nodeReservation.target.identity);
  const priorExecutionId = await authority.authorize(prior, nodeReservation.target.identity, "issue");
  assert.notEqual(priorExecutionId, null);

  let receiptReads = 0;
  const port: RouterNodePort = {
    dispatch: async () => { throw new Error("Dispatch is outside this authorization test."); },
    readReceipt: async (query) => {
      receiptReads += 1;
      assert.equal(query.commandId, request.command.commandId);
      return { status: 200, authenticatedNode: nodeAuthenticatedContext, body: buildNodeReceipt(request, "dispatching") };
    },
  };
  authority.prepareNode(request, port, priorRoute);

  const wrongReceiver = { ...nodeReservation.target.identity, workerId: "other-worker" };
  assert.equal(await authority.authorize(successor, wrongReceiver, "issue"), null);
  const successorExecutionId = await authority.authorize(successor, nodeReservation.target.identity, "issue");
  assert.notEqual(successorExecutionId, null);
  assert.notEqual(successorExecutionId, priorExecutionId);
  assert.equal(receiptReads, 2);
});

test("Node command cancel does not prepare Linear execution authority", async (t) => {
  const base = await buildNodeCancellationRequest();
  if (base.command.command !== "cancel") throw new Error("Expected a cancel fixture.");
  const request = { ...base, command: { ...base.command, target: { runId: "run-cancel-only" } } };
  const cancelled = { taskId: "task-cancel", attemptId: "attempt-cancel", runId: request.command.target.runId, organizationId: "org-1" };
  const authority = authorityFor(t, new Map([[cancelled.runId, { ...cancelled, sourceTeamId: "team-1" }]]));
  let receiptReads = 0;
  authority.prepareNode(request, {
    dispatch: async () => { throw new Error("Dispatch is outside this authorization test."); },
    readReceipt: async () => {
      receiptReads += 1;
      throw new Error("Cancel must not bind a Linear execution receiver.");
    },
  }, undefined);

  assert.equal(await authority.authorize(cancelled, nodeReservation.target.identity, "issue"), null);
  assert.equal(receiptReads, 0);
});
