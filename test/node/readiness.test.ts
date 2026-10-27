import assert from "node:assert/strict";
import test from "node:test";

import { deriveNodeConfiguration } from "../../src/configuration.js";
import { nodeReadiness } from "../../src/node/readiness.js";
import { configurationFixture, fixtureKey } from "../support/configuration-fixture.js";

test("TOML inventory distinguishes ready Workers, refused Codex probes and unsupported Claude readiness", async () => {
  const configuration = deriveNodeConfiguration(configurationFixture({
    nodes: [{ id: "node-bin", port: 9080, eventPort: 9081, defaultWorker: "node-worker-a", workers: [
      { id: "node-worker-a", kind: "codex", port: 9082 },
      { id: "node-worker-b", kind: "codex", port: 9083 },
      { id: "worker-bin", kind: "codex", port: 9084 },
      { id: "claude-worker-bin", kind: "claude-code", port: 9085 },
      { id: "rejected-claude-worker-bin", kind: "claude-code", port: 9086 },
    ].map(worker => ({ ...worker, labels: [], workspaces: [{ organizationId: "org-bin", workspaceRoot: "/tmp/example-workspace" }] })) }],
  }), undefined, fixtureKey);
  const readiness = {
    status: "ready", modelAvailability: "not_checked",
    profiles: Object.fromEntries(["explorer", "architect", "reviewer", "worker"].map(role => [role, { availability: "available", attempt: "not_attempted" }])),
    degradedReasons: [],
  };
  const probed: string[] = [];
  const report = await nodeReadiness(configuration, { fetch: async (url, options) => {
    const worker = configuration.workers.find(worker => url === `${worker.workerEndpoint}/v1/worker/readiness`);
    assert.ok(worker);
    assert.equal(options.headers.authorization, `Bearer ${worker.commandToken}`);
    probed.push(worker.target.identity.workerId);
    if (worker.target.identity.workerId === "worker-bin") {
      throw new TypeError("fetch failed", { cause: Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" }) });
    }
    return Response.json(readiness);
  } });
  assert.deepEqual(probed, ["node-worker-a", "node-worker-b", "worker-bin"]);
  assert.deepEqual(report.workers, [
    { workerId: "node-worker-a", transport: "reachable", readinessSupport: "supported", readiness },
    { workerId: "node-worker-b", transport: "reachable", readinessSupport: "supported", readiness },
    { workerId: "worker-bin", transport: "unknown", readinessSupport: "supported" },
    { workerId: "claude-worker-bin", transport: "unknown", readinessSupport: "unsupported" },
    { workerId: "rejected-claude-worker-bin", transport: "unknown", readinessSupport: "unsupported" },
  ]);
});
