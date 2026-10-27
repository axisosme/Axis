import assert from "node:assert/strict";
import test from "node:test";

import {
  NodeWorkerInventoryEmptyError,
  NodeWorkerSelectionConflictError,
  NodeWorkerSelectionError,
  selectNodeInventoryWorker,
} from "../../src/node/runtime-config.js";
import type { NodeRuntimeConfig } from "../../src/node/runtime-config.js";
import { DEFAULT_WORKER_LABEL_ROUTES, EXECUTOR_LABELS, executorLabelsOf, workerSelectionFromLabels } from "../../src/shared/worker-selection.js";

import { deriveNodeConfiguration } from "../../src/configuration.js";
import { configurationFixture, fixtureKey } from "../support/configuration-fixture.js";
import { nodeConfigurationFixture } from "../support/node-configuration.js";
import { HttpNodeWorkerClient } from "../../src/node/worker-client.js";

const LINEAR_SCOPE = { system: "linear" as const, tenantId: "organization-fixture", scopeId: "team-one" };
const worker = (kind: "codex" | "claude-code", port: number) => ({ id: kind === "codex" ? "codex" : "claude", key: kind === "codex" ? "codex-key" : "claude-key", kind, port, workspaces: [{ organizationId: "organization-fixture", workspaceRoot: "/tmp/workspace" }] });
const CODEX = worker("codex", 4111), CLAUDE = worker("claude-code", 4101);
function nodeConfig(overrides: Record<string, unknown> = {}): NodeRuntimeConfig {
  return deriveNodeConfiguration(configurationFixture({ nodes: [{ id: "physical-node", port: 4100, eventPort: 4102, defaultWorker: "codex-key",
    workers: [CLAUDE, CODEX], workerScopeRules: [{ scope: LINEAR_SCOPE, workerKey: "claude-key" }], ...overrides }] }), undefined, fixtureKey);
}
function policy(scope: typeof LINEAR_SCOPE, executorLabels: readonly string[]) { return { kind: "policy" as const, scope, executorLabels }; }
function resolvedKey(config: NodeRuntimeConfig, selection: unknown): string { return selectNodeInventoryWorker(config, selection).workerKey; }

test("runtime fixtures preserve the immutable inventory required by the private Worker client", () => {
  const production = nodeConfig();
  const fixture = nodeConfigurationFixture({ ...production, workers: production.workers });
  assert.doesNotThrow(() => new HttpNodeWorkerClient({ worker: fixture.workers[0]!, workerDispatchTimeoutMs: fixture.workerDispatchTimeoutMs }));
  assert.throws(() => { (fixture.workers[0]!.target.identity as { workerId: string }).workerId = "changed"; }, TypeError);
});

test("a Team scope rule selects Claude for unlabelled work while other Teams use the default", () => {
  const config = nodeConfig();
  assert.equal(resolvedKey(config, policy(LINEAR_SCOPE, [])), "claude-key");
  assert.equal(resolvedKey(config, policy(LINEAR_SCOPE, ["codex"])), "codex-key");
  assert.equal(resolvedKey(config, policy({ ...LINEAR_SCOPE, scopeId: "team-other" }, [])), "codex-key");
  assert.equal(resolvedKey(config, policy({ ...LINEAR_SCOPE, tenantId: "organization-other" }, [])), "codex-key");
});
test("each fixed executor label resolves to the Worker the Node maps it to", () => {
  const config = nodeConfig();
  assert.deepEqual(["claude", "codex"].map(label => resolvedKey(config, policy(LINEAR_SCOPE, [label]))), ["claude-key", "codex-key"]);
  assert.throws(() => resolvedKey(config, policy(LINEAR_SCOPE, ["review"])), NodeWorkerSelectionError);
});
test("two different executor labels are refused as a conflict rather than resolved by order", () => {
  const config = nodeConfig();
  assert.throws(() => resolvedKey(config, policy(LINEAR_SCOPE, ["claude", "codex"])), NodeWorkerSelectionConflictError);
  assert.equal(resolvedKey(config, policy(LINEAR_SCOPE, ["codex"])), "codex-key");
  assert.throws(() => resolvedKey(config, policy(LINEAR_SCOPE, ["codex", "claude"])), NodeWorkerSelectionError);
  assert.throws(() => resolvedKey(config, policy(LINEAR_SCOPE, ["claude", "claude"])), NodeWorkerSelectionError);
});
test("an executor this Node cannot satisfy fails outright and never falls back to the default", () => {
  const config = nodeConfig({ workers: [{ ...CLAUDE, labels: [] }, CODEX] });
  assert.throws(() => resolvedKey(config, policy(LINEAR_SCOPE, ["claude"])), NodeWorkerSelectionError);
  assert.equal(resolvedKey(config, policy(LINEAR_SCOPE, ["codex"])), "codex-key");
});
test("one configured Worker is its own default; two or more must name one", () => {
  const single = nodeConfig({ workers: [CODEX], defaultWorker: undefined, workerScopeRules: [] });
  assert.equal(single.defaultWorkerKey, "codex-key");
  assert.equal(resolvedKey(single, policy(LINEAR_SCOPE, [])), "codex-key");
  assert.throws(() => nodeConfig({ defaultWorker: undefined }), /nodes.defaultWorker/u);
  const bare = nodeConfig({ workers: [], defaultWorker: undefined, workerScopeRules: [] });
  assert.equal(bare.defaultWorkerKey, undefined);
  assert.throws(() => resolvedKey(bare, policy(LINEAR_SCOPE, [])), NodeWorkerInventoryEmptyError);
});
test("a rule naming a Worker this Node does not have is refused at startup, not on the first issue", () => {
  assert.throws(() => nodeConfig({ workerScopeRules: [{ scope: LINEAR_SCOPE, workerKey: "absent" }] }), /nodes.workerScopeRules/u);
  assert.throws(() => nodeConfig({ workers: [CLAUDE, { ...CODEX, labels: ["claude"] }] }), /nodes.workers.labels/u);
  assert.throws(() => nodeConfig({ workerScopeRules: [{ scope: LINEAR_SCOPE, workerKey: "claude-key" }, { scope: LINEAR_SCOPE, workerKey: "codex-key" }] }), /nodes.workerScopeRules/u);
});
test("unknown selection kinds are rejected", () => {
  const config = nodeConfig();
  assert.throws(() => resolvedKey(config, { kind: "fictional-selection" }), NodeWorkerSelectionError);
  assert.equal(resolvedKey(config, policy(LINEAR_SCOPE, [])), "claude-key");
});

test("each WORKER group label selects its executor, matched case-insensitively", () => {
  assert.deepEqual(executorLabelsOf(["WORKER:CODEX"]), ["codex"]);
  assert.deepEqual(executorLabelsOf(["WORKER:CLAUDE"]), ["claude"]);
  // The default table covers exactly the supported executors, once each.
  assert.deepEqual(DEFAULT_WORKER_LABEL_ROUTES.map(route => route.executorLabel).sort(), [...EXECUTOR_LABELS].sort());
  for (const spelling of ["worker:codex", "Worker:Codex", "WORKER:CODEX", "  WORKER:CODEX  "]) {
    assert.deepEqual(executorLabelsOf(["build", spelling]), ["codex"], spelling);
  }
});

test("bare executor labels require exact case-sensitive matches", () => {
  assert.deepEqual(executorLabelsOf(["claude"]), ["claude"]);
  assert.deepEqual(executorLabelsOf(["codex", "claude"]), ["claude", "codex"]);
  // An ungrouped uppercase `CODEX` is neither a bare executor label nor a
  // WORKER route, and neither is an unsupported WORKER child.
  assert.deepEqual(executorLabelsOf(["CODEX", "Claude", "WORKER:OTHER", "WORKER", "OTHER:CODEX"]), []);
});

test("both spellings of one executor collapse; two executors stay two for the Node to refuse", () => {
  assert.deepEqual(executorLabelsOf(["WORKER:CODEX", "codex"]), ["codex"]);
  assert.deepEqual(executorLabelsOf(["WORKER:CODEX", "WORKER:CLAUDE"]), ["claude", "codex"]);
  const selection = workerSelectionFromLabels({ status: "available", labels: ["WORKER:CODEX", "WORKER:CLAUDE"] }, LINEAR_SCOPE);
  assert.deepEqual(selection.executorLabels, ["claude", "codex"]);
  // The resulting selection reaches the Node's existing conflict refusal unchanged.
  const config = nodeConfig();
  assert.throws(() => resolvedKey(config, selection), (error: unknown) => error instanceof NodeWorkerSelectionConflictError);
  // And one WORKER label resolves to the Worker the Node maps that executor to.
  const single = workerSelectionFromLabels({ status: "available", labels: ["bug", "Worker:Claude"] }, LINEAR_SCOPE);
  assert.deepEqual(single, { kind: "policy", scope: LINEAR_SCOPE, executorLabels: ["claude"] });
  assert.equal(resolvedKey(config, single), "claude-key");
});

test("a caller-supplied route table replaces the default one", () => {
  const routes = [{ label: "team:review", executorLabel: "claude" as const }];
  assert.deepEqual(executorLabelsOf(["Team:Review"], routes), ["claude"]);
  // The default WORKER spellings are not consulted when a table is passed, but
  // the bare executor names belong to no table and still apply.
  assert.deepEqual(executorLabelsOf(["WORKER:CODEX"], routes), []);
  assert.deepEqual(executorLabelsOf(["codex"], routes), ["codex"]);
  assert.deepEqual(executorLabelsOf(["WORKER:CODEX"], []), []);
});
