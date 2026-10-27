import type { NodeConfiguration } from "../../src/configuration.js";
import type { NodeRuntimeWorkerConfig } from "../../src/node/runtime-config.js";

export type FixtureWorker = Omit<NodeRuntimeWorkerConfig, "nodeId" | "nodeRef" | "readinessMode"> & { readonly readinessMode: string };
export type NodeFixture = { -readonly [K in keyof Omit<Partial<NodeConfiguration>, "workers">]: NodeConfiguration[K] } & { workers?: readonly FixtureWorker[] };

function freeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    for (const nested of Object.values(value)) freeze(nested);
    Object.freeze(value);
  }
  return value;
}

/** Explicit runtime inputs let transport-fault tests isolate the behavior under test. */
export function nodeConfigurationFixture(input: NodeFixture): NodeConfiguration {
  const nodeId = input.nodeId ?? "fixture-node", nodeRef = input.nodeRef ?? "fixture-node", nodeVersion = input.nodeVersion ?? "v1";
  return freeze(structuredClone({ contractVersion: "1.0.0" as const, database: ":memory:", nodeId, nodeRef, nodeVersion, token: "fixture-node-command",
    host: "127.0.0.1", port: 9080, eventHost: "127.0.0.1", eventPort: 9081,
    routerEndpoint: "http://127.0.0.1:8788/", routerEventToken: "fixture-node-events", workerScopeRules: [], workerExecutorBindings: [], workerHostRoots: [],
    workerDispatchTimeoutMs: 30_000, routerEventTimeoutMs: 10_000, workerReadinessTimeoutMs: 5_000,
    eventRetryInitialDelayMs: 1_000, eventRetryMaxDelayMs: 30_000, eventMaxConcurrentRuns: 8,
    ...input, workers: (input.workers ?? []).map(worker => ({ ...worker, nodeId, nodeRef: { ref: nodeRef, version: nodeVersion }, readinessMode: worker.readinessMode === "codex" ? "codex" as const : "unsupported" as const })) }));
}
