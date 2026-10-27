import {
  NODE_ROUTING_SNAPSHOT_VERSION, routerNodeBindingDigest,
  type RouterNodeRunTargetEvidence, type RouterNodeTargetSnapshot,
} from "../../src/shared/node-routing-projection.js";
import type { ExecutorLabel, WorkerSelection, WorkerSelectionScope } from "../../src/shared/worker-selection.js";

/**
 * Build policy selections. Empty executorLabels lets the Node apply its scope rule or
 * default; a named executor label resolves through the Node's executor-to-Worker
 * mapping.
 */
export function workerSelectionFixture(
  scope: WorkerSelectionScope = { system: "linear", tenantId: "tenant-1", scopeId: "team-1" },
  executorLabels: readonly ExecutorLabel[] = [],
): WorkerSelection {
  return { kind: "policy", scope, executorLabels };
}

/** Shared canonical inputs for Router, channel and CLI implementations. */
export function nodeRoutingSnapshotFixture(system: "linear" = "linear", executorLabels: readonly ExecutorLabel[] = []): RouterNodeTargetSnapshot {
  const scope = { system, tenantId: "tenant-1", scopeId: "team-1" } as const;
  const fields: Omit<RouterNodeTargetSnapshot, "bindingDigest"> = {
    contractVersion: NODE_ROUTING_SNAPSHOT_VERSION,
    scope,
    bindingRevision: 1,
    nodeRef: { ref: "logical-node-route-1", version: "v1" },
    workerSelection: workerSelectionFixture(scope, executorLabels),
    workspacePolicyRef: null,
  };
  return { ...fields, bindingDigest: routerNodeBindingDigest(fields) };
}

export function nodeRunTargetFixture(recorded = false): RouterNodeRunTargetEvidence {
  return {
    contractVersion: "1.0.0", runId: "run-1", target: nodeRoutingSnapshotFixture(),
    execution: recorded ? { status: "recorded", workerId: "worker-1", executorId: "executor-1" } : { status: "pending", workerId: null, executorId: null },
  };
}
