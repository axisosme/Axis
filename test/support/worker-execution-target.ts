import type { WorkerExecutionTarget } from "../../src/shared/router-worker.js";

/**
 * Build the executionTarget required by WorkerCommandService for this configuration.
 * executorKind matches runtimeKind; an absent runtimeProfile uses executorId with
 * version v1. Callers address the Worker they constructed, and identity-rule drift
 * fails with identity_mismatch.
 */
export function executionTargetFor(configuration: {
  readonly nodeId: string;
  readonly workerId: string;
  readonly runtimeKind: string;
  readonly executorId: string;
  readonly runtimeProfile?: { readonly ref: string; readonly version: string };
}): WorkerExecutionTarget {
  return {
    identity: {
      nodeId: configuration.nodeId,
      workerId: configuration.workerId,
      executorKind: configuration.runtimeKind,
      runtimeProfile: configuration.runtimeProfile === undefined ? { ref: configuration.executorId, version: "v1" } : { ...configuration.runtimeProfile },
    },
    sessionBindingExecutorId: configuration.executorId,
  };
}
