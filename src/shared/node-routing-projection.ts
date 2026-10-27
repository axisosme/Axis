import { createHash } from "node:crypto";
import { NodeContractError } from "./router-node.js";
import { isWorkerOpaqueIdentifier, type WorkerVersionedReference } from "./router-worker.js";
import { validateWorkerSelection, type WorkerSelection } from "./worker-selection.js";

/** Frozen Node routing authority for one Task. */
export const NODE_ROUTING_SNAPSHOT_VERSION = "1.0.0-node" as const;
export interface RouterNodeTargetSnapshot {
  readonly contractVersion: typeof NODE_ROUTING_SNAPSHOT_VERSION;
  readonly scope: { readonly system: "linear"; readonly tenantId: string; readonly scopeId: string };
  readonly bindingRevision: number;
  readonly bindingDigest: `sha256:${string}`;
  readonly nodeRef: WorkerVersionedReference;
  readonly workerSelection: WorkerSelection;
  readonly workspacePolicyRef: WorkerVersionedReference | null;
}

/** Persistence exposes the actual pair atomically; pending never guesses either ID. */
export type RouterRunExecutionEvidence =
  | { readonly status: "pending"; readonly workerId: null; readonly executorId: null }
  | { readonly status: "recorded"; readonly workerId: string; readonly executorId: string };
export interface RouterNodeRunTargetEvidence {
  readonly contractVersion: "1.0.0";
  readonly runId: string;
  readonly target: RouterNodeTargetSnapshot;
  readonly execution: RouterRunExecutionEvidence;
}
/** Path-free CLI projection of one Run target. */
export interface AxisCliNodeRunTarget {
  readonly schemaVersion: "axis.cli.run.v1";
  readonly runId: string;
  readonly nodeRef: WorkerVersionedReference;
  readonly workerSelection: WorkerSelection;
  readonly workspacePolicyRef: WorkerVersionedReference | null;
  readonly workerId: string | null;
  readonly executorId: string | null;
}

function closed(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) throw new NodeContractError();
  const item = value as Record<string, unknown>;
  if (Object.keys(item).length !== keys.length || keys.some(key => !Object.hasOwn(item, key))) throw new NodeContractError();
  return item;
}
function identifier(value: unknown): string {
  if (!isWorkerOpaqueIdentifier(value)) throw new NodeContractError();
  return value;
}
function reference(value: unknown): WorkerVersionedReference {
  const item = closed(value, ["ref", "version"]);
  if (!isWorkerOpaqueIdentifier(item.ref) || !isWorkerOpaqueIdentifier(item.version)) throw new NodeContractError();
  return Object.freeze({ ref: item.ref, version: item.version });
}
export function validateRouterNodeTargetSnapshot(value: unknown): RouterNodeTargetSnapshot {
  const item = closed(value, ["contractVersion", "scope", "bindingRevision", "bindingDigest", "nodeRef", "workerSelection", "workspacePolicyRef"]);
  if (item.contractVersion !== NODE_ROUTING_SNAPSHOT_VERSION || !Number.isSafeInteger(item.bindingRevision) || (item.bindingRevision as number) < 1) throw new NodeContractError();
  if (typeof item.bindingDigest !== "string" || !/^sha256:[a-f0-9]{64}$/u.test(item.bindingDigest)) throw new NodeContractError();
  const scope = closed(item.scope, ["system", "tenantId", "scopeId"]);
  if (scope.system !== "linear") throw new NodeContractError();
  let workerSelection: WorkerSelection;
  try { workerSelection = validateWorkerSelection(item.workerSelection); } catch { throw new NodeContractError(); }
  const nodeRef = reference(item.nodeRef);
  return Object.freeze({
    contractVersion: NODE_ROUTING_SNAPSHOT_VERSION,
    scope: Object.freeze({ system: scope.system, tenantId: identifier(scope.tenantId), scopeId: identifier(scope.scopeId) }),
    bindingRevision: item.bindingRevision as number,
    bindingDigest: item.bindingDigest as `sha256:${string}`,
    nodeRef, workerSelection,
    workspacePolicyRef: item.workspacePolicyRef === null ? null : reference(item.workspacePolicyRef),
  });
}

/** Digest the normalized frozen mapping; no current inventory lookup is permitted. */
export function routerNodeBindingDigest(snapshot: Omit<RouterNodeTargetSnapshot, "bindingDigest">): `sha256:${string}` {
  const normalized = validateRouterNodeTargetSnapshot({ ...snapshot, bindingDigest: `sha256:${"0".repeat(64)}` });
  const { bindingDigest: _digest, ...fields } = normalized;
  return `sha256:${createHash("sha256").update("axis.router.node-target.v1\n").update(JSON.stringify(fields)).digest("hex")}`;
}

export function validateRouterRunExecutionEvidence(value: unknown): RouterRunExecutionEvidence {
  const item = closed(value, ["status", "workerId", "executorId"]);
  if (item.status === "pending" && item.workerId === null && item.executorId === null) return Object.freeze({ status: "pending", workerId: null, executorId: null });
  if (item.status !== "recorded") throw new NodeContractError("axis.node.recovery_required");
  return Object.freeze({ status: "recorded", workerId: identifier(item.workerId), executorId: identifier(item.executorId) });
}

export function validateRouterNodeRunTargetEvidence(value: unknown): RouterNodeRunTargetEvidence {
  const item = closed(value, ["contractVersion", "runId", "target", "execution"]);
  if (item.contractVersion !== "1.0.0") throw new NodeContractError();
  return Object.freeze({ contractVersion: "1.0.0", runId: identifier(item.runId), target: validateRouterNodeTargetSnapshot(item.target), execution: validateRouterRunExecutionEvidence(item.execution) });
}

export function axisCliNodeRunTarget(value: RouterNodeRunTargetEvidence): AxisCliNodeRunTarget {
  const evidence = validateRouterNodeRunTargetEvidence(value);
  return Object.freeze({
    schemaVersion: "axis.cli.run.v1", runId: evidence.runId,
    nodeRef: evidence.target.nodeRef, workerSelection: evidence.target.workerSelection,
    workspacePolicyRef: evidence.target.workspacePolicyRef,
    workerId: evidence.execution.workerId, executorId: evidence.execution.executorId,
  });
}
