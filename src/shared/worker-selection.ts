import { isWorkerOpaqueIdentifier } from "./router-worker.js";

/**
 * The fixed executor vocabulary: a contract constant, not configuration. A label names a kind of
 * executor, never a workerKey or process identity; which local Worker a kind resolves to is the
 * Node's configuration (`src/node/runtime-config.ts`). The order is canonical, because the selection
 * is digested into `bindingDigest` and persisted. Source label spellings that map onto these names
 * are in the route table below.
 */
export const EXECUTOR_LABELS = Object.freeze(["claude", "codex"] as const);
export type ExecutorLabel = (typeof EXECUTOR_LABELS)[number];

export interface WorkerSelectionScope {
  readonly system: "linear";
  readonly tenantId: string;
  readonly scopeId: string;
}

/**
 * What a command says about which local Worker should run it. `policy` carries facts and no
 * decision: the source's scope and its executor labels. The Node alone turns that into a Worker.
 * Any other payload misses the exact key set and is rejected as malformed.
 */
export interface WorkerSelectionPolicy {
  readonly kind: "policy";
  readonly scope: WorkerSelectionScope;
  readonly executorLabels: readonly ExecutorLabel[];
}
export type WorkerSelection = WorkerSelectionPolicy;

export type WorkerLabelContext = { readonly status: "available"; readonly labels: readonly string[] } | { readonly status: "unavailable" };
export class WorkerSelectionError extends Error {
  constructor(readonly code: "labels_unavailable" | "malformed" = "malformed") {
    super(code === "labels_unavailable" ? "Worker selection requires available labels." : "Worker selection is malformed.");
    this.name = "WorkerSelectionError";
  }
}
function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) throw new WorkerSelectionError();
  return value as Record<string, unknown>;
}
function exact(value: Record<string, unknown>, keys: readonly string[]): void {
  if (Object.keys(value).length !== keys.length || keys.some(key => !Object.hasOwn(value, key))) throw new WorkerSelectionError();
}
export function validateWorkerKey(value: unknown): string {
  if (!isWorkerOpaqueIdentifier(value)) throw new WorkerSelectionError();
  return value;
}
export function isExecutorLabel(value: unknown): value is ExecutorLabel {
  return typeof value === "string" && (EXECUTOR_LABELS as readonly string[]).includes(value);
}

/** One source label that names an executor, beside the bare executor names themselves. */
export interface WorkerLabelRoute {
  readonly label: string;
  readonly executorLabel: ExecutorLabel;
}

export const DEFAULT_WORKER_LABEL_ROUTES: readonly WorkerLabelRoute[] = Object.freeze([
  Object.freeze({ label: "WORKER:CODEX", executorLabel: "codex" }),
  Object.freeze({ label: "WORKER:CLAUDE", executorLabel: "claude" }),
] as const);

function routeKey(label: string): string {
  return label.trim().toLowerCase();
}

/**
 * Reduce a source's labels to the fixed executor vocabulary, in canonical order, deduplicated. A
 * label names an executor when it is exactly one of `EXECUTOR_LABELS` (case-sensitive) or matches an
 * entry of `routes` case-insensitively after trimming. No priority or conflict resolution is applied
 * here; that belongs to the Node.
 */
export function executorLabelsOf(labels: readonly string[], routes: readonly WorkerLabelRoute[] = DEFAULT_WORKER_LABEL_ROUTES): readonly ExecutorLabel[] {
  const routed = new Map<string, ExecutorLabel>();
  for (const route of routes) {
    if (typeof route?.label !== "string" || !route.label.trim() || !isExecutorLabel(route.executorLabel)) throw new WorkerSelectionError();
    routed.set(routeKey(route.label), route.executorLabel);
  }
  const present = new Set<string>();
  for (const label of labels) {
    if (typeof label !== "string") throw new WorkerSelectionError();
    if (isExecutorLabel(label)) present.add(label);
    const executor = routed.get(routeKey(label));
    if (executor !== undefined) present.add(executor);
  }
  return Object.freeze(EXECUTOR_LABELS.filter(label => present.has(label)));
}

/**
 * Exported so the Node's rule parser validates a configured scope with the same function the wire
 * uses.
 */
export function validateWorkerSelectionScope(value: unknown): WorkerSelectionScope {
  return scopeOf(value);
}

function scopeOf(value: unknown): WorkerSelectionScope {
  const item = record(value);
  exact(item, ["system", "tenantId", "scopeId"]);
  if (item.system !== "linear") throw new WorkerSelectionError();
  return Object.freeze({ system: item.system, tenantId: validateWorkerKey(item.tenantId), scopeId: validateWorkerKey(item.scopeId) });
}

export function validateWorkerSelection(value: unknown): WorkerSelection {
  const item = record(value);
  if (item.kind !== "policy") throw new WorkerSelectionError();
  exact(item, ["kind", "scope", "executorLabels"]);
  const scope = scopeOf(item.scope);
  if (!Array.isArray(item.executorLabels)) throw new WorkerSelectionError();
  // Canonical form is part of the contract: the selection is digested into `bindingDigest`.
  const labels = Object.freeze(item.executorLabels.map(label => {
    if (!isExecutorLabel(label)) throw new WorkerSelectionError();
    return label;
  }));
  if (new Set(labels).size !== labels.length) throw new WorkerSelectionError();
  if (labels.some((label, index) => index > 0 && EXECUTOR_LABELS.indexOf(label) <= EXECUTOR_LABELS.indexOf(labels[index - 1]!))) throw new WorkerSelectionError();
  return Object.freeze({ kind: "policy", scope, executorLabels: labels });
}

/**
 * Build the Relay's contribution to a new session's route. `unavailable` throws
 * `labels_unavailable`: a context that could not be fetched must stay a classifiable refusal,
 * never an empty label list that would route to the default Worker.
 */
export function workerSelectionFromLabels(context: unknown, scope: WorkerSelectionScope): WorkerSelectionPolicy {
  const item = record(context);
  if (item.status === "unavailable") { exact(item, ["status"]); throw new WorkerSelectionError("labels_unavailable"); }
  exact(item, ["status", "labels"]);
  if (item.status !== "available" || !Array.isArray(item.labels)) throw new WorkerSelectionError();
  for (const label of item.labels) if (typeof label !== "string" || !label.trim()) throw new WorkerSelectionError();
  const selection = validateWorkerSelection({ kind: "policy", scope: scopeOf(scope), executorLabels: executorLabelsOf(item.labels) });
  // Narrowed to the policy selection rather than returned as the wide union.
  if (selection.kind !== "policy") throw new WorkerSelectionError();
  return selection;
}
