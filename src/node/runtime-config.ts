import type { NODE_CONTRACT_VERSION, NodeStaticTarget } from "../shared/router-node.js";
import type { NodeReservationEvidence } from "../shared/router-node-commands.js";
import { validateWorkerSelection, type ExecutorLabel, type WorkerSelection, type WorkerSelectionScope } from "../shared/worker-selection.js";

type ReadinessMode = "codex" | "unsupported";

export interface NodeRuntimeWorkerConfig extends NodeStaticTarget {
  /** Static registration material only; no Worker transport is opened here. */
  /** The logical alias a command selection names. It is neither a Worker ID nor an executor kind. */
  readonly workerKey: string;
  readonly workerEndpoint: string;
  readonly commandToken: string;
  readonly eventToken: string;
  readonly readinessMode: ReadinessMode;
}

export interface NodeWorkerHostRoot {
  readonly workerId: string;
  readonly hostRoot: string;
}

export interface NodeWorkerScopeRule {
  readonly scope: WorkerSelectionScope;
  readonly workerKey: string;
}

/**
 * Which local Worker a fixed executor label names on this Node.
 *
 * This mapping is deliberately explicit and is never derived from a Worker's
 * `target.identity.executorKind`. That field is an unconstrained operator
 * string (`src/shared/router-worker.ts` types it `readonly executorKind: string`),
 * so deriving the mapping would make the `codex` label work or silently stop
 * working depending on whether an operator happened to spell the kind
 * `codex` rather than, say, `codex-cli`. The ticket's rule is the same one:
 * a label names a kind of executor, and the Node configures which Worker that
 * kind is here.
 */
export interface NodeWorkerExecutorBinding {
  readonly executorLabel: ExecutorLabel;
  readonly workerKey: string;
}

export interface NodeRuntimeConfig {
  readonly contractVersion: typeof NODE_CONTRACT_VERSION;
  readonly database: string;
  readonly nodeId: string;
  readonly nodeRef: string;
  readonly nodeVersion: string;
  readonly token: string;
  readonly host: string;
  readonly port: number;
  readonly eventHost: string;
  readonly eventPort: number;
  readonly routerEndpoint: string;
  readonly workers: readonly NodeRuntimeWorkerConfig[];
  /**
   * Present exactly when `workers` is non-empty. An empty inventory has no key
   * for it to name, so it is absent then.
   *
   * Still derived from the configured inventory and never from which
   * Workers happen to be reachable. A single-Worker Node may now leave
   * `defaultWorker` unset and have this filled in from that one
   * entry; two or more Workers must still name the default explicitly. An
   * offline Worker does not make its Node single-Worker, because `workers` is
   * the configuration list, not a liveness view.
   */
  readonly defaultWorkerKey?: string;
  /** Team/repo rules, owned here rather than by the Relay. */
  readonly workerScopeRules: readonly NodeWorkerScopeRule[];
  /** The fixed executor labels this Node can satisfy. */
  readonly workerExecutorBindings: readonly NodeWorkerExecutorBinding[];
  readonly workerHostRoots: readonly NodeWorkerHostRoot[];
  readonly workerDispatchTimeoutMs: number;
  readonly routerEventTimeoutMs: number;
  readonly workerReadinessTimeoutMs: number;
  readonly eventRetryInitialDelayMs: number;
  readonly eventRetryMaxDelayMs: number;
  readonly eventMaxConcurrentRuns: number;
}

/** A safe local failure: configuration values and tokens are intentionally not reflected. */
export class NodeWorkerSelectionError extends Error {
  constructor() {
    super("The requested Node Worker is not configured.");
    this.name = "NodeWorkerSelectionError";
  }
}

/**
 * The Node is configured and running but carries no Worker inventory at
 * all, so no selection -- default or explicit -- can resolve. Kept distinct from
 * its base class because the two answer different operator questions: this one
 * means "deploy a Worker on this Node", while a bare `NodeWorkerSelectionError`
 * means "this Node has Workers, but not the one you asked for".
 */
export class NodeWorkerInventoryEmptyError extends NodeWorkerSelectionError {
  constructor() {
    super();
    this.message = "This Node has no configured Worker inventory.";
    this.name = "NodeWorkerInventoryEmptyError";
  }
}

/**
 * The source named two or more different executor kinds at once, so
 * there is no single answer to "which executor should run this".
 *
 * Kept distinct from its base class because the operator answer differs: a bare
 * `NodeWorkerSelectionError` is answered by configuring or deploying a Worker; this
 * one is answered on the source, by removing a label.
 */
export class NodeWorkerSelectionConflictError extends NodeWorkerSelectionError {
  constructor(readonly executorLabels: readonly ExecutorLabel[]) {
    super();
    this.message = "The request names more than one executor.";
    this.name = "NodeWorkerSelectionConflictError";
  }
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

/**
 * Resolves a command logical selection against this Node's immutable local inventory.
 * It deliberately returns a detached reservation and does not dispatch or open a transport.
 */
export function selectNodeInventoryWorker(
  config: Pick<NodeRuntimeConfig, "workers" | "defaultWorkerKey" | "workerScopeRules" | "workerExecutorBindings">,
  value: unknown,
): NodeReservationEvidence {
  let selection: WorkerSelection;
  try { selection = validateWorkerSelection(value); } catch { throw new NodeWorkerSelectionError(); }
  // Checked after the selection itself validates, so a malformed
  // selection is still reported as malformed rather than as a deployment gap.
  if (config.workers.length === 0) throw new NodeWorkerInventoryEmptyError();
  const workerKey = resolveSelectedWorkerKey(config, selection);
  if (workerKey === undefined) throw new NodeWorkerSelectionError();
  const worker = config.workers.find((candidate) => candidate.workerKey === workerKey);
  if (worker === undefined) throw new NodeWorkerSelectionError();
  return deepFreeze({ workerKey, target: structuredClone(worker.target) });
}

/**
 * The whole of this Node's selection policy, in one place.
 *
 * The order is executor label, then Team/repo rule, then default, and it is
 * applied only here -- on a *new* session's first assignment. A continuation
 * never reaches this function at all: `src/node/command.ts` gives a follow-up
 * `admitRetainedReservation` and only a `start` gets `resolveNewSessionReservation`,
 * which is what makes "relabelling an issue does not move an open session"
 * true by construction rather than by a comparison somebody has to remember.
 *
 * Every refusal below is deliberate. Once a request names an executor, or
 * matches a scope rule, the Node either honours it or fails -- it never slides
 * down to the next tier, because a silent demotion to the default Worker is
 * exactly the outcome an operator who wrote the rule was trying to prevent.
 * The tiers are a priority order for *which rule applies*, not a fallback
 * chain for when one cannot be satisfied.
 */
function resolveSelectedWorkerKey(
  config: Pick<NodeRuntimeConfig, "defaultWorkerKey" | "workerScopeRules" | "workerExecutorBindings">,
  selection: WorkerSelection,
): string | undefined {
  if (selection.executorLabels.length > 1) throw new NodeWorkerSelectionConflictError(selection.executorLabels);
  const [executorLabel] = selection.executorLabels;
  if (executorLabel !== undefined) {
    // No default fallback: an issue labelled `claude` on a Node with no Claude
    // Worker is a definite failure. Running it on Codex instead would be a
    // wrong answer that looks like a right one.
    return config.workerExecutorBindings.find(binding => binding.executorLabel === executorLabel)?.workerKey;
  }
  const scope = selection.scope;
  const rule = config.workerScopeRules.find(candidate =>
    candidate.scope.system === scope.system && candidate.scope.tenantId === scope.tenantId && candidate.scope.scopeId === scope.scopeId);
  // An unmatched scope is not a failure -- it is the ordinary case the default
  // Worker exists for. A *matched* scope whose Worker is missing cannot happen
  // here, because the configuration loader refuses such a rule at startup.
  return rule === undefined ? config.defaultWorkerKey : rule.workerKey;
}
