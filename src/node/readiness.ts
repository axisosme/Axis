import { parseStrictRouterWorkerJson, type WorkerVersionedReference } from "../shared/router-worker.js";
import { NODE_CONTRACT_VERSION } from "../shared/router-node.js";
import type { CodexAppServerReadiness } from "../executor/codex.js";
import type { NodeRuntimeConfig, NodeRuntimeWorkerConfig } from "./runtime-config.js";

const READINESS_BODY_LIMIT_BYTES = 64 * 1024;
const roles = ["explorer", "architect", "reviewer", "worker"] as const;
const degradedReasons = ["app_server_unavailable", "app_server_identity_unavailable", "app_server_config_unavailable", "profile_registry_unavailable"] as const;

type Timer = ReturnType<typeof setTimeout>;
type Fetch = (url: string, init: Readonly<{ method: "GET"; headers: Readonly<{ authorization: string }>; redirect: "error"; signal: AbortSignal }>) => Promise<Response>;

export interface NodeReadinessWorker {
  readonly workerId: string;
  readonly transport: "reachable" | "degraded" | "unknown";
  readonly readinessSupport: "supported" | "unsupported";
  readonly readiness?: CodexAppServerReadiness;
}

/** This is intentionally an aggregate, not an overall Node ready/not-ready verdict. */
export interface NodeReadiness {
  readonly contractVersion: typeof NODE_CONTRACT_VERSION;
  readonly nodeRef: WorkerVersionedReference;
  readonly observedAt: string;
  readonly service: { readonly status: "configured" };
  readonly storage: { readonly status: "available" | "unavailable"; readonly safeCode?: "axis.node.storage_unavailable" };
  readonly workers: readonly NodeReadinessWorker[];
}

export interface NodeReadinessDependencies {
  readonly fetch?: Fetch;
  /** Read-only local storage check. It must not create, mutate, or recover a journal. */
  readonly readStorage?: () => Promise<void> | void;
  readonly now?: () => Date;
  readonly setTimeout?: (callback: () => void, delayMs: number) => Timer;
  readonly clearTimeout?: (timer: Timer) => void;
}

function record(value: unknown): Readonly<Record<string, unknown>> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype
    ? value as Readonly<Record<string, unknown>> : undefined;
}

function exact(value: Readonly<Record<string, unknown>>, keys: readonly string[]): boolean {
  return Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}

/** Strictly accepts only the closed Codex HTTP readiness representation. */
export function validateCodexAppServerReadiness(value: unknown): CodexAppServerReadiness | undefined {
  const item = record(value);
  if (item === undefined || !exact(item, ["status", "modelAvailability", "profiles", "degradedReasons"])
    || (item.status !== "ready" && item.status !== "degraded") || item.modelAvailability !== "not_checked" || !Array.isArray(item.degradedReasons)) return undefined;
  const profiles = record(item.profiles);
  if (profiles === undefined || !exact(profiles, roles)) return undefined;
  const normalizedProfiles: Record<string, { availability: "available" | "unavailable"; attempt: "not_attempted" }> = {};
  for (const role of roles) {
    const profile = record(profiles[role]);
    if (profile === undefined || !exact(profile, ["availability", "attempt"])
      || (profile.availability !== "available" && profile.availability !== "unavailable") || profile.attempt !== "not_attempted") return undefined;
    normalizedProfiles[role] = { availability: profile.availability, attempt: "not_attempted" };
  }
  const reasons: CodexAppServerReadiness["degradedReasons"][number][] = [];
  for (const reason of item.degradedReasons) {
    if (typeof reason !== "string" || !(degradedReasons as readonly string[]).includes(reason) || reasons.includes(reason as CodexAppServerReadiness["degradedReasons"][number])) return undefined;
    reasons.push(reason as CodexAppServerReadiness["degradedReasons"][number]);
  }
  if ((item.status === "ready" && reasons.length !== 0) || (item.status === "degraded" && reasons.length === 0)) return undefined;
  return { status: item.status, modelAvailability: "not_checked", profiles: normalizedProfiles as CodexAppServerReadiness["profiles"], degradedReasons: reasons };
}

async function readResponse(response: Response): Promise<unknown> {
  if (response.body === null) throw new Error("missing readiness body");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > READINESS_BODY_LIMIT_BYTES) throw new Error("readiness body too large");
      chunks.push(next.value);
    }
  } finally {
    try { await reader.cancel(); } catch { /* response ownership is best effort */ }
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
  return parseStrictRouterWorkerJson(body);
}

async function probe(worker: NodeRuntimeWorkerConfig, timeoutMs: number, dependencies: NodeReadinessDependencies): Promise<NodeReadinessWorker> {
  if (worker.readinessMode === "unsupported") return { workerId: worker.target.identity.workerId, transport: "unknown", readinessSupport: "unsupported" };
  const fetch = dependencies.fetch ?? ((url, init) => globalThis.fetch(url, init));
  const schedule = dependencies.setTimeout ?? ((callback, delayMs) => setTimeout(callback, delayMs));
  const cancel = dependencies.clearTimeout ?? ((timer) => clearTimeout(timer));
  const abort = new AbortController();
  let timer: Timer | undefined;
  const timeout = new Promise<void>((resolve) => { timer = schedule(() => { abort.abort(); resolve(); }, timeoutMs); });
  const attempt = (async (): Promise<NodeReadinessWorker> => {
    try {
      const response = await fetch(`${worker.workerEndpoint}/v1/worker/readiness`, {
        method: "GET", headers: { authorization: `Bearer ${worker.commandToken}` }, redirect: "error", signal: abort.signal,
      });
      const readiness = validateCodexAppServerReadiness(await readResponse(response));
      if (readiness === undefined || (response.status !== 200 && response.status !== 503)
        || (response.status === 200 && readiness.status !== "ready") || (response.status === 503 && readiness.status !== "degraded")) throw new Error("invalid readiness response");
      return { workerId: worker.target.identity.workerId, transport: readiness.status === "ready" ? "reachable" : "degraded", readinessSupport: "supported", readiness };
    } catch {
      return { workerId: worker.target.identity.workerId, transport: "unknown", readinessSupport: "supported" };
    }
  })();
  try {
    const outcome = await Promise.race([
      attempt.then((value) => ({ kind: "value" as const, value })),
      timeout.then(() => ({ kind: "timeout" as const })),
    ]);
    return outcome.kind === "value" ? outcome.value : { workerId: worker.target.identity.workerId, transport: "unknown", readinessSupport: "supported" };
  } finally {
    if (timer !== undefined) cancel(timer);
  }
}

function uniqueWorkers(workers: readonly NodeRuntimeWorkerConfig[]): readonly NodeRuntimeWorkerConfig[] {
  const seen = new Set<string>();
  return workers.filter((worker) => {
    const workerId = worker.target.identity.workerId;
    if (seen.has(workerId)) return false;
    seen.add(workerId);
    return true;
  });
}

/** Pure, injected readiness aggregation. It starts no service and touches no command or journal mutation path. */
export async function nodeReadiness(config: Pick<NodeRuntimeConfig, "contractVersion" | "nodeRef" | "nodeVersion" | "workers" | "workerReadinessTimeoutMs">, dependencies: NodeReadinessDependencies = {}): Promise<NodeReadiness> {
  const storage = Promise.resolve().then(() => dependencies.readStorage?.()).then(
    () => ({ status: "available" as const }),
    () => ({ status: "unavailable" as const, safeCode: "axis.node.storage_unavailable" as const }),
  );
  const workers = uniqueWorkers(config.workers);
  const workerResults = Promise.all(workers.map((worker) => probe(worker, config.workerReadinessTimeoutMs, dependencies)));
  const observedAt = (dependencies.now ?? (() => new Date()))().toISOString();
  return { contractVersion: NODE_CONTRACT_VERSION, nodeRef: { ref: config.nodeRef, version: config.nodeVersion }, observedAt, service: { status: "configured" }, storage: await storage, workers: await workerResults };
}
