import {
  AxisConfigurationError, deriveNodeConfiguration, deriveRelayConfiguration, deriveWorkerConfiguration,
  readAxisSecret, secretNames, type AxisConfiguration, type LinearSecrets, type SecretReader, type WorkerConfiguration,
} from "./configuration.js";
import { startNode, type NodeRuntime } from "./node/runtime.js";
import { startRelay, type RelayRuntime, type RelayRuntimeOptions } from "./relay/runtime.js";
import { startCodexWorker } from "./worker/runtime.js";
import { canonicalClaudeExecutable, startClaudeWorker } from "./worker/claude-runtime.js";
import type { WorkerHttpService } from "./worker/http.js";
import type { NodeReadiness } from "./node/readiness.js";

export interface ServeWorkerRuntime {
  readonly service: WorkerHttpService;
  close(): Promise<void>;
}
export interface ServeOptions {
  readonly readSecret?: SecretReader;
  readonly linearSecrets?: LinearSecrets;
  readonly environment?: Readonly<Record<string, string | undefined>>;
  readonly signal?: AbortSignal;
  readonly relay?: Pick<RelayRuntimeOptions, "provider" | "audit" | "workerRouting" | "onTickError">;
  /** Trusted embedding seam for deterministic executor fixtures. */
  readonly startWorker?: (config: WorkerConfiguration, signal?: AbortSignal) => Promise<ServeWorkerRuntime>;
}
export interface ServeRuntime {
  readonly relay: RelayRuntime;
  readonly node: NodeRuntime;
  readonly worker: ServeWorkerRuntime;
  ready(): Promise<boolean>;
  close(): Promise<void>;
}

/** Refuse remote routes and listener collisions before reading keys or opening storage. */
export function validateServeConfiguration(config: AxisConfiguration): void {
  const node = config.nodes[0];
  if (config.nodes.length !== 1 || node?.workers.length !== 1) throw new AxisConfigurationError("serve.topology", "required (one Node and one Worker)");
  const worker = node.workers[0]!;
  if (config.linear === undefined) throw new AxisConfigurationError("linear", "required");
  for (const host of [config.router.host, node.host, node.eventHost, worker.host]) {
    if (host !== "127.0.0.1") throw new AxisConfigurationError("serve.host", "required (127.0.0.1)");
  }
  const routerOrigin = new URL(`http://127.0.0.1:${config.router.port}/`).href;
  const nodeOrigin = new URL(`http://127.0.0.1:${node.port}/`).href;
  if (config.router.url !== routerOrigin || node.url !== nodeOrigin) {
    throw new AxisConfigurationError("serve.url", "invalid (must match private listeners)");
  }
  if (new Set([config.relay.port, config.router.port, node.port, node.eventPort, worker.port]).size !== 5) {
    throw new AxisConfigurationError("serve.ports", "invalid (listener collision)");
  }
  if (worker.kind === "claude-code" && worker.claudeExecutable === undefined) throw new AxisConfigurationError("serve.claudeExecutable", "required");
}

/** Compose the existing authenticated HTTP roles in one process; only Relay may bind externally. */
export async function startServe(config: AxisConfiguration, options: ServeOptions = {}): Promise<ServeRuntime> {
  validateServeConfiguration(config);
  options.signal?.throwIfAborted();
  // Snapshot every credential once and reject reuse across all purposes, including different roles.
  const read = options.readSecret ?? (name => readAxisSecret(config, name));
  const keys = new Map(secretNames(config).map(name => [name, read(name)]));
  if (new Set(keys.values()).size !== keys.size) throw new AxisConfigurationError("secrets", "invalid or reused");
  const secret: SecretReader = name => {
    const value = keys.get(name);
    if (value === undefined) throw new AxisConfigurationError("secrets", "required");
    return value;
  };
  const nodeId = config.nodes[0]!.id, workerId = config.nodes[0]!.workers[0]!.id;
  const relayConfig = deriveRelayConfiguration(config, secret, options.linearSecrets);
  const nodeConfig = deriveNodeConfiguration(config, nodeId, secret);
  const workerConfig = deriveWorkerConfiguration(config, workerId, nodeId, secret);
  if (workerConfig.kind === "claude-code") await canonicalClaudeExecutable(workerConfig.executable);
  let node: NodeRuntime | undefined, worker: ServeWorkerRuntime | undefined, relay: RelayRuntime | undefined;
  let stopping = false, started = false;
  let closingLocal: Promise<void> | undefined, closing: Promise<void> | undefined;
  const closeLocal = (): Promise<void> => closingLocal ??= (async () => {
    const failures: unknown[] = [];
    for (const release of [() => worker?.close(), () => node?.close()]) {
      try { await release(); } catch (error) { failures.push(error); }
    }
    if (failures.length !== 0) throw new AggregateError(failures, "Local role shutdown failed.");
  })();
  const localReady = async (): Promise<boolean> => {
    if (stopping || node?.service.server.listening !== true || node.service.eventServer.listening !== true || worker?.service.server.listening !== true) return false;
    try {
      const response = await fetch(new URL("/v1/node/readiness", config.nodes[0]!.url), {
        headers: { authorization: `Bearer ${nodeConfig.token}` }, redirect: "error",
        signal: AbortSignal.timeout(nodeConfig.workerReadinessTimeoutMs + 1_000),
      });
      if (response.status !== 200) { await response.body?.cancel(); return false; }
      const readiness = await response.json() as NodeReadiness;
      return readiness.storage.status === "available" && readiness.workers.length === 1
        && (workerConfig.kind === "claude-code" || readiness.workers[0]?.readiness?.status === "ready");
    } catch { return false; }
  };
  const close = (): Promise<void> => closing ??= (async () => {
    stopping = true;
    if (relay !== undefined) await relay.close();
    else await closeLocal();
  })();
  const ready = async (): Promise<boolean> => started && !stopping && relay !== undefined
    && relay.channel.ready && relay.service.server.listening && relay.routerService.server.listening && await localReady();
  try {
    relay = await startRelay(relayConfig, {
      ...options.relay,
      ready,
      beforeIngress: async () => {
        try {
          options.signal?.throwIfAborted();
          node = await startNode(nodeConfig);
          options.signal?.throwIfAborted();
          worker = await (options.startWorker?.(workerConfig, options.signal) ?? (workerConfig.kind === "codex"
            ? startCodexWorker(workerConfig, options.environment, options.signal)
            : startClaudeWorker(workerConfig, options.environment)));
          options.signal?.throwIfAborted();
          if (!await localReady()) throw new Error("Local roles are not ready.");
          return closeLocal;
        } catch (error) {
          try { await closeLocal(); } catch (cleanup) { throw new AggregateError([error, cleanup], "Local role startup failed."); }
          throw error;
        }
      },
    });
    options.signal?.throwIfAborted();
    if (!relay.channel.ready) throw new Error("Relay is not ready.");
    started = true;
    return { relay, node: node!, worker: worker!, ready, close };
  } catch (error) {
    try { await close(); } catch (cleanup) { throw new AggregateError([error, cleanup], "Axis startup failed."); }
    throw error;
  }
}
