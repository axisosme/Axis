import { createRequire } from "node:module";
import { dirname, join } from "node:path";

import { CodexExecutor, PackageLocalCodexAppServerTransport, codexExecutorRegistration, resolveCodexAgentProfileRegistry } from "../executor/codex.js";
import { LinearExecutionClient, linearExecutionEnvironment } from "../shared/linear-execution-client.js";
import { LinearCodexExecutor } from "./linear-codex-executor.js";
import { RouterHttpEventSink } from "./router-event-sink.js";
import { SqliteWorkerCommandJournal, WorkerCommandService } from "./command.js";
import { startWorkerHttpService, type WorkerHttpService } from "./http.js";
import type { WorkerConfiguration } from "../configuration.js";

/** Resolve only the installed package entry; PATH lookup and runtime fallbacks are forbidden. */
export function packageLocalCodexExecutable(resolve: NodeRequire["resolve"] = createRequire(import.meta.url).resolve): string {
  const manifest = resolve("@openai/codex/package.json");
  return join(dirname(manifest), "bin", "codex.js");
}

/** Deployment configuration is already validated; environment belongs only to native children. */
export async function startCodexWorker(config: WorkerConfiguration, environment: Readonly<Record<string, string | undefined>> = process.env, signal?: AbortSignal) {
  signal?.throwIfAborted();
  if (config.kind !== "codex") throw new Error("The Worker configuration requires Codex.");
  const linear = config.linearExecution;
  const instructions = config.instructions;
  const nativeEnvironment: Readonly<Record<string, string | undefined>> = { ...environment, ...(config.codexHome === undefined ? {} : { CODEX_HOME: config.codexHome }) };
  const codexHome = nativeEnvironment.CODEX_HOME ?? (nativeEnvironment.HOME === undefined ? undefined : join(nativeEnvironment.HOME, ".codex"));
  const options = {
    command: packageLocalCodexExecutable(),
    agentProfiles: () => resolveCodexAgentProfileRegistry(codexHome === undefined ? {} : { codexHome }),
    workerFallbackPolicy: config.profileFallbackPolicy,
    ...(instructions === undefined ? {} : { instructions }),
  };
  // A disabled Worker and a configured Run without authority receive distinct status instructions.
  const executor = linear === null
    ? new CodexExecutor({ ...options, transport: new PackageLocalCodexAppServerTransport({ ...options,
      executionEnvironment: linearExecutionEnvironment(nativeEnvironment, undefined, undefined, "disabled") }) })
    : new LinearCodexExecutor({ ...options, environment: nativeEnvironment, linear: { relay: linear.relay, client: new LinearExecutionClient(linear) } });
  let journal: SqliteWorkerCommandJournal | undefined;
  let worker: WorkerCommandService | undefined;
  let service: WorkerHttpService | undefined;
  const abort = (): void => executor.close();
  signal?.addEventListener("abort", abort, { once: true });
  try {
    journal = new SqliteWorkerCommandJournal(config.database);
    // Registration includes every supported operation, including continuation.
    const registrations = [codexExecutorRegistration({ executorId: config.executorId, adapter: executor })];
    const composedWorker = new WorkerCommandService({ configuration: { bearerToken: config.token, workerId: config.workerId, runtimeKind: "codex", executorId: config.executorId, nodeId: config.nodeId, runtimeProfile: config.runtimeProfile, methodRef: { version: "v1", method: "codex.execute" }, workspaces: config.workspaces }, registrations, eventSink: new RouterHttpEventSink({ endpoint: config.routerEndpoint, bearerToken: config.routerToken }) , journal });
    worker = composedWorker;
    await executor.readiness();
    signal?.throwIfAborted();
    await composedWorker.recoverActiveRuns();
    signal?.throwIfAborted();
    await composedWorker.replayUndeliveredEvents();
    signal?.throwIfAborted();
    service = await startWorkerHttpService({ commandService: composedWorker, readiness: () => executor.readiness(), host: config.host, port: config.port });
    signal?.throwIfAborted();
    const openedService = service;
    const openedJournal = journal;
    return { service, close: async () => { try { await openedService.close(); } finally { composedWorker.close(); executor.close(); openedJournal.close(); } } };
  } catch (error) {
    try { await service?.close(); } finally { worker?.close(); executor.close(); journal?.close(); }
    throw error;
  } finally { signal?.removeEventListener("abort", abort); }
}
