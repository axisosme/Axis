import { ClaudeCodeExecutor, claudeCodeExecutorRegistration } from "../executor/claude-code.js";
import { LinearExecutionClient } from "../shared/linear-execution-client.js";
import { realpath } from "node:fs/promises";
import { isAbsolute } from "node:path";
import type { WorkerConfiguration } from "../configuration.js";
import { SqliteWorkerCommandJournal, WorkerCommandService } from "./command.js";
import { startWorkerHttpService } from "./http.js";
import { RouterHttpEventSink } from "./router-event-sink.js";

export async function canonicalClaudeExecutable(configured: string | undefined, canonicalize: (path: string) => Promise<string> = realpath): Promise<string> {
  if (configured === undefined || configured.trim().length === 0) throw new Error("Claude executable is required.");
  if (!isAbsolute(configured)) throw new Error("Claude executable is invalid.");
  let executable: string;
  try { executable = await canonicalize(configured); } catch { throw new Error("Claude executable is invalid."); }
  if (!isAbsolute(executable)) throw new Error("Claude executable is invalid.");
  return executable;
}

/** Native account environment is independent from the selected deployment role. */
export async function startClaudeWorker(config: WorkerConfiguration, environment: Readonly<Record<string, string | undefined>> = process.env) {
  if (config.kind !== "claude-code") throw new Error("The Worker configuration requires Claude Code.");
  const executable = await canonicalClaudeExecutable(config.executable);
  const linear = config.linearExecution;
  const instructions = config.instructions;
  const executor = new ClaudeCodeExecutor({
    command: executable,
    environment,
    ...(linear === null ? {} : { linear: { relay: linear.relay, client: new LinearExecutionClient(linear) } }),
    ...(instructions === undefined ? {} : { instructions }),
    ...(config.nativeSessionStorePath === undefined ? {} : { nativeSessionStorePath: config.nativeSessionStorePath }),
  });
  let journal: SqliteWorkerCommandJournal | undefined;
  let worker: WorkerCommandService | undefined;
  try {
    journal = new SqliteWorkerCommandJournal(config.database);
    // Fail closed before binding the private listener.
    await executor.preflight();
    const composedWorker = new WorkerCommandService({
      configuration: {
        bearerToken: config.token,
        workerId: config.workerId,
        runtimeKind: "claude-code",
        executorId: config.executorId,
        nodeId: config.nodeId,
        runtimeProfile: config.runtimeProfile,
        methodRef: { version: "v1", method: "claude-code.execute" },
        workspaces: config.workspaces,
      },
      registrations: [claudeCodeExecutorRegistration({ executorId: config.executorId, adapter: executor })],
      eventSink: new RouterHttpEventSink({ endpoint: config.routerEndpoint, bearerToken: config.routerToken }),
      journal,
    });
    worker = composedWorker;
    // The adapter has no active-run recovery proof. Resolve retained non-terminal
    // runs as observational and release the recovery gate before accepting commands.
    await composedWorker.recoverActiveRuns();
    await composedWorker.replayUndeliveredEvents();
    const service = await startWorkerHttpService({ commandService: composedWorker, host: config.host, port: config.port });
    const openedJournal = journal;
    return {
      service,
      nativeSessionLaunchCount: () => executor.nativeSessionLaunchCount(),
      close: async () => { try { await service.close(); } finally { composedWorker.close(); executor.close(); openedJournal.close(); } },
    };
  } catch (error) {
    worker?.close();
    executor.close();
    journal?.close();
    throw error;
  }
}
