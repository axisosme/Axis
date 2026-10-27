import type { NodeConfiguration } from "../configuration.js";
import { NodeCommandService } from "./command.js";
import { NodeEventIngress } from "./event-ingress.js";
import { NodeEventPump } from "./event-pump.js";
import { startNodeHttpService, type NodeHttpService } from "./http.js";
import { SqliteNodeJournal } from "./journal.js";
import { NodeMaintenanceService } from "./maintenance.js";
import { nodeReadiness } from "./readiness.js";
import { RouterEventDelivery, type RouterEventDeliveryOptions, type RouterEventDeliveryPort } from "./router-event-delivery.js";
import { selectNodeInventoryWorker, type NodeRuntimeConfig } from "./runtime-config.js";
import { HttpNodeWorkerClient, NodeWorkerClientDispatcher, type HttpNodeWorkerClientOptions } from "./worker-client.js";
import { NodeContractError } from "../shared/router-node.js";
import type { WorkerExecutionIdentity } from "../shared/router-worker.js";
import { NODE_COMMAND_CONTRACT_VERSION, type NodeWorkerDispatchPort } from "../shared/router-node-commands.js";

export interface NodeRuntime {
  readonly service: NodeHttpService;
  /** Retained sessions whose Worker this start's inventory does not admit; their follow-ups are refused. */
  readonly retainedSessionsWithoutWorker: number;
  close(): Promise<void>;
}

/**
 * The operator-facing lines a started Node writes to stderr, each a fixed code and a count only -- never a
 * Run id, session id, path or credential: one when retained sessions name a Worker the inventory does not admit.
 */
export function nodeStartupDiagnostics(runtime: Pick<NodeRuntime, "retainedSessionsWithoutWorker">): readonly string[] {
  return runtime.retainedSessionsWithoutWorker === 0 ? [] : [JSON.stringify({ code: "axis.node.sessions_without_worker", count: runtime.retainedSessionsWithoutWorker })];
}

/**
 * The Node's two outbound transports. `startNode` supplies the real ones; a test may
 * substitute either. The Worker client dispatches and, for convergence, reads back what a Worker recorded.
 */
export interface NodeRuntimeFactories {
  readonly createWorkerClient: (options: HttpNodeWorkerClientOptions) => NodeWorkerDispatchPort;
  /** The factory owns the distinct Node-event bearer from the private key file. */
  readonly createRouterEventDelivery: (options: Omit<RouterEventDeliveryOptions, "bearerToken">) => RouterEventDeliveryPort;
}

/**
 * The Node's Worker inventory as dispatch sees it. Exported so the routing preflight
 * counts retained sessions through the very construction `startNode` dispatches through, rather than a copy of it.
 * Constructing a client performs no I/O.
 */
export function nodeWorkerDispatcher(
  config: NodeRuntimeConfig,
  createWorkerClient: NodeRuntimeFactories["createWorkerClient"] = (options) => new HttpNodeWorkerClient(options),
): NodeWorkerClientDispatcher {
  return new NodeWorkerClientDispatcher(config.workers.map((worker) => ({
    target: worker.target,
    client: createWorkerClient({ worker, workerDispatchTimeoutMs: config.workerDispatchTimeoutMs }),
  })));
}

async function cleanup(input: { readonly service: NodeHttpService | undefined; readonly pump: NodeEventPump | undefined; readonly journal: SqliteNodeJournal | undefined }): Promise<void> {
  const failures: unknown[] = [];
  if (input.service !== undefined) {
    try { await input.service.close(); } catch (error) { failures.push(error); }
  }
  if (input.pump !== undefined) {
    try { await input.pump.stop(); } catch (error) { failures.push(error); }
  }
  if (input.journal !== undefined) {
    try { input.journal.close(); } catch (error) { failures.push(error); }
  }
  if (failures.length !== 0) throw new AggregateError(failures, "The Node runtime could not release its resources cleanly.");
}

/**
 * The command pump's route classifier. Exported so the pump's handling of every answer it can give is
 * tested against a real journal, not a restatement of it.
 *
 * On a real command journal a command whose receipt is not `accepted` has no event route, so this returns
 * `null`. A route owned by another Node sharing the journal throws `binding_conflict` from the receipt
 * read, and a storage failure throws too. The pump defers that one event on its retry backoff in every
 * such case -- another Node's pump delivers its own events -- and never stops delivery for other Runs.
 *
 * A receipt that is not `accepted` -- `not_found`
 * included, which an accepted route cannot produce -- throws, and so is deferred per event exactly like
 * the other unroutable answers; it never becomes a background failure.
 */
export function nodeDeliveryRouteResolver(
  journal: SqliteNodeJournal,
  authenticatedNode: Parameters<SqliteNodeJournal["readReceipt"]>[0]["authenticatedNode"],
) {
  return async (runId: string) => {
    const route = await journal.readRunRoute(runId);
    if (route === null) return null;
    const receipt = await journal.readReceipt({
      query: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: route.nodeRef, commandId: route.commandId },
      authenticatedNode,
    });
    if (receipt.status !== "accepted") throw new NodeContractError("axis.node.storage_unavailable");
    return { nodeRef: route.nodeRef, origin: route.target.identity } as const;
  };
}

/**
 * The event door's convergence trigger. A refused event converges its Run's command only when
 * that command is still `ambiguous` and the event came from the Worker the route names -- no other
 * Worker's credential can cause a read against it.
 */
async function convergeAmbiguousRun(journal: SqliteNodeJournal, service: NodeCommandService, runId: string, identity: WorkerExecutionIdentity, signal: AbortSignal): Promise<void> {
  const pending = await journal.readAmbiguousCommandForRun(runId);
  if (pending === null) return;
  const owner = pending.target.identity;
  if (owner.nodeId !== identity.nodeId || owner.workerId !== identity.workerId || owner.executorKind !== identity.executorKind
    || owner.runtimeProfile.ref !== identity.runtimeProfile.ref || owner.runtimeProfile.version !== identity.runtimeProfile.version) return;
  await service.convergeAmbiguous(pending.commandId, signal);
}

async function assembleNode(
  config: NodeConfiguration,
  factories: NodeRuntimeFactories,
): Promise<NodeRuntime> {
  let journal: SqliteNodeJournal | undefined;
  let pump: NodeEventPump | undefined;
  let service: NodeHttpService | undefined;
  let retainedSessionsWithoutWorker = 0;
  // Convergence work started by the event door and the open-time sweep. Shutdown -- `close` and a
  // failed startup share `shutdownNode` -- first refuses new work (each trigger checks the signal before it
  // starts a read) and aborts the Worker reads in flight, then waits for what already started, and only then
  // closes the listeners and the journal. So no attempt outlives the journal it writes to, and no read holds
  // shutdown for a dispatch timeout. An aborted read leaves its receipt `ambiguous` for the next open's sweep.
  const background = new Set<Promise<void>>();
  const shutdown = new AbortController();
  const track = (start: () => Promise<void>): void => {
    if (shutdown.signal.aborted) return;
    const tracked = start().catch(() => undefined);
    background.add(tracked);
    void tracked.finally(() => { background.delete(tracked); });
  };
  const shutdownNode = async (): Promise<void> => {
    shutdown.abort();
    // Nothing joins `background` after the abort: `track` refuses synchronously, and a sweep's later reads
    // run inside its own tracked promise. One wait covers everything that started.
    await Promise.all([...background]);
    await cleanup({ service, pump, journal });
  };
  try {
    journal = await SqliteNodeJournal.open(config.database);
    // No inventory refuses startup, empty or not, whatever routes the journal retains. A follow-up
    // for a session whose Worker is absent is refused at dispatch through `admission`, a new
    // session through `selectNodeInventoryWorker`; the count below only reports how many there are.
    const worker = nodeWorkerDispatcher(config, factories.createWorkerClient);
    retainedSessionsWithoutWorker = await journal.retainedSessionsWithoutWorker(worker);
    let pumpFailed = false;
    const authenticatedNode = { nodeRef: { ref: config.nodeRef, version: config.nodeVersion }, nodeId: config.nodeId } as const;
    pump = new NodeEventPump({
      nodeRef: { ref: config.nodeRef, version: config.nodeVersion },
      journal: {
        listPendingPage: (input) => journal!.listPendingPage(input),
        markDelivered: async (eventId) => { await journal!.markDelivered({ eventId, deliveredAt: new Date().toISOString() }); },
        recordDeliveryFailure: async (input) => { await journal!.recordDeliveryFailure(input); },
        quarantine: async (input) => { await journal!.quarantine(input); },
      },
      delivery: factories.createRouterEventDelivery({ endpoint: config.routerEndpoint, timeoutMs: config.routerEventTimeoutMs }),
      resolveDeliveryRoute: nodeDeliveryRouteResolver(journal, authenticatedNode),
      retryInitialDelayMs: config.eventRetryInitialDelayMs,
      retryMaxDelayMs: config.eventRetryMaxDelayMs,
      maxConcurrentRuns: config.eventMaxConcurrentRuns,
      clock: () => new Date().toISOString(),
      schedule: (callback, delayMs) => setTimeout(callback, delayMs),
      cancel: (timer) => clearTimeout(timer as ReturnType<typeof setTimeout>),
      onBackgroundFailure: () => { pumpFailed = true; },
    });
    const openedJournal = journal;
    // The only command service the Node composes.
    const commandService = new NodeCommandService({
      authenticatedNode,
      journal: openedJournal,
      resolveNewSessionReservation: (selection) => selectNodeInventoryWorker(config, selection),
      worker,
      convergeRetryInitialDelayMs: config.eventRetryInitialDelayMs,
      convergeRetryMaxDelayMs: config.eventRetryMaxDelayMs,
    });
    const eventIngress = new NodeEventIngress({
      journal,
      afterRecord: () => pump!.requestDelivery(),
      // An event refused while its command is `ambiguous` is the Worker reporting that it accepted.
      // Convergence is requested without waiting: the refusal stands, and the Worker retries the event.
      onUnroutableRun: ({ runId, identity }: { readonly runId: string; readonly identity: WorkerExecutionIdentity }) => {
        track(() => convergeAmbiguousRun(openedJournal, commandService, runId, identity, shutdown.signal));
      },
    });
    const maintenance = new NodeMaintenanceService({ config, journal });
    await pump.start();
    if (pumpFailed || pump.backgroundFailure !== null) throw new NodeContractError("axis.node.storage_unavailable");
    service = await startNodeHttpService({
      configuration: config,
      commandService,
      readiness: () => nodeReadiness(config, {
        readStorage: async () => {
          if (pumpFailed || pump!.backgroundFailure !== null) throw new NodeContractError("axis.node.storage_unavailable");
          await journal!.listPendingPage({ afterGlobalOrder: 0, limit: 1 });
        },
      }),
      eventIngress,
      maintenance,
    });
    // The open-time sweep, for an `ambiguous` command whose Run will never send the event that
    // would otherwise trigger it. The one full scan of command state, run once per open.
    track(async () => {
      for (const commandId of await openedJournal.listAmbiguousCommandIds()) {
        if (shutdown.signal.aborted) return;
        await commandService.convergeAmbiguous(commandId, shutdown.signal);
      }
    });
  } catch (error) {
    try { await shutdownNode(); } catch { /* Startup exposes only the original safe failure. */ }
    throw error;
  }

  let closing: Promise<void> | undefined;
  return {
    service,
    retainedSessionsWithoutWorker,
    close() {
      closing ??= shutdownNode();
      return closing;
    },
  };
}

/**
 * Validate the distinct Node-event credential before any database or listener opens; it is always required.
 */
/**
 * Compose one standalone Node from a validated role projection.
 * Tests may substitute either outbound transport through `factories`.
 */
export async function startNode(
  config: NodeConfiguration,
  factories: Partial<NodeRuntimeFactories> = {},
): Promise<NodeRuntime> {
  return assembleNode(config, {
    createWorkerClient: factories.createWorkerClient ?? (options => new HttpNodeWorkerClient(options)),
    createRouterEventDelivery: factories.createRouterEventDelivery ?? (options => new RouterEventDelivery({ ...options, bearerToken: config.routerEventToken })),
  });
}
