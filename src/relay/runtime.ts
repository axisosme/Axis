import type { RelayConfiguration } from "../configuration.js";
import { startRelayHttpService, type RelayHttpService } from "./http.js";
import { composeLinearChannel, startLinearChannelProcess, type LinearChannel, type LinearChannelProcess, type LinearLoopFailure } from "./linear/composition.js";
import { LinearGraphQlProvider } from "./linear/provider.js";
import { composeLinearApiRuntime, type LinearApiRuntime } from "./linear/api-runtime.js";
import { LinearExecutionAuthority, LinearExecutionCredentialEndpoint } from "./linear/execution-runtime.js";
import { composeNodeRouter } from "./node-routing-composition.js";
import { EncryptedFileCredentialVault, EnvelopeCipher } from "./security.js";
import { startRouterHttpServiceWithNodeEvents, type RouterHttpService } from "../router/http.js";
import { SqliteTeamBindingStore } from "../router/team-bindings.js";
import type { Router } from "../router/router.js";
import type { LinearApiAudit } from "./linear/api-proxy.js";
import type { LinearWorkerRoutingOptions } from "./linear/channel.js";
import type { LinearProviderPort } from "./linear/provider.js";

export interface RelayRuntimeOptions {
  readonly provider?: LinearProviderPort;
  /** Runs after Router opens and before public ingress or channel loops start. */
  readonly beforeIngress?: () => Promise<void | (() => Promise<void>)>;
  readonly ready?: () => boolean | Promise<boolean>;
  readonly workerRouting?: LinearWorkerRoutingOptions;
  readonly audit?: (event: LinearApiAudit) => void;
  readonly onTickError?: (failure: LinearLoopFailure) => void;
}
export interface RelayRuntime {
  readonly service: RelayHttpService;
  readonly routerService: RouterHttpService;
  readonly channel: LinearChannel;
  closeIngress(): Promise<void>;
  close(): Promise<void>;
}

/** Start the Relay and Router from one validated role projection. No process configuration is read here. */
export async function startRelay(config: RelayConfiguration, options: RelayRuntimeOptions = {}): Promise<RelayRuntime> {
  if (config.linear === null) throw new Error("Linear configuration is required for the Relay.");
  const linearConfig = config.linear;
  const cipher = new EnvelopeCipher(config.masterKey);
  const vault = new EncryptedFileCredentialVault(config.vaultPath, cipher);
  const provider = options.provider ?? new LinearGraphQlProvider();
  let teamBindings: SqliteTeamBindingStore | undefined;
  let router: Router | undefined;
  let channel: LinearChannel | undefined;
  let linearProcess: LinearChannelProcess | undefined;
  let linearApi: LinearApiRuntime | null = null;
  let linearExecution: LinearExecutionAuthority | null = null;
  let routerService: RouterHttpService | undefined;
  let service: RelayHttpService | undefined;
  let closeDependents: void | (() => Promise<void>);
  let closing: Promise<void> | undefined;
  let stoppingIngress: Promise<void> | undefined;
  const closeIngress = (): Promise<void> => stoppingIngress ??= (async () => {
    const results = await Promise.allSettled([
      ...(service === undefined ? [] : [service.close()]),
      ...(linearProcess === undefined ? [] : [linearProcess.close()]),
    ]);
    const failures = results.filter(result => result.status === "rejected").map(result => result.reason);
    if (failures.length !== 0) throw new AggregateError(failures, "Relay ingress shutdown failed.");
  })();
  const close = (): Promise<void> => closing ??= (async () => {
    const results: PromiseSettledResult<void>[] = [];
    for (const release of [closeIngress, () => closeDependents?.(), () => routerService?.close()]) {
      results.push(...await Promise.allSettled([Promise.resolve().then(release)]));
    }
    const stores = await Promise.allSettled([
      () => linearApi?.close(), () => linearExecution?.close(), () => channel?.close(), () => router?.close(), () => teamBindings?.close(),
    ].map(async release => release()));
    const failures = [...results, ...stores].filter(result => result.status === "rejected").map(result => result.reason);
    if (failures.length !== 0) throw new AggregateError(failures, "Relay shutdown failed.");
  })();
  try {
    teamBindings = new SqliteTeamBindingStore({ filename: config.runtime.routerDatabase });
    const composedRouter = composeNodeRouter(config, {
      teamBindings,
      beforeNodeDispatch: (request, port, prior) => linearExecution?.prepareNode(request, port, prior),
    });
    router = composedRouter;
    const composedChannel = composeLinearChannel(linearConfig, {
      cipher, vault, provider, router: composedRouter,
      workerRouting: options.workerRouting ?? { audit: { record() {} } },
    });
    channel = composedChannel;
    linearExecution = config.linearExecution === null ? null : new LinearExecutionAuthority({ config: config.linearExecution, router: composedRouter, installations: composedChannel.installations });
    linearApi = composeLinearApiRuntime(config.linearApi, { channel: composedChannel, provider, executions: linearExecution, audit: options.audit ?? (() => {}) });
    routerService = await startRouterHttpServiceWithNodeEvents({ router: composedRouter,
      host: config.runtime.routerHost, port: config.runtime.routerPort, nodeEventBindings: config.eventBindings });
    closeDependents = await options.beforeIngress?.();
    service = await startRelayHttpService({ linear: composedChannel.ingress, ready: async () => composedChannel.ready && (await options.ready?.() ?? true),
      diagnostics: () => composedChannel.diagnose(), oauth: composedChannel.oauth, operatorToken: config.operatorToken, host: config.host, port: config.port,
      ...(linearApi === null ? {} : { linearApi: linearApi.proxy }),
      ...(linearExecution === null || linearApi?.access == null || config.linearExecution === null ? {} : {
        linearExecution: new LinearExecutionCredentialEndpoint({ authority: linearExecution, access: linearApi.access, receivers: config.linearExecution.receivers }),
      }),
    });
    linearProcess = startLinearChannelProcess({ ticks: composedChannel.ticks, intervals: linearConfig.tickIntervals,
      onTickError: options.onTickError ?? (() => {}) });
    return { service, routerService, channel: composedChannel, closeIngress, close };
  } catch (error) {
    try { await close(); } catch (cleanupError) { throw new AggregateError([error, cleanupError], "Relay startup failed."); }
    throw error;
  }
}
