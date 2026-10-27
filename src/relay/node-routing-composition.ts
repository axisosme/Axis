import { channelBindingResolver } from "../router/binding-resolver.js";
import { ChannelCommandError, channelCommandOutcomeFromRouterResult, channelWorkerSelection, linearTaskRefFromChannelCommand, type ChannelCommandPort } from "../router/channel-command.js";
import { HttpRouterNodePort, type RouterNodePort } from "../router/node-port.js";
import { Router, RouterError, type RouterDefaultTeamBinding, type RouterTeamBindingResolver } from "../router/router.js";
import type { NodeCommandRequest, NodePriorRoute } from "../shared/router-node-commands.js";
import type { RelayConfiguration } from "../configuration.js";

function key(ref: { readonly ref: string; readonly version: string }): string { return `${ref.ref}\u0000${ref.version}`; }

/** One exact Node transport per versioned ref. A missing frozen destination is never re-resolved. */
export function composeNodeRouter(configuration: Pick<RelayConfiguration, "runtime">, input: {
  readonly teamBindings: RouterTeamBindingResolver;
  readonly beforeNodeDispatch?: (request: NodeCommandRequest, port: RouterNodePort, prior: NodePriorRoute | undefined) => void;
}): Router {
  const { runtime } = configuration;
  const { teamBindings } = input;
  const transports = new Map(runtime.nodeEndpoints.map(endpoint => [key(endpoint.nodeRef), new HttpRouterNodePort({
    endpoint: endpoint.endpoint, bearerToken: endpoint.bearerToken,
    authenticatedNode: { nodeRef: endpoint.nodeRef, nodeId: endpoint.nodeId },
  })]));
  const transport = (ref: { readonly ref: string; readonly version: string }) => {
    const port = transports.get(key(ref));
    if (port === undefined) throw new RouterError("axis.router.recovery_required");
    return port;
  };
  const port: RouterNodePort = {
    dispatch: request => transport(request.nodeRef).dispatch(request),
    readReceipt: query => transport(query.nodeRef).readReceipt(query),
  };
  const defaultTarget = runtime.defaultEndpointRef === undefined ? undefined
    : runtime.nodeTargets.find(target => key(target.endpointRef) === key(runtime.defaultEndpointRef!));
  const defaultTeamBinding: RouterDefaultTeamBinding | undefined = defaultTarget === undefined ? undefined : {
    profile: { ...defaultTarget.identity, endpointRef: defaultTarget.endpointRef },
    excludedTeamIds: runtime.defaultExcludedTeamIds,
  };
  return new Router({
    filename: runtime.routerDatabase,
    nodeRouting: {
      bindingResolver: channelBindingResolver({
        linearTeamBindings: teamBindings,
        ...(defaultTeamBinding === undefined ? {} : { linearDefaultBinding: defaultTeamBinding }),
        nodeRoutes: runtime.nodeTargets.map(({ endpointRef, nodeRef }) => ({ endpointRef, nodeRef })),
      }),
      port,
      ...(runtime.nativeSteer === true ? { nativeSteer: true } : {}),
      ...(input.beforeNodeDispatch === undefined ? {} : { beforeDispatch: (request, prior) => input.beforeNodeDispatch!(request, port, prior) }),
    },
  });
}

/** Channel adapter onto Router's Node commands. */
export function nodeChannelCommandPort(router: Router): ChannelCommandPort {
  return Object.freeze({
    async submit(command) {
      const taskRef = linearTaskRefFromChannelCommand(command);
      const base = { ...taskRef, commandId: command.commandId };
      if (command.kind === "start") return channelCommandOutcomeFromRouterResult(await router.startNode({
        ...base, instruction: command.instruction, workerSelection: channelWorkerSelection(command),
      }));
      if (Object.hasOwn(command, "workerSelection")) throw new ChannelCommandError("unmodeled_input");
      return channelCommandOutcomeFromRouterResult(command.kind === "continue"
        ? await router.continueNode({ ...base, instruction: command.instruction })
        : await router.cancelNode(base));
    },
    reconcile: query => router.reconcileCommand(query.commandId),
    reconcileAccepted: query => router.reconcileAcceptedCommand(query.commandId),
    reconcileInterrupted: query => router.reconcileInterruptedCommand(query.commandId),
    reconcileSteered: query => router.reconcileSteeredCommand(query.commandId),
  } satisfies ChannelCommandPort);
}
