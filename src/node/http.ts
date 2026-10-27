import { createServer, type IncomingMessage, type RequestListener, type Server, type ServerResponse } from "node:http";
import type { Socket } from "node:net";

import type { NodeCommandExecutionDisposition } from "./command.js";
import { nodeEventIngressHandler, type NodeEventIngress } from "./event-ingress.js";
import type { NodeMaintenanceServicePort } from "./maintenance.js";
import type { NodeReadiness } from "./readiness.js";
import type { NodeRuntimeConfig } from "./runtime-config.js";
import {
  NodeContractError,
  NODE_ERROR_STATUS,
  type NodeErrorCode,
} from "../shared/router-node.js";
import {
  nodeCommandError,
  type NodeReceipt,
} from "../shared/router-node-commands.js";
import {
  enforceNoNodeBody,
  failNode,
  matchRawNodeRoute,
  numericLoopback,
  rawHeaderValues,
  readNodeJsonBody,
  secureEqual,
  sendNodeError,
  sendNodeJson,
} from "./http-transport.js";

type NodeHttpConfiguration = Pick<NodeRuntimeConfig, "token" | "host" | "port" | "eventHost" | "eventPort" | "nodeRef" | "nodeVersion" | "workers">;

export interface NodeHttpCommandService {
  executeWithDisposition(value: unknown): Promise<NodeCommandExecutionDisposition>;
  readReceipt(value: unknown): Promise<NodeReceipt>;
}

export interface NodeHttpOptions {
  readonly configuration: NodeHttpConfiguration;
  /** The Node's command service; the command listener serves only the command contract. */
  readonly commandService: NodeHttpCommandService;
  readonly readiness: () => Promise<NodeReadiness> | NodeReadiness;
  readonly eventIngress: NodeEventIngress;
  readonly maintenance: NodeMaintenanceServicePort;
  readonly closeTimeoutMs?: number;
}

export interface NodeHttpService {
  readonly server: Server;
  readonly eventServer: Server;
  readonly url: URL;
  readonly eventUrl: URL;
  close(): Promise<void>;
}

const DEFAULT_CLOSE_TIMEOUT_MS = 30_000;
const CLOSE_ERROR_MESSAGE = "The Node HTTP service did not close before its local deadline.";
const START_ERROR_MESSAGE = "The Node HTTP service could not start its listeners.";

function authenticate(request: IncomingMessage, token: string): void {
  const values = rawHeaderValues(request, "authorization");
  const candidate = values.length === 1 ? values[0] ?? "" : "";
  if (values.length !== 1 || !secureEqual(candidate, `Bearer ${token}`)) failNode("axis.node.unauthorized");
}

function sendCommand(response: ServerResponse, result: NodeCommandExecutionDisposition): void {
  sendNodeJson(response, 202, result.receipt);
}

function sendNodeCommandError(response: ServerResponse, code: NodeErrorCode, status = NODE_ERROR_STATUS[code]): void {
  sendNodeJson(response, status, nodeCommandError(code));
}

function sendFailure(response: ServerResponse, error: unknown): void {
  sendNodeCommandError(response, error instanceof NodeContractError ? error.code : "axis.node.storage_unavailable");
}


/**
 * Every refusal the command listener sends is a `NodeCommandError` body -- authentication, route,
 * method, media and size rejections raised before a service is reached included, classified by
 * `NODE_DEFINITE_ERROR_CODES`. The event listener is
 * Worker-facing and keeps its own envelope.
 */
function commandListener(options: NodeHttpOptions, track: (operation: Promise<void>) => void): RequestListener {
  return (request, response) => {
    const operation = handle(request, response).catch((error: unknown) => {
      if (!response.headersSent) sendFailure(response, error);
      else response.destroy();
    });
    track(operation);
    void operation;
  };

  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const target = matchRawNodeRoute(request, ["/v1/node/readiness", "/v1/node/commands", "/v1/node/command-receipts"] as const);
    const route = target.route;
    const expectedMethod = route === "/v1/node/readiness" ? "GET"
      : route === "/v1/node/commands" || route === "/v1/node/command-receipts" ? "POST"
        : undefined;
    if (expectedMethod === undefined) {
      sendNodeCommandError(response, "axis.node.target_unknown");
      return;
    }
    if (request.method !== expectedMethod) {
      response.setHeader("allow", expectedMethod);
      sendNodeCommandError(response, "axis.node.malformed", 405);
      return;
    }
    authenticate(request, options.configuration.token);
    if (target.query !== "") failNode();

    if (route === "/v1/node/readiness") {
      await enforceNoNodeBody(request);
      let value: NodeReadiness;
      try { value = await options.readiness(); }
      catch { failNode("axis.node.storage_unavailable"); }
      sendNodeJson(response, 200, value);
      return;
    }

    const parsed = await readNodeJsonBody(request);
    // The command service validates the exact contract version, so any other body is refused as
    // malformed before any effect.
    if (route === "/v1/node/commands") {
      sendCommand(response, await options.commandService.executeWithDisposition(parsed));
      return;
    }
    sendNodeJson(response, 200, await options.commandService.readReceipt(parsed));
  }
}

/**
 * Which listener failed to bind and the bounded `errno` code it failed with, appended to the
 * fixed start message, so `EADDRINUSE`, `EACCES` and `EADDRNOTAVAIL` are distinguishable. Only the
 * listener name and the code are carried: both are bounded values in the sense `docs/SECURITY-MODEL.md`
 * requires. The address, the raw error and its stack are not.
 */
function startFailure(listener: "command" | "event", cause: unknown): Error {
  const code = cause instanceof Error ? (cause as NodeJS.ErrnoException).code : undefined;
  return new Error(code === undefined ? START_ERROR_MESSAGE : `${START_ERROR_MESSAGE} (${listener} listener: ${code})`);
}

function listen(server: Server, port: number, host: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const failed = (error: Error) => { server.off("listening", ready); reject(error); };
    const ready = () => { server.off("error", failed); resolve(); };
    server.once("error", failed);
    server.once("listening", ready);
    server.listen(port, host);
  });
}

function localUrl(server: Server): URL {
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error(START_ERROR_MESSAGE);
  return new URL(`http://${address.address.includes(":") ? `[${address.address}]` : address.address}:${address.port}`);
}

function closeServer(server: Server): Promise<void> {
  if (!server.listening) return Promise.resolve();
  return new Promise((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
}

function trackSockets(server: Server, sockets: Set<Socket>): void {
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
}

function forceClose(server: Server, sockets: Set<Socket>): void {
  server.closeAllConnections();
  for (const socket of sockets) socket.destroy();
}

export async function startNodeHttpService(options: NodeHttpOptions): Promise<NodeHttpService> {
  const closeTimeoutMs = options.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
  if (!Number.isSafeInteger(closeTimeoutMs) || closeTimeoutMs < 1 || closeTimeoutMs > 120_000
    || options.configuration.token.trim().length === 0 || !numericLoopback(options.configuration.eventHost)
    || options.configuration.workers.some((worker) => secureEqual(worker.eventToken, options.configuration.token)
      || secureEqual(worker.commandToken, options.configuration.token))) {
    throw new Error(START_ERROR_MESSAGE);
  }

  let accepting = true;
  const admitted = new Set<Promise<void>>();
  const commandSockets = new Set<Socket>(), eventSockets = new Set<Socket>();
  const track = (operation: Promise<void>) => {
    admitted.add(operation);
    void operation.finally(() => admitted.delete(operation)).catch(() => undefined);
  };
  const commandHandler = commandListener(options, track);
  const eventHandler = nodeEventIngressHandler({ configuration: options.configuration, ingress: options.eventIngress, maintenance: options.maintenance, track });
  const gated = (handler: RequestListener, refuse: (response: ServerResponse) => void): RequestListener => (request, response) => {
    if (!accepting) {
      response.setHeader("connection", "close");
      refuse(response);
      return;
    }
    handler(request, response);
  };
  const server = createServer(gated(commandHandler, (response) => sendNodeCommandError(response, "axis.node.storage_unavailable")));
  const eventServer = createServer(gated(eventHandler, (response) => sendNodeError(response, "axis.node.storage_unavailable")));
  trackSockets(server, commandSockets);
  trackSockets(eventServer, eventSockets);

  const bind = async (listener: "command" | "event", target: Server, port: number, host: string): Promise<void> => {
    try { await listen(target, port, host); }
    catch (error) { throw startFailure(listener, error); }
  };

  try {
    await bind("command", server, options.configuration.port, options.configuration.host);
    await bind("event", eventServer, options.configuration.eventPort, options.configuration.eventHost);
  } catch (error) {
    accepting = false;
    forceClose(server, commandSockets);
    forceClose(eventServer, eventSockets);
    await Promise.allSettled([closeServer(server), closeServer(eventServer)]);
    throw error;
  }

  const url = localUrl(server), eventUrl = localUrl(eventServer);
  let closing: Promise<void> | undefined;
  return {
    server,
    eventServer,
    url,
    eventUrl,
    close() {
      if (closing !== undefined) return closing;
      accepting = false;
      const deadline = Date.now() + closeTimeoutMs;
      const listenersClosed = Promise.allSettled([closeServer(server), closeServer(eventServer)]);
      const workClosed = Promise.allSettled([...admitted]);
      const complete = Promise.all([listenersClosed, workClosed]);
      closing = new Promise<void>((resolve, reject) => {
        let deadlineExpired = false;
        const remaining = Math.max(0, deadline - Date.now());
        const timer = setTimeout(() => {
          deadlineExpired = true;
          forceClose(server, commandSockets);
          forceClose(eventServer, eventSockets);
        }, remaining);
        void complete.then(([listenerResults, workResults]) => {
          clearTimeout(timer);
          const failed = [...listenerResults, ...workResults].some(({ status }) => status === "rejected");
          if (deadlineExpired || failed) {
            if (!deadlineExpired) { forceClose(server, commandSockets); forceClose(eventServer, eventSockets); }
            reject(new Error(CLOSE_ERROR_MESSAGE));
          }
          else resolve();
        });
      });
      return closing;
    },
  };
}
