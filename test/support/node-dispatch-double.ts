import { createServer } from "node:http";

import type { NodeRuntimeWorkerConfig } from "../../src/node/runtime-config.js";
import { HttpNodeWorkerClient } from "../../src/node/worker-client.js";
import { ROUTER_NODE_EVENT_HTTP_PATH } from "../../src/router/http.js";
import type { RouterNodePortResult, RouterNodePort } from "../../src/router/node-port.js";
import { RouterWorkerDispatchError, type Router, type RouterEventReceipt, type RouterNodeEventOrigin } from "../../src/router/router.js";
import { NODE_COMMAND_CONTRACT_VERSION, completeNodeWorkerCommand, validateNodePriorRoute, type NodeAuthenticatedContext, type NodeCommandRequest, type NodePriorRoute, type NodeReceiptQuery, type NodeReservationEvidence } from "../../src/shared/router-node-commands.js";
import { nodeKnownWorkerFailure, type NodeTarget } from "../../src/shared/router-node.js";

import type { RouterRunLifecycleEnvelope, WorkerCommand, WorkerCommandResponse, WorkerExecutionIdentity, WorkerVersionedReference } from "../../src/shared/router-worker.js";
import { runtimeSourceSessionIdentity } from "../../src/shared/runtime-boundary.js";

/**
 * Node command carrier for channel tests with their own Worker dispatcher.
 * completeNodeWorkerCommand constructs the Worker command from Router's request, then
 * the double wraps the Worker's answer in a command receipt. The Worker-facing command
 * therefore follows production completion rules.
 *
 * The reservation is fixed at construction; this double does not exercise Worker
 * selection, HTTP transport or intermediate reserved/dispatching/ambiguous receipts.
 * Use the real Node for those behaviors.
 */
/**
 * Worker command dispatcher used by a Node double or an HTTP-backed Worker hop: one
 * completed Worker command in, one Worker response out.
 */
export interface WorkerCommandDispatcher { dispatch(command: WorkerCommand): Promise<WorkerCommandResponse>; }

export interface NodeDispatchDoubleOptions {
  /** The test's own executor double. Its `dispatch` decides every outcome this port reports. */
  readonly worker: WorkerCommandDispatcher;
  /** The logical Node every `nodeRoutes` entry in the companion resolver points at. */
  readonly nodeRef: WorkerVersionedReference;
  /**
   * What the Node reports it reserved. `target.identity` must equal the
   * identity the Worker double answers with -- `validateNodeReceipt`
   * rejects a receipt whose result identity disagrees with its evidence, so
   * a mismatch here fails loudly rather than silently routing elsewhere.
   */
  readonly reservation: NodeReservationEvidence;
}

/** How `NodeDispatchDouble#failNextResponse` breaks the Node-to-Router hop. */
export type NodeResponseFault = "lost" | "type_error" | "identity_drift";

export class NodeDispatchDouble implements RouterNodePort {
  readonly authenticatedNode: NodeAuthenticatedContext;
  readonly #worker: WorkerCommandDispatcher;
  readonly #reservation: NodeReservationEvidence;
  readonly #priors = new Map<string, NodePriorRoute>();
  /** The receipt each commandId produced, retained so a re-drive replays rather than re-executes. */
  readonly #retained = new Map<string, unknown>();

  constructor(options: NodeDispatchDoubleOptions) {
    this.#worker = options.worker;
    this.#reservation = options.reservation;
    // A real Node authenticates as the physical node its reservation names,
    // so there is nothing to override: taking it from the reservation is what
    // keeps the two from disagreeing.
    this.authenticatedNode = Object.freeze({
      nodeRef: options.nodeRef,
      nodeId: options.reservation.target.identity.nodeId,
    });
  }

  /** The predecessor route for a non-start command, keyed exactly as Router keys its own: by run id. */
  #prior(request: NodeCommandRequest): NodePriorRoute | undefined {
    if (request.command.command === "start") return undefined;
    const runId = request.command.command === "continue" ? request.command.continuation.priorRunId : request.command.target.runId;
    return this.#priors.get(runId);
  }

  /** Retains the route a completed start/continue established, so the next command on that run can be validated against it. */
  #retainPrior(request: NodeCommandRequest, command: WorkerCommand): void {
    if (command.command === "cancel" || command.command === "steer" || request.command.command === "cancel" || request.command.command === "steer") return;
    const source = runtimeSourceSessionIdentity(command.start.identity.task.sourceRef);
    if (source.sessionId === undefined) throw new Error("The Node dispatch double requires a source session.");
    this.#priors.set(command.start.identity.run.runId, validateNodePriorRoute({
      nodeRef: request.nodeRef,
      requestedSelection: request.target.selection,
      reservation: this.#reservation,
      sessionBinding: command.start.identity.sessionBinding,
      sourceSystem: source.system,
      sourceSessionId: source.sessionId,
      sourceRef: command.start.identity.task.sourceRef,
      workspacePolicyRef: request.command.workspacePolicyRef,
    }));
  }

  async #receipt(request: NodeCommandRequest): Promise<unknown> {
    const prior = this.#prior(request);
    const command = await completeNodeWorkerCommand(request, this.#reservation, prior);
    const common = { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: request.nodeRef, commandId: request.command.commandId } as const;
    let result;
    try {
      result = await this.#worker.dispatch(command);
    } catch (error) {
      // A real Node converts a DEFINITE executor failure into a `rejected`
      // receipt and leaves everything else to surface as a transport failure.
      //
      // The disposition is load-bearing and an earlier cut dropped it. An
      // `ambiguous` failure means the Node does not know whether the executor
      // ran; answering `rejected` would tell Router it definitely did not, and
      // Router would report the session as never started while the work was in
      // fact in flight. Ambiguity has to stay ambiguous, which at this boundary
      // means an unread response, not a refusal.
      if (!(error instanceof RouterWorkerDispatchError) || error.disposition === "ambiguous") throw error;
      return { ...common, status: "rejected", reservation: this.#reservation, failure: nodeKnownWorkerFailure(error.safeCode) };
    }
    this.#retainPrior(request, command);
    const sessionBinding = command.command === "cancel" || command.command === "steer" ? prior!.sessionBinding : command.start.identity.sessionBinding;
    return { ...common, status: "accepted", evidence: { ...this.#reservation, sessionBinding }, result };
  }

  /**
   * Lose the next Node-to-Router response after the Worker executes and the receipt is
   * retained. Router can re-drive through readReceipt without another execution. The
   * injection clears when it fires so it cannot affect later commands.
   */
  loseNextResponse(): void { this.failNextResponse("lost"); }

  /**
   * Fail the Node-to-Router hop after the Worker executes and the correct receipt is
   * retained. lost models an unread transport response, type_error an unexpected
   * exception and identity_drift a response with mismatched Worker evidence. The
   * before_dispatch control fails before Worker dispatch instead.
   */
  failNextResponse(fault: NodeResponseFault): void { this.#responseFault = fault; }
  #responseFault: NodeResponseFault | null = null;

  async dispatch(request: NodeCommandRequest): Promise<RouterNodePortResult> {
    const body = await this.#receipt(request);
    this.#retained.set(request.command.commandId, body);
    const fault = this.#responseFault;
    this.#responseFault = null;
    if (fault === "lost") throw new RouterWorkerDispatchError("axis.worker.transport_unknown", "ambiguous");
    if (fault === "type_error") throw new TypeError("the Node response to Router raised after the Worker accepted");
    if (fault === "identity_drift") {
      const drifted = structuredClone(body) as { result?: { identity?: Record<string, unknown> } };
      if (drifted.result?.identity === undefined) throw new Error("identity_drift needs an accepted start receipt");
      drifted.result.identity = { ...drifted.result.identity, workerId: "drifted-worker" };
      return { status: 202, body: drifted, authenticatedNode: this.authenticatedNode };
    }
    return { status: 202, body, authenticatedNode: this.authenticatedNode };
  }

  /**
   * Worker origin that a real Node ingress authenticates independently. Router's Node
   * event ingress uses this identity as authority, not merely as an envelope label.
   */
  get eventOrigin(): Extract<RouterNodeEventOrigin, { readonly authentication: "authenticated" }> {
    return { authentication: "authenticated", node: this.authenticatedNode, identity: this.#reservation.target.identity };
  }

  /**
   * Replay the retained receipt for commandId without dispatching again. Router uses
   * this read after transport failure; manufacturing a fresh receipt would hide
   * identity conflicts and duplicate Worker execution.
   */
  async readReceipt(query: NodeReceiptQuery): Promise<RouterNodePortResult> {
    const retained = this.#retained.get(query.commandId);
    return {
      status: 200, authenticatedNode: this.authenticatedNode,
      body: retained ?? { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: query.nodeRef, commandId: query.commandId, status: "not_found" },
    };
  }
}

/** Builds a channel test reservation from its Worker target and Node inventory key. */
export function nodeReservationFromEndpoint(endpoint: NodeTarget, workerKey: string): NodeReservationEvidence {
  return Object.freeze({ workerKey, target: Object.freeze({ ...endpoint }) });
}

/**
 * Ingests one Worker lifecycle envelope through the Node event door and
 * returns the projected receipt, failing loudly on any other disposition.
 *
 * The strictness is the point. `ingestNodeRunEvent` can also answer
 * `buffered` (no execution evidence yet) or `quarantined` (origin refused),
 * and both are green-looking non-events: a test whose event never
 * reached the Task would still pass every later assertion about what the Task
 * did NOT contain. A caller that wants one of those dispositions asserts on
 * `ingestNodeRunEvent` directly instead of reaching for this.
 */
export async function projectNodeEvent(
  router: Pick<Router, "ingestNodeRunEvent">,
  port: NodeDispatchDouble,
  input: { readonly envelope: RouterRunLifecycleEnvelope; readonly recordedAt: string },
): Promise<RouterEventReceipt> {
  const result = await router.ingestNodeRunEvent({ ...input, origin: port.eventOrigin });
  if (result.status !== "projected") throw new Error(`Node run event was ${result.status}, not projected.`);
  return result.receipt;
}

/**
 * A Node transport that refuses to be used.
 *
 * For a composition whose Router must EXIST on the command route but must never
 * dispatch -- a wiring test, or a cost measurement over observe/outbox ticks.
 * A permissive stub would quietly accept a dispatch the fixture never meant to
 * make, and fold its cost or its side effects into whatever was being
 * measured; throwing makes that mistake loud instead of merely invisible.
 */
export const unreachableNodePort: RouterNodePort = Object.freeze({
  dispatch() { throw new Error("this fixture must not dispatch a Node command"); },
  readReceipt() { throw new Error("this fixture must not query a Node receipt"); },
});

export const defaultStartWorkerRouting = Object.freeze({ audit: Object.freeze({ record(): void {} }) });

/**
 * Carry completed Worker commands through the real HttpNodeWorkerClient. This exercises
 * the Node's target check, loopback-only endpoint and timeout bounds against a real
 * Worker HTTP listener.
 */
export function httpNodeWorkerPort(options: {
  readonly workerUrl: URL;
  readonly commandToken: string;
  readonly nodeRef: WorkerVersionedReference;
  readonly workerKey: string;
  readonly target: NodeTarget;
  readonly workerDispatchTimeoutMs?: number;
}): WorkerCommandDispatcher {
  const worker: NodeRuntimeWorkerConfig = deepFreeze({
    nodeRef: options.nodeRef, nodeId: options.target.identity.nodeId, target: options.target, workerKey: options.workerKey,
    workerEndpoint: options.workerUrl.origin, commandToken: options.commandToken, eventToken: `${options.workerKey}-unused-event-token`, readinessMode: "unsupported",
  });
  const client = new HttpNodeWorkerClient({ worker, workerDispatchTimeoutMs: options.workerDispatchTimeoutMs ?? 30_000 });
  return { dispatch: (command) => client.dispatch(worker.target, command) };
}
function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

/**
 * Forward authenticated Worker events to Router's real Node event HTTP ingress,
 * stamping the configured Worker origin. Relay the Router response with its status,
 * content type and recomputed content length so the Worker sees the actual disposition.
 *
 * This exercises Router's bearer, sameNode, allowedOrigins and request-shape checks.
 * The forwarder itself does not model a Node's durable buffering, ordering or retry
 * pump. In production that pump classifies and quarantines Router conflicts after
 * acknowledging Worker ingestion; this direct forwarder instead returns the Router
 * refusal to the Worker's sink.
 */
export async function startNodeEventForwarder(options: { readonly routerUrl: URL; readonly nodeRef: WorkerVersionedReference; readonly origin: WorkerExecutionIdentity; readonly workerToken: string; readonly nodeEventToken: string; /**
                                                                                                                                                                                                                                        * Hold
                                                                                                                                                                                                                                        * each
                                                                                                                                                                                                                                        * event
                                                                                                                                                                                                                                        * until
                                                                                                                                                                                                                                        * the
                                                                                                                                                                                                                                        * gate
                                                                                                                                                                                                                                        * resolves
                                                                                                                                                                                                                                        * so
                                                                                                                                                                                                                                        * a
                                                                                                                                                                                                                                        * test
                                                                                                                                                                                                                                        * can
                                                                                                                                                                                                                                        * delay
                                                                                                                                                                                                                                        * Run
                                                                                                                                                                                                                                        * evidence
                                                                                                                                                                                                                                        * until
                                                                                                                                                                                                                                        * the
                                                                                                                                                                                                                                        * failing
                                                                                                                                                                                                                                        * tick
                                                                                                                                                                                                                                        * finishes.
                                                                                                                                                                                                                                        */ readonly gate?: () => Promise<void> }): Promise<{ readonly url: URL; close(): Promise<void> }> {
  const server = createServer((request, response) => { void (async () => {
    if (request.headers.authorization !== `Bearer ${options.workerToken}`) { response.writeHead(401); response.end(); return; }
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    const input = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { readonly envelope: RouterRunLifecycleEnvelope; readonly recordedAt: string };
    if (options.gate !== undefined) await options.gate();
    const forwarded = await fetch(new URL(ROUTER_NODE_EVENT_HTTP_PATH, options.routerUrl), {
      method: "POST",
      headers: { authorization: `Bearer ${options.nodeEventToken}`, "content-type": "application/json" },
      body: JSON.stringify({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: options.nodeRef, origin: options.origin, envelope: input.envelope, recordedAt: input.recordedAt }),
    });
    const text = await forwarded.text();
    response.writeHead(forwarded.status, { "content-type": "application/json", "content-length": String(Buffer.byteLength(text)) });
    response.end(text);
  })().catch(() => response.destroy()); });
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("the Node event forwarder did not bind");
  return {
    url: new URL(`http://127.0.0.1:${address.port}`),
    async close(): Promise<void> {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
    },
  };
}
