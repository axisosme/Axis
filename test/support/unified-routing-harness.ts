import { nodeConfigurationFixture } from "./node-configuration.js";
import assert from "node:assert/strict";
import { createHmac, randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { startNode, type NodeRuntime } from "../../src/node/runtime.js";
import { composeLinearChannel } from "../../src/relay/linear/composition.js";
import { startRelayHttpService } from "../../src/relay/http.js";
import { composeNodeRouter } from "../../src/relay/node-routing-composition.js";
import type { RelayRuntimeConfig } from "../../src/relay/runtime-config.js";
import { EnvelopeCipher } from "../../src/relay/security.js";
import { startRouterHttpServiceWithNodeEvents } from "../../src/router/http.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { workerFailureTuples, type WorkerCommand } from "../../src/shared/router-worker.js";
import type { NodeTarget } from "../../src/shared/router-node.js";
import { FakeLinearProvider, MemoryVault, currentFixture, masterKey, SIGNING_SECRET } from "../helpers.js";
import { buildNodeAcceptedEvent, buildNodeSucceededEvent } from "./router-node-fixtures.js";

export const audit = { operatorRef: "test-operator", source: "conversation", requestRef: "test-request", authorizationRef: "test-authority" } as const;
export const linearScope = { organizationId: "organization-fixture", linearTeamId: "team-fixture" } as const;
export const nodeRefs = [{ ref: "node-a", version: "v1" }, { ref: "node-b", version: "v1" }] as const;
export const targets: readonly NodeTarget[] = ["review", "default"].map(name => ({
  endpointRef: { ref: `worker-${name}`, version: "v1" },
  identity: { nodeId: "physical-node", workerId: `actual-${name}`, executorKind: "codex", runtimeProfile: { ref: "runtime", version: "v1" } },
  sessionBindingExecutorId: `executor-${name}`,
}));
/**
 * Node routing configuration for the harness: claude maps to the review Worker and
 * codex to the default Worker. Fixed executor labels select through those mappings;
 * ordinary business labels do not select a Worker.
 */
export const executorWorkers = [{ executorLabel: "claude" as const, workerKey: "review-key" }, { executorLabel: "codex" as const, workerKey: "default-key" }];

async function listen(server: Server): Promise<URL> {
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing test address");
  return new URL(`http://127.0.0.1:${address.port}`);
}
async function close(server: Server): Promise<void> {
  if (!server.listening) return;
  server.closeAllConnections();
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
}
export interface PortReservation {
  /** The reserved ports, in the order they were taken. Each one stays bound here until `release` names it. */
  readonly ports: readonly number[];
  /** Hand the named ports back to the OS -- every port still held, when called with none. */
  release(...ports: readonly number[]): Promise<void>;
}
/**
 * Ports this harness assigns to one composition, held rather than handed out as bare numbers.
 *
 * Every listener stays open until the last port is taken: closing one before asking for the next lets the
 * OS hand the same port out twice, and a Node whose command and event ports collide refuses to start
 * (`eventPort is invalid.`).
 *
 * Each listener stays open until the process under test is about to bind that port. Parallel test files
 * open ephemeral listeners too, so release each port immediately before its Node starts to limit the
 * interval in which another listener could take it.
 */
export async function reservePorts(count: number): Promise<PortReservation> {
  const held = new Map<number, Server>();
  try {
    for (let index = 0; index < count; index += 1) {
      const server = createServer();
      held.set(Number((await listen(server)).port), server);
    }
    if (held.size !== count) throw new Error("the OS handed out a duplicate port");
  } catch (error) {
    await Promise.all([...held.values()].map(close));
    throw error;
  }
  const ports: readonly number[] = Object.freeze([...held.keys()]);
  return {
    ports,
    async release(...requested) {
      const releasing = (requested.length === 0 ? ports : requested).map(port => {
        const server = held.get(port);
        held.delete(port);
        return server;
      });
      await Promise.all(releasing.filter((server): server is Server => server !== undefined).map(close));
    },
  };
}
export async function eventually(predicate: () => boolean | Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 8_000;
  while (Date.now() < deadline) { if (await predicate()) return; await new Promise(resolve => setTimeout(resolve, 10)); }
  assert.fail("unified route did not settle");
}
export async function post(url: URL, token: string, body: unknown) {
  const response = await fetch(url, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify(body) });
  return { status: response.status, body: await response.json() as Record<string, unknown> };
}

export interface UnifiedHarnessOptions {
  /**
   * Node indices that start with an empty Worker inventory. Omit
   * defaultWorkerKey because there is no Worker key to name; readiness and
   * dispatch remain reachable.
   */
  readonly emptyWorkerInventoryNodes?: readonly number[];
  /**
   * Team rules for every non-empty Node in the composition. Left
   * out, a Node carries none and an unlabelled source reaches its default
   * Worker, which is what most of these tests want.
   */
  readonly workerScopeRules?: readonly { readonly scope: { readonly system: "linear"; readonly tenantId: string; readonly scopeId: string }; readonly workerKey: string }[];
  /**
   * Replace a Worker's controlled double with a caller-built listener, such as a real
   * WorkerCommandService or proxy. It receives the inventory entry and Node 0 event
   * URL. Its commands are not recorded in commands.
   */
  readonly workerListeners?: Readonly<Record<number, (input: UnifiedWorkerListenerInput) => Promise<URL>>>;
  /** Worker inventory and Router's frozen target identity, defaulting to targets. */
  readonly targets?: readonly NodeTarget[];
  /**
   * Explicit Router runtime overrides.
   */
  readonly relayConfig?: Partial<RelayRuntimeConfig>;
  /** The Linear stores' clock (e.g. to step past a redispatch's spacing). Router keeps the wall clock. */
  readonly channelNow?: () => Date;
  /**
   * Linear unconfirmedReservationMs used by the observe sweep for stranded
   * reservations.
   */
  readonly linearUnconfirmedReservationMs?: number;
}

export interface UnifiedWorkerListenerInput {
  readonly target: NodeTarget;
  readonly commandToken: string;
  readonly eventToken: string;
  readonly nodeEventUrl: URL;
}

/** Real production compositions/listeners and durable stores; only provider APIs and Workers are controlled. */
export async function unifiedHarness(context: { after(fn: () => unknown): void }, options: UnifiedHarnessOptions = {}) {
  const directory = await mkdtemp(join(tmpdir(), "axis-unified-"));
  const cleanup: (() => unknown | Promise<unknown>)[] = [() => rm(directory, { recursive: true, force: true })];
  const dispose = async () => {
    const failures: unknown[] = [];
    while (cleanup.length > 0) {
      try { await cleanup.pop()!(); } catch (error) { failures.push(error); }
    }
    if (failures.length > 0) throw new AggregateError(failures, "Harness cleanup failed");
  };
  context.after(dispose);
  try {
  const commands: { worker: number; command: WorkerCommand }[] = [];
  let rejectContinuation: string | null = null;
  let beforeReply: ((command: WorkerCommand) => Promise<void>) | undefined;
  let steerAnswer: "steered" | "interrupt_requested" = "steered";
  const inventory = options.targets ?? targets;
  const reserved = await reservePorts(5);
  cleanup.push(() => reserved.release());
  const [nodeEventPort0, nodeEventPort1, nodePort0, nodePort1, routerPort] = reserved.ports;
  const nodeEventPorts = [nodeEventPort0!, nodeEventPort1!];
  const workers: URL[] = [];
  for (const [index, target] of inventory.entries()) {
    const listener = options.workerListeners?.[index];
    if (listener !== undefined) {
      workers.push(await listener({ target, commandToken: `command-${index}`, eventToken: `worker-event-${index}`, nodeEventUrl: new URL(`http://127.0.0.1:${nodeEventPorts[0]}`) }));
      continue;
    }
    const server = createServer((request, response) => { void (async () => {
      if (request.headers.authorization !== `Bearer command-${index}`) { response.writeHead(401); response.end(); return; }
      const buffers: Buffer[] = []; for await (const chunk of request) buffers.push(Buffer.from(chunk));
      const command = JSON.parse(Buffer.concat(buffers).toString()) as WorkerCommand;
      commands.push({ worker: index, command });
      if (rejectContinuation !== null && command.command === "continue") {
        const tuple = workerFailureTuples[rejectContinuation]!;
        response.writeHead(tuple.statuses[0]!, { "content-type": "application/json" });
        response.end(JSON.stringify({ error: { code: rejectContinuation, message: tuple.message } })); return;
      }
      await beforeReply?.(command);
      const result = command.command === "cancel" || command.command === "steer" ? { contractVersion: command.contractVersion, commandId: command.commandId, status: command.command === "steer" ? steerAnswer : "interrupt_requested", runId: command.target.runId }
        : { contractVersion: command.contractVersion, commandId: command.commandId, status: "accepted", identity: target.identity, runAccepted: { ...buildNodeAcceptedEvent(command.start), messageId: `accepted-${command.commandId}` } };
      response.writeHead(202, { "content-type": "application/json" }); response.end(JSON.stringify(result));
    })().catch(() => response.destroy()); });
    cleanup.push(() => close(server));
    workers.push(await listen(server));
  }
  const ports = [nodePort0!, nodePort1!];
  const runtimeConfig: RelayRuntimeConfig = {
    routerDatabase: join(directory, "router.sqlite"), routerHost: "127.0.0.1", routerPort: routerPort!,
    nodeEndpoints: nodeRefs.map((nodeRef, index) => ({ nodeRef, nodeId: "physical-node", endpoint: `http://127.0.0.1:${ports[index]}`, bearerToken: `node-command-${index}` })),
    nodeTargets: nodeRefs.map((nodeRef, index) => ({ nodeRef, endpointRef: { ref: `scope-${index}`, version: "v1" }, identity: inventory[0]!.identity, sessionBindingExecutorId: "binding-only" })),
    defaultEndpointRef: { ref: "scope-0", version: "v1" }, defaultExcludedTeamIds: ["excluded-team"], ...options.relayConfig,
  };
  const routingConfig = { eventBindings: nodeRefs.map((nodeRef, index) => ({ bearerToken: `node-event-${index}`, authenticatedNode: { nodeRef, nodeId: "physical-node" }, allowedOrigins: inventory.map(target => target.identity) })) };
  const teamBindings = new SqliteTeamBindingStore({ filename: runtimeConfig.routerDatabase }); cleanup.push(() => teamBindings.close());
  const profile = (index: number) => ({ ...inventory[0]!.identity, endpointRef: { ref: `scope-${index}`, version: "v1" } });
  teamBindings.configure({ scope: linearScope, expectedRevision: 0, profile: profile(0), audit });
  const routingAudits: unknown[] = [];
  const workerRouting = { audit: { record(entry: unknown) { routingAudits.push(entry); } } };
  const router = composeNodeRouter({ runtime: runtimeConfig }, { teamBindings }); cleanup.push(() => router.close());
  const routerHttp = await startRouterHttpServiceWithNodeEvents({ router, nodeEventBindings: routingConfig.eventBindings }); cleanup.push(() => routerHttp.close());
  const empty = new Set(options.emptyWorkerInventoryNodes ?? []);
  const workerEntries = inventory.map((target, worker) => ({ workerKey: worker === 0 ? "review-key" : "default-key", target, workerEndpoint: workers[worker]!.origin, commandToken: `command-${worker}`, eventToken: `worker-event-${worker}`, readinessMode: target.identity.executorKind === "codex" ? "codex" : "unsupported" }));
  const populatedWorkers = workerEntries;
  const executorBindings = executorWorkers;
  const scopeRules = options.workerScopeRules ?? [];
  const nodeConfigs = nodeRefs.map((nodeRef, index) => ({
    database: join(directory, `node-${index}.sqlite`), nodeId: "physical-node", nodeRef: nodeRef.ref, nodeVersion: nodeRef.version,
    token: `node-command-${index}`, host: "127.0.0.1", port: ports[index]!, eventHost: "127.0.0.1", eventPort: nodeEventPorts[index]!,
    routerEndpoint: routerHttp.url.origin, routerEventToken: `node-event-${index}`,
    ...(empty.has(index) ? {} : { defaultWorkerKey: "default-key" }),
    eventRetryInitialDelayMs: 100, eventRetryMaxDelayMs: 1000,
    workers: empty.has(index) ? [] : populatedWorkers,
    ...(empty.has(index) ? {} : { workerExecutorBindings: executorBindings, workerScopeRules: scopeRules }),
  }));
  const nodes: NodeRuntime[] = [];
  // Release a Node's two reserved ports immediately before starting that Node. Keep
  // their concrete values in nodeConfigs so restarts remain reachable through Router's
  // frozen endpoint map.
  for (const [index, env] of nodeConfigs.entries()) {
    await reserved.release(ports[index]!, nodeEventPorts[index]!);
    nodes.push(await startNode(nodeConfigurationFixture(env)));
    cleanup.push(() => nodes[index]!.close());
  }
  // Nothing binds `routerPort`: the Router listener above takes an OS-assigned port of its own, and the
  // number remains reserved until the Node listeners have started.
  await reserved.release(routerPort!);
  const provider = new FakeLinearProvider(), vault = new MemoryVault();
  await vault.put("linear:organization-fixture:app-user-fixture", JSON.stringify({ version: 1, accessToken: "fixture-token", refreshToken: null, expiresAt: null, scope: "read write app:assignable app:mentionable" }));
  const linear = composeLinearChannel({
      databases: { channel: join(directory, "linear-channel.sqlite"), conversations: join(directory, "linear-conversations.sqlite"), runChain: join(directory, "linear-chain.sqlite"), delivery: join(directory, "linear-outbox.sqlite") },
      webhookSecret: SIGNING_SECRET,
      oauth: { clientId: "fixture-client", clientSecret: "fixture-secret", redirectUri: "https://relay.invalid/oauth/linear/callback" },
      defaultTeamIds: [linearScope.linearTeamId],
      allowAllPublicTeams: false,
      integrityCheckIntervalMs: 60_000,
      tickIntervals: { ingress: 1_000, observe: 1_000, outbox: 1_000 },
    }, {
      cipher: new EnvelopeCipher(masterKey()),
      vault,
      provider,
      router,
      workerRouting,
      sleep: async () => {},
      ...(options.channelNow === undefined ? {} : { now: options.channelNow }),
      ...(options.linearUnconfirmedReservationMs === undefined ? {} : { unconfirmedReservationMs: options.linearUnconfirmedReservationMs }),
    }); cleanup.push(() => linear.close());
  linear.installations.install({ organizationId: linearScope.organizationId, appUserId: "app-user-fixture", credentialRef: "linear:organization-fixture:app-user-fixture", teamIds: [linearScope.linearTeamId], canAccessAllPublicTeams: false });
  const relayHttp = await startRelayHttpService({ linear: linear.ingress, ready: () => linear.ready, diagnostics: () => linear.diagnose(), oauth: linear.oauth, operatorToken: "operator-token" }); cleanup.push(() => relayHttp.close());
  return {
    directory, commands, nodes, nodeConfigs, router, routerHttp, runtimeConfig, routingConfig, teamBindings, profile, linear, provider, routingAudits,
    /** Inventory an empty-inventory Node adopts when its first Worker is deployed. */
    populatedWorkers,
    /** Answers every continue with this Worker failure (default: the ambiguous `recovery_required`); `null` stops. */
    rejectContinuation(code: string | null = "axis.worker.recovery_required") { rejectContinuation = code; },
    beforeReply(callback: typeof beforeReply) { beforeReply = callback; },
    /**
     * Worker double's steer response: steered by default, or the interrupt_requested
     * fallback.
     */
    steerAnswer(answer: typeof steerAnswer) { steerAnswer = answer; },
    async linearPost(payload: unknown = currentFixture("agent-session-created.json"), deliveryId = randomUUID()) {
      const body = JSON.stringify(payload);
      const response = await fetch(new URL("/webhooks/linear", relayHttp.url), { method: "POST", headers: { "content-type": "application/json", "linear-delivery": deliveryId, "linear-event": "AgentSessionEvent", "linear-signature": createHmac("sha256", SIGNING_SECRET).update(body).digest("hex"), "linear-timestamp": String(Date.now()) }, body });
      assert.equal(response.status, 200); return linear.ticks.ingress();
    },
    async event(index: number, phase: "accepted" | "succeeded", node = 0) {
      const entry = commands[index]!; if (entry.command.command === "cancel" || entry.command.command === "steer") throw new Error("start required");
      const envelope = { ...(phase === "accepted" ? buildNodeAcceptedEvent(entry.command.start) : buildNodeSucceededEvent(entry.command.start)), messageId: `${phase}-${entry.command.commandId}` };
      const result = await post(new URL("/v1/router/run-events", nodes[node]!.service.eventUrl), `worker-event-${entry.worker}`, { envelope, recordedAt: new Date().toISOString() });
      assert.ok(result.status === 200 || result.status === 202, JSON.stringify(result)); return envelope;
    },
  };
  } catch (error) {
    try { await dispose(); } catch (cleanupError) { throw new AggregateError([error, cleanupError], "Harness setup failed"); }
    throw error;
  }
}
