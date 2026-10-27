import assert from "node:assert/strict";
import { createHmac, randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  RouterHttpEventSink,
  WorkerCommandService,
  deterministicElicitationOtherOption,
  deterministicElicitationReplyValue,
  startWorkerHttpService,
  type StructuredElicitation,
  type WorkerHttpService,
} from "../../../src/index.js";
import { LINEAR_INBOX_LEASE_OWNER, composeLinearChannel, startLinearChannelProcess, type LinearChannel, type LinearChannelProcess } from "../../../src/relay/linear/composition.js";
import { linearIssueContextStep, LINEAR_CHANNEL_COPY } from "../../../src/relay/linear/channel.js";
import { LinearGraphQlProvider } from "../../../src/relay/linear/provider.js";
import { RelayError, classifyStartupFailure, renderStartupFailure } from "../../../src/relay/errors.js";
import { linearActivityContent } from "../../../src/relay/linear/delivery.js";
import { channelInstructionDigest, type ChannelMessageContent } from "../../../src/router/channel-conversation-types.js";
import { sha256 } from "../../../src/relay/security.js";
import { startRelayHttpService, type RelayHttpService } from "../../../src/relay/http.js";
import type { LinearLoopFailure } from "../../../src/relay/linear/composition.js";
import { EnvelopeCipher } from "../../../src/relay/security.js";
import { ROUTER_NODE_EVENT_HTTP_PATH, startRouterHttpServiceWithNodeEvents } from "../../../src/router/http.js";
import type { RouterRunLifecycleEnvelope, WorkerExecutionIdentity } from "../../../src/shared/router-worker.js";
import { NODE_COMMAND_CONTRACT_VERSION } from "../../../src/shared/router-node-commands.js";
import { channelBindingResolver } from "../../../src/router/binding-resolver.js";
import { Router } from "../../../src/router/router.js";
import type { RouterCancelInput, RouterCommandResult, RouterContinueInput, RouterNodeStartInput } from "../../../src/router/router.js";
import { NodeDispatchDouble, defaultStartWorkerRouting, httpNodeWorkerPort, nodeReservationFromEndpoint, type WorkerCommandDispatcher } from "../../support/node-dispatch-double.js";
import { SqliteTeamBindingStore } from "../../../src/router/team-bindings.js";
import type { WorkerCommand, WorkerCommandResponse } from "../../../src/shared/router-worker.js";
import { currentFixture, FakeLinearProvider, masterKey, MemoryVault, promptActivity, SIGNING_SECRET } from "../../helpers.js";
import { LINEAR_ACTIVITY_COPY } from "../../../src/relay/linear/activity.js";
import { renderLinearPromptRejection } from "../../../src/relay/linear/instruction.js";
/**
 * Assert that success reaches Linear as the agent's response body and excludes the
 * exported no-result summary. Refer to the summary symbol so the check continues to
 * distinguish the projection paths when copy changes.
 */
import { ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY } from "../../../src/shared/router-relay-event.js";
import { workerSelectionFromLabels } from "../../../src/shared/worker-selection.js";

/**
 * Summary text in its JSON-string representation. The guards search
 * JSON.stringify(activities()), so the expected text must use the same escaping to
 * detect multiline or quoted summaries.
 */
const SUMMARY_IN_JSON = JSON.stringify(ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY).slice(1, -1);
import { ScriptedExecutor, scriptedExecutorRegistration, type ScriptedLiveBinding } from "../../support/scripted-executor.js";

/**
 * Exercise signed HTTP ingress, the real scheduled ingress/observe/outbox loops,
 * production channel composition and six SQLite stores, with a real Router and Worker
 * service over HTTP. Router events pass its real Node event ingress and authority
 * checks.
 *
 * Linear's GraphQL API, agent model and credential vault are doubles. The
 * Worker-to-Node forwarder exercises Router admission but does not model a Node's
 * durable event pump; its own comment describes that boundary. The vault reports ready,
 * while OAuth token parsing remains real.
 *
 * Tests push lifecycle events explicitly and poll observed state. Negative checks allow
 * scheduled loops to run and pair with positive evidence so inactivity cannot
 * masquerade as a refusal. Waiting decisions and stop paths use Router-minted Tasks and
 * real Worker events.
 */

const ORGANIZATION_ID = "organization-fixture";
const TEAM_ID = "team-fixture";
const AGENT_SESSION_ID = "agent-session-fixture";
/**
 * Second AgentSession on the same Relay, allowing one test to exercise multiple sweep
 * outcomes.
 */
const SECOND_AGENT_SESSION_ID = "agent-session-fixture-2";
const APP_USER_ID = "app-user-fixture";
const CREDENTIAL_REF = `linear:${ORGANIZATION_ID}:${APP_USER_ID}`;
const OPERATOR_TOKEN = "operator-fixture-token";
const ROUTER_WORKER_TOKEN = "e2e-router-worker-token";
/**
 * Router Node event binding token, configured through
 * AXIS_ROUTER_NODE_EVENT_BINDINGS_JSON.
 */
const NODE_EVENT_TOKEN = "e2e-node-event-token";
const NODE_REF = { ref: "e2e-node-route-1", version: "v1" } as const;
const EXECUTOR_ID = "e2e-scripted-executor-1";
const WORKER_IDENTITY = { nodeId: "e2e-node-1", workerId: "e2e-worker-1", executorKind: "scripted", runtimeProfile: { ref: EXECUTOR_ID, version: "v1" } } as const;
const ENDPOINT_REF = { ref: "e2e-endpoint-1", version: "v1" } as const;
const TICK_INTERVALS = { ingress: 5, observe: 5, outbox: 5 } as const;

/** The stored OAuth credential the real `LinearOAuthManager#accessToken` parses out of the vault. */
const STORED_CREDENTIAL = JSON.stringify({ version: 1, accessToken: "e2e-linear-access-token", refreshToken: null, expiresAt: null, scope: "read write app:assignable app:mentionable" });

type TickFailure = LinearLoopFailure;

interface Harness {
  readonly channel: LinearChannel;
  readonly provider: FakeLinearProvider;
  readonly executor: ScriptedExecutor;
  readonly relayHttp: RelayHttpService;
  readonly tickFailures: readonly TickFailure[];
  /** Errors the Node event forwarder hit relaying an event; named in `waitFor`'s failure message. */
  readonly forwarderFailures: readonly unknown[];
  /** The Activity contents this chain actually posted to Linear, in order. */
  activities(): readonly { readonly type: string; readonly body?: string; readonly action?: string }[];
  /** Node-to-Router hop with a response-loss seam after Worker execution. */
  readonly nodePort: NodeDispatchDouble;
  /** Holds every Worker event on the Node hop; see `startNodeEventForwarder`'s gate. */
  holdNodeEvents(): void;
  releaseNodeEvents(): void;
  /** Envelope or delivery types of Worker events currently parked by holdNodeEvents. */
  parkedNodeEvents(): readonly string[];
  /** Router database file for read-only assertions about retained state. */
  readonly routerDatabase: string;
  /**
   * Closes the Team-binding store `channelBindingResolver` reads, so the NEXT
   * start cannot resolve a route at all.
   *
   * Router contains that as a DECIDED rejection rather than letting it escape:
   * `selectionError` (`src/router/router.ts`) lists the binding codes, and
   * `startNode` returns `{ status: "rejected", safeCode }` for them instead of
   * throwing. So this reaches the channel as a returned rejection, not as a
   * post-submit throw -- which is exactly why it is useful here: it is the one
   * genuinely-did-not-start start this file can drive end to end.
   *
   * ⚠️ It is NOT a way to drive `converged === "absent"`, and an earlier
   * revision of this helper claimed it was. `absent` needs Router to hold no
   * command row for the reservation, and nothing in this harness can produce
   * that: every `NodeDispatchDouble` fault lands after `#claimNodeCommand` has
   * committed the row (Router then answers `unknown`), and the only
   * pre-write escape hatches are a corrupt task-identity row or a failed
   * SQLite write, neither of which is injectable from here. Production
   * reaches it by dying between the channel's `reserve` and Router's write.
   */
  breakRouteSelection(): void;
  /**
   * Crash the next dispatch before Router records a command. Close the channel stores
   * under the issuing tick so its in-place convergence cannot run. The real reservation
   * remains reserved while Router's real reconciliation answers absent.
   *
   * A returned route rejection, Node transport fault or ordinary post-submit throw
   * cannot create this state: those paths retain Router evidence or settle in the same
   * tick. This seam models Relay process death between reserve and Router admission. No
   * Activity survives the crash, making a later terminal Activity attributable to the
   * real sweep.
   */
  crashNextDispatchBeforeRouterSeesIt(): void;
  /** How many dispatches `crashNextDispatchBeforeRouterSeesIt` has actually taken. */
  crashedDispatches(): number;
  /**
   * The stale window the NEXT composition hands `reconcileStale`. Applies from
   * the next `restart()`, never to the composition already running, so a test
   * can shorten it for the sweep it means to observe without also inviting the
   * sweeper to race a live dispatch on the way there.
   */
  sweepStrandedReservationsAfter(milliseconds: number): void;
  /**
   * Unconfirmed window passed to reconcileStale by the next restart. Set it with the
   * stale window because a reservation must cross the stale threshold before reaching
   * the unconfirmed branch.
   */
  unconfirmedReservationsAfter(milliseconds: number): void;
  /**
   * Inbox lease used by claim, applied on the next restart. Shortening it allows
   * crashed deliveries and same-session ordering waits to be reclaimed within the
   * test's runtime.
   */
  holdInboxLeaseFor(milliseconds: number): void;
  post(payload: Record<string, unknown>, options?: { readonly deliveryId?: string; readonly tamperSignature?: boolean }): Promise<{ readonly status: number; readonly body: unknown }>;
  /** Tears down the loops, the listeners and the stores, leaving the database FILES intact for a restart. */
  stop(): Promise<void>;
  /** Recomposes the channel over the same database files -- same paths, same master key -- and restarts the loops, as a process restart would. */
  restart(): Promise<void>;
}

/**
 * A Worker port that forwards to the real HTTP transport once it exists.
 *
 * Router must be constructed before the Worker listener (the Worker's event
 * sink needs Router's URL) and the Worker port needs the Worker's URL, so one
 * of the two has to be late-bound. Throwing rather than silently dropping a
 * dispatch is what keeps a wiring mistake from looking like a Worker that
 * never answered.
 */
class ForwardWorkerPort implements WorkerCommandDispatcher {
  target: WorkerCommandDispatcher | undefined;
  async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> {
    if (this.target === undefined) throw new Error("The Worker HTTP transport is not wired yet.");
    return this.target.dispatch(command);
  }
}

/**
 * Forward the Worker's authenticated { envelope, recordedAt } request to Router's Node
 * event HTTP ingress with the configured origin. Relay the real status, content type
 * and body; record non-200/202 responses as failures so refusals cannot appear only as
 * timeouts.
 *
 * This establishes that Worker events pass Router's bearer, sameNode, allowedOrigins
 * and request-shape checks on the accepting path. Origin is fixed for this
 * single-Worker topology rather than derived per credential.
 *
 * The forwarder has no durable Node ingestion, buffering, ordering or retry pump. It
 * returns Router refusals directly to the Worker sink. A production Node instead
 * acknowledges Worker ingestion independently and classifies and quarantines Router
 * conflicts in its own pump.
 */
async function startNodeEventForwarder(routerUrl: URL, origin: WorkerExecutionIdentity): Promise<{
  readonly url: URL;
  readonly failures: readonly unknown[];
  readonly parked: readonly string[];
  hold(): void;
  release(): void;
  close(): Promise<void>;
}> {
  const failures: unknown[] = [];
  /**
   * Hold Worker events so a lost dispatch response leaves Router without evidence of
   * execution. Without the gate, a concurrent run.accepted could converge the command
   * before the failure is observed. Release forwards the real delayed events; retries
   * remain deduplicated by messageId.
   */
  let gate: Promise<void> | null = null;
  let open: (() => void) | null = null;
  // Track event types at the gate so tests can observe a terminal already journaled by
  // the Worker but not yet delivered to Router.
  const parked: string[] = [];
  const server = createServer((request, response) => { void (async () => {
    if (request.headers.authorization !== `Bearer ${ROUTER_WORKER_TOKEN}`) { response.writeHead(401); response.end(); return; }
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    const input = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { readonly envelope: RouterRunLifecycleEnvelope; readonly recordedAt: string };
    if (gate !== null) {
      const kind = "deliveryType" in input.envelope ? String(input.envelope.deliveryType) : String(input.envelope.envelopeType);
      parked.push(kind);
      try { await gate; } finally { parked.splice(parked.indexOf(kind), 1); }
    }
    const forwarded = await fetch(new URL(ROUTER_NODE_EVENT_HTTP_PATH, routerUrl), {
      method: "POST",
      headers: { authorization: `Bearer ${NODE_EVENT_TOKEN}`, "content-type": "application/json" },
      body: JSON.stringify({ contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: NODE_REF, origin, envelope: input.envelope, recordedAt: input.recordedAt }),
    });
    const text = await forwarded.text();
    if (forwarded.status !== 200 && forwarded.status !== 202) failures.push(new Error(`the node event door answered ${forwarded.status}: ${text}`));
    response.writeHead(forwarded.status, { "content-type": forwarded.headers.get("content-type") ?? "application/json", "content-length": String(Buffer.byteLength(text)) });
    response.end(text);
  })().catch((error: unknown) => {
    // Recorded for `waitFor`'s failure message, and logged. Destroying the
    // socket makes the Worker's sink raise a transient transport error and
    // retry until some `waitFor` deadline fires, so without this the failure
    // message would name the observed state and never the cause.
    failures.push(error);
    console.error("node event forwarder failed", error);
    response.destroy();
  }); });
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("the Node event forwarder did not bind");
  return {
    url: new URL(`http://127.0.0.1:${address.port}`),
    failures,
    parked,
    hold(): void { if (gate === null) gate = new Promise<void>((resolve) => { open = resolve; }); },
    release(): void { open?.(); gate = null; open = null; },
    async close(): Promise<void> {
      // Released first: a held request is parked inside the handler, and
      // `close` waits for in-flight ones. Teardown would otherwise hang on the
      // gate rather than on anything the test meant to observe.
      open?.();
      gate = null;
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
    },
  };
}

/**
 * nativeSteer configures Router's AXIS_ROUTER_NATIVE_STEER, disabled unless selected by
 * the test. steer controls whether the scripted executor declares native steer support.
 */
interface HarnessOptions { readonly nativeSteer?: boolean; readonly steer?: boolean }

async function harness(context: { after(fn: () => unknown): void }, harnessOptions: HarnessOptions = {}): Promise<Harness> {
  const directory = await mkdtemp(join(tmpdir(), "axis-channel-composition-e2e-"));

  const databases = {
    channel: join(directory, "linear-channel.sqlite"),
    conversations: join(directory, "linear-conversations.sqlite"),
    runChain: join(directory, "linear-run-chain.sqlite"),
    delivery: join(directory, "linear-delivery.sqlite"),
  } as const;

  const teamBindings = new SqliteTeamBindingStore({ filename: join(directory, "router.sqlite") });
  teamBindings.configure({
    scope: { organizationId: ORGANIZATION_ID, linearTeamId: TEAM_ID },
    expectedRevision: 0,
    profile: { ...WORKER_IDENTITY, endpointRef: ENDPOINT_REF },
    audit: { operatorRef: "e2e-operator", source: "channel-composition_e2e", requestRef: "e2e-request", authorizationRef: "e2e-authorization" },
  });

  // Set by `breakRouteSelection()`; teardown must not close the store twice.
  let teamBindingsClosed = false;

  // Hoisted above the Router below, which reaches `closeChannel` from inside a
  // dispatch. Declared here rather than beside `relayHttp`/`loops` only for
  // that reason; nothing else uses them before `start()`.
  let channel: LinearChannel | undefined;
  let channelClosed = false;
  let crashNextDispatch = false;
  let crashedDispatches = 0;
  let staleReservationMs: number | undefined;
  let unconfirmedReservationMs: number | undefined;
  let inboxLeaseMs: number | undefined;
  /** Idempotent: a crash closes the stores, and `stop()` must not close them again. */
  const closeChannel = (): void => {
    if (channelClosed) return;
    channelClosed = true;
    channel?.close();
  };

  const forward = new ForwardWorkerPort();
  // Dispatch through a Node reservation to the real Worker service over HTTP.
  const port = new NodeDispatchDouble({
    worker: forward, nodeRef: NODE_REF,
    reservation: nodeReservationFromEndpoint({ endpointRef: ENDPOINT_REF, identity: WORKER_IDENTITY, sessionBindingExecutorId: EXECUTOR_ID }, "worker-key-e2e"),
  });
  /**
   * Close stores before rejecting this pre-admission dispatch so the tick resumes into
   * the simulated dead process. The call chain and SQLite statements are synchronous
   * here, so no statement is running on a handle while it closes.
   */
  const dieInsideDispatch = (kind: string): never => {
    crashNextDispatch = false;
    crashedDispatches += 1;
    closeChannel();
    throw new Error(`the relay process died inside this ${kind} dispatch, before Router recorded the command`);
  };
  // Place the crash seam before all three Router dispatch entry points so the next
  // dispatch of any kind is covered. Unarmed calls use the real superclass methods.
  const router = new (class extends Router {
    override async startNode(input: RouterNodeStartInput): Promise<RouterCommandResult> {
      return crashNextDispatch ? dieInsideDispatch("start") : super.startNode(input);
    }
    override async continueNode(input: RouterContinueInput): Promise<RouterCommandResult> {
      return crashNextDispatch ? dieInsideDispatch("continue") : super.continueNode(input);
    }
    override async cancelNode(input: RouterCancelInput): Promise<RouterCommandResult> {
      return crashNextDispatch ? dieInsideDispatch("cancel") : super.cancelNode(input);
    }
  })({
    filename: join(directory, "router.sqlite"),
    nodeRouting: {
      bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef: ENDPOINT_REF, nodeRef: NODE_REF }] }),
      port,
      ...(harnessOptions.nativeSteer === undefined ? {} : { nativeSteer: harnessOptions.nativeSteer }),
    },
  });

  // The REAL listener, with the node-event opt-in. `nodeEventBindings` is the
  // fixture the door's authority checks run against: this binding's bearer,
  // the logical Node it speaks for, and the Worker identities it may carry
  // events for. What that does and does not prove is in this file's header.
  const routerHttp = await startRouterHttpServiceWithNodeEvents({
    router,
    nodeEventBindings: [{
      bearerToken: NODE_EVENT_TOKEN,
      authenticatedNode: { nodeRef: NODE_REF, nodeId: WORKER_IDENTITY.nodeId },
      allowedOrigins: [WORKER_IDENTITY],
    }],
  });
  const nodeEvents = await startNodeEventForwarder(routerHttp.url, port.eventOrigin.identity);

  const executor = new ScriptedExecutor();
  const worker = new WorkerCommandService({
    configuration: {
      bearerToken: ROUTER_WORKER_TOKEN,
      workerId: WORKER_IDENTITY.workerId,
      runtimeKind: "scripted",
      executorId: EXECUTOR_ID,
      nodeId: WORKER_IDENTITY.nodeId,
      runtimeProfile: { ref: EXECUTOR_ID, version: "v1" },
      methodRef: { version: "v1", method: "scripted.execute" },
      workspaces: [{ organizationId: ORGANIZATION_ID, workspaceRoot: directory }],
    },
    registrations: [scriptedExecutorRegistration({ executorId: EXECUTOR_ID, adapter: executor, ...(harnessOptions.steer === undefined ? {} : { steer: harnessOptions.steer }) })],
    eventSink: new RouterHttpEventSink({ endpoint: nodeEvents.url, bearerToken: ROUTER_WORKER_TOKEN }),
  });
  const workerHttp: WorkerHttpService = await startWorkerHttpService({ commandService: worker });
  forward.target = httpNodeWorkerPort({ workerUrl: workerHttp.url, commandToken: ROUTER_WORKER_TOKEN, nodeRef: NODE_REF, workerKey: "worker-key-e2e", target: { endpointRef: ENDPOINT_REF, identity: WORKER_IDENTITY, sessionBindingExecutorId: EXECUTOR_ID } });

  const provider = new FakeLinearProvider();
  const vault = new MemoryVault();
  await vault.put(CREDENTIAL_REF, STORED_CREDENTIAL);
  // Hoisted OUT of `compose()` on purpose. `masterKey()` is `randomBytes(32)`,
  // so building the cipher inside `compose()` gave every `restart()` a
  // different `AXIS_RELAY_MASTER_KEY` -- the one value a real restart must
  // preserve. Nothing failed, because no inbox row happened to cross a restart
  // still encrypted; but that is precisely the case a restart test most needs
  // to be able to cover, and a rotating key made it structurally impossible.
  const cipher = new EnvelopeCipher(masterKey());
  const tickFailures: TickFailure[] = [];

  let relayHttp: RelayHttpService | undefined;
  let loops: LinearChannelProcess | undefined;

  const compose = (): LinearChannel => composeLinearChannel({
      databases,
      webhookSecret: SIGNING_SECRET,
      oauth: { clientId: "e2e-client", clientSecret: "e2e-client-secret", redirectUri: "https://relay.invalid/oauth/linear/callback" },
      defaultTeamIds: [TEAM_ID],
      allowAllPublicTeams: false,
      integrityCheckIntervalMs: 60_000,
      tickIntervals: { ingress: 1_000, observe: 1_000, outbox: 1_000 },
    }, {
      cipher,
      vault,
      provider,
      router,
      workerRouting: defaultStartWorkerRouting,
      // A bounded instruction re-read must never really wait; the retry itself
    // is still the real one.
    sleep: async () => { /* deterministic */ },
      ...(staleReservationMs === undefined ? {} : { staleReservationMs }),
      ...(unconfirmedReservationMs === undefined ? {} : { unconfirmedReservationMs }),
      ...(inboxLeaseMs === undefined ? {} : { inboxLeaseMs }),
    });

  const start = async (): Promise<void> => {
    channel = compose();
    channelClosed = false;
    // Disarmed on every (re)start. `dieInsideDispatch` clears the flag when it
    // fires, so a leak needs an arm nothing consumed -- and that arm would
    // otherwise survive `restart()` and kill an unrelated dispatch later in the
    // same test, presenting as a test that died inside a dispatch it never
    // armed.
    crashNextDispatch = false;
    relayHttp = await startRelayHttpService({
      linear: channel.ingress,
      ready: () => channel!.ready,
      diagnostics: () => channel!.diagnose(),
      oauth: channel.oauth,
      operatorToken: OPERATOR_TOKEN,
    });
    loops = startLinearChannelProcess({
      ticks: channel.ticks,
      intervals: TICK_INTERVALS,
      onTickError: (failure) => tickFailures.push(failure),
    });
  };

  const stop = async (): Promise<void> => {
    await loops?.close();
    await relayHttp?.close();
    closeChannel();
    loops = undefined;
    relayHttp = undefined;
    channel = undefined;
  };

  // Stop loops and listeners before closing their stores, then close the shared Router
  // and remove files. Keep one teardown hook so registration order cannot remove
  // databases while scheduled work still uses them.
  context.after(async () => {
    await stop();
    await workerHttp.close();
    worker.close();
    executor.closeAll();
    await nodeEvents.close();
    await routerHttp.close();
    router.close();
    if (!teamBindingsClosed) teamBindings.close();
    await rm(directory, { recursive: true, force: true });
  });

  await start();
  // Install authority in the shape produced by /oauth/linear/install without calling
  // the fake provider's OAuth exchange. Subsequent operations read the stored
  // credential through the real LinearOAuthManager.accessToken.
  channel!.installations.install({ organizationId: ORGANIZATION_ID, appUserId: APP_USER_ID, credentialRef: CREDENTIAL_REF, teamIds: [TEAM_ID], canAccessAllPublicTeams: false });

  return {
    get channel() { return channel!; },
    provider,
    executor,
    get relayHttp() { return relayHttp!; },
    tickFailures,
    forwarderFailures: nodeEvents.failures,
    activities() {
      return provider.created.map((record) => record.content as { readonly type: string; readonly body?: string; readonly action?: string });
    },
    nodePort: port,
    holdNodeEvents() { nodeEvents.hold(); },
    releaseNodeEvents() { nodeEvents.release(); },
    parkedNodeEvents() { return [...nodeEvents.parked]; },
    routerDatabase: join(directory, "router.sqlite"),
    breakRouteSelection() { teamBindings.close(); teamBindingsClosed = true; },
    crashNextDispatchBeforeRouterSeesIt() { crashNextDispatch = true; },
    crashedDispatches() { return crashedDispatches; },
    sweepStrandedReservationsAfter(milliseconds) { staleReservationMs = milliseconds; },
    unconfirmedReservationsAfter(milliseconds) { unconfirmedReservationMs = milliseconds; },
    holdInboxLeaseFor(milliseconds) { inboxLeaseMs = milliseconds; },
    async post(payload, options = {}) {
      const body = Buffer.from(JSON.stringify(payload));
      const signature = createHmac("sha256", SIGNING_SECRET).update(body).digest("hex");
      const response = await fetch(new URL("/webhooks/linear", relayHttp!.url), {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "linear-delivery": options.deliveryId ?? randomUUID(),
          "linear-event": "AgentSessionEvent",
          "linear-signature": options.tamperSignature === true ? tamper(signature) : signature,
          "linear-timestamp": String(Date.now()),
        },
        body: new Uint8Array(body),
      });
      return { status: response.status, body: await response.json() };
    },
    stop,
    async restart() { await stop(); await start(); },
  };
}

/** Unconditionally different from `signature` regardless of its trailing character, so tampering is never an accidental no-op. */
function tamper(signature: string): string {
  return `${signature.slice(0, -1)}${signature.at(-1) === "0" ? "1" : "0"}`;
}

/**
 * Polls a predicate against a deadline, driven only by the real scheduler.
 *
 * Nothing here advances the chain: the loops are already running on their own
 * timers, and this only decides when to stop looking. A failure reports what
 * the chain actually produced, so a timeout reads as a diagnosis rather than
 * as "flaky".
 */
async function waitFor(harnessState: Harness, description: string, predicate: () => boolean, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      assert.fail(`${description} did not happen within ${timeoutMs}ms.\nActivities posted: ${JSON.stringify(harnessState.activities())}\nTick failures: ${JSON.stringify(harnessState.tickFailures.map((failure) => ({ loop: failure.loop, subject: failure.subject, error: String(failure.error) })))}\nForwarder failures: ${JSON.stringify(harnessState.forwarderFailures.map(String))}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

/** Gives the running loops several intervals to do something, then asserts they did not. */
async function settle(cycles = 40): Promise<void> {
  for (let cycle = 0; cycle < cycles; cycle += 1) await new Promise((resolve) => setTimeout(resolve, 5));
}

/** The longest fragment of `value` that `escapeMarkdown` passes through unchanged. */
function unescaped(value: string): string {
  return value.split(/[\\`*_[\]{}()#+.!|<>-]/u).reduce((longest, part) => (part.length > longest.length ? part : longest), "");
}

function bodyOf(harnessState: Harness, type: string): string | undefined {
  return harnessState.activities().find((content) => content.type === type)?.body;
}
function countOf(harnessState: Harness, type: string): number {
  return harnessState.activities().filter((content) => content.type === type).length;
}
function bodies(harnessState: Harness, type: string): readonly (string | undefined)[] {
  return harnessState.activities().filter((content) => content.type === type).map((content) => content.body);
}
/**
 * Count Activities by exact body, independently of type. Compare body strings directly
 * so multiline copy is not hidden by JSON escaping in negative assertions.
 */
function bodyCount(harnessState: Harness, body: string): number {
  return harnessState.activities().filter((content) => content.body === body).length;
}

function elicitation(): StructuredElicitation {
  const decisionId = `axis-decision-${"e".repeat(48)}`;
  const fastOptionId = `axis-option-${"f".repeat(48)}`;
  const thoroughOptionId = `axis-option-${"d".repeat(48)}`;
  return {
    contractVersion: "1.0.0-fixed-choice",
    decisionId,
    sourceEventId: `axis-elicitation-${"b".repeat(48)}`,
    recommendedOptionId: fastOptionId,
    header: "Choose an approach",
    question: "Which approach should Axis take?",
    options: [
      { optionId: fastOptionId, replyValue: deterministicElicitationReplyValue(decisionId, fastOptionId), label: "Fast", description: "Use the fast approach." },
      { optionId: thoroughOptionId, replyValue: deterministicElicitationReplyValue(decisionId, thoroughOptionId), label: "Thorough", description: "Use the thorough approach." },
    ],
  };
}

/** Re-ask emitted when the agent cannot resolve a free-text reply. */
function reAskElicitation(): StructuredElicitation {
  const decisionId = `axis-decision-${"c".repeat(48)}`;
  const soonId = `axis-option-${"a".repeat(48)}`;
  const carefulId = `axis-option-${"b".repeat(48)}`;
  return {
    contractVersion: "1.0.0-fixed-choice",
    decisionId,
    sourceEventId: `axis-elicitation-${"c".repeat(48)}`,
    recommendedOptionId: soonId,
    header: "Which did you mean",
    question: "Did you mean the fast approach or the thorough one?",
    options: [
      { optionId: soonId, replyValue: deterministicElicitationReplyValue(decisionId, soonId), label: "Soon", description: "You meant the fast approach." },
      { optionId: carefulId, replyValue: deterministicElicitationReplyValue(decisionId, carefulId), label: "Careful", description: "You meant the thorough approach." },
    ],
  };
}

/** Waiting question that offers a custom answer. */
function elicitationWithOther(): StructuredElicitation {
  const base = elicitation();
  return { ...base, contractVersion: "1.0.0-other", other: deterministicElicitationOtherOption(base.decisionId) };
}

/** The binding of the Nth turn the scripted executor opened, once it exists. */
function binding(harnessState: Harness, index: number): ScriptedLiveBinding {
  const value = harnessState.executor.bindings[index];
  if (value === undefined) throw new Error(`the scripted executor has opened ${harnessState.executor.bindings.length} turns, not ${index + 1}`);
  return value;
}

test("created -> progress -> response: one signed webhook reaches the real Worker through the real scheduler, and both Activities come back", async (context) => {
  const state = await harness(context);

  const response = await state.post(currentFixture("agent-session-created.json"));
  assert.equal(response.status, 200);
  assert.deepEqual(response.body, { accepted: true, duplicate: false });

  // The ingress loop claims the delivery, authorizes it, resolves the
  // instruction against the (faked) Linear API and dispatches through the real
  // Router into the real Worker. Nothing in this test calls a tick.
  await waitFor(state, "the scheduled ingress loop to dispatch a start into the real Worker", () => state.executor.bindings.length === 1);

  // Both acknowledgement and progress use thought Activities. Wait for the progress
  // body itself so an acknowledgement alone cannot satisfy this assertion.
  state.executor.pushProgress(binding(state, 0));
  await waitFor(state, "the observe and outbox loops to project and post the progress thought", () => bodies(state, "thought").includes("Scripted run is progressing."));

  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "e2e-final-1", projectionStatus: "projected", body: "Axis implemented DEMO-9.", truncated: false, truncations: [] });
  await waitFor(state, "the terminal response Activity to reach Linear", () => countOf(state, "response") === 1);
  // Require the agent's answer and exclude the no-result summary through the full
  // webhook, scheduler, Worker and outbox path. Both assertions distinguish actual
  // response delivery from fallback-only success.
  assert.equal(bodyOf(state, "response"), "Axis implemented DEMO-9.");
  assert.equal(JSON.stringify(state.activities()).includes(SUMMARY_IN_JSON), false,
    "a successful Linear run must post the agent's reply, never the no-result summary");

  // Not merely "some Activity arrived": the delivered rows are the ones the
  // outbox recorded as delivered, which is what the feedback-loop gate reads
  // back through `deliveredMessage`.
  for (const record of state.provider.created) {
    assert.equal(state.channel.outbox.outboxState(record.id), "delivered", `outbox row ${record.id} was posted but never recorded delivered`);
  }
  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), [], "no loop may have thrown during a clean run");
  assert.deepEqual(state.forwarderFailures.map(String), [], "no event may have been refused or failed at the node door during a clean run");
  assert.ok(state.executor.instructions[0]?.includes("<linear-agent-guidance>"));
  assert.ok(state.executor.instructions[0]?.includes("Use the captured Team execution profile."));
});

/**
 * A created session with no initial Activity reads its creator before loading the
 * dispatch Issue. Transient creator-user read failures must retry and still reach
 * dispatch.
 */
test("a created start's creator-user read retries a transient provider failure and still dispatches", async (context) => {
  const state = await harness(context);
  state.provider.exactUserFailures = 2;

  const response = await state.post(currentFixture("agent-session-created.json"));
  assert.equal(response.status, 200);
  assert.deepEqual(response.body, { accepted: true, duplicate: false });

  await waitFor(state, "the start to reach the Worker despite two transient creator-user read failures", () => state.executor.bindings.length === 1);

  // Proves the retries actually ran rather than the fixture simply not being
  // configured to fail: both configured failures were consumed, across three
  // calls to `loadExactUser` for the one delivery.
  assert.equal(state.provider.exactUserFailures, 0, "both configured transient failures must have been consumed by retries");
  assert.equal(state.provider.exactUserCalls, 3, "two failed attempts and the one that succeeded, all within readExactLinear's own bounded retry");
  assert.equal(countOf(state, "error"), 0, "a creator-user read that eventually succeeds must never surface as a refusal");
  assert.equal(JSON.stringify(state.activities()).includes("did not return the person who started this session"), false,
    "a transient creator-user read failure that is retried must not become the definite linear_user_unavailable refusal");
});

/**
 * Exhausted retryable creator reads remain subject to the inbox's retry budget. Inspect
 * the inbox row to distinguish this outcome from an immediate provider_unavailable
 * refusal; eventual success alone cannot test that distinction.
 */
test("a created start's creator-user read that keeps failing is left to the inbox's own retry budget, not the definite linear_user_unavailable refusal", async (context) => {
  const state = await harness(context);
  // Comfortably more than readExactLinear's own three attempts, so the read
  // never gets a chance to succeed inside this test.
  state.provider.exactUserFailures = 999;
  const deliveryId = randomUUID();

  const response = await state.post(currentFixture("agent-session-created.json"), { deliveryId });
  assert.equal(response.status, 200);

  await waitFor(state, "the ingress loop to claim and attempt the delivery at least once", () => (state.channel.inbox.snapshot(deliveryId)?.attempt ?? 0) >= 1);
  // Several more scheduled ingress intervals, so a wrongly-immediate refusal
  // (which would already have posted and completed the delivery by now) has
  // every opportunity to show up before the negative assertions below run.
  await settle();

  const snapshot = state.channel.inbox.snapshot(deliveryId);
  assert.equal(snapshot?.state, "processing",
    "an exhausted readExactLinear must rethrow retryably and leave the delivery to the inbox's own retry budget, not resolve it to a terminal state");
  assert.equal(snapshot?.attempt, 1,
    "still on its first attempt -- the default 30s inbox lease has not expired, so no second claim can have happened yet");
  assert.equal(state.executor.bindings.length, 0, "a delivery that never got past the creator-user read must never reach the Worker");
  assert.equal(countOf(state, "error"), 0, "no definite refusal Activity may be posted while the delivery is still inside its retry budget");
  assert.equal(JSON.stringify(state.activities()).includes("did not return the person who started this session"), false,
    "an exhausted-but-still-retryable creator-user read must never produce the definite linear_user_unavailable refusal");
});

/**
 * A delegation can name an artificial AgentSession root Comment while resolving through
 * the direct created path. If its creator cannot be verified, retain and deliver a
 * startup refusal instead of silently failing as missing instruction.
 */
test("a refused delegation start through an artificial session root is answered and retained, not failed silently as a missing instruction", async (context) => {
  const state = await harness(context);
  state.provider.exactComment = {
    id: "comment-root", parentId: null, body: "", bodyData: "", issueId: "issue-fixture", teamId: "team-fixture",
    isArtificialAgentSessionRoot: true, user: null, botAuthored: true, member: null, membershipComplete: false,
  };
  state.provider.loadExactUser = async (_token: string, userId: string) => ({ id: userId, active: true, app: true, url: `https://linear.app/acme/profiles/${userId}` });
  const payload = currentFixture("agent-session-created.json");
  const deliveryId = randomUUID();

  const response = await state.post({ ...payload, agentSession: { ...(payload.agentSession as object), commentId: "comment-root" } }, { deliveryId });
  assert.equal(response.status, 200);

  await waitFor(state, "the refused delegation start to be answered", () => countOf(state, "error") === 1);
  assert.equal(state.channel.inbox.snapshot(deliveryId)?.state, "completed", "a retained startup rejection is a durable answer, not a failed delivery");
  assert.equal(JSON.stringify(state.activities()).includes("could not verify the person who started this session"), true,
    "the user is told the real reason, the owner_unverified sentence");
  assert.equal(JSON.stringify(state.activities()).includes("did not include an executable instruction"), false,
    "a delegation is never reported as a missing instruction");
  assert.equal(state.provider.exactCommentCalls, 1, "the artificial root was exact-read before taking the direct path");
  assert.equal(state.executor.bindings.length, 0, "an unverified start never reaches the Worker");
});

/**
 * A delegation's initial prompt Activity can be creator-authored under an artificial
 * root Comment. The direct created path verifies that Activity as the start
 * instruction.
 */
test("a delegation start whose initial prompt Activity hangs from the artificial session root reaches the Worker", async (context) => {
  const state = await harness(context);
  state.provider.exactComment = {
    id: "comment-root", parentId: null, body: "", bodyData: "", issueId: "issue-fixture", teamId: "team-fixture",
    isArtificialAgentSessionRoot: true, user: null, botAuthored: true, member: null, membershipComplete: false,
  };
  state.provider.activities.push({ ...promptActivity("activity-delegation", "Work on DEMO-9"), sourceCommentId: "comment-root" });
  const payload = currentFixture("agent-session-created.json");
  const deliveryId = randomUUID();

  const response = await state.post({
    ...payload,
    agentSession: { ...(payload.agentSession as object), commentId: "comment-root" },
    agentActivity: { id: "activity-delegation", user: { id: "human-user-fixture" }, sourceCommentId: "comment-root", body: "Work on DEMO-9", createdAt: "2026-08-20T16:00:00.000Z" },
  }, { deliveryId });
  assert.equal(response.status, 200);

  await waitFor(state, "the delegation start to reach the Worker", () => state.executor.bindings.length === 1);
  assert.equal(countOf(state, "error"), 0, "a delegation's own initial prompt is not refused");
  assert.equal(state.provider.exactCommentCalls, 1, "the artificial root was exact-read before taking the direct path");
  assert.equal(state.provider.exactActivityCalls, 1, "the initial prompt Activity was exact-read");
});

/**
 * A direct prompt Activity whose source Comment is anything but the proven artificial root is still
 * refused, and the start is answered with that reason rather than the generic Validation copy.
 */
test("a delegation start whose initial prompt Activity hangs from another Comment is refused as an Activity mismatch", async (context) => {
  const state = await harness(context);
  state.provider.exactComment = {
    id: "comment-root", parentId: null, body: "", bodyData: "", issueId: "issue-fixture", teamId: "team-fixture",
    isArtificialAgentSessionRoot: true, user: null, botAuthored: true, member: null, membershipComplete: false,
  };
  state.provider.activities.push({ ...promptActivity("activity-delegation", "Work on DEMO-9"), sourceCommentId: "comment-other" });
  const payload = currentFixture("agent-session-created.json");
  const deliveryId = randomUUID();

  const response = await state.post({
    ...payload,
    agentSession: { ...(payload.agentSession as object), commentId: "comment-root" },
    agentActivity: { id: "activity-delegation", user: { id: "human-user-fixture" }, body: "Work on DEMO-9", createdAt: "2026-08-20T16:00:00.000Z" },
  }, { deliveryId });
  assert.equal(response.status, 200);

  await waitFor(state, "the refused delegation start to be answered", () => countOf(state, "error") === 1);
  assert.equal(JSON.stringify(state.activities()).includes("is not the one this session's start named"), true);
  assert.equal(JSON.stringify(state.activities()).includes("could not complete startup validation safely"), false);
  assert.equal(state.executor.bindings.length, 0);
});

test("a prompted webhook continues the SAME conversation on the real Worker and its response comes back", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "e2e-final-1", projectionStatus: "projected", body: "First turn done.", truncated: false, truncations: [] });
  await waitFor(state, "the first response Activity", () => countOf(state, "response") === 1);

  // Make the prompt Activity re-readable through the provider. The resolver verifies
  // that exact read rather than trusting the webhook body.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Please continue with the tests."));
  const prompted = await state.post(currentFixture("agent-session-prompted.json"));
  assert.equal(prompted.status, 200);

  await waitFor(state, "the scheduled ingress loop to dispatch a continue", () => state.executor.bindings.length === 2);
  // A continuation is a new turn on the SAME native thread -- the property the
  // scripted binding preserves from the real Codex adapter.
  assert.equal(binding(state, 1).threadId, binding(state, 0).threadId, "a continue must reuse the conversation's native thread, not open a new one");
  assert.notEqual(binding(state, 1).turnId, binding(state, 0).turnId);

  state.executor.pushSucceeded(binding(state, 1), { sourceEventId: "e2e-final-2", projectionStatus: "projected", body: "Continued and finished.", truncated: false, truncations: [] });
  await waitFor(state, "the second response Activity", () => countOf(state, "response") === 2);
  // Distinct response bodies establish that the prompt continues the same conversation
  // and produces a second answer in order.
  assert.deepEqual(
    state.activities().filter((content) => content.type === "response").map((content) => content.body),
    ["First turn done.", "Continued and finished."],
  );
  assert.equal(JSON.stringify(state.activities()).includes(SUMMARY_IN_JSON), false,
    "each turn's own body must reach Linear, never the no-result summary");
});

test("a stop webhook cancels the live run on the real Worker and Linear receives the cancellation response", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  const stopResponse = await state.post(currentFixture("agent-session-stop.json"));
  assert.equal(stopResponse.status, 200);
  await waitFor(state, "the cancel to reach the real executor", () => state.executor.wasCancelRequested(binding(state, 0)));

  // A real adapter only ACKNOWLEDGES the interrupt; the run actually stopping
  // is a separately observed terminal, which is why this is pushed rather
  // than assumed.
  state.executor.pushCancelled(binding(state, 0));
  await waitFor(state, "the cancellation response Activity", () => countOf(state, "response") === 1);
  assert.equal(bodyOf(state, "response"), "Axis stopped the active execution as requested.");
});

test("a redelivered webhook is absorbed by the inbox and never starts a second run", async (context) => {
  const state = await harness(context);

  const deliveryId = randomUUID();
  const payload = currentFixture("agent-session-created.json");
  assert.deepEqual((await state.post(payload, { deliveryId })).body, { accepted: true, duplicate: false });
  await waitFor(state, "the first start to reach the Worker", () => state.executor.bindings.length === 1);

  // Linear's own retry re-signs with a fresh timestamp; the delivery id and
  // the semantic digest are what make it a replay.
  assert.deepEqual((await state.post(payload, { deliveryId })).body, { accepted: true, duplicate: true });
  await settle();
  assert.equal(state.executor.bindings.length, 1, "a redelivered webhook must not dispatch a second real run");
});

test("a tampered signature is refused at the HTTP entry and never reaches the inbox, the loops, or the Worker", async (context) => {
  const state = await harness(context);

  const response = await state.post(currentFixture("agent-session-created.json"), { tamperSignature: true });
  assert.equal(response.status, 401);
  await settle();
  assert.equal(state.executor.bindings.length, 0);
  assert.deepEqual(state.activities(), []);

  // Positive control: without it, a run in which the loops never started at
  // all would pass every assertion above identically. A correctly signed
  // delivery on the SAME service must still go all the way through.
  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "a correctly signed delivery on the same service to reach the Worker", () => state.executor.bindings.length === 1);
});

test("durable state survives a process restart: the same databases are reopened, a replayed delivery is still absorbed, and the retained conversation is resumed", async (context) => {
  const state = await harness(context);

  const created = currentFixture("agent-session-created.json");
  const createdDeliveryId = randomUUID();
  assert.deepEqual((await state.post(created, { deliveryId: createdDeliveryId })).body, { accepted: true, duplicate: false });
  await waitFor(state, "the start to reach the Worker before the restart", () => state.executor.bindings.length === 1);
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "e2e-final-pre-restart", projectionStatus: "projected", body: "Before the restart.", truncated: false, truncations: [] });
  await waitFor(state, "the pre-restart response Activity", () => countOf(state, "response") === 1);

  const taskIdBefore = state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.taskId;
  assert.ok(taskIdBefore !== undefined, "the run chain must hold the conversation before the restart");

  // Everything the process owns goes away -- loops, listener, every SQLite
  // handle -- and is rebuilt over the same files, exactly as a restart does.
  await state.restart();

  // The reopened run chain is the SAME one, not a fresh database that happens
  // to be empty. Without this, every assertion below would also pass against a
  // brand-new set of files.
  assert.equal(state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.taskId, taskIdBefore);

  // The inbox's replay rule survived: the same delivery, re-signed the way a
  // real Linear retry re-signs it, is absorbed rather than reprocessed.
  assert.deepEqual((await state.post(created, { deliveryId: createdDeliveryId })).body, { accepted: true, duplicate: true });
  await settle();
  assert.equal(state.executor.bindings.length, 1, "a delivery completed before the restart must not be reprocessed after it");

  // And the restarted process's own loops are live: a NEW prompt continues the
  // retained conversation rather than opening a second one.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Please continue with the tests."));
  assert.deepEqual((await state.post(currentFixture("agent-session-prompted.json"))).body, { accepted: true, duplicate: false });
  await waitFor(state, "the restarted ingress loop to dispatch a continue", () => state.executor.bindings.length === 2);
  assert.equal(binding(state, 1).threadId, binding(state, 0).threadId, "the restart must resume the retained conversation, not start a new one");

  state.executor.pushSucceeded(binding(state, 1), { sourceEventId: "e2e-final-post-restart", projectionStatus: "projected", body: "After the restart.", truncated: false, truncations: [] });
  await waitFor(state, "the post-restart response Activity", () => countOf(state, "response") === 2);
  // Distinct bodies across restart establish a second response rather than replay of
  // the first; threadId equality separately proves resumption.
  assert.deepEqual(bodies(state, "response"), ["Before the restart.", "After the restart."]);
  assert.equal(JSON.stringify(state.activities()).includes(SUMMARY_IN_JSON), false,
    "the executor's own response body must reach Linear, never the no-result summary");
});

/**
 * The feedback-loop gate, end to end -- the one that stops Axis from reading
 * its own Agent Activity back as a user instruction.
 *
 * A mutation pass caught this gap: replacing the composition's
 * `feedback: deliverer` with a stub that always answers `false` SURVIVED both
 * suites. "No second run started" is not enough to notice, because a stubbed
 * gate sends the event down the prompt path instead, where it is refused for a
 * different reason and also starts nothing. The distinguishing observable is
 * the durable one: a recognised feedback event is QUARANTINED, while a prompt
 * that could not be verified is FAILED.
 */
test("an Activity Axis itself delivered, arriving back as a prompted webhook, is recognised as its own and quarantined -- never read as a new instruction", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "e2e-final-feedback", projectionStatus: "projected", body: "Axis wrote this.", truncated: false, truncations: [] });
  await waitFor(state, "the response Activity Axis posts to Linear", () => countOf(state, "response") === 1);

  // The id Axis actually delivered under, taken from what the provider was
  // asked to create rather than fabricated -- a made-up id would prove nothing
  // about the lookup.
  const ownActivityId = state.provider.created.find((record) => (record.content as { type?: string }).type === "response")!.id;
  assert.equal(state.channel.outbox.outboxState(ownActivityId), "delivered");

  const echoed = currentFixture("agent-session-prompted.json");
  echoed.agentActivity = { ...(echoed.agentActivity as Record<string, unknown>), id: ownActivityId };
  const deliveryId = randomUUID();
  assert.deepEqual((await state.post(echoed, { deliveryId })).body, { accepted: true, duplicate: false });

  await waitFor(state, "the ingress loop to terminate the echoed delivery", () => state.channel.inbox.snapshot(deliveryId)?.state !== "accepted" && state.channel.inbox.snapshot(deliveryId)?.state !== "processing");
  assert.equal(
    state.channel.inbox.snapshot(deliveryId)?.state,
    "quarantined",
    "an Activity Axis delivered must be recognised through the outbox and quarantined; `failed` here means the feedback gate did not recognise it and the prompt path refused it for some other reason",
  );
  assert.equal(state.executor.bindings.length, 1, "Axis must never dispatch a run for its own Activity");
});

/**
 * Drive waiting_for_input through a Router-minted Task and a real Worker. Require both
 * an elicitation Activity and an open run-chain decision so the visible question has a
 * claimable reply path.
 */
test("a real waiting_for_input park produces an elicitation Activity and an open run-chain decision", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  const asked = elicitation();
  state.executor.pushWaiting(binding(state, 0), asked);
  await settle();

  // The user is shown the question.
  assert.equal(countOf(state, "elicitation"), 1, "has waiting_for_input, so a real park must raise exactly one elicitation");
  assert.ok(bodyOf(state, "elicitation")?.includes(asked.question), "and the elicitation must carry the question the Worker actually asked");

  // Require the run chain's open decision to reference the parked Run alongside the
  // delivered elicitation. A visible question without its decision row cannot accept a
  // reply.
  const decision = state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID });
  assert.equal(decision?.status, "waiting", "the run chain must hold the decision the user was shown");
  assert.equal(state.channel.runChain.currentRun({ conversationId: AGENT_SESSION_ID })?.state, "waiting_for_input");

  // Require native select options and absence of reply tokens in the body. Options
  // alone could duplicate the question; body changes without options would leave it
  // unanswerable.
  const native = state.provider.createdNative.filter((value) => value !== null);
  assert.equal(native.length, 1, "exactly one Activity -- the elicitation -- carries a signal; every other one is posted with null");
  assert.equal(native[0]?.signal, "select");
  assert.deepEqual(
    native[0]?.options,
    asked.options.map((option) => ({ label: option.label, value: option.replyValue })),
    "each option is offered natively -- `label` is what the user sees, `value` is the decision-bound token that comes back as an ordinary prompt",
  );

  const shown = bodyOf(state, "elicitation") ?? "";
  for (const option of asked.options) {
    // Compared on the longest run the renderer's markdown escaping leaves
    // untouched, rather than on the raw string: `escapeMarkdown` rewrites `.`
    // and friends, so a raw `includes` fails for a reason that has nothing to
    // do with whether the text survived. Derived from the fixture, never a
    // second copy of the expected body.
    assert.ok(shown.includes(unescaped(option.label)), `the body still names ${option.label}`);
    assert.ok(shown.includes(unescaped(option.description)), "and still carries its description, which a native option has no field for");
  }
  // Asserted on the token PREFIX, not on each `replyValue`: a token re-added
  // through `escapeMarkdown` would not match the raw value, so a per-option
  // `includes` check would pass while the tokens were back on screen. This
  // fixture declares no `other`, which is the one token that legitimately
  // stays in the body -- see `renderElicitationMarkdown`.
  assert.ok(!shown.includes("axis-reply-"), "no reply token is printed; options are delivered through native signal controls");

  // The run is parked from the executor's side, so nothing terminal follows --
  // stated so this test cannot be read as "waiting silently succeeds".
  assert.equal(countOf(state, "response"), 0);
});

/**
 * Stop a Router-minted Linear Run after a real Worker parks it. Assert waiting state
 * and the open decision before posting stop so the test cannot pass without reaching
 * the parked path. Stop closes the decision locally without cancel or continuation
 * work.
 */

/**
 * Free text follows the waiting question's constraints: an offered Other consumes it as
 * a custom answer; a closed option set sends question and options to the model for
 * interpretation; invalid input is refused with the decision left open. Each path needs
 * its own observable outcome.
 */
test("free text answers a waiting question that offered Other, with no token typed", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  const asked = elicitationWithOther();
  state.executor.pushWaiting(binding(state, 0), asked);
  await waitFor(state, "the elicitation to reach the user", () => countOf(state, "elicitation") === 1);

  // Ordinary prose. No token in it anywhere -- that is the whole point.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Use whichever one finishes tonight."));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);

  await waitFor(state, "the free text to reach the Worker as a continue", () => state.executor.bindings.length === 2);
  // Waited for, not asserted at this instant. The dispatch reaching the Worker
  // and the decision being resolved are two separate steps -- the resolve lands
  // when the reservation settles -- so asserting here passed locally and raced
  // on CI. Converging on the terminal state is the assertion; a decision that
  // never resolved fails this by timing out rather than by luck.
  await waitFor(
    state,
    "answering with prose to consume the decision, exactly as a typed Other token does",
    () => state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID }) === null,
  );

  // Inspect the selected answer and instruction path as well as decision consumption.
  // Both custom-answer and interpretation paths consume the decision, so consumption
  // alone cannot distinguish them.
  const outcome = state.channel.ledgers.decisionOutcome(asked.decisionId);
  assert.deepEqual({ outcome: outcome?.outcome, label: outcome?.selectedLabel }, { outcome: "resolved", label: "Other" },
    "D1.2: an Other question's free text is the ANSWER, recorded with its selection -- not handed to the model to interpret");
  assert.ok((state.executor.instructions[1] ?? "").includes("Other: Use whichever one finishes tonight."),
    "and the Worker receives the custom answer itself, not an envelope asking it to choose between the listed options");
});

test("free text on a waiting question with a closed option set reaches the model, carrying the question and its options", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  // Use a closed question with two options and no Other.
  const asked = elicitation();
  state.executor.pushWaiting(binding(state, 0), asked);
  await waitFor(state, "the elicitation to reach the user", () => countOf(state, "elicitation") === 1);
  assert.equal(state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID })?.status, "waiting",
    "precondition: the decision really is open, so what follows is not passing vacuously");

  // Prose that names an option without containing its label or its token. This
  // is D1.1's own example shape: the relay cannot match it and is not asked to.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Use whichever one finishes tonight."));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);

  await waitFor(state, "the free text to reach the Worker as a continue", () => state.executor.bindings.length === 2);

  // Inspect the model's instruction, including the question and options. Dispatch
  // counts alone would pass if the context needed to interpret the answer were missing.
  const dispatched = state.executor.instructions[1] ?? "";
  assert.ok(dispatched.includes(asked.question), "the model must be told which question the reply is about");
  assert.ok(dispatched.includes("Use whichever one finishes tonight."), "and what the user actually said");
  for (const option of asked.options) {
    assert.ok(dispatched.includes(option.label), `the model must be given the option "${option.label}" it is allowed to choose`);
    assert.ok(dispatched.includes(option.description), `and that option's description, which is what makes "the fast one" resolvable`);
  }
  // D1.2's second half, D1.3 and D1.1's prohibition, each as a sentence the
  // model can act on. Matched on a distinctive fragment rather than the whole
  // line so a wording fix does not fail this, while deleting the rule does.
  assert.ok(dispatched.includes("Only those options are available"), "D1.2: prose may express an existing option, never invent a new one");
  assert.ok(dispatched.includes("ask the question again instead of choosing"), "D1.3: ambiguity is re-asked, not guessed at");
  assert.ok(dispatched.includes("do not re-decide the owner"), "D1.1: the model must not decide owner, authorization, or which question this answers");
  // The reply tokens are the relay's closed matching surface. Handing them to
  // the model invites it to emit one as though it were the user, which would
  // let a generated token answer a question no human answered.
  for (const option of asked.options) {
    assert.equal(dispatched.includes(option.replyValue), false, "no reply token may be quoted into the model's instruction");
  }

  // The decision settles with its Run rather than remaining open indefinitely.
  await waitFor(
    state,
    "the original decision to settle once the successor Run supersedes it",
    () => state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID }) === null,
  );

  // The interpreted decision settles as superseded rather than answered: no option was
  // chosen.
  const outcome = state.channel.ledgers.decisionOutcome(asked.decisionId);
  assert.equal(outcome?.outcome, "superseded", "an interpretation is not an answer");
  assert.equal(outcome?.selectedOptionId, null, "and it selected nothing");
  assert.equal(state.channel.ledgers.resolvedDecisionByActivity(AGENT_SESSION_ID, "activity-human-prompt"), null,
    "the answered-decision reader must not find it, or a later replay would resolve a question no one answered");

  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), [], "the dispatch is a decision, not a thrown transition");
  assert.equal(countOf(state, "error"), 0, "and the user is not told their reply was rejected");

  // Linear redelivers webhooks. The identical prompted event, arriving after
  // the decision has settled, must land on the command already dispatched --
  // not re-derive a different instruction from the same Activity. The
  // `superseded` row is what makes that possible: the ingress finds it, rebuilds
  // the identical envelope, and the run chain recognises the replay. Without it
  // the body would be re-read as an ordinary continuation, whose intent digest
  // differs, and the deterministic `commandId` would make that a `command_rebound`
  // refusal -- an ordinary redelivery turned into a user-visible failure.
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);
  await settle();
  assert.equal(state.executor.bindings.length, 2, "an exact redelivery must not dispatch a second time");
  assert.equal(countOf(state, "error"), 0, "and must not be reported to the user as a failure");
  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), [], "nor throw inside a loop");
});

/**
 * Invalid reply text receives refusal copy without consuming the decision or removing
 * the elicitation. Assert both durable decision state and the visible question so it
 * remains answerable.
 */
test("free text that fails the reply input constraints is refused, and leaves the decision unconsumed", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  state.executor.pushWaiting(binding(state, 0), elicitation());
  await waitFor(state, "the elicitation to reach the user", () => countOf(state, "elicitation") === 1);

  // Longer than the 4096 code points `freeformAnswer` allows, so the prompt
  // gate passes it and the DECISION input constraint is what refuses it. A typed `Other` answer of this length
  // is refused by the same bound, which is what "沿用现有输入约束" means.
  state.provider.activities.push(promptActivity("activity-human-prompt", "x".repeat(5_000)));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);
  await settle();

  assert.equal(state.executor.bindings.length, 1, "D1.3: a reply that does not meet the input constraints is not dispatched for interpretation");
  assert.equal(countOf(state, "elicitation"), 1, "and the question the user still has to answer is the one already in front of them");
  assert.equal(
    state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID })?.status,
    "waiting",
    "and the decision stays unconsumed, so the user still has exactly one unanswered question",
  );
  // The two assertions above are true of a CLEAN refusal and of a crash alike,
  // so these two tell those apart.
  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), [], "the refusal is a decision, not a thrown transition");
  assert.equal(countOf(state, "error"), 1, "and the user is told once, rather than left with a silently failing delivery");
});
test("a stop while a Router-minted Linear Run is parked closes the decision locally, with one response and no cancel", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  const asked = elicitation();
  state.executor.pushWaiting(binding(state, 0), asked);
  await waitFor(state, "the elicitation Activity", () => countOf(state, "elicitation") === 1);

  // Preconditions, not decoration: the Run really is parked and the run chain
  // really holds the open decision, both read back through the real stores.
  assert.equal(state.channel.runChain.currentRun({ conversationId: AGENT_SESSION_ID })?.state, "waiting_for_input");
  assert.equal(state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID })?.status, "waiting");
  assert.equal(countOf(state, "response"), 0, "no response yet, so the one below is the stop's");

  const stopResponse = await state.post(currentFixture("agent-session-stop.json"));
  assert.equal(stopResponse.status, 200);
  await waitFor(state, "the waiting-stop response Activity", () => countOf(state, "response") === 1);

  // Exactly one final `response`, carrying the waiting-stop copy rather than
  // the cancellation copy -- the two are different strings for different
  // situations, and `docs/LINEAR-PROVIDER.md` requires this one here.
  assert.equal(countOf(state, "response"), 1);
  assert.equal(bodyOf(state, "response"), LINEAR_ACTIVITY_COPY.waitingStopResponse);
  assert.notEqual(LINEAR_ACTIVITY_COPY.waitingStopResponse, LINEAR_ACTIVITY_COPY.cancellationResponse,
    "if these ever became the same string this assertion would stop distinguishing the two paths");

  // The decision is CLOSED, so Linear leaves the awaiting-input presentation
  // and a later reply cannot claim a decision nobody is waiting on.
  assert.equal(state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID }), null,
    "the pending decision must be closed by the stop, not left open");

  // And nothing was interrupted: `waiting_for_input` is already terminal, so
  // there is no live Run to cancel and no continuation for the stop itself.
  // Asserted through the real executor rather than inferred from the absence
  // of a response.
  assert.equal(state.executor.wasCancelRequested(binding(state, 0)), false,
    "a parked Run has nothing executing, so no cancel may reach the Worker");
  assert.equal(state.executor.bindings.length, 1, "and the stop must not start a second run");
});

/** The one healthy execution-context read, so the `update` arm gets past the read. */
const ISSUE_CONTEXT_OK = JSON.stringify({
  data: { issue: { id: "issue_1", labels: { nodes: [], pageInfo: { hasNextPage: false } }, state: { type: "unstarted" }, delegate: null, team: { id: "team_1", states: { nodes: [] } } } },
});

/**
 * classifyIssueSyncFailure distinguishes retryable transport failures from definite
 * startup failures using errors produced by the real provider. Drive the provider
 * through HTTP responses instead of constructing error strings in the test. Assert
 * retryable as well as code because the caller branches on that flag.
 */
test("the real LinearGraphQlProvider's issue-sync errors classify into the retryable code and each definite one", async (context) => {
  const cases: readonly {
    readonly what: string;
    readonly replies: readonly { readonly status?: number; readonly body: string }[];
    readonly code: string;
    readonly retryable: boolean;
  }[] = [
    {
      what: "Linear answers that the issue is not there",
      replies: [{ body: JSON.stringify({ data: { issue: null } }) }],
      code: "axis.relay.issue_sync_not_found", retryable: false,
    },
    {
      what: "the execution context comes back in a shape Axis cannot verify",
      replies: [{ body: JSON.stringify({ data: { issue: { id: "issue_1", labels: { nodes: [], pageInfo: { hasNextPage: true } }, state: { type: "unstarted" }, delegate: null, team: { id: "team_1" } } } }) }],
      code: "axis.relay.issue_sync_malformed", retryable: false,
    },
    {
      what: "Linear returns a GraphQL error on the read",
      replies: [{ body: JSON.stringify({ errors: [{ message: "no", extensions: { code: "INVALID_INPUT" } }] }) }],
      code: "axis.relay.issue_sync_read_rejected", retryable: false,
    },
    {
      what: "the read succeeds and `issueUpdate` answers success: false",
      replies: [{ body: ISSUE_CONTEXT_OK }, { body: JSON.stringify({ data: { issueUpdate: { success: false } } }) }],
      code: "axis.relay.issue_sync_update_rejected", retryable: false,
    },
    {
      what: "Linear answers 5xx",
      replies: [{ status: 503, body: JSON.stringify({ errors: [] }) }],
      code: "linear.provider.rejected", retryable: true,
    },
    {
      what: "Linear rate-limits the read",
      replies: [{ body: JSON.stringify({ errors: [{ message: "slow down", extensions: { code: "RATELIMITED" } }] }) }],
      code: "linear.provider.rejected", retryable: true,
    },
    {
      what: "Linear answers 200 with a body that is not JSON at all",
      replies: [{ body: "<html>maintenance</html>" }],
      code: "axis.relay.issue_sync_failed", retryable: false,
    },
  ];

  const seen: { readonly what: string; readonly code: string; readonly retryable: boolean }[] = [];
  for (const scenario of cases) {
    const pending = [...scenario.replies];
    const operations: string[] = [];
    const server = createServer((request, response) => { void (async () => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      const sent = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { readonly query: string };
      operations.push(sent.query.includes("AxisIssueUpdate") ? "update" : "read");
      const reply = pending.shift();
      if (reply === undefined) { response.writeHead(500, { "content-type": "application/json" }); response.end("{}"); return; }
      response.writeHead(reply.status ?? 200, { "content-type": "application/json" });
      response.end(reply.body);
    })().catch(() => { response.destroy(); }); });
    await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("the GraphQL stand-in did not bind");
    context.after(async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
    });

    const provider = new LinearGraphQlProvider({ endpoint: `http://127.0.0.1:${address.port}/graphql`, timeoutMs: 4_000 });
    const step = linearIssueContextStep(provider);
    const thrown = await step.synchronizeIssueForStart({
      accessToken: "lin_oauth_test", issueId: "issue_1", teamId: "team_1", appUserId: "app_user_1", agentSessionId: AGENT_SESSION_ID,
    }).then(() => null, (error: unknown) => error);

    assert.ok(thrown instanceof RelayError, `${scenario.what}: the step must fail with a RelayError, got ${String(thrown)}`);
    seen.push({ what: scenario.what, code: thrown.code, retryable: thrown.retryable });

    // The reads really did reach the provider, so a case that failed before the
    // HTTP call -- which would classify identically and prove nothing -- is
    // visible rather than silently green.
    assert.ok(operations.length >= 1, `${scenario.what}: the provider must have actually called Linear`);
    if (scenario.code === "axis.relay.issue_sync_update_rejected") {
      assert.deepEqual(operations, ["read", "update"], "the update arm must be reached through a successful read");
    }
  }

  assert.deepEqual(
    seen,
    cases.map((scenario) => ({ what: scenario.what, code: scenario.code, retryable: scenario.retryable })),
    "each issue-sync failure keeps its own cause, and only the transport arm stays retryable",
  );

  // A guard on the table itself: if every arm collapsed to one code the
  // comparison above would still pass while the classification had stopped
  // distinguishing anything.
  assert.equal(new Set(seen.map((entry) => entry.code)).size, 6, "the seven answers must still classify into six distinct codes -- only the two transport ones share");
});

/**
 * Delivery and provider reconciliation must hash the same canonical Activity content
 * with the same field order. A mismatch terminally classifies an accepted Activity as
 * conflict. Compare both implementations through real provider responses, including
 * content with and without the optional result field.
 */
test("the Linear delivery digest and the provider's canonical content order agree, read from both real implementations", async (context) => {
  const contents = [
    { type: "action", action: "read", parameter: "issue-1", result: "done" },
    { type: "action", action: "read", parameter: "issue-1" },
    { type: "thought", body: "thinking" },
  ] as const;

  for (const content of contents) {
    const narrowed = linearActivityContent(content as ChannelMessageContent);
    assert.notEqual(narrowed, null, `linearActivityContent must admit ${JSON.stringify(content)}`);
    const json = JSON.stringify(narrowed);

    // Form: `channelInstructionDigest` is what the outbox stores and `sha256`
    // is what the provider compares against. Neither is spelled out here.
    assert.equal(channelInstructionDigest(json), sha256(json),
      "the outbox's ChannelDigest and the provider's sha256 must be the same string, or every reconcile is a spurious conflict");
  }

  // Order: the provider's own rebuild, obtained by asking it.
  const server = createServer((request, response) => { void (async () => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    const sent = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { readonly variables: { readonly activityId: string } };
    // Deliberately NOT in the canonical order: if the provider echoed the wire
    // object instead of rebuilding it, this case would still agree with
    // `linearActivityContent` only by accident, and the assertion below would
    // stop testing the rebuild.
    const wire = sent.variables.activityId === "activity_with_result"
      ? { __typename: "AgentActivityActionContent", result: "done", parameter: "issue-1", action: "read" }
      : { __typename: "AgentActivityActionContent", parameter: "issue-1", action: "read", result: null };
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ data: { agentActivity: {
      id: sent.variables.activityId, updatedAt: "2026-09-18T00:00:00.000Z", user: null,
      agentSession: { id: "agent_session_1" }, sourceComment: null, content: wire,
    } } }));
  })().catch(() => { response.destroy(); }); });
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("the GraphQL stand-in did not bind");
  context.after(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
  });
  const provider = new LinearGraphQlProvider({ endpoint: `http://127.0.0.1:${address.port}/graphql`, timeoutMs: 4_000 });

  for (const [activityId, content] of [
    ["activity_with_result", { type: "action", action: "read", parameter: "issue-1", result: "done" }],
    ["activity_without_result", { type: "action", action: "read", parameter: "issue-1" }],
  ] as const) {
    const rebuilt = await provider.loadExactAgentActivity("lin_oauth_test", activityId);
    assert.equal(
      JSON.stringify(rebuilt.activity.content),
      JSON.stringify(linearActivityContent(content as ChannelMessageContent)),
      `${activityId}: linearActivityContent must emit the provider's canonical key order, or the stored digest can never match the reconcile`,
    );
    // And the digest the outbox would have stored really does confirm against
    // the provider's comparison, which is the property both halves exist for.
    assert.equal(
      channelInstructionDigest(JSON.stringify(linearActivityContent(content as ChannelMessageContent))),
      sha256(JSON.stringify(rebuilt.activity.content)),
      `${activityId}: the stored digest must equal what reconcileAgentActivity computes from Linear's own answer`,
    );
  }
});

/**
 * Render pending and decided ambiguous startup copy through production functions and
 * require different sentences. Pending copy must not claim the outcome is already
 * unconfirmed. axis.relay.internal is the safe code for the post-submit Router-stage
 * failure driven here.
 */
const PENDING_STARTUP_COPY = renderStartupFailure({ certainty: "ambiguous", stage: "Router", reason: "outcome_pending", safeCode: "axis.relay.internal" });
const UNCONFIRMED_STARTUP_COPY = renderStartupFailure({ certainty: "ambiguous", stage: "Router", reason: "outcome_unknown", safeCode: "axis.relay.internal" });

test("pending and decided unconfirmed startup copy describe different outcomes", () => {
  assert.notEqual(PENDING_STARTUP_COPY, UNCONFIRMED_STARTUP_COPY,
    "pending and decided startup outcomes must render distinct response bodies");
  assert.match(UNCONFIRMED_STARTUP_COPY, /^Axis could not confirm whether this session started\./u);
  assert.doesNotMatch(PENDING_STARTUP_COPY, /could not confirm/u);
  assert.match(PENDING_STARTUP_COPY, /^Axis has not yet confirmed whether this session started\./u);
  // Neither implies failure, and neither promises success; the operational
  // fact both must keep is the one about retrying, which is true of both.
  for (const copy of [PENDING_STARTUP_COPY, UNCONFIRMED_STARTUP_COPY]) assert.match(copy, /Axis will not retry automatically\.$/u);
});

test("a start whose dispatch reply is lost while the Worker really ran is told Axis has not confirmed YET, and the Run's own answer follows", async (context) => {
  const state = await harness(context);

  // The Worker's `run.accepted` is held on the Node hop so Router provably has
  // no evidence when the dispatch response goes missing, rather than merely
  // usually having none; the gate's own comment records how narrow that window
  // measured and why it is closed anyway.
  state.holdNodeEvents();
  state.nodePort.failNextResponse("lost");

  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);

  // Require an accepted Worker turn so the pending Activity describes work that is
  // actually running.
  await waitFor(state, "the real Worker to accept and open the turn whose dispatch reply is then lost", () => state.executor.bindings.length === 1);
  await waitFor(state, "the startup copy for the lost dispatch reply", () => bodies(state, "thought").includes(PENDING_STARTUP_COPY));

  // Inspect every Activity visible while the Run executes. A correct pending body is
  // insufficient if another Activity falsely reports a decided failure.
  assert.equal(JSON.stringify(state.activities()).includes("could not confirm"), false,
    "a Run that really started must never be described as one Axis could not confirm");

  // Release the events and let the Run's own answer follow the pending copy. No
  // corrective Activity is needed.
  state.releaseNodeEvents();
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "lost-reply-final", projectionStatus: "projected", body: "Axis finished the lost-reply run.", truncated: false, truncations: [] });
  await waitFor(state, "the Run's own terminal answer to reach Linear", () => countOf(state, "response") === 1);
  assert.equal(bodyOf(state, "response"), "Axis finished the lost-reply run.");

  assert.equal(JSON.stringify(state.activities()).includes("could not confirm"), false,
    "nor after the Run has answered");
  assert.equal(bodies(state, "thought").filter((body) => body === PENDING_STARTUP_COPY).length, 1,
    "one in-flight sentence, not one per tick");

  // The observe loop adopts Router's accepted lineage after held events are released.
  // Settling a reservation this way must not produce the absent-start terminal answer:
  // the Run actually executed.
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 0,
    "an accepted start receives the Run's own response without a corrective Activity");
});

test("a definite route-selection rejection tells the user the session did not start", async (context) => {
  const state = await harness(context);

  // A route-selection rejection is a definite failure before execution and must still
  // tell the user that no session started. This controls for pending copy incorrectly
  // replacing decided failures.
  state.breakRouteSelection();
  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);

  await waitFor(state, "the decided startup failure to reach Linear", () => countOf(state, "error") === 1);
  assert.equal(state.executor.bindings.length, 0, "nothing may have run: this is the genuinely-not-started case");

  const told = bodyOf(state, "error");
  assert.match(String(told), /^Axis could not start this session\./u, "a decided failure states it, and does not hedge");
  assert.match(String(told), /then create a new Agent session\.$/u, "and says what to do about it");

  // Neither pending nor unresolved ambiguous copy belongs on a definite refusal.
  assert.equal(bodyCount(state, PENDING_STARTUP_COPY), 0,
    "a decided failure must not be reported as an outcome Axis is still waiting on");
  assert.equal(JSON.stringify(state.activities()).includes("could not confirm"), false,
    "a decided failure must not be reported as one Axis could not confirm either");
});

/**
 * A Relay crash before Router admission leaves a reserved command with no Run. After
 * its dispatching inbox delivery becomes terminal, the real sweep settles it absent,
 * frees the slot and emits exactly one terminal Activity saying the session did not
 * start.
 *
 * Make the inbox row terminal through the real store because the sweep retains
 * reservations whose delivery is still accepted or processing. Additional observe ticks
 * establish that the terminal message is not repeated.
 */
test("a start the relay lost before Router ever saw it is finally told, once, that it did not start", async (context) => {
  const state = await harness(context);

  // The crash. See `crashNextDispatchBeforeRouterSeesIt` for why this is the only
  // shape that reaches `reconcileStale`'s `absent` arm, and for the line
  // between what it fakes and what stays real.
  state.crashNextDispatchBeforeRouterSeesIt();
  const deliveryId = randomUUID();
  assert.equal((await state.post(currentFixture("agent-session-created.json"), { deliveryId })).status, 200);
  await waitFor(state, "the relay to die inside the start dispatch", () => state.crashedDispatches() === 1);

  // Nothing ran -- this is a start that genuinely did not happen -- and the
  // dying tick posted nothing, because its stores went with it. Both matter:
  // the first is what makes the copy below true, the second is what makes
  // every Activity from here on attributable to the sweep.
  assert.equal(state.executor.bindings.length, 0, "nothing may have run: Router never received this command");
  assert.deepEqual(state.activities(), [], "the crash must leave no Activity of its own");

  // The process comes back over the same database files. The window is
  // shortened for this composition only: five minutes of real time is what
  // `DEFAULT_STALE_RESERVATION_MS` buys a live dispatch, and there is no live
  // dispatch left to protect -- the one that held this reservation died with
  // the previous process.
  state.sweepStrandedReservationsAfter(0);
  await state.restart();

  // While the crashed delivery remains processing, retain protects its reservation.
  // Terminalize it through the real inbox store to permit the sweep, as retry
  // exhaustion would.
  state.channel.inbox.fail(deliveryId, LINEAR_INBOX_LEASE_OWNER, "axis.relay.interrupted-dispatch_test_terminal");

  await waitFor(state, "the terminal answer for a start that never reached Router",
    () => bodies(state, "error").includes(LINEAR_ACTIVITY_COPY.startNeverReachedRouter));

  // The sweep is what decided it, not a reclaim that re-dispatched and failed:
  // `axis.channel.confirmed_absent` is `CONFIRMED_ABSENT_SAFE_CODE`
  // (`src/router/channel-run-chain.ts`), written by `#sweepSettle` on the
  // `absent` arm and by nothing else in either channel.
  const settled = state.channel.runChain.initialStartReservation({ conversationId: AGENT_SESSION_ID });
  assert.equal(settled?.status, "rejected", "the sweep must have settled the stranded reservation");
  assert.equal(settled?.safeCode, "axis.channel.confirmed_absent", "and settled it on the confirmed-absent arm specifically");

  // The copy itself, at the layer the user reads it: a decided answer, the
  // retry fact, and a way forward.
  const told = bodyOf(state, "error");
  assert.match(String(told), /^Axis has confirmed that this session did not start\./u, "a decided answer states it");
  assert.match(String(told), /Axis will not retry it automatically\./u, "the confirmed-absence response states that Axis will not retry automatically");
  assert.match(String(told), /create a new Agent session on this issue to run it\.$/iu, "and says what to do instead");

  // Pending work and decided absence require different copy; neither may claim the
  // other's outcome.
  assert.equal(JSON.stringify(state.activities()).includes("could not confirm"), false,
    "confirmed absence must not be described as an unconfirmed outcome");
  assert.equal(bodyCount(state, PENDING_STARTUP_COPY), 0,
    "nor leave the user on the in-flight sentence: this outcome is decided");

  // Dozens more observe ticks, and still one. The sweep runs every interval;
  // this is the assertion that a per-tick key would fail.
  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 1,
    "one terminal answer, not one per observe tick");

  // ------------------------------------------------------------------
  // The sweep's OTHER answers, on the same relay, with the window still 0.
  // ------------------------------------------------------------------
  //
  // Only the decided negative may speak. Every other disposition is either
  // undecided or decided the other way, and telling a user their session did
  // not start on any of them is the failure mode this half exists to catch.
  //
  // A 0ms window is what makes them reachable at all: `reconcileSettledEarly`
  // takes `reserved_at > now - staleAfterMs`, the exact complement of the
  // sweep's, so at 0 it claims nothing and EVERY answer about this second
  // start comes back through `reconcileStale` -- `unknown` on every tick while
  // the Worker's events are held, then `accepted` once they are released. With
  // the shipped five-minute window the early settle takes the second one and
  // no suite reaches either.
  //
  // Router holds this command's row (`#claimNodeCommand` commits inside the
  // synchronous transaction that precedes any dispatch), so the sweep can
  // never mistake this live start for the absent one above.
  state.holdNodeEvents();
  state.nodePort.failNextResponse("lost");
  assert.equal((await state.post(currentFixture("agent-session-created.json", {
    agentSession: {
      id: SECOND_AGENT_SESSION_ID,
      status: "pending",
      creator: { id: "human-user-fixture" },
      issue: { id: "issue-fixture", team: { id: TEAM_ID } },
    },
  }))).status, 200);

  await waitFor(state, "the real Worker to open the second turn whose dispatch reply is then lost", () => state.executor.bindings.length === 1);
  await waitFor(state, "the in-flight sentence for the second, lost dispatch reply", () => bodies(state, "thought").includes(PENDING_STARTUP_COPY));
  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 1,
    "dozens of `unknown` sweep answers are not an answer: an undecided outcome must not be told as a decided one");

  state.releaseNodeEvents();
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "interrupted-dispatch-second", projectionStatus: "projected", body: "Axis finished the second run.", truncated: false, truncations: [] });
  await waitFor(state, "the second Run's own terminal answer to reach Linear", () => countOf(state, "response") === 1);
  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 1,
    "an accepted settlement must not add a corrective Activity");
});

/**
 * Lose a continuation's dispatch reply while holding Worker events so in-place
 * convergence remains unknown. Deliver pending outcome copy until the real Run answer
 * arrives; do not claim the continuation never reached Router. The no-op retry pause
 * keeps this window deterministic.
 */
test("a continue whose dispatch reply is lost while the Worker really ran is told Axis has not confirmed YET, and the Run's own answer follows", async (context) => {
  const state = await harness(context);

  // A session that really started, ran and answered -- the continue under
  // test is dispatched against it.
  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);
  await waitFor(state, "the start to reach the real Worker", () => state.executor.bindings.length === 1);
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "active-follow-up-continue-first", projectionStatus: "projected", body: "Axis finished the first run.", truncated: false, truncations: [] });
  await waitFor(state, "the first Run's answer to reach Linear", () => countOf(state, "response") === 1);

  // Make the prompt Activity available for the resolver's exact provider reread.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Please continue with the tests."));
  state.holdNodeEvents();
  state.nodePort.failNextResponse("lost");
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);

  // The Worker really accepted and really opened the second turn. This is what
  // makes the pending copy true rather than merely pessimistic: the prompt the
  // user is about to be told about not-yet-confirmed IS running.
  await waitFor(state, "the real Worker to open the second turn whose dispatch reply is then lost", () => state.executor.bindings.length === 2);
  await waitFor(state, "the continue's pending copy for the lost dispatch reply", () => bodies(state, "thought").includes(LINEAR_CHANNEL_COPY.continueOutcomePending));

  // Assertion 1: the pending thought is posted exactly once.
  assert.equal(bodyCount(state, LINEAR_CHANNEL_COPY.continueOutcomePending), 1,
    "one pending sentence for the lost-reply continue, not zero and not more than one");

  // Assertion 2: the decided negative must never appear while the outcome is
  // still owed -- an undecided outcome must not be told as a decided one.
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.continueNeverReachedRouter), 0,
    "an outcome still owed must never be reported as the decided negative");

  // Release the events so the Run's own answer follows, without another corrective
  // Activity.
  state.releaseNodeEvents();
  state.executor.pushSucceeded(binding(state, 1), { sourceEventId: "active-follow-up-continue-final", projectionStatus: "projected", body: "Axis finished the lost-reply continue.", truncated: false, truncations: [] });
  await waitFor(state, "the second Run's own terminal answer to reach Linear", () => countOf(state, "response") === 2);
  assert.deepEqual(bodies(state, "response"), ["Axis finished the first run.", "Axis finished the lost-reply continue."]);

  // Assertion 3: releasing and the Run's own answer must not have changed
  // either count.
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.continueNeverReachedRouter), 0,
    "nor after the Run has answered");
  assert.equal(bodyCount(state, LINEAR_CHANNEL_COPY.continueOutcomePending), 1,
    "one pending sentence, not one per tick");
});

/**
 * A crash can strand a continuation as well as a start. When the sweep decides absent,
 * free the slot and use continueNeverReachedRouter: the session already started, but
 * this instruction never reached Router. A session-did-not-start sentence would be
 * false.
 */
test("an absent continuation frees its slot and reports that instruction never reached Router", async (context) => {
  const state = await harness(context);

  // A session that really started, ran and answered. Everything the copy under
  // test would be false about.
  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);
  await waitFor(state, "the start to reach the real Worker", () => state.executor.bindings.length === 1);
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "interrupted-dispatch-continue-first", projectionStatus: "projected", body: "Axis finished the first run.", truncated: false, truncations: [] });
  await waitFor(state, "the first Run's answer to reach Linear", () => countOf(state, "response") === 1);

  // Crash the continuation before Router admission. Provide the exact Activity reread
  // used to verify its instruction.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Please continue with the tests."));
  state.crashNextDispatchBeforeRouterSeesIt();
  const deliveryId = randomUUID();
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"), { deliveryId })).status, 200);
  await waitFor(state, "the relay to die inside the continue dispatch", () => state.crashedDispatches() === 1);

  state.sweepStrandedReservationsAfter(0);
  await state.restart();

  // The crashed delivery's processing state retains its reservation. Terminalize that
  // row through the inbox before expecting the sweep to act.
  state.channel.inbox.fail(deliveryId, LINEAR_INBOX_LEASE_OWNER, "axis.relay.interrupted-dispatch_test_terminal");

  // The sweep really ran on it: the conversation's single command slot, held
  // by the stranded continue, is released -- which only `settle` does, and on
  // this path only `#sweepSettle`'s `absent` arm reaches `settle`.
  await waitFor(state, "the sweep to settle the stranded continue and free the conversation's slot",
    () => state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.pendingCommandId === null);

  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 0,
    "a continue that never reached Router must not be answered with the start sentence");
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.continueNeverReachedRouter), 1,
    "and must be answered with its own sentence instead");
});

/**
 * Keep a start's reply and Worker events unconfirmed past the configured window. Settle
 * its reservation terminal exactly once and guard the conversation from further
 * instructions. This start has no local live Run eligible for a stop.
 */
test("a start whose dispatch reply stays unconfirmed past the window is settled terminal, once, and guards the conversation", async (context) => {
  const state = await harness(context);

  state.holdNodeEvents();
  state.nodePort.failNextResponse("lost");
  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);

  await waitFor(state, "the real Worker to open the turn whose dispatch reply is then lost", () => state.executor.bindings.length === 1);
  await waitFor(state, "the startup copy for the lost dispatch reply", () => bodies(state, "thought").includes(PENDING_STARTUP_COPY));

  // Both windows to 0, applied from the next restart -- the same rule
  // `sweepStrandedReservationsAfter`'s own doc states. Router genuinely holds
  // this command's row (the real Worker really dispatched), so nothing here
  // can be mistaken for the sweep's `absent` arm.
  state.sweepStrandedReservationsAfter(0);
  state.unconfirmedReservationsAfter(0);
  await state.restart();

  await waitFor(state, "the unconfirmed sweep's terminal answer for the start",
    () => bodies(state, "error").includes(LINEAR_ACTIVITY_COPY.startOutcomeUnconfirmed));

  // The slot is released: the unconfirmed arm reaches `settle`, same as `absent`.
  assert.equal(state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.pendingCommandId, null,
    "the unconfirmed sweep must release the conversation's command slot");

  // Exactly once, across many further ticks -- the same one-shot property
  // `ABSENT_COMMAND_COPY`'s `axis.start.absent:` prefix gives `absent`,
  // mirrored here by `UNCONFIRMED_COMMAND_COPY`'s `axis.command.unconfirmed:`.
  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startOutcomeUnconfirmed), 1,
    "one terminal answer, not one per observe tick");

  // This outcome was never decided `absent` -- Router genuinely holds the
  // command row -- so the absent copy, and any dispatch-failure copy, must
  // never appear.
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 0,
    "an unconfirmed ending must never be told as the decided `absent` negative");
  assert.equal(JSON.stringify(state.activities()).includes("could not dispatch"), false,
    "nor as a dispatch failure -- nothing about this command's dispatch failed");

  // The guard: a later prompt on this conversation is refused
  // `session_unconfirmed`, rendered through the same three-line copy every
  // other prompt rejection uses.
  // The fixture's `agentActivity.id` is the fixed literal `"activity-human-prompt"`
  // (see `test/fixtures/linear/agent-session-prompted.json`), which is what
  // `commandId` derives from for a session-origin prompt -- so this must match
  // it exactly for the exact-activity reread to find it.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Try again please."));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);
  await waitFor(state, "the guarded conversation's session_unconfirmed refusal",
    () => bodies(state, "error").includes(renderLinearPromptRejection("session_unconfirmed")));
  assert.equal(state.executor.bindings.length, 1, "the guard must refuse before any dispatch reaches the Worker");

  // Late Run output is observed independently through the session ledger and can still
  // reach Linear. It does not reopen the rejected unconfirmed reservation or remove the
  // conversation guard; assert both the late answer and continued guard.
  state.releaseNodeEvents();
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "unconfirmed-t1-late", projectionStatus: "projected", body: "Axis finished the late run.", truncated: false, truncations: [] });
  await waitFor(state, "the late Run's own answer to still be projected after the unconfirmed terminal Activity",
    () => bodies(state, "response").includes("Axis finished the late run."));

  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startOutcomeUnconfirmed), 1,
    "the unconfirmed sweep's terminal answer must still be exactly one, not corrected or duplicated by the late output");
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 0,
    "late output must not retroactively produce the decided `absent` negative either");
  assert.equal(JSON.stringify(state.activities()).includes("could not dispatch"), false,
    "nor a dispatch-failure copy");
  assert.equal(state.tickFailures.length, 0, "projecting the late output must not fail a tick");
});

/**
 * Start and finish one Run, then lose the reply for a continuation opening another Run.
 * If that outcome stays unconfirmed, settle it once and guard later prompts rather than
 * creating another reservation that can be falsely described as absent.
 */
test("a continue that opens a new Run and stays unconfirmed past the window is settled terminal, once, and guards the conversation", async (context) => {
  const state = await harness(context);

  // Begin with a session that has genuinely started and answered before dispatching the
  // continuation under test.
  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);
  await waitFor(state, "the start to reach the real Worker", () => state.executor.bindings.length === 1);
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "unconfirmed-t2-first", projectionStatus: "projected", body: "Axis finished the first run.", truncated: false, truncations: [] });
  await waitFor(state, "the first Run's answer to reach Linear", () => countOf(state, "response") === 1);

  // The fixture's `agentActivity.id` is the fixed literal `"activity-human-prompt"`.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Please continue with the tests."));
  state.holdNodeEvents();
  state.nodePort.failNextResponse("lost");
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);

  await waitFor(state, "the real Worker to open the second turn whose dispatch reply is then lost", () => state.executor.bindings.length === 2);
  await waitFor(state, "the continue's pending copy for the lost dispatch reply", () => bodies(state, "thought").includes(LINEAR_CHANNEL_COPY.continueOutcomePending));

  state.sweepStrandedReservationsAfter(0);
  state.unconfirmedReservationsAfter(0);
  await state.restart();

  await waitFor(state, "the unconfirmed sweep's terminal answer for the continue",
    () => bodies(state, "error").includes(LINEAR_ACTIVITY_COPY.continueOutcomeUnconfirmed));

  assert.equal(state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.pendingCommandId, null,
    "the unconfirmed sweep must release the conversation's command slot");

  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.continueOutcomeUnconfirmed), 1,
    "one terminal answer for the continue, not one per observe tick");

  // Never use continueNeverReachedRouter for an outcome that stayed unconfirmed.
  // Require the guard to prevent another prompt from creating a second stranded
  // reservation later reported as absent.
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.continueNeverReachedRouter), 0,
    "an unconfirmed continue that opened a new Run must never be told the decided `absent` negative");

  // The guard itself: a later, DISTINCT prompt (a fresh `agentActivity.id`,
  // so it computes a fresh `commandId` and genuinely reaches `reserve` rather
  // than replaying the settled continue above) on this conversation is
  // refused `session_unconfirmed`. `currentFixture` merges shallowly, so the
  // whole `agentActivity` object is restated rather than only its `id`.
  const secondPromptId = "activity-unconfirmed-t2-second-prompt";
  state.provider.activities.push(promptActivity(secondPromptId, "Try again please."));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json", {
    agentActivity: { id: secondPromptId, user: { id: "human-user-fixture" }, body: "Try again please.", signal: "continue", createdAt: new Date().toISOString() },
  }))).status, 200);
  await waitFor(state, "the guarded conversation's session_unconfirmed refusal",
    () => bodies(state, "error").includes(renderLinearPromptRejection("session_unconfirmed")));
  assert.equal(state.executor.bindings.length, 2, "the guard must refuse before any dispatch reaches the Worker");

  // Allow additional real loop intervals before rechecking that the guarded
  // conversation cannot produce a false absent answer.
  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.continueNeverReachedRouter), 0,
    "still never, after the guard has had many more ticks to be bypassed if it were missing");

  state.releaseNodeEvents();
  await settle();
});

/**
 * Lose the reply for a continuation answering a waiting question, leaving the local
 * current Run parked. Once the reservation becomes unconfirmed, refuse stop with
 * stopSessionUnconfirmed rather than claim the already-answered question was stopped.
 *
 * Release late events and emit success. The late answer must reach Linear without
 * repeated unknown_run observe failures, while the unconfirmed guard remains.
 */
test("an unconfirmed waiting-answer continuation refuses stop and permits late output without observe failures", async (context) => {
  const state = await harness(context);

  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  const asked = elicitationWithOther();
  state.executor.pushWaiting(binding(state, 0), asked);
  await waitFor(state, "the elicitation to reach the user", () => countOf(state, "elicitation") === 1);
  assert.equal(state.channel.runChain.currentRun({ conversationId: AGENT_SESSION_ID })?.state, "waiting_for_input",
    "precondition: the local current Run is parked, not null and not settled");

  state.holdNodeEvents();
  state.nodePort.failNextResponse("lost");
  // Answer the open decision with ordinary prose, claiming it through the free-text
  // path.
  state.provider.activities.push(promptActivity("activity-human-prompt", "Use whichever one finishes tonight."));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);

  await waitFor(state, "the real Worker to open the continuation turn whose dispatch reply is then lost", () => state.executor.bindings.length === 2);
  await waitFor(state, "the continue's pending copy for the lost dispatch reply", () => bodies(state, "thought").includes(LINEAR_CHANNEL_COPY.continueOutcomePending));

  state.sweepStrandedReservationsAfter(0);
  state.unconfirmedReservationsAfter(0);
  await state.restart();

  await waitFor(state, "the unconfirmed sweep's terminal answer for the continue",
    () => bodies(state, "error").includes(LINEAR_ACTIVITY_COPY.continueOutcomeUnconfirmed));

  // corr-f1: the stop on this guarded, still-parked conversation.
  const stopResponse = await state.post(currentFixture("agent-session-stop.json"));
  assert.equal(stopResponse.status, 200);
  await waitFor(state, "the guarded stop's truthful refusal", () => bodies(state, "error").includes(LINEAR_CHANNEL_COPY.stopSessionUnconfirmed));
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.waitingStopResponse), 0,
    "corr-f1: a parked (not executing) Run must never be told Axis stopped a waiting request it never touched");
  assert.equal(state.executor.wasCancelRequested(binding(state, 1)), false,
    "and the answered continuation's own turn must never receive a cancel it was never sent");

  // corr-f2: late acceptance of the continuation.
  state.releaseNodeEvents();
  state.executor.pushSucceeded(binding(state, 1), { sourceEventId: "unconfirmed-t3-late", projectionStatus: "projected", body: "Late continuation output.", truncated: false, truncations: [] });
  await waitFor(state, "the late continuation's own answer to still be projected after the unconfirmed terminal Activity",
    () => bodies(state, "response").includes("Late continuation output."));

  await settle();
  assert.equal(state.tickFailures.length, 0,
    "corr-f2: a guarded conversation's late-accepted successor Run must not wedge the observe tick with a permanent unknown_run failure");
});

/**
 * Reservation ownership follows its dispatching inbox delivery: accepted or processing
 * retains a stale reservation; terminal or missing delivery permits the absent sweep.
 * Exercise both with the same crash so disabling the sweep entirely cannot satisfy the
 * retention case.
 */
test("a stale reservation whose dispatching delivery is still processing is retained, and the delivery's own reclaim dispatches the original start", async (context) => {
  const state = await harness(context);

  // Shortened so the created delivery's own reclaim -- once its lease expires
  // -- happens inside this test's real time, rather than the shipped 30s
  // (`LINEAR_INBOX_LEASE_MS`). Applies from the next restart, so it must be in
  // place before the very first claim: the one the crash below stalls.
  state.holdInboxLeaseFor(200);
  await state.restart();

  state.crashNextDispatchBeforeRouterSeesIt();
  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);
  await waitFor(state, "the relay to die inside the start dispatch", () => state.crashedDispatches() === 1);
  assert.equal(state.executor.bindings.length, 0, "nothing may have run yet: Router never received this command");
  assert.deepEqual(state.activities(), [], "the crash must leave no Activity of its own");

  // The sweep's window is 0 -- with no `retain`, it would settle this
  // reservation `absent` on its very first tick after restart. The inbox
  // delivery is still `processing` (its own reclaim has not run: the lease is
  // still live), so it must not.
  state.sweepStrandedReservationsAfter(0);
  await state.restart();

  // Several ingress AND observe ticks pass while the delivery's lease is
  // still live -- both loops run every 5ms (`TICK_INTERVALS`), so this is
  // dozens of sweep attempts, not one. `retain` must hold through every one.
  await settle(10);
  const duringLease = state.channel.runChain.initialStartReservation({ conversationId: AGENT_SESSION_ID });
  assert.equal(duringLease?.status, "reserved",
    "the sweep must retain a reservation whose dispatching delivery has not reached a terminal inbox state");
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 0,
    "and must not have told the user their session did not start while that delivery could still reclaim it");

  // The lease now expires (200ms, well past); the SAME created delivery
  // reclaims its own reservation and this time -- unarmed since the one crash
  // above already fired -- really dispatches it.
  await waitFor(state, "the delivery's own reclaim to actually dispatch the original start", () => state.executor.bindings.length === 1);

  // `executor.bindings` fills inside the Worker's HTTP handler, which is
  // strictly upstream of `submit` resolving and of `channel.settle` writing
  // the reservation -- so a reservation read taken the instant the binding
  // appears can still observe `reserved` in flight. Wait for settlement to
  // actually land before reading its outcome; this does not weaken the
  // assertion below, since the sweep winning the race also ends the
  // reservation's `reserved` state (as `rejected`), so the wait still
  // distinguishes who settled the row, not merely how soon.
  await waitFor(state, "the reclaimed reservation to settle",
    () => state.channel.runChain.initialStartReservation({ conversationId: AGENT_SESSION_ID })?.status !== "reserved");

  // The reservation is settled by the reclaim's own dispatch, never by the
  // sweep: had the sweep won the race even once, this would be `rejected`
  // with `axis.channel.confirmed_absent` and the reclaim would have answered
  // `already_started` without ever reaching the Worker above.
  const settledAfter = state.channel.runChain.initialStartReservation({ conversationId: AGENT_SESSION_ID });
  assert.equal(settledAfter?.status, "accepted", "the reclaim's own dispatch settles the reservation, not the sweep");

  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 0,
    "the start really ran: the sweep must never have settled it `absent`, at any point in the race");
});

test("a stale reservation whose dispatching delivery has already reached a terminal inbox state is still settled absent, exactly once", async (context) => {
  const state = await harness(context);

  state.crashNextDispatchBeforeRouterSeesIt();
  const deliveryId = randomUUID();
  assert.equal((await state.post(currentFixture("agent-session-created.json"), { deliveryId })).status, 200);
  await waitFor(state, "the relay to die inside the start dispatch", () => state.crashedDispatches() === 1);
  assert.equal(state.executor.bindings.length, 0, "nothing may have run: Router never received this command");
  assert.deepEqual(state.activities(), [], "the crash must leave no Activity of its own");

  state.sweepStrandedReservationsAfter(0);
  await state.restart();

  // Terminalize the stranded delivery directly, through the real inbox store
  // and its real `fail` method -- exactly the state an unrelated retry-budget
  // exhaustion or a decisive non-retryable failure would leave it in.
  // `retain` reads only `state`, never how a delivery arrived at it, so this
  // is a faithful way to construct "already terminal" without needing to
  // drive N real retries first. This delivery will now never reclaim its
  // reservation.
  state.channel.inbox.fail(deliveryId, LINEAR_INBOX_LEASE_OWNER, "axis.relay.stale-reconcile_test_terminal");
  assert.equal(state.channel.inbox.snapshot(deliveryId)?.state, "failed", "the delivery must actually be terminal before the assertions below mean anything");

  await waitFor(state, "the terminal answer for a start whose delivery can no longer reclaim it",
    () => bodies(state, "error").includes(LINEAR_ACTIVITY_COPY.startNeverReachedRouter));

  // The terminal inbox delivery allows the sweep's decided-absent answer.
  const settled = state.channel.runChain.initialStartReservation({ conversationId: AGENT_SESSION_ID });
  assert.equal(settled?.status, "rejected", "the sweep must have settled the stranded reservation");
  assert.equal(settled?.safeCode, "axis.channel.confirmed_absent", "and settled it on the confirmed-absent arm specifically");

  // Additional observe ticks must not duplicate the terminal Activity.
  await settle();
  assert.equal(bodyCount(state, LINEAR_ACTIVITY_COPY.startNeverReachedRouter), 1, "one terminal answer, not one per observe tick");
  assert.equal(state.executor.bindings.length, 0, "and still nothing ran: Router really never held this command");
});

/**
 * Redelivery of an interpreted reply must preserve its original question binding even
 * when the agent has opened a re-ask. Require no error and an open, answerable re-ask
 * so replay cannot bind the original Activity to the newer decision.
 */
test("a redelivery arriving after the agent re-asked does not bind to the new question", async (context) => {
  const state = await harness(context);
  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  const asked = elicitation();
  state.executor.pushWaiting(binding(state, 0), asked);
  await waitFor(state, "the elicitation to reach the user", () => countOf(state, "elicitation") === 1);

  state.provider.activities.push(promptActivity("activity-human-prompt", "Use whichever one finishes tonight."));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);
  await waitFor(state, "the free text to reach the Worker", () => state.executor.bindings.length === 2);
  await waitFor(state, "the first decision to settle", () => state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID }) === null);

  // The agent cannot resolve the prose and opens a second decision on the successor
  // Run.
  const reAsk = reAskElicitation();
  state.executor.pushWaiting(binding(state, 1), reAsk);
  await waitFor(state, "the re-ask to reach the user", () => countOf(state, "elicitation") === 2);
  assert.equal(state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID })?.status, "waiting");

  // Now Linear redelivers the ORIGINAL prompt. It must not be read as an
  // answer to the question the agent just asked.
  const before = state.executor.instructions.length;
  assert.equal((await state.post(currentFixture("agent-session-prompted.json"))).status, 200);
  await settle();

  assert.equal(countOf(state, "error"), 0, "an ordinary redelivery must not be reported to the user as a failure");
  assert.equal(state.executor.instructions.length - before, 0, "and must not dispatch the old body a second time");
  assert.equal(state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID })?.status, "waiting",
    "while the re-ask stays open and answerable -- D1.3's user-visible invariant");
  assert.equal(state.channel.ledgers.decisionOutcome(reAsk.decisionId), null,
    "and nothing may settle the question the agent just asked");
  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), [], "the redelivery is absorbed, not thrown");
});

/**
 * loadSessionActivities requests a bounded page and accepts it only when
 * pageInfo.hasNextPage proves completeness. Drive the real provider through HTTP and
 * record its query so an error before the request cannot masquerade as pagination
 * validation.
 */
test("loadSessionActivities requests one bounded page and refuses a page it cannot prove complete", async (context) => {
  const thought = { id: "activity_1", updatedAt: "2026-09-22T00:00:00.000Z", user: null, content: { __typename: "AgentActivityThoughtContent", body: "thinking" } };
  const response = { id: "activity_2", updatedAt: "2026-09-22T00:00:01.000Z", user: null, content: { __typename: "AgentActivityResponseContent", body: "done" } };

  const cases: readonly {
    readonly what: string;
    readonly body: string;
    readonly outcome:
      | { readonly kind: "resolved"; readonly activities: readonly { readonly id: string; readonly type: string }[] }
      | { readonly kind: "rejected"; readonly message: string };
  }[] = [
    {
      what: "a complete page with two Activities",
      body: JSON.stringify({ data: { agentSession: { activities: { edges: [{ node: thought }, { node: response }], pageInfo: { hasNextPage: false } } } } }),
      outcome: { kind: "resolved", activities: [{ id: "activity_1", type: "thought" }, { id: "activity_2", type: "response" }] },
    },
    {
      what: "Linear reports more pages exist",
      body: JSON.stringify({ data: { agentSession: { activities: { edges: [{ node: thought }], pageInfo: { hasNextPage: true } } } } }),
      outcome: { kind: "rejected", message: "linear.provider.session_activities_incomplete" },
    },
    {
      what: "the session itself is not there",
      body: JSON.stringify({ data: { agentSession: null } }),
      outcome: { kind: "rejected", message: "linear.provider.session_unavailable" },
    },
    {
      what: "an empty connection that proves it is complete",
      body: JSON.stringify({ data: { agentSession: { activities: { edges: [], pageInfo: { hasNextPage: false } } } } }),
      outcome: { kind: "resolved", activities: [] },
    },
  ];

  for (const scenario of cases) {
    const operations: string[] = [];
    const server = createServer((request, response_) => { void (async () => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      const sent = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { readonly query: string };
      operations.push(sent.query);
      response_.writeHead(200, { "content-type": "application/json" });
      response_.end(scenario.body);
    })().catch(() => { response_.destroy(); }); });
    await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("the GraphQL stand-in did not bind");
    context.after(async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
    });

    const provider = new LinearGraphQlProvider({ endpoint: `http://127.0.0.1:${address.port}/graphql`, timeoutMs: 4_000 });
    const outcome = await provider.loadSessionActivities("lin_oauth_test", AGENT_SESSION_ID).then(
      (activities) => ({ kind: "resolved" as const, activities }),
      (error: unknown) => ({ kind: "rejected" as const, error }),
    );

    // The read really did reach the provider, so a case that failed before the
    // HTTP call -- which would prove nothing about the guard -- is visible.
    assert.ok(operations.length >= 1, `${scenario.what}: the provider must have actually called Linear`);
    const query = operations[0] ?? "";
    assert.ok(query.includes("activities(first: 250)"), `${scenario.what}: the query must bound the page to 250 -- a page 51+ would still silently truncate without this`);
    assert.ok(query.includes("pageInfo { hasNextPage }"), `${scenario.what}: the query must ask for the completeness signal`);

    if (scenario.outcome.kind === "resolved") {
      assert.equal(outcome.kind, "resolved", `${scenario.what}: expected a resolved list, got ${outcome.kind === "rejected" ? String(outcome.error) : "?"}`);
      if (outcome.kind === "resolved") {
        assert.deepEqual(
          outcome.activities.map((activity) => ({ id: activity.id, type: activity.content.type })),
          scenario.outcome.activities,
          `${scenario.what}: the mapped Activities must match the stand-in's data`,
        );
      }
    } else {
      assert.equal(outcome.kind, "rejected", `${scenario.what}: expected a rejection, got a resolved list`);
      if (outcome.kind === "rejected") {
        assert.ok(outcome.error instanceof Error, `${scenario.what}: the rejection must be an Error`);
        assert.equal((outcome.error as Error).message, scenario.outcome.message, `${scenario.what}: wrong refusal code`);
      }
    }
  }
});

/**
 * The workspace's `WORKER` label group holds `CODEX` and `CLAUDE`. Linear
 * answers a group child's `name` without its group, so the provider must ask
 * for `parent { name }` and qualify the label itself. Driven
 * through the REAL provider against a `node:http` stand-in, and the result is
 * fed to the real `workerSelectionFromLabels`, so the case covers the step from
 * Linear's wire shape to the executor label the Node receives.
 */
test("loadIssueExecutionContext qualifies a grouped label, so WORKER/CODEX selects codex", async (context) => {
  const issue = (nodes: readonly unknown[]) => JSON.stringify({ data: { issue: {
    id: "issue_1", labels: { nodes, pageInfo: { hasNextPage: false } }, state: { type: "unstarted" }, delegate: null, team: { id: "team_1", states: { nodes: [] } },
  } } });
  const cases: readonly { readonly what: string; readonly body: string; readonly labels: readonly string[] | null }[] = [
    {
      what: "a WORKER group child beside an ungrouped label",
      body: issue([{ name: "build", parent: null }, { name: "CODEX", parent: { name: "WORKER" } }]),
      labels: ["build", "WORKER:CODEX"],
    },
    { what: "a group without a name", body: issue([{ name: "CODEX", parent: { name: " " } }]), labels: null },
    { what: "a group that is not an object", body: issue([{ name: "CODEX", parent: "WORKER" }]), labels: null },
    { what: "a label node with no parent key at all", body: issue([{ name: "CODEX" }]), labels: null },
  ];
  for (const scenario of cases) {
    const operations: string[] = [];
    const server = createServer((request, response) => { void (async () => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      operations.push((JSON.parse(Buffer.concat(chunks).toString("utf8")) as { readonly query: string }).query);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(scenario.body);
    })().catch(() => { response.destroy(); }); });
    await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("the GraphQL stand-in did not bind");
    context.after(async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
    });
    const provider = new LinearGraphQlProvider({ endpoint: `http://127.0.0.1:${address.port}/graphql`, timeoutMs: 4_000 });
    const outcome = await provider.loadIssueExecutionContext("lin_oauth_test", "issue_1").then(
      (loaded) => ({ kind: "resolved" as const, loaded }),
      (error: unknown) => ({ kind: "rejected" as const, error }),
    );
    assert.equal(operations.length, 1, `${scenario.what}: the provider must have actually called Linear`);
    assert.ok(operations[0]!.includes("nodes { name parent { name } }"), `${scenario.what}: the query must ask for each label's group`);
    if (scenario.labels === null) {
      assert.equal(outcome.kind, "rejected", `${scenario.what}: expected a refusal`);
      if (outcome.kind === "rejected") assert.equal((outcome.error as Error).message, "linear.provider.malformed_issue_context", scenario.what);
      continue;
    }
    assert.equal(outcome.kind, "resolved", `${scenario.what}: ${outcome.kind === "rejected" ? String(outcome.error) : ""}`);
    if (outcome.kind !== "resolved") continue;
    assert.deepEqual(outcome.loaded.labels, scenario.labels, scenario.what);
    const selection = workerSelectionFromLabels({ status: "available", labels: outcome.loaded.labels }, { system: "linear", tenantId: "organization_1", scopeId: "team_1" });
    assert.deepEqual(selection.executorLabels, ["codex"], `${scenario.what}: the qualified label must select codex`);
  }
});

// Per-session FIFO prevents a prompt from passing a retrying created start. A stop may
// preempt queued instructions only by visibly superseding them. Use transient provider
// failures to hold the predecessor before dispatch, keeping these ordering scenarios
// focused on two commands.

test("a same-session prompt is never claimed while an earlier created start's delivery for that session is still processing", async (context) => {
  const state = await harness(context);
  // Use a long lease so the blocking assertion depends on claim's ordering guard rather
  // than racing expiration. End the predecessor directly below instead of waiting out
  // the lease.
  state.holdInboxLeaseFor(600_000);
  await state.restart();

  // Exhaust bounded creator-read attempts and leave the created delivery processing
  // under its lease. It has neither dispatched nor received a definite refusal.
  state.provider.exactUserFailures = 3;
  const startDeliveryId = randomUUID();
  assert.equal((await state.post(currentFixture("agent-session-created.json"), { deliveryId: startDeliveryId })).status, 200);

  // Ensure the two deliveries have distinct accepted_at milliseconds. The store breaks
  // same-millisecond ties by random delivery_id, so back-to-back local requests alone
  // do not establish predecessor order.
  await new Promise((resolve) => setTimeout(resolve, 5));
  // The prompt, same session, arrives while the start is still retrying.
  state.provider.activities.push(promptActivity("activity-order-b", "Second instruction."));
  const promptDeliveryId = randomUUID();
  assert.equal((await state.post(
    currentFixture("agent-session-prompted.json", { agentActivity: { id: "activity-order-b", user: { id: "human-user-fixture" }, body: "Second instruction.", signal: "continue", createdAt: new Date().toISOString() } }),
    { deliveryId: promptDeliveryId },
  )).status, 200);

  // Waited on directly -- never guessed with a sleep -- so this half is
  // immune to how loaded the machine happens to be: the start's own claim
  // runs, exhausts its three injected failures, and the tick leaves the
  // delivery `processing` under its (600s) lease, per `retryableDelivery`.
  await waitFor(state, "the start to be claimed and left processing under its lease", () => state.channel.inbox.snapshot(startDeliveryId)?.state === "processing");
  assert.equal(state.provider.exactUserFailures, 0, "the injected failures must have been consumed by a real retry, not skipped");

  // A real settle window before reading the prompt's own attempt count: the
  // guard this test exists to prove is what keeps the ingress loop's own
  // later ticks from ever touching the prompt while the start is stuck, so
  // those later ticks have to actually get a chance to run first -- reading
  // `attempt` the instant the start's own `processing` state is observed
  // would let this assertion pass even with the guard deleted, simply
  // because no later tick had fired yet.
  await settle();

  // While the created delivery remains processing, the queued same-session prompt must
  // not be claimed. The long lease keeps expiration from explaining the result.
  assert.equal(state.channel.inbox.snapshot(promptDeliveryId)?.attempt, 0,
    "claim's per-session FIFO must keep the prompt unclaimed while the created start's delivery is still processing");
  assert.equal(state.executor.bindings.length, 0, "nothing may have reached the Worker yet");

  // End the predecessor directly through the real inbox. This scenario tests claim
  // ordering, which depends on the predecessor no longer being accepted or processing
  // rather than on how retry completion occurred.
  const startSnapshot = state.channel.inbox.snapshot(startDeliveryId);
  if (startSnapshot === null || startSnapshot.leaseOwner === null) throw new Error("the start's delivery must still be leased at this point");
  state.channel.inbox.complete(startDeliveryId, startSnapshot.leaseOwner);
  assert.equal(state.channel.inbox.snapshot(startDeliveryId)?.state, "completed");

  // Once the predecessor is terminal, the prompt may be claimed. No session was bound
  // because the start never dispatched, so inspect the inbox attempt and terminal state
  // rather than expecting an Activity whose delivery authority needs that session.
  await waitFor(state, "the prompt to be claimed only after the start's delivery has terminated", () => (state.channel.inbox.snapshot(promptDeliveryId)?.attempt ?? 0) >= 1);
  await waitFor(state, "the prompt's own claim to reach a terminal state", () => state.channel.inbox.snapshot(promptDeliveryId)?.state !== "processing");
  assert.equal(state.channel.inbox.snapshot(promptDeliveryId)?.state, "completed", "sessionMissing completes the delivery, whether or not its Activity was deliverable");
  assert.equal(state.executor.bindings.length, 0, "nothing may ever have reached the Worker in this test");
});

test("one prompt exhausting its retry budget does not drag its same-session successor down with it", async (context) => {
  const state = await harness(context);
  state.holdInboxLeaseFor(30);
  await state.restart();

  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);
  // Settle the start's own turn first -- otherwise `reserve` finds the Run
  // still active (not `waiting_for_input`) and dispatches the eventual
  // continue below as an interrupt request on that same live turn rather
  // than as a fresh, accepted one, which never opens a new binding on this
  // scripted executor without a separate interrupt handshake this test does
  // not drive. Matches the existing "a prompted webhook continues the SAME
  // conversation" test's own shape.
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "e2e-exhaust-start", projectionStatus: "projected", body: "Start done.", truncated: false, truncations: [] });
  await waitFor(state, "the start's own response to settle the Run", () => countOf(state, "response") === 1);

  // Keep prompt A's exact dispatch-Issue read retryable until its ten-attempt inbox
  // budget ends it. Fail before reserve so A does not open a Worker turn; the successor
  // must receive its own independent budget.
  state.provider.exactIssueFailures = 999;
  state.provider.activities.push(promptActivity("activity-exhaust-a", "Give up on this one."));
  assert.equal((await state.post(
    currentFixture("agent-session-prompted.json", { agentActivity: directPromptActivity("activity-exhaust-a", "Give up on this one.") }),
  )).status, 200);

  // A real gap, not just the next microtask -- see the ordering test above's
  // own note on why: `claim`'s outrank guard tie-breaks two same-session
  // rows accepted in the same millisecond by `delivery_id`, a random UUID
  // unrelated to arrival order, and two fast local POSTs can land in that
  // same millisecond often enough to matter.
  await new Promise((resolve) => setTimeout(resolve, 5));
  // Prompt B, same session, arrives while A is still retrying.
  state.provider.activities.push(promptActivity("activity-exhaust-b", "Do this instead."));
  assert.equal((await state.post(
    currentFixture("agent-session-prompted.json", { agentActivity: directPromptActivity("activity-exhaust-b", "Do this instead.") }),
  )).status, 200);

  // Today's shape: exactly one give-up message for A, and B still dispatches
  // and succeeds once A is out of the way -- the exhaustion must not connect
  // to B at all, neither dropping it nor exhausting it too.
  const exhausted = LINEAR_ACTIVITY_COPY.attemptsExhausted.replace("{attempts}", "10");
  await waitFor(state, "prompt A to exhaust its retry budget", () => bodies(state, "error").includes(exhausted), 30_000);
  assert.equal(bodyCount(state, exhausted), 1, "exactly one give-up message, never two");
  // The fault injection is per-PROVIDER, not per-delivery: B is about to
  // become claimable now that A is terminal, and it must not inherit A's
  // still-huge failure count and exhaust itself the same way. A stray B
  // attempt claimed in the instant before this line runs just retries again
  // once this takes effect -- B's own 10-attempt budget is far from spent by
  // one early miss.
  state.provider.exactIssueFailures = 0;

  // B is the conversation's ONLY-EVER continue (A never reserved one), so
  // this is the same two-dispatch shape the ordering test above relies on.
  await waitFor(state, "prompt B to dispatch once prompt A's delivery has terminated", () => state.executor.bindings.length === 2, 30_000);
  assert.equal(state.executor.instructions.at(-1)?.includes("Do this instead."), true);
  state.executor.pushSucceeded(binding(state, 1), { sourceEventId: "e2e-exhaust-final", projectionStatus: "projected", body: "Done.", truncated: false, truncations: [] });
  await waitFor(state, "prompt B's response to reach Linear", () => countOf(state, "response") === 2);
  assert.equal(bodies(state, "response").at(-1), "Done.", "B's own response, the second one posted (the start's own settle response is the first)");
  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), [], "neither the exhaustion nor B's own dispatch may have thrown");
});

/** One `agentActivity` for a direct (non-comment) prompt, matching the shape `agent-session-prompted.json` ships with. */
function directPromptActivity(activityId: string, body: string): Record<string, unknown> {
  return { id: activityId, user: { id: "human-user-fixture" }, body, signal: "continue", createdAt: new Date().toISOString() };
}

test("a stop visibly supersedes a same-session instruction that is still queued, and the Worker never sees it", async (context) => {
  const state = await harness(context);

  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  // Both the queued prompt and the stop that supersedes it are inserted
  // directly through the real inbox, back to back, rather than through
  // `post()`. The real ingress loop keeps ticking on its OWN 5ms timer the
  // entire time this test runs -- independently of this test's own
  // synchronous code -- so posting the queued prompt through the HTTP layer
  // and only THEN posting the stop would leave a real window in which that
  // timer could claim and dispatch the queued prompt before the stop's own
  // row even exists to outrank it. Constructing both rows through one
  // synchronous pair of `accept()` calls closes that window: whichever tick
  // claims this session next, it already sees both rows and picks the stop
  // (`preempts` beats `accepted_at` unconditionally). Real ciphertext, a
  // real fixture body: if the race were ever lost and this delivery were
  // claimed and processed anyway, it would be dispatched as an ordinary
  // prompt rather than throw on unparseable content, so the two apparent
  // failure modes stay recognisably different.
  // The two ids are deliberately ordered `queued < stop` lexicographically
  // (a fixed low/high prefix, not the natural randomness of `randomUUID()`):
  // `claim`'s outer `ORDER BY accepted_at ASC, delivery_id ASC` falls back to
  // this comparison whenever both rows land in the same millisecond, which
  // two `accept()` calls with no `await` between them routinely do. Picking
  // the order at random would make roughly half of all runs pass even with
  // the outrank guard deleted -- session ordering would be "supersede the
  // sibling with the lexicographically later id", which is not the claim
  // this test makes. Fixing `queued` as the lexicographically SMALLER id
  // means the base ordering (no guard) would always favour it over the
  // stop, so only `preempts` -- exercised through the real guard -- can make
  // the stop win instead.
  const queuedDeliveryId = `00000000-0000-4000-8000-${randomUUID().slice(24)}`;
  const queuedPayload = currentFixture("agent-session-prompted.json", { agentActivity: directPromptActivity("activity-superseded", "This must never reach the Worker.") });
  state.provider.activities.push(promptActivity("activity-superseded", "This must never reach the Worker."));
  state.channel.inbox.accept({
    deliveryId: queuedDeliveryId,
    webhookId: String(queuedPayload.webhookId),
    eventName: "AgentSessionEvent",
    bodyDigest: sha256(Buffer.from(JSON.stringify(queuedPayload))),
    semanticDigest: sha256(Buffer.from(JSON.stringify(queuedPayload))),
    organizationId: ORGANIZATION_ID,
    payloadCiphertext: state.channel.ingress.cipher.encrypt(JSON.stringify(queuedPayload)),
    unsupportedShape: null,
    sessionKey: AGENT_SESSION_ID,
    preempts: false,
  });
  // Fixed stop and later-prompt suffixes guarantee stopDeliveryId < p2DeliveryId even
  // within one accepted_at millisecond. This makes P2 unambiguously later under
  // arrivedBefore's tie-break.
  const stopDeliveryId = "ffffffff-ffff-4fff-bfff-aaaaaaaaaaaa";
  const stopPayload = currentFixture("agent-session-stop.json");
  state.channel.inbox.accept({
    deliveryId: stopDeliveryId,
    webhookId: String(stopPayload.webhookId),
    eventName: "AgentSessionEvent",
    bodyDigest: sha256(Buffer.from(JSON.stringify(stopPayload))),
    semanticDigest: sha256(Buffer.from(JSON.stringify(stopPayload))),
    organizationId: ORGANIZATION_ID,
    payloadCiphertext: state.channel.ingress.cipher.encrypt(JSON.stringify(stopPayload)),
    unsupportedShape: null,
    sessionKey: AGENT_SESSION_ID,
    preempts: true,
  });
  // Insert P2 synchronously with the stop so both exist before the scheduled tick
  // chooses queuedAheadForSession. Inserting it after stop processing would not
  // exercise that selection boundary.
  const p2DeliveryId = "ffffffff-ffff-4fff-bfff-ffffffffffff";
  const p2Payload = currentFixture("agent-session-prompted.json", { agentActivity: directPromptActivity("activity-after-stop", "This must not be cancelled by the stop.") });
  state.provider.activities.push(promptActivity("activity-after-stop", "This must not be cancelled by the stop."));
  state.channel.inbox.accept({
    deliveryId: p2DeliveryId,
    webhookId: String(p2Payload.webhookId),
    eventName: "AgentSessionEvent",
    bodyDigest: sha256(Buffer.from(JSON.stringify(p2Payload))),
    semanticDigest: sha256(Buffer.from(JSON.stringify(p2Payload))),
    organizationId: ORGANIZATION_ID,
    payloadCiphertext: state.channel.ingress.cipher.encrypt(JSON.stringify(p2Payload)),
    unsupportedShape: null,
    sessionKey: AGENT_SESSION_ID,
    preempts: false,
  });

  await waitFor(state, "the stop's own outcome and the superseded instruction's cancellation to both reach Linear",
    () => bodies(state, "thought").includes(LINEAR_CHANNEL_COPY.stopRequested) && bodies(state, "error").includes(LINEAR_CHANNEL_COPY.supersededByStop));

  assert.equal(state.executor.bindings.length, 1, "the Worker must never have been asked to run the superseded instruction");
  assert.equal(state.executor.wasCancelRequested(binding(state, 0)), true, "the live run must still have been asked to stop");
  const supersededSnapshot = state.channel.inbox.snapshot(queuedDeliveryId);
  assert.equal(supersededSnapshot?.state, "quarantined", "the superseded delivery must reach a terminal state");
  assert.equal(supersededSnapshot?.failureCode, "axis.channel.superseded_by_stop");

  // P2 arrived after the stop, so queuedAheadForSession must not select it for
  // supersededByStop.
  const p2SupersededSourceEventId = `axis.session.superseded:${p2DeliveryId}`;
  assert.equal(
    state.provider.created.some((record) => record.sourceEventId === p2SupersededSourceEventId),
    false,
    "P2 arrived after the stop and must never receive its own supersededByStop notice",
  );
  // ...nor the same terminal quarantine the earlier, truly-superseded delivery got.
  const p2Snapshot = state.channel.inbox.snapshot(p2DeliveryId);
  assert.equal(
    p2Snapshot?.state === "quarantined" && p2Snapshot?.failureCode === "axis.channel.superseded_by_stop",
    false,
    "P2 must not be quarantined as superseded-by-stop; what eventually happens to it is out of scope here",
  );
});

test("a stop whose reservation is refused because an earlier command still holds the slot gets its own copy, not silence", async (context) => {
  const state = await harness(context);

  // Crash after committing the start's reservation but before Router admission. Its
  // unsettled pendingCommandId holds the conversation slot for the subsequent stop
  // refusal.
  state.crashNextDispatchBeforeRouterSeesIt();
  assert.equal((await state.post(currentFixture("agent-session-created.json"))).status, 200);
  await waitFor(state, "the relay to die inside the start dispatch", () => state.crashedDispatches() === 1);
  assert.equal(state.executor.bindings.length, 0, "nothing may have run: Router never received this command");

  // The default stale window (five minutes) is left in place: this test
  // means to exercise the REFUSAL, not the sweep that would eventually
  // release the slot.
  await state.restart();
  const held = state.channel.runChain.initialStartReservation({ conversationId: AGENT_SESSION_ID });
  assert.equal(held?.status, "reserved", "the crashed start's reservation must still hold the slot after the restart");

  assert.equal((await state.post(currentFixture("agent-session-stop.json"))).status, 200);

  await waitFor(state, "the stop's own refusal copy to reach Linear", () => bodies(state, "error").includes(LINEAR_CHANNEL_COPY.stopNotApplied));

  assert.equal(state.executor.bindings.length, 0, "still nothing may have run: this stop never reached a Run to interrupt");
  assert.equal(JSON.stringify(state.activities()).includes(LINEAR_CHANNEL_COPY.stopRequested), false,
    "a refused stop must never claim it asked anything to stop");
  assert.equal(JSON.stringify(state.activities()).includes("Axis stopped the active execution"), false,
    "and must never claim to have stopped anything");
});

// Follow-up on a running Run with AXIS_ROUTER_NATIVE_STEER enabled.

/** Router's retained rows for the follow-ups that reused a running Run (an interrupt or a steer). */
function nativeSteerFollowUps(state: Harness): readonly { readonly commandId: string; readonly state: string; readonly command: string; readonly status: string | null }[] {
  const database = new DatabaseSync(state.routerDatabase, { readOnly: true });
  try {
    return (database.prepare("SELECT command_id, state, intent_json, result_json FROM router_node_command_state WHERE kind = 'continue' AND node_command_id != command_id ORDER BY rowid").all() as { command_id: string; state: string; intent_json: string; result_json: string | null }[])
      .map((row) => ({ commandId: row.command_id, state: row.state, command: (JSON.parse(row.intent_json) as { nodeCommand: { command: { command: string } } }).nodeCommand.command.command, status: row.result_json === null ? null : (JSON.parse(row.result_json) as { status: string }).status }));
  } finally { database.close(); }
}
function nativeSteerRouterRowsUnsettled(state: Harness): number {
  const database = new DatabaseSync(state.routerDatabase, { readOnly: true });
  try { return (database.prepare("SELECT COUNT(*) AS n FROM router_node_command_state WHERE state NOT IN ('accepted', 'rejected')").get() as { n: number }).n; }
  finally { database.close(); }
}
async function nativeSteerPrompt(state: Harness, activityId: string, body: string): Promise<void> {
  state.provider.activities.push(promptActivity(activityId, body));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json", { agentActivity: directPromptActivity(activityId, body) }))).status, 200);
}

test("a follow-up on a running Run is steered into it: no interrupt, no new Run, one steered thought, and one final reply", async (context) => {
  const state = await harness(context, { nativeSteer: true, steer: true });
  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);

  await nativeSteerPrompt(state, "activity-native-steer-steer", "Also update the changelog.");
  await waitFor(state, "the steer to reach the running turn", () => state.executor.steers.length === 1);
  assert.equal(state.executor.steers[0]!.instruction.includes("Also update the changelog."), true);
  assert.deepEqual(state.executor.steers[0]!.binding, binding(state, 0), "the running turn itself, not a new one");
  await waitFor(state, "the steered thought", () => bodies(state, "thought").includes(LINEAR_CHANNEL_COPY.steered));
  assert.equal(state.executor.wasCancelRequested(binding(state, 0)), false, "a steer interrupts nothing");

  const [followUp, ...others] = nativeSteerFollowUps(state);
  assert.deepEqual(others, []);
  assert.deepEqual({ state: followUp?.state, command: followUp?.command, status: followUp?.status }, { state: "accepted", command: "steer", status: "steered" });
  assert.equal(state.channel.runChain.reservation({ commandId: followUp!.commandId })?.status, "steered");
  assert.equal(state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }), null, "a steer queues no redispatch");
  assert.equal(state.channel.runChain.currentRun({ conversationId: AGENT_SESSION_ID })?.runId, state.channel.runChain.reservation({ commandId: followUp!.commandId })?.runId, "and starts no Run");

  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "e2e-native-steer-final", projectionStatus: "projected", body: "Done, changelog included.", truncated: false, truncations: [] });
  await waitFor(state, "the Run's own final reply", () => countOf(state, "response") === 1);
  await settle();
  assert.deepEqual(bodies(state, "response"), ["Done, changelog included."], "exactly one final reply: the Run's own, no cancellation reply");
  assert.equal(state.executor.bindings.length, 1, "nothing was redispatched");
  assert.equal(bodyCount(state, LINEAR_CHANNEL_COPY.steered), 1);
  assert.equal(nativeSteerRouterRowsUnsettled(state), 0);
  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), []);
});

/**
 * With native steer enabled, an executor without steer support answers
 * interrupt_requested through cancel. Compare flag-off and flag-on runs to require the
 * same interrupt, redispatch and Activity order.
 */
test("native steer fallback preserves interrupt, redispatch and Activity order", async (context) => {
  async function interruptAndRedispatch(nativeSteer: boolean) {
    const state = await harness(context, { nativeSteer });
    // Dispatch receipts and Worker events reach the outbox through independent loops.
    // Release the events after the dispatch notice so both scenarios start with the
    // same observed prefix before exercising the interrupt and redispatch sequence.
    state.holdNodeEvents();
    await state.post(currentFixture("agent-session-created.json"));
    await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);
    await waitFor(state, "the start dispatch notice", () => state.activities().some(content => content.action === LINEAR_CHANNEL_COPY.startedAction));
    state.releaseNodeEvents();
    await waitFor(state, "the Worker's acceptance notice", () => state.activities().some(content => content.action === LINEAR_ACTIVITY_COPY.acceptedAction));
    await nativeSteerPrompt(state, "activity-native-steer-interrupt", "Do this instead.");
    await waitFor(state, "the interrupt to reach the running turn", () => state.executor.wasCancelRequested(binding(state, 0)));
    await waitFor(state, "the interrupting thought", () => bodies(state, "thought").includes(LINEAR_CHANNEL_COPY.interrupting));
    assert.equal(state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }) !== null, true, "the instruction is queued");
    state.executor.pushCancelled(binding(state, 0));
    await waitFor(state, "the queued instruction to be redispatched as a continue", () => state.executor.bindings.length === 2);
    assert.equal(state.executor.instructions.at(-1)?.includes("Do this instead."), true);
    state.executor.pushSucceeded(binding(state, 1), { sourceEventId: `e2e-native-steer-redispatched-${String(nativeSteer)}`, projectionStatus: "projected", body: "Did it instead.", truncated: false, truncations: [] });
    await waitFor(state, "the redispatched Run's reply", () => bodies(state, "response").includes("Did it instead."));
    await settle();
    assert.deepEqual(state.executor.steers, []);
    assert.equal(state.executor.bindings.length, 2);
    assert.deepEqual(nativeSteerFollowUps(state).map(({ command, status }) => ({ command, status })), [{ command: nativeSteer ? "steer" : "cancel", status: "interrupt_requested" }]);
    assert.equal(nativeSteerRouterRowsUnsettled(state), 0);
    return state.activities().map((content) => ({ type: content.type, body: content.body, action: content.action }));
  }
  const withoutFlag = await interruptAndRedispatch(false);
  const withFlag = await interruptAndRedispatch(true);
  assert.deepEqual(withFlag, withoutFlag, "the same Activities, in the same order, as with the flag off");
});

/**
 * Deliver a follow-up while Router still sees a running Run but the Worker has already
 * journaled success and its terminal event is held. The Worker's decided
 * interrupt_requested response queues the instruction for exactly one ordinary
 * continuation after terminal delivery.
 */
test("a steer racing the Run's end becomes one ordinary continue: nothing ambiguous, nothing stuck", async (context) => {
  const state = await harness(context, { nativeSteer: true, steer: true });
  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);
  state.executor.pushProgress(binding(state, 0));
  await waitFor(state, "the start's events to reach Router", () => bodies(state, "thought").includes("Scripted run is progressing."));

  state.holdNodeEvents();
  state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "e2e-native-steer-race-first", projectionStatus: "projected", body: "First turn done.", truncated: false, truncations: [] });
  await waitFor(state, "the Worker to have recorded the Run's end, with Router not yet told", () => state.parkedNodeEvents().length > 0);
  await nativeSteerPrompt(state, "activity-native-steer-race", "One more thing.");
  await waitFor(state, "Router to settle the follow-up", () => nativeSteerFollowUps(state).some((row) => row.status !== null));
  assert.deepEqual(nativeSteerFollowUps(state).map(({ state: rowState, command, status }) => ({ rowState, command, status })), [{ rowState: "accepted", command: "steer", status: "interrupt_requested" }],
    "the steer was answered definitely, not left ambiguous");
  assert.deepEqual(state.executor.steers, [], "the ended Run's executor was never asked");
  assert.equal(state.executor.wasCancelRequested(binding(state, 0)), false);
  await waitFor(state, "the instruction to be queued", () => state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }) !== null);

  state.releaseNodeEvents();
  await waitFor(state, "the first Run's reply", () => bodies(state, "response").includes("First turn done."));
  await waitFor(state, "the queued instruction to run as a continue", () => state.executor.bindings.length === 2);
  assert.equal(state.executor.instructions.at(-1)?.includes("One more thing."), true);
  assert.equal(binding(state, 1).threadId, binding(state, 0).threadId, "on the same conversation");
  state.executor.pushSucceeded(binding(state, 1), { sourceEventId: "e2e-native-steer-race-second", projectionStatus: "projected", body: "And the one more thing.", truncated: false, truncations: [] });
  await waitFor(state, "the continue's reply", () => countOf(state, "response") === 2);
  await settle();
  assert.deepEqual(bodies(state, "response"), ["First turn done.", "And the one more thing."]);
  assert.equal(state.executor.bindings.length, 2, "the instruction ran exactly once");
  assert.equal(state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }), null);
  assert.equal(state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.pendingCommandId ?? null, null, "the conversation's slot is free");
  assert.equal(nativeSteerRouterRowsUnsettled(state), 0, "no Router command is left awaiting a receipt");
  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), []);
});

/**
 * A follow-up with native steer disabled reaches Router while the Run is still
 * running there and its terminal event is held on the Node hop. Worker has
 * already recorded the end, so cancel answers `interrupt_requested` without
 * calling the executor. The instruction queues and runs once as an ordinary
 * continuation when the conversation is next idle.
 */
for (const ending of ["succeeded", "failed"] as const) {
  test(`a cancel racing the Run's end (${ending}) becomes one ordinary continue: nothing ambiguous, nothing stuck`, async (context) => {
    const state = await harness(context);
    await state.post(currentFixture("agent-session-created.json"));
    await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);
    state.executor.pushProgress(binding(state, 0));
    await waitFor(state, "the start's events to reach Router", () => bodies(state, "thought").includes("Scripted run is progressing."));

    state.holdNodeEvents();
    if (ending === "succeeded") state.executor.pushSucceeded(binding(state, 0), { sourceEventId: "e2e-waiting-steer-race-first", projectionStatus: "projected", body: "First turn done.", truncated: false, truncations: [] });
    else state.executor.pushFailed(binding(state, 0));
    await waitFor(state, "the Worker to have recorded the Run's end, with Router not yet told", () => state.parkedNodeEvents().length > 0);
    await nativeSteerPrompt(state, `activity-waiting-steer-race-${ending}`, "One more thing.");
    await waitFor(state, "Router to settle the follow-up", () => nativeSteerFollowUps(state).some((row) => row.status !== null));
    assert.deepEqual(nativeSteerFollowUps(state).map(({ state: rowState, command, status }) => ({ rowState, command, status })), [{ rowState: "accepted", command: "cancel", status: "interrupt_requested" }],
      "the cancel was answered definitely, not left ambiguous");
    assert.equal(state.executor.wasCancelRequested(binding(state, 0)), false, "the ended Run's executor was never asked");
    await waitFor(state, "the instruction to be queued", () => state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }) !== null);

    state.releaseNodeEvents();
    await waitFor(state, "the queued instruction to run as a continue", () => state.executor.bindings.length === 2);
    assert.equal(state.executor.instructions.at(-1)?.includes("One more thing."), true);
    assert.equal(binding(state, 1).threadId, binding(state, 0).threadId, "on the same conversation");
    state.executor.pushSucceeded(binding(state, 1), { sourceEventId: `e2e-waiting-steer-race-${ending}-second`, projectionStatus: "projected", body: "And the one more thing.", truncated: false, truncations: [] });
    await waitFor(state, "the continue's reply", () => bodies(state, "response").includes("And the one more thing."));
    await settle();
    assert.equal(state.executor.bindings.length, 2, "the instruction ran exactly once");
    assert.equal(state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }), null);
    assert.equal(state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.pendingCommandId ?? null, null, "the conversation's slot is free");
    assert.equal(nativeSteerRouterRowsUnsettled(state), 0, "no Router command is left awaiting a receipt");
    assert.deepEqual(state.tickFailures.map((failure) => failure.loop), []);
  });
}

/**
 * If a follow-up races a waiting_for_input park, interrupt_requested must not make its
 * queued instruction answer a question it predates. Settled-state redispatch
 * eligibility keeps the question open; dispatch the instruction only when the
 * conversation becomes idle.
 */
test("a cancel racing a waiting_for_input park is decided, waits through the question, and runs once after it", async (context) => {
  const state = await harness(context);
  await state.post(currentFixture("agent-session-created.json"));
  await waitFor(state, "the start to reach the Worker", () => state.executor.bindings.length === 1);
  state.executor.pushProgress(binding(state, 0));
  await waitFor(state, "the start's events to reach Router", () => bodies(state, "thought").includes("Scripted run is progressing."));

  state.holdNodeEvents();
  state.executor.pushWaiting(binding(state, 0), elicitationWithOther());
  await waitFor(state, "the Worker to have recorded the park, with Router not yet told", () => state.parkedNodeEvents().length > 0);
  await nativeSteerPrompt(state, "activity-waiting-steer-race-waiting", "One more thing.");
  await waitFor(state, "Router to settle the follow-up", () => nativeSteerFollowUps(state).some((row) => row.status !== null));
  assert.deepEqual(nativeSteerFollowUps(state).map(({ state: rowState, command, status }) => ({ rowState, command, status })), [{ rowState: "accepted", command: "cancel", status: "interrupt_requested" }],
    "the cancel was answered definitely, not left ambiguous");
  assert.equal(state.executor.wasCancelRequested(binding(state, 0)), false, "the parked Run's executor was never asked");
  await waitFor(state, "the instruction to be queued", () => state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }) !== null);

  state.releaseNodeEvents();
  await waitFor(state, "the question to reach the user", () => countOf(state, "elicitation") === 1);
  await waitFor(state, "the run chain to hold the open decision", () => state.channel.runChain.openDecision({ conversationId: AGENT_SESSION_ID })?.status === "waiting");
  await settle();
  assert.equal(state.executor.bindings.length, 1, "the queued instruction must not be redispatched into the open question");
  assert.notEqual(state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }), null, "it stays queued while the conversation waits");
  assert.equal(state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.pendingCommandId ?? null, null, "and the slot is free for the answer");

  state.provider.activities.push(promptActivity("activity-waiting-steer-answer", "Use whichever one finishes tonight."));
  assert.equal((await state.post(currentFixture("agent-session-prompted.json", { agentActivity: directPromptActivity("activity-waiting-steer-answer", "Use whichever one finishes tonight.") }))).status, 200);
  await waitFor(state, "the answer to reach the Worker as a continue", () => state.executor.bindings.length === 2);
  assert.equal(state.executor.instructions[1]?.includes("Use whichever one finishes tonight."), true, "the answer, not the queued instruction, answers the question");
  state.executor.pushSucceeded(binding(state, 1), { sourceEventId: "e2e-waiting-steer-waiting-answered", projectionStatus: "projected", body: "Answered.", truncated: false, truncations: [] });

  await waitFor(state, "the queued instruction to run once the conversation is idle", () => state.executor.bindings.length === 3);
  assert.equal(state.executor.instructions[2]?.includes("One more thing."), true);
  state.executor.pushSucceeded(binding(state, 2), { sourceEventId: "e2e-waiting-steer-waiting-queued", projectionStatus: "projected", body: "And the one more thing.", truncated: false, truncations: [] });
  await waitFor(state, "the queued instruction's reply", () => bodies(state, "response").includes("And the one more thing."));
  await settle();
  assert.equal(state.executor.bindings.length, 3, "each instruction ran exactly once");
  assert.equal(state.channel.runChain.pendingRedispatch({ conversationId: AGENT_SESSION_ID }), null);
  assert.equal(state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.pendingCommandId ?? null, null, "the conversation's slot is free");
  assert.equal(nativeSteerRouterRowsUnsettled(state), 0, "no Router command is left awaiting a receipt");
  assert.deepEqual(state.tickFailures.map((failure) => failure.loop), []);
});

test("a definite issue-sync failure on a start leaves a rejected first-start reservation and tells the user why", async (context) => {
  const state = await harness(context);
  state.provider.issueContextError = new Error("linear.provider.issue_not_found");
  const deliveryId = randomUUID();

  assert.equal((await state.post(currentFixture("agent-session-created.json"), { deliveryId })).status, 200);

  await waitFor(state, "the startup failure to reach Linear", () => countOf(state, "error") === 1);
  assert.equal(bodyOf(state, "error"), renderStartupFailure(classifyStartupFailure("axis.relay.issue_sync_not_found", "Validation")));
  assert.equal(state.channel.inbox.snapshot(deliveryId)?.state, "completed", "an admitted start's failure is answered, not failed");
  assert.equal(state.channel.runChain.initialStartReservation({ conversationId: AGENT_SESSION_ID })?.status, "rejected",
    "the first start is reserved and settled rejected, so it stays idempotent and does not hold the slot");
  assert.equal(state.channel.runChain.conversation({ conversationId: AGENT_SESSION_ID })?.pendingCommandId, null);
  assert.equal(state.executor.bindings.length, 0, "nothing was dispatched");
});
