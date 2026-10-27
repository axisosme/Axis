import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { LINEAR_INBOX_LEASE_MS } from "../../../src/relay/linear/inbox.js";
import {
  LINEAR_INBOX_LEASE_OWNER,
  LINEAR_OAUTH_SCOPES,
  composeLinearChannel,
  startLinearChannelProcess,
  type LinearChannel,
  type LinearChannelOptions,
  type LinearLoopFailure,
} from "../../../src/relay/linear/composition.js";
import { LINEAR_LOOP_NAMES, type LinearLoopName, type LinearTick, type LinearTickResult } from "../../../src/relay/linear/observe.js";
import { EnvelopeCipher, sha256 } from "../../../src/relay/security.js";
import { webhookSemanticDigest } from "../../../src/relay/linear/payload.js";
import { linearChannelDeliveryTarget } from "../../../src/router/channel-conversation-types.js";
import { channelBindingResolver } from "../../../src/router/binding-resolver.js";
import { Router } from "../../../src/router/router.js";
import { unreachableNodePort } from "../../support/node-dispatch-double.js";
import { SqliteTeamBindingStore } from "../../../src/router/team-bindings.js";
import { currentFixture, FakeLinearProvider, masterKey, MemoryVault } from "../../helpers.js";

const ORGANIZATION_ID = "organization-composition";
const APP_USER_ID = "app-user-composition";
const TEAM_ID = "team-composition";
const AGENT_SESSION_ID = "agent-session-composition";
const CREDENTIAL_REF = `linear:${ORGANIZATION_ID}:${APP_USER_ID}`;

interface Fixture {
  readonly channel: LinearChannel;
  readonly directory: string;
  readonly databases: import("../../../src/relay/linear/runtime-config.js").LinearChannelDatabasePaths;
  readonly vault: MemoryVault;
  readonly provider: FakeLinearProvider;
  readonly router: Router;
  readonly cipher: EnvelopeCipher;
  /** Recomposes over the same files, as a restart would. */
  recompose(): LinearChannel;
}

function fixture(context: { after(fn: () => unknown): void }, overrides: Partial<LinearChannelOptions & import("../../../src/relay/linear/runtime-config.js").LinearRuntimeConfig> = {}): Fixture {
  const directory = mkdtempSync(join(tmpdir(), "axis-channel-composition-composition-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));

  const databases = {
    channel: join(directory, "linear-channel.sqlite"),
    conversations: join(directory, "linear-conversations.sqlite"),
    runChain: join(directory, "linear-run-chain.sqlite"),
    delivery: join(directory, "linear-delivery.sqlite"),
  } as const;

  const teamBindings = new SqliteTeamBindingStore({});
  context.after(() => teamBindings.close());
  // Build the Router through production Node command composition. These tests inspect wiring
  // rather than dispatch, so the supplied Node transport rejects any use.
  const router = new Router({ nodeRouting: { bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings }), port: unreachableNodePort } });
  context.after(() => router.close());

  const vault = new MemoryVault();
  const provider = new FakeLinearProvider();
  const cipher = new EnvelopeCipher(masterKey());
  const opened: LinearChannel[] = [];
  context.after(() => { for (const channel of opened) { try { channel.close(); } catch { /* already closed by the test */ } } });

  const build = (): LinearChannel => {
    const channel = composeLinearChannel({
      databases,
      webhookSecret: "composition-signing-secret",
      oauth: { clientId: "composition-client", clientSecret: "composition-secret", redirectUri: "https://relay.invalid/oauth/linear/callback" },
      defaultTeamIds: [TEAM_ID],
      allowAllPublicTeams: false,
      integrityCheckIntervalMs: 60_000,
      tickIntervals: { ingress: 1_000, observe: 1_000, outbox: 1_000 },
      ...overrides,
    }, {
      cipher,
      vault,
      provider,
      router,
      workerRouting: { audit: { record() { /* this file does not dispatch */ } } },
      sleep: async () => { /* deterministic */ },
      ...overrides,
    });
    opened.push(channel);
    return channel;
  };

  return { channel: build(), directory, databases, vault, provider, router, cipher, recompose: build };
}

/** Every table name in one SQLite file, for the co-location assertions. */
function tableNames(filename: string): readonly string[] {
  const database = new DatabaseSync(filename, { readOnly: true });
  try {
    return (database.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all() as { name: string }[]).map((row) => row.name);
  } finally {
    database.close();
  }
}

// ---------------------------------------------------------------------------
// Database wiring
// ---------------------------------------------------------------------------

test("the inbox, the ledgers and the installation store all open AXIS_LINEAR_CHANNEL_DATABASE, and the three neutral stores open their own files", (context) => {
  const state = fixture(context);
  state.channel.installations.install({ organizationId: ORGANIZATION_ID, appUserId: APP_USER_ID, credentialRef: CREDENTIAL_REF, teamIds: [TEAM_ID], canAccessAllPublicTeams: false });

  const channelTables = tableNames(state.databases.channel);
  for (const expected of ["linear_inbox_deliveries", "linear_ledger_sessions", "linear_installations", "linear_oauth_states"]) {
    assert.ok(channelTables.includes(expected), `the channel database must hold ${expected}; it holds ${channelTables.join(", ")}`);
  }

  assert.ok(tableNames(state.databases.conversations).includes("channel_conversations"));
  assert.ok(tableNames(state.databases.runChain).includes("channel_run_chain_conversations"));
  assert.ok(tableNames(state.databases.delivery).includes("channel_message_outbox"));
  assert.equal(channelTables.includes("channel_message_outbox"), false, "the outbox must NOT be in the channel database");
});

test("the composed stores are durable: a recomposition over the same files reads back what the first composition wrote", (context) => {
  const state = fixture(context);
  state.channel.installations.install({ organizationId: ORGANIZATION_ID, appUserId: APP_USER_ID, credentialRef: CREDENTIAL_REF, teamIds: [TEAM_ID], canAccessAllPublicTeams: false });
  state.channel.close();

  const reopened = state.recompose();
  assert.deepEqual(reopened.installations.authority(ORGANIZATION_ID, APP_USER_ID, TEAM_ID), { credentialRef: CREDENTIAL_REF, allowed: true });
});

// ---------------------------------------------------------------------------
// The outbox message-id rule
// ---------------------------------------------------------------------------

test("the outbox preserves a rendering's preferredActivityId as the row id, and mints a fresh one only when the rendering offers none", (context) => {
  const state = fixture(context);
  const target = linearChannelDeliveryTarget({ agentSessionId: AGENT_SESSION_ID });
  const preferred = "30000000-0000-4000-8000-000000000001";

  // `docs/LINEAR-PROVIDER.md` requires delivery not to regenerate or transform
  // the Worker receipt's own Activity id. Deleting the composition's
  // `messageId` line takes `SqliteChannelOutboxStore`'s own default --
  // `randomUUID()` -- which silently renumbers every Activity Axis posts while
  // every other test stays green, because nothing else compares the row id to
  // the receipt's.
  assert.equal(state.channel.outbox.recordProjectedBatch({
    conversationId: AGENT_SESSION_ID,
    target,
    expectedCursor: 0,
    events: [{ axisSequence: 1, sourceEventId: "source-preferred", rendered: { content: { type: "response", body: "kept" }, summary: "kept", terminal: true, preferredActivityId: preferred } as never }],
  }), true);
  const claimedPreferred = state.channel.outbox.claim("composition-test");
  assert.equal(claimedPreferred?.id, preferred);

  // Without a preference the store still mints a valid Activity id rather than
  // failing, which is what `linearActivityId`'s own fallback is for.
  assert.equal(state.channel.outbox.recordProjectedBatch({
    conversationId: AGENT_SESSION_ID,
    target,
    expectedCursor: 1,
    events: [{ axisSequence: 2, sourceEventId: "source-unpreferred", rendered: { content: { type: "response", body: "minted" }, summary: "minted", terminal: true } }],
  }), true);
  const claimedMinted = state.channel.outbox.claim("composition-test");
  assert.ok(claimedMinted !== null && claimedMinted.id !== preferred);
  assert.match(claimedMinted.id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
});

// ---------------------------------------------------------------------------
// The feedback-loop lookup
// ---------------------------------------------------------------------------

test("the deliverer's delivered-Activity lookup is wired to the outbox and matches on BOTH the row id and a diverged provider id", (context) => {
  const state = fixture(context);
  const target = linearChannelDeliveryTarget({ agentSessionId: AGENT_SESSION_ID });

  const own = state.channel.outbox.recordDirectMessage({ conversationId: AGENT_SESSION_ID, target, sourceEventId: "source-own", content: { type: "response", body: "axis said this" } });
  const diverged = state.channel.outbox.recordDirectMessage({ conversationId: AGENT_SESSION_ID, target, sourceEventId: "source-diverged", content: { type: "response", body: "axis said this too" } });

  // Recognize a process-minted Activity ID as Axis-owned in every outbox state. In
  // particular, an ambiguous row can already name an Activity visible in Linear.
  assert.equal(state.channel.deliverer.isAxisActivity(own.outboxId), true);

  const claimedFirst = state.channel.outbox.claim("composition-test");
  const claimedSecond = state.channel.outbox.claim("composition-test");
  assert.ok(claimedFirst !== null && claimedSecond !== null);
  // One row confirmed under its own id, the other under a DIFFERENT provider
  // id -- the reachable case, since `LinearGraphQlProvider#createAgentActivity`
  // sends `input.id = record.id` but confirms on whatever
  // `agentActivityCreate.agentActivity.id` Linear returns.
  const divergedProviderId = "30000000-0000-4000-8000-000000000002";
  state.channel.outbox.recordDisposition(own.outboxId, { disposition: "confirmed", providerMessageId: own.outboxId });
  state.channel.outbox.recordDisposition(diverged.outboxId, { disposition: "confirmed", providerMessageId: divergedProviderId });

  assert.equal(state.channel.deliverer.isAxisActivity(own.outboxId), true, "a delivered row's own id must be recognised as Axis's own Activity");
  assert.equal(state.channel.deliverer.isAxisActivity(divergedProviderId), true, "the provider id a delivered row was confirmed under must be recognised too -- dropping this half is how Axis starts answering itself");
  assert.equal(state.channel.deliverer.isAxisActivity(diverged.outboxId), true, "the row id half must keep working for a row whose provider id diverged");
  assert.equal(state.channel.deliverer.isAxisActivity("f0000000-0000-4000-8000-000000000000"), false);
});

// ---------------------------------------------------------------------------
// OAuth wiring
// ---------------------------------------------------------------------------

test("the OAuth manager is retargeted at the installation store: its PKCE state lands there and nowhere else", (context) => {
  const state = fixture(context);
  const authorizeUrl = new URL(state.channel.oauth.begin("/after"));

  // The state digest is not observable from outside, so the wiring is proved
  // the way it is actually used: the SAME store answers the consume. A manager
  // pointed at a different (or no) store would have nothing to consume.
  assert.equal(tableNames(state.databases.channel).includes("linear_oauth_states"), true);
  const rows = (() => {
    const database = new DatabaseSync(state.databases.channel, { readOnly: true });
    try { return (database.prepare("SELECT return_to FROM linear_oauth_states").all() as { return_to: string }[]).map((row) => row.return_to); }
    finally { database.close(); }
  })();
  assert.deepEqual(rows, ["/after"], "begin() must have written its one-time state into the installation store this composition built");

  // The scopes table is a grant, so it is pinned verbatim rather than
  // symbolically: a silent scope change is approved by a consent screen nobody
  // re-reads.
  assert.deepEqual([...LINEAR_OAUTH_SCOPES], ["read", "write", "app:assignable", "app:mentionable"]);
  assert.equal(authorizeUrl.searchParams.get("scope"), "read write app:assignable app:mentionable");
  assert.equal(authorizeUrl.searchParams.get("client_id"), "composition-client");
  assert.equal(authorizeUrl.searchParams.get("redirect_uri"), "https://relay.invalid/oauth/linear/callback");
});

test("a completed OAuth callback installs with the configured Team allowlist and public-Team switch, keyed by the linear: vault reference", async (context) => {
  const tokenEndpoint = "https://token.invalid/oauth/token";
  const requests: string[] = [];
  const state = fixture(context, {
    defaultTeamIds: ["team-one", "team-two"],
    allowAllPublicTeams: true,
    oauthTransport: {
      tokenEndpoint,
      fetch: (async (input: unknown) => {
        requests.push(String(input));
        return new Response(JSON.stringify({ access_token: "composition-access-token", scope: "read write" }), { status: 200, headers: { "content-type": "application/json" } });
      }) as unknown as typeof fetch,
    },
  });

  // `begin` mints the state this callback must consume; going through both is
  // what proves the SAME store backs the pair.
  const authorizeUrl = new URL(state.channel.oauth.begin("/done"));
  const returnTo = await state.channel.oauth.callback("authorization-code", authorizeUrl.searchParams.get("state")!);
  assert.equal(returnTo, "/done");
  assert.deepEqual(requests, [tokenEndpoint], "the transport override must be the one actually used, or this test proves nothing about the wiring");

  assert.deepEqual([...state.vault.values.keys()], ["linear:organization-fixture:app-user-fixture"]);

  // The three wiring lines under test: the Team allowlist and the public-Team
  // switch reached the installation, and the installation reached the store
  // this composition built.
  assert.deepEqual(state.channel.installations.authority("organization-fixture", "app-user-fixture", "team-one"), { credentialRef: "linear:organization-fixture:app-user-fixture", allowed: true });
  assert.deepEqual(state.channel.installations.authority("organization-fixture", "app-user-fixture", "team-two"), { credentialRef: "linear:organization-fixture:app-user-fixture", allowed: true });
  // Allowed because the public-Team switch is on, not because the Team is
  // listed -- so this case fails if `allowAllPublicTeams` stops being wired.
  assert.deepEqual(state.channel.installations.authority("organization-fixture", "app-user-fixture", "team-unlisted"), { credentialRef: "linear:organization-fixture:app-user-fixture", allowed: true });
});

test("with the public-Team switch off, only the configured Teams are granted -- the negative half of the same wiring line", async (context) => {
  const state = fixture(context, {
    defaultTeamIds: ["team-one"],
    allowAllPublicTeams: false,
    oauthTransport: {
      tokenEndpoint: "https://token.invalid/oauth/token",
      fetch: (async () => new Response(JSON.stringify({ access_token: "t" }), { status: 200, headers: { "content-type": "application/json" } })) as unknown as typeof fetch,
    },
  });
  const authorizeUrl = new URL(state.channel.oauth.begin("/"));
  await state.channel.oauth.callback("code", authorizeUrl.searchParams.get("state")!);

  assert.equal(state.channel.installations.authority("organization-fixture", "app-user-fixture", "team-one")?.allowed, true);
  assert.equal(state.channel.installations.authority("organization-fixture", "app-user-fixture", "team-unlisted")?.allowed, false);
});

// ---------------------------------------------------------------------------
// Readiness and teardown
// ---------------------------------------------------------------------------

test("readiness is asked of the vault and of every store this composition opened, and is re-read rather than captured", (context) => {
  const state = fixture(context);
  assert.equal(state.channel.ready, true);

  // A getter, not a snapshot: the vault going unreadable flips readiness on
  // the SAME channel object, which is what `/health/ready` depends on.
  state.vault.ready = false;
  assert.equal(state.channel.ready, false);
  state.vault.ready = true;
  assert.equal(state.channel.ready, true);

  // Each store is individually consulted: closing any one of them must flip
  // readiness, or a corrupt store would be reported healthy.
  for (const store of ["inbox", "ledgers", "conversations", "runChain", "outbox"] as const) {
    const isolated = fixture(context);
    isolated.channel[store].close();
    assert.equal(isolated.channel.ready, false, `readiness must consult ${store}`);
  }
});

/**
 * A failed assembly must close every store opened so far. WAL and SHM sidecars
 * disappear only when the last connection closes, so their absence establishes cleanup.
 * Reopening the database alone would also succeed with leaked handles.
 */
test("a store that fails to open releases the handles of the ones already opened", (context) => {
  const state = fixture(context);
  // Its OWN directory: the fixture's channel is still open on the fixture's
  // files, so their WAL sidecars exist no matter what this test proves.
  const directory = mkdtempSync(join(tmpdir(), "axis-channel-composition-rollback-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const databases = {
    channel: join(directory, "linear-channel.sqlite"),
    conversations: join(directory, "linear-conversations.sqlite"),
    runChain: join(directory, "linear-run-chain.sqlite"),
    delivery: join(directory, "linear-delivery.sqlite"),
  } as const;

  // A real, reachable failure: the run-chain path names a DIRECTORY, so its
  // constructor throws after the three channel-database stores and the
  // conversation store are already open. The caller never receives a
  // `LinearChannel`, so nothing else could ever close them.
  assert.throws(() => composeLinearChannel({
      databases: { ...databases, runChain: directory },
      webhookSecret: "composition-signing-secret",
      oauth: { clientId: "c", clientSecret: "s", redirectUri: "https://relay.invalid/cb" },
      defaultTeamIds: [],
      allowAllPublicTeams: false,
      integrityCheckIntervalMs: 60_000,
      tickIntervals: { ingress: 1_000, observe: 1_000, outbox: 1_000 },
    }, {
      cipher: new EnvelopeCipher(masterKey()),
      vault: new MemoryVault(),
      provider: new FakeLinearProvider(),
      router: state.router,
      workerRouting: { audit: { record() { /* this file does not dispatch */ } } },
      sleep: async () => { /* deterministic */ },
    }));

  assert.equal(existsSync(databases.channel), true, "the failing assembly must really have created the channel database, or the sidecar assertions below are vacuous");
  assert.equal(existsSync(`${databases.channel}-wal`), false, "a leaked connection keeps the WAL sidecar alive; its absence is the proof all three channel-database handles were closed");
  assert.equal(existsSync(`${databases.channel}-shm`), false);
  assert.equal(existsSync(`${databases.conversations}-wal`), false);

  // And the files are still usable afterwards -- the rollback closes handles,
  // it does not damage what was written.
  const reopened = composeLinearChannel({
      databases,
      webhookSecret: "composition-signing-secret",
      oauth: { clientId: "c", clientSecret: "s", redirectUri: "https://relay.invalid/cb" },
      defaultTeamIds: [],
      allowAllPublicTeams: false,
      integrityCheckIntervalMs: 60_000,
      tickIntervals: { ingress: 1_000, observe: 1_000, outbox: 1_000 },
    }, {
      cipher: new EnvelopeCipher(masterKey()),
      vault: new MemoryVault(),
      provider: new FakeLinearProvider(),
      router: state.router,
      workerRouting: { audit: { record() { /* this file does not dispatch */ } } },
      sleep: async () => { /* deterministic */ },
    });
  try {
    assert.equal(reopened.ready, true);
  } finally {
    reopened.close();
  }
});

test("close() closes every store the composition opened, and never the Router it was handed", (context) => {
  const state = fixture(context);
  state.channel.close();

  assert.equal(state.channel.inbox.ready(), false);
  assert.equal(state.channel.ledgers.ready(), false);
  assert.equal(state.channel.conversations.ready(), false);
  assert.equal(state.channel.runChain.ready(), false);
  assert.equal(state.channel.outbox.ready(), false);
  // The installation store has no `ready()`; a closed handle refuses a read.
  assert.throws(() => state.channel.installations.authority(ORGANIZATION_ID, APP_USER_ID, TEAM_ID));

  assert.doesNotThrow(() => state.router.listTaskEvents({ organizationId: ORGANIZATION_ID, issueId: "issue-composition", teamId: TEAM_ID, agentSessionId: AGENT_SESSION_ID }));
});

// ---------------------------------------------------------------------------
// The tick table and the ingress adapter
// ---------------------------------------------------------------------------

test("the tick table is exactly the three loop names, and every tick returns a frozen LinearTickResult", async (context) => {
  const state = fixture(context);
  assert.deepEqual(Object.keys(state.channel.ticks).sort(), [...LINEAR_LOOP_NAMES].sort());
  assert.deepEqual([...LINEAR_LOOP_NAMES], ["ingress", "observe", "outbox"]);

  for (const name of LINEAR_LOOP_NAMES) {
    const result = await (state.channel.ticks[name] as LinearTick<LinearTickResult>)();
    assert.deepEqual(result.failures, [], `${name} must report no failures on an empty channel`);
    assert.equal(Object.isFrozen(result), true, `${name} must return a frozen result`);
  }
});

test("the ingress tick adapter reports the underlying disposition and an empty failures list, and lets a claim fault escape to the loop", async (context) => {
  const state = fixture(context);

  const idle = await state.channel.ticks.ingress();
  assert.deepEqual(idle, { claimed: null, outcome: { disposition: "idle" }, failures: [] });

  // Require claimed to identify a delivery actually taken by the tick. Use a claimable
  // created AgentSessionEvent: an unsupported shape is quarantined at acceptance and
  // cannot test a claim. Without an installation, authority refusal is durably retained
  // as a rejected startup.
  const deliveryId = "40000000-0000-4000-8000-000000000001";
  const payload = currentFixture("agent-session-created.json");
  const raw = Buffer.from(JSON.stringify(payload));
  state.channel.inbox.accept({
    deliveryId,
    webhookId: String(payload.webhookId),
    eventName: "AgentSessionEvent",
    bodyDigest: sha256(raw),
    semanticDigest: webhookSemanticDigest(raw),
    organizationId: String(payload.organizationId),
    payloadCiphertext: state.cipher.encrypt(raw.toString("utf8")),
    unsupportedShape: null,
    // A created AgentSessionEvent carries its session as session_key; this fixture is a
    // direct start.
    sessionKey: String((payload.agentSession as { id: string }).id),
    preempts: false,
  });
  const claimedTick = await state.channel.ticks.ingress();
  assert.deepEqual(claimedTick, {
    claimed: deliveryId,
    outcome: { disposition: "startup_rejected", stage: "Validation", reason: "authorization_unavailable" },
    failures: [],
  });

  // Ingress records per-delivery failures durably before returning an empty failures
  // list. inbox.claim can throw before the tick's try block; that failure must reach
  // the scheduled loop's onError.
  state.channel.inbox.close();
  await assert.rejects(() => state.channel.ticks.ingress(), "a claim fault must escape the adapter");
});

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

test("startLinearChannelProcess arms all three loops, and every one of them reports a throwing tick to onTickError by name", async () => {
  const ran: LinearLoopName[] = [];
  const failures: LinearLoopFailure[] = [];
  const throwing = (name: LinearLoopName): LinearTick => async () => {
    ran.push(name);
    throw new Error(`${name} exploded`);
  };

  const process = startLinearChannelProcess({
    ticks: { ingress: throwing("ingress"), observe: throwing("observe"), outbox: throwing("outbox") },
    intervals: { ingress: 5, observe: 5, outbox: 5 },
    onTickError: (failure) => failures.push(failure),
  });
  try {
    const deadline = Date.now() + 5_000;
    while (new Set(failures.map((failure) => failure.loop)).size < LINEAR_LOOP_NAMES.length) {
      if (Date.now() > deadline) assert.fail(`only ${JSON.stringify([...new Set(failures.map((f) => f.loop))])} reported within the deadline; a loop that is never armed is indistinguishable from a tick that never fails`);
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  } finally {
    await process.close();
  }

  assert.deepEqual([...new Set(failures.map((failure) => failure.loop))].sort(), [...LINEAR_LOOP_NAMES].sort());
  for (const failure of failures) assert.match(String(failure.error), new RegExp(`${failure.loop} exploded`, "u"));
  assert.deepEqual([...new Set(ran)].sort(), [...LINEAR_LOOP_NAMES].sort());
});

/**
 * Contained observe and outbox failures are returned in LinearTickResult.failures so
 * one bad item does not stop other work. startLinearChannelProcess must forward those
 * returned failures to onTickError; handling only thrown scheduled-loop errors would
 * silently discard them.
 */
test("a failure a tick contains rather than throws still reaches onTickError, carrying its subject", async () => {
  const failures: LinearLoopFailure[] = [];
  const contained: LinearTick = async () => Object.freeze({
    failures: Object.freeze([{ subject: "conversation-42", error: new Error("one item failed") }]),
  });
  const quiet: LinearTick = async () => Object.freeze({ failures: [] });

  const process = startLinearChannelProcess({
    ticks: { ingress: quiet, observe: contained, outbox: quiet },
    intervals: { ingress: 5, observe: 5, outbox: 5 },
    onTickError: (failure) => failures.push(failure),
  });
  try {
    const deadline = Date.now() + 5_000;
    while (failures.length === 0) {
      if (Date.now() > deadline) assert.fail("a contained tick failure never reached onTickError -- it was collected into a value nobody read");
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  } finally {
    await process.close();
  }

  assert.equal(failures[0]?.loop, "observe");
  assert.equal(failures[0]?.subject, "conversation-42", "the subject distinguishes one contained item from the whole tick failing");
  assert.match(String(failures[0]?.error), /one item failed/u);
  // A tick that threw outright reports the same shape with no subject, so a
  // reader can tell "one conversation failed" from "the loop failed".
  assert.deepEqual([...new Set(failures.map((failure) => failure.loop))], ["observe"], "a quiet tick must report nothing");
});

test("close() disarms every loop: no tick runs after it resolves", async () => {
  // One counter PER LOOP, not one shared: a shared counter reaching 3 is
  // satisfied by the ingress loop firing three times, which would let this
  // pass with two loops never armed.
  const calls: Record<LinearLoopName, number> = { ingress: 0, observe: 0, outbox: 0 };
  const tick = (name: LinearLoopName): LinearTick => async () => { calls[name] += 1; return Object.freeze({ failures: [] }); };
  const process = startLinearChannelProcess({
    ticks: { ingress: tick("ingress"), observe: tick("observe"), outbox: tick("outbox") },
    intervals: { ingress: 5, observe: 5, outbox: 5 },
    onTickError: ({ loop, error }) => assert.fail(`${loop} threw: ${String(error)}`),
  });
  const deadline = Date.now() + 5_000;
  while (LINEAR_LOOP_NAMES.some((name) => calls[name] === 0)) {
    if (Date.now() > deadline) assert.fail(`not every loop ran: ${JSON.stringify(calls)}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  await process.close();
  const settled = { ...calls };
  await new Promise((resolve) => setTimeout(resolve, 60));
  assert.deepEqual(calls, settled, "a tick ran after close() resolved");
});

/**
 * Prove that the observe loop schedules outbox reconciliation. Plant an ambiguous row,
 * start the loops and wait for reconcile_attempted_at without driving any tick
 * manually. Only the scheduled observe path can perform that update.
 */
test("the reconciliation step is genuinely scheduled: a running process touches an ambiguous row's reconciliation attempt with no manual tick", async (context) => {
  const state = fixture(context);
  const target = linearChannelDeliveryTarget({ agentSessionId: AGENT_SESSION_ID });

  const row = state.channel.outbox.recordDirectMessage({ conversationId: AGENT_SESSION_ID, target, sourceEventId: "source-ambiguous", content: { type: "response", body: "ambiguous" } });
  const claimed = state.channel.outbox.claim("composition-test");
  assert.equal(claimed?.id, row.outboxId);
  state.channel.outbox.recordDisposition(row.outboxId, { disposition: "ambiguous", safeCode: "linear.provider.transport_ambiguous" });
  assert.equal(state.channel.outbox.outboxState(row.outboxId), "ambiguous");

  const attemptedAt = (): string | null => {
    const database = new DatabaseSync(state.databases.delivery, { readOnly: true });
    try {
      const found = database.prepare("SELECT reconcile_attempted_at FROM channel_message_outbox WHERE id = ?").get(row.outboxId) as { reconcile_attempted_at: string | null } | undefined;
      return found?.reconcile_attempted_at ?? null;
    } finally { database.close(); }
  };
  assert.equal(attemptedAt(), null, "the planted row must start with no reconciliation attempt, or this test cannot fail");

  // The provider's unknown reconciliation outcome leaves the Activity unresolved. Its
  // reconciliation attempt must still rotate the row so other ambiguous messages can be
  // examined.
  const failures: unknown[] = [];
  const process = startLinearChannelProcess({
    ticks: state.channel.ticks,
    intervals: { ingress: 5, observe: 5, outbox: 5 },
    onTickError: ({ loop, subject, error }) => failures.push([loop, subject, String(error)]),
  });
  try {
    const deadline = Date.now() + 10_000;
    while (attemptedAt() === null) {
      if (Date.now() > deadline) assert.fail(`the scheduled observe loop never touched the ambiguous row's reconciliation attempt. Loop failures: ${JSON.stringify(failures)}`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  } finally {
    await process.close();
  }
});

test("the inbox lease owner is the documented literal", () => {
  assert.equal(LINEAR_INBOX_LEASE_OWNER, "linear-relay");
});

/**
 * The inbox lease spaces retry attempts: a processing delivery cannot be reclaimed
 * before lease_expires_at. Measure the composed inbox's default claim behavior and pin
 * the 30-second lease so the ten-attempt budget cannot be consumed by rapidly repeated
 * claims.
 */
test("the composed inbox hands out the 30-second lease the retry spacing depends on", (context) => {
  const state = fixture(context);
  const payload = currentFixture("agent-session-created.json");
  const raw = Buffer.from(JSON.stringify(payload));
  const accepted = state.channel.inbox.accept({
    deliveryId: "40000000-0000-4000-8000-000000000002",
    webhookId: String(payload.webhookId),
    eventName: "AgentSessionEvent",
    bodyDigest: sha256(raw),
    semanticDigest: webhookSemanticDigest(raw),
    organizationId: String(payload.organizationId),
    payloadCiphertext: state.cipher.encrypt(raw.toString("utf8")),
    unsupportedShape: null,
    // A created AgentSessionEvent carries its session as session_key; this fixture is a
    // direct start.
    sessionKey: String((payload.agentSession as { id: string }).id),
    preempts: false,
  });
  assert.deepEqual(accepted, { duplicate: false, state: "accepted" }, "the probe delivery must be claimable, or nothing below can fail");

  const before = Date.now();
  const claim = state.channel.inbox.claim(LINEAR_INBOX_LEASE_OWNER);
  const after = Date.now();
  if (claim?.disposition !== "attempt") throw new Error(`an attempt was required: ${JSON.stringify(claim)}`);
  const expiresAt = Date.parse(claim.delivery.leaseExpiresAt);
  assert.ok(
    expiresAt >= before + LINEAR_INBOX_LEASE_MS && expiresAt <= after + LINEAR_INBOX_LEASE_MS,
    `the default lease must expire ${LINEAR_INBOX_LEASE_MS}ms out; it expires ${expiresAt - after}..${expiresAt - before}ms out`,
  );
  assert.equal(LINEAR_INBOX_LEASE_MS, 30_000);
});
