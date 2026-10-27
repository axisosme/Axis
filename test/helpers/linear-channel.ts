import { createHmac, randomUUID } from "node:crypto";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { composeLinearChannel, type LinearChannel } from "../../src/relay/linear/composition.js";
import { defaultStartWorkerRouting } from "../support/node-dispatch-double.js";
import type { LinearWorkerRoutingOptions } from "../../src/relay/linear/channel.js";
import { startRelayHttpService, type RelayHttpService } from "../../src/relay/http.js";
import type { LinearProviderPort } from "../../src/relay/linear/provider.js";
import type { LinearTickFailure } from "../../src/relay/linear/observe.js";
import type { EnvelopeCipher } from "../../src/relay/security.js";
import type { CredentialVault } from "../../src/relay/types.js";
import type { LinearChannelDatabasePaths } from "../../src/relay/linear/runtime-config.js";
import type { Router } from "../../src/router/router.js";

/**
 * Compose the production Linear channel with real SQLite stores and signed HTTP
 * ingress. Linear's GraphQL API is supplied by the caller's provider double. The
 * scheduler stays stopped so tests drive ingress, observe and outbox ticks explicitly.
 *
 * Channel snapshots read the separate channel, conversation, run-chain and delivery
 * databases. drain() round-robins the three ticks until no progress occurs, with a
 * round-count budget so scheduling delays do not exhaust a wall-clock deadline. A quiet
 * chain is settled only when its durable snapshot has no outstanding work.
 */

/** The access token every fixture installation resolves to, through the REAL `LinearOAuthManager#accessToken`. */
const LINEAR_TEST_ACCESS_TOKEN = "fixture-linear-access-token";

function linearCredentialRef(organizationId: string, appUserId: string): string {
  return `linear:${organizationId}:${appUserId}`;
}

/**
 * Stored OAuth credential in the shape LinearOAuthManager reads from the vault. Tests
 * write it directly because the OAuth exchange belongs to the provider boundary;
 * dispatch still reads each token through accessToken.
 */
function storedLinearCredential(accessToken: string = LINEAR_TEST_ACCESS_TOKEN): string {
  return JSON.stringify({ version: 1, accessToken, refreshToken: null, expiresAt: null, scope: "read write app:assignable app:mentionable" });
}

/**
 * Completes the installation an operator would have produced by walking
 * `/oauth/linear/install`, and retains its credential.
 *
 * Until this runs, a composed channel accepts webhooks and refuses every one
 * of them at the authority gate -- fail-closed by construction.
 *
 * Returns the credential reference, which is the vault key the real
 * `LinearOAuthManager#accessToken` looks the credential up under.
 * `../smoke.ts` asserts it, so the key this fixture writes and the key
 * production reads cannot drift apart unnoticed.
 */
export async function installTestLinearInstallation(input: {
  readonly channel: LinearChannel;
  readonly vault: CredentialVault;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly teamIds: readonly string[];
}): Promise<string> {
  const credentialRef = linearCredentialRef(input.organizationId, input.appUserId);
  await input.vault.put(credentialRef, storedLinearCredential());
  input.channel.installations.install({
    organizationId: input.organizationId,
    appUserId: input.appUserId,
    credentialRef,
    teamIds: [...input.teamIds],
    canAccessAllPublicTeams: false,
  });
  return credentialRef;
}

/** The four database files a composed Linear channel opens, under one directory. */
export function linearChannelDatabases(directory: string): LinearChannelDatabasePaths {
  return {
    channel: join(directory, "linear-channel.sqlite"),
    conversations: join(directory, "linear-conversations.sqlite"),
    runChain: join(directory, "linear-run-chain.sqlite"),
    delivery: join(directory, "linear-delivery.sqlite"),
  };
}

/**
 * Durable channel work counts across the channel, conversation, run-chain and delivery
 * databases. Reservations with settled_at IS NULL represent commands not yet
 * reconciled; pendingConversationCommands reads the conversation's matching
 * pending-command slot.
 *
 * The reservation CHECK constraint, integrity sweep and transactional writes enforce
 * agreement between the two counts. A disagreement indicates store damage rather than a
 * normal intermediate state.
 */
export interface LinearChannelSnapshot {
  readonly acceptedDeliveries: number;
  readonly processingDeliveries: number;
  readonly terminalDeliveries: number;
  readonly commands: number;
  readonly unsettledCommands: number;
  readonly pendingConversationCommands: number;
  readonly outbox: number;
  readonly unsettledOutbox: number;
}

function count(filename: string, sql: string): number {
  const database = new DatabaseSync(filename, { readOnly: true });
  try {
    return Number((database.prepare(sql).get() as { readonly count: number | bigint }).count);
  } finally {
    database.close();
  }
}

/** Reads the counted state of a composed channel straight off its four database files. */
export function linearChannelSnapshot(databases: LinearChannelDatabasePaths): LinearChannelSnapshot {
  return {
    acceptedDeliveries: count(databases.channel, "SELECT COUNT(*) AS count FROM linear_inbox_deliveries WHERE state = 'accepted'"),
    processingDeliveries: count(databases.channel, "SELECT COUNT(*) AS count FROM linear_inbox_deliveries WHERE state = 'processing'"),
    terminalDeliveries: count(databases.channel, "SELECT COUNT(*) AS count FROM linear_inbox_deliveries WHERE state IN ('completed', 'quarantined', 'failed')"),
    commands: count(databases.runChain, "SELECT COUNT(*) AS count FROM channel_run_chain_reservations"),
    unsettledCommands: count(databases.runChain, "SELECT COUNT(*) AS count FROM channel_run_chain_reservations WHERE settled_at IS NULL"),
    pendingConversationCommands: count(databases.runChain, "SELECT COUNT(*) AS count FROM channel_run_chain_conversations WHERE pending_command_id IS NOT NULL"),
    outbox: count(databases.delivery, "SELECT COUNT(*) AS count FROM channel_message_outbox"),
    unsettledOutbox: count(databases.delivery, "SELECT COUNT(*) AS count FROM channel_message_outbox WHERE state != 'delivered'"),
  };
}

/**
 * Whether every delivery is terminal, every command reconciled and every Activity
 * delivered. confirmed_failure, ambiguous and conflict are terminal outbox states but
 * still count as unsettled: a drain guard must not treat an undelivered Activity as
 * completed work.
 */
export function linearChannelSettled(snapshot: LinearChannelSnapshot): boolean {
  return snapshot.acceptedDeliveries === 0
    && snapshot.processingDeliveries === 0
    && snapshot.unsettledCommands === 0
    && snapshot.pendingConversationCommands === 0
    && snapshot.unsettledOutbox === 0;
}

/**
 * Redispatch dispositions that change durable state. The observe tick reports an entry
 * for every examined conversation, so redispatches.length alone is not progress;
 * counting unchanged entries would keep drain() running indefinitely.
 */
const REDISPATCH_PROGRESS_DISPOSITIONS: readonly string[] = Object.freeze(["dispatched", "dispatch_rejected", "instruction_changed"]);

/** One pass of all three ticks, and whether it moved anything. */
export interface LinearDrainRound {
  readonly claimed: string | null;
  readonly eventsApplied: number;
  readonly activitiesEnqueued: number;
  readonly reconciledOutboxId: string | null;
  /** Redispatch outcomes that changed state. */
  readonly redispatched: number;
  /**
   * Redispatch outcomes that could not proceed and left their marker in place
   * -- `unresolved`, `task_unresolved`, `credential_unavailable`,
   * `instruction_unavailable`, `reservation_refused`.
   *
   * Deliberately NOT progress: each recurs identically next round. Reported
   * because a caller that saw a quiet drain would otherwise read a
   * permanently stuck marker as "nothing to do".
   */
  readonly stalledRedispatches: readonly string[];
  /**
   * Delivery attempts this round.
   *
   * Recorded but NOT used as progress: `LinearOutboxTickResult#attempts`
   * counts an attempt that found nothing due, because the frozen port gives
   * the loop no way to distinguish that from a delivery. Outbox progress is
   * therefore read off the snapshot instead.
   */
  readonly deliveryAttempts: number;
  readonly progressed: boolean;
}

export interface LinearDrainResult {
  readonly rounds: readonly LinearDrainRound[];
  readonly snapshot: LinearChannelSnapshot;
  /**
   * Contained per-item failures from observe and outbox ticks across all rounds.
   * Ingress failures are recorded durably on inbox rows rather than returned here.
   * Retryable delivery failures also return rows to pending with a future available_at
   * without entering this list.
   *
   * An empty failure list does not establish completion; settled reads the durable work
   * counts.
   */
  readonly failures: readonly LinearTickFailure[];
  /**
   * Whether the channel is actually quiescent, per `linearChannelSettled`.
   *
   * The stop condition is "a round made no progress", which is not the same
   * thing: a retryable delivery waiting on its backoff, or a permanently
   * ambiguous row, both stop the loop with work outstanding. A caller that
   * needs quiescence asserts THIS.
   */
  readonly settled: boolean;
  /** Every stalled redispatch disposition observed in the final round, if any. */
  readonly stalledRedispatches: readonly string[];
}

/** How many full ingress/observe/outbox rounds a drain may spend before it gives up. */
const LINEAR_DRAIN_ROUNDS = 400;

export interface TestLinearChannel {
  readonly channel: LinearChannel;
  readonly http: RelayHttpService;
  readonly databases: LinearChannelDatabasePaths;
  /** POSTs a correctly signed webhook to the REAL `/webhooks/linear` over a real socket. */
  post(payload: Record<string, unknown>, options?: { readonly deliveryId?: string; readonly event?: string }): Promise<{ readonly status: number; readonly body: unknown }>;
  /** Runs the three ticks round-robin until a whole round makes no progress. */
  drain(): Promise<LinearDrainResult>;
  snapshot(): LinearChannelSnapshot;
  close(): Promise<void>;
}

export interface TestLinearChannelOptions {
  readonly databases: LinearChannelDatabasePaths;
  /** The process's Router. Neither composed nor closed here, exactly as in production. */
  readonly router: Router;
  readonly provider: LinearProviderPort;
  readonly vault: CredentialVault;
  readonly cipher: EnvelopeCipher;
  readonly signingSecret: string;
  readonly operatorToken: string;
  /** The Team allowlist a completed installation is granted. */
  readonly teamIds: readonly string[];
  /**
   * Audit sink for a start's routing context, defaulting to defaultStartWorkerRouting.
   * The Node owns Worker selection; every start carries a policy selection through the
   * Node command port.
   */
  readonly workerRouting?: LinearWorkerRoutingOptions;
}

/**
 * Compose the Linear channel and its real HTTP entry with an empty installation store.
 * A caller installs authority through channel.installations.install(...) before
 * deliveries can pass the authority gate.
 */
export async function startTestLinearChannel(options: TestLinearChannelOptions): Promise<TestLinearChannel> {
  const channel = composeLinearChannel({
      databases: options.databases,
      webhookSecret: options.signingSecret,
      oauth: { clientId: "fixture-client", clientSecret: "fixture-client-secret", redirectUri: "https://relay.invalid/oauth/linear/callback" },
      defaultTeamIds: options.teamIds,
      allowAllPublicTeams: false,
      integrityCheckIntervalMs: 60_000,
      tickIntervals: { ingress: 1_000, observe: 1_000, outbox: 1_000 },
    }, {
      cipher: options.cipher,
      vault: options.vault,
      provider: options.provider,
      router: options.router,
      workerRouting: options.workerRouting ?? defaultStartWorkerRouting,
      // A bounded instruction re-read must never really wait. The retry itself
    // is still the real one; only the pause between attempts is removed.
    sleep: async () => { /* deterministic */ },
    });

  let http: RelayHttpService | undefined;
  try {
    http = await startRelayHttpService({
      linear: channel.ingress,
      ready: () => channel.ready,
      diagnostics: () => channel.diagnose(),
      oauth: channel.oauth,
      operatorToken: options.operatorToken,
    });
  } catch (error) {
    channel.close();
    throw error;
  }

  const listener = http;
  return {
    channel,
    http: listener,
    databases: options.databases,
    async post(payload, postOptions = {}) {
      const body = Buffer.from(JSON.stringify(payload));
      const response = await fetch(new URL("/webhooks/linear", listener.url), {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "linear-delivery": postOptions.deliveryId ?? randomUUID(),
          "linear-event": postOptions.event ?? "AgentSessionEvent",
          "linear-signature": createHmac("sha256", options.signingSecret).update(body).digest("hex"),
          "linear-timestamp": String(Date.now()),
        },
        body: new Uint8Array(body),
      });
      return { status: response.status, body: await response.json() };
    },
    async drain() {
      const rounds: LinearDrainRound[] = [];
      const failures: LinearTickFailure[] = [];
      // A round IS the unit of progress here, so the budget is counted in
      // rounds directly rather than through `awaitWithinAttempts`: there is no
      // separate condition to poll, and each round already awaits real work.
      // Same denomination for the same reason -- a round on a loaded machine
      // takes longer and still counts as one round.
      let before = linearChannelSnapshot(options.databases);
      for (let round = 0; round < LINEAR_DRAIN_ROUNDS; round += 1) {
        const ingress = await channel.ticks.ingress();
        const observe = await channel.ticks.observe();
        const outbox = await channel.ticks.outbox();
        failures.push(...ingress.failures, ...observe.failures, ...outbox.failures);
        const after = linearChannelSnapshot(options.databases);
        const dispositions = observe.redispatches.map((entry) => entry.outcome.disposition);
        const observed: LinearDrainRound = {
          claimed: ingress.claimed,
          eventsApplied: observe.eventsApplied,
          activitiesEnqueued: observe.activitiesEnqueued,
          // Recorded, NOT progress. `reconciledOutboxId` is
          // `ambiguousMessages()[0]?.id`, so it is non-null whenever ANY row
          // sits in `ambiguous` -- whether or not this tick's reconcile changed
          // anything. A non-resolving reconcile leaves the row `ambiguous` and
          // only touches `reconcile_attempted_at`, which the snapshot does not
          // read, so counting it as progress makes a permanently ambiguous row
          // spend the entire round budget and then report "never stopped making
          // progress" -- the opposite of what happened. It stays here because a
          // caller debugging a stuck drain wants to see it.
          reconciledOutboxId: observe.reconciledOutboxId,
          redispatched: dispositions.filter((disposition) => REDISPATCH_PROGRESS_DISPOSITIONS.includes(disposition)).length,
          stalledRedispatches: dispositions.filter((disposition) => disposition !== "not_pending" && !REDISPATCH_PROGRESS_DISPOSITIONS.includes(disposition)),
          deliveryAttempts: outbox.attempts,
          // Durable state moving is the primary signal, because it is the one
          // thing no tick can misreport. The tick fields add the cases that
          // change run-chain or Router state this snapshot does not count.
          progressed: JSON.stringify(after) !== JSON.stringify(before)
            || ingress.claimed !== null
            || observe.eventsApplied > 0
            || observe.activitiesEnqueued > 0
            || dispositions.some((disposition) => REDISPATCH_PROGRESS_DISPOSITIONS.includes(disposition)),
        };
        rounds.push(observed);
        if (!observed.progressed) {
          return { rounds, snapshot: after, failures, settled: linearChannelSettled(after), stalledRedispatches: observed.stalledRedispatches };
        }
        before = after;
      }
      throw new Error(`the Linear channel never stopped making progress within ${LINEAR_DRAIN_ROUNDS} rounds (last=${JSON.stringify(rounds.at(-1))})`);
    },
    snapshot() {
      return linearChannelSnapshot(options.databases);
    },
    async close() {
      await listener.close();
      channel.close();
    },
  };
}
