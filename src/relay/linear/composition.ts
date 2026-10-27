import { integrityCheckSchedule } from "../integrity-cadence.js";
import { SqliteChannelOutboxStore } from "../channel-outbox.js";
import { LinearEventRenderer } from "./activity.js";
import { SqliteLinearInbox } from "./inbox.js";
import { LinearInstallationStore } from "./installation.js";
import { LinearActivityDeliverer, linearActivityId, linearDeliveryAuthority, type LinearDeliverySessionLookup } from "./delivery.js";
import { linearIssueContextStep, linearIngressTick, type LinearIngressTickResult, type LinearWorkerRoutingOptions } from "./channel.js";
import { nodeChannelCommandPort } from "../node-routing-composition.js";
import type { LinearIngressOptions as LinearWebhookIngressOptions } from "./ingress.js";
import {
  SqliteLinearLedgers,
  linearChannelTaskRefLookup,
  linearDecisionOwnerLookup,
  linearRedispatchSourceLookup,
} from "./ledgers.js";
import {
  LINEAR_LOOP_NAMES,
  deliverLinearOutboxTick,
  observeLinearTick,
  type LinearLoopName,
  type LinearLoopTicks,
  type LinearObserveTickResult,
  type LinearOutboxTickResult,
  type LinearTick,
  type LinearTickFailure,
  type LinearTickResult,
} from "./observe.js";
import { linearRedispatchInstructionReader, type LinearRedispatchInstructionReader } from "./redispatch.js";
import type { LinearProviderPort } from "./provider.js";
import { validateStructuredElicitation } from "../../shared/elicitation.js";
import { LinearOAuthManager } from "../oauth.js";
import { scheduledLoop, type ScheduledLoop } from "../scheduled-loop.js";
import type { EnvelopeCipher } from "../security.js";
import type { CredentialVault } from "../types.js";
import type { LinearRuntimeConfig, LinearTickIntervals } from "./runtime-config.js";

import type { ChannelCommandPort } from "../../router/channel-command.js";
import { SqliteChannelConversationStore } from "../../router/channel-conversation.js";
import { SqliteChannelRunChainStore } from "../../router/channel-run-chain.js";
import { linearChannelDeliveryTarget } from "../../router/channel-conversation-types.js";
import type { Router } from "../../router/router.js";

/**
 * The Linear channel's composition root: assembles the stores, instruction
 * surface, renderer, Activity deliverer and the three ticks. It never installs
 * a credential, admits a Team, or contacts Linear; with no completed OAuth
 * installation, webhooks are persisted and refused at the authority gate.
 */

/** The OAuth scopes Axis requests. Pinned by a test so a scope change cannot pass silently. */
export const LINEAR_OAUTH_SCOPES: readonly string[] = Object.freeze(["read", "write", "app:assignable", "app:mentionable"]);

/** The lease owner Linear's inbox records against a claimed delivery. */
export const LINEAR_INBOX_LEASE_OWNER = "linear-relay";

const NO_FAILURES: readonly LinearTickFailure[] = Object.freeze([]);

/** Bounded retry backoff for the instruction resolver's exact reads, and for the redispatch re-read. */
const DEFAULT_SLEEP = (milliseconds: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, milliseconds));

export interface LinearIngressLoopTickResult extends LinearTickResult, LinearIngressTickResult {}

/** The three loops, typed by what each returns. */
export interface LinearChannelTicks extends LinearLoopTicks {
  readonly ingress: LinearTick<LinearIngressLoopTickResult>;
  readonly observe: LinearTick<LinearObserveTickResult>;
  readonly outbox: LinearTick<LinearOutboxTickResult>;
}

export interface LinearChannelOptions {
  /** Replay window for webhook timestamps; omitted uses the ingress default. */
  readonly webhookToleranceMs?: number;
  /** Test-only override of Linear's OAuth endpoints and transport. */
  readonly oauthTransport?: {
    readonly authorizeEndpoint?: string;
    readonly tokenEndpoint?: string;
    readonly fetch?: typeof fetch;
  };
  /** Encrypts inbox payloads and OAuth PKCE verifiers. The same cipher the vault's envelopes use. */
  readonly cipher: EnvelopeCipher;

  readonly vault: CredentialVault;
  readonly provider: LinearProviderPort;

  readonly router: Router;
  /** Label rules and audit for resolving a start's Worker. Required: a start cannot dispatch without a resolved selection. */
  readonly workerRouting: LinearWorkerRoutingOptions;
  /** The inbox lease owner. Defaults to `LINEAR_INBOX_LEASE_OWNER`; overridable so two processes hold distinguishable leases. */
  readonly inboxOwnerId?: string;
  /** The outbox lease owner. Defaults to `LINEAR_OUTBOX_LEASE_OWNER` inside `deliverLinearOutboxTick`. */
  readonly outboxOwnerId?: string;
  /** How long an ingress claim holds its delivery. Defaults to `LINEAR_INBOX_LEASE_MS`. */
  readonly inboxLeaseMs?: number;
  /** Delivery attempts per outbox tick. Defaults to 1. */
  readonly messagesPerTick?: number;
  /** Passed to `reconcileStale`; omitted takes its own deliberately generous default. */
  readonly staleReservationMs?: number;
  /** Passed to `reconcileStale` as `unconfirmedAfterMs`; omitted takes `DEFAULT_UNCONFIRMED_RESERVATION_MS`. */
  readonly unconfirmedReservationMs?: number;
  /** The clock stores write timestamps from. Not given to `LinearOAuthManager`, whose expiry checks must track Linear's wall clock. */
  readonly now?: () => Date;
  /** Backoff between the instruction resolver's bounded exact reads. Required to be injectable so a test never really waits. */
  readonly sleep?: (milliseconds: number) => Promise<void>;
}

export interface LinearChannel {
  readonly inbox: SqliteLinearInbox;
  readonly ledgers: SqliteLinearLedgers;
  readonly installations: LinearInstallationStore;
  readonly conversations: SqliteChannelConversationStore;
  readonly runChain: SqliteChannelRunChainStore;
  readonly outbox: SqliteChannelOutboxStore;
  readonly oauth: LinearOAuthManager;
  readonly deliverer: LinearActivityDeliverer;
  readonly renderer: LinearEventRenderer;
  readonly dispatch: ChannelCommandPort;
  /** Ready to pass straight to `ingestLinearWebhook`, and to `RelayHttpOptions#linear`. */
  readonly ingress: LinearWebhookIngressOptions;
  readonly ticks: LinearChannelTicks;
  /**
   * Whether this channel can serve traffic: the vault is readable and every
   * store in `LINEAR_CHANNEL_STORE_NAMES` passes `readyForTraffic()`, whose
   * cost is bounded by schema size, not rows. `/health/ready` is
   * unauthenticated and polled, so it never runs whole-database scans; those
   * run at open, on the observe tick's integrity schedule (conversation
   * registry and run chain), and in `diagnose()`. `LinearInstallationStore`
   * has no health check. A getter, re-read per request.
   */
  readonly ready: boolean;
  /** The full health check for every store, served at `GET /health/diagnostics` behind the operator token. */
  diagnose(): LinearChannelDiagnostics;
  /** Closes every store this composition opened. Never closes `router`, which it did not construct. */
  close(): void;
}

/** The stores `/health/ready` and `diagnose()` consult. `installations` is absent: it has no health check. */
export const LINEAR_CHANNEL_STORE_NAMES = Object.freeze(["inbox", "ledgers", "conversations", "runChain", "outbox"] as const);

export type LinearChannelStoreName = (typeof LINEAR_CHANNEL_STORE_NAMES)[number];

/** The two health questions a store answers, declared structurally across the three store classes. */
interface LinearChannelStoreHealth {
  /** Cost independent of data volume. What `/health/ready` may call. */
  readyForTraffic(): boolean;
  /** Whole-database scans. Operator-triggered only. */
  ready(): boolean;
}

type LinearChannelStores = Readonly<Record<LinearChannelStoreName, LinearChannelStoreHealth>>;

/** One `GET /health/diagnostics` answer, per store, so an operator can see which one failed. */
export interface LinearChannelDiagnostics {
  /** True only if the vault is readable and every store passes its full check. */
  readonly healthy: boolean;
  /** The credential vault, which `LinearOAuthManager#ready` reads. */
  readonly vault: boolean;
  readonly stores: Readonly<Record<LinearChannelStoreName, boolean>>;
}

/** Adapts the ingress tick to `LinearTick`. `failures` is empty: the ingress tick durably records what it contains. */
function linearIngressLoopTick(tick: () => Promise<LinearIngressTickResult>): LinearTick<LinearIngressLoopTickResult> {
  return async () => {
    const result = await tick();
    return Object.freeze({ claimed: result.claimed, outcome: result.outcome, failures: NO_FAILURES });
  };
}

/** Assemble stores and ticks from validated configuration and explicit runtime dependencies. */
export function composeLinearChannel(configuration: LinearRuntimeConfig, options: LinearChannelOptions): LinearChannel {
  const config = { ...options, ...configuration };

  const clock = config.now === undefined ? {} : { now: config.now };
  const sleep = config.sleep ?? DEFAULT_SLEEP;

  // Opened under one rollback, since a failed caller never receives a `LinearChannel` to close.
  const opened: { close(): void }[] = [];
  function open<TStore extends { close(): void }>(store: TStore): TStore {
    opened.push(store);
    return store;
  }

  try {
    // Three stores sharing the channel database, each sweeping only its own table prefix.
    const inbox = open(new SqliteLinearInbox({ filename: config.databases.channel, ...clock }));
    const ledgers = open(new SqliteLinearLedgers({ filename: config.databases.channel, ...clock }));
    const installations = open(new LinearInstallationStore(config.databases.channel, { ...clock }));

    const conversations = open(new SqliteChannelConversationStore({ filename: config.databases.conversations, ...clock }));
    const runChain = open(new SqliteChannelRunChainStore({ conversations, filename: config.databases.runChain, ...clock }));
    const outbox = open(new SqliteChannelOutboxStore(config.databases.delivery, {
      ...clock,
      // Full scan at open; see `scanAtOpen`.
      scanAtOpen: true,
      // Reuses the Worker receipt's `activityId` when the rendering offers one.
      messageId: ({ rendered }) => linearActivityIdFor(rendered),
    }));

    const oauth = new LinearOAuthManager({
      clientId: config.oauth.clientId,
      clientSecret: config.oauth.clientSecret,
      redirectUri: config.oauth.redirectUri,
      scopes: LINEAR_OAUTH_SCOPES,
      store: installations,
      cipher: config.cipher,
      vault: config.vault,
      provider: config.provider,
      defaultTeamIds: config.defaultTeamIds,
      allowAllPublicTeams: config.allowAllPublicTeams,
      ...(config.oauthTransport?.authorizeEndpoint === undefined ? {} : { authorizeEndpoint: config.oauthTransport.authorizeEndpoint }),
      ...(config.oauthTransport?.tokenEndpoint === undefined ? {} : { tokenEndpoint: config.oauthTransport.tokenEndpoint }),
      ...(config.oauthTransport?.fetch === undefined ? {} : { fetch: config.oauthTransport.fetch }),
    });

    /** The retained session identities Activity delivery re-authorizes against, read from the ledger's session row. */
    const sessions: LinearDeliverySessionLookup = Object.freeze({
      deliverySession(agentSessionId: string) {
        const record = ledgers.session(agentSessionId);
        return record === null ? null : Object.freeze({
          organizationId: record.organizationId,
          appUserId: record.appUserId,
          teamId: record.teamId,
        });
      },
    });

    const authority = linearDeliveryAuthority({ sessions, installations, credentials: oauth });

    const deliverer = new LinearActivityDeliverer({
      transport: config.provider,
      authority,
      // Matches row id or provider message id in any state; see `ownsMessageId`.
      delivered: Object.freeze({ deliveredActivity: (activityId: string) => outbox.ownsMessageId(activityId) }),
      // An absent and a malformed row both become `null`, which refuses the post.
      elicitations: Object.freeze({
        forOutbox: ({ outboxId }: { readonly outboxId: string }) => {
          const decision = outbox.waitingDecisionForOutbox(outboxId);
          return decision === null ? null : validateStructuredElicitation(decision.elicitation);
        },
      }),
    });

    const renderer = new LinearEventRenderer({ owners: linearDecisionOwnerLookup({ ledger: ledgers, runChain }) });
    const dispatch = nodeChannelCommandPort(config.router);
    const taskRefs = linearChannelTaskRefLookup(ledgers);

    /**
     * The redispatch re-reader, built per call: an access token belongs to the
     * session's installation, so it is resolved through the delivery authority
     * for the session being read. A `null` token leaves the marker for a later tick.
     */
    const instructions: LinearRedispatchInstructionReader = Object.freeze({
      async readRedispatchInstruction(input: Parameters<LinearRedispatchInstructionReader["readRedispatchInstruction"]>[0]) {
        const agentSessionId = input.source.agentSessionId;
        return linearRedispatchInstructionReader({
          provider: config.provider,
          accessToken: async () => {
            const decision = await authority.authorize({ target: linearChannelDeliveryTarget({ agentSessionId }) });
            return decision.disposition === "authorized" ? decision.accessToken : null;
          },
          sleep,
        }).readRedispatchInstruction(input);
      },
    });

    const ingress: LinearWebhookIngressOptions = {
      inbox,
      cipher: config.cipher,
      signingSecret: config.webhookSecret,
      now: config.now ?? (() => new Date()),
      ...(config.webhookToleranceMs === undefined ? {} : { toleranceMs: config.webhookToleranceMs }),
    };

    const ingressTick = linearIngressTick({
      owner: config.inboxOwnerId ?? LINEAR_INBOX_LEASE_OWNER,
      inbox,
      cipher: config.cipher,
      credentials: oauth,
      installations,
      provider: config.provider,
      ledgers,
      conversations,
      runChain,
      outbox,
      dispatch,
      feedback: deliverer,
      issueContext: linearIssueContextStep(config.provider),
      workerRouting: config.workerRouting,
      ...(config.inboxLeaseMs === undefined ? {} : { leaseMs: config.inboxLeaseMs }),
      sleep,
    });

    // Built once, outside the tick closure: a fresh schedule's first `due()` is always true.
    const integrity = integrityCheckSchedule({ ...(config.integrityCheckIntervalMs === undefined ? {} : { intervalMs: config.integrityCheckIntervalMs }) });

    const ticks: LinearChannelTicks = Object.freeze({
      ingress: linearIngressLoopTick(ingressTick),
      observe: async () => observeLinearTick({
        router: config.router,
        runChain,
        integrity,
        // Lets the sweep tell a stale reservation whose delivery is still alive from one that is not.
        inbox,
        ledgers,
        // Verified on the `integrity` schedule; not a data dependency.
        conversations,
        taskRefs,
        outbox,
        renderer,
        deliverer,
        redispatch: { runChain, taskRefs, sources: linearRedispatchSourceLookup(ledgers), instructions, dispatch, outbox },
        dispatch,
        ...(config.staleReservationMs === undefined ? {} : { staleReservationMs: config.staleReservationMs }),
        ...(config.unconfirmedReservationMs === undefined ? {} : { unconfirmedReservationMs: config.unconfirmedReservationMs }),
      }),
      outbox: async () => deliverLinearOutboxTick({
        outbox,
        deliverer,
        ...(config.outboxOwnerId === undefined ? {} : { ownerId: config.outboxOwnerId }),
        ...(config.messagesPerTick === undefined ? {} : { messagesPerTick: config.messagesPerTick }),
      }),
    });

    // Holds the stores, not their verdicts, so every health answer is re-read per call.
    const health: LinearChannelStores = Object.freeze({ inbox, ledgers, conversations, runChain, outbox });

    return {
      inbox, ledgers, installations, conversations, runChain, outbox, oauth, deliverer, renderer, dispatch, ingress, ticks,
      get ready(): boolean {
        // `every` short-circuits: the probe needs only the verdict.
        return oauth.ready && LINEAR_CHANNEL_STORE_NAMES.every((name) => health[name].readyForTraffic());
      },
      diagnose(): LinearChannelDiagnostics {
        // Not short-circuited, so an operator sees every store's verdict.
        const stores = Object.freeze(Object.fromEntries(
          LINEAR_CHANNEL_STORE_NAMES.map((name) => [name, health[name].ready()]),
        ) as Record<LinearChannelStoreName, boolean>);
        const vault = oauth.ready;
        return Object.freeze({
          healthy: vault && LINEAR_CHANNEL_STORE_NAMES.every((name) => stores[name]),
          vault,
          stores,
        });
      },
      close(): void {
        outbox.close();
        runChain.close();
        conversations.close();
        ledgers.close();
        inbox.close();
        installations.close();
      },
    };
  } catch (error) {
    // Unwind in reverse open order; each store constructor closes its own handle on failure.
    for (const store of [...opened].reverse()) {
      try { store.close(); } catch { /* preserve the original construction failure */ }
    }
    throw error;
  }
}



/** The Worker receipt's `activityId` when the rendering carries `preferredActivityId`, else a fresh UUID. */
function linearActivityIdFor(rendered: unknown): string {
  const preferred = (rendered as { readonly preferredActivityId?: unknown } | undefined)?.preferredActivityId;
  return linearActivityId(typeof preferred === "string" ? preferred : undefined);
}

// ---------------------------------------------------------------------------
// Process scheduling
// ---------------------------------------------------------------------------

/** One loop failure. `subject` is `null` when the whole tick threw, else the contained item's correlation label. */
export interface LinearLoopFailure {
  readonly loop: LinearLoopName;
  readonly subject: string | null;
  readonly error: unknown;
}

export interface LinearChannelProcessOptions {
  readonly ticks: LinearLoopTicks;
  readonly intervals: LinearTickIntervals;
  /**
   * Called for every failure a loop produces: a tick that threw and each
   * per-item failure a tick contained. Required, because contained failures
   * are returned rather than thrown and `scheduledLoop` discards return values.
   */
  readonly onTickError: (failure: LinearLoopFailure) => void;
}

export interface LinearChannelProcess {
  /** Disarms all three timers and waits for in-flight ticks, so the stores can be closed right after. */
  close(): Promise<void>;
}

/**
 * Starts all three Linear loops immediately, walking `LINEAR_LOOP_NAMES` so a
 * loop can go unscheduled only if it is also missing from `LinearLoopTicks`.
 */
export function startLinearChannelProcess(options: LinearChannelProcessOptions): LinearChannelProcess {
  const loops: ScheduledLoop[] = LINEAR_LOOP_NAMES.map((name) => scheduledLoop({
    intervalMs: options.intervals[name],
    tick: async () => {
      // Per-item failures are returned, not thrown.
      const result = await options.ticks[name]();
      for (const failure of result.failures) options.onTickError({ loop: name, subject: failure.subject, error: failure.error });
    },
    onError: (error) => options.onTickError({ loop: name, subject: null, error }),
  }));
  for (const loop of loops) loop.start();
  return {
    async close(): Promise<void> {
      await Promise.all(loops.map((loop) => loop.close()));
    },
  };
}
