import {
  deliverOneChannelMessage,
  observeChannelEgressTick,
  reconcileOneChannelMessage,
  type ChannelDeliveryTargetRefresher,
  type ChannelEgressConversation,
  type ChannelEgressConversationSource,
  type ChannelEgressTargetResolution,
  type ChannelEgressTargetResolver,
} from "../channel-egress.js";
import { LINEAR_ACTIVITY_COPY } from "./activity.js";
import { triggerLinearRedispatch, type LinearChannelTaskRefLookup, type LinearRedispatchOutcome, type LinearRedispatchTriggerOptions } from "./redispatch.js";
import type { LinearInboxPort } from "./inbox.js";
import type { SqliteLinearLedgers } from "./ledgers.js";
import type { IntegrityCheckSchedule } from "../integrity-cadence.js";
import { ChannelSessionRefusalError, isChannelRuntimeContractVersion, linearChannelDeliveryTarget } from "../../router/channel-conversation-types.js";
import type {
  ChannelEventRenderer,
  ChannelOutboxDeliverer,
  ChannelOutboxPort,
  ChannelRuntimeTask,
} from "../../router/channel-conversation-types.js";
import type { ChannelCommandKind, ChannelCommandPort } from "../../router/channel-command.js";
import { DEFAULT_UNCONFIRMED_RESERVATION_MS } from "../../router/channel-run-chain.js";
import type { ChannelRunChainSweepResult, SqliteChannelRunChainStore } from "../../router/channel-run-chain.js";
import type { SqliteChannelConversationStore } from "../../router/channel-conversation.js";
import { routerTaskEnvelope } from "../../router/router.js";
import type { Router, RouterListedEvent, RouterTaskRef } from "../../router/router.js";
import { elicitationDigest } from "../../shared/elicitation.js";

/**
 * Linear's observe and outbox loops, sequenced over the channel-neutral core:
 * the run chain, the outbox and egress loop, and Linear's renderer and
 * deliverer. This module owns the order they run in and which conversations
 * each step considers.
 *
 * Linear runs three independently scheduled loops (`LINEAR_LOOP_NAMES`):
 * `ingress` (`./channel.ts`, adapted to `LinearTick` in `./composition.ts`),
 * `observe` (`observeLinearTick`) and `outbox` (`deliverLinearOutboxTick`).
 * Keeping them apart means a Linear API outage stalls posting while `observe`
 * keeps converging local state.
 *
 * Each loop is self-exclusive through `../scheduled-loop.ts`. That matters for
 * `observeLinearTick`: it replays full run histories with no cursor and relies
 * on `recordRunState`'s idempotent no-op, so two concurrent copies would
 * corrupt silently. Across loops there is no application lock: the run chain's
 * single `pending_command_id`, taken under `BEGIN IMMEDIATE`, refuses a racing
 * reservation `reservation_in_flight`, and `observe` and `outbox` never touch
 * the same outbox row in the same state. No tick holds a transaction across an
 * `await`.
 *
 * Run-state observation walks `openRunConversations()`, since a settled Run has
 * no further state to learn. Redispatch and egress walk `allConversations()`:
 * a marker becomes eligible only once its Run settles, and the terminal
 * Activity is delivered after the Run has settled, so the open set would
 * strand both.
 */

// --------------------------------------------------------------------------
// The shared tick shape
// --------------------------------------------------------------------------

/**
 * One item a tick contained rather than threw for. `subject` is a correlation
 * label, never parsed: an `agentSessionId`, the step label
 * `"channel-outbox-reconciliation"`, a swept `commandId`,
 * `"<leaseOwner>:attempt:<index>"`, or (ingress) a delivery id. `error` is the
 * raw thrown value; it reaches a process-level handler, not a user.
 */
export interface LinearTickFailure {
  readonly subject: string;
  readonly error: unknown;
}

/**
 * The minimum every Linear loop tick returns. An empty `failures` means every
 * item this tick touched succeeded -- never "the tick did nothing".
 */
export interface LinearTickResult {
  readonly failures: readonly LinearTickFailure[];
}

/**
 * A Linear loop tick. Nullary, so collaborators are bound once at composition
 * and `scheduledLoop({ tick })` takes it directly.
 */
export type LinearTick<TResult extends LinearTickResult = LinearTickResult> = () => Promise<TResult>;

/** The three loops. A frozen array so a test can `deepEqual` the literals. */
export const LINEAR_LOOP_NAMES = Object.freeze(["ingress", "observe", "outbox"] as const);

export type LinearLoopName = (typeof LINEAR_LOOP_NAMES)[number];

/** The ticks a composition must schedule; a `Record` so a missing or extra loop is a compile error. */
export type LinearLoopTicks = Readonly<Record<LinearLoopName, LinearTick>>;

// --------------------------------------------------------------------------
// Linear's egress seam glue
// --------------------------------------------------------------------------

export { type LinearChannelTaskRefLookup, type LinearRedispatchSource, type LinearRedispatchSourceLookup } from "./redispatch.js";

/**
 * Linear's `ChannelEgressTargetResolver`: a conversation id is an
 * `agentSessionId`, so the target is derived, never looked up. A Linear target
 * has no mutable segment, and a registry read would only turn a missing row
 * into an undelivered Activity. A malformed id throws, which the egress tick
 * records as that conversation's failure. Whether a session is still
 * deliverable is `LinearActivityDeliverer`'s question.
 */
export function linearChannelEgressTargetResolver(): ChannelEgressTargetResolver {
  return {
    async resolveTarget({ conversationId }): Promise<ChannelEgressTargetResolution> {
      return Object.freeze({ disposition: "resolved" as const, target: linearChannelDeliveryTarget({ agentSessionId: conversationId }) });
    },
  };
}

/**
 * Linear's `ChannelDeliveryTargetRefresher`: the identity. The row's stored
 * target already holds every durable field, and the deliverer refuses a target
 * that disagrees with the row's snapshot.
 */
export function linearDeliveryTargetRefresher(): ChannelDeliveryTargetRefresher {
  return {
    async refreshTarget({ target }): Promise<ChannelEgressTargetResolution> {
      return Object.freeze({ disposition: "resolved" as const, target });
    },
  };
}

/** One read of the run chain's Linear conversations, split by whether their Task identity resolved. */
export interface LinearEgressConversationWalk {
  readonly conversations: readonly ChannelEgressConversation[];
  /** Linear conversations the run chain holds whose Router Task identity could not be resolved. */
  readonly unresolved: readonly string[];
}

/**
 * Every Linear conversation the run chain has bound, as egress
 * `{conversationId, taskRef}` pairs, plus those whose Task identity could not
 * be resolved. Filters by provider, since the run chain holds both systems'
 * rows. The task ref is looked up because the run chain lacks `issueId` and
 * `teamId`; an unresolved one is reported in `unresolved`, never fabricated.
 * Not memoized: `observeLinearTick` calls it once and shares the snapshot
 * between redispatch and egress.
 */
export function linearEgressConversationWalk(
  runChain: Pick<SqliteChannelRunChainStore, "allConversations">,
  taskRefs: LinearChannelTaskRefLookup,
): LinearEgressConversationWalk {
  const unresolved: string[] = [];
  const conversations: ChannelEgressConversation[] = [];
  for (const conversation of runChain.allConversations()) {
    if (conversation.scope.system !== "linear") continue;
    const taskRef = taskRefs.taskRef({ conversationId: conversation.conversationId });
    if (taskRef === null) { unresolved.push(conversation.conversationId); continue; }
    conversations.push({ conversationId: conversation.conversationId, taskRef });
  }
  return Object.freeze({ conversations: Object.freeze(conversations), unresolved: Object.freeze(unresolved) });
}


// --------------------------------------------------------------------------
// The observe / redispatch / reconcile tick
// --------------------------------------------------------------------------

/**
 * Every collaborator `observeLinearTick` needs. Collaborators are required so a
 * forgotten one is a compile error at the composition root rather than a
 * silently skipped step. The optional fields are tuning values, plus `inbox`
 * and `ledgers`, which only build the sweep's `retain` predicate;
 * `./composition.ts` passes both.
 */
export interface LinearObserveTickOptions {
  readonly router: Pick<Router, "listTaskEvents">;
  readonly runChain: Pick<SqliteChannelRunChainStore,
    "verifyIntegrity" | "openRunConversations" | "allConversations" | "recordRunState" | "raiseWaitingDecision" | "reconcileStale" | "reconcileSettledEarly"
    // `reservation` reads back the conversation and kind a sweep entry settled.
    | "reservation"
    // `sessionUnconfirmed` tells a late successor Run of a settled unconfirmed
    // command apart from a genuine `unknown_run`.
    | "sessionUnconfirmed">;
  /**
   * The conversation registry, verified at the tick boundary for integrity
   * only; nothing here reads its rows. This tick is that store's periodic full
   * check, so it is required.
   */
  readonly conversations: Pick<SqliteChannelConversationStore, "verifyIntegrity">;
  /** How often the boundary check runs. Omitted, it runs every tick: more CPU, never a lost check. */
  readonly integrity?: IntegrityCheckSchedule;
  readonly taskRefs: LinearChannelTaskRefLookup;
  readonly outbox: Pick<ChannelOutboxPort, "cursor" | "recordProjectedBatch" | "ambiguousMessages" | "recordDisposition" | "touchReconciliationAttempt"
    // `recordDirectMessage` enqueues the terminal answer for a swept command,
    // idempotent on `(conversationId, sourceEventId)`.
    | "recordDirectMessage">;
  /** `LinearEventRenderer`, typed as the neutral port so this tick cannot reach for a Linear-only field. */
  readonly renderer: ChannelEventRenderer;
  /** `LinearActivityDeliverer`, for ambiguous-Activity reconciliation only; delivery is the `outbox` loop's. */
  readonly deliverer: Pick<ChannelOutboxDeliverer, "reconcile">;
  /** Everything `triggerLinearRedispatch` needs, minus the conversation it is called for. */
  readonly redispatch: LinearRedispatchTriggerOptions;
  /** The command port `reconcileSettledEarly` and `reconcileStale` converge stranded reservations through. */
  readonly dispatch: ChannelCommandPort;
  /** Passed to `reconcileStale`, and the window `reconcileSettledEarly` stays inside. */
  readonly staleReservationMs?: number;
  /**
   * How long a reservation may answer `unknown` before the sweep settles it
   * `unconfirmed`. Defaults to `DEFAULT_UNCONFIRMED_RESERVATION_MS`; this tick
   * always passes a value, since `reconcileStale` only runs the unconfirmed
   * sweep when given one.
   */
  readonly unconfirmedReservationMs?: number;
  /** With `ledgers`, builds `retain`: the inbox state of a reservation's dispatching delivery. */
  readonly inbox?: Pick<LinearInboxPort, "snapshot">;
  /**
   * With `inbox`, builds `retain`: maps a reservation's `commandId` to the
   * delivery that dispatched it. A redispatch's own `commandId` has no session
   * input and maps to nothing.
   */
  readonly ledgers?: Pick<SqliteLinearLedgers, "sessionInputByCommand">;
}

export interface LinearObserveTickResult extends LinearTickResult {
  /** Linear conversations the run-state observation walked, after the provider filter. */
  readonly conversationsObserved: number;
  /** `recordRunState` calls that applied, excluding the replay conflicts this tick swallows by design. */
  readonly eventsApplied: number;
  /** How many conversations the egress projection walked, after the provider filter and task-ref resolution. */
  readonly conversationsProjected: number;
  /**
   * Rows the egress projection enqueued. Equals events projected, since
   * `LinearEventRenderer#render` throws rather than returning `null`.
   */
  readonly activitiesEnqueued: number;
  /** Conversations whose Router Task identity could not be resolved, so egress skipped them. Never silently empty-on-failure. */
  readonly unresolvedTaskRefs: readonly string[];
  /** One entry per conversation a redispatch was attempted for, in walk order. */
  readonly redispatches: readonly { readonly conversationId: string; readonly outcome: LinearRedispatchOutcome }[];
  /**
   * The ambiguous row this tick's reconciliation attempted, or `null`. Exact:
   * it is read with no `await` before `reconcileOneChannelMessage` reads the
   * same head row.
   */
  readonly reconciledOutboxId: string | null;
  /**
   * What the stranded-reservation sweep did, or `null` when it threw (then
   * `sweepFailure` is set); `{examined: 0}` means it ran and found nothing.
   * The sweep never invents a Run identity: `accepted_unresolved` and
   * `unknown` stay held.
   */
  readonly sweep: ChannelRunChainSweepResult | null;
  /** The sweep threw. Recorded rather than thrown, for the same reason a single conversation's failure is. */
  readonly sweepFailure?: unknown;
  /**
   * What `reconcileSettledEarly` settled inside the sweep window because Router
   * already answers with lineage, or `null` when it threw (then
   * `earlySettleFailure` is set). Lists settled entries only.
   */
  readonly earlySettle: ChannelRunChainSweepResult | null;
  /** The early settle threw. Recorded rather than thrown, and the sweep still runs after it. */
  readonly earlySettleFailure?: unknown;
}

/**
 * The `sourceEventId` prefix of every "this command never reached Router"
 * Activity, for all three command kinds; the value is a persisted dedup key.
 * The key is the swept `commandId` and nothing per-tick: the sweep runs on
 * every observe tick, so a per-tick key would post one Activity per tick.
 *
 * Exactly once: `reconcileStale` reports `absent` once per command (the row is
 * durably settled `rejected`), and `recordDirectMessage` absorbs any repeat.
 * Not covered: a crash between the run-chain settle and this enqueue (two
 * databases), or a later sweep entry throwing so the whole sweep result is
 * lost. Both leave the user without the Activity, never with a false one.
 */
const ABSENT_START_SOURCE_EVENT_PREFIX = "axis.start.absent:";

/**
 * What a swept-`absent` command of each kind is told. A total `Record`, so a
 * new `ChannelCommandKind` cannot inherit a sentence by default. `./channel.ts`
 * enqueues the same copy when it decides `absent` mid-tick.
 */
const ABSENT_COMMAND_COPY: Readonly<Record<ChannelCommandKind, string | null>> = Object.freeze({
  start: LINEAR_ACTIVITY_COPY.startNeverReachedRouter,
  continue: LINEAR_ACTIVITY_COPY.continueNeverReachedRouter,
  cancel: LINEAR_ACTIVITY_COPY.stopNeverReachedRouter,
});

/** The unconfirmed sweep's per-kind terminal copy; every kind gets an answer. */
const UNCONFIRMED_COMMAND_COPY: Readonly<Record<ChannelCommandKind, string>> = Object.freeze({
  start: LINEAR_ACTIVITY_COPY.startOutcomeUnconfirmed,
  continue: LINEAR_ACTIVITY_COPY.continueOutcomeUnconfirmed,
  cancel: LINEAR_ACTIVITY_COPY.stopOutcomeUnconfirmed,
});

/**
 * The `sourceEventId` prefix for unconfirmed-sweep terminal Activities, keyed
 * on the swept `commandId` like `ABSENT_START_SOURCE_EVENT_PREFIX`. Separate so
 * the two endings can never collide on one outbox key.
 */
const UNCONFIRMED_COMMAND_SOURCE_EVENT_PREFIX = "axis.command.unconfirmed:";

/**
 * Tells the user about every command this tick's sweep settled to a decided
 * ending: `absent` (Router holds no command row, so it never reached Router)
 * or `unconfirmed` (Axis stopped waiting; the copy claims nothing about
 * whether it ran). The ingress tick that dispatched the command has already
 * completed its delivery, so only this pass learns the ending. Other
 * dispositions are undecided or really dispatched, and the Run's own events
 * answer those.
 *
 * Filters to Linear conversations, since `reconcileStale` spans the whole run
 * chain; the `allConversations()` read happens only when there is something to
 * answer. Failures are contained per entry, because each reservation is
 * already settled.
 */
function answerAbsentLinearStarts(
  options: LinearObserveTickOptions,
  sweep: ChannelRunChainSweepResult,
  failures: LinearTickFailure[],
): void {
  const decided = sweep.entries.filter((entry) => entry.disposition === "absent" || entry.disposition === "unconfirmed");
  if (decided.length === 0) return;
  const linearConversations = new Set(options.runChain.allConversations()
    .filter((conversation) => conversation.scope.system === "linear")
    .map((conversation) => conversation.conversationId));
  for (const entry of decided) {
    try {
      const reservation = options.runChain.reservation({ commandId: entry.commandId });
      if (reservation === null) continue;
      const body = entry.disposition === "absent" ? ABSENT_COMMAND_COPY[reservation.kind] : UNCONFIRMED_COMMAND_COPY[reservation.kind];
      if (body === null) continue;
      if (!linearConversations.has(reservation.conversationId)) continue;
      const sourceEventPrefix = entry.disposition === "absent" ? ABSENT_START_SOURCE_EVENT_PREFIX : UNCONFIRMED_COMMAND_SOURCE_EVENT_PREFIX;
      // The conversation id is the `agentSessionId`, a Linear Activity's whole address.
      options.outbox.recordDirectMessage({
        conversationId: reservation.conversationId,
        target: linearChannelDeliveryTarget({ agentSessionId: reservation.conversationId }),
        sourceEventId: `${sourceEventPrefix}${entry.commandId}`,
        content: { type: "error", body },
      });
    } catch (error) {
      failures.push({ subject: entry.commandId, error });
    }
  }
}

const EVENTS_PAGE_LIMIT = 100;

/** Every event Router holds for one Linear Task, oldest first, paged past `Router#listTaskEvents`'s per-call ceiling. */
function allTaskEvents(router: Pick<Router, "listTaskEvents">, taskRef: RouterTaskRef): readonly RouterListedEvent[] {
  const events: RouterListedEvent[] = [];
  let afterCursor = 0;
  for (;;) {
    const page = router.listTaskEvents({ ...taskRef, afterCursor, limit: EVENTS_PAGE_LIMIT });
    events.push(...page);
    if (page.length < EVENTS_PAGE_LIMIT) return events;
    afterCursor = page[page.length - 1]!.cursor;
  }
}

/**
 * Runs one observe tick, in this order:
 *
 * 1. Run-chain observation over `openRunConversations()`.
 * 2. One redispatch attempt per conversation in `allConversations()`.
 * 3. Egress projection over `allConversations()`.
 * 4. Exactly one ambiguous-Activity reconciliation.
 * 5. Early settle, the stranded-reservation sweep, and answers for the sweep's
 *    decided endings.
 *
 * Observation precedes redispatch because a marker is eligible only once the
 * interrupted Run's terminal state is recorded. Redispatch precedes egress so
 * its `accepted` event is projected this tick. The sweep runs last because
 * `reconcileStale` spans all conversations and should see this tick's
 * `recordRunState` updates.
 *
 * A conversation that throws is recorded in `failures` and the rest continue;
 * every conversation is walked each tick, so none can starve another. Within
 * one, `run_state_conflict` and `run_superseded` are expected from the
 * cursor-less full-history replay and are swallowed, except a
 * `run_state_conflict` on a waiting envelope, which is reported.
 */
export async function observeLinearTick(options: LinearObserveTickOptions): Promise<LinearObserveTickResult> {
  // The full invariant check for both stores, at most once per
  // `options.integrity` interval, behind one gate so they share a schedule. A
  // conversation-registry fault is captured and rethrown at the end, so it
  // cannot abort work that never reads that store; the rethrow is what reaches
  // `onTickError`. `due()` is called once because `true` records the check.
  let conversationsFault: { readonly error: unknown } | undefined;
  if (options.integrity === undefined || options.integrity.due()) {
    try { options.conversations.verifyIntegrity(); } catch (error) { conversationsFault = { error }; }
    options.runChain.verifyIntegrity();
  }

  const failures: LinearTickFailure[] = [];
  const redispatches: { readonly conversationId: string; readonly outcome: LinearRedispatchOutcome }[] = [];
  let eventsApplied = 0;

  const open = options.runChain.openRunConversations().filter((conversation) => conversation.scope.system === "linear");
  for (const conversation of open) {
    try {
      const taskRef = options.taskRefs.taskRef({ conversationId: conversation.conversationId });
      if (taskRef === null) throw new Error(`Linear observe tick could not resolve a Router Task for conversation ${conversation.conversationId}.`);
      for (const listed of allTaskEvents(options.router, taskRef)) {
        const runtime = routerTaskEnvelope(listed.envelope);
        // Checked here so the refusal names the version instead of surfacing as
        // the run chain's bare malformed-input `TypeError`. The predicate is
        // derived from the `ChannelRuntimeTask` union, so it tracks the run
        // chain's admitted set.
        if (!isChannelRuntimeContractVersion(runtime.task.contractVersion)) {
          throw new Error(`Linear observe cannot record a Runtime ${String(runtime.task.contractVersion)} Task: the neutral run chain does not admit that contract version.`);
        }
        const task = runtime.task as unknown as ChannelRuntimeTask;
        try {
          options.runChain.recordRunState({ conversationId: conversation.conversationId, task, runId: runtime.run.runId, state: runtime.run.state });
          eventsApplied += 1;
          // `recordRunState` moves only the Run's state; this tick raises the
          // run chain's decision row from the elicitation the waiting event
          // carried, and `raiseWaitingDecision` is idempotent on a repeat
          // digest. The outbox records the elicitation itself. The decision's
          // owner is resolved later through the Run's `commandId`
          // (`linearDecisionOwner` in `./ledgers.ts`), never the session's
          // current owner.
          if (runtime.envelopeType === "task.waiting_for_input") {
            options.runChain.raiseWaitingDecision({ runId: runtime.run.runId, elicitationDigest: elicitationDigest(runtime.elicitation) });
          }
        } catch (error) {
          // A `run_state_conflict` on a waiting envelope is reported, not
          // swallowed: swallowing it would leave a question shown with no
          // decision row and the Run still `running`, so a later reply would
          // interrupt the Run instead of answering it. An honest replay cannot
          // reach it: a repeat waiting event is a same-state no-op in
          // `recordRunState`, a superseded Run raises `run_superseded`, and the
          // backward-transition replay is an `accepted` envelope.
          if (error instanceof ChannelSessionRefusalError && error.code === "run_state_conflict" && runtime.envelopeType === "task.waiting_for_input") {
            throw new Error(`Linear observe could not record a waiting Run for a Runtime ${String(runtime.task.contractVersion)} Task: the run chain refused the transition with run_state_conflict, so its decision row was not raised.`);
          }
          if (error instanceof ChannelSessionRefusalError && (error.code === "run_state_conflict" || error.code === "run_superseded")) continue;
          // On a guarded conversation, `unknown_run` is the late successor Run
          // of a command already settled `unconfirmed`: it has no local Run row,
          // and late evidence is not reconciled into the run chain (egress still
          // projects its output). Without this skip it would fail every tick.
          if (error instanceof ChannelSessionRefusalError && error.code === "unknown_run" && options.runChain.sessionUnconfirmed({ conversationId: conversation.conversationId })) continue;
          throw error;
        }
      }
    } catch (error) {
      failures.push({ subject: conversation.conversationId, error });
    }
  }

  const walk = linearEgressConversationWalk(options.runChain, options.taskRefs);

  for (const conversation of walk.conversations) {
    try {
      redispatches.push({
        conversationId: conversation.conversationId,
        outcome: await triggerLinearRedispatch(options.redispatch, { conversationId: conversation.conversationId }),
      });
    } catch (error) {
      failures.push({ subject: conversation.conversationId, error });
    }
  }

  // Egress walks the same snapshot as the redispatch pass, so
  // `unresolvedTaskRefs` describes one read.
  const egress = await observeChannelEgressTick({
    router: options.router,
    runChain: { allConversations: () => walk.conversations },
    resolver: linearChannelEgressTargetResolver(),
    outbox: options.outbox,
    renderer: options.renderer,
  });
  for (const failure of egress.failures) failures.push({ subject: failure.conversationId, error: failure.error });

  // `reconcileOneChannelMessage` rotates a non-resolving row through
  // `touchReconciliationAttempt`, so rows behind an unresolvable one are reached.
  const reconciledOutboxId = options.outbox.ambiguousMessages()[0]?.id ?? null;
  try {
    await reconcileOneChannelMessage({ outbox: options.outbox, deliverer: options.deliverer });
  } catch (error) {
    failures.push({ subject: "channel-outbox-reconciliation", error });
  }

  // Settles what Router can already answer with lineage while still inside the
  // sweep window, so the two passes never consider the same reservation in one
  // tick. A lost dispatch response often resolves only once the Run's first
  // event reaches Router; this frees the slot without waiting for the sweep.
  let earlySettle: ChannelRunChainSweepResult | null = null;
  let earlySettleFailure: unknown;
  let earlySettleThrew = false;
  try {
    earlySettle = await options.runChain.reconcileSettledEarly({
      dispatch: options.dispatch,
      ...(options.staleReservationMs === undefined ? {} : { staleAfterMs: options.staleReservationMs }),
    });
  } catch (error) { earlySettleFailure = error; earlySettleThrew = true; }

  // `retain` keeps the sweep off a reservation whose dispatching delivery is
  // still `accepted` or `processing` in the inbox, since that delivery may yet
  // reclaim it. A `commandId` with no session input (a redispatch's own) is
  // not retained.
  const retain: ((commandId: string) => boolean) | undefined = options.inbox === undefined || options.ledgers === undefined ? undefined
    : (commandId: string): boolean => {
      const sourceEventId = options.ledgers?.sessionInputByCommand(commandId)?.sourceEventId;
      if (sourceEventId === undefined) return false;
      const state = options.inbox?.snapshot(sourceEventId)?.state;
      return state === "accepted" || state === "processing";
    };

  let sweep: ChannelRunChainSweepResult | null = null;
  let sweepFailure: unknown;
  let sweepThrew = false;
  try {
    sweep = await options.runChain.reconcileStale({
      dispatch: options.dispatch,
      ...(options.staleReservationMs === undefined ? {} : { staleAfterMs: options.staleReservationMs }),
      // Always concrete: an absent `unconfirmedAfterMs` opts out of the unconfirmed sweep.
      unconfirmedAfterMs: options.unconfirmedReservationMs ?? DEFAULT_UNCONFIRMED_RESERVATION_MS,
      ...(retain === undefined ? {} : { retain }),
    });
  } catch (error) { sweepFailure = error; sweepThrew = true; }

  // The sweep's decided endings become the user's terminal answer, before the
  // boundary rethrow so a registry fault cannot cost a swept command its answer.
  if (sweep !== null) answerAbsentLinearStarts(options, sweep, failures);

  // The registry fault captured at the top, surfaced now that the work is done.
  if (conversationsFault !== undefined) throw conversationsFault.error;
  return Object.freeze({
    conversationsObserved: open.length,
    eventsApplied,
    conversationsProjected: egress.conversationsObserved,
    activitiesEnqueued: egress.eventsDelivered,
    unresolvedTaskRefs: walk.unresolved,
    redispatches: Object.freeze(redispatches),
    reconciledOutboxId,
    earlySettle,
    ...(earlySettleThrew ? { earlySettleFailure } : {}),
    sweep,
    ...(sweepThrew ? { sweepFailure } : {}),
    failures: Object.freeze(failures),
  });
}

// --------------------------------------------------------------------------
// The outbox delivery tick
// --------------------------------------------------------------------------

/**
 * The lease owner Linear's outbox records against a claimed row: a stable
 * literal, so a stuck lease is attributable to this loop.
 */
export const LINEAR_OUTBOX_LEASE_OWNER = "linear-relay";

/** Messages one `deliverLinearOutboxTick` claims and attempts unless the caller says otherwise. */
const DEFAULT_MESSAGES_PER_TICK = 1;

export interface LinearOutboxTickOptions {
  readonly outbox: Pick<ChannelOutboxPort, "claim" | "recordDisposition" | "retry">;
  /** `LinearActivityDeliverer`. */
  readonly deliverer: Pick<ChannelOutboxDeliverer, "create">;
  /** Defaults to `LINEAR_OUTBOX_LEASE_OWNER`. Overridable so two processes can hold distinguishable leases. */
  readonly ownerId?: string;
  /** Attempts per tick, default 1. */
  readonly messagesPerTick?: number;
}

export interface LinearOutboxTickResult extends LinearTickResult {
  /**
   * Delivery attempts this tick made, bounded by `messagesPerTick`. An attempt
   * that found nothing due still counts: `deliverOneChannelMessage` returns
   * `void` either way.
   */
  readonly attempts: number;
}

/**
 * Runs one outbox delivery tick: up to `messagesPerTick` calls to
 * `deliverOneChannelMessage`, which owns claim, refresh, attempt, retry and
 * backoff. This adds Linear's lease owner and refresher, and contains each
 * attempt's failure so one bad row does not end the tick. Reconciliation runs
 * in the `observe` tick, so an API outage stalls posting without also
 * stalling recovery.
 */
export async function deliverLinearOutboxTick(options: LinearOutboxTickOptions): Promise<LinearOutboxTickResult> {
  const failures: LinearTickFailure[] = [];
  const budget = options.messagesPerTick ?? DEFAULT_MESSAGES_PER_TICK;
  // Not injectable: the identity is the only correct Linear refresher.
  const refresher = linearDeliveryTargetRefresher();
  const ownerId = options.ownerId ?? LINEAR_OUTBOX_LEASE_OWNER;
  let attempts = 0;
  for (let index = 0; index < budget; index += 1) {
    attempts += 1;
    try {
      await deliverOneChannelMessage({
        outbox: options.outbox,
        deliverer: options.deliverer,
        refresher,
        ownerId,
      });
    } catch (error) {
      failures.push({ subject: `${ownerId}:attempt:${index}`, error });
    }
  }
  return Object.freeze({ attempts, failures: Object.freeze(failures) });
}
