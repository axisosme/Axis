import {
  type ChannelDeliveryTarget,
  type ChannelEventRenderer,
  type ChannelOutboxDeliverer,
  type ChannelOutboxPort,
  type ChannelRenderedMessage,
} from "../router/channel-conversation-types.js";
import { buildRouterRelayNeutralEvent, type RouterRelayNeutralEventEnvelope } from "../shared/router-relay-event.js";
import { channelBackoffDelayMs } from "./channel-outbox.js";
import { routerTaskEnvelope } from "../router/router.js";
import type { Router, RouterListedEvent, RouterTaskRef } from "../router/router.js";

/**
 * Channel-neutral egress: three independently scheduled steps.
 *
 * - `observeChannelEgressTick` enqueues Router events past the outbox cursor.
 * - `deliverOneChannelMessage` claims and attempts one message.
 * - `reconcileOneChannelMessage` resolves one ambiguous message.
 *
 * `Router#listTaskEvents` is the only event source; channel specifics sit
 * behind the renderer, deliverer, resolver and outbox ports. Every bound
 * conversation is checked on every tick, because a settled Run does not close
 * a conversation; the cursor comparison keeps a fully delivered one cheap.
 */

export type ChannelEgressTargetResolution =
  | { readonly disposition: "resolved"; readonly target: ChannelDeliveryTarget }
  /** The conversation's channel binding no longer exists. Terminal: retrying cannot make it reappear. */
  | { readonly disposition: "unregistered"; readonly safeCode: string }
  /** The target could not be resolved right now, for a reason that says nothing about whether it exists. Retriable. */
  | { readonly disposition: "unavailable"; readonly safeCode: string };

/** Resolves a conversation's current delivery target for the egress tick. */
export interface ChannelEgressTargetResolver {
  resolveTarget(input: { readonly conversationId: string }): Promise<ChannelEgressTargetResolution>;
}

/**
 * Refreshes the mutable path segments of a target an outbox row already
 * carries, without consulting the conversation registry. A channel with no
 * mutable segment (Linear) implements this as the identity.
 */
export interface ChannelDeliveryTargetRefresher {
  refreshTarget(input: { readonly target: ChannelDeliveryTarget }): Promise<ChannelEgressTargetResolution>;
}

/** One conversation this tick should check, and the Router Task its events are read from. */
export interface ChannelEgressConversation {
  readonly conversationId: string;
  readonly taskRef: RouterTaskRef;
}

/** Every conversation a channel has ever bound. */
export interface ChannelEgressConversationSource {
  allConversations(): readonly ChannelEgressConversation[];
}

export interface ChannelEgressTickOptions {
  readonly router: Pick<Router, "listTaskEvents">;
  readonly runChain: ChannelEgressConversationSource;
  readonly resolver: ChannelEgressTargetResolver;
  readonly outbox: Pick<ChannelOutboxPort, "cursor" | "recordProjectedBatch">;
  readonly renderer: ChannelEventRenderer;
}

export interface ChannelEgressFailure {
  readonly conversationId: string;
  readonly error: unknown;
}

export interface ChannelEgressTickResult {
  readonly conversationsObserved: number;
  readonly eventsDelivered: number;
  readonly failures: readonly ChannelEgressFailure[];
}

const EVENTS_PAGE_LIMIT = 100;

/**
 * The opaque conversation id of a neutral event, whichever provider named it.
 * Mirrors the private `conversationIdentityId` in `../shared/router-relay-event.ts`.
 */
function neutralConversationId(conversation: RouterRelayNeutralEventEnvelope["conversation"]): string {
  return conversation.agentSessionId;
}

/** Every event Router holds for one Task past a cursor, oldest first, paged past `Router#listTaskEvents`'s per-call ceiling. */
function eventsAfter(router: Pick<Router, "listTaskEvents">, taskRef: RouterTaskRef, afterCursor: number): readonly RouterListedEvent[] {
  const events: RouterListedEvent[] = [];
  let cursor = afterCursor;
  for (;;) {
    const page = router.listTaskEvents({ ...taskRef, afterCursor: cursor, limit: EVENTS_PAGE_LIMIT });
    events.push(...page);
    if (page.length < EVENTS_PAGE_LIMIT) return events;
    cursor = page[page.length - 1]!.cursor;
  }
}

/**
 * Runs one egress tick: for each bound conversation, renders and enqueues the
 * Router events past the outbox cursor. `recordProjectedBatch` requires a
 * gapless sequence starting at `expectedCursor + 1`.
 *
 * A failing conversation, including an unresolved target or a stale cursor,
 * is recorded in `failures` and does not stop the others.
 */
export async function observeChannelEgressTick(options: ChannelEgressTickOptions): Promise<ChannelEgressTickResult> {
  const conversations = options.runChain.allConversations();
  let eventsDelivered = 0;
  const failures: ChannelEgressFailure[] = [];
  for (const conversation of conversations) {
    try {
      const resolution = await options.resolver.resolveTarget({ conversationId: conversation.conversationId });
      if (resolution.disposition !== "resolved") {
        throw new Error(`Channel egress tick could not resolve a delivery target for conversation ${conversation.conversationId}: ${resolution.disposition} (${resolution.safeCode}).`);
      }
      const expectedCursor = options.outbox.cursor(conversation.conversationId);
      const listed = eventsAfter(options.router, conversation.taskRef, expectedCursor);
      if (listed.length === 0) continue;
      const events = listed.map((entry) => {
        const runtime = routerTaskEnvelope(entry.envelope);
        const neutral = buildRouterRelayNeutralEvent(entry.envelope, runtime.messageId, entry.cursor);
        // `recordProjectedBatch` trusts the conversation it is given, so a
        // mismatched event would be delivered to another conversation's target.
        const listedConversationId = neutralConversationId(neutral.conversation);
        if (listedConversationId !== conversation.conversationId) {
          throw new Error(`Channel egress tick listed an event for conversation ${listedConversationId} while processing ${conversation.conversationId}.`);
        }
        // A `null` render is passed through, not filtered, so the cursor still
        // advances past events this channel does not post.
        const rendered: ChannelRenderedMessage | null = options.renderer.render(neutral);
        return { axisSequence: neutral.axisSequence, sourceEventId: neutral.eventId, rendered };
      });
      const recorded = options.outbox.recordProjectedBatch({
        conversationId: conversation.conversationId,
        target: resolution.target,
        expectedCursor,
        events,
      });
      if (!recorded) throw new Error(`Channel egress tick's batch for conversation ${conversation.conversationId} was rejected by a stale expectedCursor.`);
      eventsDelivered += events.length;
    } catch (error) {
      failures.push({ conversationId: conversation.conversationId, error });
    }
  }
  return Object.freeze({ conversationsObserved: conversations.length, eventsDelivered, failures: Object.freeze(failures) });
}

export interface ChannelDeliveryOptions {
  readonly outbox: Pick<ChannelOutboxPort, "claim" | "recordDisposition" | "retry">;
  readonly deliverer: Pick<ChannelOutboxDeliverer, "create">;
  readonly refresher: ChannelDeliveryTargetRefresher;
  /** The lease owner persisted on a claimed row; required so each channel names its own. */
  readonly ownerId: string;
  readonly leaseMs?: number;
}

/**
 * Claims and attempts exactly one pending message. A no-op if none is due.
 *
 * The target is refreshed just before the attempt, since the row's copy is an
 * enqueue-time snapshot. An `unavailable` target and a `retryable` result both
 * retry with backoff and no ceiling here: a transient credential failure can
 * recover on its own, and any attempt cap is the deliverer's decision (it
 * receives `attemptCount`). The backoff schedule is shared by all channels.
 */
export async function deliverOneChannelMessage(options: ChannelDeliveryOptions): Promise<void> {
  const record = options.outbox.claim(options.ownerId, options.leaseMs);
  if (record === null) return;
  const resolution = await options.refresher.refreshTarget({ target: record.target });
  if (resolution.disposition === "unregistered") {
    options.outbox.recordDisposition(record.id, { disposition: "rejected", safeCode: resolution.safeCode });
    return;
  }
  if (resolution.disposition !== "resolved") {
    options.outbox.retry(record.id, resolution.safeCode, channelBackoffDelayMs(record.attemptCount));
    return;
  }
  const result = await options.deliverer.create({ target: resolution.target, record });
  if (result.disposition === "confirmed") {
    options.outbox.recordDisposition(record.id, { disposition: "confirmed", providerMessageId: result.providerMessageId });
    return;
  }
  if (result.disposition === "retryable") {
    options.outbox.retry(record.id, result.safeCode, channelBackoffDelayMs(record.attemptCount));
    return;
  }
  options.outbox.recordDisposition(record.id, { disposition: result.disposition, safeCode: result.safeCode });
}

export interface ChannelReconciliationOptions {
  readonly outbox: Pick<ChannelOutboxPort, "ambiguousMessages" | "recordDisposition" | "touchReconciliationAttempt">;
  readonly deliverer: Pick<ChannelOutboxDeliverer, "reconcile">;
}

/**
 * Reconciles exactly one ambiguous message. A no-op if none is ambiguous.
 *
 * Uses the row's stored target, because it must look where the earlier
 * attempt may have posted; a stale path fails safe into `unresolved`.
 * `unresolved` never re-posts (that risks a duplicate) but touches the row so
 * the least-recently-attempted ordering moves on to other rows.
 */
export async function reconcileOneChannelMessage(options: ChannelReconciliationOptions): Promise<void> {
  const record = options.outbox.ambiguousMessages()[0];
  if (record === undefined) return;
  const result = await options.deliverer.reconcile({ target: record.target, record });
  if (result.disposition === "confirmed") {
    options.outbox.recordDisposition(record.id, { disposition: "confirmed", providerMessageId: result.providerMessageId });
    return;
  }
  if (result.disposition === "conflict") {
    options.outbox.recordDisposition(record.id, { disposition: "conflict", safeCode: result.safeCode });
    return;
  }
  options.outbox.touchReconciliationAttempt(record.id);
}
