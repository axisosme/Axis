/**
 * Channel port doubles for testing callers. InMemoryChannelConversationRegistry and
 * InMemoryChannelOutbox model atomic open-or-resume, replay detection, projection
 * ordering, lease expiry into ambiguous and least-recently-attempted reconciliation
 * ordering. RecordingChannelRunChain only records calls and returns scripted results.
 *
 * These doubles do not validate SQLite schemas, migrations, transactions, competing
 * writers or the full run-chain state machine. Test storage and run-chain semantics
 * against their real stores. Injected clocks and IDs keep caller assertions
 * deterministic.
 */

import {
  CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
  ChannelSessionInputError,
  ChannelSessionRefusalError,
  channelConversationScope,
  channelDeliveryTarget,
  channelInstructionDigest,
  channelConversationScopeKey,
  channelMessageUniquenessDomain,
  channelRedispatchEligible,
  linearChannelConversationScope,
  linearChannelDeliveryTarget,
  type ChannelConversation,
  type ChannelConversationRegistryPort,
  type ChannelConversationResolution,
  type ChannelConversationScope,
  type ChannelDeliveryCreateResult,
  type ChannelDeliveryReconcileResult,
  type ChannelDeliveryTarget,
  type ChannelDigest,
  type ChannelEventRenderer,
  type ChannelInboundDeliveryReceipt,
  type ChannelMessageContent,
  type ChannelMessageInvocation,
  type ChannelMessageInvocationReceipt,
  type ChannelOutboxDeliverer,
  type ChannelOutboxPort,
  type ChannelOutboxRecord,
  type ChannelOutboxState,
  type ChannelRedispatchInstructionFetcher,
  type ChannelRedispatchInstructionResult,
  type ChannelRedispatchMarker,
  type ChannelRenderedMessage,
  type ChannelRunChainEntry,
  type ChannelRunChainPort,
  type ChannelRunChainReconcileOutcome,
  type ChannelRunChainReservation,
  type ChannelRunChainReservationReceipt,
  type ChannelRunChainReservationStatus,
  type ChannelRuntimeTask,
  type ChannelWaitingDecision,
  type ChannelWaitingDecisionReceipt,
} from "../../src/router/channel-conversation-types.js";
import type { ChannelCommandKind, ChannelCommandOutcome } from "../../src/router/channel-command.js";
import type { TenantRef } from "../../src/router/channel-identity.js";
import { linearTenantRef } from "../../src/router/channel-identity.js";
import type { RuntimeState } from "@axisosme/axis-contracts/runtime";
import type { RouterRelayNeutralEventEnvelope } from "../../src/shared/router-relay-event.js";

// --------------------------------------------------------------------------
// Fixtures
// --------------------------------------------------------------------------

export const linearScopeFixture = linearChannelConversationScope({
  organizationId: "organization-linear-1",
  agentSessionId: "agent-session-linear-1",
});
export const linearTargetFixture = linearChannelDeliveryTarget({ agentSessionId: "agent-session-linear-1" });
export const linearTenantFixture: TenantRef = linearTenantRef("organization-linear-1");
export const validScopeFixtures: readonly ChannelConversationScope[] = Object.freeze([
  linearScopeFixture,
  linearChannelConversationScope({ organizationId: "org_141", agentSessionId: "session_141" }),
]);
export const validTargetFixtures: readonly ChannelDeliveryTarget[] = Object.freeze([linearTargetFixture]);

export const invalidScopeFixtures: readonly { readonly reason: string; readonly value: unknown }[] = Object.freeze([
  { reason: "no system discriminator", value: { organizationId: "org-1", agentSessionId: "session-1" } },
  { reason: "unknown system", value: { system: "slack", channelId: "C1" } },
  { reason: "linear missing agentSessionId", value: { system: "linear", organizationId: "org-1" } },
  { reason: "linear extra field", value: { system: "linear", organizationId: "org-1", agentSessionId: "session-1", teamId: "team-1" } },
  { reason: "linear identifier carries the scope-key separator", value: { system: "linear", organizationId: "org:1", agentSessionId: "session-1" } },
  { reason: "linear identifier is empty", value: { system: "linear", organizationId: "", agentSessionId: "session-1" } },
  { reason: "not an object", value: "linear:org-1:session-1" },
  { reason: "an array", value: ["linear", "org-1", "session-1"] },
  { reason: "null", value: null },
]);

/** Values a delivery-target validator must refuse. */
export const invalidTargetFixtures: readonly { readonly reason: string; readonly value: unknown }[] = Object.freeze([
  { reason: "no system discriminator", value: { agentSessionId: "session-1" } },
  { reason: "unknown system", value: { system: "slack", channelId: "C1" } },
  { reason: "linear extra field", value: { system: "linear", agentSessionId: "session-1", organizationId: "org-1" } },
  { reason: "not an object", value: 42 },
]);

/**
 * A digest of the exact form every store in this codebase persists and
 * compares, derived from a seed so a test can name it twice. Goes through
 * `channelInstructionDigest` rather than assembling the string, so a fixture
 * can never be a shape the contract's own validator would reject.
 */
export function digestFixture(seed: string): ChannelDigest {
  return channelInstructionDigest(seed);
}

/** A minimal runtime `Task` for `recordRunState`. Only `taskId` is read by the double. */
export function runtimeTaskFixture(taskId: string): ChannelRuntimeTask {
  return { contractVersion: "1.0.0", taskId, correlationId: `correlation-${taskId}`, sourceRef: { system: "linear", issueId: "issue-1" } } as ChannelRuntimeTask;
}

// --------------------------------------------------------------------------
// Deterministic clock and id source
// --------------------------------------------------------------------------

export interface FixtureClockOptions {
  /** ISO instant the clock starts at. Defaults to a fixed, obviously-synthetic instant. */
  readonly start?: string;
  /** Milliseconds each `now()` advances by. Defaults to 1000. */
  readonly stepMs?: number;
}

/** A clock that advances a fixed step on every read, so ordering assertions are stable. */
export function fixtureClock(options: FixtureClockOptions = {}): () => Date {
  let current = new Date(options.start ?? "2026-01-01T00:00:00.000Z").getTime();
  const step = options.stepMs ?? 1_000;
  return () => {
    const instant = new Date(current);
    current += step;
    return instant;
  };
}

/** A counter-backed id source, so a synthesized `conversationId` is predictable. */
export function fixtureIds(prefix: string): () => string {
  let counter = 0;
  return () => {
    counter += 1;
    return `${prefix}-${counter}`;
  };
}

interface DoubleOptions {
  readonly now?: () => Date;
  readonly id?: () => string;
}

// --------------------------------------------------------------------------
// In-memory conversation registry
// --------------------------------------------------------------------------

/**
 * In-memory `ChannelConversationRegistryPort`.
 *
 * Faithful to `SqliteChannelConversationStore` on the four behaviours a caller
 * can observe: a resumed active conversation reports `opened: false`; a closed
 * one is succeeded by a fresh conversation on the next `openOrResume`; a
 * redelivered `deliveryId` reports `duplicate: true` with the *original*
 * `receivedAt`; and re-presenting a message with a different instruction or
 * conversation refuses `message_rebound` rather than rebinding.
 */
export class InMemoryChannelConversationRegistry implements ChannelConversationRegistryPort {
  readonly #now: () => Date;
  readonly #id: () => string;
  readonly #pointers = new Map<string, string>();
  readonly #conversations = new Map<string, ChannelConversation>();
  readonly #deliveries = new Map<string, string>();
  readonly #invocations = new Map<string, ChannelMessageInvocation>();
  #sequence = 0;

  constructor(options: DoubleOptions = {}) {
    this.#now = options.now ?? fixtureClock();
    this.#id = options.id ?? fixtureIds("conversation");
  }

  openOrResume(input: { readonly scope: ChannelConversationScope }): ChannelConversationResolution {
    const scope = channelConversationScope(input.scope);
    const key = channelConversationScopeKey(scope);
    const pointer = this.#pointers.get(key);
    const existing = pointer === undefined ? undefined : this.#conversations.get(pointer);
    if (existing !== undefined && existing.status === "active") return Object.freeze({ conversation: existing, opened: false });
    this.#sequence += 1;
    const conversation: ChannelConversation = Object.freeze({
      contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
      sequence: this.#sequence,
      scope,
      conversationId: this.#id(),
      status: "active" as const,
      createdAt: this.#now().toISOString(),
    });
    this.#conversations.set(conversation.conversationId, conversation);
    this.#pointers.set(key, conversation.conversationId);
    return Object.freeze({ conversation, opened: true });
  }

  recordDelivery(input: { readonly deliveryId: string }): ChannelInboundDeliveryReceipt {
    const seen = this.#deliveries.get(input.deliveryId);
    if (seen !== undefined) return Object.freeze({ deliveryId: input.deliveryId, receivedAt: seen, duplicate: true });
    const receivedAt = this.#now().toISOString();
    this.#deliveries.set(input.deliveryId, receivedAt);
    return Object.freeze({ deliveryId: input.deliveryId, receivedAt, duplicate: false });
  }

  recordMessageInvocation(input: {
    readonly scope: ChannelConversationScope;
    readonly messageId: string;
    readonly conversationId: string;
    readonly instructionDigest: ChannelDigest;
  }): ChannelMessageInvocationReceipt {
    const scope = channelConversationScope(input.scope);
    // Keyed on the provider's message-uniqueness domain, NOT the full
    // per issue. Keying on the scope diverged from the real store in both
    // directions -- see `channelMessageUniquenessDomain`, which exists because
    // this double got it wrong first.
    const key = `${channelMessageUniquenessDomain(scope)}\0${input.messageId}`;
    const existing = this.#invocations.get(key);
    if (existing !== undefined) {
      if (existing.instructionDigest !== input.instructionDigest || existing.conversationId !== input.conversationId) {
        throw new ChannelSessionRefusalError("message_rebound");
      }
      return Object.freeze({ invocation: existing, replayed: true });
    }
    const conversation = this.#conversations.get(input.conversationId);
    if (conversation === undefined) throw new ChannelSessionRefusalError("unknown_conversation");
    // Compares the uniqueness domain, matching the real store's
    // `conversation.scope.repositoryId !== repositoryId` check. Comparing full
    // scope keys here refused a message whose repository matches but whose
    // issue does not, which the real store accepts.
    if (channelMessageUniquenessDomain(conversation.scope) !== channelMessageUniquenessDomain(scope)) {
      throw new ChannelSessionRefusalError("conversation_scope_mismatch");
    }
    const invocation: ChannelMessageInvocation = Object.freeze({
      contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
      scope,
      messageId: input.messageId,
      conversationId: input.conversationId,
      instructionDigest: input.instructionDigest,
      acceptedAt: this.#now().toISOString(),
    });
    this.#invocations.set(key, invocation);
    return Object.freeze({ invocation, replayed: false });
  }

  closeConversation(input: { readonly conversationId: string }): ChannelConversation {
    const existing = this.#conversations.get(input.conversationId);
    if (existing === undefined) throw new ChannelSessionRefusalError("unknown_conversation");
    if (existing.status === "closed") throw new ChannelSessionRefusalError("conversation_closed");
    const closed: ChannelConversation = Object.freeze({
      ...existing,
      status: "closed" as const,
      closedAt: this.#now().toISOString(),
    });
    this.#conversations.set(closed.conversationId, closed);
    return closed;
  }
}

// --------------------------------------------------------------------------
// Recording run chain stub
// --------------------------------------------------------------------------

/**
 * Recording, caller-scripted ChannelRunChainPort with no command state machine. Use it
 * to verify that callers reserve before dispatch, consult pendingRedispatch and honor
 * eligibility.
 *
 * Use SqliteChannelRunChainStore for command semantics, including waiting versus
 * settled redispatch eligibility, explicit cancel suppression, decision claims and
 * marker survival. Reimplementing those rules here would allow a test and its double to
 * agree on the same incorrect behavior.
 */
export class RecordingChannelRunChain implements ChannelRunChainPort {
  readonly reserved: {
    readonly scope: ChannelConversationScope;
    readonly conversationId: string;
    readonly tenant: TenantRef;
    readonly commandId: string;
    readonly kind: ChannelCommandKind;
    readonly instructionDigest?: ChannelDigest | undefined;
    readonly priorRunId?: string | undefined;
  }[] = [];
  readonly settled: { readonly commandId: string; readonly outcome: ChannelCommandOutcome }[] = [];
  readonly resolvedLocally: string[] = [];
  readonly reconciled: string[] = [];
  readonly runStates: { readonly conversationId: string; readonly runId: string; readonly state: RuntimeState; readonly taskId: string }[] = [];
  readonly raisedDecisions: { readonly runId: string; readonly elicitationDigest: ChannelDigest }[] = [];

  readonly #script: RecordingChannelRunChainScript;

  constructor(script: RecordingChannelRunChainScript = {}) {
    this.#script = script;
  }

  reserve(input: {
    readonly scope: ChannelConversationScope;
    readonly conversationId: string;
    readonly tenant: TenantRef;
    readonly commandId: string;
    readonly kind: ChannelCommandKind;
    readonly instructionDigest?: ChannelDigest;
    readonly priorRunId?: string;
  }): ChannelRunChainReservationReceipt {
    channelConversationScope(input.scope);
    this.reserved.push(input);
    return this.#require("reserve", this.#script.reserve?.(input));
  }

  settle(input: { readonly commandId: string; readonly outcome: ChannelCommandOutcome }): ChannelRunChainReservation {
    // The real store refuses an outcome naming a different command outright,
    // so a caller that mixes them up must not get a pass here either. This is
    // the one invariant this stub enforces, because it is a caller-side bug
    // rather than a store behaviour.
    if (input.outcome.commandId !== input.commandId) throw new ChannelSessionInputError("malformed_conversation_scope");
    this.settled.push(input);
    return this.#require("settle", this.#script.settle?.(input));
  }

  resolveLocally(input: { readonly commandId: string }): ChannelRunChainReservation {
    this.resolvedLocally.push(input.commandId);
    return this.#require("resolveLocally", this.#script.resolveLocally?.(input));
  }

  reconcile(input: { readonly commandId: string }): ChannelRunChainReconcileOutcome {
    this.reconciled.push(input.commandId);
    return this.#script.reconcile?.(input) ?? "absent";
  }

  recordRunState(input: {
    readonly conversationId: string;
    readonly task: ChannelRuntimeTask;
    readonly runId: string;
    readonly state: RuntimeState;
  }): ChannelRunChainEntry {
    this.runStates.push({ conversationId: input.conversationId, runId: input.runId, state: input.state, taskId: input.task.taskId });
    return this.#require("recordRunState", this.#script.recordRunState?.(input));
  }

  raiseWaitingDecision(input: { readonly runId: string; readonly elicitationDigest: ChannelDigest }): ChannelWaitingDecisionReceipt {
    this.raisedDecisions.push(input);
    return this.#require("raiseWaitingDecision", this.#script.raiseWaitingDecision?.(input));
  }

  openDecision(input: { readonly conversationId: string }): ChannelWaitingDecision | null {
    return this.#script.openDecision?.(input) ?? null;
  }

  pendingRedispatch(input: { readonly conversationId: string }): ChannelRedispatchMarker | null {
    return this.#script.pendingRedispatch?.(input) ?? null;
  }

  #require<T>(member: string, value: T | undefined): T {
    if (value === undefined) throw new Error(`RecordingChannelRunChain: ${member} was called but the test scripted no result for it.`);
    return value;
  }
}

/**
 * What a `RecordingChannelRunChain` returns. Every member is optional; a
 * member a test does not script throws when called, rather than returning a
 * plausible-looking default that would let the test pass for the wrong reason.
 * `reconcile`, `openDecision` and `pendingRedispatch` default to the
 * "nothing there" answer, which is the only default that cannot be mistaken
 * for real state.
 */
export interface RecordingChannelRunChainScript {
  readonly reserve?: (input: {
    readonly conversationId: string;
    readonly commandId: string;
    readonly kind: ChannelCommandKind;
  }) => ChannelRunChainReservationReceipt;
  readonly settle?: (input: { readonly commandId: string; readonly outcome: ChannelCommandOutcome }) => ChannelRunChainReservation;
  readonly resolveLocally?: (input: { readonly commandId: string }) => ChannelRunChainReservation;
  readonly reconcile?: (input: { readonly commandId: string }) => ChannelRunChainReconcileOutcome;
  readonly recordRunState?: (input: { readonly conversationId: string; readonly runId: string; readonly state: RuntimeState }) => ChannelRunChainEntry;
  readonly raiseWaitingDecision?: (input: { readonly runId: string; readonly elicitationDigest: ChannelDigest }) => ChannelWaitingDecisionReceipt;
  readonly openDecision?: (input: { readonly conversationId: string }) => ChannelWaitingDecision | null;
  readonly pendingRedispatch?: (input: { readonly conversationId: string }) => ChannelRedispatchMarker | null;
}

/**
 * Builds a settled reservation for a script to return. A convenience, not a
 * model: it fills in the bookkeeping fields so a test can state only the parts
 * it cares about.
 */
export function reservationFixture(input: {
  readonly conversationId: string;
  readonly commandId: string;
  readonly kind: ChannelCommandKind;
  readonly status?: ChannelRunChainReservationStatus;
  readonly sequence?: number;
  readonly runId?: string;
  readonly taskId?: string;
  readonly decisionId?: string;
  readonly priorRunId?: string;
  readonly interruptRunId?: string;
}): ChannelRunChainReservation {
  return Object.freeze({
    contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
    sequence: input.sequence ?? 1,
    conversationId: input.conversationId,
    commandId: input.commandId,
    kind: input.kind,
    intentDigest: digestFixture(`${input.commandId} ${input.kind}`),
    ...(input.priorRunId === undefined ? {} : { priorRunId: input.priorRunId }),
    ...(input.interruptRunId === undefined ? {} : { interruptRunId: input.interruptRunId }),
    ...(input.decisionId === undefined ? {} : { decisionId: input.decisionId }),
    status: input.status ?? "reserved",
    ...(input.runId === undefined ? {} : { runId: input.runId }),
    ...(input.taskId === undefined ? {} : { taskId: input.taskId }),
    reservedAt: "2026-01-01T00:00:00.000Z",
  });
}

/**
 * Builds a redispatch marker whose `eligible` is derived from a current Run
 * state through `channelRedispatchEligible`, so a test cannot accidentally
 * script the unsafe combination the real store refuses to produce.
 */
export function redispatchMarkerFixture(input: {
  readonly conversationId: string;
  readonly commandId: string;
  readonly interruptedRunId: string;
  readonly currentRunState: RuntimeState | null;
  readonly instructionDigest?: ChannelDigest;
}): ChannelRedispatchMarker {
  return Object.freeze({
    contractVersion: CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION,
    conversationId: input.conversationId,
    commandId: input.commandId,
    interruptedRunId: input.interruptedRunId,
    instructionDigest: input.instructionDigest ?? digestFixture(input.commandId),
    eligible: channelRedispatchEligible(input.currentRunState),
    requestedAt: "2026-01-01T00:00:00.000Z",
  });
}

// --------------------------------------------------------------------------
// In-memory outbox
// --------------------------------------------------------------------------

interface OutboxRow {
  readonly id: string;
  readonly conversationId: string;
  readonly target: ChannelDeliveryTarget;
  readonly sourceEventId: string;
  readonly content: ChannelMessageContent;
  readonly contentDigest: ChannelDigest;
  readonly axisSequence: number;
  readonly createdAt: string;
  state: ChannelOutboxState;
  attemptCount: number;
  availableAt: string;
  leaseOwner?: string | undefined;
  leaseExpiresAt?: string | undefined;
  reconcileAttemptedAt?: string | undefined;
  providerMessageId?: string | undefined;
  safeCode?: string | undefined;
}

/**
 * In-memory `ChannelOutboxPort`.
 *
 * caller depends on: a batch whose sequences are not gapless refuses; a stale
 * `expectedCursor` returns `false` rather than throwing; an event rendering to
 * `null` advances the cursor without enqueuing; `claim` expires a timed-out
 * lease into `ambiguous` (never back to `pending`, which would risk a
 * duplicate post) and orders by `axisSequence` rather than wall clock; and
 * `ambiguousMessages()` orders least-recently-attempted first so one unresolvable row
 * cannot starve the rest.
 */
export class InMemoryChannelOutbox implements ChannelOutboxPort {
  readonly #now: () => Date;
  readonly #id: () => string;
  readonly #cursors = new Map<string, number>();
  readonly #rows = new Map<string, OutboxRow>();

  constructor(options: DoubleOptions = {}) {
    this.#now = options.now ?? fixtureClock();
    this.#id = options.id ?? fixtureIds("outbox");
  }

  cursor(conversationId: string): number {
    return this.#cursors.get(conversationId) ?? 0;
  }

  recordProjectedBatch(input: {
    readonly conversationId: string;
    readonly target: ChannelDeliveryTarget;
    readonly expectedCursor: number;
    readonly events: readonly {
      readonly axisSequence: number;
      readonly sourceEventId: string;
      readonly rendered: ChannelRenderedMessage | null;
    }[];
  }): boolean {
    const target = channelDeliveryTarget(input.target);
    if (this.cursor(input.conversationId) !== input.expectedCursor) return false;
    let next = input.expectedCursor;
    for (const event of input.events) {
      if (event.axisSequence !== next + 1) throw new ChannelSessionRefusalError("outbox_not_gapless");
      next = event.axisSequence;
    }
    const createdAt = this.#now().toISOString();
    for (const event of input.events) {
      if (event.rendered === null) continue;
      const contentDigest = digestFixture(JSON.stringify(event.rendered.content));
      const duplicate = [...this.#rows.values()].some(
        (row) => row.conversationId === input.conversationId && row.sourceEventId === event.sourceEventId && row.contentDigest === contentDigest,
      );
      if (duplicate) continue;
      const id = this.#id();
      this.#rows.set(id, {
        id,
        conversationId: input.conversationId,
        target,
        sourceEventId: event.sourceEventId,
        content: event.rendered.content,
        contentDigest,
        axisSequence: event.axisSequence,
        createdAt,
        state: "pending",
        attemptCount: 0,
        availableAt: createdAt,
      });
    }
    this.#cursors.set(input.conversationId, next);
    return true;
  }

  recordDirectMessage(input: {
    readonly conversationId: string;
    readonly target: ChannelDeliveryTarget;
    readonly sourceEventId: string;
    readonly content: ChannelMessageContent;
  }): { readonly outboxId: string; readonly created: boolean } {
    const target = channelDeliveryTarget(input.target);
    // Deliberately not keyed on the digest too: keying on it would let a
    // later, differently-rendered delivery through as a second, inconsistent
    // message. First content for a `sourceEventId` wins.
    const existing = [...this.#rows.values()].find(
      (row) => row.conversationId === input.conversationId && row.sourceEventId === input.sourceEventId,
    );
    if (existing !== undefined) return Object.freeze({ outboxId: existing.id, created: false });
    const highest = [...this.#rows.values()]
      .filter((row) => row.conversationId === input.conversationId)
      .reduce((accumulator, row) => Math.max(accumulator, row.axisSequence), 0);
    const createdAt = this.#now().toISOString();
    const id = this.#id();
    this.#rows.set(id, {
      id,
      conversationId: input.conversationId,
      target,
      sourceEventId: input.sourceEventId,
      content: input.content,
      contentDigest: digestFixture(JSON.stringify(input.content)),
      axisSequence: highest + 1,
      createdAt,
      state: "pending",
      attemptCount: 0,
      availableAt: createdAt,
    });
    return Object.freeze({ outboxId: id, created: true });
  }

  claim(owner: string, leaseMs = 30_000): ChannelOutboxRecord | null {
    const now = this.#now();
    const instant = now.toISOString();
    for (const row of this.#rows.values()) {
      if (row.state !== "delivering" || row.leaseExpiresAt === undefined || row.leaseExpiresAt >= instant) continue;
      row.state = "ambiguous";
      row.safeCode = "channel.provider.lease_expired";
      row.leaseOwner = undefined;
      row.leaseExpiresAt = undefined;
    }
    const due = [...this.#rows.values()]
      .filter((row) => row.state === "pending" && row.availableAt <= instant)
      // Raw code-unit comparison, matching SQLite's BINARY collation on
      // `ORDER BY conversation_id ASC`. `localeCompare` disagrees for any
      // mixed-case id (it sorts "conv-a" before "conv-B"; SQLite does not).
      .sort((left, right) => (left.conversationId < right.conversationId ? -1 : left.conversationId > right.conversationId ? 1 : 0) || left.axisSequence - right.axisSequence)[0];
    if (due === undefined) return null;
    due.state = "delivering";
    due.attemptCount += 1;
    due.leaseOwner = owner;
    due.leaseExpiresAt = new Date(now.getTime() + leaseMs).toISOString();
    return this.#record(due);
  }

  recordDisposition(outboxId: string, input: {
    readonly disposition: "confirmed" | "rejected" | "ambiguous" | "conflict";
    readonly safeCode?: string;
    readonly providerMessageId?: string;
  }): void {
    const row = this.#rows.get(outboxId);
    if (row === undefined || (row.state !== "delivering" && row.state !== "ambiguous")) throw new ChannelSessionRefusalError("outbox_not_deliverable");
    row.state = input.disposition === "confirmed" ? "delivered" : input.disposition === "rejected" ? "confirmed_failure" : input.disposition;
    row.safeCode = input.safeCode;
    row.providerMessageId = input.providerMessageId;
    row.leaseOwner = undefined;
    row.leaseExpiresAt = undefined;
  }

  retry(outboxId: string, safeCode: string, delayMs: number): void {
    const row = this.#rows.get(outboxId);
    if (row === undefined || row.state !== "delivering") throw new ChannelSessionRefusalError("outbox_not_retryable");
    row.state = "pending";
    row.safeCode = safeCode;
    row.availableAt = new Date(this.#now().getTime() + delayMs).toISOString();
    row.leaseOwner = undefined;
    row.leaseExpiresAt = undefined;
  }

  ambiguousMessages(): readonly ChannelOutboxRecord[] {
    return Object.freeze([...this.#rows.values()]
      .filter((row) => row.state === "ambiguous")
      .sort((left, right) => (left.reconcileAttemptedAt ?? "").localeCompare(right.reconcileAttemptedAt ?? "") || left.createdAt.localeCompare(right.createdAt) || left.axisSequence - right.axisSequence)
      .map((row) => this.#record(row)));
  }

  touchReconciliationAttempt(outboxId: string): void {
    const row = this.#rows.get(outboxId);
    if (row === undefined || row.state !== "ambiguous") return;
    row.reconcileAttemptedAt = this.#now().toISOString();
  }

  /** The current state of one row, for a test asserting on an outcome it did not claim. */
  outboxState(outboxId: string): ChannelOutboxState | null {
    return this.#rows.get(outboxId)?.state ?? null;
  }

  #record(row: OutboxRow): ChannelOutboxRecord {
    return Object.freeze({
      id: row.id,
      conversationId: row.conversationId,
      target: row.target,
      sourceEventId: row.sourceEventId,
      content: row.content,
      contentDigest: row.contentDigest,
      state: row.state,
      attemptCount: row.attemptCount,
      availableAt: row.availableAt,
      createdAt: row.createdAt,
    });
  }
}

// --------------------------------------------------------------------------
// Scripted channel adapter doubles
// --------------------------------------------------------------------------

/**
 * A renderer driven by a caller-supplied function, recording every envelope it
 * was handed. A slice wiring an observe/egress tick needs a renderer it can
 * make return `null` on demand -- that is the branch where the cursor must
 * still advance -- which is exactly what a scripted double gives it and a real
 * channel renderer does not.
 */
export class ScriptedChannelEventRenderer implements ChannelEventRenderer {
  readonly rendered: RouterRelayNeutralEventEnvelope[] = [];
  readonly #script: (envelope: RouterRelayNeutralEventEnvelope) => ChannelRenderedMessage | null;

  constructor(script: (envelope: RouterRelayNeutralEventEnvelope) => ChannelRenderedMessage | null) {
    this.#script = script;
  }

  /** Renders every envelope to one fixed message, for a test that only needs *a* payload. */
  static constant(content: ChannelMessageContent, summary = "scripted message"): ScriptedChannelEventRenderer {
    return new ScriptedChannelEventRenderer(() => ({ content, summary, terminal: false }));
  }

  render(envelope: RouterRelayNeutralEventEnvelope): ChannelRenderedMessage | null {
    this.rendered.push(envelope);
    return this.#script(envelope);
  }
}

/**
 * A deliverer that returns pre-scripted results in order, recording what it
 * was asked to deliver. Running past the end of a script throws rather than
 * repeating the last result: a caller that attempted one more delivery than
 * the test scripted is a finding, not something to absorb.
 */
export class ScriptedChannelOutboxDeliverer implements ChannelOutboxDeliverer {
  readonly created: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }[] = [];
  readonly reconciled: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }[] = [];
  readonly #createScript: ChannelDeliveryCreateResult[];
  readonly #reconcileScript: ChannelDeliveryReconcileResult[];

  constructor(script: {
    readonly create?: readonly ChannelDeliveryCreateResult[];
    readonly reconcile?: readonly ChannelDeliveryReconcileResult[];
  } = {}) {
    this.#createScript = [...(script.create ?? [])];
    this.#reconcileScript = [...(script.reconcile ?? [])];
  }

  async create(input: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }): Promise<ChannelDeliveryCreateResult> {
    this.created.push(input);
    const next = this.#createScript.shift();
    if (next === undefined) throw new Error("ScriptedChannelOutboxDeliverer: create called more times than scripted.");
    return next;
  }

  async reconcile(input: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }): Promise<ChannelDeliveryReconcileResult> {
    this.reconciled.push(input);
    const next = this.#reconcileScript.shift();
    if (next === undefined) throw new Error("ScriptedChannelOutboxDeliverer: reconcile called more times than scripted.");
    return next;
  }
}

/**
 * A redispatch instruction re-fetcher backed by a map from `messageId` to the
 * result it should report. An unmapped `messageId` reports `gone`, which is
 * the safe reading of "the message this marker was authorized from is not
 * there any more".
 */
export class ScriptedChannelRedispatchInstructionFetcher implements ChannelRedispatchInstructionFetcher {
  readonly requested: { readonly scope: ChannelConversationScope; readonly messageId: string }[] = [];
  readonly #results: Map<string, ChannelRedispatchInstructionResult>;

  constructor(results: Readonly<Record<string, ChannelRedispatchInstructionResult>> = {}) {
    this.#results = new Map(Object.entries(results));
  }

  async fetchInstruction(input: { readonly scope: ChannelConversationScope; readonly messageId: string }): Promise<ChannelRedispatchInstructionResult> {
    this.requested.push(input);
    return this.#results.get(input.messageId) ?? { disposition: "gone" as const };
  }
}
