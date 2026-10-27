/**
 * Session ports for the conversation registry, run chain and delivery outbox.
 * Routing scopes select Team bindings; conversation scopes select AgentSessions.
 * Credentials stay outside these types.
 */

import { createHash } from "node:crypto";

import type { RuntimeState, Task as RuntimeTask } from "@axisosme/axis-contracts/runtime";

import type { StructuredElicitation } from "../shared/elicitation.js";
import type { RouterRelayNeutralEventEnvelope } from "../shared/router-relay-event.js";
import type { WorkerSelection } from "../shared/worker-selection.js";
import type { ChannelCommandKind, ChannelCommandOutcome } from "./channel-command.js";
import type { TenantRef } from "./channel-identity.js";

export const CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION = "1.0.0" as const;
/** Creation-time provenance for conversations opened while routed Worker selection was enabled. */
export const CHANNEL_WORKER_ROUTING_ADMISSION_VERSION = "1.0.0" as const;

export type ChannelSessionInputErrorCode = "malformed_conversation_scope" | "malformed_delivery_target" | "malformed_digest";

/**
 * Refuses a malformed input to this module's validators ("that is not a scope").
 * `ChannelSessionRefusalError` refuses a legal input a port cannot honour ("that
 * scope's conversation is closed").
 */
export class ChannelSessionInputError extends Error {
  readonly code: ChannelSessionInputErrorCode;
  constructor(code: ChannelSessionInputErrorCode) {
    super(`channel session input is malformed: ${code}`);
    this.name = "ChannelSessionInputError";
    this.code = code;
  }
}

/**
 * The refusal vocabulary every implementation of the ports below uses.
 * Persistence failures such as `schema_version_unsupported` and
 * `persisted_state_conflict` are not part of the port contract and stay in each
 * store's own error type.
 */
export type ChannelSessionRefusalCode =
  // -- conversation registry --
  /** The same message was already accepted with a different instruction or conversation: a conflicting rebind. */
  | "message_rebound"
  /** The referenced conversation does not exist, or is not the scope's current conversation. */
  | "unknown_conversation"
  /** The conversation is already closed, so nothing may move it. */
  | "conversation_closed"
  /** The message's scope does not match the conversation's. */
  | "conversation_scope_mismatch"
  // -- run chain --
  /** The same commandId was already reserved for a different intent. */
  | "command_rebound"
  /** Another command is reserved on this conversation and has not settled yet. */
  | "reservation_in_flight"
  /** The named command reservation does not exist. */
  | "unknown_reservation"
  /** The reservation already settled with a different outcome. */
  | "reservation_settled"
  /** The conversation is bound to a different Axis tenant. */
  | "tenant_mismatch"
  /** The Runtime Task identity does not match the one this conversation is bound to. */
  | "task_mismatch"
  /** The asserted predecessor Run is not the one the chain resolves. */
  | "prior_run_mismatch"
  /** The command or state change is not legal for the conversation's current Run. */
  | "run_state_conflict"
  /** The named Run does not exist in this conversation. */
  | "unknown_run"
  /** A different elicitation was presented for a Run that already raised one. */
  | "decision_rebound"
  /** The Run's wait was cancelled, so its elicitation cannot reopen one. */
  | "decision_cancelled"
  /** The Run is no longer the conversation's current one, so it cannot raise a decision. */
  | "run_superseded"
  /** An ordinary message cannot continue a wait that was already locally cancelled. */
  | "wait_cancelled"
  /**
   * An earlier start or continue was dispatched but its outcome could never be
   * confirmed; the conversation accepts no further instruction except a stop of a
   * live Run.
   */
  | "session_unconfirmed"
  // -- outbox --
  /** A projected batch's `axisSequence` values are not `expectedCursor + 1, + 2, ...` with no gap. */
  | "outbox_not_gapless"
  /** The outbox row is not in a state a disposition may be recorded for. */
  | "outbox_not_deliverable"
  /** The outbox row is not in a state a retry may be scheduled for. */
  | "outbox_not_retryable";

/**
 * The error the ports refuse with, so a caller can branch on `code` without
 * knowing the channel.
 */
export class ChannelSessionRefusalError extends Error {
  readonly code: ChannelSessionRefusalCode;
  constructor(code: ChannelSessionRefusalCode) {
    super(`channel session refused: ${code}`);
    this.name = "ChannelSessionRefusalError";
    this.code = code;
  }
}

/**
 * The `sha256:`-prefixed digest form every store here persists. A template literal
 * type, so a digest field cannot silently be handed an instruction body.
 */
export type ChannelDigest = `sha256:${string}`;

const DIGEST = /^sha256:[0-9a-f]{64}$/u;

/** Validates a digest string. */
export function channelDigest(value: unknown): ChannelDigest {
  if (typeof value !== "string" || !DIGEST.test(value)) throw new ChannelSessionInputError("malformed_digest");
  return value as ChannelDigest;
}

/** `sha256:` plus the lowercase hex digest of the instruction's UTF-8 bytes. */
export function channelInstructionDigest(instruction: string): ChannelDigest {
  if (typeof instruction !== "string") throw new ChannelSessionInputError("malformed_digest");
  return `sha256:${createHash("sha256").update(instruction, "utf8").digest("hex")}`;
}

/**
 * The opaque-identifier pattern `./channel-identity.ts` also enforces. It excludes
 * `:`, which keeps `channelConversationScopeKey` injective.
 */
const OPAQUE_IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;

/**
 * Plain data only: an ordinary object literal. A null prototype, a Symbol key, a
 * non-enumerable property and an accessor are refused.
 */
function plainObject(value: unknown, code: ChannelSessionInputErrorCode): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new ChannelSessionInputError(code);
  if (Object.getPrototypeOf(value) !== Object.prototype) throw new ChannelSessionInputError(code);
  if (Object.getOwnPropertySymbols(value).length > 0) throw new ChannelSessionInputError(code);
  for (const descriptor of Object.values(Object.getOwnPropertyDescriptors(value))) {
    if (!descriptor.enumerable || typeof descriptor.get === "function" || typeof descriptor.set === "function") {
      throw new ChannelSessionInputError(code);
    }
  }
  return value as Record<string, unknown>;
}

function exactKeys(data: Record<string, unknown>, allowed: readonly string[], code: ChannelSessionInputErrorCode): void {
  const keys = Object.keys(data);
  if (keys.length !== allowed.length || keys.some((key) => !allowed.includes(key))) throw new ChannelSessionInputError(code);
}

/**
 * `exactKeys`, tolerating an already-present matching `system` discriminator, so
 * every constructor accepts its own output. A `system` naming another provider is
 * refused.
 */
function variantKeys(data: Record<string, unknown>, allowed: readonly string[], system: string, code: ChannelSessionInputErrorCode): void {
  if (data.system !== undefined && data.system !== system) throw new ChannelSessionInputError(code);
  exactKeys(data, data.system === undefined ? allowed : ["system", ...allowed], code);
}

function opaque(value: unknown, code: ChannelSessionInputErrorCode): string {
  if (typeof value !== "string" || !OPAQUE_IDENTIFIER.test(value)) throw new ChannelSessionInputError(code);
  return value;
}

/**
 * Linear's conversation-selection key. A Linear session identity is its
 * `agentSessionId`; `organizationId` is the uniqueness domain a Linear registry
 * keys within.
 */
export interface LinearChannelConversationScope {
  readonly system: "linear";
  readonly organizationId: string;
  readonly agentSessionId: string;
}

/** Closed, provider-discriminated conversation-selection key. Never a routing key -- see this module's header. */
export type ChannelConversationScope = LinearChannelConversationScope;

/**
 * Linear's delivery target: only `agentSessionId`, since an Agent Activity is
 * posted to the session itself.
 */
export interface LinearChannelDeliveryTarget {
  readonly system: "linear";
  readonly agentSessionId: string;
}

/** Closed, provider-discriminated delivery target. */
export type ChannelDeliveryTarget = LinearChannelDeliveryTarget;

/** Validates and freezes a Linear conversation scope. */
export function linearChannelConversationScope(input: { readonly organizationId: string; readonly agentSessionId: string; readonly system?: "linear" }): LinearChannelConversationScope {
  const data = plainObject(input, "malformed_conversation_scope");
  variantKeys(data, ["organizationId", "agentSessionId"], "linear", "malformed_conversation_scope");
  return Object.freeze({
    system: "linear" as const,
    organizationId: opaque(data.organizationId, "malformed_conversation_scope"),
    agentSessionId: opaque(data.agentSessionId, "malformed_conversation_scope"),
  });
}

/**
 * Normalises an untrusted value into a validated, frozen `ChannelConversationScope`,
 * dispatching on `system`. Re-derives through the constructors, so the result is
 * validated and frozen. An unknown or absent `system` is rejected.
 */
export function channelConversationScope(value: unknown): ChannelConversationScope {
  const data = plainObject(value, "malformed_conversation_scope");
  // The untrusted object's key set is checked before any field is read out of it.
  if (data.system === "linear") {
    exactKeys(data, ["system", "organizationId", "agentSessionId"], "malformed_conversation_scope");
    return linearChannelConversationScope({
      organizationId: opaque(data.organizationId, "malformed_conversation_scope"),
      agentSessionId: opaque(data.agentSessionId, "malformed_conversation_scope"),
    });
  }
  throw new ChannelSessionInputError("malformed_conversation_scope");
}

/** Validates and freezes a Linear delivery target. */
export function linearChannelDeliveryTarget(input: { readonly agentSessionId: string; readonly system?: "linear" }): LinearChannelDeliveryTarget {
  const data = plainObject(input, "malformed_delivery_target");
  variantKeys(data, ["agentSessionId"], "linear", "malformed_delivery_target");
  return Object.freeze({ system: "linear" as const, agentSessionId: opaque(data.agentSessionId, "malformed_delivery_target") });
}

/** Normalises an untrusted value into a validated, frozen `ChannelDeliveryTarget`, dispatching on `system`. */
export function channelDeliveryTarget(value: unknown): ChannelDeliveryTarget {
  const data = plainObject(value, "malformed_delivery_target");
  // The untrusted object's key set is checked before any field is read out of it.
  if (data.system === "linear") {
    exactKeys(data, ["system", "agentSessionId"], "malformed_delivery_target");
    return linearChannelDeliveryTarget({ agentSessionId: opaque(data.agentSessionId, "malformed_delivery_target") });
  }
  throw new ChannelSessionInputError("malformed_delivery_target");
}

/**
 * A stable, injective string key for a conversation scope, for a map key or a
 * uniqueness constraint, never a durable `conversationId`. `system` then the
 * variant's fields, `:`-joined; no field's charset contains `:`, and the value is
 * re-validated here, so no field can forge a segment boundary.
 */
export function channelConversationScopeKey(scope: ChannelConversationScope): string {
  const normalized = channelConversationScope(scope);
  return `linear:${normalized.organizationId}:${normalized.agentSessionId}`;
}

export function channelMessageUniquenessDomain(scope: ChannelConversationScope): string {
  const normalized = channelConversationScope(scope);
  return `linear:${normalized.organizationId}`;
}

/**
 * The runtime `Task` shape a run chain may be handed. This type is the single
 * source of the admitted version set; see `CHANNEL_RUNTIME_CONTRACT_VERSIONS` and
 * the CHECK constraint in `channel-run-chain.ts`.
 */
export type ChannelRuntimeTask = RuntimeTask;

/** The runtime contract versions a run chain admits, derived from Task. */
export type ChannelRuntimeContractVersion = ChannelRuntimeTask["contractVersion"];

/**
 * The admitted version as a value. `Record<ChannelRuntimeContractVersion, true>`
 * keeps it aligned with `ChannelRuntimeTask`. Callers
 * use `isChannelRuntimeContractVersion`.
 */
export const CHANNEL_RUNTIME_CONTRACT_VERSIONS: Readonly<Record<ChannelRuntimeContractVersion, true>> = Object.freeze({
  "1.0.0": true,
});

/** Whether an untrusted value is one of the runtime contract versions a run chain admits. */
export function isChannelRuntimeContractVersion(value: unknown): value is ChannelRuntimeContractVersion {
  return typeof value === "string" && Object.hasOwn(CHANNEL_RUNTIME_CONTRACT_VERSIONS, value);
}

// --------------------------------------------------------------------------
// Conversation registry
// --------------------------------------------------------------------------

/**
 * `contractVersion`, here and on every record below, is this module's constant,
 * not the implementing store's.
 */
export interface ChannelConversation {
  readonly contractVersion: typeof CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION;
  /** Monotonic insertion order within the registry. The stable cursor; never a timestamp. */
  readonly sequence: number;
  readonly scope: ChannelConversationScope;
  readonly conversationId: string;
  readonly status: "active" | "closed";
  readonly createdAt: string;
  readonly closedAt?: string;
  /** Present only when this conversation was atomically opened while routed Worker selection was enabled. */
  readonly workerRoutingAdmission?: {
    readonly contractVersion: typeof CHANNEL_WORKER_ROUTING_ADMISSION_VERSION;
    readonly admittedAt: string;
  };
}

/** `opened` distinguishes a freshly synthesized conversation from a resumed one. */
export interface ChannelConversationResolution {
  readonly conversation: ChannelConversation;
  readonly opened: boolean;
}

export interface ChannelMessageInvocation {
  readonly contractVersion: typeof CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION;
  readonly scope: ChannelConversationScope;
  readonly messageId: string;
  readonly conversationId: string;
  readonly instructionDigest: ChannelDigest;
  readonly acceptedAt: string;
}

/** `replayed` is true when this exact message+instruction was already accepted. */
export interface ChannelMessageInvocationReceipt {
  readonly invocation: ChannelMessageInvocation;
  readonly replayed: boolean;
}

/** Inbound webhook/delivery de-duplication receipt. `duplicate` is true on a redelivery. */
export interface ChannelInboundDeliveryReceipt {
  readonly deliveryId: string;
  readonly receivedAt: string;
  readonly duplicate: boolean;
}

/**
 * The conversation registry a channel provides: the write/resolve surface the
 * shared core calls. Read methods stay on the concrete store.
 */
export interface ChannelConversationRegistryPort {
  /**
   * Resolves the conversation for a scope, synthesizing one when no pointer
   * exists or the pointer names a closed conversation. Must be atomic: two
   * concurrent first mentions on one scope may not both open a conversation.
   */
  openOrResume(input: { readonly scope: ChannelConversationScope; readonly admitWorkerRouting?: true }): ChannelConversationResolution;

  /** Records one inbound provider delivery id, reporting whether it was already seen. */
  recordDelivery(input: { readonly deliveryId: string }): ChannelInboundDeliveryReceipt;

  /**
   * Records one accepted instruction-bearing message. Takes the whole `scope` so the
   * implementation can pick out its own uniqueness domain.
   */
  recordMessageInvocation(input: {
    readonly scope: ChannelConversationScope;
    readonly messageId: string;
    readonly conversationId: string;
    readonly instructionDigest: ChannelDigest;
  }): ChannelMessageInvocationReceipt;

  /**
   * Closes one conversation, so the next `openOrResume` on its scope synthesizes a
   * successor. Not `close`, which every store uses for its database.
   */
  closeConversation(input: { readonly conversationId: string }): ChannelConversation;
}

// --------------------------------------------------------------------------
// Run chain
// --------------------------------------------------------------------------

/**
 * The run states a successor may chain onto. Includes `waiting_for_input`: a
 * waiting Run is finished as far as chaining goes.
 */
export const CHANNEL_RUN_TERMINAL_STATES: readonly RuntimeState[] = Object.freeze(["succeeded", "failed", "waiting_for_input"]);

/**
 * The smaller set meaning "this conversation is safely idle". Cancellation reuses
 * `failed`. See `channelRedispatchEligible` for why the two sets differ.
 */
export const CHANNEL_RUN_SETTLED_STATES: readonly RuntimeState[] = Object.freeze(["succeeded", "failed"]);

/**
 * Whether a queued redispatch may fire, given the conversation's current Run
 * state (`null` when there is none). The authority for
 * `ChannelRedispatchMarker.eligible`. Using `CHANNEL_RUN_TERMINAL_STATES` instead
 * reports a waiting conversation as ready, and the queued instruction would then
 * answer the open question.
 */
export function channelRedispatchEligible(currentRunState: RuntimeState | null): boolean {
  return currentRunState !== null && CHANNEL_RUN_SETTLED_STATES.includes(currentRunState);
}

/** `resolved_locally` is a cancel the run chain settled without Router ever seeing it -- see `resolveLocally`. */
/**
 * Declared in the neutral core. `ChannelOutboxState` is pinned against
 * `../relay/channel-outbox.ts`'s `PersistedChannelOutboxState` in
 * `test/relay/store-schema-parity.test.ts`.
 */
export type ChannelRunChainReservationStatus = "reserved" | "accepted" | "interrupt_requested" | "steered" | "rejected" | "resolved_locally";

export type ChannelWaitingDecisionStatus = "waiting" | "claimed" | "resolved" | "cancelled";

export interface ChannelRunChainReservation {
  readonly contractVersion: typeof CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION;
  /** Monotonic insertion order. The stable reservation cursor; never a timestamp. */
  readonly sequence: number;
  readonly conversationId: string;
  readonly commandId: string;
  readonly kind: ChannelCommandKind;
  readonly intentDigest: ChannelDigest;
  /** Present for a start/continue; a cancel carries no instruction, matching `ChannelCancelCommand`. */
  readonly instructionDigest?: ChannelDigest;
  /** Immutable routed Worker choice for a start; absent on continuations and cancels. */
  readonly workerSelection?: WorkerSelection;
  /** The terminal Run this command chains onto. Absent when the command interrupts instead. */
  readonly priorRunId?: string;
  /** The still-executing Run this command interrupts. */
  readonly interruptRunId?: string;
  /**
   * The waiting decision this reservation claimed, resolved internally and never
   * supplied by the user. Claiming is a side effect of `reserve`.
   */
  readonly decisionId?: string;
  readonly status: ChannelRunChainReservationStatus;
  readonly runId?: string;
  readonly taskId?: string;
  readonly safeCode?: string;
  readonly reservedAt: string;
  readonly settledAt?: string;
}

/** `replayed` is true when this exact command was already reserved. */
export interface ChannelRunChainReservationReceipt {
  readonly reservation: ChannelRunChainReservation;
  readonly replayed: boolean;
}

export interface ChannelRunChainEntry {
  readonly contractVersion: typeof CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION;
  readonly sequence: number;
  readonly conversationId: string;
  readonly runId: string;
  readonly priorRunId?: string;
  readonly taskId: string;
  readonly commandId: string;
  readonly state: RuntimeState;
  readonly startedAt: string;
  readonly updatedAt: string;
}

export interface ChannelWaitingDecision {
  readonly contractVersion: typeof CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION;
  readonly conversationId: string;
  readonly runId: string;
  readonly decisionId: string;
  readonly elicitationDigest: ChannelDigest;
  readonly status: ChannelWaitingDecisionStatus;
  readonly claimedByCommandId?: string;
  readonly raisedAt: string;
  readonly settledAt?: string;
}

/** `raised` is false when the same elicitation was already recorded for this Run. */
export interface ChannelWaitingDecisionReceipt {
  readonly decision: ChannelWaitingDecision;
  readonly raised: boolean;
}

/**
 * A queued redispatch. Carries the instruction digest, never the instruction: the
 * channel adapter owns the encrypted instruction store.
 */
export interface ChannelRedispatchMarker {
  readonly contractVersion: typeof CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION;
  readonly conversationId: string;
  readonly commandId: string;
  readonly interruptedRunId: string;
  readonly instructionDigest: ChannelDigest;
  /** True once the interrupted Run has actually reached a terminal state. */
  readonly eligible: boolean;
  readonly requestedAt: string;
}

/**
 * `resolved_locally` is reported rather than folded into `absent` ("dispatch it")
 * or `unknown` ("cannot tell"): a cancel the chain already honoured is neither.
 */
export type ChannelRunChainReconcileOutcome = "accepted" | "interrupt_requested" | "steered" | "rejected" | "resolved_locally" | "absent" | "unknown";

/**
 * The run chain a channel provides. The redispatch retry members
 * (`claimRedispatch`, `quarantineRedispatch`, `refundRedispatchAttempt`,
 * `holdRedispatchAttempt`, `redispatchAttempts`) and the read methods stay on
 * `SqliteChannelRunChainStore`, which both redispatch triggers `Pick` from.
 * `pendingRedispatch` hands out no attempt and bounds nothing.
 */
export interface ChannelRunChainPort {
  /** Reserves one command against a conversation before it is dispatched. */
  reserve(input: {
    readonly scope: ChannelConversationScope;
    readonly conversationId: string;
    readonly tenant: TenantRef;
    readonly commandId: string;
    readonly kind: ChannelCommandKind;
    readonly instructionDigest?: ChannelDigest;
    readonly priorRunId?: string;
    readonly workerSelection?: WorkerSelection;
  }): ChannelRunChainReservationReceipt;

  /** Settles a reservation with the outcome Router returned for it. */
  settle(input: { readonly commandId: string; readonly outcome: ChannelCommandOutcome }): ChannelRunChainReservation;

  /** Settles a cancel the chain honoured itself, without Router ever seeing the command. */
  resolveLocally(input: { readonly commandId: string }): ChannelRunChainReservation;

  /** What became of one command, for a caller reconciling after a crash. */
  reconcile(input: { readonly commandId: string }): ChannelRunChainReconcileOutcome;

  /** Records one observed runtime state transition for a Run in the chain. */
  recordRunState(input: {
    readonly conversationId: string;
    readonly task: ChannelRuntimeTask;
    readonly runId: string;
    readonly state: RuntimeState;
  }): ChannelRunChainEntry;

  /** Records that a Run is waiting on a structured decision. */
  raiseWaitingDecision(input: { readonly runId: string; readonly elicitationDigest: ChannelDigest }): ChannelWaitingDecisionReceipt;

  /**
   * The conversation's open waiting decision, or null. "Open" is `waiting` or
   * `claimed`, scoped to the current Run; a claim records which command is
   * answering, not that the decision is over.
   */
  openDecision(input: { readonly conversationId: string }): ChannelWaitingDecision | null;

  /**
   * The conversation's queued redispatch, or null. `eligible` is computed with
   * `channelRedispatchEligible`. The marker is written by `settle` on an
   * `interrupt_requested` continue; a `cancel` deletes it inside `reserve`.
   */
  pendingRedispatch(input: { readonly conversationId: string }): ChannelRedispatchMarker | null;
}

// --------------------------------------------------------------------------
// Rendered content and outbox
// --------------------------------------------------------------------------

export type ChannelMessageContent = Readonly<Record<string, unknown>>;

export interface ChannelWaitingProjection {
  readonly taskId: string;
  readonly attemptId: string;
  readonly runId: string;
  readonly elicitation: StructuredElicitation;
}

/**
 * One neutral event rendered for one channel. `summary` is a bounded readable
 * summary for logs and operator views; a delivery digest is taken over `content`,
 * never `summary`.
 */
export interface ChannelRenderedMessage {
  readonly content: ChannelMessageContent;
  readonly summary: string;
  readonly terminal: boolean;
  readonly waiting?: ChannelWaitingProjection;
}

/**
 * The renderer a channel adapter must provide. Returns `null` for a neutral
 * event this channel deliberately posts nothing for -- the cursor still
 * advances past it (see `ChannelOutboxPort#recordProjectedBatch`), but no
 * outbox row is created. `null` is a decision, not a failure.
 */
export interface ChannelEventRenderer {
  render(envelope: RouterRelayNeutralEventEnvelope): ChannelRenderedMessage | null;
}

export type ChannelOutboxState = "pending" | "delivering" | "delivered" | "confirmed_failure" | "ambiguous" | "conflict";

/** One enqueued outbound message, with its target nested under `target`. */
export interface ChannelOutboxRecord {
  readonly id: string;
  readonly conversationId: string;
  readonly target: ChannelDeliveryTarget;
  readonly sourceEventId: string;
  readonly content: ChannelMessageContent;
  readonly contentDigest: ChannelDigest;
  readonly state: ChannelOutboxState;
  readonly attemptCount: number;
  readonly availableAt: string;
  readonly createdAt: string;
}

/**
 * The outbox a channel provides. `outboxState`, `waitingDecision`, `ready` and
 * `close` stay on the concrete store.
 */
export interface ChannelOutboxPort {
  /** The conversation's current projection cursor; 0 when nothing has been projected. */
  cursor(conversationId: string): number;

  /**
   * Atomically advances the conversation's cursor and enqueues one row per
   * rendered event. Returns `false` on a cursor mismatch (a stale or
   * conflicting batch) rather than throwing, so a caller can reload and retry.
   * `axisSequence` values must be exactly `expectedCursor + 1, + 2, ...` with
   * no gap. An event whose `rendered` is `null` still advances the cursor and
   * creates no row.
   */
  recordProjectedBatch(input: {
    readonly conversationId: string;
    readonly target: ChannelDeliveryTarget;
    readonly expectedCursor: number;
    readonly events: readonly {
      readonly axisSequence: number;
      readonly sourceEventId: string;
      readonly rendered: ChannelRenderedMessage | null;
    }[];
  }): boolean;

  /**
   * Enqueues one message that is not a projection of a Router event (a
   * command outcome, an authorization refusal). Idempotent on
   * `(conversationId, sourceEventId)`: the first body wins and every later
   * call is absorbed as a no-op regardless of its own content, so a
   * differently-rendered redelivery cannot post a second, inconsistent
   * message. `created` reports which happened.
   */
  recordDirectMessage(input: {
    readonly conversationId: string;
    readonly target: ChannelDeliveryTarget;
    readonly sourceEventId: string;
    readonly content: ChannelMessageContent;
  }): { readonly outboxId: string; readonly created: boolean };

  /**
   * Claims the next due message under a lease, or null when none is due.
   * Must expire timed-out `delivering` leases into `ambiguous` first -- a
   * lease that simply reverted to `pending` would risk a duplicate post.
   * Ordered by the per-conversation sequence, never by wall clock, because a
   * whole batch shares one enqueue timestamp.
   */
  claim(owner: string, leaseMs?: number): ChannelOutboxRecord | null;

  /** Terminally dispositions a claimed message. */
  recordDisposition(outboxId: string, input: {
    readonly disposition: "confirmed" | "rejected" | "ambiguous" | "conflict";
    readonly safeCode?: string;
    readonly providerMessageId?: string;
  }): void;

  /** Returns a claimed message to `pending`, available again after `delayMs`. */
  retry(outboxId: string, safeCode: string, delayMs: number): void;

  /**
   * Every `ambiguous` message, least-recently-reconciliation-attempted first, so an
   * unresolvable row cannot starve the rest. `claim`, `retry` and
   * `recordDisposition` take positional parameters.
   */
  ambiguousMessages(): readonly ChannelOutboxRecord[];

  /**
   * Records a reconciliation attempt that did not resolve the row, so
   * `ambiguousMessages()` rotates. A no-op if the row is no longer ambiguous.
   */
  touchReconciliationAttempt(outboxId: string): void;
}

// --------------------------------------------------------------------------
// Channel adapter delivery ports
// --------------------------------------------------------------------------

/**
 * One create attempt's result. A caller turns `retryable` into `retry` and
 * everything else into `recordDisposition`.
 */
export type ChannelDeliveryCreateResult =
  | { readonly disposition: "confirmed"; readonly providerMessageId: string }
  | { readonly disposition: "rejected"; readonly safeCode: string }
  | { readonly disposition: "retryable"; readonly safeCode: string }
  | { readonly disposition: "ambiguous"; readonly safeCode: string };

/**
 * One reconcile attempt's result.
 *
 * `unresolved` is the "cannot tell right now" case -- never posted, posted
 * outside the scan window, or credentials currently unavailable. It must leave
 * the row `ambiguous` and rotate past it
 * (`ChannelOutboxPort#touchReconciliationAttempt`); re-queuing a fresh post on
 * that ambiguity risks the exact duplicate reconciliation exists to prevent.
 */
export type ChannelDeliveryReconcileResult =
  | { readonly disposition: "confirmed"; readonly providerMessageId: string }
  | { readonly disposition: "conflict"; readonly safeCode: string }
  | { readonly disposition: "unresolved" };

/**
 * The provider-facing delivery adapter; outbox bookkeeping belongs to
 * `ChannelOutboxPort` and retry policy to the caller. `target` is passed
 * separately so an implementation can use a freshly resolved target;
 * `record.target` is the enqueue-time snapshot.
 */
export interface ChannelOutboxDeliverer {
  create(input: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }): Promise<ChannelDeliveryCreateResult>;
  reconcile(input: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }): Promise<ChannelDeliveryReconcileResult>;
}

/**
 * The result of re-reading the message a queued redispatch was authorized from.
 * `fetched` carries only the digest; comparing it is the caller's job. `gone`
 * clears the marker, `unavailable` leaves it untouched.
 */
export type ChannelRedispatchInstructionResult =
  | { readonly disposition: "fetched"; readonly instructionDigest: ChannelDigest }
  | { readonly disposition: "gone" }
  /**
   * The bound repository or workspace has no credential registration this provider
   * administers: retriable once one exists, unlike `unavailable`.
   */
  | { readonly disposition: "unregistered" }
  | { readonly disposition: "unavailable"; readonly safeCode: string };

/** Re-reads the instruction a queued redispatch was authorized from, so it can be revalidated before firing. */
export interface ChannelRedispatchInstructionFetcher {
  fetchInstruction(input: { readonly scope: ChannelConversationScope; readonly messageId: string }): Promise<ChannelRedispatchInstructionResult>;
}
