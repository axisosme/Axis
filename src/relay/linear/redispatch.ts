import { randomUUID } from "node:crypto";

import { channelContinueCommand } from "../../router/channel-command.js";
import type { ChannelCommandOutcome, ChannelCommandPort } from "../../router/channel-command.js";
import { linearChannelIdentity } from "../../router/channel-identity.js";
import {
  ChannelSessionRefusalError,
  channelInstructionDigest,
  linearChannelConversationScope,
  linearChannelDeliveryTarget,
  type ChannelDigest,
  type ChannelOutboxPort,
  type ChannelRunChainReservation,
  type ChannelSessionRefusalCode,
} from "../../router/channel-conversation-types.js";
import { ChannelRunChainStoreError } from "../../router/channel-run-chain.js";
import type { ChannelRunChainStoreErrorCode, SqliteChannelRunChainStore } from "../../router/channel-run-chain.js";
import type { LinearRouterTaskRef } from "../../router/router.js";

import { safeError } from "../errors.js";
import { LINEAR_ACTIVITY_COPY } from "./activity.js";
import { verifiedLinearOwner, type VerifiedLinearOwner } from "./authorization.js";
import { composeLinearDispatchInstruction, readExactLinear, type LinearDispatchIssue } from "./instruction.js";
import type { LinearProviderPort } from "./provider.js";

/**
 * Redispatches a Linear `continue` that interrupted a Run, once, when the conversation is idle.
 *
 * The run chain keeps only the instruction's digest, so the instruction is re-read from Linear and
 * re-verified against the recorded session, owner and provenance: dispatching whatever Linear holds now
 * would run an instruction no authorization covered. A confirmed mismatch clears the marker; a transient
 * failure leaves it. The envelope is recomposed with `composeLinearDispatchInstruction`, as on first dispatch.
 */

/**
 * The Linear message identity a queued redispatch was authorized from, keyed by the marker's `commandId`.
 * The re-read's author must equal `ownerUserId`, already verified as an active human at accept time.
 */
export interface LinearRedispatchSource {
  readonly agentSessionId: string;
  readonly activityId: string;
  /** The frozen verified human whose authorization the instruction carries. */
  readonly ownerUserId: string;
  /** The Comment the Activity was posted from, or `null` for a direct prompt. Re-checked, never re-derived. */
  readonly sourceCommentId: string | null;
  /** The decision this instruction answered; non-null makes it un-redispatchable. */
  readonly expectedDecisionId: string | null;
  /** Recorded session identity the dispatch envelope is composed from, so a redispatch renders what the first dispatch did. */
  readonly appUserId: string;
  readonly issueId: string;
  /** The session's captured thread root, `null` for a session opened without one. */
  readonly rootCommentId: string | null;
}

/**
 * Resolves a marker's `commandId` to its Linear message identity. `null` means no source is recorded;
 * the marker is left untouched, because a lookup that cannot answer is not evidence the instruction changed.
 */
export interface LinearRedispatchSourceLookup {
  redispatchSource(input: { readonly conversationId: string; readonly commandId: string }): LinearRedispatchSource | null;
}

/**
 * The Router Task a Linear conversation's events live under. `null` means it cannot be resolved, which
 * callers record as a failure rather than skipping the conversation.
 */
export interface LinearChannelTaskRefLookup {
  taskRef(input: { readonly conversationId: string }): LinearRouterTaskRef | null;
}

/** A confirmed mismatch, which clears the marker. Transient failures are separate dispositions. */
export type LinearRedispatchRefusalReason =
  | "activity_mismatch"
  | "session_mismatch"
  | "provenance_mismatch"
  /** The Activity's author is no longer the frozen owner, or is no longer a verifiable active human. */
  | "user_mismatch"
  | "wrong_type"
  /** The Activity still verifies, but its instruction no longer digests to the marker's digest. */
  | "instruction_changed"
  /** See `DECISION_REPLY_NOT_REDISPATCHABLE`. */
  | "decision_reply"
  /** Linear answered about a different Issue ID than the session's. */
  | "issue_mismatch";

/** A verified re-read; `refused` clears the marker, the other failures leave it. */
export type LinearRedispatchInstructionResult =
  | {
    readonly disposition: "fetched";
    readonly instructionDigest: ChannelDigest;
    /** The verified user command. Router receives it inside `composeLinearDispatchInstruction`'s envelope. */
    readonly content: string;
    /** Re-verified from this read, not the recorded copy. */
    readonly owner: VerifiedLinearOwner;
    /** The exact issue identity the envelope names, re-read with the Activity. */
    readonly issue: LinearDispatchIssue;
  }
  | { readonly disposition: "refused"; readonly reason: LinearRedispatchRefusalReason }
  | { readonly disposition: "credential_unavailable" }
  | { readonly disposition: "unavailable"; readonly safeCode: string };

export interface LinearRedispatchInstructionReader {
  readRedispatchInstruction(input: { readonly source: LinearRedispatchSource }): Promise<LinearRedispatchInstructionResult>;
}

/**
 * A waiting-decision reply is never redispatched: its dispatched bytes are an expansion of the reply
 * token against an elicitation that has since been answered, so they cannot be re-derived from the
 * Activity. `settle` writes markers only for interrupts, so reaching this means the run chain and the
 * ingress ledger disagree, and the refusal is terminal.
 */
const DECISION_REPLY_NOT_REDISPATCHABLE = "decision_reply" as const;

export interface LinearRedispatchInstructionReaderOptions {
  readonly provider: Pick<LinearProviderPort, "loadExactAgentActivity" | "loadExactIssue">;
  readonly accessToken: () => Promise<string | null>;
  /** Backoff between bounded exact-read attempts. */
  readonly sleep: (milliseconds: number) => Promise<void>;
}

/**
 * Re-reads the Activity, re-applies `resolveDirectPrompt`'s identity gates against the recorded source
 * (provenance compared both ways, since the question is whether this is still the same Activity), then
 * reads the exact issue. Reply-token checks are not re-applied. A missing exact read is `credential_unavailable`.
 */
export function linearRedispatchInstructionReader(options: LinearRedispatchInstructionReaderOptions): LinearRedispatchInstructionReader {
  return {
    async readRedispatchInstruction({ source }): Promise<LinearRedispatchInstructionResult> {
      if (source.expectedDecisionId !== null) return Object.freeze({ disposition: "refused" as const, reason: DECISION_REPLY_NOT_REDISPATCHABLE });
      // Bound to the provider: the production provider is a class whose methods read private fields.
      const load = options.provider.loadExactAgentActivity?.bind(options.provider);
      const loadIssue = options.provider.loadExactIssue?.bind(options.provider);
      if (load === undefined || loadIssue === undefined) return Object.freeze({ disposition: "credential_unavailable" as const });
      const token = await options.accessToken();
      if (token === null) return Object.freeze({ disposition: "credential_unavailable" as const });
      let exact;
      try {
        exact = await readExactLinear(async () => load(token, source.activityId), options.sleep);
      } catch (error) {
        // Every throw is transient: the provider reports a missing Activity and a malformed response the
        // same way, so a deleted Activity cannot be told from an outage. `unavailable` charges the retry
        // budget, so a permanently missing Activity still ends after `RETRY_BUDGET_MAX_ATTEMPTS`.
        return Object.freeze({ disposition: "unavailable" as const, safeCode: safeError(error).code });
      }
      const { activity } = exact;
      if (activity.id !== source.activityId) return refused("activity_mismatch");
      if (exact.agentSessionId !== source.agentSessionId) return refused("session_mismatch");
      // A prompt that gained comment provenance and a comment-origin Activity whose comment changed are
      // both mismatches.
      if ((activity.sourceCommentId ?? null) !== source.sourceCommentId) return refused("provenance_mismatch");
      if (activity.userId !== source.ownerUserId) return refused("user_mismatch");
      const ownerDecision = verifiedLinearOwner(activity.user ?? null, source.ownerUserId);
      if (ownerDecision.disposition === "refused") return refused("user_mismatch");
      if (activity.content.type !== "prompt") return refused("wrong_type");
      const { body } = activity.content;
      // Read only once the Activity verifies. Any throw is transient; only a different Issue ID clears the marker.
      let issue;
      try {
        issue = await readExactLinear(async () => loadIssue(token, source.issueId), options.sleep);
      } catch (error) {
        return Object.freeze({ disposition: "unavailable" as const, safeCode: safeError(error).code });
      }
      if (issue.id !== source.issueId) return refused("issue_mismatch");
      return Object.freeze({
        disposition: "fetched" as const,
        instructionDigest: channelInstructionDigest(body),
        content: body,
        owner: ownerDecision.owner,
        issue: Object.freeze({ id: issue.id, identifier: issue.identifier, title: issue.title }),
      });
    },
  };
}

function refused(reason: LinearRedispatchRefusalReason): LinearRedispatchInstructionResult {
  return Object.freeze({ disposition: "refused" as const, reason });
}

/**
 * Announces and quarantines a redispatch whose retry budget is spent; the only producer of
 * `attempts_exhausted`. The Activity is enqueued first, so if the outbox throws the marker survives and
 * the give-up is reissued; the stable prefixed `sourceEventId` makes it post at most once.
 */
function giveUpOnLinearRedispatch(
  options: LinearRedispatchTriggerOptions,
  conversationId: string,
  commandId: string,
  attempts: number,
): LinearRedispatchOutcome {
  options.outbox.recordDirectMessage({
    conversationId,
    // The conversation id is the `agentSessionId`, so the target needs no lookup or credential.
    target: linearChannelDeliveryTarget({ agentSessionId: conversationId }),
    sourceEventId: `axis.linear.redispatch_exhausted:${commandId}`,
    content: Object.freeze({ type: "error" as const, body: renderRedispatchAttemptsExhausted(attempts) }),
  });
  options.runChain.quarantineRedispatch({ conversationId, commandId });
  return Object.freeze({ disposition: "attempts_exhausted" as const, attempts });
}

function renderRedispatchAttemptsExhausted(attempts: number): string {
  return LINEAR_ACTIVITY_COPY.redispatchAttemptsExhausted.replace("{attempts}", String(attempts));
}

export interface LinearRedispatchTriggerOptions {
  readonly runChain: Pick<SqliteChannelRunChainStore, "conversation" | "claimRedispatch" | "reserve" | "settle" | "rejectRedispatch" | "quarantineRedispatch" | "refundRedispatchAttempt" | "holdRedispatchAttempt">;
  readonly taskRefs: LinearChannelTaskRefLookup;
  readonly sources: LinearRedispatchSourceLookup;
  readonly instructions: LinearRedispatchInstructionReader;
  readonly dispatch: ChannelCommandPort;
  /** Where a give-up is announced: one error Activity on the Agent Session. */
  readonly outbox: Pick<ChannelOutboxPort, "recordDirectMessage">;
  /** Overridable so a test can pin the redispatched command's id. Production uses `randomUUID`. */
  readonly id?: () => string;
}

export type LinearRedispatchOutcome =
  /** No marker, or nothing due yet. */
  | { readonly disposition: "not_pending" }
  /** The ingress ledger holds no source for the marker's `commandId`. The marker is left untouched. */
  | { readonly disposition: "unresolved" }
  | { readonly disposition: "task_unresolved" }
  | { readonly disposition: "credential_unavailable" }
  | { readonly disposition: "instruction_unavailable"; readonly safeCode: string }
  /** A confirmed mismatch: the marker is cleared and nothing is dispatched. */
  | { readonly disposition: "instruction_changed"; readonly reason: LinearRedispatchRefusalReason }
  /** The run chain refused the reservation. Only `session_unconfirmed`, which is permanent, clears the marker. */
  | { readonly disposition: "reservation_refused"; readonly code: ChannelSessionRefusalCode | ChannelRunChainStoreErrorCode }
  | { readonly disposition: "dispatched"; readonly commandId: string; readonly taskId: string; readonly runId: string; readonly status: "accepted" | "interrupt_requested" | "steered" }
  | { readonly disposition: "dispatch_rejected"; readonly commandId: string; readonly safeCode: string }
  /** The retry budget is spent: the error Activity was enqueued and the marker quarantined. */
  | { readonly disposition: "attempts_exhausted"; readonly attempts: number };

/**
 * Attempts one redispatch for one conversation; safe to call repeatedly.
 *
 * Eligibility is decided only inside `claimRedispatch`. Do not add a second predicate here: one over
 * terminal rather than settled states would redispatch into an open elicitation and let an earlier
 * instruction answer it. The command gets a fresh id, because `reserve` replays on `commandId`.
 */
export async function triggerLinearRedispatch(
  options: LinearRedispatchTriggerOptions,
  input: { readonly conversationId: string },
): Promise<LinearRedispatchOutcome> {
  // Takes an attempt: null when nothing is due, or a `quarantine` obligation once the budget is spent.
  const claim = options.runChain.claimRedispatch({ conversationId: input.conversationId });
  if (claim === null) return Object.freeze({ disposition: "not_pending" as const });
  const marker = claim.marker;

  const conversation = options.runChain.conversation({ conversationId: input.conversationId });
  // `reserve` binds the conversation before a marker can exist, so null here is damaged state.
  if (conversation === null) throw new ChannelRunChainStoreError("persisted_state_conflict");
  // Never address one channel's instruction into another's session.
  if (conversation.scope.system !== "linear") throw new ChannelRunChainStoreError("persisted_state_conflict");

  // After the provider check, and before any step that can fail transiently.
  if (claim.disposition === "quarantine") return giveUpOnLinearRedispatch(options, input.conversationId, marker.commandId, claim.attempts);

  const source = options.sources.redispatchSource({ conversationId: input.conversationId, commandId: marker.commandId });
  if (source === null) return Object.freeze({ disposition: "unresolved" as const });
  // The conversation id is the `agentSessionId`; a source for another session must never be dispatched here.
  if (source.agentSessionId !== input.conversationId) return Object.freeze({ disposition: "unresolved" as const });

  const taskRef = options.taskRefs.taskRef({ conversationId: input.conversationId });
  if (taskRef === null) return Object.freeze({ disposition: "task_unresolved" as const });

  const read = await options.instructions.readRedispatchInstruction({ source });
  // A missing credential says nothing about the instruction: refund the attempt so a token outage cannot
  // exhaust the budget. `unavailable` (the Activity itself was unreadable) stays charged.
  if (read.disposition === "credential_unavailable") {
    options.runChain.refundRedispatchAttempt({ conversationId: input.conversationId, commandId: marker.commandId });
    return Object.freeze({ disposition: "credential_unavailable" as const });
  }
  if (read.disposition === "unavailable") return Object.freeze({ disposition: "instruction_unavailable" as const, safeCode: read.safeCode });
  if (read.disposition === "refused" || read.instructionDigest !== marker.instructionDigest) {
    // Either way the authorized instruction is gone.
    const reason: LinearRedispatchRefusalReason = read.disposition === "refused" ? read.reason : "instruction_changed";
    options.runChain.rejectRedispatch({ conversationId: input.conversationId, commandId: marker.commandId });
    return Object.freeze({ disposition: "instruction_changed" as const, reason });
  }

  const commandId = (options.id ?? randomUUID)();
  let reservation: ChannelRunChainReservation;
  try {
    reservation = options.runChain.reserve({
      // Built through the contract's validator so a malformed domain fails here, not inside the store's transaction.
      scope: linearChannelConversationScope({ organizationId: conversation.scope.domain, agentSessionId: input.conversationId }),
      conversationId: input.conversationId,
      tenant: conversation.tenant,
      commandId,
      kind: "continue",
      instructionDigest: marker.instructionDigest,
    }).reservation;
  } catch (error) {
    if (error instanceof ChannelSessionRefusalError || error instanceof ChannelRunChainStoreError) {
      // `session_unconfirmed` is permanent, so the marker is cleared rather than retried.
      if (error instanceof ChannelSessionRefusalError && error.code === "session_unconfirmed") {
        options.runChain.rejectRedispatch({ conversationId: input.conversationId, commandId: marker.commandId });
      }
      return Object.freeze({ disposition: "reservation_refused" as const, code: error.code });
    }
    throw error;
  }
  if (reservation.status !== "reserved") return dispositionFromReservation(reservation);

  const command = channelContinueCommand({
    commandId,
    identity: linearChannelIdentity(taskRef),
    instruction: composeLinearDispatchInstruction(read.content, {
      appUserId: source.appUserId,
      issue: read.issue,
      sourceCommentId: source.sourceCommentId,
      rootCommentId: source.rootCommentId,
    }),
  });
  const outcome: ChannelCommandOutcome = await options.dispatch.submit(command);
  const settled = options.runChain.settle({ commandId, outcome });
  // A draining Node recorded nothing: refund the attempt and hold the next one until the Node resumes.
  if (settled.status === "rejected" && settled.safeCode === "axis.router.node_draining") {
    options.runChain.holdRedispatchAttempt({ conversationId: input.conversationId, commandId: marker.commandId });
  }
  return dispositionFromReservation(settled);
}

function dispositionFromReservation(reservation: ChannelRunChainReservation): LinearRedispatchOutcome {
  if (reservation.status === "rejected") {
    if (reservation.safeCode === undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    return Object.freeze({ disposition: "dispatch_rejected" as const, commandId: reservation.commandId, safeCode: reservation.safeCode });
  }
  if (reservation.status === "accepted" || reservation.status === "interrupt_requested" || reservation.status === "steered") {
    if (reservation.runId === undefined || reservation.taskId === undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    return Object.freeze({ disposition: "dispatched" as const, commandId: reservation.commandId, taskId: reservation.taskId, runId: reservation.runId, status: reservation.status });
  }
  // `reserved` returns before this call, and `resolved_locally` only comes from a cancel path.
  throw new ChannelRunChainStoreError("persisted_state_conflict");
}
