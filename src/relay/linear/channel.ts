import { selectElicitationReply as selectElicitationReplyImport, type StructuredElicitation } from "../../shared/elicitation.js";
import { workerSelectionFromLabels, type ExecutorLabel, type WorkerSelection, type WorkerSelectionPolicy } from "../../shared/worker-selection.js";

import { channelCancelCommand, channelContinueCommand, channelStartCommand } from "../../router/channel-command.js";
import type { ChannelCommand, ChannelCommandKind, ChannelCommandOutcome, ChannelCommandPort } from "../../router/channel-command.js";
import { linearChannelIdentity } from "../../router/channel-identity.js";
import { linearTenantRef } from "../../router/channel-identity.js";
import type { SqliteChannelConversationStore } from "../../router/channel-conversation.js";
import { ChannelRunChainStoreError, type ChannelRunChainSweepDisposition, type SqliteChannelRunChainStore } from "../../router/channel-run-chain.js";
import { ChannelSessionRefusalError, channelInstructionDigest, linearChannelConversationScope, linearChannelDeliveryTarget } from "../../router/channel-conversation-types.js";
import type { ChannelDigest, ChannelRunChainReservation, LinearChannelConversationScope } from "../../router/channel-conversation-types.js";

import type { SqliteChannelOutboxStore } from "../channel-outbox.js";
import { RelayError, classifyStartupFailure, renderStartupFailure, safeError, type StartupFailure, type StartupFailureStage } from "../errors.js";
import { LINEAR_ACTIVITY_COPY } from "./activity.js";
import { authorizeLinearEvent, type LinearFeedbackLoopPort } from "./authorization.js";
import { LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE, LINEAR_UNSUPPORTED_SHAPE_CODE, type ClaimedLinearDelivery, type LinearInboxClaim, type LinearInboxPort } from "./inbox.js";
import type { LinearInstallationPort } from "./installation.js";
import { composeLinearDispatchInstruction, resolveLinearInstruction } from "./instruction.js";
import { renderLinearPromptRejection } from "./instruction.js";
import type { LinearCommentProvenance, LinearCommentRefusalCode, LinearDispatchIssue, LinearInstructionRefusal, LinearPromptRejectionReason, VerifiedLinearInstruction } from "./instruction.js";
import {
  LINEAR_LEDGER_EVENT_TYPES,
  linearChannelCommandId,
  linearDecisionLedger,
  linearLogicalEventKey,
  linearRaisedDecisionForRun,
  linearStartupIntentDigest,
  type LinearDecisionOutcomeKind,
  type LinearLogicalEventOrigin,
  type SqliteLinearLedgers,
} from "./ledgers.js";
import type { IssueExecutionContext, LinearProviderPort } from "./provider.js";
import type { LinearCredentialAccess } from "../oauth.js";
import { sha256, type EnvelopeCipher } from "../security.js";
import type { AgentSessionEventPayload, LinearActivityContent } from "../types.js";
import { parseLinearWebhook, type ParsedWebhookPayload } from "./payload.js";

/**
 * The Linear ingress tick: one claimed webhook delivery, from ciphertext to a dispatched (or refused)
 * neutral command.
 *
 * It composes, in order: the inbox claim (`./inbox.ts`), payload parsing (`./payload.ts`), installation
 * lifecycle writes (`./installation.ts`), feedback-loop and installation/Team authorization
 * (`./authorization.ts`), instruction resolution (`./instruction.ts`), Linear's durable ledgers
 * (`./ledgers.ts`), conversation identity and the single command slot
 * (`../../router/channel-conversation.ts`, `../../router/channel-run-chain.ts`), the Activity outbox
 * (`../channel-outbox.ts`), and the injected `ChannelCommandPort`. Routing and authorization decisions
 * belong to those modules; this one owns their order and which inbox terminal each outcome earns.
 *
 * Every exit ends in exactly one of `complete`, `quarantine`, `fail`, or a deliberate no-call. Two
 * outcomes the inbox log has no member for are recorded in `linear_ledger_events`: `feedback_ignored`
 * (Axis observing its own Activity, then quarantined) and `retryable` (no inbox call, so the lease
 * expires and the delivery is reclaimed; the audit row tells this apart from a crashed worker). `fail` is
 * used only for a terminal failure that produced no durable answer; a comment refusal or a definite
 * startup rejection completes instead.
 *
 * The Issue-context seam (`loadIssueExecutionContext` + `markIssueStartedAndDelegated`) fires exactly
 * once, on the `start` that first resolves the conversation's Worker selection, comment-origin or direct.
 * Retries that reuse that selection, replays, continues and cancels skip it. It is a required
 * collaborator so it cannot be silently left unwired.
 *
 * The observe tick (`./observe.ts`) owns Router event projection and queued redispatches. This module
 * writes only the reservation half of the run chain and never clears a redispatch marker; `settle`
 * already does.
 */

/**
 * Fixed user-visible copy for the dispatch outcomes this module projects. The comment acknowledgement and
 * the waiting-stop response live in `LINEAR_ACTIVITY_COPY`. Tests pin this table against literals.
 */
export const LINEAR_CHANNEL_COPY = Object.freeze({
  startedAction: "Execution started",
  continuedAction: "Prompt continued",
  dispatchedParameter: "Axis dispatched the turn to the captured Worker binding.",
  interrupting: "Axis is interrupting the active run and will pick up this instruction next.",
  /**
   * A follow-up Router handed to the running agent as a native steer. Nothing was interrupted, so the
   * Run's own answer is the session's one final reply.
   */
  steered: "Axis passed this instruction to the running agent; it takes effect at the agent's next step. Send stop to interrupt instead.",
  stopRequested: "Axis asked the active execution to stop and will not start replacement work.",
  dispatchFailed: "Axis could not dispatch this turn to the captured Worker binding.",
  /**
   * A follow-up refused because the session's Worker has left its Node's inventory. The session is intact
   * and nothing was sent to any Worker.
   */
  continueWorkerUnavailable: "Axis did not send this prompt: the Worker this session runs on is no longer available on its Node. Nothing was sent to any Worker. Ask an operator to deploy that Worker again, or adjust the Node's Worker inventory, then send the prompt again.",
  stopWorkerUnavailable: "Axis could not ask the active execution to stop: the Worker this session runs on is no longer available on its Node, so the stop request did not reach it. Ask an operator to deploy that Worker again, or adjust the Node's Worker inventory, then stop the session again.",
  /**
   * A continue the session's Worker refused because its configuration no longer matches its Node
   * inventory entry. No stop variant: a cancel carries no `executionTarget`.
   */
  continueWorkerIdentityChanged: "Axis did not run this prompt: the Worker this session runs on refused it because its configuration no longer matches its Node inventory entry. Nothing was run. Ask an operator to align that Worker's configuration with the Node inventory, then send the prompt again.",
  // Says "create a new Agent session" rather than "try again": the native session is gone, so resending
  // cannot succeed.
  continueSessionUnresumable: "Axis did not run this prompt: the previous native agent session for this conversation could not be resumed, and nothing was run. Create a new Agent session on this issue to keep working. Axis will not replay this prompt; restate it there if it is still needed.",
  /**
   * A follow-up refused because the session's Node is draining. Nothing was sent, and the same prompt
   * succeeds once the Node resumes. No stop variant: a draining Node still accepts a cancel.
   */
  continueNodeDraining: "Axis did not send this prompt: the execution Node this session runs on is paused for an upgrade or maintenance and is not taking new work. Nothing was sent to any Worker, and this session is intact. Send the prompt again in a few minutes.",
  /** The fixed guidance a `prompted` event with no local AgentSession receives. */
  sessionMissing: "Axis has no record of this Agent session, so this prompt was not dispatched. Start a new Agent session on this issue.",
  /**
   * Posted for each queued same-session instruction a stop supersedes, before `supersedeQueued`
   * terminates it.
   */
  supersededByStop: "Axis canceled this instruction: a stop for this session arrived before it was dispatched, and the stop takes priority.",
  /**
   * A stop whose reservation is refused (`reservation_in_flight`, or `run_state_conflict`). Nothing was
   * stopped; any instruction queued ahead of the stop was already superseded before the refusal.
   */
  stopNotApplied: "Axis did not stop the active execution: an earlier command for this session is still in flight and has not been confirmed yet, so nothing has been interrupted. Any of this session's instructions that were still queued and not yet dispatched have been canceled. Send the stop again once the earlier command settles.",
  /**
   * A stop refused with `session_unconfirmed`. Unlike `stopNotApplied`, resending will not help: an
   * unconfirmed session never settles on its own.
   */
  stopSessionUnconfirmed: "Axis did not stop anything: this session has no execution Axis can confirm is running, because an earlier request's outcome could never be confirmed. Any of this session's instructions that were still queued and not yet dispatched have been canceled. Create a new Agent session on this issue to keep working.",
  /**
   * A continue whose post-submit convergence left the outcome still owed (`STARTUP_OUTCOME_STILL_OWED`).
   * It neither says the prompt did not run (a native turn may exist) nor invites a resend (which could
   * duplicate it).
   */
  continueOutcomePending: "Axis has not yet confirmed whether this prompt reached the active execution. It may already be running. Axis will not resend it automatically, so do not send it again while this is unresolved.",
  /** The stop's counterpart to `continueOutcomePending`. */
  stopOutcomePending: "Axis has not yet confirmed whether this stop reached the active execution. The execution may or may not have been asked to stop yet. Axis will not resend the stop automatically.",
} as const);

/** Backoff sleep for the instruction resolver's exact reads and for post-submit convergence. */
const DEFAULT_SLEEP = (milliseconds: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, milliseconds));

/**
 * How many times a post-submit failure asks Router about its own command before leaving the reservation
 * to `reconcileStale`.
 *
 * One ask is not enough: the Node's dispatch receipt and the Run's own event travel on concurrent paths,
 * and when the dispatch response fails it often loses the race to its own event, so an immediate ask
 * answers `unknown` for a command the Worker already accepted. Five attempts at 25ms doubling sleep at
 * most 375ms on the serial ingress loop, spent only on an already-failed dispatch; `reconcile` itself is
 * an in-process read. Only `unknown` is re-asked: `absent` is stable because Router writes the command
 * row before dispatching.
 */
const CONVERGENCE_ATTEMPTS = 5;
const CONVERGENCE_BACKOFF_BASE_MS = 25;

/**
 * Whether a convergence answer leaves the dispatch's outcome still owed to a later pass. It decides which
 * of `renderStartupFailure`'s ambiguous sentences a start gets, and whether a continue or stop gets
 * `*OutcomePending` (owed) or `*NeverReachedRouter` (decided `absent`).
 *
 * `true`: the reservation stays held and `reconcileSettledEarly` / `reconcileStale` ask Router again, so
 * "could not confirm" would be false; a reservation that never confirms is answered terminally by the
 * observe tick (`answerAbsentLinearStarts`). `false`: Axis has a decided answer. `absent` is decided
 * because Router writes the command row before dispatching.
 *
 * A total `Record`, so a new disposition must be classified at compile time; either default would be
 * wrong for some case.
 */
const STARTUP_OUTCOME_STILL_OWED: Readonly<Record<ChannelRunChainSweepDisposition, boolean>> = Object.freeze({
  unknown: true,
  accepted_unresolved: true,
  settle_refused: true,
  absent: false,
  accepted: false,
  interrupt_requested: false,
  // Decided: the instruction reached the running Run.
  steered: false,
  // Decided: the unconfirmed sweep gives a terminal answer. `reconcileNow` never reports it.
  unconfirmed: false,
});

async function requiredAccessToken(read: () => Promise<string | null>): Promise<string> {
  const token = await read();
  if (token === null) throw new RelayError("linear.auth.revoked", "The Linear credential is unavailable.", 403);
  return token;
}

/**
 * The Issue-context step. Called once, on the `start` that first resolves the conversation's Worker
 * selection, after authorization and the durable ledger writes and before dispatch, so a failure prevents
 * the dispatch. A transient failure is thrown retryably; each definite one has its own code
 * (`classifyIssueSyncFailure`).
 */
export interface LinearIssueStartContext {
  readonly accessToken: string;
  readonly issueId: string;
  readonly teamId: string;
  readonly appUserId: string;
  readonly agentSessionId: string;
}

export interface LinearIssueContextStep {
  synchronizeIssueForStart(context: LinearIssueStartContext): Promise<{
    readonly issue: Awaited<ReturnType<LinearProviderPort["loadIssueExecutionContext"]>>;
    readonly queryCount: 1;
    readonly lookupLatencyMs: number;
  }>;
}

export const LINEAR_WORKER_ROUTING_AUDIT_VERSION = "1.0.0" as const;

interface LinearWorkerRoutingAuditEnvelope {
  readonly contractVersion: typeof LINEAR_WORKER_ROUTING_AUDIT_VERSION;
  readonly issueId: string;
  readonly teamId: string;
  readonly agentSessionId: string;
  readonly conversationId: string;
  readonly commandId: string;
  /** Labels are embedded in the required execution-context request; no extra Issue lookup occurs. */
  readonly labelSource: "issue_context";
  readonly requiredLookup: false;
  readonly queryCount: number;
  readonly lookupLatencyMs: number;
}

/**
 * Safe routing evidence: it carries neither arbitrary label names nor instruction content. The Node owns
 * Worker selection, so a start records only which of the fixed executor labels its source carried, plus
 * the channel's own identifiers. Which Worker ran is the Node's evidence to report.
 */
export type LinearWorkerRoutingAudit =
  | (LinearWorkerRoutingAuditEnvelope & { readonly outcome: "routing_context"; readonly executorLabels: readonly ExecutorLabel[] })
  | (LinearWorkerRoutingAuditEnvelope & { readonly outcome: "unavailable"; readonly safeCode: string });

export interface LinearWorkerRoutingOptions {
  /** Required on the opt-in path so every selection or refusal leaves evidence. */
  readonly audit: { record(entry: LinearWorkerRoutingAudit): void };
}

/**
 * The default step: read the issue's execution context, refuse if its Team no longer matches the captured
 * authority, then mark it started and delegated. The Team check is an authenticated reread, so an issue
 * moved to another Team after the webhook is not acted on under the old Team's authority.
 */
export function linearIssueContextStep(provider: Pick<LinearProviderPort, "loadIssueExecutionContext" | "markIssueStartedAndDelegated">): LinearIssueContextStep {
  return Object.freeze({
    async synchronizeIssueForStart(context: LinearIssueStartContext) {
      const startedAt = Date.now();
      let issue: IssueExecutionContext;
      try {
        issue = await provider.loadIssueExecutionContext(context.accessToken, context.issueId);
      } catch (error) {
        throw classifyIssueSyncFailure(error, "read");
      }
      const lookupLatencyMs = Math.max(0, Date.now() - startedAt);
      if (issue.teamId !== context.teamId) throw new RelayError("axis.relay.issue_sync_team_changed", "The issue Team changed outside the captured authority.", 403);
      try {
        await provider.markIssueStartedAndDelegated(context.accessToken, issue, context.appUserId);
      } catch (error) {
        throw classifyIssueSyncFailure(error, "update");
      }
      return Object.freeze({ issue, queryCount: 1 as const, lookupLatencyMs });
    },
  });
}

/**
 * Names the cause of one issue-sync failure. A `RelayError` (the provider's retryable transport error)
 * passes through. Anything else is definite and recognized by the code in the provider's `Error` message,
 * which `test/relay/linear/channel-e2e.test.ts` pins against the real provider. An unrecognized code
 * falls back to `issue_sync_failed`.
 */
function classifyIssueSyncFailure(error: unknown, step: "read" | "update"): RelayError {
  if (error instanceof RelayError) return error;
  const code = error instanceof Error ? error.message : "";
  if (step === "read" && code === "linear.provider.issue_not_found") {
    return new RelayError("axis.relay.issue_sync_not_found", "Linear did not find the issue to synchronize.", 404);
  }
  if (step === "read" && code === "linear.provider.malformed_issue_context") {
    return new RelayError("axis.relay.issue_sync_malformed", "Linear returned an issue execution context Axis cannot verify.", 502);
  }
  // A GraphQL error, `issueUpdate` answering `success: false`, or a non-2xx
  // answer that is not a GraphQL response: Linear answered, and the answer was no.
  if (code.startsWith("linear.graphql.") || code === "linear.provider.http_failure") {
    return step === "read"
      ? new RelayError("axis.relay.issue_sync_read_rejected", "Linear refused to return the issue execution context.", 502)
      : new RelayError("axis.relay.issue_sync_update_rejected", "Linear rejected the issue start and delegate update.", 502);
  }
  return new RelayError("axis.relay.issue_sync_failed", "Relay could not synchronize the verified request before dispatch.", 502);
}

/**
 * A run-chain refusal carried as an error to the classification site. `code` is the durable-state
 * conflict code `classifyStartupFailure` recognizes; `refusalCode` is the run chain's own.
 */
class LinearReservationRefusal extends RelayError {
  readonly refusalCode: string;
  constructor(refusalCode: string) {
    super("linear.ingress.logical_event_conflict", "The command conflicts with this conversation's retained run state.", 409);
    this.refusalCode = refusalCode;
  }
}

/**
 * Which prompt-rejection reason a reservation failure renders as.
 *
 * `ChannelSessionRefusalError` codes mean "cannot be run as asked" (`logical_conflict`), except
 * `reservation_in_flight` ("not yet") and `session_unconfirmed` ("this session is over"). The two
 * `ChannelRunChainStoreError` codes are faults, not refusals (`state_unavailable`):
 * `persisted_state_conflict` includes an ordinary locked-database moment, and
 * `schema_version_unsupported` is a rollback. An unclassified code defaults to `logical_conflict`.
 */
function reservationPromptReason(refusalCode: string): LinearPromptRejectionReason {
  if (refusalCode === "reservation_in_flight") return "command_in_flight";
  if (refusalCode === "session_unconfirmed") return "session_unconfirmed";
  if (refusalCode === "persisted_state_conflict" || refusalCode === "schema_version_unsupported") return "state_unavailable";
  return "logical_conflict";
}

/**
 * The startup-failure code a created start's prompt refusal renders under, by reason. Unlisted reasons
 * fall back to `axis.relay.internal`. `user_mismatch` stays one reason so authorization does not reveal
 * which owner check failed.
 */
const createdStartupFailureCodeByReason: Readonly<Partial<Record<LinearPromptRejectionReason, string>>> = Object.freeze({
  capability_unavailable: "axis.relay.linear_capability_unavailable",
  credential_unavailable: "axis.relay.linear_credential_unavailable",
  provider_unavailable: "axis.relay.linear_user_unavailable",
  user_mismatch: "axis.relay.owner_unverified",
  // No author, or Axis's own App User: both render as an unverified owner.
  user_missing: "axis.relay.owner_unverified",
  axis_authored: "axis.relay.owner_unverified",
  issue_unavailable: "axis.relay.issue_unavailable",
  issue_mismatch: "axis.relay.issue_mismatch",
  activity_mismatch: "axis.relay.activity_mismatch",
  session_mismatch: "axis.relay.activity_session_mismatch",
  wrong_type: "axis.relay.activity_not_prompt",
});

/** A prompt refusal, carried as an error so it reaches the one classification site. */
class LinearPromptRejection extends RelayError {
  readonly reason: LinearPromptRejectionReason;
  constructor(reason: LinearPromptRejectionReason) {
    super("axis.relay.missing_instruction", "The authenticated Linear event did not contain an instruction that Axis can dispatch.", 422);
    this.reason = reason;
  }
}

export interface LinearIngressOptions {
  /** The lease owner. One per process; the inbox refuses an empty one. */
  readonly owner: string;
  readonly inbox: LinearInboxPort;
  readonly cipher: Pick<EnvelopeCipher, "encrypt" | "decrypt">;
  readonly credentials: LinearCredentialAccess;
  readonly installations: LinearInstallationPort;
  readonly provider: LinearProviderPort;
  readonly ledgers: SqliteLinearLedgers;
  readonly conversations: Pick<SqliteChannelConversationStore, "openOrResume" | "recordMessageInvocation">;
  /** `reconcileNow` is required: post-submit convergence depends on it. */
  readonly runChain: Pick<SqliteChannelRunChainStore, "reserve" | "settle" | "reservation" | "initialStartReservation" | "currentRun" | "resolveLocally" | "openDecision" | "reconcileNow">;
  readonly outbox: Pick<SqliteChannelOutboxStore, "recordDirectMessage" | "waitingDecisionsForConversation">;
  /** The one dispatch path into Router. */
  readonly dispatch: ChannelCommandPort;
  /** The Activity-ledger half of the feedback-loop check, implemented by `LinearActivityDeliverer` (`./delivery.ts`). */
  readonly feedback: LinearFeedbackLoopPort;
  /** Required; see the module header. */
  readonly issueContext: LinearIssueContextStep;
  /** The audit sink for a start's routing context. Required: every start dispatches with a resolved selection. */
  readonly workerRouting: LinearWorkerRoutingOptions;
  readonly leaseMs?: number;
  /** No `now` option: every timestamp comes from the clock of the store that writes it. */
  readonly sleep?: (milliseconds: number) => Promise<void>;
}

export type LinearIngressDisposition =
  /** No delivery was claimable. */
  | { readonly disposition: "idle" }
  /** A shape Axis cannot parse. Quarantined; no command exists. */
  | { readonly disposition: "unsupported_shape" }
  /** A Team-access change was applied to the installation. */
  | { readonly disposition: "permission_change_applied" }
  /** The installation was revoked and its credential deleted. */
  | { readonly disposition: "installation_revoked" }
  /** Axis observed its own Activity. Audited and quarantined. */
  | { readonly disposition: "feedback_ignored" }
  /** An authority gate refused. `safeCode` is its own code. */
  | { readonly disposition: "authorization_refused"; readonly safeCode: string }
  /** The comment channel refused. `responded` says whether a fixed body was queued. */
  | { readonly disposition: "comment_refused"; readonly safeCode: LinearCommentRefusalCode; readonly responded: boolean }
  /** A `prompted` event for an AgentSession Axis holds no record of. Guidance queued; nothing dispatched. */
  | { readonly disposition: "session_missing" }
  /** A definite `created` failure retained as a rejected startup, with its fixed error Activity. */
  | { readonly disposition: "startup_rejected"; readonly stage: string; readonly reason: string }
  /**
   * A `created` start that was admitted and then failed. The startup stays admitted and the user gets the
   * startup-failure copy.
   *
   * `converged` is present when the failure followed `dispatch.submit` and convergence returned an answer
   * (never `accepted`, which reports `dispatched`). `reason` is `outcome_pending` while the outcome is
   * still owed (`unknown`, `accepted_unresolved`, `settle_refused`, or no answer) and `outcome_unknown`
   * for `absent`. Only tests read this field.
   */
  | { readonly disposition: "startup_failed"; readonly stage: string; readonly reason: string; readonly converged?: ChannelRunChainSweepDisposition }
  /** A prompt that could not be verified. `responded` says whether the fixed body was queued. */
  | { readonly disposition: "prompt_rejected"; readonly reason: LinearPromptRejectionReason; readonly responded: boolean }
  /**
   * The later of a comment mention's `created`/`prompted` pair, in either order, pointing at the command
   * the first one made. Nothing dispatched, no Activity queued.
   */
  | { readonly disposition: "already_started"; readonly commandId: string }
  /** The run chain refused the reservation itself. */
  | { readonly disposition: "reservation_refused"; readonly code: string }
  /** A stop on a waiting conversation, closed locally with one response Activity. No Router command. */
  | { readonly disposition: "closed_waiting"; readonly commandId: string }
  /** Router accepted the command, or accepted it as an interrupt of a still-executing Run. */
  | { readonly disposition: "dispatched"; readonly commandId: string; readonly kind: ChannelCommandKind; readonly status: "accepted" | "interrupt_requested" | "steered" }
  /** Router rejected the command outright. */
  | { readonly disposition: "dispatch_rejected"; readonly commandId: string; readonly safeCode: string }
  /** A failure that may still succeed. Audited `retryable`; the lease is left to expire. */
  | { readonly disposition: "retryable"; readonly safeCode: string }
  /**
   * The delivery spent its retry budget; the user was told and it was quarantined. `responded` is false
   * only for a delivery with no AgentSession to answer on.
   */
  | { readonly disposition: "attempts_exhausted"; readonly attempts: number; readonly responded: boolean }
  /**
   * A terminal failure; the delivery is `fail`ed. `converged` is present when the failure followed
   * `dispatch.submit` and convergence returned an answer. An `accepted`/`interrupt_requested` answer
   * normally exits as `dispatched` instead, so it appears here only if the settled reservation cannot be
   * read back. Only tests read this field.
   */
  | { readonly disposition: "failed"; readonly safeCode: string; readonly converged?: ChannelRunChainSweepDisposition };

/**
 * One tick's result. A non-null `claimed` tells the scheduled loop work was done and it may run again
 * immediately; `outcome` says what that work was.
 */
export interface LinearIngressTickResult {
  readonly claimed: string | null;
  readonly outcome: LinearIngressDisposition;
}

const IDLE: LinearIngressTickResult = Object.freeze({ claimed: null, outcome: Object.freeze({ disposition: "idle" as const }) });

/** Binds one set of collaborators into the nullary tick the scheduled loop runs. */
export function linearIngressTick(options: LinearIngressOptions): () => Promise<LinearIngressTickResult> {
  return async () => processLinearIngressTick(options);
}

/**
 * The give-up terminal for a delivery whose retry budget is spent: tell the user, then quarantine.
 *
 * Announce first: if the outbox refuses, this throws with the delivery still `processing`, so the
 * obligation is reissued on the next claim rather than ending silently. The source event id is prefixed
 * because the outbox keeps the first content per `(conversationId, sourceEventId)` and earlier exits may
 * already have used the bare delivery id; the prefix stays stable across reissued obligations.
 */
function quarantineExhaustedDelivery(
  options: LinearIngressOptions,
  claimed: ClaimedLinearDelivery,
  attempts: number,
  enqueue: (agentSessionId: string, sourceEventId: string, content: LinearActivityContent) => void,
): LinearIngressDisposition {
  const agentSessionId = exhaustedDeliverySession(options, claimed);
  if (agentSessionId !== null) {
    enqueue(agentSessionId, `axis.inbox.attempts_exhausted:${claimed.deliveryId}`, {
      type: "error",
      body: renderAttemptsExhausted(attempts),
    });
  }
  options.inbox.quarantine(claimed.deliveryId, options.owner, LINEAR_INBOX_ATTEMPTS_EXHAUSTED_CODE);
  return Object.freeze({ disposition: "attempts_exhausted" as const, attempts, responded: agentSessionId !== null });
}

/**
 * The AgentSession an exhausted delivery can be answered on, or null for a lifecycle envelope or a
 * payload that no longer decrypts or parses. Null is the one silent give-up, which is why
 * `attempts_exhausted` carries `responded`.
 */
function exhaustedDeliverySession(options: LinearIngressOptions, claimed: ClaimedLinearDelivery): string | null {
  try {
    const payload = parseLinearWebhook(Buffer.from(options.cipher.decrypt(claimed.payloadCiphertext), "utf8"));
    return payload.type === "AgentSessionEvent" ? payload.agentSession.id : null;
  } catch {
    return null;
  }
}

/** Substitutes the attempt count into `LINEAR_ACTIVITY_COPY.attemptsExhausted`. */
function renderAttemptsExhausted(attempts: number): string {
  return LINEAR_ACTIVITY_COPY.attemptsExhausted.replace("{attempts}", String(attempts));
}

/**
 * Runs one claimed delivery through every gate to a Router dispatch, a durable refusal, or a deliberate
 * retry. Every return has made exactly one inbox terminal call, or deliberately none (`retryable`).
 */
export async function processLinearIngressTick(options: LinearIngressOptions): Promise<LinearIngressTickResult> {
  const claim = options.inbox.claim(options.owner, options.leaseMs);
  if (claim === null) return IDLE;
  return new LinearDeliveryTick(options, claim.delivery).run(claim);
}

/** A `created` start's progress, read by the failure path to decide how a failure is retained. */
interface StartupProgress {
  readonly intentDigest: string;
  logicalEventKey: string;
  /**
   * Whether a failure may be retained as a rejected startup. A direct start is eligible from the outset;
   * a comment-origin one only once its comment gates pass, because a comment refusal has its own durable
   * channel; and a prompt refusal unless the prompt-first sibling already bound the session.
   */
  rejectable: boolean;
  /**
   * Set once `recordStartupAcceptance` admitted the start. Before admission a definite failure is retained
   * as a rejected startup; after it the accepted startup stays and the user gets failure copy.
   */
  admittedCommandId: string | null;
}

/** An `AgentSessionEvent` that passed authorization and instruction resolution. */
interface ResolvedEvent {
  readonly event: AgentSessionEventPayload;
  readonly agentSessionId: string;
  readonly accessToken: () => Promise<string | null>;
  readonly verified: VerifiedLinearInstruction;
  readonly provenance: LinearCommentProvenance | null;
  readonly dispatchIssue: LinearDispatchIssue | null;
  readonly activityId: string | null;
  readonly logicalEventKey: string;
  readonly commandId: string;
}

/** A resolved event bound to its conversation, with its command kind and recorded input. */
interface PreparedCommand extends ResolvedEvent {
  readonly scope: LinearChannelConversationScope;
  readonly conversationId: string;
  readonly instructionDigest: ChannelDigest;
  readonly kind: ChannelCommandKind;
  readonly closesWaiting: boolean;
  readonly dispatchedInstruction: string;
}

interface PendingDecisionOutcome {
  readonly decisionId: string;
  readonly outcome: LinearDecisionOutcomeKind;
  readonly selected: { readonly optionId: string; readonly label: string } | null;
}

type ActivityQueue = (agentSessionId: string, sourceEventId: string, content: LinearActivityContent) => void;

/**
 * One claimed delivery. The success path is `#process`; any throw lands in `#handleFailure`, which reads
 * the progress fields below to choose the outcome.
 */
class LinearDeliveryTick {
  readonly #options: LinearIngressOptions;
  readonly #claimed: ClaimedLinearDelivery;
  readonly #deliveryId: string;
  readonly #sleep: (milliseconds: number) => Promise<void>;

  #event: AgentSessionEventPayload | null = null;
  #startup: StartupProgress | null = null;
  #reservedCommandId: string | null = null;
  #submitAttempted = false;

  constructor(options: LinearIngressOptions, claimed: ClaimedLinearDelivery) {
    this.#options = options;
    this.#claimed = claimed;
    this.#deliveryId = claimed.deliveryId;
    this.#sleep = options.sleep ?? DEFAULT_SLEEP;
  }

  async run(claim: LinearInboxClaim): Promise<LinearIngressTickResult> {
    // A spent retry budget is handled outside the try/catch, whose `fail()` would end the delivery
    // silently. If telling the user fails, the lease expires and the obligation is handed back; a broken
    // outbox therefore holds the head of the queue, which is loud rather than silent.
    if (claim.disposition === "quarantine") return this.#finish(quarantineExhaustedDelivery(this.#options, this.#claimed, claim.attempts, this.#enqueue));
    let outcome: LinearIngressDisposition;
    try {
      outcome = await this.#process();
    } catch (error) {
      outcome = await this.#handleFailure(error);
    }
    return this.#finish(outcome);
  }

  #finish(outcome: LinearIngressDisposition): LinearIngressTickResult {
    return Object.freeze({ claimed: this.#deliveryId, outcome });
  }

  /** Queues one Activity. For Linear the conversation id is the `agentSessionId`, which is the whole delivery target. */
  readonly #enqueue: ActivityQueue = (agentSessionId, sourceEventId, content) => {
    this.#options.outbox.recordDirectMessage({
      conversationId: agentSessionId,
      target: linearChannelDeliveryTarget({ agentSessionId }),
      sourceEventId,
      content,
    });
  };

  #complete(outcome: LinearIngressDisposition): LinearIngressDisposition {
    this.#options.inbox.complete(this.#deliveryId, this.#options.owner);
    return outcome;
  }

  // -------------------------------------------------------------------------
  // Success path
  // -------------------------------------------------------------------------

  async #process(): Promise<LinearIngressDisposition> {
    const options = this.#options;
    const payload: ParsedWebhookPayload = parseLinearWebhook(Buffer.from(options.cipher.decrypt(this.#claimed.payloadCiphertext), "utf8"));

    if (payload.type === "Unsupported") {
      // Not reachable normally: `accept` quarantines unrecognized envelopes. Kept so a payload that stops
      // parsing between accept and claim cannot reach a dispatch.
      options.inbox.quarantine(this.#deliveryId, options.owner, LINEAR_UNSUPPORTED_SHAPE_CODE);
      return { disposition: "unsupported_shape" };
    }
    if (payload.type === "PermissionChange") {
      // The Team write is organization-scoped: a change naming a stale App User still edits the current
      // installation's allowlist. Only the public-Team switch is App-User-scoped.
      options.installations.applyPermissionChange(payload);
      return this.#complete({ disposition: "permission_change_applied" });
    }
    if (payload.type === "OAuthApp") {
      const credentialRef = options.installations.revoke(payload.organizationId, payload.appUserId);
      if (credentialRef !== null) await options.credentials.delete(credentialRef);
      return this.#complete({ disposition: "installation_revoked" });
    }

    this.#event = payload;
    const resolved = await this.#resolve(payload);
    if ("disposition" in resolved) return resolved;

    const reconciled = this.#reconcileSiblings(resolved);
    if (reconciled !== null) return reconciled;

    const prepared = this.#prepare(resolved);
    const workerSelection = prepared.kind === "start" ? await this.#startWorkerSelection(prepared) : undefined;
    if (prepared.kind === "cancel") this.#cutQueueForStop(prepared.agentSessionId);

    const reservation = this.#reserve(prepared, workerSelection);
    this.#acknowledge(prepared);

    // A replayed reservation that already settled carries its own outcome; re-dispatching would ask Router
    // to accept a `commandId` it may have retired for a different reason than idempotent replay.
    if (reservation.status !== "reserved") {
      // Read before the terminal call: `dispositionFromReservation` can throw, and throwing after
      // `complete` would make the failure path attempt a second terminal call.
      return this.#complete(dispositionFromReservation(reservation));
    }

    const decisionOutcome = this.#claimDecision(prepared, reservation);
    if (prepared.closesWaiting) return this.#closeWaiting(prepared);
    return this.#submit(prepared, reservation, decisionOutcome);
  }

  /**
   * Authorization and instruction resolution, up to the command identity. Returns a disposition for an
   * event that ends here, else the resolved event.
   */
  async #resolve(event: AgentSessionEventPayload): Promise<ResolvedEvent | LinearIngressDisposition> {
    const options = this.#options;
    const agentSessionId = event.agentSession.id;

    if (event.action === "created") {
      // Taken before the authorization gates, so an authorization refusal can still be retained as a
      // rejected startup.
      this.#startup = {
        intentDigest: linearStartupIntentDigest(event),
        logicalEventKey: linearLogicalEventKey({ kind: "session", action: "created", agentSessionId }),
        rejectable: (event.agentSession.sourceCommentId ?? event.agentSession.commentId ?? null) === null,
        admittedCommandId: null,
      };
    }

    const decision = authorizeLinearEvent(event, { feedback: options.feedback, installations: options.installations });
    if (decision.disposition === "feedback_ignored") {
      options.ledgers.recordChannelEvent(this.#deliveryId, LINEAR_LEDGER_EVENT_TYPES.feedbackIgnored, { agentSessionId });
      options.inbox.quarantine(this.#deliveryId, options.owner, decision.safeCode);
      return { disposition: "feedback_ignored" };
    }
    if (decision.disposition === "refused") {
      // Both refusal codes are recorded as-is here; `./delivery.ts` collapses them into
      // `linear.auth.revoked` on the outbox row.
      throw new RelayError(decision.safeCode, "The Linear installation does not cover this event.", 403);
    }
    const accessToken = async (): Promise<string | null> => options.credentials.accessToken(decision.credentialRef);

    // A direct `prompted` event for an AgentSession with no local record. Checked before instruction
    // resolution, which would spend a provider read and could mask this as `provider_unavailable`. A
    // comment-origin prompt may open a session (prompt-first); a direct one may not. Answered with fixed
    // guidance and queued without opening a conversation, so no run-chain state is minted for a session
    // Axis never admitted.
    if (event.action === "prompted"
      && (event.agentActivity?.sourceCommentId ?? null) === null
      && options.ledgers.session(agentSessionId) === null) {
      this.#enqueue(agentSessionId, `axis.session.unknown:${agentSessionId}:${event.agentActivity?.id ?? this.#deliveryId}`, {
        type: "error", body: LINEAR_CHANNEL_COPY.sessionMissing,
      });
      return this.#complete({ disposition: "session_missing" });
    }

    const resolution = await resolveLinearInstruction(event, {
      provider: options.provider,
      decisions: linearDecisionLedger({ ledger: options.ledgers, runChain: options.runChain, outbox: options.outbox }),
      accessToken,
      sleep: this.#sleep,
    });
    if (resolution.disposition === "refused") return this.#refuse(event, resolution.refusal);

    const provenance = resolution.provenance;
    if (this.#startup !== null && provenance !== null) this.#startup.rejectable = true;

    const activityId = event.agentActivity?.id ?? null;
    if (event.action === "prompted" && activityId === null) {
      throw new RelayError("axis.relay.invalid_transition", "A prompt requires a stable Agent Activity identity.", 409);
    }

    const origin: LinearLogicalEventOrigin = provenance !== null
      ? (event.action === "created"
        ? { kind: "comment", action: "created", sourceCommentId: provenance.sourceCommentId }
        : { kind: "comment", action: "prompted", sourceCommentId: provenance.sourceCommentId, activityId: activityId! })
      : (event.action === "created"
        ? { kind: "session", action: "created", agentSessionId }
        : { kind: "session", action: "prompted", agentSessionId, activityId: activityId! });
    const logicalEventKey = linearLogicalEventKey(origin);
    if (this.#startup !== null) this.#startup.logicalEventKey = logicalEventKey;

    options.ledgers.bindSession({
      agentSessionId,
      organizationId: event.organizationId,
      appUserId: event.appUserId,
      issueId: event.agentSession.issueId,
      teamId: event.agentSession.issueTeamId,
      creatorId: event.agentSession.creatorId,
      rootCommentId: provenance?.rootCommentId ?? null,
    });

    return {
      event,
      agentSessionId,
      accessToken,
      verified: resolution.instruction,
      provenance,
      dispatchIssue: resolution.issue,
      activityId,
      logicalEventKey,
      commandId: linearChannelCommandId(logicalEventKey),
    };
  }

  /**
   * Ends an event whose command already exists: the later half of a comment mention's `created`/`prompted`
   * pair, or a replay of an admitted start. Admits a fresh `created` start as a side effect.
   */
  #reconcileSiblings(resolved: ResolvedEvent): LinearIngressDisposition | null {
    const options = this.#options;
    const { event, agentSessionId, provenance, logicalEventKey, commandId } = resolved;
    const startup = this.#startup;

    if (provenance !== null) {
      const started = options.ledgers.commentInvocation(provenance.sourceCommentId);
      if (started !== null && started.disposition === "accepted" && started.agentSessionId === agentSessionId && started.commandId !== null) {
        if (startup !== null) {
          // Prompt-first: Linear delivered the mention's `prompted` Activity before `created`. The prompt
          // started the session; `created` dispatches nothing and is retained as an accepted startup
          // pointing at the prompt's command, so a further arrival replays. The prompt's text is kept.
          options.ledgers.recordStartupAcceptance({
            agentSessionId,
            logicalEventKey,
            startupIntentDigest: startup.intentDigest,
            commandId: started.commandId,
            inputSourceEventId: started.deliveryId,
          });
          startup.admittedCommandId = started.commandId;
        }
        // Created-first is the mirror: `prompted` dispatches nothing, records no startup acceptance and
        // queues no Activity, and redeliveries re-read the same row. Without this the sibling would be
        // refused `reservation_in_flight`, or reserve a continue and trip `recordCommentInvocation`'s
        // rebind guard, and either would tell the user to resend a comment they wrote once.
        //
        // Only `accepted` with a `commandId` qualifies, so a genuine rebind still conflicts. The match is
        // keyed on `sourceCommentId` alone: Linear mints only the `created`/`prompted` pair per mention,
        // and a second turn is a new reply comment.
        return this.#complete({ disposition: "already_started", commandId: started.commandId });
      }
    }

    if (startup !== null) {
      const admitted = options.ledgers.recordStartupAcceptance({
        agentSessionId, logicalEventKey, startupIntentDigest: startup.intentDigest, commandId, inputSourceEventId: this.#deliveryId,
      });
      startup.admittedCommandId = admitted.commandId;
      // A replay of an admitted startup continues only while its reservation is unsettled (a crash between
      // admitting, reserving and settling), where `reserve` replies `replayed`. A settled one ends here:
      // continuing would reclassify it as a `continue`, which `reserve` refuses as `command_rebound`.
      if (!admitted.created) {
        const existing = options.runChain.reservation({ commandId: admitted.commandId });
        if (admitted.commandId !== commandId || (existing !== null && existing.status !== "reserved")) {
          return this.#complete({ disposition: "already_started", commandId: admitted.commandId });
        }
      }
    }
    return null;
  }

  /** Opens the conversation, records the comment invocation, picks the command kind and records the input. */
  #prepare(resolved: ResolvedEvent): PreparedCommand {
    const options = this.#options;
    const { event, agentSessionId, verified, provenance, dispatchIssue, activityId, commandId } = resolved;

    const scope = linearChannelConversationScope({ organizationId: event.organizationId, agentSessionId });
    const conversationId = options.conversations.openOrResume({ scope, admitWorkerRouting: true as const }).conversation.conversationId;

    const instructionDigest: ChannelDigest = channelInstructionDigest(verified.content);
    if (provenance !== null) {
      // The neutral message-invocation table is the Comment-level idempotency and rebind gate; Linear's
      // ledger records the thread root and prompt Activity alongside. Both are written before dispatch.
      try {
        options.conversations.recordMessageInvocation({ scope, messageId: provenance.sourceCommentId, conversationId, instructionDigest });
      } catch (error) {
        if (error instanceof ChannelSessionRefusalError && error.code === "message_rebound") {
          throw new RelayError("linear.ingress.logical_event_conflict", "The comment identity conflicts with a prior invocation.", 409);
        }
        throw error;
      }
    }

    const currentRun = options.runChain.currentRun({ conversationId });
    // `kind` must be stable across redeliveries, because `reserve`'s replay check includes it in the
    // intent digest. A command already reserved reuses its recorded kind; a new one is `cancel` for a
    // stop, else `start` when the conversation has no Run yet and `continue` otherwise.
    const kind: ChannelCommandKind = options.runChain.reservation({ commandId })?.kind
      ?? (isStopSignal(event) ? "cancel" : currentRun === null ? "start" : "continue");

    // The command dispatches the recorded encrypted input, not a fresh composition: the envelope's title
    // comes from a fresh issue read, and a renamed issue would otherwise present the same `commandId` with
    // different bytes, which Router refuses as `axis.router.intent_conflict`. A stop reserves `cancel`,
    // which carries no instruction, so it needs no issue. `./redispatch.ts` composes queued continues
    // through the same function.
    const composeInstruction = (): string => {
      if (dispatchIssue === null) {
        throw new RelayError("axis.relay.invalid_transition", "A dispatched Linear instruction requires its exact issue identity.", 409);
      }
      return composeLinearDispatchInstruction(verified.content, {
        appUserId: event.appUserId,
        issue: dispatchIssue,
        sourceCommentId: provenance?.sourceCommentId ?? null,
        rootCommentId: provenance?.rootCommentId ?? null,
        guidance: event.guidance,
      });
    };
    const recordedInput = options.ledgers.recordSessionInput({
      sourceEventId: this.#deliveryId,
      agentSessionId,
      commandId,
      activityId,
      sourceCommentId: provenance?.sourceCommentId ?? null,
      inputDigest: sha256(verified.content),
      inputCiphertext: options.cipher.encrypt(kind === "cancel" ? verified.content : composeInstruction()),
      owner: verified.owner,
    });

    return {
      ...resolved,
      scope,
      conversationId,
      instructionDigest,
      kind,
      closesWaiting: kind === "cancel" && currentRun?.state === "waiting_for_input",
      dispatchedInstruction: options.cipher.decrypt(recordedInput.inputCiphertext),
    };
  }

  /**
   * The Worker selection a start dispatches with. Reuses the conversation's recorded selection, else
   * synchronizes the issue and builds one from its labels, auditing either result. A definite failure
   * still reserves a rejected first-start tombstone so the start stays idempotent, but never guesses a
   * default Worker.
   */
  async #startWorkerSelection(prepared: PreparedCommand): Promise<WorkerSelection | undefined> {
    const options = this.#options;
    const { event, agentSessionId, conversationId, commandId } = prepared;
    const recorded = options.runChain.initialStartReservation({ conversationId })?.workerSelection;
    if (recorded !== undefined) return recorded;

    const audit = {
      contractVersion: LINEAR_WORKER_ROUTING_AUDIT_VERSION,
      issueId: event.agentSession.issueId,
      teamId: event.agentSession.issueTeamId,
      agentSessionId,
      conversationId,
      commandId,
      labelSource: "issue_context" as const,
      requiredLookup: false as const,
    };
    let queryStartedAt: number | null = null;
    let queryCount = 0;
    let resolved: WorkerSelectionPolicy;
    try {
      const token = await requiredAccessToken(prepared.accessToken);
      queryStartedAt = Date.now();
      queryCount = 1;
      const synchronized = await options.issueContext.synchronizeIssueForStart({
        accessToken: token,
        issueId: event.agentSession.issueId,
        teamId: event.agentSession.issueTeamId,
        appUserId: event.appUserId,
        agentSessionId,
      });
      // Labels and source scope go to the Node as facts. The organization is the tenant and the Team the
      // scope, the same identity the Router resolved this Node from.
      resolved = workerSelectionFromLabels(
        { status: "available", labels: synchronized.issue.labels },
        { system: "linear", tenantId: event.organizationId, scopeId: event.agentSession.issueTeamId },
      );
    } catch (error) {
      const failure = safeError(error);
      options.workerRouting.audit.record(Object.freeze({
        ...audit,
        queryCount,
        lookupLatencyMs: queryStartedAt === null ? 0 : Math.max(0, Date.now() - queryStartedAt),
        outcome: "unavailable" as const,
        safeCode: failure.code,
      }));
      if (!failure.retryable) this.#reserve(prepared, undefined);
      throw error;
    }
    options.workerRouting.audit.record(Object.freeze({
      ...audit,
      queryCount: 1,
      lookupLatencyMs: queryStartedAt === null ? 0 : Math.max(0, Date.now() - queryStartedAt),
      outcome: "routing_context" as const,
      executorLabels: resolved.executorLabels,
    }));
    return resolved;
  }

  /**
   * A stop cuts its session's queue by visibly superseding every instruction queued ahead of it. Runs only
   * after the stop's own instruction is verified, because `preempts` comes from the unauthorized payload
   * signal: safe for ordering, not for destroying other instructions. Runs before `reserve` and regardless
   * of its outcome, so `stopNotApplied` can truthfully say the queue was cut.
   */
  #cutQueueForStop(agentSessionId: string): void {
    const options = this.#options;
    const queuedAhead = options.inbox.queuedAheadForSession(agentSessionId, this.#deliveryId);
    for (const supersededId of queuedAhead) {
      this.#enqueue(agentSessionId, `axis.session.superseded:${supersededId}`, { type: "error", body: LINEAR_CHANNEL_COPY.supersededByStop });
    }
    if (queuedAhead.length > 0) options.inbox.supersedeQueued(queuedAhead, options.owner, "axis.channel.superseded_by_stop");
  }

  /**
   * Takes the conversation's command slot. Both run-chain error classes are rethrown as
   * `LinearReservationRefusal`, so a corrupt-database error does not escape with the lease held and the
   * failure path sees the run chain's own code.
   */
  #reserve(prepared: PreparedCommand, workerSelection: WorkerSelection | undefined): ChannelRunChainReservation {
    const { scope, conversationId, event, commandId, kind, instructionDigest } = prepared;
    try {
      const { reservation } = this.#options.runChain.reserve({
        scope,
        conversationId,
        tenant: linearTenantRef(event.organizationId),
        commandId,
        kind,
        // A cancel carries no instruction, and `reserve` refuses one.
        ...(kind === "cancel" ? {} : { instructionDigest }),
        ...(workerSelection === undefined ? {} : { workerSelection }),
      });
      this.#reservedCommandId = commandId;
      return reservation;
    } catch (error) {
      if (error instanceof ChannelSessionRefusalError || error instanceof ChannelRunChainStoreError) {
        throw new LinearReservationRefusal(error.code);
      }
      throw error;
    }
  }

  /**
   * Acknowledges a reserved command: a comment invocation is recorded and acknowledged on its comment; a
   * direct start is acknowledged on its `commandId`, which is stable across a semantic replay and distinct
   * from the delivery id the dispatch Activity uses.
   */
  #acknowledge(prepared: PreparedCommand): void {
    const { agentSessionId, provenance, logicalEventKey, commandId, instructionDigest, kind } = prepared;
    if (provenance !== null) {
      const responseSourceEventId = `axis.comment.ack:${provenance.sourceCommentId}`;
      this.#options.ledgers.recordCommentInvocation({
        sourceCommentId: provenance.sourceCommentId,
        rootCommentId: provenance.rootCommentId,
        agentSessionId,
        activityId: provenance.activityId,
        deliveryId: this.#deliveryId,
        logicalEventKey,
        commandId,
        instructionDigest,
        responseSourceEventId,
      });
      this.#enqueue(agentSessionId, responseSourceEventId, { type: "thought", body: LINEAR_ACTIVITY_COPY.commentAcknowledgement });
    } else if (kind === "start") {
      this.#enqueue(agentSessionId, `axis.session.ack:${commandId}`, { type: "thought", body: LINEAR_ACTIVITY_COPY.commentAcknowledgement });
    }
  }

  /**
   * Checks the decision claim before anything is dispatched: a decision can be cancelled or superseded
   * between the resolver's read and the reservation, and a check after `settle` would disown work already
   * running.
   *
   * The run chain claims the open decision for any continue off a `waiting_for_input` Run. A claim with no
   * selected option is admissible only when `./instruction.ts` bound this text to this decision
   * (`expectedDecisionId === raised.decisionId`) after checking the sender against its owner.
   */
  #claimDecision(prepared: PreparedCommand, reservation: ChannelRunChainReservation): PendingDecisionOutcome | null {
    if (reservation.decisionId === undefined || reservation.priorRunId === undefined) return null;
    const { verified } = prepared;
    const raised = linearRaisedDecisionForRun(this.#options.outbox, prepared.agentSessionId, reservation.priorRunId);
    const selected = raised === null ? null : selectDecisionOption(raised.elicitation, verified.decisionReply);
    const interpreted = raised !== null && selected === null && verified.expectedDecisionId === raised.decisionId;
    if (raised === null || (selected === null && !interpreted) || (verified.expectedDecisionId !== null && verified.expectedDecisionId !== raised.decisionId)) {
      throw new RelayError("axis.relay.invalid_transition", "The pending decision claim is stale or mismatched.", 409);
    }
    return selected === null
      ? { decisionId: raised.decisionId, outcome: "superseded", selected: null }
      : { decisionId: raised.decisionId, outcome: "resolved", selected };
  }

  /**
   * A stop on a waiting conversation is honoured locally: nothing is executing. The reservation is still
   * taken, as the mutual exclusion against a concurrent continue. The open decision is read before
   * `resolveLocally` cancels it, and through the Run, because the run chain's decision id and the
   * elicitation's never meet.
   */
  #closeWaiting(prepared: PreparedCommand): LinearIngressDisposition {
    const options = this.#options;
    const { agentSessionId, conversationId, commandId, activityId, verified } = prepared;
    const openRunId = options.runChain.openDecision({ conversationId })?.runId ?? null;
    const closing = openRunId === null ? null : linearRaisedDecisionForRun(options.outbox, agentSessionId, openRunId);
    const settled = options.runChain.resolveLocally({ commandId });
    if (closing !== null && activityId !== null) {
      options.ledgers.recordDecisionOutcome({
        decisionId: closing.decisionId,
        agentSessionId,
        outcome: "closed",
        sourceEventId: this.#deliveryId,
        activityId,
        ownerUserId: verified.owner.userId,
        selected: null,
      });
    }
    // Keyed on the delivery, so an exact webhook replay of the same stop lands on the same outbox row
    // instead of answering twice.
    this.#enqueue(agentSessionId, this.#deliveryId, { type: "response", body: LINEAR_ACTIVITY_COPY.waitingStopResponse });
    return this.#complete({ disposition: "closed_waiting", commandId: settled.commandId });
  }

  /** Submits the command to Router, settles the reservation from its answer, and projects the outcome. */
  async #submit(prepared: PreparedCommand, reservation: ChannelRunChainReservation, decisionOutcome: PendingDecisionOutcome | null): Promise<LinearIngressDisposition> {
    const options = this.#options;
    const { event, agentSessionId, commandId, kind, activityId, verified, dispatchedInstruction } = prepared;
    const identity = linearChannelIdentity({
      organizationId: event.organizationId,
      issueId: event.agentSession.issueId,
      teamId: event.agentSession.issueTeamId,
      agentSessionId,
    });
    // The reservation's retained selection is passed through whole, so a replayed start reuses the facts
    // its first attempt was admitted with.
    const reservedSelection = reservation.workerSelection;
    const command: ChannelCommand = kind === "start"
      ? channelStartCommand({ commandId, identity, instruction: dispatchedInstruction, ...(reservedSelection === undefined ? {} : { workerSelection: reservedSelection }) })
      : kind === "cancel"
        ? channelCancelCommand({ commandId, identity })
        : channelContinueCommand({ commandId, identity, instruction: dispatchedInstruction });

    this.#submitAttempted = true;
    const outcome: ChannelCommandOutcome = await options.dispatch.submit(command);
    const settled = options.runChain.settle({ commandId, outcome });

    // The decision ending is recorded only once Router accepted the command; a rejected reservation returns
    // the claim. `interpretedDecisionByActivity` reads this row to rebuild the envelope for an exact webhook
    // replay, so skipping it would turn a redelivery into `command_rebound`.
    if (decisionOutcome !== null && settled.status === "accepted" && activityId !== null) {
      options.ledgers.recordDecisionOutcome({
        decisionId: decisionOutcome.decisionId,
        agentSessionId,
        outcome: decisionOutcome.outcome,
        sourceEventId: this.#deliveryId,
        activityId,
        ownerUserId: verified.owner.userId,
        selected: decisionOutcome.selected,
      });
    }

    const projected = dispositionFromReservation(settled);
    projectDispatchOutcome(settled, kind, agentSessionId, this.#deliveryId, this.#enqueue);
    return this.#complete(projected);
  }

  // -------------------------------------------------------------------------
  // Refusals
  // -------------------------------------------------------------------------

  /**
   * The resolver's refusal. A comment refusal without a body (non-mention, Axis-authored comment,
   * unavailable authority, unprovable evidence) is quarantined silently, so the App's presence is not
   * confirmed to an unauthorized party and no bot reply loop starts; one with a body is answered once and
   * the delivery completes. A prompt refusal and an unresolvable event throw to the failure path.
   */
  #refuse(event: AgentSessionEventPayload, refusal: LinearInstructionRefusal): LinearIngressDisposition {
    const options = this.#options;
    if (refusal.kind === "comment") {
      if (refusal.responseBody === null || refusal.provenance === null) {
        options.inbox.quarantine(this.#deliveryId, options.owner, refusal.safeCode);
        return { disposition: "comment_refused", safeCode: refusal.safeCode, responded: false };
      }
      const responseSourceEventId = `axis.comment.rejection:${refusal.provenance.sourceCommentId}`;
      const agentSessionId = event.agentSession.id;
      // A mention delivers both `created` and `prompted`, each with its own logical event key and
      // `activityId`, so the second disagrees with the first's retained row. The same refusal already
      // answered for this Comment is that sibling: complete without answering again. A different refusal,
      // or a refusal after an acceptance, still conflicts.
      const retainedComment = options.ledgers.commentInvocation(refusal.provenance.sourceCommentId);
      if (retainedComment !== null && retainedComment.disposition === "rejected" && retainedComment.safeCode === refusal.safeCode) {
        return this.#complete({ disposition: "comment_refused", safeCode: refusal.safeCode, responded: false });
      }
      options.ledgers.bindSession({
        agentSessionId,
        organizationId: event.organizationId,
        appUserId: event.appUserId,
        issueId: event.agentSession.issueId,
        teamId: event.agentSession.issueTeamId,
        creatorId: event.agentSession.creatorId,
        rootCommentId: refusal.provenance.rootCommentId,
      });
      options.ledgers.recordCommentRejection({
        sourceCommentId: refusal.provenance.sourceCommentId,
        rootCommentId: refusal.provenance.rootCommentId,
        agentSessionId,
        activityId: refusal.provenance.activityId,
        deliveryId: this.#deliveryId,
        logicalEventKey: linearLogicalEventKey(event.action === "created"
          ? { kind: "comment", action: "created", sourceCommentId: refusal.provenance.sourceCommentId }
          : { kind: "comment", action: "prompted", sourceCommentId: refusal.provenance.sourceCommentId, activityId: event.agentActivity?.id ?? refusal.provenance.sourceCommentId }),
        // Digested over the refusal rather than over an instruction: there is no verified instruction
        // here, and reusing the unverified body would put attacker-chosen bytes into an idempotency key.
        instructionDigest: sha256(JSON.stringify(["comment-rejection", refusal.safeCode])),
        safeCode: refusal.safeCode,
        responseSourceEventId,
      });
      this.#enqueue(agentSessionId, responseSourceEventId, { type: "error", body: refusal.responseBody });
      return this.#complete({ disposition: "comment_refused", safeCode: refusal.safeCode, responded: true });
    }
    if (refusal.kind === "prompt") {
      // A prompt refusal on a `created` event has no channel of its own, so it is retained as a rejected
      // startup and answered even when the payload carries a `commentId` (Linear sets one on a plain
      // delegation's artificial session root). Not when the session is already bound: the prompt-first
      // sibling started it.
      if (this.#startup !== null && options.ledgers.session(event.agentSession.id) === null) this.#startup.rejectable = true;
      throw new LinearPromptRejection(refusal.reason);
    }
    // `unresolvable`.
    throw new RelayError("axis.relay.missing_instruction", "The authenticated Linear event did not contain an instruction that Axis can dispatch.", 422);
  }

  // -------------------------------------------------------------------------
  // Failure path
  // -------------------------------------------------------------------------

  /**
   * Turns a thrown failure into exactly one outcome, in this order: a retryable no-call; a post-submit
   * failure that convergence shows was dispatched after all; a retained startup failure; a reservation
   * refusal; a prompt rejection; and otherwise a terminal `fail`, with a notice for a continue or stop
   * whose outcome Router may still owe.
   */
  async #handleFailure(error: unknown): Promise<LinearIngressDisposition> {
    const failure = safeError(error);
    const event = this.#event;

    if (failure.retryable) return this.#retryable(failure.code);

    const converged = await this.#settleAfterFailure(failure.code);

    // A post-submit failure whose convergence settled the reservation as a success is a dispatched
    // command, with the same outcome and Activity as the normal exit.
    if (converged === "accepted" || converged === "interrupt_requested" || converged === "steered") {
      const settled = this.#options.runChain.reservation({ commandId: this.#reservedCommandId! });
      if (settled !== null) {
        const projected = dispositionFromReservation(settled);
        // For Linear the conversation id is the agentSessionId.
        projectDispatchOutcome(settled, settled.kind, settled.conversationId, this.#deliveryId, this.#enqueue);
        return this.#complete(projected);
      }
    }

    // A refused created start is classified by its reason; the rejection's own code is shared by all.
    const startupCode = error instanceof LinearPromptRejection
      ? createdStartupFailureCodeByReason[error.reason] ?? "axis.relay.internal"
      : failure.code;
    const startup = this.#retainStartupFailure(startupCode, converged);
    if (startup !== null) return startup;

    // The run-chain arm precedes the code arm: `LinearReservationRefusal` carries
    // `linear.ingress.logical_event_conflict`, which the code arm would map to the generic conflict copy.
    const promptReason: LinearPromptRejectionReason | null = error instanceof LinearPromptRejection
      ? error.reason
      : error instanceof LinearReservationRefusal
        ? reservationPromptReason(error.refusalCode)
        : failure.code === "linear.ingress.logical_event_conflict" || failure.code === "axis.relay.invalid_transition" ? "logical_conflict" : null;
    const stop = event !== null && isStopSignal(event);
    // A stop's own reservation refusal gets `stopNotApplied` or `stopSessionUnconfirmed` below instead of a
    // per-reason prompt rejection.
    const rejectionReason = event !== null && event.action === "prompted" && promptReason !== null
      && !(stop && error instanceof LinearReservationRefusal)
      ? promptReason
      : null;
    // Projected before the terminal call and regardless of the reported disposition, so the operator's
    // refusal code never costs the user their explanation.
    const responded = rejectionReason === null ? false : this.#projectPromptRejection(event!, rejectionReason);

    if (error instanceof LinearReservationRefusal) return this.#reservationRefused(error, stop);
    if (rejectionReason !== null) {
      this.#options.inbox.fail(this.#deliveryId, this.#options.owner, failure.code);
      return { disposition: "prompt_rejected", reason: rejectionReason, responded };
    }

    this.#announceOwedOutcome(converged);
    // A created start's prompt refusal that still lands here (its retention failed, or the session was
    // already bound) is recorded under its reason's code.
    const terminalCode = error instanceof LinearPromptRejection && this.#startup !== null ? startupCode : failure.code;
    this.#options.inbox.fail(this.#deliveryId, this.#options.owner, terminalCode);
    return { disposition: "failed", safeCode: terminalCode, ...(converged === null ? {} : { converged }) };
  }

  /**
   * Axis chose to let this lease expire: no inbox call, plus an audit row saying the no-call was
   * deliberate. `claim`'s per-session FIFO keeps the session's later deliveries from overtaking it.
   */
  #retryable(safeCode: string): LinearIngressDisposition {
    this.#options.ledgers.recordChannelEvent(this.#deliveryId, LINEAR_LEDGER_EVENT_TYPES.retryable, { code: safeCode });
    return { disposition: "retryable", safeCode };
  }

  /**
   * Releases or converges this tick's own reservation after a definite failure, returning the convergence
   * answer (null when nothing was submitted or no answer was obtained).
   *
   * A reservation never submitted is settled `rejected` (what `reconcileStale` would write for an `absent`
   * command), so the conversation's slot is not wedged. Once `submit` was attempted Router may hold the
   * command, so it is not settled on a guess: Router is asked now, through the same `reconcileNow` mapping
   * `reconcileStale` uses, because a held slot would otherwise refuse the user's next follow-up even though
   * the Worker accepted and ran.
   */
  async #settleAfterFailure(safeCode: string): Promise<ChannelRunChainSweepDisposition | null> {
    const commandId = this.#reservedCommandId;
    if (commandId === null) return null;
    if (!this.#submitAttempted) {
      try {
        this.#options.runChain.settle({ commandId, outcome: { commandId, status: "rejected", safeCode } });
      } catch { /* leave it for the sweeper rather than masking the original failure */ }
      return null;
    }
    return this.#convergeReservation(commandId);
  }

  /**
   * Asks Router about a reservation this tick submitted and could not settle, and settles it from Router's
   * answer. Never throws; returns null when no answer was obtained, leaving the reservation to
   * `reconcileStale`.
   */
  async #convergeReservation(commandId: string): Promise<ChannelRunChainSweepDisposition | null> {
    const options = this.#options;
    try {
      for (let attempt = 1; ; attempt += 1) {
        const entry = await options.runChain.reconcileNow({ commandId, dispatch: options.dispatch });
        if (entry === null) return null;
        if (entry.disposition !== "unknown" || attempt >= CONVERGENCE_ATTEMPTS) return entry.disposition;
        await this.#sleep(CONVERGENCE_BACKOFF_BASE_MS * 2 ** (attempt - 1));
      }
    } catch {
      // Swallowed: the caller is already reporting the original failure, and `reconcileStale` remains.
      return null;
    }
  }

  /**
   * Turns a `created` failure into a durable outcome, or returns null for any other failure.
   *
   * Before admission a definite failure is retained as a rejected startup, and a minimal AgentSession is
   * bound so a later prompt on it is a retry rather than an orphan. After admission the startup stays
   * accepted and the user gets the startup-failure copy. Both complete the delivery.
   */
  #retainStartupFailure(safeCode: string, converged: ChannelRunChainSweepDisposition | null): LinearIngressDisposition | null {
    const options = this.#options;
    const event = this.#event;
    const startup = this.#startup;
    if (event === null || startup === null || !startup.rejectable) return null;
    // `Router` only once a dispatch was attempted; before that every failure is Axis's own validation.
    const startupFailure = classifyStartupFailure(safeCode, this.#submitAttempted ? "Router" : "Validation");
    if (startup.admittedCommandId === null) {
      if (startupFailure.certainty !== "definite") return null;
      try {
        options.ledgers.bindSession({
          agentSessionId: event.agentSession.id,
          organizationId: event.organizationId,
          appUserId: event.appUserId,
          issueId: event.agentSession.issueId,
          teamId: event.agentSession.issueTeamId,
          creatorId: event.agentSession.creatorId,
          rootCommentId: null,
        });
        const retained = options.ledgers.recordStartupRejection({
          agentSessionId: event.agentSession.id,
          logicalEventKey: startup.logicalEventKey,
          startupIntentDigest: startup.intentDigest,
          failureStage: startupFailure.stage,
          failureReason: startupFailure.reason,
        });
        // Only the delivery that retained the rejection answers, so a semantic replay under a fresh
        // delivery id does not queue a second identical error.
        if (retained.created) {
          this.#enqueue(event.agentSession.id, this.#deliveryId, { type: "error", body: renderStartupFailure(startupFailure) });
        }
        return this.#complete({ disposition: "startup_rejected", stage: startupFailure.stage, reason: startupFailure.reason });
      } catch {
        // Not durably retained: fall through to a terminal failure.
        return null;
      }
    }
    try {
      // Which ambiguous sentence is sent depends on whether the outcome is still owed, not on the safe code,
      // which arrives both with the reservation settled `rejected` and with it held. A held reservation is
      // later answered by its Run's own events, or terminally by `answerAbsentLinearStarts` (`./observe.ts`)
      // once settled `absent` or unconfirmed. A replay of this stage never reaches here: the reservation is
      // settled, so the replay returns `already_started` first.
      const confirmationPending = startupFailure.certainty === "ambiguous" && outcomeStillOwed(converged);
      const reported: StartupFailure = confirmationPending ? { ...startupFailure, reason: "outcome_pending" } : startupFailure;
      this.#enqueue(event.agentSession.id, this.#deliveryId, reported.certainty === "ambiguous"
        ? { type: "thought", body: renderStartupFailure(reported) }
        : { type: "error", body: renderStartupFailure(reported) });
      return this.#complete({ disposition: "startup_failed", stage: reported.stage, reason: reported.reason, ...(converged === null ? {} : { converged }) });
    } catch { return null; }
  }

  /**
   * A reservation refusal is terminal and asks Router nothing about the slot holder: this tick is not its
   * dispatcher. Converging it here could settle a start whose own delivery is about to reclaim and
   * dispatch it, and leaving this delivery `retryable` would let later follow-ups overtake it. The holder
   * is released by its dispatcher's convergence, the observe tick's `reconcileSettledEarly` and
   * `reconcileStale`, or its own reclaim. A stop is answered even here, because the queue cut already
   * ran; `session_unconfirmed` gets its own copy because the earlier command will never settle on its own.
   */
  #reservationRefused(error: LinearReservationRefusal, stop: boolean): LinearIngressDisposition {
    if (stop) {
      this.#enqueue(this.#event!.agentSession.id, this.#deliveryId, {
        type: "error",
        body: error.refusalCode === "session_unconfirmed" ? LINEAR_CHANNEL_COPY.stopSessionUnconfirmed : LINEAR_CHANNEL_COPY.stopNotApplied,
      });
    }
    this.#options.inbox.fail(this.#deliveryId, this.#options.owner, `axis.channel.${error.refusalCode}`);
    return { disposition: "reservation_refused", code: error.refusalCode };
  }

  /**
   * A post-submit failure on a continue or stop is answered: an intermediate thought while the outcome is
   * still owed, or an error when Router never received the command.
   */
  #announceOwedOutcome(converged: ChannelRunChainSweepDisposition | null): void {
    if (this.#reservedCommandId === null || !this.#submitAttempted) return;
    const held = this.#options.runChain.reservation({ commandId: this.#reservedCommandId });
    if (held === null || (held.kind !== "continue" && held.kind !== "cancel")) return;
    const isStop = held.kind === "cancel";
    this.#enqueue(held.conversationId, this.#deliveryId, outcomeStillOwed(converged)
      ? { type: "thought", body: isStop ? LINEAR_CHANNEL_COPY.stopOutcomePending : LINEAR_CHANNEL_COPY.continueOutcomePending }
      : { type: "error", body: isStop ? LINEAR_ACTIVITY_COPY.stopNeverReachedRouter : LINEAR_ACTIVITY_COPY.continueNeverReachedRouter });
  }

  /**
   * Queues the prompt-rejection body for this reason and reports whether it was queued. Requires an
   * Activity to reply to and a locally known AgentSession, so no outbox state is minted for an unadmitted
   * session. Rendering is inside the `try`, so an unmapped reason fails closed (no Activity).
   *
   * The outbox key `axis.prompt.rejection:<session>:<activity>` makes redelivery idempotent: one refusal
   * per logical Activity, and the first explanation wins.
   */
  #projectPromptRejection(event: AgentSessionEventPayload, reason: LinearPromptRejectionReason): boolean {
    const activityId = event.agentActivity?.id ?? null;
    if (activityId === null || this.#options.ledgers.session(event.agentSession.id) === null) return false;
    try {
      this.#enqueue(event.agentSession.id, `axis.prompt.rejection:${event.agentSession.id}:${activityId}`, {
        type: "error", body: renderLinearPromptRejection(reason),
      });
      return true;
    } catch {
      // The refusal stays fail-closed when its safe projection cannot be persisted; the delivery still fails.
      return false;
    }
  }
}

function isStopSignal(event: AgentSessionEventPayload): boolean {
  return event.agentActivity?.signal === "stop";
}

/** Whether a convergence answer (null: none obtained) leaves the dispatch's outcome still owed. */
function outcomeStillOwed(converged: ChannelRunChainSweepDisposition | null): boolean {
  return converged === null || STARTUP_OUTCOME_STILL_OWED[converged];
}

/**
 * Queues the one Activity a dispatched command earns. A dispatched cancel gets a thought, not a response:
 * `interrupt_requested` means Router accepted the request, and the confirmation arrives later as Router's
 * cancellation terminal (`LINEAR_ACTIVITY_COPY.cancellationResponse`).
 */
function projectDispatchOutcome(
  settled: ChannelRunChainReservation,
  kind: ChannelCommandKind,
  agentSessionId: string,
  sourceEventId: string,
  queue: ActivityQueue,
): void {
  const content = dispatchOutcomeActivity(settled, kind);
  if (content !== null) queue(agentSessionId, sourceEventId, content);
}

function dispatchOutcomeActivity(settled: ChannelRunChainReservation, kind: ChannelCommandKind): LinearActivityContent | null {
  if (settled.status === "rejected") {
    const safeCode = settled.safeCode ?? "axis.downstream.unknown";
    if (kind === "start") {
      const failure = classifyStartupFailure(safeCode, "Router");
      return failure.certainty === "ambiguous"
        ? { type: "thought", body: renderStartupFailure(failure) }
        : { type: "error", body: renderStartupFailure(failure) };
    }
    // The Worker refused the continue because its configuration no longer matches its Node inventory
    // entry. A stop never gets this code: a cancel carries no execution target.
    if (safeCode === "axis.router.worker_identity_changed") return { type: "error", body: LINEAR_CHANNEL_COPY.continueWorkerIdentityChanged };
    // The Node refused the follow-up before contacting any Worker; the session is intact.
    if (safeCode === "axis.router.node_worker_unavailable") {
      return { type: "error", body: kind === "cancel" ? LINEAR_CHANNEL_COPY.stopWorkerUnavailable : LINEAR_CHANNEL_COPY.continueWorkerUnavailable };
    }
    // The Node is draining. It still accepts a cancel, so a cancel with this code takes the generic text.
    if (safeCode === "axis.router.node_draining" && kind !== "cancel") return { type: "error", body: LINEAR_CHANNEL_COPY.continueNodeDraining };
    // The Worker could not resume the conversation's native session. A cancel carries no continuation.
    if (safeCode === "axis.router.continuation_unresumable") return { type: "error", body: LINEAR_CHANNEL_COPY.continueSessionUnresumable };
    return { type: "error", body: LINEAR_CHANNEL_COPY.dispatchFailed };
  }
  // A steer only answers a continue; the Run's own terminal still answers the session.
  if (settled.status === "steered") return { type: "thought", body: LINEAR_CHANNEL_COPY.steered };
  if (settled.status === "interrupt_requested") {
    return { type: "thought", body: kind === "cancel" ? LINEAR_CHANNEL_COPY.stopRequested : LINEAR_CHANNEL_COPY.interrupting };
  }
  if (settled.status === "accepted") {
    return kind === "cancel"
      ? { type: "thought", body: LINEAR_CHANNEL_COPY.stopRequested }
      : { type: "action", action: kind === "start" ? LINEAR_CHANNEL_COPY.startedAction : LINEAR_CHANNEL_COPY.continuedAction, parameter: LINEAR_CHANNEL_COPY.dispatchedParameter };
  }
  return null;
}

/** Reads a settled reservation as this module's own outcome. */
function dispositionFromReservation(reservation: ChannelRunChainReservation): LinearIngressDisposition {
  if (reservation.status === "resolved_locally") return Object.freeze({ disposition: "closed_waiting" as const, commandId: reservation.commandId });
  if (reservation.status === "rejected") {
    if (reservation.safeCode === undefined) throw new ChannelRunChainStoreError("persisted_state_conflict");
    return Object.freeze({ disposition: "dispatch_rejected" as const, commandId: reservation.commandId, safeCode: reservation.safeCode });
  }
  if (reservation.status === "accepted" || reservation.status === "interrupt_requested" || reservation.status === "steered") {
    return Object.freeze({ disposition: "dispatched" as const, commandId: reservation.commandId, kind: reservation.kind, status: reservation.status });
  }
  // `reserved` cannot reach here: every caller checks `status !== "reserved"`.
  throw new ChannelRunChainStoreError("persisted_state_conflict");
}

/** The option a decision reply selected, or null. Reply-token rules live in `../../shared/elicitation.ts`. */
function selectDecisionOption(elicitation: StructuredElicitation, reply: string): { readonly optionId: string; readonly label: string } | null {
  const selected = selectElicitationReplyImport(elicitation, reply);
  return selected === null ? null : Object.freeze({ optionId: selected.optionId, label: selected.label });
}
