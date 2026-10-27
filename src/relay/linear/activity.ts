import { renderElicitationMarkdown, validateCanonicalLinearUserUrl } from "../../shared/elicitation.js";
import { ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY, type RouterRelayNeutralEvent, type RouterRelayNeutralEventEnvelope } from "../../shared/router-relay-event.js";
import type { ChannelEventRenderer, ChannelRenderedMessage, ChannelWaitingProjection } from "../../router/channel-conversation-types.js";

import { RelayError } from "../errors.js";
import type { VerifiedLinearOwner } from "./authorization.js";
import type { LinearActivityContent } from "../types.js";

/**
 * Every fixed, user-visible string this channel posts, including inbound-side
 * copy (the comment acknowledgement, the waiting-stop response), so tests pin
 * the literals against one symbol. `successFallback` aliases
 * `ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY`: Router mints that string for a
 * success with no agent response, and this is the fallback when that summary
 * arrives blank.
 */
export const LINEAR_ACTIVITY_COPY = Object.freeze({
  /** `action` content for an accepted turn. */
  acceptedAction: "Execution accepted",
  acceptedParameter: "The Worker accepted this turn.",
  /** Fallbacks for a blank or absent provider-supplied summary. */
  progressFallback: "Axis is processing this turn.",
  /** Says the execution ended, never that the request was carried out. */
  successFallback: ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY,
  failureFallback: "Axis could not complete this execution safely.",
  /** The cancellation `response`, byte-identical to the one Router itself emits. */
  cancellationResponse: "Axis stopped the active execution as requested.",
  /** Inbound-side copy, defined here so this channel has exactly one copy table. */
  commentAcknowledgement: "Axis received this request and is preparing the captured execution route.",
  waitingStopResponse: "Axis stopped this waiting request without starting continuation work.",
  /**
   * Says Axis gave up, not that the request failed: every attempt was
   * classified retryable, and the delivery is quarantined, not discarded
   * (`../../shared/retry-budget.ts`). `{attempts}` is substituted by
   * `renderAttemptsExhausted` in `./channel.ts`.
   */
  attemptsExhausted: "Axis tried to process this request {attempts} times and has stopped retrying it. This is not a rejection of the request: each attempt failed in a way that looked temporary, and Axis will not keep retrying unattended. The request is retained rather than discarded, so an operator can look at it -- please contact the Axis operators if this was unexpected.",
  /**
   * The give-up for a queued instruction that never got to run, so it does not
   * claim Axis "tried to process" it. Same policy and sentence order as
   * `attemptsExhausted`. `{attempts}` is substituted by
   * `renderRedispatchAttemptsExhausted` in `./redispatch.ts`.
   */
  redispatchAttemptsExhausted: "Axis tried {attempts} times to run the instruction it queued behind an earlier one, and has stopped retrying it. This is not a rejection of the instruction: each attempt failed in a way that looked temporary, and Axis will not keep retrying unattended. The queued instruction is retained rather than discarded, so an operator can look at it -- send it again if you want Axis to try now, and contact the Axis operators if this keeps happening.",
  /**
   * The terminal answer for a start the stranded-reservation sweep settled
   * `absent`, sent by `answerAbsentLinearStarts` in `./observe.ts`. Router
   * writes its command row before any dispatch, so no row means the session
   * definitely did not start, and the copy says so. Nothing retries it, so it
   * points at a new Agent session; it must not reuse `renderStartupFailure`'s
   * ambiguous wording.
   */
  startNeverReachedRouter: "Axis has confirmed that this session did not start.\nReason: The startup request never reached Axis's Router, so nothing was run.\nAxis will not retry it automatically. Create a new Agent session on this issue to run it.",
  /**
   * The continue's `startNeverReachedRouter`: a prompt that never reached
   * Router never ran, so resending it cannot duplicate a turn.
   */
  continueNeverReachedRouter: "Axis has confirmed that this prompt was not run.\nReason: The request never reached Axis's Router, so nothing was run.\nAxis will not retry it automatically. Send the prompt again to run it.",
  /** The stop's `startNeverReachedRouter`. */
  stopNeverReachedRouter: "Axis has confirmed that this stop was not delivered.\nReason: The stop request never reached Axis's Router, so nothing was stopped and the execution is still running.\nAxis will not retry it automatically. Send the stop again.",
  /**
   * The unconfirmed sweep's terminal answer for a `start` still `unknown` past
   * the unconfirmed window. Not a decided negative: it says only that the
   * outcome is unknown, that it may have run, and that Axis will not resend it.
   * It points at a new session because `reserve`'s `session_unconfirmed` guard
   * refuses further instruction here. Output "may still appear" because egress
   * keeps projecting Router's Task events after the reservation settles.
   */
  startOutcomeUnconfirmed: "Axis could not confirm whether this session started, and has stopped waiting for an answer.\nReason: The startup request was handed to its execution backend, but its outcome was never reported back. It may have run.\nAxis will not send it again. If it did run, its output may still appear here later. Create a new Agent session on this issue to keep working; check this issue and its repository for the earlier request's effects before asking for the same work again.",
  /** The `continue` counterpart to `startOutcomeUnconfirmed`, with the same hedges for the same reasons. */
  continueOutcomeUnconfirmed: "Axis could not confirm whether this prompt ran, and has stopped waiting for an answer.\nReason: The prompt was handed to its execution backend, but its outcome was never reported back. It may have run.\nAxis will not send it again. If it did run, its output may still appear here later. This session cannot take new prompts; create a new Agent session on this issue to keep working, and check this issue and its repository for this prompt's effects before asking for the same work again.",
  /**
   * The `cancel` counterpart. It does not point at a new session, because
   * `reserve` still allows a stop of a live Run on a guarded conversation.
   */
  stopOutcomeUnconfirmed: "Axis could not confirm whether this stop reached the active execution, and has stopped waiting for an answer.\nReason: The stop was handed to its execution backend, but its outcome was never reported back. The execution may still be running.\nAxis will not send the stop again. While the execution is still running, this session still accepts a stop.",
} as const);

/** Bound applied to every provider-supplied body. */
const BODY_LIMIT = 512;

/** Bound applied to `ChannelRenderedMessage#summary`. */
const SUMMARY_LIMIT = 256;

/**
 * Resolves the frozen decision owner for one Linear AgentSession. The URL is
 * validated here (see `frozenOwner`) so an absent or non-canonical owner fails
 * with one `RelayError`, rather than `renderElicitationMarkdown`'s plain
 * `Error`, which `safeError` would record as `axis.relay.internal`. Required,
 * so the waiting branch cannot be left unwired.
 */
export interface LinearDecisionOwnerLookup {
  decisionOwner(agentSessionId: string): VerifiedLinearOwner | null;
}

/**
 * Linear's rendered message: `ChannelRenderedMessage` plus
 * `preferredActivityId`, the Worker receipt's `activityId` for the
 * `task_agent_response` payload. Whether to keep it
 * is `linearActivityId`'s decision (`./delivery.ts`, which enforces UUID v4);
 * absent means no receipt id was offered.
 *
 * The neutral outbox port cannot name this field, so Linear's composition
 * passes the outbox an id factory that narrows the rendering:
 *
 *     ({ rendered }) => linearActivityId((rendered as LinearRenderedMessage | undefined)?.preferredActivityId)
 *
 * A zero-argument id generator would silently discard the preference.
 */
export interface LinearRenderedMessage extends ChannelRenderedMessage {
  readonly content: LinearActivityContent;
  readonly preferredActivityId?: string;
}

function bounded(value: unknown, fallback: string, limit = BODY_LIMIT): string {
  if (typeof value !== "string" || value.trim().length === 0) return fallback;
  return [...value].slice(0, limit).join("");
}

/**
 * The bounded human-readable summary. Not an idempotency key: two payloads can
 * summarise identically, so the delivery digest is taken over `content`.
 */
function summarize(content: LinearActivityContent): string {
  const detail = content.type === "action" ? `${content.action}: ${content.parameter}` : content.body;
  return `${content.type}: ${[...detail].slice(0, SUMMARY_LIMIT).join("")}`;
}

function ownerUnavailable(): never {
  throw new RelayError("axis.relay.invalid_transition", "The waiting decision owner is unavailable.", 409);
}

/**
 * The frozen owner, or the waiting-owner refusal, both for no owner and for an
 * owner whose URL is not a canonical Linear profile URL. The `null` check stays
 * explicit although the URL validator would also refuse it.
 */
function frozenOwner(owner: VerifiedLinearOwner | null): VerifiedLinearOwner {
  if (owner === null || validateCanonicalLinearUserUrl(owner.userUrl) === null) ownerUnavailable();
  return owner;
}

/** The one place a `LinearRenderedMessage` is assembled, so its optional fields have a single spread site. */
function message(
  content: LinearActivityContent,
  terminal: boolean,
  extra: { readonly preferredActivityId?: string; readonly waiting?: ChannelWaitingProjection } = {},
): LinearRenderedMessage {
  return {
    content,
    summary: summarize(content),
    terminal,
    ...(extra.waiting === undefined ? {} : { waiting: extra.waiting }),
    ...(extra.preferredActivityId === undefined ? {} : { preferredActivityId: extra.preferredActivityId }),
  };
}

type WaitingPayload = Extract<RouterRelayNeutralEvent, { eventType: "axis.router.v1.waiting_for_input" }>["payload"];

function waitingFrom(payload: WaitingPayload): { readonly waiting: ChannelWaitingProjection } {
  const { taskId, attemptId, runId, elicitation } = payload;
  return { waiting: { taskId, attemptId, runId, elicitation } };
}

export interface LinearEventRendererOptions {
  readonly owners: LinearDecisionOwnerLookup;
}

/** Linear's `ChannelEventRenderer`. */
export class LinearEventRenderer implements ChannelEventRenderer {
  readonly #owners: LinearDecisionOwnerLookup;

  constructor(options: LinearEventRendererOptions) {
    this.#owners = options.owners;
  }

  render(envelope: RouterRelayNeutralEventEnvelope): LinearRenderedMessage | null {
    if (envelope.conversation.system !== "linear") {
      throw new RelayError("axis.relay.invalid_transition", "The Router event does not belong to a Linear conversation.", 409);
    }
    const { agentSessionId } = envelope.conversation;
    const { event } = envelope;

    switch (event.eventType) {
      case "axis.router.v1.accepted":
        return message({ type: "action", action: LINEAR_ACTIVITY_COPY.acceptedAction, parameter: LINEAR_ACTIVITY_COPY.acceptedParameter }, false);

      case "axis.router.v1.progressed":
        return message({ type: "thought", body: bounded(event.payload.safeSummary, LINEAR_ACTIVITY_COPY.progressFallback) }, false);

      case "axis.router.v1.waiting_for_input": {
        const owner = frozenOwner(this.#owners.decisionOwner(agentSessionId));
        const { waiting } = waitingFrom(event.payload);
        return message(
          { type: "elicitation", body: renderElicitationMarkdown(waiting.elicitation, owner.userUrl) },
          // See the header: a parked Linear Run ends the observation this
          // projection belongs to, even though the AgentSession stays open.
          true,
          { waiting },
        );
      }

      case "axis.router.v1.succeeded": {
        if ("delivery" in event.payload) {
          // The Worker's own response body, passed through unbounded: it is
          // contract-validated, and already normalized and redacted by the
          // executor adapter (`normalizeExecutorTerminalResponse`,
          // `../../shared/worker-executor.ts`). `successFallback` is for the
          // other payload shape, which carries only a free-form summary.
          const { agentActivity } = event.payload.delivery;
          return message({ type: "response", body: agentActivity.content.body }, true, { preferredActivityId: agentActivity.activityId });
        }
        return message({ type: "response", body: bounded(event.payload.safeSummary, LINEAR_ACTIVITY_COPY.successFallback) }, true);
      }

      case "axis.router.v1.cancelled":
        return message({ type: "response", body: LINEAR_ACTIVITY_COPY.cancellationResponse }, true);

      case "axis.router.v1.failed":
        return message({ type: "error", body: bounded(event.payload.error.safeMessage, LINEAR_ACTIVITY_COPY.failureFallback) }, true);

      default: {
        // Exhaustive over the six event types. Throwing rather than returning
        // `null` refuses the batch instead of silently dropping a new event
        // type, and the `never` binding makes adding one a compile error first.
        const unprojected: never = event;
        void unprojected;
        throw new RelayError("axis.relay.invalid_transition", "The Router observation could not be projected.", 409);
      }
    }
  }
}
