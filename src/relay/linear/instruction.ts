import {
  elicitationInterpretationInstruction,
  elicitationReplyInstruction,
  normalizeElicitationDecisionReply,
  normalizeElicitationReply,
  selectElicitationReply,
  type StructuredElicitation,
} from "../../shared/elicitation.js";

import { validateCommentInstruction } from "../comment-invocation.js";
import { RelayError } from "../errors.js";
import { verifiedLinearOwner, type VerifiedLinearOwner } from "./authorization.js";
import type { ImmutableLinearComment, ImmutableLinearIssue, LinearProviderPort } from "./provider.js";
import type { AgentSessionEventPayload } from "../types.js";

/**
 * Linear instruction resolution: turns one authenticated, already-authorized
 * `AgentSessionEvent` into a closed verified execution input or a fixed safe
 * refusal (`docs/LINEAR-PROVIDER.md`, `docs/SECURITY-MODEL.md` "Public ingress").
 * Reads only; writes nothing.
 *
 * No webhook payload field ever becomes execution input. Input comes only from
 * exact provider rereads (one `agentActivity(id)`, the source Comment, the
 * issue), with no fallback to history or the webhook's copy of the body. A
 * verified input is only the user command.
 *
 * Refusal kinds: `comment` (a `linear.comment.*` code; a `null` body is
 * silent, so an unauthorized party or a bot gets no reply), `prompt` (a
 * reason `./channel.ts` renders to copy), and `unresolvable` (the closed
 * residual for an action outside `created | prompted`). An unverifiable owner
 * is a `prompt` refusal even on the comment path. Bodies are fixed literals
 * and never carry provider diagnostics, user text, or display names.
 *
 * A retryable exact-read failure on a created start, and waiting-ledger
 * corruption on the comment path, throw rather than refuse, because a refusal
 * is durable.
 */

/** Fixed copy for a proven-eligible comment whose instruction could not be verified. */
export const LINEAR_COMMENT_REJECTION_BODY = "Axis could not verify this comment instruction, so it was not dispatched.";
/** The single generic denial a proven current nonmember receives; it names no policy detail. */
export const LINEAR_COMMENT_MEMBER_REJECTION_BODY = "Axis could not dispatch this request because the commenter is not an authorized member of this Team.";

/** Bounded retry for a created start's exact reads. Beyond this the delivery is left retryable. */
const EXACT_INPUT_READ_ATTEMPTS = 3;
const EXACT_INPUT_RETRY_BASE_MS = 25;

/** Why a prompt could not be verified. Reaches a durable audit row and selects the copy `renderLinearPromptRejection` renders. */
export type LinearPromptRejectionReason =
  | "capability_unavailable"
  | "credential_unavailable"
  | "provider_unavailable"
  | "activity_mismatch"
  | "session_mismatch"
  | "user_missing"
  | "user_mismatch"
  | "axis_authored"
  | "wrong_type"
  | "logical_conflict"
  /** From `./channel.ts`: an earlier command on the conversation has not settled; send again shortly. */
  | "command_in_flight"
  /** From `./channel.ts`: an earlier dispatch was never confirmed, so the session takes no more instructions. */
  | "session_unconfirmed"
  /** From `./channel.ts`: the run-chain store could not read its own state; nothing the user did. */
  | "state_unavailable"
  /** Axis's own checks failed (credential vault, waiting ledger, or an unexpected throw), not the provider. */
  | "internal_error"
  /** The exact issue reread does not match the AgentSession's own issue/Team identity. */
  | "issue_mismatch"
  /** The exact issue read failed definitely, or the adapter cannot read issues. */
  | "issue_unavailable";

interface PromptRejectionCopy { readonly reasonCopy: string; readonly actionCopy: string; }

/**
 * The user-visible refusal copy, keyed on the reason: provider-neutral, never
 * interpolated, and total over the closed union. Each entry is what Axis
 * observed plus what the user can do. Owner-verification failures all render
 * `user_mismatch`, so the copy never reveals which property of a user failed.
 */
const promptRejectionCopyByReason: Readonly<Record<LinearPromptRejectionReason, PromptRejectionCopy>> = Object.freeze({
  capability_unavailable: { reasonCopy: "Axis cannot read Agent Activities in this workspace right now.", actionCopy: "Restore Axis's Linear access, then send this request again." },
  credential_unavailable: { reasonCopy: "Axis has no usable Linear credential for this workspace.", actionCopy: "Reconnect Axis to Linear, then send this request again." },
  provider_unavailable: { reasonCopy: "Linear did not return this request's Activity.", actionCopy: "This is often temporary; send this request again." },
  activity_mismatch: { reasonCopy: "The Activity Linear returned is not the one this request named.", actionCopy: "Send this request again as a new Agent Activity." },
  session_mismatch: { reasonCopy: "The Activity Linear returned belongs to a different Agent session.", actionCopy: "Send this request again in this Agent session." },
  user_missing: { reasonCopy: "This request carries no Linear author Axis can identify.", actionCopy: "Send this request again from a signed-in Linear account." },
  user_mismatch: { reasonCopy: "This request was not written by the person Axis verified for this Agent session.", actionCopy: "The person who started this session can send it instead." },
  axis_authored: { reasonCopy: "This request was authored by Axis itself rather than by a person.", actionCopy: "Send the request as a person in this Agent session." },
  wrong_type: { reasonCopy: "This Activity is not a prompt, so it carries no instruction to run.", actionCopy: "Send this request as a prompt in this Agent session." },
  logical_conflict: { reasonCopy: "This request conflicts with what Axis has already recorded for this conversation.", actionCopy: "Check this session's most recent Activity, then send a new request." },
  command_in_flight: { reasonCopy: "An earlier request on this conversation has not finished settling, so Axis did not start this one.", actionCopy: "Wait for the earlier request to finish, then send this one again." },
  session_unconfirmed: { reasonCopy: "An earlier request in this session was handed to its execution backend, but its outcome could never be confirmed, so this session cannot take new instructions.", actionCopy: "Create a new Agent session on this issue to keep working. Check this issue and its repository for the earlier request's effects before asking for the same work again." },
  state_unavailable: { reasonCopy: "Axis could not read the state it keeps for this conversation, so it did not start this request.", actionCopy: "Nothing was started; send this request again, and contact the Axis operators if it keeps happening." },
  internal_error: { reasonCopy: "Axis could not complete its own checks on this request.", actionCopy: "Nothing was started; send this request again, and contact the Axis operators if it keeps happening." },
  issue_mismatch: { reasonCopy: "This request's issue is not the issue this Agent session was started on.", actionCopy: "Start a new Agent session on the issue you want Axis to work in." },
  issue_unavailable: { reasonCopy: "Linear did not return the issue this request belongs to.", actionCopy: "Check that Axis can still read this issue, then send this request again." },
});

/**
 * Renders one reason into the fixed body a refused user reads. The guard is
 * reachable because `./channel.ts` maps reasons from untyped run-chain codes;
 * throwing yields silence rather than a wrong explanation.
 */
export function renderLinearPromptRejection(reason: LinearPromptRejectionReason): string {
  const copy = (promptRejectionCopyByReason as Partial<Record<string, PromptRejectionCopy>>)[reason];
  if (copy === undefined) throw new Error("The prompt rejection reason has no user-visible copy.");
  return `Axis did not run this request.\nReason: ${copy.reasonCopy}\n${copy.actionCopy}`;
}

/** Stable internal comment refusal codes. Closed so no provider string can become one. */
export type LinearCommentRefusalCode =
  | "linear.comment.root_identity_missing"
  | "linear.comment.self_authored"
  | "linear.comment.capability_unavailable"
  | "linear.comment.credential_unavailable"
  | "linear.comment.provider_unavailable"
  | "linear.comment.identity_mismatch"
  | "linear.comment.root_identity_mismatch"
  | "linear.comment.self_or_nonhuman_author"
  | "linear.comment.membership_unavailable"
  | "linear.comment.team_member_forbidden"
  | "linear.comment.activity_provenance_mismatch"
  | "linear.comment.activity_unavailable"
  | "linear.comment.activity_identity_mismatch"
  | "linear.comment.pending_decision_mismatch"
  | "linear.comment.stale_decision_reply"
  | "linear.comment.not_mentioned"
  | "linear.comment.instruction_malformed";

/** Linear provenance binding an input to its exact source Comment and thread root. */
export interface LinearCommentProvenance {
  readonly sourceCommentId: string;
  readonly rootCommentId: string;
  readonly activityId: string | null;
}

/** A closed, fully verified execution input. Nothing partial is ever returned. */
export interface VerifiedLinearInstruction {
  /** The exact bytes to dispatch; for a decision reply, the reply token or `Other: <answer>`. */
  readonly content: string;
  /** The normalized reply the store matches against the waiting ledger. */
  readonly decisionReply: string;
  /** Set only when this input claims a specific pending decision. */
  readonly expectedDecisionId: string | null;
  /** The frozen active human owner, including canonical Linear `User.url`. */
  readonly owner: VerifiedLinearOwner;
}

export type LinearInstructionRefusal =
  | {
    readonly kind: "comment";
    readonly safeCode: LinearCommentRefusalCode;
    /** `null` means answer nothing at all; see the silence rationale above. */
    readonly responseBody: string | null;
    readonly provenance: LinearCommentProvenance | null;
  }
  | {
    readonly kind: "prompt";
    readonly reason: LinearPromptRejectionReason;
    /** Resolver-side only (`./channel.ts` renders from `reason`): set for a non-stop prompted event, else `null`. */
    readonly responseBody: string | null;
  }
  | { readonly kind: "unresolvable" };

export type LinearInstructionResolution =
  | {
    readonly disposition: "verified";
    readonly instruction: VerifiedLinearInstruction;
    /** Non-null exactly when this input came through the comment channel. */
    readonly provenance: LinearCommentProvenance | null;
    /** The exact issue identity the dispatch envelope names; `null` only for an explicit stop. */
    readonly issue: LinearDispatchIssue | null;
  }
  | { readonly disposition: "refused"; readonly refusal: LinearInstructionRefusal };

/** A comment resolution additionally reports "this event is not comment-origin". */
export type LinearCommentInvocationResolution =
  | LinearInstructionResolution
  | { readonly disposition: "not_comment_origin" };

/** One claimed or pending waiting decision, as the ledger records it. */
export interface LinearDecisionClaim {
  readonly decisionId: string;
  readonly ownerUserId: string;
  readonly ownerUserUrl: string;
  readonly elicitation: StructuredElicitation;
}

/** The read-only view of the waiting-decision ledger this module consults. */
export interface LinearDecisionLedger {
  /** The decision this exact reply value claims, bound to Activity and (optionally) Comment provenance. */
  decisionClaimForReply(
    agentSessionId: string,
    activityId: string,
    sourceCommentId: string | null,
    replyValue: string,
  ): LinearDecisionClaim | null;
  /** The currently unanswered decision for the session, if any. */
  pendingDecision(agentSessionId: string): LinearDecisionClaim | null;
  /** The decision a free-text reply from this Activity is interpreted against, open or replayed. */
  interpretationClaim(agentSessionId: string, activityId: string, sourceCommentId: string | null): LinearDecisionClaim | null;
  /** Whether this normalized text equals any option label ever offered in the session. */
  matchesReservedDecisionOptionLabel(agentSessionId: string, reply: string): boolean;
}

export interface LinearInstructionContext {
  readonly provider: LinearProviderPort;
  readonly decisions: LinearDecisionLedger;
  /** Resolves the installation's current access token, or `null` when unavailable. */
  readonly accessToken: () => Promise<string | null>;
  /** Backoff between bounded exact-read attempts. Required so a test never really waits. */
  readonly sleep: (milliseconds: number) => Promise<void>;
}

function commentRefusal(
  safeCode: LinearCommentRefusalCode,
  options: { readonly responseBody?: string; readonly provenance?: LinearCommentProvenance } = {},
): LinearInstructionResolution {
  return {
    disposition: "refused",
    refusal: {
      kind: "comment",
      safeCode,
      responseBody: options.responseBody ?? null,
      provenance: options.provenance ?? null,
    },
  };
}

/** Attaches copy only for a non-stop prompted event; every refusal is still audited. */
function promptRefusal(reason: LinearPromptRejectionReason, payload: AgentSessionEventPayload): LinearInstructionResolution {
  const answerable = payload.action === "prompted" && payload.agentActivity?.signal !== "stop";
  return {
    disposition: "refused",
    refusal: { kind: "prompt", reason, responseBody: answerable ? renderLinearPromptRejection(reason) : null },
  };
}

/** A Linear-hosted comment permalink (fragment allowed; no query, userinfo or port), or `null`. */
export function canonicalLinearCommentUrl(value: string): string | null {
  if (value.length > 2_048) return null;
  try {
    const url = new URL(value);
    return url.protocol === "https:" && url.port === "" && url.username.length === 0 && url.password.length === 0 && url.search.length === 0
      && (url.hostname === "linear.app" || url.hostname.endsWith(".linear.app"))
      ? url.toString()
      : null;
  } catch {
    return null;
  }
}

/** The issue identity every Linear dispatch carries, from an exact `issue(id)` read. */
export interface LinearDispatchIssue {
  readonly id: string;
  readonly identifier: string;
  readonly title: string;
}

/** Everything `composeLinearDispatchInstruction` renders besides the user command, all authenticated state. */
export interface LinearDispatchContext {
  readonly appUserId: string;
  readonly issue: LinearDispatchIssue;
  /** The proven source Comment, or `null` for a direct (non-comment) input. */
  readonly sourceCommentId: string | null;
  /** The proven thread root; the parent Comment is derived from it. */
  readonly rootCommentId: string | null;
  /** Rules supplied by this event, in Linear's precedence order. */
  readonly guidance?: AgentSessionEventPayload["guidance"];
}

/** Only this envelope's own tag names are neutralized; any other `<` a user writes is left alone. */
const LINEAR_DISPATCH_TAG = /<(?=\s*\/?\s*(?:user-command|comment-id|parent-comment-id|issue-title|issue|linear-agent-guidance|guidance-rule)(?![A-Za-z0-9_]))/giu;
const LINEAR_USER_MENTION = /<user\b([^<>]*)>[^<]*<\/user>[ \t]*/gu;

function envelopeValue(value: string): string {
  return value.replace(LINEAR_DISPATCH_TAG, "&lt;");
}

function guidanceSection(guidance: AgentSessionEventPayload["guidance"] | undefined): string | null {
  if (guidance === undefined || guidance === null || guidance.length === 0) return null;
  const rules = guidance.map((rule) => {
    const origin = rule.origin.type === "Organization"
      ? 'origin="organization"'
      : `origin="team" team-name="${rule.origin.team.displayName.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")}"`;
    return `<guidance-rule ${origin}>\n${envelopeValue(rule.body)}\n</guidance-rule>`;
  });
  return `<linear-agent-guidance>\n${rules.join("\n")}\n</linear-agent-guidance>`;
}

/**
 * Removes the Axis App User's own `<user id="…">` mention markup from a prompt
 * body, matching on `id`. The element shape is assumed, not confirmed against
 * a real AgentActivity body; other shapes pass through unchanged.
 */
export function stripLinearAppUserMentions(text: string, appUserId: string): string {
  return text.replace(LINEAR_USER_MENTION, (mention: string, attributes: string) =>
    /(?:^|\s)id\s*=\s*"([^"]*)"/u.exec(attributes)?.[1] === appUserId ? "" : mention);
}

/**
 * The one composer for every instruction Axis dispatches on Linear: the user
 * command, comment IDs, issue identity, and a hint to read the description and
 * thread from Linear.
 *
 * Digests are computed over the bare command (`VerifiedLinearInstruction#content`),
 * never this envelope, so a queued redispatch matches its own marker. Every
 * interpolated `<` that starts one of this envelope's tag names becomes
 * `&lt;`, so user text cannot forge a field.
 */
export function composeLinearDispatchInstruction(command: string, context: LinearDispatchContext): string {
  const userCommand = stripLinearAppUserMentions(command, context.appUserId).trim();
  // Threads are one level deep, so the parent is the root unless the source is the root.
  const parentCommentId = context.sourceCommentId !== null && context.rootCommentId !== null && context.rootCommentId !== context.sourceCommentId
    ? context.rootCommentId
    : null;
  const fields: string[] = [];
  const guidance = guidanceSection(context.guidance);
  if (guidance !== null) fields.push(guidance);
  if (userCommand.length > 0) fields.push(`<user-command>\n${envelopeValue(userCommand)}\n</user-command>`);
  if (context.sourceCommentId !== null) fields.push(`<comment-id>${envelopeValue(context.sourceCommentId)}</comment-id>`);
  if (parentCommentId !== null) fields.push(`<parent-comment-id>${envelopeValue(parentCommentId)}</parent-comment-id>`);
  fields.push(`<issue-title>${envelopeValue(context.issue.title)}</issue-title>`);
  fields.push(`<issue>${envelopeValue(context.issue.identifier)} - ${envelopeValue(context.issue.id)}</issue>`);
  return `${fields.join("\n")}\n\n`
    + "This instruction does not include the Linear issue description or comment thread. "
    + `Read them from Linear when you need them (for example \`linear issue view ${envelopeValue(context.issue.identifier)}\`). `
    + "If you cannot read them, say so plainly instead of guessing what they contain.";
}

/** Retries a retryable `RelayError` with backoff, then throws a still-retryable error. */
export async function readExactLinear<T>(
  read: () => Promise<T>,
  sleep: (milliseconds: number) => Promise<void>,
): Promise<T> {
  for (let attempt = 0; attempt < EXACT_INPUT_READ_ATTEMPTS; attempt += 1) {
    try {
      return await read();
    } catch (error) {
      if (!(error instanceof RelayError && error.retryable)) throw error;
      if (attempt + 1 < EXACT_INPUT_READ_ATTEMPTS) {
        await sleep(EXACT_INPUT_RETRY_BASE_MS * 2 ** attempt);
      }
    }
  }
  throw new RelayError("linear.provider.rejected", "Linear could not provide the exact execution input.", 503, true);
}

/**
 * The exact issue read behind every dispatched instruction. A created start
 * must match the session's Issue ID and Team, a prompted input only the Issue
 * ID. A retryable failure stays retryable; a definite one is `issue_unavailable`.
 */
async function loadDispatchIssue(
  token: string,
  payload: AgentSessionEventPayload,
  context: LinearInstructionContext,
): Promise<{ readonly issue: LinearDispatchIssue } | { readonly refusal: LinearInstructionResolution }> {
  const created = payload.action === "created";
  if (context.provider.loadExactIssue === undefined) {
    return { refusal: promptRefusal(created ? "capability_unavailable" : "issue_unavailable", payload) };
  }
  let issue: ImmutableLinearIssue;
  try {
    issue = await readExactLinear(() => context.provider.loadExactIssue!(token, payload.agentSession.issueId), context.sleep);
  } catch (error) {
    if (error instanceof RelayError && error.retryable) throw error;
    return { refusal: promptRefusal("issue_unavailable", payload) };
  }
  if (issue.id !== payload.agentSession.issueId || (created && issue.teamId !== payload.agentSession.issueTeamId)) {
    return { refusal: promptRefusal("issue_mismatch", payload) };
  }
  return { issue: Object.freeze({ id: issue.id, identifier: issue.identifier, title: issue.title }) };
}

/** A prompted input that passed every check and is now owed only its issue read. */
async function verifiedDispatch(
  token: string,
  payload: AgentSessionEventPayload,
  context: LinearInstructionContext,
  instruction: VerifiedLinearInstruction,
  provenance: LinearCommentProvenance | null,
): Promise<LinearInstructionResolution> {
  const loaded = await loadDispatchIssue(token, payload, context);
  if ("refusal" in loaded) return loaded.refusal;
  return { disposition: "verified", instruction, provenance, issue: loaded.issue };
}

/**
 * Resolves the instruction for one authorized event. An event with comment
 * provenance is only ever resolved through the comment path, so a failed
 * comment gate cannot be retried as an ordinary prompt.
 */
export async function resolveLinearInstruction(
  payload: AgentSessionEventPayload,
  context: LinearInstructionContext,
): Promise<LinearInstructionResolution> {
  const comment = await resolveLinearCommentInvocation(payload, context);
  if (comment.disposition !== "not_comment_origin") return comment;
  return resolveLinearDirectInstruction(payload, context);
}

/**
 * The comment channel (`docs/LINEAR-PROVIDER.md` "Issue-comment invocation").
 *
 * There is no raw `Comment` webhook ingress: a real `AgentSessionEvent` must
 * already carry comment provenance, and everything below is a reread of exact
 * provider state rather than a reading of the payload. The gates run in this
 * order, and each can only remove eligibility:
 *
 * 1. non-null captured thread root on the session;
 * 2. an expected human author that is not the Axis App User;
 * 3. provider capability and a live credential;
 * 4. the exact `Comment(id)`, whose ID, issue, Team, and author must all match
 *    the event;
 * 5. the comment's root (`parentId ?? id`) equals the captured session root;
 * 6. the author is a real, active, non-App, non-bot human;
 * 7. that author is exactly present in a *complete* current Team membership
 *    answer -- an incomplete membership answer is silence, never an approval;
 * 8. then, per action, the exact prompt Activity (prompted) or the structured
 *    mention (created).
 *
 * Gate 7's completeness flag separates a proven nonmember (denied) from a
 * partial page (silence and a retry).
 */
export async function resolveLinearCommentInvocation(
  payload: AgentSessionEventPayload,
  context: LinearInstructionContext,
): Promise<LinearCommentInvocationResolution> {
  // Linear supplies only commentId on some created mentions and assignments.
  // The exact reread below proves that candidate; null sourceCommentId must
  // not silently turn a user instruction into an empty direct assignment.
  const sourceCommentId: string | null = (payload.action === "created"
    ? payload.agentSession.sourceCommentId ?? payload.agentSession.commentId
    : payload.agentActivity?.sourceCommentId) ?? null;
  if (sourceCommentId === null) return { disposition: "not_comment_origin" };
  const capturedRootCommentId = payload.agentSession.commentId ?? null;
  if (capturedRootCommentId === null) return commentRefusal("linear.comment.root_identity_missing");
  const expectedUserId = payload.action === "created"
    ? payload.agentSession.creatorId
    : payload.agentActivity?.userId ?? null;
  if (expectedUserId === null || expectedUserId === payload.appUserId) {
    return commentRefusal("linear.comment.self_authored");
  }
  if (context.provider.loadExactComment === undefined) {
    return commentRefusal("linear.comment.capability_unavailable");
  }
  const token = await context.accessToken();
  if (token === null) return commentRefusal("linear.comment.credential_unavailable");
  let comment: ImmutableLinearComment;
  try {
    // A created start retries and rethrows retryably; a prompted reply keeps a
    // single attempt, since a later delivery can carry it again.
    comment = payload.action === "created"
      ? await readExactLinear(() => context.provider.loadExactComment!(token, sourceCommentId, expectedUserId), context.sleep)
      : await context.provider.loadExactComment(token, sourceCommentId, expectedUserId);
  } catch (error) {
    if (payload.action === "created" && error instanceof RelayError && error.retryable) throw error;
    return commentRefusal("linear.comment.provider_unavailable");
  }
  if (
    comment.id !== sourceCommentId
    || comment.issueId !== payload.agentSession.issueId
    || comment.teamId !== payload.agentSession.issueTeamId
  ) return commentRefusal("linear.comment.identity_mismatch");
  const rootCommentId = comment.parentId ?? comment.id;
  if (capturedRootCommentId !== rootCommentId) {
    return commentRefusal("linear.comment.root_identity_mismatch");
  }
  if (
    payload.action === "created"
    && payload.agentSession.sourceCommentId === null
    && comment.isArtificialAgentSessionRoot === true
  ) {
    return { disposition: "not_comment_origin" };
  }
  if (comment.user?.id !== expectedUserId) return commentRefusal("linear.comment.identity_mismatch");
  // Provenance exists only once identity and root are proven.
  const provenance: LinearCommentProvenance = {
    sourceCommentId,
    rootCommentId,
    activityId: payload.agentActivity?.id ?? null,
  };
  if (
    comment.botAuthored
    || comment.user === null
    || comment.user.id === payload.appUserId
    || comment.user.active !== true
    || comment.user.app !== false
  ) return commentRefusal("linear.comment.self_or_nonhuman_author");
  if (!comment.membershipComplete) return commentRefusal("linear.comment.membership_unavailable");
  if (
    comment.member === null
    || comment.member.id !== expectedUserId
    || comment.member.active !== true
    || comment.member.app !== false
  ) {
    return commentRefusal("linear.comment.team_member_forbidden", { responseBody: LINEAR_COMMENT_MEMBER_REJECTION_BODY, provenance });
  }
  if (payload.action === "prompted") {
    return resolveCommentPrompt(payload, context, { token, expectedUserId, sourceCommentId, provenance });
  }
  // AgentSession creation already delegates to this App, so no @mention is required.
  const validated = validateCommentInstruction(comment.bodyData, payload.appUserId, false);
  if (validated.disposition === "ignored") return commentRefusal("linear.comment.not_mentioned");
  if (validated.disposition === "rejected") {
    return commentRefusal(`linear.comment.instruction_${validated.reason}`, { responseBody: LINEAR_COMMENT_REJECTION_BODY, provenance });
  }
  // A missing or non-Linear permalink is an identity failure.
  if (canonicalLinearCommentUrl(comment.url ?? "") === null) {
    return commentRefusal("linear.comment.identity_mismatch", { responseBody: LINEAR_COMMENT_REJECTION_BODY, provenance });
  }
  const loaded = await loadDispatchIssue(token, payload, context);
  if ("refusal" in loaded) return loaded.refusal;
  const owner = verifiedLinearOwner(comment.user, expectedUserId);
  if (owner.disposition === "refused") return promptRefusal(owner.reason, payload);
  // The bare instruction; the envelope is added at dispatch.
  return {
    disposition: "verified",
    instruction: { content: validated.instruction, decisionReply: validated.instruction, expectedDecisionId: null, owner: owner.owner },
    provenance,
    issue: loaded.issue,
  };
}

/**
 * The interpretation dispatch a plain free-text reply makes to the waiting
 * question, or `null`. The model reads the prose against the restated
 * question and options. Owner comparison stays at each call site;
 * `interpretationClaim` returns no claim for an Other-bearing question.
 */
function interpretationForPendingDecision(
  context: LinearInstructionContext,
  agentSessionId: string,
  activityId: string,
  sourceCommentId: string | null,
  body: string,
): { readonly claim: LinearDecisionClaim; readonly instruction: string } | null {
  if (/^\s*axis-reply-/u.test(body)) return null;
  const claim = context.decisions.interpretationClaim(agentSessionId, activityId, sourceCommentId);
  if (claim === null) return null;
  const instruction = elicitationInterpretationInstruction(claim.elicitation, body);
  return instruction === null ? null : { claim, instruction };
}

/**
 * The custom answer a plain free-text reply makes to the waiting question,
 * when it offered an Other option: the text becomes the Other reply the token
 * path accepts, with the same limits and one-shot claim. Owner comparison
 * stays at each call site.
 */
function customAnswerForPendingDecision(
  context: LinearInstructionContext,
  agentSessionId: string,
  activityId: string,
  sourceCommentId: string | null,
  body: string,
): { readonly claim: LinearDecisionClaim; readonly reply: string } | null {
  if (/^\s*axis-reply-/u.test(body)) return null;
  const pending = context.decisions.pendingDecision(agentSessionId);
  if (pending === null || pending.elicitation.other === undefined) return null;
  const reply = normalizeElicitationDecisionReply(`${pending.elicitation.other.replyValue} ${body}`);
  if (reply === null) return null;
  const claim = context.decisions.decisionClaimForReply(agentSessionId, activityId, sourceCommentId, reply);
  if (claim === null || selectElicitationReply(claim.elicitation, reply) === null) return null;
  return { claim, reply };
}

/**
 * The prompted half of the comment channel: provenance from the Comment,
 * authority and body from the exact Activity, bound by identity rather than
 * text equality.
 */
async function resolveCommentPrompt(
  payload: AgentSessionEventPayload,
  context: LinearInstructionContext,
  bound: {
    readonly token: string;
    readonly expectedUserId: string;
    readonly sourceCommentId: string;
    readonly provenance: LinearCommentProvenance;
  },
): Promise<LinearInstructionResolution> {
  const { token, expectedUserId, sourceCommentId, provenance } = bound;
  const activity = payload.agentActivity;
  // The `sourceCommentId` comparison is defense in depth; capability absence is the reachable trigger.
  if (activity === null || activity.sourceCommentId !== sourceCommentId || context.provider.loadExactAgentActivity === undefined) {
    return commentRefusal("linear.comment.activity_provenance_mismatch", { responseBody: LINEAR_COMMENT_REJECTION_BODY, provenance });
  }
  let exact;
  try {
    exact = await context.provider.loadExactAgentActivity(token, activity.id);
  } catch {
    return commentRefusal("linear.comment.activity_unavailable");
  }
  if (
    exact.agentSessionId !== payload.agentSession.id
    || exact.activity.id !== activity.id
    || exact.activity.sourceCommentId !== sourceCommentId
    || exact.activity.userId !== expectedUserId
    || exact.activity.content.type !== "prompt"
  ) {
    return commentRefusal("linear.comment.activity_identity_mismatch", { responseBody: LINEAR_COMMENT_REJECTION_BODY, provenance });
  }
  const ownerDecision = verifiedLinearOwner(exact.activity.user ?? null, expectedUserId);
  if (ownerDecision.disposition === "refused") return promptRefusal(ownerDecision.reason, payload);
  const owner = ownerDecision.owner;
  const body = exact.activity.content.body;
  const normalizedReply = normalizeElicitationReply(body);
  const reply = normalizeElicitationDecisionReply(body);
  if (reply !== null) {
    const pendingDecision = context.decisions.decisionClaimForReply(payload.agentSession.id, activity.id, sourceCommentId, reply);
    if (
      pendingDecision === null
      || expectedUserId !== pendingDecision.ownerUserId
      || owner.userId !== pendingDecision.ownerUserId
      || owner.userUrl !== pendingDecision.ownerUserUrl
      || selectElicitationReply(pendingDecision.elicitation, reply) === null
    ) {
      return commentRefusal("linear.comment.pending_decision_mismatch", { responseBody: LINEAR_COMMENT_REJECTION_BODY, provenance });
    }
    return verifiedDispatch(token, payload, context, {
      content: elicitationReplyInstruction(pendingDecision.elicitation, reply)!,
      decisionReply: reply,
      expectedDecisionId: pendingDecision.decisionId,
      owner,
    }, provenance);
  }
  // Free text as the Other answer; the owner comparison is at this site.
  const customAnswer = customAnswerForPendingDecision(context, payload.agentSession.id, activity.id, sourceCommentId, body);
  if (
    customAnswer !== null
    && expectedUserId === customAnswer.claim.ownerUserId
    && owner.userId === customAnswer.claim.ownerUserId
    && owner.userUrl === customAnswer.claim.ownerUserUrl
  ) {
    return verifiedDispatch(token, payload, context, {
      content: elicitationReplyInstruction(customAnswer.claim.elicitation, customAnswer.reply)!,
      decisionReply: customAnswer.reply,
      expectedDecisionId: customAnswer.claim.decisionId,
      owner,
    }, provenance);
  }
  // Otherwise dispatch the text for interpretation against the offered options.
  const interpretation = interpretationForPendingDecision(context, payload.agentSession.id, activity.id, sourceCommentId, body);
  if (
    interpretation !== null
    && expectedUserId === interpretation.claim.ownerUserId
    && owner.userId === interpretation.claim.ownerUserId
    && owner.userUrl === interpretation.claim.ownerUserUrl
  ) {
    return verifiedDispatch(token, payload, context, {
      content: interpretation.instruction,
      decisionReply: body,
      expectedDecisionId: interpretation.claim.decisionId,
      owner,
    }, provenance);
  }
  // An outstanding decision this text does not answer, or a malformed reply
  // token, must not become a fresh instruction.
  if (context.decisions.pendingDecision(payload.agentSession.id) !== null || /^\s*axis-reply-/u.test(body)) {
    return commentRefusal("linear.comment.pending_decision_mismatch", { responseBody: LINEAR_COMMENT_REJECTION_BODY, provenance });
  }
  // Option labels stay reserved, so a late "Yes" cannot become an instruction.
  if (normalizedReply !== null && context.decisions.matchesReservedDecisionOptionLabel(payload.agentSession.id, normalizedReply)) {
    return commentRefusal("linear.comment.stale_decision_reply", { responseBody: LINEAR_COMMENT_REJECTION_BODY, provenance });
  }
  return verifiedDispatch(token, payload, context, { content: body, decisionReply: body, expectedDecisionId: null, owner }, provenance);
}

/**
 * The direct (non-comment) channel.
 *
 * - An explicit `stop` carries no instruction but still freezes an owner.
 * - A `created` event verifies an initial prompt Activity when present.
 * - A `prompted` event reads exactly one Activity and dispatches its body.
 */
export async function resolveLinearDirectInstruction(
  payload: AgentSessionEventPayload,
  context: LinearInstructionContext,
): Promise<LinearInstructionResolution> {
  if (payload.agentActivity?.signal === "stop") {
    const userId = payload.agentActivity.userId;
    if (userId === null || context.provider.loadExactUser === undefined) return promptRefusal("user_missing", payload);
    const token = await context.accessToken();
    if (token === null) return promptRefusal("credential_unavailable", payload);
    let user;
    try {
      // A single attempt, unlike the `created` read below.
      user = await context.provider.loadExactUser(token, userId);
    } catch {
      return promptRefusal("provider_unavailable", payload);
    }
    const owner = verifiedLinearOwner(user, userId);
    if (owner.disposition === "refused") return promptRefusal(owner.reason, payload);
    return {
      disposition: "verified",
      instruction: { content: "", decisionReply: "", expectedDecisionId: null, owner: owner.owner },
      provenance: null,
      issue: null,
    };
  }
  if (payload.action === "created") {
    // A created assignment may carry its initial prompt as an Activity.
    if (payload.agentActivity !== null) {
      if (payload.agentActivity.userId !== payload.agentSession.creatorId) return promptRefusal("user_mismatch", payload);
      return resolveDirectPrompt(payload, context);
    }
    if (context.provider.loadExactUser === undefined) return promptRefusal("capability_unavailable", payload);
    const token = await context.accessToken();
    if (token === null) return promptRefusal("credential_unavailable", payload);
    let user;
    try {
      user = await readExactLinear(() => context.provider.loadExactUser!(token, payload.agentSession.creatorId), context.sleep);
    } catch (error) {
      if (error instanceof RelayError && error.retryable) throw error;
      return promptRefusal("provider_unavailable", payload);
    }
    const loaded = await loadDispatchIssue(token, payload, context);
    if ("refusal" in loaded) return loaded.refusal;
    const owner = verifiedLinearOwner(user, payload.agentSession.creatorId);
    if (owner.disposition === "refused") return promptRefusal(owner.reason, payload);
    // No user command; `promptContext` is presentation, not an instruction.
    return {
      disposition: "verified",
      instruction: { content: "", decisionReply: "", expectedDecisionId: null, owner: owner.owner },
      provenance: null,
      issue: loaded.issue,
    };
  }
  if (payload.action === "prompted") return resolveDirectPrompt(payload, context);
  return { disposition: "refused", refusal: { kind: "unresolvable" } };
}

/**
 * One exact Activity read and its checks, then the issue read, then dispatch.
 * A comment-origin Activity here is refused, so the comment gates cannot be skipped.
 */
async function resolveDirectPrompt(
  payload: AgentSessionEventPayload,
  context: LinearInstructionContext,
): Promise<LinearInstructionResolution> {
  const checked = await verifyDirectPrompt(payload, context);
  if (checked.disposition !== "checked") return checked;
  // Outside the guard, so a retryable issue read stays retryable.
  return verifiedDispatch(checked.token, payload, context, checked.instruction, null);
}

/**
 * The only comment a direct prompt Activity may hang from. A plain delegation's `created` event carries
 * Linear's initial prompt Activity under the session's artificial root comment, which
 * `resolveLinearCommentInvocation` has already reread and proven artificial before handing the event to
 * the direct path; every other direct prompt has no source comment.
 */
function directPromptSourceCommentId(payload: AgentSessionEventPayload): string | null {
  return payload.action === "created" ? payload.agentSession.commentId ?? null : null;
}

async function verifyDirectPrompt(
  payload: AgentSessionEventPayload,
  context: LinearInstructionContext,
): Promise<LinearInstructionResolution | { readonly disposition: "checked"; readonly token: string; readonly instruction: VerifiedLinearInstruction }> {
  const activity = payload.agentActivity;
  if (activity === null || activity.userId === null) return promptRefusal("user_missing", payload);
  if (activity.userId === payload.appUserId) return promptRefusal("axis_authored", payload);
  if (context.provider.loadExactAgentActivity === undefined) return promptRefusal("capability_unavailable", payload);
  /** The reason the guard reports for a throw: `provider_unavailable` only around the provider call. */
  let guarded: LinearPromptRejectionReason = "internal_error";
  try {
    const token = await context.accessToken();
    if (token === null) return promptRefusal("credential_unavailable", payload);
    guarded = "provider_unavailable";
    const exact = await context.provider.loadExactAgentActivity(token, activity.id);
    guarded = "internal_error";
    if (exact.activity.id !== activity.id) return promptRefusal("activity_mismatch", payload);
    if (exact.agentSessionId !== payload.agentSession.id) return promptRefusal("session_mismatch", payload);
    if ((exact.activity.sourceCommentId ?? null) !== directPromptSourceCommentId(payload)) return promptRefusal("activity_mismatch", payload);
    if (exact.activity.userId === null) return promptRefusal("user_missing", payload);
    if (exact.activity.userId === payload.appUserId) return promptRefusal("axis_authored", payload);
    if (exact.activity.userId !== activity.userId) return promptRefusal("user_mismatch", payload);
    if (exact.activity.content.type !== "prompt") return promptRefusal("wrong_type", payload);
    const body = exact.activity.content.body;
    const ownerDecision = verifiedLinearOwner(exact.activity.user ?? null, activity.userId);
    if (ownerDecision.disposition === "refused") return promptRefusal(ownerDecision.reason, payload);
    const owner = ownerDecision.owner;
    const normalizedReply = normalizeElicitationReply(body);
    const reply = normalizeElicitationDecisionReply(body);
    if (reply !== null) {
      const pendingDecision = context.decisions.decisionClaimForReply(payload.agentSession.id, activity.id, null, reply);
      if (
        pendingDecision === null
        || activity.userId !== pendingDecision.ownerUserId
        || owner.userUrl !== pendingDecision.ownerUserUrl
        || selectElicitationReply(pendingDecision.elicitation, reply) === null
      ) return promptRefusal("logical_conflict", payload);
      return {
        disposition: "checked",
        token,
        instruction: {
          content: elicitationReplyInstruction(pendingDecision.elicitation, reply)!,
          decisionReply: reply,
          expectedDecisionId: pendingDecision.decisionId,
          owner,
        },
      };
    }
    // Prompt-origin twin of the comment path's custom answer.
    const customAnswer = customAnswerForPendingDecision(context, payload.agentSession.id, activity.id, null, body);
    if (
      customAnswer !== null
      && activity.userId === customAnswer.claim.ownerUserId
      && owner.userUrl === customAnswer.claim.ownerUserUrl
    ) {
      return {
        disposition: "checked",
        token,
        instruction: {
          content: elicitationReplyInstruction(customAnswer.claim.elicitation, customAnswer.reply)!,
          decisionReply: customAnswer.reply,
          expectedDecisionId: customAnswer.claim.decisionId,
          owner,
        },
      };
    }
    // Prompt-origin twin of the comment path's interpretation.
    const interpretation = interpretationForPendingDecision(context, payload.agentSession.id, activity.id, null, body);
    if (
      interpretation !== null
      && activity.userId === interpretation.claim.ownerUserId
      && owner.userUrl === interpretation.claim.ownerUserUrl
    ) {
      return {
        disposition: "checked",
        token,
        instruction: {
          content: interpretation.instruction,
          decisionReply: body,
          expectedDecisionId: interpretation.claim.decisionId,
          owner,
        },
      };
    }
    if (context.decisions.pendingDecision(payload.agentSession.id) !== null || /^\s*axis-reply-/u.test(body)) {
      return promptRefusal("logical_conflict", payload);
    }
    if (normalizedReply !== null && context.decisions.matchesReservedDecisionOptionLabel(payload.agentSession.id, normalizedReply)) {
      return promptRefusal("logical_conflict", payload);
    }
    return { disposition: "checked", token, instruction: { content: body, decisionReply: body, expectedDecisionId: null, owner } };
  } catch {
    return promptRefusal(guarded, payload);
  }
}
