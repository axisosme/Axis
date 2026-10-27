/**
 * Linear's `ChannelOutboxDeliverer`: the only part of outbound Linear delivery that talks to the Linear
 * API. Outbox bookkeeping stays with `ChannelOutboxPort` and retry policy with the caller.
 *
 * Linear accepts only UUID v4 Activity ids, so a non-v4 row is refused locally and the persisted id is
 * never regenerated. Authority is re-checked before every create and reconcile: an unusable authority is
 * `rejected` on create and `unresolved` on reconcile, since `conflict` would fail a possibly delivered row.
 */

import { randomUUID } from "node:crypto";

import {
  ChannelSessionInputError,
  channelDeliveryTarget,
  channelInstructionDigest,
  type ChannelDeliveryCreateResult,
  type ChannelDeliveryReconcileResult,
  type ChannelMessageContent,
  type ChannelOutboxDeliverer,
  type ChannelOutboxRecord,
  type ChannelDeliveryTarget,
  type LinearChannelDeliveryTarget,
} from "../../router/channel-conversation-types.js";

import { RelayError } from "../errors.js";
import { authorizeLinearAuthority, type LinearFeedbackLoopPort } from "./authorization.js";
import type { LinearAuthorityPort } from "./installation.js";
import type { LinearCredentialAccess } from "../oauth.js";
import type { LinearNativeSignal, LinearProviderPort } from "./provider.js";
import { nativeElicitationOptions, type StructuredElicitation } from "../../shared/elicitation.js";
import type { LinearActivityContent, OutboxRecord } from "../types.js";

/**
 * Linear's `AgentActivityCreateInput.id` rule: a UUID version 4 (version nibble at index 14, RFC 4122
 * variant at index 19). A generic UUID validator would accept the v5 ids Linear rejects.
 */
const LINEAR_ACTIVITY_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;

/** True only for a UUID v4, which is the only id Linear accepts for an Agent Activity. */
export function isLinearActivityId(value: unknown): value is string {
  return typeof value === "string" && LINEAR_ACTIVITY_ID_PATTERN.test(value);
}

/**
 * The Activity id allocation rule, called once inside the transaction that enqueues the outbox row.
 *
 * A Worker receipt's own `preferredActivityId` is preserved when it is a UUID v4, so the Activity id
 * correlates with the receipt, and replaced otherwise so an unpostable row is never created.
 */
export function linearActivityId(preferredActivityId?: string, randomId: () => string = randomUUID): string {
  if (isLinearActivityId(preferredActivityId)) return preferredActivityId;
  const allocated = randomId();
  if (!isLinearActivityId(allocated)) {
    // An id source that does not produce UUID v4 is a wiring fault; fail here rather than one row at a time.
    throw new RelayError("axis.relay.invalid_transition", "The Linear Activity id source did not produce a UUID v4.", 409);
  }
  return allocated;
}

/** Why delivery was refused. A reason, not a safe code: the durable code is chosen by the deliverer. */
export type LinearDeliveryRefusalReason =
  /** No local AgentSession for the target. Keeps its own durable code. */
  | "session_unknown"
  /** No active installation for the session's workspace/App User. */
  | "installation_revoked"
  /** Active installation, but the session's Issue Team is not reachable. */
  | "team_forbidden"
  /** Authority is fine, but the encrypted credential could not be read. */
  | "credential_unavailable";

/**
 * The result of the pre-delivery authority/token check. `authorized` carries the workspace because a
 * Linear delivery target names only `agentSessionId` and the provider's `OutboxRecord` needs it.
 */
export type LinearDeliveryAuthorization =
  | { readonly disposition: "authorized"; readonly accessToken: string; readonly workspaceId: string }
  | { readonly disposition: "refused"; readonly reason: LinearDeliveryRefusalReason };

/**
 * The pre-delivery authority hook; `linearDeliveryAuthority` is the production implementation. It must
 * fail closed: a revoked installation, an unreachable Team and an unreadable credential are all `refused`.
 */
export interface LinearDeliveryAuthority {
  /** Takes the target alone: authority belongs to the conversation, never to an individual message. */
  authorize(input: { readonly target: LinearChannelDeliveryTarget }): Promise<LinearDeliveryAuthorization>;
}

/**
 * The retained session identities Activity delivery re-authorizes against. The Team is the session's
 * own `issueTeamId`, never one named in an instruction.
 */
export interface LinearDeliverySession {
  readonly organizationId: string;
  readonly appUserId: string;
  readonly teamId: string;
}

export interface LinearDeliverySessionLookup {
  deliverySession(agentSessionId: string): LinearDeliverySession | null;
}

/**
 * The production `LinearDeliveryAuthority`: session lookup, then `authorizeLinearAuthority`, then the
 * credential read. Authority is decided first, so a revoked installation never touches the vault.
 */
export function linearDeliveryAuthority(ports: {
  readonly sessions: LinearDeliverySessionLookup;
  readonly installations: LinearAuthorityPort;
  readonly credentials: LinearCredentialAccess;
}): LinearDeliveryAuthority {
  return {
    async authorize(input): Promise<LinearDeliveryAuthorization> {
      const session = ports.sessions.deliverySession(input.target.agentSessionId);
      if (session === null) return { disposition: "refused", reason: "session_unknown" };
      const decision = authorizeLinearAuthority(
        ports.installations.authority(session.organizationId, session.appUserId, session.teamId),
      );
      if (decision.disposition === "refused") {
        return {
          disposition: "refused",
          reason: decision.safeCode === "linear.auth.team_forbidden" ? "team_forbidden" : "installation_revoked",
        };
      }
      const accessToken = await ports.credentials.accessToken(decision.credentialRef);
      return accessToken === null
        ? { disposition: "refused", reason: "credential_unavailable" }
        : { disposition: "authorized", accessToken, workspaceId: session.organizationId };
    },
  };
}

/**
 * Recognises Axis's own Activities for the feedback gate. Must match both the outbox row id and the
 * recorded `providerMessageId`: create confirms on whatever id Linear returns, which can differ from
 * the id sent.
 */
export interface LinearDeliveredActivityLookup {
  deliveredActivity(activityId: string): boolean;
}

export type LinearActivityTransport = Pick<LinearProviderPort, "createAgentActivity" | "reconcileAgentActivity">;

/**
 * Reads back the structured decision an `elicitation` row was enqueued for, so it can be offered as
 * Linear's native `select`. Reads the immutable `elicitation_json` written with the row in
 * `channel_waiting_decision_ledger`; nothing is re-derived from the rendered body.
 */
export interface LinearElicitationLookup {
  forOutbox(input: { readonly outboxId: string }): StructuredElicitation | null;
}

/**
 * Narrows neutral `ChannelMessageContent` back to Linear Activity content. Content Linear would reject
 * returns `null` and is refused without an API call.
 */
export function linearActivityContent(value: ChannelMessageContent): LinearActivityContent | null {
  const type = value.type;
  if (type === "action") {
    if (typeof value.action !== "string" || typeof value.parameter !== "string") return null;
    if (value.result !== undefined && typeof value.result !== "string") return null;
    return {
      type: "action",
      action: value.action,
      parameter: value.parameter,
      ...(typeof value.result === "string" ? { result: value.result } : {}),
    };
  }
  if (type === "thought" || type === "elicitation" || type === "response" || type === "error") {
    return typeof value.body === "string" ? { type, body: value.body } : null;
  }
  return null;
}

/**
 * Safe codes this module produces, as opposed to the provider's `linear.graphql.*` codes. All of them
 * land on an outbox row and are operator-visible.
 */
export const LINEAR_DELIVERY_SAFE_CODES = Object.freeze({
  /** The persisted row id is not a UUID v4, so Linear can never accept it. */
  nonUuidV4Id: "linear.activity.non_uuid_v4_id",
  /** The persisted content is not a Linear Agent Activity content object. */
  invalidContent: "linear.activity.invalid_content",
  /** The freshly resolved target names a different AgentSession than the row was enqueued for. */
  targetMismatch: "linear.activity.delivery_target_mismatch",
  /** The persisted content does not hash to the persisted digest, so it could never be reconciled. */
  contentDigestMismatch: "linear.activity.content_digest_mismatch",
  /**
   * An `elicitation` row whose decision has no options to offer. Fails closed: the rendered body
   * prints no reply tokens, so an elicitation without native options could not be answered.
   */
  elicitationOptionsUnavailable: "linear.activity.elicitation_options_unavailable",
  /** The code every authority refusal except a missing session collapses to. */
  authorityUnavailable: "linear.auth.revoked",
  /** The authority refusal for a missing session, which keeps its own code. */
  sessionMissing: "axis.relay.session_missing",
} as const);

/**
 * Whether the content to be posted hashes to the persisted digest; otherwise it would post and then
 * reconcile as `conflict` forever. Digests the narrowed value, which is in the provider's canonical
 * field order (checked in `test/relay/linear/channel-e2e.test.ts`), so extra fields are refused too.
 */
function contentMatchesDigest(content: LinearActivityContent, record: ChannelOutboxRecord): boolean {
  return channelInstructionDigest(JSON.stringify(content)) === record.contentDigest;
}

function linearTarget(target: ChannelDeliveryTarget): LinearChannelDeliveryTarget {
  const validated = channelDeliveryTarget(target);
  // A non-Linear target here is a composition fault, not a delivery outcome.
  if (validated.system !== "linear") throw new ChannelSessionInputError("malformed_delivery_target");
  return validated;
}

/** Builds the provider's `OutboxRecord`. `contentDigest` passes through: both digest forms are the same string. */
function providerRecord(
  record: ChannelOutboxRecord,
  target: LinearChannelDeliveryTarget,
  content: LinearActivityContent,
  workspaceId: string,
): OutboxRecord {
  return {
    id: record.id,
    workspaceId,
    agentSessionId: target.agentSessionId,
    sourceEventId: record.sourceEventId,
    content,
    contentDigest: record.contentDigest,
    state: record.state,
    attemptCount: record.attemptCount,
    availableAt: record.availableAt,
    createdAt: record.createdAt,
  };
}

/** The single place a refusal reason becomes a persisted code; all but `session_unknown` collapse to `linear.auth.revoked`. */
function authorityRefusalCode(reason: LinearDeliveryRefusalReason): string {
  return reason === "session_unknown"
    ? LINEAR_DELIVERY_SAFE_CODES.sessionMissing
    : LINEAR_DELIVERY_SAFE_CODES.authorityUnavailable;
}

export interface LinearActivityDelivererOptions {
  readonly transport: LinearActivityTransport;
  readonly authority: LinearDeliveryAuthority;
  readonly delivered: LinearDeliveredActivityLookup;
  /** Required, not optional: see `LinearNativeSignal`. An uninjected lookup would silently post unanswerable decisions. */
  readonly elicitations: LinearElicitationLookup;
}

export class LinearActivityDeliverer implements ChannelOutboxDeliverer, LinearFeedbackLoopPort {
  readonly #transport: LinearActivityTransport;
  readonly #authority: LinearDeliveryAuthority;
  readonly #delivered: LinearDeliveredActivityLookup;
  readonly #elicitations: LinearElicitationLookup;

  constructor(options: LinearActivityDelivererOptions) {
    this.#transport = options.transport;
    this.#authority = options.authority;
    this.#delivered = options.delivered;
    this.#elicitations = options.elicitations;
  }

  async create(input: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }): Promise<ChannelDeliveryCreateResult> {
    const target = linearTarget(input.target);
    const { record } = input;

    const postable = this.#postable(record, target);
    if ("safeCode" in postable) return { disposition: "rejected", safeCode: postable.safeCode };
    const { content } = postable;

    const authority = await this.#authority.authorize({ target });
    if (authority.disposition === "refused") {
      return { disposition: "rejected", safeCode: authorityRefusalCode(authority.reason) };
    }

    // An elicitation is posted with Linear's native `select`; other content carries no signal.
    let native: LinearNativeSignal | null = null;
    if (content.type === "elicitation") {
      const elicitation = this.#elicitations.forOutbox({ outboxId: record.id });
      if (elicitation === null) return { disposition: "rejected", safeCode: LINEAR_DELIVERY_SAFE_CODES.elicitationOptionsUnavailable };
      // Throws on an invalid projection rather than posting a partial one.
      native = { signal: "select", options: nativeElicitationOptions(elicitation) };
    }

    const result = await this.#transport.createAgentActivity(
      authority.accessToken,
      providerRecord(record, target, content, authority.workspaceId),
      native,
    );
    if (result.disposition === "confirmed") {
      return { disposition: "confirmed", providerMessageId: result.activityId };
    }
    if (result.disposition === "ambiguous") {
      return { disposition: "ambiguous", safeCode: result.safeCode };
    }
    // The provider flags the retriable subset of rejections; the caller retries those and records the rest.
    return result.retryable === true
      ? { disposition: "retryable", safeCode: result.safeCode }
      : { disposition: "rejected", safeCode: result.safeCode };
  }

  async reconcile(input: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }): Promise<ChannelDeliveryReconcileResult> {
    const target = linearTarget(input.target);
    const { record } = input;

    // Same guards as `create`, but an unpostable row says nothing about Linear's state, so it stays
    // `ambiguous` (`unresolved`) for the caller to rotate past, and the reason is discarded.
    const postable = this.#postable(record, target);
    if ("safeCode" in postable) return { disposition: "unresolved" };
    const { content } = postable;

    const authority = await this.#authority.authorize({ target });
    if (authority.disposition === "refused") return { disposition: "unresolved" };

    // One exact read: only identity plus content-digest match confirms, and `unknown` is never read as absence.
    const result = await this.#transport.reconcileAgentActivity(
      authority.accessToken,
      providerRecord(record, target, content, authority.workspaceId),
    );
    if (result.disposition === "confirmed") return { disposition: "confirmed", providerMessageId: result.activityId };
    if (result.disposition === "conflict") return { disposition: "conflict", safeCode: result.safeCode };
    return { disposition: "unresolved" };
  }

  /**
   * Whether `activityId` is an Activity Axis posted; consumed by `isLinearFeedbackLoop`. A blank id
   * answers `false` without a query, so it is treated as an inbound prompt and fully verified.
   */
  isAxisActivity(activityId: string): boolean {
    return typeof activityId === "string" && activityId.length > 0 && this.#delivered.deliveredActivity(activityId);
  }

  /**
   * The local, permanent refusals `create` and `reconcile` share, all decided before
   * `#authority.authorize` so an unpostable row costs no credential read or API call. Returns the
   * narrowed content or the safe code to refuse with.
   */
  #postable(record: ChannelOutboxRecord, target: LinearChannelDeliveryTarget): { readonly content: LinearActivityContent } | { readonly safeCode: string } {
    if (!isLinearActivityId(record.id)) return { safeCode: LINEAR_DELIVERY_SAFE_CODES.nonUuidV4Id };
    const mismatch = this.#targetMismatch(record, target);
    if (mismatch !== null) return { safeCode: mismatch };
    const content = linearActivityContent(record.content);
    if (content === null) return { safeCode: LINEAR_DELIVERY_SAFE_CODES.invalidContent };
    if (!contentMatchesDigest(content, record)) return { safeCode: LINEAR_DELIVERY_SAFE_CODES.contentDigestMismatch };
    return { content };
  }

  /**
   * A Linear target carries only the immutable `agentSessionId`, so a freshly resolved target can never
   * legitimately differ from the row's snapshot. A difference would post into another conversation.
   */
  #targetMismatch(record: ChannelOutboxRecord, target: LinearChannelDeliveryTarget): string | null {
    const snapshot = record.target;
    if (snapshot.system !== "linear" || snapshot.agentSessionId !== target.agentSessionId) {
      return LINEAR_DELIVERY_SAFE_CODES.targetMismatch;
    }
    return null;
  }
}
