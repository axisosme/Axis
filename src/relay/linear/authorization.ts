import { validateCanonicalLinearUserUrl } from "../../shared/elicitation.js";

import type { LinearAuthorityPort, LinearInstallationAuthority } from "./installation.js";
import type { VerifiedLinearUser } from "./provider.js";
import type { AgentSessionEventPayload } from "../types.js";

/**
 * The decision half of the Linear authorization boundary: whether an installation grant
 * (`./installation.ts`) covers one authenticated event. Pure over its inputs and the injected authority
 * read, and returns closed decisions instead of throwing, so a refusal must be read before proceeding.
 *
 * `authorizeLinearEvent` runs the feedback-loop exclusion first, so dropping Axis's own Activities never
 * depends on an installation lookup (a retryable lookup failure would turn a feedback loop into a retry
 * loop), then installation and Team authority on `(organizationId, appUserId, issueTeamId)`.
 *
 * Only `authorized` carries `credentialRef`. The two refusal codes stay distinct so an operator can tell
 * an uninstalled app from an ungranted Team; no decision carries provider diagnostics or user text.
 */

/** Stable internal refusal codes. Both classify as `authorization_unavailable` for user-facing copy. */
export type LinearAuthorizationRefusalCode = "linear.auth.revoked" | "linear.auth.team_forbidden";

export type LinearAuthorizationDecision =
  /** Gate 1: the event describes Axis talking to itself. Not an error and not a denial -- there is no work. */
  | { readonly disposition: "feedback_ignored"; readonly safeCode: "linear.ingress.feedback_loop" }
  /** Gate 2: no active installation, or an active installation without this Team. */
  | { readonly disposition: "refused"; readonly safeCode: LinearAuthorizationRefusalCode }
  /** Both gates passed. This is the only shape carrying a credential reference. */
  | { readonly disposition: "authorized"; readonly credentialRef: string };

/**
 * Whether Axis emitted an Activity id: catches Axis-authored Activities whose webhook `userId` is
 * absent or no longer equals the App User.
 */
export interface LinearFeedbackLoopPort {
  isAxisActivity(activityId: string): boolean;
}

/**
 * Whether this event is Axis observing its own output: the Activity's author is the App User, or the
 * delivery ledger recognises the Activity id. The ledger is consulted only when an Activity is named, so
 * a `created` event is never mistaken for feedback.
 */
export function isLinearFeedbackLoop(payload: AgentSessionEventPayload, port: LinearFeedbackLoopPort): boolean {
  if (payload.agentActivity?.userId === payload.appUserId) return true;
  const activityId = payload.agentActivity?.id;
  return activityId !== null && activityId !== undefined && port.isAxisActivity(activityId);
}

/**
 * Runs both gates for one authenticated `AgentSessionEvent`. The Team is the session issue's
 * `issueTeamId`, so a prompt cannot choose the Team it is authorized against.
 */
export function authorizeLinearEvent(
  payload: AgentSessionEventPayload,
  ports: { readonly feedback: LinearFeedbackLoopPort; readonly installations: LinearAuthorityPort },
): LinearAuthorizationDecision {
  if (isLinearFeedbackLoop(payload, ports.feedback)) {
    return { disposition: "feedback_ignored", safeCode: "linear.ingress.feedback_loop" };
  }
  return authorizeLinearAuthority(
    ports.installations.authority(payload.organizationId, payload.appUserId, payload.agentSession.issueTeamId),
  );
}

/**
 * The authority gate alone, for re-checks before command dispatch and Activity delivery. Reports the two
 * refusal codes separately; Activity delivery maps both to `linear.auth.revoked` itself.
 */
export function authorizeLinearAuthority(
  authority: LinearInstallationAuthority | null,
): Exclude<LinearAuthorizationDecision, { disposition: "feedback_ignored" }> {
  if (authority === null) return { disposition: "refused", safeCode: "linear.auth.revoked" };
  if (!authority.allowed) return { disposition: "refused", safeCode: "linear.auth.team_forbidden" };
  return { disposition: "authorized", credentialRef: authority.credentialRef };
}

/** The frozen execution owner: an exact active human with a canonical Linear profile URL. */
export interface VerifiedLinearOwner {
  readonly userId: string;
  readonly userUrl: string;
}

export type LinearOwnerDecision =
  | { readonly disposition: "verified"; readonly owner: VerifiedLinearOwner }
  /** One reason for every failure; see below. */
  | { readonly disposition: "refused"; readonly reason: "user_mismatch" };

/**
 * Freezes one exact active, non-App human with a canonical Linear `User.url` as the owner of an execution
 * input, from an authenticated provider reread. There is no fallback owner.
 *
 * Every failure collapses to `user_mismatch`, because the reason reaches a durable audit row and must
 * not reveal which property of a named user failed. The URL is required because waiting decisions render
 * it for Linear's mention conversion; a non-Linear URL would put an attacker-chosen link into Axis's copy.
 */
export function verifiedLinearOwner(user: VerifiedLinearUser | null, expectedUserId: string): LinearOwnerDecision {
  const url = validateCanonicalLinearUserUrl(user?.url);
  if (user === null || user.id !== expectedUserId || user.active !== true || user.app !== false || url === null) {
    return { disposition: "refused", reason: "user_mismatch" };
  }
  return { disposition: "verified", owner: { userId: user.id, userUrl: url } };
}
