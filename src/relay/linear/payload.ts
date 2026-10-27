import { RelayError } from "../errors.js";
import { sha256 } from "../security.js";
import type {
  AgentActivitySnapshot,
  AgentSessionEventPayload,
  LinearWebhookGuidance,
  OAuthRevocationPayload,
  PermissionChangePayload,
  SupportedWebhookPayload,
} from "../types.js";

export interface UnsupportedWebhookPayload {
  readonly type: "Unsupported";
  readonly originalType: string;
  readonly originalAction: string;
  readonly webhookId: string;
  readonly webhookTimestamp: number;
  readonly organizationId: string | null;
}

export type ParsedWebhookPayload = SupportedWebhookPayload | UnsupportedWebhookPayload;

function record(value: unknown, field: string): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw malformed(`${field} must be an object.`);
  }
  return value as Readonly<Record<string, unknown>>;
}

function string(value: unknown, field: string, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) throw malformed(`${field} must be a string.`);
  return value;
}

/**
 * An optional free-text field, or one no consumer reads: absent becomes null and empty is allowed.
 * Used for presentation-only fields (`promptContext`, `agentActivity.body`; the instruction always comes
 * from the exact Activity fetched from Linear) and the unread `agentActivity.createdAt`. Identifiers a
 * consumer reads must use `optionalIdentifier`.
 */
function optionalText(value: unknown, field: string): string | null {
  return value === null || value === undefined ? null : string(value, field, true);
}

/**
 * An optional identifier: absent becomes null, blank (`trim().length === 0`, as `text()` in
 * `../../router/router.ts` refuses) is malformed. Accepted values are returned verbatim, never trimmed,
 * so `" org "` and `"org"` never become one tenant identity.
 */
function optionalIdentifier(value: unknown, field: string): string | null {
  if (value === null || value === undefined) return null;
  return nonBlank(string(value, field, true), field);
}

/** A required identifier; absent and blank fail with distinct messages. */
function requiredIdentifier(value: string | null, field: string): string {
  if (value === null) throw malformed(`${field} is required.`);
  return nonBlank(value, field);
}

function nonBlank(candidate: string, field: string): string {
  if (candidate.trim().length === 0) throw malformed(`${field} must be a non-empty identifier.`);
  return candidate;
}

function guidance(value: unknown): LinearWebhookGuidance | null {
  if (value === null || value === undefined) return null;
  if (!Array.isArray(value) || value.length > 64) throw malformed("guidance must be an array of at most 64 rules.");
  if (Buffer.byteLength(JSON.stringify(value), "utf8") > 64 * 1_024) throw malformed("guidance must not exceed 64 KiB.");
  return value.map((item: unknown) => {
    const rule = record(item, "guidance rule");
    const body = nonBlank(string(rule.body, "guidance body"), "guidance body");
    const origin = record(rule.origin, "guidance origin");
    if (origin.type === "Organization") return { body, origin: { type: "Organization" as const } };
    if (origin.type !== "Team") throw malformed("guidance origin type must be Organization or Team.");
    const team = record(origin.team, "guidance team");
    return {
      body,
      origin: {
        type: "Team" as const,
        team: {
          id: nonBlank(string(team.id, "guidance team id"), "guidance team id"),
          key: nonBlank(string(team.key, "guidance team key"), "guidance team key"),
          name: nonBlank(string(team.name, "guidance team name"), "guidance team name"),
          displayName: nonBlank(string(team.displayName, "guidance team displayName"), "guidance team displayName"),
          parentId: optionalIdentifier(team.parentId, "guidance team parentId"),
        },
      },
    };
  });
}

function promptBody(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  const content = value as Readonly<Record<string, unknown>>;
  return typeof content.body === "string" ? content.body : null;
}

function stringArray(value: unknown, field: string): readonly string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string" || item.length === 0)) {
    throw malformed(`${field} must be an array of strings.`);
  }
  return [...new Set(value as string[])];
}

function malformed(message: string): RelayError {
  return new RelayError("linear.ingress.malformed", message, 400);
}

function decodeWebhook(rawBody: Buffer): Readonly<Record<string, unknown>> {
  let decoded: unknown;
  try {
    decoded = JSON.parse(rawBody.toString("utf8"));
  } catch {
    throw malformed("The webhook body is not valid JSON.");
  }
  return record(decoded, "webhook");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const object = value as Readonly<Record<string, unknown>>;
  return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`).join(",")}}`;
}

export function webhookSemanticDigest(rawBody: Buffer): string {
  const body = decodeWebhook(rawBody);
  const semanticBody = Object.fromEntries(
    Object.entries(body).filter(([key]) => key !== "webhookTimestamp"),
  );
  return sha256(canonicalJson(semanticBody));
}

function timestamp(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) throw malformed("webhookTimestamp is invalid.");
  return value;
}

function agentActivity(value: unknown): AgentActivitySnapshot | null {
  if (value === null || value === undefined) return null;
  const activity = record(value, "agentActivity");
  const user = activity.user === null || activity.user === undefined ? null : record(activity.user, "agentActivity.user");
  const signal = activity.signal;
  if (signal !== null && signal !== undefined && !["auth", "continue", "select", "stop"].includes(String(signal))) {
    throw malformed("agentActivity.signal is unsupported.");
  }
  return {
    // A real identifier (feedback-loop lookup, `sourceEventId`, retry comparison), so blank is rejected.
    id: nonBlank(string(activity.id, "agentActivity.id"), "agentActivity.id"),
    // Real payloads use the nested `user` shape, so both shapes reject a blank actor.
    userId: user === null ? optionalIdentifier(activity.userId, "agentActivity.userId") : nonBlank(string(user.id, "agentActivity.user.id"), "agentActivity.user.id"),
    promptBody: promptBody(activity.content),
    body: optionalText(activity.body, "agentActivity.body"),
    signal: (signal ?? null) as AgentActivitySnapshot["signal"],
    createdAt: optionalText(activity.createdAt, "agentActivity.createdAt"),
    sourceCommentId: optionalIdentifier(activity.sourceCommentId, "agentActivity.sourceCommentId"),
  };
}

export function parseLinearWebhook(rawBody: Buffer): ParsedWebhookPayload {
  const body = decodeWebhook(rawBody);
  const type = string(body.type, "type");
  const action = string(body.action, "action");
  // Required for every shape and compared for retry-conflict equality, so blank is rejected.
  const webhookId = nonBlank(string(body.webhookId, "webhookId"), "webhookId");
  const webhookTimestamp = timestamp(body.webhookTimestamp);
  // Permissive here so an unknown event shape can still be recorded and quarantined; each supported
  // branch requires it with `requiredIdentifier`. It is persisted on the delivery row, so the
  // `Unsupported` return normalizes blank to null.
  const organizationId = optionalText(body.organizationId, "organizationId");

  if (type === "AgentSessionEvent" && (action === "created" || action === "prompted")) {
    const session = record(body.agentSession, "agentSession");
    const issue = record(session.issue, "agentSession.issue");
    const team = record(issue.team, "agentSession.issue.team");
    const creator = record(session.creator, "agentSession.creator");
    // Compared against activity, session and comment actors for self-authorship, so blank must fail closed.
    const appUserId = nonBlank(string(body.appUserId, "appUserId"), "appUserId");
    const tenantId = requiredIdentifier(organizationId, "organizationId");
    const payload: AgentSessionEventPayload = {
      type,
      action,
      webhookId,
      webhookTimestamp,
      organizationId: tenantId,
      appUserId,
      agentSession: {
        // Router refuses a blank id, so it is rejected here too.
        id: nonBlank(string(session.id, "agentSession.id"), "agentSession.id"),
        organizationId: tenantId,
        appUserId,
        // Also refused blank by Router, and compared against a fetched comment's issue and Team.
        issueId: nonBlank(string(issue.id, "agentSession.issue.id"), "agentSession.issue.id"),
        issueTeamId: nonBlank(string(team.id, "agentSession.issue.team.id"), "agentSession.issue.team.id"),
        // The expected comment/owner author id.
        creatorId: nonBlank(string(creator.id, "agentSession.creator.id"), "agentSession.creator.id"),
        // Only folded into an opaque idempotency digest, so it is not required to be non-blank.
        status: string(session.status, "agentSession.status"),
        commentId: optionalIdentifier(session.commentId, "agentSession.commentId"),
        sourceCommentId: optionalIdentifier(session.sourceCommentId, "agentSession.sourceCommentId"),
      },
      agentActivity: agentActivity(body.agentActivity),
      promptContext: optionalText(body.promptContext, "promptContext"),
      guidance: guidance(body.guidance),
    };
    if (action === "prompted" && payload.agentActivity === null) throw malformed("prompted requires agentActivity.");
    return payload;
  }

  if (type === "PermissionChange" && action === "teamAccessChanged") {
    const tenantId = requiredIdentifier(organizationId, "organizationId");
    const payload: PermissionChangePayload = {
      type,
      action,
      webhookId,
      webhookTimestamp,
      organizationId: tenantId,
      // Scopes `applyPermissionChange`'s `app_user_id = ?`.
      appUserId: nonBlank(string(body.appUserId, "appUserId"), "appUserId"),
      addedTeamIds: stringArray(body.addedTeamIds, "addedTeamIds"),
      removedTeamIds: stringArray(body.removedTeamIds, "removedTeamIds"),
      canAccessAllPublicTeams: body.canAccessAllPublicTeams === true,
    };
    if (typeof body.canAccessAllPublicTeams !== "boolean") throw malformed("canAccessAllPublicTeams must be a boolean.");
    return payload;
  }

  if (type === "OAuthApp" && action === "revoked") {
    const tenantId = requiredIdentifier(organizationId, "organizationId");
    const payload: OAuthRevocationPayload = {
      type,
      action,
      webhookId,
      webhookTimestamp,
      organizationId: tenantId,
      // Scopes `revoke(organizationId, appUserId)`.
      appUserId: nonBlank(string(body.appUserId, "appUserId"), "appUserId"),
    };
    return payload;
  }

  // Normalized, not rejected: this envelope is quarantine-bound, but its organization is persisted, and
  // `null` means "no organization identified".
  return { type: "Unsupported", originalType: type, originalAction: action, webhookId, webhookTimestamp, organizationId: organizationId !== null && organizationId.trim().length === 0 ? null : organizationId };
}

export function verifyPayloadTimestamp(payload: ParsedWebhookPayload, nowMs: number, toleranceMs = 60_000): void {
  if (Math.abs(nowMs - payload.webhookTimestamp) > toleranceMs) {
    throw new RelayError("linear.ingress.stale_timestamp", "The webhook timestamp is outside the accepted window.", 401);
  }
}
