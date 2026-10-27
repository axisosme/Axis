import { fixture, currentFixture } from "../../helpers.js";

export type LinearWebhookCatalogId =
  | "agent_session_created"
  | "agent_session_prompted"
  | "agent_session_stop"
  | "permission_change"
  | "oauth_revoked"
  | "unsupported_preview_event"
  | "malformed_missing_issue"
  | "malformed_prompt_without_activity"
  | "malformed_unknown_signal"
  | "malformed_permission_flag";

type ParsedExpectation =
  | { readonly disposition: "accepted"; readonly type: "AgentSessionEvent" | "PermissionChange" | "OAuthApp"; readonly action: string }
  | { readonly disposition: "unsupported"; readonly type: "Unsupported" }
  | { readonly disposition: "malformed"; readonly code: "linear.ingress.malformed" };

export interface LinearWebhookCatalogEntry {
  readonly id: LinearWebhookCatalogId;
  readonly fixture: string;
  readonly parser: ParsedExpectation;
  readonly relay: {
    readonly effect: "start" | "continue" | "cancel" | "permission_change" | "oauth_revocation" | "quarantine" | "rejected";
    readonly deliveryState?: "completed" | "quarantined";
  };
}

const ENTRIES: Readonly<Record<LinearWebhookCatalogId, LinearWebhookCatalogEntry>> = {
  agent_session_created: {
    id: "agent_session_created", fixture: "agent-session-created.json",
    parser: { disposition: "accepted", type: "AgentSessionEvent", action: "created" },
    relay: { effect: "start", deliveryState: "completed" },
  },
  agent_session_prompted: {
    id: "agent_session_prompted", fixture: "agent-session-prompted.json",
    parser: { disposition: "accepted", type: "AgentSessionEvent", action: "prompted" },
    relay: { effect: "continue", deliveryState: "completed" },
  },
  agent_session_stop: {
    id: "agent_session_stop", fixture: "agent-session-stop.json",
    parser: { disposition: "accepted", type: "AgentSessionEvent", action: "prompted" },
    relay: { effect: "cancel", deliveryState: "completed" },
  },
  permission_change: {
    id: "permission_change", fixture: "permission-change.json",
    parser: { disposition: "accepted", type: "PermissionChange", action: "teamAccessChanged" },
    relay: { effect: "permission_change", deliveryState: "completed" },
  },
  oauth_revoked: {
    id: "oauth_revoked", fixture: "oauth-revoked.json",
    parser: { disposition: "accepted", type: "OAuthApp", action: "revoked" },
    relay: { effect: "oauth_revocation", deliveryState: "completed" },
  },
  unsupported_preview_event: {
    id: "unsupported_preview_event", fixture: "unsupported-preview-event.json",
    parser: { disposition: "unsupported", type: "Unsupported" },
    relay: { effect: "quarantine", deliveryState: "quarantined" },
  },
  malformed_missing_issue: {
    id: "malformed_missing_issue", fixture: "agent-session-created.json",
    parser: { disposition: "malformed", code: "linear.ingress.malformed" },
    relay: { effect: "rejected" },
  },
  malformed_prompt_without_activity: {
    id: "malformed_prompt_without_activity", fixture: "agent-session-prompted.json",
    parser: { disposition: "malformed", code: "linear.ingress.malformed" },
    relay: { effect: "rejected" },
  },
  malformed_unknown_signal: {
    id: "malformed_unknown_signal", fixture: "agent-session-prompted.json",
    parser: { disposition: "malformed", code: "linear.ingress.malformed" },
    relay: { effect: "rejected" },
  },
  malformed_permission_flag: {
    id: "malformed_permission_flag", fixture: "permission-change.json",
    parser: { disposition: "malformed", code: "linear.ingress.malformed" },
    relay: { effect: "rejected" },
  },
};

export const LINEAR_WEBHOOK_CATALOG_IDS = Object.keys(ENTRIES) as LinearWebhookCatalogId[];

/**
 * The only test API for the pinned Linear webhook corpus. It keeps raw fixture
 * loading, fresh delivery identity, and bounded malformed drift in one place.
 */
export function linearWebhookCase(id: LinearWebhookCatalogId): {
  readonly entry: LinearWebhookCatalogEntry;
  readonly payload: Record<string, unknown>;
} {
  const entry = ENTRIES[id];
  const payload = currentFixture(entry.fixture);

  switch (id) {
    case "malformed_missing_issue":
      return { entry, payload: { ...payload, agentSession: { id: "session-without-issue" } } };
    case "malformed_prompt_without_activity":
      return { entry, payload: { ...payload, agentActivity: null } };
    case "malformed_unknown_signal":
      return {
        entry,
        payload: {
          ...payload,
          agentActivity: { ...(fixture(entry.fixture).agentActivity as Record<string, unknown>), signal: "unknown" },
        },
      };
    case "malformed_permission_flag":
      return { entry, payload: { ...payload, canAccessAllPublicTeams: "no" } };
    default:
      return { entry, payload };
  }
}
