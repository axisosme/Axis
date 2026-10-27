export type InboxState = "accepted" | "processing" | "completed" | "quarantined" | "failed";
export type OutboxState = "pending" | "delivering" | "delivered" | "confirmed_failure" | "ambiguous" | "conflict";

export interface LinearHeaders {
  readonly deliveryId: string;
  readonly event: string;
  readonly signature: string;
  readonly timestamp: string;
}

export interface AgentSessionSnapshot {
  readonly id: string;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly issueId: string;
  readonly issueTeamId: string;
  readonly creatorId: string;
  readonly status: string;
  /** Linear provenance for AgentSessions created from an issue comment. */
  readonly commentId?: string | null;
  /** Canonical source comment, when Linear supplies it. */
  readonly sourceCommentId?: string | null;
}

export interface AgentActivitySnapshot {
  readonly id: string;
  readonly userId: string | null;
  /** Official AgentActivity prompt content, when the webhook supplies it. */
  readonly promptBody?: string | null;
  /** Activity preview text; `promptBody` is the official prompt content. */
  readonly body: string | null;
  readonly signal: "auth" | "continue" | "select" | "stop" | null;
  readonly createdAt: string | null;
  /** Canonical source comment for comment-origin prompt Activities. */
  readonly sourceCommentId?: string | null;
}

export interface LinearGuidanceTeam {
  readonly id: string;
  readonly key: string;
  readonly name: string;
  readonly displayName: string;
  readonly parentId: string | null;
}

export interface LinearGuidanceRule {
  readonly body: string;
  readonly origin:
    | { readonly type: "Organization" }
    | { readonly type: "Team"; readonly team: LinearGuidanceTeam };
}

export type LinearWebhookGuidance = readonly LinearGuidanceRule[];

export interface AgentSessionEventPayload {
  readonly type: "AgentSessionEvent";
  readonly action: "created" | "prompted";
  readonly webhookId: string;
  readonly webhookTimestamp: number;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly agentSession: AgentSessionSnapshot;
  readonly agentActivity: AgentActivitySnapshot | null;
  readonly promptContext: string | null;
  readonly guidance: LinearWebhookGuidance | null;
}

export interface PermissionChangePayload {
  readonly type: "PermissionChange";
  readonly action: "teamAccessChanged";
  readonly webhookId: string;
  readonly webhookTimestamp: number;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly addedTeamIds: readonly string[];
  readonly removedTeamIds: readonly string[];
  readonly canAccessAllPublicTeams: boolean;
}

export interface OAuthRevocationPayload {
  readonly type: "OAuthApp";
  readonly action: "revoked";
  readonly webhookId: string;
  readonly webhookTimestamp: number;
  readonly organizationId: string;
  readonly appUserId: string;
}

export type SupportedWebhookPayload = AgentSessionEventPayload | PermissionChangePayload | OAuthRevocationPayload;

export type LinearActivityContent =
  | { readonly type: "thought"; readonly body: string }
  | { readonly type: "elicitation"; readonly body: string }
  | { readonly type: "action"; readonly action: string; readonly parameter: string; readonly result?: string }
  | { readonly type: "response"; readonly body: string }
  | { readonly type: "error"; readonly body: string };

export interface OutboxRecord {
  readonly id: string;
  readonly workspaceId: string;
  readonly agentSessionId: string;
  readonly sourceEventId: string;
  readonly content: LinearActivityContent;
  readonly contentDigest: string;
  readonly state: OutboxState;
  readonly attemptCount: number;
  readonly availableAt: string;
  readonly createdAt: string;
}

export interface CredentialVault {
  readonly ready: boolean;
  put(reference: string, value: string): Promise<void>;
  get(reference: string): Promise<string | null>;
  delete(reference: string): Promise<void>;
}
