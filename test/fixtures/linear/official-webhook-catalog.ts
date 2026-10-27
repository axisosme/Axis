import { createHmac, randomUUID } from "node:crypto";

import { SIGNING_SECRET } from "../../helpers.js";

/**
 * Offline snapshot of Linear's published webhook model/action catalog.
 * This corpus deliberately does not replace the Axis Developer Preview corpus
 * in catalog.ts; it protects the supported ingress boundary from widening.
 */
export const LINEAR_OFFICIAL_WEBHOOK_SNAPSHOT = {
  verifiedOn: "2026-08-31",
  schemaPublishedAt: "2026-08-24T21:07:51.494Z",
  schemaSha256: "842fb723ecbf018552b8f6db8f68b39e3f535238e404e15ebe431efa58d472c1",
  docsUrl: "https://linear.app/developers/webhooks",
  schemaUrl: "https://studio.apollographql.com/public/Linear-Webhooks/variant/current/schema/reference/objects",
} as const;

const DATA_CHANGE_TYPES = [
  "Attachment", "Comment", "Customer", "CustomerNeed", "Cycle", "Document", "Initiative",
  "InitiativeUpdate", "Issue", "IssueLabel", "Project", "ProjectUpdate", "Reaction", "User",
] as const;
const DATA_CHANGE_ACTIONS = ["create", "update", "remove"] as const;
const ISSUE_SLA_ACTIONS = ["set", "highRisk", "breached"] as const;

type DataChangeType = typeof DATA_CHANGE_TYPES[number];
type DataChangeAction = typeof DATA_CHANGE_ACTIONS[number];
type IssueSlaAction = typeof ISSUE_SLA_ACTIONS[number];
type DataChangeTuple = `${DataChangeType}.${DataChangeAction}`;
type OfficialWebhookTuple = DataChangeTuple | `IssueSLA.${IssueSlaAction}` | "OAuthApp.revoked";

type CatalogClassification = "data_change" | "convenience";
type ParserExpectation =
  | { readonly disposition: "unsupported"; readonly type: "Unsupported" }
  | { readonly disposition: "accepted"; readonly type: "OAuthApp"; readonly action: "revoked" };

export interface OfficialLinearWebhookCatalogEntry {
  readonly id: OfficialWebhookTuple;
  readonly type: DataChangeType | "IssueSLA" | "OAuthApp";
  readonly action: DataChangeAction | IssueSlaAction | "revoked";
  /** Official webhook classification; distinct from Axis Developer Preview fixtures. */
  readonly classification: CatalogClassification;
  readonly parser: ParserExpectation;
  readonly relay: {
    readonly effect: "quarantine" | "oauth_revocation";
    readonly deliveryState: "quarantined" | "completed";
  };
}

export interface OfficialLinearWebhookCase {
  readonly entry: OfficialLinearWebhookCatalogEntry;
  readonly payload: Record<string, unknown>;
  readonly rawBody: Buffer;
  readonly headers: {
    readonly deliveryId: string;
    readonly event: string;
    readonly signature: string;
    readonly timestamp: string;
  };
}

type OfficialFactory = (timestamp?: number) => OfficialLinearWebhookCase;

function syntheticModel(type: DataChangeType): Record<string, unknown> {
  const id = randomUUID();
  switch (type) {
    case "Attachment": return { id, title: "Synthetic attachment", url: "https://example.invalid/attachment", issue: { id: randomUUID() } };
    case "Comment": return { id, body: "Synthetic comment", issue: { id: randomUUID() }, user: { id: randomUUID() } };
    case "Customer": return { id, name: "Synthetic customer", domains: ["example.invalid"] };
    case "CustomerNeed": return { id, body: "Synthetic customer need", customer: { id: randomUUID() } };
    case "Cycle": return { id, name: "Synthetic cycle", number: 42, team: { id: randomUUID() } };
    case "Document": return { id, title: "Synthetic document", url: "https://linear.app/document/synthetic" };
    case "Initiative": return { id, name: "Synthetic initiative", description: "Offline fixture" };
    case "InitiativeUpdate": return { id, body: "Synthetic initiative update", initiative: { id: randomUUID() } };
    case "Issue": return { id, identifier: "SYN-42", title: "Synthetic issue", team: { id: randomUUID() } };
    case "IssueLabel": return { id, name: "synthetic-label", color: "#5E6AD2" };
    case "Project": return { id, name: "Synthetic project", status: { id: randomUUID(), name: "Planned" } };
    case "ProjectUpdate": return { id, body: "Synthetic project update", project: { id: randomUUID() } };
    case "Reaction": return { id, emoji: "eyes", comment: { id: randomUUID() }, user: { id: randomUUID() } };
    case "User": return { id, name: "Synthetic user", email: "synthetic-user@example.invalid", active: true };
  }
}

function entry(
  type: OfficialLinearWebhookCatalogEntry["type"],
  action: OfficialLinearWebhookCatalogEntry["action"],
  classification: CatalogClassification,
): OfficialLinearWebhookCatalogEntry {
  const id = `${type}.${action}` as OfficialWebhookTuple;
  return type === "OAuthApp"
    ? { id, type, action: "revoked", classification, parser: { disposition: "accepted", type: "OAuthApp", action: "revoked" }, relay: { effect: "oauth_revocation", deliveryState: "completed" } }
    : { id, type, action, classification, parser: { disposition: "unsupported", type: "Unsupported" }, relay: { effect: "quarantine", deliveryState: "quarantined" } };
}

/**
 * Build either official data-change or Agent-category cases while preserving the input
 * entry type. Official catalog callers infer OfficialLinearWebhookCase.
 */
function makeCase<TEntry extends { readonly type: string; readonly action: string }>(
  catalogEntry: TEntry,
  payload: Record<string, unknown>,
  timestamp = Date.now(),
): { readonly entry: TEntry; readonly payload: Record<string, unknown>; readonly rawBody: Buffer; readonly headers: OfficialLinearWebhookCase["headers"] } {
  const body = {
    actor: { id: randomUUID(), name: "Synthetic webhook actor" },
    createdAt: new Date(timestamp).toISOString(),
    url: `https://example.invalid/linear/webhooks/${catalogEntry.type.toLowerCase()}/${catalogEntry.action}`,
    webhookTimestamp: timestamp,
    webhookId: randomUUID(),
    organizationId: catalogEntry.type === "OAuthApp" ? "organization-fixture" : "official-organization-fixture",
    type: catalogEntry.type,
    action: catalogEntry.action,
    ...payload,
  };
  const rawBody = Buffer.from(JSON.stringify(body));
  return {
    entry: catalogEntry,
    payload: body,
    rawBody,
    headers: {
      deliveryId: randomUUID(),
      event: catalogEntry.type,
      signature: createHmac("sha256", SIGNING_SECRET).update(rawBody).digest("hex"),
      timestamp: String(timestamp),
    },
  };
}

function dataChange(type: DataChangeType, action: DataChangeAction, timestamp?: number): OfficialLinearWebhookCase {
  return makeCase(entry(type, action, "data_change"), {
    data: syntheticModel(type),
    ...(action === "update" ? { updatedFrom: { id: randomUUID(), title: "Synthetic previous value" } } : {}),
  }, timestamp);
}

function issueSla(action: IssueSlaAction, timestamp?: number): OfficialLinearWebhookCase {
  return makeCase(entry("IssueSLA", action, "convenience"), {
    issueData: { id: randomUUID(), issue: { id: randomUUID(), identifier: "SYN-42" }, status: action, updatedAt: new Date(timestamp ?? Date.now()).toISOString() },
  }, timestamp);
}

function oauthRevoked(timestamp?: number): OfficialLinearWebhookCase {
  // appUserId is an Axis local extension: existing revocation handling needs the installed App User.
  return makeCase(entry("OAuthApp", "revoked", "convenience"), {
    oauthClientId: randomUUID(),
    appUserId: "app-user-fixture",
  }, timestamp);
}

const OFFICIAL_FACTORIES = {
  "Attachment.create": (timestamp) => dataChange("Attachment", "create", timestamp), "Attachment.update": (timestamp) => dataChange("Attachment", "update", timestamp), "Attachment.remove": (timestamp) => dataChange("Attachment", "remove", timestamp),
  "Comment.create": (timestamp) => dataChange("Comment", "create", timestamp), "Comment.update": (timestamp) => dataChange("Comment", "update", timestamp), "Comment.remove": (timestamp) => dataChange("Comment", "remove", timestamp),
  "Customer.create": (timestamp) => dataChange("Customer", "create", timestamp), "Customer.update": (timestamp) => dataChange("Customer", "update", timestamp), "Customer.remove": (timestamp) => dataChange("Customer", "remove", timestamp),
  "CustomerNeed.create": (timestamp) => dataChange("CustomerNeed", "create", timestamp), "CustomerNeed.update": (timestamp) => dataChange("CustomerNeed", "update", timestamp), "CustomerNeed.remove": (timestamp) => dataChange("CustomerNeed", "remove", timestamp),
  "Cycle.create": (timestamp) => dataChange("Cycle", "create", timestamp), "Cycle.update": (timestamp) => dataChange("Cycle", "update", timestamp), "Cycle.remove": (timestamp) => dataChange("Cycle", "remove", timestamp),
  "Document.create": (timestamp) => dataChange("Document", "create", timestamp), "Document.update": (timestamp) => dataChange("Document", "update", timestamp), "Document.remove": (timestamp) => dataChange("Document", "remove", timestamp),
  "Initiative.create": (timestamp) => dataChange("Initiative", "create", timestamp), "Initiative.update": (timestamp) => dataChange("Initiative", "update", timestamp), "Initiative.remove": (timestamp) => dataChange("Initiative", "remove", timestamp),
  "InitiativeUpdate.create": (timestamp) => dataChange("InitiativeUpdate", "create", timestamp), "InitiativeUpdate.update": (timestamp) => dataChange("InitiativeUpdate", "update", timestamp), "InitiativeUpdate.remove": (timestamp) => dataChange("InitiativeUpdate", "remove", timestamp),
  "Issue.create": (timestamp) => dataChange("Issue", "create", timestamp), "Issue.update": (timestamp) => dataChange("Issue", "update", timestamp), "Issue.remove": (timestamp) => dataChange("Issue", "remove", timestamp),
  "IssueLabel.create": (timestamp) => dataChange("IssueLabel", "create", timestamp), "IssueLabel.update": (timestamp) => dataChange("IssueLabel", "update", timestamp), "IssueLabel.remove": (timestamp) => dataChange("IssueLabel", "remove", timestamp),
  "Project.create": (timestamp) => dataChange("Project", "create", timestamp), "Project.update": (timestamp) => dataChange("Project", "update", timestamp), "Project.remove": (timestamp) => dataChange("Project", "remove", timestamp),
  "ProjectUpdate.create": (timestamp) => dataChange("ProjectUpdate", "create", timestamp), "ProjectUpdate.update": (timestamp) => dataChange("ProjectUpdate", "update", timestamp), "ProjectUpdate.remove": (timestamp) => dataChange("ProjectUpdate", "remove", timestamp),
  "Reaction.create": (timestamp) => dataChange("Reaction", "create", timestamp), "Reaction.update": (timestamp) => dataChange("Reaction", "update", timestamp), "Reaction.remove": (timestamp) => dataChange("Reaction", "remove", timestamp),
  "User.create": (timestamp) => dataChange("User", "create", timestamp), "User.update": (timestamp) => dataChange("User", "update", timestamp), "User.remove": (timestamp) => dataChange("User", "remove", timestamp),
  "IssueSLA.set": (timestamp) => issueSla("set", timestamp), "IssueSLA.highRisk": (timestamp) => issueSla("highRisk", timestamp), "IssueSLA.breached": (timestamp) => issueSla("breached", timestamp),
  "OAuthApp.revoked": oauthRevoked,
} satisfies Record<OfficialWebhookTuple, OfficialFactory>;

/** Intentionally independent from OFFICIAL_FACTORIES: catches missing, duplicate, and extra tuples. */
const EXPECTED_OFFICIAL_TUPLES = [
  "Attachment.create", "Attachment.update", "Attachment.remove", "Comment.create", "Comment.update", "Comment.remove",
  "Customer.create", "Customer.update", "Customer.remove", "CustomerNeed.create", "CustomerNeed.update", "CustomerNeed.remove",
  "Cycle.create", "Cycle.update", "Cycle.remove", "Document.create", "Document.update", "Document.remove",
  "Initiative.create", "Initiative.update", "Initiative.remove", "InitiativeUpdate.create", "InitiativeUpdate.update", "InitiativeUpdate.remove",
  "Issue.create", "Issue.update", "Issue.remove", "IssueLabel.create", "IssueLabel.update", "IssueLabel.remove",
  "Project.create", "Project.update", "Project.remove", "ProjectUpdate.create", "ProjectUpdate.update", "ProjectUpdate.remove",
  "Reaction.create", "Reaction.update", "Reaction.remove", "User.create", "User.update", "User.remove",
  "IssueSLA.set", "IssueSLA.highRisk", "IssueSLA.breached", "OAuthApp.revoked",
] as const satisfies readonly OfficialWebhookTuple[];

function assertOfficialCatalogContract(): void {
  const expected = new Set<string>(EXPECTED_OFFICIAL_TUPLES);
  const actual = Object.keys(OFFICIAL_FACTORIES);
  if (expected.size !== EXPECTED_OFFICIAL_TUPLES.length || actual.length !== expected.size || actual.some((tuple) => !expected.has(tuple))) {
    throw new Error("Official Linear webhook catalog must contain exactly the pinned 46 tuples.");
  }
}

assertOfficialCatalogContract();

export const OFFICIAL_LINEAR_WEBHOOK_CATALOG_IDS = EXPECTED_OFFICIAL_TUPLES;

export function officialLinearWebhookCase(id: OfficialWebhookTuple, timestamp = Date.now()): OfficialLinearWebhookCase {
  return OFFICIAL_FACTORIES[id](timestamp);
}

export function officialMalformedWebhookCases(): readonly OfficialLinearWebhookCase[] {
  const dataChange = officialLinearWebhookCase("Attachment.create");
  const issueSla = officialLinearWebhookCase("IssueSLA.set");
  const oauth = officialLinearWebhookCase("OAuthApp.revoked");
  return [
    makeCase(dataChange.entry, { ...dataChange.payload, webhookTimestamp: "not-a-timestamp" }),
    makeCase(issueSla.entry, { ...issueSla.payload, action: "" }),
    // Compatibility gap: official OAuth revocations without Axis's appUserId are malformed today.
    makeCase(oauth.entry, Object.fromEntries(Object.entries(oauth.payload).filter(([key]) => key !== "appUserId"))),
  ];
}

/**
 * Independent snapshot of Linear's Agent-category webhooks. AppUserNotification belongs
 * to the Inbox Notifications category, whose provenance differs from the 46-tuple
 * data-change corpus. Keep its counts separate from that corpus.
 *
 * The installed SDK's AppUserNotificationWebhookPayload and
 * IssueUnassignedFromYouNotificationWebhookPayload define this shape. It contains no
 * delegate field; IssueWebhookPayload treats assigneeId and delegateId separately. This
 * fixture establishes the notification shape, not whether clearing an assignee or
 * delegate triggers it.
 */
export const AGENT_CATEGORY_WEBHOOK_SNAPSHOT = {
  verifiedOn: "2026-09-22",
  docsUrl: "https://linear.app/developers/agent-best-practices",
  sdk: "@linear/sdk@95.0.0",
  sdkTypesFile: "dist/index-DkIHAmYE.d.mts",
} as const;

type AgentCategoryWebhookTuple = "AppUserNotification.issueUnassignedFromYou";

interface AgentCategoryWebhookCatalogEntry {
  readonly id: AgentCategoryWebhookTuple;
  readonly type: "AppUserNotification";
  readonly action: "issueUnassignedFromYou";
}

export interface AgentCategoryWebhookCase {
  readonly entry: AgentCategoryWebhookCatalogEntry;
  readonly payload: Record<string, unknown>;
  readonly rawBody: Buffer;
  readonly headers: OfficialLinearWebhookCase["headers"];
}

function issueUnassignedFromYou(timestamp = Date.now()): AgentCategoryWebhookCase {
  const issueId = randomUUID();
  const teamId = randomUUID();
  const nowIso = new Date(timestamp).toISOString();
  return makeCase<AgentCategoryWebhookCatalogEntry>(
    { id: "AppUserNotification.issueUnassignedFromYou", type: "AppUserNotification", action: "issueUnassignedFromYou" },
    {
      // appUserId/oauthClientId: real `AppUserNotificationWebhookPayload` fields
      // (required, non-null in the SDK schema), not a local Axis extension --
      // unlike `oauthRevoked`'s appUserId above.
      appUserId: "app-user-fixture",
      oauthClientId: randomUUID(),
      notification: {
        type: "issueUnassignedFromYou",
        id: randomUUID(),
        actorId: randomUUID(),
        issueId,
        // `issue` here is Axis fixture convenience for a readable payload, not
        // a field this parser reads: an Unsupported envelope is quarantined on
        // type/action alone, before any nested field is inspected.
        issue: { id: issueId, identifier: "SYN-42", title: "Synthetic issue", teamId, team: { id: teamId, key: "SYN", name: "Synthetic team" } },
        userId: randomUUID(),
        createdAt: nowIso,
        updatedAt: nowIso,
      },
    },
    timestamp,
  );
}

const AGENT_CATEGORY_FACTORIES = {
  "AppUserNotification.issueUnassignedFromYou": issueUnassignedFromYou,
} satisfies Record<AgentCategoryWebhookTuple, (timestamp?: number) => AgentCategoryWebhookCase>;

/**
 * `webhookId`/`webhookTimestamp` are written here because the SDK schema
 * declares both required and non-null on `AppUserNotificationWebhookPayload`
 * -- but Linear's own Inbox Notifications documentation example for this
 * envelope does not list either field, and `parseLinearWebhook` requires both
 * unconditionally (for every shape, including `Unsupported`). That
 * documentation/schema discrepancy is unverified against real Linear
 * traffic; this fixture follows the SDK schema so the delivery reaches the
 * parser instead of being rejected pre-parse by `Relay#acceptWebhook`.
 */
export function agentCategoryWebhookCase(id: AgentCategoryWebhookTuple, timestamp = Date.now()): AgentCategoryWebhookCase {
  return AGENT_CATEGORY_FACTORIES[id](timestamp);
}
