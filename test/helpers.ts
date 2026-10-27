import assert from "node:assert/strict";
import { createHmac, randomBytes, randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { ClaimedLinearDelivery, LinearInboxPort } from "../src/relay/linear/inbox.js";
import type { ImmutableLinearComment, ImmutableLinearIssue, LinearNativeSignal } from "../src/relay/linear/provider.js";
import { RelayError } from "../src/relay/errors.js";
import { LINEAR_CHANNEL_STORE_NAMES, type LinearChannelDiagnostics, type LinearChannelStoreName } from "../src/relay/linear/composition.js";

import type {
  CredentialVault,
  ImmutableLinearActivity,
  IssueExecutionContext,
  LinearActivityContent,
  LinearCredentialAccess,
  LinearHeaders,
  LinearProviderPort,
  OutboxRecord,
  ProviderCreateResult,
  ProviderReconcileResult,
} from "../src/index.js";

export const SIGNING_SECRET = "fixture-signing-secret";

export function fixture(name: string): Record<string, unknown> {
  return JSON.parse(readFileSync(join(process.cwd(), "test", "fixtures", "linear", name), "utf8")) as Record<string, unknown>;
}

export function currentFixture(name: string, overrides: Readonly<Record<string, unknown>> = {}): Record<string, unknown> {
  return { ...fixture(name), webhookTimestamp: Date.now(), webhookId: randomUUID(), ...overrides };
}

export function signed(body: Buffer, deliveryId: string = randomUUID(), event = "AgentSessionEvent"): LinearHeaders {
  return {
    deliveryId,
    event,
    signature: createHmac("sha256", SIGNING_SECRET).update(body).digest("hex"),
    timestamp: String(Date.now()),
  };
}

export function temporaryDatabase(): { readonly filename: string; cleanup(): void } {
  const directory = mkdtempSync(join(tmpdir(), "axis-linear-relay-test-"));
  return { filename: join(directory, "relay.sqlite"), cleanup: () => rmSync(directory, { recursive: true, force: true }) };
}

export function masterKey(): string {
  return randomBytes(32).toString("base64");
}

/**
 * Fixed diagnostics for fixtures that supply no composed channel to diagnose. Derive
 * the store names from LINEAR_CHANNEL_STORE_NAMES so this wiring fixture stays aligned
 * with the composition. Tests of diagnostics supply their own answer and independent
 * expected store map.
 */
export const HEALTHY_DIAGNOSTICS_FIXTURE: LinearChannelDiagnostics = Object.freeze({
  healthy: true,
  vault: true,
  stores: Object.freeze(
    Object.fromEntries(LINEAR_CHANNEL_STORE_NAMES.map((name) => [name, true])) as Record<LinearChannelStoreName, boolean>,
  ),
});

export class MemoryVault implements CredentialVault {
  ready = true;
  readonly values = new Map<string, string>();
  async put(reference: string, value: string): Promise<void> { this.values.set(reference, value); }
  async get(reference: string): Promise<string | null> { return this.values.get(reference) ?? null; }
  async delete(reference: string): Promise<void> { this.values.delete(reference); }
}

export class FakeCredentials implements LinearCredentialAccess {
  ready = true;
  readonly deleted: string[] = [];
  token: string | null = "fixture-access-token";
  async accessToken(): Promise<string | null> { return this.token; }
  async delete(reference: string): Promise<void> { this.deleted.push(reference); }
}

export class FakeLinearProvider implements LinearProviderPort {
  readonly created: OutboxRecord[] = [];
  readonly issueUpdates: { context: IssueExecutionContext; appUserId: string }[] = [];
  issueContextCalls = 0;
  issueContextError: Error | null = null;
  issueLabels: string[] = [];
  readonly activities: ImmutableLinearActivity[] = [];
  exactActivity: { readonly agentSessionId: string; readonly activity: ImmutableLinearActivity } | null = null;
  exactActivityCalls = 0;
  exactComment: ImmutableLinearComment | null = null;
  exactCommentCalls = 0;
  exactCommentFailures = 0;
  exactUserCalls = 0;
  exactUserFailures = 0;
  exactIssue: ImmutableLinearIssue = {
    id: "issue-fixture", teamId: "team-fixture", identifier: "DEMO-9", title: "Build the relay", description: "Implement the exact relay input.",
  };
  readonly exactIssues = new Map<string, ImmutableLinearIssue>();
  exactIssueCalls = 0;
  exactIssueFailures = 0;
  createResults: ProviderCreateResult[] = [];
  reconcileResult: ProviderReconcileResult = { disposition: "unknown" };

  /**
   * Record native options along with the Activity. Decision reply tokens are carried in
   * those options, so a double that drops them could report an elicitation that the
   * user cannot answer.
   */
  readonly createdNative: (LinearNativeSignal | null)[] = [];

  async createAgentActivity(_token: string, record: OutboxRecord, native: LinearNativeSignal | null): Promise<ProviderCreateResult> {
    this.created.push(record);
    this.createdNative.push(native);
    return this.createResults.shift() ?? { disposition: "confirmed", activityId: record.id };
  }
  async reconcileAgentActivity(_token: string, record: OutboxRecord): Promise<ProviderReconcileResult> {
    if (this.reconcileResult.disposition === "confirmed") return { disposition: "confirmed", activityId: record.id };
    return this.reconcileResult;
  }
  async loadSessionActivities(): Promise<readonly ImmutableLinearActivity[]> { return this.activities; }
  async loadExactAgentActivity(_token: string, activityId: string): Promise<{ readonly agentSessionId: string; readonly activity: ImmutableLinearActivity }> {
    this.exactActivityCalls += 1;
    if (this.exactActivity !== null) return this.exactActivity;
    const activity = this.activities.find((candidate) => candidate.id === activityId);
    if (activity === undefined) throw new Error("linear.provider.activity_unavailable");
    return { agentSessionId: "agent-session-fixture", activity };
  }
  async loadExactUser(_token: string, userId: string) {
    this.exactUserCalls += 1;
    if (this.exactUserFailures > 0) {
      this.exactUserFailures -= 1;
      throw new RelayError("linear.provider.rejected", "temporary exact-user fixture failure", 503, true);
    }
    return { id: userId, active: true, app: false, url: `https://linear.app/acme/profiles/${userId}` };
  }
  async loadExactComment(_token: string, commentId: string): Promise<ImmutableLinearComment> {
    this.exactCommentCalls += 1;
    if (this.exactCommentFailures > 0) {
      this.exactCommentFailures -= 1;
      throw new RelayError("linear.provider.rejected", "temporary exact-comment fixture failure", 503, true);
    }
    if (this.exactComment === null || this.exactComment.id !== commentId) throw new Error("linear.provider.comment_unavailable");
    return this.exactComment;
  }
  async loadExactIssue(_token: string, issueId: string): Promise<ImmutableLinearIssue> {
    this.exactIssueCalls += 1;
    if (this.exactIssueFailures > 0) {
      this.exactIssueFailures -= 1;
      throw new RelayError("linear.provider.rejected", "temporary exact-issue fixture failure", 503, true);
    }
    const issue = this.exactIssues.get(issueId) ?? this.exactIssue;
    if (issue.id !== issueId) throw new Error("linear.provider.issue_unavailable");
    return issue;
  }
  async loadIssueExecutionContext(_token: string, issueId: string): Promise<IssueExecutionContext> {
    this.issueContextCalls += 1;
    if (this.issueContextError !== null) throw this.issueContextError;
    return {
      issueId, teamId: "team-fixture", labels: [...this.issueLabels], stateType: "unstarted", delegateId: null, firstStartedStateId: "state-started",
    };
  }
  async markIssueStartedAndDelegated(_token: string, context: IssueExecutionContext, appUserId: string): Promise<void> {
    this.issueUpdates.push({ context, appUserId });
  }
  async resolveViewer(): Promise<{ readonly appUserId: string; readonly organizationId: string }> {
    return { appUserId: "app-user-fixture", organizationId: "organization-fixture" };
  }
}

export function promptActivity(id: string = randomUUID(), body = "Continue"): ImmutableLinearActivity {
  return { id, updatedAt: new Date().toISOString(), userId: "human-user-fixture", user: { id: "human-user-fixture", active: true, app: false, url: "https://linear.app/acme/profiles/human-user-fixture" }, content: { type: "prompt", body } };
}

export function bodyFor(content: LinearActivityContent): Buffer {
  return Buffer.from(JSON.stringify(content));
}

export function reachableStrings(value: unknown, seen = new Set<unknown>()): readonly string[] {
  if (typeof value === "string") return [value];
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") return [String(value)];
  if (value === null || typeof value !== "object" || seen.has(value)) return [];
  seen.add(value);
  const children: unknown[] = [];
  if (value instanceof Map) for (const [key, item] of value) children.push(key, item);
  else if (value instanceof Set) for (const item of value) children.push(item);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined) continue;
    if ("value" in descriptor) { children.push(descriptor.value); continue; }
    // Accessors are read, not skipped. A getter is the cheapest way to expose
    // a live credential while remaining invisible to `Object.keys`,
    // `JSON.stringify` and a data-property-only walk -- so a scan that ignored
    // them would pass while the value was fully readable by any caller.
    if (descriptor.get !== undefined) { try { children.push(descriptor.get.call(value)); } catch { /* an accessor that throws exposes nothing */ } }
  }
  return children.flatMap((child) => reachableStrings(child, seen));
}

/** Every property key reachable from `value`, including symbol keys rendered as strings. */
export function reachableKeys(value: unknown, seen = new Set<unknown>()): readonly string[] {
  if (value === null || typeof value !== "object" || seen.has(value)) return [];
  seen.add(value);
  const found: string[] = [];
  // A Map key is a key: `{ held: new Map([["installationId", x]]) }` must be
  // reported, not only its value.
  if (value instanceof Map) for (const [key, item] of value) { if (typeof key === "string") found.push(key); found.push(...reachableKeys(item, seen)); }
  if (value instanceof Set) for (const item of value) found.push(...reachableKeys(item, seen));
  for (const key of Reflect.ownKeys(value)) {
    found.push(typeof key === "symbol" ? key.toString() : key);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined) continue;
    if ("value" in descriptor) found.push(...reachableKeys(descriptor.value, seen));
    else if (descriptor.get !== undefined) { try { found.push(...reachableKeys(descriptor.get.call(value), seen)); } catch { /* ignore */ } }
  }
  return found;
}

/**
 * Claim the next inbox delivery and assert that it is work, not a retry-budget give-up
 * obligation. Callers expect a processable delivery; an exhausted budget fails here
 * with that distinction rather than producing undefined downstream.
 */
export function claimInboxWork(inbox: Pick<LinearInboxPort, "claim">, owner: string, leaseMs?: number): ClaimedLinearDelivery | null {
  const claim = leaseMs === undefined ? inbox.claim(owner) : inbox.claim(owner, leaseMs);
  if (claim === null) return null;
  assert.equal(claim.disposition, "attempt", "expected work, not a spent retry budget");
  return claim.delivery;
}
