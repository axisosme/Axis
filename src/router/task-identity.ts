/** Canonical Router task identity and its persisted Linear columns. */
import type { DatabaseSync } from "node:sqlite";

import type { ResolvedRoutingScope } from "./binding-resolver.js";
import type { ChannelSourceRef, System } from "./channel-identity.js";

export const ROUTER_TASK_IDENTITY_CONTRACT_VERSION = "1.0.0" as const;

export type RouterTaskIdentityErrorCode =
  /** A supplied or stored identity value is not a usable persisted value. */
  | "malformed"
  /** The stored `router_tasks` shape is not the current one. */
  | "schema_drift"
  /** A stored row's neutral identity disagrees with its Linear columns. */
  | "identity_drift";

const errorMessages: Readonly<Record<RouterTaskIdentityErrorCode, string>> = {
  malformed: "The Router task identity value is malformed.",
  schema_drift: "The persisted router_tasks shape is not a recognized Router schema.",
  identity_drift: "A persisted Router task identity disagrees with its Linear columns.",
};

export class RouterTaskIdentityError extends Error {
  constructor(readonly code: RouterTaskIdentityErrorCode, options?: ErrorOptions) { super(errorMessages[code], options); this.name = "RouterTaskIdentityError"; }
}
function fail(code: RouterTaskIdentityErrorCode): never { throw new RouterTaskIdentityError(code); }

/**
 * The neutral identity one `router_tasks` row carries.
 *
 * `scopeId` is null for a row with no recorded routing scope.
 */
export interface RouterNeutralTaskIdentity {
  readonly system: System;
  readonly tenantId: string;
  readonly source: RouterPersistedSourceRef;
  readonly scopeId: string | null;
}

/**
 * The source refs this store persists: `ChannelSourceRef` with Linear's
 * `agentSessionId` required, so a source ref has one canonical spelling and one
 * unique-index entry.
 */
export type RouterPersistedSourceRef = ChannelSourceRef & { readonly agentSessionId: string };

/** The exact column values one neutral identity persists to. */
export interface RouterTaskIdentityColumns {
  readonly sourceSystem: System;
  readonly tenantId: string;
  readonly sourceRefJson: string;
  readonly routingScopeId: string | null;
}

/** One persisted task row's identity, with its Linear columns (absent for a non-Linear row). */
export interface RouterPersistedTaskIdentity {
  readonly contractVersion: typeof ROUTER_TASK_IDENTITY_CONTRACT_VERSION;
  readonly taskId: string;
  readonly identity: RouterNeutralTaskIdentity;
  readonly linearColumns: { readonly organizationId: string; readonly issueId: string; readonly agentSessionId: string; readonly linearTeamId: string | null } | null;
}

export interface RouterTaskIdentityVerification {
  readonly contractVersion: typeof ROUTER_TASK_IDENTITY_CONTRACT_VERSION;
  readonly rows: number;
  readonly linearRows: number;
  /** Linear rows whose neutral identity re-encodes to exactly their Linear columns. */
  readonly linearRoundTrips: number;
}

/**
 * The objects this module owns, checked on every open. The `task_id` primary key's
 * `sqlite_autoindex_router_tasks_1` is excluded by the `router_tasks%`
 * name filter.
 */
export const ROUTER_TASK_IDENTITY_SCHEMA = Object.freeze({
  "table:router_tasks": "CREATE TABLE router_tasks(task_id TEXT PRIMARY KEY, organization_id TEXT, issue_id TEXT, agent_session_id TEXT, linear_team_id TEXT, attempt_id TEXT NOT NULL, correlation_id TEXT NOT NULL, operation_id TEXT NOT NULL, task_json TEXT NOT NULL, target_snapshot_json TEXT, current_run_id TEXT, cursor INTEGER NOT NULL DEFAULT 0, source_system TEXT NOT NULL DEFAULT 'linear' CHECK(source_system IN ('linear')), tenant_id TEXT, source_ref_json TEXT, routing_scope_id TEXT, CHECK(organization_id IS NOT NULL AND issue_id IS NOT NULL AND agent_session_id IS NOT NULL))",
  "index:router_tasks_linear_identity": "CREATE UNIQUE INDEX router_tasks_linear_identity ON router_tasks(organization_id, issue_id, agent_session_id) WHERE source_system = 'linear'",
  "index:router_tasks_neutral_identity": "CREATE UNIQUE INDEX router_tasks_neutral_identity ON router_tasks(source_system, tenant_id, source_ref_json)",
});

/** Reproduces `canonicalSql` in `./router.ts`; sqlite_master returns the SQL as written, so both spellings must normalize identically. */
function canonicalSql(value: string | null): string { if (value === null) fail("schema_drift"); return value.replace(/\s+/gu, " ").replace(/\s*([(),])/gu, "$1").trim(); }
const taskIdentityFingerprints = Object.freeze(Object.fromEntries(Object.entries(ROUTER_TASK_IDENTITY_SCHEMA).map(([name, sql]) => [name, canonicalSql(sql)])));

/** Reproduces `canonical` in `./router.ts` and `canonicalize` in `./binding-resolver.ts`: sorted keys, no whitespace. */
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") { const object = value as Record<string, unknown>; return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonical(object[key])}`).join(",")}}`; }
  return JSON.stringify(value);
}

/**
 * Mirrors `text()` in `./router.ts`: any string with a non-whitespace character,
 * preserved verbatim.
 */
function persistedValue(value: unknown): string { if (typeof value !== "string" || value.trim().length === 0) fail("malformed"); return value; }
function optionalPersistedValue(value: unknown): string | null { return value === null || value === undefined ? null : persistedValue(value); }
function persistedSystem(value: unknown): System { if (value !== "linear") fail("malformed"); return value; }

/** Closed key-set validation of a source ref, per system. An unknown or missing key is rejected, never ignored. */
function persistedSourceRef(value: unknown): RouterPersistedSourceRef {
  const candidate = value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
  if (candidate === undefined) fail("malformed");
  const system = persistedSystem(candidate.system), keys = Object.keys(candidate).sort().join(",");
  if (keys !== "agentSessionId,issueId,system") fail("malformed");
  return Object.freeze({ system, issueId: persistedValue(candidate.issueId), agentSessionId: persistedValue(candidate.agentSessionId) });
}

/**
 * A Linear `RouterTaskRef` under the neutral taxonomy. `teamId` becomes the routing
 * scope and is not duplicated into the source ref.
 */
export function linearTaskIdentity(ref: { readonly organizationId: string; readonly issueId: string; readonly agentSessionId: string; readonly teamId?: string | null }): RouterNeutralTaskIdentity {
  return Object.freeze({ system: "linear" as const, tenantId: persistedValue(ref.organizationId), source: Object.freeze({ system: "linear" as const, issueId: persistedValue(ref.issueId), agentSessionId: persistedValue(ref.agentSessionId) }), scopeId: optionalPersistedValue(ref.teamId) });
}

export function encodeRouterTaskIdentity(identity: RouterNeutralTaskIdentity): RouterTaskIdentityColumns {
  const source = persistedSourceRef(identity.source), system = persistedSystem(identity.system);
  if (source.system !== system) fail("malformed");
  return Object.freeze({ sourceSystem: system, tenantId: persistedValue(identity.tenantId), sourceRefJson: canonical(source), routingScopeId: optionalPersistedValue(identity.scopeId) });
}

function decodeRouterTaskIdentity(columns: { readonly sourceSystem: unknown; readonly tenantId: unknown; readonly sourceRefJson: unknown; readonly routingScopeId: unknown }): RouterNeutralTaskIdentity {
  const system = persistedSystem(columns.sourceSystem), raw = persistedValue(columns.sourceRefJson);
  let parsed: unknown; try { parsed = JSON.parse(raw); } catch (error) { throw new RouterTaskIdentityError("malformed", { cause: error }); }
  const source = persistedSourceRef(parsed);
  // Canonical form is part of the stored key: a row whose JSON re-serializes
  // differently would sit under a unique index entry no lookup can reproduce.
  if (source.system !== system || canonical(source) !== raw) fail("malformed");
  return Object.freeze({ system, tenantId: persistedValue(columns.tenantId), source, scopeId: optionalPersistedValue(columns.routingScopeId) });
}

/** The `ResolvedRoutingScope` for a task identity, or `null` for a row that never recorded a scope. */
export function routerTaskIdentityScope(identity: RouterNeutralTaskIdentity): ResolvedRoutingScope | null {
  return identity.scopeId === null ? null : Object.freeze({ system: identity.system, tenantId: identity.tenantId, scopeId: identity.scopeId });
}

type Row = Record<string, unknown>;
const objectSql = (db: DatabaseSync): Readonly<Record<string, string>> => Object.fromEntries((db.prepare("SELECT type, name, sql FROM sqlite_master WHERE name LIKE 'router_tasks%'").all() as Row[]).map((object) => [`${String(object.type)}:${String(object.name)}`, canonicalSql(object.sql as string | null)]));

function transaction<T>(db: DatabaseSync, mode: "BEGIN" | "BEGIN IMMEDIATE", action: () => T): T {
  db.exec(mode);
  try { const value = action(); db.exec("COMMIT"); return value; }
  catch (error) { try { db.exec("ROLLBACK"); } catch { /* the transaction is already gone */ } throw error; }
}

/** Decodes every row's neutral identity and checks each Linear row re-encodes to its Linear columns. */
export function verifyRouterTaskIdentity(db: DatabaseSync): RouterTaskIdentityVerification {
  const rows = db.prepare("SELECT task_id, organization_id, issue_id, agent_session_id, linear_team_id, source_system, tenant_id, source_ref_json, routing_scope_id FROM router_tasks ORDER BY task_id").all() as Row[];
  let linearRows = 0, linearRoundTrips = 0;
  for (const row of rows) {
    const identity = decodeRouterTaskIdentity({ sourceSystem: row.source_system, tenantId: row.tenant_id, sourceRefJson: row.source_ref_json, routingScopeId: row.routing_scope_id });
    linearRows += 1;
    const linearColumns = encodeRouterTaskIdentity(linearTaskIdentity({ organizationId: persistedValue(row.organization_id), issueId: persistedValue(row.issue_id), agentSessionId: persistedValue(row.agent_session_id), teamId: row.linear_team_id as string | null }));
    // Tenant and source ref are the unique-index key and must match exactly; the routing
    // scope is compared only while `linear_team_id` records a Team.
    if (linearColumns.tenantId !== identity.tenantId || linearColumns.sourceRefJson !== row.source_ref_json) fail("identity_drift");
    if (linearColumns.routingScopeId !== null && linearColumns.routingScopeId !== identity.scopeId) fail("identity_drift");
    linearRoundTrips += 1;
  }
  return Object.freeze({ contractVersion: ROUTER_TASK_IDENTITY_CONTRACT_VERSION, rows: rows.length, linearRows, linearRoundTrips });
}

/**
 * The on-open gate: the owned objects are exactly the current schema, and every
 * row verifies. One drifted row refuses the whole Router.
 */
export function assertRouterTaskIdentity(db: DatabaseSync): RouterTaskIdentityVerification {
  return transaction(db, "BEGIN", () => {
    const installed = objectSql(db);
    if (Object.keys(installed).length !== Object.keys(taskIdentityFingerprints).length || Object.entries(taskIdentityFingerprints).some(([name, sql]) => installed[name] !== sql)) fail("schema_drift");
    return verifyRouterTaskIdentity(db);
  });
}

/** One task's persisted identity, read by Router's own task id. Read-only: it resolves no binding and reaches no endpoint. */
export function readRouterTaskIdentity(db: DatabaseSync, taskId: string): RouterPersistedTaskIdentity | null {
  return persistedIdentity(db.prepare("SELECT task_id, organization_id, issue_id, agent_session_id, linear_team_id, source_system, tenant_id, source_ref_json, routing_scope_id FROM router_tasks WHERE task_id = ?").get(persistedValue(taskId)) as Row | undefined);
}

export function resolveRouterTaskIdentity(db: DatabaseSync, identity: RouterNeutralTaskIdentity): RouterPersistedTaskIdentity | null {
  const columns = encodeRouterTaskIdentity(identity);
  return persistedIdentity(db.prepare("SELECT task_id, organization_id, issue_id, agent_session_id, linear_team_id, source_system, tenant_id, source_ref_json, routing_scope_id FROM router_tasks WHERE source_system = ? AND tenant_id = ? AND source_ref_json = ?").get(columns.sourceSystem, columns.tenantId, columns.sourceRefJson) as Row | undefined);
}

function persistedIdentity(row: Row | undefined): RouterPersistedTaskIdentity | null {
  if (row === undefined) return null;
  const identity = decodeRouterTaskIdentity({ sourceSystem: row.source_system, tenantId: row.tenant_id, sourceRefJson: row.source_ref_json, routingScopeId: row.routing_scope_id });
  const linearColumns = Object.freeze({ organizationId: persistedValue(row.organization_id), issueId: persistedValue(row.issue_id), agentSessionId: persistedValue(row.agent_session_id), linearTeamId: optionalPersistedValue(row.linear_team_id) });
  return Object.freeze({ contractVersion: ROUTER_TASK_IDENTITY_CONTRACT_VERSION, taskId: persistedValue(row.task_id), identity, linearColumns });
}
