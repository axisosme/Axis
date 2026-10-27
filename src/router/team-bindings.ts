import { createHash } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";

import { executionProfile, executionProfileDigest, executionProfileValidatorOutcome } from "./execution-profile.js";
import type { ExecutionProfile, ExecutionProfileValidator, ExecutionProfileValidatorInvalid, ExecutionProfileValidatorResult, ExecutionProfileVersionedRef } from "./execution-profile.js";
import { canonical, copy, durableStoreInput, frozen, openDurableStoreConnection, type DurableStoreConnection } from "../shared/durable-store.js";
import { schemaFingerprintTable, schemaLedgerAgrees, schemaObjectsMatch } from "../shared/schema-ledger.js";

/** Closed, durable team-to-executor binding contract. */
export const TEAM_BINDING_CONTRACT_VERSION = "1.0.0" as const;

export type TeamBindingErrorCode = "unconfigured" | "disabled" | "reference_missing" | "stale" | "incompatible" | "revision_conflict" | "persisted_state_conflict";
const errorMessages: Readonly<Record<TeamBindingErrorCode, string>> = {
  unconfigured: "Team binding unconfigured.", disabled: "Team binding disabled.", reference_missing: "Team binding reference missing.",
  stale: "Team binding stale.", incompatible: "Team binding incompatible.", revision_conflict: "Team binding revision conflict.", persisted_state_conflict: "Team binding persisted state conflict.",
};
const errorCodes = new Set<TeamBindingErrorCode>(Object.keys(errorMessages) as TeamBindingErrorCode[]);
export class TeamBindingError extends Error {
  readonly code: TeamBindingErrorCode;
  constructor(code: unknown) {
    const normalized: TeamBindingErrorCode = typeof code === "string" && errorCodes.has(code as TeamBindingErrorCode) ? code as TeamBindingErrorCode : "persisted_state_conflict";
    super(errorMessages[normalized]); this.code = normalized;
    this.name = "TeamBindingError";
  }
}

export interface TeamBindingScope { readonly organizationId: string; readonly linearTeamId: string; }
/** The channel-neutral profile shape from `./execution-profile.ts`, under this module's names. */
export type TeamBindingVersionedRef = ExecutionProfileVersionedRef;
export type { ExecutionProfile };
export interface TeamBindingMutationAudit { readonly operatorRef: string; readonly source: string; readonly requestRef: string; readonly authorizationRef: string; }
export interface TeamBindingRevision {
  readonly contractVersion: typeof TEAM_BINDING_CONTRACT_VERSION;
  readonly scope: TeamBindingScope;
  readonly revision: number;
  readonly status: "configured" | "disabled";
  readonly profile?: ExecutionProfile;
  readonly profileDigest?: string;
  readonly audit: TeamBindingMutationAudit;
  readonly createdAt: string;
}
export interface TeamBindingValidatorValid { readonly status: "valid"; }
export type TeamBindingValidatorInvalid = ExecutionProfileValidatorInvalid;
export type TeamBindingValidatorResult = ExecutionProfileValidatorResult;
export type TeamBindingValidator = ExecutionProfileValidator;
export interface TeamBindingValidation {
  readonly contractVersion: typeof TEAM_BINDING_CONTRACT_VERSION;
  readonly scope: TeamBindingScope;
  readonly revision: number;
  readonly profileDigest: string;
}
export interface TeamBindingResolution {
  readonly contractVersion: typeof TEAM_BINDING_CONTRACT_VERSION;
  readonly scope: TeamBindingScope;
  readonly revision: number;
  readonly profileDigest: string;
  readonly profile: ExecutionProfile;
}
export interface SqliteTeamBindingStoreOptions {
  readonly filename?: string;
  readonly now?: () => Date;
  readonly busyTimeoutMs?: number;
  readonly validator?: TeamBindingValidator;
}

type Row = Record<string, unknown>;
const validatorCodes = new Set(["reference_missing", "stale", "incompatible"]);
const schemaDefinitions = Object.freeze({
  "table:team_binding_schema": "CREATE TABLE team_binding_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
  "table:team_binding_revisions": "CREATE TABLE team_binding_revisions(\n          organization_id TEXT NOT NULL, linear_team_id TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision > 0),\n          contract_version TEXT NOT NULL, action TEXT NOT NULL CHECK(action IN ('configure', 'disable')), status TEXT NOT NULL CHECK(status IN ('configured', 'disabled')), profile_json TEXT, profile_digest TEXT,\n          created_at TEXT NOT NULL, PRIMARY KEY(organization_id, linear_team_id, revision),\n          CHECK((status = 'configured' AND profile_json IS NOT NULL AND profile_digest IS NOT NULL) OR (status = 'disabled' AND profile_json IS NULL AND profile_digest IS NULL))\n        )",
  "table:team_binding_audits": "CREATE TABLE team_binding_audits(\n          organization_id TEXT NOT NULL, linear_team_id TEXT NOT NULL, revision INTEGER NOT NULL, contract_version TEXT NOT NULL,\n          action TEXT NOT NULL CHECK(action IN ('configure', 'disable')), status TEXT NOT NULL CHECK(status IN ('configured', 'disabled')), created_at TEXT NOT NULL, profile_digest TEXT, audit_json TEXT NOT NULL,\n          PRIMARY KEY(organization_id, linear_team_id, revision),\n          FOREIGN KEY(organization_id, linear_team_id, revision) REFERENCES team_binding_revisions(organization_id, linear_team_id, revision)\n        )",
  "table:team_binding_current": "CREATE TABLE team_binding_current(\n          organization_id TEXT NOT NULL, linear_team_id TEXT NOT NULL, revision INTEGER NOT NULL,\n          PRIMARY KEY(organization_id, linear_team_id),\n          FOREIGN KEY(organization_id, linear_team_id, revision) REFERENCES team_binding_revisions(organization_id, linear_team_id, revision)\n        )",
  "trigger:team_binding_revisions_immutable_update": "CREATE TRIGGER team_binding_revisions_immutable_update BEFORE UPDATE ON team_binding_revisions BEGIN SELECT RAISE(ABORT, 'team binding revisions are immutable'); END",
  "trigger:team_binding_revisions_immutable_delete": "CREATE TRIGGER team_binding_revisions_immutable_delete BEFORE DELETE ON team_binding_revisions BEGIN SELECT RAISE(ABORT, 'team binding revisions are immutable'); END",
  "trigger:team_binding_audits_immutable_update": "CREATE TRIGGER team_binding_audits_immutable_update BEFORE UPDATE ON team_binding_audits BEGIN SELECT RAISE(ABORT, 'team binding audits are immutable'); END",
  "trigger:team_binding_audits_immutable_delete": "CREATE TRIGGER team_binding_audits_immutable_delete BEFORE DELETE ON team_binding_audits BEGIN SELECT RAISE(ABORT, 'team binding audits are immutable'); END",
});
const schemaFingerprints = schemaFingerprintTable(schemaDefinitions);
class TeamBindingInputError extends TypeError { constructor() { super("Team binding input is malformed."); } }
function malformed(): never { throw new TeamBindingInputError(); }
const { plainData, opaque, expected } = durableStoreInput(TeamBindingInputError);
function scope(value: unknown): TeamBindingScope {
  const data = plainData(value, ["organizationId", "linearTeamId"]);
  return { organizationId: opaque(data.organizationId), linearTeamId: opaque(data.linearTeamId) };
}
function ref(value: unknown): TeamBindingVersionedRef {
  const data = plainData(value, ["ref", "version"]);
  return { ref: opaque(data.ref), version: opaque(data.version) };
}
function profile(value: unknown): ExecutionProfile {
  try { return executionProfile(value); }
  catch { malformed(); }
}
function audit(value: unknown): TeamBindingMutationAudit {
  const data = plainData(value, ["operatorRef", "source", "requestRef", "authorizationRef"]);
  return { operatorRef: opaque(data.operatorRef), source: opaque(data.source), requestRef: opaque(data.requestRef), authorizationRef: opaque(data.authorizationRef) };
}
function options(value: unknown): { readonly filename?: string; readonly now?: () => Date; readonly busyTimeoutMs?: number; readonly validator?: TeamBindingValidator } {
  const data = plainData(value);
  const keys = Object.keys(data), allowed = ["filename", "now", "busyTimeoutMs", "validator"];
  if (keys.some((key) => !allowed.includes(key))) malformed();
  if (data.filename !== undefined && typeof data.filename !== "string") malformed();
  if (data.now !== undefined && typeof data.now !== "function") malformed();
  if (data.busyTimeoutMs !== undefined && (!Number.isSafeInteger(data.busyTimeoutMs) || (data.busyTimeoutMs as number) < 0)) malformed();
  if (data.validator !== undefined && typeof data.validator !== "function") malformed();
  return data as { readonly filename?: string; readonly now?: () => Date; readonly busyTimeoutMs?: number; readonly validator?: TeamBindingValidator };
}
const digest = executionProfileDigest;

/**
 * A Router-database-local binding store. Callers resolve a closed profile before
 * any composition decision and own all provider interaction.
 */
export class SqliteTeamBindingStore {
  readonly #db: DatabaseSync;
  readonly #now: () => Date;
  readonly #validator: TeamBindingValidator;
  readonly #connection: DurableStoreConnection;

  constructor(input: SqliteTeamBindingStoreOptions = {}) {
    const normalized = options(input);
    this.#connection = openDurableStoreConnection(normalized, {
      conflict: () => new TeamBindingError("persisted_state_conflict"),
      preserve: (error) => error instanceof TeamBindingError,
    });
    this.#db = this.#connection.db;
    this.#now = normalized.now ?? (() => new Date());
    this.#validator = normalized.validator ?? (() => ({ status: "valid" }));
    this.#connection.initialize(() => { this.#initializeSchema(); this.#assertInvariants(); });
  }

  close(): void { this.#connection.close(); }
  ready(): boolean {
    try { this.#assertOpen(); this.#assertInvariants(); return true; } catch { return false; }
  }

  configure(input: { readonly scope: TeamBindingScope; readonly expectedRevision: number; readonly profile: ExecutionProfile; readonly audit: TeamBindingMutationAudit }): TeamBindingRevision {
    const data = plainData(input, ["scope", "expectedRevision", "profile", "audit"]);
    const normalizedScope = scope(data.scope), normalizedProfile = profile(data.profile), normalizedAudit = audit(data.audit), revision = expected(data.expectedRevision);
    this.#assertOpen(); this.#assertInvariants();
    this.#validateProfile(normalizedProfile);
    return this.#transaction(() => { this.#assertInvariants(); return this.#write(normalizedScope, revision, "configured", normalizedProfile, normalizedAudit); });
  }
  disable(input: { readonly scope: TeamBindingScope; readonly expectedRevision: number; readonly audit: TeamBindingMutationAudit }): TeamBindingRevision {
    const data = plainData(input, ["scope", "expectedRevision", "audit"]);
    const normalizedScope = scope(data.scope), normalizedAudit = audit(data.audit), revision = expected(data.expectedRevision);
    this.#assertOpen(); this.#assertInvariants();
    return this.#transaction(() => {
      this.#assertInvariants(); const prior = this.#current(normalizedScope);
      if (prior === null) throw new TeamBindingError("unconfigured");
      if (prior.status === "disabled") throw new TeamBindingError("disabled");
      if (prior.revision !== revision) throw new TeamBindingError("revision_conflict");
      return this.#write(normalizedScope, revision, "disabled", undefined, normalizedAudit);
    });
  }
  current(input: { readonly scope: TeamBindingScope }): TeamBindingRevision | null {
    const data = plainData(input, ["scope"]);
    return this.#current(scope(data.scope));
  }
  history(input: { readonly scope: TeamBindingScope; readonly afterRevision?: number; readonly limit?: number }): readonly TeamBindingRevision[] {
    const data = plainData(input), keys = Object.keys(data);
    if (keys.some((key) => !["scope", "afterRevision", "limit"].includes(key)) || !Object.hasOwn(data, "scope")) malformed();
    const normalizedScope = scope(data.scope);
    const afterRevision = data.afterRevision === undefined ? undefined : expected(data.afterRevision);
    const limit = data.limit === undefined ? 100 : expected(data.limit);
    if (limit < 1 || limit > 100) malformed();
    this.#assertOpen(); this.#assertInvariants();
    return (this.#db.prepare(`SELECT r.*, a.audit_json, a.contract_version AS audit_contract_version, a.action AS audit_action, a.status AS audit_status, a.created_at AS audit_created_at, a.profile_digest AS audit_profile_digest FROM team_binding_revisions r JOIN team_binding_audits a
      ON a.organization_id = r.organization_id AND a.linear_team_id = r.linear_team_id AND a.revision = r.revision
      WHERE r.organization_id = ? AND r.linear_team_id = ? ${afterRevision === undefined ? "" : "AND r.revision > ?"} ORDER BY r.revision ASC LIMIT ?`).all(normalizedScope.organizationId, normalizedScope.linearTeamId, ...(afterRevision === undefined ? [] : [afterRevision]), limit) as Row[])
      .map((row) => this.#revision(row));
  }
  listCurrent(input: { readonly organizationId: string; readonly afterLinearTeamId?: string; readonly limit?: number }): readonly TeamBindingRevision[] {
    const data = plainData(input), keys = Object.keys(data);
    if (keys.some((key) => !["organizationId", "afterLinearTeamId", "limit"].includes(key)) || !Object.hasOwn(data, "organizationId")) malformed();
    const organizationId = opaque(data.organizationId);
    const afterLinearTeamId = data.afterLinearTeamId === undefined ? undefined : opaque(data.afterLinearTeamId);
    const limit = data.limit === undefined ? 100 : expected(data.limit);
    if (limit < 1 || limit > 100) malformed();
    this.#assertOpen(); this.#assertInvariants();
    const cursor = afterLinearTeamId ?? null;
    return Object.freeze((this.#db.prepare(`SELECT r.*, a.audit_json, a.contract_version AS audit_contract_version, a.action AS audit_action, a.status AS audit_status, a.created_at AS audit_created_at, a.profile_digest AS audit_profile_digest FROM team_binding_current c JOIN team_binding_revisions r
      ON r.organization_id = c.organization_id AND r.linear_team_id = c.linear_team_id AND r.revision = c.revision
      JOIN team_binding_audits a ON a.organization_id = r.organization_id AND a.linear_team_id = r.linear_team_id AND a.revision = r.revision
      WHERE c.organization_id = ? AND (? IS NULL OR c.linear_team_id COLLATE BINARY > ?) ORDER BY c.linear_team_id COLLATE BINARY ASC LIMIT ?`).all(organizationId, cursor, cursor, limit) as Row[])
      .map((row) => this.#revision(row)));
  }
  validate(input: { readonly scope: TeamBindingScope }): TeamBindingValidation {
    const data = plainData(input, ["scope"]);
    const current = this.#configured(scope(data.scope));
    this.#validateProfile(current.profile!);
    return frozen({ contractVersion: TEAM_BINDING_CONTRACT_VERSION, scope: current.scope, revision: current.revision, profileDigest: current.profileDigest! });
  }
  resolve(input: { readonly scope: TeamBindingScope }): TeamBindingResolution {
    const data = plainData(input, ["scope"]);
    const current = this.#configured(scope(data.scope));
    this.#validateProfile(current.profile!);
    return frozen({ contractVersion: TEAM_BINDING_CONTRACT_VERSION, scope: current.scope, revision: current.revision, profile: current.profile!, profileDigest: current.profileDigest! });
  }

  #initializeSchema(): void {
    this.#transaction(() => {
      const existing = this.#db.prepare("SELECT 1 FROM sqlite_master WHERE name LIKE 'team_binding_%' LIMIT 1").get();
      if (existing !== undefined) return;
      for (const definition of Object.values(schemaDefinitions)) this.#db.exec(definition);
      this.#db.prepare("INSERT INTO team_binding_schema(version, applied_at) VALUES (1, ?)").run(this.#now().toISOString());
    });
  }
  #write(scopeValue: TeamBindingScope, expectedRevision: number, state: "configured" | "disabled", profileValue: ExecutionProfile | undefined, auditValue: TeamBindingMutationAudit): TeamBindingRevision {
    const prior = this.#current(scopeValue);
    const actual = prior?.revision ?? 0;
    if (actual !== expectedRevision) throw new TeamBindingError("revision_conflict");
    const revision = actual + 1, createdAt = this.#now().toISOString(), action = state === "configured" ? "configure" : "disable", profileDigest = profileValue === undefined ? undefined : digest(profileValue);
    this.#db.prepare("INSERT INTO team_binding_revisions(organization_id, linear_team_id, revision, contract_version, action, status, profile_json, profile_digest, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(scopeValue.organizationId, scopeValue.linearTeamId, revision, TEAM_BINDING_CONTRACT_VERSION, action, state, profileValue === undefined ? null : canonical(profileValue), profileDigest ?? null, createdAt);
    this.#db.prepare("INSERT INTO team_binding_audits(organization_id, linear_team_id, revision, contract_version, action, status, created_at, profile_digest, audit_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(scopeValue.organizationId, scopeValue.linearTeamId, revision, TEAM_BINDING_CONTRACT_VERSION, action, state, createdAt, profileDigest ?? null, canonical(auditValue));
    this.#db.prepare(`INSERT INTO team_binding_current(organization_id, linear_team_id, revision) VALUES (?, ?, ?)
      ON CONFLICT(organization_id, linear_team_id) DO UPDATE SET revision = excluded.revision`).run(scopeValue.organizationId, scopeValue.linearTeamId, revision);
    return frozen({ contractVersion: TEAM_BINDING_CONTRACT_VERSION, scope: copy(scopeValue), revision, status: state, ...(profileValue === undefined ? {} : { profile: copy(profileValue), profileDigest: profileDigest! }), audit: copy(auditValue), createdAt });
  }
  /**
   * Reads the pointer and the revision it selects under one snapshot, so a
   * concurrent writer cannot produce a false `persisted_state_conflict`.
   */
  #current(scopeValue: TeamBindingScope): TeamBindingRevision | null {
    this.#assertOpen();
    return this.#readSnapshot(() => this.#currentUnderSnapshot(scopeValue));
  }
  #currentUnderSnapshot(scopeValue: TeamBindingScope): TeamBindingRevision | null {
    // Inside the snapshot, so the invariant check and the read see the same state.
    this.#assertInvariants();
    const pointer = this.#db.prepare("SELECT revision FROM team_binding_current WHERE organization_id = ? AND linear_team_id = ?")
      .get(scopeValue.organizationId, scopeValue.linearTeamId) as Row | undefined;
    const row = this.#db.prepare(`SELECT r.*, a.audit_json, a.contract_version AS audit_contract_version, a.action AS audit_action, a.status AS audit_status, a.created_at AS audit_created_at, a.profile_digest AS audit_profile_digest FROM team_binding_current c JOIN team_binding_revisions r
      ON r.organization_id = c.organization_id AND r.linear_team_id = c.linear_team_id AND r.revision = c.revision
      JOIN team_binding_audits a ON a.organization_id = r.organization_id AND a.linear_team_id = r.linear_team_id AND a.revision = r.revision
      WHERE c.organization_id = ? AND c.linear_team_id = ?`).get(scopeValue.organizationId, scopeValue.linearTeamId) as Row | undefined;
    if (row !== undefined) return this.#revision(row);
    if (pointer !== undefined || this.#db.prepare("SELECT 1 FROM team_binding_revisions WHERE organization_id = ? AND linear_team_id = ? LIMIT 1")
      .get(scopeValue.organizationId, scopeValue.linearTeamId) !== undefined) throw new TeamBindingError("persisted_state_conflict");
    return null;
  }
  #configured(scopeValue: TeamBindingScope): TeamBindingRevision {
    const current = this.#current(scopeValue);
    if (current === null) throw new TeamBindingError("unconfigured");
    if (current.status === "disabled") throw new TeamBindingError("disabled");
    return current;
  }
  #revision(row: Row): TeamBindingRevision {
    try {
      const scopeValue = scope({ organizationId: row.organization_id, linearTeamId: row.linear_team_id });
      const revision = Number(row.revision), state = row.status;
      const action = state === "configured" ? "configure" : "disable";
      if (!Number.isSafeInteger(revision) || revision < 1 || row.contract_version !== TEAM_BINDING_CONTRACT_VERSION || row.audit_contract_version !== TEAM_BINDING_CONTRACT_VERSION || row.action !== action || row.audit_action !== action || (state !== "configured" && state !== "disabled") || row.audit_status !== state || typeof row.created_at !== "string" || row.audit_created_at !== row.created_at || new Date(row.created_at).toISOString() !== row.created_at) throw new Error();
      const auditJson = String(row.audit_json); const auditValue = audit(JSON.parse(auditJson));
      if (canonical(auditValue) !== auditJson) throw new Error();
      if (state === "disabled") {
        if (row.profile_json !== null || row.profile_digest !== null || row.audit_profile_digest !== null) throw new Error();
        return frozen({ contractVersion: TEAM_BINDING_CONTRACT_VERSION, scope: scopeValue, revision, status: state, audit: auditValue, createdAt: row.created_at });
      }
      const profileJson = String(row.profile_json); const profileValue = profile(JSON.parse(profileJson));
      if (canonical(profileValue) !== profileJson) throw new Error();
      const profileDigest = typeof row.profile_digest === "string" ? row.profile_digest : "";
      if (profileDigest !== digest(profileValue) || row.audit_profile_digest !== profileDigest) throw new Error();
      return frozen({ contractVersion: TEAM_BINDING_CONTRACT_VERSION, scope: scopeValue, revision, status: state, profile: profileValue, profileDigest, audit: auditValue, createdAt: row.created_at });
    } catch { throw new TeamBindingError("persisted_state_conflict"); }
  }
  #validateProfile(profileValue: ExecutionProfile): TeamBindingValidatorResult {
    const outcome = executionProfileValidatorOutcome(this.#validator, profileValue);
    if (outcome !== "valid") throw new TeamBindingError(outcome);
    return { status: "valid" };
  }
  #assertOpen(): void { this.#connection.assertOpen(); }
  #assertInvariants(): void {
    try { this.#readSnapshot(() => this.#assertInvariantsUnsafe()); }
    catch (error) {
      if (error instanceof TeamBindingError) throw error;
      throw new TeamBindingError("persisted_state_conflict");
    }
  }
  #assertInvariantsUnsafe(): void {
    const integrity = this.#db.prepare("PRAGMA integrity_check").get() as { integrity_check?: string } | undefined;
    if (integrity?.integrity_check !== "ok") throw new TeamBindingError("persisted_state_conflict");
    const foreignKeys = this.#db.prepare("PRAGMA foreign_key_check").all();
    if (foreignKeys.length !== 0) throw new TeamBindingError("persisted_state_conflict");
    const objects = this.#db.prepare("SELECT type, name, sql FROM sqlite_master WHERE (name LIKE 'team_binding_%' OR tbl_name LIKE 'team_binding_%') AND name NOT GLOB 'sqlite_*'").all() as Row[];
    if (!schemaObjectsMatch(objects, schemaFingerprints)) throw new TeamBindingError("persisted_state_conflict");
    const ledger = this.#db.prepare("SELECT version, applied_at FROM team_binding_schema").all() as Row[];
    if (!schemaLedgerAgrees(ledger, 1)) throw new TeamBindingError("persisted_state_conflict");
    const scopes = this.#db.prepare("SELECT organization_id, linear_team_id FROM team_binding_revisions GROUP BY organization_id, linear_team_id").all() as Row[];
    for (const value of scopes) {
      const organizationId = opaque(value.organization_id), linearTeamId = opaque(value.linear_team_id);
      const revisions = this.#db.prepare("SELECT r.*, a.audit_json, a.contract_version AS audit_contract_version, a.action AS audit_action, a.status AS audit_status, a.created_at AS audit_created_at, a.profile_digest AS audit_profile_digest FROM team_binding_revisions r LEFT JOIN team_binding_audits a ON a.organization_id = r.organization_id AND a.linear_team_id = r.linear_team_id AND a.revision = r.revision WHERE r.organization_id = ? AND r.linear_team_id = ? ORDER BY r.revision").all(organizationId, linearTeamId) as Row[];
      if (revisions.length === 0) throw new TeamBindingError("persisted_state_conflict");
      revisions.forEach((row, index) => { if (Number(row.revision) !== index + 1) throw new TeamBindingError("persisted_state_conflict"); this.#revision(row); });
      const current = this.#db.prepare("SELECT revision FROM team_binding_current WHERE organization_id = ? AND linear_team_id = ?").get(organizationId, linearTeamId) as Row | undefined;
      if (current === undefined || Number(current.revision) !== revisions.length) throw new TeamBindingError("persisted_state_conflict");
    }
    const pointers = this.#db.prepare("SELECT organization_id, linear_team_id FROM team_binding_current").all() as Row[];
    if (pointers.length !== scopes.length) throw new TeamBindingError("persisted_state_conflict");
  }
  /**
   * Runs a multi-statement read under one snapshot. In WAL mode every statement
   * takes its own snapshot, so a concurrent writer between the revision-list and
   * pointer reads would otherwise make a healthy database report
   * `persisted_state_conflict`. Nesting inside a write transaction is a no-op.
   */
  #readSnapshot<T>(operation: () => T): T { return this.#connection.readSnapshot(operation); }
  #transaction<T>(operation: () => T): T {
    this.#assertOpen();
    try { this.#db.exec("BEGIN IMMEDIATE"); }
    catch { throw new TeamBindingError("persisted_state_conflict"); }
    this.#connection.inTransaction = true;
    try {
      const result = operation();
      try { this.#db.exec("COMMIT"); } catch { try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ } throw new TeamBindingError("persisted_state_conflict"); }
      return result;
    } catch (error) {
      try { this.#db.exec("ROLLBACK"); } catch { /* best effort */ }
      if (error instanceof TeamBindingError) throw error;
      throw new TeamBindingError("persisted_state_conflict");
    } finally { this.#connection.inTransaction = false; }
  }
}
