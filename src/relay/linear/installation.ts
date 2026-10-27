import { DatabaseSync } from "node:sqlite";

import { RelayError } from "../errors.js";
import { schemaFingerprintTable, schemaLedgerAgrees, schemaObjectsMatch } from "../../shared/schema-ledger.js";

/**
 * Linear installation state: which App User each organization installed Axis as, the vault reference
 * of its credential (never the credential), its public-Team access and its Team allowlist, plus the
 * one-time OAuth handshake rows. See `docs/LINEAR-PROVIDER.md` "Authorization drift".
 *
 * It lives in its own database (the configured channel database) so authority can be re-checked before
 * every command and delivery without loading session state.
 *
 * `authority` answers `null` (no active installation for this exact organization/App User pair; mapped
 * to `linear.auth.revoked`), `{ allowed: false }` (Team not reachable) or `{ allowed: true }`. A refusal
 * still carries `credentialRef`; stripping it is `authorizeLinearAuthority`'s job in `./authorization.ts`.
 *
 * `revoke` flips state and returns the credential reference for the caller to delete afterwards, so a
 * crash in between leaves the installation closed rather than authorized with a dead token.
 *
 * Team rows are keyed by organization only (one installation per organization), so a permission change
 * naming a stale App User still edits the current installation's allowlist; the public-Team flag update
 * is scoped to the active App User.
 */

/** The only authority projection callers may act on. See `authority`. */
export interface LinearInstallationAuthority {
  /** Vault reference for the installation's encrypted OAuth credential. Never the credential. */
  readonly credentialRef: string;
  /** Whether the requested Team is reachable, via the explicit allowlist or public-Team access. */
  readonly allowed: boolean;
}

export interface LinearInstallationInput {
  readonly organizationId: string;
  readonly appUserId: string;
  readonly credentialRef: string;
  readonly teamIds: readonly string[];
  readonly canAccessAllPublicTeams: boolean;
}

/** Relay-private setup projection. The credential reference never contains the credential. */
export interface ActiveLinearInstallation {
  readonly organizationId: string;
  readonly appUserId: string;
  readonly credentialRef: string;
  readonly revision: number;
}

export interface LinearInstallationInstallOptions {
  readonly expectedRevision?: number;
}

/**
 * The `PermissionChange.teamAccessChanged` fields this store consumes, declared structurally so no other
 * part of the webhook envelope can be persisted.
 */
export interface LinearPermissionChangeInput {
  readonly organizationId: string;
  readonly appUserId: string;
  readonly addedTeamIds: readonly string[];
  readonly removedTeamIds: readonly string[];
  readonly canAccessAllPublicTeams: boolean;
}

export interface LinearOAuthStateInput {
  readonly stateDigest: string;
  readonly verifierCiphertext: string;
  readonly returnTo: string;
  readonly expiresAt: string;
}

export interface LinearOAuthStateRecord {
  readonly verifierCiphertext: string;
  readonly returnTo: string;
}

/** The authority read, isolated so a consumer that only decides can hold nothing else. */
export interface LinearAuthorityPort {
  authority(organizationId: string, appUserId: string, teamId: string): LinearInstallationAuthority | null;
}

/** Relay-private execution grant snapshot; revision changes on every installation/permission write. */
export interface LinearExecutionInstallationAuthority extends LinearInstallationAuthority {
  readonly revision: number;
}

export interface LinearExecutionInstallationPort {
  executionAuthority(organizationId: string, appUserId: string, teamId: string): LinearExecutionInstallationAuthority | null;
}

/** The store surface `LinearOAuthManager` (`../oauth.ts`) uses: one-time PKCE state and the post-exchange install. */
export interface LinearInstallationOAuthPort {
  saveOAuthState(input: LinearOAuthStateInput): void;
  consumeOAuthState(stateDigest: string): LinearOAuthStateRecord | null;
  install(input: LinearInstallationInput, options?: LinearInstallationInstallOptions): void;
}

/** The complete channel-facing surface: authorization reads plus lifecycle writes. */
export interface LinearInstallationPort extends LinearAuthorityPort, LinearInstallationOAuthPort {
  revoke(organizationId: string, appUserId: string): string | null;
  applyPermissionChange(input: LinearPermissionChangeInput): void;
}

/** Bounded, secret-free installation audit. Counts, identifiers, and booleans only. */
export interface LinearInstallationAuditEvent {
  readonly eventType: "linear.installation.active" | "linear.installation.revoked" | "linear.permission.changed";
  readonly subject: string;
  readonly metadata: Readonly<Record<string, string | number | boolean>>;
  readonly recordedAt: string;
}

export const LINEAR_INSTALLATION_SCHEMA_VERSION = 1;

const schemaDefinitions = Object.freeze({
  "table:linear_channel_schema": `CREATE TABLE linear_channel_schema (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
)`,
  "table:linear_installations": `CREATE TABLE linear_installations (
  organization_id TEXT PRIMARY KEY,
  app_user_id TEXT NOT NULL,
  credential_ref TEXT NOT NULL,
  can_access_all_public_teams INTEGER NOT NULL DEFAULT 0 CHECK (can_access_all_public_teams IN (0, 1)),
  state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
  installed_at TEXT NOT NULL,
  revoked_at TEXT
)`,
  "table:linear_installation_teams": `CREATE TABLE linear_installation_teams (
  organization_id TEXT NOT NULL REFERENCES linear_installations(organization_id) ON DELETE CASCADE,
  team_id TEXT NOT NULL,
  PRIMARY KEY (organization_id, team_id)
)`,
  "table:linear_oauth_states": `CREATE TABLE linear_oauth_states (
  state_digest TEXT PRIMARY KEY,
  code_verifier_ciphertext TEXT NOT NULL,
  return_to TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  consumed_at TEXT,
  created_at TEXT NOT NULL
)`,
  "table:linear_installation_audit": `CREATE TABLE linear_installation_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  subject TEXT NOT NULL,
  safe_metadata_json TEXT NOT NULL,
  recorded_at TEXT NOT NULL
)`,
});
const schemaFingerprints = schemaFingerprintTable(schemaDefinitions);

interface InstallationRow {
  credential_ref: string;
  can_access_all_public_teams: number;
  team_allowed: number;
}

export class LinearInstallationStore implements LinearInstallationPort {
  readonly #db: DatabaseSync;
  readonly #now: () => Date;

  /**
   * @param filename the configured channel database path, or `:memory:` in tests.
   */
  constructor(filename: string, options: { readonly now?: () => Date; readonly busyTimeoutMs?: number } = {}) {
    this.#db = new DatabaseSync(filename);
    this.#now = options.now ?? (() => new Date());
    // `foreign_keys=ON` rejects Team rows for an organization with no installation row.
    this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;");
    this.#db.exec(`PRAGMA busy_timeout=${options.busyTimeoutMs ?? 5_000};`);
    try { this.#initializeSchema(); }
    catch (error) { this.#db.close(); throw error; }
  }

  /** Creates the complete schema once and validates the current schema on every open. */
  #initializeSchema(): void {
    this.#transaction(() => {
      const objects = this.#db.prepare("SELECT type, name, sql FROM sqlite_master WHERE (name GLOB 'linear_installation*' OR name GLOB 'linear_oauth*' OR name GLOB 'linear_channel*' OR tbl_name GLOB 'linear_installation*' OR tbl_name GLOB 'linear_oauth*' OR tbl_name GLOB 'linear_channel*') AND name NOT GLOB 'sqlite_*'").all();
      if (objects.length === 0) {
        for (const definition of Object.values(schemaDefinitions)) this.#db.exec(definition);
        this.#db.prepare("INSERT INTO linear_channel_schema(version, applied_at) VALUES (?, ?)")
          .run(LINEAR_INSTALLATION_SCHEMA_VERSION, this.#instant());
      } else {
        if (!schemaObjectsMatch(objects, schemaFingerprints)) {
          throw new RelayError("axis.relay.schema_version_unsupported", "The Linear channel database does not match the current schema.", 500);
        }
        const ledger = this.#db.prepare("SELECT version, applied_at FROM linear_channel_schema").all();
        if (!schemaLedgerAgrees(ledger, LINEAR_INSTALLATION_SCHEMA_VERSION)) {
          throw new RelayError("axis.relay.schema_version_unsupported", "The Linear channel database does not match the current schema version.", 500);
        }
      }
    });
  }

  close(): void {
    this.#db.close();
  }

  /** Active installations for Relay-private setup polling, ordered by organization. */
  activeInstallations(): readonly ActiveLinearInstallation[] {
    const rows = this.#db.prepare(`
      SELECT i.organization_id, i.app_user_id, i.credential_ref,
             (SELECT MAX(a.id) FROM linear_installation_audit a WHERE a.subject = i.organization_id) AS revision
      FROM linear_installations i WHERE i.state = 'active' ORDER BY i.organization_id
    `).all() as unknown as {
      organization_id: string; app_user_id: string; credential_ref: string; revision: number | null;
    }[];
    return rows.map((row) => {
      if (row.revision === null || !Number.isSafeInteger(row.revision) || row.revision < 1) {
        throw new RelayError("linear.auth.invalid_state", "The Linear installation revision is unavailable.", 500);
      }
      return {
        organizationId: row.organization_id,
        appUserId: row.app_user_id,
        credentialRef: row.credential_ref,
        revision: row.revision,
      };
    });
  }

  /**
   * Records a completed OAuth installation, replacing any prior one for the organization. The Team
   * allowlist is replaced, not merged, so a Team removed while uninstalled does not survive a reinstall.
   * An expected revision narrows an unchanged active installation in the same transaction as the write.
   */
  install(input: LinearInstallationInput, options: LinearInstallationInstallOptions = {}): void {
    this.#transaction(() => {
      if (options.expectedRevision !== undefined) {
        const current = this.#db.prepare(`
          SELECT i.app_user_id, i.credential_ref,
                 (SELECT MAX(a.id) FROM linear_installation_audit a WHERE a.subject = i.organization_id) AS revision
          FROM linear_installations i WHERE i.organization_id = ? AND i.state = 'active'
        `).get(input.organizationId) as { app_user_id: string; credential_ref: string; revision: number | null } | undefined;
        if (
          current === undefined
          || current.app_user_id !== input.appUserId
          || current.credential_ref !== input.credentialRef
          || !Number.isSafeInteger(options.expectedRevision)
          || options.expectedRevision < 1
          || current.revision !== options.expectedRevision
        ) {
          throw new RelayError("linear.auth.invalid_state", "The Linear installation changed during setup.", 409);
        }
      }
      const now = this.#instant();
      this.#db.prepare(`
        INSERT INTO linear_installations(
          organization_id, app_user_id, credential_ref, can_access_all_public_teams, state, installed_at, revoked_at
        ) VALUES (?, ?, ?, ?, 'active', ?, NULL)
        ON CONFLICT(organization_id) DO UPDATE SET
          app_user_id = excluded.app_user_id,
          credential_ref = excluded.credential_ref,
          can_access_all_public_teams = excluded.can_access_all_public_teams,
          state = 'active', installed_at = excluded.installed_at, revoked_at = NULL
      `).run(input.organizationId, input.appUserId, input.credentialRef, input.canAccessAllPublicTeams ? 1 : 0, now);
      this.#db.prepare("DELETE FROM linear_installation_teams WHERE organization_id = ?").run(input.organizationId);
      const insert = this.#db.prepare("INSERT INTO linear_installation_teams(organization_id, team_id) VALUES (?, ?)");
      for (const teamId of [...new Set(input.teamIds)]) insert.run(input.organizationId, teamId);
      // The audit reports the requested list length, not the deduplicated row count.
      this.#audit("linear.installation.active", input.organizationId, {
        appUserId: input.appUserId,
        teamCount: input.teamIds.length,
      });
    });
  }

  /**
   * Closes new-work authority for an active installation and returns the credential reference the caller
   * must delete from the vault. `null` when nothing is active, so a replayed `OAuthApp.revoked` is inert.
   */
  revoke(organizationId: string, appUserId: string): string | null {
    return this.#transaction(() => {
      const row = this.#db.prepare(`
        SELECT credential_ref FROM linear_installations
        WHERE organization_id = ? AND app_user_id = ? AND state = 'active'
      `).get(organizationId, appUserId) as { credential_ref: string } | undefined;
      if (row === undefined) return null;
      this.#db.prepare(`
        UPDATE linear_installations SET state = 'revoked', revoked_at = ? WHERE organization_id = ?
      `).run(this.#instant(), organizationId);
      this.#audit("linear.installation.revoked", organizationId, { appUserId });
      return row.credential_ref;
    });
  }

  /**
   * Applies one `PermissionChange.teamAccessChanged` atomically, so a removal can never commit without its
   * paired public-flag update.
   */
  applyPermissionChange(input: LinearPermissionChangeInput): void {
    this.#transaction(() => {
      this.#db.prepare(`
        UPDATE linear_installations SET can_access_all_public_teams = ?
        WHERE organization_id = ? AND app_user_id = ? AND state = 'active'
      `).run(input.canAccessAllPublicTeams ? 1 : 0, input.organizationId, input.appUserId);
      const remove = this.#db.prepare("DELETE FROM linear_installation_teams WHERE organization_id = ? AND team_id = ?");
      for (const teamId of input.removedTeamIds) remove.run(input.organizationId, teamId);
      // Removals run first, so a Team named on both sides ends up added.
      const add = this.#db.prepare("INSERT OR IGNORE INTO linear_installation_teams(organization_id, team_id) VALUES (?, ?)");
      for (const teamId of input.addedTeamIds) add.run(input.organizationId, teamId);
      this.#audit("linear.permission.changed", input.organizationId, {
        addedCount: input.addedTeamIds.length,
        removedCount: input.removedTeamIds.length,
        allPublic: input.canAccessAllPublicTeams,
      });
    });
  }

  /**
   * The one authorization read. Returns `null` for any organization/App User
   * pair without an active installation; otherwise reports the credential
   * reference and whether this exact Team is reachable.
   */
  authority(organizationId: string, appUserId: string, teamId: string): LinearInstallationAuthority | null {
    const row = this.#db.prepare(`
      SELECT i.credential_ref, i.can_access_all_public_teams,
             EXISTS(SELECT 1 FROM linear_installation_teams t
                    WHERE t.organization_id = i.organization_id AND t.team_id = ?) AS team_allowed
      FROM linear_installations i
      WHERE i.organization_id = ? AND i.app_user_id = ? AND i.state = 'active'
    `).get(teamId, organizationId, appUserId) as InstallationRow | undefined;
    if (row === undefined) return null;
    return { credentialRef: row.credential_ref, allowed: row.can_access_all_public_teams === 1 || row.team_allowed === 1 };
  }

  /**
   * Reads authority and its revision in one snapshot. The revision is the latest audit row id, which
   * changes on every revoke, reinstall and permission change even at the same instant. Missing revision
   * evidence refuses access; callers must not cache this projection across requests.
   */
  executionAuthority(organizationId: string, appUserId: string, teamId: string): LinearExecutionInstallationAuthority | null {
    const row = this.#db.prepare(`
      SELECT i.credential_ref, i.can_access_all_public_teams,
             EXISTS(SELECT 1 FROM linear_installation_teams t
                    WHERE t.organization_id = i.organization_id AND t.team_id = ?) AS team_allowed,
             (SELECT MAX(a.id) FROM linear_installation_audit a WHERE a.subject = i.organization_id) AS revision
      FROM linear_installations i
      WHERE i.organization_id = ? AND i.app_user_id = ? AND i.state = 'active'
    `).get(teamId, organizationId, appUserId) as (InstallationRow & { revision: number | null }) | undefined;
    if (row === undefined || row.revision === null || !Number.isSafeInteger(row.revision) || row.revision < 1) return null;
    return {
      credentialRef: row.credential_ref,
      allowed: row.can_access_all_public_teams === 1 || row.team_allowed === 1,
      revision: row.revision,
    };
  }

  saveOAuthState(input: LinearOAuthStateInput): void {
    this.#db.prepare(`
      INSERT INTO linear_oauth_states(state_digest, code_verifier_ciphertext, return_to, expires_at, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(input.stateDigest, input.verifierCiphertext, input.returnTo, input.expiresAt, this.#instant());
  }

  /** Returns an unexpired, unconsumed state and marks it consumed in the same transaction, so a replay finds nothing. */
  consumeOAuthState(stateDigest: string): LinearOAuthStateRecord | null {
    return this.#transaction(() => {
      const now = this.#instant();
      const row = this.#db.prepare(`
        SELECT code_verifier_ciphertext, return_to FROM linear_oauth_states
        WHERE state_digest = ? AND consumed_at IS NULL AND expires_at > ?
      `).get(stateDigest, now) as { code_verifier_ciphertext: string; return_to: string } | undefined;
      if (row === undefined) return null;
      this.#db.prepare("UPDATE linear_oauth_states SET consumed_at = ? WHERE state_digest = ? AND consumed_at IS NULL")
        .run(now, stateDigest);
      return { verifierCiphertext: row.code_verifier_ciphertext, returnTo: row.return_to };
    });
  }

  /** Operator/test projection of the bounded audit trail, oldest first. */
  auditEvents(): readonly LinearInstallationAuditEvent[] {
    const rows = this.#db.prepare(`
      SELECT event_type, subject, safe_metadata_json, recorded_at
      FROM linear_installation_audit ORDER BY id ASC
    `).all() as unknown as { event_type: string; subject: string; safe_metadata_json: string; recorded_at: string }[];
    return rows.map((row) => ({
      eventType: row.event_type as LinearInstallationAuditEvent["eventType"],
      subject: row.subject,
      metadata: JSON.parse(row.safe_metadata_json) as LinearInstallationAuditEvent["metadata"],
      recordedAt: row.recorded_at,
    }));
  }

  #audit(
    eventType: LinearInstallationAuditEvent["eventType"],
    subject: string,
    metadata: Readonly<Record<string, string | number | boolean>>,
  ): void {
    this.#db.prepare(`
      INSERT INTO linear_installation_audit(event_type, subject, safe_metadata_json, recorded_at)
      VALUES (?, ?, ?, ?)
    `).run(eventType, subject, JSON.stringify(metadata), this.#instant());
  }

  #transaction<T>(operation: () => T): T {
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.#db.exec("COMMIT");
      return result;
    } catch (error) {
      this.#db.exec("ROLLBACK");
      throw error;
    }
  }

  #instant(): string {
    return this.#now().toISOString();
  }
}
