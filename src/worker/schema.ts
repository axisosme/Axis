import type { DatabaseSync } from "node:sqlite";

import { schemaFingerprintTable, schemaLedgerAgrees, schemaObjectsMatch } from "../shared/schema-ledger.js";

const WORKER_SCHEMA_VERSION = 1;

const schemaDefinitions = Object.freeze({
  "table:worker_schema": "CREATE TABLE worker_schema(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
  "table:worker_commands": "CREATE TABLE worker_commands(command_id TEXT PRIMARY KEY NOT NULL, intent_json TEXT NOT NULL, intent_digest TEXT NOT NULL, response_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
  "table:worker_runs": "CREATE TABLE worker_runs(run_id TEXT PRIMARY KEY NOT NULL, intent_json TEXT NOT NULL, intent_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('reserved','binding','accepted','running','succeeded','failed','observational')), binding_json TEXT, accepted_json TEXT, terminal_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
  "table:worker_outbox": "CREATE TABLE worker_outbox(message_id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL, ordinal INTEGER NOT NULL, event_json TEXT NOT NULL, created_at TEXT NOT NULL, delivered_at TEXT, UNIQUE(run_id, ordinal))",
  "table:worker_outbox_quarantine": "CREATE TABLE worker_outbox_quarantine(sequence INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT NOT NULL UNIQUE, run_id TEXT NOT NULL, code TEXT NOT NULL, quarantined_at TEXT NOT NULL)",
  "table:worker_commentary_state": "CREATE TABLE worker_commentary_state(run_id TEXT PRIMARY KEY NOT NULL, emitted_count INTEGER NOT NULL CHECK(emitted_count BETWEEN 1 AND 5), window_started_at_ms INTEGER NOT NULL, pending_digest TEXT, pending_event_json TEXT, pending_due_at_ms INTEGER, updated_at TEXT NOT NULL, CHECK((pending_digest IS NULL AND pending_event_json IS NULL AND pending_due_at_ms IS NULL) OR (pending_digest IS NOT NULL AND pending_event_json IS NOT NULL AND pending_due_at_ms IS NOT NULL)))",
  "table:worker_commentary_digests": "CREATE TABLE worker_commentary_digests(run_id TEXT NOT NULL, commentary_digest TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(run_id, commentary_digest))",
  "table:worker_workspace_resolution_audits": "CREATE TABLE worker_workspace_resolution_audits(sequence INTEGER PRIMARY KEY AUTOINCREMENT, contract_version TEXT NOT NULL, command_id TEXT NOT NULL UNIQUE, task_id TEXT NOT NULL, run_id TEXT NOT NULL, system TEXT NOT NULL DEFAULT 'linear' CHECK(system IN ('linear')), organization_id TEXT NOT NULL, node_id TEXT NOT NULL, workspace_policy_ref_json TEXT, outcome TEXT NOT NULL CHECK(outcome IN ('mapped','default_root','fallback_root','unavailable')), reason TEXT NOT NULL CHECK(reason IN ('catalog_match','no_ref','catalog_miss','mapped_path_unavailable','root_unavailable')), occurred_at TEXT NOT NULL, CHECK((outcome = 'mapped' AND reason = 'catalog_match') OR (outcome = 'default_root' AND reason = 'no_ref') OR (outcome = 'fallback_root' AND reason IN ('catalog_miss','mapped_path_unavailable')) OR (outcome = 'unavailable' AND reason IN ('root_unavailable','catalog_miss','mapped_path_unavailable'))), CHECK((workspace_policy_ref_json IS NULL AND ((outcome = 'default_root' AND reason = 'no_ref') OR (outcome = 'unavailable' AND reason = 'root_unavailable'))) OR (workspace_policy_ref_json IS NOT NULL AND NOT ((outcome = 'default_root' AND reason = 'no_ref') OR (outcome = 'unavailable' AND reason = 'root_unavailable')))))",
  "table:worker_waiting_runs": "CREATE TABLE worker_waiting_runs(run_id TEXT PRIMARY KEY NOT NULL, terminal_json TEXT NOT NULL, created_at TEXT NOT NULL)",
  "table:worker_run_observations": "CREATE TABLE worker_run_observations(run_id TEXT PRIMARY KEY NOT NULL, cause TEXT NOT NULL, occurred_at TEXT NOT NULL)",
  "table:worker_response_projection_refusals": "CREATE TABLE worker_response_projection_refusals(run_id TEXT PRIMARY KEY NOT NULL, source_event_id TEXT NOT NULL, diagnostics_json TEXT NOT NULL, occurred_at TEXT NOT NULL)",
  "trigger:worker_workspace_resolution_audits_immutable_update": "CREATE TRIGGER worker_workspace_resolution_audits_immutable_update BEFORE UPDATE ON worker_workspace_resolution_audits BEGIN SELECT RAISE(ABORT, 'workspace resolution audits are immutable'); END",
  "trigger:worker_workspace_resolution_audits_immutable_delete": "CREATE TRIGGER worker_workspace_resolution_audits_immutable_delete BEFORE DELETE ON worker_workspace_resolution_audits BEGIN SELECT RAISE(ABORT, 'workspace resolution audits are immutable'); END",
});

const schemaFingerprints = schemaFingerprintTable(schemaDefinitions);

/** Verifies all Worker-owned objects, attached objects, and the single current schema record. */
export function assertWorkerSchema(database: DatabaseSync): void {
  const objects = database.prepare("SELECT type, name, sql FROM sqlite_master WHERE (name LIKE 'worker%' OR tbl_name LIKE 'worker%') AND name NOT GLOB 'sqlite_*'").all();
  if (!schemaObjectsMatch(objects, schemaFingerprints)) throw new Error("Worker journal schema is unavailable.");
  const ledger = database.prepare("SELECT version, applied_at FROM worker_schema").all();
  if (!schemaLedgerAgrees(ledger, WORKER_SCHEMA_VERSION)) throw new Error("Worker journal schema record is unavailable.");
}

/** Creates the complete schema only when the database contains no Worker objects. */
export function initializeWorkerSchema(database: DatabaseSync): void {
  database.exec("BEGIN IMMEDIATE");
  try {
    const present = database.prepare("SELECT 1 FROM sqlite_master WHERE name LIKE 'worker%' OR tbl_name LIKE 'worker%' LIMIT 1").get();
    if (present === undefined) {
      for (const definition of Object.values(schemaDefinitions)) database.exec(definition);
      database.prepare("INSERT INTO worker_schema(version, applied_at) VALUES (?, ?)").run(WORKER_SCHEMA_VERSION, new Date().toISOString());
    }
    assertWorkerSchema(database);
    database.exec("COMMIT");
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}
