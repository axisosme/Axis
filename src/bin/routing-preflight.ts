#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { lstat, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { backup, DatabaseSync } from "node:sqlite";

import { SqliteNodeJournal } from "../node/journal.js";
import type { NodeRuntimeConfig } from "../node/runtime-config.js";
import { nodeWorkerDispatcher } from "../node/runtime.js";
import type { RelayRuntimeConfig } from "../relay/runtime-config.js";
import { databasePaths, deriveRelayConfiguration, deriveNodeConfiguration, deriveWorkerConfiguration } from "../configuration.js";
import { loadRoleConfiguration, parseRoleOptions } from "../configuration-cli.js";
import { canonicalClaudeExecutable } from "../worker/claude-runtime.js";
import { Router } from "../router/router.js";
import { WORKER_COMMAND_VERSION } from "../shared/router-worker.js";
import { SqliteWorkerCommandJournal } from "../worker/command.js";

/**
 * `axis-routing-preflight <relay|node|worker>`: the read-only cutover preflight, run on each host from the release
 * that host is running.
 *
 * Operator-invoked only, against isolated snapshots. It opens no listener and performs no network I/O; SQLite
 * backup includes committed WAL content, and every schema or semantic check runs on a scratch copy that is
 * removed before exit. Its output is one JSON receipt with bounded counts and blocker codes -- never a credential,
 * database path, Run id, repository name or issue number -- and exit status 1 when blocked.
 *
 * A blocker means the new stack cannot start or cannot run correctly: an invalid configuration, an
 * incompatible store (including a Node journal the Node refuses to open), a retained Node version with no endpoint,
 * or a host not on the release this cutover installs. The owner ruled the cutover forced: it does not wait for
 * in-flight work. Every count of in-flight or historical work is therefore evidence and never a blocker.
 *
 * Shipped as a compiled bin rather than a repository script so it runs from the installed release, and reads the
 * release's version from its OWN package's `package.json` (located from this module, never from the working
 * directory): a host still on another release is the host the lock-step upgrade missed.
 */

type Role = "relay" | "node" | "worker";
interface Candidate { readonly present: boolean; readonly path: string; }
interface Receipt {
  readonly contractVersion: "1.0.0";
  readonly role: Role | null;
  status: "blocked" | "ready_for_isolated_rehearsal";
  axisPackageName: string | null;
  axisPackageVersion: string | null;
  readonly workerCommandVersion: string;
  configuration: "unverified" | "valid";
  storage: "unverified" | "refused" | "verified_copy" | "verified_fresh";
  readonly counts: Record<string, number>;
  readonly blockers: string[];
}

const ROLES: readonly Role[] = ["relay", "node", "worker"];
const STORAGE_BLOCKERS = ["node_journal_requires_archive"];

// An event that ends its Run, in either outbox envelope shape.
const TERMINAL_EVENT = `(json_extract(e.event_json,'$.envelopeType') IN ('run.succeeded','run.failed')
  OR json_extract(e.event_json,'$.envelope.envelopeType') IN ('run.succeeded','run.failed'))`;

/** Router counts, all evidence: work this Router database still holds open. */
export const ROUTER_COUNT_SQL = Object.freeze({
  nonterminalRuns: "SELECT COUNT(*) AS n FROM router_runs WHERE state NOT IN ('succeeded','failed') OR waiting_for_input=1",
  unresolvedNodeCommands: "SELECT COUNT(*) AS n FROM router_node_command_state WHERE state NOT IN ('accepted','rejected')",
  unprojectedEvents: "SELECT COUNT(*) AS n FROM router_node_events WHERE state!='projected'",
});

const role = process.argv[2];
let scratch: string | undefined;
let candidates = 0;
const receipt: Receipt = {
  // The role is echoed only when it is one this bin knows: an arbitrary argument could be a path or a secret.
  contractVersion: "1.0.0", role: ROLES.includes(role as Role) ? role as Role : null, status: "blocked",
  axisPackageName: null, axisPackageVersion: null, workerCommandVersion: WORKER_COMMAND_VERSION,
  configuration: "unverified", storage: "unverified", counts: {}, blockers: [],
};
const blocker = (code: string): void => { if (!receipt.blockers.includes(code)) receipt.blockers.push(code); };
const count = (db: DatabaseSync, sql: string, ...parameters: string[]): number => Number((db.prepare(sql).get(...parameters) as { n: number }).n);
const has = (db: DatabaseSync, table: string): boolean => db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table) !== undefined;
function readOnly<T>(filename: string, read: (db: DatabaseSync) => T): T {
  const db = new DatabaseSync(filename, { readOnly: true });
  try { return read(db); } finally { db.close(); }
}

/** A scratch copy of `filename`; when the source is absent, a scratch path with no file behind it. */
async function snapshot(filename: string): Promise<Candidate> {
  let present = true;
  try { if (!(await lstat(filename)).isFile()) throw new Error("not_regular"); }
  catch (error) { if ((error as { readonly code?: unknown }).code === "ENOENT") present = false; else throw error; }
  scratch ??= await mkdtemp(join(tmpdir(), "axis-routing-preflight-"));
  const path = join(scratch, `candidate-${candidates += 1}.sqlite`);
  if (present) {
    const source = new DatabaseSync(filename, { readOnly: true });
    try { await backup(source, path); } finally { source.close(); }
  }
  return { present, path };
}

async function nodeRole(config: NodeRuntimeConfig, candidate: Candidate): Promise<void> {
  const counts = receipt.counts;
  let journal: SqliteNodeJournal;
  // The Node's own open: a journal it refuses has to be archived.
  try { journal = await SqliteNodeJournal.open(candidate.path); }
  catch { blocker("node_journal_requires_archive"); return; }
  try {
    Object.assign(counts, await journal.maintenanceBlockerCounts());
    // Sessions whose Worker this configuration no longer admits, through dispatch's own inventory.
    counts.retainedSessionsWithoutWorker = await journal.retainedSessionsWithoutWorker(nodeWorkerDispatcher(config));
  } finally { journal.close(); }
  readOnly(candidate.path, (db) => {
    counts.routes = count(db, "SELECT COUNT(*) AS n FROM routes");
    counts.nonterminalRoutes = count(db, `SELECT COUNT(*) AS n FROM routes r
      WHERE NOT EXISTS (SELECT 1 FROM event_outbox e WHERE e.run_id=r.run_id AND ${TERMINAL_EVENT})`);
  });
}

async function relayRole(config: RelayRuntimeConfig, candidate: Candidate, runChain: string): Promise<void> {
  const counts = receipt.counts;
  const routerCounts = (): void => readOnly(candidate.path, (db) => { for (const [key, sql] of Object.entries(ROUTER_COUNT_SQL)) counts[key] = count(db, sql); });
  let counted = false;
  if (candidate.present) {
    // Counted on the raw copy first, before Router is constructed, so the evidence exists even if a later step
    // refuses; a fresh store is counted after schema creation below.
    try { routerCounts(); counted = true; } catch { counted = false; }
  }
  const router = new Router({ filename: candidate.path });
  const snapshots = new DatabaseSync(candidate.path, { readOnly: true });
  try {
    for (const row of snapshots.prepare("SELECT * FROM router_tasks").all() as Record<string, string | null>[]) {
      router.listTaskEvents({ organizationId: row.organization_id!, teamId: row.linear_team_id!, issueId: row.issue_id!, agentSessionId: row.agent_session_id!, limit: 1 });
      const target = row.target_snapshot_json === null ? null : JSON.parse(row.target_snapshot_json!) as { readonly contractVersion?: string; readonly nodeRef?: { readonly ref: string; readonly version: string } };
      if (target?.contractVersion === "1.0.0-node" && !config.nodeEndpoints.some((endpoint) =>
        endpoint.nodeRef.ref === target.nodeRef?.ref && endpoint.nodeRef.version === target.nodeRef?.version)) {
        blocker("retained_node_version_missing");
      }
    }
  } finally { snapshots.close(); router.close(); }
  if (!counted) routerCounts();

  // Channel reservations still `reserved`: evidence of commands whose outcome a channel never settled.
  const runChains: [string, string | undefined][] = [["reservedLinearCommands", runChain]];
  for (const [key, filename] of runChains) {
    if (filename === undefined || filename.length === 0) continue;
    const chain = await snapshot(filename);
    counts[key] = chain.present ? readOnly(chain.path, (db) => has(db, "channel_run_chain_reservations") ? count(db, "SELECT COUNT(*) AS n FROM channel_run_chain_reservations WHERE status = 'reserved'") : 0) : 0;
  }
}

function workerRole(candidate: Candidate): void {
  if (candidate.present) {
    readOnly(candidate.path, (db) => {
      // The macOS drain snapshot's own predicate for a Worker Run that has not ended.
      receipt.counts.nonterminalWorkerRuns = has(db, "worker_runs") ? count(db, "SELECT COUNT(*) AS n FROM worker_runs WHERE state IN ('reserved','binding','accepted','running','observational')") : 0;
    });
  }
  // Opening the journal is the Worker's own storage check.
  new SqliteWorkerCommandJournal(candidate.path).close();
}

async function main(): Promise<void> {
  try {
    // Every role reports the release it runs from -- this module's own package -- and blocks unless the operator
    // named the release this cutover installs and it matches.
    try {
      const manifest = JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as { readonly name?: unknown; readonly version?: unknown };
      receipt.axisPackageName = typeof manifest.name === "string" ? manifest.name : null;
      receipt.axisPackageVersion = typeof manifest.version === "string" ? manifest.version : null;
    } catch { receipt.axisPackageName = null; receipt.axisPackageVersion = null; }
    if (!ROLES.includes(role as Role)) throw new Error("arguments");
    const options = parseRoleOptions(process.argv.slice(3), role as Role, ["--expected-version", "--require-linear-execution"]);
    const expected = options.flags.get("--expected-version");
    if (expected === undefined || expected.trim().length === 0) blocker("axis_version_unverified");
    else if (expected !== receipt.axisPackageVersion) blocker("axis_version_mismatch");

    const required = options.flags.get("--require-linear-execution");
    if (required !== undefined && required !== "true" && required !== "false" || role === "node" && required !== undefined) throw new Error("arguments");
    const requiresLinearExecution = required === "true";
    const topology = loadRoleConfiguration(options, role as Role);
    if (role === "relay") {
      const config = deriveRelayConfiguration(topology);
      if (requiresLinearExecution && config.linearExecution === null) blocker("linear_execution_required");
      receipt.configuration = "valid";
      const candidate = await snapshot(config.runtime.routerDatabase);
      await relayRole(config.runtime, candidate, databasePaths(topology).runChain);
      receipt.storage = candidate.present ? "verified_copy" : "verified_fresh";
    } else if (role === "node") {
      const config = deriveNodeConfiguration(topology, options.nodeId);
      receipt.configuration = "valid";
      const candidate = await snapshot(config.database);
      await nodeRole(config, candidate);
      receipt.storage = STORAGE_BLOCKERS.some(code => receipt.blockers.includes(code)) ? "refused" : candidate.present ? "verified_copy" : "verified_fresh";
    } else {
      const config = deriveWorkerConfiguration(topology, options.workerId, options.nodeId);
      if (config.kind === "claude-code") await canonicalClaudeExecutable(config.executable);
      if (requiresLinearExecution && config.linearExecution === null) blocker("linear_execution_required");
      receipt.configuration = "valid";
      const candidate = await snapshot(config.database);
      workerRole(candidate);
      receipt.storage = candidate.present ? "verified_copy" : "verified_fresh";
    }
  } catch (error) {
    blocker(receipt.configuration === "valid" ? "storage_or_inventory_incompatible" : "configuration_invalid");
  } finally {
    if (scratch !== undefined) await rm(scratch, { recursive: true, force: true });
  }
  receipt.status = receipt.blockers.length === 0 ? "ready_for_isolated_rehearsal" : "blocked";
  process.stdout.write(`${JSON.stringify(receipt)}\n`);
  process.exitCode = receipt.status === "blocked" ? 1 : 0;
}

void main();
