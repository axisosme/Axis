import { deriveWorkerConfiguration } from "../configuration.js";
import { loadHostConfiguration } from "../lifecycle/configuration.js";
import { lstat, readFile, realpath, stat } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { assertWorkerSchema } from "../worker/schema.js";
import { validateHostState } from "../lifecycle/host-state.js";
import { readWorkerStuckRunAcknowledgements } from "../lifecycle/stuck-run-acknowledgements.js";
import {
  NODE_CONTRACT_VERSION,
  nodeMaintenanceStatus,
  type NodeBlockerCounts,
  type NodeMaintenanceStatus,
} from "../shared/router-node.js";
import type { NodeJournalBlockerCounts, NodeMaintenanceMode, SqliteNodeJournal } from "./journal.js";
import type { NodeRuntimeConfig, NodeRuntimeWorkerConfig } from "./runtime-config.js";

export interface NodeWorkerBlockerCounts {
  readonly activeWorkerRuns: number;
  readonly pendingWorkerOutbox: number;
  readonly unknownWorkerEvidence: number;
}

export interface NodeMaintenanceServicePort {
  status(): Promise<NodeMaintenanceStatus>;
  setMode(mode: NodeMaintenanceMode): Promise<NodeMaintenanceStatus>;
}

type MaintenanceJournal = Pick<SqliteNodeJournal, "maintenanceMode" | "setMaintenanceMode" | "maintenanceBlockerCounts">;
type WorkerEvidenceReader = (config: NodeRuntimeConfig) => Promise<NodeWorkerBlockerCounts>;

function safeCount(value: unknown): number {
  const count = Number(value);
  if (!Number.isSafeInteger(count) || count < 0) throw new Error("invalid drain evidence");
  return count;
}

async function ownerPrivateJson(path: string): Promise<unknown> {
  if (!isAbsolute(path) || path.includes("\u0000")) throw new Error("invalid evidence path");
  const canonical = await realpath(path), direct = await lstat(path), metadata = await stat(canonical);
  if (direct.isSymbolicLink() || canonical !== resolve(path) || !metadata.isFile()
    || metadata.uid !== (process.getuid?.() ?? metadata.uid) || (metadata.mode & 0o077) !== 0) throw new Error("unsafe evidence file");
  return JSON.parse(await readFile(canonical, "utf8")) as unknown;
}

function expectedExecutorKind(runtime: "codex" | "claude"): string {
  return runtime === "claude" ? "claude-code" : runtime;
}

async function workerDatabase(worker: NodeRuntimeWorkerConfig, hostRoot: string): Promise<string> {
  const state = validateHostState(await ownerPrivateJson(join(hostRoot, "axis.host.v1.json")));
  if (state.role !== "worker" || state.workerRuntime === null
    || expectedExecutorKind(state.workerRuntime) !== worker.target.identity.executorKind) throw new Error("worker host identity mismatch");
  const configuration = loadHostConfiguration(state.configuration);
  const actual = deriveWorkerConfiguration(configuration, state.configuration.workerId!, state.configuration.nodeId!);
  const database = actual.database;
  if (actual.workerId !== worker.target.identity.workerId || actual.nodeId !== worker.target.identity.nodeId
    || actual.executorId !== worker.target.sessionBindingExecutorId || actual.token !== worker.commandToken || actual.routerToken !== worker.eventToken
    || actual.runtimeProfile.ref !== worker.target.identity.runtimeProfile.ref || actual.runtimeProfile.version !== worker.target.identity.runtimeProfile.version
    || actual.kind !== worker.target.identity.executorKind) throw new Error("worker configuration identity mismatch");
  const canonical = await realpath(database), direct = await lstat(database), metadata = await stat(canonical);
  if (direct.isSymbolicLink() || canonical !== resolve(database) || !metadata.isFile()
    || metadata.uid !== (process.getuid?.() ?? metadata.uid) || (metadata.mode & 0o022) !== 0) throw new Error("unsafe worker database");
  return canonical;
}

function inspectWorkerDatabase(path: string): Omit<NodeWorkerBlockerCounts, "unknownWorkerEvidence"> {
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    database.exec("PRAGMA query_only=ON");
    const integrity = database.prepare("PRAGMA integrity_check").get() as { readonly integrity_check?: unknown } | undefined;
    if (integrity?.integrity_check !== "ok") throw new Error("worker database integrity failure");
    assertWorkerSchema(database);
    const states = (database.prepare("SELECT DISTINCT state FROM worker_runs ORDER BY state").all() as { readonly state?: unknown }[]).map(({ state }) => state);
    const known = new Set(["reserved", "binding", "accepted", "running", "succeeded", "failed", "observational"]);
    if (states.some((state) => typeof state !== "string" || !known.has(state))) throw new Error("unknown worker state");
    const active = database.prepare("SELECT COUNT(*) AS count FROM worker_runs WHERE state IN ('reserved','binding','accepted','running','observational')").get() as { readonly count?: unknown } | undefined;
    const outbox = database.prepare("SELECT COUNT(*) AS count FROM worker_outbox WHERE delivered_at IS NULL").get() as { readonly count?: unknown } | undefined;
    return { activeWorkerRuns: safeCount(active?.count), pendingWorkerOutbox: safeCount(outbox?.count) };
  } finally { database.close(); }
}

interface WorkerCandidate { readonly workerId: string; readonly worker: NodeRuntimeWorkerConfig; readonly hostRoot: string | undefined; }

/**
 * The candidate resolution every reader below shares with the raw blocker count, factored out so
 * each new reader selects exactly the databases the raw count selected -- the same dedup-by-`workerId`,
 * the same "one configured host root" requirement, and the same ambiguity rule (below).
 */
function workerCandidates(config: NodeRuntimeConfig): readonly WorkerCandidate[] {
  const byWorker = new Map<string, NodeRuntimeWorkerConfig>();
  for (const worker of config.workers) if (!byWorker.has(worker.target.identity.workerId)) byWorker.set(worker.target.identity.workerId, worker);
  const roots = new Map(config.workerHostRoots.map(({ workerId, hostRoot }) => [workerId, hostRoot]));
  return [...byWorker].map(([workerId, worker]) => ({ workerId, worker, hostRoot: roots.get(workerId) }));
}
async function resolvedWorkerDatabases(config: NodeRuntimeConfig): Promise<readonly { readonly workerId: string; readonly hostRoot: string | undefined; readonly database: string | null }[]> {
  return Promise.all(workerCandidates(config).map(async ({ workerId, worker, hostRoot }) => {
    if (hostRoot === undefined) return { workerId, hostRoot, database: null };
    try { return { workerId, hostRoot, database: await workerDatabase(worker, hostRoot) }; }
    catch { return { workerId, hostRoot, database: null }; }
  }));
}
/**
 * A resolved database counts only when it is the sole candidate that resolved to that exact file (never
 * the Node's own journal, and never two Workers claiming the same file) -- the raw count's own ambiguity
 * rule. `unknownDatabases`'s existing candidates that fail it are `unknownWorkerEvidence`, unchanged.
 */
function unambiguousDatabases(config: NodeRuntimeConfig, candidates: readonly { readonly workerId: string; readonly hostRoot: string | undefined; readonly database: string | null }[]): Map<string, string> {
  const frequencies = new Map<string, number>();
  for (const { database } of candidates) if (database !== null) frequencies.set(database, (frequencies.get(database) ?? 0) + 1);
  const resolved = new Map<string, string>();
  for (const candidate of candidates) {
    if (candidate.database === null || candidate.database === resolve(config.database) || frequencies.get(candidate.database) !== 1) continue;
    resolved.set(candidate.workerId, candidate.database);
  }
  return resolved;
}
async function verifiedHostRootDirectory(hostRoot: string): Promise<boolean> {
  try {
    const direct = await lstat(hostRoot), canonical = await realpath(hostRoot);
    const uid = process.getuid?.() ?? direct.uid;
    return !direct.isSymbolicLink() && direct.isDirectory() && direct.uid === uid && canonical === resolve(hostRoot) && (direct.mode & 0o077) === 0;
  } catch { return false; }
}

/** Read-only, fail-closed evidence from every distinct configured Worker. */
export async function inspectNodeWorkerBlockers(config: NodeRuntimeConfig): Promise<NodeWorkerBlockerCounts> {
  const candidates = await resolvedWorkerDatabases(config);
  const resolved = unambiguousDatabases(config, candidates);
  let activeWorkerRuns = 0, pendingWorkerOutbox = 0, unknownWorkerEvidence = 0;
  for (const candidate of candidates) {
    const database = resolved.get(candidate.workerId);
    if (database === undefined) { unknownWorkerEvidence += 1; continue; }
    try {
      const evidence = inspectWorkerDatabase(database);
      activeWorkerRuns += evidence.activeWorkerRuns;
      pendingWorkerOutbox += evidence.pendingWorkerOutbox;
    } catch { unknownWorkerEvidence += 1; }
  }
  return { activeWorkerRuns, pendingWorkerOutbox, unknownWorkerEvidence };
}

/**
 * The Worker host root an operator's acknowledgement may safely write into for `workerId` -- only when
 * `workerDatabase()` passes and the host root directory itself is a real, owner-private, non-symlinked
 * directory. `null` otherwise; never throws.
 */
export async function verifiedWorkerHostRoot(config: NodeRuntimeConfig, workerId: string): Promise<{ readonly hostRoot: string; readonly database: string } | null> {
  const candidates = await resolvedWorkerDatabases(config);
  const resolved = unambiguousDatabases(config, candidates);
  const database = resolved.get(workerId);
  const hostRoot = candidates.find((candidate) => candidate.workerId === workerId)?.hostRoot;
  if (database === undefined || hostRoot === undefined || !(await verifiedHostRootDirectory(hostRoot))) return null;
  return { hostRoot, database };
}

function openWorkerDatabaseForRunEvidence(path: string): DatabaseSync {
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    database.exec("PRAGMA query_only=ON");
    const integrity = database.prepare("PRAGMA integrity_check").get() as { readonly integrity_check?: unknown } | undefined;
    if (integrity?.integrity_check !== "ok") throw new Error("worker database integrity failure");
    assertWorkerSchema(database);
    return database;
  } catch (error) {
    database.close();
    throw error;
  }
}

const WORKER_RUN_STATES: ReadonlySet<string> = new Set(["reserved", "binding", "accepted", "running", "succeeded", "failed", "observational"]);

export interface NodeWorkerRunQuery { readonly workerId: string; readonly runId: string; readonly commandId: string; }
export interface NodeWorkerRunEvidence {
  readonly workerId: string;
  readonly runId: string;
  readonly commandId: string;
  readonly state: "reserved" | "binding" | "accepted" | "running" | "succeeded" | "failed" | "observational" | "absent" | "unknown";
  readonly intentDigest: string | null;
  /** The recorded cause when the Worker isolated this Run. */
  readonly observationalCause: string | null;
  readonly recorded: boolean;
}

/** Read-only Worker-side evidence for a fixed list of `(workerId, runId, commandId)` -- `list` and `acknowledge` share it. */
export async function inspectNodeWorkerRuns(config: NodeRuntimeConfig, entries: readonly NodeWorkerRunQuery[]): Promise<readonly NodeWorkerRunEvidence[]> {
  const candidates = await resolvedWorkerDatabases(config);
  const resolved = unambiguousDatabases(config, candidates);
  const open = new Map<string, DatabaseSync>();
  const unknown = (entry: NodeWorkerRunQuery): NodeWorkerRunEvidence => ({ ...entry, state: "unknown", intentDigest: null, observationalCause: null, recorded: false });
  try {
    return entries.map((entry) => {
      const path = resolved.get(entry.workerId);
      if (path === undefined) return unknown(entry);
      let database = open.get(path);
      if (database === undefined) {
        try { database = openWorkerDatabaseForRunEvidence(path); open.set(path, database); }
        catch { return unknown(entry); }
      }
      try {
        const run = database.prepare("SELECT state, intent_digest FROM worker_runs WHERE run_id=?").get(entry.runId) as { readonly state?: unknown; readonly intent_digest?: unknown } | undefined;
        if (run === undefined) return { ...entry, state: "absent", intentDigest: null, observationalCause: null, recorded: false };
        const state = typeof run.state === "string" && WORKER_RUN_STATES.has(run.state) ? run.state as NodeWorkerRunEvidence["state"] : "unknown";
        const commandRow = database.prepare("SELECT response_json FROM worker_commands WHERE command_id=?").get(entry.commandId) as { readonly response_json?: unknown } | undefined;
        const recorded = commandRow !== undefined && commandRow.response_json !== null && commandRow.response_json !== undefined;
        const observation = database.prepare("SELECT cause FROM worker_run_observations WHERE run_id=?").get(entry.runId) as { readonly cause?: unknown } | undefined;
        const observationalCause = typeof observation?.cause === "string" ? observation.cause : null;
        return { workerId: entry.workerId, runId: entry.runId, commandId: entry.commandId, state, intentDigest: typeof run.intent_digest === "string" ? run.intent_digest : null, observationalCause, recorded };
      } catch { return unknown(entry); }
    });
  } finally { for (const database of open.values()) database.close(); }
}

/**
 * How many of the Node gate's `activeWorkerRuns` (raw, via `inspectNodeWorkerBlockers`) a recorded
 * Worker-side acknowledgement now releases. Matches a stuck row (`reserved`/`observational`) to a record in
 * JavaScript, on `runId`, `workerIntentDigest` and `acknowledgedState` together -- never SQL `NOT IN`
 * (a single NULL in that sub-select would release every row). A candidate the raw count put into
 * `unknownWorkerEvidence` contributes 0. An invalid acknowledgement record file fails closed: it throws, so
 * this candidate is never silently treated as releasing nothing.
 */
export async function releasedWorkerRuns(config: NodeRuntimeConfig): Promise<number> {
  const candidates = await resolvedWorkerDatabases(config);
  const resolved = unambiguousDatabases(config, candidates);
  let released = 0;
  for (const candidate of candidates) {
    const path = resolved.get(candidate.workerId);
    if (path === undefined || candidate.hostRoot === undefined) continue;
    const acknowledgements = await readWorkerStuckRunAcknowledgements(candidate.hostRoot);
    let database: DatabaseSync;
    try { database = openWorkerDatabaseForRunEvidence(path); } catch { continue; }
    try {
      const rows = database.prepare("SELECT run_id, intent_digest, state FROM worker_runs WHERE state IN ('reserved','observational')").all() as { readonly run_id?: unknown; readonly intent_digest?: unknown; readonly state?: unknown }[];
      for (const row of rows) {
        if (row.state !== "reserved" && row.state !== "observational") continue;
        const runId = String(row.run_id), intentDigest = String(row.intent_digest);
        if (acknowledgements.some((ack) => ack.runId === runId && ack.workerIntentDigest === intentDigest && ack.acknowledgedState === row.state)) released += 1;
      }
    } finally { database.close(); }
  }
  return released;
}

export class NodeMaintenanceService implements NodeMaintenanceServicePort {
  readonly #config: NodeRuntimeConfig;
  readonly #journal: MaintenanceJournal;
  readonly #workerEvidence: WorkerEvidenceReader;

  constructor(input: { readonly config: NodeRuntimeConfig; readonly journal: MaintenanceJournal; readonly workerEvidence?: WorkerEvidenceReader }) {
    this.#config = input.config;
    this.#journal = input.journal;
    this.#workerEvidence = input.workerEvidence ?? inspectNodeWorkerBlockers;
  }

  async status(): Promise<NodeMaintenanceStatus> { return this.#snapshot(await this.#journal.maintenanceMode()); }

  async setMode(mode: NodeMaintenanceMode): Promise<NodeMaintenanceStatus> {
    return this.#snapshot(await this.#journal.setMaintenanceMode(mode));
  }

  async #snapshot(mode: NodeMaintenanceMode): Promise<NodeMaintenanceStatus> {
    const [node, worker] = await Promise.all([this.#journal.maintenanceBlockerCounts(), this.#workerEvidence(this.#config)]);
    const blockerCounts: NodeBlockerCounts = { ...node, ...worker };
    return nodeMaintenanceStatus({
      contractVersion: NODE_CONTRACT_VERSION,
      nodeRef: { ref: this.#config.nodeRef, version: this.#config.nodeVersion },
      mode,
      idle: Object.values(blockerCounts).every((count) => count === 0),
      blockerCounts,
    });
  }
}

export function combineNodeBlockers(node: NodeJournalBlockerCounts, worker: NodeWorkerBlockerCounts): NodeBlockerCounts {
  return { ...node, ...worker };
}
