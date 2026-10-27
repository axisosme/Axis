import { lstat, mkdir, readFile, realpath, rename, stat, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

import { HostStateError } from "./host-state.js";
import { LINEAR_INBOX_ACKNOWLEDGEMENT_REASON_LIMIT } from "../relay/linear/inbox.js";
import type { NodeBlockerCounts } from "../shared/router-node.js";

/**
 * The two owner-private, append-only JSON records an operator writes to release a permanently
 * `ambiguous` Node command (and the Worker Run it left stuck) from an `axis upgrade` gate. This module
 * owns both files and their shared validators; nothing here migrates a journal, writes a database, or
 * changes a command/Run's own state. The evidence rows stay exactly as they are -- these records only
 * change what the drain gates count.
 */

const NODE_RECORD_FILENAME = "node-ambiguous-acknowledgements.v1.json";
const WORKER_RECORD_FILENAME = "worker-stuck-run-acknowledgements.v1.json";
const NODE_SCHEMA_VERSION = "axis.node.ambiguous-acknowledgements.v1";
const WORKER_SCHEMA_VERSION = "axis.worker.stuck-run-acknowledgements.v1";

const OPAQUE_REF = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._-]*$/u;
const DIGEST = /^sha256:[a-f0-9]{64}$/u;

/** A command less than this old may still converge on its own. */
export const AMBIGUOUS_ACKNOWLEDGEMENT_MIN_AGE_MS = 900_000;
/** The shared 1..512-character, no-control-character reason limit `axis-cli.ts`'s `acknowledgementReason` already enforces. */
export const STUCK_RUN_ACKNOWLEDGEMENT_REASON_LIMIT = LINEAR_INBOX_ACKNOWLEDGEMENT_REASON_LIMIT;

function invalid(): never { throw new HostStateError("stuck-run acknowledgement record is invalid"); }

function closedRecord(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) invalid();
  const item = value as Record<string, unknown>;
  if (Object.keys(item).sort().join(",") !== [...keys].sort().join(",")) invalid();
  return item;
}
function opaque(value: unknown): string { if (typeof value !== "string" || !OPAQUE_REF.test(value)) invalid(); return value; }
function identifier(value: unknown): string { if (typeof value !== "string" || !IDENTIFIER.test(value)) invalid(); return value; }
function nullableIdentifier(value: unknown): string | null { return value === null ? null : identifier(value); }
function digest(value: unknown): string { if (typeof value !== "string" || !DIGEST.test(value)) invalid(); return value; }
function instant(value: unknown): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value)) || new Date(value).toISOString() !== value) invalid();
  return value;
}
function reason(value: unknown): string | null {
  if (value === null) return null;
  // eslint-disable-next-line no-control-regex
  if (typeof value !== "string" || value.length === 0 || value.length > STUCK_RUN_ACKNOWLEDGEMENT_REASON_LIMIT || /[\u0000-\u001F\u007F]/u.test(value)) invalid();
  return value;
}
function sequence(value: unknown): number { if (!Number.isSafeInteger(value) || (value as number) < 1) invalid(); return value as number; }
function literal<T extends string>(value: unknown, allowed: readonly T[]): T { if (typeof value !== "string" || !(allowed as readonly string[]).includes(value)) invalid(); return value as T; }

export type StuckRunAcknowledgementOperation = "start" | "continue" | "cancel" | "steer";
export type StuckRunAcknowledgementWorkerRelease = "recorded" | "not_needed" | "not_configured";
export type StuckRunAcknowledgedState = "reserved" | "observational";

export interface NodeAmbiguousAcknowledgementRecord {
  readonly sequence: number;
  readonly commandId: string;
  readonly intentDigest: string;
  readonly operation: StuckRunAcknowledgementOperation;
  readonly runId: string | null;
  readonly priorRunId: string | null;
  readonly commandCreatedAt: string;
  readonly workerRelease: StuckRunAcknowledgementWorkerRelease;
  readonly acknowledgedAt: string;
  readonly operatorRef: string;
  readonly authorizationRef: string;
  readonly source: string;
  readonly requestRef: string;
  readonly reason: string | null;
}

/** Today the only `origin.kind`. */
export interface WorkerStuckRunAcknowledgementOrigin {
  readonly kind: "node_ambiguous_command";
  readonly nodeId: string;
  readonly nodeCommandId: string;
}

export interface WorkerStuckRunAcknowledgement {
  readonly sequence: number;
  readonly runId: string;
  readonly workerIntentDigest: string;
  readonly acknowledgedState: StuckRunAcknowledgedState;
  readonly observationalCause: string | null;
  readonly origin: WorkerStuckRunAcknowledgementOrigin;
  readonly acknowledgedAt: string;
  readonly operatorRef: string;
  readonly authorizationRef: string;
  readonly source: string;
  readonly requestRef: string;
  readonly reason: string | null;
}

const NODE_ENTRY_KEYS: readonly (keyof NodeAmbiguousAcknowledgementRecord)[] = [
  "sequence", "commandId", "intentDigest", "operation", "runId", "priorRunId", "commandCreatedAt",
  "workerRelease", "acknowledgedAt", "operatorRef", "authorizationRef", "source", "requestRef", "reason",
];
const WORKER_ENTRY_KEYS: readonly (keyof WorkerStuckRunAcknowledgement)[] = [
  "sequence", "runId", "workerIntentDigest", "acknowledgedState", "observationalCause", "origin",
  "acknowledgedAt", "operatorRef", "authorizationRef", "source", "requestRef", "reason",
];

function validateNodeEntry(value: unknown): NodeAmbiguousAcknowledgementRecord {
  const item = closedRecord(value, NODE_ENTRY_KEYS);
  return {
    sequence: sequence(item.sequence),
    commandId: identifier(item.commandId),
    intentDigest: digest(item.intentDigest),
    operation: literal(item.operation, ["start", "continue", "cancel", "steer"]),
    runId: nullableIdentifier(item.runId),
    priorRunId: nullableIdentifier(item.priorRunId),
    commandCreatedAt: instant(item.commandCreatedAt),
    workerRelease: literal(item.workerRelease, ["recorded", "not_needed", "not_configured"]),
    acknowledgedAt: instant(item.acknowledgedAt),
    operatorRef: opaque(item.operatorRef),
    authorizationRef: opaque(item.authorizationRef),
    source: opaque(item.source),
    requestRef: opaque(item.requestRef),
    reason: reason(item.reason),
  };
}
function validateWorkerOrigin(value: unknown): WorkerStuckRunAcknowledgementOrigin {
  const item = closedRecord(value, ["kind", "nodeId", "nodeCommandId"]);
  return { kind: literal(item.kind, ["node_ambiguous_command"]), nodeId: identifier(item.nodeId), nodeCommandId: identifier(item.nodeCommandId) };
}
function validateWorkerEntry(value: unknown): WorkerStuckRunAcknowledgement {
  const item = closedRecord(value, WORKER_ENTRY_KEYS);
  return {
    sequence: sequence(item.sequence),
    runId: identifier(item.runId),
    workerIntentDigest: digest(item.workerIntentDigest),
    acknowledgedState: literal(item.acknowledgedState, ["reserved", "observational"]),
    observationalCause: item.observationalCause === null ? null : identifier(item.observationalCause),
    origin: validateWorkerOrigin(item.origin),
    acknowledgedAt: instant(item.acknowledgedAt),
    operatorRef: opaque(item.operatorRef),
    authorizationRef: opaque(item.authorizationRef),
    source: opaque(item.source),
    requestRef: opaque(item.requestRef),
    reason: reason(item.reason),
  };
}

async function ownerPrivateFilePath(path: string): Promise<string> {
  const direct = await lstat(path);
  const canonical = await realpath(path);
  const metadata = await stat(canonical);
  if (direct.isSymbolicLink() || canonical !== resolve(path) || !metadata.isFile()
    || metadata.uid !== (process.getuid?.() ?? metadata.uid) || (metadata.mode & 0o077) !== 0) invalid();
  return canonical;
}

async function readAcknowledgements<T extends { readonly sequence: number }>(
  path: string, schemaVersion: string, validateEntry: (value: unknown) => T, matchKey: (entry: T) => string,
): Promise<readonly T[]> {
  let canonical: string;
  try { canonical = await ownerPrivateFilePath(path); }
  catch (error) {
    if (error instanceof HostStateError) throw error;
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    invalid();
  }
  let parsed: unknown;
  try { parsed = JSON.parse(await readFile(canonical, "utf8")); } catch { invalid(); }
  const document = closedRecord(parsed, ["schemaVersion", "acknowledgements"]);
  if (document.schemaVersion !== schemaVersion || !Array.isArray(document.acknowledgements)) invalid();
  const seen = new Set<string>(); let previousSequence = 0; const result: T[] = [];
  for (const candidate of document.acknowledgements) {
    const entry = validateEntry(candidate);
    if (entry.sequence <= previousSequence) invalid();
    previousSequence = entry.sequence;
    const key = matchKey(entry);
    if (seen.has(key)) invalid();
    seen.add(key);
    result.push(entry);
  }
  return result;
}

async function atomicWriteRecord(path: string, data: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = join(dirname(path), `.${basename(path)}.${process.pid}.${Date.now()}`);
  await writeFile(temporary, data, { mode: 0o600, flag: "wx" });
  await rename(temporary, path);
}

/**
 * Appends one record, or returns the existing one unmodified (`applied: false`) when a prior call already
 * recorded the same match key. Callers must already hold `acquireLock()`: the Node and every Worker host
 * root on one machine share a single lock keyed by the user's home, so one lock covers both writes.
 */
async function appendAcknowledgement<T extends { readonly sequence: number }>(options: {
  readonly path: string;
  readonly schemaVersion: string;
  readonly read: () => Promise<readonly T[]>;
  readonly matchKey: (entry: T) => string;
  readonly input: Omit<T, "sequence">;
}): Promise<{ readonly record: T; readonly applied: boolean }> {
  const existing = await options.read();
  const key = options.matchKey({ ...(options.input as T), sequence: 0 });
  const found = existing.find((entry) => options.matchKey(entry) === key);
  if (found !== undefined) return { record: found, applied: false };
  const record = { ...options.input, sequence: existing.length + 1 } as T;
  const document = { schemaVersion: options.schemaVersion, acknowledgements: [...existing, record] };
  await atomicWriteRecord(options.path, `${JSON.stringify(document)}\n`);
  const reread = await options.read();
  const written = reread.find((entry) => options.matchKey(entry) === key);
  if (written === undefined) throw new HostStateError("stuck-run acknowledgement record could not be verified after write");
  return { record: written, applied: true };
}

function nodeMatchKey(entry: { readonly commandId: string; readonly intentDigest: string }): string { return `${entry.commandId}\u0000${entry.intentDigest}`; }
function workerMatchKey(entry: { readonly runId: string; readonly workerIntentDigest: string; readonly acknowledgedState: string }): string {
  return `${entry.runId}\u0000${entry.workerIntentDigest}\u0000${entry.acknowledgedState}`;
}

export async function readNodeAmbiguousAcknowledgements(hostRoot: string): Promise<readonly NodeAmbiguousAcknowledgementRecord[]> {
  return readAcknowledgements(join(hostRoot, NODE_RECORD_FILENAME), NODE_SCHEMA_VERSION, validateNodeEntry, nodeMatchKey);
}
export async function readWorkerStuckRunAcknowledgements(hostRoot: string): Promise<readonly WorkerStuckRunAcknowledgement[]> {
  return readAcknowledgements(join(hostRoot, WORKER_RECORD_FILENAME), WORKER_SCHEMA_VERSION, validateWorkerEntry, workerMatchKey);
}
export async function appendNodeAmbiguousAcknowledgement(
  hostRoot: string, input: Omit<NodeAmbiguousAcknowledgementRecord, "sequence">,
): Promise<{ readonly record: NodeAmbiguousAcknowledgementRecord; readonly applied: boolean }> {
  return appendAcknowledgement({
    path: join(hostRoot, NODE_RECORD_FILENAME), schemaVersion: NODE_SCHEMA_VERSION,
    read: () => readNodeAmbiguousAcknowledgements(hostRoot), matchKey: nodeMatchKey, input,
  });
}
export async function appendWorkerStuckRunAcknowledgement(
  hostRoot: string, input: Omit<WorkerStuckRunAcknowledgement, "sequence">,
): Promise<{ readonly record: WorkerStuckRunAcknowledgement; readonly applied: boolean }> {
  return appendAcknowledgement({
    path: join(hostRoot, WORKER_RECORD_FILENAME), schemaVersion: WORKER_SCHEMA_VERSION,
    read: () => readWorkerStuckRunAcknowledgements(hostRoot), matchKey: workerMatchKey, input,
  });
}

export interface NodeAmbiguousReleaseCounts { readonly ambiguousCommands: number; readonly activeWorkerRuns: number; }

/** The Node upgrade gate's refusal. `blockers` is always the effective (post-release) 8 counts. */
export class NodeUpgradeBlockedError extends HostStateError {
  readonly blockers: NodeBlockerCounts;
  readonly acknowledged: NodeAmbiguousReleaseCounts;
  /** Evidence detail, not part of the printed JSON envelope: the still-blocking commands' ids. */
  readonly unacknowledgedAmbiguous: readonly string[];
  constructor(message: string, detail: { readonly blockers: NodeBlockerCounts; readonly acknowledged: NodeAmbiguousReleaseCounts; readonly unacknowledgedAmbiguous: readonly string[] }) {
    super(message);
    this.blockers = detail.blockers; this.acknowledged = detail.acknowledged; this.unacknowledgedAmbiguous = detail.unacknowledgedAmbiguous;
  }
}

/** A Worker host's own `axis upgrade` gate refusal, when `worker_runs` is what blocked it. */
export class WorkerUpgradeBlockedError extends HostStateError {
  readonly stuckRuns: number;
  readonly releasedStuckRuns: number;
  readonly otherBlockingTables: readonly string[];
  constructor(detail: { readonly stuckRuns: number; readonly releasedStuckRuns: number; readonly otherBlockingTables: readonly string[] }) {
    super("active work or outbox prevents upgrade");
    this.stuckRuns = detail.stuckRuns; this.releasedStuckRuns = detail.releasedStuckRuns; this.otherBlockingTables = detail.otherBlockingTables;
  }
}

/**
 * Subtracts what has been released from the raw 8 counts `SqliteNodeJournal.inspectMaintenance` and
 * `inspectNodeWorkerBlockers` produced. A negative result means the evidence changed between the raw read
 * and the release read (a race, not a corruption): fail closed rather than under- or over-report.
 */
export function effectiveNodeBlockers(raw: NodeBlockerCounts, released: NodeAmbiguousReleaseCounts): NodeBlockerCounts {
  const ambiguousCommands = raw.ambiguousCommands - released.ambiguousCommands;
  const activeWorkerRuns = raw.activeWorkerRuns - released.activeWorkerRuns;
  if (ambiguousCommands < 0 || activeWorkerRuns < 0) {
    throw new NodeUpgradeBlockedError("drain evidence changed while it was read; run axis upgrade again", { blockers: raw, acknowledged: released, unacknowledgedAmbiguous: [] });
  }
  return { ...raw, ambiguousCommands, activeWorkerRuns };
}
