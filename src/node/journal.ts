import { createHash, randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

import {
  NODE_ERROR_STATUS,
  NodeContractError,
  nodeEventDigest,
  nodeEventId,
  nodeEventRunId,
  validateNodeEvent,
  type NodeDurableRoute,
  type NodeErrorCode,
  type NodeTarget,
} from "../shared/router-node.js";
import {
  NODE_COMMAND_CONTRACT_VERSION,
  canonicalNodeCommandIntent,
  completeNodeWorkerCommand,
  nodeCommandIntentDigest,
  validateNodeCommandCandidate,
  validateNodeCommandRequest,
  validateNodeCommandResponse,
  validateNodePriorRoute,
  validateNodeReceiptQuery,
  validateNodeReceipt,
  nodeReceiptExpectation,
  validateNodeReservationEvidence,
  type NodeAuthenticatedContext,
  type NodeCommandRequest,
  type NodePriorRoute,
  type NodeReceiptQuery,
  type NodeReceipt,
  type NodeReservationEvidence,
  type NodeWorkerInventoryPort,
} from "../shared/router-node-commands.js";
import { isWorkerOpaqueIdentifier, type RouterRunLifecycleEnvelope, type WorkerCommandResponse, type WorkerVersionedReference } from "../shared/router-worker.js";
import { runtimeSourceSessionIdentity, type RuntimeSourceSystem, type SourceRef } from "../shared/runtime-boundary.js";
import { validateWorkerSelection } from "../shared/worker-selection.js";

const SCHEMA_VERSION = "1";
const DELIVERY_DIAGNOSTIC_VERSION = "1.0.0";
const DEFAULT_BUSY_TIMEOUT_MS = 5_000;
const eventStates = ["pending", "delivered", "quarantined"] as const;
const commandStates = ["reserved", "dispatching", "accepted", "rejected", "ambiguous"] as const;
const deliveryFailureCodes = {
  "axis.node.event_delivery_unavailable": true,
  "axis.node.event_protocol_conflict": false,
} as const;
const metadataKeys = [
  "command_count",
  "command_high_water_append_order",
  "command_tail_digest",
  "event_count",
  "event_high_water_global_order",
  "event_tail_digest",
  "maintenance_mode",
  "route_count",
  "route_high_water_append_order",
  "route_tail_digest",
  "schema_version",
] as const;
type EventState = typeof eventStates[number];
type CommandState = typeof commandStates[number];
type NodeAcceptedReceipt = Extract<NodeReceipt, { readonly status: "accepted" }>;
type NodeAcceptedEventReceipt = NodeAcceptedReceipt & {
  readonly result: Extract<WorkerCommandResponse, { readonly status: "accepted" }>;
};
export type NodeMaintenanceMode = "drain" | "resume";
type JournalPopulation = "command" | "route";
export type NodeJournalDeliveryFailureCode = keyof typeof deliveryFailureCodes;
type JsonRecord = Readonly<Record<string, unknown>>;
type Row = Record<string, unknown>;

const schemaStatements = [
  `CREATE TABLE metadata (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
  ) STRICT`,
  `CREATE TABLE commands (
    command_id TEXT PRIMARY KEY NOT NULL,
    append_order INTEGER UNIQUE NOT NULL CHECK (append_order > 0),
    append_chain_digest TEXT NOT NULL,
    lifecycle_digest TEXT NOT NULL,
    intent_digest TEXT NOT NULL,
    intent_json TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('start','continue','cancel','steer')),
    node_ref_json TEXT NOT NULL,
    requested_selection_json TEXT NOT NULL,
    reservation_json TEXT NOT NULL,
    run_id TEXT,
    prior_run_id TEXT,
    state TEXT NOT NULL CHECK (state IN ('reserved','dispatching','accepted','rejected','ambiguous')),
    receipt_json TEXT NOT NULL,
    dispatch_owner_pid INTEGER,
    dispatch_owner_token TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((state = 'dispatching' AND dispatch_owner_pid > 0 AND dispatch_owner_token IS NOT NULL)
      OR (state != 'dispatching' AND dispatch_owner_pid IS NULL AND dispatch_owner_token IS NULL)),
    CHECK ((operation IN ('cancel','steer') AND run_id IS NULL AND prior_run_id IS NOT NULL)
      OR (operation = 'start' AND run_id IS NOT NULL AND prior_run_id IS NULL)
      OR (operation = 'continue' AND run_id IS NOT NULL AND prior_run_id IS NOT NULL))
  ) STRICT`,
  `CREATE TABLE routes (
    run_id TEXT PRIMARY KEY NOT NULL,
    append_order INTEGER UNIQUE NOT NULL CHECK (append_order > 0),
    append_chain_digest TEXT NOT NULL,
    command_id TEXT UNIQUE NOT NULL REFERENCES commands(command_id) ON DELETE RESTRICT,
    route_digest TEXT NOT NULL,
    node_ref_json TEXT NOT NULL,
    requested_selection_json TEXT NOT NULL,
    reservation_json TEXT NOT NULL,
    task_id TEXT NOT NULL,
    source_system TEXT NOT NULL CHECK (source_system IN ('linear')),
    source_session_id TEXT NOT NULL,
    source_ref_json TEXT NOT NULL,
    session_binding_id TEXT NOT NULL,
    prior_run_id TEXT,
    workspace_policy_ref_json TEXT,
    created_at TEXT NOT NULL
  ) STRICT`,
  `CREATE UNIQUE INDEX routes_first_source_session
    ON routes(source_system,source_session_id)
    WHERE prior_run_id IS NULL`,
  `CREATE TABLE event_outbox (
    event_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES routes(run_id) ON DELETE RESTRICT,
    event_digest TEXT NOT NULL,
    event_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    global_order INTEGER UNIQUE NOT NULL CHECK (global_order > 0),
    append_position_digest TEXT NOT NULL,
    lifecycle_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','delivered','quarantined')),
    delivery_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempt_count >= 0),
    last_delivery_failure_json TEXT,
    delivered_at TEXT,
    quarantine_code TEXT,
    terminal_at TEXT,
    UNIQUE(run_id, ordinal),
    CHECK ((delivery_attempt_count = 0 AND last_delivery_failure_json IS NULL)
      OR (delivery_attempt_count > 0 AND last_delivery_failure_json IS NOT NULL)),
    CHECK ((state = 'pending' AND delivered_at IS NULL AND quarantine_code IS NULL AND terminal_at IS NULL)
      OR (state = 'delivered' AND delivered_at IS NOT NULL AND quarantine_code IS NULL AND terminal_at = delivered_at)
      OR (state = 'quarantined' AND delivered_at IS NULL AND quarantine_code IS NOT NULL AND terminal_at IS NOT NULL))
  ) STRICT`,
] as const;

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const item = value as Record<string, unknown>;
    return `{${Object.keys(item).sort().map((key) => `${JSON.stringify(key)}:${canonical(item[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
function digest(domain: string, value: unknown): `sha256:${string}` {
  return `sha256:${createHash("sha256").update(domain).update(canonical(value)).digest("hex")}`;
}
function eventAppendPositionDigest(input: { readonly eventId: string; readonly runId: string; readonly eventDigest: string; readonly ordinal: number; readonly globalOrder: number }): `sha256:${string}` {
  return digest("axis.node.event.append-position.v1\0", input);
}
function eventOutboxTailDigest(input: { readonly eventCount: number; readonly highWaterGlobalOrder: number; readonly tailAppendPositionDigest: string | null }): `sha256:${string}` {
  return digest("axis.node.event.tail.v1\0", input);
}
function commandIntentDigest(intent: unknown): `sha256:${string}` { return digest("axis.node.command.intent.v1\0", intent); }
function commandAppendChainAnchor(): `sha256:${string}` { return digest("axis.node.command.append-chain.v1\0", null); }
function commandAppendChainDigest(input: { readonly previousTailDigest: string; readonly appendOrder: number; readonly commandId: string; readonly intentDigest: string }): `sha256:${string}` {
  return digest("axis.node.command.append-chain.v1\0", input);
}
function routeAppendChainAnchor(): `sha256:${string}` { return digest("axis.node.route.append-chain.v1\0", null); }
function routeAppendChainDigest(input: { readonly previousTailDigest: string; readonly appendOrder: number; readonly runId: string; readonly commandId: string; readonly routeDigest: string }): `sha256:${string}` {
  return digest("axis.node.route.append-chain.v1\0", input);
}
function routeDigest(route: NodeJournalRunRoute): `sha256:${string}` { return digest("axis.node.route.v1\0", route); }
function commandLifecycleDigest(input: { readonly commandId: string; readonly runId: string | null; readonly priorRunId: string | null; readonly state: CommandState; readonly createdAt: string; readonly updatedAt: string; readonly reservationJson: string; readonly receiptJson: string; readonly dispatchOwnerPid: number | null; readonly dispatchOwnerToken: string | null }): `sha256:${string}` {
  return digest("axis.node.command.lifecycle.v1\0", input);
}
function eventLifecycleDigest(input: { readonly eventId: string; readonly runId: string; readonly eventDigest: string; readonly recordedAt: string; readonly state: EventState; readonly deliveryAttemptCount: number; readonly lastDeliveryFailureJson: string | null; readonly deliveredAt: string | null; readonly quarantineCode: string | null; readonly terminalAt: string | null }): `sha256:${string}` {
  return digest("axis.node.event.lifecycle.v1\0", input);
}
/**
 * Drain admits cancel and steer: both act on a Run already executing and mint no route. A fresh start or
 * continue is refused with the definite `axis.node.draining` before anything is written; a replayed one still
 * `reserved` or `dispatching` answers `axis.node.recovery_required`, since it may already have reached a Worker.
 */
function drainExempt(command: "start" | "continue" | "cancel" | "steer"): boolean { return command === "cancel" || command === "steer"; }
function normalizeSql(value: string): string { return value.replace(/\s+/gu, " ").trim(); }
function storageError(): NodeContractError { return new NodeContractError("axis.node.storage_unavailable"); }
function fail(code: NodeErrorCode = "axis.node.storage_unavailable"): never { throw new NodeContractError(code); }
function processAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
}
function timestamp(value: string): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value)) || new Date(value).toISOString() !== value) fail();
  return value;
}
function sourceSystem(value: unknown): RuntimeSourceSystem {
  if (value !== "linear") fail();
  return value;
}
function identifier(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(value)) fail();
  return value;
}
function callerIdentifier(value: unknown): string {
  if (!isWorkerOpaqueIdentifier(value)) fail("axis.node.malformed");
  return value;
}
function parseRecord(value: unknown): JsonRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) fail();
  return value as JsonRecord;
}
function parseCanonicalJson(value: unknown): unknown {
  if (typeof value !== "string") fail();
  try {
    const parsed = JSON.parse(value) as unknown;
    if (canonical(parsed) !== value) fail();
    return parsed;
  } catch (error) {
    if (error instanceof NodeContractError) throw error;
    fail();
  }
}
function parseCanonicalRecord(value: unknown): JsonRecord { return parseRecord(parseCanonicalJson(value)); }
function versionedReference(value: unknown): WorkerVersionedReference {
  const item = parseRecord(value);
  if (Object.keys(item).length !== 2) fail();
  return { ref: identifier(item.ref), version: identifier(item.version) };
}
function same(left: unknown, right: unknown): boolean { return canonical(left) === canonical(right); }
function changes(result: unknown): number { return Number((result as { readonly changes?: number }).changes ?? 0); }
function quarantineCode(value: unknown): NodeErrorCode {
  if (typeof value !== "string" || !Object.hasOwn(NODE_ERROR_STATUS, value)) fail();
  return value as NodeErrorCode;
}
function deliveryFailureCode(value: unknown): NodeJournalDeliveryFailureCode {
  if (typeof value !== "string" || !Object.hasOwn(deliveryFailureCodes, value)) fail();
  return value as NodeJournalDeliveryFailureCode;
}
function parseDeliveryFailure(value: unknown): NodeJournalDeliveryFailure {
  const item = parseCanonicalRecord(value);
  if (Object.keys(item).sort().join(",") !== "code,contractVersion,failedAt,retryable" || item.contractVersion !== DELIVERY_DIAGNOSTIC_VERSION) fail();
  const code = deliveryFailureCode(item.code), failedAt = timestamp(String(item.failedAt));
  if (item.retryable !== deliveryFailureCodes[code]) fail();
  return { contractVersion: DELIVERY_DIAGNOSTIC_VERSION, code, retryable: deliveryFailureCodes[code], failedAt };
}
function eventEnvelope(event: RouterRunLifecycleEnvelope) { return "deliveryType" in event ? event.envelope : event; }
function immutableRunIdentity(value: unknown): JsonRecord {
  const run = parseRecord(value);
  return {
    contractVersion: run.contractVersion,
    runId: run.runId,
    taskId: run.taskId,
    attemptId: run.attemptId,
    sessionBindingId: run.sessionBindingId,
    correlationId: run.correlationId,
  };
}

export interface NodeAcceptedRunRoute extends NodeDurableRoute {
  readonly commandId: string;
  readonly sessionBindingId: string;
  readonly priorRunId: string | null;
  readonly workspacePolicyRef: WorkerVersionedReference | null;
  readonly createdAt: string;
}
export interface NodeJournalRunRoute {
  readonly commandId: string;
  readonly nodeRef: WorkerVersionedReference;
  readonly requestedSelection: NodeCommandRequest["target"]["selection"];
  readonly reservation: NodeReservationEvidence;
  readonly taskId: string;
  readonly sourceSystem: RuntimeSourceSystem;
  readonly sourceSessionId: string;
  readonly sourceRef: SourceRef;
  readonly runId: string;
  readonly sessionBindingId: string;
  readonly priorRunId: string | null;
  readonly workspacePolicyRef: WorkerVersionedReference | null;
  readonly createdAt: string;
}
export interface NodeJournalCommandRecord {
  readonly commandId: string;
  readonly intentDigest: string;
  readonly canonicalIntent: JsonRecord;
  readonly operation: "start" | "continue" | "cancel" | "steer";
  readonly nodeRef: WorkerVersionedReference;
  readonly requestedSelection: NodeCommandRequest["target"]["selection"];
  readonly reservation: NodeReservationEvidence;
  readonly runId: string | null;
  readonly priorRunId: string | null;
  readonly state: CommandState;
  readonly receipt: Exclude<NodeReceipt, { readonly status: "not_found" }>;
  readonly dispatchOwnerPid: number | null;
  readonly dispatchOwnerToken: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
}
/** A Worker response already validated against its `ambiguous` command, waiting for the compare-and-set write. */
export interface NodeAmbiguousConvergence {
  readonly commandId: string;
  readonly receiptJson: string;
}
export interface NodeJournalEventRecord {
  readonly eventId: string;
  readonly runId: string;
  readonly digest: string;
  readonly event: RouterRunLifecycleEnvelope;
  readonly recordedAt: string;
  readonly ordinal: number;
  readonly globalOrder: number;
  readonly state: EventState;
  readonly deliveryAttemptCount: number;
  readonly lastDeliveryFailure: NodeJournalDeliveryFailure | null;
  readonly deliveredAt: string | null;
  readonly quarantineCode: NodeErrorCode | null;
  readonly terminalAt: string | null;
}
export interface NodeJournalDeliveryFailure {
  readonly contractVersion: typeof DELIVERY_DIAGNOSTIC_VERSION;
  readonly code: NodeJournalDeliveryFailureCode;
  readonly retryable: boolean;
  readonly failedAt: string;
}
export interface NodeJournalBlockerCounts {
  readonly reservedCommands: number;
  readonly dispatchingCommands: number;
  readonly ambiguousCommands: number;
  readonly pendingNodeEvents: number;
  readonly quarantinedNodeEvents: number;
}
/** One command stuck `ambiguous`, and the route evidence an operator release needs. */
export interface NodeAmbiguousCommandEvidence {
  readonly commandId: string;
  readonly operation: "start" | "continue" | "cancel" | "steer";
  readonly runId: string | null;
  readonly priorRunId: string | null;
  readonly intentDigest: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly workerId: string;
  readonly taskId: string;
  readonly sourceSystem: RuntimeSourceSystem;
  readonly sourceSessionId: string;
}
/** One acknowledged command id's current state, once it is no longer `ambiguous`. */
export interface NodeResolvedAmbiguousCommand {
  readonly commandId: string;
  readonly state: "reserved" | "dispatching" | "accepted" | "rejected" | "ambiguous" | "absent";
}
export interface SqliteNodeJournalOptions { readonly busyTimeoutMs?: number; }

/** Package-private Node persistence. Dispatch and event delivery must happen outside these transactions. */
export class SqliteNodeJournal {
  readonly #db: DatabaseSync;
  readonly #ownerToken: string;
  readonly #commandCache = new Map<string, NodeJournalCommandRecord>();
  #dataVersion: number | null = null;
  #closed = false;
  #pendingOperations = 0;
  #operationTail: Promise<void> = Promise.resolve();

  private constructor(database: DatabaseSync) { this.#db = database; this.#ownerToken = randomUUID(); }

  /** Opens or creates the journal. Any other schema fails `axis.node.storage_unavailable`. */
  static async open(filename: string, options: SqliteNodeJournalOptions = {}): Promise<SqliteNodeJournal> {
    return SqliteNodeJournal.#open(filename, options, false);
  }

  /** Explicit crash recovery. The caller must use this only after the former Node process has stopped. */
  static async recover(filename: string, options: SqliteNodeJournalOptions = {}): Promise<SqliteNodeJournal> {
    return SqliteNodeJournal.#open(filename, options, true);
  }

  static async #open(filename: string, options: SqliteNodeJournalOptions, recover: boolean): Promise<SqliteNodeJournal> {
    const busyTimeoutMs = options.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS;
    if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0 || busyTimeoutMs > 60_000) throw storageError();
    let database: DatabaseSync | undefined;
    try {
      database = new DatabaseSync(filename);
      database.exec(`PRAGMA busy_timeout=${busyTimeoutMs}; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;`);
      database.exec("PRAGMA journal_mode=WAL;");
      const journal = new SqliteNodeJournal(database);
      await journal.#initialize(recover);
      return journal;
    } catch {
      try { database?.close(); } catch { /* best effort */ }
      throw storageError();
    }
  }

  /** Read-only stopped-service evidence. It never recovers journal rows. */
  static async inspectMaintenance(filename: string, options: SqliteNodeJournalOptions = {}): Promise<{ readonly mode: NodeMaintenanceMode; readonly blockerCounts: NodeJournalBlockerCounts }> {
    const busyTimeoutMs = options.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS;
    if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0 || busyTimeoutMs > 60_000) throw storageError();
    let database: DatabaseSync | undefined;
    try {
      database = new DatabaseSync(filename, { readOnly: true });
      database.exec(`PRAGMA busy_timeout=${busyTimeoutMs}; PRAGMA foreign_keys=ON; PRAGMA query_only=ON;`);
      const journal = new SqliteNodeJournal(database);
      journal.#validateSchema();
      await journal.#validateRows();
      const result = { mode: journal.#maintenanceMode(), blockerCounts: journal.#maintenanceBlockerCounts() } as const;
      journal.#closed = true;
      database.close();
      return result;
    } catch {
      try { database?.close(); } catch { /* best effort */ }
      throw storageError();
    }
  }

  /**
   * Read-only evidence for every `ambiguous` command, in `append_order`, plus the current state of each
   * `acknowledgedCommandIds` entry that is not ambiguous. Opened like `inspectMaintenance`; the reads share one
   * deferred transaction, since the service may still be live.
   */
  static async inspectAmbiguousCommands(
    filename: string,
    options: SqliteNodeJournalOptions & { readonly acknowledgedCommandIds?: ReadonlySet<string> } = {},
  ): Promise<{ readonly ambiguous: readonly NodeAmbiguousCommandEvidence[]; readonly resolved: readonly NodeResolvedAmbiguousCommand[] }> {
    const busyTimeoutMs = options.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS;
    if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0 || busyTimeoutMs > 60_000) throw storageError();
    let database: DatabaseSync | undefined;
    try {
      database = new DatabaseSync(filename, { readOnly: true });
      database.exec(`PRAGMA busy_timeout=${busyTimeoutMs}; PRAGMA foreign_keys=ON; PRAGMA query_only=ON;`);
      const journal = new SqliteNodeJournal(database);
      journal.#validateSchema();
      await journal.#validateRows();
      const ambiguous: NodeAmbiguousCommandEvidence[] = [];
      database.exec("BEGIN DEFERRED");
      const rows = database.prepare("SELECT command_id, operation, run_id, prior_run_id, intent_digest, created_at, updated_at, reservation_json FROM commands WHERE state='ambiguous' ORDER BY append_order").all() as Row[];
      for (const row of rows) {
        const operationValue = row.operation;
        if (operationValue !== "start" && operationValue !== "continue" && operationValue !== "cancel" && operationValue !== "steer") fail();
        const operation = operationValue as NodeAmbiguousCommandEvidence["operation"];
        const runId = row.run_id === null ? null : identifier(row.run_id);
        const priorRunId = row.prior_run_id === null ? null : identifier(row.prior_run_id);
        const reservation = validateNodeReservationEvidence(parseCanonicalJson(String(row.reservation_json)));
        // Cancel and steer mint no Run: their route is the prior Run's.
        const routeRunId = operation === "cancel" || operation === "steer" ? priorRunId : runId;
        if (routeRunId === null) fail();
        const route = await journal.#readRoute(routeRunId);
        if (route === null) fail();
        ambiguous.push({
          commandId: identifier(row.command_id),
          operation, runId, priorRunId,
          intentDigest: String(row.intent_digest),
          createdAt: timestamp(String(row.created_at)),
          updatedAt: timestamp(String(row.updated_at)),
          workerId: reservation.target.identity.workerId,
          taskId: route.taskId,
          sourceSystem: route.sourceSystem,
          sourceSessionId: route.sourceSessionId,
        });
      }
      const ambiguousIds = new Set(ambiguous.map((item) => item.commandId));
      const resolved: NodeResolvedAmbiguousCommand[] = [];
      for (const commandId of options.acknowledgedCommandIds ?? []) {
        if (ambiguousIds.has(commandId)) continue;
        const current = database.prepare("SELECT state FROM commands WHERE command_id=?").get(commandId) as { readonly state?: unknown } | undefined;
        if (current === undefined) { resolved.push({ commandId, state: "absent" }); continue; }
        if (!commandStates.includes(current.state as CommandState)) fail();
        resolved.push({ commandId, state: current.state as NodeResolvedAmbiguousCommand["state"] });
      }
      database.exec("COMMIT");
      journal.#closed = true;
      database.close();
      return { ambiguous, resolved };
    } catch {
      try { database?.close(); } catch { /* best effort */ }
      throw storageError();
    }
  }

  /** Atomically reserves a command and its immutable route before any Worker call. */
  async claimCommandAndRoute(input: {
    readonly request: NodeCommandRequest;
    /** Invoked synchronously only for a new first route, inside its immediate transaction. */
    readonly resolveNewSessionReservation?: () => NodeReservationEvidence;
    /**
     * Invoked synchronously only for a new `continue`, `cancel` or `steer`, inside its immediate transaction,
     * with the prior route's reservation, before anything is written. A throw rolls the claim back. Never
     * invoked for a replay, which may already have been dispatched.
     */
    readonly admitRetainedReservation?: (reservation: NodeReservationEvidence) => void;
    readonly recordedAt: string;
  }): Promise<{ readonly replayed: boolean; readonly command: NodeJournalCommandRecord; readonly route: NodeJournalRunRoute | null; readonly priorRoute: NodePriorRoute | null }> {
    return this.#guard(async () => {
      const recordedAt = timestamp(input.recordedAt);
      const candidate = await validateNodeCommandCandidate(input.request);
      const commandId = callerIdentifier(candidate.command.commandId);
      const candidateIntent = canonicalNodeCommandIntent(candidate);
      const candidateDigest = nodeCommandIntentDigest(candidate);
      const claimed = await this.#transactionAsync(async () => {
        const existingRow = this.#db.prepare("SELECT operation,intent_digest,intent_json FROM commands WHERE command_id=?").get(commandId) as Row | undefined;
        if (existingRow !== undefined) {
          const persistedIntent = parseCanonicalRecord(existingRow.intent_json);
          if (commandIntentDigest(persistedIntent) !== existingRow.intent_digest) fail();
          if (existingRow.operation !== candidate.command.command
            || existingRow.intent_digest !== candidateDigest
            || !same(persistedIntent, candidateIntent)) fail("axis.node.intent_conflict");
          const old = await this.#readCommand(commandId);
          if (old === null) fail();
          const prior = old.priorRunId === null ? null : await this.#readPriorRoute(old.priorRunId);
          const request = await validateNodeCommandRequest(candidate, prior);
          if (!drainExempt(request.command.command) && (old.state === "reserved" || old.state === "dispatching") && this.#maintenanceMode() === "drain") {
            fail("axis.node.recovery_required");
          }
          const oldRoute = old.runId === null ? null : await this.#readRoute(old.runId);
          return { replayed: true, command: old, route: oldRoute, priorRoute: prior };
        }
        if (!drainExempt(candidate.command.command) && this.#maintenanceMode() === "drain") fail("axis.node.draining");

        const priorRunId = candidate.command.command === "start" ? null
          : identifier(candidate.command.command === "continue" ? candidate.command.continuation.priorRunId : candidate.command.target.runId);
        const runId = candidate.command.command === "cancel" || candidate.command.command === "steer" ? null : identifier(candidate.command.start.identity.run.runId);
        if (runId !== null && this.#db.prepare("SELECT run_id FROM routes WHERE run_id=?").get(runId) !== undefined) {
          fail("axis.node.binding_conflict");
        }
        if (candidate.command.command === "start") {
          const source = runtimeSourceSessionIdentity(candidate.command.start.identity.task.sourceRef);
          const existingFirst = this.#db.prepare("SELECT run_id FROM routes WHERE source_system=? AND source_session_id=? AND prior_run_id IS NULL")
            .get(source.system, identifier(source.sessionId));
          if (existingFirst !== undefined) fail("axis.node.binding_conflict");
        }
        const prior = priorRunId === null ? null : await this.#readPriorRoute(priorRunId);
        const request = await validateNodeCommandRequest(candidate, prior);
        const reservation = request.command.command === "start"
          ? validateNodeReservationEvidence(input.resolveNewSessionReservation?.() ?? fail("axis.node.recovery_required"))
          : prior?.reservation ?? fail("axis.node.recovery_required");
        if (request.command.command !== "start") input.admitRetainedReservation?.(reservation);
        await completeNodeWorkerCommand(request, reservation, prior);

        const route = request.command.command === "cancel" || request.command.command === "steer" ? null : this.#routeFromRequest(request, reservation, recordedAt);

        const intent = candidateIntent, intentDigest = candidateDigest;
        const receipt = {
          contractVersion: NODE_COMMAND_CONTRACT_VERSION,
          nodeRef: request.nodeRef,
          commandId,
          status: "reserved",
          reservation,
        } as const;
        const reservationJson = canonical(reservation), receiptJson = canonical(receipt);
        const commandAppend = this.#nextPopulationAppend("command");
        const commandChainDigest = commandAppendChainDigest({ previousTailDigest: commandAppend.previousTailDigest, appendOrder: commandAppend.appendOrder, commandId, intentDigest });
        const lifecycleDigest = commandLifecycleDigest({ commandId, runId: route?.runId ?? null, priorRunId, state: "reserved", createdAt: recordedAt, updatedAt: recordedAt, reservationJson, receiptJson, dispatchOwnerPid: null, dispatchOwnerToken: null });
        this.#db.prepare("INSERT INTO commands(command_id,append_order,append_chain_digest,lifecycle_digest,intent_digest,intent_json,operation,node_ref_json,requested_selection_json,reservation_json,run_id,prior_run_id,state,receipt_json,dispatch_owner_pid,dispatch_owner_token,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
          .run(commandId, commandAppend.appendOrder, commandChainDigest, lifecycleDigest, intentDigest, canonical(intent), request.command.command, canonical(request.nodeRef), canonical(request.target.selection), reservationJson, route?.runId ?? null, priorRunId, "reserved", receiptJson, null, null, recordedAt, recordedAt);
        this.#writePopulationIntegrity("command", commandAppend.appendOrder, commandChainDigest);

        if (route !== null) {
          const persistedRouteDigest = routeDigest(route), routeAppend = this.#nextPopulationAppend("route");
          const routeChainDigest = routeAppendChainDigest({ previousTailDigest: routeAppend.previousTailDigest, appendOrder: routeAppend.appendOrder, runId: route.runId, commandId, routeDigest: persistedRouteDigest });
          this.#db.prepare("INSERT INTO routes(run_id,append_order,append_chain_digest,command_id,route_digest,node_ref_json,requested_selection_json,reservation_json,task_id,source_system,source_session_id,source_ref_json,session_binding_id,prior_run_id,workspace_policy_ref_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .run(route.runId, routeAppend.appendOrder, routeChainDigest, commandId, persistedRouteDigest, canonical(route.nodeRef), canonical(route.requestedSelection), reservationJson, route.taskId, route.sourceSystem, route.sourceSessionId, canonical(route.sourceRef), route.sessionBindingId, route.priorRunId, route.workspacePolicyRef === null ? null : canonical(route.workspacePolicyRef), route.createdAt);
          this.#writePopulationIntegrity("route", routeAppend.appendOrder, routeChainDigest);
        }
        const rereadCache = new Map(this.#commandCache);
        rereadCache.delete(commandId);
        const command = await this.#readCommand(commandId, rereadCache);
        if (command === null) fail();
        return { replayed: false, command, route, priorRoute: prior };
      });
      this.#commandCache.set(commandId, claimed.command);
      return claimed;
    });
  }

  async readFirstRoute(input: { readonly sourceSystem: RuntimeSourceSystem; readonly sourceSessionId: string }): Promise<NodeJournalRunRoute | null> {
    return this.#guard(async () => {
      const source = sourceSystem(input.sourceSystem), sessionId = identifier(input.sourceSessionId);
      const first = this.#db.prepare("SELECT run_id FROM routes WHERE source_system=? AND source_session_id=? AND prior_run_id IS NULL")
        .get(source, sessionId) as { run_id: string } | undefined;
      if (first === undefined) return null;
      const route = await this.#readRoute(identifier(first.run_id));
      if (route === null) fail();
      return route;
    });
  }

  async readReceipt(input: { readonly query: NodeReceiptQuery; readonly authenticatedNode: NodeAuthenticatedContext }): Promise<NodeReceipt> {
    return this.#guard(async () => {
      const query = validateNodeReceiptQuery(input.query);
      const record = await this.#readCommand(query.commandId);
      if (record === null) return validateNodeReceipt({ ...query, status: "not_found" }, query);
      if (!same(record.nodeRef, query.nodeRef)) fail("axis.node.binding_conflict");
      const request = await this.#requestFromCommand(record);
      const prior = record.priorRunId === null ? undefined : await this.#readPriorRoute(record.priorRunId);
      return validateNodeReceipt(record.receipt, { nodeRef: query.nodeRef, commandId: query.commandId }, { authenticatedNode: input.authenticatedNode, expectation: await nodeReceiptExpectation(request, prior), ...(prior === undefined ? {} : { priorRoute: prior }) });
    });
  }

  async recordReceipt(input: {
    readonly request: NodeCommandRequest;
    readonly authenticatedNode: NodeAuthenticatedContext;
    readonly receipt: NodeReceipt;
    readonly recordedAt: string;
  }): Promise<NodeJournalCommandRecord> {
    return this.#guard(async () => {
      const commandId = callerIdentifier(input.request.command.commandId), recordedAt = timestamp(input.recordedAt);
      const updated = await this.#transactionAsync(async () => {
        const old = await this.#readCommand(commandId);
        if (old === null || old.intentDigest !== nodeCommandIntentDigest(input.request) || !same(old.canonicalIntent, canonicalNodeCommandIntent(input.request))) fail("axis.node.intent_conflict");
        const prior = old.priorRunId === null ? undefined : await this.#readPriorRoute(old.priorRunId);
        const receipt = await validateNodeReceipt(input.receipt, { nodeRef: old.nodeRef, commandId }, {
          authenticatedNode: input.authenticatedNode,
          expectation: await nodeReceiptExpectation(input.request, prior),
          ...(prior === undefined ? {} : { priorRoute: prior }),
        });
        if (receipt.status === "not_found") fail("axis.node.intent_conflict");
        if (same(receipt, old.receipt)) {
          if (old.state === "dispatching" && old.dispatchOwnerToken !== this.#ownerToken) fail("axis.node.binding_conflict");
          return old;
        }
        let next: CommandState;
        if (old.state === "reserved" && receipt.status === "dispatching") next = "dispatching";
        else if (old.state === "dispatching" && receipt.status === "accepted") next = "accepted";
        else if (old.state === "dispatching" && receipt.status === "rejected") next = "rejected";
        else if (old.state === "dispatching" && receipt.status === "ambiguous") next = "ambiguous";
        else fail("axis.node.intent_conflict");
        if (old.state === "dispatching" && old.dispatchOwnerToken !== this.#ownerToken) fail("axis.node.binding_conflict");
        const dispatchOwnerPid = next === "dispatching" ? process.pid : null;
        const dispatchOwnerToken = next === "dispatching" ? this.#ownerToken : null;
        const reservationJson = canonical(old.reservation), receiptJson = canonical(receipt);
        const lifecycleDigest = commandLifecycleDigest({ commandId, runId: old.runId, priorRunId: old.priorRunId, state: next, createdAt: old.createdAt, updatedAt: recordedAt, reservationJson, receiptJson, dispatchOwnerPid, dispatchOwnerToken });
        if (changes(this.#db.prepare("UPDATE commands SET state=?,receipt_json=?,dispatch_owner_pid=?,dispatch_owner_token=?,updated_at=?,lifecycle_digest=? WHERE command_id=? AND state=?").run(next, receiptJson, dispatchOwnerPid, dispatchOwnerToken, recordedAt, lifecycleDigest, commandId, old.state)) !== 1) fail();
        const rereadCache = new Map(this.#commandCache);
        rereadCache.delete(commandId);
        const reread = await this.#readCommand(commandId, rereadCache);
        if (reread === null) fail();
        return reread;
      });
      this.#commandCache.set(commandId, updated);
      return updated;
    });
  }

  /**
   * Validates a Worker's recorded response for an `ambiguous` start or continue against the expectation rebuilt
   * from retained state, without taking the write lock. Returns `null` when the command is absent, a cancel or
   * steer, or not `ambiguous`; a response that does not validate throws.
   */
  async prepareAmbiguousConvergence(input: {
    readonly commandId: string;
    readonly authenticatedNode: NodeAuthenticatedContext;
    readonly workerResponse: unknown;
  }): Promise<NodeAmbiguousConvergence | null> {
    return this.#guard(async () => {
      const commandId = callerIdentifier(input.commandId);
      const old = await this.#readCommand(commandId);
      if (old === null || old.state !== "ambiguous" || old.operation === "cancel" || old.operation === "steer") return null;
      const request = await this.#requestFromCommand(old);
      const prior = old.priorRunId === null ? undefined : await this.#readPriorRoute(old.priorRunId);
      const workerCommand = await completeNodeWorkerCommand(request, old.reservation, prior);
      if (workerCommand.command === "cancel" || workerCommand.command === "steer") return null;
      const { nativeSessionRef: _nativeSessionRef, ...sessionBinding } = workerCommand.start.identity.sessionBinding;
      const expectation = await nodeReceiptExpectation(request, prior);
      const response = await validateNodeCommandResponse({
        contractVersion: NODE_COMMAND_CONTRACT_VERSION,
        nodeRef: old.nodeRef,
        commandId,
        status: "accepted",
        evidence: { ...old.reservation, sessionBinding },
        result: input.workerResponse,
      }, expectation, input.authenticatedNode, prior);
      const receipt = await validateNodeReceipt(response, { nodeRef: old.nodeRef, commandId }, {
        authenticatedNode: input.authenticatedNode,
        expectation,
        ...(prior === undefined ? {} : { priorRoute: prior }),
      });
      if (receipt.status !== "accepted") fail("axis.node.intent_conflict");
      return Object.freeze({ commandId, receiptJson: canonical(receipt) });
    });
  }

  /**
   * Writes a prepared convergence. The compare-and-set on `state='ambiguous'` decides the writer across Node
   * processes sharing this journal; the loser reports `already_settled`.
   */
  async commitAmbiguousConvergence(input: { readonly prepared: NodeAmbiguousConvergence; readonly recordedAt: string }): Promise<"converged" | "already_settled"> {
    return this.#guard(async () => {
      const commandId = callerIdentifier(input.prepared.commandId), recordedAt = timestamp(input.recordedAt);
      const outcome = await this.#transactionAsync(async () => {
        const current = await this.#readCommand(commandId, new Map());
        if (current === null) fail();
        const reservationJson = canonical(current.reservation), receiptJson = input.prepared.receiptJson;
        const lifecycleDigest = commandLifecycleDigest({ commandId, runId: current.runId, priorRunId: current.priorRunId, state: "accepted", createdAt: current.createdAt, updatedAt: recordedAt, reservationJson, receiptJson, dispatchOwnerPid: null, dispatchOwnerToken: null });
        if (changes(this.#db.prepare("UPDATE commands SET state='accepted',receipt_json=?,updated_at=?,lifecycle_digest=? WHERE command_id=? AND state='ambiguous'").run(receiptJson, recordedAt, lifecycleDigest, commandId)) !== 1) return "already_settled" as const;
        const reread = await this.#readCommand(commandId, new Map());
        if (reread === null || reread.state !== "accepted") fail();
        return "converged" as const;
      });
      this.#commandCache.delete(commandId);
      return outcome;
    });
  }

  /** The Worker target of a start or continue that is still `ambiguous`, or `null`. */
  async readAmbiguousCommandTarget(commandId: string): Promise<NodeTarget | null> {
    return this.#guard(async () => {
      const command = await this.#readCommand(callerIdentifier(commandId));
      return command === null || command.state !== "ambiguous" || command.operation === "cancel" || command.operation === "steer" ? null : command.reservation.target;
    });
  }

  /** The `ambiguous` command that owns a Run's route, or `null`. Two primary-key reads. */
  async readAmbiguousCommandForRun(runId: string): Promise<{ readonly commandId: string; readonly target: NodeTarget } | null> {
    return this.#guard(async () => {
      const route = await this.#readRoute(identifier(runId));
      if (route === null) return null;
      const command = await this.#readCommand(route.commandId);
      return command === null || command.state !== "ambiguous" || command.operation === "cancel" || command.operation === "steer" ? null : { commandId: command.commandId, target: command.reservation.target };
    });
  }

  /** Every start or continue still `ambiguous`, for the sweep a Node runs when it opens. A full scan. */
  async listAmbiguousCommandIds(): Promise<readonly string[]> {
    return this.#guard(async () => (this.#db.prepare("SELECT command_id FROM commands WHERE state='ambiguous' AND operation IN ('start','continue') ORDER BY rowid").all() as { command_id: string }[]).map((row) => row.command_id));
  }

  /** The route an event for `runId` is admitted and delivered on: an accepted start or continue's route. */
  async readRunRoute(runId: string): Promise<NodeAcceptedRunRoute | null> {
    return this.#guard(async () => (await this.#readAcceptedEventRoute(identifier(runId)))?.route ?? null);
  }
  async maintenanceMode(): Promise<NodeMaintenanceMode> { return this.#guard(() => this.#maintenanceMode()); }
  async setMaintenanceMode(mode: NodeMaintenanceMode): Promise<NodeMaintenanceMode> {
    if (mode !== "drain" && mode !== "resume") throw storageError();
    return this.#guard(async () => this.#transactionAsync(async () => {
      if (changes(this.#db.prepare("UPDATE metadata SET value=? WHERE key='maintenance_mode'").run(mode)) !== 1) fail();
      return this.#maintenanceMode();
    }));
  }
  async maintenanceBlockerCounts(): Promise<NodeJournalBlockerCounts> {
    return this.#guard(() => this.#maintenanceBlockerCounts());
  }

  /**
   * How many retained sessions name a Worker that `inventory.admission` does not admit: the sessions whose
   * follow-ups this Node refuses as `axis.node.worker_unavailable`. Counted per session, since a terminal Run
   * still authorizes its session's next continuation.
   */
  async retainedSessionsWithoutWorker(inventory: Pick<NodeWorkerInventoryPort, "admission">): Promise<number> {
    return this.#guard(async () => {
      const sessions = new Set<string>();
      for (const row of this.#db.prepare("SELECT source_system,source_session_id,reservation_json FROM routes").all() as Row[]) {
        const reservation = validateNodeReservationEvidence(parseCanonicalJson(row.reservation_json));
        if (inventory.admission(reservation.target).kind !== "available") sessions.add(`${sourceSystem(row.source_system)}\u0000${identifier(row.source_session_id)}`);
      }
      return sessions.size;
    });
  }

  async recordEvent(input: { readonly event: RouterRunLifecycleEnvelope; readonly recordedAt: string }): Promise<{ readonly replayed: boolean; readonly record: NodeJournalEventRecord }> {
    const record = async () => {
      const eventId = nodeEventId(input.event), runId = nodeEventRunId(input.event), eventDigest = nodeEventDigest(input.event), recordedAt = timestamp(input.recordedAt);
      return this.#transactionAsync(async () => {
        const accepted = await this.#readAcceptedEventRoute(runId);
        if (accepted === null) fail("axis.node.binding_conflict");
        this.#requireEventAuthority(input.event, accepted.route, accepted.receipt.result.runAccepted);
        const old = await this.#readEvent(eventId);
        if (old !== null) {
          if (old.digest !== eventDigest) fail("axis.node.intent_conflict");
          return { replayed: true, record: old };
        }
        const ordinal = Number((this.#db.prepare("SELECT COALESCE(MAX(ordinal),0)+1 AS value FROM event_outbox WHERE run_id=?").get(runId) as { value: number }).value);
        const globalOrder = Number((this.#db.prepare("SELECT COALESCE(MAX(global_order),0)+1 AS value FROM event_outbox").get() as { value: number }).value);
        const appendPositionDigest = eventAppendPositionDigest({ eventId, runId, eventDigest, ordinal, globalOrder });
        const lifecycleDigest = eventLifecycleDigest({ eventId, runId, eventDigest, recordedAt, state: "pending", deliveryAttemptCount: 0, lastDeliveryFailureJson: null, deliveredAt: null, quarantineCode: null, terminalAt: null });
        this.#db.prepare("INSERT INTO event_outbox(event_id,run_id,event_digest,event_json,recorded_at,ordinal,global_order,append_position_digest,lifecycle_digest,state,delivery_attempt_count,last_delivery_failure_json,delivered_at,quarantine_code,terminal_at) VALUES (?,?,?,?,?,?,?,?,?,'pending',0,NULL,NULL,NULL,NULL)")
          .run(eventId, runId, eventDigest, canonical(input.event), recordedAt, ordinal, globalOrder, appendPositionDigest, lifecycleDigest);
        this.#writeEventOutboxIntegrity(globalOrder, globalOrder, appendPositionDigest);
        const record = await this.#readEvent(eventId);
        if (record === null) fail();
        return { replayed: false, record };
      });
    };
    return this.#guard(record);
  }
  async listPending(limit: number): Promise<readonly NodeJournalEventRecord[]> {
    return this.listPendingPage({ afterGlobalOrder: 0, limit });
  }
  /** Package-private eligible-head pagination; it never changes the durable ordering authority. */
  async listPendingPage(input: { readonly afterGlobalOrder: number; readonly limit: number }): Promise<readonly NodeJournalEventRecord[]> {
    return this.#guard(async () => {
      if (!Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 1_000 || !Number.isSafeInteger(input.afterGlobalOrder) || input.afterGlobalOrder < 0) fail();
      const rows = this.#db.prepare(`SELECT event_id FROM event_outbox e
        WHERE state='pending' AND NOT EXISTS (
          SELECT 1 FROM event_outbox p WHERE p.run_id=e.run_id AND p.ordinal<e.ordinal AND p.state IN ('pending','quarantined')
        ) AND global_order>? ORDER BY global_order LIMIT ?`).all(input.afterGlobalOrder, input.limit) as { event_id: string }[];
      return Promise.all(rows.map(async ({ event_id }) => {
        const value = await this.#readEvent(event_id); if (value === null) fail(); return value;
      }));
    });
  }
  async markDelivered(input: { readonly eventId: string; readonly deliveredAt: string }): Promise<NodeJournalEventRecord> {
    return this.#terminalEvent(input.eventId, "delivered", { terminalAt: timestamp(input.deliveredAt) }, null);
  }
  async quarantine(input: { readonly eventId: string; readonly code: NodeErrorCode; readonly quarantinedAt: string }): Promise<NodeJournalEventRecord> {
    return this.#terminalEvent(input.eventId, "quarantined", { terminalAt: timestamp(input.quarantinedAt) }, input.code);
  }
  async recordDeliveryFailure(input: { readonly eventId: string; readonly code: NodeJournalDeliveryFailureCode; readonly failedAt: string }): Promise<NodeJournalEventRecord> {
    return this.#guard(async () => this.#transactionAsync(async () => {
      const eventId = identifier(input.eventId), code = deliveryFailureCode(input.code), failedAt = timestamp(input.failedAt);
      const old = await this.#readEvent(eventId);
      if (old === null || old.state !== "pending") fail("axis.node.intent_conflict");
      const failure: NodeJournalDeliveryFailure = { contractVersion: DELIVERY_DIAGNOSTIC_VERSION, code, retryable: deliveryFailureCodes[code], failedAt };
      const deliveryAttemptCount = old.deliveryAttemptCount + 1;
      if (!Number.isSafeInteger(deliveryAttemptCount)) fail();
      const lastDeliveryFailureJson = canonical(failure);
      const lifecycleDigest = eventLifecycleDigest({ eventId, runId: old.runId, eventDigest: old.digest, recordedAt: old.recordedAt, state: "pending", deliveryAttemptCount, lastDeliveryFailureJson, deliveredAt: null, quarantineCode: null, terminalAt: null });
      if (changes(this.#db.prepare("UPDATE event_outbox SET delivery_attempt_count=?,last_delivery_failure_json=?,lifecycle_digest=? WHERE event_id=? AND state='pending' AND delivery_attempt_count=?").run(deliveryAttemptCount, lastDeliveryFailureJson, lifecycleDigest, eventId, old.deliveryAttemptCount)) !== 1) fail();
      const updated = await this.#readEvent(eventId); if (updated === null) fail(); return updated;
    }));
  }

  close(): void {
    if (this.#closed) return;
    if (this.#pendingOperations !== 0) throw storageError();
    this.#closed = true;
    let failed = false;
    try { this.#db.exec("PRAGMA wal_checkpoint(TRUNCATE)"); } catch { failed = true; }
    try { this.#db.close(); } catch { failed = true; }
    if (failed) throw storageError();
  }

  async #initialize(recover: boolean): Promise<void> {
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const objects = this.#db.prepare("SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").all();
      if (objects.length === 0) {
        for (const statement of schemaStatements) this.#db.exec(statement);
        const insertMetadata = this.#db.prepare("INSERT INTO metadata(key,value) VALUES (?,?)");
        insertMetadata.run("command_count", "0");
        insertMetadata.run("command_high_water_append_order", "0");
        insertMetadata.run("command_tail_digest", commandAppendChainAnchor());
        insertMetadata.run("event_count", "0");
        insertMetadata.run("event_high_water_global_order", "0");
        insertMetadata.run("event_tail_digest", eventOutboxTailDigest({ eventCount: 0, highWaterGlobalOrder: 0, tailAppendPositionDigest: null }));
        insertMetadata.run("maintenance_mode", "resume");
        insertMetadata.run("route_count", "0");
        insertMetadata.run("route_high_water_append_order", "0");
        insertMetadata.run("route_tail_digest", routeAppendChainAnchor());
        insertMetadata.run("schema_version", SCHEMA_VERSION);
      }
      this.#db.exec("COMMIT");
    } catch (error) { this.#db.exec("ROLLBACK"); throw error; }
    this.#validateSchema();
    await this.#transactionAsync(async () => {
      await this.#validateRows();
      if (recover) {
        await this.#recoverDispatchingCommands();
        await this.#validateRows();
      }
    });
    this.#dataVersion = this.#readDataVersion();
  }

  async #recoverDispatchingCommands(): Promise<void> {
    const rows = this.#db.prepare("SELECT command_id FROM commands WHERE state='dispatching' ORDER BY append_order").all() as { command_id: string }[];
    const commands = await Promise.all(rows.map(({ command_id }) => this.#readCommand(command_id)));
    if (commands.some((command) => command === null || command.dispatchOwnerPid === null || processAlive(command.dispatchOwnerPid))) fail("axis.node.recovery_required");
    for (const { command_id } of rows) {
      const command = await this.#readCommand(command_id);
      if (command === null || command.state !== "dispatching") fail();
      const receipt = { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: command.nodeRef, commandId: command.commandId, status: "ambiguous", reservation: command.reservation } as const;
      const reservationJson = canonical(command.reservation), receiptJson = canonical(receipt);
      const lifecycleDigest = commandLifecycleDigest({ commandId: command.commandId, runId: command.runId, priorRunId: command.priorRunId, state: "ambiguous", createdAt: command.createdAt, updatedAt: command.updatedAt, reservationJson, receiptJson, dispatchOwnerPid: null, dispatchOwnerToken: null });
      if (changes(this.#db.prepare("UPDATE commands SET state='ambiguous',receipt_json=?,dispatch_owner_pid=NULL,dispatch_owner_token=NULL,lifecycle_digest=? WHERE command_id=? AND state='dispatching'").run(receiptJson, lifecycleDigest, command.commandId)) !== 1) fail();
    }
  }

  #validateSchema(): void {
    const integrity = this.#db.prepare("PRAGMA integrity_check").get() as { integrity_check?: string } | undefined;
    if (integrity?.integrity_check !== "ok") fail();
    const foreignKeys = this.#db.prepare("PRAGMA foreign_key_check").all();
    if (foreignKeys.length !== 0 || Number((this.#db.prepare("PRAGMA foreign_keys").get() as { foreign_keys: number }).foreign_keys) !== 1) fail();
    const objects = this.#db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name").all() as { type: string; name: string; sql: string | null }[];
    const expected = schemaStatements.map((sql) => {
      const match = /^CREATE (TABLE|UNIQUE INDEX) ([a-z0-9_]+)/u.exec(sql);
      if (match?.[1] === undefined || match[2] === undefined) fail();
      return { type: match[1] === "TABLE" ? "table" : "index", name: match[2], sql: normalizeSql(sql) };
    }).sort((left, right) => left.type.localeCompare(right.type) || left.name.localeCompare(right.name));
    const actual = objects.map((row) => ({ type: row.type, name: row.name, sql: row.sql === null ? "" : normalizeSql(row.sql) }));
    if (!same(actual, expected)) fail();
    if (this.#readMetadata().get("schema_version") !== SCHEMA_VERSION) fail();
    const tables = this.#db.prepare("PRAGMA table_list").all() as { name: string; strict: number }[];
    for (const table of schemaStatements.flatMap((sql) => /^CREATE TABLE ([a-z0-9_]+)/u.exec(sql)?.[1] ?? [])) {
      if (tables.find((row) => row.name === table)?.strict !== 1) fail();
    }
  }

  async #validateRows(): Promise<void> {
    const commandRows = this.#db.prepare("SELECT * FROM commands ORDER BY append_order").all() as Row[];
    const routeRows = this.#db.prepare("SELECT * FROM routes ORDER BY append_order").all() as Row[];
    const commandCache = new Map<string, NodeJournalCommandRecord>();
    const commands: (NodeJournalCommandRecord | null)[] = [];
    for (const { command_id } of commandRows) commands.push(await this.#readCommand(identifier(command_id), commandCache));
    const routes = await Promise.all(routeRows.map(({ run_id }) => this.#readRoute(identifier(run_id))));
    if (commands.some((record) => record === null) || routes.some((record) => record === null)) fail();
    const checkedCommands = commands as NodeJournalCommandRecord[], checkedRoutes = routes as NodeJournalRunRoute[];

    let commandTail: string = commandAppendChainAnchor();
    for (const [index, command] of checkedCommands.entries()) {
      const appendOrder = Number(commandRows[index]?.append_order);
      if (!Number.isSafeInteger(appendOrder) || appendOrder !== index + 1) fail();
      commandTail = commandAppendChainDigest({ previousTailDigest: commandTail, appendOrder, commandId: command.commandId, intentDigest: command.intentDigest });
      if (commandRows[index]?.append_chain_digest !== commandTail) fail();
    }
    this.#validatePopulationIntegrity("command", checkedCommands.length, commandTail);

    let routeTail: string = routeAppendChainAnchor();
    for (const [index, route] of checkedRoutes.entries()) {
      const appendOrder = Number(routeRows[index]?.append_order);
      if (!Number.isSafeInteger(appendOrder) || appendOrder !== index + 1) fail();
      routeTail = routeAppendChainDigest({ previousTailDigest: routeTail, appendOrder, runId: route.runId, commandId: route.commandId, routeDigest: routeDigest(route) });
      if (routeRows[index]?.append_chain_digest !== routeTail) fail();
    }
    this.#validatePopulationIntegrity("route", checkedRoutes.length, routeTail);

    const commandById = new Map(checkedCommands.map((command) => [command.commandId, command]));
    const routeByRun = new Map(checkedRoutes.map((route) => [route.runId, route]));
    const routeAppendOrderByRun = new Map(checkedRoutes.map((route, index) => [route.runId, Number(routeRows[index]?.append_order)]));
    for (const command of checkedCommands) {
      const request = await this.#requestFromCommand(command, commandCache);
      if (command.operation === "cancel" || command.operation === "steer") {
        if (command.runId !== null || command.priorRunId === null) fail();
        continue;
      }
      const route = command.runId === null ? undefined : routeByRun.get(command.runId);
      if (route === undefined || route.commandId !== command.commandId || !same(route.nodeRef, command.nodeRef)
        || !same(route.requestedSelection, command.requestedSelection) || !same(route.reservation, command.reservation)) fail();
      if (request.command.command === "cancel" || request.command.command === "steer") fail();
      const identity = request.command.start.identity;
      if (route.taskId !== identity.task.taskId || route.runId !== identity.run.runId || route.sessionBindingId !== identity.run.sessionBindingId
        || !same(route.sourceRef, identity.task.sourceRef) || !same(route.workspacePolicyRef, request.command.workspacePolicyRef)) fail();
      const source = runtimeSourceSessionIdentity(identity.task.sourceRef);
      if (route.sourceSystem !== source.system || route.sourceSessionId !== source.sessionId) fail();
      const expectedPriorRunId = request.command.command === "continue" ? request.command.continuation.priorRunId : null;
      if (route.priorRunId !== expectedPriorRunId || command.priorRunId !== expectedPriorRunId) fail();
      if (expectedPriorRunId !== null) {
        const priorOrder = routeAppendOrderByRun.get(expectedPriorRunId), routeOrder = routeAppendOrderByRun.get(route.runId);
        if (priorOrder === undefined || routeOrder === undefined || priorOrder >= routeOrder) fail();
      }
    }
    for (const route of checkedRoutes) if (commandById.get(route.commandId)?.runId !== route.runId) fail();

    const events = await Promise.all((this.#db.prepare("SELECT * FROM event_outbox ORDER BY global_order").all() as Row[]).map((row) => this.#eventFromRow(row)));
    const nextOrdinalByRun = new Map<string, number>();
    for (const [index, event] of events.entries()) {
      if (event.globalOrder !== index + 1) fail();
      const expectedOrdinal = (nextOrdinalByRun.get(event.runId) ?? 0) + 1;
      if (event.ordinal !== expectedOrdinal) fail();
      nextOrdinalByRun.set(event.runId, expectedOrdinal);
      const accepted = await this.#readAcceptedEventRoute(event.runId); if (accepted === null) fail();
      this.#requireEventAuthority(event.event, accepted.route, accepted.receipt.result.runAccepted);
    }
    this.#validateEventOutboxIntegrity(events);
    this.#commandCache.clear();
    for (const [commandId, command] of commandCache) this.#commandCache.set(commandId, command);
  }

  #readMetadata(): ReadonlyMap<string, string> {
    const rows = this.#db.prepare("SELECT key,value FROM metadata ORDER BY key").all() as { key: string; value: string }[];
    if (!same(rows.map(({ key }) => key), metadataKeys)) fail();
    return new Map(rows.map(({ key, value }) => [key, value]));
  }

  #nextPopulationAppend(population: JournalPopulation): { readonly appendOrder: number; readonly previousTailDigest: string } {
    const metadata = this.#readMetadata();
    const count = metadata.get(`${population}_count`), highWater = metadata.get(`${population}_high_water_append_order`), previousTailDigest = metadata.get(`${population}_tail_digest`);
    if (count === undefined || highWater === undefined || previousTailDigest === undefined || count !== highWater || !/^(?:0|[1-9][0-9]*)$/u.test(highWater)) fail();
    const appendOrder = Number(highWater) + 1;
    if (!Number.isSafeInteger(appendOrder)) fail();
    return { appendOrder, previousTailDigest };
  }

  #writePopulationIntegrity(population: JournalPopulation, appendOrder: number, tailDigest: string): void {
    const update = this.#db.prepare("UPDATE metadata SET value=? WHERE key=?");
    const values = [
      [`${population}_count`, String(appendOrder)],
      [`${population}_high_water_append_order`, String(appendOrder)],
      [`${population}_tail_digest`, tailDigest],
    ] as const;
    for (const [key, value] of values) if (changes(update.run(value, key)) !== 1) fail();
  }

  #validatePopulationIntegrity(population: JournalPopulation, count: number, tailDigest: string): void {
    const metadata = this.#readMetadata();
    if (metadata.get(`${population}_count`) !== String(count)
      || metadata.get(`${population}_high_water_append_order`) !== String(count)
      || metadata.get(`${population}_tail_digest`) !== tailDigest) fail();
  }


  #routeFromRequest(request: NodeCommandRequest, reservation: NodeReservationEvidence, recordedAt: string): NodeJournalRunRoute {
    if (request.command.command === "cancel" || request.command.command === "steer") fail("axis.node.intent_conflict");
    const identity = request.command.start.identity;
    const source = runtimeSourceSessionIdentity(identity.task.sourceRef);
    return {
      commandId: request.command.commandId,
      nodeRef: request.nodeRef,
      requestedSelection: request.target.selection,
      reservation,
      taskId: identifier(identity.task.taskId),
      sourceSystem: sourceSystem(source.system),
      sourceSessionId: identifier(source.sessionId),
      sourceRef: identity.task.sourceRef,
      runId: identifier(identity.run.runId),
      sessionBindingId: identifier(identity.run.sessionBindingId),
      priorRunId: request.command.command === "continue" ? identifier(request.command.continuation.priorRunId) : null,
      workspacePolicyRef: request.command.workspacePolicyRef,
      createdAt: recordedAt,
    };
  }

  async #readRoute(runId: string): Promise<NodeJournalRunRoute | null> {
    const row = this.#db.prepare("SELECT * FROM routes WHERE run_id=?").get(runId) as Row | undefined;
    if (row === undefined) return null;
    const nodeRef = versionedReference(parseCanonicalJson(row.node_ref_json));
    const requestedSelection = validateWorkerSelection(parseCanonicalJson(row.requested_selection_json));
    const reservation = validateNodeReservationEvidence(parseCanonicalJson(row.reservation_json));
    const sourceRef = parseCanonicalRecord(row.source_ref_json) as unknown as SourceRef;
    const source = runtimeSourceSessionIdentity(sourceRef);
    const route: NodeJournalRunRoute = {
      commandId: identifier(row.command_id), nodeRef, requestedSelection, reservation,
      taskId: identifier(row.task_id), sourceSystem: sourceSystem(row.source_system), sourceSessionId: identifier(row.source_session_id), sourceRef,
      runId: identifier(row.run_id), sessionBindingId: identifier(row.session_binding_id), priorRunId: row.prior_run_id === null ? null : identifier(row.prior_run_id),
      workspacePolicyRef: row.workspace_policy_ref_json === null ? null : versionedReference(parseCanonicalJson(row.workspace_policy_ref_json)),
      createdAt: timestamp(String(row.created_at)),
    };
    if (source.system !== route.sourceSystem || source.sessionId !== route.sourceSessionId || row.route_digest !== routeDigest(route)) fail();
    return route;
  }

  async #readAcceptedEventRoute(runId: string): Promise<{ readonly route: NodeAcceptedRunRoute; readonly receipt: NodeAcceptedEventReceipt } | null> {
    const persisted = await this.#readRoute(runId);
    if (persisted === null) return null;
    const command = await this.#readCommand(persisted.commandId, new Map());
    if (command === null || command.runId !== persisted.runId) fail();
    if (command.receipt.status !== "accepted") return null;
    const result = command.receipt.result;
    if (result.status !== "accepted") fail();
    const route: NodeAcceptedRunRoute = {
      commandId: persisted.commandId,
      nodeRef: persisted.nodeRef,
      target: command.receipt.evidence.target,
      taskId: persisted.taskId,
      sourceSystem: persisted.sourceSystem,
      sourceSessionId: persisted.sourceSessionId,
      runId: persisted.runId,
      sessionBindingId: persisted.sessionBindingId,
      priorRunId: persisted.priorRunId,
      workspacePolicyRef: persisted.workspacePolicyRef,
      createdAt: persisted.createdAt,
    };
    return { route, receipt: { ...command.receipt, result } };
  }

  async #readPriorRoute(
    runId: string,
    commandCache: Map<string, NodeJournalCommandRecord> = this.#commandCache,
  ): Promise<NodePriorRoute> {
    const route = await this.#readRoute(runId);
    if (route === null) fail("axis.node.recovery_required");
    const command = await this.#readCommand(route.commandId, commandCache);
    if (command === null || command.receipt.status !== "accepted") fail("axis.node.recovery_required");
    return validateNodePriorRoute({
      nodeRef: route.nodeRef,
      requestedSelection: route.requestedSelection,
      reservation: route.reservation,
      sessionBinding: command.receipt.evidence.sessionBinding,
      sourceSystem: route.sourceSystem,
      sourceSessionId: route.sourceSessionId,
      sourceRef: route.sourceRef,
      workspacePolicyRef: route.workspacePolicyRef,
    });
  }

  async #requestFromCanonicalIntent(intent: JsonRecord, prior: NodePriorRoute | null): Promise<NodeCommandRequest> {
    const command = parseRecord(intent.command);
    let rehydratedCommand: JsonRecord;
    if (command.command === "cancel") rehydratedCommand = command;
    else {
      const instructionSha256 = command.instructionSha256;
      if (typeof instructionSha256 !== "string" || !/^sha256:[0-9a-f]{64}$/u.test(instructionSha256)) fail();
      rehydratedCommand = Object.fromEntries(Object.entries(command).flatMap(([key, value]) => key === "instructionSha256" ? [["instruction", "rehydration-placeholder"]] : [[key, value]]));
    }
    return validateNodeCommandRequest({ ...intent, command: rehydratedCommand }, prior);
  }

  async #requestFromCommand(
    command: NodeJournalCommandRecord,
    commandCache: Map<string, NodeJournalCommandRecord> = this.#commandCache,
  ): Promise<NodeCommandRequest> {
    const prior = command.priorRunId === null ? null : await this.#readPriorRoute(command.priorRunId, commandCache);
    return this.#requestFromCanonicalIntent(command.canonicalIntent, prior);
  }

  async #readCommand(
    commandId: string,
    commandCache: Map<string, NodeJournalCommandRecord> = this.#commandCache,
  ): Promise<NodeJournalCommandRecord | null> {
    const cached = commandCache.get(commandId);
    if (cached !== undefined) return cached;
    const lineage: { readonly commandId: string; readonly row: Row }[] = [];
    const resolving = new Set<string>();
    let currentCommandId = commandId;
    while (!commandCache.has(currentCommandId)) {
      if (resolving.has(currentCommandId)) fail();
      resolving.add(currentCommandId);
      const row = this.#db.prepare("SELECT * FROM commands WHERE command_id=?").get(currentCommandId) as Row | undefined;
      if (row === undefined) {
        if (lineage.length === 0) return null;
        fail();
      }
      lineage.push({ commandId: currentCommandId, row });
      if (row.prior_run_id === null) break;
      const priorRoute = await this.#readRoute(identifier(row.prior_run_id));
      if (priorRoute === null) fail();
      currentCommandId = priorRoute.commandId;
    }

    for (let index = lineage.length - 1; index >= 0; index -= 1) {
      const item = lineage[index]!;
      const priorRunId = item.row.prior_run_id === null ? null : identifier(item.row.prior_run_id);
      const prior = priorRunId === null ? null : await this.#readPriorRoute(priorRunId, commandCache);
      const record = await this.#commandFromRow(item.commandId, item.row, prior);
      commandCache.set(item.commandId, record);
    }
    return commandCache.get(commandId) ?? fail();
  }

  async #commandFromRow(commandId: string, row: Row, prior: NodePriorRoute | null): Promise<NodeJournalCommandRecord> {
    const canonicalIntent = parseCanonicalRecord(row.intent_json), intentDigest = String(row.intent_digest);
    if (commandIntentDigest(canonicalIntent) !== intentDigest) fail();
    const operationValue = row.operation;
    if (operationValue !== "start" && operationValue !== "continue" && operationValue !== "cancel" && operationValue !== "steer") fail();
    const operation = operationValue as NodeJournalCommandRecord["operation"];
    const runId = row.run_id === null ? null : identifier(row.run_id), priorRunId = row.prior_run_id === null ? null : identifier(row.prior_run_id);
    const persistedCommand = parseRecord(canonicalIntent.command);
    let expectedRunId: string | null, expectedPriorRunId: string | null;
    if (operation === "cancel" || operation === "steer") {
      expectedRunId = null;
      expectedPriorRunId = identifier(parseRecord(persistedCommand.target).runId);
    } else {
      expectedRunId = identifier(parseRecord(parseRecord(parseRecord(persistedCommand.start).identity).run).runId);
      expectedPriorRunId = operation === "continue" ? identifier(parseRecord(persistedCommand.continuation).priorRunId) : null;
    }
    if (persistedCommand.commandId !== commandId || persistedCommand.command !== operation || runId !== expectedRunId || priorRunId !== expectedPriorRunId
      || (runId !== null && priorRunId === runId)) fail();
    const request = await this.#requestFromCanonicalIntent(canonicalIntent, prior);
    const nodeRef = versionedReference(parseCanonicalJson(row.node_ref_json));
    const requestedSelection = validateWorkerSelection(parseCanonicalJson(row.requested_selection_json));
    const reservationJson = String(row.reservation_json), reservation = validateNodeReservationEvidence(parseCanonicalJson(reservationJson));
    if (request.command.commandId !== commandId || request.command.command !== operation || !same(request.nodeRef, nodeRef) || !same(request.target.selection, requestedSelection)) fail();
    const state = row.state;
    if (!commandStates.includes(state as CommandState)) fail();
    const receiptJson = String(row.receipt_json), receiptValue = parseCanonicalJson(receiptJson);
    const receipt = await validateNodeReceipt(receiptValue, { nodeRef, commandId }, {
      authenticatedNode: { nodeRef, nodeId: reservation.target.identity.nodeId },
      expectation: await nodeReceiptExpectation(request, prior),
      ...(prior === null ? {} : { priorRoute: prior }),
    });
    if (receipt.status === "not_found") fail();
    const receiptReservation = receipt.status === "accepted" ? { workerKey: receipt.evidence.workerKey, target: receipt.evidence.target } : receipt.reservation;
    if (!same(receiptReservation, reservation) || receipt.status !== state) fail();
    const dispatchOwnerPid = row.dispatch_owner_pid === null ? null : Number(row.dispatch_owner_pid);
    const dispatchOwnerToken = row.dispatch_owner_token === null ? null : identifier(row.dispatch_owner_token);
    if ((state === "dispatching" && (!Number.isSafeInteger(dispatchOwnerPid) || dispatchOwnerPid === null || dispatchOwnerPid < 1 || dispatchOwnerToken === null))
      || (state !== "dispatching" && (dispatchOwnerPid !== null || dispatchOwnerToken !== null))) fail();
    const createdAt = timestamp(String(row.created_at)), updatedAt = timestamp(String(row.updated_at));
    if (row.lifecycle_digest !== commandLifecycleDigest({ commandId, runId, priorRunId, state: state as CommandState, createdAt, updatedAt, reservationJson, receiptJson, dispatchOwnerPid, dispatchOwnerToken })) fail();
    return { commandId, intentDigest, canonicalIntent, operation, nodeRef, requestedSelection, reservation, runId, priorRunId, state: state as CommandState, receipt, dispatchOwnerPid, dispatchOwnerToken, createdAt, updatedAt };
  }

  #maintenanceMode(): NodeMaintenanceMode {
    const mode = this.#readMetadata().get("maintenance_mode");
    if (mode !== "drain" && mode !== "resume") fail();
    return mode;
  }

  #maintenanceBlockerCounts(): NodeJournalBlockerCounts {
    const count = (sql: string): number => {
      const value = Number((this.#db.prepare(sql).get() as { count?: unknown } | undefined)?.count);
      if (!Number.isSafeInteger(value) || value < 0) fail();
      return value;
    };
    return {
      reservedCommands: count("SELECT COUNT(*) AS count FROM commands WHERE state='reserved'"),
      dispatchingCommands: count("SELECT COUNT(*) AS count FROM commands WHERE state='dispatching'"),
      ambiguousCommands: count("SELECT COUNT(*) AS count FROM commands WHERE state='ambiguous'"),
      pendingNodeEvents: count("SELECT COUNT(*) AS count FROM event_outbox WHERE state='pending'"),
      quarantinedNodeEvents: count("SELECT COUNT(*) AS count FROM event_outbox WHERE state='quarantined'"),
    };
  }

  #writeEventOutboxIntegrity(eventCount: number, highWaterGlobalOrder: number, tailAppendPositionDigest: string): void {
    const update = this.#db.prepare("UPDATE metadata SET value=? WHERE key=?");
    const values = [
      ["event_count", String(eventCount)],
      ["event_high_water_global_order", String(highWaterGlobalOrder)],
      ["event_tail_digest", eventOutboxTailDigest({ eventCount, highWaterGlobalOrder, tailAppendPositionDigest })],
    ] as const;
    for (const [key, value] of values) if (changes(update.run(value, key)) !== 1) fail();
  }

  #validateEventOutboxIntegrity(events: readonly NodeJournalEventRecord[]): void {
    const metadata = this.#readMetadata();
    const tail = events[events.length - 1];
    const eventCount = events.length, highWaterGlobalOrder = tail?.globalOrder ?? 0;
    const tailAppendPositionDigest = tail === undefined ? null : eventAppendPositionDigest({ eventId: tail.eventId, runId: tail.runId, eventDigest: tail.digest, ordinal: tail.ordinal, globalOrder: tail.globalOrder });
    if (metadata.get("event_count") !== String(eventCount)
      || metadata.get("event_high_water_global_order") !== String(highWaterGlobalOrder)
      || metadata.get("event_tail_digest") !== eventOutboxTailDigest({ eventCount, highWaterGlobalOrder, tailAppendPositionDigest })) fail();
  }

  async #terminalEvent(
    eventIdValue: string,
    state: "delivered" | "quarantined",
    evidence: { readonly terminalAt: string },
    codeValue: unknown,
  ): Promise<NodeJournalEventRecord> {
    return this.#guard(async () => this.#transactionAsync(async () => {
      const eventId = identifier(eventIdValue), code = state === "quarantined" ? quarantineCode(codeValue) : null;
      const old = await this.#readEvent(eventId);
      if (old === null) fail("axis.node.intent_conflict");
      if (old.state === state) {
        // Repeating a terminal outcome is idempotent whatever its timestamp; the first writer's is kept.
        if (old.quarantineCode === code) return old;
        fail("axis.node.intent_conflict");
      }
      if (old.state !== "pending") fail("axis.node.intent_conflict");
      const terminalAt = evidence.terminalAt;
      const deliveredAt = state === "delivered" ? terminalAt : null;
      const lastDeliveryFailureJson = old.lastDeliveryFailure === null ? null : canonical(old.lastDeliveryFailure);
      const lifecycleDigest = eventLifecycleDigest({ eventId, runId: old.runId, eventDigest: old.digest, recordedAt: old.recordedAt, state, deliveryAttemptCount: old.deliveryAttemptCount, lastDeliveryFailureJson, deliveredAt, quarantineCode: code, terminalAt });
      if (changes(this.#db.prepare("UPDATE event_outbox SET state=?,delivered_at=?,quarantine_code=?,terminal_at=?,lifecycle_digest=? WHERE event_id=? AND state='pending'").run(state, deliveredAt, code, terminalAt, lifecycleDigest, eventId)) !== 1) fail();
      const updated = await this.#readEvent(eventId); if (updated === null) fail(); return updated;
    }));
  }

  async #readEvent(eventId: string): Promise<NodeJournalEventRecord | null> {
    const row = this.#db.prepare("SELECT * FROM event_outbox WHERE event_id=?").get(eventId) as Row | undefined;
    if (row === undefined) return null;
    const event = await this.#eventFromRow(row);
    const accepted = await this.#readAcceptedEventRoute(event.runId); if (accepted === null) fail();
    this.#requireEventAuthority(event.event, accepted.route, accepted.receipt.result.runAccepted);
    return event;
  }
  async #eventFromRow(row: Row): Promise<NodeJournalEventRecord> {
    const event = await validateNodeEvent(parseCanonicalJson(row.event_json));
    const eventId = nodeEventId(event), runId = nodeEventRunId(event), eventDigest = nodeEventDigest(event);
    if (eventId !== row.event_id || runId !== row.run_id || eventDigest !== row.event_digest) fail();
    const state = row.state;
    if (!eventStates.includes(state as EventState) || !Number.isSafeInteger(row.ordinal) || Number(row.ordinal) < 1 || !Number.isSafeInteger(row.global_order) || Number(row.global_order) < 1) fail();
    const ordinal = Number(row.ordinal), globalOrder = Number(row.global_order);
    if (eventAppendPositionDigest({ eventId, runId, eventDigest, ordinal, globalOrder }) !== row.append_position_digest) fail();
    if (!Number.isSafeInteger(row.delivery_attempt_count) || Number(row.delivery_attempt_count) < 0) fail();
    const deliveryAttemptCount = Number(row.delivery_attempt_count);
    const lastDeliveryFailureJson = row.last_delivery_failure_json === null ? null : String(row.last_delivery_failure_json);
    const lastDeliveryFailure = lastDeliveryFailureJson === null ? null : parseDeliveryFailure(lastDeliveryFailureJson);
    if ((deliveryAttemptCount === 0) !== (lastDeliveryFailure === null)) fail();
    const recordedAt = timestamp(String(row.recorded_at)), deliveredAt = row.delivered_at === null ? null : timestamp(String(row.delivered_at));
    const persistedQuarantineCode = row.quarantine_code === null ? null : quarantineCode(row.quarantine_code), terminalAt = row.terminal_at === null ? null : timestamp(String(row.terminal_at));
    const record: NodeJournalEventRecord = { eventId, runId, digest: String(row.event_digest), event, recordedAt, ordinal, globalOrder, state: state as EventState, deliveryAttemptCount, lastDeliveryFailure, deliveredAt, quarantineCode: persistedQuarantineCode, terminalAt };
    if ((state === "pending" && (record.deliveredAt !== null || record.quarantineCode !== null || record.terminalAt !== null))
      || (state === "delivered" && (record.deliveredAt === null || record.quarantineCode !== null || record.terminalAt !== record.deliveredAt))
      || (state === "quarantined" && (record.deliveredAt !== null || record.quarantineCode === null || record.terminalAt === null))) fail();
    if (row.lifecycle_digest !== eventLifecycleDigest({ eventId, runId, eventDigest, recordedAt, state: state as EventState, deliveryAttemptCount, lastDeliveryFailureJson, deliveredAt, quarantineCode: persistedQuarantineCode, terminalAt })) fail();
    return record;
  }

  #requireEventAuthority(event: RouterRunLifecycleEnvelope, route: NodeAcceptedRunRoute, authorityValue: unknown): void {
    const authority = parseRecord(authorityValue);
    const startTask = parseRecord(authority.task), startAttempt = parseRecord(authority.attempt), startRun = immutableRunIdentity(authority.run);
    const envelope = eventEnvelope(event), eventSource = runtimeSourceSessionIdentity(envelope.task.sourceRef);
    // The source terms are implied by `same(envelope.task, startTask)`; they are checked anyway because the
    // conversation identity is what this check guarantees.
    if (envelope.run.runId !== route.runId || envelope.task.taskId !== route.taskId || envelope.run.taskId !== route.taskId
      || eventSource.system !== route.sourceSystem || eventSource.sessionId !== route.sourceSessionId || envelope.run.sessionBindingId !== route.sessionBindingId
      || !same(envelope.task, startTask) || !same(envelope.attempt, startAttempt) || !same(immutableRunIdentity(envelope.run), startRun)
      || envelope.correlationId !== authority.correlationId || envelope.operationId !== authority.operationId || envelope.causationMessageId !== authority.causationMessageId) fail("axis.node.binding_conflict");
    // A delivery's conversation is already pinned by the checks above; it is checked against the route anyway.
    // Only Linear routes mint `run_agent_response` deliveries.
    if ("deliveryType" in event) {
      if (event.deliveryType === "run_agent_response") {
        const conversation = event.agentActivity.conversation;
        if (route.sourceSystem !== "linear" || conversation.system !== "linear" || conversation.agentSessionId !== route.sourceSessionId
          || event.agentActivity.runId !== route.runId || event.agentActivity.taskId !== route.taskId
          || event.agentActivity.attemptId !== startAttempt.attemptId
          || event.agentActivity.operationId !== authority.operationId) fail("axis.node.binding_conflict");
      }

    }
  }

  async #transactionAsync<T>(operation: () => Promise<T>): Promise<T> {
    this.#db.exec("BEGIN IMMEDIATE");
    try { const result = await operation(); this.#db.exec("COMMIT"); return result; }
    catch (error) { try { this.#db.exec("ROLLBACK"); } catch { /* preserve original */ } throw error; }
  }
  async #guard<T>(operation: () => T | Promise<T>): Promise<T> {
    if (this.#closed) throw storageError();
    this.#pendingOperations += 1;
    const queued = this.#operationTail.then(async () => {
      if (this.#closed) throw storageError();
      try {
        const dataVersion = this.#readDataVersion();
        if (this.#dataVersion !== null && dataVersion !== this.#dataVersion) this.#commandCache.clear();
        this.#dataVersion = dataVersion;
        return await operation();
      } catch (error) { if (error instanceof NodeContractError) throw error; throw storageError(); }
    });
    this.#operationTail = queued.then(() => undefined, () => undefined);
    try { return await queued; }
    finally { this.#pendingOperations -= 1; }
  }

  #readDataVersion(): number {
    const value = Number((this.#db.prepare("PRAGMA data_version").get() as { data_version?: unknown } | undefined)?.data_version);
    if (!Number.isSafeInteger(value) || value < 1) fail();
    return value;
  }
}
