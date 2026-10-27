import { selectedWorker } from "../configuration.js";
import { loadHostConfiguration, managedConfigurationReference } from "./configuration.js";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline/promises";

import { HostStateError } from "./host-state.js";
import type { HostRole, HostState, LifecycleReceipt } from "./host-state.js";
import { MacosLifecycleAdapter } from "./macos-adapter.js";
import type { AcknowledgeAmbiguousCommandInput, AcknowledgeAmbiguousCommandResult, NodeAmbiguousCommandListingEntry, NodeAmbiguousCommandsListing, NodeResolvedAmbiguousCommandListingEntry } from "./macos-adapter.js";
import { NodeUpgradeBlockedError, STUCK_RUN_ACKNOWLEDGEMENT_REASON_LIMIT, WorkerUpgradeBlockedError } from "./stuck-run-acknowledgements.js";
import { forceUpgrade, ForcedUpgradeError, install, upgrade } from "./transaction.js";
import type { ForcedUpgradeReceipt, ForcedUpgradeStage } from "./transaction.js";
import type { NodeMaintenanceStatus } from "../shared/router-node.js";

type Output = "human" | "json";
interface Parsed { readonly positional: readonly string[]; readonly flags: ReadonlyMap<string, string>; }
export interface LifecycleIo { readonly stdout: (text: string) => void; readonly stderr: (text: string) => void; readonly isTTY: boolean; readonly chooseRole?: () => Promise<HostRole>; }

/** Flags that are present or absent and never take a value. Only `upgrade` accepts one; `only` refuses it elsewhere. */
const PRESENCE_FLAGS: ReadonlySet<string> = new Set(["force"]);
function parse(argv: readonly string[]): Parsed {
  const positional: string[] = []; const flags = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index]; if (token === undefined || !token.startsWith("--")) { positional.push(token ?? ""); continue; }
    const name = token.slice(2); if (name.length === 0 || flags.has(name)) throw new HostStateError("invalid input");
    if (PRESENCE_FLAGS.has(name)) { flags.set(name, "present"); continue; }
    const value = argv[index + 1]; if (value === undefined || value.startsWith("--")) throw new HostStateError("invalid input"); flags.set(name, value); index += 1;
  }
  return { positional, flags };
}
function value(parsed: Parsed, name: string): string | undefined { return parsed.flags.get(name); }
function only(parsed: Parsed, names: readonly string[]): void { if ([...parsed.flags.keys()].some((name) => !names.includes(name))) throw new HostStateError("invalid input"); }
function output(parsed: Parsed): Output { const mode = value(parsed, "output") ?? "human"; if (mode !== "human" && mode !== "json") throw new HostStateError("invalid output"); return mode; }
function absolute(value_: string | undefined, name: string): string { if (value_ === undefined || !value_.startsWith("/")) throw new HostStateError(`invalid ${name}`); return value_; }
function role(value_: string | undefined): HostRole | undefined { if (value_ === undefined) return undefined; if (value_ === "controller" || value_ === "worker" || value_ === "node") return value_; throw new HostStateError("invalid role"); }
function sourceDefault(): string { return fileURLToPath(new URL("../..", import.meta.url)); }
function print(io: LifecycleIo, mode: Output, document: LifecycleReceipt): void { if (mode === "json") io.stdout(`${JSON.stringify(document)}\n`); else io.stdout(`axis ${document.operation} ${document.status}: role=${document.role} version=${document.version} service=${document.serviceRef} rollback=${document.rollback}\n`); }
function printMaintenance(io: LifecycleIo, mode: Output, operation: "drain" | "resume" | "status", document: NodeMaintenanceStatus, note?: string): void {
  if (mode === "json") { io.stdout(`${JSON.stringify(document)}\n`); return; }
  io.stdout(`axis node ${operation}: mode=${document.mode} idle=${document.idle} blockers=${JSON.stringify(document.blockerCounts)}\n`);
  if (note !== undefined) io.stdout(`${note}\n`);
}
/**
 * The `axis node drain|status` `note:` line -- human mode only, and it never fails the command.
 * A local read failure (the Node's own acknowledgement record, or a linked Worker's) falls back to the
 * pending wording with no counts, rather than surfacing a second error for what is already visible via
 * `axis node ambiguous-commands list`.
 */
async function ambiguousDrainNote(adapter: MacosLifecycleAdapter, state: HostState, configuration: import("../configuration.js").AxisConfiguration, document: NodeMaintenanceStatus): Promise<string> {
  const pending = "note: An ambiguous command's Worker reply was lost and Axis never resolves it on its own. See axis node ambiguous-commands list --host-root <host-root>; release each for upgrade with axis node ambiguous-commands acknowledge.";
  try {
    const listing = await adapter.listAmbiguousCommands(state, configuration);
    const n = document.blockerCounts.ambiguousCommands, k = document.blockerCounts.activeWorkerRuns;
    const m = listing.upgradeGate.acknowledged.ambiguousCommands, released = listing.upgradeGate.acknowledged.activeWorkerRuns;
    if (m >= n) return `note: ambiguousCommands=${n} (${m} acknowledged), activeWorkerRuns=${k} (${released} released). These rows stay as evidence; axis upgrade will not be blocked by them. The same release applies to the linked Worker's own axis upgrade.`;
    return `note: ambiguousCommands=${n} (${m} acknowledged). An ambiguous command's Worker reply was lost and Axis never resolves it on its own. See axis node ambiguous-commands list --host-root <host-root>; release each for upgrade with axis node ambiguous-commands acknowledge.`;
  } catch { return pending; }
}
type AmbiguousCommandsErrorCode = "unauthorized" | "not_ambiguous" | "invalid_input" | "too_recent" | "unavailable";
class AmbiguousCommandsCliError extends Error { constructor(readonly code: AmbiguousCommandsErrorCode) { super(code); } }
const AMBIGUOUS_COMMANDS_SCHEMA_VERSION = "axis.node.ambiguous-commands.v1";
const AMBIGUOUS_COMMANDS_ERROR_MESSAGES: Readonly<Record<AmbiguousCommandsErrorCode, string>> = {
  unauthorized: "Explicit operator authority is required for this mutation.",
  not_ambiguous: "That command is not ambiguous on this Node, so there is nothing to acknowledge. Run axis node ambiguous-commands list --host-root <host-root> to see the ones that are.",
  invalid_input: "An argument is missing or malformed; nothing was recorded.",
  too_recent: "That command became ambiguous less than 15 minutes ago; its Worker may still be finishing the launch, or Axis may still resolve it on its own. Nothing was recorded. Wait, then run axis node ambiguous-commands list --host-root <host-root> again.",
  unavailable: "The Node journal, the linked Worker's host root or database, or an acknowledgement record could not be read, verified or written; nothing was recorded. Check that the Worker is listed in nodes.workers[].hostRoot, that its host state and TOML file are owner-private and belong to this user, then run the command again.",
};
function printAmbiguousCommandsError(io: LifecycleIo, mode: Output, code: AmbiguousCommandsErrorCode): void {
  const message = AMBIGUOUS_COMMANDS_ERROR_MESSAGES[code];
  if (mode === "json") io.stdout(`${JSON.stringify({ schemaVersion: AMBIGUOUS_COMMANDS_SCHEMA_VERSION, error: { code, message } })}\n`);
  else io.stderr(`${message}\n`);
}
function ambiguousAcknowledgeInput(parsed: Parsed): AcknowledgeAmbiguousCommandInput {
  const authority = value(parsed, "authority");
  if (authority === undefined) throw new AmbiguousCommandsCliError("unauthorized");
  const commandId = value(parsed, "command"), operator = value(parsed, "operator"), source = value(parsed, "audit-source"), request = value(parsed, "request");
  if (commandId === undefined || operator === undefined || source === undefined || request === undefined) throw new AmbiguousCommandsCliError("invalid_input");
  const opaque = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
  for (const candidate of [commandId, operator, authority, source, request]) if (!opaque.test(candidate)) throw new AmbiguousCommandsCliError("invalid_input");
  const reasonValue = value(parsed, "reason");
  if (reasonValue !== undefined) {
    // eslint-disable-next-line no-control-regex
    if (reasonValue.length === 0 || reasonValue.length > STUCK_RUN_ACKNOWLEDGEMENT_REASON_LIMIT || /[\u0000-\u001F\u007F]/u.test(reasonValue)) throw new AmbiguousCommandsCliError("invalid_input");
  }
  return { commandId, operatorRef: operator, authorizationRef: authority, source, requestRef: request, reason: reasonValue ?? null };
}
function acknowledgementTail(acknowledgement: { readonly acknowledgedAt: string; readonly operatorRef: string; readonly authorizationRef: string; readonly source: string; readonly requestRef: string; readonly reason: string | null } | null): string {
  const reasonText = acknowledgement === null || acknowledgement.reason === null ? "none" : JSON.stringify(acknowledgement.reason);
  return `acknowledged=${acknowledgement?.acknowledgedAt ?? "none"} operator=${acknowledgement?.operatorRef ?? "none"} authority=${acknowledgement?.authorizationRef ?? "none"} auditSource=${acknowledgement?.source ?? "none"} request=${acknowledgement?.requestRef ?? "none"} reason=${reasonText}`;
}
function ambiguousRow(entry: NodeAmbiguousCommandListingEntry): string {
  const { row, worker, acknowledgement, workerRelease } = entry;
  return `ambiguousCommand: command=${row.commandId} operation=${row.operation} run=${row.runId ?? "none"} prior=${row.priorRunId ?? "none"} task=${row.taskId} conversation=${row.sourceSystem}:${row.sourceSessionId} worker=${row.workerId} created=${row.createdAt} updated=${row.updatedAt} workerRun=${worker.state} cause=${worker.observationalCause ?? "none"} workerRecorded=${worker.recorded ? "yes" : "no"} workerRelease=${workerRelease} ${acknowledgementTail(acknowledgement)}`;
}
function resolvedRow(entry: NodeResolvedAmbiguousCommandListingEntry): string {
  return `resolvedAfterAcknowledgement: command=${entry.commandId} state=${entry.state} ${acknowledgementTail(entry.acknowledgement)}`;
}
function printAmbiguousCommandsList(io: LifecycleIo, mode: Output, listing: NodeAmbiguousCommandsListing): void {
  if (mode === "json") {
    io.stdout(`${JSON.stringify({
      schemaVersion: AMBIGUOUS_COMMANDS_SCHEMA_VERSION,
      ambiguousCommands: listing.ambiguousCommands.map(({ row, worker, acknowledgement, workerRelease }) => ({
        commandId: row.commandId, operation: row.operation, runId: row.runId, priorRunId: row.priorRunId,
        taskId: row.taskId, sourceSystem: row.sourceSystem, sourceSessionId: row.sourceSessionId, workerId: row.workerId,
        createdAt: row.createdAt, updatedAt: row.updatedAt,
        worker: { run: worker.state, observationalCause: worker.observationalCause, recorded: worker.recorded, release: workerRelease },
        acknowledgement,
      })),
      resolvedAfterAcknowledgement: listing.resolvedAfterAcknowledgement.map(({ commandId, state, acknowledgement }) => ({
        commandId, state,
        acknowledgedAt: acknowledgement?.acknowledgedAt ?? null, operatorRef: acknowledgement?.operatorRef ?? null,
        authorizationRef: acknowledgement?.authorizationRef ?? null, source: acknowledgement?.source ?? null,
        requestRef: acknowledgement?.requestRef ?? null, reason: acknowledgement?.reason ?? null,
      })),
      upgradeGate: listing.upgradeGate,
    })}\n`);
    return;
  }
  io.stdout(`axis node ambiguous-commands list: ambiguous=${listing.ambiguousCommands.length} acknowledged=${listing.upgradeGate.acknowledged.ambiguousCommands} upgradeGate=${listing.upgradeGate.blocked ? "blocked" : "clear"}\n`);
  if (listing.ambiguousCommands.length === 0) io.stdout("ambiguousCommand: none\n");
  else for (const entry of listing.ambiguousCommands) io.stdout(`${ambiguousRow(entry)}\n`);
  for (const entry of listing.resolvedAfterAcknowledgement) io.stdout(`${resolvedRow(entry)}\n`);
}
function printAmbiguousAcknowledge(io: LifecycleIo, mode: Output, result: Extract<AcknowledgeAmbiguousCommandResult, { readonly outcome: "acknowledged" }>, entry: NodeAmbiguousCommandListingEntry | null): void {
  if (mode === "json") {
    io.stdout(`${JSON.stringify({ schemaVersion: AMBIGUOUS_COMMANDS_SCHEMA_VERSION, verification: { node: result.node, worker: result.worker }, acknowledgement: result.acknowledgement, workerAcknowledgement: result.workerAcknowledgement })}\n`);
    return;
  }
  io.stdout(`axis node ambiguous-commands acknowledge: node=${result.node} worker=${result.worker}\n`);
  if (entry !== null) io.stdout(`${ambiguousRow(entry)}\n`);
}
export function nodeUpgradeBlockedText(error: NodeUpgradeBlockedError): string {
  const b = error.blockers, a = error.acknowledged;
  return [
    "axis upgrade refused: this Node is not drained, so nothing was stopped or staged. The Node is now in drain mode and refuses new work until you resume it or an upgrade succeeds.",
    `- ambiguousCommands=${b.ambiguousCommands} not acknowledged (${a.ambiguousCommands} acknowledged): a command whose Worker reply was lost; Axis never resolves it on its own. Inspect with "axis node ambiguous-commands list --host-root <host-root>", then release each one with "axis node ambiguous-commands acknowledge --host-root <host-root> --command <command-id> --operator <operator-id> --authority <authority-ref> --audit-source <source> --request <request-id>". The command is kept as evidence and nothing is re-sent; the same acknowledgement also releases the linked Worker's own upgrade.`,
    `- activeWorkerRuns=${b.activeWorkerRuns}: a Worker Run is active, or is stuck reserved or observational. An active Run finishes on its own: wait and run axis upgrade again. A stuck Run whose command is ambiguous is released with that command's acknowledgement. Any other stuck Run has no release command yet; axis upgrade --force skips drain and archives this Node's journal.`,
    `- reservedCommands=${b.reservedCommands} / dispatchingCommands=${b.dispatchingCommands}: a dispatch is in flight; wait a few seconds and run axis upgrade again.`,
    `- pendingNodeEvents=${b.pendingNodeEvents} / pendingWorkerOutbox=${b.pendingWorkerOutbox}: events not yet delivered; they drain once Router is reachable. Check that Router is up, wait, and run axis upgrade again.`,
    `- quarantinedNodeEvents=${b.quarantinedNodeEvents}: events this Node gave up delivering; no release command exists yet; axis upgrade --force skips drain and archives this Node's journal.`,
    `- unknownWorkerEvidence=${b.unknownWorkerEvidence}: a configured Worker's database could not be read or verified. Check nodes.workers[].hostRoot and that Worker's host state, then run axis upgrade again.`,
    "To take work again without upgrading: axis node resume --host-root <host-root>",
  ].join("\n");
}
export function nodeUpgradeBlockedDocument(error: NodeUpgradeBlockedError): Readonly<Record<string, unknown>> {
  return { schemaVersion: "axis.host.receipt.v1", error: { code: "node_not_drained", blockers: error.blockers, acknowledged: error.acknowledged } };
}
function printNodeUpgradeBlocked(io: LifecycleIo, mode: Output, error: NodeUpgradeBlockedError): void {
  if (mode === "json") io.stdout(`${JSON.stringify(nodeUpgradeBlockedDocument(error))}\n`);
  else io.stderr(`${nodeUpgradeBlockedText(error)}\n`);
}
export function workerUpgradeBlockedText(error: WorkerUpgradeBlockedError): string {
  const lines = [
    "axis upgrade refused: this Worker's database still holds work that blocks an upgrade, so nothing was staged or replaced.",
    `- stuck Runs=${error.stuckRuns} (${error.releasedStuckRuns} released): Runs left reserved or observational. A stuck Run whose command is ambiguous on its Node is released from the Node host with "axis node ambiguous-commands list --host-root <node-host-root>" and "axis node ambiguous-commands acknowledge …"; the Worker needs no command of its own. Any other stuck Run has no release command yet; axis upgrade --force skips drain and archives this Worker's database.`,
  ];
  for (const table of error.otherBlockingTables) lines.push(`- ${table}: active work or undelivered events that drain on their own; wait and run axis upgrade again.`);
  return lines.join("\n");
}
export function workerUpgradeBlockedDocument(error: WorkerUpgradeBlockedError): Readonly<Record<string, unknown>> {
  return { schemaVersion: "axis.host.receipt.v1", error: { code: "worker_not_drained", stuckRuns: error.stuckRuns, releasedStuckRuns: error.releasedStuckRuns, otherBlockingTables: error.otherBlockingTables } };
}
function printWorkerUpgradeBlocked(io: LifecycleIo, mode: Output, error: WorkerUpgradeBlockedError): void {
  if (mode === "json") io.stdout(`${JSON.stringify(workerUpgradeBlockedDocument(error))}\n`);
  else io.stderr(`${workerUpgradeBlockedText(error)}\n`);
}
function forcedFiles(archive: ForcedUpgradeReceipt["archive"]): string { return archive === null || archive.files.length === 0 ? "none" : archive.files.map((file) => `${file.name}:${file.bytes}`).join(","); }
function printForced(io: LifecycleIo, mode: Output, document: ForcedUpgradeReceipt): void {
  if (mode === "json") io.stdout(`${JSON.stringify(document)}\n`);
  else io.stdout(`axis upgrade --force ${document.status}: role=${document.role} version=${document.version} service=${document.serviceRef} archive=${document.archive?.directory ?? "none"} files=${forcedFiles(document.archive)} rollback=false\n`);
}
/** What the operator does next, by the stage a forced upgrade stopped at. It never claims an archive the error does not carry. */
const FORCED_NEXT_STEP: Readonly<Record<ForcedUpgradeStage, string>> = Object.freeze({
  stop: "The managed service could not be stopped. Stop it with launchctl bootout, then run axis upgrade --force again.",
  archive: "The service is stopped. Fix the cause (permissions or disk), then run axis upgrade --force again: it moves what is left into a new archive directory, so the archive then spans two directories, each with its own manifest.",
  activate: "The service is stopped and the new release did not start. Run axis upgrade --force again. If that only verifies, the host state already names the new release: load the managed LaunchAgent with launchctl bootstrap first. If it refuses the host state as unmanaged, the state file and the plist disagree and need manual repair.",
  verify: "Readiness or the unauthenticated 401 check failed. Check that the managed service is loaded, its log and its configuration, then run axis upgrade --force again: on the target release it only verifies.",
  resume: "The Node is on the target release and ready, but its maintenance could not be resumed. Run axis node resume, or axis upgrade --force again.",
});
/** The document `axis upgrade --force --output json` prints for a failure from the stop onwards: the stage and what moved, never paths or credentials. */
export function forcedFailureDocument(error: ForcedUpgradeError): Readonly<Record<string, unknown>> {
  return { schemaVersion: "axis.host.forced-upgrade.v1", operation: "upgrade", forced: true, rollback: false, error: { code: "forced_upgrade_incomplete", stage: error.stage, archive: error.archive } };
}
/** The human text for the same failure. */
export function forcedFailureText(error: ForcedUpgradeError): string {
  const archive = error.archive === null ? "Nothing was archived by this run." : `Archived into ${error.archive.directory}: ${forcedFiles(error.archive)}. Archived files are not put back.`;
  return `axis upgrade --force stopped at ${error.stage}; nothing was rolled back. ${archive} ${FORCED_NEXT_STEP[error.stage]}`;
}
function printForcedFailure(io: LifecycleIo, mode: Output, error: ForcedUpgradeError): void {
  if (mode === "json") io.stdout(`${JSON.stringify(forcedFailureDocument(error))}\n`);
  else io.stderr(`${forcedFailureText(error)}\n`);
}
function forcedPreflightFailure(io: LifecycleIo, mode: Output): void {
  if (mode === "json") io.stdout(`${JSON.stringify({ schemaVersion: "axis.host.forced-upgrade.v1", operation: "upgrade", forced: true, rollback: false, error: { code: "invalid_or_unavailable", stage: "preflight" } })}\n`);
  else io.stderr("axis upgrade --force stopped, archived and activated nothing: its input, the managed host state, release staging, or the archive check failed.\n");
}
function failure(io: LifecycleIo, mode: Output): void { if (mode === "json") io.stdout(`${JSON.stringify({ schemaVersion: "axis.host.receipt.v1", error: { code: "invalid_or_unavailable" } })}\n`); else io.stderr("Axis host lifecycle could not safely complete; no further action was taken.\n"); }

async function promptRole(): Promise<HostRole> {
  const line = createInterface({ input: process.stdin, output: process.stderr });
  try { const answer = (await line.question("Axis host role: 1) controller  2) worker  3) node [1/2/3]: ")).trim(); if (answer === "1" || answer === "controller") return "controller"; if (answer === "2" || answer === "worker") return "worker"; if (answer === "3" || answer === "node") return "node"; throw new HostStateError("invalid role"); } finally { line.close(); }
}

export function lifecycleExitCode(document: LifecycleReceipt): number { return document.status === "installed" || document.status === "upgraded" ? 0 : 1; }
export function isLifecycleCommand(argv: readonly string[]): boolean { return argv[0] === "install" || argv[0] === "upgrade" || argv[0] === "node"; }
export async function runLifecycleCli(argv: readonly string[], io: LifecycleIo = { stdout: (text) => process.stdout.write(text), stderr: (text) => process.stderr.write(text), isTTY: process.stdin.isTTY === true }): Promise<number> {
  let mode: Output = "human"; const forcedCommand = argv[0] === "upgrade" && argv.includes("--force");
  try {
    const parsed = parse(argv); mode = output(parsed); const command = parsed.positional.join(" "); const sourcePackage = absolute(value(parsed, "source-package") ?? sourceDefault(), "source package"); const hostRoot = value(parsed, "host-root"); const adapter = new MacosLifecycleAdapter({ ...(hostRoot === undefined ? {} : { hostRoot: absolute(hostRoot, "host root") }), sourcePackage });
    if (command === "install") {
      only(parsed, ["role", "config", "node", "worker", "service-label", "host-root", "source-package", "output"]);
      const selected = role(value(parsed, "role")) ?? (io.isTTY ? await (io.chooseRole ?? promptRole)() : (() => { throw new HostStateError("role required"); })()); const configuration = managedConfigurationReference(value(parsed, "config") ?? "axis.toml", selected, value(parsed, "node"), value(parsed, "worker"));
      const selectedRuntime = selected === "worker" ? (selectedWorker(loadHostConfiguration(configuration), configuration.workerId!, configuration.nodeId!).worker.kind === "codex" ? "codex" : "claude") : undefined;
      if (((selected === "controller" || selected === "node") && selectedRuntime !== undefined) || (selected === "worker" && selectedRuntime === undefined)) throw new HostStateError("invalid role runtime combination");
      const label = value(parsed, "service-label") ?? (selected === "controller" ? "com.axis.axis-linear-relay" : selected === "node" ? "com.axis.axis-node" : `com.axis.axis-${selectedRuntime}-worker`); if (!/^[A-Za-z][A-Za-z0-9.-]{2,127}$/u.test(label)) throw new HostStateError("invalid service label");
      const result = await install(adapter, { role: selected, workerRuntime: selectedRuntime ?? null, configuration, ...(value(parsed, "service-label") === undefined ? {} : { serviceLabel: label }) }, adapter.managedPlistPathFor(label)); print(io, mode, result); return 0;
    }
    if (command === "upgrade") {
      only(parsed, ["host-root", "source-package", "force", "output"]);
      if (parsed.flags.has("force")) { const forced = await forceUpgrade(adapter); printForced(io, mode, forced); return 0; }
      const result = await upgrade(adapter); print(io, mode, result); return result.status === "upgraded" ? 0 : 1;
    }
    if (command === "node drain" || command === "node resume" || command === "node status") {
      only(parsed, ["host-root", "output"]);
      if (hostRoot === undefined) throw new HostStateError("host root required");
      const releaseLock = await adapter.acquireLock();
      try {
        const state = await adapter.readState();
        if (state === null || state.role !== "node" || !await adapter.managedPlistMatches(state)) throw new HostStateError("missing or unmanaged Node lifecycle state");
        const configuration = await adapter.loadConfiguration(state.configuration);
        const operation = command.slice("node ".length) as "drain" | "resume" | "status";
        const result = await adapter.nodeMaintenance(state, configuration, operation);
        const note = mode === "human" && operation !== "resume" && result.blockerCounts.ambiguousCommands > 0 ? await ambiguousDrainNote(adapter, state, configuration, result) : undefined;
        printMaintenance(io, mode, operation, result, note);
        return operation === "drain" && !result.idle ? 1 : 0;
      } finally { await releaseLock(); }
    }
    if (command === "node ambiguous-commands list" || command === "node ambiguous-commands acknowledge") {
      const acknowledging = command === "node ambiguous-commands acknowledge";
      only(parsed, acknowledging ? ["host-root", "command", "operator", "authority", "audit-source", "request", "reason", "output"] : ["host-root", "output"]);
      if (hostRoot === undefined) throw new HostStateError("host root required");
      try {
        if (!acknowledging) {
          const state = await adapter.readState();
          if (state === null || state.role !== "node" || !await adapter.managedPlistMatches(state)) throw new AmbiguousCommandsCliError("unavailable");
          const configuration = await adapter.loadConfiguration(state.configuration);
          const listing = await adapter.listAmbiguousCommands(state, configuration);
          printAmbiguousCommandsList(io, mode, listing);
          return 0;
        }
        const input = ambiguousAcknowledgeInput(parsed);
        const releaseLock = await adapter.acquireLock();
        try {
          const state = await adapter.readState();
          if (state === null || state.role !== "node" || !await adapter.managedPlistMatches(state)) throw new AmbiguousCommandsCliError("unavailable");
          const configuration = await adapter.loadConfiguration(state.configuration);
          const result = await adapter.acknowledgeAmbiguousCommand(state, configuration, input);
          if (result.outcome !== "acknowledged") throw new AmbiguousCommandsCliError(result.outcome);
          const listing = await adapter.listAmbiguousCommands(state, configuration);
          const entry = listing.ambiguousCommands.find((candidate) => candidate.row.commandId === input.commandId) ?? null;
          printAmbiguousAcknowledge(io, mode, result, entry);
          return 0;
        } finally { await releaseLock(); }
      } catch (error) {
        const failed = error instanceof AmbiguousCommandsCliError ? error : new AmbiguousCommandsCliError("unavailable");
        printAmbiguousCommandsError(io, mode, failed.code);
        return 1;
      }
    }
    throw new HostStateError("invalid lifecycle command");
  } catch (error) {
    if (error instanceof ForcedUpgradeError) printForcedFailure(io, mode, error);
    else if (error instanceof NodeUpgradeBlockedError) printNodeUpgradeBlocked(io, mode, error);
    else if (error instanceof WorkerUpgradeBlockedError) printWorkerUpgradeBlocked(io, mode, error);
    else if (forcedCommand) forcedPreflightFailure(io, mode);
    else failure(io, mode);
    return 1;
  }
}

export function lifecycleHelp(): string { return "  axis install --role controller|worker|node --config <owner-private-axis.toml> [--node <id>] [--worker <id>] [--service-label <safe-label>] [--host-root <absolute>] [--source-package <absolute>] [--output human|json]\n  axis upgrade [--host-root <absolute>] [--source-package <absolute>] [--force] [--output human|json]\n    --force stops the service without any drain check (in-flight work is abandoned), moves a Node journal or Worker database with WAL/SHM into <host-root>/archive/<timestamp>, starts the new release, and never rolls back\n  axis node drain|resume|status --host-root <absolute> [--output human|json]\n  axis node ambiguous-commands list --host-root <absolute> [--output human|json]\n  axis node ambiguous-commands acknowledge --host-root <absolute> --command <command-id> --operator <operator-id> --authority <authority-ref> --audit-source <source> --request <request-id> [--reason <printable text, 1..512 chars>] [--output human|json]\n"; }
