import type { AxisConfiguration } from "../configuration.js";
import { validateHostState } from "./host-state.js";
import type { HostRole, HostState, LifecycleReceipt, WorkerRuntime } from "./host-state.js";
import { hostInstallIdentity, HostStateError, receipt, serviceRef } from "./host-state.js";
import type { NodeBlockerCounts, NodeMaintenanceStatus } from "../shared/router-node.js";

export interface StagedRelease { readonly packagePath: string; readonly version: string; readonly manifestDigest: string; }
export interface LifecycleAdapter {
  readonly acquireLock: () => Promise<() => Promise<void>>;
  readonly readState: () => Promise<HostState | null>;
  readonly managedPlistExists: (path: string) => Promise<boolean>;
  readonly managedPlistMatches: (state: HostState) => Promise<boolean>;
  readonly stage: (role: HostRole, runtime: WorkerRuntime | null) => Promise<StagedRelease>;
  readonly verifyDrain: (state: HostState, configuration: AxisConfiguration) => Promise<void>;
  readonly nodeMaintenance: (state: HostState, configuration: AxisConfiguration, operation: "drain" | "resume" | "status") => Promise<NodeMaintenanceStatus>;
  /** The Node upgrade gate applied to a still-running Node service's own raw counts (`upgrade()`'s gate 1). */
  readonly assertNodeUpgradeGate: (state: HostState, configuration: AxisConfiguration, raw: NodeBlockerCounts) => Promise<void>;
  readonly loadConfiguration: (reference: HostState["configuration"]) => Promise<AxisConfiguration>;
  readonly activate: (state: HostState) => Promise<void>;
  readonly deactivate: () => Promise<void>;
  readonly verify: (state: HostState) => Promise<void>;
  readonly backup: (state: HostState) => Promise<void>;
  readonly restore: () => Promise<HostState>;
  readonly cleanupNew: () => Promise<void>;
  /**
   * `upgrade --force`: the storage it archives, planned into a fresh `archive/<UTC timestamp>` this method has
   * checked is free and on the same volume as that storage. `null` for a role whose storage stays in place (controller).
   */
  readonly planForcedArchive: (state: HostState, configuration: AxisConfiguration) => Promise<ForcedArchive | null>;
  /**
   * Stops the managed service and waits, within a bound, until it has really stopped (launchd no longer lists
   * it and the process it ran has exited). Throws if it has not stopped in time, so nothing is archived under a live
   * process.
   */
  readonly stopService: (state: HostState) => Promise<void>;
  /** Moves (never copies or deletes) the planned storage into its archive; throws `ForcedUpgradeError("archive")` with what already moved. */
  readonly archiveForcedStorage: (state: HostState, configuration: AxisConfiguration, plan: ForcedArchive, target: StagedRelease) => Promise<ForcedArchive>;
}

export interface InstallRequest { readonly role: HostRole; readonly workerRuntime: WorkerRuntime | null; readonly configuration: HostState["configuration"]; readonly serviceLabel?: string; }
function assertInstallRequest(request: InstallRequest): void {
  if ((request.role === "controller" || request.role === "node") && request.workerRuntime === null) return;
  if (request.role === "worker" && (request.workerRuntime === "codex" || request.workerRuntime === "claude")) return;
  throw new HostStateError("invalid lifecycle identity");
}

export function serviceLabelFor(request: InstallRequest): string {
  return request.serviceLabel ?? (request.role === "controller" ? "com.axis.axis-linear-relay" : request.role === "node" ? "com.axis.axis-node" : `com.axis.axis-${request.workerRuntime}-worker`);
}
export function makeState(input: { readonly request: InstallRequest; readonly managedPlist: string; readonly release: StagedRelease; readonly revision: number; }): HostState {
  assertInstallRequest(input.request);
  const serviceLabel = serviceLabelFor(input.request);
  const identity = { role: input.request.role, workerRuntime: input.request.workerRuntime, serviceLabel, configuration: input.request.configuration, managedPlist: input.managedPlist, activeRelease: input.release };
  return validateHostState({ schemaVersion: "axis.host.v1", ...identity, installIdentity: hostInstallIdentity(identity), revision: input.revision });
}

export async function install(adapter: LifecycleAdapter, request: InstallRequest, managedPlist: string): Promise<LifecycleReceipt> {
  assertInstallRequest(request);
  const releaseLock = await adapter.acquireLock();
  try {
  const existing = await adapter.readState();
  if (existing !== null) {
    if (existing.role !== request.role || existing.workerRuntime !== request.workerRuntime || JSON.stringify(existing.configuration) !== JSON.stringify(request.configuration) || existing.serviceLabel !== serviceLabelFor(request) || existing.managedPlist !== managedPlist || !await adapter.managedPlistMatches(existing)) throw new HostStateError("existing lifecycle identity mismatch");
    await adapter.loadConfiguration(request.configuration); await adapter.verify(existing); return receipt("install", existing, "installed");
  }
  if (await adapter.managedPlistExists(managedPlist)) throw new HostStateError("existing managed lifecycle state");
  await adapter.loadConfiguration(request.configuration);
  const release = await adapter.stage(request.role, request.workerRuntime);
  const state = makeState({ request, managedPlist, release, revision: 1 });
  try {
    await adapter.activate(state); await adapter.verify(state);
    return receipt("install", state, "installed");
  } catch (error) {
    try { await adapter.deactivate(); } catch { throw new HostStateError("installation recovery failed"); }
    await adapter.cleanupNew();
    throw error;
  }
  } finally { await releaseLock(); }
}

export async function upgrade(adapter: LifecycleAdapter): Promise<LifecycleReceipt> {
  const releaseLock = await adapter.acquireLock();
  try {
  const current = await adapter.readState();
  if (current === null || !await adapter.managedPlistMatches(current)) throw new HostStateError("missing or unmanaged lifecycle state");
  const configuration = await adapter.loadConfiguration(current.configuration);
  if (current.role === "node") {
    const draining = await adapter.nodeMaintenance(current, configuration, "drain");
    if (draining.mode !== "drain") throw new HostStateError("active work or unknown evidence prevents Node upgrade");
    await adapter.assertNodeUpgradeGate(current, configuration, draining.blockerCounts);
  } else await adapter.verifyDrain(current, configuration);
  const release = await adapter.stage(current.role, current.workerRuntime);
  if (release.version === current.activeRelease.version && release.manifestDigest === current.activeRelease.manifestDigest) {
    await adapter.verify(current);
    if (current.role === "node") {
      const resumed = await adapter.nodeMaintenance(current, configuration, "resume");
      if (resumed.mode !== "resume") throw new HostStateError("Node maintenance resume failed");
    }
    return receipt("upgrade", current, "upgraded");
  }
  const candidate = makeState({ request: { role: current.role, workerRuntime: current.workerRuntime, configuration: current.configuration, serviceLabel: current.serviceLabel }, managedPlist: current.managedPlist, release, revision: current.revision + 1 });
  await adapter.deactivate();
  let backedUp = false;
  try {
    await adapter.verifyDrain(current, configuration); await adapter.backup(current); backedUp = true; await adapter.activate(candidate); await adapter.verify(candidate);
    if (candidate.role === "node") {
      const resumed = await adapter.nodeMaintenance(candidate, configuration, "resume");
      if (resumed.mode !== "resume") throw new HostStateError("Node maintenance resume failed");
    }
    return receipt("upgrade", candidate, "upgraded");
  } catch (error) {
    let recoveryState = current;
    if (backedUp) { await adapter.deactivate(); recoveryState = await adapter.restore(); }
    await adapter.activate(recoveryState); await adapter.verify(recoveryState);
    return receipt("upgrade", recoveryState, "rolled_back", true);
  }
  } finally { await releaseLock(); }
}

export interface ForcedArchiveFile { readonly name: string; readonly bytes: number; }
/** An archive directory relative to the host root (`archive/<UTC timestamp>`), and the files moved into it. */
export interface ForcedArchive { readonly directory: string; readonly files: readonly ForcedArchiveFile[]; }
export type ForcedUpgradeStage = "stop" | "archive" | "activate" | "verify" | "resume";

export interface ForcedUpgradeReceipt {
  readonly schemaVersion: "axis.host.forced-upgrade.v1";
  readonly operation: "upgrade";
  readonly forced: true;
  readonly status: "upgraded" | "already_current";
  readonly role: HostRole;
  readonly version: string;
  readonly manifestDigest: string;
  /** A domain-separated identity reference; never the launchd label itself. */
  readonly serviceRef: string;
  readonly archive: ForcedArchive | null;
  readonly rollback: false;
}

/**
 * A forced upgrade that stopped after it began stopping the service. Nothing is rolled back: `stage` says where it
 * stopped, and `archive` what had already moved (`null` when nothing is known to have moved), so the operator can act.
 */
export class ForcedUpgradeError extends HostStateError {
  readonly stage: ForcedUpgradeStage;
  readonly archive: ForcedArchive | null;
  constructor(stage: ForcedUpgradeStage, archive: ForcedArchive | null) { super(`forced upgrade stopped at ${stage}`); this.stage = stage; this.archive = archive; }
}

function forcedReceipt(status: ForcedUpgradeReceipt["status"], state: HostState, archive: ForcedArchive | null): ForcedUpgradeReceipt {
  return { schemaVersion: "axis.host.forced-upgrade.v1", operation: "upgrade", forced: true, status, role: state.role, version: state.activeRelease.version, manifestDigest: state.activeRelease.manifestDigest, serviceRef: serviceRef(state.serviceLabel), archive, rollback: false };
}

/**
 * `axis upgrade --force`, the owner's forced upgrade: stop the service regardless of in-flight work, upgrade,
 * start it again. Lost in-flight work is accepted, because the work lives in Linear and can be dispatched again.
 *
 * - No drain check of any kind: no Node maintenance, no Controller or Worker database drain, no `verifyDrain`.
 * - It stages first, as `upgrade` does. A staging failure leaves the service running, and staging is how it learns
 *   the host is already on the target release, in which case it only verifies and archives nothing.
 * - Otherwise it checks the archive directory is free before stopping anything. Then it stops the service, moves the
 *   storage a new release would refuse or wedge on (the Node journal or the Worker database, with WAL/SHM; nothing for
 *   a controller), activates, and verifies.
 * - Nothing is rolled back. A failure from the stop onwards throws `ForcedUpgradeError` and leaves everything where it
 *   is; archived files are never moved back.
 */
export async function forceUpgrade(adapter: LifecycleAdapter): Promise<ForcedUpgradeReceipt> {
  const releaseLock = await adapter.acquireLock();
  try {
    const current = await adapter.readState();
    if (current === null || !await adapter.managedPlistMatches(current)) throw new HostStateError("missing or unmanaged lifecycle state");
    const configuration = await adapter.loadConfiguration(current.configuration);
    const release = await adapter.stage(current.role, current.workerRuntime);
    if (release.version === current.activeRelease.version && release.manifestDigest === current.activeRelease.manifestDigest) {
      try { await adapter.verify(current); } catch { throw new ForcedUpgradeError("verify", null); }
      // A Node left drained by an earlier refused `upgrade` verifies ready yet refuses every command, because readiness
      // does not report maintenance mode. Resuming is not a drain check: it is what forcing a current release into
      // service means. (A new release starts on a fresh journal, which is already resumed.)
      if (current.role === "node") {
        let resumed: NodeMaintenanceStatus;
        try { resumed = await adapter.nodeMaintenance(current, configuration, "resume"); } catch { throw new ForcedUpgradeError("resume", null); }
        if (resumed.mode !== "resume") throw new ForcedUpgradeError("resume", null);
      }
      return forcedReceipt("already_current", current, null);
    }
    const plan = await adapter.planForcedArchive(current, configuration);
    const candidate = makeState({ request: { role: current.role, workerRuntime: current.workerRuntime, configuration: current.configuration, serviceLabel: current.serviceLabel }, managedPlist: current.managedPlist, release, revision: current.revision + 1 });
    // Not `deactivate` alone: renaming storage while the old process still has it open would let that process keep
    // writing into the archived files, and the new release would find its port taken.
    try { await adapter.stopService(current); } catch { throw new ForcedUpgradeError("stop", null); }
    let archive: ForcedArchive | null = null;
    if (plan !== null) {
      try { archive = await adapter.archiveForcedStorage(current, configuration, plan, release); }
      catch (error) { if (error instanceof ForcedUpgradeError) throw error; throw new ForcedUpgradeError("archive", null); }
    }
    try { await adapter.activate(candidate); } catch { throw new ForcedUpgradeError("activate", archive); }
    try { await adapter.verify(candidate); } catch { throw new ForcedUpgradeError("verify", archive); }
    return forcedReceipt("upgraded", candidate, archive);
  } finally { await releaseLock(); }
}
