import { deriveNodeConfiguration, deriveWorkerConfiguration, type AxisConfiguration } from "../../../src/configuration.js";
import { managedConfigurationReference } from "../../../src/lifecycle/configuration.js";
import { writeConfigurationFixture } from "../../support/configuration-fixture.js";
import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { runLifecycleCli } from "../../../src/lifecycle/cli.js";
import type { LifecycleIo } from "../../../src/lifecycle/cli.js";
import { MacosLifecycleAdapter, manifestDigest } from "../../../src/lifecycle/macos-adapter.js";
import { makeState } from "../../../src/lifecycle/transaction.js";
import {
  NodeUpgradeBlockedError,
  readNodeAmbiguousAcknowledgements,
  readWorkerStuckRunAcknowledgements,
  WorkerUpgradeBlockedError,
} from "../../../src/lifecycle/stuck-run-acknowledgements.js";
import { SqliteWorkerCommandJournal } from "../../../src/index.js";
import { NodeCommandService } from "../../../src/node/command.js";
import { SqliteNodeJournal } from "../../../src/node/journal.js";
import type { NodeWorkerInventoryPort } from "../../../src/shared/router-node-commands.js";
import { nodeFixtureTarget } from "../../support/router-node-fixtures.js";
import { buildNodeStartRequest, nodeAuthenticatedContext, nodeReservation } from "../../support/router-node-commands-fixtures.js";

/**
 * Exercise Node and Worker upgrade gates against real journal files. A throwing
 * dispatch makes NodeCommandService record launch_ambiguous without needing HTTP. The
 * Worker's blocked row is constructed with reserveRun and recordObservational.
 * Acknowledgement releases the respective gates while retaining ambiguous evidence.
 */

const T0 = Date.parse("2026-01-01T00:00:00.000Z");
const FIFTEEN_MINUTES_MS = 900_000;

function plistPath(label: string): string { return `/Users/operator/Library/LaunchAgents/${label}.plist`; }

/**
 * `verifyDrain`'s final step, `#verifyStateRelease`, recomputes the release's manifest digest from the real
 * `releases/<id>/node_modules` tree on disk (`manifestDigest`) and requires `activeRelease.packagePath` to
 * live under `<hostRoot>/releases/`. A made-up digest fails that check, so the fixture builds a real
 * (minimal) release tree and hashes it exactly the way `MacosLifecycleAdapter.stage()` does.
 */
async function buildRelease(hostRoot: string): Promise<{ readonly packagePath: string; readonly version: string; readonly manifestDigest: string }> {
  const nodeModules = join(hostRoot, "releases", "r1", "node_modules");
  const packagePath = join(nodeModules, "@axisosme", "axis");
  await mkdir(packagePath, { recursive: true });
  await writeFile(join(packagePath, "package.json"), JSON.stringify({ name: "@axisosme/axis", version: "0.99.0" }));
  return { packagePath, version: "0.99.0", manifestDigest: await manifestDigest(nodeModules) };
}

async function writeHostState(input: {
  readonly hostRoot: string;
  readonly configuration: AxisConfiguration;
  readonly role: "node" | "worker";
  readonly workerRuntime: "codex" | null;
  readonly serviceLabel: string;
}) {
  await mkdir(input.hostRoot, { recursive: true, mode: 0o700 });
  const state = makeState({
    request: { role: input.role, workerRuntime: input.workerRuntime, configuration: managedConfigurationReference(input.configuration.filename, input.role), serviceLabel: input.serviceLabel },
    managedPlist: plistPath(input.serviceLabel),
    release: await buildRelease(input.hostRoot),
    revision: 1,
  });
  const statePath = join(input.hostRoot, "axis.host.v1.json");
  await writeFile(statePath, JSON.stringify(state), { mode: 0o600 });
  await chmod(statePath, 0o600);
  return state;
}

/**
 * Construct an ambiguous Node command directly through the service with a throwing
 * Worker dispatch.
 */
async function buildAmbiguousNodeCommand(path: string): Promise<{ readonly commandId: string; readonly runId: string }> {
  const journal = await SqliteNodeJournal.open(path);
  try {
    const failingWorker: NodeWorkerInventoryPort = {
      admission: () => ({ kind: "available" }),
      dispatch: async () => { throw new Error("Synthetic Worker launch failure"); },
      readCommand: async () => ({ status: "not_recorded" }),
    };
    const service = new NodeCommandService({
      authenticatedNode: nodeAuthenticatedContext,
      journal,
      resolveNewSessionReservation: () => nodeReservation,
      worker: failingWorker,
      now: () => new Date(T0),
    });
    const request = await buildNodeStartRequest();
    if (request.command.command !== "start") throw new Error("expected a start command");
    const disposition = await service.executeWithDisposition(request);
    assert.equal(disposition.receipt.status, "ambiguous", "launch_ambiguous is an ambiguous Worker failure");
    // `verifyDrain`'s Node branch reads the journal's persisted maintenance mode directly (it does not call
    // the live HTTP service); a real drain always sets this first, via `axis node drain` or `upgrade()`'s
    // first gate, before the second gate reads the stopped Node's journal.
    await journal.setMaintenanceMode("drain");
    return { commandId: request.command.commandId, runId: request.command.start.identity.run.runId };
  } finally { journal.close(); }
}

function collectIo(): { readonly io: LifecycleIo; output(): string } {
  let stdout = "";
  return { io: { stdout: (text) => { stdout += text; }, stderr: () => undefined, isTTY: false }, output: () => stdout };
}

test("acknowledging a permanently ambiguous Node command also releases its Worker's own axis upgrade", async (context) => {
  // `os.tmpdir()` on macOS is `/var/folders/...`, and `/var` is itself a symlink to `/private/var`; every
  // strict owner-private-file check below compares against `realpath`, so the root must already be canonical.
  const root = await realpath(await mkdtemp(join(tmpdir(), "axis-node-lifecycle-")));
  context.after(async () => { await rm(root, { recursive: true, force: true }); });

  const workerHostRoot = join(root, "worker-host"), nodeHostRoot = join(root, "node-host");
  const configuration = await writeConfigurationFixture(root, { nodes: [{ id: nodeFixtureTarget.identity.nodeId,
    ref: "logical-node-route-1", port: 41000, eventPort: 41001, defaultWorker: nodeReservation.workerKey,
    workers: [{ id: nodeFixtureTarget.identity.workerId, key: nodeReservation.workerKey, executorId: nodeFixtureTarget.sessionBindingExecutorId,
      endpointRef: nodeFixtureTarget.endpointRef.ref, endpointVersion: nodeFixtureTarget.endpointRef.version,
      runtimeProfile: nodeFixtureTarget.identity.runtimeProfile, kind: "codex", port: 41003, hostRoot: workerHostRoot,
      workspaces: [{ organizationId: "example-org", workspaceRoot: root }] }] }] });
  const nodeDatabase = deriveNodeConfiguration(configuration).database;
  const { commandId, runId } = await buildAmbiguousNodeCommand(nodeDatabase);
  const workerDatabase = deriveWorkerConfiguration(configuration).database;
  const workerIntentDigest = `sha256:${"1".repeat(64)}`;
  const workerJournal = new SqliteWorkerCommandJournal(workerDatabase);
  workerJournal.reserveRun({ runId, intentJson: "{}", intentDigest: workerIntentDigest, occurredAt: new Date(T0).toISOString() });
  workerJournal.recordObservational({ runId, cause: "worker.observe_start_invalid", occurredAt: new Date(T0).toISOString() });
  workerJournal.close();
  const workerState = await writeHostState({ hostRoot: workerHostRoot, configuration, role: "worker", workerRuntime: "codex", serviceLabel: "com.axis.node-lifecycle-worker" });
  const nodeState = await writeHostState({ hostRoot: nodeHostRoot, configuration, role: "node", workerRuntime: null, serviceLabel: "com.axis.axis-node" });
  // Start the adapter clock five minutes after ambiguity, within the 15-minute
  // acknowledgement floor.
  let clockMs = T0 + 5 * 60_000;
  const nodeAdapter = new MacosLifecycleAdapter({ hostRoot: nodeHostRoot, sourcePackage: root, home: root, now: () => new Date(clockMs) });
  const workerAdapter = new MacosLifecycleAdapter({ hostRoot: workerHostRoot, sourcePackage: root, home: root, now: () => new Date(clockMs) });

  // 1. Before acknowledgement: both gates are blocked.
  await assert.rejects(() => nodeAdapter.verifyDrain(nodeState, configuration), (error: unknown) => {
    assert.ok(error instanceof NodeUpgradeBlockedError);
    assert.equal(error.blockers.ambiguousCommands, 1);
    assert.equal(error.blockers.activeWorkerRuns, 1);
    return true;
  });
  await assert.rejects(() => workerAdapter.verifyDrain(workerState, configuration), (error: unknown) => {
    assert.ok(error instanceof WorkerUpgradeBlockedError);
    assert.equal(error.stuckRuns, 1);
    assert.equal(error.releasedStuckRuns, 0);
    return true;
  });

  // 2. list shows the command with its Worker-side evidence, unreleased.
  const beforeListing = await nodeAdapter.listAmbiguousCommands(nodeState, configuration);
  assert.equal(beforeListing.ambiguousCommands.length, 1);
  const beforeEntry = beforeListing.ambiguousCommands[0]!;
  assert.equal(beforeEntry.row.commandId, commandId);
  assert.equal(beforeEntry.worker.state, "observational");
  assert.equal(beforeEntry.workerRelease, "none");
  assert.equal(beforeListing.upgradeGate.blocked, true);

  // 3. Acknowledge.
  const input = { commandId, operatorRef: "operator-1", authorizationRef: "authority-1", source: "test-suite", requestRef: "request-1", reason: null };

  const tooRecent = await nodeAdapter.acknowledgeAmbiguousCommand(nodeState, configuration, input);
  assert.deepEqual(tooRecent, { outcome: "too_recent" });
  assert.deepEqual(await readNodeAmbiguousAcknowledgements(nodeHostRoot), [], "too_recent writes nothing to the Node host root");
  assert.deepEqual(await readWorkerStuckRunAcknowledgements(workerHostRoot), [], "too_recent writes nothing to the Worker host root");

  clockMs = T0 + FIFTEEN_MINUTES_MS + 60_000; // Advance past the acknowledgement floor.

  const unknownId = await nodeAdapter.acknowledgeAmbiguousCommand(nodeState, configuration, { ...input, commandId: "unknown-command-id" });
  assert.deepEqual(unknownId, { outcome: "not_ambiguous" });

  const acknowledged = await nodeAdapter.acknowledgeAmbiguousCommand(nodeState, configuration, input);
  if (acknowledged.outcome !== "acknowledged") throw new Error(`expected acknowledged, got ${acknowledged.outcome}`);
  assert.equal(acknowledged.node, "applied_and_verified");
  assert.equal(acknowledged.worker, "recorded");
  const workerAcksAfterFirst = await readWorkerStuckRunAcknowledgements(workerHostRoot);
  assert.equal(workerAcksAfterFirst.length, 1);
  assert.equal(workerAcksAfterFirst[0]?.runId, runId);
  assert.equal(workerAcksAfterFirst[0]?.workerIntentDigest, workerIntentDigest);
  assert.equal(workerAcksAfterFirst[0]?.acknowledgedState, "observational");
  assert.equal(workerAcksAfterFirst[0]?.origin.kind, "node_ambiguous_command");
  assert.equal(workerAcksAfterFirst[0]?.origin.nodeCommandId, commandId);

  const again = await nodeAdapter.acknowledgeAmbiguousCommand(nodeState, configuration, { ...input, operatorRef: "operator-2" });
  if (again.outcome !== "acknowledged") throw new Error(`expected acknowledged, got ${again.outcome}`);
  assert.equal(again.node, "already_applied_and_verified");
  assert.equal(again.worker, "already_recorded");
  assert.equal(again.acknowledgement.operatorRef, "operator-1", "the second call keeps the first call's operatorRef");
  assert.equal((await readWorkerStuckRunAcknowledgements(workerHostRoot)).length, 1, "the second call did not append a duplicate Worker record");

  // Acknowledgement does not redispatch or converge a command. This row remains
  // ambiguous, so it stays in ambiguousCommands and resolvedAfterAcknowledgement is
  // empty. Convergence to accepted requires a validated Worker response and is outside
  // this acknowledgement scenario.
  const stillAmbiguousListing = await nodeAdapter.listAmbiguousCommands(nodeState, configuration);
  assert.deepEqual(stillAmbiguousListing.resolvedAfterAcknowledgement, [], "an acknowledged command that is still ambiguous is not yet \"resolved\"");
  assert.equal(stillAmbiguousListing.ambiguousCommands.length, 1);
  assert.equal(stillAmbiguousListing.ambiguousCommands[0]?.row.commandId, commandId);

  // 4. After acknowledgement: both gates resolve.
  await nodeAdapter.verifyDrain(nodeState, configuration);
  await workerAdapter.verifyDrain(workerState, configuration);

  // 5. Evidence kept: the rows are unchanged, not re-dispatched, not deleted.
  const { ambiguous: rowsAfter } = await SqliteNodeJournal.inspectAmbiguousCommands(nodeDatabase);
  assert.equal(rowsAfter.length, 1);
  assert.equal(rowsAfter[0]?.commandId, commandId);
  const workerJournalAfter = new SqliteWorkerCommandJournal(workerDatabase, { readOnly: true });
  try { assert.equal(workerJournalAfter.readRun(runId)?.state, "observational"); }
  finally { workerJournalAfter.close(); }

  // A missing --authority is unauthorized -- a CLI-argument concern, checked before `acquireLock`/`readState`
  // even touch the (fake, for this fixture) managed lifecycle state, so it exercises the real CLI routing
  // without needing a matching plist on disk.
  const unauthorized = collectIo();
  const unauthorizedCode = await runLifecycleCli(["node", "ambiguous-commands", "acknowledge", "--host-root", nodeHostRoot, "--command", commandId, "--operator", "operator-3", "--audit-source", "test", "--request", "request-2", "--output", "json"], unauthorized.io);
  assert.equal(unauthorizedCode, 1);
  assert.equal((JSON.parse(unauthorized.output()) as { readonly error: { readonly code: string } }).error.code, "unauthorized");
});
