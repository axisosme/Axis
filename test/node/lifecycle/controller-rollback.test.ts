import { databasePaths, type AxisConfiguration } from "../../../src/configuration.js";
import { managedConfigurationReference } from "../../../src/lifecycle/configuration.js";
import { writeConfigurationFixture, configurationFixture } from "../../support/configuration-fixture.js";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { HostStateError } from "../../../src/lifecycle/host-state.js";
import type { HostState } from "../../../src/lifecycle/host-state.js";
import { lifecycleDatabasePaths, MacosLifecycleAdapter } from "../../../src/lifecycle/macos-adapter.js";
import { makeState, serviceLabelFor, upgrade } from "../../../src/lifecycle/transaction.js";
import type { LifecycleAdapter, StagedRelease } from "../../../src/lifecycle/transaction.js";
import { SqliteChannelConversationStore } from "../../../src/router/channel-conversation.js";
import { SqliteChannelRunChainStore } from "../../../src/router/channel-run-chain.js";

const controllerRequest = (filename: string) => ({ role: "controller" as const, workerRuntime: null, configuration: managedConfigurationReference(filename, "controller") });
const release = (hostRoot: string, name: string, version: string): StagedRelease => ({ packagePath: `${hostRoot}/releases/${name}/node_modules/@axisosme/axis`, version, manifestDigest: `sha256:${name.repeat(64)}` });

function controllerDatabases(config: AxisConfiguration) {
  const { vault: _vault, ...databases } = databasePaths(config); return databases;
}
function openLinearStores(paths: ReturnType<typeof controllerDatabases>): void {
  const conversations = new SqliteChannelConversationStore({ filename: paths.conversations });
  try { new SqliteChannelRunChainStore({ conversations, filename: paths.runChain }).close(); } finally { conversations.close(); }
}
function openConversations(paths: ReturnType<typeof controllerDatabases>): void { new SqliteChannelConversationStore({ filename: paths.conversations }).close(); }

/** Injects an unsupported schema to exercise snapshot restoration. */
function recordUnsupportedSchema(path: string, ledger: string): void {
  const database = new DatabaseSync(path);
  try { database.prepare(`UPDATE ${ledger} SET version = ?`).run(987654321); } finally { database.close(); }
}
function writeMarker(path: string, value: string): void {
  const database = new DatabaseSync(path);
  try { database.exec("CREATE TABLE IF NOT EXISTS marker(value TEXT NOT NULL)"); database.exec("DELETE FROM marker"); database.prepare("INSERT INTO marker(value) VALUES (?)").run(value); } finally { database.close(); }
}
const digestOf = async (path: string): Promise<string> => createHash("sha256").update(await readFile(path)).digest("hex");
const unexpected = (name: string) => async (): Promise<never> => { throw new Error(`unexpected ${name}`); };

test("a failed controller upgrade restores Linear databases for the restored release to reopen", async (context) => {
  const root = await mkdtemp(join(tmpdir(), "axis-controller-rollback-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const hostRoot = join(root, "host"), data = join(root, "data");
  await mkdir(hostRoot, { recursive: true, mode: 0o700 }); await mkdir(data, { recursive: true, mode: 0o700 });
  const config = await writeConfigurationFixture(root, { linear: { clientId: "client-one", redirectUri: "https://relay.example.invalid/oauth" } });
  const environment = controllerDatabases(config), environmentFile = config.filename;

  const real = new MacosLifecycleAdapter({ hostRoot, sourcePackage: root, home: root });
  const state: HostState = makeState({ request: controllerRequest(environmentFile), managedPlist: real.managedPlistPathFor(serviceLabelFor(controllerRequest(environmentFile))), release: release(hostRoot, "a", "0.1.0"), revision: 1 });
  const statePath = join(hostRoot, "axis.host.v1.json");
  await writeFile(statePath, `${JSON.stringify(state)}\n`, { mode: 0o600 });
  await mkdir(join(root, "Library", "LaunchAgents"), { recursive: true }); await writeFile(state.managedPlist, "<plist/>\n", { mode: 0o600 });

  // Snapshot the installed release's databases: both Linear stores use the current
  // schema, and every other controller database contains a marker the candidate
  // overwrites.
  openLinearStores(environment);
  const markerKeys = (["router", "channel", "delivery"] as const);
  for (const key of markerKeys) writeMarker(environment[key]!, "old");
  const databases = Object.keys(environment) as (keyof typeof environment)[];
  assert.equal(databases.length, 5, "every controller database is in play");
  const before = new Map(await Promise.all(databases.map(async (key) => [key, await digestOf(environment[key]!)] as const)));

  const calls: string[] = [];
  const candidateRelease = release(hostRoot, "b", "1.0.0-candidate");
  const adapter: LifecycleAdapter = {
    acquireLock: () => real.acquireLock(),
    readState: () => real.readState(),
    managedPlistExists: unexpected("managedPlistExists"),
    managedPlistMatches: async () => true,
    stage: async () => candidateRelease,
    verifyDrain: async () => { calls.push("drain"); },
    nodeMaintenance: unexpected("nodeMaintenance"),
    assertNodeUpgradeGate: unexpected("assertNodeUpgradeGate"),
    loadConfiguration: (reference) => real.loadConfiguration(reference),
    activate: async (next) => {
      calls.push(`activate ${next.activeRelease.version}`);
      await writeFile(statePath, `${JSON.stringify(next)}\n`, { mode: 0o600 });
      if (next.activeRelease.version !== candidateRelease.version) return;
      // Inject schema and data faults before the candidate fails its readiness check.
      recordUnsupportedSchema(environment.runChain, "channel_run_chain_schema");
      recordUnsupportedSchema(environment.conversations, "channel_conversation_schema");
      for (const key of markerKeys) writeMarker(environment[key]!, "new");
    },
    deactivate: async () => { calls.push("deactivate"); },
    verify: async (next) => {
      calls.push(`verify ${next.activeRelease.version}`);
      if (next.activeRelease.version !== candidateRelease.version) return;
      // Confirm that the fault prevents opening the current schema.
      assert.throws(() => openConversations(environment), (error: Error & { code?: string }) => error.code === "schema_version_unsupported");
      throw new HostStateError("service health verification failed");
    },
    backup: (backed) => real.backup(backed),
    restore: () => real.restore(),
    cleanupNew: unexpected("cleanupNew"),
    planForcedArchive: unexpected("planForcedArchive"),
    stopService: unexpected("stopService"),
    archiveForcedStorage: unexpected("archiveForcedStorage"),
  };

  const receipt = await upgrade(adapter);
  assert.equal(receipt.status, "rolled_back");
  assert.equal(receipt.version, "0.1.0");
  assert.deepEqual(calls, ["drain", "deactivate", "drain", "activate 1.0.0-candidate", "verify 1.0.0-candidate", "deactivate", "activate 0.1.0", "verify 0.1.0"]);
  // The restored release can open both Linear databases, and every controller database
  // matches its snapshot byte for byte.
  openLinearStores(environment);
  for (const key of databases) assert.equal(await digestOf(environment[key]!), before.get(key), `${key} is restored`);
});

test("controller database paths are the channel and Router files derived from dataDir", () => {
  const config = configurationFixture({ linear: { clientId: "example-client", redirectUri: "https://relay.example.invalid/oauth" } });
  const state = makeState({ request: { role: "controller", workerRuntime: null, configuration: { filename: config.filename, digest: `sha256:${"a".repeat(64)}`, nodeId: null, workerId: null } }, managedPlist: "/opt/example/relay.plist", release: release("/host", "a", "0.1.0"), revision: 1 });
  assert.deepEqual(lifecycleDatabasePaths(state, config), Object.values(controllerDatabases(config)));
});
