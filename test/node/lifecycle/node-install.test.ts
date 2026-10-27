import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod, lstat, mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { promisify } from "node:util";

import { nodeUpgradeBlockedDocument, nodeUpgradeBlockedText, runLifecycleCli, workerUpgradeBlockedDocument, workerUpgradeBlockedText } from "../../../src/lifecycle/cli.js";
import { SqliteWorkerCommandJournal } from "../../../src/worker/command.js";
import { HostStateError, validateHostState } from "../../../src/lifecycle/host-state.js";
import type { HostState } from "../../../src/lifecycle/host-state.js";
import { lifecycleDatabasePaths, lifecycleVerificationConfig, MacosLifecycleAdapter } from "../../../src/lifecycle/macos-adapter.js";
import { NodeUpgradeBlockedError, WorkerUpgradeBlockedError } from "../../../src/lifecycle/stuck-run-acknowledgements.js";
import { install, makeState, serviceLabelFor, upgrade } from "../../../src/lifecycle/transaction.js";
import type { LifecycleAdapter, StagedRelease } from "../../../src/lifecycle/transaction.js";
// Fake maintenance status goes through the real validator, as `MacosLifecycleAdapter` does.
import { nodeMaintenanceStatus, type NodeBlockerCounts } from "../../../src/shared/router-node.js";

const run = promisify(execFile);
const digest = `sha256:${"a".repeat(64)}`;
const baseRelease: StagedRelease = { packagePath: "/host/releases/a/node_modules/@axisosme/axis", version: "0.13.1", manifestDigest: digest };
const FAILING_NODE_BLOCKERS: NodeBlockerCounts = { reservedCommands: 0, dispatchingCommands: 0, ambiguousCommands: 1, pendingNodeEvents: 0, quarantinedNodeEvents: 0, activeWorkerRuns: 0, pendingWorkerOutbox: 0, unknownWorkerEvidence: 0 };

import { managedConfigurationReference, loadHostConfiguration } from "../../../src/lifecycle/configuration.js";
import { deriveNodeConfiguration, deriveWorkerConfiguration, secretNames, secretPath, type AxisConfiguration } from "../../../src/configuration.js";
import { configurationFixture, writeConfigurationFixture } from "../../support/configuration-fixture.js";

interface Fleet { readonly states: Map<string, HostState>; readonly plists: Map<string, HostState>; }
function fleet(): Fleet { return { states: new Map(), plists: new Map() }; }
function reference(filename = "/secure/axis.toml", workerId: string | null = null) { return { filename, digest, nodeId: "node-one", workerId }; }
function plist(label: string): string { return `/opt/example/LaunchAgents/${label}.plist`; }
function nodeRequest() { return { role: "node" as const, workerRuntime: null, configuration: reference() }; }
function nodeState(): HostState { return makeState({ request: nodeRequest(), managedPlist: plist("com.axis.axis-node"), release: baseRelease, revision: 1 }); }
function fakeAdapter(shared: Fleet, root: string, configuration: AxisConfiguration = configurationFixture(), options: { failVerify?: boolean; failDeactivate?: boolean; failConfiguration?: boolean; failNodeUpgradeGate?: boolean } = {}) {
  const calls: string[] = [];
  const adapter: LifecycleAdapter = {
    acquireLock: async () => { calls.push("lock"); return async () => { calls.push("unlock"); }; },
    readState: async () => { calls.push("read"); return shared.states.get(root) ?? null; },
    managedPlistExists: async path => { calls.push("exists"); return shared.plists.has(path); },
    managedPlistMatches: async state => { calls.push("matches"); return shared.plists.get(state.managedPlist)?.installIdentity === state.installIdentity; },
    stage: async () => { calls.push("stage"); return { ...baseRelease, packagePath: `${root}/releases/a/node_modules/@axisosme/axis` }; },
    loadConfiguration: async () => { calls.push("configuration"); if (options.failConfiguration) throw new HostStateError("unsafe configuration"); return configuration; },
    verifyDrain: async () => { calls.push("drain"); },
    assertNodeUpgradeGate: async () => { calls.push("assert-node-gate"); if (options.failNodeUpgradeGate) throw new NodeUpgradeBlockedError("blocked", { blockers: FAILING_NODE_BLOCKERS, acknowledged: { ambiguousCommands: 0, activeWorkerRuns: 0 }, unacknowledgedAmbiguous: ["command-one"] }); },
    nodeMaintenance: async (_state, _config, operation) => { calls.push(`node-${operation}`); return nodeMaintenanceStatus({ contractVersion: "1.0.0", nodeRef: { ref: "node-one", version: "v1" }, mode: operation === "status" ? "resume" : operation, idle: true, blockerCounts: { ...FAILING_NODE_BLOCKERS, ambiguousCommands: 0 } }); },
    activate: async state => { calls.push("activate"); shared.states.set(root, state); shared.plists.set(state.managedPlist, state); },
    deactivate: async () => { calls.push("deactivate"); if (options.failDeactivate) throw new HostStateError("stop failed"); },
    verify: async () => { calls.push("verify"); if (options.failVerify) throw new HostStateError("readiness failed"); },
    backup: async () => { calls.push("backup"); },
    restore: async () => { calls.push("restore"); return shared.states.get(root)!; },
    cleanupNew: async () => { calls.push("cleanup"); const state = shared.states.get(root); shared.states.delete(root); if (state) shared.plists.delete(state.managedPlist); },
    planForcedArchive: async () => { throw new Error("unexpected force"); }, stopService: async () => { throw new Error("unexpected force"); }, archiveForcedStorage: async () => { throw new Error("unexpected force"); },
  };
  return { adapter, calls };
}

test("Node and two Workers install with independent state, plist, configuration, and database identities", async () => {
  const shared = fleet();
  for (const [id, workerRuntime] of [["codex", "codex"], ["claude", "claude"]] as const) {
    const request = { role: "worker" as const, workerRuntime, configuration: reference(`/secure/${id}.toml`, id) };
    await install(fakeAdapter(shared, `/hosts/${id}`).adapter, request, plist(serviceLabelFor(request)));
  }
  await install(fakeAdapter(shared, "/hosts/node").adapter, nodeRequest(), plist("com.axis.axis-node"));
  assert.equal(shared.states.size, 3); assert.equal(shared.plists.size, 3);
  assert.deepEqual([...shared.states.values()].map(state => state.configuration.workerId), ["codex", "claude", null]);
});
test("Node defaults and host identity bind the configuration digest and local selectors", () => {
  const state = nodeState();
  assert.equal(serviceLabelFor(nodeRequest()), "com.axis.axis-node");
  assert.equal(validateHostState(state).schemaVersion, "axis.host.v1");
  for (const configuration of [{ ...state.configuration, digest: `sha256:${"b".repeat(64)}` }, { ...state.configuration, nodeId: "other" }]) assert.throws(() => validateHostState({ ...state, configuration }), HostStateError);
  assert.throws(() => validateHostState({ ...state, schemaVersion: "fictional.host.schema" }), HostStateError);
});
test("invalid Node runtime fails before lock or host inspection", async () => {
  const subject = fakeAdapter(fleet(), "/hosts/node");
  await assert.rejects(() => install(subject.adapter, { ...nodeRequest(), workerRuntime: "codex" }, plist("com.axis.axis-node")), HostStateError);
  assert.deepEqual(subject.calls, []);
});
test("root and service-label collisions preserve the prior installation", async () => {
  const shared = fleet(), first = fakeAdapter(shared, "/hosts/worker");
  const request = { role: "worker" as const, workerRuntime: "codex" as const, configuration: reference("/secure/worker.toml", "worker-one"), serviceLabel: "com.axis.shared" };
  await install(first.adapter, request, plist("com.axis.shared"));
  const prior = shared.states.get("/hosts/worker");
  await assert.rejects(() => install(first.adapter, nodeRequest(), plist("com.axis.axis-node")), HostStateError);
  await assert.rejects(() => install(fakeAdapter(shared, "/hosts/node").adapter, { ...nodeRequest(), serviceLabel: "com.axis.shared" }, plist("com.axis.shared")), HostStateError);
  assert.equal(shared.states.get("/hosts/worker"), prior); assert.equal(shared.states.size, 1);
});
test("unsafe configuration and failed Node readiness leave no candidate state", async () => {
  for (const options of [{ failConfiguration: true }, { failVerify: true }]) {
    const shared = fleet(), subject = fakeAdapter(shared, "/hosts/node", configurationFixture(), options);
    await assert.rejects(() => install(subject.adapter, nodeRequest(), plist("com.axis.axis-node")), HostStateError);
    assert.equal(shared.states.size, 0);
  }
  const shared = fleet(), retained = fakeAdapter(shared, "/hosts/node", configurationFixture(), { failVerify: true, failDeactivate: true });
  await assert.rejects(() => install(retained.adapter, nodeRequest(), plist("com.axis.axis-node")), HostStateError);
  assert.equal(shared.states.size, 1);
});
test("managed configuration refuses drift and unsafe files without writing host state", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-config-permissions-")); context.after(() => rm(root, { recursive: true, force: true }));
  const config = await writeConfigurationFixture(root), ref = managedConfigurationReference(config.filename, "node");
  assert.equal(loadHostConfiguration(ref).filename, config.filename);
  await writeFile(config.filename, "schemaVersion = 1\n", { mode: 0o600 });
  assert.throws(() => loadHostConfiguration(ref), /configuration/u);
  await chmod(config.filename, 0o644);
  assert.throws(() => managedConfigurationReference(config.filename, "node"), HostStateError);
});
test("invalid canonical host state is refused and preserved", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-host-invalid-")); context.after(() => rm(root, { recursive: true, force: true }));
  const path = join(root, "axis.host.v1.json"); await writeFile(path, "retained", { mode: 0o600 });
  const adapter = new MacosLifecycleAdapter({ hostRoot: root, sourcePackage: root, home: root });
  await assert.rejects(() => adapter.readState(), HostStateError);
  assert.equal(await readFile(path, "utf8"), "retained");
});

test("host initialization inspects only the canonical state file", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-host-state-")); context.after(() => rm(root, { recursive: true, force: true }));
  const path = join(root, "host-notes.json"); await writeFile(path, "retained", { mode: 0o600 });
  const adapter = new MacosLifecycleAdapter({ hostRoot: root, sourcePackage: root, home: root });
  assert.equal(await adapter.readState(), null);
  await writeFile(join(root, "axis.host.v1.json"), JSON.stringify(nodeState()), { mode: 0o600 });
  assert.deepEqual(await adapter.readState(), nodeState());
  assert.equal(await readFile(path, "utf8"), "retained");
});

test("each lifecycle role backs up and restores with only its own generated keys provisioned", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-role-backup-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const roleKeys = {
    controller: ["relay-master", "relay-operator", "node-node-one-command", "node-node-one-router-events"],
    node: ["node-node-one-command", "node-node-one-router-events", "worker-worker-one-command", "worker-worker-one-events"],
    worker: ["worker-worker-one-command", "worker-worker-one-events"],
  } as const;
  for (const role of ["controller", "node", "worker"] as const) {
    const home = join(root, role), hostRoot = join(home, "host");
    const config = await writeConfigurationFixture(home, { linear: { clientId: "example-client", redirectUri: "https://relay.example.invalid/oauth" } });
    const needed: readonly string[] = roleKeys[role];
    for (const name of secretNames(config)) if (!needed.includes(name)) await rm(secretPath(config, name));
    await mkdir(hostRoot);
    const adapter = new MacosLifecycleAdapter({ hostRoot, sourcePackage: home, home });
    const state = makeState({ request: { role, workerRuntime: role === "worker" ? "codex" : null, configuration: managedConfigurationReference(config.filename, role) }, managedPlist: adapter.managedPlistPathFor(`com.example.${role}`), release: baseRelease, revision: 1 });
    await writeFile(join(hostRoot, "axis.host.v1.json"), JSON.stringify(state), { mode: 0o600 });
    await mkdir(join(home, "Library", "LaunchAgents"), { recursive: true });
    await writeFile(state.managedPlist, "<plist/>", { mode: 0o600 });
    const originals = await Promise.all(needed.map(name => readFile(secretPath(config, name), "utf8")));
    await adapter.backup(state);
    for (const name of needed) await writeFile(secretPath(config, name), "changed");
    assert.deepEqual(await adapter.restore(), state);
    assert.deepEqual(await Promise.all(needed.map(name => readFile(secretPath(config, name), "utf8"))), originals);
  }
});

test("install and upgrade stage an extracted distribution without its source workspace", async (context) => {
  const root = await mkdtemp(join(tmpdir(), "axis-lifecycle-package-staging-"));
  const originalPath = process.env.PATH;
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  context.after(async () => {
    Object.defineProperty(process, "platform", platform);
    if (originalPath === undefined) delete process.env.PATH; else process.env.PATH = originalPath;
    await rm(root, { recursive: true, force: true });
  });
  const source = join(root, "package"), hostRoot = join(root, "host"), bin = join(root, "bin"), callsFile = join(root, "npm-calls.jsonl");
  await mkdir(join(source, "dist", "bin"), { recursive: true }); await mkdir(bin);
  const writeSource = async (version: string) => {
    await writeFile(join(source, "package.json"), JSON.stringify({
      name: "@axisosme/axis", version, engines: { node: ">=24" }, files: ["dist"],
      bin: { "axis-node": "dist/bin/node.js" }, workspaces: ["packages/contracts"],
      scripts: { prepare: "npm run build", prepack: "npm run build", build: "npm run build --workspace @axisosme/axis-contracts" },
    }));
    await writeFile(join(source, "dist", "bin", "node.js"), `process.stdout.write(${JSON.stringify(version)});\n`);
  };
  await writeSource("0.1.0");
  await assert.rejects(() => readFile(join(source, "packages", "contracts", "package.json")), { code: "ENOENT" });
  // Run the real packer. Simulate installation by extracting its archive; no npm install or host service runs.
  await writeFile(join(bin, "npm"), `#!${process.execPath}
const { appendFileSync, mkdirSync, statSync } = require("node:fs");
const { execFileSync, spawnSync } = require("node:child_process");
const { join } = require("node:path");
const args = process.argv.slice(2);
appendFileSync(${JSON.stringify(callsFile)}, JSON.stringify(args) + "\\n");
if (args[0] === "pack") {
  const result = spawnSync("npm", args, { encoding: "utf8", env: { ...process.env, PATH: ${JSON.stringify(originalPath)}, npm_config_cache: ${JSON.stringify(join(root, "npm-cache"))} } });
  process.stdout.write(result.stdout ?? ""); process.stderr.write(result.stderr ?? ""); process.exit(result.status ?? 1);
}
if (args[0] !== "install") throw new Error("unexpected npm command");
const archive = args.at(-1);
if (!archive.endsWith(".tgz") || !statSync(archive).isFile()) throw new Error("directory installation would run workspace preparation");
const destination = join(args[args.indexOf("--prefix") + 1], "node_modules", "@axisosme", "axis");
mkdirSync(destination, { recursive: true });
execFileSync("tar", ["-xzf", archive, "--strip-components=1", "-C", destination]);
`, { mode: 0o700 });
  process.env.PATH = `${bin}:${originalPath ?? ""}`;
  Object.defineProperty(process, "platform", { ...platform, value: "darwin" });
  const real = new MacosLifecycleAdapter({ hostRoot, sourcePackage: source, home: root });
  const shared = fleet(); const subject = fakeAdapter(shared, hostRoot);
  const adapter: LifecycleAdapter = { ...subject.adapter, stage: (role, runtime) => real.stage(role, runtime) };
  await install(adapter, nodeRequest(), plist("com.axis.axis-node"));
  const first = shared.states.get(hostRoot)!;
  assert.equal(first.activeRelease.version, "0.1.0");
  assert.equal((await lstat(first.activeRelease.packagePath)).isSymbolicLink(), false);
  await writeSource("0.1.1");
  assert.equal((await upgrade(adapter)).status, "upgraded");
  const second = shared.states.get(hostRoot)!;
  assert.equal(second.activeRelease.version, "0.1.1");
  assert.notEqual(second.activeRelease.manifestDigest, first.activeRelease.manifestDigest);
  assert.equal(JSON.parse(await readFile(join(first.activeRelease.packagePath, "package.json"), "utf8")).version, "0.1.0");
  await assert.rejects(() => readFile(join(hostRoot, "axis.host.v1.json")), { code: "ENOENT" });
  assert.deepEqual(await readdir(hostRoot), ["releases"]);
  const commands = (await readFile(callsFile, "utf8")).trim().split("\n").map(line => JSON.parse(line) as string[]);
  assert.deepEqual(commands.map(args => args[0]), ["pack", "install", "pack", "install"]);
  assert.ok(commands.filter(args => args[0] === "pack").every(args => args.includes("--ignore-scripts")));
});

test("Node upgrade uses maintenance and a blocked gate preserves the running service", async () => {
  const shared = fleet(), subject = fakeAdapter(shared, "/hosts/node");
  await install(subject.adapter, nodeRequest(), plist("com.axis.axis-node")); subject.calls.length = 0;
  assert.equal((await upgrade(subject.adapter)).status, "upgraded");
  assert.deepEqual(subject.calls, ["lock", "read", "matches", "configuration", "node-drain", "assert-node-gate", "stage", "verify", "node-resume", "unlock"]);
  const blocked = fakeAdapter(shared, "/hosts/node", configurationFixture(), { failNodeUpgradeGate: true });
  await assert.rejects(() => upgrade(blocked.adapter), NodeUpgradeBlockedError);
  assert.equal(blocked.calls.includes("deactivate"), false);
  const error = new NodeUpgradeBlockedError("blocked", { blockers: FAILING_NODE_BLOCKERS, acknowledged: { ambiguousCommands: 0, activeWorkerRuns: 0 }, unacknowledgedAmbiguous: [] });
  assert.match(nodeUpgradeBlockedText(error), /axis node ambiguous-commands acknowledge/u);
  assert.match(nodeUpgradeBlockedText(error), /axis node resume/u);
  assert.equal((nodeUpgradeBlockedDocument(error).error as { code: string }).code, "node_not_drained");
  const worker = new WorkerUpgradeBlockedError({ stuckRuns: 1, releasedStuckRuns: 0, otherBlockingTables: [] });
  assert.match(workerUpgradeBlockedText(worker), /axis node ambiguous-commands/u);
  assert.equal((workerUpgradeBlockedDocument(worker).error as { code: string }).code, "worker_not_drained");
});
test("a blocked Worker-role verifyDrain rejects with WorkerUpgradeBlockedError, and its JSON names worker_not_drained", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-worker-drain-"));
  context.after(() => rm(root, { recursive: true, force: true }));
  const config = await writeConfigurationFixture(root), worker = deriveWorkerConfiguration(config);
  new SqliteWorkerCommandJournal(worker.database).close();
  const database = new DatabaseSync(worker.database);
  database.prepare("INSERT INTO worker_runs VALUES ('run-one','{}','sha256:aa','reserved',NULL,NULL,NULL,'2026-01-01T00:00:00.000Z','2026-01-01T00:00:00.000Z')").run();
  database.close();
  const state = makeState({ request: { role: "worker", workerRuntime: "codex", configuration: managedConfigurationReference(config.filename, "worker") }, managedPlist: plist("com.axis.axis-codex-worker"), release: baseRelease, revision: 1 });
  const adapter = new MacosLifecycleAdapter({ hostRoot: join(root, "host"), sourcePackage: root, home: root });
  await assert.rejects(() => adapter.verifyDrain(state, config), WorkerUpgradeBlockedError);
  const error = new WorkerUpgradeBlockedError({ stuckRuns: 1, releasedStuckRuns: 0, otherBlockingTables: [] });
  assert.match(workerUpgradeBlockedText(error), /axis node ambiguous-commands/u);
  assert.equal((workerUpgradeBlockedDocument(error).error as { code: string }).code, "worker_not_drained");
});
test("lifecycle probes use the same Node and Worker keys, identities and stores as startup", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-host-probes-")); context.after(() => rm(root, { recursive: true, force: true }));
  const config = await writeConfigurationFixture(root);
  const state = makeState({ request: { ...nodeRequest(), configuration: managedConfigurationReference(config.filename, "node") }, managedPlist: plist("com.axis.axis-node"), release: baseRelease, revision: 1 });
  const node = deriveNodeConfiguration(config);
  assert.deepEqual(lifecycleDatabasePaths(state, config), [node.database]);
  assert.deepEqual(lifecycleVerificationConfig(state, config), {
    healthUrl: `http://127.0.0.1:${node.port}/v1/node/readiness`, healthToken: node.token,
    authUrl: `http://127.0.0.1:${node.port}/v1/node/readiness`, authMethod: "GET", nodeIdentity: { ref: node.nodeRef, version: node.nodeVersion },
  });
  const workerState = makeState({ request: { role: "worker", workerRuntime: "codex", configuration: managedConfigurationReference(config.filename, "worker") }, managedPlist: plist("com.axis.axis-codex-worker"), release: baseRelease, revision: 1 });
  const worker = deriveWorkerConfiguration(config);
  assert.deepEqual(lifecycleDatabasePaths(workerState, config), [worker.database]);
  const verification = lifecycleVerificationConfig(workerState, config);
  assert.deepEqual(verification, { healthUrl: `http://127.0.0.1:${worker.port}/v1/worker/readiness`, healthToken: worker.token, authUrl: `http://127.0.0.1:${worker.port}/v1/worker/commands` });
  const adapter = new MacosLifecycleAdapter({ hostRoot: join(root, "host"), sourcePackage: root, home: root });
  const original = globalThis.fetch; let reads = 0, boundary = 0;
  globalThis.fetch = async input => { if (String(input).endsWith("readiness")) return new Response(null, { status: ++reads < 8 ? 503 : 200 }); boundary++; return new Response(null, { status: 401 }); };
  context.after(() => { globalThis.fetch = original; });
  await adapter.verify(workerState); assert.equal(reads, 8); assert.equal(boundary, 1);
});
test("Node CLI rejects unsupported install flags without leaking details", async () => {
  let text = ""; const code = await runLifecycleCli(["install", "--role", "node", "--worker-runtime", "codex", "--output", "json"], { stdout: value => { text += value; }, stderr: () => {}, isTTY: false });
  assert.equal(code, 1); assert.equal(JSON.parse(text).error.code, "invalid_or_unavailable");
});
test("the package-local lifecycle runner forwards pinned TOML selectors and external credentials", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-host-runner-")); context.after(() => rm(root, { recursive: true, force: true }));
  const config = await writeConfigurationFixture(root), packagePath = join(root, "package"); await mkdir(packagePath);
  await writeFile(join(packagePath, "package.json"), JSON.stringify({ bin: { "axis-node": "node.mjs" } }));
  await writeFile(join(packagePath, "node.mjs"), 'process.stdout.write(JSON.stringify({args:process.argv.slice(2),secret:process.env.LINEAR_CLIENT_SECRET}));');
  const state = makeState({ request: { ...nodeRequest(), configuration: managedConfigurationReference(config.filename, "node") }, managedPlist: plist("com.axis.axis-node"), release: { ...baseRelease, packagePath }, revision: 1 });
  const stateFile = join(root, "state.json"); await writeFile(stateFile, JSON.stringify(state), { mode: 0o600 });
  const result = await run(process.execPath, ["--import=tsx", "src/lifecycle/runner.ts", "--state", stateFile], { cwd: process.cwd(), timeout: 30_000, env: { ...process.env, LINEAR_CLIENT_SECRET: "fictional-secret" } });
  assert.deepEqual(JSON.parse(result.stdout), { args: ["--config", config.filename, "--node", "node-one"], secret: "fictional-secret" });
});
