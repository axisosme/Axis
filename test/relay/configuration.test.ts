import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { chmodSync, existsSync, lstatSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test, { type TestContext } from "node:test";
import { parse, stringify } from "smol-toml";

import {
  AxisConfigurationError, databasePaths, deriveNodeConfiguration, deriveRelayConfiguration, deriveWorkerConfiguration,
  generateAxisSecrets, initializeAxis, loadAxisConfiguration, parseAxisConfiguration, readAxisSecret, readLinearSecrets,
  secretNames, secretPath, selectedNode, selectedWorker,
} from "../../src/configuration.js";
import { selectNodeInventoryWorker } from "../../src/node/runtime-config.js";
import { HttpNodeWorkerClient } from "../../src/node/worker-client.js";
import { isExecutorLabel } from "../../src/shared/worker-selection.js";
import { validateNodeTarget } from "../../src/shared/router-node.js";
import { validateNodeAuthenticatedContext } from "../../src/shared/router-node-commands.js";
import { loadRoleConfiguration, parseRoleOptions } from "../../src/configuration-cli.js";

const example = readFileSync(new URL("../../axis.example.toml", import.meta.url), "utf8");
const filename = "/example/config/axis.toml";
function fixture(context: TestContext): { readonly root: string; readonly filename: string } {
  const root = mkdtempSync(join(tmpdir(), "axis-configuration-"));
  context.after(() => rmSync(root, { recursive: true, force: true }));
  return { root, filename: join(root, "axis.toml") };
}
function modified(edit: (value: Record<string, unknown>) => void): string {
  const value = parse(example); edit(value); return stringify(value);
}
function nodes(value: Record<string, unknown>): Record<string, unknown>[] { return value.nodes as Record<string, unknown>[]; }
function workers(value: Record<string, unknown>): Record<string, unknown>[] { return nodes(value)[0]!.workers as Record<string, unknown>[]; }
function fakeSecret(name: string): string { return Buffer.from(name.padEnd(32, "x").slice(0, 32)).toString("base64"); }

test("service arguments select a TOML file and local identities, refusing unknown or repeated flags", context => {
  const local = fixture(context);
  writeFileSync(local.filename, example);
  assert.equal(parseRoleOptions([], "relay").filename, resolve("axis.toml"));
  const options = parseRoleOptions(["--config", local.filename, "--node", "node-one", "--worker", "worker-one"], "worker");
  const config = loadRoleConfiguration(options, "worker");
  assert.equal(selectedWorker(config, options.workerId, options.nodeId).worker.id, "worker-one");
  assert.throws(() => loadRoleConfiguration({ ...options, workerId: "unknown" }, "worker"), AxisConfigurationError);
  assert.throws(() => loadRoleConfiguration({ ...options, nodeId: "unknown" }, "node"), AxisConfigurationError);
  for (const args of [["--config"], ["--config", ""], ["--config", "a", "--config", "b"], ["--environment-file", "a"], ["--worker-runtime", "codex"]]) {
    assert.throws(() => parseRoleOptions(args, "worker"), AxisConfigurationError);
  }
  assert.throws(() => parseRoleOptions(["--node", "node-one"], "relay"), AxisConfigurationError);
  assert.throws(() => parseRoleOptions(["--worker", "worker-one"], "node"), AxisConfigurationError);
});

test("the complete TOML example resolves relative paths from its file and round-trips", () => {
  const config = parseAxisConfiguration(example, filename);
  assert.equal(config.dataDir, "/example/config/data");
  assert.equal(config.nodes[0]!.workers[0]!.workspaces[0]!.workspaceRoot, "/example/config/workspaces/example");
  assert.equal(config.nodes[0]!.workers[0]!.workspaces[0]!.workspaceCatalog![0]!.workspaceRoot, "/example/config/workspaces/example/project");
  assert.deepEqual(parseAxisConfiguration(stringify(parse(example)), filename), config);
  assert.ok(Object.isFrozen(config.nodes[0]!.workers[0]!.workspaces));
  assert.throws(() => { (config.nodes as unknown[]).push({}); }, TypeError);
});

test("TOML errors, unknown keys at every boundary, missing sections and bad types are rejected without input values", () => {
  const bad = [
    "dataDir = [",
    modified(value => { delete value.router; }),
    modified(value => { delete value.nodes; }),
    modified(value => { delete value.dataDir; }),
    modified(value => { value.nodes = []; }),
    modified(value => { value.schemaVersion = 2; }),
    modified(value => { value.dataDir = 42; }),
    modified(value => { value.LINEAR_CLIENT_SECRET = "secret-input-marker"; }),
    modified(value => { (value.router as Record<string, unknown>).port = "8788"; }),
    modified(value => { (value.router as Record<string, unknown>).nativeSteer = "true"; }),
    modified(value => { (value.router as Record<string, unknown>).unknown = "secret-input-marker"; }),
    modified(value => { (value.linear as Record<string, unknown>).clientSecret = "secret-input-marker"; }),
    modified(value => { (value.relay as Record<string, unknown>).operatorToken = "secret-input-marker"; }),
    modified(value => { nodes(value)[0]!.unknown = "secret-input-marker"; }),
    modified(value => { workers(value)[0]!.unknown = "secret-input-marker"; }),
    modified(value => { workers(value)[0]!.port = 1.5; }),
    modified(value => { workers(value)[0]!.port = 0; }),
    modified(value => { workers(value)[0]!.port = 65536; }),
    modified(value => { workers(value)[0]!.kind = "unknown"; }),
    modified(value => { workers(value)[0]!.workspaces = []; }),
    modified(value => { workers(value)[0]!.runtimeProfile = { ref: "example", version: "v1", unknown: "secret-input-marker" }; }),
    modified(value => { (workers(value)[0]!.instructions as Record<string, unknown>).linearExecution = "secret-input-marker"; }),
    modified(value => { ((workers(value)[0]!.workspaces as Record<string, unknown>[])[0]!).unknown = "secret-input-marker"; }),
  ];
  for (const source of bad) assert.throws(() => parseAxisConfiguration(source, "/private/path-marker/axis.toml"), error => {
    assert.ok(error instanceof AxisConfigurationError);
    assert.ok(!error.message.includes("secret-input-marker"));
    assert.ok(!error.message.includes("path-marker"));
    return true;
  });
});

test("invalid identities, transport settings and contradictory topology fail before reading keys", () => {
  const edits: ((value: Record<string, unknown>) => void)[] = [
    value => { nodes(value).push({ ...nodes(value)[0] }); },
    value => { workers(value).push({ ...workers(value)[0] }); },
    value => { nodes(value)[0]!.eventPort = nodes(value)[0]!.port; },
    value => { workers(value)[0]!.port = nodes(value)[0]!.eventPort; },
    value => { nodes(value)[0]!.eventHost = "0.0.0.0"; },
    value => { workers(value)[0]!.host = "localhost"; },
    value => { nodes(value)[0]!.url = "http://node.example.invalid/"; },
    value => { nodes(value)[0]!.url = "http://127.1:9080/"; },
    value => { nodes(value)[0]!.url = "https://user:secret@node.example.invalid/"; },
    value => { nodes(value)[0]!.url = "https://node.example.invalid/path"; },
    value => { nodes(value)[0]!.eventRetryMaxDelayMs = 1000; nodes(value)[0]!.eventRetryInitialDelayMs = 2000; },
    value => { nodes(value)[0]!.defaultWorker = "missing"; },
    value => { (value.router as Record<string, unknown>).defaultWorker = "missing"; },
    value => { workers(value)[0]!.labels = ["codex", "codex"]; },
    value => { workers(value)[0]!.key = "invalid key"; },
    value => { workers(value)[0]!.instructions = { sessionResponse: "text", omitSessionResponse: true }; },
    value => { nodes(value)[0]!.workerScopeRules = [{ scope: { system: "linear", tenantId: "example-org", scopeId: "team-example" }, workerKey: "missing" }]; },
    value => { (value.linear as Record<string, unknown>).apiEnabled = false; },
    value => { (value.linear as Record<string, unknown>).executionGrants = []; },
  ];
  for (const edit of edits) assert.throws(() => parseAxisConfiguration(modified(edit), filename), AxisConfigurationError);
});

test("all databases and native session storage derive from one data directory", () => {
  const config = parseAxisConfiguration(example, filename);
  assert.deepEqual(databasePaths(config), {
    router: "/example/config/data/router.sqlite", channel: "/example/config/data/linear-channel.sqlite",
    conversations: "/example/config/data/linear-conversations.sqlite", runChain: "/example/config/data/linear-run-chain.sqlite",
    delivery: "/example/config/data/linear-delivery.sqlite", vault: "/example/config/data/relay-vault.json",
  });
  assert.equal(deriveNodeConfiguration(config, undefined, fakeSecret).database, "/example/config/data/node-node-one.sqlite");
  assert.equal(deriveWorkerConfiguration(config, undefined, undefined, fakeSecret).database, "/example/config/data/worker-worker-one.sqlite");
  const claude = parseAxisConfiguration(modified(value => { workers(value)[0]!.kind = "claude-code"; workers(value)[0]!.claudeExecutable = "./bin/claude"; }), filename);
  const worker = deriveWorkerConfiguration(claude, undefined, undefined, fakeSecret);
  assert.equal(worker.nativeSessionStorePath, "/example/config/data/worker-worker-one-native-sessions.json");
  assert.equal(worker.executable, "/example/config/bin/claude");
});

test("one topology derives matching Relay endpoints, targets, allowed origins, Node inventory and Worker credentials", () => {
  const config = parseAxisConfiguration(example, filename), relay = deriveRelayConfiguration(config, fakeSecret, { clientSecret: "example-client-secret", webhookSecret: "example-webhook-secret" });
  const node = deriveNodeConfiguration(config, undefined, fakeSecret), worker = deriveWorkerConfiguration(config, undefined, undefined, fakeSecret);
  const endpoint = relay.runtime.nodeEndpoints[0]!, target = relay.runtime.nodeTargets[0]!, binding = relay.eventBindings[0]!, inventory = node.workers[0]!;
  assert.deepEqual(validateNodeAuthenticatedContext(binding.authenticatedNode), { nodeRef: endpoint.nodeRef, nodeId: endpoint.nodeId });
  assert.deepEqual(binding.allowedOrigins, [target.identity]);
  assert.deepEqual(validateNodeTarget(inventory.target), { endpointRef: target.endpointRef, identity: target.identity, sessionBindingExecutorId: target.sessionBindingExecutorId });
  assert.deepEqual(inventory.nodeRef, endpoint.nodeRef);
  assert.equal(endpoint.bearerToken, node.token);
  assert.equal(binding.bearerToken, node.routerEventToken);
  assert.equal(inventory.commandToken, worker.token);
  assert.equal(inventory.eventToken, worker.routerToken);
  assert.equal(worker.routerEndpoint, "http://127.0.0.1:9081/");
  assert.equal(relay.linearExecution!.receivers[0]!.bearerToken, worker.linearExecution!.receiverToken);
  assert.deepEqual(relay.linearExecution!.receivers[0]!.identity, target.identity);
  assert.deepEqual(selectNodeInventoryWorker(node, { kind: "policy", scope: { system: "linear", tenantId: "example-org", scopeId: "team-example" }, executorLabels: [] }).target, inventory.target);
  assert.equal(new Set([relay.masterKey, relay.operatorToken, node.token, node.routerEventToken, worker.token, worker.routerToken, worker.linearExecution!.receiverToken]).size, 7);
});

test("Worker ports 80 and 443 derive canonical origins accepted by the private client", () => {
  for (const host of ["127.0.0.1", "::1"]) {
    for (const port of [80, 443]) {
      const config = parseAxisConfiguration(modified(value => { Object.assign(workers(value)[0]!, { host, port }); }), filename);
      const node = deriveNodeConfiguration(config, undefined, fakeSecret);
      const worker = node.workers[0]!;
      const authority = host === "::1" ? "[::1]" : host;
      assert.equal(worker.workerEndpoint, `http://${authority}${port === 80 ? "" : ":443"}`);
      assert.doesNotThrow(() => new HttpNodeWorkerClient({ worker, workerDispatchTimeoutMs: node.workerDispatchTimeoutMs }));
    }
  }
});

test("configured executor labels use the runtime vocabulary", () => {
  assert.throws(() => parseAxisConfiguration(modified(value => { workers(value)[0]!.labels = ["unknown-executor"]; }), filename), AxisConfigurationError);
  const node = deriveNodeConfiguration(parseAxisConfiguration(example, filename), undefined, fakeSecret);
  assert.ok(node.workerExecutorBindings.every(binding => isExecutorLabel(binding.executorLabel)));
});

test("the same scope cannot target two Workers when its TOML keys are reordered", () => {
  const source = modified(value => {
    const second = structuredClone(workers(value)[0]!);
    Object.assign(second, { id: "worker-two", key: "worker-two", endpointRef: "worker-two", port: 9083, kind: "claude-code", labels: ["claude"], hostRoot: "./hosts/worker-two" });
    workers(value).push(second);
    nodes(value)[0]!.workerScopeRules = [
      { scope: { system: "linear", tenantId: "example-org", scopeId: "team-example" }, workerKey: "worker-one" },
      { scope: { scopeId: "team-example", system: "linear", tenantId: "example-org" }, workerKey: "worker-two" },
    ];
  });
  assert.throws(() => parseAxisConfiguration(source, filename), error => error instanceof AxisConfigurationError && error.field === "nodes.workerScopeRules");
  const control = parse(source);
  const rules = nodes(control)[0]!.workerScopeRules as { scope: Record<string, unknown> }[];
  rules[1]!.scope.scopeId = "team-other";
  assert.doesNotThrow(() => parseAxisConfiguration(stringify(control), filename));
});

test("a custom runtime profile is preserved in every advertised and Worker identity", () => {
  const profile = { ref: "custom-profile", version: "command" };
  const config = parseAxisConfiguration(modified(value => { workers(value)[0]!.runtimeProfile = profile; }), filename);
  const relay = deriveRelayConfiguration(config, fakeSecret, { clientSecret: "example-client-secret", webhookSecret: "example-webhook-secret" }), node = deriveNodeConfiguration(config, undefined, fakeSecret);
  const worker = deriveWorkerConfiguration(config, undefined, undefined, fakeSecret);
  assert.deepEqual(worker.runtimeProfile, profile);
  assert.deepEqual(node.workers[0]!.target.identity.runtimeProfile, worker.runtimeProfile);
  assert.deepEqual(relay.runtime.nodeTargets[0]!.identity.runtimeProfile, worker.runtimeProfile);
  assert.deepEqual(relay.eventBindings[0]!.allowedOrigins[0]!.runtimeProfile, worker.runtimeProfile);
  assert.deepEqual(relay.linearExecution!.receivers[0]!.identity.runtimeProfile, worker.runtimeProfile);
});

test("local role projections read only their own secrets and support multiple physical Nodes", () => {
  const source = modified(value => {
    const second = structuredClone(nodes(value)[0]!);
    second.id = "node-two"; second.ref = "node-two";
    const worker = (second.workers as Record<string, unknown>[])[0]!;
    worker.id = "worker-two"; worker.key = "worker-two"; worker.endpointRef = "worker-two";
    second.defaultWorker = "worker-two";
    second.workerScopeRules = [];
    nodes(value).push(second);
    value.host = {};
  });
  const config = parseAxisConfiguration(source, filename);
  assert.throws(() => selectedNode(config), AxisConfigurationError);
  assert.throws(() => selectedWorker(config, "worker-two", "node-one"), AxisConfigurationError);
  const requested: string[] = [];
  deriveNodeConfiguration(config, "node-two", name => { requested.push(name); return fakeSecret(name); });
  assert.deepEqual(new Set(requested), new Set(["node-node-two-command", "node-node-two-router-events", "worker-worker-two-command", "worker-worker-two-events"]));
  requested.length = 0;
  deriveWorkerConfiguration(config, "worker-two", "node-two", name => { requested.push(name); return fakeSecret(name); });
  assert.deepEqual(new Set(requested), new Set(["worker-worker-two-command", "worker-worker-two-events", "worker-worker-two-linear"]));
});

test("init writes a valid default configuration and unique 0600 keys, and repeat init preserves all bytes", context => {
  const f = fixture(context), config = initializeAxis({ filename: f.filename, dataDir: "state" });
  assert.equal(config.dataDir, join(f.root, "state"));
  assert.deepEqual(loadAxisConfiguration(f.filename), config);
  assert.equal(lstatSync(join(config.dataDir, "secrets")).mode & 0o777, 0o700);
  const contents = secretNames(config).map(name => {
    const path = secretPath(config, name);
    assert.equal(lstatSync(path).mode & 0o777, 0o600);
    assert.equal(Buffer.from(readAxisSecret(config, name), "base64").length, 32);
    return readFileSync(path, "utf8");
  });
  assert.equal(new Set(contents).size, contents.length);
  const before = readFileSync(f.filename, "utf8");
  assert.throws(() => initializeAxis({ filename: f.filename }), AxisConfigurationError);
  assert.equal(readFileSync(f.filename, "utf8"), before);
  assert.deepEqual(secretNames(config).map(name => readFileSync(secretPath(config, name), "utf8")), contents);
  assert.equal(existsSync(join(config.dataDir, ".init.lock")), false);
});

test("init provisions an existing complete TOML unchanged, with separate keys for all declared services", context => {
  const f = fixture(context); writeFileSync(f.filename, example);
  const config = initializeAxis({ filename: f.filename });
  assert.equal(readFileSync(f.filename, "utf8"), example);
  assert.equal(secretNames(config).length, 7);
  assert.equal(readdirSync(join(config.dataDir, "secrets")).length, 7);
  const relay = deriveRelayConfiguration(config, undefined, { clientSecret: "example-client-secret", webhookSecret: "example-webhook-secret" }), node = deriveNodeConfiguration(config), worker = deriveWorkerConfiguration(config);
  assert.equal(relay.runtime.nodeEndpoints[0]!.bearerToken, node.token);
  assert.equal(node.workers[0]!.commandToken, worker.token);
  assert.equal(relay.linearExecution!.receivers[0]!.bearerToken, worker.linearExecution!.receiverToken);
  assert.throws(() => initializeAxis({ filename: f.filename, dataDir: "other" }), AxisConfigurationError);
});

test("preexisting keys and dangling symlinks refuse initialization before any key or config is overwritten", context => {
  const f = fixture(context), config = parseAxisConfiguration(example, f.filename);
  const directory = join(config.dataDir, "secrets"); mkdirSync(directory, { recursive: true, mode: 0o700 });
  const name = secretNames(config).at(-1)!;
  const path = secretPath(config, name); symlinkSync(join(f.root, "missing"), path);
  assert.throws(() => generateAxisSecrets(config), AxisConfigurationError);
  assert.equal(readdirSync(directory).length, 1);
  assert.equal(lstatSync(path).isSymbolicLink(), true);
  assert.equal(existsSync(f.filename), false);
});

test("key readers refuse unsafe modes, symlinks, invalid key contents and unsafe data directories", context => {
  const f = fixture(context), config = initializeAxis({ filename: f.filename }), path = secretPath(config, "relay-master");
  const key = readFileSync(path, "utf8");
  chmodSync(path, 0o640); assert.throws(() => readAxisSecret(config, "relay-master"), AxisConfigurationError); chmodSync(path, 0o600);
  writeFileSync(path, "secret-input-marker"); assert.throws(() => readAxisSecret(config, "relay-master"), error => error instanceof AxisConfigurationError && !error.message.includes("secret-input-marker"));
  writeFileSync(path, "=".repeat(44)); assert.throws(() => readAxisSecret(config, "relay-master"), AxisConfigurationError);
  writeFileSync(path, key); rmSync(path); symlinkSync(secretPath(config, "relay-operator"), path);
  assert.throws(() => readAxisSecret(config, "relay-master"), AxisConfigurationError);
  assert.throws(() => readAxisSecret(config, "../relay-master"), AxisConfigurationError);
  chmodSync(config.dataDir, 0o755); assert.throws(() => readAxisSecret(config, "relay-operator"), AxisConfigurationError);
});

test("init refuses unsafe directories and an active initialization lock", context => {
  const f = fixture(context), directory = join(f.root, "data"); mkdirSync(directory, { mode: 0o755 }); chmodSync(directory, 0o755);
  assert.throws(() => initializeAxis({ filename: f.filename }), AxisConfigurationError);
  assert.equal(existsSync(join(directory, "secrets")), false);
  chmodSync(directory, 0o700); mkdirSync(join(directory, "secrets"), { mode: 0o700 });
  writeFileSync(join(directory, ".init.lock"), "", { mode: 0o600 });
  assert.throws(() => initializeAxis({ filename: f.filename }), AxisConfigurationError);
  assert.equal(existsSync(f.filename), false);
});

test("external Linear secrets are obtained only from the supplied environment", () => {
  assert.deepEqual(readLinearSecrets({ LINEAR_CLIENT_SECRET: "environment-client", LINEAR_WEBHOOK_SECRET: "environment-webhook" }), { clientSecret: "environment-client", webhookSecret: "environment-webhook" });
  assert.throws(() => readLinearSecrets({}), AxisConfigurationError);
  assert.throws(() => readLinearSecrets({ clientSecret: "untrusted", webhookSecret: "untrusted" }), AxisConfigurationError);
  const config = parseAxisConfiguration(example, filename);
  assert.equal(JSON.stringify(config).includes("environment-client"), false);
});

test("top-level and init help share the exact init synopsis", () => {
  const bin = resolve("src/bin/axis.ts");
  const topLevel = execFileSync(process.execPath, ["--import=tsx", bin, "--help"], { encoding: "utf8", timeout: 10_000 });
  const init = execFileSync(process.execPath, ["--import=tsx", bin, "init", "--help"], { encoding: "utf8", timeout: 10_000 });
  const synopsis = init.split("\n")[0]!;
  assert.match(synopsis, /\[--interactive \| --non-interactive\]/u);
  assert.ok(synopsis.includes("[--env-file <axis.env>]"));
  assert.deepEqual(topLevel.split("\n").filter(line => line.trimStart().startsWith("axis init")), [`  ${synopsis}`]);
});

test("axis init CLI succeeds without a listener and refuses duplicate flags or existing keys without exposing paths", context => {
  const f = fixture(context), bin = resolve("src/bin/axis.ts");
  const args = ["--import=tsx", bin, "init", "--config", f.filename, "--data-dir", "state"];
  assert.match(execFileSync(process.execPath, args, { encoding: "utf8" }), /initialized/u);
  const again = spawnSync(process.execPath, args, { encoding: "utf8" });
  assert.equal(again.status, 1); assert.ok(!again.stderr.includes(f.root));
  const duplicate = spawnSync(process.execPath, ["--import=tsx", bin, "init", "--data-dir", "one", "--data-dir", "two"], { encoding: "utf8" });
  assert.equal(duplicate.status, 1);
  assert.match(duplicate.stderr, /initialization refused/u);
});
