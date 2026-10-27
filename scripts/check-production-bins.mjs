import { temporaryConfiguration } from "./write-smoke-configuration.mjs";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

const root = process.cwd();
const temporary = await mkdtemp(join(tmpdir(), "axis-production-bins-"));
const workspaceRoot = join(temporary, "workspace");
const secret = "bin-smoke-secret";
let nodeWorkerSecret, nodeWorkerSecretB;
const nodeEventSecret = "node-router-event-bin-secret";
const nodeRef = { ref: "node-route-bin", version: "v1" };
const masterKey = Buffer.alloc(32, 7).toString("base64");
const started = [];
const nodeWorkerServers = [];

async function port() {
  const server = createServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  assert.ok(address !== null && typeof address !== "string");
  await new Promise((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
  return address.port;
}

async function installedBins() {
  const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  assert.deepEqual(manifest.bin, { "axis": "./dist/bin/axis.js", "axis-linear-cli": "./dist/bin/linear-cli.js", "axis-claude-worker": "./dist/bin/claude-worker.js", "axis-linear-relay": "./dist/bin/relay.js", "axis-codex-worker": "./dist/bin/worker.js", "axis-node": "./dist/bin/node.js", "axis-routing-preflight": "./dist/bin/routing-preflight.js" });
  const binDirectory = join(temporary, "installed", "node_modules", ".bin");
  await mkdir(binDirectory, { recursive: true });
  const aliases = {};
  for (const [name, target] of Object.entries(manifest.bin)) {
    assert.equal(typeof target, "string");
    const source = join(root, target.slice(2));
    assert.equal((await readFile(source, "utf8")).startsWith("#!/usr/bin/env node\n"), true, `${name} must retain a Node shebang`);
    await chmod(source, 0o755);
    const alias = join(binDirectory, name);
    await symlink(source, alias);
    aliases[name] = alias;
  }
  return aliases;
}

// Shaped like the native CLI's `--help`: one option per row, with the
// `--permission-mode` choices on a wrapped continuation line. The executor
// admits a CLI only when that row itself offers `bypassPermissions` .
const FAKE_CLAUDE_HELP = [
  ...["--safe-mode", "-p", "--session-id", "--resume", "--output-format", "--verbose", "--include-partial-messages", "--replay-user-messages", "--tools"].map((flag) => `  ${flag}                                Native option`),
  "  --input-format <format>               Input format (only works with --print): \"text\" (default), or \"stream-json\" (realtime streaming input)",
  "                                        (choices: \"text\", \"stream-json\")",
  "  --permission-mode <mode>              Permission mode to use for the session",
  "                                        (choices: \"acceptEdits\", \"auto\", \"bypassPermissions\", \"manual\", \"dontAsk\", \"plan\")",
].join("\n") + "\n";

async function fakeClaude(name, loggedIn) {
  const executable = join(temporary, name);
  const source = `#!${process.execPath}\nconst args = process.argv.slice(2);\nif (args[0] === "--help") process.stdout.write(${JSON.stringify(FAKE_CLAUDE_HELP)});\nelse if (args[0] === "auth" && args[1] === "status" && args[2] === "--json") { process.stdout.write("native account organization and auth fields must be discarded"); process.exitCode = ${loggedIn ? "0" : "1"}; }\nelse process.exitCode = 2;\n`;
  await writeFile(executable, source, { mode: 0o700 });
  return executable;
}

async function fakeNodeWorker(port, token) {
  const readiness = { status: "ready", modelAvailability: "not_checked", profiles: {
    explorer: { availability: "available", attempt: "not_attempted" }, architect: { availability: "available", attempt: "not_attempted" },
    reviewer: { availability: "available", attempt: "not_attempted" }, worker: { availability: "available", attempt: "not_attempted" },
  }, degradedReasons: [] };
  const server = createHttpServer((request, response) => {
    const body = request.headers.authorization === `Bearer ${token}` && request.method === "GET" && request.url === "/v1/worker/readiness"
      ? readiness : {};
    const status = body === readiness ? 200 : 404;
    const encoded = JSON.stringify(body);
    response.writeHead(status, { "content-type": "application/json", "content-length": Buffer.byteLength(encoded) });
    response.end(encoded);
  });
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(port, "127.0.0.1", resolve); });
  nodeWorkerServers.push(server);
}

function start(command, arguments_, environment, ready) {
  const child = spawn(command, arguments_, { cwd: root, env: { ...process.env, ...environment }, stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  let launchError;
  const collect = (chunk) => { output += chunk.toString(); };
  child.stdout.on("data", collect); child.stderr.on("data", collect);
  child.once("error", (error) => { launchError = error; });
  const closed = new Promise((resolve) => child.once("close", (code, signal) => resolve({ code, signal })));
  const waitReady = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${command} did not become ready: ${output.slice(0, 500)}`)), 10_000);
    const poll = () => {
      if (output.includes(ready)) { clearTimeout(timer); resolve(); return; }
      if (launchError !== undefined || child.exitCode !== null || child.signalCode !== null) { clearTimeout(timer); reject(new Error(`${command} exited before ready (${String(child.exitCode)}/${String(child.signalCode)}): ${launchError?.message ?? output.slice(0, 500)}`)); return; }
      setTimeout(poll, 20);
    };
    poll();
  });
  const launched = { child, closed, output: () => output, waitReady };
  started.push(launched);
  return launched;
}

async function terminate(process, expected = false) {
  if (process.child.exitCode === null && process.child.signalCode === null) process.child.kill("SIGTERM");
  let timer;
  const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("production bin did not stop after SIGTERM")), 10_000); });
  let outcome;
  try {
    outcome = await Promise.race([process.closed, timeout]);
  } catch (error) {
    if (process.child.exitCode === null && process.child.signalCode === null) process.child.kill("SIGKILL");
    await process.closed;
    throw error;
  } finally {
    clearTimeout(timer);
  }
  if (expected) assert.deepEqual(outcome, { code: 0, signal: null }, "production bin must stop cleanly after SIGTERM");
}

try {
  await mkdir(workspaceRoot);
  const aliases = await installedBins();
  const authenticatedClaude = await fakeClaude("claude-authenticated", true);
  const unauthenticatedClaude = await fakeClaude("claude-unauthenticated", false);
  const [workerPort, claudeWorkerPort, rejectedClaudePort, routerPort, relayPort, nodePort, nodeEventPort, nodeWorkerPortA, nodeWorkerPortB] = await Promise.all([port(), port(), port(), port(), port(), port(), port(), port(), port()]);
  const deployment = await temporaryConfiguration(temporary, { relay: { port: relayPort }, router: { port: routerPort },
    linear: { clientId: "bin-client", redirectUri: "https://relay.example.invalid/oauth" },
    nodes: [{ id: "node-bin", ref: nodeRef.ref, port: nodePort, eventPort: nodeEventPort, defaultWorker: "node-worker-a",
      workers: [
        { id: "node-worker-a", kind: "codex", port: nodeWorkerPortA }, { id: "node-worker-b", kind: "codex", port: nodeWorkerPortB },
        { id: "worker-bin", kind: "codex", port: workerPort },
        { id: "claude-worker-bin", kind: "claude-code", port: claudeWorkerPort, claudeExecutable: authenticatedClaude },
        { id: "rejected-claude-worker-bin", kind: "claude-code", port: rejectedClaudePort, claudeExecutable: unauthenticatedClaude },
      ].map(worker => ({ ...worker, labels: [], workspaces: [{ organizationId: "org-bin", workspaceRoot }] })) }] });
  nodeWorkerSecret = deployment.key("worker-node-worker-a-command"); nodeWorkerSecretB = deployment.key("worker-node-worker-b-command");
  const nodeCommandKey = deployment.key("node-node-bin-command");
  await Promise.all([fakeNodeWorker(nodeWorkerPortA, nodeWorkerSecret), fakeNodeWorker(nodeWorkerPortB, nodeWorkerSecretB)]);
  const rejectedNode = start(aliases["axis-node"], ["--config", join(temporary, "missing.toml")], {}, "never ready");
  await assert.rejects(rejectedNode.waitReady);
  assert.deepEqual(await rejectedNode.closed, { code: 1, signal: null });
  assert.equal(rejectedNode.output(), "axis.node.startup_failed\n");
  const node = start(aliases["axis-node"], ["--config", deployment.filename, "--node", "node-bin"], {}, "Axis Node ready");
  await node.waitReady;
  const nodeReadiness = await fetch(`http://127.0.0.1:${nodePort}/v1/node/readiness`, { headers: { authorization: `Bearer ${nodeCommandKey}` } });
  assert.equal(nodeReadiness.status, 200);
  const nodeReadinessBody = await nodeReadiness.json();
  assert.deepEqual(nodeReadinessBody.workers.map(({ workerId, transport, readinessSupport }) => [workerId, transport, readinessSupport]), [
    ["node-worker-a", "reachable", "supported"],
    ["node-worker-b", "reachable", "supported"],
    ["worker-bin", "unknown", "supported"],
    ["claude-worker-bin", "unknown", "unsupported"],
    ["rejected-claude-worker-bin", "unknown", "unsupported"],
  ]);
  assert.ok(nodeReadinessBody.workers.filter(({ transport }) => transport === "unknown").every(worker => !Object.hasOwn(worker, "readiness")));
  assert.equal((await fetch(`http://127.0.0.1:${nodePort}/v1/node/commands`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" })).status, 401);
  assert.equal((await fetch(`http://127.0.0.1:${nodeEventPort}/v1/router/run-events`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" })).status, 401);
  const roleArgs = worker => ["--config", deployment.filename, "--node", "node-bin", "--worker", worker];
  const worker = start(aliases["axis-codex-worker"], roleArgs("worker-bin"), {}, "Axis Codex Worker ready");
  await worker.waitReady;
  assert.equal((await fetch(`http://127.0.0.1:${workerPort}/v1/worker/commands`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" })).status, 401);
  const claudeWorker = start(aliases["axis-claude-worker"], roleArgs("claude-worker-bin"), {}, "Axis Claude Code Worker ready");
  await claudeWorker.waitReady;
  assert.equal((await fetch(`http://127.0.0.1:${claudeWorkerPort}/v1/worker/commands`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" })).status, 401);
  const rejectedClaude = start(aliases["axis-claude-worker"], roleArgs("rejected-claude-worker-bin"), {}, "Axis Claude Code Worker ready");
  await assert.rejects(rejectedClaude.waitReady);
  assert.deepEqual(await rejectedClaude.closed, { code: 1, signal: null });
  assert.equal(rejectedClaude.output(), "axis.worker.startup_failed\n");
  const relay = start(aliases["axis-linear-relay"], ["--config", deployment.filename], { LINEAR_CLIENT_SECRET: secret, LINEAR_WEBHOOK_SECRET: secret }, "Axis Router listener ready");
  await relay.waitReady;
  assert.equal((await fetch(`http://127.0.0.1:${relayPort}/health/live`)).status, 200);
  assert.equal((await fetch(`http://127.0.0.1:${relayPort}/health/ready`)).status, 200);
  // The Router accepts Node events only; the Worker event path is unknown here.
  assert.equal((await fetch(`http://127.0.0.1:${routerPort}/v1/router/run-events`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" })).status, 404);
  assert.equal((await fetch(`http://127.0.0.1:${routerPort}/v1/router/node-run-events`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" })).status, 401);
  await terminate(node, true);
  assert.equal((await fetch(`http://127.0.0.1:${nodeWorkerPortA}/v1/worker/readiness`, { headers: { authorization: `Bearer ${nodeWorkerSecret}` } })).status, 200);
  assert.equal((await fetch(`http://127.0.0.1:${nodeWorkerPortB}/v1/worker/readiness`, { headers: { authorization: `Bearer ${nodeWorkerSecretB}` } })).status, 200);
  await terminate(relay, true); await terminate(claudeWorker, true); await terminate(worker, true);
  for (const process of started) assert.equal([secret, nodeWorkerSecret, nodeEventSecret, masterKey, workspaceRoot, temporary].some((sensitive) => process.output().includes(sensitive)) || /native/iu.test(process.output()), false, "production bin output leaked a secret, path, or native detail");
  process.stdout.write("Packaged Relay, Node, Codex Worker, and Claude Worker production-bin smoke passed.\n");
} finally {
  await Promise.allSettled(started.map((process) => terminate(process)));
  await Promise.allSettled(nodeWorkerServers.map((server) => new Promise((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)))));
  await rm(temporary, { recursive: true, force: true });
}
