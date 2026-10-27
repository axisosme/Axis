import { temporaryConfiguration } from "./write-smoke-configuration.mjs";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { rm, mkdtemp } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

const [axisNode, consumer] = process.argv.slice(2);
assert.ok(axisNode && consumer, "installed Node smoke requires the installed bin and consumer root");
const temporary = await mkdtemp(join(tmpdir(), "axis-installed-node-"));
const servers = [];
let child;

function send(response, status, value) {
  const encoded = JSON.stringify(value);
  response.writeHead(status, { "content-type": "application/json", "content-length": Buffer.byteLength(encoded) });
  response.end(encoded);
}

async function requestBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function listen(server, port) {
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(port, "127.0.0.1", resolve); });
  servers.push(server);
}

async function availablePort() {
  const server = createNetServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  assert.ok(address !== null && typeof address !== "string");
  await new Promise((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
  return address.port;
}

async function closeServer(server) {
  if (!server.listening) return;
  await new Promise((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error)));
}

function acceptedEvent(start, messageId = "installed-worker-accepted") {
  return {
    contractVersion: start.contractVersion,
    messageId,
    correlationId: start.correlationId,
    operationId: start.operationId,
    causationMessageId: start.messageId,
    occurredAt: "2026-09-06T00:00:01.000Z",
    producer: "worker",
    consumer: "router",
    envelopeType: "run.accepted",
    task: start.identity.task,
    attempt: start.identity.attempt,
    run: { ...start.identity.run, state: "running" },
  };
}

function routerReceipt(payload) {
  // Node command is the only composition, so the Node delivers through the
  // authenticated Node-event door and expects that door's receipt.
  return {
    contractVersion: "1.0.0",
    nodeRef: payload.nodeRef,
    sourceMessageId: payload.envelope.messageId,
    runId: payload.envelope.run.runId,
    status: "buffered",
    replayed: false,
  };
}

async function fakeWorker(port, target, token) {
  const readiness = {
    status: "ready",
    modelAvailability: "not_checked",
    profiles: {
      explorer: { availability: "available", attempt: "not_attempted" },
      architect: { availability: "available", attempt: "not_attempted" },
      reviewer: { availability: "available", attempt: "not_attempted" },
      worker: { availability: "available", attempt: "not_attempted" },
    },
    degradedReasons: [],
  };
  const commands = [];
  const server = createHttpServer((request, response) => {
    void (async () => {
      if (request.headers.authorization !== `Bearer ${token}`) return send(response, 401, {});
      if (request.method === "GET" && request.url === "/v1/worker/readiness") return send(response, 200, readiness);
      if (request.method !== "POST" || request.url !== "/v1/worker/commands") return send(response, 404, {});
      const command = await requestBody(request);
      commands.push(command);
      return send(response, 202, {
        contractVersion: command.contractVersion,
        commandId: command.commandId,
        status: "accepted",
        identity: target.identity,
        runAccepted: acceptedEvent(command.start),
      });
    })().catch(() => response.destroy());
  });
  await listen(server, port);
  return { port, commands };
}

async function post(url, token, value) {
  const response = await fetch(url, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify(value),
  });
  return { status: response.status, body: await response.json() };
}

async function eventually(predicate, label) {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(`${label} did not settle`);
}

try {
  const [nodePort, eventPort, routerPort, workerPort1, workerPort2] = await Promise.all([
    availablePort(), availablePort(), availablePort(), availablePort(), availablePort(),
  ]);
  const nodeRef = { ref: "installed-node", version: "v1" };
  const target1 = {
    endpointRef: { ref: "installed-endpoint-1", version: "v1" },
    identity: { nodeId: "installed-node-id", workerId: "worker-1", executorKind: "codex", runtimeProfile: { ref: "installed-profile-1", version: "v1" } },
    sessionBindingExecutorId: "executor-1",
  };
  const target2 = {
    endpointRef: { ref: "installed-endpoint-2", version: "v1" },
    identity: { nodeId: "installed-node-id", workerId: "worker-2", executorKind: "codex", runtimeProfile: { ref: "installed-profile-2", version: "v1" } },
    sessionBindingExecutorId: "executor-2",
  };
  const deployment = await temporaryConfiguration(temporary, { router: { port: routerPort }, nodes: [{ id: "installed-node-id", ref: nodeRef.ref,
    port: nodePort, eventPort, defaultWorker: "worker-key-1", workers: [target1, target2].map((target, index) => ({
      id: target.identity.workerId, key: `worker-key-${index + 1}`, executorId: target.sessionBindingExecutorId, kind: "codex", labels: [],
      runtimeProfile: target.identity.runtimeProfile, endpointRef: target.endpointRef.ref, endpointVersion: target.endpointRef.version,
      port: index === 0 ? workerPort1 : workerPort2, workspaces: [{ organizationId: "org-smoke", workspaceRoot: temporary }],
    })) }] });
  const nodeToken = deployment.key("node-installed-node-id-command"), routerToken = deployment.key("node-installed-node-id-router-events");
  const eventToken1 = deployment.key("worker-worker-1-events");
  const workerToken1 = deployment.key("worker-worker-1-command"), workerToken2 = deployment.key("worker-worker-2-command");
  const worker1 = await fakeWorker(workerPort1, target1, workerToken1);
  await fakeWorker(workerPort2, target2, workerToken2);
  const routedEvents = [];
  const routedOrigins = [];
  const router = createHttpServer((request, response) => {
    void (async () => {
      if (request.method !== "POST" || request.url !== "/v1/router/node-run-events" || request.headers.authorization !== `Bearer ${routerToken}`) return send(response, 401, {});
      const payload = await requestBody(request);
      routedEvents.push(payload.envelope);
      routedOrigins.push(payload.origin);
      return send(response, 202, routerReceipt(payload));
    })().catch(() => response.destroy());
  });
  await listen(router, routerPort);

  const contractVersion = "1.0.0";
  const start = {
    contractVersion, messageId: "m-run-start", correlationId: "c1", causationMessageId: "m-submit", occurredAt: "2026-08-25T00:00:01Z",
    producer: "router", consumer: "worker", envelopeType: "run.start", operationId: "operation-1",
    identity: {
      contractVersion,
      task: { contractVersion, taskId: "task-1", correlationId: "c1", sourceRef: { system: "linear", organizationId: "org-1", issueId: "issue-1", agentSessionId: "installed-session-1" } },
      attempt: { contractVersion, attemptId: "attempt-1", taskId: "task-1", correlationId: "c1" },
      run: { contractVersion, runId: "run-1", taskId: "task-1", attemptId: "attempt-1", sessionBindingId: "binding-1", correlationId: "c1", state: "pending" },
      sessionBinding: { contractVersion, sessionBindingId: "binding-1", taskId: "task-1", attemptId: "attempt-1", runId: "run-1", correlationId: "c1", workerId: "worker-1", executorId: "executor-1" },
    },
  };
  // A command start names a selection, not a target: the Node picks the Worker from
  // its own policy and completes the pending session binding from that Worker.
  // `policy` is the only selection shape. No executor label and no
  // scope rule configured below, so this resolves to nodes.defaultWorker.
  const { workerId: _workerId, executorId: _executorId, nativeSessionRef: _nativeSessionRef, ...pendingBinding } = start.identity.sessionBinding;
  const request = {
    contractVersion: "1.0.0",
    nodeRef,
    target: { selection: { kind: "policy", scope: { system: "linear", tenantId: "organization-smoke", scopeId: "team-smoke" }, executorLabels: [] } },
    command: {
      contractVersion: "1.0.0",
      commandId: "installed-command-1",
      command: "start",
      start: { ...start, identity: { ...start.identity, sessionBinding: pendingBinding } },
      instruction: "installed smoke",
      workspacePolicyRef: null,
    },
  };

  let output = "";
  child = spawn(axisNode, ["--config", deployment.filename, "--node", "installed-node-id"], {
    cwd: consumer, stdio: ["ignore", "pipe", "pipe"], env: process.env,
  });
  child.stdout.on("data", (chunk) => { output += chunk.toString(); });
  child.stderr.on("data", (chunk) => { output += chunk.toString(); });
  const closed = new Promise((resolve) => child.once("close", (code, signal) => resolve({ code, signal })));
  await eventually(() => output.includes("Axis Node ready\n") || child.exitCode !== null, "installed axis-node startup");
  assert.equal(output, "Axis Node ready\n");

  assert.equal((await fetch(`http://127.0.0.1:${nodePort}/v1/node/readiness`)).status, 401);
  const readiness = await fetch(`http://127.0.0.1:${nodePort}/v1/node/readiness`, { headers: { authorization: `Bearer ${nodeToken}` } });
  assert.equal(readiness.status, 200);
  assert.deepEqual((await readiness.json()).workers.map(({ workerId, transport }) => [workerId, transport]), [["worker-1", "reachable"], ["worker-2", "reachable"]]);
  assert.equal((await post(`http://127.0.0.1:${nodePort}/v1/node/commands`, nodeToken, request)).status, 202);
  assert.equal(worker1.commands.length, 1);
  // the installed Node addresses the command to the exact inventory identity it reserved.
  assert.deepEqual(worker1.commands[0].executionTarget, { identity: target1.identity, sessionBindingExecutorId: target1.sessionBindingExecutorId });
  const event = acceptedEvent(start, "installed-event-1");
  assert.equal((await post(`http://127.0.0.1:${eventPort}/v1/router/run-events`, eventToken1, { envelope: event, recordedAt: "2026-09-06T00:00:02.000Z" })).status, 202);
  await eventually(() => routedEvents.length === 1, "installed event delivery");
  assert.equal(routedEvents[0].messageId, "installed-event-1");
  assert.deepEqual(routedOrigins[0], target1.identity, "the event reaches Router attributed to the Worker the default key chose");

  child.kill("SIGTERM");
  assert.deepEqual(await closed, { code: 0, signal: null });
  child = undefined;
  assert.equal((await fetch(`http://127.0.0.1:${workerPort1}/v1/worker/readiness`, { headers: { authorization: `Bearer ${workerToken1}` } })).status, 200);
  assert.equal((await fetch(`http://127.0.0.1:${workerPort2}/v1/worker/readiness`, { headers: { authorization: `Bearer ${workerToken2}` } })).status, 200);
  process.stdout.write("Installed axis-node command, event, readiness, and shutdown smoke passed.\n");
} finally {
  if (child !== undefined && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  await Promise.allSettled(servers.map(closeServer));
  await rm(temporary, { recursive: true, force: true });
}
