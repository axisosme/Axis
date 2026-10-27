import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test, { type TestContext } from "node:test";

import { databasePaths, generateAxisSecrets, parseAxisConfiguration, readAxisSecret, type WorkerConfiguration } from "../../src/configuration.js";
import { CodexExecutor, codexExecutorRegistration, type CodexAppServerReadiness, type CodexAppServerTransport } from "../../src/executor/codex.js";
import { EncryptedFileCredentialVault, EnvelopeCipher } from "../../src/relay/security.js";
import { startServe, type ServeRuntime, type ServeWorkerRuntime } from "../../src/serve.js";
import { SqliteWorkerCommandJournal, WorkerCommandService } from "../../src/worker/command.js";
import { startWorkerHttpService } from "../../src/worker/http.js";
import { RouterHttpEventSink } from "../../src/worker/router-event-sink.js";
import { FakeLinearProvider, currentFixture, signed } from "../helpers.js";

const ready: CodexAppServerReadiness = {
  status: "ready", modelAvailability: "not_checked", degradedReasons: [],
  profiles: {
    explorer: { availability: "available", attempt: "not_attempted" },
    architect: { availability: "available", attempt: "not_attempted" },
    reviewer: { availability: "available", attempt: "not_attempted" },
    worker: { availability: "available", attempt: "not_attempted" },
  },
};
async function closeServer(server: Server): Promise<void> {
  if (server.listening) await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
}
async function freePorts(): Promise<number[]> {
  const servers: Server[] = [];
  try {
    const ports: number[] = [];
    for (let i = 0; i < 5; i += 1) {
      const server = createServer();
      servers.push(server);
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address();
      assert.ok(address !== null && typeof address !== "string");
      ports.push(address.port);
    }
    return ports;
  } finally { await Promise.all(servers.map(closeServer)); }
}
async function until(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 8_000;
  while (!predicate()) {
    if (Date.now() >= deadline) assert.fail("The fake execution did not converge.");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

async function fixture(context: TestContext) {
  const root = await mkdtemp(join(tmpdir(), "axis-serve-e2e-"));
  let runtime: ServeRuntime | undefined;
  context.after(async () => { try { await runtime?.close(); } finally { await rm(root, { recursive: true, force: true }); } });
  const [relayPort, routerPort, nodePort, eventPort, workerPort] = await freePorts();
  const config = parseAxisConfiguration(`schemaVersion = 1
dataDir = "data"
[relay]
port = ${relayPort}
[router]
port = ${routerPort}
defaultWorker = "worker-one"
[linear]
clientId = "example-client"
redirectUri = "http://127.0.0.1:${relayPort}/oauth/linear/callback"
teamIds = ["team-fixture"]
ingressTickIntervalMs = 5
observeTickIntervalMs = 5
outboxTickIntervalMs = 5
[[nodes]]
id = "node-one"
port = ${nodePort}
eventPort = ${eventPort}
eventRetryInitialDelayMs = 100
eventRetryMaxDelayMs = 1000
[[nodes.workers]]
id = "worker-one"
kind = "codex"
port = ${workerPort}
workspaces = [{ organizationId = "organization-fixture", workspaceRoot = "." }]
`, join(root, "axis.toml"));
  generateAxisSecrets(config);
  const provider = new FakeLinearProvider();
  const failures: unknown[] = [];
  const starts: string[] = [];
  let readiness = ready;
  let workerClosed = false;
  const startWorker = async (workerConfig: WorkerConfiguration): Promise<ServeWorkerRuntime> => {
    const transport: CodexAppServerTransport = {
      async readiness() { return readiness; },
      async startTurn(input) { starts.push(input.instruction); return { runtimeKind: "codex", threadId: "fixture-thread", turnId: "fixture-turn" }; },
      async continueTurn() { throw new Error("Unexpected continuation."); },
      async interruptTurn() {},
      async *observeTurn(binding) {
        yield { binding, outcome: "started" };
        yield { binding, outcome: "succeeded", response: { sourceEventId: "fixture-final", projectionStatus: "projected", body: "The fixture completed.", truncated: false, truncations: [] } };
      },
    };
    const executor = new CodexExecutor({ command: "/fixture/codex.js", transport });
    const journal = new SqliteWorkerCommandJournal(workerConfig.database);
    let worker: WorkerCommandService | undefined;
    try {
      worker = new WorkerCommandService({
        configuration: { bearerToken: workerConfig.token, workerId: workerConfig.workerId, runtimeKind: "codex", executorId: workerConfig.executorId,
          nodeId: workerConfig.nodeId, runtimeProfile: workerConfig.runtimeProfile, methodRef: { version: "v1", method: "codex.execute" }, workspaces: workerConfig.workspaces },
        registrations: [codexExecutorRegistration({ executorId: workerConfig.executorId, adapter: executor })],
        journal, eventSink: new RouterHttpEventSink({ endpoint: workerConfig.routerEndpoint, bearerToken: workerConfig.routerToken }),
      });
      await worker.recoverActiveRuns();
      await worker.replayUndeliveredEvents();
      const service = await startWorkerHttpService({ commandService: worker, readiness: () => executor.readiness(), host: workerConfig.host, port: workerConfig.port });
      const commandService = worker;
      return { service, close: async () => { try { await service.close(); } finally { commandService.close(); executor.close(); journal.close(); workerClosed = true; } } };
    } catch (error) { worker?.close(); executor.close(); journal.close(); throw error; }
  };
  const options = { startWorker, linearSecrets: { clientSecret: "example-secret", webhookSecret: "fixture-signing-secret" },
    relay: { provider, onTickError: ({ error }: { error: unknown }) => { failures.push(error); } } };
  return {
    config, provider, failures, starts, options,
    async start() { runtime = await startServe(config, options); return runtime; },
    degrade() { readiness = { ...ready, status: "degraded", degradedReasons: ["app_server_unavailable"] }; },
    workerClosed: () => workerClosed,
  };
}

test("serve delivers a signed webhook through Node and returns Worker events through Router", async context => {
  const state = await fixture(context);
  const vault = new EncryptedFileCredentialVault(databasePaths(state.config).vault, new EnvelopeCipher(readAxisSecret(state.config, "relay-master")));
  await vault.put("linear:organization-fixture:app-user-fixture", JSON.stringify({ version: 1, accessToken: "fixture-token", refreshToken: null, expiresAt: null, scope: "read write app:assignable app:mentionable" }));
  const runtime = await state.start();
  runtime.relay.channel.installations.install({ organizationId: "organization-fixture", appUserId: "app-user-fixture", credentialRef: "linear:organization-fixture:app-user-fixture", teamIds: ["team-fixture"], canAccessAllPublicTeams: false });
  assert.equal(await runtime.ready(), true);
  assert.equal((await fetch(new URL("/health/ready", runtime.relay.service.url))).status, 200);
  for (const server of [runtime.relay.routerService.server, runtime.node.service.server, runtime.node.service.eventServer, runtime.worker.service.server]) {
    const address = server.address();
    assert.ok(address !== null && typeof address !== "string");
    assert.equal(address.address, "127.0.0.1");
  }
  assert.equal((await fetch(new URL("/v1/node/readiness", runtime.relay.service.url))).status, 404);
  assert.equal((await fetch(new URL("/v1/node/readiness", runtime.node.service.url))).status, 401);
  const workerHeaders = { authorization: `Bearer ${readAxisSecret(state.config, "worker-worker-one-command")}` };
  assert.equal((await fetch(new URL("/v1/worker/readiness", runtime.worker.service.url), { headers: workerHeaders })).status, 200);
  const record = await fetch(new URL("/v1/worker/commands/unknown-command", runtime.worker.service.url), { headers: workerHeaders });
  assert.equal(record.status, 200);
  assert.deepEqual(await record.json(), { commandId: "unknown-command", status: "not_recorded" });
  const invalid = await fetch(new URL("/v1/worker/commands", runtime.worker.service.url), { method: "POST",
    headers: { ...workerHeaders, "content-type": "application/json" },
    body: JSON.stringify({ contractVersion: "fictional-contract", commandId: "invalid-command", command: "cancel", target: { runId: "unknown-run" } }) });
  assert.equal(invalid.status, 400);
  assert.equal(state.starts.length, 0);
  assert.equal((await fetch(new URL("/fictional/worker/commands", runtime.worker.service.url), { method: "POST", headers: workerHeaders })).status, 404);
  const body = Buffer.from(JSON.stringify(currentFixture("agent-session-created.json")));
  const headers = signed(body);
  const response = await fetch(new URL("/webhooks/linear", runtime.relay.service.url), { method: "POST", body,
    headers: { "Linear-Delivery": headers.deliveryId, "Linear-Event": headers.event, "Linear-Signature": headers.signature, "Linear-Timestamp": headers.timestamp } });
  assert.equal(response.status, 200);
  await until(() => state.provider.created.some(row => row.content.type === "response"));
  assert.equal(state.starts.length, 1);
  assert.ok(state.provider.created.some(row => row.content.type === "response" && row.content.body === "The fixture completed."));
  const database = new DatabaseSync(databasePaths(state.config).router, { readOnly: true });
  try { assert.ok(Number(database.prepare("SELECT COUNT(*) AS count FROM router_receipts").get()!.count) >= 2); }
  finally { database.close(); }
  state.degrade();
  assert.equal((await fetch(new URL("/health/ready", runtime.relay.service.url))).status, 503);
  await Promise.all([runtime.close(), runtime.close()]);
  assert.equal(await runtime.ready(), false);
  assert.equal(state.workerClosed(), true);
  assert.equal(runtime.node.service.server.listening, false);
  assert.equal(runtime.relay.routerService.server.listening, false);
  assert.deepEqual(state.failures, []);
});

test("serve keeps public ingress closed until readiness and releases roles on startup failure", async context => {
  const state = await fixture(context);
  await assert.rejects(startServe(state.config, { ...state.options, startWorker: async config => {
    await assert.rejects(fetch(state.config.relay.url, { signal: AbortSignal.timeout(500) }));
    return state.options.startWorker(config).then(runtime => {
      state.degrade();
      return runtime;
    });
  } }));
  assert.equal(state.workerClosed(), true);
  const node = state.config.nodes[0]!;
  const servers: Server[] = [];
  try {
    for (const port of [state.config.relay.port, state.config.router.port, node.port, node.eventPort, node.workers[0]!.port]) {
      const server = createServer();
      servers.push(server);
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(port, "127.0.0.1", resolve);
      });
    }
  } finally { await Promise.all(servers.map(closeServer)); }
});
