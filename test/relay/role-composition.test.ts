import assert from "node:assert/strict";
import { createServer } from "node:http";
import { existsSync, mkdtempSync, mkdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, mock, test, type TestContext } from "node:test";
import { stringify } from "smol-toml";

import {
  deriveNodeConfiguration, deriveRelayConfiguration, deriveWorkerConfiguration, generateAxisSecrets, parseAxisConfiguration,
  type WorkerConfiguration,
} from "../../src/configuration.js";
import * as codex from "../../src/executor/codex.js";
import * as claude from "../../src/executor/claude-code.js";
import { CODEX_EXECUTOR_CONTRACT_VERSION, executorError, type ExecutorStartRequest } from "../../src/shared/worker-executor.js";
import { completeNodeWorkerCommand } from "../../src/shared/router-node-commands.js";
import type { NodeHttpOptions } from "../../src/node/http.js";
import type { WorkerHttpOptions } from "../../src/worker/http.js";
import { selectNodeInventoryWorker } from "../../src/node/runtime-config.js";
import { buildNodeStartRequest } from "../support/router-node-commands-fixtures.js";
import { fakeAppServer } from "../support/codex-app-server.js";
import { SqliteWorkerCommandJournal } from "../../src/worker/command.js";

const workers: WorkerHttpOptions[] = [];
const transports: codex.PackageLocalCodexAppServerTransportOptions[] = [];
const nativeLaunchArguments: (readonly string[])[] = [];
const claudeOptions: claude.ClaudeCodeExecutorOptions[] = [];
const claudeStarts: ExecutorStartRequest[] = [];
let nodeOptions: NodeHttpOptions | undefined;
let refuseRelay = false;
let routerCloses = 0;
let stallNativeAt: "initialize" | "config/read" | undefined;
let onNativeRequest: (() => void) | undefined;
let nativeKills = 0;
const server = () => ({ server: createServer(), url: new URL("http://127.0.0.1:12345/"), close: async () => {} });

// Retain the real journals, selection and command services. Only the listener and native process boundaries are replaced.
const mocks = [
  mock.module(new URL("../../src/worker/http.ts", import.meta.url).href, { namedExports: {
    startWorkerHttpService: async (options: WorkerHttpOptions) => { workers.push(options); return server(); },
  } }),
  mock.module(new URL("../../src/node/http.ts", import.meta.url).href, { namedExports: {
    startNodeHttpService: async (options: NodeHttpOptions) => { nodeOptions = options; return { ...server(), eventServer: createServer(), eventUrl: new URL("http://127.0.0.1:12346/") }; },
  } }),
  mock.module(new URL("../../src/router/http.ts", import.meta.url).href, { namedExports: {
    startRouterHttpServiceWithNodeEvents: async () => ({ ...server(), close: async () => { routerCloses += 1; } }),
  } }),
  mock.module(new URL("../../src/relay/http.ts", import.meta.url).href, { namedExports: {
    startRelayHttpService: async () => { if (refuseRelay) throw new Error("listener refused"); return server(); },
  } }),
  mock.module(new URL("../../src/worker/router-event-sink.ts", import.meta.url).href, { namedExports: {
    RouterHttpEventSink: class { async emit() {} },
  } }),
  mock.module(new URL("../../src/executor/codex.ts", import.meta.url).href, { namedExports: {
    ...codex,
    PackageLocalCodexAppServerTransport: class extends codex.PackageLocalCodexAppServerTransport {
      readonly native: ReturnType<typeof fakeAppServer>;
      constructor(options: codex.PackageLocalCodexAppServerTransportOptions) {
        const native = fakeAppServer();
        if (stallNativeAt !== undefined) {
          native.child.stdin!.removeAllListeners("data");
          native.child.stdin!.on("data", (chunk: string) => {
            const request = JSON.parse(chunk.trim()) as { id: number; method: string };
            if (request.method === stallNativeAt) onNativeRequest?.();
            else if (request.id !== undefined) native.child.stdout!.emit("data", `${JSON.stringify({ id: request.id, result: {} })}\n`);
          });
          native.child.kill = () => { nativeKills += 1; return true; };
        }
        super({ ...options, spawnProcess: (_command, args) => { nativeLaunchArguments.push(args); return native.child; } });
        transports.push(options);
        this.native = native;
      }
      override async startTurn(input: codex.CodexAppServerStartTurnInput) {
        const binding = await super.startTurn(input);
        this.native.notify("turn/started", { threadId: binding.threadId, turn: { id: binding.turnId, status: "inProgress" } });
        return binding;
      }
    },
  } }),
  mock.module(new URL("../../src/executor/claude-code.ts", import.meta.url).href, { namedExports: {
    ...claude,
    ClaudeCodeExecutor: class extends claude.ClaudeCodeExecutor {
      constructor(options: claude.ClaudeCodeExecutorOptions) { super(options); claudeOptions.push(options); }
      override async preflight() {}
      override async start(request: ExecutorStartRequest) {
        claudeStarts.push(request);
        return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "start" as const,
          status: "failed" as const, binding: null, error: executorError("start", "executor.launch_failed") };
      }
    },
  } }),
];
after(() => { for (const entry of mocks) entry.restore(); });
const { startCodexWorker } = await import("../../src/worker/runtime.js");
const { startClaudeWorker } = await import("../../src/worker/claude-runtime.js");
const { startNode } = await import("../../src/node/runtime.js");
const { startRelay } = await import("../../src/relay/runtime.js");

for (const method of ["initialize", "config/read"] as const) {
  test(`Codex startup cancellation releases a pending ${method} handshake and its journal`, async context => {
    const { config } = fixture(context);
    const workerConfig = deriveWorkerConfiguration(config, "worker-1");
    const controller = new AbortController();
    const pending = new Promise<void>(resolve => { onNativeRequest = resolve; });
    const beforeWorkers = workers.length, beforeKills = nativeKills;
    let closes = 0;
    const originalClose = SqliteWorkerCommandJournal.prototype.close;
    context.mock.method(SqliteWorkerCommandJournal.prototype, "close", function(this: SqliteWorkerCommandJournal) { closes += 1; originalClose.call(this); });
    stallNativeAt = method;
    const starting = startCodexWorker(workerConfig, {}, controller.signal);
    try {
      await pending;
      controller.abort();
      await assert.rejects(starting, error => error === controller.signal.reason);
      assert.equal(workers.length, beforeWorkers, "no Worker listener opens after cancellation");
      assert.equal(nativeKills - beforeKills, 1);
      assert.equal(closes, 1);
    } finally { controller.abort(); stallNativeAt = undefined; onNativeRequest = undefined; }
  });
}

function fixture(context: TestContext, codexHome: string | null = "native-home") {
  const root = mkdtempSync(join(tmpdir(), "axis-role-composition-"));
  context.after(() => rmSync(root, { recursive: true, force: true }));
  const workspaceRoot = join(root, "workspace");
  mkdirSync(workspaceRoot);
  const config = parseAxisConfiguration(stringify({
    schemaVersion: 1, dataDir: "data", router: { port: 8788 },
    linear: { clientId: "example-client", redirectUri: "https://relay.example.invalid/oauth/linear/callback" },
    nodes: [{ id: "physical-node-1", port: 9080, eventPort: 9081, defaultWorker: "codex-primary", workers: [
      { id: "worker-1", key: "codex-primary", kind: "codex", port: 9082, executorId: "executor-1", runtimeProfile: { ref: "custom-profile", version: "v1" },
        ...(codexHome === null ? {} : { codexHome }), instructions: { sessionResponse: "Return the requested result." }, workspaces: [{ organizationId: "org-1", workspaceRoot }] },
      { id: "worker-claude", kind: "claude-code", port: 9083, executorId: "executor-claude", runtimeProfile: { ref: "custom-claude", version: "command" },
        claudeExecutable: process.execPath, instructions: { omitSessionResponse: true }, workspaces: [{ organizationId: "org-1", workspaceRoot }] },
    ] }],
  }), join(root, "axis.toml"));
  generateAxisSecrets(config);
  return { config, root };
}

async function commandFor(configuration: WorkerConfiguration) {
  const request = await buildNodeStartRequest();
  return completeNodeWorkerCommand(request, { workerKey: "codex-primary", target: {
    endpointRef: { ref: configuration.workerId, version: "v1" },
    identity: { nodeId: configuration.nodeId, workerId: configuration.workerId, executorKind: configuration.kind, runtimeProfile: configuration.runtimeProfile },
    sessionBindingExecutorId: configuration.executorId,
  } });
}

test("role projections start Node and Codex directly and retain custom identities across restart", async context => {
  const { config } = fixture(context);
  const nodeConfig = deriveNodeConfiguration(config);
  const node = await startNode(nodeConfig);
  context.after(() => node.close());
  assert.deepEqual(nodeOptions?.configuration, nodeConfig);
  assert.equal(selectNodeInventoryWorker(nodeConfig, { kind: "policy", scope: { system: "linear", tenantId: "org-1", scopeId: "team-example" }, executorLabels: [] }).workerKey, "codex-primary");
  const workerConfig = deriveWorkerConfiguration(config, "worker-1");
  const inherited = { PATH: "/usr/bin", HOME: "/example/native-home", CODEX_HOME: "/example/unselected-home", AXIS_WORKER_ID: "ignored-identity", AXIS_WORKER_TOKEN: "host-secret", LINEAR_API_KEY: "personal-secret" };
  let runtime = await startCodexWorker(workerConfig, inherited);
  context.after(() => runtime.close());
  const service = workers.at(-1)!.commandService;
  const command = await commandFor(workerConfig);
  const result = await service.execute(command);
  assert.equal(result.response.status, "accepted");
  if (result.response.status !== "accepted") throw new Error("Expected acceptance.");
  assert.deepEqual(result.response.identity.runtimeProfile, { ref: "custom-profile", version: "v1" });
  assert.equal(service.authenticate(workerConfig.token), true);
  assert.equal(service.authenticate(inherited.AXIS_WORKER_TOKEN), false);
  const native = transports.at(-1)!;
  assert.equal(native.executionEnvironment?.HOME, inherited.HOME);
  assert.equal(native.executionEnvironment?.CODEX_HOME, workerConfig.codexHome);
  assert.equal(native.executionEnvironment?.LINEAR_EXECUTION_STATUS, "disabled");
  assert.equal(native.executionEnvironment?.AXIS_WORKER_TOKEN, undefined);
  assert.deepEqual(native.instructions, { sessionResponse: "Return the requested result." });
  assert.equal(inherited.LINEAR_API_KEY, "personal-secret");
  await runtime.close();
  runtime = await startCodexWorker(workerConfig, inherited);
  assert.equal(workers.at(-1)!.commandService.readRecordedResponse(command.commandId)?.status, "accepted");
});

test("Codex profile discovery follows the native home when the role supplies no override", async context => {
  for (const setting of ["CODEX_HOME", "HOME"] as const) {
    const { config, root } = fixture(context, null);
    const home = join(root, "native-account");
    const codexHome = setting === "CODEX_HOME" ? join(root, "native-codex") : join(home, ".codex");
    const profiles = join(codexHome, "agents");
    mkdirSync(profiles, { recursive: true });
    const profile = join(profiles, "worker.toml");
    writeFileSync(profile, 'name = "worker"\ndescription = "Native worker profile"\ndeveloper_instructions = "Use the native worker instructions."\n');
    const workerConfig = deriveWorkerConfiguration(config, "worker-1");
    const environment = { HOME: home, ...(setting === "CODEX_HOME" ? { CODEX_HOME: codexHome } : {}) };
    const runtime = await startCodexWorker(workerConfig, environment);
    try {
      const launch = nativeLaunchArguments.at(-1)!;
      assert.ok(launch.includes(`agents.worker.config_file=${JSON.stringify(realpathSync(profile))}`), `${setting} must select the child's profile source`);
      assert.equal(transports.at(-1)!.executionEnvironment?.HOME, home);
    } finally { await runtime.close(); }
  }
});

test("Claude startup uses its projected session store, instructions and identity with independent native environment", async context => {
  const { config } = fixture(context);
  const workerConfig = deriveWorkerConfiguration(config, "worker-claude");
  const environment = { HOME: "/example/native-home", PATH: "/usr/bin", AXIS_WORKER_ID: "ignored-identity" };
  const runtime = await startClaudeWorker(workerConfig, environment);
  context.after(() => runtime.close());
  const options = claudeOptions.at(-1)!;
  assert.equal(options.nativeSessionStorePath, join(config.dataDir, "worker-worker-claude-native-sessions.json"));
  assert.deepEqual(options.instructions, { sessionResponse: null });
  assert.equal(options.environment?.HOME, environment.HOME);
  const command = await commandFor(workerConfig);
  await assert.rejects(workers.at(-1)!.commandService.execute(command), /launch failed/);
  assert.equal(claudeStarts.at(-1)?.workingDirectory, realpathSync(config.nodes[0]!.workers[1]!.workspaces[0]!.workspaceRoot));
  assert.equal(runtime.nativeSessionLaunchCount(), 0);
});

test("invalid role configuration fails before a journal or listener can open", async context => {
  const { config } = fixture(context);
  const nodeDatabase = join(config.dataDir, "node-physical-node-1.sqlite");
  const sameKey = () => Buffer.alloc(32, 1).toString("base64");
  assert.throws(() => deriveNodeConfiguration(config, undefined, sameKey), /secrets/);
  assert.equal(existsSync(nodeDatabase), false);
  const workerConfig = deriveWorkerConfiguration(config, "worker-claude");
  await assert.rejects(startClaudeWorker({ ...workerConfig, executable: "/missing/example-claude" }, {}), /executable/);
  assert.equal(existsSync(workerConfig.database), false);
  await assert.rejects(startCodexWorker(workerConfig, {}), /requires Codex/);
  assert.equal(existsSync(workerConfig.database), false);
});

test("Relay starts from its projection and releases stores and the Router listener after a partial startup failure", async context => {
  const { config } = fixture(context);
  const relayConfig = deriveRelayConfiguration(config, undefined, { clientSecret: "example-client-secret", webhookSecret: "example-webhook-secret" });
  refuseRelay = true;
  const before = routerCloses;
  await assert.rejects(startRelay(relayConfig), /listener refused/);
  refuseRelay = false;
  assert.equal(routerCloses, before + 1);
  assert.equal(existsSync(`${relayConfig.runtime.routerDatabase}-wal`), false);
  assert.equal(existsSync(`${relayConfig.linear!.databases.channel}-wal`), false);
  const relay = await startRelay(relayConfig);
  assert.equal(relay.channel.ready, true);
  await relay.close();
  assert.equal(relay.channel.inbox.ready(), false);
});
