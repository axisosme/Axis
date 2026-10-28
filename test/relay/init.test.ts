import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { PassThrough, Writable } from "node:stream";
import test, { type TestContext } from "node:test";

import { databasePaths, deriveRelayConfiguration, initializeAxis, loadAxisConfiguration, readAxisSecret, secretNames } from "../../src/configuration.js";
import { bindInitTeam, checkInitExecutor, initEnvironmentFile, runInitWizard, type InitDependencies } from "../../src/init.js";
import { terminalInitPrompts, type InitPrompts } from "../../src/init-prompts.js";
import { LinearInstallationStore } from "../../src/relay/linear/installation.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { startRelay, type RelayRuntime } from "../../src/relay/runtime.js";

function fixture(context: TestContext, answers?: readonly string[]) {
  const root = mkdtempSync(join(process.cwd(), ".init-test-"));
  context.after(() => rmSync(root, { recursive: true, force: true }));
  const filename = join(root, "axis.toml"), envFile = join(root, "axis.env"), dataDir = join(root, "data");
  const inputs = [...answers ?? ["https://axis.example.com", "example-client", "example$client-secret", "example-webhook-secret", "2", "1", root]];
  const output: string[] = [], secretPrompts: boolean[] = [];
  const prompts: InitPrompts = {
    async ask(_label, options) { secretPrompts.push(options?.secret === true); assert.ok(inputs.length, "unexpected extra prompt"); return inputs.shift()!; },
    write: message => { output.push(message); }, close() {},
  };
  const state = { closed: 0, started: 0, begun: 0 };
  const dependencies: InitDependencies = {
    provider: {
      async resolveViewer(token) { assert.equal(token, "example-access-token"); return { organizationId: "example-org", appUserId: "example-app" }; },
      async listTeams(token) { assert.equal(token, "example-access-token"); return [{ id: "team-one", name: "First", key: "ONE" }, { id: "team-two", name: "Second", key: "TWO" }]; },
    },
    async startRelay(config, secrets) {
      assert.equal(secrets.clientSecret, "example$client-secret");
      state.started++;
      const store = new LinearInstallationStore(databasePaths(config).channel);
      let closed = false;
      const close = async (): Promise<void> => { if (!closed) { closed = true; state.closed++; store.close(); } };
      context.after(close);
      return { installations: store, close, oauth: {
        begin() { state.begun++; store.install({ organizationId: "example-org", appUserId: "example-app", credentialRef: "example-credential", teamIds: [], canAccessAllPublicTeams: false }); return "https://linear.example.com/oauth/authorize"; },
        async accessToken() { return "example-access-token"; },
      } };
    },
    async checkExecutor() { return ["executor available and native login status succeeded (fixture)."] ; },
    async checkService() { assert.equal(state.closed, 1); return "service not reachable; the temporary setup Relay has stopped."; },
    installationTimeoutMs: 30, pollIntervalMs: 1,
  };
  return { root, filename, envFile, dataDir, options: { filename, envFile, dataDir }, inputs, output, secretPrompts, prompts, dependencies, state };
}

test("scripted wizard persists a single-host config, private secrets, selected installation and audited binding", async context => {
  const f = fixture(context);
  await runInitWizard(f.options, f.prompts, f.dependencies);
  assert.deepEqual(f.state, { started: 1, begun: 1, closed: 1 });
  const config = loadAxisConfiguration(f.filename);
  assert.equal(config.nodes[0]!.workers[0]!.kind, "codex");
  assert.equal(config.nodes[0]!.workers[0]!.workspaces[0]!.organizationId, "example-org");
  assert.equal(config.nodes[0]!.workers[0]!.workspaces[0]!.workspaceRoot, f.root);
  assert.deepEqual(config.linear!.teamIds, ["team-two"]);
  assert.match(readFileSync(f.filename, "utf8"), /\[serve\]/u);
  assert.doesNotMatch(readFileSync(f.filename, "utf8"), /secret|setup-pending|\0/u);
  assert.equal(lstatSync(f.envFile).mode & 0o777, 0o600);
  assert.equal(lstatSync(f.filename).mode & 0o777, 0o600);
  assert.equal(readFileSync(f.envFile, "utf8"), "LINEAR_CLIENT_SECRET='example$client-secret'\nLINEAR_WEBHOOK_SECRET='example-webhook-secret'\n");
  for (const name of secretNames(config)) assert.ok(readAxisSecret(config, name));
  assert.equal(f.secretPrompts.filter(Boolean).length, 2);
  assert.doesNotMatch(f.output.join("\n"), /example\$client-secret|example-webhook-secret|example-access-token/u);
  assert.match(f.output.join("\n"), /Callback URL: https:\/\/axis.example.com\/oauth\/linear\/callback/u);
  assert.match(f.output.join("\n"), /Webhook event categories: AgentSessionEvent.*PermissionChange.*OAuthApp/u);
  assert.match(f.output.join("\n"), /node --env-file=.* serve --config/u);
  const installations = new LinearInstallationStore(databasePaths(config).channel);
  try {
    assert.equal(installations.authority("example-org", "example-app", "team-two")?.allowed, true);
    assert.equal(installations.authority("example-org", "example-app", "team-one")?.allowed, false);
  } finally { installations.close(); }
  const bindings = new SqliteTeamBindingStore({ filename: databasePaths(config).router });
  try {
    const row = bindings.current({ scope: { organizationId: "example-org", linearTeamId: "team-two" } });
    assert.equal(row?.revision, 1);
    assert.equal(row?.profile?.nodeId, "local");
    assert.equal(row?.profile?.workerId, "local-worker");
    assert.deepEqual(row?.profile?.endpointRef, { ref: "local-worker", version: "v1" });
    assert.equal(row?.audit.source, "axis-init");
    assert.equal(row?.audit.authorizationRef, "interactive-setup");
    assert.throws(() => bindInitTeam(config, "example-org", "team-two"), /binding was refused/u);
    assert.equal(bindings.history({ scope: { organizationId: "example-org", linearTeamId: "team-two" } }).length, 1);
  } finally { bindings.close(); }
});

test("wizard supports Claude Code selection without launching an executor session", async context => {
  const f = fixture(context);
  f.inputs.splice(4, 3, "1", "2", f.root, process.execPath);
  await runInitWizard(f.options, f.prompts, f.dependencies);
  const worker = loadAxisConfiguration(f.filename).nodes[0]!.workers[0]!;
  assert.equal(worker.kind, "claude-code");
  assert.equal(worker.claudeExecutable, process.execPath);
});

test("temporary Relay handles a real local callback with fake OAuth transport and releases listeners", async context => {
  const f = fixture(context);
  const localFetch = globalThis.fetch;
  const requests: string[] = [];
  context.mock.method(globalThis, "fetch", (async (input: string | URL | Request) => {
    const url = String(input); requests.push(url);
    if (url === "https://api.linear.app/oauth/token") return Response.json({ access_token: "example-access-token", scope: "read write" });
    if (url === "https://api.linear.app/graphql") return Response.json({ data: { viewer: { id: "example-app", organization: { id: "example-org" } } } });
    throw new Error("Unexpected transport request");
  }) as typeof fetch);
  let runtime: RelayRuntime | undefined, callback: Promise<Response> | undefined;
  context.after(async () => { await runtime?.close(); });
  const prompts: InitPrompts = { ...f.prompts, write(message) {
    f.prompts.write(message);
    if (message.startsWith("Open this URL")) {
      const authorize = new URL(message.split("\n")[1]!);
      const url = new URL("/oauth/linear/callback", runtime!.service.url);
      url.searchParams.set("code", "example-code"); url.searchParams.set("state", authorize.searchParams.get("state")!);
      callback = localFetch(url, { redirect: "manual", signal: AbortSignal.timeout(2_000) });
    }
  } };
  await runInitWizard(f.options, prompts, { ...f.dependencies, installationTimeoutMs: 5_000,
    async startRelay(config, secrets) {
      const derived = deriveRelayConfiguration(config, undefined, secrets);
      runtime = await startRelay({ ...derived, port: 0, runtime: { ...derived.runtime, routerPort: 0 } });
      return { oauth: runtime.channel.oauth, installations: runtime.channel.installations, close: async () => { await runtime!.close(); f.state.closed++; } };
    },
  });
  const response = await callback!;
  await response.body?.cancel();
  assert.equal(response.status, 303);
  assert.deepEqual(requests, ["https://api.linear.app/oauth/token", "https://api.linear.app/graphql"]);
  assert.equal(runtime!.service.server.listening, false);
  assert.equal(runtime!.routerService.server.listening, false);
});

test("existing config, env symlinks and keys are preserved", async context => {
  const f = fixture(context);
  writeFileSync(f.filename, "existing config");
  await assert.rejects(runInitWizard(f.options, f.prompts, f.dependencies), /requires new/u);
  assert.equal(readFileSync(f.filename, "utf8"), "existing config");
  rmSync(f.filename);
  const target = join(f.root, "other.env"); writeFileSync(target, "existing secret"); symlinkSync(target, f.envFile);
  await assert.rejects(runInitWizard(f.options, f.prompts, f.dependencies), /requires new/u);
  assert.equal(readFileSync(target, "utf8"), "existing secret");
  rmSync(f.envFile);
  const existing = initializeAxis({ filename: f.filename, dataDir: f.dataDir });
  const key = readAxisSecret(existing, "relay-master"); rmSync(f.filename);
  await assert.rejects(runInitWizard(f.options, f.prompts, f.dependencies), /already present/u);
  assert.equal(readAxisSecret(existing, "relay-master"), key);
  assert.equal(f.state.started, 0);
});

test("provider failure and installation timeout close temporary resources and preserve generated files", async context => {
  const f = fixture(context);
  await assert.rejects(runInitWizard(f.options, f.prompts, { ...f.dependencies,
    provider: { ...f.dependencies.provider, async listTeams() { throw new Error("fixture provider failure"); } },
  }), /fixture provider failure/u);
  assert.equal(f.state.closed, 1);
  assert.ok(existsSync(f.envFile));
  const g = fixture(context);
  await assert.rejects(runInitWizard(g.options, g.prompts, { ...g.dependencies,
    async startRelay(config, secrets) {
      const relay = await g.dependencies.startRelay(config, secrets);
      return { ...relay, oauth: { ...relay.oauth, begin: () => "https://linear.example.com/oauth/authorize" } };
    },
  }), /timed out/u);
  assert.equal(g.state.closed, 1);
});

test("revocation during Team choice cannot reactivate the installation or create a binding", async context => {
  const f = fixture(context);
  const ask = f.prompts.ask;
  const prompts: InitPrompts = { ...f.prompts, async ask(label, options) {
    if (label === "Choose a Team number") {
      const store = new LinearInstallationStore(join(f.dataDir, "linear-channel.sqlite"));
      try { store.revoke("example-org", "example-app"); } finally { store.close(); }
    }
    return ask(label, options);
  } };
  await assert.rejects(runInitWizard(f.options, prompts, f.dependencies), /installation changed/u);
  assert.equal(f.state.closed, 1);
  assert.doesNotMatch(readFileSync(f.filename, "utf8"), /organizationId = "example-org"/u);
});

test("environment file quoting is literal across Node dotenv parsing and rejects unsafe values", context => {
  const f = fixture(context);
  writeFileSync(f.envFile, initEnvironmentFile({ clientSecret: "literal$token", webhookSecret: "example-webhook" }), { mode: 0o600 });
  const run = spawnSync(process.execPath, [`--env-file=${f.envFile}`, "--input-type=module", "-e", "if (process.env.LINEAR_CLIENT_SECRET !== 'literal$token') process.exitCode = 1"], { encoding: "utf8", timeout: 5_000 });
  assert.equal(run.status, 0, run.stderr);
  for (const clientSecret of ["", "new\nline", "a'b", 'a"b', "a\\b", "a b", "a\u00a0b", "a\0b"]) assert.throws(() => initEnvironmentFile({ clientSecret, webhookSecret: "example" }));
});

test("Codex health reads profiles and probes login in the worker's effective native home", async context => {
  const f = fixture(context);
  const config = initializeAxis({ filename: f.filename, dataDir: f.dataDir });
  const codexHome = join(f.root, "native"), home = join(f.root, "home");
  mkdirSync(join(codexHome, "agents"), { recursive: true });
  for (const role of ["explorer", "architect", "reviewer", "worker"]) writeFileSync(join(codexHome, "agents", `${role}.toml`),
    `name = "${role}"\ndescription = "Example role"\ndeveloper_instructions = "Complete the assigned work."\n`);
  const observed: (string | undefined)[] = [];
  const probe = async (_command: string, _args: readonly string[], environment: Readonly<Record<string, string | undefined>>): Promise<boolean> => {
    observed.push(environment.CODEX_HOME); return true;
  };
  assert.ok((await checkInitExecutor(config, { CODEX_HOME: codexHome, HOME: home }, probe)).includes("Codex role profiles valid."));
  assert.deepEqual(observed, [codexHome, codexHome]);
  const worker = config.nodes[0]!.workers[0]!;
  const configured = { ...config, nodes: [{ ...config.nodes[0]!, workers: [{ ...worker, codexHome }] }] };
  observed.length = 0;
  assert.ok((await checkInitExecutor(configured, { CODEX_HOME: join(f.root, "unavailable"), HOME: home }, probe)).includes("Codex role profiles valid."));
  assert.deepEqual(observed, [codexHome, codexHome]);
  const fallbackHome = join(f.root, "fallback"); mkdirSync(fallbackHome);
  symlinkSync(codexHome, join(fallbackHome, ".codex"));
  assert.ok((await checkInitExecutor(config, { HOME: fallbackHome }, probe)).includes("Codex role profiles valid."));
});

test("terminal prompts suppress secret echo and restore ordinary echo; EOF cancels a pending question", async () => {
  const input = new PassThrough();
  let output = "";
  const destination = new Writable({ write(chunk, _encoding, done) { output += String(chunk); done(); } });
  const prompts = terminalInitPrompts(input, destination);
  try {
    const secret = prompts.ask("Secret", { secret: true });
    input.write("hidden-credential\r");
    assert.equal(await secret, "hidden-credential");
    assert.doesNotMatch(output, /hidden-credential/u);
    const ordinary = prompts.ask("Team"); input.write("one\r"); assert.equal(await ordinary, "one");
    // A redraw clears from column 1; the label must be redrawn with the answer, not erased by it.
    assert.match(output.slice(output.lastIndexOf("\u001b[0J")), /^\u001b\[0JTeam: .*one/u);
    const pending = prompts.ask("Pending"); input.end();
    await assert.rejects(pending);
  } finally { prompts.close(); input.destroy(); destination.end(); }
});

test("non-TTY mode fails clearly and explicit script flags preserve initialization", context => {
  const f = fixture(context);
  const invoke = (args: string[]) => spawnSync(process.execPath, ["--import=tsx", "src/bin/axis.ts", "init", ...args], { encoding: "utf8", timeout: 10_000 });
  for (const args of [[], ["--interactive"], ["--interactive", "--config", f.filename]]) {
    const result = invoke(args); assert.equal(result.status, 1); assert.match(result.stderr, /requires a terminal.*--non-interactive/u);
  }
  assert.equal(existsSync(f.filename), false);
  assert.equal(invoke(["--config", f.filename, "--data-dir", f.dataDir]).status, 0);
  assert.equal(invoke(["--config", f.filename]).status, 1);
  assert.equal(invoke(["--interactive", "--non-interactive"]).status, 1);
  // Call the parser directly: the development tsx loader consumes Node's identically named flag.
  const invalidEnv = spawnSync(process.execPath, ["--import=tsx", "--input-type=module", "-e",
    `import { runInitCli } from './src/init-cli.ts'; process.exitCode = await runInitCli(${JSON.stringify(["--config", f.filename, "--env-file", f.envFile])});`,
  ], { encoding: "utf8", timeout: 10_000 });
  assert.equal(invalidEnv.status, 1, invalidEnv.stderr);
});
