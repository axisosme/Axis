import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, mock, test, type TestContext } from "node:test";

import { generateAxisSecrets, parseAxisConfiguration } from "../../src/configuration.js";
import * as routerHttp from "../../src/router/http.js";
import type { RelayHttpOptions } from "../../src/relay/http.js";

const events: string[] = [];
let failIngress = false, failWorker = false, failWorkerClose = false;
let relayOptions: RelayHttpOptions | undefined;
let abortDuringWorker: AbortController | undefined;
function listener(name: string) {
  events.push(`${name}:start`);
  const server = createServer();
  let active = true;
  Object.defineProperty(server, "listening", { get: () => active });
  return { server, url: new URL("http://127.0.0.1:8787/"), close: async () => { active = false; events.push(`${name}:stop`); } };
}
async function worker(kind: string) {
  if (failWorker) throw new Error("Fixture Worker startup failed.");
  const service = listener(kind);
  abortDuringWorker?.abort();
  return { service, close: async () => { await service.close(); if (failWorkerClose) throw new Error("Fixture Worker close failed."); } };
}
const mocks = [
  mock.module(new URL("../../src/router/http.ts", import.meta.url).href, { namedExports: {
    ...routerHttp, startRouterHttpServiceWithNodeEvents: async () => listener("router"),
  } }),
  mock.module(new URL("../../src/relay/http.ts", import.meta.url).href, { namedExports: {
    startRelayHttpService: async (options: RelayHttpOptions) => {
      relayOptions = options;
      if (failIngress) throw new Error("Fixture ingress startup failed.");
      return listener("ingress");
    },
  } }),
  mock.module(new URL("../../src/node/http.ts", import.meta.url).href, { namedExports: {
    startNodeHttpService: async () => { const service = listener("node"); return { ...service, eventServer: service.server, eventUrl: service.url }; },
  } }),
  mock.module(new URL("../../src/worker/runtime.ts", import.meta.url).href, { namedExports: {
    startCodexWorker: async () => worker("codex"),
  } }),
  mock.module(new URL("../../src/worker/claude-runtime.ts", import.meta.url).href, { namedExports: {
    canonicalClaudeExecutable: async (path: string) => path,
    startClaudeWorker: async () => worker("claude-code"),
  } }),
];
after(() => { for (const entry of mocks) entry.restore(); });
const { startServe } = await import("../../src/serve.js");

function fixture(context: TestContext, kind = "codex") {
  events.length = 0; failIngress = false; failWorker = false; failWorkerClose = false; abortDuringWorker = undefined;
  const root = mkdtempSync(join(tmpdir(), "axis-serve-lifecycle-"));
  context.after(() => rmSync(root, { recursive: true, force: true }));
  const config = parseAxisConfiguration(`schemaVersion = 1
dataDir = "data"
[serve]
kind = "${kind}"
organizationId = "example-organization"
${kind === "claude-code" ? 'claudeExecutable = "bin/claude"' : ""}
[linear]
clientId = "example-client"
redirectUri = "http://127.0.0.1:8787/oauth/linear/callback"
`, join(root, "axis.toml"));
  generateAxisSecrets(config);
  context.mock.method(globalThis, "fetch", async () => new Response(JSON.stringify({ storage: { status: "available" }, workers: [{ readiness: { status: "ready" } }] })));
  return config;
}
const options = { linearSecrets: { clientSecret: "example-secret", webhookSecret: "example-webhook-secret" } };

for (const kind of ["codex", "claude-code"]) {
  test(`serve starts ${kind} before ingress and closes every role in reverse order once`, async context => {
    const runtime = await startServe(fixture(context, kind), options);
    try {
      assert.deepEqual(events, ["router:start", "node:start", `${kind}:start`, "ingress:start"]);
      assert.equal(await relayOptions!.ready(), true);
      assert.equal(await runtime.ready(), true);
    } finally { await Promise.all([runtime.close(), runtime.close()]); }
    assert.deepEqual(events.slice(4), ["ingress:stop", `${kind}:stop`, "node:stop", "router:stop"]);
    assert.equal(await runtime.ready(), false);
  });
}

test("serve releases started roles when Worker startup, ingress binding or startup cancellation fails", async context => {
  for (const failure of ["worker", "ingress", "abort"]) {
    const config = fixture(context);
    failWorker = failure === "worker"; failIngress = failure === "ingress";
    const signal = failure === "abort" ? new AbortController() : undefined;
    abortDuringWorker = signal;
    await assert.rejects(startServe(config, { ...options, ...(signal === undefined ? {} : { signal: signal.signal }) }));
    assert.deepEqual(events, failure === "worker"
      ? ["router:start", "node:start", "node:stop", "router:stop"]
      : ["router:start", "node:start", "codex:start", "codex:stop", "node:stop", "router:stop"]);
  }
});

test("a Worker close error still closes Node and Router and is reported", async context => {
  const runtime = await startServe(fixture(context), options);
  failWorkerClose = true;
  await assert.rejects(runtime.close(), AggregateError);
  assert.deepEqual(events.slice(4), ["ingress:stop", "codex:stop", "node:stop", "router:stop"]);
});

test("public readiness and runtime readiness both refuse a stopped Router", async context => {
  const runtime = await startServe(fixture(context), options);
  try {
    await runtime.relay.routerService.close();
    assert.equal(await relayOptions!.ready(), false);
    assert.equal(await runtime.ready(), false);
  } finally { await runtime.close(); }
});
