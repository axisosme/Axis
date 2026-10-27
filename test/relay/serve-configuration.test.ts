import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { stringify } from "smol-toml";

import { AxisConfigurationError, deriveNodeConfiguration, deriveRelayConfiguration, initializeAxis, parseAxisConfiguration } from "../../src/configuration.js";
import { startServe, validateServeConfiguration } from "../../src/serve.js";

const source = `schemaVersion = 1
dataDir = "data"
[serve]
organizationId = "example-organization"
[linear]
clientId = "example-client"
redirectUri = "http://127.0.0.1:8787/oauth/linear/callback"
`;

test("single-host shorthand derives the full topology and supports either executor", () => {
  const config = parseAxisConfiguration(source, "/example/axis.toml");
  validateServeConfiguration(config);
  assert.equal(config.nodes.length, 1);
  assert.equal(config.nodes[0]!.workers.length, 1);
  assert.equal(config.nodes[0]!.workers[0]!.kind, "codex");
  assert.equal(config.nodes[0]!.workers[0]!.workspaces[0]!.workspaceRoot, "/example");
  assert.equal(config.router.defaultWorker, config.nodes[0]!.workers[0]!.id);
  const claude = parseAxisConfiguration(source.replace('[serve]', '[serve]\nkind = "claude-code"\nclaudeExecutable = "bin/claude"'), "/example/axis.toml");
  validateServeConfiguration(claude);
  assert.equal(claude.nodes[0]!.workers[0]!.claudeExecutable, "/example/bin/claude");
  assert.throws(() => validateServeConfiguration(parseAxisConfiguration(source.replace('[serve]', '[serve]\nkind = "claude-code"'))), AxisConfigurationError);
  for (const addition of ['\n[router]\nport = 8788', '\nnodes = []', '\n[host]\nnode = "local"']) {
    assert.throws(() => parseAxisConfiguration(source.replace('[serve]', `${addition}\n[serve]`)), AxisConfigurationError);
  }
  assert.throws(() => parseAxisConfiguration(source.replace('organizationId =', 'unknown =')), AxisConfigurationError);
});

test("explicit private ports retain normalized Router and Node destinations", () => {
  const readSecret = (name: string): string => createHash("sha256").update(name).digest("base64");
  for (const role of ["router", "node"]) {
    for (const port of [80, 443]) {
      for (const explicitUrl of [false, true]) {
        const origin = `http://127.0.0.1:${port}/`;
        const expected = port === 80 ? "http://127.0.0.1/" : "http://127.0.0.1:443/";
        const config = parseAxisConfiguration(stringify({
          schemaVersion: 1, dataDir: "data",
          linear: { clientId: "example-client", redirectUri: "http://127.0.0.1:8787/oauth/linear/callback" },
          router: { port: role === "router" ? port : 8788, ...(role === "router" && explicitUrl ? { url: origin } : {}) },
          nodes: [{ id: "local", port: role === "node" ? port : 9080, eventPort: 9081,
            ...(role === "node" && explicitUrl ? { url: origin } : {}),
            workers: [{ id: "local-worker", kind: "codex", port: 9082, workspaces: [{ organizationId: "example-organization", workspaceRoot: "." }] }],
          }],
        }), "/example/axis.toml");
        assert.equal(role === "router" ? config.router.url : config.nodes[0]!.url, expected);
        assert.doesNotThrow(() => validateServeConfiguration(config));
        const node = deriveNodeConfiguration(config, undefined, readSecret);
        const relay = deriveRelayConfiguration(config, readSecret, { clientSecret: "example-secret", webhookSecret: "example-webhook-secret" });
        assert.equal(node.routerEndpoint, config.router.url);
        assert.equal(relay.runtime.nodeEndpoints[0]!.endpoint, config.nodes[0]!.url);
        const mismatchedUrl = `http://127.0.0.1:${port + 1}/`;
        for (const url of [mismatchedUrl, "https://127.0.0.1/"]) {
          const invalid = role === "router" ? { ...config, router: { ...config.router, url } }
            : { ...config, nodes: [{ ...config.nodes[0]!, url }] };
          assert.throws(() => validateServeConfiguration(invalid), AxisConfigurationError);
        }
      }
    }
  }
});

test("serve rejects unsafe routes, cardinality and collisions before secret reads or storage", async context => {
  const root = mkdtempSync(join(tmpdir(), "axis-serve-config-"));
  context.after(() => rmSync(root, { recursive: true, force: true }));
  const config = parseAxisConfiguration(source, join(root, "axis.toml"));
  const node = config.nodes[0]!;
  const invalid = [
    { ...config, nodes: [] },
    { ...config, nodes: [node, { ...node, id: "second" }] },
    { ...config, nodes: [{ ...node, workers: [] }] },
    { ...config, nodes: [{ ...node, host: "0.0.0.0" }] },
    { ...config, router: { ...config.router, host: "::1" } },
    { ...config, router: { ...config.router, url: "https://router.example/" } },
    { ...config, nodes: [{ ...node, url: "https://node.example/" }] },
    { ...config, relay: { ...config.relay, port: node.port } },
  ];
  for (const entry of invalid) {
    await assert.rejects(startServe(entry, { readSecret: () => { assert.fail("invalid topology must not read secrets"); } }), AxisConfigurationError);
  }
  await assert.rejects(startServe(config, { readSecret: () => Buffer.alloc(32, 1).toString("base64") }), AxisConfigurationError);
  assert.equal(existsSync(config.dataDir), false);
});

test("init emits a serve template and the CLI documents serve and refuses invalid flags", context => {
  const root = mkdtempSync(join(tmpdir(), "axis-serve-init-"));
  context.after(() => rmSync(root, { recursive: true, force: true }));
  validateServeConfiguration(initializeAxis({ filename: join(root, "axis.toml") }));
  const invoke = (args: string[]) => spawnSync(process.execPath, ["--import=tsx", "src/bin/axis.ts", ...args], { encoding: "utf8", timeout: 10_000 });
  assert.match(invoke(["serve", "--help"]).stdout, /axis serve.*--config/u);
  assert.match(invoke(["--help"]).stdout, /axis serve/u);
  for (const args of [["--config"], ["--node", "local"], ["--config", "one", "--config", "two"], ["extra"]]) {
    const result = invoke(["serve", ...args]);
    assert.equal(result.status, 1);
    assert.equal(result.stderr.trim(), "axis.serve.startup_failed");
  }
});
