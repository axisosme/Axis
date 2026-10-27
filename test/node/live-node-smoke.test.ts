import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { placeholderConfiguration, placeholderInstallation, placeholderRuntimeConfigurations, runLiveNodeSmoke, validatePlaceholderInstallation } from "../../scripts/live-node-smoke.mjs";
import { SqliteNodeJournal } from "../../src/node/journal.js";

const commit = "a".repeat(40);
const args = [
  "--live-read-only",
  "--command-origin", "https://node.example.invalid",
  "--event-origin", "http://127.0.0.1:4102",
  "--node-ref", "node-placeholder",
  "--node-version", "command",
  "--reviewed-commit", commit,
];
const executionScenarios = ["two_independent_starts", "supported_continue", "supported_cancel", "event_return", "rollback"];
const backupChild = `
  import { backup, DatabaseSync } from "node:sqlite";
  let database;
  try {
    database = new DatabaseSync(process.argv[1], { readOnly: true });
    database.exec("PRAGMA busy_timeout=50; PRAGMA query_only=ON;");
    await backup(database, process.argv[2]);
  } catch {
    process.exitCode = 1;
  } finally {
    try { database?.close(); } catch {}
  }
`;

const codexReadiness = {
  status: "ready",
  modelAvailability: "not_checked",
  profiles: Object.fromEntries(["explorer", "architect", "reviewer", "worker"].map((role) => [role, { availability: "available", attempt: "not_attempted" }])),
  degradedReasons: [],
};

function jsonResponse(status: number, value: unknown): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}

function assertUnverifiedExecutionScenarios(checks: readonly { readonly name: string; readonly status: string }[]): void {
  assert.deepEqual(
    checks.filter(({ name }) => executionScenarios.includes(name)).map(({ name, status }) => ({ name, status })),
    executionScenarios.map((name) => ({ name, status: "unverified" })),
  );
}

test("placeholder Node, two Worker, and Router configurations pass their TOML projections", () => {
  // Every service's databases must be distinct from every other's.
  assert.deepEqual(validatePlaceholderInstallation(), { services: 4, workers: 2, distinctHostRoots: 4, distinctDatabases: 8, distinctAuthorities: 6 });
  const { controller, node, first, second } = placeholderRuntimeConfigurations();
  assert.equal(controller.runtime.nodeTargets.length, 2);
  assert.equal(node.workers.length, 2);
  assert.equal(first.workerId, "worker-a");
  assert.equal(second.workerId, "worker-b");
  assert.equal(JSON.stringify(placeholderInstallation).includes("/Users/"), false);
  assert.equal(JSON.stringify(placeholderInstallation).includes("private instruction"), false);
  assert.match(placeholderConfiguration, /REPLACE_WITH_/u);
});

test("the operator runbook embeds the exact parser-backed placeholder TOML", () => {
  const operations = readFileSync(new URL("../../docs/MULTI-HOST-DEPLOYMENT.md", import.meta.url), "utf8");
  const nodeMvp = readFileSync(new URL("../../docs/NODE-MVP.md", import.meta.url), "utf8");
  assert.ok(operations.includes(placeholderConfiguration.trim()));
  assert.equal(operations.includes("AXIS_WORKER_ENDPOINTS_JSON='"), false);
  assert.equal(operations.includes("Safe maintenance sequence"), false);
  assert.equal(nodeMvp.includes("stop every owner and take a current matched backup"), false);
  assert.equal(nodeMvp.includes("Keep every continuation-owning direct or Node-only Codex Worker alive"), false);
  assert.ok(operations.includes("Node upgrade is supported only through its drain-gated lifecycle"));
  assert.equal(operations.includes("restore the prior direct configuration and all four matched journals/binaries together"), false);
  const nodeReadinessDistinction = operations.indexOf("Node bearer authentication and readiness do not prove provider authentication.");
  const providerPrerequisite = operations.indexOf("every selected provider runtime is already authenticated in its\nservice context");
  const noAutoLogin = operations.indexOf("Never auto-login");
  const missingAuthUnverified = operations.indexOf("If safe proof is absent, do not run the\naffected manual native action and keep those scenarios `unverified`");
  assert.ok(nodeReadinessDistinction >= 0 && nodeReadinessDistinction < providerPrerequisite && providerPrerequisite < noAutoLogin && noAutoLogin < missingAuthUnverified);
  const nodeMvpProviderPrerequisite = nodeMvp.indexOf("each selected provider runtime is already authenticated in its service context");
  const nodeMvpScenarios = nodeMvp.indexOf("| Scenario | Reproducible operator action");
  assert.ok(nodeMvpProviderPrerequisite >= 0 && nodeMvpProviderPrerequisite < nodeMvpScenarios);
  assert.ok(nodeMvp.includes("Node bearer authentication and readiness do not prove provider authentication."));
  assert.ok(nodeMvp.includes("Never auto-login."));
  assert.ok(nodeMvp.includes("If safe proof is absent, do not run the affected manual native\naction and leave that scenario and every dependent scenario `unverified`."));
  assert.match(operations, /axis node drain --host-root \/ABSOLUTE\/PATH\/TO\/node --output json/u);
  assert.equal(operations.includes("/Users/"), false);
});

/** Once the owner stops, a read-only backup of the checkpointed Node journal succeeds and is intact. */
test("a stopped Node journal backs up intact", async () => {
  const directory = mkdtempSync(join(tmpdir(), "axis-node-backup-"));
  const source = join(directory, "node.sqlite");
  const destination = join(directory, "node-backup.sqlite");
  const journal = await SqliteNodeJournal.open(source, { busyTimeoutMs: 50 });
  let closed = false;
  try {
    journal.close();
    closed = true;
    const afterClose = spawnSync(process.execPath, ["--input-type=module", "-e", backupChild, source, destination], {
      encoding: "utf8",
      timeout: 5_000,
    });
    assert.equal(afterClose.signal, null);
    assert.equal(afterClose.status, 0);
    const backup = new DatabaseSync(destination, { readOnly: true });
    try {
      assert.equal((backup.prepare("PRAGMA integrity_check").get() as { integrity_check: string }).integrity_check, "ok");
      assert.equal((backup.prepare("SELECT COUNT(*) AS count FROM metadata").get() as { count: number }).count > 0, true);
    } finally {
      backup.close();
    }
  } finally {
    if (!closed) journal.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("no arguments performs no network call or mutation and reports live checks as unverified", async () => {
  let calls = 0;
  const outcome = await runLiveNodeSmoke({
    args: [],
    env: {},
    fetch: async () => { calls += 1; throw new Error("network must not be reached"); },
  });
  assert.equal(outcome.exitCode, 0);
  assert.equal(outcome.receipt.mode, "configuration-only");
  assert.equal(outcome.receipt.result, "unverified");
  assert.equal(outcome.receipt.mutationAttempted, false);
  assert.equal(calls, 0);
  assert.deepEqual(outcome.receipt.checks.map(({ status }) => status), ["passed", "unverified", "unverified", "unverified", "unverified", "unverified"]);
});

test("live mode refuses missing target parameters before network access", async () => {
  let calls = 0;
  const fetch = async () => { calls += 1; throw new Error("network must not be reached"); };
  const missingTarget = await runLiveNodeSmoke({ args: ["--live-read-only"], env: { AXIS_NODE_LIVE_TOKEN: "sensitive" }, fetch });
  assert.equal(missingTarget.exitCode, 2);
  assert.equal(missingTarget.receipt.checks[0]?.safeCode, "axis.node.smoke.invalid_arguments");
  assertUnverifiedExecutionScenarios(missingTarget.receipt.checks);
  assert.equal(calls, 0);
});

test("authorized live mode performs only bounded GETs and emits sanitized aggregate evidence", async () => {
  const calls: { url: string; init: RequestInit }[] = [];
  const responses = [
    jsonResponse(401, { error: "never-read" }),
    jsonResponse(200, {
      contractVersion: "1.0.0",
      nodeRef: { ref: "node-placeholder", version: "command" },
      observedAt: "2026-09-08T00:00:00.000Z",
      service: { status: "configured" },
      storage: { status: "available" },
      workers: [
        { workerId: "worker-a", transport: "reachable", readinessSupport: "supported", readiness: codexReadiness },
        { workerId: "worker-b", transport: "reachable", readinessSupport: "supported", readiness: codexReadiness },
      ],
    }),
    jsonResponse(200, {
      contractVersion: "1.0.0",
      nodeRef: { ref: "node-placeholder", version: "command" },
      mode: "drain",
      idle: true,
      blockerCounts: {
        reservedCommands: 0, dispatchingCommands: 0, ambiguousCommands: 0, pendingNodeEvents: 0,
        quarantinedNodeEvents: 0, activeWorkerRuns: 0, pendingWorkerOutbox: 0, unknownWorkerEvidence: 0,
      },
    }),
  ];
  const outcome = await runLiveNodeSmoke({
    args,
    env: { AXIS_NODE_LIVE_TOKEN: "do-not-disclose-live-token" },
    fetch: async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), init: init ?? {} });
      const response = responses.shift();
      if (response === undefined) throw new Error("unexpected request");
      return response;
    },
  });
  assert.equal(outcome.exitCode, 0);
  assert.equal(outcome.receipt.result, "unverified");
  assert.equal(outcome.receipt.mutationAttempted, false);
  assert.equal(calls.length, 3);
  assert.ok(calls.every(({ init }) => init.method === "GET" && init.redirect === "error"));
  assert.deepEqual(outcome.receipt.checks.slice(0, 4).map(({ status }) => status), ["passed", "passed", "passed", "passed"]);
  assertUnverifiedExecutionScenarios(outcome.receipt.checks);
  const encoded = JSON.stringify(outcome.receipt);
  assert.equal(encoded.includes("do-not-disclose-live-token"), false);
  assert.equal(encoded.includes("node.example.invalid"), false);
  assert.equal(encoded.includes("worker-a"), false);
  assert.equal(encoded.includes("worker-b"), false);
});

test("live transport failures never disclose credentials, target URLs, or raw errors", async () => {
  const sentinel = "do-not-disclose-live-token";
  const outcome = await runLiveNodeSmoke({
    args,
    env: { AXIS_NODE_LIVE_TOKEN: sentinel },
    fetch: async () => { throw new Error(`${sentinel} https://node.example.invalid native raw output`); },
  });
  assert.equal(outcome.exitCode, 1);
  assert.equal(outcome.receipt.result, "failed");
  assertUnverifiedExecutionScenarios(outcome.receipt.checks);
  const encoded = JSON.stringify(outcome.receipt);
  for (const sensitive of [sentinel, "node.example.invalid", "native raw output"]) assert.equal(encoded.includes(sensitive), false);
});
