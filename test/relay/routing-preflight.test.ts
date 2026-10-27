import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { writeConfigurationFixture } from "../support/configuration-fixture.js";

interface Receipt { readonly status: string; readonly configuration: string; readonly blockers: readonly string[]; }
const { version } = JSON.parse(await readFile(new URL("../../package.json", import.meta.url), "utf8")) as { readonly version: string };
async function preflight(role: "relay" | "worker", filename: string, flags: readonly string[] = []): Promise<{ code: number; report: Receipt }> {
  return new Promise((resolve, reject) => {
    execFile(process.execPath, ["--import=tsx", "src/bin/routing-preflight.ts", role, "--config", filename, "--expected-version", version, ...flags],
      { env: { PATH: process.env.PATH, LINEAR_CLIENT_SECRET: "fictional-client-secret", LINEAR_WEBHOOK_SECRET: "fictional-webhook-secret" }, timeout: 30_000 }, (error, stdout) => {
        try { resolve({ code: error === null ? 0 : Number(error.code), report: JSON.parse(stdout) }); } catch { reject(new Error("preflight returned no receipt")); }
      });
  });
}
test("Linear AgentSession preflight blocks disabled execution and accepts the complete local Controller and Worker configuration", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-preflight-")); context.after(() => rm(root, { recursive: true, force: true }));
  for (const enabled of [false, true]) {
    const config = await writeConfigurationFixture(join(root, String(enabled)), {
      linear: { clientId: "example-client", redirectUri: "https://relay.example.invalid/oauth", apiEnabled: true,
        executionGrants: [{ organizationId: "org-one", appUserId: "app-one", scope: "installation-workspace" }] },
      nodes: [{ id: "node-one", port: 9080, eventPort: 9081, workers: [{ id: "worker-one", kind: "codex", port: 9082, linearExecution: enabled,
        workspaces: [{ organizationId: "org-one", workspaceRoot: root }] }] }],
    });
    for (const role of ["relay", "worker"] as const) {
      const result = await preflight(role, config.filename, ["--require-linear-execution", "true"]);
      assert.deepEqual([result.code, result.report.configuration, result.report.blockers], enabled ? [0, "valid", []] : [1, "valid", ["linear_execution_required"]]);
    }
  }
});
test("Worker preflight accepts Linear execution with the account's normal Codex home", async context => {
  const root = await mkdtemp(join(tmpdir(), "axis-worker-preflight-")); context.after(() => rm(root, { recursive: true, force: true }));
  const config = await writeConfigurationFixture(root);
  const result = await preflight("worker", config.filename);
  assert.deepEqual([result.code, result.report.configuration, result.report.blockers], [0, "valid", []]);
});
