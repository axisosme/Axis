import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { linearExecutionContractVersion } from "@axisosme/axis-contracts/linear-execution";

import { linearExecutionEnvironment } from "../../src/shared/linear-execution-client.js";
import { AxisLinearRequestError, axisLinearSdkFromEnvironment } from "../../src/shared/linear-sdk.js";

const credential = `axlea1_${"a".repeat(43)}`;

test("generated child metadata replaces inherited values and enables the SDK with local expiry refusal", async context => {
  const inherited = { HOME: "/example/native-home", AXIS_RELAY_OPERATOR_TOKEN: "host-secret", LINEAR_API_KEY: "personal-key",
    LINEAR_EXECUTION_STATUS: "available", LINEAR_EXECUTION_EXPIRES_AT: "invalid", LINEAR_EXECUTION_SDK_MODULE: "file:///untrusted/example.js", LINEAR_CLI_LAUNCHER_ACTIVE: "1" };
  const env = linearExecutionEnvironment(inherited, "https://relay.example.invalid/", {
    contractVersion: linearExecutionContractVersion, requestId: "request-example", status: "issued",
    execution: { taskId: "task-example", attemptId: "attempt-example", runId: "run-example", organizationId: "org-example" },
    credentialId: "credential-example", credential, expiresAt: new Date(Date.now() + 60_000).toISOString(),
  });
  assert.equal(env.LINEAR_EXECUTION_STATUS, "available");
  assert.equal(env.LINEAR_API_KEY, credential);
  assert.equal(env.AXIS_RELAY_OPERATOR_TOKEN, undefined);
  assert.equal(env.LINEAR_CLI_LAUNCHER_ACTIVE, undefined);
  assert.notEqual(env.LINEAR_EXECUTION_SDK_MODULE, inherited.LINEAR_EXECUTION_SDK_MODULE);
  assert.equal(typeof (await import(env.LINEAR_EXECUTION_SDK_MODULE!)).axisLinearSdkFromEnvironment, "function");
  let calls = 0;
  context.mock.method(globalThis, "fetch", async () => {
    calls += 1;
    return new Response(JSON.stringify({ data: { viewer: { id: "app-example" } } }), { headers: { "content-type": "application/json" } });
  });
  assert.equal((await axisLinearSdkFromEnvironment(env).viewer).id, "app-example");
  assert.equal(calls, 1);
  const expired = axisLinearSdkFromEnvironment({ ...env, LINEAR_EXECUTION_EXPIRES_AT: new Date(Date.now() - 1_000).toISOString() });
  await assert.rejects(expired.viewer, error => error instanceof AxisLinearRequestError && error.code === "linear.execution.expired");
  assert.equal(calls, 1);
  const disabled = linearExecutionEnvironment(inherited, undefined, undefined, "disabled");
  assert.equal(disabled.LINEAR_EXECUTION_EXPIRES_AT, undefined);
  assert.equal(disabled.LINEAR_EXECUTION_SDK_MODULE, undefined);
  assert.throws(() => axisLinearSdkFromEnvironment(disabled), error => error instanceof AxisLinearRequestError && error.code === "linear.execution.unavailable");
  assert.equal(inherited.LINEAR_API_KEY, "personal-key");
});

test("the dedicated CLI replaces Run credentials, clears execution metadata and refuses a recursive child", context => {
  const root = mkdtempSync(join(tmpdir(), "axis-cli-child-"));
  context.after(() => rmSync(root, { recursive: true, force: true }));
  const directory = join(root, ".axis", "linear-cli");
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const launcher = fileURLToPath(new URL("../../src/bin/linear-cli.ts", import.meta.url));
  const executable = join(root, "linear-example.mjs");
  writeFileSync(executable, `#!${process.execPath}\n` + [
    'import { spawnSync } from "node:child_process";',
    'if (process.argv[2] === "recurse") {',
    `  const child = spawnSync(process.execPath, ["--import=tsx", ${JSON.stringify(launcher)}], { env: process.env, encoding: "utf8" });`,
    '  process.stdout.write(JSON.stringify({ status: child.status, stderr: child.stderr }));',
    '} else process.stdout.write(JSON.stringify({ key: process.env.LINEAR_API_KEY, status: process.env.LINEAR_EXECUTION_STATUS, expires: process.env.LINEAR_EXECUTION_EXPIRES_AT, sdk: process.env.LINEAR_EXECUTION_SDK_MODULE, active: process.env.LINEAR_CLI_LAUNCHER_ACTIVE, args: process.argv.slice(2) }));',
  ].join("\n"));
  chmodSync(executable, 0o700);
  const cliCredential = `axlcli1_${"b".repeat(43)}`;
  writeFileSync(join(directory, "config.json"), JSON.stringify({ executable, endpoint: "https://relay.example.invalid/api/linear/graphql", credential: cliCredential }), { mode: 0o600 });
  const environment: NodeJS.ProcessEnv = { ...process.env, HOME: root, LINEAR_API_KEY: credential, LINEAR_EXECUTION_STATUS: "available",
    LINEAR_EXECUTION_EXPIRES_AT: new Date(Date.now() + 60_000).toISOString(), LINEAR_EXECUTION_SDK_MODULE: "file:///example/module.js" };
  delete environment.LINEAR_CLI_LAUNCHER_ACTIVE;
  const run = (args: readonly string[]) => spawnSync(process.execPath, ["--import=tsx", launcher, ...args], { env: environment, encoding: "utf8", timeout: 10_000 });
  const normal = run(["issue", "list"]);
  assert.equal(normal.status, 0, normal.stderr);
  assert.deepEqual(JSON.parse(normal.stdout), { key: cliCredential, status: "available", active: "1", args: ["issue", "list"] });
  const recursive = run(["recurse"]);
  assert.equal(recursive.status, 0, recursive.stderr);
  const refused = JSON.parse(recursive.stdout) as { status: number; stderr: string };
  assert.equal(refused.status, 1);
  assert.match(refused.stderr, /configuration is missing or invalid/);
});
