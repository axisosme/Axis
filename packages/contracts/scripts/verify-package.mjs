import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { runRuntimeV1Golden } from "../dist/src/runtime/golden.js";

const execFileAsync = promisify(execFile);
const root = path.resolve(fileURLToPath(new URL("../", import.meta.url)));
const contractVersion = "1.0.0";
const packageVersion = "1.0.0";
const resourceRoots = ["schemas/runtime", "schemas/agent-activity", "schemas/linear-execution", "fixtures/agent-activity", "conformance/runtime", "conformance/agent-activity", "docs"];
const codeSubpaths = ["./runtime", "./agent-activity", "./linear-execution"];
const resourceSubpaths = ["./schemas/*", "./fixtures/*", "./conformance/*"];

async function filesBelow(directory) {
  const absolute = path.join(root, directory);
  const entries = await readdir(absolute, { withFileTypes: true });
  const nested = await Promise.all(entries.sort((a, b) => a.name.localeCompare(b.name)).map(async (entry) => {
    const relative = path.posix.join(directory, entry.name);
    return entry.isDirectory() ? filesBelow(relative) : [relative];
  }));
  return nested.flat();
}

function sorted(values) { return [...values].sort((a, b) => a.localeCompare(b)); }

async function pack(args) {
  const { stdout } = await execFileAsync("npm", ["pack", "--json", "--ignore-scripts", ...args], { cwd: root, maxBuffer: 8 * 1024 * 1024 });
  const result = JSON.parse(stdout);
  assert.ok(Array.isArray(result) && result.length === 1, "npm pack must return exactly one record");
  return result[0];
}

function expectedCompiledFiles() {
  const extensions = ["js", "d.ts", "js.map"];
  const modules = {
    runtime: ["golden", "index", "registry", "types.generated", "validate"],
    "agent-activity": ["index", "registry", "types.generated", "validate"],
    "linear-execution": ["index"],
    "": ["runtime-paths", "registry-cache"],
  };
  return Object.entries(modules).flatMap(([directory, names]) => names.flatMap((name) =>
    extensions.map((extension) => path.posix.join("dist/src", directory, `${name}.${extension}`))));
}

const packageJson = JSON.parse(await readFile(path.join(root, "package.json"), "utf8"));
assert.equal(packageJson.version, packageVersion);
assert.equal(packageJson.bin, undefined);
assert.deepEqual(sorted(Object.keys(packageJson.exports)), sorted([...codeSubpaths, ...resourceSubpaths]));
for (const subpath of codeSubpaths) {
  const family = subpath.slice(2);
  assert.deepEqual(packageJson.exports[subpath], { types: `./dist/src/${family}/index.d.ts`, import: `./dist/src/${family}/index.js` });
}
for (const subpath of resourceSubpaths) assert.equal(packageJson.exports[subpath], subpath);

const relativeReport = "conformance/runtime/expected-report.json";
const reportBytes = await readFile(path.join(root, relativeReport));
assert.equal(`${JSON.stringify(await runRuntimeV1Golden(), null, 2)}\n`, reportBytes.toString("utf8"), "the golden report must match the current corpus");
const reportHash = createHash("sha256").update(reportBytes).digest("hex");
const resources = (await Promise.all(resourceRoots.map(filesBelow))).flat();
const expectedFiles = sorted(["package.json", "README.md", "CHANGELOG.md", "LICENSE", ...expectedCompiledFiles(), ...resources]);
const dryRun = await pack(["--dry-run"]);
assert.deepEqual(sorted(dryRun.files.map((file) => file.path)), expectedFiles, "npm pack dry-run must match the package allowlist");

async function verifyInstalled(archive) {
  const temp = await mkdtemp(path.join(os.tmpdir(), "axis-contracts-installed-"));
  try {
    await writeFile(path.join(temp, "package.json"), "{\"private\":true,\"type\":\"module\"}\n");
    await execFileAsync("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", archive], { cwd: temp, maxBuffer: 8 * 1024 * 1024 });
    const installed = path.join(temp, "node_modules", "@axisosme", "axis-contracts");
    const load = (family) => import(pathToFileURL(path.join(installed, "dist/src", family, "index.js")).href);
    const runtime = await load("runtime");
    const activity = await load("agent-activity");
    assert.equal(runtime.runtimeContractVersion, contractVersion);
    const report = await runtime.runRuntimeV1Golden();
    assert.equal(`${JSON.stringify(report, null, 2)}\n`, await readFile(path.join(installed, relativeReport), "utf8"));
    assert.equal(report.status, "passed");
    assert.equal(activity.agentActivityContractVersion, contractVersion);
    assert.equal(/codex/iu.test(activity.agentActivityFallbackResponseBody), false);
    for (const level of ["run", "task", "executor"]) {
      for (const kind of ["waiting", "response"]) {
        const fixture = JSON.parse(await readFile(path.join(installed, `fixtures/agent-activity/valid/${level}-${kind}.json`), "utf8"));
        const validate = kind === "waiting" ? activity.validateAgentWaitingDelivery : activity.validateAgentResponseDelivery;
        const result = await validate(fixture);
        assert.equal(result.valid, true, `installed ${level} ${kind} delivery: ${JSON.stringify(result.diagnostics)}`);
      }
    }
    const conformance = await execFileAsync(process.execPath, [path.join(installed, "conformance/agent-activity/consumer-conformance.mjs")]);
    assert.equal(conformance.stdout, "Installed Agent Activity v1 waiting-delivery conformance passed.\nInstalled Agent Activity v1 response-delivery conformance passed.\n");
    await writeFile(path.join(temp, "public-exports-smoke.mjs"), `
      import assert from "node:assert/strict";
      import { runtimeContractVersion, validateRuntimeBoundaryMessage } from "@axisosme/axis-contracts/runtime";
      import { agentActivityContractVersion, validateAgentWaitingDelivery, validateAgentResponseDelivery } from "@axisosme/axis-contracts/agent-activity";
      import { linearExecutionContractVersion, validateLinearExecutionCredentialRequest, validateLinearExecutionCredentialResponse } from "@axisosme/axis-contracts/linear-execution";
      assert.equal(runtimeContractVersion, "1.0.0");
      assert.equal(agentActivityContractVersion, "1.0.0");
      assert.equal(linearExecutionContractVersion, "1.0.0");
      for (const validate of [validateRuntimeBoundaryMessage, validateAgentWaitingDelivery, validateAgentResponseDelivery]) assert.equal(typeof validate, "function");
      const request = validateLinearExecutionCredentialRequest({ contractVersion: "1.0.0", requestId: "installed-check", operation: "issue", execution: { taskId: "task", attemptId: "attempt", runId: "run", organizationId: "org" } });
      const response = { ...request, status: "issued", credentialId: "credential", credential: "axlea1_" + "a".repeat(43), expiresAt: "2026-09-11T00:05:00.000Z" };
      delete response.operation;
      const now = Date.parse("2026-09-11T00:00:00.000Z");
      assert.equal(validateLinearExecutionCredentialResponse(response, request, now).status, "issued");
      assert.throws(() => validateLinearExecutionCredentialResponse({ ...response, execution: { ...response.execution, runId: "other" } }, request, now));
      assert.throws(() => validateLinearExecutionCredentialResponse({ ...response, credential: "lin_api_personal" }, request, now));
      await assert.rejects(import("@axisosme/axis-contracts"), { code: "ERR_PACKAGE_PATH_NOT_EXPORTED" });
    `);
    await execFileAsync(process.execPath, [path.join(temp, "public-exports-smoke.mjs")]);
  } finally {
    await rm(temp, { recursive: true, force: true });
  }
}

if (process.env.AXIS_CONTRACTS_PACKAGE_DRY_RUN_ONLY !== "1") {
  const archive = process.env.AXIS_CONTRACTS_PACKAGE_TARBALL;
  if (archive) await verifyInstalled(archive);
  else {
    const temp = await mkdtemp(path.join(os.tmpdir(), "axis-contracts-pack-"));
    try {
      const packed = await pack(["--pack-destination", temp]);
      await verifyInstalled(path.join(temp, packed.filename));
    } finally { await rm(temp, { recursive: true, force: true }); }
  }
}
process.stdout.write(`Package ${packageVersion} allowlist verified; ${contractVersion} golden ${reportBytes.byteLength} bytes sha256:${reportHash}.\n`);
