import { temporaryConfiguration } from "./write-smoke-configuration.mjs";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertArchiveArtifacts,
  assertInstalledArtifacts,
  assertInstalledDependencyTree,
  assertPackageManifest,
  runtimeProbeSource,
  typeProbeSource,
} from "./package-surface-guardrails.mjs";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const temporary = mkdtempSync(join(tmpdir(), "axis-package-check-"));
const staging = join(temporary, "staging");
const artifacts = join(temporary, "artifacts");
const consumer = join(temporary, "consumer");

function redactDiagnostic(value) {
  let diagnostic = value
    .replace(/https:\/\/[^\s@/]+@github\.com/giu, "https://[redacted]@github.com")
    .replace(/https:\/\/([^\s:/@]+):[^\s@/]+@/giu, "https://$1:[redacted]@")
    .replace(/(?:gh[pousr]_[A-Za-z0-9]+|github_pat_[A-Za-z0-9_]+)/gu, "[redacted]")
    .replace(/(authorization\s*:\s*(?:Bearer|Basic|Token)?\s*)\S+/giu, "$1[redacted]")
    .replace(/(?:Bearer|Token)\s+\S+/giu, "Bearer [redacted]")
    .replace(/(_authToken=)\S+/giu, "$1[redacted]");
  for (const [environmentName, token] of Object.entries(process.env)) {
    if (!/(?:TOKEN|PASSWORD|SECRET)/iu.test(environmentName)) continue;
    if (token !== undefined && token.length > 0) diagnostic = diagnostic.split(token).join("[redacted]");
  }
  return diagnostic.slice(0, 2_000);
}

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", env: process.env });
  if (result.status !== 0) {
    throw new Error(`${command} ${args[0] ?? ""} failed with exit ${String(result.status)}: ${redactDiagnostic(`${result.stdout}\n${result.stderr}`)}`);
  }
  return result.stdout;
}

try {
  mkdirSync(staging, { recursive: true });
  mkdirSync(artifacts, { recursive: true });
  mkdirSync(consumer, { recursive: true });
  for (const entry of [
    "src", "docs", "scripts", ".agents", ".claude", ".npmrc", "package.json", "package-lock.json", "README.md", "CHANGELOG.md", "LICENSE", "openapi.yaml", "tsconfig.json", "tsconfig.build.json",
  ]) cpSync(join(root, entry), join(staging, entry), { recursive: true, verbatimSymlinks: true });
  cpSync(join(root, "packages", "contracts"), join(staging, "packages", "contracts"), {
    recursive: true,
    filter: (path) => !["dist", "node_modules"].includes(path.split(/[\\/]/u).at(-1)),
  });
  assert.equal(existsSync(join(staging, "dist")), false, "clean staging must not contain dist");
  assert.equal(existsSync(join(staging, "packages", "contracts", "dist")), false, "clean contracts staging must not contain dist");
  const dependencies = join(staging, "node_modules");
  mkdirSync(join(dependencies, "@axisosme"), { recursive: true });
  for (const entry of readdirSync(join(root, "node_modules"))) {
    if (entry !== "@axisosme") symlinkSync(join(root, "node_modules", entry), join(dependencies, entry), "dir");
  }
  symlinkSync(join(staging, "packages", "contracts"), join(dependencies, "@axisosme", "axis-contracts"), "dir");
  const [contractsPack] = JSON.parse(run("npm", ["pack", "./packages/contracts", "--json", "--pack-destination", artifacts], staging));
  const contractsArchive = join(artifacts, contractsPack.filename);
  run("npm", ["pack", "--json", "--pack-destination", artifacts], staging);
  const archives = readdirSync(artifacts).filter((entry) => entry.endsWith(".tgz") && entry !== contractsPack.filename);
  assert.equal(archives.length, 1, "clean package build must emit one archive");
  const archive = join(artifacts, archives[0]);
  assertArchiveArtifacts(run("tar", ["-tzf", archive], staging).split("\n"));

  assert.equal(existsSync(join(consumer, ".npmrc")), false, "consumer must start without .npmrc");
  writeFileSync(join(consumer, "package.json"), `${JSON.stringify({ private: true, type: "module" })}\n`);
  writeFileSync(join(consumer, ".npmrc"), "registry=https://registry.npmjs.org/\n");
  run("npm", ["install", "--no-audit", "--no-fund", "--no-package-lock", "--no-save", contractsArchive, archive], consumer);

  const installed = join(consumer, "node_modules", "@axisosme", "axis");
  const installedManifest = JSON.parse(readFileSync(join(installed, "package.json"), "utf8"));
  assertPackageManifest(installedManifest);
  await assertInstalledArtifacts(installed);
  await assertInstalledDependencyTree(consumer, installed);
  const axis = join(consumer, "node_modules", ".bin", "axis");
  assert.match(run(axis, ["--help"], consumer), /team-bindings set/u);
  assert.equal(run(axis, ["--version"], consumer), `axis ${installedManifest.version}\n`);
  const axisNode = join(consumer, "node_modules", ".bin", "axis-node");
  const nodeStartup = spawnSync(axisNode, [], { cwd: consumer, encoding: "utf8", env: process.env });
  assert.deepEqual({ status: nodeStartup.status, signal: nodeStartup.signal, stdout: nodeStartup.stdout, stderr: nodeStartup.stderr }, { status: 1, signal: null, stdout: "", stderr: "axis.node.startup_failed\n" });
  assert.equal(run(process.execPath, [join(root, "scripts", "installed-node-smoke.mjs"), axisNode, consumer], consumer), "Installed axis-node command, event, readiness, and shutdown smoke passed.\n");
  // Run preflight beside a decoy package.json to prove its version comes from the installed package,
  // independently of the working directory.
  const preflight = join(consumer, "node_modules", ".bin", "axis-routing-preflight");
  const installedVersion = installedManifest.version;
  const decoy = join(temporary, "decoy-release");
  mkdirSync(decoy, { recursive: true });
  writeFileSync(join(decoy, "package.json"), `${JSON.stringify({ name: "decoy-release", version: "0.0.0-decoy" })}\n`);
  const deployment = await temporaryConfiguration(join(temporary, "preflight"), { nodes: [{ id: "node-package", port: 4100, eventPort: 4102,
    workers: [{ id: "worker-package", kind: "codex", port: 4101, workspaces: [{ organizationId: "org-package", workspaceRoot: temporary }] }] }] });
  new DatabaseSync(join(deployment.config.dataDir, "worker-worker-package.sqlite")).close();
  const preflightRun = (expected) => {
    const result = spawnSync(preflight, ["worker", "--config", deployment.filename, ...(expected === undefined ? [] : ["--expected-version", expected])], { cwd: decoy, encoding: "utf8", env: { PATH: process.env.PATH } });
    const receipt = JSON.parse(result.stdout);
    assert.equal(result.stdout.includes(temporary), false, "the preflight receipt must not carry a local path");
    return { status: result.status, receipt };
  };
  const matching = preflightRun(installedVersion);
  assert.deepEqual({ status: matching.status, name: matching.receipt.axisPackageName, version: matching.receipt.axisPackageVersion, storage: matching.receipt.storage, blockers: matching.receipt.blockers, runs: matching.receipt.counts.nonterminalWorkerRuns },
    { status: 0, name: "@axisosme/axis", version: installedVersion, storage: "verified_copy", blockers: [], runs: 0 });
  const mismatching = preflightRun("0.0.0-decoy");
  assert.deepEqual({ status: mismatching.status, version: mismatching.receipt.axisPackageVersion, blockers: mismatching.receipt.blockers }, { status: 1, version: installedVersion, blockers: ["axis_version_mismatch"] });
  const unverified = preflightRun(undefined);
  assert.deepEqual({ status: unverified.status, blockers: unverified.receipt.blockers }, { status: 1, blockers: ["axis_version_unverified"] });
  const cliSmoke = JSON.parse(run(axis, ["--output", "json", "team-bindings", "list", "--database", join(consumer, "router.sqlite"), "--organization", "org_package_smoke"], consumer));
  assert.deepEqual(cliSmoke, { schemaVersion: "axis.cli.v1", teamBindingContractVersion: "1.0.0", command: "team-bindings list", result: { organizationId: "org_package_smoke", bindings: [] } });
  const runtime = join(consumer, "runtime-check.mjs");
  writeFileSync(runtime, runtimeProbeSource());
  run(process.execPath, [runtime], consumer);
  const probe = join(consumer, "axis-consumer.ts");
  writeFileSync(probe, typeProbeSource());
  run(process.execPath, [join(root, "node_modules", "typescript", "bin", "tsc"), "--noEmit", "--strict", "--target", "ES2024", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--skipLibCheck", probe], consumer);
  process.stdout.write("Clean no-dist package archive install passed current public-surface checks.\n");
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
