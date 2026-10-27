import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { access, cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import {
  assertInstalledArtifacts,
  assertInstalledDependencyTree,
  assertPackageManifest,
  runtimeProbeSource,
  typeProbeSource,
} from "./package-surface-guardrails.mjs";

const execFileAsync = promisify(execFile);
const sourceRoot = process.cwd();

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

async function run(file, args, cwd, timeout = 180_000, extraEnv = {}) {
  try {
    const { stdout } = await execFileAsync(file, args, {
      cwd, encoding: "utf8", timeout, maxBuffer: 10 * 1024 * 1024, windowsHide: true,
      env: {
        ...process.env,
        npm_config_audit: "false", npm_config_fund: "false", npm_config_legacy_peer_deps: "false", npm_config_prefer_offline: "false", npm_config_offline: "false",
        GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_COUNT: "0", GIT_TERMINAL_PROMPT: "0", GIT_SSH_COMMAND: "/usr/bin/false",
        ...extraEnv,
      },
    });
    return stdout.trim();
  } catch (error) {
    const failed = error instanceof Error ? error : new Error(String(error));
    const details = error !== null && typeof error === "object" ? error : {};
    const stdout = "stdout" in details && typeof details.stdout === "string" ? details.stdout : "";
    const stderr = "stderr" in details && typeof details.stderr === "string" ? details.stderr : "";
    throw new Error(`${file} ${args[0] ?? ""} failed: ${redactDiagnostic(`${failed.message}\n${stdout}\n${stderr}`)}`);
  }
}

async function createPackageRepository(target) {
  const files = (await run("git", ["ls-files", "--cached", "--others", "--exclude-standard", "-z"], sourceRoot)).split("\0").filter(Boolean);
  for (const file of files) {
    const source = join(sourceRoot, file); const destination = join(target, file);
    try { await access(source); } catch (error) { if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT") continue; throw error; }
    await mkdir(dirname(destination), { recursive: true }); await cp(source, destination);
  }
  await run("git", ["init", "--initial-branch=main"], target);
  await run("git", ["config", "user.name", "Axis Test"], target); await run("git", ["config", "user.email", "axis-test@example.invalid"], target);
  await run("git", ["config", "commit.gpgsign", "false"], target); await run("git", ["add", "."], target); await run("git", ["commit", "-m", "package fixture"], target);
  return run("git", ["rev-parse", "HEAD"], target);
}

const workspace = await mkdtemp(join(tmpdir(), "axis-git-install-"));
const repository = join(workspace, "package-repository");
const consumer = join(workspace, "consumer");
const cache = join(workspace, "npm-cache");
const artifacts = join(workspace, "artifacts");
try {
  await mkdir(repository); await mkdir(consumer); await mkdir(cache); await mkdir(artifacts);
  const [contractsPack] = JSON.parse(await run("npm", ["pack", "./packages/contracts", "--json", "--pack-destination", artifacts], sourceRoot));
  const contractsArchive = join(artifacts, contractsPack.filename);
  const sourceManifest = await readFile(join(sourceRoot, "package.json"), "utf8");
  const commit = await createPackageRepository(repository);
  await assert.rejects(access(join(repository, "dist")), { code: "ENOENT" });
  assert.equal(await readFile(join(repository, "package.json"), "utf8"), sourceManifest, "cold fixture must retain the unmodified Axis manifest");
  const repositoryNpmrc = await readFile(join(repository, ".npmrc"), "utf8");
  assert.equal(repositoryNpmrc, "registry=https://registry.npmjs.org/\n");
  await assert.rejects(access(join(consumer, ".npmrc")), { code: "ENOENT" });
  const packageUrl = `git+${pathToFileURL(repository).href}#${commit}`;
  assert.match(packageUrl, /^git\+file:/u, "cold dependency must use a pinned local Git commit");
  await writeFile(join(consumer, "package.json"), `${JSON.stringify({ name: "axis-git-consumer", private: true, type: "module", dependencies: { "@axisosme/axis": packageUrl, "@axisosme/axis-contracts": `file:${contractsArchive}` } }, null, 2)}\n`);
  await writeFile(join(consumer, ".npmrc"), "registry=https://registry.npmjs.org/\n");
  await run("npm", ["install", "--package-lock=false"], consumer, 180_000, { npm_config_cache: cache });

  const installed = join(consumer, "node_modules", "@axisosme", "axis");
  assertPackageManifest(JSON.parse(await readFile(join(installed, "package.json"), "utf8")));
  await assertInstalledArtifacts(installed);
  await assertInstalledDependencyTree(consumer, installed);
  const axis = join(consumer, "node_modules", ".bin", "axis");
  assert.match(await run(axis, ["--help"], consumer), /team-bindings set/u);
  assert.equal(await run(axis, ["--version"], consumer), `axis ${JSON.parse(sourceManifest).version}`);
  const axisNode = join(consumer, "node_modules", ".bin", "axis-node");
  await assert.rejects(execFileAsync(axisNode, [], { cwd: consumer, encoding: "utf8", env: process.env }), (error) => {
    assert.equal(error.code, 1);
    assert.equal(error.stdout, "");
    assert.equal(error.stderr, "axis.node.startup_failed\n");
    return true;
  });
  assert.equal(await run(process.execPath, [join(sourceRoot, "scripts", "installed-node-smoke.mjs"), axisNode, consumer], consumer), "Installed axis-node command, event, readiness, and shutdown smoke passed.");
  const cliSmoke = JSON.parse(await run(axis, ["--output", "json", "team-bindings", "list", "--database", join(consumer, "router.sqlite"), "--organization", "org_git_smoke"], consumer));
  assert.deepEqual(cliSmoke, { schemaVersion: "axis.cli.v1", teamBindingContractVersion: "1.0.0", command: "team-bindings list", result: { organizationId: "org_git_smoke", bindings: [] } });
  const runtime = join(consumer, "runtime-check.mjs");
  await writeFile(runtime, runtimeProbeSource());
  await run(process.execPath, [runtime], consumer);
  const probe = join(consumer, "axis-consumer.ts");
  await writeFile(probe, typeProbeSource());
  await run(process.execPath, [join(sourceRoot, "node_modules", "typescript", "bin", "tsc"), "--noEmit", "--strict", "--target", "ES2024", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--skipLibCheck", probe], consumer);
  process.stdout.write("Cold pinned no-dist local Git install passed current public-surface checks.\n");
} finally {
  await rm(workspace, { recursive: true, force: true });
}
