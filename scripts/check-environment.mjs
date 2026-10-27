#!/usr/bin/env node
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Check the compiler and exact contracts dependency before using validation results. Read package
 * manifests directly from node_modules because the contracts exports map does not expose its package.json.
 */

export const REQUIRED_CONTRACTS_VERSION = "0.1.0";

function packageVersion(nodeModules, packageName) {
  const manifestPath = join(nodeModules, ...packageName.split("/"), "package.json");
  if (!existsSync(manifestPath)) return null;
  try {
    return JSON.parse(readFileSync(manifestPath, "utf8")).version ?? null;
  } catch {
    return null;
  }
}

/**
 * @param {string} cwd absolute path of the package root whose `node_modules` is being checked.
 * @param {string} requiredContractsVersion exact version `@axisosme/axis-contracts` must resolve to.
 */
export function checkEnvironment(cwd, requiredContractsVersion = REQUIRED_CONTRACTS_VERSION) {
  const nodeModules = join(cwd, "node_modules");
  const problems = [];

  const tscBinary = join(nodeModules, ".bin", "tsc");
  const typescriptVersion = packageVersion(nodeModules, "typescript");
  if (!existsSync(tscBinary) || typescriptVersion === null) {
    problems.push(`\`tsc\` does not resolve from ${nodeModules} (missing ${!existsSync(tscBinary) ? "node_modules/.bin/tsc" : "node_modules/typescript/package.json"}). This node_modules tree is missing, emptied, or a broken symlink -- run \`npm ci\` in this worktree rather than trusting any tsc output against it.`);
  }

  const contractsVersion = packageVersion(nodeModules, "@axisosme/axis-contracts");
  if (contractsVersion === null) {
    problems.push(`\`@axisosme/axis-contracts\` does not resolve from ${nodeModules}. Run \`npm ci\` in this worktree rather than trusting any tsc output against it.`);
  } else if (contractsVersion !== requiredContractsVersion) {
    problems.push(`\`@axisosme/axis-contracts\` resolves to ${contractsVersion}, not the required ${requiredContractsVersion}. A wrong-version tree produces type errors across files this branch never touched -- indistinguishable from a real regression. Run \`npm ci\` in this worktree; do not point node_modules at another worktree's tree without checking its version first.`);
  }

  if (problems.length > 0) return { ok: false, typescriptVersion, contractsVersion, problems };
  return { ok: true, typescriptVersion, contractsVersion, problems: [] };
}

function main() {
  const result = checkEnvironment(process.cwd());
  if (!result.ok) {
    process.stderr.write("check-environment: this dependency tree cannot be trusted to gate against:\n");
    for (const problem of result.problems) process.stderr.write(`  - ${problem}\n`);
    process.exitCode = 1;
    return;
  }
  process.stdout.write(`check-environment: ok (typescript ${result.typescriptVersion}, @axisosme/axis-contracts ${result.contractsVersion})\n`);
}

if (process.argv[1] !== undefined && fileURLToPath(import.meta.url) === process.argv[1]) {
  main();
}
