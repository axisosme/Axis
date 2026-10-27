import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

// Every test file must be selected by its owning package script. Resolve suite globs and compare their
// files with the repository test inventory.
const sourceRoot = dirname(dirname(fileURLToPath(import.meta.url)));

// Kept in step with the `--test` globs in package.json by the assertion below,
// which reads those scripts rather than trusting this list.
const SUITE_ROOTS = ["test/relay", "test/node", "test/e2e"];

function filesUnder(directory) {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...filesUnder(path));
    else if (entry.isFile()) files.push(path);
  }
  return files;
}

const testFiles = filesUnder(join(sourceRoot, "test"))
  .filter((path) => path.endsWith(".test.ts"))
  .map((path) => relative(sourceRoot, path).replaceAll("\\", "/"));

// A scan that found nothing would satisfy every assertion below.
assert.ok(testFiles.length > 0, "no test files found under test/ -- this check would pass vacuously");

const unreachable = testFiles.filter((path) => !SUITE_ROOTS.some((root) => path.startsWith(`${root}/`))).sort();
assert.deepEqual(
  unreachable,
  [],
  `these test files are under no suite root (${SUITE_ROOTS.join(", ")}), so no package.json glob runs them: ${unreachable.join(", ")}`,
);

// And the roots this file names are the roots the scripts actually glob, so the
// two cannot drift apart.
const pkg = JSON.parse(await import("node:fs").then((fs) => fs.promises.readFile(join(sourceRoot, "package.json"), "utf8")));
const globbed = new Set();
for (const command of Object.values(pkg.scripts ?? {})) {
  for (const match of command.matchAll(/"(test\/[\w-]+)\/\*\*\/\*\.test\.ts"/g)) globbed.add(match[1]);
}
assert.deepEqual([...globbed].sort(), [...SUITE_ROOTS].sort(), "the suite roots checked here and the suite roots package.json globs have diverged");

// A sharded suite is only complete if every shard of it actually runs. A shard
// left out of the CI matrix, or a set that skips an index, runs none of its
// files while the run still reports success -- the same silent-under-run shape
// as a truncating test runner, and just as invisible.
const shardsBySuite = new Map();
for (const [name, command] of Object.entries(pkg.scripts ?? {})) {
  for (const match of command.matchAll(/--test-shard=(\d+)\/(\d+)/g)) {
    const total = Number(match[2]);
    const key = `${name.replace(/:\d+of\d+$/u, "")}/${total}`;
    const seen = shardsBySuite.get(key) ?? { total, indexes: new Set(), scripts: [] };
    seen.indexes.add(Number(match[1]));
    seen.scripts.push(name);
    shardsBySuite.set(key, seen);
  }
}
const workflow = readFileSync(join(sourceRoot, ".github/workflows/ci.yml"), "utf8");
for (const [key, { total, indexes, scripts }] of shardsBySuite) {
  const expected = Array.from({ length: total }, (_, index) => index + 1);
  assert.deepEqual([...indexes].sort((a, b) => a - b), expected, `${key}: shard scripts do not cover 1..${total} -- the missing shard's files would never run`);
  for (const name of scripts) {
    assert.ok(workflow.includes(`"${name}"`), `package.json defines ${name} but the CI matrix never runs it, so its shard of the suite runs nowhere`);
  }
}

const shardNote = shardsBySuite.size === 0 ? "" : `; ${[...shardsBySuite].map(([key, v]) => `${key} sharded ${[...v.indexes].sort().join("+")} of ${v.total}, all in the CI matrix`).join(", ")}`;
process.stdout.write(`Test-file registration guardrail passed (${testFiles.length} files, all under ${SUITE_ROOTS.join(" / ")}; each root is globbed by a package.json script${shardNote}).\n`);
