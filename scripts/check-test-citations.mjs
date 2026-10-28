import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync, realpathSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Validate that test-file paths cited as evidence resolve. Read UTF-8 text directly so binary-detection
// heuristics cannot hide citations. Path existence does not establish that the cited test proves the
// surrounding claim.
const sourceRoot = dirname(dirname(fileURLToPath(import.meta.url)));

// Missing test citations in these source and documentation roots fail the check.
const ENFORCED_ROOTS = ["docs", "scripts", "src", ".agents/skills"];

// Report unresolved test-root citations separately from enforced source and documentation citations.
const REPORTED_ROOTS = ["test"];

const TEXT_SUFFIXES = [".ts", ".mts", ".mjs", ".js", ".md", ".json", ".yml", ".yaml"];
const CITATION = /test\/[A-Za-z0-9_./-]*\.test\.ts/g;

function filesUnder(directory) {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...filesUnder(path));
    else if (entry.isFile()) files.push(path);
  }
  return files;
}

function scan(roots) {
  const dead = [];
  let citations = 0;
  let files = 0;
  const seen = new Set();
  for (const root of roots) {
    for (const absolute of filesUnder(join(sourceRoot, root))) {
      const path = relative(sourceRoot, absolute).replaceAll("\\", "/");
      if (!TEXT_SUFFIXES.some((suffix) => path.endsWith(suffix)) || seen.has(path)) continue;
      seen.add(path);
      files += 1;
      for (const [index, line] of readFileSync(absolute, "utf8").split("\n").entries()) {
        for (const match of line.matchAll(CITATION)) {
          citations += 1;
          if (!existsSync(join(sourceRoot, match[0]))) dead.push(`${path}:${index + 1} cites ${match[0]}`);
        }
      }
    }
  }
  return { dead: dead.sort(), citations, files };
}

const enforced = scan(ENFORCED_ROOTS);

// Check local Markdown links and section fragments in repository agent instructions
// alongside evidence citations, including the deployment entry points.
const markdownFiles = ["README.md", "AGENTS.md", ...filesUnder(join(sourceRoot, ".agents/skills"))
  .filter((path) => path.endsWith(".md"))];
let markdownLinks = 0;
for (const filename of markdownFiles) {
  const absolute = realpathSync(resolve(sourceRoot, filename));
  const content = readFileSync(absolute, "utf8");
  for (const match of content.matchAll(/\[[^\]\n]+\]\(([^\s)]+)\)/gu)) {
    const target = match[1];
    if (/^[a-z][a-z0-9+.-]*:/iu.test(target)) continue;
    markdownLinks += 1;
    const [path, fragment] = target.split("#");
    const destination = path === "" ? absolute : resolve(dirname(absolute), decodeURIComponent(path));
    assert.ok(existsSync(destination), `${filename} links to missing ${target}`);
    if (!fragment) continue;
    const headings = readFileSync(destination, "utf8").split("\n")
      .filter((line) => /^#{1,6} /u.test(line))
      .map((line) => line.replace(/^#{1,6} /u, "").trim().toLowerCase()
        .replace(/[^\p{L}\p{N}_\-\s]/gu, "").replace(/\s/gu, "-"));
    assert.ok(headings.includes(decodeURIComponent(fragment)), `${filename} links to missing section ${target}`);
  }
}
assert.ok(markdownLinks > 0, "no local Markdown links found in agent instructions or entry points");
process.stdout.write(`Agent documentation link guardrail passed (${markdownLinks} local links and section targets).\n`);

// A scan that read nothing, or that stopped matching the citation shape, would
// satisfy the assertion below without checking anything -- the exact vacuity
// this file exists to catch in prose.
assert.ok(enforced.files > 0, `no files to scan under ${ENFORCED_ROOTS.join(", ")} -- this check would pass vacuously`);
assert.ok(enforced.citations > 0, "no test-file citations found at all -- the citation pattern has stopped matching");

assert.deepEqual(
  enforced.dead,
  [],
  `these sentences cite a test file that does not exist, so a reader is told a guarantee is checked when it is not:\n  ${enforced.dead.join("\n  ")}`,
);

// Exclude enforced roots from the reported remainder to avoid double counting.
const enforcedPrefixes = ENFORCED_ROOTS.map((root) => `${root}/`);
const remainder = scan(REPORTED_ROOTS).dead.filter((entry) => !enforcedPrefixes.some((prefix) => entry.startsWith(prefix)));

process.stdout.write(`Test-citation guardrail passed (${enforced.citations} citations across ${enforced.files} files under ${ENFORCED_ROOTS.join(" / ")}; every cited path resolves).\n`);
if (remainder.length > 0) {
  process.stdout.write(`  ${remainder.length} dead citations remain outside the enforced roots, left for a follow-up:\n`);
  const byFile = new Map();
  for (const entry of remainder) {
    const file = entry.slice(0, entry.indexOf(":"));
    byFile.set(file, (byFile.get(file) ?? 0) + 1);
  }
  for (const [file, count] of [...byFile].sort((left, right) => right[1] - left[1])) process.stdout.write(`    ${String(count).padStart(3)}  ${file}\n`);
}
