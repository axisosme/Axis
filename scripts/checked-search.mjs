#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { verifyRefResolves } from "./git-ref-support.mjs";

/**
 * Read searched content directly as UTF-8, including NUL-containing files, so binary-detection heuristics
 * cannot turn real matches into an apparent absence.
 */
export function readTargetText(target) {
  if ("file" in target) {
    return readFileSync(target.file, "utf8");
  }
  const { ref, path, cwd = process.cwd() } = target;
  // Reject an unresolved ref before reading its content; a failed upstream command must not become an empty search result.
  try {
    verifyRefResolves(cwd, ref);
  } catch (error) {
    throw new Error(`checked-search: ${error instanceof Error ? error.message : String(error)} -- refusing to report a search result against it.`);
  }
  // `git cat-file -e <ref>:<path>` alone exits 0 for a DIRECTORY too (a tree
  // object exists there), and `git show <ref>:<path>` on one returns a tree
  // listing rather than throwing -- a caller who passed a directory by
  // mistake would silently get a "proven" search result against that
  // listing instead of the file they meant. Requiring the object's type be
  // exactly `blob` is what actually proves this path names a file.
  let objectType;
  try {
    objectType = execFileSync("git", ["cat-file", "-t", `${ref}:${path}`], { cwd, encoding: "utf8" }).trim();
  } catch {
    objectType = "";
  }
  if (objectType !== "blob") {
    throw new Error(`checked-search: path ${JSON.stringify(path)} does not exist at ref ${JSON.stringify(ref)} (${objectType === "" ? "no such path" : `found a ${objectType}, not a file`}) -- refusing to report a search result against it.`);
  }
  return execFileSync("git", ["show", `${ref}:${path}`], { cwd, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
}

/**
 * Non-overlapping literal-substring occurrence count. Never a regex: a caller who wants a fixed string
 * counted should never have to think about escaping it.
 */
export function countOccurrences(text, needle) {
  if (needle.length === 0) throw new Error("checked-search: the search string must not be empty.");
  let count = 0;
  let index = text.indexOf(needle);
  while (index !== -1) {
    count += 1;
    index = text.indexOf(needle, index + needle.length);
  }
  return count;
}

/**
 * Report literal occurrence counts. A zero requires a matching known anchor from the same read; without
 * that anchor, absence is unverified and the search throws.
 */
export function checkedSearch({ target, pattern, anchor }) {
  const text = readTargetText(target);
  const patternCount = countOccurrences(text, pattern);
  const anchorCount = countOccurrences(text, anchor);
  if (patternCount === 0 && anchorCount === 0) {
    throw new Error(
      `checked-search: self-proof failed -- anchor ${JSON.stringify(anchor)} also matched 0 times in this target, so the 0 for pattern ${JSON.stringify(pattern)} is UNPROVEN, not a confirmed absence. ` +
      "Report this as \"unverified\", not as \"not found\"; do not conclude the pattern is absent from this evidence alone.",
    );
  }
  return { patternCount, anchorCount, proven: true };
}

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (!flag.startsWith("--")) throw new Error(`checked-search: unexpected argument ${JSON.stringify(flag)}.`);
    const name = flag.slice(2);
    const value = argv[index + 1];
    if (value === undefined) throw new Error(`checked-search: --${name} requires a value.`);
    args[name] = value;
    index += 1;
  }
  return args;
}

function main(argv) {
  const args = parseArgs(argv);
  if (args.pattern === undefined || args.anchor === undefined) {
    throw new Error("usage: checked-search.mjs --pattern <text> --anchor <text> (--file <path> | --ref <ref> --path <path>)");
  }
  const target = args.file !== undefined ? { file: args.file } : { ref: args.ref, path: args.path };
  if (target.file === undefined && (target.ref === undefined || target.path === undefined)) {
    throw new Error("usage: checked-search.mjs --pattern <text> --anchor <text> (--file <path> | --ref <ref> --path <path>)");
  }
  const result = checkedSearch({ target, pattern: args.pattern, anchor: args.anchor });
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (process.argv[1] !== undefined && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
