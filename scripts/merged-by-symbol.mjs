#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { verifyRefResolves } from "./git-ref-support.mjs";

/**
 * Check whether a distinctive symbol appears in the target tree. Squash merges rewrite commits, so
 * ancestry alone cannot establish that content landed. Return matching file/count evidence rather than
 * claiming certainty from symbol presence.
 */

/**
 * `git grep -c`'s own exit code convention: 0 = at least one match, 1 = no matches, anything else = a real
 * error (bad ref, git not usable, etc.).
 */
function gitGrepCount(cwd, ref, symbol) {
  let stdout;
  try {
    stdout = execFileSync("git", ["grep", "--fixed-strings", "-c", "-e", symbol, ref], { cwd, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
  } catch (error) {
    const status = error !== null && typeof error === "object" && "status" in error ? error.status : undefined;
    if (status === 1) return { totalOccurrences: 0, files: [] };
    throw new Error(`merged-by-symbol: \`git grep\` failed against ref ${JSON.stringify(ref)}: ${error instanceof Error ? error.message : String(error)}`);
  }
  const files = [];
  let totalOccurrences = 0;
  for (const line of stdout.split("\n").filter((line) => line.length > 0)) {
    // `git grep -c <ref>` prints `<ref>:<path>:<count>` per matching file.
    const separator = line.indexOf(":");
    const rest = line.slice(separator + 1);
    const pathSeparator = rest.lastIndexOf(":");
    const path = rest.slice(0, pathSeparator);
    const count = Number(rest.slice(pathSeparator + 1));
    files.push(path);
    totalOccurrences += count;
  }
  return { totalOccurrences, files };
}

function verifyRef(cwd, ref) {
  try {
    verifyRefResolves(cwd, ref);
  } catch (error) {
    throw new Error(`merged-by-symbol: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Compares `symbol`'s occurrence in `sourceRef`'s tree against `targetRef`'s
 * tree. Throws if `symbol` does not even appear at `sourceRef` -- comparing
 * a mistyped or never-introduced symbol against `targetRef` would produce a
 * meaningless "absent from target" that looks identical to a real absence.
 *
 * Returns a verdict, never a bare boolean: a distinctive symbol's presence
 * in `targetRef` is strong evidence of having merged, but this tool has no
 * way to rule out an unrelated reintroduction of the same name, so it
 * reports the counts and files it found rather than asserting certainty it
 * cannot back up.
 */
export function mergedBySymbol({ symbol, sourceRef, targetRef, cwd = process.cwd() }) {
  verifyRef(cwd, sourceRef);
  verifyRef(cwd, targetRef);
  const source = gitGrepCount(cwd, sourceRef, symbol);
  if (source.totalOccurrences === 0) {
    throw new Error(`merged-by-symbol: symbol ${JSON.stringify(symbol)} does not appear at sourceRef ${JSON.stringify(sourceRef)} either -- nothing to compare. Check the symbol name.`);
  }
  const target = gitGrepCount(cwd, targetRef, symbol);
  return {
    symbol,
    sourceRef, sourceOccurrences: source.totalOccurrences, sourceFiles: source.files,
    targetRef, targetOccurrences: target.totalOccurrences, targetFiles: target.files,
    verdict: target.totalOccurrences > 0 ? "present_in_target" : "absent_from_target",
  };
}

function main(argv) {
  const [symbol, sourceRef, targetRef] = argv;
  if (symbol === undefined || sourceRef === undefined || targetRef === undefined) {
    throw new Error("usage: merged-by-symbol.mjs <symbol> <sourceRef> <targetRef>");
  }
  const result = mergedBySymbol({ symbol, sourceRef, targetRef });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  if (result.verdict === "absent_from_target") {
    process.stdout.write(`ABSENT from ${targetRef}: ${symbol} appears ${result.sourceOccurrences}x at ${sourceRef} (${result.sourceFiles.join(", ")}) but 0x at ${targetRef} -- evidence consistent with NOT merged, for a distinctive symbol name.\n`);
  } else {
    process.stdout.write(`PRESENT in ${targetRef}: ${symbol} appears ${result.targetOccurrences}x (${result.targetFiles.join(", ")}).\n`);
  }
}

if (process.argv[1] !== undefined && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
