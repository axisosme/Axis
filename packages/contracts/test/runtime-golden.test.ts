import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fromRuntimePackageRoot } from "../src/runtime-paths.js";
import { runRuntimeV1Golden } from "../src/runtime/golden.js";

test("runtime golden report is byte-stable and passes every trace, mutation, and admission-invariant case", async () => {
  const report = `${JSON.stringify(await runRuntimeV1Golden(), null, 2)}\n`;
  const expected = await readFile(fromRuntimePackageRoot("conformance", "runtime", "expected-report.json"), "utf8");
  assert.equal(report, expected);
  const parsed = JSON.parse(report) as { status: string; traces: { status: string }[]; mutations: { status: string }[]; admissionCases: { status: string }[] };
  assert.equal(parsed.status, "passed");
  assert.equal(parsed.traces.length, 6);
  assert.ok(parsed.traces.every((trace) => trace.status === "passed"));
  // The golden corpus must retain the self-link, cross-Task, cross-tenant, non-terminal predecessor,
  // submit/Run mismatch, first-Run bypass, and changed-predecessor cases.
  assert.equal(parsed.mutations.length, 11);
  assert.ok(parsed.mutations.every((mutation) => mutation.status === "rejected"));
  assert.equal(parsed.admissionCases.length, 6);
  assert.ok(parsed.admissionCases.every((admissionCase) => admissionCase.status === "passed"));
});
