/**
 * Exercise final-answer redaction and receipt validation with the real Worker service
 * and SQLite journal and a scripted agent model. Local Markdown images become safe
 * text; rejected response receipts retain a diagnostic while the user receives the
 * fallback. This suite does not access Linear or verify its image-ingestion behavior.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { agentActivityFallbackResponseBody } from "@axisosme/axis-contracts/agent-activity";
import {
  SqliteWorkerCommandJournal,
  WorkerCommandService,
  type RouterRunEventSink,
  type RouterRunLifecycleEnvelope,
} from "../../src/index.js";
import { normalizeExecutorTerminalResponse } from "../../src/shared/worker-executor.js";
import { WORKER_COMMAND_VERSION } from "../../src/shared/router-worker.js";
import { buildWorkerBoundStartRequest } from "../support/router-node-fixtures.js";
import { ScriptedExecutor, scriptedExecutorRegistration } from "../support/scripted-executor.js";
import { executionTargetFor } from "../support/worker-execution-target.js";

/**
 * Use the contract's explicit SYNTHETIC_SECRET_MARKER branch to trigger unsafe_content
 * at /content/body. A synthetic marker keeps refusal coverage independent of credential
 * heuristics that may also match ordinary technical text.
 */
const secretMarker = "SYNTHETIC_SECRET_MARKER";

/**
 * `workerId`, `executorId` and `organizationId` are the fixture start's own
 * values, not this file's inventions: `parseCommand` refuses a command whose
 * `sessionBinding` names a different Worker or executor than its execution
 * target, and workspace resolution keys on the task's Linear organization.
 */
const configuration = {
  bearerToken: "worker-projection-token",
  workerId: "worker-1",
  runtimeKind: "scripted",
  executorId: "executor-1",
  nodeId: "node-projection-1",
  methodRef: { version: "v1", method: "scripted.execute" },
  workspaces: [{ organizationId: "org-1", workspaceRoot: tmpdir() }],
} as const;

/** Collects every envelope the Worker hands Router, in order. */
class CollectingSink implements RouterRunEventSink {
  readonly events: RouterRunLifecycleEnvelope[] = [];
  async emit(event: RouterRunLifecycleEnvelope): Promise<void> { this.events.push(event); }
}

async function waitFor(what: string, ready: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!ready() && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(ready(), `timed out after ${timeoutMs}ms waiting for ${what}`);
}

/**
 * Runs one Linear-sourced start to its terminal with `body` as the agent's
 * answer, and hands back the Run's durable state.
 *
 * The journal is file-backed rather than `:memory:` so the refusal test can
 * read the stored column with a second, read-only handle.
 */
async function runTerminalResponse(context: { after(fn: () => unknown): void }, body: string) {
  const request = await buildWorkerBoundStartRequest("command-projection-1");
  const start = request.command.start;
  const executor = new ScriptedExecutor();
  const directory = mkdtempSync(join(tmpdir(), "axis-response-projection-projection-"));
  const journalPath = join(directory, "worker.sqlite");
  const journal = new SqliteWorkerCommandJournal(journalPath);
  const sink = new CollectingSink();
  const service = new WorkerCommandService({
    configuration,
    registrations: [scriptedExecutorRegistration({ executorId: configuration.executorId, adapter: executor })],
    eventSink: sink,
    journal,
  });
  context.after(() => { service.close(); executor.closeAll(); journal.close(); rmSync(directory, { recursive: true, force: true }); });

  await service.execute({
    contractVersion: WORKER_COMMAND_VERSION,
    executionTarget: executionTargetFor(configuration),
    commandId: "command-projection-1",
    command: "start",
    start,
    instruction: "Summarize the ads report.",
    workspacePolicyRef: null,
  });
  const binding = executor.lastBinding;
  assert.ok(binding !== undefined, "the scripted executor must have been started");
  executor.pushSucceeded(binding, { sourceEventId: "projection-source-1", projectionStatus: "projected", body, truncated: false, truncations: [] });
  executor.close(binding);

  const runId = start.identity.run.runId;
  // Read off the sink rather than the journal: this is the envelope Router --
  // and through it, Linear -- actually receives, so an assertion on it cannot
  // pass on a durable row the delivery path never picked up.
  const delivered = () => sink.events.find((event): event is Extract<RouterRunLifecycleEnvelope, { readonly deliveryType: "run_agent_response" }> =>
    "deliveryType" in event && event.deliveryType === "run_agent_response");
  await waitFor("the Worker to deliver a response activity to Router", () => delivered() !== undefined);
  return { runId, journal, journalPath, activity: delivered()!.agentActivity };
}

test("a markdown image pointing at a local file becomes text that keeps the redacted path", () => {
  // A relative sandbox image path can appear directly in an answer without being
  // observed by a tool. The terminal response normalizer must handle it at the
  // projection boundary.
  assert.equal(
    normalizeExecutorTerminalResponse("Posted and confirmed.\n\n![private draft](tmp/private_draft_image.png)\n\nDone.", undefined),
    "Posted and confirmed.\n\n[Axis could not embed this local image: tmp/private_draft_image.png]\n\nDone.",
  );

  // Absolute forms reach the rule already redacted, which is the whole reason
  // it runs after the path rewrites rather than before: one rule, and the path
  // left in the body is the redacted one.
  assert.equal(
    normalizeExecutorTerminalResponse(`Here: ![shot](${homedir()}/Pictures/a.png)`, undefined),
    "Here: [Axis could not embed this local image: <home>/Pictures/a.png]",
  );
  assert.equal(
    normalizeExecutorTerminalResponse("Here: ![shot](/tmp/axis/a.png)", undefined),
    "Here: [Axis could not embed this local image: <temporary-path>]",
    "the temporary-path placeholder must survive intact -- it is shaped like a pointy-bracket destination",
  );
  assert.equal(
    normalizeExecutorTerminalResponse("Here: ![shot](/some/workspace/out/a.png)", "/some/workspace"),
    "Here: [Axis could not embed this local image: out/a.png]",
  );

  // Non-local destinations remain byte-for-byte intact. HTTP and HTTPS use both
  // bare and bracketed CommonMark destinations; unsupported schemes are also
  // non-local and must not be described as local files.
  for (const remote of [
    "![chart](https://example.com/chart.png)",
    "![chart](http://example.com/chart.png)",
    "![chart](//cdn.example.com/chart.png)",
    "![dot](data:image/png;base64,iVBORw0KGgo=)",
    "![chart](<https://example.com/chart.png>)",
    "![chart](<http://example.com/chart.png>)",
    "![dot](<data:image/png;base64,iVBORw0KGgo=>)",
    "![a](mailto:synthetic-user@example.invalid)",
    "![a](ftp://example.com/a.png)",
    "![a](attachment://abc123)",
    "![a](#section)",
    "![nothing]()",
    "![nothing](<>)",
  ]) {
    assert.equal(normalizeExecutorTerminalResponse(remote, undefined), remote, `a non-local or empty image destination must not be rewritten: ${remote}`);
  }

  // A `file:` URL does name a file on this machine, so it is rewritten like a bare path.
  assert.equal(
    normalizeExecutorTerminalResponse("Here: ![shot](file:///opt/axis/a.png)", undefined),
    "Here: [Axis could not embed this local image: file:///opt/axis/a.png]",
  );

  // A genuine pointy-bracket destination -- the form that exists so a path may contain spaces --
  // is still rewritten, and still keeps its path.
  assert.equal(
    normalizeExecutorTerminalResponse("Here: ![shot](<out/my shot.png>)", undefined),
    "Here: [Axis could not embed this local image: <out/my shot.png>]",
  );

  // Replace only the image reference. An earlier unclosed ![ on the line must not
  // consume surrounding prose up to this image's closing bracket.
  assert.equal(
    normalizeExecutorTerminalResponse("Figure ![1 shows the totals, and the capture is ![shot](out/a.png) here.", undefined),
    "Figure ![1 shows the totals, and the capture is [Axis could not embed this local image: out/a.png] here.",
  );

  // Not an image: a plain link to a local path is left alone. Linear renders it
  // as a link, not as an upload, so it is not the failure this rule is for.
  assert.equal(normalizeExecutorTerminalResponse("[see the file](tmp/a.png)", undefined), "[see the file](tmp/a.png)");
});

test("a refused response receipt records why in the Worker journal, and the user still gets the fallback", async (context) => {
  const answer = `The ads report lives in ${secretMarker} and covers 30 days.`;
  const { runId, journal, journalPath, activity } = await runTerminalResponse(context, answer);

  // A refused response still produces the fixed fallback and excludes the rejected
  // answer from delivery. Recording a diagnostic must not weaken that boundary.
  assert.equal(activity.projectionStatus, "fallback");
  assert.equal(activity.content.body, agentActivityFallbackResponseBody);

  const refusal = journal.readResponseProjectionRefusal(runId);
  assert.ok(refusal !== undefined, "the refused projection must be recorded in the Worker journal");
  assert.equal(refusal.sourceEventId, "projection-source-1");
  assert.deepEqual(
    refusal.diagnostics.map(({ code, instancePath }) => ({ code, instancePath })),
    [{ code: "unsafe_content", instancePath: "/content/body" }],
    "the record must name the rule that fired and where it fired, not merely that something did",
  );
  assert.equal(refusal.diagnostics[0]?.safeMessage, "Agent activity display text must not contain unredacted credential values.");

  // The constraint that makes this safe to persist: the record says WHERE, never
  // WHAT. A diagnostic that carried the matched substring would write the very
  // text the refusal exists to keep out of a durable store.
  //
  // Asserted on the STORED COLUMN, not on `readResponseProjectionRefusal`'s
  // result. That reader rebuilds every entry from the same five field names the
  // write side copies, so anything it hands back is five fields by
  // construction; a negative assertion routed through it could not fail. A
  // second, read-only handle on the same file sees what the Worker wrote.
  const raw = new DatabaseSync(journalPath, { readOnly: true });
  context.after(() => raw.close());
  const row = raw.prepare("SELECT diagnostics_json FROM worker_response_projection_refusals WHERE run_id = ?").get(runId) as { diagnostics_json: string } | undefined;
  assert.ok(row !== undefined, "the refusal must be stored under this Run's id");
  assert.equal(row.diagnostics_json.includes(secretMarker), false, "the stored bytes must not carry the matched marker");
  assert.equal(row.diagnostics_json.includes("The ads report lives in"), false, "the stored bytes must not carry any of the answer text");
  assert.deepEqual(
    (JSON.parse(row.diagnostics_json) as readonly object[]).map((entry) => Object.keys(entry).sort()),
    [["code", "instancePath", "keyword", "safeMessage", "schemaId"]],
    "the stored diagnostic must carry exactly the five fields the write side names",
  );
});

test("a response the contract accepts is delivered and records no refusal", async (context) => {
  const { runId, journal, activity } = await runTerminalResponse(context, "The ads report covers 30 days.");

  assert.equal(activity.projectionStatus, "projected");
  assert.equal(activity.content.body, "The ads report covers 30 days.");
  assert.equal(journal.readResponseProjectionRefusal(runId), undefined,
    "a projected response must leave no refusal behind -- otherwise the row above proves nothing about the refusal path");
});
