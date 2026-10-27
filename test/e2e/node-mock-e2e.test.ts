import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { deterministicElicitationReplyValue } from "../../src/index.js";
import { ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY } from "../../src/shared/router-relay-event.js";

/**
 * The summary as it appears inside a JSON string: the guard below searches
 * serialized provider evidence, so a summary containing any character JSON
 * escapes -- a newline, say, which the sibling refusal copy already uses --
 * would silently stop matching and the guard would stop being able to fail.
 */
const SUMMARY_IN_JSON = JSON.stringify(ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY).slice(1, -1);
import {
  createNodeMockE2EHarness,
  databaseIntegrity,
  pathExists,
  waitForPhase,
  type DatabaseSnapshot,
  type NodeMockE2EHarness,
} from "../helpers/node-mock-e2e-harness.js";

function readRows<T>(filename: string, sql: string): readonly T[] {
  const database = new DatabaseSync(filename, { readOnly: true });
  try {
    database.exec("PRAGMA busy_timeout=5000; PRAGMA query_only=ON;");
    return database.prepare(sql).all() as T[];
  } finally {
    database.close();
  }
}

function routerEventTypes(filename: string): readonly string[] {
  return readRows<{ readonly source_json: string }>(filename, "SELECT source_json FROM router_receipts ORDER BY rowid")
    .map(({ source_json }) => {
      const event = JSON.parse(source_json) as { readonly deliveryType?: string; readonly envelopeType?: string; readonly envelope?: { readonly envelopeType?: string } };
      return event.envelopeType ?? event.envelope?.envelopeType ?? event.deliveryType ?? "unknown";
    });
}

function workerIssueIds(filename: string): readonly string[] {
  return readRows<{ readonly accepted_json: string }>(filename, "SELECT accepted_json FROM worker_runs WHERE accepted_json IS NOT NULL ORDER BY created_at, run_id")
    .map(({ accepted_json }) => {
      const accepted = JSON.parse(accepted_json) as { readonly task: { readonly sourceRef: { readonly issueId: string } } };
      return accepted.task.sourceRef.issueId;
    });
}

async function pumpUntil(
  harness: NodeMockE2EHarness,
  phase: string,
  predicate: (snapshot: DatabaseSnapshot) => boolean | Promise<boolean>,
): Promise<void> {
  await waitForPhase(phase, async () => {
    await harness.pump();
    return await predicate(harness.snapshot());
  });
}

async function databaseMaterial(filename: string): Promise<string> {
  const parts: Buffer[] = [];
  for (const candidate of [filename, `${filename}-wal`]) {
    try { parts.push(await readFile(candidate)); } catch { /* a checkpointed database need not have a WAL */ }
  }
  return Buffer.concat(parts).toString("utf8");
}

/**
 * Exercise the production Linear channel and Node command Router composition with one Node
 * and two Worker processes. Events reach Router through the Node's pump and event
 * ingress.
 *
 * Change the default Worker only after Node and channel settlement, using a guarded
 * Node restart. Repeat the change in both directions while preserving concrete Worker
 * affinity for existing conversations. Pending events, receipt loss and replay must
 * block unsafe changes without repeating native execution; an unavailable Worker must
 * not prevent dispatch to its healthy peer.
 */
test("two Workers preserve affinity and execution across drain-gated default changes and receipt replay", { timeout: 120_000 }, async () => {
  const harness = await createNodeMockE2EHarness();
  const root = harness.root;
  try {
    const initialReadiness = await harness.readiness() as {
      readonly storage: { readonly status: string };
      readonly workers: readonly { readonly workerId: string; readonly transport: string }[];
    };
    assert.equal(initialReadiness.storage.status, "available", "phase=initial-readiness storage is available");
    assert.deepEqual(initialReadiness.workers.map(({ workerId, transport }) => [workerId, transport]), [
      ["mock-worker-a", "reachable"],
      ["mock-worker-b", "reachable"],
    ], "phase=initial-readiness both isolated Workers are reachable");

    const firstDelivery = await harness.postCreated("first");
    assert.equal(firstDelivery.duplicate, false, "phase=worker-a-start the Relay accepts one new webhook");
    await waitForPhase("worker-a-start", async () => { await harness.pump(); return (await harness.workers.a.stats()).starts === 1 && harness.snapshot().router.receipts >= 1; });
    assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: 0, cancels: 0, bindings: 1 });
    assert.deepEqual(await harness.workers.b.stats(), { starts: 0, continues: 0, cancels: 0, bindings: 0 });

    const replay = await harness.postCreated("first", firstDelivery.deliveryId);
    assert.equal(replay.duplicate, true, "phase=webhook-replay the exact Relay delivery is idempotent");
    await harness.pump();
    assert.equal((await harness.workers.a.stats()).starts, 1, "phase=webhook-replay native start is not repeated");

    const blockedCutover = await harness.guardedDefaultWorker("b");
    assert.equal(blockedCutover.changed, false, "phase=active-cutover-block binding mutation is refused");
    assert.equal(blockedCutover.changes, 0);
    assert.equal(blockedCutover.maintenance.mode, "drain");
    assert.equal(blockedCutover.maintenance.idle, false);
    assert.equal(blockedCutover.maintenance.blockerCounts.activeWorkerRuns, 1);
    assert.deepEqual(harness.currentRouting(), { changes: 0, defaultWorkerKey: "mock-worker-a" });
    assert.equal((await harness.maintenance("resume")).mode, "resume");

    await harness.workers.a.emit(1, "progressed");
    await waitForPhase("worker-a-progress", () => routerEventTypes(harness.paths.router).filter((type) => type === "run.progressed").length === 1);
    await harness.workers.a.emit(1, "waiting_for_input");
    await pumpUntil(harness, "worker-a-waiting", async (snapshot) => snapshot.router.receipts === 3
      && snapshot.workers.a.activeRuns === 0
      && snapshot.linear.unsettledOutbox === 0
      && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0);

    const cutover = await harness.guardedDefaultWorker("b");
    assert.equal(cutover.changed, true, "phase=quiescent-cutover the complete settlement gate permits the first routing change");
    assert.equal(cutover.changes, 1);
    assert.equal(cutover.maintenance.idle, true);
    assert.ok(Object.values(cutover.maintenance.blockerCounts).every((count) => count === 0));
    assert.deepEqual(harness.currentRouting(), { changes: 1, defaultWorkerKey: "mock-worker-b" });
    assert.equal((await harness.maintenance("resume")).mode, "resume");

    // The sealed-token decision reply drives the continue. Relay refuses an unclaimable sealed token,
    // so `continues === 1` is reachable only through a claim on the waiting-decision ledger.
    const decisionId = `axis-decision-${"a".repeat(48)}`;
    const optionId = `axis-option-${"c".repeat(48)}`;
    await harness.postPrompt("first", deterministicElicitationReplyValue(decisionId, optionId));
    await waitForPhase("historical-affinity-continue", async () => { await harness.pump(); return (await harness.workers.a.stats()).continues === 1; });
    assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: 1, cancels: 0, bindings: 2 },
      "phase=historical-affinity-continue the sealed-token reply drove exactly one continue on the existing Task");
    // An existing Task retains its resolved Worker key across a default-key change.
    // Continuing it on the new default would split one conversation across Workers.
    assert.deepEqual(await harness.workers.b.stats(), { starts: 0, continues: 0, cancels: 0, bindings: 0 },
      "phase=historical-affinity-continue the changed default does not retarget the old Task's continue");

    await harness.postStop("first");
    await waitForPhase("historical-affinity-cancel", async () => { await harness.pump(); return (await harness.workers.a.stats()).cancels === 1; });
    assert.equal((await harness.workers.a.stats()).cancels, 1);
    assert.deepEqual(await harness.workers.b.stats(), { starts: 0, continues: 0, cancels: 0, bindings: 0 },
      "phase=historical-affinity-cancel the changed default does not retarget the old Task's cancel either");
    await harness.workers.a.emit(2, "cancelled");
    await pumpUntil(harness, "cancel-terminal", async (snapshot) => snapshot.workers.a.activeRuns === 0
      && snapshot.router.activeRuns === 0
      && snapshot.linear.unsettledOutbox === 0
      && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0);

    await harness.postCreated("beta");
    await waitForPhase("worker-b-start", async () => { await harness.pump(); return (await harness.workers.b.stats()).starts === 1
      && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0;
    });
    assert.equal((await harness.workers.a.stats()).starts, 1);
    await harness.workers.b.emit(1, "progressed");
    await waitForPhase("worker-b-progress", async () => routerEventTypes(harness.paths.router).filter((type) => type === "run.progressed").length === 2
      && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0);

    const receiptsBeforeLoss = harness.snapshot().router.receipts;
    harness.proxy.arm();
    await harness.workers.b.emit(1, "succeeded");
    await harness.proxy.waitForDrop();
    await waitForPhase("lost-router-receipt", async () => {
      const snapshot = harness.snapshot();
      return snapshot.router.receipts === receiptsBeforeLoss + 1
        && snapshot.workers.b.pendingOutbox === 0
        && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 1;
    });
    const blockedChangeBack = await harness.guardedDefaultWorker("a");
    assert.equal(blockedChangeBack.changed, false, "phase=pending-event-change-block a committed Router event without Node receipt blocks a routing change");
    assert.equal(blockedChangeBack.maintenance.blockerCounts.pendingNodeEvents, 1);
    assert.deepEqual(harness.currentRouting(), { changes: 1, defaultWorkerKey: "mock-worker-b" });

    await harness.restartNode("SIGKILL");
    assert.equal(harness.snapshot().node.failedEventAttempts, 1, "phase=node-crash-cutline the lost response is durable before restart");
    await waitForPhase("node-restart-replay", async () => {
      const snapshot = harness.snapshot();
      return snapshot.node.pendingEvents === 1
        && harness.proxy.replayed >= 1
        && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0;
      // Give Node restart and event replay 480 attempts with 25ms pauses, 1.5 times the
      // default phase budget. The third waitForPhase parameter counts attempts rather
      // than milliseconds.
    }, 480);
    assert.equal(harness.snapshot().router.receipts, receiptsBeforeLoss + 1, "phase=node-restart-replay Router keeps one receipt for the replayed event");
    assert.equal((await harness.workers.b.stats()).starts, 1, "phase=node-restart-replay native execution is not repeated");
    await pumpUntil(harness, "post-restart-relay-settlement", (snapshot) => snapshot.router.activeRuns === 0 && snapshot.linear.unsettledOutbox === 0);

    const changeBack = await harness.guardedDefaultWorker("a");
    assert.equal(changeBack.changed, true, "phase=quiescent-change-back complete durable evidence permits a second routing change");
    assert.equal(changeBack.changes, 2);
    assert.equal(changeBack.maintenance.idle, true);
    assert.deepEqual(harness.currentRouting(), { changes: 2, defaultWorkerKey: "mock-worker-a" });
    assert.equal((await harness.maintenance("resume")).mode, "resume");

    await harness.postCreated("gamma");
    await waitForPhase("post-change-back-worker-a", async () => { await harness.pump(); return (await harness.workers.a.stats()).starts === 2; });
    await harness.workers.a.emit(3, "succeeded");
    // Node idleness alone does not permit a routing change while the Linear channel
    // remains unsettled. The guard must require both sides.
    await waitForPhase("node-idle-linear-unsettled", async () => {
      const s = harness.snapshot();
      return s.workers.a.activeRuns === 0 && s.workers.a.pendingOutbox === 0 && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0;
    });
    {
      const attempt = await harness.guardedDefaultWorker("b");
      assert.equal(attempt.maintenance.idle, true, "precondition: the Node itself is idle");
      assert.equal(attempt.changed, false, "a Node-idle but channel-unsettled state must not permit a routing change");
      assert.equal((await harness.maintenance("resume")).mode, "resume");
    }
    await pumpUntil(harness, "post-change-back-worker-a-terminal", async (snapshot) => snapshot.router.activeRuns === 0
      && snapshot.linear.unsettledOutbox === 0
      && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0);

    const finalCutover = await harness.guardedDefaultWorker("b");
    assert.equal(finalCutover.changed, true);
    assert.equal(finalCutover.changes, 3);
    assert.equal((await harness.maintenance("resume")).mode, "resume");
    await harness.workers.a.stop();
    const degradedReadiness = await harness.readiness() as {
      readonly workers: readonly { readonly workerId: string; readonly transport: string }[];
    };
    assert.deepEqual(degradedReadiness.workers.map(({ workerId, transport }) => [workerId, transport]), [
      ["mock-worker-a", "unknown"],
      ["mock-worker-b", "reachable"],
    ], "phase=single-worker-unavailable Worker B remains independently reachable");

    await harness.postCreated("delta");
    await waitForPhase("healthy-peer-dispatch", async () => { await harness.pump(); return (await harness.workers.b.stats()).starts === 2; });
    await harness.workers.b.emit(2, "succeeded");
    await pumpUntil(harness, "healthy-peer-terminal", async (snapshot) => snapshot.router.activeRuns === 0
      && snapshot.linear.unsettledOutbox === 0
      && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0);

    await harness.stopNodeForInspection();
    const final = harness.snapshot();
    assert.deepEqual(final.linear, { accepted: 0, processing: 0, unsettledCommands: 0, outbox: final.linear.outbox, unsettledOutbox: 0 });
    assert.deepEqual(final.router, { commands: 6, unsettledCommands: 0, runs: 5, activeRuns: 0, receipts: 12 });
    assert.deepEqual(final.node, { commands: 6, ambiguousCommands: 0, routes: 5, events: 12, pendingEvents: 0, failedEventAttempts: 1, deliveredEvents: 12 });
    assert.deepEqual(final.workers.a, { commands: 4, runs: 3, activeRuns: 0, outbox: 7, pendingOutbox: 0 });
    assert.deepEqual(final.workers.b, { commands: 2, runs: 2, activeRuns: 0, outbox: 5, pendingOutbox: 0 });
    assert.deepEqual(await harness.workers.b.stats(), { starts: 2, continues: 0, cancels: 0, bindings: 2 });
    assert.deepEqual(harness.currentRouting(), { changes: 3, defaultWorkerKey: "mock-worker-b" });
    assert.deepEqual([...workerIssueIds(harness.paths.workers.a)].sort(), ["issue-first", "issue-first", "issue-gamma"]);
    assert.deepEqual([...workerIssueIds(harness.paths.workers.b)].sort(), ["issue-beta", "issue-delta"]);
    assert.deepEqual(routerEventTypes(harness.paths.router), [
      "run.accepted", "run.progressed", "run.waiting_for_input",
      "run.accepted", "run.failed",
      "run.accepted", "run.progressed", "run.succeeded",
      "run.accepted", "run.succeeded",
      "run.accepted", "run.succeeded",
    ]);
    assert.equal(harness.proxy.dropped, 1);
    assert.ok(harness.proxy.replayed >= 1);

    for (const filename of [...Object.values(harness.paths.linear), harness.paths.router, harness.paths.node, harness.paths.workers.a, harness.paths.workers.b]) {
      assert.equal(databaseIntegrity(filename), "ok", "phase=final-integrity every independent journal passes SQLite integrity_check");
    }
    const providerEvidence = JSON.stringify(harness.provider.created);
    // Assert that the agent's own reply reaches Linear before checking the projection
    // for secrets and local material. Otherwise sanitization could pass without any
    // agent-authored content crossing the boundary.
    assert.ok(providerEvidence.includes("Mock execution completed."),
      "phase=response the agent's own reply body reaches the Linear projection");
    // Compare against the exported summary in its JSON-escaped form to distinguish
    // response delivery from the no-result fallback regardless of wording.
    assert.equal(providerEvidence.includes(SUMMARY_IN_JSON), false,
      "phase=response a success must not fall back to the no-result summary");
    const childEvidence = harness.childOutput.join("\n");
    for (const secret of [...harness.tokens, harness.nativeSentinel]) {
      assert.equal(providerEvidence.includes(secret), false, "phase=sanitization provider evidence excludes runtime secrets and native identity");
      assert.equal(childEvidence.includes(secret), false, "phase=sanitization child control output excludes runtime secrets and native identity");
    }
    for (const filename of [...Object.values(harness.paths.linear), harness.paths.router, harness.paths.node]) {
      assert.equal((await databaseMaterial(filename)).includes(harness.nativeSentinel), false, "phase=sanitization native identity stops at the Worker boundary");
    }
    for (const forbidden of [root, "/Users/", "/Volumes/", "file://", "private instruction", "native-"]) {
      assert.equal(providerEvidence.includes(forbidden), false, "phase=sanitization Relay projection contains no local or native material");
    }

  } finally {
    await harness.close();
  }
  assert.equal(await pathExists(root), false, "phase=cleanup the complete temporary topology is removed");
});
