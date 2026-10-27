import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { LINEAR_ACTIVITY_COPY } from "../../src/relay/linear/activity.js";
import { composeLinearChannel, type LinearChannel } from "../../src/relay/linear/composition.js";
import { ingestLinearWebhook } from "../../src/relay/linear/ingress.js";
import { EnvelopeCipher } from "../../src/relay/security.js";
import { channelBindingResolver } from "../../src/router/binding-resolver.js";
import { channelInstructionDigest, linearChannelConversationScope } from "../../src/router/channel-conversation-types.js";
import { linearTenantRef } from "../../src/router/channel-identity.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { Router } from "../../src/router/router.js";
import type { WorkerCommand, WorkerCommandResponse } from "../../src/shared/router-worker.js";
import { currentFixture, FakeLinearProvider, masterKey, MemoryVault, promptActivity, signed, SIGNING_SECRET } from "../helpers.js";
import { installTestLinearInstallation, linearChannelDatabases, linearChannelSnapshot } from "../helpers/linear-channel.js";
import { acceptedEnvelope, cancelledEnvelope } from "../linear-loop-harness.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint, projectNodeEvent, defaultStartWorkerRouting } from "../support/node-dispatch-double.js";
import { buildNodeActivityDelivery, buildNodeWaitingEvent } from "../support/router-node-fixtures.js";

const organizationId = "organization-fixture";
const teamId = "team-fixture";
const agentSessionId = "agent-session-fixture";
const scope = linearChannelConversationScope({ organizationId, agentSessionId });
const tenant = linearTenantRef(organizationId);
const conversation = { conversationId: agentSessionId };

/** Real Linear ingress, observation, redispatch and delivery, with in-process Node and provider doubles. */
async function harness(context: test.TestContext) {
  const directory = mkdtempSync(join(tmpdir(), "axis-channel-run-chain-"));
  const databases = linearChannelDatabases(directory);
  const endpointRef = { ref: "endpoint-run-chain", version: "v1" } as const;
  const nodeRef = { ref: "node-run-chain", version: "v1" } as const;
  const identity = { nodeId: "physical-node-run-chain", workerId: "worker-run-chain", executorKind: "codex", runtimeProfile: { ref: "executor-run-chain", version: "v1" } } as const;
  const teamBindings = new SqliteTeamBindingStore({});
  teamBindings.configure({
    scope: { organizationId, linearTeamId: teamId }, expectedRevision: 0, profile: { ...identity, endpointRef },
    audit: { operatorRef: "operator", source: "test", requestRef: "request", authorizationRef: "authorization" },
  });
  const commands: WorkerCommand[] = [];
  const port = new NodeDispatchDouble({
    nodeRef,
    reservation: nodeReservationFromEndpoint({ endpointRef, identity, sessionBindingExecutorId: identity.runtimeProfile.ref }, "worker-key-run-chain"),
    worker: {
      async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> {
        commands.push(command);
        if (command.command === "cancel" || command.command === "steer") return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: command.target.runId };
        return { contractVersion: command.contractVersion, commandId: command.commandId, status: "accepted", identity, runAccepted: acceptedEnvelope(command.start, `accepted-${command.commandId}`) };
      },
    },
  });
  const routerFilename = join(directory, "router.sqlite");
  const router = new Router({
    filename: routerFilename,
    nodeRouting: { bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef, nodeRef }] }), port },
    now: () => new Date("2026-09-09T00:00:00.000Z"),
  });
  const provider = new FakeLinearProvider();
  const vault = new MemoryVault();
  const cipher = new EnvelopeCipher(masterKey());
  const compose = () => composeLinearChannel({
      databases,
      webhookSecret: SIGNING_SECRET,
      oauth: { clientId: "test-client", clientSecret: "test-secret", redirectUri: "https://relay.invalid/oauth/linear/callback" },
      defaultTeamIds: [teamId],
      allowAllPublicTeams: false,
      integrityCheckIntervalMs: 60_000,
      tickIntervals: { ingress: 1_000, observe: 1_000, outbox: 1_000 },
    }, {
      cipher,
      vault,
      provider,
      router,
      workerRouting: defaultStartWorkerRouting,
      sleep: async () => undefined,
      staleReservationMs: 0,
      unconfirmedReservationMs: 0,
    });
  let channel = compose();
  context.after(() => { channel.close(); router.close(); teamBindings.close(); rmSync(directory, { recursive: true, force: true }); });
  await installTestLinearInstallation({ channel, vault, organizationId, appUserId: "app-user-fixture", teamIds: [teamId] });

  const startAt = (index: number) => {
    const command = commands[index];
    assert.ok(command !== undefined && (command.command === "start" || command.command === "continue"));
    return command.start;
  };
  const ingest = (envelope: Parameters<typeof projectNodeEvent>[2]["envelope"]) => projectNodeEvent(router, port, {
    envelope, recordedAt: "deliveryType" in envelope ? envelope.envelope.occurredAt : envelope.occurredAt,
  });
  const post = (payload: Record<string, unknown>) => {
    const rawBody = Buffer.from(JSON.stringify(payload));
    return ingestLinearWebhook(channel.ingress, { rawBody, headers: signed(rawBody) });
  };
  const observe = async () => {
    const tick = await channel.ticks.observe();
    assert.deepEqual(tick.failures, []);
    assert.equal(tick.sweepFailure, undefined);
    assert.equal(tick.earlySettleFailure, undefined);
    return tick;
  };
  const drain = async () => {
    for (let attempt = 0; attempt < 30; attempt += 1) {
      const tick = await channel.ticks.outbox();
      assert.deepEqual(tick.failures, []);
      if (linearChannelSnapshot(databases).unsettledOutbox === 0) return;
    }
    assert.fail("the outbox did not drain");
  };
  const start = async (overrides: Record<string, unknown> = {}) => {
    assert.equal(post(currentFixture("agent-session-created.json", overrides)).duplicate, false);
    const tick = await channel.ticks.ingress();
    assert.equal(tick.outcome?.disposition, "dispatched");
    assert.equal(commands.length, 1);
    await ingest(acceptedEnvelope(startAt(0), "accepted-start"));
    await observe();
    return startAt(0);
  };
  const park = async () => {
    await start();
    await ingest({ ...buildNodeWaitingEvent(startAt(0)), occurredAt: "2026-09-09T00:00:02.000Z" });
    await observe();
    assert.equal(channel.runChain.currentRun(conversation)?.state, "waiting_for_input");
    assert.equal(channel.runChain.openDecision(conversation)?.status, "waiting");
  };
  const prompt = async (activityId: string, body: string) => {
    provider.activities.push(promptActivity(activityId, body));
    const payload = currentFixture("agent-session-prompted.json", {
      agentActivity: { id: activityId, user: { id: "human-user-fixture" }, body, signal: "continue", createdAt: new Date().toISOString() },
    });
    assert.equal(post(payload).duplicate, false);
    return channel.ticks.ingress();
  };
  const stop = async () => {
    const payload = currentFixture("agent-session-stop.json");
    assert.equal(post(payload).duplicate, false);
    return { payload, tick: await channel.ticks.ingress() };
  };
  return {
    get channel(): LinearChannel { return channel; }, commands, port, provider, router, routerFilename, startAt, ingest, post, observe, drain, start, park, prompt, stop,
    reopen() { channel.close(); channel = compose(); },
  };
}

test("an explicit cancel clears a queued run-chain redispatch before terminal observation can dispatch it", async context => {
  const h = await harness(context);
  const first = await h.start();
  const queued = await h.prompt("activity-queued", "Do this instead.");
  assert.ok(queued.outcome?.disposition === "dispatched" && queued.outcome.status === "interrupt_requested");
  assert.notEqual(h.channel.runChain.pendingRedispatch(conversation), null);

  const stopped = await h.stop();
  assert.ok(stopped.tick.outcome?.disposition === "dispatched" && stopped.tick.outcome.kind === "cancel");
  assert.equal(h.channel.runChain.pendingRedispatch(conversation), null);
  await h.ingest(cancelledEnvelope(first, "cancelled-start"));
  const observed = await h.observe();
  assert.deepEqual(observed.redispatches.map(entry => entry.outcome), [{ disposition: "not_pending" }]);
  assert.equal(h.channel.runChain.currentRun(conversation)?.state, "failed");
  await h.observe();
  assert.equal(h.commands.filter(command => command.command === "start" || command.command === "continue").length, 1);
});

test("a waiting cancel stays locally resolved across store reopen and redelivery without Worker traffic", async context => {
  const h = await harness(context);
  await h.park();
  const { payload, tick } = await h.stop();
  assert.equal(tick.outcome?.disposition, "closed_waiting");
  if (tick.outcome?.disposition !== "closed_waiting") throw new Error("closed waiting required");
  const commandId = tick.outcome.commandId;
  const settled = h.channel.runChain.reservation({ commandId });
  assert.equal(settled?.status, "resolved_locally");
  assert.equal(h.commands.length, 1);
  h.reopen();

  assert.equal(h.channel.runChain.openDecision(conversation), null);
  assert.equal(h.channel.runChain.currentRun(conversation)?.state, "waiting_for_input");
  assert.equal(h.channel.conversations.conversation(agentSessionId)?.status, "active");
  assert.deepEqual(h.channel.runChain.reserve({ scope, ...conversation, tenant, commandId, kind: "cancel" }), { reservation: settled, replayed: true });
  assert.deepEqual(h.channel.runChain.resolveLocally({ commandId }), settled);
  assert.equal(h.post(payload).duplicate, false);
  assert.deepEqual((await h.channel.ticks.ingress()).outcome, tick.outcome);
  await h.observe();
  await h.drain();
  assert.equal(h.commands.length, 1);
  assert.equal(h.channel.runChain.openDecision(conversation), null);
  assert.equal(h.provider.created.filter(row => row.content.type === "response" && row.content.body === LINEAR_ACTIVITY_COPY.waitingStopResponse).length, 1);
});

test("a reply after a persisted waiting cancel is refused wait_cancelled and leaves the original Run unchanged", async context => {
  const h = await harness(context);
  await h.park();
  const original = h.channel.runChain.currentRun(conversation);
  assert.equal((await h.stop()).tick.outcome?.disposition, "closed_waiting");
  h.reopen();
  const reply = await h.prompt("activity-after-wait-cancel", "Use approach A.");
  assert.deepEqual(reply.outcome, { disposition: "reservation_refused", code: "wait_cancelled" });
  assert.equal(h.commands.length, 1);
  assert.deepEqual(h.channel.runChain.currentRun(conversation), original);
  assert.equal(h.channel.runChain.openDecision(conversation), null);
  assert.equal(h.channel.runChain.conversation(conversation)?.pendingCommandId, null);
});

test("a queued redispatch survives reopening and delivers one continuation's output exactly once", async context => {
  const h = await harness(context);
  const first = await h.start();
  const queued = await h.prompt("activity-durable-queued", "Do this instead.");
  assert.ok(queued.outcome?.disposition === "dispatched" && queued.outcome.status === "interrupt_requested");
  const marker = h.channel.runChain.pendingRedispatch(conversation);
  assert.notEqual(marker, null);
  await h.ingest(cancelledEnvelope(first, "cancelled-for-redispatch"));
  h.reopen();
  assert.deepEqual(h.channel.runChain.pendingRedispatch(conversation), marker);

  const observed = await h.observe();
  assert.deepEqual(observed.redispatches.map(entry => entry.outcome.disposition), ["dispatched"]);
  const successor = h.commands.at(-1);
  assert.ok(successor?.command === "continue");
  assert.equal(successor.continuation.priorRunId, first.identity.run.runId);
  assert.match(successor.instruction, /Do this instead\./u);
  assert.equal(h.channel.runChain.currentRun(conversation)?.priorRunId, first.identity.run.runId);
  assert.equal(h.channel.runChain.pendingRedispatch(conversation), null);
  assert.equal(h.commands.filter(command => command.command === "continue").length, 1);

  await h.ingest(acceptedEnvelope(successor.start, "accepted-successor"));
  const output = buildNodeActivityDelivery(successor.start);
  await h.ingest({ ...output, envelope: { ...output.envelope, occurredAt: "2026-09-09T00:00:03.000Z" } });
  await h.observe();
  await h.drain();
  await h.observe();
  await h.drain();
  assert.equal(h.channel.runChain.currentRun(conversation)?.state, "succeeded");
  assert.equal(h.channel.runChain.currentRun(conversation)?.runId, successor.start.identity.run.runId);
  assert.equal(h.commands.filter(command => command.command === "continue").length, 1);
  assert.equal(h.provider.created.filter(row => row.content.type === "response" && row.content.body === "Completed.").length, 1);
});

test("interrupt_requested settlement releases a claimed waiting decision and permits the next reply to claim it", async context => {
  const h = await harness(context);
  await h.park();
  const first = h.startAt(0);
  const raised = h.channel.runChain.openDecision(conversation);
  assert.ok(raised !== null);
  const input = { scope, ...conversation, tenant, kind: "continue", instructionDigest: channelInstructionDigest("Use approach A.") } as const;
  const reply = h.channel.runChain.reserve({ ...input, commandId: "decision-interrupt" }).reservation;
  assert.equal(reply.decisionId, raised.decisionId);
  assert.equal(reply.interruptRunId, undefined);
  assert.equal(h.channel.runChain.openDecision(conversation)?.status, "claimed");
  // This store-level outcome cannot be produced by a continue onto a parked Run through Router.
  h.channel.runChain.settle({ commandId: reply.commandId, outcome: { commandId: reply.commandId, status: "interrupt_requested", taskId: first.identity.task.taskId, runId: first.identity.run.runId } });
  const released = h.channel.runChain.openDecision(conversation);
  assert.equal(released?.decisionId, raised.decisionId);
  assert.equal(released?.status, "waiting");
  assert.equal(released?.claimedByCommandId, undefined);
  assert.equal(h.channel.runChain.conversation(conversation)?.pendingCommandId, null);
  const next = h.channel.runChain.reserve({ ...input, commandId: "next-decision-reply" }).reservation;
  assert.equal(next.decisionId, raised.decisionId);
  assert.equal(h.channel.runChain.openDecision(conversation)?.claimedByCommandId, next.commandId);
  assert.equal(h.channel.runChain.readyForTraffic(), true);
});

test("an unconfirmed cancel settles once without guarding the session and replays its stop-specific outcome", async context => {
  const h = await harness(context);
  await h.start();
  h.port.loseNextResponse();
  const { tick } = await h.stop();
  const cancel = h.commands.at(-1);
  assert.ok(cancel?.command === "cancel");
  assert.equal(h.channel.runChain.reservation({ commandId: cancel.commandId })?.status, "reserved");
  assert.equal(h.channel.dispatch.reconcile({ contractVersion: "1.0.0", commandId: cancel.commandId }), "unknown");
  assert.notEqual(tick.outcome?.disposition, "dispatched");

  const observed = await h.observe();
  assert.deepEqual(observed.sweep?.entries, [{ commandId: cancel.commandId, disposition: "unconfirmed" }]);
  const settled = h.channel.runChain.reservation({ commandId: cancel.commandId });
  assert.equal(settled?.status, "rejected");
  assert.equal(settled?.safeCode, "axis.channel.outcome_unconfirmed");
  assert.equal(h.channel.runChain.conversation(conversation)?.pendingCommandId, null);
  assert.equal(h.channel.runChain.sessionUnconfirmed(conversation), false);
  await h.drain();
  assert.equal(h.provider.created.filter(row => row.content.type === "error" && row.content.body === LINEAR_ACTIVITY_COPY.stopOutcomeUnconfirmed).length, 1);

  h.reopen();
  assert.deepEqual(h.channel.runChain.reserve({ scope, ...conversation, tenant, commandId: cancel.commandId, kind: "cancel" }), { reservation: settled, replayed: true });
  const replay = currentFixture("agent-session-stop.json");
  assert.equal(h.post(replay).duplicate, false);
  assert.deepEqual((await h.channel.ticks.ingress()).outcome, { disposition: "dispatch_rejected", commandId: cancel.commandId, safeCode: "axis.channel.outcome_unconfirmed" });
  await h.observe();
  await h.drain();
  assert.equal(h.commands.filter(command => command.command === "cancel").length, 1);
  assert.equal(h.provider.created.filter(row => row.content.type === "error" && row.content.body === LINEAR_ACTIVITY_COPY.stopOutcomeUnconfirmed).length, 1);
  assert.equal(h.provider.created.some(row => row.content.type === "error" && (row.content.body === LINEAR_ACTIVITY_COPY.startOutcomeUnconfirmed || row.content.body === LINEAR_ACTIVITY_COPY.continueOutcomeUnconfirmed)), false);
  assert.equal(h.channel.runChain.sessionUnconfirmed(conversation), false);

  const next = h.channel.runChain.reserve({ scope, ...conversation, tenant, commandId: "fresh-stop", kind: "cancel" });
  assert.equal(next.replayed, false);
  assert.equal(next.reservation.status, "reserved");
});

for (const withGuidance of [true, false]) {
  test(`created guidance is ${withGuidance ? "forwarded" : "absent"} at the Worker command boundary`, async context => {
    const h = await harness(context);
    await h.start(withGuidance ? {} : { guidance: undefined });
    const command = h.commands[0];
    assert.ok(command?.command === "start");
    assert.equal(command.instruction.includes("<linear-agent-guidance>"), withGuidance);
    assert.equal(command.instruction.includes("Use the captured Team execution profile."), withGuidance);
    await h.prompt("activity-guidance-followup", "Continue the task.");
    const following = h.commands[1];
    assert.ok(following?.command === "cancel");
    const first = h.startAt(0);
    await h.ingest(cancelledEnvelope(first, "cancelled-guidance-start"));
    await h.observe();
    const successor = h.commands.find(entry => entry.command === "continue");
    assert.ok(successor?.command === "continue");
    assert.equal(successor.instruction.includes("<linear-agent-guidance>"), false);
  });
}

test("20 KiB guidance reaches Worker within the command byte budget", async context => {
  const h = await harness(context);
  const body = "g".repeat(20_000);
  await h.start({ guidance: [{ body, origin: { type: "Organization" } }] });
  const command = h.commands[0];
  assert.ok(command?.command === "start");
  assert.ok(command.instruction.includes(body));
});

for (const guidance of [
  [{ body: "g".repeat(65_000), origin: { type: "Organization" } }],
  [{ body: "Rule.", origin: { type: "Team", team: { id: "team", key: "TEAM", name: "Team", displayName: "&".repeat(13_000), parentId: null } } }],
]) {
  test(`oversized composed guidance is rejected before Router admission (${guidance[0]?.origin.type})`, async context => {
    const h = await harness(context);
    h.post(currentFixture("agent-session-created.json", { guidance }));
    const tick = await h.channel.ticks.ingress();
    assert.ok(tick.outcome?.disposition === "dispatch_rejected");
    assert.equal(tick.outcome.safeCode, "axis.router.malformed");
    assert.equal(h.commands.length, 0);
    const initial = h.channel.runChain.initialStartReservation(conversation);
    assert.equal(initial?.safeCode, "axis.router.malformed");
    assert.ok(initial !== null);
    assert.equal(h.router.reconcileCommand(initial.commandId), "absent");
    const database = new DatabaseSync(h.routerFilename, { readOnly: true });
    try {
      for (const table of ["router_tasks", "router_runs", "router_node_runs", "router_node_command_state"]) {
        assert.equal(database.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get()?.count, 0, `${table} must remain empty`);
      }
    } finally { database.close(); }
  });
}

test("an oversized successor rolls back without preventing a valid continuation", async context => {
  const h = await harness(context);
  const first = await h.start();
  await h.ingest(cancelledEnvelope(first, "finished-guidance-start"));
  await h.observe();
  const ref = { organizationId, issueId: "issue-fixture", teamId, agentSessionId };
  const rejected = await h.router.continueNode({ ...ref, commandId: "oversized-successor", instruction: "g".repeat(65_000) });
  assert.deepEqual(rejected, { commandId: "oversized-successor", status: "rejected", safeCode: "axis.router.malformed" });
  assert.equal(h.router.reconcileCommand("oversized-successor"), "absent");
  assert.equal(h.commands.length, 1);
  const accepted = await h.router.continueNode({ ...ref, commandId: "valid-successor", instruction: "Continue the task." });
  assert.equal(accepted.status, "accepted");
  assert.equal(h.commands.length, 2);
});
