import { nodeConfigurationFixture } from "../support/node-configuration.js";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { nodeStartupDiagnostics, startNode } from "../../src/node/runtime.js";
import { nodeChannelCommandPort } from "../../src/relay/node-routing-composition.js";
import { ChannelCommandError, channelCancelCommand, channelContinueCommand, channelStartCommand } from "../../src/router/channel-command.js";
import { linearChannelIdentity } from "../../src/router/channel-identity.js";
import { RouterError, type LinearRouterTaskRef } from "../../src/router/router.js";
import { buildNodeAcceptedEvent } from "../support/router-node-fixtures.js";
import { buildNodeStartRequest } from "../support/router-node-commands-fixtures.js";
import { workerSelectionFixture } from "../support/node-routing-fixtures.js";
import { DEFAULT_WORKER_LABEL_ROUTES } from "../../src/shared/worker-selection.js";
import { classifyStartupFailure, renderStartupFailure } from "../../src/relay/errors.js";
import { LINEAR_CHANNEL_COPY } from "../../src/relay/linear/channel.js";
import { currentFixture, promptActivity } from "../helpers.js";
import { audit, eventually, linearScope, nodeRefs, post, targets, unifiedHarness } from "../support/unified-routing-harness.js";

const taskRef: LinearRouterTaskRef = { organizationId: linearScope.organizationId, teamId: linearScope.linearTeamId, issueId: "issue-fixture", agentSessionId: "agent-session-fixture" };

test("harness setup failure releases listeners without waiting for an after hook", async () => {
  const listeners = () => process.getActiveResourcesInfo().filter(name => name === "TCPServerWrap").length;
  const before = listeners();
  const after: (() => unknown)[] = [];
  await assert.rejects(() => unifiedHarness({ after: fn => { after.push(fn); } }, {
    workerListeners: { 1: async () => { throw new Error("injected setup failure"); } },
  }), /injected setup failure/u);
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(listeners(), before);
  for (const release of after) await release();
});

function linearSessionFixture(name: string, ref: LinearRouterTaskRef): Record<string, unknown> {
  const payload = currentFixture(name);
  payload.agentSession = { ...(payload.agentSession as Record<string, unknown>), id: ref.agentSessionId, issue: { id: ref.issueId, team: { id: ref.teamId } } };
  return payload;
}

test("Node restart retains accepted Worker authority for event return and continuation", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  const first = h.commands[0]!.command;
  if (first.command !== "start") throw new Error("start required");
  await h.nodes[0]!.close();
  h.nodes[0] = await startNode(nodeConfigurationFixture(h.nodeConfigs[0]!));
  assert.equal(h.commands.length, 1, "restart must not redispatch accepted work");
  await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  h.provider.issueLabels = [];
  h.provider.activities.push(promptActivity("activity-human-prompt", "Continue after restart."));
  await h.linearPost(currentFixture("agent-session-prompted.json"));
  assert.equal(h.commands.length, 2);
  assert.equal(h.commands[1]!.worker, 0);
  const next = h.commands[1]!.command;
  if (next.command !== "continue") throw new Error("continue required");
  assert.equal(next.continuation.priorRunId, first.start.identity.run.runId);
});

test("Linear sessions use executor-label/default selection and carry authenticated Node events back to the shared Router", async context => {
  const h = await unifiedHarness(context);
  // build is an ordinary business label; claude is a fixed executor label mapped to the
  // review Worker. Relay forwards the executor label and discards business labels for
  // selection.
  h.provider.issueLabels = ["build", "claude"];
  const tick = await h.linearPost();
  assert.equal(h.commands.length, 1, JSON.stringify(tick));
  assert.equal(h.commands[0]!.worker, 0, "the Node resolves the executor label to its configured Worker");
  assert.equal(h.provider.issueContextCalls, 1);
  const start = h.commands[0]!.command;
  if (start.command === "cancel" || start.command === "steer") throw new Error("start required");
  const read = h.router.readRunTarget({ organizationId: linearScope.organizationId, teamId: linearScope.linearTeamId, issueId: "issue-fixture", agentSessionId: "agent-session-fixture", runId: start.start.identity.run.runId });
  assert.equal(read.contractVersion, "1.0.0");
  if (read.contractVersion !== "1.0.0") throw new Error("Run target evidence required");
  assert.deepEqual(read.target.nodeRef, nodeRefs[0]);
  assert.deepEqual(read.execution, { status: "recorded", workerId: "actual-review", executorId: "executor-review" });
  const cli = JSON.parse(execFileSync(process.execPath, ["--import=tsx", "src/bin/axis.ts", "--output", "json", "topology", "list", "--database", h.runtimeConfig.routerDatabase,
    "--organization", taskRef.organizationId, "--team", taskRef.teamId, "--issue", taskRef.issueId, "--agent-session", taskRef.agentSessionId, "--run", start.start.identity.run.runId], { encoding: "utf8" }));
  assert.equal(cli.result.historicalRunBinding.schemaVersion, "axis.cli.run.v1");
  assert.equal(cli.result.historicalRunBinding.workerId, "actual-review");
  await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents({ ...linearScope, organizationId: linearScope.organizationId, teamId: linearScope.linearTeamId, issueId: "issue-fixture", agentSessionId: "agent-session-fixture" }).length === 2);
  await h.linear.ticks.observe(); await h.linear.ticks.outbox(); await h.linear.ticks.outbox();
  assert.ok(h.provider.created.length > 0);
  h.provider.issueLabels = [];
  const defaultRef = { ...taskRef, agentSessionId: "default-session" };
  const defaultTick = await h.linearPost(linearSessionFixture("agent-session-created.json", defaultRef));
  assert.equal(h.commands.length, 2, JSON.stringify(defaultTick));
  assert.equal(h.commands[1]!.worker, 1, "known empty labels select Node default");
  await h.event(1, "accepted"); await h.event(1, "succeeded");
  await eventually(() => h.router.listTaskEvents(defaultRef).length === 2);
  await eventually(() => {
    const db = new DatabaseSync(h.runtimeConfig.routerDatabase, { readOnly: true });
    try { return (db.prepare("SELECT COUNT(*) AS n FROM router_receipts").get() as { n: number }).n === 4; } finally { db.close(); }
  });
  assert.equal(h.routingAudits.length, 2);
  context.diagnostic(JSON.stringify({ labelRoutingMeasurements: h.routingAudits }));
});

/**
 * Route unlabelled work through the real Relay, Router and Node using a Team scope
 * rule. This establishes that Relay builds the same tenant/Team identity the Node rule
 * expects; resolver-only tests cannot detect incorrect scope construction.
 */
test("a Team rule routes an unlabelled issue to that Team's Worker, and leaves other Teams on the default", async context => {
  const h = await unifiedHarness(context, {
    workerScopeRules: [{ scope: { system: "linear", tenantId: linearScope.organizationId, scopeId: linearScope.linearTeamId }, workerKey: "review-key" }],
  });
  // No executor label, and not an empty-labels special case either: an
  // ordinary business label the Relay drops on the floor.
  h.provider.issueLabels = ["build"];
  const tick = await h.linearPost();
  assert.equal(h.commands.length, 1, JSON.stringify(tick));
  assert.equal(h.commands[0]!.worker, 0, "the Team rule selected the Worker without any label on the issue");
  // The audit records what the Relay actually passed: the facts, with no
  // executor named, which is what makes the Node's rule the thing that decided.
  assert.deepEqual((h.routingAudits[0] as { outcome: string; executorLabels: readonly string[] }).outcome, "routing_context");
  assert.deepEqual((h.routingAudits[0] as { executorLabels: readonly string[] }).executorLabels, []);
  const otherRef = { ...taskRef, teamId: "other-team", issueId: "other-issue", agentSessionId: "other-session" };
  h.linear.installations.install({ organizationId: linearScope.organizationId, appUserId: "app-user-fixture", credentialRef: "linear:organization-fixture:app-user-fixture", teamIds: [linearScope.linearTeamId, otherRef.teamId], canAccessAllPublicTeams: false });
  h.provider.exactIssues.set(otherRef.issueId, { ...h.provider.exactIssue, id: otherRef.issueId, teamId: otherRef.teamId });
  const loadIssueContext = h.provider.loadIssueExecutionContext.bind(h.provider);
  h.provider.loadIssueExecutionContext = async (token, issueId) => ({ ...await loadIssueContext(token, issueId), teamId: issueId === otherRef.issueId ? otherRef.teamId : linearScope.linearTeamId });
  const other = await h.linearPost(linearSessionFixture("agent-session-created.json", otherRef));
  assert.equal(h.commands.length, 2, JSON.stringify(other));
  assert.equal(h.commands[1]!.worker, 1, "an unmatched scope reaches the Node default");
});

/**
 * The WORKER:CODEX label outranks the Team scope rule through the real routing stack.
 * FakeLinearProvider supplies the already-qualified label, so this scenario checks
 * routing rather than provider label qualification.
 */
test("a WORKER:CODEX label routes Linear work to the codex Worker ahead of a scope rule", async context => {
  const h = await unifiedHarness(context, {
    workerScopeRules: [
      { scope: { system: "linear", tenantId: linearScope.organizationId, scopeId: linearScope.linearTeamId }, workerKey: "review-key" },
    ],
  });
  h.provider.issueLabels = ["build", "WORKER:CODEX"];
  const tick = await h.linearPost();
  assert.equal(h.commands.length, 1, JSON.stringify(tick));
  assert.equal(h.commands[0]!.worker, 1, "the WORKER:CODEX label selected the codex Worker over the Team rule");
  assert.deepEqual((h.routingAudits[0] as { executorLabels: readonly string[] }).executorLabels, ["codex"]);
});

/**
 * The conflict refusal, followed all the way to what the user is actually told.
 *
 * The rendered text names the labels the user must change. Retrying unchanged
 * cannot succeed while both labels are on the issue.
 */
test("two executor labels are refused with copy that names the labels, not Router state", async context => {
  const h = await unifiedHarness(context);
  // Drive the conflict through the real command port so the rendered labels are tied to
  // the system-produced safe code, rather than a hand-constructed renderer input.
  const refused = await nodeChannelCommandPort(h.router).submit(channelStartCommand({
    commandId: "selection-conflict", identity: linearChannelIdentity({ ...taskRef, agentSessionId: "selection-conflict" }), instruction: "start",
    workerSelection: { kind: "policy", scope: { system: "linear", tenantId: linearScope.organizationId, scopeId: linearScope.linearTeamId }, executorLabels: ["claude", "codex"] },
  }));
  assert.equal(h.commands.length, 0, "a conflicting start must not reach any Worker");
  assert.equal(refused.status, "rejected");
  if (refused.status !== "rejected") throw new Error("rejected required");
  assert.equal(refused.safeCode, "axis.router.selection_conflict");

  // Everything below renders THAT code, not a literal.
  const failure = classifyStartupFailure(refused.safeCode, "Router");
  assert.equal(failure.certainty, "definite", "nothing ran, so the outcome is not ambiguous");
  const rendered = renderStartupFailure(failure);
  assert.match(rendered, /more than one executor/u);
  // Anchor refusal copy to the default route table's WORKER labels.
  for (const route of DEFAULT_WORKER_LABEL_ROUTES) assert.ok(rendered.includes(route.label), `the Linear copy names ${route.label}`);
  assert.doesNotMatch(rendered, /Resolve Router state/u, "the fix is on the issue, not in Router");
});

test("composed channel reconciliation reads accepted and interrupted Node commands without dispatch", async context => {
  const h = await unifiedHarness(context);
  await h.linearPost();
  await h.linearPost(linearSessionFixture("agent-session-created.json", { ...taskRef, agentSessionId: "second-session" }));
  assert.equal(h.commands.length, 2);
  const port = nodeChannelCommandPort(h.router);
  for (const entry of h.commands) {
    assert.equal(port.reconcile({ contractVersion: "1.0.0", commandId: entry.command.commandId }), "accepted");
    assert.equal(entry.command.command, "start");
    if (entry.command.command !== "start") throw new Error("start required");
    assert.deepEqual(port.reconcileAccepted?.({ contractVersion: "1.0.0", commandId: entry.command.commandId }),
      { taskId: entry.command.start.identity.task.taskId, runId: entry.command.start.identity.run.runId });
  }
  const before = h.commands.length;
  assert.equal(port.reconcile({ contractVersion: "1.0.0", commandId: "absent-command" }), "absent");
  assert.equal(h.commands.length, before);
  const cancelled = await port.submit(channelCancelCommand({ commandId: "joined-cancel", identity: linearChannelIdentity(taskRef) }));
  assert.equal(cancelled.status, "interrupt_requested");
  if (cancelled.status !== "interrupt_requested") throw new Error("interrupt required");
  const after = h.commands.length;
  assert.equal(port.reconcile({ contractVersion: "1.0.0", commandId: cancelled.commandId }), "interrupt_requested");
  assert.deepEqual(port.reconcileInterrupted({ contractVersion: "1.0.0", commandId: cancelled.commandId }), { taskId: cancelled.taskId, runId: cancelled.runId });
  assert.equal(h.commands.length, after, "reconciliation is read-only");
});

test("channel continuations stay on the frozen Node and Worker after labels and scope bindings change", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost(); await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  h.teamBindings.configure({ scope: linearScope, expectedRevision: 1, profile: h.profile(1), audit });
  h.provider.issueLabels = ["codex"];
  h.provider.activities.push(promptActivity("activity-human-prompt", "Continue the tests."));
  const tick = await h.linearPost(currentFixture("agent-session-prompted.json"));
  assert.equal(h.commands.length, 2, JSON.stringify(tick));
  const first = h.commands[0]!.command, continuation = h.commands[1]!.command;
  assert.equal(continuation.command, "continue");
  if (continuation.command !== "continue" || first.command !== "start") throw new Error("continuation required");
  assert.equal(h.commands[1]!.worker, 0);
  assert.equal(continuation.continuation.priorRunId, first.start.identity.run.runId);
  assert.equal(h.provider.issueContextCalls, 1, "prompted must not requery routing labels");
  const continuedTarget = h.router.readRunTarget({ ...taskRef, runId: continuation.start.identity.run.runId });
  if (continuedTarget.contractVersion !== "1.0.0") throw new Error("Run target evidence required");
  assert.deepEqual(continuedTarget.target.nodeRef, nodeRefs[0]);
  const newTaskRef = { ...taskRef, agentSessionId: "explicit-new-session" };
  const next = await nodeChannelCommandPort(h.router).submit(channelStartCommand({ commandId: "explicit-new-command", identity: linearChannelIdentity(newTaskRef), instruction: "new session", workerSelection: workerSelectionFixture() }));
  assert.equal(next.status, "accepted");
  const fresh = h.commands.at(-1)!.command;
  if (fresh.command !== "start") throw new Error("start required");
  const freshTarget = h.router.readRunTarget({ ...newTaskRef, runId: fresh.start.identity.run.runId });
  if (freshTarget.contractVersion !== "1.0.0") throw new Error("Run target evidence required");
  assert.deepEqual(freshTarget.target.nodeRef, nodeRefs[1]);
  assert.equal(h.commands.at(-1)!.worker, 1);

  h.teamBindings.configure({ scope: linearScope, expectedRevision: 2, profile: h.profile(0), audit });
  h.provider.issueLabels = ["claude"];
  const secondRef = { ...taskRef, agentSessionId: "second-frozen-session" };
  await h.linearPost(linearSessionFixture("agent-session-created.json", secondRef)); const secondIndex = h.commands.length - 1;
  await h.event(secondIndex, "accepted"); await h.event(secondIndex, "succeeded");
  const secondStart = h.commands[secondIndex]!.command;
  if (secondStart.command !== "start") throw new Error("start required");
  await eventually(() => h.router.listTaskEvents(secondRef).length === 2);
  h.teamBindings.configure({ scope: linearScope, expectedRevision: 3, profile: h.profile(1), audit });
  h.provider.issueLabels = ["build"];
  const body = "Continue on the frozen Worker.";
  h.provider.exactActivity = { agentSessionId: secondRef.agentSessionId, activity: promptActivity("second-frozen-prompt", body) };
  const continued = await retainedWorkerPrompt(h, "second-frozen-prompt", body, secondRef);
  assert.equal(h.commands.length, secondIndex + 2, JSON.stringify(continued));
  assert.equal(h.commands.at(-1)!.worker, 0);
  const secondContinuation = h.commands.at(-1)!.command;
  if (secondContinuation.command !== "continue") throw new Error("continue required");
  assert.equal(secondContinuation.continuation.priorRunId, secondStart.start.identity.run.runId);
  await h.event(h.commands.length - 1, "accepted", 0);
  h.provider.issueLabels = [];
  const newSession = await h.linearPost(linearSessionFixture("agent-session-created.json", { ...taskRef, agentSessionId: "fresh-default-session" }));
  assert.equal(h.commands.length, secondIndex + 3, JSON.stringify(newSession));
  await h.event(h.commands.length - 1, "accepted", 1);
});

test("duplicate and competing starts execute once through the real Node endpoint", async context => {
  const h = await unifiedHarness(context), port = nodeChannelCommandPort(h.router);
  const command = channelStartCommand({ commandId: "duplicate-command", identity: linearChannelIdentity(taskRef), instruction: "once", workerSelection: workerSelectionFixture(undefined, ["claude"]) });
  const results = await Promise.all([port.submit(command), port.submit(command)]);
  assert.deepEqual(results[0], results[1]); assert.equal(h.commands.length, 1);
  await assert.rejects(port.submit({ ...command, commandId: "competing-command" }), error => error instanceof RouterError && error.code === "axis.router.conflict");
  assert.equal(h.commands.length, 1);

  const base = await buildNodeStartRequest();
  if (base.command.command !== "start") throw new Error("start required");
  const request = { ...base, nodeRef: nodeRefs[0] };
  const start = base.command.start;
  const rival = { ...request, command: { ...base.command, commandId: "rival-command", start: { ...start, identity: { ...start.identity,
    run: { ...start.identity.run, runId: "rival-run", sessionBindingId: "rival-binding" },
    sessionBinding: { ...start.identity.sessionBinding, runId: "rival-run", sessionBindingId: "rival-binding" },
  } } } };
  const endpoint = new URL("/v1/node/commands", h.nodes[0]!.service.url);
  const concurrent = await Promise.all([post(endpoint, "node-command-0", request), post(endpoint, "node-command-0", rival)]);
  assert.deepEqual(concurrent.map(result => result.status).sort(), [202, 409], JSON.stringify(concurrent));
  assert.equal(h.commands.length, 2, "conflict side must make zero Worker calls");
  const winner = concurrent[0]!.status === 202 ? request : rival;
  assert.equal((await post(endpoint, "node-command-0", winner)).status, 202);
  assert.equal(h.commands.length, 2);
});

test("missing labels, an unresolved start, an unsatisfiable executor, mismatched version and unavailable native binding never fall back", async context => {
  const h = await unifiedHarness(context), port = nodeChannelCommandPort(h.router);
  h.provider.issueContextError = new Error("unavailable");
  await h.linearPost(); assert.equal(h.commands.length, 0);
  const unresolved = channelStartCommand({ commandId: "unresolved", identity: linearChannelIdentity({ ...taskRef, agentSessionId: "unresolved" }), instruction: "start" });
  await assert.rejects(port.submit(unresolved), error => error instanceof ChannelCommandError && error.code === "unmodeled_input");
  // Keep the populated inventory and Codex default, but leave Claude unmapped.
  // A valid Claude selection must receive a definite rejection before any
  // Worker is contacted, rather than falling back to the Codex Worker.
  await h.nodes[0]!.close();
  h.nodes[0] = await startNode(nodeConfigurationFixture({
    ...h.nodeConfigs[0]!,
    workerExecutorBindings: [{ executorLabel: "codex" as const, workerKey: "default-key" }],
  }));
  const unknownKey = await port.submit({ ...unresolved, commandId: "unknown", workerSelection: workerSelectionFixture(undefined, ["claude"]) });
  assert.equal(unknownKey.status, "rejected");
  if (unknownKey.status !== "rejected") throw new Error("rejected required");
  assert.equal(unknownKey.safeCode, "axis.router.endpoint_unknown");
  assert.equal(classifyStartupFailure(unknownKey.safeCode, "Router").certainty, "definite");
  assert.equal(h.commands.length, 0);
  const base = await buildNodeStartRequest();
  const result = await post(new URL("/v1/node/commands", h.nodes[0]!.service.url), "node-command-0", { ...base, nodeRef: { ...nodeRefs[0], version: "wrong-version" } });
  assert.equal(result.status, 409);
  assert.equal(h.commands.length, 0);
  const accepted = await port.submit(channelStartCommand({ commandId: "recover-start", identity: linearChannelIdentity({ ...taskRef, agentSessionId: "recover-session" }), instruction: "start", workerSelection: workerSelectionFixture() }));
  assert.equal(accepted.status, "accepted");
  await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents({ ...taskRef, agentSessionId: "recover-session" }).length === 2);
  h.rejectContinuation();
  const continuation = channelContinueCommand({ commandId: "recover-continue", identity: linearChannelIdentity({ ...taskRef, agentSessionId: "recover-session" }), instruction: "continue" });
  await assert.rejects(port.submit(continuation), error => error instanceof RouterError && error.code === "axis.router.recovery_required");
  await assert.rejects(port.submit(continuation), error => error instanceof RouterError && error.code === "axis.router.recovery_required");
  assert.deepEqual(h.commands.map(entry => entry.command.command), ["start", "continue"]);
  assert.deepEqual(h.commands.map(entry => entry.worker), [1, 1]);
});

test("authenticated event arriving before command receipt buffers then projects without re-execution", async context => {
  const h = await unifiedHarness(context);
  h.beforeReply(async command => {
    if (command.command !== "start") return;
    const event = { ...buildNodeAcceptedEvent(command.start), messageId: `accepted-${command.commandId}` };
    const result = await post(new URL("/v1/router/node-run-events", h.routerHttp.url), "node-event-0", {
      contractVersion: "1.0.0", nodeRef: nodeRefs[0], origin: targets[1]!.identity, envelope: event, recordedAt: new Date().toISOString(),
    });
    assert.equal(result.status, 202); assert.equal(result.body.status, "buffered");
  });
  await h.linearPost(); assert.equal(h.commands.length, 1);
  assert.equal(h.router.listTaskEvents(taskRef).length, 1);
  await h.event(0, "accepted");
  assert.equal(h.router.listTaskEvents(taskRef).length, 1);
});

test("logical Node credentials cannot impersonate another frozen Node even on the same physical machine", async context => {
  const h = await unifiedHarness(context);
  await h.linearPost();
  const command = h.commands[0]!.command;
  if (command.command === "cancel" || command.command === "steer") throw new Error("start required");
  const envelope = { ...buildNodeAcceptedEvent(command.start), messageId: `isolated-${command.commandId}` };
  const payload = { contractVersion: "1.0.0", nodeRef: nodeRefs[0], origin: targets[1]!.identity, envelope, recordedAt: new Date().toISOString() };
  const endpoint = new URL("/v1/router/node-run-events", h.routerHttp.url);
  assert.equal((await post(endpoint, "invalid-credential", payload)).status, 401);
  assert.equal((await post(endpoint, "node-event-1", payload)).status, 409);
  assert.equal((await post(endpoint, "node-event-1", { ...payload, nodeRef: nodeRefs[1] })).status, 409);
  assert.equal((await post(endpoint, "node-event-0", payload)).status, 202);
});

/**
 * A Node with no Worker must start, report readiness and remain associable. Dispatch
 * returns a definite deployment-gap refusal. After restarting with its first Worker
 * inventory, the same database and journal accept work normally.
 */
test("a Node with no Worker deployed starts, refuses dispatch deterministically, and takes work once its first Worker arrives", async context => {
  const h = await unifiedHarness(context, { emptyWorkerInventoryNodes: [0] });
  const env = h.nodeConfigs[0]!;
  const nodeUrl = new URL(`http://127.0.0.1:${env.port}`);

  const response = await fetch(new URL("/v1/node/readiness", nodeUrl), { headers: { authorization: `Bearer ${env.token}` } });
  assert.equal(response.status, 200, "an empty inventory must not fail readiness");
  const readiness = await response.json() as { readonly service: { readonly status: string }; readonly storage: { readonly status: string }; readonly workers: readonly unknown[] };
  assert.equal(readiness.service.status, "configured");
  assert.equal(readiness.storage.status, "available");
  assert.deepEqual(readiness.workers, [], "readiness reports the empty inventory rather than claiming a ready Worker");

  const port = nodeChannelCommandPort(h.router);
  const refused = await port.submit(channelStartCommand({ commandId: "inventory-empty", identity: linearChannelIdentity(taskRef), instruction: "run on an empty Node", workerSelection: workerSelectionFixture(undefined, ["claude"]) }));
  assert.equal(h.commands.length, 0, "no Worker may be contacted by a Node that has none");
  assert.equal(refused.status, "rejected");
  if (refused.status !== "rejected") throw new Error("rejected required");
  assert.equal(refused.safeCode, "axis.router.node_worker_unavailable");
  assert.notEqual(refused.safeCode, "axis.router.recovery_required", "a deployment gap is decided, not a command to recover");
  assert.notEqual(refused.safeCode, "axis.router.worker_rejected", "distinct from an uncertain Worker-tier refusal");
  const classified = classifyStartupFailure(refused.safeCode, "Router");
  assert.equal(classified.certainty, "definite", "the channel must not render this as an unknown outcome");
  const copy = renderStartupFailure(classified);
  assert.match(copy, /Deploy a Worker on the routed Node/u, "the operator is told what to do, not to check the target ref");
  assert.doesNotMatch(copy, /could not confirm/u);

  // What changed is the answer, not the record. The row is written exactly as the
  // recovery path always wrote it, because the only state the one-way transition
  // trigger accepts for a decided answer is terminal `rejected`, which must carry
  // reservation and receipt evidence a pre-receipt refusal does not have -- and a
  // row shaped otherwise is unreadable to an earlier build whose `Router`
  // constructor decodes every command row. Pinned here because a future change
  // that "finishes the job" by persisting a terminal row would break rollback.
  const stored = new DatabaseSync(h.runtimeConfig.routerDatabase, { readOnly: true });
  try {
    assert.deepEqual(
      { ...stored.prepare("SELECT state,reservation_json,receipt_digest,result_json FROM router_node_command_state WHERE command_id=?").get("inventory-empty") },
      { state: "receipt_pending", reservation_json: null, receipt_digest: null, result_json: null },
    );
  } finally { stored.close(); }

  // First Worker deployed: same database, same journal, no rebuild.
  await h.nodes[0]!.close();
  // Deploy the executor mapping with the Worker inventory in the same restart so a
  // claude policy selection resolves.
  h.nodes[0] = await startNode(nodeConfigurationFixture({ ...env, workers: h.populatedWorkers, defaultWorkerKey: "default-key", workerExecutorBindings: [{ executorLabel: "claude" as const, workerKey: "review-key" }] }));
  const accepted = await port.submit(channelStartCommand({ commandId: "inventory-deployed", identity: linearChannelIdentity({ ...taskRef, agentSessionId: "agent-session-after-deploy" }), instruction: "run after deployment", workerSelection: workerSelectionFixture(undefined, ["claude"]) }));
  assert.equal(accepted.status, "accepted", JSON.stringify(accepted));
  assert.equal(h.commands.length, 1, "the newly deployed Worker takes the command");
  assert.equal(h.commands[0]!.worker, 0);
});

/**
 * Start a Node with retained routes but no inventory, report sessions it cannot serve
 * and refuse follow-ups definitely without contacting a Worker. Restarting with the
 * Worker restored makes the same sessions usable again; inventory is read once at
 * startup.
 */
async function emptyInventoryWithoutInventory(h: Ax512Harness): Promise<void> {
  await h.nodes[0]!.close();
  const emptied: import("../support/node-configuration.js").NodeFixture = { ...h.nodeConfigs[0]!, workers: [] };
  delete emptied.defaultWorkerKey;
  // Keep executor mappings and scope rules aligned with inventory. A rule naming an
  // absent Worker fails at startup rather than silently failing when an issue matches
  // it.
  delete emptied.workerExecutorBindings;
  delete emptied.workerScopeRules;
  h.nodes[0] = await startNode(nodeConfigurationFixture(emptied));

  const env = h.nodeConfigs[0]!;
  const response = await fetch(new URL("/v1/node/readiness", `http://127.0.0.1:${env.port}`), { headers: { authorization: `Bearer ${env.token}` } });
  assert.equal(response.status, 200);
  assert.deepEqual((await response.json() as { readonly workers: readonly unknown[] }).workers, [], "readiness reports the empty inventory");
  assert.equal(h.nodes[0]!.retainedSessionsWithoutWorker, 1, "the operator sees the one session this Node cannot serve");
  assert.deepEqual(nodeStartupDiagnostics(h.nodes[0]!), [JSON.stringify({ code: "axis.node.sessions_without_worker", count: 1 })]);
}

async function emptyInventoryRedeployed(h: Ax512Harness): Promise<void> {
  await h.nodes[0]!.close();
  h.nodes[0] = await startNode(nodeConfigurationFixture(h.nodeConfigs[0]!));
  assert.equal(h.nodes[0]!.retainedSessionsWithoutWorker, 0, "control: with the session's Worker deployed, nothing is reported");
  assert.deepEqual(nodeStartupDiagnostics(h.nodes[0]!), []);
}

test("a Node holding command routes starts with no inventory, refuses a continue without contacting a Worker, and continues the session once its Worker is back", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  assert.equal(h.commands.length, 1, "a route must exist before the inventory is emptied");
  await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  await emptyInventoryWithoutInventory(h);
  const workerCalls = h.commands.length;

  const refused = retainedWorkerRejected(await retainedWorkerPrompt(h, "activity-human-prompt", "Continue after every Worker was removed."));
  assert.equal(h.commands.length, workerCalls, "no Worker is contacted");
  assert.equal(h.linear.runChain.reservation({ commandId: refused })?.status, "rejected", "the reservation is settled, not left reserved");
  assert.equal(h.router.reconcileCommand(refused), "absent");
  assert.equal(Object.values(retainedWorkerNodeCommandStates(h)).includes("ambiguous"), false, "the Node records no ambiguous receipt");
  assert.equal(retainedWorkerNodeCommandStates(h)[refused], undefined, "the Node records nothing for the refused command");
  assert.equal(retainedWorkerOutboxText(h).includes(JSON.stringify(LINEAR_CHANNEL_COPY.continueWorkerUnavailable).slice(1, -1)), true, "the user is told the Worker is unavailable");

  // The conversation is not held: a later prompt is dispatched and refused for the same reason.
  const again = retainedWorkerRejected(await retainedWorkerPrompt(h, "activity-empty-inventory-again", "Try once more."));
  assert.notEqual(again, refused);
  assert.equal(h.commands.length, workerCalls);

  // A new session on the same Node also receives a definite no-Worker refusal.
  const fresh = await nodeChannelCommandPort(h.router).submit(channelStartCommand({ commandId: "empty-inventory-new-session", identity: linearChannelIdentity({ ...taskRef, agentSessionId: "agent-session-empty-inventory-new" }), instruction: "start on a Node with no Worker", workerSelection: workerSelectionFixture(undefined, ["claude"]) }));
  assert.equal(fresh.status, "rejected", JSON.stringify(fresh));
  if (fresh.status !== "rejected") throw new Error("rejected required");
  assert.equal(fresh.safeCode, "axis.router.node_worker_unavailable");
  assert.equal(h.commands.length, workerCalls);

  await emptyInventoryRedeployed(h);
  const resumed = await retainedWorkerPrompt(h, "activity-empty-inventory-resumed", "Continue now that the Worker is back.");
  assert.equal((resumed as { outcome?: { disposition?: string } }).outcome?.disposition, "dispatched", JSON.stringify(resumed));
  assert.equal(h.commands.length, workerCalls + 1);
  assert.equal(h.commands[workerCalls]!.command.command, "continue");
  assert.equal(h.commands[workerCalls]!.worker, 0, "on the session's own Worker");
});

test("a Node holding a live Run starts with no inventory, refuses a stop without contacting a Worker, and reaches the Worker once it is back", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  await h.event(0, "accepted");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 1);
  await emptyInventoryWithoutInventory(h);
  const workerCalls = h.commands.length;

  const refused = retainedWorkerRejected(await h.linearPost(currentFixture("agent-session-stop.json")));
  assert.equal(h.commands.length, workerCalls, "no Worker is contacted");
  assert.equal(h.linear.runChain.reservation({ commandId: refused })?.status, "rejected");
  assert.equal(h.router.reconcileCommand(refused), "absent");
  assert.equal(Object.values(retainedWorkerNodeCommandStates(h)).includes("ambiguous"), false, "the Node records no ambiguous receipt");
  const outbox = retainedWorkerOutboxText(h);
  assert.equal(outbox.includes(JSON.stringify(LINEAR_CHANNEL_COPY.stopWorkerUnavailable).slice(1, -1)), true, "the user is told the stop did not reach the Worker");
  assert.equal(outbox.includes("could not start this session"), false, "never the startup copy");

  const next = await retainedWorkerPrompt(h, "activity-empty-inventory-after-stop", "A prompt after the refused stop.");
  assert.notEqual((next as { outcome?: { disposition?: string } }).outcome?.disposition, "reservation_refused", JSON.stringify(next));

  await emptyInventoryRedeployed(h);
  const before = h.commands.length;
  const stopAgain = currentFixture("agent-session-stop.json");
  stopAgain.agentActivity = { ...(stopAgain.agentActivity as Record<string, unknown>), id: "activity-empty-inventory-stop-again" };
  const stopped = await h.linearPost(stopAgain);
  const outcome = (stopped as { outcome?: { disposition?: string; status?: string } }).outcome;
  assert.equal(outcome?.disposition, "dispatched", JSON.stringify(stopped));
  assert.equal(outcome?.status, "interrupt_requested", JSON.stringify(stopped));
  assert.equal(h.commands.length, before + 1, "the stop is dispatched to a Worker");
  assert.equal(h.commands[before]!.command.command, "cancel");
  assert.equal(h.commands[before]!.worker, 0, "on the session's own Worker");
});

/**
 * Remove a session's Worker while retaining a smaller nonempty inventory, then send a
 * follow-up through the real Relay, Router and Node. A definite refusal must release
 * the reservation so the conversation remains usable when its Worker returns.
 */
type Ax512Harness = Awaited<ReturnType<typeof unifiedHarness>>;

async function retainedWorkerWithoutRouteWorker(h: Ax512Harness): Promise<void> {
  await h.nodes[0]!.close();
  // The route was taken with the `claude` executor label, i.e. `review-key`
  // (Worker 0); keep only the other Worker.
  const remaining = (h.populatedWorkers).filter(entry => entry.workerKey === "default-key");
  h.nodes[0] = await startNode(nodeConfigurationFixture({
    ...h.nodeConfigs[0]!,
    workers: remaining,
    // Remove the executor mapping with the Worker it names so startup succeeds and the
    // test reaches the retained-route refusal.
    workerExecutorBindings: [{ executorLabel: "codex" as const, workerKey: "default-key" }],
  }));
  // Report an unavailable session Worker in a nonempty inventory just as in an empty
  // inventory.
  assert.equal(h.nodes[0]!.retainedSessionsWithoutWorker, 1, "the operator sees the session this Node cannot serve");
}

function retainedWorkerNodeCommandStates(h: Ax512Harness): Readonly<Record<string, string>> {
  const database = new DatabaseSync(h.nodeConfigs[0]!.database!, { readOnly: true });
  try { return Object.fromEntries((database.prepare("SELECT command_id,state FROM commands").all() as { command_id: string; state: string }[]).map(row => [row.command_id, row.state])); }
  finally { database.close(); }
}

function retainedWorkerOutboxText(h: Ax512Harness): string {
  const database = new DatabaseSync(`${h.directory}/linear-outbox.sqlite`, { readOnly: true });
  try { return JSON.stringify(database.prepare("SELECT * FROM channel_message_outbox").all()); }
  finally { database.close(); }
}

function retainedWorkerPrompt(h: Ax512Harness, activityId: string, body: string, ref = taskRef) {
  h.provider.activities.push(promptActivity(activityId, body));
  const payload = linearSessionFixture("agent-session-prompted.json", ref);
  payload.agentActivity = { ...(payload.agentActivity as Record<string, unknown>), id: activityId, body };
  return h.linearPost(payload);
}

function retainedWorkerRejected(tick: unknown): string {
  const outcome = (tick as { outcome?: { disposition?: string; commandId?: string; safeCode?: string } }).outcome;
  assert.equal(outcome?.disposition, "dispatch_rejected", JSON.stringify(tick));
  assert.equal(outcome?.safeCode, "axis.router.node_worker_unavailable", JSON.stringify(tick));
  return outcome!.commandId!;
}

test("a continue whose Worker left the Node's inventory is refused without contacting a Worker, frees the conversation, and dispatches once the Worker is back", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  await retainedWorkerWithoutRouteWorker(h);
  const workerCalls = h.commands.length;

  const refused = retainedWorkerRejected(await retainedWorkerPrompt(h, "activity-human-prompt", "Continue after the Worker was removed."));
  assert.equal(h.commands.length, workerCalls, "no Worker is contacted");
  assert.equal(h.linear.runChain.reservation({ commandId: refused })?.status, "rejected", "the reservation is settled, not left reserved");
  assert.equal(h.router.reconcileCommand(refused), "absent", "Router retains nothing, so a sweep could settle it even after a crash");
  assert.equal(Object.values(retainedWorkerNodeCommandStates(h)).includes("ambiguous"), false, "the Node records no ambiguous receipt");
  assert.equal(retainedWorkerNodeCommandStates(h)[refused], undefined, "the Node records nothing for the refused command");
  assert.equal(retainedWorkerOutboxText(h).includes(JSON.stringify(LINEAR_CHANNEL_COPY.continueWorkerUnavailable).slice(1, -1)), true, "the user is told the Worker is unavailable");

  // The conversation is not held: a later prompt is dispatched (and refused for the same reason),
  // not turned away as a command still in flight.
  const again = retainedWorkerRejected(await retainedWorkerPrompt(h, "activity-retained-worker-again", "Try once more."));
  assert.notEqual(again, refused);
  assert.equal(h.commands.length, workerCalls);

  // The Worker is deployed again: the next prompt continues the session on it.
  await h.nodes[0]!.close();
  h.nodes[0] = await startNode(nodeConfigurationFixture(h.nodeConfigs[0]!));
  const resumed = await retainedWorkerPrompt(h, "activity-retained-worker-resumed", "Continue now that the Worker is back.");
  assert.equal((resumed as { outcome?: { disposition?: string } }).outcome?.disposition, "dispatched", JSON.stringify(resumed));
  assert.equal(h.commands.length, workerCalls + 1);
  assert.equal(h.commands[workerCalls]!.command.command, "continue");
  assert.equal(h.commands[workerCalls]!.worker, 0, "on the session's own Worker");
});

test("a stop of a live Run whose Worker left the Node's inventory is refused without contacting a Worker, frees the conversation, and reaches the Worker once it is back", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  await h.event(0, "accepted");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 1);
  await retainedWorkerWithoutRouteWorker(h);
  const workerCalls = h.commands.length;

  const refused = retainedWorkerRejected(await h.linearPost(currentFixture("agent-session-stop.json")));
  assert.equal(h.commands.length, workerCalls, "no Worker is contacted");
  assert.equal(h.linear.runChain.reservation({ commandId: refused })?.status, "rejected");
  assert.equal(h.router.reconcileCommand(refused), "absent");
  assert.equal(Object.values(retainedWorkerNodeCommandStates(h)).includes("ambiguous"), false, "the Node records no ambiguous receipt");
  const outbox = retainedWorkerOutboxText(h);
  assert.equal(outbox.includes(JSON.stringify(LINEAR_CHANNEL_COPY.stopWorkerUnavailable).slice(1, -1)), true, "the user is told the stop did not reach the Worker");
  assert.equal(outbox.includes("could not start this session"), false, "never the startup copy");

  const next = await retainedWorkerPrompt(h, "activity-retained-worker-after-stop", "A prompt after the refused stop.");
  assert.notEqual((next as { outcome?: { disposition?: string } }).outcome?.disposition, "reservation_refused", JSON.stringify(next));

  // Control pole: the Worker is deployed again, and a stop reaches it.
  await h.nodes[0]!.close();
  h.nodes[0] = await startNode(nodeConfigurationFixture(h.nodeConfigs[0]!));
  const before = h.commands.length;
  const stopAgain = currentFixture("agent-session-stop.json");
  stopAgain.agentActivity = { ...(stopAgain.agentActivity as Record<string, unknown>), id: "activity-retained-worker-stop-again" };
  const stopped = await h.linearPost(stopAgain);
  const outcome = (stopped as { outcome?: { disposition?: string; status?: string } }).outcome;
  assert.equal(outcome?.disposition, "dispatched", JSON.stringify(stopped));
  assert.equal(outcome?.status, "interrupt_requested", JSON.stringify(stopped));
  assert.equal(h.commands.length, before + 1, "the stop is dispatched to a Worker");
  assert.equal(h.commands[before]!.command.command, "cancel");
  assert.equal(h.commands[before]!.worker, 0, "on the session's own Worker");
});

function targetIdentityRefused(tick: unknown): string {
  const outcome = (tick as { outcome?: { disposition?: string; commandId?: string; safeCode?: string } }).outcome;
  assert.equal(outcome?.disposition, "dispatch_rejected", JSON.stringify(tick));
  assert.equal(outcome?.safeCode, "axis.router.worker_rejected", JSON.stringify(tick));
  return outcome!.commandId!;
}

/**
 * A definitely refused successor Run never executes. Preserve its refusal but restore
 * the prior Run as the continuation target so subsequent prompts can still use the
 * session.
 */
test("a continue a Worker definitely refuses keeps the session: the next prompt reaches the Worker, and the session continues once it accepts", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  const first = h.commands[0]!.command;
  if (first.command !== "start") throw new Error("start required");

  h.rejectContinuation("axis.worker.workspace_unavailable");
  const refused = targetIdentityRefused(await retainedWorkerPrompt(h, "activity-target-identity-refused", "Continue."));
  assert.equal(h.commands.length, 2, "the continue reached the Worker");
  assert.equal(h.linear.runChain.reservation({ commandId: refused })?.status, "rejected");

  const again = await retainedWorkerPrompt(h, "activity-target-identity-again", "Continue again.");
  targetIdentityRefused(again);
  assert.equal(h.commands.length, 3, "the next prompt is dispatched to the Worker again, not refused inside Router");

  h.rejectContinuation(null);
  const resumed = await retainedWorkerPrompt(h, "activity-target-identity-resumed", "Continue now.");
  assert.equal((resumed as { outcome?: { disposition?: string; status?: string } }).outcome?.disposition, "dispatched", JSON.stringify(resumed));
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.equal(continuation.continuation.priorRunId, first.start.identity.run.runId, "the continue follows the Run that executed, not a refused one");
});

test("a sequence of definite continuation refusals keeps the Linear conversation able to continue", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost(); const index = h.commands.length - 1;
  await h.event(index, "accepted"); await h.event(index, "succeeded");
  const start = h.commands[index]!.command;
  if (start.command !== "start") throw new Error("start required");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);

  h.rejectContinuation("axis.worker.workspace_unavailable");
  const refused = await retainedWorkerPrompt(h, "refused-prompt", "Continue.");
  assert.equal(h.commands.length, index + 2, JSON.stringify(refused));
  const again = await retainedWorkerPrompt(h, "refused-again-prompt", "Continue again.");
  assert.equal(h.commands.length, index + 3, `the next prompt reaches the Worker again: ${JSON.stringify(again)}`);

  h.rejectContinuation(null);
  const resumed = await retainedWorkerPrompt(h, "accepted-prompt", "Continue now.");
  assert.equal(h.commands.length, index + 4, JSON.stringify(resumed));
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.equal(continuation.continuation.priorRunId, start.start.identity.run.runId);
});

/**
 * A continuation refused before reserveRun has definitely not executed. Preserve that
 * classification through the Node receipt and Router safe code into decided Linear
 * refusal copy, releasing the conversation's command slot.
 */
test("a continuation the Worker cannot resume is answered with its own definite code and decided copy, not an ambiguous receipt nothing settles", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  const first = h.commands[0]!.command;
  if (first.command !== "start") throw new Error("start required");

  h.rejectContinuation("axis.worker.continuation_unresumable");
  const tick = await retainedWorkerPrompt(h, "activity-unresumable-unresumable", "Continue.");
  const outcome = (tick as { outcome?: { disposition?: string; commandId?: string; safeCode?: string } }).outcome;
  assert.equal(h.commands.length, 2, "the continue did reach the Worker");
  assert.equal(outcome?.disposition, "dispatch_rejected", JSON.stringify(tick));
  // The fold is the defect: every Worker code except `identity_mismatch` became
  // `axis.router.worker_rejected`, which is classified ambiguous.
  assert.equal(outcome?.safeCode, "axis.router.continuation_unresumable", JSON.stringify(tick));
  assert.equal(h.linear.runChain.reservation({ commandId: outcome!.commandId! })?.status, "rejected");

  const outbox = retainedWorkerOutboxText(h);
  assert.equal(outbox.includes(JSON.stringify(LINEAR_CHANNEL_COPY.continueSessionUnresumable).slice(1, -1)), true, `the session is told why, and what to do instead: ${outbox}`);
  assert.ok(!outbox.includes(LINEAR_CHANNEL_COPY.dispatchFailed), "not the generic dispatch failure");

  // A definite refusal must leave the session usable for another prompt.
  h.rejectContinuation(null);
  const resumed = await retainedWorkerPrompt(h, "activity-unresumable-resumed", "Continue now.");
  assert.equal((resumed as { outcome?: { disposition?: string } }).outcome?.disposition, "dispatched", JSON.stringify(resumed));
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.equal(continuation.continuation.priorRunId, first.start.identity.run.runId, "the continue follows the Run that executed, not the refused one");
});

/** The content of every Linear outbox row, one entry per activity Axis will post. */
function linearOutboxContents(h: Ax512Harness): readonly string[] {
  const database = new DatabaseSync(`${h.directory}/linear-outbox.sqlite`, { readOnly: true });
  try { return (database.prepare("SELECT content FROM channel_message_outbox ORDER BY rowid").all() as { content: string }[]).map((row) => row.content); }
  finally { database.close(); }
}
async function linearRunning(h: Ax512Harness) {
  h.provider.issueLabels = ["claude"];
  await h.linearPost(); const index = h.commands.length - 1;
  const start = h.commands[index]!.command;
  if (start.command !== "start") throw new Error("start required");
  await h.event(index, "accepted");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 1);
  return { index, start, conversationId: taskRef.agentSessionId };
}

test("a Linear prompt on a still-running Run is steered into it, acknowledged once, and queues nothing", async context => {
  const h = await unifiedHarness(context, { relayConfig: { nativeSteer: true } });
  assert.equal(h.runtimeConfig.nativeSteer, true);
  const { index, start, conversationId } = await linearRunning(h);

  const steered = await retainedWorkerPrompt(h, "steered-prompt", "also update the changelog");
  assert.equal(h.commands.length, index + 2, JSON.stringify(steered));
  const steer = h.commands.at(-1)!.command;
  if (steer.command !== "steer") throw new Error(`steer required: ${JSON.stringify(steer)}`);
  assert.equal(steer.target.runId, start.start.identity.run.runId);
  assert.match(steer.instruction, /also update the changelog/u);
  const outcome = steered.outcome;
  if (outcome?.disposition !== "dispatched" || outcome.status !== "steered") throw new Error(`steered required: ${JSON.stringify(steered)}`);
  const commandId = outcome.commandId;
  assert.equal(h.linear.runChain.reservation({ commandId })?.status, "steered");
  assert.equal(h.linear.runChain.pendingRedispatch({ conversationId }), null);
  assert.equal(h.router.reconcileCommand(commandId), "steered");
  assert.equal(Object.values(retainedWorkerNodeCommandStates(h)).includes("ambiguous"), false);
  const acknowledgement = LINEAR_CHANNEL_COPY.steered;
  assert.equal(linearOutboxContents(h).filter((content) => content.includes(JSON.stringify(acknowledgement).slice(1, -1))).length, 1, "the prompt is acknowledged exactly once");
  assert.match(acknowledgement, /passed this instruction to the running agent/u);

  await h.event(index, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  const tick = await h.linear.ticks.observe();
  assert.deepEqual(tick.redispatches.map((entry) => entry.outcome.disposition), tick.redispatches.map(() => "not_pending"), JSON.stringify(tick.redispatches));
  assert.equal(h.commands.length, index + 2, "the Run's end redispatches nothing: the steered instruction already ran in it");
});

test("a Linear prompt the Worker answers interrupt_requested queues the instruction and redispatches", async context => {
  const h = await unifiedHarness(context, { relayConfig: { nativeSteer: true } });
  h.steerAnswer("interrupt_requested");
  const { index, start, conversationId } = await linearRunning(h);

  const interrupted = await retainedWorkerPrompt(h, "interrupted-prompt", "do this instead");
  assert.equal(h.commands.at(-1)!.command.command, "steer");
  const outcome = interrupted.outcome;
  if (outcome?.disposition !== "dispatched" || outcome.status !== "interrupt_requested") throw new Error(`interrupt required: ${JSON.stringify(interrupted)}`);
  const commandId = outcome.commandId;
  assert.equal(h.linear.runChain.reservation({ commandId })?.status, "interrupt_requested");
  assert.notEqual(h.linear.runChain.pendingRedispatch({ conversationId }), null, "the instruction is queued");
  assert.equal(linearOutboxContents(h).some((content) => content.includes(JSON.stringify(LINEAR_CHANNEL_COPY.steered).slice(1, -1))), false, "no steered acknowledgement for an interrupt");

  await h.event(index, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  const tick = await h.linear.ticks.observe();
  assert.deepEqual(tick.redispatches.map((entry) => entry.outcome.disposition), ["dispatched"], JSON.stringify(tick.redispatches));
  assert.equal(h.commands.length, index + 3);
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.equal(continuation.continuation.priorRunId, start.start.identity.run.runId);
  assert.match(continuation.instruction, /do this instead/u);
  assert.equal(h.linear.runChain.pendingRedispatch({ conversationId }), null);
});

/**
 * With native steer enabled, forward follow-ups to a running Run while its Node drains.
 * The steer must settle without retaining the conversation's command slot.
 */
test("a follow-up steered into a running Run while its Node drains settles, and the session continues after the resume", async context => {
  const h = await unifiedHarness(context, { relayConfig: { nativeSteer: true } });
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  await h.event(0, "accepted");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 1);
  const maintenance = (mode: "drain" | "resume") => post(new URL("/v1/node/maintenance", h.nodes[0]!.service.eventUrl), "node-command-0", { contractVersion: "1.0.0", mode });
  assert.equal((await maintenance("drain")).body.mode, "drain");

  const steered = await retainedWorkerPrompt(h, "activity-draining-steer-steer", "Also update the changelog.");
  const outcome = (steered as { outcome?: { disposition?: string; status?: string; commandId?: string } }).outcome;
  assert.equal(outcome?.disposition, "dispatched", JSON.stringify(steered));
  assert.equal(outcome?.status, "steered", JSON.stringify(steered));
  assert.equal(h.commands.at(-1)!.command.command, "steer");
  assert.equal(h.linear.runChain.reservation({ commandId: outcome!.commandId! })?.status, "steered");
  assert.equal(h.router.reconcileCommand(outcome!.commandId!), "steered");
  assert.equal(retainedWorkerOutboxText(h).includes(JSON.stringify(LINEAR_CHANNEL_COPY.steered).slice(1, -1)), true, "the user is told the instruction reached the running agent");

  // The slot is not held: a second follow-up while still draining is steered too.
  const again = await retainedWorkerPrompt(h, "activity-draining-steer-again", "And the docs.");
  assert.equal((again as { outcome?: { status?: string } }).outcome?.status, "steered", JSON.stringify(again));

  await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  assert.equal((await maintenance("resume")).body.mode, "resume");
  await h.linear.ticks.observe();
  const database = new DatabaseSync(h.runtimeConfig.routerDatabase, { readOnly: true });
  try { assert.deepEqual(database.prepare("SELECT state FROM router_node_command_state WHERE state NOT IN ('accepted','rejected')").all(), [], "no Router row is left unsettled"); }
  finally { database.close(); }

  const next = await retainedWorkerPrompt(h, "activity-draining-steer-next", "Continue after the resume.");
  assert.equal((next as { outcome?: { disposition?: string; status?: string } }).outcome?.status, "accepted", JSON.stringify(next));
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.match(continuation.instruction, /Continue after the resume/u);
});

/**
 * A draining Node definitely refuses fresh starts and continuations with
 * axis.node.draining. Propagate that decision through Router and the channel so the
 * slot is released and queued redispatch remains available after resume.
 */
function nodeDrainMaintenance(h: Ax512Harness, mode: "drain" | "resume") {
  return post(new URL("/v1/node/maintenance", h.nodes[0]!.service.eventUrl), "node-command-0", { contractVersion: "1.0.0", mode });
}
function nodeDrainDraining(tick: unknown): string {
  const outcome = (tick as { outcome?: { disposition?: string; commandId?: string; safeCode?: string } }).outcome;
  assert.equal(outcome?.disposition, "dispatch_rejected", JSON.stringify(tick));
  assert.equal(outcome?.safeCode, "axis.router.node_draining", JSON.stringify(tick));
  return outcome!.commandId!;
}
function nodeDrainRouterUnsettled(h: Ax512Harness): unknown[] {
  const database = new DatabaseSync(h.runtimeConfig.routerDatabase, { readOnly: true });
  try { return database.prepare("SELECT command_id,state FROM router_node_command_state WHERE state NOT IN ('accepted','rejected')").all(); }
  finally { database.close(); }
}

test("a prompt to an ended Run while its Node drains is refused definitely, frees the slot, and continues after the resume", async context => {
  const h = await unifiedHarness(context, { linearUnconfirmedReservationMs: 0 });
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  await h.event(0, "accepted"); await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  const first = h.commands[0]!.command;
  if (first.command !== "start") throw new Error("start required");
  assert.equal((await nodeDrainMaintenance(h, "drain")).body.mode, "drain");
  const workerCalls = h.commands.length;

  const refused = nodeDrainDraining(await retainedWorkerPrompt(h, "activity-node-drain-refused", "Continue while the Node drains."));
  assert.equal(h.commands.length, workerCalls, "no Worker is contacted");
  assert.equal(h.linear.runChain.reservation({ commandId: refused })?.status, "rejected", "the reservation is settled, not left reserved");
  assert.equal(h.router.reconcileCommand(refused), "absent", "Router retains nothing, so a sweep could settle it even after a crash");
  assert.equal(retainedWorkerNodeCommandStates(h)[refused], undefined, "the Node records nothing for the refused command");
  assert.equal(Object.values(retainedWorkerNodeCommandStates(h)).includes("ambiguous"), false, "the Node records no ambiguous receipt");
  const outbox = retainedWorkerOutboxText(h);
  assert.equal(outbox.includes(JSON.stringify(LINEAR_CHANNEL_COPY.continueNodeDraining).slice(1, -1)), true, `the user is told the Node is paused: ${outbox}`);
  assert.equal(outbox.includes(JSON.stringify(LINEAR_CHANNEL_COPY.dispatchFailed).slice(1, -1)), false, "not the generic dispatch failure");

  // The slot is not held: a second prompt while still draining is refused the same way, not `reservation_in_flight`.
  const again = nodeDrainDraining(await retainedWorkerPrompt(h, "activity-node-drain-again", "Try once more."));
  assert.notEqual(again, refused);
  assert.equal(h.commands.length, workerCalls);

  // A zero-window sweep finds nothing to settle unconfirmed, so the session stays usable after the resume.
  await h.linear.ticks.observe();
  assert.deepEqual(nodeDrainRouterUnsettled(h), [], "no Router row is left unsettled");
  assert.equal((await nodeDrainMaintenance(h, "resume")).body.mode, "resume");
  const resumed = await retainedWorkerPrompt(h, "activity-node-drain-resumed", "Continue after the resume.");
  assert.equal((resumed as { outcome?: { disposition?: string; status?: string } }).outcome?.status, "accepted", JSON.stringify(resumed));
  assert.equal(h.commands.length, workerCalls + 1);
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.equal(continuation.continuation.priorRunId, first.start.identity.run.runId, "the continue follows the Run that executed, not a refused one");
  assert.match(continuation.instruction, /Continue after the resume/u);
});

test("an interrupt's redispatch refused while draining keeps the instruction and dispatches after the resume", async context => {
  let offsetMs = 0;
  const h = await unifiedHarness(context, { channelNow: () => new Date(Date.now() + offsetMs) });
  h.provider.issueLabels = ["claude"];
  await h.linearPost();
  await h.event(0, "accepted");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 1);
  const first = h.commands[0]!.command;
  if (first.command !== "start") throw new Error("start required");
  const conversationId = taskRef.agentSessionId;

  const interrupt = await retainedWorkerPrompt(h, "activity-node-drain-interrupt", "Do this instead.");
  assert.equal((interrupt as { outcome?: { status?: string } }).outcome?.status, "interrupt_requested", JSON.stringify(interrupt));
  assert.notEqual(h.linear.runChain.pendingRedispatch({ conversationId }), null, "the instruction is queued");
  assert.equal((await nodeDrainMaintenance(h, "drain")).body.mode, "drain");
  await h.event(0, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  const workerCalls = h.commands.length;

  for (let tick = 0; tick < 2; tick += 1) {
    const before = Date.now();
    const observed = await h.linear.ticks.observe();
    const outcomes = (observed as { redispatches: readonly { outcome: { disposition: string; safeCode?: string } }[] }).redispatches.map(entry => entry.outcome);
    if (tick === 0) {
      assert.deepEqual(outcomes.map(outcome => [outcome.disposition, outcome.safeCode]), [["dispatch_rejected", "axis.router.node_draining"]], JSON.stringify(outcomes));
    } else {
      assert.deepEqual(outcomes.map(outcome => outcome.disposition), ["not_pending"], `the next attempt is held: ${JSON.stringify(outcomes)}`);
    }
    assert.notEqual(h.linear.runChain.pendingRedispatch({ conversationId }), null, "the instruction is still queued");
    const attempts = h.linear.runChain.redispatchAttempts({ conversationId });
    assert.equal(attempts?.attemptCount, 0, "a draining refusal spends no attempt");
    assert.equal(attempts?.quarantinedAt, null);
    if (tick === 0) {
      const next = Date.parse(attempts!.nextAttemptAt!);
      assert.ok(next >= before + offsetMs + 60_000 && next <= Date.now() + offsetMs + 60_000, `held a minute out: ${attempts!.nextAttemptAt}`);
    }
  }
  assert.equal(h.commands.length, workerCalls, "no Worker is contacted while draining");
  assert.deepEqual(nodeDrainRouterUnsettled(h), [], "no Router row is left unsettled");

  assert.equal((await nodeDrainMaintenance(h, "resume")).body.mode, "resume");
  offsetMs += 61_000;
  const observed = await h.linear.ticks.observe();
  const outcomes = (observed as { redispatches: readonly { outcome: { disposition: string } }[] }).redispatches.map(entry => entry.outcome.disposition);
  assert.deepEqual(outcomes, ["dispatched"], JSON.stringify(outcomes));
  assert.equal(h.commands.length, workerCalls + 1);
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.equal(continuation.continuation.priorRunId, first.start.identity.run.runId);
  assert.match(continuation.instruction, /Do this instead/u);
  assert.equal(h.linear.runChain.pendingRedispatch({ conversationId }), null, "the queued instruction was delivered");
});

test("a Linear prompt while its Node drains is settled as rejected and the conversation continues after the resume", async context => {
  const h = await unifiedHarness(context);
  h.provider.issueLabels = ["claude"];
  await h.linearPost(); const index = h.commands.length - 1;
  await h.event(index, "accepted"); await h.event(index, "succeeded");
  const start = h.commands[index]!.command;
  if (start.command !== "start") throw new Error("start required");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  assert.equal((await nodeDrainMaintenance(h, "drain")).body.mode, "drain");

  const refused = nodeDrainDraining(await retainedWorkerPrompt(h, "draining-prompt", "Continue while draining."));
  assert.equal(h.commands.length, index + 1, "no Worker is contacted");
  const reservation = h.linear.runChain.reservation({ commandId: refused });
  assert.equal(reservation?.status, "rejected");
  assert.equal(reservation?.safeCode, "axis.router.node_draining");
  assert.equal(h.router.reconcileCommand(refused), "absent");
  const message = LINEAR_CHANNEL_COPY.continueNodeDraining;
  assert.match(message, /paused for an upgrade or maintenance/u);
  assert.doesNotMatch(message, /Please try again/u);
  assert.equal(linearOutboxContents(h).some(content => content.includes(JSON.stringify(message).slice(1, -1))), true, "the conversation is told the Node is paused");

  const again = nodeDrainDraining(await retainedWorkerPrompt(h, "draining-again-prompt", "Try once more."));
  assert.equal(h.linear.runChain.reservation({ commandId: again })?.safeCode, "axis.router.node_draining", "the slot is not held");

  assert.equal((await nodeDrainMaintenance(h, "resume")).body.mode, "resume");
  await retainedWorkerPrompt(h, "resumed-prompt", "Continue after the resume.");
  assert.equal(h.commands.length, index + 2);
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.equal(continuation.continuation.priorRunId, start.start.identity.run.runId);
});

test("a Linear interrupt's redispatch retains its instruction and retry budget across a draining refusal", async context => {
  let offsetMs = 0;
  const h = await unifiedHarness(context, { channelNow: () => new Date(Date.now() + offsetMs) });
  const { index, start, conversationId } = await linearRunning(h);
  const interrupted = await retainedWorkerPrompt(h, "draining-interrupt-prompt", "do this instead");
  const outcome = interrupted.outcome;
  if (outcome?.disposition !== "dispatched" || outcome.status !== "interrupt_requested") throw new Error(`interrupt required: ${JSON.stringify(interrupted)}`);
  assert.equal(h.linear.runChain.reservation({ commandId: outcome.commandId })?.status, "interrupt_requested");
  assert.equal((await nodeDrainMaintenance(h, "drain")).body.mode, "drain");
  await h.event(index, "succeeded");
  await eventually(() => h.router.listTaskEvents(taskRef).length === 2);
  const workerCalls = h.commands.length;

  const refused = await h.linear.ticks.observe();
  assert.deepEqual(refused.redispatches.map(entry => [entry.outcome.disposition, (entry.outcome as { safeCode?: string }).safeCode]), [["dispatch_rejected", "axis.router.node_draining"]], JSON.stringify(refused.redispatches));
  assert.equal(h.commands.length, workerCalls);
  assert.notEqual(h.linear.runChain.pendingRedispatch({ conversationId }), null, "the instruction is still queued");
  const attempts = h.linear.runChain.redispatchAttempts({ conversationId });
  assert.equal(attempts?.attemptCount, 0, "a draining refusal spends no attempt");
  assert.equal(attempts?.quarantinedAt, null);

  assert.equal((await nodeDrainMaintenance(h, "resume")).body.mode, "resume");
  offsetMs += 61_000;
  const tick = await h.linear.ticks.observe();
  assert.deepEqual(tick.redispatches.map(entry => entry.outcome.disposition), ["dispatched"], JSON.stringify(tick.redispatches));
  assert.equal(h.commands.length, workerCalls + 1);
  const continuation = h.commands.at(-1)!.command;
  if (continuation.command !== "continue") throw new Error("continue required");
  assert.equal(continuation.continuation.priorRunId, start.start.identity.run.runId);
  assert.match(continuation.instruction, /do this instead/u);
  assert.equal(h.linear.runChain.pendingRedispatch({ conversationId }), null);
});

test("a start while the Node drains is answered definitely through the command port and Linear ingress", async context => {
  const h = await unifiedHarness(context);
  assert.equal((await nodeDrainMaintenance(h, "drain")).body.mode, "drain");

  // Linear: through the real port, so the rendered code is the one the system produces.
  const refused = await nodeChannelCommandPort(h.router).submit(channelStartCommand({
    commandId: "node-drain-start", identity: linearChannelIdentity({ ...taskRef, agentSessionId: "node-drain-start" }), instruction: "start",
    workerSelection: { kind: "policy", scope: { system: "linear", tenantId: linearScope.organizationId, scopeId: linearScope.linearTeamId }, executorLabels: ["claude"] },
  }));
  assert.equal(h.commands.length, 0, "no Worker is contacted");
  if (refused.status !== "rejected") throw new Error(`rejected required: ${JSON.stringify(refused)}`);
  assert.equal(refused.safeCode, "axis.router.node_draining");
  const failure = classifyStartupFailure(refused.safeCode, "Router");
  assert.equal(failure.certainty, "definite", "nothing ran, so the outcome is not ambiguous");
  const rendered = renderStartupFailure(failure);
  assert.match(rendered, /paused for an upgrade or maintenance/u);
  assert.doesNotMatch(rendered, /could not confirm/u);

  h.provider.issueLabels = ["claude"];
  const tick = await h.linearPost();
  const commandId = nodeDrainDraining(tick);
  const reservation = h.linear.runChain.reservation({ commandId });
  assert.equal(reservation?.status, "rejected");
  assert.equal(reservation?.safeCode, "axis.router.node_draining");
  assert.equal(h.commands.length, 0);
  assert.equal(linearOutboxContents(h).some(content => content.includes(JSON.stringify(rendered).slice(1, -1))), true, "the session is told the Node is paused");
});
