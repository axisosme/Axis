import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import {
  agentCategoryWebhookCase,
  OFFICIAL_LINEAR_WEBHOOK_CATALOG_IDS,
  officialLinearWebhookCase,
} from "../fixtures/linear/official-webhook-catalog.js";
import type { LinearChannelDatabasePaths } from "../../src/relay/linear/runtime-config.js";
import { renderLinearPromptRejection } from "../../src/relay/linear/instruction.js";
import { channelInstructionDigest } from "../../src/router/channel-conversation-types.js";
import { currentFixture, promptActivity } from "../helpers.js";
import {
  createNodeMockE2EHarness,
  databaseIntegrity,
  waitForPhase,
  type NodeMockE2EHarness,
} from "../helpers/node-mock-e2e-harness.js";

/**
 * Normal-path webhook coverage through the composed Linear channel, Node command route and
 * two Worker processes. The pinned data-change and Agent-category corpora exercise
 * envelopes that dispatch work or settle without Worker work. Assert durable outcomes
 * and eventual channel rest for each family.
 *
 * Linear's provider and the agent model are faked. This suite does not establish
 * adversarial signature, timestamp, malformed-shape or authority refusals.
 */

/** The `linear_inbox_deliveries` state a delivery reached, or `undefined` if the Relay never stored it. */
function deliveryState(filename: string, deliveryId: string): string | undefined {
  const database = new DatabaseSync(filename, { readOnly: true });
  try {
    database.exec("PRAGMA busy_timeout=5000; PRAGMA query_only=ON;");
    const found = database.prepare("SELECT state FROM linear_inbox_deliveries WHERE delivery_id = ?").get(deliveryId) as
      | { readonly state: string }
      | undefined;
    return found?.state;
  } finally {
    database.close();
  }
}

/** Every Worker-visible effect across both Workers, as one comparable value. */
async function totalWork(harness: NodeMockE2EHarness): Promise<{ starts: number; continues: number; cancels: number }> {
  const [a, b] = await Promise.all([harness.workers.a.stats(), harness.workers.b.stats()]);
  return { starts: a.starts + b.starts, continues: a.continues + b.continues, cancels: a.cancels + b.cancels };
}

/**
 * Pumps until the WHOLE topology is at rest, not just the Linear channel.
 *
 * The Node delivers Worker events to the Router on its own schedule, over a
 * real socket, and `pump()` drives only the channel's three ticks. A caller
 * that waits on the channel alone can therefore send the next turn while the
 * Router still believes the previous Run is live, and the Router will refuse
 * the new command as unresumable -- a failure that looks like a product defect
 * and is really a test racing its own subprocesses. The Node-side condition is
 * `pendingNodeEvents === 0`, which is the same gate the drain guard uses.
 */
async function pumpUntilSettled(harness: NodeMockE2EHarness, phase: string): Promise<void> {
  await waitForPhase(phase, async () => {
    await harness.pump();
    const { linear, router, workers } = harness.snapshot();
    return linear.accepted === 0 && linear.processing === 0 && linear.unsettledCommands === 0 && linear.unsettledOutbox === 0
      && router.activeRuns === 0
      && workers.a.activeRuns === 0 && workers.b.activeRuns === 0
      && (await harness.maintenance()).blockerCounts.pendingNodeEvents === 0;
  });
}

/**
 * The instruction digest the Relay bound to a comment invocation, read straight
 * off the durable ledger.
 *
 * The dispatched instruction itself is deliberately unreadable from here:
 * `ExecutorStartRequest#instruction` is documented as ephemeral and is never
 * copied into a receipt, an event or a journal, so a Worker-side assertion on
 * its text would require making the harness leak exactly what the shipped code
 * is careful not to. The ledger digest is the durable, content-derived record
 * of what the Relay decided the instruction was, which is the thing worth
 * pinning.
 */
function commentInvocationDigest(databases: LinearChannelDatabasePaths, sourceCommentId: string): string | undefined {
  for (const filename of Object.values(databases) as readonly string[]) {
    const database = new DatabaseSync(filename, { readOnly: true });
    try {
      database.exec("PRAGMA busy_timeout=5000; PRAGMA query_only=ON;");
      const present = database
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='linear_ledger_comment_invocations'")
        .get();
      if (present === undefined) continue;
      const row = database
        .prepare("SELECT instruction_digest FROM linear_ledger_comment_invocations WHERE source_comment_id = ?")
        .get(sourceCommentId) as { readonly instruction_digest: string } | undefined;
      return row?.instruction_digest;
    } finally {
      database.close();
    }
  }
  return undefined;
}

/** The `commandId` a comment invocation's `accepted` ledger row retained, straight off the ledger. */
function commentInvocationCommandId(databases: LinearChannelDatabasePaths, sourceCommentId: string): string | undefined {
  for (const filename of Object.values(databases) as readonly string[]) {
    const database = new DatabaseSync(filename, { readOnly: true });
    try {
      database.exec("PRAGMA busy_timeout=5000; PRAGMA query_only=ON;");
      const present = database
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='linear_ledger_comment_invocations'")
        .get();
      if (present === undefined) continue;
      const row = database
        .prepare("SELECT command_id FROM linear_ledger_comment_invocations WHERE source_comment_id = ?")
        .get(sourceCommentId) as { readonly command_id: string | null } | undefined;
      if (row !== undefined) return row.command_id ?? undefined;
    } finally {
      database.close();
    }
  }
  return undefined;
}

/** Every Activity body queued to Linear for one AgentSession's conversation, off `channel_message_outbox`. */
function outboxBodiesFor(databases: LinearChannelDatabasePaths, agentSessionId: string): readonly string[] {
  const database = new DatabaseSync(databases.delivery, { readOnly: true });
  try {
    database.exec("PRAGMA busy_timeout=5000; PRAGMA query_only=ON;");
    const rows = database
      .prepare("SELECT content FROM channel_message_outbox WHERE conversation_id = ?")
      .all(agentSessionId) as { readonly content: string }[];
    return rows.map((row) => row.content);
  } finally {
    database.close();
  }
}

/**
 * The webhook families that carry no Axis work.
 *
 * These are the envelopes a real Linear workspace sends CONSTANTLY -- every
 * issue edit, every comment, every reaction, every label change -- and getting
 * them wrong is not a quiet defect: an `Unsupported` envelope that reached
 * dispatch would start an agent on someone's unrelated issue edit. The property
 * worth a real chain is therefore "durably accepted, terminal, and provably no
 * Worker work", which is exactly what cannot be shown by a parser test alone.
 *
 * The two lifecycle envelopes run LAST and in this order deliberately.
 * `PermissionChange` rewrites the installation's Team allowlist and
 * `OAuthApp.revoked` deletes its credential outright; either one ahead of the
 * catalog sweep would make every later delivery fail the authority gate, and
 * the sweep would then be asserting "no dispatch" about deliveries that were
 * refused rather than recognized.
 */
test("every Linear webhook family that carries no work settles without reaching a Worker", { timeout: 180_000 }, async () => {
  const harness = await createNodeMockE2EHarness();
  try {
    const channelDatabase = harness.paths.linear.channel;

    // 1. The Developer Preview shape the parser reports as `Unsupported`: a known
    //    type with an action Axis does not implement. Quarantined rather than
    //    guessed at, which is the whole point of a closed parser.
    const preview = await harness.postWebhook(currentFixture("unsupported-preview-event.json"));
    await harness.pump();
    assert.equal(deliveryState(channelDatabase, preview.deliveryId), "quarantined",
      "phase=unsupported an unrecognized AgentSession action is durably quarantined");

    // 2. Linear's published data-change catalog, in full. Every model Linear can
    //    emit, across create/update/remove, plus the three IssueSLA convenience
    //    actions. `OAuthApp.revoked` is in the same catalog but is a lifecycle
    //    event with a real effect, so it is held back for step 4.
    const dataChangeIds = OFFICIAL_LINEAR_WEBHOOK_CATALOG_IDS.filter((id) => id !== "OAuthApp.revoked");
    assert.equal(dataChangeIds.length, 45, "the pinned official catalog is 45 data-change/convenience tuples plus OAuthApp.revoked");
    for (const id of dataChangeIds) {
      const official = officialLinearWebhookCase(id);
      // Posted through the harness rather than with the catalog's own signature:
      // the catalog signs with the shared fixture secret, while this topology
      // owns a freshly generated one. Re-signing here keeps the BODY the
      // catalog's and the ingress genuinely authenticated.
      const posted = await harness.postWebhook(official.payload, { event: official.entry.type });
      await harness.pump();
      assert.equal(deliveryState(channelDatabase, posted.deliveryId), "quarantined", `phase=official-catalog ${id} is quarantined`);
    }

    // AppUserNotification issueUnassignedFromYou is an Agent-category envelope,
    // separate from the data-change corpus. Axis does not subscribe to this category
    // and the parser has no accepting branch, so it is quarantined without Worker work.
    // This establishes parser behavior, not the notification's delegation-removal
    // trigger.
    const agentCategory = agentCategoryWebhookCase("AppUserNotification.issueUnassignedFromYou");
    const posted = await harness.postWebhook(agentCategory.payload, { event: agentCategory.entry.type });
    await harness.pump();
    assert.equal(deliveryState(channelDatabase, posted.deliveryId), "quarantined",
      "phase=agent-category an AppUserNotification.issueUnassignedFromYou delivery is durably quarantined");

    // PermissionChange grants the installation a Team, updates the allowlist and
    // completes without a session or Worker work. Keep the change additive so the
    // topology retains authority for subsequent checks.
    const permissionChange = { ...currentFixture("permission-change.json"), addedTeamIds: ["team-granted"], removedTeamIds: [] };
    const permission = await harness.postWebhook(permissionChange, { event: "PermissionChange" });
    await harness.pump();
    assert.equal(deliveryState(channelDatabase, permission.deliveryId), "completed",
      "phase=permission-change an applied Team grant completes rather than quarantines");

    // 4. `OAuthApp.revoked`: the installation is uninstalled and its credential
    //    deleted. Last, because after this the channel has no authority left.
    const revocation = await harness.postWebhook(currentFixture("oauth-revoked.json"), { event: "OAuthApp" });
    await harness.pump();
    assert.equal(deliveryState(channelDatabase, revocation.deliveryId), "completed",
      "phase=oauth-revoked a revocation completes as a local credential deletion");

    await pumpUntilSettled(harness, "no-work-settlement");

    // The load-bearing assertion for all four families at once. Every delivery
    // above was signed, accepted and verified by the real ingress; not one of
    // them may have produced Worker work.
    assert.deepEqual(await totalWork(harness), { starts: 0, continues: 0, cancels: 0 },
      "phase=no-work no non-AgentSession webhook family reached a Worker");
    assert.equal(harness.snapshot().router.commands, 0, "phase=no-work the Router was never asked to do anything");

    // The Node's own journal is only readable once its process has released the
    // file, so the strongest form of the assertion above -- nothing was ever
    // written to the Node's command journal either -- comes after the stop.
    await harness.stopNodeForInspection();
    assert.deepEqual(harness.snapshot().node, {
      commands: 0, ambiguousCommands: 0, routes: 0, events: 0, pendingEvents: 0, failedEventAttempts: 0, deliveredEvents: 0,
    }, "phase=no-work the Node journal is completely empty");

    for (const filename of [...Object.values(harness.paths.linear), harness.paths.router, harness.paths.node]) {
      assert.equal(databaseIntegrity(filename), "ok", "phase=integrity every journal passes SQLite integrity_check");
    }
  } finally {
    await harness.close();
  }
});

/** Waits for a Worker-visible effect, pumping the channel each round. */
async function waitForWork(
  harness: NodeMockE2EHarness,
  phase: string,
  predicate: (work: { starts: number; continues: number; cancels: number }) => boolean,
): Promise<void> {
  await waitForPhase(phase, async () => {
    await harness.pump();
    return predicate(await totalWork(harness));
  });
}

/**
 * The four `prompted` signals, and why all four belong in one normal-path test.
 *
 * Linear's parser accepts exactly four signals on a `prompted` AgentSession
 * event: `continue`, `auth`, `select` and `stop`. Only `stop` is a different
 * KIND of turn -- it withdraws the active Run. The other three are ordinary
 * continuation input and differ only in what the Linear UI did to produce them:
 * a typed reply, an authorization prompt answered, an option picked from an
 * elicitation. `./node-mock-e2e.test.ts` drives one of them.
 *
 * Driving all three matters because the signal is the one field on the
 * continuation path that is read off the ENVELOPE rather than off the
 * authenticated provider Activity, which makes it exactly the field a
 * signal-aware branch could start treating as a gate. If `auth` or `select`
 * ever stopped being ordinary continuation input, a user answering an
 * authorization prompt would sit there with the agent never resuming -- and no
 * test in the suite would go red. This is that test.
 */
test("every Linear AgentSession signal drives its own turn through a real Worker", { timeout: 180_000 }, async () => {
  const harness = await createNodeMockE2EHarness();
  try {
    // A direct `created` event: Linear delegates an Issue to Axis with no comment
    // involved. The Relay builds the start input from the authenticated exact
    // Issue read, never from the envelope's `promptContext`.
    const created = await harness.postCreated("first");
    assert.equal(created.duplicate, false, "phase=created the first delivery is new");
    await waitForWork(harness, "created-start", (work) => work.starts === 1);
    assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: 0, cancels: 0, bindings: 1 },
      "phase=created one `created` webhook is exactly one native start");

    // Linear retries deliveries. The exact same delivery replayed is answered
    // from the retained delivery state and does not start a second agent.
    const replay = await harness.postCreated("first", created.deliveryId);
    assert.equal(replay.duplicate, true, "phase=replay the retained delivery identity answers the retry");
    await harness.pump();
    assert.equal((await harness.workers.a.stats()).starts, 1, "phase=replay a webhook retry never starts a second agent");

    await harness.workers.a.emit(1, "succeeded");
    await pumpUntilSettled(harness, "created-settlement");

    // The three continuation signals, each on its own turn. Each one opens a new
    // Worker binding on the SAME AgentSession, which is what `bindings` counts.
    for (const [ordinal, signal] of [[2, "continue"], [3, "auth"], [4, "select"]] as const) {
      await harness.postPrompt("first", `Turn ${ordinal} arrived as ${signal}.`, signal);
      await waitForWork(harness, `prompted-${signal}`, (work) => work.continues === ordinal - 1);
      assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: ordinal - 1, cancels: 0, bindings: ordinal },
        `phase=prompted-${signal} a \`${signal}\` prompt is one ordinary continuation on the same session`);
      // Every turn but the last is run to completion. A turn is terminalized
      // with `succeeded` rather than `waiting_for_input` deliberately: a Run
      // parked on `waiting_for_input` has raised an elicitation and is waiting
      // for a DECISION REPLY, so an ordinary prompt is correctly refused there.
      // That sealed-token path is `./node-mock-e2e.test.ts`'s subject; this test
      // is about the ordinary turn, which follows a completed one.
      if (signal !== "select") {
        await harness.workers.a.emit(ordinal, "succeeded");
        await pumpUntilSettled(harness, `prompted-${signal}-settlement`);
      }
    }

    // `stop`, the one signal that withdraws work rather than adding it. It is sent
    // against the `select` turn deliberately left running above: a stop with
    // nothing in flight would prove nothing about cancellation.
    await harness.postStop("first");
    await waitForWork(harness, "stop-cancel", (work) => work.cancels === 1);
    assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: 3, cancels: 1, bindings: 4 },
      "phase=stop a `stop` prompt cancels the active Run without opening another binding");
    await harness.workers.a.emit(4, "cancelled");
    await pumpUntilSettled(harness, "stop-settlement");

    // Every turn above was one AgentSession on one Issue, and the idle Worker
    // proves the route never fanned a turn out to a second machine.
    assert.deepEqual(await harness.workers.b.stats(), { starts: 0, continues: 0, cancels: 0, bindings: 0 },
      "phase=affinity one AgentSession's turns all stayed on the Worker that opened it");

    const { linear } = harness.snapshot();
    assert.equal(linear.accepted, 0, "phase=settled no delivery is left unprocessed");
    assert.equal(linear.unsettledOutbox, 0, "phase=settled every Activity reached Linear");
    for (const filename of [...Object.values(harness.paths.linear), harness.paths.router, harness.paths.workers.a]) {
      assert.equal(databaseIntegrity(filename), "ok", "phase=integrity every journal passes SQLite integrity_check");
    }
  } finally {
    await harness.close();
  }
});

/**
 * A structured Axis mention opens a session from the human's exact Comment. The Relay
 * verifies the rich-text mention and derives the Worker instruction from that comment,
 * excluding unrelated envelope promptContext. This exercises a different provenance
 * path from a direct Issue delegation.
 */
test("a mentioned comment opens a session on its own instruction, and a mentioned reply continues it", { timeout: 180_000 }, async () => {
  const harness = await createNodeMockE2EHarness();
  try {
    await harness.postCommentCreated("mention", "Explain the failing migration.");
    await waitForWork(harness, "mention-start", (work) => work.starts === 1);
    assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: 0, cancels: 0, bindings: 1 },
      "phase=mention a structured @Axis mention opens exactly one session");

    // The session the Worker accepted is the one the mention named -- not a
    // second session invented alongside it.
    assert.deepEqual(acceptedSessions(harness.paths.workers.a), ["agent-session-mention"],
      "phase=mention the Worker's task is bound to the mentioned AgentSession");

    // And the instruction the Relay bound to that invocation is the human's own
    // comment with the mention node removed -- not the envelope's
    // `promptContext`, which the shipped fixture sets to unrelated presentation
    // text. Comparing digests is comparing content: a different instruction
    // cannot produce this one.
    assert.equal(commentInvocationDigest(harness.paths.linear, "comment-mention"),
      channelInstructionDigest("Explain the failing migration."),
      "phase=mention the durable invocation is bound to the comment's own instruction");
    assert.notEqual(channelInstructionDigest("Explain the failing migration."),
      channelInstructionDigest(String(currentFixture("agent-session-created.json").promptContext)),
      "phase=mention the two candidate instructions are genuinely distinguishable");

    await harness.workers.a.emit(1, "succeeded");
    await pumpUntilSettled(harness, "mention-settlement");

    // The human's second turn: a reply under the thread the mention opened.
    // This is a different envelope shape from `postPrompt`'s direct prompt --
    // it names both the reply comment and the captured root, and the Relay
    // checks the reply's `parentId` against that root before continuing -- so
    // it is its own normal flow rather than a variation of one already covered.
    await harness.postCommentPrompt("mention", "Now check the rollback path.");
    await waitForWork(harness, "mention-reply", (work) => work.continues === 1);
    assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: 1, cancels: 0, bindings: 2 },
      "phase=mention-reply a mentioned reply continues the same session it replied to");
    assert.deepEqual(acceptedSessions(harness.paths.workers.a), ["agent-session-mention", "agent-session-mention"],
      "phase=mention-reply the continuation stayed on the session the mention opened");
    // The reply's OWN invocation is bound, keyed by the reply comment rather
    // than the root -- a second turn is a second invocation, not a rewrite of
    // the first.
    //
    // Note the asymmetry with the start above, which is real and worth pinning:
    // a `created` mention digests the mention-EXCLUDED comment instruction,
    // because the comment is what authorized the session. A `prompted` mention
    // digests the exact provider Activity body UNCHANGED, markup and all,
    // because on a continuation the authenticated Activity -- not the rendered
    // comment -- is the execution authority.
    assert.equal(commentInvocationDigest(harness.paths.linear, "comment-mention-reply-1"),
      channelInstructionDigest(`<user id="app-user-fixture" notify>axis</user> Now check the rollback path.`),
      "phase=mention-reply the reply's exact Activity body is what was bound");

    await harness.workers.a.emit(2, "succeeded");
    await pumpUntilSettled(harness, "mention-reply-settlement");
    for (const filename of [...Object.values(harness.paths.linear), harness.paths.router, harness.paths.workers.a]) {
      assert.equal(databaseIntegrity(filename), "ok", "phase=integrity every journal passes SQLite integrity_check");
    }
  } finally {
    await harness.close();
  }
});

/**
 * A mention can yield created and prompted AgentSessionEvents for the same source
 * comment with distinct activity IDs and logical event keys. Process created first,
 * then deliver prompted while its start is unsettled.
 *
 * The accepted comment-invocation row must reconcile the prompted sibling to the
 * existing start: one command, no additional Activity and no logical-conflict or
 * command-in-flight refusal telling the user to resend.
 */
test("a comment mention's prompted sibling defers to the created event that already started it", { timeout: 180_000 }, async () => {
  const harness = await createNodeMockE2EHarness();
  try {
    const suffix = "siblings";
    const commentId = `comment-${suffix}`;
    const agentSessionId = `agent-session-${suffix}`;
    const instruction = "Explain the failing migration.";

    await harness.postCommentCreated(suffix, instruction);
    await waitForWork(harness, "siblings-start", (work) => work.starts === 1);
    assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: 0, cancels: 0, bindings: 1 },
      "phase=siblings-start the created mention opens exactly one session");
    const startCommandId = commentInvocationCommandId(harness.paths.linear, commentId);
    assert.notEqual(startCommandId, undefined, "phase=siblings-start the created mention retained an accepted comment invocation");

    // The `prompted` sibling for the SAME human act: same `sourceCommentId` as
    // the `created` event above (`comment-siblings`), NOT a reply -- Linear
    // mints a fresh `replyCommentId` for a genuine reply, and this is
    // deliberately not that. `postCommentPrompt` cannot be reused for this
    // reason; this mirrors its shape by hand instead. Posted while the
    // `created` start's command is still unsettled -- neither
    // `workers.a.emit` nor a settlement pump has run yet -- which is exactly
    // the window the defect needs.
    const activityId = `mock-comment-prompt-sibling-${commentId}`;
    const body = `<user id="app-user-fixture" notify>axis</user> ${instruction}`;
    harness.provider.exactActivity = {
      agentSessionId,
      activity: { ...promptActivity(activityId, body), sourceCommentId: commentId },
    };
    const promptedPayload = currentFixture("agent-session-prompted.json");
    const promptedAgentSession = promptedPayload.agentSession as Record<string, unknown>;
    const promptedIssue = promptedAgentSession.issue as Record<string, unknown>;
    promptedPayload.agentSession = {
      ...promptedAgentSession,
      id: agentSessionId,
      issue: { ...promptedIssue, id: `issue-${suffix}` },
      commentId,
    };
    promptedPayload.agentActivity = {
      ...(promptedPayload.agentActivity as Record<string, unknown>),
      id: activityId,
      body,
      signal: "continue",
      sourceCommentId: commentId,
    };
    const sibling = await harness.postWebhook(promptedPayload);
    const tick = await harness.pump();
    assert.equal(tick.claimed, sibling.deliveryId, "phase=siblings-prompted the prompted sibling was the delivery this tick claimed");

    // 1. No second command was dispatched.
    assert.deepEqual(await totalWork(harness), { starts: 1, continues: 0, cancels: 0 },
      "phase=siblings-prompted the prompted sibling dispatched no second command");
    assert.deepEqual(await harness.workers.a.stats(), { starts: 1, continues: 0, cancels: 0, bindings: 1 },
      "phase=siblings-prompted the Worker saw no additional binding either");

    // No queued Activity may tell the user to resend this already-started comment.
    // Compare the real rendered command-in-flight copy; the following rejection checks
    // also exclude logical-conflict responses.
    const commandInFlightCopy = renderLinearPromptRejection("command_in_flight");
    const bodies = outboxBodiesFor(harness.paths.linear, agentSessionId);
    assert.ok(bodies.every((entry) => !entry.includes(commandInFlightCopy)),
      "phase=siblings-prompted no queued Activity tells the user to resend a request that would duplicate their comment");

    // 3. The delivery itself ended `already_started`, pointing at the command
    // the `created` sibling already made.
    assert.deepEqual(tick.outcome, { disposition: "already_started", commandId: startCommandId },
      "phase=siblings-prompted the prompted sibling resolves to the created sibling's own command");

    await harness.workers.a.emit(1, "succeeded");
    await pumpUntilSettled(harness, "siblings-settlement");
    for (const filename of [...Object.values(harness.paths.linear), harness.paths.router, harness.paths.workers.a]) {
      assert.equal(databaseIntegrity(filename), "ok", "phase=integrity every journal passes SQLite integrity_check");
    }
  } finally {
    await harness.close();
  }
});

/** The AgentSession of every task a Worker accepted, in acceptance order. */
function acceptedSessions(filename: string): readonly string[] {
  const database = new DatabaseSync(filename, { readOnly: true });
  try {
    database.exec("PRAGMA busy_timeout=5000; PRAGMA query_only=ON;");
    const rows = database
      .prepare("SELECT accepted_json FROM worker_runs WHERE accepted_json IS NOT NULL ORDER BY created_at, run_id")
      .all() as { readonly accepted_json: string }[];
    return rows.map(({ accepted_json }) => {
      const accepted = JSON.parse(accepted_json) as { readonly task: { readonly sourceRef: { readonly agentSessionId: string } } };
      return accepted.task.sourceRef.agentSessionId;
    });
  } finally {
    database.close();
  }
}
