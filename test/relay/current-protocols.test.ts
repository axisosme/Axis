import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import { validateStructuredElicitation } from "../../src/shared/elicitation.js";
import { axisCliNodeRunTarget, validateRouterNodeRunTargetEvidence, validateRouterNodeTargetSnapshot } from "../../src/shared/node-routing-projection.js";
import { nodeCommandIntentDigest, validateNodeCommandRequest, validateNodeReceiptQuery } from "../../src/shared/router-node-commands.js";
import { validateRouterRelayNeutralEvent } from "../../src/shared/router-relay-event.js";
import { validateRouterWorkerLifecycleEvent } from "../../src/shared/router-worker.js";
import { elicitationFixture } from "../support/elicitation-fixtures.js";
import { nodeRunTargetFixture } from "../support/node-routing-fixtures.js";
import { buildNodeStartRequest } from "../support/router-node-commands-fixtures.js";
import { buildNodeWaitingEvent, buildWorkerBoundStartRequest } from "../support/router-node-fixtures.js";

test("Node commands and receipt queries accept only their current wire identifier", async () => {
  const request = await buildNodeStartRequest();
  assert.equal(request.contractVersion, "1.0.0");
  assert.equal(request.command.contractVersion, "1.0.0");
  assert.deepEqual(await validateNodeCommandRequest(request), request);
  assert.deepEqual(validateNodeReceiptQuery({ contractVersion: "1.0.0", nodeRef: request.nodeRef, commandId: request.command.commandId }),
    { contractVersion: "1.0.0", nodeRef: request.nodeRef, commandId: request.command.commandId });
  await assert.rejects(validateNodeCommandRequest({ ...request, contractVersion: "fictional-contract" }));
  await assert.rejects(validateNodeCommandRequest({ ...request, command: { ...request.command, contractVersion: "fictional-contract" } }));
  assert.throws(() => validateNodeReceiptQuery({ contractVersion: "fictional-contract", nodeRef: request.nodeRef, commandId: request.command.commandId }));
  assert.equal(nodeCommandIntentDigest(request), "sha256:5ffd4cc04713f38172d746d0dbc88d4cb88ee8dee6e168026dcfed3f8e29b819");
});

test("frozen Node authority and Run CLI evidence have distinct current identifiers", () => {
  const evidence = nodeRunTargetFixture();
  assert.equal(evidence.contractVersion, "1.0.0");
  assert.equal(evidence.target.contractVersion, "1.0.0-node");
  assert.equal(evidence.target.bindingDigest, "sha256:594716b68288a5ecca9439b6eaa679bf27225c0d698a26842ff4478e5e4c8c9b");
  assert.deepEqual(validateRouterNodeRunTargetEvidence(evidence), evidence);
  assert.equal(axisCliNodeRunTarget(evidence).schemaVersion, "axis.cli.run.v1");
  assert.throws(() => validateRouterNodeRunTargetEvidence({ ...evidence, contractVersion: "fictional-contract" }));
  assert.throws(() => validateRouterNodeTargetSnapshot({ ...evidence.target, contractVersion: "fictional-contract" }));
});

test("both elicitation shapes pass through the current Worker waiting extension", async () => {
  const { command: { start } } = await buildWorkerBoundStartRequest();
  const waiting = buildNodeWaitingEvent(start);
  assert.equal(waiting.contractVersion, "1.0.0");
  for (const other of [false, true]) {
    const elicitation = elicitationFixture("a", other);
    assert.equal(elicitation.contractVersion, other ? "1.0.0-other" : "1.0.0-fixed-choice");
    const event = { ...waiting, elicitation };
    assert.deepEqual(await validateRouterWorkerLifecycleEvent(event), event);
    assert.equal(validateStructuredElicitation({ ...elicitation, contractVersion: "fictional-contract" }), null);
  }
  await assert.rejects(validateRouterWorkerLifecycleEvent({ ...waiting, contractVersion: "fictional-contract" }));
});

test("Router Relay validation binds the current event identifier and digest", async () => {
  const event = { eventType: "axis.router.v1.accepted", payload: { safeSummary: "Worker accepted the execution." } };
  const envelope = { schemaVersion: "1.0.0", conversation: { system: "linear", agentSessionId: "session-1" }, axisSequence: 1,
    eventId: "event-1", eventDigest: `sha256:${createHash("sha256").update(JSON.stringify(event)).digest("hex")}`,
    sourceArtifactId: null, event, recordedAt: "2026-01-01T00:00:00.000Z" };
  assert.deepEqual(await validateRouterRelayNeutralEvent(envelope), envelope);
  await assert.rejects(validateRouterRelayNeutralEvent({ ...envelope, schemaVersion: "fictional-contract" }));
  const invalidEvent = { ...event, eventType: "axis.router.fictional.accepted" };
  await assert.rejects(validateRouterRelayNeutralEvent({ ...envelope, event: invalidEvent,
    eventDigest: `sha256:${createHash("sha256").update(JSON.stringify(invalidEvent)).digest("hex")}` }));
});
