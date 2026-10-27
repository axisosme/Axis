#!/usr/bin/env node
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  agentActivityFallbackResponseBody,
  agentActivityTruncationMarker,
  compareAgentResponseDeliveries,
  truncateAgentActivityText,
  validateAgentResponseDelivery,
  validateExecutorAgentResponseDelivery,
  validateRunAgentResponseDelivery,
  validateTaskAgentResponseDelivery,
} from "../../dist/src/agent-activity/index.js";

const corpus = JSON.parse(await readFile(new URL("./cases.json", import.meta.url), "utf8"));
const cases = corpus.response;
assert.equal(corpus.contractVersion, "1.0.0");

async function fixture(relative) {
  return JSON.parse(await readFile(new URL(`../../fixtures/agent-activity/${relative}`, import.meta.url), "utf8"));
}

const specificValidator = (deliveryType) => deliveryType === "executor_agent_response" ? validateExecutorAgentResponseDelivery
  : deliveryType === "run_agent_response" ? validateRunAgentResponseDelivery
  : validateTaskAgentResponseDelivery;

// Every bundled fixture wraps an succeeded envelope and round-trips through both the dispatching
// validator and its specific validator.
for (const relative of cases.validFixtures) {
  const value = await fixture(relative);
  assert.equal(value.envelope.contractVersion, corpus.contractVersion, `${relative} must wrap an envelope`);
  assert.equal(value.agentActivity.content.type, "response");
  const dispatched = await validateAgentResponseDelivery(value);
  assert.equal(dispatched.valid, true, `${relative} (dispatched): ${JSON.stringify(dispatched.diagnostics)}`);
  const specific = await specificValidator(value.deliveryType)(value);
  assert.equal(specific.valid, true, `${relative} (specific): ${JSON.stringify(specific.diagnostics)}`);
}

const runResponse = await fixture("valid/run-response.json");
const taskResponse = await fixture("valid/task-response.json");

// Response-delivery composites require Runtime envelopes. Unknown values are rejected independently at Run and Task levels.
for (const invalidVersion of ["fictional-contract"]) {
  for (const [label, base, validate] of [["run", runResponse, validateRunAgentResponseDelivery], ["task", taskResponse, validateTaskAgentResponseDelivery]]) {
    const invalid = structuredClone(base);
    invalid.envelope.contractVersion = invalidVersion;
    invalid.envelope.task.contractVersion = invalidVersion;
    if (invalid.envelope.attempt) invalid.envelope.attempt.contractVersion = invalidVersion;
    if (invalid.envelope.run?.contractVersion) invalid.envelope.run.contractVersion = invalidVersion;
    const result = await validate(invalid);
    assert.equal(result.valid, false, `${label}-level delivery must reject a ${invalidVersion} envelope`);
    assert.ok(
      result.diagnostics.some((item) => item.code === "schema_violation" && item.instancePath.startsWith("/envelope")),
      `${label}-level rejection of ${invalidVersion} must come from the envelope, not from some unrelated field`,
    );
  }
}

// mismatched-conversation: a receipt conversation identity that does not
// match the envelope's task source must be rejected, not silently passed.
const mismatchedConversation = structuredClone(runResponse);
mismatchedConversation.agentActivity.conversation = { system: "linear", agentSessionId: "wrong-session" };
assert.equal((await validateRunAgentResponseDelivery(mismatchedConversation)).valid, false, "a receipt with the wrong conversation identity must be rejected");

// result-ref-must-match-envelope: resultRef is the response surface's
// durable anchor (it stands where a waiting delivery's decisionId stands),
// so the receipt must never project a result the envelope did not deliver.
const mismatchedResultRef = structuredClone(runResponse);
mismatchedResultRef.agentActivity.resultRef = "a-different-result";
assert.equal((await validateRunAgentResponseDelivery(mismatchedResultRef)).valid, false, "the receipt resultRef must match the success envelope resultRef");

// activity-id-only-change-is-duplicate: the canonical tuple excludes
// activityId -- a redelivery of
// the same durable result carries a fresh activityId and must still compare
// as "duplicate", not "distinct".
const changedActivityId = structuredClone(runResponse);
changedActivityId.agentActivity.activityId = "a-different-activity-id";
assert.equal(compareAgentResponseDeliveries(runResponse, changedActivityId), "duplicate", "changing only activityId must not make two deliveries of the same result compare as distinct");

// conversation-identity-participates-in-canonical-tuple: two deliveries that
// genuinely differ in conversation identity must be "distinct", never
// collapse into one canonical identity and get misjudged a content conflict.
const changedConversation = structuredClone(runResponse);
changedConversation.agentActivity.conversation = { system: "linear", agentSessionId: "another-session" };
changedConversation.envelope.task.sourceRef.agentSessionId = "another-session";
assert.equal((await validateRunAgentResponseDelivery(changedConversation)).valid, true, "the re-pointed delivery must be internally coherent on its own");
assert.equal(compareAgentResponseDeliveries(runResponse, changedConversation), "distinct", "a genuine conversation identity difference must compare as distinct, not conflict");

// coherent-body-drift-is-conflict: two INDEPENDENTLY VALID deliveries of the
// same durable result whose projected bodies disagree must be a conflict.
// The succeeded envelope carries no body, so body drift cannot be
// caught by envelope/receipt coherence -- only the replay comparator can see
// it, which is exactly why this case has to exist.
const driftedBody = structuredClone(runResponse);
driftedBody.agentActivity.content.body = "A materially different answer for the same result.";
assert.equal((await validateRunAgentResponseDelivery(driftedBody)).valid, true, "the drifted delivery must be internally coherent on its own");
assert.equal(compareAgentResponseDeliveries(runResponse, driftedBody), "conflict", "two independently valid deliveries of the same result whose bodies differ must compare as conflict");

// Fallback receipts accept the provider-neutral constant and reject any other body.
assert.equal(/codex/iu.test(agentActivityFallbackResponseBody), false, "the fallback body must not name a specific executor");
const taskFallback = await fixture("valid/task-fallback.json");
assert.equal(taskFallback.agentActivity.content.body, agentActivityFallbackResponseBody);
const invalidFallbackBody = structuredClone(taskFallback);
invalidFallbackBody.agentActivity.content.body = "This is not the required fallback response.";
assert.equal((await validateTaskAgentResponseDelivery(invalidFallbackBody)).valid, false, "a fallback body that differs from the required constant must be rejected");
const projectedFallback = structuredClone(taskFallback);
projectedFallback.agentActivity.projectionStatus = "projected";
projectedFallback.agentActivity.content.body = "A genuinely projected answer.";
assert.equal((await validateTaskAgentResponseDelivery(projectedFallback)).valid, true, "a projected response is still free-form");

// Truncation reporting: the projection-status/truncation data axis needs to
// report on. A visible marker and its structured record must always appear
// together, in both directions, and the counts must be arithmetically true.
const longBody = "x".repeat(9000);
const truncated = truncateAgentActivityText(longBody);
assert.ok(truncated.truncation, "a 9000-scalar body must truncate");
assert.equal(truncated.truncation.fieldPath, "/content/body");
const truncatedDelivery = structuredClone(runResponse);
truncatedDelivery.agentActivity.content.body = truncated.value;
truncatedDelivery.agentActivity.truncated = true;
truncatedDelivery.agentActivity.truncations = [truncated.truncation];
const truncatedResult = await validateRunAgentResponseDelivery(truncatedDelivery);
assert.equal(truncatedResult.valid, true, `a coherently truncated response must validate: ${JSON.stringify(truncatedResult.diagnostics)}`);

// truncation-marker-without-record
const markerWithoutRecord = structuredClone(truncatedDelivery);
markerWithoutRecord.agentActivity.truncated = false;
markerWithoutRecord.agentActivity.truncations = [];
assert.equal((await validateRunAgentResponseDelivery(markerWithoutRecord)).valid, false, "a visible truncation marker without its structured record must be rejected");

// truncation-record-without-marker
const recordWithoutMarker = structuredClone(truncatedDelivery);
recordWithoutMarker.agentActivity.content.body = "A short untruncated answer.";
assert.equal((await validateRunAgentResponseDelivery(recordWithoutMarker)).valid, false, "a truncation record without a visible marker must be rejected");

// truncation-counts-must-be-coherent: the marker still matches, but the
// arithmetic does not. Silent miscounting is exactly what the truncation contract forbids.
const incoherentCounts = structuredClone(truncatedDelivery);
incoherentCounts.agentActivity.truncations = [{
  ...truncated.truncation,
  originalScalarCount: truncated.truncation.originalScalarCount + 500,
}];
assert.equal((await validateRunAgentResponseDelivery(incoherentCounts)).valid, false, "truncation counts that do not add up must be rejected");

// The marker helper is the single source of the visible wording; a record
// whose omittedScalarCount disagrees with the marker in the body is caught.
const wrongMarkerCount = structuredClone(truncatedDelivery);
wrongMarkerCount.agentActivity.truncations = [{ ...truncated.truncation, omittedScalarCount: truncated.truncation.omittedScalarCount + 1 }];
assert.ok(!truncated.value.endsWith(agentActivityTruncationMarker(truncated.truncation.omittedScalarCount + 1)));
assert.equal((await validateRunAgentResponseDelivery(wrongMarkerCount)).valid, false, "a record whose omitted count disagrees with the visible marker must be rejected");

// task-level-unbound-attempt-does-not-key-identity: the task.succeeded
// envelope carries no `attempt`, so nothing binds the receipt's attemptId at
// that level. A field no validator constrains must not key replay identity,
// or one durable result compares as two replies and a relay posts it twice.
const taskResponseForAttempt = await fixture("valid/task-response.json");
assert.equal("attempt" in taskResponseForAttempt.envelope, false, "the task.succeeded envelope has no attempt to bind against");
const unboundAttempt = structuredClone(taskResponseForAttempt);
unboundAttempt.agentActivity.attemptId = "attempt-from-a-totally-different-task";
assert.equal((await validateTaskAgentResponseDelivery(unboundAttempt)).valid, true, "task level has no authority to reject an unbound attemptId");
assert.notEqual(compareAgentResponseDeliveries(taskResponseForAttempt, unboundAttempt), "distinct", "an unbound field must never make one result look like a second reply");
assert.equal(compareAgentResponseDeliveries(taskResponseForAttempt, unboundAttempt), "conflict");

// semantically-incoherent-nested-envelope-rejected: the nested Runtime v1
// check is the only thing that catches an envelope which passes its schema
// but contradicts itself.
const incoherentEnvelope = structuredClone(runResponse);
incoherentEnvelope.envelope.run.state = "running";
const incoherentResult = await validateRunAgentResponseDelivery(incoherentEnvelope);
assert.equal(incoherentResult.valid, false, "a run.succeeded envelope whose run is still running must be rejected");
assert.ok(incoherentResult.diagnostics.some((item) => item.instancePath.startsWith("/envelope")), "the rejection must come from the nested runtime envelope check");

process.stdout.write("Installed Agent Activity v1 response-delivery conformance passed.\n");
