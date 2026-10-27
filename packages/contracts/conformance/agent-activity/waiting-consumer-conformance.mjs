#!/usr/bin/env node
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  compareAgentWaitingDeliveries,
  validateAgentWaitingDelivery,
  validateExecutorAgentWaitingDelivery,
  validateRunAgentWaitingDelivery,
  validateTaskAgentWaitingDelivery,
} from "../../dist/src/agent-activity/index.js";

const corpus = JSON.parse(await readFile(new URL("./cases.json", import.meta.url), "utf8"));
const cases = corpus.waiting;
assert.equal(corpus.contractVersion, "1.0.0");

async function fixture(relative) {
  return JSON.parse(await readFile(new URL(`../../fixtures/agent-activity/${relative}`, import.meta.url), "utf8"));
}

function mutateElicitations(value, mutation) {
  if (!value || typeof value !== "object") return;
  if (typeof value.decisionId === "string" && Array.isArray(value.options) && typeof value.recommendedOptionId === "string") mutation(value);
  for (const child of Object.values(value)) mutateElicitations(child, mutation);
}

// Every bundled valid fixture round-trips clean through both the
// dispatching validateAgentWaitingDelivery and its own specific validator.
for (const relative of cases.validFixtures) {
  const value = await fixture(relative);
  assert.ok(value.agentActivity.elicitation.options.length === 2 || value.agentActivity.elicitation.options.length === 3);
  assert.deepEqual(value.agentActivity.elicitation.other, relative.endsWith("-fixed-choice.json") ? undefined : { optionId: "other", replyValue: "other:" });
  const dispatched = await validateAgentWaitingDelivery(value);
  assert.equal(dispatched.valid, true, `${relative} (dispatched): ${JSON.stringify(dispatched.diagnostics)}`);
  const specific = value.deliveryType === "executor_agent_waiting" ? await validateExecutorAgentWaitingDelivery(value)
    : value.deliveryType === "run_agent_waiting" ? await validateRunAgentWaitingDelivery(value)
    : await validateTaskAgentWaitingDelivery(value);
  assert.equal(specific.valid, true, `${relative} (specific): ${JSON.stringify(specific.diagnostics)}`);
}

// mismatched-conversation: a receipt's conversation identity that doesn't
// match the envelope's task source must be rejected, not silently passed.
const runWaiting = await fixture("valid/run-waiting.json");
const mismatchedConversation = structuredClone(runWaiting);
mismatchedConversation.agentActivity.conversation = { system: "linear", agentSessionId: "wrong-session" };
const mismatchedResult = await validateRunAgentWaitingDelivery(mismatchedConversation);
assert.equal(mismatchedResult.valid, false, "a receipt with the wrong conversation identity must be rejected");

// activity-id-only-change-is-duplicate: the canonical
// identity tuple must NOT key on activityId -- changing only activityId
// must still compare as "duplicate" (same durable decision), not "distinct".
const changedActivityId = structuredClone(runWaiting);
changedActivityId.agentActivity.activityId = "a-different-activity-id";
assert.equal(compareAgentWaitingDeliveries(runWaiting, changedActivityId), "duplicate", "changing only activityId must not make two deliveries of the same decision compare as distinct");

// conversation-identity-participates-in-canonical-tuple: conversation
// identity is part of the canonical tuple -- two
// deliveries that genuinely differ in conversation identity (while every
// other field, including decisionId, matches) must compare as "distinct"
// (two different waiting decisions that happen to share other fields), not
// silently collapse into the same canonical identity and get judged a
// content "conflict" against each other.
const changedConversation = structuredClone(runWaiting);
changedConversation.agentActivity.conversation = { system: "linear", agentSessionId: "a-different-session" };
changedConversation.envelope.task.sourceRef.agentSessionId = "a-different-session";
assert.equal((await validateRunAgentWaitingDelivery(changedConversation)).valid, true, "the changed conversation must remain coherent");
assert.equal(compareAgentWaitingDeliveries(runWaiting, changedConversation), "distinct", "a genuine conversation identity difference must compare as distinct, not conflict");

// A renderer and reply consumer must agree on elicitation content under one decision ID. Compare two
// independently valid deliveries with coherent but different content to prove replay conflict.
const driftedQuestion = structuredClone(runWaiting);
const driftedQuestionText = "A different question entirely.";
driftedQuestion.agentActivity.elicitation.question = driftedQuestionText;
driftedQuestion.envelope.elicitation.question = driftedQuestionText;
driftedQuestion.envelope.run.elicitation.question = driftedQuestionText;
const driftedValidation = await validateRunAgentWaitingDelivery(driftedQuestion);
assert.equal(driftedValidation.valid, true, `the drifted delivery must be internally coherent on its own: ${JSON.stringify(driftedValidation.diagnostics)}`);
assert.equal(compareAgentWaitingDeliveries(runWaiting, driftedQuestion), "conflict", "two independently valid deliveries under the same decisionId that differ in elicitation content must compare as conflict");

const mismatchedOther = structuredClone(runWaiting);
delete mismatchedOther.agentActivity.elicitation.other;
assert.equal((await validateRunAgentWaitingDelivery(mismatchedOther)).valid, false, "envelope and Agent Activity must agree on Other capability");

for (const field of ["optionId", "replyValue"]) {
  const collidingOther = structuredClone(runWaiting);
  mutateElicitations(collidingOther, (elicitation) => { elicitation.other[field] = elicitation.options[0][field]; });
  assert.equal((await validateRunAgentWaitingDelivery(collidingOther)).valid, false, `Other ${field} must be distinct from every explicit option`);
}

const recommendedOther = structuredClone(runWaiting);
mutateElicitations(recommendedOther, (elicitation) => { elicitation.recommendedOptionId = elicitation.other.optionId; });
assert.equal((await validateRunAgentWaitingDelivery(recommendedOther)).valid, false, "Other must never be recommended");

const driftedOther = structuredClone(runWaiting);
mutateElicitations(driftedOther, (elicitation) => { elicitation.other.replyValue = "custom:"; });
assert.equal((await validateRunAgentWaitingDelivery(driftedOther)).valid, true);
assert.equal(compareAgentWaitingDeliveries(runWaiting, driftedOther), "conflict", "Other participates in durable delivery content equality");

process.stdout.write("Installed Agent Activity v1 waiting-delivery conformance passed.\n");
