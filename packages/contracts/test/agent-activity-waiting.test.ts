import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fromRuntimePackageRoot } from "../src/runtime-paths.js";
import {
  compareAgentWaitingDeliveries,
  validateAgentWaitingDelivery,
  validateExecutorAgentWaitingDelivery,
  validateRunAgentWaitingDelivery,
  validateTaskAgentWaitingDelivery,
} from "../src/agent-activity/index.js";
import { validateRuntimeBoundaryMessage } from "../src/runtime/index.js";

async function fixture(relative: string): Promise<any> {
  return JSON.parse(await readFile(fromRuntimePackageRoot("fixtures", "agent-activity", relative), "utf8"));
}

async function conformanceCases(): Promise<{ contractVersion: string; deliveryTypes: string[]; validFixtures: string[] }> {
  const corpus = JSON.parse(await readFile(fromRuntimePackageRoot("conformance", "agent-activity", "cases.json"), "utf8"));
  return { contractVersion: corpus.contractVersion, ...corpus.waiting };
}

function mutateElicitations(value: any, mutation: (elicitation: any) => void): void {
  if (!value || typeof value !== "object") return;
  if (typeof value.decisionId === "string" && Array.isArray(value.options) && typeof value.recommendedOptionId === "string") mutation(value);
  for (const child of Object.values(value)) mutateElicitations(child, mutation);
}

test("every bundled waiting-delivery fixture validates through both the dispatching and specific validators", async () => {
  const cases = await conformanceCases();
  assert.equal(cases.deliveryTypes.length, 3);
  const explicitOptionCounts = new Set<number>();
  for (const relative of cases.validFixtures) {
    const value = await fixture(relative);
    explicitOptionCounts.add(value.agentActivity.elicitation.options.length);
    assert.deepEqual(value.agentActivity.elicitation.other, relative.endsWith("-fixed-choice.json") ? undefined : { optionId: "other", replyValue: "other:" });
    const dispatched = await validateAgentWaitingDelivery(value);
    assert.equal(dispatched.valid, true, `${relative}: ${JSON.stringify(dispatched.diagnostics)}`);
    const specific = value.deliveryType === "executor_agent_waiting" ? await validateExecutorAgentWaitingDelivery(value)
      : value.deliveryType === "run_agent_waiting" ? await validateRunAgentWaitingDelivery(value)
      : await validateTaskAgentWaitingDelivery(value);
    assert.equal(specific.valid, true, `${relative} (specific): ${JSON.stringify(specific.diagnostics)}`);
  }
  assert.deepEqual([...explicitOptionCounts].sort(), [2, 3]);
});

test("Runtime validates Other semantics on both run and task waiting envelopes", async () => {
  for (const relative of ["valid/run-waiting.json", "valid/task-waiting.json"]) {
    const delivery = await fixture(relative);
    const result = await validateRuntimeBoundaryMessage(delivery.envelope);
    assert.equal(result.valid, true, `${relative}: ${JSON.stringify(result.diagnostics)}`);
  }
});

test("Other is a closed capability marker and cannot carry answer or display fields", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  for (const mutation of [
    (other: any) => { delete other.replyValue; },
    (other: any) => { other.label = "Other"; },
    (other: any) => { other.description = "Write another answer"; },
    (other: any) => { other.answerText = "private answer"; },
  ]) {
    const malformed = structuredClone(runWaiting);
    mutateElicitations(malformed, (elicitation) => mutation(elicitation.other));
    const result = await validateRunAgentWaitingDelivery(malformed);
    assert.equal(result.valid, false, "malformed or unknown Other fields must be rejected");
    assert.ok(result.diagnostics.some((item) => item.code === "schema_violation"));
  }
});

test("Other optionId and replyValue must be distinct from every explicit option", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  for (const field of ["optionId", "replyValue"] as const) {
    const colliding = structuredClone(runWaiting);
    mutateElicitations(colliding, (elicitation) => { elicitation.other[field] = elicitation.options[1][field]; });
    const result = await validateRunAgentWaitingDelivery(colliding);
    assert.equal(result.valid, false);
    assert.ok(result.diagnostics.some((item) => item.instancePath.endsWith(`/other/${field}`)));
  }
});

test("recommendation remains the first explicit option and cannot select Other", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  for (const recommendation of ["opt-b", "other"]) {
    const invalid = structuredClone(runWaiting);
    mutateElicitations(invalid, (elicitation) => { elicitation.recommendedOptionId = recommendation; });
    const result = await validateRunAgentWaitingDelivery(invalid);
    assert.equal(result.valid, false);
    assert.ok(result.diagnostics.some((item) => item.instancePath.endsWith("/recommendedOptionId")));
  }
});

test("delivery rejects an Other capability mismatch between envelope and Agent Activity", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  const missing = structuredClone(runWaiting);
  delete missing.agentActivity.elicitation.other;
  assert.equal((await validateRunAgentWaitingDelivery(missing)).valid, false);

  const drifted = structuredClone(runWaiting);
  drifted.agentActivity.elicitation.other.replyValue = "different:";
  assert.equal((await validateRunAgentWaitingDelivery(drifted)).valid, false);
});

test("waiting deliveries reject unsupported contract versions", async () => {
  const waiting = await fixture("valid/run-waiting.json");
  for (const contractVersion of ["fictional-contract"]) {
    assert.equal((await validateAgentWaitingDelivery({ ...waiting, contractVersion })).valid, false);
    assert.equal((await validateRunAgentWaitingDelivery({ ...waiting, agentActivity: { ...waiting.agentActivity, contractVersion } })).valid, false);
    assert.equal((await validateRunAgentWaitingDelivery({ ...waiting, envelope: { ...waiting.envelope, contractVersion } })).valid, false);
  }
});

test("waiting deliveries require closed Linear source and conversation identities", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  const mutations: Array<[string, (value: any) => void]> = [
    ["source system", (value) => { value.envelope.task.sourceRef.system = "unsupported"; }],
    ["source organization", (value) => { delete value.envelope.task.sourceRef.organizationId; }],
    ["source issue", (value) => { delete value.envelope.task.sourceRef.issueId; }],
    ["source extension", (value) => { value.envelope.task.sourceRef.unexpected = "extra"; }],
    ["conversation system", (value) => { value.agentActivity.conversation.system = "unsupported"; }],
    ["conversation session", (value) => { delete value.agentActivity.conversation.agentSessionId; }],
    ["conversation extension", (value) => { value.agentActivity.conversation.unexpected = "extra"; }],
  ];
  for (const [label, mutate] of mutations) {
    const value = structuredClone(runWaiting);
    mutate(value);
    const result = await validateRunAgentWaitingDelivery(value);
    assert.equal(result.valid, false, `${label} must be rejected`);
    assert.ok(result.diagnostics.some((item) => item.code === "schema_violation"), `${label} must fail schema validation`);
  }
});

test("a receipt with a conversation identity mismatched against the envelope's task source is rejected", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  const mismatched = structuredClone(runWaiting);
  mismatched.agentActivity.conversation = { system: "linear", agentSessionId: "wrong-session" };
  const result = await validateRunAgentWaitingDelivery(mismatched);
  assert.equal(result.valid, false);
});

// Replay identity excludes per-retry activityId and includes conversation identity.
test("changing only activityId compares as a duplicate of the same durable decision", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  const changed = structuredClone(runWaiting);
  changed.agentActivity.activityId = "a-different-activity-id";
  assert.equal(compareAgentWaitingDeliveries(runWaiting, changed), "duplicate");
});

test("a genuine conversation identity difference compares as distinct, not conflict", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  const changed = structuredClone(runWaiting);
  changed.agentActivity.conversation = { system: "linear", agentSessionId: "a-different-session" };
  changed.envelope.task.sourceRef.agentSessionId = "a-different-session";
  assert.equal((await validateRunAgentWaitingDelivery(changed)).valid, true, "the changed conversation must remain coherent");
  assert.equal(compareAgentWaitingDeliveries(runWaiting, changed), "distinct");
});

// Validation rejects a delivery whose receipt and envelope disagree on elicitation content.
test("a single delivery with mismatched envelope/receipt elicitation content is rejected", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  const drifted = structuredClone(runWaiting);
  drifted.agentActivity.elicitation.question = "A different question entirely.";
  const result = await validateRunAgentWaitingDelivery(drifted);
  assert.equal(result.valid, false);
});

// Two independently valid deliveries with different elicitation content under one identity must compare as conflict.
test("two independently valid deliveries with drifted elicitation content compare as conflict", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  const drifted = structuredClone(runWaiting);
  const driftedQuestion = "A different question entirely.";
  drifted.agentActivity.elicitation.question = driftedQuestion;
  drifted.envelope.elicitation.question = driftedQuestion;
  drifted.envelope.run.elicitation.question = driftedQuestion;
  const validation = await validateRunAgentWaitingDelivery(drifted);
  assert.equal(validation.valid, true, JSON.stringify(validation.diagnostics));
  assert.equal(compareAgentWaitingDeliveries(runWaiting, drifted), "conflict");
});

test("two independently valid deliveries with different Other reply prefixes compare as conflict", async () => {
  const runWaiting = await fixture("valid/run-waiting.json");
  const drifted = structuredClone(runWaiting);
  mutateElicitations(drifted, (elicitation) => { elicitation.other.replyValue = "custom:"; });
  const validation = await validateRunAgentWaitingDelivery(drifted);
  assert.equal(validation.valid, true, JSON.stringify(validation.diagnostics));
  assert.equal(compareAgentWaitingDeliveries(runWaiting, drifted), "conflict");
});
