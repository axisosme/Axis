import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fromRuntimePackageRoot } from "../src/runtime-paths.js";
import {
  agentActivityContractVersion,
  agentActivityFallbackResponseBody,
  agentActivitySchemaIds,
  agentActivityTruncationMarker,
  compareAgentResponseDeliveries,
  createAgentActivitySchemaRegistry,
  truncateAgentActivityText,
  validateAgentActivitySourceEvent,
  validateAgentActivityResponseReceipt,
  validateAgentResponseDelivery,
  validateExecutorAgentResponseDelivery,
  validateRunAgentResponseDelivery,
  validateTaskAgentResponseDelivery,
} from "../src/agent-activity/index.js";

async function fixture(relative: string): Promise<any> {
  return JSON.parse(await readFile(fromRuntimePackageRoot("fixtures", "agent-activity", relative), "utf8"));
}

async function conformanceCases(): Promise<{ contractVersion: string; deliveryTypes: string[]; validFixtures: string[]; requiredMutations: string[] }> {
  const corpus = JSON.parse(await readFile(fromRuntimePackageRoot("conformance", "agent-activity", "cases.json"), "utf8"));
  return { contractVersion: corpus.contractVersion, ...corpus.response };
}

const specificValidator = (deliveryType: string) => deliveryType === "executor_agent_response" ? validateExecutorAgentResponseDelivery
  : deliveryType === "run_agent_response" ? validateRunAgentResponseDelivery
  : validateTaskAgentResponseDelivery;

test("every bundled response-delivery fixture validates through both the dispatching and specific validators", async () => {
  const cases = await conformanceCases();
  assert.equal(cases.contractVersion, agentActivityContractVersion);
  assert.equal(cases.deliveryTypes.length, 3);
  const deliveryTypes = new Set<string>();
  for (const relative of cases.validFixtures) {
    const value = await fixture(relative);
    deliveryTypes.add(value.deliveryType);
    const dispatched = await validateAgentResponseDelivery(value);
    assert.equal(dispatched.valid, true, `${relative}: ${JSON.stringify(dispatched.diagnostics)}`);
    const specific = await specificValidator(value.deliveryType)(value);
    assert.equal(specific.valid, true, `${relative} (specific): ${JSON.stringify(specific.diagnostics)}`);
  }
  assert.deepEqual([...deliveryTypes].sort(), ["executor_agent_response", "run_agent_response", "task_agent_response"]);
  // The declared corpus contract is pinned, so deleting or renaming a mutation
  // block in the consumer script cannot silently shrink stated coverage.
  assert.deepEqual(cases.requiredMutations, [
    "unknown-envelope-version-rejected",
    "mismatched-conversation",
    "result-ref-must-match-envelope",
    "activity-id-only-change-is-duplicate",
    "conversation-identity-participates-in-canonical-tuple",
    "coherent-body-drift-is-conflict",
    "fallback-body-must-be-the-neutral-constant",
    "truncation-marker-without-record",
    "truncation-record-without-marker",
    "truncation-counts-must-be-coherent",
    "task-level-unbound-attempt-does-not-key-identity",
    "semantically-incoherent-nested-envelope-rejected",
  ]);
});

// Response fixtures and composite schemas must both bind the Runtime envelope.
test("response deliveries wrap the current runtime succeeded envelopes", async () => {
  const cases = await conformanceCases();
  for (const relative of cases.validFixtures) {
    const value = await fixture(relative);
    assert.equal(value.envelope.contractVersion, "1.0.0", relative);
  }
  const registry = await createAgentActivitySchemaRegistry();
  for (const [name, envelopeSchema] of [
    ["run-agent-response-delivery", "run-succeeded-envelope"],
    ["task-agent-response-delivery", "task-succeeded-envelope"],
    ["executor-agent-response-delivery", "executor-succeeded-envelope"],
  ] as const) {
    const schema = registry.ajv.getSchema(agentActivitySchemaIds[name])?.schema as { properties: { envelope: { $ref: string } } };
    assert.equal(
      schema.properties.envelope.$ref,
      `https://github.com/axisosme/Axis/schemas/runtime/${envelopeSchema}.schema.json`,
      `${name} must $ref the succeeded envelope`,
    );
  }
});

// Validate unknown envelope discriminators independently at Run and Task levels.
test("response deliveries reject succeeded envelopes with an unknown runtime discriminator", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const taskResponse = await fixture("valid/task-response.json");
  for (const invalidVersion of ["fictional-contract"]) {
    for (const [label, base, validate] of [
      ["run", runResponse, validateRunAgentResponseDelivery],
      ["task", taskResponse, validateTaskAgentResponseDelivery],
    ] as const) {
      const invalid = structuredClone(base);
      invalid.envelope.contractVersion = invalidVersion;
      invalid.envelope.task.contractVersion = invalidVersion;
      if (invalid.envelope.attempt) invalid.envelope.attempt.contractVersion = invalidVersion;
      if (invalid.envelope.run?.contractVersion) invalid.envelope.run.contractVersion = invalidVersion;
      const result = await validate(invalid);
      assert.equal(result.valid, false, `${label}-level must reject a ${invalidVersion} envelope`);
      assert.ok(
        result.diagnostics.some((item) => item.code === "schema_violation" && item.instancePath.startsWith("/envelope")),
        `${label}-level rejection of ${invalidVersion} must come from the envelope`,
      );
    }
  }
});

test("response conversation identities are closed Linear agent sessions", async () => {
  const runResponse = await fixture("valid/run-response.json");
  for (const conversation of [
    { system: "unsupported", agentSessionId: "session-1" },
    { system: "linear" },
    { system: "linear", agentSessionId: "session-1", unexpected: "extra" },
  ]) {
    const value = structuredClone(runResponse);
    value.agentActivity.conversation = conversation;
    const result = await validateRunAgentResponseDelivery(value);
    assert.equal(result.valid, false);
    assert.ok(result.diagnostics.some((item) => item.code === "schema_violation"));
  }
});

test("a run-level response receipt binds task, run, attempt, conversation, operation, and result to the envelope", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const mutations: Array<[string, (value: any) => void]> = [
    ["taskId", (value) => { value.agentActivity.taskId = "other-task"; }],
    ["runId", (value) => { value.agentActivity.runId = "other-run"; }],
    ["attemptId", (value) => { value.agentActivity.attemptId = "other-attempt"; }],
    ["operationId", (value) => { value.agentActivity.operationId = "other-operation"; }],
    ["resultRef", (value) => { value.agentActivity.resultRef = "other-result"; }],
    ["conversation", (value) => { value.agentActivity.conversation = { system: "linear", agentSessionId: "other-session" }; }],
  ];
  for (const [label, mutate] of mutations) {
    const value = structuredClone(runResponse);
    mutate(value);
    const result = await validateRunAgentResponseDelivery(value);
    assert.equal(result.valid, false, `a receipt whose ${label} disagrees with the envelope must be rejected`);
    assert.ok(result.diagnostics.some((item) => item.code === "identity_mismatch"), `${label} must be reported as an identity mismatch`);
  }
});

// A linear sourceRef may legally omit agentSessionId (it is not required by
// the schema), so the delivery has to fail closed rather than read
// undefined as a match.
test("a linear task source without an agent session identifier cannot resolve a conversation", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const value = structuredClone(runResponse);
  delete value.envelope.task.sourceRef.agentSessionId;
  const result = await validateRunAgentResponseDelivery(value);
  assert.equal(result.valid, false);
  assert.ok(result.diagnostics.some((item) => item.instancePath === "/envelope/task/sourceRef"));
});

test("the fallback response body is the provider-neutral constant", async () => {
  assert.equal(/codex/iu.test(agentActivityFallbackResponseBody), false);
  const taskFallback = await fixture("valid/task-fallback.json");
  assert.equal(taskFallback.agentActivity.content.body, agentActivityFallbackResponseBody);
  const invalidFallback = structuredClone(taskFallback);
  invalidFallback.agentActivity.content.body = "This is not the required fallback response.";
  assert.equal((await validateTaskAgentResponseDelivery(invalidFallback)).valid, false, "a fallback body that differs from the required constant must be rejected");
  const truncatedFallback = structuredClone(taskFallback);
  truncatedFallback.agentActivity.truncated = true;
  assert.equal((await validateTaskAgentResponseDelivery(truncatedFallback)).valid, false, "a fallback response must never claim truncation");
});

test("truncation reporting requires the visible marker and its structured record together", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const truncated = truncateAgentActivityText("x".repeat(9000));
  assert.ok(truncated.truncation);
  assert.equal(truncated.truncation.fieldPath, "/content/body");
  assert.equal(Array.from(truncated.value).length, 4096);

  const coherent = structuredClone(runResponse);
  coherent.agentActivity.content.body = truncated.value;
  coherent.agentActivity.truncated = true;
  coherent.agentActivity.truncations = [truncated.truncation];
  const coherentResult = await validateRunAgentResponseDelivery(coherent);
  assert.equal(coherentResult.valid, true, JSON.stringify(coherentResult.diagnostics));

  const markerWithoutRecord = structuredClone(coherent);
  markerWithoutRecord.agentActivity.truncated = false;
  markerWithoutRecord.agentActivity.truncations = [];
  assert.equal((await validateRunAgentResponseDelivery(markerWithoutRecord)).valid, false);

  const recordWithoutMarker = structuredClone(coherent);
  recordWithoutMarker.agentActivity.content.body = "A short answer.";
  assert.equal((await validateRunAgentResponseDelivery(recordWithoutMarker)).valid, false);

  const incoherentCounts = structuredClone(coherent);
  incoherentCounts.agentActivity.truncations = [{ ...truncated.truncation, originalScalarCount: truncated.truncation.originalScalarCount + 500 }];
  assert.equal((await validateRunAgentResponseDelivery(incoherentCounts)).valid, false);

  const flagWithoutRecords = structuredClone(runResponse);
  flagWithoutRecords.agentActivity.truncated = true;
  assert.equal((await validateRunAgentResponseDelivery(flagWithoutRecords)).valid, false);
});

test("response display text stays bounded, sanitized, and free of unredacted credentials", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const unsafe: Array<[string, string]> = [
    ["a credential value", "The token is ghp_abcdefghijklmnopqrstuvwxyz012345."],
    ["a bidirectional control", "Answer‮evisrever"],
    ["a CR line ending", "Answer\r\nsecond line"],
    ["a disallowed control character", "Answer\u0007bell"],
  ];
  for (const [label, body] of unsafe) {
    const value = structuredClone(runResponse);
    value.agentActivity.content.body = body;
    const result = await validateRunAgentResponseDelivery(value);
    assert.equal(result.valid, false, `${label} must be rejected`);
  }
  const overlong = structuredClone(runResponse);
  overlong.agentActivity.content.body = "x".repeat(4097);
  assert.equal((await validateRunAgentResponseDelivery(overlong)).valid, false, "an untruncated overlong body must be rejected");
});

test("replay comparison keys on the durable result rather than the per-delivery activity identifier", async () => {
  const runResponse = await fixture("valid/run-response.json");

  const sameResultFreshActivity = structuredClone(runResponse);
  sameResultFreshActivity.agentActivity.activityId = "activity-2";
  assert.equal(compareAgentResponseDeliveries(runResponse, sameResultFreshActivity), "duplicate");

  const differentResult = structuredClone(runResponse);
  differentResult.agentActivity.resultRef = "result-run-2";
  differentResult.envelope.resultRef = "result-run-2";
  differentResult.envelope.run.resultRef = "result-run-2";
  assert.equal((await validateRunAgentResponseDelivery(differentResult)).valid, true, "the re-pointed delivery must be internally coherent");
  assert.equal(compareAgentResponseDeliveries(runResponse, differentResult), "distinct");

  const differentConversation = structuredClone(runResponse);
  differentConversation.agentActivity.conversation = { system: "linear", agentSessionId: "session-2" };
  differentConversation.envelope.task.sourceRef.agentSessionId = "session-2";
  assert.equal((await validateRunAgentResponseDelivery(differentConversation)).valid, true);
  assert.equal(compareAgentResponseDeliveries(runResponse, differentConversation), "distinct");

  // The succeeded envelope carries no body, so body drift is
  // invisible to envelope/receipt coherence -- only the comparator sees it.
  const driftedBody = structuredClone(runResponse);
  driftedBody.agentActivity.content.body = "A materially different answer.";
  assert.equal((await validateRunAgentResponseDelivery(driftedBody)).valid, true);
  assert.equal(compareAgentResponseDeliveries(runResponse, driftedBody), "conflict");

  const differentDeliveryType = structuredClone(await fixture("valid/task-response.json"));
  assert.equal(compareAgentResponseDeliveries(runResponse, differentDeliveryType), "distinct");
});

test("the standalone response receipt validator and durable source-event comparison are exported", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const receipt = runResponse.agentActivity;
  assert.equal((await validateAgentActivityResponseReceipt(receipt)).valid, true);
  assert.equal(validateAgentActivitySourceEvent(receipt, receipt.sourceEventId).valid, true);
  const mismatched = validateAgentActivitySourceEvent(receipt, "a-different-event");
  assert.equal(mismatched.valid, false);
  assert.equal(mismatched.diagnostics[0]?.instancePath, "/sourceEventId");

  const wrongContractVersion = structuredClone(receipt);
  wrongContractVersion.contractVersion = "unsupported-contract";
  assert.equal((await validateAgentActivityResponseReceipt(wrongContractVersion)).valid, false);
});

test("the registry carries the envelope family alongside its own schemas", async () => {
  const registry = await createAgentActivitySchemaRegistry();
  const ids = [...registry.schemaIds];
  assert.equal(ids.filter((id) => id.includes("/schemas/agent-activity/")).length, 8);
  assert.ok(ids.some((id) => id.endsWith("/schemas/runtime/run-succeeded-envelope.schema.json")));
  assert.ok(ids.some((id) => id.endsWith("/schemas/runtime/task-succeeded-envelope.schema.json")));
  assert.ok(ids.some((id) => id.endsWith("/schemas/runtime/executor-succeeded-envelope.schema.json")));
  assert.equal(new Set(ids).size, ids.length);
});

// Task success has no attempt authority. Different unbound attempt IDs on the same result must compare as
// content conflict rather than distinct replies.
test("an envelope-unbound task-level attemptId cannot make one result look like two replies", async () => {
  const taskResponse = await fixture("valid/task-response.json");
  assert.equal("attempt" in taskResponse.envelope, false, "the task.succeeded envelope has no attempt to bind against");

  const otherAttempt = structuredClone(taskResponse);
  otherAttempt.agentActivity.attemptId = "attempt-from-a-totally-different-task";
  assert.equal((await validateTaskAgentResponseDelivery(otherAttempt)).valid, true, "task level has no authority to reject this, and must not pretend otherwise");
  assert.notEqual(
    compareAgentResponseDeliveries(taskResponse, otherAttempt),
    "distinct",
    "the same durable result must never compare as a second, new reply on the strength of an unbound field",
  );
  assert.equal(compareAgentResponseDeliveries(taskResponse, otherAttempt), "conflict");

  // The levels whose envelope DOES carry an attempt still bind it, and there
  // attemptId legitimately keys identity.
  for (const [relative, validate] of [
    ["valid/run-response.json", validateRunAgentResponseDelivery],
    ["valid/executor-response.json", validateExecutorAgentResponseDelivery],
  ] as const) {
    const base = await fixture(relative);
    const mutated = structuredClone(base);
    mutated.agentActivity.attemptId = "attempt-from-a-totally-different-task";
    assert.equal((await validate(mutated)).valid, false, `${relative} must reject an attemptId the envelope does not carry`);
  }
  const runResponse = await fixture("valid/run-response.json");
  const runOtherAttempt = structuredClone(runResponse);
  runOtherAttempt.agentActivity.attemptId = "attempt-2";
  runOtherAttempt.envelope.attempt.attemptId = "attempt-2";
  runOtherAttempt.envelope.run.attemptId = "attempt-2";
  assert.equal((await validateRunAgentResponseDelivery(runOtherAttempt)).valid, true, "the re-pointed run delivery must be internally coherent");
  assert.equal(compareAgentResponseDeliveries(runResponse, runOtherAttempt), "distinct", "a bound attempt difference is a genuine identity difference");
});

// Keep omitted-count digit widths equal so arithmetic remains coherent and only the visible truncation marker disagrees.
test("a truncation record whose visible marker disagrees is caught by the marker guard alone", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const claimed = 4982;
  const recorded = 4981;
  assert.equal(String(claimed).length, String(recorded).length, "the two counts must render the same width or the arithmetic check would fire instead");

  const claimedMarker = agentActivityTruncationMarker(claimed);
  const recordedMarker = agentActivityTruncationMarker(recorded);
  assert.equal(Array.from(claimedMarker).length, Array.from(recordedMarker).length);
  const retained = 4096 - Array.from(recordedMarker).length;

  const value = structuredClone(runResponse);
  value.agentActivity.content.body = `${"y".repeat(retained)}${claimedMarker}`;
  value.agentActivity.truncated = true;
  value.agentActivity.truncations = [{ fieldPath: "/content/body", truncated: true, originalScalarCount: retained + recorded, omittedScalarCount: recorded }];

  const result = await validateRunAgentResponseDelivery(value);
  assert.equal(result.valid, false, "a body whose visible marker contradicts its record must be rejected");
  assert.deepEqual(
    result.diagnostics.map((item) => item.safeMessage),
    ["A truncated field must end with the exact visible Axis truncation marker."],
    "this input must be caught by the marker guard and nothing else",
  );
});

// NFC and lone-surrogate validation must each reject invalid response text.
test("response display text must be NFC and free of lone surrogates", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const decomposed = "Cafe\u0301 finished the work.";
  assert.notEqual(decomposed.normalize("NFC"), decomposed, "the fixture text must actually be non-NFC");
  const nfc = structuredClone(runResponse);
  nfc.agentActivity.content.body = decomposed;
  const nfcResult = await validateRunAgentResponseDelivery(nfc);
  assert.equal(nfcResult.valid, false, "non-NFC display text must be rejected");
  assert.ok(nfcResult.diagnostics.some((item) => item.safeMessage.includes("NFC")));

  const lone = structuredClone(runResponse);
  lone.agentActivity.content.body = "Answer \ud800 truncated";
  const loneResult = await validateRunAgentResponseDelivery(lone);
  assert.equal(loneResult.valid, false, "a lone surrogate must be rejected");
  assert.ok(loneResult.diagnostics.some((item) => item.safeMessage.includes("Unicode scalar")));
});

// Validate a schema-valid but semantically incoherent nested Runtime envelope.
test("a schema-valid but semantically incoherent runtime envelope is rejected", async () => {
  const runResponse = await fixture("valid/run-response.json");
  const incoherent = structuredClone(runResponse);
  incoherent.envelope.run.state = "running";
  const result = await validateRunAgentResponseDelivery(incoherent);
  assert.equal(result.valid, false, "a run.succeeded envelope whose run is still running must be rejected");
  assert.ok(
    result.diagnostics.some((item) => item.instancePath.startsWith("/envelope")),
    "the rejection must come from the nested runtime envelope check",
  );
});
