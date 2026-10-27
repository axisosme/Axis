import { isDeepStrictEqual } from "node:util";
import type { ErrorObject } from "ajv";
import { sameElicitationContent, structuredElicitationDiagnostics, validateRuntimeBoundaryMessage } from "../runtime/validate.js";
import { agentActivitySchemaIds, agentActivityValidatorFor, createAgentActivitySchemaRegistry, type AgentActivitySchemaName } from "./registry.js";
import {
  agentActivityFallbackResponseBody, agentActivityMaxDisplayScalars,
  type AgentActivityTruncation, type AgentActivityResponseReceipt, type AgentResponseDelivery,
  type AgentActivityElicitationReceipt, type AgentWaitingDelivery, type ConversationIdentity,
  type ExecutorAgentWaitingDelivery, type RunAgentWaitingDelivery, type TaskAgentWaitingDelivery,
  type ExecutorAgentResponseDelivery, type RunAgentResponseDelivery, type TaskAgentResponseDelivery,
} from "./types.generated.js";
import { memoizedRegistry } from "../registry-cache.js";

/** every validation in this module shares one compiled registry; see `registry-cache.ts`. */
const sharedRegistry = memoizedRegistry(createAgentActivitySchemaRegistry);

export type AgentActivityDiagnosticCode = "schema_violation" | "unsafe_content" | "invalid_truncation" | "identity_mismatch";
export interface AgentActivityDiagnostic { code: AgentActivityDiagnosticCode; instancePath: string; schemaId: string | null; keyword: string | null; safeMessage: string; }
export interface AgentActivityValidationResult<T> { valid: boolean; value: T | null; diagnostics: AgentActivityDiagnostic[]; }
export type AgentActivityReplayResult = "distinct" | "duplicate" | "conflict";

const maxDiagnostics = 32;
const bidi = /[\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]/u;
const disallowedControl = /[\u0000-\u0009\u000b-\u001f\u007f-\u009f]/u;
const secretLike = /(?:SYNTHETIC_(?:SECRET|TOKEN)_MARKER|\b(?:api[_-]?key|cookie|password|secret|token)\s*[:=]\s*(?!\[REDACTED\])\S+|\bauthorization\s*:\s*(?:bearer|basic)\s+(?!\[REDACTED\])\S+|\bbearer\s+(?!\[REDACTED\])[A-Za-z0-9._~+/-]{8,}|\bsk_(?:live|test)_[A-Za-z0-9]{16,}\b|\bghp_[A-Za-z0-9]{20,}\b|\bgithub_pat_[A-Za-z0-9_]{20,}\b|\bAKIA[0-9A-Z]{16}\b|\bxox[a-zA-Z]-[A-Za-z0-9-]{10,}\b|\bglpat-[A-Za-z0-9_-]{16,}\b|\b[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----)/iu;

function diagnostic(code: AgentActivityDiagnosticCode, instancePath: string, safeMessage: string, schemaId: string | null = null, keyword: string | null = null): AgentActivityDiagnostic {
  return { code, instancePath, schemaId, keyword, safeMessage };
}

function schemaDiagnostics(errors: ErrorObject[] | null | undefined, schemaId: string): AgentActivityDiagnostic[] {
  return (errors ?? []).map((error) => diagnostic("schema_violation", error.instancePath, "The value does not match the closed agent-activity schema.", schemaId, error.keyword));
}

function hasOnlyUnicodeScalars(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) return false;
  }
  return true;
}

function elicitationReceiptDiagnostics(receipt: AgentActivityElicitationReceipt, path = ""): AgentActivityDiagnostic[] {
  const result: AgentActivityDiagnostic[] = [];
  const { elicitation } = receipt;
  result.push(...textDiagnostics(elicitation.header, `${path}/elicitation/header`));
  result.push(...textDiagnostics(elicitation.question, `${path}/elicitation/question`));
  for (const [index, option] of elicitation.options.entries()) {
    result.push(...textDiagnostics(option.label, `${path}/elicitation/options/${index}/label`));
    result.push(...textDiagnostics(option.description, `${path}/elicitation/options/${index}/description`));
  }
  result.push(...structuredElicitationDiagnostics(elicitation, `${path}/elicitation`).map((item) =>
    diagnostic("identity_mismatch", item.instancePath, item.safeMessage)));
  return result;
}

async function validateSchema<T>(name: AgentActivitySchemaName, value: unknown): Promise<AgentActivityValidationResult<T>> {
  const registry = await sharedRegistry();
  const validate = agentActivityValidatorFor(registry, name);
  const diagnostics = validate(value) ? [] : schemaDiagnostics(validate.errors, agentActivitySchemaIds[name]);
  return finish(value, diagnostics);
}

function finish<T>(value: unknown, diagnostics: AgentActivityDiagnostic[]): AgentActivityValidationResult<T> {
  const sorted = [...diagnostics].sort((left, right) => left.instancePath.localeCompare(right.instancePath) || left.code.localeCompare(right.code)).slice(0, maxDiagnostics);
  return { valid: sorted.length === 0, value: sorted.length === 0 ? value as T : null, diagnostics: sorted };
}

export async function validateAgentActivityElicitationReceipt(value: unknown): Promise<AgentActivityValidationResult<AgentActivityElicitationReceipt>> {
  const schema = await validateSchema<AgentActivityElicitationReceipt>("agent-activity-elicitation-receipt", value);
  if (!schema.valid || !schema.value) return schema;
  return finish(schema.value, elicitationReceiptDiagnostics(schema.value));
}

function mismatch(actual: string, expected: string, path: string, message: string): AgentActivityDiagnostic[] {
  return actual === expected ? [] : [diagnostic("identity_mismatch", path, message)];
}

function sameConversationIdentity(receiptConversation: ConversationIdentity, sourceConversation: ConversationIdentity): boolean {
  return receiptConversation.system === sourceConversation.system && receiptConversation.agentSessionId === sourceConversation.agentSessionId;
}

function commonWaitingDeliveryDiagnostics(
  delivery: AgentWaitingDelivery,
  taskId: string,
  runId: string,
  sourceConversation: ConversationIdentity,
  operationId: string,
): AgentActivityDiagnostic[] {
  const receipt = delivery.agentActivity;
  const diagnostics = elicitationReceiptDiagnostics(receipt, "/agentActivity");
  diagnostics.push(...mismatch(receipt.taskId, taskId, "/agentActivity/taskId", "The elicitation activity must reference the delivered task."));
  diagnostics.push(...mismatch(receipt.runId, runId, "/agentActivity/runId", "The elicitation activity must reference the delivered run."));
  if (!sameConversationIdentity(receipt.conversation, sourceConversation)) {
    diagnostics.push(diagnostic("identity_mismatch", "/agentActivity/conversation", "The elicitation activity must use the task source's conversation identity."));
  }
  diagnostics.push(...mismatch(receipt.operationId, operationId, "/agentActivity/operationId", "The elicitation activity and waiting envelope must use the same operation identifier."));
  // Compare the complete structured elicitation so the renderer and reply consumer agree on question and
  // option content under the same decision ID.
  if (!sameElicitationContent(receipt.elicitation, delivery.envelope.elicitation)) {
    diagnostics.push(diagnostic("identity_mismatch", "/agentActivity/elicitation", "The elicitation activity must carry the same structured elicitation content as the waiting envelope."));
  }
  return diagnostics;
}

function conversationFromSourceRef(sourceRef: { system: "linear"; agentSessionId?: string }): ConversationIdentity | null {
  return sourceRef.agentSessionId === undefined ? null : { system: "linear", agentSessionId: sourceRef.agentSessionId };
}

async function nestedEnvelopeDiagnostics(envelope: unknown): Promise<AgentActivityDiagnostic[]> {
  const result = await validateRuntimeBoundaryMessage(envelope);
  return result.diagnostics.map((item) => diagnostic(
    item.code === "schema_violation" ? "schema_violation" : "identity_mismatch",
    `/envelope${item.instancePath}`,
    "The nested Runtime v1 waiting_for_input envelope is not semantically coherent.",
    item.schemaId,
    item.keyword,
  ));
}

export async function validateExecutorAgentWaitingDelivery(value: unknown): Promise<AgentActivityValidationResult<ExecutorAgentWaitingDelivery>> {
  const schema = await validateSchema<ExecutorAgentWaitingDelivery>("executor-agent-waiting-delivery", value);
  if (!schema.valid || !schema.value) return schema;
  const delivery = schema.value;
  const { task, attempt, run } = delivery.envelope.identity;
  const conversation = conversationFromSourceRef(task.sourceRef);
  const diagnostics = [...await nestedEnvelopeDiagnostics(delivery.envelope)];
  if (!conversation) diagnostics.push(diagnostic("identity_mismatch", "/envelope/identity/task/sourceRef", "The delivered task source must contain a resolvable conversation identity."));
  else diagnostics.push(...commonWaitingDeliveryDiagnostics(delivery, task.taskId, run.runId, conversation, delivery.envelope.operationId));
  diagnostics.push(...mismatch(delivery.agentActivity.attemptId, attempt.attemptId, "/agentActivity/attemptId", "The elicitation activity must reference the delivered attempt."));
  return finish(delivery, diagnostics);
}

export async function validateRunAgentWaitingDelivery(value: unknown): Promise<AgentActivityValidationResult<RunAgentWaitingDelivery>> {
  const schema = await validateSchema<RunAgentWaitingDelivery>("run-agent-waiting-delivery", value);
  if (!schema.valid || !schema.value) return schema;
  const delivery = schema.value;
  const conversation = conversationFromSourceRef(delivery.envelope.task.sourceRef);
  const diagnostics = [...await nestedEnvelopeDiagnostics(delivery.envelope)];
  if (!conversation) diagnostics.push(diagnostic("identity_mismatch", "/envelope/task/sourceRef", "The delivered task source must contain a resolvable conversation identity."));
  else diagnostics.push(...commonWaitingDeliveryDiagnostics(delivery, delivery.envelope.task.taskId, delivery.envelope.run.runId, conversation, delivery.envelope.operationId));
  diagnostics.push(...mismatch(delivery.agentActivity.attemptId, delivery.envelope.attempt.attemptId, "/agentActivity/attemptId", "The elicitation activity must reference the delivered attempt."));
  return finish(delivery, diagnostics);
}

export async function validateTaskAgentWaitingDelivery(value: unknown): Promise<AgentActivityValidationResult<TaskAgentWaitingDelivery>> {
  const schema = await validateSchema<TaskAgentWaitingDelivery>("task-agent-waiting-delivery", value);
  if (!schema.valid || !schema.value) return schema;
  const delivery = schema.value;
  const conversation = conversationFromSourceRef(delivery.envelope.task.sourceRef);
  const diagnostics = [...await nestedEnvelopeDiagnostics(delivery.envelope)];
  if (!conversation) diagnostics.push(diagnostic("identity_mismatch", "/envelope/task/sourceRef", "The delivered task source must contain a resolvable conversation identity."));
  else diagnostics.push(...commonWaitingDeliveryDiagnostics(delivery, delivery.envelope.task.taskId, delivery.envelope.run.runId, conversation, delivery.envelope.operationId));
  return finish(delivery, diagnostics);
}

export async function validateAgentWaitingDelivery(value: unknown): Promise<AgentActivityValidationResult<AgentWaitingDelivery>> {
  if (!value || typeof value !== "object") return validateSchema<AgentWaitingDelivery>("executor-agent-waiting-delivery", value);
  const type = (value as { deliveryType?: unknown }).deliveryType;
  if (type === "executor_agent_waiting") return validateExecutorAgentWaitingDelivery(value);
  if (type === "run_agent_waiting") return validateRunAgentWaitingDelivery(value);
  if (type === "task_agent_waiting") return validateTaskAgentWaitingDelivery(value);
  return validateSchema<AgentWaitingDelivery>("executor-agent-waiting-delivery", value);
}

// Replay identity includes the durable decision and conversation; activityId identifies one delivery attempt.
function canonicalConversationIdentity(conversation: ConversationIdentity): string {
  return `linear\u0000${conversation.agentSessionId}`;
}

function canonicalWaitingDeliveryIdentity(delivery: AgentWaitingDelivery): string {
  const receipt = delivery.agentActivity;
  return [delivery.deliveryType, receipt.elicitation.decisionId, receipt.taskId, receipt.attemptId, receipt.runId, canonicalConversationIdentity(receipt.conversation), receipt.sourceEventId, receipt.operationId].join("\u0000");
}

export function compareAgentWaitingDeliveries(existing: AgentWaitingDelivery, incoming: AgentWaitingDelivery): AgentActivityReplayResult {
  if (canonicalWaitingDeliveryIdentity(existing) !== canonicalWaitingDeliveryIdentity(incoming)) return "distinct";
  // activityId identifies a delivery retry and is excluded from replay comparison. Conversation identity
  // participates in the canonical identity.
  const { activityId: _existingActivityId, ...existingReceiptWithoutActivityId } = existing.agentActivity;
  const { activityId: _incomingActivityId, ...incomingReceiptWithoutActivityId } = incoming.agentActivity;
  const contentMatches = isDeepStrictEqual(existing.envelope, incoming.envelope) &&
    isDeepStrictEqual(existingReceiptWithoutActivityId, incomingReceiptWithoutActivityId);
  return contentMatches ? "duplicate" : "conflict";
}

export interface TruncatedAgentActivityText { value: string; truncation: AgentActivityTruncation | null; }

const truncationMarkerPattern = /\[… Axis truncated this response to 4096 characters; ([1-9][0-9]*) characters omitted\.\]$/u;

function scalarCount(value: string): number {
  return Array.from(value).length;
}

export function agentActivityTruncationMarker(omittedScalarCount: number): string {
  if (!Number.isSafeInteger(omittedScalarCount) || omittedScalarCount < 1) throw new RangeError("omittedScalarCount must be a positive safe integer");
  return `[… Axis truncated this response to ${agentActivityMaxDisplayScalars} characters; ${omittedScalarCount} characters omitted.]`;
}

export function truncateAgentActivityText(value: string): TruncatedAgentActivityText {
  const scalars = Array.from(value);
  if (scalars.length <= agentActivityMaxDisplayScalars) return { value, truncation: null };
  let omittedScalarCount = scalars.length - agentActivityMaxDisplayScalars;
  for (;;) {
    const marker = agentActivityTruncationMarker(omittedScalarCount);
    const retainedScalarCount = agentActivityMaxDisplayScalars - scalarCount(marker);
    if (retainedScalarCount < 1) throw new RangeError("The truncation marker exceeds the display-text limit");
    const nextOmittedScalarCount = scalars.length - retainedScalarCount;
    if (nextOmittedScalarCount === omittedScalarCount) {
      return {
        value: `${scalars.slice(0, retainedScalarCount).join("")}${marker}`,
        truncation: { fieldPath: "/content/body", truncated: true, originalScalarCount: scalars.length, omittedScalarCount },
      };
    }
    omittedScalarCount = nextOmittedScalarCount;
  }
}

function textDiagnostics(value: string, path: string): AgentActivityDiagnostic[] {
  const result: AgentActivityDiagnostic[] = [];
  if (value.trim().length === 0) result.push(diagnostic("unsafe_content", path, "Agent activity display text must contain nonblank text."));
  if (!hasOnlyUnicodeScalars(value)) result.push(diagnostic("unsafe_content", path, "Agent activity display text must contain only valid Unicode scalar values."));
  if (value.normalize("NFC") !== value) result.push(diagnostic("unsafe_content", path, "Agent activity display text must use NFC normalization."));
  if (value.includes("\r")) result.push(diagnostic("unsafe_content", path, "Agent activity display text must use LF-only line endings."));
  if (disallowedControl.test(value)) result.push(diagnostic("unsafe_content", path, "Agent activity display text must not contain disallowed control characters."));
  if (bidi.test(value)) result.push(diagnostic("unsafe_content", path, "Agent activity display text must not contain bidirectional control characters."));
  if (secretLike.test(value)) result.push(diagnostic("unsafe_content", path, "Agent activity display text must not contain unredacted credential values."));
  if (scalarCount(value) > agentActivityMaxDisplayScalars) result.push(diagnostic("unsafe_content", path, `Agent activity display text must not exceed ${agentActivityMaxDisplayScalars} Unicode scalar values.`));
  return result;
}

// A response receipt carries exactly one display field. The schema limits
// `truncations` to one `/content/body` record. Its visible marker and structured
// record must appear together, and their counts must reconcile with the text.
function receiptDiagnostics(receipt: AgentActivityResponseReceipt, path = ""): AgentActivityDiagnostic[] {
  const result: AgentActivityDiagnostic[] = [];
  const body = receipt.content.body;
  result.push(...textDiagnostics(body, `${path}/content/body`));

  const [truncation] = receipt.truncations;
  if (truncation) {
    const marker = agentActivityTruncationMarker(truncation.omittedScalarCount);
    if (!body.endsWith(marker)) result.push(diagnostic("invalid_truncation", `${path}/content/body`, "A truncated field must end with the exact visible Axis truncation marker."));
    const retainedScalarCount = scalarCount(body) - scalarCount(marker);
    if (retainedScalarCount + truncation.omittedScalarCount !== truncation.originalScalarCount) {
      result.push(diagnostic("invalid_truncation", `${path}/truncations`, "Truncation counts must equal the retained text plus the omitted source text."));
    }
  }

  if (truncationMarkerPattern.test(body) !== (receipt.truncations.length > 0)) {
    result.push(diagnostic("invalid_truncation", `${path}/content/body`, "A visible truncation marker and its structured record must always appear together."));
  }
  if (receipt.truncated !== (receipt.truncations.length > 0)) result.push(diagnostic("invalid_truncation", `${path}/truncated`, "The truncated flag must match whether truncation records are present."));
  if (receipt.projectionStatus === "fallback" && (body !== agentActivityFallbackResponseBody || receipt.truncated || receipt.truncations.length !== 0)) {
    result.push(diagnostic("invalid_truncation", `${path}/content/body`, "The fallback response must be the fixed untruncated safe body."));
  }
  return result;
}

export async function validateAgentActivityResponseReceipt(value: unknown): Promise<AgentActivityValidationResult<AgentActivityResponseReceipt>> {
  const schema = await validateSchema<AgentActivityResponseReceipt>("agent-activity-response-receipt", value);
  if (!schema.valid || !schema.value) return schema;
  return finish(schema.value, receiptDiagnostics(schema.value));
}

// Succeeded envelopes carry no sourceEventId, so the receipt binds it through
// comparison with the durable producer's source event. Waiting envelopes carry
// their source event in the structured elicitation.
export function validateAgentActivitySourceEvent(receipt: AgentActivityResponseReceipt, durableSourceEventId: string): AgentActivityValidationResult<AgentActivityResponseReceipt> {
  return finish(receipt, mismatch(receipt.sourceEventId, durableSourceEventId, "/sourceEventId", "The agent activity must use the durable producer source event identifier."));
}

function commonDeliveryDiagnostics(
  delivery: AgentResponseDelivery,
  taskId: string,
  runId: string,
  sourceConversation: ConversationIdentity,
  operationId: string,
  resultRef: string,
): AgentActivityDiagnostic[] {
  const receipt = delivery.agentActivity;
  const diagnostics = receiptDiagnostics(receipt, "/agentActivity");
  diagnostics.push(...mismatch(receipt.taskId, taskId, "/agentActivity/taskId", "The response activity must reference the delivered task."));
  diagnostics.push(...mismatch(receipt.runId, runId, "/agentActivity/runId", "The response activity must reference the delivered run."));
  if (!sameConversationIdentity(receipt.conversation, sourceConversation)) {
    diagnostics.push(diagnostic("identity_mismatch", "/agentActivity/conversation", "The response activity must use the task source's conversation identity."));
  }
  diagnostics.push(...mismatch(receipt.operationId, operationId, "/agentActivity/operationId", "The response activity and success envelope must use the same operation identifier."));
  diagnostics.push(...mismatch(receipt.resultRef, resultRef, "/agentActivity/resultRef", "The response activity and success envelope must use the same result reference."));
  return diagnostics;
}

async function responseEnvelopeDiagnostics(envelope: unknown): Promise<AgentActivityDiagnostic[]> {
  const result = await validateRuntimeBoundaryMessage(envelope);
  return result.diagnostics.map((item) => diagnostic(
    item.code === "schema_violation" ? "schema_violation" : "identity_mismatch",
    `/envelope${item.instancePath}`,
    "The nested Runtime v1 success envelope is not semantically coherent.",
    item.schemaId,
    item.keyword,
  ));
}

export async function validateExecutorAgentResponseDelivery(value: unknown): Promise<AgentActivityValidationResult<ExecutorAgentResponseDelivery>> {
  const schema = await validateSchema<ExecutorAgentResponseDelivery>("executor-agent-response-delivery", value);
  if (!schema.valid || !schema.value) return schema;
  const delivery = schema.value;
  const { task, attempt, run } = delivery.envelope.identity;
  const conversation = conversationFromSourceRef(task.sourceRef);
  const diagnostics = [...await responseEnvelopeDiagnostics(delivery.envelope)];
  if (!conversation) diagnostics.push(diagnostic("identity_mismatch", "/envelope/identity/task/sourceRef", "The delivered task source must contain a resolvable conversation identity."));
  else diagnostics.push(...commonDeliveryDiagnostics(delivery, task.taskId, run.runId, conversation, delivery.envelope.operationId, delivery.envelope.resultRef));
  diagnostics.push(...mismatch(delivery.agentActivity.attemptId, attempt.attemptId, "/agentActivity/attemptId", "The response activity must reference the delivered attempt."));
  return finish(delivery, diagnostics);
}

export async function validateRunAgentResponseDelivery(value: unknown): Promise<AgentActivityValidationResult<RunAgentResponseDelivery>> {
  const schema = await validateSchema<RunAgentResponseDelivery>("run-agent-response-delivery", value);
  if (!schema.valid || !schema.value) return schema;
  const delivery = schema.value;
  const conversation = conversationFromSourceRef(delivery.envelope.task.sourceRef);
  const diagnostics = [...await responseEnvelopeDiagnostics(delivery.envelope)];
  if (!conversation) diagnostics.push(diagnostic("identity_mismatch", "/envelope/task/sourceRef", "The delivered task source must contain a resolvable conversation identity."));
  else diagnostics.push(...commonDeliveryDiagnostics(delivery, delivery.envelope.task.taskId, delivery.envelope.run.runId, conversation, delivery.envelope.operationId, delivery.envelope.resultRef));
  diagnostics.push(...mismatch(delivery.agentActivity.attemptId, delivery.envelope.attempt.attemptId, "/agentActivity/attemptId", "The response activity must reference the delivered attempt."));
  return finish(delivery, diagnostics);
}

export async function validateTaskAgentResponseDelivery(value: unknown): Promise<AgentActivityValidationResult<TaskAgentResponseDelivery>> {
  const schema = await validateSchema<TaskAgentResponseDelivery>("task-agent-response-delivery", value);
  if (!schema.valid || !schema.value) return schema;
  const delivery = schema.value;
  const conversation = conversationFromSourceRef(delivery.envelope.task.sourceRef);
  const diagnostics = [...await responseEnvelopeDiagnostics(delivery.envelope)];
  if (!conversation) diagnostics.push(diagnostic("identity_mismatch", "/envelope/task/sourceRef", "The delivered task source must contain a resolvable conversation identity."));
  else diagnostics.push(...commonDeliveryDiagnostics(delivery, delivery.envelope.task.taskId, delivery.envelope.run.runId, conversation, delivery.envelope.operationId, delivery.envelope.resultRef));
  return finish(delivery, diagnostics);
}

export async function validateAgentResponseDelivery(value: unknown): Promise<AgentActivityValidationResult<AgentResponseDelivery>> {
  if (!value || typeof value !== "object") return validateSchema<AgentResponseDelivery>("executor-agent-response-delivery", value);
  const type = (value as { deliveryType?: unknown }).deliveryType;
  if (type === "executor_agent_response") return validateExecutorAgentResponseDelivery(value);
  if (type === "run_agent_response") return validateRunAgentResponseDelivery(value);
  if (type === "task_agent_response") return validateTaskAgentResponseDelivery(value);
  return validateSchema<AgentResponseDelivery>("executor-agent-response-delivery", value);
}

// The canonical identity excludes retry-specific activityId, includes the conversation, and anchors
// response delivery to resultRef. Redelivery of the same result compares as duplicate.
function canonicalDeliveryIdentity(delivery: AgentResponseDelivery): string {
  const receipt = delivery.agentActivity;
  // Task envelopes carry no attempt authority. Exclude attemptId from task-level replay identity so a
  // different unbound attempt on the same result is a content conflict, not a second reply.
  const attemptIdentity = delivery.deliveryType === "task_agent_response" ? "" : receipt.attemptId;
  return [delivery.deliveryType, receipt.resultRef, receipt.taskId, attemptIdentity, receipt.runId, canonicalConversationIdentity(receipt.conversation), receipt.sourceEventId, receipt.operationId].join("\u0000");
}

export function compareAgentResponseDeliveries(existing: AgentResponseDelivery, incoming: AgentResponseDelivery): AgentActivityReplayResult {
  if (canonicalDeliveryIdentity(existing) !== canonicalDeliveryIdentity(incoming)) return "distinct";
  const { activityId: _existingActivityId, ...existingReceiptWithoutActivityId } = existing.agentActivity;
  const { activityId: _incomingActivityId, ...incomingReceiptWithoutActivityId } = incoming.agentActivity;
  const contentMatches = isDeepStrictEqual(existing.envelope, incoming.envelope) &&
    isDeepStrictEqual(existingReceiptWithoutActivityId, incomingReceiptWithoutActivityId);
  return contentMatches ? "duplicate" : "conflict";
}
