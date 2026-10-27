import { createHash } from "node:crypto";
import { validateTaskAgentResponseDelivery } from "@axisosme/axis-contracts/agent-activity";
import type { TaskAgentResponseDelivery } from "@axisosme/axis-contracts/agent-activity";
import type { ConversationIdentity, LinearConversationIdentity } from "@axisosme/axis-contracts/agent-activity";
import { validateStructuredElicitation, type StructuredElicitation } from "./elicitation.js";
import { runtimeSourceSessionIdentity } from "./runtime-boundary.js";
import { routerTaskEnvelope, type RouterTaskEvent, type RouterTaskWaitingForInputEnvelope } from "../router/router.js";

export class RouterRelayEventError extends Error {
  constructor() { super("The Router emitted an invalid Relay event."); this.name = "RouterRelayEventError"; }
}
function invalid(): never { throw new RouterRelayEventError(); }
function object(value: unknown): Readonly<Record<string, unknown>> { if (value === null || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) invalid(); return value as Readonly<Record<string, unknown>>; }
function closed(value: unknown, keys: readonly string[]): Readonly<Record<string, unknown>> { const result = object(value); if (Object.keys(result).length !== keys.length || keys.some((key) => !(key in result))) invalid(); return result; }
function string(value: unknown, maximum = 128): string { if (typeof value !== "string" || value.length === 0 || [...value].length > maximum) invalid(); return value; }
function identifier(value: unknown): string { const result = string(value); if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(result)) invalid(); return result; }
function instant(value: unknown): string { const result = string(value); if (Number.isNaN(Date.parse(result)) || new Date(result).toISOString() !== result) invalid(); return result; }

/** Restore the local question's canonical representation before hashing a persisted waiting event. */
export function routerRelayWaitingPayload(waiting: RouterTaskWaitingForInputEnvelope) {
  const elicitation = validateStructuredElicitation(waiting.elicitation);
  if (elicitation === null) invalid();
  return { taskId: waiting.task.taskId, attemptId: waiting.attemptId, runId: waiting.run.runId, elicitation };
}

export const ROUTER_RELAY_NEUTRAL_EVENT_VERSION = "1.0.0" as const;

export const ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY =
  "Axis finished running this request and has no result to show here. This is not a report that the requested work was done; check the work itself before relying on it.";

export interface RouterRelayNeutralEventEnvelope {
  readonly schemaVersion: typeof ROUTER_RELAY_NEUTRAL_EVENT_VERSION;
  readonly conversation: ConversationIdentity;
  readonly axisSequence: number;
  readonly eventId: string;
  readonly eventDigest: `sha256:${string}`;
  readonly sourceArtifactId: string | null;
  readonly event: RouterRelayNeutralEvent;
  readonly recordedAt: string;
}

export type RouterRelayNeutralEvent =
  | { readonly eventType: "axis.router.v1.accepted"; readonly payload: { readonly safeSummary: string } }
  | { readonly eventType: "axis.router.v1.progressed"; readonly payload: { readonly safeSummary: string } }
  | { readonly eventType: "axis.router.v1.waiting_for_input"; readonly payload: { readonly taskId: string; readonly attemptId: string; readonly runId: string; readonly elicitation: StructuredElicitation } }
  | { readonly eventType: "axis.router.v1.succeeded"; readonly payload: { readonly delivery: TaskAgentResponseDelivery } }
  | { readonly eventType: "axis.router.v1.succeeded"; readonly payload: { readonly safeSummary: string; readonly resultRef: string } }
  | { readonly eventType: "axis.router.v1.failed"; readonly payload: { readonly error: { readonly code: string; readonly safeMessage: string } } }
  | { readonly eventType: "axis.router.v1.cancelled"; readonly payload: { readonly safeSummary: string } };

/** The Router-owned digest covers only the canonical closed event payload, never the routing fields the envelope carries around it. */
export function routerRelayNeutralEventDigest(event: RouterRelayNeutralEvent): `sha256:${string}` {
  return `sha256:${createHash("sha256").update(JSON.stringify(event)).digest("hex")}`;
}

function closedConversation(value: unknown): ConversationIdentity {
  const record = object(value);
  const system = record.system;
  if (system === "linear") { const result = closed(record, ["system", "agentSessionId"]); return { system: "linear", agentSessionId: identifier(result.agentSessionId) }; }
  return invalid();
}
/** The single opaque id a `ConversationIdentity` carries, whichever provider named it. */
function conversationIdentityId(conversation: ConversationIdentity): string {
  return conversation.agentSessionId;
}

export async function validateRouterRelayNeutralEvent(value: unknown): Promise<RouterRelayNeutralEventEnvelope> {
  const envelope = closed(value, ["schemaVersion", "conversation", "axisSequence", "eventId", "eventDigest", "sourceArtifactId", "event", "recordedAt"]);
  if (envelope.schemaVersion !== ROUTER_RELAY_NEUTRAL_EVENT_VERSION || !Number.isSafeInteger(envelope.axisSequence) || (envelope.axisSequence as number) < 1 || !/^sha256:[a-f0-9]{64}$/u.test(string(envelope.eventDigest)) || (envelope.sourceArtifactId !== null && !identifier(envelope.sourceArtifactId))) invalid();
  const conversation = closedConversation(envelope.conversation);
  const event = closed(envelope.event, ["eventType", "payload"]);
  const eventType = string(event.eventType);
  let payload: RouterRelayNeutralEvent["payload"];
  switch (eventType) {
    case "axis.router.v1.accepted":
    case "axis.router.v1.progressed":
    case "axis.router.v1.cancelled": {
      const record = closed(event.payload, ["safeSummary"]); payload = { safeSummary: string(record.safeSummary, 512) }; break;
    }
    case "axis.router.v1.waiting_for_input": {
      const waiting = closed(event.payload, ["taskId", "attemptId", "runId", "elicitation"]);
      const elicitation = validateStructuredElicitation(waiting.elicitation);
      if (elicitation === null) invalid();
      payload = { taskId: identifier(waiting.taskId), attemptId: identifier(waiting.attemptId), runId: identifier(waiting.runId), elicitation };
      break;
    }
    case "axis.router.v1.succeeded": {
      const record = object(event.payload);
      if (Object.keys(record).length === 1 && "delivery" in record) {
        const validation = await validateTaskAgentResponseDelivery(record.delivery);
        if (!validation.valid || validation.value === null) invalid();
        payload = { delivery: validation.value };
      } else {
        const summary = closed(event.payload, ["safeSummary", "resultRef"]);
        payload = { safeSummary: string(summary.safeSummary, 512), resultRef: string(summary.resultRef, 2_048) };
      }
      break;
    }
    case "axis.router.v1.failed": {
      const record = closed(event.payload, ["error"]); const error = closed(record.error, ["code", "safeMessage"]); payload = { error: { code: identifier(error.code), safeMessage: string(error.safeMessage, 512) } }; break;
    }
    default: invalid();
  }
  const canonicalEvent = { eventType, payload } as RouterRelayNeutralEvent;
  const eventDigest = string(envelope.eventDigest) as `sha256:${string}`;
  if (eventDigest !== routerRelayNeutralEventDigest(canonicalEvent)) invalid();
  const result = {
    schemaVersion: ROUTER_RELAY_NEUTRAL_EVENT_VERSION,
    conversation,
    axisSequence: envelope.axisSequence as number,
    eventId: identifier(envelope.eventId),
    eventDigest,
    sourceArtifactId: envelope.sourceArtifactId as string | null,
    event: canonicalEvent,
    recordedAt: instant(envelope.recordedAt),
  };
  // Activity identity matches the target conversation for both delivery kinds, comparing whichever
  // provider both name.
  if (eventType === "axis.router.v1.succeeded" && "delivery" in payload) {
    const responseDelivery = payload as { readonly delivery: TaskAgentResponseDelivery };
    const activityConversation = responseDelivery.delivery.agentActivity.conversation;
    // The `linear` restriction is deliberate: only Linear response deliveries are produced
    // (`../worker/command.ts#succeeded`).
    if (conversation.system !== "linear" || activityConversation.system !== "linear" || activityConversation.agentSessionId !== conversation.agentSessionId) invalid();
  }
  return result;
}

export async function validateRouterRelayNeutralEventBatch(
  events: readonly unknown[],
  expected: { readonly conversation: ConversationIdentity; readonly afterCursor: number },
): Promise<readonly RouterRelayNeutralEventEnvelope[]> {
  const conversation = closedConversation(expected.conversation);
  if (!Number.isSafeInteger(expected.afterCursor) || expected.afterCursor < 0) invalid();
  let cursor = expected.afterCursor;
  const validatedEvents: RouterRelayNeutralEventEnvelope[] = [];
  for (const event of events) {
    const validated = await validateRouterRelayNeutralEvent(event);
    const validatedId = conversationIdentityId(validated.conversation);
    const expectedId = conversationIdentityId(conversation);
    if (validated.conversation.system !== conversation.system || validatedId !== expectedId || validated.axisSequence !== ++cursor) invalid();
    validatedEvents.push(validated);
  }
  return validatedEvents;
}

/**
 * Projects one `RouterTaskEvent` (`src/router/router.ts`) into the neutral Relay event shape,
 * deriving `conversation` from the envelope's own `task.sourceRef`. The only projection into the
 * Relay event shape, for both providers.
 */
export function buildRouterRelayNeutralEvent(envelope: RouterTaskEvent, eventId: string, cursor: number): RouterRelayNeutralEventEnvelope {
  const runtime = routerTaskEnvelope(envelope);
  const conversation = neutralEventConversation(runtime.task.sourceRef);
  const event = neutralEventPayload(envelope, runtime);
  return { schemaVersion: ROUTER_RELAY_NEUTRAL_EVENT_VERSION, conversation, axisSequence: cursor, eventId, eventDigest: routerRelayNeutralEventDigest(event), sourceArtifactId: runtime.causationMessageId, event, recordedAt: runtime.occurredAt };
}
function neutralEventConversation(sourceRef: Parameters<typeof runtimeSourceSessionIdentity>[0]): ConversationIdentity {
  const identity = runtimeSourceSessionIdentity(sourceRef);
  if (identity.sessionId === undefined) throw new RouterRelayEventError();
  return { system: "linear", agentSessionId: identity.sessionId };
}
function neutralEventPayload(envelope: RouterTaskEvent, runtime: ReturnType<typeof routerTaskEnvelope>): RouterRelayNeutralEvent {
  switch (runtime.envelopeType) {
    case "task.accepted": return { eventType: "axis.router.v1.accepted", payload: { safeSummary: "Worker accepted the execution." } };
    case "task.progressed": return { eventType: "axis.router.v1.progressed", payload: { safeSummary: runtime.progress.safeMessage as string } };
    case "task.waiting_for_input": return { eventType: "axis.router.v1.waiting_for_input", payload: routerRelayWaitingPayload(envelope as RouterTaskWaitingForInputEnvelope) };
    case "task.succeeded": return "deliveryType" in envelope && envelope.deliveryType === "task_agent_response"
      ? { eventType: "axis.router.v1.succeeded", payload: { delivery: envelope } }
      : { eventType: "axis.router.v1.succeeded", payload: { safeSummary: ROUTER_RELAY_UNREPORTED_SUCCESS_SUMMARY, resultRef: runtime.resultRef as string } };
    case "task.failed": return runtime.error.code === "execution_cancelled"
      ? { eventType: "axis.router.v1.cancelled", payload: { safeSummary: "Axis stopped the active execution as requested." } }
      : { eventType: "axis.router.v1.failed", payload: { error: { code: runtime.error.code, safeMessage: runtime.error.safeMessage } } };
  }
}
