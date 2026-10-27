import { createHash } from "node:crypto";

import {
  parseStrictRouterWorkerJson,
  validateRouterWorkerLifecycleEvent, workerFailureTuples,
  type RouterRunLifecycleEnvelope, type WorkerCommand,
  type WorkerExecutionIdentity, type WorkerVersionedReference,
} from "./router-worker.js";
import type { RuntimeSourceSystem } from "./runtime-boundary.js";

/** Package-private contract seams only: no listener, durable storage, or runtime binding. */
export const NODE_CONTRACT_VERSION = "1.0.0" as const;
export const NODE_OUTER_BODY_LIMIT_BYTES = 128 * 1024;
export const NODE_NESTED_BODY_LIMIT_BYTES = 64 * 1024;
const errors = {
  "axis.node.malformed": [400, "The Node request is malformed."],
  "axis.node.unauthorized": [401, "Node authorization is required."],
  "axis.node.target_forbidden": [403, "The requested Node target is forbidden."],
  "axis.node.target_unknown": [404, "The requested Node target is unknown."],
  // The Node is reachable and authorized but has no Worker able to take the command. Definite.
  "axis.node.worker_unavailable": [409, "No Worker is available on this Node."],
  // The command named two or more different executor labels. Definite: the request is not
  // self-consistent.
  "axis.node.selection_conflict": [409, "The Node command names more than one executor."],
  "axis.node.intent_conflict": [409, "The Node command conflicts with its durable intent."],
  "axis.node.binding_conflict": [409, "The Node target conflicts with the durable binding."],
  "axis.node.body_too_large": [413, "The Node request body exceeds the accepted limit."],
  "axis.node.unsupported_media": [415, "The Node request media type is unsupported."],
  "axis.node.recovery_required": [503, "The Node command requires safe recovery."],
  // The Node is draining (`POST /v1/maintenance` mode `drain`) and refuses a new start or continue
  // before recording anything. Transient, but definite: see NODE_DEFINITE_ERROR_CODES.
  "axis.node.draining": [503, "The Node is draining and is not accepting new work."],
  "axis.node.dispatch_ambiguous": [503, "The Node dispatch outcome is ambiguous."],
  "axis.node.storage_unavailable": [503, "Node durable storage is unavailable."],
} as const;
export type NodeErrorCode = keyof typeof errors;
export const NODE_ERROR_STATUS: Readonly<Record<NodeErrorCode, number>> = Object.freeze(
  Object.fromEntries(Object.entries(errors).map(([code, tuple]) => [code, tuple[0]])) as Record<NodeErrorCode, number>,
);
/**
 * The Node error codes a transport may treat as a definite refusal: the command was rejected
 * before any Worker dispatch, so nothing ran. The rule is that the code cannot leave the command
 * path once a dispatch may have been attempted; `src/node/command.ts`'s `#afterDispatch` downgrades a
 * definite code from the post-dispatch journal calls to `axis.node.dispatch_ambiguous`. A replayed
 * `executeWithDisposition` can still raise `intent_conflict`, and `readReceipt` `binding_conflict`,
 * which is safe while `nodeRefusalSafeCodes` maps only `worker_unavailable`, `target_unknown` and
 * `draining`. The listener serializes every refusal, including pre-service ones, as `NodeCommandError`.
 *
 * `selection_conflict` and `draining` are raised inside `claimCommandAndRoute` before any row is
 * written, so no dispatch can precede them. `draining` is raised only on the fresh branch; a replayed
 * `reserved` or `dispatching` row still raises `recovery_required`.
 */
export const NODE_DEFINITE_ERROR_CODES: readonly NodeErrorCode[] = Object.freeze([
  "axis.node.malformed",
  "axis.node.unauthorized",
  "axis.node.target_forbidden",
  "axis.node.target_unknown",
  "axis.node.selection_conflict",
  "axis.node.worker_unavailable",
  "axis.node.intent_conflict",
  "axis.node.binding_conflict",
  "axis.node.body_too_large",
  "axis.node.unsupported_media",
  "axis.node.draining",
]);
export class NodeContractError extends Error {
  readonly status: number;
  readonly code: NodeErrorCode;
  constructor(code: NodeErrorCode = "axis.node.malformed") {
    const safeCode = Object.hasOwn(errors, code) ? code : "axis.node.malformed";
    super(errors[safeCode][1]); this.code = safeCode; this.status = errors[safeCode][0]; this.name = "NodeContractError";
  }
}
export function nodeError(code: NodeErrorCode) {
  if (!Object.hasOwn(errors, code)) fail();
  return { contractVersion: NODE_CONTRACT_VERSION, error: { code, safeMessage: errors[code][1] } };
}
export interface NodeTarget { readonly endpointRef: WorkerVersionedReference; readonly identity: WorkerExecutionIdentity; readonly sessionBindingExecutorId: string; }
export interface NodeKnownWorkerFailure { readonly code: string; readonly safeMessage: string; readonly certainty: "definite" | "ambiguous"; }
export interface NodeMaintenanceRequest { readonly contractVersion: typeof NODE_CONTRACT_VERSION; readonly mode: "drain" | "resume"; }
export interface NodeBlockerCounts {
  readonly reservedCommands: number; readonly dispatchingCommands: number; readonly ambiguousCommands: number;
  readonly pendingNodeEvents: number; readonly quarantinedNodeEvents: number; readonly activeWorkerRuns: number;
  readonly pendingWorkerOutbox: number; readonly unknownWorkerEvidence: number;
}
export interface NodeMaintenanceStatus { readonly contractVersion: typeof NODE_CONTRACT_VERSION; readonly nodeRef: WorkerVersionedReference; readonly mode: "drain" | "resume"; readonly idle: boolean; readonly blockerCounts: NodeBlockerCounts; }
export interface NodeStaticTarget { readonly nodeRef: WorkerVersionedReference; readonly nodeId: string; readonly target: NodeTarget; }

export interface NodeDurableRoute { readonly nodeRef: WorkerVersionedReference; readonly target: NodeTarget; readonly taskId: string; readonly sourceSystem: RuntimeSourceSystem; readonly sourceSessionId: string; readonly runId: string; readonly workspacePolicyRef?: WorkerVersionedReference | null; }

/** One Worker command dispatch to a static target; the caller owns durability around it. */
export interface NodeWorkerCommandPort { dispatch(target: NodeTarget, command: WorkerCommand): Promise<unknown>; }
type R = Readonly<Record<string, unknown>>;
function fail(code: NodeErrorCode = "axis.node.malformed"): never { throw new NodeContractError(code); }
function object(value: unknown): R {
  if (value === null || typeof value !== "object" || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) fail();
  return value as R;
}
function closed(value: unknown, keys: readonly string[]): R {
  const item = object(value);
  if (Object.keys(item).length !== keys.length || keys.some((key) => !Object.hasOwn(item, key))) fail();
  return item;
}
function id(value: unknown): string { if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(value)) fail(); return value; }
function ref(value: unknown): WorkerVersionedReference { const item = closed(value, ["ref", "version"]); return { ref: id(item.ref), version: id(item.version) }; }
function identity(value: unknown): WorkerExecutionIdentity {
  const item = closed(value, ["nodeId", "workerId", "executorKind", "runtimeProfile"]);
  return { nodeId: id(item.nodeId), workerId: id(item.workerId), executorKind: id(item.executorKind), runtimeProfile: ref(item.runtimeProfile) };
}
function canon(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(canon).join(",") + "]";
  if (value !== null && typeof value === "object") {
    const item = value as R; return "{" + Object.keys(item).sort().map((key) => JSON.stringify(key) + ":" + canon(item[key])).join(",") + "}";
  }
  return JSON.stringify(value);
}
function same(left: unknown, right: unknown): boolean { return canon(left) === canon(right); }
function digest(domain: string, value: unknown): `sha256:${string}` { return `sha256:${createHash("sha256").update(domain).update(canon(value)).digest("hex")}`; }
function size(value: unknown, limit: number): void {
  try { if (Buffer.byteLength(JSON.stringify(value), "utf8") > limit) fail("axis.node.body_too_large"); }
  catch (error) { if (error instanceof NodeContractError) throw error; fail(); }
}
function validateNodeNestedSize(value: unknown): void { size(value, NODE_NESTED_BODY_LIMIT_BYTES); }
export function parseNodeJson(buffer: Uint8Array): unknown {
  if (buffer.byteLength > NODE_OUTER_BODY_LIMIT_BYTES) fail("axis.node.body_too_large");
  try { return parseStrictRouterWorkerJson(buffer); } catch { fail(); }
}
/** Transport preflight seam only; no listener is installed. */
export function validateNodeTransportBody(buffer: Uint8Array, media: { readonly contentType: string; readonly contentEncoding?: string }): unknown {
  if (!/^application\/json(?:\s*;\s*charset=utf-8)?$/iu.test(media.contentType.trim()) || media.contentEncoding !== undefined) fail("axis.node.unsupported_media");
  return parseNodeJson(buffer);
}
export function validateNodeTarget(value: unknown): NodeTarget {
  const item = closed(value, ["endpointRef", "identity", "sessionBindingExecutorId"]);
  return { endpointRef: ref(item.endpointRef), identity: identity(item.identity), sessionBindingExecutorId: id(item.sessionBindingExecutorId) };
}
export function nodeKnownWorkerFailure(code: string): NodeKnownWorkerFailure {
  if (!Object.hasOwn(workerFailureTuples, code)) fail("axis.node.dispatch_ambiguous");
  const tuple = workerFailureTuples[code]!;
  return { code, safeMessage: tuple.message, certainty: tuple.disposition };
}
export function validateNodeKnownWorkerFailure(value: unknown): NodeKnownWorkerFailure {
  try {
    const item = closed(value, ["code", "safeMessage", "certainty"]);
    if (typeof item.code !== "string") fail();
    const result = nodeKnownWorkerFailure(item.code);
    if (!same(item, result)) fail();
    return result;
  } catch { fail("axis.node.dispatch_ambiguous"); }
}
export function validateNodeMaintenanceRequest(value: unknown): NodeMaintenanceRequest {
  const item = closed(value, ["contractVersion", "mode"]);
  if (item.contractVersion !== NODE_CONTRACT_VERSION || (item.mode !== "drain" && item.mode !== "resume")) fail();
  return { contractVersion: NODE_CONTRACT_VERSION, mode: item.mode };
}
export function nodeMaintenanceStatus(value: unknown): NodeMaintenanceStatus {
  const item = closed(value, ["contractVersion", "nodeRef", "mode", "idle", "blockerCounts"]);
  const mode = validateNodeMaintenanceRequest({ contractVersion: item.contractVersion, mode: item.mode }).mode;
  const counts = closed(item.blockerCounts, ["reservedCommands", "dispatchingCommands", "ambiguousCommands", "pendingNodeEvents", "quarantinedNodeEvents", "activeWorkerRuns", "pendingWorkerOutbox", "unknownWorkerEvidence"]);
  if (!Object.values(counts).every((count) => typeof count === "number" && Number.isSafeInteger(count) && count >= 0)
    || item.idle !== Object.values(counts).every((count) => count === 0)) fail();
  return { contractVersion: NODE_CONTRACT_VERSION, nodeRef: ref(item.nodeRef), mode, idle: item.idle as boolean, blockerCounts: counts as unknown as NodeBlockerCounts };
}

const validatedEvents = new WeakSet<object>();
function freeze(value: unknown): void {
  if (value !== null && typeof value === "object") { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
}
export async function validateNodeEvent(value: unknown): Promise<RouterRunLifecycleEnvelope> {
  validateNodeNestedSize(value);
  try {
    const event = await validateRouterWorkerLifecycleEvent(structuredClone(object(value)));
    freeze(event); validatedEvents.add(event); return event;
  } catch { fail(); }
}
function validatedEvent(event: RouterRunLifecycleEnvelope): RouterRunLifecycleEnvelope {
  if (!validatedEvents.has(event)) fail();
  return event;
}
export function nodeEventId(event: RouterRunLifecycleEnvelope): string {
  validatedEvent(event);
  return id(("deliveryType" in event ? event.envelope : event).messageId);
}
export function nodeEventRunId(event: RouterRunLifecycleEnvelope): string {
  validatedEvent(event);
  return id(("deliveryType" in event ? event.envelope : event).run.runId);
}
export function nodeEventDigest(event: RouterRunLifecycleEnvelope): `sha256:${string}` {
  return digest("axis.node.event.v1\0", validatedEvent(event));
}
