import { homedir } from "node:os";
import { relative, sep } from "node:path";

import type { MethodRef } from "./runtime-boundary.js";
import type { AgentActivityTruncation } from "@axisosme/axis-contracts/agent-activity";
import type { StructuredElicitation } from "./elicitation.js";
import type { LinearExecutionIdentity } from "@axisosme/axis-contracts/linear-execution";

/** The active private executor protocol version. */
export const CODEX_EXECUTOR_CONTRACT_VERSION = "1.0.0" as const;

/** These names are Worker capabilities, not a provider selection mechanism. */
export type ExecutorCapability = "start" | "continue" | "cancel" | "steer" | "observe";
export type CodexExecutorOperation = Exclude<ExecutorCapability, "observe">;
export type ExecutorLifecycleOutcome = "started" | "progressed" | "waiting_for_input" | "succeeded" | "failed" | "cancelled";
/**
 * An in-process refinement of a `failed` lifecycle fact, read only by the emitting registration's
 * projection. Worker accepts each kind from exactly one runtime: `model_capacity` from Codex,
 * `permission_denied` from Claude Code (a tool call was denied). It never crosses a wire.
 */
export type ExecutorFailureKind = "model_capacity" | "permission_denied";

export type ExecutorErrorCode =
  | "executor.invalid_request"
  | "executor.idempotency_conflict"
  | "executor.transport_unavailable"
  | "executor.start_failed"
  | "executor.start_rejected"
  | "executor.continue_failed"
  | "executor.interrupt_failed"
  | "executor.steer_too_late"
  | "executor.runtime_missing"
  | "executor.runtime_incompatible"
  | "executor.authentication_required"
  | "executor.probe_timeout"
  | "executor.capability_unavailable"
  | "executor.launch_failed"
  | "executor.launch_ambiguous";

export interface ExecutorError {
  readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION;
  readonly code: ExecutorErrorCode;
  readonly operation: CodexExecutorOperation;
  readonly category: "validation" | "runtime";
  readonly retryable: boolean;
  /** Fixed Axis-owned copy. Native output and instructions never cross this boundary. */
  readonly safeMessage: string;
}

/** Normalize provider commentary before it can become a lifecycle fact or durable Worker state. */
export function normalizeExecutorCommentary(value: unknown): string | null {
  if (typeof value !== "string") return null;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return null;
      index += 1;
      continue;
    }
    if (code >= 0xdc00 && code <= 0xdfff) return null;
  }
  if (/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F\u061C\u200E\u200F\u202A-\u202E\u2066-\u2069]/u.test(value)) return null;
  const normalized = value.normalize("NFC").replace(/axlea1_[A-Za-z0-9_-]{43}/gu, "[REDACTED]").replace(/\s+/gu, " ").trim();
  if (normalized.length === 0) return null;
  const bounded = [...normalized].slice(0, 512).join("").trim();
  return bounded.length === 0 ? null : bounded;
}

function escapeRegExp(value: string): string { return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&"); }

/**
 * A markdown image, captured by its destination. The alt text is matched and discarded; an
 * optional CommonMark title is matched so it is not read as part of the destination. Both
 * destination shapes are matched: pointy-bracket and bare. Reference-style images, raw `<img>` and
 * bare destinations with parentheses pass through. The alt text admits neither `[` nor `]`, so a
 * stray `![` earlier on the line cannot open a match that swallows the text up to a later image.
 */
const markdownImageReference = /!\[[^\][\n]*\]\(\s*(<[^<>\n]*>|[^()\s]+)(?:\s+(?:"[^"\n]*"|'[^'\n]*'|\([^()\n]*\)))?\s*\)/gu;
/**
 * A destination that does not name a local file: a URI scheme other than `file:`, a
 * protocol-relative `//host/...`, or a bare `#fragment`. These are left untouched.
 */
const nonLocalImageDestination = /^(?:\/\/|#|(?!file:)[A-Za-z][A-Za-z0-9+.-]*:)/iu;
/**
 * The destination as CommonMark reads it, with one layer of pointy brackets removed. Used to
 * decide, never to print: printing the unwrapped form would strip the brackets off a
 * `<temporary-path>` placeholder.
 */
function imageDestinationTarget(destination: string): string {
  return destination.startsWith("<") && destination.endsWith(">") ? destination.slice(1, -1).trim() : destination;
}

/**
 * Normalizes a completed provider answer before it can become durable Agent
 * Activity content. This is intentionally shared rather than tied to a
 * particular native transport: every executor that projects a final answer
 * has the same path, credential, and Unicode redaction boundary.
 */
export function normalizeExecutorTerminalResponse(value: unknown, workspaceRoot: string | undefined): string | null {
  if (typeof value !== "string") return null;
  let normalized = value.replace(/\r\n?/gu, "\n").normalize("NFC").trim();
  if (normalized.length === 0 || /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F\u202A-\u202E\u2066-\u2069]/u.test(normalized) || /[\uD800-\uDFFF]/u.test(normalized)) return null;

  if (workspaceRoot !== undefined) {
    const root = workspaceRoot.endsWith(sep) ? workspaceRoot.slice(0, -1) : workspaceRoot;
    const workspacePattern = new RegExp(`${escapeRegExp(root)}(?:${escapeRegExp(sep)}([^\\s)\\]}>\"']+))?(?=$|[\\s)\\]}>\"'])`, "gu");
    normalized = normalized.replace(workspacePattern, (_match, suffix: string | undefined) => suffix === undefined ? "." : relative(root, `${root}${sep}${suffix}`).split(sep).join("/"));
  }
  const home = homedir();
  normalized = normalized.replace(new RegExp(`${escapeRegExp(home)}(?:${escapeRegExp(sep)}([^\\s)\\]}>\"']+))?(?=$|[\\s)\\]}>\"'])`, "gu"), (_match, suffix: string | undefined) => suffix === undefined ? "<home>" : `<home>/${suffix.split(sep).join("/")}`);
  normalized = normalized.replace(/\/(?:private\/)?(?:tmp|var\/folders)\/[^\s)\]}>"']+/gu, "<temporary-path>");

  /**
   * A markdown image pointing at a local file is replaced by a text notice that keeps the
   * destination, since Linear cannot fetch a local path and would publish a permanently broken
   * upload link. This runs after the path rewrites above, so workspace, `$HOME` and temporary paths
   * are already redacted. Only destinations with no URI scheme, or `file:`, are local. Markdown
   * structure is not parsed, so an image inside a code span or fence is rewritten too.
   */
  normalized = normalized.replace(markdownImageReference, (reference: string, destination: string) => {
    const target = imageDestinationTarget(destination);
    return target.length === 0 || nonLocalImageDestination.test(target) ? reference : `[Axis could not embed this local image: ${destination}]`;
  });

  normalized = normalized
    .replace(/axlea1_[A-Za-z0-9_-]{43}/gu, "[REDACTED]")
    .replace(/\b((?:api[_-]?key|cookie|password|secret|token)\s*[:=]\s*)\S+?([.,;!?])?(?=\s|$)/giu, "$1[REDACTED]$2")
    .replace(/\b(authorization\s*:\s*(?:bearer|basic)\s+)\S+?([.,;!?])?(?=\s|$)/giu, "$1[REDACTED]$2")
    .replace(/\bbearer\s+[A-Za-z0-9._~+/-]{8,}?([.,;!?])?(?=\s|$)/giu, "Bearer [REDACTED]$1")
    .replace(/\b(?:sk_(?:live|test)_[A-Za-z0-9]{16,}|ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|AKIA[0-9A-Z]{16}|xox[a-zA-Z]-[A-Za-z0-9-]{10,}|glpat-[A-Za-z0-9_-]{16,})\b/gu, "[REDACTED]");
  return normalized.trim().length === 0 ? null : normalized;
}

const ERROR_DEFINITIONS = Object.freeze({
  "executor.invalid_request": { category: "validation", retryable: false, safeMessage: "The executor request is invalid." },
  "executor.idempotency_conflict": { category: "validation", retryable: false, safeMessage: "The executor request conflicts with an existing request." },
  "executor.transport_unavailable": { category: "runtime", retryable: true, safeMessage: "The Codex app-server transport is unavailable." },
  "executor.start_failed": { category: "runtime", retryable: true, safeMessage: "The executor start failed." },
  "executor.start_rejected": { category: "runtime", retryable: false, safeMessage: "The executor start was rejected." },
  "executor.continue_failed": { category: "runtime", retryable: true, safeMessage: "The executor continuation failed." },
  "executor.interrupt_failed": { category: "runtime", retryable: true, safeMessage: "The executor interrupt request failed." },
  // Not retryable: a steer against a finished turn cannot succeed, and the caller falls back to an
  // ordinary continuation.
  "executor.steer_too_late": { category: "runtime", retryable: false, safeMessage: "The executor turn had already finished, so the steer was not delivered." },
  "executor.runtime_missing": { category: "runtime", retryable: false, safeMessage: "The configured executor runtime is unavailable." },
  "executor.runtime_incompatible": { category: "runtime", retryable: false, safeMessage: "The configured executor runtime is incompatible." },
  "executor.authentication_required": { category: "runtime", retryable: false, safeMessage: "The configured executor runtime requires authentication." },
  "executor.probe_timeout": { category: "runtime", retryable: true, safeMessage: "The executor preflight probe did not respond in time." },
  "executor.capability_unavailable": { category: "runtime", retryable: false, safeMessage: "The configured executor capability is unavailable." },
  "executor.launch_failed": { category: "runtime", retryable: false, safeMessage: "The executor launch failed before execution began." },
  "executor.launch_ambiguous": { category: "runtime", retryable: false, safeMessage: "The executor launch outcome is ambiguous." },
} as const satisfies Readonly<Record<ExecutorErrorCode, {
  readonly category: ExecutorError["category"];
  readonly retryable: boolean;
  readonly safeMessage: string;
}>>);

export function executorError(operation: CodexExecutorOperation, code: ExecutorErrorCode): ExecutorError {
  const definition = ERROR_DEFINITIONS[code];
  return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, code, operation, category: definition.category, retryable: definition.retryable, safeMessage: definition.safeMessage };
}

/**
 * A lifecycle failure that names why the Run was isolated. `reason` is a fixed, adapter-authored
 * label, never derived from native output: Worker persists it in the journal. `message` is human
 * prose and is not persisted.
 */
export class ExecutorLifecycleError extends Error {
  readonly reason: string;
  constructor(reason: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ExecutorLifecycleError";
    this.reason = reason;
  }
}

/**
 * The label a thrown lifecycle failure carries, or `undefined`. Shape-checked rather than
 * `instanceof`, because a Worker and an adapter can load two copies of this module.
 */
export function executorLifecycleReason(error: unknown): string | undefined {
  if (error === null || typeof error !== "object") return undefined;
  const reason = (error as { readonly reason?: unknown }).reason;
  return typeof reason === "string" && reason.length > 0 && reason.length <= 120 ? reason : undefined;
}

/** The exact native identity required by Codex's `turn/interrupt` request. */
export interface ExecutorLiveBinding { readonly runtimeKind: string; }
export interface CodexLiveBinding extends ExecutorLiveBinding { readonly runtimeKind: "codex"; readonly threadId: string; readonly turnId: string; }
/** Package-internal Claude Code identity. It is intentionally not a package-root export. */
export interface ClaudeCodeLiveBinding extends ExecutorLiveBinding { readonly runtimeKind: "claude-code"; readonly sessionId: string; }

export interface ExecutorOperationRequest { readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION; /** Caller-stable idempotency key for this one executor operation. */ readonly requestId: string; }
export interface ExecutorStartRequest extends ExecutorOperationRequest { readonly workingDirectory: string; /** Ephemeral only: never copied into errors, results, or transport diagnostics. */ readonly instruction: string; /** Worker-owned context after command validation; null explicitly marks a non-Linear execution. No credentials or new wire fields. */ readonly linearExecution?: LinearExecutionIdentity | null; }
export interface ExecutorContinueRequest<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> extends ExecutorStartRequest { /** The previously returned, still-live native binding. */ readonly binding: TBinding; }
export interface ExecutorCancelRequest<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> extends ExecutorOperationRequest { readonly binding: TBinding; }
/** Hands one more instruction to the binding's running turn without interrupting it. */
export interface ExecutorSteerRequest<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> extends ExecutorOperationRequest { readonly binding: TBinding; /** Ephemeral only: never copied into errors, results, or transport diagnostics. */ readonly instruction: string; }
/**
 * Internal provider proof for a durable binding after Worker restart: a Run that ended
 * `succeeded`/`failed`, or one parked on `waiting_for_input`. An adapter that never parks a Run on
 * `waiting_for_input` rejects that outcome as an invalid request.
 */
export interface ExecutorTerminalReattestationRequest<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> extends ExecutorOperationRequest {
  readonly binding: TBinding;
  readonly expectedOutcome: "succeeded" | "failed" | "waiting_for_input";
}
/** The provider must return the exact binding it was asked to prove. */
export interface ExecutorTerminalReattestationResult<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> extends ExecutorOperationRequest {
  readonly status: "attested";
  readonly binding: TBinding;
}

/**
 * Exact provider proof used only to reconnect a durable, non-terminal Worker
 * record. It cannot create a successor binding or start new native work.
 */
export interface ExecutorRecoveryRequest<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> extends ExecutorOperationRequest {
  readonly runId: string;
  readonly binding: TBinding;
}

export interface ExecutorAcceptedResult<TOperation extends "start" | "continue", TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> { readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION; readonly requestId: string; readonly operation: TOperation; readonly status: "accepted"; readonly binding: TBinding; }
/** This acknowledges only an interrupt request; completion is observed separately. */
export interface ExecutorInterruptRequestedResult<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> { readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION; readonly requestId: string; readonly operation: "cancel"; readonly status: "interrupt_requested"; readonly binding: TBinding; }
export interface ExecutorFailedResult<TOperation extends CodexExecutorOperation, TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> { readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION; readonly requestId: string | null; readonly operation: TOperation; readonly status: "failed" | "invalid_request"; readonly binding: TBinding | null; readonly error: ExecutorError; }
export type ExecutorStartResult<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> = ExecutorAcceptedResult<"start", TBinding> | ExecutorFailedResult<"start", TBinding>;
export type ExecutorContinueResult<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> = ExecutorAcceptedResult<"continue", TBinding> | ExecutorFailedResult<"continue", TBinding>;
export type ExecutorCancelResult<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> = ExecutorInterruptRequestedResult<TBinding> | ExecutorFailedResult<"cancel", TBinding>;
/**
 * This acknowledges only that the running turn was handed the instruction; the native side may still be about to
 * read it, and the turn's outcome is observed separately. A turn that could no longer take it fails with
 * `executor.steer_too_late`.
 */
export interface ExecutorSteeredResult<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> { readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION; readonly requestId: string; readonly operation: "steer"; readonly status: "steered"; readonly binding: TBinding; }
export type ExecutorSteerResult<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> = ExecutorSteeredResult<TBinding> | ExecutorFailedResult<"steer", TBinding>;

interface ExecutorLifecycleEventBase<TBinding extends ExecutorLiveBinding> {
  readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION;
  readonly binding: TBinding;
  readonly sequence: number;
}

/** A provider-bound terminal response that has already crossed the safe display boundary. */
export interface ExecutorTerminalResponse {
  readonly sourceEventId: string;
  readonly projectionStatus: "projected" | "fallback";
  readonly body: string;
  readonly truncated: boolean;
  readonly truncations: readonly AgentActivityTruncation[];
}

/** A redacted lifecycle fact for one already accepted live native binding. */
export type ExecutorLifecycleEvent<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> =
  | (ExecutorLifecycleEventBase<TBinding> & { readonly outcome: "started" })
  | (ExecutorLifecycleEventBase<TBinding> & {
      readonly outcome: "progressed";
      readonly progress: { readonly kind: "generic" } | { readonly kind: "commentary"; readonly safeMessage: string };
    })
  | (ExecutorLifecycleEventBase<TBinding> & { readonly outcome: "waiting_for_input"; readonly elicitation: StructuredElicitation })
  | (ExecutorLifecycleEventBase<TBinding> & { readonly outcome: "succeeded"; readonly response?: ExecutorTerminalResponse })
  | (ExecutorLifecycleEventBase<TBinding> & {
      readonly outcome: "failed";
      readonly failureKind?: ExecutorFailureKind;
    })
  | (ExecutorLifecycleEventBase<TBinding> & { readonly outcome: "cancelled" });

type ExecutorRecoveryTerminal<TBinding extends ExecutorLiveBinding> = Exclude<ExecutorLifecycleEvent<TBinding>, { readonly outcome: "started" | "progressed" }>;

/** The provider either restores observation of the same binding or proves one terminal already reached during downtime. */
export type ExecutorRecoveryResult<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> =
  | {
    readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION;
    readonly requestId: string;
    readonly runId: string;
    readonly status: "active";
    readonly binding: TBinding;
    readonly observe: AsyncIterable<ExecutorLifecycleEvent<TBinding>>;
  }
  | {
    readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION;
    readonly requestId: string;
    readonly runId: string;
    readonly status: "terminal";
    readonly binding: TBinding;
    readonly terminal: ExecutorRecoveryTerminal<TBinding>;
  };

/** Provider-owned adapter. A registration exposes only the methods it supports. */
export interface Executor<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> {
  readonly contractVersion: typeof CODEX_EXECUTOR_CONTRACT_VERSION;
  start?(request: ExecutorStartRequest): Promise<ExecutorStartResult<TBinding>>;
  continue?(request: ExecutorContinueRequest<TBinding>): Promise<ExecutorContinueResult<TBinding>>;
  cancel?(request: ExecutorCancelRequest<TBinding>): Promise<ExecutorCancelResult<TBinding>>;
  /** Optional: only a registration that lists `steer` may be asked, and it must then implement this. */
  steer?(request: ExecutorSteerRequest<TBinding>): Promise<ExecutorSteerResult<TBinding>>;
  /** Rejects malformed or unprovable bindings rather than guessing native state. */
  observe?(binding: TBinding): AsyncIterable<ExecutorLifecycleEvent<TBinding>>;
  /** Optional native recovery proof for an already persisted binding. */
  recover?(input: ExecutorRecoveryRequest<TBinding>): Promise<ExecutorRecoveryResult<TBinding> | null>;
}

/** The only Worker-to-provider lifecycle bridge; its values are safe to persist and route. */
export type AxisSafeLifecycleProjection =
  | { readonly kind: "progress"; readonly completed: number; readonly total: number; readonly safeMessage: string }
  | { readonly kind: "succeeded"; readonly resultRef: string; readonly response?: ExecutorTerminalResponse }
  | { readonly kind: "waiting_for_input"; readonly elicitation: StructuredElicitation }
  | { readonly kind: "failed"; readonly code: string; readonly safeMessage: string };

export interface ExecutorRegistration<TBinding extends ExecutorLiveBinding = ExecutorLiveBinding> {
  readonly runtimeKind: string;
  readonly executorId: string;
  /** Opaque to Worker; the registration alone defines equality. */
  readonly methodRef: MethodRef;
  readonly supportedOperations: readonly ExecutorCapability[];
  readonly adapter: Executor<TBinding>;
  validateBinding(value: unknown): value is TBinding;
  bindingsEqual(left: TBinding, right: TBinding): boolean;
  methodRefEquals(left: MethodRef, right: MethodRef): boolean;
  projectLifecycle(input: {
    readonly event: Exclude<ExecutorLifecycleEvent<TBinding>, { readonly outcome: "started" }>;
    readonly runId: string;
  }): AxisSafeLifecycleProjection;
  /**
   * Optional provider-owned restart proof for a Run that ended `succeeded`/`failed` or is parked on
   * `waiting_for_input`. It never starts, observes or mutates a successor. `null` means the binding
   * cannot be resumed (the provider declined, or the adapter lacks the evidence) and Worker answers
   * `axis.worker.continuation_unresumable`. An unreachable oracle or invalid request must reject
   * instead; Worker answers `axis.worker.capability_unavailable`.
   */
  reattestTerminal?(input: ExecutorTerminalReattestationRequest<TBinding>): Promise<ExecutorTerminalReattestationResult<TBinding> | null>;
  /** Optional provider-owned active-run recovery. Missing or unprovable recovery remains fail-closed. */
  recover?(input: ExecutorRecoveryRequest<TBinding>): Promise<ExecutorRecoveryResult<TBinding> | null>;
}

export interface ExecutorSelectionInput { readonly runtimeKind: string; readonly executorId: string; readonly methodRef: MethodRef; }

function nonblank(value: unknown): value is string { return typeof value === "string" && value.trim().length > 0; }
function methodRefIsValid(value: unknown): value is MethodRef {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const candidate = value as Readonly<Record<string, unknown>>;
  return Object.keys(candidate).length === 2 && nonblank(candidate.version) && nonblank(candidate.method);
}

/** Select exactly one reviewed registration before the Worker accepts commands. */
export function selectExecutorRegistration(input: ExecutorSelectionInput, registrations: readonly ExecutorRegistration[]): ExecutorRegistration {
  if (!nonblank(input.runtimeKind) || !nonblank(input.executorId) || !methodRefIsValid(input.methodRef) || registrations.length === 0) throw new Error("Worker executor registration is invalid.");
  const identities = new Set<string>();
  const matching: ExecutorRegistration[] = [];
  for (const registration of registrations) {
    if (!nonblank(registration.runtimeKind) || !nonblank(registration.executorId) || !methodRefIsValid(registration.methodRef) || registration.adapter.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || !Array.isArray(registration.supportedOperations) || new Set(registration.supportedOperations).size !== registration.supportedOperations.length || registration.supportedOperations.some((operation) => !["start", "continue", "cancel", "steer", "observe"].includes(operation)) || (registration.supportedOperations.includes("start") && typeof registration.adapter.start !== "function") || (registration.supportedOperations.includes("continue") && typeof registration.adapter.continue !== "function") || (registration.supportedOperations.includes("cancel") && typeof registration.adapter.cancel !== "function") || (registration.supportedOperations.includes("steer") && typeof registration.adapter.steer !== "function") || (registration.supportedOperations.includes("observe") && typeof registration.adapter.observe !== "function") || typeof registration.validateBinding !== "function" || typeof registration.bindingsEqual !== "function" || typeof registration.methodRefEquals !== "function" || typeof registration.projectLifecycle !== "function" || (registration.reattestTerminal !== undefined && typeof registration.reattestTerminal !== "function") || (registration.recover !== undefined && typeof registration.recover !== "function")) throw new Error("Worker executor registration is invalid.");
    const identity = `${registration.runtimeKind}\u0000${registration.executorId}\u0000${registration.methodRef.version}\u0000${registration.methodRef.method}`;
    if (identities.has(identity)) throw new Error("Worker executor registration is invalid.");
    identities.add(identity);
    let methodMatches = false;
    try { methodMatches = registration.methodRefEquals(registration.methodRef, input.methodRef); } catch { throw new Error("Worker executor registration is invalid."); }
    if (registration.runtimeKind === input.runtimeKind && registration.executorId === input.executorId && methodMatches) matching.push(registration);
  }
  if (matching.length !== 1) throw new Error("Worker executor registration is invalid.");
  return matching[0]!;
}
