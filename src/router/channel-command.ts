/** Execution command boundary between a channel adapter and Router. */
import { ChannelIdentityError, channelIdentityToRouterTaskRef, linearRoutingScope, linearSourceRef, linearTenantRef } from "./channel-identity.js";
import type { ChannelRoutingScope, ChannelSourceRef, LinearChannelIdentity, TenantRef } from "./channel-identity.js";
import type { RouterCommandResult, RouterTaskRef } from "./router.js";

import { validateWorkerSelection, type WorkerSelection } from "../shared/worker-selection.js";

export const CHANNEL_COMMAND_CONTRACT_VERSION = "1.0.0" as const;

export type ChannelCommandKind = "start" | "continue" | "cancel";

export type ChannelCommandErrorCode =
  /** A field required by this boundary was missing, blank, or not a valid opaque identifier. */
  | "malformed"
  /** A compatibility path would otherwise erase a field it cannot represent. */
  | "unmodeled_input"
  /** A Linear-only conversion was asked to accept a non-Linear command. */
  | "system_unsupported"
  /** A Linear command reached the boundary without the `agentSessionId` `RouterTaskRef` requires. */
  | "linear_session_missing";

const errorMessages: Readonly<Record<ChannelCommandErrorCode, string>> = {
  malformed: "Channel command input is malformed.",
  unmodeled_input: "Channel command input carries fields this boundary does not model.",
  system_unsupported: "Channel command system is not supported by this conversion.",
  linear_session_missing: "Linear channel command requires agentSessionId.",
};

export class ChannelCommandError extends Error {
  readonly code: ChannelCommandErrorCode;
  constructor(code: ChannelCommandErrorCode, options?: ErrorOptions) {
    super(errorMessages[code], options);
    this.code = code;
    this.name = "ChannelCommandError";
  }
}

/**
 * Mirrors `text()` in `./router.ts`: a string with at least one non-whitespace
 * character, preserved verbatim. Router digests the raw instruction and keys
 * idempotency on the raw `commandId`, so trimming would change both.
 */
function channelCommandText(value: unknown): string {
  if (typeof value !== "string" || value.trim().length === 0) throw new ChannelCommandError("malformed");
  return value;
}

interface ChannelCommandEnvelope {
  readonly contractVersion: typeof CHANNEL_COMMAND_CONTRACT_VERSION;
  readonly kind: ChannelCommandKind;
  /** Sole idempotency key, matching `router_commands.command_id`. Channel-neutral already. */
  readonly commandId: string;

  readonly tenant: TenantRef;
  /** Closed, provider-discriminated conversation subject. */
  readonly source: ChannelSourceRef;
  /** Binding-resolution key only. Never used as a conversation key. */
  readonly scope: ChannelRoutingScope;
  /** The resolved routing selection. */
  readonly workerSelection?: WorkerSelection;
}

export interface ChannelStartCommand extends ChannelCommandEnvelope {
  readonly kind: "start";
  readonly instruction: string;
}
export interface ChannelContinueCommand extends ChannelCommandEnvelope {
  readonly kind: "continue";
  readonly instruction: string;
}
export interface ChannelCancelCommand extends ChannelCommandEnvelope {
  readonly kind: "cancel";
}
export type ChannelCommand = ChannelStartCommand | ChannelContinueCommand | ChannelCancelCommand;

/**
 * Neutral restatement of `RouterCommandResult`, so a channel adapter need not
 * import `router.ts`. `safeCode` is a bare `string`, not pinned to Router's enum.
 */
export type ChannelCommandOutcome =
  | { readonly commandId: string; readonly taskId: string; readonly runId: string; readonly status: "accepted" | "interrupt_requested" | "steered" }
  | { readonly commandId: string; readonly status: "rejected"; readonly safeCode: string };

/** The outcomes `Router#reconcileCommand` returns, keyed by `commandId` alone. */
export type ChannelReconcileOutcome = "accepted" | "interrupt_requested" | "steered" | "absent" | "unknown";

export interface ChannelReconcileQuery {
  readonly contractVersion: typeof CHANNEL_COMMAND_CONTRACT_VERSION;
  readonly commandId: string;
}

/** Interrupted-command lineage, matching `Router#reconcileInterruptedCommand`'s return shape. */
export interface ChannelInterruptedCommandRef {
  readonly taskId: string;
  readonly runId: string;
}

/** The boundary a channel adapter implements. */
export interface ChannelCommandPort {
  submit(command: ChannelCommand): Promise<ChannelCommandOutcome>;
  reconcile(query: ChannelReconcileQuery): ChannelReconcileOutcome;
  /** Router-backed adapters expose accepted lineage when available. */
  reconcileAccepted?(query: ChannelReconcileQuery): { readonly taskId: string; readonly runId: string } | null;
  reconcileInterrupted(query: ChannelReconcileQuery): ChannelInterruptedCommandRef | null;
  /**
   * Steered-command lineage, matching `Router#reconcileSteeredCommand`. A port without
   * it never answers `steered`.
   */
  reconcileSteered?(query: ChannelReconcileQuery): ChannelInterruptedCommandRef | null;
}

/**
 * Re-derives the identity through the `./channel-identity.ts` constructors: a
 * structural interface accepts an unvalidated literal, and the constructors freeze
 * what they return.
 */
function validatedIdentity(identity: LinearChannelIdentity): { readonly tenant: TenantRef; readonly source: ChannelSourceRef; readonly scope: ChannelRoutingScope } {
  try {
    if (identity.tenant.system !== "linear" || identity.source.system !== "linear" || identity.scope.system !== "linear") throw new ChannelIdentityError("expected a Linear-system identity");
    return {
      tenant: linearTenantRef(identity.tenant.tenantId),
      source: linearSourceRef({ issueId: identity.source.issueId, ...(identity.source.agentSessionId === undefined ? {} : { agentSessionId: identity.source.agentSessionId }) }),
      scope: linearRoutingScope(identity.scope.scopeId),
    };
  } catch (error) {
    if (error instanceof ChannelIdentityError) throw new ChannelCommandError("malformed", { cause: error });
    throw error;
  }
}

function commandEnvelope(kind: ChannelCommandKind, commandId: string, identity: LinearChannelIdentity): ChannelCommandEnvelope {
  return {
    contractVersion: CHANNEL_COMMAND_CONTRACT_VERSION,
    kind,
    commandId: channelCommandText(commandId),
    ...validatedIdentity(identity),
  };
}

function channelSelection(input: { readonly workerSelection?: WorkerSelection }): { readonly workerSelection?: WorkerSelection } {
  if (!Object.hasOwn(input, "workerSelection")) return {};
  try { return { workerSelection: validateWorkerSelection(input.workerSelection) }; }
  catch (cause) { throw new ChannelCommandError("malformed", { cause }); }
}

/** command starts must resolve labels before admission; omission never means default. */
export function channelWorkerSelection(command: ChannelStartCommand): WorkerSelection {
  if (!Object.hasOwn(command, "workerSelection")) throw new ChannelCommandError("unmodeled_input");
  const selected = channelSelection(command);
  if (selected.workerSelection === undefined) throw new ChannelCommandError("unmodeled_input");
  return selected.workerSelection;
}

/** Neutral `start`. */
export function channelStartCommand(input: { readonly commandId: string; readonly identity: LinearChannelIdentity; readonly instruction: string; readonly workerSelection?: WorkerSelection }): ChannelStartCommand {
  return Object.freeze({ ...commandEnvelope("start", input.commandId, input.identity), ...channelSelection(input), kind: "start" as const, instruction: channelCommandText(input.instruction) });
}

/** Neutral `continue`. Carries a fresh instruction, matching `RouterContinueInput`. */
export function channelContinueCommand(input: { readonly commandId: string; readonly identity: LinearChannelIdentity; readonly instruction: string; readonly workerSelection?: WorkerSelection }): ChannelContinueCommand {
  return Object.freeze({ ...commandEnvelope("continue", input.commandId, input.identity), ...channelSelection(input), kind: "continue" as const, instruction: channelCommandText(input.instruction) });
}

/** Neutral `cancel`. Carries no instruction, matching `RouterCancelInput` and Router's cancel intent. */
export function channelCancelCommand(input: { readonly commandId: string; readonly identity: LinearChannelIdentity; readonly workerSelection?: WorkerSelection }): ChannelCancelCommand {
  return Object.freeze({ ...commandEnvelope("cancel", input.commandId, input.identity), ...channelSelection(input), kind: "cancel" as const });
}

export function channelReconcileQuery(commandId: string): ChannelReconcileQuery {
  return Object.freeze({ contractVersion: CHANNEL_COMMAND_CONTRACT_VERSION, commandId: channelCommandText(commandId) });
}

// ---------------------------------------------------------------------------
// Narrowing to Router's task refs
// ---------------------------------------------------------------------------

export function linearTaskRefFromChannelCommand(command: ChannelCommand): RouterTaskRef {
  const { tenant, source, scope } = command;
  if (tenant.system !== "linear" || source.system !== "linear" || scope.system !== "linear") throw new ChannelCommandError("system_unsupported");
  try {
    return channelIdentityToRouterTaskRef({ tenant, source, scope });
  } catch (error) {
    if (error instanceof ChannelIdentityError) throw new ChannelCommandError("linear_session_missing", { cause: error });
    throw error;
  }
}

/** Restates a `RouterCommandResult` as a neutral outcome. */
export function channelCommandOutcomeFromRouterResult(result: RouterCommandResult): ChannelCommandOutcome {
  return result.status === "rejected"
    ? Object.freeze({ commandId: result.commandId, status: "rejected" as const, safeCode: result.safeCode })
    : Object.freeze({ commandId: result.commandId, taskId: result.taskId, runId: result.runId, status: result.status });
}
