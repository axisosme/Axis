import { axisAgentGuidanceSections, resolveAgentInstructions, type AgentInstructionOverrides, type AgentInstructions } from "./response-guidance.js";
import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { accessSync, readFileSync, realpathSync, statSync } from "node:fs";
import { constants } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";

import { agentActivityFallbackResponseBody, truncateAgentActivityText } from "@axisosme/axis-contracts/agent-activity";
import { parse as parseToml } from "smol-toml";
import { elicitationFromCodexRequest, type StructuredElicitation } from "../shared/elicitation.js";

import {
  CODEX_EXECUTOR_CONTRACT_VERSION,
  executorError,
  normalizeExecutorCommentary,
  normalizeExecutorTerminalResponse,
  type CodexLiveBinding,
  type ExecutorCancelRequest,
  type ExecutorCancelResult,
  type ExecutorContinueRequest,
  type ExecutorContinueResult,
  type ExecutorFailedResult,
  type ExecutorFailureKind,
  type ExecutorLifecycleEvent,
  type ExecutorLifecycleOutcome,
  type ExecutorRecoveryRequest,
  type ExecutorRecoveryResult,
  type ExecutorTerminalReattestationRequest,
  type ExecutorTerminalReattestationResult,
  type ExecutorTerminalResponse,
  type ExecutorRegistration,
  type CodexExecutorOperation,
  type ExecutorStartRequest,
  type ExecutorStartResult,
  type Executor,
} from "../shared/worker-executor.js";

export interface CodexAppServerStartTurnInput {
  readonly workingDirectory: string;
  /** Sent only through the app-server's stdin JSON-RPC request. */
  readonly instruction: string;
}

/** The closed set Axis configures; request routing never invents profile names. */
export const CODEX_AGENT_PROFILE_ROLES = ["explorer", "architect", "reviewer", "worker"] as const;
export type CodexAgentProfileRole = typeof CODEX_AGENT_PROFILE_ROLES[number];
export type CodexProfileAvailability = "available" | "unavailable";
export type CodexProfileAttempt = "not_attempted";

export interface CodexAgentProfileRegistry {
  /** Canonical regular TOML paths, deliberately internal to the app-server launch. */
  readonly configuredPaths: Readonly<Partial<Record<CodexAgentProfileRole, string>>>;
  /** Stable, path-free source health used by readiness. */
  readonly sourceAvailability: Readonly<Record<CodexAgentProfileRole, CodexProfileAvailability>>;
}

export interface CodexAppServerReadiness {
  readonly status: "ready" | "degraded";
  /** Model routing is intentionally not probed by the profile registry check. */
  readonly modelAvailability: "not_checked";
  readonly profiles: Readonly<Record<CodexAgentProfileRole, { readonly availability: CodexProfileAvailability; readonly attempt: CodexProfileAttempt }>>;
  /** Closed operational reasons only: no paths, native output, config, or profile content. */
  readonly degradedReasons: readonly ("app_server_unavailable" | "app_server_identity_unavailable" | "app_server_config_unavailable" | "profile_registry_unavailable")[];
}

export interface ResolveCodexAgentProfileRegistryOptions {
  readonly codexHome?: string;
  readonly realpath?: (path: string) => string;
  readonly stat?: (path: string) => { isFile(): boolean };
  readonly access?: (path: string) => void;
  readonly read?: (path: string) => string;
}

function profileRole(value: string): value is CodexAgentProfileRole {
  return (CODEX_AGENT_PROFILE_ROLES as readonly string[]).includes(value) && /^[a-z][a-z0-9_-]{0,63}$/u.test(value);
}

/**
 * Resolve symlinked profile sources once, before Codex reads them. Passing a
 * canonical regular path in `-c agents.<role>.config_file=...` avoids asking
 * the native process to follow an operator-managed final symlink component.
 */
export function resolveCodexAgentProfileRegistry(options: ResolveCodexAgentProfileRegistryOptions = {}): CodexAgentProfileRegistry {
  const configuredPaths: Partial<Record<CodexAgentProfileRole, string>> = {};
  const sourceAvailability = {} as Record<CodexAgentProfileRole, CodexProfileAvailability>;
  const source = join(options.codexHome ?? join(homedir(), ".codex"), "agents");
  const resolve = options.realpath ?? realpathSync;
  const stat = options.stat ?? statSync;
  const access = options.access ?? ((path: string) => accessSync(path, constants.R_OK));
  const read = options.read ?? ((path: string) => readFileSync(path, "utf8"));
  for (const role of CODEX_AGENT_PROFILE_ROLES) {
    if (!profileRole(role)) throw new Error("Codex profile registry is invalid.");
    try {
      const canonical = resolve(join(source, `${role}.toml`));
      if (!isAbsolute(canonical) || !stat(canonical).isFile()) throw new Error("invalid profile source");
      access(canonical);
      const profile = parseToml(read(canonical)) as Record<string, unknown>;
      if (profile.name !== role || typeof profile.description !== "string" || profile.description.trim().length === 0 || typeof profile.developer_instructions !== "string" || profile.developer_instructions.trim().length === 0) throw new Error("invalid profile content");
      configuredPaths[role] = canonical;
      sourceAvailability[role] = "available";
    } catch {
      sourceAvailability[role] = "unavailable";
    }
  }
  return { configuredPaths, sourceAvailability };
}

function emptyCodexAgentProfileRegistry(): CodexAgentProfileRegistry {
  return {
    configuredPaths: {},
    sourceAvailability: Object.fromEntries(CODEX_AGENT_PROFILE_ROLES.map((role) => [role, "unavailable"])) as CodexAgentProfileRegistry["sourceAvailability"],
  };
}

export function codexProfileRoutingInstructions(workerFallbackPolicy: "allow" | "deny"): string {
  const fallback = workerFallbackPolicy === "allow"
    ? "Default-runtime fallback is permitted only when the request policy also permits it."
    : "Default-runtime fallback is forbidden by the Worker policy.";
  return [
    "Axis profile-routing policy:",
    "- Resolve and attempt each requested agent role independently. A failure for one role never implies another role is unavailable.",
    "- Keep unattempted roles as not_attempted; mark only an attempted failing role as unavailable.",
    "- Keep model availability separate from profile availability. Do not change model or provider because a profile is unavailable.",
    `- ${fallback}`,
    "- If default fallback starts, report model_active, requested_profile_unavailable, runtime_role_default, and that profile-specific instructions were not loaded.",
  ].join("\n");
}

/**
 * Session transport context: the facts that are true of a Codex thread in
 * particular. It describes projection without restricting native tools or auth.
 * The shared sections are sent alongside it by `#composeDeveloperInstructions`.
 */
export function axisManagedRuntimeInstructions(): string {
  return [
    "Axis session transport context:",
    "- This Codex thread is connected to a Linear AgentSession through Axis.",
    "- Progress and the final result reach that session through the normal Codex response channel.",
    "- When Axis Linear execution clients are available for this Run, they provide the AgentSession identity.",
  ].join("\n");
}

export interface CodexAppServerContinueTurnInput extends CodexAppServerStartTurnInput {
  readonly binding: CodexLiveBinding;
}

/** Safe, binding-scoped projection of one app-server turn notification. */
export type CodexAppServerTurnLifecycleEvent =
  | { readonly binding: CodexLiveBinding; readonly outcome: "started" | "cancelled" }
  | { readonly binding: CodexLiveBinding; readonly outcome: "failed"; readonly failureKind?: ExecutorFailureKind }
  | { readonly binding: CodexLiveBinding; readonly outcome: "waiting_for_input"; readonly elicitation: StructuredElicitation }
  | { readonly binding: CodexLiveBinding; readonly outcome: "succeeded"; readonly response?: ExecutorTerminalResponse }
  | { readonly binding: CodexLiveBinding; readonly outcome: "progressed"; readonly progress: { readonly kind: "commentary"; readonly safeMessage: string } };

/**
 * A process-local app-server session. The same transport must survive from
 * start through continuation and interruption so its returned turn IDs remain
 * truthful. It is deliberately injectable for deterministic worker tests.
 */
export interface CodexAppServerTransport {
  startTurn(input: CodexAppServerStartTurnInput): Promise<CodexLiveBinding>;
  continueTurn(input: CodexAppServerContinueTurnInput): Promise<CodexLiveBinding>;
  interruptTurn(binding: CodexLiveBinding): Promise<void>;
  observeTurn(binding: CodexLiveBinding): AsyncIterable<CodexAppServerTurnLifecycleEvent>;
  /** Optional exact native rebind proof supplied by a restart-capable app-server transport. */
  recoverTurn?(binding: CodexLiveBinding): Promise<AsyncIterable<CodexAppServerTurnLifecycleEvent> | null>;
  /**
   * Read-only restart proof: `true` when the durable thread is still listed as
   * resumable, `false` only when the provider definitively does not list it. Rejects when the
   * provider cannot be asked or answers malformed.
   */
  threadResumable?(threadId: string): Promise<boolean>;
  /** Closed per-process profile/configuration evidence, if this transport owns an app-server. */
  readiness?(): Promise<CodexAppServerReadiness>;
  /** Best-effort process cleanup during a Worker shutdown. */
  close?(): void;
}

export interface CodexExecutorOptions {
  /** Absolute package-local `@openai/codex` executable entry; never PATH-resolved. */
  readonly command: string;
  readonly transport?: CodexAppServerTransport;
  readonly agentProfiles?: CodexAgentProfileRegistry | (() => CodexAgentProfileRegistry);
  readonly workerFallbackPolicy?: "allow" | "deny";
  /** This agent's instruction configuration, forwarded to a transport this executor creates itself. */
  readonly instructions?: AgentInstructionOverrides;
  /**
   * Set only by an executor whose `continue` resumes the durable thread in a fresh app-server
   * (`LinearCodexExecutor`). Without it `reattestTerminal` answers `null`: this executor's own
   * `continue` never resumes, and a fresh app-server answers `turn/start` on a thread it never
   * loaded with "thread not found".
   */
  readonly resumesThreadOnContinue?: boolean;
}

export interface PackageLocalCodexAppServerTransportOptions {
  readonly command: string;
  /** Worker-owned private configuration, never sourced from instructions or wire payloads. */
  readonly executionEnvironment?: NodeJS.ProcessEnv;
  readonly ephemeral?: boolean;
  /**
   * Resume the durable thread before a successor turn so it adopts this
   * process's execution environment. Only a caller that issues a fresh
   * per-Run credential may enable it.
   */
  readonly resumeBeforeContinue?: boolean;
  /** Test-only injection point. Production always uses node's `spawn`. */
  readonly spawnProcess?: (file: string, args: readonly string[], options: AppServerSpawnOptions) => ChildProcess;
  /** Re-evaluated once for every app-server process generation. */
  readonly agentProfiles?: CodexAgentProfileRegistry | (() => CodexAgentProfileRegistry);
  readonly workerFallbackPolicy?: "allow" | "deny";
  /** This agent's instruction configuration. Absent sections take the Axis defaults. */
  readonly instructions?: AgentInstructionOverrides;
}

export interface AppServerSpawnOptions {
  readonly env?: NodeJS.ProcessEnv;
  readonly shell: false;
  readonly stdio: ["pipe", "pipe", "ignore"];
  readonly detached: false;
}

type JsonRecord = Readonly<Record<string, unknown>>;

/**
 * `thread/list` query for the restart proof, against the locked codex version.
 * Axis threads are recorded with source `vscode` even though Axis speaks app-server, so the
 * query names every top-level source kind rather than `appServer` alone, and leaves out the
 * sub-agent kinds, which Axis never binds. `modelProviders: []` means every provider, so a
 * provider change since the thread was created cannot hide it. `archived: false` is the point:
 * an archived thread still answers `thread/read` but can no longer be resumed. The server caps
 * a page at 100 threads.
 */
function codexResumableThreadQuery(cursor: string | undefined): JsonRecord {
  return {
    archived: false,
    modelProviders: [],
    sourceKinds: ["cli", "vscode", "exec", "appServer", "unknown"],
    limit: 100,
    ...(cursor === undefined ? {} : { cursor }),
  };
}
/** 5,000 threads. Hitting the cap without an answer rejects; it never reads as "not listed". */
const CODEX_RESUMABLE_THREAD_PAGE_CAP = 50;

/**
 * Axis has no synchronous operator sitting behind app-server approval RPCs.
 * Run the native Codex session in the same non-interactive full-access posture
 * as an operator-started Codex session: never ask and do not add an Axis
 * sandbox around the host account.
 */
function fullAccessCodexThreadPolicy(): {
  readonly approvalPolicy: "never";
  readonly sandbox: "danger-full-access";
} {
  return { approvalPolicy: "never", sandbox: "danger-full-access" };
}

function fullAccessCodexTurnPolicy(): {
  readonly approvalPolicy: "never";
  readonly sandboxPolicy: {
    readonly type: "dangerFullAccess";
  };
} {
  return {
    approvalPolicy: "never",
    sandboxPolicy: { type: "dangerFullAccess" },
  };
}

function record(value: unknown): JsonRecord | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as JsonRecord
    : null;
}

function nonblank(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function exactKeys(value: JsonRecord, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

/**
 * Classifies only the documented app-server translation of Codex's native
 * `server_overloaded` error identifier. Provider prose is never inspected.
 */
export function classifyCodexTurnFailure(turn: unknown): ExecutorFailureKind | null {
  const candidate = record(turn);
  const error = record(candidate?.error);
  // `TurnError` requires only `message`; its other members may be omitted or null.
  if (candidate?.status !== "failed" || error === null
    || !Object.keys(error).every(key => ["message", "codexErrorInfo", "additionalDetails", "misalignment"].includes(key))) return null;
  const additionalDetails = error.additionalDetails ?? null, misalignment = error.misalignment ?? null;
  if (typeof error.message !== "string" || !(additionalDetails === null || typeof additionalDetails === "string")
    || !(misalignment === null || record(misalignment) !== null)) return null;
  return error.codexErrorInfo === "serverOverloaded" ? "model_capacity" : null;
}

function identifier(value: string): boolean {
  return [...value].length <= 128 && /^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(value);
}

function stableSourceEventId(binding: CodexLiveBinding, nativeId: string): string {
  if (identifier(nativeId)) return nativeId;
  return `codex-event-${createHash("sha256").update(`${binding.threadId}\u0000${binding.turnId}\u0000${nativeId}`).digest("hex").slice(0, 48)}`;
}

function requestId(value: unknown): string | null { return nonblank(record(value)?.requestId); }

function binding(value: unknown): CodexLiveBinding | null {
  const candidate = record(value)?.binding;
  const native = record(candidate);
  const threadId = nonblank(native?.threadId);
  const turnId = nonblank(native?.turnId);
  return native?.runtimeKind === "codex" && threadId !== null && turnId !== null
    ? { runtimeKind: "codex", threadId, turnId }
    : null;
}

function startRequestIsValid(value: unknown): value is ExecutorStartRequest {
  const candidate = record(value);
  return candidate?.contractVersion === CODEX_EXECUTOR_CONTRACT_VERSION
    && requestId(value) !== null
    && typeof candidate.workingDirectory === "string"
    && candidate.workingDirectory.trim().length > 0
    && isAbsolute(candidate.workingDirectory)
    && typeof candidate.instruction === "string"
    && candidate.instruction.trim().length > 0;
}

function continueRequestIsValid(value: unknown): value is ExecutorContinueRequest {
  return startRequestIsValid(value) && binding(value) !== null;
}

function cancelRequestIsValid(value: unknown): value is ExecutorCancelRequest {
  const candidate = record(value);
  return candidate?.contractVersion === CODEX_EXECUTOR_CONTRACT_VERSION && requestId(value) !== null && binding(value) !== null;
}

function failed<TOperation extends CodexExecutorOperation>(
  operation: TOperation,
  input: unknown,
  status: "failed" | "invalid_request",
  code: Parameters<typeof executorError>[1],
  nativeBinding: CodexLiveBinding | null = null,
): ExecutorFailedResult<TOperation, CodexLiveBinding> {
  return {
    contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
    requestId: requestId(input),
    operation,
    status,
    binding: nativeBinding,
    error: executorError(operation, code),
  };
}

function sameBinding(left: CodexLiveBinding, right: CodexLiveBinding): boolean {
  return left.runtimeKind === right.runtimeKind && left.threadId === right.threadId && left.turnId === right.turnId;
}

function liveBindingIsValid(value: unknown): value is CodexLiveBinding {
  const candidate = record(value);
  return candidate?.runtimeKind === "codex" && nonblank(candidate.threadId) !== null && nonblank(candidate.turnId) !== null;
}

/** The reviewed production registration; Worker never needs to name Codex to use it. */
export function codexExecutorRegistration(input: {
  readonly executorId: string;
  readonly adapter: CodexExecutor;
}): ExecutorRegistration<CodexLiveBinding> {
  const methodRef = { version: "v1", method: "codex.execute" } as const;
  return {
    runtimeKind: "codex",
    executorId: input.executorId,
    methodRef,
    supportedOperations: ["start", "continue", "cancel", "observe"],
    adapter: input.adapter,
    validateBinding: liveBindingIsValid,
    bindingsEqual: sameBinding,
    methodRefEquals: (left, right) => left.version === right.version && left.method === right.method,
    projectLifecycle: ({ event, runId }) => {
      if (event.outcome === "progressed") return { kind: "progress", completed: 1, total: 1, safeMessage: event.progress.kind === "commentary" ? event.progress.safeMessage : "Codex session is progressing." };
      if (event.outcome === "waiting_for_input") return { kind: "waiting_for_input", elicitation: event.elicitation };
      if (event.outcome === "succeeded") return { kind: "succeeded", resultRef: `axis-evidence://${runId}/codex-terminal`, ...(event.response === undefined ? {} : { response: event.response }) };
      if (event.outcome === "failed") return event.failureKind === "model_capacity"
        ? { kind: "failed", code: "codex_model_capacity", safeMessage: "Codex model capacity is temporarily unavailable. Retry this run after reviewing prior progress." }
        : { kind: "failed", code: "codex_turn_failed", safeMessage: "Codex turn failed." };
      if (event.outcome === "cancelled") return { kind: "failed", code: "execution_cancelled", safeMessage: "Execution was cancelled." };
      throw new Error("The executor lifecycle is invalid.");
    },
    reattestTerminal: input.adapter.reattestTerminal.bind(input.adapter),
    recover: (request) => input.adapter.recover(request),
  };
}

interface PendingRequest {
  readonly resolve: (result: unknown) => void;
  readonly reject: () => void;
}

interface TerminalCandidate {
  readonly sourceEventId: string;
  readonly completedAtMs: number;
  readonly ordinal: number;
  readonly body: string;
  readonly truncated: boolean;
  readonly truncations: ExecutorTerminalResponse["truncations"];
}
interface TerminalCandidateState { finalAnswer?: TerminalCandidate; unphasedAnswer?: TerminalCandidate; unphasedAmbiguous: boolean; }

interface PendingElicitation {
  readonly requestId: number | string;
  readonly binding: CodexLiveBinding;
  readonly elicitation: StructuredElicitation;
  interruptAcknowledged: boolean;
  interruptedTerminalSeen: boolean;
}

function lifecycleKey(nativeBinding: CodexLiveBinding): string {
  return `${nativeBinding.threadId}\u0000${nativeBinding.turnId}`;
}

function lifecycleError(): Error { return new Error("Codex app-server lifecycle is unavailable."); }

class TurnLifecycleObserver implements AsyncIterable<CodexAppServerTurnLifecycleEvent> {
  readonly #events: CodexAppServerTurnLifecycleEvent[] = [];
  readonly #waiters: (() => void)[] = [];
  #state: "awaiting_start" | "running" | "terminal" = "awaiting_start";
  #failure = false;

  consume(event: CodexAppServerTurnLifecycleEvent): void {
    if (this.#failure || this.#state === "terminal") return;
    if (event.outcome === "started") {
      if (this.#state !== "awaiting_start") return this.fail();
      this.#state = "running";
    } else if (event.outcome === "progressed") {
      if (this.#state !== "running") return this.fail();
    } else {
      if (this.#state !== "running") return this.fail();
      this.#state = "terminal";
    }
    this.#events.push(event);
    this.#wake();
  }

  fail(): void {
    if (this.#failure || this.#state === "terminal") return;
    this.#failure = true;
    this.#wake();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<CodexAppServerTurnLifecycleEvent> {
    for (;;) {
      const event = this.#events.shift();
      if (event !== undefined) {
        yield event;
        continue;
      }
      if (this.#failure) throw lifecycleError();
      if (this.#state === "terminal") return;
      await new Promise<void>((resolve) => this.#waiters.push(resolve));
    }
  }

  #wake(): void {
    for (const waiter of this.#waiters.splice(0)) waiter();
  }
}

/**
 * Minimal package-local JSON-RPC client for the `codex app-server`
 * stdio transport. It intentionally owns one live server process; replacing
 * that process would lose the only authority able to interrupt its live turn.
 */
export class PackageLocalCodexAppServerTransport implements CodexAppServerTransport {
  readonly #command: string;
  readonly #spawnProcess: (file: string, args: readonly string[], options: AppServerSpawnOptions) => ChildProcess;
  readonly #resolveAgentProfiles: () => CodexAgentProfileRegistry;
  readonly #workerFallbackPolicy: "allow" | "deny";
  readonly #executionEnvironment: NodeJS.ProcessEnv | undefined;
  readonly #instructions: AgentInstructions;
  readonly #ephemeral: boolean;
  readonly #resumeBeforeContinue: boolean;
  #activeAgentProfiles = emptyCodexAgentProfileRegistry();
  #child: ChildProcess | undefined;
  #initialization: Promise<void> | undefined;
  #capability: Promise<CodexAppServerReadiness> | undefined;
  #threadDeveloperInstructions: string;
  #nextRequestId = 0;
  #buffer = "";
  readonly #pending = new Map<number, PendingRequest>();
  readonly #lifecycleBuffers = new Map<string, CodexAppServerTurnLifecycleEvent[]>();
  readonly #lifecycleObservers = new Map<string, TurnLifecycleObserver>();
  readonly #ambiguousThreads = new Set<string>();
  readonly #workingDirectories = new Map<string, string>();
  readonly #pendingWorkingDirectories = new Map<string, string>();
  readonly #terminalCandidates = new Map<string, TerminalCandidateState>();
  readonly #pendingElicitations = new Map<string, PendingElicitation>();
  readonly #serverRequestIds = new Set<string>();
  #notificationOrdinal = 0;

  constructor(options: PackageLocalCodexAppServerTransportOptions) {
    this.#command = options.command;
    this.#executionEnvironment = options.executionEnvironment === undefined ? undefined : { ...options.executionEnvironment };
    this.#ephemeral = options.ephemeral ?? false;
    this.#resumeBeforeContinue = options.resumeBeforeContinue ?? false;
    this.#spawnProcess = options.spawnProcess ?? ((file, args, spawnOptions) => spawn(file, [...args], spawnOptions));
    const agentProfiles = options.agentProfiles;
    this.#resolveAgentProfiles = typeof agentProfiles === "function" ? agentProfiles : () => agentProfiles ?? emptyCodexAgentProfileRegistry();
    this.#workerFallbackPolicy = options.workerFallbackPolicy ?? "allow";
    this.#instructions = resolveAgentInstructions(options.instructions);
    this.#threadDeveloperInstructions = this.#composeDeveloperInstructions();
  }

  /**
   * The one place a Codex thread's developer instructions are assembled.
   *
   * Used at construction, on the `config/read` reply that may prepend the
   * operator's own inherited instructions, and on the reset after a server
   * generation ends.
   *
   * Order is meaningful: profile routing, then the shared agent sections
   * (session response and the Linear status paragraph derived
   * from this process's own execution environment), then the Codex-specific
   * transport context. `inherited` is the operator's own configured text and
   * goes first, ahead of anything Axis says.
   */
  #composeDeveloperInstructions(inherited?: string | null): string {
    return [
      inherited ?? null,
      codexProfileRoutingInstructions(this.#workerFallbackPolicy),
      ...axisAgentGuidanceSections(this.#executionEnvironment, this.#instructions),
      axisManagedRuntimeInstructions(),
    ].filter((value): value is string => value !== null && value.length > 0).join("\n\n");
  }

  async readiness(): Promise<CodexAppServerReadiness> {
    try {
      await this.#ready();
      return this.#capability ?? this.#degraded("app_server_unavailable");
    } catch {
      return this.#degraded("app_server_unavailable");
    }
  }

  async startTurn(input: CodexAppServerStartTurnInput): Promise<CodexLiveBinding> {
    await this.#ready();
    const threadResponse = await this.#request("thread/start", {
      cwd: input.workingDirectory,
      ...fullAccessCodexThreadPolicy(),
      developerInstructions: this.#threadDeveloperInstructions,
      ...(this.#ephemeral ? { ephemeral: true } : {}),
      ...this.#threadConfig(),
    });
    const threadId = nonblank(record(record(threadResponse)?.thread)?.id);
    if (threadId === null) throw new Error("Codex app-server response is invalid.");
    return this.#startTurn(threadId, input);
  }

  async continueTurn(input: CodexAppServerContinueTurnInput): Promise<CodexLiveBinding> {
    if (this.#resumeBeforeContinue) await this.#resumeThread(input.binding.threadId, input.workingDirectory);
    return this.#startTurn(input.binding.threadId, input);
  }

  /** Per-thread overrides; `turn/start` accepts no configuration of its own. */
  #threadConfig(): { config?: JsonRecord } {
    if (this.#executionEnvironment === undefined) return {};
    return { config: {
      "shell_environment_policy.set": Object.fromEntries(Object.entries(this.#executionEnvironment).filter(([key]) => key.startsWith("LINEAR_"))),
    } };
  }

  /**
   * app-server rejoins a thread that is still loaded and silently discards
   * `config`, so a successor Run would keep the prior Run's revoked
   * credential while every response still reported success. Unsubscribe
   * first; a thread this process never loaded simply has nothing to drop.
   */
  async #resumeThread(threadId: string, workingDirectory: string): Promise<void> {
    await this.#request("thread/unsubscribe", { threadId }).catch(() => undefined);
    const resumed = await this.#request("thread/resume", {
      threadId,
      cwd: workingDirectory,
      ...fullAccessCodexThreadPolicy(),
      developerInstructions: this.#threadDeveloperInstructions,
      ...this.#threadConfig(),
    });
    if (nonblank(record(record(resumed)?.thread)?.id) !== threadId) throw new Error("Codex app-server did not resume the requested thread.");
  }

  async interruptTurn(nativeBinding: CodexLiveBinding): Promise<void> {
    await this.#request("turn/interrupt", { threadId: nativeBinding.threadId, turnId: nativeBinding.turnId });
  }

  /**
   * Membership in the non-archived `thread/list` view, paged to the end. `thread/read` alone is
   * not proof: it succeeds for an archived thread that `thread/resume` will not reopen. Nothing
   * here resumes, starts a turn on, or unarchives the thread.
   */
  async threadResumable(threadId: string): Promise<boolean> {
    let cursor: string | undefined;
    for (let page = 0; page < CODEX_RESUMABLE_THREAD_PAGE_CAP; page++) {
      const response = record(await this.#request("thread/list", codexResumableThreadQuery(cursor)));
      const data = response?.data;
      if (response === null || !Array.isArray(data)) throw new Error("Codex app-server response is invalid.");
      for (const entry of data) {
        const id = nonblank(record(entry)?.id);
        if (id === null) throw new Error("Codex app-server response is invalid.");
        if (id === threadId) return true;
      }
      if (response.nextCursor === undefined || response.nextCursor === null) return false;
      const next = nonblank(response.nextCursor);
      if (next === null || next === cursor) throw new Error("Codex app-server response is invalid.");
      cursor = next;
    }
    throw new Error("Codex app-server thread list exceeded its page cap.");
  }

  observeTurn(nativeBinding: CodexLiveBinding): AsyncIterable<CodexAppServerTurnLifecycleEvent> {
    if (!liveBindingIsValid(nativeBinding)) return this.#failedLifecycle();
    const key = lifecycleKey(nativeBinding);
    if (this.#lifecycleObservers.has(key) || this.#ambiguousThreads.has(nativeBinding.threadId)) return this.#failedLifecycle();
    const observer = new TurnLifecycleObserver();
    this.#lifecycleObservers.set(key, observer);
    for (const event of this.#lifecycleBuffers.get(key) ?? []) observer.consume(event);
    if (this.#isTerminal(key)) this.#lifecycleObservers.delete(key);
    return observer;
  }

  close(): void {
    this.#child?.kill("SIGTERM");
    this.#failed();
  }

  async #startTurn(threadId: string, input: CodexAppServerStartTurnInput): Promise<CodexLiveBinding> {
    this.#pendingWorkingDirectories.set(threadId, input.workingDirectory);
    try {
      const turnResponse = await this.#request("turn/start", {
        threadId,
        cwd: input.workingDirectory,
        ...fullAccessCodexTurnPolicy(),
        input: [{ type: "text", text: input.instruction, text_elements: [] }],
      });
      const turnId = nonblank(record(record(turnResponse)?.turn)?.id);
      if (turnId === null) throw new Error("Codex app-server response is invalid.");
      const nativeBinding = { runtimeKind: "codex", threadId, turnId } as const;
      if (!this.#isTerminal(lifecycleKey(nativeBinding))) this.#workingDirectories.set(lifecycleKey(nativeBinding), input.workingDirectory);
      return nativeBinding;
    } finally {
      this.#pendingWorkingDirectories.delete(threadId);
    }
  }

  async #request(method: string, params: JsonRecord): Promise<unknown> {
    await this.#ready();
    return this.#requestRaw(method, params);
  }

  async #ready(): Promise<void> {
    if (this.#initialization !== undefined) return this.#initialization;
    try {
      // Run the package-local JavaScript entry through this exact Node binary.
      // Executing the entrypoint directly would honor its env-based shebang and
      // silently depend on the LaunchAgent PATH containing `node`.
      try { this.#activeAgentProfiles = this.#resolveAgentProfiles(); } catch { this.#activeAgentProfiles = emptyCodexAgentProfileRegistry(); }
      const profileOverrides = CODEX_AGENT_PROFILE_ROLES.flatMap((role) => {
        const canonical = this.#activeAgentProfiles.configuredPaths[role];
        return canonical === undefined ? [] : ["-c", `agents.${role}.config_file=${JSON.stringify(canonical)}`];
      });
      const child = this.#spawnProcess(process.execPath, [this.#command, "app-server", "--stdio", ...profileOverrides], {
        shell: false, stdio: ["pipe", "pipe", "ignore"], detached: false,
        ...(this.#executionEnvironment === undefined ? {} : { env: this.#executionEnvironment }),
      });
      const stdin = child.stdin;
      const stdout = child.stdout;
      if (stdin === null || stdout === null) throw new Error("Codex app-server transport is unavailable.");
      this.#child = child;
      child.once("error", () => { if (this.#child === child) this.#failed(); });
      child.once("close", () => { if (this.#child === child) this.#failed(); });
      stdout.setEncoding("utf8");
      stdout.on("data", (chunk: string) => this.#onData(chunk));
      const initialization = this.#requestRaw("initialize", {
        clientInfo: { name: "axis-executor", version: CODEX_EXECUTOR_CONTRACT_VERSION },
        capabilities: null,
      }).then(async (initializeResult) => {
        this.#notify("initialized", {});
        this.#capability = this.#requestRaw("config/read", {}).then((configResult) => {
          const inherited = nonblank(record(record(configResult)?.config)?.developer_instructions);
          this.#threadDeveloperInstructions = this.#composeDeveloperInstructions(inherited);
          return this.#capabilitySnapshot(initializeResult, configResult);
        }).catch(() => this.#degraded("app_server_config_unavailable"));
        await this.#capability;
      }).catch(() => {
        this.#failed();
        throw new Error("Codex app-server transport is unavailable.");
      });
      this.#initialization = initialization;
      return initialization;
    } catch {
      this.#failed();
      throw new Error("Codex app-server transport is unavailable.");
    }
  }

  #requestRaw(method: string, params: JsonRecord): Promise<unknown> {
    const id = ++this.#nextRequestId;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject: () => reject(new Error("Codex app-server transport is unavailable.")) });
      if (!this.#write({ id, method, params })) {
        this.#pending.delete(id);
        reject(new Error("Codex app-server transport is unavailable."));
      }
    });
  }

  #notify(method: string, params: JsonRecord): void {
    this.#write({ method, params });
  }

  #write(message: JsonRecord): boolean {
    const stdin = this.#child?.stdin;
    if (stdin === null || stdin === undefined || stdin.destroyed) return false;
    try {
      stdin.write(`${JSON.stringify(message)}\n`);
      return true;
    } catch {
      return false;
    }
  }

  #onData(chunk: string): void {
    this.#buffer += chunk;
    for (;;) {
      const newline = this.#buffer.indexOf("\n");
      if (newline < 0) return;
      const line = this.#buffer.slice(0, newline);
      this.#buffer = this.#buffer.slice(newline + 1);
      let message: JsonRecord | null = null;
      try { message = record(JSON.parse(line)); } catch { this.#failed(); return; }
      if (message === null) continue;
      const id = typeof message.id === "number" || typeof message.id === "string" ? message.id : null;
      const method = nonblank(message.method);
      if (id !== null && method !== null) {
        this.#onServerRequest(id, method, message);
        continue;
      }
      if (id === null && method !== null) {
        this.#onNotification(message);
        continue;
      }
      if (id === null || typeof id !== "number" || method !== null) { this.#failed(); return; }
      const pending = this.#pending.get(id);
      if (pending === undefined) continue;
      this.#pending.delete(id);
      if (record(message.error) !== null) pending.reject();
      else pending.resolve(message.result);
    }
  }

  #failed(): void {
    this.#initialization = undefined;
    this.#capability = undefined;
    this.#threadDeveloperInstructions = this.#composeDeveloperInstructions();
    this.#child = undefined;
    for (const pending of this.#pending.values()) pending.reject();
    this.#pending.clear();
    for (const observer of this.#lifecycleObservers.values()) observer.fail();
    this.#lifecycleObservers.clear();
    this.#pendingWorkingDirectories.clear();
    this.#workingDirectories.clear();
    this.#terminalCandidates.clear();
    this.#pendingElicitations.clear();
    this.#serverRequestIds.clear();
  }

  #degraded(reason: CodexAppServerReadiness["degradedReasons"][number]): CodexAppServerReadiness {
    return {
      status: "degraded",
      modelAvailability: "not_checked",
      profiles: Object.fromEntries(CODEX_AGENT_PROFILE_ROLES.map((role) => [role, { availability: this.#activeAgentProfiles.sourceAvailability[role], attempt: "not_attempted" as const }])) as CodexAppServerReadiness["profiles"],
      degradedReasons: [reason],
    };
  }

  #capabilitySnapshot(initializeResult: unknown, configResult: unknown): CodexAppServerReadiness {
    const initialize = record(initializeResult);
    const config = record(record(configResult)?.config);
    const userAgent = nonblank(initialize?.userAgent);
    const codexHome = nonblank(initialize?.codexHome);
    const agents = record(config?.agents);
    const profiles = {} as Record<CodexAgentProfileRole, { availability: CodexProfileAvailability; attempt: CodexProfileAttempt }>;
    let profilesMatch = true;
    for (const role of CODEX_AGENT_PROFILE_ROLES) {
      const expectedPath = this.#activeAgentProfiles.configuredPaths[role];
      const configuredPath = nonblank(record(agents?.[role])?.config_file);
      const available = expectedPath !== undefined && configuredPath === expectedPath && this.#activeAgentProfiles.sourceAvailability[role] === "available";
      profiles[role] = { availability: available ? "available" : "unavailable", attempt: "not_attempted" };
      if (!available) profilesMatch = false;
    }
    const degradedReasons: CodexAppServerReadiness["degradedReasons"][number][] = [];
    if (userAgent === null || codexHome === null) degradedReasons.push("app_server_identity_unavailable");
    if (config === null) degradedReasons.push("app_server_config_unavailable");
    if (!profilesMatch) degradedReasons.push("profile_registry_unavailable");
    return { status: degradedReasons.length === 0 ? "ready" : "degraded", modelAvailability: "not_checked", profiles, degradedReasons };
  }

  #failedLifecycle(): AsyncIterable<CodexAppServerTurnLifecycleEvent> {
    return {
      async *[Symbol.asyncIterator](): AsyncIterator<CodexAppServerTurnLifecycleEvent> { throw lifecycleError(); },
    };
  }

  #isTerminal(key: string): boolean {
    const events = this.#lifecycleBuffers.get(key);
    const last = events?.at(-1);
    return last !== undefined && last.outcome !== "started" && last.outcome !== "progressed";
  }

  #onNotification(message: JsonRecord): void {
    const method = nonblank(message.method);
    const params = record(message.params);
    if (method === null || params === null) return;
    if (method === "turn/started" || method === "turn/completed") {
      const threadId = nonblank(params.threadId);
      const turn = record(params.turn);
      const turnId = nonblank(turn?.id);
      if (threadId === null || turnId === null) {
        if (threadId !== null) this.#failThread(threadId);
        return;
      }
      if (method === "turn/started") {
        if (turn?.status !== "inProgress") return this.#failBinding({ runtimeKind: "codex", threadId, turnId });
        this.#routeLifecycle({ binding: { runtimeKind: "codex", threadId, turnId }, outcome: "started" });
        return;
      }
      const outcome = turn?.status === "completed"
        ? "succeeded"
        : turn?.status === "failed"
          ? "failed"
          : turn?.status === "interrupted"
            ? "cancelled"
            : null;
      if (outcome === null) return this.#failBinding({ runtimeKind: "codex", threadId, turnId });
      const nativeBinding = { runtimeKind: "codex", threadId, turnId } as const;
      const pendingElicitation = this.#pendingElicitations.get(lifecycleKey(nativeBinding));
      if (pendingElicitation !== undefined && outcome !== "cancelled") { this.#failBinding(nativeBinding); return; }
      if (outcome !== "succeeded") {
        this.#workingDirectories.delete(lifecycleKey(nativeBinding));
        this.#terminalCandidates.delete(lifecycleKey(nativeBinding));
      }
      if (outcome === "cancelled") {
        const pending = this.#pendingElicitations.get(lifecycleKey(nativeBinding));
        if (pending !== undefined) {
          pending.interruptedTerminalSeen = true;
          this.#finishElicitation(pending);
          return;
        }
      }
      const failureKind = outcome === "failed" ? classifyCodexTurnFailure(turn) : null;
      this.#routeLifecycle(outcome === "succeeded"
        ? { binding: nativeBinding, outcome, response: this.#terminalResponse(nativeBinding) }
        : outcome === "failed" && failureKind !== null
          ? { binding: nativeBinding, outcome, failureKind }
          : { binding: nativeBinding, outcome });
      return;
    }
    if (method !== "item/completed") return;
    const threadId = nonblank(params.threadId);
    const turnId = nonblank(params.turnId);
    const item = record(params.item);
    if (item?.type !== "agentMessage") return;
    // Provider events are additive. Validate consumed fields, not the complete
    // key set: newer Codex versions add metadata (for example questions).
    const phase = item.phase ?? null;
    const delivery = item.delivery ?? null;
    const memoryCitation = item.memoryCitation ?? null;
    const questions = item.questions ?? null;
    if (threadId === null || turnId === null || !Number.isSafeInteger(params.completedAtMs) || (params.completedAtMs as number) < 0
      || nonblank(item.id) === null || typeof item.text !== "string"
      || !["commentary", "partial_answer", "final_answer", null].includes(phase as string | null)
      || !(delivery === null || delivery === "async")
      || !(memoryCitation === null || record(memoryCitation) !== null)
      || !(questions === null || (Array.isArray(questions) && questions.every((value: unknown) => {
        const question = record(value);
        return question !== null && typeof question.title === "string"
          && (question.options == null || (Array.isArray(question.options) && question.options.every((option: unknown) => typeof option === "string")));
      })))) {
      this.#projectionRejected("invalid_agent_message");
      return;
    }
    // A partial answer may still be revised or followed by tools; only the final answer is the response.
    if (phase === "partial_answer") return;
    const nativeBinding = { runtimeKind: "codex", threadId, turnId } as const;
    if (item.phase === "commentary") {
      const commentary = normalizeExecutorCommentary(item.text);
      if (commentary === null) { this.#projectionRejected("invalid_commentary_text"); return; }
      this.#routeLifecycle({ binding: nativeBinding, outcome: "progressed", progress: { kind: "commentary", safeMessage: commentary } });
      return;
    }
    const body = normalizeExecutorTerminalResponse(item.text, this.#workingDirectories.get(lifecycleKey(nativeBinding)) ?? this.#pendingWorkingDirectories.get(threadId));
    if (body === null) { this.#projectionRejected("invalid_response_text"); return; }
    const bounded = truncateAgentActivityText(body);
    const candidate: TerminalCandidate = {
      sourceEventId: stableSourceEventId(nativeBinding, item.id as string),
      completedAtMs: params.completedAtMs as number,
      ordinal: ++this.#notificationOrdinal,
      body: bounded.value,
      truncated: bounded.truncation !== null,
      truncations: bounded.truncation === null ? [] : [bounded.truncation],
    };
    const key = lifecycleKey(nativeBinding);
    const candidates = this.#terminalCandidates.get(key) ?? { unphasedAmbiguous: false };
    if (item.phase === "final_answer") {
      const current = candidates.finalAnswer;
      if (current === undefined || candidate.completedAtMs > current.completedAtMs || (candidate.completedAtMs === current.completedAtMs && candidate.ordinal > current.ordinal)) candidates.finalAnswer = candidate;
    } else if (candidates.unphasedAnswer === undefined && !candidates.unphasedAmbiguous) candidates.unphasedAnswer = candidate;
    else { delete candidates.unphasedAnswer; candidates.unphasedAmbiguous = true; }
    this.#terminalCandidates.set(key, candidates);
  }

  #projectionRejected(reason: "invalid_agent_message" | "invalid_commentary_text" | "invalid_response_text"): void {
    // Never include provider payloads, identifiers, or text in rejection logs.
    console.warn(`axis.codex.projection_rejected reason=${reason}`);
  }

  #onServerRequest(id: number | string, method: string, message: JsonRecord): void {
    const requestKey = `${typeof id}:${String(id)}`;
    if (this.#serverRequestIds.has(requestKey)) { this.#failed(); return; }
    this.#serverRequestIds.add(requestKey);
    if (method !== "item/tool/requestUserInput" || !exactKeys(message, ["id", "method", "params"])) { this.#failed(); return; }
    const params = record(message.params);
    const threadId = nonblank(params?.threadId), turnId = nonblank(params?.turnId);
    if (threadId === null || turnId === null) { this.#failed(); return; }
    const nativeBinding = { runtimeKind: "codex", threadId, turnId } as const;
    const key = lifecycleKey(nativeBinding);
    const elicitation = elicitationFromCodexRequest(params, nativeBinding);
    if (elicitation === null || this.#pendingElicitations.has(key) || this.#isTerminal(key)) { this.#failBinding(nativeBinding); return; }
    const pending: PendingElicitation = { requestId: id, binding: nativeBinding, elicitation, interruptAcknowledged: false, interruptedTerminalSeen: false };
    this.#pendingElicitations.set(key, pending);
    void this.#requestRaw("turn/interrupt", { threadId, turnId }).then(() => {
      if (this.#pendingElicitations.get(key) !== pending) return;
      pending.interruptAcknowledged = true;
      this.#finishElicitation(pending);
    }).catch(() => this.#failBinding(nativeBinding));
  }

  #finishElicitation(pending: PendingElicitation): void {
    if (!pending.interruptAcknowledged || !pending.interruptedTerminalSeen) return;
    const key = lifecycleKey(pending.binding);
    if (this.#pendingElicitations.get(key) !== pending) return this.#failBinding(pending.binding);
    this.#pendingElicitations.delete(key);
    this.#workingDirectories.delete(key);
    this.#terminalCandidates.delete(key);
    this.#routeLifecycle({ binding: pending.binding, outcome: "waiting_for_input", elicitation: pending.elicitation });
  }

  #terminalResponse(nativeBinding: CodexLiveBinding): ExecutorTerminalResponse {
    const key = lifecycleKey(nativeBinding);
    const candidates = this.#terminalCandidates.get(key);
    this.#terminalCandidates.delete(key);
    this.#workingDirectories.delete(key);
    const selected = candidates?.finalAnswer ?? (candidates?.unphasedAmbiguous === false ? candidates.unphasedAnswer : undefined);
    if (selected === undefined) {
      return {
        sourceEventId: stableSourceEventId(nativeBinding, nativeBinding.turnId),
        projectionStatus: "fallback",
        body: agentActivityFallbackResponseBody,
        truncated: false,
        truncations: [],
      };
    }
    return {
      sourceEventId: selected.sourceEventId,
      projectionStatus: "projected",
      body: selected.body,
      truncated: selected.truncated,
      truncations: selected.truncations,
    };
  }

  #routeLifecycle(event: CodexAppServerTurnLifecycleEvent): void {
    const key = lifecycleKey(event.binding);
    const events = this.#lifecycleBuffers.get(key) ?? [];
    if (this.#isTerminal(key)) return;
    events.push(event);
    this.#lifecycleBuffers.set(key, events);
    const observer = this.#lifecycleObservers.get(key);
    if (observer === undefined) return;
    observer.consume(event);
    if (event.outcome !== "started" && event.outcome !== "progressed") this.#lifecycleObservers.delete(key);
  }

  #failBinding(nativeBinding: CodexLiveBinding): void {
    const key = lifecycleKey(nativeBinding);
    this.#ambiguousThreads.add(nativeBinding.threadId);
    this.#lifecycleObservers.get(key)?.fail();
    this.#workingDirectories.delete(key);
    this.#terminalCandidates.delete(key);
    this.#pendingElicitations.delete(key);
  }

  #failThread(threadId: string): void {
    this.#ambiguousThreads.add(threadId);
    this.#pendingWorkingDirectories.delete(threadId);
    for (const [key, observer] of this.#lifecycleObservers) {
      if (key.startsWith(`${threadId}\u0000`)) {
        observer.fail();
        this.#lifecycleObservers.delete(key);
      }
    }
    for (const key of this.#workingDirectories.keys()) {
      if (key.startsWith(`${threadId}\u0000`)) this.#workingDirectories.delete(key);
    }
    for (const key of this.#terminalCandidates.keys()) {
      if (key.startsWith(`${threadId}\u0000`)) this.#terminalCandidates.delete(key);
    }
    for (const key of this.#pendingElicitations.keys()) {
      if (key.startsWith(`${threadId}\u0000`)) this.#pendingElicitations.delete(key);
    }
  }
}

/**
 * `codex exec resume <thread_id> -` is intentionally not used here. Its JSONL
 * `turn.started` event lacks a turn ID, so it cannot return a truthful binding
 * for later `turn/interrupt`. continuations stay on the app-server session.
 */
export class CodexExecutor implements Executor<CodexLiveBinding> {
  readonly contractVersion = CODEX_EXECUTOR_CONTRACT_VERSION;
  readonly #transport: CodexAppServerTransport;
  readonly #resumesThreadOnContinue: boolean;
  readonly #cancellations = new Map<string, { readonly binding: CodexLiveBinding; readonly result: Promise<ExecutorCancelResult<CodexLiveBinding>> }>();

  constructor(options: CodexExecutorOptions) {
    this.#resumesThreadOnContinue = options.resumesThreadOnContinue ?? false;
    this.#transport = options.transport ?? new PackageLocalCodexAppServerTransport({
      command: options.command,
      ...(options.agentProfiles === undefined ? {} : { agentProfiles: options.agentProfiles }),
      ...(options.workerFallbackPolicy === undefined ? {} : { workerFallbackPolicy: options.workerFallbackPolicy }),
      ...(options.instructions === undefined ? {} : { instructions: options.instructions }),
    });
  }

  async readiness(): Promise<CodexAppServerReadiness> {
    if (this.#transport.readiness !== undefined) return this.#transport.readiness();
    return {
      status: "degraded",
      modelAvailability: "not_checked",
      profiles: Object.fromEntries(CODEX_AGENT_PROFILE_ROLES.map((role) => [role, { availability: "unavailable" as const, attempt: "not_attempted" as const }])) as CodexAppServerReadiness["profiles"],
      degradedReasons: ["app_server_unavailable"],
    };
  }

  /** Stop the owned app-server without attempting to reconnect or recover turns. */
  close(): void {
    this.#transport.close?.();
  }

  async start(request: ExecutorStartRequest): Promise<ExecutorStartResult<CodexLiveBinding>> {
    if (!startRequestIsValid(request)) return failed("start", request, "invalid_request", "executor.invalid_request");
    try {
      const nativeBinding = await this.#transport.startTurn({
        workingDirectory: request.workingDirectory,
        instruction: request.instruction,
      });
      if (!liveBindingIsValid(nativeBinding)) throw new Error("Invalid binding.");
      return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "start", status: "accepted", binding: nativeBinding };
    } catch {
      return failed("start", request, "failed", "executor.start_failed");
    }
  }

  async continue(request: ExecutorContinueRequest<CodexLiveBinding>): Promise<ExecutorContinueResult<CodexLiveBinding>> {
    const priorBinding = binding(request);
    if (!continueRequestIsValid(request) || priorBinding === null) return failed("continue", request, "invalid_request", "executor.invalid_request");
    try {
      const nativeBinding = await this.#transport.continueTurn({
        binding: priorBinding,
        workingDirectory: request.workingDirectory,
        instruction: request.instruction,
      });
      if (!liveBindingIsValid(nativeBinding) || nativeBinding.threadId !== priorBinding.threadId || nativeBinding.turnId === priorBinding.turnId) throw new Error("Invalid continuation binding.");
      return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "continue", status: "accepted", binding: nativeBinding };
    } catch {
      return failed("continue", request, "failed", "executor.continue_failed");
    }
  }

  async recover(request: ExecutorRecoveryRequest<CodexLiveBinding>): Promise<ExecutorRecoveryResult<CodexLiveBinding> | null> {
    const nativeBinding = binding(request);
    if (nativeBinding === null || request.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || !nonblank(request.requestId) || !nonblank(request.runId)) return null;
    try {
      const observation = this.#transport.recoverTurn === undefined ? null : await this.#transport.recoverTurn(nativeBinding);
      if (observation === null) return null;
      return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, runId: request.runId, status: "active", binding: nativeBinding, observe: this.#recoverObservation(nativeBinding, observation) };
    } catch { return null; }
  }

  /**
   * Restart proof for a prior Run that ended `succeeded`/`failed` or parked on
   * `waiting_for_input`, the one state only Codex produces. `attested` means the durable thread
   * is still listed as resumable by the app-server, so the `continue` that follows resumes it;
   * `null` means it is definitively not listed (archived, deleted, never existed), or that this
   * executor's `continue` could not resume it anyway (`resumesThreadOnContinue`). An invalid
   * request, a transport without the proof, or an unreachable or malformed oracle rejects, as
   * the registration contract requires. Read-only: no Run, credential, or turn is started.
   */
  async reattestTerminal(request: ExecutorTerminalReattestationRequest<CodexLiveBinding>): Promise<ExecutorTerminalReattestationResult<CodexLiveBinding> | null> {
    const nativeBinding = binding(request);
    if (record(request)?.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || requestId(request) === null || nativeBinding === null
      || (request.expectedOutcome !== "succeeded" && request.expectedOutcome !== "failed" && request.expectedOutcome !== "waiting_for_input")) throw new Error("Codex restart proof request is invalid.");
    if (!this.#resumesThreadOnContinue) return null;
    if (this.#transport.threadResumable === undefined) throw new Error("Codex restart proof is unavailable.");
    if (!await this.#transport.threadResumable(nativeBinding.threadId)) return null;
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, status: "attested", binding: request.binding };
  }

  cancel(request: ExecutorCancelRequest<CodexLiveBinding>): Promise<ExecutorCancelResult<CodexLiveBinding>> {
    const nativeBinding = binding(request);
    if (!cancelRequestIsValid(request) || nativeBinding === null) return Promise.resolve(failed("cancel", request, "invalid_request", "executor.invalid_request"));
    const existing = this.#cancellations.get(request.requestId);
    if (existing !== undefined) {
      return sameBinding(existing.binding, nativeBinding)
        ? existing.result
        : Promise.resolve(failed("cancel", request, "invalid_request", "executor.idempotency_conflict"));
    }
    const result = this.#interrupt(request.requestId, nativeBinding);
    this.#cancellations.set(request.requestId, { binding: nativeBinding, result });
    return result;
  }

  async *observe(nativeBinding: CodexLiveBinding): AsyncIterableIterator<ExecutorLifecycleEvent<CodexLiveBinding>> {
    if (!liveBindingIsValid(nativeBinding)) throw lifecycleError();
    let state: "awaiting_start" | "running" | "terminal" = "awaiting_start";
    let terminalSeen = false;
    let sequence = 0;
    for await (const event of this.#transport.observeTurn(nativeBinding)) {
      if (!liveBindingIsValid(event.binding) || !sameBinding(event.binding, nativeBinding)) throw lifecycleError();
      if (event.outcome === "started") {
        if (state !== "awaiting_start") throw lifecycleError();
        state = "running";
      } else if (event.outcome === "progressed") {
        if (state !== "running") throw lifecycleError();
      } else {
        if (state !== "running") throw lifecycleError();
        state = "terminal";
        terminalSeen = true;
      }
      const common = { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding: nativeBinding, sequence: ++sequence } as const;
      if (event.outcome === "progressed") yield { ...common, outcome: event.outcome, progress: event.progress };
      else if (event.outcome === "waiting_for_input") yield { ...common, outcome: event.outcome, elicitation: event.elicitation };
      else if (event.outcome === "succeeded") yield { ...common, outcome: event.outcome, ...(event.response === undefined ? {} : { response: event.response }) };
      else if (event.outcome === "failed") yield { ...common, outcome: event.outcome, ...(event.failureKind === undefined ? {} : { failureKind: event.failureKind }) };
      else yield { ...common, outcome: event.outcome };
      if (terminalSeen) return;
    }
    if (!terminalSeen) throw lifecycleError();
  }

  async *#recoverObservation(nativeBinding: CodexLiveBinding, observation: AsyncIterable<CodexAppServerTurnLifecycleEvent>): AsyncIterableIterator<ExecutorLifecycleEvent<CodexLiveBinding>> {
    let sequence = 0;
    for await (const event of observation) {
      if (!liveBindingIsValid(event.binding) || !sameBinding(event.binding, nativeBinding) || event.outcome === "started") throw lifecycleError();
      const common = { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding: nativeBinding, sequence: ++sequence } as const;
      if (event.outcome === "progressed") yield { ...common, outcome: event.outcome, progress: event.progress };
      else if (event.outcome === "waiting_for_input") { yield { ...common, outcome: event.outcome, elicitation: event.elicitation }; return; }
      else if (event.outcome === "succeeded") { yield { ...common, outcome: event.outcome, ...(event.response === undefined ? {} : { response: event.response }) }; return; }
      else if (event.outcome === "failed") { yield { ...common, outcome: event.outcome, ...(event.failureKind === undefined ? {} : { failureKind: event.failureKind }) }; return; }
      else { yield { ...common, outcome: event.outcome }; return; }
    }
    throw lifecycleError();
  }

  async #interrupt(requestIdValue: string, nativeBinding: CodexLiveBinding): Promise<ExecutorCancelResult<CodexLiveBinding>> {
    try {
      await this.#transport.interruptTurn(nativeBinding);
      return {
        contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
        requestId: requestIdValue,
        operation: "cancel",
        status: "interrupt_requested",
        binding: nativeBinding,
      };
    } catch {
      return failed("cancel", { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: requestIdValue }, "failed", "executor.interrupt_failed", nativeBinding);
    }
  }
}
