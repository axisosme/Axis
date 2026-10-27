import { axisTaskInput, resolveAgentInstructions, type AgentInstructionOverrides, type AgentInstructions } from "./response-guidance.js";
import { spawn, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, isAbsolute } from "node:path";

import type { LinearExecutionIdentity } from "@axisosme/axis-contracts/linear-execution";
import { agentActivityFallbackResponseBody, truncateAgentActivityText } from "@axisosme/axis-contracts/agent-activity";

import { LinearExecutionClient, linearExecutionEnvironment } from "../shared/linear-execution-client.js";

import {
  CODEX_EXECUTOR_CONTRACT_VERSION,
  ExecutorLifecycleError,
  executorError,
  normalizeExecutorTerminalResponse,
  type ClaudeCodeLiveBinding,
  type Executor,
  type ExecutorAcceptedResult,
  type ExecutorCancelRequest,
  type ExecutorCancelResult,
  type ExecutorContinueRequest,
  type ExecutorContinueResult,
  type ExecutorErrorCode,
  type ExecutorFailedResult,
  type ExecutorFailureKind,
  type ExecutorLifecycleEvent,
  type ExecutorRegistration,
  type ExecutorStartRequest,
  type ExecutorStartResult,
  type ExecutorSteerRequest,
  type ExecutorSteerResult,
  type ExecutorTerminalReattestationRequest,
  type ExecutorTerminalReattestationResult,
  type ExecutorTerminalResponse,
} from "../shared/worker-executor.js";

const PROBE_TIMEOUT_MS = 5_000;
const PROBE_OUTPUT_LIMIT_BYTES = 64 * 1024;
/**
 * Per-line ceiling, and the only real memory bound on this path: `#buffer` holds a line's bytes
 * until its newline. A single `tool_result` line can carry a whole file read or command output.
 */
const STREAM_LINE_LIMIT_BYTES = 1024 * 1024;
/**
 * Cumulative ceiling for one native session's stdout. Every payload is discarded at parse, so this
 * is a runaway stop, not a memory bound.
 */
const STREAM_OUTPUT_LIMIT_BYTES = 16 * 1024 * 1024;
/**
 * Hook lifecycle subtypes a SessionStart hook streams for this session, both ahead of `init` and
 * after this Run's terminal `result`. Both call sites in `#consume` share this set and its
 * session/`hook_event` guard.
 */
const SESSION_START_HOOK_SUBTYPES: ReadonlySet<unknown> = new Set(["hook_started", "hook_progress", "hook_response"]);
/**
 * How long a session that has written its final `result` may keep its process alive before this
 * adapter reaps it. Only process exit finalizes a turn, and a descendant that inherited stdout can
 * hold the pipe open after the CLI exits. Set above the documented post-result bounds: background
 * Bash ends about five seconds after the final result once stdin closes, and exit waits at most
 * 30 seconds for queued output to drain.
 */
const RESULT_EXIT_GRACE_MS = 60_000;
/** How long the reap waits after its SIGTERM before sending SIGKILL. */
const RESULT_KILL_GRACE_MS = 5_000;
/**
 * How long a silent but live session may run before this adapter emits a running heartbeat.
 */
const PROGRESS_HEARTBEAT_INTERVAL_MS = 5 * 60 * 1_000;
/** How long `cancel` waits for the native `control_response` that answers its interrupt. */
const CONTROL_RESPONSE_TIMEOUT_MS = 10_000;
const PERMISSION_DENIED_SAFE_MESSAGE = "Claude Code was denied at least one tool call, so the requested work may be incomplete. Check the work itself before relying on it.";
/**
 * The native capabilities this executor exercises at its spawn site. `--replay-user-messages` is
 * required because a steer is settled only by its echo.
 */
const REQUIRED_HELP_FLAGS = [
  "-p",
  "--session-id",
  "--resume",
  "--output-format",
  "--input-format",
  "--verbose",
  "--include-partial-messages",
  "--replay-user-messages",
  "--permission-mode",
] as const;

/**
 * The permission mode `#launch` passes. `--permission-mode` must list it among its own `choices`
 * (see `helpFlagChoices`), since other help rows also name the value.
 */
const REQUIRED_PERMISSION_MODE = "bypassPermissions";

/** The input format `#launch` passes; admitted the same way as `REQUIRED_PERMISSION_MODE`. */
const REQUIRED_INPUT_FORMAT = "stream-json";

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

/**
 * Reads one flag's `choices` from its own `--help` row and the wrapped continuation lines under
 * it, and nowhere else.
 */
function helpFlagChoices(help: string, flag: string): ReadonlySet<string> {
  const lines = help.split(/\r?\n/u);
  const start = lines.findIndex((line) => new RegExp(`^\\s*${escapeRegExp(flag)}(?![a-z0-9-])`, "u").test(line));
  if (start < 0) return new Set();
  let row = lines[start]!;
  for (const line of lines.slice(start + 1)) {
    if (line.trim().length === 0 || /^\s*-/u.test(line)) break;
    row += ` ${line.trim()}`;
  }
  const choices = /\(choices:([^)]*)\)/u.exec(row)?.[1] ?? "";
  return new Set([...choices.matchAll(/"([^"]+)"/gu)].map((match) => match[1]!));
}

export interface ClaudeCodeSpawnOptions {
  readonly cwd?: string;
  readonly env: Readonly<Record<string, string>>;
  readonly shell: false;
  readonly detached: false;
  readonly stdio: readonly ["ignore" | "pipe", "pipe", "ignore"];
}

export interface ClaudeCodeExecutorOptions {
  /** Exact absolute, canonical executable supplied by the deployment runtime. */
  readonly command: string;
  readonly spawnProcess?: (file: string, args: readonly string[], options: ClaudeCodeSpawnOptions) => ChildProcess;
  readonly randomUuid?: () => string;
  readonly environment?: Readonly<Record<string, string | undefined>>;
  readonly probeTimeoutMs?: number;
  readonly probeOutputLimitBytes?: number;
  /** Test-only override of how long a session that wrote its `result` may keep running before it is reaped. */
  readonly resultExitGraceMs?: number;
  /** Test-only override of how long the reap waits after its SIGTERM before sending SIGKILL. */
  readonly resultKillGraceMs?: number;
  /** Test-only override of the running-session heartbeat window. */
  readonly progressHeartbeatIntervalMs?: number;
  /** Test-only override of how long `cancel` waits for its interrupt's `control_response`. */
  readonly controlResponseTimeoutMs?: number;
  /**
   * Test-only override of the monotonic clock the heartbeat window is timed against. Defaults to
   * `performance.now`, since a wall-clock rewind would silently stop the heartbeat.
   */
  readonly now?: () => number;
  /**
   * Relay-issued Linear execution access, supplied only by a Worker configured for it. Absent means
   * this Worker was never configured, which differs from a Run that carries no Linear authority.
   */
  readonly linear?: ClaudeCodeLinearExecutionOptions;
  /** This agent's instruction configuration. Absent sections take the Axis defaults. */
  readonly instructions?: AgentInstructionOverrides;
  /**
   * Absolute path to a Worker-private JSON file recording the `workingDirectory` of each session that
   * became continuable. It lets a restarted Worker continue a session it never launched, keeping the
   * same-working-directory guard (native `--resume` looks a session up by id alone). Omitted:
   * nothing is recorded and a continuation after a restart fails closed.
   */
  readonly nativeSessionStorePath?: string;
}

export interface ClaudeCodeLinearExecutionOptions {
  readonly relay: string;
  readonly client: Pick<LinearExecutionClient, "issue" | "revoke">;
}

type JsonRecord = Readonly<Record<string, unknown>>;

function record(value: unknown): JsonRecord | null {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as JsonRecord : null;
}

function nonblank(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function exactKeys(value: JsonRecord, keys: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function canonicalUuid(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(value);
}

function helpFlagTokens(value: string): ReadonlySet<string> {
  return new Set(value.match(/(?<![a-z0-9-])--?[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?![a-z0-9-])/gu) ?? []);
}

function startRequestIsValid(value: unknown): value is ExecutorStartRequest {
  const candidate = record(value);
  return candidate?.contractVersion === CODEX_EXECUTOR_CONTRACT_VERSION
    && nonblank(candidate.requestId)
    && nonblank(candidate.workingDirectory)
    && isAbsolute(candidate.workingDirectory)
    && nonblank(candidate.instruction);
}

function bindingIsValid(value: unknown): value is ClaudeCodeLiveBinding {
  const candidate = record(value);
  return candidate !== null
    && exactKeys(candidate, ["runtimeKind", "sessionId"])
    && candidate.runtimeKind === "claude-code"
    && canonicalUuid(candidate.sessionId);
}

function sameBinding(left: ClaudeCodeLiveBinding, right: ClaudeCodeLiveBinding): boolean {
  return left.runtimeKind === right.runtimeKind && left.sessionId === right.sessionId;
}

/** Native sessions inherit the service account environment without Axis filtering. */
function inheritedEnvironment(source: Readonly<Record<string, string | undefined>>): Readonly<Record<string, string>> {
  return Object.freeze(Object.fromEntries(Object.entries(source).filter((entry): entry is [string, string] => entry[1] !== undefined)));
}

/** One revocation per issued credential, however many paths reach it. */
function revokeOnce(revoke: () => Promise<void>): () => Promise<void> {
  let done = false;
  return async () => { if (done) return; done = true; await revoke(); };
}

function cancelRequestIsValid(value: unknown): value is ExecutorCancelRequest<ClaudeCodeLiveBinding> {
  const candidate = record(value);
  return candidate?.contractVersion === CODEX_EXECUTOR_CONTRACT_VERSION && nonblank(candidate.requestId) && bindingIsValid(candidate.binding);
}

function steerRequestIsValid(value: unknown): value is ExecutorSteerRequest<ClaudeCodeLiveBinding> {
  const candidate = record(value);
  return candidate?.contractVersion === CODEX_EXECUTOR_CONTRACT_VERSION && nonblank(candidate.requestId) && bindingIsValid(candidate.binding) && nonblank(candidate.instruction);
}

/**
 * One stream-json user message tagged with this adapter's own `uuid`. With
 * `--replay-user-messages` the CLI echoes that `uuid` when it consumes the message, and its
 * `command_lifecycle` lines name it as `command_uuid`.
 */
function userMessageLine(uuid: string, content: string): string {
  return `${JSON.stringify({ type: "user", uuid, message: { role: "user", content } })}\n`;
}

function failed<TOperation extends "start" | "continue" | "cancel" | "steer" = "start">(request: unknown, code: ExecutorErrorCode, operation: TOperation = "start" as TOperation, binding: ClaudeCodeLiveBinding | null = null): ExecutorFailedResult<TOperation, ClaudeCodeLiveBinding> {
  return {
    contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
    requestId: nonblank(record(request)?.requestId) ? record(request)!.requestId as string : null,
    operation,
    status: code === "executor.invalid_request" ? "invalid_request" : "failed",
    binding,
    error: executorError(operation, code),
  };
}

class ClaudeCodeStartError extends Error {
  constructor(readonly code: ExecutorErrorCode) {
    super("Claude Code start failed.");
  }
}

/**
 * Every reason this adapter can refuse a lifecycle. Worker persists the exact label, so it must
 * never contain anything read from the native stream. Adding a call site means reusing a key or
 * adding one.
 */
const CLAUDE_CODE_AMBIGUOUS_REASONS = {
  /** stdin or stdout itself errored, or stdout was unavailable at spawn -- a channel failure, not anything read from it. */
  streamError: "claude-code.stream_error",
  /** The stdout stream grew past its byte/line bounds, or a complete line failed to parse as a typed JSON object. */
  streamMalformed: "claude-code.stream_malformed",
  /** Writing this turn's own instruction to stdin failed; whether the CLI read any of it is unknown. */
  instructionWriteFailed: "claude-code.instruction_write_failed",
  /**
   * A message ahead of `init` that this state machine does not admit: not a SessionStart hook event,
   * an own `command_lifecycle` report, or the pre-init refusal.
   */
  unexpectedPreInitMessage: "claude-code.unexpected_pre_init_message",
  /**
   * A message after this Run's terminal `result` that is neither an own `command_lifecycle` report
   * nor a same-session SessionStart hook event.
   */
  unexpectedPostResultMessage: "claude-code.unexpected_post_result_message",
  /** A second `system/init` this session did not open a further turn for, or a `result` line that does not match this session. */
  unexpectedMidTurnMessage: "claude-code.unexpected_mid_turn_message",
  /** A `control_response` naming an interrupt this session never issued or already settled. */
  unexpectedControlResponse: "claude-code.unexpected_control_response",
  /**
   * A partial line was still buffered when the process exited, outside the terminate-after-interrupt
   * reap.
   */
  trailingPartialLine: "claude-code.trailing_partial_line",
  /** The process exited in a way this adapter cannot read as either a definite refusal or a genuine result. */
  inconclusiveExit: "claude-code.inconclusive_exit",
  /** `observe()` for a binding this executor holds no session for, or whose shape it never admits. */
  noSuchSession: "claude-code.no_session_for_binding",
  /** The same lifecycle was observed twice: a caller bug, not evidence about the run. */
  alreadyObserved: "claude-code.lifecycle_already_observed",
  /** Only reachable if a future call site sets the ambiguous flag without a label. */
  unlabelled: "claude-code.unlabelled",
} as const;

/**
 * Any key whose label is spelled the same as another key's. `never` when every label is distinct,
 * which is the only state `ClaudeCodeAmbiguousReason` resolves to a usable type in.
 */
type DuplicateClaudeCodeAmbiguousReason = {
  [K in keyof typeof CLAUDE_CODE_AMBIGUOUS_REASONS]: (typeof CLAUDE_CODE_AMBIGUOUS_REASONS)[K] extends (typeof CLAUDE_CODE_AMBIGUOUS_REASONS)[Exclude<keyof typeof CLAUDE_CODE_AMBIGUOUS_REASONS, K>] ? K : never;
}[keyof typeof CLAUDE_CODE_AMBIGUOUS_REASONS];

/**
 * The label a refusal carries: one of the table's values. Collapses to `never` if two keys share a
 * label, which stops every `#ambiguous()` call site from compiling.
 */
type ClaudeCodeAmbiguousReason = [DuplicateClaudeCodeAmbiguousReason] extends [never]
  ? (typeof CLAUDE_CODE_AMBIGUOUS_REASONS)[keyof typeof CLAUDE_CODE_AMBIGUOUS_REASONS]
  : never;

function claudeCodeLifecycleUnavailable(reason: ClaudeCodeAmbiguousReason): ExecutorLifecycleError {
  return new ExecutorLifecycleError(reason, "The Claude Code lifecycle is unavailable.");
}

interface ProbeResult {
  readonly output: string;
  readonly status: number | null;
  readonly signal: NodeJS.Signals | null;
}

class ClaudeCodeLifecycle implements AsyncIterable<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>> {
  readonly #binding: ClaudeCodeLiveBinding;
  readonly #events: ExecutorLifecycleEvent<ClaudeCodeLiveBinding>[] = [];
  readonly #waiters: (() => void)[] = [];
  #sequence = 0;
  #terminal = false;
  #ambiguous = false;
  #ambiguousReason: ClaudeCodeAmbiguousReason | undefined;
  #observed = false;
  #terminalDelivered = false;
  readonly #onTerminalDelivered: () => void;

  constructor(binding: ClaudeCodeLiveBinding, onTerminalDelivered: () => void) { this.#binding = binding; this.#onTerminalDelivered = onTerminalDelivered; }

  get terminalDelivered(): boolean { return this.#terminalDelivered; }

  emit(outcome: "started" | "progressed" | "succeeded" | "failed" | "cancelled", failureKind?: ExecutorFailureKind, response?: ExecutorTerminalResponse): void {
    if (this.#terminal || this.#ambiguous) return;
    const common = { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding: this.#binding, sequence: ++this.#sequence } as const;
    this.#events.push(outcome === "progressed"
      ? { ...common, outcome, progress: { kind: "generic" } }
      : outcome === "failed" && failureKind !== undefined ? { ...common, outcome, failureKind }
      : outcome === "succeeded" && response !== undefined ? { ...common, outcome, response }
      : { ...common, outcome });
    if (outcome === "succeeded" || outcome === "failed" || outcome === "cancelled") this.#terminal = true;
    this.#wake();
  }

  /**
   * `reason` is a fixed `CLAUDE_CODE_AMBIGUOUS_REASONS` label, never derived from the native stream.
   * It rides on the thrown `ExecutorLifecycleError` and Worker persists it beside the isolated Run.
   * The first reason wins.
   */
  ambiguous(reason: ClaudeCodeAmbiguousReason): void {
    if (this.#terminal || this.#ambiguous) return;
    this.#ambiguous = true;
    this.#ambiguousReason = reason;
    this.#wake();
  }

  /**
   * A running-session heartbeat emitted as a `progressed` commentary event.
   */
  heartbeat(safeMessage: string): void {
    if (this.#terminal || this.#ambiguous) return;
    const common = { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding: this.#binding, sequence: ++this.#sequence } as const;
    this.#events.push({ ...common, outcome: "progressed", progress: { kind: "commentary", safeMessage } });
    this.#wake();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>> {
    // A caller bug, labelled separately from evidence-based refusals.
    if (this.#observed) throw claudeCodeLifecycleUnavailable(CLAUDE_CODE_AMBIGUOUS_REASONS.alreadyObserved);
    this.#observed = true;
    for (;;) {
      const event = this.#events.shift();
      if (event !== undefined) {
        const terminal = event.outcome === "succeeded" || event.outcome === "failed" || event.outcome === "cancelled";
        if (terminal) { this.#terminalDelivered = true; this.#onTerminalDelivered(); }
        yield event;
        if (terminal) return;
        continue;
      }
      if (this.#ambiguous) throw claudeCodeLifecycleUnavailable(this.#ambiguousReason ?? CLAUDE_CODE_AMBIGUOUS_REASONS.unlabelled);
      if (this.#terminal) return;
      await new Promise<void>((resolve) => this.#waiters.push(resolve));
    }
  }

  #wake(): void { for (const waiter of this.#waiters.splice(0)) waiter(); }
}

class ClaudeCodeSession {
  readonly binding: ClaudeCodeLiveBinding;
  readonly lifecycle: ClaudeCodeLifecycle;
  readonly accepted: Promise<ClaudeCodeLiveBinding>;
  /**
   * Resolves once this session can never run again, terminal or ambiguous. Every transition into
   * those states goes through `#settle`, so a per-Run Linear credential's revocation hangs from here.
   */
  readonly settled: Promise<void>;
  readonly workingDirectory: string;
  readonly #child: ChildProcess;
  readonly #requestId: string;
  readonly #resultExitGraceMs: number;
  readonly #resultKillGraceMs: number;
  readonly #progressHeartbeatIntervalMs: number;
  readonly #now: () => number;
  #resolveAccepted!: (binding: ClaudeCodeLiveBinding) => void;
  #rejectAccepted!: (error: ClaudeCodeStartError) => void;
  #resolveSettled!: () => void;
  #acceptanceSettled = false;
  #spawned = false;
  #state: "awaiting_init" | "running" | "awaiting_exit" | "terminal" | "ambiguous" = "awaiting_init";
  #buffer = "";
  #bytes = 0;
  #progressSent = false;
  #result: "success" | "error" | undefined;
  /**
   * The native `result` line's own `result` string (`SDKResultMessage.result`), the turn's final
   * answer. Overwritten on every `result` line, so the last terminal turn's answer reaches
   * `#buildTerminalResponse`. Never logged or journaled.
   */
  #resultText: unknown;
  #denied = false;
  #reaped = false;
  #stdinClosed = false;
  #exitInfo: { readonly code: number | null; readonly signal: NodeJS.Signals | null } | undefined;
  #exitGraceHandle: ReturnType<typeof setTimeout> | undefined;
  #reapKillHandle: ReturnType<typeof setTimeout> | undefined;
  /** Fixed once this session first becomes `running`; the origin the heartbeat's elapsed-minutes text is measured from. */
  #observedAt: number | undefined;
  #heartbeatHandle: ReturnType<typeof setTimeout> | undefined;
  /**
   * Interrupts this turn has written and not yet seen answered, keyed by the generated
   * `control_request.request_id`. Each settles once: from its `control_response`, its wait bound, a
   * failed write, or this session settling first (`#transition`).
   */
  readonly #pendingInterrupts = new Map<string, (acknowledged: boolean) => void>();
  /** Set once an interrupt was written for this turn; only then may an error `result` finalize as `cancelled`. */
  #cancelRequested = false;
  /** Set by an error `result` for this session that arrived before `init`; see `#consume`. */
  #refusedBeforeInitResult = false;
  /**
   * Every user-message `uuid` this session wrote. Only a `command_lifecycle` naming one of them, for
   * this session, is tolerated outside `running`.
   */
  readonly #writtenUuids = new Set<string>();
  /**
   * Steers written to this turn whose echo has not arrived. Added before the write, so a racing
   * `result` still sees it: a message on stdin cannot be withdrawn, and the CLI runs it in this turn
   * or a further turn of the same process.
   */
  readonly #pendingSteers = new Set<string>();
  /**
   * A `result` arrived while a steer was pending, so a further turn is expected; only then is a
   * second `system/init` accepted.
   */
  #awaitingSteerTurn = false;
  /**
   * A cancel met a steer the CLI still holds queued: the interrupted turn's `result` starts the reap
   * at once, so the queued turn never runs.
   */
  #terminateAfterInterrupt = false;
  /**
   * The reap `#terminateAfterInterrupt` started is under way. Until exit, stdout is drained rather
   * than parsed strictly, since the process keeps writing until the signal lands.
   */
  #terminating = false;

  /** `onContinuable` runs once, when this session first satisfies `canContinue`. */
  constructor(child: ChildProcess, sessionId: string, requestId: string, workingDirectory: string, resultExitGraceMs: number, resultKillGraceMs: number, progressHeartbeatIntervalMs: number, now: () => number, onContinuable: () => void) {
    this.#child = child;
    this.#requestId = requestId;
    this.workingDirectory = workingDirectory;
    this.#resultExitGraceMs = resultExitGraceMs;
    this.#resultKillGraceMs = resultKillGraceMs;
    this.#progressHeartbeatIntervalMs = progressHeartbeatIntervalMs;
    this.#now = now;
    this.binding = Object.freeze({ runtimeKind: "claude-code", sessionId });
    // `canContinue` turns true only here: the terminal is the lifecycle's last event and is delivered
    // once, after `#state` and `#result` are final.
    this.lifecycle = new ClaudeCodeLifecycle(this.binding, () => { if (this.canContinue) onContinuable(); });
    this.accepted = new Promise((resolve, reject) => { this.#resolveAccepted = resolve; this.#rejectAccepted = reject; });
    this.settled = new Promise((resolve) => { this.#resolveSettled = resolve; });
    child.once("spawn", () => { this.#spawned = true; });
    child.once("error", () => this.#onError());
    // `exit` as well as `close`: a descendant that inherited stdout can hold `close` back after the CLI
    // is gone. `exit` alone never finalizes an unreaped session, because stdout may still deliver its
    // result; it is recorded for the reap in `#armResultExitGrace`.
    child.once("exit", (code, signal) => this.#onExit(code, signal));
    child.once("close", (code, signal) => this.#finalize(code, signal, this.#reaped));
    child.stdin?.on("error", () => this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamError));
    const stdout = child.stdout;
    if (stdout === null) { this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamError); return; }
    stdout.on("error", () => this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamError));
    stdout.setEncoding("utf8");
    stdout.on("data", (chunk: string) => this.#onData(chunk));
  }

  /**
   * The only writer of `#state`, so every route into a settled state reaches `settled` and hands the
   * turn's credential back. Resolving twice is a no-op.
   */
  #transition(state: "running" | "awaiting_exit" | "terminal" | "ambiguous"): void {
    this.#state = state;
    if (state === "running") this.#armHeartbeat();
    // Stdin stays open through the turn so an interrupt or a steer can land, and closes once the turn
    // can no longer accept one: on the `result` that ends the Run (`awaiting_exit`) or either settled
    // state.
    if (state === "awaiting_exit" || state === "terminal" || state === "ambiguous") this.#closeStdin();
    if (state === "terminal" || state === "ambiguous") {
      this.#clearHeartbeat();
      // A pending interrupt can no longer get a meaningful answer; it fails rather than waiting out its
      // bound.
      for (const settle of [...this.#pendingInterrupts.values()]) settle(false);
      this.#resolveSettled();
    }
  }

  /** Idempotent, and safe on a stdin already closed, ended, or destroyed. */
  #closeStdin(): void {
    if (this.#stdinClosed) return;
    this.#stdinClosed = true;
    const stdin = this.#child.stdin;
    if (stdin === null || stdin.destroyed || stdin.writableEnded) return;
    try { stdin.end(); } catch {}
  }

  /** First entry into `running` only: fixes `#observedAt` and starts the first window. */
  #armHeartbeat(): void {
    this.#observedAt = this.#now();
    this.#scheduleHeartbeat();
  }

  /**
   * (Re)starts one heartbeat window from now without touching `#observedAt`. Called on the first
   * `running` transition and whenever real progress resets the window. Each firing re-arms itself.
   */
  #scheduleHeartbeat(): void {
    this.#clearHeartbeat();
    this.#heartbeatHandle = setTimeout(() => this.#fireHeartbeat(), this.#progressHeartbeatIntervalMs);
    this.#heartbeatHandle.unref?.();
  }

  #clearHeartbeat(): void {
    if (this.#heartbeatHandle !== undefined) clearTimeout(this.#heartbeatHandle);
    this.#heartbeatHandle = undefined;
  }

  /**
   * Liveness is the same-Run child having neither exited nor left `running`. If either fails the
   * heartbeat stops.
   */
  #fireHeartbeat(): void {
    if (this.#state !== "running" || this.#exitInfo !== undefined) return;
    const observedAt = this.#observedAt ?? this.#now();
    const elapsedBucket = Math.max(0, Math.floor((this.#now() - observedAt) / this.#progressHeartbeatIntervalMs)) * 5;
    this.lifecycle.heartbeat(`Claude Code is still running; about ${elapsedBucket} minutes elapsed.`);
    this.#scheduleHeartbeat();
  }

  close(): void { this.#closeStdin(); try { this.#child.kill("SIGTERM"); } catch {} }

  /**
   * Writes one `{"type":"control_request","request_id":...,"request":{"subtype":"interrupt"}}`
   * line on stdin. Resolves true only once the matching `control_response` reports `success`; false
   * when the session is not `running`, the write fails, the answer is not a success, `timeoutMs`
   * passes, or the session settles first. The turn's own `result` and exit still decide its terminal.
   */
  interrupt(requestId: string, timeoutMs: number): Promise<boolean> {
    const stdin = this.#child.stdin;
    if (this.#state !== "running" || this.#stdinClosed || stdin === null || stdin.destroyed || stdin.writableEnded) return Promise.resolve(false);
    return new Promise((resolve) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const settle = (acknowledged: boolean): void => {
        if (timer !== undefined) clearTimeout(timer);
        this.#pendingInterrupts.delete(requestId);
        resolve(acknowledged);
      };
      this.#pendingInterrupts.set(requestId, settle);
      const line = `${JSON.stringify({ type: "control_request", request_id: requestId, request: { subtype: "interrupt" } })}\n`;
      try { stdin.write(line, (error) => { if (error !== null && error !== undefined) settle(false); }); } catch { settle(false); return; }
      this.#cancelRequested = true;
      // A steer written and not yet echoed is still queued in the CLI, which would run it right after
      // the interrupted turn. `#controlResponse` sets the same flag from `still_queued`.
      if (this.#pendingSteers.size > 0) this.#terminateAfterInterrupt = true;
      timer = setTimeout(() => settle(false), timeoutMs);
      timer.unref?.();
    });
  }

  /**
   * Writes this turn's instruction as its first user message, tagged with a recorded `uuid`. A failed
   * write makes the session ambiguous.
   */
  writeInstruction(content: string): void {
    const stdin = this.#child.stdin;
    if (stdin === null) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.instructionWriteFailed);
    const uuid = randomUUID();
    this.#writtenUuids.add(uuid);
    try { stdin.write(userMessageLine(uuid, content), (error) => { if (error !== null && error !== undefined) this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.instructionWriteFailed); }); } catch { this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.instructionWriteFailed); }
  }

  /**
   * Hands one more user message to the running turn without interrupting it. The CLI consumes it at
   * the next tool boundary or, after the turn's final `result`, in a further turn of the same process;
   * `#consume` follows both. Refused (false) when not `running`, stdin is closed, or a cancel was
   * written. Resolves true once the write is flushed, not when the echo arrives. Never sets
   * `#cancelRequested`.
   */
  steer(content: string): Promise<boolean> {
    const stdin = this.#child.stdin;
    if (this.#state !== "running" || this.#cancelRequested || this.#stdinClosed || stdin === null || stdin.destroyed || stdin.writableEnded) return Promise.resolve(false);
    const uuid = randomUUID();
    this.#writtenUuids.add(uuid);
    this.#pendingSteers.add(uuid);
    return new Promise((resolve) => {
      const refused = (): void => { this.#pendingSteers.delete(uuid); resolve(false); };
      try { stdin.write(userMessageLine(uuid, content), (error) => { if (error !== null && error !== undefined) refused(); else resolve(true); }); } catch { refused(); }
    });
  }
  // A result alone is not completion: require process exit and consumption of its terminal.
  get canContinue(): boolean { return this.#state === "terminal" && this.#result !== undefined && this.lifecycle.terminalDelivered; }

  #onData(chunk: string): void {
    if (this.#state === "terminal" || this.#state === "ambiguous") return;
    if (this.#terminating) return this.#drainWhileTerminating(chunk);
    this.#bytes += Buffer.byteLength(chunk);
    if (this.#bytes > STREAM_OUTPUT_LIMIT_BYTES) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamMalformed);
    this.#buffer += chunk;
    if (Buffer.byteLength(this.#buffer) > STREAM_LINE_LIMIT_BYTES && !this.#buffer.includes("\n")) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamMalformed);
    for (;;) {
      const newline = this.#buffer.indexOf("\n");
      if (newline < 0) return;
      const line = this.#buffer.slice(0, newline);
      this.#buffer = this.#buffer.slice(newline + 1);
      if (Buffer.byteLength(line) > STREAM_LINE_LIMIT_BYTES || line.length === 0) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamMalformed);
      let message: JsonRecord | null = null;
      try { message = record(JSON.parse(line)); } catch { return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamMalformed); }
      if (message === null || !nonblank(message.type)) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamMalformed);
      this.#consume(message);
      // The line just consumed started the terminate-after-interrupt reap; drain the rest of the chunk.
      if (this.#terminating) { const rest = this.#buffer; this.#buffer = ""; return this.#drainWhileTerminating(rest); }
    }
  }

  /**
   * Stdout during the terminate-after-interrupt reap. The interrupted turn's `result` is recorded and
   * the process is being signalled, so later output is discarded rather than made ambiguous, except a
   * `control_response` a pending interrupt is waiting for. A trailing partial line is never waited
   * for.
   */
  #drainWhileTerminating(chunk: string): void {
    this.#buffer += chunk;
    for (let newline = this.#buffer.indexOf("\n"); newline >= 0; newline = this.#buffer.indexOf("\n")) {
      const line = this.#buffer.slice(0, newline);
      this.#buffer = this.#buffer.slice(newline + 1);
      let message: JsonRecord | null = null;
      try { message = record(JSON.parse(line)); } catch { continue; }
      const response = message?.type === "control_response" ? record(message.response) : null;
      const settle = typeof response?.request_id === "string" ? this.#pendingInterrupts.get(response.request_id) : undefined;
      if (settle !== undefined) settle(response!.subtype === "success");
    }
    if (Buffer.byteLength(this.#buffer) > STREAM_LINE_LIMIT_BYTES) this.#buffer = "";
  }

  #consume(message: JsonRecord): void {
    if (this.#state === "awaiting_init") {
      // `--resume` of a session whose transcript is gone answers with one error `result` for the same
      // session and no `init`, then exits 1. It is recorded and `#finalize` treats it as a definite
      // refusal. The refusal must be the last line.
      if (this.#refusedBeforeInitResult) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.unexpectedPreInitMessage);
      if (message.type === "result" && message.is_error === true && message.session_id === this.binding.sessionId) { this.#refusedBeforeInitResult = true; return; }
      // A SessionStart hook streams `hook_started` / `hook_progress` / `hook_response` before `init`.
      // Only that event, for this session, is skipped.
      if (message.type === "system" && SESSION_START_HOOK_SUBTYPES.has(message.subtype) && message.hook_event === "SessionStart" && message.session_id === this.binding.sessionId) return;
      // A uuid-tagged instruction is reported `queued` and `started` before `init`; skipped for this
      // session and its own uuids only.
      if (this.#ownCommandLifecycle(message)) return;
      if (message.type !== "system" || message.subtype !== "init" || message.session_id !== this.binding.sessionId) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.unexpectedPreInitMessage);
      this.#transition("running");
      this.lifecycle.emit("started");
      this.#acceptanceSettled = true;
      this.#resolveAccepted(this.binding);
      return;
    }
    // The answer to an interrupt this adapter wrote. It settles that interrupt and is not progress. A
    // `control_response` for any other `request_id`, or with nothing pending, is ambiguous.
    if (message.type === "control_response" && (this.#state === "running" || this.#state === "awaiting_exit")) return this.#controlResponse(message);
    if (this.#state === "awaiting_exit" && message.type === "prompt_suggestion") return;
    // The `completed` report for an own message lands right after the `result`.
    if (this.#state === "awaiting_exit" && this.#ownCommandLifecycle(message)) return;
    // A same-session SessionStart hook event can also arrive after the terminal `result`; same guard
    // as before `init`.
    if (this.#state === "awaiting_exit" && message.type === "system" && SESSION_START_HOOK_SUBTYPES.has(message.subtype) && message.hook_event === "SessionStart" && message.session_id === this.binding.sessionId) return;
    if (this.#state !== "running") return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.unexpectedPostResultMessage);
    // A second `init` is the further turn a pending steer opens, accepted only then and only for this
    // session. It does not re-run the first `running` transition.
    if (message.type === "system" && message.subtype === "init") {
      if (!this.#awaitingSteerTurn || message.session_id !== this.binding.sessionId) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.unexpectedMidTurnMessage);
      this.#awaitingSteerTurn = false;
    }
    // The echo `--replay-user-messages` writes when the CLI consumes a pending steer.
    if (message.type === "user" && message.isReplay === true && typeof message.uuid === "string") this.#pendingSteers.delete(message.uuid);
    // `SDKPermissionDeniedMessage`: a tool call denied without a prompt. Best-effort; the result's
    // `permission_denials` is authoritative, and either marks the turn denied.
    if (message.type === "system" && message.subtype === "permission_denied") this.#denied = true;
    if (message.type === "result") {
      if (message.session_id !== this.binding.sessionId || typeof message.is_error !== "boolean" || !nonblank(message.subtype)) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.unexpectedMidTurnMessage);
      // `SDKResultMessage.permission_denials`. A present non-array is malformed; an absent field is
      // tolerated, leaving the best-effort event as the only record.
      const denials = message.permission_denials;
      if (denials !== undefined && !Array.isArray(denials)) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.unexpectedMidTurnMessage);
      if (Array.isArray(denials) && denials.length > 0) this.#denied = true;
      this.#result = message.is_error === false && message.subtype === "success" ? "success" : "error";
      // `SDKResultMessage.result`, captured raw and validated by `#buildTerminalResponse`.
      this.#resultText = message.result;
      // Which turn this `result` ends decides what follows it.
      //   - A cancel met a queued steer: this is the interrupted turn, so the reap starts now and
      //     `#finalize` projects `cancelled`.
      //   - A steer is still pending: the same process will run it in a further turn, so this is not the
      //     Run's terminal. Stdin stays open and the next `init` is expected.
      //   - Otherwise stdin closes and the exit grace starts.
      if (this.#terminateAfterInterrupt || (this.#cancelRequested && this.#pendingSteers.size > 0)) {
        this.#terminating = true;
        this.#transition("awaiting_exit");
        if (this.#exitInfo !== undefined) return this.#finalize(this.#exitInfo.code, this.#exitInfo.signal, false);
        this.#reap();
        return;
      }
      if (this.#pendingSteers.size > 0) {
        this.#awaitingSteerTurn = true;
        this.#scheduleHeartbeat();
        return;
      }
      this.#transition("awaiting_exit");
      this.#armResultExitGrace();
      return;
    }
    if (!this.#progressSent) {
      this.#progressSent = true;
      this.lifecycle.emit("progressed");
    }
    // Real progress resets the heartbeat window: reached for every accepted, in-order message while
    // `running`.
    this.#scheduleHeartbeat();
  }

  #controlResponse(message: JsonRecord): void {
    const response = record(message.response);
    const settle = typeof response?.request_id === "string" ? this.#pendingInterrupts.get(response.request_id) : undefined;
    if (settle === undefined) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.unexpectedControlResponse);
    // `response.response.still_queued` lists the user messages the CLI still holds at the interrupt.
    // Any at all means a turn would follow the interrupted one.
    const stillQueued = record(response!.response)?.still_queued;
    if (Array.isArray(stillQueued) && stillQueued.length > 0) this.#terminateAfterInterrupt = true;
    settle(response!.subtype === "success");
  }

  /** A `command_lifecycle` report for a user message this session wrote. */
  #ownCommandLifecycle(message: JsonRecord): boolean {
    return message.type === "command_lifecycle" && message.session_id === this.binding.sessionId && typeof message.command_uuid === "string" && this.#writtenUuids.has(message.command_uuid);
  }

  #onError(): void {
    if (this.#state === "terminal" || this.#state === "ambiguous") return;
    if (!this.#spawned && this.#state === "awaiting_init") {
      this.#transition("terminal");
      if (!this.#acceptanceSettled) { this.#acceptanceSettled = true; this.#rejectAccepted(new ClaudeCodeStartError("executor.launch_failed")); }
      return;
    }
    this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.streamError);
  }

  #onExit(code: number | null, signal: NodeJS.Signals | null): void {
    if (this.#exitInfo !== undefined) return;
    this.#exitInfo = { code, signal };
    if (this.#reaped) this.#finalize(code, signal, true);
  }

  /**
   * The signals the reap itself sent. A reaped exit is forgiven only when it is a plain exit code or
   * a signal in this set.
   */
  readonly #reapSignals = new Set<NodeJS.Signals>();

  /**
   * Only process exit finalizes the turn. Past the grace, a process that already exited (its `close`
   * held back by a descendant) is finalized from its recorded exit. Otherwise it is reaped, and the
   * result decides, provided the exit is a plain code or a signal in `#reapSignals`. A trailing line
   * buffered at a reaped exit is not waited for.
   */
  #armResultExitGrace(): void {
    if (this.#exitGraceHandle !== undefined) return;
    this.#exitGraceHandle = setTimeout(() => {
      if (this.#state !== "awaiting_exit") return;
      if (this.#exitInfo !== undefined) { this.#finalize(this.#exitInfo.code, this.#exitInfo.signal, false); return; }
      this.#reap();
    }, this.#resultExitGraceMs);
    this.#exitGraceHandle.unref?.();
  }

  /**
   * SIGTERM, then SIGKILL after `resultKillGraceMs`; each signal is recorded in `#reapSignals` before
   * it is sent. Shared by the exit grace and the terminate-after-interrupt rule.
   */
  #reap(): void {
    this.#reaped = true;
    this.#reapSignals.add("SIGTERM");
    try { this.#child.kill("SIGTERM"); } catch {}
    this.#reapKillHandle = setTimeout(() => { if (this.#exitInfo === undefined) { this.#reapSignals.add("SIGKILL"); try { this.#child.kill("SIGKILL"); } catch {} } }, this.#resultKillGraceMs);
    this.#reapKillHandle.unref?.();
  }

  #clearExitGrace(): void {
    if (this.#exitGraceHandle !== undefined) clearTimeout(this.#exitGraceHandle);
    if (this.#reapKillHandle !== undefined) clearTimeout(this.#reapKillHandle);
  }

  #finalize(code: number | null, signal: NodeJS.Signals | null, reaped: boolean): void {
    if (this.#state === "terminal" || this.#state === "ambiguous") return;
    this.#clearExitGrace();
    // A partial line at exit is ambiguous, except during the terminate-after-interrupt reap.
    if (this.#buffer.length > 0 && !this.#terminating) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.trailingPartialLine);
    if (this.#state === "awaiting_init") {
      // An empty buffer means stdout carried at most the pre-init error result. The CLI reports a refused
      // launch (an invalid flag, or `bypassPermissions` as root or under `sudo`) on stderr and exits
      // non-zero, so an exit code with no output, or with only that result, is a definite refusal. A
      // signal or exit 0 stays ambiguous. A refusal that loses the race with the instruction write
      // surfaces as a stdin error and stays ambiguous.
      if (code !== null && code !== 0 && signal === null) return this.#refusedBeforeInit();
      return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.inconclusiveExit);
    }
    if (this.#state === "running") {
      if (code === 0 && signal === null) return this.#ambiguous(CLAUDE_CODE_AMBIGUOUS_REASONS.inconclusiveExit);
      return this.#terminalFailure();
    }
    // One guard per line. An error result after this adapter's own interrupt is the cancellation it
    // asked for, unless the exit was a signal someone else sent. A denial, an error result, an exit this
    // adapter did not cause, or a foreign signal during a reap each fail the turn. A turn that
    // succeeded before the interrupt landed stays `succeeded`.
    if (this.#cancelRequested && this.#result === "error" && (signal === null || (reaped && this.#reapSignals.has(signal)))) return this.#cancelled();
    if (this.#denied) return this.#terminalFailure();
    if (this.#result === "error") return this.#terminalFailure();
    if (!reaped && (code !== 0 || signal !== null)) return this.#terminalFailure();
    if (reaped && signal !== null && !this.#reapSignals.has(signal)) return this.#terminalFailure();
    this.#transition("terminal");
    this.lifecycle.emit("succeeded", undefined, this.#buildTerminalResponse());
  }

  /**
   * The provider-facing answer for this Run's `succeeded` outcome, built from `#resultText`. Absent,
   * non-string, blank or refused text produces a fixed fallback, so a success always carries a response.
   */
  #buildTerminalResponse(): ExecutorTerminalResponse {
    const sourceEventId = this.#terminalSourceEventId();
    const body = normalizeExecutorTerminalResponse(this.#resultText, this.workingDirectory);
    if (body === null) {
      return { sourceEventId, projectionStatus: "fallback", body: agentActivityFallbackResponseBody, truncated: false, truncations: [] };
    }
    const bounded = truncateAgentActivityText(body);
    return {
      sourceEventId,
      projectionStatus: "projected",
      body: bounded.value,
      truncated: bounded.truncation !== null,
      truncations: bounded.truncation === null ? [] : [bounded.truncation],
    };
  }

  /**
   * Stable for this Run's `succeeded` event and unique across Runs. The native `sessionId` is shared
   * by every continuation of a conversation, so it is paired with Axis's per-call `requestId` to stay
   * distinct under the Relay outbox's `UNIQUE(conversation_id, source_event_id, content_digest)`.
   */
  #terminalSourceEventId(): string {
    return `claude-code-terminal-${createHash("sha256").update(this.binding.sessionId).update("\0").update(this.#requestId).digest("hex").slice(0, 48)}`;
  }

  /** A definite refusal before any turn began; see the note in `#finalize`. */
  #refusedBeforeInit(): void {
    this.#transition("terminal");
    if (!this.#acceptanceSettled) { this.#acceptanceSettled = true; this.#rejectAccepted(new ClaudeCodeStartError("executor.launch_failed")); }
  }

  #cancelled(): void {
    this.#transition("terminal");
    this.lifecycle.emit("cancelled");
  }

  #terminalFailure(): void {
    this.#transition("terminal");
    if (this.#denied) return this.lifecycle.emit("failed", "permission_denied");
    this.lifecycle.emit("failed");
  }

  #ambiguous(reason: ClaudeCodeAmbiguousReason): void {
    if (this.#state === "terminal" || this.#state === "ambiguous") return;
    this.#transition("ambiguous");
    this.#clearExitGrace();
    this.lifecycle.ambiguous(reason);
    if (!this.#acceptanceSettled) { this.#acceptanceSettled = true; this.#rejectAccepted(new ClaudeCodeStartError("executor.launch_ambiguous")); }
    try { this.#child.kill("SIGTERM"); } catch {}
  }
}

export class ClaudeCodeExecutor implements Executor<ClaudeCodeLiveBinding> {
  readonly contractVersion = CODEX_EXECUTOR_CONTRACT_VERSION;
  readonly #command: string;
  readonly #spawnProcess: (file: string, args: readonly string[], options: ClaudeCodeSpawnOptions) => ChildProcess;
  readonly #randomUuid: () => string;
  readonly #environment: Readonly<Record<string, string>>;
  readonly #probeTimeoutMs: number;
  readonly #probeOutputLimitBytes: number;
  readonly #resultExitGraceMs: number;
  readonly #resultKillGraceMs: number;
  readonly #progressHeartbeatIntervalMs: number;
  readonly #controlResponseTimeoutMs: number;
  readonly #now: () => number;
  readonly #linear: ClaudeCodeLinearExecutionOptions | undefined;
  /** The child environment when no Linear access is configured at all; fixed for this executor's life. */
  readonly #disabledEnvironment: Readonly<Record<string, string>>;
  readonly #instructions: AgentInstructions;
  /** Issued credentials whose session has not settled yet, so `close` can still revoke them. */
  readonly #outstandingRevocations = new Set<() => Promise<void>>();
  readonly #requests = new Map<string, { readonly digest: string; readonly result: Promise<ExecutorStartResult<ClaudeCodeLiveBinding>> }>();
  readonly #sessions = new Map<string, ClaudeCodeSession>();
  readonly #continuations = new Map<string, { readonly digest: string; readonly result: Promise<ExecutorContinueResult<ClaudeCodeLiveBinding>> }>();
  readonly #continuing = new Set<string>();
  readonly #cancellations = new Map<string, { readonly binding: ClaudeCodeLiveBinding; readonly result: Promise<ExecutorCancelResult<ClaudeCodeLiveBinding>> }>();
  readonly #steers = new Map<string, { readonly binding: ClaudeCodeLiveBinding; readonly digest: string; readonly result: Promise<ExecutorSteerResult<ClaudeCodeLiveBinding>> }>();
  readonly #nativeSessionStorePath: string | undefined;
  /**
   * `sessionId -> workingDirectory` for every session known to have become continuable, seen by this
   * process or read from `nativeSessionStorePath`. Consulted only for sessions `#sessions` does not
   * hold.
   */
  readonly #nativeSessionDirectories = new Map<string, string>();
  /** Serializes this process's own store writes; see `#persistNativeSession`. */
  #nativeSessionStoreWrite: Promise<void> = Promise.resolve();
  #closed = false;
  #nativeSessionLaunches = 0;
  #preflight: Promise<void> | undefined;

  constructor(options: ClaudeCodeExecutorOptions) {
    if (!isAbsolute(options.command)) throw new Error("Claude Code executable must be absolute.");
    this.#command = options.command;
    this.#spawnProcess = options.spawnProcess ?? ((file, args, spawnOptions) => spawn(file, [...args], { ...spawnOptions, stdio: [...spawnOptions.stdio] }));
    this.#randomUuid = options.randomUuid ?? randomUUID;
    this.#environment = inheritedEnvironment(options.environment ?? process.env);
    this.#probeTimeoutMs = options.probeTimeoutMs ?? PROBE_TIMEOUT_MS;
    this.#probeOutputLimitBytes = options.probeOutputLimitBytes ?? PROBE_OUTPUT_LIMIT_BYTES;
    this.#resultExitGraceMs = options.resultExitGraceMs ?? RESULT_EXIT_GRACE_MS;
    this.#resultKillGraceMs = options.resultKillGraceMs ?? RESULT_KILL_GRACE_MS;
    this.#progressHeartbeatIntervalMs = options.progressHeartbeatIntervalMs ?? PROGRESS_HEARTBEAT_INTERVAL_MS;
    this.#controlResponseTimeoutMs = options.controlResponseTimeoutMs ?? CONTROL_RESPONSE_TIMEOUT_MS;
    this.#now = options.now ?? ((): number => performance.now());
    this.#linear = options.linear;
    this.#disabledEnvironment = Object.freeze(linearExecutionEnvironment(this.#environment, undefined, undefined, "disabled"));
    this.#instructions = resolveAgentInstructions(options.instructions);
    this.#nativeSessionStorePath = options.nativeSessionStorePath;
  }

  /** Closed install/capability/auth gate. Production runs it before opening the listener. */
  async preflight(): Promise<void> {
    if (this.#preflight === undefined) this.#preflight = this.#runPreflight();
    return this.#preflight;
  }

  start(request: ExecutorStartRequest): Promise<ExecutorStartResult<ClaudeCodeLiveBinding>> {
    if (!startRequestIsValid(request)) return Promise.resolve(failed(request, "executor.invalid_request"));
    request = Object.freeze({ ...request });
    const digest = createHash("sha256").update(request.workingDirectory).update("\0").update(request.instruction).digest("hex");
    if (this.#continuations.has(request.requestId)) return Promise.resolve(failed(request, "executor.idempotency_conflict"));
    const existing = this.#requests.get(request.requestId);
    if (existing !== undefined) return existing.digest === digest ? existing.result : Promise.resolve(failed(request, "executor.idempotency_conflict"));
    const result = this.#startOnce(request);
    this.#requests.set(request.requestId, { digest, result });
    return result;
  }

  continue(request: ExecutorContinueRequest<ClaudeCodeLiveBinding>): Promise<ExecutorContinueResult<ClaudeCodeLiveBinding>> {
    if (!startRequestIsValid(request) || !bindingIsValid(request.binding)) return Promise.resolve(failed(request, "executor.invalid_request", "continue"));
    request = Object.freeze({ ...request, binding: Object.freeze({ ...request.binding }) });
    const digest = createHash("sha256").update(request.binding.sessionId).update("\0").update(request.workingDirectory).update("\0").update(request.instruction).digest("hex");
    if (this.#requests.has(request.requestId)) return Promise.resolve(failed(request, "executor.idempotency_conflict", "continue"));
    const existing = this.#continuations.get(request.requestId);
    if (existing !== undefined) return existing.digest === digest ? existing.result : Promise.resolve(failed(request, "executor.idempotency_conflict", "continue"));
    const result = this.#continueOnce(request);
    this.#continuations.set(request.requestId, { digest, result });
    return result;
  }

  async #continueOnce(request: ExecutorContinueRequest<ClaudeCodeLiveBinding>): Promise<ExecutorContinueResult<ClaudeCodeLiveBinding>> {
    const sessionId = request.binding.sessionId;
    const prior = this.#sessions.get(sessionId);
    if (this.#closed || (prior !== undefined && (!prior.canContinue || prior.workingDirectory !== request.workingDirectory)) || this.#continuing.has(sessionId)) return failed(request, "executor.capability_unavailable", "continue");
    // Reserve synchronously before any probe or store read yields; different
    // request IDs must never launch competing turns against the same transcript.
    this.#continuing.add(sessionId);
    try {
      // A session this process does not hold continues only from its durable record, and only in the
      // recorded working directory. The relaunch proves the native transcript still exists.
      if (prior === undefined && await this.#durableWorkingDirectory(sessionId) !== request.workingDirectory) return failed(request, "executor.capability_unavailable", "continue");
      await this.preflight();
      await this.#runPreflight();
      if (this.#closed) return failed(request, "executor.capability_unavailable", "continue");
      return await this.#launch(request, sessionId, "continue");
    } catch (error) {
      return failed(request, error instanceof ClaudeCodeStartError ? error.code : "executor.runtime_incompatible", "continue");
    } finally {
      this.#continuing.delete(sessionId);
    }
  }

  /**
   * Restart proof for a terminal binding. `attested` means only that this Worker recorded the session
   * as continuable; the `continue` relaunch checks the transcript, and a missing one is a definite
   * `executor.launch_failed`. `null` becomes `axis.worker.continuation_unresumable`. Read-only.
   */
  async reattestTerminal(request: ExecutorTerminalReattestationRequest<ClaudeCodeLiveBinding>): Promise<ExecutorTerminalReattestationResult<ClaudeCodeLiveBinding> | null> {
    if (record(request)?.contractVersion !== CODEX_EXECUTOR_CONTRACT_VERSION || !nonblank(request.requestId) || !bindingIsValid(request.binding) || (request.expectedOutcome !== "succeeded" && request.expectedOutcome !== "failed")) throw new ClaudeCodeStartError("executor.invalid_request");
    if (await this.#durableWorkingDirectory(request.binding.sessionId) === undefined) return null;
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, status: "attested", binding: request.binding };
  }

  /**
   * The recorded working directory for a session, re-reading the store when this process holds no
   * record, since an overlapping instance may have written it.
   */
  async #durableWorkingDirectory(sessionId: string): Promise<string | undefined> {
    const known = this.#nativeSessionDirectories.get(sessionId);
    if (known !== undefined) return known;
    for (const [key, value] of await this.#readNativeSessionStoreFile()) {
      if (!this.#nativeSessionDirectories.has(key)) this.#nativeSessionDirectories.set(key, value);
    }
    return this.#nativeSessionDirectories.get(sessionId);
  }

  /**
   * Absent, unreadable or corrupt reads as empty. An entry is kept only when its key is a canonical
   * session UUID and its value is a non-empty `workingDirectory`.
   */
  async #readNativeSessionStoreFile(): Promise<ReadonlyMap<string, string>> {
    if (this.#nativeSessionStorePath === undefined) return new Map();
    try {
      const parsed = record(JSON.parse(await readFile(this.#nativeSessionStorePath, "utf8")));
      if (parsed === null) return new Map();
      const entries = new Map<string, string>();
      for (const [key, value] of Object.entries(parsed)) {
        const workingDirectory = record(value)?.workingDirectory;
        if (canonicalUuid(key) && nonblank(workingDirectory)) entries.set(key, workingDirectory);
      }
      return entries;
    } catch {
      return new Map();
    }
  }

  /**
   * Best-effort: merged with the on-disk content immediately before writing, written to a temporary
   * file and renamed into place with mode 0600, and silent on failure. This process's own writes are
   * chained so they cannot rename out of order.
   */
  #persistNativeSession(sessionId: string, workingDirectory: string): void {
    this.#nativeSessionDirectories.set(sessionId, workingDirectory);
    const path = this.#nativeSessionStorePath;
    if (path === undefined) return;
    this.#nativeSessionStoreWrite = this.#nativeSessionStoreWrite.then(async () => {
      try {
        for (const [key, value] of await this.#readNativeSessionStoreFile()) {
          if (!this.#nativeSessionDirectories.has(key)) this.#nativeSessionDirectories.set(key, value);
        }
        const snapshot = Object.fromEntries([...this.#nativeSessionDirectories].map(([key, value]) => [key, { workingDirectory: value }]));
        await mkdir(dirname(path), { recursive: true });
        const tmpPath = `${path}.${randomUUID()}.tmp`;
        await writeFile(tmpPath, JSON.stringify(snapshot), { encoding: "utf8", mode: 0o600 });
        await rename(tmpPath, path);
      } catch {
        // Best-effort only; see above.
      }
    });
  }

  /**
   * Interrupts the binding's current turn through the native `control_request` interrupt
   * (`ClaudeCodeSession#interrupt`). The same request ID with the same binding shares one result;
   * with another binding it is a conflict. `interrupt_requested` acknowledges only the native answer;
   * the `cancelled` terminal is observed separately.
   */
  cancel(request: ExecutorCancelRequest<ClaudeCodeLiveBinding>): Promise<ExecutorCancelResult<ClaudeCodeLiveBinding>> {
    if (!cancelRequestIsValid(request)) return Promise.resolve(failed(request, "executor.invalid_request", "cancel"));
    const nativeBinding: ClaudeCodeLiveBinding = Object.freeze({ runtimeKind: request.binding.runtimeKind, sessionId: request.binding.sessionId });
    const existing = this.#cancellations.get(request.requestId);
    if (existing !== undefined) return sameBinding(existing.binding, nativeBinding) ? existing.result : Promise.resolve(failed(request, "executor.idempotency_conflict", "cancel"));
    const result = this.#cancelOnce(request.requestId, nativeBinding);
    this.#cancellations.set(request.requestId, { binding: nativeBinding, result });
    return result;
  }

  async #cancelOnce(requestId: string, nativeBinding: ClaudeCodeLiveBinding): Promise<ExecutorCancelResult<ClaudeCodeLiveBinding>> {
    const session = this.#sessions.get(nativeBinding.sessionId);
    const acknowledged = !this.#closed && session !== undefined && sameBinding(session.binding, nativeBinding) && await session.interrupt(randomUUID(), this.#controlResponseTimeoutMs);
    return acknowledged
      ? { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId, operation: "cancel", status: "interrupt_requested", binding: nativeBinding }
      : failed({ requestId }, "executor.interrupt_failed", "cancel", nativeBinding);
  }

  /**
   * Hands one more instruction to the binding's running turn (`ClaudeCodeSession#steer`). The same
   * request ID with the same binding and instruction digest shares one result; anything else under
   * that ID is `executor.idempotency_conflict`. No live session or a closed executor is
   * `executor.capability_unavailable`; a turn that can no longer take a steer is
   * `executor.steer_too_late`. `steered` says only that the instruction reached the turn.
   */
  steer(request: ExecutorSteerRequest<ClaudeCodeLiveBinding>): Promise<ExecutorSteerResult<ClaudeCodeLiveBinding>> {
    if (!steerRequestIsValid(request)) return Promise.resolve(failed(request, "executor.invalid_request", "steer"));
    const nativeBinding: ClaudeCodeLiveBinding = Object.freeze({ runtimeKind: request.binding.runtimeKind, sessionId: request.binding.sessionId });
    const digest = createHash("sha256").update(request.instruction).digest("hex");
    const existing = this.#steers.get(request.requestId);
    if (existing !== undefined) return sameBinding(existing.binding, nativeBinding) && existing.digest === digest ? existing.result : Promise.resolve(failed(request, "executor.idempotency_conflict", "steer"));
    const result = this.#steerOnce(request.requestId, nativeBinding, request.instruction);
    this.#steers.set(request.requestId, { binding: nativeBinding, digest, result });
    return result;
  }

  async #steerOnce(requestId: string, nativeBinding: ClaudeCodeLiveBinding, instruction: string): Promise<ExecutorSteerResult<ClaudeCodeLiveBinding>> {
    const session = this.#sessions.get(nativeBinding.sessionId);
    if (this.#closed || session === undefined || !sameBinding(session.binding, nativeBinding)) return failed({ requestId }, "executor.capability_unavailable", "steer", nativeBinding);
    // Same content a continuation sends: the instruction alone, without the
    // guidance the session was already given at start.
    const delivered = await session.steer(axisTaskInput(instruction, "continue", undefined, this.#instructions));
    return delivered
      ? { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId, operation: "steer", status: "steered", binding: nativeBinding }
      : failed({ requestId }, "executor.steer_too_late", "steer", nativeBinding);
  }

  observe(nativeBinding: ClaudeCodeLiveBinding): AsyncIterable<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>> {
    if (!bindingIsValid(nativeBinding)) return this.#failedLifecycle(CLAUDE_CODE_AMBIGUOUS_REASONS.noSuchSession);
    const session = this.#sessions.get(nativeBinding.sessionId);
    return session !== undefined && sameBinding(session.binding, nativeBinding) ? session.lifecycle : this.#failedLifecycle(CLAUDE_CODE_AMBIGUOUS_REASONS.noSuchSession);
  }

  /**
   * Best-effort owned-process shutdown only; this never claims cancellation. Credentials are revoked
   * here too, since a shutdown can outlive the children it signalled.
   */
  close(): void {
    this.#closed = true;
    for (const session of this.#sessions.values()) session.close();
    for (const revoke of [...this.#outstandingRevocations]) { this.#outstandingRevocations.delete(revoke); void revoke(); }
  }

  /**
   * Relay remains the live authority: a revocation this call could not deliver
   * still leaves the capability bounded by its own expiry, so a failure here is
   * never escalated into the Run's outcome.
   */
  async #revoke(identity: LinearExecutionIdentity): Promise<void> {
    try { await this.#linear?.client.revoke(identity); } catch { /* Relay live authority and expiry remain enforced. */ }
  }

  /**
   * This Run's own Linear execution environment, and the three statements it
   * can make. No Linear option at all is `disabled` -- the Worker was never
   * configured. A configured Worker whose Run carries no Linear identity is
   * `unavailable`. A Linear-sourced Run issues its own credential and is
   * `available`.
   *
   * The child inherits the service account environment so native tools behave
   * exactly as they do in an operator-started Claude session. The two sentinel
   * values still make the Axis-provided Linear path fail loudly when it is not
   * available; they do not disable another tool or credential already
   * configured for the host account.
   *
   * An identity the caller never supplied is refused rather than guessed: on a
   * Linear-enabled Worker that field is Worker-owned context, and treating its
   * absence as "no Linear authority" would hand a Linear Run a silent
   * downgrade instead of a failure.
   */
  async #linearEnvironment(request: ExecutorStartRequest): Promise<{ readonly environment: Readonly<Record<string, string>>; readonly identity: LinearExecutionIdentity | null } | null> {
    const linear = this.#linear;
    if (linear === undefined) return { environment: this.#disabledEnvironment, identity: null };
    if (request.linearExecution === undefined) return null;
    const identity = request.linearExecution;
    let issued;
    try {
      if (identity !== null) issued = await linear.client.issue(identity);
      return { environment: Object.freeze(linearExecutionEnvironment(this.#environment, linear.relay, issued)), identity };
    } catch {
      if (issued !== undefined && identity !== null) await this.#revoke(identity);
      return null;
    }
  }

  /** Package-internal, content-free evidence sourced only at the native task spawn site. */
  nativeSessionLaunchCount(): number { return this.#nativeSessionLaunches; }

  async #startOnce(request: ExecutorStartRequest): Promise<ExecutorStartResult<ClaudeCodeLiveBinding>> {
    try {
      await this.preflight();
      // Listener startup may cache its own gate, but every distinct start must
      // immediately repeat the complete runtime/capability/authentication set.
      await this.#runPreflight();
    } catch (error) {
      return failed(request, error instanceof ClaudeCodeStartError ? error.code : "executor.runtime_incompatible");
    }
    const sessionId = this.#randomUuid();
    if (!canonicalUuid(sessionId) || this.#sessions.has(sessionId) || this.#closed) return failed(request, "executor.launch_failed");
    return this.#launch(request, sessionId, "start");
  }

  /**
   * Full native launch posture: no `--tools` or `--safe-mode`, `--permission-mode bypassPermissions`,
   * and the complete service account environment. This loads the account's CLAUDE.md, skills,
   * plugins, hooks, MCP servers, tools, credentials and network access.
   *
   * `--input-format stream-json` and `--output-format stream-json`: the instruction is one uuid-tagged
   * user message and stdin stays open through the turn for `cancel`'s interrupt and `steer`'s further
   * messages. `--replay-user-messages` echoes each message as the CLI consumes it.
   *
   * `bypassPermissions` is not containment: every tool call runs without a prompt, including Bash,
   * network access and writes outside the workspace. Axis adds no prompt, sandbox, environment
   * filter or network policy, and `HOME` is passed through, so every file the Worker's uid can read is
   * reachable. The injected Linear environment supplies the right identity but is not isolation.
   */
  async #launch<TOperation extends "start" | "continue">(request: ExecutorStartRequest, sessionId: string, operation: TOperation): Promise<ExecutorAcceptedResult<TOperation, ClaudeCodeLiveBinding> | ExecutorFailedResult<TOperation, ClaudeCodeLiveBinding>> {
    // Every turn issues its own Linear access before its process exists; a continuation never reuses
    // the previous turn's credential.
    const access = await this.#linearEnvironment(request);
    if (access === null) return failed(request, "executor.capability_unavailable", operation);
    const identity = access.identity;
    const revoke = identity === null ? null : revokeOnce(() => this.#revoke(identity));
    if (revoke !== null) this.#outstandingRevocations.add(revoke);
    const release = (): void => { if (revoke === null) return; this.#outstandingRevocations.delete(revoke); void revoke(); };
    // `close()` can land during the await above. This guard sits after the revocation is registered,
    // so refusing here still hands the credential back.
    if (this.#closed) { release(); return failed(request, "executor.capability_unavailable", operation); }
    let child: ChildProcess;
    try {
      child = this.#spawnProcess(this.#command, [
        "-p",
        operation === "start" ? "--session-id" : "--resume", sessionId,
        "--output-format", "stream-json",
        "--input-format", "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--replay-user-messages",
        "--permission-mode", "bypassPermissions",
      ], { cwd: request.workingDirectory, env: access.environment, shell: false, detached: false, stdio: ["pipe", "pipe", "ignore"] });
      if (operation === "start") this.#nativeSessionLaunches += 1;
    } catch {
      release();
      return failed(request, "executor.launch_failed", operation);
    }
    // Recorded when the turn first becomes continuable, not when it is accepted.
    const workingDirectory = request.workingDirectory;
    const session = new ClaudeCodeSession(child, sessionId, request.requestId, workingDirectory, this.#resultExitGraceMs, this.#resultKillGraceMs, this.#progressHeartbeatIntervalMs, this.#now, () => this.#persistNativeSession(sessionId, workingDirectory));
    this.#sessions.set(sessionId, session);
    void session.settled.then(release);
    session.writeInstruction(axisTaskInput(request.instruction, operation, access.environment, this.#instructions));
    try {
      const nativeBinding = await session.accepted;
      return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation, status: "accepted", binding: nativeBinding };
    } catch (error) {
      return failed(request, error instanceof ClaudeCodeStartError ? error.code : "executor.launch_ambiguous", operation);
    }
  }

  async #runPreflight(): Promise<void> {
    const help = await this.#probe(["--help"], "executor.capability_unavailable");
    const flags = helpFlagTokens(help.output);
    if (help.status !== 0 || help.signal !== null || REQUIRED_HELP_FLAGS.some((flag) => !flags.has(flag))) throw new ClaudeCodeStartError("executor.capability_unavailable");
    if (!helpFlagChoices(help.output, "--permission-mode").has(REQUIRED_PERMISSION_MODE)) throw new ClaudeCodeStartError("executor.capability_unavailable");
    if (!helpFlagChoices(help.output, "--input-format").has(REQUIRED_INPUT_FORMAT)) throw new ClaudeCodeStartError("executor.capability_unavailable");
    await this.#requireAuthentication();
  }

  async #requireAuthentication(): Promise<void> {
    // Authentication is proven only by the bounded exit status; native account fields are never
    // parsed.
    const auth = await this.#probe(["auth", "status", "--json"], "executor.authentication_required", false);
    if (auth.status !== 0 || auth.signal !== null) throw new ClaudeCodeStartError("executor.authentication_required");
  }

  #probe(args: readonly string[], failureCode: ExecutorErrorCode, captureOutput = true): Promise<ProbeResult> {
    return new Promise((resolve, reject) => {
      let child: ChildProcess;
      try { child = this.#spawnProcess(this.#command, args, { env: this.#environment, shell: false, detached: false, stdio: ["ignore", "pipe", "ignore"] }); }
      catch { reject(new ClaudeCodeStartError("executor.runtime_missing")); return; }
      let output = "";
      let outputBytes = 0;
      let settled = false;
      const finish = (result?: ProbeResult, error?: ClaudeCodeStartError) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (error !== undefined) reject(error); else resolve(result!);
      };
      const timer = setTimeout(() => { try { child.kill("SIGKILL"); } catch {} finish(undefined, new ClaudeCodeStartError(failureCode)); }, this.#probeTimeoutMs);
      timer.unref?.();
      if (child.stdout === null) { finish(undefined, new ClaudeCodeStartError(failureCode)); return; }
      // Persistent: keeps swallowing later stream errors while `finish` stays one-shot.
      child.stdout.on("error", () => {
        try { child.kill("SIGKILL"); } catch {}
        finish(undefined, new ClaudeCodeStartError(failureCode));
      });
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => {
        outputBytes += Buffer.byteLength(chunk);
        if (outputBytes > this.#probeOutputLimitBytes) { try { child.kill("SIGKILL"); } catch {} finish(undefined, new ClaudeCodeStartError(failureCode)); return; }
        if (captureOutput) output += chunk;
      });
      child.once("error", () => finish(undefined, new ClaudeCodeStartError("executor.runtime_missing")));
      child.once("close", (status, signal) => finish({ output, status, signal }));
    });
  }

  #failedLifecycle(reason: ClaudeCodeAmbiguousReason): AsyncIterable<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>> {
    return { async *[Symbol.asyncIterator](): AsyncIterator<ExecutorLifecycleEvent<ClaudeCodeLiveBinding>> { throw claudeCodeLifecycleUnavailable(reason); } };
  }
}

/** Closed package-internal registration used only by the Claude Worker composition. */
export function claudeCodeExecutorRegistration(input: { readonly executorId: string; readonly adapter: ClaudeCodeExecutor }): ExecutorRegistration<ClaudeCodeLiveBinding> {
  const methodRef = { version: "v1", method: "claude-code.execute" } as const;
  return {
    runtimeKind: "claude-code",
    executorId: input.executorId,
    methodRef,
    supportedOperations: ["start", "continue", "cancel", "steer", "observe"],
    adapter: input.adapter,
    validateBinding: bindingIsValid,
    bindingsEqual: sameBinding,
    methodRefEquals: (left, right) => left.version === right.version && left.method === right.method,
    reattestTerminal: input.adapter.reattestTerminal.bind(input.adapter),
    projectLifecycle: ({ event, runId }) => {
      if (event.outcome === "progressed") {
        return {
          kind: "progress",
          completed: 1,
          total: 1,
          safeMessage: event.progress.kind === "commentary" ? event.progress.safeMessage : "Claude Code session is progressing.",
        };
      }
      if (event.outcome === "succeeded") return { kind: "succeeded", resultRef: `axis-evidence://${runId}/claude-code-terminal`, ...(event.response === undefined ? {} : { response: event.response }) };
      if (event.outcome === "cancelled") return { kind: "failed", code: "execution_cancelled", safeMessage: "Execution was cancelled." };
      if (event.outcome === "failed" && event.failureKind === "permission_denied") return { kind: "failed", code: "claude_code_permission_denied", safeMessage: PERMISSION_DENIED_SAFE_MESSAGE };
      if (event.outcome === "failed") return { kind: "failed", code: "claude_code_terminal_failed", safeMessage: "Claude Code execution failed." };
      throw new Error("The Claude Code lifecycle is invalid.");
    },
  };
}
