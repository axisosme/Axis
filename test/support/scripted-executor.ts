/**
 * Deterministic Executor adapter for the real Worker/Router boundary with only the
 * agent model scripted. Test code pushes every lifecycle event explicitly. observe()
 * yields queued events immediately and otherwise waits for a push to resolve its
 * pending promise, without polling or clock races.
 */
import {
  CODEX_EXECUTOR_CONTRACT_VERSION,
  executorError,
  type Executor,
  type ExecutorCancelRequest,
  type ExecutorCancelResult,
  type ExecutorContinueRequest,
  type ExecutorContinueResult,
  type ExecutorErrorCode,
  type ExecutorLifecycleEvent,
  type ExecutorLiveBinding,
  type ExecutorRegistration,
  type ExecutorStartRequest,
  type ExecutorStartResult,
  type ExecutorSteerRequest,
  type ExecutorSteerResult,
  type ExecutorTerminalReattestationRequest,
  type ExecutorTerminalReattestationResult,
  type ExecutorTerminalResponse,
} from "../../src/shared/worker-executor.js";
import type { StructuredElicitation } from "../../src/shared/elicitation.js";

/** The `progress` discriminant a `progressed` lifecycle event must carry, restated from `ExecutorLifecycleEvent`'s own union member. */
type ExecutorProgress = Extract<ExecutorLifecycleEvent, { readonly outcome: "progressed" }>["progress"];

/**
 * `threadId` identifies the native conversation; `turnId` identifies one
 * native call within it. A continuation reuses the prior `threadId` and
 * mints only a fresh `turnId` -- mirroring the real Codex adapter's own
 * invariant (`src/executor/codex.ts`: `CodexAppServerTransport#continueTurn`
 * reuses `input.binding.threadId`, and `CodexExecutor#continue` rejects
 * any response whose `threadId` drifted). A double that instead minted an
 * unrelated identity for every call could not catch a real regression in
 * that continuity, which is the whole reason this shape -- not a bare opaque
 * session id -- is preserved here.
 */
export interface ScriptedLiveBinding extends ExecutorLiveBinding {
  readonly runtimeKind: "scripted";
  readonly threadId: string;
  readonly turnId: string;
}

function bindingIsValid(value: unknown): value is ScriptedLiveBinding {
  const candidate = value as { readonly runtimeKind?: unknown; readonly threadId?: unknown; readonly turnId?: unknown } | null;
  return candidate !== null && typeof candidate === "object" && candidate.runtimeKind === "scripted"
    && typeof candidate.threadId === "string" && candidate.threadId.trim().length > 0
    && typeof candidate.turnId === "string" && candidate.turnId.trim().length > 0;
}
function bindingsEqual(left: ScriptedLiveBinding, right: ScriptedLiveBinding): boolean {
  return left.threadId === right.threadId && left.turnId === right.turnId;
}

interface Session {
  readonly events: ExecutorLifecycleEvent<ScriptedLiveBinding>[];
  readonly waiters: (() => void)[];
  sequence: number;
  closed: boolean;
  cancelled: boolean;
}

/**
 * A scriptable Codex-shaped executor. `start` mints a fresh `threadId`;
 * `continue` reuses the prior binding's `threadId` and mints only a fresh
 * `turnId` (see `ScriptedLiveBinding`'s own comment for why). Each call also
 * opens a fresh per-turn event queue with its own `sequence` counter reset to
 * 0 -- mirroring the real Codex adapter's own `observe()`, which resets its
 * sequence per call rather than carrying one across turns -- and immediately
 * pushes a "started" event; every other outcome is pushed only when a test
 * calls the corresponding `push*` method, so a scenario's timeline is
 * exactly the sequence the test writes, never inferred or scheduled.
 */
export class ScriptedExecutor implements Executor<ScriptedLiveBinding> {
  readonly contractVersion = CODEX_EXECUTOR_CONTRACT_VERSION;
  readonly #sessions = new Map<string, Session>();
  #nextThreadId = 0;
  #nextTurnId = 0;
  /**
   * Every binding minted by `start`/`continue`, oldest first.
   * `ExecutorStartRequest` carries no run identifier the test could
   * otherwise correlate against (real executors don't get one either --
   * Worker learns the binding from the accepted result), so a scripted test
   * reads `lastBinding` immediately after each `service.execute(...)` call
   * to get the binding its next `push*` call needs. Kept append-only rather
   * than a single mutable field, so a future multi-run harness that reads a
   * binding late never silently loses an earlier one to an overwrite.
   */
  readonly bindings: ScriptedLiveBinding[] = [];
  /**
   * Instruction text carried by each start/continue, indexed alongside bindings. This
   * lets tests inspect what the model receives, including waiting questions, options
   * and interpretation rules; a dispatch count alone cannot prove the instruction
   * arrived intact.
   *
   * Production instruction text is ephemeral and must stay out of errors, results and
   * transport diagnostics. This test adapter records it at the intended receiving
   * boundary.
   */
  readonly instructions: string[] = [];
  get lastBinding(): ScriptedLiveBinding | undefined { return this.bindings.at(-1); }

  #session(turnId: string): Session {
    const session = this.#sessions.get(turnId);
    if (session === undefined) throw new Error(`ScriptedExecutor: unknown turn ${turnId}`);
    return session;
  }

  #openTurn(threadId: string): { readonly binding: ScriptedLiveBinding; readonly session: Session } {
    const turnId = `scripted-turn-${++this.#nextTurnId}`;
    const session: Session = { events: [], waiters: [], sequence: 0, closed: false, cancelled: false };
    this.#sessions.set(turnId, session);
    const binding: ScriptedLiveBinding = { runtimeKind: "scripted", threadId, turnId };
    this.bindings.push(binding);
    return { binding, session };
  }

  async start(request: ExecutorStartRequest): Promise<ExecutorStartResult<ScriptedLiveBinding>> {
    this.instructions.push(request.instruction);
    const { binding, session } = this.#openTurn(`scripted-thread-${++this.#nextThreadId}`);
    this.#pushTo(session, binding, "started");
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "start", status: "accepted", binding };
  }

  /**
   * The next continuation refuses with this code. `echoBinding` returns the prior
   * binding on failure; `status` can select the contract's `invalid_request` refusal.
   */
  refuseNextContinue: { readonly code: ExecutorErrorCode; readonly echoBinding: boolean; readonly status?: "failed" | "invalid_request" } | undefined;

  async continue(request: ExecutorContinueRequest<ScriptedLiveBinding>): Promise<ExecutorContinueResult<ScriptedLiveBinding>> {
    const refusal = this.refuseNextContinue;
    if (refusal !== undefined) {
      this.refuseNextContinue = undefined;
      return {
        contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
        requestId: request.requestId,
        operation: "continue",
        status: refusal.status ?? "failed",
        binding: refusal.echoBinding ? request.binding : null,
        error: executorError("continue", refusal.code),
      };
    }
    this.instructions.push(request.instruction);
    const { binding, session } = this.#openTurn(request.binding.threadId);
    this.#pushTo(session, binding, "started");
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "continue", status: "accepted", binding };
  }

  /**
   * Only acknowledges the interrupt request; actual
   * cancellation completion is a separately observed terminal event, pushed
   * by the test via `pushCancelled`/`pushFailed`/`pushSucceeded` once the
   * scripted scenario decides the run has actually stopped.
   */
  async cancel(request: ExecutorCancelRequest<ScriptedLiveBinding>): Promise<ExecutorCancelResult<ScriptedLiveBinding>> {
    this.#session(request.binding.turnId).cancelled = true;
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "cancel", status: "interrupt_requested", binding: request.binding };
  }

  /**
   * The next `steer()` answer, consumed by that call. Only reachable when the
   * registration opts in with `steer: true`; the default registration declares no `steer`.
   *
   * - `"steered"`: the running turn took the instruction (the default).
   * - `"too_late"`: the turn was already finishing -- `executor.steer_too_late`.
   * - `"failed"`: any other executor failure.
   */
  nextSteer: "steered" | "too_late" | "failed" = "steered";
  /** Every `steer()` call, oldest first, with the instruction it carried. */
  readonly steers: { readonly requestId: string; readonly binding: ScriptedLiveBinding; readonly instruction: string }[] = [];

  async steer(request: ExecutorSteerRequest<ScriptedLiveBinding>): Promise<ExecutorSteerResult<ScriptedLiveBinding>> {
    this.steers.push({ requestId: request.requestId, binding: request.binding, instruction: request.instruction });
    const mode = this.nextSteer;
    this.nextSteer = "steered";
    if (mode === "steered") return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "steer", status: "steered", binding: request.binding };
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "steer", status: "failed", binding: request.binding, error: executorError("steer", mode === "too_late" ? "executor.steer_too_late" : "executor.capability_unavailable") };
  }

  /**
   * Persistent response mode for subsequent reattestTerminal calls. attested returns a
   * valid binding proof; null is a positive provider decline and yields
   * continuation_unresumable. unavailable returns mismatched proof, and throw rejects
   * the proof request; both yield capability_unavailable.
   */
  nextReattestTerminal: "attested" | "null" | "unavailable" | "throw" = "attested";

  async reattestTerminal(request: ExecutorTerminalReattestationRequest<ScriptedLiveBinding>): Promise<ExecutorTerminalReattestationResult<ScriptedLiveBinding> | null> {
    const mode = this.nextReattestTerminal;
    if (mode === "throw") throw new Error("ScriptedExecutor: reattestTerminal oracle unreachable.");
    if (mode === "null") return null;
    if (mode === "unavailable") {
      return {
        contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION,
        requestId: request.requestId,
        status: "attested",
        binding: { ...request.binding, turnId: `${request.binding.turnId}-mismatched` },
      };
    }
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, status: "attested", binding: request.binding };
  }

  async *observe(binding: ScriptedLiveBinding): AsyncIterableIterator<ExecutorLifecycleEvent<ScriptedLiveBinding>> {
    const session = this.#session(binding.turnId);
    for (;;) {
      const event = session.events.shift();
      if (event !== undefined) { yield event; continue; }
      if (session.closed) return;
      await new Promise<void>((resolve) => session.waiters.push(resolve));
    }
  }

  #pushTo(session: Session, binding: ScriptedLiveBinding, outcome: "started"): void;
  #pushTo(session: Session, binding: ScriptedLiveBinding, outcome: "progressed", progress: ExecutorProgress): void;
  #pushTo(session: Session, binding: ScriptedLiveBinding, outcome: "waiting_for_input", elicitation: StructuredElicitation): void;
  #pushTo(session: Session, binding: ScriptedLiveBinding, outcome: "succeeded", response?: ExecutorTerminalResponse): void;
  #pushTo(session: Session, binding: ScriptedLiveBinding, outcome: "failed"): void;
  #pushTo(session: Session, binding: ScriptedLiveBinding, outcome: "cancelled"): void;
  #pushTo(session: Session, binding: ScriptedLiveBinding, outcome: ExecutorLifecycleEvent["outcome"], extra?: StructuredElicitation | ExecutorTerminalResponse | ExecutorProgress): void {
    const common = { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, binding, sequence: ++session.sequence } as const;
    const event = outcome === "waiting_for_input"
      ? { ...common, outcome, elicitation: extra as StructuredElicitation }
      : outcome === "succeeded"
        ? { ...common, outcome, ...(extra === undefined ? {} : { response: extra as ExecutorTerminalResponse }) }
        // Every progressed lifecycle event requires its progress discriminant. The real
        // Worker rejects invalid lifecycle events, marks the Run observational and
        // stops supervision, losing later terminal events.
        : outcome === "progressed"
          ? { ...common, outcome, progress: (extra as ExecutorProgress | undefined) ?? { kind: "generic" as const } }
          : { ...common, outcome };
    session.events.push(event as ExecutorLifecycleEvent<ScriptedLiveBinding>);
    for (const waiter of session.waiters.splice(0)) waiter();
  }

  /**
   * Push progress without elicitation or terminal meaning. The default generic
   * discriminant is journaled and forwarded to Router immediately. Commentary takes the
   * separate commentary-journal path and is dropped if no such journal is configured.
   * Every event must carry a valid progress discriminant so Worker supervision can
   * continue.
   */
  pushProgress(binding: ScriptedLiveBinding, progress: ExecutorProgress = { kind: "generic" }): void {
    this.#pushTo(this.#session(binding.turnId), binding, "progressed", progress);
  }
  /** Pushes the waiting-for-input terminal this run's `observe()` will yield next. */
  pushWaiting(binding: ScriptedLiveBinding, elicitation: StructuredElicitation): void { this.#pushTo(this.#session(binding.turnId), binding, "waiting_for_input", elicitation); }
  /** Pushes the succeeded terminal this run's `observe()` will yield next. */
  pushSucceeded(binding: ScriptedLiveBinding, response?: ExecutorTerminalResponse): void { this.#pushTo(this.#session(binding.turnId), binding, "succeeded", response); }
  /** Pushes the failed terminal this run's `observe()` will yield next. */
  pushFailed(binding: ScriptedLiveBinding): void { this.#pushTo(this.#session(binding.turnId), binding, "failed"); }
  /** Pushes the cancelled terminal this run's `observe()` will yield next -- the real completion of a prior `cancel()` acknowledgement. */
  pushCancelled(binding: ScriptedLiveBinding): void { this.#pushTo(this.#session(binding.turnId), binding, "cancelled"); }
  /** Whether `cancel()` has been called for this binding's turn. */
  wasCancelRequested(binding: ScriptedLiveBinding): boolean { return this.#session(binding.turnId).cancelled; }
  /** Ends this binding's `observe()` iterator once its queued events drain -- call after the scenario's last `push*`. */
  close(binding: ScriptedLiveBinding): void {
    const session = this.#session(binding.turnId);
    session.closed = true;
    for (const waiter of session.waiters.splice(0)) waiter();
  }
  /** Ends every open session's `observe()` iterator -- for test teardown. */
  closeAll(): void {
    for (const session of this.#sessions.values()) { session.closed = true; for (const waiter of session.waiters.splice(0)) waiter(); }
  }
}

/**
 * Mirrors `codexExecutorRegistration` (src/executor/codex.ts): the same
 * `projectLifecycle` shape, including `waiting_for_input`.
 */
export function scriptedExecutorRegistration(input: { readonly executorId: string; readonly adapter: ScriptedExecutor; readonly steer?: boolean }): ExecutorRegistration<ScriptedLiveBinding> {
  const methodRef = { version: "v1", method: "scripted.execute" } as const;
  return {
    runtimeKind: "scripted",
    executorId: input.executorId,
    methodRef,
    supportedOperations: input.steer === true ? ["start", "continue", "cancel", "steer", "observe"] : ["start", "continue", "cancel", "observe"],
    adapter: input.adapter,
    validateBinding: bindingIsValid,
    bindingsEqual,
    reattestTerminal: (request) => input.adapter.reattestTerminal(request),
    methodRefEquals: (left, right) => left.version === right.version && left.method === right.method,
    projectLifecycle: ({ event, runId }) => {
      if (event.outcome === "progressed") return { kind: "progress", completed: 1, total: 1, safeMessage: "Scripted run is progressing." };
      if (event.outcome === "waiting_for_input") return { kind: "waiting_for_input", elicitation: event.elicitation };
      if (event.outcome === "succeeded") return { kind: "succeeded", resultRef: `axis-evidence://${runId}/scripted-terminal`, ...(event.response === undefined ? {} : { response: event.response }) };
      if (event.outcome === "failed") return { kind: "failed", code: "scripted_run_failed", safeMessage: "Scripted run failed." };
      if (event.outcome === "cancelled") return { kind: "failed", code: "execution_cancelled", safeMessage: "Execution was cancelled." };
      throw new Error("The scripted executor lifecycle is invalid.");
    },
  };
}
