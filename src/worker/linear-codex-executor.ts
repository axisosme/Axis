import { CodexExecutor, PackageLocalCodexAppServerTransport, type CodexExecutorOptions, type PackageLocalCodexAppServerTransportOptions } from "../executor/codex.js";
import { LinearExecutionClient, linearExecutionEnvironment } from "../shared/linear-execution-client.js";
import {
  CODEX_EXECUTOR_CONTRACT_VERSION, executorError, type CodexLiveBinding, type ExecutorStartRequest,
  type ExecutorCancelRequest, type ExecutorContinueRequest, type ExecutorContinueResult, type ExecutorStartResult,
  type ExecutorLifecycleEvent, type ExecutorRecoveryRequest, type ExecutorAcceptedResult, type ExecutorFailedResult,
  type ExecutorTerminalReattestationRequest,
} from "../shared/worker-executor.js";
import type { LinearExecutionIdentity } from "@axisosme/axis-contracts/linear-execution";

export interface LinearCodexExecutorOptions extends CodexExecutorOptions {
  readonly environment: Readonly<Record<string, string | undefined>>;
  readonly linear: { readonly relay: string; readonly client: Pick<LinearExecutionClient, "issue" | "revoke"> };
  /** Tests may replace native execution while retaining the real Worker credential boundary. */
  readonly createExecutor?: (environment: NodeJS.ProcessEnv) => CodexExecutor;
  /**
   * Test-only injection point that keeps the real transport, so the thread
   * lifetime and resume handshake this executor configures stay observable.
   * Production always uses node's `spawn`.
   */
  readonly spawnProcess?: PackageLocalCodexAppServerTransportOptions["spawnProcess"];
}
function key(binding: CodexLiveBinding): string { return JSON.stringify([binding.threadId, binding.turnId]); }
/**
 * Only the fields the transport actually reads. The executor's own options --
 * the credential client and the Worker environment holding the receiver token --
 * must not travel in the transport's option bag, and an absent test seam must
 * not be forwarded as an explicit `undefined` under `exactOptionalPropertyTypes`.
 */
function transportOptions(options: LinearCodexExecutorOptions): PackageLocalCodexAppServerTransportOptions {
  const { command, agentProfiles, workerFallbackPolicy, spawnProcess, instructions } = options;
  return { command,
    ...(agentProfiles === undefined ? {} : { agentProfiles }),
    ...(workerFallbackPolicy === undefined ? {} : { workerFallbackPolicy }),
    ...(spawnProcess === undefined ? {} : { spawnProcess }),
    // Every per-Run transport this executor builds goes through here, so the
    // agent's configured instructions reach a resumed thread as well as a new one.
    ...(instructions === undefined ? {} : { instructions }) };
}
interface ActiveExecution {
  readonly executor: CodexExecutor;
  readonly identity: LinearExecutionIdentity | null;
  /** Set only for a successor Run, so a second successor cannot resume the same thread. */
  readonly resumedThreadId?: string;
}
type LaunchResult<TOperation extends "start" | "continue"> =
  ExecutorAcceptedResult<TOperation, CodexLiveBinding> | ExecutorFailedResult<TOperation, CodexLiveBinding>;

/**
 * One app-server process and one durable thread per Run. A Run's thread is
 * durable because a successor Run resumes it; that thread's rollout, and the
 * credential the app-server child's environment contributes to CODEX_HOME,
 * are an operator retention concern documented in LINEAR-EXECUTION-CLIENTS.
 * No credential is reused across Runs: each Run issues its own and the prior
 * Run's is revoked at its terminal event.
 */
export class LinearCodexExecutor extends CodexExecutor {
  readonly #options: LinearCodexExecutorOptions;
  readonly #active = new Map<string, ActiveExecution>();
  readonly #starting = new Set<ActiveExecution>();
  /** Threads with a successor Run already launching; see `#launch`. */
  readonly #continuing = new Set<string>();
  #closed = false;
  constructor(options: LinearCodexExecutorOptions) {
    super({ ...options, resumesThreadOnContinue: true, transport: new PackageLocalCodexAppServerTransport({ ...transportOptions(options),
      executionEnvironment: linearExecutionEnvironment(options.environment), ephemeral: true }) });
    // That base transport answers `readiness()` and the restart proof only; every Run gets its own.
    this.#options = options;
  }
  override async start(request: ExecutorStartRequest): Promise<ExecutorStartResult<CodexLiveBinding>> {
    return this.#launch("start", request, executor => executor.start(request));
  }
  /**
   * A successor Run resumes the prior Run's durable thread in a new process
   * that carries only this Run's freshly issued credential. The prior Run's
   * credential was already revoked at its terminal event, so continuation
   * never widens or reuses an earlier authorization.
   */
  override async continue(request: ExecutorContinueRequest<CodexLiveBinding>): Promise<ExecutorContinueResult<CodexLiveBinding>> {
    return this.#launch("continue", request, executor => executor.continue(request));
  }
  /**
   * The single credential lifecycle for both operations: issue before any
   * native call, revoke on every path that does not hand the executor to
   * `observe`, and never leave a launched process outside `#starting`/`#active`
   * where shutdown could not reach it. `start` and `continue` differ only in
   * the native call and whether the thread is resumed first.
   */
  async #launch<TOperation extends "start" | "continue">(
    operation: TOperation,
    request: ExecutorStartRequest,
    run: (executor: CodexExecutor) => Promise<LaunchResult<TOperation>>,
  ): Promise<LaunchResult<TOperation>> {
    const reject = (ambiguous = false): LaunchResult<TOperation> => ({ contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId,
      operation, status: "failed", binding: null, error: executorError(operation, ambiguous ? "executor.launch_ambiguous" : "executor.capability_unavailable") });
    if (this.#closed || request.linearExecution === undefined) return reject();
    // Two successor Runs resuming one durable thread would own one rollout from
    // two processes under two live credentials, which is both a double start
    // and an interleaving this implementation has never validated. Refuse the
    // second definitely rather than discover the consequence in a transcript.
    // A first successor while the prior Run is still live is not checked here:
    // the Worker already requires that Run to be terminal.
    const thread = operation === "continue" ? (request as ExecutorContinueRequest<CodexLiveBinding>).binding.threadId : undefined;
    if (thread !== undefined && (this.#continuing.has(thread) || this.#threadHasSuccessor(thread))) return reject();
    const identity = request.linearExecution;
    let executor: CodexExecutor | undefined;
    let pending: ActiveExecution | undefined;
    let nativeStarted = false;
    try {
      const issued = identity === null ? undefined : await this.#options.linear.client.issue(identity);
      if (this.#closed) throw new Error();
      const environment = linearExecutionEnvironment(this.#options.environment, this.#options.linear.relay, issued);
      executor = this.#options.createExecutor?.(environment) ?? new CodexExecutor({ ...this.#options,
        transport: new PackageLocalCodexAppServerTransport({ ...transportOptions(this.#options), executionEnvironment: environment,
          ephemeral: false, ...(operation === "continue" ? { resumeBeforeContinue: true } : {}) }) });
      pending = { executor, identity }; this.#starting.add(pending); nativeStarted = true;
      if (thread !== undefined) this.#continuing.add(thread);
      const result = await run(executor);
      if (result.status !== "accepted") {
        executor.close(); await this.#revoke(identity); return result;
      }
      if (this.#closed) { executor.close(); await this.#revoke(identity); return reject(true); }
      this.#active.set(key(result.binding), { executor, identity, ...(thread === undefined ? {} : { resumedThreadId: thread }) });
      return result;
    } catch { executor?.close(); await this.#revoke(identity); return reject(nativeStarted); }
    finally { if (pending !== undefined) this.#starting.delete(pending); if (thread !== undefined) this.#continuing.delete(thread); }
  }
  #threadHasSuccessor(threadId: string): boolean {
    for (const active of this.#active.values()) if (active.resumedThreadId === threadId) return true;
    return false;
  }
  override async cancel(request: ExecutorCancelRequest<CodexLiveBinding>) {
    const active = this.#active.get(key(request.binding));
    if (active !== undefined) return active.executor.cancel(request);
    return { contractVersion: CODEX_EXECUTOR_CONTRACT_VERSION, requestId: request.requestId, operation: "cancel" as const,
      status: "failed" as const, binding: request.binding, error: executorError("cancel", "executor.capability_unavailable") };
  }
  override async *observe(binding: CodexLiveBinding): AsyncIterableIterator<ExecutorLifecycleEvent<CodexLiveBinding>> {
    const active = this.#active.get(key(binding));
    if (active === undefined) throw new Error("Linear execution binding is unavailable.");
    try {
      for await (const event of active.executor.observe(binding)) {
        if (event.outcome !== "started" && event.outcome !== "progressed") {
          active.executor.close(); this.#active.delete(key(binding)); await this.#revoke(active.identity);
        }
        yield event;
      }
    } finally { this.#active.delete(key(binding)); active.executor.close(); await this.#revoke(active.identity); }
  }
  /**
   * Answered by the base transport, which carries no Linear credential and shares the
   * successor transports' thread store. No credential is issued and nothing enters
   * `#starting`/`#active`; `continue` then resumes the thread exactly as it does in-process.
   */
  override async reattestTerminal(request: ExecutorTerminalReattestationRequest<CodexLiveBinding>) {
    if (this.#closed) throw new Error("Linear Codex executor is closed.");
    return super.reattestTerminal(request);
  }
  override async recover(_request: ExecutorRecoveryRequest<CodexLiveBinding>) { return null; }
  override close(): void {
    this.#closed = true; super.close();
    for (const active of [...this.#active.values(), ...this.#starting]) { active.executor.close(); void this.#revoke(active.identity); }
    this.#active.clear(); this.#starting.clear();
  }
  async #revoke(identity: LinearExecutionIdentity | null): Promise<void> { if (identity === null) return; try { await this.#options.linear.client.revoke(identity); } catch { /* Relay live authority and expiry remain enforced. */ } }
}
