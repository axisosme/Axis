import {
  NODE_DEFINITE_ERROR_CODES,
  NodeContractError,
  validateNodeKnownWorkerFailure,
} from "../shared/router-node.js";
import {
  NODE_COMMAND_CONTRACT_VERSION,
  completeNodeWorkerCommand,
  nodeCommandIntentDigest,
  validateNodeAuthenticatedContext,
  validateNodeCommandCandidate,
  validateNodeCommandResponse,
  nodeReceiptExpectation,
  validateNodeReceiptQuery,
  validateNodeReservationEvidence,
  type NodeAuthenticatedContext,
  type NodeCommandRequest,
  type NodeReceiptQuery,
  type NodeReceipt,
  type NodeReservationEvidence,
  type NodeWorkerInventoryPort,
} from "../shared/router-node-commands.js";
import type { SqliteNodeJournal } from "./journal.js";
import { NodeWorkerInventoryEmptyError, NodeWorkerSelectionConflictError, NodeWorkerSelectionError } from "./runtime-config.js";
import { NodeWorkerDispatchError, NodeWorkerTargetAbsentError } from "./worker-client.js";

export interface NodeCommandServiceOptions {
  readonly authenticatedNode: NodeAuthenticatedContext;
  readonly journal: SqliteNodeJournal;
  /** Synchronous inventory lookup. The journal invokes it only while claiming a new first route. */
  readonly resolveNewSessionReservation: (selection: NodeCommandRequest["target"]["selection"]) => NodeReservationEvidence;
  /**
   * The Worker inventory, which both dispatches and answers whether it can take a command for a
   * target. Its `admission` is what turns a follow-up whose Worker has left the inventory into a decided
   * `worker_unavailable` instead of an ambiguous receipt that never converges.
   */
  readonly worker: NodeWorkerInventoryPort;
  /** Backoff between convergence attempts for one `ambiguous` command the Worker has not recorded. */
  readonly convergeRetryInitialDelayMs?: number;
  readonly convergeRetryMaxDelayMs?: number;
  readonly now?: () => Date;
}

/** Package-private transport evidence; the HTTP boundary always returns admitted command receipts at 202. */
export interface NodeCommandExecutionDisposition {
  readonly replayed: boolean;
  readonly receipt: Exclude<NodeReceipt, { readonly status: "not_found" }>;
}

type PendingCommand = { readonly digest: string; readonly promise: Promise<Exclude<NodeReceipt, { readonly status: "not_found" }>> };

const journalCommands = new WeakMap<SqliteNodeJournal, Map<string, PendingCommand>>();

function commandsFor(journal: SqliteNodeJournal): Map<string, PendingCommand> {
  const existing = journalCommands.get(journal);
  if (existing !== undefined) return existing;
  const commands = new Map<string, PendingCommand>();
  journalCommands.set(journal, commands);
  return commands;
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

/** The Node's command service. */
export class NodeCommandService {
  readonly #authenticatedNode: NodeAuthenticatedContext;
  readonly #journal: SqliteNodeJournal;
  readonly #resolveNewSessionReservation: NodeCommandServiceOptions["resolveNewSessionReservation"];
  readonly #worker: NodeWorkerInventoryPort;
  readonly #convergeInitialDelayMs: number;
  readonly #convergeMaxDelayMs: number;
  /** One convergence attempt per command at a time, however many refused events ask for it. */
  readonly #converging = new Map<string, Promise<void>>();
  readonly #convergeBackoff = new Map<string, { readonly attempts: number; readonly notBeforeMs: number }>();
  /**
   * Commands whose Worker is no longer in this Node's inventory. They cannot converge from here,
   * so they are not asked about again for the life of the process. They stay `ambiguous`, and remain
   * visible to operators in maintenance's `ambiguousCommands` count.
   */
  readonly #unconvergeable = new Set<string>();
  readonly #now: () => Date;
  readonly #commands: Map<string, PendingCommand>;

  constructor(options: NodeCommandServiceOptions) {
    this.#authenticatedNode = validateNodeAuthenticatedContext(options.authenticatedNode);
    this.#journal = options.journal;
    this.#resolveNewSessionReservation = options.resolveNewSessionReservation;
    this.#worker = options.worker;
    this.#convergeInitialDelayMs = options.convergeRetryInitialDelayMs ?? 250;
    this.#convergeMaxDelayMs = options.convergeRetryMaxDelayMs ?? 30_000;
    if (!Number.isSafeInteger(this.#convergeInitialDelayMs) || !Number.isSafeInteger(this.#convergeMaxDelayMs)
      || this.#convergeInitialDelayMs < 1 || this.#convergeInitialDelayMs > this.#convergeMaxDelayMs) throw new Error("Node command convergence backoff is invalid.");
    this.#now = options.now ?? (() => new Date());
    this.#commands = commandsFor(options.journal);
  }

  execute(value: unknown): Promise<Exclude<NodeReceipt, { readonly status: "not_found" }>> {
    return this.executeWithDisposition(value).then(({ receipt }) => receipt);
  }

  async executeWithDisposition(value: unknown): Promise<NodeCommandExecutionDisposition> {
    let snapshot: unknown;
    try { snapshot = structuredClone(value); }
    catch { throw new NodeContractError("axis.node.malformed"); }
    const request = deepFreeze(await validateNodeCommandCandidate(snapshot));
    this.#authenticateRequest(request);
    const commandId = request.command.commandId, digest = nodeCommandIntentDigest(request);
    const existing = this.#commands.get(commandId);
    if (existing !== undefined) {
      if (existing.digest !== digest) throw new NodeContractError("axis.node.intent_conflict");
      return { replayed: true, receipt: await existing.promise };
    }

    const execution = this.#execute(request);
    const pending: PendingCommand = { digest, promise: execution.then(({ receipt }) => receipt) };
    this.#commands.set(commandId, pending);
    // Released however it settles. The map only joins concurrent identical requests; once one
    // settles, the journal answers every later presentation. A refusal raised inside the
    // claim rolls it back and records nothing, so a retained rejection here would be the ONLY
    // memory of it -- and would keep answering `worker_unavailable` to the same command after
    // its Worker is back, until the process restarts.
    const release = () => { if (this.#commands.get(commandId) === pending) this.#commands.delete(commandId); };
    void pending.promise.then(release, release);
    return execution;
  }

  async readReceipt(value: unknown): Promise<NodeReceipt> {
    let snapshot: unknown;
    try { snapshot = structuredClone(value); }
    catch { throw new NodeContractError("axis.node.malformed"); }
    const query = validateNodeReceiptQuery(snapshot);
    if (!sameReference(query.nodeRef, this.#authenticatedNode.nodeRef)) throw new NodeContractError("axis.node.binding_conflict");
    return this.#journal.readReceipt({ query, authenticatedNode: this.#authenticatedNode });
  }

  /**
   * Converge an `ambiguous` command receipt from the Worker's recorded response to that command.
   *
   * Callers are triggers that cannot tell whether convergence is possible -- a refused Run event, the
   * open-time sweep -- so this never throws and never re-dispatches. It asks the Worker's read-only
   * command record, and the journal accepts the answer only after validating it against the command's
   * expectation. Every other outcome (not recorded, unreachable, invalid, already converged) leaves
   * the receipt as it is. Concurrent callers share one attempt; an attempt that does not converge
   * backs off exponentially before the next one is made.
   *
   * `signal` is the Node's shutdown and aborts the Worker read; the receipt stays `ambiguous`, for the next
   * open-time sweep. Refusing to start work once shutdown began is the caller's check (src/node/runtime.ts).
   */
  convergeAmbiguous(commandId: string, signal?: AbortSignal): Promise<void> {
    if (this.#unconvergeable.has(commandId)) return Promise.resolve();
    const inFlight = this.#converging.get(commandId);
    if (inFlight !== undefined) return inFlight;
    const backoff = this.#convergeBackoff.get(commandId);
    if (backoff !== undefined && this.#now().getTime() < backoff.notBeforeMs) return Promise.resolve();
    const work = this.#convergeAmbiguousOnce(commandId, signal).finally(() => { this.#converging.delete(commandId); });
    this.#converging.set(commandId, work);
    return work;
  }

  async #convergeAmbiguousOnce(commandId: string, signal?: AbortSignal): Promise<void> {
    try {
      const target = await this.#journal.readAmbiguousCommandTarget(commandId);
      if (target === null) { this.#convergeBackoff.delete(commandId); return; }
      const recorded = await this.#worker.readCommand(target, commandId, signal);
      if (recorded.status === "recorded") {
        const prepared = await this.#journal.prepareAmbiguousConvergence({ commandId, authenticatedNode: this.#authenticatedNode, workerResponse: recorded.response });
        // `null`: settled while the Worker was being asked. Either commit outcome is an end state: this
        // process converged it, or another process sharing the journal did first.
        if (prepared !== null) await this.#journal.commitAmbiguousConvergence({ prepared, recordedAt: this.#now().toISOString() });
        this.#convergeBackoff.delete(commandId);
        return;
      }
    } catch (error) {
      if (error instanceof NodeWorkerTargetAbsentError) {
        this.#unconvergeable.add(commandId);
        this.#convergeBackoff.delete(commandId);
        return;
      }
      /* Unknown stays unknown: the receipt remains `ambiguous` and the next trigger retries. */
    }
    const attempts = (this.#convergeBackoff.get(commandId)?.attempts ?? 0) + 1;
    const delayMs = Math.min(this.#convergeMaxDelayMs, this.#convergeInitialDelayMs * 2 ** Math.min(attempts - 1, 30));
    this.#convergeBackoff.set(commandId, { attempts, notBeforeMs: this.#now().getTime() + delayMs });
  }

  #authenticateRequest(request: NodeCommandRequest): void {
    if (!sameReference(request.nodeRef, this.#authenticatedNode.nodeRef)) throw new NodeContractError("axis.node.binding_conflict");
  }

  #authenticateReservation(reservationValue: NodeReservationEvidence): NodeReservationEvidence {
    const reservation = validateNodeReservationEvidence(reservationValue);
    if (reservation.target.identity.nodeId !== this.#authenticatedNode.nodeId) throw new NodeContractError("axis.node.binding_conflict");
    return reservation;
  }

  async #execute(request: NodeCommandRequest): Promise<NodeCommandExecutionDisposition> {
    const claimed = await this.#journal.claimCommandAndRoute({
      request,
      ...(request.command.command === "start" ? {
        resolveNewSessionReservation: () => {
          try { return this.#authenticateReservation(this.#resolveNewSessionReservation(request.target.selection)); }
          catch (error) {
            // An empty inventory is a deployment gap, not a bad target
            // ref. Collapsing both into `target_unknown` sent operators looking
            // for a typo in the target reference on a Node that simply has no
            // Worker installed yet.
            if (error instanceof NodeWorkerInventoryEmptyError) throw new NodeContractError("axis.node.worker_unavailable");
            // Ordered before the base class because the conflict error
            // extends it. Collapsing the two would tell an operator whose issue
            // carries both `claude` and `codex` that their Node target is
            // unknown, sending them to inspect a Node that is configured
            // perfectly well.
            if (error instanceof NodeWorkerSelectionConflictError) throw new NodeContractError("axis.node.selection_conflict");
            if (error instanceof NodeWorkerSelectionError) throw new NodeContractError("axis.node.target_unknown");
            throw error;
          }
        },
      } : {
        // A follow-up's reservation is the one retained from its session's route, and the
        // Worker it names may have left the inventory since. Refused here -- inside the claim,
        // before anything is written, and never for a replay -- the refusal is decided: nothing is
        // recorded and no Worker is contacted. Reaching `dispatch` instead would reject without a
        // classification and record an ambiguous receipt nothing can ever converge, which held the
        // channel's conversation slot for good. `#afterDispatch` still downgrades this code if a
        // journal call raises it after a dispatch, so the definite band's invariant is unchanged.
        admitRetainedReservation: (retained: NodeReservationEvidence) => {
          const admission = this.#worker.admission(retained.target);
          if (admission.kind !== "available") throw new NodeContractError("axis.node.worker_unavailable");
        },
      }),
      recordedAt: this.#now().toISOString(),
    });
    const reservation = this.#authenticateReservation(claimed.command.reservation);
    if (claimed.command.state === "accepted" || claimed.command.state === "rejected" || claimed.command.state === "ambiguous") {
      return { replayed: claimed.replayed, receipt: await this.#readback(request) };
    }
    if (claimed.command.state === "dispatching") {
      return { replayed: true, receipt: await this.#readback(request) };
    }

    const workerCommand = await completeNodeWorkerCommand(request, reservation, claimed.priorRoute);
    const dispatching = {
      contractVersion: NODE_COMMAND_CONTRACT_VERSION,
      nodeRef: request.nodeRef,
      commandId: request.command.commandId,
      status: "dispatching",
      reservation,
    } as const;
    try {
      await this.#journal.recordReceipt({
        request,
        authenticatedNode: this.#authenticatedNode,
        receipt: dispatching,
        recordedAt: this.#now().toISOString(),
      });
    } catch (error) {
      if (error instanceof NodeContractError
        && (error.code === "axis.node.binding_conflict" || error.code === "axis.node.intent_conflict")) {
        return { replayed: true, receipt: await this.#readback(request) };
      }
      throw error;
    }

    let terminal: Exclude<NodeReceipt, { readonly status: "not_found" }>;
    try {
      const result = await this.#worker.dispatch(reservation.target, workerCommand);
      // Cancel and steer act on the prior route's Run and mint none, so its binding is the evidence.
      const sessionBinding = workerCommand.command === "cancel" || workerCommand.command === "steer"
        ? claimed.priorRoute?.sessionBinding ?? fail("axis.node.recovery_required")
        : workerCommand.start.identity.sessionBinding;
      const { nativeSessionRef: _nativeSessionRef, ...safeSessionBinding } = sessionBinding;
      terminal = await validateNodeCommandResponse({
        contractVersion: NODE_COMMAND_CONTRACT_VERSION,
        nodeRef: request.nodeRef,
        commandId: request.command.commandId,
        status: "accepted",
        evidence: { ...reservation, sessionBinding: safeSessionBinding },
        result,
      }, await nodeReceiptExpectation(request, claimed.priorRoute), this.#authenticatedNode, claimed.priorRoute);
    } catch (error) {
      let definiteFailure: ReturnType<typeof validateNodeKnownWorkerFailure> | null = null;
      if (error instanceof NodeWorkerDispatchError && error.failure !== null) {
        try {
          const failure = validateNodeKnownWorkerFailure(error.failure);
          if (failure.certainty === "definite") definiteFailure = failure;
        } catch { /* Untrusted classifications remain ambiguous. */ }
      }
      terminal = definiteFailure === null
        ? { ...dispatching, status: "ambiguous" }
        : { ...dispatching, status: "rejected", failure: definiteFailure };
    }
    await this.#afterDispatch(this.#journal.recordReceipt({ request, authenticatedNode: this.#authenticatedNode, receipt: terminal, recordedAt: this.#now().toISOString() }));
    return { replayed: claimed.replayed, receipt: await this.#readback(request) };
  }

  /**
   * Keeps the definite band's invariant -- no definite code leaves this
   * service once a Worker dispatch may have been attempted -- enforced rather
   * than merely asserted in `NODE_DEFINITE_ERROR_CODES`' comment.
   *
   * A journal refusal after the dispatch (`intent_conflict`, `binding_conflict`,
   * `malformed` from receipt validation) says nothing about whether the command
   * ran, but those codes are on the definite list, and Router answers a definite
   * refusal of a start with a decision meaning "nothing ran". Emitting one here
   * would make that a lie, so it is reported as what is actually known:
   * `dispatch_ambiguous`.
   */
  async #afterDispatch<T>(operation: Promise<T>): Promise<T> {
    try { return await operation; }
    catch (error) {
      if (error instanceof NodeContractError && NODE_DEFINITE_ERROR_CODES.includes(error.code)) throw new NodeContractError("axis.node.dispatch_ambiguous");
      throw error;
    }
  }

  /**
   * Every caller is past `claimCommandAndRoute`, so the command may already
   * have been dispatched -- by this call or by the process that owns it -- which
   * is why the read is wrapped by `#afterDispatch`.
   */
  async #readback(request: NodeCommandRequest): Promise<Exclude<NodeReceipt, { readonly status: "not_found" }>> {
    const receipt = await this.#afterDispatch(this.#journal.readReceipt({
      query: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: request.nodeRef, commandId: request.command.commandId },
      authenticatedNode: this.#authenticatedNode,
    }));
    if (receipt.status === "not_found") throw new NodeContractError("axis.node.storage_unavailable");
    return receipt;
  }
}

function sameReference(left: NodeAuthenticatedContext["nodeRef"], right: NodeAuthenticatedContext["nodeRef"]): boolean {
  return left.ref === right.ref && left.version === right.version;
}

function fail(code: NodeContractError["code"]): never { throw new NodeContractError(code); }
