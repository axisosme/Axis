import type { RouterRunLifecycleEnvelope, WorkerExecutionIdentity, WorkerVersionedReference } from "../shared/router-worker.js";
import { RouterEventDeliveryFailure, type RouterEventDeliveryPort } from "./router-event-delivery.js";

type DeliveryFailureCode = "axis.node.event_delivery_unavailable" | "axis.node.event_protocol_conflict";

export interface NodeEventPumpFailure {
  readonly code: "axis.node.storage_unavailable";
}

export interface NodeEventPumpRecord {
  readonly eventId: string;
  readonly runId: string;
  readonly globalOrder: number;
  readonly event: RouterRunLifecycleEnvelope;
  readonly recordedAt: string;
  readonly deliveryAttemptCount: number;
  readonly lastDeliveryFailure: { readonly code: DeliveryFailureCode; readonly failedAt: string } | null;
}

export interface NodeEventPumpJournal {
  listPendingPage(input: { readonly afterGlobalOrder: number; readonly limit: number }): Promise<readonly NodeEventPumpRecord[]>;
  markDelivered(eventId: string): Promise<void>;
  recordDeliveryFailure(input: { readonly eventId: string; readonly code: DeliveryFailureCode; readonly failedAt: string }): Promise<void>;
  quarantine(input: { readonly eventId: string; readonly code: "axis.node.binding_conflict"; readonly quarantinedAt: string }): Promise<void>;
}

export interface NodeEventPumpOptions {
  readonly nodeRef: WorkerVersionedReference;
  readonly journal: NodeEventPumpJournal;
  /** The Router event delivery. Its credential is owned by the injecting composition. */
  readonly delivery: RouterEventDeliveryPort;
  /**
   * The accepted-route classifier, repeated before every attempt, including retries after reopen.
   * `null` or a throw defers that one event on the retry backoff; it never stops the pump.
   */
  readonly resolveDeliveryRoute: (runId: string) => Promise<{
    readonly nodeRef: WorkerVersionedReference;
    readonly origin: WorkerExecutionIdentity;
  } | null>;
  readonly retryInitialDelayMs: number;
  readonly retryMaxDelayMs: number;
  readonly maxConcurrentRuns: number;
  readonly clock: () => string;
  readonly schedule: (callback: () => void, delayMs: number) => unknown;
  readonly cancel: (timer: unknown) => void;
  readonly queueMicrotask?: (callback: () => void) => void;
  readonly onBackgroundFailure?: (failure: NodeEventPumpFailure) => void;
}

function validPositive(value: number): boolean { return Number.isSafeInteger(value) && value > 0; }
function sameReference(left: WorkerVersionedReference, right: WorkerVersionedReference): boolean {
  return left.ref === right.ref && left.version === right.version;
}

/** Package-private durable outbox pump. Runtime composition owns when it is started. */
export class NodeEventPump {
  readonly #options: NodeEventPumpOptions;
  readonly #queueMicrotask: (callback: () => void) => void;
  readonly #activeRuns = new Map<string, { readonly controller: AbortController; readonly promise: Promise<void> }>();
  #generation = 0;
  #stopped = false;
  #timer: unknown;
  #timerDeadline = 0;
  #started: Promise<void> | null = null;
  #scanFlight: { readonly generation: number; readonly promise: Promise<void> } | null = null;
  readonly #backgroundWork = new Set<Promise<void>>();
  #backgroundFailure: NodeEventPumpFailure | null = null;
  #rescanGeneration: number | null = null;

  constructor(options: NodeEventPumpOptions) {
    if (!validPositive(options.retryInitialDelayMs) || !validPositive(options.retryMaxDelayMs)
      || options.retryInitialDelayMs > options.retryMaxDelayMs || !validPositive(options.maxConcurrentRuns)) throw new Error("Node event pump configuration is invalid.");
    this.#options = options;
    this.#queueMicrotask = options.queueMicrotask ?? queueMicrotask;
  }

  get backgroundFailure(): NodeEventPumpFailure | null { return this.#backgroundFailure; }

  async requestDelivery(): Promise<void> {
    if (this.#stopped || this.#backgroundFailure !== null) return;
    await this.#supervise(this.#requestScan(this.#generation));
  }

  start(): Promise<void> {
    if (this.#started !== null) return this.#started;
    this.#stopped = false;
    this.#generation += 1;
    this.#started = this.requestDelivery();
    return this.#started;
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    this.#generation += 1;
    this.#started = null;
    this.#rescanGeneration = null;
    if (this.#timer !== undefined) { this.#options.cancel(this.#timer); this.#timer = undefined; }
    this.#timerDeadline = 0;
    const active = [...this.#activeRuns.values()];
    for (const item of active) item.controller.abort();
    await Promise.all([...this.#backgroundWork, ...active.map((item) => item.promise)]);
  }

  #requestScan(generation: number): Promise<void> {
    if (this.#stopped || generation !== this.#generation || this.#backgroundFailure !== null) return Promise.resolve();
    if (this.#scanFlight?.generation === generation) {
      this.#rescanGeneration = generation;
      return this.#scanFlight.promise;
    }
    const promise = this.#scan(generation).finally(() => {
      if (this.#scanFlight?.promise === promise) this.#scanFlight = null;
      if (this.#rescanGeneration === generation) {
        this.#rescanGeneration = null;
        this.#startBackgroundScan(generation);
      }
    });
    this.#scanFlight = { generation, promise };
    return promise;
  }

  async #scan(generation: number): Promise<void> {
    if (this.#stopped || generation !== this.#generation || this.#backgroundFailure !== null) return;
    let afterGlobalOrder = 0;
    const pageSize = Math.max(2, Math.min(1_000, this.#options.maxConcurrentRuns * 2));
    for (;;) {
      const records = await this.#options.journal.listPendingPage({ afterGlobalOrder, limit: pageSize });
      if (this.#stopped || generation !== this.#generation || this.#backgroundFailure !== null || records.length === 0) return;
      for (const record of records) {
        if (this.#stopped || generation !== this.#generation || this.#backgroundFailure !== null) return;
        afterGlobalOrder = record.globalOrder;
        if (this.#activeRuns.has(record.runId)) continue;
        if (record.lastDeliveryFailure?.code === "axis.node.event_protocol_conflict") {
          await this.#options.journal.quarantine({ eventId: record.eventId, code: "axis.node.binding_conflict", quarantinedAt: record.lastDeliveryFailure.failedAt });
          continue;
        }
        if (record.lastDeliveryFailure?.code === "axis.node.event_delivery_unavailable" && !this.#ready(record)) {
          this.#scheduleRetry(record);
          continue;
        }
        if (this.#activeRuns.size >= this.#options.maxConcurrentRuns) continue;
        const controller = new AbortController();
        const promise = this.#deliver(record, generation, controller);
        this.#activeRuns.set(record.runId, { controller, promise });
      }
      if (records.length < pageSize || this.#activeRuns.size >= this.#options.maxConcurrentRuns) return;
    }
  }

  #ready(record: NodeEventPumpRecord): boolean {
    const failedAt = record.lastDeliveryFailure?.failedAt;
    if (failedAt === undefined) return false;
    const delay = Math.min(this.#options.retryMaxDelayMs, this.#options.retryInitialDelayMs * 2 ** Math.max(0, record.deliveryAttemptCount - 1));
    return Date.parse(failedAt) + delay <= Date.parse(this.#options.clock());
  }

  #scheduleRetry(record: NodeEventPumpRecord): void {
    if (this.#backgroundFailure !== null) return;
    const failedAt = record.lastDeliveryFailure?.failedAt;
    if (failedAt === undefined) return;
    const delay = Math.min(this.#options.retryMaxDelayMs, this.#options.retryInitialDelayMs * 2 ** Math.max(0, record.deliveryAttemptCount - 1));
    const remaining = Math.max(0, Date.parse(failedAt) + delay - Date.parse(this.#options.clock()));
    const deadline = Date.parse(failedAt) + delay;
    if (this.#timer !== undefined && this.#timerDeadline <= deadline) return;
    if (this.#timer !== undefined) this.#options.cancel(this.#timer);
    const generation = this.#generation;
    this.#timerDeadline = deadline;
    this.#timer = this.#options.schedule(() => {
      this.#timer = undefined; this.#timerDeadline = 0;
      this.#startBackgroundScan(generation);
    }, Math.max(0, deadline - Date.parse(this.#options.clock())));
  }

  #supervise(work: Promise<void>): Promise<void> {
    const tracked = work.catch(() => { this.#reportBackgroundFailure(); });
    this.#backgroundWork.add(tracked);
    void tracked.then(() => { this.#backgroundWork.delete(tracked); });
    return tracked;
  }

  #startBackgroundScan(generation: number): void {
    if (this.#stopped || generation !== this.#generation || this.#backgroundFailure !== null) return;
    void this.#supervise(this.#requestScan(generation));
  }

  #reportBackgroundFailure(): void {
    if (this.#backgroundFailure !== null) return;
    const failure: NodeEventPumpFailure = { code: "axis.node.storage_unavailable" };
    this.#backgroundFailure = failure;
    try { this.#options.onBackgroundFailure?.(failure); } catch {}
  }

  /** An event whose route is unreadable or not yet deliverable waits on the delivery backoff, alone. */
  async #deferUnroutable(record: NodeEventPumpRecord, generation: number): Promise<void> {
    if (this.#stopped || generation !== this.#generation || this.#backgroundFailure !== null) return;
    try { await this.#options.journal.recordDeliveryFailure({ eventId: record.eventId, code: "axis.node.event_delivery_unavailable", failedAt: this.#options.clock() }); }
    catch { this.#reportBackgroundFailure(); }
  }

  async #deliver(record: NodeEventPumpRecord, generation: number, controller: AbortController): Promise<void> {
    try {
      let route: Awaited<ReturnType<NodeEventPumpOptions["resolveDeliveryRoute"]>>;
      {
        // A route decision belongs to this event alone; only the journal refusing to record the
        // decision is a storage failure. An event whose route belongs to another Node is deferred, not
        // quarantined: Nodes may share one journal, and that Node's own pump delivers it. With the
        // production classifier that event never reaches the nodeRef guard -- its receipt read is
        // authenticated as this Node and throws `binding_conflict`, landing in the catch below. The
        // guard is defence in depth for a classifier that returns another Node's route.
        try { route = await this.#options.resolveDeliveryRoute(record.runId); }
        catch { await this.#deferUnroutable(record, generation); return; }
        if (this.#stopped || generation !== this.#generation) return;
        if (route === null) { await this.#deferUnroutable(record, generation); return; }
        if (!sameReference(route.nodeRef, this.#options.nodeRef)) { await this.#deferUnroutable(record, generation); return; }
      }
      try {
        await this.#options.delivery.deliver({
          event: record.event,
          recordedAt: record.recordedAt,
          signal: controller.signal,
          nodeRef: route.nodeRef,
          origin: route.origin,
        });
      } catch (error) {
        if (!this.#stopped && generation === this.#generation && this.#backgroundFailure === null) {
          const code: DeliveryFailureCode = error instanceof RouterEventDeliveryFailure && error.kind === "protocol_conflict"
            ? "axis.node.event_protocol_conflict" : "axis.node.event_delivery_unavailable";
          const failedAt = this.#options.clock();
          try {
            await this.#options.journal.recordDeliveryFailure({ eventId: record.eventId, code, failedAt });
            if (code === "axis.node.event_protocol_conflict") await this.#options.journal.quarantine({ eventId: record.eventId, code: "axis.node.binding_conflict", quarantinedAt: failedAt });
          } catch { this.#reportBackgroundFailure(); }
        }
        return;
      }
      if (!this.#stopped && generation === this.#generation) {
        try { await this.#options.journal.markDelivered(record.eventId); } catch { this.#reportBackgroundFailure(); }
      }
    } finally {
      this.#activeRuns.delete(record.runId);
      if (!this.#stopped && generation === this.#generation && this.#backgroundFailure === null) this.#queueMicrotask(() => { this.#startBackgroundScan(generation); });
    }
  }
}
