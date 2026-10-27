import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { channelBindingResolver } from "../src/router/binding-resolver.js";
import { nodeChannelCommandPort } from "../src/relay/node-routing-composition.js";
import type { ChannelCommand, ChannelCommandOutcome, ChannelCommandPort, ChannelInterruptedCommandRef, ChannelReconcileOutcome, ChannelReconcileQuery } from "../src/router/channel-command.js";
import { channelContinueCommand, channelStartCommand } from "../src/router/channel-command.js";
import {
  channelInstructionDigest,
  linearChannelConversationScope,
  type ChannelDeliveryCreateResult,
  type ChannelDeliveryReconcileResult,
  type ChannelDeliveryTarget,
  type ChannelDigest,
  type ChannelMessageContent,
  type ChannelOutboxDeliverer,
  type ChannelOutboxRecord,
  type ChannelRuntimeTask,
} from "../src/router/channel-conversation-types.js";
import { linearChannelIdentity, linearTenantRef } from "../src/router/channel-identity.js";
import { SqliteChannelConversationStore } from "../src/router/channel-conversation.js";
import { SqliteChannelRunChainStore } from "../src/router/channel-run-chain.js";
import { SqliteTeamBindingStore } from "../src/router/team-bindings.js";
import { Router } from "../src/router/router.js";
import type { RouterEventReceipt } from "../src/router/router.js";
import type { LinearRouterTaskRef } from "../src/router/router.js";
import { SqliteChannelOutboxStore } from "../src/relay/channel-outbox.js";
import { LinearEventRenderer, type LinearDecisionOwnerLookup } from "../src/relay/linear/activity.js";
import { linearActivityId } from "../src/relay/linear/delivery.js";
import type { VerifiedLinearOwner } from "../src/relay/linear/authorization.js";
import type { LinearChannelTaskRefLookup } from "../src/relay/linear/observe.js";
import type { LinearRedispatchSource, LinearRedispatchSourceLookup } from "../src/relay/linear/redispatch.js";
import type { WorkerCommand, WorkerCommandResponse } from "../src/shared/router-worker.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint, projectNodeEvent, type WorkerCommandDispatcher } from "./support/node-dispatch-double.js";
import type { RunAcceptedEnvelope, RunFailedEnvelope, RunProgressedEnvelope, RunStartEnvelope, RunSucceededEnvelope } from "../src/shared/runtime-boundary.js";
import { workerSelectionFixture } from "./support/node-routing-fixtures.js";

/**
 * Real Router, conversation, run-chain and outbox stores with Worker and Linear
 * provider doubles. These tests need persisted reservation, decision and redispatch
 * semantics, which a caller-scripted run chain cannot establish.
 *
 * Use the SQL outbox ordering for starvation rotation: least recently attempted
 * reconciliation first, then creation time and sequence. A scripted return value or
 * independent JavaScript sort would not test that ordering.
 */

export const ORGANIZATION_ID = "organization-linear-411";
export const TEAM_ID = "team-linear-411";
export const ISSUE_ID = "issue-linear-411";
export const APP_USER_ID = "app-synthetic-user";

const endpointRef = { ref: "endpoint-linear-411", version: "v1" } as const;
const nodeRef = { ref: "node-linear-411", version: "v1" } as const;
const sessionBindingExecutorId = "executor-linear-411";
const workerIdentity = { nodeId: "node-linear-411", workerId: "worker-linear-411", executorKind: "codex", runtimeProfile: { ref: "executor-linear-411", version: "v1" } } as const;
const audit = { operatorRef: "operator-linear-411", source: "linear-loop_test", requestRef: "request-linear-411", authorizationRef: "authorization-linear-411" } as const;

export class WorkerPortDouble implements WorkerCommandDispatcher {
  readonly commands: WorkerCommand[] = [];

  async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> {
    this.commands.push(command);
    if (command.command === "cancel") {
      return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: command.target.runId };
    }
    if (command.command === "steer") {
      return { contractVersion: command.contractVersion, commandId: command.commandId, status: "steered", runId: command.target.runId };
    }
    return {
      contractVersion: command.contractVersion, commandId: command.commandId, status: "accepted", identity: workerIdentity,
      runAccepted: acceptedEnvelope(command.start, `accepted-${command.commandId}`),
    };
  }

  /** The `run.start` envelope for one dispatched command, by index. */
  start(index: number): RunStartEnvelope {
    const command = this.commands[index];
    if (command === undefined || command.command === "cancel" || command.command === "steer") throw new Error(`no start/continue command at index ${index}`);
    return command.start;
  }
}

export function acceptedEnvelope(start: RunStartEnvelope, messageId: string): RunAcceptedEnvelope {
  return { contractVersion: start.contractVersion, messageId, correlationId: start.correlationId, operationId: start.operationId, causationMessageId: start.messageId, occurredAt: "2026-09-09T00:00:01.000Z", producer: "worker", consumer: "router", envelopeType: "run.accepted", task: start.identity.task, attempt: start.identity.attempt, run: { ...start.identity.run, state: "running" } };
}
export function progressedEnvelope(start: RunStartEnvelope, messageId: string, safeMessage = "Working."): RunProgressedEnvelope {
  return { ...acceptedEnvelope(start, messageId), envelopeType: "run.progressed", progress: { completed: 1, total: 2, safeMessage } };
}
export function succeededEnvelope(start: RunStartEnvelope, messageId: string): RunSucceededEnvelope {
  const resultRef = `axis-evidence://${start.identity.run.runId}/result`;
  return { ...acceptedEnvelope(start, messageId), envelopeType: "run.succeeded", run: { ...start.identity.run, state: "succeeded", resultRef }, resultRef };
}
export function failedEnvelope(start: RunStartEnvelope, messageId: string, code = "execution_failed", safeMessage = "The execution failed."): RunFailedEnvelope {
  const error = { code, safeMessage };
  return { ...acceptedEnvelope(start, messageId), envelopeType: "run.failed", run: { ...start.identity.run, state: "failed", error }, error };
}
/** A `failed` envelope whose error code is `execution_cancelled`, which the neutral projection reports as `cancelled`. */
export function cancelledEnvelope(start: RunStartEnvelope, messageId: string): RunFailedEnvelope {
  return failedEnvelope(start, messageId, "execution_cancelled", "Execution was cancelled.");
}

/**
 * A Linear Runtime v1 `Task` for one AgentSession.
 *
 * The `sourceRef` key set is exact and load-bearing: the run chain's own
 * `#task` accepts a Linear source only as
 * `{agentSessionId, issueId, organizationId, system}` and treats any other key
 * set as malformed rather than mismatched. A helper is what keeps every test
 * from hand-writing that literal and drifting from it.
 */
export function linearTask(agentSessionId: string, input: { readonly taskId?: string; readonly correlationId?: string; readonly issueId?: string } = {}): ChannelRuntimeTask {
  return {
    contractVersion: "1.0.0",
    taskId: input.taskId ?? "task-linear-411",
    correlationId: input.correlationId ?? "correlation-linear-411",
    sourceRef: { system: "linear", organizationId: ORGANIZATION_ID, issueId: input.issueId ?? ISSUE_ID, agentSessionId },
  } as unknown as ChannelRuntimeTask;
}

/** A Router outcome for a command Router accepted, as `settle` consumes it. */
export function acceptedOutcome(commandId: string, taskId: string, runId: string): ChannelCommandOutcome {
  return { commandId, taskId, runId, status: "accepted" };
}
/**
 * A Router outcome for a `continue` Router accepted as an interrupt. This is
 * the ONLY outcome that writes a redispatch marker, and `runId` must be the
 * still-executing Run the reservation named -- `settle` refuses a mismatch
 * with `run_state_conflict`.
 */
export function interruptedOutcome(commandId: string, taskId: string, runId: string): ChannelCommandOutcome {
  return { commandId, taskId, runId, status: "interrupt_requested" };
}

/** The frozen decision owner every test uses, with the canonical HTTPS profile URL `renderElicitationMarkdown` requires. */
export const OWNER: VerifiedLinearOwner = Object.freeze({ userId: "synthetic-user", userUrl: "https://linear.app/example-workspace/profiles/synthetic-user" });

export function ownerLookup(owner: VerifiedLinearOwner | null = OWNER): LinearDecisionOwnerLookup {
  return { decisionOwner: () => owner };
}

/** A `LinearChannelTaskRefLookup` over an explicit table, so a test can make one conversation unresolvable. */
export function taskRefLookup(table: ReadonlyMap<string, LinearRouterTaskRef>): LinearChannelTaskRefLookup {
  return { taskRef: ({ conversationId }) => table.get(conversationId) ?? null };
}

/** A `LinearRedispatchSourceLookup` over an explicit table, keyed exactly as the port is: by `(conversationId, commandId)`. */
export function redispatchSourceLookup(table: ReadonlyMap<string, LinearRedispatchSource>): LinearRedispatchSourceLookup & { readonly queries: readonly string[] } {
  const queries: string[] = [];
  return {
    queries,
    redispatchSource: ({ conversationId, commandId }) => {
      queries.push(`${conversationId}|${commandId}`);
      return table.get(`${conversationId}|${commandId}`) ?? null;
    },
  };
}

export interface LinearLoopHarness {
  readonly router: Router;
  readonly worker: WorkerPortDouble;
  readonly conversations: SqliteChannelConversationStore;
  readonly runChain: SqliteChannelRunChainStore;
  readonly outbox: SqliteChannelOutboxStore;
  readonly dispatch: ChannelCommandPort;
  readonly renderer: LinearEventRenderer;
  readonly taskRefs: LinearChannelTaskRefLookup;
  readonly taskRefTable: Map<string, LinearRouterTaskRef>;
  /** Opens a conversation for one `agentSessionId` and returns its task ref, registering it with `taskRefs`. */
  bind(agentSessionId: string, issueId?: string): LinearRouterTaskRef;
  /**
   * Project a Worker lifecycle envelope onto the Task through Router's Node event
   * ingress.
   */
  ingest(input: { readonly envelope: Parameters<Router["ingestNodeRunEvent"]>[0]["envelope"]; readonly recordedAt: string }): Promise<RouterEventReceipt>;
  /** Reserves, dispatches and settles one command, returning the outcome and the reservation's commandId. */
  submit(input: { readonly agentSessionId: string; readonly kind: "start" | "continue"; readonly instruction: string; readonly commandId: string }): Promise<{ readonly outcome: ChannelCommandOutcome; readonly instructionDigest: ChannelDigest }>;
  /**
   * Advance the run-chain clock explicitly to reach a persisted next_attempt_at. The
   * normal clock advances only 1ms per read, so consecutive ticks cannot bypass retry
   * spacing.
   */
  advanceRunChainClock(milliseconds: number): void;
}

export function linearLoopHarness(context: { after(fn: () => unknown): void }): LinearLoopHarness {
  const directory = mkdtempSync(join(tmpdir(), "axis-linear-loop-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));

  const teamBindings = new SqliteTeamBindingStore({});
  context.after(() => teamBindings.close());
  teamBindings.configure({
    scope: { organizationId: ORGANIZATION_ID, linearTeamId: TEAM_ID },
    expectedRevision: 0,
    profile: { ...workerIdentity, endpointRef },
    audit,
  });

  const worker = new WorkerPortDouble();
  // Resolve endpointRef to one logical Node; the Node reservation carries the concrete
  // Worker identity.
  const port = new NodeDispatchDouble({
    worker, nodeRef,
    reservation: nodeReservationFromEndpoint({ endpointRef, identity: workerIdentity, sessionBindingExecutorId }, "worker-key-linear-411"),
  });
  const router = new Router({
    nodeRouting: {
      bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef, nodeRef }] }),
      port,
    },
    now: () => new Date("2026-09-09T00:00:00.000Z"),
  });
  context.after(() => router.close());

  const conversations = new SqliteChannelConversationStore({ filename: join(directory, "conversations.sqlite") });
  context.after(() => conversations.close());

  let runChainClockMs = Date.parse("2026-09-09T00:00:00.000Z");
  const advanceRunChainClock = (milliseconds: number): void => { runChainClockMs += milliseconds; };
  const runChain = new SqliteChannelRunChainStore({ conversations, filename: join(directory, "run-chain.sqlite"), now: () => new Date(runChainClockMs++) });
  context.after(() => runChain.close());

  let outboxClockMs = Date.parse("2026-09-09T00:00:00.000Z");
  const outbox = new SqliteChannelOutboxStore(join(directory, "outbox.sqlite"), {
    now: () => new Date(outboxClockMs++),
    // Preserve the Worker receipt's activityId when rendering supplies one; otherwise
    // mint a UUID v4. This keeps the harness from discarding the preferred delivery
    // identity.
    messageId: ({ rendered }) => linearActivityId((rendered as { readonly preferredActivityId?: string } | undefined)?.preferredActivityId),
  });
  context.after(() => outbox.close());

  const dispatch = nodeChannelCommandPort(router);
  const renderer = new LinearEventRenderer({ owners: ownerLookup() });
  const taskRefTable = new Map<string, LinearRouterTaskRef>();

  return {
    router, worker, conversations, runChain, outbox, dispatch, renderer,
    taskRefTable, advanceRunChainClock,
    ingest: (input) => projectNodeEvent(router, port, input),
    taskRefs: taskRefLookup(taskRefTable),
    bind(agentSessionId, issueId = ISSUE_ID): LinearRouterTaskRef {
      const scope = linearChannelConversationScope({ organizationId: ORGANIZATION_ID, agentSessionId });
      const resolution = conversations.openOrResume({ scope });
      // Linear's agentSessionId is the conversation identity. Assert that the registry
      // preserves it because all later identity comparisons depend on it.
      if (resolution.conversation.conversationId !== agentSessionId) {
        throw new Error(`the registry minted ${resolution.conversation.conversationId} for agent session ${agentSessionId}`);
      }
      const taskRef: LinearRouterTaskRef = { organizationId: ORGANIZATION_ID, issueId, teamId: TEAM_ID, agentSessionId };
      taskRefTable.set(agentSessionId, taskRef);
      return taskRef;
    },
    async submit({ agentSessionId, kind, instruction, commandId }) {
      const taskRef = taskRefTable.get(agentSessionId);
      if (taskRef === undefined) throw new Error(`agent session ${agentSessionId} is not bound`);
      const instructionDigest = channelInstructionDigest(instruction);
      runChain.reserve({
        scope: linearChannelConversationScope({ organizationId: ORGANIZATION_ID, agentSessionId }),
        conversationId: agentSessionId,
        tenant: linearTenantRef(ORGANIZATION_ID),
        commandId,
        kind,
        instructionDigest,
      });
      const identity = linearChannelIdentity(taskRef);
      // command requires the routing intent to be explicit on a start and absent
      // on a continue: `channelWorkerSelection` refuses a start with no
      // `workerSelection` property at all, and `nodeChannelCommandPort` refuses
      // a continue that carries one. Omission is never read as "default".
      const command = kind === "start"
        ? channelStartCommand({ commandId, identity, instruction, workerSelection: workerSelectionFixture() })
        : channelContinueCommand({ commandId, identity, instruction });
      const outcome = await dispatch.submit(command);
      runChain.settle({ commandId, outcome });
      return { outcome, instructionDigest };
    },
  };
}

/**
 * A `ChannelOutboxDeliverer` that records what it was asked to post or
 * reconcile and answers from a script.
 *
 * The recording is the observation these tests need. `ChannelOutboxPort` has
 * no "list the pending rows" reader -- deliberately, since `claim` is the only
 * safe way to look at one -- so the way to see what a tick enqueued is to
 * drain it through the real delivery loop and watch what reaches the
 * deliverer. That also makes the assertion end-to-end rather than a peek at
 * an intermediate table.
 *
 * `create` confirms with the row's own id as the provider message id, which is
 * what Linear genuinely does: the outbox row id IS the Agent Activity id
 * (`docs/LINEAR-PROVIDER.md` requires delivery not to regenerate or transform
 * it), so a fabricated provider id here would model the wrong channel.
 */
export class RecordingLinearDeliverer implements ChannelOutboxDeliverer {
  readonly created: { readonly id: string; readonly content: ChannelMessageContent; readonly target: ChannelDeliveryTarget }[] = [];
  readonly reconciled: string[] = [];
  /** Answers `create`. Default: confirm. */
  createResult: (record: ChannelOutboxRecord) => ChannelDeliveryCreateResult = (record) => ({ disposition: "confirmed", providerMessageId: record.id });
  /** Answers `reconcile`. Default: `unresolved`, the outcome that must rotate. */
  reconcileResult: (record: ChannelOutboxRecord) => ChannelDeliveryReconcileResult = () => ({ disposition: "unresolved" });

  async create(input: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }): Promise<ChannelDeliveryCreateResult> {
    this.created.push({ id: input.record.id, content: input.record.content, target: input.target });
    return this.createResult(input.record);
  }

  async reconcile(input: { readonly target: ChannelDeliveryTarget; readonly record: ChannelOutboxRecord }): Promise<ChannelDeliveryReconcileResult> {
    this.reconciled.push(input.record.id);
    return this.reconcileResult(input.record);
  }
}

/** A `ChannelCommandPort` that records submissions and answers from a script. */
export class RecordingCommandPort implements ChannelCommandPort {
  readonly submitted: ChannelCommand[] = [];
  readonly reconciled: string[] = [];
  outcome: (command: ChannelCommand) => ChannelCommandOutcome = (command) => ({ commandId: command.commandId, status: "rejected", safeCode: "axis.router.conflict" });
  reconcileOutcome: ChannelReconcileOutcome = "unknown";
  interrupted: ChannelInterruptedCommandRef | null = null;
  /**
   * Router's retained accepted lineage. null represents acceptance without resolvable
   * Run lineage.
   */
  acceptedLineage: ReturnType<NonNullable<ChannelCommandPort["reconcileAccepted"]>> = null;

  async submit(command: ChannelCommand): Promise<ChannelCommandOutcome> { this.submitted.push(command); return this.outcome(command); }
  reconcile(query: ChannelReconcileQuery): ChannelReconcileOutcome { this.reconciled.push(query.commandId); return this.reconcileOutcome; }
  reconcileInterrupted(_query: ChannelReconcileQuery): ChannelInterruptedCommandRef | null { return this.interrupted; }
  reconcileAccepted(_query: ChannelReconcileQuery): ReturnType<NonNullable<ChannelCommandPort["reconcileAccepted"]>> { return this.acceptedLineage; }
}
