/**
 * The scripted executor drives the real Worker and Router boundary using Linear
 * Task refs. Router creates every Task and Run through its Node commands,
 * dispatched in-process through `NodeDispatchDouble` to `WorkerCommandService`.
 * Worker events reach `ingestNodeRunEvent` with the Node's authenticated origin.
 * The executor and Node transport are doubles; no network service is called.
 */
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import test from "node:test";

import {
  SqliteWorkerCommandJournal,
  WorkerCommandError,
  WorkerCommandService,
  deterministicElicitationReplyValue,
  type RouterRunEventSink,
  type RouterRunLifecycleEnvelope,
  type StructuredElicitation,
  type WorkerCommandOptions,
} from "../../src/index.js";
import { channelBindingResolver } from "../../src/router/binding-resolver.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { Router, RouterError, routerTaskEnvelope, type LinearRouterTaskRef, type RouterNodeEventIngestResult } from "../../src/router/router.js";
import type { WorkerCommand, WorkerCommandResponse } from "../../src/shared/router-worker.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint } from "../support/node-dispatch-double.js";
import { ScriptedExecutor, scriptedExecutorRegistration } from "../support/scripted-executor.js";
import { workerSelectionFixture } from "../support/node-routing-fixtures.js";

const decisionId = `axis-decision-${"a".repeat(48)}`;
const fastOptionId = `axis-option-${"c".repeat(48)}`;
const thoroughOptionId = `axis-option-${"d".repeat(48)}`;
const elicitation: StructuredElicitation = {
  contractVersion: "1.0.0-fixed-choice", decisionId, sourceEventId: `axis-elicitation-${"b".repeat(48)}`, recommendedOptionId: fastOptionId,
  header: "Choose mode", question: "Which mode?",
  options: [
    { optionId: fastOptionId, replyValue: deterministicElicitationReplyValue(decisionId, fastOptionId), label: "Fast", description: "Use fast mode." },
    { optionId: thoroughOptionId, replyValue: deterministicElicitationReplyValue(decisionId, thoroughOptionId), label: "Thorough", description: "Use thorough mode." },
  ],
};

const organizationId = "organization-scripted-1";
const teamId = "team-scripted-1";
const endpointRef = { ref: "endpoint-scripted-1", version: "v1" } as const;
const nodeRef = { ref: "node-logical-scripted-1", version: "v1" } as const;
const identity = { nodeId: "node-scripted-1", workerId: "worker-scripted-1", executorKind: "scripted", runtimeProfile: { ref: "executor-scripted-1", version: "v1" } } as const;
const linearRef = (conversation: string): LinearRouterTaskRef => ({ organizationId, issueId: `issue-${conversation}`, teamId, agentSessionId: `session-${conversation}` });

/** Forwards every Worker-emitted envelope through Router's Node event door, as the Node's event ingress does. */
class RouterNodeIngestSink implements RouterRunEventSink {
  readonly results: RouterNodeEventIngestResult[] = [];
  #clockMs = Date.parse("2026-09-07T00:00:01.000Z");
  constructor(private readonly router: () => Router, private readonly port: () => NodeDispatchDouble) {}
  async emit(event: RouterRunLifecycleEnvelope): Promise<void> {
    const recordedAt = new Date(this.#clockMs).toISOString();
    this.#clockMs += 1_000;
    this.results.push(await this.router().ingestNodeRunEvent({ envelope: event, recordedAt, origin: this.port().eventOrigin }));
  }
}

function harness(context: { after(fn: () => unknown): void }, messagePrefix: string) {
  const executor = new ScriptedExecutor();
  const journal = new SqliteWorkerCommandJournal(":memory:");
  const teamBindings = new SqliteTeamBindingStore({});
  teamBindings.configure({ scope: { organizationId, linearTeamId: teamId }, expectedRevision: 0, profile: { ...identity, endpointRef }, audit: { operatorRef: "operator", source: "test", requestRef: "request", authorizationRef: "authorization" } });
  let router!: Router, port!: NodeDispatchDouble, service!: WorkerCommandService;
  const sink = new RouterNodeIngestSink(() => router, () => port);
  let id = 0;
  const config: WorkerCommandOptions = {
    configuration: {
      bearerToken: "worker-scripted-token", workerId: identity.workerId, runtimeKind: "scripted", executorId: identity.runtimeProfile.ref, nodeId: identity.nodeId,
      methodRef: { version: "v1", method: "scripted.execute" }, workspaces: [{ organizationId, workspaceRoot: tmpdir() }],
    },
    registrations: [scriptedExecutorRegistration({ executorId: identity.runtimeProfile.ref, adapter: executor })],
    eventSink: sink,
    journal,
    now: () => new Date("2026-09-07T00:00:01.000Z"),
    messageId: () => `${messagePrefix}-${++id}`,
  };
  service = new WorkerCommandService(config);
  const commands: WorkerCommand[] = [];
  port = new NodeDispatchDouble({
    worker: { async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> { commands.push(command); return (await service.execute(command)).response; } },
    nodeRef,
    reservation: nodeReservationFromEndpoint({ endpointRef, identity, sessionBindingExecutorId: identity.runtimeProfile.ref }, "worker-key-scripted-1"),
  });
  router = new Router({
    nodeRouting: { bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef, nodeRef }] }), port },
    now: () => new Date("2026-09-07T00:00:02.000Z"),
  });
  context.after(() => { service.close(); executor.closeAll(); journal.close(); router.close(); teamBindings.close(); });
  /**
   * Simulate a Worker restart with a fresh WorkerCommandService on the same journal.
   * In-memory observed bindings reset while durable Run records and the scripted
   * executor remain available. Dispatch reads the current service binding, and the
   * prior service is closed so only one instance is live.
   */
  function restart(): void {
    service.close();
    service = new WorkerCommandService(config);
  }
  return { executor, journal, router, sink, commands, restart };
}

/**
 * Waits, on a real deadline, until Router's own history for the Task holds at least `count` events.
 * Router's history rather than the sink: a Run's `run.accepted` can reach the door before the Node's
 * receipt does and be buffered first, so the sink's own results are not the Task's history.
 */
async function settled(router: Router, ref: LinearRouterTaskRef, count: number, deliveryTimeoutMs = 10_000): Promise<readonly string[]> {
  const deadline = Date.now() + deliveryTimeoutMs;
  const kinds = () => router.listTaskEvents(ref).map(({ envelope }) => "deliveryType" in envelope ? envelope.deliveryType : routerTaskEnvelope(envelope).envelopeType);
  while (kinds().length < count && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(kinds().length >= count, `expected at least ${count} Router events, saw ${JSON.stringify(kinds())} after waiting up to ${deliveryTimeoutMs}ms`);
  return kinds();
}
/** Every Worker event must end in Router's history, not in quarantine. */
function assertNoQuarantine(sink: RouterNodeIngestSink): void {
  assert.deepEqual(sink.results.filter((result) => result.status === "quarantined"), [], "no Worker event may be quarantined");
}

test("scripted success: a Linear run reaches Router's task.succeeded", async (context) => {
  const { executor, router, sink } = harness(context, "worker-scripted-success");
  const ref = linearRef("run-scripted-success-1");
  const started = await router.startNode({ ...ref, commandId: "command-scripted-success", instruction: "Do the work.", workerSelection: workerSelectionFixture() });
  assert.equal(started.status, "accepted");
  const binding = executor.lastBinding;
  assert.ok(binding !== undefined);
  executor.pushSucceeded(binding);
  executor.close(binding);

  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", "task.succeeded"]);
  assertNoQuarantine(sink);
});

test("scripted failure: a Linear run reaches Router's task.failed", async (context) => {
  const { executor, router, sink } = harness(context, "worker-scripted-failure");
  const ref = linearRef("run-scripted-failure-1");
  assert.equal((await router.startNode({ ...ref, commandId: "command-scripted-failure", instruction: "Do the work.", workerSelection: workerSelectionFixture() })).status, "accepted");
  const binding = executor.lastBinding;
  assert.ok(binding !== undefined);
  executor.pushFailed(binding);
  executor.close(binding);

  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", "task.failed"]);
  assertNoQuarantine(sink);
});

test("scripted waiting: a Linear run's elicitation reaches Router as run.waiting_for_input, projected to task.waiting_for_input", async (context) => {
  const { executor, router, sink } = harness(context, "worker-scripted-waiting");
  const ref = linearRef("run-scripted-waiting-1");
  assert.equal((await router.startNode({ ...ref, commandId: "command-scripted-waiting", instruction: "Do the work.", workerSelection: workerSelectionFixture() })).status, "accepted");
  const binding = executor.lastBinding;
  assert.ok(binding !== undefined);
  executor.pushWaiting(binding, elicitation);
  executor.close(binding);

  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", "task.waiting_for_input"]);
  assertNoQuarantine(sink);

  // Router retains the Linear source identity and the elicitation in the bare Axis extension.
  const waiting = router.listTaskEvents(ref)[1]?.envelope;
  assert.ok(waiting !== undefined && waiting.envelopeType === "task.waiting_for_input");
  if (waiting?.envelopeType !== "task.waiting_for_input") throw new Error("unreachable");
  assert.deepEqual(waiting.task.sourceRef, { system: "linear", organizationId: ref.organizationId, issueId: ref.issueId, agentSessionId: ref.agentSessionId });
  assert.equal(waiting.elicitation.decisionId, decisionId);
  assert.equal(waiting.elicitation.sourceEventId, elicitation.sourceEventId);
});

test("scripted continue-after-waiting: a follow-up instruction on a Linear run reaches Router's task.succeeded", async (context) => {
  const { executor, router, sink, commands } = harness(context, "worker-scripted-continue");
  const ref = linearRef("run-scripted-continue-1");
  const started = await router.startNode({ ...ref, commandId: "command-scripted-continue-start", instruction: "Do the work.", workerSelection: workerSelectionFixture() });
  assert.equal(started.status, "accepted");
  if (started.status !== "accepted") throw new Error("unreachable");
  const startBinding = executor.lastBinding;
  assert.ok(startBinding !== undefined);
  executor.pushWaiting(startBinding, elicitation);
  executor.close(startBinding);
  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", "task.waiting_for_input"]);

  // Router creates the successor Run itself and the Worker receives a real `continue` whose
  // continuation names the waiting Run.
  const continued = await router.continueNode({ ...ref, commandId: "command-scripted-continue-followup", instruction: "Fast" });
  assert.equal(continued.status, "accepted");
  if (continued.status !== "accepted") throw new Error("unreachable");
  assert.notEqual(continued.runId, started.runId);
  const continuation = commands.at(-1);
  assert.equal(continuation?.command, "continue");
  if (continuation?.command !== "continue") throw new Error("unreachable");
  assert.equal(continuation.continuation.priorRunId, started.runId);

  const continueBinding = executor.lastBinding;
  assert.ok(continueBinding !== undefined && continueBinding.turnId !== startBinding.turnId, "a continuation must bind to a fresh native turn, not reuse the waiting run's");
  assert.equal(continueBinding?.threadId, startBinding.threadId, "a continuation must stay on the same native thread as the run it continues, exactly like the real Codex adapter");
  executor.pushSucceeded(continueBinding);
  executor.close(continueBinding);

  const kinds = await settled(router, ref, 4);
  assert.deepEqual(kinds.slice(2), ["task.accepted", "task.succeeded"]);
  const successorEvents = router.listTaskEvents(ref).slice(2).map(({ envelope }) => routerTaskEnvelope(envelope).run.runId);
  assert.deepEqual(successorEvents, [continued.runId, continued.runId]);
  assertNoQuarantine(sink);
});

test("scripted cancel: an interrupt while a Linear run is active is acknowledged, and its real completion reaches Router's task.failed", async (context) => {
  const { executor, journal, router, sink } = harness(context, "worker-scripted-cancel");
  const ref = linearRef("run-scripted-cancel-1");
  const started = await router.startNode({ ...ref, commandId: "command-scripted-cancel-start", instruction: "Do the work.", workerSelection: workerSelectionFixture() });
  assert.equal(started.status, "accepted");
  if (started.status !== "accepted") throw new Error("unreachable");
  const binding = executor.lastBinding;
  assert.ok(binding !== undefined);
  assert.deepEqual(await settled(router, ref, 1), ["task.accepted"]);

  // The run is genuinely still active here -- no terminal has been pushed -- so this exercises
  // cancellation of a run that has not yet reached any terminal state on its own.
  const cancelled = await router.cancelNode({ ...ref, commandId: "command-scripted-cancel-interrupt" });
  assert.equal(cancelled.status, "interrupt_requested");
  assert.ok(executor.wasCancelRequested(binding), "the scripted executor's cancel() must actually have been called");

  // Cancellation is only ACKNOWLEDGED above -- a real cancel does not complete until the provider's
  // own terminal is observed. Checked two ways: Worker's own journal holds no terminal outcome yet,
  // and Router's history holds none either.
  assert.equal(journal.readRun(started.runId)?.terminal, undefined, "cancellation acknowledgement must not itself record a terminal outcome");
  assert.deepEqual(await settled(router, ref, 1), ["task.accepted"]);

  executor.pushCancelled(binding);
  executor.close(binding);

  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", "task.failed"]);
  assertNoQuarantine(sink);
});

/**
 * Classify launch failures by result shape as well as error code. Read the Worker's own
 * answer: runtime_missing leaves the Run reserved, while launch_ambiguous marks it
 * observational. NodeDispatchDouble bypasses the real Node's definite/ambiguous receipt
 * classification.
 */
test("a continuation failure that echoes back the prior binding is still classified exactly, and does not isolate the Run", async (context) => {
  const { executor, router, journal, commands } = harness(context, "worker-scripted-echo");
  const ref = linearRef("run-scripted-echo-1");
  const started = await router.startNode({ ...ref, commandId: "command-scripted-echo-start", instruction: "Do the work.", workerSelection: workerSelectionFixture() });
  assert.equal(started.status, "accepted");
  if (started.status !== "accepted") throw new Error("unreachable");
  const startBinding = executor.lastBinding;
  assert.ok(startBinding !== undefined);
  executor.pushWaiting(startBinding, elicitation);
  executor.close(startBinding);
  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", "task.waiting_for_input"]);

  // Exactly what `failedContinue(request, code, request.binding)` produces.
  executor.refuseNextContinue = { code: "executor.runtime_missing", echoBinding: true };
  await assert.rejects(
    router.continueNode({ ...ref, commandId: "command-scripted-echo-followup", instruction: "Fast" }),
    (error: unknown) => {
      assert.ok(error instanceof RouterError);
      const cause = error.cause;
      assert.ok(cause instanceof WorkerCommandError, "the Worker's own refusal must reach Router");
      assert.equal(cause.code, "axis.worker.runtime_missing", "an echoed prior binding must not downgrade a definite refusal to axis.worker.launch_ambiguous");
      return true;
    },
  );

  // The successor Run was reserved and then refused; it must not be isolated as observational,
  // which is what the unclassified fall-through did to it.
  const refused = commands.at(-1);
  assert.equal(refused?.command, "continue");
  if (refused?.command !== "continue") throw new Error("unreachable");
  assert.equal(journal.readRun(refused.start.identity.run.runId)?.state, "reserved", "an exactly classified refusal must not mark the Run observational");
});

/**
 * Exercise reattestTerminal after resetting the Worker's in-memory observed bindings.
 * Only a positive provider decline means continuation_unresumable; invalid or
 * unavailable proof means capability_unavailable.
 *
 * Give each scenario its own conversation because a refusal leaves this in-process
 * Router command dispatching. Assert WorkerCommandError codes directly: this harness
 * does not exercise the real Node receipt or Router safe-code mapping.
 */
test("a restarted Worker's reattestTerminal proof settles a continuation exactly: only a positive decline is told the session is unresumable", async (context) => {
  async function priorSucceededRun(prefix: string) {
    const h = harness(context, prefix);
    const ref = linearRef(`run-${prefix}`);
    const started = await h.router.startNode({ ...ref, commandId: `command-${prefix}-start`, instruction: "Do the work.", workerSelection: workerSelectionFixture() });
    assert.equal(started.status, "accepted");
    if (started.status !== "accepted") throw new Error("unreachable");
    const binding = h.executor.lastBinding;
    assert.ok(binding !== undefined);
    h.executor.pushSucceeded(binding);
    h.executor.close(binding);
    assert.deepEqual(await settled(h.router, ref, 2), ["task.accepted", "task.succeeded"]);
    // Simulate the Worker restart that empties `#observedBindings` and forces the continuation
    // binding resolution onto `reattestTerminal`.
    h.restart();
    return { ...h, ref };
  }

  // 1. `reattestTerminal` answers `null`: the provider was reached and positively declined. This
  // must settle as the definite, session-is-gone code -- not the overloaded `capability_unavailable`.
  {
    const { executor, router, ref } = await priorSucceededRun("worker-scripted-reattest-null");
    executor.nextReattestTerminal = "null";
    await assert.rejects(
      router.continueNode({ ...ref, commandId: "command-scripted-reattest-null-continue", instruction: "Continue." }),
      (error: unknown) => {
        assert.ok(error instanceof RouterError);
        const cause = error.cause;
        assert.ok(cause instanceof WorkerCommandError, "the Worker's own refusal must reach Router");
        assert.equal(cause.code, "axis.worker.continuation_unresumable", "a positively declined proof must settle as the definite continuation_unresumable, not capability_unavailable");
        return true;
      },
    );
  }

  // 2. `reattestTerminal` answers a proof that does not validate (binding mismatch): the provider
  // was reached, but the answer is unusable. This is a capability problem, not a session-is-gone
  // claim, and must NOT be upgraded to `continuation_unresumable`.
  {
    const { executor, router, ref } = await priorSucceededRun("worker-scripted-reattest-unavailable");
    executor.nextReattestTerminal = "unavailable";
    await assert.rejects(
      router.continueNode({ ...ref, commandId: "command-scripted-reattest-unavailable-continue", instruction: "Continue." }),
      (error: unknown) => {
        assert.ok(error instanceof RouterError);
        const cause = error.cause;
        assert.ok(cause instanceof WorkerCommandError);
        assert.equal(cause.code, "axis.worker.capability_unavailable", "an unusable proof must stay on capability_unavailable, not be upgraded to continuation_unresumable");
        return true;
      },
    );
  }

  // An unreachable proof oracle yields capability_unavailable. It must not be reported
  // as the positive decline continuation_unresumable, which would incorrectly tell the
  // user that the session is gone.
  {
    const { executor, router, ref } = await priorSucceededRun("worker-scripted-reattest-throw");
    executor.nextReattestTerminal = "throw";
    await assert.rejects(
      router.continueNode({ ...ref, commandId: "command-scripted-reattest-throw-continue", instruction: "Continue." }),
      (error: unknown) => {
        assert.ok(error instanceof RouterError);
        const cause = error.cause;
        assert.ok(cause instanceof WorkerCommandError);
        assert.equal(cause.code, "axis.worker.capability_unavailable", "an unreachable oracle must not be told to the user as \"your session is gone\"");
        return true;
      },
    );
  }
});
