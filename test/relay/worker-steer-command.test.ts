/**
 * Exercise Worker steer through the real WorkerCommandService and SQLite journal. A
 * real Router creates the running Run through NodeDispatchDouble; steer requests then
 * enter the service directly as HTTP ingress would hand them over.
 */
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import test from "node:test";

import {
  SqliteWorkerCommandJournal,
  WORKER_COMMAND_VERSION,
  WorkerCommandError,
  WorkerCommandService,
  type RouterRunEventSink,
  type RouterRunLifecycleEnvelope,
  type WorkerCommandOptions,
} from "../../src/index.js";
import { channelBindingResolver } from "../../src/router/binding-resolver.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { Router, routerTaskEnvelope, type LinearRouterTaskRef } from "../../src/router/router.js";
import { RouterHttpTransportError, validateRouterWorkerResponse, type WorkerCommand, type WorkerCommandResponse, type WorkerCommandSteer } from "../../src/shared/router-worker.js";
import { elicitationFixture } from "../support/elicitation-fixtures.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint } from "../support/node-dispatch-double.js";
import { workerSelectionFixture } from "../support/node-routing-fixtures.js";
import { ScriptedExecutor, scriptedExecutorRegistration } from "../support/scripted-executor.js";

const organizationId = "organization-steer-1";
const teamId = "team-steer-1";
const endpointRef = { ref: "endpoint-steer-1", version: "v1" } as const;
const nodeRef = { ref: "node-logical-steer-1", version: "v1" } as const;
const identity = { nodeId: "node-steer-1", workerId: "worker-steer-1", executorKind: "scripted", runtimeProfile: { ref: "executor-steer-1", version: "v1" } } as const;
const linearRef = (conversation: string): LinearRouterTaskRef => ({ organizationId, issueId: `issue-${conversation}`, teamId, agentSessionId: `session-${conversation}` });

class RouterNodeIngestSink implements RouterRunEventSink {
  #clockMs = Date.parse("2026-09-24T00:00:01.000Z");
  constructor(private readonly router: () => Router, private readonly port: () => NodeDispatchDouble) {}
  async emit(event: RouterRunLifecycleEnvelope): Promise<void> {
    const recordedAt = new Date(this.#clockMs).toISOString();
    this.#clockMs += 1_000;
    await this.router().ingestNodeRunEvent({ envelope: event, recordedAt, origin: this.port().eventOrigin });
  }
}

function harness(context: { after(fn: () => unknown): void }, prefix: string, steer: boolean) {
  const executor = new ScriptedExecutor();
  const journal = new SqliteWorkerCommandJournal(":memory:");
  const teamBindings = new SqliteTeamBindingStore({});
  teamBindings.configure({ scope: { organizationId, linearTeamId: teamId }, expectedRevision: 0, profile: { ...identity, endpointRef }, audit: { operatorRef: "operator", source: "test", requestRef: "request", authorizationRef: "authorization" } });
  let router!: Router, port!: NodeDispatchDouble;
  let id = 0;
  const config: WorkerCommandOptions = {
    configuration: {
      bearerToken: "worker-steer-token", workerId: identity.workerId, runtimeKind: "scripted", executorId: identity.runtimeProfile.ref, nodeId: identity.nodeId,
      methodRef: { version: "v1", method: "scripted.execute" }, workspaces: [{ organizationId, workspaceRoot: tmpdir() }],
    },
    registrations: [scriptedExecutorRegistration({ executorId: identity.runtimeProfile.ref, adapter: executor, steer })],
    eventSink: new RouterNodeIngestSink(() => router, () => port),
    journal,
    now: () => new Date("2026-09-24T00:00:01.000Z"),
    messageId: () => `${prefix}-${++id}`,
  };
  const service = new WorkerCommandService(config);
  port = new NodeDispatchDouble({
    worker: { async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> { return (await service.execute(command)).response; } },
    nodeRef,
    reservation: nodeReservationFromEndpoint({ endpointRef, identity, sessionBindingExecutorId: identity.runtimeProfile.ref }, "worker-key-steer-1"),
  });
  router = new Router({
    nodeRouting: { bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef, nodeRef }] }), port },
    now: () => new Date("2026-09-24T00:00:02.000Z"),
  });
  context.after(() => { service.close(); executor.closeAll(); journal.close(); router.close(); teamBindings.close(); });

  /** Starts a Linear Task through Router and returns its running Run and native binding. */
  async function running(conversation: string) {
    const ref = linearRef(conversation);
    const started = await router.startNode({ ...ref, commandId: `${prefix}-start-${conversation}`, instruction: "Do the work.", workerSelection: workerSelectionFixture() });
    if (started.status !== "accepted") throw new Error(`start was not accepted: ${JSON.stringify(started)}`);
    const binding = executor.lastBinding;
    assert.ok(binding !== undefined);
    return { ref, runId: started.runId, binding };
  }
  return { executor, journal, router, service, running };
}

const steerCommand = (commandId: string, runId: string, instruction: string): WorkerCommandSteer => ({ contractVersion: WORKER_COMMAND_VERSION, commandId, command: "steer", target: { runId }, instruction });

async function settled(router: Router, ref: LinearRouterTaskRef, count: number): Promise<readonly string[]> {
  const deadline = Date.now() + 10_000;
  const kinds = () => router.listTaskEvents(ref).map(({ envelope }) => "deliveryType" in envelope ? envelope.deliveryType : routerTaskEnvelope(envelope).envelopeType);
  while (kinds().length < count && Date.now() < deadline) await new Promise((resolve) => setImmediate(resolve));
  return kinds();
}

async function refusal(action: Promise<unknown>): Promise<{ readonly code: string; readonly statusCode: number }> {
  try { await action; } catch (error) {
    assert.ok(error instanceof WorkerCommandError, String(error));
    return { code: error.code, statusCode: error.statusCode };
  }
  assert.fail("expected a WorkerCommandError");
}

test("steer: an executor that declares steer takes the instruction into the running turn and the Worker answers steered, replayably", async (context) => {
  const { executor, journal, router, service, running } = harness(context, "steer-supported", true);
  const { ref, runId, binding } = await running("supported");

  const first = await service.execute(steerCommand("steer-supported-1", runId, "Also update the changelog."));
  assert.deepEqual(first, { response: { contractVersion: WORKER_COMMAND_VERSION, commandId: "steer-supported-1", status: "steered", runId }, replayed: false });
  assert.deepEqual(executor.steers, [{ requestId: "steer-supported-1", binding, instruction: "Also update the changelog." }]);
  assert.equal(executor.wasCancelRequested(binding), false, "a delivered steer must not interrupt the turn");

  // Same command id and instruction: the journaled response, without asking the executor again.
  assert.deepEqual(await service.execute(steerCommand("steer-supported-1", runId, "Also update the changelog.")), { response: first.response, replayed: true });
  assert.equal(executor.steers.length, 1);
  // Same command id, different instruction: the intent digest covers the instruction.
  assert.deepEqual(await refusal(service.execute(steerCommand("steer-supported-1", runId, "Something else."))), { code: "axis.worker.intent_conflict", statusCode: 409 });

  // The instruction itself is never journaled; only its digest is.
  assert.doesNotMatch(journal.readCommand("steer-supported-1")?.intentJson ?? "", /changelog/u);
  assert.match(journal.readCommand("steer-supported-1")?.intentJson ?? "", /instructionDigest/u);

  // The steered turn still ends on its own and reaches Router.
  executor.pushSucceeded(binding);
  executor.close(binding);
  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", "task.succeeded"]);
});

test("steer: a turn that is already finishing (steer_too_late) answers interrupt_requested without cancelling it", async (context) => {
  const { executor, service, running } = harness(context, "steer-late", true);
  const { runId, binding } = await running("late");
  executor.nextSteer = "too_late";

  const result = await service.execute(steerCommand("steer-late-1", runId, "Too late."));
  assert.deepEqual(result.response, { contractVersion: WORKER_COMMAND_VERSION, commandId: "steer-late-1", status: "interrupt_requested", runId });
  assert.equal(executor.steers.length, 1);
  assert.equal(executor.wasCancelRequested(binding), false, "steer_too_late must not call cancel");
});

test("steer: any other executor steer failure is ambiguous, as a malformed cancel result is", async (context) => {
  const { executor, service, running } = harness(context, "steer-failed", true);
  const { runId, binding } = await running("failed");
  executor.nextSteer = "failed";

  assert.deepEqual(await refusal(service.execute(steerCommand("steer-failed-1", runId, "Try."))), { code: "axis.worker.launch_ambiguous", statusCode: 503 });
  assert.equal(executor.wasCancelRequested(binding), false);
});

test("steer: an executor without steer takes cancel's path and answers interrupt_requested", async (context) => {
  const { executor, service, running } = harness(context, "steer-unsupported", false);
  const { runId, binding } = await running("unsupported");

  const result = await service.execute(steerCommand("steer-unsupported-1", runId, "Follow up."));
  assert.deepEqual(result.response, { contractVersion: WORKER_COMMAND_VERSION, commandId: "steer-unsupported-1", status: "interrupt_requested", runId });
  assert.equal(executor.wasCancelRequested(binding), true, "the fallback must interrupt the turn exactly as cancel does");
  assert.deepEqual(executor.steers, [], "an undeclared steer must never be called");
});

test("steer: a Run the Worker has no record of is refused with cancel's code", async (context) => {
  const { executor, service } = harness(context, "steer-idle", true);
  assert.deepEqual(await refusal(service.execute(steerCommand("steer-idle-unknown", "run-never-existed", "Hello."))), await refusal(service.execute({ contractVersion: WORKER_COMMAND_VERSION, commandId: "cancel-idle-unknown", command: "cancel", target: { runId: "run-never-existed" } })));
  assert.deepEqual(executor.steers, []);
});

/**
 * A steer arriving after the Run ends receives interrupt_requested without calling the
 * executor. Router can queue its instruction as an ordinary continuation rather than
 * retain an ambiguous steer.
 */
test("steer: a Run that already succeeded is answered interrupt_requested, definitely, without calling the executor", async (context) => {
  const { executor, router, service, running } = harness(context, "steer-ended", true);
  const { ref, runId, binding } = await running("ended");
  executor.pushSucceeded(binding);
  executor.close(binding);
  assert.deepEqual(await settled(router, ref, 2), ["task.accepted", "task.succeeded"]);

  const result = await service.execute(steerCommand("steer-ended-1", runId, "Hello."));
  assert.deepEqual(result, { response: { contractVersion: WORKER_COMMAND_VERSION, commandId: "steer-ended-1", status: "interrupt_requested", runId }, replayed: false });
  assert.deepEqual(executor.steers, [], "the adapter's steer must not be called for a Run that has ended");
  assert.equal(executor.wasCancelRequested(binding), false);
  assert.deepEqual(await service.execute(steerCommand("steer-ended-1", runId, "Hello.")), { response: result.response, replayed: true });
  // Cancel answers the same ended Run with interrupt_requested.
  assert.deepEqual((await service.execute({ contractVersion: WORKER_COMMAND_VERSION, commandId: "cancel-ended-1", command: "cancel", target: { runId } })).response, { contractVersion: WORKER_COMMAND_VERSION, commandId: "cancel-ended-1", status: "interrupt_requested", runId });
  assert.equal(executor.wasCancelRequested(binding), false);
});

/**
 * For an executor without steer, a follow-up uses cancel or takes cancel's
 * path through steer. A Run that ended succeeded, failed, or waiting for input
 * answers `interrupt_requested` without calling the executor: nothing is running
 * to interrupt.
 */
for (const ending of ["succeeded", "failed", "waiting_for_input"] as const) {
  test(`cancel: a Run that already ended ${ending} is answered interrupt_requested, definitely, without calling the executor`, async (context) => {
    const { executor, router, service, running } = harness(context, `cancel-ended-${ending}`, false);
    const { ref, runId, binding } = await running(`ended-${ending}`);
    if (ending === "succeeded") executor.pushSucceeded(binding);
    else if (ending === "failed") executor.pushFailed(binding);
    else executor.pushWaiting(binding, elicitationFixture("c"));
    executor.close(binding);
    assert.deepEqual(await settled(router, ref, 2), ["task.accepted", ending === "succeeded" ? "task.succeeded" : ending === "failed" ? "task.failed" : "task.waiting_for_input"]);

    const cancel = { contractVersion: WORKER_COMMAND_VERSION, commandId: `cancel-ended-${ending}-1`, command: "cancel", target: { runId } } as const;
    const result = await service.execute(cancel);
    assert.deepEqual(result, { response: { contractVersion: WORKER_COMMAND_VERSION, commandId: cancel.commandId, status: "interrupt_requested", runId }, replayed: false });
    assert.equal(executor.wasCancelRequested(binding), false, "a Run that has ended is not interrupted");
    assert.deepEqual(await service.execute(cancel), { response: result.response, replayed: true });
    // A steer on an executor without steer takes cancel's path, and so answers the same.
    assert.deepEqual((await service.execute(steerCommand(`steer-ended-${ending}-1`, runId, "Hello."))).response, { contractVersion: WORKER_COMMAND_VERSION, commandId: `steer-ended-${ending}-1`, status: "interrupt_requested", runId });
    assert.deepEqual(executor.steers, []);
  });
}

test("steer: malformed steer commands are refused as malformed", async (context) => {
  const { service } = harness(context, "steer-malformed", true);
  const valid = steerCommand("steer-malformed-1", "run-1", "Hello.");
  for (const candidate of [
    { ...valid, contractVersion: "fictional-contract" },
    { ...valid, instruction: "   " },
    { ...valid, extra: true },
    { ...valid, target: { runId: "run-1", taskId: "task-1" } },
    (({ instruction: _instruction, ...rest }) => rest)(valid),
  ]) assert.deepEqual(await refusal(service.execute(candidate)), { code: "axis.worker.malformed", statusCode: 400 }, JSON.stringify(candidate));
});

test("validateRouterWorkerResponse: steered belongs to steer only, and steer also accepts the interrupt_requested fallback", async () => {
  const steer = { contractVersion: WORKER_COMMAND_VERSION, commandId: "command-1", command: "steer", target: { runId: "run-1" } } as const;
  const cancel = { ...steer, command: "cancel" } as const;
  const steered = { contractVersion: WORKER_COMMAND_VERSION, commandId: "command-1", status: "steered", runId: "run-1" };
  const interrupted = { ...steered, status: "interrupt_requested" };

  assert.deepEqual(await validateRouterWorkerResponse(steered, steer), steered);
  assert.deepEqual(await validateRouterWorkerResponse(interrupted, steer), interrupted);
  assert.deepEqual(await validateRouterWorkerResponse(interrupted, cancel), interrupted);
  await assert.rejects(validateRouterWorkerResponse(steered, cancel), RouterHttpTransportError);
  await assert.rejects(validateRouterWorkerResponse({ ...steered, runId: "run-2" }, steer), RouterHttpTransportError);
  await assert.rejects(validateRouterWorkerResponse({ ...steered, extra: true }, steer), RouterHttpTransportError);
  await assert.rejects(validateRouterWorkerResponse({ ...steered, commandId: "command-2" }, steer), RouterHttpTransportError);
});
