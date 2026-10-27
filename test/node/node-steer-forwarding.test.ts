import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { NodeCommandService } from "../../src/node/command.js";
import { SqliteNodeJournal } from "../../src/node/journal.js";
import type { NodeRuntimeWorkerConfig } from "../../src/node/runtime-config.js";
import { HttpNodeWorkerClient, NodeWorkerClientDispatcher } from "../../src/node/worker-client.js";
import { NodeContractError } from "../../src/shared/router-node.js";
import {
  NODE_COMMAND_CONTRACT_VERSION,
  canonicalNodeCommandIntent,
  nodeReceiptExpectationIntent,
  validateNodePriorRoute,
  validateNodeReceiptExpectation,
  validateNodeReceipt,
  type NodeCommandRequest,
  type NodePriorRoute,
  type NodeReceipt,
} from "../../src/shared/router-node-commands.js";
import { runtimeSourceSessionIdentity } from "../../src/shared/runtime-boundary.js";
import { workerFailureTuples, type WorkerCommand } from "../../src/shared/router-worker.js";
import { buildNodeAcceptedEvent, nodeFixtureRef, nodeFixtureTarget } from "../support/router-node-fixtures.js";
import { buildNodeStartRequest, nodeAuthenticatedContext, nodeReservation } from "../support/router-node-commands-fixtures.js";

/**
 * Forward steer to the Worker running the prior Run through a real
 * NodeCommandService, journal and HttpNodeWorkerClient, with a stub Worker over HTTP.
 * Receipt validators check each answer against its command.
 */

type SteerAnswer = "steered" | "interrupt_requested" | "malformed-refusal";

async function stubWorker(): Promise<{ readonly url: URL; readonly steers: WorkerCommand[]; answer: SteerAnswer; close(): Promise<void> }> {
  const state = { answer: "steered" as SteerAnswer, steers: [] as WorkerCommand[] };
  const reply = (response: ServerResponse, status: number, body: unknown) => {
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify(body));
  };
  const server: Server = createServer((request: IncomingMessage, response: ServerResponse) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      const command = JSON.parse(Buffer.concat(chunks).toString("utf8")) as WorkerCommand;
      if (command.command === "start") {
        reply(response, 202, { contractVersion: command.contractVersion, commandId: command.commandId, status: "accepted", identity: nodeFixtureTarget.identity, runAccepted: buildNodeAcceptedEvent(command.start) });
        return;
      }
      if (command.command !== "steer") { reply(response, 400, { error: { code: "axis.worker.malformed", message: workerFailureTuples["axis.worker.malformed"]!.message } }); return; }
      state.steers.push(command);
      // The stub returns the current fixed malformed refusal: HTTP
      // 400 is definite.
      if (state.answer === "malformed-refusal") reply(response, 400, { error: { code: "axis.worker.malformed", message: workerFailureTuples["axis.worker.malformed"]!.message } });
      else reply(response, 202, { contractVersion: command.contractVersion, commandId: command.commandId, status: state.answer, runId: command.target.runId });
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("stub Worker did not bind");
  return {
    url: new URL(`http://127.0.0.1:${address.port}`),
    steers: state.steers,
    get answer() { return state.answer; },
    set answer(value: SteerAnswer) { state.answer = value; },
    close: () => new Promise<void>((resolve) => { server.closeAllConnections(); server.close(() => resolve()); }),
  };
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

function steerRequest(start: NodeCommandRequest, commandId: string, instruction: string): NodeCommandRequest {
  if (start.command.command !== "start") throw new Error("start fixture required");
  return {
    contractVersion: NODE_COMMAND_CONTRACT_VERSION,
    nodeRef: start.nodeRef,
    target: start.target,
    command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId, command: "steer", target: { runId: start.command.start.identity.run.runId }, instruction },
  };
}

/** The prior route Router rebuilds from its own state: the start's route and the binding its accepted receipt carried. */
function priorRouteOf(start: NodeCommandRequest, receipt: NodeReceipt): NodePriorRoute {
  if (start.command.command !== "start" || receipt.status !== "accepted") throw new Error("accepted start required");
  const source = runtimeSourceSessionIdentity(start.command.start.identity.task.sourceRef);
  return validateNodePriorRoute({
    nodeRef: start.nodeRef, requestedSelection: start.target.selection, reservation: nodeReservation,
    sessionBinding: receipt.evidence.sessionBinding, sourceSystem: source.system, sourceSessionId: source.sessionId,
    sourceRef: start.command.start.identity.task.sourceRef, workspacePolicyRef: start.command.workspacePolicyRef,
  });
}

async function fixture(t: test.TestContext) {
  const directory = await mkdtemp(join(tmpdir(), "axis-node-steer-steer-"));
  const worker = await stubWorker();
  const journal = await SqliteNodeJournal.open(join(directory, "node.sqlite"));
  t.after(async () => { journal.close(); await worker.close(); await rm(directory, { recursive: true, force: true }); });
  const config: NodeRuntimeWorkerConfig = deepFreeze({
    nodeRef: nodeFixtureRef, nodeId: nodeFixtureTarget.identity.nodeId, target: nodeFixtureTarget, workerKey: nodeReservation.workerKey,
    workerEndpoint: worker.url.origin, commandToken: "node-steer-command-token", eventToken: "node-steer-event-token", readinessMode: "unsupported",
  });
  const client = new HttpNodeWorkerClient({ worker: config, workerDispatchTimeoutMs: 30_000 });
  const service = new NodeCommandService({
    authenticatedNode: nodeAuthenticatedContext, journal,
    resolveNewSessionReservation: () => nodeReservation,
    worker: new NodeWorkerClientDispatcher([{ target: nodeFixtureTarget, client }]),
  });
  const start = await buildNodeStartRequest();
  const started = await service.execute(start);
  assert.equal(started.status, "accepted");
  return { journal, worker, service, start, started, prior: priorRouteOf(start, started) };
}

test("a steer the Worker took is an accepted receipt answered `steered`, with the prior Run's binding", async (t) => {
  const { worker, service, start, started, prior } = await fixture(t);
  const receipt = await service.execute(steerRequest(start, "command-steer-1", "Also cover the error path."));
  assert.equal(receipt.status, "accepted");
  if (receipt.status !== "accepted" || started.status !== "accepted") return;
  assert.deepEqual(receipt.result, { contractVersion: "1.0.0", commandId: "command-steer-1", status: "steered", runId: prior.sessionBinding.runId });
  assert.deepEqual(receipt.evidence.sessionBinding, started.evidence.sessionBinding);
  // The Worker got exactly the steer: no start, no executionTarget.
  assert.deepEqual(worker.steers, [{ contractVersion: "1.0.0", commandId: "command-steer-1", command: "steer", target: { runId: prior.sessionBinding.runId }, instruction: "Also cover the error path." }]);
});

test("a steer the Worker fell back on is passed through as `interrupt_requested`", async (t) => {
  const { worker, service, start, prior } = await fixture(t);
  worker.answer = "interrupt_requested";
  const receipt = await service.execute(steerRequest(start, "command-steer-fallback", "Stop and summarize instead."));
  assert.equal(receipt.status, "accepted");
  if (receipt.status !== "accepted") return;
  assert.deepEqual(receipt.result, { contractVersion: "1.0.0", commandId: "command-steer-fallback", status: "interrupt_requested", runId: prior.sessionBinding.runId });
});

test("a malformed Worker refusal is definite and replay does not redispatch", async (t) => {
  const { worker, service, start } = await fixture(t);
  worker.answer = "malformed-refusal";
  const request = steerRequest(start, "command-steer-malformed-refusal", "Use the other fixture.");
  const receipt = await service.execute(request);
  assert.equal(receipt.status, "rejected");
  if (receipt.status !== "rejected") return;
  assert.deepEqual(receipt.failure, { code: "axis.worker.malformed", safeMessage: workerFailureTuples["axis.worker.malformed"]!.message, certainty: "definite" });
  assert.equal(worker.steers.length, 1);
  const replayed = await service.executeWithDisposition(request);
  assert.equal(replayed.replayed, true);
  assert.deepEqual(replayed.receipt, receipt);
  assert.equal(worker.steers.length, 1, "a recorded rejection is answered from the journal, not re-sent");
});

test("a receipt re-read rebuilt without the instruction validates to the same steer receipt", async (t) => {
  const { service, start, prior } = await fixture(t);
  const instruction = "Mention the migration in the summary.";
  const request = steerRequest(start, "command-steer-reread", instruction);
  const receipt = await service.execute(request);
  const query = { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: request.nodeRef, commandId: request.command.commandId } as const;
  const reread = await service.readReceipt(query);
  assert.deepEqual(reread, receipt);
  // What Router holds after a restart: no instruction, only its digest in the retained intent.
  const expectation = await validateNodeReceiptExpectation({
    expectation: "node-receipt", nodeRef: request.nodeRef, selection: request.target.selection,
    command: { contractVersion: NODE_COMMAND_CONTRACT_VERSION, commandId: request.command.commandId, command: "steer", target: { runId: prior.sessionBinding.runId } },
  }, prior);
  const instructionSha256 = `sha256:${createHash("sha256").update(instruction).digest("hex")}`;
  assert.deepEqual(nodeReceiptExpectationIntent(expectation, instructionSha256), canonicalNodeCommandIntent(request));
  const validated = await validateNodeReceipt(structuredClone(reread), { nodeRef: query.nodeRef, commandId: query.commandId }, { authenticatedNode: nodeAuthenticatedContext, expectation, priorRoute: prior });
  assert.deepEqual(validated, receipt);
});

/**
 * Steer and cancel remain permitted while draining; a fresh start receives the definite
 * axis.node.draining refusal before a command row is written.
 */
test("a draining Node forwards a steer, and still refuses a new start", async (t) => {
  const { journal, worker, service, start, prior } = await fixture(t);
  await journal.setMaintenanceMode("drain");
  const receipt = await service.execute(steerRequest(start, "command-steer-drain", "Keep going."));
  assert.equal(receipt.status, "accepted");
  if (receipt.status !== "accepted") return;
  assert.deepEqual(receipt.result, { contractVersion: "1.0.0", commandId: "command-steer-drain", status: "steered", runId: prior.sessionBinding.runId });
  assert.equal(worker.steers.length, 1);
  if (start.command.command !== "start") throw new Error("start fixture required");
  const another = { ...start, command: { ...start.command, commandId: "command-start-drain" } };
  await assert.rejects(service.execute(another), (error: unknown) => error instanceof NodeContractError && error.code === "axis.node.draining");
  const query = { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: start.nodeRef, commandId: "command-start-drain" } as const;
  assert.equal((await service.readReceipt(query)).status, "not_found");
});

/**
 * A fresh command receives the definite axis.node.draining refusal. A replay whose
 * reserved row may already be dispatching remains ambiguous with
 * axis.node.recovery_required; Router must not forget a command that still blocks the
 * Node's drain.
 */
test("a draining Node answers a replayed in-flight command recovery_required, not draining", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "axis-node-drain-drain-"));
  const worker = await stubWorker();
  const journal = await SqliteNodeJournal.open(join(directory, "node.sqlite"));
  t.after(async () => { journal.close(); await worker.close(); await rm(directory, { recursive: true, force: true }); });
  const client = new HttpNodeWorkerClient({ worker: deepFreeze({
    nodeRef: nodeFixtureRef, nodeId: nodeFixtureTarget.identity.nodeId, target: nodeFixtureTarget, workerKey: nodeReservation.workerKey,
    workerEndpoint: worker.url.origin, commandToken: "node-drain-command-token", eventToken: "node-drain-event-token", readinessMode: "unsupported",
  }) as NodeRuntimeWorkerConfig, workerDispatchTimeoutMs: 30_000 });
  const service = new NodeCommandService({
    authenticatedNode: nodeAuthenticatedContext, journal,
    resolveNewSessionReservation: () => nodeReservation,
    worker: new NodeWorkerClientDispatcher([{ target: nodeFixtureTarget, client }]),
  });
  const start = await buildNodeStartRequest();
  const claimed = await journal.claimCommandAndRoute({ request: start, resolveNewSessionReservation: () => nodeReservation, recordedAt: new Date().toISOString() });
  assert.equal(claimed.replayed, false);
  assert.equal(claimed.command.state, "reserved");
  await journal.setMaintenanceMode("drain");
  await assert.rejects(service.execute(start), (error: unknown) => error instanceof NodeContractError && error.code === "axis.node.recovery_required");
  assert.equal((await journal.maintenanceBlockerCounts()).reservedCommands, 1, "the reserved row is still the Node's own drain blocker");
});
