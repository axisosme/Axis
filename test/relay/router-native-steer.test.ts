/**
 * What Router sends for a Linear follow-up on a still-running Run, with native steer off and on.
 *
 * Off is compared with the current Linear flag-off baseline generated through
 * `test/support/router-interrupt-scenario.ts`. On is checked against the real
 * Node receipt validator, which the Node double's receipts pass through inside Router.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { channelBindingResolver } from "../../src/router/binding-resolver.js";
import type { RouterNodePort } from "../../src/router/node-port.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { Router } from "../../src/router/router.js";
import type { NodeCommandRequest } from "../../src/shared/router-node-commands.js";
import type { WorkerCommand, WorkerCommandResponse } from "../../src/shared/router-worker.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint } from "../support/node-dispatch-double.js";
import { workerSelectionFixture } from "../support/node-routing-fixtures.js";
import { buildNodeAcceptedEvent } from "../support/router-node-fixtures.js";
import { runRouterInterruptScenario, type RouterInterruptScenarioRecord } from "../support/router-interrupt-scenario.js";

const golden = JSON.parse(readFileSync(new URL("../fixtures/router-interrupt/flag-off-golden.json", import.meta.url), "utf8")) as { readonly provenance: { readonly scenario: string; readonly nativeSteer: false }; readonly record: RouterInterruptScenarioRecord };

test("flag off: a Linear follow-up on a running Run matches the baseline cancel, intent and rows", async () => {
  assert.equal(golden.provenance.scenario, "test/support/router-interrupt-scenario.ts");
  assert.equal(golden.provenance.nativeSteer, false);
  assert.equal(golden.record.requests.at(-1)?.command.command, "cancel", "the golden must record the interrupt this test is about");
  for (const nativeSteer of [undefined, false] as const) {
    const record = await runRouterInterruptScenario(nativeSteer === undefined ? {} : { nativeSteer });
    assert.equal(JSON.stringify(record), JSON.stringify(golden.record), `nativeSteer ${String(nativeSteer)}: Router must send and retain the baseline cancel`);
  }
});

test("flag on: the follow-up is a steer carrying the instruction, retained with its digest, and answered steered", async () => {
  const record = await runRouterInterruptScenario({ nativeSteer: true });
  const offRequest = golden.record.requests.at(-1)!, request = record.requests.at(-1)!;
  assert.equal(request.command.command, "steer");
  if (request.command.command !== "steer" || offRequest.command.command !== "cancel") throw new Error("steer and cancel required");
  assert.deepEqual(request.command, { contractVersion: offRequest.command.contractVersion, commandId: offRequest.command.commandId, command: "steer", target: offRequest.command.target, instruction: "Also update the changelog." });
  assert.deepEqual({ ...request, command: undefined }, { ...offRequest, command: undefined }, "only the command itself differs from the flag-off request");
  assert.deepEqual(record.workerCommands.at(-1), { contractVersion: record.workerCommands.at(-1)!.contractVersion, commandId: offRequest.command.commandId, command: "steer", target: offRequest.command.target, instruction: "Also update the changelog." });
  assert.deepEqual(record.results.at(-1), { ...golden.record.results.at(-1)!, status: "steered" });

  const row = record.commandRows.at(-1)!, intent = JSON.parse(String(row.intent_json)) as { nodeCommand: { command: { command: string; instructionSha256: string } } };
  assert.equal(row.kind, "continue", "the row is still a continue: router_node_command_state needs no migration");
  assert.equal(row.state, "accepted");
  assert.equal(intent.nodeCommand.command.command, "steer");
  assert.equal(intent.nodeCommand.command.instructionSha256, `sha256:${createHash("sha256").update("Also update the changelog.").digest("hex")}`);
  assert.doesNotMatch(String(row.intent_json), /changelog/u, "the instruction itself is never retained");
  assert.deepEqual(record.commandRows.slice(0, -1), golden.record.commandRows.slice(0, -1), "the start is untouched by the flag");
});

/**
 * The flag is read when the row is claimed and never again. A steer whose Node response was lost is re-presented to
 * a Router restarted with the flag OFF: it must re-read the steer's receipt, not rebuild a cancel.
 */
test("the steer-or-cancel choice is fixed when the row is claimed: a replay after the flag flips re-reads the steer", async (context) => {
  const directory = mkdtempSync(join(tmpdir(), "axis-native-steer-flip-"));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const organizationId = "organization-native-steer-flip";
  const teamId = "team-native-steer-flip";
  const endpointRef = { ref: "endpoint-native-steer-flip", version: "v1" } as const, nodeRef = { ref: "node-native-steer-flip", version: "v1" } as const;
  const identity = { nodeId: "node-native-steer-flip", workerId: "worker-native-steer-flip", executorKind: "scripted", runtimeProfile: { ref: "executor-native-steer-flip", version: "v1" } } as const;
  const teamBindings = new SqliteTeamBindingStore({}); context.after(() => teamBindings.close());
  teamBindings.configure({ scope: { organizationId, linearTeamId: teamId }, expectedRevision: 0, profile: { ...identity, endpointRef }, audit: { operatorRef: "operator", source: "test", requestRef: "request", authorizationRef: "authorization" } });
  const workerCommands: WorkerCommand[] = [], requests: NodeCommandRequest[] = [];
  const double = new NodeDispatchDouble({
    nodeRef, reservation: nodeReservationFromEndpoint({ endpointRef, identity, sessionBindingExecutorId: identity.runtimeProfile.ref }, "worker-key-native-steer-flip"),
    worker: {
      async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> {
        workerCommands.push(command);
        if (command.command === "steer") return { contractVersion: command.contractVersion, commandId: command.commandId, status: "steered", runId: command.target.runId };
        if (command.command === "cancel") return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: command.target.runId };
        return { contractVersion: command.contractVersion, commandId: command.commandId, status: "accepted", identity, runAccepted: buildNodeAcceptedEvent(command.start) } as WorkerCommandResponse;
      },
    },
  });
  const port: RouterNodePort = { dispatch: (request) => { requests.push(request); return double.dispatch(request); }, readReceipt: (query) => double.readReceipt(query) };
  const open = (nativeSteer: boolean) => new Router({ filename: join(directory, "router.sqlite"), nodeRouting: { bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef, nodeRef }] }), port, nativeSteer } });
  const ref = { organizationId, issueId: "issue-native-steer-flip", teamId, agentSessionId: "session-native-steer-flip" } as const;
  const followUp = { ...ref, commandId: "native-steer-flip-follow-up", instruction: "Also update the changelog." };

  const on = open(true);
  const start = await on.startNode({ ...ref, commandId: "native-steer-flip-start", instruction: "Do the work.", workerSelection: workerSelectionFixture() });
  assert.equal(start.status, "accepted");
  if (start.status !== "accepted") throw new Error("accepted required");
  assert.deepEqual(on.readRunTarget({ ...ref, runId: start.runId }).execution, { status: "recorded", workerId: identity.workerId, executorId: identity.runtimeProfile.ref });
  double.loseNextResponse();
  await assert.rejects(on.continueNode(followUp), { code: "axis.router.recovery_required" });
  assert.equal(on.reconcileCommand(followUp.commandId), "unknown");
  on.close();
  assert.equal(workerCommands.at(-1)?.command, "steer", "the Worker already took the steer");

  const off = open(false); context.after(() => off.close());
  const replayed = await off.continueNode(followUp);
  assert.equal(replayed.status, "steered", "the replay reads the steer's own receipt");
  assert.equal(requests.length, 2, "and dispatches nothing new");
  assert.equal(workerCommands.length, 2);
  assert.equal(off.reconcileCommand(followUp.commandId), "steered");
  if (replayed.status !== "steered") throw new Error("steered required");
  assert.deepEqual(off.reconcileSteeredCommand(followUp.commandId), { taskId: replayed.taskId, runId: replayed.runId });
  assert.equal(off.reconcileInterruptedCommand(followUp.commandId), null);

  const database = new DatabaseSync(join(directory, "router.sqlite"), { readOnly: true });
  try {
    const executions = (database.prepare("SELECT COUNT(*) AS n FROM router_node_run_execution").get() as { n: number }).n;
    const runs = (database.prepare("SELECT COUNT(*) AS n FROM router_runs").get() as { n: number }).n;
    assert.deepEqual({ executions, runs }, { executions: 1, runs: 1 }, "a steer, like a cancel, writes no execution evidence and no Run");
  } finally { database.close(); }
});
