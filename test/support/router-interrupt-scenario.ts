import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { channelBindingResolver } from "../../src/router/binding-resolver.js";
import type { RouterNodePort } from "../../src/router/node-port.js";
import { SqliteTeamBindingStore } from "../../src/router/team-bindings.js";
import { Router, type RouterCommandResult } from "../../src/router/router.js";
import type { NodeCommandRequest } from "../../src/shared/router-node-commands.js";
import type { WorkerCommand, WorkerCommandResponse } from "../../src/shared/router-worker.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint } from "./node-dispatch-double.js";
import { workerSelectionFixture } from "./node-routing-fixtures.js";
import { buildNodeAcceptedEvent } from "./router-node-fixtures.js";

/**
 * One Linear follow-up on a still-running Run, through the real Router, with
 * every clock and id fixed so the commands Router emits are a byte-comparable record.
 *
 * The record contains each Node request (instruction included), each Worker
 * command the Node double completed, each result, and every column of
 * `router_node_command_state`. The fixture generator records the flag-off baseline.
 */
export interface RouterInterruptScenarioRecord {
  readonly requests: readonly NodeCommandRequest[];
  readonly workerCommands: readonly WorkerCommand[];
  readonly results: readonly RouterCommandResult[];
  readonly commandRows: readonly Record<string, unknown>[];
}

export async function runRouterInterruptScenario(options: { readonly nativeSteer?: boolean } = {}): Promise<RouterInterruptScenarioRecord> {
  const directory = mkdtempSync(join(tmpdir(), "axis-native-steer-interrupt-"));
  const organizationId = "organization-native-steer";
  const teamId = "team-native-steer";
  const endpointRef = { ref: "endpoint-native-steer", version: "v1" } as const;
  const nodeRef = { ref: "node-native-steer", version: "v1" } as const;
  const identity = { nodeId: "node-native-steer", workerId: "worker-native-steer", executorKind: "scripted", runtimeProfile: { ref: "executor-native-steer", version: "v1" } } as const;
  const teamBindings = new SqliteTeamBindingStore({});
  const requests: NodeCommandRequest[] = [], workerCommands: WorkerCommand[] = [], results: RouterCommandResult[] = [];
  let router: Router | undefined;
  try {
    teamBindings.configure({ scope: { organizationId, linearTeamId: teamId }, expectedRevision: 0, profile: { ...identity, endpointRef }, audit: { operatorRef: "operator", source: "test", requestRef: "request", authorizationRef: "authorization" } });
    const double = new NodeDispatchDouble({
      nodeRef,
      reservation: nodeReservationFromEndpoint({ endpointRef, identity, sessionBindingExecutorId: identity.runtimeProfile.ref }, "worker-key-native-steer"),
      worker: {
        async dispatch(command: WorkerCommand): Promise<WorkerCommandResponse> {
          workerCommands.push(structuredClone(command));
          if (command.command === "cancel") return { contractVersion: command.contractVersion, commandId: command.commandId, status: "interrupt_requested", runId: command.target.runId };
          if (command.command === "steer") return { contractVersion: command.contractVersion, commandId: command.commandId, status: "steered", runId: command.target.runId };
          return { contractVersion: command.contractVersion, commandId: command.commandId, status: "accepted", identity, runAccepted: buildNodeAcceptedEvent(command.start) } as WorkerCommandResponse;
        },
      },
    });
    const port: RouterNodePort = {
      dispatch: (request) => { requests.push(structuredClone(request)); return double.dispatch(request); },
      readReceipt: (query) => double.readReceipt(query),
    };
    let id = 0;
    router = new Router({
      filename: join(directory, "router.sqlite"),
      nodeRouting: {
        bindingResolver: channelBindingResolver({ linearTeamBindings: teamBindings, nodeRoutes: [{ endpointRef, nodeRef }] }),
        port,
        ...(options.nativeSteer === undefined ? {} : { nativeSteer: options.nativeSteer }),
      },
      now: () => new Date("2026-09-24T00:00:02.000Z"),
      id: () => `native-steer-id-${++id}`,
    });
    const ref = { organizationId, issueId: "issue-native-steer", teamId, agentSessionId: "session-native-steer" } as const;
    results.push(await router.startNode({ ...ref, commandId: "native-steer-start", instruction: "Do the work.", workerSelection: workerSelectionFixture() }));
    results.push(await router.continueNode({ ...ref, commandId: "native-steer-follow-up", instruction: "Also update the changelog." }));
    router.close(); router = undefined;
    const database = new DatabaseSync(join(directory, "router.sqlite"), { readOnly: true });
    try {
      const commandRows = (database.prepare("SELECT * FROM router_node_command_state ORDER BY rowid").all() as Record<string, unknown>[]).map((row) => ({ ...row }));
      return { requests, workerCommands, results, commandRows };
    } finally { database.close(); }
  } finally {
    router?.close();
    teamBindings.close();
    rmSync(directory, { recursive: true, force: true });
  }
}
