import { strict as assert } from "node:assert";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { EnvelopeCipher } from "../src/index.js";
import { channelBindingResolver } from "../src/router/binding-resolver.js";
import { Router } from "../src/router/router.js";
import { NodeDispatchDouble, nodeReservationFromEndpoint, type WorkerCommandDispatcher } from "./support/node-dispatch-double.js";
import { SqliteTeamBindingStore } from "../src/router/team-bindings.js";
import type { WorkerCommand, WorkerCommandResponse } from "../src/shared/router-worker.js";
import { currentFixture, FakeLinearProvider, masterKey, MemoryVault, SIGNING_SECRET } from "./helpers.js";
import { installTestLinearInstallation, linearChannelDatabases, startTestLinearChannel } from "./helpers/linear-channel.js";
import { acceptedEnvelope } from "./linear-loop-harness.js";
import { nodeFixtureTarget } from "./support/router-node-fixtures.js";

/**
 * Post-install distribution smoke for signed Linear webhook ingress through the real
 * composed channel into a Router command and Worker dispatch. Drive one ingress tick
 * directly for deterministic wiring validation.
 *
 * The Worker, Linear provider and credential vault are doubles. The scheduler, observe
 * loop and outbox delivery are outside this smoke's scope.
 */

const ORGANIZATION_ID = "organization-fixture";
const TEAM_ID = "team-fixture";
const APP_USER_ID = "app-user-fixture";
const OPERATOR_TOKEN = "smoke-operator-token";

/**
 * Accept Worker commands with the exact identity Router expects so the smoke reaches a
 * terminal Router outcome. Reuse the exported acceptedEnvelope builder for the accepted
 * lifecycle event.
 */
class SmokeWorkerPort implements WorkerCommandDispatcher {
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
      contractVersion: command.contractVersion,
      commandId: command.commandId,
      status: "accepted",
      identity: nodeFixtureTarget.identity,
      runAccepted: acceptedEnvelope(command.start, `smoke-accepted-${command.commandId}`),
    };
  }
}

const directory = await mkdtemp(join(tmpdir(), "axis-linear-smoke-"));
const databases = linearChannelDatabases(directory);
const bindings = new SqliteTeamBindingStore({ filename: join(directory, "router.sqlite") });
bindings.configure({
  scope: { organizationId: ORGANIZATION_ID, linearTeamId: TEAM_ID },
  expectedRevision: 0,
  profile: { ...nodeFixtureTarget.identity, endpointRef: nodeFixtureTarget.endpointRef },
  audit: { operatorRef: "smoke-operator", source: "smoke", requestRef: "smoke-request", authorizationRef: "smoke-authorization" },
});

const worker = new SmokeWorkerPort();
// Use the Node command route: nodeFixtureTarget is the Node's concrete Worker reservation.
const smokeNodeRef = { ref: "node-smoke-1", version: "v1" } as const;
const port = new NodeDispatchDouble({
  worker, nodeRef: smokeNodeRef,
  reservation: nodeReservationFromEndpoint(nodeFixtureTarget, "worker-key-smoke"),
});
const router = new Router({
  filename: join(directory, "router.sqlite"),
  nodeRouting: {
    bindingResolver: channelBindingResolver({ linearTeamBindings: bindings, nodeRoutes: [{ endpointRef: nodeFixtureTarget.endpointRef, nodeRef: smokeNodeRef }] }),
    port,
  },
});

const provider = new FakeLinearProvider();
const vault = new MemoryVault();
const linear = await startTestLinearChannel({
  databases,
  router,
  provider,
  vault,
  cipher: new EnvelopeCipher(masterKey()),
  signingSecret: SIGNING_SECRET,
  operatorToken: OPERATOR_TOKEN,
  teamIds: [TEAM_ID],
});

try {
  assert.equal((await fetch(new URL("/health/live", linear.http.url))).status, 200);
  // Answers from the composed channel's own `ready` getter: the vault plus
  // every store this composition opened.
  assert.equal((await fetch(new URL("/health/ready", linear.http.url))).status, 200);

  const credentialRef = await installTestLinearInstallation({ channel: linear.channel, vault, organizationId: ORGANIZATION_ID, appUserId: APP_USER_ID, teamIds: [TEAM_ID] });
  // The vault key production reads the credential back under. Asserted so the
  // fixture's key and `LinearOAuthManager`'s expectation cannot drift apart.
  assert.equal(credentialRef, `linear:${ORGANIZATION_ID}:${APP_USER_ID}`);

  const payload = currentFixture("agent-session-created.json");
  const accepted = await linear.post(payload, { deliveryId: "50000000-0000-4000-8000-000000000500" });
  assert.equal(accepted.status, 200);
  assert.deepEqual(accepted.body, { accepted: true, duplicate: false });
  assert.equal(linear.channel.inbox.snapshot("50000000-0000-4000-8000-000000000500")?.state, "accepted");

  // The same delivery again: accepted, and reported as a duplicate rather
  // than queued twice.
  const duplicate = await linear.post(payload, { deliveryId: "50000000-0000-4000-8000-000000000500" });
  assert.equal(duplicate.status, 200);
  assert.deepEqual(duplicate.body, { accepted: true, duplicate: true });

  const ingress = await linear.channel.ticks.ingress();
  assert.equal(ingress.claimed, "50000000-0000-4000-8000-000000000500");
  assert.equal(ingress.outcome.disposition, "dispatched");
  // Deliberately NOT `assert.deepEqual(ingress.failures, [])`. The ingress
  // loop adapter returns a frozen empty `failures` by construction (every
  // ingress failure is recorded on the inbox row before the tick returns), so
  // that assertion cannot fail and would read as coverage it is not. The
  // disposition above is where an ingress refusal actually shows up.

  // Assert that the real Router command reaches the Worker, beyond durable inbox
  // acceptance.
  assert.equal(worker.commands.length, 1);
  assert.equal(worker.commands[0]?.command, "start");

  const state = linear.snapshot();
  assert.equal(state.terminalDeliveries, 1);
  assert.equal(state.commands, 1);
  assert.equal(state.unsettledCommands, 0);
} finally {
  await linear.close();
  router.close();
  bindings.close();
  await rm(directory, { recursive: true, force: true });
}
