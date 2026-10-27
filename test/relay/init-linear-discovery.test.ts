import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { RelayError } from "../../src/relay/errors.js";
import { LinearInstallationStore, type LinearInstallationInput } from "../../src/relay/linear/installation.js";
import { LinearGraphQlProvider } from "../../src/relay/linear/provider.js";

const firstTeam = { id: "team-one", name: "Example Engineering", key: "ENG" };
const secondTeam = { id: "team-two", name: "Example Design", key: "DES" };

function page(nodes: readonly unknown[], hasNextPage = false, endCursor: string | null = null): unknown {
  return { data: { teams: { nodes, pageInfo: { hasNextPage, endCursor } } } };
}

function providerWithPages(pages: readonly unknown[]) {
  const requests: { after: string | null }[] = [];
  const provider = new LinearGraphQlProvider({
    endpoint: "https://example.com/graphql",
    fetch: async (url, init) => {
      assert.equal(url, "https://example.com/graphql");
      assert.equal(init?.method, "POST");
      assert.equal(new Headers(init?.headers).get("authorization"), "Bearer fixture-access-token");
      assert.equal(typeof init?.body, "string");
      const request = JSON.parse(init?.body as string) as { variables: { after: string | null } };
      const index = requests.length;
      requests.push(request.variables);
      assert.ok(index < pages.length, "Team discovery must stop after the supplied pages.");
      return Response.json(pages[index]);
    },
  });
  return { provider, requests };
}

test("team discovery follows cursors and returns only accessible team identities", async () => {
  const { provider, requests } = providerWithPages([
    page([{ ...firstTeam, description: "Excluded provider field" }], true, "cursor-one"),
    page([secondTeam], false, "cursor-two"),
  ]);
  assert.deepEqual(await provider.listTeams("fixture-access-token"), [firstTeam, secondTeam]);
  assert.deepEqual(requests, [{ after: null }, { after: "cursor-one" }]);
});

test("team discovery accepts a complete empty connection", async () => {
  const { provider, requests } = providerWithPages([page([])]);
  assert.deepEqual(await provider.listTeams("fixture-access-token"), []);
  assert.equal(requests.length, 1);
});

test("team discovery rejects malformed responses", async (t) => {
  const scenarios = [
    { name: "null envelope", response: null, code: "invalid_teams_shape" },
    { name: "missing connection", response: { data: {} }, code: "invalid_teams_shape" },
    { name: "missing nodes", response: { data: { teams: { pageInfo: { hasNextPage: false, endCursor: null } } } }, code: "invalid_teams_shape" },
    { name: "null node", response: page([null]), code: "invalid_team_shape" },
    { name: "non-string id", response: page([{ ...firstTeam, id: 12 }]), code: "invalid_team_shape" },
    { name: "blank id", response: page([{ ...firstTeam, id: " " }]), code: "invalid_team_shape" },
    { name: "blank name", response: page([{ ...firstTeam, name: " " }]), code: "invalid_team_shape" },
    { name: "missing key", response: page([{ id: firstTeam.id, name: firstTeam.name }]), code: "invalid_team_shape" },
    { name: "invalid page info", response: { data: { teams: { nodes: [], pageInfo: { hasNextPage: "false", endCursor: null } } } }, code: "invalid_teams_shape" },
    { name: "missing continuation cursor", response: page([firstTeam], true), code: "teams_incomplete" },
  ];
  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const { provider } = providerWithPages([scenario.response]);
      await assert.rejects(provider.listTeams("fixture-access-token"), {
        message: `linear.provider.${scenario.code}`,
      });
    });
  }
});

test("team discovery rejects cursor cycles instead of returning a partial list", async () => {
  const { provider, requests } = providerWithPages([
    page([firstTeam], true, "cursor-one"),
    page([secondTeam], true, "cursor-two"),
    page([], true, "cursor-one"),
  ]);
  await assert.rejects(provider.listTeams("fixture-access-token"), { message: "linear.provider.teams_incomplete" });
  assert.equal(requests.length, 3);
});

test("team discovery rejects duplicate team identities across pages", async () => {
  const { provider } = providerWithPages([
    page([firstTeam], true, "cursor-one"),
    page([{ ...firstTeam, name: "Conflicting name" }]),
  ]);
  await assert.rejects(provider.listTeams("fixture-access-token"), { message: "linear.provider.invalid_team_shape" });
});

test("team discovery rejects partial GraphQL results with a safe error", async () => {
  const { provider } = providerWithPages([
    page([firstTeam], true, "cursor-one"),
    {
      data: { teams: { nodes: [secondTeam], pageInfo: { hasNextPage: false, endCursor: null } } },
      errors: [{ message: "Provider rejected fixture-access-token", extensions: { code: "FORBIDDEN" } }],
    },
  ]);
  await assert.rejects(provider.listTeams("fixture-access-token"), { message: "linear.graphql.forbidden" });
});

function installationDatabase(t: test.TestContext) {
  const directory = mkdtempSync(fileURLToPath(new URL("../../.test-tmp-init-linear-", import.meta.url)));
  const filename = join(directory, "installations.sqlite");
  const stores = new Set<LinearInstallationStore>();
  t.after(() => {
    try {
      for (const store of stores) store.close();
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
  return {
    open() {
      const store = new LinearInstallationStore(filename, { now: () => new Date("2026-01-01T00:00:00.000Z") });
      stores.add(store);
      return store;
    },
    close(store: LinearInstallationStore) {
      store.close();
      stores.delete(store);
    },
  };
}

function installation(organizationId = "organization-one"): LinearInstallationInput {
  return {
    organizationId,
    appUserId: "app-user-one",
    credentialRef: `linear:${organizationId}:app-user-one`,
    teamIds: [],
    canAccessAllPublicTeams: false,
  };
}

test("active installation polling observes SQLite lifecycle writes without exposing credentials", (t) => {
  const database = installationDatabase(t);
  const writer = database.open();
  const reader = database.open();
  assert.deepEqual(reader.activeInstallations(), []);
  const first = installation();
  const second = installation("organization-two");
  writer.install(second);
  writer.install(first);
  const active = reader.activeInstallations();
  assert.deepEqual(active.map(({ revision, ...identity }) => {
    assert.ok(Number.isSafeInteger(revision) && revision > 0);
    return identity;
  }), [first, second].map(({ organizationId, appUserId, credentialRef }) => ({ organizationId, appUserId, credentialRef })));
  const audit = writer.auditEvents();
  assert.deepEqual(reader.activeInstallations(), active);
  assert.deepEqual(writer.auditEvents(), audit);

  assert.equal(writer.revoke(first.organizationId, "other-app-user"), null);
  assert.deepEqual(reader.activeInstallations(), active);
  assert.equal(writer.revoke(first.organizationId, first.appUserId), first.credentialRef);
  assert.deepEqual(reader.activeInstallations(), active.filter((entry) => entry.organizationId === second.organizationId));
  assert.equal(reader.authority(first.organizationId, first.appUserId, firstTeam.id), null);
});

test("a reopened store can narrow the same active installation with its observed revision", (t) => {
  const database = installationDatabase(t);
  const original = database.open();
  const input = installation();
  original.install(input);
  const snapshot = original.activeInstallations()[0]!;
  database.close(original);

  const reopened = database.open();
  assert.deepEqual(reopened.activeInstallations(), [snapshot]);
  reopened.install({ ...input, teamIds: [firstTeam.id] }, { expectedRevision: snapshot.revision });
  assert.deepEqual(reopened.authority(input.organizationId, input.appUserId, firstTeam.id), {
    credentialRef: input.credentialRef, allowed: true,
  });
  assert.deepEqual(reopened.authority(input.organizationId, input.appUserId, secondTeam.id), {
    credentialRef: input.credentialRef, allowed: false,
  });
  assert.ok(reopened.activeInstallations()[0]!.revision > snapshot.revision);
});

test("a stale guarded install cannot revive or overwrite changed installation state", async (t) => {
  const scenarios = [
    { name: "revoked", change: (store: LinearInstallationStore, input: LinearInstallationInput) => { store.revoke(input.organizationId, input.appUserId); } },
    { name: "reinstalled", change: (store: LinearInstallationStore, input: LinearInstallationInput) => { store.install(input); } },
    { name: "App User replaced", change: (store: LinearInstallationStore, input: LinearInstallationInput) => { store.install({ ...input, appUserId: "app-user-two" }); } },
    { name: "credential replaced", change: (store: LinearInstallationStore, input: LinearInstallationInput) => { store.install({ ...input, credentialRef: "linear:replacement" }); } },
    { name: "permissions changed", change: (store: LinearInstallationStore, input: LinearInstallationInput) => {
      store.applyPermissionChange({ ...input, addedTeamIds: [secondTeam.id], removedTeamIds: [] });
    } },
  ];
  for (const scenario of scenarios) {
    await t.test(scenario.name, (t) => {
      const database = installationDatabase(t);
      const original = database.open();
      const input = installation();
      original.install(input);
      const snapshot = original.activeInstallations()[0]!;
      scenario.change(original, input);
      database.close(original);

      const reopened = database.open();
      const current = reopened.activeInstallations();
      const audit = reopened.auditEvents();
      const authority = reopened.authority(input.organizationId, input.appUserId, secondTeam.id);
      assert.throws(() => reopened.install({ ...input, teamIds: [firstTeam.id] }, { expectedRevision: snapshot.revision }),
        (error) => error instanceof RelayError && error.message === "The Linear installation changed during setup.");
      assert.deepEqual(reopened.activeInstallations(), current);
      assert.deepEqual(reopened.auditEvents(), audit);
      assert.deepEqual(reopened.authority(input.organizationId, input.appUserId, secondTeam.id), authority);
    });
  }
});

test("a guarded install requires matching active identity even with the current revision", (t) => {
  const database = installationDatabase(t);
  const store = database.open();
  const input = installation();
  store.install(input);
  const snapshot = store.activeInstallations()[0]!;
  for (const candidate of [
    { ...input, organizationId: "organization-absent" },
    { ...input, appUserId: "app-user-two" },
    { ...input, credentialRef: "linear:replacement" },
  ]) {
    assert.throws(() => store.install(candidate, { expectedRevision: snapshot.revision }),
      (error) => error instanceof RelayError && error.message === "The Linear installation changed during setup.");
  }
  assert.deepEqual(store.activeInstallations(), [snapshot]);
  assert.equal(store.auditEvents().length, 1);
});
