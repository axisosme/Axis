import test from "node:test";
import { LinearExecutionClient } from "../../src/shared/linear-execution-client.js";
import { AxisLinearSdk } from "../../src/shared/linear-sdk.js";
import { LinearExecutionAuthority, LinearExecutionCredentialEndpoint } from "../../src/relay/linear/execution-runtime.js";
import { LinearExecutionAccess } from "../../src/relay/linear/execution-access.js";
import { LinearInstallationStore } from "../../src/relay/linear/installation.js";
import { LinearApiProxy } from "../../src/relay/linear/api-proxy.js";
import { startRelayHttpService } from "../../src/relay/http.js";
import { createMemoryLinearInbox } from "../../src/relay/linear/inbox.js";
import { EnvelopeCipher } from "../../src/relay/security.js";
import { HEALTHY_DIAGNOSTICS_FIXTURE, masterKey, SIGNING_SECRET } from "../helpers.js";

export const identity = (suffix = "a") => ({ taskId: `task-${suffix}`, attemptId: `attempt-${suffix}`, runId: `run-${suffix}`, organizationId: `org-${suffix}` });
export const worker = (suffix = "a") => ({ nodeId: `node-${suffix}`, workerId: `worker-${suffix}`, executorKind: "codex", runtimeProfile: { ref: "codex", version: "v1" } });
export const receiverToken = (suffix = "a") => `receiver-${suffix}-${"x".repeat(32)}`;
export const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

export async function fixture(t: test.TestContext) {
  const installations = new LinearInstallationStore(":memory:");
  const current = new Map(["a", "b"].map(suffix => [identity(suffix).runId, { ...identity(suffix), sourceTeamId: `team-${suffix}` }]));
  const config = { grants: ["a", "b"].map(suffix => ({ organizationId: `org-${suffix}`, appUserId: `app-${suffix}`, scope: "installation-workspace" as const })),
    receivers: ["a", "b"].map(suffix => ({ bearerToken: receiverToken(suffix), identity: worker(suffix) })) };
  for (const suffix of ["a", "b"]) installations.install({ organizationId: `org-${suffix}`, appUserId: `app-${suffix}`, credentialRef: `vault-${suffix}`, teamIds: [`team-${suffix}`], canAccessAllPublicTeams: false });
  const authority = new LinearExecutionAuthority({ config, installations, router: { readLinearExecution: runId => current.get(runId) ?? null } });
  let now = Date.now();
  const access = new LinearExecutionAccess({ executions: authority, installations, now: () => new Date(now),
    credentials: { ready: true, accessToken: async ref => `oauth-${ref.slice(-1)}` },
    provider: { resolveViewer: async token => ({ organizationId: `org-${token.slice(-1)}`, appUserId: `app-${token.slice(-1)}` }) } });
  const calls: { token: string; body: any }[] = [];
  let upstream: typeof fetch = async (_url, init) => json({ data: { viewer: { id: `app-${new Headers(init?.headers).get("authorization")!.slice(-1)}`, name: "Axis fixture" } } });
  const audits: unknown[] = [];
  const proxy = new LinearApiProxy({ access, audit: event => audits.push(event), fetch: async (url, init) => {
    calls.push({ token: new Headers(init?.headers).get("authorization")!, body: JSON.parse(String(init?.body)) }); return upstream(url, init);
  } });
  const inbox = createMemoryLinearInbox();
  const service = await startRelayHttpService({ linear: { inbox, cipher: new EnvelopeCipher(masterKey()), signingSecret: SIGNING_SECRET, now: () => new Date() },
    ready: () => true, diagnostics: () => HEALTHY_DIAGNOSTICS_FIXTURE, oauth: { begin: () => "", callback: async () => "" }, operatorToken: "operator",
    linearApi: proxy, linearExecution: new LinearExecutionCredentialEndpoint({ authority, access, receivers: config.receivers }) });
  t.after(async () => { await service.close(); access.close(); authority.close(); installations.close(); inbox.close(); });
  const client = (suffix = "a") => new LinearExecutionClient({ relay: service.url, receiverToken: receiverToken(suffix) });
  const prepare = (suffix = "a") => authority.prepare(identity(suffix).runId, async () => worker(suffix));
  const sdk = async (suffix = "a") => {
    const issued = await client(suffix).issue(identity(suffix));
    return new AxisLinearSdk({ endpoint: new URL("/api/linear/graphql", service.url).href, credential: issued.credential, expiresAt: issued.expiresAt });
  };
  return { authority, access, config, installations, current, calls, audits, service, client, prepare, sdk,
    upstream: (value: typeof fetch) => { upstream = value; }, advance: (ms: number) => { now += ms; } };
}
