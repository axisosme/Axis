import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { closeSync, constants, fchmodSync, fstatSync, fsyncSync, ftruncateSync, lstatSync, openSync, readFileSync, realpathSync, statSync, writeFileSync, writeSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { stringify } from "smol-toml";

import { runAxisCli } from "./axis-cli.js";
import { databasePaths, deriveRelayConfiguration, initializeAxis, loadAxisConfiguration, parseAxisConfiguration, readAxisSecret, secretNames, type AxisConfiguration, type LinearSecrets } from "./configuration.js";
import { resolveCodexAgentProfileRegistry } from "./executor/codex.js";
import type { InitPrompts } from "./init-prompts.js";
import { LINEAR_OAUTH_SCOPES } from "./relay/linear/composition.js";
import { LinearInstallationStore } from "./relay/linear/installation.js";
import { LinearGraphQlProvider } from "./relay/linear/provider.js";
import type { LinearOAuthManager } from "./relay/oauth.js";
import { startRelay } from "./relay/runtime.js";
import { validateServeConfiguration } from "./serve.js";
import { packageLocalCodexExecutable } from "./worker/runtime.js";

export class InitError extends Error {}
export interface InitOptions { readonly filename?: string; readonly dataDir?: string; readonly envFile?: string; }
export interface InitRelay {
  readonly oauth: Pick<LinearOAuthManager, "begin" | "accessToken">;
  readonly installations: Pick<LinearInstallationStore, "activeInstallations">;
  close(): Promise<void>;
}
export interface InitDependencies {
  readonly provider: Pick<LinearGraphQlProvider, "listTeams" | "resolveViewer">;
  startRelay(config: AxisConfiguration, secrets: LinearSecrets): Promise<InitRelay>;
  checkExecutor(config: AxisConfiguration): Promise<readonly string[]>;
  checkService(config: AxisConfiguration): Promise<string>;
  readonly installationTimeoutMs?: number;
  readonly pollIntervalMs?: number;
}

function absent(filename: string): void {
  try { lstatSync(filename); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
  throw new InitError("Interactive setup requires new configuration and env files. Existing files were preserved; use the manual setup procedure for an existing installation.");
}
function createPrivate(filename: string, source: string): number {
  const fd = openSync(filename, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try { fchmodSync(fd, 0o600); writeFileSync(fd, source); fsyncSync(fd); return fd; }
  catch (error) { closeSync(fd); throw error; }
}
function safeToken(value: string): boolean { return value.length > 0 && value.length <= 4096 && !/[\s\x00-\x1f\x7f-\x9f"'\\]/u.test(value); }
// Single quotes keep dollar signs literal in both Compose and systemd files.
export function initEnvironmentFile(secrets: LinearSecrets): string {
  if (![secrets.clientSecret, secrets.webhookSecret].every(safeToken)) throw new InitError("Secrets must be single-line tokens without quotes, backslashes or control characters.");
  return `LINEAR_CLIENT_SECRET='${secrets.clientSecret}'\nLINEAR_WEBHOOK_SECRET='${secrets.webhookSecret}'\n`;
}
async function answer(prompts: InitPrompts, label: string, valid: (value: string) => boolean, secret = false): Promise<string> {
  for (;;) {
    prompts.signal?.throwIfAborted();
    const value = await prompts.ask(label, { secret });
    if (valid(value)) return value;
    prompts.write("Invalid value; please enter it again.");
  }
}
function publicOrigin(value: string): string | null {
  try {
    const url = new URL(value);
    if (url.protocol !== "https:" || url.username || url.password || url.pathname !== "/" || url.search || url.hash || /[\x00-\x20\x7f]/u.test(value)) return null;
    return url.origin;
  } catch { return null; }
}
function visible(value: string): string { return value.replace(/[\x00-\x1f\x7f-\x9f]/gu, ""); }
function shellQuote(value: string): string { return `'${value.replaceAll("'", "'\\''")}'`; }
function configurationSource(input: { dataDir: string; clientId: string; origin: string; organizationId: string; teamIds?: readonly string[]; kind?: string; workspace?: string; claudeExecutable?: string }): string {
  return stringify({ schemaVersion: 1, dataDir: input.dataDir,
    serve: { organizationId: input.organizationId, kind: input.kind ?? "codex", workspaceRoot: input.workspace ?? ".",
      ...(input.claudeExecutable === undefined ? {} : { claudeExecutable: input.claudeExecutable }) },
    linear: { clientId: input.clientId, redirectUri: `${input.origin}/oauth/linear/callback`, teamIds: [...input.teamIds ?? []], allowAllPublicTeams: false },
  });
}

/** The wizard supplies the existing audited CLI command's flags, never a second binding writer. */
export function bindInitTeam(config: AxisConfiguration, organizationId: string, teamId: string): void {
  const node = config.nodes[0]!, worker = node.workers[0]!;
  const code = runAxisCli(["--output", "json", "team-bindings", "set",
    "--database", databasePaths(config).router, "--organization", organizationId, "--team", teamId,
    "--profile", worker.runtimeProfile.ref, "--profile-revision", worker.runtimeProfile.version,
    "--node", node.id, "--worker", worker.id, "--executor", worker.kind,
    "--endpoint", worker.endpointRef, "--endpoint-revision", worker.endpointVersion,
    "--expected-revision", "absent", "--operator", "local-operator", "--authority", "interactive-setup",
    "--audit-source", "axis-init", "--request", randomUUID(),
  ], { stdout() {}, stderr() {} });
  if (code !== 0) throw new InitError("Team binding was refused. Existing bindings were preserved; inspect them with axis team-bindings before retrying.");
}

async function installed(relay: InitRelay, prompts: InitPrompts, dependencies: InitDependencies) {
  const deadline = Date.now() + (dependencies.installationTimeoutMs ?? 600_000);
  while (Date.now() < deadline) {
    prompts.signal?.throwIfAborted();
    const entries = relay.installations.activeInstallations();
    if (entries.length > 1) throw new InitError("More than one installation completed; inspect the installation store before continuing manually.");
    if (entries[0] !== undefined) return entries[0];
    await delay(dependencies.pollIntervalMs ?? 500, undefined, { ...(prompts.signal === undefined ? {} : { signal: prompts.signal }) });
  }
  throw new InitError("OAuth installation timed out. Created files were preserved; finish installation using the manual setup procedure.");
}

/** Fresh setup owns its config descriptor until finalization; existing files and keys are never replaced. */
export async function runInitWizard(options: InitOptions, prompts: InitPrompts, dependencies: InitDependencies = defaultInitDependencies): Promise<void> {
  const filename = resolve(options.filename ?? "axis.toml"), envFile = resolve(options.envFile ?? join(dirname(filename), "axis.env"));
  absent(filename); absent(envFile);
  if (filename === envFile) throw new InitError("Configuration and environment paths must differ.");
  let configFd: number | undefined, relay: InitRelay | undefined;
  try {
    const origin = publicOrigin(await answer(prompts, "Public HTTPS base URL (for example https://axis.example.com)", value => publicOrigin(value) !== null))!;
    prompts.write(`Create a Linear OAuth application in Settings > API > OAuth applications.\nName: Axis\nCallback URL: ${origin}/oauth/linear/callback\nWebhook URL: ${origin}/webhooks/linear\nEnable the agent application with assignable and mentionable capabilities.\nWebhook event categories: AgentSessionEvent (created, prompted), PermissionChange (teamAccessChanged), OAuthApp (revoked).\nRequested OAuth scopes: ${LINEAR_OAUTH_SCOPES.join(", ")}\nForward the public HTTPS origin to this host's Relay on 127.0.0.1:8787 before continuing.\nDo not send tasks until setup is complete.`);
    const clientId = await answer(prompts, "Client ID", value => /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(value));
    const secrets = {
      clientSecret: await answer(prompts, "Client secret (token without quotes or backslashes)", safeToken, true),
      webhookSecret: await answer(prompts, "Webhook signing secret (token without quotes or backslashes)", safeToken, true),
    };
    const input = { dataDir: options.dataDir ?? "data", clientId, origin, organizationId: "setup-pending" };
    const provisional = configurationSource(input);
    // Reject invalid paths/topology before creating any private material.
    parseAxisConfiguration(provisional, filename);
    configFd = createPrivate(filename, provisional);
    const config = initializeAxis({ filename });
    const envFd = createPrivate(envFile, initEnvironmentFile(secrets)); closeSync(envFd);
    relay = await dependencies.startRelay(config, secrets);
    if (relay.installations.activeInstallations().length !== 0) throw new InitError("An installation already exists; complete setup manually with the existing state.");
    prompts.write(`Open this URL to install Axis in your Linear workspace:\n${relay.oauth.begin()}\nWaiting up to ten minutes for OAuth installation. Cancel with Ctrl-C.`);
    const installation = await installed(relay, prompts, dependencies);
    const token = await relay.oauth.accessToken(installation.credentialRef);
    if (token === null) throw new InitError("The installation credential is unavailable; complete setup manually.");
    const viewer = await dependencies.provider.resolveViewer(token);
    if (viewer.organizationId !== installation.organizationId || viewer.appUserId !== installation.appUserId) throw new InitError("The installation identity changed; complete setup manually.");
    const teams = await dependencies.provider.listTeams(token);
    if (teams.length === 0) throw new InitError("The installed application cannot access any Teams. Review its Linear permissions and finish setup manually.");
    teams.forEach((team, index) => prompts.write(`${index + 1}. ${visible(team.name)} (${visible(team.key)})`));
    const selected = Number(await answer(prompts, "Choose a Team number", value => /^[1-9][0-9]*$/u.test(value) && Number(value) <= teams.length)) - 1;
    const team = teams[selected]!;
    const kind = await answer(prompts, "Executor (1 = Codex, 2 = Claude Code)", value => value === "1" || value === "2");
    const workspace = await answer(prompts, "Absolute workspace directory (must exist)", value => {
      try { return isAbsolute(value) && statSync(value).isDirectory() && !/[\x00-\x1f\x7f]/u.test(value); } catch { return false; }
    });
    const claudeExecutable = kind === "2" ? await answer(prompts, "Absolute Claude Code executable", value => {
      try { return isAbsolute(value) && statSync(realpathSync(value)).isFile() && !/[\x00-\x1f\x7f]/u.test(value); } catch { return false; }
    }) : undefined;
    const source = configurationSource({ ...input, organizationId: installation.organizationId, teamIds: [team.id], kind: kind === "1" ? "codex" : "claude-code", workspace,
      ...(claudeExecutable === undefined ? {} : { claudeExecutable }) });
    const finalConfig = parseAxisConfiguration(source, filename);
    validateServeConfiguration(finalConfig);
    await relay.close(); relay = undefined;
    const store = new LinearInstallationStore(databasePaths(finalConfig).channel);
    try { store.install({ organizationId: installation.organizationId, appUserId: installation.appUserId, credentialRef: installation.credentialRef,
      teamIds: [team.id], canAccessAllPublicTeams: false }, { expectedRevision: installation.revision }); }
    finally { store.close(); }
    const file = lstatSync(filename), owned = fstatSync(configFd);
    if (!file.isFile() || file.ino !== owned.ino || file.dev !== owned.dev || readFileSync(filename, "utf8") !== provisional) throw new InitError("The configuration changed during setup and was preserved. Complete setup manually.");
    ftruncateSync(configFd, 0); writeSync(configFd, source, 0, "utf8"); fsyncSync(configFd);
    bindInitTeam(finalConfig, installation.organizationId, team.id);
    const checked = loadAxisConfiguration(filename); validateServeConfiguration(checked);
    for (const name of secretNames(checked)) readAxisSecret(checked, name);
    prompts.write("Health: configuration valid; private keys present with mode 0600; Linear installation present; Team binding audited and verified.");
    for (const message of await dependencies.checkExecutor(checked)) prompts.write(`Health: ${message}`);
    prompts.write(`Health: ${await dependencies.checkService(checked)}`);
    prompts.write(`Next command:\nnode --env-file=${shellQuote(envFile)} ${shellQuote(fileURLToPath(new URL("./bin/axis.js", import.meta.url)))} serve --config ${shellQuote(filename)}`);
    prompts.write("Setup files are ready. Resolve any health findings before admitting tasks; see docs/DEPLOYMENT.md for executor profiles and service setup.");
  } finally {
    try { await relay?.close(); } finally { if (configFd !== undefined) closeSync(configFd); }
  }
}

function probe(command: string, args: readonly string[], environment: Readonly<Record<string, string | undefined>>): Promise<boolean> {
  return new Promise(resolve => execFile(command, [...args], { env: environment, timeout: 5_000, killSignal: "SIGKILL", maxBuffer: 64 * 1024 }, error => resolve(error === null)));
}
export async function checkInitExecutor(config: AxisConfiguration, environment: Readonly<Record<string, string | undefined>> = process.env, check: typeof probe = probe): Promise<readonly string[]> {
  const worker = config.nodes[0]!.workers[0]!;
  const nativeEnvironment: Readonly<Record<string, string | undefined>> = { ...environment, ...(worker.codexHome === undefined ? {} : { CODEX_HOME: worker.codexHome }) };
  const codexHome = nativeEnvironment.CODEX_HOME ?? (nativeEnvironment.HOME === undefined ? undefined : join(nativeEnvironment.HOME, ".codex"));
  const command = worker.kind === "codex" ? process.execPath : worker.claudeExecutable!;
  const prefix = worker.kind === "codex" ? [packageLocalCodexExecutable()] : [];
  if (!await check(command, [...prefix, "--version"], nativeEnvironment)) return ["executor unavailable; install the configured executor."];
  const authenticated = await check(command, [...prefix, ...(worker.kind === "codex" ? ["login", "status"] : ["auth", "status", "--json"])], nativeEnvironment);
  const result = [authenticated ? "executor available and native login status succeeded (no model request performed)." : "executor available; native authentication required before serve."];
  if (worker.kind === "codex") {
    const profiles = resolveCodexAgentProfileRegistry(codexHome === undefined ? {} : { codexHome });
    result.push(Object.values(profiles.sourceAvailability).every(value => value === "available") ? "Codex role profiles valid." : "Codex role profiles incomplete; provision all four profiles before serve.");
  }
  return result;
}
export const defaultInitDependencies: InitDependencies = {
  provider: new LinearGraphQlProvider(),
  async startRelay(config, secrets) {
    const runtime = await startRelay(deriveRelayConfiguration(config, undefined, secrets));
    return { oauth: runtime.channel.oauth, installations: runtime.channel.installations, close: () => runtime.close() };
  },
  checkExecutor: checkInitExecutor,
  async checkService(config) {
    try {
      const response = await fetch(new URL("/health/ready", config.relay.url), { redirect: "error", signal: AbortSignal.timeout(2_000) });
      await response.body?.cancel();
      return response.status === 200 ? "running service is reachable and ready." : "service is reachable but not ready; inspect its health endpoint.";
    } catch { return "service not reachable; the temporary setup Relay has stopped."; }
  },
};
