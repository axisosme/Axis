import { randomBytes } from "node:crypto";
import { isIP } from "node:net";
import { closeSync, constants, fchmodSync, fstatSync, lstatSync, mkdirSync, openSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { parse } from "smol-toml";

import { NODE_CONTRACT_VERSION, validateNodeTarget } from "./shared/router-node.js";
import { isExecutorLabel, validateWorkerKey, validateWorkerSelectionScope, type ExecutorLabel } from "./shared/worker-selection.js";
import { numericLoopback } from "./node/http-transport.js";
import type { NodeRuntimeConfig } from "./node/runtime-config.js";
import type { RelayRuntimeConfig } from "./relay/runtime-config.js";
import type { RouterNodeEventHttpBinding } from "./router/http.js";
import type { WorkerRuntimeConfig } from "./worker/runtime-config.js";
import type { WorkerWorkspaceConfiguration } from "./shared/router-worker.js";
import type { LinearExecutionConsent, LinearExecutionRuntimeConfig } from "./relay/linear/execution-runtime.js";
import type { LinearCliGrant } from "./relay/linear/cli-access.js";
import type { AgentInstructionOverrides } from "./executor/response-guidance.js";

import { DEFAULT_INTEGRITY_CHECK_INTERVAL_MS } from "./relay/integrity-cadence.js";
import { LINEAR_DEFAULT_TICK_INTERVAL_MS, type LinearRuntimeConfig } from "./relay/linear/runtime-config.js";
import type { LinearApiRuntimeConfig } from "./relay/linear/api-runtime.js";
import { linearRelayUrl } from "./shared/linear-execution-client.js";
import { validateWorkerWorkspaceConfigurations } from "./worker/workspace-policy.js";

type Table = Record<string, unknown>;
type Validator = (value: unknown, path: string) => unknown;

export class AxisConfigurationError extends Error {
  constructor(readonly field: string, reason = "invalid") {
    super(`axis.toml ${field} is ${reason}.`);
    this.name = "AxisConfigurationError";
  }
}
function fail(path: string, reason?: string): never { throw new AxisConfigurationError(path, reason); }
const text: Validator = (value, path) => typeof value === "string" && value.trim().length > 0 && !value.includes("\0") ? value : fail(path);
const id: Validator = (value, path) => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(value) ? value : fail(path);
const boolean: Validator = (value, path) => typeof value === "boolean" ? value : fail(path);
const integer = (minimum: number, maximum = Number.MAX_SAFE_INTEGER): Validator => (value, path) =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= minimum && value <= maximum ? value : fail(path);
const port = integer(1, 65_535);
function choice(...values: string[]): Validator { return (value, path) => typeof value === "string" && values.includes(value) ? value : fail(path); }
function list(item: Validator): Validator { return (value, path) => Array.isArray(value) ? value.map((entry, i) => item(entry, `${path}[${i}]`)) : fail(path); }
function table(fields: Record<string, Validator>, required: readonly string[] = []): Validator {
  return (value, path) => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) fail(path);
    const input = value as Table;
    if (Object.keys(input).some(key => !Object.hasOwn(fields, key))) fail(path, "invalid (unknown key)");
    for (const key of required) if (!Object.hasOwn(input, key)) fail(`${path}.${key}`, "required");
    return Object.fromEntries(Object.entries(input).map(([key, entry]) => [key, fields[key]!(entry, `${path}.${key}`)]));
  };
}
const reference = table({ ref: id, version: id }, ["ref", "version"]);
const workspace = table({ organizationId: id, workspaceRoot: text, workspaceCatalog: list(table({ workspacePolicyRef: reference, workspaceRoot: text }, ["workspacePolicyRef", "workspaceRoot"])) }, ["organizationId", "workspaceRoot"]);
const scopeRule = table({ scope: table({ system: choice("linear"), tenantId: text, scopeId: text }, ["system", "tenantId", "scopeId"]), workerKey: id }, ["scope", "workerKey"]);
const workerSchema = table({
  id, key: id, kind: choice("codex", "claude-code"), executorId: id, version: id, endpointRef: id, endpointVersion: id,
  runtimeProfile: reference, host: text, port, labels: list((value, path) => isExecutorLabel(value) ? value : fail(path)), hostRoot: text,
  workspaces: list(workspace), profileFallbackPolicy: choice("allow", "deny"), codexHome: text, claudeExecutable: text,
  linearExecution: boolean, instructions: table({ sessionResponse: text, omitSessionResponse: boolean }),
}, ["id", "kind", "port", "workspaces"]);
const nodeSchema = table({
  id, ref: id, version: id, url: text, host: text, port, eventHost: text, eventPort: port, defaultWorker: id,
  workerScopeRules: list(scopeRule), workers: list(workerSchema), workerDispatchTimeoutMs: integer(1_000, 120_000),
  routerEventTimeoutMs: integer(1_000, 60_000), workerReadinessTimeoutMs: integer(500, 30_000),
  eventRetryInitialDelayMs: integer(100, 60_000), eventRetryMaxDelayMs: integer(1_000, 300_000), eventMaxConcurrentRuns: integer(2, 64),
}, ["id", "port", "eventPort", "workers"]);
const serveSchema = table({
  kind: choice("codex", "claude-code"), organizationId: id, workspaceRoot: text,
  claudeExecutable: text, codexHome: text, profileFallbackPolicy: choice("allow", "deny"),
}, ["organizationId"]);
const rootSchema = table({
  schemaVersion: (value, path) => value === 1 ? 1 : fail(path), dataDir: text,
  host: table({ node: id, worker: id }),
  relay: table({ host: text, port, url: text }),
  router: table({ host: text, port, url: text, defaultWorker: id, defaultExcludedTeamIds: list(id), nativeSteer: boolean }, ["port"]),
  linear: table({ clientId: text, redirectUri: text, teamIds: list(id), allowAllPublicTeams: boolean,
    ingressTickIntervalMs: integer(1), observeTickIntervalMs: integer(1), outboxTickIntervalMs: integer(1), integrityCheckIntervalMs: integer(1),
    apiEnabled: boolean, apiTimeoutMs: integer(1, 120_000),
    executionGrants: list(table({ organizationId: text, appUserId: text, scope: choice("installation-workspace") }, ["organizationId", "appUserId", "scope"])),
    cliGrants: list(table({ credentialId: text, credentialHash: text, organizationId: text, appUserId: text, expiresAt: text }, ["credentialId", "credentialHash", "organizationId", "appUserId", "expiresAt"])),
  }, ["clientId", "redirectUri"]),
  nodes: list(nodeSchema),
  serve: serveSchema,
}, ["schemaVersion", "dataDir"]);

export interface AxisWorker {
  readonly id: string; readonly key: string; readonly kind: "codex" | "claude-code"; readonly executorId: string; readonly version: string;
  readonly endpointRef: string; readonly endpointVersion: string; readonly runtimeProfile: { readonly ref: string; readonly version: string };
  readonly host: string; readonly port: number; readonly labels: readonly ExecutorLabel[]; readonly hostRoot?: string;
  readonly workspaces: readonly WorkerWorkspaceConfiguration[]; readonly profileFallbackPolicy: "allow" | "deny"; readonly codexHome?: string;
  readonly claudeExecutable?: string; readonly linearExecution: boolean; readonly instructions?: { readonly sessionResponse?: string; readonly omitSessionResponse?: boolean };
}
export interface AxisNode {
  readonly id: string; readonly ref: string; readonly version: string; readonly url: string; readonly host: string;
  readonly port: number; readonly eventHost: string; readonly eventPort: number; readonly workers: readonly AxisWorker[];
  readonly defaultWorker?: string; readonly workerScopeRules: readonly NodeRuntimeConfig["workerScopeRules"][number][];
  readonly workerDispatchTimeoutMs?: number; readonly routerEventTimeoutMs?: number; readonly workerReadinessTimeoutMs?: number;
  readonly eventRetryInitialDelayMs?: number; readonly eventRetryMaxDelayMs?: number; readonly eventMaxConcurrentRuns?: number;
}
export interface AxisConfiguration {
  readonly schemaVersion: 1; readonly filename: string; readonly dataDir: string;
  readonly host: { readonly node?: string; readonly worker?: string };
  readonly relay: { readonly host: string; readonly port: number; readonly url: string };
  readonly router: { readonly host: string; readonly port: number; readonly url: string; readonly defaultWorker?: string; readonly defaultExcludedTeamIds: readonly string[]; readonly nativeSteer: boolean };
  readonly linear?: { readonly clientId: string; readonly redirectUri: string; readonly teamIds?: readonly string[]; readonly allowAllPublicTeams?: boolean;
    readonly ingressTickIntervalMs?: number; readonly observeTickIntervalMs?: number; readonly outboxTickIntervalMs?: number; readonly integrityCheckIntervalMs?: number;
    readonly apiEnabled?: boolean; readonly apiTimeoutMs?: number; readonly executionGrants?: readonly LinearExecutionConsent[]; readonly cliGrants?: readonly LinearCliGrant[] };
  readonly nodes: readonly AxisNode[];
}
function origin(host: string, port: number): string { return `http://${host.includes(":") ? `[${host}]` : host}:${port}/`; }
function url(value: string, path: string): string {
  try {
    const result = new URL(value);
    if (!["http:", "https:"].includes(result.protocol) || result.username || result.password || result.pathname !== "/" || result.search || result.hash) fail(path);
    if (result.protocol === "http:") {
      const hostname = result.hostname.replace(/^\[|\]$/gu, "");
      const authority = value.slice("http://".length).split(/[/?#]/u, 1)[0];
      if (!numericLoopback(hostname) || authority !== `${result.hostname}${result.port ? `:${result.port}` : ""}` && authority !== `${result.hostname}:80`) fail(path);
    }
    return result.href;
  } catch { fail(path); }
}
function freeze<T>(value: T): T {
  if (value !== null && typeof value === "object") { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
  return value;
}

/** Strict parsing is side-effect free. All relative paths are anchored to this file. */
export function parseAxisConfiguration(source: string, filename = resolve("axis.toml")): AxisConfiguration {
  let raw: unknown;
  try { raw = parse(source); } catch { fail("document", "invalid (TOML syntax)"); }
  const root = rootSchema(raw, "configuration") as Table;
  if (root.serve !== undefined) {
    if (root.nodes !== undefined || root.router !== undefined || root.host !== undefined) fail("serve", "incompatible with explicit topology");
    const { organizationId, workspaceRoot = ".", ...options } = root.serve as Table;
    root.router = { port: 8788, defaultWorker: "local-worker" };
    root.nodes = [{ id: "local", port: 9080, eventPort: 9081, workers: [{
      id: "local-worker", kind: "codex", port: 9082, ...options,
      workspaces: [{ organizationId, workspaceRoot }],
    }] }];
  } else {
    if (root.router === undefined) fail("configuration.router", "required");
    if (root.nodes === undefined) fail("configuration.nodes", "required");
  }
  const base = dirname(resolve(filename));
  const localPath = (value: string): string => resolve(base, value);
  const relay = { host: "127.0.0.1", port: 8787, ...(root.relay as Table | undefined) } as AxisConfiguration["relay"];
  const router = { host: "127.0.0.1", defaultExcludedTeamIds: [], nativeSteer: false, ...(root.router as Table) } as unknown as AxisConfiguration["router"];
  const nodeIds = new Set<string>(), refs = new Set<string>(), workerIds = new Set<string>(), targetRefs = new Set<string>();
  const nodes = (root.nodes as Table[]).map((entry): AxisNode => {
    const node = { host: "127.0.0.1", eventHost: "127.0.0.1", ref: entry.id, version: "v1", workerScopeRules: [], ...entry } as unknown as AxisNode;
    if (nodeIds.has(node.id) || refs.has(`${node.ref}\0${node.version}`)) fail("nodes", "invalid (duplicate identity)");
    nodeIds.add(node.id); refs.add(`${node.ref}\0${node.version}`);
    const ports = new Set([node.port, node.eventPort]);
    if (ports.size !== 2) fail("nodes.eventPort");
    const workers = (entry.workers as Table[]).map((item): AxisWorker => {
      const worker = { host: "127.0.0.1", key: item.id, executorId: item.id, version: "v1", endpointRef: item.id, endpointVersion: "v1", profileFallbackPolicy: "allow", linearExecution: false, labels: [item.kind === "codex" ? "codex" : "claude"], ...item } as unknown as AxisWorker;
      if (workerIds.has(worker.id) || targetRefs.has(`${worker.endpointRef}\0${worker.endpointVersion}`) || ports.has(worker.port)) fail("nodes.workers", "invalid (duplicate identity or port)");
      workerIds.add(worker.id); targetRefs.add(`${worker.endpointRef}\0${worker.endpointVersion}`); ports.add(worker.port);
      if (worker.instructions?.sessionResponse !== undefined && worker.instructions.omitSessionResponse === true) fail("nodes.workers.instructions");
      const workspaces = (item.workspaces as Table[]).map(workspace => ({ ...workspace, workspaceRoot: localPath(workspace.workspaceRoot as string),
        ...(workspace.workspaceCatalog === undefined ? {} : { workspaceCatalog: (workspace.workspaceCatalog as Table[]).map(entry => ({ ...entry, workspaceRoot: localPath(entry.workspaceRoot as string) })) }) })) as unknown as readonly WorkerWorkspaceConfiguration[];
      return { ...worker, runtimeProfile: worker.runtimeProfile ?? { ref: worker.executorId, version: worker.version }, workspaces,
        ...(worker.hostRoot === undefined ? {} : { hostRoot: localPath(worker.hostRoot) }),
        ...(worker.codexHome === undefined ? {} : { codexHome: localPath(worker.codexHome) }),
        ...(worker.claudeExecutable === undefined ? {} : { claudeExecutable: localPath(worker.claudeExecutable) }) };
    });
    return { ...node, url: url(node.url ?? origin(node.host, node.port), "nodes.url"), workers };
  });
  if (nodes.length === 0) fail("nodes", "required");
  if (router.defaultWorker !== undefined && !workerIds.has(router.defaultWorker)) fail("router.defaultWorker");
  const config: AxisConfiguration = {
    schemaVersion: 1, filename: resolve(filename), dataDir: localPath(root.dataDir as string), host: (root.host ?? {}) as AxisConfiguration["host"],
    relay: { ...relay, url: url(relay.url ?? origin(relay.host, relay.port), "relay.url") },
    router: { ...router, url: url(router.url ?? origin(router.host, router.port), "router.url") }, nodes,
    ...(root.linear === undefined ? {} : { linear: root.linear as AxisConfiguration["linear"] & {} }),
  };
  if (config.dataDir === "/") fail("dataDir");
  validateDeclarations(config);
  if (config.host.node !== undefined || config.host.worker !== undefined) selectedNode(config);
  if (config.host.worker !== undefined) selectedWorker(config);
  return freeze(config);
}
export function loadAxisConfiguration(filename = resolve("axis.toml")): AxisConfiguration {
  let source: string;
  try { source = readFileSync(filename, "utf8"); } catch { return fail("document", "unreadable"); }
  return parseAxisConfiguration(source, filename);
}
export function selectedNode(config: AxisConfiguration, nodeId = config.host.node): AxisNode {
  const node = nodeId === undefined && config.nodes.length === 1 ? config.nodes[0] : config.nodes.find(entry => entry.id === nodeId);
  return node ?? fail("host.node", "required or unknown");
}
export function selectedWorker(config: AxisConfiguration, workerId = config.host.worker, nodeId = config.host.node): { readonly node: AxisNode; readonly worker: AxisWorker } {
  const node = selectedNode(config, nodeId);
  const worker = workerId === undefined && node.workers.length === 1 ? node.workers[0] : node.workers.find(entry => entry.id === workerId);
  return worker === undefined ? fail("host.worker", "required or unknown") : { node, worker };
}
export function databasePaths(config: AxisConfiguration) {
  return Object.freeze({ router: join(config.dataDir, "router.sqlite"), channel: join(config.dataDir, "linear-channel.sqlite"),
    conversations: join(config.dataDir, "linear-conversations.sqlite"), runChain: join(config.dataDir, "linear-run-chain.sqlite"), delivery: join(config.dataDir, "linear-delivery.sqlite"),
    vault: join(config.dataDir, "relay-vault.json") });
}
export type SecretReader = (name: string) => string;
export function secretPath(config: AxisConfiguration, name: string): string {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(name)) fail("secrets");
  return join(config.dataDir, "secrets", `${name}.key`);
}
export function readAxisSecret(config: AxisConfiguration, name: string): string {
  let fd: number | undefined;
  try {
    const directory = join(config.dataDir, "secrets");
    const data = lstatSync(config.dataDir);
    if (!data.isDirectory() || data.isSymbolicLink() || data.uid !== process.getuid?.() || (data.mode & 0o077) !== 0) fail("dataDir", "unsafe");
    const dir = lstatSync(directory);
    if (!dir.isDirectory() || dir.isSymbolicLink() || (dir.mode & 0o077) !== 0 || dir.uid !== process.getuid?.()) fail("secrets", "unsafe");
    fd = openSync(secretPath(config, name), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const metadata = fstatSync(fd);
    if (!metadata.isFile() || (metadata.mode & 0o777) !== 0o600 || metadata.uid !== process.getuid?.() || metadata.size > 128) fail("secrets", "unsafe");
    const value = readFileSync(fd, "utf8").trim();
    if (!/^[A-Za-z0-9+/]{43}=$/u.test(value) || Buffer.from(value, "base64").length !== 32 || Buffer.from(value, "base64").toString("base64") !== value) fail("secrets");
    return value;
  } catch (error) { if (error instanceof AxisConfigurationError) throw error; return fail("secrets", "unreadable"); }
  finally { if (fd !== undefined) closeSync(fd); }
}
function identity(node: AxisNode, worker: AxisWorker) { return { nodeId: node.id, workerId: worker.id, executorKind: worker.kind, runtimeProfile: worker.runtimeProfile }; }
function target(node: AxisNode, worker: AxisWorker) { return { endpointRef: { ref: worker.endpointRef, version: worker.endpointVersion }, identity: identity(node, worker), sessionBindingExecutorId: worker.executorId }; }
export interface RelayConfiguration {
  readonly runtime: RelayRuntimeConfig;
  readonly eventBindings: readonly RouterNodeEventHttpBinding[];
  readonly linearExecution: LinearExecutionRuntimeConfig | null;
  readonly linear: LinearRuntimeConfig | null;
  readonly linearApi: LinearApiRuntimeConfig | null;
  readonly host: string; readonly port: number; readonly vaultPath: string; readonly masterKey: string; readonly operatorToken: string;
}

/** All three Router relationships come from the same Node/Worker declarations. */
export function deriveRelayConfiguration(config: AxisConfiguration, read: SecretReader = name => readAxisSecret(config, name), secrets?: LinearSecrets): RelayConfiguration {
  const paths = databasePaths(config);
  const defaultWorker = config.nodes.flatMap(node => node.workers).find(worker => worker.id === config.router.defaultWorker);
  const linear = config.linear;
  const credentials = linear === undefined ? undefined : secrets ?? readLinearSecrets();
  const readKey = roleSecretReader(read);
  return freeze({
    host: config.relay.host, port: config.relay.port, vaultPath: paths.vault, masterKey: readKey("relay-master"), operatorToken: readKey("relay-operator"),
    linear: linear === undefined ? null : {
      databases: { channel: paths.channel, conversations: paths.conversations, runChain: paths.runChain, delivery: paths.delivery },
      oauth: { clientId: linear.clientId, clientSecret: requiredLinearSecret(credentials!.clientSecret), redirectUri: linear.redirectUri },
      webhookSecret: requiredLinearSecret(credentials!.webhookSecret),
      defaultTeamIds: [...new Set(linear.teamIds ?? [])], allowAllPublicTeams: linear.allowAllPublicTeams ?? false,
      tickIntervals: { ingress: linear.ingressTickIntervalMs ?? LINEAR_DEFAULT_TICK_INTERVAL_MS,
        observe: linear.observeTickIntervalMs ?? LINEAR_DEFAULT_TICK_INTERVAL_MS, outbox: linear.outboxTickIntervalMs ?? LINEAR_DEFAULT_TICK_INTERVAL_MS },
      integrityCheckIntervalMs: linear.integrityCheckIntervalMs ?? DEFAULT_INTEGRITY_CHECK_INTERVAL_MS,
    },
    linearApi: linear?.apiEnabled !== true ? null : { timeoutMs: linear.apiTimeoutMs ?? 30_000,
      ...(linear.cliGrants === undefined ? {} : { cliGrants: linear.cliGrants }) },
    runtime: {
      routerDatabase: paths.router, routerHost: config.router.host, routerPort: config.router.port, nativeSteer: config.router.nativeSteer,
      defaultExcludedTeamIds: config.router.defaultExcludedTeamIds,
      ...(defaultWorker === undefined ? {} : { defaultEndpointRef: { ref: defaultWorker.endpointRef, version: defaultWorker.endpointVersion } }),
      nodeEndpoints: config.nodes.map(node => ({ nodeRef: { ref: node.ref, version: node.version }, nodeId: node.id, endpoint: node.url, bearerToken: readKey(`node-${node.id}-command`) })),
      nodeTargets: config.nodes.flatMap(node => node.workers.map(worker => ({ ...target(node, worker), nodeRef: { ref: node.ref, version: node.version } }))),
    },
    linearExecution: config.nodes.some(node => node.workers.some(worker => worker.linearExecution)) ? {
      grants: config.linear!.executionGrants!,
      receivers: config.nodes.flatMap(node => node.workers.filter(worker => worker.linearExecution).map(worker => ({ identity: identity(node, worker), bearerToken: readKey(`worker-${worker.id}-linear`) }))),
    } : null,
    eventBindings: config.nodes.map(node => ({ bearerToken: readKey(`node-${node.id}-router-events`), authenticatedNode: { nodeRef: { ref: node.ref, version: node.version }, nodeId: node.id }, allowedOrigins: node.workers.map(worker => identity(node, worker)) })),
  });
}

export interface NodeConfiguration extends NodeRuntimeConfig { readonly routerEventToken: string; }
export function deriveNodeConfiguration(config: AxisConfiguration, nodeId?: string, read: SecretReader = name => readAxisSecret(config, name)): NodeConfiguration {
  const node = selectedNode(config, nodeId);
  const defaultWorkerKey = node.defaultWorker ?? (node.workers.length === 1 ? node.workers[0]!.key : undefined);
  const readKey = roleSecretReader(read);
  return freeze({
    contractVersion: NODE_CONTRACT_VERSION, database: join(config.dataDir, `node-${node.id}.sqlite`), nodeId: node.id, nodeRef: node.ref, nodeVersion: node.version,
    host: node.host, port: node.port, eventHost: node.eventHost, eventPort: node.eventPort,
    token: readKey(`node-${node.id}-command`), routerEventToken: readKey(`node-${node.id}-router-events`), routerEndpoint: config.router.url,
    ...(defaultWorkerKey === undefined ? {} : { defaultWorkerKey }),
    workers: node.workers.map(worker => ({ nodeRef: { ref: node.ref, version: node.version }, nodeId: node.id, workerKey: worker.key, target: target(node, worker),
      workerEndpoint: new URL(origin(worker.host, worker.port)).origin, commandToken: readKey(`worker-${worker.id}-command`), eventToken: readKey(`worker-${worker.id}-events`), readinessMode: worker.kind === "codex" ? "codex" : "unsupported" })),
    workerScopeRules: node.workerScopeRules,
    workerExecutorBindings: node.workers.flatMap(worker => worker.labels.map(executorLabel => ({ executorLabel, workerKey: worker.key }))),
    workerHostRoots: node.workers.flatMap(worker => worker.hostRoot === undefined ? [] : [{ workerId: worker.id, hostRoot: worker.hostRoot }]),
    workerDispatchTimeoutMs: node.workerDispatchTimeoutMs ?? 30_000, routerEventTimeoutMs: node.routerEventTimeoutMs ?? 10_000,
    workerReadinessTimeoutMs: node.workerReadinessTimeoutMs ?? 5_000, eventRetryInitialDelayMs: node.eventRetryInitialDelayMs ?? 1_000,
    eventRetryMaxDelayMs: node.eventRetryMaxDelayMs ?? 30_000, eventMaxConcurrentRuns: node.eventMaxConcurrentRuns ?? 8,
  });
}

export interface WorkerConfiguration extends WorkerRuntimeConfig {
  readonly kind: AxisWorker["kind"];
  readonly runtimeProfile: AxisWorker["runtimeProfile"];
  readonly executable?: string;
  readonly nativeSessionStorePath?: string;
  readonly instructions?: AgentInstructionOverrides;
  readonly linearExecution: { readonly relay: string; readonly receiverToken: string } | null;
}
export function deriveWorkerConfiguration(config: AxisConfiguration, workerId?: string, nodeId?: string, read: SecretReader = name => readAxisSecret(config, name)): WorkerConfiguration {
  const { node, worker } = selectedWorker(config, workerId, nodeId);
  const readKey = roleSecretReader(read);
  return freeze({
    kind: worker.kind, runtimeProfile: worker.runtimeProfile, database: join(config.dataDir, `worker-${worker.id}.sqlite`), workerId: worker.id, nodeId: node.id, executorId: worker.executorId,
    host: worker.host, port: worker.port, token: readKey(`worker-${worker.id}-command`), routerEndpoint: origin(node.eventHost, node.eventPort), routerToken: readKey(`worker-${worker.id}-events`),
    profileFallbackPolicy: worker.profileFallbackPolicy, workspaces: worker.workspaces,
    ...(worker.codexHome === undefined ? {} : { codexHome: worker.codexHome }),
    ...(worker.claudeExecutable === undefined ? {} : { executable: worker.claudeExecutable }),
    ...(worker.kind !== "claude-code" ? {} : { nativeSessionStorePath: join(config.dataDir, `worker-${worker.id}-native-sessions.json`) }),
    ...(worker.instructions === undefined ? {} : { instructions: worker.instructions.omitSessionResponse ? { sessionResponse: null } : worker.instructions.sessionResponse === undefined ? {} : { sessionResponse: worker.instructions.sessionResponse } }),
    linearExecution: worker.linearExecution ? { relay: config.relay.url, receiverToken: readKey(`worker-${worker.id}-linear`) } : null,
  });
}

export interface LinearSecrets { readonly clientSecret: string; readonly webhookSecret: string; }
function requiredLinearSecret(value: string): string {
  if (typeof value !== "string" || value.trim().length === 0) fail("linear.secrets", "required");
  return value;
}
/** Refuse credentials shared across purposes before composition opens storage. */
function roleSecretReader(read: SecretReader): SecretReader {
  const values = new Set<string>();
  return name => {
    const value = read(name);
    if (typeof value !== "string" || !/^[A-Za-z0-9+/]{43}=$/u.test(value) || Buffer.from(value, "base64").toString("base64") !== value
      || values.has(value)) fail("secrets", "invalid or reused");
    values.add(value);
    return value;
  };
}
/** External service credentials never come from TOML or the generated transport-key files. */
export function readLinearSecrets(env: Readonly<Record<string, string | undefined>> = process.env): LinearSecrets {
  const required = (name: string): string => {
    const value = env[name]; if (value === undefined || value.trim().length === 0) fail(name, "required"); return value;
  };
  return { clientSecret: required("LINEAR_CLIENT_SECRET"), webhookSecret: required("LINEAR_WEBHOOK_SECRET") };
}

function validateDeclarations(config: AxisConfiguration): void {
  const listener = (host: string, path: string): void => {
    if (/\s/u.test(host) || host.includes("/") || host.includes("@") || host.includes("\0") || host.includes(":") && isIP(host) !== 6) fail(path);
  };
  listener(config.relay.host, "relay.host"); listener(config.router.host, "router.host");
  if (config.relay.port === config.router.port) fail("router.port", "invalid (listener collision)");
  for (const node of config.nodes) {
    listener(node.host, "nodes.host");
    if (!numericLoopback(node.eventHost)) fail("nodes.eventHost");
    if ((node.eventRetryMaxDelayMs ?? 30_000) < (node.eventRetryInitialDelayMs ?? 1_000)) fail("nodes.eventRetryMaxDelayMs");
    const keys = new Set<string>(), labels = new Set<string>(), roots = new Set<string>();
    for (const worker of node.workers) {
      try { validateNodeTarget(target(node, worker)); } catch { fail("nodes.workers"); }
      try { validateWorkerKey(worker.key); } catch { fail("nodes.workers.key"); }
      if (keys.has(worker.key)) fail("nodes.workers.key", "invalid (duplicate)"); keys.add(worker.key);
      if (!numericLoopback(worker.host)) fail("nodes.workers.host");
      for (const label of worker.labels) { if (labels.has(label)) fail("nodes.workers.labels", "invalid (duplicate binding)"); labels.add(label); }
      if (worker.hostRoot !== undefined) { if (worker.hostRoot === "/" || roots.has(worker.hostRoot)) fail("nodes.workers.hostRoot"); roots.add(worker.hostRoot); }
      if (worker.instructions?.sessionResponse !== undefined && Buffer.byteLength(worker.instructions.sessionResponse, "utf8") > 65_536) fail("nodes.workers.instructions.sessionResponse");
      try { validateWorkerWorkspaceConfigurations(worker.workspaces); } catch { fail("nodes.workers.workspaces"); }
      if (worker.linearExecution) {
        if (!config.linear?.apiEnabled || !config.linear.executionGrants?.length) fail("linear.executionGrants", "required for enabled Workers");
        try { linearRelayUrl(config.relay.url); } catch { fail("relay.url"); }
      }
    }
    if (node.defaultWorker !== undefined && !keys.has(node.defaultWorker) || node.workers.length > 1 && node.defaultWorker === undefined) fail("nodes.defaultWorker");
    const scopes = new Set<string>();
    for (const rule of node.workerScopeRules) {
      try { validateWorkerSelectionScope(rule.scope); } catch { fail("nodes.workerScopeRules.scope"); }
      const key = JSON.stringify([rule.scope.system, rule.scope.tenantId, rule.scope.scopeId]);
      if (!keys.has(rule.workerKey) || scopes.has(key)) fail("nodes.workerScopeRules"); scopes.add(key);
    }
  }
  if (config.linear !== undefined) {
    try { const uri = new URL(config.linear.redirectUri); if (uri.username || uri.password || !["http:", "https:"].includes(uri.protocol)) fail("linear.redirectUri"); } catch { fail("linear.redirectUri"); }
    const organizations = new Set<string>();
    for (const grant of config.linear.executionGrants ?? []) {
      const value = grant as { organizationId: string; appUserId: string };
      if (![value.organizationId, value.appUserId].every(id => /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(id)) || organizations.has(value.organizationId)) fail("linear.executionGrants");
      organizations.add(value.organizationId);
    }
    if ((config.linear.executionGrants?.length ?? 0) > 1_000 || (config.linear.cliGrants?.length ?? 0) > 1_000) fail("linear");
    if (config.linear.cliGrants?.length && !config.linear.apiEnabled) fail("linear.apiEnabled", "required for CLI grants");
    const ids = new Set<string>(), hashes = new Set<string>();
    for (const grant of config.linear.cliGrants ?? []) {
      const value = grant as { credentialId: string; organizationId: string; appUserId: string; credentialHash: string; expiresAt: string };
      if (![value.credentialId, value.organizationId, value.appUserId].every(id => /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(id))
        || !/^[a-f0-9]{64}$/u.test(value.credentialHash) || !Number.isFinite(Date.parse(value.expiresAt)) || ids.has(value.credentialId) || hashes.has(value.credentialHash)) fail("linear.cliGrants");
      ids.add(value.credentialId); hashes.add(value.credentialHash);
    }
  }
}

const DEFAULT_CONFIGURATION_TOML = `schemaVersion = 1\ndataDir = "data"\n\n[serve]\nkind = "codex"\norganizationId = "example-organization"\nworkspaceRoot = "."\n\n[linear]\nclientId = "example-client"\nredirectUri = "http://127.0.0.1:8787/oauth/linear/callback"\n`;
function present(path: string): boolean {
  try { lstatSync(path); return true; } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
}
export function secretNames(config: AxisConfiguration): readonly string[] {
  return ["relay-master", "relay-operator", ...config.nodes.flatMap(node => [`node-${node.id}-command`, `node-${node.id}-router-events`,
    ...node.workers.flatMap(worker => [`worker-${worker.id}-command`, `worker-${worker.id}-events`, ...(worker.linearExecution ? [`worker-${worker.id}-linear`] : [])])])];
}
function privateDirectories(config: AxisConfiguration): void {
  mkdirSync(config.dataDir, { recursive: true, mode: 0o700 });
  const directory = join(config.dataDir, "secrets");
  for (const path of [config.dataDir, directory]) {
    if (path === directory) mkdirSync(directory, { recursive: true, mode: 0o700 });
    const metadata = lstatSync(path);
    if (!metadata.isDirectory() || metadata.isSymbolicLink() || metadata.uid !== process.getuid?.() || (metadata.mode & 0o077) !== 0) fail("dataDir", "unsafe");
  }
}
function createSecrets(config: AxisConfiguration): void {
  const names = secretNames(config);
  // Inspect all destinations before writing the first one. Exclusive opens also protect against races.
  if (names.some(name => present(secretPath(config, name)))) fail("secrets", "already present");
  for (const name of names) {
    const fd = openSync(secretPath(config, name), "wx", 0o600);
    try { fchmodSync(fd, 0o600); writeFileSync(fd, `${randomBytes(32).toString("base64")}\n`); }
    finally { closeSync(fd); }
  }
}
function withInitializationLock<T>(config: AxisConfiguration, action: () => T): T {
  privateDirectories(config);
  const path = join(config.dataDir, ".init.lock");
  let fd: number;
  try { fd = openSync(path, "wx", 0o600); } catch { return fail("initialization", "already active or incomplete"); }
  try { return action(); } finally { closeSync(fd); unlinkSync(path); }
}
/** Generates the complete declared topology once. It never rotates or repairs existing private files. */
export function generateAxisSecrets(config: AxisConfiguration): void {
  withInitializationLock(config, () => createSecrets(config));
}

/** A new file gets a minimal topology; an existing TOML file is preserved and provisions its own topology. */
export function initializeAxis(options: { readonly filename?: string; readonly dataDir?: string } = {}): AxisConfiguration {
  const filename = resolve(options.filename ?? "axis.toml");
  if (present(filename)) {
    if (options.dataDir !== undefined) fail("initialization", "invalid (dataDir is already configured)");
    const config = loadAxisConfiguration(filename); generateAxisSecrets(config); return config;
  }
  const dataDir = options.dataDir ?? "data";
  const source = DEFAULT_CONFIGURATION_TOML.replace('dataDir = "data"', `dataDir = ${JSON.stringify(dataDir)}`);
  const config = parseAxisConfiguration(source, filename);
  withInitializationLock(config, () => {
    if (present(filename) || secretNames(config).some(name => present(secretPath(config, name)))) fail("initialization", "already present");
    // Reserve the configuration path before creating keys, so concurrent initializers cannot replace it.
    writeFileSync(filename, source, { flag: "wx", mode: 0o600 });
    createSecrets(config);
  });
  return config;
}
