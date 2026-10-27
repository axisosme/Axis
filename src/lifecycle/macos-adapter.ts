import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { lookup } from "node:dns/promises";
import { access, chmod, copyFile, cp, lstat, mkdir, mkdtemp, readFile, readdir, readlink, realpath, rename, rm, rmdir, stat, writeFile } from "node:fs/promises";
import { Agent, request as httpRequest, type ClientRequestArgs } from "node:http";
import { createConnection, isIP, type Socket } from "node:net";
import { DatabaseSync } from "node:sqlite";
import { homedir, networkInterfaces } from "node:os";
import { basename, dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { assertAbsolutePath, HostStateError, validateHostState } from "./host-state.js";
import type { HostRole, HostState, WorkerRuntime } from "./host-state.js";
import {
  AMBIGUOUS_ACKNOWLEDGEMENT_MIN_AGE_MS,
  appendNodeAmbiguousAcknowledgement,
  appendWorkerStuckRunAcknowledgement,
  effectiveNodeBlockers,
  NodeUpgradeBlockedError,
  readNodeAmbiguousAcknowledgements,
  readWorkerStuckRunAcknowledgements,
  WorkerUpgradeBlockedError,
  type NodeAmbiguousAcknowledgementRecord,
  type NodeAmbiguousReleaseCounts,
  type WorkerStuckRunAcknowledgement,
} from "./stuck-run-acknowledgements.js";
import type { ForcedArchive, ForcedArchiveFile, LifecycleAdapter, StagedRelease } from "./transaction.js";
import { ForcedUpgradeError } from "./transaction.js";
import type { NodeAmbiguousCommandEvidence, NodeResolvedAmbiguousCommand } from "../node/journal.js";
import { SqliteNodeJournal } from "../node/journal.js";
import { inspectNodeWorkerBlockers, inspectNodeWorkerRuns, releasedWorkerRuns, verifiedWorkerHostRoot, type NodeWorkerRunEvidence } from "../node/maintenance.js";
import type { NodeRuntimeConfig } from "../node/runtime-config.js";
import { databasePaths, deriveRelayConfiguration, deriveNodeConfiguration, deriveWorkerConfiguration, readAxisSecret, secretPath, type AxisConfiguration } from "../configuration.js";
import { loadHostConfiguration } from "./configuration.js";
import { assertWorkerSchema } from "../worker/schema.js";
import { assertRouterSchema } from "../router/router.js";
import { RETRY_BUDGET_EXHAUSTED_CODE } from "../shared/retry-budget.js";
import { NODE_CONTRACT_VERSION, nodeMaintenanceStatus, type NodeBlockerCounts, type NodeMaintenanceStatus } from "../shared/router-node.js";

const run = promisify(execFile);
// Codex readiness starts a fresh app-server process, which can take many seconds on a cold start.
const SERVICE_READY_ATTEMPTS = 20;
const SERVICE_READY_RETRY_MS = 250;

/** One row `axis node ambiguous-commands list` reports, joining the Node's evidence with the Worker's. */
export interface NodeAmbiguousCommandListingEntry {
  readonly row: NodeAmbiguousCommandEvidence;
  readonly worker: NodeWorkerRunEvidence;
  readonly acknowledgement: NodeAmbiguousAcknowledgementRecord | null;
  readonly workerRelease: "none" | "recorded" | "not_needed" | "not_configured" | "stale";
}
/** One acknowledged command that is no longer `ambiguous`, resolved by convergence or another operator action. */
export interface NodeResolvedAmbiguousCommandListingEntry {
  readonly commandId: string;
  readonly state: NodeResolvedAmbiguousCommand["state"];
  readonly acknowledgement: NodeAmbiguousAcknowledgementRecord | null;
}
export interface NodeAmbiguousCommandsListing {
  readonly ambiguousCommands: readonly NodeAmbiguousCommandListingEntry[];
  readonly resolvedAfterAcknowledgement: readonly NodeResolvedAmbiguousCommandListingEntry[];
  readonly upgradeGate: { readonly blocked: boolean; readonly blockers: NodeBlockerCounts; readonly acknowledged: NodeAmbiguousReleaseCounts };
}
export interface AcknowledgeAmbiguousCommandInput {
  readonly commandId: string;
  readonly operatorRef: string;
  readonly authorizationRef: string;
  readonly source: string;
  readonly requestRef: string;
  readonly reason?: string | null;
}
export type AcknowledgeAmbiguousCommandResult =
  | { readonly outcome: "not_ambiguous" | "too_recent" | "unavailable" }
  | {
      readonly outcome: "acknowledged";
      readonly node: "applied_and_verified" | "already_applied_and_verified";
      readonly worker: "recorded" | "already_recorded" | "not_needed" | "not_configured";
      readonly acknowledgement: NodeAmbiguousAcknowledgementRecord;
      readonly workerAcknowledgement: WorkerStuckRunAcknowledgement | null;
    };

export interface MacosAdapterOptions {
  readonly hostRoot?: string; readonly sourcePackage: string; readonly nodePath?: string; readonly home?: string;
  /** Names `upgrade --force` archive directories; defaults to the wall clock. */
  readonly now?: () => Date;
  /** How long `stopService` waits for the service to really stop after bootout; defaults to 30 000 ms. */
  readonly stopTimeoutMs?: number;
  /** How often `stopService` asks launchd while it waits; defaults to 250 ms. */
  readonly stopPollMs?: number;
}

interface CommandResult { readonly stdout: string | Buffer; }
type NodeAttestationCommand = (file: string, arguments_: readonly string[], options: { readonly timeout: number; readonly maxBuffer: number }) => Promise<CommandResult>;
export interface NodeSocketTuple { readonly family: "IPv4" | "IPv6"; readonly localAddress: string; readonly localPort: number; readonly remoteAddress: string; readonly remotePort: number; }
export interface NodeReadinessAttestationDependencies { readonly command?: NodeAttestationCommand; readonly connect?: (endpoint: URL, timeout: number) => Promise<Socket>; readonly delay?: (milliseconds: number) => Promise<void>; readonly timeout?: number; }

const ATTESTATION_OUTPUT_LIMIT = 64 * 1024;
const defaultAttestationCommand: NodeAttestationCommand = async (file, arguments_, options) => {
  const result = await run(file, [...arguments_], options); return { stdout: result.stdout };
};

function sha(value: string | Buffer): string { return `sha256:${createHash("sha256").update(value).digest("hex")}`; }
function xml(value: string): string { return value.replace(/&/gu, "&amp;").replace(/</gu, "&lt;").replace(/>/gu, "&gt;").replace(/"/gu, "&quot;").replace(/'/gu, "&apos;"); }
function urlHostname(value: string): string { return value.includes(":") && !value.startsWith("[") ? `[${value}]` : value; }
function runtimeBin(role: HostRole, workerRuntime: WorkerRuntime | null): string {
  if (role === "controller") return "axis-linear-relay";
  if (role === "node") return "axis-node";
  if (workerRuntime === "codex") return "axis-codex-worker";
  if (workerRuntime === "claude") return "axis-claude-worker";
  throw new HostStateError("missing worker runtime");
}
function lifecycleAsset(name: "runner" | "host-state"): string {
  const current = fileURLToPath(import.meta.url); return join(dirname(current), `${name}${current.endsWith(".ts") ? ".ts" : ".js"}`);
}

async function regularPrivateFile(path: string): Promise<string> {
  const canonical = await realpath(assertAbsolutePath(path, "file")); const metadata = await stat(canonical);
  if (!metadata.isFile() || metadata.uid !== (process.getuid?.() ?? metadata.uid) || (metadata.mode & 0o077) !== 0) throw new HostStateError("configuration file must be owner-private");
  return canonical;
}
async function atomicWrite(path: string, data: string, mode = 0o600): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = join(dirname(path), `.${basename(path)}.${process.pid}.${Date.now()}`);
  await writeFile(temporary, data, { mode, flag: "wx" }); await rename(temporary, path);
}
export async function manifestDigest(path: string): Promise<string> {
  const entries: { readonly path: string; readonly kind: "directory" | "file" | "symlink" }[] = [];
  async function walk(directory: string): Promise<void> {
    for (const item of await readdir(directory, { withFileTypes: true })) {
      const full = join(directory, item.name);
      if (item.isDirectory()) { entries.push({ path: full, kind: "directory" }); await walk(full); }
      else if (item.isFile()) entries.push({ path: full, kind: "file" });
      else if (item.isSymbolicLink()) entries.push({ path: full, kind: "symlink" });
      else throw new HostStateError("release contains unsupported file type");
    }
  }
  await walk(path); entries.sort((left, right) => left.path.localeCompare(right.path)); const hash = createHash("sha256");
  for (const entry of entries) { hash.update(entry.kind); hash.update("\u0000"); hash.update(relative(path, entry.path)); hash.update("\u0000"); if (entry.kind === "file") hash.update(await readFile(entry.path)); else if (entry.kind === "symlink") hash.update(await readlink(entry.path)); hash.update("\u0000"); }
  return `sha256:${hash.digest("hex")}`;
}

export function localHealthEndpoint(input: string, resolvedAddresses: readonly string[], hostAddresses: readonly string[]): URL | null {
  let endpoint: URL;
  try { endpoint = new URL(input); } catch { return null; }
  const local = new Set(hostAddresses); const isLocal = (address: string): boolean => address === "127.0.0.1" || address === "::1" || local.has(address);
  if (endpoint.protocol !== "http:" || endpoint.username !== "" || endpoint.password !== "" || resolvedAddresses.length === 0 || !resolvedAddresses.every(isLocal)) return null;
  const target = new URL(endpoint); target.hostname = urlHostname(resolvedAddresses[0] ?? ""); return target;
}
function assignedHostAddresses(): string[] { return Object.values(networkInterfaces()).flatMap((entries) => entries?.map((entry) => entry.address) ?? []); }
function configuredPort(value: string | undefined, fallback?: number): number {
  if (value === undefined && fallback !== undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 65_535) throw new HostStateError("service verification is not configured");
  return parsed;
}
function localServiceUrl(host: string | undefined, port: number, path: string, fallbackHost?: string): string {
  const configured = host ?? fallbackHost;
  if (configured === undefined || configured.trim().length === 0 || /\s/u.test(configured)) throw new HostStateError("service verification is not configured");
  const targetHost = configured === "0.0.0.0" ? "127.0.0.1" : configured === "::" ? "::1" : configured;
  const endpoint = new URL("http://127.0.0.1"); endpoint.hostname = urlHostname(targetHost); endpoint.port = String(port); endpoint.pathname = path;
  return endpoint.toString();
}
function nodeReadinessMatches(value: unknown, identity: { readonly ref: string; readonly version: string }): boolean {
  try {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
    const document = value as Record<string, unknown>;
    if (Object.keys(document).sort().join(",") !== "contractVersion,nodeRef,observedAt,service,storage,workers") return false;
    const nodeRef = document.nodeRef, service = document.service, storage = document.storage;
    if (nodeRef === null || typeof nodeRef !== "object" || Array.isArray(nodeRef) || Object.keys(nodeRef).sort().join(",") !== "ref,version") return false;
    if (service === null || typeof service !== "object" || Array.isArray(service) || Object.keys(service).join(",") !== "status") return false;
    if (storage === null || typeof storage !== "object" || Array.isArray(storage) || Object.keys(storage).join(",") !== "status") return false;
    const reference = nodeRef as Record<string, unknown>;
    return document.contractVersion === "1.0.0" && reference.ref === identity.ref && reference.version === identity.version
      && typeof document.observedAt === "string" && !Number.isNaN(Date.parse(document.observedAt))
      && (service as Record<string, unknown>).status === "configured"
      && (storage as Record<string, unknown>).status === "available" && Array.isArray(document.workers);
  } catch { return false; }
}

function canonicalIp(value: string): string {
  if (value.includes("%") || isIP(value) === 0) throw new HostStateError("node socket ownership evidence is invalid");
  if (isIP(value) === 4) return value.split(".").map((part) => String(Number(part))).join(".");
  const hostname = new URL(`http://[${value}]/`).hostname.slice(1, -1).toLowerCase();
  const mapped = /^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/u.exec(hostname);
  if (mapped === null) return hostname;
  const high = Number.parseInt(mapped[1] ?? "", 16), low = Number.parseInt(mapped[2] ?? "", 16);
  return `${high >>> 8}.${high & 255}.${low >>> 8}.${low & 255}`;
}
function parseSocketEndpoint(value: string): { readonly address: string; readonly port: number } {
  const bracketed = /^\[([^\]]+)\]:(\d{1,5})$/u.exec(value), unbracketed = /^([^:]+):(\d{1,5})$/u.exec(value); const match = bracketed ?? unbracketed;
  if (match === null) throw new HostStateError("node socket ownership evidence is invalid");
  const port = Number(match[2]); if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) throw new HostStateError("node socket ownership evidence is invalid");
  return { address: canonicalIp(match[1] ?? ""), port };
}
function parseSocketName(value: string): { readonly local: { readonly address: string; readonly port: number }; readonly remote: { readonly address: string; readonly port: number } } {
  const parts = value.split("->"); if (parts.length !== 2) throw new HostStateError("node socket ownership evidence is invalid");
  return { local: parseSocketEndpoint(parts[0] ?? ""), remote: parseSocketEndpoint(parts[1] ?? "") };
}
interface LsofSocketRecord { readonly pid: number; readonly parentPid: number; readonly uid: number; readonly family: string; readonly protocol: string; readonly name: string; readonly state: string; }
function parseLsofSockets(output: string): readonly LsofSocketRecord[] {
  if (Buffer.byteLength(output) === 0 || Buffer.byteLength(output) >= ATTESTATION_OUTPUT_LIMIT) throw new HostStateError("node socket ownership evidence is invalid");
  let processFields: { pid?: number; parentPid?: number; uid?: number; files: number } | null = null; let fileFields: { family?: string; protocol?: string; name?: string; state?: string } | null = null; const records: LsofSocketRecord[] = [];
  const integer = (value: string): number => { if (!/^\d+$/u.test(value)) throw new HostStateError("node socket ownership evidence is invalid"); const parsed = Number(value); if (!Number.isSafeInteger(parsed)) throw new HostStateError("node socket ownership evidence is invalid"); return parsed; };
  const finishFile = (): void => {
    if (fileFields === null) return;
    if (processFields?.pid === undefined || processFields.parentPid === undefined || processFields.uid === undefined || fileFields.family === undefined || fileFields.protocol === undefined || fileFields.name === undefined || fileFields.state === undefined) throw new HostStateError("node socket ownership evidence is invalid");
    if ((fileFields.family !== "IPv4" && fileFields.family !== "IPv6") || fileFields.protocol !== "TCP" || fileFields.state !== "ESTABLISHED") throw new HostStateError("node socket ownership evidence is invalid");
    parseSocketName(fileFields.name); records.push({ pid: processFields.pid, parentPid: processFields.parentPid, uid: processFields.uid, family: fileFields.family, protocol: fileFields.protocol, name: fileFields.name, state: fileFields.state }); processFields.files += 1; fileFields = null;
  };
  const finishProcess = (): void => { finishFile(); if (processFields !== null && (processFields.pid === undefined || processFields.parentPid === undefined || processFields.uid === undefined || processFields.files === 0)) throw new HostStateError("node socket ownership evidence is invalid"); };
  const assign = (target: object, key: string, value: string | number): void => { const record = target as Record<string, unknown>; if (record[key] !== undefined) throw new HostStateError("node socket ownership evidence is invalid"); record[key] = value; };
  for (const field of output.split(/[\u0000\n]/u).filter((entry) => entry.length > 0)) {
    const id = field[0], value = field.slice(1);
    if (id === "p") { finishProcess(); processFields = { files: 0 }; assign(processFields, "pid", integer(value)); continue; }
    if (processFields === null) throw new HostStateError("node socket ownership evidence is invalid");
    if (id === "R") { if (fileFields !== null) throw new HostStateError("node socket ownership evidence is invalid"); assign(processFields, "parentPid", integer(value)); continue; }
    if (id === "u") { if (fileFields !== null) throw new HostStateError("node socket ownership evidence is invalid"); assign(processFields, "uid", integer(value)); continue; }
    if (id === "f") { finishFile(); fileFields = {}; continue; }
    if (fileFields === null) throw new HostStateError("node socket ownership evidence is invalid");
    if (id === "t") assign(fileFields, "family", value);
    else if (id === "P") assign(fileFields, "protocol", value);
    else if (id === "n") assign(fileFields, "name", value);
    else if (id === "T" && value.startsWith("ST=")) assign(fileFields, "state", value.slice(3));
    else if (id === "T" && /^(?:QR|QS)=\d+$/u.test(value)) { /* ancillary machine-readable queue evidence */ }
    else throw new HostStateError("node socket ownership evidence is invalid");
  }
  finishProcess(); return records;
}
export function assertNodeSocketOwnershipEvidence(input: { readonly output: string; readonly runnerPidBefore: number; readonly runnerPidAfter: number; readonly uid: number; readonly tuple: NodeSocketTuple }): number {
  if (input.runnerPidBefore < 2 || input.runnerPidBefore !== input.runnerPidAfter || input.tuple.localPort < 1 || input.tuple.remotePort < 1) throw new HostStateError("node socket ownership evidence is invalid");
  const localAddress = canonicalIp(input.tuple.localAddress), remoteAddress = canonicalIp(input.tuple.remoteAddress);
  const matches = parseLsofSockets(input.output).filter((record) => {
    if (record.parentPid !== input.runnerPidBefore || record.uid !== input.uid || record.family !== input.tuple.family || record.protocol !== "TCP" || record.state !== "ESTABLISHED") return false;
    const tuple = parseSocketName(record.name);
    return tuple.local.address === remoteAddress && tuple.local.port === input.tuple.remotePort && tuple.remote.address === localAddress && tuple.remote.port === input.tuple.localPort;
  });
  if (matches.length !== 1 || matches[0] === undefined || matches[0].pid < 2 || matches[0].pid === input.runnerPidBefore) throw new HostStateError("node socket ownership evidence is invalid");
  return matches[0].pid;
}
function runnerPid(output: string | Buffer): number {
  const value = String(output); if (!/^\s*\d+\s*$/u.test(value)) throw new HostStateError("node service identity is unavailable");
  const pid = Number(value.trim()); if (!Number.isSafeInteger(pid) || pid < 2) throw new HostStateError("node service identity is unavailable"); return pid;
}
async function connectWithoutWriting(endpoint: URL, timeout: number): Promise<Socket> {
  const host = endpoint.hostname.replace(/^\[|\]$/gu, ""), port = Number(endpoint.port); const family = isIP(host);
  if ((family !== 4 && family !== 6) || !Number.isSafeInteger(port) || port < 1 || port > 65_535) throw new HostStateError("node service identity is unavailable");
  const socket = createConnection({ host, port, family });
  await new Promise<void>((resolvePromise, rejectPromise) => {
    const fail = (error: Error): void => { socket.destroy(); rejectPromise(error); }; socket.once("error", fail); socket.setTimeout(timeout, () => fail(new Error("socket timeout")));
    socket.once("connect", () => { socket.setTimeout(0); resolvePromise(); });
  });
  return socket;
}
function socketTuple(socket: Socket): NodeSocketTuple {
  const local = socket.address(), remoteAddress = socket.remoteAddress, remotePort = socket.remotePort, remoteFamily = socket.remoteFamily;
  if (typeof local === "string" || !("family" in local) || (local.family !== "IPv4" && local.family !== "IPv6") || remoteFamily !== local.family || remoteAddress === undefined || remotePort === undefined || socket.bytesRead !== 0 || socket.bytesWritten !== 0) throw new HostStateError("node socket ownership evidence is invalid");
  return { family: local.family, localAddress: local.address, localPort: local.port, remoteAddress, remotePort };
}
class RetainedSocketAgent extends Agent {
  readonly #socket: Socket; #used = false;
  constructor(socket: Socket) { super({ keepAlive: false, maxSockets: 1 }); this.#socket = socket; }
  override createConnection(_options: ClientRequestArgs): Socket { if (this.#used) throw new HostStateError("node verification attempted a replacement connection"); this.#used = true; return this.#socket; }
  assertUsedOnce(): void { if (!this.#used) throw new HostStateError("node verification did not use the attested connection"); }
}
async function requestOnRetainedSocket(endpoint: URL, socket: Socket, token: string | null, timeout: number): Promise<{ readonly status: number; readonly body: string }> {
  const agent = new RetainedSocketAgent(socket);
  try {
    return await new Promise((resolvePromise, rejectPromise) => {
      let settled = false; const fail = (error: Error): void => { if (!settled) { settled = true; rejectPromise(error); } };
      const request = httpRequest(endpoint, { method: "GET", agent, ...(token === null ? {} : { headers: { authorization: `Bearer ${token}` } }), signal: AbortSignal.timeout(timeout) }, (response) => {
        if (response.socket !== socket) { request.destroy(new HostStateError("node verification changed connections")); return; }
        const chunks: Buffer[] = []; response.on("data", (chunk: Buffer | string) => { chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)); }); response.once("error", fail); response.once("end", () => { if (!settled) { settled = true; resolvePromise({ status: response.statusCode ?? 0, body: Buffer.concat(chunks).toString("utf8") }); } });
      });
      request.once("socket", (assigned) => { if (assigned !== socket) request.destroy(new HostStateError("node verification changed connections")); }); request.once("error", fail); request.end();
    });
  } finally { agent.assertUsedOnce(); agent.destroy(); }
}
async function attestedNodeGet(input: { readonly endpoint: URL; readonly serviceLabel: string; readonly uid: number; readonly token: string | null }, dependencies: NodeReadinessAttestationDependencies): Promise<{ readonly status: number; readonly body: string }> {
  const timeout = dependencies.timeout ?? 3_000, command = dependencies.command ?? defaultAttestationCommand, connect = dependencies.connect ?? connectWithoutWriting; const socket = await connect(input.endpoint, timeout);
  try {
    const tuple = socketTuple(socket), target = `gui/${input.uid}/${input.serviceLabel}`;
    const before = runnerPid((await command("/bin/launchctl", ["kickstart", "-p", target], { timeout, maxBuffer: ATTESTATION_OUTPUT_LIMIT })).stdout);
    const lsof = await command("/usr/sbin/lsof", ["-nP", "-a", "-u", String(input.uid), `-iTCP:${tuple.remotePort}`, "-sTCP:ESTABLISHED", "-Ts", "-F0pRuftPnT"], { timeout, maxBuffer: ATTESTATION_OUTPUT_LIMIT });
    const after = runnerPid((await command("/bin/launchctl", ["kickstart", "-p", target], { timeout, maxBuffer: ATTESTATION_OUTPUT_LIMIT })).stdout);
    assertNodeSocketOwnershipEvidence({ output: String(lsof.stdout), runnerPidBefore: before, runnerPidAfter: after, uid: input.uid, tuple });
    if (socket.destroyed || socket.bytesRead !== 0 || socket.bytesWritten !== 0) throw new HostStateError("node socket ownership evidence is invalid");
    return await requestOnRetainedSocket(input.endpoint, socket, input.token, timeout);
  } catch (error) { socket.destroy(); throw error; }
}
export async function verifyOwnedNodeReadiness(input: { readonly endpoint: URL; readonly serviceLabel: string; readonly token: string; readonly identity: { readonly ref: string; readonly version: string }; readonly uid?: number }, dependencies: NodeReadinessAttestationDependencies = {}): Promise<void> {
  const uid = input.uid ?? process.getuid?.(); if (uid === undefined || !Number.isSafeInteger(uid) || uid < 0 || input.token.length === 0 || !/^[A-Za-z][A-Za-z0-9.-]{2,127}$/u.test(input.serviceLabel)) throw new HostStateError("node service identity is unavailable");
  const wait = dependencies.delay ?? (async (milliseconds: number) => delay(milliseconds)); let healthy = false;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try { const response = await attestedNodeGet({ endpoint: input.endpoint, serviceLabel: input.serviceLabel, uid, token: input.token }, dependencies); healthy = response.status === 200 && nodeReadinessMatches(JSON.parse(response.body) as unknown, input.identity); if (healthy) break; } catch { /* bounded readiness retry */ }
    if (attempt < 4) await wait(250);
  }
  if (!healthy) throw new HostStateError("service health verification failed");
  try { const response = await attestedNodeGet({ endpoint: input.endpoint, serviceLabel: input.serviceLabel, uid, token: null }, dependencies); if (response.status !== 401) throw new HostStateError("service authorization boundary verification failed"); }
  catch (error) { if (error instanceof HostStateError && error.message === "service authorization boundary verification failed") throw error; throw new HostStateError("service authorization boundary verification failed"); }
}
export function lifecycleVerificationConfig(state: HostState, configuration: AxisConfiguration): { readonly healthUrl: string; readonly healthToken: string | null; readonly authUrl: string; readonly authMethod?: "GET"; readonly nodeIdentity?: { readonly ref: string; readonly version: string } } {
  if (state.role === "controller") {
    return { healthUrl: localServiceUrl(configuration.relay.host, configuration.relay.port, "/health/ready"), healthToken: null,
      authUrl: localServiceUrl(configuration.router.host, configuration.router.port, "/v1/router/node-run-events") };
  }
  if (state.role === "node") {
    const node = deriveNodeConfiguration(configuration, state.configuration.nodeId!);
    const readiness = localServiceUrl(node.host, node.port, "/v1/node/readiness");
    return { healthUrl: readiness, healthToken: node.token, authUrl: readiness, authMethod: "GET", nodeIdentity: { ref: node.nodeRef, version: node.nodeVersion } };
  }
  const worker = deriveWorkerConfiguration(configuration, state.configuration.workerId!, state.configuration.nodeId!);
  if ((worker.kind === "codex" ? "codex" : "claude") !== state.workerRuntime) throw new HostStateError("Worker runtime identity mismatch");
  return { healthUrl: localServiceUrl(worker.host, worker.port, "/v1/worker/readiness"), healthToken: worker.token,
    authUrl: localServiceUrl(worker.host, worker.port, "/v1/worker/commands") };
}

export function lifecycleDatabasePaths(state: HostState, configuration: AxisConfiguration): readonly string[] {
  if (state.role === "controller") {
    const paths = databasePaths(configuration);
    return [paths.router, ...(configuration.linear === undefined ? [] : [paths.channel, paths.conversations, paths.runChain, paths.delivery])];
  }
  return state.role === "node" ? [deriveNodeConfiguration(configuration, state.configuration.nodeId!).database]
    : [deriveWorkerConfiguration(configuration, state.configuration.workerId!, state.configuration.nodeId!).database];
}

export function managedRunnerPath(state: HostState): string { return join(state.activeRelease.packagePath, "dist", "lifecycle", "runner.js"); }
const FORCED_STORAGE_SUFFIXES: readonly string[] = Object.freeze(["", "-wal", "-shm"]);
const FORCED_ARCHIVE_MANIFEST = "forced-upgrade.v1.json";
const FORCED_ARCHIVE_MANIFEST_VERSION = "axis.host.forced-upgrade.archive.v1";

/** Explicit force archives only the selected Node or Worker store. */
export function forcedStoragePaths(state: HostState, configuration: AxisConfiguration): readonly { readonly key: string; readonly path: string }[] {
  return state.role === "controller" ? [] : lifecycleDatabasePaths(state, configuration).map(path => ({ key: basename(path), path }));
}
/** `2026-09-14T12:00:00.123Z` -> `20260914T120000Z`: sortable, second resolution, no characters a path must escape. */
export function forcedArchiveName(instant: Date): string { return instant.toISOString().replace(/\.\d{3}Z$/u, "Z").replace(/[-:]/gu, ""); }
async function forcedStorageFile(path: string): Promise<{ readonly size: number } | null> {
  try {
    const metadata = await lstat(path);
    if (!metadata.isFile()) throw new HostStateError("forced upgrade storage is not a regular file");
    return { size: metadata.size };
  } catch (error) {
    if (error instanceof HostStateError) throw error;
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw new HostStateError("forced upgrade storage cannot be inspected");
  }
}
/** Whether a process exists. `EPERM` means it exists but belongs to someone else, which still counts as running. */
function processExists(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
}
function positiveMilliseconds(value: number | undefined, fallback: number): number { return value !== undefined && Number.isSafeInteger(value) && value > 0 ? value : fallback; }
async function existingAncestorDirectory(path: string): Promise<string> {
  for (let candidate = path; ; candidate = dirname(candidate)) {
    try {
      if ((await stat(candidate)).isDirectory()) return candidate;
      throw new HostStateError("forced upgrade archive root is not a directory");
    } catch (error) {
      if (error instanceof HostStateError) throw error;
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" || dirname(candidate) === candidate) throw new HostStateError("forced upgrade archive root cannot be inspected");
    }
  }
}
/**
 * The "is there active work" clauses `assertLifecycleDatabaseDrained` checks. A database without a
 * clause's table skips it.
 *
 * A Linear inbox delivery quarantined with `RETRY_BUDGET_EXHAUSTED_CODE` still blocks drain: it is
 * unfinished user work. It is keyed on `failure_code`, because `quarantined` is also the routine
 * outcome for `linear.ingress.unsupported_shape`. `axis linear-inbox-give-ups acknowledge` releases
 * the block by writing a row to `linear_inbox_give_up_dispositions`; the delivery itself keeps its
 * evidence. The redispatch marker is not a drain blocker.
 */
interface DrainCheck {
  readonly table: string;
  readonly where: string;
  /** An extra conjunct applied only when `table` is present, narrowing what this clause counts. */
  readonly releasedBy?: { readonly table: string; readonly where: string };
}

const DRAIN_CHECKS: readonly DrainCheck[] = Object.freeze([
  { table: "router_runs", where: "state IN ('pending','running')" },
  { table: "worker_runs", where: "state IN ('reserved','binding','accepted','running','observational')" },
  { table: "worker_outbox", where: "delivered_at IS NULL" },
  { table: "channel_message_outbox", where: "state IN ('pending','delivering')" },
  { table: "linear_inbox_deliveries", where: "state IN ('accepted','processing')" },
  {
    table: "linear_inbox_deliveries",
    where: `state = 'quarantined' AND failure_code = '${RETRY_BUDGET_EXHAUSTED_CODE}'`,
    // A sub-select, so a delivery with no disposition row is still counted. `delivery_id IS NOT NULL`
    // is required: one NULL in a `NOT IN` sub-select makes the predicate NULL for every row, and this
    // runs on a raw read-only handle with no schema check.
    releasedBy: {
      table: "linear_inbox_give_up_dispositions",
      where: "delivery_id NOT IN (SELECT delivery_id FROM linear_inbox_give_up_dispositions WHERE acknowledged_at IS NOT NULL AND delivery_id IS NOT NULL)",
    },
  },
]);

/**
 * `release` applies an operator's Worker-side stuck-run acknowledgements to this database's
 * `worker_runs` clause only.
 */
export function assertLifecycleDatabaseDrained(path: string, release?: { readonly workerStuckRuns: readonly WorkerStuckRunAcknowledgement[] }): void {
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    const integrity = database.prepare("PRAGMA integrity_check").get() as { integrity_check?: string } | undefined; if (integrity?.integrity_check !== "ok") throw new HostStateError("database integrity check failed");
    const tables = database.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as { name: string }[];
    const names = new Set(tables.map((entry) => entry.name));
    if (database.prepare("SELECT 1 FROM sqlite_master WHERE name GLOB 'router_*'").get() !== undefined) {
      try { assertRouterSchema(database); }
      catch { throw new HostStateError("Router database schema is unavailable"); }
    }
    if (database.prepare("SELECT 1 FROM sqlite_master WHERE name LIKE 'worker%' OR tbl_name LIKE 'worker%'").get() !== undefined) {
      try { assertWorkerSchema(database); }
      catch { throw new HostStateError("Worker database schema is unavailable"); }
    }
    const count = (table: string, where: string): number => Number((database.prepare(`SELECT COUNT(*) AS count FROM ${table} WHERE ${where}`).get() as { count: number }).count);
    // `releasedBy` is a conjunction with the clause's own `where`, never a replacement.
    const clause = ({ where, releasedBy }: DrainCheck): string =>
      releasedBy !== undefined && names.has(releasedBy.table) ? `(${where}) AND (${releasedBy.where})` : where;
    let stuckRuns = 0, releasedStuckRuns = 0;
    const blocking: string[] = [];
    for (const check of DRAIN_CHECKS) {
      if (!names.has(check.table)) continue;
      if (check.table === "worker_runs" && release !== undefined) {
        const rows = database.prepare(`SELECT run_id, intent_digest, state FROM worker_runs WHERE ${check.where}`).all() as { readonly run_id?: unknown; readonly intent_digest?: unknown; readonly state?: unknown }[];
        let remaining = 0;
        for (const row of rows) {
          if (row.state !== "reserved" && row.state !== "observational") { remaining += 1; continue; }
          stuckRuns += 1;
          const runId = String(row.run_id), intentDigest = String(row.intent_digest);
          if (release.workerStuckRuns.some((ack) => ack.runId === runId && ack.workerIntentDigest === intentDigest && ack.acknowledgedState === row.state)) releasedStuckRuns += 1;
          else remaining += 1;
        }
        if (remaining > 0) blocking.push(check.table);
        continue;
      }
      if (check.table === "worker_runs") {
        const stuck = database.prepare(`SELECT COUNT(*) AS count FROM worker_runs WHERE ${clause(check)} AND state IN ('reserved','observational')`).get() as { readonly count?: unknown } | undefined;
        stuckRuns = Number(stuck?.count ?? 0);
      }
      if (count(check.table, clause(check)) > 0) blocking.push(check.table);
    }
    if (blocking.length > 0) {
      if (names.has("worker_runs") && blocking.includes("worker_runs")) {
        throw new WorkerUpgradeBlockedError({ stuckRuns, releasedStuckRuns, otherBlockingTables: blocking.filter((table) => table !== "worker_runs") });
      }
      throw new HostStateError("active work or outbox prevents upgrade");
    }
  } finally { database.close(); }
}

export class MacosLifecycleAdapter implements LifecycleAdapter {
  readonly #root: string; readonly #source: string; readonly #node: string; readonly #home: string; readonly #statePath: string; readonly #backupRoot: string; readonly #lockPath: string;
  #backupPath: string | null = null; #databaseBackups: { readonly source: string; readonly backup: string | null }[] = []; #newRelease: string | null = null;
  readonly #now: () => Date; readonly #archiveRoot: string; readonly #stopTimeoutMs: number; readonly #stopPollMs: number;
  constructor(options: MacosAdapterOptions) {
    this.#home = options.home ?? homedir(); this.#root = resolve(options.hostRoot ?? join(this.#home, ".axis", "host")); this.#source = resolve(options.sourcePackage); this.#node = options.nodePath ?? process.execPath;
    this.#now = options.now ?? (() => new Date()); this.#archiveRoot = join(this.#root, "archive");
    this.#stopTimeoutMs = positiveMilliseconds(options.stopTimeoutMs, 30_000); this.#stopPollMs = positiveMilliseconds(options.stopPollMs, 250);
    this.#statePath = join(this.#root, "axis.host.v1.json"); this.#backupRoot = join(this.#root, "backups"); this.#lockPath = join(this.#home, ".axis", ".lifecycle.lock");
  }
  async acquireLock(): Promise<() => Promise<void>> {
    await mkdir(dirname(this.#lockPath), { recursive: true, mode: 0o700 });
    try { await mkdir(this.#lockPath, { mode: 0o700 }); } catch (error) { if ((error as NodeJS.ErrnoException).code === "EEXIST") throw new HostStateError("another lifecycle operation is active"); throw new HostStateError("lifecycle lock is unavailable"); }
    let released = false;
    return async () => { if (!released) { await rmdir(this.#lockPath); released = true; } };
  }
  managedPlistPathFor(label: string): string { return join(this.#home, "Library", "LaunchAgents", `${label}.plist`); }
  async readState(): Promise<HostState | null> {
    try {
      const file = await regularPrivateFile(this.#statePath); return validateHostState(JSON.parse(await readFile(file, "utf8")) as unknown); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw new HostStateError("invalid host lifecycle state"); }
  }
  async managedPlistExists(path: string): Promise<boolean> {
    try { await access(path); return true; } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new HostStateError("managed plist cannot be inspected"); }
    const filename = basename(path); const label = filename.endsWith(".plist") ? filename.slice(0, -".plist".length) : "";
    if (!/^[A-Za-z][A-Za-z0-9.-]{2,127}$/u.test(label)) throw new HostStateError("managed service label is invalid");
    try { await run("launchctl", ["print", `gui/${process.getuid?.() ?? 0}/${label}`], { timeout: 30_000 }); return true; }
    catch (error) { if ((error as { readonly code?: unknown }).code === 113) return false; throw new HostStateError("managed service cannot be inspected"); }
  }
  async managedPlistMatches(state: HostState): Promise<boolean> { try { return (await readFile(state.managedPlist, "utf8")) === this.#plist(state); } catch { return false; } }
  async loadConfiguration(reference: HostState["configuration"]): Promise<AxisConfiguration> { return loadHostConfiguration(reference); }
  async stage(role: HostRole, runtime: WorkerRuntime | null): Promise<StagedRelease> {
    if (process.platform !== "darwin") throw new HostStateError("host lifecycle requires macOS");
    if (Number(process.versions.node.split(".", 1)[0]) < 24) throw new HostStateError("Node.js 24 or newer is required");
    const source = await realpath(this.#source); await mkdir(this.#root, { recursive: true, mode: 0o700 }); const scratch = await mkdtemp(join(this.#root, ".stage-"));
    try {
      let archive = source;
      if ((await stat(source)).isDirectory()) {
        // Stage the built distribution without running source-only build hooks or linking the source directory.
        await run("npm", ["pack", source, "--ignore-scripts", "--pack-destination", scratch], { timeout: 120_000, maxBuffer: 1024 * 1024, env: { ...process.env } });
        const archives = (await readdir(scratch)).filter(name => name.endsWith(".tgz"));
        if (archives.length !== 1) throw new HostStateError("package staging did not produce one archive");
        archive = join(scratch, archives[0]!);
      }
      await run("npm", ["install", "--prefix", scratch, "--no-package-lock", "--no-save", archive], { timeout: 120_000, maxBuffer: 1024 * 1024, env: { ...process.env } });
      const packagePath = join(scratch, "node_modules", "@axisosme", "axis"); const document = JSON.parse(await readFile(join(packagePath, "package.json"), "utf8")) as { name?: unknown; version?: unknown; bin?: unknown; engines?: { node?: unknown } };
      const bin = runtimeBin(role, runtime);
      if (document.name !== "@axisosme/axis" || typeof document.version !== "string" || document.engines?.node !== ">=24" || document.bin === null || typeof document.bin !== "object" || typeof (document.bin as Record<string, unknown>)[bin] !== "string") throw new HostStateError("staged package does not match Axis release contract");
      const binPath = (document.bin as Record<string, unknown>)[bin]; if (typeof binPath !== "string") throw new HostStateError("staged package executable is invalid");
      const executable = resolve(packagePath, binPath); if (!executable.startsWith(`${packagePath}/`) || !(await stat(executable)).isFile()) throw new HostStateError("staged package executable is invalid");
      const digest = await manifestDigest(join(scratch, "node_modules")); const releaseRoot = join(this.#root, "releases", digest.slice("sha256:".length)); const releaseNodeModules = join(releaseRoot, "node_modules"); const destination = join(releaseNodeModules, "@axisosme", "axis");
      try { await access(destination); if (await manifestDigest(releaseNodeModules) !== digest) throw new HostStateError("existing immutable release manifest mismatch"); await rm(scratch, { recursive: true, force: true }); } catch (error) { if (error instanceof HostStateError) throw error; await mkdir(releaseRoot, { recursive: true, mode: 0o700 }); await rename(join(scratch, "node_modules"), releaseNodeModules); this.#newRelease = releaseRoot; await rm(scratch, { recursive: true, force: true }); }
      return { packagePath: destination, version: document.version, manifestDigest: digest };
    } catch (error) { await rm(scratch, { recursive: true, force: true }); throw error; }
  }
  async verifyDrain(state: HostState, configuration: AxisConfiguration): Promise<void> {
    if (state.role === "node") {
      try {
        const config = deriveNodeConfiguration(configuration, state.configuration.nodeId!);
        if (resolve(config.database) !== resolve(lifecycleDatabasePaths(state, configuration)[0]!)) throw new HostStateError("Node database identity mismatch");
        const [node, worker] = await Promise.all([SqliteNodeJournal.inspectMaintenance(config.database), inspectNodeWorkerBlockers(config)]);
        if (node.mode !== "drain") throw new HostStateError("active work or unknown evidence prevents Node upgrade");
        await this.assertNodeUpgradeGate(state, configuration, { ...node.blockerCounts, ...worker });
        await this.#verifyStateRelease(state);
        return;
      } catch (error) {
        if (error instanceof HostStateError) throw error;
        throw new HostStateError("Node drain evidence is unavailable");
      }
    }
    if (state.role === "worker") {
      const release = { workerStuckRuns: await readWorkerStuckRunAcknowledgements(this.#root) };
      for (const candidate of lifecycleDatabasePaths(state, configuration)) assertLifecycleDatabaseDrained(candidate, release);
    } else {
      for (const candidate of lifecycleDatabasePaths(state, configuration)) assertLifecycleDatabaseDrained(candidate);
    }
    await this.#verifyStateRelease(state);
  }

  /**
   * The Node upgrade gate, at `transaction.ts`'s gate 1 (counts a running Node reported) and this
   * adapter's gate 2 (`raw` built from a stopped Node's journal and Worker evidence). `raw` is only
   * released here, never read.
   */
  async assertNodeUpgradeGate(state: HostState, configuration: AxisConfiguration, raw: NodeBlockerCounts): Promise<void> {
    if (state.role !== "node") throw new HostStateError("Node upgrade gate requires a Node host");
    const { rows, acks, released } = await this.#nodeReleases(state, configuration);
    const effective = effectiveNodeBlockers(raw, released);
    if (Object.values(effective).some((count) => count !== 0)) {
      const ackKeys = new Set(acks.map((ack) => `${ack.commandId}\u0000${ack.intentDigest}`));
      throw new NodeUpgradeBlockedError("active work or unknown evidence prevents Node upgrade", {
        blockers: effective,
        acknowledged: released,
        unacknowledgedAmbiguous: rows.filter((row) => !ackKeys.has(`${row.commandId}\u0000${row.intentDigest}`)).map((row) => row.commandId),
      });
    }
  }

  /** What the Node's own acknowledgement record and the linked Workers' records release. */
  async #nodeReleases(state: HostState, configuration: AxisConfiguration): Promise<{ readonly config: NodeRuntimeConfig; readonly rows: readonly NodeAmbiguousCommandEvidence[]; readonly resolved: readonly NodeResolvedAmbiguousCommand[]; readonly acks: readonly NodeAmbiguousAcknowledgementRecord[]; readonly released: NodeAmbiguousReleaseCounts }> {
    if (state.role !== "node") throw new HostStateError("Node upgrade gate requires a Node host");
    const config = deriveNodeConfiguration(configuration, state.configuration.nodeId!);
    const [acks, activeWorkerRuns] = await Promise.all([
      readNodeAmbiguousAcknowledgements(this.#root),
      releasedWorkerRuns(config),
    ]);
    const { ambiguous: rows, resolved } = await SqliteNodeJournal.inspectAmbiguousCommands(config.database, { acknowledgedCommandIds: new Set(acks.map((ack) => ack.commandId)) });
    const ackKeys = new Set(acks.map((ack) => `${ack.commandId}\u0000${ack.intentDigest}`));
    const ambiguousCommands = rows.filter((row) => ackKeys.has(`${row.commandId}\u0000${row.intentDigest}`)).length;
    return { config, rows, resolved, acks, released: { ambiguousCommands, activeWorkerRuns } };
  }

  /** The Node journal's ambiguous rows, their Worker-side evidence, both kinds of record, the Node gate status, and any acknowledged command that has since resolved on its own. */
  async listAmbiguousCommands(state: HostState, configuration: AxisConfiguration): Promise<NodeAmbiguousCommandsListing> {
    if (state.role !== "node") throw new HostStateError("ambiguous-commands list requires a Node host");
    const { config, rows, resolved, acks, released } = await this.#nodeReleases(state, configuration);
    const [node, worker] = await Promise.all([SqliteNodeJournal.inspectMaintenance(config.database), inspectNodeWorkerBlockers(config)]);
    const ackByCommand = new Map(acks.map((ack) => [ack.commandId, ack]));
    const ackByCommandAndDigest = new Map(acks.map((ack) => [`${ack.commandId}\u0000${ack.intentDigest}`, ack]));
    const workerEvidence = await inspectNodeWorkerRuns(config, rows.map((row) => ({ workerId: row.workerId, runId: row.runId ?? row.priorRunId ?? "", commandId: row.commandId })));
    const workerAcks = await Promise.all(rows.map((row) => this.#workerReleaseFor(config, row)));
    const ambiguousCommands: NodeAmbiguousCommandListingEntry[] = rows.map((row, index) => {
      const acknowledgement = ackByCommandAndDigest.get(`${row.commandId}\u0000${row.intentDigest}`) ?? null;
      const worker = workerEvidence[index]!;
      return { row, worker, acknowledgement, workerRelease: workerAcks[index]! };
    });
    const resolvedAfterAcknowledgement: NodeResolvedAmbiguousCommandListingEntry[] = resolved.map((entry) => ({
      commandId: entry.commandId, state: entry.state, acknowledgement: ackByCommand.get(entry.commandId) ?? null,
    }));
    const raw = { ...node.blockerCounts, ...worker };
    const effective = effectiveNodeBlockers(raw, released);
    const blocked = Object.values(effective).some((count) => count !== 0);
    return { ambiguousCommands, resolvedAfterAcknowledgement, upgradeGate: { blocked, blockers: effective, acknowledged: released } };
  }

  /**
   * The Worker-host release status for one ambiguous command's row: `none` (no acknowledgement yet),
   * `not_needed`/`not_configured` (the recorded acknowledgement's own `workerRelease`), `recorded` (a
   * matching Worker record exists and the Worker row is still in that state) or `stale` (the Worker
   * row has moved to a different stuck state).
   */
  async #workerReleaseFor(config: NodeRuntimeConfig, row: NodeAmbiguousCommandEvidence): Promise<"none" | "recorded" | "not_needed" | "not_configured" | "stale"> {
    const acks = await readNodeAmbiguousAcknowledgements(this.#root);
    const acknowledgement = acks.find((ack) => ack.commandId === row.commandId && ack.intentDigest === row.intentDigest);
    if (acknowledgement === undefined) return "none";
    if (acknowledgement.workerRelease !== "recorded") return acknowledgement.workerRelease;
    const runId = row.runId ?? row.priorRunId;
    if (runId === null) return "not_needed";
    const [evidence] = await inspectNodeWorkerRuns(config, [{ workerId: row.workerId, runId, commandId: row.commandId }]);
    if (evidence === undefined || (evidence.state !== "reserved" && evidence.state !== "observational")) return "recorded";
    const verified = await verifiedWorkerHostRoot(config, row.workerId);
    if (verified === null) return "recorded";
    const workerAcks = await readWorkerStuckRunAcknowledgements(verified.hostRoot);
    const matched = workerAcks.some((ack) => ack.runId === runId && ack.workerIntentDigest === evidence.intentDigest && ack.acknowledgedState === evidence.state);
    return matched ? "recorded" : "stale";
  }

  /**
   * Runs under the caller's lock, which covers the Node and every Worker host root on this machine.
   * Every field the two records carry, apart from the audit fields in `input`, comes from the journal
   * and Worker reads.
   */
  async acknowledgeAmbiguousCommand(state: HostState, configuration: AxisConfiguration, input: AcknowledgeAmbiguousCommandInput): Promise<AcknowledgeAmbiguousCommandResult> {
    if (state.role !== "node") throw new HostStateError("ambiguous-commands acknowledge requires a Node host");
    const { config, rows } = await this.#nodeReleases(state, configuration);
    const row = rows.find((candidate) => candidate.commandId === input.commandId);
    if (row === undefined) return { outcome: "not_ambiguous" };
    const now = this.#now().getTime();
    if (now - Date.parse(row.updatedAt) < AMBIGUOUS_ACKNOWLEDGEMENT_MIN_AGE_MS) return { outcome: "too_recent" };

    let workerRelease: "recorded" | "not_needed" | "not_configured" = "not_needed";
    let workerAcknowledgement: WorkerStuckRunAcknowledgement | null = null;
    let workerOutcome: "recorded" | "already_recorded" | "not_needed" | "not_configured" = "not_needed";
    // Only a start or continue has its own linked Worker run; a cancel or steer acts on `priorRunId`'s
    // run and gets no Worker-side release.
    const runId = row.operation === "start" || row.operation === "continue" ? row.runId : null;
    if (runId !== null) {
      const [evidence] = await inspectNodeWorkerRuns(config, [{ workerId: row.workerId, runId, commandId: row.commandId }]);
      if (evidence !== undefined && (evidence.state === "reserved" || evidence.state === "observational")) {
        if (config.workers.every((worker) => worker.target.identity.workerId !== row.workerId)) { workerRelease = "not_configured"; workerOutcome = "not_configured"; }
        else {
          const verified = await verifiedWorkerHostRoot(config, row.workerId);
          if (verified === null) return { outcome: "unavailable" };
          const appended = await appendWorkerStuckRunAcknowledgement(verified.hostRoot, {
            runId, workerIntentDigest: evidence.intentDigest ?? "", acknowledgedState: evidence.state,
            observationalCause: evidence.observationalCause,
            origin: { kind: "node_ambiguous_command", nodeId: config.nodeId, nodeCommandId: row.commandId },
            acknowledgedAt: this.#now().toISOString(),
            operatorRef: input.operatorRef, authorizationRef: input.authorizationRef, source: input.source, requestRef: input.requestRef, reason: input.reason ?? null,
          });
          const verifyRead = await readWorkerStuckRunAcknowledgements(verified.hostRoot);
          if (!verifyRead.some((ack) => ack.runId === appended.record.runId && ack.workerIntentDigest === appended.record.workerIntentDigest && ack.acknowledgedState === appended.record.acknowledgedState)) return { outcome: "unavailable" };
          workerRelease = "recorded"; workerAcknowledgement = appended.record; workerOutcome = appended.applied ? "recorded" : "already_recorded";
        }
      }
    }

    const appendedNode = await appendNodeAmbiguousAcknowledgement(this.#root, {
      commandId: row.commandId, intentDigest: row.intentDigest, operation: row.operation, runId: row.runId, priorRunId: row.priorRunId,
      commandCreatedAt: row.createdAt, workerRelease,
      acknowledgedAt: this.#now().toISOString(),
      operatorRef: input.operatorRef, authorizationRef: input.authorizationRef, source: input.source, requestRef: input.requestRef, reason: input.reason ?? null,
    });
    const verifyNode = await readNodeAmbiguousAcknowledgements(this.#root);
    if (!verifyNode.some((ack) => ack.commandId === appendedNode.record.commandId && ack.intentDigest === appendedNode.record.intentDigest)) return { outcome: "unavailable" };
    return {
      outcome: "acknowledged",
      node: appendedNode.applied ? "applied_and_verified" : "already_applied_and_verified",
      worker: workerOutcome,
      acknowledgement: appendedNode.record,
      workerAcknowledgement,
    };
  }
  async nodeMaintenance(state: HostState, configuration: AxisConfiguration, operation: "drain" | "resume" | "status"): Promise<NodeMaintenanceStatus> {
    if (state.role !== "node") throw new HostStateError("Node maintenance requires a Node host");
    const node = deriveNodeConfiguration(configuration, state.configuration.nodeId!);
    const token = node.token, ref = node.nodeRef, version = node.nodeVersion;
    const endpoint = localServiceUrl(node.eventHost, node.eventPort, "/v1/node/maintenance");
    try {
      const response = await fetch(endpoint, {
        method: operation === "status" ? "GET" : "POST",
        headers: operation === "status" ? { authorization: `Bearer ${token}` } : { authorization: `Bearer ${token}`, "content-type": "application/json" },
        ...(operation === "status" ? {} : { body: JSON.stringify({ contractVersion: NODE_CONTRACT_VERSION, mode: operation }) }),
        redirect: "error",
        signal: AbortSignal.timeout(3_000),
      });
      if (!response.ok) throw new Error("maintenance request failed");
      const status = nodeMaintenanceStatus(await response.json());
      if (status.nodeRef.ref !== ref || status.nodeRef.version !== version || (operation !== "status" && status.mode !== operation)) throw new Error("maintenance identity mismatch");
      return status;
    } catch { throw new HostStateError("Node maintenance evidence is unavailable"); }
  }
  async backup(state: HostState): Promise<void> {
    const directory = join(this.#backupRoot, `${Date.now()}-${state.revision}`); await mkdir(directory, { recursive: true, mode: 0o700 });
    await copyFile(this.#statePath, join(directory, "state.json")); await copyFile(state.managedPlist, join(directory, "service.plist"));
    const configurationBackup = join(directory, "axis.toml"); await copyFile(state.configuration.filename, configurationBackup);
    this.#databaseBackups = [{ source: state.configuration.filename, backup: configurationBackup }];
    this.#backupPath = directory; const config = await this.loadConfiguration(state.configuration); await this.#backupData(state, config);
  }
  async activate(state: HostState): Promise<void> {
    await this.#verifyStateRelease(state); if (state.managedPlist !== this.managedPlistPathFor(state.serviceLabel)) throw new HostStateError("unexpected managed plist path");
    await atomicWrite(this.#statePath, `${JSON.stringify(state)}\n`); await atomicWrite(state.managedPlist, this.#plist(state), 0o600);
    await run("plutil", ["-lint", state.managedPlist], { timeout: 30_000 }); await run("launchctl", ["bootstrap", `gui/${process.getuid?.() ?? 0}`, state.managedPlist], { timeout: 30_000 }); await run("launchctl", ["kickstart", "-k", `gui/${process.getuid?.() ?? 0}/${state.serviceLabel}`], { timeout: 30_000 });
  }
  async deactivate(): Promise<void> {
    const state = await this.readState(); if (state === null) throw new HostStateError("managed service state is unavailable");
    try { await run("launchctl", ["bootout", `gui/${process.getuid?.() ?? 0}`, state.managedPlist], { timeout: 30_000 }); }
    catch (error) { if ((error as { readonly code?: unknown }).code !== 113) throw new HostStateError("managed service could not be stopped"); }
  }
  async verify(state: HostState): Promise<void> { await this.#verifyRole(state, await this.loadConfiguration(state.configuration)); }
  async #verifyRole(state: HostState, configuration: AxisConfiguration): Promise<void> {
    const verification = lifecycleVerificationConfig(state, configuration);
    const resolveLocal = async (target: string): Promise<URL> => {
      let resolved: { readonly address: string }[];
      try { const endpoint = new URL(target); const hostname = endpoint.hostname.replace(/^\[|\]$/gu, ""); resolved = await lookup(hostname, { all: true, verbatim: true }); } catch { throw new HostStateError("service verification is not configured"); }
      const endpoint = localHealthEndpoint(target, resolved.map((entry) => entry.address), assignedHostAddresses());
      if (endpoint === null) throw new HostStateError("service verification is not configured");
      return endpoint;
    };
    const endpoint = await resolveLocal(verification.healthUrl);
    if (verification.nodeIdentity !== undefined) {
      if (verification.healthToken === null) throw new HostStateError("service verification is not configured");
      await verifyOwnedNodeReadiness({ endpoint, serviceLabel: state.serviceLabel, token: verification.healthToken, identity: verification.nodeIdentity }); return;
    }
    let healthy = false;
    for (let attempt = 0; attempt < SERVICE_READY_ATTEMPTS; attempt += 1) {
      try {
        const response = await fetch(endpoint, { ...(verification.healthToken === null ? {} : { headers: { authorization: `Bearer ${verification.healthToken}` } }), signal: AbortSignal.timeout(3_000) });
        if (response.ok) { healthy = true; break; }
      } catch { /* bounded readiness retry */ }
      if (attempt < SERVICE_READY_ATTEMPTS - 1) await delay(SERVICE_READY_RETRY_MS);
    }
    if (!healthy) throw new HostStateError("service health verification failed");
    const authEndpoint = await resolveLocal(verification.authUrl);
    try {
      const response = await fetch(authEndpoint, verification.authMethod === "GET" ? { method: "GET", signal: AbortSignal.timeout(3_000) } : { method: "POST", headers: { "content-type": "application/json" }, body: "{}", signal: AbortSignal.timeout(3_000) });
      if (response.status !== 401) throw new HostStateError("service authorization boundary verification failed");
    } catch (error) {
      if (error instanceof HostStateError) throw error;
      throw new HostStateError("service authorization boundary verification failed");
    }
  }
  async restore(): Promise<HostState> {
    if (this.#backupPath === null) throw new HostStateError("missing lifecycle backup");
    for (const database of this.#databaseBackups) { if (database.backup === null) await rm(database.source, { force: true }); else await copyFile(database.backup, database.source); }
    await copyFile(join(this.#backupPath, "state.json"), this.#statePath); const state = await this.readState(); if (state === null) throw new HostStateError("backup state invalid"); await copyFile(join(this.#backupPath, "service.plist"), state.managedPlist); return state;
  }
  async cleanupNew(): Promise<void> { const state = await this.readState(); await rm(this.#statePath, { force: true }); if (state !== null) await rm(state.managedPlist, { force: true }); if (this.#newRelease !== null) await rm(this.#newRelease, { recursive: true, force: true }); }
  async stopService(state: HostState): Promise<void> {
    // The pid is read BEFORE bootout: once launchd has unloaded the job it no longer reports which process ran it, and
    // "unloaded" alone does not mean that process has finished closing its databases.
    const before = await this.#launchdService(state.serviceLabel);
    await this.deactivate();
    const deadline = Date.now() + this.#stopTimeoutMs;
    for (;;) {
      const current = await this.#launchdService(state.serviceLabel);
      if (!current.loaded && (before.pid === null || !processExists(before.pid))) return;
      if (Date.now() >= deadline) throw new HostStateError("managed service did not stop in time");
      await delay(this.#stopPollMs);
    }
  }
  async #launchdService(label: string): Promise<{ readonly loaded: boolean; readonly pid: number | null }> {
    try {
      const { stdout } = await run("launchctl", ["print", `gui/${process.getuid?.() ?? 0}/${label}`], { timeout: 30_000, maxBuffer: 1024 * 1024 });
      const match = /^\s*pid = (\d+)\s*$/mu.exec(String(stdout));
      return { loaded: true, pid: match === null ? null : Number(match[1]) };
    } catch (error) {
      if ((error as { readonly code?: unknown }).code === 113) return { loaded: false, pid: null };
      throw new HostStateError("managed service cannot be inspected");
    }
  }
  async planForcedArchive(state: HostState, configuration: AxisConfiguration): Promise<ForcedArchive | null> {
    const storage = forcedStoragePaths(state, configuration);
    if (storage.length === 0) return null;
    const name = forcedArchiveName(this.#now()), directory = join(this.#archiveRoot, name);
    // Refused here, before anything stops: an archive is never overwritten or merged into.
    try { await lstat(directory); throw new HostStateError("forced upgrade archive directory already exists"); }
    catch (error) { if (error instanceof HostStateError) throw error; if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new HostStateError("forced upgrade archive directory cannot be inspected"); }
    // A rename cannot cross volumes, and a copy-then-delete is not a move, so storage on another volume is refused
    // before the service stops rather than half-moved after.
    const archiveDevice = (await stat(await existingAncestorDirectory(directory))).dev;
    const files: ForcedArchiveFile[] = [];
    for (const { key, path } of storage) {
      let storageDevice: number;
      try { storageDevice = (await stat(dirname(path))).dev; } catch { throw new HostStateError("forced upgrade storage directory is unavailable"); }
      if (storageDevice !== archiveDevice) throw new HostStateError("forced upgrade archive must be on the same volume as the role storage");
      for (const suffix of FORCED_STORAGE_SUFFIXES) {
        const metadata = await forcedStorageFile(`${path}${suffix}`);
        if (metadata !== null) files.push({ name: `${key}${suffix}`, bytes: metadata.size });
      }
    }
    return { directory: `archive/${name}`, files };
  }
  async archiveForcedStorage(state: HostState, configuration: AxisConfiguration, plan: ForcedArchive, target: StagedRelease): Promise<ForcedArchive> {
    const directory = resolve(this.#root, plan.directory);
    if (dirname(directory) !== this.#archiveRoot) throw new ForcedUpgradeError("archive", null);
    // `mkdir` without `recursive` on the archive itself: an existing directory is refused, never reused.
    try { await mkdir(this.#archiveRoot, { recursive: true, mode: 0o700 }); await mkdir(directory, { mode: 0o700 }); }
    catch { throw new ForcedUpgradeError("archive", null); }
    const moved: ForcedArchiveFile[] = [];
    // Local evidence for the operator, next to the files: what was moved for which role and target release.
    const manifest = (complete: boolean): string => `${JSON.stringify({ schemaVersion: FORCED_ARCHIVE_MANIFEST_VERSION, role: state.role, fromVersion: state.activeRelease.version, targetVersion: target.version, targetManifestDigest: target.manifestDigest, complete, files: moved })}\n`;
    try {
      await writeFile(join(directory, FORCED_ARCHIVE_MANIFEST), manifest(false), { mode: 0o600, flag: "wx" });
      for (const { key, path } of forcedStoragePaths(state, configuration)) for (const suffix of FORCED_STORAGE_SUFFIXES) {
        const source = `${path}${suffix}`, metadata = await forcedStorageFile(source);
        if (metadata === null) continue;
        await rename(source, join(directory, `${key}${suffix}`));
        moved.push({ name: `${key}${suffix}`, bytes: metadata.size });
      }
      await atomicWrite(join(directory, FORCED_ARCHIVE_MANIFEST), manifest(true));
    } catch { throw new ForcedUpgradeError("archive", { directory: plan.directory, files: [...moved] }); }
    return { directory: plan.directory, files: [...moved] };
  }
  async #backupData(state: HostState, configuration: AxisConfiguration): Promise<void> {
    if (this.#backupPath === null) throw new HostStateError("missing lifecycle backup");
    for (const database of lifecycleDatabasePaths(state, configuration)) for (const suffix of ["", "-wal", "-shm"]) {
      const source = `${database}${suffix}`, backup = join(this.#backupPath, `${sha(source).slice(7)}${suffix}`);
      try { await copyFile(source, backup); this.#databaseBackups.push({ source, backup }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; this.#databaseBackups.push({ source, backup: null }); }
    }
    const keys = new Set<string>();
    const readKey = (name: string): string => { keys.add(secretPath(configuration, name)); return readAxisSecret(configuration, name); };
    // External OAuth credentials are not files; only the role's generated keys belong in its backup.
    if (state.role === "controller") deriveRelayConfiguration(configuration, readKey, { clientSecret: "backup-only", webhookSecret: "backup-only" });
    else if (state.role === "node") deriveNodeConfiguration(configuration, state.configuration.nodeId!, readKey);
    else deriveWorkerConfiguration(configuration, state.configuration.workerId!, state.configuration.nodeId!, readKey);
    const files = [...keys,
      ...(state.role === "controller" ? [databasePaths(configuration).vault] : []),
      ...(state.role === "worker" ? (() => {
        const worker = deriveWorkerConfiguration(configuration, state.configuration.workerId!, state.configuration.nodeId!);
        return worker.nativeSessionStorePath === undefined ? [] : [worker.nativeSessionStorePath];
      })() : [])];
    for (const source of files) {
      const backup = join(this.#backupPath, `${sha(source).slice(7)}.file`);
      try { await copyFile(source, backup); await chmod(backup, 0o600); this.#databaseBackups.push({ source, backup }); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; this.#databaseBackups.push({ source, backup: null }); }
    }
  }
  #plist(state: HostState): string {
    const runner = managedRunnerPath(state);
    return `<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n<plist version="1.0"><dict><!-- axis.host.v1 managed -->\n<key>Label</key><string>${xml(state.serviceLabel)}</string>\n<key>ProgramArguments</key><array><string>${xml(this.#node)}</string><string>${xml(runner)}</string><string>--state</string><string>${xml(this.#statePath)}</string></array>\n<key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>\n`;
  }
  async #verifyStateRelease(state: HostState): Promise<void> {
    if (!state.activeRelease.packagePath.startsWith(`${this.#root}/releases/`)) throw new HostStateError("release is outside host root"); const current = await manifestDigest(dirname(dirname(state.activeRelease.packagePath))); if (current !== state.activeRelease.manifestDigest) throw new HostStateError("release manifest mismatch");
  }
}
