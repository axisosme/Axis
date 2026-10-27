import { pathToFileURL } from "node:url";

import { nodeMaintenanceStatus, parseNodeJson } from "../src/shared/router-node.js";
import { createHash } from "node:crypto";
import { parseAxisConfiguration, deriveRelayConfiguration, deriveNodeConfiguration, deriveWorkerConfiguration } from "../src/configuration.js";
import { validateCodexAppServerReadiness } from "../src/node/readiness.js";

const CONTRACT_VERSION = "axis.node.live-smoke.receipt.v1";
const RESPONSE_LIMIT_BYTES = 128 * 1024;

export const placeholderConfiguration = `schemaVersion = 1
dataDir = "/opt/example/axis/data"

[relay]
port = 4098
url = "https://controller.example.invalid/"

[router]
port = 4099
defaultWorker = "worker-a"

[linear]
clientId = "REPLACE_WITH_LINEAR_CLIENT_ID"
redirectUri = "https://controller.example.invalid/oauth/linear/callback"

[[nodes]]
id = "node-placeholder"
version = "command"
port = 4100
eventPort = 4102
url = "https://node.example.invalid/"
defaultWorker = "worker-a"

[[nodes.workers]]
id = "worker-a"
kind = "codex"
executorId = "codex-a"
port = 4101
hostRoot = "/opt/example/axis/hosts/worker-a"
profileFallbackPolicy = "deny"
workspaces = [{ organizationId = "REPLACE_WITH_LINEAR_ORGANIZATION_ID", workspaceRoot = "/opt/example/workspace-a" }]

[[nodes.workers]]
id = "worker-b"
kind = "claude-code"
executorId = "claude-b"
port = 4111
hostRoot = "/opt/example/axis/hosts/worker-b"
claudeExecutable = "/opt/example/bin/claude"
workspaces = [{ organizationId = "REPLACE_WITH_LINEAR_ORGANIZATION_ID", workspaceRoot = "/opt/example/workspace-b" }]
`;

export const placeholderInstallation = Object.freeze({
  controller: Object.freeze({ hostRoot: "/opt/example/axis/hosts/controller", serviceLabel: "com.example.axis-controller" }),
  node: Object.freeze({ hostRoot: "/opt/example/axis/hosts/node", serviceLabel: "com.example.axis-node" }),
  workerA: Object.freeze({ hostRoot: "/opt/example/axis/hosts/worker-a", serviceLabel: "com.example.axis-worker-a" }),
  workerB: Object.freeze({ hostRoot: "/opt/example/axis/hosts/worker-b", serviceLabel: "com.example.axis-worker-b" }),
});

export function placeholderRuntimeConfigurations() {
  const config = parseAxisConfiguration(placeholderConfiguration, "/opt/example/axis/axis.toml");
  const read = name => createHash("sha256").update(name).digest("base64");
  return { controller: deriveRelayConfiguration(config, read, { clientSecret: "fixture-client-secret", webhookSecret: "fixture-webhook-secret" }),
    node: deriveNodeConfiguration(config, undefined, read),
    first: deriveWorkerConfiguration(config, "worker-a", undefined, read),
    second: deriveWorkerConfiguration(config, "worker-b", undefined, read) };
}

function requireCondition(condition) {
  if (!condition) throw new Error("placeholder configuration mismatch");
}

export function validatePlaceholderInstallation() {
  const { controller, node, first, second } = placeholderRuntimeConfigurations();
  const roots = Object.values(placeholderInstallation).map(({ hostRoot }) => hostRoot);
  const labels = Object.values(placeholderInstallation).map(({ serviceLabel }) => serviceLabel);
  const databases = [...Object.values(controller.linear.databases), controller.runtime.routerDatabase, node.database, first.database, second.database];
  requireCondition(new Set(roots).size === roots.length && new Set(labels).size === labels.length && new Set(databases).size === databases.length);
  const nodeEndpoint = controller.runtime.nodeEndpoints[0];
  requireCondition(controller.runtime.nodeEndpoints.length === 1 && controller.runtime.nodeTargets.length === 2 && node.workers.length === 2);
  requireCondition(nodeEndpoint?.bearerToken === node.token);
  requireCondition(node.workers[0]?.commandToken === first.token && node.workers[0]?.eventToken === first.routerToken);
  requireCondition(node.workers[1]?.commandToken === second.token && node.workers[1]?.eventToken === second.routerToken);
  requireCondition(first.routerEndpoint === "http://127.0.0.1:4102/" && second.routerEndpoint === first.routerEndpoint);
  requireCondition(controller.eventBindings.length === 1 && controller.eventBindings[0]?.bearerToken === node.routerEventToken);
  requireCondition(node.defaultWorkerKey === node.workers[0]?.workerKey);
  const authorities = [node.token, node.routerEventToken, first.token, first.routerToken, second.token, second.routerToken];
  requireCondition(new Set(authorities).size === authorities.length);
  return { services: 4, workers: node.workers.length, distinctHostRoots: roots.length, distinctDatabases: databases.length, distinctAuthorities: authorities.length };
}

function check(name, status, safeCode, evidence = undefined) {
  return { name, status, safeCode, ...(evidence === undefined ? {} : { evidence }) };
}

function executionScenarioChecks(safeCode) {
  return [
    check("two_independent_starts", "unverified", safeCode),
    check("supported_continue", "unverified", safeCode),
    check("supported_cancel", "unverified", safeCode),
    check("event_return", "unverified", safeCode),
    check("rollback", "unverified", safeCode),
  ];
}

function configurationReceipt() {
  const evidence = validatePlaceholderInstallation();
  return {
    contractVersion: CONTRACT_VERSION,
    mode: "configuration-only",
    result: "unverified",
    mutationAttempted: false,
    checks: [
      check("placeholder_configuration", "passed", "axis.node.smoke.configuration_valid", evidence),
      ...executionScenarioChecks("axis.node.smoke.live_execution_not_requested"),
    ],
  };
}

function exactObject(value, keys) {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    && Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}

function validIdentifier(value) {
  return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(value);
}

function numericLoopback(hostname) {
  if (hostname === "[::1]" || hostname === "::1") return true;
  const octets = hostname.split(".");
  return octets.length === 4 && octets[0] === "127" && octets.every((octet) => /^(?:0|[1-9][0-9]{0,2})$/u.test(octet) && Number(octet) <= 255);
}

function liveOrigin(value, eventListener) {
  const parsed = new URL(value);
  if (parsed.username !== "" || parsed.password !== "" || parsed.pathname !== "/" || parsed.search !== "" || parsed.hash !== "") throw new Error("invalid live origin");
  const loopbackHttp = parsed.protocol === "http:" && numericLoopback(parsed.hostname);
  if ((eventListener && !loopbackHttp) || (!eventListener && parsed.protocol !== "https:" && !loopbackHttp)) throw new Error("invalid live origin");
  return parsed.origin;
}

function parseArguments(args) {
  if (args.length === 0) return { mode: "configuration-only" };
  if (args[0] !== "--live-read-only") throw new Error("unsupported live mode");
  const values = new Map();
  for (let index = 1; index < args.length; index += 2) {
    const name = args[index], value = args[index + 1];
    if (!name?.startsWith("--") || value === undefined || values.has(name)) throw new Error("invalid live arguments");
    values.set(name, value);
  }
  const allowed = ["--command-origin", "--event-origin", "--node-ref", "--node-version", "--reviewed-commit"];
  if (values.size !== allowed.length || allowed.some((name) => !values.has(name)) || [...values.keys()].some((name) => !allowed.includes(name))) throw new Error("invalid live arguments");
  const reviewedCommit = values.get("--reviewed-commit");
  if (!/^[0-9a-f]{40}$/u.test(reviewedCommit) || !validIdentifier(values.get("--node-ref")) || !validIdentifier(values.get("--node-version"))) throw new Error("invalid live arguments");
  return {
    mode: "live-read-only",
    commandOrigin: liveOrigin(values.get("--command-origin"), false),
    eventOrigin: liveOrigin(values.get("--event-origin"), true),
    nodeRef: { ref: values.get("--node-ref"), version: values.get("--node-version") },
    reviewedCommit,
  };
}

async function boundedJson(response) {
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > RESPONSE_LIMIT_BYTES) throw new Error("live response too large");
  return parseNodeJson(bytes);
}

function validateReadiness(value, expectedRef) {
  if (!exactObject(value, ["contractVersion", "nodeRef", "observedAt", "service", "storage", "workers"])
    || value.contractVersion !== "1.0.0" || !exactObject(value.nodeRef, ["ref", "version"])
    || value.nodeRef.ref !== expectedRef.ref || value.nodeRef.version !== expectedRef.version
    || !exactObject(value.service, ["status"]) || value.service.status !== "configured"
    || !exactObject(value.storage, ["status"]) || value.storage.status !== "available"
    || !Array.isArray(value.workers) || value.workers.length < 2 || !Number.isFinite(Date.parse(value.observedAt))) throw new Error("invalid readiness evidence");
  const workerIds = new Set();
  for (const worker of value.workers) {
    if (!exactObject(worker, ["workerId", "transport", "readinessSupport", "readiness"])
      || !validIdentifier(worker.workerId) || worker.transport !== "reachable" || worker.readinessSupport !== "supported"
      || validateCodexAppServerReadiness(worker.readiness)?.status !== "ready") throw new Error("invalid readiness evidence");
    workerIds.add(worker.workerId);
  }
  if (workerIds.size !== value.workers.length) throw new Error("invalid readiness evidence");
  return { workerCount: value.workers.length, reachableWorkers: value.workers.length, storage: "available" };
}

async function liveReadOnly(input, env, fetchImpl) {
  if (typeof env.AXIS_NODE_LIVE_TOKEN !== "string" || env.AXIS_NODE_LIVE_TOKEN.length === 0 || env.AXIS_NODE_LIVE_TOKEN.startsWith("REPLACE_WITH_")) {
    throw new Error("live authorization missing");
  }
  const request = (url, token) => fetchImpl(url, {
    method: "GET",
    headers: token === undefined ? {} : { authorization: `Bearer ${token}` },
    redirect: "error",
    signal: AbortSignal.timeout(5_000),
  });
  const unauthenticated = await request(`${input.commandOrigin}/v1/node/readiness`);
  if (unauthenticated.status !== 401) throw new Error("unauthenticated boundary mismatch");
  await unauthenticated.body?.cancel();
  const readinessResponse = await request(`${input.commandOrigin}/v1/node/readiness`, env.AXIS_NODE_LIVE_TOKEN);
  if (readinessResponse.status !== 200) throw new Error("readiness unavailable");
  const readiness = validateReadiness(await boundedJson(readinessResponse), input.nodeRef);
  const maintenanceResponse = await request(`${input.eventOrigin}/v1/node/maintenance`, env.AXIS_NODE_LIVE_TOKEN);
  if (maintenanceResponse.status !== 200) throw new Error("maintenance unavailable");
  const maintenance = nodeMaintenanceStatus(await boundedJson(maintenanceResponse));
  if (maintenance.nodeRef.ref !== input.nodeRef.ref || maintenance.nodeRef.version !== input.nodeRef.version) throw new Error("maintenance identity mismatch");
  return {
    contractVersion: CONTRACT_VERSION,
    mode: "live-read-only",
    result: "unverified",
    mutationAttempted: false,
    reviewedCommit: input.reviewedCommit,
    checks: [
      check("placeholder_configuration", "passed", "axis.node.smoke.configuration_valid", validatePlaceholderInstallation()),
      check("unauthenticated_command_boundary", "passed", "axis.node.smoke.unauthorized_as_expected"),
      check("authenticated_readiness", "passed", "axis.node.smoke.readiness_valid", readiness),
      check("maintenance_status", "passed", "axis.node.smoke.maintenance_valid", { mode: maintenance.mode, idle: maintenance.idle, blockerCounts: maintenance.blockerCounts }),
      check("reviewed_build_at_target", "unverified", "axis.node.smoke.build_attestation_required"),
      ...executionScenarioChecks("axis.node.smoke.operator_execution_required"),
    ],
  };
}

function failedReceipt(mode, safeCode) {
  const scenarioCode = mode === "live-read-only" ? "axis.node.smoke.operator_execution_required" : "axis.node.smoke.live_execution_not_requested";
  return { contractVersion: CONTRACT_VERSION, mode, result: "failed", mutationAttempted: false, checks: [check("preflight", "failed", safeCode), ...executionScenarioChecks(scenarioCode)] };
}

export async function runLiveNodeSmoke(options = {}) {
  const args = options.args ?? [];
  const env = options.env ?? process.env;
  const fetchImpl = options.fetch ?? globalThis.fetch;
  let input;
  try {
    input = parseArguments(args);
  } catch {
    return { exitCode: 2, receipt: failedReceipt("refused", "axis.node.smoke.invalid_arguments") };
  }
  if (input.mode === "configuration-only") {
    try { return { exitCode: 0, receipt: configurationReceipt() }; }
    catch { return { exitCode: 1, receipt: failedReceipt(input.mode, "axis.node.smoke.configuration_invalid") }; }
  }
  try {
    return { exitCode: 0, receipt: await liveReadOnly(input, env, fetchImpl) };
  } catch {
    return { exitCode: 1, receipt: failedReceipt(input.mode, "axis.node.smoke.live_read_failed") };
  }
}

const entry = process.argv[1];
if (entry !== undefined && import.meta.url === pathToFileURL(entry).href) {
  const outcome = await runLiveNodeSmoke({ args: process.argv.slice(2) });
  process.stdout.write(`${JSON.stringify(outcome.receipt)}\n`);
  process.exitCode = outcome.exitCode;
}
