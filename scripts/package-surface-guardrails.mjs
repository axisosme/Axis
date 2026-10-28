import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { access, readFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const sourceRoot = dirname(dirname(fileURLToPath(import.meta.url)));

const activeDistBases = [
  "configuration", "configuration-cli", "init", "init-cli", "init-prompts", "serve", "serve-cli", "axis-cli", "bin/axis", "bin/linear-cli", "bin/claude-worker", "bin/relay", "bin/worker",
  "index", "executor/response-guidance", "executor/claude-code", "executor/codex",
  "lifecycle/configuration", "lifecycle/cli", "lifecycle/host-state", "lifecycle/macos-adapter", "lifecycle/runner", "lifecycle/transaction", "lifecycle/stuck-run-acknowledgements",
  "relay/comment-invocation", "relay/errors",
  "relay/http", "relay/linear/payload", "relay/linear/provider",
  "relay/oauth", "relay/runtime", "relay/runtime-config", "relay/security",
  "relay/types", "router/binding-resolver", "router/channel-command", "router/channel-identity", "router/channel-conversation-types",
  "router/execution-profile", "router/channel-conversation", "router/channel-run-chain", "router/http",
  "router/router", "router/task-identity", "router/team-bindings",
  "relay/scheduled-loop",
  "router/node-port", "node/event-ingress", "node/http", "node/http-transport", "node/journal", "node/maintenance", "node/readiness", "node/worker-client",
  "node/command", "node/router-event-delivery", "node/event-pump", "shared/runtime-boundary", "shared/elicitation",
  "shared/router-node", "shared/router-node-commands", "shared/worker-selection", "shared/node-routing-projection", "shared/router-relay-event", "shared/router-worker", "shared/worker-executor", "node/runtime-config", "worker/claude-runtime",
  "worker/command", "worker/schema", "worker/http", "worker/router-event-sink", "worker/runtime", "worker/runtime-config",
  "worker/workspace-policy", "bin/node", "bin/routing-preflight",
  "node/runtime",
  "relay/linear/ingress", "relay/linear/inbox",
  "relay/linear/installation", "relay/linear/authorization", "relay/linear/execution-access", "relay/linear/execution-access-contract", "relay/linear/instruction",
  "relay/linear/api-proxy", "relay/linear/api-runtime", "relay/linear/cli-access",
  "relay/linear/execution-runtime", "shared/linear-execution-client", "shared/linear-sdk", "worker/linear-codex-executor",
  "relay/linear/activity", "relay/linear/delivery",
  "relay/channel-outbox", "relay/channel-egress",
  "shared/schema-ledger",
  "shared/durable-store",
  "shared/retry-budget",
  "relay/linear/observe", "relay/linear/redispatch",
  "relay/linear/channel", "relay/linear/ledgers",
  "relay/linear/composition", "relay/linear/runtime-config", "relay/node-routing-composition",
  "relay/integrity-cadence"
];

export const activeDistArtifacts = activeDistBases.flatMap((base) => [
  `dist/${base}.js`, `dist/${base}.d.ts`, `dist/${base}.js.map`,
]);

const activeRuntimeExports = [
  "AxisConfigurationError", "parseAxisConfiguration", "loadAxisConfiguration", "databasePaths",
  "deriveRelayConfiguration", "deriveNodeConfiguration", "deriveWorkerConfiguration",
  "initializeAxis", "generateAxisSecrets", "secretNames", "secretPath", "readAxisSecret", "readLinearSecrets", "selectedNode", "selectedWorker",
  "CodexExecutor", "codexExecutorRegistration", "PackageLocalCodexAppServerTransport", "CODEX_EXECUTOR_CONTRACT_VERSION", "executorError",
  "RelayError", "safeError",
  "startRelayHttpService",
  "Router", "RouterError", "ROUTER_VERSION", "SqliteTeamBindingStore", "TeamBindingError", "TEAM_BINDING_CONTRACT_VERSION",
  "RouterRelayEventError",
  "RouterHttpEventSink", "RouterHttpTransportError", "WORKER_COMMAND_VERSION",
  "RouterEventRefusedError", "WORKER_OUTBOX_REFUSED_CODE", "WORKER_OUTBOX_UNPROCESSABLE_CODE",
  "ELICITATION_CONTRACT_VERSION", "ELICITATION_OTHER_CONTRACT_VERSION", "deterministicElicitationOtherOption", "deterministicElicitationIds", "deterministicElicitationReplyValue", "elicitationDigest", "elicitationFromCodexRequest", "renderElicitationMarkdown", "selectElicitationOption", "validateCanonicalLinearUserUrl", "validateStructuredElicitation",
  "startWorkerHttpService", "SqliteWorkerCommandJournal", "WORKER_WORKSPACE_RESOLUTION_AUDIT_VERSION", "WorkerCommandError", "WorkerCommandService",
  "packageLocalCodexExecutable", "startCodexWorker", "startClaudeWorker", "startNode", "nodeStartupDiagnostics", "startRelay", "composeLinearChannel", "startServe", "validateServeConfiguration",
  "parseLinearWebhook", "verifyPayloadTimestamp", "LinearGraphQlProvider", "LinearOAuthManager",
  "EncryptedFileCredentialVault", "EnvelopeCipher", "redactSecrets", "sha256", "verifyLinearWebhook",
];

const activeTypes = [
  "ServeRuntime", "ServeOptions", "ServeWorkerRuntime",
  "AxisConfiguration", "AxisNode", "AxisWorker", "RelayConfiguration", "NodeConfiguration", "WorkerConfiguration", "LinearSecrets", "SecretReader", "NodeRuntime", "NodeRuntimeFactories", "RelayRuntime", "RelayRuntimeOptions", "LinearChannel", "LinearChannelOptions", "LinearRuntimeConfig",
  "RelayHttpOptions", "RelayHttpService",
  "RouterOptions", "RouterDefaultTeamBinding", "RouterTeamBindingResolver", "RouterTaskRef", "RouterTaskEvent", "SqliteTeamBindingStoreOptions", "TeamBindingMutationAudit", "TeamBindingErrorCode", "ExecutionProfile", "TeamBindingVersionedRef", "TeamBindingResolution", "TeamBindingRevision", "TeamBindingScope", "TeamBindingValidation", "TeamBindingValidator", "TeamBindingValidatorInvalid", "TeamBindingValidatorResult", "TeamBindingValidatorValid",
  "RouterRunEvent", "RouterEventReceipt", "RouterStartInput", "RouterContinueInput", "RouterCancelInput", "RouterReadRunTargetInput", "RouterRunTargetEvidence", "RouterCommandResult", "RouterErrorCode", "RouterListedEvent", "WorkerExecutionIdentity", "WorkerVersionedReference",
  "RouterHttpEventSinkOptions", "RouterHttpOptions", "RouterHttpService",
  "WorkerHttpOptions", "WorkerHttpService", "WorkerCommand", "WorkerCommandContractVersion", "WorkerCommandStart", "WorkerCommandContinue", "WorkerCommandCancel", "WorkerCommandSteer", "WorkerIngressCommand", "WorkerIngressResponse", "WorkerSteeredResponse",
  "WorkerCommandErrorCode", "WorkerCommandOptions", "WorkerCommandResponse", "WorkerCommandJournal", "WorkerCommandJournalClaim", "WorkerCommandRecord",
  "WorkerCommandOutboxEvent", "WorkerCommandRunClaim", "WorkerCommandRunRecord", "WorkerWorkspaceResolutionAudit", "RouterRunEventSink", "RouterRunLifecycleEnvelope", "WorkerRunWaitingForInputEnvelope", "WorkerRunConfiguration", "WorkerWorkspaceCatalogEntry", "WorkerWorkspaceConfiguration", "ElicitationOption", "ElicitationOtherOption", "StructuredElicitation",
  "RelayRuntimeConfig", "WorkerRuntimeConfig", "AppServerSpawnOptions", "CodexLiveBinding", "Executor", "CodexExecutorOptions",
  "CodexAppServerTransport", "CodexAppServerStartTurnInput", "CodexAppServerContinueTurnInput", "CodexAppServerTurnLifecycleEvent",
  "PackageLocalCodexAppServerTransportOptions", "ExecutorAcceptedResult", "ExecutorCancelRequest", "ExecutorCancelResult", "ExecutorSteerRequest", "ExecutorSteerResult", "ExecutorSteeredResult",
  "ExecutorContinueRequest", "ExecutorContinueResult", "ExecutorErrorCode", "ExecutorError", "ExecutorFailedResult", "ExecutorTerminalReattestationRequest", "ExecutorTerminalReattestationResult", "ExecutorRecoveryRequest", "ExecutorRecoveryResult",
  "ExecutorInterruptRequestedResult", "ExecutorLifecycleEvent", "ExecutorLifecycleOutcome", "ExecutorLiveBinding", "ExecutorOperationRequest", "CodexExecutorOperation",
  "ExecutorStartRequest", "ExecutorStartResult", "ExecutorCapability", "ExecutorRegistration", "ExecutorSelectionInput", "AxisSafeLifecycleProjection", "RelayErrorCode",
  "ParsedWebhookPayload", "UnsupportedWebhookPayload", "ImmutableLinearActivity", "ImmutableLinearComment", "ImmutableLinearIssue", "VerifiedLinearUser", "IssueExecutionContext", "LinearProviderPort",
  "ProviderCreateResult", "ProviderReconcileResult", "LinearCredentialAccess", "LinearOAuthOptions",
  "AgentActivitySnapshot", "AgentSessionEventPayload", "AgentSessionSnapshot", "CredentialVault", "InboxState", "LinearActivityContent", "LinearHeaders",
  "OAuthRevocationPayload", "OutboxRecord", "OutboxState", "PermissionChangePayload", "SupportedWebhookPayload",
  "WorkerOutboxQuarantineCode", "WorkerOutboxQuarantineDisposition", "WorkerQuarantinedOutboxEvent",
  "WorkerResponseProjectionDiagnostic", "WorkerResponseProjectionRefusal",
];

const maximumIndexSourceLength = 1_000_000;

function sortedInventory(values, label) {
  assert.equal(new Set(values).size, values.length, `${label} contains duplicate names`);
  return [...values].sort();
}

function lexicalTokens(source, label) {
  assert.ok(source.length <= maximumIndexSourceLength, `${label} exceeds the parser limit`);
  const tokens = [];
  for (let index = 0; index < source.length;) {
    const character = source[index];
    if (character === undefined) break;
    if (/\s/u.test(character)) { index += 1; continue; }
    if (character === "/" && source[index + 1] === "/") {
      index = source.indexOf("\n", index + 2); if (index < 0) break; continue;
    }
    if (character === "/" && source[index + 1] === "*") {
      const end = source.indexOf("*/", index + 2); assert.notEqual(end, -1, `${label} has an unterminated block comment`); index = end + 2; continue;
    }
    if (character === "\"" || character === "'" || character === "`") {
      const quote = character; index += 1;
      while (index < source.length && source[index] !== quote) {
        if (source[index] === "\\") index += 1;
        index += 1;
      }
      assert.ok(index < source.length, `${label} has an unterminated string`);
      index += 1; tokens.push({ kind: "string", value: "string" }); continue;
    }
    if (/[A-Za-z_$]/u.test(character)) {
      const start = index; index += 1;
      while (index < source.length && /[A-Za-z0-9_$]/u.test(source[index] ?? "")) index += 1;
      tokens.push({ kind: "word", value: source.slice(start, index) }); continue;
    }
    if (["{", "}", ",", ";"].includes(character)) { tokens.push({ kind: character, value: character }); index += 1; continue; }
    throw new Error(`${label} uses unsupported token ${JSON.stringify(character)}; update the named-export parser deliberately.`);
  }
  return tokens;
}

function sourceIndexSurface() {
  const label = "src/index.ts";
  const tokens = lexicalTokens(readFileSync(join(sourceRoot, "src", "index.ts"), "utf8"), label);
  const runtime = [];
  const types = [];
  let cursor = 0;
  const peek = () => tokens[cursor];
  const take = () => tokens[cursor++];
  const expect = (value) => {
    const token = take();
    assert.equal(token?.value, value, `${label} must use named export lists; expected ${value}`);
  };
  const name = () => {
    const token = take();
    assert.equal(token?.kind, "word", `${label} expected an exported identifier`);
    return token.value;
  };
  while (cursor < tokens.length) {
    expect("export");
    const declarationTypeOnly = peek()?.value === "type";
    if (declarationTypeOnly) take();
    expect("{");
    for (;;) {
      const specifierTypeOnly = peek()?.value === "type";
      if (specifierTypeOnly) take();
      const localName = name();
      const exportedName = peek()?.value === "as" ? (take(), name()) : localName;
      (declarationTypeOnly || specifierTypeOnly ? types : runtime).push(exportedName);
      if (peek()?.kind === ",") { take(); if (peek()?.kind === "}") { take(); break; } continue; }
      expect("}"); break;
    }
    expect("from");
    assert.equal(take()?.kind, "string", `${label} expected a module specifier`);
    if (peek()?.kind === ";") take();
  }
  return { runtime: sortedInventory(runtime, `${label} runtime exports`), types: sortedInventory(types, `${label} type exports`) };
}

function filesUnder(directory) {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...filesUnder(path));
    else if (entry.isFile()) files.push(path);
  }
  return files;
}

function sourceEmittedArtifacts() {
  const sourceDirectory = join(sourceRoot, "src");
  return filesUnder(sourceDirectory)
    .filter((path) => path.endsWith(".ts") && !path.endsWith(".d.ts"))
    .flatMap((path) => {
      const base = relative(sourceDirectory, path).replaceAll("\\", "/").slice(0, -3);
      return [`dist/${base}.js`, `dist/${base}.d.ts`, `dist/${base}.js.map`];
    });
}

function emittedArtifactsIn(directory) {
  return filesUnder(directory)
    .map((path) => relative(dirname(directory), path).replaceAll("\\", "/"))
    .filter((path) => path.endsWith(".js") || path.endsWith(".d.ts") || path.endsWith(".js.map"));
}

function assertExactArtifacts(actual, label) {
  assert.deepEqual(sortedInventory(actual, `${label} artifacts`), sortedInventory(activeDistArtifacts, "activeDistArtifacts"), `${label} artifacts drifted from the active source inventory`);
}

export function assertDeclaredSurface() {
  const source = sourceIndexSurface();
  assert.deepEqual(sortedInventory(activeRuntimeExports, "activeRuntimeExports"), source.runtime, "activeRuntimeExports must exactly match src/index.ts value exports");
  assert.deepEqual(sortedInventory(activeTypes, "activeTypes"), source.types, "activeTypes must exactly match src/index.ts type exports");
  assertExactArtifacts(sourceEmittedArtifacts(), "src");
}

export function assertCurrentBuildArtifactInventory() {
  assertDeclaredSurface();
  assertExactArtifacts(emittedArtifactsIn(join(sourceRoot, "dist")), "current build");
}

export function assertPackageManifest(pkg) {
  assert.equal(pkg.version, "1.0.0");
  assert.equal(pkg.private, true);
  assert.equal(pkg.scripts?.prepare, "npm run build");
  assert.equal(pkg.scripts?.prepack, "npm run build");
  assert.deepEqual(pkg.workspaces, ["packages/contracts"]);
  assert.deepEqual(pkg.bundleDependencies, ["@axisosme/axis-contracts"]);
  assert.ok(pkg.files.includes("LICENSE"), "package files must include LICENSE");
  assert.deepEqual(pkg.files, ["dist", "axis.example.toml", "docs", "README.md", "CHANGELOG.md", "openapi.yaml", "LICENSE"], "package allowlist must keep repository agent instructions out of the package");
  assert.deepEqual(pkg.exports, { ".": { types: "./dist/index.d.ts", default: "./dist/index.js" }, "./linear": { types: "./dist/shared/linear-sdk.d.ts", default: "./dist/shared/linear-sdk.js" } });
  assert.deepEqual(pkg.bin, { "axis": "./dist/bin/axis.js", "axis-linear-cli": "./dist/bin/linear-cli.js", "axis-claude-worker": "./dist/bin/claude-worker.js", "axis-linear-relay": "./dist/bin/relay.js", "axis-codex-worker": "./dist/bin/worker.js", "axis-node": "./dist/bin/node.js", "axis-routing-preflight": "./dist/bin/routing-preflight.js" });
  assert.deepEqual(pkg.dependencies, { "@axisosme/axis-contracts": "1.0.0", "@linear/sdk": "95.0.0", "@openai/codex": "0.165.0", "graphql": "16.14.2", "smol-toml": "1.8.0" });
}

export function assertArchiveArtifacts(listing) {
  assertDeclaredSurface();
  for (const root of [".agents", ".claude"]) {
    assert.equal(listing.some((path) => path === `package/${root}` || path.startsWith(`package/${root}/`)), false, `archive must exclude repository-only ${root}`);
  }
  assert.equal(listing.includes("package/LICENSE"), true, "archive must include LICENSE");
  assert.equal(listing.includes("package/node_modules/@axisosme/axis-contracts/LICENSE"), true, "bundled contracts must include LICENSE");
  for (const path of activeDistArtifacts) assert.equal(listing.includes(`package/${path}`), true, `archive is missing ${path}`);
  assertExactArtifacts(listing
    .filter((path) => path.startsWith("package/dist/"))
    .map((path) => path.slice("package/".length)), "archive");
  assert.equal(listing.includes("package/node_modules/@axisosme/axis-contracts/package.json"), true, "archive must bundle the contracts workspace");
  for (const family of ["runtime", "agent-activity", "linear-execution"]) {
    assert.equal(listing.includes(`package/node_modules/@axisosme/axis-contracts/dist/src/${family}/index.js`), true, `bundled ${family} contracts must be built`);
  }
}

export async function assertInstalledArtifacts(installedRoot) {
  assertDeclaredSurface();
  await access(join(installedRoot, "LICENSE"));
  await Promise.all(activeDistArtifacts.map((path) => access(join(installedRoot, path))));
  assertExactArtifacts(emittedArtifactsIn(join(installedRoot, "dist")), "installed package");
}

export async function assertInstalledDependencyTree(consumer, installedRoot) {
  const graphql = JSON.parse(await readFile(join(consumer, "node_modules", "graphql", "package.json"), "utf8"));
  const sdk = JSON.parse(await readFile(join(consumer, "node_modules", "@linear", "sdk", "package.json"), "utf8"));
  assert.equal(sdk.version, "95.0.0");
  assert.equal(graphql.version, "16.14.2");
  const contractsRoot = join(consumer, "node_modules", "@axisosme", "axis-contracts");
  const contracts = JSON.parse(await readFile(join(contractsRoot, "package.json"), "utf8"));
  assert.equal(contracts.version, "1.0.0");
  assert.deepEqual(Object.keys(contracts.exports).sort(), ["./runtime", "./agent-activity", "./linear-execution", "./schemas/*", "./fixtures/*", "./conformance/*"].sort());
  assert.ok(contracts.files.includes("LICENSE"), "contracts files must include LICENSE");
  const license = await readFile(join(installedRoot, "LICENSE"), "utf8");
  assert.equal(await readFile(join(contractsRoot, "LICENSE"), "utf8"), license, "contracts license must match the root license");
  const bundledContractsRoot = join(installedRoot, "node_modules", "@axisosme", "axis-contracts");
  const bundledContracts = JSON.parse(await readFile(join(bundledContractsRoot, "package.json"), "utf8"));
  assert.equal(bundledContracts.version, contracts.version);
  assert.equal(await readFile(join(bundledContractsRoot, "LICENSE"), "utf8"), license, "bundled contracts license must match the root license");
}

export function runtimeProbeSource() {
  assertDeclaredSurface();
  return [
    'import assert from "node:assert/strict";',
    'import * as axis from "@axisosme/axis";',
    'import { AxisLinearSdk, AxisLinearRequestError, axisLinearSdkFromEnvironment } from "@axisosme/axis/linear";',
    'assert.equal(typeof AxisLinearSdk, "function"); assert.equal(typeof AxisLinearRequestError, "function"); assert.throws(() => axisLinearSdkFromEnvironment({}), /unavailable/);',
    `assert.deepEqual(Object.keys(axis).sort(), ${JSON.stringify(sortedInventory(activeRuntimeExports, "activeRuntimeExports"))}, "installed root runtime exports must exactly match src/index.ts");`,
    'assert.equal(axis.ROUTER_VERSION, "1.0.0");',
    'assert.equal(axis.CODEX_EXECUTOR_CONTRACT_VERSION, "1.0.0");',
    'const decisionId = `axis-decision-${"c".repeat(48)}`;',
    'const optionOneId = `axis-option-${"a".repeat(48)}`; const optionOne = { optionId: optionOneId, replyValue: axis.deterministicElicitationReplyValue(decisionId, optionOneId), label: "Safe", description: "Use the bounded route." };',
    'const optionTwoId = `axis-option-${"b".repeat(48)}`; const optionTwo = { optionId: optionTwoId, replyValue: axis.deterministicElicitationReplyValue(decisionId, optionTwoId), label: "Fast", description: "Use the faster route." };',
    'const elicitation = { contractVersion: "1.0.0-fixed-choice", decisionId, sourceEventId: `axis-elicitation-${"d".repeat(48)}`, recommendedOptionId: optionOne.optionId, header: "Choose route", question: "Which route should Axis use?", options: [optionOne, optionTwo] };',
    'assert.equal(axis.validateStructuredElicitation(elicitation)?.recommendedOptionId, optionOne.optionId);',
    'assert.equal(axis.selectElicitationOption(elicitation, optionOne.replyValue)?.optionId, optionOne.optionId); assert.equal(axis.selectElicitationOption(elicitation, optionOne.label), null);',
    'const rendered = axis.renderElicitationMarkdown(elicitation, "https://linear.app/acme/profiles/human"); assert.match(rendered, /Recommended/u); assert.match(rendered, /owner https:\\/\\/linear\\.app\\/acme\\/profiles\\/human/u); assert.doesNotMatch(rendered, /\\[Linear\\]\\(/u);',
    'const { recommendedOptionId: _recommendation, ...missingRecommendation } = elicitation; assert.equal(axis.validateStructuredElicitation(missingRecommendation), null);',
    'const currentBindings = new axis.SqliteTeamBindingStore(); assert.deepEqual(currentBindings.listCurrent({ organizationId: "org_136_consumer" }), []); currentBindings.close();',
    'assert.equal((await import("@axisosme/axis-contracts/runtime")).runtimeContractVersion, "1.0.0");',
    'const activity = await import("@axisosme/axis-contracts/agent-activity"); assert.equal(activity.agentActivityContractVersion, "1.0.0"); assert.equal(typeof activity.validateAgentWaitingDelivery, "function"); assert.equal(typeof activity.validateAgentResponseDelivery, "function");',
    'assert.equal((await import("@axisosme/axis-contracts/linear-execution")).linearExecutionContractVersion, "1.0.0");',
  ].join("\n");
}

export function typeProbeSource() {
  assertDeclaredSurface();
  const activeTypeReferences = activeTypes.map((type) => {
    if (type === "ExecutorAcceptedResult") return "ExecutorAcceptedResult<\"start\">";
    if (type === "ExecutorFailedResult") return "ExecutorFailedResult<CodexExecutorOperation>";
    return type;
  });
  return [
    `import type { ${activeTypes.join(", ")} } from "@axisosme/axis";`,
    `declare const activeSurface: [${activeTypeReferences.join(", ")}];`,
    "void activeSurface;",
    'declare const binding: CodexLiveBinding;',
    'const commentaryLifecycle: ExecutorLifecycleEvent<CodexLiveBinding> = { contractVersion: "1.0.0", binding, sequence: 2, outcome: "progressed", progress: { kind: "commentary", safeMessage: "Safe commentary." } };',
    'if (commentaryLifecycle.outcome === "progressed" && commentaryLifecycle.progress.kind === "commentary") commentaryLifecycle.progress.safeMessage satisfies string;',
    'const recommendedOption: ElicitationOption = { optionId: `axis-option-${"a".repeat(48)}`, replyValue: `axis-reply-${"e".repeat(48)}`, label: "Safe", description: "Use the bounded route." };',
    'const alternateOption: ElicitationOption = { optionId: `axis-option-${"b".repeat(48)}`, replyValue: `axis-reply-${"f".repeat(48)}`, label: "Fast", description: "Use the faster route." };',
    'const elicitation: StructuredElicitation = { contractVersion: "1.0.0-fixed-choice", decisionId: `axis-decision-${"c".repeat(48)}`, sourceEventId: `axis-elicitation-${"d".repeat(48)}`, recommendedOptionId: recommendedOption.optionId, header: "Choose route", question: "Which route should Axis use?", options: [recommendedOption, alternateOption] };',
    'const waitingLifecycle: ExecutorLifecycleEvent<CodexLiveBinding> = { contractVersion: "1.0.0", binding, sequence: 3, outcome: "waiting_for_input", elicitation };',
    'if (waitingLifecycle.outcome === "waiting_for_input") waitingLifecycle.elicitation.recommendedOptionId satisfies string;',
    '// @ts-expect-error recommendedOptionId is mandatory',
    'const missingRecommendation: StructuredElicitation = { contractVersion: "1.0.0-fixed-choice", decisionId: `axis-decision-${"c".repeat(48)}`, sourceEventId: `axis-elicitation-${"d".repeat(48)}`, header: "Choose route", question: "Which route should Axis use?", options: [recommendedOption, alternateOption] };',
    'void missingRecommendation;',
    '// @ts-expect-error replyValue is mandatory; option label is not a reply protocol',
    'const missingReplyValue: ElicitationOption = { optionId: `axis-option-${"a".repeat(48)}`, label: "Safe", description: "Use the bounded route." };',
    'void missingReplyValue;',
    '// @ts-expect-error progressed lifecycle facts require an explicit generic or commentary progress discriminator',
    'const malformedProgress: ExecutorLifecycleEvent<CodexLiveBinding> = { contractVersion: "1.0.0", binding, sequence: 4, outcome: "progressed" };',
    'void malformedProgress;',
  ].join("\n");
}
