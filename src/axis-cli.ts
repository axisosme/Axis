import { readFileSync } from "node:fs";

import { initUsage } from "./init-cli.js";
import { Router, RouterError } from "./router/router.js";
import type { RouterRunTargetEvidence } from "./router/router.js";
import { axisCliNodeRunTarget } from "./shared/node-routing-projection.js";
import type { AxisCliNodeRunTarget } from "./shared/node-routing-projection.js";
import { SqliteTeamBindingStore, TEAM_BINDING_CONTRACT_VERSION, TeamBindingError } from "./router/team-bindings.js";
import type { ExecutionProfile, TeamBindingMutationAudit, TeamBindingRevision } from "./router/team-bindings.js";
import { CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION } from "./router/channel-conversation-types.js";
import { SqliteWorkerCommandJournal, WORKER_WORKSPACE_RESOLUTION_AUDIT_VERSION } from "./worker/command.js";
import { ChannelRedispatchDispositions, ChannelRunChainStoreError } from "./router/channel-run-chain.js";
import type { ChannelRedispatchDisposition } from "./router/channel-run-chain.js";
import { LINEAR_INBOX_ACKNOWLEDGEMENT_REASON_LIMIT, LINEAR_INBOX_CONTRACT_VERSION, LinearInboxGiveUpDispositions, SqliteLinearInbox } from "./relay/linear/inbox.js";
import { RelayError } from "./relay/errors.js";
import type { LinearInboxGiveUpDisposition } from "./relay/linear/inbox.js";

export const CLI_SCHEMA_VERSION = "axis.cli.v1" as const;

type OutputMode = "human" | "json";
type Command = "team-bindings list" | "team-bindings get" | "team-bindings history" | "team-bindings set" | "team-bindings disable" | "topology list" | "topology tree" | "workspace-audit list" | "outbox-quarantine list" | "linear-inbox-give-ups list" | "linear-inbox-give-ups acknowledge" | "redispatch-give-ups list";
type ErrorCode = "unconfigured" | "disabled" | "reference_missing" | "stale" | "incompatible" | "identity_mismatch" | "revision_conflict" | "persisted_state_conflict" | "unauthorized" | "invalid_input" | "unavailable";
/** Which JSON-envelope contract-version key and value a command's family reports. */
type CommandFamily = "team-binding" | "channel-conversation" | "linear-inbox";

interface CliIo {
  readonly stdout: (text: string) => void;
  readonly stderr: (text: string) => void;
}

interface ParsedArguments {
  readonly positional: readonly string[];
  readonly flags: ReadonlyMap<string, string | true>;
}

interface SafeAudit {
  readonly operatorRef: string;
  readonly source: string;
}

interface ConfiguredTarget {
  readonly organizationId: string;
  readonly linearTeamId: string;
  readonly revision: number;
  readonly operation: "set" | "disable";
  readonly status: "configured" | "disabled";
  readonly profileDigest: string | null;
  readonly profile: {
    readonly nodeId: string;
    readonly workerId: string;
    readonly executorKind: string;
    readonly runtimeProfile: { readonly ref: string; readonly version: string };
    readonly workspacePolicyRef: { readonly ref: string; readonly version: string } | null;
  } | null;
  readonly audit: SafeAudit;
  readonly createdAt: string;
}

interface BindingProjection {
  readonly configuredTarget: ConfiguredTarget | null;
  readonly currentEvidence: {
    readonly status: "unavailable";
    readonly source: null;
    readonly observedAt: null;
  };
  readonly historicalRunBinding: HistoricalRunBinding;
}

type HistoricalRunBinding =
  | { readonly status: "not_requested"; readonly runId: null; readonly target: null }
  | AxisCliNodeRunTarget;

interface RunSelector {
  readonly issueId: string;
  readonly linearTeamId: string;
  readonly agentSessionId: string;
  readonly runId: string;
}

class CliFailure extends Error {
  constructor(readonly code: ErrorCode) { super(code); }
}

const errorMessages: Readonly<Record<ErrorCode, string>> = {
  unconfigured: "The requested Team has no configured binding.",
  disabled: "The requested Team binding is disabled.",
  reference_missing: "A referenced Team binding resource is unavailable.",
  stale: "The requested Team binding is stale.",
  incompatible: "The requested Team binding is incompatible.",
  identity_mismatch: "The exact Agent Profile and execution target identities do not match.",
  revision_conflict: "The binding changed; read it again before retrying.",
  persisted_state_conflict: "Persisted binding state is inconsistent and was not changed.",
  unauthorized: "Explicit operator authority is required for this mutation.",
  invalid_input: "Input is invalid. Correct the command and retry.",
  unavailable: "The authoritative binding source is unavailable. Retry only after it is restored.",
};

const allCommands: readonly Command[] = [
  "team-bindings list", "team-bindings get", "team-bindings history", "team-bindings set", "team-bindings disable", "topology list", "topology tree", "workspace-audit list",
  "outbox-quarantine list",
  "linear-inbox-give-ups list", "linear-inbox-give-ups acknowledge",
  "redispatch-give-ups list",
];

/** Every command's contract family. `topology`, `workspace-audit` and `outbox-quarantine` report `team-binding`. */
const commandFamilies: Readonly<Record<Command, CommandFamily>> = {
  "team-bindings list": "team-binding", "team-bindings get": "team-binding", "team-bindings history": "team-binding", "team-bindings set": "team-binding", "team-bindings disable": "team-binding",
  "topology list": "team-binding", "topology tree": "team-binding", "workspace-audit list": "team-binding",
  "outbox-quarantine list": "team-binding",
  "linear-inbox-give-ups list": "linear-inbox", "linear-inbox-give-ups acknowledge": "linear-inbox",
  "redispatch-give-ups list": "channel-conversation",
};
const familyContractVersionKey: Readonly<Record<CommandFamily, string>> = { "team-binding": "teamBindingContractVersion", "channel-conversation": "channelConversationContractVersion", "linear-inbox": "linearInboxContractVersion" };
const familyContractVersionValue: Readonly<Record<CommandFamily, string>> = { "team-binding": TEAM_BINDING_CONTRACT_VERSION, "channel-conversation": CHANNEL_CONVERSATION_CORE_CONTRACT_VERSION, "linear-inbox": LINEAR_INBOX_CONTRACT_VERSION };
/** Defaults to `team-binding` for any string outside the closed `Command` set (e.g. the pre-parse `"unknown"` sentinel). */
function commandFamily(commandName: string): CommandFamily { return Object.hasOwn(commandFamilies, commandName) ? commandFamilies[commandName as Command] : "team-binding"; }

function packageVersion(): string {
  try {
    const value = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as { version?: unknown };
    return typeof value.version === "string" ? value.version : "unknown";
  } catch { return "unknown"; }
}

function parse(argv: readonly string[]): ParsedArguments {
  const positional: string[] = [];
  const flags = new Map<string, string | true>();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === undefined || token === "--") throw new CliFailure("invalid_input");
    if (!token.startsWith("--")) { positional.push(token); continue; }
    const name = token.slice(2);
    if (name.length === 0 || flags.has(name)) throw new CliFailure("invalid_input");
    if (name === "help" || name === "version") { flags.set(name, true); continue; }
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) throw new CliFailure("invalid_input");
    flags.set(name, value); index += 1;
  }
  return { positional, flags };
}

function has(parsed: ParsedArguments, name: string): boolean { return parsed.flags.get(name) === true; }
function value(parsed: ParsedArguments, name: string): string | undefined {
  const found = parsed.flags.get(name);
  return typeof found === "string" ? found : undefined;
}
function required(parsed: ParsedArguments, name: string): string {
  const found = value(parsed, name);
  if (found === undefined) throw new CliFailure("invalid_input");
  return found;
}
function allowOnly(parsed: ParsedArguments, names: readonly string[]): void {
  for (const name of parsed.flags.keys()) if (!names.includes(name)) throw new CliFailure("invalid_input");
}
function outputMode(parsed: ParsedArguments): OutputMode {
  const mode = value(parsed, "output") ?? "human";
  if (mode !== "human" && mode !== "json") throw new CliFailure("invalid_input");
  return mode;
}
function command(parsed: ParsedArguments): Command {
  const candidate = parsed.positional.join(" ");
  if (!allCommands.includes(candidate as Command)) throw new CliFailure("invalid_input");
  return candidate as Command;
}
function expectedRevision(parsed: ParsedArguments): number {
  const input = required(parsed, "expected-revision");
  if (input === "absent") return 0;
  if (!/^(?:0|[1-9][0-9]*)$/u.test(input)) throw new CliFailure("invalid_input");
  const revision = Number(input);
  if (!Number.isSafeInteger(revision) || revision < 0) throw new CliFailure("invalid_input");
  return revision;
}
function limit(parsed: ParsedArguments): number | undefined {
  const input = value(parsed, "limit");
  if (input === undefined) return undefined;
  if (!/^(?:[1-9][0-9]*)$/u.test(input)) throw new CliFailure("invalid_input");
  const parsedLimit = Number(input);
  if (!Number.isSafeInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 100) throw new CliFailure("invalid_input");
  return parsedLimit;
}
function optionalRevision(parsed: ParsedArguments, name: string): number | undefined {
  const input = value(parsed, name);
  if (input === undefined || !/^(?:0|[1-9][0-9]*)$/u.test(input)) {
    if (input === undefined) return undefined;
    throw new CliFailure("invalid_input");
  }
  const revision = Number(input);
  if (!Number.isSafeInteger(revision) || revision < 0) throw new CliFailure("invalid_input");
  return revision;
}
function openStore(parsed: ParsedArguments): SqliteTeamBindingStore { return new SqliteTeamBindingStore({ filename: required(parsed, "database") }); }
function runSelector(parsed: ParsedArguments): RunSelector | undefined {
  const selector = {
    issueId: value(parsed, "issue"),
    linearTeamId: value(parsed, "team"),
    agentSessionId: value(parsed, "agent-session"),
    runId: value(parsed, "run"),
  };
  const provided = Object.values(selector).filter((entry) => entry !== undefined).length;
  if (provided === 0) return undefined;
  if (provided !== Object.keys(selector).length) throw new CliFailure("invalid_input");
  return selector as RunSelector;
}

/** Compare every execution-profile field when verifying a Team binding mutation. */
function executionProfilesMatch(a: ExecutionProfile, b: ExecutionProfile): boolean {
  return a.nodeId === b.nodeId && a.workerId === b.workerId && a.executorKind === b.executorKind
    && a.runtimeProfile.ref === b.runtimeProfile.ref && a.runtimeProfile.version === b.runtimeProfile.version
    && a.endpointRef?.ref === b.endpointRef?.ref && a.endpointRef?.version === b.endpointRef?.version
    && a.workspacePolicyRef?.ref === b.workspacePolicyRef?.ref && a.workspacePolicyRef?.version === b.workspacePolicyRef?.version;
}

function configuredTarget(revision: TeamBindingRevision): ConfiguredTarget {
  return {
    organizationId: revision.scope.organizationId,
    linearTeamId: revision.scope.linearTeamId,
    revision: revision.revision,
    operation: revision.status === "configured" ? "set" : "disable",
    status: revision.status,
    profileDigest: revision.profileDigest ?? null,
    profile: revision.profile === undefined ? null : {
      nodeId: revision.profile.nodeId,
      workerId: revision.profile.workerId,
      executorKind: revision.profile.executorKind,
      runtimeProfile: { ref: revision.profile.runtimeProfile.ref, version: revision.profile.runtimeProfile.version },
      workspacePolicyRef: revision.profile.workspacePolicyRef === undefined ? null : { ref: revision.profile.workspacePolicyRef.ref, version: revision.profile.workspacePolicyRef.version },
    },
    audit: { operatorRef: revision.audit.operatorRef, source: revision.audit.source },
    createdAt: revision.createdAt,
  };
}

function project(revision: TeamBindingRevision | null): BindingProjection {
  return {
    configuredTarget: revision === null ? null : configuredTarget(revision),
    currentEvidence: { status: "unavailable", source: null, observedAt: null },
    historicalRunBinding: { status: "not_requested", runId: null, target: null },
  };
}

function historicalRunBinding(evidence?: RouterRunTargetEvidence): HistoricalRunBinding {
  if (evidence === undefined) return { status: "not_requested", runId: null, target: null };
  return axisCliNodeRunTarget(evidence);
}

function mutationAudit(parsed: ParsedArguments): TeamBindingMutationAudit {
  const authority = value(parsed, "authority");
  if (authority === undefined) throw new CliFailure("unauthorized");
  const audit = { operatorRef: required(parsed, "operator"), source: required(parsed, "audit-source"), requestRef: required(parsed, "request"), authorizationRef: authority };
  const opaque = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
  if (Object.values(audit).some((entry) => !opaque.test(entry))) throw new CliFailure("invalid_input");
  return audit;
}

function executionProfile(parsed: ParsedArguments): ExecutionProfile {
  const endpoint = value(parsed, "endpoint"), endpointRevision = value(parsed, "endpoint-revision");
  const workspacePolicy = value(parsed, "workspace-policy"), workspacePolicyRevision = value(parsed, "workspace-policy-revision");
  if ((endpoint === undefined) !== (endpointRevision === undefined)) throw new CliFailure("invalid_input");
  if ((workspacePolicy === undefined) !== (workspacePolicyRevision === undefined)) throw new CliFailure("invalid_input");
  return {
    nodeId: required(parsed, "node"),
    workerId: required(parsed, "worker"),
    executorKind: required(parsed, "executor"),
    runtimeProfile: { ref: required(parsed, "profile"), version: required(parsed, "profile-revision") },
    ...(endpoint === undefined ? {} : { endpointRef: { ref: endpoint, version: endpointRevision! } }),
    ...(workspacePolicy === undefined ? {} : { workspacePolicyRef: { ref: workspacePolicy, version: workspacePolicyRevision! } }),
  };
}

function assertReread(requested: { readonly organizationId: string; readonly linearTeamId: string; readonly expectedRevision: number; readonly operation: "set" | "disable"; readonly auditSource: string; readonly profile?: ExecutionProfile }, reread: TeamBindingRevision | null): ConfiguredTarget {
  if (reread === null || reread.scope.organizationId !== requested.organizationId || reread.scope.linearTeamId !== requested.linearTeamId || reread.revision !== requested.expectedRevision + 1 || reread.audit.source !== requested.auditSource) throw new CliFailure("unavailable");
  if (requested.operation === "set") {
    if (reread.status !== "configured" || reread.profile === undefined || requested.profile === undefined || reread.profileDigest === undefined || !executionProfilesMatch(reread.profile, requested.profile)) throw new CliFailure("unavailable");
  } else if (reread.status !== "disabled" || reread.profile !== undefined || reread.profileDigest !== undefined) throw new CliFailure("unavailable");
  return configuredTarget(reread);
}

function topologyTree(entries: readonly BindingProjection[]): { readonly nodes: readonly { readonly nodeId: string; readonly teams: readonly BindingProjection[] }[]; readonly unassigned: readonly BindingProjection[] } {
  const byNode = new Map<string, BindingProjection[]>();
  const unassigned: BindingProjection[] = [];
  for (const entry of entries) {
    const nodeId = entry.configuredTarget?.profile?.nodeId;
    if (nodeId === undefined) { unassigned.push(entry); continue; }
    const group = byNode.get(nodeId) ?? [];
    group.push(entry); byNode.set(nodeId, group);
  }
  return {
    nodes: [...byNode.entries()].sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0).map(([nodeId, teams]) => ({ nodeId, teams })),
    unassigned,
  };
}

function humanBinding(binding: BindingProjection): string[] {
  const target = binding.configuredTarget;
  if (target === null) return ["configuredTarget: unconfigured", "currentEvidence: unavailable", "historicalRunBinding: not_requested"];
  const output = [
    `configuredTarget: team=${target.linearTeamId} revision=${target.revision} status=${target.status} operation=${target.operation}`,
    `audit: operator=${target.audit.operatorRef} source=${target.audit.source} at=${target.createdAt}`,
  ];
  if (target.profile !== null) output.splice(1, 0, `profile: node=${target.profile.nodeId} worker=${target.profile.workerId} executor=${target.profile.executorKind} runtimeProfile=${target.profile.runtimeProfile.ref}@${target.profile.runtimeProfile.version} workspacePolicy=${target.profile.workspacePolicyRef === null ? "none" : `${target.profile.workspacePolicyRef.ref}@${target.profile.workspacePolicyRef.version}`}`);
  output.push("currentEvidence: unavailable", "historicalRunBinding: not_requested");
  return output;
}

function humanHistorical(binding: HistoricalRunBinding): string[] {
  if ("schemaVersion" in binding) {
    // A policy selection is rendered as the facts it carries, because
    // that is all it holds -- the Worker it resolved to is reported by the
    // execution evidence on the next line, which is the honest place for it.
    // Printing a Worker here would mean re-deriving a decision this process
    // does not own and cannot see the Node's configuration for.
    const selection = `policy=${binding.workerSelection.scope.system}:${binding.workerSelection.scope.scopeId} executors=${binding.workerSelection.executorLabels.join(",") || "none"}`;
    const execution = binding.workerId === null
      ? "executionEvidence: unavailable (no Worker/executor evidence recorded)"
      : `executionEvidence: recorded worker=${binding.workerId} executor=${binding.executorId}`;
    return [
      `historicalRunBinding: ${binding.schemaVersion} run=${binding.runId}`,
      `nodeTarget: node=${binding.nodeRef.ref}@${binding.nodeRef.version} workerSelection=${selection} workspacePolicy=${binding.workspacePolicyRef === null ? "none" : `${binding.workspacePolicyRef.ref}@${binding.workspacePolicyRef.version}`}`,
      execution,
    ];
  }
  return ["historicalRunBinding: not_requested"];
}

function human(commandName: Command, result: unknown): string {
  const title = `Axis CLI ${CLI_SCHEMA_VERSION} | ${commandName}`;
  if (commandName === "workspace-audit list") {
    const value = result as { readonly audits: readonly { readonly sequence: number; readonly commandId: string; readonly runId: string; readonly system: string; readonly organizationId: string; readonly workspacePolicyRef: { readonly ref: string; readonly version: string } | null; readonly outcome: string; readonly reason: string; readonly occurredAt: string }[] };
    return [title, ...(value.audits.length === 0 ? ["workspaceAudit: none"] : value.audits.map((audit) => `workspaceAudit: sequence=${audit.sequence} command=${audit.commandId} run=${audit.runId} system=${audit.system} source=${audit.organizationId} workspacePolicy=${audit.workspacePolicyRef === null ? "none" : `${audit.workspacePolicyRef.ref}@${audit.workspacePolicyRef.version}`} outcome=${audit.outcome} reason=${audit.reason} at=${audit.occurredAt}`))].join("\n");
  }
  if (commandName === "linear-inbox-give-ups list") {
    const value = result as { readonly dispositions: readonly LinearInboxGiveUpDisposition[] };
    return [title, ...(value.dispositions.length === 0 ? ["inboxGiveUp: none"] : value.dispositions.map(humanInboxGiveUp))].join("\n");
  }
  if (commandName === "linear-inbox-give-ups acknowledge") {
    const value = result as { readonly verification: { readonly status: string }; readonly disposition: LinearInboxGiveUpDisposition };
    return [title, `mutation: ${value.verification.status}`, humanInboxGiveUp(value.disposition)].join("\n");
  }
  if (commandName === "redispatch-give-ups list") {
    const value = result as { readonly dispositions: readonly ChannelRedispatchDisposition[] };
    return [title, ...(value.dispositions.length === 0 ? ["redispatchGiveUp: none"] : value.dispositions.map((entry) => `redispatchGiveUp: sequence=${entry.sequence} conversation=${entry.conversationId} command=${entry.commandId} attempts=${entry.attempts} code=${entry.failureCode} at=${entry.givenUpAt}`))].join("\n");
  }
  if (commandName === "outbox-quarantine list") {
    const value = result as { readonly dispositions: readonly { readonly sequence: number; readonly runId: string; readonly messageId: string; readonly code: string; readonly quarantinedAt: string }[] };
    return [title, ...(value.dispositions.length === 0 ? ["outboxQuarantine: none"] : value.dispositions.map((entry) => `outboxQuarantine: sequence=${entry.sequence} run=${entry.runId} message=${entry.messageId} code=${entry.code} at=${entry.quarantinedAt}`))].join("\n");
  }
  if (commandName === "team-bindings get") return [title, ...humanBinding(result as BindingProjection)].join("\n");
  if (commandName === "team-bindings set" || commandName === "team-bindings disable") {
    const value = result as { readonly verification: { readonly status: string }; readonly binding: BindingProjection };
    return [title, `mutation: ${value.verification.status}`, ...humanBinding(value.binding)].join("\n");
  }
  if (commandName === "team-bindings list" || commandName === "topology list") {
    const value = result as { readonly organizationId: string; readonly bindings?: readonly BindingProjection[]; readonly entries?: readonly BindingProjection[]; readonly historicalRunBinding?: HistoricalRunBinding };
    const entries = value.bindings ?? value.entries ?? [];
    return [title, `organization: ${value.organizationId}`, ...(value.historicalRunBinding === undefined ? [] : humanHistorical(value.historicalRunBinding)), entries.length === 0 ? "configuredTarget: none" : entries.flatMap((entry) => ["---", ...humanBinding(entry)]).join("\n")].join("\n");
  }
  if (commandName === "team-bindings history") {
    const value = result as { readonly organizationId: string; readonly linearTeamId: string; readonly revisions: readonly BindingProjection[] };
    return [title, `organization: ${value.organizationId} team: ${value.linearTeamId}`, value.revisions.length === 0 ? "configuredTarget: none" : value.revisions.flatMap((entry) => ["---", ...humanBinding(entry)]).join("\n")].join("\n");
  }
  const value = result as { readonly organizationId: string; readonly historicalRunBinding: HistoricalRunBinding; readonly tree: { readonly nodes: readonly { readonly nodeId: string; readonly teams: readonly BindingProjection[] }[]; readonly unassigned: readonly BindingProjection[] } };
  const lines = [title, `organization: ${value.organizationId}`, ...humanHistorical(value.historicalRunBinding)];
  for (const node of value.tree.nodes) {
    lines.push(`node: ${node.nodeId}`);
    for (const team of node.teams) lines.push(...humanBinding(team).map((line) => `  ${line}`));
  }
  for (const team of value.tree.unassigned) lines.push("node: unassigned", ...humanBinding(team).map((line) => `  ${line}`));
  if (value.tree.nodes.length === 0 && value.tree.unassigned.length === 0) lines.push("configuredTarget: none");
  return lines.join("\n");
}

/**
 * One recorded inbox give-up as a single line, in the `outboxQuarantine:`
 * field order with the acknowledgement appended.
 *
 * The acknowledgement half is always printed, with `none` where a field is
 * absent, so the line has ONE shape whether or not the gate has been
 * released -- a reader (or an `awk`) does not have to branch on the length of
 * the line.
 *
 * `reason` is last AND quoted, because it is the only field an operator
 * writes freely: the other five are opaque-reference validated and cannot
 * contain a space or an `=`, while a reason can contain both. Unquoted, a
 * reason reading `why reason=spoofed operator=someone-else` appended tokens
 * that parse exactly like the audit fields in front of it -- a forged
 * attribution. `JSON.stringify` also makes the absent case (`none`) distinguishable from
 * an operator who literally wrote "none".
 */
function humanInboxGiveUp(entry: LinearInboxGiveUpDisposition): string {
  const acknowledgement = entry.acknowledgement;
  return `inboxGiveUp: sequence=${entry.sequence} delivery=${entry.deliveryId} attempts=${entry.attempts} code=${entry.failureCode} at=${entry.givenUpAt}`
    + ` acknowledged=${acknowledgement?.acknowledgedAt ?? "none"} operator=${acknowledgement?.operatorRef ?? "none"}`
    + ` authority=${acknowledgement?.authorizationRef ?? "none"} source=${acknowledgement?.source ?? "none"}`
    + ` request=${acknowledgement?.requestRef ?? "none"} reason=${acknowledgement?.reason === null || acknowledgement?.reason === undefined ? "none" : JSON.stringify(acknowledgement.reason)}`;
}

/**
 * The optional `--reason` an operator may attach to a release.
 *
 * `SqliteLinearInbox#acknowledgeGiveUp` applies the same bound as the durable
 * boundary. This check runs before the database is opened and reports
 * `invalid_input` rather than the store's `unavailable`.
 */
function acknowledgementReason(parsed: ParsedArguments): string | undefined {
  const reason = value(parsed, "reason");
  if (reason === undefined) return undefined;
  // eslint-disable-next-line no-control-regex
  if (reason.length === 0 || reason.length > LINEAR_INBOX_ACKNOWLEDGEMENT_REASON_LIMIT || /[\u0000-\u001F\u007F]/u.test(reason)) throw new CliFailure("invalid_input");
  return reason;
}

/**
 * The operator-visible code for a `RelayError` reaching this CLI.
 *
 * Only the Linear inbox give-up surfaces raise one here, and without this arm
 * they all collapsed to `unavailable` -- so a disposition holding a code this
 * build never writes was reported as "retry later" on the inbox side and as
 * `persisted_state_conflict` on the redispatch side, for the same class of
 * damaged state. `retryable` is the Relay's own transient/permanent
 * discrimination, so it is what decides which of the two this is rather than
 * a second judgement made here.
 */
function relayCliFailureCode(error: RelayError): ErrorCode {
  if (error.retryable) return "unavailable";
  return error.statusCode === 400 ? "invalid_input" : "persisted_state_conflict";
}

function envelope(commandName: string, rest: Record<string, unknown>): Record<string, unknown> {
  const family = commandFamily(commandName);
  return { schemaVersion: CLI_SCHEMA_VERSION, [familyContractVersionKey[family]]: familyContractVersionValue[family], command: commandName, ...rest };
}
function emit(io: CliIo, mode: OutputMode, commandName: Command, result: unknown): void {
  if (mode === "json") io.stdout(`${JSON.stringify(envelope(commandName, { result }))}\n`);
  else io.stdout(`${human(commandName, result)}\n`);
}
function emitFailure(io: CliIo, mode: OutputMode, commandName: string, failure: CliFailure): void {
  const message = errorMessages[failure.code];
  if (mode === "json") io.stdout(`${JSON.stringify(envelope(commandName, { error: { code: failure.code, message } }))}\n`);
  else io.stderr(`${failure.code}: ${message}\n`);
}

export function help(): string {
  return `Axis operator CLI

Usage:
  ${initUsage}
  axis serve [--config <axis.toml>]
  axis [--output human|json] team-bindings list --database <router-db> --organization <organization-id> [--after-team <linear-team-id>] [--limit <1..100>]
  axis [--output human|json] team-bindings get --database <router-db> --organization <organization-id> --team <linear-team-id>
  axis [--output human|json] team-bindings history --database <router-db> --organization <organization-id> --team <linear-team-id> [--after-revision <revision>] [--limit <1..100>]
  axis [--output human|json] team-bindings set --database <router-db> --organization <organization-id> --team <linear-team-id> --profile <runtime-profile-id> --profile-revision <revision> --node <node-id> --worker <worker-id> --executor <executor-kind> [--endpoint <endpoint-ref> --endpoint-revision <revision>] [--workspace-policy <policy-ref> --workspace-policy-revision <revision>] --expected-revision <revision|absent> --operator <operator-id> --authority <authority-ref> --audit-source <source> --request <request-id>
  axis [--output human|json] team-bindings disable --database <router-db> --organization <organization-id> --team <linear-team-id> --expected-revision <revision> --operator <operator-id> --authority <authority-ref> --audit-source <source> --request <request-id>
  axis [--output human|json] topology list --database <router-db> --organization <organization-id> [--after-team <linear-team-id>] [--limit <1..100>] [--issue <issue-id> --team <linear-team-id> --agent-session <agent-session-id> --run <run-id>]
  axis [--output human|json] topology tree --database <router-db> --organization <organization-id> [--after-team <linear-team-id>] [--limit <1..100>] [--issue <issue-id> --team <linear-team-id> --agent-session <agent-session-id> --run <run-id>]
  axis [--output human|json] workspace-audit list --database <worker-db> [--after-sequence <sequence>] [--limit <1..100>]
  axis [--output human|json] outbox-quarantine list --database <worker-db> [--after-sequence <sequence>] [--limit <1..100>]
  axis [--output human|json] linear-inbox-give-ups list --database <linear-channel-db> [--after-sequence <sequence>] [--limit <1..100>]
  axis [--output human|json] linear-inbox-give-ups acknowledge --database <linear-channel-db> --delivery <delivery-id> --operator <operator-id> --authority <authority-ref> --audit-source <source> --request <request-id> [--reason <printable text, 1..512 chars>]
  axis [--output human|json] redispatch-give-ups list --database <run-chain-db> [--after-sequence <sequence>] [--limit <1..100>]
  axis install --role controller|worker|node --config <owner-private-axis.toml> [--node <id>] [--worker <id>] [--service-label <safe-label>] [--host-root <absolute>] [--source-package <absolute>] [--output human|json]
  axis upgrade [--host-root <absolute>] [--source-package <absolute>] [--force] [--output human|json]
    --force stops the service without any drain check (in-flight work is abandoned), moves a Node journal or Worker database with WAL/SHM into <host-root>/archive/<timestamp>, starts the new release, and never rolls back
  axis node drain|resume|status --host-root <absolute> [--output human|json]

Team-only binding operations use the public store. The opaque Team workspace policy is visible in axis.cli.v1, while Worker-local paths remain hidden. Workspace audit reads are path-free and open the Worker database read-only. The CLI never discovers targets, reads credentials, or treats current evidence as historical Run evidence. A topology Run selector is all-or-none and reads only the Router public frozen-target evidence seam.

The give-up surfaces report what Axis stopped retrying, so operational debt is countable without opening a database file. Both listings open read-only and read only their own append-only disposition table: they never join the delivery or the redispatch marker they describe, so a disposition stays visible after the row it describes is gone, and neither can return provider content because neither table holds any. \`linear-inbox-give-ups acknowledge\` releases one inbox give-up's hold on host upgrade while RETAINING the delivery, its payload and its attempt count -- it clears the block, never the evidence. It requires explicit operator authority, is idempotent, and cannot be rewritten once recorded. Redispatch give-ups have a listing but no acknowledgement: they do not block drain, so there is no gate to release.

Host lifecycle commands use the local-only axis.host.v1 state contract. Install pins a TOML digest and local role selectors; upgrade validates that configuration and the existing managed service identity. Public axis.host.receipt.v1 receipts intentionally omit configuration values, host paths, database names, service labels, and health endpoints.
`;
}

interface Closable { close(): void }
interface CommandContext {
  readonly parsed: ParsedArguments;
  own<T extends Closable>(resource: T): T;
}
type CommandHandler = (context: CommandContext) => unknown;

function page(parsed: ParsedArguments, afterName: string, afterKey: string): Record<string, unknown> {
  const after = afterName === "after-sequence" || afterName === "after-revision" ? optionalRevision(parsed, afterName) : value(parsed, afterName), pageLimit = limit(parsed);
  return { ...(after === undefined ? {} : { [afterKey]: after }), ...(pageLimit === undefined ? {} : { limit: pageLimit }) };
}

/** Reads through a read-only reader that never opens the inbox store, so listing never writes the database or reads a ciphertext. */
function listLinearInboxGiveUps({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "after-sequence", "limit"]);
  const options = page(parsed, "after-sequence", "afterSequence");
  return { dispositions: own(new LinearInboxGiveUpDispositions(required(parsed, "database"))).list(options) };
}

function listRedispatchGiveUps({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "after-sequence", "limit"]);
  const options = page(parsed, "after-sequence", "afterSequence");
  return { dispositions: own(new ChannelRedispatchDispositions(required(parsed, "database"))).list(options) };
}

/** Releases one recorded inbox give-up. The disposition is probed read-only before the store opens. */
function acknowledgeLinearInboxGiveUp({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "delivery", "operator", "authority", "audit-source", "request", "reason"]);
  const deliveryId = required(parsed, "delivery");
  const audit = mutationAudit(parsed), reason = acknowledgementReason(parsed);
  const filename = required(parsed, "database");
  if (own(new LinearInboxGiveUpDispositions(filename)).recorded(deliveryId) === null) throw new CliFailure("invalid_input");
  const outcome = own(new SqliteLinearInbox({ filename })).acknowledgeGiveUp({
    deliveryId,
    operatorRef: audit.operatorRef,
    authorizationRef: audit.authorizationRef,
    source: audit.source,
    requestRef: audit.requestRef,
    ...(reason === undefined ? {} : { reason }),
  });
  if (outcome === null) throw new CliFailure("invalid_input");
  if (outcome.disposition.acknowledgement === null) throw new CliFailure("persisted_state_conflict");
  return {
    verification: { status: outcome.applied ? "applied_and_verified" : "already_applied_and_verified" },
    disposition: outcome.disposition,
  };
}

function listWorkspaceAudits({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "after-sequence", "limit"]);
  const options = page(parsed, "after-sequence", "afterSequence");
  const workerJournal = own(new SqliteWorkerCommandJournal(required(parsed, "database"), { readOnly: true }));
  return { auditContractVersion: WORKER_WORKSPACE_RESOLUTION_AUDIT_VERSION, audits: workerJournal.listWorkspaceResolutionAudits(options) };
}

/** Reads the quarantine sidecar alone, which carries no payload. */
function listOutboxQuarantine({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "after-sequence", "limit"]);
  const options = page(parsed, "after-sequence", "afterSequence");
  const workerJournal = own(new SqliteWorkerCommandJournal(required(parsed, "database"), { readOnly: true }));
  return { dispositions: workerJournal.listQuarantineDispositions(options) };
}

const listingFlags = ["output", "database", "organization", "after-team", "limit"];

function currentTeamBindings(parsed: ParsedArguments, store: SqliteTeamBindingStore): { readonly organizationId: string; readonly entries: readonly BindingProjection[] } {
  const organizationId = required(parsed, "organization");
  return { organizationId, entries: store.listCurrent({ organizationId, ...page(parsed, "after-team", "afterLinearTeamId") }).map(project) };
}

function listTeamBindings({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, listingFlags);
  const { organizationId, entries } = currentTeamBindings(parsed, own(openStore(parsed)));
  return { organizationId, bindings: entries };
}

function topology(context: CommandContext): { readonly organizationId: string; readonly historicalRunBinding: HistoricalRunBinding; readonly entries: readonly BindingProjection[] } {
  const { parsed, own } = context;
  allowOnly(parsed, [...listingFlags, "issue", "team", "agent-session", "run"]);
  const { organizationId, entries } = currentTeamBindings(parsed, own(openStore(parsed)));
  const selector = runSelector(parsed);
  if (selector === undefined) return { organizationId, historicalRunBinding: historicalRunBinding(), entries };
  const router = own(new Router({ filename: required(parsed, "database") }));
  const evidence = router.readRunTarget({ organizationId, issueId: selector.issueId, teamId: selector.linearTeamId, agentSessionId: selector.agentSessionId, runId: selector.runId });
  return { organizationId, historicalRunBinding: historicalRunBinding(evidence), entries };
}

function getTeamBinding({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "organization", "team"]);
  return project(own(openStore(parsed)).current({ scope: { organizationId: required(parsed, "organization"), linearTeamId: required(parsed, "team") } }));
}

function teamBindingHistory({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "organization", "team", "after-revision", "limit"]);
  const store = own(openStore(parsed));
  const organizationId = required(parsed, "organization"), linearTeamId = required(parsed, "team");
  const revisions = store.history({ scope: { organizationId, linearTeamId }, ...page(parsed, "after-revision", "afterRevision") });
  return { organizationId, linearTeamId, revisions: revisions.map(project) };
}

function setTeamBinding({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "organization", "team", "profile", "profile-revision", "node", "worker", "executor", "endpoint", "endpoint-revision", "workspace-policy", "workspace-policy-revision", "expected-revision", "operator", "authority", "audit-source", "request"]);
  const organizationId = required(parsed, "organization"), linearTeamId = required(parsed, "team"), expected = expectedRevision(parsed), audit = mutationAudit(parsed), profile = executionProfile(parsed);
  const store = own(openStore(parsed));
  store.configure({ scope: { organizationId, linearTeamId }, expectedRevision: expected, audit, profile });
  const verified = assertReread({ organizationId, linearTeamId, expectedRevision: expected, operation: "set", auditSource: audit.source, profile }, store.current({ scope: { organizationId, linearTeamId } }));
  return { verification: { status: "applied_and_verified" }, binding: projectFromTarget(verified) };
}

function disableTeamBinding({ parsed, own }: CommandContext): unknown {
  allowOnly(parsed, ["output", "database", "organization", "team", "expected-revision", "operator", "authority", "audit-source", "request"]);
  const organizationId = required(parsed, "organization"), linearTeamId = required(parsed, "team"), expected = expectedRevision(parsed), audit = mutationAudit(parsed);
  const store = own(openStore(parsed));
  store.disable({ scope: { organizationId, linearTeamId }, expectedRevision: expected, audit });
  const verified = assertReread({ organizationId, linearTeamId, expectedRevision: expected, operation: "disable", auditSource: audit.source }, store.current({ scope: { organizationId, linearTeamId } }));
  return { verification: { status: "applied_and_verified" }, binding: projectFromTarget(verified) };
}

const commandHandlers: Readonly<Record<Command, CommandHandler>> = {
  "team-bindings list": listTeamBindings,
  "team-bindings get": getTeamBinding,
  "team-bindings history": teamBindingHistory,
  "team-bindings set": setTeamBinding,
  "team-bindings disable": disableTeamBinding,
  "topology list": topology,
  "topology tree": (context) => { const { organizationId, historicalRunBinding, entries } = topology(context); return { organizationId, historicalRunBinding, tree: topologyTree(entries) }; },
  "workspace-audit list": listWorkspaceAudits,
  "outbox-quarantine list": listOutboxQuarantine,
  "linear-inbox-give-ups list": listLinearInboxGiveUps,
  "linear-inbox-give-ups acknowledge": acknowledgeLinearInboxGiveUp,
  "redispatch-give-ups list": listRedispatchGiveUps,
};

function storeFailureCode(code: string): ErrorCode { return code === "persisted_state_conflict" ? "persisted_state_conflict" : "incompatible"; }

function routerFailureCode(error: RouterError): ErrorCode {
  if (error.code === "axis.router.recovery_required") return "persisted_state_conflict";
  if (error.code === "axis.router.malformed" || error.code === "axis.router.not_found" || error.code === "axis.router.authority_mismatch") return "invalid_input";
  return "unavailable";
}

function cliFailure(error: unknown): CliFailure {
  if (error instanceof CliFailure) return error;
  if (error instanceof TeamBindingError) return new CliFailure(error.code);
  if (error instanceof ChannelRunChainStoreError) return new CliFailure(storeFailureCode(error.code));
  if (error instanceof RelayError) return new CliFailure(relayCliFailureCode(error));
  if (error instanceof RouterError) return new CliFailure(routerFailureCode(error));
  if (error instanceof TypeError) return new CliFailure("invalid_input");
  return new CliFailure("unavailable");
}

export function runAxisCli(argv: readonly string[], io: CliIo = { stdout: (text) => process.stdout.write(text), stderr: (text) => process.stderr.write(text) }): number {
  let parsed: ParsedArguments | undefined;
  let mode: OutputMode = "human";
  let commandName = "unknown";
  const opened: Closable[] = [];
  try {
    parsed = parse(argv);
    mode = outputMode(parsed);
    if (has(parsed, "version")) {
      if (argv.length !== 1) throw new CliFailure("invalid_input");
      io.stdout(`axis ${packageVersion()}\n`);
      return 0;
    }
    if (has(parsed, "help")) {
      if (parsed.positional.length > 2) throw new CliFailure("invalid_input");
      io.stdout(help());
      return 0;
    }
    const requested = command(parsed);
    commandName = requested;
    const result = commandHandlers[requested]({ parsed, own: (resource) => { opened.push(resource); return resource; } });
    emit(io, mode, requested, result);
    return 0;
  } catch (error) {
    if (parsed !== undefined) {
      try { mode = outputMode(parsed); } catch { mode = "human"; }
    }
    emitFailure(io, mode, commandName, cliFailure(error));
    return 1;
  } finally {
    for (const resource of opened.reverse()) resource.close();
  }
}

function projectFromTarget(target: ConfiguredTarget): BindingProjection {
  return { configuredTarget: target, currentEvidence: { status: "unavailable", source: null, observedAt: null }, historicalRunBinding: { status: "not_requested", runId: null, target: null } };
}
