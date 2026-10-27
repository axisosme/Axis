export type RelayErrorCode =
  | "linear.ingress.body_too_large"
  | "linear.ingress.invalid_signature"
  | "linear.ingress.stale_timestamp"
  | "linear.ingress.malformed"
  | "linear.ingress.delivery_conflict"
  | "linear.ingress.logical_event_conflict"
  | "linear.ingress.storage_unavailable"
  | "linear.auth.invalid_state"
  | "linear.auth.revoked"
  | "linear.auth.team_forbidden"
  | "linear.provider.rejected"
  | "linear.provider.ambiguous"
  | "linear.provider.conflict"
  | "axis.relay.invalid_transition"
  | "axis.relay.missing_instruction"
  | "axis.relay.issue_sync_failed"
  // Definite issue-sync causes; `axis.relay.issue_sync_failed` covers the rest.
  | "axis.relay.issue_sync_not_found"
  | "axis.relay.issue_sync_malformed"
  | "axis.relay.issue_sync_team_changed"
  | "axis.relay.issue_sync_read_rejected"
  | "axis.relay.issue_sync_update_rejected"
  | "axis.relay.not_ready"
  | "axis.relay.vault_unavailable"
  | "axis.relay.schema_version_unsupported";

export class RelayError extends Error {
  readonly code: RelayErrorCode;
  readonly statusCode: number;
  readonly retryable: boolean;

  constructor(code: RelayErrorCode, safeMessage: string, statusCode = 500, retryable = false) {
    super(safeMessage);
    this.name = "RelayError";
    this.code = code;
    this.statusCode = statusCode;
    this.retryable = retryable;
  }
}

export function safeError(error: unknown): { code: string; safeMessage: string; retryable: boolean } {
  if (error instanceof RelayError) {
    return { code: error.code, safeMessage: error.message, retryable: error.retryable };
  }
  return {
    code: "axis.relay.internal",
    safeMessage: "The relay could not complete the operation.",
    retryable: false,
  };
}

export type StartupFailureStage = "Validation" | "Router" | "Worker" | "Executor";
export type StartupFailureReason = "instruction_missing" | "authorization_unavailable" | "durable_state_conflict" | "validation_failed" | "route_unavailable" | "state_conflict" | "request_rejected" | "policy_forbidden" | "worker_identity_changed" | "workspace_unavailable" | "operation_unsupported" | "native_session_unavailable" | "runtime_unavailable" | "runtime_incompatible" | "authentication_required" | "probe_timeout" | "capability_unavailable" | "outcome_unknown"
  // A created start refused while its input was verified.
  | "linear_capability_unavailable" | "credential_unavailable" | "user_unavailable" | "owner_unverified" | "issue_unavailable" | "issue_mismatch"
  | "activity_mismatch" | "activity_session_mismatch" | "activity_not_prompt"
  // A created start whose issue could not be synchronized before dispatch.
  | "issue_not_found" | "issue_context_malformed" | "issue_team_changed" | "issue_update_rejected"
  /**
   * Like `outcome_unknown`, but not yet final: Axis has not confirmed rather
   * than cannot confirm. Not produced by `classifyStartupFailure`; only
   * `retainStartupFailure` (`./linear/channel.ts`) knows an answer is still owed.
   */
  | "outcome_pending";
export interface StartupFailure { readonly certainty: "definite" | "ambiguous"; readonly stage: StartupFailureStage; readonly reason: StartupFailureReason; readonly safeCode: string; }
interface DefiniteCopy { readonly stage: StartupFailureStage; readonly reason: Exclude<StartupFailureReason, "outcome_unknown" | "outcome_pending">; readonly reasonCopy: string; readonly actionCopy: string; }

function routeUnavailable(): DefiniteCopy { return { stage: "Router", reason: "route_unavailable", reasonCopy: "No eligible execution route is available.", actionCopy: "Restore the execution route, then create a new Agent session." }; }
function routerStateConflict(): DefiniteCopy { return { stage: "Router", reason: "state_conflict", reasonCopy: "Router state rejected the startup command.", actionCopy: "Resolve Router state, then create a new Agent session." }; }
function workerRequestRejected(): DefiniteCopy { return { stage: "Worker", reason: "request_rejected", reasonCopy: "The Worker rejected the startup request before execution.", actionCopy: "Restore Worker connectivity and configuration, then create a new Agent session." }; }
function executorFailure(reason: DefiniteCopy["reason"], reasonCopy: string, actionCopy: string): DefiniteCopy { return { stage: "Executor", reason, reasonCopy, actionCopy }; }

const definiteStartupFailureByCode: Readonly<Record<string, DefiniteCopy>> = Object.freeze({
  "axis.relay.missing_instruction": { stage: "Validation", reason: "instruction_missing", reasonCopy: "The created event did not include an executable instruction.", actionCopy: "Send a new prompt to start the session." },
  "linear.auth.revoked": { stage: "Validation", reason: "authorization_unavailable", reasonCopy: "Linear authorization for this session is unavailable.", actionCopy: "Restore Linear access, then send a new prompt." },
  "linear.auth.team_forbidden": { stage: "Validation", reason: "authorization_unavailable", reasonCopy: "Linear authorization for this session is unavailable.", actionCopy: "Restore Linear access, then send a new prompt." },
  "linear.ingress.logical_event_conflict": { stage: "Validation", reason: "durable_state_conflict", reasonCopy: "The created session conflicts with retained Relay state.", actionCopy: "Create a new Agent session before retrying." },
  "axis.relay.invalid_transition": { stage: "Validation", reason: "durable_state_conflict", reasonCopy: "The created session conflicts with retained Relay state.", actionCopy: "Create a new Agent session before retrying." },
  "axis.relay.issue_sync_failed": { stage: "Validation", reason: "validation_failed", reasonCopy: "Relay could not synchronize the verified comment request safely.", actionCopy: "Create a new Agent session before retrying." },
  // Why a created start's input could not be verified. `./linear/channel.ts` maps
  // `LinearPromptRejectionReason` onto these codes, which only select copy and are never persisted.
  // `owner_unverified` is one sentence for every owner-verification failure, by design.
  "axis.relay.linear_capability_unavailable": { stage: "Validation", reason: "linear_capability_unavailable", reasonCopy: "Relay cannot read the Linear users and issues it needs to verify this session.", actionCopy: "Restore Axis's Linear integration, then create a new Agent session." },
  "axis.relay.linear_credential_unavailable": { stage: "Validation", reason: "credential_unavailable", reasonCopy: "Axis has no usable Linear credential for this workspace.", actionCopy: "Reconnect Axis to Linear, then create a new Agent session." },
  "axis.relay.linear_user_unavailable": { stage: "Validation", reason: "user_unavailable", reasonCopy: "Linear did not return the person who started this session.", actionCopy: "This is often temporary; create a new Agent session to retry." },
  "axis.relay.owner_unverified": { stage: "Validation", reason: "owner_unverified", reasonCopy: "Axis could not verify the person who started this session.", actionCopy: "Create a new Agent session from a signed-in Linear account." },
  "axis.relay.issue_unavailable": { stage: "Validation", reason: "issue_unavailable", reasonCopy: "Linear did not return the issue this session was started on.", actionCopy: "Check that Axis can still read this issue, then create a new Agent session." },
  "axis.relay.issue_mismatch": { stage: "Validation", reason: "issue_mismatch", reasonCopy: "The issue Linear returned does not match the issue and Team this session was started on.", actionCopy: "Create a new Agent session on the issue you want Axis to work in." },
  "axis.relay.activity_mismatch": { stage: "Validation", reason: "activity_mismatch", reasonCopy: "The initial Activity Linear returned is not the one this session's start named.", actionCopy: "Create a new Agent session to retry." },
  "axis.relay.activity_session_mismatch": { stage: "Validation", reason: "activity_session_mismatch", reasonCopy: "The initial Activity Linear returned belongs to a different Agent session.", actionCopy: "Create a new Agent session to retry." },
  "axis.relay.activity_not_prompt": { stage: "Validation", reason: "activity_not_prompt", reasonCopy: "The initial Activity of this session is not a prompt, so it carries no instruction to run.", actionCopy: "Create a new Agent session with a prompt." },
  // Definite issue-sync failures. Transient ones are thrown retryably by the provider and never reach here.
  "axis.relay.issue_sync_not_found": { stage: "Validation", reason: "issue_not_found", reasonCopy: "Linear did not find the issue this session was started on.", actionCopy: "Check that the issue still exists and Axis can read it, then create a new Agent session." },
  "axis.relay.issue_sync_read_rejected": { stage: "Validation", reason: "issue_unavailable", reasonCopy: "Linear did not return the issue this session was started on.", actionCopy: "Check that Axis can still read this issue, then create a new Agent session." },
  "axis.relay.issue_sync_malformed": { stage: "Validation", reason: "issue_context_malformed", reasonCopy: "Linear returned this issue's labels, state, or Team in a form Axis cannot verify.", actionCopy: "Ask an Axis operator to check this issue, then create a new Agent session." },
  "axis.relay.issue_sync_team_changed": { stage: "Validation", reason: "issue_team_changed", reasonCopy: "This issue is no longer in the Linear Team this session was authorized for.", actionCopy: "Create a new Agent session on the issue in its current Team." },
  "axis.relay.issue_sync_update_rejected": { stage: "Validation", reason: "issue_update_rejected", reasonCopy: "Linear rejected marking this issue as started and delegated to Axis.", actionCopy: "Check that Axis may update this issue, then create a new Agent session." },
  "axis.relay.internal": { stage: "Validation", reason: "validation_failed", reasonCopy: "Relay could not complete startup validation safely.", actionCopy: "Create a new Agent session before retrying." },
  // Reachable here because `LinearOAuthManager#accessToken` reads the vault during startup validation.
  "axis.relay.vault_unavailable": { stage: "Validation", reason: "validation_failed", reasonCopy: "Relay could not read stored credentials required to start this session.", actionCopy: "Restore Relay's credential store, then send a new prompt." },
  "axis.router.binding_unconfigured": routeUnavailable(), "axis.router.binding_disabled": routeUnavailable(), "axis.router.binding_reference_missing": routeUnavailable(), "axis.router.binding_stale": routeUnavailable(), "axis.router.binding_incompatible": routeUnavailable(),
  // Classification of stored channel reservation safe codes.
  "axis.router.endpoint_unknown": routeUnavailable(), "axis.router.authority_mismatch": routeUnavailable(),
  // The routed Node has no Worker: a route problem, with copy pointing at Worker deployment
  // rather than routing configuration.
  "axis.router.node_worker_unavailable": { stage: "Router", reason: "route_unavailable", reasonCopy: "The routed Node has no Worker available to run this session.", actionCopy: "Deploy a Worker on the routed Node, then create a new Agent session." },
  // The routed Node is draining and refused the start before recording anything; the fix is to wait.
  "axis.router.node_draining": { stage: "Router", reason: "route_unavailable", reasonCopy: "The routed Node is paused for an upgrade or maintenance and is not taking new sessions.", actionCopy: "Nothing was run. Create a new Agent session in a few minutes." },
  "axis.router.malformed": routerStateConflict(), "axis.router.intent_conflict": routerStateConflict(), "axis.router.conflict": routerStateConflict(), "axis.router.not_found": routerStateConflict(),
  // Own copy rather than `routerStateConflict()`: the fix is on the issue, not in Router.
  "axis.router.selection_conflict": { stage: "Router", reason: "state_conflict", reasonCopy: "This request names more than one executor, so Axis cannot tell which one should run it.", actionCopy: "Leave only one WORKER label (WORKER:CODEX or WORKER:CLAUDE) on this issue, then create a new Agent session." },
  // `axis.router.endpoint_unknown` keeps generic route copy above: it covers both an unsatisfiable
  // executor label and an unknown Node target, so executor-specific wording would mislead.
  "axis.worker.malformed": workerRequestRejected(), "axis.worker.unauthorized": workerRequestRejected(), "axis.worker.method_not_allowed": workerRequestRejected(), "axis.worker.body_too_large": workerRequestRejected(), "axis.worker.unsupported_media": workerRequestRejected(), "axis.worker.intent_conflict": workerRequestRejected(),
  // The Worker refused before anything ran; its configuration and its Node inventory entry disagree.
  "axis.router.worker_identity_changed": { stage: "Worker", reason: "worker_identity_changed", reasonCopy: "The Worker's configured identity no longer matches its Node inventory entry, so it refused this session before running anything.", actionCopy: "Ask an operator to align the Worker's configuration with its Node inventory entry, then create a new Agent session." },
  "axis.worker.policy_forbidden": { stage: "Worker", reason: "policy_forbidden", reasonCopy: "Worker policy does not permit this startup request.", actionCopy: "Correct the Worker policy, then create a new Agent session." },
  "axis.worker.workspace_unavailable": { stage: "Worker", reason: "workspace_unavailable", reasonCopy: "No trusted Worker workspace is available.", actionCopy: "Restore the Worker workspace, then create a new Agent session." },
  "axis.worker.operation_unsupported": { stage: "Worker", reason: "operation_unsupported", reasonCopy: "The selected Worker does not support this startup operation.", actionCopy: "Restore the required Worker capability, then create a new Agent session." },
  // A definite refusal to resume the native binding. A replacement session is a fresh identity on
  // current configuration; the channel never replays the failed prompt.
  "axis.worker.recovery_required": { stage: "Worker", reason: "native_session_unavailable", reasonCopy: "The previous native Agent session could not be resumed.", actionCopy: "Create a new Agent session on this issue. It will use the current configuration; the previous Task and route remain unchanged, and Axis will not replay the failed prompt." },
  "axis.worker.runtime_missing": executorFailure("runtime_unavailable", "The configured executor runtime is unavailable.", "Restore the executor runtime, then create a new Agent session."),
  "axis.worker.runtime_incompatible": executorFailure("runtime_incompatible", "The configured executor runtime is incompatible.", "Correct the executor runtime, then create a new Agent session."),
  "axis.worker.authentication_required": executorFailure("authentication_required", "The configured executor runtime requires authentication.", "Restore executor authentication, then create a new Agent session."),
  // A probe timeout says nothing about credentials, so the copy suggests a retry, not re-authentication.
  "axis.worker.probe_timeout": executorFailure("probe_timeout", "The executor preflight probe did not respond in time.", "This is often transient; create a new Agent session to retry."),
  "axis.worker.capability_unavailable": executorFailure("capability_unavailable", "The configured executor capability is unavailable.", "Restore the executor capability, then create a new Agent session."),
});
const ambiguousStartupStageByCode: Readonly<Record<string, StartupFailureStage>> = Object.freeze({
  "axis.router.recovery_required": "Router", "axis.router.protocol_violation": "Router", "axis.router.worker_rejected": "Worker",
  "axis.worker.adapter_invalid": "Worker", "axis.worker.transport_unknown": "Worker", "axis.worker.launch_failed": "Executor", "axis.worker.launch_ambiguous": "Executor",
});

export function classifyStartupFailure(safeCode: string, fallbackStage: StartupFailureStage): StartupFailure {
  const definite = definiteStartupFailureByCode[safeCode];
  if (definite !== undefined && (fallbackStage === "Validation" ? definite.stage === "Validation" : definite.stage !== "Validation")) return { certainty: "definite", stage: definite.stage, reason: definite.reason, safeCode };
  if (fallbackStage === "Validation") return { certainty: "definite", stage: "Validation", reason: "validation_failed", safeCode: "axis.relay.internal" };
  return { certainty: "ambiguous", stage: ambiguousStartupStageByCode[safeCode] ?? fallbackStage, reason: "outcome_unknown", safeCode };
}

export function renderStartupFailure(failure: StartupFailure): string {
  if (failure.certainty === "ambiguous") {
    // "Not yet confirmed" vs "could not confirm" are different claims. Both
    // keep the no-retry fact, and neither promises what appears next: not
    // every ending is guaranteed a later answer (see `retainStartupFailure`).
    if (failure.reason === "outcome_pending") return `Axis has not yet confirmed whether this session started.\nStage: ${failure.stage}.\nReason: The startup outcome is not known yet.\nAxis will not retry automatically.`;
    return `Axis could not confirm whether this session started.\nStage: ${failure.stage}.\nReason: The startup outcome is unknown.\nAxis will not retry automatically.`;
  }
  const copy = definiteStartupFailureByCode[failure.safeCode];
  if (copy === undefined || copy.stage !== failure.stage || copy.reason !== failure.reason) throw new Error("The startup failure classification is not renderable.");
  return `Axis could not start this session.\nStage: ${copy.stage}.\nReason: ${copy.reasonCopy}\n${copy.actionCopy}`;
}
