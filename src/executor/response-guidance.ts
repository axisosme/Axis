/** Session response and environment-derived instructions shared by native executors. */

/** Where an answer goes and what makes it honest. Per-agent configurable. */
export const DEFAULT_SESSION_RESPONSE_GUIDANCE = `Axis session response:
- Always return progress and results through the current Axis session, including after a successful write somewhere else. Axis publishes the session response as an Agent Activity authored by the Axis App User, and no other write replaces that answer.
- Use the tools and authentication already configured for this Run, within the task's scope. This guidance grants no credentials and bypasses no access restriction.
- Report unavailable capabilities and failed operations honestly. Verify the outcome of every write, include that outcome and a link when one is available, and never claim an operation succeeded unless it actually did.`;

/**
 * One agent's configuration. An absent key takes the Axis default; `null`
 * sends nothing for that section. The distinction is why this is not read
 * positionally: "I did not configure this" and "I configured this to be empty"
 * are different deployments.
 */
export interface AgentInstructionOverrides {
  readonly sessionResponse?: string | null;
}

/** One agent's resolved instruction set. `null` means the agent sends no such section. */
export interface AgentInstructions {
  readonly sessionResponse: string | null;
}

/** What an agent that configures nothing sends. */
export const DEFAULT_AGENT_INSTRUCTIONS: AgentInstructions = Object.freeze({
  sessionResponse: DEFAULT_SESSION_RESPONSE_GUIDANCE,
});

/** Applies one agent's configuration over the defaults. */
export function resolveAgentInstructions(overrides?: AgentInstructionOverrides): AgentInstructions {
  if (overrides === undefined) return DEFAULT_AGENT_INSTRUCTIONS;
  return Object.freeze({
    sessionResponse: overrides.sessionResponse === undefined ? DEFAULT_SESSION_RESPONSE_GUIDANCE : overrides.sessionResponse,
  });
}

/**
 * How an agent reaches Linear for this Run, and why nothing else is a
 * substitute. One text for every native executor that injects a per-Run Linear
 * execution environment, so the Codex and Claude Code agents are told the same
 * rules. The Codex adapter
 * delivers it as thread developer instructions; the Claude Code adapter
 * delivers it in the start turn's input.
 *
 * `undefined` means the caller injects no Linear environment at all and has
 * nothing truthful to say about it. A Worker without Linear execution access
 * (`disabled`) says nothing either: its agents have no Axis clients to describe.
 * Every status this does describe is read from `LINEAR_EXECUTION_STATUS` in the
 * environment the child actually receives, never from a separate flag that could
 * disagree with it.
 *
 * Permission to operate on Linear lives here rather than in the session
 * guidance so a Run receives permission to use Axis clients only when its
 * execution environment actually provides them.
 */
export function linearExecutionClientInstructions(environment: Readonly<Record<string, string | undefined>> | undefined): string | null {
  if (environment === undefined || environment.LINEAR_EXECUTION_STATUS === "disabled") return null;
  if (environment.LINEAR_EXECUTION_STATUS !== "available") {
    return [
      "Axis Linear execution clients are unavailable because this Run carries no Axis Linear execution authority.",
      "Report that exact cause when an Axis Linear execution client is required. Do not describe the clients as missing, uninstalled, logged out, or unreachable; none of those has been observed.",
      "This status does not disable other Linear tools or authentication already configured for the host account; use them only when they are authorized for the task.",
    ].join("\n");
  }
  return [
    "Axis Linear execution clients are available for this Run through Relay.",
    "Linear operations are authorized within the task's scope, including posting comments or replies and updating issue status.",
    "When this turn reaches a definite conclusion, post it as a top-level Comment on the issue this AgentSession belongs to. This default is the request for that one Comment: do not wait for the user to ask for it, and do not skip it because a skill or tool permits writes only on explicit user request. A reader of the ticket only sees that Comment if you post it, and nothing else creates it for you. When this turn is a clarifying question, a partial finding, a failure, a blocker, or otherwise not yet definite, do not post a top-level Comment -- reply in the controlling AgentSession thread only. When the user explicitly names a destination, follow that instead of this default, and resolve the exact parent for a threaded reply.",
    "The Axis session response always posts to this AgentSession thread regardless of that choice, and is never a substitute for a top-level Comment or the reverse; when you do post a Comment, keep the session response brief -- confirm it was posted and give its link -- instead of repeating the full conclusion in both places. Confirm a Comment landed by rereading it through Linear, never by posting a second one to check.",
    "Use existing Linear CLI commands with the injected environment when the AgentSession identity is required. Other Linear tools and authentication already configured for the host account remain available within the task's scope.",
    "For the official SDK, import process.env.LINEAR_EXECUTION_SDK_MODULE and call axisLinearSdkFromEnvironment(). Its generated models retain the Relay request adapter.",
    "Access for this Run ends at LINEAR_EXECUTION_EXPIRES_AT and is never renewed in place; a later turn of this session receives its own access.",
    "A refusal carries a specific Relay code. Report it exactly: expired, revoked, execution_denied, installation_denied, authorization_changed and workspace_mismatch are different causes, and none of them means a client is missing.",
    "Never print or persist execution credentials. Inspect partial results or unknown write outcomes before another write; do not retry automatically.",
  ].join("\n");
}

/**
 * The guidance sections one agent sends, in order, with empty ones dropped.
 * Exported because the Codex adapter inserts its own transport block between
 * these and must not reimplement the ordering or the emptiness rule.
 */
export function axisAgentGuidanceSections(
  linearEnvironment: Readonly<Record<string, string | undefined>> | undefined,
  instructions: AgentInstructions,
): readonly string[] {
  return [instructions.sessionResponse, linearExecutionClientInstructions(linearEnvironment)]
    .filter((section): section is string => section !== null && section.trim().length > 0);
}

/**
 * Native task input for one Axis turn. Only the turn that creates a native
 * session carries the guidance; a continuation resumes that session, whose
 * history already holds it, and sends the instruction alone.
 *
 * That rule holds for the Linear paragraph too, and only because its wording is
 * status-describing rather than credential-carrying: the status is a function of
 * the Worker's own configuration and the Run's source system, both fixed for a
 * native session's life, and the paragraph tells the agent outright that a later
 * turn receives its own access and that `LINEAR_EXECUTION_EXPIRES_AT` is read from
 * the environment. Each continuation still spawns a process carrying its own
 * freshly issued credential; nothing about the access itself is
 * inherited from the start turn's text.
 *
 * It holds for a per-agent override too: the agent's configuration is read once
 * when the Worker starts, so every turn of a native session was started under
 * the same instruction set the start turn carried.
 */
export function axisTaskInput(
  instruction: string,
  operation: "start" | "continue",
  linearEnvironment: Readonly<Record<string, string | undefined>> | undefined,
  instructions: AgentInstructions,
): string {
  return operation === "start"
    ? [...axisAgentGuidanceSections(linearEnvironment, instructions), `Task:\n${instruction}`].join("\n\n")
    : instruction;
}
