# Executor registration contract

## Agent instruction set

Every dispatched task carries an instruction set assembled from two Axis sections, in this order.

- **Session response.** Progress and results always return through the Axis session, which Axis publishes as an Agent Activity authored by the Axis App User; no write anywhere else replaces that answer. Agents use the tools and authentication already configured for the Run, within the task's scope, report unavailable capabilities and failed operations honestly, verify the outcome of every write, and never claim an operation succeeded unless it did. The set provisions no credentials and bypasses no access control.
- **Linear execution clients.** Present only for an adapter that injects a per-Run Linear execution environment, and derived from `LINEAR_EXECUTION_STATUS` in the environment the child actually receives. Permission to operate on Linear — in-scope comments, replies and status changes, with the requested destination and the exact parent for a threaded reply — lives in this section rather than in the session response, so it is sent **only when Axis Linear execution clients are actually available for the Run**. A Run whose status is `disabled` or carries no execution authority receives no permission to use Axis clients. Its unavailable branch names the exact cause and is sent only when the clients really are unavailable. Both branches state that an Axis Linear status never disables Linear tooling the host account already has, because stating that once in a configurable section would let an agent that sets `sessionResponse` to `null` lose it while still receiving the unavailable paragraph.

Each rule appears in exactly one section. Codex receives the sections as thread developer instructions, after any developer instructions the operator configured for the app-server itself and its own profile-routing policy, and followed by a short Codex transport block; the other adapters prepend the sections to task input only when a start creates a native session. A continuation resumes that session, whose history already carries them, and sends the instruction alone.

Linear Agent guidance is supplied separately from these Axis sections, inside the dispatched instruction's `<linear-agent-guidance>` section. Relay validates the signed event's `guidance` array and preserves the supplied rule order: workspace guidance precedes team guidance, with the nearest team's rule last. Codex receives that instruction in `turn/start` input; Claude Code receives it in its user message. Router, Node and Worker forward the same instruction string; no Runtime schema extension is needed.

Guidance is event-scoped: only an event carrying rules adds a section. Exact ingress retries reuse the encrypted composed input. Continuations, steer and queued redispatch without rules add no section; native session history remains owned by the executor. Axis does not persist guidance as conversation configuration or supplement a duplicate delayed `created` event after a prompt-first start. `promptContext` is not forwarded, so its repeated guidance is not duplicated.

### Per-agent configuration

Each Worker deployment is one agent. Only the session response is configurable;
Axis supplies no writing-style preference. Configure the selected Worker's TOML:

```toml
[nodes.workers.instructions]
sessionResponse = "Report progress and verified results through this session."
```

Omit `sessionResponse` to retain the default, or use `omitSessionResponse = true`
to suppress it. The two cannot be set together. A supplied response must be a
nonblank string of at most 64 KiB. Unknown keys or invalid values fail parsing
before the journal opens or an executor spawns. The Linear execution paragraph
is derived from the Run's actual child environment and cannot be overridden by
these instructions. Managed services require
[configuration replacement](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade) for edits.

## Optional Linear execution clients

With the selected Worker's `linearExecution = true`, Codex allocates a separate app-server
process per Run and starts or resumes a durable thread. Claude Code reads the
same TOML setting and uses a separate native CLI process per turn. A validated
Linear start supplies the private Task/Attempt/Run/workspace tuple for a Relay
execution bearer; missing context rejects before native launch.

A successor Run issues its own bearer and resumes the predecessor's thread or
session. Terminal settlement revokes the turn's grant. Active native Runs are
not recovered after Worker restart; a successor may continue a terminal
predecessor only after `reattestTerminal` proves its retained binding. Codex also
permits a predecessor parked on `waiting_for_input`. Native history and
credential snapshots remain on the service account, so keep unrelated tenants
out of that account. See [client setup and verification](LINEAR-EXECUTION-CLIENTS.md).

## Worker composition

Worker selects exactly one reviewed provider-neutral executor registration at construction. A registration binds a runtime kind, executor ID, opaque Runtime v1 `MethodRef`, supported `start`/`continue`/`cancel`/`observe` operations, a provider adapter, provider-owned binding validation/equality, and Axis-safe lifecycle projection. Duplicate, malformed, absent, or non-exact runtime/executor/method selections reject before the Worker accepts a command. Worker never branches on a provider name.

Each production binary constructs exactly one closed registration. `axis-codex-worker` uses the package-local `@openai/codex` executable with hardcoded `codex` / `v1/codex.execute` for start, continuation, observation, and interruption. `axis-claude-worker` uses only the canonical executable supplied by `nodes.workers[].claudeExecutable` and hardcoded `claude-code` / `v1/claude-code.execute` for start, same-session continuation, observation, interruption, and steer. Native Agent version strings are not admission inputs. Neither binary dynamically selects a provider, resolves a provider from `PATH`, invokes a shell, or falls back to the other registration.

Before any start or continuation reaches a registration, Worker resolves the frozen opaque workspace reference through its local allowlisted catalog, applies the organization-root fallback when necessary, and supplies the canonical result as the shared `workingDirectory`. Codex uses that directory for app-server `thread/start` and every `turn/start`, so Git-root discovery, `AGENTS.md`, repository environment, and worktree context come from the selected checkout. Claude Code spawns directly in it. Provider input and output cannot change it; an unavailable organization root fails before the adapter call.

The executor accepts only the exact `{ threadId, turnId }` binding returned by its owned app-server process. Initial start creates one thread/turn; continuation starts the next turn on the exact terminal predecessor; cancel interrupts only the exact active turn. An interrupt acknowledgement is nonterminal. Only the matching observed interrupted terminal becomes cancellation truth.

Every package-local Codex thread start/resume and turn start pins the native
approval policy to `never` and the sandbox to `danger-full-access`. Axis adds no
filesystem sandbox, writable-root list, network restriction, or native approval
prompt. The host account's access is the effective filesystem and network boundary.
The existing structured `item/tool/requestUserInput` flow remains separate.
Any other server-request method is still native ambiguity and fails closed.

## Codex message projection compatibility

`item/completed` agent messages validate required identity, timestamp, text, and
known metadata types without requiring an exact provider key set. Optional
`phase`, `delivery`, `memoryCitation`, and `questions` may be absent or null.
Additional fields are ignored and never projected. Questions metadata does not
create a waiting-input transition; only the existing structured request path
owns that transition. Commentary normalization, final-response sanitization,
truncation, and unambiguous final-answer selection apply.

Rejected agent messages emit `axis.codex.projection_rejected` with one fixed
reason: `invalid_agent_message`, `invalid_commentary_text`, or
`invalid_response_text`. These diagnostics contain no provider text, metadata,
identifiers, or credentials. A fallback means no usable terminal candidate was
selected; Worker outbox delivery confirms Router receipt, not Linear activity
creation. Investigate Relay/provider delivery separately before claiming Linear
visibility.

## Codex Agent Profile registry and readiness

The Codex Worker owns a closed registry for `explorer`, `architect`, `reviewer`, and `worker`. Before every app-server process generation it resolves only `<codexHome>/agents/<role>.toml` (TOML `nodes.workers[].codexHome`, otherwise the inherited `CODEX_HOME` or `~/.codex`), follows links in Axis, verifies the canonical target is a readable regular file with matching nonblank profile metadata, and launches the package-local app-server with a TOML-quoted `-c agents.<role>.config_file=<canonical-path>` override for every valid role. Paths and profile content remain internal. The app-server generation runs `initialize`, sends `initialized`, then reads effective configuration. It verifies app-server identity without comparing its native version and verifies that every configured role points to the canonical path. The result is cached only while that child process lives; both the snapshot and source resolution are discarded on process failure/replacement.

`GET /v1/worker/readiness` is bearer-authenticated and exposes only a closed snapshot. It never probes or reports model availability (`modelAvailability: "not_checked"`); each role has separate `availability` and initial `attempt: "not_attempted"`; it returns stable degraded reasons without host paths, profile text, raw configuration, or native diagnostics. A capability/config/profile problem does not select another model or provider.

The developer instructions placed on every new Codex thread require independent per-role attempts. One failed role cannot mark another unavailable. `nodes.workers[].profileFallbackPolicy` (`allow` or `deny`) is the Worker maximum, defaulting to `allow`; default-runtime fallback requires both this policy and request policy. A permitted fallback must state `model_active`, `requested_profile_unavailable`, `runtime_role_default`, and that profile-specific instructions were not loaded.

Bindings are persisted as opaque JSON without schema rewriting. The selected registration validates both returned and historical bindings before use; foreign, malformed, mismatched, or ambiguous bindings fail closed; post-restart use requires adapter attestation. There is no invented binding, alternate executor, or provider fallback. Claude continuation uses the exact retained session ID; across a Worker restart it continues a terminal binding only through its `reattestTerminal` proof and never recovers an active one. Linear-enabled Codex continuation after a restart requires its own attestation, and also covers a predecessor parked on `waiting_for_input`: the proof pages the app-server's non-archived `thread/list` view for the binding's thread, with all supported top-level `sourceKinds` values and `modelProviders: []`. A listed thread is attested; an unlisted one (archived, deleted, never existed) is `null`; a JSON-RPC error, malformed page, or the 50-page cap rejects. Only `LinearCodexExecutor` asks, through its credential-free base app-server; the plain executor answers `null` because its `continue` never resumes a thread its own app-server did not load. `recover()` still returns `null`.

Claude startup runs bounded, output-limited help and authentication probes;
every distinct start or continuation repeats them before launch. Native version
strings are not admission inputs. Authentication stdout is discarded; only exit
status is used. Exact request replay shares the result without probing or
spawning again. A start validates one canonical lowercase UUID and invokes the
configured executable once with `-p`, `--session-id`, stream-JSON/verbose/partial
output, `--input-format stream-json`, `--replay-user-messages`, and
`--permission-mode bypassPermissions`. Missing help capabilities fail preflight.

The adapter passes neither `--tools` nor `--safe-mode` and forwards the native
service environment after removing inherited deployment and Linear values and
injecting the current Run's Linear variables. It writes a fresh
UUID-tagged user message to stdin and keeps stdin open for steer or interrupt.
Matching `system/init.session_id` is required before acceptance. Before init and
after result, only exact same-session SessionStart hook messages and
`command_lifecycle` messages naming an adapter-written UUID are skipped.
Malformed, missing, duplicate, mismatched, truncated, over-limit, or unexpected
output remains observational ambiguity, never a reason to relaunch. Fixed
`claude-code.*` reason labels enter the Worker journal without native text.

## Safe lifecycle projection

Executor contract `1.0.0` includes explicit lifecycle progress and `waiting_for_input`. `progressed` requires either `{ kind: "generic" }` or `{ kind: "commentary", safeMessage }`; waiting carries only a validated structured elicitation. For Axis's supported native request shape, `options[0]` is the canonical recommendation source and becomes the mandatory `recommendedOptionId`; no label or Markdown inference is used. Every option requires a unique `axis-reply-…` value recomputed only from `decisionId + optionId`; labels are display-only and label-only replies are rejected. Display fields reject credentials, absolute filesystem paths including assignment and bare/protocol-relative `//` forms, file URIs, URL/DSN userinfo, credential query/fragment values, controls, bidirectional controls, and normalized duplicate labels while retaining valid explicit-scheme URLs. The Codex transport distinguishes JSON-RPC responses, server requests, and notifications. It captures only the exact-bound blocking `item/tool/requestUserInput` shape, never sends a response or fake answer, and issues exact `turn/interrupt`. Waiting is emitted only after that interrupt response is acknowledged and the same turn reaches the interrupted terminal. Unsupported shapes, mismatches, duplicates, competing terminals, native ambiguity, or transport loss fail closed. Router/Worker identity, correlation, operation, and sequence remain exact; raw native IDs are replaced by deterministic hashes and never projected.

Worker checks `observe` before `start`, and checks each requested capability before its adapter call. Unsupported operations fail with HTTP 503, `axis.worker.operation_unsupported`, and `The executor operation is not supported.` before native side effects. Worker persists `run.accepted` after observing the first exact `started` fact. Only later observed lifecycle facts enter the Router-facing outbox through the selected registration's safe projection. For the package-local Codex adapter, only an exact bound `item/completed` `agentMessage` with `phase: "commentary"` is eligible for progress; `item/started`, `item/updated`, raw/unknown items, reasoning, tools, plans, final answers, null phases, malformed shapes, and foreign bindings are silent. Text is rejected for lone surrogates or prohibited control/bidirectional characters, normalized to NFC with collapsed whitespace, trimmed, scalar-safe truncated to 512 Unicode values, and only then enters lifecycle state. Worker independently requires the commentary to already equal that normalized form before journaling. Raw native items are never journaled.

The Worker journals commentary under exact `runId` scope. The first unique safe message is immediate; later unique messages share a non-sliding 30-second window whose latest pending immutable envelope replaces earlier pending content. A waiting terminal seals progress and is journaled once with its elicitation before Router delivery. On startup, binding/accepted/running records receive one bounded registration-native `recover()` proof: it must return the original binding and either a resumed observer or one authoritative terminal. A missing adapter, timeout, unavailable, malformed, or mismatched proof (including invalid accepted reconstruction, observer, or terminal) is durably made `observational`, preserving retained binding/accepted/intent evidence and never starting, continuing, cancelling, or replaying old native work. Only an observational persistence failure keeps the global startup barrier; a durably quarantined Run remains `recovery_required` **for that Run alone** and does not block other Runs/issues. Success remains `axis-evidence://<run-id>/codex-terminal`; failure and interruption retain their fixed safe errors. Every emitted Worker event is persisted before Router delivery, and transient delivery retries never rerun the adapter.

Claude `continue` uses the original `{ runtimeKind, sessionId }` binding. A continuation spawns one new CLI process with `--resume <sessionId>` in the original canonical working directory, never `--continue`, a new UUID, or a fallback start. Claude's native local session persistence is enabled so the next process can restore context; Axis does not read or journal the transcript. Both operations retain request digests and results, reject cross-operation request-ID reuse, and reserve a session before asynchronous probes to prevent overlapping turns. The preceding matching result, process exit, and delivered terminal must all be known before replacing its observer; each new Run gets a fresh lifecycle sequence. Missing bindings, changed directories and active/unobserved/ambiguous turns cannot be continued. Spawned ambiguous turns remain unavailable without replay. Recovering a turn that was active when the Worker restarted remains unsupported; a terminal one continues after a restart as described below. A session without a native transcript cannot be resumed.

Claude `cancel` writes one `{"type":"control_request","request_id":"<uuid>","request":{"subtype":"interrupt"}}` line on the current turn's stdin, which keeps open for the turn. `cancel` returns `interrupt_requested` only when the `control_response` with the same `request_id` arrives with `subtype: "success"` within `CONTROL_RESPONSE_TIMEOUT_MS` (10 seconds). It returns `failed` / `executor.interrupt_failed` if the session is not running, the stdin write fails, the answer is not a success, the wait times out, or the session settles first. A timeout does not make the session ambiguous. The same request ID with the same binding shares one result. The same ID with another binding returns `executor.idempotency_conflict`. A malformed request or binding returns `executor.invalid_request`. A `control_response` is never progress. A `control_response` with any other `request_id`, or one that arrives with nothing pending (including a late answer after the wait timed out), makes the session ambiguous. The acknowledgement is not the terminal. The turn ends `cancelled` only if an interrupt was written and that turn's `result` is `is_error: true`. The exit must also carry no signal, or only one the result reap sent. An interrupted turn may exit nonzero after stdin closes. If the turn had already succeeded when the interrupt landed, it stays `succeeded`. A cancelled turn is a delivered terminal with a result, so it can be continued with `--resume`, like any other finished turn.

The executor contract has an optional `steer` capability (`ExecutorSteerRequest` with `binding` and an ephemeral `instruction`, `ExecutorSteerResult`, `Executor.steer?`). A registration may list `steer` only if its adapter implements it. Claude declares it (`["start", "continue", "cancel", "steer", "observe"]`). A steer hands one more instruction to the **running** turn without interrupting it. The adapter writes it on the open stdin as a uuid-tagged user message with the instruction alone, the same content a continuation sends. It returns `steered` once the write has flushed. It does not wait for the echo, because a message that reached stdin cannot be withdrawn. With `--replay-user-messages` the CLI echoes `{"type":"user","uuid":<same>,"isReplay":true,...}` the moment it consumes the message. That echo settles the steer and is otherwise ordinary progress. The adapter handles these cases:

- **Steer during a tool call.** The CLI consumes the steer at the tool boundary, and the echo follows that tool's `tool_result`. The same turn ends with one `result`, which follows the steer.
- **Steer while the model writes its final text.** That turn ends first: `result` #1 does not include the steer. A `result` that arrives while a steer is still unechoed is not the Run's terminal. The session stays `running`, stdin stays open, and one further same-session `system/init` is accepted, without emitting `started` again. The echo and `result` #2 follow, and #2 decides the Run. Denials recorded by either turn still count. A second `init` with no steer pending stays ambiguous.
- **Stop with a steer queued.** Stop means everything stops. The adapter marks the session terminate-after-interrupt in either case: a steer is still pending when the interrupt is written, or the `control_response` lists one in `response.response.still_queued`. When the interrupted turn's `result` arrives, the adapter does not let the CLI run the queued turn. It closes stdin and starts the reap at once: SIGTERM, then SIGKILL after the kill grace, both recorded as reap signals. Until the exit, stdout is drained instead of parsed. The process may keep writing until the signal lands: `command_lifecycle` lines, a new `system/init`, and the queued echo. A partial trailing line is ignored. A matching `control_response` is still honoured. This relaxation applies only to that reap. The Run ends `cancelled` under the rule, and the queued steer is discarded.

`steer` returns `executor.steer_too_late` (runtime, not retryable) if the turn can no longer take it: it is not running, its stdin is closed, or a cancel was already written. A caller should then fall back to an ordinary continuation. It returns `executor.capability_unavailable` if the executor is closed or holds no session for the binding. A malformed request, including a blank instruction, returns `executor.invalid_request`. The same request ID with the same binding and the same instruction digest shares one result, and any other reuse of that ID is `executor.idempotency_conflict`. The Worker steer command, the Node's forwarding and Router's opt-in `router.nativeSteer` carry it end to end. A successful steer proves delivery to the native turn, not that the model followed it.

Claude continuation across Worker restart requires a private session record.
Once a session is terminal, its result has been read, and its terminal delivered
to Worker, the adapter records session ID and working directory in
`nativeSessionStorePath`. The TOML Worker projection derives it as
`<dataDir>/worker-<id>-native-sessions.json`; it has no operator path override.
Private atomic snapshots are described in [Persistence](PERSISTENCE.md).

`reattestTerminal` only checks that record; it spawns nothing and does not prove
the native transcript still exists. Without the exact recorded working directory,
continuation is refused as `axis.worker.continuation_unresumable`. A resume that
returns only the recognized missing-session error result for the same session,
no init, and exit 1 is definite `executor.launch_failed`. Other pre-init output,
post-error output, exit 0, or a signal remains ambiguous. Active Runs cannot be
recovered by this record.

Claude maps progress to `1/1` and `Claude Code session is progressing.`, success to `axis-evidence://<run-id>/claude-code-terminal`, terminal failure to `claude_code_terminal_failed` / `Claude Code execution failed.`, cancellation to `execution_cancelled` / `Execution was cancelled.` (the same projection as Codex), and a failure whose turn recorded a denied tool call (a `system/permission_denied` message or a non-empty result `permission_denials`) to `claude_code_permission_denied` / `Claude Code was denied at least one tool call, so the requested work may be incomplete. Check the work itself before relying on it.` That refinement travels only as the in-process lifecycle `failureKind: "permission_denied"`, which Worker accepts from a `claude-code` binding alone; the `run.failed` wire shape is unchanged. Definite pre-side-effect `runtime_missing`, `runtime_incompatible`, `authentication_required`, `capability_unavailable`, and `launch_failed` results remain non-accepted with no lifecycle; a task CLI that exits non-zero with nothing on stdout before `system/init` is `launch_failed`. `launch_ambiguous` is observational. Every same-Run startup error is non-retryable; Worker maps only an exact closed error tuple and treats malformed or extended tuples as ambiguity. A `succeeded` outcome carries the terminal response: the native `result` line's own `result` text -- the terminal turn's final answer, the last one written when a steer opened a further turn -- passed through the shared `normalizeExecutorTerminalResponse` redaction boundary and truncation, falling back to the contract's fixed fallback body rather than no response at all when that text is missing or refused; `sourceEventId` hashes `sessionId` together with the Worker's own per-call `requestId`, because `--resume` reuses the same `sessionId` across every continuation. Failed, cancelled, and permission-denied turns carry no response, and neither does a terminal recovered after a Worker restart, so Linear still shows the no-result summary for that case.

## Export boundary

`CodexExecutor`, its registration factory, and generic registration types are
package-root exports for reviewed in-process adapters. Claude implementation,
configuration, and registration symbols are package-internal and shipped for
`axis-claude-worker`. Worker composition uses `runtimeKind` and a closed
`registrations` collection; it does not load a compatibility adapter.

## Native Other answers

Codex accepts native `isOther: true` within the existing one-question, non-secret, two/three explicit-option subset. It produces local elicitation `1.0.0-other`, with a separate deterministic Other reply identity; `isOther: false` produces `1.0.0-fixed-choice`. Axis still interrupts and waits for the matching interrupted terminal instead of answering the server request. Human continuation starts a new turn on the captured thread; native capability still depends on the actual app-server mode/configuration. Axis does not force Plan mode or enable experimental APIs.
