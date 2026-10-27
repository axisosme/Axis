# Security model

## Independent CLI credentials

The [CLI mode](LINEAR-CLI.md) grants installation-workspace access independently
of Runs. Relay retains credential hashes and checks active installation/OAuth
identity on each call. Device credentials are stored in the service account's
private CLI configuration. Revocation removes the TOML `linear.cliGrants` entry and applies
[configuration replacement](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade) before Relay restarts.
Personal credentials and execution-unavailable environment values are overridden
by the CLI launcher; shared OS-account access remains the trust boundary.

## Optional execution-authenticated Linear API

The [GraphQL proxy](LINEAR-API-PROXY.md) accepts execution capabilities or explicit CLI grants,
binds the Axis token to the verified installation and refuses redirects or client
upstream selectors. It has no Team-subset or issue-level isolation. No credential,
request content, response content or exception enters its private audit. Ambiguous
writes are not replayed; a generated request ID joins safe dispatch/result evidence.
The endpoint is off by default, and enabled-but-missing both authentication modes fails
closed. Original AgentActivity output remains owned by its existing channel.

Execution grants are captured from an admitted live Run, an explicit operator grant
for the installation workspace and the installation revision at dispatch. The
credential transport authenticates a dedicated receiver mapped to the selected
Worker; Node command selection must be proven by the authenticated retained Node
reservation. Existing Node/Worker transport authentication alone grants no
Linear access. OAuth tokens stay in Relay. The native Codex process receives
only its expiring execution bearer; terminal state, revocation, changed grants
or installation permissions invalidate it. Grants and capabilities are memory
only and do not survive Relay restart.

Each enabled Run uses a separate app-server process and one durable thread. The
bearer enters its environment/private shell configuration, never command argv,
prompts or Axis journals. It does reach disk under `CODEX_HOME`: Codex writes
the process environment to a shell snapshot, and a durable thread also retains
its conversation rollout, which every later agent on that host can read. The
Worker uses the service account's normal `~/.codex` by default so native
configuration, tools and authentication match an interactive Codex session; an
explicit `nodes.workers[].codexHome` override remains available. See [durable threads and on-disk history](LINEAR-EXECUTION-CLIENTS.md). Disabled clients get a nonempty refusal credential and
loopback refusal endpoint to override personal CLI configuration. This is
process isolation, not an OS sandbox against mutually hostile same-user
processes. See [support limits and rollback](LINEAR-EXECUTION-CLIENTS.md).

Axis pins each Codex app-server thread and turn to approval policy `never` and
`danger-full-access`. Axis adds no filesystem sandbox, writable-root list,
network restriction, or native approval prompt. The Worker account's complete
filesystem, credential and network reach is therefore available to the agent.
Axis still treats an unexpected native approval RPC as protocol ambiguity;
full access means no such request should be emitted.

## Private execution boundaries

`POST /v1/worker/commands` accepts one configured Worker bearer. Router's only
event route, `POST /v1/router/node-run-events`, accepts one bearer from its
configured Node bindings. Missing, duplicate, malformed, and incorrect
credentials yield indistinguishable `401` responses. POST ingress rejects
queries/content encoding, enforces strict UTF-8 JSON and duplicate-key checks,
and validates closed messages. Worker commands are limited to 64 KiB and use
exactly `1.0.0`; Node event wrappers use their 128 KiB outer limit.

`GET /v1/worker/commands/{commandId}` uses the Worker command bearer, performs
one read-only recorded-result lookup, and returns only that response or
`not_recorded`. Query strings and malformed IDs are `400`. Like readiness, a
non-GET method returns `405` before authentication, exposing route existence
but no command state. `/v1/router/run-events` belongs only to Node's loopback
event listener; it is not a Router route.

Router Node command is Router's only lifecycle,
composed by Relay over an injected port. Its port
must return independently authenticated logical/physical Node context with
every command receipt; Router validates that context and the exact durable
request before accepting Worker or executor identity. Router's listener, whose
only route is `POST /v1/router/node-run-events`, supplies the separate event
authority; Router has no bearer of its own. Its bearer maps in memory to exactly one logical/physical Node and a
closed allowlist of Worker origins; body-provided Node and origin fields have no
authority until they exactly match that mapping. The authenticated Node context
is not discarded at the HTTP boundary: Router compares its logical reference
with the addressed Run's frozen Task target before any durable write or replay
acknowledgement, while the separately authenticated Worker origin must still
match durable reservation and SessionBinding evidence. Shared physical Worker
identity therefore cannot confer cross-logical-Node authority. Bindings reject
blank or duplicate credentials, credentials colliding with a Node command-endpoint
bearer, duplicate origins, malformed identifiers, and origins belonging to
another physical Node. The Relay process mounts the route whenever it starts;
there is no other Router listener. Node lifecycle events
otherwise require an authenticated Worker origin supplied out of band.
Envelope IDs alone cannot establish authority, and an event cannot bootstrap
execution identity. Missing/wrong origin, a late conflicting receipt, or
lineage drift records only bounded IDs, digests, safe conflict codes, and time;
it never records credentials, endpoint URLs, paths, native bindings,
instructions, remote text, or diagnostics. The [configuration contract](NODE-ROUTING-CONFIGURATION.md) requires distinct event
credentials and validates complete logical-Node bindings before startup.

The Node-side command service reuses the existing Node bearer-
authenticated command listener and is selected only by an exact top-level
`1.0.0` contract version after transport authentication and strict JSON
parsing. It authenticates the request's Node reference and the reservation's
physical Node ID before Worker I/O. Configuration rejects any logical
`workerKey` equal to the Node bearer or any Worker command/event
bearer, while not requiring the bearer credentials themselves to be mutually
distinct. Diagnostics remain field-name-only and never echo the colliding
value. Durable intent stores only the instruction digest; receipts exclude
bearers, endpoint URLs, raw failure bodies, unvalidated Worker output, and
native session handles. Only allowlisted definite Worker failures are retained
as rejection; every other Worker uncertainty is the fixed `ambiguous` state.
The `axis-node` runtime always injects this service; it is the only Node command
service.

`GET /v1/worker/readiness` uses the same Worker bearer credential and rejects query strings. It returns no profile path, symlink target, TOML content, native app-server response, model routing evidence, credential, or diagnostic. Its only configuration detail is a closed role availability/attempt projection plus stable degraded reason codes. Profile-source validation happens before the child receives a canonical TOML path; a source that is missing, unreadable, non-regular, or invalid is unavailable rather than passed through.

Structured elicitation permits one bounded non-secret question, two or three bounded explicit options, and an optional separately identified Other choice. Credentials, absolute POSIX/Windows/UNC paths including assignment and bare/protocol-relative `//` forms, file URIs, URL/DSN userinfo, credential query/fragment values, controls, bidirectional controls, and normalized duplicate labels are rejected rather than redacted; a valid explicit URL scheme remains display-safe. Axis defines native `options[0]` as the supported subset's canonical recommendation and requires its exact `optionId`; it never guesses from display text. Each unique `replyValue` is deterministically recomputed from only `decisionId + optionId`, so the same or reordered label in another decision cannot authorize it. Native thread, turn, item, and JSON-RPC identities are never projected; only deterministic hashes cross the Worker boundary. The authenticated active human who caused the Run is frozen with canonical Linear `User.url`. Waiting creates one elicitation Activity with that URL as plain text for Linear's native mention conversion and makes no direct Comment API call; outbound `bodyData` is forbidden. Exact owner, Session/root thread, Activity/Comment provenance, body, author, Team, decision claim, and reply value are required. Wrong-owner, stale, duplicate, non-exact, ambiguous, or restart-derived decisions fail closed. Explicit stop closes the pending decision with immutable audit evidence and cannot create continuation or cancel an already-terminal waiting Run.

Callers cannot provide Codex thread/turn IDs, Claude session IDs, workspace paths, executor selection, or provider output. A new Task may carry only the opaque workspace reference selected by its trusted Team Binding; Router freezes it and Worker resolves it solely through deployment-owned local configuration. Linear payloads, prompts, provider output, and executor bindings cannot select or override a path. Worker canonicalizes and checks the mapped directory before executor side effects, falls back only to the configured organization root, and otherwise returns the fixed path-free `axis.worker.workspace_unavailable` error. Instructions remain ephemeral. Responses, Relay projections, externally readable journals, logs, errors, and workspace audits exclude instructions, native IDs/output, paths, tokens, and transport diagnostics.

The Worker acts only on an exact validated native binding. Interrupt acknowledgement is nonterminal. Active native Runs cannot recover after Worker restart. A successor may continue a terminal or waiting predecessor only through the adapter-specific attestation described in [Persistence](PERSISTENCE.md); missing, mismatched, or ambiguous proof fails closed.

## Team-binding authority

The in-process team-binding store accepts only closed, bounded opaque identifiers and versioned references. It accepts no endpoint URL, filesystem path, credential value, arbitrary provider option, alias, diagnostic, or extension field. The required `TeamBindingMutationAudit` contains only opaque operator/source/request/authorization references. `ExecutionProfile` JSON, canonical digest, audit, and revision history are retained for integrity, but secrets and runtime values are not accepted for persistence. `listCurrent` accepts only a closed organization-scoped cursor/limit input, performs no validation/provider/audit callback, and returns only already-persisted current revisions; it adds no HTTP/OpenAPI, schema/migration, credential, or runtime integration surface.

Validation is synchronous and side-effect-free over the exact persisted profile. It may report only `reference_missing`, `stale`, or `incompatible`; unconfigured and disabled scopes never invoke it. Corrupt JSON, digest, audit, or current-pointer state fails closed as `persisted_state_conflict`, with no fallback binding, mutation, recovery, or secret-bearing diagnostic.

## Target and review authority

Trusted Team binding or Node Worker selection freezes each Task's target at
creation. No HTTP, webhook, prompt, ordinary caller field, provider output, or
native session can choose an executor, endpoint, permission, or fallback.
Review scheduling belongs to the caller's orchestration and reaches Axis as an
ordinary start; one Run's success does not grant another Run a workspace.

Frozen-target output may expose only opaque node/worker/executor/runtime/endpoint references, binding revision/digest, lineage IDs, timestamps, and safe outcome references. It never exposes endpoint URLs, bearer credentials, workspace paths, prompts, transcripts, provider output, native session identity, audit authorization/request references, or raw diagnostics.

## Operator CLI projection

The `axis` CLI accepts an explicit local database path only to construct the public Team Binding store and, for a complete topology Run selector, the public Router Run-target evidence seam; the path is never emitted. It never issues SQL, reads Router private fields, imports Registry/TeamRegistration facts, or derives a target from environment, prompt content, provider output, or discovery. Mutations require distinct bounded opaque authority, operator, audit-source, and request references. The CLI records them through immutable audit but projects only operator and source; missing authority fails as `unauthorized`.

Every success and error path uses the closed `axis.cli.v1` projection. It omits endpoint, credential, request, authority, database-path, token, native-session, workspace-path, raw-error, and provider-output fields. The configured and frozen workspace policy is visible only as opaque `{ ref, version }` or `null`. Current readiness is explicitly unavailable until a supported source exists. Historical Run evidence is either `not_requested` or the nested `axis.cli.run.v1` Node Run DTO. The Run DTO exposes only opaque Node/selection and recorded Worker/executor IDs; its actual IDs remain explicit `null` before receipt evidence and it never derives them from inventory or current bindings. Workspace-audit output is likewise path-free. There is no debug mode or output switch that weakens this redaction.

`team-bindings` uses compare-and-swap mutations and an immutable audit. Topology Run evidence reports the target persisted with the Task.

## Claude Worker execution

Claude's own local session files are retained to support exact-ID continuation; they may contain prompts and native output and remain under the runtime account's Claude storage. They are not copied into Axis journals, public events or diagnostics. This uses the existing runtime account and its native environment after inherited deployment and Linear values and the registry authentication token are removed; other account credentials remain available to native tools. Continuation is restricted to this adapter's observed completed sessions in the same working directory -- observed by this process, or, after a Worker restart, recorded in the Worker-private mode-`0600` `<dataDir>/worker-<id>-native-sessions.json`, which holds only session IDs and working-directory paths; arbitrary native IDs, concurrent turns, fallback starts and recovery of a turn still active at the restart are not admitted.

The separate Claude Worker canonicalizes its TOML `nodes.workers[].claudeExecutable`, verifies required help capabilities and authentication before listening, repeats the complete help/authentication gate before every distinct start or continuation, and never searches `PATH` or invokes a shell. A start validates one canonical lowercase UUID before its single `-p` spawn, writes the instruction only to stdin, passes no `--tools` or `--safe-mode`, forwards the native service environment after deployment/Linear credential scrubbing, and uses `--permission-mode bypassPermissions`. CLAUDE.md, skills, plugins, hooks, MCP servers, Bash, network access, file writes, and the account's credentials are therefore available exactly as they are to an interactive session. Axis adds no approval prompt, filesystem sandbox or network policy. The environment scrub does not isolate credentials stored in the account's files. Identity mismatch, malformed/truncated/over-limit JSONL, or post-spawn uncertainty is observational and is never retried or relaunched.

### What the Claude tool grant is and is not contained by

Axis grants the native Claude agent its full default toolset, native customizations, native service environment, and `--permission-mode bypassPermissions`. There is intentionally no Axis containment boundary around the child.

What the native CLI documents and Axis does not control: `bypassPermissions` runs tool calls without prompting but provider-owned rules can still deny explicit `ask` settings, `AskUserQuestion`, critical-path removal, root/sudo use, managed settings, or reads blocked by native settings. Axis deliberately does not pass `--safe-mode`, so workspace and account customizations run. The native documentation recommends an isolated environment for this mode; this path intentionally provides none.

Axis requires `--permission-mode bypassPermissions` on every start and continuation and verifies that the CLI advertises it before launch. Child construction removes inherited deployment and Linear values and the registry authentication token, then injects the Run's Linear execution variables. Those select the AgentSession identity; other native account settings and credential stores remain reachable. Native denials, malformed streams, identity mismatch, and process failures remain failed or observational Runs rather than being reported as success.

The Worker account is the security boundary. Axis provides no container,
namespace, uid change, filesystem jail, or network policy. Forwarded `HOME`
leaves every file readable by that account available to native tools, including
personal credential stores. Per-Run Linear variables select the intended App
User but do not isolate other credentials. An installed standalone
[CLI launcher](LINEAR-CLI.md) overrides those variables with its own grant,
even when the Run has no execution access; Axis does not detect that substitution.

Native transcripts may retain credential-bearing command output. Permission
denial detection is best-effort: absent both `permission_denied` and
`permission_denials`, a denial may still project as success. Native restrictions,
stream behavior, and reap timing require verification against the installed
CLI; fake-runtime tests alone do not establish them. A refusal racing an
instruction write can remain `executor.launch_ambiguous`. The 60-second exit
grace and five-second kill grace are fixed bounds, not guarantees that every
native session settles within them. Use a dedicated non-root Worker account
whose filesystem, credentials, and network access match the intended scope.

## Public ingress

Linear ingress is untrusted until exact raw bytes pass HMAC-SHA256 and timestamp validation. Parsed fields are shape-checked before persistence or dispatch. Relay resolves every created instruction from an authenticated exact Issue read that binds Issue and Team IDs to the webhook; comment-origin starts additionally require the verified exact Comment instruction and provenance. Webhook `promptContext`, history, and timeline fields are presentation-only and never supply or veto created input. Relay retries exact created-input reads at most three times with bounded backoff; exhaustion retains the delivery lease for a later retry and never dispatches. Every non-stop prompted instruction is read only through one authenticated exact Activity-ID lookup. The returned Activity/session/user/type checks must all pass before input or command persistence; webhook prompt, context, history, and timeline values cannot supply or veto a continuation input. Validated signed-event guidance supplies a separate instruction section after these authorization gates, never the user command. Guidance must be the official rule array (at most 64 rules and 64 KiB of UTF-8 JSON); malformed guidance is rejected before persistence. Rule order is preserved, and reserved dispatch delimiters are neutralized in rule bodies and user input. Guidance is encrypted with the composed instruction and is not stored as conversation configuration. Missing capability/token, provider failure, malformed or mismatched identity, Axis/null user, and wrong type fail closed without a Router call and project only the fixed safe error Activity. Authenticated unsupported preview actions are quarantined rather than guessed.

For new created-session admission, Relay persists only an encrypted instruction plus its digest on acceptance. A definite non-comment validation rejection persists no instruction row and no command; its immutable ledger contains only trusted identities, a startup-intent digest, closed `Validation` stage/reason, safe Activity reference, and timestamp. Later Router, Worker, or Executor outcomes cannot rewrite that intake evidence. Fixed failure rendering exposes only `Validation`, `Router`, `Worker`, or `Executor`, an allowlisted reason sentence, and an Axis-owned next action. `outcome_unknown` and `outcome_pending` are displayable only for ambiguous post-boundary uncertainty, differ only in whether that uncertainty is Relay's final answer, and both always state that Axis will not retry automatically. Neither discloses anything the other does not: same closed stage set, same fixed sentences, no provider diagnostics, no instruction text.

Comment prompts do not create a second ingress. A comment-origin AgentSession event must carry source-Comment provenance, and Relay exact-rereads the Comment, prompt Activity, and current Team membership after installation/Team authorization. The exact Comment root must match non-null AgentSession root provenance. For created events, `sourceCommentId ?? commentId` selects the exact comment candidate. When a created event lacks `sourceCommentId`, only a matching exact Comment marked by Linear as an artificial AgentSession root may take the direct-created path after its ID, root, Issue, and Team bindings pass; it cannot supply a user instruction or provenance. The authenticated AgentSession creation supplies delegation authority, so a verified creator instruction need not repeat an @mention. Raw Comment ingress is still unsupported; creator, root, issue, Team and membership checks are mandatory for human Comments. A direct created event carrying an Activity must exact-reread and verify that typed prompt from the session creator rather than discard it. The bounded ProseMirror walk caps raw bytes, depth, nodes, and mention-excluded instruction bytes. Prompted execution instead dispatches only the exact verified typed Activity body after binding its source Comment, Session, Activity, and human identities; it does not require equality with rendered `Comment.body`. If that verified prompt establishes the local Session before the matching created delivery arrives, Relay reconciles the created event only through verified Comment provenance, a prompted action-specific command key, the retained ordinal-1 comment invocation, start command, acknowledgement, and exact Session/root/source-Comment identity; even a historical created event carrying an Activity cannot satisfy that proof. Reconciliation adds no new executable work and rejects missing provenance or drift. Self/App/bot/nonmember, identity drift, and unsupported shapes cannot reach Router or Worker. Internal rejection codes are stable `linear.comment.*` values and never include comment text or provider diagnostics.

A conversation's Team authorization is fixed when its first `start` resolves the Worker selection. Later `continue` commands and retries of a refused start re-check only that the webhook's `issueTeamId` is still authorized (`authorizeLinearEvent`); they do not re-read the Issue with the access token to confirm it still belongs to that Team. A current source-Team authorization check therefore does not prove the Issue still belongs to that Team.

OAuth uses `actor=app`, one-time state, S256 PKCE, an exact redirect URI, and least-privilege scopes. Team access is checked before new work; permission removal and OAuth revocation fail closed. Relay sets Axis as `delegate` and does not mutate issue assignment.

## Secrets and content

- OAuth access/refresh tokens live only in the encrypted vault referenced by SQLite.
- Raw webhooks, prompt context, user input, and PKCE verifiers use AES-256-GCM envelopes.
- Relay/Router/Worker bearer tokens, OAuth secrets, webhook secrets, and Claude authentication state are deployment inputs and are never persisted or logged by Axis.
- Safe errors and audits contain only bounded codes, IDs, counts, digests, and summaries; they never contain prompt plaintext or structured guidance.
- HTTP errors never echo provider responses, request bodies, GraphQL messages, or stack traces.

The vault is written atomically with mode `0600` inside a `0700` directory. Back up database and vault together; store the master key separately.

## Replay and ambiguity

Transport delivery ID, semantic delivery digest, logical AgentSession-event identity, Relay command ID, Router command ID, Worker command ID, Run ID, event message ID, and Linear Activity UUID are separate controls. Reuse with changed immutable identity fails closed.

Provider transport loss does not prove absence. Ambiguous Linear Activity delivery is never blindly recreated, and ambiguous native execution is never reattached or relaunched. Operator evidence must remain secret-free.

The private Router accepts a Worker failure as definite only when the HTTP status, safe code, and fixed message exactly match the built-in allowlist. Unknown codes, changed messages, malformed or extended error envelopes, and transport loss collapse to `axis.worker.transport_unknown` and ambiguous `outcome_unknown`; response text is discarded. This classification changes no Worker request/response schema and cannot turn uncertainty into permission to dispatch again.

## Logging and retention

Do not enable HTTP body logging. Retention/deletion of encrypted user content is an operator policy; preserve only the minimum non-secret digests and audit state required for replay protection.


Other free-form answers use the same exact owner/source authorization and atomic decision claim as fixed replies. A token alone is not an Other answer; the normalized answer is nonempty and at most 4096 Unicode scalar values. The fixed audit label is `Other`; answer text stays in encrypted session inputs and is never copied into waiting display fields, option labels, or decision-resolution rows. Explicit stop retains the stale-reply guard. Runtime Tasks admit the Other capability.

## Execution access to Linear installations

The optional Relay-private [execution access contract](LINEAR-EXECUTION-ACCESS.md)
binds a short-lived opaque bearer to an independently approved active execution
and one installation revision. It rechecks authority and OAuth viewer identity
before trusted token handoff. Scope is installation-visible resources in one
workspace; the source Team is an admission check, with no execution-specific
Team subset or current-issue isolation. OAuth tokens remain inside Relay. The protected exchange delivers the capability to a configured
Worker receiver; Runtime and Agent Activity envelopes contain no credential.
