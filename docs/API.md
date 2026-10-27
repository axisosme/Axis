# Axis Relay and Router API

## Configuration initialization and library API

`axis init` and the package root configuration API provide the versioned TOML
model, role projections, derived data paths and private key provisioning. See
[the configuration reference](CONFIGURATION.md) for the complete example and
exported functions. Built-in Relay, Node and Worker binaries load TOML with
`--config` (default `axis.toml` in the current directory). Node also accepts
`--node`; Workers accept `--node` and `--worker`. Selectors override `[host]`
defaults, with sole-candidate selection when unambiguous. Their role projections
derive storage, identities and key files. HTTP request, response and
authentication shapes are unchanged.

## Agent instructions and Linear guidance

The selected Worker's `nodes.workers[].instructions` TOML table accepts a
nonblank `sessionResponse` string up to 64 KiB or `omitSessionResponse = true`;
they cannot be set together. Omission retains the default; unknown keys fail
parsing before startup effects. Axis supplies no writing-style preference.
The Linear access paragraph is derived from the actual child environment and
remains outside this configurable table.

Signed Linear AgentSession events accept optional `guidance` as the bounded official rule array (64 rules, 64 KiB total UTF-8 JSON). Invalid shapes use the existing `linear.ingress.malformed` response. Valid rules appear as a separate `<linear-agent-guidance>` section in the existing instruction string passed through Router, Node and Worker to Codex and Claude Code. Rule order is the provider's precedence order. Guidance is scoped to the event's dispatch, without conversation configuration or injection from `promptContext`; later events without guidance add no section. See [provider shapes and continuation semantics](LINEAR-PROVIDER.md#pinned-shapes) and [executor instructions](EXECUTOR-ADAPTER.md#agent-instruction-set).

Guidance travels in the existing instruction string. Router checks the full serialized pending command (64,334 bytes) and outer request (128 KiB) before admission; oversize returns `axis.router.malformed` without persisting a command or dispatching to Node. The byte budget includes guidance, escaping, and envelope context.

## Optional Linear GraphQL proxy

`POST /api/linear/graphql` accepts an execution credential or an independent
[Axis CLI credential](LINEAR-CLI.md), and a standard
`{query, variables?, operationName?}` JSON envelope. It selects the bound Axis
installation, preserves GraphQL data/errors/pagination and rate-limit information,
and never automatically replays a write. An uncertain write returns
`linear.proxy.outcome_unknown` with a request ID. Enablement is explicit; execution access connects explicit workspace grants, live Router execution authority and
separately authenticated Worker receivers through
`POST /api/linear/execution-credentials`. The protected transport is owned by
`@axisosme/axis-contracts/linear-execution`; it does not extend existing command/event envelopes.
See the [proxy protocol](LINEAR-API-PROXY.md) and
[CLI/SDK setup and support limits](LINEAR-EXECUTION-CLIENTS.md).

Axis exposes the Linear Relay, Router, Node, and Worker APIs. Codex and Claude Code share organization-based workspace selection; provider selection is deployment-owned and never a request field. Native Agent versions are not runtime admission inputs.

## Execution boundaries

Axis execution uses `Router`, Node (routing), and Worker. Router reaches a Worker only through a Node. Router maps Linear `{ organizationId, issueId, agentSessionId }` to one Task and immutable initial/successor Runs. Native Codex thread/turn IDs, prompts, and workspace paths are never caller-selected routing fields. Only an exact-bound, sanitized terminal `agentMessage` may become a Relay-safe response projection.

The Linear channel dispatches commands into Router through the channel-neutral `ChannelCommandPort` (`src/router/channel-command.ts`) and `nodeChannelCommandPort` (`src/relay/node-routing-composition.ts`). Conversation, run-chain, and delivery-outbox abstractions remain channel-neutral and serve Linear. The Router-to-Relay event projection is the in-process neutral schema versioned `1.0.0` (`ROUTER_RELAY_NEUTRAL_EVENT_VERSION`, `src/shared/router-relay-event.ts`); it does not cross HTTP.

Each protocol accepts its exact current identifier. Shape names distinguish
independent protocols and projections:

| Surface | Current identifier |
| --- | --- |
| Router, Node command/receipt, Worker command, Executor lifecycle, Worker waiting extension, Router-to-Relay envelope, routing preflight | `1.0.0` |
| Frozen Node Task target | `1.0.0-node` |
| Run target evidence | `1.0.0` |
| Fixed-choice elicitation / elicitation with Other | `1.0.0-fixed-choice` / `1.0.0-other` |
| Router-to-Relay event types | `axis.router.v1.*` |
| CLI envelope / nested Run projection | `axis.cli.v1` / `axis.cli.run.v1` |
| Local host state / public lifecycle receipt | `axis.host.v1` / `axis.host.receipt.v1` |

Node command-intent, command append-chain, route append-chain, route record,
and command lifecycle hash domains use `.v1`; the frozen binding domain is
`axis.router.node-target.v1`. Public host service references use
`axis.host.receipt.v1.service-ref`. Package-owned Runtime and AgentActivity
schema identifiers are defined by the contracts package.

`WorkerSelectionScope` is closed to `{ system: "linear", tenantId, scopeId }`,
where `tenantId` is the Linear organization and `scopeId` is the Team.
Worker workspace configuration retains `{ organizationId, workspaceRoot,
workspaceCatalog? }` without a `system` discriminator; see
[Multi-host deployment](MULTI-HOST-DEPLOYMENT.md#production-topology).

### Created-session startup outcomes

For a newly accepted `AgentSessionEvent.created`, Relay atomically persists the AgentSession, encrypted instruction, pending start command, fixed acknowledgement Activity, and immutable startup-ledger linkage in its own SQLite transaction. A definite non-comment validation rejection persists only a minimal open AgentSession, one fixed failure Activity, and the immutable rejected outcome; it creates no instruction row or command, and a later prompt may retry. The ledger is keyed by the stable logical created-event identity, and exact semantic replay adds nothing. A changed intent or a retained accepted/rejected disposition mismatch fails as `linear.ingress.logical_event_conflict`. Startup records are created only by the accepted or rejected ingress transaction; they never authorize replay of an AgentSession.

The acknowledgement body is exactly `Axis received this request and is preparing the captured execution route.` Definite startup failures use fixed four-line Axis copy beginning `Axis could not start this session.`; ambiguous post-boundary outcomes use one of two four-line bodies and both end `Axis will not retry automatically.` An outcome Relay has decided against begins `Axis could not confirm whether this session started.` (reason `outcome_unknown`). One still owed an answer -- the reservation left held for the sweeper, or already settled by another pass -- begins `Axis has not yet confirmed whether this session started.` (reason `outcome_pending`). Which of the two is sent is decided by what is still owed an answer, not by the safe code. Only closed `Validation`, `Router`, `Worker`, and `Executor` stages and their allowlisted reasons may be rendered. Provider/native text, unknown error codes, response bodies, instructions, paths, and credentials are never copied into the Activity.

A created start refused while its input is verified renders, and persists as `failure_reason`, one of `linear_capability_unavailable`, `credential_unavailable`, `user_unavailable`, `owner_unverified`, `issue_unavailable`, `issue_mismatch`, `activity_mismatch`, `activity_session_mismatch`, or `activity_not_prompt`, according to its verified refusal cause. `owner_unverified` is deliberately one reason for every owner-verification failure. A definite failure to synchronize the issue before dispatch renders `issue_not_found`, `issue_unavailable`, `issue_context_malformed`, `issue_team_changed`, or `issue_update_rejected`; that startup was already admitted, so its reason is recorded as the rejected start reservation's safe code (`axis.relay.issue_sync_not_found`, `axis.relay.issue_sync_read_rejected`, `axis.relay.issue_sync_malformed`, `axis.relay.issue_sync_team_changed`, `axis.relay.issue_sync_update_rejected`). `axis.relay.issue_sync_failed` and its copy remain for a failure none of those names. A transient synchronization failure (timeout, network loss, Linear 5xx, rate limit) produces no startup copy: the delivery is retried within its retry budget and, if that is spent, answered once with the give-up Activity. Stored `failure_reason` values are not rendered back as arbitrary text.

`POST /v1/worker/commands` returns the closed Worker `1.0.0` success union or generic error envelope. A definite startup refusal requires an exact allowlisted HTTP status, safe code, and fixed message tuple; unknown or malformed responses are ambiguous. Router sees Worker outcomes through Node receipts, never through a direct Worker route.

### Linear issue-comment prompts

Comment-origin execution uses the existing AgentSession ingress. Signed `AgentSessionEvent.created|prompted` remains the sole execution ingress and raw `Comment` webhook payloads remain unsupported. Provider provenance remains nullable for non-comment AgentSessions, but any comment-origin event requires a non-null captured `agentSession.commentId` before provider access. Exact Issue, Team, `parentId ?? id == agentSession.commentId` thread root, human author, and current Team membership gate the existing AgentSession command path. Comment-origin created events require well-formed bounded `bodyData`; authenticated session creation supplies delegation authority for the verified creator instruction. For prompted work, the exact authenticated typed AgentActivity remains execution authority; its body need not equal rendered `Comment.body`.

Relay permits the first provenance-verified comment-origin `prompted` event in a captured thread to create the local AgentSession only when it computes to the first `start` command. The existing transaction records one Session, ordinal-1 encrypted input/start command, accepted comment invocation, and canonical acknowledgement; the command's action-specific Comment key binds its prompted Activity identity and cannot be confused with a historical comment-created start. Exact logical replay remains inert, while a later distinct verified child reply produces ordinal-2 `continue`. The prompted delivery itself creates no `session_startup_ledger` row. If the matching comment-origin `created` event arrives later, Relay verifies the captured Session and Comment identities plus the retained ordinal-1 input, accepted invocation, prompted command key, start command, and acknowledgement, then atomically records the created-event startup ledger against those existing artifacts and returns it as a duplicate. It never adds another input, command, acknowledgement, or dispatch; missing verified Comment provenance or mismatched delayed-created state remains `linear.ingress.logical_event_conflict`. Direct prompted-before-session and first-event stop remain rejected.

The package-root `LinearProviderPort` exact-read surface also resolves the authenticated active human and canonical `User.url` that caused each Run. Input and decision rows freeze that owner and retain immutable explicit-stop closure evidence. Comment `bodyData` remains inbound authority evidence only and is never sent by Axis.

An accepted comment-origin created event links its existing `comment_invocations` record, encrypted input, start command, canonical acknowledgement, and startup ledger in one transaction. A rejected comment eligibility decision remains owned by `comment_invocations`, retains the open minimal Session behavior, and does not create a startup-ledger rejection; this preserves the established ability for a later distinct valid prompt to create the first start command.

### In-process team bindings

`SqliteTeamBindingStore` is an additive package-root TypeScript API, not an HTTP route or Router dispatch path. `TeamBindingVersionedRef`, `ExecutionProfile`, and `TeamBindingMutationAudit` resolve the composite `{ organizationId, linearTeamId }` scope to one closed profile at contract version `1.0.0`: `nodeId`, `workerId`, `executorKind`, `runtimeProfile { ref, version }`, plus only optional versioned `endpointRef`, `credentialRef`, and `workspacePolicyRef`. Every field is a bounded opaque identifier; URLs, paths, secrets, free-form values, and extensions are rejected. A `TeamBindingRevision` exposes `status` and `createdAt` (never `state` or `recordedAt`). The synchronous `listCurrent({ organizationId, afterLinearTeamId?, limit? })` lists only current bindings for one organization in binary `linearTeamId` order; it is read-only, uses exclusive pagination, defaults to `100`, and accepts a safe integer limit from `1` through `100`.

`configure` requires the exact current revision (`0` only for initial configuration) and an opaque audit `{ operatorRef, source, requestRef, authorizationRef }`; `disable` requires an existing configured revision. `current` and ascending bounded `history` (with exclusive `afterRevision`) return fresh deeply frozen copies. `validate` and `resolve` throw stable `unconfigured`, `disabled`, `reference_missing`, `stale`, or `incompatible` errors when they cannot return a successful exact profile result; a disabled revision has no profile and never falls back or invokes the supplied synchronous validator. Successful configuration resolves an exact canonical-JSON SHA-256 profile digest. There is no provider lookup, Registry import, Worker selection, or execution side effect. Router composition may separately supply one explicit operator fallback endpoint for an otherwise-unconfigured Team; it does not change this store's exact `resolve` behavior.

### Router Run target evidence

`Router.readRunTarget({ organizationId, issueId, teamId, agentSessionId, runId })` is a synchronous package-root instance seam for one already-persisted Run. Its input is closed to those exact five nonblank fields. Router first proves the trusted Task scope, then proves that the supplied Run belongs to that Task. The evidence is exactly `{ contractVersion: "1.0.0", runId, target, execution }`; the target uses `1.0.0-node`, and `execution` is either pending with both IDs null or recorded with both authenticated IDs present. Snapshots are never re-resolved. Unknown Task/Run and cross-Task Run use `axis.router.not_found`; a captured-Team mismatch uses `axis.router.authority_mismatch`; malformed or extended input uses `axis.router.malformed`; malformed non-null persisted snapshots use `axis.router.recovery_required`.

This historical read never resolves or validates current Team Binding state, selects an endpoint, calls a Worker/provider, dispatches a command, opens a transaction, or writes durable state. It exposes only the frozen Node routing facts and recorded opaque Worker/executor IDs. It omits endpoint URLs/tokens, Worker objects/commands, instructions, native/provider bindings, database identifiers, and topology formatting. It is not itself an HTTP, Runtime v1, OpenAPI, CLI, or Relay API.

### Package-root Router Node lifecycle

`Router.startNode`, `continueNode`, `cancelNode`, and `ingestNodeRunEvent` are
public instance methods on the package-root `Router` class. They are Router's
only command lifecycle and require an injected package-internal Node command port;
the Relay composition injects its HTTP adapter, `HttpRouterNodePort`. That
port and adapter are not root exports or Relay/CLI commands. The
`RouterRunTargetEvidence` root union includes Node evidence at `1.0.0`. Start
requires an explicit Worker selection. Router commits pending identity and a
redacted canonical intent before one Node dispatch. After that cutline, an
exact caller replay can only query the exact receipt; intermediate, not-found,
malformed, unauthenticated, or transport-ambiguous evidence remains
recovery-required and never authorizes a second start. Competing intermediate
receipts retain the first authenticated reservation transactionally; a later
different reservation or not-found observation cannot replace or erase it.

Accepted receipt evidence must match the authenticated Node, frozen selection,
complete Task/Attempt/Run/source/operation lineage, reservation, SessionBinding,
and nested Worker result. Continue and cancel retain the predecessor route and
never re-resolve current binding or inventory. An early lifecycle event is
buffered only with authenticated origin evidence, then enters the Router
receipt/cursor projection after accepted evidence proves the same
Worker/executor pair. Missing or conflicting origin is quarantined. A definite
command rejection atomically quarantines any buffered events, and competing
accepted/rejected receipts use a first-terminal-writer cutline: the loser is
recorded as a conflict and cannot mutate execution or core Run state. Exact
replay of an authenticated event first received after definite rejection is
the same durable `axis.node.binding_conflict` quarantine and creates no second
conflict. Projected replay revalidates source, receipt, cursor, time, sequence,
and relay projection integrity.

Router's listener serves `POST /v1/router/node-run-events` as its only route
and has no bearer of its own. Each credential is bound in memory to one exact
Node reference/physical ID and a closed allowlist of Worker origins; a closed
`1.0.0` request must match both before Router receives an authenticated origin.
Router ingestion carries that authenticated `{nodeRef,nodeId}` alongside the
Worker origin and rechecks the logical reference against the Task's frozen Node
target before any new durable event write or replay acknowledgement. Its small
`buffered | projected` acknowledgement is `202` for the first durable
observation and `200` for exact replay; a durable quarantine is `409`. The
package root does not export this listener. The Node's package-private
`RouterEventDelivery` derives each request's Node reference and Worker origin
from the accepted durable Node route, sends it with
`node-<id>-router-events.key`, and accepts only the closed correlated success
receipt. See the [Node routing configuration](NODE-ROUTING-CONFIGURATION.md).

The route always returns JSON with `cache-control: no-store`.
Success has the closed shape
`{contractVersion,nodeRef,sourceMessageId,runId,status,replayed}`. Failure has
the closed shape `{error:{code,message}}`; it never includes credentials,
bindings, Relay projections, instructions, endpoints, or diagnostic detail.

| HTTP | Error code | Meaning and durability |
| --- | --- | --- |
| `200` | — | Exact replay of an already durable `buffered` or `projected` event; no duplicate row, conflict, or cursor. |
| `202` | — | First durable `buffered` or `projected` observation. The acknowledgement is sent only after the Router transaction commits. |
| `400` | `axis.router.malformed` | Query, wrapper, strict UTF-8 JSON, duplicate-key, or timestamp failure before lifecycle ingestion; no event/conflict/receipt write. |
| `401` | `axis.router.unauthorized` | Missing, duplicate, malformed, or unmatched bearer; checked before query, media, or body parsing; no ingestion. |
| `404` | `axis.router.malformed` | Exact path is not mounted or does not match; no ingestion. |
| `405` | `axis.router.malformed` | Non-`POST` request; `Allow: POST`; no ingestion. |
| `409` | `axis.router.conflict` | Either pre-ingestion Node/body/origin authority mismatch or unknown Run (no new durable effect), or a durably quarantined event (one event row plus one bounded conflict on first observation). The wire response intentionally does not reveal which case occurred. |
| `409` | `axis.router.intent_conflict` | Message ID already belongs to different event/origin evidence; existing state is retained and no new event/conflict/receipt is written. |
| `413` | `axis.router.body_too_large` | Outer body exceeds 128 KiB; no ingestion. |
| `415` | `axis.router.unsupported_media` | Media type is not exact JSON/UTF-8 or content encoding is present; no ingestion. |
| `502` | `axis.router.protocol_violation` | Lifecycle envelope fails the existing closed event validator; no ingestion. |
| `503` | `axis.router.recovery_required` or `axis.router.worker_rejected` | Router cannot safely classify/acknowledge the event. Any attempted transaction is rolled back; previously retained evidence is unchanged. |

Because both authority rejection and durable quarantine deliberately collapse to
the same `409 axis.router.conflict` body, callers may retry the exact request but
must not infer durability from a `409`. Only `200`/`202` transfers or confirms
durable reconciliation ownership.

### Node command execution

`NodeCommandService` is the package-private implementation of the command and
receipt routes, which serve only the command contract. A top-level
command or receipt query with an unsupported `contractVersion` is refused
`400 axis.node.malformed` before any service runs. A command that has been
durably admitted returns its retained receipt at `202`, including exact replay
and terminal `accepted`, `rejected`, or `ambiguous` receipts.
`POST /v1/node/command-receipts` returns the retained receipt or `not_found` at
`200`. Every refusal the command listener sends -- validation, binding,
recovery, storage, authentication `401`, unknown route `404`, query or
undecodable body `400`, media `415`, outer size `413`, method `405`, and the
closing listener `503` -- uses the fixed Node error statuses and the command error
envelope `{ contractVersion: "1.0.0", error: { code, safeMessage } }`. A wrong
method is `axis.node.malformed` at `405`. The Worker-facing event listener
(`/v1/router/run-events` on the Node's event port, and `/v1/node/maintenance`)
keeps its `1.0.0` error envelope.

`GET` and `POST /v1/node/maintenance` return exactly
`{contractVersion,nodeRef,mode,idle,blockerCounts}`; `axis upgrade` validates
that shape on both sides of an upgrade, so it does not grow.

The service validates and clones the candidate before effects, authenticates
the configured Node reference and physical Node ID, and lets the journal
resolve inventory only for a new first start. It persists `reserved` and then
an owner-bound `dispatching` receipt before the sole Worker `1.0.0` call.
Only contextually validated Worker evidence becomes `accepted`; a known
definite Worker rejection becomes `rejected`, while transport uncertainty,
malformed output, or identity mismatch becomes `ambiguous`. Continue,
cancel and steer use only the predecessor's retained reservation and never query current
inventory. The `axis-node` composition injects this service. No public export
names these types.

`steer` is `{ contractVersion: "1.0.0", commandId, command: "steer", target: { runId }, instruction }`.
It is shaped like `cancel` -- `target.runId` must be the predecessor route's
Run, it mints no Run and no route, and its receipt expectation carries only the
redacted intent's `instructionSha256` -- plus the non-empty instruction, held to
the same pending-command size bound as a continue. The Node completes the
Worker `1.0.0` steer `{ contractVersion, commandId, command: "steer", target, instruction }`
without `executionTarget`, as for cancel. An accepted receipt's evidence is the
predecessor's session binding and its `result.status` is the Worker's
`steered`, or `interrupt_requested` when the Worker fell back. A draining Node
forwards a steer, as it does a cancel. A fresh start or continue on a draining
Node is refused with the definite `503 axis.node.draining`, answered before the
Node records anything: Router answers a start `rejected` with
`axis.router.node_draining`, and forgets a refused continue so the
conversation's slot is free and the same prompt succeeds after the resume. A
replayed command whose row is still `reserved` or `dispatching` is refused with
the ambiguous `axis.node.recovery_required`. A steer takes no part in
`ambiguous` convergence (it has no Run of its own). OpenAPI documents it as
`NodeCommandSteer`.

With `router.nativeSteer = true`, a follow-up that reaches a Run Router
still holds `pending`/`running` -- a `continue` row whose Node command ID
differs from its command ID -- is sent as this `steer`, carrying the
instruction, instead of a `cancel`. The choice is made when the row is claimed
and retained in its redacted intent (`nodeCommand.command.command`, with the
instruction's `instructionSha256`), so a receipt re-read or an exact
re-presentation after the switch flips validates the command that was actually
sent. Like a cancel, a steer writes no execution evidence and no Run, and its
accepted receipt must carry the prior route's session binding.
`RouterCommandResult`, `reconcileCommand`, and the channel
`ChannelCommandOutcome` / `ChannelReconcileOutcome` include `steered`;
`reconcileSteeredCommand` (channel port `reconcileSteered`) returns the retained
Task/Run lineage so a channel sweep can settle a steered follow-up whose own
settle was lost. A Worker that cannot take the steer answers
`interrupt_requested`, which Router and the channels treat as an interrupt: the
instruction is queued and redispatched as a continue once the Run is terminal.

Channels map a `steered` reservation to `dispatched` with `status: "steered"`.
Linear posts one thought, `LINEAR_CHANNEL_COPY.steered`: "Axis passed this
instruction to the running agent; it takes effect at the agent's next step. Send
stop to interrupt instead." No redispatch is queued and nothing is
cancelled, so the session's final reply is the running Run's own terminal.

### Operator CLI

`axis` is the supported non-interactive operator entrypoint. It opens an explicitly named Router database through the public `SqliteTeamBindingStore` contract and, only for a complete topology Run selector, the public `Router.readRunTarget` seam; it does not access SQLite tables, derive a route from deployment configuration, or create another fact source. `axis team-bindings list/get/history/set/disable` and `axis topology list/tree` use immutable organization and Linear Team IDs. `list` and topology pagination call only `listCurrent`; current/history and mutations call their corresponding public methods.

All command output has the closed `axis.cli.v1` envelope when `--output json` is selected. It declares the Team Binding contract version and separates `configuredTarget`, `currentEvidence`, and `historicalRunBinding`. Every non-null configured profile includes `workspacePolicyRef` as `{ ref, version } | null`. Current evidence remains deterministically unavailable; topology selectors remain all-or-none. A selected Run places the closed `axis.cli.run.v1` DTO in `historicalRunBinding`: it distinguishes `nodeRef` and logical `workerSelection` from actual `workerId` and `executorId`. The selection has exactly one shape, `policy`, rendered as the source scope and executor labels. The actual IDs are both explicit `null` until Router records execution evidence.

`set` requires `--profile` and `--profile-revision` for the exact `runtimeProfile` reference, plus explicit `--node`, `--worker`, and `--executor`; no Profile Registry is consulted. Optional endpoint and workspace-policy references each require an all-or-none ref/revision flag pair. Each mutation also requires immutable CAS and explicit operator authority. It returns success only after the authoritative reread matches the complete requested profile, including the workspace reference.

The output projection never includes database paths, endpoint/credential references, audit request/authority references, tokens, provider-native session identity, raw rows, provider output, local workspace paths, or filesystem diagnostics. The workspace policy reference is intentionally visible operator metadata. `axis workspace-audit list --database <worker-db>` returns a bounded exclusive-sequence page of path-free `mapped`, `default_root`, `fallback_root`, or `unavailable` decisions and their stable reasons. `axis outbox-quarantine list --database <worker-db>` returns a bounded exclusive-sequence page of the Worker outbox rows this build gave up on, projected to sequence, run id, message id, disposition code and timestamp. The reader behind it queries the disposition sidecar alone and carries no payload, so this surface cannot print the executor's response body even by accident; the retained row stays readable through a separate journal reader so a caller can prove its payload was never rewritten -- for a row whose bytes this build can parse. Since a row whose `event_json` is not readable as an envelope at all is also given up on; that reader reports it rather than failing, but carries a placeholder instead of the bytes, so the retention proof for such a row is a direct read of `worker_outbox`, which quarantine never writes to. A Worker journal must have its complete current schema and one version `1` record in `worker_schema`; unsupported or partial storage is refused as `axis.worker.recovery_required`. `axis linear-inbox-give-ups list --database <linear-channel-db>` and `axis redispatch-give-ups list --database <run-chain-db>` return the same bounded exclusive-sequence page shape over what Axis stopped retrying : the inbox listing projects sequence, delivery id, attempts, failure code, give-up instant and the operator acknowledgement or null; the redispatch listing projects sequence, conversation id, command id, attempts, failure code and instant. Both open read-only and validate their store's complete current schema and single version `1` record before reading its append-only disposition table -- it never joins the delivery or the redispatch marker it describes, so a disposition survives the deletion of the row it is about, and it holds no payload column, so neither surface can return provider content. Both listings refuse unsupported or record-less storage. `axis linear-inbox-give-ups acknowledge --database <linear-channel-db> --delivery <delivery-id>` releases one inbox give-up's hold on host drain while retaining the delivery, its payload and its attempt count; it takes the four required operator-authority arguments `--operator`, `--authority`, `--audit-source`, and `--request`, refuses with `unauthorized` without `--authority`, persists an optional `--reason` (printable text, 1 to 512 characters; an empty or control-character reason is refused rather than truncated, because the column is append-only and an audit reason cut in half is worse than a command that says it was too long), and is idempotent -- a second call reports `already_applied_and_verified` and does not overwrite the recorded operator, authority or instant.

### Host lifecycle and routing preflight

`axis install --role controller|node|worker --config <path>` selects TOML
(default `./axis.toml`). Node accepts `--node <id>`; Worker accepts `--node <id>`
and `--worker <id>`. Controller rejects these selectors, and Node rejects
`--worker`. Worker runtime follows the selected declaration's `kind`.
`--host-root`, `--service-label`, `--source-package` and `--output human|json`
control local installation. Non-interactive use requires `--role`.

Host state is strict `axis.host.v1`, stored as `axis.host.v1.json`. It records
`configuration: { filename, digest, nodeId, workerId }` with a canonical absolute
TOML path, exact-byte SHA-256 digest and nullable pinned selections. The file is
owner-private, regular and accessed without symlinks; the managed runner checks
the pinned digest before invoking the selected package-local binary. Any TOML
change requires restoring the pinned bytes or stopped-service matched backup
and fresh host installation. Retain journals, identities, keys,
vault and native state through [the installation procedure](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade).

`axis upgrade` accepts `--host-root`, `--source-package`, `--output` and optional
`--force`; it reads role, configuration and selectors from managed state.
Node maintenance and ambiguous-commands continue to use `--host-root`.
Public lifecycle receipts remain `axis.host.receipt.v1`; host-state paths,
selectors, TOML and credentials are not added to public receipts.

`axis-routing-preflight <relay|node|worker>` accepts the same `--config` and role
selector flags as the service binaries, plus `--expected-version <version>`.
Relay and Worker optionally accept `--require-linear-execution true|false`;
Node rejects it. This flag takes a value. Preflight opens no listener and makes
no network request, checks stores on disposable SQLite copies, and returns
bounded counts and blocker codes. It reads role keys and Relay's external
Linear secrets; it does not prove provider authentication. See
[the preflight contract](NODE-ROUTING-CONFIGURATION.md#the-preflight-receipt).

### `POST /v1/worker/commands`

Private bearer-authenticated Worker command ingress. The closed command union is `start`, `continue`, `cancel`, or `steer`:

- start carries a Router-owned Run start envelope and ephemeral instruction;
- continue carries the successor start envelope, prior Run ID, and ephemeral instruction;
- cancel carries only the current Run ID;
- steer carries the current Run ID and an ephemeral instruction: `{ contractVersion, commandId, command: "steer", target: { runId }, instruction }`, exact keys.

`steer` hands the instruction to the Run's running turn without interrupting it. It acts on the same Runs as `cancel` and is refused with the same codes: a Run that is not `accepted`/`running`, or that this Worker process is not supervising, is `axis.worker.recovery_required` 503 -- with one exception: when the Run already ended `succeeded`, `failed`, or parked on `waiting_for_input` (only Codex parks), with a consistent binding (a cancel or steer that raced the Run's end), the Worker answers `interrupt_requested` for that Run, definitely and without calling the executor, so the caller continues it as an ordinary follow-up. For a parked Run that is safe because the run chain queues an interrupting continue's instruction behind `channelRedispatchEligible`, which a waiting conversation does not pass: the instruction waits through the question and runs once the conversation is next idle. When the executor registration declares the `steer` capability, the Worker answers `{ contractVersion, commandId, status: "steered", runId }` once the executor reports that the running turn took the instruction; the turn still ends with its own single terminal event. When the turn is already finishing (`executor.steer_too_late`), the Worker answers `interrupt_requested` for the same Run **without** interrupting anything: that turn is ending on its own, and the caller continues it once it is terminal, exactly as after a cancel. When the registration does not declare `steer` (Codex), the Worker takes `cancel`'s path exactly -- for a running Run the executor's cancel is called and the answer is `interrupt_requested`, and a registration without `cancel` stays `axis.worker.operation_unsupported` -- so those Workers behave byte-for-byte as for a `cancel`. Any other executor steer outcome is `axis.worker.launch_ambiguous` 503. A response validator must therefore accept `steered` or `interrupt_requested` for a `steer`, and `steered` for nothing else. The instruction is journaled only as a digest, like start/continue; same `commandId` and instruction replays `200` with the recorded response, and a different instruction under the same `commandId` is `axis.worker.intent_conflict` 409.

Node forwards `steer` through the Worker `1.0.0` command union. `WorkerCommand`,
`WorkerResponseExpectation`, and `WorkerCommandResponse` include `steer` and
`steered`; package-root `WorkerIngressCommand` and `WorkerIngressResponse` alias
those unions, and `WorkerSteeredResponse` aliases the `steered` member. Router
sends steer only with `router.nativeSteer = true`.

New durable commands return `202`; exact replay returns `200`. The route accepts POST only, no query or content encoding, strict UTF-8 JSON up to 65,536 bytes, and no duplicate keys. Safe failures are `400`, `401`, `403`, `405`, `409`, `413`, `415`, `502`, or `503`. The Worker never reconnects, reconstructs, or automatically relaunches an uncertain native turn.

The sole exact command version is `1.0.0` (`WORKER_COMMAND_VERSION`, exported from the package root). Start and continue require `workspacePolicyRef: { ref, version } | null` and `executionTarget: { identity: { nodeId, workerId, executorKind, runtimeProfile: { ref, version } }, sessionBindingExecutorId }`; cancel has neither. The Worker compares every `executionTarget` field with its own configured identity before it reserves or launches anything and refuses a difference with the definite `axis.worker.identity_mismatch` 409; Router settles that as `axis.router.worker_identity_changed` and the channel says which operator action fixes it. A session binding whose `workerId` or `executorId` disagrees with `executionTarget`, a missing target, and any other `contractVersion` are `axis.worker.malformed` 400. The Linear `run.waiting_for_input` event a Worker sends keeps its own version `1.0.0` (`WORKER_WAITING_EXTENSION_VERSION`). The Worker looks up only its process-local trusted catalog, canonicalizes a usable directory, and records one immutable path-free audit before executor side effects. Worker admission requires the exact current command schema. Unsupported durable state requires operator recovery.

The Claude Worker supports `start`, `cancel`, and same-session `continue` after an observed terminal in the same workspace. `cancel` writes a native `control_request` interrupt to the running turn's stdin and returns `interrupt_requested` once the matching `control_response` arrives; the interrupted turn is a delivered terminal projected as `execution_cancelled`, and, like any other finished turn, it can be `continue`d afterward. Continuation keeps the exact native session binding and uses Claude local session persistence; old non-persistent sessions are not recoverable. A terminal binding continues within the same Worker process directly, and across a Worker restart only through the Claude registration's `reattestTerminal` proof, which needs the Worker-private record writes beside the Worker database; without it the continuation fails as `axis.worker.continuation_unresumable`, and a resume whose native transcript is gone fails as `axis.worker.launch_failed`. Unsupported/failed adapter continuation is handled by the existing conservative Worker `axis.worker.launch_ambiguous` path; missing prior Worker observation remains `axis.worker.recovery_required`. No continuation failure starts a replacement session. Definite local-runtime failures use fixed `axis.worker.runtime_missing`, `axis.worker.runtime_incompatible`, `axis.worker.authentication_required`, `axis.worker.probe_timeout`, `axis.worker.capability_unavailable`, or `axis.worker.launch_failed` 503 responses. Claude Code authentication-probe failures, including timeouts, return `axis.worker.authentication_required`. Uncertain launch/output identity uses `axis.worker.launch_ambiguous`. Worker failures use the closed `{ error: { code, message } }` response schema.

The Codex Worker continues a predecessor that ended `succeeded`/`failed` or parked on `waiting_for_input` within the same Worker process directly, and across a Worker restart only through the Codex registration's `reattestTerminal` proof, which a Linear-enabled Codex Worker answers from its app-server's non-archived `thread/list` view. A thread that is no longer listed (archived, deleted, or never existed) fails as `axis.worker.continuation_unresumable`; a list that cannot be read fails as `axis.worker.capability_unavailable`. Both refuse before a Run is reserved, so nothing runs and no command is replayed. A Codex Worker without Linear execution enabled still refuses every continuation after a restart as `axis.worker.continuation_unresumable`, and a Codex Run still running at the restart is not recovered.

### `GET /v1/worker/commands/{commandId}`

 read-only command record, authenticated with the same Worker command bearer as `POST /v1/worker/commands`. It returns `200` with exactly one of two closed bodies:

- `{ commandId, status: "recorded", response }`, where `response` is byte-for-byte the response this Worker recorded when it answered the command's dispatch;
- `{ commandId, status: "not_recorded" }`, when this Worker holds no recorded response for that command. Either it never received the command, or it failed before recording a response.

The route performs one primary-key read. It never begins, reserves, or executes a command, and returns no workspace, executor, instruction, or other Worker state. Queries and malformed identifiers are `400`, credentials `401`, and any non-GET method `405` with `Allow: GET` (checked before authentication, as on `/v1/worker/readiness`). A Worker commits a start's or continue's response in the same transaction that records the Run as accepted, so an accepted Run always has a recorded response. A Node command uses this route to converge an `ambiguous` receipt from the Worker's own recorded response instead of re-dispatching.

### Run lifecycle acknowledgements

Cancel acknowledgement means only `interrupt_requested`. Only the exact observed `execution_cancelled` terminal event establishes cancellation. A steer acknowledgement, `steered`, likewise means only that the running turn took the instruction; the Run's outcome is its own later terminal event.

## Router-to-Relay event projection

The projection is the closed `RouterRelayNeutralEventEnvelope` `1.0.0` shape:

```text
{ schemaVersion, conversation, axisSequence, eventId, eventDigest,
  sourceArtifactId, event, recordedAt }
```

`conversation` is the channel-neutral `ConversationIdentity`, used here as
Linear's `{ system: "linear", agentSessionId }`.

There are exactly six lifecycle variants under `axis.router.v1.*`: accepted, progressed, waiting_for_input, succeeded, failed, and cancelled. Waiting carries exact `{ taskId, attemptId, runId, elicitation }`; the closed elicitation has deterministic hashed identities and one question with two or three explicit options. Its mandatory `recommendedOptionId` must equal the first option because Axis's supported native mapping defines `options[0]` as the sole recommendation source. Every option also requires a unique `replyValue` that validates as the deterministic hash of only the enclosing `decisionId` and its `optionId`. Accepted, progressed, and cancelled have exactly `{ safeSummary }`; failed has exactly `{ error: { code, safeMessage } }`. Linear waiting uses only the bare Axis `run.waiting_for_input` extension at Worker version `1.0.0`; Router projects its elicitation directly into the neutral waiting event. Succeeded additionally accepts a validated `task_agent_response` delivery; its Activity identity must name the envelope's own Linear conversation.

An exact blocking Codex `item/tool/requestUserInput` is never answered by Axis. One non-secret question with two or three explicit options, an optional native Other choice, and no auto-resolution is accepted. Fixed-choice elicitation uses `1.0.0-fixed-choice`; Other uses `1.0.0-other` with required closed `other { optionId, replyValue }` alongside the explicit options. After exact interrupt acknowledgement and the matching interrupted terminal, the current Run becomes terminal `waiting_for_input`. Mismatch, duplication, unsupported/freeform-only/secret/multi-question input, terminal races, or transport loss fail closed and cannot become success or waiting.

For Codex success, the transport selects the last exact-bound completed `agentMessage` with `phase: "final_answer"`. It accepts a `phase: null` candidate only when exactly one exists and no final answer exists; commentary, reasoning, plan, tool, raw, mismatched-turn, malformed, and unknown items never become the response. Display text is NFC/LF normalized, repository paths are made workspace-relative, home/temporary paths and credential values are redacted, and disallowed controls or bidi characters fail closed. An inline markdown image whose destination names a file on the machine that ran the agent — a relative path, an absolute one, a `file:` URL, or a path already rewritten to `<home>/…` or `<temporary-path>` — is replaced by `[Axis could not embed this local image: <destination>]`, keeping whatever the path redactions above left; for the temporary family that is the bare `<temporary-path>` placeholder, because the redaction it runs after replaces the filename too. The rule leaves every other destination untouched: any other URI scheme (`http(s):`, `data:`, `mailto:`, `ftp:`, `attachment:`), the protocol-relative `//host/…` form, a bare `#fragment`, and an empty destination. The notice is what this rule emits, not necessarily what is published — the credential redaction named above runs after it, so a destination that is itself credential-shaped (`out/token=…`) has its tail replaced by `[REDACTED]`, closing bracket included, like any other text. Only the inline form is rewritten: reference-style images, raw `<img>` HTML, and bare destinations containing balanced parentheses pass through unchanged, and the rewrite does not exempt code spans or fenced blocks. The response is limited to 4,096 Unicode scalars including the exact visible marker `[… Axis truncated this response to 4096 characters; N characters omitted.]`, with matching structured truncation counts. If no safe candidate survives, the fixed body is `The agent completed the turn, but no safe response could be projected.`

Worker durably stores one validated `RunAgentResponseDelivery`; Router preserves the same immutable `agentActivity` receipt in a validated `TaskAgentResponseDelivery`. Relay uses that exact receipt content and deterministic UUID Activity ID, so retries/restarts reconcile the same Linear AgentActivity instead of creating a second response.

Provider-neutral session/event/artifact IDs and failure codes use exactly `[A-Za-z0-9][A-Za-z0-9._-]*` and are limited to 128 code points; colons, whitespace, and control characters are rejected. `safeSummary` and `safeMessage` are nonempty and limited to 512 code points; `resultRef` is nonempty and limited to 2,048; `sourceArtifactId` is either `null` or a bounded valid identifier. `eventDigest` is exactly lowercase `sha256:` plus 64 hexadecimal characters and must equal the SHA-256 of the canonical closed `event` JSON payload. `recordedAt` must be canonical UTC ISO/RFC3339 text.

For the active AgentSession, Relay requires the whole observed batch to use that exact session ID and a contiguous strictly increasing sequence beginning at `afterCursor + 1`. It validates the complete batch before creating an Activity or changing observation state. A wrong session, duplicate/decreasing/gapped sequence, old/future schema, malformed value, or extension is recorded as `axis.observe.failed` with no projection or cursor mutation.

Relay keeps prompt eligibility separate from observation ownership. `last_operation_key` retains the latest Router operation needed to continue the same AgentSession and Attempt; nullable `observation_operation_key` identifies only the Run that still needs polling. A terminal event atomically persists its projection, advances the cursor, and clears observation ownership under an operation-key-and-cursor compare-and-set. For waiting, the same transaction appends the immutable decision ledger and exactly one `elicitation` Activity whose Markdown contains the frozen canonical Linear `User.url` as plain URL text plus exact reply values; Linear converts that URL to a native mention and derives the single Comment from the Activity. Axis does not call `commentCreate` or send `bodyData`. The AgentSession remains open. Only the same authenticated frozen owner may resolve the pending decision, and only with an exact decision-bound `replyValue` from the exact provider-reread Activity-derived Comment or prompt Activity. Relay uses that value to find the ledger decision and option before setting its expected decision claim. The claim, resolution, and successor command are atomic; exact accepted-event replay returns its original command, while an unknown/stale token or second source cannot consume either the old or current decision. Every label from every immutable decision ledger row in the AgentSession remains permanently reserved as a non-reply and cannot degrade into an ordinary continuation prompt after later decisions resolve. Ordinary comments still require the structured `@Axis` mention rule. Wrong-owner, wrong-thread, stale, duplicate, or non-exact replies fail closed. Labels alone do not authorize decision resolution. For the waiting-stop exception, an authenticated explicit stop atomically appends its closure and one final `response` Activity while closing the pending decision but retaining the AgentSession. Exact stop replay adds nothing, and no cancel or continuation command is created for the stop itself.

Explicit stop has two disjoint paths. For an ordinary active Run, Relay sends the exact Router cancel request and keeps observing until correlated cancellation clears that Run's observation ownership; the AgentSession remains open. For a pending waiting decision, the Run is already terminal: Relay instead atomically records the closure and exactly one final `response` Activity, retains the AgentSession, and sends no cancel or continuation for the stop.

Observed accepted, progressed, waiting, succeeded, failed, and cancelled events use `action`, `thought`, `elicitation`, `response`, `error`, and cancellation-response Activity behavior. Stop, cancellation, dispatch failure, and runtime failure do not end an AgentSession. `Agent Session Ended` is reserved for a future verified Linear archive ingress; until that fact is supported, Relay does not emit or infer it.

## Component versions

Worker commands use exactly `1.0.0`; Router-to-Relay events use exactly `1.0.0`.
Components must run the same compatible build. There is no alternate wire reader
or replay translation. The [OpenAPI contract](../openapi.yaml) describes the HTTP
routes; in-process interfaces retain their own versions.

## `POST /webhooks/linear`

Required headers are `Linear-Delivery` (UUID v4), `Linear-Event`, `Linear-Signature`, and `Linear-Timestamp`. The parsed `webhookTimestamp` must also be within one minute. Relay supports the pinned `AgentSessionEvent.created`, `AgentSessionEvent.prompted`, `PermissionChange.teamAccessChanged`, and `OAuthApp.revoked` shapes; authenticated unsupported preview actions are durably quarantined without execution.

- `200`: delivery and encrypted payload are durable; exact or permitted timestamp-only replay is identified as duplicate.
- `400`: owned input/header/JSON shape is malformed.
- `401`: signature or timestamp is invalid.
- `409`: delivery identity or authenticated semantic provenance conflicts.
- `413`: raw request exceeds 1 MiB.
- `503`: durable acceptance is unavailable.

No execution result is returned synchronously.

For AgentSession events, Relay resolves a final instruction before persisting an encrypted session input or executable command. Created events use an authenticated exact Issue read that must match the webhook Issue and Team. The instruction Router receives for every Linear dispatch is one envelope: the exact Issue key, ID, and title; the user command when there is one (the validated exact Comment instruction for a comment-origin start, the verified Activity body for a prompt, with the Axis App User's own mention markup removed); the source and parent Comment IDs for proven comment provenance; and a closing hint to read the Issue description and comment thread from Linear. The Issue description is never included and never refuses a start, and every stored digest covers the user command alone (`docs/LINEAR-PROVIDER.md` "Pinned shapes"). Validated signed-event guidance is a separate instruction section; webhook `promptContext` and presentation fields never become the user command. The bounded exact read retries only provider-classified temporary failures (`RelayError.retryable`) up to three times and fails retryably without dispatch if no exact input can be obtained; permanent provider/schema/authorization failures are terminal and fail closed. Every non-comment, non-stop prompted event uses only an authenticated exact provider Activity read with matching Activity/session/human identity, and typed prompt content. Once the Activity verifies, a prompted event also makes one exact Issue read for the envelope: a retryable failure leaves the delivery to its retry budget, and a definite one refuses the prompt with the `issue_unavailable` reason. A verification failure on that non-comment path uses the existing `axis.relay.missing_instruction` delivery code, does not reach Router, and emits exactly one refusal Activity per logical Activity. That Activity's body is rendered from the refusal's own closed reason (`Axis did not run this request.` / `Reason: <fixed reason copy>` / `<fixed action copy>`) and is provider-neutral -- it names no executor. It carries no interpolated value of any kind: the reason enum selects one of a frozen table of fixed sentences, and no provider diagnostic, prompt byte, or identity reaches it. Comment-origin events use exact thread, author, and membership provenance gates plus fixed comment-specific rejection rules; authenticated session creation supplies delegation authority for a verified creator instruction; prompted events retain exact typed Activity authority. Prompt text and webhook presentation fields remain excluded from safe errors, logs, audit metadata, and Router durable state.

## OAuth and health

- `GET /oauth/linear/install`: operator-bearer-gated redirect using `actor=app`, one-time state, S256 PKCE, and a local-only `return_to`.
- `GET /oauth/linear/callback`: consumes state once, exchanges the code, resolves App User/workspace identity, stores only an encrypted credential reference, and redirects locally.
- `GET /health/live`: listener liveness only.
- With `axis serve`, `GET /health/ready` additionally requires active component
  listeners, available Node storage and ready Codex app-server evidence through
  a bounded authenticated Node probe. Claude Code uses its startup preflight
  and active listener; continuous native readiness is unsupported. The response
  shapes and authorization remain the same as standalone Relay readiness.
- `GET /health/ready`: unauthenticated traffic readiness, with `200 {"status":"ready"}` or `503 {"status":"not_ready"}`. It checks credential-store readability and channel-store schemas without whole-database pragmas. Every store runs full integrity checks at construction. During operation, conversation and run-chain data checks run on the observe tick with `linear.integrityCheckIntervalMs` (60 seconds by default); inbox, ledger, and outbox data checks require `/health/diagnostics`. Schema drift is checked on each call. A store condemned by a full check refuses operations until a later full check passes. Data damage can be served before detection: the scheduled detection delay is `max(integrity-check interval, observe-tick interval)` plus a tick's duration, which provider calls can lengthen. Tick intervals default to one second and are configured independently of integrity cadence.
- `GET /health/diagnostics`: operator-bearer-gated, on demand, never polled. Runs the full per-store check -- `PRAGMA integrity_check`, `PRAGMA foreign_key_check`, the schema fingerprint sweep and the cross-table consistency queries -- and reports each store separately: `200 {"status":"healthy","vault":true,"stores":{...}}` / `503 {"status":"degraded",...}`, and `401` without the operator token, refused before any scan is run. These are whole-database reads on a synchronous handle, which is why they are not on the readiness path; a `ready` process can still report `degraded` here.

## Internal execution interface

`ChannelCommandPort` (`src/router/channel-command.ts`) is the channel-neutral dispatch interface: `submit`, `reconcile`, and `reconcileInterrupted`. `nodeChannelCommandPort` (`src/relay/node-routing-composition.ts`) is the packaged implementation over a real `Router`'s Node route, used by Linear. Router owns command/event journals; Worker owns native binding.

## Routing and review ownership

Router freezes the trusted Team binding or Node selection at Task creation.
Review scheduling belongs to the caller's orchestration and reaches Axis as an
ordinary start. HTTP clients and provider output cannot select a native session,
workspace path, executor, or fallback.

The canonical HTTP contract is [openapi.yaml](../openapi.yaml).

### Native Other answers

Other is separate from the two or three explicit options and cannot be recommended. Its IDs are deterministic and decision-bound; its display label is always `Other`. Linear accepts the exact Other reply token, whitespace, and a nonempty answer from the same frozen human owner and exact reread source. The answer is normalized to NFC and LF, trimmed, and limited to 4096 Unicode scalar values; lone surrogates, unsupported controls and bidirectional controls are rejected. Ordinary fixed-option replies still require the exact token alone. The opaque Other token is removed from the continuation instruction, which contains `Other: ` plus the answer. Raw answer text is stored only through the existing encrypted session input, never in the immutable selected label or public waiting projection. Literal `Other` and stale or malformed reply tokens remain reserved after resolution or stop.

## Relay-private Linear execution access

[Linear execution access 1.0.0](LINEAR-EXECUTION-ACCESS.md) defines the internal
issuer, verifier, invalidation, safe context and error surfaces.
The optional protected HTTP exchange uses the separate credential contract; Runtime v1 envelopes carry no credential. Clients cannot select
an installation; the trusted execution grant fixes workspace access. Token use
requires the Relay-only `withAccess` boundary, not a cached verification result.

## Independent Linear CLI authentication

The GraphQL proxy also accepts [Axis CLI credentials](LINEAR-CLI.md), independent
of Run authority. They use the same request/response and write-ambiguity rules.
