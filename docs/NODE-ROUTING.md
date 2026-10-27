# Node routing contract

Router dispatches every channel command to a Node, and the Node dispatches it to one of its Workers. Settings
and operator procedures are in [`NODE-ROUTING-CONFIGURATION.md`](NODE-ROUTING-CONFIGURATION.md).

**Router.** An authorized scope's opaque endpoint reference is projected to exactly one logical `nodeRef`, and
`freezeRouterNodeTaskTarget` freezes that result in a Task row. `Router.startNode` calls it inside its command
claim when a new start has no Task yet; the freeze never creates a Run, command, or SessionBinding, chooses a
Worker, or calls a Node. `startNode`, `continueNode`, `cancelNode` (which sends a Node
`steer` when its `interrupt` is `"steer"`) and authenticated `ingestNodeRunEvent` are Router's only lifecycle. Router persists a pending Run and redacted canonical command
intent before its one dispatch. Once dispatch has crossed that cutline, exact recovery calls only the Node
receipt query; it never sends a second start. An authenticated event arriving before accepted receipt evidence
is buffered and projected through the Router cursor/order logic only after its origin and complete Runtime
lineage match the accepted route; missing or conflicting origin evidence is durably quarantined. Continue,
cancel and steer use only the predecessor's frozen selection, reservation, and complete SessionBinding.

Router's listener, whose only route is `POST /v1/router/node-run-events`, supplies that origin evidence: its
bearer is mapped out of band to one exact `{nodeRef,nodeId}` and a closed Worker-origin allowlist, and its
closed `1.0.0` body must claim the same Node and one allowlisted origin. Router compares that authenticated
logical Node with the Run's immutable Task target on first observation and every replay; the Worker origin
remains subject to the durable reservation/SessionBinding checks. A different logical Node cannot address the
Run even with the identical physical Worker origin. Each Node event binding's token is the only credential this
listener accepts.

**Node.** `startNode` consumes a validated `NodeConfiguration`; the installed
`axis-node` entry point derives it from the selected TOML declaration. It opens the accepted-receipt-aware
journal, dispatches commands through an exact-target client inventory, and runs the Worker-authenticated event
ingress and durable outbox. Before every delivery attempt the pump rereads the Run route and classifies it
through durable receipt evidence. It delivers only events whose Run has an accepted route, deriving `nodeRef` and
Worker origin from the retained route rather than from the event envelope, and uses the separate bearer
`node-<id>-router-events.key`. The adapter retries only transport/timeout/interrupted reads and retryable
HTTP outcomes, and maps permanent protocol or authority failures to quarantine. Delivered and quarantined state
is restart-safe, and the journal's eligible-head query preserves per-Run FIFO.

## Authority and versions

The Router freezes a `1.0.0-node` target containing provider-scoped `scope`, `bindingRevision`, `bindingDigest`,
the versioned `nodeRef`, `workerSelection`, and `workspacePolicyRef`. It does not store a Worker, executor, or
Worker endpoint in this target. The Node selects the actual Worker from its local inventory; the selected
identity belongs to Run evidence, not Task authority.

`src/shared/node-routing-projection.ts` owns the snapshot and Run-reading DTO. `routerNodeBindingDigest`
computes a deterministic digest of normalized frozen mapping fields; it identifies configuration evidence and
is not an authentication signature.

Logical Worker keys and both members of Node/workspace references use one opaque identifier grammar at
selection, snapshot, and Node request admission: 1–128 ASCII letters, digits, dots, underscores, or hyphens,
starting with a letter or digit. Both provider scope members, `tenantId` and `scopeId`, use the same bounded
domain at snapshot validation, digest construction, and Run reading.

The resolver first applies scope binding authority. Linear keeps exact binding, unconfigured-only default
fallback, and Team exclusions; disabled, stale, or incompatible bindings never fall back. Once authorized,
the binding's versioned `endpointRef` joins a Router-local projection to
exactly one versioned `nodeRef`; zero, duplicate, or malformed matches fail closed. The binding contributes only
scope, revision, that join reference, and the optional workspace-policy reference.

`freezeRouterNodeTaskTarget` performs lookup, resolution, and insertion under one `BEGIN IMMEDIATE`
transaction. An existing Task returns its stored target without reading current bindings, Node routes, or
selection input. A missing Task resolves and validates one `1.0.0-node` target, proves its scope matches the
Task identity, then atomically writes Task identity, `task_json`, and `target_snapshot_json`. An existing
snapshot is never updated, so a changed mapping affects only a new Task.

The Node protocol is version `1.0.0`. The completed downstream Worker command is `1.0.0`, validated with Runtime
v1, and carries the reservation's `executionTarget` so the Worker can refuse a command addressed to an identity
it no longer has. Before Node selection, the start identity contains the SessionBinding lineage without its
`workerId` and `executorId`; only the Node supplies these fields, validates the complete graph, and sends it to
the Worker.

`NodeSessionBinding` excludes native executor session handles in both pending and resolved forms;
native-session storage is Worker-owned.

`src/shared/router-node-commands.ts` exports `validateNodeCommandRequest` and `completeNodeWorkerCommand`. A
request contains `nodeRef`, `target: {selection}`, and the `command`. Completion consumes an immutable
`{workerKey, target}` reservation; continue, cancel and steer also require the prior durable route. Neither
function searches the inventory.

`NodePriorRoute.sourceRef` retains the complete Runtime Task source reference from the original command:
organization, issue and AgentSession IDs for Linear. It must agree
with the `(sourceSystem, sourceSessionId)` lookup pair, whose system is `linear`.
Callers derive it from the retained original Task, never from the incoming continuation.

Pending start/continue commands have a UTF-8 JSON budget, `NODE_PENDING_COMMAND_LIMIT_BYTES`, that reserves
room for the largest legal execution IDs and `executionTarget`, keeping every admitted command within the
65,536-byte Worker limit after completion. The outer request limit is 128 KiB.
`buildNodeCompletionBoundaryVectors` supplies byte and command oracles for both operations.

`validateNodeCommandResponse` checks a closed accepted response containing `contractVersion`, `nodeRef`,
`commandId`, `status`, `evidence`, and `result`. `evidence` contains the resolved key, actual target, and full
SessionBinding; `result` is the Worker response. The caller supplies the authenticated Node reference and
physical Node ID separately from wire data.

## Node execution service

`NodeCommandService` is the Node's command service. `POST /v1/node/commands` and
`POST /v1/node/command-receipts` serve only exact top-level `1.0.0` bodies; anything else is refused
`400 axis.node.malformed` before any service runs. Admitted command receipts always use `202`, including
replay and terminal receipts; receipt reads use `200`, including `not_found`.

For a new start, the journal first checks command intent, maintenance, Run ownership, and provider-scoped
first-route ownership inside `BEGIN IMMEDIATE`, then invokes the synchronous inventory resolver and atomically
retains its `{workerKey, target}` reservation. Continue, cancel and steer load the accepted predecessor and never
call inventory. A command ID already owned by a different operation or canonical intent is
`axis.node.intent_conflict` before predecessor lookup or selection. A new start losing a first-route race is
`axis.node.binding_conflict`.

After reservation the service records an exclusive owner-bound `dispatching` receipt before its one Worker call.
Separate service facades over the same journal share singleflight, and SQLite dispatch ownership prevents a
second connection or process from dispatching the same command. A validated matching Worker response becomes
`accepted`; an allowlisted definite Worker rejection becomes `rejected`; transport uncertainty, malformed
response, or execution-identity mismatch becomes `ambiguous`. Every result is returned from durable readback,
and stopped-process `recover` converts a crashed dispatch to `ambiguous` without authorizing redispatch.

## Worker selection

Local Worker assignment belongs to the Node. The Relay validates and passes on the source's identity and label
facts; it configures no rules and produces no `workerKey`. The Router selects a Node and freezes that route, and
retains the Node's reported choice as evidence.

`src/shared/worker-selection.ts` carries the facts. A `policy` selection holds the source scope
(`{system:"linear", tenantId, scopeId}`, the same triple the Router freezes into its target snapshot) and the executor
labels present on the source, deduplicated and in the fixed order `claude, codex`. The order is
part of the contract: the selection is folded into `bindingDigest`. `policy` is the only selection shape; any
other payload is refused as malformed.

`src/node/runtime-config.ts` owns the decision, applied only when a new session is first assigned:

| Priority | Input | Node configuration | Result |
| --- | --- | --- | --- |
| 1 | exactly one executor label | `nodes.workers[].labels` | that kind's configured Worker |
| 2 | no executor label, scope matches a rule | `nodes.workerScopeRules` | that scope's Worker |
| 3 | no executor label, no matching rule | `nodes.defaultWorker` | the default Worker |

The tiers decide which rule applies; they are not a fallback chain. A named executor this Node cannot satisfy is
a definite refusal (`axis.node.target_unknown`), never the default. Two or more different executor labels is
`axis.node.selection_conflict`. Both refusals are raised inside the command claim, before any row is written and
before dispatch.

A label names a kind of executor and is never a `workerKey` or a process identity. The mapping is explicit and
uses each Worker's explicit `labels` array, defaulting to `codex` or `claude`
from its `kind`. Each label may occur on only one Worker within a Node; when
one kind has several Workers, give the selected one that label and set the
others' labels explicitly, for example `labels = []`. Labels outside this vocabulary take no part in
selection.

A source names an executor in one of two spellings. The bare names `claude` and `codex` match
exactly and case-sensitively. `DEFAULT_WORKER_LABEL_ROUTES` adds `WORKER:CLAUDE` and `WORKER:CODEX`, matched case-insensitively after trimming. Linear reports a label inside a
label group as `<group>:<name>`, so the `WORKER` group's children arrive in that form.
Both spellings of one executor collapse to one entry. The table changes
only which source labels count; the wire carries the two executor names.

| Label context | Sent to the Node | Outcome |
| --- | --- | --- |
| Available, one executor label | `policy` with that label | its configured Worker, or a definite refusal |
| Available, several executor labels | `policy` with each | `axis.node.selection_conflict` |
| Available, no executor label | `policy` with `[]` | scope rule, else the Node default |
| Unavailable or malformed | no command | reject or retry, never the default |

"Could not obtain a trustworthy label context" and "the source carries no executor label" stay distinct: only
the second uses the default. `channelWorkerSelection` rejects an unresolved start.

Selection runs once. A follow-up gets `admitRetainedReservation` and only a start gets
`resolveNewSessionReservation` (`src/node/command.ts`), so changing labels, scope rules, the default or the
inventory never migrates an open session.

## Worker inventory and deterministic refusal

A Node may start before any Worker is deployed onto it. Set `workers = []` in
its `[[nodes]]` declaration and omit `defaultWorker`. Relay derives no logical
targets and an empty allowed-origin list for that Node, denying every event
origin. At least one Node declaration is required in the topology.

`/v1/node/readiness` reports such a Node as `service.status = "configured"` with an empty `workers` array.
Readiness is an aggregate and never claims a Worker it does not have.

A Node starts whatever its journal holds. When the inventory no longer names a session's Worker (empty or not):

- the Node starts, and readiness reports only the Workers it has;
- a `continue`, `cancel` or `steer` for that session is refused through the inventory's `admission` before the
  Node records or sends anything, with `axis.node.worker_unavailable`;
- a new session whose selection cannot resolve is refused with `409 axis.node.worker_unavailable`, distinct from
  `404 axis.node.target_unknown`, which means the requested target ref is not one this Node knows. This is an
  HTTP-level refusal made before any receipt exists.

At startup `axis-node` writes `{"code":"axis.node.sessions_without_worker","count":N}` to stderr after
`Axis Node ready` when `N` retained sessions name a Worker the inventory does not admit, and
`axis-routing-preflight node` reports the same count as `retainedSessionsWithoutWorker`, never as a blocker. Both
use the dispatcher's own `admission`. A Worker deployed again is picked up when the Node restarts.

`HttpRouterNodePort` classifies failures deterministically: a non-expected status is parsed as a strict
`NodeCommandError`, and a code on `NODE_DEFINITE_ERROR_CODES` served at exactly its defined status is definite.
Everything else (a 5xx code other than `draining`, an unknown code, a status/code mismatch, an unparseable body)
is ambiguous. `503 axis.node.draining` is the one definite 5xx code: a draining Node raises it only for a fresh
start or continue, inside the claim transaction and before anything is written. Every refusal the command
listener sends uses the command error envelope: authentication `401`, unknown route `404 target_unknown`, query string
or undecodable body `400`, media `415`, outer size `413`, method `405`, the closing listener `503`, and unexpected
failures `503 storage_unavailable`. A wrong method is `malformed` served at `405` rather than `400`, so it is
ambiguous.

Router answers a definite refusal of a **start** with the command's decision instead of parking it in recovery,
for `worker_unavailable` (as `axis.router.node_worker_unavailable`), `target_unknown` (as
`axis.router.endpoint_unknown`), `selection_conflict` (as `axis.router.selection_conflict`) and `draining` (as
`axis.router.node_draining`). A follow-up is answered the same way only for `worker_unavailable` and `draining`,
the two refusals the Node makes for a follow-up before recording anything; Router then retains nothing for it
(`#forgetRefusedNodeFollowUp`), so `reconcileCommand` reads `absent` and the channel's slot is freed. Other
definite codes take the recovery path.

The row is written exactly as the recovery path writes it (`receipt_pending`, no reservation, no receipt digest,
no result); only the answer returned to the caller differs. A terminal `rejected` row is not written, because it
needs reservation and receipt evidence a pre-receipt refusal does not have, and the
`router_node_command_state` transition trigger forbids returning the row to `prepared`. So the decision is
live-path only: resubmitting the same command id takes the receipt-query branch and answers recovery. No channel
resubmits a settled command.

## Session identity and retries

Session identity is the pair `(sourceSystem, sourceSessionId)` from the source reference:
`linear` and the Linear AgentSession ID.

The Node reserves a first route before calling a Worker. The same command and intent replay the durable receipt.
A different start for the same session conflicts with `axis.node.binding_conflict` and never calls a Worker; it
never becomes a continuation.

Continue, cancel and steer use the original durable route. A known route with an unavailable Worker or native
session reports `axis.node.recovery_required`; it is not an unbound session eligible for fresh selection.

If a continuation carries `run.priorRunId`, it must match both the outer continuation predecessor and the durable
route. Absence is valid. A continuation also retains the
prior Task's correlation ID and Runtime version. `buildNodeContinuationVectors`,
`buildNodeContinuationIdentityVectors` and `buildNodeContinuationSourceVectors` cover matching, conflicting
and absent predecessors, identity drift per Runtime version, and source-reference drift.

## Execution evidence and event ordering

Authenticated Node receipts are the authority for the actual execution identity. A receipt is correlated with the
exact command, Node reference, Run, Task, SessionBinding lineage, and completed Worker response before it is
recorded. A matching identifier alone is not proof of origin.

| Observation | Required disposition |
| --- | --- |
| No accepted receipt | Keep actual execution evidence pending |
| Valid accepted receipt | Persist Worker and executor identity atomically |
| Identical replay | Keep the same evidence; do not execute again |
| Conflicting identity or lineage | Reject/quarantine; never overwrite trusted evidence |
| Event before accepted receipt | Hold it untrusted until the receipt is recovered and checked |
| Receipt lost after dispatch | Reconcile the same command; do not create a new start |
| Ambiguous dispatch or missing native binding | Keep an explicit ambiguous/recovery state |

An event arriving first never bootstraps a trusted Worker identity. Matching events are projected only after the
accepted receipt establishes that identity and the independently authenticated event origin matches it. Missing
or wrong origin is rejected even when every envelope identifier matches.

The Router HTTP ingress accepts a 128 KiB outer request. Authentication happens before query, media, or body
parsing. On durable buffering or projection it returns only the `1.0.0` Node reference, source message ID, Run ID,
state, and replay bit (`202` first observation, `200` replay). Quarantine is a stable `409 axis.router.conflict`;
it is durable but is not acceptance of the lifecycle fact. The success/error/status and durability matrix is in
[`API.md`](API.md#package-root-router-node-lifecycle).

`validateNodeReceipt` validates accepted and intermediate receipt states. Intermediate states carry
`reservation`, never `evidence`. `nodeReceiptExecutionEvidence` accepts only validated receipts and yields
`null` before acceptance. Re-read JSON is validated again.

`buildNodeTimingVectors` in `test/support/router-node-commands-fixtures.ts` exports ordered inputs and expected
states for early events, receipt loss and reconciliation, conflicting identities, and replay versus a different
start on the same session. Waiting events carry the Worker waiting-extension `1.0.0` outer version
(`WORKER_WAITING_EXTENSION_VERSION`), distinct from the nested Runtime version used for binding lineage.

## Run reading and CLI

`RouterNodeRunTargetEvidence` separates the frozen target from execution. `RouterRunExecutionEvidence` is either
`pending` with both IDs `null`, or `recorded` with both IDs populated. Run, Worker, and executor IDs use the
bounded opaque grammar of receipt identities.

`axisCliNodeRunTarget` produces the `axis.cli.run.v1` Run-reading DTO with the Node target, logical selection,
workspace policy, and actual Worker/executor IDs. `axis topology list` and `axis topology tree` place it in
`historicalRunBinding` when their Run selector resolves a Node Run. Before evidence is available both IDs are
`null`, and human output says no Worker/executor evidence is recorded. The CLI never queries inventory to guess
either value. Consumers branch on `historicalRunBinding.schemaVersion`.

## Integration boundaries

Linear default-Team/exclusion behaviour is preserved. A default Worker
selection grants no additional scope access.

There is no label-context cache. Linear's `AxisIssueExecutionContext` read already carries `labels(first: 250)` in
one Issue request.

## Worker identity drift

A Worker whose configured identity (`nodeId`, `workerId`, `executorKind`, `runtimeProfile`, or session executor)
no longer matches its Node inventory entry refuses every start and continue with the definite
`axis.worker.identity_mismatch` 409, before `reserveRun`. The Node writes a `rejected` receipt, Router settles
the reservation as `axis.router.worker_identity_changed` and releases the session slot, and the channel tells the
operator to align the Worker with the inventory. Cancel and steer carry no execution target and are not
identity-checked.

A definitely refused continue has already minted a successor Run. When the call that claimed the continue also
settles its rejected receipt, Router restores `router_tasks.current_run_id` to the value it recorded at that
claim (`#restoreClaimedPointerFromRefusedSuccessor`), so the next prompt routes from the prior Run, and
`#latestNodeRun` skips a successor with no execution and only a rejected command. A rejected receipt settled
later by the receipt query has no recorded claim pointer, so the pointer stays on the successor and the next
continue fails `axis.router.recovery_required`.

Every journal open re-validates each accepted receipt's Worker response against `WORKER_COMMAND_VERSION`, so a
journal holding an accepted receipt from another command version is refused (`SqliteNodeJournal.open` and
`inspectMaintenance` fail `axis.node.storage_unavailable`).
