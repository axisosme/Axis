# Node process contract

The [Node routing contract](NODE-ROUTING.md) defines the Router/Node boundary, and the Node command
listener serves only that contract (`contractVersion "1.0.0"`). Package-private contract version `1.0.0` in
`src/shared/router-node.ts` names the Worker-facing event listener, readiness, and maintenance shapes.

The package ships the standalone `axis-node` process. It composes the durable SQLite journal, the command
service, outbound Worker clients, the Router event-delivery pump, readiness, and a two-listener HTTP owner.
There is no package-root export, Worker supervision, or native runtime ownership.

## Standalone runtime

`src/node/runtime.ts` parses all configuration before side effects, opens the journal with
`SqliteNodeJournal.open`, builds `NodeCommandService` over one `HttpNodeWorkerClient` per configured
Worker, and dispatches only after the journal has reserved a Worker for the command. The journal takes no
SQLite exclusive lock, so a second process can open it.

No Worker inventory refuses startup. Startup counts the retained sessions whose Worker the inventory does not
admit; their follow-ups are refused at dispatch. When that count is non-zero, `axis-node` writes one stderr
line after `Axis Node ready`:

```json
{"code":"axis.node.sessions_without_worker","count":N}
```

The runtime then starts the durable event pump and both HTTP listeners. Startup succeeds only after journal
integrity, the sweep, and both listeners are established. An initialization failure closes any partial
listeners, stops the pump, and releases the journal so the same configuration can retry.

`axis-node` prints only `Axis Node ready` to stdout. `SIGINT` or `SIGTERM` first gates and closes both
listeners, then stops the pump and closes the journal. It never starts, signals, or closes a Worker or native
executor. Startup failure is the fixed `axis.node.startup_failed`; shutdown failure is the fixed
`axis.node.shutdown_failed`.

Readiness performs a read-only journal probe and reports storage unavailable if the pump has latched a
background storage failure. Delivery keeps one active request per Run while a bounded number of Runs (eight by
default) progress independently.

## Router event delivery

`RouterEventDelivery` (`src/node/router-event-delivery.ts`) makes one bearer-authenticated JSON `POST` to
Router's `/v1/router/node-run-events` per delivery, using `node-<id>-router-events.key`. It rejects
redirects, bounds each attempt with an abort timeout, accepts only the closed correlated receipt of
[the command contract](NODE-ROUTING.md), and retains no raw transport response or error detail.

The pump (`src/node/event-pump.ts`) reads pending heads from the journal, allows one active delivery per Run,
persists only the closed unavailable/protocol-conflict diagnostics, and quarantines a permanent conflict as
`axis.node.binding_conflict`. It delivers only events whose Run has an accepted command route. Transient retries use
the persisted attempt count and timestamp with capped exponential delay. A durable event admission wakes the
pump.

## HTTP boundary

`src/node/http.ts` exposes `startNodeHttpService` through its module path only. It owns a command server at
`host`/`port` and a separate event server at the numeric-loopback `eventHost`/`eventPort`.

The command server accepts only Node-token-authenticated `POST /v1/node/commands`,
`POST /v1/node/command-receipts`, and `GET /v1/node/readiness`, and the first two serve only the command contract.
Commands and receipt queries accept exactly `contractVersion "1.0.0"`; any other identifier is
`400 axis.node.malformed` before any service runs.
Every refusal the command listener sends uses the command envelope
`{ contractVersion: "1.0.0", error: { code, safeMessage } }`: authentication `401`, unknown route
`404 target_unknown`, query string or undecodable body `400`, media `415`, outer size `413`, method `405`, the
closing listener `503`, and unexpected failures `503 storage_unavailable`. Router's command transport classifies
those codes by `NODE_DEFINITE_ERROR_CODES`. A wrong method is `malformed` at `405` rather than its defined
`400`, so it stays ambiguous. Readiness bodies keep `contractVersion "1.0.0"`.

The event server mounts only `POST /v1/router/run-events`, authenticated by the per-Worker event-token registry
with a durable Worker identity check, plus Node-token-authenticated `GET` and `POST /v1/node/maintenance`. It
keeps its own `1.0.0` error envelope. A Node token equal to any event token refuses startup, so the two token
classes cannot authenticate each other's listener.

Route matching is exact on the raw origin-form path. The listener splits the request target once at the first
`?` and compares the raw path with its route table; it does not decode, fold case, or normalize the path. Every
non-exact target returns `404 axis.node.target_unknown` before method, authentication, or body handling. For an
exact path with a nonempty query, a wrong method is `405` with the route's `Allow`, a correct method without
valid authority is `401`, and an authenticated correct method is `400 axis.node.malformed`.

An admitted command returns its retained receipt at `202`, including exact replay and terminal receipts; a
command receipt query returns the retained receipt or `not_found` at `200` (see
[the command contract](NODE-ROUTING.md#node-execution-service)). Readiness authenticates before calling its
producer, accepts no query or body, returns every safe aggregate at `200` even when storage or Workers are
degraded, and fixes producer failure at `503` storage unavailable. Event ingress answers the first durable ACK
with `202` and replay with `200`.

Both servers require one exact Authorization header, compare SHA-256 digests with `timingSafeEqual`, reject
unsupported media or content encoding before streaming a body, and use strict UTF-8 JSON with duplicate-key
detection at every depth. The outer limit is 128 KiB and the nested command/event limit is 64 KiB.
`src/node/http-transport.ts` owns these shared transport primitives; listener-local authentication, route and
method policy, and Worker identity lookup stay with each listener. The event registry retains only token
digests.

The service owns its servers and accepted sockets, not the injected command service, journal, readiness
producer, event ingress, or pump. `close()` gates new admission, closes both listeners together, lets work
whose body was already admitted finish, and awaits it all under one non-sliding deadline (30 seconds by
default). On expiry it force-closes owned connections and rejects one fixed local error; repeated calls share
the result. A second-listener startup failure closes the first. No journal transaction spans Worker or Router
network I/O.

## Configuration

`src/configuration.ts` parses the shared TOML declaration without side effects.
`axis-node --config <path> --node <id>` loads it and calls
`deriveNodeConfiguration`; `--config` defaults to `axis.toml` in the current
directory. A Node selector overrides `[host].node`, and a sole Node selects
itself. Role derivation reads only its generated credentials and returns a deeply
frozen snapshot. The local `NODE_CONTRACT_VERSION` (`1.0.0`) still describes
readiness/maintenance correlation; command and receipt contracts use `1.0.0`.

| Category | TOML fields and exact rule |
| --- | --- |
| Identity/storage | `nodes.id`, `nodes.ref` (defaults to ID), `nodes.version` (defaults to `v1`). IDs use bounded opaque identifiers. Journal is `<dataDir>/node-<id>.sqlite`. |
| Listeners | `nodes.host` defaults to `127.0.0.1`; `nodes.eventHost` requires numeric loopback and has the same default. Required `port` and `eventPort` are integers 1–65535 and differ. |
| Router events | `router.url`, otherwise derived from Router host/port. Numeric-loopback HTTP or HTTPS origin only, without credentials, base path, query or fragment. Bearer comes from `node-<id>-router-events.key`. |
| Workers | `nodes.workers` is a closed array, possibly empty. `nodes.defaultWorker` is a local key, required with multiple Workers, implicit with one, omitted with none. Each Worker has a globally unique ID and endpoint reference; `key` defaults to its ID. Targets and exact Node identity are derived together and checked by the shared validator. |
| Worker safety | Worker `host` is numeric loopback. Ports differ from both Node ports and every sibling Worker port. Command/event credentials are separate generated files, checked for format and reuse before storage opens. Readiness mode derives from `kind`: `codex` or `unsupported` for Claude Code. |
| Host roots | Optional `nodes.workers[].hostRoot` resolves relative to TOML and must be a unique non-root path within its Node. It supplies maintenance ownership metadata, without process supervision. |
| Timing/selection | `nodes.workerDispatchTimeoutMs`, `routerEventTimeoutMs`, `workerReadinessTimeoutMs`, `eventRetryInitialDelayMs`, `eventRetryMaxDelayMs`, `eventMaxConcurrentRuns`; see [bounds and defaults](CONFIGURATION.md#schema-reference). Worker `labels` and `nodes.workerScopeRules` define selection. |

Configuration errors name schema fields without configured endpoints, tokens,
paths or input values; the binary emits fixed `axis.node.startup_failed` on
startup failure. See [routing configuration](NODE-ROUTING-CONFIGURATION.md) and
[managed configuration replacement](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade).

## Readiness

`src/node/readiness.ts` produces a safe inspection aggregate with separate configured-service and
read-only-storage entries and no aggregate `ready` verdict. For the first configured entry of each Worker ID,
only `readinessMode:"codex"` makes the authenticated `GET {workerEndpoint}/v1/worker/readiness` request, with
`Authorization: Bearer {commandToken}`, no redirects, its own timeout, and at most 64 KiB of strict UTF-8
JSON. Only the closed Codex readiness shape and its matching `200/ready` or `503/degraded` pair are projected.
Other Workers make no request and no provider claim; transport failures and invalid responses project as
`unknown` without an error or endpoint.

## Wire shapes

All objects and union branches are closed; nested Runtime shapes use the pinned axis-contracts validators. The
command listener's shapes are the [command contract](NODE-ROUTING.md).

| Surface | Exact fields |
| --- | --- |
| Node ref | `{ref,version}` |
| Worker identity | `{nodeId,workerId,executorKind,runtimeProfile:{ref,version}}` |
| NodeTarget | `{endpointRef:{ref,version},identity,sessionBindingExecutorId}` |
| Event-listener error | `{contractVersion,error:{code,safeMessage}}` (`1.0.0`) |
| Worker failure | `{code,safeMessage,certainty}` |
| Maintenance request | `{contractVersion,mode}` |
| Maintenance status | `{contractVersion,nodeRef,mode,idle,blockerCounts}` |

Worker commands are version `1.0.0`. Start is exactly
`{contractVersion,commandId,command:"start",start,instruction,workspacePolicyRef,executionTarget}`, where
`executionTarget` is `{identity:{nodeId,workerId,executorKind,runtimeProfile},sessionBindingExecutorId}`
completed by the Node from the reservation. Continue uses `command:"continue"` and adds only
`continuation:{priorRunId}`. Cancel is exactly `{contractVersion,commandId,command:"cancel",target:{runId}}`.
Instructions are nonblank. Command IDs, continuation/cancel Run IDs, and both fields of a non-null workspace
policy reference use the Worker opaque-ID grammar `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. Workspace policy is a
closed reference or null. RunStartEnvelope validation checks Task/Attempt/Run/session-binding and correlation
relations; Task source requires a nonempty `agentSessionId`.

Worker results are exactly `{contractVersion,commandId,status:"accepted",identity,runAccepted}` or
`{contractVersion,commandId,status:"interrupt_requested",runId}`. An interrupt request does not establish that
execution stopped.

## Commands and routing

`NodeCommandService` is the command service, described in
[the command contract](NODE-ROUTING.md#node-execution-service); its journal is in
[`PERSISTENCE.md`](PERSISTENCE.md#node-journal). Receipts move
`reserved → dispatching → accepted | rejected | ambiguous`, and a receipt read may also answer `not_found`. It
records `dispatching` before its one Worker call, never performs network I/O inside a journal transaction, and
returns every receipt from durable readback. Continue and cancel use only the accepted predecessor's retained
reservation.

Static registration is `{nodeRef,nodeId,target}`: `nodeRef` is a logical versioned reference, and `nodeId` is
the separately configured physical Node identity, never derived from `nodeRef.ref`. Logical ref
`logical-node-route-1` can name physical Node `physical-node-1`. Start/continue session binding `workerId` and
`executorId` match the target identity and `sessionBindingExecutorId`. Continue requires a durable prior route
with the same Node, target, Task, agent session, and prior Run, and the successor Run must differ. Cancel
resolves its target only from the durable route; the caller's target only corroborates it. A missing route is
`recovery_required`, never a guessed rebinding.

`validateRouterWorkerResponse` is shared by the Node Worker client and command receipt validation. It deeply closes
responses and correlates version, commandId, status, the full Task/Attempt/Run/session binding, agent session,
correlation, operation, and causation to `command.start`. Cancel requires the exact Run. Node additionally
correlates the Node wrapper and Worker identity. Unknown or malformed post-send data becomes
`dispatch_ambiguous`. Router sees a Worker's outcome only through the Node's receipt.

## Worker client

`HttpNodeWorkerClient` takes one deeply frozen Worker configuration and the 1000..120000 ms dispatch timeout.
It verifies the target before transport, sends one bearer-authenticated JSON `POST` to the numeric-loopback
origin at `/v1/worker/commands`, rejects redirects, and never retries. Only `200`/`202` is a candidate success,
parsed by the strict bounded reader and `validateRouterWorkerResponse`. A non-2xx response is classified only
by an exact status/code/message tuple. Every error has fixed safe copy and excludes credentials, URLs,
instructions, response bodies, and native diagnostics.

`readCommand` is a bearer-authenticated `GET /v1/worker/commands/{commandId}` under the same target check,
timeout, and redirect refusal. It accepts only the closed `recorded` or `not_recorded` answer for the same
command ID; the journal validates a recorded response against the command's own expectation before anything
is written. `NodeCommandService` uses it to converge an `ambiguous` receipt, triggered by a refused Run event
from the route owner or by the open-time sweep, and never re-dispatches. Attempts that do not converge back off
using `nodes.eventRetryInitialDelayMs` and `nodes.eventRetryMaxDelayMs`. A command whose Worker
is no longer in the inventory is not asked about again.

## Errors

Only these Node codes, statuses, and safeMessage values exist. No exception, instruction, path, URL, or
provider text enters the error wire.

| Code | Status | Exact safeMessage |
| --- | ---: | --- |
| axis.node.malformed | 400 | The Node request is malformed. |
| axis.node.unauthorized | 401 | Node authorization is required. |
| axis.node.target_forbidden | 403 | The requested Node target is forbidden. |
| axis.node.target_unknown | 404 | The requested Node target is unknown. |
| axis.node.worker_unavailable | 409 | No Worker is available on this Node. |
| axis.node.selection_conflict | 409 | The Node command names more than one executor. |
| axis.node.intent_conflict | 409 | The Node command conflicts with its durable intent. |
| axis.node.binding_conflict | 409 | The Node target conflicts with the durable binding. |
| axis.node.body_too_large | 413 | The Node request body exceeds the accepted limit. |
| axis.node.unsupported_media | 415 | The Node request media type is unsupported. |
| axis.node.recovery_required | 503 | The Node command requires safe recovery. |
| axis.node.draining | 503 | The Node is draining and is not accepting new work. |
| axis.node.dispatch_ambiguous | 503 | The Node dispatch outcome is ambiguous. |
| axis.node.storage_unavailable | 503 | Node durable storage is unavailable. |

Known Worker failures reuse the HTTP tuples in `router-worker.ts`: safeMessage equals the tuple message and
certainty equals disposition, and HTTP also checks the original status. Arbitrary code/message/certainty is
rejected. The `axis.worker` suffixes are malformed, unauthorized, policy_forbidden, method_not_allowed,
body_too_large, unsupported_media, adapter_invalid, launch_failed, launch_ambiguous, intent_conflict,
recovery_required, operation_unsupported, runtime_missing, runtime_incompatible, authentication_required,
probe_timeout, capability_unavailable, workspace_unavailable, and identity_mismatch (definite `409`). Unknown
or malformed failures become `dispatch_ambiguous`.

## Limits, events, and digests

Outer strict UTF-8 JSON accepts exactly 128 KiB. Serialized nested Worker commands, responses, and events each
accept at most 64 KiB; Runtime field limits may be tighter. Invalid UTF-8 and duplicate keys at every depth,
including escaped keys and arrays, are rejected. `validateNodeTransportBody` is a pure preflight: JSON media
with optional UTF-8 charset and no content encoding. Oversized post-send responses are ambiguous; oversized
pre-dispatch requests and events are `body_too_large`.

`validateNodeEvent` runs shared validation before IDs, digest, and journal entry. It validates Runtime
accepted/progressed/succeeded/failed and the complete RunAgentResponseDelivery including Activity.
Linear waiting-for-input uses the bare Axis `run.waiting_for_input` extension at `1.0.0`:
its extension version, state, and elicitation are checked, then its shared
identity and timestamp fields are validated through a Runtime accepted projection. Validated events are frozen
snapshots. Event origin is the Worker's authenticated event token and the accepted command route.

Event ID is the Runtime `messageId`; for a response delivery it is the nested Runtime envelope's `messageId`.
Run ID follows the same envelope selection.

Canonical command intent recursively sorts keys, preserves array order, and replaces `instruction` with
`instructionSha256:"sha256:<hex>"`. The command digest is SHA-256 over `axis.node.command.intent.v1\0`
followed by canonical JSON; the event digest is over `axis.node.event.v1\0` followed by the validated event,
including Activity. `\0` is one NUL byte; both return `sha256:<hex>`.

The outer Node `recordedAt` is a separate observation, never an event field or digest material. Exact replay
keeps the first `recordedAt`; the same ID with a different digest is `intent_conflict` and keeps the original
observation.

## Maintenance

Modes are drain and resume. `blockerCounts` has exactly eight nonnegative integers: reservedCommands,
dispatchingCommands, ambiguousCommands, pendingNodeEvents, quarantinedNodeEvents, activeWorkerRuns,
pendingWorkerOutbox, and unknownWorkerEvidence (which includes unreadable Worker evidence). `idle` is true iff
all are zero.

The status has exactly five keys, because `axis upgrade` validates a running Node's status with the upgrading
CLI's own validator.

| Owner | Responsibility |
| --- | --- |
| Node journal | Reserve, dispatch, and transition commands and routes; record, list, deliver, and quarantine events of accepted routes |
| `NodeCommandService` and `HttpNodeWorkerClient` | Dispatch to the reserved Worker |
| `RouterEventDelivery` | Deliver validated events to `/v1/router/node-run-events` |
| Worker | Execution, native binding, lifecycle, outbox evidence |
| Router | Task/Attempt allocation and target selection |

No transaction spans dispatch or event delivery. Every claim is durable before network activity and
transitions afterward; ambiguous outcomes require explicit recovery.

## Runtime operations

`Supported` means the runtime's Worker registration declares the operation. An unsupported operation fails
closed at the Worker; there is no provider fallback.

| Runtime | Start | Continue | Cancel | Steer | Observe |
| --- | --- | --- | --- | --- | --- |
| Codex | Supported | Supported | Supported | Unsupported | Supported |
| Claude Code | Supported | Supported | Supported | Supported | Supported |

## Operator acceptance

The placeholder configuration and credential directions are in
[`MULTI-HOST-DEPLOYMENT.md`](MULTI-HOST-DEPLOYMENT.md#node-four-service-configuration). `scripts/live-node-smoke.mjs` separates
parser proof from live proof. With no arguments it validates the four-service placeholder bundle through the
Relay/Router, Node, and Worker parsers and makes no network or state-changing call. Its explicitly authorized
live mode is read-only and checks only readiness, authentication, and maintenance projections; it never
starts, continues, or cancels native work.

Real-environment acceptance is manual and opt-in. Use two separately authorized disposable provider
conversations whose routes select different Node target endpoint refs. Do not reuse an AgentSession, Task,
command ID, or Run between the two starts. Keep task instructions private and out of receipts. Collect only
stable IDs as hashes, operation/status values, counts, revisions, and digests; never capture provider output,
prompt text, tokens, local paths, or response bodies.

Immediately before any manual native scenario, require safe operator proof that each selected provider runtime is already authenticated in its service context.
Node bearer authentication and readiness do not prove provider authentication.
Never auto-login. If safe proof is absent, do not run the affected manual native
action and leave that scenario and every dependent scenario `unverified`.

| Scenario | Reproducible operator action | Sufficient safe evidence | Failed / unverified boundary |
| --- | --- | --- | --- |
| Two independent starts | Admit one new disposable start through each approved provider route. | Two different Task/Run/command hashes; two `commands` start rows accepted; two `routes` rows bound to different Worker/endpoint refs; matching Worker command rows; no second launch for either command. | Any shared identity, mismatch, ambiguous command, or duplicate launch is failed. No authorized starts is unverified. |
| Continue | Wait for Worker A's first Run to reach a state its adapter supports for follow-up, then send one new provider input in the same conversation. | One accepted Node `continue` command; the successor route has the same Task, conversation, and Worker and the exact `prior_run_id`; Router and Worker show one successor Run and no replacement session. | Missing predecessor proof, re-routing, or a second primary start is failed. No authorized input is unverified. |
| Cancel | While Worker B has an active Run, use the provider's explicit stop path once. | One Node/Worker `cancel` command for the original Run; `interrupt_requested` is recorded only as a request receipt; a later matching terminal event proves the outcome. | Treating the interrupt acknowledgement as terminal, guessing the target, or retrying an ambiguous cancel is failed. No authorized stop is unverified. |
| Event return | For each tested Run, wait for the Worker and Node delivery pumps to settle. | Worker outbox `delivered_at` is set; the same event hash is Node `delivered`; Router has one correlated receipt and cursor; Relay has the corresponding delivered Activity projection. | A pending, quarantined, or conflicting duplicate event, or a mismatched Task/Run, is failed. Inaccessible provider or database evidence is unverified. |
| Release parity | Run `axis-routing-preflight <role>` on every host with the same expected release version. | Every receipt has empty `blockers` and the same `axisPackageVersion`. | Any blocker or hosts on different releases is failed. Hosts not checked are unverified. |

Inspect databases with read-only connections and aggregate/count queries over the schema in
[`PERSISTENCE.md`](PERSISTENCE.md). Router has no table named "outbox"; its command, Run, and receipt rows are
the Router-side evidence. `not_found` from `/v1/node/command-receipts` proves only journal absence and never
means "the provider did not execute".

The acceptance receipt labels each row `passed`, `failed`, or `unverified` independently and records the
reviewed build and route revision. Parser tests, deterministic Node suites, and the read-only smoke do not
upgrade live provider execution or deployment to `passed`.
