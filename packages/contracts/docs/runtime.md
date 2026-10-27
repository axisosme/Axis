# Runtime contract

`./runtime` exports the current schemas, generated types, validators, pure
admission helpers, and deterministic golden runner. Every envelope and identity
object carrying `contractVersion` requires exactly `1.0.0`.

## Identity and authority

A RuntimeIdentityGraph contains one Task, Attempt, Run, and SessionBinding for
the addressed Run. Runtime identifiers are provider-neutral. Task source is a
closed Linear reference with organization, issue, and optional AgentSession
identity. Waiting and response delivery require a resolvable AgentSession.
Identity validity alone does not authorize routing, execution, recovery, or
credential issuance. Components retain independently trusted evidence.

The contract roles communicate through eighteen closed messages:

```text
Relay -- TaskSubmit --> Router -- RunStart --> Worker -- ExecutorStart --> Codex Adapter
Codex Adapter -- Executor{Accepted,Progressed,Succeeded,Failed,WaitingForInput} --> Worker
Worker -- Run{Accepted,Progressed,Succeeded,Failed,WaitingForInput} --> Router
Router -- Task{Accepted,Progressed,Succeeded,Failed,WaitingForInput} --> Relay
```

These are logical roles. Router-to-Worker transport passes through Node; this
contract does not define that private HTTP topology or executor discovery.
Router defines the identity graph. TaskSubmit carries Task; task-facing returns
project Task and a closed Run projection. Run-facing messages carry Task,
Attempt, and Run; executor-facing messages carry the graph. Task projections
exclude native session references and Worker/executor identities.

Each hop has a new `messageId` and retains the opaque `operationId`. TaskSubmit
is the root without causation. Other messages name a permitted predecessor in
`causationMessageId`; trace checks enforce projections and boundary ordering.
Startup uses `codex.execute`. Optional `{ version, method }` method references
are opaque and enable no discovery.

## Lifecycle and admission

`pending` may become `running`, then `succeeded`, `failed`, or
`waiting_for_input`. Terminal Runs remain terminal. Continuation creates a new
TaskSubmit and RunStart under the same Task and source, with verified
`priorRunId`. Duplicate-submit comparison includes the idempotency key,
operation, full Task/source identity, correlation target, and predecessor.
The pure decision helper reports RunStart eligibility without persisting or
emitting it. [Source and delivery rules](runtime-source-and-delivery.md) define
the atomic Task-scoped continuation evidence callers must supply.

The pre-dispatch terminal exception is Router `task.failed` caused directly by
TaskSubmit with `configured_worker_unavailable`. It carries a failed Run
projection without fabricating Attempt, SessionBinding, Worker, or executor
identity. Separate continue, cancel, approval, reconcile, observe, discovery,
retry, resume, and alternate-executor operations return `unsupported_operation`.
Private component protocols own those capabilities, leases, recovery, policy,
and Worker selection.

## Conformance

`conformance/runtime/traces` contains success, failure, duplicate-submit,
continuation, cancellation, and continue-from-cancelled cases.
`runRuntimeV1Golden` replays them with recording Relay, Router, Worker, and
Codex Adapter doubles through public validators and `decideRuntimeTaskSubmit`.
The exported `goldenRules` cover identity lineage, correlation, legal state
order, exactly one terminal result, start idempotency, and continuation chains.
Identity, lifecycle ordering, and terminal count are checked per Run, including
waiting; fork detection spans the supplied corpus. Duplicate replay emits no
second RunStart or executor start. Mutations exercise rejection paths.

Reports use deterministic ordering and match `expected-report.json` byte for
byte. Report rows contain safe identity, version, rule, and error evidence.
Conformance proves pure contract behavior; provider delivery, durable atomicity,
native execution, restart recovery, and deployment readiness need evidence at
their owning component boundaries.
