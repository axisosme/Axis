# Persistence and recovery

## Database ownership

Axis uses four independent Linear channel databases, the Router database, a Node journal per Node, and a Worker database per Worker:

- The Linear channel owns four SQLite files: `<dataDir>/linear-channel.sqlite` (encrypted webhook inbox, session/startup/decision ledgers, OAuth installation), `<dataDir>/linear-conversations.sqlite` (channel-neutral conversation registry), `<dataDir>/linear-run-chain.sqlite` (channel-neutral run chain), and `<dataDir>/linear-delivery.sqlite` (channel-neutral Agent Activity outbox and waiting-decision ledger). See [Linear channel persistence](#linear-channel-persistence) below.
- `Router` owns Task/Attempt/Run identity, command idempotency, Worker event receipts, and the strictly increasing Relay-safe task cursor.
- `SqliteNodeJournal` owns the standalone `axis-node` process's command dispatch evidence, immutable Run-to-Worker routes, and durable Router event outbox.
- `SqliteWorkerCommandJournal` owns immutable Worker command intent, the selected provider's opaque binding state, safe lifecycle state, and the durable Router-delivery outbox.

Never point multiple active processes at copied databases or restore one journal independently of the others. Each owner enables WAL, foreign keys where applicable, `synchronous=FULL`, and a bounded busy timeout.

## Node journal

`SqliteNodeJournal` is a package-private Node 24 `node:sqlite` owner with schema version `1`. Its exact strict schema is `metadata`, `commands`, `routes` (with the partial unique index `routes_first_source_session` on `(source_system, source_session_id) WHERE prior_run_id IS NULL`), and `event_outbox`, whose `run_id` references `routes`. Opening an empty file creates the complete schema in one immediate transaction. Every later open checks the owned object definitions, schema metadata, foreign keys, database integrity, canonical bytes for every JSON column, digests, command/route relationships, continuation and cancel/steer targets, receipts, and Node events. A journal under any other schema, and unknown, partial, altered, corrupt, or semantically tampered state, fails as `axis.node.storage_unavailable`; the journal never resets or repairs it. A journal the Node refuses is archived with `axis upgrade --force` (see [Operations](MULTI-HOST-DEPLOYMENT.md)), which abandons its in-flight Runs.

`open` is the storage entrypoint `axis-node` startup calls. `recover` is explicit stopped-service crash recovery. `inspectMaintenance` and `inspectAmbiguousCommands` are read-only (`readOnly`, `query_only=ON`), validate the same schema and rows, and never recover or write.

### Operator release records live in the host root, not the journal

Releasing a permanently `ambiguous` command (and the Worker Run it usually left stuck `reserved`
or `observational`) from the `axis upgrade` drain gates does not touch the Node journal or the
Worker database. The journal schema must match the build exactly, so an operator
acknowledgement is one owner-private, append-only JSON record per side, held next to
`axis.host.v1.json` in the same host root: `node-ambiguous-acknowledgements.v1.json` on the Node
host, `worker-stuck-run-acknowledgements.v1.json` on the linked Worker host. Both are read-only
evidence to the drain gates that read them (`assertNodeUpgradeGate`, `assertLifecycleDatabaseDrained`'s
Worker-role release); nothing in either service process reads them. A CLI must validate these
records to release the corresponding drain blocks.

### Commands and routes

The journal takes no SQLite exclusive lock, so a second process can open it; cross-process dispatch safety comes from the dispatch-owner evidence below. A read-only backup of a stopped, checkpointed journal is the supported backup procedure. An owner-internal FIFO serializes every asynchronous journal operation on its single connection, so distinct valid calls cannot overlap transactions; synchronous `close()` fails safely while an operation is queued or running and succeeds after the caller has awaited it.

A command and its start/continue route are claimed in one `BEGIN IMMEDIATE` transaction. Commands and routes each have an independent append order and chained digest plus metadata count, high-water, and tail evidence, all advanced in that transaction; reopen recomputes the chains and rejects sole, middle, tail, or coordinated deletion. Route digests include their creation timestamp. A lifecycle digest binds each command's stored Run and predecessor identities, state, timestamps, reservation, receipt, and dispatch owner, and advances atomically with every transition. Rehydration compares those identities with the canonical intent before resolving a predecessor, detects cycles, and validates retained command/route relationships through one iterative shared cache with work proportional to the retained chain; continuation routes must point to an earlier retained route. A live handle keeps that validated cache across operations; each queued operation compares SQLite `data_version` and discards the cache after another connection commits.

For a new first start, the claim transaction checks existing command intent, maintenance, Run ownership, and first-route ownership before synchronously invoking the inventory resolver. It then stores the validated intent digest, immutable Worker reservation, route, and `reserved` receipt before any Worker call. An exact command/intent retry is revalidated as a closed request and returns the persisted receipt and reservation without selecting again. A different operation or canonical intent under an existing command ID is `axis.node.intent_conflict`; another start for the same provider-scoped source session, or a different command reusing an owned Run, is `axis.node.binding_conflict`, before anything is appended. Continue, cancel, and steer accept only an accepted prior route and its reservation; anything else is `axis.node.recovery_required`. Cancel and steer name the prior Run and mint no route.

Receipt transitions are `reserved → dispatching → accepted | rejected | ambiguous`. Every receipt is canonical and lifecycle-digested; accepted evidence is revalidated against the exact request, authenticated Node, immutable reservation, and prior route before persistence and again on readback. `dispatching` is recorded before the one Worker call and binds the owning process ID and a random journal-handle token. A known definite Worker failure becomes `rejected`; transport uncertainty, malformed response, or identity mismatch becomes `ambiguous`, with no raw response persisted. An `ambiguous` start or continue converges to `accepted` only through a Worker response validated against the rebuilt expectation and a compare-and-set on `state='ambiguous'`. Ordinary `open` never sweeps another handle's in-flight work. `recover` first checks every dispatch owner, then atomically converts each `dispatching` receipt to `ambiguous`; any live owner fails the whole recovery without mutation. The maintenance blocker counts are reserved, dispatching, and ambiguous commands plus pending and quarantined events, and drain refuses new or unresolved start and continue claims.

Routes are append-only recovery authority for the lifetime of one Node journal. A terminal and delivered Run is still eligible to become the predecessor of a later continuation, so neither event delivery nor command completion authorizes route deletion. `nodes.ref` and `nodes.version` identify that durable Node service; they are not package-version fields and must not be rotated while reopening the same database. Retain every historical Worker target the journal requires. To retire a target or Node identity, first drain the complete Relay/Router/Node/Worker set and prove that no continuation, cancel, receipt replay, or pending event can reference the old journal; stop all owners; capture one matched backup; then preserve the old Node database, binaries, and configuration as an immutable recovery set and initialize a different empty `<dataDir>/node-<id>.sqlite` for the replacement identity. Never clear or rewrite the old journal to make new configuration pass.

The command record stores only the canonical intent and digest, which replace instruction text with its SHA-256 digest, plus safe target and route evidence. It does not store endpoint URLs, bearer tokens, provider-native bindings or sessions, local paths, diagnostics, or raw provider output, and no transaction spans a network call.

### Event outbox

Only validated Node events whose Run, Task, provider session, and SessionBinding all match their accepted receipt enter `event_outbox`. Worker owns the safe Router-delivery projection, so Node persists and replays that validated Runtime or AgentActivity envelope unchanged with its canonical event digest. Stable event identity retains the first `recordedAt` on exact replay, while a changed envelope under the same identity conflicts.

Each insert atomically assigns a Run ordinal and global insertion order and stores a digest binding the event identity and canonical event digest to both append coordinates. The same transaction advances metadata containing the total event count, global high-water mark, and a tail digest bound to the final append-position digest; reopen verifies this evidence before accepting the contiguous global and per-Run sequences, so deletion of a predecessor, final event, or sole event fails closed. A lifecycle digest binds each event identity and digest to `recordedAt`, state, retry count and closed failure evidence, delivery/quarantine evidence, and terminal timestamp. Pending reads expose at most one head per Run in global order; a pending or quarantined predecessor blocks only its own Run. `recordDeliveryFailure` increments a durable attempt count and retains only a closed `1.0.0` diagnostic containing one of `axis.node.event_delivery_unavailable` or `axis.node.event_protocol_conflict`, its fixed retryability, and the failure timestamp. Delivered and quarantined rows retain immutable terminal evidence; repeating the same terminal outcome keeps the first timestamp and any Node error code.

The delivery pump treats persisted pending heads as authority, preserves this per-Run ordering, and uses the retained failure timestamp and attempt count to keep restart and repeated-wake retry delay fail-closed.

## Router task identity

Each Router task stores its canonical provider-neutral identity alongside its
Linear identity columns. `readTaskIdentity` and `resolveTaskIdentity` return
`identity` and `linearColumns`; `verifyTaskIdentity` reports `rows`, `linearRows`
and `linearRoundTrips`. Verification checks the canonical source JSON, tenant
and recorded Team scope against the same row's Linear columns. Opening a Router
with a malformed or inconsistent task identity fails without rewriting the row.

## Router-local team bindings

`SqliteTeamBindingStore` opens the chosen Router SQLite file with its own connection and additive `team_binding_*` tables; it does not read or change Router dispatch state. Schema version 1 is created with immutable configured/disabled revision rows, one mandatory audit row per revision, and a current pointer keyed by `{ organization_id, linear_team_id }` only when none of its owned objects exist. A partial, unknown, or altered owned schema is never repaired or rewritten: reopen fails closed without changing it. Revision and audit updates/deletes are blocked by SQLite triggers. Configure/disable append the revision, audit, and pointer update in one `BEGIN IMMEDIATE` transaction with exact compare-and-set revision ownership; revision numbers never reuse or decrease, and a later configure may follow disable.

Configured rows store canonical closed-profile JSON and a `sha256:<hex>` digest. Disabled rows store neither. Revision and audit rows each durably store the contract version, configure/disable action, status, creation time, and matching configured digest (nullable only for disabled), in addition to scope/revision and canonical audit JSON. On every read, the exact SQLite table/trigger definitions, JSON shape, digest, audit, revision, timestamp, and current pointer consistency are revalidated; malformed or drifted persistence fails closed as `persisted_state_conflict` and is never repaired automatically. `ready()` performs `PRAGMA integrity_check`; reopen preserves current and historical records.

## Matched recovery sets

Back up and restore Router core tables, Team bindings, and corresponding Node
and Worker journals as one matched operational set. Restoring or copying only
one owner can invalidate exactly-once evidence and must fail closed.

## Linear channel persistence

The Linear channel uses the four databases named above. Each store owns its schema ledger and exact version. There is no importer for another Relay database layout; replacing binaries does not convert unrelated retained tables into current authority.

`<dataDir>/linear-channel.sqlite` holds three stores at schema version 1 in one file, each with its own schema ledger and table prefix: The inbox and ledger stores, like the channel delivery outbox (`src/relay/channel-outbox.ts`), create their current schema and one ledger row in an empty database and open only a database whose ledger is at that version; an unsupported version is `schema_version_unsupported`, and a malformed ledger is refused as invalid persisted state.

- `linear_installations` / `linear_installation_teams` / `linear_oauth_states` / `linear_installation_audit` (`src/relay/linear/installation.ts`, schema version 1, tracked in `linear_channel_schema`) hold the OAuth installation row, the Team allowlist, PKCE state, and installation audit. `applyPermissionChange` writes `linear_installation_teams` scoped only to `(organization_id, team_id)` -- organization-scoped, not App-User-scoped -- while it writes `linear_installations.can_access_all_public_teams` scoped to `(organization_id, app_user_id, state = 'active')`. After an OAuth reinstall under a new App User, a permission-change event naming the prior App User still edits the Team allowlist (the write has no App-User predicate to fail) but leaves the public-Team flag untouched (its App-User predicate no longer matches); reinstall itself resets the allowlist to the installation's configured default Team set (`install()` deletes all `linear_installation_teams` rows for the organization before reinserting), but does not prevent a later stale-App-User event from drifting it again. Installation revision checks do not change that organization-scoped Team-write behavior.
- `linear_inbox_deliveries` / `linear_inbox_events` (`src/relay/linear/inbox.ts`, schema version 1, tracked in `linear_inbox_schema`) hold the encrypted webhook inbox with lease-based claim/reclaim (`state IN ('accepted','processing','completed','quarantined','failed')`) and its own append-only, immutable audit trail: `linear_inbox_events.event_type IN ('accepted','quarantined','claimed','reclaimed','completed','failed')` with `delivery_id`, `detail_json` (whose individual field values are each truncated to at most 128 characters before serialization, not a whole-column bound), and `recorded_at`. `reclaimed` fires specifically when a claim is granted against a `processing` row whose lease already expired (a crashed or stalled processor's delivery being taken over), so it is a directly alertable crash-recovery signal.
- `linear_ledger_sessions` / `linear_ledger_session_inputs` / `linear_ledger_startups` / `linear_ledger_comment_invocations` / `linear_ledger_decision_outcomes` (`src/relay/linear/ledgers.ts`, schema version 1, tracked in `linear_ledger_schema`) hold session, startup, comment-invocation, and decision state. CHECK constraints require one input per `(agent_session_id, turn_ordinal)` starting at 1; an accepted startup requires command and input while a rejected startup requires neither; an accepted comment invocation requires a command and a rejected invocation requires a safe code; a resolved decision requires a selected option and label while other outcomes forbid both. The append-only `linear_ledger_events` table carries `feedback_ignored` and `retryable` audit signals.

Current audit streams use these locations:

| Table | `event_type` | Identity and metadata |
| --- | --- | --- |
| `linear_inbox_events` | `accepted`, `quarantined`, `claimed`, `reclaimed`, `completed`, `failed` | `sequence`, foreign-keyed `delivery_id`, `detail_json`, `recorded_at` |
| `linear_ledger_events` | `feedback_ignored`, `retryable` | Ledger-owned append-only evidence |

`<dataDir>/linear-conversations.sqlite` and `<dataDir>/linear-run-chain.sqlite` use the channel-neutral `channel_conversations`/`channel_run_chain_*` schemas described under [Channel session stores](#channel-session-stores) (`src/router/channel-conversation.ts`, schema version 1; `src/router/channel-run-chain.ts`, schema version 1), in separate files. `<dataDir>/linear-delivery.sqlite` owns the channel-neutral `channel_message_outbox`/`channel_message_delivery_attempts`/`channel_waiting_decision_ledger` schema (`src/relay/channel-outbox.ts`, schema version 1).

Sensitive payloads and PKCE verifiers are encrypted outside ordinary evidence. SQLite stores credential references, digests, bounded safe metadata, immutable identities, states, and leases.

## Channel backups

Each Linear channel database belongs to its current owner. Back up the Relay
files and vault with the Router, Node, and Worker journals as a matched set
before replacing binaries or restoring state.

## Router task identity

`src/router/task-identity.ts` owns `router_tasks`'s identity columns. A Router task's identity is `(source_system, tenant_id, source_ref_json)`, where `source_system` is `linear`, `tenant_id` is the organization, and `source_ref_json` is canonical JSON of the closed channel source ref `{ system: "linear", issueId, agentSessionId }`. `routing_scope_id` carries the Linear Team ID as the neutral routing scope.

`router_tasks_neutral_identity` is the neutral unique key. `router_tasks_linear_identity` is partial, `WHERE source_system = 'linear'`, over the Linear triple. The table accepts only `source_system = 'linear'`, and a `CHECK` requires the full organization, issue, and AgentSession triple. Every open re-runs the owned-object fingerprint check and decodes every row's identity; `Router#verifyTaskIdentity` re-runs it on demand. Any other shape fails closed as `axis.router.recovery_required`.

## Router target snapshots and Worker recovery

Router freezes one Node target before executable work. The target excludes URLs, credentials, native identities, paths, and instructions. Malformed snapshots are recovery-required.

Router admits the `1.0.0-node` Task target through the
package-private `freezeRouterNodeTaskTarget` helper. The target freezes neutral
provider scope, binding revision/digest, a versioned logical Node reference,
closed scope-and-executor-label Worker selection, and an optional workspace-policy
reference. It contains no actual Worker or executor identity, endpoint
reference/URL, credential, path, or instruction. The
`target_snapshot_json` column stores it.
The helper owns one `BEGIN IMMEDIATE`: it returns an existing Task's stored
target after validation without re-resolving, or resolves and inserts one new
Task row atomically. It writes no Run, command, SessionBinding, receipt, or
execution evidence itself. `Router.startNode` runs the same freeze inside its
own command-claim transaction when a new start has no Task yet. Existing
snapshot bytes are never updated. Two first writers therefore
converge through the neutral identity unique index on the one frozen Task.

Router schema version `1` is recorded in `router_node_schema`. Router creates its schema only when no Router objects exist; a missing ledger or any other version fails closed as `axis.router.recovery_required`. It never repairs a database. Lifecycle drain inspection validates the same schema through a read-only Router seam before counting active work. Router persists an independent `router_node_*` family. A command
claim, pending Run identity, Task current-Run pointer, and canonical redacted
Node command intent are committed before network I/O. `dispatching` and
`receipt_pending` are recovery cutlines: an exact replay queries the receipt and
never redispatches start. Intermediate receipt updates use a checked immediate
transaction and retain the first authenticated reservation; neither a competing
reservation nor a later not-found observation can overwrite it. Accepted
evidence inserts the reservation and full Worker/executor SessionBinding once,
atomically finalizes the command, and classifies buffered events. A definite
rejection instead finalizes the command and quarantines every buffered event in
the same transaction. A matching
authenticated event arriving only after that rejection is directly quarantined
as `axis.node.binding_conflict`; exact replay does not append another conflict.
Competing accepted and rejected observations use checked first-terminal-writer updates;
the losing observation records an immutable receipt conflict without changing
execution or core Run state. Event rows retain canonical envelope and
authenticated-origin digests; only a matching origin can move a buffered row
one way to the existing Router receipt/cursor projection. Missing or conflicting
evidence moves it one way to quarantine. Schema fingerprints, integrity checks,
canonical JSON/digest validation, projected receipt/source/relay correlation,
and update/delete triggers reject partial, relaxed, reversed, or corrupted
state on live exact replay and reopen. An exact replay of a buffered residue
after accepted evidence reclassifies it without Node redispatch. No historical row is
backfilled or reinterpreted, and instruction plaintext is never stored.

The channel-neutral command reconciliation projects the strictly decoded
`router_node_command_state` result: accepted and interrupt-requested
receipts retain those outcomes, while prepared, dispatching, receipt-pending,
and rejected rows remain `unknown`. An unknown ID is `absent`. Interrupted
reconciliation exposes Task/Run lineage only from a validated
`interrupt_requested` result. These reads never query Node, retry a receipt,
advance state, or dispatch a command; malformed retained Node evidence fails
closed as recovery-required.

Router's Node event listener authenticates and validates its closed
wrapper before calling the existing `ingestNodeRunEvent` transaction. A
`202 buffered` acknowledgement transfers durable reconciliation ownership to
Router; a lost response can replay to either `buffered` or `projected` without
adding an event, conflict, or cursor. Quarantined input remains durable but is
reported as `409` and never becomes a Router receipt. Before either path,
Router rechecks the independently authenticated logical
Node against the Task's frozen target. A mismatch, including replay under a
different logical Node credential sharing the same physical Worker, returns
`409` without inserting or changing event, conflict, receipt, or cursor state.
The package-private Node command runtime connects accepted-route authority to the
event pump and Router endpoint. Each attempt rereads the accepted route, sends
its logical Node and reserved Worker identity, and marks delivery only after a
closed correlated `200`/`202` receipt. Retry evidence, per-Run FIFO, quarantine,
and reopen retain the lifecycle digests and transitions. A pending event with
no accepted command route is quarantined.

Node Task snapshots use only `1.0.0-node`. Recovery retains the matched
binary, configuration, and state set; retained rows are never rewritten or
reinterpreted to reconcile incompatible state.

Router persists a command before Worker dispatch and accepts each exact Worker source message once. It can replay durable command results and ordered task projections after a compatible restart. New-start idempotency includes the trusted Team scope; an unconfigured, disabled, stale, incompatible, or endpoint-missing binding stores and replays only a fixed safe rejection before any Task, Attempt, Run, or Worker command is created.

The Worker journal creates its complete schema atomically and records exactly version `1`
with a canonical timestamp in `worker_schema`. Its tables are `worker_commands`, `worker_runs`,
`worker_outbox`, `worker_outbox_quarantine`, `worker_commentary_state`,
`worker_commentary_digests`, `worker_workspace_resolution_audits`, `worker_waiting_runs`,
`worker_run_observations`, and `worker_response_projection_refusals`. The workspace audit
update/delete triggers use the same `worker_` prefix. Writable opens, read-only opens, and
lifecycle inspection validate the schema record and every owned or attached schema object.
Unsupported versions, missing records, partial schemas, altered definitions, and foreign
indexes or triggers attached to Worker tables are refused without conversion; unrelated
SQLite objects are preserved. Journal opens report refusal as `axis.worker.recovery_required`.

`worker_outbox_quarantine` is an immutable sidecar containing sequence,
message ID, Run ID, disposition, and timestamp. `worker_outbox` retains its
six-column shape; quarantine never rewrites or deletes its payload. The two
dispositions are `axis.worker.outbox_unprocessable` (unreadable by this build)
and `axis.worker.outbox_refused` (permanent refusal of its give-up announcement).
Quarantine removes a row from retry but leaves `delivered_at IS NULL`, so
`pendingWorkerOutbox` and drain checks still count it.

`listQuarantineDispositions` and `axis outbox-quarantine list` provide bounded
identity/disposition pagination. `listQuarantinedEvents` joins retained payloads
and omits dispositions whose payload was removed; an unparseable envelope is
represented by a placeholder. Inspect original bytes only through private
read-only access to `worker_outbox`. A code alone cannot distinguish an
unsupported envelope from unreadable bytes. Discharging this debt requires
explicit operator action: rollback does not drain a row whose substitute
terminal already made the Router Run terminal.

`linear_inbox_give_up_dispositions` and
`channel_run_chain_redispatch_dispositions` are append-only records of stopped
retries, each with an autoincrement sequence for bounded exclusive cursors.
Their read-only readers join no source payload and retain evidence even if a
run-chain redispatch row is removed by stop, cancel, supersession, or rejection.
Neither table has a payload column, so their listings cannot return provider
content. Both disposition readers validate their store's complete current schema and
single version `1` record before each read; unsupported or record-less storage is refused.

The inbox disposition also carries `acknowledged_at`, `acknowledged_by`,
`acknowledged_authority`, `acknowledged_source`, `acknowledged_request`, and
optional `acknowledged_reason`. `axis linear-inbox-give-ups acknowledge` writes
that acknowledgement once. CHECK constraints enforce all-or-nothing fields;
triggers prevent rewriting either the give-up or its acknowledgement.
`assertLifecycleDatabaseDrained` narrows its give-up clause by sub-select only
when the disposition table exists. Missing disposition evidence keeps the drain
blocked. Records are never backfilled with invented observation timestamps.
Redispatch give-ups do not block drain and have no acknowledgement fields.

Worker resolves the frozen reference before reserving a Run or starting an executor. `worker_workspace_resolution_audits` stores one immutable, monotonically sequenced, path-free resolution record per Worker command: command/task/run/organization/node identities, the opaque reference or `null`, outcome, reason, and timestamp. Update/delete triggers protect the rows. `SqliteWorkerCommandJournal.listWorkspaceResolutionAudits` and `axis workspace-audit list` provide bounded exclusive-cursor pagination without returning configured or canonical paths. Catalog match, catalog miss, mapped-path failure, organization-root fallback, and root unavailability are therefore restart-auditable without persisting host layout. Audit persistence failure enters recovery-required state before executor launch.

After resolution, Worker reserves a Run before starting an executor and persists every safe lifecycle envelope before attempting Router delivery. A Router outage or lost response redelivers the same immutable message ID without relaunching an executor. A Worker restart can replay stored projections but cannot revive an active native binding. Unknown, partial, drifted, or ambiguous native state is read-only and fails closed; unprovable launch outcomes are observational and never enter a retry queue.

`worker_response_projection_refusals` records a rejected final projection's
Run, source event, and contract diagnostics (`code`, `instancePath`, `schemaId`,
`keyword`, `safeMessage`). First write wins per Run. Diagnostics contain only
rule names, JSON pointers, and fixed safe messages, never matched answer text.
A response-bearing delivery uses the contract fallback with
`projectionStatus: "fallback"` when projection is refused; a plain success
envelope cannot carry that body. The refusal stays private and is not a Linear
projection. The table is created on read-write open; read-only inspection of a
journal without it reports no refusal.

Claude Code retains its native local session files for continuation; Axis neither parses nor copies them. These files are separate from Worker journal persistence. Removing them makes the session unavailable for resume; a session without a retained native transcript cannot be restored. Recovering a Claude Run that was still active when its Worker restarted remains unsupported; continuing a terminal one is covered below.

The Worker-private `<dataDir>/worker-<id>-native-sessions.json` file is derived
from TOML without an operator path override. It maps each Claude `sessionId`
that became continuable -- terminal, with its result read and terminal delivered
to Worker -- to `{ "workingDirectory": "<absolute path>" }`, the session's original
directory. The executor writes best-effort whole-file snapshots through a temporary file renamed into place with mode `0600`, merging current contents so overlapping Worker instances preserve each other's entries. Absent, unreadable, or corrupt files read as empty, so continuations fail closed without failing startup. Entries are never removed. Losing the file makes earlier sessions unresumable after the next restart (`axis.worker.continuation_unresumable`); it never makes a session resume in the wrong place.

The Claude Worker uses the same journal schema without storing transcripts, result text, prompts, authentication material, executable/workspace paths, or native JSONL. Its opaque binding is exactly `{ runtimeKind: "claude-code", sessionId }`; it becomes durable only after matching `system/init` identity. Definite pre-side-effect startup failures produce no accepted lifecycle. Any post-spawn uncertainty is marked observational, and the permanent process-local request guard plus durable Run reservation prevent a second launch.

A terminal predecessor may normally continue only while the same Worker process retains the exact binding. After restart, Claude Code and Linear-enabled Codex provide a `reattestTerminal` proof before admitting a successor. Claude attests only when its private session record exists and resumes only in the recorded working directory. The proof does not query the CLI or prove the native transcript exists; a missing transcript is a definite `executor.launch_failed` during resume. Linear-enabled Codex requires its credential-free app-server's non-archived `thread/list` view, paged to the end, to list the binding's thread. It also supports a predecessor parked on `waiting_for_input`, the state only Codex produces. This proof never starts, resumes, or unarchives a thread. The plain Codex executor declines restart attestation because it cannot resume a thread into a fresh app-server.

A proof the adapter positively declines returns `axis.worker.continuation_unresumable` before successor admission; a missing, malformed, or thrown proof returns `axis.worker.capability_unavailable`. Active native runs remain unrecoverable across restart. Cancel requests interrupt only the active successor and are never terminal by themselves. The `worker_waiting_runs` table and Router waiting marker preserve audit linkage.

## Relay replay and ambiguity

`Linear-Delivery` is the transport identity. Exact raw replay is accepted; a fresh authenticated retry may change only top-level `webhookTimestamp` when stored event, subscription, organization, and complete canonical JSON semantics are unchanged. A stable logical AgentSession-event key prevents a distinct transport delivery from issuing the same directive twice.

For created events, the startup-intent digest excludes the transport timestamp and webhook presentation fields while binding trusted Session identities and the captured source-comment provenance when present. The executable input itself is derived only after exact Issue and, for comment-origin starts, exact Comment reads succeed. Exhausted bounded input reads retain their processing lease for later reclaim without input, command, Activity, or dispatch; after acceptance, exact semantic retry returns the original outcome without duplication. comment rejection replay remains governed by `comment_invocations`, and prompted replay remains governed by its existing Activity/logical-command identities; neither path is converted into historical startup-ledger data.

Activity UUID/content are persisted before provider delivery. Transport loss makes the row ambiguous and never returns it to pending. Only an exact immutable provider match confirms delivery; conflict or inability to prove outcome remains operator-visible.

## Supported persistence owners

Relay, Router, Node, and Worker each own their journal schema, ledger and
recovery rules. Startup validates the current objects owned by each configured
store.

## Channel session stores

`src/router/channel-conversation.ts` (schema version 1, `channel_conversation*`) and `src/router/channel-run-chain.ts` (schema version 1, `channel_run_chain_*`) are channel-neutral stores used by Linear in separate physical files. A store creates its current schema and one ledger row in an empty database and opens only a database whose ledger is at that version. A database without a ledger is `persisted_state_conflict`; an unsupported version is `schema_version_unsupported`, and a malformed ledger is refused as invalid persisted state. Every public method re-checks the owned objects against the exact `sqlite_master` definitions.

The conversation store keeps conversations, the current pointer per scope, immutable messages and deliveries, and an immutable Worker-routing admission written in the transaction that opens a conversation. Linear uses the AgentSession identity to find the retained conversation; continuation retains its frozen routing admission and Run chain.

The run chain keeps reservations, Runs, decisions, the current Runtime contract version per conversation, redispatch markers, and append-only redispatch dispositions. A start reservation may retain one immutable keyed Worker selection as recovery authority: an unsettled replay uses the retained choice and never consults current labels. Reservation status `steered` records a follow-up handed to the running Run as a native steer (`router.nativeSteer`); it carries a Run and a Task and no safe code, writes no redispatch marker or Run row, and releases rather than opens a waiting decision. A redispatch marker carries `attempt_count`, `next_attempt_at` and `quarantined_at`, with `CHECK(quarantined_at IS NULL OR attempt_count > 0)`. A given-up redispatch is a `quarantined_at` stamp with no failure code and is not part of the drain check (`SELECT * FROM channel_run_chain_redispatch WHERE quarantined_at IS NOT NULL`).

A Linear inbox give-up (`state = 'quarantined'`, `failure_code = 'axis.retry.attempts_exhausted'`) blocks `assertLifecycleDatabaseDrained` until its retained disposition is explicitly acknowledged. Acknowledgement releases only the gate and never clears or retries the source row.

Other decision resolution reuses the existing Linear ledger and encrypted input tables: selected option ID is the decision's Other ID, selected label is the constant `Other`, and free-form text is never a plaintext audit column. Decision replies use the current schema.

## Ephemeral Linear execution credentials

The optional [execution access owner](LINEAR-EXECUTION-ACCESS.md) keeps only bearer
hashes and safe bindings in memory; restart invalidates all issued credentials.
No database schema changes. `LinearInstallationStore.executionAuthority` reads
permissions and the workspace's latest installation-audit ID in one snapshot.
That existing append-only ID invalidates grants across reinstall and any
permission change, even at an identical clock instant. Preserve the installation
audit while grants are in use; do not prune or restore it independently.
