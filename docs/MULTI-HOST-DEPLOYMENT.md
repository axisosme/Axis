# Multi-host advanced deployment

Use this guide when Relay/Router and one or more Node/Worker sets run as
separate processes. Start with [single-host deployment](SINGLE-HOST-DEPLOYMENT.md)
for one machine and one executor. All roles use the same [TOML configuration
model](CONFIGURATION.md) and reviewed release; each role owns its own journals
and credentials.

## Production topology

For multiple processes or hosts, run one `axis-linear-relay` (it hosts Router), one or more `axis-node` processes, and the `axis-codex-worker` or `axis-claude-worker` processes each Node's inventory names, all from the same reviewed release. Relay hosts the public Linear listener plus Router's only private route, the Node event door (`POST /v1/router/node-run-events`); each Node hosts a command listener and a loopback event listener; each Worker hosts a private command listener and sends its events to its Node. Router has no direct route to a Worker and never chooses a Worker by provider, fallback, or discovery: a Task freezes its Node target, and the Node selects the Worker.

Every built-in service reads the same versioned TOML model. `--config <path>`
selects the file; omission reads `axis.toml` in the current working directory.
`axis-linear-relay` takes only `--config`. `axis-node` also accepts `--node <id>`;
`axis-codex-worker` and `axis-claude-worker` accept `--node <id>` and `--worker <id>`.
Selectors override `[host].node` and `[host].worker`; a sole Node or a sole
Worker on the selected Node selects itself. Multiple candidates require a
selection. Duplicate, unknown or missing-value flags fail startup. Each Worker
binary requires its selected declaration's `kind` to match that binary.
See [Configuration](CONFIGURATION.md) and [the complete example](../axis.example.toml).

A Worker's nonempty `nodes.workers[].workspaces` array has `organizationId`, a
fallback `workspaceRoot`, and an optional `workspaceCatalog`; it has no `system`
discriminator. Catalog entries pair an opaque immutable `{ ref, version }` with
a local `workspaceRoot`. Paths resolve relative to the TOML file before runtime
validation. Duplicate organization IDs, catalog references and unknown keys
are rejected at startup. The same reference may resolve to different paths on
different Worker hosts.

Only the frozen Team-selected workspace reference can select a catalog entry. Linear payloads, prompts, provider output, and executor bindings cannot supply a path or override the selection. Before every start or continuation, Worker canonicalizes the selected directory, verifies that it is a readable, writable, searchable directory, and records a path-free audit. A missing catalog entry or unusable mapped directory falls back to that entry's own root. If the root is also unavailable, Worker returns `503 axis.worker.workspace_unavailable` before an executor side effect. No reference preserves the existing organization-root behavior.

`axis-claude-worker` uses `nodes.workers[].linearExecution` like the Codex
Worker, validated before its journal opens; see
[Linear CLI and SDK execution access](LINEAR-EXECUTION-CLIENTS.md). Native
configuration and transcripts use the service account's normal `HOME`, so
continuations and later sessions use its CLAUDE.md, skills, plugins, hooks, MCP
servers and authentication. Set `nodes.workers[].claudeExecutable` to the
reviewed executable path. The process canonicalizes it once and requires
permission-mode help capabilities and authentication before opening the listener.
Every distinct start or continuation repeats the bounded help/authentication
gate immediately before its single native spawn; native version strings are
not admission inputs. Authentication stdout is discarded; only exit status is
used. Treat capability or authentication failures as deployment repair gates;
do not add a fallback executable or provider.

The Claude session store is derived as
`<dataDir>/worker-<id>-native-sessions.json`. This Worker-private mode-`0600`
file records each continuable session's ID and working directory. Back it up,
restore it and move it with the Worker journal; do not share it between active
Workers. It holds no transcript. A missing session record refuses restart
continuation as unresumable, and a missing native transcript refuses resume
instead of starting a replacement. See [Persistence](PERSISTENCE.md).

The native Claude session runs with the CLI's full default toolset and `--permission-mode bypassPermissions`, so every tool call runs without an Axis prompt, including Bash, network access, and file writes inside or outside the workspace. Axis passes no `--safe-mode` and forwards the native service environment after removing inherited deployment and Linear values and injecting the current Run's Linear values, so the account's normal CLAUDE.md, skills, plugins, hooks, MCP servers, tools and credentials are available. Axis adds no OS-level sandbox. The native CLI may still enforce provider-owned restrictions: it refuses `bypassPermissions` as root or under `sudo`, and managed settings can disable it. A native permission denial or refusal remains a failed Run rather than being hidden. The Worker account's complete filesystem, credential, environment and network reach is the intentional blast radius; see `docs/SECURITY-MODEL.md` for the exact boundary.

Relay requires `[linear]` with `clientId` and `redirectUri`, plus the external
secrets `LINEAR_CLIENT_SECRET` and `LINEAR_WEBHOOK_SECRET` in its process
environment. Other operator settings come from TOML; generated role credentials
come from `<dataDir>/secrets`, including `relay-master.key` and
`relay-operator.key`. Keep TOML owner-private and provision the keys before
starting services. `axis init --config /opt/example/axis/axis.toml` provisions
the declared topology once; it never rotates or repairs existing keys.

The Linear channel's four databases are separate derived files:

| File under `dataDir` | Holds |
| --- | --- |
| `linear-channel.sqlite` | Encrypted webhook inbox, session/startup/decision ledgers, OAuth installation, Team allowlist, PKCE state and installation audit. Three stores use separate table prefixes. |
| `linear-conversations.sqlite` | Channel-neutral conversation registry for Linear AgentSessions. |
| `linear-run-chain.sqlite` | Channel-neutral run chain: reservations, Run state and redispatch markers. |
| `linear-delivery.sqlite` | Channel-neutral Agent Activity outbox and waiting-decision ledger. |

The controller lifecycle backs up, restores and drains these four files and
`router.sqlite` together. It blocks while any of `router_runs`
(`pending`/`running`), `worker_runs` (not terminal), `worker_outbox`
(undelivered), `channel_message_outbox` (`pending`/`delivering`), or
`linear_inbox_deliveries` (`accepted`/`processing`) holds work; a database without
one of those tables contributes nothing to that clause.

### Router endpoint transport configuration

Each `[[nodes]]` declares physical Node identity (`id`, `ref`, `version`) and
its command origin (`url`, otherwise derived from `host` and `port`). Its nested
`[[nodes.workers]]` declarations derive logical targets, Node inventory,
Worker identities and the Router's allowed event origins from one topology.
`endpointRef`/`endpointVersion` identify each logical target;
`executorId`, `kind` and `runtimeProfile` define execution identity.
`router.defaultWorker` optionally names a global Worker ID whose endpoint is
the Router fallback. Exact Node references and Worker endpoint references must
be unique. Router persists only the opaque target snapshot, never a URL or key.

Node command and event keys and each Worker's command and event keys are
separate generated files. Keep them out of TOML, shell history, logs and Router
SQLite. See [the key ownership table](CONFIGURATION.md#data-directory-and-secrets).

Never leave a Node replaying against a Router from a different snapshot, and uncertainty is never permission to issue a second start.

### Standalone Node process

Start `axis-node --config <path> --node <id>` separately from Workers and
Relay/Router. It derives `<dataDir>/node-<id>.sqlite`, command/event listeners,
Worker inventory, and the Router event destination from the selected declaration.
The command and event ports must differ. `nodes.eventHost` and Worker `host`
values require numeric loopback; a Node command listener can use a protected
private-network address. `router.url` is the outbound event origin.

`nodes.workers` may be empty. `nodes.defaultWorker` names a local Worker **key**:
it is required with two or more Workers, defaults to the sole Worker's key, and
must be omitted with no Workers. `nodes.workers[].labels` maps the fixed labels
`claude` and `codex` to one local Worker per label. Source labels use bare names
or the `WORKER:` group spelling (`WORKER:CODEX`, `WORKER:CLAUDE`).
`nodes.workerScopeRules` has `{ scope = { system = "linear", tenantId, scopeId },
workerKey }` Team rules. Selection runs once for each new session: executor
label, then scope rule, then default. Every rule names a configured key.
See [Worker selection](NODE-ROUTING.md#worker-selection).

Node timing bounds and defaults are in [Configuration](CONFIGURATION.md#schema-reference).
`nodes.workers[].hostRoot` provides local ownership metadata for maintenance;
it does not make Node supervise Workers. All settings are read once at startup.
For managed services, changes require fresh installation roots as described below.

`nodes.ref` and `nodes.version` identify the durable Node service, independently
of the installed package version, and must remain unchanged when reopening its
journal. Targets remain configured for that journal's entire
lifetime because a terminal Run can still authorize a later continuation; Node
does not remap a route to a newer endpoint. Removing a session's Worker, or
every Worker, does not stop the Node from starting: that session's
follow-ups are refused as `worker_unavailable` before anything is recorded or
sent, and the user is told the Worker is unavailable. When at least one retained
session names a Worker the inventory does not admit, `axis-node` writes one line
to stderr after `Axis Node ready`:

```json
{"code":"axis.node.sessions_without_worker","count":N}
```

It carries only the count and is absent when every retained session's Worker is
configured. `axis-routing-preflight node` reports the same count as
`retainedSessionsWithoutWorker`, which never blocks. A Worker deployed again is
picked up only when the Node restarts, because `nodes.workers` is read
once at startup: after deploying, restoring, or reconfiguring a Worker, restart
its Node. The same holds when a follow-up is refused `worker_unavailable`: fix the
Worker, restart the Node, and have the user send the prompt again.

The Node opens only a journal under its own exact schema. A journal it refuses
(`axis.node.storage_unavailable` at startup, `node_journal_requires_archive` in
`axis-routing-preflight node`) is archived by `axis upgrade --force`, which
abandons that journal's in-flight Runs.

Retirement uses journal rollover, never in-place pruning. Drain the complete
Relay/Router/Node/Worker set and prove no continuation, cancel, receipt replay,
or pending event can reference the old Node journal. Stop every owner and take
one matched backup, preserving the old Node database, binary, configuration,
and identity together. The replacement then uses a different empty
`node-<id>.sqlite` under its configured `dataDir` and may use a new Node identity or reduced Worker
inventory. If that proof is unavailable, retain the old identity and targets
or keep the matched old service set isolated; do not clear the journal or
silently retarget its routes.

```sh
node dist/bin/node.js --config /opt/example/axis/axis.toml --node node-placeholder
# prints: Axis Node ready
```

Only that ready line proves configuration, journal opening, event
pump initialization, and both listeners succeeded. Before admitting traffic,
authenticate `GET /v1/node/readiness` on the command listener and confirm the
expected exact Worker IDs. Stop with `SIGTERM` or `SIGINT`: Node gates both
listeners and gives admitted work a bounded graceful-close interval. If that
interval expires it force-closes inbound sockets, but it keeps the journal open
until every already-admitted handler has settled under its own bounded
downstream timeout; only then does shutdown return its fixed failure. Node then
stops event delivery and closes its own journal. It does not stop or signal any
Worker/native process. Fixed
`axis.node.startup_failed` or `axis.node.shutdown_failed` output contains no
credential, endpoint, database path, prompt, or native diagnostic. Retry a
failed startup only after correcting the reported deployment condition; startup
cleanup releases partial listener and journal ownership.

### Node four-service configuration

The following TOML is the exact parser-backed `placeholderConfiguration` from
`scripts/live-node-smoke.mjs`, exercised by `test/node/live-node-smoke.test.ts`.
One shared file supplies Controller/Router, Node and both Worker roles.
`REPLACE_WITH_` values, `.invalid` hosts and `/opt/example` paths are fictional;
replace them before provisioning. Secrets are not part of this file.

```toml
schemaVersion = 1
dataDir = "/opt/example/axis/data"

[relay]
port = 4098
url = "https://controller.example.invalid/"

[router]
port = 4099
defaultWorker = "worker-a"

[linear]
clientId = "REPLACE_WITH_LINEAR_CLIENT_ID"
redirectUri = "https://controller.example.invalid/oauth/linear/callback"

[[nodes]]
id = "node-placeholder"
version = "command"
port = 4100
eventPort = 4102
url = "https://node.example.invalid/"
defaultWorker = "worker-a"

[[nodes.workers]]
id = "worker-a"
kind = "codex"
executorId = "codex-a"
port = 4101
hostRoot = "/opt/example/axis/hosts/worker-a"
profileFallbackPolicy = "deny"
workspaces = [{ organizationId = "REPLACE_WITH_LINEAR_ORGANIZATION_ID", workspaceRoot = "/opt/example/workspace-a" }]

[[nodes.workers]]
id = "worker-b"
kind = "claude-code"
executorId = "claude-b"
port = 4111
hostRoot = "/opt/example/axis/hosts/worker-b"
claudeExecutable = "/opt/example/bin/claude"
workspaces = [{ organizationId = "REPLACE_WITH_LINEAR_ORGANIZATION_ID", workspaceRoot = "/opt/example/workspace-b" }]
```

Keep the service owners separate even on one Mac. Each has a distinct host root,
label and derived databases; the TOML file and data directory can be shared:

| Service | Host root | Service label | Owned files under `dataDir` |
| --- | --- | --- | --- |
| Controller/Router | `/opt/example/axis/hosts/controller` | `com.example.axis-controller` | Four `linear-*.sqlite` files, `router.sqlite`, `relay-vault.json` |
| Node | `/opt/example/axis/hosts/node` | `com.example.axis-node` | `node-node-placeholder.sqlite` |
| Worker A | `/opt/example/axis/hosts/worker-a` | `com.example.axis-worker-a` | `worker-worker-a.sqlite` |
| Worker B | `/opt/example/axis/hosts/worker-b` | `com.example.axis-worker-b` | `worker-worker-b.sqlite`, `worker-worker-b-native-sessions.json` |

Provision the shared transport keys once with `axis init --config <path>` after
finalizing the declaration, and supply Relay's external Linear secrets through
its service context. For separate hosts, securely provision matching keys to
their authorized readers and keep databases local; do not independently generate
the same logical transport's keys on both sides.

| Flow | Shared file under `dataDir/secrets` | Readers |
| --- | --- | --- |
| Router to Node commands/readiness | `node-node-placeholder-command.key` | Controller and Node |
| Node to Worker A/B commands | `worker-worker-a-command.key`, `worker-worker-b-command.key` | Node and the matching Worker |
| Worker A/B to Node events | `worker-worker-a-events.key`, `worker-worker-b-events.key` | Matching Worker and Node |
| Node to Router events | `node-node-placeholder-router-events.key` | Node and Controller |

These are six distinct transport authorities. OAuth, webhook, vault and operator
credentials are separate. Node's command listener can be reachable through the
protected private network; Node event and Worker listeners remain numeric loopback.

Standalone commands select from the same file:

```sh
axis-codex-worker --config /opt/example/axis/axis.toml --node node-placeholder --worker worker-a
axis-claude-worker --config /opt/example/axis/axis.toml --node node-placeholder --worker worker-b
axis-node --config /opt/example/axis/axis.toml --node node-placeholder
axis-linear-relay --config /opt/example/axis/axis.toml
```

Run each in its own process. Host installation is a separate operation. Use the
matching host root and label from the table with `axis install --role controller`,
`--role node --node node-placeholder`, or
`--role worker --node node-placeholder --worker worker-a` (or `worker-b`), plus
`--config /opt/example/axis/axis.toml`. Worker runtime is derived from `kind`.
A Node never owns, starts, stops or upgrades its Workers.

#### Native steer

With `router.nativeSteer = true`, Router sends a follow-up on a
still-running Run to that Run as a soft steer instead of interrupting it. The
value is a TOML boolean, defaulting to `false`; other types fail parsing. The choice is fixed per command when Router first records it, so
flipping the switch never changes how a recorded follow-up is re-read. A steer
needs every Worker, Node, and Router on the same release.

#### Maintenance and provider authentication

For routine Node maintenance, drain before touching the Node
and proceed only when mode is `drain`, `idle` is true, and every blocker count is
zero:

```sh
axis node drain --host-root /ABSOLUTE/PATH/TO/node --output json
```

While draining, a Node still takes a cancel and a steer -- both act on a Run
that is already executing -- and refuses a new start or continue with
`axis.node.draining`, answered before the Node records anything.
Router reads it as a definite refusal. A refused prompt is answered at once
("paused for an upgrade or maintenance ... send the prompt again in a few
minutes" on Linear), Router retains
nothing for it, and the conversation's slot is free, so the same prompt
succeeds after the resume. A redispatch an interrupt queued (a follow-up on a
running Run with `router.nativeSteer` false, or a Worker's
`interrupt_requested` fallback, whose Run then ends while the Node drains) is
kept: each refusal while draining spends no attempt and holds the next one a
minute out, and the observe tick sends it within a minute of the resume. A new session
started while draining is told nothing ran; the user creates a new Linear
Agent session later.

A command the Node had already claimed (`reserved` or `dispatching`) when the
drain began and that is replayed into it is still answered with the ambiguous
`axis.node.recovery_required` and holds its conversation. Keep drain windows
short and resume promptly.

**A refused upgrade leaves the Node drained, not stopped.** `axis node drain|status`'s
human output adds a `note:` line whenever `blockerCounts.ambiguousCommands` is nonzero; the JSON
output and exit code are unchanged. A permanently `ambiguous` command -- its Worker reply was
lost, and Axis never resolves it on its own -- and the Worker Run it usually left stuck `reserved`
or `observational` are released for upgrade, per command, with:

```sh
axis node ambiguous-commands list --host-root /ABSOLUTE/PATH/TO/node --output json
axis node ambiguous-commands acknowledge --host-root /ABSOLUTE/PATH/TO/node \
  --command <command-id> --operator <operator-id> --authority <authority-ref> \
  --audit-source <source> --request <request-id> [--reason <text>]
```

One acknowledgement also releases the linked Worker's own `axis upgrade` -- the Worker Run it left
stuck blocks that Worker host's drain too (`worker_runs`), and the same operator record clears
both gates. `axis node drain`'s raw counts are never reduced by an acknowledgement; only the
`note:` line, and the two upgrade gates themselves, say whether a count is released. The command
and the Run are kept as evidence; nothing is re-sent or re-executed.

Node bearer authentication and readiness do not prove provider authentication.
Before any separately authorized real provider execution, require safe operator
proof that every selected provider runtime is already authenticated in its
service context. Never auto-login. If safe proof is absent, do not run the
affected manual native action and keep those scenarios `unverified`; otherwise
follow the verification procedure in [NODE-MVP.md](NODE-MVP.md) and retain safe
results in private operator records.

#### Safe smoke receipt

The default command validates only the checked-in placeholder bundle with the
real parsers. It performs no fetch, native/provider task, file write, route
mutation, or maintenance mutation:

```sh
node --import=tsx scripts/live-node-smoke.mjs
```

For a separately authorized deployed-host read, load the real Node token into
`AXIS_NODE_LIVE_TOKEN` from secret storage without putting it in argv, use
`--live-read-only` as the explicit opt-in, and supply both exact listener origins,
the durable Node ref/version, and the reviewed 40-hex commit:

```sh
node --import=tsx scripts/live-node-smoke.mjs --live-read-only \
  --command-origin https://node.example.invalid \
  --event-origin http://127.0.0.1:4102 \
  --node-ref node-placeholder \
  --node-version command \
  --reviewed-commit 0000000000000000000000000000000000000000
```

This mode makes exactly three bounded GETs: unauthenticated and authenticated
Node readiness, then authenticated maintenance status. It does not send a
command, post maintenance state, touch a provider, or write a receipt file.
Output is one sanitized JSON receipt. A `passed` check has validated evidence;
`failed` means the fixed check contradicted or could not read its evidence;
`unverified` means no safe proof was collected. The supplied commit remains
`unverified` until the operator independently attests the running build.

Optional Relay settings are `relay.host` (loopback by default), `relay.port`
(8787), `linear.teamIds` (an array, empty by default), and
`linear.allowAllPublicTeams` (`false` by default). Each background loop takes a
positive integer interval: `linear.ingressTickIntervalMs`,
`linear.observeTickIntervalMs` and `linear.outboxTickIntervalMs`, all defaulting
to 1000 ms. `linear.integrityCheckIntervalMs` (60000 ms by default, positive
integer) sets the minimum gap between full store-integrity checks on the observe
tick. Integrity cadence is independent of loop intervals. Raising it widens the
data-damage detection window to `max(integrity interval, observe interval)` plus
one tick's duration; every store still checks schema damage on every call.
`router.defaultWorker` selects a declared Worker's endpoint as the explicit
fallback for otherwise-unconfigured Teams; `router.defaultExcludedTeamIds` is
an array of Teams that must never use it. Durable Team bindings take precedence,
and each new Task freezes the selected fallback profile. `router.nativeSteer`
is a boolean (`false` by default); see "Native steer" above.

Team bindings select a configured endpoint. Scheduling a second agent or a review is the caller's orchestration responsibility and reaches Axis as an ordinary start.


Keep every token and key in deployment-owned secret storage. Bind Router and Worker listeners only to trusted network paths and never print configuration values.

## Startup and validation

```sh
npm ci
npm run check
npm run build
axis-codex-worker --config /opt/example/axis/axis.toml --node node-placeholder --worker worker-a
# In a separate process, after setting worker-b's claudeExecutable in TOML:
axis-claude-worker --config /opt/example/axis/axis.toml --node node-placeholder --worker worker-b
axis-node --config /opt/example/axis/axis.toml --node node-placeholder
axis-linear-relay --config /opt/example/axis/axis.toml
```

Probe `/health/live` and `/health/ready` on Relay. Confirm both private listeners reject unauthenticated requests and Router/Worker database paths are distinct. `/health/ready` is deliberately **not** an integrity check: it runs no `PRAGMA integrity_check` and no `PRAGMA foreign_key_check`, so an unauthenticated route cannot be made to read whole databases and its cost stays independent of how much data they hold. Confirm `PRAGMA integrity_check` is `ok` for each journal as a separate step: for the Linear channel's stores probe the operator-bearer-gated `GET /health/diagnostics`, which runs the full per-store check -- both pragmas, the schema fingerprint sweep and the cross-table consistency queries -- and reports each store's verdict, so a process that answers `ready` can still report `degraded` there. `npm run test:bins` starts the built Relay plus both Worker bins against fake local runtimes, verifies Claude authentication rejection is fixed/redacted, and shuts them down with `SIGTERM`; `npm run test:smoke` performs a non-interactive local Relay probe.

`linear_ledger_startups` (`<dataDir>/linear-channel.sqlite`) is written in the created-event ingress transaction. An accepted created event must show one linked `linear_ledger_sessions`/`linear_ledger_session_inputs`/startup-row/inbox-outbox set (`disposition = 'accepted'` with a non-null `command_id` and `input_source_event_id`). A definite non-comment validation rejection must show one minimal `disposition = 'rejected'` row with a `failure_stage`/`failure_reason` and no linked command or input.


The real Claude gate is deliberately separate and authenticated. Select the
canonical executable with `--executable` and opt in with `--live`:

```sh
npm run test:claude-live -- --live --executable /opt/example/bin/claude
```

It creates temporary local Worker/Router state, submits one harmless text-only instruction, and checks fixed redacted evidence. It skips unless explicitly opted in. Never run it from an unaudited CI credential context.

For read-only Linear development verification, provide `LINEAR_DEV_ACCESS_TOKEN` and `LINEAR_DEV_AGENT_SESSION_ID`, then run `npm run test:linear-live`. Never print either value.

## Host-local install and upgrade

Use `axis install --role controller|node|worker --config <path>` once per
service. `--config` defaults to `axis.toml` in the current directory. Nodes
accept `--node <id>`; Workers accept `--node <id> --worker <id>`. Selectors
otherwise use `[host]` defaults or a sole candidate. The controller rejects
Node/Worker selectors; a Node rejects a Worker selector. Worker runtime is
derived from the selected Worker's `kind`. Non-interactive installation
requires `--role`; a TTY can prompt for it.

For services sharing a Mac, use a distinct `--host-root`, `--service-label`,
and derived database per service. They may share a finalized TOML file and
`dataDir`. Node defaults to `com.axis.axis-node`; its package-local runner
selects `axis-node`. `--source-package <absolute path>` selects a built package
directory or archive; `--output human|json` selects output. Reusing a host root
with a different installation identity or an existing service label is refused
before staging. The default host root is `~/.axis/host`, so explicitly select
one for each additional service.

Managed state is strict `axis.host.v1` in `<host-root>/axis.host.v1.json`. Its
`configuration` is exactly `{ filename, digest, nodeId, workerId }`: a canonical
absolute TOML filename, SHA-256 of its exact bytes, and pinned selected IDs (null
where inapplicable). The file must be regular, owned by the current user,
owner-private, and accessed without symlinks, including symlinked parent paths.
The runner verifies the hash before launching the role with explicit
`--config`, `--node` and `--worker` arguments. It inherits the native service
environment for external secrets and native process settings. TOML content and
key material never enter the plist, public receipt or lifecycle diagnostics.

The TOML file is immutable for that installation's lifetime. Even whitespace
changes invalidate its hash. Restore the pinned bytes or stop the services,
take a matched backup and perform fresh installs into new host roots with the
replacement configuration. Preserve the previous roots for recovery. Retain
compatible journals, vault, generated keys, durable identities and native
session state; do not regenerate transport keys or empty the data directory to
make a configuration change pass. When reusing a service label, stop its old
launchd job and preserve/remove its old managed plist before installing it in
the fresh root. Never run old and new owners against the same data concurrently.

`axis init` provisions new keys only. Preserve existing credentials, journals
and native session state as a matched recovery set when installing services.

`axis upgrade` accepts `--host-root`, `--source-package` and `--output` (plus
`--force` for the path below). It takes no role, configuration or selector
flags: it reads the pinned command state and verifies the exact managed plist.
Node upgrade is supported only through its drain-gated lifecycle: it verifies
idle before staging and again after stop, backs up matched state, activates and
verifies the candidate, and resumes only after success; rollback restores the
prior release and leaves Node drained. Node upgrade never starts, stops or
upgrades separate Workers and never substitutes their readiness for Node drain
evidence.

`axis upgrade --force` is the only lifecycle path that skips drain.

- **What it does.** It stops the managed service regardless of in-flight work, which is abandoned: the work lives in Linear and is dispatched again.
  - After `launchctl bootout` it waits, for at most 30 seconds, until launchd no longer lists the service and the process it reported before the stop has exited. If that does not happen, it fails at `stop` and archives nothing, so no file is moved while the old process still has it open.
  - It moves the storage the new release would refuse or wedge on, with its `-wal` and `-shm` files, into `<host-root>/archive/<UTC timestamp>/`: `<dataDir>/node-<id>.sqlite` for a Node, `<dataDir>/worker-<id>.sqlite` for a Worker, and nothing for a controller, whose Router and Relay databases stay in place.
  - It starts the new release and verifies readiness and the unauthenticated `401`.
- **What it never does.** It performs no Node maintenance drain, no database drain check, and no rollback. Files are renamed, never copied or deleted.
- **Ordering.** It stages before stopping, so a staging failure leaves the service running. It refuses before stopping anything when the archive directory already exists or is on a different volume from the storage.
- **Failures.** A failure after that point stops where it is and exits non-zero with the stage (`stop`, `archive`, `activate`, `verify`, or `resume`), the archive directory, and the files already moved. Archived files are not put back.
  - **A crash inside activation needs manual recovery.**
    - **Why.** Activation writes `<host-root>/axis.host.v1.json` before the managed plist. If the process dies, or the plist write fails, between those two writes, the state file names the new release while the plist still describes the old one.
    - **Symptom.** From then on every `axis upgrade` and `axis upgrade --force` stops at preflight (JSON `error.code` `invalid_or_unavailable`, stage `preflight` for `--force`), because the lifecycle integrity check finds that the managed plist does not match the state.
    - **No command repairs this in place.** Recover by hand:
      1. Run `launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/<service label>.plist`, and ignore "not loaded".
      2. Preserve the state and plist with the matched backup, then remove
         the inconsistent `<host-root>/axis.host.v1.json` and that plist.
      3. Run `axis install` again with the same `--role`, `--config`, selectors, `--host-root`, and `--service-label`. It stages, activates, and verifies the release again.
    - **What is kept.** The archive and the staged releases are not touched.
- **Re-runs.** Run on a host already at the target release, it only verifies and archives nothing, so re-run it after fixing a failed verification.
  - On a Node it also resumes maintenance, so a Node left drained by an earlier refused `axis upgrade` takes commands again.
  - A re-run after an `archive` failure moves what is left into a new timestamped directory, so that archive spans two directories, each with its own `forced-upgrade.v1.json`.
  - A re-run in the same second as an existing archive directory is refused before anything stops.
- **Receipt.** The receipt is `axis.host.forced-upgrade.v1`: `forced: true`, `rollback: false`, role, release version and digest, `serviceRef`, and the archive directory relative to the host root with each file's name (`node-<id>.sqlite`, `node-<id>.sqlite-wal`, and so on) and byte count. It never contains credentials or host names.
- **Without `--force`.** `axis upgrade` enforces the ordinary drain and rollback gates.

Public lifecycle receipts are `axis.host.receipt.v1`; they contain role, release version/digest, a domain-separated `serviceRef`, result, and rollback status, never the service label, path, secret, or raw error. A refused drain answers with `error.code` `node_not_drained` or `worker_not_drained` and the effective blocker counts; every other failure answers `invalid_or_unavailable`.

Install and upgrade stage the built package. A directory source is packed with
scripts disabled before installation, so the default installed package needs no
workspace sources and never links its mutable directory into a release. An
explicit `--source-package` archive is installed directly. Build a source
checkout before selecting it as the package source.

The TOML file is parsed without shell evaluation. Package staging uses public
dependencies and requires no registry token. Before replacement, ordinary
upgrade performs SQLite integrity and active-work/outbox checks, copies matched
state/plist, pinned TOML, generated keys, vault, role DB/WAL/SHM artifacts and
Claude session store locally, then starts the candidate package-local runner.
Backup discovers only the selected role's generated keys through its role
projection; unrelated topology keys need not be present. Relay's external
OAuth values are not backed-up files and must remain available in its service
context. Verification derives endpoints and credentials from the selected
projection; it requires healthy readiness and an unauthenticated `401` at the
private Router or Worker command boundary. Ambiguous ownership, corrupt state,
unsafe drain or failed restoration is fail-closed.

Node installation derives its command-listener URL from `nodes.host` and
`nodes.port`. It authenticates `GET /v1/node/readiness` with
`node-<id>-command.key`, requires the existing contract version and matching
`nodes.ref`/`nodes.version`, configured service and available storage, then
requires an unauthenticated GET to return `401`. Worker readiness entries remain
informational: unsupported or degraded provider readiness is not reported as
ready. Failed installation stops the candidate before removing its managed
state, plist and newly staged release; if stopping fails, recovery evidence is
retained. Installation never rewrites TOML, hosts Workers or changes their event
sinks.

## Deploy and rollback

### Worker command lockstep

Worker command `1.0.0` has no compatibility reader in either direction: a Worker refuses any other command version
as `axis.worker.malformed`. Every component runs the same build. `axis upgrade --force` skips the drain checks,
archives the role's stores (the Node journal `<dataDir>/node-<id>.sqlite` or the Worker journal, with WAL/SHM; nothing for
a controller), activates the new release, and does not roll back. Every Node journal open re-validates each
accepted receipt's Worker response against `WORKER_COMMAND_VERSION`, so a journal holding an accepted receipt from
another command version is refused with `axis.node.storage_unavailable`.

A Worker whose configuration drifts from its Node inventory entry refuses each new session and continue with
`axis.router.worker_identity_changed`; nothing runs and the session stays usable. Align the selected TOML Worker's `id`,
`executorId`, `kind` and `runtimeProfile` across the deployed declarations, apply the
managed configuration replacement procedure when applicable, restart the affected
services, and send the prompt again.


Drain new work before replacement. Stop Relay, Router, Node, and Worker, then take consistent backups of the Relay database/vault and the matched Router, Node, and Worker journals, including each SQLite database plus WAL/SHM artifacts and the Worker's `worker-<id>-native-sessions.json` session store beside its database. Include the Team Binding tables in the same Router backup. Preserve the exact reviewed package commit and the Relay/Router/Node/Worker configuration fingerprints with the backup. Restore and restart those four owners only as the same matched set; never replay a Node outbox against Router state from a different snapshot.

Upgrade Relay/Router, Node, Worker, and API clients as one matched maintenance
operation. Worker commands use exactly `1.0.0` at `/v1/worker/commands`;
Router-to-Relay events use exactly `1.0.0` with `axis.router.v1.*`. There is no
alternate reader, dual route, or replay translation. A Worker enumerates active
state before admitting new commands and accepts recovery only through exact
Executor attestation. Missing, malformed, or mismatched proof remains
recovery-required and never launches a replacement. Rollback requires draining
again and restoring compatible databases and binaries together.

Supported execution ingress is the signed Linear webhook.

Do not restore Router or Worker independently. A different process cannot safely continue an in-flight native binding. A restarted Linear-enabled Codex Worker continues a durable succeeded, failed, or waiting predecessor only while its app-server still lists the thread as not archived, so restore the Codex home (`nodes.workers[].codexHome`, or the native account default) with the Worker journal. A restarted Claude Worker requires its private session store and the native transcript in the recorded workspace. A proof the adapter declines returns `axis.worker.continuation_unresumable`; an unavailable proof returns `axis.worker.capability_unavailable`. Restore a matched compatible set only for durable replay/inspection; unresolved native work requires a new supervised Router command.

The Linear channel's four databases (see `docs/PERSISTENCE.md`'s Linear channel persistence section) each carry their own independent schema version -- `linear_installations` in `<dataDir>/linear-channel.sqlite` is schema version 1, `linear_inbox_deliveries` in the same file is schema version 1 and `linear_ledger_sessions` is schema version 1; `<dataDir>/linear-conversations.sqlite` is schema version 1 and `<dataDir>/linear-run-chain.sqlite` is schema version 1 (both use channel-neutral modules); `<dataDir>/linear-delivery.sqlite` is schema version 1. `channel_conversation_cursors`/`channel_message_outbox` own the neutral Activity outbox: Relay atomically projects a validated observation batch into it, advances its cursor, and clears observation ownership with operation-and-cursor compare-and-set. An explicit stop during active execution waits for observed cancellation; a stop while already waiting closes only the pending decision and enqueues one final response. Both paths retain the AgentSession for a later prompt. Every store refuses a database at any other schema version with `schema_version_unsupported`, so a binary rollback across a schema change needs the file restored from before the upgrade. Each schema ledger records exactly one row for the current schema. Stores create their complete schema once and never convert an existing database.

## Operational signals

- webhook acknowledgement completes within 5 seconds;
- first created-session Activity delivery begins within 10 seconds;
- each created-session intake has exactly one immutable startup-ledger row;
- readiness fails when Relay persistence or credential storage is unavailable;
- undelivered Worker outbox entries remain observable and retry only the immutable Router event;
- ambiguous Activity, command, or native binding state is never automatically converted back to pending;
- a permanently ambiguous Node command's upgrade block is released only by an explicit operator acknowledgement (`axis node ambiguous-commands acknowledge`), and the command and its Worker Run are kept as evidence, never re-sent.

## Incident handling

1. Stop the affected process before copying SQLite or vault files.
2. Check health, disk durability, journal integrity, and safe state/count/code projections; do not dump encrypted payloads or native output.
3. Restore only a matched backup set and the same reviewed build.
4. Allow durable Router event and Linear Activity delivery to replay with their original IDs.
5. Do not relaunch an ambiguous Codex or Claude Code Run, synthesize a native binding, or create a replacement Activity for an uncertain provider outcome. Releasing a permanently `ambiguous` Node command's or a stuck Worker Run's upgrade block (`axis node ambiguous-commands acknowledge`) is the one exception: it clears the upgrade gate only, retains the evidence row exactly as it is, and never re-sends or re-executes anything.
6. If a mapped workspace becomes unavailable, verify the path-free audit outcome and the configured organization root. Worker uses the root fallback when it remains usable and fails new work before executor launch only when the root is also unavailable.

**Known gap: a stranded `accepted` or `confirmed_cancel` command in the channel-neutral run chain cannot currently be resolved to a Task/Run lineage by reconciliation.** The run-chain reconciliation sweep (`ChannelRunChain#reconcileStale`), built on the neutral `ChannelCommandPort#reconcile`'s bare `accepted` result, reports such a command only as `accepted_unresolved` or `unknown` -- it remains operator-visible in `channel_run_chain_reservations` (status `accepted`/`interrupt_requested`), but reconciliation itself does not surface the `taskId`/`runId` a human would need to inspect it further, even though the underlying parsed reconciliation result carries them for every case but this one. Do not expect an automated reconciliation report to name the affected Task/Run for these two states; read `channel_run_chain_reservations`/`channel_run_chain_runs` directly instead.

**Unconfirmed command terminal window.** Router `receipt_pending` and Node
`ambiguous` rows may remain indefinitely after dispatch uncertainty. The
channel run chain bounds only its own reservation: after
`DEFAULT_UNCONFIRMED_RESERVATION_MS` (30 minutes), the observe sweep settles an
`unknown` outcome as `rejected` with `axis.channel.outcome_unconfirmed`, releases
the command slot, and posts one terminal Activity that claims neither execution
nor non-execution. `LinearChannelOptions.unconfirmedReservationMs` is an
in-process composition option, not an environment setting.

An unconfirmed start or continue guards the Session with `session_unconfirmed`;
only a stop of a genuinely executing Run bypasses that guard. A waiting Run is
already terminal. An unconfirmed stop releases the slot without guarding the
Session. Late evidence does not re-settle that reservation or remove its guard,
but Router Task events can still project later Activities, including a final
answer.

This sweep does not release Router or Node durable rows. Node ambiguous commands
still block upgrade until explicitly acknowledged through
`axis node ambiguous-commands acknowledge` with command, operator, authority,
audit-source, and request references. Acknowledgement releases only the upgrade
gate, preserves command/Run evidence, leaves Router `receipt_pending` unchanged,
and never resends or re-executes work.

**Known limitation: a Worker outbox entry the Node never accepts keeps `pendingWorkerOutbox` above zero.** `axis node ambiguous-commands acknowledge` releases only a Node command's own `ambiguousCommands`/`activeWorkerRuns` blockers and the linked Worker Run it left `reserved` or `observational`; nothing releases such an outbox entry.

**Known limitation: a stuck Worker Run not linked to any ambiguous Node command has no release command.**

## Node routing configuration

See [`NODE-ROUTING-CONFIGURATION.md`](NODE-ROUTING-CONFIGURATION.md) for the Relay/Node configuration contract,
operating Workers, and the `axis-routing-preflight` receipt. Activation requires the host-local installation and verification procedure.

## Optional Linear execution access

The optional GraphQL route uses `linear.apiEnabled = true` and
`linear.apiTimeoutMs` (default 30000, range 1–120000) in `axis.toml`.
Omitting enablement or setting it to `false` leaves the route absent (404). An enabled route without execution wiring or independent CLI grants returns 503;
there is no personal-token fallback. Existing webhook/AgentActivity channels
continue independently. See [protocol, audit retention and rollback](LINEAR-API-PROXY.md).

Optional execution access supplies Codex and Claude CLI/SDK environments. Set
`nodes.workers[].linearExecution = true` for each participating Worker and
`linear.apiEnabled = true`, with explicit `linear.executionGrants` workspace
consent. Relay derives receiver identities from those Workers and reads their
dedicated `worker-<id>-linear.key` files; each Worker derives the Relay origin
from `relay.url` and reads its own matching key. Follow the
[configuration and controlled acceptance guide](LINEAR-EXECUTION-CLIENTS.md).
Enabled Codex Workers support new Runs, observe, cancel and continue: a
follow-up turn in one AgentSession is a successor Run that issues its own
credential and resumes the prior Run's thread. An enabled Worker uses the native
account's Codex home unless `nodes.workers[].codexHome` selects another directory.
The native home exposes the same configuration, skills, plugins, authentication
and durable thread rollouts as an interactive Codex session. Disable
`linearExecution` for every participating Worker to remove Relay execution
receivers; children keep explicit unavailable Linear settings. Managed services
require the configuration replacement procedure below for any TOML change.

The Codex adapter pins every app-server thread/resume and turn to approval
policy `never` and `danger-full-access`. Axis adds no filesystem sandbox,
writable-root list, network restriction, or approval prompt. A Worker can pass
readiness and standalone CLI checks while missing this behavior, so live
acceptance requires a fresh AgentSession turn that exercises filesystem access
outside the workspace, network access, and an installed tool; binary/version
checks alone do not exercise this boundary.

