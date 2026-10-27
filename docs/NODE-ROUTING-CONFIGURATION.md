# Node routing configuration

The topology is Relay/Router -> Node -> Worker. This document lists the settings each process needs, how
an operator moves work between Workers, and what `axis-routing-preflight` reports.

Everything below uses TOML field names and fictional path templates only. Keep the deployment inventory (hosts, host
roots, labels, credentials) in the operator's local records, never in the repository, a pull request, or a
ticket.

## Configuration contract

| Process | TOML field or derived credential | Meaning |
| --- | --- | --- |
| Relay | `nodes[].id`, `ref`, `version`, `url`; nested `nodes.workers` | Physical Node transports and logical Worker targets come from the same declarations; see [Multi-host deployment](MULTI-HOST-DEPLOYMENT.md#router-endpoint-transport-configuration). A binding's `endpointRef` joins exactly one versioned Worker target. |
| Relay | `nodes.workers[].id`, `kind`, `runtimeProfile` | Derives the exact allowed event origins for each Node; an empty Worker list denies every origin. |
| Node | `nodes.workers`, `nodes.defaultWorker` | Inventory and default local **key**. Multiple Workers require an explicit default, a sole Worker defaults to itself, and an empty inventory must omit the default. Reachability does not change this rule. |
| Node | `nodes.workers[].labels` | Maps the fixed executor labels `claude`, `codex` to a local Worker. At most one Worker per label. Defaults follow `kind`; set `labels = []` on Workers that should not claim a label. |
| Node | `nodes.workerScopeRules` | `{ scope = { system = "linear", tenantId, scopeId }, workerKey }` Team rules. Organization is `tenantId`, Team is `scopeId`. One rule per scope, naming an inventory key. |
| Node and Relay | `node-<id>-router-events.key` | Dedicated Node-to-Router event bearer under `dataDir/secrets`. |
| Worker | `nodes.id`, `nodes.workers[].id`, `executorId`, `kind`, `runtimeProfile` | Worker execution identity. Command and event bearers come from separate `worker-<id>-command.key` and `worker-<id>-events.key` files. |

`--config` selects TOML (default `./axis.toml`); `--node` selects a Node and
`--worker` additionally selects a Worker. Otherwise `[host]` defaults or a sole
candidate apply. Settings are read once at startup; invalid declarations fail
before a store opens. Give each Node separate command and event keys. Never
reuse Worker command/event, Node command/event, operator or OAuth credentials,
and never place credentials in rule keys or logs.

Managed installations pin the complete TOML hash and selected IDs. Every
configuration edit mentioned below requires stopped-service matched backup and
fresh installation roots; restarting against edited pinned TOML is refused.
See [the replacement procedure](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade).

The first Task in a conversation freezes its `1.0.0-node` authority and label selection. Continuations do not
re-read labels, bindings, or the default inventory. An unknown selected key, a missing frozen Node version, or
a native recovery refusal never retries against another Worker.

## Operating Workers

- **Deploy or restore a Worker, then restart its Node.** `nodes.workers` is read once at startup, so a
  Worker added or brought back is picked up only when its Node restarts. A follow-up refused
  `worker_unavailable` is fixed the same way: correct the Worker, restart the Node, and send the prompt again.
- **Switching Workers.** The Node owns local Worker assignment; a Router binding's endpoint reference does not
  move work between Workers.
  - Per Linear issue, without touching configuration: put one of `claude` or `codex`,
    or its `WORKER` group label (`WORKER:CODEX`, `WORKER:CLAUDE`), on the source.
    It overrides the Team rule and the default for that new session. Two different executor labels at once
    is refused.
  - Per Team: change `nodes.workerScopeRules` and restart the Node.
  - For all otherwise-unrouted new work: drain the Node, confirm its work settled, restart it with a new
    `nodes.defaultWorker`, and resume.
  - Conversations already open are not affected: a continue or cancel uses the reservation frozen for that
    session, and relabelling an issue afterwards does not move it.
  - An executor label this Node has no Worker for fails outright (`axis.node.target_unknown`); it never runs on
    a different executor.
- **Archives are for audit only.** `axis upgrade --force` renames a Node or Worker journal into
  `<host-root>/archive/<UTC timestamp>/`. Open an archived journal read-only; never restore one into service.

## The preflight receipt

`axis-routing-preflight <relay|node|worker>` ships in the package and reads the version of the release it runs
from. It opens no listener and makes no network request; it copies each database with SQLite backup and checks
only the copy. Its receipt carries bounded counts and blocker codes, never a credential, path, id,
or issue number, and exits 1 when blocked.

```sh
<host-root>/releases/<release-digest>/node_modules/.bin/axis-routing-preflight node \
  --config /opt/example/axis/axis.toml --node node-placeholder \
  --expected-version <release-version>
```

Use `relay`, `node` or `worker` as the positional role. Relay accepts `--config`;
Node also accepts `--node`; Worker accepts `--node` and `--worker`. Defaults and
selection rules match the service binaries. All roles accept `--expected-version`.

`<release-digest>` is the upgrade receipt's `manifestDigest` without its `sha256:` prefix.

When a selected Relay/Worker pair serves Linear AgentSessions, invoke its preflight with
`--require-linear-execution true`. It turns an otherwise valid default-off execution configuration
into the `linear_execution_required` blocker, and checks the enabled Relay and Worker configuration against
their actual configuration gates. This value-taking flag accepts `true` or
`false` and is valid only for Relay and Worker. Preflight reads local keys and,
for Relay, external Linear secrets; it does not probe provider authentication.
A Claude Worker preflight canonicalizes its configured executable.

**Blockers** (the stack cannot start or cannot run correctly):

| Blocker | Meaning |
| --- | --- |
| `axis_version_mismatch` / `axis_version_unverified` | The release this host runs is not `--expected-version`, or that flag is omitted. |
| `configuration_invalid` | The role's configuration does not parse. |
| `linear_execution_required` | This Relay or Worker was deployed for Linear AgentSessions but execution access is switched off. |
| `storage_or_inventory_incompatible` | A store cannot be opened or initialized on the copy. |
| `node_journal_requires_archive` | The Node refuses to open the Node journal. Archive it with `axis upgrade --force`. |
| `retained_node_version_missing` | A `1.0.0-node` Task names a Node version no configured endpoint serves. |

**Counts** are evidence only and never block; `retainedSessionsWithoutWorker` (Node) and
`nonterminalWorkerRuns` (Worker) are the ones an operator acts on.
