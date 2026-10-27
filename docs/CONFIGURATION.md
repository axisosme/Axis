# Configuration model and initialization

The versioned `axis.toml` model supplies immutable role configurations to both
built-in service commands and library composition APIs. `axis init` provisions
the declaration and private keys. All service commands accept `--config <path>`,
defaulting to `axis.toml` in the current directory. Node accepts `--node <id>`;
Worker binaries accept `--node <id> --worker <id>` to override `[host]` defaults.
A sole candidate selects itself. The chosen Worker kind must match the binary.
Unknown, duplicate or incomplete flags fail startup. There is no deployment
environment fallback. Creating or editing TOML does not reconfigure a running
service or migrate a database. Managed installs pin the file hash and selectors;
see [host lifecycle and replacement](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade).

## Initialize

With Node 24 and a built package, bare `axis init` starts the fresh-install
wizard when both input and output are terminals:

```sh
axis init
```

Without a TTY it fails with instructions to use explicit flags. The
[interactive deployment procedure](SINGLE-HOST-DEPLOYMENT.md#interactive-single-host-setup)
covers Linear application registration, temporary OAuth listeners, Team and
executor selection, and local health findings. Success leaves a single-host
`[serve]` configuration and a stopped service.

| Invocation | Behavior |
| --- | --- |
| `axis init` | Interactive on a TTY; fails without a TTY. |
| `axis init --interactive` | Explicit wizard mode; requires a TTY. |
| `axis init --interactive --config ./axis.toml --data-dir ./data --env-file ./axis.env` | Wizard with custom output paths. |
| `axis init --config ./axis.toml` | Scripted key initialization, including for an existing custom configuration. |
| `axis init --data-dir ./data` | Scripted initialization of a new default configuration and keys. |
| `axis init --non-interactive` | Scripted initialization with default `axis.toml` and `data` paths. |

Explicit `--config` or `--data-dir` keeps scripted behavior unless
`--interactive` is also supplied. The two mode flags are mutually exclusive.
`--env-file` selects wizard output only; it is not a secret-file loader for
initialization or service commands. With custom `--config` or `--data-dir`,
include `--interactive` to use it; `--non-interactive --env-file` is refused.
Unknown, duplicate and missing-value flags are refused.

The wizard defaults to `axis.toml` in the current directory, `data` relative to
the configuration, and `axis.env` beside that configuration. Explicit config
and env-file paths resolve from the current directory; `dataDir` resolves from
the configuration file. Config and env-file paths must differ. Their parent
directories must exist. Existing configuration, env files or required key
paths, including symlinks, are refused. The wizard preserves partial files on
failure or cancellation and does not resume or repair an installation; follow
[manual recovery](SINGLE-HOST-DEPLOYMENT.md#initialization-health-and-manual-recovery).

Scripted initialization opens no listener and makes no network request. A new
configuration is a single-host Codex template with placeholder organization
and Linear client identifiers to replace before connecting an installation.
It generates private transport keys but creates no Linear env file, installs
no service, and authenticates neither Linear nor the executor.

To initialize a complete custom topology, copy [the complete example](../axis.example.toml),
edit it, then run:

```sh
axis init --config ./axis.toml
```

An existing configuration file is preserved. `--data-dir` applies only when
creating a new configuration. Both modes check every required key path
before generating any key and refuse if any already exists, including a
symlink. Neither mode rotates keys or overwrites a pre-existing configuration. A private lock
serializes initializers for a data directory. A crash or storage failure can
leave a partially initialized directory; preserve it for inspection before
explicit recovery. A surviving `.init.lock` also requires operator inspection.

## Schema reference

`schemaVersion = 1` and `dataDir` are required. Supply either `[serve]` or
`[router]` and at least one `[[nodes]]`. Unknown keys, wrong types, duplicate identities, contradictory
selection rules and invalid transport settings fail parsing. Errors name
schema fields without quoting input values or TOML parser diagnostics.

All configured paths, including `dataDir`, workspace roots, host roots,
`codexHome` and `claudeExecutable`, resolve relative to the configuration file.
A parsed configuration is deeply immutable. Parsing reads no secret, opens no
store, starts no process and creates no directory.

| Table | Fields and defaults |
| --- | --- |
| Root | `schemaVersion = 1`; required `dataDir` path. |
| `serve` | Single-host shorthand: required `organizationId`; `kind = "codex"`, `workspaceRoot = "."`; optional `claudeExecutable`, `codexHome`, `profileFallbackPolicy`. Cannot be combined with `host`, `router` or `nodes`. |
| `host` | Optional `node` and `worker` IDs select the local role. A sole Node or sole Worker selects itself. Multiple candidates require an explicit selection when projecting that role. |
| `relay` | `host = "127.0.0.1"`, `port = 8787`, optional advertised origin `url`, otherwise derived from host and port. |
| `router` | Required `port`; `host = "127.0.0.1"`; optional advertised origin `url`; optional `defaultWorker` names a global Worker ID; `defaultExcludedTeamIds = []`; `nativeSteer = false`. |
| `linear` | Optional for Node/Worker-only use, required to start Relay; required non-secret `clientId` and `redirectUri`; `teamIds = []`, `allowAllPublicTeams = false`, `apiEnabled = false`. Optional tick and integrity intervals are positive integer milliseconds; `apiTimeoutMs` is 1–120000. |
| `linear.executionGrants` | Array of explicit consent entries with `organizationId`, `appUserId` and `scope = "installation-workspace"`. One entry per organization, at most 1000. |
| `linear.cliGrants` | Array with `credentialId`, SHA-256 `credentialHash`, `organizationId`, `appUserId`, `expiresAt`. IDs and hashes are unique, at most 1000. Requires `apiEnabled = true`. Raw credentials are never stored in TOML. |
| `nodes` | Required `id`, command `port`, `eventPort`, `workers` array. `ref` defaults to `id`, `version = "v1"`, `host = "127.0.0.1"`, `eventHost = "127.0.0.1"`. Optional advertised `url`, otherwise derived from command listener. |
| `nodes.workers` | Required globally unique `id`, `kind` (`codex` or `claude-code`), `port` and nonempty `workspaces`. `key`, `executorId` and `endpointRef` default to `id`; `version` and `endpointVersion` default to `v1`; runtime profile defaults to the executor ID and Worker version. |
| Worker options | `host = "127.0.0.1"`; `labels` defaults to `codex` or `claude` according to kind; `profileFallbackPolicy = "allow"`; `linearExecution = false`. Optional `hostRoot`, `codexHome`, `claudeExecutable`, `runtimeProfile = { ref, version }`. |
| `nodes.workers.instructions` | Optional nonblank `sessionResponse` up to 64 KiB, or `omitSessionResponse = true`; the two cannot be set together. The Linear execution paragraph is not configurable. |
| `nodes.workers.workspaces` | Required `organizationId`, `workspaceRoot`; optional `workspaceCatalog` array of `{ workspacePolicyRef = { ref, version }, workspaceRoot }`. Organization mappings and catalog references are unique within their owner. |

Only numeric loopback HTTP origins or HTTPS origins are accepted for advertised
Relay, Router and Node URLs. Credentials, paths, query strings and fragments
are not part of an advertised origin. Node event and Worker listeners require
numeric loopback addresses. Worker ports must be distinct from each other and
from both Node ports on their Node. Relay and Router listen on different ports.

Node selection stays with the Node. `nodes.defaultWorker` names a local Worker
**key**; a single Worker is its own default, and multiple Workers require an
explicit default. `nodes.workerScopeRules` contains `{ scope = { system =
"linear", tenantId, scopeId }, workerKey }` entries. A scope can appear once
and must name a local key. Each executor label (`codex`, `claude`) can
bind to one local Worker through its `labels` array. Explicit labels take
precedence over matching scope rules, then the Node default applies.

Node timing options retain their runtime bounds:

| Option | Default | Allowed milliseconds, or count |
| --- | ---: | ---: |
| `workerDispatchTimeoutMs` | 30000 | 1000–120000 |
| `routerEventTimeoutMs` | 10000 | 1000–60000 |
| `workerReadinessTimeoutMs` | 5000 | 500–30000 |
| `eventRetryInitialDelayMs` | 1000 | 100–60000 |
| `eventRetryMaxDelayMs` | 30000 | 1000–300000, at least the initial delay |
| `eventMaxConcurrentRuns` | 8 | 2–64 |

A Worker with `linearExecution = true` requires a Linear API configuration and
explicit execution consent. Its receiver identity and dedicated credential
are derived from that Worker's declaration.

## Single-host process

This complete single-host declaration runs Relay, Router, Node and one Worker
with `axis serve --config ./axis.toml`:

```toml
schemaVersion = 1
dataDir = "data"

[serve]
kind = "codex"
organizationId = "example-organization"
workspaceRoot = "workspaces/example"

[relay]
host = "0.0.0.0"
port = 8787
url = "https://axis.example/"

[linear]
clientId = "example-client"
redirectUri = "https://axis.example/oauth/linear/callback"
teamIds = ["example-team"]
```

Replace the fictional identifiers and origin with your Linear application and
workspace settings. Create the workspace directory and authenticate the native
executor as the service account. Run `axis init --config ./axis.toml` once to
generate keys, then supply `LINEAR_CLIENT_SECRET` and `LINEAR_WEBHOOK_SECRET`
through the process environment. This explicit `--config` path does not
authenticate Linear or the executor; complete the normal operator-authorized
OAuth installation. For a fresh interactive deployment, use
[the wizard](SINGLE-HOST-DEPLOYMENT.md#interactive-single-host-setup) instead of copying this
file first.

The shorthand expands to Node `local` and Worker `local-worker`. Router uses
port 8788, Node commands 9080, Node events 9081, and Worker commands 9082, all
bound to `127.0.0.1`. The Router default and Node inventory derive from that
Worker. Relay defaults to `127.0.0.1:8787`; only its host and port may expose a
public listener. Use a TLS reverse proxy for public webhook and OAuth traffic.
The external listener retains the existing health, authenticated operator and
optional Linear API routes. No internal command or event route is forwarded.

For Claude Code, use `kind = "claude-code"` and supply `claudeExecutable`,
resolved relative to TOML, in `[serve]`. Its native preflight must succeed.
For custom internal ports, instructions, workspace catalogs or Linear execution
access, use the explicit topology schema above with exactly one Node and one
Worker. `axis serve` requires every private host to be exactly `127.0.0.1`,
Router and Node advertised URLs to match their listeners, and all five ports to
be distinct. The same explicit declarations remain usable by standalone role
commands. The shorthand is an additive schema-version-1 input form; the parsed
configuration contains the normal expanded role declarations.

Startup validates the topology and all role credentials before opening stores
or listeners. Router starts first, then Node, then Worker. Relay ingress and
its scheduled channel loops start only after local readiness succeeds. Readiness
requires available Node storage and, for Codex, ready app-server evidence.
Claude Code uses its successful native startup preflight and active listener;
it has no continuous native readiness probe. `GET /health/ready` aggregates
channel readiness, active listeners and the bounded authenticated Node probe;
failure returns 503. `SIGINT` and `SIGTERM` stop ingress and channel loops,
then Worker, Node, and Router, and close their stores. Failed startup releases
every successfully started role in the same reverse order.

## Data directory and secrets

Operators configure one `dataDir`. There are no database-path fields in the
schema. The role projections derive:

| File under `dataDir` | Owner |
| --- | --- |
| `router.sqlite` | Router and Team bindings |
| `linear-channel.sqlite` | Linear inbox, installation and ledgers |
| `linear-conversations.sqlite` | Conversations |
| `linear-run-chain.sqlite` | Run chain |
| `linear-delivery.sqlite` | Delivery outbox |
| `node-<id>.sqlite` | Node journal |
| `worker-<id>.sqlite` | Worker journal |
| `worker-<id>-native-sessions.json` | Claude native session store |
| `relay-vault.json` | Encrypted OAuth credential vault |

`dataDir` and `dataDir/secrets` must be real, owner-private
directories. Initialization creates them with mode `0700` and refuses an
existing unsafe directory. Each generated key uses 32 random bytes, encoded
as base64 with a trailing newline. Key files have mode `0600`; readers refuse
symlinks, non-regular files, wrong owners, unsafe permissions and invalid key
contents. Generated key paths and key material stay out of initialization output.

| File under `dataDir/secrets` | Readers |
| --- | --- |
| `relay-master.key` | Relay; encryption master key |
| `relay-operator.key` | Relay; operator API bearer |
| `node-<id>-command.key` | Relay and that Node |
| `node-<id>-router-events.key` | Relay/Router and that Node |
| `worker-<id>-command.key` | That Node and Worker |
| `worker-<id>-events.key` | That Node and Worker |
| `worker-<id>-linear.key` | Relay and that Worker, only with Linear execution enabled |

For separate hosts, retain the matching declaration identities and securely
provision only the key files each role needs into its local `dataDir/secrets`.
Each host has its own data directory and database files; co-located roles may
share a data directory while retaining separate derived files. Generate a shared
transport key once and copy it securely to its authorized readers; separately
initializing both sides would generate unrelated credentials. Installed service configuration and the operational provisioning procedure are
described in [Multi-host deployment](MULTI-HOST-DEPLOYMENT.md).

`readLinearSecrets()` reads only `LINEAR_CLIENT_SECRET` and
`LINEAR_WEBHOOK_SECRET` from its environment argument (default `process.env`).
Those values cannot be configured in TOML. OAuth client ID, redirect URI,
feature flags and Team lists belong in TOML. Internal transport tokens and the
master key belong in generated files. Per-Run credentials and provider-issued
OAuth tokens remain runtime credential material, outside this configuration.

### Linear environment file

The wizard stores only the nonsecret client ID and other deployment settings
in TOML. It writes the client secret and webhook signing secret to `axis.env`
(or `--env-file`) with exclusive creation (`wx`) and mode `0600`. Secret prompts
hide input. Values must be nonempty single-line tokens without quotes,
backslashes, whitespace or control characters. The file uses single quotes
to preserve literal values, including dollar signs:

```dotenv
LINEAR_CLIENT_SECRET='example-client-secret'
LINEAR_WEBHOOK_SECRET='example-webhook-secret'
```

Keep the completed file private and uncommitted. Its syntax works with systemd
`EnvironmentFile`, Compose `.env` interpolation or
`docker compose --env-file ./axis.env`, and Node `--env-file=./axis.env`.
Axis does not load it automatically. `docker run --env-file` treats quotes as
literal characters: use a separate unquoted `NAME=value` file for that command,
as shown in [Single-host deployment](SINGLE-HOST-DEPLOYMENT.md#executor-authentication-and-secrets).

## Library surface

The package root exports `parseAxisConfiguration`, `loadAxisConfiguration`,
`databasePaths`, `deriveRelayConfiguration`, `deriveNodeConfiguration`,
`deriveWorkerConfiguration`, `selectedNode`, `selectedWorker`,
`initializeAxis`, `generateAxisSecrets`, `secretNames`, `secretPath`,
`readAxisSecret`, `readLinearSecrets`, their public types, and
`AxisConfigurationError`. It also exports `startRelay`, `startNode`,
`startCodexWorker`, `startClaudeWorker` and `composeLinearChannel` with their
configuration and runtime types.
`startServe`, `validateServeConfiguration`, `ServeRuntime`, `ServeOptions` and
`ServeWorkerRuntime` expose the single-process composition. `startServe` accepts
the parsed topology and optionally explicit secrets, native environment, a
startup cancellation signal and trusted fake-provider/Worker factories for
embedding tests. It returns all three role runtimes, `ready()` and an idempotent
`close()`. Signal handling belongs to the CLI or embedding process.
An injected Worker starter receives the startup signal and owns cancellation of
its own pending work. The built-in Codex starter accepts an optional third
`AbortSignal` argument and closes its initializing transport on cancellation,
then releases its journal before the enclosing composition closes Node and Router.

The Relay projection derives physical endpoints, logical targets, allowed
origins and optional Linear execution receivers from the same nested topology.
Node inventory targets and Worker identities use the same references. Each
projection reads only that role's private files. An optional `SecretReader`
parameter provides deterministic credentials for composition tests; it is a
trusted library argument, never a TOML setting or environment fallback.

## Composition boundary

Parsing validates declarations with the shared Node target, Worker selection
and workspace validators before any role reads private files. Derivation
checks role credentials for format and reuse and returns a deeply immutable
projection. Native executable canonicalization happens before a Claude Worker
opens its journal. These are startup checks; the wire validators still validate
every incoming command and event.

`deriveRelayConfiguration(config, readSecret?, linearSecrets?)` includes the
Router runtime, authenticated event bindings, Linear channel settings, optional
API settings and execution receivers. With a Linear declaration it requires
`LINEAR_CLIENT_SECRET` and `LINEAR_WEBHOOK_SECRET`, or the explicit
`readLinearSecrets()` result supplied as `linearSecrets`. The latter is a trusted
library parameter. A declaration without Linear settings can derive the routing
projection; `startRelay` refuses it before opening storage.

```ts
import {
  loadAxisConfiguration, deriveRelayConfiguration, deriveNodeConfiguration,
  deriveWorkerConfiguration, readLinearSecrets, startRelay, startNode,
  startCodexWorker,
} from "@axisosme/axis";

const configuration = loadAxisConfiguration("./axis.toml");
const relay = await startRelay(
  deriveRelayConfiguration(configuration, undefined, readLinearSecrets()),
);
const node = await startNode(deriveNodeConfiguration(configuration, "node-one"));
const worker = await startCodexWorker(
  deriveWorkerConfiguration(configuration, "worker-one", "node-one"),
  process.env,
);
// Close each runtime when the embedding process shuts down.
```

Each starter accepts its role projection directly. `composeLinearChannel`
accepts the Relay projection's `linear` settings plus explicit dependencies
(cipher, vault, provider, Router and Worker-routing audit). It does not parse
configuration or reread environment values. Relay audit and tick-failure
callbacks are supplied through `startRelay` options. The installed entry point
supplies its process logging callbacks.
Embedding options also accept a provider, an additional readiness predicate,
and `beforeIngress`, called after Router starts. Its returned cleanup callback
runs after ingress stops and before Router closes, including on startup failure.
`closeIngress()` stops only the public listener and scheduled loops;
`close()` releases the complete Relay runtime.

Worker starters receive inherited native-process environment as a separate
argument. It provides the native account's home, executable search path and
package settings. Role identity, transport credentials, instructions, databases,
Linear receiver configuration and runtime profile come from the projection.
An explicit `codexHome` also selects the native Codex home. Otherwise profile
discovery uses the inherited `CODEX_HOME`, or `.codex` under the native `HOME`,
matching the child process. Configured `codexHome` paths resolve relative to
TOML first. A Worker projection
must match the selected starter's executor kind. There is one runtime assembly
path per role; built-in entry points parse TOML and derive these same shapes.

## Native child protocol

The Worker generates `LINEAR_EXECUTION_STATUS`,
`LINEAR_EXECUTION_EXPIRES_AT` and `LINEAR_EXECUTION_SDK_MODULE` for each native
execution. They describe the issued capability and installed SDK; they are not
operator settings. Child environment construction removes inherited deployment
and Linear namespaces and the registry authentication token, then injects only
the current execution's values. Disabled or unavailable executions receive a
nonempty refusal key and endpoint, with no inherited expiry or SDK module.
Codex applies the same Linear values to each thread's shell environment policy.
The SDK checks the injected status and expiry, and executor instructions describe
the values the child actually receives.

The dedicated CLI launcher generates `LINEAR_CLI_LAUNCHER_ACTIVE=1` in its child
and refuses recursive invocation. It replaces inherited Linear credentials with
its dedicated configuration and clears execution expiry and SDK metadata. It
never changes the operator's personal CLI configuration. See
[Linear execution clients](LINEAR-EXECUTION-CLIENTS.md) for capability lifetimes
and usage.

`CLI_SCHEMA_VERSION` is the CLI's in-process schema constant, whose value is
`axis.cli.v1`; it is not an environment setting. Deployment-namespace scrubbing
matches the namespace boundary rather than naming a partial environment key.
