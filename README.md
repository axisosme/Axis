# Axis

**Run AI agents from Linear, on machines you control.**

Mention @Axis in a Linear issue to send work to Codex or Claude Code. Axis
routes it to your configured workspace, tracks the execution, and returns
progress and results to the issue. Follow-up messages continue the same
session; cancellation and durable delivery keep retries from starting duplicate
work. The executor does the reasoning and coding; Axis coordinates its lifecycle.

## Quick start: about 10 minutes

You need Node **24**, npm, a Linear workspace where you can create and install
an OAuth application, and an existing writable task workspace. Have your
executor authenticated and, for Codex, its four role profiles ready; follow
[executor setup](docs/SINGLE-HOST-DEPLOYMENT.md#native-executor-profiles).
Both executors can write files and use network tools with the service account's
permissions; see [Security](docs/SECURITY-MODEL.md).

### 1. Clone and build

```sh
git clone https://github.com/axisosme/Axis.git
cd Axis
npm install
```

`npm install` builds the source through its `prepare` hook, including the
bundled contracts workspace; no registry token is needed. For this shell,
expose the built CLI with Node's optional environment-file loading:

```sh
axis() { node --env-file-if-exists=./axis.env ./dist/bin/axis.js "$@"; }
axis --help
```

### 2. Connect Linear

Prepare [public HTTPS forwarding](docs/WEBHOOK-EXPOSURE.md) to
`127.0.0.1:8787` first. Cloudflare Tunnel, ngrok and a TLS reverse proxy are
supported deployment options. The forwarder must be running during setup so
Linear can reach the temporary OAuth callback listener.

```sh
axis init
```

Run this in a terminal, with no existing `axis.toml`, `axis.env` or data keys.
The wizard prints the exact OAuth callback and webhook URLs, tells you what to
enable in the Linear application, then asks for its client ID and secrets.
Open the authorization URL, install Axis, choose a Team and executor, and enter
the absolute directory in which tasks should run. Claude Code also needs its
executable's absolute path.

Success writes private configuration/keys and `axis.env`, installs an audited
Team binding, and reports local health. Keep these files uncommitted. The wizard
does not install/login an executor or start a persistent service. Resolve any
executor/profile findings before continuing; “service not reachable” is expected
while the service is stopped. See [setup and recovery](docs/SINGLE-HOST-DEPLOYMENT.md#interactive-single-host-setup)
for custom paths or interrupted setup.

### 3. Start Axis

```sh
axis serve --config ./axis.toml
```

The shell function above loads the wizard's `axis.env`; an installed `axis`
command does not load it automatically. Equivalently, use the wizard's printed
`node --env-file=...` command. Wait for `Axis serve ready`. In another terminal:

```sh
curl --fail http://127.0.0.1:8787/health/ready
```

`serve` runs Relay, Router, Node and one Worker in one process. Keep the HTTPS
forwarder running. Only Relay's port 8787 belongs behind it; the internal ports
remain private. Ctrl-C or SIGTERM stops the whole instance.

Prefer containers or a managed service? Follow [Docker Compose](docs/SINGLE-HOST-DEPLOYMENT.md#docker-compose)
or [Linux systemd](docs/SINGLE-HOST-DEPLOYMENT.md#linux-systemd). Compose needs
container paths, persistent executor state and credentials; it does not consume
the host wizard's data automatically.

### 4. Mention @Axis

In an issue in the selected Linear Team, mention **@Axis** and give it a task,
for example “Inspect this repository and summarize how its tests run.” Axis
creates an AgentSession and posts Activities as the executor works. Reply in
that session for follow-up work. A successful turn verifies actual execution;
health and native login checks alone make no model request.

If setup or delivery fails, use the [troubleshooting table](docs/SINGLE-HOST-DEPLOYMENT.md#logs-and-troubleshooting).

## Supported executors

| Runtime | Worker binary | Supported operations |
| --- | --- | --- |
| Codex | `axis-codex-worker` | Start, observe, continue, cancel. Continuation uses the bound native thread; no native soft steer. |
| Claude Code | `axis-claude-worker` | Start, observe, continue, cancel, native soft steer on a running turn. Continuation resumes the retained session in its original workspace. |

Codex uses the package-local app-server. Claude Code uses your authenticated
native executable with its default tools and `bypassPermissions`; it supports
coding and file writes. Codex uses `danger-full-access` with approval policy
`never`. Axis adds no OS sandbox. Exact bindings, native evidence and capability
checks govern follow-ups and restart recovery; an ambiguous result never
licenses replacement work. Native steer is opt-in through `router.nativeSteer`.
See [executor adapters](docs/EXECUTOR-ADAPTER.md) for restrictions and recovery.

## How it fits together

**Linear → Relay → Router → Node → Worker → executor → Linear Activities.**

Relay verifies and journals source events. Router owns Task/Attempt/Run
identities and freezes the target. Node selects a co-located Worker and journals
dispatch; Worker resolves the allowed workspace and tracks the native executor.
Each boundary retains separate credentials and durable journals, including
inside `serve`. A Node never supervises Worker processes.

A Task groups execution attempts; a Run is one turn. Continuation creates a
successor Run using the retained native session. Axis owns dispatch and lifecycle,
while the executor owns its work, tools and reasoning. For more machines or
multiple Workers, use the [advanced deployment guide](docs/MULTI-HOST-DEPLOYMENT.md).

## Documentation

| Guide | Contents |
| --- | --- |
| [Single-host deployment](docs/SINGLE-HOST-DEPLOYMENT.md) | Setup, executor profiles, containers/systemd, upgrade, backup, logs and troubleshooting |
| [Multi-host deployment](docs/MULTI-HOST-DEPLOYMENT.md) | Separate services, routing, preflight and managed host lifecycle |
| [Webhook exposure](docs/WEBHOOK-EXPOSURE.md) | Cloudflare Tunnel, ngrok and Caddy with TLS |
| [Configuration](docs/CONFIGURATION.md) | TOML schema, `init` flags and secret ownership |
| [API](docs/API.md) / [OpenAPI](openapi.yaml) | HTTP/wire contracts and operator commands |
| [Executor adapters](docs/EXECUTOR-ADAPTER.md) | Native capabilities, bindings and recovery proofs |
| [Linear integration](docs/LINEAR-PROVIDER.md) | Provider behavior and authorization |
| [Linear execution clients](docs/LINEAR-EXECUTION-CLIENTS.md) | Optional scoped CLI/SDK access for executor tasks |
| [Persistence](docs/PERSISTENCE.md) / [Security](docs/SECURITY-MODEL.md) | Journal ownership, recovery and trust boundaries |
| [Contributing](CONTRIBUTING.md) / [Changelog](CHANGELOG.md) | Validation and release notes |

For repository validation, run `npm run check` on Node 24. It covers static,
Relay/Node lifecycle and distribution checks. Authenticated live-runtime checks
are separate; see [Contributing](CONTRIBUTING.md).
