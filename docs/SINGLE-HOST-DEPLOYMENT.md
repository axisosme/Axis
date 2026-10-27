# Single-host quick deployment

Use `axis serve` for one Relay, Router, Node and Worker in a single process.
Docker and systemd run that same CLI; `axis install` and `axis upgrade` remain
optional macOS LaunchAgent management commands. These examples do not install
host services automatically. See [Configuration](CONFIGURATION.md#single-host-process)
for the topology, credential ownership and readiness contract.

## Before you start

Install Node 24, build the source checkout with `npm install` (its `prepare`
hook builds the package), and use `node ./dist/bin/axis.js` wherever this guide
says `axis`. An installed package exposes that same entry point as `axis`.
Have a Linear workspace where you can create and install an OAuth application,
an authenticated native executor, and an existing writable workspace directory.
Provision [native executor profiles](#native-executor-profiles) before starting.
Allow about ten minutes for interactive setup once these prerequisites and a
[public HTTPS endpoint](WEBHOOK-EXPOSURE.md) are ready.

## Interactive single-host setup

With Node 24 and a built package, run `axis init` in a terminal for a fresh
installation. From a source build, use `node ./dist/bin/axis.js init`.
Both input and output must be TTYs; scripts and containers should use the
[noninteractive flags](CONFIGURATION.md#initialize). To customize wizard paths:

```sh
axis init --interactive --config ./axis.toml --data-dir ./data --env-file ./axis.env
```

Prepare [public HTTPS forwarding](WEBHOOK-EXPOSURE.md) before completing OAuth. The wizard asks for
the public origin, for example `https://axis.example.com`, without credentials,
a path, query or fragment. Forward that origin to Relay at `127.0.0.1:8787`;
do not expose Router or other private ports. The wizard does not create the
proxy or its TLS certificate.

In Linear Settings > API > OAuth applications, register the application using
the values printed by the wizard:

| Setting | Value for the example origin |
| --- | --- |
| Name | `Axis` |
| Callback URL | `https://axis.example.com/oauth/linear/callback` |
| Webhook URL | `https://axis.example.com/webhooks/linear` |
| Agent capabilities | Assignable and mentionable |
| Webhook categories and actions | `AgentSessionEvent` (`created`, `prompted`), `PermissionChange` (`teamAccessChanged`), `OAuthApp` (`revoked`) |
| OAuth scopes | `read`, `write`, `app:assignable`, `app:mentionable` |

Enter the client ID, client secret and webhook signing secret when prompted.
Only the client ID belongs in TOML; the two secrets go into the private
[Linear environment file](CONFIGURATION.md#linear-environment-file), defaulting
to `axis.env` beside the configuration. Existing config, env files or required
keys are refused; do not copy a template before using the wizard.

Setup starts a real temporary Relay on `127.0.0.1:8787` and Router on
`127.0.0.1:8788`. Reserve both ports. It starts no Node, Worker or executor
session. Open the displayed authorization URL and install Axis in the intended
Linear workspace within ten minutes. The public reverse proxy must already
reach Relay for the OAuth callback. Keep task ingress idle during setup.

The initial Team allowlist is empty, with all-public-Team access disabled.
After installation, the wizard verifies the installed identity and uses its
OAuth token to fetch all pages of accessible Teams. Choose a numbered Team,
then `1` for Codex or `2` for Claude Code, and an absolute workspace directory
that already exists. Claude Code also requires an absolute path to its existing
executable. The wizard does not install or log in either native executor.

Setup narrows the installation allowlist to that Team with an expected-revision
guard and writes the same Team into TOML with `allowAllPublicTeams = false`.
It creates the binding through `axis team-bindings set --expected-revision absent`
with operator, authority, request and audit-source metadata. Concurrent
installation changes or an existing binding cause refusal rather than an
overwrite. Success leaves a single-host `[serve]` declaration for the installed
organization, chosen executor and workspace.

The final local health report checks configuration, private keys, native
`--version`, login status, the Codex role-profile registry when applicable, and
service reachability. It makes no model request. Resolve reported authentication
or profile findings using the [executor setup](#executor-authentication-and-secrets)
below. Temporary listeners, loops and stores close on success, failure or
cancellation; the service remains stopped. For the default source-build paths,
start it explicitly:

```sh
node --env-file=./axis.env ./dist/bin/axis.js serve --config ./axis.toml
```

For an installed package whose `axis` command is the Node entry point:

```sh
node --env-file=./axis.env "$(command -v axis)" serve --config ./axis.toml
```

Use the wizard's printed command for custom paths. Initialization neither
installs nor starts a persistent service. Created files remain on failure or
Ctrl-C; follow [manual recovery](SINGLE-HOST-DEPLOYMENT.md#initialization-health-and-manual-recovery)
before retrying. For an existing or scripted deployment, continue with the
explicit `init --config` examples below; those paths make no network requests
and preserve custom TOML while generating keys.

## Run on the host

After successful initialization, use the wizard's printed start command. With
default source-checkout paths:

```sh
node --env-file=./axis.env ./dist/bin/axis.js serve --config ./axis.toml
```

`serve` takes only `--config`; it does not load `axis.env` automatically.
Wait for `Axis serve ready`, then check aggregate readiness:

```sh
curl --fail http://127.0.0.1:8787/health/ready
```

`GET /health/live` checks process liveness. Relay opens last; SIGINT/SIGTERM
stop ingress and channel loops before closing Worker, Node and Router. Default
internal listeners use `127.0.0.1` ports 8788, 9080, 9081 and 9082; reserve
these ports as well as Relay's 8787. Configuration/key errors fail before
opening stores. Startup failure closes roles already started. Never run
`serve` and standalone services against the same stores.

In a Linear issue in the selected Team, mention **@Axis** with a concrete task,
for example “Inspect this repository and summarize how its tests run.” Watch
for the AgentSession and its Activities. A follow-up in the same AgentSession
uses its existing execution binding. Readiness and native login status are
prerequisites; this authorized turn verifies actual model execution.

## Native executor profiles

Codex uses the package-local executable. From a source checkout, authenticate
as the account that will run Axis:

```sh
node node_modules/@openai/codex/bin/codex.js login --device-auth
node node_modules/@openai/codex/bin/codex.js login status
```

Before `serve`, provision all four role files under the native Codex home's
`agents` directory: `explorer.toml`, `architect.toml`, `reviewer.toml` and
`worker.toml`. Use the existing `CODEX_HOME` when set, otherwise `$HOME/.codex`.
For a fresh account, this creates a minimal valid set without overwriting any
existing profile (review instructions and model/tool settings for your tasks):

```sh
axis_profile_dir="${CODEX_HOME:-$HOME/.codex}/agents"
mkdir -p "$axis_profile_dir"
for role in explorer architect reviewer worker; do
  if [ ! -e "$axis_profile_dir/$role.toml" ]; then
    (umask 077; set -C; cat > "$axis_profile_dir/$role.toml" <<EOF
name = "$role"
description = "Perform assigned $role work."
developer_instructions = "Complete the assigned $role task and report verified results."
EOF
    )
  fi
done
```

Axis requires every file to be readable and regular, its `name` to match the
role, and nonblank `description` and `developer_instructions`. Existing files
must satisfy the same contract. See [profile readiness](EXECUTOR-ADAPTER.md#codex-agent-profile-registry-and-readiness).

For Claude Code, install a native executable, authenticate it as the service
account, and supply its absolute path to the wizard. It probes `--help`,
`--version` and `auth status --json`; it neither installs nor logs in the CLI.
Claude uses the account's native home and requires no Codex role files.
Both executors can write files and use network tools with the service account's
reach; Axis adds no OS sandbox. See [executor capabilities](EXECUTOR-ADAPTER.md)
and [the security model](SECURITY-MODEL.md). Container/systemd restrictions
below also apply to executor children.

## Initialization health and manual recovery

The wizard reports local checks of parsed configuration, required private keys,
the Linear installation and audited Team binding. Native checks use
`--version` and `codex login status` or `claude auth status --json`; Codex also
checks the four role profiles. Unavailable executors or failed login status
are health findings to resolve before `serve`. It does not run a model request
or executor task. The final `/health/ready` reachability check is diagnostic:
the temporary setup Relay and Router have already closed, so an unreachable
service is expected until the explicit start command. A reachable service does
not prove the new configuration is running. Check aggregate readiness after
starting it and separately validate an authorized executor turn.

Interactive setup is fresh-only. Existing config/env files, required keys or
an existing installation are refused. On failure, timeout or cancellation,
temporary listeners, loops and stores close, while created files remain for
inspection. There is no automatic rollback, cleanup or resume. Preserve the
config, private env file, keys, databases and vault together; a partial config
may still have the provisional organization and empty Team allowlist.

For manual recovery, first confirm no process owns the stores and preserve the
matched state. Inspect the config and installation's organization, Team access
and revision, and use `axis team-bindings list`, `get` and `history` to inspect
the Router binding. Complete the
[manual configuration](CONFIGURATION.md#single-host-process),
[Linear installation procedure](LINEAR-PROVIDER.md), and
[executor provisioning](SINGLE-HOST-DEPLOYMENT.md#executor-authentication-and-secrets)
against that state. The wizard's `team-bindings set` uses
`--expected-revision absent` with audit metadata; an existing binding or changed
installation revision requires inspection before any manual mutation. Do not
regenerate keys, discard OAuth state or delete a surviving `.init.lock` to make
a retry pass. Inspect the lock and its owner before explicit recovery; choose
new paths for a separate fresh installation and retain the previous state.

## Local Docker image

The multi-stage Dockerfile uses Node 24.18.0 on Debian bookworm slim and copies
only compiled application code, contract schemas and production dependencies
into the runtime stage. It includes Codex 0.153.4. Repository tests, development
dependencies, local TOML, data and credentials are excluded from the runtime.
The image runs as `node` (UID/GID 1000), with data and executor state under
`/var/lib/axis`. Application files remain owned by root. Images are built
locally; no registry account or publishing step is needed.

```sh
docker build --tag axis:local .
cp deploy/docker/axis.example.toml axis.toml
```

Edit `axis.toml`: replace the fictional Linear organization, client and Team
IDs and the public HTTPS origin. Keep `dataDir`, `workspaceRoot` and `codexHome`
as absolute container paths from the example. The Relay binds `0.0.0.0:8787`
inside the container; all other listeners stay on container loopback. Only
Relay port 8787 is published, on host loopback. Put a TLS reverse proxy in front
of it for Linear webhooks and OAuth. A proxy in another container needs an
explicit shared network and a Relay upstream on that network; its loopback is
not the Axis container's loopback.

Initialize the persistent volume with the TOML mounted read-only:

```sh
docker volume create axis-data
docker run --rm --mount source=axis-data,target=/var/lib/axis \
  --mount type=bind,source="$PWD/axis.toml",target=/etc/axis/axis.toml,readonly \
  axis:local init --config /etc/axis/axis.toml
```

The existing TOML is preserved; `init` writes only generated keys and private
data directories in the volume. Do not override the image user. A new Docker
volume inherits the image directory ownership. For bind-mounted storage,
provision it for UID/GID 1000 first, with the data directory mode 0700, the
`secrets` directory mode 0700 and each key mode 0600. Axis rejects mismatched
ownership, permissive key modes and symlinks. Never share live SQLite storage
between multiple instances.

### Executor authentication and secrets

Before starting the service, provision all four reviewed native role profiles:
`explorer.toml`, `architect.toml`, `reviewer.toml` and `worker.toml`. Axis requires
readable regular files in `$CODEX_HOME/agents`; each file must have `name`
matching its role and nonblank `description` and `developer_instructions`.
For example, a minimal `worker.toml` has this shape (choose reviewed model,
tool and instruction settings for your deployment):

```toml
name = "worker"
description = "Execute assigned implementation work."
developer_instructions = "Complete the assigned work and report verified results."
```

Prepare all four profiles in `/etc/axis/codex-agents` on the host, then copy
them into the volume as the service user:

```sh
docker run --rm --mount source=axis-data,target=/var/lib/axis \
  --mount type=bind,source=/etc/axis/codex-agents,target=/run/axis-agents,readonly \
  --entrypoint sh axis:local -c 'mkdir -p -m 0700 "$CODEX_HOME/agents" && cp /run/axis-agents/*.toml "$CODEX_HOME/agents/" && chmod 600 "$CODEX_HOME/agents/"*.toml'
```

All four profiles must validate before `/health/ready` can pass, including with
`profileFallbackPolicy = "allow"`. Login alone does not satisfy profile
readiness. See [the native profile contract](EXECUTOR-ADAPTER.md#codex-agent-profile-registry-and-readiness).

Authenticate the packaged Codex CLI as the same container user with the same
persistent native home:

```sh
docker run --rm -it --mount source=axis-data,target=/var/lib/axis \
  --entrypoint codex axis:local login --device-auth
```

Alternatively provision native credential files into the private
`/var/lib/axis/codex` directory using Codex's supported login workflow.
`CODEX_HOME` is writable because Codex also stores configuration, session and
runtime state there; do not make the entire native home read-only. Scope
credentials to this service. Do not copy an operator's whole home into the
image or mount a Docker socket. Native login status and `/health/ready` do not
prove that a model request, workspace write or network tool will succeed;
validate an authorized executor turn before admitting real tasks.

The stock image supports Codex. Claude Code is not bundled: selecting
`kind = "claude-code"` requires an operator-provisioned Linux-compatible
executable, its runtime dependencies, an absolute `claudeExecutable` path in
TOML, and authentication under the container's writable native home. Use a
reviewed derived image or dedicated mount for that executable; do not mount a
macOS binary. Native credentials must be supplied at runtime, never through a
Dockerfile, build arguments, image layers or committed files.

Axis reads `LINEAR_CLIENT_SECRET` and `LINEAR_WEBHOOK_SECRET` from the process
environment. Supply them from a secret manager or a local environment file
outside the repository, owned by its operator with mode 0600. For
`docker run --env-file`, use a separate file with unquoted values:

```dotenv
LINEAR_CLIENT_SECRET=example-client-secret
LINEAR_WEBHOOK_SECRET=example-webhook-secret
```

Do not pass the wizard's single-quoted `axis.env` directly to
`docker run --env-file`: Docker preserves those quote characters in the secret.
Compose, systemd and Node accept the wizard's file as described
[in Configuration](CONFIGURATION.md#linear-environment-file).

```sh
docker run --detach --name axis --init --restart unless-stopped \
  --stop-timeout 30 --cap-drop ALL --security-opt no-new-privileges:true \
  --publish 127.0.0.1:8787:8787 \
  --env-file /etc/axis/container.env \
  --mount source=axis-data,target=/var/lib/axis \
  --mount type=bind,source="$PWD/axis.toml",target=/etc/axis/axis.toml,readonly \
  axis:local
docker inspect --format '{{.State.Health.Status}}' axis
docker exec axis node deploy/docker/healthcheck.mjs /etc/axis/axis.toml
```

The default entry point is `axis serve --config /etc/axis/axis.toml`.
The healthcheck reads that file's Relay port and calls the existing
`GET /health/ready` endpoint over loopback, with a six-second timeout. It accepts
only HTTP 200, follows no redirects and prints no configuration or credentials.
If you override the configuration path, override the healthcheck path too.
Complete Linear's operator-authorized OAuth installation before admitting work.

## Docker Compose

The host wizard's paths and stores are not automatically available in a
container. For a fresh container-only instance, use the scripted provisioning
below, then complete [manual Linear installation](API.md#oauth-and-health)
and the Team binding before sending tasks. To move a completed host-wizard
installation, stop it and preserve its entire matched data/native-state set;
copy it into the volume with UID/GID 1000 ownership, translate the host paths
to container paths, and set `[relay].host = "0.0.0.0"`. Keep its organization,
Team binding, transport keys and OAuth vault unchanged. Do not re-run `init`
against existing keys. Native host binaries and credentials may require
container-specific provisioning. Use the [configuration reference](CONFIGURATION.md#single-host-process)
for path fields and [backups](#updating-and-backing-up) for consistency.


Compose uses a project-scoped `axis-data` volume, separate from the explicitly
named volume above. Use one deployment method for each instance. From the
repository root, prepare `axis.toml` as above and export the two Linear secrets
from your secret manager into the shell running Compose. Do not store them in
TOML or commit an environment file. Alternatively, use the wizard's
single-quoted `axis.env` as Compose's `.env` interpolation file, or pass
`--env-file ./axis.env` to each Compose command, for example
`docker compose --env-file ./axis.env up --detach --wait --wait-timeout 120`.
Exported shell values take precedence over file values; ensure they select the
intended secrets. The sequence below is for a fresh scripted deployment; skip
key initialization if the selected data directory already holds its keys.
Compose passes the two secret variables at runtime; empty values allow
configuration inspection but Axis refuses startup without secrets.
`docker compose config` can print resolved secrets; use `--quiet` for validation.

```sh
docker compose config --quiet
docker compose build
docker compose run --rm axis init --config /etc/axis/axis.toml
docker compose run --rm --volume /etc/axis/codex-agents:/run/axis-agents:ro \
  --entrypoint sh axis -c 'mkdir -p -m 0700 "$CODEX_HOME/agents" && cp /run/axis-agents/*.toml "$CODEX_HOME/agents/" && chmod 600 "$CODEX_HOME/agents/"*.toml'
docker compose run --rm --entrypoint codex axis login --device-auth
docker compose up --detach --wait --wait-timeout 120
docker compose ps
docker compose exec axis node deploy/docker/healthcheck.mjs /etc/axis/axis.toml
```

`init: true` reaps executor children, and shutdown allows 30 seconds for the
CLI's SIGTERM cleanup. `docker compose down` retains the data volume; do not use
`down --volumes` on a persistent deployment. Docker marks readiness failures
unhealthy but does not restart an unhealthy running process automatically;
monitor that state separately from the process restart policy.

For task repositories and additional tools, provision a workspace volume owned
by UID/GID 1000 and install the required Linux tools in a reviewed derived image.
The image provides the Axis/Codex runtime, not every possible task toolchain.

## Linux systemd

The [sample unit](../deploy/systemd/axis.service) expects Node 24 at
`/usr/bin/node` and the built production package at `/opt/axis`, including
production `node_modules` and the contracts workspace's compiled output and
schemas. Keep the package root-owned and readable/executable by the service
user. If Node is installed elsewhere, update `ExecStart` and `PATH` explicitly;
a login shell's version-manager configuration does not apply to systemd.

Create a dedicated service account and writable state before initialization:

```sh
sudo useradd --system --home-dir /var/lib/axis --shell /usr/sbin/nologin axis
sudo install -d -o root -g axis -m 0750 /etc/axis
sudo install -d -o axis -g axis -m 0700 /var/lib/axis \
  /var/lib/axis/data /var/lib/axis/home /var/lib/axis/codex \
  /var/lib/axis/workspaces
sudo install -o root -g axis -m 0640 deploy/docker/axis.example.toml /etc/axis/axis.toml
sudo install -o root -g root -m 0600 deploy/systemd/axis.env.example /etc/axis/axis.env
```

Edit `/etc/axis/axis.toml` with the real nonsecret deployment values and
`/etc/axis/axis.env` with the real Linear secrets. The same absolute paths from
the Docker sample work here. The system manager reads the root-only
`EnvironmentFile` before dropping privileges. The wizard's single-quoted
`NAME='literal'` file is compatible with this directive; provision it at
`/etc/axis/axis.env` with the same root-only ownership and permissions. Use
systemd environment-file syntax, without `export`; do not treat the file as a
shell script. A completed wizard installation already has keys; preserve them
with its configuration and data instead of repeating initialization.

Prepare the four reviewed profiles in `/etc/axis/codex-agents` as described
above. Install them into the native home, initialize keys and authenticate the
native executor as `axis`, never root:

```sh
sudo install -d -o axis -g axis -m 0700 /var/lib/axis/codex/agents
sudo install -o axis -g axis -m 0600 /etc/axis/codex-agents/*.toml /var/lib/axis/codex/agents/
sudo -u axis env HOME=/var/lib/axis/home CODEX_HOME=/var/lib/axis/codex \
  /usr/bin/node /opt/axis/dist/bin/axis.js init --config /etc/axis/axis.toml
sudo -u axis env HOME=/var/lib/axis/home CODEX_HOME=/var/lib/axis/codex \
  /usr/bin/node /opt/axis/node_modules/@openai/codex/bin/codex.js login --device-auth
sudo install -o root -g root -m 0644 deploy/systemd/axis.service /etc/systemd/system/axis.service
sudo systemctl daemon-reload
sudo systemctl enable --now axis.service
sudo systemctl status axis.service
curl --fail http://127.0.0.1:8787/health/ready
```

The unit restarts failed processes, uses a private temporary directory, denies
privilege escalation and capabilities, and makes the filesystem read-only
except `/var/lib/axis`. It hides host home directories. Keep task workspaces
and executor state under `/var/lib/axis`; explicitly extend `ReadWritePaths`
and ownership if the approved deployment needs another writable root. These
restrictions also apply to executor children and may need a reviewed override
for a task-specific sandbox or toolchain. `Type=simple` reports process startup,
so use the readiness endpoint for availability monitoring. Logs go to the
journal; do not enable credential or raw native-output logging.

For separate services or additional hosts, follow
[multi-host advanced deployment](MULTI-HOST-DEPLOYMENT.md).

## Updating and backing up

Keep new Linear tasks idle and let active turns and delivery outboxes settle.
There is no `serve` drain subcommand. Check pending work with the operator
[inspection commands](API.md#operator-cli), then stop the whole instance.
For Compose use `docker compose stop`; for systemd use
`sudo systemctl stop axis.service`; for a foreground process send SIGTERM or
Ctrl-C. Wait for exit before copying state.
Back up `axis.toml`, the private Linear env file, the entire `dataDir` (keys,
vault, every SQLite database and any WAL/SHM companions), and native executor
state together. For Codex preserve the configured `codexHome` or native account
home; for Claude preserve the private native-session store and transcripts in
the recorded workspace. Retain workspace content needed by those sessions.
Record the reviewed binary revision and keep the matched backup private. Rebuild the image or replace the
root-owned systemd package from the reviewed revision and lockfile, then restart
and check readiness and an authorized executor turn. A binary rollback across
a storage schema change requires the matching pre-upgrade backup; never run
old and new binaries concurrently on the same volume.

The Docker base uses an exact Node release tag, and `npm ci` consumes the
lockfile, including exact Codex 0.153.4. Tags can be republished: deployments
requiring immutable operating-system bytes should record and pin the reviewed
base-image digest and retain the built image. This example does not claim
bit-for-bit reproducibility across mutable upstream base tags.

## Logs and troubleshooting

```sh
docker compose logs --tail 100 axis
sudo journalctl --unit axis.service --lines 100 --no-pager
```

A foreground `serve` writes readiness and sanitized audit events to stdout,
and fixed startup/shutdown or loop-failure codes to stderr. Do not enable raw
native-output or credential logging.

| Symptom | Check and action |
| --- | --- |
| Interactive setup refuses to start | Run from a real terminal with new config/env paths; explicit `--config` alone selects scripted initialization. Preserve partial state and use [manual recovery](#initialization-health-and-manual-recovery). |
| `axis.serve.startup_failed` | Check TOML, private key ownership/modes and port conflicts. Confirm both Linear secrets are in the process environment. Check executor login and all four Codex profiles. |
| Compose cannot reach Relay | Use the container example's `0.0.0.0` Relay host, publish only host loopback 8787, and preserve container paths and UID/GID 1000 ownership. |
| Readiness fails | Probe `/health/ready`; check executor availability/profiles and storage access. Container health does not itself restart a running unhealthy process. |
| OAuth callback fails | The public origin must reach the temporary Relay during setup. Use exactly `/oauth/linear/callback`, valid TLS and the matching client secret. |
| Mention produces no work | Check the issue is in the chosen Team, app mentionable capability, installed Team access/binding, webhook URL `/webhooks/linear`, signing secret, and delivery status in Linear. |
| A follow-up cannot resume | Preserve the journal and native state together. Never delete journals or launch replacement work to hide uncertainty; see [recovery](PERSISTENCE.md). |

For upgrades and incidents with multiple independent owners, see
[deployment and rollback](MULTI-HOST-DEPLOYMENT.md#deploy-and-rollback).
