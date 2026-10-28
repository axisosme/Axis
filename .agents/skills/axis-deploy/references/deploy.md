# Setup and verification

Read this for a new deployment. Start with the
[deployment guide chooser](../../../../docs/DEPLOYMENT.md), then load only the
selected deployment sections. Commands assume the repository root; substitute
the selected instance's actual nonsecret paths.

## Choose and preflight

Agree the host, service account, existing writable task workspace, executor,
public HTTPS origin and deployment shape with the user:

| Shape | Source of truth |
| --- | --- |
| Foreground, one host | [Interactive setup and host run](../../../../docs/SINGLE-HOST-DEPLOYMENT.md#interactive-single-host-setup) |
| Docker Compose | [Container provisioning and Compose](../../../../docs/SINGLE-HOST-DEPLOYMENT.md#docker-compose) |
| Linux systemd | [Account, paths and sample unit](../../../../docs/SINGLE-HOST-DEPLOYMENT.md#linux-systemd) |
| Multi-host | [Topology and startup](../../../../docs/MULTI-HOST-DEPLOYMENT.md), including [managed macOS install](../../../../docs/MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade) |

Run these checks yourself, in the target service context where applicable:

1. Check `node --version` is Node 24, `npm --version`, `git --version`, checkout
   status and `git rev-parse HEAD`. The first `node` on the service account's
   `PATH` may be another tool's runtime; when it is not Node 24, give the user
   and every start command the absolute path of a Node 24 binary. Check free disk space with `df -h .` and on
   selected state/workspace volumes. Allow space for dependencies, the build,
   task files and a complete backup; the docs specify no fixed minimum.
   For a source deployment, `npm install` builds through its prepare hook.
2. Inspect listening sockets for Relay 8787, Router 8788 and internal ports
   9080, 9081 and 9082 before a fresh default single-host start. Wizard setup
   needs both 8787 and 8788 free. For custom or multi-host topology, derive
   listeners from TOML. Identify existing owners rather than killing them.
3. Verify the task workspace exists and is writable by the runtime account.
   For containers, check mounted container paths and UID/GID 1000, plus needed
   task tools; a writable host path alone is insufficient. For systemd, check
   the unit's allowed write paths and explicit Node path.
4. Follow [native executor setup](../../../../docs/SINGLE-HOST-DEPLOYMENT.md#native-executor-profiles)
   and [profile readiness](../../../../docs/EXECUTOR-ADAPTER.md#codex-agent-profile-registry-and-readiness).
   Codex uses the package-local binary: check
   `node node_modules/@openai/codex/bin/codex.js --version` and
   `node node_modules/@openai/codex/bin/codex.js login status`.
   Verify readable regular role files named `explorer.toml`, `architect.toml`,
   `reviewer.toml` and `worker.toml` in the effective native Codex home's agents
   directory, with matching names and nonblank descriptions/instructions.
   For Claude Code, verify its existing absolute executable path using its
   help/version and `auth status --json` probes. Discard authentication probe
   output; retain only success/failure. Have the user complete native login as
   the service account if needed. Do not inspect credential files.
5. Choose [Cloudflare Tunnel, ngrok or a TLS reverse proxy](../../../../docs/WEBHOOK-EXPOSURE.md)
   with the user. Check its prerequisites and keep it running for OAuth setup
   and normal operation. Forward only the documented callback/webhook routes
   to Relay, with valid public HTTPS and no browser-login middleware. Keep
   internal ports private. Have the user provision tunnel credentials in their
   own terminal; never pass real tokens through agent commands.

## Human terminal and Linear setup

For a fresh host installation, give the user these instructions. Do not launch
the secret-prompting wizard in an agent terminal or use terminal capture while
it runs. For custom paths, follow
[initialization modes](../../../../docs/CONFIGURATION.md#initialize).

1. In your own terminal at the built repository root, run
   `node ./dist/bin/axis.js init`. Existing config/env files or keys are refused;
   do not copy a template first. Enter the public HTTPS origin, without a path,
   credentials, query or fragment.
2. Open **Linear Settings > API > OAuth applications** and create an app named
   **Axis**. Use the exact callback and webhook URLs printed by the wizard:
   the chosen origin plus `/oauth/linear/callback` and `/webhooks/linear`.
   Enable **Assignable** and **Mentionable** agent capabilities; subscribe to
   **AgentSessionEvent** (`created`, `prompted`), **PermissionChange**
   (`teamAccessChanged`) and **OAuthApp** (`revoked`). Enable scopes `read`,
   `write`, `app:assignable` and `app:mentionable`.
3. Enter the client ID, client secret and webhook signing secret directly into
   the wizard's prompts. Do not send these values to the agent. Open its
   authorization URL and install Axis in the intended Linear workspace within
   ten minutes, granting the intended Team access. Choose that accessible Team
   in the wizard, then Codex or Claude Code, the existing absolute workspace
   directory, and Claude's absolute executable path when applicable.
4. Tell the agent whether setup succeeded and any sanitized health finding.
   Keep configuration, the private env file and generated state uncommitted.
   The wizard creates the audited Team binding and closes its temporary Relay
   and Router; it starts no persistent service or model turn. A stopped service
   being unreachable at the end of setup is expected.

For Compose/systemd, follow the chosen guide's provisioning sequence instead
of applying host-wizard paths directly. Fresh scripted provisioning still
requires the guide's manual Linear installation and Team binding; do not mark
it ready before these are complete. A host-wizard installation can be moved
only with approved stop/move operations and its entire matched state, translated
container/service paths and correct ownership. Keep secret entry with the user.
The [environment-file reference](../../../../docs/CONFIGURATION.md#linear-environment-file)
explains Node, Compose and systemd loading. Axis does not load `axis.env`
automatically; do not print resolved Compose configuration or use shell tracing.
Multi-host managed `axis install` is macOS LaunchAgent management, not a
replacement for the Linux systemd procedure.

## Start and prove operation

Use the wizard's printed start command. For default source-checkout paths:

```sh
node --env-file=./axis.env ./dist/bin/axis.js serve --config ./axis.toml
```

For Compose or systemd, use the selected guide's start and status commands;
keep its persistent volumes, service account and executor environment intact.
For multi-host, follow its startup/validation procedure for all owners.

Wait for readiness, then run on the Relay host:

```sh
curl --fail http://127.0.0.1:8787/health/ready
```

Use the configured local Relay address if changed. Readiness and native login
checks make no model request. Ask the user to mention **@Axis** in an issue in
the selected Team, or delegate the issue to the app, with a concrete harmless
task that leaves checkable evidence, such as “Create `hello.md` in your
workspace containing the output of `hostname`.” Confirm that the AgentSession
receives Activities, the executor completes a turn and the evidence exists.
Then reply once in the same session and confirm the follow-up runs as a new Run
of the same Task on the same native session. Use available read-only
Linear access, or have the user confirm the Activities and result if unavailable.
Disclose user-confirmed evidence. Keep the forwarder running throughout.

For failures, use the [troubleshooting table](../../../../docs/SINGLE-HOST-DEPLOYMENT.md#logs-and-troubleshooting):
check TTY/fresh paths for wizard refusal, TOML/key modes/ports/environment for
startup, container listeners/ownership, executor profiles and storage for
readiness, callback TLS/secret consistency, and Team access/binding, mentionable
capability, webhook URL/signature and delivery status for missing work. Keep
credential checks in the user's terminal. Preserve journals and native state
for follow-up failures; use the guide's manual recovery rather than resetting.

A Run that fails with `codex_turn_failed` reached Codex: the native turn itself
failed, and Axis routing, the Worker and the workspace worked. Do not open
native Codex session files; they hold raw task content and tool output. Ask the
user to check the failed turn's error in their own Codex tooling. A common
cause is a model, selected by the service account's Codex configuration, that
the bundled Codex cannot use with that account; the user chooses a supported
model rather than you editing their configuration.
