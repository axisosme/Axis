# Maintenance and recovery

Read this before an update, upgrade, backup or rollback. First identify the
running revision, service manager, owners, configuration, env-file location,
whole data directory, native state and task workspace. Use metadata and
nonsecret configuration; copy private files opaquely without exposing values.
Inspect checkout changes and confirm the reviewed target revision with the
user. Do not discard local modifications or guess a release to install.
Present the concrete stop/backup/replacement/restore plan for approval under
the skill's authority rules before disrupting the service.

## Single-host checkout, Compose or systemd

Follow [Updating and backing up](../../../../docs/SINGLE-HOST-DEPLOYMENT.md#updating-and-backing-up)
in this order:

1. Keep new Linear tasks idle; let active turns and delivery outboxes settle.
   Use the operator inspection commands linked by that section to assess work.
   There is no drain subcommand for the single-process service. Do not mistake
   readiness for proof that work is idle.
2. With approval, stop the entire instance: Ctrl-C/SIGTERM for foreground,
   `docker compose stop` for Compose, or `sudo systemctl stop axis.service`
   for the documented unit. Wait for process exit and confirm no owner still
   has the stores open before copying them.
3. Create a private matched backup of `axis.toml`, the private Linear env file,
   the **entire data directory** (all keys, vault, SQLite databases and any
   WAL/SHM companions), and native executor state together. For Codex include
   configured codexHome or the native account home. For Claude include the
   private native-session store and transcripts in the recorded workspace.
   Preserve workspace content required by those sessions. For Compose copy the
   actual project-scoped volume, not a different similarly named volume.
   Preserve file modes/ownership and keep backups outside the source checkout.
   Record `git rev-parse HEAD` and retain the previous package/image and exact
   configuration alongside the matched backup. Verify the backup inventory and
   that it is readable without displaying its private contents. A backup-only
   operation ends by restarting the same revision and verifying it below.
4. Update the checkout to the agreed reviewed revision without overwriting
   local work; record its exact revision. For a source checkout run
   `npm install` to rebuild. For Compose rebuild the image, or for systemd
   replace the root-owned package, from the reviewed revision and lockfile as
   the guide specifies. Do not run initialization against existing keys.
5. Restart with the same manager and environment. Run
   `curl --fail http://127.0.0.1:8787/health/ready` (or the configured Relay
   address), then verify an authorized real Linear executor turn and its
   Activities as in [deployment verification](deploy.md#start-and-prove-operation).

Each store accepts only its own current schema. Axis creates complete schemas
for fresh stores and performs **no conversion of existing data**. See
[deployment and rollback](../../../../docs/MULTI-HOST-DEPLOYMENT.md#deploy-and-rollback).
If the candidate refuses existing data, stop it, restore the matching backup
and previous revision as below, and report the refusal. Do not delete stores,
change schema ledgers, or use a forced upgrade to make the data acceptable.
Never run old and new binaries concurrently on the same data.

## Managed macOS and multi-host upgrades

Read [host-local install and upgrade](../../../../docs/MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade)
and [coordinated deploy/rollback](../../../../docs/MULTI-HOST-DEPLOYMENT.md#deploy-and-rollback).
Keep Relay/Router, Node, Worker and API clients on the same build in one matched
maintenance operation. Coordinate all owners and backups; a Node does not
start, stop or upgrade separate Workers.

For a managed Node, use `axis node drain --host-root <absolute>` and
`axis node status --host-root <absolute>` and wait for the documented idle
evidence. Build the reviewed source package, then use
`axis upgrade --host-root <absolute> --source-package <absolute>` separately
for each installed role as the guide directs. Upgrade reads the pinned role,
configuration and selectors; it takes no role/configuration/selector flags.
Keep the TOML's exact bytes intact; configuration replacement requires the
guide's fresh-host-root procedure with approved stops/moves and retained roots.

Ordinary upgrade checks integrity and pending work, backs up matched local
state, activates and verifies the candidate. Node upgrade checks idle before
staging and after stop, resumes only after success, and restores the previous
release on rollback while leaving the Node drained. Inspect the receipt and
`axis node status --host-root <absolute>` before deciding whether recovery is
complete; do not resume a failed upgrade simply to clear the drain. Local
managed backups do not replace the coordinated whole-deployment backup or
preservation of external OAuth secrets and native executor state.

`axis upgrade --force` abandons in-flight work, archives a Node journal or
Worker database with WAL/SHM (no controller storage), skips drain and provides
**no rollback**. Obtain explicit approval for those effects and use only the
guide's failure/receipt/manual-recovery procedure. A failure may leave archived
files and a stopped or inconsistent installation; stop and report the exact
stage rather than repeatedly forcing it.

## Roll back or restore a backup

1. Identify the exact pre-upgrade revision/package/image and its matching
   private backup. Verify that configuration, env file, all stores/keys/vault,
   native state and required workspace content belong to the same snapshot.
   For multiple owners, coordinate them as a set; never restore Router or
   Worker independently or replay a Node outbox into another Router snapshot.
2. Present the exact service stops, data moves/replacements and restore targets
   for approval. Quiesce work again, stop every affected owner and wait for
   exit. Preserve the failed candidate's complete state separately before
   replacing anything; do not delete evidence or overlap old/new owners.
3. Restore the previous reviewed revision and **the complete matched backup**
   together, including WAL/SHM companions and correct permissions/ownership.
   Rebuild the source with `npm install`, or select the retained image/package.
   For managed installations follow the guide's matched state/plist/pinned
   configuration recovery; there is no standalone rollback CLI to invent.
   If restoration fails or ownership is uncertain, keep the service stopped
   and report the gap.
4. Restart the matching owners and environment, verify readiness and an
   authorized executor turn. Native continuation still requires adapter
   attestation; a restored snapshot does not prove an in-flight session can
   resume. Preserve refusal evidence and report unresolved native work for
   supervised recovery instead of dispatching a replacement automatically.
