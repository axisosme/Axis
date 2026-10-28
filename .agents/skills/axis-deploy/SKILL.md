---
name: axis-deploy
description: Deploy, install, set up, upgrade, update, back up, roll back, or troubleshoot an Axis deployment from this repository. Use for operating Axis for a user, not for developing Axis itself.
---

# Deploy and maintain Axis

Take the user's chosen deployment to a running service with readiness and a
real Linear executor turn verified. Use the current checkout's
[deployment guides](../../../docs/DEPLOYMENT.md) and CLI help as authority;
this skill coordinates them rather than defining another deployment method.
Run commands from the repository root. In a built source checkout, `axis`
means `node ./dist/bin/axis.js`; inspect `axis --help` before acting.

## Authority and private state

- Confirm the target host, checkout, service account and intended operation.
  Inspect current state yourself; ask only for missing decisions or human steps.
- Before stopping a running service, deleting or moving anything, or changing a
  service manager, present the exact targets, effect and recovery plan and obtain
  user approval. Reuse explicit approval for those exact operations. A request
  to upgrade alone does not approve abandoning work.
- `axis upgrade --force` needs explicit user approval: it abandons in-flight
  work, archives selected role storage and has no rollback. Never select it to
  bypass a refused ordinary upgrade.
- Run interactive `axis init` only in the user's own terminal. Have the user
  enter the client ID and secrets directly. Never read, echo, store or paste
  secret values, place them in URLs or command lines, or open private env,
  key, vault or native credential files in agent tools. Runtime secret loading
  and approved opaque backup copies do not require exposing their contents.
  Never commit `axis.toml`, `axis.env`, the data directory or backups.
- Keep one active owner per store. Never run old and new binaries, or `axis serve`
  and standalone services, against the same data. Preserve matched configuration,
  keys, vault, journals and native executor state for recovery.

## Choose the path

1. For a fresh deployment, read [setup and verification](references/deploy.md).
   Choose with the user: foreground `axis serve` on one host, Docker Compose,
   Linux systemd, or multi-host. Keep an existing service manager unless a change
   is approved. Use only the selected guide's paths and process environment.
2. For an upgrade, update, backup or rollback, read
   [maintenance and recovery](references/maintenance.md) before any mutation.
   Single-host checkout updates and managed macOS `axis upgrade` are different
   lifecycles. Identify which is in use first.
3. For a failed setup or deployment, inspect the
   [troubleshooting table](../../../docs/SINGLE-HOST-DEPLOYMENT.md#logs-and-troubleshooting)
   and [initialization recovery](../../../docs/SINGLE-HOST-DEPLOYMENT.md#initialization-health-and-manual-recovery).
   Preserve partial state. Do not regenerate keys, clear journals, retry the
   fresh-only wizard over existing state, or launch replacement work to hide
   uncertain execution. If the guide leaves a required step undefined, report
   that gap before attempting it.

## Completion report

Report privately to the user, without credentials or raw executor output:

- Operation and deployment shape; target host, service account and checkout.
- Running revision (and retained previous revision for an upgrade).
- Configuration/data locations and exact start/stop commands for this instance.
- Matched backup location and the applicable restore procedure.
- Readiness result and real Linear turn/Activities result.
- Anything not verified, any remaining human step, and the next action.
