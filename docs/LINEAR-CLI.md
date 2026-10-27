# Axis credentials for Linear CLI

This mode authenticates the CLI to the Axis installation workspace. It does not
bind a request to a Task, Worker or Run and does not choose an Agent's commands.
The existing execution-bound mode remains separate and optional.

## Relay configuration

Set `apiEnabled = true` in `[linear]` and add `[[linear.cliGrants]]`
records containing exactly `credentialId`, `credentialHash`,
`organizationId`, `appUserId`, and `expiresAt`. Generate a separate random
`axlcli1_` credential with 32 random bytes encoded as unpadded base64url for each
installation of the CLI. `credentialHash` is its lowercase SHA-256 hex digest;
Relay stores the digest, never the CLI secret. Use an explicit ISO expiration
and opaque credential ID. The grant authorizes all resources visible to the
named Axis application installation in that workspace, independently of Runs.

Relay rechecks the active installation and the OAuth viewer before forwarding.
OAuth access/refresh tokens remain in the existing Relay vault. Linear owns
resource permissions. Remove a grant and replace the managed configuration through
[the host lifecycle procedure](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade),
or restart standalone Relay with the updated TOML, to revoke that CLI
credential; replacing the grant rotates access. In-flight operations cannot be undone.
Do not retry ambiguous writes automatically.

## Fresh client installation

1. Install the agreed `schpet/linear-cli` version and resolve its actual binary
   before installing a launcher. Verify dedicated command help and `api --help`.
2. Install the Axis package containing `dist/bin/linear-cli.js`.
3. As the Agent's OS account, create `~/.axis/linear-cli` with mode `0700` and
   `config.json` with mode `0600`, owned by that account. The JSON contains
   `executable` (absolute original CLI binary), `endpoint` (HTTPS Relay origin
   plus `/api/linear/graphql`), and `credential` (the newly generated secret).
   Loopback HTTP is supported only for local verification.
4. Install a `linear` launcher on the effective Agent PATH that executes the
   package's `dist/bin/linear-cli.js` with the deployment's absolute Node 24
   executable and forwards all arguments unchanged. Preserve the original
   launcher/link for rollback. Never point `executable` back at the launcher.
5. Run `linear --version`, `linear api --help`, and
   `linear api '{ viewer { id name organization { id name } } }'` as that OS
   account. Compare the returned identity with the configured Axis App User and
   workspace. Run a dedicated read such as `linear team list` as well.

The launcher reads its own file on every call and supplies `LINEAR_API_KEY` and
`LINEAR_GRAPHQL_ENDPOINT` unconditionally, so it overrides whatever it inherited:
a personal value, an unavailable Run's sentinel, and equally an issued per-Run
execution credential. On a host where this launcher is installed, the
identity a `linear` call uses is this file's, never the Run's. A missing,
invalid, symlinked, incorrectly owned or overly permissive
config fails before launching the underlying CLI. CLI 2.5.0 rejects
`--workspace` while this key is set, preventing selection of a personal account.
No `linear auth login` or distribution of the Relay's OAuth tokens is needed.
The shared OS account is the trust boundary; this is not a sandbox against an
Agent deliberately replacing binaries, reading credentials or changing config.

## Operator verification

First pass local automated authentication tests with a real CLI and synthetic
upstream. Then verify the configured local CLI against the live Axis installation
using read-only calls. Only after local success configure other authorized
machines and repeat the checks for each effective Agent account and PATH.

Every subsequent deployment checks executable resolution, version/help,
configuration ownership/mode, authenticated viewer/workspace, and a dedicated
read command before and after activation. Record safe results only. Do not
print credentials, auth-token output, environment dumps, or configuration bodies.
A binary-only check does not prove CLI readiness. Do not submit Agent tasks or
write Linear objects merely to verify deployment.

The CLI check establishes standalone CLI authentication. Native executors that
remove shell tools or use a separate daemon PATH need their own tool availability
verification; CLI installation does not establish that an Agent invoked it.
