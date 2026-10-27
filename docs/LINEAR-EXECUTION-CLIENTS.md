# Linear CLI and SDK execution access

This document describes the optional Run-bound mode. For a CLI configured once
with its own Axis credential, use [standalone CLI setup](LINEAR-CLI.md); that
mode does not require these Worker settings or Run identities.

An enabled Codex Worker obtains a short-lived Relay capability for its admitted
Run, starts a separate native app-server process, and supplies a CLI and official
SDK environment. An enabled Claude Code Worker obtains the same capability and
supplies the same environment to the native CLI process it spawns for that turn.
Claude continuations use `--resume` against the native transcript under the
forwarded `HOME`. Axis does not set `CLAUDE_CONFIG_DIR` or isolate other local
credentials. `nodes.workers[].codexHome` optionally selects the Codex home;
otherwise the inherited `CODEX_HOME` or native account default applies.
Only Relay reads Linear OAuth tokens. Runtime/Node/Worker wire envelopes and
persistent journals contain no execution credential.

## Supported execution boundary

| Surface | Support boundary |
| --- | --- |
| Codex `start`, `observe`, `cancel` | One process and one durable thread per Run. |
| Codex `continue` | A successor Run issues a fresh credential, starts its own process, and resumes the predecessor's durable thread. |
| Worker restart | Active native Runs cannot be rebound. A terminal or waiting predecessor needs adapter attestation before a successor may continue. |
| Node command | Relay checks the authenticated Node's retained reservation before delivering to the selected Worker; unknown or mismatched reservations fail closed. |
| Claude Code `start` and `continue` | One credential per turn, revoked at settlement. Continuation resumes the native session using its new credential and the native service environment after deployment/Linear credential scrubbing. |
| Live authorship, notifications, and feedback loops | Require controlled verification on the target deployment. |

Local fixtures and synthetic upstreams do not establish live Linear authorship, notifications, or multi-host behavior. Verify those separately on controlled objects and record the native, CLI, SDK, Node, and Axis versions used.

The app-server process is non-interactive. Axis therefore starts/resumes every
thread and starts every turn with approval policy `never` and
`danger-full-access`. Axis supplies no filesystem sandbox, writable-root list,
network restriction, or native approval prompt. Unexpected approval RPCs remain
a fail-closed protocol error because this posture should never emit one.

The default capability lifetime is five minutes; fifteen
minutes remains its maximum. Expiry stops new Linear operations even if a Run
continues computing. There is no automatic renewal or mutation replay: a single
turn that outlives its credential fails closed with the Relay `expired` code and
must not fall back to a personal identity or replay a write whose outcome is
unknown. Raising the ceiling would require an `axis-contracts` change to carry a
requested lifetime. A *later turn* is a different
matter: it is a new Run with its own credential, so waiting between turns costs
nothing. Disabling execution access restores the ordinary Codex lifecycle
capabilities, while keeping Linear personal-credential fallback disabled.

## Continuation across Runs

A follow-up message in one Linear AgentSession arrives as a `continue` command
for a new Run. Each Run is a distinct execution with its own credential; the
prior Run's credential is revoked at its terminal event and is never reused.

Continuation therefore cannot work by keeping a process alive. It works because
the Codex thread outlives the process:

1. the successor Run issues its own Relay credential;
2. it starts its own app-server process carrying only that credential;
3. it resumes the prior Run's thread with that credential supplied as the
   thread's `shell_environment_policy.set` override; and
4. it starts the next turn on that same `threadId`.

The app-server transport requires these properties:

- **The thread must be durable.** A thread started with `ephemeral: true` is
  never written to disk, and both `thread/resume` and `thread/fork` reject it
  with `no rollout found`. Linear Runs therefore use durable threads. This is the
  required persistence boundary for continuation.
- **The thread must not already be loaded.** app-server *rejoins* a thread that
  is still loaded and silently discards the `config` override, returning success
  while the thread keeps its previous environment. The transport unsubscribes
  before resuming so that rejoin cannot happen. In the shipped flow the prior
  process is already closed and the successor's process has never loaded the
  thread, so the unsubscribe reports `notLoaded` and releases nothing; it guards
  a shape the current composition does not produce. Note what is and is not
  checked: the transport verifies the resumed thread's identity, not that the
  new `config` took effect, so the protection here is the ordering, not a
  detection of the failure.

`turn/start` accepts no configuration of its own, so there is no per-turn
override and no way to rotate a credential inside a running turn.

### Durable threads and on-disk history

`CODEX_HOME` holds credential snapshots and durable conversation rollouts.

**Credential snapshots.** Codex writes the app-server
child's environment to `$CODEX_HOME/shell_snapshots/<threadId>.<ns>.sh`, mode
`0644`, once per Run. Because the Worker supplies the Relay capability through
that environment, the capability is in that file in cleartext. An `ephemeral: true` thread also writes this snapshot. Supplying a value only through the thread `config` does keep it
out of the snapshot, but the Worker uses both channels, so that alone does not
protect the credential.

**Conversation rollouts.** A durable thread retains its
rollout under `$CODEX_HOME/sessions/**`. The
thread `config` is not part of the rollout, but agent command output is, so a
command whose output contains a credential persists it for the rollout's life.
Codex has the full host account's access, so every later agent on that account
can read earlier Runs' rollouts.

The Worker uses TOML `nodes.workers[].codexHome` when supplied, otherwise the
inherited `CODEX_HOME` or the service account's normal `~/.codex`. Sharing the normal home is
intentional so configuration, skills, plugins, authentication and durable
rollouts match the interactive Codex experience. Ageing files
out is still a retention policy because the credential window is
measured in minutes. The exposure is otherwise bounded by the five-to-fifteen
minute capability lifetime and by terminal revocation, and the agent is
instructed never to print a credential.

An active Run cannot recover its credential or native turn after Worker restart.
A terminal or waiting predecessor can authorize a successor only after the
adapter attests its retained thread, as described in [Persistence](PERSISTENCE.md).

## Operator configuration

Keep the existing Relay/Node/Worker authentication, workspace catalogs and
installation setup. Supply the following independently of task instructions.

In the shared TOML, configure workspace consent under `[linear]`:

```toml
[linear]
clientId = "example-client"
redirectUri = "https://relay.example.invalid/oauth/linear/callback"
apiEnabled = true

[[linear.executionGrants]]
organizationId = "example-org"
appUserId = "example-app"
scope = "installation-workspace"
```

Set `linearExecution = true` on each participating `[[nodes.workers]]` entry.
The declaration supplies that Worker's Node ID, Worker ID, `kind` and
`runtimeProfile`; Relay derives receiver bindings from those exact identities.
Each enabled Worker derives the issuer origin from `relay.url` and reads
`<dataDir>/secrets/worker-<id>-linear.key`, also read by Relay. Finalize the
complete topology before `axis init --config <path>` provisions these dedicated
keys. Do not reuse a transport, operator or OAuth credential. Across hosts,
securely provision the matching key to both readers instead of generating it
independently. Node never receives an OAuth token or adds a secret field to a
forwarded command. TLS is required between hosts; HTTP is accepted only at
numeric loopback on one trusted host. URLs cannot carry credentials, query
parameters, fragments or base paths.

The grants list is explicit business approval for **all resources visible to
that installation in its workspace**. It is not an issue-only or execution-Team
allowlist. Relay captures the installation revision and source-Team admission
at trusted dispatch, before any receiver request. Linear enforces target object
permissions, including nested resource IDs. A receiver's token or claimed Run ID
alone grants nothing. Installations must already be active and usable.

Worker `linearExecution` and `linear.apiEnabled` default to `false`; Relay
execution wiring is present only while at least one Worker enables it. Relay's credential route is absent while off; an API
enabled without execution authority refuses access. Missing/invalid enabled
Worker configuration prevents startup, and issuance refusal prevents that Run
from launching with an unverified identity. With access disabled, Codex receives
a nonempty unavailable key and a loopback refusal endpoint, so the CLI cannot
search personal configuration. Existing personal CLI files are never edited.

For a Relay/Worker pair selected to serve Linear AgentSessions, deployment must
run `axis-routing-preflight relay` and `axis-routing-preflight worker` with
`--config <path> --expected-version <release-version>` and
`--require-linear-execution true` (plus Worker selectors when needed). That invocation-only flag turns
the default-off state into a `linear_execution_required` blocker and checks each
host's local execution startup prerequisites. It neither mints a Run capability
nor substitutes for the controlled two-turn acceptance below.

## Agent usage

Worker injects `LINEAR_GRAPHQL_ENDPOINT`, a nonempty `LINEAR_API_KEY` containing
the Relay capability, `LINEAR_EXECUTION_STATUS`, `LINEAR_EXECUTION_EXPIRES_AT`, and the
installed `LINEAR_EXECUTION_SDK_MODULE` URL. These are per-process values, also set in
the Codex thread's shell environment policy, which is what a successor Run
replaces when it resumes the thread. They are sent through
private stdio, never argv or the instruction text. Do not print or persist them.
The `LINEAR_EXECUTION_*` fields are generated child protocol metadata, not
operator configuration. Child construction scrubs inherited deployment and
Linear values before injecting the current Run. The dedicated CLI launcher
uses `LINEAR_CLI_LAUNCHER_ACTIVE` to refuse recursive invocation and clears
Run expiry and SDK metadata when substituting its own credential.

Use the installed Linear CLI skill and existing dedicated CLI
commands first. Use
`linear api` when no dedicated command supports the operation, or the provided
official SDK adapter when needed. Do not run `linear auth`, write a CLI config,
unset the injected key, or pass `--workspace` (CLI 2.5.0 rejects that combination).

```sh
linear api 'query { viewer { id name organization { id name } } }'
linear api 'query($id: String!) { issue(id: $id) { id identifier title } }' --variables-json '{"id":"<issue-id>"}'
```

For the official typed SDK, the installed module is available without changing
the task repository's dependencies:

```js
const { axisLinearSdkFromEnvironment, AxisLinearRequestError } =
  await import(process.env.LINEAR_EXECUTION_SDK_MODULE);
const linear = axisLinearSdkFromEnvironment();
const viewer = await linear.viewer;
const created = await linear.createIssue({ teamId: "<team-id>", title: "<title>" });
if (!created.success) throw new Error("Linear did not report creation success.");
const issueId = created.issueId; // Retain before any lazy relationship read.
const issue = await created.issue; // A separate query; its failure does not undo creation.
```

Installed consumers may also import these exports from `@axisosme/axis/linear`.
`AxisLinearSdk` extends the official `LinearSdk`, preserving generated result
types and lazy relationship queries. `sdk.comment` takes `{ id }`; `sdk.issue`,
`sdk.project` and `sdk.projectMilestone` take an ID string in SDK 95.0.0.

CLI failures exit nonzero. Partial GraphQL responses retain data and errors.
The SDK throws `AxisLinearRequestError` with safe `code`, `status`, `requestId`,
`outcome`, partial `data`, GraphQL `errors`, and a numeric `retryAfter` when
available. Responses containing both data and errors have `outcome: "partial"`;
inspect successful fields before any further write. A `success: false` payload
remains a typed result to inspect.
Transport exceptions, incomplete responses and `linear.proxy.outcome_unknown`
on writes require read-before-retry reconciliation; neither client adapter nor
Relay automatically resubmits the mutation. Do not interpret an error as proof
that creation did not occur. See [proxy semantics](LINEAR-API-PROXY.md).

When execution clients are available, business Comments may target explicitly
authorized objects, including top-level issue comments and replies in the
controlling AgentSession thread. Without a user-named destination, the Run
instructions set the default: a definite conclusion is posted
as one top-level Comment on the AgentSession's issue, and that default counts as
the request for it, so a skill that writes only on explicit user request does not
suppress it. A clarifying question, partial finding, failure, or blocker is
answered in the AgentSession thread only. The session response is still posted
every turn; after a Comment it confirms the Comment and links it. Resolve the
requested destination and exact parent ID before writing a threaded reply;
verify the returned object and parent with a read. Report an uncertain write as
uncertain and reconcile before retrying.

Return normal task progress/final output through the Axis response channel;
its existing AgentActivity projection owns those messages. Do not duplicate
that output as a Comment. After a requested business Comment, report the write
result and link without repeating the published body. This is agent tool
guidance, not a new Comment-based reply/outbox path or an access grant. An
unavailable client never permits a personal-credential fallback.

## Deployment readiness

Verify CLI installation, the service user's PATH, and runtime instructions on each target host; retain safe results in operator-local records. Check the deployed CLI/SDK versions rather than assuming compatibility from binary installation alone. Installation and capability checks do not establish live Axis authorship or Inbox delivery.

## Lifetime, shutdown and rollback

Terminal observation closes the native process and requests execution-wide
revocation before projecting terminal lifecycle. Shutdown also closes starting
and active processes; a shutdown racing native start remains ambiguous. A
revocation request cannot undo an in-flight business mutation. Receiver failure
does not restore a personal key; Relay's live lifecycle checks and expiry still
apply. Relay restart loses all capability/grant state and does not reconstruct
old grants from durable Run IDs. Capacity is bounded to 10,000 prepared grants
per Relay lifetime, consistent with the bounded credential owner.

Turn off both execution switches and restart the affected services through the
authorized operational procedure to withdraw this capability. Close Relay's
capability owner before stores/listeners are disposed. No database migration or
personal CLI cleanup is required. This document grants no deployment authority.

## Verification

Run the local validation with Node 24:

```sh
npm run build
npm run check
```

`test/relay/linear-execution-runtime.test.ts` checks that a Node command continuation
prepares a fresh execution grant and that a cancel prepares none. Live authorship,
feedback loops, Inbox notifications, and multi-host deployment require separate
operator verification.

## Enabling continuation

Continuation needs no additional switch beyond Linear execution access. Enabling
it is an operator decision and requires separate authorization. Check both ends:

Relay:

1. `linear.apiEnabled = true` and at least one Worker has `linearExecution = true`.
2. `linear.executionGrants` names each workspace's organization and Axis App User,
   with an active installation.
3. The shared Node/Worker declaration supplies each receiver's exact identity and
   runtime profile. Relay and the Worker read the matching dedicated
   `worker-<id>-linear.key`.
4. The credential route answers; with no enabled Workers it is absent.

Codex Worker:

5. Its `linearExecution = true`, and `relay.url` points at Relay over TLS (or
   numeric-loopback HTTP on one trusted host). Invalid or incomplete declarations
   stop startup. Apply any managed TOML change through
   [fresh installation](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade).
6. The Worker advertises `continue`. A follow-up refused with
   `axis.worker.operation_unsupported` means the advertised operation set cannot
   admit continuation.
7. `nodes.workers[].codexHome` optionally selects the native home; relative TOML
   paths resolve against the file. Without it, the inherited `CODEX_HOME` or the
   service account's `~/.codex` applies. The selected home holds per-Run credential snapshots
   and conversation rollouts; retain it as sensitive state with its own policy.
   See [durable threads and on-disk history](#durable-threads-and-on-disk-history).

## Controlled live verification

Use this procedure only on a separately authorized deployment. Keep its results
in private operator records rather than treating this guide as an acceptance receipt.

On an explicitly authorized nonproduction deployment, record its exact Axis and
contracts revisions, installed CLI/SDK/native versions, workspace/App User and
Team IDs. Use named disposable objects and a unique acceptance marker; retain
each created ID before further reads or mutations. Read and, where authorized, create/update controlled issues and comments, verify the Axis author in Linear, and record exact root and reply IDs.
Check that the controlling session has only its expected AgentActivities and
that business Comments cause no additional Axis Run. Reconcile an intentionally
lost write result by its retained ID/marker, then prove no duplicate was created.
Use separate allowed/denied Teams and workspaces for refusal checks, and record
real rate-limit/upstream responses where safely reproducible. Clean up only the
recorded disposable objects after inspection.

Finally, the user must confirm Inbox behavior for the named subscribed issue,
thread, mention and notification settings. Record what was actually observed and
any unverified case. Record any notification behavior that could not be verified.
