# Linear execution access

The optional [HTTP forwarding layer](LINEAR-API-PROXY.md) uses trusted grants
and [CLI/SDK delivery](LINEAR-EXECUTION-CLIENTS.md).
The authorization owner remains a **Relay-private, in-process** contract
`1.0.0`; enabling execution clients constructs it through the ordinary Relay
binary. Both endpoints are optional and disabled by default.

## Ownership and inputs

`src/relay/linear/execution-access-contract.ts` owns the TypeScript input/output
contract; `linear-execution-access.ts` owns ephemeral execution credentials.
`LinearInstallationStore` remains the installation authority and
`LinearOAuthManager` remains the encrypted OAuth credential/refresh owner.

The separate credential transport is owned by
`@axisosme/axis-contracts/linear-execution`. Existing Runtime v1,
Router/Node/Worker commands and events remain closed and carry no secrets. This
in-process interface is not permission to append credentials to an envelope or
journal.

The trusted Relay composer injects `LinearExecutionAuthorityPort.resolve`.
For an admitted execution ID it must read independent, current business grant
evidence and return the exact immutable Task/Attempt/Run, workspace organization,
App User, verified source Team, installation revision, active state and explicit
`installation-workspace` scope. Return `null` for an unknown or unapproved
execution. The source Team must already be verified to belong to that workspace.
It cannot come from the current client's request or prompt. Node authentication
alone cannot implement this port. An ID lookup is not a grant by itself.

The grant's `installationRevision` is captured when business access is approved,
using the installation store's `executionAuthority` snapshot. Do not recapture
it per request or silently upgrade an old execution after a permission change.
Only the exact closed shape with `state: "active"` and
`scope: "installation-workspace"` is accepted. A team/issue-only grant is
refused; it cannot be widened into workspace access. An inactive execution ID
must never be reactivated. A new Run/attempt requires a new authorized identity.

## Access boundary

The grant authorizes the resources **actually visible to the selected Axis
installation inside one Linear workspace**, subject to Linear's OAuth scopes
and resource permissions. It does not grant every resource in that workspace.
Linear describes app authorization and scope enforcement in its
[actor authorization](https://linear.app/developers/oauth-actor-authorization)
and [OAuth documentation](https://linear.app/developers/oauth-2-0-authentication).

Enforcement has three distinct parts:

1. The bearer resolves a server-owned record. Neither the issuance input nor the
   verification input can select an installation or vault reference. An optional
   expected workspace can only narrow the result, never select a different one.
2. The execution's source Team is rechecked through the existing installation
   permission decision. Its public-Team flag is an admission policy; that
   method does not resolve arbitrary target IDs or prove a Team is public.
3. Before a trusted Relay operation gets an OAuth token, `resolveViewer(token)`
   must match the bound workspace **and** App User. The operation must use this
   token only against the fixed Linear upstream. Linear enforces access to actual
   objects, including IDs in nested mutations or aliases; a request's self-
   reported Team has no security role.

There is **no execution-specific Team subset or current-issue isolation**. If an
installation sees two Teams, a workspace grant may access either. A narrower
promise requires a separately enforced target-object policy for every supported
operation, including nested/related objects; this generic grant cannot prove it.
The authority check alone does not implement GraphQL forwarding. The proxy owns the
fixed upstream, request/response handling and enforcement at that handoff.

## Operations and outputs

| Operation | Input | Output / side effects |
| --- | --- | --- |
| `issue` | Trusted execution ID; optional TTL | Resolves current grants, creates a random bearer, returns `{credential, context}` once. No OAuth token read. No installation selector. |
| `verify` | Raw bearer; optional expected workspace | Safe immutable access context after live authority checks. No vault reference/token; not sufficient to bypass the later `withAccess` check. |
| `withAccess` | Raw bearer; trusted Relay callback; optional expected workspace | Rechecks, reads/refreshes OAuth, verifies viewer identity, rechecks again, invokes the callback once. The OAuth token is passed only to this in-process privileged callback. |
| `revoke` | Non-secret credential ID | Idempotently refuses that credential for the rest of its lifetime. |
| `revokeExecution` | Execution ID known to this owner | Revokes its current credentials and tombstones future issuance in this owner. Unknown IDs are inert; close the authoritative grant first to prevent future issuance there too. |
| `close` | None | Disposes all credentials and refuses future issuance on this owner. |

Success is `{authorized:true,value:T}`. A verification context is exactly
`{contractVersion,credentialId,execution,expiresAt}`; it is a safe internal
projection, not an OAuth credential or a transferable authorization object.
It is not accepted as an input to token access. A privileged callback must never
return/serialize its token or reference to a client; it owns its sanitized
GraphQL result. Do not expose issuer, authority port or callbacks over HTTP.

`issue` is a protected control-plane operation. Deliver its credential only to
the already-authorized execution over an execution-specific protected channel.
This is a bearer capability: theft permits reuse until expiration/revocation;
it is not proof of the presenting process. The Worker owns child-process
delivery and lifecycle revocation; there is no automatic in-turn refresh.

## Lifetime and invalidation

Credentials contain 256 random bits with a purpose/version prefix. The owner
keeps only SHA-256 hashes, a random non-secret credential ID, and binding state
in memory. Default TTL is five minutes; maximum TTL is fifteen minutes; explicit
TTL must be a positive integer number of milliseconds. Expiration is exclusive
and never slides on use. Expired records may be removed at later issuance, so
an expired bearer can subsequently report `invalid_credential`. An observed
clock reading before issuance or at/after expiry permanently invalidates it.

Restart, replacement or disposal loses all bearer authority. Credentials cannot
be shared across replicas or restored from logs. This is intentionally
fail-closed, with no recovery key or automatic token renewal. Long executions
require fresh trusted issuance before expiry; that does not extend the old
credential. The authority source must retain execution closure across restarts.

Each check rereads execution state and the installation snapshot. An inactive,
missing or changed execution fails. Installation revocation fails before vault
deletion. Reinstall and **any** installation permission write change its revision,
including remove/re-add between requests, same-time reinstall with the same
App User/reference, and permission widening. An old grant cannot come back to
life. OAuth refresh alone does not change the installation revision.

The revision is the workspace's latest existing installation-audit ID, read
atomically with its current permission snapshot. No migration or new persistence
is added. Missing revision evidence refuses access. Preserve these audit rows
while using execution grants; never prune/restore them independently of the
installation store. Restoring an old store into a live owner is unsupported.

Revocation/expiry/state checks repeat after asynchronous OAuth and viewer reads,
immediately before operation handoff. They close new work after a locally
observed revocation; they cannot cancel an operation already in flight or erase
an accepted upstream write. Remote permission changes still depend on upstream
enforcement and delivery of the permission webhook. There is no automatic replay.

The default capacity is 10,000 records plus execution tombstones; the configured
limit is a positive integer at most 100,000. Capacity failure never evicts an
active credential. Known-execution revocation can add one tombstone per retained
execution; it remains bounded by twice the capacity. Tombstones persist until
owner disposal. Unknown revocation IDs consume no space.

## Errors and logging

Failures are `{authorized:false,status,error:{code,message}}`. `message` is
always `Linear execution access was refused.` HTTP adapters may project only
`{error}` with the indicated status. No exception message or request field is
copied. All codes below have prefix `linear.execution.`:

| Code suffix | Status | Meaning |
| --- | --- | --- |
| `invalid_request` | 400 | Invalid trusted issuance ID or TTL. |
| `invalid_credential` | 401 | Missing, malformed, forged, unknown, wrong-purpose, or lost-on-restart bearer. |
| `expired` / `revoked` | 401 | Known credential lifetime ended or explicit revocation. |
| `execution_denied` | 403 | Missing/inactive/changed execution or unsupported grant scope. |
| `installation_denied` | 403 | Missing/revoked installation or source Team no longer admitted. |
| `authorization_changed` | 403 | Installation revision/reference changed or OAuth viewer mismatched. |
| `workspace_mismatch` | 403 | The valid bearer cannot satisfy the requested workspace constraint. |
| `capacity` | 503 | Issuance capacity reached. |
| `unavailable` | 503 | Authority, clock, OAuth, provider, disposal or operation failure. |

`unavailable` is **not** a retry guarantee. If the callback started, its exception
may represent an uncertain write. The proxy must map upstream outcomes inside
the callback, record bounded reconciliation evidence and never automatically
replay a mutation. This module makes no exactly-once claim.

The only audit callback receives `{action,outcome,credentialId}` with closed
actions/codes and a generated ID or `null`. It never receives the bearer/hash,
vault reference, OAuth token, execution input, provider output or exception.
Audit callback failure cannot change authority. Issuance results are secret-
bearing: never log them. HTTP body/header logging and credential-bearing URLs
remain forbidden. Tests use fake secrets and verify both success and denial
projections; no production token or live write is needed.

## Verification boundary

`withAccess` is consumed at the GraphQL proxy boundary. The trusted lifecycle
adapter and Worker receiver supply execution-specific delivery through the
separate transport. Authorization, expiry, revocation, identity drift,
restart, capacity, and safe diagnostics need checks at those boundaries.
The retained `test/relay/linear-execution-runtime.test.ts` covers Node command
continuation preparing a fresh grant and cancel preparing none; it does not
establish all authorization or native-client behavior. Live Linear authorship,
notifications, and multi-host behavior require controlled operator verification
under the [client guide](LINEAR-EXECUTION-CLIENTS.md).
