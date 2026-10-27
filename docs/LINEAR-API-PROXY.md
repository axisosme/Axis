# Authenticated Linear GraphQL forwarding

`POST /api/linear/graphql` forwards one standard GraphQL request to the fixed
`https://api.linear.app/graphql` endpoint as the bound installation's Axis App User.
Linear owns its schema, resource permissions, CRUD and pagination. Relay adds no
schema/resolver or resource-specific endpoint. OpenAPI owns this HTTP contract;
Execution access's authorization interface stays Relay-private `1.0.0`.

## Enablement and runtime ownership

`linear.apiEnabled` omitted or `false` leaves the route absent (404). Only a
TOML boolean is accepted. `linear.apiTimeoutMs` defaults to 30000 and must be an
integer 1–120000 whenever supplied, including when the route is disabled. No
upstream URL or workspace override exists.

`deriveRelayConfiguration` projects these settings into `linearApi`;
`composeLinearApiRuntime` owns the optional runtime. The composer reuses the channel's `LinearInstallationStore`, `LinearOAuthManager`
and provider viewer check. Supply Execution access's trusted `LinearExecutionAuthorityPort`;
use the returned `access` only for local issuance/invalidation and pass `proxy`
to `startRelayHttpService({linearApi})`. Close the runtime before its stores.

**Execution integration:** the ordinary binary supplies trusted execution authority
when separately configured with explicit business grants and receiver bindings.
An API enabled without execution wiring or independent CLI grants returns 503
`linear.proxy.execution_access_unavailable`. Node/operator authentication and a
Run ID lookup alone are not business authority. See the
[execution client guide](LINEAR-EXECUTION-CLIENTS.md) for its protected issuer,
supported Codex mode, CLI/SDK environment and live verification requirements.

Independent CLI grants are configured through
`linear.cliGrants` in TOML; execution mode may remain disabled. See
[standalone CLI setup](LINEAR-CLI.md).

## Request and access boundary

Send `Content-Type: application/json` and `Authorization: Bearer <Axis credential>`.
An Axis credential is either a Run-bound execution capability or an independently
configured [CLI credential](LINEAR-CLI.md). Bare credentials are also accepted
for CLI API-key header compatibility. Personal keys, Node/operator tokens and Linear OAuth tokens fail.

```json
{
  "query": "query Issues($after: String) { issues(first: 2, after: $after) { nodes { id title } pageInfo { hasNextPage endCursor } } }",
  "variables": { "after": null },
  "operationName": "Issues"
}
```

`query` is required. `variables` is an optional object/null; `operationName` is
an optional string/null. The selected operation must be a query or mutation.
GraphQL's parser handles syntax, fragments, aliases and operation selection;
Linear validates schema/arguments. Batching, subscriptions, GET, query parameters,
extra envelope fields, multipart/incremental responses and compressed requests
are unsupported. Limits: 1 MiB request, 8 MiB response, 20000 parser tokens and
64 concurrent authenticated requests per proxy.

The bearer alone selects the installation. Execution credentials recheck Run
state, installation revision and source-Team admission. CLI credentials check
the configured hash, expiry and active installation without a Run or source
Team. Both read/refresh OAuth, verify the viewer's workspace/App User, and
recheck authority immediately before handoff.
Only the Axis token is put in upstream Authorization. Client headers/cookies and
URL overrides are never forwarded; redirects are refused.

Scope is resources actually visible to the installation in one workspace, with
**no execution-specific Team subset or current-issue isolation**. Linear enforces
target permissions including nested IDs. Remote permissions may take effect before
their webhook reaches Relay. Recorded revocation/change stops subsequent calls;
it cannot undo an already in-flight write. See [execution access](LINEAR-EXECUTION-ACCESS.md).

## Responses and failure semantics

Valid envelopes retain upstream HTTP status, `data`, `errors`, paths/locations/
extensions and pagination cursors/pageInfo. A 200 or `response_received` outcome
is **not business success**; callers must inspect errors and payloads such as
`success: false`. Partial success retains both data and errors. If an upstream
response echoes an exact execution/OAuth credential, only that secret is masked.

All responses include `Cache-Control: no-store`, generated `x-axis-request-id`,
`x-axis-linear-outcome` and `x-axis-linear-audit`. Well-formed numeric request,
endpoint and complexity quota headers, `X-Complexity`, bounded
`X-RateLimit-Endpoint-Name` and `Retry-After` pass through. Other upstream headers,
including cookies/location/auth, do not. HTTP 400 plus GraphQL `RATELIMITED` is
preserved, as are ordinary 401/403/429 GraphQL responses.

Local failures are GraphQL `errors[]` with a fixed message and `extensions`
containing `code`, `requestId`, `outcome` and observed `upstreamStatus` if available.

| Failure | HTTP and code |
| --- | --- |
| Execution authorization refusal | Its 400/401/403/503 and safe `linear.execution.*` code; no send. |
| Missing authority wiring | 503 `linear.proxy.execution_access_unavailable`. |
| Invalid envelope/syntax/operation | 400 `linear.proxy.invalid_request`. |
| Wrong method/media or request too large | 405 / 415 / 413: `linear.proxy.method_not_allowed`, `unsupported_media_type`, `body_too_large`. |
| Capacity / dispatch audit failure | 503 `linear.proxy.capacity` / `audit_unavailable`; no send. |
| OAuth expiry/refresh or viewer failure | Before handoff: 503 `linear.execution.unavailable`; no send. The existing identity port does not expose quota headers. |
| Deadline before handoff | 504 `linear.proxy.timeout`, `not_dispatched`; late OAuth completion cannot send. |
| Query transport/invalid/oversized response | Safe 502 `linear.proxy.upstream_unavailable`, `invalid_response`, `response_too_large`; valid upstream error envelopes retain status/body. |
| Mutation transport/deadline/redirect/invalid/oversized response or upstream 5xx | 502 `linear.proxy.outcome_unknown`. Valid 5xx data/errors remain, with an additional uncertainty error. |

The proxy never automatically retries either query or mutation, including a
forwarded 401. OAuth refresh happens before sending, not as refresh-and-replay.
Disconnect/deadline before handoff prevents a late send. A client disconnected
after handoff may miss a committed result and must treat its write as uncertain.
Preserve the original request privately, join the audit by request ID and inspect
the actual Linear objects before deciding to retry.

There is **no universal exactly-once guarantee, deduplication key or replay ledger**.
Submitting the same envelope twice makes two calls. An error does not prove that
no write occurred, including transport failure delivering the proxy's own response.

## Audit, rollback and verification

The required private audit records dispatch intent before sending and completion.
Both join request ID, timestamp, exact execution/Task/Attempt/Run, caller credential
ID, workspace/App User/source Team, fixed upstream target, operation kind, outcome
and HTTP status. Caller means authorized bearer identity, not OS-process proof;
unrecognized credentials have null caller, never client-reported IDs.

`requestDigest` is an HMAC of the forwarded envelope under a random in-memory key.
It correlates exact target/input envelopes within one proxy lifetime without raw
arguments or offline dictionary checks. It neither recovers input nor survives
restart as a portable digest. No credential, vault reference, query/operation name,
variables, response body or exception is logged.

Dispatch-audit failure prevents sending. Completion-audit failure preserves the
upstream evidence and sets `x-axis-linear-audit: unavailable`; prior intent and
request ID remain the reconciliation anchor. The binary uses operator stdout;
durable retention belongs to the deployment's log collector, not a new database.
HTTP/load-balancer logs must omit Authorization and request/response bodies.

Rollback: set `linear.apiEnabled = false` and replace the managed configuration
through [fresh host installation](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade),
or restart standalone services with the updated TOML. Webhook and AgentActivity channels
remain available independently. Live CRUD and notification behavior requires separate verification on controlled objects.

Protocol references: Linear's [GraphQL and error guidance](https://linear.app/developers/graphql),
[rate limits](https://linear.app/developers/rate-limiting), and
[OAuth](https://linear.app/developers/oauth-2-0-authentication).
Live CLI/SDK authorship and notifications require separate operator verification.
