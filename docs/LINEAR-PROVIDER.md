# Linear Developer Preview provider contract

Provider endpoints used by the in-repository adapter:

- Axis's in-repo HTTP/GraphQL provider targets Linear's public GraphQL API
- public GraphQL endpoint `https://api.linear.app/graphql`
- OAuth token endpoint `https://api.linear.app/oauth/token`

Authoritative references:

- [Getting started with Agents](https://linear.app/developers/agents)
- [Developing the Agent Interaction](https://linear.app/developers/agent-interaction)
- [Interaction Best Practices](https://linear.app/developers/agent-best-practices)
- [Webhooks](https://linear.app/developers/webhooks)
- [OAuth 2.0 authentication](https://linear.app/developers/oauth-2-0-authentication)
- [GraphQL API](https://linear.app/developers/graphql)
- [Raw public SDL](https://raw.githubusercontent.com/linear/linear/refs/heads/master/packages/sdk/src/schema.graphql)

## Pinned shapes

`AgentSessionEvent.guidance` is optional or null, otherwise an array of at most 64 rules totaling at most 64 KiB of serialized UTF-8 JSON. Each rule has a nonblank markdown `body` and an `origin` of `{ type: "Organization" }` or `{ type: "Team", team: { id, key, name, displayName, parentId } }`; team text fields are nonblank strings and `parentId` is a nonblank identifier or null (omission normalizes to null). Invalid guidance returns the existing HTTP 400 `linear.ingress.malformed` error before admission. A string or object is invalid. The composed instruction, including guidance and escaping, also shares the Node command byte budget. Router rejects a pending command over 64,334 bytes or an outer request over 128 KiB before command admission, using `axis.router.malformed`; no Node dispatch occurs.

Authenticated event guidance becomes a separate `<linear-agent-guidance>` section of the dispatch instruction, retaining Linear's array order and each rule's origin. The expected precedence order is organization first, parent teams next, nearest team last. `promptContext` remains unused. Guidance applies to the dispatch caused by the event carrying it, with no conversation-level guidance persistence. A continuation or steer without guidance adds no section; queued redispatch re-reads the user command without supplementing guidance. Exact ingress retries retain the first encrypted composed instruction. A duplicate delayed `created` event after a prompt-first start does not dispatch extra guidance.

Agent session webhook actions are `created` and `prompted`. Every `created` start is verified against an authenticated exact `issue(id)` read whose Issue and Team IDs match the webhook. The Issue description is neither part of the instruction nor a condition of admission, so an empty or long description never refuses a start. An initial prompt Activity is reread and retained even on a direct assignment; only starts with neither a comment nor an Activity have no user command. A comment-origin start's user command is only the validated exact Comment instruction; its exact Comment must still carry a canonical Linear URL. Webhook `body`, `content`, `promptContext`, history, and timeline fields are presentation-only and never become executable input. Relay retries exact created-input reads at most three times with bounded exponential backoff; exhausted reads are retryable and never dispatch. Every non-stop `prompted` continuation obtains its instruction only through one authenticated exact `agentActivity(id)` read. The returned Activity ID and AgentSession ID must match the event; its non-null user must match the event Activity user and not be the Axis App User; content must be typed `prompt`. The verified provider body is digested and encrypted unchanged and becomes the user command of the dispatch envelope below.

Every Linear dispatch -- a direct or comment-origin start, a prompted continuation or prompt-first start, and a queued redispatch -- reaches Router as one envelope, composed at dispatch time by `composeLinearDispatchInstruction`:

```text
<user-command>
…
</user-command>
<comment-id>…</comment-id>
<parent-comment-id>…</parent-comment-id>
<issue-title>…</issue-title>
<issue>KEY - ID</issue>

This instruction does not include the Linear issue description or comment thread. Read them from Linear when you need them (for example `linear issue view KEY`). If you cannot read them, say so plainly instead of guessing what they contain.
```

`<issue-title>`, `<issue>`, and the closing hint are always present; the hint is what tells an executor with no user command where the rest of the request lives. `<user-command>` is present when the verified input is nonblank after the Axis App User's own mention markup (a `<user …>…</user>` element whose `id` attribute is exactly the App User ID) is removed; mentions of anyone else stay. `<comment-id>` appears only for proven comment provenance, and `<parent-comment-id>` only when that Comment is a reply. In every interpolated value, a `<` that begins one of the envelope's own tag names is written as `&lt;`; nothing else is escaped. The idempotency and redispatch digests (`instructionDigest`, `inputDigest`, the comment invocation, the redispatch marker) cover the user command alone. The encrypted session input holds the envelope as first composed for its command, and every attempt of that command dispatches those recorded bytes, so a command resubmitted after a lost outcome keeps Router's per-command idempotency even if the Issue was renamed in between. A queued redispatch -- which re-reads the raw Activity and the exact Issue, and takes the Comment and App User identities from the session binding -- still matches its marker, because the marker digests the user command alone. A queued redispatch is a new command that re-reads the Issue, so its envelope carries the Issue's current key and title; if the Issue was renamed in between, those differ from the original dispatch. The byte-for-byte guarantee above holds within one command, not across a redispatch. A prompted input and a queued redispatch each make one exact `issue(id)` read for the key and title, after the Activity verifies, and its Issue ID must match the session. Only a created start also compares the Team; prompt and redispatch reads do not compare the current Issue Team.

The supported outbound Activity content is limited to:

- `thought { body }`
- `action { action, parameter, result? }`
- `elicitation { body }`
- `response { body }`
- `error { body }`

The waiting-decision workflow described below, and closed-token reply handling, are **reachable end to end on the Linear channel**. `Router#createTask` mints every Linear Task under Runtime/Task contract `1.0.0`, whose `RuntimeState` includes `waiting_for_input`. So a Worker that parks moves the Run into that state, `observeLinearTick` raises a run-chain decision row alongside the elicitation Activity in one transaction (`src/relay/linear/observe.ts`), and a sealed-token reply is correlated back to that decision by the store and dispatched as a continuation on the existing Task. `waiting_for_input` is terminal in the contract, so the way out of it is that successor Run, not a transition on the parked one.

On success, the AgentSession receives the executor's sanitized response body
with projection status and truncation metadata through Agent Activity ,
which wraps the exact Runtime succeeded envelope. Response text lives
only in the receipt, never in the Runtime envelope. A plain succeeded envelope
has no body and produces the fixed no-result summary; it must not be represented
as a rich response delivery.

Response delivery requires the bound executor to project an `ExecutorTerminalResponse`. Codex and Claude Code both project terminal responses. When no response is available, the plain envelope reports that execution ended with no result to show; it does not claim that the requested work was completed.

The user-only `prompt` Activity is never emitted. The documented signal enum is `auth | continue | select | stop`. When a Run is active, `stop` maps to the current Router cancellation request and pauses only that Run; the AgentSession remains prompt-eligible after cancellation. When the current Run has already ended as `waiting_for_input`, `stop` atomically closes the pending decision and enqueues one final `response` Activity so Linear leaves the awaiting-input presentation, while retaining the AgentSession for a later prompt. Exact replay adds no second response. Relay sends neither cancel for the terminal waiting Run nor a continuation for the stop itself. Agent Session presentation state is derived by Linear from Activities and is not manually mutated.

A Router Run terminal is not the end of the whole AgentSession. Success still creates a `response`, failure still creates an `error`, and cancellation still creates the cancellation `response`; after that projection Relay stops observing only that Run and leaves the AgentSession able to receive a later `prompted` webhook. The later prompt is dispatched as a continuation and becomes observable under its new Router operation. `thought`, `action`, and `elicitation` remain separate Activity types. `Agent Session Ended` is reserved for an explicit user archive fact from Linear. Linear does not publish any agent-session archive ingress for Relay to adopt: see "Delegation removal and session archive" below for the platform evidence. Relay has no supported source event from which to infer that state.

A dispatch whose outcome Router never resolves gets a bounded, rather than infinite, wait. If a start, continue, or stop still reconciles `unknown` after the unconfirmed window (30 minutes by default), the observe tick's sweep settles it terminal and posts exactly one `error` Activity -- `LINEAR_ACTIVITY_COPY.startOutcomeUnconfirmed` / `.continueOutcomeUnconfirmed` / `.stopOutcomeUnconfirmed` -- that says Axis has stopped waiting for an answer without asserting the command did or did not run, and that it will not be resent. Only an unconfirmed START or CONTINUE guards the AgentSession: it then refuses every further instruction with `session_unconfirmed`, telling the user to start a new Agent session -- with one exemption inside that guard, a stop of a Run that is still actually executing (not merely unsettled; a Run parked `waiting_for_input` does not qualify). An unconfirmed STOP does not guard the session at all: the slot is released and the conversation is left accepting instructions normally, including a stop, for as long as its Run is genuinely still live.

### Delegation removal and session archive

Relay accepts AgentSession actions `created` and `prompted`. It has no archive
ingress or archive-state inference. A terminal Run does not archive the Session.

`AppUserNotification.issueUnassignedFromYou` is a separate webhook category.
Axis does not adopt it as delegation-removal authority: the parser quarantines
it as `linear.ingress.unsupported_shape` without start, continue, or cancel.
Its payload does not distinguish delegate clearing from assignee clearing, so
an execution decision cannot safely be inferred from the name alone. The
fixture in `test/fixtures/linear/official-webhook-catalog.ts` is separate from
the data-change webhook corpus.

### Package-root provider port

`LinearProviderPort` and its immutable snapshot types are package-root TypeScript extension seams: `loadExactIssue` is an optional capability and `ImmutableLinearComment.url` remains optional. Relay requires `loadExactIssue` for every created start, every prompted input, and every queued redispatch, and a canonical HTTPS Linear comment URL for comment-origin created starts; an adapter lacking either capability is rejected before input persistence or dispatch (a prompted input as `issue_unavailable`; a queued redispatch stays queued, as for a missing Activity capability). The in-repository `LinearGraphQlProvider` implements both capabilities.

For `loadExactIssue` and `loadExactComment`, adapters own temporary-failure classification: only a `RelayError` whose `retryable` field is true is retried (up to three bounded attempts) and retained for delivery-lease reclaim. GraphQL/schema/authorization/identity failures must reject without that flag and are terminal, fail-closed created-start failures. On a prompted input the same classification applies to its `loadExactIssue` read: an exhausted retryable read leaves the delivery to the inbox retry budget, which stays silent until it is spent and then posts its one give-up Activity, while a definite failure refuses the prompt as `issue_unavailable` (not `provider_unavailable`, whose copy names the Activity). A queued redispatch treats every issue-read failure as transient and charges its own retry budget. The built-in provider classifies fetch/timeout, HTTP 5xx, and `RATELIMITED` GraphQL responses as retryable; it classifies malformed responses and other GraphQL errors as permanent. A `null` Issue `description` (Linear's field is nullable) is read as the empty description, not as a malformed response.

`AgentActivityCreateInput` accepts `agentSessionId`, JSON `content`, optional `ephemeral`, optional UUID v4 `id`, and optional signal metadata. The Relay persists and supplies `id`. The API does not document blind-create idempotency, so a timeout remains ambiguous.

### Session external URLs

Axis does not publish an external result page or attach result URLs to a
Session. Executor `resultRef` values are internal `axis-evidence://` pointers,
not user-openable URLs. They must not be passed to Linear as public result links.

### Native select options

For a structured waiting elicitation, Relay sends sibling `signal: "select"`
and `signalMetadata: { options: [{ label, value }] }` fields beside `content`.
The provider maps each explicit option's `label` to `label` and `replyValue` to
`value`. Descriptions and optional Other instructions remain in the elicitation
body; Other is a decision-bound prefix plus private free text, not a fixed
native option.

A selection returns through the ordinary `prompted` Activity path. Axis expects
an exact decision-bound reply value, never a display label; native option-return
behavior must be verified on the deployed provider. The local contract allows
two or three explicit options, labels at most 80 characters, and fixed-length
`axis-reply-` values. Reconciliation compares content and does not assume
`signalMetadata` is returned by provider reads.

### Activity ID validation

Axis requires a UUID v4 for outbound `AgentActivityCreateInput.id` and validates it before any provider call. Internal response-receipt IDs are not automatically valid provider Activity IDs.

Worker response receipts retain their existing deterministic internal identity.
When projecting a new event, Relay preserves a supplied UUID v4 and otherwise
allocates a random UUID v4 in the same transaction as the outbox row and Router
cursor. That persisted outbox ID is the identity used for both create and exact
reconciliation. Event replay and restart reuse the durable row; delivery does
not regenerate or transform its ID. Existing outbox rows are not rewritten or
requeued, including `confirmed_failure`, `ambiguous`, and delivered activities.
Recovery of historical failures remains a separately scoped operator action;
check exact provider identity/outcome before any repair or replay.

Create rejection diagnostics retain only closed top-level input fields (`id`,
`agentSessionId`, `content`, `ephemeral`, `signal`) and known validator names.
Unknown validation shapes keep `linear.graphql.invalid_input`.
Targets, values, messages, nested content, and arbitrary provider strings are
never copied to diagnostics. Rate-limit retry and ambiguous reconciliation
semantics are unchanged. Linear derives completed presentation from a delivered
`response`; Relay's internal Session remains open for later prompts.

### Local pre-send validation and safety codes

Relay records four persistent, operator-visible safety codes (`src/relay/linear/delivery.ts`), produced by the same local `#postable` check on both `create` and `reconcile`, before any Linear API call is made:

- `linear.activity.non_uuid_v4_id` -- the persisted outbox row's `id` is not a UUID v4.
- `linear.activity.invalid_content` -- the persisted `content` does not parse as a valid Agent Activity content object.
- `linear.activity.delivery_target_mismatch` -- the freshly resolved delivery target's `agentSessionId` no longer matches the row's stored target.
- `linear.activity.content_digest_mismatch` -- the persisted content no longer hashes to the persisted digest.

Local non-v4 IDs use `linear.activity.non_uuid_v4_id`. Provider-side validation of the `id` field may separately produce `linear.graphql.invalid_input.id.is_uuid`; neither code contains the rejected value.

Relay makes exactly one `agentActivity(id)` lookup for every non-stop prompted event, and once that Activity verifies, one exact `issue(id)` read for the dispatch envelope. Missing capability or token, provider errors, malformed/missing/mismatched data, wrong type, blank content, or excess size fail closed before input/command persistence and dispatch; only a retryable issue read is left for the delivery to retry. The rejection is one stable `error` Activity whose fixed safe copy is selected by the refusal's own closed reason: `Axis did not run this request.`, then `Reason: ` and the reason's fixed sentence, then its fixed action sentence. The copy is provider-neutral -- it names neither Codex nor Claude -- and no provider diagnostics or prompt content are projected. Relay never substitutes session history or the latest Activity for the current instruction.

### Issue-comment invocation

Issue-comment invocation uses AgentSession events rather than raw `Comment` ingress. Linear must first create a real `AgentSessionEvent` carrying comment provenance: `agentSession.sourceCommentId` (with `commentId` as the created-event compatibility field), or `agentActivity.sourceCommentId` for a prompted continuation. Relay then rereads the exact `Comment(id)` and, for prompted events, the exact `AgentActivity(id)`. It requires matching Issue, Team, Session, Activity, source-comment, and human-author identities. The exact Comment root (`parentId ?? id`) must equal a non-null `agentSession.commentId`; this binds both created `sourceCommentId`/`commentId` combinations and prompted replies to the captured thread root. The author must be active, non-App, non-bot, and exactly present in a bounded current Team membership query. Existing installation Team authority is checked before these reads and again before dispatch and Activity delivery.

The standalone comment validator still requires an exact structured Axis mention by default. For an authenticated, authorized AgentSession creation, the session itself supplies delegation authority: a user may supply an instruction without repeating a mention. Both modes retain the bounded ProseMirror parser (128 KiB raw size, depth 16, 2,048 nodes) and reject malformed instructions. Raw Comment webhooks still never authorize execution.

A created event uses `sourceCommentId ?? commentId` to locate its instruction. The authenticated session creation is the delegation authority, so its verified comment need not repeat an @mention. Linear may instead generate an artificial AgentSession root with no human author and no `sourceCommentId`; after exact ID, root, Issue, and Team binding, Relay treats only that marked root as a direct created assignment. Linear's initial prompt Activity hanging from that root (for example "Work on this issue") is then verified as the direct created prompt; any other direct prompt Activity with a source comment is refused as `activity_mismatch`. The exact creator, root, issue, Team, human membership and content checks still apply to every human Comment; an invalid instruction is refused rather than replaced with an empty assignment.

A verified created comment event uses the validated exact Comment instruction as its user command, dispatched in the envelope described under "Pinned shapes" with the exact Issue key, ID, and title and the Comment's own ID and parent ID -- never the Issue body; it never retains webhook `promptContext`. Validated event guidance is dispatched as a separate instruction section. For a verified ordinary prompted event, the source Comment establishes exact thread and human provenance, while execution authority remains the exact authenticated typed prompt Activity. Its source Comment, Session, Activity, and user must match; its body need not equal the rendered exact `Comment.body`. The exact Activity body is encrypted unchanged and dispatched as the envelope's user command.

Relay allows the first provenance-verified comment-origin prompted Activity in the captured thread to establish the local Session as one ordinal-1 `start`; it creates the accepted invocation and canonical acknowledgement through the existing transaction. Its command key is `["Comment","prompted",sourceCommentId,activityId]`, while comment-created commands retain `["Comment","create",sourceCommentId]`; this durably prevents a historical created event carrying an Activity from qualifying as prompt-first. The same logical Activity replay is inert, and a later distinct verified child reply is ordinal-2 `continue`. That prompted delivery creates no startup-ledger row. A matching comment-origin created event that arrives afterward is reconciled against the retained Session/root/source Comment, first input, accepted invocation, prompted command key, start command, and acknowledgement; Relay adds only the immutable created-event ledger linkage and does not dispatch again. Missing verified Comment provenance or identity/provenance drift remains a conflict. A direct prompted Activity without a local Session, and a first-event stop, remain non-dispatching. Missing exact-Issue or required Comment-URL capability fails closed before dispatch.

A waiting decision uses a separate closed reply surface. Each option carries a unique `axis-reply-<48 lowercase hex>` value deterministically bound to that decision ID and option ID. The elicitation Markdown tells the frozen owner to reply with one complete value. Relay first exact-rereads the Activity and, when present, its source Comment; the normalized whole body must be precisely one reply value. Relay then looks up that value against the Session ledger and exact Activity/Comment provenance before setting `expectedDecisionId`. Decision replies retain their separate exact-token authority checks. Labels, mention-bearing labels, unknown or stale token-shaped values, and a second source event never authorize a continuation.

A free-text reply is authorized against the pending decision and frozen human owner before dispatch. It is handled according to the question's declared options:

- **The question offered `Other`.** The text IS that custom answer. It is turned into the `<other token> <answer>` reply the closed path already accepts, so the claim, the provenance binding, the input limits and the one-shot consumption are the identical ones a typed `Other` reply goes through.
- **The option set is closed.** The text is dispatched to the model as a candidate reference to one of the options already offered, with the question and its options restated and with three rules stated in the instruction: only the listed options are available, an unclear reply is asked again rather than guessed at, and the model must not re-decide the owner, the authorization, or which question the reply belongs to. The relay has already decided all three before the dispatch is built — the sender is compared against the decision's frozen owner exactly as a token reply's sender is — and the model is told so rather than left to infer it.
- **The text does not meet the reply input constraints**, is not from the decision's owner, or merely looks like a token. Refused exactly as before, and the decision stays open, so the user still has one unanswered question.

The original decision row settles when the interpretation is dispatched: the reservation claimed it and the successor Run supersedes the parked one, and a decision row outliving its Run is an invalid unresolved state. It settles with a **distinguishing** outcome, `superseded`, never `resolved` — nobody selected an option, so nothing reads back as though somebody had. If the model cannot resolve the text it raises a new elicitation on the successor Run, which is where the user-visible "I still have an unanswered question" invariant is kept. All labels from all decisions in that AgentSession remain permanently reserved, including after later distinct-label decisions resolve. Exact replay of the already accepted Activity/Comment returns the original command. A display label alone is not a decision token.

Every execution input freezes one exact active non-app human owner from an authenticated provider reread, including canonical Linear `User.url`; there is no owner fallback. A waiting decision creates one `elicitation` Agent Activity whose Markdown includes that canonical URL as plain URL text, because Linear converts plain Linear URLs into native mentions. It is never wrapped as a Markdown link. Linear derives its Comment. Axis never invokes `commentCreate` and never sends `bodyData` outbound.

Axis-authored comments, unavailable authority, and unprovable provider evidence are silent. A verified session comment that is malformed receives one fixed safe error Activity; a proven current nonmember receives one generic denial. These responses use the existing Agent Activity outbox and do not expose provider diagnostics or comment text.

## Issue behavior

On a new implementation Session, the Relay queries the issue and its Team’s first `started` workflow state. It changes state only when the current state is not started/completed/canceled. It sets the workspace App User as delegate only when no delegate exists. `assigneeId` is never included, so the human assignee remains unchanged.

This synchronization runs before dispatch, and a failure still prevents the dispatch. Read and update failures retain their distinct safe causes. Transport loss (including the 4.5 s request timeout), a Linear 5xx, and a `RATELIMITED` GraphQL error are retryable: the delivery is left to the inbox's retry budget, nothing is said while it lasts, and only the single give-up Activity is posted if it is spent. Retrying an update whose outcome was lost is safe, because every attempt rereads the issue first and sends only a state and delegate that are still unset. While a created start is being retried, a prompt or stop on the same AgentSession waits for it, within its own retry budget, rather than starting the session ahead of it or being refused for want of its Run; once the created delivery reaches a terminal state the prompt or stop proceeds as before. These failures are definite and refuse the start at once, each with its own startup-failure reason and copy: the issue is not found; the execution context is malformed (including more than 250 labels); the issue is no longer in the authorized Team; Linear refuses the read; Linear refuses the update or answers `success: false`. No provider message is copied into the Activity or a durable row.

The same read supplies the labels used for Worker selection. Each label is read with its group (`parent { name }`), and a label inside a label group is reported as `<group>:<name>`; an ungrouped label keeps its plain name. A group without a usable name makes the execution context malformed. So the children of the workspace's `WORKER` label group arrive as `WORKER:CODEX` and `WORKER:CLAUDE`, which select the `codex` and `claude` executors (matched case-insensitively). The bare `claude` and `codex` labels still work. Two different executors on one issue are refused with copy asking the user to leave only one WORKER label (see [NODE-ROUTING](NODE-ROUTING.md#worker-selection)).

## Authorization drift

`PermissionChange.teamAccessChanged` atomically adds/removes Team IDs and updates public-Team access. `OAuthApp.revoked` closes new-work authority before deleting encrypted credentials. A token/Team check is performed again before each command and Activity delivery.

Unknown authenticated actions, including raw `Comment` webhook payloads, are quarantined. Unknown required fields are rejected rather than guessed. Update the redacted fixtures and this pin document before adopting a Developer Preview shape change.

## Official webhook catalog snapshot

The separate offline contract corpus lives at `test/fixtures/linear/official-webhook-catalog.ts`. It is not the Axis Agent/Developer Preview fixture catalog (`test/fixtures/linear/catalog.ts`) and does not imply that an official webhook model is a supported Axis command.

Snapshot source: Linear's [Webhooks documentation](https://linear.app/developers/webhooks) and [official Webhooks schema explorer](https://studio.apollographql.com/public/Linear-Webhooks/variant/current/schema/reference/objects), verified 2026-08-31. The schema publication timestamp is `2026-08-24T21:07:51.494Z`; the pinned Webhooks-schema SHA-256 is `842fb723ecbf018552b8f6db8f68b39e3f535238e404e15ebe431efa58d472c1`.

The exact 46-tuple inventory is:

- Data-change (42): `create`, `update`, and `remove` for `Attachment`, `Comment`, `Customer`, `CustomerNeed`, `Cycle`, `Document`, `Initiative`, `InitiativeUpdate`, `Issue`, `IssueLabel`, `Project`, `ProjectUpdate`, `Reaction`, and `User`.
- Convenience (3): `IssueSLA.set`, `IssueSLA.highRisk`, and `IssueSLA.breached`.
- Local credential lifecycle (1): `OAuthApp.revoked`.

All fixtures are synthetic and generated offline: they contain no live Linear payloads, credentials, customer content, or network dependency. Every case carries the documented common envelope (`actor`, `createdAt`, `url`, `webhookTimestamp`, `webhookId`, `organizationId`, `type`, and `action`), a fresh UUID-v4 delivery identity, matching `Linear-Event`, current header/body timestamps, and an HMAC-SHA256 calculated from its exact raw bytes. Data-change events carry `data` and update events also carry `updatedFrom`; IssueSLA convenience events carry `issueData`, not `data`. The 42 data-change tuples and three IssueSLA tuples must parse as `Unsupported`, then be durably quarantined with `linear.ingress.unsupported_shape` and cause no Axis start, continuation, or cancellation. `OAuthApp.revoked` carries the documented `oauthClientId` and organization envelope; its `appUserId` is an explicitly labeled Axis local extension needed by the existing local credential-revocation behavior. It completes with no Axis dispatch and no generic `data` field. An otherwise official revocation without the local extension remains malformed as an explicit compatibility gap.

When Linear publishes a change, refresh this snapshot only after recording the new documentation/schema URLs, publication timestamp, SHA-256, and exact model/action delta. Update the independent expected-tuple assertion, factories, focused ingress tests, and this document together. Do not widen the parser, dispatcher, public exports, live endpoints, or database schema merely to make a new catalog tuple pass; that needs a separately approved provider-contract change.
