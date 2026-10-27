# Agent Activity contract

`./agent-activity` exports one current contract for waiting elicitation and
terminal response deliveries. Executor, Run, and Task wrappers, their receipts,
and nested Runtime envelopes require exactly wire version `1.0.0`. The
`deliveryType` discriminator identifies the delivery capability and level.

Each receipt binds Task, Run, operation, and a closed conversation identity
`{ system: "linear", agentSessionId }` matching the Task source. Attempt is
bound wherever the envelope supplies it. Missing source AgentSession identity
fails closed. Validators perform no provider I/O or delivery reconciliation.

## Waiting and structured Other

Waiting receipts, nested envelopes, and Run projections carry the same complete
structured elicitation. Two or three explicit options are required, with unique
option identifiers and reply values. `recommendedOptionId` equals the first
explicit option's identifier.

The optional sibling `other: { optionId, replyValue }` declares a user-authored
reply capability. It is not an explicit option, cannot be recommended, and has
no label, description, text, or answer field. Its identifier and reply value are
distinct from all explicit options. Fixed-choice requests omit `other`.

For Linear, `other.replyValue` is the exact decision-bound claim prefix,
including its separator. A private reply is composed outside this contract from
that prefix and nonblank user text. Relay transports answers separately; waiting
envelopes and receipts carry only the capability declaration. Equality and
replay comparison include the full elicitation and presence/content of Other.

## Response and display safety

Succeeded envelopes carry durable `resultRef`, not the projected response body.
`AgentActivityResponseReceipt` carries that result reference, display body,
`activityId`, `projectionStatus`, and `truncated`/`truncations` metadata. The
only display field and truncation `fieldPath` is `/content/body`.

Display text uses valid Unicode scalars, NFC normalization, LF line endings,
and no unsafe control, bidirectional, or unredacted credential content.
Responses are bounded to 4,096 Unicode scalars. A visible truncation marker and
its structured record appear together with coherent retained and omitted
counts. `agentActivityFallbackResponseBody` defines the fixed provider-neutral,
untruncated fallback. Producers sanitize contextual secrets and local paths
before validation; validation alone cannot safely project raw provider output.

Receipt Task, Run, operation, result, conversation, and bound Attempt match the
wrapped envelope. Producers compare the receipt's source event with retained
evidence through `validateAgentActivitySourceEvent`.

## Replay

Comparators return `distinct` for different durable identities, `duplicate` for
equivalent content, and `conflict` for changed content under the same identity.
`activityId` identifies a delivery attempt and is excluded from replay identity.
Waiting uses the durable decision; response uses durable `resultRef`. Both
include conversation, source event, operation, and bound lineage.

Response Attempt participates only when the envelope supplies authority for it.
Task success carries no Attempt, so an attempt-only change under the same
result identity is a content conflict rather than a distinct reply. A different
conversation is distinct. Coherent body drift under one result is a conflict
that the succeeded envelope alone cannot detect. Consumers apply replay
comparison before publishing responses and reconcile delivery with the provider.
