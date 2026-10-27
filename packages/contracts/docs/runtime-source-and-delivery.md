# Runtime source and delivery rules

Runtime accepts a closed Linear `SourceRef`:

```json
{ "system": "linear", "organizationId": "org-1", "issueId": "issue-1", "agentSessionId": "session-1" }
```

`organizationId` and `issueId` are required. `agentSessionId` may be absent
before a conversation is available. Agent Activity waiting and response
deliveries require it: their closed conversation identity is
`{ system: "linear", agentSessionId }`, and its session must equal the task
source's session. Unknown systems and properties are rejected.

Task identity includes every source field. Runtime trace validation and the
golden runner compare organization, issue, and optional agent session across
all messages for the same Task. The golden continuation trace includes two
Runs and rejects source identity drift.

## Waiting and continuation

`waiting_for_input` is terminal for a Run. The executor, run, and task waiting
envelopes carry the same complete structured elicitation as their Agent
Activity receipt, including its optional Other capability. Private
answer text travels outside these envelopes and receipts.

`priorRunId` on a continuation submit and its Run must agree. The predecessor
must be terminal and belong to the same Task and source. Self-links, cycles,
forks, first-run bypass, and changed-predecessor retries are conflicts. The
caller supplies predecessor and existing-child information from an atomic
Task-scoped store lookup; the validators perform no persistence or network
operations. Trace identity, state order, and terminal count are checked per
Run, and fork detection spans the full supplied trace corpus.

## Delivery and replay

Waiting replay identity includes delivery type, decision, Task, attempt, Run,
conversation, source event, and operation. Response replay identity uses the
durable `resultRef` in place of the decision; attempt participates only when
the envelope carries an attempt. `activityId` identifies a delivery attempt
and does not key replay.

A changed conversation is distinct. Two coherent deliveries with the same
durable identity and different content conflict. The receipt's Task, Run,
conversation, operation, result, and bound attempt must match its envelope.
Response deliveries wrap Runtime succeeded envelopes and enforce
safe display text, provider-neutral fallback content, and coherent truncation
markers and records.
