# External Activity Handoff Runbook

Use this when a workflow is waiting on `execute_activity_external`, such as a
manager approval or webhook callback — the design rationale for this pattern
is [`docs/adr/0002-rust-native-execution-boundary.md`](../adr/0002-rust-native-execution-boundary.md).
Handoff list/detail responses expose
identity, state, token, and timing metadata only; raw workflow inputs, activity
inputs, outputs, signal bodies, and secrets stay redacted.

## Find pending handoffs

```bash
harvest handoff list --state PENDING
```

Narrow the inbox during an incident:

```bash
harvest handoff list \
  --state PENDING \
  --workflow-name billing_checkout \
  --activity-name manager_approval \
  --due-before 2026-05-08T12:00:00Z \
  --limit 50
```

For automation, use JSON:

```bash
harvest handoff list --state PENDING --output json
```

The API route is:

```text
GET /admin/external-handoffs?state=PENDING&workflow_name=billing_checkout
```

## Inspect one handoff

```bash
harvest handoff inspect <TOKEN>
```

API:

```text
GET /admin/external-handoffs/<TOKEN>
```

Workflow detail also embeds pending external handoffs:

```bash
harvest workflow get <EXECUTION_ID>
harvest workflow stack <EXECUTION_ID>
```

## Complete a handoff

Inline JSON output:

```bash
harvest handoff complete <TOKEN> --output-json '{"approved":true,"manager":"alice"}'
```

Output from a file:

```bash
harvest handoff complete <TOKEN> --output-file approval-result.json
```

Output from stdin:

```bash
printf '{"approved":true}' | harvest handoff complete <TOKEN> --output-file -
```

API:

```text
POST /activities/external/<TOKEN>/complete
Content-Type: application/json

{"output":{"approved":true}}
```

Successful first completion returns `status: "completed"` and
`newly_resolved: true`. Repeating the same command is idempotent and returns
`status: "already_terminal"` with the current terminal state.

## Fail a handoff

```bash
harvest handoff fail <TOKEN> --error "manager rejected"
```

With structured error details compacted into the durable error string:

```bash
harvest handoff fail <TOKEN> --error-json '{"code":"rejected","reason":"budget"}'
```

API:

```text
POST /activities/external/<TOKEN>/fail
Content-Type: application/json

{"error":"manager rejected","retryable":false}
```

Repeated failures after the handoff is terminal are idempotent and return
`status: "already_terminal"`.

## Extend the deadline

```bash
harvest handoff heartbeat <TOKEN> --extend-by-secs 3600
```

API:

```text
POST /activities/external/<TOKEN>/heartbeat
Content-Type: application/json

{"extend_by_secs":3600}
```

Omitting `extend_by_secs` resets the deadline using the activity's original
schedule-to-close window.

## Shard coverage

List responses include `shard_coverage`. If a shard cannot be inspected, the
response status becomes `partial` and `unavailable_shards` names the shard and
reason. Do not treat a partial response as proof that no matching handoff exists
until the unavailable shard is repaired or explicitly excluded with `--shard-id`.
