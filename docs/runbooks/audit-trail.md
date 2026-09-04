# Runbook: Who Changed This Workflow / Schedule / DLQ Entry?

Use the Harvest audit trail to answer "who did what, when, and did it succeed?"
for every high-impact management mutation. The audit log is a Postgres-backed,
independently-retained record of operator actions — it is separate from the
workflow event history and never stores raw payloads.

## Covered operations

`workflow.start`, `workflow.signal`, `workflow.cancel`, `workflow.reset`,
`dag.trigger`, `dag.patch`, `schedule.create`, `schedule.pause`,
`schedule.resume`, `schedule.delete`, `dlq.replay`, `dlq.replay.bulk`,
`dlq.discard.bulk`, `batch.submit`, `retention.run_now`,
`external_activity.complete`, `external_activity.fail`.

Read-only routes (health checks, list/get/query, worker heartbeats, activity
heartbeats) intentionally produce no audit rows.

## Shipping the trail off-box

By default these rows live per shard, **inside the same Postgres databases they
describe** — so the principal who can rewrite the record of what they did is
the same principal who has database access. If you are answering a SOC 2 /
ISO 27001 question about where privileged-action logs ship and how you know
none were lost, turn on audit export (issue #953): Harvest streams every audit
record to a sink you run with at-least-once delivery, a dense per-shard
sequence the receiver can check for contiguity, a visible lag metric, and a
redrive path. See **[`docs/audit-export.md`](../audit-export.md)**.

The scenarios below query the local trail directly and work either way.

---

## Scenario 1 — "Who cancelled workflow X?"

### Via CLI

```bash
harvest audit list \
  --operation workflow.cancel \
  --target-id <execution-id> \
  --limit 10
```

Example output (default table):

```
OCCURRED_AT               ACTOR      OPERATION        TARGET                                                     STATUS     SRC  ERROR
2026-05-05T14:23:01.123Z  alice@co   workflow.cancel  workflow:01966b7a-0000-7000-0001-000000000001               succeeded  cli  -
```

### Via API

```bash
curl -s "https://app.example.com/api/harvest/admin/audit?operation=workflow.cancel&target_id=01966b7a-0000-7000-0001-000000000001" \
  -H "Authorization: Bearer $HARVEST_TOKEN" | jq .
```

---

## Scenario 2 — "Who changed this schedule?"

Schedule audit rows store the **schedule UUID** as `target_id` (the UUID returned by
`POST /admin/schedules/workflow` and visible in `GET /admin/schedules`). Look up the
UUID first, then query the audit log.

### Step 1 — find the schedule UUID

```bash
curl -s "https://app.example.com/api/harvest/admin/schedules" \
  -H "Authorization: Bearer $HARVEST_TOKEN" \
  | jq '.[] | select(.workflow_name == "approval_workflow") | .id'
# → "a1b2c3d4-0000-7000-0001-000000000042"
```

### Step 2 — query the audit log by UUID

#### Via CLI

```bash
harvest audit list \
  --target-type schedule \
  --target-id a1b2c3d4-0000-7000-0001-000000000042 \
  --since 2026-05-01T00:00:00Z
```

#### Via API

```bash
curl -s "https://app.example.com/api/harvest/admin/audit?target_type=schedule&target_id=a1b2c3d4-0000-7000-0001-000000000042&since=2026-05-01T00%3A00%3A00Z" \
  -H "Authorization: Bearer $HARVEST_TOKEN" | jq .
```

---

## Scenario 3 — "Who replayed this DLQ entry?"

### Via CLI

```bash
harvest audit list \
  --target-type dead_letter \
  --target-id <dead-letter-uuid>
```

### Via API

```bash
curl -s "https://app.example.com/api/harvest/admin/audit?target_type=dead_letter&target_id=<dead-letter-uuid>" \
  -H "Authorization: Bearer $HARVEST_TOKEN" | jq .
```

---

## Scenario 4 — "Show me all failures by operator alice in the last hour"

### Via CLI

```bash
harvest audit list \
  --actor alice@co \
  --status failed \
  --since 2026-05-05T13:00:00Z \
  --limit 50
```

### Via API

```bash
curl -s "https://app.example.com/api/harvest/admin/audit?actor=alice%40co&status=failed&since=2026-05-05T13%3A00%3A00Z&limit=50" \
  -H "Authorization: Bearer $HARVEST_TOKEN" | jq .
```

---

## Query reference

| Filter | CLI flag | Query parameter | Notes |
|--------|----------|-----------------|-------|
| Operator identity | `--actor` | `actor` | Exact match |
| Operation name | `--operation` | `operation` | e.g. `workflow.cancel` |
| Target resource type | `--target-type` | `target_type` | e.g. `workflow`, `schedule`, `dead_letter` |
| Target resource ID | `--target-id` | `target_id` | Exact match |
| Outcome | `--status` | `status` | `succeeded` or `failed` |
| Lower time bound (inclusive) | `--since` | `since` | RFC 3339 |
| Upper time bound (exclusive) | `--before` | `before` | RFC 3339 |
| Page size | `--limit` | `limit` | 1–500, default 50 |

Results are always ordered `occurred_at DESC`. The CLI prints a table by
default; pass `--output json` for machine-readable output.

---

## Configuring actor identity

Without configuration, all records carry `actor = "anonymous"`. This is
acceptable only in local or development deployments.

**CLI** — pass `--actor` or set `HARVEST_ACTOR` in the environment:

```bash
HARVEST_ACTOR=alice@co harvest workflow cancel <execution-id>
# or
harvest --actor alice@co workflow cancel <execution-id>
```

**Plugin embedder** — register a custom extractor in your Autumn app:

```rust
api_state.set_actor_extractor(|headers| {
    headers
        .get("x-authenticated-user")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or("anonymous")
        .to_string()
});
```

The `x-harvest-actor` header value from CLI requests is passed verbatim to the
extractor's `HeaderMap` argument, so the same hook handles both API and CLI
callers without branching.

---

## Audit retention

The default retention is **90 days**, designed to cover most incident review
windows. Override it with:

```rust
api_state.set_audit_retention_days(180); // keep 6 months
```

Audit retention runs on the same cadence as workflow-history retention but is
fully independent — changing one does not affect the other.

---

## Sharded deployments

`GET /admin/audit` aggregates records from all shards in a single response,
merged and re-sorted by `occurred_at DESC`. The response shape is identical for
single-shard and multi-shard deployments; callers do not need to be shard-aware.
