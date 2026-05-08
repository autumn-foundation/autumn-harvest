# Replay Fixture Export Runbook

Use this playbook before changing deployed workflow code. The goal is to prove
the new binary can replay representative stored histories before it meets
production. Determinism bugs are cheaper when they are trapped in CI instead of
left to haunt an on-call rotation.

## Export One Execution

Full-payload exports are accepted by `WorkflowReplayer::replay_from_json` and
`harvest-replay` in JSON mode:

```bash
harvest history export <execution-id> \
  --payload-policy full \
  --output-file fixtures/<workflow>/<execution-id>.json
```

The equivalent read-only API route is:

```text
GET /api/harvest/workflows/{execution_id}/history/export?payload_policy=full
```

The export document includes `schema`, `version`, `payload_policy`,
`exported_at`, `size_limit`, `workflow_name`, `execution_id`, and `events`.
The top-level `workflow_name`, `execution_id`, and `events` fields preserve the
shape expected by replay tooling.

## Export A Batch

Use batch export when preparing a release gate:

```bash
harvest history export-batch \
  --workflow-name billing_checkout \
  --state-group terminal \
  --updated-after 2026-05-01T00:00:00Z \
  --limit 1000 \
  --payload-policy full \
  --max-bytes 10485760 \
  --output-file fixtures/billing_checkout_2026-05-01.json
```

Useful filters:

| Filter | Purpose |
| --- | --- |
| `--workflow-name` | Limit to one workflow type. |
| `--state-group active` | Sample histories that may still replay after deploy. |
| `--state-group terminal` | Sample completed/failed/cancelled histories for regression CI. |
| `--updated-after` / `--updated-before` | Bound by latest history event timestamp, falling back to execution row time only when no event exists. |
| `--shard-id` | Re-export one shard after a partial batch. |
| `--limit` | Cap the number of exported histories. |

The equivalent API route is:

```text
GET /api/harvest/admin/history/exports?workflow_name=billing_checkout&state_group=terminal&payload_policy=full
```

## Interpret Batch Results

Batch responses include:

| Field | Meaning |
| --- | --- |
| `status` | `complete` when all requested shards were read, `partial` when at least one shard failed. |
| `exports` | Replay fixture documents. |
| `failures` | Per-execution failures such as size-limit rejection. |
| `shard_coverage` | Shards inspected, matched, and unavailable. |
| `filters` | The resolved request filters and payload policy. |

Treat `status: "partial"` as a failed release gate unless the unavailable
shard is outside the deploy blast radius and that waiver is recorded in the
release notes. Re-run with `--shard-id <id>` after the shard recovers.

## Payload Policy

`redacted` is the default. It replaces payload-bearing fields with deterministic
summaries containing a digest and byte count. Use it for support tickets and
debugging where raw payloads are unnecessary.

`full` keeps raw payloads and is required for replay CI. Store these fixtures in
private CI artifact storage or a private test-fixture repository. Do not attach
full exports to public issues, chat threads, or logs unless you enjoy incident
reviews with dramatic lighting.

## Size Limits

Exports default to a 10 MiB serialized document limit per history. The exporter
does not truncate replay fixtures; if the limit is exceeded, it returns an error
with `actual_bytes`, `max_bytes`, and `truncation_behavior: "fail"`.

For large workflows, either raise `--max-bytes` for private CI storage or pick a
more focused sample. Truncated replay histories are forbidden because they would
prove nothing useful.

## CI Gate

Wire exported fixtures into a replay regression test:

```rust
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};

#[tokio::test]
async fn billing_histories_replay() {
    for entry in std::fs::read_dir("fixtures/billing_checkout").unwrap() {
        let json = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        let report = WorkflowReplayer::new()
            .register_fn("billing_checkout", billing_checkout)
            .replay_from_json(&json)
            .await
            .expect("history fixture must parse");

        assert!(matches!(report.status, ReplayStatus::ReplaySucceeded), "{report}");
    }
}
```

Run the same fixtures through `harvest-replay` when you want a CLI gate:

```bash
harvest-replay \
  --workflow billing_checkout \
  --history-source json \
  --json-path fixtures/billing_checkout/<execution-id>.json
```

Release checklist:

1. Export recent terminal histories with `--payload-policy full`.
2. Export a smaller active-history sample if the release changes replay-heavy
   code paths.
3. Fail the gate on any partial shard coverage unless explicitly waived.
4. Run `WorkflowReplayer` or `harvest-replay` against every full fixture.
5. Keep redacted exports for operator debugging; keep full exports private.
