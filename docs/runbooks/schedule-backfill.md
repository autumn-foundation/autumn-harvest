# Schedule Backfill Runbook

Use this runbook to recover missed scheduled runs after downtime, a paused schedule, a bad deploy window, or disabled workers. The backfill API is idempotent: repeating the same request never creates duplicate workflow executions or DAG runs.

## Prerequisites

- Harvest CLI configured with `HARVEST_BASE_URL` pointing to your management API.
- Operator access to the management API (see `docs/runbooks/audit-trail.md` for auth headers).
- The schedule ID you want to backfill (`harvest schedule list` shows all IDs).

## Step 1 — Identify the missed window

```bash
# List all schedules and note the schedule ID and expression.
harvest schedule list
```

Calculate `--from` and `--to` as RFC 3339 timestamps. For a 7-day hourly billing schedule that was down from Monday 00:00 UTC to Sunday 23:59 UTC:

```
--from 2026-04-28T00:00:00Z
--to   2026-05-04T23:59:59Z
```

## Step 2 — Dry-run to verify the planned timestamps

Always dry-run first. This is non-destructive: it plans but dispatches nothing.

```bash
harvest schedule backfill <schedule-id> \
  --from 2026-04-28T00:00:00Z \
  --to   2026-05-04T23:59:59Z \
  --dry-run
```

**Check the output:**

- `status: dry_run` — no runs were created.
- `total` — number of timestamps that would fire (e.g. 168 for a 7-day hourly schedule).
- `dispatched` — estimated count that would be started (respects `max_active_runs`).
- `skipped` — estimated count that would be skipped (e.g. due to `max_active_runs` saturation).
- `planned_timestamps` — the full list. Verify the first and last timestamps are correct.

If `total` exceeds the default limit of 1000, reduce the window or pass `--max-count <n>`:

```bash
harvest schedule backfill <schedule-id> \
  --from 2026-04-28T00:00:00Z \
  --to   2026-05-04T23:59:59Z \
  --dry-run \
  --max-count 500
```

## Step 3 — Execute the backfill

Once you are satisfied with the dry-run output:

```bash
harvest schedule backfill <schedule-id> \
  --from 2026-04-28T00:00:00Z \
  --to   2026-05-04T23:59:59Z
```

**Check the response:**

| Field | Expected | Notes |
|---|---|---|
| `status` | `complete` | `partial` means at least one shard failed — see `partial_shard_failures` |
| `dispatched` | ≥ 1 | New runs created |
| `skipped` | Any | Already-existing runs for those timestamps (idempotent) |
| `skipped_reasons` | Object | `already_exists` = duplicate; `max_active_runs` = concurrency cap hit |
| `failed` | 0 | Non-zero requires investigation |

## Step 4 — Inspect the resulting runs

For **workflow** schedules, query the executions:

```bash
harvest workflow list --workflow-name billing_workflow
```

For **DAG** schedules, inspect DAG runs via the management API:

```
GET /admin/dags/<dag-name>/runs
```

Backfill-created DAG runs carry `"_harvest_run_source": "backfill"` in their `conf` field, distinguishing them from scheduler-tick runs.

## Step 5 — Retry safely if the client disconnects

The backfill endpoint is idempotent: re-running the exact same request with the same `--from`/`--to` window will skip timestamps that were already dispatched and only create runs for any that were missed due to partial shard failures.

```bash
# Safe to repeat — duplicates are reported as skipped, not double-started.
harvest schedule backfill <schedule-id> \
  --from 2026-04-28T00:00:00Z \
  --to   2026-05-04T23:59:59Z
```

After retrying, verify `dispatched` + `skipped` = `total` and `failed` = 0.

## Handling paused schedules

If the schedule is currently paused, the backfill request will be rejected with a 400 error unless you explicitly opt in:

```bash
harvest schedule backfill <schedule-id> \
  --from 2026-04-28T00:00:00Z \
  --to   2026-05-04T23:59:59Z \
  --include-paused
```

Resume the schedule afterwards if appropriate:

```bash
harvest schedule resume <schedule-id>
```

## Handling max_active_runs saturation

If `skipped_reasons` reports `max_active_runs`, the concurrency cap was reached before all timestamps could be dispatched. Options:

1. **Wait for running executions to finish**, then re-run the backfill — idempotency ensures already-dispatched timestamps are skipped and only the deferred ones are dispatched.
2. **Increase `max_active_runs`** temporarily via `harvest schedule update` (if implemented), then re-run.
3. **Batch the window** into smaller sub-ranges and run them sequentially once capacity opens.

## Viewing backfill history

`harvest schedule list` returns a `last_backfill` field for each schedule showing the most recent backfill request: window, actor, counts, status, and timestamps. Use this to confirm an earlier operator ran a backfill and to compare windows if running a follow-up.

## Partial shard failures

If `status` is `partial`, `partial_shard_failures` lists each shard that could not be reached. Investigate DB connectivity for those shards, then re-run the backfill — timestamps that succeeded on reachable shards will be reported as `skipped` (idempotent), and only the failed ones will be retried.

## Audit trail

Every backfill request (including dry-runs) is recorded in the audit log with operation `schedule.backfill`. Query recent backfill audit records:

```bash
harvest audit list --operation schedule.backfill --target-id <schedule-id>
```

## Success metric

For a 7-day hourly schedule (168 timestamps): dry-run planning should return in under 1 s. A full backfill dispatch should complete in under 5 s under normal worker availability. After three repeated client retries, `dispatched + skipped = total` with zero duplicates in the workflow execution or DAG run tables.
