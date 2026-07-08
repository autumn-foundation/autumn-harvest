# Schedule Run History — Triaging a Flaky Cron

Use this runbook when a scheduled job (nightly ETL, hourly reconciliation, a cron
DAG) starts misbehaving and the first question is: **"show me this schedule's last
N runs and which ones failed."**

`GET /api/harvest/admin/schedules/{id}/runs` (issue #534) answers it in one call.
Every execution a schedule launched is durably attributed to it (a `schedule_id`
linkage on the execution row) and tagged with a dispatch **origin** —
`scheduled`, `backfill`, or `manual_trigger` — so a backfill storm or an ad-hoc
operator fire never gets mistaken for the normal cadence.

## Prerequisites

- Harvest CLI configured with `HARVEST_BASE_URL` pointing at your management API.
- Operator (admin) access to the management API (see `docs/runbooks/audit-trail.md`
  for auth headers). This endpoint is admin-guarded and read-only.
- The schedule ID (`harvest schedule list` shows all IDs).

## The triage play

### Step 1 — List the schedule's recent runs

```bash
harvest schedule runs <schedule-id>
```

The response is newest-first and includes a cadence **summary**:

```json
{
  "schedule_id": "…",
  "status": "complete",
  "summary": { "succeeded": 27, "failed": 3, "timed_out": 0, "running": 1, … },
  "runs": [
    {
      "execution_id": "…",
      "nominal_fire_time": "2026-06-28T03:00:00Z",
      "started_at": "2026-06-28T03:00:01Z",
      "completed_at": "2026-06-28T03:04:12Z",
      "state": "FAILED",
      "origin": "scheduled"
    }
  ],
  "limit": 100,
  "next_cursor": null,
  "shards": [{ "shard_id": 0, "status": "inspected", "error": null }]
}
```

Read the **failure ratio straight off `summary`**: `failed: 3` out of a cadence of
`27 + 3 + … `. The summary counts **only `scheduled`-origin runs**, so any backfill
or manual fire you ran while investigating does not inflate the number.

### Step 2 — Drill into just the failures

```bash
# Only the failed / timed-out runs in the last 24 hours.
harvest schedule runs <schedule-id> --state FAILED --state TIMED_OUT --since 24h
```

`--since` accepts an RFC 3339 timestamp or a relative duration (`24h`, `7d`).
`--state` is repeatable. Take an `execution_id` from a failed row and inspect its
history to find the root cause:

```bash
harvest workflow get <execution_id>
harvest workflow history <execution_id>
```

### Step 3 — Tell cadence apart from noise

If the failure count looks worse than reality, filter by origin to confirm a
backfill or manual run isn't muddying the picture:

```bash
# Normal cadence only (what the SLO is measured against).
harvest schedule runs <schedule-id> --origin scheduled

# Everything an operator kicked off by hand.
harvest schedule runs <schedule-id> --origin manual_trigger
```

| origin | meaning |
|--------|---------|
| `scheduled` | a normal scheduler-tick fire — the cadence the summary counts |
| `backfill` | a run created by `POST /admin/schedules/{id}/backfill` |
| `manual_trigger` | an ad-hoc `trigger-now` fire (attributed, but carries no `nominal_fire_time`) |

### Step 4 — Decide: pause, fix, or backfill

- **Failures are ongoing and you need to stop the bleeding** → pause the schedule:
  ```bash
  harvest schedule pause <schedule-id>
  ```
- **You shipped a fix and want to recover the missed slots** → backfill the window
  (see `docs/runbooks/schedule-backfill.md`); the recovered runs show up here under
  `origin = backfill`.
- **One bad slot needs a re-run now** → `harvest schedule trigger-now <schedule-id>`
  (shows up as `origin = manual_trigger`).

## Pagination

The list is capped (default 20, max 200). When more runs match than the cap, the
response carries a `next_cursor`; pass it back to page through:

```bash
harvest schedule runs <schedule-id> --limit 50
harvest schedule runs <schedule-id> --limit 50 --cursor "<next_cursor>"
```

The `limit` is always reported in the response — runs are never silently
truncated. `next_cursor` is `null` on the last page.

## Cross-shard behaviour

A schedule's runs can land on multiple shards. The endpoint fans out across all
shards and merges the results. If a shard is unreachable, the response is **not** a
hard failure: `status` becomes `partial` (or `unavailable` if none could be read)
and the offending shard is named in `shards[]` with its error. Treat a `partial`
result as incomplete — the true failure count may be higher than shown.

## Backward compatibility

Executions started **before** this feature was deployed have `schedule_id = NULL`
and are un-attributable: they will not appear under any schedule's `runs`. This is
expected. Attribution applies to every run started after the upgrade, so the view
fills in going forward. (Historical scheduled rows whose `workflow_id` encodes the
schedule were backfilled to `origin = scheduled` by the migration, but pre-upgrade
backfills cannot be distinguished from cadence and are reported as `scheduled`.)

## See also

- `docs/runbooks/schedule-backfill.md` — recover missed slots after downtime.
- `docs/runbooks/schedule-trigger-now.md` — fire a one-off run by hand.
- Scheduler **decision** telemetry (why a tick fired/skipped) is a separate view:
  `GET /admin/schedules/{id}/decisions` (#325). This runbook is about execution
  **outcomes**, not fire decisions.
