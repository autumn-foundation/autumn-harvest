# Schedule Pause / Resume

This document describes the operational contract for pausing and resuming Harvest
schedules — the primary incident-response lever for stopping a runaway or
misbehaving cron/interval schedule without a code deploy.

## Quick reference

```bash
# Pause a schedule (record an optional reason)
curl -X POST https://<host>/api/harvest/admin/schedules/<uuid>/pause \
     -H 'Content-Type: application/json' \
     -H 'X-Harvest-Actor: ops-team' \
     -d '{"reason": "downstream API degraded — incident INC-4821"}'

# Resume when the incident is resolved
curl -X POST https://<host>/api/harvest/admin/schedules/<uuid>/resume \
     -H 'Content-Type: application/json' \
     -H 'X-Harvest-Actor: ops-team' \
     -d '{"reason": "downstream API recovered"}'

# Inspect pause state
curl https://<host>/api/harvest/admin/schedules/<uuid>
curl https://<host>/api/harvest/admin/schedules         # lists all schedules
```

## Behaviour contract

### What pause does

- Sets `is_paused = true` on the schedule config row.
- Records `paused_at` (timestamp), `paused_by` (actor from `X-Harvest-Actor`
  header), and `pause_reason` (optional free-text from the request body).
- The scheduler tick loop skips any schedule where `is_paused = true`, so **no
  new workflow executions or DAG runs are dispatched** after the pause takes
  effect.
- p95 time from the pause API call returning 200 to the first tick that does not
  fire is ≤ one scheduler tick interval (default ≤ 5 s).

### What pause does NOT do

**Pausing a schedule does not cancel or affect already-running workflow
executions or DAG runs that were started by prior ticks.** In-flight work
continues to completion uninterrupted. Use the batch cancel API (issue #102) if
you need to terminate running executions as well.

Pausing a schedule also does not affect task queue items that have already been
claimed by a worker — those tasks run to completion normally.

### What resume does

- Clears `is_paused`, `paused_at`, `paused_by`, and `pause_reason` (all set to
  `NULL`/`false`).
- The schedule resumes firing on its next natural tick based on its cron or
  interval expression; the scheduler does not retroactively fire ticks that
  were skipped during the pause window.

### No backfill on resume (default)

By default, **resume does not backfill missed ticks** that occurred while the
schedule was paused. The scheduler simply resumes from the next due tick after
the resume time.

If you need to recover missed runs, use the backfill endpoint (issue #177):

```bash
curl -X POST https://<host>/api/harvest/admin/schedules/<uuid>/backfill \
     -H 'Content-Type: application/json' \
     -d '{"from": "2026-05-13T00:00:00Z", "to": "2026-05-13T08:00:00Z"}'
```

### Idempotency

Both endpoints are idempotent:

- **Pause**: pausing an already-paused schedule returns `{"ok": true}` without
  altering `paused_at` or `paused_by`. The original pause timestamp and actor
  are preserved.
- **Resume**: resuming an already-active schedule returns `{"ok": true}` with
  no effect on the schedule row.

This makes both operations safe to retry.

### Audit trail

Every pause and resume action is recorded in `harvest_audit_log` with the
operation (`schedule.pause` or `schedule.resume`), the actor, and the schedule
UUID as `target_id`. The `pause_reason` is additionally stored directly on the
`harvest_schedules` row and returned by the GET endpoints.

### Multi-shard deployments

Pause and resume fan out across all configured shards. A schedule only lives on
one shard (its UUID encodes the shard assignment), so only one shard row is
mutated; the others are no-ops. The contract is **"no new runs anywhere"** —
once the API returns 200, no shard will dispatch a new execution for that
schedule.

## Response fields

`GET /admin/schedules` and `GET /admin/schedules/{id}` include the following
pause-related fields on each entry:

| Field | Type | Description |
|---|---|---|
| `is_paused` | `bool` | `true` when the schedule is paused |
| `paused_at` | `string \| null` | ISO-8601 timestamp of the most recent pause; `null` when active |
| `paused_by` | `string \| null` | Actor identity that issued the pause; `null` when active |
| `pause_reason` | `string \| null` | Free-text reason from the pause request body; `null` when active or no reason given |

All four fields are cleared to `null`/`false` on resume.
