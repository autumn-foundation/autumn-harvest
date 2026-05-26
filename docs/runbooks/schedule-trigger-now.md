# Schedule Trigger-Now Runbook

**Endpoint:** `POST /admin/schedules/{id}/trigger`  
**Issue:** #343

## Purpose

Fire an immediate one-off run of a workflow or DAG schedule **without advancing its
`next_fire_at` cursor**. Use this in incident response to re-run a scheduled job
that was missed, to test a schedule definition before its next natural fire time,
or to manually kick off a workflow when automatic scheduling was paused.

## When to use

- A scheduled run was missed due to a worker outage and you need to fire it now
  rather than waiting for the next cadence tick.
- A schedule was paused for maintenance and you want to fire one run immediately
  without resuming the schedule.
- You need to test a new schedule definition against production data before its
  next scheduled fire time.

## Usage

### Fire a schedule by UUID

```bash
harvest schedule trigger-now <SCHEDULE_UUID>
```

### Provide a human-readable audit reason

```bash
harvest schedule trigger-now <SCHEDULE_UUID> --reason "incident-2026-05-26: missed overnight run"
```

### Trigger a paused schedule

```bash
harvest schedule trigger-now <SCHEDULE_UUID> --force
```

### HTTP API

```bash
# Basic trigger
curl -X POST https://example.com/api/harvest/admin/schedules/<UUID>/trigger

# With reason and overlap_policy override
curl -X POST https://example.com/api/harvest/admin/schedules/<UUID>/trigger \
  -H 'Content-Type: application/json' \
  -d '{"reason": "manual incident recovery", "overlap_policy": "skip"}'

# Force-trigger a paused schedule
curl -X POST 'https://example.com/api/harvest/admin/schedules/<UUID>/trigger?force=true'
```

## Response

```json
{
  "execution_id": "018f...",
  "workflow_id": "manual-<schedule-uuid>-<timestamp-millis>",
  "triggered_at": "2026-05-26T10:00:00Z"
}
```

## Error cases

| Status | Cause | Resolution |
|--------|-------|------------|
| 404 | Schedule UUID not found | Verify the UUID with `GET /admin/schedules` |
| 409 | Schedule is paused and `?force=true` was not passed | Append `?force=true` to override |
| 400 | Invalid `overlap_policy` value in the request body | Use one of: `skip`, `buffer_one`, `buffer_all`, `cancel_other`, `terminate_other` |
| 500 | Database error starting the workflow execution | Check Postgres connectivity and worker logs |

## Important notes

- **Cadence is not affected.** The `next_fire_at` cursor on the schedule row is
  not advanced. The next scheduled run fires at its normal time.
- **Not strictly idempotent.** Each successful call creates a new execution with a
  unique `workflow_id` derived from the current millisecond timestamp. Retrying
  within the same millisecond will hit `AllowDuplicate` semantics and return the
  same execution.
- **Audit trail.** Every call is recorded in `harvest_audit_log` under operation
  `schedule.trigger`. The optional `reason` field is stored in the `error_summary`
  column (repurposed as a notes field for successful triggers) for operator
  traceability.
- **Paused schedules.** A paused schedule means the scheduler will not fire new
  runs automatically; `trigger-now` bypasses that gate when `?force=true` is
  provided. The schedule remains paused after the manual run.

## Metrics

The trigger emits `harvest.schedule.manual_trigger{schedule.name, outcome}` where
`outcome` is one of:
- `fired` — execution started successfully
- `rejected_paused` — schedule is paused and `force=true` was not passed
- `skipped_overlap` — execution could not be started (e.g. a DB error)

Monitor the `rejected_paused` and `skipped_overlap` outcomes to detect misconfigured
automation that calls this endpoint blindly.
