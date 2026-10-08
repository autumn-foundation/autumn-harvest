# Schedule Next-Fires Preview

> **Issue #348** — Read-only endpoint for pre-commit cron validation and incident response.

The schedule preview API lets operators verify that a cron expression, timezone, jitter window, overlap policy, and calendar all combine to produce the firing pattern they intend — **before persisting the config or unpausing a schedule during an incident**.

---

## Endpoints

### `GET /admin/schedules/{id}/preview`

Preview the next N firing instants for an **existing, saved** schedule.

**Query parameters**

| Parameter | Default | Max | Description |
|-----------|---------|-----|-------------|
| `count` | `10` | `100` | Number of firing instants to return |
| `from` | now (UTC) | — | ISO-8601 UTC start instant, e.g. `2026-06-01T09:00:00Z` |

**Example request**

```
GET /api/harvest/admin/schedules/3f7a1b2c-…/preview?count=5&from=2026-06-01T00:00:00Z
```

---

### `POST /admin/schedules/preview`

Validate a **candidate** schedule config and preview its next N firings **without writing to `harvest_schedules`**. Use this to catch bad cron expressions before committing.

**Request body** — same shape as `POST /admin/schedules/workflow`:

```json
{
  "schedule_expr": "0 9 * * 1-5",
  "timezone": "America/Los_Angeles",
  "jitter_secs": 300,
  "overlap_policy": "skip",
  "calendar": "us-federal-holidays",
  "skip_policy": "run_next_business_day",
  "max_active_runs": 1,
  "paused": false,
  "count": 10,
  "from": "2026-06-01T00:00:00Z"
}
```

Returns `400 Bad Request` when `schedule_expr` or `timezone` is invalid, with a JSON body pointing at the offending token:

```json
{"error": "unknown timezone 'America/Bogus'", "field": "schedule_expr"}
```

---

## Response format

Both endpoints return the same JSON envelope:

```json
{
  "entries": [ ... ],
  "is_paused": false,
  "pause_reason": null,
  "from": "2026-06-01T00:00:00Z",
  "count_requested": 10
}
```

### Entry fields

| Field | Always present | Description |
|-------|---------------|-------------|
| `scheduled_at` | ✓ | UTC wall-clock instant the cron/interval computed |
| `local_at` | ✓ | `scheduled_at` in the schedule's configured timezone (RFC 3339) |
| `effective_at` | when not suppressed | After calendar adjustment and jitter application |
| `effective_local_at` | when `effective_at` is present | `effective_at` in the schedule's timezone |
| `reason` | ✓ | See [Reason codes](#reason-codes) |
| `jitter_earliest_at` | when jitter enabled | Earliest possible fire = `scheduled_at` |
| `jitter_latest_at` | when jitter enabled | Latest possible fire = `scheduled_at + jitter_secs` |
| `would_skip_if_active` | ✓ | Advisory: `true` when overlap policy could silently drop this firing |

### Reason codes

| Code | Meaning |
|------|---------|
| `cron` | Fires as scheduled (no jitter applied) |
| `cron+jitter` | Fires at `effective_at` after deterministic jitter |
| `skipped:calendar-excluded` | Suppressed by calendar exclusion + `skip` policy |
| `deferred:calendar` | Moved to a different day by calendar `run_next_business_day` / `run_prev_business_day` |

### Paused schedules

When a schedule is paused (`is_paused: true`), `entries` is always empty and a `pause_reason` summary is included at the top level.

```json
{
  "entries": [],
  "is_paused": true,
  "pause_reason": "Paused for DST rollover maintenance",
  "from": "2026-11-02T00:00:00Z",
  "count_requested": 10
}
```

### `would_skip_if_active` advisory

The preview is **stateless** — it does not query how many runs are currently active. When the overlap policy is `skip` or `buffer_one`, a firing will be silently dropped at dispatch time if `max_active_runs` is already running. The `would_skip_if_active: true` flag is an advisory warning that the operator should account for run duration when unpausing.

Policies that **never** produce advisory warnings:

| Policy | `would_skip_if_active` |
|--------|----------------------|
| `cancel_other` | always `false` |
| `terminate_other` | always `false` |
| `buffer_all` | always `false` |

Policies that **always** produce advisory warnings when `effective_at` is non-null:

| Policy | `would_skip_if_active` |
|--------|----------------------|
| `skip` | always `true` |
| `buffer_one` | always `true` |

---

## Example: verify a timezone-aware cron before committing

```bash
curl -s -X POST https://your-app/api/harvest/admin/schedules/preview \
  -H "Content-Type: application/json" \
  -d '{
    "schedule_expr": "0 9 * * 1-5",
    "timezone": "America/Los_Angeles",
    "count": 5
  }' | jq '.entries[] | {scheduled_at, local_at, reason}'
```

Sample output (week of 2026-06-01):

```json
{"scheduled_at": "2026-06-01T16:00:00Z", "local_at": "2026-06-01T09:00:00-07:00", "reason": "cron"}
{"scheduled_at": "2026-06-02T16:00:00Z", "local_at": "2026-06-02T09:00:00-07:00", "reason": "cron"}
{"scheduled_at": "2026-06-03T16:00:00Z", "local_at": "2026-06-03T09:00:00-07:00", "reason": "cron"}
{"scheduled_at": "2026-06-04T16:00:00Z", "local_at": "2026-06-04T09:00:00-07:00", "reason": "cron"}
{"scheduled_at": "2026-06-05T16:00:00Z", "local_at": "2026-06-05T09:00:00-07:00", "reason": "cron"}
```

---

## Example: inspect jitter window bounds

```bash
curl -s -X POST https://your-app/api/harvest/admin/schedules/preview \
  -H "Content-Type: application/json" \
  -d '{
    "schedule_expr": "0 * * * *",
    "jitter_secs": 300,
    "count": 3
  }' | jq '.entries[] | {scheduled_at, effective_at, jitter_earliest_at, jitter_latest_at, reason}'
```

Sample output:

```json
{
  "scheduled_at": "2026-06-01T10:00:00Z",
  "effective_at": "2026-06-01T10:02:43Z",
  "jitter_earliest_at": "2026-06-01T10:00:00Z",
  "jitter_latest_at": "2026-06-01T10:05:00Z",
  "reason": "cron+jitter"
}
```

---

## Example: preview calendar exclusions

```bash
curl -s -X POST https://your-app/api/harvest/admin/schedules/preview \
  -H "Content-Type: application/json" \
  -d '{
    "schedule_expr": "0 9 * * *",
    "calendar": "us-federal-holidays",
    "skip_policy": "run_next_business_day",
    "count": 5,
    "from": "2026-07-03T00:00:00Z"
  }' | jq '.entries[] | {scheduled_at, effective_at, reason}'
```

Sample output (July 4th deferred to July 5th):

```json
{"scheduled_at": "2026-07-03T09:00:00Z", "effective_at": "2026-07-03T09:00:00Z", "reason": "cron"}
{"scheduled_at": "2026-07-04T09:00:00Z", "effective_at": "2026-07-05T09:00:00Z", "reason": "deferred:calendar"}
{"scheduled_at": "2026-07-05T09:00:00Z", "effective_at": "2026-07-05T09:00:00Z", "reason": "cron"}
```

---

## Incident response checklist

When validating a schedule before unpausing during an incident:

1. **Fetch current schedule state** → `GET /admin/schedules/{id}` (check `is_paused`, `pause_reason`, `next_run_at`).
2. **Preview next 10 fires from now** → `GET /admin/schedules/{id}/preview?count=10`.
3. **Confirm local-timezone times** — check `local_at` matches expected fire times in your timezone.
4. **Check `would_skip_if_active`** — if `true`, verify there are no lingering active runs before resuming.
5. **Resume** → `POST /admin/schedules/{id}/resume`.

Target MTTR for the "is this config safe?" decision: **< 60 seconds** (see `synthetic-incident-drills.md`).

---

## Rate limiting

The preview endpoints are read-only and require the same auth posture as `GET /admin/schedules`. Infrastructure-level rate limiting of **30 req/min per embedder** is recommended to prevent expensive cron expansion (large `count` + complex timezone) from being weaponized. Configure this at your reverse proxy or API gateway layer.

---

## Determinism contract

- **Jitter disabled** (`jitter_secs = 0`): responses are fully deterministic for a given `(schedule_config, from)` pair.
- **Jitter enabled**: `effective_at` is deterministic (seahash of `schedule_id ‖ fire_time`). The `jitter_earliest_at` and `jitter_latest_at` bounds describe the full window so operators can reason about worst-case timing.
- **`POST /admin/schedules/preview`**: uses `uuid::Uuid::nil()` as a placeholder `schedule_id` for jitter hashing, so repeated calls with the same body produce identical `effective_at` values.

---

## Related runbooks

- [`schedule-backfill.md`](schedule-backfill.md) — recover missed runs after unpausing.
- [`schedule-trigger-now.md`](schedule-trigger-now.md) — manually fire a schedule once.
- [`synthetic-incident-drills.md`](synthetic-incident-drills.md) — MTTR drill playbook.
- [`harvest-alerts.md`](harvest-alerts.md) — alert conditions for `schedule.runs` / `schedule.skipped`.
