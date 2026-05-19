# Calendar-Aware Schedules (issue #337)

Harvest schedules can be attached to a **named calendar** — a set of excluded dates (holidays, maintenance windows, shutdown days) — so that fires landing on excluded dates are handled according to a configurable **skip policy** instead of being silently dispatched or silently skipped with no visibility into why.

---

## Built-in Calendars

Three calendars are seeded by the `20260519000000_harvest_calendar_awareness` migration:

| Name | Description |
|------|-------------|
| `weekends-off` | Saturday and Sunday (rolling; no fixed exclusion rows — handled by day-of-week logic built into `apply_skip_policy`) |
| `us-federal-holidays` | US federal public holidays for 2025–2026 |
| `nyse` | NYSE market holidays for 2025–2026 |

Built-in calendars (`built_in = true`) cannot be deleted via `DELETE /calendars/{name}`. Operators can extend them by calling `PUT /calendars/{name}` to replace the exclusion set.

---

## Skip Policies

| Value | Behaviour when fire date is excluded |
|-------|--------------------------------------|
| `skip` (default) | Drop the firing; record `harvest.schedule.skipped` metric with `reason = "calendar"`. The schedule advances to the next computed fire time. |
| `run_next_business_day` | Advance to the nearest non-excluded weekday **after** the excluded date (scans forward up to 365 days). |
| `run_prev_business_day` | Retreat to the nearest non-excluded weekday **before** the excluded date (scans backward up to 365 days). |

---

## Attaching a Calendar to a Schedule

### Via the management API

```http
POST /api/harvest/admin/schedules/workflow
Content-Type: application/json

{
  "workflow_name": "generate_payroll",
  "schedule_expr": "cron:0 9 * * 1",
  "timezone": "America/New_York",
  "calendar": "us-federal-holidays",
  "skip_policy": "run_next_business_day"
}
```

When `calendar` is `null` (the default) no filtering is applied and all fires dispatch as usual.

### Via `WorkflowSchedule` builder

```rust
use autumn_harvest::prelude::*;

let schedule = WorkflowSchedule::new("generate_payroll", Schedule::Cron("0 9 * * 1".into()))
    .with_timezone("America/New_York")
    .with_calendar(Some("us-federal-holidays".into()))
    .with_skip_policy(SkipPolicy::RunNextBusinessDay);
```

---

## Calendar CRUD API

### List calendars

```http
GET /api/harvest/calendars
```

Response: array of `{ name, description, built_in, created_at, updated_at }`.

### Get calendar with exclusions

```http
GET /api/harvest/calendars/{name}
```

Response: `{ name, description, built_in, created_at, updated_at, exclusion_dates: ["YYYY-MM-DD", ...] }`.

### Create a custom calendar

```http
POST /api/harvest/calendars          (admin required)
Content-Type: application/json

{ "name": "plant-shutdown", "description": "Annual factory downtime" }
```

Response: `201 Created` with the calendar object.

### Replace exclusion dates

```http
PUT /api/harvest/calendars/{name}    (admin required)
Content-Type: application/json

{
  "exclusion_dates": ["2026-12-24", "2026-12-25", "2026-12-26"]
}
```

Response: `204 No Content`. The call **replaces** the full set; omitting a date removes it.

### Delete a calendar

```http
DELETE /api/harvest/calendars/{name}  (admin required)
```

Returns `204 No Content`. Fails with `409 Conflict` if the calendar is `built_in`.

---

## Schedule Fire Preview

Preview the next N effective fire times for any schedule, respecting its calendar and skip policy:

```http
GET /api/harvest/admin/schedules/{id}/preview?count=10
```

Response:

```json
{
  "entries": [
    { "scheduled_at": "2026-01-05T09:00:00Z", "effective_at": "2026-01-05T09:00:00Z", "reason": "on schedule" },
    { "scheduled_at": "2026-01-19T09:00:00Z", "effective_at": null,                    "reason": "excluded by calendar (skip)" },
    { "scheduled_at": "2026-01-26T09:00:00Z", "effective_at": "2026-01-26T09:00:00Z", "reason": "on schedule" }
  ]
}
```

`effective_at: null` means the fire is suppressed (`skip` policy). `effective_at` differs from `scheduled_at` when the date was shifted by a `run_next_business_day` / `run_prev_business_day` policy.

---

## Programmatic Helpers

Pure functions are available without the `db` feature:

```rust
use autumn_harvest::calendar::{is_excluded_date, apply_skip_policy};
use autumn_harvest::policy::SkipPolicy;
use chrono::NaiveDate;

let excluded = vec![
    NaiveDate::from_ymd_opt(2026, 1, 19).unwrap(), // MLK Day
];

let fire_date = NaiveDate::from_ymd_opt(2026, 1, 19).unwrap();

// Returns None → skip
assert!(apply_skip_policy(fire_date, SkipPolicy::Skip, &excluded).is_none());

// Returns Some(2026-01-20) → Tuesday after the holiday
let next = apply_skip_policy(fire_date, SkipPolicy::RunNextBusinessDay, &excluded);
assert_eq!(next, NaiveDate::from_ymd_opt(2026, 1, 20));
```

---

## Observability

When a firing is suppressed by the calendar, the scheduler emits:

```
harvest.schedule.skipped{workflow="generate_payroll", queue="default", reason="calendar"}
```

This is separate from `reason="overlap"` skips (overlap policy) so dashboards can distinguish holiday-driven suppression from concurrency-driven suppression.

---

## Notes

- Calendar filtering happens **after** jitter is applied and **before** overlap policy evaluation. The effective fire time shown in the preview already includes jitter.
- `weekends-off` is enforced by `apply_skip_policy` checking `weekday()` on the candidate date directly; Saturday and Sunday are never "business days" regardless of what exclusion rows say.
- If a calendar is deleted while schedules still reference it, those schedules degrade gracefully to no filtering (the calendar lookup returns an empty exclusion set).
