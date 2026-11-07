# Runtime retry and timeout overrides for an activity type

An activity type declares five defaults in code, through
`#[activity(retry = ..., start_to_close = ..., heartbeat_timeout = ...,
schedule_to_start = ..., schedule_to_close = ...)]`. It can also declare
a fleet-wide cap with `max_concurrent = ...`. An operator override changes
them at runtime, with no restart and no deploy (epic #2384).

Use it when a downstream slows down. Give its activity a longer timeout,
stop its retries, or lower its cap, now.

## Quick reference

```bash
# A payment provider is slow: allow 90 s per attempt and stop the retries.
curl -X PATCH https://<host>/api/harvest/admin/activity-types/charge_card/defaults \
     -H 'Content-Type: application/json' \
     -H 'X-Harvest-Actor: ops-team' \
     -d '{"start_to_close_ms": 90000, "retry_max_attempts": 1, "reason": "INC-5120"}'

# Slow the retries instead: start the backoff at 30 s.
curl -X PATCH https://<host>/api/harvest/admin/activity-types/charge_card/defaults \
     -H 'Content-Type: application/json' \
     -d '{"retry_max_attempts": null, "retry_initial_interval_ms": 30000}'

# A partner API throttles: run at most 2 tasks of the activity at a time.
curl -X PATCH https://<host>/api/harvest/admin/activity-types/call_partner/defaults \
     -H 'Content-Type: application/json' \
     -d '{"max_concurrent": 2, "reason": "INC-5121"}'

# Return start_to_close to its code value, and keep the other fields.
curl -X PATCH https://<host>/api/harvest/admin/activity-types/charge_card/defaults \
     -H 'Content-Type: application/json' \
     -d '{"start_to_close_ms": null}'

# Return every field to its code value.
curl -X PATCH https://<host>/api/harvest/admin/activity-types/charge_card/defaults \
     -H 'Content-Type: application/json' \
     -d '{"clear_override": true}'

# Read the code value, the override and the effective value.
curl https://<host>/api/harvest/admin/activity-types/charge_card/defaults
```

`GET /admin/overrides` and `harvest overrides list` list every activity
override with its actor, reason and time.

## Fields

Each field is tri-state. An absent key keeps the stored value. A `null`
returns the field to the code value. A number sets the override.

| Field | Unit | Replaces |
|---|---|---|
| `retry_max_attempts` | count, the first attempt included | `RetryPolicy::max_attempts` |
| `retry_initial_interval_ms` | ms | `RetryPolicy::initial_interval` |
| `retry_max_interval_ms` | ms | `RetryPolicy::max_interval` |
| `start_to_close_ms` | ms | `default_start_to_close` |
| `heartbeat_timeout_ms` | ms | `default_heartbeat_timeout` |
| `schedule_to_start_ms` | ms | `default_schedule_to_start` |
| `schedule_to_close_ms` | ms | `default_schedule_to_close` |
| `max_concurrent` | count of running tasks | `max_concurrent` |
| `clear_override` | bool | Deletes the stored fields before the others merge. |
| `reason` | text | Stored with the override and shown in the list. |

The backoff coefficient, the jitter and `non_retryable_errors` stay as
declared. Use `retry_max_attempts = 1` to stop the retries.

The write refuses these values:

- a zero value;
- a `retry_max_attempts` or a `max_concurrent` over 2147483647;
- a timeout over 100 years;
- a retry interval over 365 days;
- a merged `retry_max_interval_ms` below `retry_initial_interval_ms`.

The route also refuses a field that cannot apply:

- a retry field when the activity declares no retry policy, because its
  author did not declare it safe to retry;
- `heartbeat_timeout_ms` when the activity declares no heartbeat timeout,
  because an activity that does not heartbeat would time out on each
  attempt;
- `heartbeat_timeout_ms`, `schedule_to_start_ms` or `schedule_to_close_ms`
  for a local activity, because a local activity has none of them;
- `max_concurrent` when the activity declares no cap, because its tasks
  have no concurrency key;
- `max_concurrent` for a local activity, because it has no task row;
- `max_concurrent` when another activity shares the concurrency key,
  because one key would then hold two caps;
- any field of a reserved engine activity.

Each `PATCH` writes one `activity_type.defaults` audit row, refused or not.
A change and its audit row commit in one transaction, so no change applies
unaudited.

## Precedence

> call site, then override, then code, then builder default.

A value that the workflow passes at the call site still wins. For example,
`ctx.execute_activity_with_opts(&info, input, None, Some(policy), None)`
keeps `policy`. A DAG task's `.retry()` and `.start_to_close()` are call-site
values too.

A field that the override does not set inherits the code value. The builder
default from `WorkerConfig` applies only when neither sets a value. An
override that sets `schedule_to_close_ms` counts as an attempt bound, so the
builder default `start_to_close` then does not apply.

A local activity takes `retry_*` and `start_to_close_ms`. A raw local call,
such as `execute_local_activity_raw` from a DAG, reads no code value. It
resolves call site, then override, then builder default, and a retry field
applies only over the builder policy. The worker still
clamps its `start_to_close` to `WorkerConfig::max_local_activity_start_to_close`.
`effective` does not show that clamp, because the cap belongs to each worker.

A fan-out resolves its values once, when the call starts. So a windowed
fan-out keeps the values of its start for each later window.

## When a change takes effect

The process that serves the `PATCH` applies it at its next schedule. Each
other process reloads the overrides within five seconds plus one refresh
period. The workflow run-default overrides share that cache and its refresh.

## The concurrency cap

The cap is the one field that also applies to tasks that are already
scheduled. Each task row stores its cap, and each claim compares the
running count of the key to that stored cap. So a `PATCH` that names
`max_concurrent` or `clear_override` sets the stored cap of each live row
of the activity, on every shard:

- A pending task waits under the new cap. The next claim already uses it.
- A running task runs on. The pass changes the cap column only, never the
  state, the worker or the attempt. When its count is above a lower cap, no
  new task of the key starts until enough tasks finish.
- A retry of a running task waits under the new cap.

The response reports the first pass in `cap_rewrite`:

```json
"cap_rewrite": { "rewritten_tasks": 140, "unavailable_shards": [], "superseded": false }
```

The pass runs again after 10 seconds, 1 minute and 5 minutes. Another
process applies a change only after its cache reloads. A task that it
schedules before then stores the old cap, and a later pass sets it. A pass
also sets a row that an earlier pass skipped because another transaction
held it. A pass stops when a newer change replaced the cap. A shard in
`unavailable_shards` keeps the old cap until a later pass reaches it. Send
the request again to retry it at once.

A dead-letter replay or redrive also puts a task back under the current
cap.

The key stays as declared. The adaptive limit of each worker is separate,
and it moves on its own. `GET /admin/concurrency` shows the cap that the
live rows store.

## Activities already scheduled

A change to any other field applies to the activities scheduled after it.
An activity that is already scheduled keeps its values:

- The task row stores the resolved retry policy and timeouts. A retry reuses
  the row. So a running or waiting activity keeps its budget and its bounds.
- `ActivityScheduled` stores none of them. So the replay of a run that spans
  the change succeeds.
- A local activity freezes its values in `LocalActivityScheduled`. A local
  activity that recovers mid-retry keeps them.

To change an activity that is already scheduled, cancel or reset its run.

## Mixed versions

Set an override only after every worker runs this release. An older worker
ignores the table, and it schedules with the code values. An older process
answers the route with a 404.

To roll back, deploy the older release first. Then run the down migration of
`20261107194243_harvest_activity_default_max_concurrent`, then the one of
`20261106145707_harvest_activity_default_overrides`. The task rows keep the
cap that the last pass stored, and each one drains under it. This release reads the
table, so its activity refresh fails once the table is gone. The workflow
overrides still load.

## Grants

The runtime role needs `SELECT`, `INSERT`, `UPDATE` and `DELETE` on
`harvest_activity_default_overrides`. The cap pass updates
`harvest_task_queue`, as the run-default concurrency raise does. `GET /admin/preflight` reports a
missing grant.

## See also

- [`run-default-overrides.md`](run-default-overrides.md): the same idea for
  workflow types.
- [`schedule-overrides.md`](schedule-overrides.md): the same idea for
  schedules.
