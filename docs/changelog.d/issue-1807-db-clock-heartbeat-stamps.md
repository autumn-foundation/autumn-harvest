## Fix — heartbeat stamps use the database clock (issue #1807)

`record_heartbeat` wrote `last_heartbeat_at` from the worker host clock. The
heartbeat-timeout scan compares that column with the database `NOW()`. A host
60 seconds behind the database stamped a heartbeat that was already too old. The
scan then timed out a healthy activity, which caused a false retry and a possible
double execution. A host that ran ahead hid a stuck activity.

The same fault class was fixed for retry deadlines in #1389 and #1392.

### Changes

| Site | Column | Now |
|---|---|---|
| `queue::record_heartbeat` | `last_heartbeat_at` | `clock_timestamp()` |
| `poison_pill` stuck-task requeue | `scheduled_at` | `clock_timestamp()` |
| `queue::force_retry_activity_now` | `scheduled_at` | `clock_timestamp()` |

`queue::db_clock_stamp` builds the expression. `clock_timestamp()` reads the
real time at execution. `NOW()` stays fixed at the start of the transaction.

`force_retry_activity_now` no longer subtracts the 5 second skew allowance. The
stamp and the claim predicate use the same clock. The eligibility check uses
`db_clock_now`. `RetryActivityOutcome::scheduled_at` holds the stored value.

### Tests

- `tests/integration/host_clock_skew_tests.rs` checks each stamp against the
  database clock. One test runs the real heartbeat scan after a heartbeat.
- All five tests failed under `faketime -f -60s` before the fix. All five pass
  under that skew after it.
- `tests/host_clock_write_guard.rs` fails the build when non-test code writes
  `Utc::now()` to `last_heartbeat_at`, `scheduled_at` or `schedule_to_close_at`.
  A deliberate host write needs a `host-clock-ok: <reason>` comment.

### Out of scope

- `EnqueueParams::new` backdates `scheduled_at` by 5 seconds on the host clock.
  The guard marks it. A host more than 5 seconds ahead delays a new task.
- `primary_repend_workflow_task` binds a host-clock `scheduled_at`. Its
  backdate is part of the schedule-to-start latency floor. The text guard
  cannot see a bound parameter.
- The stuck-task requeue compares a host-clock cutoff with `started_at`.
- `batch.rs` and `external_task.rs` stamp `updated_at` from the host.
  They use a lease that is not a `NOW()` timeout scan.

No migration, no `harvest_events` change, and no replay impact.
