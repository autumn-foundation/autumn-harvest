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
| `poison_pill::requeue_orphan_stmt` | `scheduled_at` | `clock_timestamp()` |
| `queue::force_retry_activity_now` | `scheduled_at` | `clock_timestamp()` |

`queue::db_clock_stamp` builds the expression. `clock_timestamp()` reads the
real time at execution. `NOW()` stays fixed at the start of the transaction.

`force_retry_activity_now` no longer subtracts the 5 second skew allowance. The
stamp and the claim predicate use the same clock. The eligibility check uses
`db_clock_now`. `RetryActivityOutcome::scheduled_at` holds the stored value.
The dispatch hint caps its due time at the host time. The dispatcher compares
the due time with the host clock.

### Tests

- `tests/integration/host_clock_skew_tests.rs` checks each stamp against the
  database clock. One test runs the real heartbeat scan after a heartbeat.
- All five tests failed under `faketime -f -60s` before the fix. All five pass
  under that skew after it.
- `tests/host_clock_write_guard.rs` fails the build when non-test code writes
  `Utc::now()` to `last_heartbeat_at`, `scheduled_at` or `schedule_to_close_at`.
  A deliberate host write needs a `host-clock-ok: <reason>` comment.

### Out of scope

Each item needs its own design decision. Open a follow-up issue for each.

- `EnqueueParams::new` backdates `scheduled_at` by 5 seconds on the host clock.
  The guard marks it. A host more than 5 seconds ahead delays a new task.
- `worker.rs` writes `schedule_to_close_at` for an activity as host time plus
  the timeout. The value passes through a local variable, so the text guard
  cannot see it. The task-queue scan and `claim_task` compare it with `NOW()`.
- `worker.rs` and `execution.rs` stamp `deadline_at`, `sla_deadline_at`,
  `chain_deadline_at` and `child_sla_deadline_at` from the host at start,
  redrive and resume. Moving them to the database clock needs a replay review.
- The rate-limit and session-acquire deferrals in `worker.rs` pass a host-clock
  `scheduled_at` to `queue::defer_claimed_rate_limited_task`. The guard marks
  both. A fix changes that public function to take a delay.
- `primary_repend_workflow_task` binds a host-clock `scheduled_at`. Its
  backdate is part of the schedule-to-start latency floor.
- The orphan and stuck-task scans compare a host-clock cutoff with database
  stamps. `requeue_orphan_stmt` binds a host `now` in its liveness check.
- `batch.rs` and `external_task.rs` stamp `updated_at` from the host. They use
  a lease that is not a `NOW()` timeout scan.
- `external_task.rs` stamps `schedule_to_close_at` from the host. Its scan also
  uses the host clock, so the two agree. The guard marks it.

The guard follows a value only when `Utc::now` is in the same expression as the
column. A value that passes through a local variable is not covered.

No migration, no `harvest_events` change, and no replay impact.
