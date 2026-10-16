## Phase — SQLite: schedules, retention and the append-only guard (issue #2031)

The SQLite backend had no schedules and no retention. Its `harvest_events`
table had no append-only guard.

**What shipped**

- `SqliteRuntime::upsert_schedule(&WorkflowSchedule)` stores a cron or
  interval schedule in the new `harvest_schedules` table. The workflow name
  is the key, as on Postgres. An upsert with the same spec keeps its next
  slot and its pause, so an app can upsert on every open.
- `poll_once`, `poll_once_as_of` and `run_until_idle` fire each due schedule
  before they drive the fleet. `run_until_idle` fires on its first pass only.
  A schedule that cannot fire logs a warning and keeps its slot due. One transaction checks the overlap, starts the
  run and moves the slot. The run of a slot has the `workflow_id`
  `sched:<workflow>:<slot seconds>`.
- Catch-up is off: after a gap, a schedule fires one run and realigns. The
  overlap policies are `Skip` and `AllowAll`. `Skip` counts continue-as-new
  and retry successors, through the new `harvest_executions.schedule_name`
  column. It does not count a run past its deadline or with a pending
  cancel. `jitter` is ignored.
- `pause_schedule`, `resume_schedule`, `delete_schedule` and
  `list_schedules`. A resume counts the next slot from now.
- Each other `WorkflowSchedule` field this backend cannot honor is rejected
  by name with `SqliteError::Unsupported`. New errors:
  `SqliteError::InvalidSchedule` and `SqliteError::ScheduleNotFound`.
- `SqliteRuntime::purge_terminal(older_than)` removes each terminal run
  closed before the window, with its events, tasks, timers, signals and
  attempts, in batches of 256 on a keyset cursor. Only the close time
  counts, as on Postgres. It keeps every `RUNNING` run. A run with no
  `closed_at` stays. New `exec_id` indexes on `harvest_tasks` and
  `harvest_activity_attempts` keep the deletes off a full scan.
- Two triggers keep `harvest_events` append-only. They refuse every `UPDATE`
  and an `INSERT OR REPLACE` of a stored event. `DELETE` stays open. An
  older file gains them on its next open. A test fixture that must damage
  an event calls the hidden `append_only::with_guard_off`. Two daemon
  fixtures now do.
- Core: the new `autumn_harvest::schedule_plan` module holds the cron and
  interval slot arithmetic with no `db` gate. The Postgres scheduler calls
  it, unchanged in behavior.

**Migration**

Additive, applied on open: the `harvest_schedules` table, the
`harvest_executions.schedule_name` column, four indexes and two triggers. No new `WorkflowEvent` variant.

**Test evidence**

- `schedules.rs` (24 tests): a reopen keeps every slot and the next run
  starts on time; a late fire keeps the cadence; a gap fires once; cron and
  cron-in-zone slots; `Skip`, `AllowAll` and `max_active_runs`; successors,
  overdue and cancelled runs; pause, resume and delete; a slot that has its
  run; a fast clock under `run_until_idle`; unregistered and damaged rows;
  each refused field.
- `retention.rs` (7 tests): every table is emptied for a purged run; live,
  in-window and pre-#2027 runs stay; an old run of a live chain goes; a
  damaged id is skipped; more runs than one batch. A unit test pins an index
  seek for every delete.
- `append_only_guard.rs` (5 tests): every `UPDATE` form fails; the replace
  and upsert forms fail; `DELETE` works; an old file gains the guard;
  `with_guard_off` restores it, also after an error.
