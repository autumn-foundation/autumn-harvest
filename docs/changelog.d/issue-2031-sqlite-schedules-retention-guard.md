## Phase — SQLite: schedules, retention and the append-only guard (issue #2031)

The SQLite backend had no schedules and no retention. Its `harvest_events`
table had no append-only guard.

**What shipped**

- `SqliteRuntime::upsert_schedule(&WorkflowSchedule)` stores a cron or
  interval schedule in the new `harvest_schedules` table. The workflow name
  is the key, as on Postgres. An upsert with the same spec keeps its next
  slot and its pause, so an app can upsert on every open.
- `poll_once`, `poll_once_as_of` and `run_until_idle` fire each due schedule
  before they drive the fleet. One transaction checks the overlap, starts the
  run and moves the slot. The run of a slot has the `workflow_id`
  `sched:<workflow>:<slot seconds>`.
- Catch-up is off: after a gap, a schedule fires one run and realigns. The
  overlap policies are `Skip` and `AllowAll`. `Skip` counts continue-as-new
  and retry successors, through the new `harvest_executions.schedule_name`
  column. `jitter` is ignored.
- `pause_schedule`, `resume_schedule`, `delete_schedule` and
  `list_schedules`. A resume counts the next slot from now.
- Each other `WorkflowSchedule` field this backend cannot honor is rejected
  by name with `SqliteError::Unsupported`. New errors:
  `SqliteError::InvalidSchedule` and `SqliteError::ScheduleNotFound`.
- `SqliteRuntime::purge_terminal(older_than)` removes each terminal run
  closed before the window, with its events, tasks, timers, signals and
  attempts, in batches of 256. It keeps live runs, and a terminal run whose
  key has a `RUNNING` run. A run with no `closed_at` stays.
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
`harvest_executions.schedule_name` column, two partial indexes and two
triggers. No new `WorkflowEvent` variant.
