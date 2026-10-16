# Design — Issue #2031: schedules, retention and the append-only guard on SQLite

The SQLite backend has no schedules and no retention. Its `harvest_events`
table has no append-only guard. This change adds all three.

**No new `WorkflowEvent` variant.** The schema gains one table, one column,
two triggers and four indexes. Each change is additive.

---

## 0. Planning record

### 0.1 Facts found before the plan

- No SQLite code path updates `harvest_events`. The store only inserts and
  reads events. So a guard breaks no current writer.
- SQLite has no erase and no codec rotation. So it has no sanctioned event
  rewrite, and its guard needs no sanction flag.
- `INSERT OR REPLACE` deletes the old row and inserts a new one. It fires no
  `UPDATE` trigger. A guard on `UPDATE` alone does not stop it.
- `schedule_decision.rs` and `scheduler.rs` are behind the core `db`
  feature. The SQLite crate builds the core without `db`. The cron and
  interval arithmetic (`next_run_after`) is pure, but it is private to
  `scheduler.rs`.
- `Schedule`, `WorkflowSchedule`, `OverlapPolicy` and `validate_schedule` are
  in `policy.rs`, which has no `db` gate.
- Postgres keys a workflow schedule by its workflow name. With catch-up off,
  it fires the oldest due slot once. The next slot is the first slot after
  that one, or the first slot after now when that slot is already past.
- Postgres counts the active runs of a schedule by its schedule id, and
  includes continue-as-new successors.
- A terminal write sets `closed_at` in epoch microseconds (issue #2027). A
  row written before that issue has no `closed_at`.
- A continue-as-new successor keeps the `(workflow_name, workflow_id)` key.
  A keyed signal of the chain stays on the run that received it. The chain
  dedupe (issue #2028) reads every run of the key.
- Postgres retention selects a run by its close time only. It removes an
  old run of a chain even while the newest run of the chain runs.
- A retry successor takes all signal rows of the failed run with it.
- `harvest_tasks` and `harvest_activity_attempts` have no `exec_id` index.

### 0.2 Brainstorm — how can SQLite add the three features?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A `BEFORE UPDATE` trigger on `harvest_events` that aborts. | **Adopted.** It is the issue's direction. |
| B2 | A `BEFORE INSERT` trigger that aborts when the key already exists. | **Adopted.** It closes `INSERT OR REPLACE`. |
| B3 | Apply the triggers on every open with `IF NOT EXISTS`, as `APPEND_ONLY_GUARD` after `SCHEMA`. | **Adopted.** An old file gains the guard on its next open. A test fixture can drop and restore the same text. |
| B4 | A sanction flag in a `TEMP` table, as on Postgres. | Rejected. SQLite has no sanctioned writer. |
| B5 | Accept the core `WorkflowSchedule`. Reject each field this backend cannot honor, by name. | **Adopted.** One declaration works on both backends. |
| B6 | A new SQLite-only schedule type. | Rejected. Users write a schedule twice. |
| B7 | Move `next_run_after` and the catch-up-off plan to a core module with no `db` gate. | **Adopted.** Both backends then compute the same slots. |
| B8 | Fire due schedules at the start of each `poll_once` pass. | **Adopted.** The new run is driven in the same pass. |
| B9 | Fire, start the run and move `next_run_at` in one transaction. | **Adopted.** A crash cannot fire a slot twice. |
| B10 | A deterministic `workflow_id` per slot, started with `RejectDuplicate`. | **Adopted.** It mirrors Postgres. |
| B11 | A `schedule_name` column on `harvest_executions`. | **Adopted.** The overlap count reads it. A successor copies it. |
| B12 | `purge_terminal(older_than)` that deletes in batches. | **Adopted.** Each batch is one short transaction. |
| B12a | Keep a terminal run while its key has a `RUNNING` run. | Rejected in review. It differs from Postgres. A chain that continues as new for ever then grows for ever. Its probe is quadratic in the chain length. |
| B13 | Purge by a `DELETE ... RETURNING` cascade with foreign keys. | Rejected. The tables have no foreign keys, and old files cannot gain them. |
| B14 | Run `VACUUM` after a purge. | Rejected. It rewrites the whole file. Freed pages are reused. |
| B15 | Honor schedule jitter. | Rejected. Jitter spreads a fleet. One writer has no fleet. Retry jitter has the same rule. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The guard blocks a current writer. | No writer updates events (0.1). The full suite runs with the guard. |
| R2 | `INSERT OR REPLACE` rewrites an event past the guard. | The `BEFORE INSERT` trigger (B2). A test proves it. |
| R3 | An old file never gets the guard. | `open` applies `APPEND_ONLY_GUARD` on every open. A test opens an old file. |
| R4 | A reopen resets a schedule, so a slot fires late or twice. | `next_run_at` is durable. A re-upsert of the same spec keeps it. A test reopens. |
| R5 | A crash between the start and the slot update fires the slot twice. | One transaction (B9). The slot `workflow_id` is a second guard (B10). |
| R6 | A long downtime fires a burst of runs. | Catch-up off fires one run, then realigns. Catch-up on is rejected. A test covers it. |
| R7 | A schedule fires while its last run still runs. | `Skip` counts `RUNNING` rows of the schedule, successors included. A run past its deadline or with a pending cancel does not count. Tests cover each case. |
| R8 | A silently ignored field changes the run. | Each field this backend cannot honor is rejected by name. A test covers each. |
| R9 | A schedule for an unregistered workflow loses its slot, or stops the fleet. | The slot stays due and the fire logs a warning. The pass does not fail. |
| R9a | `run_until_idle` never quiesces, because each pass meets a new slot. | It fires on its first pass only. |
| R10 | A purge removes a live run. | Only a known terminal state with a `closed_at` before the cutoff is a candidate. |
| R11 | A purge breaks the signal dedupe of a live chain. | Accepted, as on Postgres. Only the close time counts. The window bounds the risk. Section 2 records it. |
| R12 | A purge leaves rows behind. | It deletes events, tasks, timers, signals and attempts. A test counts each table. |
| R13 | A purge holds the write lock for a long time. | Batches of 256 runs, one transaction each, on a keyset cursor. Every delete seeks an `exec_id` index. A plan test pins it. |
| R14 | A purge removes a pre-#2027 run of unknown age. | A row with no `closed_at` stays. Section 2 records it. |
| R15 | The core refactor changes the Postgres slots. | The scheduler calls the moved code unchanged. Its unit tests still run. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. No current writer updates events. The
  scheduler maths is pure but gated.
- **Red (feelings).** Rejecting many schedule fields feels strict. A schedule
  that silently differs from Postgres feels worse.
- **Black (risks).** Section 0.3. A purge after a long gap can remove many
  runs. Each batch is short, so other work interleaves.
- **Yellow (benefits).** An embedded app drops its own timer loop. The file
  stops growing without bound. History is guarded as on Postgres.
- **Green (ideas).** Catch-up policies, `max_runs` and `end_at` later. An
  incremental vacuum for files that must shrink.
- **Blue (process).** Red tests and stubs, then green code, then refactor.
  Then a review from several angles, and an AC table with evidence.

---

## 1. Change

### 1.1 The append-only guard

`schema::APPEND_ONLY_GUARD` holds two triggers on `harvest_events`. `open`
applies it after `SCHEMA`:

- `harvest_events_append_only_upd` aborts every `UPDATE`.
- `harvest_events_append_only_ins` aborts an `INSERT` whose `(exec_id, seq)`
  already exists. A plain duplicate already fails on the key. This trigger
  also stops `INSERT OR REPLACE`.

`DELETE` is not guarded. Retention needs it, as on Postgres. The hidden
`append_only::with_guard_off` lets a test fixture damage a row. It drops
the triggers, runs the write and restores them in one transaction.

### 1.2 Retention

`SqliteRuntime::purge_terminal(older_than)` removes each run that is in a
terminal state and has a `closed_at` older than `now - older_than`. This is
the Postgres rule. The window counts on the real clock, as `closed_at` does.

It deletes the run's events, tasks, timers, signals, activity attempts and
row. It works in batches of 256 runs on a `(closed_at, exec_id)` cursor. A
damaged id is skipped. It returns the number of runs removed.

### 1.3 Schedules

The table `harvest_schedules` holds one row per workflow name:

| Column | Meaning |
|---|---|
| `workflow_name` | The key, as on Postgres. |
| `schedule_json` | The `Schedule` as JSON. |
| `input_json` | The input of each run. |
| `overlap_policy` | `skip` or `allow_all`. |
| `max_active_runs` | The `skip` limit. |
| `paused` | 1 when paused. |
| `next_run_at` | The next slot in epoch milliseconds. NULL never fires. |
| `last_run_at` | The slot of the last start, in epoch milliseconds. |

`harvest_executions.schedule_name` names the schedule that started a run. A
continue-as-new or retry successor copies it.

API on `SqliteRuntime`: `upsert_schedule`, `delete_schedule`,
`pause_schedule`, `resume_schedule` and `list_schedules`.

`poll_once`, `poll_once_as_of` and `run_until_idle` fire each due schedule
before they drive the fleet. `run_until_idle` fires on its first pass only.
`run_until_blocked` drives one run and fires nothing. A schedule that
cannot fire logs a warning and keeps its slot due.

A fire starts one run for the due slot, with `workflow_id`
`sched:<workflow_name>:<slot seconds>`. Under `skip`, a schedule at its
`max_active_runs` drops the slot. A run past its deadline or with a pending
cancel does not count. Either way, `next_run_at` moves to the next slot in
the same transaction.

The core module `autumn_harvest::schedule_plan` holds the slot arithmetic.
The Postgres scheduler calls it too.

---

## 2. Known limits

- A run closed before issue #2027 has no `closed_at`. A purge keeps it.
- A purge removes an old run of a live chain, as on Postgres. Its
  `successor` is then `ExecutionNotFound`. A keyed signal that it received
  no longer dedupes a retry.
- A purge frees pages for reuse. The file does not shrink. Run `VACUUM` to
  shrink it.
- A purged run is gone. A later start with its `workflow_id` creates a fresh
  run, as after a Postgres retention sweep.
- Schedules ignore jitter. They support catch-up off only, and the `skip` and
  `allow_all` overlap policies.
- A schedule fires only while the app polls. A fire is late by up to one
  poll interval.

---

## 3. Review record

Four review agents read the change from four angles: schedule correctness,
retention and the guard, the core refactor with the docs, and test quality.
A Codex review found nothing. The fixes:

- `Skip` counted a run past its deadline, and a run with a pending cancel.
  Postgres has already ended both. The count now skips them.
- `run_until_idle` met a new slot on each pass of a slow clock and ended
  `Runaway`. It now fires on its first pass only.
- A schedule for a removed workflow failed every `poll_once`. A fire error
  now logs a warning and keeps the slot due.
- A slot that already had its run did not become `last_run_at`. It now does.
- The live-key rule of retention differed from Postgres. It let a long
  chain grow for ever, and its probe was quadratic. It is gone.
- The deletes on `harvest_tasks` and `harvest_activity_attempts` scanned
  the whole table. Both tables now have an `exec_id` index.
- A damaged id could stall retention. A keyset cursor now moves past it.
- The purge window used the runtime clock, but `closed_at` uses the real
  clock. The window now uses the real clock, with a hidden `_as_of` seam.
- The guard broke two daemon fixtures that damage an event. They now call
  `append_only::with_guard_off`.
- Stale "no scheduler" and "no retention" text in the agent crate, the
  guide and the comments. Missing `# Errors` variants.
- New tests close each gap that a mutation exposed.

---

## 4. Tests and acceptance criteria

| AC | Test |
|----|------|
| An interval schedule starts runs on time across a reopen. | `schedules::an_interval_schedule_starts_runs_on_time_across_a_reopen`, `schedules::a_late_fire_keeps_the_cadence` |
| Purge removes terminal runs past the window and keeps live ones. | `retention::purge_removes_terminal_runs_past_the_window_and_keeps_live_ones`, `retention::purge_never_removes_a_running_row` |
| An `UPDATE` on `harvest_events` fails. | `append_only_guard::an_update_on_harvest_events_fails`, `append_only_guard::an_old_file_gains_the_guard_on_open` |
