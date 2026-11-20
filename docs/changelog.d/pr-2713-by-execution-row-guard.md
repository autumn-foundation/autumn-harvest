## Phase X.Y — By-execution task row statements stay on their key index (issue #2713)

A statement that finds task rows by `workflow_exec_id` guards them on
`state`, `task_type` or `worker_id`. A partial index matches each guard, so
the planner could choose it over `idx_harvest_tq_workflow`. The wake
fallback then read every `RUNNING` row in the table. With idle statistics,
a wake read every live row.

- `row_guard_sql!` and `row_guard` now also wrap the by-execution guards.
  `WHERE` drops NULL as it drops false, so each statement keeps the same
  rows.
- The fix covers the three wake statements, `owner_keys`, the fail and
  cancel of open tasks, and the session liveness check. It also covers the
  deadline shift of a resume, the pending-row delete of a cancel, the run
  drains, the fork loser cancel, the in-flight counts, and the parked-row
  read, seal cancel and signal re-pend of a migration.
- Each guarded statement names its key index. The plan test checks it at 5
  to 200 `RUNNING` rows per worker and after an idle analyze.
- The source walks now read every engine file. They fail on a plain guard
  beside an `id` or a `workflow_exec_id` key.
- No migration, no new event variant, no change to `harvest_events`.
- Evidence: `row_guard_plan_db_tests` checks the plans, a differential run
  over one execution per row, and a wake fallback and re-pend beside a
  concurrent park.
- On a 1M-row fixture, the wake fallback reads 507 → 9 buffers at 5 rows
  per worker. After an idle analyze, the wake re-pend reads 1,411 → 4. See
  `docs/performance-claim-release-guard.md`.
