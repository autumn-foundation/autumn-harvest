## Phase X.Y — By-id task row statements stay on the primary key (issue #2713)

A statement that finds a `harvest_task_queue` row by `id` guards it on
`state` and `worker_id`. A partial index matches each guard, so the planner
could choose that index over the primary key. The claim-release rechecks
then read every `RUNNING` row of their worker. A plan from idle-table
statistics read every live row.

- `row_guard_sql!` and `row_guard` wrap the guard in
  `CASE WHEN <guard> THEN true END`. No index can match it, so the plan
  reads the primary key. `WHERE` drops NULL as it drops false, so each
  statement keeps the same rows.
- The fix covers the pause rechecks and the unconditional and over-cap
  releases. It also covers the park statements, the claim fence of each
  fenced write and the claim locks. The capability-miss statements, the
  suspended-claim release, the pending-hint read, the orphan requeue and the
  other by-id releases, requeues, defers and fails are in scope too.
- Two unit tests walk the engine source, so a new plain by-id guard fails.
- The by-id claim is not in scope. Issue #2751 measures it.
- No migration, no new event variant, no change to `harvest_events`.
- Evidence: `row_guard_plan_db_tests` checks the plan of each statement at
  5, 20, 50 and 200 `RUNNING` rows per worker and after an idle analyze. It
  checks the generic and the custom plan. It also proves the same rows by a
  truth table, a differential run and a concurrent writer.
- On a 1M-row fixture, a recheck reads 24 → 6 buffers at 20 rows per
  worker, and 201 → 6 after an idle analyze. End to end, the three rechecks
  read 24.5% fewer buffers. See `docs/performance-claim-release-guard.md`.
