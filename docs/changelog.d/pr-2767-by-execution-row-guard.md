## Phase X.Y — Keyed task row statements stay on their key index (issue #2713, PR #2767)

A statement that finds task rows by `workflow_exec_id` guards them on
`state` or `worker_id`. A partial index matches each guard, so the planner
could choose it over the execution index. The wake fallback then read every
`RUNNING` row in the table. With idle statistics, a wake read every live
row.

- `row_guard_sql!` and `row_guard` now also wrap the by-execution guards.
  `WHERE` drops NULL as it drops false, so each statement keeps the same
  rows.
- Migration `20261120145312_harvest_tq_workflow_task_index` adds
  `idx_harvest_tq_workflow_task (workflow_exec_id) WHERE task_type =
  'workflow'`. A wake, the owner key read and the migration statements
  keep `task_type = 'workflow'` plain and read one row for any run size.
  The build is guarded. Its header gives the `CONCURRENTLY` recipe. An
  update of a workflow task row adds about 40 bytes of WAL.
- The fix covers the three wake statements, `owner_keys`, the fail and
  cancel of open tasks, and the session liveness check. It also covers the
  deadline shift of a resume, the pending-row delete of a cancel, the run
  drains, the fork loser cancel, the in-flight counts, and the parked-row
  read, seal cancel and signal re-pend of a migration.
- The timeout batch and probe re-reads load their ids through the primary
  key in a materialized CTE.
- Each guarded statement names its key index. The plan test checks it at 5
  to 200 `RUNNING` rows per worker and after an idle analyze. It fails when
  no plain form leaves its key index.
- The source walks read every engine file and each top-level term of a
  keyed `WHERE` clause. They also find a `concat!` that drops
  `row_guard_sql!`.
- No new event variant and no change to `harvest_events`.
- On a 1M-row fixture, the wake fallback reads 509 → 9 buffers at 5 rows
  per worker. After an idle analyze, the wake re-pend reads 1,411 → 4. For a
  run with 50,000 rows, it reads 1,343 → 4. In `e2e_bench`, the due-row
  read of a wake reads 22.21 → 3.76 buffers per call, all statements read
  2.5% fewer buffers, and WAL grows by 3.0%. See
  `docs/performance-claim-release-guard.md`.
