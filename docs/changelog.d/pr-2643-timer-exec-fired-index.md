## Open-timer lookups read only the open timers of a run (issue #2643)

Each workflow decision reads the open timers of its run from
`harvest_timers`, by `workflow_exec_id` and `fired`. A fired timer row stays
until retention deletes its run. The old `idx_harvest_timers_exec` held
`workflow_exec_id` only. So each read fetched every timer that the run had
fired, and its cost grew with the length of the run.

- Two migrations give `idx_harvest_timers_exec` the shape
  `(workflow_exec_id, fired)`. `20261110194313` builds
  `idx_harvest_timers_exec_fired` with a guarded plain build (`SHARE` on
  `harvest_timers`, `lock_timeout` 5 s). `20261110194400` drops the old index
  in its own transaction (`ACCESS EXCLUSIVE` for a short time) and gives the
  new index the old name. Each header gives the out-of-band `CONCURRENTLY`
  recipe. The upgrade guide has a row for each migration.
- The leading column keeps each use of the old index: the rebalance timer
  copy and delete, and each `ON DELETE CASCADE` from an execution. The
  rebalance plan gate passes unchanged.
- Diesel binds `fired` as a parameter, so a partial `WHERE NOT fired` index
  cannot serve a cached generic plan. A plain key column can. The three-column
  form with `timer_id` was rejected: 55 MB against 8.7 MB on the fixture.
- Measured with a worker-driven harness,
  `tests/integration/timer_pending_exec_lookup_perf.rs`, on a seeded
  1.13-million-row fixture. Timer-lookup buffers fall 47%, 66% and 78% for
  runs of 250, 1,000 and 2,000 timers, and buffers per lookup are flat at 4.0.
  WAL per run changes by under 1%. Timer digests and five full-result hashes
  match before and after. See `docs/performance-timer-pending-exec-lookup.md`.
- No query text changes. No new `WorkflowEvent` variant. No schema column
  changes.
