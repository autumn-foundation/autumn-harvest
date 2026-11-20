## DLQ depth gauge reads a counter, not the whole DLQ (issue #2677)

`dlq::dead_letter_count` feeds the `harvest.dlq.entries` gauge. It sent
`COUNT(*)` over `harvest_dead_letters`: 744,158 buffers per call at
1,000,000 dead letters, from each worker, for each assigned shard, every
gauge interval. It now reads `dlq::DLQ_DEPTH_SQL`, which calls
`harvest_dead_letter_depth()`: 1 buffer per call at any DLQ size. The count
stays exact, so the `harvest_dlq_growth` alert keeps its meaning.
`docs/design/DESIGN-2677.md` holds the plan and the review record.
`docs/performance-dlq-depth-counter.md` holds the measurement.

- **Migration `20261119233037_harvest_dead_letter_counts`.** One table, four
  functions and three statement triggers on `harvest_dead_letters`. Each
  `INSERT` and `DELETE` statement inserts one delta row, its row count from
  its transition table, in the same transaction. `TRUNCATE` empties the
  counter. A writer locks and reads no counter row. So it never waits, and
  it raises no serialization failure at `REPEATABLE READ` or
  `SERIALIZABLE`.
- **Fold.** `harvest_dead_letter_fold()` folds the free delta rows into one.
  A writer calls it with a chance of 1 in 32, and the DLQ sampler calls it
  each gauge interval through `dlq::fold_dead_letter_counts`.
- **Recount.** `harvest_dead_letter_recount()` and
  `dlq::recount_dead_letters` set the counter from an exact count. The
  function refuses any level above `READ COMMITTED`. `harvest dr promote`
  runs it on each promoted shard, because a logical standby applies rows
  with the triggers off.
- **No new grant.** The trigger, the fold and the depth read are
  `SECURITY DEFINER`, with `pg_temp` last in `search_path`. Only the owner
  runs the recount.
- **Cost of a write.** About 3 to 9 more buffers and 0.03 to 0.05 ms per
  one-row statement. A delete reads 1 more buffer per row for its
  transition table. A long open snapshot does not slow the writers.
- **Preflight.** `dlq_read_access` reads one row of `harvest_dead_letters`
  as well as the depth.
- **Migration runner.** `migrate::apply_in_transaction` pins
  `READ COMMITTED`, so a body that counts after a lock sees every committed
  row.
- **Harness.** `worker_sampler_cadence_perf` gains
  `zz_capture_dead_letter_write_costs`, with a held-snapshot phase and a
  `PERF_WITHOUT_COUNTER=1` baseline mode.

No `WorkflowEvent` variant, no route change, no new metric, no replay
impact.

Tests: `dlq_depth_counter_tests` covers the read cost at 50,000 dead
letters, each write path, open writers that must not wait, writers at
`REPEATABLE READ` and `SERIALIZABLE`, a role with no counter grant, the
recount and its isolation guard, the fold and the migration seed.
`metrics_integration::dlq_gauge_growth_of_six_fires_the_growth_alert` drives
the real sampler and checks the alert rule.
