## DLQ depth gauge reads a counter, not the whole DLQ (issue #2677)

`dlq::dead_letter_count` feeds the `harvest.dlq.entries` gauge. It sent
`COUNT(*)` over `harvest_dead_letters`: 744,158 buffers per call at
1,000,000 dead letters, from each worker, for each assigned shard, every
gauge interval. It now reads `dlq::DLQ_DEPTH_SQL`, the sum of the new
`harvest_dead_letter_counts` table. That read is a few buffers at any DLQ
size. The count stays exact, so the `harvest_dlq_growth` alert keeps its
meaning. `docs/design/DESIGN-2677.md` holds the plan and
`docs/performance-dlq-depth-counter.md` the measurement.

- **Migration `20261119233037_harvest_dead_letter_counts`.** One table, two
  functions and three statement triggers on `harvest_dead_letters`. Each
  `INSERT` and `DELETE` statement adds its row count, from its transition
  table, to one counter row in the same transaction. `TRUNCATE` empties the
  counter. A writer locks a free counter row with `FOR UPDATE SKIP LOCKED`,
  or inserts a new row, so it never waits for another writer. The counter
  adds no lock wait and no deadlock to a failure transaction.
  `harvest_dead_letter_recount()` sets the counter to the exact count and
  folds it into one row. The migration seeds with it under `SHARE ROW
  EXCLUSIVE`.
- **Cost of a write.** The nested counter update reads 6 buffers per
  statement, and a delete reads 1 more buffer per row for its transition
  table. A one-row insert or discard costs 0.02 to 0.07 ms more. The gauge
  read falls from 744,158 buffers to 1 at 1,000,000 dead letters.
- **Grant.** A role that writes or deletes dead letters needs `SELECT`,
  `INSERT` and `UPDATE` on `harvest_dead_letter_counts`. The startup
  preflight checks it.
- **Operations.** The alert runbook and the backup runbook say when to run
  the recount: after a write with the triggers off, or after a data-only
  restore.
- **Harness.** `worker_sampler_cadence_perf` gains
  `zz_capture_dead_letter_write_costs`. Its read capture now matches the new
  statement.

No `WorkflowEvent` variant, no route change, no new metric, no replay
impact.

Tests: `dlq_depth_counter_tests` covers the read plan at 50,000 dead letters,
each write path, open writers that must not wait, the recount and the
migration seed. `metrics_integration::dlq_gauge_growth_of_six_fires_the_growth_alert`
drives the real sampler and checks the alert rule. A preflight unit test
checks the grant.
