## Performance — scaling signals, canary status and shard rebalance use an index (issue #2424)

The Ledger census of the plugin suites found four statements that read a whole
table. Each one now reads an index.

- **Scaling signals.** `queue::queue_task_counts` counted every task row,
  finished rows included. An autoscaler polls it on every shard. It now reads
  `PENDING` and `RUNNING` rows only, through the existing
  `idx_harvest_tq_live_created`. The counts do not change. A queue with only
  finished tasks no longer gets an all-zero row from the task side. It still
  gets a row when a healthy, active worker lists it.
- **Canary status.** `GET /admin/canary` read each execution row to find the
  canary runs. The statement moves to `canary::CANARY_ROWS_SQL`. It names the
  canary prefix as a literal, so a partial index can serve it. The new index
  `idx_harvest_we_canary_completed` holds canary runs only, newest first. The
  read stops after its row cap.
- **Shard rebalance.** The quiescence check and the cutover read every
  `harvest_cross_shard_children` row to find the children of one parent. The
  timer copy and the timer delete read every `harvest_timers` row. The new
  indexes `idx_harvest_cross_shard_children_parent` and
  `idx_harvest_timers_exec` serve them. The timer index also serves each
  `ON DELETE CASCADE` from an execution, which retention triggers.

Migration `20261030015439_harvest_census_scan_indexes` builds the three
indexes. Migration `20260901130054` left out the parent index on purpose. Since
then, the rebalance check and the PII erasure added lookups by parent.

**Upgrade.** A plain `CREATE INDEX` takes `SHARE` on its table and blocks
writes until the build ends. On a large deployment, build the three indexes
`CONCURRENTLY` first. The header of the migration's `up.sql` gives the
statements.

`census_scan_index_tests` gates each plan with `enable_seqscan` off and a
generic plan. It also checks the counts of each live state, and that the canary
read matches the prefix only, newest first, under its cap. No new
`WorkflowEvent` variant and no replay impact.
