## Performance — signal reads by execution use an index (issue #2367)

The Ledger census found four statements that read every signal of one
execution, consumed rows included. The two `harvest_signals` indexes are
partial and skip consumed rows. So each statement read the whole table:

- the signal move of a workflow-level retry,
  `signal::forward_signals_to_retry_attempt` (issue #843);
- the signal scrub of a PII erasure, `erase::db::scrub_execution_node`
  (issue #495);
- the signal copy and the signal delete of a shard rebalance. The copy
  moves to `shard_rebalance::STAGED_SIGNALS_SQL`;
- each `ON DELETE CASCADE` from an execution, which retention triggers.

Migration `20261030182800_harvest_signals_exec_index` builds `idx_harvest_signals_exec` on
`harvest_signals (workflow_exec_id)`. It serves all four. On 100,000 signals,
the retry move falls from 4,481 to 529 buffers, and the execution delete
from 4,151 to 149. The index adds about 2 buffers and 100 WAL bytes to each
signal insert and each consume. See `docs/performance-signals-exec-index.md`.

The `harvest_timers` cascade that the issue also names shipped in issue
#2424 as `idx_harvest_timers_exec`.

**Upgrade.** A plain `CREATE INDEX` takes `SHARE` on `harvest_signals` and
blocks signal writes until the build ends. On a large deployment, build the
index `CONCURRENTLY` first. The header of the migration's `up.sql` gives the
statement.

`census_scan_index_tests` gates each plan with `enable_seqscan` off and a
generic plan. The retry and erasure gates plan the SQL that the engine
sends. The erasure gate also checks that a consumed signal is scrubbed. No
new `WorkflowEvent` variant and no replay impact.
