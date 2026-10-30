# Signal reads by execution use an index (issue #2367)

The Ledger census found four statements that read every `harvest_signals`
row of one execution, consumed rows included. No index served them, so each
one read the whole table. Migration `20261030182800_harvest_signals_exec_index`
adds `idx_harvest_signals_exec` on `harvest_signals (workflow_exec_id)`.

## 🎯 Workload

Each statement reads the signals of one execution:

- `signal::forward_signals_to_retry_attempt` moves the mailbox of a failed
  attempt to its workflow-level retry (issue #843).
- `erase::db::scrub_execution_node` tombstones the signal payloads of a PII
  erasure (issue #495).
- `shard_rebalance::db::stage_copy` copies the signals of a moved execution
  with `STAGED_SIGNALS_SQL`. The staged delete removes them.
- Retention's `delete_candidate_execution` deletes an execution. Postgres
  then runs the `ON DELETE CASCADE` of `harvest_signals` as a nested query.

`idx_harvest_signals_pending` and `uq_harvest_signals_idem` are partial
indexes. Neither one holds a consumed row, so neither can serve a statement
that must reach consumed rows.

The fixture has 4,000 completed executions with 25 signals each, 100,000
rows in all. 24 of each 25 signals are consumed. The table is 4,000 pages.

## 📈 Profile

PostgreSQL 16, `pg_stat_statements.track = all`. One pass loads the catalog
caches. The second pass is measured. A count is `shared_blks_hit +
shared_blks_read` for one call. A top-level count includes its nested
queries.

| Statement | Before | After |
|---|---:|---:|
| Retry move (`UPDATE ... SET workflow_exec_id`) | 4,481 | 529 |
| Erasure scrub (`UPDATE ... SET payload`) | 4,261 | 310 |
| Rebalance copy (`STAGED_SIGNALS_SQL`) | 4,030 | 28 |
| Execution delete, top level | 4,151 | 149 |
| — its nested `harvest_signals` cascade | 4,055 | 53 |

Before, each statement read all 4,000 pages of the table. After, each one
reads the 25 rows of its execution. The cost no longer grows with the table.

## 💸 Write cost

The index adds one entry for each signal insert. A consume update changes
`consumed`, which is in the predicate of `idx_harvest_signals_pending`. So a
consume update was never HOT, and the new index adds one entry for it too.

| 1,000 rows | Buffers before | Buffers after | WAL bytes before | WAL bytes after |
|---|---:|---:|---:|---:|
| Signal insert | 11,990 | 13,981 | 346,972 | 431,936 |
| Consume update | 15,199 | 17,460 | 257,200 | 364,970 |

That is about 2 buffers and 100 WAL bytes for each signal write. The index
is 100 pages for 100,000 rows. The consume statement here scans by
`signal_name`, so its buffer count is not the cost of the engine's consume.
Only the difference applies.

## 🔁 Reproduce

The fixture and the statements are in
[`perf-artifacts/signals-exec-index/`](perf-artifacts/signals-exec-index/).
The results are in `before.txt` and `after.txt`. A count varies by about 1%
between runs. Load `pg_stat_statements` in `shared_preload_libraries` first.

```sh
new=autumn-harvest/migrations/20261030182800_harvest_signals_exec_index
createdb sig_before
for dir in autumn-harvest/migrations/*/; do
  [ "${dir%/}" = "$new" ] && break
  psql -d sig_before -1 -f "$dir/up.sql"
done
cd docs/perf-artifacts/signals-exec-index
psql -d sig_before -c 'CREATE EXTENSION pg_stat_statements' -f seed.sql
createdb -T sig_before sig_after
psql -d sig_after -1 -f "../../../$new/up.sql" -c 'ANALYZE harvest_signals'
psql -d sig_before -f measure.sql -f write.sql
psql -d sig_after -f measure.sql -f write.sql
```

## 🔒 Gate

`census_scan_index_tests` plans each statement with `enable_seqscan` off and
a generic plan. The retry gate and the erasure gate plan the SQL that the
engine function sends. The erasure gate also checks that a consumed signal
is scrubbed. The cascade gate plans the query text that Postgres builds for
the cascade.
