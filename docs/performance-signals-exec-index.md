# Signal reads by execution use an index (issue #2367)

The Ledger census found statements that read every `harvest_signals` row of
one execution, consumed rows included. The issue adds the `ON DELETE CASCADE`,
which the census cannot see. No index served them, so each one read the whole
table. Migration
`20261030182800_harvest_signals_exec_index` adds `idx_harvest_signals_exec` on
`harvest_signals (workflow_exec_id)`.

## 🎯 Workload

Each statement reads the signals of one execution:

- `signal::forward_signals_to_retry_attempt` moves the mailbox of a failed
  attempt to its workflow-level retry (issue #843).
- `erase::db::scrub_execution_node` tombstones the signal payloads of a PII
  erasure (issue #495).
- `shard_rebalance::db::stage_copy` copies the signals of a moved execution.
  It sends `STAGED_SIGNALS_SQL` through `db::read_json`. The staged delete
  removes the signals again when a copy is discarded.
- Retention's `delete_candidate_execution` deletes an execution. Postgres
  then runs the `ON DELETE CASCADE` of `harvest_signals` as a nested query.
- `backup_verify::probes::effect_verdict` checks for a signal name along a
  retry chain, when the signal has no idempotency key.

`idx_harvest_signals_pending` skips consumed rows. `uq_harvest_signals_idem`
skips rows with no idempotency key. So neither index holds every row of one
execution, and neither can serve these statements.

The fixture has 4,000 completed executions with 25 signals each, 100,000
rows in all. 24 of each 25 signals are consumed. The table is 4,000 pages.

## 📈 Profile

PostgreSQL 16, `pg_stat_statements.track = all`. One pass loads the catalog
caches. The second pass is measured. A count is `shared_blks_hit +
shared_blks_read` for one call. A top-level count includes its nested
queries.

| Statement | Before | After |
|---|---:|---:|
| Retry move (`UPDATE ... SET workflow_exec_id`) | 4,480 | 529 |
| Erasure scrub (`UPDATE ... SET payload`) | 4,257 | 311 |
| Rebalance copy (`STAGED_SIGNALS_SQL`) | 4,029 | 28 |
| Rebalance staged delete | 4,054 | 53 |
| Execution delete, top level | 4,150 | 149 |
| — its nested `harvest_signals` cascade | 4,054 | 53 |
| Backup verify, unkeyed signal check, no match | 4,029 | 32 |

Before, each statement read all 4,000 pages of the table. After, each one
reads the 25 rows of its execution. The cost no longer grows with the table.

The retry move includes 125 buffers of nested foreign key checks, one for
each moved row. The backup check stops at its first match. A name that no
signal has is the case that reads the most rows.

## ⚠️ Generic plans of the pending reads

Before this change, each pending read bound the flag as `consumed = $2`. A
generic plan cannot match the predicate `WHERE NOT consumed` with a bound
value. Before the index, the only generic plan was a `Seq Scan`, so Postgres
always kept a custom plan.

With the index, a generic plan can read `idx_harvest_signals_exec` and filter
`consumed`. A cached statement can switch to that plan after five calls.
`signal::has_pending_signals` did so in a test. It read 27 buffers instead of
4, and its cost grew with every signal of the run.

So each pending read now sends `NOT consumed` as a literal, through
`diesel::dsl::not`. A generic plan then uses the partial index too.

## 💸 Write cost

The index adds one entry for each signal insert. A consume update changes
`consumed`, which is in the predicate of `idx_harvest_signals_pending`. So a
consume update was never HOT, and the new index adds one entry for it too.
The erasure scrub changes `payload` only, so it stays HOT-eligible.

`write.sql` runs a checkpoint first. So the first change to each page writes a
full-page image, as in production.

| 1,000 rows | Buffers before | Buffers after | Full-page images before | after | WAL bytes before | after |
|---|---:|---:|---:|---:|---:|---:|
| Signal insert | 11,987 | 13,997 | 477 | 518 | 3,010,957 | 3,407,174 |
| Consume update | 15,208 | 17,442 | 0 | 12 | 264,864 | 364,380 |

That is about 2 more buffers for each signal write. An insert after a
checkpoint writes about 400 more WAL bytes, most of it in full-page images of
index leaf pages. Each leaf page writes one image per checkpoint cycle. A
consume writes about 100 more WAL bytes. The consume statement here scans by
`signal_name`, so its buffer count is not the cost of the engine's consume.
Only the difference applies.

The index size depends on how many signals one execution has:

| Shape | Index pages for 100,000 rows |
|---|---:|
| 25 per execution, sorted build | 100 |
| 25 per execution, random inserts | about 140 |
| 1 per execution, random inserts | about 530 |

At one signal per execution, the index is the size of the primary key. Each
vacuum of `harvest_signals` also scans the new index.

## 🔁 Reproduce

The fixture and the statements are in
[`perf-artifacts/signals-exec-index/`](perf-artifacts/signals-exec-index/).
The results are in `before.txt`, `after.txt` and `size.txt`. A count varies
by about 1% between runs, and an index size by about 5%. Load
`pg_stat_statements` in `shared_preload_libraries` first.

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

`size.sql` needs an empty database with every migration applied.

## 🔒 Gate

`census_scan_index_tests` plans each engine statement with `enable_seqscan`
off and a generic plan:

- The retry, erasure and pending-signal gates plan the SQL that the engine
  function sends.
- The erasure gate also checks that a consumed signal is scrubbed.
- The cascade gate deletes an execution and counts the scans of the index.
- `no_query_binds_the_consumed_flag` fails on a `consumed.eq(false)` filter
  in any crate source.

The backup check has no gate. It runs only in an offline restore check.
