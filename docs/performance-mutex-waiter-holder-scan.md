# The mutex terminal sweep scanned every waiter row

🎯 **Workload.** Each workflow terminal transition runs
`mutex::sweep_terminal_holder_and_wake` (`autumn-harvest/src/mutex.rs`).
The completion-trigger evaluator, continue-as-new sealing and reset call it.
[The `table_present` page](performance-mutex-terminal-sweep-table-present.md)
lists the exact paths. The first statement of the sweep is
`terminal_keys_for_holder_stmt`. It reads the keys that the run holds and
the keys that it waits on:

```sql
SELECT 0 AS part, lock_key FROM harvest_mutex_locks WHERE holder_exec_id = $1
UNION ALL
SELECT DISTINCT 1 AS part, lock_key FROM harvest_mutex_waiters WHERE waiter_exec_id = $1
ORDER BY part, lock_key
```

The holder half has `idx_harvest_mutex_locks_holder`. The waiter half filters
on `waiter_exec_id` alone. The three indexes of `harvest_mutex_waiters` are
`(id)`, `(lock_key, waiter_exec_id)` and `(lock_key, id)`. None of them leads
with `waiter_exec_id`, and Postgres 16 has no skip scan. So the waiter half
scanned the whole waiter heap at the end of every run. Most runs never take
a mutex, so most of those scans returned no row.

The waiter table holds one row for each run that waits on a key. A grant
keeps the waiter row of its holder, and only the release deletes it. So the
table also holds one row for each current holder. Its size is the number of
runs that are inside or queued on a `ctx.mutex` region, on the whole shard.
A deployment that serializes work on a few hot keys can queue thousands of
runs.

The harness is
`autumn-harvest/tests/integration/mutex_waiter_holder_scan_perf.rs`. It seeds
[`fixture.sql`](perf-artifacts/mutex-waiter-holder-scan/fixture.sql) at
1,000, 10,000 and 100,000 waiter rows. The rows stand for other runs on the
shard:

- 80% of the rows queue on 20 hot keys with a squared skew. The deepest key
  holds 17,650 rows at the largest size.
- 20% are one-row keys, each with its lock row.
- A churn pass first inserts and deletes as many rows again, so about half of
  the heap tuples are dead. Autovacuum is off for the table, so the counters
  stay deterministic.

Then 200 three-step runs start through `start_or_load_workflow_execution`.
Every second run takes one of four contended keys around its second step. One
worker drives every run to its end. The harness reads `pg_stat_statements`
for the database. Each size runs twice in the same session: without the new
index (the `without` arm), and with it (the `with` arm).

📈 **Profile.** Workload totals and the key read, `without` arm
([`capture-capture.txt`](perf-artifacts/mutex-waiter-holder-scan/capture-capture.txt);
[`before-capture.txt`](perf-artifacts/mutex-waiter-holder-scan/before-capture.txt)
is the baseline commit, with the same numbers):

| fixture waiters | workload buffers | key read buffers | share of buffers | key read calls | share of calls |
|--:|--:|--:|--:|--:|--:|
| 1,000 | 328,144 | 4,439 | 1.4% | 200 | 0.4% |
| 10,000 | 361,300 | 37,800 | 10.5% | 200 | 0.3% |
| 100,000 | 717,227 | 374,409 | 52.2% | 200 | 0.4% |

At 10,000 waiters the key read is the third statement by buffers, after the
claim CTE (33%) and the event insert (11%). At 100,000 it is the first. It is
never in the top 10 by calls: one call per run. No statement wrote a temp
block.

At 1,000 waiters the key read is 1.4% of buffers, under the 5% bar. The
change is justified by the larger sizes, not by that one.

🧭 **Plan.** The generic plan of the key read, as the prepared statement cache
runs it, for a run that never took a mutex
([`capture-explain.txt`](perf-artifacts/mutex-waiter-holder-scan/capture-explain.txt)).
`without`, 100,000 waiters:

```
->  Seq Scan on public.harvest_mutex_waiters (actual rows=0 loops=1)
      Filter: (harvest_mutex_waiters.waiter_exec_id = $1)
      Rows Removed by Filter: 100000
      Buffers: shared hit=1870
```

`with`, 100,000 waiters:

```
->  Index Scan using idx_harvest_mutex_waiters_waiter on public.harvest_mutex_waiters (actual rows=0 loops=1)
      Index Cond: (harvest_mutex_waiters.waiter_exec_id = $1)
      Buffers: shared hit=3
```

The capture explains three cases at each size: a run with no mutex row, a
waiter on the deepest hot key, and the holder of a one-row key. Each case
changes the same way. The seq scan reads every heap page at every size. The
index scan reads 2 to 4 pages at every size.

💡 **Hypothesis.** The predicate is `waiter_exec_id = $1`. Each index of the
table leads with another column, so the waiter half is a seq scan. Its cost
grows with the heap pages of the waiter table, dead tuples included. An index
that leads with `waiter_exec_id` makes it one probe.

🔧 **Change.** One migration,
`20261121172305_harvest_mutex_waiters_waiter_index`:

```sql
CREATE INDEX idx_harvest_mutex_waiters_waiter ON harvest_mutex_waiters (waiter_exec_id);
```

The query text does not change. The migration is a guarded build. It accepts
a valid index that an operator prebuilt with `CREATE INDEX CONCURRENTLY`, and
it refuses an `INVALID` index or another definition.

**Lock.** The plain build takes `SHARE` on `harvest_mutex_waiters`. Reads
continue. Waiter inserts and deletes wait for the build. At 100,000 waiter
rows the build took 51 ms
([`migration.txt`](perf-artifacts/mutex-waiter-holder-scan/migration.txt)).
`lock_timeout` is 5 s. The table is not on the hot-table list of
[the lock-safe migration guide](upgrading/online-migrations.md).
The rollback drops the index under `ACCESS EXCLUSIVE` for about 1 ms.

📊 **Measurement.** Same harness, same fixture, same session.
`pg_stat_statements` for buffers, calls and WAL. `EXPLAIN (ANALYZE, BUFFERS)`
for rows read.

| fixture waiters | key read buffers per call | rows read at the waiter scan | workload buffers |
|--:|--:|--:|--:|
| 1,000 | 22.2 → 6.3 (−72%) | 1,000 → 0 | 328,144 → 324,206 (−1.2%) |
| 10,000 | 189.0 → 5.0 (−97%) | 10,000 → 0 | 361,300 → 322,185 (−10.8%) |
| 100,000 | 1,872.0 → 6.0 (−99.7%) | 100,000 → 0 | 717,227 → 317,779 (−55.7%) |

The rows column is for a run with no waiter row. The seq scan reads every
live tuple and removes each one by the filter.

- **Statement count:** unchanged, 200 key reads for 200 runs.
- **Temp blocks:** 0 in both arms.
- **Index use:** `idx_scan` on the new index is 400 to 402 in the `with` arm.
  The workload uses it.
- **Plan shape:** the `without` cost grows with the table (19, 187 and 1,870
  heap pages). The `with` cost stays at 2 to 4 index pages. That holds at
  three sizes.

The workload total at 10,000 waiters moves by more than the key read alone,
39,115 buffers against 36,800. The other 2,315 are spread over the task
queue and event statements. Each of them differs between arms by under 8%,
in both directions. Gate on the key read row.

✅ **Equivalence.** The query text is the same. Only the access path changes,
so the result set of each call is the same set of rows. The harness checks
the effect end to end:

- Every run completes in both arms, and the sweep leaves no lock or waiter
  row of any run.
- After the workload, the harness runs `sweep_terminal_holder_and_wake` for a
  fixed sample of 100 fixture executions. The sample holds 50 holders of
  one-row keys and 50 waiters spread through the hot queues, heads included.
  Then it digests every fixture lock row and waiter row, in FIFO order.
- The digest of each size is the same in both arms. The capture ends with
  `equivalence confirmed:` and the three digests.

Edge cases: a run with no mutex row (the empty result), a run with a held key
and its own waiter row, a waiter deep in a 17,650-row queue, and a waiter at
the head of a queue, whose sweep wakes the next waiter. `waiter_exec_id` is
`NOT NULL`, so a NULL predicate cannot arise. The existing mutex suites pass
unchanged: `mutex_tests` (17), `mutex_terminal_sweep_db_tests` (2),
`decision_boundary_db_tests` (15) and the 25 unit tests of `mutex.rs`.

💸 **Write cost.** Each waiter insert writes one more index entry. A release
deletes the waiter row and writes no index WAL. Vacuum removes the dead entry
later.

| fixture waiters | waiter insert WAL bytes, 100 inserts | buffers, 100 inserts | index size |
|--:|--:|--:|--:|
| 1,000 | 38,597 → 46,000 | 1,305 → 1,507 | 90,112 B |
| 10,000 | 38,597 → 46,004 | 1,298 → 1,486 | 827,392 B |
| 100,000 | 38,597 → 45,988 | 1,247 → 1,320 | 8,634,368 B |

That is about 74 WAL bytes and 1 to 2 buffers for each `ctx.mutex` acquire
that queues a waiter row. A re-park hits `ON CONFLICT DO NOTHING` and adds no
row. The index costs about 86 bytes for each waiter row. Total workload WAL is
6.42M to 6.49M bytes in both arms. The difference is under the run-to-run
spread of the other statements.

A run that never takes a mutex pays no write cost. Each run pays the read
saving at its end.

**Not done.** The unique key `(lock_key, waiter_exec_id)` could become
`(waiter_exec_id, lock_key)` and serve this read with no new index.
`ON CONFLICT (lock_key, waiter_exec_id)` infers its arbiter by column set, so
it would still match. The `(lock_key, id)` index serves each `lock_key` read.
That swap drops a constraint, which takes `ACCESS EXCLUSIVE`. It needs a
human decision, so this page does not make it.

🔬 **Reproduce.** The server needs `pg_stat_statements` in
`shared_preload_libraries`.

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo xtask perf mutex-waiter-holder-scan
```

This writes `capture-capture.txt` and `capture-explain.txt`. Set `PERF_LABEL`
to change the prefix. The plain-database guard runs in CI:

```sh
cargo test -p autumn-harvest --features db,testing --test integration -- \
  mutex_waiter_holder_scan_perf::terminal_key_read_probes_the_waiter_index
```

To read the plans in `psql` on a migrated database:

```sh
psql -v waiters=100000 -f docs/perf-artifacts/mutex-waiter-holder-scan/fixture.sql
psql -f docs/perf-artifacts/mutex-waiter-holder-scan/measure.sql
```
