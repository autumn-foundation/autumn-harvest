# The unsharded event-batch scanner sorted every due row to fire one

🎯 **Workload.** The worker's timeout loop calls
`event_batch::fire_due_event_batches_on_conn_shard`
(`autumn-harvest/src/event_batch.rs`) on each tick. Without a sharded pool,
it calls `fire_due_on_conn` once. That function fires one due batch per
transaction, so that one bad row cannot hold up a healthy one. Each
transaction reads the next due row:

```sql
SELECT id, workflow_name, batch_key, workflow_id, queue_name,
       buffered_payloads, start_options, fire_at, max_size, shard_id
FROM harvest_event_batches
WHERE fire_at <= $1
ORDER BY fire_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED
```

`$2` is 1. The table has one scanner index, `(shard_id, fire_at)`. The
sharded read adds `AND shard_id = $3`, so that index serves it. The
unsharded read has no `shard_id` predicate. The index cannot then give rows
in `fire_at` order, and Postgres 16 has no skip scan. So each read took every
due row through the index, fetched each from the heap, sorted them, and
returned one. A tick that fires `D` batches reads about `D * D / 2` heap rows.

A batch row lives from its first admission until its `fire_at`, which is
`first_seen + max_wait`. The table holds one row for each open batch key. A
deployment that batches per customer or per entity holds many open rows. The
due rows pile up when a tick is late: after a worker restart, a slow shard,
or a burst of keys that open together.

The harness is
`autumn-harvest/tests/integration/event_batch_fire_scan_perf.rs`. It seeds
open batches at 1,000, 10,000 and 100,000 rows:

- 20 batched workflow types with a cubic skew.
- 2% of the rows are due. Their `fire_at` falls in the last two minutes. The
  rest fall due in 10 to 15 minutes. Each `fire_at` is distinct, so the fire
  order is total.
- 70% of the rows buffer 1 to 5 events, 25% buffer 6 to 50, and 5% buffer 51
  to 98. An event is about 230 bytes, so the large rows go to TOAST: 7,729
  TOAST pages beside 17,577 heap pages at the largest size.
- 30% of the rows carry a memo.
- Half of the rows then take one more event, as an append does. Autovacuum
  is off for the table. So a third of the heap tuples are dead: 50,000 dead
  beside 100,000 live at the largest size.

At each size the workload is one scanner tick through
`fire_due_event_batches`, then 200 `admit_batched_start` calls. Half of them
append to an open batch, and half open a new one. The admissions measure
the write cost of the index. Each size runs twice, in its own copy of one
migrated template database: once without the index, once with it.
[`fixture` rows](perf-artifacts/event-batch-fire-scan/before.pg_stat_statements.txt)
open each snapshot.

📈 **Profile.** `pg_stat_statements`, reset for the database before the
workload, tracking top-level statements. Without the index:

| open batches | due rows fired | due-read buffers | share of workload buffers | share of calls |
|--:|--:|--:|--:|--:|
| 1,000 | 25 | 732 | 11.4% | 2.5% |
| 10,000 | 213 | 47,095 | 75.0% | 5.1% |
| 100,000 | 1,982 | 3,228,273 | 95.4% | 5.8% |

At 10,000 and 100,000 rows the due read is the first statement by buffers. The
next three at 100,000 rows are the start-path inserts into
`harvest_task_queue`, `harvest_workflow_executions` and `harvest_events`.
Each one is 1.0% to 1.4% of the buffers. Those are inherent: a fired batch
starts a run. By calls, the due read ties with each per-fire statement. It
runs once for each fire plus one empty read that ends the tick. So the cost
is in the plan, not in the statement count. No statement wrote a temp block.

🧭 **Plan.** The first due read of the 100,000-row tick, without the index
([`before.explain.txt`](perf-artifacts/event-batch-fire-scan/before.explain.txt)):

```text
Limit (actual rows=1 loops=1)
  Buffers: shared hit=2583 read=78
  ->  LockRows (actual rows=1 loops=1)
        ->  Sort (actual rows=1 loops=1)
              Sort Key: harvest_event_batches.fire_at
              Sort Method: quicksort  Memory: 1825kB
              ->  Index Scan using idx_harvest_event_batches_shard_fire_at
                    (actual rows=1982 loops=1)
                    Index Cond: (fire_at <= '...'::timestamp with time zone)
```

The index scan walks the whole `(shard_id, fire_at)` index to apply a
condition on its second column. It returns all 1,982 due rows, and the sort
keeps one. Each later read in the tick does the same over the rows left.
With the index
([`after.explain.txt`](perf-artifacts/event-batch-fire-scan/after.explain.txt)):

```text
Limit (actual rows=1 loops=1)
  Buffers: shared hit=5
  ->  LockRows (actual rows=1 loops=1)
        ->  Index Scan using idx_harvest_event_batches_fire_at
              (actual rows=1 loops=1)
```

The Sort node is gone, and the scan reads one row. The plan changes the same
way at all three sizes.

💡 **Hypothesis.** The read orders on `fire_at`, and no index leads with
`fire_at`. An index on `(fire_at)` gives the rows in order, so `LIMIT 1`
stops at the first due row that `SKIP LOCKED` does not skip. The cost of the
read then does not depend on the number of due rows.

🔧 **Change.** Migration
`20261127173207_harvest_event_batches_fire_at_index`:

```sql
CREATE INDEX idx_harvest_event_batches_fire_at ON harvest_event_batches (fire_at);
```

The query text does not change. The build is guarded, as in
`20261121172305_harvest_mutex_waiters_waiter_index`. An operator can build
the index `CONCURRENTLY` first. The migration then accepts it if its
definition matches and it is valid, and it raises an error otherwise.
`harvest_event_batches` is not a hot table for `migration_lock_safety`.

[`migration.txt`](perf-artifacts/event-batch-fire-scan/migration.txt)
records the apply and the rollback at 100,000 rows. The plain build holds
`SHARE` on `harvest_event_batches` for 74 ms. That blocks admissions and
fires, and reads go on. The rollback's `DROP INDEX` holds `ACCESS EXCLUSIVE`
for 3 ms. A second apply accepts the index that is already there.

📊 **Measurement.** Same harness, same fixture, same session.
`pg_stat_statements` for buffers, calls, WAL bytes and temp blocks.
`EXPLAIN (ANALYZE, BUFFERS)` for the rows read.

| open batches | due-read buffers/call | due-read buffers | workload buffers | change |
|--:|--:|--:|--:|--:|
| 1,000 | 28.2 → 5.8 | 732 → 150 | 6,418 → 6,239 | −2.8% |
| 10,000 | 220.1 → 5.9 | 47,095 → 1,263 | 62,810 → 17,540 | −72.1% |
| 100,000 | 1,628.0 → 11.4 | 3,228,273 → 22,678 | 3,384,147 → 178,487 | −94.7% |

| counter, 100,000 open batches | without | with |
|---|--:|--:|
| rows read by the first due read (`EXPLAIN`) | 1,982 | 1 |
| Sort node in the due read | yes | no |
| statements in the workload | 34,298 | 34,298 |
| temp blocks written | 0 | 0 |
| `idx_scan` on `idx_harvest_event_batches_fire_at` | — | 1,985 |

The statement count does not change. The tick still fires one row per
transaction. The read cost per fire grows with the due backlog without the
index, and stays flat with it. Three runs of the baseline gave 3,382,980,
3,382,982 and 3,384,147 workload buffers. The third run is the published one.
It is the first with appends that match their batch type (see below), so its
admissions touch a few more pages.

With the index, the read still costs 11.4 buffers per fire at 100,000 rows,
against 5.8 at 1,000. Each fire deletes its row. The next read steps over the
index entries of the rows that this tick already deleted, until a scan marks
them dead.

✅ **Equivalence.** Each arm records the started runs in fire order, by
`created_at`, with type, workflow id and an `md5` of the input. It also
records each batch row left after the admissions, by type and key, with its size and
`md5` digests of the payload and the start options. Each fire commits its
own transaction, so `created_at` orders the runs. The capture asserts that
both arms match at each size.
[`result-rows.txt`](perf-artifacts/event-batch-fire-scan/result-rows.txt)
has the counts and a digest for each size.

`event_batch_fire_index_does_not_change_what_fires` always runs with a
database. It fires an edge fixture in two copies of one database, without
and with the index:

- due rows on four shard ids, since the unsharded read fires every shard id;
- three due rows that tie on `fire_at`. The read orders on `fire_at` alone,
  so their relative order is not part of its contract. The test checks that
  all three fire, and that the other rows keep their order;
- a row due one millisecond before the seed, a row due one minute after it
  and a row due in an hour. The last two stay;
- a row one event below its `max_size`.

The column is `NOT NULL`, so a NULL `fire_at` cannot occur. The read takes no
bi-temporal predicate. The transaction boundaries and the `SKIP LOCKED`
clause do not change.

💸 **Write cost.** The index is 4.4 MB at 100,000 rows in the capture. It was
built before the seed, so inserts and updates shaped it. A fresh build on a
100,000-row table with no dead tuples is 0.7 MB (`migration.txt`). The admission upsert of the workload:

| open batches | upsert WAL bytes, 200 calls | upsert buffers, 200 calls |
|--:|--:|--:|
| 1,000 | 392,651 → 246,053 | 2,643 → 3,046 |
| 10,000 | 220,848 → 237,838 | 2,736 → 3,298 |
| 100,000 | 223,993 → 237,087 | 3,321 → 4,066 |

Each set of 200 holds 100 appends to open batches and 100 new batches. The
capture asserts the row count that this mix leaves. At 10,000 and 100,000
rows, that is 65 to 85 more WAL bytes and 3 to 4 more buffers for each
admission. The 1,000-row WAL pair is not usable. Its `without` arm ran first
after a server restart, so its writes carry the full-page images of the
first change after a checkpoint. WAL bytes depend on the checkpoint timing,
and buffers do not. The buffer delta at 1,000 rows is 2 for each admission. An
append changes `buffered_payloads`, and `fire_at` is already in an index. So
the new index does not stop a heap-only update. The fire path deletes the
row, and a delete writes no index entry.

🔬 **Reproduce.** The server needs `pg_stat_statements` in
`shared_preload_libraries`, and the URL must be a superuser's.

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo xtask perf event-batch-fire-scan
```

This writes the artifacts in `docs/perf-artifacts/event-batch-fire-scan/`.
The equivalence check runs in CI. Without `HARVEST_TEST_DATABASE_URL` it
starts its own Postgres container:

```sh
cargo test -p autumn-harvest --features db,testing --test integration -- \
  event_batch_fire_scan_perf::event_batch_fire_index_does_not_change_what_fires
```
