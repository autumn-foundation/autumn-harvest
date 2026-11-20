# The concurrency gauge read the whole task queue

This page follows issue #2678. `queue::concurrency_key_stats` feeds the gauges
`harvest.concurrency.in_flight` and `harvest.concurrency.deferred`. It counts
the `PENDING` and `RUNNING` rows with a concurrency key and a cap. No index
held the `PENDING` rows with a key. So each call was a sequential scan of
`harvest_task_queue`, in parallel at the medium and large sizes. `docs/design/DESIGN-2678.md` records the
decision.

## 🎯 Workload

Two harnesses, PG 16, `pg_stat_statements`, default `shared_buffers`. Evidence
is calls, buffers and WAL, not wall-clock.

1. **Read.** `autumn-harvest/tests/integration/worker_sampler_cadence_perf.rs`
   replays 20 poll ticks of two workers at three sizes. It is the harness of
   `docs/performance-gauge-sample-interval.md`, unchanged. Its fixture has
   10,000, 100,000 and 300,000 pending rows. About 3,450 live rows hold a key
   and a cap at the large size.
2. **Write.** `autumn-harvest/tests/integration/concurrency_live_index_perf.rs`
   runs 2,000 tasks through `queue::enqueue`, `queue::claim_task` and
   `queue::complete_claimed_task`. Inside, `enqueue` and
   `complete_task_inner` send the writes. One phase gives each task a key and
   a high cap. The other gives no key. The background holds 100,000 pending
   rows with no key and 3,000 live keyed rows. The harness runs once with the
   index and once after `DROP INDEX`.

## 📈 Profile (before)

`concurrency_key_stats` buffers per call
(`docs/perf-artifacts/worker-sampler-cadence/after-statement-costs.md`, the
state on `trunk-dev` before this change):

| pending rows | buffers per call |
|--:|--:|
| 10,000 | 672 |
| 100,000 | 6,794 |
| 300,000 | 20,230 |

The cost follows the table. At 300,000 pending rows the scan read about
630,000 rows to return 56 groups.

## 🧭 Plan

Before, at the large size (`after-large-explain.txt`):

```
Finalize GroupAggregate
  ->  Gather Merge
        ->  Parallel Seq Scan on harvest_task_queue  (rows=1150 loops=3)
              Rows Removed by Filter: 208850
```

After (`live-index-large-explain.txt`):

```
GroupAggregate  (rows=56)
  Buffers: shared hit=1467 read=6
  ->  Sort
        ->  Bitmap Heap Scan on harvest_task_queue  (rows=3449)
              Heap Blocks: exact=1467
              ->  Bitmap Index Scan on idx_harvest_tq_concurrency_live  (rows=3449)
                    Buffers: shared read=6
```

## 💡 Hypothesis

A partial index on the live rows with a key and a cap makes the read follow
the keyed rows. Only keyed rows enter it, so a write of a row with no key
pays nothing.

## 🔧 Change

`20261119233226_harvest_task_queue_concurrency_live_index` builds:

```sql
CREATE INDEX idx_harvest_tq_concurrency_live
    ON harvest_task_queue (concurrency_key, task_type)
    WHERE concurrency_key IS NOT NULL
      AND concurrency_cap IS NOT NULL
      AND state IN ('PENDING', 'RUNNING');
```

No query text changes.

## 📊 Measurement

### Read

`concurrency_key_stats` buffers per call
(`docs/perf-artifacts/worker-sampler-cadence/live-index-statement-costs.md`):

| pending rows | before | after | change |
|--:|--:|--:|--:|
| 10,000 | 672 | 44 | −93.5% |
| 100,000 | 6,794 | 499 | −92.7% |
| 300,000 | 20,230 | 1,473 | −92.7% |

At the large size, 3,449 live rows hold a key and a cap. One call reads 6
index pages and 1,467 heap pages, so the cost follows the keyed rows.

The other statements of the harness read the same buffers as in
`after-statement-costs.md`, with one exception. At the medium size `oldest_pending_ages` read 875 buffers against
1,614. Its plan does not use the index. A direct `EXPLAIN` on a fresh medium
fixture reads 1,361 buffers with and without the index
(`oldest-pending-ages-*-medium.txt`, seeded with `medium-fixture.sql`). So
the difference comes from the
state of the run, not from the index.

### Write

Per call (`docs/perf-artifacts/concurrency-live-index/write-costs.md`):

| phase | statement | buffers before | buffers after | WAL records before | WAL records after | WAL bytes before | WAL bytes after |
|:--|:--|--:|--:|--:|--:|--:|--:|
| keyed | enqueue | 13.2 | 15.2 | 8.05 | 9.08 | 2,292 | 2,397 |
| keyed | claim | 1,094.2 | 1,096.3 | 10.12 | 11.18 | 748 | 835 |
| keyed | claim cap re-check | 4.0 | 4.0 | 0 | 0 | 0 | 0 |
| keyed | complete | 19.1 | 19.1 | 6.07 | 6.08 | 516 | 517 |
| plain | enqueue | 13.2 | 13.2 | 8.04 | 8.05 | 2,359 | 2,357 |
| plain | claim | 97.2 | 97.2 | 9.08 | 9.09 | 679 | 679 |
| plain | complete | 19.1 | 19.1 | 6.03 | 6.03 | 505 | 506 |

- A keyed enqueue adds one index entry: one WAL record, 105 bytes, and two
  buffers.
- A keyed claim adds one index entry for the `RUNNING` version of the row:
  one WAL record and 87 bytes.
- A completion adds none. A finished row is not live.
- A row with no key pays nothing.
- An operator cap rewrite (`set_live_activity_caps`) changes only
  `concurrency_cap`. Before, no index held that column, so the update could be
  HOT. The predicate of the index names it, so the update is not HOT now. It
  adds one entry to each index of the row. The rewrite is rare, so the harness
  does not measure it.
- One keyed task costs 3,749 WAL bytes in all, against 3,556 before. That is
  5.4% more.

## Other readers

- `concurrency_key_workflows` has the same row filter. Its plan gate shows a
  bitmap scan of the new index.
- The `concurrency_pending_keys` step of the full claim reads the due keyed
  rows. Only a fallback claim runs it. The claim turns off bitmap scans and
  sorts (`CLAIM_PLAN_SETTINGS_SQL`). With those settings, on the large
  fixture, the step read 20,154 buffers in a sequential scan. With the index
  it reads 3,444 in an index scan, 83% fewer
  (`docs/perf-artifacts/concurrency-live-index/pending-keys-*-large.txt`).
  `large-fixture.sql` in that directory seeds the fixture.
- The claim cap re-check and `release_claim_if_over_cap` count `RUNNING` rows.
  Their count has no `concurrency_cap IS NOT NULL` filter, so the planner
  cannot prove the predicate of the index. They keep
  `harvest_task_queue_concurrency_key_running`.

## Follow-ups

- A keyed claim reads about 1,090 buffers, against 97 for a plain claim. The
  index does not cause this cost, and it does not change it. The step
  `seek_running_counts` reads every `RUNNING` row through
  `harvest_task_queue_running_worker_idx`. It then joins them to the pending
  keys. So each keyed claim reads about one buffer for each running task
  (`keyed-claim-plan.txt`, from `keyed-claim.sql` on
  `keyed-claim-fixture.sql`). Issue #2744 tracks it.
- An index-only form (`INCLUDE (state, worker_id, concurrency_cap,
  queue_name)`) read 27 buffers on a vacuumed fixture, in a design-time run
  with no committed artifact. A live queue churns, so its rows are seldom
  all-visible. The design keeps the plain form.

## 🔬 Reproduce

```sh
PERF_LABEL=live-index HARVEST_TEST_DATABASE_URL=postgres://… cargo test -p autumn-harvest \
  --test integration -- --ignored --exact \
  worker_sampler_cadence_perf::zz_capture_worker_sampler_statement_costs --nocapture
HARVEST_TEST_DATABASE_URL=postgres://… cargo test -p autumn-harvest \
  --test integration -- --ignored --exact \
  concurrency_live_index_perf::zz_capture_concurrency_live_index_write_costs --nocapture
```

The `psql` artifacts run on a database with every migration applied. Seed it,
then run the statement file:

```sh
psql -d <db> -f docs/perf-artifacts/concurrency-live-index/large-fixture.sql
psql -d <db> -c 'VACUUM ANALYZE harvest_task_queue'
psql -d <db> -f docs/perf-artifacts/concurrency-live-index/pending-keys.sql
```

Drop `idx_harvest_tq_concurrency_live` first for a "before" run.
