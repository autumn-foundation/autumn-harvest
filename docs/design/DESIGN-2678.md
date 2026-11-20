# DESIGN-2678: a partial index for the concurrency gauge

## Problem

`queue::concurrency_key_stats` feeds the gauges `harvest.concurrency.in_flight`
and `harvest.concurrency.deferred`. It counts the `PENDING` and `RUNNING` rows
that hold a concurrency key and a cap. No index holds the `PENDING` rows with
a key. So the plan is a sequential scan of `harvest_task_queue`. It runs in
parallel at the medium and large sizes.

At 300,000 pending rows one call reads 20,230 buffers to return 56 groups
(`docs/perf-artifacts/worker-sampler-cadence/after-statement-costs.md`).
The cost follows the size of the table, not the number of keyed rows.

## Facts (white hat)

Measured on PG 16 with the fixture of `worker_sampler_cadence_perf` at the
large size (300,000 pending rows, 30,000 running rows). These are design-time
`psql` runs with no committed artifact. The harness run of the chosen index
reads 1,473 buffers (`docs/performance-concurrency-live-index.md`).

| candidate index | stats buffers | index pages |
|:--|--:|--:|
| none (today) | 20,230 | — |
| A: `(concurrency_key, task_type)`, key and cap not null, live state | 1,475 | 5 |
| B: `(concurrency_key, task_type, state)`, key not null, live state | 8,384 | 24 |
| C: A with `INCLUDE (state, worker_id, concurrency_cap, queue_name)` | 27 | 27 |

- The fixture sets the key and the cap at random, apart from each other. So
  it has 24,099 live rows with a key and only 3,449 with a key and a cap.
  The engine sets both together. Only an operator can clear a cap.
- `concurrency_key`, `task_type`, `state`, `worker_id` and `queue_name` are
  in other indexes already. `concurrency_cap` is in no index.
- `run_defaults::db::raise_waiting_concurrency_caps` also tests
  `concurrency_cap < $2`, so it can use A. Review found it after the decision.
- A generic plan of the stats statement can use each of A, B and C.
- `concurrency_key_workflows` has the same row filter, so it can use the
  same index.
- The claim recheck and `release_claim_if_over_cap` count `RUNNING` rows by
  key and task type. The count has no `concurrency_cap IS NOT NULL` filter,
  so A and C cannot serve them.
  `harvest_task_queue_concurrency_key_running` serves them today, at the
  same buffer count as B.
- The full claim statement has the CTE `concurrency_pending_keys`. It reads
  the due `PENDING` rows with a key and a cap. The window claim drops it, so
  it runs only on a fallback. A fallback runs when the window finds no row,
  for example when each key at the head is at its cap. The claim turns off
  bitmap scans and sorts. With that setting, at 300,000 pending rows, that CTE reads
  20,154 buffers in a sequential scan. With A it reads 3,444.

## Brainstorm (green hat)

1. A partial index on the keyed live rows (A, B or C).
2. A counter table that each enqueue, claim and completion updates.
3. One elected sampler for each shard.
4. A cache of the stats in the worker, refreshed from the claim results.
5. Index B, then drop `harvest_task_queue_concurrency_key_running`.
6. An index on `(concurrency_key, task_type, state)` that holds `worker_id`
   too, for an index-only recheck.

## Reverse brainstorm: how to make it worse

| way to fail | guard in this design |
|:--|:--|
| Lock the hot table during the build | `lock_timeout` 5 s, a guarded build, and an out-of-band `CONCURRENTLY` recipe. |
| Put every task row in the index | The predicate keeps only live rows with a key and a cap. |
| Make a frequent update lose HOT | Index only columns that other indexes hold already, apart from `concurrency_cap`. |
| Change the plan of the claim recheck | A tests the cap, so the recheck cannot pick it. |
| Return other groups than before | The SQL text does not change. A test compares the groups. |
| Accept an `INVALID` index from a failed build | The guard rejects it and names the fix. |
| Pass a plan gate on an empty table and miss the real plan | A buffer test seeds unkeyed rows and bounds the reads. |
| Claim a write cost with no number | A harness measures the WAL of each write statement. |

## Six hats

- **White.** The facts above.
- **Red.** The issue proposes A. A is small and clear.
- **Black.** Each new index adds work to each write of a row that it holds.
  A puts `concurrency_cap` in a predicate, so an operator cap rewrite loses
  HOT. Option 2 adds a write to every task. Option 5 changes the claim plan
  for a gauge, which is the wrong trade. C reads few buffers only while the
  visibility map is current, and a live queue churns.
- **Yellow.** A cuts the gauge read by 93% at the large size. It also cuts the
  claim fallback read by 83%. Only rows with a key enter it. A row with no key
  pays nothing.
- **Green.** Options 1 to 6.
- **Blue.** Build A. Prove the plan and the buffer bound with tests first.
  Measure the read and the write cost, and record both on a Ledger page.
  Keep B, C and option 5 as measured alternatives.

## Decision

Build `idx_harvest_tq_concurrency_live`:

```sql
CREATE INDEX idx_harvest_tq_concurrency_live
    ON harvest_task_queue (concurrency_key, task_type)
    WHERE concurrency_key IS NOT NULL
      AND concurrency_cap IS NOT NULL
      AND state IN ('PENDING', 'RUNNING');
```

- The key columns are the `GROUP BY` columns. A plain index scan can feed
  the group step in order. The planner picks a bitmap scan and a sort at the
  large size.
- The migration uses the guarded build of
  `docs/upgrading/online-migrations.md`, section 5. The test bundle sends all
  migrations as one batch, which rejects `CONCURRENTLY`.
- No SQL text changes. `harvest_task_queue_concurrency_key_running` stays.

## Acceptance criteria

1. At 300,000 pending rows, `concurrency_key_stats` reads buffers in
   proportion to the keyed rows, not to the table.
2. The write cost on the claim path is measured and recorded on a Ledger
   page.

## Test plan

- RED: plan gates for `CONCURRENCY_STATS_SQL` and
  `CONCURRENCY_ATTRIBUTION_SQL` name the new index. Buffer gates seed unkeyed
  rows and a few keyed rows. They bound the reads of one stats call and of
  the pending-keys CTE under the claim settings. A plan gate cannot cover
  that CTE, because other indexes can serve it on an empty table.
- A group test compares the stats with hand-counted groups. The SQL text does
  not change, so it passes before and after. It pins the results.
- GREEN: the migration. `migration_lock_safety` and `migration_hygiene` pass.
- Evidence: `worker_sampler_cadence_perf` at three sizes, and a write-cost
  harness for enqueue, claim and completion, with and without the index.
