# Queue age gauge: seek the oldest rows instead of reading the backlog (issue #2339)

`queue::oldest_pending_ages` feeds the `harvest.queue.oldest_pending_age` gauge
and the load shedder. It returns one timestamp per queue. Before this change it
read every eligible pending row of every queue to find that timestamp. This note
records the profile and the change.

## TL;DR

* **The query was 90% to 99% of all buffers in a poll tick.** The depth sampler
  runs it once per worker `poll_interval`, beside `claim_task`. At 300,000
  pending rows it read 448,085 buffers per call.
* **Its cost was linear in the backlog.** The answer is one timestamp per queue.
  The old plan fetched one heap tuple per eligible row to test the claim gates.
* **It now reads the oldest 32 rows per queue.** Buffers per call are 2,652,
  1,626 and 2,163 at 20,000, 100,000 and 300,000 pending rows. They no longer
  grow with the backlog.
* **No migration, no new index, same results.** The query walks the existing
  `idx_harvest_tq_coverage_sample`. A queue that cannot be settled from the
  window falls back to the old full read.

## 🎯 Workload

`tests/integration/queue_age_gauge_perf.rs` replays worker poll ticks. One tick
runs, for each of four workers, `queue_depths`, `oldest_pending_ages` and one
`claim_task` with its `complete_claimed_task`. Ten ticks make 480 statements.
This is the shape of a worker with a metrics recorder configured. Without a
recorder the sampler returns before it issues any SQL.

The fixture has 20 queues with a cubic skew. It holds 40% workflow tasks, 1%
paused executions, 5% future-scheduled rows and 10% rows with a
`schedule_to_close_at`. It holds 5% concurrency-capped rows and 3% rate-limited
rows. One terminal row per pending row stays in the table. Three sizes run:
20,000, 100,000 and 300,000 pending rows. Every value derives from a seeded
`random()`, so one size gives one table.

## 📈 Profile

Baseline, from `before-pg_stat_statements.txt` (Postgres 16):

| Pending rows | All buffers | Age buffers | Share of buffers | Share of calls | Buffers per call |
| --: | --: | --: | --: | --: | --: |
| 20,000 | 1,063,486 | 951,450 | 89.5% | 8.3% (40 of 480) | 23,786 |
| 100,000 | 4,736,092 | 4,578,458 | 96.7% | 8.3% | 114,461 |
| 300,000 | 18,175,535 | 17,923,384 | 98.6% | 8.3% | 448,085 |

The depth query is the second cost at 32,623 to 126,272 buffers. It is an
index-only scan that grows with the backlog too. This change does not touch it.
The claim is third. [`performance-deep-backlog.md`](performance-deep-backlog.md)
covers the claim.

## 🧭 Plan

Before, 100,000 pending rows (`before-explain.txt`). The root node reads 114,478
buffers:

```
GroupAggregate
  ->  Nested Loop Anti Join                      rows=91615
        ->  Merge Anti Join                      rows=91615
              ->  Index Scan using idx_harvest_tq_claim_seek
                    Buffers: shared hit=113330 read=1147
                    SubPlan (concurrency count, 236 loops): 18502 buffers
```

After, same fixture (`after-explain.txt`). The root node reads 885 buffers:

```
Nested Loop Left Join
  CTE seek
    ->  Index Scan using idx_harvest_tq_coverage_sample    (32 rows x 20 queues)
  ->  Hash Left Join  (seen, refined)
  ->  Aggregate  (fallback)
        One-Time Filter: (count(*) >= 32) AND (oldest IS NULL OR oldest > max(scheduled_at))
        ->  Bitmap Index Scan on idx_harvest_tq_poll   (never executed)
```

The changed node is the scan. The unbounded index scan on `idx_harvest_tq_claim_seek`
becomes a bounded ordered walk. The fallback never runs on this fixture.

## 💡 Hypothesis

The old query computes `MIN(GREATEST(scheduled_at, created_at))` after the claim
gates. The gates read columns that no index carries: the execution state, the
concurrency count and the rate-limit bucket. So each eligible row costs one
heap fetch, and the cost grows with the backlog. The answer needs only the
oldest claimable row.

`idx_harvest_tq_coverage_sample` holds `(queue_name, scheduled_at, id)` for
pending rows. A walk of it per queue returns rows oldest first with a plain
`LIMIT`. The planner picks that walk without help, because the seek has no gate.
A first prototype put the gates inside the `LIMIT` subquery. The planner then
estimated one qualifying row, rejected the ordered walk and sorted the queue.

## 🔧 Change

The query has four steps. No migration and no index changes.

1. **Seek.** Each queue reads its oldest 32 eligible rows by `scheduled_at`. No
   gate runs here.
2. **Refine.** The claim gates run over those rows only. The minimum
   `GREATEST(scheduled_at, created_at)` of the passing rows is the candidate.
3. **Check.** A row outside the window has a `scheduled_at` no earlier than the
   last window row. Its value is at least that. So a candidate not later than the
   last window row is exact. A window with fewer than 32 rows holds the whole
   queue, so it is exact too.
4. **Fallback.** A queue that fails the check reads the whole queue under the
   same gates, as before. This happens when the 32 oldest rows are all gated, or
   when `created_at` is later than `scheduled_at` by more than the window spans.

One macro holds the gate text. The refine step and the fallback both embed it,
so they cannot drift. The refine step aliases the seek rows as
`harvest_task_queue`. The existing shape tests read the gates through that name,
and they pass unchanged. The window size is `OLDEST_PENDING_SEEK_WINDOW`. A unit
test pins it to the SQL literal.

### Worst case

A queue whose oldest 32 rows are all gated reads the whole queue, as the old
query did, plus the seek. With the largest queue paused on the 100,000-row
fixture, the new query read 10,598 buffers. The old query read 118,387. The
fallback adds the seek, at most 32 rows per queue, to the old read of that queue.

## 📊 Measurement

Same harness, same fixture, same session. Tool: `pg_stat_statements` for the
workload, and `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` for the plan.

| Pending rows | Age buffers before | Age buffers after | Change | All buffers before | All buffers after | Change |
| --: | --: | --: | --: | --: | --: | --: |
| 20,000 | 951,450 | 106,095 | -88.8% | 1,063,486 | 218,148 | -79.5% |
| 100,000 | 4,578,458 | 65,057 | -98.6% | 4,736,092 | 222,766 | -95.3% |
| 300,000 | 17,923,384 | 86,532 | -99.5% | 18,175,535 | 338,760 | -98.1% |

Per call, from the plans (`plans-summary.txt`, made by `summarize-plans.py`):

| Pending rows | Buffers before | Buffers after | Heap rows read before | Heap rows read after |
| --: | --: | --: | --: | --: |
| 20,000 | 19,874 | 2,649 | 18,880 | 640 |
| 100,000 | 114,478 | 885 | 103,130 | 720 |
| 300,000 | 448,095 | 2,158 | 374,244 | 1,309 |

* Statements: 480 before and after. This is not an N+1 change.
* Spills: none. No plan, before or after, has a temp read, a temp write or an
  external sort.
* WAL bytes: 44,200 / 46,811 / 710,859 before. 44,198 / 46,638 / 710,897 after.
  The change writes nothing, and the differences are claim noise.
* The depth and claim statements read the same buffers within noise.

The remaining age cost is the correlated concurrency count. It runs once per
seeked row that carries a concurrency key and a cap. A follow-up could batch it.
This change does not.

The impact floor needs 20% fewer buffers on a statement that is at least 5% of
the workload. The statement is 89.5% to 98.6% of buffers, and the change removes
88.8% to 99.5% of them.

## ✅ Equivalence

`age_gauge_matches_the_reference_query` runs the public function and the
verbatim old query inside one transaction, so `NOW()` is the same for both. It
asserts that the sorted `(queue, age)` lists are equal. The small cases ran on
the old code and on the new code. The large cases ran on the new code against
the reference query. The test covers:

* An empty table and an empty queue.
* A future-scheduled oldest row.
* `created_at` later than `scheduled_at`, and a NULL `created_at`.
* A paused queue, a paused activity and a paused execution.
* An expired `schedule_to_close_at`.
* A full concurrency key.
* A rate-limited row with and without a bucket.
* Circuit-breaker activities in `$2`.
* A queue with exactly 32 rows, with 33 rows, and with 100 rows of one timestamp.
* A queue whose 40 oldest rows are gated, so the fallback decides the answer.
* A queue whose oldest row has a `created_at` far past the window.
* A queue with 100 gated rows, which has no age.

Both queries also returned the same 20 rows on a 100,000-row benchmark-shaped
fixture. `EXCEPT` in both directions gave zero rows.
The existing shape tests pass unchanged: `claim_gate_mirrors_carry_the_*` and
`claim_mirror_queries_contain_the_queue_pause_anti_join`. So do the 76 tests
selected by `queue_pause`, `load_shed` and `claim_mirror`. The four
`oldest_pending_age_*` tests in `metrics_integration.rs` need a Docker daemon.
They did not run in this environment.

## 💸 Write cost

None. The change adds no index and writes no rows.

## 🔬 Reproduce

```sh
export HARVEST_TEST_DATABASE_URL=postgres://postgres@localhost/postgres
# The server needs shared_preload_libraries=pg_stat_statements and a superuser.
PERF_LABEL=after cargo test -p autumn-harvest --features db --test integration \
  queue_age_gauge_perf -- --include-ignored --nocapture --test-threads=1
python3 -I docs/perf-artifacts/queue-age-gauge/summarize-plans.py \
  docs/perf-artifacts/queue-age-gauge/before-explain.txt \
  docs/perf-artifacts/queue-age-gauge/after-explain.txt
```

Check out the commit before the engine change and set `PERF_LABEL=before` to
rebuild the baseline. The seed takes several minutes at 300,000 rows.
