# Queue depth gauge: count up to a cap (issue #2356)

`queue::queue_depths` fed the `harvest.queue.depth` gauge. It counts every
eligible pending row of each queue, so its cost grows with the backlog. After
issue #2339 it was the largest poll-tick cost at 300,000 pending rows. This
note records the profile and the change.
[`design/DESIGN-2356.md`](design/DESIGN-2356.md) records the options and the
decision.

## TL;DR

* **The depth count was linear in the backlog.** It read 816, 1,538 and 3,157
  buffers per call at 20,000, 100,000 and 300,000 pending rows.
* **It now counts each queue up to 10,000 rows.** Statistics pick one of two
  ways to count. The walk reads each queue in index order and stops at the
  cap. The pass reads the table once and caps each count. A run costs no more
  than the smaller of the two.
* **Vacuumed table:** depth buffers fall by 20.1% at 100,000 and 36.7% at
  300,000 pending rows. One deep queue of 1,000,000 rows reads 114 buffers
  instead of 22,744.
* **Empty visibility map:** the count makes one pass and costs what the exact
  count costs (14,139 buffers against 14,152). A walk would read 81,479 to
  155,878.
* **The small fixture pays 7.6% more.** No queue there reaches the cap. Each
  queue costs one index descent and one catalog read.
* **Below 10,000 the gauge is exact.** A value of 10,000 means 10,000 or
  more.
* **No migration, no new index, no new write.**

## 🎯 Workload

The harness is the poll-tick harness of issue #2339,
`tests/integration/queue_age_gauge_perf.rs`. One tick runs, for each of four
workers, the depth count, `oldest_pending_ages` and one `claim_task` with its
`complete_claimed_task`. Ten ticks make 480 statements before the change and
600 after it. The fixture has 20 queues with a cubic skew, 5% future rows and
one terminal row per pending row. Three sizes run: 20,000, 100,000 and
300,000 pending rows. The harness runs `VACUUM ANALYZE` after the seed.

Two scripts add points that the harness does not reach:

* `deep-queue.sql`: one queue holds 1,000,000 eligible rows and 1,000,000
  terminal rows, after a vacuum.
* `empty-visibility-map.sql`: 300,000 eligible rows in 20 queues, analyzed
  but not vacuumed, in three heap orders. It runs on a migrated database.

## 📈 Profile

Baseline, from `before-pg_stat_statements.txt` (Postgres 16, same session):

| Pending rows | All buffers | Depth buffers | Share of buffers | Share of calls | Buffers per call |
| --: | --: | --: | --: | --: | --: |
| 20,000 | 218,108 | 32,623 | 15.0% | 8.3% (40 of 480) | 816 |
| 100,000 | 193,220 | 61,527 | 31.8% | 8.3% | 1,538 |
| 300,000 | 338,752 | 126,263 | 37.3% | 8.3% | 3,157 |

The baseline repeats the issue figures within 8 buffers. The plan is an
index-only scan of `idx_harvest_tq_poll` that reads every eligible entry.

## 💡 Hypothesis

An exact count must visit each eligible row. The gauge does not need an exact
value for a deep queue. The two starter alerts compare depth with 100 and
1,000. A count that stops at 10,000 serves them, and its cost per queue has a
fixed bound.

The bound holds only while the visibility map covers the walked rows. Queues
share heap pages, so a walk of one queue can visit a new heap page for each
row on an uncovered page. One pass visits each page once.

## 🔧 Change

`queue::bounded_queue_depths` holds two ways to count in one statement:

1. **Walk.** One `LATERAL` subquery per distinct queue name walks
   `idx_harvest_tq_coverage_sample` in key order with a `LIMIT` at the cap.
2. **Pass.** One aggregate over every eligible row, capped with `LEAST`.

`pg_class` picks the walk only when its worst case,
`(1 − all-visible share) × queues × cap` heap visits, is below the page count
of the table. Postgres skips the branch that does not run. The count runs in
its own transaction with `SET LOCAL enable_sort = off`, so a walk cannot sort
every eligible row first. The sampler sums the shards and caps the sum again.

## 📊 Measurement

Same harness, same fixture, same session. Tool: `pg_stat_statements`. The
table is vacuumed, so the count walks.

| Pending rows | Depth buffers before | Depth buffers after | Change | All buffers before | All buffers after | Change |
| --: | --: | --: | --: | --: | --: | --: |
| 20,000 | 32,623 | 35,089 | +7.6% | 218,108 | 220,514 | +1.1% |
| 100,000 | 61,527 | 49,155 | -20.1% | 193,220 | 210,283 | +8.8% |
| 300,000 | 126,263 | 79,871 | -36.7% | 338,752 | 292,304 | -13.7% |

* The all-buffers column includes the age statement, which this change does
  not touch. It read 35,511 buffers at 100,000 rows before and 64,946 after.
  Issue #2339 measured 65,057. That difference is noise in the age statement,
  and it hides the depth saving in the 100,000 total.
* Statements: 480 before, 600 after. Each depth call adds `BEGIN`,
  `SET LOCAL` and `COMMIT`. Those read no buffers.
* WAL bytes of the depth statement: 1,824, 1,596 and 1,539, before and after.
  It writes nothing.
* `after-explain.txt` holds the plans of the exact and the bounded count on
  the same table. In each bounded plan the walk runs and the pass shows
  `never executed`. Each walk is an index-only scan of
  `idx_harvest_tq_coverage_sample` with at most 10,000 rows per loop.

One deep queue, from `deep-queue-explain.txt`:

| Query | Plan | Buffers |
| --- | --- | --: |
| Exact count | Parallel sequential scan, 1,000,000 rows | 22,744 |
| Bounded count | Walk: index-only scan, 10,000 rows | 114 |

Empty visibility map, from `empty-visibility-map-explain.txt`. The exact count
makes one sequential pass. Each count ran twice, and the table shows the
second run.

| Heap order | Exact count | Walk as planned | Walk, sorts off | Shipped count |
| --- | --: | --: | --: | --: |
| fifo | 14,152 | 99,829 | 81,479 | 14,139 (pass) |
| mixed | 14,152 | 99,127 | 108,202 | 14,139 (pass) |
| random | 14,152 | 98,937 | 155,878 | 14,139 (pass) |
| mixed, vacuumed | 2,228 | — | — | 1,480 (walk) |

The walk as planned sorts the bitmap scan of each queue. With sorts off it
visits a heap page per uncovered row. Neither beats one pass while the map is
empty. The shipped count makes one pass there and walks after the vacuum.

The impact floor needs 20% fewer buffers on a statement that is at least 5%
of the workload. The statement is 31.8% and 37.3% of the buffers at the two
larger sizes. The change removes 20.1% and 36.7% of them.

## ✅ Equivalence

The gauge value changes above 10,000 by design. Below the cap it does not.

* `bounded_depths_match_the_exact_count_up_to_the_cap` runs the bounded and
  the exact count at ten caps, each cap in one transaction, so `NOW()` is the
  same for both. It runs before a vacuum, on the pass, and after it, on the
  walk. The cases are an empty table and list, a repeated name, future,
  running and completed rows, a paused queue and a queue outside the list.
* `bounded_depth_scan_stops_at_the_cap` reads the plan with an empty
  visibility map and with future rows. Each scan must walk the index and read
  at most the cap.
* `bounded_depth_counts_in_one_pass_until_a_vacuum` checks the pass with an
  empty map at the gauge cap, and the walk after a vacuum.
* `queue_depth_gauge_caps_the_sum_of_the_shards` runs a worker on two shards
  of 6,000 rows and expects 10,000.
* `formal/verus/src/depth_cap.rs` proves the shard fold for every input.

## 💸 Write cost

None. The change adds no index and writes no rows.

## 🔬 Reproduce

```sh
export HARVEST_TEST_DATABASE_URL=postgres://postgres@localhost/postgres
# The server needs shared_preload_libraries=pg_stat_statements and a superuser.
# Baseline: the parent commit, whose harness writes to queue-age-gauge/.
PERF_LABEL=before cargo test -p autumn-harvest --features db --test integration \
  queue_age_gauge_perf -- --include-ignored --nocapture --test-threads=1
# This change:
PERF_ARTIFACT_DIR=queue-depth-gauge PERF_LABEL=after cargo test -p autumn-harvest \
  --features db --test integration queue_age_gauge_perf -- --include-ignored \
  --nocapture --test-threads=1
psql -d scratch -f docs/perf-artifacts/queue-depth-gauge/deep-queue.sql
# On a database that holds the harvest schema:
psql -X -d migrated -f docs/perf-artifacts/queue-depth-gauge/empty-visibility-map.sql
```
