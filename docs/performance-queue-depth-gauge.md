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
* **It now counts each queue up to 10,000 rows.** Each queue scan walks
  `idx_harvest_tq_coverage_sample` and stops at the cap. Depth buffers fall by
  20.2% at 100,000 and 36.8% at 300,000 pending rows. One deep queue of
  1,000,000 rows reads 111 buffers instead of 22,744.
* **The small fixture pays 7.2% more.** No queue there reaches the cap, and
  each queue now costs one index descent.
* **Below 10,000 the gauge is exact.** A value of 10,000 means 10,000 or
  more.
* **No migration, no new index, no new write.**

## 🎯 Workload

The harness is the poll-tick harness of issue #2339,
`tests/integration/queue_age_gauge_perf.rs`. One tick runs, for each of four
workers, the depth count, `oldest_pending_ages` and one `claim_task` with its
`complete_claimed_task`. Ten ticks make 480 statements. The fixture has 20
queues with a cubic skew, 5% future rows and one terminal row per pending
row. Three sizes run: 20,000, 100,000 and 300,000 pending rows.

`deep-queue.sql` adds one point that the harness does not reach. One queue
holds 1,000,000 eligible rows and 1,000,000 terminal rows. The table holds
the columns that the queries read and the three pending indexes of the
migrations.

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

## 🔧 Change

`queue::bounded_queue_depths` runs one `LATERAL` subquery per distinct queue
name. The subquery walks `idx_harvest_tq_coverage_sample` in key order with a
`LIMIT` at the cap. The `ORDER BY` matters. Without it the planner can pick a
sequential scan when the visibility map is low, or an index that also reads
future rows. A queue below the cap then reads the whole table on each tick.
The sampler sums the shards and caps the sum again.

## 📊 Measurement

Same harness, same fixture, same session. Tool: `pg_stat_statements`.

| Pending rows | Depth buffers before | Depth buffers after | Change | All buffers before | All buffers after | Change |
| --: | --: | --: | --: | --: | --: | --: |
| 20,000 | 32,623 | 34,969 | +7.2% | 218,108 | 225,220 | +3.3% |
| 100,000 | 61,527 | 49,115 | -20.2% | 193,220 | 180,706 | -6.5% |
| 300,000 | 126,263 | 79,751 | -36.8% | 338,752 | 292,224 | -13.7% |

* Statements: 480 before and after.
* WAL bytes of the depth statement: 1,824, 1,596 and 1,539, before and after.
  It writes nothing.
* The claim statement read 4,703 more buffers at 20,000 rows. It is the same
  code. The difference is noise.
* `after-explain.txt` holds the plans of the exact and the bounded count on
  the same table. Each bounded scan is an index-only scan of
  `idx_harvest_tq_coverage_sample` with at most 10,000 rows per loop.

One deep queue, from `deep-queue-explain.txt`:

| Query | Plan | Buffers |
| --- | --- | --: |
| Exact count | Parallel sequential scan, 1,000,000 rows | 22,744 |
| Bounded count | Index-only scan, 10,000 rows | 111 |

The cost per call is now bounded by the number of queues times the cap, not
by the backlog. A queue below the cap costs what it costs today, plus one
index descent. At 20,000 rows no queue reaches the cap, so that descent is
the whole difference. The entries of `idx_harvest_tq_coverage_sample` are
wider than those of `idx_harvest_tq_poll`, so a queue near the cap reads a
few more pages than the exact count would.

The impact floor needs 20% fewer buffers on a statement that is at least 5%
of the workload. The statement is 31.8% and 37.3% of the buffers at the two
larger sizes. The change removes 20.2% and 36.8% of them.

## ✅ Equivalence

The gauge value changes above 10,000 by design. Below the cap it does not.

* `bounded_depths_match_the_exact_count_up_to_the_cap` runs the bounded and
  the exact count at ten caps, each cap in one transaction, so `NOW()` is the
  same for both. The cases are an empty table and list, a repeated name,
  future, running and completed rows, a paused queue and a queue outside the
  list.
* `bounded_depth_scan_stops_at_the_cap` reads the plan with an empty
  visibility map and with future rows. Each scan must walk the index and read
  at most the cap.
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
```
