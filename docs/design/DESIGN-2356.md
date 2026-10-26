# Design — Issue #2356: bound the queue depth gauge

`queue::queue_depths` fed the `harvest.queue.depth` gauge. It counts every
eligible pending row of each queue, so its cost grows with the backlog. After
issue #2339 it is the largest poll-tick cost at 300,000 pending rows and the
second largest at 100,000. This change bounds the count per queue. The gauge
stays exact below the bound.

**No migration. No new index. No write on the hot path.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- The depth sampler (`spawn_queue_depth_sampler` in `worker.rs`) is the only
  engine caller of `queue_depths`. It runs once per worker `poll_interval`
  when a metrics recorder is set.
- The sampler sums the depth of each queue over every shard. Every worker
  computes the same fleet-wide value.
- The query is an exact `COUNT(*)`. On the issue fixture it reads 816, 1,538
  and 3,157 buffers per call at 20,000, 100,000 and 300,000 pending rows. That
  is 15.0%, 31.8% and 37.3% of the poll-tick buffers.
- The load shedder does not read depth. It reads `oldest_pending_ages` only
  (`load_shed.rs`). The issue states otherwise. That statement is not
  correct.
- Autoscaling does not read depth either. `GET /admin/queues/scaling` serves
  `harvest_queue_backlog` from `queue_task_counts`, an exact count.
- Two starter alerts read depth. `harvest_queue_backlog_growth` fires on
  `max_over_time(depth[10m]) > 100 and deriv(depth[10m]) > 0`. A variant of
  the queue-pause alert fires on `depth > 1000`.
- `harvest.queue.oldest_pending_age` has no cap. It applies the claim gates,
  so it reads 0 for a paused queue. The schedule-to-start alert is the
  primary capacity page.
- `idx_harvest_tq_coverage_sample` holds `(queue_name, scheduled_at, id)` for
  pending rows. A walk in that order can stop after a fixed number of rows.
- Eligibility depends on `scheduled_at <= NOW()`. A row becomes eligible as
  time passes, with no write.
- About 50 test files build `WorkerRuntimeConfig` as a struct literal. A new
  field changes all of them.

### 0.2 Brainstorm — how can the depth cost stop growing?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A counter row per queue, kept by triggers. | Rejected. Every enqueue, claim and complete writes one hot row. Claims then wait on it. A counter also cannot see a row become eligible with time. |
| B2 | A rollup table refreshed by a job. | Rejected. It needs a migration and a maintenance invariant. It is stale by design. |
| B3 | The planner row estimate. | Rejected. The estimate ignores `scheduled_at <= NOW()` and drifts with autovacuum. |
| B4 | A narrower partial index. | Rejected. The cost stays linear. It adds a write to the hottest table. |
| B5 | Count every N ticks. | Rejected. The cost stays linear. It only divides it. |
| B6 | Refresh period grows with the last depth. | Rejected. A refresh tick still reads the whole backlog. A deep queue holds a stale value after it drains. |
| B7 | One worker counts for the fleet. | Rejected. It needs leader election and a shared store. |
| B8 | Count each queue up to a fixed cap. Report the cap when the queue holds more. | **Adopted.** The cost per queue has a fixed bound. Below the cap the value is exact. |
| B9 | Make the cap a `WorkerRuntimeConfig` field. | Rejected. About 50 test files change. A constant serves the alerts that ship. |
| B10 | Keep `queue_depths` exact and add a bounded function. | **Adopted.** The public contract does not change. The exact function is the test oracle. |
| B11 | Sum the shards, then cap the sum. | **Adopted.** Each shard value is `min(count, cap)`. So the capped sum is `min(total, cap)`. Verus proves it. |
| B12 | At the cap, read growth from the oldest pending age. | **Adopted.** Without it the backlog alert clears when the backlog is largest. A clause on the level alone would fire on a healthy drain. |
| B15 | At the cap, also fire when the age stays 0 for the whole window. | **Adopted.** The age gauge applies the claim gates. When a gate blocks every row, it reads 0 and shows no growth. |
| B16 | Test the rule with promtool. | **Adopted.** A loadable copy of the rule and promtool cases run in the `lint` job. A guard keeps the copy equal to the pack. |
| B13 | Order the bounded scan by the index key. | **Adopted.** It pins the plan to the ordered index walk. |
| B14 | Keep the cap in `telemetry`. | **Adopted.** It is the contract of the gauge. The alert guard then runs in the CI leg without the `db` feature. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The gauge goes flat at the cap. `deriv` is 0, so the backlog alert clears during the worst backlog. | At the cap the rule fires on the growth of the oldest pending age. A guard test pins the clause to the constant. |
| R11 | A gate blocks every row of a deep queue. The age gauge reads 0, so the age clause sees no growth either. | At the cap, an age of 0 for the whole window fires the rule. A promtool case covers it. |
| R2 | A cap at or below an alert threshold. The alert can never fire. | The cap is 10,000. The largest strict starter threshold is 1,000. A guard test fails when a starter threshold cannot fire under the cap. |
| R3 | `LIMIT` after `GROUP BY` caps the number of queues, not the rows. | The limit sits in a `LATERAL` subquery per queue. A shape test checks it. |
| R4 | A `LIMIT` with no order lets the planner pick a sequential scan, or an index that reads future rows. A shallow queue then reads the whole table on each tick. | The subquery orders by `scheduled_at, id`. A test with an empty visibility map and future rows checks that each scan walks the index and reads at most the cap. |
| R5 | The shard sum reports `shards × cap`. | The sampler caps the sum. A Verus proof, a proptest and a two-shard worker test cover it. |
| R6 | A duplicate queue name in the input counts twice. | The query takes `DISTINCT` names. A test covers it. |
| R7 | The bounded value differs from the exact count below the cap. | A test compares both functions at ten caps, each cap in one transaction. |
| R8 | A drained queue keeps a stale value. | The sampler reads every queue on every tick, as before. |
| R9 | An operator alert above 10,000 no longer fires. | The changelog, the telemetry reference and the alert notes state the cap. They name the exact `harvest_queue_backlog` metric. |
| R10 | The perf harness stops measuring the depth statement. | The SQL keeps the `AS depth` alias that the harness filters on. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The count is exact and linear.
- **Red (feelings).** A gauge that stops at a cap feels like it hides the
  incident. At that depth the operator needs "very deep" and a trend. The age
  gauge gives the trend, and the scaling endpoint gives the exact count.
- **Black (risks).** Section 0.3. The main risks are a silent alert and a
  plan that reads the whole table.
- **Yellow (benefits).** The cost per queue has a fixed bound at any backlog.
  No migration and no write tax. Below the cap the value is exact, so small
  queues see no change.
- **Green (ideas).** A `harvest.queue.depth.capped` gauge. A configurable
  cap. None is needed for this issue.
- **Blue (process).** Write the red tests, then the green code, then
  refactor. Measure with the issue harness. Then review from several angles.

### 0.5 Decision

The gauge reports `min(eligible pending rows, 10,000)` per queue, summed over
shards and then capped. The value is exact below 10,000. A value of 10,000
means "10,000 or more".

---

## 1. Change

- `telemetry::QUEUE_DEPTH_CAP` is 10,000.
- `queue::bounded_queue_depths(conn, queues, cap)` counts each queue up to
  `cap`. A `LATERAL` subquery per distinct queue name walks
  `idx_harvest_tq_coverage_sample` in key order and stops after `cap` rows.
  `bounded_queue_depths_query()` returns the SQL, so tests can read its plan.
- `queue::queue_depths` stays exact. It is the oracle of the tests.
- `queue::add_bounded_depth` adds one shard value to a running total and
  caps the result. The sampler folds the shards with it.
  `formal/verus/src/depth_cap.rs` proves that the fold equals the capped
  total.
- `harvest_queue_backlog_growth` fires at the cap when the oldest pending age
  grows over the window, or stays 0 for the whole window.
  `docs/alerts/starter-pack-v0.1.0.backlog-growth.test.yml` tests the rule
  with promtool in the `lint` job.

## 2. Known limits

- Above the cap the gauge shows no trend. Use
  `harvest.queue.oldest_pending_age` and schedule-to-start. The age gauge
  reads 0 for a paused queue.
- A custom alert on a depth above 10,000 can no longer fire.
- The bound per call is the number of queues times the cap, not the backlog.
  A queue at the cap reads 10,000 index entries, plus heap pages that the
  visibility map does not cover.

## 3. Tests and acceptance criteria

| AC | Test |
|----|------|
| Below the cap, the bounded depth equals the exact depth. At or above the cap, it equals the cap. | `bounded_depths_match_the_exact_count_up_to_the_cap` |
| Each queue scan walks the index and reads at most the cap, with no visibility map and with future rows. | `bounded_depth_scan_stops_at_the_cap` |
| The sampler caps the sum of the shards. | `queue_depth_gauge_caps_the_sum_of_the_shards` |
| The capped shard sum equals the capped total. | `add_bounded_depth_caps_the_sum_of_the_shards`, `a_fold_of_bounded_shards_is_the_capped_total` and `formal/verus/src/depth_cap.rs` |
| Each starter depth threshold can fire under the cap, and the backlog rule fires at the cap. | `queue_depth_alerts_sit_below_the_gauge_cap` |
| The backlog rule fires on growth and on a fully gated backlog at the cap, and stays silent on a drain and a steady queue. | promtool cases in `docs/alerts/starter-pack-v0.1.0.backlog-growth.test.yml` and `the_backlog_rule_file_matches_the_starter_pack` |
| Depth buffers drop at 100,000 and 300,000 pending rows. | `queue_age_gauge_perf`, see `docs/performance-queue-depth-gauge.md` |
