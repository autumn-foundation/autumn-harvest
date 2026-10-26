## Phase — Queue depth gauge counts up to a cap (Ledger, issue #2356)

The `harvest.queue.depth` sampler no longer counts every eligible pending row.
`queue::bounded_queue_depths` counts each queue up to
`telemetry::QUEUE_DEPTH_CAP` (10,000). A `LATERAL` subquery per queue walks
`idx_harvest_tq_coverage_sample` in key order and stops at the cap. The sampler
sums the shards with `add_bounded_depth` and caps the sum again. Below 10,000
the gauge is exact. A value of 10,000 means 10,000 or more.
`queue::queue_depths` stays exact. There is no migration, no new index and no
new write.

**Behaviour change.** An alert that needs a depth above 10,000 can no longer
fire. The starter rule `harvest_queue_backlog_growth` keys on growth, and a
gauge at the cap shows none. While the gauge holds at the cap, the rule now
reads growth from `harvest.queue.oldest_pending_age`, which has no cap. It also
fires when that age stays 0, because then a gate blocks every row. promtool
cases in `docs/alerts/starter-pack-v0.1.0.backlog-growth.test.yml` test the
rule in the `lint` job. For an exact count, scrape `harvest_queue_backlog`
from `GET /admin/queues/scaling?format=prometheus`. Autoscaling reads that metric,
so it does not change. The load shedder reads the age, not the depth, so it
does not change either.

`docs/design/DESIGN-2356.md` records the options and the decision. Tests:
`bounded_depths_match_the_exact_count_up_to_the_cap` compares the bounded and
exact counts at ten caps, each cap in one transaction.
`bounded_depth_scan_stops_at_the_cap` checks that each queue scan walks the
index and reads at most the cap, with no visibility map and with future rows.
`queue_depth_gauge_caps_the_sum_of_the_shards` runs a worker on two shards of
6,000 rows each and expects 10,000. `queue_depth_alerts_sit_below_the_gauge_cap`
keeps the starter thresholds within reach of the gauge.
`formal/verus/src/depth_cap.rs` proves that the shard fold equals the capped
total. The measurement is in `docs/performance-queue-depth-gauge.md`.
