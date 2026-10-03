## Phase — DB-pool, query-latency and poller metrics, and a gray-failure signal (issue #1815)

Harvest reported no saturation of its database, and worker health was a
heartbeat check only. A worker that was alive but sick passed that check.

**New metrics.** All labels are bounded.

- `harvest.db.pool.in_use{shard}` and `harvest.db.pool.idle{shard}`: gauges
  from the deadpool status of each shard pool. A new in-memory sampler reads
  them on the `poll_interval` cadence. No query runs.
- `harvest.db.pool.wait_duration{shard}`: histogram of the wait for a pooled
  connection. The claim path, the timeout scanner and the activity heartbeat
  flush record it. A failed or timed-out wait counts too.
- `harvest.db.query.duration{op}`: histogram per op.
  - `claim` times one claim query.
  - `persist` times the workflow-task persist transaction, COMMIT included.
    A drop guard records a transaction that a timeout cancels.
  - `scan` times one timeout-scanner pass.
  - `heartbeat` times one activity heartbeat write.
- `harvest.worker.pollers{queue}`: gauge set when a poll loop starts and
  ends. The count covers every worker in the process. A drained process reads 0.
- `harvest.worker.outlier{dimension}`: gauge, 1 or 0, for `failure_ratio`
  and `latency_p99`.

**Gray-failure signal.** New module `worker_outlier.rs`.

- Each worker keeps a rolling window of its task outcomes. The window holds
  the last 5 minutes and at most 1024 samples.
- A failed activity attempt counts as a failure. A workflow task counts as a
  failure when it returns an error or times out. A release is not counted.
- Every liveness heartbeat writes a snapshot to the new table
  `harvest_worker_task_stats`. One heartbeat per worker then compares it.
  A multi-shard worker compares on its first shard only, so the gauge does
  not flap between peer sets.
- The peers are the live `Active` workers that poll the same queues, with
  fresh stats. A worker on a slow queue is not compared with a fast queue.
- A worker is an outlier on failure ratio when its ratio is at least 20
  points above the peer median. The ratio must also be at least twice the
  median.
- A worker is an outlier on latency when its p99 is at least 3 times the
  peer median. The p99 must also be at least 100 ms above the median.
- A worker needs 20 tasks, and 2 such peers, to be judged. The rule uses the
  peer median, so a fault that hits the whole fleet flags no worker.
- A draining worker, or a tick that cannot compare, sets the gauge to 0.
- No worker label is added. Each worker reports itself, and the scrape
  `instance` label tells them apart.
- The tick deletes stats rows older than one hour, so orphan rows from
  restarted workers do not pile up.

**`GET /admin/status`.** The `workers` block gains `outliers`, the worst 20
flagged workers with their stats and the peer medians. It also gains
`outliers_total`. Any outlier degrades the block with reason code
`worker_outlier`. A failed stats read skips only the outlier signal; the
shard still counts as inspected.

**Preflight.** `/admin/preflight` checks `SELECT`, `INSERT`, `UPDATE` and
`DELETE` on `harvest_worker_task_stats`.

**Built-in scrape endpoint.** `HarvestMetricsRecorder` renders all six new
metrics. The two histograms render as `_count` and `_sum`.

**Dashboard and alerts.**

- A new collapsed row, "Database pool, queries & pollers", with five panels.
- Three new rules with runbook sections: `harvest_worker_gray_failure`,
  `harvest_db_pool_wait_high` and `harvest_db_query_latency_high`.
- The two latency rules carry a bucket-less average fallback.

**Migration.** `20261003203735_harvest_worker_task_stats` adds one table and
an index on `updated_at`. No `WorkflowEvent` variant, no change to
`harvest_events`, no replay impact.

**Also fixed.** `activity_default_timeout_tests.rs` did not set the
`resident_workflows` field that #1892 added. The `integration` target then
did not compile on `trunk-dev`.

**Tests.**

- `worker_outlier.rs` unit tests. The issue's RED test is
  `worker_failing_half_its_tasks_is_flagged_and_healthy_peers_are_not`.
  Other tests cover a fleet-wide fault, latency, cohorts, min samples, min
  peers, `min_peers: 0` and the window bounds.
- `worker.rs` unit tests cover `pool_occupancy` and the process-wide
  `PollerGuard`.
- Telemetry name and no-op tests. A metrics-rs bridge test checks each name,
  label and value. A scrape-recorder render test does the same.
- `status_summary.rs` tests cover the 50% worker, the cross-shard dedupe,
  cohorts and the cap.
- `tests/integration/worker_saturation_metrics_tests.rs` runs against
  Postgres.
  - `outlier_tick_flags_the_worker_failing_half_its_tasks` runs the real tick
    for a 50% worker and three healthy peers.
  - A running worker emits every new metric. Its heartbeat publishes a stats
    row that counts the failed attempts.
  - The suite also covers cohorts, draining, frozen rows, the prune, the FK
    cascade and the `scan` op.
- `dashboard_pack_docs` and `alert_pack_docs` cover the new panels and rules.
