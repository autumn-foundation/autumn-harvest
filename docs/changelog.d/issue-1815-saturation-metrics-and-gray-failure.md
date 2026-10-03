## Phase — DB-pool, query-latency and poller metrics, and a gray-failure signal (issue #1815)

Harvest reported no saturation of its database, and worker health was a
heartbeat check only. A worker that was alive but sick passed that check.

**New metrics.** All labels are bounded.

- `harvest.db.pool.in_use{shard}` and `harvest.db.pool.idle{shard}`: gauges
  from the deadpool status of each shard pool. A new in-memory sampler reads
  them on the `poll_interval` cadence. No query runs.
- `harvest.db.pool.wait_duration{shard}`: histogram of the wait for a pooled
  connection on the claim path, the timeout scanner and the activity
  heartbeat flush. A failed or timed-out wait counts too.
- `harvest.db.query.duration{op}`: histogram per op. `claim` times one claim
  query. `persist` times the workflow-task persist transaction, COMMIT
  included. `scan` times one timeout-scanner pass. `heartbeat` times one
  activity heartbeat write or one worker liveness tick.
- `harvest.worker.pollers{queue}`: gauge set when a poll loop starts and
  ends. One loop claims from all of a worker's queues. A drained worker reads 0.
- `harvest.worker.outlier{dimension}`: gauge, 1 or 0, for `failure_ratio`
  and `latency_p99`.

**Gray-failure signal.** New module `worker_outlier.rs`. Each worker keeps a
rolling window of its task outcomes: 1024 samples at most, 5 minutes at
most. An activity attempt that fails counts as a failure. A workflow task
counts as a failure when it returns an error or times out. A release is not
counted. The liveness heartbeat writes a snapshot to the new table
`harvest_worker_task_stats`, then compares it with its live `Active` peers.
A worker is an outlier when its failure ratio is at least 20 points above the
peer median and at least twice it, or when its p99 latency is at least 3
times the peer median and at least 100 ms above it. A worker needs 20 tasks,
and 2 such peers, to be judged. The rule uses the peer median, so a fault
that hits the whole fleet flags no worker. No worker label is added: each
worker reports itself, and the scrape `instance` label tells them apart.

**`GET /admin/status`.** The `workers` block gains `outliers`, a list of the
flagged workers with their stats and the peer medians. Any entry degrades the
block with reason code `worker_outlier`. A failed stats read skips only the
outlier signal; the shard still counts as inspected.

**Built-in scrape endpoint.** `HarvestMetricsRecorder` renders all six new
metrics. The two histograms render as `_count` and `_sum`.

**Dashboard and alerts.** A new collapsed row, "Database pool, queries &
pollers", with five panels. Three new rules with runbook sections:
`harvest_worker_gray_failure`, `harvest_db_pool_wait_high` and
`harvest_db_query_latency_high`.

**Migration.** `20261003203735_harvest_worker_task_stats`: one new table.
No `WorkflowEvent` variant, no change to `harvest_events`, no replay impact.

**Also fixed.** `activity_default_timeout_tests.rs` did not set the
`resident_workflows` field that #1892 added, so the `integration` target did
not compile on `trunk-dev`.

**Tests.**

- `worker_outlier.rs` unit tests. The issue's RED test is
  `worker_failing_half_its_tasks_is_flagged_and_healthy_peers_are_not`. Other
  tests cover a fleet-wide fault, latency, min samples, min peers and the
  window bounds.
- Telemetry name and no-op tests. A metrics-rs bridge test checks each name,
  label and value. A scrape-recorder render test does the same.
- `status_summary.rs`: `merge_bundle_flags_the_worker_failing_half_its_tasks`
  and two response tests.
- `tests/integration/worker_saturation_metrics_tests.rs` against Postgres.
  `outlier_tick_flags_the_worker_failing_half_its_tasks` runs the real tick
  for a 50% worker and three healthy peers. A running worker emits every new
  metric and publishes a stats row that counts its failed attempts. The
  suite also covers the live-peer filter, the FK cascade and the `scan` op.
- `dashboard_pack_docs` and `alert_pack_docs` cover the new panels and rules.
