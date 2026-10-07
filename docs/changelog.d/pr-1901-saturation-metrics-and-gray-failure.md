## Phase — DB-pool, query-latency and poller metrics, and a gray-failure signal (issue #1815)

Harvest reported no saturation of its database, and worker health was a
heartbeat check only. A worker that was alive but sick passed that check.

**New metrics.** All labels are bounded.

- `harvest.db.pool.in_use{shard}` and `harvest.db.pool.idle{shard}`: gauges
  from the deadpool status of each shard pool. A new in-memory sampler reads
  them on the `poll_interval` cadence. No query runs. When runtimes in one
  process share a metrics sink with separate pools for one shard, `in_use` is
  their sum and `idle` reads 0 while any of them is exhausted. A worker
  without a sharded pool claims every assigned shard through its one pool,
  so it reports that pool under each assigned shard label.
- `harvest.db.pool.wait_duration{shard}`: histogram of the wait for a pooled
  connection. The claim path, the timeout scanner and the activity heartbeat
  flush record it. A failed or timed-out wait counts too. A worker without
  a sharded pool records each wait and each query sample under each assigned
  shard, as its pool gauges do. Its timeout scanner does the same, so a
  shard-filtered panel shows the state and the use of one pool together.
- `harvest.db.query.duration{op, shard}`: histogram per op and shard, so a
  slow shard of a multi-shard worker stays visible.
  - `claim` times one claim query.
  - `persist` times the workflow-task persist transaction, COMMIT included.
    A drop guard records a transaction that a timeout cancels. A failure
    that an early error path or a history-cap breach commits instead is
    timed as a `persist` too.
  - `scan` times one timeout-scanner pass.
  - `heartbeat` times one activity heartbeat write.
- `harvest.worker.pollers{queue}`: gauge set when a poll loop starts and
  ends. The count covers every worker in the process. A drained process reads 0.
- `harvest.worker.outlier{dimension}`: gauge, 1 or 0, for `failure_ratio`
  and `latency_p99`.

**Gray-failure signal.** New module `worker_outlier.rs`.

- Each worker keeps a rolling window of its task outcomes. The window holds
  at most 1024 samples, from the last 5 minutes or two heartbeat intervals,
  whichever is longer. A slow heartbeat then still publishes every outcome.
  When a codec key reload moves the worker into a new cohort, the window
  drops the tasks dispatched before the reload, so outcomes from the old
  cohort do not flag it. The codec registry stamps each key change when it
  happens, so a task dispatched after the change stays, and a change back
  to an earlier key still restarts the window. A retirement of a key that
  is not registered changes nothing, so it keeps the window. The worker
  enters its cohort before any task runs.
- A failed activity attempt counts as a failure. So does an attempt that an
  open circuit breaker rejects, because the breaker belongs to the worker.
  An activity that succeeds but does not finalize also counts as a failure.
  So does an activity whose setup loses its database write. A failed
  deferral write counts too, for the retry budget, the rate limit, an open
  breaker in defer mode and the adaptive limit. When that write
  fails with a transient database error, or with a transaction conflict
  whose retries ran out, the dispatch loop releases the claim first. The attempt then counts with the release time, and not at all
  when a peer took the claim.
  A cancelled activity attempt is not counted, and neither is one whose
  claim a later owner took before it finalized. An attempt that the timeout
  scanner timed out is a failure, although the scanner cancels it and takes
  its claim: the handler hung. A session acquire or release
  counts when it finalizes or fails, but not when it defers for capacity.
  A workflow task counts as a failure when it returns an error or times out,
  and when its cycle fails the run, as a workflow body that returns `Err` does.
  So does a cycle that deadlocks, or panics within its retry budget, and so
  re-pends the task while the run stays `RUNNING`. So does a panic past that
  budget, which fails the run terminally. So does a cycle whose history
  reaches the event or byte cap, which moves the run to the DLQ.
  The failure counts after its claim-fenced reset or quarantine, with its
  latency taken then. It is left out when that write finds a peer owns the
  claim. An early error path can fail the run itself before the reset. The
  row then stays failed under this claim's fence, so that failure counts.
  A release is not counted.
  `worker::reset_timed_out_workflow_task` now returns a `ClaimRecovery`.
  The panic re-pend is now fenced by the claim, like the deadlock re-pend,
  through the new `queue::requeue_claimed_workflow_task_after_panic`. A stale
  dispatcher no longer re-pends a peer's newer claim.
- Every liveness heartbeat writes a snapshot to the new table
  `harvest_worker_task_stats` and reads the live peers of its shard. Each
  heartbeat then compares the worker with the merged peers of all its
  shards. They all see the same peer set, so the gauge does not flap. A
  healthy shard keeps the verdict live when another shard fails.
- A stopped or aborted heartbeat retires the worker's verdict.
- Each stats row carries a `snapshot_seq` that the worker writes. When two
  shards hold a row for one worker, the higher sequence wins. Shard clocks
  can differ, so `updated_at` is not used to order them. A restarted worker
  that keeps its id always writes above the rows of its previous process.
- The snapshot and its cohort key are read in one serialized step. A codec
  change can still land between the two reads, so the step reads the key
  again after the snapshot and captures again until the key holds. When the
  key changes after each of five snapshots, the tick publishes nothing and
  clears its view. The next tick captures again.
- Startup registration drops the worker's stats row in its transaction. A
  restarted process therefore does not show its previous process's
  failures before its first heartbeat.
- Two workers in one process share the gauge, so it reports the OR of their
  verdicts.
- The build id is the only code identity in the cohort key. Workers without
  one share a cohort across code versions that register the same names, so
  set `build_id` for a rolling deployment.
- The peers are the live `Active` workers that poll the same queues with the
  same `queue_weights` (with weights, a listed-twice queue and the order of
  the zero-weight queues count too), on the same build with the same labels and the same
  slots per task kind (or the same slot-tuner band and tuner policy),
  session capacity, priority aging, activity eligibility, shards, registered
  handlers, circuit-breaker policies, retry-budget policies, adaptive-limit policies, dispatch route
  per shard (a dispatch channel or the Postgres claim), outcome window and peer
  freshness limit (both follow the heartbeat interval), and execution policy
  (`sticky_timeout`, `workflow_cache_size`, `resident_workflows`,
  `workflow_task_timeout`, `max_local_activity_start_to_close`,
  `workflow_panic_max_attempts`, `poison_pill_threshold`,
  `cancellation_grace_period`), and payload policy
  (`max_activity_input_bytes`, `max_workflow_input_bytes`,
  `max_activity_result_bytes`, `max_signal_payload_bytes`,
  `max_current_details_bytes`, the history policy, the workflow execution
  timeout ceiling, the offload threshold and store id, the
  activity interceptor chain, and each activity's effective result and input
  caps, local flag, rate limit, concurrency limit and WASM binding, a local
  activity's own retry, start-to-close and schedule-to-close defaults, the registry's
  local-activity defaults and retry-after ceiling, the hot-code-swap
  module-host policy, each workflow's effective input cap, DAG
  classification, quota and declared execution timeout, the declarative
  query and update handlers, and the workflow
  log policy), the registered payload codec ids and default codec, and
  the registered and active codec key ids, which the
  heartbeat reads on every tick because a reload can change them, with
  fresh stats. The cohort is
  keyed on every claim setting in one place, `workers::CohortPolicy`. Those decide which tasks a worker can claim. A
  worker on a slow queue is not compared with a fast queue. Each heartbeat
  reads only its own cohort, so the read stays small in a large fleet.
- `ActivityInterceptor` gains `policy()`, a stable description of what the
  interceptor does. It defaults to the type name. The cohort key holds the
  policy of each interceptor, in chain order.
- `SlotTuner` gains `policy()`, a stable description of every setting
  that changes `decide`. It defaults to the type name, because `name()`
  need not be unique. `DefaultSlotTuner`
  includes its grow step, shrink step and wait threshold. A custom tuner
  with settings overrides it, so that differently tuned workers are not
  peers.
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
- Every shard heartbeat deletes stats rows older than one hour, so orphan
  rows from restarted workers do not pile up. A row whose own cohort has a
  longer peer freshness window keeps it, so a fast worker's prune leaves a
  slow peer's live row.

**`GET /admin/status`.** The `workers` block gains `outliers`, the worst 20
flagged workers with their stats and the peer medians. It also gains
`outliers_total`. Any outlier degrades the block with reason code
`worker_outlier`. A failed stats read skips only the outlier signal; the
shard still counts as inspected. Status keeps each cohort's rows for that
cohort's own freshness limit, read from its key, so a fast API runtime does
not drop a slow cohort between its heartbeats
(`workers::load_live_worker_task_stats_per_cohort`).

**Preflight.** `/admin/preflight` checks `SELECT`, `INSERT`, `UPDATE` and
`DELETE` on `harvest_worker_task_stats`.

**Built-in scrape endpoint.** `HarvestMetricsRecorder` renders all six new
metrics. The two histograms render as `_count` and `_sum`.

**Dashboard and alerts.**

- A new collapsed row, "Database pool, queries & pollers", with five panels.
- Three new rules with runbook sections: `harvest_worker_gray_failure`,
  `harvest_db_pool_wait_high` and `harvest_db_query_latency_high`.
- The two latency rules carry a bucket-less average fallback. Both keep the
  `instance` label, so one slow replica cannot hide behind healthy ones.

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
