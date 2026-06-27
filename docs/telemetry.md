# Wiring harvest metrics into your Prometheus / OTLP pipeline

Autumn-harvest ships a `MetricsRecorder` trait and nine pre-defined metric
instruments that cover every production-observable aspect of the engine. The
default implementation (`NoOpMetrics`) is zero-cost; no samples are emitted
until you register a recorder.

## Quick start with `metrics-exporter-prometheus`

Enable the in-tree `metrics-rs` adapter in your application's `Cargo.toml`:

```toml
[dependencies]
autumn-harvest = { version = "0.2", features = ["metrics-rs"] }
metrics-exporter-prometheus = "0.16"
```

Then install an exporter and wire the recorder before starting any workers:

```rust
use autumn_harvest::metrics_rs_adapter::MetricsRsRecorder;
use autumn_harvest::telemetry::TelemetryConfig;
use metrics_exporter_prometheus::Matcher;
use std::sync::Arc;

// 1. Install the Prometheus exporter (do this once at process start).
//
//    `metrics-exporter-prometheus` renders every histogram as a Prometheus
//    *summary* (client-side quantiles) UNLESS you configure explicit bucket
//    boundaries. The starter alert pack pages on
//    `histogram_quantile(0.99, rate(harvest_queue_schedule_to_start_bucket[5m]))`,
//    which only exists when buckets are set — and server-side `_bucket` series
//    are the only form you can aggregate across replicas.
//
//    Scope the seconds-oriented boundaries to the *latency* histograms with
//    `set_buckets_for_metric` rather than the global `set_buckets`. A global
//    bucket set is applied to every histogram, including non-duration ones such
//    as `harvest.workflow.history_size` (durable event counts, soft threshold
//    10k) and the payload-size histogram (bytes) — seconds buckets would dump
//    every sample above 600 into `+Inf` and break count/byte dashboards.
//
//    NOTE: `Matcher::Full` is matched against the *sanitized* metric name —
//    `metrics-exporter-prometheus` calls `sanitize_metric_name` before looking up
//    the per-metric distribution — so the matcher must use the Prometheus form
//    with underscores (`harvest_queue_schedule_to_start`), NOT the dotted name
//    registered via the `metrics` macros. A dotted matcher silently fails to match
//    and the metric falls back to a summary (no `_bucket` series).
const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];
metrics_exporter_prometheus::PrometheusBuilder::new()
    .set_buckets_for_metric(
        Matcher::Full("harvest_queue_schedule_to_start".into()),
        LATENCY_BUCKETS,
    )
    .expect("valid bucket boundaries")
    .set_buckets_for_metric(
        Matcher::Full("harvest_workflow_duration".into()),
        LATENCY_BUCKETS,
    )
    .expect("valid bucket boundaries")
    .set_buckets_for_metric(
        Matcher::Full("harvest_activity_duration".into()),
        LATENCY_BUCKETS,
    )
    .expect("valid bucket boundaries")
    // history-size is a *count* histogram (durable events), not seconds — give
    // it its own boundaries so it stays usable for count dashboards/alerts.
    .set_buckets_for_metric(
        Matcher::Full("harvest_workflow_history_size".into()),
        &[10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0],
    )
    .expect("valid bucket boundaries")
    .install()
    .expect("failed to install Prometheus exporter");

// 2. Build a TelemetryConfig that forwards samples to the global registry.
let telemetry = TelemetryConfig::builder()
    .metrics(Arc::new(MetricsRsRecorder))
    .build();

// 3. Pass it to HarvestBuilder.
let harvest = autumn_harvest::HarvestBuilder::new(pool)
    .telemetry(telemetry)
    // ... rest of your config
    .build();
```

The exporter now serves `GET /metrics` in Prometheus text format. Grafana /
your OTLP collector can scrape it directly.

## OTLP / OpenTelemetry

Replace step 1 with your OTel SDK setup and a `metrics-exporter-otlp` or
`opentelemetry-prometheus` bridge. The `MetricsRsRecorder` adapter is
backend-agnostic — it writes to whichever exporter the `metrics` crate's
global recorder is pointing at.

## Custom recorder

If you already have a metrics backend that does not use the `metrics` crate
facade, implement `MetricsRecorder` directly:

```rust
use autumn_harvest::telemetry::{
    ActivityStatus, MetricsRecorder, WorkflowStatus,
};

struct MyRecorder { /* … */ }

impl MetricsRecorder for MyRecorder {
    fn record_workflow_started(&self, workflow_name: &str, queue: &str) {
        my_counter!("harvest.workflow.started", workflow = workflow_name, queue = queue);
    }
    // implement the other methods you care about; all have default no-ops
}
```

---

## Metric catalogue (ADR-0001 §7)

See [`docs/adr/0001-otel-trace-contract.md`](adr/0001-otel-trace-contract.md)
for the canonical, authoritative catalogue. The table below lists where each
metric is emitted in the source code.

| Metric | Instrument | Call site |
|--------|-----------|-----------|
| `harvest.workflow.started` | Counter | `worker.rs` — `process_workflow_task`, on first live invocation |
| `harvest.workflow.duration` | Histogram | `worker.rs` — `process_workflow_task`, on executor cycle completion |
| `harvest.activity.duration` | Histogram | `worker.rs` — `dispatch_activity_handler`, on activity completion (success or failure) |
| `harvest.activity.failed` | Counter | `worker.rs` — `dispatch_activity_handler`, on each failed attempt; richer labels than `harvest.activity.attempts` (`workflow.type`, `error.type`, `non_retryable`) |
| `harvest.activity.attempts` | Counter | `worker.rs` — `dispatch_activity_handler`, once per attempt for **both** outcomes; use for success-rate SLOs: `rate(attempts{outcome="completed"}[5m]) / rate(attempts[5m])` (issue #528) |
| `harvest.activity.retries` | Counter | `worker.rs` — `handle_activity_result`, once per retry actually scheduled (after the `schedule_to_close` deadline check); use for retry-storm detection (issue #528) |
| `harvest.timer.started` | Counter | `worker.rs` — `persist_timer_command`, when a durable timer is written |
| `harvest.queue.depth` | Gauge | `worker.rs` — `spawn_queue_depth_sampler`, periodic (5 s default). Aggregated **across all shards** of the worker's `ShardedDbPool` (summed per queue) so multi-shard backlog is fleet-wide, not default-shard-only (issue #522) |
| `harvest.queue.schedule_to_start` | Histogram | `worker.rs` — `dispatch_task`, recorded after the concurrency permit is acquired so it captures worker-local backpressure; skew-discounted (issue #501) |
| `harvest.queue.oldest_pending_age` | Gauge | `worker.rs` — `spawn_queue_depth_sampler`, alongside depth; excludes PAUSED executions, skew-discounted, periodic (5 s default) (issue #501). Aggregated across all shards as the **max** age per queue (the single oldest task fleet-wide) (issue #522) |
| `harvest.queue.dispatched` | Counter | `worker.rs` — `dispatch_task`, once per dispatched task; lets operators confirm the live per-queue dispatch split matches `WorkerConfig::queue_weights` (issue #515) |
| `harvest.dlq.entries` | Gauge | `worker.rs` — `spawn_dlq_depth_sampler`, periodic (5 s default) |
| `harvest.worker.slots_in_use` | Gauge | `worker.rs` — `spawn_worker_slot_sampler`, periodic (5 s default). Pure in-memory read of the workflow/activity dispatch `Semaphore`s against their configured maxima — no DB access (issue #531) |
| `harvest.worker.slots_available` | Gauge | `worker.rs` — `spawn_worker_slot_sampler`, alongside `slots_in_use`. Invariant: `slots_in_use + slots_available == configured_max` per `slot_type` within one sampler interval (issue #531) |
| `harvest.schedule.runs` | Counter | `scheduler.rs` — `tick_one_workflow_schedule` / DAG tick, on successful dispatch |
| `harvest.schedule.skipped` | Counter | `scheduler.rs` — `tick_one_workflow_schedule` / DAG tick, when a run is skipped |
| `harvest.retention.deleted` | Counter | `retention.rs` — `run_shard_tick`, per tick per shard |

### Label sets

| Metric | Labels |
|--------|--------|
| `harvest.workflow.started` | `workflow`, `queue` |
| `harvest.workflow.duration` | `workflow`, `queue`, `status` (`completed\|failed\|suspended\|continued_as_new`) |
| `harvest.activity.duration` | `activity`, `queue`, `status` (`completed\|failed`) |
| `harvest.activity.failed` | `activity`, `workflow.type`, `error.type`, `non_retryable` |
| `harvest.activity.attempts` | `activity`, `queue`, `outcome` (`completed\|failed`) |
| `harvest.activity.retries` | `activity`, `queue` |
| `harvest.timer.started` | _(none)_ |
| `harvest.queue.depth` | `queue` |
| `harvest.queue.schedule_to_start` | `queue` |
| `harvest.queue.oldest_pending_age` | `queue` |
| `harvest.dlq.entries` | `shard` |
| `harvest.worker.slots_in_use` | `slot_type` (`workflow\|activity`) |
| `harvest.worker.slots_available` | `slot_type` (`workflow\|activity`) |
| `harvest.schedule.runs` | `kind` (`workflow\|dag`), `name` |
| `harvest.schedule.skipped` | `kind`, `name`, `reason` (`paused\|max_active_runs_reached\|catchup_disabled`) |
| `harvest.retention.deleted` | `shard` |

**Cardinality rule:** `execution.id` is **never** a metric label. It is
span-only (see ADR-0001 §4). The `MetricsRecorder` API enforces this by
construction — no `record_*` method accepts an `ExecutionId`.

---

## Grafana dashboard example queries

```promql
# Activity failure rate
rate(harvest_activity_duration_count{status="failed"}[5m])

# Current queue backlog
harvest_queue_depth{queue="default"}

# p99 schedule-to-start latency per queue (canonical worker-capacity SLI, issue #501)
histogram_quantile(0.99, sum by (le, queue) (rate(harvest_queue_schedule_to_start_bucket[5m])))

# Age of the oldest unclaimed eligible task per queue (fast-detection gauge, issue #501)
harvest_queue_oldest_pending_age{queue="default"}

# DLQ depth per shard
harvest_dlq_entries{shard="0"}

# Worker dispatch-slot utilization per slot type (issue #531):
# "are my workers saturated?" — pairs with queue.depth to tell a worker
# bottleneck apart from a queue/DB bottleneck.
harvest_worker_slots_in_use / (harvest_worker_slots_in_use + harvest_worker_slots_available)

# Effective schedule run rate (runs - skips)
rate(harvest_schedule_runs_total[1h]) - rate(harvest_schedule_skipped_total[1h])
```
