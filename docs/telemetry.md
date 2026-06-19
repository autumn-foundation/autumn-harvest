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
use std::sync::Arc;

// 1. Install the Prometheus exporter (do this once at process start).
metrics_exporter_prometheus::PrometheusBuilder::new()
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
| `harvest.activity.duration` | Histogram | `worker.rs` — `dispatch_activity_handler`, on activity completion |
| `harvest.timer.started` | Counter | `worker.rs` — `persist_timer_command`, when a durable timer is written |
| `harvest.queue.depth` | Gauge | `worker.rs` — `spawn_queue_depth_sampler`, periodic (5 s default) |
| `harvest.queue.schedule_to_start` | Histogram | `worker.rs` — claim path (`poll_once`), recorded at claim time (issue #501) |
| `harvest.queue.oldest_pending_age` | Gauge | `worker.rs` — `spawn_queue_depth_sampler`, alongside depth, periodic (5 s default) (issue #501) |
| `harvest.dlq.entries` | Gauge | `worker.rs` — `spawn_dlq_depth_sampler`, periodic (5 s default) |
| `harvest.schedule.runs` | Counter | `scheduler.rs` — `tick_one_workflow_schedule` / DAG tick, on successful dispatch |
| `harvest.schedule.skipped` | Counter | `scheduler.rs` — `tick_one_workflow_schedule` / DAG tick, when a run is skipped |
| `harvest.retention.deleted` | Counter | `retention.rs` — `run_shard_tick`, per tick per shard |

### Label sets

| Metric | Labels |
|--------|--------|
| `harvest.workflow.started` | `workflow`, `queue` |
| `harvest.workflow.duration` | `workflow`, `queue`, `status` (`completed\|failed\|suspended\|continued_as_new`) |
| `harvest.activity.duration` | `activity`, `queue`, `status` (`completed\|failed`) |
| `harvest.timer.started` | _(none)_ |
| `harvest.queue.depth` | `queue` |
| `harvest.queue.schedule_to_start` | `queue` |
| `harvest.queue.oldest_pending_age` | `queue` |
| `harvest.dlq.entries` | `shard` |
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

# Effective schedule run rate (runs - skips)
rate(harvest_schedule_runs_total[1h]) - rate(harvest_schedule_skipped_total[1h])
```
