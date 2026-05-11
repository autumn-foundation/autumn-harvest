//! Benchmark: cost of metrics hot-path calls when the default `NoOpMetrics`
//! recorder is in use.
//!
//! The acceptance criterion from ADR-0001 / issue #138 is that sampling cost
//! on the hot paths is *zero* (or nanosecond-level) when no custom recorder
//! is configured. These benchmarks measure:
//!
//! - `noop/workflow_started` — counter on the workflow dispatch entry point
//! - `noop/workflow_completed` — histogram on the workflow exit point
//! - `noop/activity_completed` — histogram on the activity exit point
//! - `noop/dlq_entries` — gauge sampled by the DLQ background ticker
//!
//! Criterion is configured via `[[bench]]` in `Cargo.toml`. Run with:
//!
//! ```sh
//! cargo bench -p autumn-harvest --bench metrics_noop
//! ```

use autumn_harvest::telemetry::{ActivityStatus, MetricsRecorder, NoOpMetrics, WorkflowStatus};
use criterion::{Criterion, criterion_group, criterion_main};

fn bench_noop_workflow_started(c: &mut Criterion) {
    let rec = NoOpMetrics;
    c.bench_function("noop/workflow_started", |b| {
        b.iter(|| rec.record_workflow_started("onboarding", "default"));
    });
}

fn bench_noop_workflow_completed(c: &mut Criterion) {
    let rec = NoOpMetrics;
    c.bench_function("noop/workflow_completed", |b| {
        b.iter(|| {
            rec.record_workflow_completed("onboarding", "default", 1.0, WorkflowStatus::Completed);
        });
    });
}

fn bench_noop_activity_completed(c: &mut Criterion) {
    let rec = NoOpMetrics;
    c.bench_function("noop/activity_completed", |b| {
        b.iter(|| {
            rec.record_activity_completed("send_email", "default", 0.5, ActivityStatus::Completed);
        });
    });
}

fn bench_noop_dlq_entries(c: &mut Criterion) {
    let rec = NoOpMetrics;
    c.bench_function("noop/dlq_entries", |b| {
        b.iter(|| rec.record_dlq_entries(0, 42));
    });
}

criterion_group!(
    benches,
    bench_noop_workflow_started,
    bench_noop_workflow_completed,
    bench_noop_activity_completed,
    bench_noop_dlq_entries,
);
criterion_main!(benches);
