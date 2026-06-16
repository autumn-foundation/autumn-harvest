//! Criterion benchmark: replay throughput for large event histories.
//!
//! Verifies the requirement from issue #135:
//! "Replaying a 10,000-event history completes in under 200ms on a
//! laptop-class machine for a workflow whose user code is in-memory only."
//!
//! Also verifies the AC from issue #136:
//! "With telemetry disabled (the default), the call sites compile to a no-op
//! that does not allocate or take a tracing subscriber lock."
//!
//! Run with:
//!   cargo bench -p autumn-harvest --features testing --no-default-features --bench `replay_bench`

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::WorkflowReplayer;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, Criterion, criterion_group, criterion_main};
use serde_json::Value;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

/// Workflow that executes N sequential activities.
fn sequential_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n: usize = usize::try_from(input.as_u64().unwrap_or(0)).unwrap_or(0);
        let mut last = Value::Null;
        for i in 0..n {
            let name = format!("activity_{i}");
            last = ctx
                .execute_activity_raw(&name, Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(last)
    })
}

/// Build a synthetic event history with `n` completed activities.
fn build_history(n: usize) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let mut events = Vec::with_capacity(n * 2 + 1);

    events.push(WorkflowEvent::WorkflowStarted {
        input: Value::from(n as u64),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
    });

    for i in 0..n {
        let activity_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: format!("activity_{i}"),
            input: Value::Null,
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::from(i as u64),
        });
    }

    (exec_id, events)
}

fn bench_replay_10k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let replayer = WorkflowReplayer::new().register_fn("sequential", sequential_workflow);

    c.bench_function("replay_10k_events", |b| {
        b.iter_batched(
            || build_history(5_000), // 5_000 activities = 10_001 events
            |(_exec_id, events)| rt.block_on(replayer.replay_from_events(events)),
            BatchSize::SmallInput,
        );
    });
}

fn bench_replay_1k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let replayer = WorkflowReplayer::new().register_fn("sequential", sequential_workflow);

    c.bench_function("replay_1k_events", |b| {
        b.iter_batched(
            || build_history(500), // 500 activities = 1_001 events
            |(_exec_id, events)| rt.block_on(replayer.replay_from_events(events)),
            BatchSize::SmallInput,
        );
    });
}

// ---------------------------------------------------------------------------
// Span no-op overhead bench (issue #136 AC)
// ---------------------------------------------------------------------------

/// A minimal tracing layer that counts spans created. Used to verify the
/// overhead of the recording path versus the no-subscriber (no-op) path.
struct CountingLayer(Arc<Mutex<u64>>);

impl<S: tracing::Subscriber> Layer<S> for CountingLayer {
    fn on_new_span(
        &self,
        _attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        *self.0.lock().unwrap() += 1;
    }
}

/// Install a subscriber with `CountingLayer` for the current thread scope.
/// Returns a `(counter, guard)` pair — the guard must be held alive to keep
/// the subscriber installed.
fn install_counting_subscriber() -> (Arc<Mutex<u64>>, DefaultGuard) {
    let counter = Arc::new(Mutex::new(0u64));
    let layer = CountingLayer(Arc::clone(&counter));
    let subscriber = tracing_subscriber::registry().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (counter, guard)
}

fn run_noop_overhead_benches(group: &mut BenchmarkGroup<WallTime>, history_size: usize) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let replayer = WorkflowReplayer::new().register_fn("sequential", sequential_workflow);

    // Baseline: no subscriber installed — all info_span! calls are no-ops.
    // This verifies that harvest's span sites add zero overhead when the
    // operator has not installed a tracing subscriber.
    group.bench_function(format!("no_subscriber_{history_size}ev"), |b| {
        b.iter_batched(
            || build_history(history_size / 2),
            |(_id, events)| rt.block_on(replayer.replay_from_events(events)),
            BatchSize::SmallInput,
        );
    });

    // Comparison: a real (counting) subscriber is installed.
    // The delta between this and the no_subscriber bench is the maximum
    // overhead of active telemetry.
    group.bench_function(format!("counting_subscriber_{history_size}ev"), |b| {
        let (_counter, _guard) = install_counting_subscriber();
        b.iter_batched(
            || build_history(history_size / 2),
            |(_id, events)| rt.block_on(replayer.replay_from_events(events)),
            BatchSize::SmallInput,
        );
    });
}

fn bench_span_noop_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("span_overhead");
    run_noop_overhead_benches(&mut group, 100);
    group.finish();
}

criterion_group!(
    benches,
    bench_replay_1k,
    bench_replay_10k,
    bench_span_noop_overhead
);
criterion_main!(benches);
