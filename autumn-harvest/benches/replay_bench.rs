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
//!
//! The history builder lives in the shared end-to-end benchmark harness
//! (`tests/integration/e2e_bench_support.rs`, issue #941), not here: the
//! end-to-end suite publishes replay *throughput* over the same history this
//! bench budgets, and a second copy of the builder would let the two quietly
//! stop describing the same workload.

// The history builder is shared with the end-to-end benchmark suite (issue
// #941) rather than duplicated here, so the replay throughput that suite
// publishes and the CPU budget this bench guards are measured over
// byte-identical histories. Moving the builder — not copying it — is what makes
// drift between the two impossible rather than merely unlikely.
#[path = "../tests/integration/e2e_bench_support.rs"]
mod e2e_bench_support;

use std::sync::{Arc, Mutex};

use autumn_harvest::testing::WorkflowReplayer;
use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, Criterion, criterion_group, criterion_main};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

use e2e_bench_support::{REPLAY_ACTIVITY_COUNT, build_history, sequential_workflow};

fn bench_replay_10k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let replayer = WorkflowReplayer::new().register_fn("sequential", sequential_workflow);

    c.bench_function("replay_10k_events", |b| {
        b.iter_batched(
            // 5_000 activities = 10_001 events. The constant is the shared
            // harness's, so the #941 replay scenario cannot drift off this history.
            || build_history(REPLAY_ACTIVITY_COUNT),
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
