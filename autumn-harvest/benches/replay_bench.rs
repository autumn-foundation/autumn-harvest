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
// The shared harness's `db` section reaches the percentile/redaction helpers in
// `claim_bench_support` through the crate root, and `db` is a DEFAULT feature --
// so this module must be declared here too, or a plain
// `cargo bench --bench replay_bench` fails to resolve it. It carries
// `#![allow(dead_code)]`, so the only cost is compile time.
#[path = "../tests/integration/claim_bench_support.rs"]
mod claim_bench_support;
#[path = "../tests/integration/e2e_bench_support.rs"]
mod e2e_bench_support;

use std::sync::{Arc, Mutex};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::context::WorkflowCommand;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::resident::{self, ResidentWorkflow};
use autumn_harvest::testing::WorkflowReplayer;
use autumn_harvest::types::ExecutionId;
use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
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
// Decision cost against history length (issue #1798)
// ---------------------------------------------------------------------------

/// History lengths, in events, that the decision groups measure.
const DECISION_COST_EVENTS: [usize; 3] = [1_000, 5_000, 10_000];

/// Builds the history and input of one decision.
///
/// With `extra = 0` the input matches the history, so the decision replays
/// every event and completes. With `extra = 1` the workflow then schedules
/// one more activity and suspends, which is a live decision of a long run.
fn decision_history(
    activities: usize,
    extra: u64,
) -> (ExecutionId, Vec<WorkflowEvent>, serde_json::Value) {
    let (exec_id, mut events) = build_history(activities);
    let input = serde_json::Value::from(activities as u64 + extra);
    if let Some(WorkflowEvent::WorkflowStarted { input: started, .. }) = events.first_mut() {
        started.clone_from(&input);
    }
    (exec_id, events, input)
}

/// Registers one decision bench per history length in `group`.
fn bench_decisions(
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &tokio::runtime::Runtime,
    extra: u64,
) {
    for events in DECISION_COST_EVENTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(events),
            &events,
            |b, &events| {
                b.iter_batched(
                    || decision_history(events / 2, extra),
                    |(exec_id, history, input)| {
                        rt.block_on(run_workflow(exec_id, history, sequential_workflow, input))
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

/// Measures the cost of one decision at 1k, 5k and 10k events.
///
/// The bench calls `executor::run_workflow`, the entry point a worker uses
/// for each decision. Each decision replays the workflow from the top.
///
/// - `decision_cost` measures the replay work. Its cost grows linearly with
///   history length, so a run of n decisions costs O(n²) in total.
/// - `decision_wall` measures a decision that suspends. The executor waits
///   for a fixed 100 ms suspension timeout (issue #1797), so this group
///   reads about max(100 ms, replay). It shows that floor, not the slope.
///
/// Resident workflow state (issue #1798, step 2) would make the replay work
/// of a warm decision roughly constant. The 100 ms floor stays until issue
/// #1797 lands.
fn bench_decision_cost(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Each shape must produce the outcome that its group claims to measure,
    // at every size. A replay that overran the suspension timeout would
    // otherwise time a cut-off run.
    for events in DECISION_COST_EVENTS {
        let (exec_id, history, input) = decision_history(events / 2, 0);
        let done = rt.block_on(run_workflow(exec_id, history, sequential_workflow, input));
        assert!(
            matches!(done, WorkflowOutcome::Completed { .. }),
            "a full-history decision at {events} events must complete: {done:?}"
        );
        let (exec_id, history, input) = decision_history(events / 2, 1);
        let live = rt.block_on(run_workflow(exec_id, history, sequential_workflow, input));
        assert!(
            matches!(live, WorkflowOutcome::Suspended { .. }),
            "a live decision at {events} events must suspend: {live:?}"
        );
    }

    let mut group = c.benchmark_group("decision_cost");
    bench_decisions(&mut group, &rt, 0);
    group.finish();

    // Each sample waits about 100 ms, so keep the sample count small.
    let mut group = c.benchmark_group("decision_wall");
    group.sample_size(10);
    bench_decisions(&mut group, &rt, 1);
    group.finish();
}

/// A resident workflow at `events` history events, and its next delta.
///
/// The cold decision replays the full history and schedules one more
/// activity. The delta is that activity's `ActivityScheduled` and
/// `ActivityCompleted`. Resuming with it schedules the next activity.
fn warm_decision(
    rt: &tokio::runtime::Runtime,
    events: usize,
) -> (ResidentWorkflow, Vec<WorkflowEvent>) {
    let (exec_id, history, input) = decision_history(events / 2, 2);
    let (outcome, resident) = rt.block_on(resident::start(
        exec_id,
        history,
        sequential_workflow,
        input,
    ));
    let WorkflowOutcome::Suspended { commands } = outcome else {
        panic!("a live decision at {events} events must suspend: {outcome:?}");
    };
    let delta = commands
        .iter()
        .find_map(|cmd| match cmd {
            WorkflowCommand::ScheduleActivity {
                activity_id,
                name,
                input,
                queue,
                ..
            } => Some(vec![
                WorkflowEvent::ActivityScheduled {
                    activity_id: *activity_id,
                    name: name.clone(),
                    input: input.clone(),
                    queue: queue.clone(),
                },
                WorkflowEvent::ActivityCompleted {
                    activity_id: *activity_id,
                    output: serde_json::Value::Null,
                },
            ]),
            _ => None,
        })
        .expect("the decision schedules an activity");
    let resident = resident.expect("an activity suspension stays resident");
    (resident, delta)
}

/// Measures one warm decision at 1k, 5k and 10k events (issue #1798).
///
/// A warm decision resumes the resident workflow with one new result. It
/// does not replay history, so its cost must stay roughly constant across
/// the three sizes. Compare with `decision_cost`, which grows linearly.
fn bench_decision_cost_warm(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Each size must resume and suspend again, or the group times a decline.
    for events in DECISION_COST_EVENTS {
        let (resident, delta) = warm_decision(&rt, events);
        let resumed = rt.block_on(resident.resume(&delta));
        assert!(
            matches!(
                &resumed,
                Ok((WorkflowOutcome::Suspended { .. }, Some(_)))
            ),
            "a warm decision at {events} events must resume and suspend: {resumed:?}"
        );
    }

    let mut group = c.benchmark_group("decision_cost_warm");
    for events in DECISION_COST_EVENTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(events),
            &events,
            |b, &events| {
                b.iter_batched(
                    || warm_decision(&rt, events),
                    |(resident, delta)| rt.block_on(resident.resume(&delta)),
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
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
    bench_decision_cost,
    bench_decision_cost_warm,
    bench_span_noop_overhead
);
criterion_main!(benches);
