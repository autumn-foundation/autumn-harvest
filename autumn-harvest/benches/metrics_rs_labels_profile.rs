//! Non-criterion instruction/allocation-count profiling harness for the
//! label-building cost of [`MetricsRsRecorder`] on the per-task hot path.
//!
//! Every activity attempt records four `metrics`-crate samples through the
//! adapter: `harvest.queue.dispatched`, `harvest.queue.schedule_to_start`,
//! `harvest.activity.attempts` and `harvest.activity.duration`. Each sample
//! carries the activity name, queue and build id as label values. This
//! harness drives exactly that set of calls, so its cost per rep is the
//! adapter's cost per activity attempt.
//!
//! The installed recorder returns no-op handles. The measured cost is
//! therefore the adapter's own work (label values, the label `Vec`, the
//! `Key`), not an exporter's registry lookup.
//!
//! Wall-clock timing is unreliable on a shared-vCPU machine, so this binary
//! is not measured with criterion. Run it under
//! `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts), like the other
//! `*_profile.rs` harnesses. See `docs/performance-metrics-rs-labels.md`.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest --features metrics-rs \
//!   --bench metrics_rs_labels_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable'
//!
//! valgrind --tool=dhat --dhat-out-file=dhat.out <path>
//! valgrind --tool=callgrind --callgrind-out-file=callgrind.out <path>
//! ```
//!
//! `METRICS_LABELS_PROFILE_REPS` (default 10000) sets the number of
//! simulated activity attempts.

use std::hint::black_box;

use autumn_harvest::metrics_rs_adapter::MetricsRsRecorder;
use autumn_harvest::telemetry::{ActivityStatus, MetricsRecorder};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

/// A recorder that hands out no-op handles, so only the adapter is measured.
struct NoopHandles;

impl Recorder for NoopHandles {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        black_box(key);
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        black_box(key);
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        black_box(key);
        Histogram::noop()
    }
}

/// Registered activity names. A real worker cycles through a small fixed set.
const ACTIVITIES: [&str; 4] = ["send_email", "charge_card", "reserve_stock", "ship_order"];

fn main() {
    let reps: usize = std::env::var("METRICS_LABELS_PROFILE_REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    let recorder = NoopHandles;
    metrics::with_local_recorder(&recorder, || {
        let rec = MetricsRsRecorder;
        for i in 0..reps {
            let activity = black_box(ACTIVITIES[i % ACTIVITIES.len()]);
            let queue = black_box("default");
            let build_id = black_box("build-2026.10.18");
            rec.record_task_dispatched(queue);
            rec.record_schedule_to_start(queue, 0.002);
            rec.record_activity_attempt_for_build(
                activity,
                queue,
                build_id,
                ActivityStatus::Completed,
            );
            rec.record_activity_completed_for_build(
                activity,
                queue,
                build_id,
                0.05,
                ActivityStatus::Completed,
                None,
            );
        }
    });
    println!("metrics_rs_labels_profile: {reps} simulated activity attempts");
}
