//! Deterministic (non-criterion) instruction/allocation-count harness for
//! `ShapeCensus::observe` and `ShapeCensus::judge`.
//!
//! Wall-clock timing is not admissible on the shared vCPU. Run the binary
//! under `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! A corpus of one workflow type. It has `SC_SHAPES` distinct shapes (default
//! 300, at most 300). Shape `i` holds `i / 2 + 1` timer start/fire pairs and then
//! ends in `WorkflowCompleted` (even `i`) or `WorkflowFailed` (odd `i`). Each
//! shape thus has 3 to 302 events. A cubic skew picks the shape of each corpus
//! run, so a few shapes dominate. `SC_CORPUS` (default 20000) runs are observed.
//! Then `SC_JUDGE` (default 2000) runs are judged against the census.
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench shape_census_profile --no-run
//! valgrind --tool=callgrind --callgrind-out-file=cg.out <binary>
//! valgrind --tool=dhat --dhat-out-file=dhat.json <binary>
//! ```

use std::hint::black_box;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::shape_census::ShapeCensus;
use autumn_harvest::types::TimerId;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn build_shape(i: usize) -> Vec<WorkflowEvent> {
    let mut events = Vec::with_capacity(i + 3);
    for p in 0..=(i / 2) {
        let timer_id = TimerId::new(format!("t{p}"));
        events.push(WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 5,
        });
        events.push(WorkflowEvent::TimerFired { timer_id });
    }
    events.push(if i.is_multiple_of(2) {
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!(1),
        }
    } else {
        WorkflowEvent::WorkflowFailed {
            error: "boom".into(),
            error_type: None,
            details: None,
            non_retryable: Some(false),
        }
    });
    events
}

/// Pick a shape index with a cubic skew, from a fixed LCG.
fn pick(state: &mut u64, shapes: usize) -> usize {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    let u = (*state >> 11) as f64 / (1u64 << 53) as f64;
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let idx = (u * u * u * shapes as f64) as usize;
    idx.min(shapes - 1)
}

fn main() {
    let shapes = env_usize("SC_SHAPES", 300).clamp(1, 300);
    let corpus = env_usize("SC_CORPUS", 20_000);
    let judged = env_usize("SC_JUDGE", 2_000);
    let histories: Vec<Vec<WorkflowEvent>> = (0..shapes).map(build_shape).collect();
    let mut rng = 0x5eed_u64;
    let mut census = ShapeCensus::new();
    for _ in 0..corpus {
        black_box(census.observe(&histories[pick(&mut rng, shapes)]));
    }
    let mut seen = 0usize;
    for _ in 0..judged {
        let verdict = census.judge(&histories[pick(&mut rng, shapes)]);
        seen += verdict.seen + verdict.divergence.unwrap_or(0);
    }
    black_box((seen, census.distinct()));
    println!(
        "distinct={} total={} checksum={seen}",
        census.distinct(),
        census.total()
    );
}
