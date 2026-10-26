//! Deterministic (non-criterion) instruction-count profiling harness for the
//! signal wait scan (`HistoryMatcher::match_signal`).
//!
//! Wall-clock timing is too noisy for this comparison. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! `SIGNAL_PROFILE_SHAPE` picks the recorded history. Each signal has its own
//! name `sig-<i>`, as in a workflow that waits on one approval per item:
//!
//! - `fwd` (default): `N` signals arrive. The workflow waits in arrival order.
//! - `rev`: `N` signals arrive. The workflow waits in reverse order.
//! - `after_fanout`: `N` activities start, then `N` signals arrive. The
//!   workflow waits for the signals in arrival order, with the activities
//!   still unclaimed.
//!
//! The harness replays through the public `HistoryMatcher::new` and
//! `match_signal` entry points. The history is built once per rep.
//! `SIGNAL_PROFILE_REPS` (default 1) repeats the replay.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench signal_wait_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="signal_wait_profile") | .executable')
//! SIGNAL_PROFILE_SHAPE=rev SIGNAL_PROFILE_N=1000 valgrind --tool=callgrind \
//!   --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! ```

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};
use autumn_harvest::types::ActivityExecId;
use serde_json::{Value, json};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

fn name(i: usize) -> String {
    format!("sig-{i:08}")
}

fn history(shape: &str, n: usize) -> Vec<WorkflowEvent> {
    let mut events = Vec::with_capacity(2 * n);
    if shape == "after_fanout" {
        events.extend((0..n).map(|i| WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "work".into(),
            input: json!({ "i": i }),
            queue: "default".into(),
        }));
    }
    events.extend((0..n).map(|i| WorkflowEvent::SignalReceived {
        signal_name: name(i),
        payload: json!({ "i": i }),
    }));
    events
}

fn main() {
    let n = env_usize("SIGNAL_PROFILE_N", 1000);
    let reps = env_usize("SIGNAL_PROFILE_REPS", 1);
    let shape = std::env::var("SIGNAL_PROFILE_SHAPE").unwrap_or_else(|_| "fwd".into());
    let order: Vec<usize> = match shape.as_str() {
        "fwd" | "after_fanout" => (0..n).collect(),
        "rev" => (0..n).rev().collect(),
        other => panic!("unknown SIGNAL_PROFILE_SHAPE {other:?}"),
    };

    let mut hits = 0usize;
    for _ in 0..reps {
        let mut matcher = HistoryMatcher::new(history(&shape, n));
        for &i in &order {
            match matcher.match_signal(&name(i)) {
                HistoryMatch::Matched { output } => {
                    std::hint::black_box::<&Value>(&output);
                    hits += 1;
                }
                other => panic!("expected Matched, got {other:?}"),
            }
        }
    }
    println!("signal_wait_profile: shape={shape} n={n} reps={reps} matched={hits}");
}
