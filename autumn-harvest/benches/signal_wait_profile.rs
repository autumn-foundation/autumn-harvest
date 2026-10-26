//! Deterministic (non-criterion) instruction-count profiling harness for the
//! signal wait scan (`HistoryMatcher::match_signal`, issue #2349).
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
//! - `loop`: `N` steps of one activity. Each signal arrives before the
//!   completion of its activity. The activity scan stashes the signal, and
//!   the next wait takes it. Every signal has the name `go`, so the stash
//!   holds one entry at a time.
//! - `loop_named`: `loop`, with a name per signal.
//!
//! The harness replays through the public `HistoryMatcher::new`,
//! `match_activity` and `match_signal` entry points. The history is built
//! once per rep, inside the measured run. `SIGNAL_PROFILE_REPS` (default 1)
//! repeats the replay.
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

/// The signal name of step `i` of `shape`.
fn signal_name(shape: &str, i: usize) -> String {
    if shape == "loop" {
        "go".into()
    } else {
        name(i)
    }
}

fn scheduled(activity_id: ActivityExecId, i: usize) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id,
        name: "work".into(),
        input: json!({ "i": i }),
        queue: "default".into(),
    }
}

fn signal(shape: &str, i: usize) -> WorkflowEvent {
    WorkflowEvent::SignalReceived {
        signal_name: signal_name(shape, i),
        payload: json!({ "i": i }),
    }
}

fn history(shape: &str, n: usize) -> Vec<WorkflowEvent> {
    let mut events = Vec::with_capacity(3 * n);
    if shape == "loop" || shape == "loop_named" {
        for i in 0..n {
            let activity_id = ActivityExecId::new();
            events.push(scheduled(activity_id, i));
            events.push(signal(shape, i));
            events.push(WorkflowEvent::ActivityCompleted {
                activity_id,
                output: json!({ "i": i }),
            });
        }
        return events;
    }
    if shape == "after_fanout" {
        events.extend((0..n).map(|i| scheduled(ActivityExecId::new(), i)));
    }
    events.extend((0..n).map(|i| signal(shape, i)));
    events
}

fn expect_matched(result: HistoryMatch) {
    match result {
        HistoryMatch::Matched { output } => {
            std::hint::black_box::<&Value>(&output);
        }
        other => panic!("expected Matched, got {other:?}"),
    }
}

fn main() {
    let n = env_usize("SIGNAL_PROFILE_N", 1000);
    let reps = env_usize("SIGNAL_PROFILE_REPS", 1);
    let shape = std::env::var("SIGNAL_PROFILE_SHAPE").unwrap_or_else(|_| "fwd".into());
    let order: Vec<usize> = match shape.as_str() {
        "fwd" | "after_fanout" | "loop" | "loop_named" => (0..n).collect(),
        "rev" => (0..n).rev().collect(),
        other => panic!("unknown SIGNAL_PROFILE_SHAPE {other:?}"),
    };
    let steps = shape.starts_with("loop");

    let mut hits = 0usize;
    for _ in 0..reps {
        let mut matcher = HistoryMatcher::new(history(&shape, n));
        for &i in &order {
            if steps {
                expect_matched(matcher.match_activity("work"));
            }
            expect_matched(matcher.match_signal(&signal_name(&shape, i)));
            hits += 1;
        }
    }
    println!("signal_wait_profile: shape={shape} n={n} reps={reps} matched={hits}");
}
