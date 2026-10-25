//! Deterministic (non-criterion) instruction-count profiling harness for the
//! timer fire scan (`HistoryMatcher::match_timer`) and the child terminal scan
//! (`HistoryMatcher::match_child_workflow`), issue #2338.
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! `TIMER_CHILD_PROFILE_SHAPE` picks the recorded history:
//!
//! - `timers` (default): one decision starts `N` timers. They fire in start
//!   order.
//! - `timers_rev`: as `timers`, but the timers fire in reverse order.
//! - `timers_seq`: start, fire, start, fire, `N` times.
//! - `children`: one decision starts `N` children. They complete in start
//!   order.
//! - `children_rev`: as `children`, but the children complete in reverse
//!   order.
//! - `children_seq`: start, complete, start, complete, `N` times.
//!
//! The harness replays the history through the public `HistoryMatcher::new`
//! and `match_timer` or `match_child_workflow` entry points, once per timer
//! or child. The history is built once, outside the measured loop.
//! `TIMER_CHILD_PROFILE_REPS` (default 1) repeats the replay.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench timer_child_fanout_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="timer_child_fanout_profile") | .executable')
//! TIMER_CHILD_PROFILE_SHAPE=timers TIMER_CHILD_PROFILE_N=1000 valgrind --tool=callgrind \
//!   --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! ```

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};
use autumn_harvest::types::{ExecutionId, TimerId};
use serde_json::{Value, json};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

fn timer_id(i: usize) -> TimerId {
    TimerId::new(format!("timer-{i:08}"))
}

fn started(i: usize) -> WorkflowEvent {
    WorkflowEvent::TimerStarted {
        timer_id: timer_id(i),
        duration_secs: 60,
    }
}

fn fired(i: usize) -> WorkflowEvent {
    WorkflowEvent::TimerFired {
        timer_id: timer_id(i),
    }
}

fn child_started(id: ExecutionId) -> WorkflowEvent {
    WorkflowEvent::ChildWorkflowStarted {
        child_id: id,
        workflow_name: "child".into(),
        input: Value::Null,
    }
}

fn child_completed(i: usize, id: ExecutionId) -> WorkflowEvent {
    WorkflowEvent::ChildWorkflowCompleted {
        child_id: id,
        output: json!({ "child": i }),
    }
}

/// The order of the fires or completions of a fan-out.
fn order(n: usize, reversed: bool) -> Vec<usize> {
    if reversed {
        (0..n).rev().collect()
    } else {
        (0..n).collect()
    }
}

fn history(shape: &str, n: usize) -> Vec<WorkflowEvent> {
    let children: Vec<ExecutionId> = (0..n).map(|_| ExecutionId::new()).collect();
    let mut events = Vec::with_capacity(2 * n);
    match shape {
        "timers_seq" => {
            for i in 0..n {
                events.push(started(i));
                events.push(fired(i));
            }
        }
        "timers" | "timers_rev" => {
            events.extend((0..n).map(started));
            events.extend(order(n, shape == "timers_rev").into_iter().map(fired));
        }
        "children_seq" => {
            for (i, id) in children.iter().enumerate() {
                events.push(child_started(*id));
                events.push(child_completed(i, *id));
            }
        }
        "children" | "children_rev" => {
            events.extend(children.iter().map(|id| child_started(*id)));
            events.extend(
                order(n, shape == "children_rev")
                    .into_iter()
                    .map(|i| child_completed(i, children[i])),
            );
        }
        other => panic!("unknown TIMER_CHILD_PROFILE_SHAPE {other:?}"),
    }
    events
}

fn main() {
    let n = env_usize("TIMER_CHILD_PROFILE_N", 1000);
    let reps = env_usize("TIMER_CHILD_PROFILE_REPS", 1);
    let shape = std::env::var("TIMER_CHILD_PROFILE_SHAPE").unwrap_or_else(|_| "timers".into());
    let timers = shape.starts_with("timers");

    let mut hits = 0usize;
    for _ in 0..reps {
        let mut matcher = HistoryMatcher::new(history(&shape, n));
        for i in 0..n {
            let result = if timers {
                matcher.match_timer(timer_id(i).as_str())
            } else {
                matcher.match_child_workflow("child", &Value::Null)
            };
            match result {
                HistoryMatch::Matched { output } => {
                    std::hint::black_box(&output);
                    hits += 1;
                }
                other => panic!("expected Matched, got {other:?}"),
            }
        }
    }
    println!("timer_child_fanout_profile: shape={shape} n={n} reps={reps} matched={hits}");
}
