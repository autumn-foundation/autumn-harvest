//! Deterministic (non-criterion) instruction-count profiling harness for the
//! external stash of `HistoryMatcher`, issue #2346.
//!
//! Wall-clock timing is too noisy for this comparison. Drive the binary under
//! `valgrind --tool=callgrind`.
//!
//! # Workload
//!
//! `EXTERNAL_STASH_PROFILE_SHAPE` picks the recorded history:
//!
//! - `loop` (default): one external await, then `N` loop steps. Each step is
//!   an activity, then an external signal. The await terminal comes last, and
//!   the harness matches the await last. So the stash holds the await for the
//!   whole replay.
//! - `loop_seq`: as `loop`, with no await. The stash is empty between the
//!   loop steps.
//! - `fanout`: one decision requests `N` external signals, then the
//!   deliveries arrive. The harness matches the signals in request order.
//!
//! The harness replays the history through the public `HistoryMatcher::new`,
//! `match_activity`, `match_external_signal` and `match_external_await` entry
//! points. The history is built once, outside the measured loop.
//! `EXTERNAL_STASH_PROFILE_REPS` (default 1) repeats the replay.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench external_stash_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="external_stash_profile") | .executable')
//! EXTERNAL_STASH_PROFILE_SHAPE=loop EXTERNAL_STASH_PROFILE_N=1000 valgrind --tool=callgrind \
//!   --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! ```

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};
use autumn_harvest::types::{
    ActivityExecId, ExecutionId, ExternalAwaitId, ExternalSignalId, ExternalTarget,
};
use serde_json::{Value, json};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

fn payload(i: usize) -> Value {
    json!({
        "order_id": format!("order-{i:08}"),
        "customer_id": i % 4096,
        "amount_cents": (i as u64 * 137) % 1_000_000,
        "currency": "USD",
    })
}

/// The execution that external call `i` targets.
const fn callee(i: usize) -> ExecutionId {
    ExecutionId::from_uuid(uuid::Uuid::from_u128(i as u128 + 1))
}

/// The execution that the `loop` shape awaits.
const fn awaited() -> ExecutionId {
    ExecutionId::from_uuid(uuid::Uuid::from_u128(2346))
}

fn signal_requested(i: usize, signal_id: ExternalSignalId) -> WorkflowEvent {
    WorkflowEvent::ExternalSignalRequested {
        signal_id,
        target: ExternalTarget::ExecutionId(callee(i)),
        signal_name: "go".into(),
        payload: payload(i),
        idempotency_key: None,
    }
}

fn history(shape: &str, n: usize) -> Vec<WorkflowEvent> {
    let signals: Vec<ExternalSignalId> = (0..n).map(|_| ExternalSignalId::new()).collect();
    let mut events = Vec::with_capacity(4 * n + 2);
    match shape {
        "loop" | "loop_seq" => {
            let await_id = ExternalAwaitId::new();
            if shape == "loop" {
                events.push(WorkflowEvent::ExternalAwaitRequested {
                    await_id,
                    target: awaited(),
                });
            }
            for (i, signal_id) in signals.iter().enumerate() {
                let activity_id = ActivityExecId::new();
                events.push(WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name: "charge".into(),
                    input: payload(i),
                    queue: "default".into(),
                });
                events.push(WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output: payload(i),
                });
                events.push(signal_requested(i, *signal_id));
                events.push(WorkflowEvent::ExternalSignalDelivered {
                    signal_id: *signal_id,
                });
            }
            if shape == "loop" {
                events.push(WorkflowEvent::ExternalAwaitResolved {
                    await_id,
                    output: Value::Null,
                });
            }
        }
        "fanout" => {
            events.extend(
                signals
                    .iter()
                    .enumerate()
                    .map(|(i, id)| signal_requested(i, *id)),
            );
            events.extend(
                signals
                    .iter()
                    .map(|id| WorkflowEvent::ExternalSignalDelivered { signal_id: *id }),
            );
        }
        other => panic!("unknown EXTERNAL_STASH_PROFILE_SHAPE {other:?}"),
    }
    events
}

fn expect_matched(result: HistoryMatch) {
    match result {
        HistoryMatch::Matched { output } => {
            std::hint::black_box(&output);
        }
        other => panic!("expected Matched, got {other:?}"),
    }
}

fn main() {
    let n = env_usize("EXTERNAL_STASH_PROFILE_N", 1000);
    let reps = env_usize("EXTERNAL_STASH_PROFILE_REPS", 1);
    let shape = std::env::var("EXTERNAL_STASH_PROFILE_SHAPE").unwrap_or_else(|_| "loop".into());

    for _ in 0..reps {
        let mut matcher = HistoryMatcher::new(history(&shape, n));
        for i in 0..n {
            if shape != "fanout" {
                expect_matched(matcher.match_activity("charge"));
            }
            let target = ExternalTarget::ExecutionId(callee(i));
            expect_matched(matcher.match_external_signal(&target, "go"));
        }
        if shape == "loop" {
            expect_matched(matcher.match_external_await(awaited()));
        }
        assert!(!matcher.is_replaying(), "the replay left history unmatched");
    }
    println!("external_stash_profile: shape={shape} n={n} reps={reps}");
}
