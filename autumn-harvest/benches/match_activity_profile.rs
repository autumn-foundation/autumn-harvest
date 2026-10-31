//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `HistoryMatcher::match_activity`, the non-strict activity
//! matcher that ordinary workers use on every replayed decision.
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! `MATCH_ACTIVITY_PROFILE_SHAPE` picks the recorded history:
//!
//! - `fanout` (default): one decision schedules `N` activities, then they
//!   complete in scheduling order. Workflow code awaits them in order.
//! - `fanout_rev`: as `fanout`, but the completions arrive in reverse order.
//! - `sequential`: schedule, complete, schedule, complete, `N` times.
//! - `fanout_extsig`: as `fanout`, after one `ExternalSignalRequested` and
//!   `ExternalSignalDelivered` pair. The harness matches the signal after the
//!   fan-out, so the stash holds it for the whole fan-out (issue #2338).
//!
//! The harness replays the history through the public `HistoryMatcher::new`
//! and `match_activity` entry points, once per activity. Each activity carries
//! a realistic order record as input and output. The history is built once,
//! outside the measured loop. `MATCH_ACTIVITY_PROFILE_REPS` (default 1)
//! repeats the replay.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench match_activity_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="match_activity_profile") | .executable')
//! MATCH_ACTIVITY_PROFILE_N=1000 valgrind --tool=callgrind --branch-sim=no \
//!   --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! MATCH_ACTIVITY_PROFILE_N=1000 valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};
use autumn_harvest::types::{ActivityExecId, ExecutionId, ExternalSignalId, ExternalTarget};
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
        "items": [{ "sku": format!("sku-{}", i % 512), "qty": 1 + i % 5 }],
    })
}

fn scheduled(i: usize, id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: "charge".into(),
        input: payload(i),
        queue: "default".into(),
    }
}

fn completed(i: usize, id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityCompleted {
        activity_id: id,
        output: payload(i),
    }
}

/// The target of the external signal in the `fanout_extsig` shape.
fn signal_target() -> ExternalTarget {
    ExternalTarget::ExecutionId(ExecutionId::from_uuid(uuid::Uuid::from_u128(2338)))
}

fn history(shape: &str, n: usize) -> Vec<WorkflowEvent> {
    let ids: Vec<ActivityExecId> = (0..n).map(|_| ActivityExecId::new()).collect();
    let mut events = Vec::with_capacity(2 * n + 2);
    if shape == "fanout_extsig" {
        let signal_id = ExternalSignalId::new();
        events.push(WorkflowEvent::ExternalSignalRequested {
            signal_id,
            target: signal_target(),
            signal_name: "go".into(),
            payload: Value::Null,
            idempotency_key: None,
        });
        events.push(WorkflowEvent::ExternalSignalDelivered { signal_id });
    }
    match shape {
        "sequential" => {
            for (i, id) in ids.iter().enumerate() {
                events.push(scheduled(i, *id));
                events.push(completed(i, *id));
            }
        }
        "fanout" | "fanout_rev" | "fanout_extsig" => {
            events.extend(ids.iter().enumerate().map(|(i, id)| scheduled(i, *id)));
            if shape != "fanout_rev" {
                events.extend(ids.iter().enumerate().map(|(i, id)| completed(i, *id)));
            } else {
                events.extend(
                    ids.iter()
                        .enumerate()
                        .rev()
                        .map(|(i, id)| completed(i, *id)),
                );
            }
        }
        other => panic!("unknown MATCH_ACTIVITY_PROFILE_SHAPE {other:?}"),
    }
    events
}

fn main() {
    let n = env_usize("MATCH_ACTIVITY_PROFILE_N", 1000);
    let reps = env_usize("MATCH_ACTIVITY_PROFILE_REPS", 1);
    let shape = std::env::var("MATCH_ACTIVITY_PROFILE_SHAPE").unwrap_or_else(|_| "fanout".into());

    let mut hits = 0usize;
    for _ in 0..reps {
        let mut matcher = HistoryMatcher::new(history(&shape, n));
        for _ in 0..n {
            match matcher.match_activity("charge") {
                HistoryMatch::Matched { output } => {
                    std::hint::black_box(&output);
                    hits += 1;
                }
                other => panic!("expected Matched, got {other:?}"),
            }
        }
        if shape == "fanout_extsig" {
            let signal = matcher.match_external_signal(&signal_target(), "go");
            assert!(
                matches!(signal, HistoryMatch::Matched { .. }),
                "expected the stashed signal, got {signal:?}"
            );
        }
    }
    println!("match_activity_profile: shape={shape} n={n} reps={reps} matched={hits}");
}
