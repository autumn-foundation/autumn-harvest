//! Deterministic (non-criterion) instruction-count profiling harness for the
//! recorded-update lookup (`HistoryMatcher::match_update`), which
//! `WorkflowContext::execute_admitted_update` calls once per admitted update.
//!
//! Drive the binary under `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! A long-lived workflow that serves `N` updates. Each update is admitted,
//! runs one activity, and completes (`UpdateAdmitted`, `ActivityScheduled`,
//! `ActivityCompleted`, `UpdateCompleted`). Replay matches the activity, then
//! looks up the update, once per update, through the public `HistoryMatcher`
//! entry points. `UPDATE_PROFILE_REPS` (default 1) repeats the replay.
//! `UPDATE_PROFILE_ONLY_ACTIVITY=1` skips the lookup, to price the rest of
//! the replay.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench update_lookup_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="update_lookup_profile") | .executable')
//! UPDATE_PROFILE_N=1000 valgrind --tool=callgrind --branch-sim=no \
//!   --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! ```

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};
use autumn_harvest::types::{ActivityExecId, UpdateId};
use serde_json::json;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

fn history(n: usize) -> (Vec<WorkflowEvent>, Vec<UpdateId>) {
    let mut events = Vec::with_capacity(4 * n);
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let update_id = UpdateId::new();
        let activity_id = ActivityExecId::new();
        ids.push(update_id);
        events.push(WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "bump".into(),
            input: json!({ "i": i }),
            timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        });
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "work".into(),
            input: json!({ "i": i }),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: json!({ "i": i }),
        });
        events.push(WorkflowEvent::UpdateCompleted {
            update_id,
            output: json!({ "i": i }),
        });
    }
    (events, ids)
}

fn main() {
    let n = env_usize("UPDATE_PROFILE_N", 1000);
    let reps = env_usize("UPDATE_PROFILE_REPS", 1);
    let only_activity = env_usize("UPDATE_PROFILE_ONLY_ACTIVITY", 0) == 1;
    let mut hits = 0usize;
    for _ in 0..reps {
        let (events, ids) = history(n);
        let mut matcher = HistoryMatcher::new(events);
        for id in ids {
            assert!(matches!(
                matcher.match_activity("work"),
                HistoryMatch::Matched { .. }
            ));
            if !only_activity {
                match matcher.match_update(id) {
                    HistoryMatch::Matched { output } => {
                        std::hint::black_box(&output);
                    }
                    other => panic!("expected Matched, got {other:?}"),
                }
            }
            hits += 1;
        }
    }
    println!("update_lookup_profile: n={n} reps={reps} matched={hits}");
}
