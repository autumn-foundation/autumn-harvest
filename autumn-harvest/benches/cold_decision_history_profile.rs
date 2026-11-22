//! Deterministic (non-criterion) instruction-count profiling harness for the
//! history that a worker gives to one cold decision.
//!
//! A cold decision replays the whole history, and the worker needs the
//! history again after the replay. A worker that gives the context a history
//! for good must copy it first. A worker that lends it gets it back. This
//! harness measures both, for a decision that stays resident and for one
//! that ends the run. See `docs/performance-cold-decision-lent-history.md`.
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind`.
//!
//! # Workload
//!
//! The history holds `N` completed activities of the sequential workflow of
//! `replay_profile_support.rs`. Each decision replays all of them.
//!
//! - `COLD_DECISION_PROFILE_MODE`: `owned` (default) copies the history and
//!   gives the copy away, as the worker did. `lent` lends the history.
//! - `COLD_DECISION_PROFILE_END`: `park` (default) asks for one more
//!   activity, so the decision suspends and stays resident. `complete` ends
//!   the run.
//! - `COLD_DECISION_PROFILE_N` (default 100) sets the activity count.
//! - `COLD_DECISION_PROFILE_REPS` (default 1) repeats the decision. The
//!   difference of two rep counts is the cost of the decisions alone.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench cold_decision_history_profile --no-run
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no "$BIN"
//! ```

#[path = "replay_profile_support.rs"]
mod support;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::WorkflowOutcome;
use autumn_harvest::resident;
use autumn_harvest::types::ExecutionId;
use serde_json::Value;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

fn env_choice(key: &str, default: &str, choices: [&str; 2]) -> String {
    let value = std::env::var(key).unwrap_or_else(|_| default.to_string());
    assert!(
        choices.contains(&value.as_str()),
        "{key} must be one of {choices:?}, got {value:?}"
    );
    value
}

/// One cold decision over `history`. Returns the history for the next one.
fn decide(
    rt: &tokio::runtime::Runtime,
    lend: bool,
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    input: &Value,
    park: bool,
) -> Vec<WorkflowEvent> {
    let (outcome, resident, history) = if lend {
        let (outcome, resident, returned) = rt.block_on(resident::start_lending(
            exec_id,
            history,
            support::sequential_workflow,
            input.clone(),
        ));
        (outcome, resident, returned.expect("the history comes back"))
    } else {
        // The worker kept the history and gave the context a copy.
        let (outcome, resident) = rt.block_on(resident::start(
            exec_id,
            history.clone(),
            support::sequential_workflow,
            input.clone(),
        ));
        (outcome, resident, history)
    };
    if park {
        assert!(matches!(outcome, WorkflowOutcome::Suspended { .. }));
        assert!(resident.is_some(), "the suspension must stay resident");
    } else {
        assert!(matches!(outcome, WorkflowOutcome::Completed { .. }));
        assert!(resident.is_none());
    }
    // The worker drops the resident workflow of an evicted run the same way.
    drop(std::hint::black_box(resident));
    history
}

fn main() {
    let n = env_usize("COLD_DECISION_PROFILE_N", 100);
    let reps = env_usize("COLD_DECISION_PROFILE_REPS", 1);
    let mode = env_choice("COLD_DECISION_PROFILE_MODE", "owned", ["owned", "lent"]);
    let end = env_choice("COLD_DECISION_PROFILE_END", "park", ["park", "complete"]);
    let park = end == "park";

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    let (exec_id, mut history) = support::build_history(n);
    // One more activity than the history holds makes the decision suspend.
    let input = Value::from(u64::try_from(n).expect("n fits") + u64::from(park));

    for _ in 0..reps {
        history = decide(&rt, mode == "lent", exec_id, history, &input, park);
    }
    assert_eq!(
        history.len(),
        n * 2 + 1,
        "the worker still holds the history"
    );

    println!("cold_decision_history_profile: mode={mode} end={end} n={n} reps={reps}");
}
