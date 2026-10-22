//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `debugger::diff_traces`, the engine behind
//! `harvest debug diff <LEFT> <RIGHT>` (the CI gate that compares two
//! recorded histories). Wall-clock timing is not admissible on this
//! shared-vCPU machine, so drive the binary under `valgrind`.
//!
//! # Workload
//!
//! Two independent recordings of one workflow, as `run_diff` loads them from
//! two `HistorySnapshot` files. Each history is the issue #135 shape from
//! `replay_profile_support.rs`: `DBG_DIFF_N` activities (default `5_000`,
//! `10_001` events), each with a realistic JSON payload. The activity ids
//! differ between the two recordings, so the diff must normalize them and
//! walk every step. That is the passing-gate case. The measured region is the
//! whole `run_diff` pipeline minus file I/O and rendering: the redaction probe,
//! the `HistorySnapshot` parse, two `ReplayTrace::from_history` calls and
//! `diff_traces`. History generation and JSON serialization are setup.
//! Read each stage's inclusive cost from callgrind.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features debugger \
//!   --bench debugger_diff_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.executable != null) | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --inclusive=yes --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

#[allow(dead_code)]
#[path = "replay_profile_support.rs"]
mod support;

use autumn_harvest::debugger::{ReplayTrace, diff_traces, is_redacted_export};
use autumn_harvest::testing::HistorySnapshot;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn snapshot_json(n: usize) -> String {
    let (execution_id, events) = support::build_history(n);
    let snapshot = HistorySnapshot {
        workflow_name: "order_flow".to_string(),
        execution_id,
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        start_provenance: None,
        workflow_id: None,
        queue_name: None,
    };
    serde_json::to_string(&snapshot).expect("serialize snapshot")
}

fn load(text: &str) -> HistorySnapshot {
    let redacted = is_redacted_export(text);
    assert!(!redacted);
    serde_json::from_str(text).expect("parse snapshot")
}

fn main() {
    let n = env_usize("DBG_DIFF_N", 5_000);

    let left_text = snapshot_json(n);
    let right_text = snapshot_json(n);

    let left_snapshot = load(&left_text);
    let right_snapshot = load(&right_text);
    let left = ReplayTrace::from_history(
        left_snapshot.workflow_name.clone(),
        left_snapshot.execution_id,
        &left_snapshot.events,
    );
    let right = ReplayTrace::from_history(
        right_snapshot.workflow_name.clone(),
        right_snapshot.execution_id,
        &right_snapshot.events,
    );
    let diff = diff_traces(std::hint::black_box(&left), std::hint::black_box(&right));
    std::hint::black_box(&diff);

    println!(
        "debugger_diff_profile: n={n} steps={} clean={}",
        diff.left_steps,
        diff.is_clean()
    );
}
