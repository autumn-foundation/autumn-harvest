//! Deterministic (non-criterion) harness for profiling `ReplayVerifier`'s
//! batch fixture-verification cost -- issue #251's realistic budget
//! ("verifying 1,000 fixtures averaging 1k events each completes in under
//! 30 seconds on a 4-core laptop, in-memory user code, no DB").
//!
//! Unlike `replay_profile.rs` (which constructs `Vec<WorkflowEvent>` directly
//! in memory and calls `WorkflowReplayer::replay_from_events`),
//! `ReplayVerifier::verify_dir` exercises a *different* boundary that no
//! existing profiling harness in this repo touches: real filesystem I/O
//! (`std::fs::read_to_string` over a directory walk) and JSON *deserialize*
//! of `HistorySnapshot`/`WorkflowEvent` from a string -- the same
//! `serde_json::from_str` boundary a production worker crosses every time it
//! loads a recorded history from `harvest_events`. `replay_profile.rs`
//! constructs events as Rust struct literals and never exercises this path
//! at all.
//!
//! Mirrors `replay_verifier_bench.rs`'s exact fixture shape (`Value::Null`
//! payloads, `activity_count` activities per fixture) so this harness
//! measures the *same* documented issue #251 workload, not a bespoke shape
//! invented to flatter a particular change. The one difference is the
//! *fixture count*, which is reduced by default (see
//! `VERIFY_PROFILE_FIXTURES` below) purely to keep a single valgrind run
//! tractable -- callgrind emulation is roughly one to two orders of
//! magnitude slower than native execution, and issue #251's full 1,000
//! fixtures is calibrated against a 30-second *native* wall-clock budget.
//! Fixture count and total instructions scale linearly (see
//! `docs/performance-verify.md`), so a reduced run is representative; set
//! `VERIFY_PROFILE_FIXTURES=1000` to reproduce the exact documented shape
//! given enough wall-clock headroom.
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is driven
//! directly under `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts/bytes), which are deterministic
//! across runs. See `docs/performance-verify.md` for the numbers this
//! produces and how to reproduce them.
//!
//! `harness = false`, own `main()` -- same shape as `replay_profile.rs` -- so
//! the compiled artifact is a plain executable a profiler can be pointed at
//! directly, with no criterion wall-clock loop diluting the measured work.
//!
//! # Running
//!
//! ```text
//! # Locate the compiled binary (no criterion timing loop runs; this just
//! # resolves the path cargo built):
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench verify_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable'
//!
//! # Instruction counts:
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=callgrind.out <path>
//! callgrind_annotate callgrind.out
//!
//! # Allocation counts/bytes:
//! valgrind --tool=dhat --dhat-out-file=dhat.json <path>
//! ```
//!
//! `VERIFY_PROFILE_FIXTURES` (default `20`) sets the number of fixture files
//! written to a temp directory and verified. `VERIFY_PROFILE_ACTIVITIES`
//! (default `500`, matching `replay_verifier_bench.rs`'s
//! `bench_1000_fixtures` -- `500` activities = `1_001` events per fixture,
//! issue #251's exact per-fixture shape) sets activities per fixture.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{HistorySnapshot, ReplayVerifier};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::Value;

/// Workflow that executes N sequential activities. Structurally identical to
/// `replay_verifier_bench.rs`'s `sequential_workflow`.
fn sequential_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = usize::try_from(input.as_u64().unwrap_or(0)).unwrap_or(0);
        for i in 0..n {
            ctx.execute_activity_raw(&format!("activity_{i}"), Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(Value::Null)
    })
}

/// Build a `HistorySnapshot` JSON string with `activity_count` completed
/// activities. Byte-for-byte the same shape `replay_verifier_bench.rs`'s
/// `build_fixture_json` produces.
fn build_fixture_json(activity_count: usize) -> String {
    let exec_id = ExecutionId::new();
    let mut events = Vec::with_capacity(activity_count * 2 + 1);
    events.push(WorkflowEvent::WorkflowStarted {
        input: Value::from(activity_count as u64),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    });
    for i in 0..activity_count {
        let activity_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: format!("activity_{i}"),
            input: Value::Null,
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::Null,
        });
    }
    let snapshot = HistorySnapshot {
        workflow_name: "sequential".to_string(),
        execution_id: exec_id,
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
        queue_name: None,
    };
    serde_json::to_string(&snapshot).unwrap()
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let fixtures = env_usize("VERIFY_PROFILE_FIXTURES", 20);
    let activities = env_usize("VERIFY_PROFILE_ACTIVITIES", 500);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    // Setup: write `fixtures` copies of the same fixture JSON to a temp dir.
    // Not part of the measured "verify" call, but unavoidably part of total
    // process cost under a profiler with no way to bracket a sub-region --
    // same caveat `replay_profile.rs` documents for `build_history`.
    let dir = tempfile::tempdir().expect("tempdir");
    let fixture_json = build_fixture_json(activities);
    for i in 0..fixtures {
        std::fs::write(
            dir.path().join(format!("fixture_{i:05}.json")),
            &fixture_json,
        )
        .expect("write fixture");
    }

    let report = rt.block_on(async {
        ReplayVerifier::new()
            .register_fn("sequential", sequential_workflow)
            .verify_dir(dir.path())
            .await
    });

    assert_eq!(report.fixtures_total, fixtures, "fixture count mismatch");
    assert_eq!(
        report.failed + report.harness_errors,
        0,
        "unexpected fixture failure(s): {report:?}"
    );

    println!(
        "verify_profile: fixtures={fixtures} activities_per_fixture={activities} \
         total_events_verified={} succeeded={}",
        fixtures * (activities * 2 + 1),
        report.succeeded,
    );
}
