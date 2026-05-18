//! Tests for `WorkflowReplayer` — the replay harness described in issue #135.
//!
//! These tests exercise the public API in `autumn_harvest::testing`:
//!
//! - `WorkflowReplayer::new().register_fn(name, handler)` — fluent construction
//! - `replay_from_events` — hand-authored fixture replays
//! - `replay_from_json` — round-trip through the JSON snapshot format
//! - Non-determinism detection across activity, timer, version, and signal kinds

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{
    HistorySnapshot, NonDeterminismKind, ReplayStatus, WorkflowReplayer,
};
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Test workflow handler functions
// ---------------------------------------------------------------------------

/// Two sequential activities then a timer — the "canonical" replay fixture.
fn canonical_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Same workflow but with activities in reversed order — triggers non-determinism.
fn reordered_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // step_two before step_one — diverges from canonical history
        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Workflow that uses `ctx.version()` to gate a new code path — correctly fenced.
fn versioned_workflow_fenced<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        // Version gate: if version >= 2, run step_three; old histories skip it.
        let version = ctx.version("add_step_three", 1, 2);
        if version >= 2 {
            ctx.execute_activity_raw("step_three", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
        }

        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Workflow that adds a new activity WITHOUT a version fence — causes non-determinism.
fn versioned_workflow_unfenced<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        // step_three inserted WITHOUT a version gate — diverges against old histories
        ctx.execute_activity_raw("step_three", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Workflow that starts a timer before any activities.
fn timer_first_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("wait", 30).await.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// ---------------------------------------------------------------------------
// Helper: build canonical event history
// ---------------------------------------------------------------------------

fn canonical_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let timer_id = TimerId::new("cooldown");
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("result_one"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id2,
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id2,
            output: serde_json::json!("result_two"),
        },
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 60,
        },
        WorkflowEvent::TimerFired { timer_id },
    ];
    (exec_id, events)
}

fn build_replayer() -> WorkflowReplayer {
    WorkflowReplayer::new()
        .register_fn("canonical_workflow", canonical_workflow)
        .register_fn("reordered_workflow", reordered_workflow)
        .register_fn("versioned_workflow_fenced", versioned_workflow_fenced)
        .register_fn("versioned_workflow_unfenced", versioned_workflow_unfenced)
        .register_fn("timer_first_workflow", timer_first_workflow)
}

/// Build a snapshot from a `(exec_id, events)` pair with a given workflow name.
fn make_snapshot(name: &str, exec_id: ExecutionId, events: Vec<WorkflowEvent>) -> HistorySnapshot {
    HistorySnapshot {
        workflow_name: name.to_string(),
        execution_id: exec_id,
        events,
    }
}

// ---------------------------------------------------------------------------
// (a) Unchanged workflow against its own history → ReplaySucceeded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_unchanged_workflow_succeeds() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("canonical_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "unchanged workflow must succeed replay, got: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
}

// ---------------------------------------------------------------------------
// (b) Activities reordered → NonDeterminismDetected { ActivityScheduleMismatch }
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_reordered_activities_detects_non_determinism() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("reordered_workflow", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                *kind,
                NonDeterminismKind::ActivityScheduleMismatch,
                "reordered activities must produce ActivityScheduleMismatch, got {kind:?}"
            );
        }
        other => panic!("expected NonDeterminismDetected, got: {other:?}\nreport: {report}"),
    }
}

// ---------------------------------------------------------------------------
// (c) New code path correctly fenced behind ctx.version() → ReplaySucceeded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_version_fenced_workflow_succeeds() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("versioned_workflow_fenced", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "version-fenced new path must succeed replay, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// (d) Same new path NOT fenced → NonDeterminismDetected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_version_unfenced_detects_non_determinism() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "versioned_workflow_unfenced",
            exec_id,
            events,
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "unfenced new code path must produce NonDeterminismDetected, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Display impl gives useful failure message
// ---------------------------------------------------------------------------

#[tokio::test]
async fn report_display_is_useful_on_failure() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("reordered_workflow", exec_id, events))
        .await;

    let display = format!("{report}");
    assert!(
        display.contains("NonDeterminism") || display.contains("mismatch"),
        "Display impl must mention the error class: {display}"
    );
}

// ---------------------------------------------------------------------------
// replay_from_json round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_from_json_succeeds_with_unchanged_workflow() {
    use autumn_harvest::testing::HistorySnapshot;

    let (exec_id, events) = canonical_history();
    let snapshot = HistorySnapshot {
        workflow_name: "canonical_workflow".to_string(),
        execution_id: exec_id,
        events,
    };
    let json = serde_json::to_string(&snapshot).expect("serialization must succeed");

    let replayer = build_replayer();
    let report = replayer
        .replay_from_json(&json)
        .await
        .expect("JSON deserialisation must succeed");

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "JSON round-trip with unchanged workflow must succeed: {report}"
    );
}

#[tokio::test]
async fn replay_from_json_detects_non_determinism() {
    use autumn_harvest::testing::HistorySnapshot;

    let (exec_id, events) = canonical_history();
    let snapshot = HistorySnapshot {
        workflow_name: "reordered_workflow".to_string(),
        execution_id: exec_id,
        events,
    };
    let json = serde_json::to_string(&snapshot).expect("serialization must succeed");

    let replayer = build_replayer();
    let report = replayer
        .replay_from_json(&json)
        .await
        .expect("JSON deserialisation must succeed");

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "JSON replay with reordered workflow must detect non-determinism: {report}"
    );
}

#[tokio::test]
async fn replay_from_json_rejects_invalid_json() {
    let replayer = build_replayer();
    let result = replayer.replay_from_json("not valid json").await;
    assert!(result.is_err(), "invalid JSON must return Err");
}

// ---------------------------------------------------------------------------
// Unknown workflow name surfaces as WorkflowFailed with descriptive error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_unknown_workflow_surfaces_as_failed() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("unknown_workflow", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::WorkflowFailed { error, .. } => {
            assert!(
                error.contains("unknown_workflow"),
                "error must name the missing workflow: {error}"
            );
        }
        other => panic!("expected WorkflowFailed for unknown workflow, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// ReplayReport struct fields are populated correctly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn report_fields_are_populated() {
    let (exec_id, events) = canonical_history();
    let event_count = events.len();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("canonical_workflow", exec_id, events))
        .await;

    assert_eq!(report.execution_id, exec_id, "execution_id must match");
    assert!(
        report.events_replayed <= event_count,
        "events_replayed must not exceed total events"
    );
}

// ---------------------------------------------------------------------------
// Timer mismatch: workflow expects timer, history has activity at that position
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_activity_history_for_timer_workflow_detects_timer_mismatch() {
    // Build a history with activity events, but the workflow starts with a timer.
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        // History has activity first, but timer_first_workflow starts with a timer
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("ok"),
        },
    ];

    let replayer = build_replayer();
    let report = replayer
        .replay_from_snapshot(make_snapshot("timer_first_workflow", exec_id, events))
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::TimerMismatch,
                ..
            }
        ),
        "timer-missing divergence must produce TimerMismatch, got: {:?}",
        report.status
    );
}

// ---------------------------------------------------------------------------
// Non-determinism report includes expected and actual strings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn non_determinism_report_includes_expected_actual() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("reordered_workflow", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::NonDeterminismDetected {
            expected, actual, ..
        } => {
            assert!(!expected.is_empty(), "expected must not be empty");
            assert!(!actual.is_empty(), "actual must not be empty");
        }
        other => panic!("expected NonDeterminismDetected, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Activity input mismatch is detected (strict replay mode)
// ---------------------------------------------------------------------------

/// Workflow that calls `step_one` with a NON-NULL input that won't match history.
fn changed_input_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // History records step_one with input=null but this code now passes 42.
        ctx.execute_activity_raw("step_one", serde_json::json!(42), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

#[tokio::test]
async fn replay_activity_with_changed_input_detects_non_determinism() {
    // Build a history where step_one was called with input=null.
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null, // recorded input was null
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("ok"),
        },
    ];

    // Replay with a workflow that passes 42 — strict mode should catch the mismatch.
    let replayer = WorkflowReplayer::new().register_fn("changed", changed_input_workflow);
    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "changed".to_string(),
            execution_id: exec_id,
            events,
        })
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ActivityScheduleMismatch,
                ..
            }
        ),
        "changed input must produce ActivityScheduleMismatch, got: {:?}",
        report.status
    );
}

// ---------------------------------------------------------------------------
// Replayer is usable as a static helper in CI (one-liner pattern)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_liner_ci_pattern_compiles_and_runs() {
    let (_exec_id, events) = canonical_history();

    // This is the pattern shown in README docs — single handler, bare events list.
    let report = WorkflowReplayer::new()
        .register_fn("canonical_workflow", canonical_workflow)
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "one-liner pattern must succeed: {report}"
    );
}

// ---------------------------------------------------------------------------
// Early-completion detection
// ---------------------------------------------------------------------------

/// Workflow that only executes `step_one`, ignoring the rest of the history.
fn early_return_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        // Returns before consuming step_two or the timer — early completion.
        Ok(Value::Null)
    })
}

/// A workflow that returns early leaves unconsumed history events → `NonDeterminismDetected`
/// with `EarlyCompletion` kind.
#[tokio::test]
async fn replay_early_completion_detects_non_determinism() {
    let (exec_id, events) = canonical_history();

    let report = WorkflowReplayer::new()
        .register_fn("early_return_workflow", early_return_workflow)
        .replay_from_snapshot(make_snapshot("early_return_workflow", exec_id, events))
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::EarlyCompletion,
                ..
            }
        ),
        "early-return workflow must produce EarlyCompletion non-determinism, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Suspended-during-replay detection
// ---------------------------------------------------------------------------

/// Workflow that issues a new activity not in the recorded history.
fn extra_step_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        // NEW step not in the canonical 2-activity history → suspends.
        ctx.execute_activity_raw("step_three_new", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// A workflow that adds a new command beyond recorded history suspends during
/// strict replay → `NonDeterminismDetected` (not `ReplaySucceeded`).
#[tokio::test]
async fn replay_new_command_beyond_history_detects_non_determinism() {
    // Build a 2-activity history (step_one, step_two only — no step_three).
    let exec_id = ExecutionId::new();
    let id1 = autumn_harvest::types::ActivityExecId::new();
    let id2 = autumn_harvest::types::ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id2,
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id2,
            output: Value::Null,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("extra_step_workflow", extra_step_workflow)
        .replay_from_snapshot(make_snapshot("extra_step_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "workflow adding a new command beyond history must be non-deterministic: {report}"
    );
}

// ---------------------------------------------------------------------------
// Terminal lifecycle events ignored by early-completion check
// ---------------------------------------------------------------------------

fn single_step_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// ---------------------------------------------------------------------------
// Acceptance criteria: multiple concurrent version changes in a single workflow
// (spec AC#3: "Must support multiple concurrent version changes")
// ---------------------------------------------------------------------------

/// Workflow with two sequential version gates that BOTH have recorded markers.
fn two_gate_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let va = ctx.version("gate_alpha", 1, 3);
        let vb = ctx.version("gate_beta", 1, 5);
        Ok(serde_json::json!({"va": va, "vb": vb}))
    })
}

/// History with two version markers: `gate_alpha=2`, `gate_beta=4`.
fn two_gate_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_alpha".into(),
            details: serde_json::json!(2_u32),
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_beta".into(),
            details: serde_json::json!(4_u32),
        },
    ];
    (exec_id, events)
}

/// Both version markers are consumed in order — no non-determinism.
#[tokio::test]
async fn replay_two_concurrent_version_gates_succeed() {
    let (exec_id, events) = two_gate_history();
    let report = WorkflowReplayer::new()
        .register_fn("two_gate_workflow", two_gate_workflow)
        .replay_from_snapshot(make_snapshot("two_gate_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "two concurrent version gates must replay cleanly: {report}"
    );
}

/// A version gate interleaved with an activity also replays correctly.
fn interleaved_gate_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let va = ctx.version("gate_alpha", 1, 3);
        let r = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let vb = ctx.version("gate_beta", 1, 5);
        Ok(serde_json::json!({"va": va, "r": r, "vb": vb}))
    })
}

fn interleaved_gate_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let aid = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_alpha".into(),
            details: serde_json::json!(2_u32),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: aid,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: aid,
            output: serde_json::json!("done"),
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_beta".into(),
            details: serde_json::json!(4_u32),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_interleaved_version_gate_and_activity_succeed() {
    let (exec_id, events) = interleaved_gate_history();
    let report = WorkflowReplayer::new()
        .register_fn("interleaved_gate_workflow", interleaved_gate_workflow)
        .replay_from_snapshot(make_snapshot("interleaved_gate_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "interleaved version gate + activity must replay cleanly: {report}"
    );
}

// ---------------------------------------------------------------------------
// Acceptance criteria: clear warnings for improper use
// (spec AC#5: "Must provide clear compilation errors or runtime warnings if
//  versioning is used improperly")
// ---------------------------------------------------------------------------

/// A workflow that was updated to use `version("gate_new", …)` where history
/// still records `version("gate_old", …)`.  The unconsumed old marker causes
/// the next command (an activity) to see a `MarkerRecorded(version:gate_old)`
/// at the position where it expects `ActivityScheduled(step)`.
/// The replayer must classify this as `VersionMarkerMismatch`, not a generic
/// `ActivityScheduleMismatch`, so the error message clearly points to the
/// version gate as the source of the problem.
fn renamed_gate_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Code was updated: "gate_old" → "gate_new", but history still has gate_old
        let _v = ctx.version("gate_new", 1, 2);
        ctx.execute_activity_raw("step", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn history_with_old_gate() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let aid = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_old".into(),
            details: serde_json::json!(1_u32),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: aid,
            name: "step".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: aid,
            output: serde_json::json!("done"),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_renamed_version_gate_classified_as_version_marker_mismatch() {
    let (exec_id, events) = history_with_old_gate();
    let report = WorkflowReplayer::new()
        .register_fn("renamed_gate_workflow", renamed_gate_workflow)
        .replay_from_snapshot(make_snapshot("renamed_gate_workflow", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                *kind,
                NonDeterminismKind::VersionMarkerMismatch,
                "a renamed version gate must produce VersionMarkerMismatch, got {kind:?}\nreport: {report}"
            );
        }
        other => panic!("expected NonDeterminismDetected, got: {other:?}\nreport: {report}"),
    }
}

/// Histories loaded from a completed execution include `WorkflowCompleted` as
/// the last event. An unchanged workflow should still report `ReplaySucceeded`.
#[tokio::test]
async fn replay_history_with_workflow_completed_tail_succeeds() {
    let exec_id = ExecutionId::new();
    let id1 = autumn_harvest::types::ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: Value::Null,
        },
        // Terminal lifecycle event — the executor appends this after the
        // workflow returns. It must not be treated as "unconsumed history".
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("single_step_workflow", single_step_workflow)
        .replay_from_snapshot(make_snapshot("single_step_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "history with WorkflowCompleted tail must not trigger early-completion: {report}"
    );
}

// ---------------------------------------------------------------------------
// Child workflow replay tests
// ---------------------------------------------------------------------------

fn child_spawning_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .spawn_child_workflow_raw("child_processor", serde_json::json!({"item": "A"}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"processed": result}))
    })
}

fn renamed_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Name changed from "child_processor" → triggers non-determinism
        let result = ctx
            .spawn_child_workflow_raw("renamed_processor", serde_json::json!({"item": "A"}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

fn changed_input_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Input changed — triggers non-determinism
        let result = ctx
            .spawn_child_workflow_raw("child_processor", serde_json::json!({"item": "CHANGED"}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

fn child_spawning_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let child_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "child_processor".into(),
            input: serde_json::json!({"item": "A"}),
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id,
            output: serde_json::json!({"done": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"processed": {"done": true}}),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replayer_succeeds_for_workflow_spawning_a_child() {
    let (exec_id, events) = child_spawning_history();
    let report = WorkflowReplayer::new()
        .register_fn("child_spawning_workflow", child_spawning_workflow)
        .replay_from_snapshot(make_snapshot("child_spawning_workflow", exec_id, events))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "child spawn workflow must replay successfully: {report}"
    );
}

#[tokio::test]
async fn replayer_detects_changed_child_workflow_name() {
    let (exec_id, events) = child_spawning_history();
    let report = WorkflowReplayer::new()
        .register_fn("renamed_child_workflow", renamed_child_workflow)
        .replay_from_snapshot(make_snapshot("renamed_child_workflow", exec_id, events))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "renamed child workflow must trigger non-determinism: {report}"
    );
}

#[tokio::test]
async fn replayer_reports_child_workflow_mismatch_kind() {
    let (exec_id, events) = child_spawning_history();
    let report = WorkflowReplayer::new()
        .register_fn("renamed_child_workflow", renamed_child_workflow)
        .replay_from_snapshot(make_snapshot("renamed_child_workflow", exec_id, events))
        .await;
    match &report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                *kind,
                NonDeterminismKind::ChildWorkflowMismatch,
                "wrong non-determinism kind: {kind:?}"
            );
        }
        other => panic!("expected NonDeterminismDetected, got {other:?}"),
    }
}

#[tokio::test]
async fn replayer_detects_changed_child_workflow_input() {
    let (exec_id, events) = child_spawning_history();
    let report = WorkflowReplayer::new()
        .register_fn("changed_input_child_workflow", changed_input_child_workflow)
        .replay_from_snapshot(make_snapshot(
            "changed_input_child_workflow",
            exec_id,
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "changed child input must trigger non-determinism: {report}"
    );
}

// ---------------------------------------------------------------------------
// External signal replay tests (issue #330)
// ---------------------------------------------------------------------------

/// Build a history with `ExternalSignalRequested` + `ExternalSignalDelivered`.
fn external_signal_delivered_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let signal_id = autumn_harvest::types::ExternalSignalId::new();
    let target = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ExternalSignalRequested {
            signal_id,
            target,
            signal_name: "tenant_cancel".into(),
            payload: serde_json::json!({"reason": "billing_lapse"}),
        },
        WorkflowEvent::ExternalSignalDelivered { signal_id },
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    (exec_id, events)
}

/// Build a history with `ExternalSignalRequested` + `ExternalSignalFailed`.
fn external_signal_failed_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let signal_id = autumn_harvest::types::ExternalSignalId::new();
    let target = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ExternalSignalRequested {
            signal_id,
            target,
            signal_name: "tenant_cancel".into(),
            payload: Value::Null,
        },
        WorkflowEvent::ExternalSignalFailed {
            signal_id,
            reason_code: "target_terminal".into(),
        },
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    (exec_id, events)
}

/// Workflow that signals an external workflow, then returns Ok.
fn external_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().unwrap_or("");
        let target: ExecutionId = target_str.parse().unwrap();
        let _result = ctx
            .signal_external_workflow(
                target,
                "tenant_cancel",
                serde_json::json!({"reason": "billing_lapse"}),
            )
            .await;
        Ok(Value::Null)
    })
}

/// Workflow that signals an external workflow with a DIFFERENT signal name than history.
fn external_signal_wrong_name_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().unwrap_or("");
        let target: ExecutionId = target_str.parse().unwrap();
        // Uses "wrong_signal" instead of "tenant_cancel" — triggers non-determinism
        ctx.signal_external_workflow(target, "wrong_signal", Value::Null)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

#[tokio::test]
async fn replayer_replays_external_signal_delivered_successfully() {
    let (_exec_id, events) = external_signal_delivered_history();
    // Extract target from events
    let target = events
        .iter()
        .find_map(|e| {
            if let WorkflowEvent::ExternalSignalRequested { target, .. } = e {
                Some(*target)
            } else {
                None
            }
        })
        .unwrap();

    let input = serde_json::json!({"target": target.to_string()});
    let events_with_input: Vec<_> = events
        .iter()
        .map(|e| match e {
            WorkflowEvent::WorkflowStarted { timestamp, .. } => WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: *timestamp,
            },
            other => other.clone(),
        })
        .collect();

    let report = WorkflowReplayer::new()
        .register_fn("external_signal_workflow", external_signal_workflow)
        .replay_from_events(events_with_input)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "external signal delivered history must replay cleanly: {report}"
    );
}

#[tokio::test]
async fn replayer_detects_external_signal_name_mismatch() {
    let (_exec_id, events) = external_signal_delivered_history();
    let target = events
        .iter()
        .find_map(|e| {
            if let WorkflowEvent::ExternalSignalRequested { target, .. } = e {
                Some(*target)
            } else {
                None
            }
        })
        .unwrap();

    let input = serde_json::json!({"target": target.to_string()});
    let events_with_input: Vec<_> = events
        .iter()
        .map(|e| match e {
            WorkflowEvent::WorkflowStarted { timestamp, .. } => WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: *timestamp,
            },
            other => other.clone(),
        })
        .collect();

    let report = WorkflowReplayer::new()
        .register_fn(
            "external_signal_wrong_name_workflow",
            external_signal_wrong_name_workflow,
        )
        .replay_from_events(events_with_input)
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ExternalSignalMismatch,
                ..
            }
        ),
        "wrong signal name must trigger ExternalSignalMismatch: {report}"
    );
}

#[tokio::test]
async fn replayer_replays_external_signal_failed_history() {
    let (_exec_id, events) = external_signal_failed_history();
    let target = events
        .iter()
        .find_map(|e| {
            if let WorkflowEvent::ExternalSignalRequested { target, .. } = e {
                Some(*target)
            } else {
                None
            }
        })
        .unwrap();

    let input = serde_json::json!({"target": target.to_string()});
    let events_with_input: Vec<_> = events
        .iter()
        .map(|e| match e {
            WorkflowEvent::WorkflowStarted { timestamp, .. } => WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: *timestamp,
            },
            other => other.clone(),
        })
        .collect();

    // The workflow handles the error from signal_external_workflow by ignoring it
    // (the `let _result = ...` pattern), so the workflow itself succeeds.
    let report = WorkflowReplayer::new()
        .register_fn("external_signal_workflow", external_signal_workflow)
        .replay_from_events(events_with_input)
        .await;

    // The workflow catches the error and returns Ok, so replay succeeds.
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "external signal failed history should replay successfully when error is handled: {report}"
    );
}
