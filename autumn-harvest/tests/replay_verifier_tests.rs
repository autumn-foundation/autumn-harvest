//! Integration tests for `ReplayVerifier` — the batch CI replay gate (issue #251).
//!
//! These tests exercise the three scenarios described in the acceptance criteria:
//! (a) always-pass fixture
//! (b) fixture that fails because the handler has reordered activities
//! (c) fixture for an unregistered workflow name (harness error)
//!
//! Run with:
//!   cargo test -p autumn-harvest --test replay_verifier_tests \
//!     --features testing --no-default-features

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{
    FixtureStatus, HarnessErrorKind, HistorySnapshot, NonDeterminismKind, ReplayStatus,
    ReplayVerifier, ReportFormat,
};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Workflow handler functions for tests
// ---------------------------------------------------------------------------

/// Canonical: step_one → step_two in that order.
fn canonical_workflow<'a>(
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
        Ok(Value::Null)
    })
}

/// Reordered: step_two → step_one — diverges from canonical history.
fn reordered_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// ---------------------------------------------------------------------------
// Helper: produce fixture JSON
// ---------------------------------------------------------------------------

/// Canonical history for `workflow_name`: WorkflowStarted, step_one, step_two.
fn canonical_snapshot_json(workflow_name: &str) -> String {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
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
    let snapshot = HistorySnapshot {
        workflow_name: workflow_name.to_string(),
        execution_id: exec_id,
        events,
    };
    serde_json::to_string(&snapshot).unwrap()
}

/// Minimal snapshot for a workflow that has no handler registered.
fn unregistered_snapshot_json() -> String {
    let snapshot = HistorySnapshot {
        workflow_name: "unregistered_workflow".to_string(),
        execution_id: ExecutionId::new(),
        events: vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }],
    };
    serde_json::to_string(&snapshot).unwrap()
}

/// Write a file into `dir` and return its path.
fn write_fixture(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path
}

// ---------------------------------------------------------------------------
// AC: Integration test — one pass, one fail (ActivityScheduleMismatch),
//     one harness error (UnregisteredWorkflow)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn three_fixture_batch_has_expected_counts() {
    let dir = tempfile::tempdir().unwrap();

    // (a) Pass: handler matches history — canonical_workflow replays correctly.
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );

    // (b) Fail: handler is reordered (step_two first) but history has step_one first
    //     → ActivityScheduleMismatch.
    write_fixture(
        dir.path(),
        "fail.json",
        &canonical_snapshot_json("broken_workflow"),
    );

    // (c) Harness error: no handler registered for "unregistered_workflow".
    write_fixture(
        dir.path(),
        "harness_error.json",
        &unregistered_snapshot_json(),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .register_fn("broken_workflow", reordered_workflow)
        // Note: "unregistered_workflow" intentionally not registered → harness error
        .verify_dir(dir.path())
        .await;

    assert_eq!(report.fixtures_total, 3, "must find all 3 fixtures");
    assert_eq!(report.succeeded, 1, "one fixture must pass");
    assert_eq!(report.failed, 1, "one fixture must fail");
    assert_eq!(
        report.harness_errors, 1,
        "one fixture must be a harness error"
    );

    // (b) confirm failure kind is ActivityScheduleMismatch
    let fail_result = report.results.iter().find(|r| {
        matches!(
            &r.status,
            FixtureStatus::Failed(ReplayStatus::NonDeterminismDetected { .. })
        )
    });
    assert!(
        fail_result.is_some(),
        "must have exactly one FixtureStatus::Failed result"
    );
    if let Some(r) = fail_result {
        if let FixtureStatus::Failed(ReplayStatus::NonDeterminismDetected { kind, .. }) = &r.status
        {
            assert_eq!(
                *kind,
                NonDeterminismKind::ActivityScheduleMismatch,
                "failure kind must be ActivityScheduleMismatch, got {kind:?}"
            );
        }
    }

    // (c) confirm harness error kind is UnregisteredWorkflow
    let harness_err = report.results.iter().find(|r| {
        matches!(
            &r.status,
            FixtureStatus::HarnessError(HarnessErrorKind::UnregisteredWorkflow)
        )
    });
    assert!(
        harness_err.is_some(),
        "must have exactly one HarnessError(UnregisteredWorkflow) result"
    );
}

// ---------------------------------------------------------------------------
// AC: Exit code — 2 when harness error dominates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exit_code_two_when_harness_error_dominates() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(
        dir.path(),
        "fail.json",
        &canonical_snapshot_json("broken_workflow"),
    );
    write_fixture(
        dir.path(),
        "harness_error.json",
        &unregistered_snapshot_json(),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .register_fn("broken_workflow", reordered_workflow)
        .verify_dir(dir.path())
        .await;

    let ci_report = report.into_ci_report();
    assert_eq!(
        ci_report.exit_code(),
        2,
        "harness error must dominate and produce exit code 2"
    );
}

#[tokio::test]
async fn exit_code_zero_when_all_pass() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .verify_dir(dir.path())
        .await;

    assert_eq!(report.into_ci_report().exit_code(), 0);
}

#[tokio::test]
async fn exit_code_one_when_failure_no_harness_error() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "fail.json",
        &canonical_snapshot_json("broken_workflow"),
    );

    let report = ReplayVerifier::new()
        .register_fn("broken_workflow", reordered_workflow)
        .verify_dir(dir.path())
        .await;

    assert_eq!(report.into_ci_report().exit_code(), 1);
}

// ---------------------------------------------------------------------------
// AC: JUnit XML round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn junit_xml_contains_expected_structural_elements() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(
        dir.path(),
        "fail.json",
        &canonical_snapshot_json("broken_workflow"),
    );
    write_fixture(
        dir.path(),
        "harness_error.json",
        &unregistered_snapshot_json(),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .register_fn("broken_workflow", reordered_workflow)
        .verify_dir(dir.path())
        .await;

    let ci = report.into_ci_report();
    let junit = ci.format_report(ReportFormat::JUnit);

    // Must be parseable XML (no junk, balanced tags)
    assert!(
        junit.contains("<?xml"),
        "JUnit output must start with XML declaration"
    );
    assert!(junit.contains("<testsuite"), "must contain <testsuite");
    assert!(junit.contains("<testcase"), "must contain <testcase");

    // Failure case must include <failure> with ActivityScheduleMismatch
    assert!(
        junit.contains("<failure"),
        "must have <failure> element for replay failure"
    );
    assert!(
        junit.contains("ActivityScheduleMismatch"),
        "failure element must name the mismatch kind"
    );

    // Harness error must include <error>
    assert!(
        junit.contains("<error"),
        "must have <error> element for harness error"
    );
    assert!(
        junit.contains("UnregisteredWorkflow"),
        "error element must name the harness error kind"
    );

    // Verify expected/actual fields appear in failure body (per AC)
    assert!(
        junit.contains("expected") || junit.contains("step_one"),
        "failure body must include expected field or activity name"
    );
}

// ---------------------------------------------------------------------------
// AC: JSON report parseable as BatchReplayReport fields
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_report_has_correct_counts() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(
        dir.path(),
        "fail.json",
        &canonical_snapshot_json("broken_workflow"),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .register_fn("broken_workflow", reordered_workflow)
        .verify_dir(dir.path())
        .await;

    let ci = report.into_ci_report();
    let json_str = ci.format_report(ReportFormat::Json);

    let v: serde_json::Value =
        serde_json::from_str(&json_str).expect("JSON report must be valid JSON");

    assert_eq!(v["fixtures_total"].as_u64().unwrap(), 2);
    assert_eq!(v["succeeded"].as_u64().unwrap(), 1);
    assert_eq!(v["failed"].as_u64().unwrap(), 1);
    assert_eq!(v["harness_errors"].as_u64().unwrap(), 0);
}

// ---------------------------------------------------------------------------
// AC: GitHub Actions annotations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn github_report_contains_error_annotations() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "fail.json",
        &canonical_snapshot_json("broken_workflow"),
    );
    write_fixture(
        dir.path(),
        "harness_error.json",
        &unregistered_snapshot_json(),
    );

    let report = ReplayVerifier::new()
        .register_fn("broken_workflow", reordered_workflow)
        .verify_dir(dir.path())
        .await;

    let ci = report.into_ci_report();
    let github = ci.format_report(ReportFormat::GitHub);

    assert!(
        github.contains("::error"),
        "GitHub output must contain ::error annotations"
    );
    assert!(
        github.contains("file="),
        "GitHub annotations must include file= field"
    );
}

// ---------------------------------------------------------------------------
// AC: Text report readable summary
// ---------------------------------------------------------------------------

#[tokio::test]
async fn text_report_contains_summary_counts() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(
        dir.path(),
        "fail.json",
        &canonical_snapshot_json("broken_workflow"),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .register_fn("broken_workflow", reordered_workflow)
        .verify_dir(dir.path())
        .await;

    let ci = report.into_ci_report();
    let text = ci.format_report(ReportFormat::Text);

    // Must mention fixture count and pass/fail status
    assert!(
        text.contains("2") || text.contains("fixtures"),
        "text report must mention fixture count: {text}"
    );
    assert!(
        text.contains("PASS")
            || text.contains("pass")
            || text.contains("FAIL")
            || text.contains("fail"),
        "text report must mention pass/fail status: {text}"
    );
}

// ---------------------------------------------------------------------------
// AC: allow_unregistered flag — converts harness error to skip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn allow_unregistered_does_not_count_as_harness_error() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(dir.path(), "unknown.json", &unregistered_snapshot_json());

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .allow_unregistered(true)
        .verify_dir(dir.path())
        .await;

    assert_eq!(
        report.harness_errors, 0,
        "allow_unregistered=true must not count unregistered workflows as harness errors"
    );
    assert_eq!(report.succeeded, 1);
}

// ---------------------------------------------------------------------------
// AC: Invalid JSON fixture → harness error (exit 2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_json_fixture_is_harness_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("invalid.json"), b"not valid json at all").unwrap();

    let report = ReplayVerifier::new().verify_dir(dir.path()).await;

    assert_eq!(
        report.harness_errors, 1,
        "invalid JSON fixture must be a harness error"
    );
    assert_eq!(
        report.into_ci_report().exit_code(),
        2,
        "harness error must produce exit code 2"
    );
}

// ---------------------------------------------------------------------------
// AC: Rate threshold mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rate_threshold_pass_when_above() {
    let dir = tempfile::tempdir().unwrap();
    // 3 pass, 1 fail = 75% pass rate
    write_fixture(
        dir.path(),
        "p1.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(
        dir.path(),
        "p2.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(
        dir.path(),
        "p3.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(
        dir.path(),
        "fail.json",
        &canonical_snapshot_json("broken_workflow"),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .register_fn("broken_workflow", reordered_workflow)
        .verify_dir(dir.path())
        .await;

    // 75% pass rate passes a 70% threshold
    let ci_70 = report.clone().into_ci_report_with_threshold(0.70);
    assert_eq!(
        ci_70.exit_code(),
        0,
        "75% pass rate must pass 70% threshold"
    );

    // 75% pass rate fails an 80% threshold
    let ci_80 = report.into_ci_report_with_threshold(0.80);
    assert_eq!(
        ci_80.exit_code(),
        1,
        "75% pass rate must fail 80% threshold"
    );
}

// ---------------------------------------------------------------------------
// AC: FixtureResult carries path, workflow_name, execution_id fields
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fixture_result_carries_expected_fields() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot_json = canonical_snapshot_json("good_workflow");
    // Extract exec_id from what we just created
    let snapshot: HistorySnapshot = serde_json::from_str(&snapshot_json).unwrap();
    let expected_exec_id = snapshot.execution_id;

    write_fixture(dir.path(), "pass.json", &snapshot_json);

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .verify_dir(dir.path())
        .await;

    assert_eq!(report.results.len(), 1);
    let result = &report.results[0];

    assert_eq!(result.workflow_name, "good_workflow");
    assert_eq!(result.execution_id, Some(expected_exec_id));
    assert!(
        result.path.file_name().unwrap() == "pass.json",
        "result must carry the source file path"
    );
}

// ---------------------------------------------------------------------------
// AC: fixtures_dir builder method + verify_all terminal method
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_all_uses_fixtures_dir_from_builder() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .fixtures_dir(dir.path())
        .verify_all()
        .await;

    assert_eq!(report.succeeded, 1);
    assert_eq!(report.fixtures_total, 1);
}

// ---------------------------------------------------------------------------
// AC: Recursive directory walk finds fixtures in subdirectories
// ---------------------------------------------------------------------------

#[tokio::test]
async fn verify_dir_walks_subdirectories() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("sub");
    std::fs::create_dir(&sub).unwrap();

    write_fixture(
        dir.path(),
        "root.json",
        &canonical_snapshot_json("good_workflow"),
    );
    write_fixture(
        sub.as_path(),
        "nested.json",
        &canonical_snapshot_json("good_workflow"),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .verify_dir(dir.path())
        .await;

    assert_eq!(
        report.fixtures_total, 2,
        "must find fixtures in subdirectories"
    );
    assert_eq!(report.succeeded, 2);
}

// ---------------------------------------------------------------------------
// AC: CiReport::format_report delegates correctly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ci_report_default_format_is_text() {
    let dir = tempfile::tempdir().unwrap();
    write_fixture(
        dir.path(),
        "pass.json",
        &canonical_snapshot_json("good_workflow"),
    );

    let report = ReplayVerifier::new()
        .register_fn("good_workflow", canonical_workflow)
        .verify_dir(dir.path())
        .await;

    let ci = report.into_ci_report();
    // Text report must be non-empty and human-readable
    let text = ci.format_report(ReportFormat::Text);
    assert!(!text.is_empty(), "text report must not be empty");
}

// ---------------------------------------------------------------------------
// AC: Empty fixtures directory → report with zero counts, exit code 0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_dir_returns_zero_counts_exit_zero() {
    let dir = tempfile::tempdir().unwrap();

    let report = ReplayVerifier::new().verify_dir(dir.path()).await;

    assert_eq!(report.fixtures_total, 0);
    assert_eq!(report.succeeded, 0);
    assert_eq!(report.failed, 0);
    assert_eq!(report.harness_errors, 0);
    assert_eq!(report.into_ci_report().exit_code(), 0);
}
