//! Integration tests for the in-flight replay-drift gate (issue #798).
//!
//! These cover the two library-side acceptance criteria:
//!
//! * **AC4** — `replay_bundle(dir) -> ReplayDriftReport` replays a directory of
//!   exported histories against registered workflow fns and returns an aggregate
//!   `{ total, succeeded, diverged: [{ execution_id, workflow_name, kind,
//!   first_divergence }] }`.
//! * **AC5** — `ReplayDriftReport::is_clean()` plus an exit-code helper so the
//!   report drops straight into a CI step.
//!
//! The load-bearing distinction from the pre-existing `ReplayVerifier::verify_dir`
//! batch gate (issue #251) is that **this gate replays *in-flight* executions**.
//! A healthy non-terminal run parked at its recorded frontier replays to
//! `WorkflowOutcome::Suspended`, which strict replay classifies as
//! `NonDeterminismDetected`. A gate built on strict replay would therefore be
//! 100% false-red on exactly the population #798 exists to protect, so
//! `replay_bundle` runs frontier-tolerant (canary) replay.
//!
//! Run with:
//! ```bash
//!   cargo test -p autumn-harvest --features testing --no-default-features \
//!     --test integration replay_drift
//! ```

use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::replay_sample::{SampleManifest, SampleStatus, SampleWorkflowCoverage};
use autumn_harvest::testing::{
    HistorySnapshot, NonDeterminismKind, ReplayDriftReport, ReplayVerifier,
};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Workflow handlers under test
// ---------------------------------------------------------------------------

/// Canonical shape: `step_one` → `step_two`.
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

/// Drifted shape: the two activities were swapped by a code change.
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
// Fixture builders
// ---------------------------------------------------------------------------

fn started_event() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn snapshot_json(workflow_name: &str, exec_id: ExecutionId, events: Vec<WorkflowEvent>) -> String {
    let snapshot = HistorySnapshot {
        workflow_name: workflow_name.to_string(),
        execution_id: exec_id,
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
    };
    serde_json::to_string(&snapshot).unwrap()
}

/// An **in-flight** history: `step_one` is scheduled and still running, so the
/// workflow parks at the frontier exactly as a live non-terminal execution does.
///
/// This is the population #798 samples, and the shape that a strict-replay gate
/// would false-flag as divergent.
fn in_flight_snapshot_json(workflow_name: &str) -> String {
    let exec_id = ExecutionId::new();
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
    ];
    snapshot_json(workflow_name, exec_id, events)
}

/// An in-flight history that is one activity further along: `step_one` completed,
/// `step_two` is in flight.
fn in_flight_second_step_snapshot_json(workflow_name: &str) -> (String, ExecutionId) {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let events = vec![
        started_event(),
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
            activity_id: ActivityExecId::new(),
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
    ];
    (snapshot_json(workflow_name, exec_id, events), exec_id)
}

fn write(dir: &Path, name: &str, contents: &str) {
    std::fs::write(dir.join(name), contents).unwrap();
}

// ===========================================================================
// AC4 — the money test: in-flight histories replay CLEAN
// ===========================================================================

/// The headline guarantee. Three healthy **in-flight** executions replayed
/// against unchanged code must report zero drift.
///
/// Under strict replay every one of these is a `NonDeterminismDetected`
/// (frontier suspension), so this test is what forces `replay_bundle` onto the
/// frontier-tolerant path.
#[tokio::test]
async fn in_flight_bundle_replays_clean_against_unchanged_code() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    write(dir.path(), "b.json", &in_flight_snapshot_json("wf"));
    let (second, _) = in_flight_second_step_snapshot_json("wf");
    write(dir.path(), "c.json", &second);

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.total, 3, "all three fixtures must be replayed");
    assert_eq!(
        report.succeeded, 3,
        "a healthy in-flight run parked at its frontier is NOT drift: {report}"
    );
    assert!(
        report.diverged.is_empty(),
        "expected zero drift, got: {:?}",
        report.diverged
    );
    assert!(report.is_clean(), "report must be clean: {report}");
    assert_eq!(report.exit_code(), 0);
}

// ===========================================================================
// AC4 — drift IS caught (zero false negatives on the sampled set)
// ===========================================================================

/// A workflow-logic change that reorders activities must be caught on an
/// in-flight fixture, with the full per-entry drift detail the AC specifies.
#[tokio::test]
async fn reordered_activities_are_reported_as_drift_with_full_detail() {
    let dir = tempfile::tempdir().unwrap();
    let (json, exec_id) = in_flight_second_step_snapshot_json("wf");
    write(dir.path(), "drifted.json", &json);

    let report = ReplayVerifier::new()
        .register_fn("wf", reordered_workflow)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.total, 1);
    assert_eq!(report.succeeded, 0);
    assert_eq!(
        report.diverged.len(),
        1,
        "expected one drift entry: {report}"
    );

    let drift = &report.diverged[0];
    assert_eq!(drift.execution_id, Some(exec_id));
    assert_eq!(drift.workflow_name, "wf");
    assert_eq!(drift.kind, NonDeterminismKind::ActivityScheduleMismatch);
    assert!(
        !drift.first_divergence.is_empty(),
        "first_divergence must describe the mismatch"
    );
    assert!(
        drift.first_divergence.contains("step_one") || drift.first_divergence.contains("step_two"),
        "first_divergence should name the mismatched activity, got: {}",
        drift.first_divergence
    );

    assert!(!report.is_clean());
    assert_eq!(report.exit_code(), 1, "divergence must exit non-zero");
}

/// Every fixture in a drifted bundle is reported — the gate must not stop at
/// the first divergence, or an operator only ever sees one of N regressions.
#[tokio::test]
async fn all_diverged_fixtures_are_reported_not_just_the_first() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..4 {
        let (json, _) = in_flight_second_step_snapshot_json("wf");
        write(dir.path(), &format!("f{i}.json"), &json);
    }

    let report = ReplayVerifier::new()
        .register_fn("wf", reordered_workflow)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.total, 4);
    assert_eq!(report.diverged.len(), 4);
}

// ===========================================================================
// AC5 — the false-green guard
// ===========================================================================

/// A gate that passes on an empty bundle is worse than no gate: a typo'd
/// `--output-dir`, a failed export, or a filter that matched nothing would all
/// report "green" while verifying nothing.
#[tokio::test]
async fn empty_bundle_is_not_clean_and_exits_non_zero() {
    let dir = tempfile::tempdir().unwrap();

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.total, 0);
    assert!(report.is_empty());
    assert!(
        !report.is_clean(),
        "a vacuous bundle must not report clean — that is a false green"
    );
    assert_ne!(report.exit_code(), 0, "vacuous bundle must exit non-zero");
}

/// A fleet with genuinely nothing in flight is a legitimate state, so the
/// vacuous-bundle guard must be opt-out-able — and both helpers must agree.
#[tokio::test]
async fn empty_bundle_can_be_explicitly_allowed() {
    let dir = tempfile::tempdir().unwrap();

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .allow_empty_bundle(true)
        .replay_bundle(dir.path())
        .await;

    assert!(report.is_empty());
    assert!(
        report.is_clean(),
        "allow_empty_bundle must make empty clean"
    );
    assert_eq!(report.exit_code(), 0);
}

/// A missing directory is a harness error, never a silent pass.
#[tokio::test]
async fn missing_bundle_directory_is_a_harness_error() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does-not-exist");

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(&missing)
        .await;

    assert!(!report.is_clean());
    assert!(!report.blocked.is_empty(), "expected a blocked entry");
    assert_eq!(report.exit_code(), 2);
}

/// A fixture naming a workflow this binary does not register must be a harness
/// error (exit 2), not a silent pass — otherwise a renamed `#[workflow]` fn
/// silently drops its whole in-flight population out of the gate.
#[tokio::test]
async fn unregistered_workflow_blocks_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    write(
        dir.path(),
        "orphan.json",
        &in_flight_snapshot_json("no_such_wf"),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.total, 2);
    assert_eq!(report.blocked.len(), 1);
    assert_eq!(report.blocked[0].workflow_name, "no_such_wf");
    assert!(!report.is_clean());
    assert_eq!(
        report.exit_code(),
        2,
        "harness errors dominate over drift, mirroring CiReport::exit_code"
    );
}

/// A monorepo may hold fixtures from several binaries; `allow_unregistered`
/// downgrades those to skips so the gate stays green for the ones it owns.
#[tokio::test]
async fn allow_unregistered_downgrades_orphan_fixtures_to_skips() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    write(
        dir.path(),
        "orphan.json",
        &in_flight_snapshot_json("no_such_wf"),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .allow_unregistered(true)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.skipped, 1);
    assert!(report.blocked.is_empty());
    assert!(report.is_clean());
    assert_eq!(report.exit_code(), 0);
}

/// Unparseable JSON must block, never silently vanish from the denominator.
#[tokio::test]
async fn unparseable_fixture_blocks_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    write(dir.path(), "broken.json", "{ not valid json");

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.total, 2,
        "a broken fixture still counts in the total"
    );
    assert_eq!(report.blocked.len(), 1);
    assert_eq!(report.exit_code(), 2);
}

// ===========================================================================
// AC4 — the issue-named entry point on `WorkflowReplayer`
// ===========================================================================

/// Issue #798 names `WorkflowReplayer::replay_bundle`; it must exist and agree
/// with the `ReplayVerifier` entry point.
#[tokio::test]
async fn workflow_replayer_replay_bundle_agrees_with_verifier() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));

    let via_replayer = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;
    let via_verifier = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(via_replayer.total, via_verifier.total);
    assert_eq!(via_replayer.succeeded, via_verifier.succeeded);
    assert_eq!(via_replayer.is_clean(), via_verifier.is_clean());
    assert!(via_replayer.is_clean());
}

// ===========================================================================
// AC2 — coverage travels with the bundle
// ===========================================================================

/// The export writes a `manifest.json` carrying per-workflow coverage. The gate
/// surfaces it so CI can see "50 of 4,000 sampled" rather than assuming the
/// bundle is the whole fleet.
#[tokio::test]
async fn bundle_manifest_coverage_is_surfaced_on_the_report() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));

    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Complete,
        states: vec!["RUNNING".into(), "PAUSED".into()],
        per_workflow: vec![SampleWorkflowCoverage {
            workflow_name: "wf".into(),
            sampled: 1,
            in_flight_total: 4000,
        }],
        sampled_total: 1,
        in_flight_total: 4000,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
    };
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    // The manifest must not be replayed as if it were a fixture.
    assert_eq!(report.total, 1, "manifest.json is not a fixture: {report}");
    assert!(report.is_clean());

    let coverage = report.coverage.as_ref().expect("manifest must be surfaced");
    assert_eq!(coverage.sampled_total, 1);
    assert_eq!(coverage.in_flight_total, 4000);
    assert_eq!(coverage.per_workflow[0].workflow_name, "wf");
    assert!(
        coverage.per_workflow[0].truncated(),
        "1 of 4000 sampled is truncated coverage"
    );
}

/// A bundle exported while a shard was unreachable covers less than the fleet.
/// The gate must be able to refuse to go green on a knowingly-partial sample.
#[tokio::test]
async fn partial_shard_coverage_can_block_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));

    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Partial,
        states: vec!["RUNNING".into()],
        per_workflow: vec![SampleWorkflowCoverage {
            workflow_name: "wf".into(),
            sampled: 1,
            in_flight_total: 1,
        }],
        sampled_total: 1,
        in_flight_total: 1,
        unavailable_shards: vec!["shard 1: connection refused".into()],
        inspected_shards: vec![0],
    };
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    // Default: a partial sample still replays clean (it is a sample by design).
    let lenient = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;
    assert!(lenient.is_clean());

    // Opt-in: refuse to certify a bundle that is knowingly missing a shard.
    let strict = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .require_complete_coverage(true)
        .replay_bundle(dir.path())
        .await;
    assert!(
        !strict.is_clean(),
        "require_complete_coverage must reject a partial sample"
    );
    assert_eq!(
        strict.exit_code(),
        4,
        "a knowingly-partial sample is exit 4, the rung operators key on"
    );
}

/// `require_complete_coverage(true)` must fail **closed** when the bundle
/// carries no manifest at all.
///
/// The flag means "never certify a partial read of the fleet". A missing or
/// unparseable manifest is not evidence of complete coverage — it is the absence
/// of evidence, which is exactly what the flag exists to refuse. Treating it as
/// satisfied turns a lost artifact, a `.gitignore`d file, or a CI glob that
/// missed one filename into a silently green release gate.
#[tokio::test]
async fn require_complete_coverage_fails_closed_without_a_manifest() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    // Deliberately no manifest written.

    // Without the flag, a manifest-less bundle is still a normal clean gate.
    let lenient = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;
    assert!(
        lenient.is_clean(),
        "a bundle that makes no coverage claim replays normally: {lenient}"
    );

    let strict = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .require_complete_coverage(true)
        .replay_bundle(dir.path())
        .await;
    assert!(
        !strict.is_clean(),
        "an unverifiable coverage claim must not satisfy \
         require_complete_coverage: {strict}"
    );
    assert_eq!(strict.exit_code(), 4);
}

/// Same fail-closed rule when the manifest exists but cannot be parsed.
#[tokio::test]
async fn require_complete_coverage_fails_closed_on_a_corrupt_manifest() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        "{ this is not valid json",
    );

    let strict = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .require_complete_coverage(true)
        .replay_bundle(dir.path())
        .await;
    assert!(
        !strict.is_clean(),
        "a truncated manifest is not proof of complete coverage: {strict}"
    );
    assert_eq!(strict.exit_code(), 4);
}

// ===========================================================================
// AC4 — the report is machine-readable
// ===========================================================================

#[tokio::test]
async fn drift_report_serializes_to_json_for_ci_tooling() {
    let dir = tempfile::tempdir().unwrap();
    let (json, _) = in_flight_second_step_snapshot_json("wf");
    write(dir.path(), "drifted.json", &json);

    let report = ReplayVerifier::new()
        .register_fn("wf", reordered_workflow)
        .replay_bundle(dir.path())
        .await;

    let value: Value = serde_json::to_value(&report).unwrap();
    assert_eq!(value["total"], 1);
    assert_eq!(value["succeeded"], 0);
    assert_eq!(value["diverged"][0]["workflow_name"], "wf");
    assert_eq!(
        value["diverged"][0]["kind"], "ActivityScheduleMismatch",
        "kind must serialize to the NonDeterminismKind name"
    );
    assert!(value["diverged"][0]["first_divergence"].is_string());
    assert!(value["diverged"][0]["execution_id"].is_string());
}

/// `Display` must be usable directly in a CI failure message.
#[tokio::test]
async fn drift_report_display_names_the_diverged_workflow() {
    let dir = tempfile::tempdir().unwrap();
    let (json, _) = in_flight_second_step_snapshot_json("wf");
    write(dir.path(), "drifted.json", &json);

    let report: ReplayDriftReport = ReplayVerifier::new()
        .register_fn("wf", reordered_workflow)
        .replay_bundle(dir.path())
        .await;

    let rendered = report.to_string();
    assert!(rendered.contains("wf"), "rendered: {rendered}");
    assert!(
        rendered.contains("ActivityScheduleMismatch"),
        "rendered: {rendered}"
    );
}

// ===========================================================================
// Drift classification — the other ways code can diverge
// ===========================================================================

/// A workflow that **completes** against a history still holding unconsumed
/// events is drift, not a clean replay.
///
/// This is the shape of a real regression that deleting a step produces: the
/// new code returns `Ok` two activities early, so nothing "mismatches" — the
/// replay simply stops short. A gate that only compared command-by-command
/// would call this clean and certify a build that abandons in-flight work
/// mid-pipeline. It must land in `diverged`.
#[tokio::test]
async fn early_completion_against_unconsumed_history_is_drift_not_a_pass() {
    fn stops_early<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        // The recorded history has two activities; this build schedules none.
        Box::pin(async move { Ok(Value::Null) })
    }

    let dir = tempfile::tempdir().unwrap();
    let (json, exec_id) = in_flight_second_step_snapshot_json("wf");
    write(dir.path(), "early.json", &json);

    let report = ReplayVerifier::new()
        .register_fn("wf", stops_early)
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.is_clean(),
        "completing early over unconsumed history must not read as clean: {report}"
    );
    assert_eq!(report.exit_code(), 1, "{report}");
    assert_eq!(report.diverged.len(), 1, "{report}");
    assert_eq!(report.diverged[0].execution_id, Some(exec_id));
    assert_eq!(report.diverged[0].workflow_name, "wf");
    assert!(
        !report.diverged[0].first_divergence.is_empty(),
        "a divergence must carry an actionable description: {report}"
    );
}

/// A workflow that returns `Err` during replay is reported as drift too.
///
/// The distinction matters for the *verdict*, not the taxonomy: the candidate
/// build cannot resume this execution, whether it failed a determinism check or
/// panicked on data the old build handled. Either way the run does not survive
/// the deploy, so the gate must not exit 0. The `kind` is `None` because there
/// is no recorded-vs-emitted command pair to name — the failure is the workflow's
/// own error, and it rides `first_divergence` instead.
#[tokio::test]
async fn a_workflow_that_errors_during_replay_is_reported_not_swallowed() {
    fn always_errs<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Err("new code panics on this shape".to_string()) })
    }

    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "errs.json", &in_flight_snapshot_json("wf"));

    let report = ReplayVerifier::new()
        .register_fn("wf", always_errs)
        .replay_bundle(dir.path())
        .await;

    assert!(!report.is_clean(), "{report}");
    assert_eq!(report.exit_code(), 1, "{report}");
    assert_eq!(report.diverged.len(), 1, "{report}");
    assert!(
        report.diverged[0]
            .first_divergence
            .contains("new code panics on this shape"),
        "the workflow's own error must reach the operator: {report}"
    );
}

// ===========================================================================
// Exit-code ladder — the rungs must dominate in a fixed order
// ===========================================================================

/// A blocked fixture outranks a divergence.
///
/// The ordering is not cosmetic. Exit 1 says "your code regressed"; exit 2 says
/// "this gate did not fully run". A bundle containing both must report 2,
/// because the divergence count is a *lower bound* — the blocked fixture might
/// have diverged too, and an operator who reads a `1` treats the listed
/// divergences as the complete set.
#[tokio::test]
async fn a_blocked_fixture_outranks_a_divergence_in_the_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let (drifted, _) = in_flight_second_step_snapshot_json("wf");
    write(dir.path(), "drifted.json", &drifted);
    write(dir.path(), "garbage.json", "{ not a snapshot");

    let report = ReplayVerifier::new()
        .register_fn("wf", reordered_workflow)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.diverged.len(), 1, "the divergence is still reported");
    assert_eq!(report.blocked.len(), 1);
    assert_eq!(
        report.exit_code(),
        2,
        "a partially-run gate must not report the narrower 'your code regressed': {report}"
    );
    assert!(!report.is_clean(), "{report}");
}

/// A divergence outranks the coverage rungs (3 and 4).
///
/// An incomplete sample that nonetheless caught a real regression must report
/// the regression: exit 4 reads as "inconclusive, re-run", which would invite an
/// operator to widen the sample and promote — shipping the drift.
#[tokio::test]
async fn a_divergence_outranks_the_coverage_rungs() {
    let dir = tempfile::tempdir().unwrap();
    let (drifted, _) = in_flight_second_step_snapshot_json("wf");
    write(dir.path(), "drifted.json", &drifted);

    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Partial,
        states: vec!["RUNNING".into()],
        per_workflow: vec![SampleWorkflowCoverage {
            workflow_name: "wf".into(),
            sampled: 1,
            in_flight_total: 900,
        }],
        sampled_total: 1,
        in_flight_total: 900,
        unavailable_shards: vec!["shard 1: connection refused".into()],
        inspected_shards: vec![0],
    };
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", reordered_workflow)
        .require_complete_coverage(true)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.exit_code(),
        1,
        "a real regression must not be masked by an inconclusive-coverage exit: {report}"
    );
}

/// `is_clean()` and `exit_code()` can never disagree.
///
/// They are two readings of one verdict, and CI wires up whichever is handier.
/// If they could drift apart, a job asserting `is_clean()` and a job checking
/// `$?` would certify different builds. Pinned across every rung.
#[tokio::test]
async fn is_clean_always_agrees_with_exit_code_on_every_rung() {
    // Rung 0: clean.
    let clean_dir = tempfile::tempdir().unwrap();
    write(clean_dir.path(), "a.json", &in_flight_snapshot_json("wf"));

    // Rung 1: diverged.
    let diverged_dir = tempfile::tempdir().unwrap();
    let (drifted, _) = in_flight_second_step_snapshot_json("wf");
    write(diverged_dir.path(), "a.json", &drifted);

    // Rung 2: blocked.
    let blocked_dir = tempfile::tempdir().unwrap();
    write(blocked_dir.path(), "a.json", "{ not a snapshot");

    // Rung 3: empty.
    let empty_dir = tempfile::tempdir().unwrap();

    for (label, dir, expected) in [
        ("clean", clean_dir.path(), 0),
        ("diverged", diverged_dir.path(), 1),
        ("blocked", blocked_dir.path(), 2),
        ("empty", empty_dir.path(), 3),
    ] {
        let handler: WorkflowHandlerFn = if label == "diverged" {
            reordered_workflow
        } else {
            canonical_workflow
        };
        let report = ReplayVerifier::new()
            .register_fn("wf", handler)
            .replay_bundle(dir)
            .await;

        assert_eq!(report.exit_code(), expected, "{label}: {report}");
        assert_eq!(
            report.is_clean(),
            report.exit_code() == 0,
            "{label}: is_clean() and exit_code() disagree: {report}"
        );
    }
}
