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
        queue_name: None,
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

/// A manifest claiming complete, untruncated, failure-free coverage.
fn complete_manifest(workflow_name: &str, sampled: u64, in_flight_total: u64) -> SampleManifest {
    SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Complete,
        states: vec!["RUNNING".into(), "PAUSED".into()],
        per_workflow: vec![SampleWorkflowCoverage {
            workflow_name: workflow_name.into(),
            sampled,
            in_flight_total,
            // No identities claimed: these helpers predate the inventory
            // reconciliation and exercise the count path.
            sampled_execution_ids: Vec::new(),
        }],
        sampled_total: sampled,
        in_flight_total,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
    }
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
            sampled_execution_ids: Vec::new(),
        }],
        sampled_total: 1,
        in_flight_total: 4000,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
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

/// The export stops accumulating once it hits the aggregate byte budget, so a
/// bundle can be *smaller than the sample the operator asked for* even though
/// every shard was reachable and every fixture present replays clean.
///
/// Recording that on the manifest is not enough on its own: a CI gate reads the
/// exit code, not the rendered report, so a size-cut bundle that exits `0`
/// certifies the build against a quietly-trimmed subset. The bytes are dropped
/// in export order, so what goes missing is the *tail* of each type's
/// risk-ordered slice — precisely the executions the `oldest` default exists to
/// prioritise.
#[tokio::test]
async fn a_size_truncated_bundle_blocks_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));

    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Complete,
        states: vec!["RUNNING".into()],
        per_workflow: vec![SampleWorkflowCoverage {
            workflow_name: "wf".into(),
            sampled: 1,
            in_flight_total: 1,
            sampled_execution_ids: Vec::new(),
        }],
        sampled_total: 1,
        in_flight_total: 1,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
        truncated_by_size: true,
        export_failures: 0,
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

    // Every fixture that IS present replays clean — the gate must still refuse.
    assert!(
        report.diverged.is_empty(),
        "no divergence was planted: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "a size-truncated export did not fully run the gate: {report}"
    );
    assert!(
        !report.is_clean(),
        "a size-truncated bundle must never read as clean: {report}"
    );
}

/// A candidate the sample *selected* can still fail to export — it exceeds
/// `max_bytes`, or its shard becomes unreadable between selection and fetch.
/// The export records the failure and moves on, so the bundle silently holds
/// fewer fixtures than the sample chose.
///
/// This is the false-GREEN the manifest exists to prevent, and the count check
/// alone cannot see it: the manifest's `sampled_total` counts *survivors*, so it
/// agrees with the bundle exactly. Without a dedicated signal the gate replays a
/// biased subset — biased against the largest histories, which are the longest
/// running and therefore the most likely to span the code change under test —
/// and exits `0`.
#[tokio::test]
async fn an_export_that_dropped_candidates_blocks_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));

    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Complete,
        states: vec!["RUNNING".into()],
        per_workflow: vec![SampleWorkflowCoverage {
            workflow_name: "wf".into(),
            // The survivor count agrees with the bundle, which is exactly why
            // the fixture-count check cannot catch this.
            sampled: 1,
            in_flight_total: 1,
            sampled_execution_ids: Vec::new(),
        }],
        sampled_total: 1,
        in_flight_total: 1,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 1,
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

    assert!(
        report.diverged.is_empty(),
        "no divergence was planted: {report}"
    );
    assert!(
        !report.fixture_count_disagrees_with_manifest(),
        "the survivor count agrees with the bundle, so only a dedicated \
         export-failure signal can catch this: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "an export that dropped selected candidates did not fully run the \
         gate: {report}"
    );
    assert!(!report.is_clean(), "{report}");
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
            sampled_execution_ids: Vec::new(),
        }],
        sampled_total: 1,
        in_flight_total: 1,
        unavailable_shards: vec!["shard 1: connection refused".into()],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
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
///
/// The rung moved from `4` to `2` in Codex round 21, and the move is the point:
/// a corrupt manifest disables the rung-`2` and rung-`3` guards for *every*
/// caller, so blocking only under the opt-in `require_complete_coverage` left the
/// default gate green on a damaged bundle. Rung `2` ("the gate could not fully
/// run") both describes it correctly and dominates rung `4`, so this bundle is
/// now refused whether or not the flag is set — see
/// `corrupt_manifest_blocks_the_default_gate`.
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
    assert_eq!(
        strict.exit_code(),
        2,
        "an unreadable manifest blocks at rung 2, which dominates rung 4: {strict}"
    );
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
            sampled_execution_ids: Vec::new(),
        }],
        sampled_total: 1,
        in_flight_total: 900,
        unavailable_shards: vec!["shard 1: connection refused".into()],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
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

// ===========================================================================
// History policy must match the deployment (issue #614 / PR #1171 review)
// ===========================================================================

/// A workflow whose control flow depends on `should_continue_as_new()`.
///
/// `continue_as_new_threshold` is a *deployment* setting
/// (`HarvestBuilder::history_policy`), not a property of the recorded history —
/// so the bundle cannot carry it and the gate must be told.
fn checkpointing_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let next = if ctx.should_continue_as_new() {
            "checkpoint"
        } else {
            "keep_working"
        };
        ctx.execute_activity_raw(next, Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// An in-flight history recorded by a deployment running a LOW
/// `continue_as_new_threshold`, so the workflow took the `checkpoint` branch.
fn checkpointed_snapshot_json(workflow_name: &str) -> String {
    let exec_id = ExecutionId::new();
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "checkpoint".into(),
            input: Value::Null,
            queue: "default".into(),
        },
    ];
    snapshot_json(workflow_name, exec_id, events)
}

/// Replaying under the DEFAULT policy when production runs a customized one
/// reports drift for a workflow that never drifted — a false red that blocks a
/// healthy deploy. `with_history_policy` is what makes the gate faithful.
///
/// Both halves are asserted against the same bundle, because the builder is
/// only meaningful as a pair: if the default-policy replay were also clean, the
/// test would pass without the fix doing anything.
#[tokio::test]
async fn bundle_replays_under_the_configured_history_policy() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &checkpointed_snapshot_json("wf"));

    // The deployment that produced this history: checkpoint early.
    let production_policy =
        autumn_harvest::context::WorkflowHistoryPolicy::default().with_continue_as_new_threshold(1);

    let faithful = ReplayVerifier::new()
        .register_fn("wf", checkpointing_workflow as WorkflowHandlerFn)
        .with_history_policy(production_policy)
        .replay_bundle(dir.path())
        .await;
    assert!(
        faithful.is_clean(),
        "replaying under the deployment's own policy must be clean: {faithful}"
    );

    // Same bundle, same code, default policy — the threshold is no longer met,
    // so the workflow schedules `keep_working` and the gate sees a divergence
    // that does not exist in production.
    let unfaithful = ReplayVerifier::new()
        .register_fn("wf", checkpointing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;
    assert_eq!(
        unfaithful.diverged.len(),
        1,
        "control: the default policy must genuinely change the outcome, else \
         this test would pass without `with_history_policy` doing anything: {unfaithful}"
    );
}

/// `WorkflowReplayer::replay_bundle` delegates to `ReplayVerifier`; the policy
/// must survive that hop, or the builder is silently dropped on that path.
#[tokio::test]
async fn workflow_replayer_bundle_delegation_carries_the_history_policy() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &checkpointed_snapshot_json("wf"));

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", checkpointing_workflow as WorkflowHandlerFn)
        .with_history_policy(
            autumn_harvest::context::WorkflowHistoryPolicy::default()
                .with_continue_as_new_threshold(1),
        )
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "the replayer's own history policy must reach the bundle walk: {report}"
    );
}

/// A workflow that embeds its own business `workflow_id` in an activity input.
///
/// `workflow_id` lives in no `WorkflowEvent`, so a fixture that omits it leaves
/// the replay context with `""` unless a fallback is threaded in — making this
/// the cheapest way to prove the fallback actually arrives.
fn workflow_id_bearing_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let id = ctx.info().workflow_id;
        ctx.execute_activity_raw("step_one", serde_json::json!({ "wf_id": id }), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// An in-flight history recorded by a run whose business `workflow_id` was
/// `order-42`, in a snapshot that does **not** carry the id — a hand-built
/// fixture, or one exported before issue #698 threaded the field.
fn workflow_id_snapshot_json(workflow_name: &str) -> String {
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            input: serde_json::json!({"wf_id": "order-42"}),
            queue: "default".into(),
        },
    ];
    snapshot_json(workflow_name, ExecutionId::new(), events)
}

/// `WorkflowReplayer::replay_bundle` delegates to `ReplayVerifier`; every
/// configured replay-context fallback must survive that hop (Codex round-10 P2).
///
/// Issue #614 already proved this for `history_policy`. The four
/// `HistorySnapshot`-metadata fallbacks — `context_headers`,
/// `execution_timeout`, `parent_execution_id`, `workflow_id` — were still being
/// discarded, so a caller who configured them on the replayer and then called
/// `replay_bundle` silently replayed under a *different* context and got drift
/// for a workflow that never drifted.
#[tokio::test]
async fn workflow_replayer_bundle_delegation_carries_the_workflow_id_fallback() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &workflow_id_snapshot_json("wf"));

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", workflow_id_bearing_workflow as WorkflowHandlerFn)
        .with_workflow_id("order-42")
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "the replayer's own workflow_id must reach the bundle walk: {report}"
    );
}

/// The control that keeps the fallback test honest: without the builder call the
/// replay context resolves `workflow_id` to `""`, the activity input differs, and
/// the fixture genuinely diverges.
///
/// Without this, the test above would pass even if the fallback did nothing.
#[tokio::test]
async fn the_workflow_id_fallback_genuinely_changes_the_outcome() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &workflow_id_snapshot_json("wf"));

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", workflow_id_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.diverged.len(),
        1,
        "control: omitting the id must genuinely change the outcome, else the \
         fallback test would pass without the delegation carrying anything: {report}"
    );
}

// ===========================================================================
// The execution's queue must survive the fixture round trip (Codex round-12 P1)
// ===========================================================================

/// A workflow that embeds its own execution queue in an activity input.
///
/// `ctx.queue_name()` is sourced from the live worker's task row and lives in no
/// `WorkflowEvent`, so a fixture that omits it leaves the replay context at the
/// empty-string default — the same family as `context_headers` / `workflow_id`.
fn queue_bearing_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("step_one", serde_json::json!({ "queue": queue }), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// An in-flight history recorded by a run on the `priority-queue` task queue,
/// in a snapshot that **does** carry that queue.
fn queue_snapshot_json(workflow_name: &str, queue: Option<&str>) -> String {
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            input: serde_json::json!({"queue": "priority-queue"}),
            queue: "default".into(),
        },
    ];
    let snapshot = HistorySnapshot {
        workflow_name: workflow_name.to_string(),
        execution_id: ExecutionId::new(),
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
        queue_name: queue.map(ToString::to_string),
    };
    serde_json::to_string(&snapshot).unwrap()
}

/// The money test: an exported fixture carries the execution's queue, so a
/// workflow that branches on `ctx.queue_name()` replays clean.
///
/// Without the field, `run_workflow_canary` leaves the replay context's queue at
/// `""`, the recorded activity input no longer matches, and an **unchanged**
/// candidate false-reports drift — the worst outcome a release gate can produce.
#[tokio::test]
async fn a_snapshot_carrying_the_queue_replays_clean() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &queue_snapshot_json("wf", Some("priority-queue")),
    );

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", queue_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "the snapshot's own queue_name must reach the replay context: {report}"
    );
}

/// The control that keeps the money test honest: a snapshot that omits the queue
/// (a legacy export) genuinely diverges, so the test above cannot pass on a
/// no-op.
#[tokio::test]
async fn a_snapshot_without_the_queue_genuinely_diverges() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &queue_snapshot_json("wf", None));

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", queue_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.diverged.len(),
        1,
        "control: omitting the queue must genuinely change the outcome, else the \
         money test would pass without the field carrying anything: {report}"
    );
}

/// `replay_bundle` delegates to `ReplayVerifier`; the replayer's own configured
/// queue fallback must survive that hop, exactly like `workflow_id` (round-10 P2).
#[tokio::test]
async fn workflow_replayer_bundle_delegation_carries_the_queue_fallback() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &queue_snapshot_json("wf", None));

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", queue_bearing_workflow as WorkflowHandlerFn)
        .with_queue_name("priority-queue")
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "the replayer's own queue_name must reach the bundle walk: {report}"
    );
}

// ===========================================================================
// The gate must not certify what it did not verify (PR #1171 review)
// ===========================================================================

/// A manifest declaring more fixtures than the bundle holds means files went
/// missing after the export — a truncated artifact upload, a partial copy.
///
/// Every surviving fixture can replay cleanly, so neither the divergence rung
/// nor `require_complete_coverage` catches it: the manifest's shard status still
/// reads `complete`, because the *export* was complete; it was the *bundle* that
/// lost files. Left unguarded, the gate certifies a strict subset of the sample
/// and reports success.
#[tokio::test]
async fn a_bundle_missing_manifest_declared_fixtures_blocks_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    // One fixture on disk...
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    // ...but the manifest says three were exported.
    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Complete,
        states: vec!["RUNNING".into()],
        per_workflow: vec![SampleWorkflowCoverage {
            workflow_name: "wf".into(),
            sampled: 3,
            in_flight_total: 3,
            sampled_execution_ids: Vec::new(),
        }],
        sampled_total: 3,
        in_flight_total: 3,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
    };
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.is_clean(),
        "a bundle missing two thirds of its declared fixtures must not certify a \
         release: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "an incomplete bundle is a gate that could not fully run, which outranks \
         a divergence: {report}"
    );
    assert!(
        format!("{report}").contains("BUNDLE INCOMPLETE"),
        "the operator must be told the bundle disagrees with its manifest: {report}"
    );
}

/// A bundle whose every fixture was skipped replayed no workflow at all, so it
/// verified exactly as much as an empty directory: nothing.
///
/// Reachable with `allow_unregistered(true)` when a gate binary is wired to the
/// wrong bundle, or registers none of the sampled types.
#[tokio::test]
async fn a_bundle_whose_fixtures_were_all_skipped_verifies_nothing() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    write(dir.path(), "b.json", &in_flight_snapshot_json("wf"));

    let report = ReplayVerifier::new()
        // Deliberately registers a DIFFERENT workflow than the bundle contains.
        .register_fn(
            "some_other_workflow",
            canonical_workflow as WorkflowHandlerFn,
        )
        .allow_unregistered(true)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.total, 2, "{report}");
    assert_eq!(report.skipped, 2, "{report}");
    assert!(
        report.verified_nothing(),
        "no workflow was replayed, so nothing was verified: {report}"
    );
    assert_eq!(
        report.exit_code(),
        3,
        "an all-skipped run is the same non-answer as an empty bundle: {report}"
    );

    // ...and the opt-out still works for a caller who genuinely accepts it.
    let permitted = ReplayVerifier::new()
        .register_fn(
            "some_other_workflow",
            canonical_workflow as WorkflowHandlerFn,
        )
        .allow_unregistered(true)
        .allow_empty_bundle(true)
        .replay_bundle(dir.path())
        .await;
    assert!(permitted.is_clean(), "{permitted}");
}

/// A workflow type with in-flight work but **zero** sampled fixtures was never
/// replayed at all — materially worse than ordinary truncation, where at least
/// some executions of the type are checked.
#[tokio::test]
async fn a_workflow_type_sampled_zero_times_fails_a_complete_coverage_claim() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));

    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Complete,
        states: vec!["RUNNING".into()],
        per_workflow: vec![
            SampleWorkflowCoverage {
                workflow_name: "wf".into(),
                sampled: 1,
                in_flight_total: 1,
                sampled_execution_ids: Vec::new(),
            },
            // Starved by the global cap: in flight, never sampled.
            SampleWorkflowCoverage {
                workflow_name: "never_sampled_wf".into(),
                sampled: 0,
                in_flight_total: 900,
                sampled_execution_ids: Vec::new(),
            },
        ],
        sampled_total: 1,
        in_flight_total: 901,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
    };
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow as WorkflowHandlerFn)
        .require_complete_coverage(true)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.zero_coverage_types(),
        vec!["never_sampled_wf"],
        "{report}"
    );
    assert_eq!(
        report.exit_code(),
        4,
        "a type that was never replayed must not pass a complete-coverage claim: {report}"
    );
    assert!(
        format!("{report}").contains("NOT REPLAYED AT ALL"),
        "the un-replayed type must be named: {report}"
    );

    // Control: the shard status alone is `complete`, so without the zero-coverage
    // check this bundle would have certified the release.
    let without_claim = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;
    assert!(
        without_claim.is_clean(),
        "opt-in only: the default gate is unchanged: {without_claim}"
    );
}

// ===========================================================================
// Offloaded payloads must block, not masquerade as drift (PR #1171 review)
// ===========================================================================

/// A workflow that passes a real, non-trivial input to its activity.
///
/// The input matters: the guard exists because a claim-check envelope is
/// compared *against a real value*, so a workflow passing `Value::Null` could
/// not surface the bug at all.
fn payload_bearing_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw(
            "step_one",
            serde_json::json!({"blob": "x".repeat(64)}),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// An in-flight history whose activity input is either the real payload or a
/// large-payload **offload reference envelope** (issue #524) standing in for it.
///
/// The envelope is built by hand rather than through `PayloadOffloader` so the
/// fixture needs no `PayloadStore`: the wire shape is the contract the guard
/// keys on, and building it here proves the guard reads the same discriminator
/// the offloader writes.
fn payload_snapshot_json(workflow_name: &str, offloaded: bool) -> String {
    let real = serde_json::json!({"blob": "x".repeat(64)});
    let input = if offloaded {
        serde_json::json!({
            "_harvest_offload_envelope": 1,
            "store_id": "s3",
            "key": "sha256:deadbeef",
            "len": 64,
            "checksum": "deadbeef",
        })
    } else {
        real
    };
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            input,
            queue: "default".into(),
        },
    ];
    snapshot_json(workflow_name, ExecutionId::new(), events)
}

/// The money test for the offload guard.
///
/// The export path loads history with `load_history_undecoded`, so on a
/// deployment with a `PayloadStore` registered any payload over the offload
/// threshold reaches the bundle as a claim-check envelope. The bundle walk
/// builds its `WorkflowReplayer` with `payload_offloader: None` and cannot
/// inflate it, so a **healthy** workflow computing the real input would be
/// compared against the envelope and reported as drift.
///
/// That is a false red on a clean deploy — the single worst outcome a release
/// gate can produce. It must be a harness error (exit 2) naming the fix.
#[tokio::test]
async fn an_offloaded_fixture_blocks_the_gate_instead_of_reporting_drift() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "offloaded.json",
        &payload_snapshot_json("wf", true),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", payload_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.total, 1,
        "the fixture still counts in the denominator"
    );
    assert!(
        report.diverged.is_empty(),
        "an un-inflatable envelope is NOT a determinism regression: {:?}",
        report.diverged
    );
    assert_eq!(
        report.blocked.len(),
        1,
        "expected a harness error: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "a bundle this gate cannot honestly evaluate must not read as drift: {report}"
    );

    let reason = report.blocked[0].reason.to_string();
    assert!(
        reason.contains("offloaded payload reference"),
        "the reason must name the cause: {reason}"
    );
    assert!(
        reason.contains("payload_offload_threshold"),
        "the reason must name a concrete fix: {reason}"
    );
    assert!(
        reason.contains("sha256:deadbeef"),
        "the reason must name the blob so an operator can find it: {reason}"
    );
}

/// The control that keeps the guard honest: an envelope-free fixture carrying
/// the **real** payload is untouched and replays clean.
///
/// Without this, the guard could pass by blocking every payload-bearing bundle.
#[tokio::test]
async fn a_payload_bearing_fixture_without_envelopes_is_untouched() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "real.json", &payload_snapshot_json("wf", false));

    let report = ReplayVerifier::new()
        .register_fn("wf", payload_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(report.succeeded, 1, "{report}");
    assert!(report.blocked.is_empty(), "{report}");
    assert!(report.is_clean(), "{report}");
    assert_eq!(report.exit_code(), 0);
}

/// The discriminator appearing inside ordinary payload *data* must not block.
///
/// The guard fast-rejects on a raw substring scan, so a workflow whose own JSON
/// happens to mention the key reaches the structured check — which must find no
/// real envelope and let the fixture through.
#[tokio::test]
async fn the_envelope_key_appearing_as_plain_data_does_not_block() {
    let dir = tempfile::tempdir().unwrap();
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            // The key is present as a *string value*, not an envelope object.
            input: serde_json::json!({"note": "we set _harvest_offload_envelope upstream"}),
            queue: "default".into(),
        },
    ];
    write(
        dir.path(),
        "mentions.json",
        &snapshot_json("wf", ExecutionId::new(), events),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", mentions_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.blocked.is_empty(),
        "a substring hit is not an envelope: {report}"
    );
    assert_eq!(report.succeeded, 1, "{report}");
    assert!(report.is_clean(), "{report}");
}

/// An in-flight history whose activity input is a **codec envelope**
/// (issue #608) standing in for the real payload.
///
/// Built by hand rather than through a `PayloadCodecs` registry so the fixture
/// needs no codec: the wire shape is the contract the guard keys on, and
/// building it here proves the guard reads the same discriminator the codec
/// encode path writes.
fn codec_snapshot_json(workflow_name: &str) -> String {
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            input: serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": "aes-gcm",
                "data": "Y2lwaGVydGV4dA==",
            }),
            queue: "default".into(),
        },
    ];
    snapshot_json(workflow_name, ExecutionId::new(), events)
}

/// The money test for the codec guard (Codex round-10 P1).
///
/// The export loads history with `load_history_undecoded`, and
/// `read_path_decoder` returns `None` whenever `decode_payloads_on_read` is
/// left at its default `false` — so on a codec-**encrypting** deployment every
/// payload-bearing field reaches the bundle as ciphertext in a codec envelope.
///
/// The bundle walk builds its `WorkflowReplayer` with no codec registry and
/// cannot decode it, so a **healthy** workflow computing the real plaintext
/// input would be compared against the envelope and reported as drift: a false
/// red blocking a clean deploy, the single worst outcome a release gate can
/// produce. It must be a harness error (exit 2) naming the fix.
#[tokio::test]
async fn a_codec_encrypted_fixture_blocks_the_gate_instead_of_reporting_drift() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "encrypted.json", &codec_snapshot_json("wf"));

    let report = ReplayVerifier::new()
        .register_fn("wf", payload_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.total, 1,
        "the fixture still counts in the denominator"
    );
    assert!(
        report.diverged.is_empty(),
        "an undecodable envelope is NOT a determinism regression: {:?}",
        report.diverged
    );
    assert_eq!(
        report.blocked.len(),
        1,
        "expected a harness error: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "a bundle this gate cannot honestly evaluate must not read as drift: {report}"
    );

    let reason = report.blocked[0].reason.to_string();
    assert!(
        reason.contains("codec"),
        "the reason must name the cause: {reason}"
    );
    assert!(
        reason.contains("decode_payloads_on_read"),
        "the reason must name a concrete fix: {reason}"
    );
}

/// The money test for the contradicted-emptiness rule (Codex round-18 P1).
///
/// `allow_empty_bundle(true)` is the right setting for a fleet that may
/// legitimately be idle. But an **export outage** produces an empty bundle too:
/// every shard query failing yields zero fixtures and a manifest whose status is
/// `unavailable`. From the fixture count alone the two are identical.
///
/// Neither existing rung catches it. Rung 2's `export_is_incomplete` is
/// `truncated_by_size || export_failures > 0`, and a *total* outage selects no
/// candidates — so nothing failed to fetch and no size ceiling was hit. Rung 4
/// reads the status but only under `require_complete_coverage`, off by default.
///
/// Honoring the opt-out therefore exits 0 on a bundle that inspected nothing —
/// a release certified against zero workflows, the worst verdict this gate can
/// produce.
#[tokio::test]
async fn an_empty_bundle_from_a_failed_export_is_not_licensed_by_allow_empty_bundle() {
    let dir = tempfile::tempdir().unwrap();
    // No fixtures, and a manifest reporting that no shard could be read.
    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Unavailable,
        states: vec!["RUNNING".into()],
        per_workflow: vec![],
        sampled_total: 0,
        in_flight_total: 0,
        unavailable_shards: vec!["shard 0: connection refused".into()],
        inspected_shards: vec![],
        truncated_by_size: false,
        export_failures: 0,
    };
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .allow_empty_bundle(true)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.emptiness_is_contradicted(),
        "an `unavailable` manifest contradicts the claim that the fleet is idle"
    );
    assert_eq!(
        report.exit_code(),
        3,
        "an export outage must not be certified as an idle fleet: {report}"
    );
    assert!(
        format!("{report}").contains("did not read the whole fleet"),
        "the operator must be told the manifest is what voided the opt-out: {report}"
    );
}

/// A **partial** read is the same defect as a total one when nothing was
/// verified: the shards that were reachable held no in-flight work, but the
/// unreachable ones are exactly where the drift would have been.
#[tokio::test]
async fn an_empty_bundle_from_a_partial_export_is_not_licensed_either() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Partial,
        states: vec!["RUNNING".into()],
        per_workflow: vec![],
        sampled_total: 0,
        in_flight_total: 0,
        unavailable_shards: vec!["shard 1: connection refused".into()],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
    };
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .allow_empty_bundle(true)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.exit_code(),
        3,
        "a half-read fleet is not a proven-idle fleet: {report}"
    );
}

/// The complement, so the fix does not simply break the flag it is tightening:
/// a genuinely idle fleet — every shard read, nothing in flight — still exits 0
/// under the opt-out.
#[tokio::test]
async fn an_empty_bundle_with_complete_coverage_is_still_licensed() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Complete,
        states: vec!["RUNNING".into()],
        per_workflow: vec![],
        sampled_total: 0,
        in_flight_total: 0,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
    };
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .allow_empty_bundle(true)
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.emptiness_is_contradicted(),
        "complete coverage does not contradict an idle fleet"
    );
    assert_eq!(
        report.exit_code(),
        0,
        "a genuinely idle fleet must still pass: {report}"
    );
}

/// The scope boundary: a bundle with **no manifest** keeps the opt-out.
///
/// The exporter writes the manifest unconditionally — alongside the fixtures,
/// and even when it produced none (see `build_bundle_files`). So a
/// Harvest-produced bundle always carries one, and "no manifest" means the
/// bundle did not come from the exporter: a hand-assembled directory, or a gate
/// binary pointed at the wrong path. That is not the export-outage case, and
/// `require_complete_coverage(true)` is the lever for an operator who wants
/// absent evidence to fail closed.
///
/// This pins the boundary so a future tightening cannot silently swallow the
/// documented opt-out along with the bug.
#[tokio::test]
async fn an_empty_bundle_with_no_manifest_keeps_the_opt_out() {
    let dir = tempfile::tempdir().unwrap();
    let report = ReplayVerifier::new()
        .allow_empty_bundle(true)
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.emptiness_is_contradicted(),
        "no manifest is no contradiction — it is simply no evidence"
    );
    assert_eq!(
        report.exit_code(),
        0,
        "an absent manifest is not the export-outage case: {report}"
    );

    // ...and the operator who *does* want proof has a lever that fails closed.
    let demanded = ReplayVerifier::new()
        .allow_empty_bundle(true)
        .require_complete_coverage(true)
        .replay_bundle(dir.path())
        .await;
    assert_eq!(
        demanded.exit_code(),
        4,
        "require_complete_coverage must still fail closed on absent evidence: {demanded}"
    );
}

/// An in-flight-looking history whose activity input is an **erasure
/// tombstone** (issue #495) standing in for the real payload.
fn erased_snapshot_json(workflow_name: &str) -> String {
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            input: autumn_harvest::erase::erasure_tombstone(),
            queue: "default".into(),
        },
    ];
    snapshot_json(workflow_name, ExecutionId::new(), events)
}

/// The money test for the erasure guard (Codex round-16 P2).
///
/// A bundle acquires an erased fixture two ways, and the second needs no race:
/// the sample export selects an execution in flight and it completes and is
/// erased before the sequential per-candidate fetch reaches it; or the **batch**
/// export, which has no in-flight restriction, exports a terminal execution —
/// exactly the population erasure is allowed to touch.
///
/// Either way the payload is gone irreversibly, so replaying it compares the
/// candidate's real inputs against scrubbed placeholders. That is a false red
/// (drift that does not exist) or, worse, a clean result certifying nothing.
/// It must be a harness error (exit 2).
#[tokio::test]
async fn an_erased_fixture_blocks_the_gate_instead_of_reporting_drift() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "erased.json", &erased_snapshot_json("wf"));

    let report = ReplayVerifier::new()
        .register_fn("wf", payload_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.total, 1,
        "the fixture still counts in the denominator"
    );
    assert!(
        report.diverged.is_empty(),
        "an erased payload is NOT a determinism regression: {:?}",
        report.diverged
    );
    assert_eq!(
        report.blocked.len(),
        1,
        "expected a harness error: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "a bundle this gate cannot honestly evaluate must not read as drift: {report}"
    );

    let reason = report.blocked[0].reason.to_string();
    assert!(
        reason.contains("erased"),
        "the reason must name the cause: {reason}"
    );
    assert!(
        reason.contains("#495"),
        "the reason must point at the erasure feature: {reason}"
    );
}

/// The guard keys on a tombstone in a **payload field**, not on the string
/// appearing anywhere in the document. A workflow whose own data happens to
/// mention the marker is perfectly replayable, and blocking it would be a new
/// false red invented by the fix for the old one.
#[tokio::test]
async fn a_payload_that_merely_mentions_the_tombstone_key_still_replays() {
    let dir = tempfile::tempdir().unwrap();
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            // The key occurs as ordinary *data*, not as the payload field's
            // own object key — exactly what a workflow logging about erasure
            // would carry.
            input: serde_json::json!({ "note": "_harvest_erased is set by #495" }),
            queue: "default".into(),
        },
    ];
    write(
        dir.path(),
        "mentions.json",
        &snapshot_json("wf", ExecutionId::new(), events),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", payload_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.blocked.is_empty(),
        "a mere mention of the key is not an erased payload: {report}"
    );
}

/// A fixture whose payload carries the issue #608 **undecodable marker** — the
/// per-field graceful degrade a lossy decode writes when it *tried* and failed
/// (unknown codec, bad base64, codec error, invalid JSON).
///
/// Decoding was attempted and the plaintext is gone, so replaying it is just as
/// dishonest as replaying the raw envelope. Same treatment.
#[tokio::test]
async fn an_undecodable_fixture_blocks_the_gate_instead_of_reporting_drift() {
    let dir = tempfile::tempdir().unwrap();
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            input: serde_json::json!({
                "_harvest_undecodable": {"codec_id": "aes-gcm", "reason": "unknown_codec"},
            }),
            queue: "default".into(),
        },
    ];
    write(
        dir.path(),
        "undecodable.json",
        &snapshot_json("wf", ExecutionId::new(), events),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", payload_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.diverged.is_empty(),
        "a failed decode is NOT a determinism regression: {:?}",
        report.diverged
    );
    assert_eq!(
        report.blocked.len(),
        1,
        "expected a harness error: {report}"
    );
    assert_eq!(report.exit_code(), 2, "{report}");
}

/// The codec discriminator appearing inside ordinary payload *data* must not
/// block — the same false-positive guard the offload half already carries.
#[tokio::test]
async fn the_codec_key_appearing_as_plain_data_does_not_block() {
    let dir = tempfile::tempdir().unwrap();
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".into(),
            // The key is present as a *string value*, not an envelope object.
            input: serde_json::json!({"note": "we set _harvest_codec_envelope upstream"}),
            queue: "default".into(),
        },
    ];
    write(
        dir.path(),
        "mentions_codec.json",
        &snapshot_json("wf", ExecutionId::new(), events),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", mentions_codec_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.blocked.is_empty(),
        "a substring hit is not an envelope: {report}"
    );
    assert_eq!(report.succeeded, 1, "{report}");
    assert!(report.is_clean(), "{report}");
}

/// Companion to [`the_codec_key_appearing_as_plain_data_does_not_block`].
fn mentions_codec_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw(
            "step_one",
            serde_json::json!({"note": "we set _harvest_codec_envelope upstream"}),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Companion to [`the_envelope_key_appearing_as_plain_data_does_not_block`].
fn mentions_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw(
            "step_one",
            serde_json::json!({"note": "we set _harvest_offload_envelope upstream"}),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// ===========================================================================
// The candidate build id must reach every fixture context (Codex round-13 P1)
// ===========================================================================
//
// `ctx.build_id()` differs in kind from every other value this gate threads.
// `queue_name`, `workflow_id`, `parent_execution_id` and `context_headers` are
// all *recorded per-execution row values*, so the fixture carries them and the
// snapshot wins. `build_id` is not: the live worker supplies its **own
// configured** `WorkerConfig::build_id` through `WorkflowExecuteSpanMeta`, not
// the execution's recorded `assigned_build_id`.
//
// That inverts the correct source. Recording the *exporting* worker's build into
// the fixture and replaying under it would replay the **old** build's id — the
// precise value that hides the divergence this gate exists to find. The
// candidate build id is therefore a uniform, operator-supplied *replay-config*
// value, deliberately with no snapshot field.

/// A workflow that embeds its worker's build id in an activity input.
fn build_id_bearing_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let build = ctx.build_id().unwrap_or("<none>").to_string();
        ctx.execute_activity_raw("step_one", serde_json::json!({ "build": build }), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// The shape the finding actually describes: a **candidate-only branch**.
///
/// Build `v2` takes a new path; every other build takes the historical one. The
/// recorded history below was produced by `v1`, so replaying it under the
/// candidate `v2` *must* surface the divergence — that is the gate working.
/// With `build_id` stuck at `None` the candidate branch is unreachable, the old
/// path replays cleanly, and the gate reports **false GREEN** on code that will
/// diverge the moment it is promoted.
fn candidate_branching_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let step = if ctx.build_id() == Some("v2") {
            "step_two_v2"
        } else {
            "step_one"
        };
        ctx.execute_activity_raw(step, Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// An in-flight history recorded by a worker running build `v1`.
fn build_id_snapshot_json(workflow_name: &str, recorded_input: Value, activity: &str) -> String {
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: activity.into(),
            input: recorded_input,
            queue: "default".into(),
        },
    ];
    let snapshot = HistorySnapshot {
        workflow_name: workflow_name.to_string(),
        execution_id: ExecutionId::new(),
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

/// The money test: with the candidate build id threaded, a `v2`-only branch is
/// actually taken during the gate, so the drift it will cause after promotion is
/// caught **before** the deploy.
///
/// Without the fix this reports clean — the exact false GREEN the finding names.
#[tokio::test]
async fn a_candidate_only_branch_is_caught_when_the_build_id_is_threaded() {
    let dir = tempfile::tempdir().unwrap();
    // Recorded by build v1: it scheduled `step_one`.
    write(
        dir.path(),
        "a.json",
        &build_id_snapshot_json("wf", Value::Null, "step_one"),
    );

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", candidate_branching_workflow as WorkflowHandlerFn)
        .with_build_id("v2")
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.diverged.len(),
        1,
        "the candidate build's v2-only branch must be reachable during the gate, \
         or the gate false-GREENs on code that diverges after promotion: {report}"
    );
}

/// The control that keeps the money test honest: replayed as the *recorded*
/// build, the same fixture and the same candidate code replay cleanly — so the
/// divergence above is genuinely attributable to the candidate branch and not to
/// some unrelated mismatch in the fixture.
#[tokio::test]
async fn the_same_fixture_replays_clean_under_the_recorded_build() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &build_id_snapshot_json("wf", Value::Null, "step_one"),
    );

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", candidate_branching_workflow as WorkflowHandlerFn)
        .with_build_id("v1")
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "control: under the recorded build the candidate branch is not taken, so \
         the fixture must replay clean — otherwise the money test proves nothing \
         about the build id specifically: {report}"
    );
}

/// The value must reach the replay context verbatim, not merely flip a branch:
/// a workflow that embeds `ctx.build_id()` in an activity input replays clean
/// only when the configured build id matches what the recording worker reported.
#[tokio::test]
async fn the_configured_build_id_reaches_the_replayed_activity_input() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &build_id_snapshot_json("wf", serde_json::json!({"build": "v2"}), "step_one"),
    );

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", build_id_bearing_workflow as WorkflowHandlerFn)
        .with_build_id("v2")
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "the replayer's configured build id must reach the bundle walk's replay \
         context: {report}"
    );
}

/// Control for the test above: with no build id configured the context reports
/// `None`, the recorded input no longer matches, and the fixture diverges — so
/// the delegation genuinely carries the value.
#[tokio::test]
async fn an_unset_build_id_genuinely_diverges() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &build_id_snapshot_json("wf", serde_json::json!({"build": "v2"}), "step_one"),
    );

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", build_id_bearing_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.diverged.len(),
        1,
        "control: omitting the build id must genuinely change the outcome, else \
         the tests above would pass without the setting carrying anything: {report}"
    );
}

/// `ReplayVerifier` is the gate's own public surface — the CLI binary builds one
/// directly rather than going through `WorkflowReplayer::replay_bundle` — so the
/// candidate build id must be settable there too.
#[tokio::test]
async fn replay_verifier_carries_the_candidate_build_id() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &build_id_snapshot_json("wf", serde_json::json!({"build": "v2"}), "step_one"),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", build_id_bearing_workflow as WorkflowHandlerFn)
        .with_build_id("v2")
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "ReplayVerifier::with_build_id must reach the fixture replay context: {report}"
    );
}

// ---------------------------------------------------------------------------
// Candidate payload caps (Codex round 20)
// ---------------------------------------------------------------------------

/// Comfortably under the library default activity-input cap (2 MiB) and
/// comfortably over the candidate cap the tests below configure, so the two
/// configurations genuinely disagree about this one workflow.
const CANDIDATE_CAP_BYTES: u64 = 512 * 1024;
const PAYLOAD_BYTES: usize = 700 * 1024;

/// Schedules a single activity whose input sits between the candidate worker's
/// configured cap and the library default — i.e. accepted by one and rejected by
/// the other. The history stops at `WorkflowStarted`, so this dispatch happens
/// at the frontier, which is where an in-flight fixture actually exercises the
/// cap: an already-recorded activity matches history and never re-checks it.
fn large_input_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let payload = Value::String("x".repeat(PAYLOAD_BYTES));
        ctx.execute_activity_raw("big_step", payload, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// A candidate build that *lowers* a payload cap is the false-GREEN direction:
/// replay accepts the input under the library default, the promoted worker
/// rejects it with `PayloadTooLarge`, and the gate has certified a build that
/// will break the very executions it sampled.
#[tokio::test]
async fn candidate_activity_input_cap_reaches_the_replay_context() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &snapshot_json("wf", ExecutionId::new(), vec![started_event()]),
    );

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", large_input_workflow as WorkflowHandlerFn)
        .with_payload_caps(CANDIDATE_CAP_BYTES, 0, 0)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.diverged.len(),
        1,
        "the candidate's activity-input cap must be applied during replay, so a payload \
         the promoted worker would reject is reported rather than certified: {report}"
    );
}

/// Control: without the setting the same bundle replays clean, so the assertion
/// above is carried by the cap rather than by the fixture happening to fail.
#[tokio::test]
async fn omitting_the_candidate_cap_certifies_the_oversized_input() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &snapshot_json("wf", ExecutionId::new(), vec![started_event()]),
    );

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", large_input_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "control: under the library default cap this payload is accepted, so the test \
         above is proving the candidate cap carries — not that the fixture is broken: {report}"
    );
}

/// The offload threshold (#524) modulates the same decision: a worker that
/// offloads large payloads *skips* the cap, so a gate that knows the cap but not
/// the threshold reports drift the promoted worker would never hit — the
/// false-RED direction of the same gap.
#[tokio::test]
async fn candidate_offload_threshold_reaches_the_replay_context() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &snapshot_json("wf", ExecutionId::new(), vec![started_event()]),
    );

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("wf", large_input_workflow as WorkflowHandlerFn)
        .with_payload_caps(CANDIDATE_CAP_BYTES, 0, 0)
        .with_payload_offload_threshold(Some(CANDIDATE_CAP_BYTES / 2))
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "a payload above the offload threshold is offloaded rather than capped, so the \
         gate must not report drift the promoted worker would not hit: {report}"
    );
}

/// `ReplayVerifier` is the gate's own public surface — the embedder's gate binary
/// builds one directly — so the candidate payload limits must be settable there
/// too, exactly as the build id and history policy already are.
#[tokio::test]
async fn replay_verifier_carries_the_candidate_payload_caps() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &snapshot_json("wf", ExecutionId::new(), vec![started_event()]),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", large_input_workflow as WorkflowHandlerFn)
        .with_payload_caps(CANDIDATE_CAP_BYTES, 0, 0)
        .replay_bundle(dir.path())
        .await;

    assert_eq!(
        report.diverged.len(),
        1,
        "ReplayVerifier::with_payload_caps must reach the fixture replay context: {report}"
    );
}

// ---------------------------------------------------------------------------
// Codex round 21 — a present-but-unreadable manifest must block the gate
// ---------------------------------------------------------------------------

/// A manifest that is **present but unreadable** must block the *default* gate.
///
/// This is a different case from an absent manifest, and the distinction is the
/// whole fix. An **absent** manifest means the bundle did not come from the
/// exporter at all (a hand-assembled directory), which stays deliberately
/// supported — it simply carries no coverage claim. A manifest that **exists**
/// asserts the opposite: this bundle *is* an exporter product. If it cannot be
/// read, the claim it carried was damaged in transit, and every manifest-derived
/// guard silently evaporates:
///
/// * `sampled_total` — the fixture-count cross-check (rung `2`)
/// * `export_failures` / `truncated_by_size` — the export-shortfall check (rung `2`)
/// * `status` / `unavailable_shards` — the partial-shard check (rung `3`)
///
/// All three read through `Option::is_some_and`, so a `None` coverage turns each
/// into `false` and the surviving fixtures replay to a green exit `0`. That is a
/// release certified against an export whose own record of what it did was lost.
///
/// Rung `2` ("the gate could not fully run") is the right verdict rather than
/// rung `4`: rung `4` is gated on the opt-in `require_complete_coverage`, so a
/// default gate would still exit `0` — which is precisely the hole.
#[tokio::test]
async fn corrupt_manifest_blocks_the_default_gate() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    // A manifest truncated mid-transfer: present, well-named, unparseable.
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        "{\"generated_at\":\"2026-01-01T00:00:00Z\",\"status\":\"comp",
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.is_clean(),
        "a damaged coverage claim must not certify a release: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "an unreadable manifest means the gate could not fully run: {report}"
    );
    assert!(
        report.manifest_unreadable.is_some(),
        "the report must name the unreadable manifest: {report}"
    );
    // The failure must be legible in the rendered report, not just the code.
    let rendered = report.to_string();
    assert!(
        rendered.contains("manifest"),
        "the rendered report must explain the block: {rendered}"
    );
}

/// The absent-manifest path must stay fail-open — the fix must not turn every
/// hand-assembled bundle into a blocked gate.
///
/// This is the regression guard for the deliberate support decision the
/// unreadable-manifest fix sits next to: "no manifest" is a bundle that makes no
/// coverage claim, not a bundle whose claim was lost.
#[tokio::test]
async fn absent_manifest_still_replays_normally() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    // Deliberately no manifest written.

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "a bundle that makes no coverage claim still replays: {report}"
    );
    assert_eq!(report.exit_code(), 0);
    assert!(
        report.manifest_unreadable.is_none(),
        "an absent manifest is not an unreadable one: {report}"
    );
}

/// A well-formed manifest must not be mistaken for a damaged one.
#[tokio::test]
async fn readable_manifest_is_not_reported_as_unreadable() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.json", &in_flight_snapshot_json("wf"));
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&complete_manifest("wf", 1, 1)).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert!(report.is_clean(), "{report}");
    assert!(report.manifest_unreadable.is_none(), "{report}");
    assert!(report.coverage.is_some(), "{report}");
}

// ---------------------------------------------------------------------------
// Codex round 21 — replay must register the candidate's declarative handlers
// ---------------------------------------------------------------------------

/// A workflow that branches on whether a declarative query handler is
/// registered.
///
/// The live worker registers a workflow's declarative `#[query]` / `#[update]`
/// handlers *before any workflow code runs* (`executor.rs`, "Auto-register
/// declarative handlers before any workflow code runs"), and
/// `ctx.list_query_names()` merges the declarative map into its result. So this
/// branch is genuinely observable from workflow code — the same class as the
/// payload caps fixed in round 20: **candidate configuration that lives in no
/// `WorkflowEvent`**, which replay must mirror or it is not replaying the
/// candidate at all.
fn query_branching_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Branch on the candidate's registered declarative handlers.
        let step = if ctx.list_query_names().iter().any(|n| n == "progress") {
            "step_one"
        } else {
            "step_two"
        };
        ctx.execute_activity_raw(step, Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// The `Result` is not optional: this fn is stored as a `QueryHandlerFn`, whose
// signature the engine fixes. A handler that happens never to fail still has to
// match it.
#[allow(clippy::unnecessary_wraps)]
fn progress_query(_ctx: &WorkflowContext, _args: Value) -> Result<Value, String> {
    Ok(Value::String("running".into()))
}

fn progress_query_info() -> autumn_harvest::info::QueryHandlerInfo {
    autumn_harvest::info::QueryHandlerInfo {
        name: "progress",
        workflow: "wf",
        module: "replay_drift_tests",
        input_type_hint: "()",
        output_type_hint: "String",
        handler: progress_query,
        description: None,
        arg_schema: None,
        response_schema: None,
    }
}

/// The gate must replay with the candidate's declarative query handlers
/// registered, or it false-reports drift on unchanged code.
///
/// The recorded history was produced by a live worker that had `progress`
/// registered, so it scheduled `step_one`. Replaying the *same* workflow with an
/// empty declarative registry takes the `else` branch and schedules `step_two` —
/// reported as drift on a workflow nobody changed (a false RED that blocks a
/// good release).
#[tokio::test]
async fn candidate_declarative_query_handlers_are_registered_during_replay() {
    let dir = tempfile::tempdir().unwrap();
    // History recorded by a worker where `progress` WAS registered → step_one.
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
    write(dir.path(), "a.json", &snapshot_json("wf", exec_id, events));

    let report = ReplayVerifier::new()
        .register_fn("wf", query_branching_workflow)
        .queries(vec![progress_query_info()])
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "replay with the candidate's declarative handlers must not report drift \
         on unchanged code: {report}"
    );
    assert_eq!(report.exit_code(), 0, "{report}");
}

/// The mirror image: without the candidate's handlers, the same fixture takes
/// the other branch.
///
/// This is the falsifying control — it proves the test above is actually
/// exercising the registration and not passing for an unrelated reason. It also
/// pins the false-GREEN direction: a candidate that *removed* the registration
/// genuinely drifts, and the gate must still say so.
#[tokio::test]
async fn removing_a_declarative_handler_is_still_reported_as_drift() {
    let dir = tempfile::tempdir().unwrap();
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
    write(dir.path(), "a.json", &snapshot_json("wf", exec_id, events));

    // Candidate dropped the `progress` query → takes the `step_two` branch.
    let report = ReplayVerifier::new()
        .register_fn("wf", query_branching_workflow)
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.is_clean(),
        "dropping a declarative handler changes the command stream and must \
         surface as drift: {report}"
    );
    assert_eq!(report.exit_code(), 1, "{report}");
}

// ---------------------------------------------------------------------------
// Codex round 22 — per-workflow candidate config must not be applied bundle-wide
//
// One family, two members: configuration the live worker resolves *by workflow
// type* but which replay previously applied to every fixture uniformly.
//
//   1. declarative handlers — `worker.rs` narrows both registries with
//      `filter(|h| h.workflow == wf_name)` before building the context.
//   2. the workflow-input cap — `worker.rs` resolves
//      `workflow.max_input_bytes.map_or(global, |per| per.max(global))`.
//
// A gate is handed the candidate's *whole* registry, so applying it unfiltered
// replays a context the promoted worker will never construct.
// ---------------------------------------------------------------------------

/// The same `progress` query name, but declared on a *different* workflow type.
fn foreign_progress_query_info() -> autumn_harvest::info::QueryHandlerInfo {
    autumn_harvest::info::QueryHandlerInfo {
        name: "progress",
        // Note: NOT "wf" — this handler belongs to another workflow in the same
        // `queries![…]` collection, exactly as a real multi-workflow candidate
        // build would hand the gate.
        workflow: "other_wf",
        module: "replay_drift_tests",
        input_type_hint: "()",
        output_type_hint: "String",
        handler: progress_query,
        description: None,
        arg_schema: None,
        response_schema: None,
    }
}

/// A handler belonging to another workflow must not be registered on this one
/// (false-RED direction).
///
/// The recording worker filtered by workflow type, so `wf` never saw
/// `other_wf`'s `progress` query and scheduled `step_two`. Replaying with the
/// candidate's whole registry unfiltered puts that foreign handler on `wf`'s
/// context, flips the branch to `step_one`, and reports drift on a workflow
/// nobody changed — blocking a good release.
#[tokio::test]
async fn foreign_workflow_declarative_handler_is_not_registered_during_replay() {
    let dir = tempfile::tempdir().unwrap();
    // History from a worker that correctly filtered: `wf` saw no `progress`.
    let exec_id = ExecutionId::new();
    let events = vec![
        started_event(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
    ];
    write(dir.path(), "a.json", &snapshot_json("wf", exec_id, events));

    let report = ReplayVerifier::new()
        .register_fn("wf", query_branching_workflow)
        // The candidate's whole registry — this entry targets `other_wf`.
        .queries(vec![foreign_progress_query_info()])
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "a declarative handler declared on another workflow type must not be \
         registered on this one: {report}"
    );
    assert_eq!(report.exit_code(), 0, "{report}");
}

/// The false-GREEN direction, and the reason the filter is load-bearing rather
/// than cosmetic.
///
/// The candidate genuinely **dropped** `wf`'s own `progress` handler, but kept a
/// same-named one on `other_wf`. The promoted worker will filter, show `wf`
/// nothing, and take the `step_two` branch — a real drift from the recorded
/// `step_one`. Registering unfiltered lets the foreign handler stand in for the
/// deleted one, the replay still matches history, and the gate certifies a build
/// that changed behavior.
#[tokio::test]
async fn dropping_a_handler_is_still_drift_when_another_workflow_keeps_the_name() {
    let dir = tempfile::tempdir().unwrap();
    // History from the OLD build, where `wf` did have its own `progress`.
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
    write(dir.path(), "a.json", &snapshot_json("wf", exec_id, events));

    let report = ReplayVerifier::new()
        .register_fn("wf", query_branching_workflow)
        // Candidate dropped `wf`'s own handler; only `other_wf` keeps the name.
        .queries(vec![foreign_progress_query_info()])
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.is_clean(),
        "a same-named handler on another workflow must not mask the removal of \
         this workflow's own handler: {report}"
    );
    assert_eq!(report.exit_code(), 1, "{report}");
}

/// A workflow that records a large side-effect value, then parks on an activity.
///
/// `max_workflow_input` caps side-effect values as well as child-workflow inputs
/// (`WorkflowContext::with_payload_caps`), and the cap is only consulted at the
/// frontier — which is exactly where an in-flight fixture lands, so this is the
/// shape the gate actually samples.
fn big_side_effect_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _recorded: String = ctx
            .side_effect("big", || "x".repeat(4096))
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("step", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Build a `WorkflowInfo` carrying a per-workflow input-cap override.
///
/// Written as a literal rather than via `#[workflow]` so the test can set
/// `max_input_bytes` directly — the macro attribute and this field are the same
/// value, and the literal keeps the cap under test visible at the call site.
fn capped_wf_info(max_input_bytes: Option<u64>) -> autumn_harvest::info::WorkflowInfo {
    autumn_harvest::info::WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        name: "wf",
        module: "replay_drift_tests",
        handler: big_side_effect_workflow,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
        mcp: false,
    }
}

/// A `#[workflow(max_input_bytes = …)]` override must raise the cap for *that*
/// workflow during replay, exactly as the live worker raises it.
///
/// The worker resolves `per.max(global)` per workflow type. A gate that applies
/// only the global cap rejects a frontier dispatch the promoted worker will
/// happily accept — `PayloadTooLarge` reported as drift on a workflow nobody
/// changed.
#[tokio::test]
async fn per_workflow_input_cap_override_is_applied_during_replay() {
    let dir = tempfile::tempdir().unwrap();
    let exec_id = ExecutionId::new();
    // In-flight: parked with only the start event recorded, so the side effect
    // below is a frontier dispatch and the cap is genuinely consulted.
    write(
        dir.path(),
        "a.json",
        &snapshot_json("wf", exec_id, vec![started_event()]),
    );

    let report = ReplayVerifier::new()
        // Global cap far below the 4 KiB side-effect value...
        .with_payload_caps(1024, 1024, 1024)
        // ...raised for this workflow type by its own declared override.
        .register(vec![capped_wf_info(Some(1_000_000))])
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.is_clean(),
        "the workflow's own max_input_bytes override must raise the replay cap \
         the way the live worker does: {report}"
    );
    assert_eq!(report.exit_code(), 0, "{report}");
}

/// The falsifying control: with no declared override the global cap binds, and
/// the same fixture legitimately fails.
///
/// This proves the test above passes *because* the override is applied and not
/// for some unrelated reason — and pins the direction that must keep working: a
/// candidate whose global cap is genuinely too low is still caught.
#[tokio::test]
async fn without_an_override_the_global_input_cap_still_binds() {
    let dir = tempfile::tempdir().unwrap();
    let exec_id = ExecutionId::new();
    write(
        dir.path(),
        "a.json",
        &snapshot_json("wf", exec_id, vec![started_event()]),
    );

    let report = ReplayVerifier::new()
        .with_payload_caps(1024, 1024, 1024)
        // Same workflow, same fixture — only the override is gone.
        .register(vec![capped_wf_info(None)])
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.is_clean(),
        "with no per-workflow override the global cap must still bind: {report}"
    );
}

// ---------------------------------------------------------------------------
// Bundle ↔ manifest inventory reconciliation (round-23 P1)
// ---------------------------------------------------------------------------

/// An in-flight history for `workflow_name` under a caller-chosen execution id.
///
/// The identity tests below need to control which executions the bundle holds so
/// they can disagree with the manifest in a *count-preserving* way — the exact
/// corruption an aggregate comparison accepts.
fn in_flight_snapshot_with_id(workflow_name: &str, exec_id: ExecutionId) -> String {
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

/// A two-type manifest whose per-type entries carry the exported identities.
fn manifest_with_identities(entries: Vec<(&str, Vec<ExecutionId>)>) -> SampleManifest {
    let per_workflow: Vec<SampleWorkflowCoverage> = entries
        .into_iter()
        .map(|(name, ids)| SampleWorkflowCoverage {
            workflow_name: name.into(),
            sampled: ids.len() as u64,
            in_flight_total: ids.len() as u64,
            sampled_execution_ids: ids,
        })
        .collect();
    let sampled_total = per_workflow.iter().map(|entry| entry.sampled).sum();
    SampleManifest {
        generated_at: Utc::now(),
        status: SampleStatus::Complete,
        states: vec!["RUNNING".into()],
        per_workflow,
        sampled_total,
        in_flight_total: sampled_total,
        unavailable_shards: vec![],
        inspected_shards: vec![0],
        truncated_by_size: false,
        export_failures: 0,
    }
}

/// The money test for the count-preserving substitution across workflow types.
///
/// The manifest declares one fixture each for `wf_a` and `wf_b`. The delivered
/// bundle holds two files — but both are `wf_b`, and `wf_a` never made it. The
/// **total** still reads 2 of 2, so the aggregate guard is satisfied; both
/// `wf_b` fixtures replay cleanly, so no divergence is reported. Before the
/// per-type reconciliation this exits `0`: a green release gate that never
/// replayed a single `wf_a` history.
#[tokio::test]
async fn a_count_preserving_substitution_across_types_is_caught() {
    let dir = tempfile::tempdir().unwrap();
    let id_a = ExecutionId::new();
    let id_b = ExecutionId::new();

    // `wf_a`'s fixture was lost in transit; a duplicate `wf_b` took its place.
    write(
        dir.path(),
        "a.json",
        &in_flight_snapshot_with_id("wf_b", id_b),
    );
    write(
        dir.path(),
        "b.json",
        &in_flight_snapshot_with_id("wf_b", id_b),
    );
    let manifest = manifest_with_identities(vec![("wf_a", vec![id_a]), ("wf_b", vec![id_b])]);
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf_a", canonical_workflow as WorkflowHandlerFn)
        .register_fn("wf_b", canonical_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    // The aggregate guard is genuinely satisfied — that is the whole point.
    assert!(
        !report.fixture_count_disagrees_with_manifest(),
        "the totals must agree, or this test is not exercising the gap: {report}"
    );
    assert!(
        report.diverged.is_empty(),
        "both delivered fixtures replay cleanly: {report}"
    );

    assert_eq!(
        report.exit_code(),
        2,
        "a type the bundle never delivered must block the gate: {report}"
    );
    let mismatches = report.bundle_inventory_mismatches();
    let names: Vec<&str> = mismatches
        .iter()
        .map(|mismatch| mismatch.workflow_name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["wf_a", "wf_b"],
        "both the starved and the doubled type are disagreements: {report}"
    );
    let wf_a = &mismatches[0];
    assert_eq!((wf_a.declared, wf_a.found), (1, 0));
    assert_eq!(wf_a.missing_execution_ids, vec![id_a]);
}

/// The same corruption one level down: *within* a single type.
///
/// Per-type counts match, the total matches, and every delivered fixture
/// replays cleanly — only the identities differ. A gate that compared counts
/// alone (at any grain) would certify this.
#[tokio::test]
async fn a_count_preserving_substitution_within_one_type_is_caught() {
    let dir = tempfile::tempdir().unwrap();
    let kept = ExecutionId::new();
    let lost = ExecutionId::new();
    let substituted = ExecutionId::new();

    write(
        dir.path(),
        "a.json",
        &in_flight_snapshot_with_id("wf", kept),
    );
    write(
        dir.path(),
        "b.json",
        &in_flight_snapshot_with_id("wf", substituted),
    );
    let manifest = manifest_with_identities(vec![("wf", vec![kept, lost])]);
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.fixture_count_disagrees_with_manifest(),
        "totals agree: {report}"
    );
    let mismatches = report.bundle_inventory_mismatches();
    assert_eq!(mismatches.len(), 1, "one type disagrees: {report}");
    assert_eq!(
        (mismatches[0].declared, mismatches[0].found),
        (2, 2),
        "per-type counts agree too — identity is the only signal: {report}"
    );
    assert_eq!(mismatches[0].missing_execution_ids, vec![lost]);
    assert_eq!(mismatches[0].unexpected_execution_ids, vec![substituted]);
    assert_eq!(
        report.exit_code(),
        2,
        "an execution the bundle never delivered must block the gate: {report}"
    );
}

/// The falsifying control: a faithful bundle must still pass.
///
/// Without this, "report a mismatch unconditionally" would satisfy both tests
/// above while making the gate permanently red.
#[tokio::test]
async fn a_faithful_bundle_reconciles_against_the_manifest_identities() {
    let dir = tempfile::tempdir().unwrap();
    let first = ExecutionId::new();
    let second = ExecutionId::new();

    write(
        dir.path(),
        "a.json",
        &in_flight_snapshot_with_id("wf", first),
    );
    write(
        dir.path(),
        "b.json",
        &in_flight_snapshot_with_id("wf", second),
    );
    // Deliberately declared in the opposite order: the reconciliation compares
    // sets, not file-walk order.
    let manifest = manifest_with_identities(vec![("wf", vec![second, first])]);
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.bundle_inventory_mismatches().is_empty(),
        "a faithful bundle has no inventory disagreement: {report}"
    );
    assert!(report.is_clean(), "and the gate passes: {report}");
}

/// A pre-identity manifest makes *no* claim about which executions it holds.
///
/// Reading its empty `sampled_execution_ids` as a claim of *none* would report
/// every delivered fixture as unexpected and turn every older bundle red. Such
/// a manifest degrades to the per-type count check — still strictly stronger
/// than the total it replaced.
#[tokio::test]
async fn a_manifest_without_identities_falls_back_to_per_type_counts() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "a.json",
        &in_flight_snapshot_with_id("wf", ExecutionId::new()),
    );
    let mut manifest = manifest_with_identities(vec![("wf", vec![ExecutionId::new()])]);
    // Exactly what a manifest written before the field existed deserializes to.
    manifest.per_workflow[0].sampled_execution_ids.clear();
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf", canonical_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.bundle_inventory_mismatches().is_empty(),
        "counts agree and no identities were claimed: {report}"
    );
    assert!(report.is_clean(), "so the gate passes: {report}");
}

/// Per-type counts still bind when the manifest claims no identities.
///
/// The complement of the test above: the fallback must be a *fallback*, not an
/// exemption. A pre-identity manifest whose per-type counts disagree is still a
/// blocked gate, even though the total matches.
#[tokio::test]
async fn per_type_counts_bind_even_without_declared_identities() {
    let dir = tempfile::tempdir().unwrap();
    let id_b = ExecutionId::new();
    write(
        dir.path(),
        "a.json",
        &in_flight_snapshot_with_id("wf_b", id_b),
    );
    write(
        dir.path(),
        "b.json",
        &in_flight_snapshot_with_id("wf_b", id_b),
    );

    let mut manifest = manifest_with_identities(vec![
        ("wf_a", vec![ExecutionId::new()]),
        ("wf_b", vec![id_b]),
    ]);
    for entry in &mut manifest.per_workflow {
        entry.sampled_execution_ids.clear();
    }
    write(
        dir.path(),
        SampleManifest::FILE_NAME,
        &serde_json::to_string(&manifest).unwrap(),
    );

    let report = ReplayVerifier::new()
        .register_fn("wf_a", canonical_workflow as WorkflowHandlerFn)
        .register_fn("wf_b", canonical_workflow as WorkflowHandlerFn)
        .replay_bundle(dir.path())
        .await;

    assert!(
        !report.fixture_count_disagrees_with_manifest(),
        "totals agree: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "per-type counts must still bind without identities: {report}"
    );
}
