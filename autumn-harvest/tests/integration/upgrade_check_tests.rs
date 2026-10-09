#![cfg(feature = "testing")]
//! Upgrade-compatibility verdicts per in-flight run (issue #1995).
//!
//! No database. Each test builds a recorded history, a candidate handler and
//! two structure manifests, then asks for one verdict. One test per failure
//! mode must give a non-migrate verdict. The migrate cases come after them.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowContext;
use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{QueryHandlerInfo, SignalHandlerInfo, UpdateHandlerInfo};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::payload_store::{
    PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture,
};
use autumn_harvest::testing::HistorySnapshot;
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId, UpdateId};
use autumn_harvest::upgrade_check::{
    EncodedHistory, FindingKind, PendingSignal, RunVerdict, STRUCTURE_FORMAT, StructureManifest,
    UpgradeCheck, UpgradeReport, Verdict,
};
use chrono::Utc;
use serde_json::{Value, json};

pub const ORDER: &str = "order";
pub const SECRET: &str = "SSN-123-45-6789";

pub type WfFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

#[derive(serde::Deserialize)]
pub struct Reserved {
    amount: u64,
}

/// The candidate build: reserve, wait, ship.
pub fn order_wf(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let out = ctx
            .execute_activity_raw("reserve", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        let reserved: Reserved = serde_json::from_value(out).map_err(|e| e.to_string())?;
        ctx.timer("ship_wait", 60)
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("ship", json!(reserved.amount), "default")
            .await
            .map_err(|e| e.to_string())
    })
}

// ── histories ───────────────────────────────────────────────────────────────

pub fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

pub fn reserve_done(output: Value) -> Vec<WorkflowEvent> {
    let id = ActivityExecId::new();
    vec![
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "reserve".into(),
            input: json!({}),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output,
        },
    ]
}

/// The run has reserved and now waits on the `ship_wait` timer.
pub fn waiting_to_ship() -> Vec<WorkflowEvent> {
    let mut events = vec![started()];
    events.extend(reserve_done(json!({ "amount": 5 })));
    events.push(WorkflowEvent::TimerStarted {
        timer_id: TimerId::new("ship_wait"),
        duration_secs: 60,
    });
    events
}

/// The run waits on the `reserve` activity.
fn reserving() -> Vec<WorkflowEvent> {
    vec![
        started(),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "reserve".into(),
            input: json!({}),
            queue: "default".into(),
        },
    ]
}

fn snapshot(events: Vec<WorkflowEvent>) -> HistorySnapshot {
    HistorySnapshot {
        workflow_name: ORDER.into(),
        execution_id: ExecutionId::new(),
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
        queue_name: None,
    }
}

// ── structure manifests ─────────────────────────────────────────────────────

/// The `order` workflow graph, as `harvest-verify --emit-structure` writes it.
///
/// `changed` names the body ids whose digest differs in this build.
pub fn order_graph(changed: &[&str]) -> Value {
    let digest = |id: &str| {
        if changed.contains(&id) {
            format!("new-{id}")
        } else {
            format!("old-{id}")
        }
    };
    json!({
        "workflow": "shop::orders::order",
        "name": ORDER,
        "root": "orders::order::{closure#0}",
        "boundaries": [],
        "bodies": [
            {
                "id": "orders::order::{closure#0}",
                "digest": digest("orders::order::{closure#0}"),
                "calls": [
                    { "callee": "orders::reserve::{closure#0}", "in_loop": false },
                    { "callee": "orders::reserve::{closure#0}", "in_loop": false, "resume": true },
                    { "callee": "orders::ship::{closure#0}", "in_loop": false },
                    { "callee": "orders::ship::{closure#0}", "in_loop": false, "resume": true }
                ],
                "steps": []
            },
            {
                "id": "orders::reserve::{closure#0}",
                "digest": digest("orders::reserve::{closure#0}"),
                "calls": [],
                "steps": [
                    { "sink": "execute_activity_raw", "kind": "activity", "key": "reserve", "in_loop": false }
                ]
            },
            {
                "id": "orders::ship::{closure#0}",
                "digest": digest("orders::ship::{closure#0}"),
                "calls": [],
                "steps": [
                    { "sink": "timer", "kind": "timer", "key": "ship_wait", "in_loop": false },
                    { "sink": "execute_activity_raw", "kind": "activity", "key": "ship", "in_loop": false }
                ]
            }
        ]
    })
}

pub fn manifest(workflows: Vec<Value>) -> StructureManifest {
    let doc = json!({
        "format": STRUCTURE_FORMAT,
        "model_version": "2026.09.0",
        "rustc_version": "rustc test",
        "workflows": Value::Array(workflows),
    });
    StructureManifest::parse(&doc.to_string()).expect("a valid manifest")
}

fn check_with(baseline: Value, candidate: Value) -> UpgradeCheck {
    UpgradeCheck::new()
        .register_fn(ORDER, order_wf)
        .with_structure(manifest(vec![baseline]), manifest(vec![candidate]))
}

/// A check whose two builds have the same `order` graph.
fn unchanged_check() -> UpgradeCheck {
    check_with(order_graph(&[]), order_graph(&[]))
}

fn kinds(run: &RunVerdict) -> Vec<FindingKind> {
    run.findings.iter().map(|f| f.kind).collect()
}

// ── red: one test per failure mode ─────────────────────────────────────────

#[tokio::test]
async fn a_nondeterministic_change_is_pinned() {
    // The old build scheduled `charge` after `reserve`. The candidate starts
    // a timer there instead.
    let mut events = vec![started()];
    events.extend(reserve_done(json!({ "amount": 5 })));
    events.push(WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "charge".into(),
        input: json!(5),
        queue: "default".into(),
    });
    let run = unchanged_check().check_snapshot(snapshot(events)).await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert!(
        kinds(&run).contains(&FindingKind::Nondeterminism),
        "{run:#?}"
    );
    let finding = run
        .findings
        .iter()
        .find(|f| f.kind == FindingKind::Nondeterminism)
        .expect("a nondeterminism finding");
    assert_eq!(finding.event_index, Some(3), "{finding:#?}");
}

#[tokio::test]
async fn a_breaking_activity_output_type_is_pinned() {
    // The old build recorded `amount` as a string. The candidate reads a u64.
    let mut events = vec![started()];
    events.extend(reserve_done(json!({ "amount": "five" })));
    let run = unchanged_check().check_snapshot(snapshot(events)).await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::ReplayFailed], "{run:#?}");
}

fn approve_signal_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "by": { "type": "string" } },
        "required": ["by"]
    })
}

pub fn approve_signal() -> SignalHandlerInfo {
    SignalHandlerInfo {
        name: "approve",
        workflow: ORDER,
        module: module_path!(),
        arg_type_hint: "Approval",
        description: None,
        arg_schema: None,
    }
    .with_arg_schema_fn(approve_signal_schema)
}

fn approve(payload: Value) -> PendingSignal {
    PendingSignal {
        signal_name: "approve".into(),
        payload,
    }
}

#[tokio::test]
async fn a_buffered_signal_that_breaks_the_schema_is_pinned() {
    // The signal waits in `harvest_signals`. No replay reads it yet.
    let run = unchanged_check()
        .signals(vec![approve_signal()])
        .check_snapshot_with(snapshot(reserving()), &[approve(json!({ "by": 7 }))])
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(
        kinds(&run),
        [FindingKind::PayloadSchemaViolation],
        "{run:#?}"
    );
}

// `UpdateHandlerFn` takes the context `Arc` by value.
#[allow(clippy::needless_pass_by_value)]
fn rush_handler(
    _ctx: Arc<WorkflowContext>,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send>> {
    Box::pin(async { Ok(json!(true)) })
}

/// A candidate update handler with no schema.
fn rush_update() -> UpdateHandlerInfo {
    UpdateHandlerInfo {
        name: "rush",
        workflow: ORDER,
        module: module_path!(),
        input_type_hint: "Rush",
        output_type_hint: "bool",
        has_validator: false,
        handler: rush_handler,
        validator: None,
        mcp: false,
        description: None,
        arg_schema: None,
        response_schema: None,
    }
}

fn rush_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "days": { "type": "integer" } },
        "required": ["days"]
    })
}

#[tokio::test]
async fn a_recorded_update_that_breaks_the_schema_is_pinned() {
    let update_id = UpdateId::new();
    let mut events = waiting_to_ship();
    events.push(WorkflowEvent::UpdateAdmitted {
        update_id,
        name: "rush".into(),
        input: json!({ "days": "one" }),
        timestamp: Utc::now(),
    });
    events.push(WorkflowEvent::UpdateCompleted {
        update_id,
        output: json!(true),
    });
    let mut rush = rush_update();
    rush.arg_schema = Some(rush_schema);
    let run = unchanged_check()
        .updates(vec![rush])
        .check_snapshot(snapshot(events))
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    let violation = run
        .findings
        .iter()
        .find(|f| f.kind == FindingKind::PayloadSchemaViolation)
        .expect("a schema finding");
    assert_eq!(violation.event_index, Some(4));
    assert!(violation.detail.contains("update `rush`"), "{violation:#?}");
}

#[tokio::test]
async fn a_change_in_an_unreached_step_needs_review() {
    // `ship` changed, and the run has not reached it.
    let check = check_with(
        order_graph(&[]),
        order_graph(&["orders::ship::{closure#0}"]),
    );
    let run = check.check_snapshot(snapshot(reserving())).await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StepNotPassed], "{run:#?}");
    assert!(
        run.findings[0].detail.contains("orders::ship::{closure#0}"),
        "{run:#?}"
    );
}

// ── more non-migrate cases ──────────────────────────────────────────────────

#[tokio::test]
async fn a_pending_step_is_not_passed() {
    // `ship` changed. The run waits on its timer, inside `ship`.
    let check = check_with(
        order_graph(&[]),
        order_graph(&["orders::ship::{closure#0}"]),
    );
    let mut events = waiting_to_ship();
    events.push(WorkflowEvent::TimerFired {
        timer_id: TimerId::new("ship_wait"),
    });
    events.push(WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "ship".into(),
        input: json!(5),
        queue: "default".into(),
    });
    let run = check.check_snapshot(snapshot(events)).await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StepNotPassed]);
}

#[tokio::test]
async fn a_root_change_needs_review() {
    let check = check_with(
        order_graph(&[]),
        order_graph(&["orders::order::{closure#0}"]),
    );
    let run = check.check_snapshot(snapshot(waiting_to_ship())).await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::RootChanged]);
}

#[tokio::test]
async fn an_unknown_boundary_needs_review() {
    let mut candidate = order_graph(&[]);
    candidate["boundaries"] = json!(["external-crate-body: pricing::quote"]);
    let run = check_with(order_graph(&[]), candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::UnknownBoundary]);
}

#[tokio::test]
async fn a_missing_structure_needs_review() {
    let run = UpgradeCheck::new()
        .register_fn(ORDER, order_wf)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StructureUnavailable]);

    let mut other = order_graph(&[]);
    other["name"] = json!("other");
    let run = check_with(other.clone(), other)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(kinds(&run), [FindingKind::StructureUnavailable]);
}

#[tokio::test]
async fn an_unregistered_workflow_is_pinned() {
    let run = UpgradeCheck::new()
        .with_structure(
            manifest(vec![order_graph(&[])]),
            manifest(vec![order_graph(&[])]),
        )
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::WorkflowNotRegistered]);
}

#[tokio::test]
async fn a_pending_signal_with_no_candidate_schema_needs_review() {
    let run = unchanged_check()
        .check_snapshot_with(
            snapshot(waiting_to_ship()),
            &[approve(json!({ "by": "ops" }))],
        )
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::PayloadUnchecked]);
}

#[tokio::test]
async fn a_helper_called_in_a_loop_is_never_passed() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][0]["calls"][0]["in_loop"] = json!(true);
    let run = check_with(order_graph(&[]), candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StepNotPassed]);
}

#[tokio::test]
async fn a_step_key_used_outside_the_helper_is_never_passed() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][0]["steps"] = json!([
        { "sink": "execute_activity_raw", "kind": "activity", "key": "reserve", "in_loop": false }
    ]);
    let run = check_with(order_graph(&[]), candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StepNotPassed]);
}

#[tokio::test]
async fn a_helper_with_an_unknown_step_key_is_never_passed() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][1]["steps"][0]["key"] = Value::Null;
    let run = check_with(order_graph(&[]), candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StepNotPassed]);
}

#[tokio::test]
async fn a_helper_with_no_step_is_never_passed() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][1]["steps"] = json!([]);
    let run = check_with(order_graph(&[]), candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StepNotPassed]);
}

// ── review findings: each one gave a wrong migrate ─────────────────────────

/// `reserve` changed. The run finished its activity, but no decision has
/// run since, so the changed code after the activity has not run yet.
#[tokio::test]
async fn a_helper_whose_tail_has_not_run_needs_review() {
    let check = check_with(
        order_graph(&[]),
        order_graph(&["orders::reserve::{closure#0}"]),
    );
    let mut events = vec![started()];
    events.extend(reserve_done(json!({ "amount": 5 })));
    let run = check.check_snapshot(snapshot(events)).await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StepNotPassed]);
}

#[tokio::test]
async fn a_step_inside_a_loop_is_never_passed() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][1]["steps"][0]["in_loop"] = json!(true);
    let run = check_with(order_graph(&[]), candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
}

#[tokio::test]
async fn a_step_that_runs_twice_needs_two_completions() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][1]["steps"] = json!([
        { "sink": "execute_activity_raw", "kind": "activity", "key": "reserve", "in_loop": false },
        { "sink": "execute_activity_raw", "kind": "activity", "key": "reserve", "in_loop": false }
    ]);
    let run = check_with(order_graph(&[]), candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
}

/// The root and `reserve` both call `common`, which emits `reserve`. The
/// root's own call can complete the key before `reserve` starts.
#[tokio::test]
async fn a_callee_shared_with_the_root_is_never_passed() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][0]["calls"]
        .as_array_mut()
        .expect("calls")
        .push(json!({ "callee": "orders::common", "in_loop": false }));
    candidate["bodies"][1]["steps"] = json!([]);
    candidate["bodies"][1]["calls"] = json!([{ "callee": "orders::common", "in_loop": false }]);
    candidate["bodies"].as_array_mut().expect("bodies").push(json!({
        "id": "orders::common",
        "digest": "old-common",
        "calls": [],
        "steps": [
            { "sink": "execute_activity_raw", "kind": "activity", "key": "reserve", "in_loop": false }
        ]
    }));
    let mut baseline = candidate.clone();
    baseline["bodies"][1]["digest"] = json!("old-orders::reserve::{closure#0}");
    let run = check_with(baseline, candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
}

#[tokio::test]
async fn an_unknown_key_outside_the_helper_counts_as_shared() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][0]["steps"] = json!([
        { "sink": "execute_activity_raw", "kind": "activity", "key": null, "in_loop": false }
    ]);
    let mut baseline = candidate.clone();
    baseline["bodies"][1]["digest"] = json!("old-orders::reserve::{closure#0}");
    let run = check_with(baseline, candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
}

#[tokio::test]
async fn a_helper_that_can_park_without_a_command_is_never_passed() {
    let mut candidate = order_graph(&["orders::reserve::{closure#0}"]);
    candidate["bodies"][1]["steps"]
        .as_array_mut()
        .expect("steps")
        .push(json!({ "sink": "await_condition", "kind": "other", "key": null, "in_loop": false }));
    let run = check_with(order_graph(&[]), candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
}

/// The baseline `reserve` parks in `await_condition` after its activity. The
/// candidate drops that wait, so its own graph alone looks passed. The run
/// can still be parked in the baseline wait.
#[tokio::test]
async fn a_wait_that_only_the_baseline_helper_holds_needs_review() {
    let mut baseline = order_graph(&[]);
    baseline["bodies"][1]["steps"]
        .as_array_mut()
        .expect("steps")
        .push(json!({ "sink": "await_condition", "kind": "other", "key": null, "in_loop": false }));
    let candidate = order_graph(&["orders::reserve::{closure#0}"]);
    let run = check_with(baseline, candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::StepNotPassed]);
}

/// A signal the worker wrote into history while the run waits on its timer.
/// The workflow has not read it yet, so replay never decodes it.
#[tokio::test]
async fn a_recorded_signal_with_no_candidate_schema_is_unchecked() {
    let mut events = waiting_to_ship();
    events.push(WorkflowEvent::SignalReceived {
        signal_name: "approve".into(),
        payload: json!({ "by": 7 }),
    });
    let run = unchanged_check().check_snapshot(snapshot(events)).await;
    assert_ne!(run.verdict, Verdict::Migrate, "{run:#?}");
    let unchecked = run
        .findings
        .iter()
        .find(|f| f.kind == FindingKind::PayloadUnchecked)
        .expect("an unchecked finding");
    assert_eq!(unchecked.event_index, Some(4));
}

#[tokio::test]
async fn an_open_update_with_no_candidate_schema_is_unchecked() {
    let mut events = waiting_to_ship();
    events.push(WorkflowEvent::UpdateAdmitted {
        update_id: UpdateId::new(),
        name: "rush".into(),
        input: json!({ "days": 1 }),
        timestamp: Utc::now(),
    });
    let run = unchanged_check()
        .updates(vec![rush_update()])
        .check_snapshot(snapshot(events))
        .await;
    assert_ne!(run.verdict, Verdict::Migrate, "{run:#?}");
    assert!(
        kinds(&run).contains(&FindingKind::PayloadUnchecked),
        "{run:#?}"
    );
}

#[tokio::test]
async fn an_offloaded_payload_with_no_offloader_needs_review() {
    let mut events = vec![started()];
    events.extend(reserve_done(json!({
        "_harvest_offload_envelope": 1,
        "store": "s3",
        "key": "k",
        "size": 9,
        "sha256": "00"
    })));
    // The stub breaks the candidate type. Replay over it would give a false
    // pin, so the check stops at the review-level finding.
    let run = unchanged_check().check_snapshot(snapshot(events)).await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::PayloadOffloaded]);
}

#[tokio::test]
async fn a_pending_signal_is_checked_when_history_is_offloaded() {
    let mut events = vec![started()];
    events.extend(reserve_done(json!({
        "_harvest_offload_envelope": 1,
        "store": "s3",
        "key": "k",
        "size": 9,
        "sha256": "00"
    })));
    // History holds a stub, but a pending signal is never offloaded.
    let run = unchanged_check()
        .signals(vec![approve_signal()])
        .check_snapshot_with(snapshot(events), &[approve(json!({ "by": 7 }))])
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(
        kinds(&run),
        [
            FindingKind::PayloadOffloaded,
            FindingKind::PayloadSchemaViolation
        ],
        "{run:#?}"
    );
}

/// Business data can hold a nested value shaped like a claim check. The
/// offloader replaces only direct payload fields, so it is not offloaded.
#[tokio::test]
async fn a_nested_claim_check_shape_is_business_data() {
    let mut events = vec![started()];
    events.extend(reserve_done(json!({
        "amount": 5,
        "note": { "_harvest_offload_envelope": 1, "store_id": "s3", "key": "k" }
    })));
    let run = unchanged_check().check_snapshot(snapshot(events)).await;
    assert!(
        !kinds(&run).contains(&FindingKind::PayloadOffloaded),
        "{run:#?}"
    );
}

#[tokio::test]
async fn manifests_from_two_toolchains_need_review() {
    let baseline = manifest(vec![order_graph(&[])]);
    let mut candidate = manifest(vec![order_graph(&[])]);
    candidate.rustc_version = "rustc other".into();
    let run = UpgradeCheck::new()
        .register_fn(ORDER, order_wf)
        .with_structure(baseline, candidate)
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(kinds(&run), [FindingKind::StructureUnavailable]);
}

#[tokio::test]
async fn two_workflows_with_one_name_need_review() {
    let twice = || manifest(vec![order_graph(&[]), order_graph(&[])]);
    let run = UpgradeCheck::new()
        .register_fn(ORDER, order_wf)
        .with_structure(twice(), twice())
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(kinds(&run), [FindingKind::StructureUnavailable]);
}

#[tokio::test]
async fn declarative_update_handlers_need_review() {
    let run = unchanged_check()
        .updates(vec![rush_update()])
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::UnknownBoundary]);
}

// ── green: the migrate cases ────────────────────────────────────────────────

#[tokio::test]
async fn a_change_confined_to_activities_migrates() {
    // An activity body is not in the workflow graph, so both manifests
    // are the same. `tests/structure.rs` in `autumn-harvest-verify`
    // proves that half.
    let run = unchanged_check()
        .check_snapshot(snapshot(waiting_to_ship()))
        .await;
    assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
    assert!(run.findings.is_empty(), "{run:#?}");
}

#[tokio::test]
async fn a_change_confined_to_a_passed_step_migrates() {
    // `reserve` changed, and the run finished it.
    let check = check_with(
        order_graph(&[]),
        order_graph(&["orders::reserve::{closure#0}"]),
    );
    let run = check.check_snapshot(snapshot(waiting_to_ship())).await;
    assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
}

#[tokio::test]
async fn a_pending_signal_that_meets_the_candidate_schema_migrates() {
    let run = unchanged_check()
        .signals(vec![approve_signal()])
        .check_snapshot_with(
            snapshot(waiting_to_ship()),
            &[approve(json!({ "by": "ops" }))],
        )
        .await;
    assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
}

// ── codec: checked in memory, nothing exported ──────────────────────────────

pub fn aead_codecs() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let key = DataKey::from_bytes(&[0x42; 32]).expect("data key");
    AeadCodec::new("uc-k1", &key)
        .expect("aead codec")
        .register_with(&codecs)
        .expect("register");
    codecs
}

fn encoded(codecs: &PayloadCodecs, events: &[WorkflowEvent]) -> EncodedHistory {
    let event_data: Vec<Value> = events
        .iter()
        .map(|e| codecs.encode_event(e).expect("encode"))
        .collect();
    EncodedHistory {
        workflow_name: ORDER.into(),
        execution_id: ExecutionId::new(),
        event_data,
        pending_signals: Vec::new(),
    }
}

/// A payload store in memory, for the offload tests.
#[derive(Default)]
struct MemStore(std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>);

impl PayloadStore for MemStore {
    fn store_id(&self) -> &'static str {
        "mem"
    }
    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key = {
            let mut blobs = self.0.lock().expect("lock");
            let key = format!("k{}", blobs.len());
            blobs.insert(key.clone(), bytes.to_vec());
            key
        };
        Box::pin(async move { Ok(key) })
    }
    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let found = self.0.lock().expect("lock").get(key).cloned();
        Box::pin(async move { found.ok_or_else(|| PayloadStoreError("missing".into())) })
    }
    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        self.0.lock().expect("lock").remove(key);
        Box::pin(async move { Ok(()) })
    }
}

/// An encrypted, offloaded history: the worker encodes each event, then
/// offloads its payloads. The check must inflate first, then decode.
#[tokio::test]
async fn an_encrypted_offloaded_history_is_inflated_then_decoded() {
    let codecs = aead_codecs();
    let offloader = Arc::new(PayloadOffloader::new(
        Arc::new(MemStore::default()),
        0,
        Arc::new(autumn_harvest::telemetry::NoOpMetrics),
    ));
    let mut events = vec![started()];
    events.extend(reserve_done(json!({ "amount": 5 })));
    let mut history = encoded(&codecs, &events);
    for value in &mut history.event_data {
        offloader.offload_event_value(value).await.expect("offload");
    }
    let stored = serde_json::to_string(&history.event_data).expect("json");
    assert!(stored.contains("_harvest_offload_envelope"), "{stored}");
    let run = unchanged_check()
        .with_codecs(Arc::new(codecs))
        .with_offloader(offloader)
        .check_encoded(history)
        .await;
    assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
}

#[tokio::test]
async fn an_encrypted_history_is_checked_in_memory() {
    let codecs = aead_codecs();
    // The secret breaks the candidate type, so the serde error quotes it.
    let mut events = vec![started()];
    events.extend(reserve_done(json!({ "amount": SECRET })));
    let history = encoded(&codecs, &events);
    let stored = serde_json::to_string(&history.event_data).expect("json");
    assert!(!stored.contains(SECRET), "the stored rows are ciphertext");

    let check = unchanged_check().with_codecs(Arc::new(codecs));
    let run = check.check_encoded(history).await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::ReplayFailed]);

    let report = UpgradeReport::from_runs(vec![run]);
    assert!(!report.to_json().contains(SECRET), "{}", report.to_json());
    assert!(!report.render_text().contains(SECRET));
    assert!(!format!("{report:?}").contains(SECRET));
}

#[tokio::test]
async fn an_encrypted_history_that_fits_migrates() {
    let codecs = aead_codecs();
    let history = encoded(&codecs, &waiting_to_ship());
    let run = unchanged_check()
        .with_codecs(Arc::new(codecs))
        .check_encoded(history)
        .await;
    assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
}

#[tokio::test]
async fn an_encrypted_pending_signal_is_checked_in_memory() {
    let codecs = aead_codecs();
    let mut history = encoded(&codecs, &waiting_to_ship());
    let payload = codecs
        .encode_payload(&json!({ "by": SECRET.len() }))
        .expect("encode");
    history.pending_signals.push(approve(payload));
    let run = unchanged_check()
        .signals(vec![approve_signal()])
        .with_codecs(Arc::new(codecs))
        .check_encoded(history)
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::PayloadSchemaViolation]);
}

#[tokio::test]
async fn a_history_the_candidate_cannot_decode_is_pinned() {
    let history = encoded(&aead_codecs(), &waiting_to_ship());
    // The candidate build has no key for these rows.
    let run = unchanged_check()
        .with_codecs(Arc::new(PayloadCodecs::default()))
        .check_encoded(history)
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::HistoryUndecodable]);
}

// ── the report ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_report_gives_one_verdict_per_run_and_an_exit_code() {
    let check = check_with(
        order_graph(&[]),
        order_graph(&["orders::ship::{closure#0}"]),
    );
    let mut bad = vec![started()];
    bad.extend(reserve_done(json!({ "amount": "five" })));
    let runs = vec![
        check.check_snapshot(snapshot(waiting_to_ship())).await,
        check.check_snapshot(snapshot(bad)).await,
    ];
    let ids: Vec<String> = runs.iter().map(|r| r.execution_id.to_string()).collect();
    let report = UpgradeReport::from_runs(runs);
    assert_eq!((report.migrate, report.review, report.pin), (0, 1, 1));
    assert_eq!(report.exit_code(), 1);

    let text = report.render_text();
    for id in &ids {
        assert_eq!(
            text.lines().filter(|l| l.contains(id.as_str())).count(),
            1,
            "{text}"
        );
    }
    assert!(text.contains("review"), "{text}");
    assert!(text.contains("pin"), "{text}");

    let json: Value = serde_json::from_str(&report.to_json()).expect("json");
    assert_eq!(json["runs"].as_array().map(Vec::len), Some(2));
    assert_eq!(json["runs"][0]["verdict"], "review");
    assert_eq!(json["runs"][1]["verdict"], "pin");
}

#[test]
fn every_run_migrating_exits_zero_and_an_incomplete_check_exits_two() {
    let report = UpgradeReport::from_runs(Vec::new());
    assert_eq!(report.exit_code(), 0);
    let mut incomplete = UpgradeReport::from_runs(Vec::new());
    incomplete.incomplete.push("shard 1: unavailable".into());
    assert_eq!(incomplete.exit_code(), 2);
}

#[test]
fn a_manifest_with_another_format_is_refused() {
    let doc = json!({
        "format": "harvest-structure/99",
        "model_version": "x",
        "rustc_version": "y",
        "workflows": []
    });
    assert!(StructureManifest::parse(&doc.to_string()).is_err());
}

#[test]
fn verdicts_order_migrate_review_pin() {
    assert!(Verdict::Migrate < Verdict::Review);
    assert!(Verdict::Review < Verdict::Pin);
    assert_eq!(FindingKind::StepNotPassed.verdict(), Verdict::Review);
    assert_eq!(FindingKind::Nondeterminism.verdict(), Verdict::Pin);
}

// ── the candidate worker's configuration ────────────────────────────────────

// `QueryHandlerFn` takes the input by value and returns a `Result`.
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn status_handler(_ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    Ok(json!("open"))
}

/// A candidate query handler.
fn status_query() -> QueryHandlerInfo {
    QueryHandlerInfo {
        name: "status",
        workflow: ORDER,
        module: module_path!(),
        input_type_hint: "()",
        output_type_hint: "String",
        handler: status_handler,
        description: None,
        arg_schema: None,
        response_schema: None,
    }
}

/// The candidate takes another first step when it has no `status` query.
pub fn order_by_queries_wf(ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        if !ctx.list_query_names().iter().any(|name| name == "status") {
            return ctx
                .execute_activity_raw("legacy", json!({}), "default")
                .await
                .map_err(|e| e.to_string());
        }
        order_wf(ctx, input).await
    })
}

#[tokio::test]
async fn the_replay_registers_the_candidate_queries() {
    // The worker registers each query before workflow code runs. A replay
    // without them takes the other branch.
    let check = || {
        UpgradeCheck::new()
            .register_fn(ORDER, order_by_queries_wf)
            .with_structure(
                manifest(vec![order_graph(&[])]),
                manifest(vec![order_graph(&[])]),
            )
    };
    let blind = check().check_snapshot(snapshot(reserving())).await;
    assert_eq!(blind.verdict, Verdict::Pin, "{blind:#?}");
    let run = check()
        .queries(vec![status_query()])
        .check_snapshot(snapshot(reserving()))
        .await;
    assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
}

#[tokio::test]
async fn a_next_input_over_the_candidate_cap_is_pinned() {
    // The run has not dispatched `reserve` yet. Its input `{}` is two bytes,
    // and the candidate caps an activity input at one.
    let run = unchanged_check()
        .with_payload_caps(1, 0, 0)
        .check_snapshot(snapshot(vec![started()]))
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::ReplayFailed]);
}

#[tokio::test]
async fn a_next_input_under_the_candidate_cap_migrates() {
    let run = unchanged_check()
        .with_payload_caps(2, 0, 0)
        .check_snapshot(snapshot(vec![started()]))
        .await;
    assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
}

#[tokio::test]
async fn a_next_input_the_candidate_offloads_migrates() {
    // The input is over the cap and over the offload threshold. The worker
    // offloads it, so the cap does not apply.
    let offloader = Arc::new(PayloadOffloader::new(
        Arc::new(MemStore::default()),
        0,
        Arc::new(autumn_harvest::telemetry::NoOpMetrics),
    ));
    let run = unchanged_check()
        .with_payload_caps(1, 0, 0)
        .with_offloader(offloader)
        .check_snapshot(snapshot(vec![started()]))
        .await;
    assert_eq!(run.verdict, Verdict::Migrate, "{run:#?}");
}

/// The candidate checkpoints first when `should_continue_as_new` says so.
pub fn order_by_policy_wf(ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        if ctx.should_continue_as_new() {
            return ctx
                .execute_activity_raw("checkpoint", json!({}), "default")
                .await
                .map_err(|e| e.to_string());
        }
        order_wf(ctx, input).await
    })
}

/// The candidate takes another first step on build `v2`.
pub fn order_by_build_wf(ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        if ctx.build_id() == Some("v2") {
            return ctx
                .execute_activity_raw("checkpoint", json!({}), "default")
                .await
                .map_err(|e| e.to_string());
        }
        order_wf(ctx, input).await
    })
}

fn check_of(workflow: autumn_harvest::info::WorkflowHandlerFn) -> UpgradeCheck {
    UpgradeCheck::new()
        .register_fn(ORDER, workflow)
        .with_structure(
            manifest(vec![order_graph(&[])]),
            manifest(vec![order_graph(&[])]),
        )
}

#[tokio::test]
async fn the_replay_uses_the_candidate_history_policy() {
    // The run has two events. Under a threshold of one, the candidate
    // checkpoints instead of the recorded `reserve`.
    let blind = check_of(order_by_policy_wf)
        .check_snapshot(snapshot(reserving()))
        .await;
    assert_eq!(blind.verdict, Verdict::Migrate, "{blind:#?}");
    let policy =
        autumn_harvest::context::WorkflowHistoryPolicy::default().with_continue_as_new_threshold(1);
    let run = check_of(order_by_policy_wf)
        .with_history_policy(policy)
        .check_snapshot(snapshot(reserving()))
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::Nondeterminism]);
}

#[tokio::test]
async fn the_replay_uses_the_candidate_build_id() {
    let blind = check_of(order_by_build_wf)
        .check_snapshot(snapshot(reserving()))
        .await;
    assert_eq!(blind.verdict, Verdict::Migrate, "{blind:#?}");
    let run = check_of(order_by_build_wf)
        .with_build_id("v2")
        .check_snapshot(snapshot(reserving()))
        .await;
    assert_eq!(run.verdict, Verdict::Pin, "{run:#?}");
    assert_eq!(kinds(&run), [FindingKind::Nondeterminism]);
}
