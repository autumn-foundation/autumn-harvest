//! Tests for `#[dag]` lowering onto the workflow execution path (issue #256, Step 1).
//!
//! Requires features: `unified-dag-execution` + `testing`.
//!
//! These tests verify that when `unified-dag-execution` is enabled, `#[dag]`
//! emits a shadow `__autumn_workflow_info_{name}()` companion whose handler
//! walks the `DagDefinition` level by level, dispatches activities through
//! `ctx.execute_activity_raw`, and evaluates trigger rules deterministically.

#![allow(clippy::unused_async)]

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::ActivityExecId;
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Shared activity stubs
// ---------------------------------------------------------------------------

#[activity]
async fn extract_users(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[activity]
async fn load_users(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[activity]
async fn notify_complete(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

// ---------------------------------------------------------------------------
// DAG definitions used across tests
// ---------------------------------------------------------------------------

/// A linear two-task DAG: `extract_users` → `load_users` (`AllSuccess`, default).
#[dag(default_queue = "etl-workers")]
fn linear_dag(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_users);
    let _load = dag.activity(load_users).upstream(&extract);
}

/// Same topology but with `AllDone` trigger on the second task.
#[dag]
fn alldone_dag(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_users);
    let _load = dag
        .activity(load_users)
        .upstream(&extract)
        .trigger_rule(TriggerRule::AllDone);
}

/// Fan-out/fan-in: extract → [`load_users`, `notify_complete`] (both depend on extract).
#[dag]
fn fanout_dag(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_users);
    let _load = dag.activity(load_users).upstream(&extract);
    let _notify = dag.activity(notify_complete).upstream(&extract);
}

#[dag]
fn manual_root_dag(dag: &mut DagBuilder) {
    let _root = dag
        .activity(extract_users)
        .trigger_rule(TriggerRule::Manual);
}

#[dag]
fn one_failed_root_dag(dag: &mut DagBuilder) {
    let _root = dag
        .activity(extract_users)
        .trigger_rule(TriggerRule::OneFailed);
}

#[dag]
fn all_failed_root_dag(dag: &mut DagBuilder) {
    let _root = dag
        .activity(extract_users)
        .trigger_rule(TriggerRule::AllFailed);
}

// ---------------------------------------------------------------------------
// RED-PHASE TEST 1 — macro emits the workflow companion
// ---------------------------------------------------------------------------

/// `#[dag]` must emit `__autumn_workflow_info_{name}()` returning `WorkflowInfo`
/// with the DAG's function name when `unified-dag-execution` is enabled.
#[test]
fn dag_macro_emits_workflow_info_companion() {
    let info = __autumn_workflow_info_linear_dag();
    assert_eq!(info.name, "linear_dag");
    assert!(!info.module.is_empty(), "module path should be non-empty");
}

// ---------------------------------------------------------------------------
// RED-PHASE TEST 2 — DagInfo backward compat is preserved
// ---------------------------------------------------------------------------

/// Enabling `unified-dag-execution` must not break the existing `DagInfo`
/// companion or `build_definition()`.
#[test]
fn dag_info_backward_compat_still_works() {
    let dag_info = __autumn_dag_info_linear_dag();
    assert_eq!(dag_info.name, "linear_dag");
    assert_eq!(dag_info.default_queue, Some("etl-workers"));

    let definition = dag_info
        .build_definition()
        .expect("definition should build");
    assert_eq!(definition.tasks().len(), 2);
    assert_eq!(definition.execution_levels().len(), 2);
}

// ---------------------------------------------------------------------------
// RED-PHASE TEST 3 — lowered handler replays a fully-successful linear DAG
// ---------------------------------------------------------------------------

/// The lowered workflow handler must walk both execution levels, call
/// `execute_activity_raw` for each task, and complete without divergence when
/// the history records successful completions for both activities.
#[tokio::test]
async fn lowered_handler_replays_linear_dag_all_success() {
    let id_extract = ActivityExecId::new();
    let id_load = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: Value::Null,
            queue: "etl-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_extract,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_load,
            name: "load_users".into(),
            input: Value::Null,
            queue: "etl-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_load,
            output: Value::Null,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("linear_dag", __autumn_workflow_info_linear_dag().handler)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "linear DAG all-success replay should succeed, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// RED-PHASE TEST 4 — AllSuccess trigger skips downstream on upstream failure
// ---------------------------------------------------------------------------

/// When the upstream fails and the downstream has `TriggerRule::AllSuccess`
/// (the default), the lowered handler must skip the downstream task entirely.
/// The history therefore contains no `ActivityScheduled` for `load_users`, and
/// replay must consume exactly that history before returning the expected DAG
/// workflow failure.
#[tokio::test]
async fn lowered_handler_skips_all_success_downstream_on_upstream_failure() {
    let id_extract = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: Value::Null,
            queue: "etl-workers".into(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: id_extract,
            error: "db connection refused".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
        // No ActivityScheduled for load_users — it is skipped by AllSuccess rule.
    ];

    let expected_events = history.len();
    let report = WorkflowReplayer::new()
        .register_fn("linear_dag", __autumn_workflow_info_linear_dag().handler)
        .replay_from_events(history)
        .await;

    assert_eq!(report.events_replayed, expected_events);
    assert!(
        matches!(
            report.status,
            ReplayStatus::WorkflowFailed { ref error, .. }
                if error == "one or more DAG tasks failed"
        ),
        "AllSuccess skip should replay deterministically then fail the DAG run, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// RED-PHASE TEST 5 — AllDone trigger runs downstream even on upstream failure
// ---------------------------------------------------------------------------

/// When the upstream fails and the downstream has `TriggerRule::AllDone`,
/// the lowered handler must still schedule the downstream activity.
/// The history must include both the upstream failure and the downstream
/// completion, and replay must consume both before returning the expected DAG
/// workflow failure.
#[tokio::test]
async fn lowered_handler_runs_alldone_downstream_on_upstream_failure() {
    let id_extract = ActivityExecId::new();
    let id_load = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: id_extract,
            error: "db error".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
        // AllDone: load_users runs regardless of extract_users outcome.
        WorkflowEvent::ActivityScheduled {
            activity_id: id_load,
            name: "load_users".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_load,
            output: Value::Null,
        },
    ];

    let expected_events = history.len();
    let report = WorkflowReplayer::new()
        .register_fn("alldone_dag", __autumn_workflow_info_alldone_dag().handler)
        .replay_from_events(history)
        .await;

    assert_eq!(report.events_replayed, expected_events);
    assert!(
        matches!(
            report.status,
            ReplayStatus::WorkflowFailed { ref error, .. }
                if error == "one or more DAG tasks failed"
        ),
        "AllDone downstream should replay deterministically then fail the DAG run, got: {report}"
    );
}

async fn assert_root_trigger_rule_skips_without_upstreams(
    dag_name: &str,
    handler: WorkflowHandlerFn,
) {
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
    }];

    let report = WorkflowReplayer::new()
        .register_fn(dag_name, handler)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{dag_name} root trigger rule should skip with no upstreams, got: {report}"
    );
}

#[tokio::test]
async fn lowered_handler_applies_manual_trigger_rule_to_root_task() {
    assert_root_trigger_rule_skips_without_upstreams(
        "manual_root_dag",
        __autumn_workflow_info_manual_root_dag().handler,
    )
    .await;
}

#[tokio::test]
async fn lowered_handler_applies_one_failed_trigger_rule_to_root_task() {
    assert_root_trigger_rule_skips_without_upstreams(
        "one_failed_root_dag",
        __autumn_workflow_info_one_failed_root_dag().handler,
    )
    .await;
}

#[tokio::test]
async fn lowered_handler_applies_all_failed_trigger_rule_to_root_task() {
    assert_root_trigger_rule_skips_without_upstreams(
        "all_failed_root_dag",
        __autumn_workflow_info_all_failed_root_dag().handler,
    )
    .await;
}

// ---------------------------------------------------------------------------
// REFACTOR TEST — DagInfo::as_workflow_info() returns the lowered handler
// ---------------------------------------------------------------------------

/// `DagInfo::as_workflow_info()` must return `Some(WorkflowInfo)` whose name
/// matches the DAG name when the `unified-dag-execution` feature is enabled.
#[test]
fn dag_info_as_workflow_info_returns_some_with_matching_name() {
    let dag_info = __autumn_dag_info_linear_dag();
    let wf_info = dag_info
        .as_workflow_info()
        .expect("as_workflow_info should return Some with unified-dag-execution feature");
    assert_eq!(wf_info.name, dag_info.name);
    assert_eq!(wf_info.module, dag_info.module);
}

// ---------------------------------------------------------------------------
// STEP 2 — scheduling promoted to WorkflowSchedule
// ---------------------------------------------------------------------------

/// A DAG with a cron schedule, catchup=true, `max_active_runs`=3, and a custom
/// queue — used to verify that `as_workflow_schedule()` maps fields correctly.
#[dag(
    schedule = "0 * * * *",
    catchup = true,
    max_active_runs = 3,
    default_queue = "test-queue"
)]
fn scheduled_dag(dag: &mut DagBuilder) {
    let _extract = dag.activity(extract_users);
}

/// `DagInfo::as_workflow_schedule()` must return `Some(WorkflowSchedule)` with
/// fields that mirror the DAG's schedule, catchup, `max_active_runs`, and queue
/// when `unified-dag-execution` is enabled.
#[test]
fn as_workflow_schedule_returns_some_with_matching_fields() {
    use autumn_harvest::policy::WorkflowSchedule;

    let dag_info = __autumn_dag_info_scheduled_dag();
    let ws: WorkflowSchedule = dag_info
        .as_workflow_schedule()
        .expect("scheduled unified DAG should return Some(WorkflowSchedule)");

    assert_eq!(ws.workflow_name, "scheduled_dag");
    assert!(ws.catchup, "catchup should mirror the DAG attribute");
    assert_eq!(ws.max_active_runs, 3);
    assert_eq!(ws.queue_name, "test-queue");
    assert!(
        matches!(ws.schedule, autumn_harvest::policy::Schedule::Cron(ref e) if e == "0 * * * *"),
        "schedule should be Cron(\"0 * * * *\"), got {:?}",
        ws.schedule
    );
}

/// A DAG registered without a `schedule =` attribute must return `None` from
/// `as_workflow_schedule()` — it has no automatic firing trigger.
#[test]
fn as_workflow_schedule_returns_none_for_unscheduled_dag() {
    // linear_dag has no schedule attribute.
    let dag_info = __autumn_dag_info_linear_dag();
    assert!(
        dag_info.as_workflow_schedule().is_none(),
        "unscheduled DAG should return None from as_workflow_schedule()"
    );
}

/// `HarvestBuilder::dags()` must auto-register a `WorkflowInfo` for every
/// unified DAG (one whose `workflow_handler` is populated by the macro) so the
/// runtime can route new starts through the workflow execution path.
#[test]
fn builder_dags_auto_registers_workflow_info() {
    use autumn_harvest::builder::HarvestBuilder;

    // Two dags: one with a schedule, one without.
    let builder = HarvestBuilder::new().dags(vec![
        __autumn_dag_info_linear_dag(),
        __autumn_dag_info_scheduled_dag(),
    ]);

    assert_eq!(
        builder.workflow_count(),
        2,
        "builder should auto-register one WorkflowInfo per unified DAG"
    );
    // DAG count stays at 2 (backward-compat field).
    assert_eq!(builder.dag_count(), 2);
}

/// `HarvestBuilder::dags()` must auto-register a `WorkflowSchedule` for every
/// unified DAG that carries a schedule attribute, with no duplicates for
/// unscheduled DAGs.
#[test]
fn builder_dags_auto_registers_workflow_schedule_only_for_scheduled_dags() {
    use autumn_harvest::builder::HarvestBuilder;

    let builder = HarvestBuilder::new().dags(vec![
        __autumn_dag_info_linear_dag(),
        __autumn_dag_info_scheduled_dag(),
    ]);

    // Only scheduled_dag has a schedule; linear_dag does not.
    assert_eq!(
        builder.workflow_schedule_count(),
        1,
        "builder should auto-register one WorkflowSchedule for scheduled_dag only"
    );
}

// ---------------------------------------------------------------------------
// STEP 3 — trigger routing: unified DAGs must not require a classic catalog entry
// ---------------------------------------------------------------------------

/// When `unified-dag-execution` is on, a DAG that was promoted to the workflow
/// execution path must be recognisable purely from the workflow registry — the
/// scheduler's `DagCatalog` (classic path) will not contain it (Step 2 ensures
/// `compile_dag_catalog` skips unified DAGs). The management API validates a
/// separate DAG registration marker before this workflow handler can run.
///
/// This test constructs a minimal `HandlerRegistry` that mirrors what
/// `HarvestBuilder::dags()` produces and asserts the worker handler is present.
#[test]
fn unified_dag_registers_workflow_handler_for_worker_execution() {
    use autumn_harvest::worker::HandlerRegistry;

    // Build a registry that mimics what HarvestBuilder::dags() produces for a
    // unified DAG: the WorkflowInfo from as_workflow_info() is auto-pushed.
    let registry = HandlerRegistry::new(vec![__autumn_workflow_info_linear_dag()], vec![]);

    // The routing predicate: unified dag present.
    assert!(
        registry.workflows.contains_key("linear_dag"),
        "linear_dag was auto-registered as a workflow and must be found in the registry"
    );
    // Classic DAG (not registered as a workflow) must not match.
    assert!(
        !registry.workflows.contains_key("classic_unregistered_dag"),
        "an unregistered name must not be found"
    );
}

// ---------------------------------------------------------------------------
// RED-PHASE TEST 6 — fan-out DAG: both parallel tasks in level 1 run
// ---------------------------------------------------------------------------

/// A DAG with one root and two parallel dependents (fan-out) must schedule
/// both dependents when the root succeeds.  Tasks in the same level are
/// dispatched sequentially (by task index order) so the history order is
/// deterministic across replays.
#[tokio::test]
async fn lowered_handler_replays_fanout_dag() {
    let id_extract = ActivityExecId::new();
    let id_load = ActivityExecId::new();
    let id_notify = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        // Level 0: extract_users
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_extract,
            output: Value::Null,
        },
        // Level 1: load_users (task index 1) then notify_complete (task index 2)
        WorkflowEvent::ActivityScheduled {
            activity_id: id_load,
            name: "load_users".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_load,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_notify,
            name: "notify_complete".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_notify,
            output: Value::Null,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("fanout_dag", __autumn_workflow_info_fanout_dag().handler)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "fan-out DAG should replay correctly, got: {report}"
    );
}
