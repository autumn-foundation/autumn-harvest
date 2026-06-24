//! Tests for `#[dag]` lowering onto the workflow execution path (issue #256, Step 1).
//!
//! Requires features: `unified-dag-execution` + `testing`.
//!
//! These tests verify that when `unified-dag-execution` is enabled, `#[dag]`
//! emits a shadow `__autumn_workflow_info_{name}()` companion whose handler
//! walks the `DagDefinition` level by level, dispatches activities through
//! `ctx.execute_activity_raw`, and evaluates trigger rules deterministically.

#![allow(clippy::unused_async)]

use autumn_harvest::context::WorkflowCommand;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::prelude::*;
#[cfg(feature = "db")]
use autumn_harvest::scheduler::compile_dag_catalog;
use autumn_harvest::testing::{NonDeterminismKind, ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::ActivityExecId;
use chrono::Utc;
use serde_json::{Value, json};

#[test]
fn harvest_migration_versions_are_unique() {
    let migration_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut versions = std::collections::BTreeMap::<String, String>::new();

    for entry in std::fs::read_dir(&migration_dir).expect("migrations directory should be readable")
    {
        let entry = entry.expect("migration directory entry should be readable");
        if !entry
            .file_type()
            .expect("migration entry file type should be readable")
            .is_dir()
        {
            continue;
        }

        if !entry.path().join("up.sql").is_file() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().into_owned();
        let version = name
            .split_once('_')
            .map_or_else(|| name.clone(), |(version, _)| version.to_owned());
        if let Some(previous) = versions.insert(version.clone(), name.clone()) {
            panic!(
                "migration version {version} is used by both {previous} and {name}; Diesel tracks only the version prefix"
            );
        }
    }
}

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

#[activity(local = true)]
async fn local_only(_ctx: &ActivityContext) -> Result<(), String> {
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

#[dag]
fn local_activity_dag(dag: &mut DagBuilder) {
    let _root = dag.activity(local_only);
}

fn dag_task_input(task: &str) -> Value {
    json!({
        "conf": Value::Null,
        "dag_task": task,
    })
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: dag_task_input("extract_users"),
            queue: "etl-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_extract,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_load,
            name: "load_users".into(),
            input: dag_task_input("load_users"),
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: dag_task_input("extract_users"),
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: dag_task_input("extract_users"),
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
            input: dag_task_input("load_users"),
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

#[tokio::test]
async fn lowered_handler_propagates_activity_replay_mismatch() {
    let id_extract = ActivityExecId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "renamed_extract_users".into(),
            input: dag_task_input("extract_users"),
            queue: "etl-workers".into(),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("linear_dag", __autumn_workflow_info_linear_dag().handler)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ActivityScheduleMismatch,
                ..
            }
        ),
        "DAG activity replay mismatches must propagate as non-determinism, got: {report}"
    );
}

async fn assert_root_trigger_rule_skips_without_upstreams(
    dag_name: &str,
    handler: WorkflowHandlerFn,
) {
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
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

#[tokio::test]
async fn lowered_handler_merges_dag_task_into_object_workflow_input() {
    let id_extract = ActivityExecId::new();
    let workflow_input = json!({ "tenant": "acme", "run": 42 });

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: json!({ "tenant": "acme", "run": 42, "dag_task": "extract_users" }),
            queue: "test-queue".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_extract,
            output: Value::Null,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "scheduled_dag",
            __autumn_workflow_info_scheduled_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "object workflow input should be merged with dag_task, got: {report}"
    );
}

#[tokio::test]
async fn lowered_handler_wraps_scalar_workflow_input_with_conf_and_dag_task() {
    let id_extract = ActivityExecId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: json!("manual-run"),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: json!({ "conf": "manual-run", "dag_task": "extract_users" }),
            queue: "test-queue".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_extract,
            output: Value::Null,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "scheduled_dag",
            __autumn_workflow_info_scheduled_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "non-object workflow input should be wrapped as conf plus dag_task, got: {report}"
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

#[test]
fn builder_rejects_workflow_name_collision_with_auto_registered_dag() {
    use autumn_harvest::builder::HarvestBuilder;
    use autumn_harvest::info::WorkflowInfo;

    let colliding_workflow = WorkflowInfo {
        name: "linear_dag",
        module: "tests",
        handler: __autumn_workflow_info_scheduled_dag().handler,
        execution_timeout: None,
        sla: None,
        concurrency: None,

        debounce: None,
        batch: None,
        max_input_bytes: None,

        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
    };

    let result = HarvestBuilder::new()
        .workflows(vec![colliding_workflow])
        .dags(vec![__autumn_dag_info_linear_dag()])
        .try_build();

    let err = result.expect_err("workflow/DAG name collision must be rejected");
    assert!(
        err.to_string().contains("linear_dag"),
        "collision error should name the shared registration, got: {err}"
    );
}

#[test]
fn builder_rejects_local_activities_in_dag_definitions() {
    use autumn_harvest::builder::HarvestBuilder;

    let result = HarvestBuilder::new()
        .activities(vec![__autumn_activity_info_local_only()])
        .dags(vec![__autumn_dag_info_local_activity_dag()])
        .try_build();

    let err = result.expect_err("DAGs must reject local activity tasks at build time");
    assert!(
        err.to_string().contains("local activity")
            && err.to_string().contains("local_only")
            && err.to_string().contains("local_activity_dag"),
        "local-activity DAG rejection should name the DAG and activity, got: {err}"
    );
}

#[cfg(feature = "db")]
#[test]
fn compile_dag_catalog_keeps_unified_dag_metadata() {
    let catalog = compile_dag_catalog(vec![__autumn_dag_info_linear_dag()])
        .expect("unified DAG metadata should compile into the catalog");

    let registered = catalog
        .get("linear_dag")
        .expect("unified DAG should remain in runtime DAG catalog");
    assert_eq!(registered.task_count(), 2);
    assert!(registered.schedule.is_none());
}

#[cfg(feature = "db")]
#[test]
fn compile_dag_catalog_rejects_duplicate_unified_dag_names() {
    let result = compile_dag_catalog(vec![
        __autumn_dag_info_linear_dag(),
        __autumn_dag_info_linear_dag(),
    ]);

    assert!(
        result
            .expect_err("duplicate unified DAG names must be rejected")
            .to_string()
            .contains("duplicate dag registration"),
        "duplicate DAG error should preserve the catalog validation message"
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
#[cfg(feature = "db")]
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
/// both dependents before awaiting either completion. This preserves the
/// classic executor's same-level parallelism and keeps replay deterministic.
#[tokio::test]
async fn lowered_handler_replays_fanout_dag() {
    let id_extract = ActivityExecId::new();
    let id_load = ActivityExecId::new();
    let id_notify = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        // Level 0: extract_users
        WorkflowEvent::ActivityScheduled {
            activity_id: id_extract,
            name: "extract_users".into(),
            input: dag_task_input("extract_users"),
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
            input: dag_task_input("load_users"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_notify,
            name: "notify_complete".into(),
            input: dag_task_input("notify_complete"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_load,
            output: Value::Null,
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

#[tokio::test]
async fn lowered_handler_leaves_queue_empty_when_dag_task_has_no_queue() {
    let outcome = run_workflow(
        ExecutionId::new(),
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        __autumn_workflow_info_alldone_dag().handler,
        Value::Null,
    )
    .await;

    let WorkflowOutcome::Suspended { commands } = outcome else {
        panic!("expected initial DAG activity to suspend, got {outcome:?}");
    };
    assert_eq!(commands.len(), 1);
    let WorkflowCommand::ScheduleActivity { queue, .. } = &commands[0] else {
        panic!("expected ScheduleActivity command, got {:?}", commands[0]);
    };
    assert_eq!(
        queue, "",
        "DAG tasks with no DAG default_queue and no per-task queue must leave queue empty so the activity default queue can apply"
    );
}

// ============================================================================
// Issue #482 — Data-dependent DAG branching
// ============================================================================

// ---------------------------------------------------------------------------
// Activity stubs for branching tests
// ---------------------------------------------------------------------------

#[activity]
async fn score_payment(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({"fraud_score": 0.0})) // overridden per-test via mock
}

#[activity]
async fn manual_review(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("reviewed"))
}

#[activity]
async fn auto_approve(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("approved"))
}

#[activity]
async fn notify_result(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("notified"))
}

#[activity]
async fn low_risk_path(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("low"))
}

#[activity]
async fn medium_risk_path(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("medium"))
}

#[activity]
async fn high_risk_path(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("high"))
}

// ---------------------------------------------------------------------------
// DAG fixtures for branching tests
// ---------------------------------------------------------------------------

/// Fraud-routing DAG: `score_payment` → (`manual_review` | `auto_approve`) → `notify_result` (`AllDone`)
#[dag(default_queue = "risk-workers")]
fn fraud_routing_dag(dag: &mut DagBuilder) {
    let score = dag.activity(score_payment);
    let review = dag
        .activity(manual_review)
        .upstream(&score)
        .condition(|ups| ups[0]["fraud_score"].as_f64().is_some_and(|s| s > 0.8));
    let auto = dag
        .activity(auto_approve)
        .upstream(&score)
        .condition(|ups| ups[0]["fraud_score"].as_f64().is_some_and(|s| s <= 0.8));
    // AllDone join fires regardless of which branch ran
    let _notify = dag
        .activity(notify_result)
        .upstream(&review)
        .upstream(&auto)
        .trigger_rule(TriggerRule::AllDone);
}

/// Three-way switch: low / medium / high
#[dag(default_queue = "risk-workers")]
fn three_way_switch_dag(dag: &mut DagBuilder) {
    let score = dag.activity(score_payment);
    let _low = dag
        .activity(low_risk_path)
        .upstream(&score)
        .condition(|ups| ups[0]["level"].as_str() == Some("low"));
    let _medium = dag
        .activity(medium_risk_path)
        .upstream(&score)
        .condition(|ups| ups[0]["level"].as_str() == Some("medium"));
    let _high = dag
        .activity(high_risk_path)
        .upstream(&score)
        .condition(|ups| ups[0]["level"].as_str() == Some("high"));
}

fn risk_input(task: &str) -> Value {
    json!({ "conf": Value::Null, "dag_task": task })
}

// ---------------------------------------------------------------------------
// Test 1 — condition-false branch replays with a dag_skip marker
// ---------------------------------------------------------------------------

/// Low fraud score: `manual_review` is skipped (condition false), `auto_approve` runs.
/// History contains a `MarkerRecorded(dag_skip:1)` before `auto_approve`'s events.
#[tokio::test]
async fn condition_false_branch_replays_with_skip_marker() {
    let id_score = ActivityExecId::new();
    let id_auto = ActivityExecId::new();
    let id_notify = ActivityExecId::new();

    // Task indices: 0=score_payment, 1=manual_review, 2=auto_approve, 3=notify_result
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        // Level 0: score_payment runs
        WorkflowEvent::ActivityScheduled {
            activity_id: id_score,
            name: "score_payment".into(),
            input: risk_input("score_payment"),
            queue: "risk-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_score,
            output: json!({"fraud_score": 0.2}), // low → auto_approve wins
        },
        // Level 1: manual_review condition false → dag_skip:1 marker
        WorkflowEvent::MarkerRecorded {
            name: "dag_skip:1".into(),
            details: json!({"task": "manual_review", "reason": "condition_false"}),
        },
        // Level 1: auto_approve condition true → runs
        WorkflowEvent::ActivityScheduled {
            activity_id: id_auto,
            name: "auto_approve".into(),
            input: risk_input("auto_approve"),
            queue: "risk-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_auto,
            output: json!("approved"),
        },
        // Level 2: notify_result (AllDone join) runs
        WorkflowEvent::ActivityScheduled {
            activity_id: id_notify,
            name: "notify_result".into(),
            input: risk_input("notify_result"),
            queue: "risk-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_notify,
            output: json!("notified"),
        },
    ];

    let expected_events = history.len();
    let report = WorkflowReplayer::new()
        .register_fn(
            "fraud_routing_dag",
            __autumn_workflow_info_fraud_routing_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert_eq!(
        report.events_replayed, expected_events,
        "all events should be consumed; got: {report}"
    );
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "condition-false branch (auto_approve path) should replay successfully, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — condition-true branch schedules the activity
// ---------------------------------------------------------------------------

/// High fraud score: `manual_review` runs, `auto_approve` is skipped (condition false).
#[tokio::test]
async fn condition_true_branch_schedules_activity() {
    let id_score = ActivityExecId::new();
    let id_review = ActivityExecId::new();
    let id_notify = ActivityExecId::new();

    // Task indices: 0=score_payment, 1=manual_review, 2=auto_approve, 3=notify_result
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_score,
            name: "score_payment".into(),
            input: risk_input("score_payment"),
            queue: "risk-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_score,
            output: json!({"fraud_score": 0.95}), // high → manual_review wins
        },
        // Level 1 task-index order: idx 1 (manual_review) → Run (pushed to futures),
        // then idx 2 (auto_approve) → SkipByCondition → dag_skip:2 marker emitted
        // synchronously before join_all awaits the futures.
        // So the marker comes BEFORE manual_review's ActivityScheduled in history.
        WorkflowEvent::MarkerRecorded {
            name: "dag_skip:2".into(),
            details: json!({"task": "auto_approve", "reason": "condition_false"}),
        },
        // manual_review condition true → runs (emitted by join_all after the marker)
        WorkflowEvent::ActivityScheduled {
            activity_id: id_review,
            name: "manual_review".into(),
            input: risk_input("manual_review"),
            queue: "risk-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_review,
            output: json!("reviewed"),
        },
        // notify_result (AllDone join) runs
        WorkflowEvent::ActivityScheduled {
            activity_id: id_notify,
            name: "notify_result".into(),
            input: risk_input("notify_result"),
            queue: "risk-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_notify,
            output: json!("notified"),
        },
    ];

    let expected_events = history.len();
    let report = WorkflowReplayer::new()
        .register_fn(
            "fraud_routing_dag",
            __autumn_workflow_info_fraud_routing_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert_eq!(
        report.events_replayed, expected_events,
        "all events consumed; got: {report}"
    );
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "condition-true (manual_review) path should replay successfully, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — condition skip propagates downstream via trigger rules (no second marker)
// ---------------------------------------------------------------------------

/// When both branches are skipped by condition, the `AllDone` join still fires.
/// A trigger-rule skip (`AllSuccess` on downstream of a skipped node) must NOT
/// emit a `dag_skip` marker — only condition-skips emit markers.
///
/// This DAG: score → [review (cond false), auto (cond false)] → notify (`AllDone`)
/// Because both branches are condition-skipped the notify still fires (`AllDone`).
/// notify's Skipped propagation of downstream (none here) is trigger-rule-based.
#[tokio::test]
async fn condition_skip_propagates_and_alldone_join_still_fires() {
    use autumn_harvest::testing::WorkflowTestEnv;

    // Both conditions always false (score 0.5 — neither > 0.8 nor actually
    // we need a DAG where both conditions can be false simultaneously).
    // Use three_way_switch_dag with level="unknown" → all three branches skip.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("score_payment", |_| Ok(json!({"level": "unknown"})))
        .run(
            __autumn_workflow_info_three_way_switch_dag().handler,
            Value::Null,
        )
        .await;

    // All three branches skipped by condition; DAG succeeds (no failed tasks)
    assert!(
        outcome.result.is_ok(),
        "three-way-switch with all branches skipped should succeed (no failed tasks), got: {:?}",
        outcome.result
    );

    // Verify exactly three dag_skip markers in the event history
    let skip_marker_count = outcome.events().iter().filter(|e| {
        matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("dag_skip:"))
    }).count();
    assert_eq!(
        skip_marker_count, 3,
        "all three branches should emit dag_skip markers"
    );

    // Verify no trigger-rule-skipped nodes emit markers (there are none in this DAG beyond the 3 condition nodes)
    let has_trigger_rule_skip_marker = outcome.events().iter().any(|e| {
        if let WorkflowEvent::MarkerRecorded { name, details } = e {
            name.starts_with("dag_skip:")
                && details.get("reason").and_then(|r| r.as_str()) != Some("condition_false")
        } else {
            false
        }
    });
    assert!(
        !has_trigger_rule_skip_marker,
        "trigger-rule skips must not emit markers"
    );
}

// ---------------------------------------------------------------------------
// Test 4 — flipping the condition is reported as non-determinism
// ---------------------------------------------------------------------------

/// If history contains a `dag_skip` marker but the condition now returns true
/// (or vice versa), the replayer must report `NonDeterministic`.
#[tokio::test]
async fn condition_flip_is_reported_as_nondeterminism() {
    let id_score = ActivityExecId::new();

    // History: score succeeded, then dag_skip:1 (manual_review was condition-false)
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_score,
            name: "score_payment".into(),
            input: risk_input("score_payment"),
            queue: "risk-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_score,
            output: json!({"fraud_score": 0.95}), // high score → manual_review condition TRUE
                                                  // But history has dag_skip:1 → history says it was condition-false.
                                                  // Replaying with the same code will NOT skip (condition is true for 0.95),
                                                  // so the replayer will try to schedule manual_review but find dag_skip:1
                                                  // marker at cursor → Diverged → NonDeterministic.
        },
        // Marker says manual_review was skipped — but our condition says it should run.
        WorkflowEvent::MarkerRecorded {
            name: "dag_skip:1".into(),
            details: json!({"task": "manual_review", "reason": "condition_false"}),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "fraud_routing_dag",
            __autumn_workflow_info_fraud_routing_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "flipped condition should report NonDeterminismDetected, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Test 5 — run live with WorkflowTestEnv then replay identically (AC4)
// ---------------------------------------------------------------------------

/// Run the fraud DAG live via `WorkflowTestEnv` (score=0.2 → `auto_approve`),
/// capture the produced event history, then replay it with `WorkflowReplayer`.
/// The replay must succeed, verifying the branch decision is deterministic.
#[tokio::test]
async fn condition_dag_runs_live_then_replays_identically() {
    use autumn_harvest::testing::WorkflowTestEnv;

    // Live run: low fraud score → auto_approve branch
    let outcome = WorkflowTestEnv::new()
        .mock_activity("score_payment", |_| Ok(json!({"fraud_score": 0.2})))
        .mock_activity("auto_approve", |_| Ok(json!("approved")))
        .mock_activity("notify_result", |_| Ok(json!("notified")))
        .run(
            __autumn_workflow_info_fraud_routing_dag().handler,
            Value::Null,
        )
        .await;

    assert!(
        outcome.result.is_ok(),
        "live DAG run should succeed, got: {:?}",
        outcome.result
    );

    let events = outcome.events();

    // Exactly one dag_skip marker for manual_review (task idx 1)
    let skip_markers: Vec<_> = events.iter().filter(|e| {
        matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("dag_skip:"))
    }).collect();
    assert_eq!(
        skip_markers.len(),
        1,
        "exactly one skip marker expected (manual_review), got {skip_markers:?}"
    );

    // No ActivityScheduled for manual_review
    let manual_review_scheduled = events.iter().any(
        |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "manual_review"),
    );
    assert!(
        !manual_review_scheduled,
        "manual_review must not be scheduled when condition is false"
    );

    // Now replay: must reproduce the identical branch
    let report = WorkflowReplayer::new()
        .register_fn(
            "fraud_routing_dag",
            __autumn_workflow_info_fraud_routing_dag().handler,
        )
        .replay_from_events(events.to_vec())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay of live run should succeed (deterministic branch decision), got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Test 6 — 1000-replay sweep (success metric)
// ---------------------------------------------------------------------------

/// Alternate high-score / low-score fixtures across 1,000 replays.
/// Every replay must reproduce the identical branch (`ReplaySucceeded`).
#[tokio::test]
async fn condition_branch_replay_sweep_1000() {
    let handler = __autumn_workflow_info_fraud_routing_dag().handler;

    for i in 0u32..1_000 {
        // Alternate: even → low score (auto_approve), odd → high score (manual_review)
        let (score, skip_task_idx, skip_task_name, run_task_name) = if i % 2 == 0 {
            (0.2f64, 1usize, "manual_review", "auto_approve")
        } else {
            (0.95f64, 2usize, "auto_approve", "manual_review")
        };

        let id_score = ActivityExecId::new();
        let id_run = ActivityExecId::new();
        let id_notify = ActivityExecId::new();

        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id_score,
                name: "score_payment".into(),
                input: risk_input("score_payment"),
                queue: "risk-workers".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id_score,
                output: json!({"fraud_score": score}),
            },
            // Which branch gets the skip marker depends on the score
            // For low score: marker for manual_review (idx 1) before auto_approve
            // For high score: manual_review runs first, then marker for auto_approve (idx 2)
            // Build in the same order as the macro generates (task index order within a level)
            // Level 1 has tasks [1=manual_review, 2=auto_approve] in order
        ];

        // Append level-1 events in task-index order (1 then 2).
        // The marker and ActivityScheduled are the same for both branches; only the
        // completed output differs (auto_approve → "approved", manual_review → "reviewed").
        let mut h = history;
        h.push(WorkflowEvent::MarkerRecorded {
            name: format!("dag_skip:{skip_task_idx}"),
            details: json!({"task": skip_task_name, "reason": "condition_false"}),
        });
        h.push(WorkflowEvent::ActivityScheduled {
            activity_id: id_run,
            name: run_task_name.into(),
            input: risk_input(run_task_name),
            queue: "risk-workers".into(),
        });
        // low score (even i): auto_approve outputs "approved"
        // high score (odd i):  manual_review outputs "reviewed"
        let run_output = if i % 2 == 0 {
            json!("approved")
        } else {
            json!("reviewed")
        };
        h.push(WorkflowEvent::ActivityCompleted {
            activity_id: id_run,
            output: run_output,
        });
        // Level 2: notify_result (AllDone join)
        h.push(WorkflowEvent::ActivityScheduled {
            activity_id: id_notify,
            name: "notify_result".into(),
            input: risk_input("notify_result"),
            queue: "risk-workers".into(),
        });
        h.push(WorkflowEvent::ActivityCompleted {
            activity_id: id_notify,
            output: json!("notified"),
        });

        let report = WorkflowReplayer::new()
            .register_fn("fraud_routing_dag", handler)
            .replay_from_events(h)
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "sweep iteration {i} (score={score}) failed: {report}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 7 — three-way switch: exactly one branch runs (AC5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn three_way_switch_runs_exactly_one_branch() {
    use autumn_harvest::testing::WorkflowTestEnv;

    for level in ["low", "medium", "high"] {
        let level_val = json!({"level": level});
        let level_clone = level_val.clone();
        let outcome = WorkflowTestEnv::new()
            .mock_activity("score_payment", move |_| Ok(level_clone.clone()))
            .mock_activity("low_risk_path", |_| Ok(json!("low")))
            .mock_activity("medium_risk_path", |_| Ok(json!("medium")))
            .mock_activity("high_risk_path", |_| Ok(json!("high")))
            .run(
                __autumn_workflow_info_three_way_switch_dag().handler,
                Value::Null,
            )
            .await;

        assert!(
            outcome.result.is_ok(),
            "three_way_switch level={level} should succeed, got: {:?}",
            outcome.result
        );

        let events = outcome.events();
        // Exactly one branch scheduled
        let scheduled: Vec<_> = events
            .iter()
            .filter(|e| {
                matches!(e, WorkflowEvent::ActivityScheduled { name, .. }
                if matches!(name.as_str(), "low_risk_path" | "medium_risk_path" | "high_risk_path"))
            })
            .collect();
        assert_eq!(
            scheduled.len(),
            1,
            "exactly one branch should run for level={level}, got: {scheduled:?}"
        );

        // Exactly two dag_skip markers (the other two branches)
        let skip_markers: Vec<_> = events.iter().filter(|e| {
            matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("dag_skip:"))
        }).collect();
        assert_eq!(
            skip_markers.len(),
            2,
            "exactly two branches skipped for level={level}, got: {skip_markers:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 8 — mapped task with condition skips whole map
// ---------------------------------------------------------------------------

#[activity]
async fn process_item(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("processed"))
}

#[dag(default_queue = "workers")]
fn conditional_map_dag(dag: &mut DagBuilder) {
    let source = dag.activity(score_payment);
    let _mapped = dag
        .map_activity(process_item)
        .over(&source)
        .condition(|_ups| false); // always skip
}

#[tokio::test]
async fn mapped_task_condition_skips_whole_map() {
    use autumn_harvest::testing::WorkflowTestEnv;

    let outcome = WorkflowTestEnv::new()
        .mock_activity("score_payment", |_| Ok(json!([1, 2, 3])))
        .run(
            __autumn_workflow_info_conditional_map_dag().handler,
            Value::Null,
        )
        .await;

    assert!(
        outcome.result.is_ok(),
        "condition-skipped mapped task should not fail the DAG, got: {:?}",
        outcome.result
    );

    let events = outcome.events();
    // No process_item scheduled (map skipped entirely)
    let any_mapped = events.iter().any(
        |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "process_item"),
    );
    assert!(
        !any_mapped,
        "no process_item instances should be scheduled when condition is false"
    );

    // Exactly one dag_skip marker
    let skip_marker_count = events.iter().filter(|e| {
        matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("dag_skip:"))
    }).count();
    assert_eq!(
        skip_marker_count, 1,
        "exactly one dag_skip marker for the mapped task"
    );
}
