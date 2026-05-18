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
