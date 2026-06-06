//! Integration tests for DAG dynamic task mapping (issue #485).
//!
//! Requires features: `unified-dag-execution` + `testing`.

#![allow(clippy::unused_async)]

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::ActivityExecId;
use chrono::Utc;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Activities
// ---------------------------------------------------------------------------

#[activity]
async fn list_items(_ctx: &ActivityContext) -> Result<Vec<i32>, String> {
    Ok(vec![1, 2, 3])
}

#[activity]
async fn process_item(_ctx: &ActivityContext, item: i32) -> Result<i32, String> {
    if item < 0 {
        return Err("negative item error".to_string());
    }
    Ok(item * item)
}

#[activity]
#[allow(clippy::cast_possible_truncation, clippy::collapsible_if)]
async fn aggregate_items(_ctx: &ActivityContext, items: Vec<Value>) -> Result<i32, String> {
    let mut sum = 0;
    for val in items {
        if let Some(n) = val.as_i64() {
            sum += n as i32;
        } else if let Some(obj) = val.as_object() {
            if obj.get("status").and_then(Value::as_str) == Some("succeeded") {
                if let Some(n) = obj.get("value").and_then(Value::as_i64) {
                    sum += n as i32;
                }
            }
        }
    }
    Ok(sum)
}

// ---------------------------------------------------------------------------
// DAGs
// ---------------------------------------------------------------------------

/// list -> process (mapped, fail-fast) -> aggregate
#[dag]
fn mapped_fail_fast_dag(dag: &mut DagBuilder) {
    let list = dag.activity(list_items);
    let process = dag.map_activity(process_item).over(&list);
    let _agg = dag.activity(aggregate_items).upstream(&process);
}

/// list -> process (mapped, collect-all) -> aggregate
#[dag]
fn mapped_collect_all_dag(dag: &mut DagBuilder) {
    let list = dag.activity(list_items);
    let process = dag
        .map_activity(process_item)
        .over(&list)
        .map_failure_policy(MapFailurePolicy::CollectAll);
    let _agg = dag.activity(aggregate_items).upstream(&process);
}

fn dag_task_input(task: &str) -> Value {
    json!({
        "conf": Value::Null,
        "dag_task": task,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn dag_macro_emits_workflow_info_for_mapped_dag() {
    let info = __autumn_workflow_info_mapped_fail_fast_dag();
    assert_eq!(info.name, "mapped_fail_fast_dag");
}

#[tokio::test]
async fn happy_path_mapped_dag_execution() {
    let id_list = ActivityExecId::new();
    let id_p1 = ActivityExecId::new();
    let id_p2 = ActivityExecId::new();
    let id_agg = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_list,
            name: "list_items".into(),
            input: dag_task_input("list_items"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_list,
            output: json!([10, 20]),
        },
        // Level 1: Mapped process_item runs for 10 and 20 in parallel
        WorkflowEvent::ActivityScheduled {
            activity_id: id_p1,
            name: "process_item".into(),
            input: json!(10),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_p2,
            name: "process_item".into(),
            input: json!(20),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_p1,
            output: json!(100),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_p2,
            output: json!(400),
        },
        // Level 2: aggregate_items gets the ordered array of results [100, 400]
        WorkflowEvent::ActivityScheduled {
            activity_id: id_agg,
            name: "aggregate_items".into(),
            input: json!([100, 400]),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_agg,
            output: json!(500),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "mapped_fail_fast_dag",
            __autumn_workflow_info_mapped_fail_fast_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "Happy path replay failed: {report}"
    );
}

#[tokio::test]
async fn empty_upstream_collection_mapping() {
    let id_list = ActivityExecId::new();
    let id_agg = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_list,
            name: "list_items".into(),
            input: dag_task_input("list_items"),
            queue: "default".into(),
        },
        // Upstream output is empty array
        WorkflowEvent::ActivityCompleted {
            activity_id: id_list,
            output: json!([]),
        },
        // process_item is skipped (N = 0).
        // aggregate_items receives an empty array []
        WorkflowEvent::ActivityScheduled {
            activity_id: id_agg,
            name: "aggregate_items".into(),
            input: json!([]),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_agg,
            output: json!(0),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "mapped_fail_fast_dag",
            __autumn_workflow_info_mapped_fail_fast_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "Empty mapping replay failed: {report}"
    );
}

#[tokio::test]
async fn fail_fast_mapped_dag_execution() {
    let id_list = ActivityExecId::new();
    let id_p1 = ActivityExecId::new();
    let id_p2 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_list,
            name: "list_items".into(),
            input: dag_task_input("list_items"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_list,
            output: json!([10, -5]),
        },
        // Level 1: Mapped process_item runs
        WorkflowEvent::ActivityScheduled {
            activity_id: id_p1,
            name: "process_item".into(),
            input: json!(10),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_p2,
            name: "process_item".into(),
            input: json!(-5),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_p1,
            output: json!(100),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: id_p2,
            error: "negative item error".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: true,
            details: None,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "mapped_fail_fast_dag",
            __autumn_workflow_info_mapped_fail_fast_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::WorkflowFailed { ref error, .. }
                if error == "one or more DAG tasks failed"
        ),
        "FailFast should fail the workflow execution: {report}"
    );
}

#[tokio::test]
async fn collect_all_mapped_dag_execution() {
    let id_list = ActivityExecId::new();
    let id_p1 = ActivityExecId::new();
    let id_p2 = ActivityExecId::new();
    let id_agg = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_list,
            name: "list_items".into(),
            input: dag_task_input("list_items"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_list,
            output: json!([10, -5]),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_p1,
            name: "process_item".into(),
            input: json!(10),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_p2,
            name: "process_item".into(),
            input: json!(-5),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_p1,
            output: json!(100),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: id_p2,
            error: "negative item error".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: true,
            details: None,
        },
        // Level 2: aggregate_items gets the per-slot status array:
        // [{"status":"succeeded","value":100},{"status":"failed","error":"negative item error"}]
        WorkflowEvent::ActivityScheduled {
            activity_id: id_agg,
            name: "aggregate_items".into(),
            input: json!([
                {"status": "succeeded", "value": 100},
                {"status": "failed", "error": "negative item error"}
            ]),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_agg,
            output: json!(100),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "mapped_collect_all_dag",
            __autumn_workflow_info_mapped_collect_all_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "CollectAll replay failed: {report}"
    );
}

#[tokio::test]
async fn replay_detects_width_n_mismatch() {
    let id_list = ActivityExecId::new();
    let id_p1 = ActivityExecId::new();

    // History has M = 1 scheduled processes, but list output in history is [10, 20] which requires N = 2.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_list,
            name: "list_items".into(),
            input: dag_task_input("list_items"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_list,
            output: json!([10, 20]),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_p1,
            name: "process_item".into(),
            input: json!(10),
            queue: "default".into(),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "mapped_fail_fast_dag",
            __autumn_workflow_info_mapped_fail_fast_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::WorkflowFailed { ref error, .. }
                if error.contains("N differs between live and replay")
        ),
        "Expected non-determinism fail for N mismatch: {report}"
    );
}
