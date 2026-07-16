//! Integration tests for DAG node input binding (issue #702).
//!
//! Requires features: `unified-dag-execution` + `testing`.
//!
//! A DAG node can bind its activity input directly to one or more upstream node
//! outputs via `.input_from(&up)` / `.input_from_all(&[..])` /
//! `.input_from_aliased(&[..])`. A bound node receives the RAW upstream
//! output(s) (single verbatim, or merged into a keyed JSON object) instead of
//! the trigger-input + `dag_task` wrapper the unbound path uses.
#![cfg(all(feature = "unified-dag-execution", feature = "testing"))]
#![allow(clippy::unused_async)]

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
use autumn_harvest::types::ActivityExecId;
use chrono::Utc;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Activities
// ---------------------------------------------------------------------------

#[activity]
async fn bound_extract(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!([1, 2, 3]))
}

#[activity]
async fn bound_transform(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let _ = input;
    Ok(json!({ "records": 3 }))
}

#[activity]
async fn bound_load(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let _ = input;
    Ok(Value::Null)
}

#[activity]
async fn src_a(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("a-out"))
}

#[activity]
async fn src_b(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("b-out"))
}

#[activity]
async fn src_c(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("c-out"))
}

#[activity]
async fn sink_c(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let _ = input;
    Ok(json!("done"))
}

#[activity]
async fn fail_root(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("boom".to_string())
}

#[activity]
async fn fail_final(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let _ = input;
    Ok(json!("final"))
}

#[activity]
async fn skip_root(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("root"))
}

#[activity]
async fn skip_maybe(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("maybe"))
}

#[activity]
async fn skip_final(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let _ = input;
    Ok(json!("final"))
}

// ---------------------------------------------------------------------------
// DAGs
// ---------------------------------------------------------------------------

/// ETL: extract → transform (bound to extract) → load (unbound, upstream transform).
#[dag]
fn bound_etl_dag(dag: &mut DagBuilder) {
    let extract = dag.activity(bound_extract);
    let transform = dag.activity(bound_transform).input_from(&extract);
    let _load = dag.activity(bound_load).upstream(&transform);
}

/// Same ETL topology with NO bindings — regression guard for the unbound path.
#[dag]
fn unbound_etl_dag(dag: &mut DagBuilder) {
    let extract = dag.activity(bound_extract);
    let transform = dag.activity(bound_transform).upstream(&extract);
    let _load = dag.activity(bound_load).upstream(&transform);
}

/// Fan-in: two roots → one sink whose input is a keyed merge of both.
#[dag]
fn merged_dag(dag: &mut DagBuilder) {
    let a = dag.activity(src_a);
    let b = dag.activity(src_b);
    let _c = dag
        .activity(sink_c)
        .input_from_aliased(&[("a", &a), ("b", &b)]);
}

/// Fan-in: three roots → one sink whose input is a keyed merge of all three.
#[dag]
fn merged_three_dag(dag: &mut DagBuilder) {
    let a = dag.activity(src_a);
    let b = dag.activity(src_b);
    let c = dag.activity(src_c);
    let _sink = dag
        .activity(sink_c)
        .input_from_aliased(&[("a", &a), ("b", &b), ("c", &c)]);
}

/// A failed upstream feeding a bound downstream that runs anyway (`AllDone`).
#[dag]
fn fail_bind_dag(dag: &mut DagBuilder) {
    let root = dag.activity(fail_root);
    let _final = dag
        .activity(fail_final)
        .input_from(&root)
        .trigger_rule(TriggerRule::AllDone);
}

/// A skipped upstream feeding a bound downstream that runs anyway (`AllDone`).
#[dag]
fn skip_bind_dag(dag: &mut DagBuilder) {
    let root = dag.activity(skip_root);
    let maybe = dag
        .activity(skip_maybe)
        .upstream(&root)
        .condition(|_ups| false); // always condition-skipped
    let _final = dag
        .activity(skip_final)
        .input_from(&maybe)
        .trigger_rule(TriggerRule::AllDone);
}

fn dag_task_input(task: &str) -> Value {
    json!({ "conf": Value::Null, "dag_task": task })
}

fn scheduled_input(events: &[WorkflowEvent], activity: &str) -> Value {
    events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == activity => {
                Some(input.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no ActivityScheduled event for '{activity}'"))
}

// ---------------------------------------------------------------------------
// AC2 — a bound node receives the upstream output verbatim (live)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bound_node_receives_upstream_output_live() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("bound_extract", |_| Ok(json!([1, 2, 3])))
        .mock_activity("bound_transform", |input| {
            assert_eq!(
                input,
                json!([1, 2, 3]),
                "bound node must receive the RAW upstream output, not a wrapped conf/dag_task object"
            );
            Ok(json!({ "records": 3 }))
        })
        .mock_activity("bound_load", |_| Ok(Value::Null))
        .run(__autumn_workflow_info_bound_etl_dag().handler, Value::Null)
        .await;

    assert!(
        outcome.result.is_ok(),
        "bound ETL DAG should run to success, got: {:?}",
        outcome.result
    );

    // Authoritative check on the recorded history (independent of the mock closure).
    assert_eq!(
        scheduled_input(outcome.events(), "bound_transform"),
        json!([1, 2, 3]),
        "recorded transform input must be the raw extract output"
    );
}

// ---------------------------------------------------------------------------
// AC5 — a bound ETL history replays deterministically
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bound_etl_history_replays_successfully() {
    let id_extract = ActivityExecId::new();
    let id_transform = ActivityExecId::new();
    let id_load = ActivityExecId::new();

    // Task indices: 0=extract (unbound root), 1=transform (bound to extract),
    // 2=load (unbound, upstream transform). No default_queue → queue "".
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
            name: "bound_extract".into(),
            input: dag_task_input("bound_extract"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_extract,
            output: json!([1, 2, 3]),
        },
        // transform is BOUND → its recorded input is the raw extract output.
        WorkflowEvent::ActivityScheduled {
            activity_id: id_transform,
            name: "bound_transform".into(),
            input: json!([1, 2, 3]),
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_transform,
            output: json!({ "records": 3 }),
        },
        // load is UNBOUND → wrapped conf/dag_task input.
        WorkflowEvent::ActivityScheduled {
            activity_id: id_load,
            name: "bound_load".into(),
            input: dag_task_input("bound_load"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_load,
            output: Value::Null,
        },
    ];

    // The recorded transform input is the raw upstream output.
    assert_eq!(
        scheduled_input(&history, "bound_transform"),
        json!([1, 2, 3]),
        "constructed history must carry the raw bound input"
    );

    let expected_events = history.len();
    let report = WorkflowReplayer::new()
        .register_fn(
            "bound_etl_dag",
            __autumn_workflow_info_bound_etl_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert_eq!(report.events_replayed, expected_events);
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "bound ETL history should replay deterministically, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Success metric — 1000-replay sweep with randomized within-level completion order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bound_etl_replay_sweep_1000() {
    let handler = __autumn_workflow_info_merged_dag().handler;

    for i in 0u32..1_000 {
        let id_a = ActivityExecId::new();
        let id_b = ActivityExecId::new();
        let id_c = ActivityExecId::new();

        let a_out = json!(format!("a-{i}"));
        let b_out = json!(format!("b-{i}"));

        // Level 0 dispatches in task-index order (a idx0, then b idx1).
        let mut history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id_a,
                name: "src_a".into(),
                input: dag_task_input("src_a"),
                queue: String::new(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id_b,
                name: "src_b".into(),
                input: dag_task_input("src_b"),
                queue: String::new(),
            },
        ];

        // Randomize within-level completion ORDER: even i → a completes first,
        // odd i → b completes first. `match_activity` matches by id out of order.
        let a_completed = WorkflowEvent::ActivityCompleted {
            activity_id: id_a,
            output: a_out.clone(),
        };
        let b_completed = WorkflowEvent::ActivityCompleted {
            activity_id: id_b,
            output: b_out.clone(),
        };
        if i % 2 == 0 {
            history.push(a_completed);
            history.push(b_completed);
        } else {
            history.push(b_completed);
            history.push(a_completed);
        }

        // Level 1: sink_c bound to a merged, aliased object of both outputs.
        history.push(WorkflowEvent::ActivityScheduled {
            activity_id: id_c,
            name: "sink_c".into(),
            input: json!({ "a": a_out, "b": b_out }),
            queue: String::new(),
        });
        history.push(WorkflowEvent::ActivityCompleted {
            activity_id: id_c,
            output: json!("done"),
        });

        let report = WorkflowReplayer::new()
            .register_fn("merged_dag", handler)
            .replay_from_events(history)
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "sweep iteration {i} must replay deterministically, got: {report}"
        );
    }
}

/// Stronger sweep: a 3-source merged fan-in with a genuine rotating permutation
/// of the within-level completion order (3! = 6 orderings). Asserts
/// `ReplaySucceeded` on every iteration — which is only possible if the handler
/// reconstructs the merged keyed input identically regardless of the order the
/// three sources completed in.
#[tokio::test]
async fn merged_three_source_replay_sweep_1000() {
    // All 6 permutations of the level-0 completion order for [a, b, c].
    const PERMS: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];

    let handler = __autumn_workflow_info_merged_three_dag().handler;

    for i in 0u32..1_000 {
        let id_a = ActivityExecId::new();
        let id_b = ActivityExecId::new();
        let id_c = ActivityExecId::new();
        let id_sink = ActivityExecId::new();

        let a_out = json!(format!("a-{i}"));
        let b_out = json!(format!("b-{i}"));
        let c_out = json!(format!("c-{i}"));

        // Level 0 dispatches in task-index order (a idx0, b idx1, c idx2).
        let mut history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id_a,
                name: "src_a".into(),
                input: dag_task_input("src_a"),
                queue: String::new(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id_b,
                name: "src_b".into(),
                input: dag_task_input("src_b"),
                queue: String::new(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id_c,
                name: "src_c".into(),
                input: dag_task_input("src_c"),
                queue: String::new(),
            },
        ];

        // Rotate the within-level completion ORDER across all 6 permutations.
        // `match_activity` matches by id out of order, so a correct handler
        // reconstructs the same merged sink input every time.
        let completions = [
            WorkflowEvent::ActivityCompleted {
                activity_id: id_a,
                output: a_out.clone(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id_b,
                output: b_out.clone(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id_c,
                output: c_out.clone(),
            },
        ];
        let perm = PERMS[(i % 6) as usize];
        for &slot in &perm {
            history.push(completions[slot].clone());
        }

        // Level 1: sink bound to a merged, aliased object of all three outputs.
        history.push(WorkflowEvent::ActivityScheduled {
            activity_id: id_sink,
            name: "sink_c".into(),
            input: json!({ "a": a_out, "b": b_out, "c": c_out }),
            queue: String::new(),
        });
        history.push(WorkflowEvent::ActivityCompleted {
            activity_id: id_sink,
            output: json!("done"),
        });

        let report = WorkflowReplayer::new()
            .register_fn("merged_three_dag", handler)
            .replay_from_events(history)
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "3-source sweep iteration {i} (perm {perm:?}) must replay \
             deterministically with the correct merged keys, got: {report}"
        );
    }
}

// ---------------------------------------------------------------------------
// AC1 (multi) — merged binding delivers a keyed object (live)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn merged_binding_delivers_keyed_object_live() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("src_a", |_| Ok(json!("a-out")))
        .mock_activity("src_b", |_| Ok(json!("b-out")))
        .mock_activity("sink_c", |input| {
            assert_eq!(
                input,
                json!({ "a": "a-out", "b": "b-out" }),
                "merged binding must deliver a keyed object of upstream outputs"
            );
            Ok(json!("done"))
        })
        .run(__autumn_workflow_info_merged_dag().handler, Value::Null)
        .await;

    assert!(
        outcome.result.is_ok(),
        "merged-binding DAG should run to success, got: {:?}",
        outcome.result
    );
    assert_eq!(
        scheduled_input(outcome.events(), "sink_c"),
        json!({ "a": "a-out", "b": "b-out" }),
        "recorded sink input must be the merged keyed object"
    );
}

// ---------------------------------------------------------------------------
// AC6 — binding to a skipped upstream yields null (live)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bound_to_skipped_upstream_yields_null() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("skip_root", |_| Ok(json!("root")))
        // skip_maybe is condition-skipped and never dispatched.
        .mock_activity("skip_final", |input| {
            assert_eq!(
                input,
                Value::Null,
                "binding to a skipped upstream must yield null"
            );
            Ok(json!("final"))
        })
        .run(__autumn_workflow_info_skip_bind_dag().handler, Value::Null)
        .await;

    assert!(
        outcome.result.is_ok(),
        "DAG with a skipped bound upstream should still succeed (AllDone join), got: {:?}",
        outcome.result
    );

    // skip_maybe must not be scheduled; skip_final must be scheduled with null input.
    let maybe_scheduled = outcome.events().iter().any(
        |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "skip_maybe"),
    );
    assert!(!maybe_scheduled, "skip_maybe must be condition-skipped");
    assert_eq!(
        scheduled_input(outcome.events(), "skip_final"),
        Value::Null,
        "recorded final input must be null for the skipped bound upstream"
    );
}

// ---------------------------------------------------------------------------
// AC6 — binding to a failed upstream yields null (live + deterministic replay)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bound_to_failed_upstream_yields_null() {
    // Live: fail_root fails, fail_final binds to it and runs anyway (AllDone).
    let outcome = WorkflowTestEnv::new()
        .mock_activity("fail_root", |_| Err("boom".to_string()))
        .mock_activity("fail_final", |input| {
            assert_eq!(
                input,
                Value::Null,
                "binding to a failed upstream must yield null"
            );
            Ok(json!("final"))
        })
        .run(__autumn_workflow_info_fail_bind_dag().handler, Value::Null)
        .await;

    // A failed upstream fails the DAG as a whole (any Failed status → Err), so
    // — unlike the condition-skip sibling — the terminal result is an error.
    assert!(
        outcome.result.is_err(),
        "a DAG with a failed task must fail overall, got: {:?}",
        outcome.result
    );

    // But the bound downstream still ran, with a deterministic null input.
    let failed = outcome
        .events()
        .iter()
        .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }));
    assert!(failed, "fail_root must record an ActivityFailed event");
    assert_eq!(
        scheduled_input(outcome.events(), "fail_final"),
        Value::Null,
        "recorded final input must be null for the failed bound upstream"
    );

    // Replay a hand-built history (ActivityFailed root, no ActivityCompleted;
    // final scheduled with the raw null output) to prove the bound-null branch
    // is replay-deterministic. A failed DAG replays to a *deterministic*
    // WorkflowFailed — never a NonDeterminismDetected.
    let id_root = ActivityExecId::new();
    let id_final = ActivityExecId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_root,
            name: "fail_root".into(),
            input: dag_task_input("fail_root"),
            queue: String::new(),
        },
        // Root FAILED — no ActivityCompleted, so its output slot stays null.
        WorkflowEvent::ActivityFailed {
            activity_id: id_root,
            error: "boom".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
        // final is BOUND to the failed root → its recorded input is null.
        WorkflowEvent::ActivityScheduled {
            activity_id: id_final,
            name: "fail_final".into(),
            input: Value::Null,
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_final,
            output: json!("final"),
        },
    ];
    assert_eq!(
        scheduled_input(&history, "fail_final"),
        Value::Null,
        "constructed history must carry the null bound input"
    );

    let expected_events = history.len();
    let report = WorkflowReplayer::new()
        .register_fn(
            "fail_bind_dag",
            __autumn_workflow_info_fail_bind_dag().handler,
        )
        .replay_from_events(history)
        .await;

    assert_eq!(report.events_replayed, expected_events);
    assert!(
        matches!(report.status, ReplayStatus::WorkflowFailed { .. }),
        "failed-upstream history must replay deterministically (WorkflowFailed, \
         never NonDeterminismDetected), got: {report}"
    );
}

// ---------------------------------------------------------------------------
// AC3 — an unbound DAG is byte-identical to today (regression guard)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unbound_dag_is_byte_identical_regression() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("bound_extract", |_| Ok(json!([1, 2, 3])))
        .mock_activity("bound_transform", |_| Ok(json!({ "records": 3 })))
        .mock_activity("bound_load", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_unbound_etl_dag().handler,
            Value::Null,
        )
        .await;

    assert!(
        outcome.result.is_ok(),
        "unbound ETL DAG should run to success, got: {:?}",
        outcome.result
    );

    // An unbound downstream node still receives the trigger-input + dag_task wrapper.
    assert_eq!(
        scheduled_input(outcome.events(), "bound_transform"),
        dag_task_input("bound_transform"),
        "unbound node input must stay the wrapped conf/dag_task form (zero regression)"
    );
}
