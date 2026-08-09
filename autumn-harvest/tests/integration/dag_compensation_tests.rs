//! Integration tests for declarative DAG node compensation (issue #780).
//!
//! Requires features: `unified-dag-execution` + `testing`.
//!
//! **Phase 1 = RED.** These tests reference the not-yet-implemented compensation
//! API (`DagTaskRef::compensate` / `compensate_named`, `DagTask::compensate`).
//! Per this repo's TDD convention a compile error against not-yet-existing API
//! is an acceptable RED — the GREEN implementation lands these symbols.
//!
//! A DAG node may declare a **compensator** activity. On terminal DAG failure
//! (`Err("one or more DAG tasks failed")`) the engine unwinds: for every node
//! that **succeeded** and declares a compensator, the compensator is dispatched
//! in **reverse topological / LIFO order** through the existing `Saga`
//! machinery, using the ordinary activity lowering
//! (`execute_activity_raw_with_opts`) on the node's own queue.
//!
//! Contract pinned by these tests:
//! * Unwind order = reverse of (levels FORWARD, ascending index within level).
//! * Compensator input envelope =
//!   `{"dag_compensate": <node>, "input": <node's resolved forward input>,
//!     "output": <node's recorded output>}`.
//! * Skipped / never-reached / failed / uncompensated nodes invoke nothing.
//! * Mapped-node compensation is **node**-granular, not cell-granular.
//! * Cancellation does NOT auto-compensate (`docs/saga.md`).
//! * A successful unwind still returns the ORIGINAL DAG error; a failed unwind
//!   surfaces `SagaCompensationFailed`.
//! * Zero new `WorkflowEvent` variants — compensation rides existing
//!   `ActivityScheduled`/`ActivityCompleted` events plus the `Saga` dedup
//!   markers (`saga_compensated:{seq}` / `saga_compensation_failed:{seq}`).
#![cfg(all(feature = "unified-dag-execution", feature = "testing"))]
#![allow(clippy::unused_async)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::{
    METRIC_SAGA_COMPENSATED, METRIC_SAGA_COMPENSATION_FAILED, MetricsRecorder,
};
use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
use autumn_harvest::types::ActivityExecId;
use autumn_harvest::{ExecutionId, WorkflowCommand, WorkflowContext};
use chrono::Utc;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// The unbound-node forward input shape when the DAG trigger input is a
/// NON-object (`run_unified_dag` wraps it under `conf` and injects `dag_task`).
fn wrapped_dag_task_input(conf: &Value, task: &str) -> Value {
    json!({ "conf": conf, "dag_task": task })
}

/// The `ActivityScheduled` names in history order, restricted to `of_interest`.
fn scheduled_names_among(events: &[WorkflowEvent], of_interest: &[&str]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityScheduled { name, .. }
                if of_interest.contains(&name.as_str()) =>
            {
                Some(name.clone())
            }
            _ => None,
        })
        .collect()
}

/// Every `ActivityScheduled` name in history order.
fn all_scheduled_names(events: &[WorkflowEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityScheduled { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// The recorded `ActivityScheduled.input` for `activity` (first occurrence).
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

/// The recorded `ActivityScheduled.queue` for `activity` (first occurrence).
fn scheduled_queue(events: &[WorkflowEvent], activity: &str) -> String {
    events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::ActivityScheduled { name, queue, .. } if name == activity => {
                Some(queue.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no ActivityScheduled event for '{activity}'"))
}

/// Every `MarkerRecorded` name in history order.
fn marker_names(events: &[WorkflowEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::MarkerRecorded { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// Buffers the two saga counters so tests can assert exact emission counts.
#[derive(Default)]
struct CompRecordingMetrics {
    samples: Mutex<Vec<&'static str>>,
}

impl CompRecordingMetrics {
    fn count(&self, metric: &str) -> usize {
        self.samples
            .lock()
            .expect("metrics lock")
            .iter()
            .filter(|name| **name == metric)
            .count()
    }
}

impl MetricsRecorder for CompRecordingMetrics {
    fn record_saga_compensated(&self, _workflow_name: &str, _queue: &str) {
        self.samples
            .lock()
            .expect("metrics lock")
            .push(METRIC_SAGA_COMPENSATED);
    }

    fn record_saga_compensation_failed(&self, _workflow_name: &str, _queue: &str) {
        self.samples
            .lock()
            .expect("metrics lock")
            .push(METRIC_SAGA_COMPENSATION_FAILED);
    }
}

// ===========================================================================
// T8 — reverse topological (LIFO) unwind order over a diamond
// ===========================================================================

#[activity]
async fn fan_a(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("a"))
}
#[activity]
async fn fan_b(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("b"))
}
#[activity]
async fn fan_c(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("c"))
}
#[activity]
async fn fan_d(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("d"))
}
#[activity]
async fn comp_a(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn comp_b(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn comp_c(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

/// Diamond: a → {b, c} → d. `d` fails; a/b/c are compensable.
/// Index order 0=a, 1=b, 2=c, 3=d. Levels: [0], [1,2], [3].
#[dag]
fn diamond_comp_dag(dag: &mut DagBuilder) {
    let a = dag.activity(fan_a).compensate(comp_a);
    let b = dag.activity(fan_b).upstream(&a).compensate(comp_b);
    let c = dag.activity(fan_c).upstream(&a).compensate(comp_c);
    let _d = dag.activity(fan_d).upstream(&b).upstream(&c);
}

/// T8 — on terminal failure, compensators for the succeeded nodes run in
/// **reverse topological order**: push order is levels-forward / ascending
/// index (`a`, `b`, `c`), so the LIFO unwind dispatches `comp_c`, `comp_b`,
/// `comp_a` — exactly, and in that order.
#[tokio::test]
async fn terminal_failure_compensates_succeeded_nodes_in_reverse_topological_order() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("fan_a", |_| Ok(json!("a")))
        .mock_activity("fan_b", |_| Ok(json!("b")))
        .mock_activity("fan_c", |_| Ok(json!("c")))
        .mock_activity("fan_d", |_| Err("d exploded".to_string()))
        .mock_activity("comp_a", |_| Ok(Value::Null))
        .mock_activity("comp_b", |_| Ok(Value::Null))
        .mock_activity("comp_c", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_diamond_comp_dag().handler,
            Value::Null,
        )
        .await;

    // A SUCCESSFUL unwind still returns the ORIGINAL DAG error.
    assert_eq!(
        outcome.result.as_ref().unwrap_err(),
        "one or more DAG tasks failed",
        "a successful unwind must return the original DAG error unchanged"
    );

    assert_eq!(
        scheduled_names_among(outcome.events(), &["comp_a", "comp_b", "comp_c"]),
        vec![
            "comp_c".to_string(),
            "comp_b".to_string(),
            "comp_a".to_string()
        ],
        "compensators must dispatch in reverse topological (LIFO) order"
    );
}

// ===========================================================================
// T9 — the compensator envelope (unbound root node)
// ===========================================================================

#[activity]
async fn env_root(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "reservation": "rsv-1" }))
}
#[activity]
async fn env_fail(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("nope".to_string())
}
#[activity]
async fn env_undo_root(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

#[dag]
fn envelope_dag(dag: &mut DagBuilder) {
    let root = dag.activity(env_root).compensate(env_undo_root);
    let _fail = dag.activity(env_fail).upstream(&root);
}

/// T9 — the compensator's RECORDED input is the fixed envelope carrying the
/// compensated node's identity, its resolved forward input, and its recorded
/// output. For an UNBOUND root node with a non-object trigger input the forward
/// input is the `{"conf": …, "dag_task": …}` wrapper.
#[tokio::test]
async fn compensator_receives_recorded_input_and_output() {
    let trigger = json!("run-7");
    let outcome = WorkflowTestEnv::new()
        .mock_activity("env_root", |_| Ok(json!({ "reservation": "rsv-1" })))
        .mock_activity("env_fail", |_| Err("nope".to_string()))
        .mock_activity("env_undo_root", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_envelope_dag().handler,
            trigger.clone(),
        )
        .await;

    assert!(outcome.result.is_err(), "the DAG must fail");

    assert_eq!(
        scheduled_input(outcome.events(), "env_undo_root"),
        json!({
            "dag_compensate": "env_root",
            "input": wrapped_dag_task_input(&trigger, "env_root"),
            "output": { "reservation": "rsv-1" },
        }),
        "the compensator envelope must carry the node identity, its resolved \
         forward input, and its recorded output"
    );
}

// ===========================================================================
// T10 — the compensator envelope for a BOUND node (issue #702 interaction)
// ===========================================================================

#[activity]
async fn bind_up(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!([1, 2, 3]))
}
#[activity]
async fn bind_mid(_ctx: &ActivityContext, i: Value) -> Result<Value, String> {
    let _ = i;
    Ok(json!({ "rows": 3 }))
}
#[activity]
async fn bind_fail(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("nope".to_string())
}
#[activity]
async fn bind_undo_mid(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

#[dag]
fn bound_comp_dag(dag: &mut DagBuilder) {
    let up = dag.activity(bind_up);
    let mid = dag
        .activity(bind_mid)
        .input_from(&up)
        .compensate(bind_undo_mid);
    let _fail = dag.activity(bind_fail).upstream(&mid);
}

/// T10 — for a node with an `input_from` binding (issue #702) the envelope's
/// `input` is the node's RESOLVED forward input, i.e. the raw upstream output —
/// never the `conf`/`dag_task` wrapper.
#[tokio::test]
async fn compensator_of_a_bound_node_receives_the_bound_input() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("bind_up", |_| Ok(json!([1, 2, 3])))
        .mock_activity("bind_mid", |_| Ok(json!({ "rows": 3 })))
        .mock_activity("bind_fail", |_| Err("nope".to_string()))
        .mock_activity("bind_undo_mid", |_| Ok(Value::Null))
        .run(__autumn_workflow_info_bound_comp_dag().handler, Value::Null)
        .await;

    assert!(outcome.result.is_err(), "the DAG must fail");

    assert_eq!(
        scheduled_input(outcome.events(), "bind_undo_mid"),
        json!({
            "dag_compensate": "bind_mid",
            "input": [1, 2, 3],
            "output": { "rows": 3 },
        }),
        "a bound node's compensator envelope must carry the RAW bound input"
    );
}

// ===========================================================================
// T11 — skipped / never-reached / uncompensated nodes invoke nothing
// ===========================================================================

#[activity]
async fn sk_root(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("root"))
}
#[activity]
async fn sk_cond(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("cond"))
}
#[activity]
async fn sk_downstream(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("down"))
}
#[activity]
async fn sk_plain(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("plain"))
}
#[activity]
async fn sk_fail(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("nope".to_string())
}
#[activity]
async fn sk_undo_root(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn sk_undo_cond(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn sk_undo_downstream(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

/// `sk_root` succeeds (compensable). `sk_cond` is condition-skipped
/// (compensable, but never ran). `sk_downstream` is trigger-rule-skipped
/// (compensable, never reached). `sk_plain` succeeds but declares NO
/// compensator. `sk_fail` fails and drives the terminal unwind.
#[dag]
fn skip_comp_dag(dag: &mut DagBuilder) {
    let root = dag.activity(sk_root).compensate(sk_undo_root);
    let cond = dag
        .activity(sk_cond)
        .upstream(&root)
        .condition(|_ups| false) // always condition-skipped
        .compensate(sk_undo_cond);
    let _down = dag
        .activity(sk_downstream)
        .upstream(&cond) // AllSuccess over a skipped upstream → trigger-rule skip
        .compensate(sk_undo_downstream);
    let _plain = dag.activity(sk_plain).upstream(&root); // no compensator
    let _fail = dag.activity(sk_fail).upstream(&root);
}

/// T11 — only a node that BOTH succeeded AND declares a compensator is
/// compensated. Condition-skipped, trigger-rule-skipped, and compensator-less
/// nodes invoke nothing.
#[tokio::test]
async fn skipped_and_never_reached_and_uncompensated_nodes_invoke_nothing() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("sk_root", |_| Ok(json!("root")))
        .mock_activity("sk_plain", |_| Ok(json!("plain")))
        .mock_activity("sk_fail", |_| Err("nope".to_string()))
        .mock_activity("sk_undo_root", |_| Ok(Value::Null))
        // Deliberately registered so a spurious dispatch SUCCEEDS and is caught
        // by the assertion below rather than masked by a missing-mock stall.
        .mock_activity("sk_undo_cond", |_| Ok(Value::Null))
        .mock_activity("sk_undo_downstream", |_| Ok(Value::Null))
        .run(__autumn_workflow_info_skip_comp_dag().handler, Value::Null)
        .await;

    assert_eq!(
        outcome.result.as_ref().unwrap_err(),
        "one or more DAG tasks failed"
    );

    let scheduled = all_scheduled_names(outcome.events());
    assert!(
        scheduled.contains(&"sk_undo_root".to_string()),
        "the succeeded compensable node must be compensated, got {scheduled:?}"
    );
    assert!(
        !scheduled.contains(&"sk_undo_cond".to_string()),
        "a CONDITION-skipped node must not be compensated, got {scheduled:?}"
    );
    assert!(
        !scheduled.contains(&"sk_undo_downstream".to_string()),
        "a TRIGGER-RULE-skipped (never reached) node must not be compensated, got {scheduled:?}"
    );
    // The compensator-less succeeded node contributes nothing to the unwind.
    assert_eq!(
        scheduled_names_among(
            outcome.events(),
            &["sk_undo_root", "sk_undo_cond", "sk_undo_downstream"],
        ),
        vec!["sk_undo_root".to_string()],
        "exactly one compensator must dispatch"
    );
}

// ===========================================================================
// T12 — the failed node itself is never compensated
// ===========================================================================

#[activity]
async fn t12_ok(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("ok"))
}
#[activity]
async fn t12_bad(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("bad".to_string())
}
#[activity]
async fn t12_undo_ok(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn t12_undo_bad(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

#[dag]
fn failed_node_comp_dag(dag: &mut DagBuilder) {
    let ok = dag.activity(t12_ok).compensate(t12_undo_ok);
    // The FAILING node also declares a compensator — it must never run, since
    // a failed step has (by the saga contract) nothing to undo.
    let _bad = dag.activity(t12_bad).upstream(&ok).compensate(t12_undo_bad);
}

/// T12 — a node that FAILED is never compensated, even when it declares a
/// compensator: only successful forward steps have an effect to undo.
#[tokio::test]
async fn the_failed_node_itself_is_never_compensated() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("t12_ok", |_| Ok(json!("ok")))
        .mock_activity("t12_bad", |_| Err("bad".to_string()))
        .mock_activity("t12_undo_ok", |_| Ok(Value::Null))
        .mock_activity("t12_undo_bad", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_failed_node_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(outcome.result.is_err());
    assert_eq!(
        scheduled_names_among(outcome.events(), &["t12_undo_ok", "t12_undo_bad"]),
        vec!["t12_undo_ok".to_string()],
        "the FAILED node's own compensator must never dispatch"
    );
}

// ===========================================================================
// T13 — mapped-node compensation is NODE-granular, not cell-granular
// ===========================================================================

#[activity]
async fn m_root(_ctx: &ActivityContext) -> Result<Vec<i32>, String> {
    Ok(vec![1, -1])
}
#[activity]
async fn m_process(_ctx: &ActivityContext, item: i32) -> Result<i32, String> {
    if item < 0 {
        return Err("negative".to_string());
    }
    Ok(item * 10)
}
#[activity]
async fn m_fail(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("nope".to_string())
}
#[activity]
async fn m_undo_root(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn m_undo_process(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

/// `CollectAll` mapped node: the node SUCCEEDS even with failed cells, so it is
/// compensated exactly once at node granularity.
#[dag]
fn mapped_collect_comp_dag(dag: &mut DagBuilder) {
    let root = dag.activity(m_root).compensate(m_undo_root);
    let process = dag
        .map_activity(m_process)
        .over(&root)
        .map_failure_policy(MapFailurePolicy::CollectAll)
        .compensate(m_undo_process);
    let _fail = dag.activity(m_fail).upstream(&process);
}

/// `FailFast` mapped node: a single failed cell FAILS the node, so it is not
/// compensated at all.
#[dag]
fn mapped_failfast_comp_dag(dag: &mut DagBuilder) {
    let root = dag.activity(m_root).compensate(m_undo_root);
    let _process = dag
        .map_activity(m_process)
        .over(&root)
        .map_failure_policy(MapFailurePolicy::FailFast)
        .compensate(m_undo_process);
}

/// T13 — mapped-node compensation is **node**-granular: a `CollectAll` mapped
/// node that reaches `Succeeded` (despite failed cells) is compensated exactly
/// ONCE with the whole cells array as its recorded output; a `FailFast` mapped
/// node that a failed cell drove to `Failed` is NOT compensated at all.
#[tokio::test]
async fn mapped_node_compensation_is_node_granular_not_cell_granular() {
    // (a) CollectAll → node Succeeded → compensated ONCE, output = cells array.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("m_root", |_| Ok(json!([1, -1])))
        .mock_activity("m_process", |item| {
            if item.as_i64().is_some_and(|n| n < 0) {
                Err("negative".to_string())
            } else {
                Ok(json!(item.as_i64().unwrap_or_default() * 10))
            }
        })
        .mock_activity("m_fail", |_| Err("nope".to_string()))
        .mock_activity("m_undo_root", |_| Ok(Value::Null))
        .mock_activity("m_undo_process", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_mapped_collect_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(outcome.result.is_err(), "the DAG must fail");

    // Compensated exactly once (node granularity — NOT once per cell), and the
    // unwind is LIFO over [m_root, m_process].
    assert_eq!(
        scheduled_names_among(outcome.events(), &["m_undo_root", "m_undo_process"]),
        vec!["m_undo_process".to_string(), "m_undo_root".to_string()],
        "a mapped node must be compensated exactly ONCE, at node granularity"
    );

    let envelope = scheduled_input(outcome.events(), "m_undo_process");
    assert_eq!(
        envelope.get("dag_compensate"),
        Some(&json!("m_process")),
        "the envelope must identify the mapped node, got {envelope}"
    );
    // The compensated output is the WHOLE CollectAll cells array, including the
    // failed cell — node granularity, not "only the succeeded cells". The
    // failed cell's `error` string is produced by the engine's own
    // `ActivityFailed` formatting, so it is matched by substring rather than
    // pinned verbatim.
    let cells = envelope
        .get("output")
        .and_then(Value::as_array)
        .unwrap_or_else(|| {
            panic!("the mapped node's output must be the cells array, got {envelope}")
        });
    assert_eq!(
        cells.len(),
        2,
        "the cells array must carry BOTH cells (the failed one included), got {envelope}"
    );
    assert_eq!(
        cells[0],
        json!({ "status": "succeeded", "value": 10 }),
        "the succeeded cell must be carried verbatim, got {envelope}"
    );
    assert_eq!(
        cells[1].get("status"),
        Some(&json!("failed")),
        "the FAILED cell must be carried too — mapped compensation is node-granular, got {envelope}"
    );
    assert!(
        cells[1]
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|e| e.contains("negative")),
        "the failed cell must carry its error, got {envelope}"
    );
    assert!(
        envelope.get("input").is_some(),
        "the envelope must always carry an `input` key, got {envelope}"
    );

    // (b) FailFast → a failed cell FAILS the node → NOT compensated.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("m_root", |_| Ok(json!([1, -1])))
        .mock_activity("m_process", |item| {
            if item.as_i64().is_some_and(|n| n < 0) {
                Err("negative".to_string())
            } else {
                Ok(json!(item.as_i64().unwrap_or_default() * 10))
            }
        })
        .mock_activity("m_undo_root", |_| Ok(Value::Null))
        .mock_activity("m_undo_process", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_mapped_failfast_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(outcome.result.is_err(), "the DAG must fail");
    assert_eq!(
        scheduled_names_among(outcome.events(), &["m_undo_root", "m_undo_process"]),
        vec!["m_undo_root".to_string()],
        "a FailFast mapped node driven to Failed by one cell must NOT be \
         compensated (partial cell success is not node success)"
    );
}

// ===========================================================================
// T14 — cancellation does NOT auto-compensate
// ===========================================================================

#[activity]
async fn c_ok(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("ok"))
}
#[activity]
async fn c_bad(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("bad".to_string())
}
#[activity]
async fn c_undo_ok(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

#[dag]
fn cancel_comp_dag(dag: &mut DagBuilder) {
    let ok = dag.activity(c_ok).compensate(c_undo_ok);
    let _bad = dag.activity(c_bad).upstream(&ok);
}

/// Forward history for `cancel_comp_dag`: `c_ok` succeeded, `c_bad` failed.
fn cancel_forward_history() -> Vec<WorkflowEvent> {
    let ok = ActivityExecId::new();
    let bad = ActivityExecId::new();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: ok,
            name: "c_ok".into(),
            input: wrapped_dag_task_input(&Value::Null, "c_ok"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: ok,
            output: json!("ok"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: bad,
            name: "c_bad".into(),
            input: wrapped_dag_task_input(&Value::Null, "c_bad"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: bad,
            error: "bad".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
    ]
}

/// T14 — **cancellation does not auto-compensate** (`docs/saga.md`). A cancelled
/// run whose node failed must dispatch ZERO compensators, record no
/// `saga_compensated:` marker, and return the original DAG error unchanged.
///
/// Driven over a hand-built history on a `for_replay` context rather than
/// `WorkflowTestEnv::with_cancellation`: that helper injects `WorkflowCancelled`
/// as event #1, which diverges the DAG's very FIRST activity dispatch
/// (`expected ActivityScheduled(c_ok), got WorkflowCancelled`) so the run never
/// reaches the terminal-failure check under test. Mirrors the mechanics of
/// `saga_tests.rs::known_limitation_durable_compensation_after_recorded_cancel_fails_and_is_counted`.
#[tokio::test]
async fn cancellation_does_not_auto_compensate() {
    let mut history = cancel_forward_history();
    history.push(WorkflowEvent::WorkflowCancelled {
        reason: "operator shutdown".into(),
    });

    let recorder = Arc::new(CompRecordingMetrics::default());
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history)
        .with_workflow_name("cancel_comp_dag")
        .with_queue_name("default")
        .with_metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>);

    let handler = __autumn_workflow_info_cancel_comp_dag().handler;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handler(&ctx, Value::Null),
    )
    .await
    .expect("a cancelled DAG run must terminate, not suspend forever");

    assert_eq!(
        result.as_ref().unwrap_err(),
        "one or more DAG tasks failed",
        "the terminal error must be the ORIGINAL DAG error — cancellation must \
         neither unwind nor rewrite it"
    );

    let commands = ctx.drain_commands();
    let scheduled: Vec<String> = commands
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::ScheduleActivity { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !scheduled.contains(&"c_undo_ok".to_string()),
        "a cancelled run must dispatch ZERO compensators, got {scheduled:?}"
    );

    let markers: Vec<String> = commands
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::RecordMarker { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !markers.iter().any(|m| m.starts_with("saga_compensated:")),
        "no unwind began, so no saga_compensated marker may be recorded, got {markers:?}"
    );
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATED),
        0,
        "a skipped unwind must emit no saga metric"
    );
}

/// T15 — a cancel landing MID-unwind terminates and never nd-blocks.
///
/// History carries a partially-recorded unwind (`saga_compensated:1` marker plus
/// one dispatched compensator) followed by `WorkflowCancelled`. Whatever the
/// engine decides to do with the remaining compensations, the run must
/// TERMINATE with an error and must not leave a deferred non-determinism error
/// behind (which issue #603 would turn into a permanent nd-block).
#[tokio::test]
async fn cancel_landing_mid_unwind_terminates_and_never_nd_blocks() {
    let undo = ActivityExecId::new();
    let mut history = cancel_forward_history();
    history.extend([
        WorkflowEvent::MarkerRecorded {
            name: "saga_compensated:1".into(),
            details: json!(1),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: undo,
            name: "c_undo_ok".into(),
            input: json!({
                "dag_compensate": "c_ok",
                "input": wrapped_dag_task_input(&Value::Null, "c_ok"),
                "output": "ok",
            }),
            queue: String::new(),
        },
        WorkflowEvent::WorkflowCancelled {
            reason: "operator shutdown mid-unwind".into(),
        },
    ]);

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);
    let handler = __autumn_workflow_info_cancel_comp_dag().handler;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handler(&ctx, Value::Null),
    )
    .await
    .expect("a cancel landing mid-unwind must terminate, not suspend forever");

    assert!(
        result.is_err(),
        "a run whose DAG failed must terminate with an error, got {result:?}"
    );
    assert_eq!(
        ctx.take_deferred_nd_error(),
        None,
        "a cancel landing mid-unwind must never record a deferred \
         non-determinism error (issue #603 would nd-block the run forever)"
    );
}

// ===========================================================================
// T16 — a failing compensator surfaces SagaCompensationFailed
// ===========================================================================

#[activity]
async fn f_a(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("a"))
}
#[activity]
async fn f_b(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("b"))
}
#[activity]
async fn f_bad(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("bad".to_string())
}
#[activity]
async fn f_undo_a(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn f_undo_b(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

#[dag]
fn failing_comp_dag(dag: &mut DagBuilder) {
    let a = dag.activity(f_a).compensate(f_undo_a);
    let b = dag.activity(f_b).upstream(&a).compensate(f_undo_b);
    let _bad = dag.activity(f_bad).upstream(&b);
}

/// T16 — when a compensator itself fails the unwind still attempts EVERY
/// remaining compensation (continue-not-abort), then surfaces
/// `SagaCompensationFailed` carrying both the original DAG error and the
/// compensation error, records the `saga_compensation_failed:1` dedup marker,
/// and fires the page-severity `harvest.saga.compensation_failed` counter.
#[tokio::test]
async fn compensator_failure_surfaces_saga_compensation_failed() {
    let recorder = Arc::new(CompRecordingMetrics::default());
    let outcome = WorkflowTestEnv::new()
        .with_workflow_name("failing_comp_dag")
        .with_queue_name("default")
        .with_metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>)
        .mock_activity("f_a", |_| Ok(json!("a")))
        .mock_activity("f_b", |_| Ok(json!("b")))
        .mock_activity("f_bad", |_| Err("bad".to_string()))
        // The LIFO-first compensator fails …
        .mock_activity("f_undo_b", |_| Err("undo_b exploded".to_string()))
        // … and the next one must STILL be attempted.
        .mock_activity("f_undo_a", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_failing_comp_dag().handler,
            Value::Null,
        )
        .await;

    let err = outcome
        .result
        .as_ref()
        .expect_err("a failed unwind must surface an error");
    assert!(
        err.contains("saga compensation failed"),
        "a failed unwind must surface SagaCompensationFailed, got: {err}"
    );
    assert!(
        err.contains("one or more DAG tasks failed"),
        "the original DAG error must be preserved in the failure, got: {err}"
    );
    assert!(
        err.contains("undo_b exploded"),
        "the compensation error must be preserved in the failure, got: {err}"
    );

    // Continue-not-abort: the remaining compensator is still dispatched.
    assert_eq!(
        scheduled_names_among(outcome.events(), &["f_undo_a", "f_undo_b"]),
        vec!["f_undo_b".to_string(), "f_undo_a".to_string()],
        "a failing compensation must not abort the rest of the unwind"
    );

    assert!(
        marker_names(outcome.events()).contains(&"saga_compensation_failed:1".to_string()),
        "the failed-unwind dedup marker must be recorded, got {:?}",
        marker_names(outcome.events())
    );
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATION_FAILED),
        1,
        "the dangling-state page must fire exactly once"
    );
}

// ===========================================================================
// T17 — compensation is recorded as ORDINARY activity events only
// ===========================================================================

/// Event type names the compensation flow is permitted to produce. A NEW
/// `WorkflowEvent` variant (which issue #780 must NOT introduce) would surface
/// here as an unknown type name.
const ALLOWED_EVENT_TYPES: &[&str] = &[
    "WorkflowStarted",
    "ActivityScheduled",
    "ActivityStarted",
    "ActivityCompleted",
    "ActivityFailed",
    "MarkerRecorded",
    "WorkflowCompleted",
    "WorkflowFailed",
];

/// T17 — compensation introduces **no new `WorkflowEvent` variant**: it rides
/// ordinary `ActivityScheduled`/`ActivityCompleted` events, and the only marker
/// it adds is the `Saga` dedup marker `saga_compensated:1`.
#[tokio::test]
async fn compensation_is_recorded_as_ordinary_activity_events_only() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("t12_ok", |_| Ok(json!("ok")))
        .mock_activity("t12_bad", |_| Err("bad".to_string()))
        .mock_activity("t12_undo_ok", |_| Ok(Value::Null))
        .mock_activity("t12_undo_bad", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_failed_node_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(outcome.result.is_err());

    for event in outcome.events() {
        assert!(
            ALLOWED_EVENT_TYPES.contains(&event.type_name()),
            "compensation must add NO new WorkflowEvent variant; saw '{}'",
            event.type_name()
        );
    }

    // The compensation itself is an ordinary scheduled+completed activity.
    assert!(
        outcome.events().iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityScheduled { name, .. } if name == "t12_undo_ok"
        )),
        "the compensator must be recorded as an ordinary ActivityScheduled"
    );
    assert!(
        outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "the compensator must be recorded as an ordinary ActivityCompleted"
    );

    // Exactly one new marker — the Saga unwind-start dedup marker. This DAG has
    // no condition skips, so no `dag_skip:` markers are expected.
    assert_eq!(
        marker_names(outcome.events()),
        vec!["saga_compensated:1".to_string()],
        "the ONLY marker compensation adds is the saga unwind-start dedup marker"
    );
}

// ===========================================================================
// T18 — a compensator dispatches on its node's queue
// ===========================================================================

#[activity]
async fn q_queued(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("queued"))
}
#[activity]
async fn q_plain(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("plain"))
}
#[activity]
async fn q_bad(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("bad".to_string())
}
#[activity]
async fn q_undo_queued(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn q_undo_plain(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

#[dag]
fn queued_comp_dag(dag: &mut DagBuilder) {
    let queued = dag
        .activity(q_queued)
        .queue("inventory")
        .compensate(q_undo_queued);
    let plain = dag.activity(q_plain).compensate(q_undo_plain);
    let _bad = dag.activity(q_bad).upstream(&queued).upstream(&plain);
}

/// T18 — a compensator is dispatched on the **compensated node's** queue, so an
/// undo lands on the same worker pool that performed the forward step. A node
/// with no queue yields the empty-string queue, exactly like its forward
/// dispatch.
#[tokio::test]
async fn compensator_dispatches_on_the_nodes_queue() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("q_queued", |_| Ok(json!("queued")))
        .mock_activity("q_plain", |_| Ok(json!("plain")))
        .mock_activity("q_bad", |_| Err("bad".to_string()))
        .mock_activity("q_undo_queued", |_| Ok(Value::Null))
        .mock_activity("q_undo_plain", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_queued_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(outcome.result.is_err());

    assert_eq!(
        scheduled_queue(outcome.events(), "q_undo_queued"),
        "inventory",
        "the compensator must dispatch on the compensated node's queue"
    );
    assert_eq!(
        scheduled_queue(outcome.events(), "q_undo_plain"),
        "",
        "an unqueued node's compensator must dispatch on the empty-string queue"
    );
    // Sanity: the forward node itself used the same queue.
    assert_eq!(scheduled_queue(outcome.events(), "q_queued"), "inventory");
}

// ===========================================================================
// T19 — the success path emits no new commands and no saga metrics
// ===========================================================================

#[activity]
async fn s_first(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("first"))
}
#[activity]
async fn s_second(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("second"))
}
#[activity]
async fn s_undo_first(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn s_undo_second(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

#[dag]
fn all_success_comp_dag(dag: &mut DagBuilder) {
    let first = dag.activity(s_first).compensate(s_undo_first);
    let _second = dag
        .activity(s_second)
        .upstream(&first)
        .compensate(s_undo_second);
}

/// T19 — a DAG that declares compensators but SUCCEEDS never constructs a saga:
/// zero compensator dispatches, zero saga markers, zero saga metrics. The
/// success path is byte-for-byte what it was before compensation existed.
#[tokio::test]
async fn success_path_emits_no_new_commands_and_no_saga_metrics() {
    let recorder = Arc::new(CompRecordingMetrics::default());
    let outcome = WorkflowTestEnv::new()
        .with_workflow_name("all_success_comp_dag")
        .with_queue_name("default")
        .with_metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>)
        .mock_activity("s_first", |_| Ok(json!("first")))
        .mock_activity("s_second", |_| Ok(json!("second")))
        .mock_activity("s_undo_first", |_| Ok(Value::Null))
        .mock_activity("s_undo_second", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_all_success_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(
        outcome.result.is_ok(),
        "the all-succeeding DAG must succeed, got {:?}",
        outcome.result
    );

    assert_eq!(
        scheduled_names_among(outcome.events(), &["s_undo_first", "s_undo_second"]),
        Vec::<String>::new(),
        "a successful DAG must dispatch NO compensators"
    );
    assert!(
        !marker_names(outcome.events())
            .iter()
            .any(|m| m.starts_with("saga_")),
        "a successful DAG must record NO saga markers, got {:?}",
        marker_names(outcome.events())
    );
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATED), 0);
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATION_FAILED), 0);
}

// ===========================================================================
// T20 — SUCCESS METRIC: zero uncompensated side effects across 1000 runs
// ===========================================================================

#[activity]
async fn reserve_inventory(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("inv"))
}
#[activity]
async fn charge_payment(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("pay"))
}
#[activity]
async fn allocate_shipment(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("ship"))
}
#[activity]
async fn print_label(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("label"))
}
#[activity]
async fn notify_customer(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("notified"))
}
#[activity]
async fn release_inventory(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn refund_payment(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn deallocate_shipment(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn void_label(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

/// Linear fulfillment chain. Index order 0..4 == topological order.
#[dag]
fn fulfillment_linear_dag(dag: &mut DagBuilder) {
    let r = dag
        .activity(reserve_inventory)
        .compensate(release_inventory);
    let c = dag
        .activity(charge_payment)
        .upstream(&r)
        .compensate(refund_payment);
    let a = dag
        .activity(allocate_shipment)
        .upstream(&c)
        .compensate(deallocate_shipment);
    let p = dag
        .activity(print_label)
        .upstream(&a)
        .compensate(void_label);
    // No compensator — a notification has nothing to undo.
    let _n = dag.activity(notify_customer).upstream(&p);
}

/// Diamond fulfillment variant: reserve → {charge, allocate} → label → notify.
/// Index order 0..4 is still topological, and level-forward / ascending-index
/// order equals index order.
#[dag]
fn fulfillment_diamond_dag(dag: &mut DagBuilder) {
    let r = dag
        .activity(reserve_inventory)
        .compensate(release_inventory);
    let c = dag
        .activity(charge_payment)
        .upstream(&r)
        .compensate(refund_payment);
    let a = dag
        .activity(allocate_shipment)
        .upstream(&r)
        .compensate(deallocate_shipment);
    let p = dag
        .activity(print_label)
        .upstream(&c)
        .upstream(&a)
        .compensate(void_label);
    let _n = dag.activity(notify_customer).upstream(&p);
}

/// Forward node names, in the index (== topological) order both fulfillment
/// DAGs declare.
const FULFILLMENT_NODES: [&str; 5] = [
    "reserve_inventory",
    "charge_payment",
    "allocate_shipment",
    "print_label",
    "notify_customer",
];

/// Compensator per node index (`None` = uncompensated).
const FULFILLMENT_COMPENSATORS: [Option<&str>; 5] = [
    Some("release_inventory"),
    Some("refund_payment"),
    Some("deallocate_shipment"),
    Some("void_label"),
    None,
];

/// The ledger resource each compensable node touches (`None` = no side effect).
const FULFILLMENT_RESOURCES: [Option<&str>; 5] = [
    Some("inventory"),
    Some("payment"),
    Some("shipment"),
    Some("label"),
    None,
];

/// Upstream indices per node for the linear topology.
const LINEAR_UPSTREAMS: [&[usize]; 5] = [&[], &[0], &[1], &[2], &[3]];
/// Upstream indices per node for the diamond topology.
const DIAMOND_UPSTREAMS: [&[usize]; 5] = [&[], &[0], &[0], &[1, 2], &[3]];

/// Independently model which nodes succeed given the failing node index.
/// Indices are in topological order, so a single forward pass suffices: a node
/// succeeds iff every upstream succeeded and it is not itself the failing node.
fn succeeded_mask(upstreams: &[&[usize]; 5], fail_idx: usize) -> [bool; 5] {
    let mut succeeded = [false; 5];
    for i in 0..5 {
        let ups_ok = upstreams[i].iter().all(|&u| succeeded[u]);
        succeeded[i] = ups_ok && i != fail_idx;
    }
    succeeded
}

/// The compensator dispatch order the unwind must produce: push in
/// levels-forward / ascending-index order (== index order for both topologies),
/// pop LIFO.
fn expected_unwind(upstreams: &[&[usize]; 5], fail_idx: usize) -> Vec<String> {
    let succeeded = succeeded_mask(upstreams, fail_idx);
    let mut pushed: Vec<String> = Vec::new();
    for i in 0..5 {
        if succeeded[i]
            && let Some(comp) = FULFILLMENT_COMPENSATORS[i]
        {
            pushed.push(comp.to_string());
        }
    }
    pushed.reverse();
    pushed
}

/// Deterministic LCG (no `rand` dependency) so the sweep is reproducible.
const fn lcg_next(seed: u64) -> u64 {
    seed.wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407)
}

type Ledger = Arc<Mutex<BTreeMap<String, i64>>>;

fn bump(ledger: &Ledger, resource: &str, delta: i64) {
    *ledger
        .lock()
        .expect("ledger lock")
        .entry(resource.to_string())
        .or_insert(0) += delta;
}

/// T20 — **SUCCESS METRIC.** Across 1000 deterministically-seeded fulfillment
/// runs (two topologies × every failure position), a compensating DAG leaves
/// **zero uncompensated side effects**: every ledger resource nets to 0, the
/// compensator dispatch order is exactly the reverse of the compensable
/// succeeded prefix, and every produced history replays deterministically.
#[tokio::test]
async fn fulfillment_dag_leaves_zero_uncompensated_side_effects_across_1000_runs() {
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;

    for iteration in 0..1000u32 {
        seed = lcg_next(seed);
        // Failure position 1..=5 → index 0..=4.
        let fail_idx = ((seed >> 33) % 5) as usize;
        let use_diamond = (seed >> 60) % 2 == 1;

        let (handler, upstreams, dag_label) = if use_diamond {
            (
                __autumn_workflow_info_fulfillment_diamond_dag().handler,
                &DIAMOND_UPSTREAMS,
                "diamond",
            )
        } else {
            (
                __autumn_workflow_info_fulfillment_linear_dag().handler,
                &LINEAR_UPSTREAMS,
                "linear",
            )
        };

        // Fresh ledger per iteration — forward steps credit, compensators debit.
        let ledger: Ledger = Arc::new(Mutex::new(BTreeMap::new()));

        let mut env = WorkflowTestEnv::new();
        for (idx, node) in FULFILLMENT_NODES.iter().enumerate() {
            let resource = FULFILLMENT_RESOURCES[idx].map(str::to_string);
            let should_fail = idx == fail_idx;
            let ledger_for_node = Arc::clone(&ledger);
            env = env.mock_activity(*node, move |_| {
                if should_fail {
                    // A failed step performs no side effect, so it credits nothing.
                    return Err("step failed".to_string());
                }
                if let Some(resource) = &resource {
                    bump(&ledger_for_node, resource, 1);
                }
                Ok(json!("ok"))
            });
        }
        for (idx, compensator) in FULFILLMENT_COMPENSATORS.iter().enumerate() {
            let Some(compensator) = *compensator else {
                continue;
            };
            let resource = FULFILLMENT_RESOURCES[idx]
                .expect("a compensable node must own a ledger resource")
                .to_string();
            let ledger_for_comp = Arc::clone(&ledger);
            env = env.mock_activity(compensator, move |_| {
                bump(&ledger_for_comp, &resource, -1);
                Ok(Value::Null)
            });
        }

        let outcome = env.run(handler, Value::Null).await;

        let context = format!("iteration {iteration} ({dag_label}, fail_idx {fail_idx})");

        assert!(
            outcome.result.is_err(),
            "{context}: a DAG with a failing node must fail, got {:?}",
            outcome.result
        );

        // (a) Zero uncompensated side effects.
        for (resource, balance) in ledger.lock().expect("ledger lock").iter() {
            assert_eq!(
                *balance, 0,
                "{context}: resource '{resource}' left uncompensated (balance {balance})"
            );
        }

        // (b) Exact reverse-of-compensable-succeeded-prefix dispatch order.
        let compensator_names: Vec<&str> =
            FULFILLMENT_COMPENSATORS.iter().flatten().copied().collect();
        assert_eq!(
            scheduled_names_among(outcome.events(), &compensator_names),
            expected_unwind(upstreams, fail_idx),
            "{context}: compensator dispatch order must be the exact reverse of \
             the compensable succeeded prefix"
        );

        // (c) The produced history replays deterministically.
        //
        // A failing DAG's handler returns `Err("one or more DAG tasks failed")`,
        // and `ReplayStatus::ReplaySucceeded` is reserved for a handler that
        // returns `Ok` — so *every* failing-DAG history (compensating or not)
        // replays to `ReplayStatus::WorkflowFailed`, which is precisely
        // "reproduced the same terminal error with ZERO divergence". Pinning
        // the exact error rules out `NonDeterminismDetected`, matching the
        // repo's established failing-DAG replay shape
        // (`dag_unified_tests.rs`, `dag_input_binding_tests.rs`,
        // `dag_mapping_tests.rs`, `dag_signal_gate_tests.rs`).
        let report = outcome.replay_check(handler).await;
        assert!(
            matches!(
                report.status,
                ReplayStatus::WorkflowFailed { ref error, .. }
                    if error == "one or more DAG tasks failed"
            ),
            "{context}: a compensating history must replay deterministically \
             (WorkflowFailed with the original DAG error, never \
             NonDeterminismDetected):\n{report}"
        );
    }
}

// ===========================================================================
// T24 — the suite is wired into CI
// ===========================================================================

/// The CI integration-suite manifest (`.github/ci/integration-suites.txt`).
const CI_SUITE_MANIFEST: &str = include_str!("../../../.github/ci/integration-suites.txt");

/// T24 — this suite must have a real CI run row, otherwise it is only ever
/// compile-checked and its guarantees never actually execute.
///
/// Required row shape: osclass `linux`, crate `autumn-harvest`, target
/// `integration`, features containing `testing` (the file is
/// `#![cfg(all(feature = "unified-dag-execution", feature = "testing"))]`, and
/// `linux` rows keep the crate defaults which already supply
/// `unified-dag-execution`), and a filter that selects this module.
#[test]
fn dag_compensation_suite_is_wired_into_ci() {
    let wired = CI_SUITE_MANIFEST.lines().any(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return false;
        }
        let cols: Vec<&str> = trimmed.split_whitespace().collect();
        if cols.len() < 5 {
            return false;
        }
        let (osclass, krate, target, features, filter) =
            (cols[0], cols[1], cols[2], cols[3], cols[4]);
        osclass == "linux"
            && krate == "autumn-harvest"
            && target == "integration"
            && features.split(',').any(|f| f.trim() == "testing")
            // A partial `module::test` filter would not credit the whole module,
            // so require the filter to be a prefix of the module name.
            && "dag_compensation_tests".starts_with(filter)
    });

    assert!(
        wired,
        "issue #780's suite must be wired into .github/ci/integration-suites.txt \
         with a row like:\n  \
         linux        autumn-harvest         integration                      testing                    dag_compensation_tests"
    );
}
