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

/// Buffers the two saga counters — with their `(workflow, queue)` labels — so
/// tests can assert both exact emission counts and correct labelling.
#[derive(Default)]
struct CompRecordingMetrics {
    samples: Mutex<Vec<(&'static str, String, String)>>,
}

impl CompRecordingMetrics {
    fn count(&self, metric: &str) -> usize {
        self.samples
            .lock()
            .expect("metrics lock")
            .iter()
            .filter(|(name, _, _)| *name == metric)
            .count()
    }

    /// The `(workflow, queue)` label pairs recorded for `metric`, in order.
    fn labels(&self, metric: &str) -> Vec<(String, String)> {
        self.samples
            .lock()
            .expect("metrics lock")
            .iter()
            .filter(|(name, _, _)| *name == metric)
            .map(|(_, workflow, queue)| (workflow.clone(), queue.clone()))
            .collect()
    }
}

impl MetricsRecorder for CompRecordingMetrics {
    fn record_saga_compensated(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().expect("metrics lock").push((
            METRIC_SAGA_COMPENSATED,
            workflow_name.to_string(),
            queue.to_string(),
        ));
    }

    fn record_saga_compensation_failed(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().expect("metrics lock").push((
            METRIC_SAGA_COMPENSATION_FAILED,
            workflow_name.to_string(),
            queue.to_string(),
        ));
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

/// A mapped node over an EMPTY upstream array dispatches zero forward
/// instances but is still marked `Succeeded` (a pre-existing, tested unified-DAG
/// behaviour — see `dag_mapping_tests::empty_upstream_collection_mapping`).
/// Compensating it would undo an activity that never executed — a refund for a
/// charge that never happened.
#[dag]
fn mapped_empty_comp_dag(dag: &mut DagBuilder) {
    let root = dag.activity(m_root).compensate(m_undo_root);
    let process = dag
        .map_activity(m_process)
        .over(&root)
        .compensate(m_undo_process);
    let _fail = dag.activity(m_fail).upstream(&process);
}

/// A mapped node whose upstream output is NOT a JSON array cannot run. The
/// upstream that already succeeded still has a side effect to undo.
#[dag]
fn mapped_non_array_comp_dag(dag: &mut DagBuilder) {
    let root = dag.activity(m_root).compensate(m_undo_root);
    let _process = dag
        .map_activity(m_process)
        .over(&root)
        .compensate(m_undo_process);
}

/// A zero-instance mapped node reaches `Succeeded` without ever dispatching a
/// forward activity, so there is nothing to undo. Compensating it would be a
/// spurious rollback of work that never happened.
#[tokio::test]
async fn an_empty_mapped_fan_out_is_never_compensated() {
    let outcome = WorkflowTestEnv::new()
        // Empty upstream array → the mapped node dispatches zero instances.
        .mock_activity("m_root", |_| Ok(json!([])))
        .mock_activity("m_process", |_| Ok(Value::Null))
        .mock_activity("m_fail", |_| Err("nope".to_string()))
        .mock_activity("m_undo_root", |_| Ok(Value::Null))
        .mock_activity("m_undo_process", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_mapped_empty_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(outcome.result.is_err());
    let dispatched = scheduled_names_among(outcome.events(), &["m_undo_root", "m_undo_process"]);
    assert_eq!(
        dispatched,
        vec!["m_undo_root".to_string()],
        "the zero-instance mapped node must NOT be compensated (its forward \
         activity never ran); the real upstream still must be"
    );
}

/// A mapped node fed a non-array upstream output fails the DAG deterministically
/// before dispatching anything. The upstream's side effect is still real, so the
/// unwind must run — while the precise diagnostic stays operator-visible.
#[tokio::test]
async fn a_non_array_mapped_input_still_unwinds_the_succeeded_upstream() {
    let outcome = WorkflowTestEnv::new()
        // NOT an array → the mapped node cannot run.
        .mock_activity("m_root", |_| Ok(json!({"not": "an array"})))
        .mock_activity("m_process", |_| Ok(Value::Null))
        .mock_activity("m_undo_root", |_| Ok(Value::Null))
        .mock_activity("m_undo_process", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_mapped_non_array_comp_dag().handler,
            Value::Null,
        )
        .await;

    let err = outcome.result.clone().expect_err("the DAG must fail");
    assert!(
        err.contains("not a JSON array"),
        "the specific shape diagnostic must stay operator-visible, got: {err}"
    );
    let dispatched = scheduled_names_among(outcome.events(), &["m_undo_root", "m_undo_process"]);
    assert_eq!(
        dispatched,
        vec!["m_undo_root".to_string()],
        "the succeeded upstream must be compensated even though the DAG failed \
         on a deterministic input-shape error; the mapped node itself never ran"
    );
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
    // The mapped node's recorded forward INPUT is the whole upstream array —
    // node granularity again, NOT any single cell's item. Pinned by exact value:
    // `is_some()` would also accept `Null`, which is what deleting the
    // mapped-node `inputs[task_idx] = upstream_val` assignment produces.
    assert_eq!(
        envelope.get("input"),
        Some(&json!([1, -1])),
        "the envelope's `input` must be the WHOLE mapped upstream array, got {envelope}"
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
// T13b — post-review P1-B: a FailFast mapped node whose failing cell is NOT
//        the last one polled must still unwind cleanly
// ===========================================================================

/// Drive `history` to completion on a fresh replay context and report the
/// terminal result together with the engine's non-determinism slot.
///
/// Issue #603 gates the permanent nd-block on `WorkflowContext::take_nd_details`
/// (the executor reads it on the `Ok(Err(..))` arm), NOT on
/// `take_deferred_nd_error` — which only the infallible issue #384 primitives
/// ever write. A test that asserts the deferred slot would pass even while the
/// run wedges forever, so every "must never nd-block" assertion in this suite
/// reads `nd_details`.
async fn drive_replay_for_nd_details(
    handler: autumn_harvest::WorkflowHandlerFn,
    history: Vec<WorkflowEvent>,
) -> (
    Result<Value, String>,
    Option<autumn_harvest::error::NonDeterministicDetails>,
) {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handler(&ctx, Value::Null),
    )
    .await
    .expect("a replayed terminal history must terminate, not suspend forever");
    let nd = ctx.take_nd_details();
    (result, nd)
}

/// Drive `handler` against a hand-built history **tolerating a suspension**.
///
/// A `for_replay` context has no driver, so any dispatch that is not already in
/// the recorded history parks forever on its oneshot. That is the expected
/// shape for a test that inspects a *partial* unwind: the `ScheduleActivity`
/// command is pushed BEFORE the await, so `ctx.drain_commands()` still observes
/// it. Returns `None` when the handler suspended.
async fn drive_replay_tolerating_suspension(
    ctx: &WorkflowContext,
    handler: autumn_harvest::WorkflowHandlerFn,
) -> Option<Result<Value, String>> {
    tokio::time::timeout(
        std::time::Duration::from_millis(250),
        handler(ctx, Value::Null),
    )
    .await
    .ok()
}

/// T13b — **post-review P1-B (blocker).** A `FailFast` mapped node aborts its
/// instance stream on the FIRST failing cell. When that cell is not the last one
/// the stream yields, the remaining instances are never polled, so their
/// recorded `ActivityScheduled` events stay unconsumed and the replay cursor is
/// left parked on one of them. Issue #780's unwind is the first thing to consume
/// history after the level loop, so it dispatched the first compensator straight
/// into that stale cursor: `match_activity` diverged, the divergence poisoned
/// `nd_details`, and the run both (a) skipped compensation entirely and (b)
/// wedged in a permanent, non-self-healing nd-block (issue #603).
///
/// The failure position is the whole point: T13's `[1, -1]` fixture fails at the
/// LAST cell, which happens to leave a clean cursor. This fixture fails in the
/// MIDDLE.
#[tokio::test]
async fn failfast_mapped_node_failing_mid_array_compensates_and_never_nd_blocks() {
    let handler = __autumn_workflow_info_mapped_failfast_comp_dag().handler;
    let outcome = WorkflowTestEnv::new()
        // THREE cells, failing in the MIDDLE — so at least one instance is left
        // unpolled by the fail-fast abort.
        .mock_activity("m_root", |_| Ok(json!([1, -1, 2])))
        .mock_activity("m_process", |item| {
            if item.as_i64().is_some_and(|n| n < 0) {
                Err("negative".to_string())
            } else {
                Ok(json!(item.as_i64().unwrap_or_default() * 10))
            }
        })
        .mock_activity("m_undo_root", |_| Ok(Value::Null))
        .mock_activity("m_undo_process", |_| Ok(Value::Null))
        .run(handler, Value::Null)
        .await;

    // (a) The compensator for the SUCCEEDED root must still dispatch.
    assert_eq!(
        scheduled_names_among(outcome.events(), &["m_undo_root", "m_undo_process"]),
        vec!["m_undo_root".to_string()],
        "a FailFast mapped node failing mid-array must still unwind the \
         succeeded prefix (the failed mapped node itself is not compensated)"
    );

    // (b) The terminal error is the ORIGINAL DAG error — a stale-cursor
    //     divergence would have been swallowed into `SagaCompensationFailed`.
    assert_eq!(
        outcome
            .result
            .as_ref()
            .expect_err("the DAG must fail")
            .as_str(),
        "one or more DAG tasks failed",
        "the unwind must succeed cleanly, leaving the original DAG error"
    );

    // (c) No non-determinism was recorded — this is the issue #603 nd-block gate.
    let (replayed, nd) = drive_replay_for_nd_details(handler, outcome.events().to_vec()).await;
    assert_eq!(
        nd, None,
        "a mid-array FailFast failure must never record a non-determinism \
         divergence (issue #603 would nd-block the run permanently)"
    );
    assert_eq!(
        replayed
            .as_ref()
            .expect_err("the replay must fail")
            .as_str(),
        "one or more DAG tasks failed"
    );

    // (d) …and the produced history replays deterministically.
    //
    // This history ends in a terminal `WorkflowFailed`, so since issue #952 the
    // verdict is `ReplaySucceeded` carrying the reproduced error rather than
    // `ReplayStatus::WorkflowFailed`. Assert on `failure_message()`, which
    // reads the reproduced error under either verdict, and pin the ND case
    // separately — that is the property this test actually guards (#603 would
    // nd-block the run permanently).
    let report = outcome.replay_check(handler).await;
    assert!(
        !matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "a mid-array FailFast history must never replay as a divergence:\n{report}"
    );
    assert_eq!(
        report.failure_message(),
        Some("one or more DAG tasks failed"),
        "a mid-array FailFast history must replay deterministically:\n{report}"
    );
}

/// T13c — the randomized sibling of T13b: sweep the failing cell across EVERY
/// position of a `FailFast` mapped node. Only the last position was covered
/// before (T13), which is exactly the one position that masked P1-B.
#[tokio::test]
async fn failfast_mapped_node_compensates_for_every_failing_cell_position() {
    const CELLS: usize = 5;

    let handler = __autumn_workflow_info_mapped_failfast_comp_dag().handler;

    for fail_pos in 0..CELLS {
        // Cell values are their index; the failing one is negated so the mock
        // rejects exactly it.
        let cells: Vec<i64> = (0..CELLS)
            .map(|i| {
                let v = i64::try_from(i).unwrap_or_default() + 1;
                if i == fail_pos { -v } else { v }
            })
            .collect();
        let cells_json = json!(cells);

        let outcome = WorkflowTestEnv::new()
            .mock_activity("m_root", move |_| Ok(cells_json.clone()))
            .mock_activity("m_process", |item| {
                if item.as_i64().is_some_and(|n| n < 0) {
                    Err("negative".to_string())
                } else {
                    Ok(json!(item.as_i64().unwrap_or_default() * 10))
                }
            })
            .mock_activity("m_undo_root", |_| Ok(Value::Null))
            .mock_activity("m_undo_process", |_| Ok(Value::Null))
            .run(handler, Value::Null)
            .await;

        let context = format!("failing cell position {fail_pos} of {CELLS}");

        assert_eq!(
            scheduled_names_among(outcome.events(), &["m_undo_root", "m_undo_process"]),
            vec!["m_undo_root".to_string()],
            "{context}: the succeeded prefix must be compensated regardless of \
             WHICH cell fails"
        );
        assert_eq!(
            outcome
                .result
                .as_ref()
                .expect_err("the DAG must fail")
                .as_str(),
            "one or more DAG tasks failed",
            "{context}: the unwind must leave the original DAG error"
        );

        let (_, nd) = drive_replay_for_nd_details(handler, outcome.events().to_vec()).await;
        assert_eq!(
            nd, None,
            "{context}: no failing-cell position may record a non-determinism \
             divergence"
        );
    }
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
/// TERMINATE with an error and must not record a non-determinism divergence
/// (which issue #603 would turn into a permanent nd-block).
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
    // Issue #603 gates the permanent nd-block on `nd_details` (read by the
    // executor's `Ok(Err(..))` arm), NOT on the deferred slot — which only the
    // infallible issue #384 primitives ever write. Asserting the deferred slot
    // would pass even while the run wedged forever.
    assert_eq!(
        ctx.take_nd_details(),
        None,
        "a cancel landing mid-unwind must never record a non-determinism \
         divergence (issue #603 would nd-block the run forever)"
    );
    // …and no further compensator may be dispatched (parity with T14).
    let scheduled: Vec<String> = ctx
        .drain_commands()
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::ScheduleActivity { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !scheduled.contains(&"c_undo_ok".to_string()),
        "the already-dispatched compensator must not be re-dispatched, got {scheduled:?}"
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
    // Post-review P2-2: the counters must carry the run's own labels.
    assert_eq!(
        recorder.labels(METRIC_SAGA_COMPENSATION_FAILED),
        vec![("failing_comp_dag".to_string(), "default".to_string())],
        "`harvest.saga.compensation_failed` must carry the (workflow, queue) labels"
    );
    // `failed <= compensated`: a failed unwind is always a counted unwind.
    assert_eq!(
        recorder.labels(METRIC_SAGA_COMPENSATED),
        vec![("failing_comp_dag".to_string(), "default".to_string())],
        "a failed unwind must also have been counted as a compensation"
    );
}

/// T16d (post-review P3-T4a) — when SEVERAL compensators fail, every one is
/// still attempted, every error is collected into the single
/// `SagaCompensationFailed`, and the page-severity counter still fires exactly
/// ONCE (it is per-unwind, not per-failed-compensation).
#[tokio::test]
async fn two_failing_compensators_collect_both_errors_and_page_once() {
    let recorder = Arc::new(CompRecordingMetrics::default());
    let outcome = WorkflowTestEnv::new()
        .with_workflow_name("failing_comp_dag")
        .with_queue_name("default")
        .with_metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>)
        .mock_activity("f_a", |_| Ok(json!("a")))
        .mock_activity("f_b", |_| Ok(json!("b")))
        .mock_activity("f_bad", |_| Err("bad".to_string()))
        .mock_activity("f_undo_b", |_| Err("undo_b exploded".to_string()))
        .mock_activity("f_undo_a", |_| Err("undo_a exploded".to_string()))
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
        err.contains("undo_b exploded") && err.contains("undo_a exploded"),
        "BOTH compensation errors must be collected, got: {err}"
    );
    // Continue-not-abort holds even when the first failure is not the last.
    assert_eq!(
        scheduled_names_among(outcome.events(), &["f_undo_a", "f_undo_b"]),
        vec!["f_undo_b".to_string(), "f_undo_a".to_string()],
        "every compensation must be attempted in LIFO order despite both failing"
    );
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATION_FAILED),
        1,
        "the page counter is per-UNWIND, not per-failed-compensation"
    );
}

// ===========================================================================
// T16b — post-review P2-2: the SUCCESSFUL unwind fires exactly one
//        `harvest.saga.compensated` with the run's (workflow, queue) labels
// ===========================================================================

/// T16b — the positive metric assertion the suite lacked: a terminal DAG
/// failure with at least one compensator must fire `harvest.saga.compensated`
/// EXACTLY once, labelled with the run's own workflow name and queue, and must
/// NOT fire the page-severity `compensation_failed` counter.
#[tokio::test]
async fn a_successful_unwind_fires_the_compensated_counter_once_with_labels() {
    let recorder = Arc::new(CompRecordingMetrics::default());
    let outcome = WorkflowTestEnv::new()
        .with_workflow_name("diamond_comp_dag")
        .with_queue_name("fulfilment")
        .with_metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>)
        .mock_activity("fan_a", |_| Ok(json!("a")))
        .mock_activity("fan_b", |_| Ok(json!("b")))
        .mock_activity("fan_c", |_| Ok(json!("c")))
        .mock_activity("fan_d", |_| Err("boom".to_string()))
        .mock_activity("comp_a", |_| Ok(Value::Null))
        .mock_activity("comp_b", |_| Ok(Value::Null))
        .mock_activity("comp_c", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_diamond_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(outcome.result.is_err(), "the DAG must fail");
    // Three compensators ran, but the counter is per-UNWIND, not per-compensation.
    assert_eq!(
        recorder.labels(METRIC_SAGA_COMPENSATED),
        vec![("diamond_comp_dag".to_string(), "fulfilment".to_string())],
        "`harvest.saga.compensated` must fire exactly once per unwind, carrying \
         the run's own (workflow, queue) labels"
    );
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATION_FAILED),
        0,
        "a clean unwind must never fire the dangling-state page"
    );
}

// ===========================================================================
// T16c — post-review P3-B2: a FAILING DAG with NO compensators is byte-clean
// ===========================================================================

#[activity]
async fn nc_ok(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("ok"))
}
#[activity]
async fn nc_bad(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("bad".to_string())
}

/// The same shape as `cancel_comp_dag`, but with NO compensator anywhere.
#[dag]
fn no_compensator_failing_dag(dag: &mut DagBuilder) {
    let ok = dag.activity(nc_ok);
    let _bad = dag.activity(nc_bad).upstream(&ok);
}

/// T16c — the FAILURE path of a DAG that declares no compensators must be
/// byte-identical to what it was before issue #780 existed: no saga marker, no
/// saga metric, no extra dispatch, and the unchanged original error.
///
/// T19 pins the same property for the SUCCESS path; this is its failure-path
/// complement, and it is the one that actually exercises `unwind_dag_compensations`
/// (which is reached, finds an empty compensation stack, and must allocate no
/// seq, record no marker, and emit no metric).
#[tokio::test]
async fn failing_dag_without_compensators_emits_no_saga_commands_or_metrics() {
    let recorder = Arc::new(CompRecordingMetrics::default());
    let outcome = WorkflowTestEnv::new()
        .with_workflow_name("no_compensator_failing_dag")
        .with_queue_name("default")
        .with_metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>)
        .mock_activity("nc_ok", |_| Ok(json!("ok")))
        .mock_activity("nc_bad", |_| Err("bad".to_string()))
        .run(
            __autumn_workflow_info_no_compensator_failing_dag().handler,
            Value::Null,
        )
        .await;

    assert_eq!(
        outcome
            .result
            .as_ref()
            .expect_err("the DAG must fail")
            .as_str(),
        "one or more DAG tasks failed",
        "an uncompensated DAG's terminal error must be untouched"
    );
    assert_eq!(
        all_scheduled_names(outcome.events()),
        vec!["nc_ok".to_string(), "nc_bad".to_string()],
        "an uncompensated failing DAG must dispatch exactly its forward nodes"
    );
    assert_eq!(
        marker_names(outcome.events()),
        Vec::<String>::new(),
        "an empty unwind must record NO marker (not even `saga_compensated:1`)"
    );
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATED), 0);
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATION_FAILED), 0);
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
// T21 — post-review P2-D: unwind order follows EXECUTION LEVELS, not the
//       builder's declaration index
// ===========================================================================

#[activity]
async fn ord_parent(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("parent"))
}
#[activity]
async fn ord_child(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("child"))
}
#[activity]
async fn ord_bad(_ctx: &ActivityContext) -> Result<Value, String> {
    Err("bad".to_string())
}
#[activity]
async fn ord_undo_parent(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn ord_undo_child(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

/// The CHILD is declared FIRST (index 0) and its parent second (index 1) — the
/// builder permits it, and it makes declaration index the exact REVERSE of
/// topological order.
#[dag]
fn out_of_order_comp_dag(dag: &mut DagBuilder) {
    let child = dag.activity(ord_child).compensate(ord_undo_child);
    let parent = dag.activity(ord_parent).compensate(ord_undo_parent);
    // Declared after both, but the edge makes `child` depend on `parent`.
    let child = child.upstream(&parent);
    let _bad = dag.activity(ord_bad).upstream(&child);
}

/// T21 — the unwind must be reverse **topological**, which is only
/// distinguishable from reverse **declaration index** when a node is declared
/// before its own parent.
///
/// Every other fixture in this suite declares nodes in topological order, so a
/// flat `for i in 0..tasks.len()` push would satisfy them all. Here the child is
/// declared first, so an index-order push would compensate the parent BEFORE the
/// child — undoing a step whose dependant is still undone.
#[tokio::test]
async fn unwind_order_is_reverse_topological_not_reverse_declaration_index() {
    let handler = __autumn_workflow_info_out_of_order_comp_dag().handler;
    let outcome = WorkflowTestEnv::new()
        .mock_activity("ord_parent", |_| Ok(json!("parent")))
        .mock_activity("ord_child", |_| Ok(json!("child")))
        .mock_activity("ord_bad", |_| Err("bad".to_string()))
        .mock_activity("ord_undo_parent", |_| Ok(Value::Null))
        .mock_activity("ord_undo_child", |_| Ok(Value::Null))
        .run(handler, Value::Null)
        .await;

    assert!(outcome.result.is_err(), "the DAG must fail");

    // Forward order is parent -> child (topological), so the LIFO unwind must be
    // child's undo FIRST. Declaration index would give the exact opposite.
    assert_eq!(
        scheduled_names_among(outcome.events(), &["ord_undo_parent", "ord_undo_child"]),
        vec!["ord_undo_child".to_string(), "ord_undo_parent".to_string()],
        "the unwind must be reverse TOPOLOGICAL order — a node declared before \
         its parent must still be compensated before it"
    );
    // Sanity: the forward pass really did run parent-then-child.
    assert_eq!(
        scheduled_names_among(outcome.events(), &["ord_parent", "ord_child"]),
        vec!["ord_parent".to_string(), "ord_child".to_string()],
    );
}

// ===========================================================================
// T22 — post-review P2-E: node-level retry (AC2 "not recovered by retry")
// ===========================================================================

#[activity]
async fn rt_flaky(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("flaky"))
}
#[activity]
async fn rt_after(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("after"))
}
#[activity]
async fn rt_undo_flaky(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}
#[activity]
async fn rt_undo_after(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

/// `rt_flaky` retries up to 3 attempts; `rt_after` follows it.
#[dag]
fn retrying_comp_dag(dag: &mut DagBuilder) {
    let flaky = dag
        .activity(rt_flaky)
        .retry(RetryPolicy::fixed(3, std::time::Duration::from_millis(1)))
        .compensate(rt_undo_flaky);
    let _after = dag
        .activity(rt_after)
        .upstream(&flaky)
        .compensate(rt_undo_after);
}

/// T22a — a node whose retry policy RECOVERS it is a success, so the DAG never
/// reaches a terminal failure and NOTHING is compensated. Compensation is the
/// last resort, not the first.
#[tokio::test]
async fn a_node_recovered_by_retry_compensates_nothing() {
    let outcome = WorkflowTestEnv::new()
        // Fails twice, succeeds on the third attempt — within the 3-attempt budget.
        .mock_activity_retries(
            "rt_flaky",
            vec![
                Err("transient".to_string()),
                Err("transient".to_string()),
                Ok(json!("flaky")),
            ],
        )
        .mock_activity("rt_after", |_| Ok(json!("after")))
        .mock_activity("rt_undo_flaky", |_| Ok(Value::Null))
        .mock_activity("rt_undo_after", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_retrying_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(
        outcome.result.is_ok(),
        "a retry-recovered node must not fail the DAG, got {:?}",
        outcome.result
    );
    assert_eq!(
        scheduled_names_among(outcome.events(), &["rt_undo_flaky", "rt_undo_after"]),
        Vec::<String>::new(),
        "a DAG recovered by retry must dispatch NO compensators"
    );
}

/// T22b — once a node EXHAUSTS its retries it is a terminal node failure: the
/// succeeded prefix is unwound and the exhausted node itself is never
/// compensated (it has no successful effect to undo).
#[tokio::test]
async fn a_node_that_exhausts_its_retries_unwinds_the_prefix_but_not_itself() {
    // The failing node is DOWNSTREAM so there is a succeeded prefix to unwind.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("rt_flaky", |_| Ok(json!("flaky")))
        .mock_activity_retries(
            "rt_after",
            vec![
                Err("transient".to_string()),
                Err("transient".to_string()),
                Err("transient".to_string()),
            ],
        )
        .mock_activity("rt_undo_flaky", |_| Ok(Value::Null))
        .mock_activity("rt_undo_after", |_| Ok(Value::Null))
        .run(
            __autumn_workflow_info_retrying_comp_dag().handler,
            Value::Null,
        )
        .await;

    assert!(
        outcome.result.is_err(),
        "an exhausted-retry node must fail the DAG"
    );
    assert_eq!(
        scheduled_names_among(outcome.events(), &["rt_undo_flaky", "rt_undo_after"]),
        vec!["rt_undo_flaky".to_string()],
        "retry exhaustion must unwind the succeeded prefix, and must never \
         compensate the exhausted node itself"
    );
}

/// T22c (post-review P3-T4c) — a compensator inherits NEITHER the compensated
/// node's `.retry(…)` NOR its `.start_to_close(…)`: those describe the FORWARD
/// step's failure budget, so the compensator activity's own `#[activity(…)]`
/// attributes apply. Pinned on the emitted command, which is where the
/// overrides would ride if they were inherited.
#[tokio::test]
async fn a_compensator_inherits_no_retry_or_timeout_override_from_its_node() {
    // `rt_flaky` declares `retry(fixed(3, 1ms))`; its compensator must not.
    let ok = ActivityExecId::new();
    let bad = ActivityExecId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: ok,
            name: "rt_flaky".into(),
            input: wrapped_dag_task_input(&Value::Null, "rt_flaky"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: ok,
            output: json!("flaky"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: bad,
            name: "rt_after".into(),
            input: wrapped_dag_task_input(&Value::Null, "rt_after"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: bad,
            error: "boom".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
    ];

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);
    let handler = __autumn_workflow_info_retrying_comp_dag().handler;
    // `rt_undo_flaky` is NOT in the recorded history, so its dispatch parks —
    // which is fine: the command this test inspects is pushed before the await.
    let _ = drive_replay_tolerating_suspension(&ctx, handler).await;

    let overrides: Vec<(Option<bool>, Option<bool>)> = ctx
        .drain_commands()
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::ScheduleActivity {
                name,
                retry_policy_override,
                start_to_close_override,
                ..
            } if name == "rt_undo_flaky" => Some((
                retry_policy_override.as_ref().map(|_| true),
                start_to_close_override.map(|_| true),
            )),
            _ => None,
        })
        .collect();

    assert_eq!(
        overrides,
        vec![(None, None)],
        "the compensator must be dispatched with NO retry / start_to_close \
         override inherited from its node"
    );
}

// ===========================================================================
// T23 — post-review P2-F: an unwind RESUMED mid-flight (crash recovery)
// ===========================================================================

/// T23 — a worker crash mid-unwind must resume from the recorded prefix: the
/// already-dispatched compensator is replayed (never re-dispatched) and only the
/// REMAINING compensations are issued, with no non-determinism.
///
/// Uses `failing_comp_dag` (T16's fixture: `f_a` -> `f_b` -> `f_bad`), whose
/// unwind is `f_undo_b` then `f_undo_a`. The hand-built history carries the
/// forward pass, the `saga_compensated:1` marker, and `f_undo_b` already
/// scheduled AND completed — i.e. exactly the state a crash right after the
/// first compensation would leave behind.
/// The recorded history of a `failing_comp_dag` run whose unwind was
/// interrupted right after its FIRST compensation (`f_undo_b`) completed.
fn interrupted_unwind_history() -> Vec<WorkflowEvent> {
    let (ia, ib, ibad, iundo_b) = (
        ActivityExecId::new(),
        ActivityExecId::new(),
        ActivityExecId::new(),
        ActivityExecId::new(),
    );
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: ia,
            name: "f_a".into(),
            input: wrapped_dag_task_input(&Value::Null, "f_a"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: ia,
            output: json!("a"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: ib,
            name: "f_b".into(),
            input: wrapped_dag_task_input(&Value::Null, "f_b"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: ib,
            output: json!("b"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: ibad,
            name: "f_bad".into(),
            input: wrapped_dag_task_input(&Value::Null, "f_bad"),
            queue: String::new(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: ibad,
            error: "bad".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
        // ── the unwind, interrupted after its FIRST compensation ────────────
        WorkflowEvent::MarkerRecorded {
            name: "saga_compensated:1".into(),
            details: json!(2),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: iundo_b,
            name: "f_undo_b".into(),
            input: json!({
                "dag_compensate": "f_b",
                "input": wrapped_dag_task_input(&Value::Null, "f_b"),
                "output": "b",
            }),
            queue: String::new(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: iundo_b,
            output: Value::Null,
        },
    ]
}

#[tokio::test]
async fn an_unwind_resumed_mid_flight_dispatches_only_the_remaining_compensations() {
    let history = interrupted_unwind_history();

    let recorder = Arc::new(CompRecordingMetrics::default());
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history)
        .with_workflow_name("failing_comp_dag")
        .with_queue_name("default")
        .with_metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>);

    let handler = __autumn_workflow_info_failing_comp_dag().handler;
    // `f_undo_b` replays from history and resolves; `f_undo_a` is NOT recorded,
    // so it dispatches and parks. Parking here is the CORRECT resume shape —
    // the remaining compensation is genuinely new durable work.
    let result = drive_replay_tolerating_suspension(&ctx, handler).await;

    if let Some(result) = result.as_ref() {
        assert_eq!(
            result
                .as_ref()
                .expect_err("the DAG must still fail")
                .as_str(),
            "one or more DAG tasks failed",
            "a cleanly-resumed unwind leaves the ORIGINAL DAG error"
        );
    }
    assert_eq!(
        ctx.take_nd_details(),
        None,
        "resuming a partially-recorded unwind must record no non-determinism"
    );

    // ONLY the remaining compensator is (re-)dispatched: `f_undo_b` is replayed
    // from history, so it emits no new command.
    let scheduled: Vec<String> = ctx
        .drain_commands()
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::ScheduleActivity { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        scheduled,
        vec!["f_undo_a".to_string()],
        "a resumed unwind must dispatch only the REMAINING compensations"
    );

    // The dedup marker is replayed, so the counter must NOT fire a second time.
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATED),
        0,
        "a resumed unwind must not re-count `harvest.saga.compensated`"
    );
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
/// zero compensator dispatches, zero saga markers, zero saga metrics.
///
/// This asserts the observable surface (dispatches / markers / metrics), which
/// is what "unchanged from a pre-#780 build" means for the success path — it is
/// not a literal byte-diff of the event log. `failing_dag_without_compensators_emits_no_saga_commands_or_metrics`
/// is the complementary FAILURE-path assertion, and it is the one that actually
/// reaches the (empty) unwind.
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
        // and the recorded history ends in a terminal `WorkflowFailed`. Since
        // issue #952 that shape replays to `ReplayStatus::ReplaySucceeded`
        // carrying the reproduced error in `reproduced_failure` — "the current
        // code failed the same way, with zero divergence", which is exactly what
        // this assertion has always meant. `failure_message()` reads that error
        // under either verdict, so the assertion states the property rather than
        // the enum variant that currently expresses it.
        let report = outcome.replay_check(handler).await;
        assert!(
            !matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
            "{context}: a compensating history must never replay as a \
             divergence:\n{report}"
        );
        assert_eq!(
            report.failure_message(),
            Some("one or more DAG tasks failed"),
            "{context}: a compensating history must replay deterministically, \
             reproducing the original DAG error:\n{report}"
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
        // `linux` is load-bearing, NOT cosmetic: an `allos` row runs with
        // `--no-default-features`, which `cfg`s this whole file away.
        osclass == "linux"
            && krate == "autumn-harvest"
            && target == "integration"
            && features.split(',').any(|f| f.trim() == "testing")
            // The filter must select THIS module and nothing narrower: a prefix
            // of the module name credits the whole module, while a
            // `module::one_test` filter (or an unrelated short filter like `dag`)
            // must not.
            && filter.starts_with("dag_compensation")
            && "dag_compensation_tests".starts_with(filter)
    });

    assert!(
        wired,
        "issue #780's suite must be wired into .github/ci/integration-suites.txt \
         with a row like:\n  \
         linux        autumn-harvest         integration                      testing                    dag_compensation_tests"
    );
}

// ===========================================================================
// Post-PR review (Codex on PR #1153) — P1: a deterministic PRE-DISPATCH
// rejection must route through the unwind, not escape past it.
// ===========================================================================

#[activity]
async fn pc_root(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(Value::Null)
}
#[activity]
async fn pc_big(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(Value::Null)
}
#[activity]
async fn pc_process(_ctx: &ActivityContext, v: Value) -> Result<Value, String> {
    Ok(v)
}
#[activity]
async fn pc_undo_root(_ctx: &ActivityContext, e: Value) -> Result<Value, String> {
    let _ = e;
    Ok(Value::Null)
}

/// `process` draws its input straight from `big`'s recorded output (issue
/// #702), so an oversized upstream output makes `process`'s input exceed the
/// configured activity-input cap — rejected by `execute_activity_raw_with_opts`
/// BEFORE it allocates an activity id or pushes a `ScheduleActivity` command.
///
/// The oversized value deliberately lives on `big`, which declares **no**
/// compensator: the compensation envelope embeds the compensated node's whole
/// input *and* output, so hanging the payload off `root` would push `root`'s own
/// envelope over the same cap and the unwind would report
/// `SagaCompensationFailed` instead of dispatching.
#[dag]
fn payload_cap_comp_dag(dag: &mut DagBuilder) {
    let root = dag.activity(pc_root).compensate(pc_undo_root);
    let big = dag.activity(pc_big).upstream(&root);
    let _process = dag.activity(pc_process).input_from(&big);
}

/// A `PayloadTooLarge` rejection is deterministic, permanent, and leaves **no**
/// history footprint and **no** side effect — exactly the class the non-array
/// mapped-input fix already routes through the unwind. Before the fix it was
/// returned as `Err` and `activity_result?` propagated it out of the level loop
/// *past* the terminal-failure check, so `root`'s side effect was left dangling.
#[tokio::test]
async fn an_oversized_activity_input_still_unwinds_the_succeeded_upstream() {
    // ~2 KiB of recorded upstream output, against a 512-byte activity-input cap.
    let oversized = Value::String("x".repeat(2048));
    let root_id = ActivityExecId::new();
    let big_id = ActivityExecId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: root_id,
            name: "pc_root".to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: root_id,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: big_id,
            name: "pc_big".to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: big_id,
            output: oversized,
        },
    ];

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history)
        .with_workflow_name("payload_cap_comp_dag")
        .with_queue_name("default")
        // activity_input = 512 bytes; every other cap left generous.
        .with_payload_caps(512, 2 * 1024 * 1024, 256 * 1024, 2 * 1024 * 1024);

    let handler = __autumn_workflow_info_payload_cap_comp_dag().handler;
    // `pc_root` and `pc_big` replay from history; `pc_process` is rejected by
    // the cap; the unwind then dispatches `pc_undo_root`, which is genuinely new
    // durable work and therefore parks on its oneshot. Parking is correct here.
    let result = drive_replay_tolerating_suspension(&ctx, handler).await;

    if let Some(result) = result.as_ref() {
        let err = result.as_ref().expect_err("the DAG must fail");
        assert!(
            err.contains("payload too large"),
            "the specific payload-cap diagnostic must stay operator-visible, got: {err}"
        );
    }
    assert_eq!(
        ctx.take_nd_details(),
        None,
        "a payload-cap rejection must not be mistaken for a replay divergence"
    );

    let scheduled: Vec<String> = ctx
        .drain_commands()
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::ScheduleActivity { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        scheduled,
        vec!["pc_undo_root".to_string()],
        "the succeeded upstream must be compensated even though the downstream \
         was rejected before dispatch"
    );
}

/// KNOWN LIMITATION (documented, not fixed): a `PayloadTooLarge` rejection is a
/// pure function of already-recorded state *plus live configuration*, and leaves
/// **no history footprint**. So if the activity-input cap is RAISED while a
/// compensating DAG is mid-unwind, replay re-evaluates the rejection against the
/// new cap, the previously-rejected node now dispatches, and its
/// `ScheduleActivity` lands where history records the compensator's — a
/// divergence.
///
/// This is the same class as any config-dependent decision that leaves no
/// history footprint. Issue #952 closed the sibling case where such a failure is
/// *uncaught* and seals the run (a trailing terminal `WorkflowFailed` is now
/// transparent to in-progress matches —
/// `replayer_tests::early_config_dependent_failure_replays_cleanly`), but a
/// rejection the unwind catches and routes around records nothing at all, so it
/// stays re-decided on every replay.
/// Issue #780 **enlarges** the surface rather than creating it: before the
/// unwind existed, a rejection `?`-escaped and sealed the run FAILED with no
/// compensation events, so there was nothing for a later dispatch to collide
/// with.
///
/// Requires a four-way conjunction — `PayloadTooLarge` + a compensating DAG + a
/// cap change + that change landing inside the unwind's decision-cycle window —
/// and the consequence is a #603 **nd-block**: the run stays `RUNNING` and
/// retries, recovering when the cap is reverted. It is never a silent partial
/// rollback, which is why this is pinned rather than papered over.
///
/// A durable fix means persisting the rejection so replay reads it back instead
/// of re-deciding it. That cannot be done locally here: the level dispatches
/// concurrently through `join_all`, so a marker pushed from inside a task future
/// has no deterministic position, and a marker pushed after the join is read too
/// late to gate the dispatch. Doing it properly means giving the *engine's*
/// activity-dispatch path a history footprint for deterministic pre-dispatch
/// rejections — an engine-wide change affecting every workflow, well outside
/// this slice.
#[tokio::test]
async fn known_limitation_raising_the_cap_mid_unwind_diverges() {
    let oversized = Value::String("x".repeat(2048));
    let root_id = ActivityExecId::new();
    let big_id = ActivityExecId::new();
    let undo_id = ActivityExecId::new();
    // The history the FIRST cycle persisted: `pc_process` was rejected before
    // dispatch (so it has no events at all) and the unwind scheduled
    // `pc_undo_root` in its place.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: root_id,
            name: "pc_root".to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: root_id,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: big_id,
            name: "pc_big".to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: big_id,
            output: oversized,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: undo_id,
            name: "pc_undo_root".to_string(),
            input: serde_json::json!({
                "dag_compensate": "pc_root",
                "input": Value::Null,
                "output": Value::Null,
            }),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: undo_id,
            output: Value::Null,
        },
    ];

    // The deploy that raised the activity-input cap from 512 bytes to 2 MiB.
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history)
        .with_workflow_name("payload_cap_comp_dag")
        .with_queue_name("default")
        .with_payload_caps(
            2 * 1024 * 1024,
            2 * 1024 * 1024,
            256 * 1024,
            2 * 1024 * 1024,
        );

    let handler = __autumn_workflow_info_payload_cap_comp_dag().handler;
    let _ = drive_replay_tolerating_suspension(&ctx, handler).await;

    assert!(
        ctx.take_nd_details().is_some(),
        "raising the cap mid-unwind must surface as a #603 non-determinism \
         block (a stuck-but-recoverable run), never as a silent divergence"
    );
}
