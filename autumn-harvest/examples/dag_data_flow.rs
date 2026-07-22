#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Declarative DAG node input binding — issue #702.
//!
//! ## The problem
//!
//! In a `#[dag]` pipeline every node's activity gets fed the DAG's *trigger
//! input* (wrapped in a `{ "conf": …, "dag_task": "<name>" }` envelope), not the
//! output of the node it depends on. So a classic `extract → transform → load`
//! pipeline — where each stage consumes the prior stage's output — forced
//! authors to either flatten the pipeline into one mega-activity or hand-thread
//! outputs through shared state. Neither is the graph you wanted to draw.
//!
//! ## The solution: `.input_from(…)`
//!
//! Bind a node's activity input **directly** to one or more upstream node
//! outputs, with one builder call per data edge and zero hand-written
//! output-threading:
//!
//! ```text
//!   extract ──▶ transform ──▶ load
//!   (rows)      (.input_from   (.input_from
//!                &extract)      &transform)
//! ```
//!
//! | Method                              | The bound node's activity receives …                          |
//! |-------------------------------------|---------------------------------------------------------------|
//! | `.input_from(&up)`                  | `up`'s recorded output, **verbatim** (no `conf`/`dag_task` wrap) |
//! | `.input_from_all(&[&a, &b])`        | a JSON object keyed by each upstream's **activity name**       |
//! | `.input_from_aliased(&[("k", &a)])` | a JSON object keyed by the **given alias**                     |
//!
//! Binding **implies the dependency edge** — no separate `.upstream(&up)` is
//! required (though an extra one is harmless). A binding to a node that was
//! skipped or failed yields a deterministic `Value::Null` for that source, so a
//! bound node can still run (give it an `AllDone` trigger rule) and branch on
//! null-vs-payload.
//!
//! ## Determinism
//!
//! A bound node's input derives **only** from already-recorded upstream outputs
//! (frozen in `harvest_events` before the bound node is dispatched), so replay
//! reproduces byte-identical inputs on every worker and every pass. Never derive
//! a node's input from process state, the system clock, or random values — bind
//! to an upstream output instead.
//!
//! ## Invariants
//!
//! - **No new `WorkflowEvent` variant, no migration.** Input binding is computed
//!   in the workflow body from already-recorded `ActivityCompleted` outputs; an
//!   unbound DAG is byte-identical to today (regression-guarded).
//! - **Unified-DAG only.** Bindings take effect on the unified workflow-execution
//!   path. Build with `--features unified-dag-execution`.
//!
//! Run with:
//!   cargo run --example dag_data_flow --features unified-dag-execution

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

// DAG activities take only `&ActivityContext` plus an optional typed input; the
// unified walker feeds each node its shaped input internally. A node with an
// `.input_from(…)` binding receives the raw upstream output(s) in that slot.

/// **Extract** — the root of the ETL. Produces the rows the pipeline operates on.
#[activity(start_to_close = "30s")]
async fn extract_rows(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!([
        { "id": 1, "amount_cents": 1_000 },
        { "id": 2, "amount_cents": 2_500 },
        { "id": 3, "amount_cents": 4_200 },
    ]))
}

/// **Transform** — consumes the `extract` output *verbatim* via `.input_from`.
/// `input` here is exactly the array `extract_rows` returned — no `conf` /
/// `dag_task` wrapper to unpack.
#[activity(start_to_close = "30s")]
async fn transform_rows(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let rows = input
        .as_array()
        .ok_or("transform expected an array of rows")?;
    let total: i64 = rows
        .iter()
        .map(|r| r["amount_cents"].as_i64().unwrap_or(0))
        .sum();
    Ok(json!({ "record_count": rows.len(), "total_cents": total }))
}

/// **Load** — consumes the `transform` output *verbatim* via `.input_from`.
/// `input` is exactly the `{ record_count, total_cents }` object above.
#[activity(start_to_close = "30s")]
async fn load_summary(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let count = input["record_count"].as_u64().unwrap_or(0);
    let total = input["total_cents"].as_i64().unwrap_or(0);
    Ok(json!(format!(
        "loaded {count} records totalling {total} cents"
    )))
}

/// **Primary teaching shape (AC2/AC3): `extract → transform → load`.**
///
/// Each stage consumes the prior stage's output with a single `.input_from(…)`
/// builder call and **zero** hand-written output threading. The dependency
/// edges are added by the bindings themselves.
#[dag]
fn etl_pipeline(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_rows);
    let transform = dag.activity(transform_rows).input_from(&extract);
    let _load = dag.activity(load_summary).input_from(&transform);
}

/// **Fan-in shape (AC1 multi): merge two upstream outputs into one keyed object.**
///
/// `.input_from_aliased(…)` delivers `sink`'s activity a JSON object of both
/// source outputs, keyed by the given aliases. Use `.input_from_all(&[&a, &b])`
/// instead to key by each upstream's activity name.
#[activity(start_to_close = "30s")]
async fn fetch_users(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "count": 3 }))
}

#[activity(start_to_close = "30s")]
async fn fetch_orders(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "count": 7 }))
}

#[activity(start_to_close = "30s")]
async fn join_report(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let users = input["users"]["count"].as_u64().unwrap_or(0);
    let orders = input["orders"]["count"].as_u64().unwrap_or(0);
    Ok(json!({ "users": users, "orders": orders }))
}

/// `fetch_users` and `fetch_orders` run in parallel; `join_report` receives a
/// merged `{ "users": …, "orders": … }` object of both outputs.
#[dag]
fn fan_in_pipeline(dag: &mut DagBuilder) {
    let users = dag.activity(fetch_users);
    let orders = dag.activity(fetch_orders);
    let _report = dag
        .activity(join_report)
        .input_from_aliased(&[("users", &users), ("orders", &orders)]);
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _dags = dags![etl_pipeline, fan_in_pipeline];
    let _acts = activities![
        extract_rows,
        transform_rows,
        load_summary,
        fetch_users,
        fetch_orders,
        join_report
    ];
    println!("dag_data_flow example compiled successfully");
    println!();
    println!("Each ETL stage binds its input to the prior stage's output:");
    println!("  extract ──▶ transform (.input_from &extract) ──▶ load (.input_from &transform)");
    println!("No hand-written output threading — the binding IS the data edge.");
}

// Gated on `testing` + `unified-dag-execution`: the example binary builds under
// `--features unified-dag-execution` alone, and the embedded replay/harness
// tests appear when `testing` is also enabled. Run with:
//   cargo test -p autumn-harvest --features testing,unified-dag-execution \
//     --example dag_data_flow
#[cfg(all(test, feature = "testing", feature = "unified-dag-execution"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};

    /// The recorded input of the first `ActivityScheduled` event for `activity`.
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

    /// AC2/AC3 (headline): each ETL stage receives EXACTLY the prior stage's
    /// output — `transform` gets the raw `extract` array, `load` gets the raw
    /// `transform` object — with no `conf`/`dag_task` wrapper anywhere.
    #[tokio::test]
    async fn each_stage_receives_the_prior_stage_output() {
        let extract_out = json!([
            { "id": 1, "amount_cents": 1_000 },
            { "id": 2, "amount_cents": 2_500 },
            { "id": 3, "amount_cents": 4_200 },
        ]);
        let transform_out = json!({ "record_count": 3, "total_cents": 7_700 });

        let expect_extract = extract_out.clone();

        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_rows", move |_| Ok(expect_extract.clone()))
            .mock_activity("transform_rows", {
                let extract_out = extract_out.clone();
                move |input| {
                    assert_eq!(
                        input, extract_out,
                        "transform must receive the RAW extract output, not a wrapped envelope"
                    );
                    Ok(json!({ "record_count": 3, "total_cents": 7_700 }))
                }
            })
            .mock_activity("load_summary", {
                let transform_out = transform_out.clone();
                move |input| {
                    assert_eq!(
                        input, transform_out,
                        "load must receive the RAW transform output, not a wrapped envelope"
                    );
                    Ok(json!("loaded 3 records totalling 7700 cents"))
                }
            })
            .run(__autumn_workflow_info_etl_pipeline().handler, Value::Null)
            .await;

        assert!(
            outcome.result.is_ok(),
            "the bound ETL DAG must run to success, got: {:?}",
            outcome.result
        );

        // Authoritative check on recorded history (independent of the closures).
        let events = outcome.events();
        assert_eq!(
            scheduled_input(events, "transform_rows"),
            extract_out,
            "recorded transform input must be the raw extract output"
        );
        assert_eq!(
            scheduled_input(events, "load_summary"),
            transform_out,
            "recorded load input must be the raw transform output"
        );
    }

    /// AC1 (multi): the fan-in node receives a keyed merge of both upstream
    /// outputs, keyed by the aliases given to `.input_from_aliased`.
    #[tokio::test]
    async fn fan_in_delivers_a_keyed_merge() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("fetch_users", |_| Ok(json!({ "count": 3 })))
            .mock_activity("fetch_orders", |_| Ok(json!({ "count": 7 })))
            .mock_activity("join_report", |input| {
                assert_eq!(
                    input,
                    json!({ "users": { "count": 3 }, "orders": { "count": 7 } }),
                    "the fan-in node must receive a keyed merge of both upstream outputs"
                );
                Ok(json!({ "users": 3, "orders": 7 }))
            })
            .run(
                __autumn_workflow_info_fan_in_pipeline().handler,
                Value::Null,
            )
            .await;

        assert!(
            outcome.result.is_ok(),
            "the fan-in DAG must run to success, got: {:?}",
            outcome.result
        );
        assert_eq!(
            scheduled_input(outcome.events(), "join_report"),
            json!({ "users": { "count": 3 }, "orders": { "count": 7 } }),
            "recorded fan-in input must be the merged keyed object"
        );
    }

    /// AC5 (self-check): a recorded ETL history — whose bound stages carry raw
    /// upstream outputs as their inputs — replays deterministically.
    #[tokio::test]
    async fn bound_etl_history_replays_deterministically() {
        // Run once to capture a real history, then replay it.
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_rows", |_| {
                Ok(json!([{ "id": 1, "amount_cents": 1_000 }]))
            })
            .mock_activity("transform_rows", |_| {
                Ok(json!({ "record_count": 1, "total_cents": 1_000 }))
            })
            .mock_activity("load_summary", |_| Ok(json!("loaded")))
            .run(__autumn_workflow_info_etl_pipeline().handler, Value::Null)
            .await;

        assert!(
            outcome.result.is_ok(),
            "ETL run must succeed to capture a history"
        );
        let events = outcome.events().to_vec();

        let report = WorkflowReplayer::new()
            .register_fn(
                "etl_pipeline",
                __autumn_workflow_info_etl_pipeline().handler,
            )
            .replay_from_events(events)
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "AC5: a bound ETL history must replay deterministically, got: {report}"
        );
    }
}
