//! Quickstart for the embedded, single-writer `SQLite` backend.
//!
//! A complete, runnable order-processing workflow: two activities (reserve
//! inventory, then charge payment) orchestrated by one `#[workflow]`, run to
//! completion on an in-memory database with no server, no Docker, and no
//! Postgres.
//!
//! Run it with:
//!
//! ```text
//! cargo run -p autumn-harvest-sqlite --example quickstart
//! ```
//!
//! It mirrors the call sequence in the crate-level docs: open → register the
//! workflow and its activity bodies → start → drive to quiescence with
//! `run_until_idle` → read the terminal outcome.

// The `#[activity]` macro expands the await-free placeholder bodies (this backend
// runs the registered closures, not the async fn bodies) into code that consumes
// the `_`-prefixed params — both are proc-macro artifacts of the placeholder
// pattern, not the hand-written example logic, so they are allowed file-wide.
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ExecutionOutcome, SqliteError, SqliteRuntime};
use serde::{Deserialize, Serialize};

/// The workflow's input — a single order to process.
#[derive(Clone, Serialize, Deserialize)]
struct Order {
    id: String,
    item: String,
    qty: u32,
    amount_cents: u64,
}

/// Orchestrates an order: reserve stock, then charge the card. Each step is a
/// durable activity whose result is recorded in history, so a restart replays
/// completed steps instead of re-running them.
#[workflow]
async fn process_order(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    let reservation: String = ctx
        .execute_activity(&reserve_inventory_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;

    let receipt: String = ctx
        .execute_activity(&charge_payment_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;

    Ok(format!(
        "order {} confirmed — {reservation}; {receipt}",
        order.id
    ))
}

// The `#[activity]` async fn supplies the macro-generated `*_info()` companion
// (its name + any declared `#[activity(...)]` defaults). This backend runs a
// caller-supplied SYNCHRONOUS body registered against that info — the async body
// below is only a placeholder for the shared `ActivityInfo`.
#[activity]
async fn reserve_inventory(_ctx: &ActivityContext, _order: Order) -> Result<String, String> {
    Ok(String::new())
}

#[activity]
async fn charge_payment(_ctx: &ActivityContext, _order: Order) -> Result<String, String> {
    Ok(String::new())
}

#[tokio::main]
async fn main() -> Result<(), SqliteError> {
    // An ephemeral, private database — perfect for a demo or a test. Use
    // `SqliteRuntime::open("orders.db")` for a durable, restart-safe file.
    let mut rt = SqliteRuntime::open_in_memory()?;

    // Register the workflow handler and each activity's synchronous body.
    rt.register_workflow(&process_order_info());

    rt.register_activity(&reserve_inventory_info(), |input| {
        let order: Order = serde_json::from_value(input).map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!(
            "reserved {}x {}",
            order.qty, order.item
        )))
    });

    rt.register_activity(&charge_payment_info(), |input| {
        let order: Order = serde_json::from_value(input).map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!(
            "charged {}.{:02} USD",
            order.amount_cents / 100,
            order.amount_cents % 100
        )))
    });

    // Start a run. `start_workflow` returns the ExecutionId immediately; nothing
    // has executed yet.
    let exec = rt.start_workflow(
        "process_order",
        serde_json::json!({
            "id": "ORD-1001",
            "item": "widget",
            "qty": 3,
            "amount_cents": 4599,
        }),
    )?;
    println!("started execution {exec}");

    // Drive the polling loop until every run is terminal or blocked. With no
    // signals or timers, this run reaches completion in a couple of cycles.
    rt.run_until_idle().await?;

    // Read the stored terminal outcome (a pure read; does not advance the run).
    match rt.outcome(exec)? {
        ExecutionOutcome::Completed(output) => {
            println!("COMPLETED: {output}");
        }
        ExecutionOutcome::Failed(err) => {
            println!("FAILED: {err}");
        }
        ExecutionOutcome::Running => {
            println!("still running (unexpected for this example)");
        }
    }

    // The full, replayable event log — the canonical source of truth.
    let history = rt.load_history(exec)?;
    println!("history has {} events", history.len());

    Ok(())
}
