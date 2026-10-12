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
//! It mirrors the call sequence in the crate-level docs. Open, register the
//! workflow and its activities, and start. Then drive to quiescence with
//! `run_until_idle` and read the terminal outcome.
//!
//! The `#[activity]` fns are ordinary async bodies with an `ActivityContext`
//! (issue #2024). The same fns run unchanged on the Postgres backend.

// `#[activity]` needs an `async fn`, also for a body with no `.await`.
#![allow(clippy::unused_async)]

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

/// Reserves stock for the order. The idempotency key is stable across retries,
/// so a real inventory service can deduplicate on it.
#[activity]
async fn reserve_inventory(ctx: &ActivityContext, order: Order) -> Result<String, String> {
    let key = ctx.idempotency_key().map_err(|e| e.to_string())?;
    Ok(format!("reserved {}x {} (key {key})", order.qty, order.item))
}

/// Charges the card. `ctx.info()` reports the attempt and the owning run.
#[activity]
async fn charge_payment(ctx: &ActivityContext, order: Order) -> Result<String, String> {
    let info = ctx.info();
    Ok(format!(
        "charged {}.{:02} USD for {} (attempt {})",
        order.amount_cents / 100,
        order.amount_cents % 100,
        info.workflow_id,
        info.attempt
    ))
}

#[tokio::main]
async fn main() -> Result<(), SqliteError> {
    // An ephemeral, private database — perfect for a demo or a test. Use
    // `SqliteRuntime::open("orders.db")` for a durable, restart-safe file.
    let mut rt = SqliteRuntime::open_in_memory()?;

    // Register the workflow and its activities. The `#[activity]` bodies run.
    rt.register_workflow(&process_order_info());
    rt.register_activity(&reserve_inventory_info());
    rt.register_activity(&charge_payment_info());

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
        ExecutionOutcome::Terminated(state) => {
            println!("terminal ({state}) — superseded/cancelled, no clean output");
        }
    }

    // The full, replayable event log — the canonical source of truth.
    let history = rt.load_history(exec)?;
    println!("history has {} events", history.len());

    Ok(())
}
