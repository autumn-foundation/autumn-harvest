//! Crash recovery for the embedded `SQLite` backend.
//!
//! Demonstrates that reopening the same database **resumes** an in-flight run by
//! deterministic replay rather than restarting it from scratch.
//!
//! Run it with:
//!
//! ```text
//! cargo run -p autumn-harvest-sqlite --example durability
//! ```
//!
//! The flow:
//!
//! 1. Open a **file-backed** runtime, start a workflow that reserves stock,
//!    waits for an `approve` signal, then ships.
//! 2. Drive it until it blocks on the signal — the reserve step has run and its
//!    result is committed to durable history.
//! 3. **Drop the runtime** (simulating a crash / process restart).
//! 4. **Reopen the same file**, re-register the handlers, deliver the signal,
//!    and drive to completion.
//!
//! Two in-process side-effect counters prove the outcome: the reserve body runs
//! **once** (session 1 only — session 2 replays it from history without
//! re-executing it), and the ship body runs **once** (session 2 only). The run
//! resumed exactly where it left off.

// The `#[activity]` macro expands the await-free placeholder bodies (this backend
// runs the registered closures, not the async fn bodies) into code that consumes
// the `_`-prefixed params — both are proc-macro artifacts of the placeholder
// pattern, not the hand-written example logic, so they are allowed file-wide.
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{RunState, SqliteError, SqliteRuntime};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    id: String,
    item: String,
    qty: u32,
}

#[workflow]
async fn fulfill_order(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    let reservation: String = ctx
        .execute_activity(&reserve_stock_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;

    // Block until an external `approve` signal is delivered. This is a clean,
    // durable park point: the reserve result is already in history when we stop.
    let approved: bool = ctx
        .receive_signal("approve")
        .await
        .map_err(|e| e.to_string())?;
    if !approved {
        return Err("order rejected".to_string());
    }

    let shipment: String = ctx
        .execute_activity(&ship_order_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;

    Ok(format!(
        "order {} done — {reservation}; {shipment}",
        order.id
    ))
}

#[activity]
async fn reserve_stock(_ctx: &ActivityContext, _order: Order) -> Result<String, String> {
    Ok(String::new())
}

#[activity]
async fn ship_order(_ctx: &ActivityContext, _order: Order) -> Result<String, String> {
    Ok(String::new())
}

/// Register the workflow + activity bodies into a runtime. The activity bodies
/// close over shared counters so we can observe, across the restart, exactly how
/// many times each body actually executes. (Registrations live in process
/// memory, not the database, so every reopened runtime must re-register.)
fn register(rt: &mut SqliteRuntime, reserve_runs: &Arc<AtomicUsize>, ship_runs: &Arc<AtomicUsize>) {
    rt.register_workflow(&fulfill_order_info());

    let reserve_runs = Arc::clone(reserve_runs);
    rt.register_activity(&reserve_stock_info(), move |input| {
        reserve_runs.fetch_add(1, Ordering::SeqCst);
        let order: Order = serde_json::from_value(input).map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!(
            "reserved {}x {}",
            order.qty, order.item
        )))
    });

    let ship_runs = Arc::clone(ship_runs);
    rt.register_activity(&ship_order_info(), move |input| {
        ship_runs.fetch_add(1, Ordering::SeqCst);
        let order: Order = serde_json::from_value(input).map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("shipped {}", order.id)))
    });
}

#[tokio::main]
async fn main() -> Result<(), SqliteError> {
    // A durable file the runtime can reopen after a "crash". Kept alive for the
    // whole run so the file survives the first runtime being dropped.
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("orders.db");

    let reserve_runs = Arc::new(AtomicUsize::new(0));
    let ship_runs = Arc::new(AtomicUsize::new(0));

    // ── Session 1: run until blocked on the signal, then drop (the "crash"). ──
    let exec = {
        let mut rt = SqliteRuntime::open(&db_path)?;
        register(&mut rt, &reserve_runs, &ship_runs);

        let exec = rt.start_workflow(
            "fulfill_order",
            serde_json::json!({ "id": "ORD-2002", "item": "gadget", "qty": 2 }),
        )?;

        let state = rt.run_until_blocked(exec).await?;
        match &state {
            RunState::WaitingSignal(name) => {
                println!("session 1: blocked waiting on signal `{name}`");
            }
            other => panic!("expected to block on a signal, got {other:?}"),
        }
        exec
        // `rt` is dropped here — the connection closes. All durable state remains
        // in the file. This is a faithful crash/restart boundary.
    };
    println!(
        "session 1: reserve body ran {} time(s), ship body ran {} time(s)",
        reserve_runs.load(Ordering::SeqCst),
        ship_runs.load(Ordering::SeqCst),
    );

    // ── Session 2: reopen the SAME file, re-register, deliver signal, finish. ──
    let mut rt = SqliteRuntime::open(&db_path)?;
    register(&mut rt, &reserve_runs, &ship_runs);
    println!("session 2: reopened {}", db_path.display());

    rt.send_signal(exec, "approve", serde_json::json!(true))?;
    let state = rt.run_until_blocked(exec).await?;

    match state {
        RunState::Completed(output) => println!("session 2: COMPLETED: {output}"),
        RunState::Failed(err) => println!("session 2: FAILED: {err}"),
        other => println!("session 2: unexpected state {other:?}"),
    }

    let reserve_total = reserve_runs.load(Ordering::SeqCst);
    let ship_total = ship_runs.load(Ordering::SeqCst);
    println!("total: reserve body ran {reserve_total} time(s), ship body ran {ship_total} time(s)");

    assert_eq!(
        reserve_total, 1,
        "reserve body must run exactly once — session 2 replays it from history, it is NOT re-run",
    );
    assert_eq!(
        ship_total, 1,
        "ship body must run exactly once — only in session 2, after the signal",
    );
    println!("resumed from history (not restarted): reserve replayed, ship ran fresh");

    Ok(())
}
