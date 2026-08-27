#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Example: Transactional workflow start (issue #763).
//!
//! `ctx.run_transactional` (see `docs/transactional-activities.md`) solves the
//! dual-write problem at the activity -> completion boundary: a domain write
//! and the `ActivityCompleted` event commit atomically.  Starting a workflow
//! from *outside* harvest — e.g. an HTTP handler that inserts an `orders` row
//! and then needs to kick off a `fulfill_order` workflow — has the identical
//! problem one level up: the domain insert and the workflow's `WorkflowStarted`
//! event are two separate commits unless something ties them together.
//!
//! `WorkflowHandleClient::start_workflow_transactional` is that tie.  It
//! accepts a caller-owned Diesel connection (or an already-open transaction on
//! one) and stages the workflow start on it.  Nothing about the run — no
//! `WorkflowStarted` event, no execution row, no dispatchable task-queue row —
//! becomes visible to any worker until the caller's own transaction commits.
//! Roll the transaction back and none of it ever existed.
//!
//! `place_order_after` and `place_order_rejected` below are real functions
//! that type-check against the actual `start_workflow_transactional` API —
//! `cargo build --example transactional_start_order --features db` (or the
//! `cargo run` below) fails to compile if this example drifts from the real
//! signature. `main()` itself is illustrative-only: it prints usage guidance
//! and never opens a live Postgres connection, so it does NOT execute
//! `place_order_after`/`place_order_rejected` against a database. The actual
//! runtime proof — including the 500-iteration randomized fault-injection
//! sweep and the dedicated ordering/rollback/idempotency/gate tests — lives
//! entirely in `tests/integration/transactional_start_tests.rs`; see
//! `docs/transactional-start.md`'s "Testing" section.
//!
//! Run with:
//!   cargo run --example transactional_start_order --features db

use autumn_harvest::error::database_error;
use autumn_harvest::prelude::*;
use autumn_harvest::{ExecutionId, HarvestError, TransactionalStartOptions, WorkflowHandleClient};
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// The workflow the order-placement handler below starts.  Registration
/// (`.with_workflows([fulfill_order_info()])` on the `WorkflowHandleClient`)
/// is what lets `start_workflow_transactional` resolve this workflow's
/// `WorkflowInfo` defaults — execution timeout, SLA, concurrency key,
/// published input schema — the same way `POST /workflows/{name}/start` does.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FulfillOrderInput {
    order_id: String,
    amount_cents: i64,
}

#[workflow]
async fn fulfill_order(ctx: &WorkflowContext, input: FulfillOrderInput) -> Result<(), String> {
    // ... charge the card, reserve inventory, ship, notify ...
    let _ = ctx;
    let _ = input;
    Ok(())
}

/// HOW THIS USED TO LOOK — a dual write across two independent commits, with
/// no way to guarantee both happen or neither does.
///
/// ```ignore
/// async fn place_order_before(pool: &Pool, order_id: &str, amount_cents: i64) -> Result<()> {
///     let mut conn = pool.get().await?;
///     // 1. Insert the domain row and commit it.
///     insert_order_row(&mut conn, order_id, amount_cents).await?;
///     // 2. Separately start the workflow. If the process crashes here, or this
///     //    call itself fails, the order row exists with NO fulfillment ever
///     //    kicked off — a silent, unrecoverable gap discovered only by an
///     //    operator (or a customer) much later. Closing this gap by hand means
///     //    an outbox table plus a reconciliation job that polls it.
///     start_workflow(&mut conn, "fulfill_order", order_id, json!({ "order_id": order_id })).await?;
///     Ok(())
/// }
/// ```
///
/// HOW IT LOOKS NOW — one Diesel transaction, one atomic unit. Either the
/// order row AND the workflow start both land, or neither does.
async fn place_order_after(
    conn: &mut AsyncPgConnection,
    client: &WorkflowHandleClient,
    order_id: String,
    amount_cents: i64,
) -> Result<ExecutionId, HarvestError> {
    let outcome = Box::pin(
        conn.transaction::<autumn_harvest::TransactionalStartOutcome, HarvestError, _>({
            let order_id = order_id.clone();
            async move |conn| {
                // 1. The caller's own domain write, on the SAME connection.
                diesel::sql_query(
                    "INSERT INTO orders (id, amount_cents, status) VALUES ($1, $2, 'PLACED')",
                )
                .bind::<Text, _>(&order_id)
                .bind::<BigInt, _>(amount_cents)
                .execute(conn)
                .await
                .map_err(database_error)?;

                // 2. Stage the workflow start on the SAME transaction. Nothing
                //    about this run is visible to any worker yet — not until
                //    the `conn.transaction(...)` block above returns `Ok` and
                //    its caller (the surrounding code) commits.
                client
                    .start_workflow_transactional(
                        conn,
                        "fulfill_order",
                        &order_id,
                        json!({ "order_id": order_id, "amount_cents": amount_cents }),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }),
    )
    .await?;

    // Past this point the transaction has committed: the `orders` row and the
    // `WorkflowStarted` event both landed together, and a worker can now claim
    // the run's first task. `finish()` is an optional latency optimization
    // (eagerly dispatches any deferred completion-trigger follow-ups) — never
    // a correctness requirement, since a periodic scanner delivers the same
    // follow-ups if it's skipped.
    let exec_id = outcome.exec_id;
    outcome.finish().await;
    Ok(exec_id)
}

/// A validation failure AFTER the domain insert rolls back BOTH writes — the
/// workflow start never happened, and the `orders` row it would have referenced
/// doesn't exist either. There is no half-written state to clean up.
async fn place_order_rejected(
    conn: &mut AsyncPgConnection,
    client: &WorkflowHandleClient,
    order_id: String,
) -> Result<(), HarvestError> {
    let result = Box::pin(conn.transaction::<(), HarvestError, _>({
        let order_id = order_id.clone();
        async move |conn| {
            diesel::sql_query(
                "INSERT INTO orders (id, amount_cents, status) VALUES ($1, $2, 'PLACED')",
            )
            .bind::<Text, _>(&order_id)
            .bind::<BigInt, _>(-1_i64) // a negative charge — deliberately invalid
            .execute(conn)
            .await
            .map_err(database_error)?;

            // A negative amount fails `fulfill_order`'s published input
            // schema (see `examples/schema_workflow.rs` for how to publish
            // one). `start_workflow_transactional` validates BEFORE writing
            // anything, so this `Err` propagates straight out of the
            // transaction closure — rolling back the `INSERT` above too.
            // (This branch never actually reaches `Ok`, so the returned
            // `#[must_use]` outcome is intentionally discarded here.)
            let _outcome = client
                .start_workflow_transactional(
                    conn,
                    "fulfill_order",
                    &order_id,
                    json!({ "order_id": order_id, "amount_cents": -1 }),
                    TransactionalStartOptions::new(),
                )
                .await?;
            Ok(())
        }
    }))
    .await;

    // The transaction rolled back: no `orders` row, no workflow execution.
    debug_assert!(result.is_err());
    let _ = result;
    Ok(())
}

fn main() {
    println!("transactional_start_order example — see source for the atomic-start usage");
    println!();
    println!("Wire up a client (single-shard shown; see docs for sharded deployments):");
    println!();
    println!("  let client = WorkflowHandleClient::single(pool, notification_url)");
    println!("      .with_workflows([fulfill_order_info()]);");
    println!();
    println!("Then, inside your own `conn.transaction(...)` block:");
    println!();
    println!("  let outcome = client");
    println!(
        "      .start_workflow_transactional(conn, \"fulfill_order\", &order_id, input, opts)"
    );
    println!("      .await?;");
    println!();
    println!(
        "See docs/transactional-start.md and tests/integration/transactional_start_tests.rs \
         for the full atomicity contract, including a 500-iteration randomized crash-point \
         fault-injection sweep proving zero orphans in either direction."
    );

    let _ = place_order_after;
    let _ = place_order_rejected;
}
