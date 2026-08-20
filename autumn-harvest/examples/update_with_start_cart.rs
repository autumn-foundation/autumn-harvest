#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Example: Atomic start-or-attach + update for entity workflows (issue #479).
//!
//! Demonstrates how a client collapses the racy *fetch-then-start-then-update*
//! trio into a single `update_with_start` call.  The update is admitted in the
//! same Postgres transaction as (or atomically against) the workflow start, so
//! there is no window where the workflow is alive but the update is lost.
//!
//! The canonical use-case is a cart service: the first `add_item` call creates
//! the cart workflow and admits the update atomically.  Subsequent calls attach
//! to the running workflow and admit their own updates without any client-side
//! coordination logic.
//!
//! Run with:
//!   cargo run --example update_with_start_cart

use autumn_harvest::prelude::*;
use autumn_harvest::{
    ExecutionId, ShardRouter, UpdateWithStartParams, WorkflowIdReusePolicy,
    update_with_start_workflow_execution,
};
use serde_json::json;

// ── Workflow & update types ───────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CartItem {
    item_id: String,
    qty: u32,
    price_cents: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CartState {
    items: Vec<CartItem>,
    total_cents: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct AddItemRequest {
    item_id: String,
    qty: u32,
    price_cents: u64,
}

/// The cart workflow — long-lived entity workflow that accumulates items until
/// the customer checks out or the cart expires.
#[workflow]
async fn cart(ctx: &WorkflowContext, tenant_id: String) -> Result<CartState, String> {
    let _ = tenant_id;

    // Register the add_item update handler.
    // Concurrent callers use `update_with_start` to ensure the workflow exists
    // before attempting to add items.
    let state: std::sync::Arc<std::sync::Mutex<CartState>> =
        std::sync::Arc::new(std::sync::Mutex::new(CartState {
            items: vec![],
            total_cents: 0,
        }));

    let state_for_handler = state.clone();
    ctx.register_update_handler_no_validator("add_item", move |input: serde_json::Value| {
        let state = state_for_handler.clone();
        async move {
            let req: AddItemRequest = serde_json::from_value(input).map_err(|e| e.to_string())?;
            let mut s = state.lock().unwrap();
            let new_total = s.total_cents + req.price_cents * req.qty as u64;
            s.items.push(CartItem {
                item_id: req.item_id.clone(),
                qty: req.qty,
                price_cents: req.price_cents,
            });
            s.total_cents = new_total;
            Ok(serde_json::json!(new_total))
        }
    });

    // Wait for the checkout signal or a 2-hour cart-expiry timer.
    let checkout = ctx
        .receive_signal_timeout::<()>("checkout", std::time::Duration::from_secs(2 * 60 * 60))
        .await
        .map_err(|e| e.to_string())?;

    if checkout.is_none() {
        // Cart expired.
        return Err("cart_expired".to_string());
    }

    let final_state = state.lock().unwrap().clone();
    Ok(final_state)
}

// ── HOW `add_item` USED TO LOOK — three calls, racy across replicas ──────────
//
// ```ignore
// async fn add_item_before(pool: &Pool, cart_id: &str, item: AddItemRequest) -> Result<u64> {
//     let mut conn = pool.get().await?;
//     // 1. fetch — does the cart workflow already exist?
//     let existing = load_workflow_by_key(&mut conn, "cart", cart_id).await?;
//     if existing.is_none() {
//         // 2a. start it …
//         start_or_load_workflow_execution(&mut conn, /* params */).await?;
//     }
//     // 2b. admit the update on whichever execution we found or created —
//     //     but there is a race window: a concurrent request may have started
//     //     a *different* execution between our fetch and our admit call.
//     let exec_id = load_active_exec_id(&mut conn, "cart", cart_id).await?;
//     let new_total = admit_and_execute_update(&mut conn, exec_id, "add_item", item).await?;
//     Ok(new_total)
// }
// ```

// ── HOW `add_item` LOOKS NOW — one atomic call, race-free ────────────────────

async fn add_item_atomic(
    conn: &mut diesel_async::AsyncPgConnection,
    cart_id: String,
    tenant_id: String,
    item: AddItemRequest,
    idempotency_key: Option<String>,
    router: &ShardRouter,
) -> Result<autumn_harvest::UpdateWithStartOutcome, autumn_harvest::HarvestError> {
    // Pick the shard that will own this cart (stable across retries).
    let shard = router.pick_for_new_workflow("cart", &cart_id);
    let exec_id = ExecutionId::new_for_shard(shard);
    let update_id = autumn_harvest::UpdateId::new();

    let outcome = update_with_start_workflow_execution(
        conn,
        UpdateWithStartParams {
            workflow_name: "cart",
            workflow_id: &cart_id,
            exec_id,
            input: json!(tenant_id),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            // On a concurrent second request: attach to the already-running
            // cart workflow and admit the new item update there.
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            // Chain-scoped lifetime cap (issue #617): SWS/UWS test fixture.
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            update_id,
            update_name: "add_item".to_string(),
            update_args: serde_json::to_value(&item).unwrap(),
            // Caller-supplied idempotency key so retried HTTP requests don't
            // admit the same item twice.
            idempotency_key,
            max_workflow_input_bytes: 0,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            workflow_retry_policy: None,
            max_workflow_attempts_ceiling: None,
            reject_fresh_if_debounced: false,
        },
    )
    .await?;

    Ok(outcome)
}

fn main() {
    println!("update_with_start_cart example — see source for the full pattern");
    println!();
    println!("# Reuse-policy × prior-state outcome matrix");
    println!();
    println!(
        "  Prior state      | AllowDuplicate          | RejectDuplicate    | AllowDuplicateFailedOnly | TerminateIfRunning       "
    );
    println!(
        "  -----------------+-------------------------+--------------------+--------------------------+--------------------------"
    );
    println!(
        "  none             | start + admit update    | start + admit      | start + admit            | start + admit            "
    );
    println!(
        "  RUNNING          | admit to existing       | Err(AlreadyExists) | admit to existing        | cancel + start + admit   "
    );
    println!(
        "  SUSPENDED        | admit to existing       | Err(AlreadyExists) | admit to existing        | cancel + start + admit   "
    );
    println!(
        "  PAUSED           | Err(WorkflowPaused)     | Err(AlreadyExists) | Err(WorkflowPaused)      | Err(WorkflowPaused)      "
    );
    println!(
        "  COMPLETED        | start fresh + admit     | Err(AlreadyExists) | start fresh + admit      | start fresh + admit      "
    );
    println!(
        "  FAILED           | start fresh + admit     | Err(AlreadyExists) | start fresh + admit      | start fresh + admit      "
    );
    println!(
        "  CANCELLED        | start fresh + admit     | Err(AlreadyExists) | start fresh + admit      | start fresh + admit      "
    );
    println!(
        "  TERMINATED       | start fresh + admit     | start fresh+admit  | start fresh + admit      | start fresh + admit      "
    );
    println!();
    println!("# Equivalent management API call:");
    println!();
    println!(
        r#"  curl -X POST http://localhost:8080/api/harvest/workflows/cart/update-with-start \"#
    );
    println!(r#"       -H 'Content-Type: application/json' \"#);
    println!(r#"       -d '{{"#);
    println!(r#"             "workflow_id": "cart-acme-session-42","#);
    println!(r#"             "start_input": "acme","#);
    println!(r#"             "update_name": "add_item","#);
    println!(
        r#"             "update_args": {{"item_id": "sku-001", "qty": 2, "price_cents": 1999}},"#
    );
    println!(r#"             "idempotency_key": "req-abc123","#);
    println!(r#"             "id_reuse_policy": "allow_duplicate""#);
    println!(r#"           }}'"#);
    println!();
    println!("Response (201 on fresh start, 200 on attach):");
    println!(r#"  {{"#);
    println!(r#"    "execution_id": "01900000-0000-7000-...","#);
    println!(r#"    "workflow_name": "cart","#);
    println!(r#"    "workflow_id": "cart-acme-session-42","#);
    println!(r#"    "state": "RUNNING","#);
    println!(r#"    "started_fresh": true,"#);
    println!(r#"    "update_id": "01900000-0000-7001-...","#);
    println!(r#"    "update_admitted": true"#);
    println!(r#"  }}"#);

    let _ = add_item_atomic;
}
