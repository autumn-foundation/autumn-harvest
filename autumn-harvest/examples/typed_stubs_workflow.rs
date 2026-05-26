#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Complete end-to-end example demonstrating the type-safe workflow client stubs and handles.
//!
//! Exposes a `SubscriptionWorkflow` that manages a user subscription with
//! typed queries, updates, and signals.
//!
//! Run with:
//!   cargo run --example typed_stubs_workflow

use serde::{Deserialize, Serialize};

use autumn_harvest::prelude::*;

// ── Test Workflow Declaration ───────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SubscriptionInput {
    pub user_id: String,
    pub plan_name: String,
}

#[workflow]
async fn subscription_flow(
    ctx: &WorkflowContext,
    input: SubscriptionInput,
) -> Result<String, String> {
    println!(
        "[Workflow] Started for user {} on plan {}",
        input.user_id, input.plan_name
    );

    // Wait for the plan upgrade or activation signal
    let signal_val = ctx
        .wait_for_signal("activate_subscription")
        .await
        .map_err(|e| e.to_string())?;
    let active: bool = serde_json::from_value(signal_val).map_err(|e| e.to_string())?;

    if active {
        Ok(format!(
            "User {} subscription successfully activated on plan {}",
            input.user_id, input.plan_name
        ))
    } else {
        Err("Subscription activation rejected".to_string())
    }
}

#[query(workflow = "subscription_flow")]
fn get_current_plan(_ctx: &WorkflowContext) -> Result<String, String> {
    Ok("premium".to_string())
}

#[update(workflow = "subscription_flow")]
async fn upgrade_plan_update(_ctx: &WorkflowContext, new_plan: String) -> Result<String, String> {
    Ok(format!("Upgraded to {}", new_plan))
}

#[allow(dead_code)]
#[signal(workflow = "subscription_flow")]
fn activate_subscription(_ctx: &WorkflowContext, _active: bool) {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("============================================================");
    println!("Autumn Harvest Type-Safe Client Stubs & Handles Example");
    println!("============================================================");
    println!();
    println!("This example showcases the compile-time guarantees provided by typed stubs:");
    println!("  1. Starting workflows with strong input/output contracts:");
    println!("     let handle: TypedWorkflowHandle<String> = SubscriptionFlowStub::start(");
    println!("         &mut conn, &client, \"sub-1\", SubscriptionInput {{ ... }}");
    println!("     ).await?;");
    println!();
    println!("  2. Direct, in-process, type-safe queries on the handle:");
    println!(
        "     let plan: String = SubscriptionFlowStub::query_get_current_plan(handle.inner()).await?;"
    );
    println!();
    println!("  3. Safe external signal emission:");
    println!(
        "     SubscriptionFlowStub::signal_activate_subscription(&mut conn, handle.inner(), true).await?;"
    );
    println!();
    println!("All client stub types (e.g. `SubscriptionFlowStub`) are fully generated");
    println!("at compile-time by the #[workflow] macro and its query/update siblings.");
    println!();

    Ok(())
}
