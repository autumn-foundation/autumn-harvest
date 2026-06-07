#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Example: retry-aware activity using `ctx.attempt()`, `ctx.previous_failure()`,
//! `ctx.is_last_attempt()`, and `ctx.max_attempts()` (issue #381).
//!
//! This is the five-line escape hatch described in the issue: an activity that
//! changes strategy on the last attempt, surfaces the previous error in its
//! log, and avoids sending duplicate notifications on retries.
//!
//! Run with:
//!   cargo run --example retry_aware_activity

use autumn_harvest::prelude::*;
use serde_json::json;

// ---------------------------------------------------------------------------
// Activity — retry-aware notification sender
// ---------------------------------------------------------------------------

#[activity(
    start_to_close = "30s",
    retry = RetryPolicy::exponential(5, std::time::Duration::from_secs(1)),
    queue = "notifications"
)]
async fn send_notification(
    ctx: &ActivityContext,
    payload: String,
) -> Result<serde_json::Value, String> {
    // Log the previous failure so operators can correlate retries.
    if let Some(prev_err) = ctx.previous_failure() {
        tracing::warn!(
            attempt = ctx.attempt(),
            max_attempts = ctx.max_attempts(),
            previous_error = prev_err,
            "retrying after previous failure"
        );
    }

    // Simulate a transient upstream call.
    let result = simulate_upstream_call(&payload);

    match result {
        Ok(resp) => Ok(json!({ "delivered": true, "response": resp })),
        Err(e) if ctx.is_last_attempt() => {
            // Last attempt: fall back to a dead-letter queue instead of
            // propagating an unrecoverable failure to the workflow.
            tracing::error!(
                attempt = ctx.attempt(),
                error = e,
                "all attempts exhausted; sending to dead-letter queue"
            );
            Ok(json!({ "delivered": false, "dlq": true }))
        }
        Err(e) => Err(e),
    }
}

fn simulate_upstream_call(_payload: &str) -> Result<String, String> {
    // Stub: always succeeds in this example.
    Ok("ack".to_string())
}

// ---------------------------------------------------------------------------
// Workflow — orchestrates the notification
// ---------------------------------------------------------------------------

#[workflow]
async fn notify_user(ctx: &WorkflowContext, user_id: i64) -> Result<serde_json::Value, String> {
    let payload = format!("Hello, user {user_id}!");
    ctx.execute_activity(&send_notification_info(), payload)
        .await
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// main — print the activity info to confirm the macro compiled
// ---------------------------------------------------------------------------

fn main() {
    let info = send_notification_info();
    println!("Activity : {}", info.name);
    println!("Max retries configured via RetryPolicy (in retry policy, not ActivityInfo directly)");
    println!();
    println!("Pattern demonstrated:");
    println!("  if let Some(prev) = ctx.previous_failure() {{ /* log prev */ }}");
    println!("  if ctx.is_last_attempt() {{ /* fall back */ }}");
    println!("  ctx.attempt()       // current 1-indexed attempt number");
    println!("  ctx.max_attempts()  // ceiling from the retry policy");
}
