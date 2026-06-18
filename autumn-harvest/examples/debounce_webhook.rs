#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Debounced webhook workflow — issue #499.
//!
//! Demonstrates the debounce primitive for collapsing webhook delivery bursts.
//!
//! ## The problem
//!
//! Webhook providers often retry failed deliveries aggressively (5–10×), and
//! fan-out systems can emit dozens of row-change notifications for a single
//! logical event. Without debouncing, each trigger starts a redundant execution.
//! Hand-rolling a timer + buffer + dedupe scheme costs ~40–60 LOC and is fragile.
//!
//! ## The solution: `#[workflow(debounce(...))]`
//!
//! Declaring a debounce policy collapses a burst of triggers for the same key
//! into one delayed run that fires once the burst settles.
//!
//! ```rust
//! use autumn_harvest::prelude::*;
//!
//! #[derive(Debug, serde::Deserialize, serde::Serialize)]
//! struct StripeWebhookPayload {
//!     account_id: String,
//!     event_type: String,
//!     payload: serde_json::Value,
//! }
//!
//! // Collapse all webhook events for the same account into one execution.
//! // The run fires 30 s after the last delivery. If bursts are continuous,
//! // the max-wait cap (5 min) guarantees the run still fires eventually.
//! #[workflow(debounce(key = "input.account_id", window = "30s", max_wait = "5m"))]
//! async fn process_stripe_webhook(
//!     ctx: &WorkflowContext,
//!     payload: StripeWebhookPayload,
//! ) -> Result<(), String> {
//!     // This run sees the *last* payload received within the window
//!     // (last-input-wins semantics). All earlier duplicate deliveries
//!     // were collapsed by the pre-start gate — no WorkflowEvent was appended
//!     // for them, so replay determinism is unaffected.
//!     ctx.execute_activity(&sync_account_info_info(), payload.account_id).await
//!         .map_err(|e| e.to_string())
//! }
//! ```
//!
//! ## Semantics
//!
//! | Property | Behaviour |
//! |---|---|
//! | **Trailing-edge** | Each trigger (re)sets the fire deadline to `now + window`. |
//! | **Burst collapse** | K triggers for the same key → exactly **1** execution. |
//! | **Last-input-wins** | The started run's input is the most recent request's input. |
//! | **Max-wait cap** | A continuously-bursting key fires no later than `first_seen + max_wait`. |
//! | **Key scoping** | Shard-local — consistent with per-key concurrency (#247). |
//! | **Replay safety** | Pre-start gate: no new `WorkflowEvent` variant, zero replay impact. |
//!
//! ## HTTP API response
//!
//! When a debounced start is admitted, the API returns `202 Accepted`:
//!
//! ```json
//! {
//!   "debounced": true,
//!   "debounce_key": "acct_1234",
//!   "fire_at": "2026-06-18T10:00:30Z",
//!   "pending_count": 3
//! }
//! ```
//!
//! The `pending_count` field accumulates across retries so the embedder can
//! observe the burst size. `fire_at` is the current projected fire deadline
//! (extended on each admission).
//!
//! ## Operator visibility
//!
//! Pending debounce records are visible via the management API:
//!
//! ```
//! GET /api/harvest/admin/debounce
//! ```
//!
//! Returns all pending records with `debounce_key`, `fire_at` (`effective_fire_at`),
//! `max_fire_at`, `pending_count`, `workflow_name`, `workflow_id`, `queue_name`,
//! `shard_id`, `created_at`, and `updated_at`.
//!
//! ## Configuration
//!
//! ### Per-workflow (macro attribute)
//!
//! ```rust
//! // Required: key + window.
//! #[workflow(debounce(key = "input.account_id", window = "30s"))]
//!
//! // Optional: max_wait overrides the server default (1 h).
//! #[workflow(debounce(key = "input.account_id", window = "30s", max_wait = "5m"))]
//! ```
//!
//! ### Per-request (fluent builder)
//!
//! ```rust
//! use autumn_harvest::debounce::DebouncePolicy;
//! use std::time::Duration;
//!
//! let info = process_stripe_webhook_info()
//!     .with_debounce(DebouncePolicy {
//!         key_expr: "input.account_id",
//!         window: Duration::from_secs(30),
//!         max_wait: Some(Duration::from_secs(300)),
//!     });
//! ```
//!
//! ### Server-side max-wait default
//!
//! The server default (1 h) can be changed per worker:
//!
//! ```rust
//! use std::time::Duration;
//! use autumn_harvest::WorkerConfig;
//!
//! WorkerConfig::default()
//!     .with_default_debounce_max_wait(Duration::from_secs(10 * 60)); // 10 min
//! ```
//!
//! ## Out of scope
//!
//! - Leading-edge debounce / throttle
//! - Event batching / aggregation (multiple payloads in one run)
//! - Debouncing signals to a *running* workflow
//! - Cross-shard global debounce coordination

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeWebhookPayload {
    pub account_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountInfo {
    pub account_id: String,
    pub name: String,
}

/// Sync account information from Stripe after a webhook burst settles.
///
/// The `window = "30s"` means each incoming webhook (re)sets the fire
/// deadline to `now + 30 s`. After 30 s of quiet the run begins.
/// `max_wait = "5m"` guarantees the run fires even during a continuous burst.
///
/// **Key expression `"input.account_id"`** is resolved against the request
/// input at admission time. Different `account_id` values produce independent
/// debounce records that fire independently — burst collapse is per-key.
#[workflow(debounce(key = "input.account_id", window = "30s", max_wait = "5m"))]
async fn process_stripe_webhook(
    ctx: &WorkflowContext,
    payload: StripeWebhookPayload,
) -> Result<(), String> {
    // This run uses the most recent payload received within the window.
    // All earlier duplicate deliveries were collapsed by the pre-start gate.
    let _: () = ctx
        .execute_activity(&sync_account_info_info(), payload.account_id)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// A workflow without debounce — every trigger starts an execution immediately.
///
/// Demonstrates backward compatibility: the debounce primitive is opt-in.
/// Omitting `debounce(...)` from `#[workflow]` leaves the start path
/// byte-for-byte unchanged.
#[workflow]
async fn process_payment_event(
    ctx: &WorkflowContext,
    payload: StripeWebhookPayload,
) -> Result<(), String> {
    let _: () = ctx
        .execute_activity(&sync_account_info_info(), payload.account_id)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[activity(start_to_close = "30s")]
async fn sync_account_info(_ctx: &ActivityContext, account_id: String) -> Result<(), String> {
    // In production: call Stripe API, write to DB, invalidate caches, etc.
    tracing::info!(account_id = %account_id, "syncing account info");
    Ok(())
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![process_stripe_webhook, process_payment_event];
    let _acts = activities![sync_account_info];
    println!("debounce_webhook example compiled successfully");
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::debounce::{DebouncePolicy, resolve_debounce_key};
    use std::time::Duration;

    #[test]
    fn debounce_policy_attached_to_workflow_info() {
        let info = process_stripe_webhook_info();
        let policy = info
            .debounce
            .expect("debounce policy should be attached by macro");
        assert_eq!(policy.key_expr, "input.account_id");
        assert_eq!(policy.window, Duration::from_secs(30));
        assert_eq!(policy.max_wait, Some(Duration::from_secs(300)));
    }

    #[test]
    fn non_debounced_workflow_has_no_policy() {
        let info = process_payment_event_info();
        assert!(
            info.debounce.is_none(),
            "workflow without debounce attr should have no policy"
        );
    }

    #[test]
    fn key_resolution_from_payload() {
        let payload = serde_json::json!({ "account_id": "acct_1234", "event_type": "charge.succeeded", "payload": {} });
        let key = resolve_debounce_key("input.account_id", &payload);
        assert_eq!(key, Some("acct_1234".to_string()));
    }

    #[test]
    fn missing_key_falls_through_to_normal_start() {
        // When the key resolves to None, the start falls through to the normal path.
        let payload = serde_json::json!({ "event_type": "charge.succeeded", "payload": {} });
        let key = resolve_debounce_key("input.account_id", &payload);
        assert_eq!(key, None);
    }

    #[test]
    fn with_debounce_builder_works() {
        let info = process_payment_event_info().with_debounce(DebouncePolicy {
            key_expr: "input.account_id",
            window: Duration::from_secs(60),
            max_wait: None,
        });
        let policy = info.debounce.expect("debounce policy attached via builder");
        assert_eq!(policy.key_expr, "input.account_id");
        assert_eq!(policy.window, Duration::from_secs(60));
        assert!(policy.max_wait.is_none());
    }
}
