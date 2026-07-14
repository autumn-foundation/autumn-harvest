#![allow(clippy::doc_markdown, clippy::missing_errors_doc, clippy::unused_async)]
//! Deterministic side-effect primitives example (issue #384).
//!
//! Demonstrates the first-class, replay-safe alternatives to the unsafe APIs the
//! determinism guardrails (HVG001 / HVG002) warn about — no local activity, no
//! magic strings, one `WorkflowContext` method call per value:
//!
//! - `ctx.system_now()`  — wall-clock instant captured once and replayed verbatim
//! - `ctx.new_uuid()`    — UUIDv7 idempotency key captured once
//! - `ctx.random_range()`— a sampling draw captured once
//! - `ctx.side_effect()` — any one-shot value (here, an environment lookup)
//!
//! Each helper lowers onto a single `SideEffectRecorded` event in
//! `harvest_events`, so the engine replays the identical sequence on every
//! worker, every time.
//!
//! Run with:
//!   cargo run --example deterministic_primitives

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

// ── Domain types ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NotificationEvent {
    /// Unix-epoch seconds when the underlying event occurred.
    pub occurred_at_epoch_secs: i64,
    pub recipient: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NotificationDecision {
    pub idempotency_key: String,
    pub send: bool,
    /// 0–99 bucket used for a percentage rollout.
    pub rollout_bucket: i32,
    pub region: String,
}

// ── Activity ─────────────────────────────────────────────────────────────────

#[activity(start_to_close = "10s", queue = "email-workers")]
pub async fn send_email(_ctx: &ActivityContext, key: String) -> Result<(), String> {
    println!("[Activity] sending email with idempotency key {key}");
    Ok(())
}

// ── Workflow ─────────────────────────────────────────────────────────────────

/// Decides whether to send a notification using only replay-safe primitives.
///
/// A real time-based decision: skip the notification if the event is already
/// older than 24h *now* (not at workflow-start time). `system_now()` captures
/// the current instant once and freezes it, so the comparison is deterministic
/// across replays.
#[workflow(description = "Replay-safe notification decision using ctx deterministic primitives")]
pub async fn notify_decision(
    ctx: &WorkflowContext,
    event: NotificationEvent,
) -> Result<NotificationDecision, String> {
    // 1. Wall-clock decision — captured once, replayed verbatim.
    let now = ctx.system_now();
    let age_secs = now.timestamp() - event.occurred_at_epoch_secs;
    let fresh = age_secs < 24 * 60 * 60;

    // 2. Idempotency key — a UUIDv7 captured once. Safe to reuse across retries.
    let idempotency_key = ctx.new_uuid().to_string();

    // 3. Sampling draw — a 10% rollout. Deterministic across replays.
    let rollout_bucket: i32 = ctx.random_range(0..100);
    let in_rollout = rollout_bucket < 10;

    // 4. Capture an environment lookup ONCE via side_effect. The closure runs
    //    only on the first execution; replays return the recorded value.
    let region: String = ctx
        .side_effect("deploy_region", || {
            // harvest-suppress: DET004 "env read is wrapped in ctx.side_effect(); recorded once and replayed deterministically"
            std::env::var("DEPLOY_REGION").unwrap_or_else(|_| "us-east-1".to_string())
        })
        .map_err(|e| e.to_string())?;

    let send = fresh && in_rollout;
    if send {
        let _: () = ctx
            .execute_activity(&send_email_info(), idempotency_key.clone())
            .await
            .map_err(|e| e.to_string())?;
    }

    Ok(NotificationDecision {
        idempotency_key,
        send,
        rollout_bucket,
        region,
    })
}

fn main() {
    println!("deterministic_primitives example loaded successfully!");
    println!();
    println!("Replay-safe primitives demonstrated:");
    println!("  ctx.system_now()        — wall-clock captured once, replayed verbatim");
    println!("  ctx.new_uuid()          — UUIDv7 idempotency key captured once");
    println!("  ctx.random_range(0..100)— sampling draw, deterministic across replays");
    println!("  ctx.side_effect(name,f) — one-shot environment lookup captured once");
    println!();
    println!("Register on a HarvestBuilder:");
    println!("  .workflows(workflows![notify_decision])");
    println!("  .activities(activities![send_email])");
}
