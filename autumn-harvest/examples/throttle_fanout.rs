#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Workflow-start throttle — pace admissions, defer the excess (issue #607).
//!
//! ## The problem
//!
//! A scheduled fan-out, a post-downtime backfill (#337), or a webhook storm
//! (#344) can admit *hundreds of distinct, legitimate starts in a single tick*.
//! When every one of those workflows hammers the same rate-limited downstream
//! (a third-party API capped at 100 req/min, a fragile internal service, a
//! vendor quota), the operator wants a front-door knob to **pace admissions to
//! a sustained rate and defer the excess** — dropping nothing, rejecting nothing.
//!
//! The existing tools each miss: activity RPS limits (#332) only kick in *after*
//! N workflows have already started and are now all parked on the bucket;
//! debounce (#499) *drops* redundant same-key triggers; the admission gate
//! (#377) is a *binary halt*; per-key concurrency (#247) bounds *in-flight
//! count*, not *rate*.
//!
//! ## The solution: `#[workflow(throttle(...))]`
//!
//! Declaring a throttle paces starts through a per-key token bucket. Excess
//! starts (when the bucket is empty) are **durably deferred** in Postgres —
//! *before* any `WorkflowStarted` event exists — and admitted later by a
//! background scanner as tokens refill.
//!
//! ```rust
//! use autumn_harvest::prelude::*;
//!
//! #[derive(Debug, serde::Deserialize, serde::Serialize)]
//! struct SyncRequest {
//!     tenant_id: String,
//!     resource: String,
//! }
//!
//! // Admit at most 100 starts/min per tenant, allowing a burst of 20. A burst
//! // of 500 starts admits 20 immediately, then drains at 100/min — every one
//! // eventually runs, none is dropped, and the downstream never exceeds quota.
//! #[workflow(throttle(rate = "100/m", burst = 20, key = "input.tenant_id"))]
//! async fn sync_tenant(ctx: &WorkflowContext, req: SyncRequest) -> Result<(), String> {
//!     ctx.execute_activity(&call_downstream_info(), req.resource)
//!         .await
//!         .map_err(|e| e.to_string())
//! }
//! ```
//!
//! ## Semantics
//!
//! | Property | Behaviour |
//! |---|---|
//! | **Token bucket / GCRA** | Admitted starts for a key never exceed `rate` (+ `burst`) over any window. |
//! | **Defer, don't drop** | An excess start is durably persisted and admitted later — nothing is dropped (contrast debounce) or rejected. |
//! | **Preserve every start** | Every start eventually runs — many pending rows per key, drained FIFO (contrast debounce's collapse-to-one). |
//! | **Durable** | Deferred starts survive a plugin/worker restart — no in-memory pending queue. |
//! | **Key scoping** | Shard-local — consistent with per-key concurrency (#247) and the activity limiter. |
//! | **Replay safety** | Pre-start gate: no new `WorkflowEvent` variant, zero replay impact. |
//!
//! ## Rate string
//!
//! `rate = "<count>/<unit>"` where `<unit>` is `s` (per second), `m` (per
//! minute), or `h` (per hour): `"5/s"`, `"100/m"`, `"3600/h"`. `burst` defaults
//! to the count when omitted (one window's worth). `schedule_to_start = "5m"`
//! (optional) times a deferred start out rather than running it stale.
//!
//! ## Interaction contracts
//!
//! - **id-reuse short-circuit consumes no token** — a start that attaches to an
//!   existing run under a reuse policy refunds its reserved token.
//! - **halt beats pace** — an active admission gate (#377) rejects the start
//!   (503) before any token is consumed or row deferred.
//! - **stale-out** — a start deferred past `schedule_to_start` is dropped by the
//!   scanner rather than run stale.
//!
//! ## HTTP API response
//!
//! When a start is deferred, the API returns `202 Accepted`:
//!
//! ```json
//! {
//!   "throttled": true,
//!   "workflow_name": "sync_tenant",
//!   "workflow_id": "…",
//!   "throttle_key": "acme",
//!   "deferred_at": "2026-07-06T10:00:00Z"
//! }
//! ```
//!
//! An under-limit start returns the normal `201 Created` (a token was available).
//!
//! ## Operator visibility
//!
//! The per-key deferred-start backlog is visible via the management API:
//!
//! ```
//! GET /api/harvest/admin/start-throttle
//! ```
//!
//! Returns per-`(workflow_name, throttle_key)` `deferred_count`,
//! `oldest_deferred_at`, and `shard_id`, so an operator can confirm a throttle
//! is active and watch the backlog drain.
//!
//! ## Configuration
//!
//! ### Per-workflow (macro attribute)
//!
//! ```rust
//! // Keyed per-tenant, 100/min, burst 20.
//! #[workflow(throttle(rate = "100/m", burst = 20, key = "input.tenant_id"))]
//!
//! // Unkeyed (global-per-workflow), 5/s, default burst (5), stale-out at 5m.
//! #[workflow(throttle(rate = "5/s", schedule_to_start = "5m"))]
//! ```
//!
//! ### Fluent builder
//!
//! ```rust
//! use autumn_harvest::throttle::ThrottlePolicy;
//!
//! let info = sync_tenant_info().with_throttle(
//!     ThrottlePolicy::from_rate_str("100/m", Some(20.0), Some("input.tenant_id"), None)
//!         .expect("valid rate"),
//! );
//! ```
//!
//! ## Out of scope (issue #607)
//!
//! - Cross-shard *global* rate coordination — buckets are per-shard, so a key's
//!   rate across a multi-shard cluster is `per-shard-rate × shards`. A true
//!   global ceiling is a documented known limitation.
//! - Activity-level RPS limiting (#332), debounce (#499), the admission gate
//!   (#377), and in-flight concurrency caps (#247) — distinct primitives.

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRequest {
    pub tenant_id: String,
    pub resource: String,
}

/// Pace tenant-sync starts at 100/min per tenant, burst 20.
///
/// A scheduled fan-out or backfill that emits 500 sync requests for one tenant
/// admits 20 immediately (the burst), then drains at 100/min — every request
/// eventually runs, and the rate-limited downstream never exceeds its quota.
/// Different `tenant_id` values throttle independently (per-key buckets).
#[workflow(throttle(rate = "100/m", burst = 20, key = "input.tenant_id"))]
async fn sync_tenant(ctx: &WorkflowContext, req: SyncRequest) -> Result<(), String> {
    let _: () = ctx
        .execute_activity(&call_downstream_info(), req.resource)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// An unkeyed (global-per-workflow) throttle: at most 5 starts/sec across the
/// whole workflow type, with a 5-minute stale-out deadline.
#[workflow(throttle(rate = "5/s", schedule_to_start = "5m"))]
async fn global_reindex(ctx: &WorkflowContext, req: SyncRequest) -> Result<(), String> {
    let _: () = ctx
        .execute_activity(&call_downstream_info(), req.resource)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// A workflow without a throttle — every start is admitted immediately.
///
/// Demonstrates backward compatibility: the throttle is opt-in. Omitting
/// `throttle(...)` leaves the start path byte-for-byte unchanged.
#[workflow]
async fn sync_now(ctx: &WorkflowContext, req: SyncRequest) -> Result<(), String> {
    let _: () = ctx
        .execute_activity(&call_downstream_info(), req.resource)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[activity(start_to_close = "30s")]
async fn call_downstream(_ctx: &ActivityContext, resource: String) -> Result<(), String> {
    // In production: call the rate-limited third-party API / fragile service.
    tracing::info!(resource = %resource, "calling downstream");
    Ok(())
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![sync_tenant, global_reindex, sync_now];
    let _acts = activities![call_downstream];
    println!("throttle_fanout example compiled successfully");
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::throttle::{ThrottlePolicy, resolve_throttle_key};

    #[test]
    fn throttle_policy_attached_to_workflow_info() {
        let info = sync_tenant_info();
        let policy = info
            .throttle
            .expect("throttle policy should be attached by macro");
        assert!((policy.refill_per_sec - 100.0 / 60.0).abs() < 1e-9);
        assert!((policy.burst - 20.0).abs() < 1e-9);
        assert_eq!(policy.key_expr, Some("input.tenant_id"));
        assert!(policy.schedule_to_start.is_none());
    }

    #[test]
    fn unkeyed_throttle_has_no_key_and_default_burst() {
        let info = global_reindex_info();
        let policy = info.throttle.expect("throttle policy attached");
        assert!((policy.refill_per_sec - 5.0).abs() < 1e-9);
        // burst defaults to the per-period count (5) when not set explicitly.
        assert!((policy.burst - 5.0).abs() < 1e-9);
        assert!(policy.key_expr.is_none());
        assert_eq!(
            policy.schedule_to_start,
            Some(std::time::Duration::from_secs(300))
        );
    }

    #[test]
    fn non_throttled_workflow_has_no_policy() {
        assert!(sync_now_info().throttle.is_none());
    }

    #[test]
    fn key_resolution_from_payload() {
        let payload = serde_json::json!({ "tenant_id": "acme", "resource": "invoices" });
        assert_eq!(
            resolve_throttle_key("input.tenant_id", &payload),
            Some("acme".to_string())
        );
    }

    #[test]
    fn with_throttle_builder_works() {
        let policy =
            ThrottlePolicy::from_rate_str("60/m", Some(10.0), Some("input.tenant_id"), None)
                .unwrap();
        let info = sync_now_info().with_throttle(policy);
        let attached = info.throttle.expect("throttle attached via builder");
        assert!((attached.refill_per_sec - 1.0).abs() < 1e-9);
        assert!((attached.burst - 10.0).abs() < 1e-9);
    }
}
