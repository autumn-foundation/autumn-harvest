#![allow(clippy::doc_markdown, clippy::missing_errors_doc, clippy::unused_async)]
//! Replay-safe custom business metrics example (issue #758, shipped as #532).
//!
//! A workflow author emits the metrics their *business* cares about —
//! `orders_fulfilled`, `orders_auto_rejected`, `order_amount_usd` — into the
//! same `MetricsRecorder` pipeline the engine's `harvest.*` metrics already
//! use, without double-counting on replay:
//!
//! - `ctx.metrics().counter(...)`   from a **workflow branch** — suppressed on
//!   every replay cycle, emitted exactly once at the live execution frontier.
//!   This is the shape activities can't cover: the meaningful event here is an
//!   orchestration *decision* ("took the auto-reject branch").
//! - `ctx.metrics().histogram(...)` from an **activity** — never suppressed;
//!   each retry attempt is a separate execution and emits independently.
//!
//! Names are auto-namespaced under `harvest.user.` (e.g.
//! `harvest.user.orders_fulfilled`), so `harvest.*` engine dashboards are never
//! polluted. Labels must be low-cardinality: `execution.id` / `workflow.id` as
//! a label key is a documented anti-pattern and is rejected (ADR-0001 §7).
//! Delivery is best-effort at-least-once across a worker crash — see the
//! "Custom workflow/activity metrics" section of `docs/telemetry.md`.
//!
//! Run with:
//!   cargo run --example business_metrics

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

// ── Domain types ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Order {
    pub order_id: String,
    pub amount_usd: f64,
    /// Low-cardinality label value ("gold" / "silver" / "bronze").
    pub tier: String,
}

// ── Activity ────────────────────────────────────────────────────────────────

/// Charges the card and emits a histogram sample per attempt.
///
/// Activity metrics are **never** suppressed: each retry is a separate
/// execution, so `charge_attempts` counts attempts, not orders — intentional
/// "each attempt counts" semantics.
#[activity(start_to_close = "30s", queue = "payments")]
pub async fn charge_card(ctx: &ActivityContext, order: Order) -> Result<String, String> {
    // Histogram from an activity: emits `harvest.user.order_amount_usd`.
    ctx.metrics().histogram(
        "order_amount_usd",
        order.amount_usd,
        &[("tier", &order.tier)],
    );
    ctx.metrics()
        .counter("charge_attempts", 1, &[("payment_method", "card")]);

    // … real charge logic lives here …
    Ok(format!("txn-{}", order.order_id))
}

// ── Workflow ────────────────────────────────────────────────────────────────

/// Fulfills an order, emitting business counters from workflow branches.
///
/// The `ctx.metrics()` calls below are replay-safe: the executor re-runs this
/// function on every replay cycle, but emission happens only at the live
/// execution frontier (`ctx.is_replaying() == false`), so a counter
/// incremented once in workflow code increments the backend exactly once.
/// No `WorkflowEvent` is recorded — history stays byte-identical.
#[workflow(description = "Order fulfillment emitting replay-safe business metrics")]
pub async fn fulfill_order(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    // Counter from a workflow BRANCH — an orchestration decision an activity
    // could not observe without awkward plumbing.
    if order.amount_usd > 10_000.0 {
        ctx.metrics()
            .counter("orders_auto_rejected", 1, &[("tier", &order.tier)]);
        return Ok("auto_rejected".to_string());
    }

    let txn: String = ctx
        .execute_activity(&charge_card_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;

    ctx.metrics()
        .counter("orders_fulfilled", 1, &[("tier", &order.tier)]);
    Ok(txn)
}

fn main() {
    let _wfs = workflows![fulfill_order];
    let _acts = activities![charge_card];
    println!("business_metrics example compiled successfully");
    println!();
    println!("Replay-safe custom metrics demonstrated:");
    println!("  ctx.metrics().counter(\"orders_fulfilled\", 1, &[(\"tier\", tier)])");
    println!("      — workflow branch counter, suppressed during replay");
    println!("  ctx.metrics().histogram(\"order_amount_usd\", amount, &[(\"tier\", tier)])");
    println!("      — activity histogram, emitted on every attempt");
    println!();
    println!("Emitted names are namespaced: harvest.user.orders_fulfilled, …");
    println!("Wire a backend with HarvestBuilder::telemetry(...) — see docs/telemetry.md.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::telemetry::{MetricsRecorder, USER_METRIC_PREFIX};
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Counting recorder: `is_enabled()` keeps its default `true`, so
    /// `UserMetrics` does not short-circuit.
    #[derive(Default)]
    struct CountingMetrics {
        counters: AtomicU64,
    }

    impl MetricsRecorder for CountingMetrics {
        fn record_user_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
            assert!(name.starts_with(USER_METRIC_PREFIX));
            self.counters.fetch_add(value, Ordering::SeqCst);
        }
    }

    fn order() -> serde_json::Value {
        json!({"order_id": "ord_1", "amount_usd": 49.99, "tier": "gold"})
    }

    #[tokio::test]
    async fn live_run_emits_the_fulfilled_counter_exactly_once() {
        let metrics = Arc::new(CountingMetrics::default());
        let outcome = WorkflowTestEnv::new()
            .with_metrics(metrics.clone())
            .mock_activity("charge_card", |_| Ok(json!("txn-ord_1")))
            .run(fulfill_order_info().handler, order())
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
        // The test env drives multiple executor iterations (live frontier +
        // replay after each suspension); the counter still nets exactly one.
        assert_eq!(metrics.counters.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn replay_of_the_recorded_history_emits_zero_metrics() {
        let metrics = Arc::new(CountingMetrics::default());
        let outcome = WorkflowTestEnv::new()
            .with_metrics(Arc::new(CountingMetrics::default()))
            .mock_activity("charge_card", |_| Ok(json!("txn-ord_1")))
            .run(fulfill_order_info().handler, order())
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        // Replay the recorded history five times: replay must succeed AND
        // emit nothing — the counter was already emitted on the live run.
        for cycle in 0..5 {
            let report = WorkflowReplayer::new()
                .register_fn("fulfill_order", fulfill_order_info().handler)
                .with_metrics(metrics.clone())
                .replay_from_events(outcome.events().to_vec())
                .await;
            assert!(
                matches!(report.status, ReplayStatus::ReplaySucceeded),
                "replay cycle {cycle} must succeed:\n{report}"
            );
        }
        assert_eq!(
            metrics.counters.load(Ordering::SeqCst),
            0,
            "workflow metrics must be suppressed during replay"
        );
    }

    #[tokio::test]
    async fn auto_reject_branch_emits_its_own_counter_once() {
        let metrics = Arc::new(CountingMetrics::default());
        let outcome = WorkflowTestEnv::new()
            .with_metrics(metrics.clone())
            .run(
                fulfill_order_info().handler,
                json!({"order_id": "ord_2", "amount_usd": 25_000.0, "tier": "gold"}),
            )
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
        assert_eq!(outcome.result.as_ref().unwrap(), &json!("auto_rejected"));
        assert_eq!(metrics.counters.load(Ordering::SeqCst), 1);
    }
}
