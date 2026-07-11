#![allow(clippy::doc_markdown, clippy::missing_errors_doc, clippy::unused_async)]
//! Activity execution interceptors (issue #680).
//!
//! An interceptor wraps *every* activity execution on the worker — regular and
//! local — with cross-cutting behavior added **once**, at registration time,
//! rather than editing every `#[activity]` function.
//!
//! This example defines a single `LoggingMetricsInterceptor` that:
//!
//! - logs a structured start line (including the propagated
//!   `x-correlation-id` context header via `ctx.header(..)`),
//! - runs the rest of the chain (`next.run(input)`),
//! - logs a finish line with the elapsed wall-clock duration (or the error),
//! - records a per-activity `activity.duration_ms` histogram via
//!   `ctx.metrics()` (the replay-safe custom-metrics API from issue #532;
//!   activity metrics are **not** replay-suppressed — each attempt is a
//!   separate execution).
//!
//! It is registered ONCE with `.activity_interceptor(...)` and applies across
//! *both* `#[activity]` functions below with zero per-activity code.
//!
//! ```text
//! HarvestBuilder::default()
//!     .activity_interceptor(LoggingMetricsInterceptor::default())
//!     .activities(activities![charge_card, send_receipt])
//! ```
//!
//! Run the embedded tests with:
//!   cargo test --example activity_interceptor --features testing

use std::time::Instant;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

// ── Domain types ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Charge {
    pub order_id: String,
    pub amount_usd: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Receipt {
    pub order_id: String,
    pub email: String,
}

// ── Activities (ordinary — no interceptor-specific code) ─────────────────────

#[activity(start_to_close = "10s")]
async fn charge_card(_ctx: &ActivityContext, charge: Charge) -> Result<String, String> {
    Ok(format!("txn-{}", charge.order_id))
}

#[activity(start_to_close = "10s")]
async fn send_receipt(_ctx: &ActivityContext, receipt: Receipt) -> Result<bool, String> {
    let _ = receipt.email;
    Ok(true)
}

// ── The interceptor ─────────────────────────────────────────────────────────

/// Logs structured start/finish lines and records a per-activity duration
/// histogram. Registered once; wraps every activity on the worker.
///
/// The context header key it reads is `x-correlation-id` (propagated by the
/// engine into every activity via the trace-context / header carrier).
#[derive(Default)]
pub struct LoggingMetricsInterceptor;

/// The context-header key this interceptor reads for correlation.
pub const CORRELATION_HEADER: &str = "x-correlation-id";

impl ActivityInterceptor for LoggingMetricsInterceptor {
    fn intercept<'a>(
        &'a self,
        invocation: &'a ActivityInvocation<'a>,
        ctx: &'a ActivityContext,
        input: serde_json::Value,
        next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a> {
        Box::pin(async move {
            let name = invocation.name().to_string();
            let is_local = invocation.is_local();
            let correlation_id = ctx
                .header(CORRELATION_HEADER)
                .unwrap_or("<none>")
                .to_string();
            let started = Instant::now();

            tracing::info!(
                activity = %name,
                is_local,
                correlation_id = %correlation_id,
                "activity start"
            );

            let result = next.run(input).await;
            #[allow(clippy::cast_precision_loss)]
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;

            // Per-activity duration metric (harvest.user.activity.duration_ms).
            // Low-cardinality label only — the activity NAME, never execution.id.
            ctx.metrics()
                .histogram("activity.duration_ms", elapsed_ms, &[("activity", &name)]);

            match &result {
                Ok(_) => tracing::info!(
                    activity = %name,
                    elapsed_ms,
                    correlation_id = %correlation_id,
                    "activity finished ok"
                ),
                Err(e) => tracing::warn!(
                    activity = %name,
                    elapsed_ms,
                    correlation_id = %correlation_id,
                    error = %e,
                    "activity finished with error"
                ),
            }

            result
        })
    }
}

fn main() {
    // Registration is a single builder call — the interceptor then wraps every
    // activity execution (regular + local) on the worker.
    let _builder = HarvestBuilder::default()
        .activity_interceptor(LoggingMetricsInterceptor)
        .activities(activities![charge_card, send_receipt]);

    println!(
        "Registered LoggingMetricsInterceptor once; it wraps every activity \
         (charge_card, send_receipt) with zero per-activity code."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::interceptor::{ActivityInvocation, run_interceptor_chain_for_test};
    use autumn_harvest::telemetry::{MetricsRecorder, USER_METRIC_PREFIX};
    use std::sync::{Arc, Mutex};

    /// Recorder that captures the histogram calls the interceptor makes.
    #[derive(Default)]
    struct CapturingMetrics {
        histograms: Mutex<Vec<(String, f64, Vec<(String, String)>)>>,
    }
    impl MetricsRecorder for CapturingMetrics {
        fn record_user_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
            assert!(name.starts_with(USER_METRIC_PREFIX));
            self.histograms.lock().unwrap().push((
                name.to_string(),
                value,
                labels
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            ));
        }
    }

    #[tokio::test]
    async fn interceptor_records_duration_metric_and_passes_result_through() {
        let metrics = Arc::new(CapturingMetrics::default());
        let ctx = ActivityContext::new_test().with_metrics(metrics.clone());
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> =
            vec![Arc::new(LoggingMetricsInterceptor)];
        let invocation = ActivityInvocation::for_test("charge_card", false, "default");

        // Drive the interceptor chain directly over a terminal that returns a
        // value — the interceptor must record a duration metric and pass the
        // result through unchanged.
        let out = run_interceptor_chain_for_test(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!({"order_id": "o1", "amount_usd": 10.0}),
            |input| Box::pin(async move { Ok(input) }),
        )
        .await
        .unwrap();

        assert_eq!(
            out,
            serde_json::json!({"order_id": "o1", "amount_usd": 10.0})
        );
        let hist = metrics.histograms.lock().unwrap();
        assert_eq!(hist.len(), 1, "one duration histogram must be recorded");
        assert_eq!(hist[0].0, "harvest.user.activity.duration_ms");
        assert_eq!(
            hist[0].2,
            vec![("activity".to_string(), "charge_card".to_string())]
        );
    }

    #[tokio::test]
    async fn interceptor_records_metric_even_on_error() {
        let metrics = Arc::new(CapturingMetrics::default());
        let ctx = ActivityContext::new_test().with_metrics(metrics.clone());
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> =
            vec![Arc::new(LoggingMetricsInterceptor)];
        let invocation = ActivityInvocation::for_test("send_receipt", false, "default");

        let err = run_interceptor_chain_for_test(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!(null),
            |_input| Box::pin(async move { Err("downstream 500".to_string()) }),
        )
        .await
        .unwrap_err();

        assert_eq!(err, "downstream 500");
        // The duration metric is recorded regardless of Ok/Err.
        assert_eq!(metrics.histograms.lock().unwrap().len(), 1);
    }

    /// An interceptor that captures the `CORRELATION_HEADER` value it read from
    /// the `ActivityContext`, so the test can assert the interceptor genuinely
    /// observes a propagated context header (issue #481).
    struct CaptureHeaderInterceptor {
        observed: Arc<Mutex<Option<String>>>,
    }
    impl ActivityInterceptor for CaptureHeaderInterceptor {
        fn intercept<'a>(
            &'a self,
            _invocation: &'a ActivityInvocation<'a>,
            ctx: &'a ActivityContext,
            input: serde_json::Value,
            next: ActivityInterceptorNext<'a>,
        ) -> ActivityInterceptorFuture<'a> {
            let observed = self.observed.clone();
            let value = ctx.header(CORRELATION_HEADER).map(str::to_string);
            Box::pin(async move {
                *observed.lock().unwrap() = value;
                next.run(input).await
            })
        }
    }

    #[tokio::test]
    async fn interceptor_reads_correlation_header_from_ctx() {
        // Build an ActivityContext carrying the correlation header, as the worker
        // does at dispatch time from the execution's propagated context_headers.
        let mut headers = std::collections::HashMap::new();
        headers.insert(CORRELATION_HEADER.to_string(), "corr-abc".to_string());
        let ctx = ActivityContext::new_test().with_context_headers(Arc::new(headers));

        let observed = Arc::new(Mutex::new(None));
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> =
            vec![Arc::new(CaptureHeaderInterceptor {
                observed: observed.clone(),
            })];
        let invocation = ActivityInvocation::for_test("charge_card", false, "default");

        run_interceptor_chain_for_test(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!(null),
            |input| Box::pin(async move { Ok(input) }),
        )
        .await
        .unwrap();

        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some("corr-abc"),
            "the interceptor must read the propagated correlation header from ctx"
        );
    }
}
