use std::time::Duration;

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

use crate::domain::{CheckoutRequest, InvoiceRequest, InvoiceResult};

pub fn activities() -> Vec<ActivityInfo> {
    activities![
        validate_checkout,
        create_customer_profile,
        delete_customer_profile,
        authorize_payment,
        void_payment_authorization,
        create_subscription_record,
        cancel_subscription_record,
        create_invoice,
        send_invoice,
        void_invoice,
        record_payment_capture,
        send_receipt,
        charge_subscription,
        export_billing_events,
        reconcile_gateway,
        notify_finance,
        scan_discrepancies,
        flag_for_audit,
        auto_close_run,
        send_reconciliation_summary,
    ]
}

/// Validate the checkout payload before any side-effects.
///
/// Demonstrates issue #227: returning `ActivityFailure::non_retryable(...)`
/// short-circuits the retry policy on a permanently-invalid input. The
/// `RetryPolicy::exponential(3, …)` would normally allow three attempts;
/// the typed `non_retryable` flag wins over the policy and routes the
/// task straight to the DLQ on attempt 1 — proving that authors no longer
/// need to register every fail-fast error string in `non_retryable_errors`.
#[activity(
    start_to_close = "10s",
    retry = RetryPolicy::exponential(3, Duration::from_millis(100)),
    queue = "billing"
)]
pub async fn validate_checkout(
    _ctx: &ActivityContext,
    request: CheckoutRequest,
) -> Result<CheckoutRequest, ActivityFailure> {
    let request = request.normalized();
    if request.seats == 0 {
        // Permanent validation failure — operators see this as
        // `error.type="PermanentValidation"` on `harvest.activity.failed`.
        return Err(ActivityFailure::non_retryable(
            "PermanentValidation",
            "seats must be greater than zero",
        ));
    }
    Ok(request)
}

#[activity(start_to_close = "30s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)), queue = "billing")]
pub async fn create_customer_profile(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({
        "customer_profile_id": format!(
            "cus_{}_{}",
            input["tenant_id"].as_str().unwrap_or("tenant"),
            input["customer_id"].as_str().unwrap_or("customer")
        )
    }))
}

#[activity(start_to_close = "30s", queue = "billing")]
pub async fn delete_customer_profile(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(profile = ?input, "deleted customer profile during billing saga rollback");
    Ok(json!({ "deleted": true }))
}

#[activity(
    start_to_close = "30s",
    heartbeat_timeout = "10s",
    retry = RetryPolicy::exponential(5, Duration::from_secs(1)),
    queue = "payments",
    max_concurrent = 8,
    concurrency_key = "stripe"
)]
pub async fn authorize_payment(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({
        "authorization_id": "auth_demo",
        "amount_cents": input["amount_cents"],
    }))
}

#[activity(
    start_to_close = "30s",
    queue = "payments",
    max_concurrent = 8,
    concurrency_key = "stripe"
)]
pub async fn void_payment_authorization(
    _ctx: &ActivityContext,
    input: Value,
) -> HarvestResult<Value> {
    tracing::info!(authorization = ?input, "voided payment authorization");
    Ok(json!({ "voided": true }))
}

#[activity(start_to_close = "30s", queue = "billing")]
pub async fn create_subscription_record(
    _ctx: &ActivityContext,
    input: Value,
) -> HarvestResult<Value> {
    Ok(json!({
        "subscription_id": input["subscription_id"],
        "state": "pending_capture",
    }))
}

#[activity(start_to_close = "30s", queue = "billing")]
pub async fn cancel_subscription_record(
    _ctx: &ActivityContext,
    input: Value,
) -> HarvestResult<Value> {
    tracing::info!(subscription = ?input, "cancelled subscription record");
    Ok(json!({ "cancelled": true }))
}

#[activity(start_to_close = "30s", queue = "invoices")]
pub async fn create_invoice(
    _ctx: &ActivityContext,
    input: InvoiceRequest,
) -> HarvestResult<InvoiceResult> {
    let tax_cents = if input.tax_enabled {
        input.subtotal_cents / 10
    } else {
        0
    };
    Ok(InvoiceResult {
        invoice_id: format!("inv_{}_{}", input.tenant_id, input.subscription_id),
        total_cents: input.subtotal_cents + tax_cents,
    })
}

#[activity(start_to_close = "30s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)), queue = "invoices")]
pub async fn send_invoice(_ctx: &ActivityContext, invoice: InvoiceResult) -> HarvestResult<Value> {
    tracing::info!(invoice_id = %invoice.invoice_id, "sent invoice");
    Ok(json!({ "sent": true }))
}

#[activity(start_to_close = "30s", queue = "invoices")]
pub async fn void_invoice(_ctx: &ActivityContext, invoice: Value) -> HarvestResult<Value> {
    tracing::info!(invoice = ?invoice, "voided invoice during billing saga rollback");
    Ok(json!({ "voided": true }))
}

#[activity(start_to_close = "30s", queue = "payments")]
pub async fn record_payment_capture(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(capture = ?input, "recorded payment capture");
    Ok(json!({ "posted": true }))
}

#[activity(start_to_close = "30s", queue = "billing")]
pub async fn send_receipt(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(receipt = ?input, "sent receipt");
    Ok(json!({ "sent": true }))
}

#[activity(start_to_close = "1m", queue = "payments")]
pub async fn charge_subscription(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(charge = ?input, "charged subscription billing cycle");
    Ok(json!({ "charged": true }))
}

#[activity(start_to_close = "2m", queue = "ops")]
pub async fn export_billing_events(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({ "exported": input }))
}

#[activity(start_to_close = "2m", queue = "ops")]
pub async fn reconcile_gateway(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({ "reconciled": input }))
}

#[activity(start_to_close = "30s", queue = "ops")]
pub async fn notify_finance(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({ "notified": input }))
}

// Activities for the data-dependent branching example (anomaly_routing DAG).

#[activity(start_to_close = "2m", queue = "ops")]
pub async fn scan_discrepancies(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    // Returns the count of billing discrepancies found in the reconciliation window.
    Ok(json!({ "discrepancy_count": 0, "window": input }))
}

// Runs only when scan_discrepancies reported at least one discrepancy.
#[activity(start_to_close = "5m", queue = "ops")]
pub async fn flag_for_audit(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::warn!(report = ?input, "discrepancies detected — flagging for finance audit");
    Ok(json!({ "flagged": true }))
}

// Runs only when scan_discrepancies reported zero discrepancies.
#[activity(start_to_close = "30s", queue = "ops")]
pub async fn auto_close_run(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(report = ?input, "reconciliation clean — auto-closing run");
    Ok(json!({ "closed": true }))
}

#[activity(start_to_close = "30s", queue = "ops")]
pub async fn send_reconciliation_summary(
    _ctx: &ActivityContext,
    input: Value,
) -> HarvestResult<Value> {
    tracing::info!(summary = ?input, "sent reconciliation summary");
    Ok(json!({ "sent": true }))
}
