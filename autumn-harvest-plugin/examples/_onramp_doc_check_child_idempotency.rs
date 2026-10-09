#![allow(dead_code, unused)]
use std::time::Duration;
use autumn_harvest::prelude::*;
mod ch5 { use super::*;
#[workflow]
async fn issue_invoice(ctx: &WorkflowContext, order_id: String) -> HarvestResult<String> {
    let pdf = ctx
        .execute_activity_raw(
            "render_invoice_pdf",
            serde_json::json!({ "order_id": order_id }),
            "default",
        )
        .await?;

    ctx.execute_activity_raw(
        "email_invoice",
        serde_json::json!({ "order_id": order_id, "pdf_url": pdf["url"] }),
        "default",
    )
    .await?;

    Ok(pdf["url"].as_str().unwrap_or("").to_owned())
}

#[workflow]
async fn checkout(ctx: &WorkflowContext, order_id: String) -> HarvestResult<String> {
    // ... reserve inventory, wait for signal, fulfill ...

    let invoice_url = ctx
        .spawn_child_workflow_raw(
            "issue_invoice",
            serde_json::json!(order_id),
        )
        .await?;

    Ok(invoice_url.as_str().unwrap_or("").to_owned())
}
}
mod ch6 { use super::*;
async fn stripe_charge(_a: u64, _c: &str, _k: &str) -> Result<String, String> { Ok(String::new()) }
#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(2)))]
async fn charge_card(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let amount_cents = input["amount_cents"].as_u64().unwrap_or(0);
    let customer_id = input["customer_id"].as_str().unwrap_or("").to_owned();

    let idem_key = ctx.idempotency_key().map_err(|e| e.to_string())?.as_str().to_owned();

    // Pass idem_key as Stripe's Idempotency-Key header. Subsequent retries
    // for this attempt carry the same key, so Stripe returns the original
    // charge instead of creating a new one.
    let charge_id = stripe_charge(amount_cents, &customer_id, &idem_key)
        .await
        .map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "charge_id": charge_id }))
}
}
fn main() {}
