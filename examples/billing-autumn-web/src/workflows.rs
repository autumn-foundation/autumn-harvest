use std::sync::{Arc, Mutex};

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

use crate::domain::{
    BillingOutcome, CHECKOUT_QUEUE, CheckoutRequest, INVOICE_QUEUE, InvoiceRequest, InvoiceResult,
    OPS_QUEUE, PAYMENT_QUEUE,
};

pub fn workflows() -> Vec<WorkflowInfo> {
    workflows![
        billing_checkout,
        issue_initial_invoice,
        monthly_billing_cycle,
    ]
}

#[workflow]
#[allow(clippy::too_many_lines)]
pub async fn billing_checkout(
    ctx: &WorkflowContext,
    request: CheckoutRequest,
) -> HarvestResult<BillingOutcome> {
    let status = Arc::new(Mutex::new(String::from("validating")));
    let query_status = Arc::clone(&status);
    ctx.register_query("status", move || {
        json!({
            "status": query_status.lock().expect("status query lock poisoned").as_str(),
        })
    });

    let validated = ctx
        .execute_activity_raw("validate_checkout", json!(request), CHECKOUT_QUEUE)
        .await?;
    let checkout: CheckoutRequest = serde_json::from_value(validated)?;
    let tax_version = ctx.version("billing_checkout_v2_tax", 1, 2);
    let subscription_uuid = ctx.random_uuid("subscription-id")?;
    let subscription_id = format!("sub_{}", subscription_uuid.simple());

    *status.lock().expect("status lock poisoned") = String::from("reserving");
    let mut saga = Saga::new(ctx);

    let customer_profile = saga
        .step(
            || async {
                ctx.execute_activity_raw(
                    "create_customer_profile",
                    json!({
                        "tenant_id": checkout.tenant_id,
                        "customer_id": checkout.customer_id,
                    }),
                    CHECKOUT_QUEUE,
                )
                .await
            },
            |profile| async move {
                ctx.execute_activity_raw("delete_customer_profile", profile, CHECKOUT_QUEUE)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    let authorization = saga
        .step(
            || async {
                ctx.execute_activity_raw(
                    "authorize_payment",
                    json!({
                        "customer_profile": customer_profile,
                        "payment_method_id": checkout.payment_method_id,
                        "amount_cents": checkout.subtotal_cents(),
                    }),
                    PAYMENT_QUEUE,
                )
                .await
            },
            |auth| async move {
                ctx.execute_activity_raw("void_payment_authorization", auth, PAYMENT_QUEUE)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    let subscription_record = saga
        .step(
            || async {
                ctx.execute_activity_raw(
                    "create_subscription_record",
                    json!({
                        "subscription_id": subscription_id,
                        "tenant_id": checkout.tenant_id,
                        "customer_id": checkout.customer_id,
                        "plan": checkout.plan,
                        "seats": checkout.seats,
                    }),
                    CHECKOUT_QUEUE,
                )
                .await
            },
            |record| async move {
                ctx.execute_activity_raw("cancel_subscription_record", record, CHECKOUT_QUEUE)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    if checkout.requires_manual_review() {
        ctx.execute_activity_external(
            "approve_high_value_subscription",
            json!({
                "subscription": subscription_record,
                "authorization": authorization,
            }),
            OPS_QUEUE,
            24 * 60 * 60,
        )
        .await?;
    }

    let invoice_input = InvoiceRequest {
        tenant_id: checkout.tenant_id.clone(),
        customer_id: checkout.customer_id.clone(),
        subscription_id: subscription_id.clone(),
        subtotal_cents: checkout.subtotal_cents(),
        tax_enabled: tax_version >= 2,
    };
    let invoice = saga
        .step(
            || async {
                ctx.spawn_child_workflow_raw("issue_initial_invoice", json!(invoice_input))
                    .await
            },
            |invoice| async move {
                ctx.execute_activity_raw("void_invoice", invoice, INVOICE_QUEUE)
                    .await
                    .map(|_| ())
            },
        )
        .await?;
    let invoice: InvoiceResult = serde_json::from_value(invoice)?;

    *status.lock().expect("status lock poisoned") = String::from("awaiting_payment_capture");
    let capture = ctx.wait_for_signal("payment_captured").await?;
    let captured = capture
        .get("captured")
        .and_then(Value::as_bool)
        .unwrap_or_default();
    if !captured {
        saga.compensate_all().await?;
        return Err(HarvestError::WorkflowFailed {
            name: "billing_checkout".to_owned(),
            reason: "payment capture was rejected".to_owned(),
        });
    }
    let Some(capture_id) = capture.get("capture_id").and_then(Value::as_str) else {
        saga.compensate_all().await?;
        return Err(HarvestError::WorkflowFailed {
            name: "billing_checkout".to_owned(),
            reason: "payment_captured signal missing capture_id".to_owned(),
        });
    };
    let capture_id = capture_id.to_owned();

    ctx.execute_activity_raw(
        "record_payment_capture",
        json!({
            "subscription_id": subscription_id,
            "invoice_id": invoice.invoice_id,
            "capture_id": capture_id,
        }),
        PAYMENT_QUEUE,
    )
    .await?;
    ctx.timer("receipt-settlement-window", 1).await?;
    ctx.execute_activity_raw(
        "send_receipt",
        json!({
            "tenant_id": checkout.tenant_id,
            "customer_id": checkout.customer_id,
            "invoice_id": invoice.invoice_id,
        }),
        CHECKOUT_QUEUE,
    )
    .await?;

    *status.lock().expect("status lock poisoned") = String::from("completed");
    Ok(BillingOutcome {
        subscription_id,
        invoice_id: invoice.invoice_id,
        capture_id,
        status: "completed".to_owned(),
    })
}

#[workflow]
pub async fn issue_initial_invoice(
    ctx: &WorkflowContext,
    request: InvoiceRequest,
) -> HarvestResult<InvoiceResult> {
    let invoice = ctx
        .execute_activity_raw("create_invoice", json!(request), INVOICE_QUEUE)
        .await?;
    let invoice: InvoiceResult = serde_json::from_value(invoice)?;
    ctx.execute_activity_raw("send_invoice", json!(invoice), INVOICE_QUEUE)
        .await?;
    Ok(invoice)
}

#[workflow]
pub async fn monthly_billing_cycle(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
    let cycle = input.get("cycle").and_then(Value::as_u64).unwrap_or(1);
    let stop_after = input
        .get("stop_after")
        .and_then(Value::as_u64)
        .unwrap_or(12);
    ctx.register_query("cycle", move || json!({ "cycle": cycle }));

    if cycle > stop_after {
        return Ok(json!({
            "status": "complete",
            "cycle": cycle,
        }));
    }

    ctx.execute_activity_raw(
        "charge_subscription",
        json!({
            "subscription_id": input.get("subscription_id").cloned().unwrap_or(Value::Null),
            "cycle": cycle,
        }),
        PAYMENT_QUEUE,
    )
    .await?;
    ctx.timer(&format!("next-cycle-{cycle}"), 86_400).await?;
    ctx.continue_as_new(json!({
        "subscription_id": input.get("subscription_id").cloned().unwrap_or(Value::Null),
        "cycle": cycle + 1,
        "stop_after": stop_after,
    }))
    .await?;

    unreachable!("continue_as_new does not return during live execution")
}
