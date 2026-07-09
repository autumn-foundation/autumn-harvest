use std::sync::{Arc, Mutex};

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

use crate::activities::{
    authorize_payment_info, cancel_subscription_record_info, charge_subscription_info,
    create_customer_profile_info, create_invoice_info, create_subscription_record_info,
    delete_customer_profile_info, record_payment_capture_info, send_invoice_info,
    send_receipt_info, validate_checkout_info, void_invoice_info, void_payment_authorization_info,
};
use crate::domain::{BillingOutcome, CheckoutRequest, InvoiceRequest, InvoiceResult, OPS_QUEUE};

pub fn workflows() -> Vec<WorkflowInfo> {
    workflows![
        billing_checkout,
        issue_initial_invoice,
        monthly_billing_cycle,
    ]
}

#[workflow(
    owner = "billing-team",
    runbook = "https://wiki.acme.com/billing-runbook",
    severity = "sev1"
)]
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

    let checkout: CheckoutRequest = ctx
        .execute_activity(&validate_checkout_info(), request)
        .await?;
    let tax_version = ctx.version("billing_checkout_v2_tax", 1, 2);
    let subscription_uuid = ctx.random_uuid("subscription-id")?;
    let subscription_id = format!("sub_{}", subscription_uuid.simple());

    *status.lock().expect("status lock poisoned") = String::from("reserving");
    let mut saga = Saga::new(ctx);

    let customer_profile: Value = saga
        .step(
            || async {
                ctx.execute_activity(
                    &create_customer_profile_info(),
                    json!({
                        "tenant_id": checkout.tenant_id,
                        "customer_id": checkout.customer_id,
                    }),
                )
                .await
            },
            |profile| async move {
                ctx.execute_activity::<_, Value>(&delete_customer_profile_info(), profile)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    let authorization: Value = saga
        .step(
            || async {
                ctx.execute_activity(
                    &authorize_payment_info(),
                    json!({
                        "customer_profile": customer_profile,
                        "payment_method_id": checkout.payment_method_id,
                        "amount_cents": checkout.subtotal_cents(),
                    }),
                )
                .await
            },
            |auth| async move {
                ctx.execute_activity::<_, Value>(&void_payment_authorization_info(), auth)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    let subscription_record: Value = saga
        .step(
            || async {
                ctx.execute_activity(
                    &create_subscription_record_info(),
                    json!({
                        "subscription_id": subscription_id,
                        "tenant_id": checkout.tenant_id,
                        "customer_id": checkout.customer_id,
                        "plan": checkout.plan,
                        "seats": checkout.seats,
                    }),
                )
                .await
            },
            |record| async move {
                ctx.execute_activity::<_, Value>(&cancel_subscription_record_info(), record)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    if checkout.requires_manual_review() {
        saga.step(
            || async {
                ctx.execute_activity_external(
                    "approve_high_value_subscription",
                    json!({
                        "subscription": subscription_record,
                        "authorization": authorization,
                    }),
                    OPS_QUEUE,
                    24 * 60 * 60,
                )
                .await
            },
            |_| async { Ok::<(), HarvestError>(()) },
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
    let invoice: InvoiceResult = saga
        .step(
            || async {
                ctx.spawn_child_workflow(&issue_initial_invoice_info(), &invoice_input)
                    .await
            },
            |invoice: InvoiceResult| async move {
                ctx.execute_activity::<_, Value>(&void_invoice_info(), json!(invoice))
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    *status.lock().expect("status lock poisoned") = String::from("awaiting_payment_capture");
    let capture: Value = ctx.receive_signal("payment_captured").await?;
    let captured = capture
        .get("captured")
        .and_then(Value::as_bool)
        .unwrap_or_default();
    if !captured {
        saga.compensate_all().await?;
        return Err(HarvestError::workflow_failed_untyped(
            "billing_checkout",
            "payment capture was rejected",
        ));
    }
    let Some(capture_id) = capture.get("capture_id").and_then(Value::as_str) else {
        saga.compensate_all().await?;
        return Err(HarvestError::workflow_failed_untyped(
            "billing_checkout",
            "payment_captured signal missing capture_id",
        ));
    };
    let capture_id = capture_id.to_owned();

    ctx.execute_activity::<_, Value>(
        &record_payment_capture_info(),
        json!({
            "subscription_id": subscription_id,
            "invoice_id": invoice.invoice_id,
            "capture_id": capture_id,
        }),
    )
    .await?;
    ctx.timer("receipt-settlement-window", 1).await?;
    ctx.execute_activity::<_, Value>(
        &send_receipt_info(),
        json!({
            "tenant_id": checkout.tenant_id,
            "customer_id": checkout.customer_id,
            "invoice_id": invoice.invoice_id,
        }),
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

#[workflow(
    owner = "billing-team",
    runbook = "https://wiki.acme.com/invoice-runbook",
    severity = "sev2"
)]
pub async fn issue_initial_invoice(
    ctx: &WorkflowContext,
    request: InvoiceRequest,
) -> HarvestResult<InvoiceResult> {
    let invoice: InvoiceResult = ctx
        .execute_activity(&create_invoice_info(), request)
        .await?;
    ctx.execute_activity::<_, Value>(&send_invoice_info(), &invoice)
        .await?;
    Ok(invoice)
}

#[workflow(
    owner = "billing-team",
    runbook = "https://wiki.acme.com/cycle-runbook",
    severity = "sev3"
)]
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

    ctx.execute_activity::<_, Value>(
        &charge_subscription_info(),
        json!({
            "subscription_id": input.get("subscription_id").cloned().unwrap_or(Value::Null),
            "cycle": cycle,
        }),
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
