#![allow(clippy::missing_errors_doc, clippy::unused_async)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::prelude::*;
use autumn_web::prelude::*;
use autumn_web::reexports::axum::extract::State;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const CHECKOUT_QUEUE: &str = "billing";
const PAYMENT_QUEUE: &str = "payments";
const INVOICE_QUEUE: &str = "invoices";
const OPS_QUEUE: &str = "ops";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CheckoutRequest {
    tenant_id: String,
    customer_id: String,
    plan: String,
    seats: u32,
    payment_method_id: String,
}

impl CheckoutRequest {
    fn normalized(mut self) -> Self {
        self.tenant_id = self.tenant_id.trim().to_ascii_lowercase();
        self.customer_id = self.customer_id.trim().to_owned();
        self.plan = self.plan.trim().to_ascii_lowercase();
        self.payment_method_id = self.payment_method_id.trim().to_owned();
        self
    }

    fn unit_price_cents(&self) -> u64 {
        match self.plan.as_str() {
            "enterprise" => 5_000,
            "pro" => 2_900,
            _ => 1_200,
        }
    }

    fn subtotal_cents(&self) -> u64 {
        self.unit_price_cents() * u64::from(self.seats)
    }

    fn requires_manual_review(&self) -> bool {
        self.subtotal_cents() >= 250_000
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CheckoutStartResponse {
    workflow_id: String,
    outbox: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PlanQuote {
    plan: &'static str,
    monthly_cents_per_seat: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct InvoiceRequest {
    tenant_id: String,
    customer_id: String,
    subscription_id: String,
    subtotal_cents: u64,
    tax_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct InvoiceResult {
    invoice_id: String,
    total_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BillingOutcome {
    subscription_id: String,
    invoice_id: String,
    capture_id: String,
    status: String,
}

fn checkout_workflow_id(request: &CheckoutRequest) -> String {
    format!(
        "billing-checkout:{}:{}:{}",
        request.tenant_id, request.customer_id, request.plan
    )
}

fn checkout_start_request(request: CheckoutRequest) -> WorkflowStartRequest {
    let request = request.normalized();
    WorkflowStartRequest {
        workflow_name: "billing_checkout".to_owned(),
        workflow_id: checkout_workflow_id(&request),
        queue_name: CHECKOUT_QUEUE.to_owned(),
        input: json!(request),
        memo: Some(json!({
            "kind": "subscription_checkout",
        })),
        search_attrs: Some(json!({
            "tenant_id": request.tenant_id,
            "customer_id": request.customer_id,
            "plan": request.plan,
        })),
    }
}

#[get("/billing/plans")]
async fn list_plans() -> Json<Vec<PlanQuote>> {
    Json(vec![
        PlanQuote {
            plan: "starter",
            monthly_cents_per_seat: 1_200,
        },
        PlanQuote {
            plan: "pro",
            monthly_cents_per_seat: 2_900,
        },
        PlanQuote {
            plan: "enterprise",
            monthly_cents_per_seat: 5_000,
        },
    ])
}

#[post("/billing/checkout")]
async fn start_checkout(
    State(state): State<autumn_web::AppState>,
    Json(request): Json<CheckoutRequest>,
) -> AutumnResult<Json<CheckoutStartResponse>> {
    let start = checkout_start_request(request);
    let pool = state.pool().cloned().ok_or_else(|| {
        AutumnError::service_unavailable_msg("billing checkout requires an application database")
    })?;
    let mut conn = pool
        .get()
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    enqueue_workflow_start_outbox(&mut conn, &start)
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    Ok(Json(CheckoutStartResponse {
        workflow_id: start.workflow_id,
        outbox: "queued",
    }))
}

#[workflow]
#[allow(clippy::too_many_lines)]
async fn billing_checkout(
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
    let capture_id = capture
        .get("capture_id")
        .and_then(Value::as_str)
        .unwrap_or("capture-missing")
        .to_owned();

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
async fn issue_initial_invoice(
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
async fn monthly_billing_cycle(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
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

#[activity(
    start_to_close = "10s",
    retry = RetryPolicy::exponential(3, Duration::from_millis(100)),
    queue = "billing"
)]
async fn validate_checkout(
    _ctx: &ActivityContext,
    request: CheckoutRequest,
) -> HarvestResult<CheckoutRequest> {
    let request = request.normalized();
    if request.seats == 0 {
        return Err(HarvestError::WorkflowFailed {
            name: "validate_checkout".to_owned(),
            reason: "seats must be greater than zero".to_owned(),
        });
    }
    Ok(request)
}

#[activity(start_to_close = "30s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)), queue = "billing")]
async fn create_customer_profile(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({
        "customer_profile_id": format!(
            "cus_{}_{}",
            input["tenant_id"].as_str().unwrap_or("tenant"),
            input["customer_id"].as_str().unwrap_or("customer")
        )
    }))
}

#[activity(start_to_close = "30s", queue = "billing")]
async fn delete_customer_profile(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
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
async fn authorize_payment(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
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
async fn void_payment_authorization(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(authorization = ?input, "voided payment authorization");
    Ok(json!({ "voided": true }))
}

#[activity(start_to_close = "30s", queue = "billing")]
async fn create_subscription_record(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({
        "subscription_id": input["subscription_id"],
        "state": "pending_capture",
    }))
}

#[activity(start_to_close = "30s", queue = "billing")]
async fn cancel_subscription_record(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(subscription = ?input, "cancelled subscription record");
    Ok(json!({ "cancelled": true }))
}

#[activity(start_to_close = "30s", queue = "invoices")]
async fn create_invoice(
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
async fn send_invoice(_ctx: &ActivityContext, invoice: InvoiceResult) -> HarvestResult<Value> {
    tracing::info!(invoice_id = %invoice.invoice_id, "sent invoice");
    Ok(json!({ "sent": true }))
}

#[activity(start_to_close = "30s", queue = "invoices")]
async fn void_invoice(_ctx: &ActivityContext, invoice: Value) -> HarvestResult<Value> {
    tracing::info!(invoice = ?invoice, "voided invoice during billing saga rollback");
    Ok(json!({ "voided": true }))
}

#[activity(start_to_close = "30s", queue = "payments")]
async fn record_payment_capture(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(capture = ?input, "recorded payment capture");
    Ok(json!({ "posted": true }))
}

#[activity(start_to_close = "30s", queue = "billing")]
async fn send_receipt(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(receipt = ?input, "sent receipt");
    Ok(json!({ "sent": true }))
}

#[activity(start_to_close = "1m", queue = "payments")]
async fn charge_subscription(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    tracing::info!(charge = ?input, "charged subscription billing cycle");
    Ok(json!({ "charged": true }))
}

#[activity(start_to_close = "2m", queue = "ops")]
async fn export_billing_events(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({ "exported": input }))
}

#[activity(start_to_close = "2m", queue = "ops")]
async fn reconcile_gateway(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({ "reconciled": input }))
}

#[activity(start_to_close = "30s", queue = "ops")]
async fn notify_finance(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({ "notified": input }))
}

#[dag(
    schedule = "0 6 * * *",
    catchup = false,
    max_active_runs = 1,
    default_queue = "ops"
)]
fn billing_reconciliation(dag: &mut DagBuilder) {
    let export = dag.activity(export_billing_events);
    let reconcile = dag
        .activity(reconcile_gateway)
        .upstream(&export)
        .retry(RetryPolicy::fixed(3, Duration::from_secs(30)));
    let _notify = dag
        .activity(notify_finance)
        .upstream(&reconcile)
        .trigger_rule(TriggerRule::AllDone);
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(routes![list_plans, start_checkout])
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![
                    billing_checkout,
                    issue_initial_invoice,
                    monthly_billing_cycle,
                ])
                .activities(activities![
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
                ])
                .dags(dags![billing_reconciliation])
                .worker(WorkerConfig::default().with_queues([
                    CHECKOUT_QUEUE,
                    PAYMENT_QUEUE,
                    INVOICE_QUEUE,
                    OPS_QUEUE,
                ]))
                .api("/api/harvest"),
        )
        .run()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::{WorkflowEvent, WorkflowSimulator};
    use serde_json::json;

    fn checkout() -> CheckoutRequest {
        CheckoutRequest {
            tenant_id: "ACME".to_owned(),
            customer_id: "cust_42".to_owned(),
            plan: "Pro".to_owned(),
            seats: 5,
            payment_method_id: "pm_card_demo".to_owned(),
        }
    }

    #[test]
    fn checkout_start_request_carries_outbox_metadata() {
        let request = checkout_start_request(checkout());

        assert_eq!(request.workflow_name, "billing_checkout");
        assert_eq!(request.queue_name, CHECKOUT_QUEUE);
        assert_eq!(request.workflow_id, "billing-checkout:acme:cust_42:pro");
        assert_eq!(
            request
                .search_attrs
                .as_ref()
                .and_then(|v| v["tenant_id"].as_str()),
            Some("acme")
        );
    }

    #[test]
    fn app_routes_and_harvest_registrations_are_present() {
        let routes = routes![list_plans, start_checkout];
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].path, "/billing/plans");
        assert_eq!(routes[1].path, "/billing/checkout");

        let workflows = workflows![
            billing_checkout,
            issue_initial_invoice,
            monthly_billing_cycle,
        ];
        assert_eq!(workflows.len(), 3);
        assert!(
            workflows
                .iter()
                .any(|workflow| workflow.name == "billing_checkout")
        );

        let authorize = __autumn_activity_info_authorize_payment();
        assert_eq!(authorize.default_queue, Some(PAYMENT_QUEUE));
        assert_eq!(authorize.max_concurrent, Some(8));
        assert_eq!(authorize.concurrency_key, Some("stripe"));

        let dag = __autumn_dag_info_billing_reconciliation();
        let definition = dag.build_definition().expect("billing DAG should compile");
        assert_eq!(definition.tasks().len(), 3);
    }

    #[tokio::test]
    async fn billing_checkout_happy_path_uses_saga_child_signal_version_and_timer() {
        let result = WorkflowSimulator::new(__autumn_workflow_info_billing_checkout().handler)
            .mock_activity("validate_checkout", Ok)
            .mock_activity("create_customer_profile", |_| {
                Ok(json!({ "customer_profile_id": "cus_demo" }))
            })
            .mock_activity("authorize_payment", |_| {
                Ok(json!({ "authorization_id": "auth_demo", "amount_cents": 14_500 }))
            })
            .mock_activity("create_subscription_record", |input| {
                Ok(json!({
                    "subscription_id": input["subscription_id"],
                    "state": "pending_capture",
                }))
            })
            .mock_child_workflow("issue_initial_invoice", |_| {
                Ok(json!({
                    "invoice_id": "inv_demo",
                    "total_cents": 15_950,
                }))
            })
            .mock_activity("record_payment_capture", |_| Ok(json!({ "posted": true })))
            .mock_activity("send_receipt", |_| Ok(json!({ "sent": true })))
            .send_signal(
                "payment_captured",
                json!({
                    "captured": true,
                    "capture_id": "cap_demo",
                }),
            )
            .run(json!(checkout()))
            .await;

        let output: BillingOutcome = serde_json::from_value(
            result
                .final_output
                .expect("billing checkout should complete"),
        )
        .expect("billing output should deserialize");
        assert_eq!(output.invoice_id, "inv_demo");
        assert_eq!(output.capture_id, "cap_demo");
        assert_eq!(output.status, "completed");

        assert!(result.history.iter().any(|event| matches!(
            event,
            WorkflowEvent::MarkerRecorded { name, details }
                if name == "version:billing_checkout_v2_tax" && details == &json!(2)
        )));
        assert!(result.history.iter().any(|event| matches!(
            event,
            WorkflowEvent::MarkerRecorded { name, .. }
                if name == "side_effect:subscription-id"
        )));
        assert!(result.history.iter().any(|event| matches!(
            event,
            WorkflowEvent::ChildWorkflowStarted { workflow_name, .. }
                if workflow_name == "issue_initial_invoice"
        )));
        assert!(result.history.iter().any(|event| matches!(
            event,
            WorkflowEvent::SignalReceived { signal_name, .. }
                if signal_name == "payment_captured"
        )));
        assert!(result.history.iter().any(|event| matches!(
            event,
            WorkflowEvent::TimerStarted { timer_id, .. }
                if timer_id.as_str() == "receipt-settlement-window"
        )));
    }

    #[tokio::test]
    async fn billing_checkout_compensates_when_invoice_child_fails() {
        let result = WorkflowSimulator::new(__autumn_workflow_info_billing_checkout().handler)
            .mock_activity("validate_checkout", Ok)
            .mock_activity("create_customer_profile", |_| {
                Ok(json!({ "customer_profile_id": "cus_demo" }))
            })
            .mock_activity("authorize_payment", |_| {
                Ok(json!({ "authorization_id": "auth_demo", "amount_cents": 14_500 }))
            })
            .mock_activity("create_subscription_record", |input| {
                Ok(json!({
                    "subscription_id": input["subscription_id"],
                    "state": "pending_capture",
                }))
            })
            .mock_child_workflow("issue_initial_invoice", |_| {
                Err("invoice service unavailable".to_owned())
            })
            .mock_activity("cancel_subscription_record", |_| {
                Ok(json!({ "cancelled": true }))
            })
            .mock_activity("void_payment_authorization", |_| {
                Ok(json!({ "voided": true }))
            })
            .mock_activity("delete_customer_profile", |_| {
                Ok(json!({ "deleted": true }))
            })
            .run(json!(checkout()))
            .await;

        let error = result
            .final_output
            .expect_err("billing checkout should fail when child workflow fails");
        assert!(error.contains("child-workflow:issue_initial_invoice"));

        let scheduled: Vec<&str> = result
            .history
            .iter()
            .filter_map(|event| match event {
                WorkflowEvent::ActivityScheduled { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect();
        assert!(scheduled.contains(&"cancel_subscription_record"));
        assert!(scheduled.contains(&"void_payment_authorization"));
        assert!(scheduled.contains(&"delete_customer_profile"));
    }

    #[tokio::test]
    async fn monthly_billing_cycle_continues_as_new_to_bound_history() {
        let result = WorkflowSimulator::new(__autumn_workflow_info_monthly_billing_cycle().handler)
            .mock_activity("charge_subscription", |_| Ok(json!({ "charged": true })))
            .run(json!({
                "subscription_id": "sub_demo",
                "cycle": 1,
                "stop_after": 3,
            }))
            .await;

        assert_eq!(
            result
                .final_output
                .expect("continue-as-new should surface next input"),
            json!({
                "subscription_id": "sub_demo",
                "cycle": 2,
                "stop_after": 3,
            })
        );
        assert!(
            result
                .history
                .iter()
                .any(|event| matches!(event, WorkflowEvent::WorkflowContinuedAsNew { .. }))
        );
    }

    #[tokio::test]
    async fn issue_initial_invoice_applies_versioned_tax_flag() {
        let result = WorkflowSimulator::new(__autumn_workflow_info_issue_initial_invoice().handler)
            .mock_activity("create_invoice", |input| {
                let request: InvoiceRequest =
                    serde_json::from_value(input).expect("invoice input should deserialize");
                Ok(json!(InvoiceResult {
                    invoice_id: "inv_taxed".to_owned(),
                    total_cents: request.subtotal_cents + (request.subtotal_cents / 10),
                }))
            })
            .mock_activity("send_invoice", |_| Ok(json!({ "sent": true })))
            .run(json!(InvoiceRequest {
                tenant_id: "acme".to_owned(),
                customer_id: "cust_42".to_owned(),
                subscription_id: "sub_demo".to_owned(),
                subtotal_cents: 14_500,
                tax_enabled: true,
            }))
            .await;

        assert_eq!(
            result.final_output.expect("invoice child should complete"),
            json!(InvoiceResult {
                invoice_id: "inv_taxed".to_owned(),
                total_cents: 15_950,
            })
        );
    }
}
