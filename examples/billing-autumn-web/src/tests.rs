use autumn_harvest::{WorkflowEvent, WorkflowSimulator};
use serde_json::json;

use crate::domain::{
    BillingOutcome, CHECKOUT_QUEUE, CheckoutRequest, InvoiceRequest, InvoiceResult, PAYMENT_QUEUE,
    checkout_start_request,
};
use crate::{activities, dags, routes, workflows};

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
    let routes = routes::routes();
    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].path, "/billing/plans");
    assert_eq!(routes[1].path, "/billing/checkout");

    let workflows = workflows::workflows();
    assert_eq!(workflows.len(), 3);
    assert!(
        workflows
            .iter()
            .any(|workflow| workflow.name == "billing_checkout")
    );

    let authorize = activities::__autumn_activity_info_authorize_payment();
    assert_eq!(authorize.default_queue, Some(PAYMENT_QUEUE));
    assert_eq!(authorize.max_concurrent, Some(8));
    assert_eq!(authorize.concurrency_key, Some("stripe"));

    let dag = dags::__autumn_dag_info_billing_reconciliation();
    let definition = dag.build_definition().expect("billing DAG should compile");
    assert_eq!(definition.tasks().len(), 3);
}

#[tokio::test]
async fn billing_checkout_happy_path_uses_saga_child_signal_version_and_timer() {
    let result =
        WorkflowSimulator::new(workflows::__autumn_workflow_info_billing_checkout().handler)
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
    let result =
        WorkflowSimulator::new(workflows::__autumn_workflow_info_billing_checkout().handler)
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
    let result =
        WorkflowSimulator::new(workflows::__autumn_workflow_info_monthly_billing_cycle().handler)
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
    let result =
        WorkflowSimulator::new(workflows::__autumn_workflow_info_issue_initial_invoice().handler)
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
