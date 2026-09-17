use autumn_harvest::{WorkflowEvent, WorkflowSimulator};
use autumn_harvest_plugin::HarvestApiState;
use autumn_harvest_plugin::prelude::HarvestMode;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::domain::{RUNNER_QUEUE, StandaloneOrder};
use crate::runtime::{standalone_builder, standalone_runtime_config};
use crate::server::build_router;
use crate::workflows;

#[test]
fn runtime_config_uses_external_runner_mode_without_outbox() {
    let config = standalone_runtime_config("postgres://runner:runner@localhost/runner".to_owned());

    assert_eq!(config.mode, HarvestMode::External);
    assert!(config.worker_enabled);
    assert!(config.scheduler_enabled);
    assert!(!config.outbox.enabled);
}

#[test]
fn builder_registers_runner_owned_workflows_and_activities() {
    let built = standalone_builder()
        .try_build()
        .expect("standalone runner registrations should build");

    assert_eq!(built.workflow_count(), 2);
    assert_eq!(built.activity_count(), 3);
    assert_eq!(built.worker_config().queues, vec![RUNNER_QUEUE.to_owned()]);
}

#[tokio::test]
async fn standalone_order_uses_version_gate_saga_and_child_workflow() {
    let result =
        WorkflowSimulator::new(workflows::__autumn_workflow_info_standalone_order().handler)
            .mock_activity("reserve_inventory", |input| {
                let order: StandaloneOrder =
                    serde_json::from_value(input).expect("order should deserialize");
                Ok(json!({
                    "reservation_id": format!("res_{}", order.order_id),
                    "sku": order.sku,
                    "quantity": order.quantity,
                }))
            })
            .mock_child_workflow("standalone_shipping", |input| {
                Ok(json!({
                    "label_id": format!("lbl_{}", input["order_id"].as_str().unwrap_or("order")),
                    "carrier": input["carrier"],
                }))
            })
            .run(json!(StandaloneOrder {
                order_id: "order-1001".to_owned(),
                sku: "sku-book".to_owned(),
                quantity: 2,
            }))
            .await;

    assert_eq!(
        result
            .final_output
            .expect("standalone order should complete"),
        json!({
            "order_id": "order-1001",
            "shipment": {
                "label_id": "lbl_order-1001",
                "carrier": "ground",
            },
            "version": 2,
        })
    );
    assert!(result.history.iter().any(|event| matches!(
        event,
        WorkflowEvent::MarkerRecorded { name, details }
            if name == "version:standalone_order_shipping_v2" && details == &json!(2)
    )));
    assert!(result.history.iter().any(|event| matches!(
        event,
        WorkflowEvent::ChildWorkflowStarted { workflow_name, .. }
            if workflow_name == "standalone_shipping"
    )));
}

/// HTTP-level coverage for the assembled router `server.rs` mounts (issue
/// #1610). `runtime_config_uses_external_runner_mode_without_outbox` and its
/// two siblings above assert the workflow/config layer; nothing before this
/// exercised the router itself, which is how the `AppState::for_test()` call
/// in the production entry point (issue #1607) and the always-`401`
/// documented `preflight` step (issue #1609) both went unnoticed until a
/// real embedder hit them. No database is needed: `HarvestApiState::new()`
/// with nothing installed matches a router that has never received traffic,
/// which is exactly the state these three routes must tolerate.
fn router_under_test() -> axum::Router {
    build_router(HarvestApiState::new(), AppState::for_test())
}

async fn get_status(app: axum::Router, uri: &str) -> StatusCode {
    app.oneshot(
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("request should build"),
    )
    .await
    .expect("router should serve the request")
    .status()
}

#[tokio::test]
async fn health_route_needs_no_database() {
    assert_eq!(
        get_status(router_under_test(), "/api/harvest/health").await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn openapi_document_is_ungated() {
    assert_eq!(
        get_status(router_under_test(), "/api/harvest/openapi.json").await,
        StatusCode::OK
    );
}

/// Pins issue #1609: the example's own README documents this exact request
/// as the deployment preflight step, but the example never declares a
/// credential, so the admin gate fails closed. This assertion is the
/// regression guard, not the fix — flip it to `OK` once #1609 gives the
/// example a credential to present.
#[tokio::test]
async fn preflight_without_a_credential_is_rejected() {
    assert_eq!(
        get_status(router_under_test(), "/api/harvest/admin/preflight").await,
        StatusCode::UNAUTHORIZED
    );
}
