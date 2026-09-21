use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::{WorkflowEvent, WorkflowSimulator};
use autumn_harvest_plugin::HarvestApiState;
use autumn_harvest_plugin::metrics_scrape::HarvestMetricsRecorder;
use autumn_harvest_plugin::prelude::HarvestMode;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::domain::{RUNNER_QUEUE, StandaloneOrder};
use crate::runtime::{standalone_builder, standalone_runtime_config};
use crate::server::{build_router, declare_deployment_profile};
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
    let built = standalone_builder(HarvestMetricsRecorder::new())
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
/// #1610). The three tests above assert only the workflow and config layer.
/// Nothing before this exercised the router itself. That gap let two live
/// defects go unnoticed until a real embedder hit them. The first was the
/// `AppState::for_test()` call in the production entry point, now removed
/// (issue #1607). The second was the always-`401` documented `preflight`
/// step (issue #1609). No database is needed here. `HarvestApiState::new()`
/// with nothing installed matches a router that has never received traffic.
/// That is exactly the state these three routes must tolerate.
fn router_under_test() -> axum::Router {
    build_router(HarvestApiState::new(), HarvestMetricsRecorder::new())
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

/// Pins issue #1609's fail-closed default. `router_under_test` never
/// declares a profile, so the admin gate has nothing to open on.
#[tokio::test]
async fn preflight_without_a_credential_is_rejected() {
    assert_eq!(
        get_status(router_under_test(), "/api/harvest/admin/preflight").await,
        StatusCode::UNAUTHORIZED
    );
}

/// Closes issue #1609. `declare_deployment_profile` is what `run` calls
/// before installing the runner's API runtime, so this reproduces the
/// exact posture the README's `AUTUMN_PROFILE=dev` command produces.
///
/// The request runs twice. The `preflight` handler used to reset the
/// deployment profile from the router's `autumn_web::AppState` on every
/// call, and a placeholder `AppState::for_test()` reports `"default"`, not
/// `"dev"`. That would have closed the gate again after the first request.
/// The router now carries no `AppState` at all (issue #1606), so the reset
/// has no source left. This pins its absence.
#[tokio::test]
async fn preflight_succeeds_once_dev_profile_is_declared() {
    let api_state = HarvestApiState::new();
    declare_deployment_profile(&api_state, Some("dev"));
    let app = build_router(api_state, HarvestMetricsRecorder::new());

    assert_eq!(
        get_status(app.clone(), "/api/harvest/admin/preflight").await,
        StatusCode::OK
    );
    assert_eq!(
        get_status(app, "/api/harvest/admin/preflight").await,
        StatusCode::OK
    );
}

/// Closes issue #1611. A standalone mount has no `autumn_web::actuator`
/// endpoint to feed, so `/metrics` is the only place its recorded samples
/// are ever readable. Before this route, an embedder who enabled the
/// recorder had every sample discarded, with no way to render them out.
#[tokio::test]
async fn metrics_route_renders_a_recorded_sample_as_prometheus_text() {
    let metrics = HarvestMetricsRecorder::new();
    metrics.record_workflow_started("standalone_order", RUNNER_QUEUE);
    let app = build_router(HarvestApiState::new(), metrics);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should serve the request");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("content-type header should be set"),
        "text/plain; version=0.0.4"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let text = String::from_utf8(body.to_vec()).expect("response body should be UTF-8");
    assert!(text.contains("# TYPE harvest_workflow_started_total counter\n"));
    assert!(text.contains(&format!(
        "harvest_workflow_started_total{{workflow=\"standalone_order\",queue=\"{RUNNER_QUEUE}\"}} 1\n"
    )));
}
