use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::{WorkflowEvent, WorkflowSimulator};
use autumn_harvest_plugin::api::{HarvestApiState, StandaloneAdminAuth, harvest_api_router};
use autumn_harvest_plugin::harvest_ui_router;
use autumn_harvest_plugin::metrics_scrape::HarvestMetricsRecorder;
use autumn_harvest_plugin::prelude::HarvestMode;
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
///
/// `run` gets its Harvest router from `HarvestEmbedding`, which needs a
/// database. `harvest_mount` builds the same composition from a bare state.
/// `tests/acceptance.rs` tests the started binary against Postgres.
fn router_under_test() -> axum::Router {
    build_router(
        harvest_mount(&StandaloneAdminAuth::new()),
        HarvestMetricsRecorder::new(),
        None,
    )
}

/// The composition `HarvestEmbedding` mounts: the API with Vantage nested,
/// under the declared auth layers. It covers the composition only.
fn harvest_mount(auth: &StandaloneAdminAuth) -> axum::Router {
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    auth.mount(router, &api_state)
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

/// Closes issue #1609. `HarvestEmbedding` reads the README's
/// `AUTUMN_PROFILE=dev` and declares it through `StandaloneAdminAuth`. This
/// declares the same profile directly.
///
/// The request runs twice. The `preflight` handler used to reset the
/// deployment profile from the router's `autumn_web::AppState` on every
/// call, and a placeholder `AppState::for_test()` reports `"default"`, not
/// `"dev"`. That would have closed the gate again after the first request.
/// The router now carries no `AppState` at all (issue #1606), so the reset
/// has no source left. This pins its absence.
#[tokio::test]
async fn preflight_succeeds_once_dev_profile_is_declared() {
    let auth = StandaloneAdminAuth::new().with_deployment_profile("dev");
    let app = build_router(harvest_mount(&auth), HarvestMetricsRecorder::new(), None);

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
    let app = build_router(harvest_mount(&StandaloneAdminAuth::new()), metrics, None);

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

/// Issue #1615. The manifest must not name `autumn-web` in any dependency
/// table. A rename through `package = "autumn-web"` counts as a name too.
#[test]
fn manifest_names_no_autumn_web() {
    let manifest: toml::Table =
        toml::from_str(include_str!("../Cargo.toml")).expect("Cargo.toml should parse");
    let offenders = autumn_web_entries(&manifest);
    assert!(
        offenders.is_empty(),
        "standalone-runner must not depend on autumn-web, found: {offenders:?}"
    );
}

/// Return each dependency key that resolves to the `autumn-web` package.
fn autumn_web_entries(manifest: &toml::Table) -> Vec<String> {
    const TABLES: [&str; 3] = ["dependencies", "dev-dependencies", "build-dependencies"];
    let mut scopes = vec![manifest];
    if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
        scopes.extend(targets.values().filter_map(toml::Value::as_table));
    }
    let mut found = Vec::new();
    for scope in scopes {
        for table in TABLES.iter().filter_map(|name| scope.get(*name)) {
            for (key, spec) in table.as_table().into_iter().flatten() {
                let package = spec
                    .get("package")
                    .and_then(toml::Value::as_str)
                    .unwrap_or(key);
                if package == "autumn-web" {
                    found.push(key.clone());
                }
            }
        }
    }
    found
}

#[test]
fn guard_detects_every_way_to_name_autumn_web() {
    let manifest: toml::Table = toml::from_str(
        r#"
        [dependencies]
        autumn-web = "0.7"
        [dev-dependencies]
        web = { package = "autumn-web", version = "0.7" }
        [target.'cfg(unix)'.build-dependencies]
        autumn-web = "0.7"
        "#,
    )
    .expect("fixture should parse");
    assert_eq!(autumn_web_entries(&manifest).len(), 3);
}

/// Issue #1615. No code line may name the `autumn_web` crate. A re-export
/// through another crate would compile, so the guard reads the source.
#[test]
fn source_names_no_autumn_web_path() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut offenders = Vec::new();
    for dir in ["src", "tests"] {
        for path in rust_files(&root.join(dir)) {
            let text = std::fs::read_to_string(&path).expect("source should read");
            for (number, line) in text.lines().enumerate() {
                let code = line.split("//").next().unwrap_or_default();
                if names_crate(code, CRATE) {
                    offenders.push(format!("{}:{}", path.display(), number + 1));
                }
            }
        }
    }
    assert!(offenders.is_empty(), "autumn-web paths: {offenders:?}");
}

/// The crate name the source guard looks for, split so this file passes.
const CRATE: &str = concat!("autumn", "_web");

/// True when `code` holds `name` as a whole identifier.
fn names_crate(code: &str, name: &str) -> bool {
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';
    code.match_indices(name).any(|(start, _)| {
        let before = code[..start].chars().next_back();
        let after = code[start + name.len()..].chars().next();
        !before.is_some_and(is_ident) && !after.is_some_and(is_ident)
    })
}

#[test]
fn source_guard_matches_whole_identifiers_only() {
    let path = format!("use {CRATE}::config;");
    let longer = format!("fn {CRATE}_entries() {{}}");
    assert!(names_crate(&path, CRATE));
    assert!(names_crate(&format!("x::{CRATE}::y"), CRATE));
    assert!(!names_crate(&longer, CRATE));
    assert!(!names_crate(&format!("my_{CRATE}"), CRATE));
}

fn rust_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
    files
}
