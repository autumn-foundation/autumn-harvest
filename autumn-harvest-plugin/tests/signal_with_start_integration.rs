//! Integration tests for `POST /workflows/{workflow_name}/signal-with-start` (issue #244).

#![allow(clippy::similar_names, clippy::redundant_clone)]

use std::sync::Arc;

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn test_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "onboarding",
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ))
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("sws-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response is JSON")
    };
    (status, json)
}

#[tokio::test]
async fn signal_with_start_starts_fresh_and_returns_created() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": "user-1",
            "start_input": {"email": "user@example.com"},
            "signal_name": "stripe.webhook",
            "signal_payload": {"event_id": "evt_1"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert_eq!(body["started_fresh"], json!(true));
    assert_eq!(body["signal_delivered"], json!(true));
    assert!(body["execution_id"].is_string());
    assert_eq!(body["workflow_name"], "onboarding");
    assert_eq!(body["workflow_id"], "user-1");
    assert_eq!(body["state"], "RUNNING");
}

#[tokio::test]
async fn signal_with_start_attaches_to_running_and_returns_ok() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (_, first_body) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": "user-2",
            "start_input": {},
            "signal_name": "first",
            "signal_payload": {}
        }),
    )
    .await;
    let first_id = first_body["execution_id"].as_str().unwrap().to_string();

    let (status, body) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": "user-2",
            "start_input": {},
            "signal_name": "second",
            "signal_payload": {"hello": "again"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["started_fresh"], json!(false));
    assert_eq!(body["signal_delivered"], json!(true));
    assert_eq!(body["execution_id"].as_str().unwrap(), first_id);
}

#[tokio::test]
async fn signal_with_start_reject_duplicate_returns_409() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (_, _) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": "user-3",
            "start_input": {},
            "signal_name": "x",
            "signal_payload": {}
        }),
    )
    .await;

    let (status, _body) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": "user-3",
            "start_input": {},
            "signal_name": "x",
            "signal_payload": {},
            "id_reuse_policy": "reject_duplicate"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn signal_with_start_idempotency_key_dedupes_signal() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (s1, b1) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": "user-4",
            "start_input": {},
            "signal_name": "webhook",
            "signal_payload": {"n": 1},
            "idempotency_key": "evt-42"
        }),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED);
    assert_eq!(b1["signal_delivered"], json!(true));

    let (s2, b2) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": "user-4",
            "start_input": {},
            "signal_name": "webhook",
            "signal_payload": {"n": 1},
            "idempotency_key": "evt-42"
        }),
    )
    .await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(
        b2["signal_delivered"],
        json!(false),
        "dedup must report signal not delivered: {b2}"
    );
    assert_eq!(b2["execution_id"], b1["execution_id"]);
}

// ── Standalone signal route idempotency ───────────────────────────────────

/// POST a standalone signal, optionally carrying an `Idempotency-Key` header.
/// The body is the raw signal payload (free-form). A `?idempotency_key=` query
/// param can be embedded directly in `uri`.
async fn post_signal(
    app: &HarvestApiApp,
    uri: &str,
    header_key: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(k) = header_key {
        builder = builder.header("idempotency-key", k);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("POST signal request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response is JSON")
    };
    (status, json)
}

/// Start a fresh RUNNING workflow and return its execution id.
async fn start_running_workflow(app: &HarvestApiApp, workflow_id: &str) -> String {
    let (status, body) = post_json(
        app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": workflow_id,
            "start_input": {},
            "signal_name": "bootstrap",
            "signal_payload": {}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "setup start failed: {body}");
    body["execution_id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn standalone_signal_same_header_key_dedupes() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec = start_running_workflow(&app, "idem-hdr-1").await;
    let uri = format!("/workflows/{exec}/signal/approval");

    let (s1, b1) = post_signal(&app, &uri, Some("k1"), json!({"a": 1})).await;
    assert_eq!(s1, StatusCode::ACCEPTED, "{b1}");
    assert_eq!(b1["ok"], json!(true));
    assert_eq!(b1["signal_delivered"], json!(true));

    // Same key → deduped.
    let (s2, b2) = post_signal(&app, &uri, Some("k1"), json!({"a": 1})).await;
    assert_eq!(s2, StatusCode::ACCEPTED, "{b2}");
    assert_eq!(
        b2["signal_delivered"],
        json!(false),
        "second delivery with same header key must dedupe: {b2}"
    );
}

#[tokio::test]
async fn standalone_signal_query_param_dedupes() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec = start_running_workflow(&app, "idem-qry-1").await;
    let uri = format!("/workflows/{exec}/signal/approval?idempotency_key=q1");

    let (_, b1) = post_signal(&app, &uri, None, json!({})).await;
    assert_eq!(b1["signal_delivered"], json!(true));

    let (_, b2) = post_signal(&app, &uri, None, json!({})).await;
    assert_eq!(
        b2["signal_delivered"],
        json!(false),
        "second delivery with same query key must dedupe: {b2}"
    );
}

#[tokio::test]
async fn standalone_signal_header_wins_over_query() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec = start_running_workflow(&app, "idem-prec-1").await;

    // First delivery: header "hk" wins over query "qk" → effective key is "hk".
    let uri_hk = format!("/workflows/{exec}/signal/approval?idempotency_key=qk");
    let (_, b1) = post_signal(&app, &uri_hk, Some("hk"), json!({})).await;
    assert_eq!(b1["signal_delivered"], json!(true));

    // A later call whose *query* key equals the header value "hk" (no header)
    // collides with the first delivery — proving the header value was the
    // effective key, not the query value "qk".
    let uri_proves = format!("/workflows/{exec}/signal/approval?idempotency_key=hk");
    let (_, b2) = post_signal(&app, &uri_proves, None, json!({})).await;
    assert_eq!(
        b2["signal_delivered"],
        json!(false),
        "header value 'hk' must have been the effective key: {b2}"
    );
}

#[tokio::test]
async fn standalone_signal_without_key_is_at_least_once() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec = start_running_workflow(&app, "idem-none-1").await;
    let uri = format!("/workflows/{exec}/signal/approval");

    let (_, b1) = post_signal(&app, &uri, None, json!({})).await;
    let (_, b2) = post_signal(&app, &uri, None, json!({})).await;
    assert_eq!(b1["signal_delivered"], json!(true));
    assert_eq!(
        b2["signal_delivered"],
        json!(true),
        "unkeyed deliveries are at-least-once: both report delivered: {b2}"
    );
}

#[tokio::test]
async fn standalone_signal_empty_header_key_is_rejected() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec = start_running_workflow(&app, "idem-empty-1").await;
    let uri = format!("/workflows/{exec}/signal/approval");

    // A present but empty Idempotency-Key header is rejected with 400 rather
    // than silently degraded to at-least-once.
    let (status, _) = post_signal(&app, &uri, Some(""), json!({})).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "empty Idempotency-Key header must be rejected with 400"
    );
}
