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
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260430000000_harvest_workflow_schedules/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508000000_harvest_external_task_updated_at/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260501020000_harvest_batch_processed_ids/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000000_harvest_signal_idempotency/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260519000000_harvest_calendar_awareness/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260522000000_harvest_schedule_decisions/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260526000001_harvest_parent_close_policy/up.sql"
    ),
    "
",
    include_str!("../../autumn-harvest/migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
);

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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
            name: "onboarding",
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            concurrency: None,
            max_input_bytes: None,
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
