#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
//! HTTP integration test for workflow-start provenance attribution (issue #740).
//!
//! Exercises the plugin `POST /workflows/{name}/start` route end-to-end and
//! asserts the created execution records `start_source = "api"`. The webhook
//! provenance path (`webhook`) is asserted in `webhook_receiver_integration.rs`
//! (`webhook_start_records_webhook_source`), which already carries the full
//! `[security.webhooks]` + HMAC-signed-delivery harness.
//!
//! Execution: honours `HARVEST_TEST_DATABASE_URL` (a migrated Postgres) so it
//! runs directly against a local instance; otherwise a fresh testcontainers
//! Postgres is booted with the full migration bundle.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowInfo, context::WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::sql_types::Text;
use diesel_async::RunQueryDsl;
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

/// Honour `HARVEST_TEST_DATABASE_URL` for a local run; else boot testcontainers.
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn plain_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "tests",
        handler: dummy_workflow,
        execution_timeout: None,
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
    }
}

fn build_app(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("start-source-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn post_start(app: &HarvestApiApp, wf: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                // `harvest_api_router` mounts routes at the root (`/workflows/...`).
                .uri(format!("/workflows/{wf}/start"))
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let jsonv = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, jsonv)
}

#[derive(diesel::QueryableByName)]
struct SourceRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    start_source: Option<String>,
}

/// AC3 (issue #740): a plain HTTP `POST /workflows/{name}/start` records
/// `start_source = "api"`.
#[tokio::test]
async fn http_start_records_api_source() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("ss_api_wf")]);

    // Isolate this test's row on a unique workflow_id.
    let wf_id = format!("ss-api-{}", uuid::Uuid::new_v4());
    let (status, body) = post_start(
        &app,
        "ss_api_wf",
        json!({ "workflow_id": wf_id, "input": {"k": "v"} }),
    )
    .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "expected 201/200, got {status}: {body}"
    );
    let exec_id = body["execution_id"]
        .as_str()
        .expect("execution_id in start response")
        .to_string();

    let mut conn = pool.get().await.expect("pool conn");
    let row: SourceRow = diesel::sql_query(
        "SELECT start_source FROM harvest_workflow_executions WHERE id = $1::uuid",
    )
    .bind::<Text, _>(&exec_id)
    .get_result(&mut conn)
    .await
    .expect("load created execution");

    assert_eq!(
        row.start_source.as_deref(),
        Some("api"),
        "an HTTP start must record start_source='api'"
    );
}
