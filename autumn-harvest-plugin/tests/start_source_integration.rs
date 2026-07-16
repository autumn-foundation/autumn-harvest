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

/// GET a management-API path and return `(status, parsed_json_body)`.
async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
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

/// Directly insert a workflow-execution row with a chosen `start_source`
/// (`None` = a NULL / pre-upgrade column). Returns the new `id` (UUID text).
/// This exercises the *read* filter over the `start_source` column regardless
/// of which start path wrote it — the correctness of each write path is
/// covered by the `*_records_*_source` tests elsewhere.
async fn seed_execution(
    pool: &DbPool,
    workflow_name: &str,
    workflow_id: &str,
    start_source: Option<&str>,
    start_source_ref: Option<&str>,
    started_by: Option<&str>,
) -> String {
    use diesel::sql_types::Nullable;
    let id = uuid::Uuid::new_v4().to_string();
    let mut conn = pool.get().await.expect("pool conn");
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, start_source, start_source_ref, started_by) \
         VALUES ($1::uuid, $2, $3, 0, '{}'::jsonb, $4, $5, $6)",
    )
    .bind::<Text, _>(&id)
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Nullable<Text>, _>(start_source)
    .bind::<Nullable<Text>, _>(start_source_ref)
    .bind::<Nullable<Text>, _>(started_by)
    .execute(&mut conn)
    .await
    .expect("seed execution");
    id
}

/// Extract the `start_source` string of each row from a bare-array list
/// response (the non-paginated happy path returns a JSON array whose elements
/// flatten `WorkflowExecution`).
fn list_sources(body: &Value) -> Vec<String> {
    body.as_array()
        .expect("list response should be a bare array")
        .iter()
        .map(|row| {
            row["start_source"]
                .as_str()
                .expect("each row surfaces start_source")
                .to_string()
        })
        .collect()
}

/// AC5 (issue #740): `GET /workflows?start_source=` narrows the list to the
/// matching provenance; `start_source=unknown` matches NULL / pre-upgrade
/// rows; omitting the param returns every row (behavior preserved).
#[tokio::test]
async fn list_filter_by_start_source_narrows_results() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![]);

    // Unique workflow_name isolates this test's rows from any concurrent test.
    let wf = format!("ssfilter_{}", uuid::Uuid::new_v4().simple());
    seed_execution(
        &pool,
        &wf,
        &format!("{wf}-s1"),
        Some("schedule"),
        None,
        None,
    )
    .await;
    seed_execution(
        &pool,
        &wf,
        &format!("{wf}-s2"),
        Some("schedule"),
        None,
        None,
    )
    .await;
    seed_execution(&pool, &wf, &format!("{wf}-a1"), Some("api"), None, None).await;
    // A NULL / pre-upgrade row → matched only by start_source=unknown.
    seed_execution(&pool, &wf, &format!("{wf}-u1"), None, None, None).await;

    // start_source=schedule → only the two schedule rows.
    let (status, body) = get_json(
        &app,
        &format!("/workflows?workflow_name={wf}&start_source=schedule"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list should 200: {body}");
    let sources = list_sources(&body);
    assert_eq!(
        sources.len(),
        2,
        "expected 2 schedule rows, got {sources:?}"
    );
    assert!(
        sources.iter().all(|s| s == "schedule"),
        "every returned row must be schedule-sourced, got {sources:?}"
    );

    // start_source=unknown → only the NULL-source row (surfaced as "unknown").
    let (status, body) = get_json(
        &app,
        &format!("/workflows?workflow_name={wf}&start_source=unknown"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sources = list_sources(&body);
    assert_eq!(
        sources,
        vec!["unknown".to_string()],
        "only the NULL row matches unknown"
    );

    // start_source=api → only the one api row.
    let (status, body) = get_json(
        &app,
        &format!("/workflows?workflow_name={wf}&start_source=api"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sources = list_sources(&body);
    assert_eq!(
        sources,
        vec!["api".to_string()],
        "only the api row matches api"
    );

    // No start_source param → all four rows (behavior preserved).
    let (status, body) = get_json(&app, &format!("/workflows?workflow_name={wf}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        list_sources(&body).len(),
        4,
        "omitting the filter returns all rows"
    );
}

/// AC5 (issue #740): an unrecognized `start_source` value is a 400 JSON error,
/// never a 500 and never a silent empty 200.
#[tokio::test]
async fn list_filter_rejects_invalid_start_source_with_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![]);

    let (status, body) = get_json(&app, "/workflows?start_source=bogus").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an invalid start_source must be a 400, got {status}: {body}"
    );
    assert_ne!(status, StatusCode::INTERNAL_SERVER_ERROR, "must never 500");
    assert_ne!(
        status,
        StatusCode::OK,
        "must never silently return an empty 200"
    );
}

/// AC4 (issue #740): a describe response surfaces `start_source = "unknown"` for
/// a NULL / pre-upgrade row and omits `start_source_ref`/`started_by` when NULL;
/// a row with a concrete source + ref + actor surfaces all three.
#[tokio::test]
async fn describe_surfaces_start_source_and_omits_absent_fields() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![]);

    let wf = format!("ssdesc_{}", uuid::Uuid::new_v4().simple());

    // NULL-source (pre-upgrade) row → "unknown", ref/started_by keys ABSENT.
    let null_id = seed_execution(&pool, &wf, &format!("{wf}-null"), None, None, None).await;
    let (status, body) = get_json(&app, &format!("/workflows/{null_id}")).await;
    assert_eq!(status, StatusCode::OK, "describe should 200: {body}");
    let exec = &body["execution"];
    assert_eq!(
        exec["start_source"].as_str(),
        Some("unknown"),
        "a NULL start_source row must serialize as 'unknown'"
    );
    assert!(
        exec.get("start_source_ref").is_none(),
        "start_source_ref must be absent when NULL, got {:?}",
        exec.get("start_source_ref")
    );
    assert!(
        exec.get("started_by").is_none(),
        "started_by must be absent when NULL, got {:?}",
        exec.get("started_by")
    );

    // Concrete-source row with ref + actor → all three surface.
    let concrete_id = seed_execution(
        &pool,
        &wf,
        &format!("{wf}-sched"),
        Some("schedule"),
        Some("sched-123"),
        Some("operator@example.com"),
    )
    .await;
    let (status, body) = get_json(&app, &format!("/workflows/{concrete_id}")).await;
    assert_eq!(status, StatusCode::OK);
    let exec = &body["execution"];
    assert_eq!(exec["start_source"].as_str(), Some("schedule"));
    assert_eq!(exec["start_source_ref"].as_str(), Some("sched-123"));
    assert_eq!(exec["started_by"].as_str(), Some("operator@example.com"));

    // The list row for the same concrete execution surfaces the fields too.
    let (status, list) = get_json(
        &app,
        &format!("/workflows?workflow_name={wf}&start_source=schedule"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows = list.as_array().expect("bare array");
    let row = rows
        .iter()
        .find(|r| r["id"].as_str() == Some(concrete_id.as_str()))
        .expect("concrete row present in list");
    assert_eq!(row["start_source"].as_str(), Some("schedule"));
    assert_eq!(row["start_source_ref"].as_str(), Some("sched-123"));
    assert_eq!(row["started_by"].as_str(), Some("operator@example.com"));
}

/// POST a JSON body to an arbitrary management-API path (admin header set) and
/// return `(status, parsed_json_body)`.
async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
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

/// The full provenance triple of a created execution row.
#[derive(diesel::QueryableByName)]
struct ProvenanceRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    start_source: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    start_source_ref: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    started_by: Option<String>,
}

async fn load_provenance(pool: &DbPool, exec_id: &str) -> ProvenanceRow {
    let mut conn = pool.get().await.expect("pool conn");
    diesel::sql_query(
        "SELECT start_source, start_source_ref, started_by \
         FROM harvest_workflow_executions WHERE id = $1::uuid",
    )
    .bind::<Text, _>(exec_id)
    .get_result(&mut conn)
    .await
    .expect("load created execution provenance")
}

/// BLOCKER 1 (issue #740): the batch-start API's *immediate* (non-throttled)
/// start branch records `start_source = "batch"` and attributes the issuing
/// operator in `started_by` — NOT the placeholder `api`.
#[tokio::test]
async fn batch_start_immediate_records_batch_source() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("ss_batch_wf")]);

    let wf_id = format!("ss-batch-{}", uuid::Uuid::new_v4());
    let (status, body) = post_json(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": false,
            "items": [
                { "workflow_name": "ss_batch_wf", "workflow_id": wf_id, "input": {"k": "v"} }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "batch_start should be 200: {body}");
    let exec_id = body["results"][0]["execution_id"]
        .as_str()
        .unwrap_or_else(|| panic!("execution_id in batch result: {body}"));

    let row = load_provenance(&pool, exec_id).await;
    assert_eq!(
        row.start_source.as_deref(),
        Some("batch"),
        "an immediate (non-throttled) batch item must record start_source='batch'"
    );
    assert!(
        row.started_by.is_some(),
        "a batch item must attribute the issuing operator in started_by, got None"
    );
}

/// BLOCKER 2 (issue #740): the manual trigger-now schedule route's *immediate*
/// (non-throttled) start branch records `start_source = "schedule"`, references
/// the schedule id in `start_source_ref`, and attributes the operator in
/// `started_by` — NOT the placeholder `api`.
#[tokio::test]
async fn trigger_now_immediate_records_schedule_source() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("ss_trigger_wf")]);

    // Create a manual workflow schedule (no auto-fire; the offline monitor never
    // ticks it — we drive it explicitly via trigger-now).
    let (status, sched) = post_json(
        &app,
        "/admin/schedules/workflow",
        json!({ "workflow_name": "ss_trigger_wf", "schedule_expr": "manual" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create schedule should be 201: {sched}"
    );
    let schedule_id = sched["id"]
        .as_str()
        .unwrap_or_else(|| panic!("schedule id in create response: {sched}"))
        .to_string();

    // Trigger it now → the direct (non-throttled) start branch (BLOCKER 2).
    let (status, trig) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/trigger"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "trigger-now should be 200: {trig}");
    let exec_id = trig["execution_id"]
        .as_str()
        .unwrap_or_else(|| panic!("execution_id in trigger response: {trig}"));

    let row = load_provenance(&pool, exec_id).await;
    assert_eq!(
        row.start_source.as_deref(),
        Some("schedule"),
        "manual trigger-now must record start_source='schedule'"
    );
    assert_eq!(
        row.start_source_ref.as_deref(),
        Some(schedule_id.as_str()),
        "manual trigger-now must reference the schedule id in start_source_ref"
    );
    assert!(
        row.started_by.is_some(),
        "manual trigger-now must attribute the operator in started_by, got None"
    );
}
