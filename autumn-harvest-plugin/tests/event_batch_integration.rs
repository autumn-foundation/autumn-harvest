#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    WorkflowInfo,
    context::WorkflowContext,
    event_batch::{BatchPolicy, fire_due_event_batches},
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260616000001_harvest_workflow_schedule_id/up.sql"
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
    include_str!("../../autumn-harvest/migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
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
    include_str!(
        "../../autumn-harvest/migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260613000000_harvest_workflow_sla/up.sql"),
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
    "\n",
    include_str!("../../autumn-harvest/migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000001_harvest_poison_pill_strikes/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000002_harvest_ownership_metadata/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260603000000_harvest_completion_triggers/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260607000001_harvest_task_required_capabilities/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260609000001_harvest_workflow_current_details/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260613000001_harvest_schedule_catchup_window/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260624000000_harvest_event_batches/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../../autumn-harvest/migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../../autumn-harvest/migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../autumn-harvest/migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260705000000_harvest_completion_deliveries/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
);

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(
        vec![
            WorkflowInfo {
                mcp: false,
                name: "test_batch_request",
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
            },
            WorkflowInfo {
                mcp: false,
                name: "test_batch_macro",
                module: "tests",
                handler: dummy_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                debounce: None,
                batch: Some(BatchPolicy {
                    key_expr: "tenant".to_string(),
                    max_size: 2,
                    max_wait: Duration::from_secs(60),
                }),
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
            },
            WorkflowInfo {
                mcp: false,
                name: "test_batch_and_debounce",
                module: "tests",
                handler: dummy_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                debounce: Some(autumn_harvest::debounce::DebouncePolicy {
                    key_expr: "tenant",
                    window: Duration::from_secs(60),
                    max_wait: None,
                }),
                batch: Some(BatchPolicy {
                    key_expr: "tenant".to_string(),
                    max_size: 2,
                    max_wait: Duration::from_secs(60),
                }),
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
            },
        ],
        vec![],
    );

    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("event-batch-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

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
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

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
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

#[tokio::test]
async fn test_request_specified_batch_size_flush() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // First start request -> should accumulate and return 202 Accepted
    let (status1, body1) = post_json(
        &app,
        "/workflows/test_batch_request/start",
        json!({
            "workflow_id": "wf-req-1",
            "batch_key": "my-key-1",
            "batch_max_size": 3,
            "batch_max_wait": "10s",
            "input": { "foo": "bar1" }
        }),
    )
    .await;
    assert_eq!(status1, StatusCode::ACCEPTED, "body: {body1:?}");
    assert_eq!(body1["batched"], true);
    assert_eq!(body1["flushed"], false);
    assert_eq!(body1["pending_count"], 1);
    assert_eq!(body1["max_size"], 3);

    // Second start request -> accumulates and returns 202 Accepted
    let (status2, body2) = post_json(
        &app,
        "/workflows/test_batch_request/start",
        json!({
            "workflow_id": "wf-req-1",
            "batch_key": "my-key-1",
            "batch_max_size": 3,
            "batch_max_wait": "10s",
            "input": { "foo": "bar2" }
        }),
    )
    .await;
    assert_eq!(status2, StatusCode::ACCEPTED, "body: {body2:?}");
    assert_eq!(body2["pending_count"], 2);

    // Third start request -> reaches max_size (3), flushes immediately and returns 201 Created
    let (status3, body3) = post_json(
        &app,
        "/workflows/test_batch_request/start",
        json!({
            "workflow_id": "wf-req-1",
            "batch_key": "my-key-1",
            "batch_max_size": 3,
            "batch_max_wait": "10s",
            "input": { "foo": "bar3" }
        }),
    )
    .await;
    assert_eq!(status3, StatusCode::CREATED, "body: {body3:?}");
    assert_eq!(body3["batched"], true);
    assert_eq!(body3["flushed"], true);
    let exec_id = body3["execution_id"].as_str().unwrap();

    // Verify workflow execution row is created and input is a JSON array of all 3 payloads in arrival order
    let input_val: Value = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(uuid::Uuid::parse_str(exec_id).unwrap()))
        .select(harvest_workflow_executions::input)
        .first(&mut conn)
        .await
        .unwrap();

    assert_eq!(
        input_val,
        json!([
            { "foo": "bar1" },
            { "foo": "bar2" },
            { "foo": "bar3" }
        ])
    );
}

#[tokio::test]
async fn test_macro_configured_batch_time_flush() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // Macro policy uses key_expr "tenant".
    // Start first request -> returns 202 Accepted.
    let (status1, body1) = post_json(
        &app,
        "/workflows/test_batch_macro/start",
        json!({
            "workflow_id": "wf-macro-1",
            "input": { "tenant": "acme-corp", "data": "payload-A" }
        }),
    )
    .await;
    assert_eq!(status1, StatusCode::ACCEPTED, "body: {body1:?}");
    assert_eq!(body1["batch_key"], "acme-corp");
    assert_eq!(body1["pending_count"], 1);

    // Call GET /batches/pending visibility endpoint
    let (p_status, p_body) = get_json(&app, "/batches/pending").await;
    assert_eq!(p_status, StatusCode::OK, "body: {p_body:?}");
    assert!(p_body.is_array());
    let list = p_body.as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["workflow_name"], "test_batch_macro");
    assert_eq!(list[0]["batch_key"], "acme-corp");
    assert_eq!(list[0]["current_size"], 1);
    assert_eq!(list[0]["max_size"], 2);

    // Now, force-expire the batch by updating its `fire_at` to the past
    let rows_updated =
        diesel::sql_query("UPDATE harvest_event_batches SET fire_at = NOW() - INTERVAL '1 minute'")
            .execute(&mut conn)
            .await
            .unwrap();
    assert_eq!(rows_updated, 1);

    // Trigger fire_due_event_batches background sweep
    let fired = fire_due_event_batches(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .unwrap();
    assert_eq!(fired, 1);

    // Verify list is now empty
    let (p_status2, p_body2) = get_json(&app, "/batches/pending").await;
    assert_eq!(p_status2, StatusCode::OK);
    assert_eq!(p_body2.as_array().unwrap().len(), 0);

    // Verify workflow execution exists with single-item payload array
    let exec_row: (uuid::Uuid, Value) = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("test_batch_macro"))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::input,
        ))
        .first(&mut conn)
        .await
        .unwrap();

    assert_eq!(
        exec_row.1,
        json!([
            { "tenant": "acme-corp", "data": "payload-A" }
        ])
    );
}

#[tokio::test]
async fn test_batch_and_debounce_mutually_exclusive() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Start request triggers both debounce and batch -> should return 400 Bad Request
    let (status, body) = post_json(
        &app,
        "/workflows/test_batch_and_debounce/start",
        json!({
            "workflow_id": "wf-both-1",
            "input": { "tenant": "acme-corp" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["error"],
        "debounce and batching are mutually exclusive"
    );
}
