//! Integration tests for the batch operations API (issue #102).
//!
//! Covers the durable wiring: submit → list → get → executor drains. Uses a
//! small synthetic fleet (50 workflows) instead of the issue's 1k target so
//! the test stays under a CI-tractable runtime; the codepath under test is
//! identical at any cardinality.

#![allow(clippy::similar_names, clippy::redundant_clone)]

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::batch::{BatchExecutorConfig, run_executor_once};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
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
    include_str!("../../autumn-harvest/migrations/20260501010000_harvest_batch_jobs/up.sql"),
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

fn test_app_state() -> AppState {
    AppState::for_test().with_profile("test")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("batch-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(test_app_state())
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

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request");
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

async fn seed_workflows(database_url: &str, workflow_name: &str, count: usize) -> Vec<ExecutionId> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for seed");
    let mut ids = Vec::with_capacity(count);
    for index in 0..count {
        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        start_or_load_workflow_execution(
            &mut conn,
            StartWorkflowParams {
                workflow_name,
                workflow_id: &format!("wf-{index}"),
                exec_id,
                input: json!({"i": index}),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                memo: None,
                search_attrs: Some(json!({"tenant": "acme"})),
                reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            },
        )
        .await
        .expect("seed workflow");
        ids.push(exec_id);
    }
    ids
}

#[tokio::test]
async fn batch_cancel_drains_filtered_workflows() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    seed_workflows(&url, "onboarding", 50).await;

    // Submit a batch-cancel job filtered to onboarding workflows.
    let (status, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding", "states": ["RUNNING"] }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "submit response: {body}");
    let job_id = body["batch_job_id"]
        .as_str()
        .expect("batch_job_id present")
        .to_string();

    // Initially Pending, no progress yet.
    let (status, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "Pending");
    assert_eq!(body["completed"], 0);
    assert_eq!(body["failed"], 0);

    // Drive the executor synchronously.
    let sharded_pool = HarvestDbPool::from(pool.clone());
    run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default())
        .await
        .expect("executor tick succeeds");

    // Job has reached terminal Completed and counters reflect the cancelled fleet.
    let (status, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "Completed", "after executor tick: {body}");
    assert_eq!(body["total"], 50);
    assert_eq!(body["completed"], 50);
    assert_eq!(body["failed"], 0);

    // Every workflow in the fleet is now CANCELLED.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .unwrap();
    let states: Vec<String> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("onboarding"))
        .select(harvest_workflow_executions::state)
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(states.len(), 50);
    assert!(
        states.iter().all(|s| s == "CANCELLED"),
        "expected all CANCELLED, got {states:?}"
    );
}

#[tokio::test]
async fn batch_submission_is_idempotent_under_retry() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let payload = json!({
        "action": "Cancel",
        "filter": { "workflow_name": "billing" },
        "idempotency_key": "incident-2026-04-28-billing-rollback"
    });

    let (s1, b1) = post_json(&app, "/batch-operations", payload.clone()).await;
    let (s2, b2) = post_json(&app, "/batch-operations", payload).await;

    assert_eq!(s1, StatusCode::ACCEPTED);
    assert_eq!(s2, StatusCode::ACCEPTED);
    assert_eq!(
        b1["batch_job_id"], b2["batch_job_id"],
        "duplicate submission must return same job id"
    );
}

#[tokio::test]
async fn batch_signal_action_requires_signal_name() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Signal",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("signal_name"),
        "expected signal_name validation error, got {body}"
    );
}

#[tokio::test]
async fn batch_filter_must_have_at_least_one_criterion() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(
        &app,
        "/batch-operations",
        json!({ "action": "Cancel", "filter": {} }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("filter"),
        "expected filter validation error, got {body}"
    );
}

#[tokio::test]
async fn batch_list_filters_by_status_and_action() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Two distinct submissions so list has something to slice.
    let (_, _) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding" },
            "idempotency_key": "k-cancel"
        }),
    )
    .await;
    let (_, _) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Signal",
            "filter": { "workflow_name": "onboarding" },
            "signal_name": "rollback",
            "idempotency_key": "k-signal"
        }),
    )
    .await;

    let (status, body) = get_json(&app, "/batch-operations").await;
    assert_eq!(status, StatusCode::OK);
    let arr = body.as_array().expect("list response is array");
    assert_eq!(arr.len(), 2);

    let (_, body) = get_json(&app, "/batch-operations?action=Signal").await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["action"], "Signal");

    let (_, body) = get_json(&app, "/batch-operations?status=Pending").await;
    assert_eq!(body.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn batch_per_target_failures_dont_abort_run() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let ids = seed_workflows(&url, "onboarding", 4).await;
    // Pre-cancel one workflow so the cancel path returns idempotent on it.
    // Then mark another COMPLETED so the cancel path *errors* on it. The
    // batch should still drain the remaining two and report failed=1.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .unwrap();
    diesel::update(harvest_workflow_executions::table.find(ids[1].as_uuid()))
        .set(harvest_workflow_executions::state.eq("COMPLETED"))
        .execute(&mut conn)
        .await
        .unwrap();

    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    let job_id = body["batch_job_id"].as_str().unwrap().to_string();

    let sharded_pool = HarvestDbPool::from(pool);
    run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default())
        .await
        .unwrap();

    let (_, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(body["status"], "Completed");
    assert_eq!(body["total"], 4);
    assert_eq!(
        body["failed"], 1,
        "completed workflow must surface as failed: {body}"
    );
    assert_eq!(body["completed"], 3);
    let errors = body["errors"].as_array().expect("errors array");
    assert_eq!(errors.len(), 1);
}

// 60-second guard: in CI, the executor must drain 50 workflows well within
// this. If the rate-limiter regresses it'll be the first thing to time out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_drains_within_time_budget() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    seed_workflows(&url, "onboarding", 50).await;
    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    let job_id = body["batch_job_id"].as_str().unwrap().to_string();

    let sharded_pool = HarvestDbPool::from(pool);
    let started = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(60),
        run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default()),
    )
    .await
    .expect("executor must finish under 60s")
    .expect("executor result");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "50-workflow batch should drain in under 30s, took {elapsed:?}"
    );

    let (_, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(body["status"], "Completed");
}
