use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::retention::RetentionConfig;
use autumn_harvest::scheduler::DagCatalog;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::workers::{WorkerStatus, register_worker};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn setup_database_url_with_migrations() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    (url, container)
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        name: "test_workflow",
        module: "tests",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        concurrency: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
    }
}

fn runtime_for(queues: &[&str], router: ShardRouter) -> HarvestApiRuntime {
    HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![workflow_info()], Vec::new())),
        Arc::new(DagCatalog::new()),
        Arc::new(Vec::new()),
        None,
        queues.iter().map(|queue| (*queue).to_string()).collect(),
        autumn_harvest::scheduler::SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        router,
    )
}

fn api_state(pool: HarvestDbPool, runtime: HarvestApiRuntime) -> HarvestApiState {
    let state = HarvestApiState::new();
    state.install_storage_pool(pool);
    state.install(runtime);
    state.set_worker_stale_threshold(Duration::from_secs(10));
    state
}

async fn register_active_worker_with_build(
    pool: &DbPool,
    worker_id: &str,
    queues: &[&str],
    shards: &[i32],
    build_id: &str,
    max_concurrency: i32,
    in_flight_count: i32,
) {
    let queue_names = queues
        .iter()
        .map(|queue| (*queue).to_string())
        .collect::<Vec<_>>();
    let mut conn = pool.get().await.expect("worker registration connection");
    register_worker(
        &mut conn,
        worker_id,
        &queue_names,
        shards,
        max_concurrency,
        "localhost",
        Some(env!("CARGO_PKG_VERSION")),
        build_id,
        Some("test-deploy"),
    )
    .await
    .expect("worker registration should succeed");

    // Force updates to status, in_flight_count since standard registration defaults them
    diesel::sql_query(
        "UPDATE harvest_workers
         SET in_flight_count = $1
         WHERE worker_id = $2",
    )
    .bind::<diesel::sql_types::Integer, _>(in_flight_count)
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(&mut conn)
    .await
    .expect("failed to set in_flight_count");
}

async fn mark_worker_stale(pool: &DbPool, worker_id: &str) {
    let mut conn = pool.get().await.expect("stale update connection");
    diesel::sql_query(
        "UPDATE harvest_workers
         SET last_heartbeat_at = NOW() - INTERVAL '10 minutes'
         WHERE worker_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(&mut conn)
    .await
    .expect("worker should be marked stale");
}

async fn mark_worker_draining(pool: &DbPool, worker_id: &str) {
    let mut conn = pool.get().await.expect("draining update connection");
    diesel::sql_query(
        "UPDATE harvest_workers
         SET status = $1
         WHERE worker_id = $2",
    )
    .bind::<diesel::sql_types::Text, _>(WorkerStatus::Draining.as_str())
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(&mut conn)
    .await
    .expect("worker should be marked draining");
}

async fn mark_worker_stopped(pool: &DbPool, worker_id: &str) {
    let mut conn = pool.get().await.expect("stopped update connection");
    diesel::sql_query(
        "UPDATE harvest_workers
         SET status = $1
         WHERE worker_id = $2",
    )
    .bind::<diesel::sql_types::Text, _>(WorkerStatus::Stopped.as_str())
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(&mut conn)
    .await
    .expect("worker should be marked stopped");
}

async fn seed_task_detailed(
    pool: &DbPool,
    queue_name: &str,
    required_build_id: Option<&str>,
    sticky_worker_id: Option<&str>,
    sticky_until_interval: Option<&str>,
    concurrency_key: Option<&str>,
    concurrency_cap: Option<i32>,
) -> uuid::Uuid {
    let mut conn = pool.get().await.expect("task seeding connection");
    let id = uuid::Uuid::new_v4();

    let required_build_clause = required_build_id
        .map(|v| format!("'{}'", v))
        .unwrap_or_else(|| "NULL".to_string());
    let sticky_worker_clause = sticky_worker_id
        .map(|v| format!("'{}'", v))
        .unwrap_or_else(|| "NULL".to_string());
    let sticky_until_clause = sticky_until_interval
        .map(|v| format!("NOW() + INTERVAL '{}'", v))
        .unwrap_or_else(|| "NULL".to_string());
    let concurrency_key_clause = concurrency_key
        .map(|v| format!("'{}'", v))
        .unwrap_or_else(|| "NULL".to_string());
    let concurrency_cap_clause = concurrency_cap
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NULL".to_string());

    diesel::sql_query(format!(
        "INSERT INTO harvest_task_queue (
            id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at,
            required_build_id, sticky_worker_id, sticky_until, concurrency_key, concurrency_cap
         ) VALUES (
            '{}', '{}', 'workflow', '{{}}'::jsonb, 'PENDING', 0, 1, NOW() - INTERVAL '5 seconds',
            {}, {}, {}, {}, {}
         )",
        id,
        queue_name,
        required_build_clause,
        sticky_worker_clause,
        sticky_until_clause,
        concurrency_key_clause,
        concurrency_cap_clause
    ))
    .execute(&mut conn)
    .await
    .expect("failed to insert test task");

    id
}

async fn read_response_body(response: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body")
        .to_vec()
}

async fn get_json_with_auth(
    app: &HarvestApiApp,
    uri: impl Into<String>,
    has_admin: bool,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("GET")
        .uri(uri.into())
        .body(Body::empty())
        .expect("request should build");

    if has_admin {
        // harvest admin bypasses checking session if admin_auth_boundary is true in State.
        // We set admin_auth_boundary=true in HarvestApiState to allow admin routes to execute
        // without a session in tests, similar to existing security.rs tests.
    }

    let response = app
        .clone()
        .oneshot(req)
        .await
        .expect("request should complete");
    let status = response.status();
    let body_bytes = read_response_body(response).await;
    let json = serde_json::from_slice(&body_bytes).expect("response must be JSON");
    (status, json)
}

#[tokio::test]
async fn test_queue_and_task_eligibility_scenarios() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    // 1. Seed tasks representing different failure reasons on "test-queue"
    // Task A: requires build v2
    seed_task_detailed(&pool, "test-queue", Some("v2"), None, None, None, None).await;

    // Task B: sticky to worker-sticky
    seed_task_detailed(
        &pool,
        "test-queue",
        None,
        Some("worker-sticky"),
        Some("5 minutes"),
        None,
        None,
    )
    .await;

    // Task C: concurrency key "tenant-1" with cap 1. We'll also seed a RUNNING task with key "tenant-1" to saturate it.
    seed_task_detailed(
        &pool,
        "test-queue",
        None,
        None,
        None,
        Some("tenant-1"),
        Some(1),
    )
    .await;
    let mut conn = pool.get().await.expect("running task connection");
    diesel::sql_query(
        "INSERT INTO harvest_task_queue (
            id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at,
            concurrency_key, concurrency_cap, worker_id
         ) VALUES (
            gen_random_uuid(), 'test-queue', 'workflow', '{}'::jsonb, 'RUNNING', 0, 1, NOW(),
            'tenant-1', 1, 'worker-concurrency'
         )",
    )
    .execute(&mut conn)
    .await
    .expect("failed to insert running task to saturate concurrency");

    // 2. Register workers
    // worker-eligible: online, active, build v2, subscribed to "test-queue", shard 0
    register_active_worker_with_build(&pool, "worker-eligible", &["test-queue"], &[0], "v2", 10, 0)
        .await;

    // worker-incompatible-build: online, active, build v1, subscribed to "test-queue", shard 0
    register_active_worker_with_build(
        &pool,
        "worker-incompatible-build",
        &["test-queue"],
        &[0],
        "v1",
        10,
        0,
    )
    .await;

    // worker-wrong-queue: online, active, build v2, subscribed to "other-queue", shard 0
    register_active_worker_with_build(
        &pool,
        "worker-wrong-queue",
        &["other-queue"],
        &[0],
        "v2",
        10,
        0,
    )
    .await;

    // worker-wrong-shard: online, active, build v2, subscribed to "test-queue", shard 1 (task is on shard 0)
    register_active_worker_with_build(
        &pool,
        "worker-wrong-shard",
        &["test-queue"],
        &[1],
        "v2",
        10,
        0,
    )
    .await;

    // worker-draining: online, draining, build v2, subscribed to "test-queue", shard 0
    register_active_worker_with_build(&pool, "worker-draining", &["test-queue"], &[0], "v2", 10, 0)
        .await;
    mark_worker_draining(&pool, "worker-draining").await;

    // worker-stopped: online, stopped, build v2, subscribed to "test-queue", shard 0
    register_active_worker_with_build(&pool, "worker-stopped", &["test-queue"], &[0], "v2", 10, 0)
        .await;
    mark_worker_stopped(&pool, "worker-stopped").await;

    // worker-stale: stale/offline worker, should be excluded
    register_active_worker_with_build(&pool, "worker-stale", &["test-queue"], &[0], "v2", 10, 0)
        .await;
    mark_worker_stale(&pool, "worker-stale").await;

    // worker-full-capacity: online, active, but at max concurrency
    register_active_worker_with_build(
        &pool,
        "worker-full-capacity",
        &["test-queue"],
        &[0],
        "v2",
        2,
        2,
    )
    .await;

    // Set up API router
    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue"], ShardRouter::single()),
    );
    // bypass admin auth check in tests
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    // --- Validate Queue Eligibility Endpoint ---
    let (status, body) =
        get_json_with_auth(&app, "/admin/queues/test-queue/eligibility", true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["queue_name"], "test-queue");
    assert_eq!(body["pending_count"], 3); // A, B, C are pending
    assert!(body["oldest_pending_age_secs"].as_i64().is_some());

    // Distinct build IDs across pending rows: "v2" and null (null should be absent or represented, required_build_ids is a list of strings)
    let req_builds: Vec<String> = body["required_build_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(req_builds.contains(&"v2".to_string()));

    // Eligible workers check: worker-eligible passes all gates for at least one task (specifically Task A and C, wait, Task C has concurrency key saturated, but Task A doesn't).
    let eligible_list = body["eligible_workers"].as_array().unwrap();
    let eligible_ids: Vec<&str> = eligible_list
        .iter()
        .map(|w| w["worker_id"].as_str().unwrap())
        .collect();
    assert!(eligible_ids.contains(&"worker-eligible"));
    // worker-stale is offline, should not be in either list
    assert!(!eligible_ids.contains(&"worker-stale"));

    // Ineligible workers check
    let ineligible_list = body["ineligible_workers"].as_array().unwrap();
    let ineligible_ids: Vec<&str> = ineligible_list
        .iter()
        .map(|w| w["worker_id"].as_str().unwrap())
        .collect();
    assert!(!ineligible_ids.contains(&"worker-stale"));

    // Check wrong_queue_subscription
    let w_wrong_q = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-wrong-queue")
        .unwrap();
    let reasons: Vec<&str> = w_wrong_q["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(reasons.contains(&"wrong_queue_subscription"));

    // Check wrong_shard_assignment
    let w_wrong_s = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-wrong-shard")
        .unwrap();
    let reasons: Vec<&str> = w_wrong_s["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(reasons.contains(&"wrong_shard_assignment"));

    // Check worker_draining
    let w_drain = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-draining")
        .unwrap();
    let reasons: Vec<&str> = w_drain["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(reasons.contains(&"worker_draining"));

    // Check worker_stopped
    let w_stop = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-stopped")
        .unwrap();
    let reasons: Vec<&str> = w_stop["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(reasons.contains(&"worker_stopped"));

    // Check diagnosis is "healthy" since we have worker-eligible which is active, eligible, and has capacity
    assert_eq!(body["summary"]["diagnosis"], "healthy");
}

#[tokio::test]
async fn test_task_eligibility_endpoints() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    // Seed task requiring build v2
    let task_id = seed_task_detailed(&pool, "test-queue", Some("v2"), None, None, None, None).await;

    // Register active worker with incompatible build
    register_active_worker_with_build(
        &pool,
        "worker-incompatible",
        &["test-queue"],
        &[0],
        "v1",
        10,
        0,
    )
    .await;

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue"], ShardRouter::single()),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) =
        get_json_with_auth(&app, format!("/admin/tasks/{}/eligibility", task_id), true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["task_id"], task_id.to_string());
    assert_eq!(body["queue_name"], "test-queue");
    assert_eq!(body["required_build_id"], "v2");
    assert_eq!(body["assigned_shard"], 0);

    let ineligible_list = body["ineligible_workers"].as_array().unwrap();
    let w_incompat = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-incompatible")
        .unwrap();
    let reasons: Vec<&str> = w_incompat["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(reasons.contains(&"build_incompatible"));

    // Since only worker-incompatible is online and it is incompatible, the diagnosis should be "no_eligible_workers"
    assert_eq!(body["summary"]["diagnosis"], "no_eligible_workers");
}
