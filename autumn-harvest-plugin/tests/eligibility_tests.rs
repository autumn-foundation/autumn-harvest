#![allow(clippy::too_many_lines)]

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

    let required_build_clause =
        required_build_id.map_or_else(|| "NULL".to_string(), |v| format!("'{v}'"));
    let sticky_worker_clause =
        sticky_worker_id.map_or_else(|| "NULL".to_string(), |v| format!("'{v}'"));
    let sticky_until_clause = sticky_until_interval
        .map_or_else(|| "NULL".to_string(), |v| format!("NOW() + INTERVAL '{v}'"));
    let concurrency_key_clause =
        concurrency_key.map_or_else(|| "NULL".to_string(), |v| format!("'{v}'"));
    let concurrency_cap_clause =
        concurrency_cap.map_or_else(|| "NULL".to_string(), |v| v.to_string());

    diesel::sql_query(format!(
        "INSERT INTO harvest_task_queue (
            id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at,
            required_build_id, sticky_worker_id, sticky_until, concurrency_key, concurrency_cap
         ) VALUES (
            '{id}', '{queue_name}', 'workflow', '{{}}'::jsonb, 'PENDING', 0, 1, NOW() - INTERVAL '5 seconds',
            {required_build_clause}, {sticky_worker_clause}, {sticky_until_clause}, {concurrency_key_clause}, {concurrency_cap_clause}
         )"
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
    let req = Request::builder()
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
    // Distinct build IDs across pending rows: "v2" and null (null should be absent or represented, required_build_ids is a list of strings)
    assert!(
        body["required_build_ids"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap() == "v2")
    );

    // Eligible workers check: worker-eligible passes all gates for at least one task (specifically Task A and C, wait, Task C has concurrency key saturated, but Task A doesn't).
    let eligible_list = body["eligible_workers"].as_array().unwrap();
    let eligible_ids: Vec<&str> = eligible_list
        .iter()
        .map(|w| w["worker_id"].as_str().unwrap())
        .collect();
    assert!(eligible_ids.contains(&"worker-eligible"));
    let w_el = eligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-eligible")
        .unwrap();
    assert_eq!(w_el["in_flight_count"], 0);
    assert_eq!(w_el["max_concurrency"], 10);

    assert!(eligible_ids.contains(&"worker-full-capacity"));
    let w_full = eligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-full-capacity")
        .unwrap();
    assert_eq!(w_full["in_flight_count"], 2);
    assert_eq!(w_full["max_concurrency"], 2);

    // worker-stale is offline, should not be in either list
    assert!(!eligible_ids.contains(&"worker-stale"));

    // Ineligible workers check
    let ineligible_list = body["ineligible_workers"].as_array().unwrap();
    assert!(
        !ineligible_list
            .iter()
            .any(|w| w["worker_id"].as_str().unwrap() == "worker-stale")
    );

    // Check wrong_queue_subscription
    let w_wrong_q = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-wrong-queue")
        .unwrap();
    assert!(
        w_wrong_q["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap() == "wrong_queue_subscription")
    );

    // Check wrong_shard_assignment
    let w_wrong_s = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-wrong-shard")
        .unwrap();
    assert!(
        w_wrong_s["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap() == "wrong_shard_assignment")
    );

    // Check worker_draining
    let w_drain = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-draining")
        .unwrap();
    assert!(
        w_drain["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap() == "worker_draining")
    );

    // Check worker_stopped
    let w_stop = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-stopped")
        .unwrap();
    assert!(
        w_stop["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap() == "worker_stopped")
    );

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
        get_json_with_auth(&app, format!("/admin/tasks/{task_id}/eligibility"), true).await;
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
    assert!(
        w_incompat["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap() == "build_incompatible")
    );

    // Since only worker-incompatible is online and it is incompatible, the diagnosis should be "no_eligible_workers"
    assert_eq!(body["summary"]["diagnosis"], "no_eligible_workers");
}

async fn seed_rate_limit_bucket(
    pool: &DbPool,
    key: &str,
    refill_rate: f64,
    burst: f64,
    tokens: f64,
) {
    let mut conn = pool
        .get()
        .await
        .expect("rate limit bucket seeding connection");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at)
         VALUES ($1, $2, $3, $4, NOW(), NOW(), NOW())"
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Double, _>(refill_rate)
    .bind::<diesel::sql_types::Double, _>(burst)
    .bind::<diesel::sql_types::Double, _>(tokens)
    .execute(&mut conn)
    .await
    .expect("failed to insert rate limit bucket");
}

async fn seed_task_with_rate_limit_and_date(
    pool: &DbPool,
    queue_name: &str,
    rate_limit_key: Option<&str>,
    activity_name: Option<&str>,
    scheduled_at_clause: &str,
    schedule_to_close_at_clause: &str,
) -> uuid::Uuid {
    let mut conn = pool.get().await.expect("task seeding connection");
    let id = uuid::Uuid::new_v4();

    let rate_limit_key_clause =
        rate_limit_key.map_or_else(|| "NULL".to_string(), |v| format!("'{v}'"));
    let activity_name_clause =
        activity_name.map_or_else(|| "NULL".to_string(), |v| format!("'{v}'"));

    diesel::sql_query(format!(
        "INSERT INTO harvest_task_queue (
            id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at,
            rate_limit_key, activity_name, schedule_to_close_at
         ) VALUES (
            '{id}', '{queue_name}', 'workflow', '{{}}'::jsonb, 'PENDING', 0, 1, {scheduled_at_clause},
            {rate_limit_key_clause}, {activity_name_clause}, {schedule_to_close_at_clause}
         )"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to insert test task");

    id
}

fn build_two_shard_pool(shard0_pool: DbPool, shard1_pool: DbPool) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), shard0_pool);
    pools.insert(ShardId::new(1), shard1_pool);
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

#[tokio::test]
async fn test_eligibility_optimizations_and_resilience() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    // 1. Seed rate limit bucket with 0 tokens (saturated)
    seed_rate_limit_bucket(&pool, "my-rate-limit-key", 0.0, 1.0, 0.0).await;

    // Seed task with saturated rate limit key
    let _task_rl = seed_task_with_rate_limit_and_date(
        &pool,
        "test-queue-rl",
        Some("my-rate-limit-key"),
        Some("my-activity"),
        "NOW() - INTERVAL '5 seconds'",
        "NULL",
    )
    .await;

    // 2. Seed non-claimable task A: scheduled in the future (scheduled_at > NOW())
    seed_task_with_rate_limit_and_date(
        &pool,
        "test-queue-rl",
        None,
        None,
        "NOW() + INTERVAL '10 minutes'",
        "NULL",
    )
    .await;

    // Seed non-claimable task B: expired (schedule_to_close_at < NOW())
    seed_task_with_rate_limit_and_date(
        &pool,
        "test-queue-rl",
        None,
        None,
        "NOW() - INTERVAL '10 minutes'",
        "NOW() - INTERVAL '5 seconds'",
    )
    .await;

    // Register active worker for rate limit queue
    register_active_worker_with_build(
        &pool,
        "worker-rate-limited",
        &["test-queue-rl"],
        &[0],
        "v1",
        10,
        0,
    )
    .await;

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue-rl"], ShardRouter::single()),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    // Query queue eligibility
    let (status, body) =
        get_json_with_auth(&app, "/admin/queues/test-queue-rl/eligibility", true).await;
    assert_eq!(status, StatusCode::OK);
    // pending_count should be exactly 1 (only the rate-limited one is claimable; scheduled in future and expired are excluded)
    assert_eq!(body["pending_count"], 1);

    // worker-rate-limited should be marked ineligible with reason "rate_limit_saturated"
    let ineligible_list = body["ineligible_workers"].as_array().unwrap();
    let w_rl = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-rate-limited")
        .unwrap();
    assert!(
        w_rl["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap() == "rate_limit_saturated")
    );

    // 3. Test capacity limit: register a worker that has max capacity reached
    // Seed a new clean queue task
    let task_cap = seed_task_detailed(&pool, "test-queue-cap", None, None, None, None, None).await;
    register_active_worker_with_build(
        &pool,
        "worker-at-capacity",
        &["test-queue-cap"],
        &[0],
        "v1",
        5,
        5, // in_flight_count = max_concurrency
    )
    .await;

    let state_cap = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue-cap"], ShardRouter::single()),
    );
    state_cap.set_admin_auth_boundary(true);
    let app_cap =
        harvest_api_router(state_cap).with_state(AppState::for_test().with_profile("test"));

    let (status_cap, body_cap) =
        get_json_with_auth(&app_cap, "/admin/queues/test-queue-cap/eligibility", true).await;
    assert_eq!(status_cap, StatusCode::OK);
    // worker-at-capacity should be in eligible_workers, but because it's full, diagnosis should be "all_capacity_full"
    let eligible_cap = body_cap["eligible_workers"].as_array().unwrap();
    assert!(
        eligible_cap
            .iter()
            .any(|w| w["worker_id"] == "worker-at-capacity")
    );
    assert_eq!(body_cap["summary"]["diagnosis"], "all_capacity_full");

    // 4. Test multi-shard resilience (graceful skip of unhealthy shard)
    // Setup two shards: Shard 0 (healthy) and Shard 1 (broken/unhealthy)
    let shard0_pool = pool.clone();
    // Simulate broken connection by using a non-existent port
    let shard1_pool = build_test_pool("postgres://postgres:postgres@localhost:54321/nonexistent");

    let sharded_pool = build_two_shard_pool(shard0_pool, shard1_pool);
    let state_sharded = api_state(
        sharded_pool,
        runtime_for(&["test-queue-cap"], two_shard_router()),
    );
    state_sharded.set_admin_auth_boundary(true);
    let app_sharded =
        harvest_api_router(state_sharded).with_state(AppState::for_test().with_profile("test"));

    // GET /admin/tasks/{id}/eligibility for task_cap on shard 0 should succeed, skipping/ignoring the unhealthy shard 1
    let (status_task, body_task) = get_json_with_auth(
        &app_sharded,
        format!("/admin/tasks/{task_cap}/eligibility"),
        true,
    )
    .await;
    assert_eq!(status_task, StatusCode::OK);
    assert_eq!(body_task["task_id"], task_cap.to_string());
    assert_eq!(body_task["assigned_shard"], 0);

    // GET /admin/queues/test-queue-cap/eligibility should also succeed because at least one shard (shard 0) is healthy
    let (status_queue, body_queue) = get_json_with_auth(
        &app_sharded,
        "/admin/queues/test-queue-cap/eligibility",
        true,
    )
    .await;
    assert_eq!(status_queue, StatusCode::OK);
    assert_eq!(body_queue["queue_name"], "test-queue-cap");

    // 5. Test multi-shard diagnosis aggregation (prevent stuck shard hiding)
    // Clean up workers table to prevent interference
    {
        let mut conn = pool.get().await.expect("connection");
        diesel::sql_query("DELETE FROM harvest_workers")
            .execute(&mut conn)
            .await
            .expect("delete workers");
    }

    // Setup two healthy shards pointing to the same pool
    let sharded_pool_healthy = build_two_shard_pool(pool.clone(), pool.clone());
    let state_sharded_healthy = api_state(
        sharded_pool_healthy,
        runtime_for(&["test-queue-multi"], two_shard_router()),
    );
    state_sharded_healthy.set_admin_auth_boundary(true);
    let app_sharded_healthy = harvest_api_router(state_sharded_healthy)
        .with_state(AppState::for_test().with_profile("test"));

    // Seed task on "test-queue-multi"
    let _task_multi =
        seed_task_detailed(&pool, "test-queue-multi", None, None, None, None, None).await;

    // Register worker 0 assigned to shard 0, but stop it (so shard 0 has no eligible workers -> no_eligible_workers)
    register_active_worker_with_build(
        &pool,
        "worker-shard0",
        &["test-queue-multi"],
        &[0],
        "v1",
        5,
        0,
    )
    .await;
    mark_worker_stopped(&pool, "worker-shard0").await;

    // Register worker 1 assigned to shard 1, healthy (so shard 1 is healthy)
    register_active_worker_with_build(
        &pool,
        "worker-shard1",
        &["test-queue-multi"],
        &[1],
        "v1",
        5,
        0,
    )
    .await;

    // Query global queue eligibility. Since shard 0 is stuck (no_eligible_workers) and shard 1 is healthy,
    // the global diagnosis must bubble up the worst-case (no_eligible_workers), rather than "healthy".
    let (status_multi, body_multi) = get_json_with_auth(
        &app_sharded_healthy,
        "/admin/queues/test-queue-multi/eligibility",
        true,
    )
    .await;
    assert_eq!(status_multi, StatusCode::OK);
    assert_eq!(body_multi["summary"]["diagnosis"], "no_eligible_workers");

    // 6. Test all_draining classification refinement
    // Clean up workers table to prevent interference
    {
        let mut conn = pool.get().await.expect("connection");
        diesel::sql_query("DELETE FROM harvest_workers")
            .execute(&mut conn)
            .await
            .expect("delete workers");
    }

    let state_drain = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue-drain"], ShardRouter::single()),
    );
    state_drain.set_admin_auth_boundary(true);
    let app_drain =
        harvest_api_router(state_drain).with_state(AppState::for_test().with_profile("test"));

    // Seed task
    let _task_drain =
        seed_task_detailed(&pool, "test-queue-drain", None, None, None, None, None).await;

    // Register worker A: draining
    register_active_worker_with_build(
        &pool,
        "worker-drain-only",
        &["test-queue-drain"],
        &[0],
        "v1",
        5,
        0,
    )
    .await;
    mark_worker_draining(&pool, "worker-drain-only").await;

    // Query when only draining worker is present -> should be "all_draining"
    let (status_d1, body_d1) = get_json_with_auth(
        &app_drain,
        "/admin/queues/test-queue-drain/eligibility",
        true,
    )
    .await;
    assert_eq!(status_d1, StatusCode::OK);
    assert_eq!(body_d1["summary"]["diagnosis"], "all_draining");

    // Now register worker B: not draining, but has wrong queue subscription (so ineligible for other reasons)
    register_active_worker_with_build(
        &pool,
        "worker-wrong-sub",
        &["test-queue-wrong"], // subscribed to wrong queue
        &[0],
        "v1",
        5,
        0,
    )
    .await;

    // Query again -> since there is a worker ineligible for a non-drain reason, it should prefer "no_eligible_workers"
    let (status_d2, body_d2) = get_json_with_auth(
        &app_drain,
        "/admin/queues/test-queue-drain/eligibility",
        true,
    )
    .await;
    assert_eq!(status_d2, StatusCode::OK);
    assert_eq!(body_d2["summary"]["diagnosis"], "no_eligible_workers");

    // Now test service unavailable: if we only query a broken queue, or if all shards fail
    let all_broken_pool = build_two_shard_pool(
        build_test_pool("postgres://postgres:postgres@localhost:54321/nonexistent"),
        build_test_pool("postgres://postgres:postgres@localhost:54321/nonexistent"),
    );
    let state_all_broken = api_state(
        all_broken_pool,
        runtime_for(&["test-queue-cap"], two_shard_router()),
    );
    state_all_broken.set_admin_auth_boundary(true);
    let app_all_broken =
        harvest_api_router(state_all_broken).with_state(AppState::for_test().with_profile("test"));

    let req = Request::builder()
        .method("GET")
        .uri("/admin/queues/test-queue-cap/eligibility")
        .body(Body::empty())
        .unwrap();
    let res = app_all_broken.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}
