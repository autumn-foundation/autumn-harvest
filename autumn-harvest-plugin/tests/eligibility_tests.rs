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
use autumn_harvest::workers::{WorkerStatus, get_worker, heartbeat_worker, register_worker};
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
use testcontainers::ImageExt;
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
        .with_tag("16")
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
        mcp: false,
        name: "test_workflow",
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
        &std::collections::HashMap::new(),
        0,
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

/// Seed a claimable PENDING task with a caller-chosen `task_type` (a real
/// circuit-breaker task is `task_type='activity'`, not `'workflow'`), naming an
/// activity and optionally carrying a rate-limit key.
async fn seed_typed_task(
    pool: &DbPool,
    queue_name: &str,
    task_type: &str,
    activity_name: Option<&str>,
    rate_limit_key: Option<&str>,
    scheduled_at_clause: &str,
) -> uuid::Uuid {
    let mut conn = pool.get().await.expect("task seeding connection");
    let id = uuid::Uuid::new_v4();

    diesel::sql_query(format!(
        "INSERT INTO harvest_task_queue (
            id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at,
            rate_limit_key, activity_name
         ) VALUES (
            $1, $2, $3, '{{}}'::jsonb, 'PENDING', 0, 1, {scheduled_at_clause},
            $4, $5
         )"
    ))
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(queue_name)
    .bind::<diesel::sql_types::Text, _>(task_type)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(rate_limit_key)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(activity_name)
    .execute(&mut conn)
    .await
    .expect("failed to insert typed test task");

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

    // worker-rate-limited should be marked ineligible with reason "rate_limit_exhausted" (AC7a).
    // The worker is otherwise perfectly eligible (right queue, shard, build, capacity),
    // and the single claimable task's only impediment is the saturated rate-limit bucket
    // (no concurrency key/cap, no circuit breaker), so the reason set is EXACTLY
    // ["rate_limit_exhausted"] — asserting exact equality guards the success metric that a
    // rate limit is never mislabeled as concurrency saturation.
    let ineligible_list = body["ineligible_workers"].as_array().unwrap();
    let w_rl = ineligible_list
        .iter()
        .find(|w| w["worker_id"] == "worker-rate-limited")
        .unwrap();
    let w_rl_reasons: Vec<&str> = w_rl["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(
        w_rl_reasons,
        vec!["rate_limit_exhausted"],
        "expected exactly rate_limit_exhausted, got {w_rl_reasons:?}"
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
    assert!(
        body_multi["eligible_workers"]
            .as_array()
            .unwrap()
            .is_empty()
    );

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

    // 7. Test concurrency saturation tied to task-level cap
    // Clean up workers table
    {
        let mut conn = pool.get().await.expect("connection");
        diesel::sql_query("DELETE FROM harvest_workers")
            .execute(&mut conn)
            .await
            .expect("delete workers");
    }

    let state_mixed = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue-mixed"], ShardRouter::single()),
    );
    state_mixed.set_admin_auth_boundary(true);
    let app_mixed =
        harvest_api_router(state_mixed).with_state(AppState::for_test().with_profile("test"));

    // Seed running task with mixed-key to occupy 1 slot
    {
        let mut conn = pool.get().await.expect("running task connection");
        diesel::sql_query(
            "INSERT INTO harvest_task_queue (
                id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at,
                concurrency_key, concurrency_cap, worker_id
             ) VALUES (
                gen_random_uuid(), 'test-queue-mixed', 'workflow', '{}'::jsonb, 'RUNNING', 0, 1, NOW(),
                'mixed-key', 1, 'worker-mixed-cap'
             )",
        )
        .execute(&mut conn)
        .await
        .expect("failed to insert running task to saturate concurrency");
    }

    // Seed task A (cap = 1, saturated)
    let _task_a = seed_task_detailed(
        &pool,
        "test-queue-mixed",
        None,
        None,
        None,
        Some("mixed-key"),
        Some(1),
    )
    .await;

    // Seed task B (cap = 5, not saturated)
    let _task_b = seed_task_detailed(
        &pool,
        "test-queue-mixed",
        None,
        None,
        None,
        Some("mixed-key"),
        Some(5),
    )
    .await;

    // Register active worker
    register_active_worker_with_build(
        &pool,
        "worker-mixed-cap",
        &["test-queue-mixed"],
        &[0],
        "v1",
        5,
        0,
    )
    .await;

    // Query queue eligibility. Since the worker is eligible for Task B (not saturated),
    // the diagnosis should be "healthy" and the worker should be in the eligible_workers list.
    let (status_mixed, body_mixed) = get_json_with_auth(
        &app_mixed,
        "/admin/queues/test-queue-mixed/eligibility",
        true,
    )
    .await;
    assert_eq!(status_mixed, StatusCode::OK);
    assert_eq!(body_mixed["summary"]["diagnosis"], "healthy");
    assert!(
        !body_mixed["eligible_workers"]
            .as_array()
            .unwrap()
            .is_empty()
    );

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

#[tokio::test]
async fn test_worker_capabilities_routing_and_triage() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    let activity = autumn_harvest::info::ActivityInfo {
        name: "gpu_activity",
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("test-queue-capabilities"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: Some("gpu = true"),
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for_activities(
            &["test-queue-capabilities"],
            vec![activity],
            ShardRouter::single(),
        ),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    // 1. Register a worker with matching labels (gpu=true)
    let mut matching_labels = std::collections::HashMap::new();
    matching_labels.insert("gpu".to_string(), "true".to_string());
    {
        let mut conn = pool.get().await.unwrap();
        register_worker(
            &mut conn,
            "worker-gpu-true",
            &["test-queue-capabilities".to_string()],
            &[0],
            4,
            "localhost",
            None,
            "v1",
            None,
            &matching_labels,
            0,
        )
        .await
        .unwrap();
    }

    // 2. Register a worker without matching labels (no labels)
    {
        let mut conn = pool.get().await.unwrap();
        register_worker(
            &mut conn,
            "worker-gpu-false",
            &["test-queue-capabilities".to_string()],
            &[0],
            4,
            "localhost",
            None,
            "v1",
            None,
            &std::collections::HashMap::new(),
            0,
        )
        .await
        .unwrap();
    }

    // 3. Seed an activity task requiring gpu=true
    {
        let mut conn = pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO harvest_task_queue (
                id, queue_name, task_type, activity_name, input, state, priority, max_attempts, scheduled_at
             ) VALUES (
                gen_random_uuid(), 'test-queue-capabilities', 'activity', 'gpu_activity', '{}'::jsonb, 'PENDING', 0, 1, NOW() - INTERVAL '5 seconds'
             )",
        )
        .execute(&mut conn)
        .await
        .unwrap();
    }

    // 4. Test capable_of filter via GET /workers?capable_of=gpu_activity
    let (status, body) = get_json_with_auth(&app, "/workers?capable_of=gpu_activity", true).await;
    assert_eq!(status, StatusCode::OK);
    let workers = body.as_array().unwrap();
    assert_eq!(
        workers.len(),
        1,
        "only 1 worker should be capable of gpu_activity"
    );
    assert_eq!(workers[0]["worker_id"], "worker-gpu-true");

    // 5. Test triage explainer via GET /admin/queues/test-queue-capabilities/eligibility
    let (status_elig, body_elig) = get_json_with_auth(
        &app,
        "/admin/queues/test-queue-capabilities/eligibility",
        true,
    )
    .await;
    assert_eq!(status_elig, StatusCode::OK);

    // worker-gpu-false should be in ineligible_workers, with reason unsatisfied_requirement:gpu=true
    let ineligible = body_elig["ineligible_workers"].as_array().unwrap();
    let gpu_false_info = ineligible
        .iter()
        .find(|w| w["worker_id"] == "worker-gpu-false")
        .unwrap();
    let reasons = gpu_false_info["reason_codes"].as_array().unwrap();
    assert!(
        reasons
            .iter()
            .any(|r| r == "unsatisfied_requirement:gpu=true"),
        "worker-gpu-false should have unsatisfied requirement gpu=true: {reasons:?}"
    );

    // worker-gpu-true should be in eligible_workers
    let eligible = body_elig["eligible_workers"].as_array().unwrap();
    assert!(eligible.iter().any(|w| w["worker_id"] == "worker-gpu-true"));
}

fn runtime_for_activities(
    queues: &[&str],
    activities: Vec<autumn_harvest::info::ActivityInfo>,
    router: ShardRouter,
) -> HarvestApiRuntime {
    HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![workflow_info()], activities)),
        Arc::new(DagCatalog::new()),
        Arc::new(Vec::new()),
        None,
        queues.iter().map(|queue| (*queue).to_string()).collect(),
        autumn_harvest::scheduler::SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        router,
    )
}

fn runtime_for_registry(
    queues: &[&str],
    registry: Arc<HandlerRegistry>,
    router: ShardRouter,
) -> HarvestApiRuntime {
    HarvestApiRuntime::new(
        registry,
        Arc::new(DagCatalog::new()),
        Arc::new(Vec::new()),
        None,
        queues.iter().map(|queue| (*queue).to_string()).collect(),
        autumn_harvest::scheduler::SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        router,
    )
}

fn cb_activity(name: &'static str, queue: &'static str) -> autumn_harvest::info::ActivityInfo {
    autumn_harvest::info::ActivityInfo {
        name,
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(queue),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: Some(autumn_harvest::policy::CircuitBreakerPolicy::new(
            3,
            Duration::from_secs(30),
            Duration::from_secs(60),
        )),
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    }
}

// AC7(b): a task whose next activity is gated by an OPEN circuit breaker reports
// `circuit_open` — never an empty impediment set — and the CB activity's
// saturated rate-limit bucket is suppressed at the claim-time explainer (issue
// #369), so the reason is `circuit_open` and not `rate_limit_exhausted`.
//
// The suppression is proven end-to-end: a SECOND, non-CB task shares the same
// rate-limit key and genuinely saturates it, so `saturated_rate_limits` really
// does contain the key. The non-CB task's own eligibility reports
// `rate_limit_exhausted` (proving the bucket is exhausted), while the CB task's
// own eligibility reports `circuit_open` and never `rate_limit_exhausted`
// (proving the #369 suppression, not a vacuously-empty saturated set). If the
// helper's `!has_cb` guard were dropped, the CB task's per-task evaluation would
// wrongly surface `rate_limit_exhausted` here.
#[tokio::test]
async fn test_open_circuit_reports_circuit_open() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    // Saturated bucket for the shared rate-limit key.
    seed_rate_limit_bucket(&pool, "cb-rl-key", 0.0, 1.0, 0.0).await;

    // Build the registry ourselves so we can force the shared, Arc-shared
    // breaker OPEN — the same in-process state the explainer reads.
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info()],
        vec![cb_activity("charge_card", "test-queue-cb")],
    ));
    registry
        .circuit_breakers()
        .force_open("charge_card", std::time::Instant::now());

    // The CB task: a real activity task naming the CB activity, carrying the
    // shared saturated rate-limit key (Fix 5: task_type='activity', not
    // 'workflow', so a future refactor gating the CB branch on task type cannot
    // silently pass).
    let cb_task_id = seed_typed_task(
        &pool,
        "test-queue-cb",
        "activity",
        Some("charge_card"),
        Some("cb-rl-key"),
        "NOW() - INTERVAL '5 seconds'",
    )
    .await;

    // A SECOND, NON-CB task sharing the same rate-limit key. Because non-CB
    // tasks are the only source that populates `rate_limit_keys`, this is what
    // makes `saturated_rate_limits` genuinely contain "cb-rl-key" (Fix 6).
    let noncb_task_id = seed_typed_task(
        &pool,
        "test-queue-cb",
        "workflow",
        None,
        Some("cb-rl-key"),
        "NOW() - INTERVAL '5 seconds'",
    )
    .await;

    // An otherwise-perfectly-eligible worker (right queue, shard, build).
    register_active_worker_with_build(&pool, "worker-cb", &["test-queue-cb"], &[0], "v1", 10, 0)
        .await;

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for_registry(&["test-queue-cb"], registry, ShardRouter::single()),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    // Queue eligibility endpoint: worker-cb is ineligible and surfaces
    // `circuit_open` (never an empty/unknown set), absent from the eligible
    // list, diagnosis no_eligible_workers. Note: at the queue level the
    // per-worker reason set is a UNION across every claimable task, so the
    // sibling non-CB task legitimately adds `rate_limit_exhausted` here — the
    // CB-activity suppression is asserted at the per-task endpoint below, where
    // the CB task is evaluated in isolation.
    let (status, body) =
        get_json_with_auth(&app, "/admin/queues/test-queue-cb/eligibility", true).await;
    assert_eq!(status, StatusCode::OK);

    let ineligible = body["ineligible_workers"].as_array().unwrap();
    let w_cb = ineligible
        .iter()
        .find(|w| w["worker_id"] == "worker-cb")
        .expect("worker-cb should be ineligible");
    let reasons: Vec<&str> = w_cb["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        reasons.contains(&"circuit_open"),
        "expected circuit_open, got {reasons:?}"
    );
    // Never left with an empty/unknown impediment set for the open-circuit cause.
    assert!(!reasons.contains(&"unknown"), "got {reasons:?}");
    // The worker must be absent from the eligible list.
    let eligible = body["eligible_workers"].as_array().unwrap();
    assert!(!eligible.iter().any(|w| w["worker_id"] == "worker-cb"));
    assert_eq!(body["summary"]["diagnosis"], "no_eligible_workers");

    // Per-task endpoint for the CB task: evaluated in isolation, worker-cb
    // reports `circuit_open` and NOT `rate_limit_exhausted` — the #369
    // suppression (Fix 6).
    let (status_cb, body_cb) =
        get_json_with_auth(&app, format!("/admin/tasks/{cb_task_id}/eligibility"), true).await;
    assert_eq!(status_cb, StatusCode::OK);
    let ineligible_cb = body_cb["ineligible_workers"].as_array().unwrap();
    let w_cb_task = ineligible_cb
        .iter()
        .find(|w| w["worker_id"] == "worker-cb")
        .expect("worker-cb should be ineligible for the CB task");
    let cb_reasons: Vec<&str> = w_cb_task["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        cb_reasons.contains(&"circuit_open"),
        "expected circuit_open for the CB task, got {cb_reasons:?}"
    );
    assert!(
        !cb_reasons.contains(&"rate_limit_exhausted"),
        "rate-limit reason must be suppressed for a CB activity, got {cb_reasons:?}"
    );

    // Per-task endpoint for the NON-CB task sharing the same key: it correctly
    // reports `rate_limit_exhausted`, proving the shared bucket is genuinely
    // saturated (so the CB suppression above is not vacuous).
    let (status_nc, body_nc) = get_json_with_auth(
        &app,
        format!("/admin/tasks/{noncb_task_id}/eligibility"),
        true,
    )
    .await;
    assert_eq!(status_nc, StatusCode::OK);
    let ineligible_nc = body_nc["ineligible_workers"].as_array().unwrap();
    let w_nc_task = ineligible_nc
        .iter()
        .find(|w| w["worker_id"] == "worker-cb")
        .expect("worker-cb should be ineligible for the non-CB task");
    assert!(
        w_nc_task["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap() == "rate_limit_exhausted"),
        "expected rate_limit_exhausted for the non-CB task, got {:?}",
        w_nc_task["reason_codes"]
    );
}

// AC7(c): a genuinely concurrency-capped task reports `concurrency_saturated`
// and nothing else.
#[tokio::test]
async fn test_concurrency_capped_reports_concurrency_saturated_only() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    // A RUNNING task occupying the single slot for key "tenant-x".
    {
        let mut conn = pool.get().await.expect("running task connection");
        diesel::sql_query(
            "INSERT INTO harvest_task_queue (
                id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at,
                concurrency_key, concurrency_cap, worker_id
             ) VALUES (
                gen_random_uuid(), 'test-queue-conc', 'workflow', '{}'::jsonb, 'RUNNING', 0, 1, NOW(),
                'tenant-x', 1, 'worker-holding'
             )",
        )
        .execute(&mut conn)
        .await
        .expect("failed to insert running task to saturate concurrency");
    }

    // The pending task for the same key/cap — saturated, no other impediment.
    seed_task_detailed(
        &pool,
        "test-queue-conc",
        None,
        None,
        None,
        Some("tenant-x"),
        Some(1),
    )
    .await;

    // Otherwise-eligible worker.
    register_active_worker_with_build(
        &pool,
        "worker-conc",
        &["test-queue-conc"],
        &[0],
        "v1",
        10,
        0,
    )
    .await;

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue-conc"], ShardRouter::single()),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) =
        get_json_with_auth(&app, "/admin/queues/test-queue-conc/eligibility", true).await;
    assert_eq!(status, StatusCode::OK);

    let ineligible = body["ineligible_workers"].as_array().unwrap();
    let w_conc = ineligible
        .iter()
        .find(|w| w["worker_id"] == "worker-conc")
        .expect("worker-conc should be ineligible");
    let reasons: Vec<&str> = w_conc["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(
        reasons,
        vec!["concurrency_saturated"],
        "expected concurrency_saturated and nothing else, got {reasons:?}"
    );
}

// Issue #619 review: `queue_paused` must lead the FINAL `reason_codes` array,
// not just the producer's intermediate list.
//
// `task_intrinsic_impediment_reasons` pushes it first, but the reasons are then
// collapsed through a `HashSet` (destroying insertion order) and re-sorted — and
// `concurrency_saturated` sorts *before* `queue_paused` alphabetically. So the
// documented priority ("the one impediment a triaging operator should see before
// anything else") only holds if it survives that merge. This test drives the real
// endpoint with BOTH impediments present, which is the only way to observe it.
#[tokio::test]
async fn test_queue_paused_leads_the_final_reason_codes_over_another_impediment() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    // Saturate the single concurrency slot for "tenant-x" so the held task has a
    // second, alphabetically-earlier impediment alongside the pause.
    {
        let mut conn = pool.get().await.expect("running task connection");
        diesel::sql_query(
            "INSERT INTO harvest_task_queue (
                id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at,
                concurrency_key, concurrency_cap, worker_id
             ) VALUES (
                gen_random_uuid(), 'test-queue-paused-prio', 'workflow', '{}'::jsonb, 'RUNNING', 0, 1, NOW(),
                'tenant-x', 1, 'worker-holding'
             )",
        )
        .execute(&mut conn)
        .await
        .expect("failed to insert running task to saturate concurrency");

        // The operator's deliberate hold on the same queue.
        diesel::sql_query(
            // `queue_name` is the PK, so stay re-run safe: every sibling test in
            // this file seeds with generated ids and CI gives each test a fresh
            // container, but a fixed PK would otherwise break a second run
            // against a shared database.
            "INSERT INTO harvest_queue_pauses (queue_name, reason, paused_by, paused_at) \
             VALUES ('test-queue-paused-prio', 'provider outage', 'alice', NOW()) \
             ON CONFLICT (queue_name) DO NOTHING",
        )
        .execute(&mut conn)
        .await
        .expect("failed to pause the queue");
    }

    seed_task_detailed(
        &pool,
        "test-queue-paused-prio",
        None,
        None,
        None,
        Some("tenant-x"),
        Some(1),
    )
    .await;

    register_active_worker_with_build(
        &pool,
        "worker-paused-prio",
        &["test-queue-paused-prio"],
        &[0],
        "v1",
        10,
        0,
    )
    .await;

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue-paused-prio"], ShardRouter::single()),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json_with_auth(
        &app,
        "/admin/queues/test-queue-paused-prio/eligibility",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let ineligible = body["ineligible_workers"].as_array().unwrap();
    let w = ineligible
        .iter()
        .find(|w| w["worker_id"] == "worker-paused-prio")
        .expect("the worker must be ineligible while the queue is held");
    let reasons: Vec<&str> = w["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();

    // NB: `reasons[0]` rather than `.first()` — this file imports Diesel's
    // prelude, whose DSL traits shadow `first` on `Vec` and blow up trait
    // resolution.
    assert!(
        !reasons.is_empty(),
        "a held, saturated task must report impediments"
    );
    assert_eq!(
        reasons[0], "queue_paused",
        "the operator's own deliberate hold must lead the array an operator \
         actually reads -- a plain alphabetical sort buries it behind \
         concurrency_saturated; got {reasons:?}"
    );
    // Both impediments are still reported: the priority reorders, it never drops.
    assert!(
        reasons.contains(&"concurrency_saturated"),
        "the other impediment must survive the reorder, got {reasons:?}"
    );
}

// Issue #619 review: a queue pause must survive the worker-reason short-circuit.
//
// A worker that is unsubscribed, assigned to another shard, draining, or stopped
// is pushed straight into `ineligible_workers` carrying only its worker-specific
// reasons and then `continue`s -- bypassing BOTH the shared pause impediment and
// `sort_reason_codes`. The sibling test above only reaches the pause check
// because its worker is healthy.
//
// So when *every* online worker has one of those conditions -- e.g. a fleet
// drained for the very outage the operator paused the queue for -- the response
// never mentions `queue_paused` at all, and the deliberate hold is invisible on
// the one surface built to answer "why is nothing dispatching?".
#[tokio::test]
async fn test_queue_paused_survives_the_worker_reason_short_circuit() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    {
        let mut conn = pool.get().await.expect("connection");
        diesel::sql_query(
            "INSERT INTO harvest_queue_pauses (queue_name, reason, paused_by, paused_at) \
             VALUES ('test-queue-paused-drain', 'provider outage', 'alice', NOW()) \
             ON CONFLICT (queue_name) DO NOTHING",
        )
        .execute(&mut conn)
        .await
        .expect("failed to pause the queue");
    }

    seed_task_detailed(
        &pool,
        "test-queue-paused-drain",
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    // The ONLY online worker is draining, so it takes the worker-reason
    // short-circuit and never reaches the per-task pause check.
    register_active_worker_with_build(
        &pool,
        "worker-paused-drain",
        &["test-queue-paused-drain"],
        &[0],
        "v1",
        10,
        0,
    )
    .await;
    mark_worker_draining(&pool, "worker-paused-drain").await;

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for(&["test-queue-paused-drain"], ShardRouter::single()),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json_with_auth(
        &app,
        "/admin/queues/test-queue-paused-drain/eligibility",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let ineligible = body["ineligible_workers"].as_array().unwrap();
    let w = ineligible
        .iter()
        .find(|w| w["worker_id"] == "worker-paused-drain")
        .expect("a draining worker must be reported ineligible");
    let reasons: Vec<&str> = w["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();

    assert!(
        reasons.contains(&"queue_paused"),
        "the operator's deliberate hold must be reported even when every worker \
         is short-circuited on a worker-specific reason -- otherwise the only \
         surface that answers \"why is nothing dispatching?\" omits the actual \
         answer; got {reasons:?}"
    );
    // NB: `reasons[0]` rather than `.first()` -- Diesel's prelude DSL traits
    // shadow `first` on `Vec` in this file.
    assert_eq!(
        reasons[0], "queue_paused",
        "the pause must still LEAD the array through this branch, which \
         previously skipped sort_reason_codes entirely; got {reasons:?}"
    );
    // The worker-specific reason is reordered, never dropped.
    assert!(
        reasons.contains(&"worker_draining"),
        "the worker condition must survive alongside the pause, got {reasons:?}"
    );
    // `all_draining` is a statement about the WORKER fleet; the queue pause is a
    // statement about the QUEUE. Both are true here, and adding the pause
    // impediment must not silently downgrade the fleet diagnosis.
    assert_eq!(
        body["summary"]["diagnosis"], "all_draining",
        "surfacing queue_paused must not change the worker-fleet diagnosis"
    );
}

#[tokio::test]
async fn test_worker_queue_filtering_for_capable_of() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    let activity = autumn_harvest::info::ActivityInfo {
        name: "transcode_activity",
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("transcoding"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: Some("gpu = true"),
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for_activities(
            &["transcoding", "other-queue"],
            vec![activity],
            ShardRouter::single(),
        ),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let mut matching_labels = std::collections::HashMap::new();
    matching_labels.insert("gpu".to_string(), "true".to_string());

    // 1. Register a worker with matching labels and matching queue
    {
        let mut conn = pool.get().await.unwrap();
        register_worker(
            &mut conn,
            "worker-gpu-transcoding",
            &["transcoding".to_string()],
            &[0],
            4,
            "localhost",
            None,
            "v1",
            None,
            &matching_labels,
            0,
        )
        .await
        .unwrap();
    }

    // 2. Register a worker with matching labels but WRONG queue
    {
        let mut conn = pool.get().await.unwrap();
        register_worker(
            &mut conn,
            "worker-gpu-other",
            &["other-queue".to_string()],
            &[0],
            4,
            "localhost",
            None,
            "v1",
            None,
            &matching_labels,
            0,
        )
        .await
        .unwrap();
    }

    // 3. Request capable_of
    let (status, body) =
        get_json_with_auth(&app, "/workers?capable_of=transcode_activity", true).await;
    assert_eq!(status, StatusCode::OK);
    let workers = body.as_array().unwrap();
    assert_eq!(
        workers.len(),
        1,
        "only 1 worker should be returned because the other is on the wrong queue"
    );
    assert_eq!(workers[0]["worker_id"], "worker-gpu-transcoding");
}

#[tokio::test]
async fn test_worker_queue_filtering_with_explicit_queue_override() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    let activity = autumn_harvest::info::ActivityInfo {
        name: "transcode_activity",
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("transcoding"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: Some("gpu = true"),
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };

    let state = api_state(
        HarvestDbPool::from(pool.clone()),
        runtime_for_activities(
            &["transcoding", "custom-queue"],
            vec![activity],
            ShardRouter::single(),
        ),
    );
    state.set_admin_auth_boundary(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let mut matching_labels = std::collections::HashMap::new();
    matching_labels.insert("gpu".to_string(), "true".to_string());

    // 1. Register a worker on default queue ("transcoding")
    {
        let mut conn = pool.get().await.unwrap();
        register_worker(
            &mut conn,
            "worker-default-queue",
            &["transcoding".to_string()],
            &[0],
            4,
            "localhost",
            None,
            "v1",
            None,
            &matching_labels,
            0,
        )
        .await
        .unwrap();
    }

    // 2. Register a worker on overridden queue ("custom-queue")
    {
        let mut conn = pool.get().await.unwrap();
        register_worker(
            &mut conn,
            "worker-custom-queue",
            &["custom-queue".to_string()],
            &[0],
            4,
            "localhost",
            None,
            "v1",
            None,
            &matching_labels,
            0,
        )
        .await
        .unwrap();
    }

    // 3. Request capable_of with queue override: GET /workers?queue=custom-queue&capable_of=transcode_activity
    let (status, body) = get_json_with_auth(
        &app,
        "/workers?queue=custom-queue&capable_of=transcode_activity",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let workers = body.as_array().unwrap();
    assert_eq!(
        workers.len(),
        1,
        "only 1 worker should be returned because only worker-custom-queue is on custom-queue"
    );
    assert_eq!(workers[0]["worker_id"], "worker-custom-queue");
}

#[tokio::test]
async fn test_worker_heartbeat_updates_labels() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    // 1. Register a worker with empty labels
    {
        let mut conn = pool.get().await.unwrap();
        register_worker(
            &mut conn,
            "worker-hb-labels-test",
            &["default".to_string()],
            &[0],
            4,
            "localhost",
            None,
            "v1",
            None,
            &std::collections::HashMap::new(),
            0,
        )
        .await
        .unwrap();
    }

    // 2. Call heartbeat_worker with new labels
    let mut updated_labels = std::collections::HashMap::new();
    updated_labels.insert("gpu".to_string(), "true".to_string());
    let labels_json = serde_json::to_value(&updated_labels).unwrap();

    {
        let mut conn = pool.get().await.unwrap();
        let affected = heartbeat_worker(&mut conn, "worker-hb-labels-test", 0, &labels_json, 0)
            .await
            .unwrap();
        assert_eq!(affected, 1);
    }

    // 3. Retrieve the worker details and verify the labels have been updated in the DB
    {
        let mut conn = pool.get().await.unwrap();
        let worker_row = get_worker(&mut conn, "worker-hb-labels-test", Duration::from_secs(10))
            .await
            .unwrap()
            .expect("worker should exist");

        let worker_labels: std::collections::HashMap<String, String> =
            serde_json::from_value(worker_row.worker.labels).unwrap();
        assert_eq!(worker_labels.get("gpu").map(String::as_str), Some("true"));
    }
}
