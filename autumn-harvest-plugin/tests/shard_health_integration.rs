use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::retention::RetentionConfig;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
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
type ThreeShardUrls = (String, String, String);

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

async fn setup_two_shards_with_migrations() -> ((String, String), ContainerAsync<Postgres>) {
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
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let shard0_db = format!("harvest_shard_health_0_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_shard_health_1_{}", uuid::Uuid::new_v4().simple());

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("failed to connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {shard0_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 0 database");
    diesel::sql_query(format!("CREATE DATABASE {shard1_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 1 database");

    let shard0_url = format!("postgres://postgres:postgres@{host}:{port}/{shard0_db}");
    let shard1_url = format!("postgres://postgres:postgres@{host}:{port}/{shard1_db}");
    autumn_web::migrate::run_pending(&shard0_url, autumn_harvest::MIGRATIONS)
        .expect("failed to migrate shard 0");
    autumn_web::migrate::run_pending(&shard1_url, autumn_harvest::MIGRATIONS)
        .expect("failed to migrate shard 1");
    ((shard0_url, shard1_url), container)
}

async fn setup_three_shards_with_migrations() -> (ThreeShardUrls, ContainerAsync<Postgres>) {
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
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let shard0_db = format!("harvest_shard_health_0_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_shard_health_1_{}", uuid::Uuid::new_v4().simple());
    let shard2_db = format!("harvest_shard_health_2_{}", uuid::Uuid::new_v4().simple());

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("failed to connect to admin database");
    for db_name in [&shard0_db, &shard1_db, &shard2_db] {
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin_conn)
            .await
            .expect("failed to create shard database");
    }

    let shard0_url = format!("postgres://postgres:postgres@{host}:{port}/{shard0_db}");
    let shard1_url = format!("postgres://postgres:postgres@{host}:{port}/{shard1_db}");
    let shard2_url = format!("postgres://postgres:postgres@{host}:{port}/{shard2_db}");
    for shard_url in [&shard0_url, &shard1_url, &shard2_url] {
        autumn_web::migrate::run_pending(shard_url, autumn_harvest::MIGRATIONS)
            .expect("failed to migrate shard");
    }

    ((shard0_url, shard1_url, shard2_url), container)
}

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn build_three_shard_pool(shard0_url: &str, shard1_url: &str, shard2_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    pools.insert(ShardId::new(2), build_test_pool(shard2_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        name: "echo_workflow",
        module: "tests",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        concurrency: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

fn activity_info(queue: Option<&'static str>) -> ActivityInfo {
    ActivityInfo {
        name: "send_email",
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_queue: queue,
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    }
}

fn runtime_for(
    queues: &[&str],
    activity_queue: Option<&'static str>,
    schedules: Vec<WorkflowSchedule>,
    router: ShardRouter,
    scheduler_monitor: SchedulerMonitor,
) -> HarvestApiRuntime {
    let activities = activity_queue
        .map(|queue| activity_info(Some(queue)))
        .into_iter()
        .collect::<Vec<_>>();
    HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![workflow_info()], activities)),
        Arc::new(DagCatalog::new()),
        Arc::new(schedules),
        None,
        queues.iter().map(|queue| (*queue).to_string()).collect(),
        scheduler_monitor,
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

async fn register_active_worker(pool: &DbPool, worker_id: &str, queues: &[&str], shards: &[i32]) {
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
        10,
        "localhost",
        Some(env!("CARGO_PKG_VERSION")),
        "",
        None,
    )
    .await
    .expect("worker registration should succeed");
}

async fn mark_worker_stale(pool: &DbPool, worker_id: &str) {
    let mut conn = pool.get().await.expect("stale update connection");
    diesel::sql_query(
        "UPDATE harvest_workers
         SET last_heartbeat_at = NOW() - INTERVAL '10 minutes',
             status = $1
         WHERE worker_id = $2",
    )
    .bind::<diesel::sql_types::Text, _>(WorkerStatus::Active.as_str())
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(&mut conn)
    .await
    .expect("worker should be marked stale");
}

async fn read_json_response(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    serde_json::from_slice(&body).expect("response must be JSON")
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri.into())
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should complete");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

fn roles(row: &Value) -> Vec<String> {
    row["roles"]
        .as_array()
        .expect("roles should be an array")
        .iter()
        .map(|role| role.as_str().expect("role should be a string").to_string())
        .collect()
}

fn blocking_reasons(row: &Value) -> Vec<String> {
    row["blocking_reasons"]
        .as_array()
        .expect("blocking reasons should be an array")
        .iter()
        .map(|reason| {
            reason
                .as_str()
                .expect("blocking reason should be a string")
                .to_string()
        })
        .collect()
}

fn reason_codes(row: &Value) -> Vec<String> {
    row["reason_codes"]
        .as_array()
        .expect("reason codes should be an array")
        .iter()
        .map(|reason| {
            reason
                .as_str()
                .expect("reason code should be a string")
                .to_string()
        })
        .collect()
}

fn find_shard(body: &Value, shard_id: i64) -> &Value {
    body["shards"]
        .as_array()
        .expect("shards should be an array")
        .iter()
        .find(|row| row["shard_id"] == shard_id)
        .unwrap_or_else(|| panic!("missing shard {shard_id}: {body}"))
}

#[tokio::test]
async fn three_shard_rollout_gate_tracks_missing_added_and_stale_worker_coverage() {
    let ((shard0_url, shard1_url, shard2_url), _container) =
        setup_three_shards_with_migrations().await;
    let pool = build_three_shard_pool(&shard0_url, &shard1_url, &shard2_url);
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "ready-worker-0",
        &["default"],
        &[0],
    )
    .await;
    register_active_worker(
        pool.pool_for(ShardId::new(1)),
        "ready-worker-1",
        &["default"],
        &[1],
    )
    .await;
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        ShardId::new(0),
    );
    let state = api_state(
        pool.clone(),
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            router,
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overall_readiness"], "degraded");
    let uncovered = find_shard(&body, 2);
    assert_eq!(uncovered["readiness"], "degraded");
    assert!(
        reason_codes(uncovered)
            .iter()
            .any(|reason| reason == "worker_queue_uncovered"),
        "missing shard 2 worker coverage should gate rollout: {uncovered}"
    );

    register_active_worker(
        pool.pool_for(ShardId::new(2)),
        "ready-worker-2",
        &["default"],
        &[2],
    )
    .await;
    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overall_readiness"], "ready");
    assert_eq!(find_shard(&body, 2)["readiness"], "ready");

    mark_worker_stale(pool.pool_for(ShardId::new(1)), "ready-worker-1").await;
    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overall_readiness"], "degraded");
    let stale_shard = find_shard(&body, 1);
    assert_eq!(stale_shard["stale_worker_count"], 1);
    assert!(
        reason_codes(stale_shard)
            .iter()
            .any(|reason| reason == "worker_health_stale"),
        "stale shard 1 worker should degrade the shard within the heartbeat window: {stale_shard}"
    );
}

#[tokio::test]
async fn single_shard_ready_state_uses_shard_zero_response_shape() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(&pool, "ready-worker", &["default", "email"], &[0]).await;
    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Some("email"),
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overall_readiness"], "ready");
    let shard = find_shard(&body, 0);
    assert_eq!(shard["readiness"], "ready");
    assert_eq!(shard["reachable"], true);
    assert_eq!(shard["schema"]["ready"], true);
    assert_eq!(shard["queue_depth"]["total_pending"], 0);
    assert_eq!(shard["dlq"]["count"], 0);
    assert_eq!(
        roles(shard),
        vec![
            "readable".to_string(),
            "writable".to_string(),
            "default".to_string()
        ]
    );
    assert!(shard["last_health_sample_time"].is_string());
}

#[tokio::test]
async fn writable_shard_reports_degraded_codes_and_worker_counts() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(&pool, "default-worker", &["default"], &[0]).await;
    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Some("email"),
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overall_readiness"], "degraded");
    let shard = find_shard(&body, 0);
    assert_eq!(shard["readiness"], "degraded");
    assert_eq!(shard["active_worker_count"], 1);
    assert_eq!(shard["stale_worker_count"], 0);
    assert!(
        reason_codes(shard)
            .iter()
            .any(|reason| reason == "worker_queue_uncovered"),
        "missing queue coverage should expose a machine reason code: {shard}"
    );
}

#[tokio::test]
async fn unreachable_writable_shard_reports_unavailable_reason_code() {
    let (shard0_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_two_shard_pool(&shard0_url, "postgres://postgres:postgres@127.0.0.1:1/nope");
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "ready-worker-0",
        &["default"],
        &[0],
    )
    .await;
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let state = api_state(
        pool,
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            router,
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overall_readiness"], "unavailable");
    let shard = find_shard(&body, 1);
    assert_eq!(shard["readiness"], "unavailable");
    assert!(
        reason_codes(shard)
            .iter()
            .any(|reason| reason == "shard_unreachable"),
        "unreachable shard should expose a machine reason code: {shard}"
    );
}

#[tokio::test]
async fn health_endpoint_can_enforce_writable_shard_readiness() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let state = api_state(
        HarvestDbPool::from(build_test_pool(&database_url)),
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
    );
    state.set_health_requires_shard_readiness(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("prod"));

    let (status, body) = get_json(&app, "/health").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["runtime_ready"], true);
    assert_eq!(body["shard_readiness_enforced"], true);
    assert_eq!(body["shard_readiness"]["overall_readiness"], "degraded");
}

#[tokio::test]
async fn health_endpoint_enforces_unavailable_writable_shard_readiness() {
    let (shard0_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_two_shard_pool(&shard0_url, "postgres://postgres:postgres@127.0.0.1:1/nope");
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "ready-worker-0",
        &["default"],
        &[0],
    )
    .await;
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let state = api_state(
        pool,
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            router,
            SchedulerMonitor::offline(),
        ),
    );
    state.set_health_requires_shard_readiness(true);
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("prod"));

    let (status, body) = get_json(&app, "/health").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["shard_readiness"]["overall_readiness"], "unavailable");
}

#[tokio::test]
async fn readable_only_candidate_without_workers_lists_promotion_blockers() {
    let ((shard0_url, shard1_url), _container) = setup_two_shards_with_migrations().await;
    let pool = build_two_shard_pool(&shard0_url, &shard1_url);
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "ready-worker-0",
        &["default"],
        &[0],
    )
    .await;
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    );
    let state = api_state(
        pool,
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            router,
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health?candidate_shard=1").await;

    assert_eq!(status, StatusCode::OK);
    let candidate = find_shard(&body, 1);
    assert_eq!(candidate["candidate"], true);
    assert_eq!(candidate["readiness"], "degraded");
    assert_eq!(roles(candidate), vec!["readable".to_string()]);
    assert!(
        blocking_reasons(candidate)
            .iter()
            .any(|reason| reason
                .contains("no healthy active worker covers required queue 'default'")),
        "candidate should explain missing worker coverage: {candidate}"
    );
}

#[tokio::test]
async fn writable_shard_with_missing_queue_coverage_is_degraded() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(&pool, "default-worker", &["default"], &[0]).await;
    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Some("email"),
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    let shard = find_shard(&body, 0);
    assert_eq!(shard["readiness"], "degraded");
    assert!(
        blocking_reasons(shard)
            .iter()
            .any(|reason| reason.contains("required queue 'email'")),
        "missing email worker coverage should be named: {shard}"
    );
}

#[tokio::test]
async fn stale_worker_coverage_makes_writable_shard_degraded() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(&pool, "stale-worker", &["default"], &[0]).await;
    mark_worker_stale(&pool, "stale-worker").await;
    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    let shard = find_shard(&body, 0);
    assert_eq!(shard["readiness"], "degraded");
    assert!(
        blocking_reasons(shard)
            .iter()
            .any(|reason| reason.contains("health data is stale")
                || reason.contains("no healthy active worker covers required queue 'default'")),
        "stale coverage should be an explicit blocker: {shard}"
    );
}

#[tokio::test]
async fn stale_scheduler_coverage_makes_scheduled_writable_shard_degraded() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(&pool, "ready-worker", &["default"], &[0]).await;
    let schedule =
        WorkflowSchedule::new("echo_workflow", Schedule::Interval(Duration::from_secs(60)));
    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default"],
            None,
            vec![schedule],
            ShardRouter::single(),
            SchedulerMonitor::new(1),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    let shard = find_shard(&body, 0);
    assert_eq!(shard["readiness"], "degraded");
    assert!(
        blocking_reasons(shard)
            .iter()
            .any(|reason| reason.contains("scheduler")),
        "scheduler freshness should be an explicit blocker: {shard}"
    );
}

#[tokio::test]
async fn unreachable_shard_returns_partial_health_for_reachable_shards() {
    let (shard0_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_two_shard_pool(&shard0_url, "postgres://postgres:postgres@127.0.0.1:1/nope");
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "ready-worker-0",
        &["default"],
        &[0],
    )
    .await;
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let state = api_state(
        pool,
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            router,
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    let healthy = find_shard(&body, 0);
    let degraded = find_shard(&body, 1);
    assert_eq!(healthy["reachable"], true);
    assert_eq!(degraded["reachable"], false);
    assert_eq!(degraded["readiness"], "unavailable");
    assert!(
        degraded["error_summary"]
            .as_str()
            .is_some_and(|summary| summary.contains("database connection could not be acquired")),
        "unreachable shard should include operator-facing error summary: {degraded}"
    );
}

#[tokio::test]
async fn migration_mismatch_marks_only_affected_shard_degraded() {
    let ((shard0_url, shard1_url), _container) = setup_two_shards_with_migrations().await;
    let mut shard1_conn = <AsyncPgConnection as AsyncConnection>::establish(&shard1_url)
        .await
        .expect("shard 1 connection");
    diesel::sql_query(
        "DELETE FROM __diesel_schema_migrations
         WHERE version = (SELECT max(version) FROM __diesel_schema_migrations)",
    )
    .execute(&mut shard1_conn)
    .await
    .expect("should remove latest migration marker");

    let pool = build_two_shard_pool(&shard0_url, &shard1_url);
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "ready-worker-0",
        &["default"],
        &[0],
    )
    .await;
    register_active_worker(
        pool.pool_for(ShardId::new(1)),
        "ready-worker-1",
        &["default"],
        &[1],
    )
    .await;
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let state = api_state(
        pool,
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            router,
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(find_shard(&body, 0)["readiness"], "ready");
    let mismatched = find_shard(&body, 1);
    assert_eq!(mismatched["readiness"], "degraded");
    assert_eq!(mismatched["schema"]["ready"], false);
    assert!(
        mismatched["schema"]["missing_migrations"]
            .as_array()
            .is_some_and(|missing| !missing.is_empty()),
        "schema mismatch should list missing migrations: {mismatched}"
    );
}

#[tokio::test]
async fn router_only_writable_shard_is_reported_unavailable() {
    let mut pools = BTreeMap::new();
    pools.insert(
        ShardId::new(0),
        build_test_pool("postgres://postgres:postgres@127.0.0.1:1/shard0"),
    );
    let pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let state = api_state(
        pool,
        runtime_for(
            &["default"],
            None,
            Vec::new(),
            router,
            SchedulerMonitor::offline(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/shards/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overall_readiness"], "unavailable");
    let router_only = find_shard(&body, 1);
    assert_eq!(router_only["reachable"], false);
    assert_eq!(router_only["readiness"], "unavailable");
    assert_eq!(
        roles(router_only),
        vec!["readable".to_string(), "writable".to_string()]
    );
    assert!(
        router_only["error_summary"].as_str().is_some_and(
            |summary| summary.contains("configured in router but has no installed pool")
        ),
        "router-only shard should explain missing pool: {router_only}"
    );
}
