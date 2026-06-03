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
use autumn_harvest_plugin::preflight::{PreflightStatus, build_preflight_report};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

const READ_ONLY_TEST_PASSWORD: &str = "harvest_readonly_password";
const WRITE_NO_SELECT_TEST_PASSWORD: &str = "harvest_write_no_select_password";
const WRITE_NO_SEQUENCE_TEST_PASSWORD: &str = "harvest_write_no_sequence_password";

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

fn database_url_with_credentials(database_url: &str, user: &str, password: &str) -> String {
    database_url.replacen(
        "postgres://postgres:postgres@",
        &format!("postgres://{user}:{password}@"),
        1,
    )
}

async fn create_read_only_harvest_role(database_url: &str) -> String {
    let role = format!("harvest_ro_{}", uuid::Uuid::new_v4().simple());
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect as migration owner");
    diesel::sql_query(format!(
        "CREATE ROLE {role} LOGIN PASSWORD '{READ_ONLY_TEST_PASSWORD}'"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to create read-only role");
    diesel::sql_query(format!("GRANT USAGE ON SCHEMA public TO {role}"))
        .execute(&mut conn)
        .await
        .expect("failed to grant schema usage to read-only role");
    diesel::sql_query(format!(
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to grant table reads to read-only role");

    database_url_with_credentials(database_url, &role, READ_ONLY_TEST_PASSWORD)
}

async fn create_harvest_role_without_table_select(database_url: &str) -> String {
    let role = format!("harvest_no_select_{}", uuid::Uuid::new_v4().simple());
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect as migration owner");
    diesel::sql_query(format!(
        "CREATE ROLE {role} LOGIN PASSWORD '{WRITE_NO_SELECT_TEST_PASSWORD}'"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to create no-select role");
    diesel::sql_query(format!("GRANT USAGE ON SCHEMA public TO {role}"))
        .execute(&mut conn)
        .await
        .expect("failed to grant schema usage to no-select role");
    diesel::sql_query(format!(
        "GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {role}"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to grant table writes to no-select role");
    diesel::sql_query(format!(
        "GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {role}"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to grant sequence usage to no-select role");

    database_url_with_credentials(database_url, &role, WRITE_NO_SELECT_TEST_PASSWORD)
}

async fn create_harvest_role_without_sequence_usage(database_url: &str) -> String {
    let role = format!("harvest_no_seq_{}", uuid::Uuid::new_v4().simple());
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect as migration owner");
    diesel::sql_query(format!(
        "CREATE ROLE {role} LOGIN PASSWORD '{WRITE_NO_SEQUENCE_TEST_PASSWORD}'"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to create no-sequence role");
    diesel::sql_query(format!("GRANT USAGE ON SCHEMA public TO {role}"))
        .execute(&mut conn)
        .await
        .expect("failed to grant schema usage to no-sequence role");
    diesel::sql_query(format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {role}"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to grant table writes to no-sequence role");

    database_url_with_credentials(database_url, &role, WRITE_NO_SEQUENCE_TEST_PASSWORD)
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
    let shard0_db = format!("harvest_preflight_shard0_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_preflight_shard1_{}", uuid::Uuid::new_v4().simple());

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

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        name: "echo_workflow",
        module: "tests",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        concurrency: None,
        max_input_bytes: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
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
        circuit_breaker: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    }
}

fn runtime_for(
    queues: &[&str],
    schedules: Vec<WorkflowSchedule>,
    router: ShardRouter,
    scheduler_monitor: SchedulerMonitor,
) -> HarvestApiRuntime {
    HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(
            vec![workflow_info()],
            vec![activity_info(Some("email"))],
        )),
        Arc::new(DagCatalog::new()),
        Arc::new(schedules),
        None,
        queues.iter().map(|queue| (*queue).to_string()).collect(),
        scheduler_monitor,
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        router,
    )
}

fn workflow_runtime_for(
    queues: &[&str],
    schedules: Vec<WorkflowSchedule>,
    router: ShardRouter,
    scheduler_monitor: SchedulerMonitor,
) -> HarvestApiRuntime {
    HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![workflow_info()], Vec::new())),
        Arc::new(DagCatalog::new()),
        Arc::new(schedules),
        None,
        queues.iter().map(|queue| (*queue).to_string()).collect(),
        scheduler_monitor,
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        router,
    )
}

fn api_state(
    pool: HarvestDbPool,
    runtime: HarvestApiRuntime,
    profile: &str,
    admin_auth_boundary: bool,
) -> HarvestApiState {
    let state = HarvestApiState::new();
    state.install_storage_pool(pool);
    state.install(runtime);
    state.set_deployment_profile(profile);
    state.set_admin_auth_boundary(admin_auth_boundary);
    state
}

async fn register_active_worker(pool: &DbPool, worker_id: &str, queues: &[String], shards: &[i32]) {
    let mut conn = pool.get().await.expect("worker registration connection");
    register_worker(
        &mut conn,
        worker_id,
        queues,
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

fn check_status(
    report: &autumn_harvest_plugin::preflight::PreflightReport,
    name: &str,
) -> PreflightStatus {
    report
        .checks
        .iter()
        .find(|check| check.name == name)
        .unwrap_or_else(|| panic!("missing check {name}; got {:?}", report.checks))
        .status
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

#[tokio::test]
async fn preflight_endpoint_returns_all_green_single_shard_report() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(
        &pool,
        "preflight-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("prod"));

    let (status, body) = get_json(&app, "/admin/preflight").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overall_status"], "pass");
    assert!(body["observed_at"].is_string());
    assert_eq!(body["version"]["package"], "autumn-harvest-plugin");
    assert!(
        body["checks"]
            .as_array()
            .expect("checks should be an array")
            .iter()
            .any(|check| check["name"] == "worker_coverage")
    );
}

#[tokio::test]
async fn missing_migration_on_one_shard_identifies_only_that_shard() {
    let ((shard0_url, shard1_url), _container) = setup_two_shards_with_migrations().await;
    let mut shard1_conn = <AsyncPgConnection as AsyncConnection>::establish(&shard1_url)
        .await
        .expect("shard 1 connection");
    diesel::sql_query("DELETE FROM __diesel_schema_migrations WHERE version = (SELECT max(version) FROM __diesel_schema_migrations)")
        .execute(&mut shard1_conn)
        .await
        .expect("should remove latest migration marker");

    let pool = build_two_shard_pool(&shard0_url, &shard1_url);
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "preflight-worker-0",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    register_active_worker(
        pool.pool_for(ShardId::new(1)),
        "preflight-worker-1",
        &["default".to_string(), "email".to_string()],
        &[1],
    )
    .await;
    let state = api_state(
        pool,
        runtime_for(
            &["default", "email"],
            Vec::new(),
            two_shard_router(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Fail);
    let migration = report
        .checks
        .iter()
        .find(|check| check.name == "migrations")
        .expect("migration check should exist");
    assert_eq!(migration.status, PreflightStatus::Fail);
    assert_eq!(migration.affected_shards, vec![1]);
    assert!(migration.details["shards"].as_array().unwrap().len() >= 2);
}

#[tokio::test]
async fn unreachable_shard_is_reported_without_hiding_healthy_shard() {
    let (shard0_url, _container) = setup_database_url_with_migrations().await;
    let bad_url = "postgres://postgres:postgres@127.0.0.1:1/unreachable";
    let pool = build_two_shard_pool(&shard0_url, bad_url);
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "preflight-worker-0",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let state = api_state(
        pool,
        runtime_for(
            &["default", "email"],
            Vec::new(),
            two_shard_router(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Fail);
    let shard = report
        .checks
        .iter()
        .find(|check| check.name == "shard_availability")
        .expect("shard availability check should exist");
    assert_eq!(shard.status, PreflightStatus::Fail);
    assert_eq!(shard.affected_shards, vec![1]);
    assert!(shard.details["shards"].as_array().unwrap().len() >= 2);
}

#[tokio::test]
async fn shard_availability_fails_when_role_lacks_harvest_table_write_privileges() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let admin_pool = build_test_pool(&database_url);
    register_active_worker(
        &admin_pool,
        "preflight-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let read_only_url = create_read_only_harvest_role(&database_url).await;
    let state = api_state(
        HarvestDbPool::from(build_test_pool(&read_only_url)),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    let shard = report
        .checks
        .iter()
        .find(|check| check.name == "shard_availability")
        .expect("shard availability check should exist");
    assert_eq!(shard.status, PreflightStatus::Fail);
    assert_eq!(shard.affected_shards, vec![0]);
    let observed = &shard.details["shards"].as_array().unwrap()[0];
    assert_eq!(observed["readable"], true);
    assert_eq!(observed["writable"], false);
    assert!(
        observed["missing_write_privileges"]
            .as_array()
            .expect("missing privileges should be reported")
            .iter()
            .any(|entry| entry["table"] == "harvest_task_queue")
    );
}

#[tokio::test]
async fn shard_availability_fails_when_role_lacks_harvest_table_select_privileges() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let admin_pool = build_test_pool(&database_url);
    register_active_worker(
        &admin_pool,
        "preflight-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let no_select_url = create_harvest_role_without_table_select(&database_url).await;
    let state = api_state(
        HarvestDbPool::from(build_test_pool(&no_select_url)),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    let shard = report
        .checks
        .iter()
        .find(|check| check.name == "shard_availability")
        .expect("shard availability check should exist");
    assert_eq!(shard.status, PreflightStatus::Fail);
    assert_eq!(shard.affected_shards, vec![0]);
    let observed = &shard.details["shards"].as_array().unwrap()[0];
    assert_eq!(observed["readable"], true);
    assert_eq!(observed["writable"], false);
    let missing = observed["missing_write_privileges"]
        .as_array()
        .expect("missing table privileges should be reported");
    assert!(missing.iter().any(|entry| {
        entry["table"] == "harvest_workflow_executions"
            && entry["privileges"]
                .as_array()
                .is_some_and(|privileges| privileges.iter().any(|value| value == "SELECT"))
    }));
    assert!(missing.iter().any(|entry| {
        entry["table"] == "harvest_task_queue"
            && entry["privileges"]
                .as_array()
                .is_some_and(|privileges| privileges.iter().any(|value| value == "SELECT"))
    }));
}

#[tokio::test]
async fn shard_availability_fails_when_role_lacks_harvest_sequence_usage() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let admin_pool = build_test_pool(&database_url);
    register_active_worker(
        &admin_pool,
        "preflight-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let no_sequence_url = create_harvest_role_without_sequence_usage(&database_url).await;
    let state = api_state(
        HarvestDbPool::from(build_test_pool(&no_sequence_url)),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    let shard = report
        .checks
        .iter()
        .find(|check| check.name == "shard_availability")
        .expect("shard availability check should exist");
    assert_eq!(shard.status, PreflightStatus::Fail);
    assert_eq!(shard.affected_shards, vec![0]);
    let observed = &shard.details["shards"].as_array().unwrap()[0];
    assert_eq!(observed["readable"], true);
    assert_eq!(observed["writable"], false);
    assert!(
        observed["missing_sequence_privileges"]
            .as_array()
            .expect("missing sequence privileges should be reported")
            .iter()
            .any(|entry| entry["sequence"] == "harvest_events_id_seq")
    );
}

#[tokio::test]
async fn registered_queue_without_active_worker_fails_preflight() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let state = api_state(
        HarvestDbPool::from(build_test_pool(&database_url)),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Fail);
    assert_eq!(
        check_status(&report, "worker_coverage"),
        PreflightStatus::Fail
    );
}

#[tokio::test]
async fn workflow_runtime_with_custom_queue_does_not_require_default_worker() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(&pool, "billing-worker", &["billing".to_string()], &[0]).await;
    let state = api_state(
        HarvestDbPool::from(pool),
        workflow_runtime_for(
            &["billing"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Pass);
    assert_eq!(
        check_status(&report, "worker_coverage"),
        PreflightStatus::Pass
    );
    let worker_coverage = report
        .checks
        .iter()
        .find(|check| check.name == "worker_coverage")
        .expect("worker coverage check should exist");
    assert_eq!(
        worker_coverage.details["required_queues"],
        json!(["billing"])
    );
}

#[tokio::test]
async fn stale_worker_only_coverage_fails_preflight() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(
        &pool,
        "stale-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let mut conn = pool.get().await.expect("stale update connection");
    diesel::sql_query(
        "UPDATE harvest_workers
         SET last_heartbeat_at = NOW() - INTERVAL '10 minutes',
             status = $1
         WHERE worker_id = 'stale-worker'",
    )
    .bind::<diesel::sql_types::Text, _>(WorkerStatus::Active.as_str())
    .execute(&mut conn)
    .await
    .expect("worker should be marked stale");

    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Fail);
    assert_eq!(
        check_status(&report, "worker_coverage"),
        PreflightStatus::Fail
    );
}

#[tokio::test]
async fn draining_worker_only_coverage_fails_preflight() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(
        &pool,
        "draining-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let mut conn = pool.get().await.expect("draining update connection");
    diesel::sql_query(
        "UPDATE harvest_workers
         SET status = $1
         WHERE worker_id = 'draining-worker'",
    )
    .bind::<diesel::sql_types::Text, _>(WorkerStatus::Draining.as_str())
    .execute(&mut conn)
    .await
    .expect("worker should be marked draining");

    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Fail);
    assert_eq!(
        check_status(&report, "worker_coverage"),
        PreflightStatus::Fail
    );
}

#[tokio::test]
async fn degraded_worker_is_warning_when_healthy_active_coverage_exists() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(
        &pool,
        "healthy-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    register_active_worker(
        &pool,
        "stale-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let mut conn = pool.get().await.expect("stale update connection");
    diesel::sql_query(
        "UPDATE harvest_workers
         SET last_heartbeat_at = NOW() - INTERVAL '10 minutes',
             status = $1
         WHERE worker_id = 'stale-worker'",
    )
    .bind::<diesel::sql_types::Text, _>(WorkerStatus::Active.as_str())
    .execute(&mut conn)
    .await
    .expect("worker should be marked stale");

    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Warn);
    assert_eq!(
        check_status(&report, "worker_coverage"),
        PreflightStatus::Warn
    );
}

#[tokio::test]
async fn schedule_referencing_missing_runtime_registration_fails() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(
        &pool,
        "preflight-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let mut conn = pool.get().await.expect("schedule insert connection");
    diesel::sql_query(
        "INSERT INTO harvest_schedules
            (id, dag_name, workflow_name, schedule_expr, timezone, catchup, max_active_runs, is_paused, workflow_input, queue_name)
         VALUES
            ($1, NULL, 'missing_workflow', 'interval:60', 'UTC', false, 1, false, 'null'::jsonb, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::new_v4())
    .execute(&mut conn)
    .await
    .expect("schedule insert should succeed");

    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::new(1),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Fail);
    assert_eq!(
        check_status(&report, "schedule_resolvability"),
        PreflightStatus::Fail
    );
}

#[tokio::test]
async fn admin_api_without_auth_boundary_fails_in_non_dev_profile() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(
        &pool,
        "preflight-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            Vec::new(),
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        false,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Fail);
    assert_eq!(
        check_status(&report, "admin_auth_boundary"),
        PreflightStatus::Fail
    );
}

#[tokio::test]
async fn registered_schedules_require_running_scheduler_path() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);
    register_active_worker(
        &pool,
        "preflight-worker",
        &["default".to_string(), "email".to_string()],
        &[0],
    )
    .await;
    let schedule =
        WorkflowSchedule::new("echo_workflow", Schedule::Interval(Duration::from_secs(60)));
    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["default", "email"],
            vec![schedule],
            ShardRouter::single(),
            SchedulerMonitor::offline(),
        ),
        "prod",
        true,
    );

    let report = build_preflight_report(&state).await;

    assert_eq!(report.overall_status, PreflightStatus::Fail);
    assert_eq!(
        check_status(&report, "schedule_resolvability"),
        PreflightStatus::Fail
    );
}
