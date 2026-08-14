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

async fn setup_two_shards_with_migrations() -> ((String, String), ContainerAsync<Postgres>) {
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
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let shard0_db = format!("harvest_scaling_shard0_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_scaling_shard1_{}", uuid::Uuid::new_v4().simple());

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

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "echo_workflow",
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
        &std::collections::HashMap::new(),
        0,
    )
    .await
    .expect("worker registration should succeed");
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

async fn seed_task(pool: &DbPool, queue_name: &str, state: &str, scheduled_at_expr: &str) {
    let mut conn = pool.get().await.expect("task seeding connection");
    diesel::sql_query(format!(
        "INSERT INTO harvest_task_queue (
            id, queue_name, task_type, input, state, priority, max_attempts, scheduled_at
         ) VALUES (
            gen_random_uuid(), '{queue_name}', 'workflow', '{{}}'::jsonb, '{state}', 0, 1, {scheduled_at_expr}
         )"
    ))
    .execute(&mut conn)
    .await
    .expect("failed to insert test task");
}

async fn read_response_body(response: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body")
        .to_vec()
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
    let body_bytes = read_response_body(response).await;
    let json = serde_json::from_slice(&body_bytes).expect("response must be JSON");
    (status, json)
}

async fn get_text(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, String) {
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
    let body_bytes = read_response_body(response).await;
    let text = String::from_utf8(body_bytes).expect("response must be valid UTF-8");
    (status, text)
}

#[tokio::test]
async fn single_shard_scaling_signals_endpoint_correctly_categorizes_tasks_and_workers() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let pool = build_test_pool(&database_url);

    // Queue A:
    // - 1 Backlog task (PENDING, scheduled_at <= NOW)
    // - 1 Scheduled task (PENDING, scheduled_at > NOW)
    // - 2 In-flight tasks (RUNNING)
    seed_task(&pool, "queue-a", "PENDING", "NOW() - INTERVAL '5 seconds'").await;
    seed_task(&pool, "queue-a", "PENDING", "NOW() + INTERVAL '1 hour'").await;
    seed_task(&pool, "queue-a", "RUNNING", "NOW()").await;
    seed_task(&pool, "queue-a", "RUNNING", "NOW()").await;

    // Queue B:
    // - 3 Backlog tasks (PENDING, scheduled_at <= NOW)
    // - 0 Scheduled tasks
    // - 1 In-flight task (RUNNING)
    seed_task(&pool, "queue-b", "PENDING", "NOW() - INTERVAL '1 second'").await;
    seed_task(&pool, "queue-b", "PENDING", "NOW() - INTERVAL '2 seconds'").await;
    seed_task(&pool, "queue-b", "PENDING", "NOW() - INTERVAL '3 seconds'").await;
    seed_task(&pool, "queue-b", "RUNNING", "NOW()").await;

    // Escaping test queue:
    // - 1 Backlog task on queue"\\a (represented in Rust as "queue\"\\\\a")
    seed_task(
        &pool,
        "queue\"\\\\a",
        "PENDING",
        "NOW() - INTERVAL '5 seconds'",
    )
    .await;

    // Register workers:
    // - worker-1: healthy active on queue-a
    // - worker-2: healthy active on queue-a & queue-b
    // - worker-3: draining on queue-a
    // - worker-4: stale on queue-b
    // - worker-escape: healthy active on queue"\\a
    register_active_worker(&pool, "worker-1", &["queue-a"], &[0]).await;
    register_active_worker(&pool, "worker-2", &["queue-a", "queue-b"], &[0]).await;
    register_active_worker(&pool, "worker-3", &["queue-a"], &[0]).await;
    mark_worker_draining(&pool, "worker-3").await;
    register_active_worker(&pool, "worker-4", &["queue-b"], &[0]).await;
    mark_worker_stale(&pool, "worker-4").await;
    register_active_worker(&pool, "worker-escape", &["queue\"\\\\a"], &[0]).await;

    let state = api_state(
        HarvestDbPool::from(pool),
        runtime_for(
            &["queue-a", "queue-b", "queue\"\\\\a"],
            ShardRouter::single(),
        ),
    );
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    // 1. JSON scaling API format
    let (status, body) = get_json(&app, "/admin/queues/scaling").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_array());
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);

    let qa = arr
        .iter()
        .find(|q| q["queue"] == "queue-a")
        .expect("missing queue-a");
    assert_eq!(qa["backlog"], 1);
    assert_eq!(qa["in_flight"], 2);
    assert_eq!(qa["scheduled"], 1);
    assert_eq!(qa["active_workers"], 2); // worker-1 and worker-2 are healthy & active on queue-a

    let qb = arr
        .iter()
        .find(|q| q["queue"] == "queue-b")
        .expect("missing queue-b");
    assert_eq!(qb["backlog"], 3);
    assert_eq!(qb["in_flight"], 1);
    assert_eq!(qb["scheduled"], 0);
    assert_eq!(qb["active_workers"], 1); // only worker-2 is healthy & active on queue-b

    let qescape = arr
        .iter()
        .find(|q| q["queue"] == "queue\"\\\\a")
        .expect("missing escaped queue");
    assert_eq!(qescape["backlog"], 1);
    assert_eq!(qescape["in_flight"], 0);
    assert_eq!(qescape["scheduled"], 0);
    assert_eq!(qescape["active_workers"], 1);

    // 2. Query format=prometheus
    let (status_prom_q, text_prom_q) =
        get_text(&app, "/admin/queues/scaling?format=prometheus").await;
    assert_eq!(status_prom_q, StatusCode::OK);
    assert!(text_prom_q.contains("harvest_queue_backlog{queue=\"queue-a\"} 1"));
    assert!(text_prom_q.contains("harvest_queue_in_flight{queue=\"queue-a\"} 2"));
    assert!(text_prom_q.contains("harvest_queue_scheduled{queue=\"queue-a\"} 1"));
    assert!(text_prom_q.contains("harvest_queue_active_workers{queue=\"queue-a\"} 2"));
    assert!(text_prom_q.contains("harvest_queue_backlog{queue=\"queue-b\"} 3"));
    assert!(text_prom_q.contains("harvest_queue_in_flight{queue=\"queue-b\"} 1"));
    assert!(text_prom_q.contains("harvest_queue_scheduled{queue=\"queue-b\"} 0"));
    assert!(text_prom_q.contains("harvest_queue_active_workers{queue=\"queue-b\"} 1"));
    assert!(text_prom_q.contains("harvest_queue_backlog{queue=\"queue\\\"\\\\\\\\a\"} 1"));
    assert!(text_prom_q.contains("harvest_queue_active_workers{queue=\"queue\\\"\\\\\\\\a\"} 1"));

    // 3. /admin/metrics endpoint
    let (status_metrics, text_metrics) = get_text(&app, "/admin/metrics").await;
    assert_eq!(status_metrics, StatusCode::OK);
    assert!(text_metrics.contains("harvest_queue_backlog{queue=\"queue-a\"} 1"));
    assert!(text_metrics.contains("harvest_queue_in_flight{queue=\"queue-a\"} 2"));
    assert!(text_metrics.contains("harvest_queue_scheduled{queue=\"queue-a\"} 1"));
    assert!(text_metrics.contains("harvest_queue_active_workers{queue=\"queue-a\"} 2"));
    assert!(text_metrics.contains("harvest_queue_backlog{queue=\"queue-b\"} 3"));
    assert!(text_metrics.contains("harvest_queue_in_flight{queue=\"queue-b\"} 1"));
    assert!(text_metrics.contains("harvest_queue_scheduled{queue=\"queue-b\"} 0"));
    assert!(text_metrics.contains("harvest_queue_active_workers{queue=\"queue-b\"} 1"));
    assert!(text_metrics.contains("harvest_queue_backlog{queue=\"queue\\\"\\\\\\\\a\"} 1"));
    assert!(text_metrics.contains("harvest_queue_active_workers{queue=\"queue\\\"\\\\\\\\a\"} 1"));
}

#[tokio::test]
async fn multi_shard_scaling_signals_are_correctly_aggregated_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_two_shards_with_migrations().await;
    let pool = build_two_shard_pool(&shard0_url, &shard1_url);

    // Shard 0:
    // - 1 Backlog task on queue-a
    // - 1 healthy active worker on queue-a
    seed_task(
        pool.pool_for(ShardId::new(0)),
        "queue-a",
        "PENDING",
        "NOW() - INTERVAL '5 seconds'",
    )
    .await;
    register_active_worker(
        pool.pool_for(ShardId::new(0)),
        "worker-shard-0",
        &["queue-a"],
        &[0],
    )
    .await;

    // Shard 1:
    // - 2 Backlog tasks on queue-a
    // - 1 In-flight task on queue-a
    // - 1 healthy active worker on queue-a
    seed_task(
        pool.pool_for(ShardId::new(1)),
        "queue-a",
        "PENDING",
        "NOW() - INTERVAL '10 seconds'",
    )
    .await;
    seed_task(
        pool.pool_for(ShardId::new(1)),
        "queue-a",
        "PENDING",
        "NOW() - INTERVAL '2 seconds'",
    )
    .await;
    seed_task(
        pool.pool_for(ShardId::new(1)),
        "queue-a",
        "RUNNING",
        "NOW()",
    )
    .await;
    register_active_worker(
        pool.pool_for(ShardId::new(1)),
        "worker-shard-1",
        &["queue-a"],
        &[1],
    )
    .await;

    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let state = api_state(pool, runtime_for(&["queue-a"], router));
    let app = harvest_api_router(state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/queues/scaling").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_array());
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);

    let qa = &arr[0];
    assert_eq!(qa["queue"], "queue-a");
    assert_eq!(qa["backlog"], 3); // 1 from shard 0 + 2 from shard 1
    assert_eq!(qa["in_flight"], 1); // 1 from shard 1
    assert_eq!(qa["scheduled"], 0);
    assert_eq!(qa["active_workers"], 2); // 1 worker-shard-0 + 1 worker-shard-1
}
