//! Admin maintenance writes respect the cross-region DR fence (issue #1823).
//!
//! The management API pins each shard generation like a worker does. After a
//! failover bumps the generation, every mutating admin route on this process
//! is refused. Read routes still answer, so an operator can inspect the node.
//!
//! The fence registry is process-global. This file is its own test binary, and
//! one mutex serializes its tests, so no other suite sees the pins.

use std::sync::Arc;

use autumn_harvest::replication::{
    FenceRegistry, ShardGeneration, bump_generation, ensure_generation_row,
};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// Serializes the tests in this binary. Each one pins the global registry.
static REGISTRY_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn replace_db_name(url: &str, db: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |q| format!("{prefix}/{db}?{q}"),
    )
}

/// A fresh, migrated database on `HARVEST_TEST_DATABASE_URL` or a container.
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    let (admin_url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let fresh_db = format!("harvest_drfence_test_{}", uuid::Uuid::new_v4().simple());
    let mut admin_conn = AsyncPgConnection::establish(&admin_url)
        .await
        .expect("connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {fresh_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("create fresh test database");
    let db_url = replace_db_name(&admin_url, &fresh_db);
    let mut conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("connect to fresh test database");
    diesel_async::SimpleAsyncConnection::batch_execute(&mut conn, &autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations");
    (db_url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("dr-fence-admin-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state)
}

async fn send(app: &axum::Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", "oncall")
                .body(if method == "GET" {
                    Body::empty()
                } else {
                    Body::from(body.to_string())
                })
                .unwrap(),
        )
        .await
        .expect("request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()));
    (status, json)
}

async fn paused_queue_count(pool: &DbPool, queue: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = pool.get().await.expect("conn");
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_queue_pauses WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .get_result::<Count>(&mut conn)
        .await
        .expect("count")
        .n
}

/// An admin maintenance write on a fenced shard is rejected, and writes
/// nothing.
#[tokio::test]
async fn an_admin_maintenance_write_against_a_fenced_shard_is_rejected() {
    let _serial = REGISTRY_SERIAL.lock().await;
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    {
        let mut conn = pool.get().await.expect("conn");
        ensure_generation_row(&mut conn, ShardId::new(0))
            .await
            .expect("provision generation 0");
    }
    FenceRegistry::clear();
    FenceRegistry::publish(
        &[(ShardId::new(0), ShardGeneration::INITIAL)],
        ShardId::new(0),
    )
    .expect("no conflicting pin in this test");

    // The current epoch admits the write.
    let (status, body) = send(
        &app,
        "POST",
        "/admin/queues/before-failover/pause",
        json!({"reason": "drill"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "pause before the bump: {body}");

    // Failover: another region now holds write authority.
    {
        let mut conn = pool.get().await.expect("conn");
        bump_generation(&mut conn, ShardId::new(0), "failover", "oncall")
            .await
            .expect("bump");
    }

    let (status, body) = send(
        &app,
        "POST",
        "/admin/queues/after-failover/pause",
        json!({"reason": "drill"}),
    )
    .await;
    let read_status = send(&app, "GET", "/admin/queues/paused", Value::Null)
        .await
        .0;
    let written = paused_queue_count(&pool, "after-failover").await;
    FenceRegistry::clear();

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a fenced process must refuse an admin write: {body}"
    );
    assert!(
        body.to_string().contains("fenced"),
        "the refusal names the fence: {body}"
    );
    assert_eq!(written, 0, "a refused admin write must write nothing");
    assert_eq!(read_status, StatusCode::OK, "read routes still answer");
}

/// A process that pinned nothing is not fenced. The check costs nothing there.
#[tokio::test]
async fn an_unpinned_process_admits_admin_writes() {
    let _serial = REGISTRY_SERIAL.lock().await;
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    FenceRegistry::clear();

    let (status, body) = send(
        &app,
        "POST",
        "/admin/queues/unfenced/pause",
        json!({"reason": "drill"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(paused_queue_count(&pool, "unfenced").await, 1);
}
