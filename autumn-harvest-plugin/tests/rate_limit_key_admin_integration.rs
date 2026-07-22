#![allow(clippy::similar_names)]
#![allow(clippy::items_after_statements)]
//! HTTP integration test for per-key activity rate limits (issue #699), AC7.
//!
//! A dynamic per-key rate-limit bucket (`dyn-rate:{expr}:{tenant}`) is stored in
//! the same `harvest_rate_limit_buckets` table as static and throttle buckets,
//! so it is enumerated by the existing read-only `GET /admin/rate-limits`
//! management endpoint with no new surface. This test proves a composite key is
//! visible to operators there.

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
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

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
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
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(vec![], vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("rlk-admin-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn admin_get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
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
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

async fn seed_bucket(url: &str, key: &str) {
    let mut conn = diesel_async::AsyncPgConnection::establish(url)
        .await
        .expect("raw connect");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, 50.0, 20.0, 20.0, NOW()) ON CONFLICT (key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(&mut conn)
    .await
    .expect("seed bucket");
}

#[tokio::test]
async fn dynamic_composite_key_is_listed_by_admin_rate_limits() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // A dynamic per-key bucket, exactly as the enqueue path would lazily register:
    // each component is a self-delimiting length-tagged literal (`L9:tenant_id`,
    // `L4:acme`) and the `input.` prefix is normalized away.
    let composite = "dyn-rate:L9:tenant_id:L4:acme";
    seed_bucket(&url, composite).await;
    // A static bucket for good measure.
    seed_bucket(&url, "send_email").await;

    let (status, body) = admin_get(&app, "/admin/rate-limits").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let arr = body.as_array().expect("array of buckets");
    let keys: Vec<&str> = arr
        .iter()
        .filter_map(|b| b.get("key").and_then(Value::as_str))
        .collect();
    assert!(
        keys.contains(&composite),
        "composite dynamic key must be enumerated by GET /admin/rate-limits; got {keys:?}"
    );
    assert!(
        keys.contains(&"send_email"),
        "static key still enumerated; got {keys:?}"
    );
    // The listed composite entry carries the seeded config.
    let entry = arr
        .iter()
        .find(|b| b.get("key").and_then(Value::as_str) == Some(composite))
        .expect("composite entry present");
    assert_eq!(entry.get("burst"), Some(&json!(20.0)));
}
