//! Integration tests for issue #1213: `GET /workers?shard_id=` (and
//! `/workers/drain-preview?shard_id=`) must not return a worker whose
//! `shard_assignments` is the empty auto/legacy shape unless the requested
//! shard is the shard that worker's row was actually read from.
//!
//! `apply_worker_filters`'s `shard_assignments_cover` treats an empty array as
//! "covers whatever shard the row was read from" (issue #1150). Both
//! cross-shard read endpoints fan out to every shard's database and, before
//! this fix, applied that predicate using the *caller's requested* shard_id
//! for every shard's rows -- including shards other than the one each row was
//! read from. A worker registered with `shard_assignments: []` in shard 0
//! then leaked into `?shard_id=1` (and vice versa).
//!
//! Dual-mode: uses a running Postgres from `HARVEST_TEST_DATABASE_URL` (as an
//! admin URL onto which two fresh shard databases are created) when set — no
//! Docker required — else boots a fresh testcontainers Postgres, matching the
//! precedent in `fanout_degradation_integration.rs`.

use std::collections::BTreeMap;

use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

type HarvestApiApp = axum::Router;

/// Derive a per-shard database URL from the admin/base URL, replacing the
/// database-name path segment with `dbname` while preserving any query
/// string. Copied from `fanout_degradation_integration.rs` (each integration
/// test binary carries its own harness by repo convention).
fn shard_url(base_url: &str, dbname: &str) -> String {
    let (base, query) = match base_url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (base_url, None),
    };
    let prefix = base.rsplit_once('/').map_or(base, |(prefix, _)| prefix);
    query.map_or_else(
        || format!("{prefix}/{dbname}"),
        |q| format!("{prefix}/{dbname}?{q}"),
    )
}

/// Two independent, live shard databases plus an optional container guard.
async fn setup_two_shards() -> ((String, String), Option<ContainerAsync<Postgres>>) {
    let (admin_url, guard) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };

    let s0 = format!("harvest_shard_{}", Uuid::new_v4().simple());
    let s1 = format!("harvest_shard_{}", Uuid::new_v4().simple());

    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connect");
    for db in [&s0, &s1] {
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create db");
    }

    let url0 = shard_url(&admin_url, &s0);
    let url1 = shard_url(&admin_url, &s1);
    for url in [&url0, &url1] {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("shard connect");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migrate shard");
    }
    ((url0, url1), guard)
}

fn build_pool(url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Sharded, admin-gated app with both shards reachable.
fn build_app(url0: &str, url1: &str) -> HarvestApiApp {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(url0));
    pools.insert(ShardId::new(1), build_pool(url1));
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

/// Seed a worker row directly, with a caller-controlled `shard_assignments`
/// JSON literal (e.g. `"[]"` for the auto/legacy shape, `"[1]"` for an
/// explicit narrow assignment).
async fn seed_worker(url: &str, worker_id: &str, shard_assignments_json: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let sql = format!(
        "INSERT INTO harvest_workers \
            (worker_id, last_heartbeat_at, status, queues, shard_assignments, max_concurrency, host) \
         VALUES \
            ('{worker_id}', NOW(), 'Active', '[]'::jsonb, '{shard_assignments_json}'::jsonb, 10, 'test-host') \
         ON CONFLICT (worker_id) DO NOTHING"
    );
    conn.batch_execute(&sql).await.expect("seed worker");
}

fn worker_ids(rows: &[Value]) -> Vec<&str> {
    rows.iter().filter_map(|w| w["worker_id"].as_str()).collect()
}

// ── GET /workers?shard_id= ────────────────────────────────────────────────

#[tokio::test]
async fn shard_id_filter_excludes_an_empty_assignment_worker_from_a_different_shard() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    // `auto` registered only in shard 0 with the empty (auto/legacy) shape.
    seed_worker(&url0, "auto", "[]").await;
    // `narrow` registered only in shard 1 with an explicit assignment to shard 1.
    seed_worker(&url1, "narrow", "[1]").await;
    let app = build_app(&url0, &url1);

    let (status, body) = get_json(&app, "/workers?shard_id=1").await;
    assert_eq!(status, StatusCode::OK, "got {body}");
    let workers = body["workers"].as_array().expect("workers array");
    let ids = worker_ids(workers);
    assert_eq!(
        ids,
        vec!["narrow"],
        "an empty-assignment worker read from shard 0 must not cover a \
         request for shard 1, even though 'narrow' genuinely does: {body}"
    );
}

#[tokio::test]
async fn shard_id_filter_still_includes_an_empty_assignment_worker_from_its_own_shard() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    seed_worker(&url0, "auto", "[]").await;
    seed_worker(&url1, "narrow", "[1]").await;
    let app = build_app(&url0, &url1);

    let (status, body) = get_json(&app, "/workers?shard_id=0").await;
    assert_eq!(status, StatusCode::OK, "got {body}");
    let workers = body["workers"].as_array().expect("workers array");
    let ids = worker_ids(workers);
    assert_eq!(
        ids,
        vec!["auto"],
        "the empty-assignment worker must still cover its own source shard: {body}"
    );
}

// ── GET /workers/drain-preview?shard_id= ──────────────────────────────────

#[tokio::test]
async fn drain_preview_shard_id_filter_excludes_an_empty_assignment_worker_from_a_different_shard()
{
    let ((url0, url1), _guard) = setup_two_shards().await;
    seed_worker(&url0, "auto", "[]").await;
    seed_worker(&url1, "narrow", "[1]").await;
    let app = build_app(&url0, &url1);

    let (status, body) = get_json(&app, "/workers/drain-preview?shard_id=1").await;
    assert_eq!(status, StatusCode::OK, "got {body}");
    let items = body.as_array().expect("bare array response");
    let ids = worker_ids(items);
    assert_eq!(
        ids,
        vec!["narrow"],
        "drain-preview must apply the same source-aware shard filter as \
         /workers: {body}"
    );
}
