//! Integration tests for `GET /admin/canary` (issue #796).
//!
//! Exercises the synthetic liveness canary freshness read model end-to-end: the
//! route is admin-gated (401 without admin); a seeded recent COMPLETED canary
//! execution reports a non-stale probe for its `(queue, shard)`; an old (or
//! never-succeeded) canary execution reports `stale: true`; and with the canary
//! disabled the endpoint returns an empty `complete` report rather than an
//! error.
//!
//! **Distinct from the #512 replay canary** (`replay_canary_integration.rs`),
//! which validates code changes by replaying histories — this reports the
//! running pipeline's liveness.
//!
//! These tests use testcontainers by default, and are compile-checked in
//! sandboxes without a Docker daemon (matching the #543/#544/#601 precedent).
//! For a Docker-less run against a local Postgres, set
//! `HARVEST_TEST_DATABASE_URL` to a base (migrated or empty) database whose role
//! can `CREATE DATABASE`; each `setup_single_shard()` call then provisions a
//! fresh, uniquely-named, migrated database, preserving the per-test isolation
//! the testcontainers path gives (these tests assert on global `(queue, shard)`
//! success/failure aggregates, so they cannot share a database).

use std::time::Duration;

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::canary::CanaryConfig;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

const CANARY_WORKFLOW: &str = "__harvest_canary_probe__default";

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

async fn setup_single_shard() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (provision_ephemeral_db(&base_url).await, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// Provision a fresh, uniquely-named, migrated database off `base_url`, giving
/// each test the same per-test isolation the testcontainers path provides. The
/// base role must be able to `CREATE DATABASE`.
async fn provision_ephemeral_db(base_url: &str) -> String {
    use diesel_async::SimpleAsyncConnection;

    let db_name = format!("harvest_canary_{}", Uuid::new_v4().simple());
    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(base_url)
        .await
        .expect("connect to base database");
    diesel::sql_query(format!("CREATE DATABASE \"{db_name}\""))
        .execute(&mut admin)
        .await
        .expect("create ephemeral database");

    let (prefix, _old_db) = base_url
        .rsplit_once('/')
        .expect("base url must carry a /<database> path segment");
    let new_url = format!("{prefix}/{db_name}");

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&new_url)
        .await
        .expect("connect to ephemeral database");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("apply migration bundle to ephemeral database");
    new_url
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

/// Admin-gated router with the canary enabled (a 30s interval → 60s staleness
/// window, `"default"` probe queue). The auth boundary is declared so
/// `/admin/canary` reaches the handler.
fn build_app_with_canary(url: &str) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(build_pool(url)));
    api_state.set_canary_config(Some(CanaryConfig::new(Duration::from_secs(30))));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Admin-gated router with the canary **disabled** (no config mirrored).
fn build_app_canary_disabled(url: &str) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(build_pool(url)));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Router with NO auth boundary — used to prove the admin guard rejects.
fn build_unauthenticated_app(url: &str) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(build_pool(url)));
    api_state.set_canary_config(Some(CanaryConfig::new(Duration::from_secs(30))));
    harvest_api_router(api_state).with_state(AppState::for_test())
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

/// Seed a canary execution on shard 0, forcing its `state` and (optionally)
/// `completed_at`. `completed_ago_secs = Some(n)` sets `completed_at = NOW() -
/// n seconds` (and `started_at` one second earlier so the probe has a
/// well-defined roundtrip); `None` leaves it in flight.
async fn seed_canary_execution(
    url: &str,
    queue: &str,
    state: &str,
    completed_ago_secs: Option<i64>,
) -> Uuid {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let wf_id = format!("{CANARY_WORKFLOW}-{}", Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: CANARY_WORKFLOW,
        workflow_id: &wf_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({ "queue": queue }),
        parent_id: None,
        queue_name: queue,
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert");
    diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(exec_id.as_uuid())))
        .set(dsl::state.eq(state))
        .execute(&mut conn)
        .await
        .expect("force state");
    if let Some(ago) = completed_ago_secs {
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
             SET started_at = NOW() - (($1 + 1) * INTERVAL '1 second'), \
                 completed_at = NOW() - ($1 * INTERVAL '1 second') \
             WHERE id = $2",
        )
        .bind::<diesel::sql_types::BigInt, _>(ago)
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("set timing");
    }
    exec_id.as_uuid()
}

fn probe_for<'a>(body: &'a Value, queue: &str) -> &'a Value {
    body["probes"]
        .as_array()
        .expect("probes array")
        .iter()
        .find(|p| p["queue"] == queue)
        .unwrap_or_else(|| panic!("expected a probe for queue {queue}: {body}"))
}

// ── (a) admin guard ─────────────────────────────────────────────────────────

#[tokio::test]
async fn admin_canary_requires_admin() {
    let (url, _c) = setup_single_shard().await;
    let app = build_unauthenticated_app(&url);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/canary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// ── (b) fresh probe ─────────────────────────────────────────────────────────

#[tokio::test]
async fn admin_canary_reports_fresh_probe() {
    let (url, _c) = setup_single_shard().await;
    // A probe that completed just now — well within the 60s staleness window.
    seed_canary_execution(&url, "default", "COMPLETED", Some(1)).await;
    let app = build_app_with_canary(&url);

    let (status, body) = get_json(&app, "/admin/canary").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    assert_eq!(body["staleness_window_secs"], 60);
    let probe = probe_for(&body, "default");
    assert_eq!(probe["stale"], false, "a fresh probe must not be stale");
    assert_eq!(probe["shard"], 0);
    assert!(
        probe["last_success_at"].is_string(),
        "last_success_at present: {probe}"
    );
    assert_eq!(probe["success_count"], 1);
    assert_eq!(probe["failure_count"], 0);
    assert!(
        probe["last_roundtrip_ms"].as_i64().unwrap() >= 0,
        "roundtrip present: {probe}"
    );
    assert!(body["unavailable_shards"].as_array().unwrap().is_empty());
}

// ── (c) stale probe ─────────────────────────────────────────────────────────

#[tokio::test]
async fn admin_canary_reports_stale_probe_for_old_success() {
    let (url, _c) = setup_single_shard().await;
    // Last success was an hour ago — far past the 60s staleness window.
    seed_canary_execution(&url, "default", "COMPLETED", Some(3600)).await;
    let app = build_app_with_canary(&url);

    let (status, body) = get_json(&app, "/admin/canary").await;
    assert_eq!(status, StatusCode::OK);
    let probe = probe_for(&body, "default");
    assert_eq!(probe["stale"], true, "an hour-old success must be stale");
    assert!(probe["age_of_last_success_secs"].as_i64().unwrap() >= 3600);
    assert_eq!(probe["success_count"], 1);
}

#[tokio::test]
async fn admin_canary_reports_stale_probe_for_never_succeeded() {
    let (url, _c) = setup_single_shard().await;
    // Only a FAILED probe — never any success ⇒ stale, no last_success_at.
    seed_canary_execution(&url, "default", "FAILED", Some(2)).await;
    let app = build_app_with_canary(&url);

    let (status, body) = get_json(&app, "/admin/canary").await;
    assert_eq!(status, StatusCode::OK);
    let probe = probe_for(&body, "default");
    assert_eq!(probe["stale"], true);
    assert!(probe["last_success_at"].is_null());
    assert_eq!(probe["success_count"], 0);
    assert_eq!(probe["failure_count"], 1);
}

// ── (d) disabled ────────────────────────────────────────────────────────────

#[tokio::test]
async fn admin_canary_disabled_returns_empty_report() {
    let (url, _c) = setup_single_shard().await;
    // No canary rows, no canary config mirrored onto the runtime.
    let app = build_app_canary_disabled(&url);

    let (status, body) = get_json(&app, "/admin/canary").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    assert!(
        body["probes"].as_array().unwrap().is_empty(),
        "disabled canary reports no probes: {body}"
    );
}
