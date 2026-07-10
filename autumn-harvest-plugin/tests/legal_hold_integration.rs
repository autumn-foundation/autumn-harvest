//! HTTP integration tests for per-execution legal hold (issue #747).
//!
//! Exercises the `POST /workflows/{id}/legal-hold`,
//! `POST /workflows/{id}/legal-hold/release`, `GET /workflows/{id}` (describe),
//! and `GET /workflows?legal_hold=true` management routes end-to-end, plus the
//! erase-while-held 409 interaction.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (run this file `--test-threads=1`, since the tests scrub
//! shared tables); otherwise a fresh testcontainers Postgres is booted with
//! `INIT_SQL` (requires Docker).

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_audit_log;
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
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260709000000_harvest_legal_hold/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
);

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("legal-hold-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// An app with NO external auth boundary (built-in admin guard active). Used to
/// exercise the admin-guard rejection: without an admin session, admin-only
/// routes return 401 before reaching the handler (no storage needed).
fn build_unauth_app() -> HarvestApiApp {
    harvest_api_router(HarvestApiState::new()).with_state(AppState::for_test())
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_audit_log",
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_events",
        "DELETE FROM harvest_workflow_executions",
    ] {
        // Some tables may not exist in a minimal testcontainers bundle; ignore.
        let _ = diesel::sql_query(stmt).execute(conn).await;
    }
}

async fn post_json_admin(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    send(app, "POST", uri, Some(body), true).await
}

async fn post_json_noauth(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    send(app, "POST", uri, Some(body), false).await
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    send(app, "GET", uri, None, false).await
}

async fn send(
    app: &HarvestApiApp,
    method: &str,
    uri: &str,
    body: Option<Value>,
    admin: bool,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if admin {
        builder = builder.header("x-harvest-admin", "true");
    }
    let req = builder
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .unwrap();
    let response = app.clone().oneshot(req).await.expect("request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

/// Seed a COMPLETED execution (shard 0) with one `WorkflowStarted` event carrying
/// PII, so an erase attempt has something to tombstone.
async fn seed_completed(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> String {
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }
    let now = Utc::now();
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at)
         VALUES ($1, $2, 0, 'COMPLETED', '{}'::jsonb, $3, $3)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Timestamptz, _>(now)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{\"pii\":\"secret\"},\"timestamp\":\"2026-01-01T00:00:00Z\"}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert event");

    autumn_harvest::types::ExecutionId::from_uuid(id).to_string()
}

async fn audit_count(conn: &mut AsyncPgConnection, operation: &str, target: &str) -> i64 {
    harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq(operation))
        .filter(harvest_audit_log::target_id.eq(target))
        .count()
        .get_result(conn)
        .await
        .unwrap()
}

// ── Tests ────────────────────────────────────────────────────────────────────

// Set places a hold (200), writes exactly one audit row, and the describe
// endpoint reflects legal_hold:true + the hold columns. Release clears it (200).
#[tokio::test]
async fn set_describe_and_release_round_trip() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_completed(&mut conn, "lh_wf", "lh-http-1").await;

    // Set the hold.
    let (status, body) = post_json_admin(
        &app,
        &format!("/workflows/{exec_id}/legal-hold"),
        json!({ "reason": "litigation hold — case 2026-CV-1234" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["held"], json!(true));
    assert_eq!(body["newly_held"], json!(true));
    assert_eq!(
        body["legal_hold_reason"],
        json!("litigation hold — case 2026-CV-1234")
    );

    // Exactly one audit row under legal_hold.set.
    assert_eq!(
        audit_count(&mut conn, "legal_hold.set", &exec_id).await,
        1,
        "one legal_hold.set audit row"
    );

    // Describe reflects the active hold.
    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "describe body: {body}");
    assert_eq!(body["legal_hold"], json!(true), "describe legal_hold flag");
    assert_eq!(
        body["execution"]["legal_hold_reason"],
        json!("litigation hold — case 2026-CV-1234"),
        "raw hold columns surfaced under execution"
    );

    // Release the hold.
    let (status, body) = post_json_admin(
        &app,
        &format!("/workflows/{exec_id}/legal-hold/release"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "release body: {body}");
    assert_eq!(body["released"], json!(true));
    assert_eq!(body["held"], json!(false));

    // Describe now shows no hold.
    let (_, body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(body["legal_hold"], json!(false));
}

// The set (and release) routes are admin-guarded: with the built-in guard
// active and no admin session, they return 401 before reaching the handler.
#[tokio::test]
async fn set_and_release_require_admin() {
    let app = build_unauth_app();
    let exec = "00000000-0000-0000-0000-000000000001";

    let (status, _body) = post_json_noauth(
        &app,
        &format!("/workflows/{exec}/legal-hold"),
        json!({ "reason": "x" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "set is admin-guarded");

    let (status, _body) = post_json_noauth(
        &app,
        &format!("/workflows/{exec}/legal-hold/release"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "release is admin-guarded");
}

// A hold_until already in the past is rejected with 400 (no false protection).
#[tokio::test]
async fn past_hold_until_returns_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_completed(&mut conn, "lh_wf", "lh-http-past").await;
    let past: DateTime<Utc> = Utc::now() - chrono::Duration::hours(1);
    let (status, body) = post_json_admin(
        &app,
        &format!("/workflows/{exec_id}/legal-hold"),
        json!({ "reason": "x", "hold_until": past.to_rfc3339() }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");

    // Nothing was persisted: describe still shows no hold.
    let (_, body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(body["legal_hold"], json!(false));
}

// The ?legal_hold=true list filter returns the held row and composes (AND) with
// the state filter.
#[tokio::test]
async fn list_filter_legal_hold_composes_with_state() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let held = seed_completed(&mut conn, "lh_wf", "lh-held").await;
    let _unheld = seed_completed(&mut conn, "lh_wf", "lh-unheld").await;

    let (status, _b) = post_json_admin(
        &app,
        &format!("/workflows/{held}/legal-hold"),
        json!({ "reason": "hold-it" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = get_json(&app, "/workflows?legal_hold=true&state=COMPLETED").await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<String> = body
        .as_array()
        .expect("array")
        .iter()
        .map(|r| r["workflow_id"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        ids.contains(&"lh-held".to_string()),
        "held row present: {ids:?}"
    );
    assert!(
        !ids.contains(&"lh-unheld".to_string()),
        "unheld row excluded: {ids:?}"
    );
}

// Erase while held → 409 naming the hold; after release, erase → 200.
#[tokio::test]
async fn erase_while_held_is_409_then_ok_after_release() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_completed(&mut conn, "lh_wf", "lh-erase").await;
    let (status, _b) = post_json_admin(
        &app,
        &format!("/workflows/{exec_id}/legal-hold"),
        json!({ "reason": "subpoena-42" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Erase is rejected while held.
    let (status, body) = post_json_admin(
        &app,
        &format!("/workflows/{exec_id}/erase-payloads"),
        json!({ "reason": "gdpr" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    assert!(
        body.to_string().contains("legal hold"),
        "409 must name the legal hold; got {body}"
    );

    // After release, erase succeeds.
    let (status, _b) = post_json_admin(
        &app,
        &format!("/workflows/{exec_id}/legal-hold/release"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = post_json_admin(
        &app,
        &format!("/workflows/{exec_id}/erase-payloads"),
        json!({ "reason": "gdpr" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "erase after release: {body}");
    assert!(body["fields_tombstoned"].as_u64().unwrap_or(0) > 0);
}
