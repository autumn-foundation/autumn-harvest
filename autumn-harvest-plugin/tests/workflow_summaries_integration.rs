//! HTTP integration tests for the tiered/summary retention list route (issue #752).
//!
//! Exercises `GET /workflows/summaries` end-to-end (list, filter, keyset
//! pagination, admin guard) plus the AC6 interaction: erasing a
//! summary-only execution (whose full execution row was already
//! retention-deleted) succeeds (200) and tombstones the summary's captured
//! `result`/`search_attrs`.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (run this file `--test-threads=1`, since the tests scrub
//! shared tables); otherwise a fresh testcontainers Postgres is booted with
//! `INIT_SQL` (requires Docker).

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_execution_summaries;
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
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
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
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260710000000_harvest_execution_summaries/up.sql"
    ),
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
        Some("summaries-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// An app with NO external auth boundary (built-in admin guard active). Used to
/// exercise the admin-guard rejection.
fn build_unauth_app() -> HarvestApiApp {
    harvest_api_router(HarvestApiState::new()).with_state(AppState::for_test())
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_audit_log",
        "DELETE FROM harvest_execution_summaries",
        "DELETE FROM harvest_events",
        "DELETE FROM harvest_workflow_executions",
    ] {
        let _ = diesel::sql_query(stmt).execute(conn).await;
    }
}

async fn get_json(app: &HarvestApiApp, uri: &str, admin: bool) -> (StatusCode, Value) {
    send(app, "GET", uri, None, admin).await
}

async fn post_json_admin(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    send(app, "POST", uri, Some(body), true).await
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

/// Seed one `harvest_execution_summaries` row on shard 0 and return its
/// (encoded) execution id string.
#[allow(clippy::too_many_arguments)]
async fn seed_summary(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    completed_at: DateTime<Utc>,
    search_attrs: Option<Value>,
    result: Option<Value>,
) -> String {
    let id =
        autumn_harvest::types::ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let uuid = id.as_uuid();
    diesel::insert_into(harvest_execution_summaries::table)
        .values((
            harvest_execution_summaries::execution_id.eq(uuid),
            harvest_execution_summaries::workflow_name.eq(workflow_name),
            harvest_execution_summaries::workflow_id.eq(workflow_id),
            harvest_execution_summaries::state.eq(state),
            harvest_execution_summaries::started_at.eq(completed_at - Duration::seconds(5)),
            harvest_execution_summaries::completed_at.eq(completed_at),
            harvest_execution_summaries::duration_ms.eq(Some(5_000i64)),
            harvest_execution_summaries::shard_id.eq(0i32),
            harvest_execution_summaries::search_attrs.eq(search_attrs),
            harvest_execution_summaries::result.eq(result),
            harvest_execution_summaries::error.eq(None::<String>),
        ))
        .execute(conn)
        .await
        .expect("insert summary");
    id.to_string()
}

// ── Tests ────────────────────────────────────────────────────────────────────

// AC4: the list route returns seeded summaries with summarized:true /
// history_available:false and the summary columns, newest-first.
#[tokio::test]
async fn list_returns_summaries_with_flags() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let now = Utc::now();
    seed_summary(
        &mut conn,
        "sum_wf",
        "sum-1",
        "COMPLETED",
        now - Duration::minutes(2),
        Some(json!({"tenant": "acme"})),
        Some(json!({"ok": true})),
    )
    .await;
    seed_summary(
        &mut conn,
        "sum_wf",
        "sum-2",
        "FAILED",
        now - Duration::minutes(1),
        Some(json!({"tenant": "globex"})),
        None,
    )
    .await;

    let (status, body) = get_json(&app, "/workflows/summaries", true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let items = body["summaries"].as_array().expect("summaries array");
    assert_eq!(items.len(), 2, "both summaries returned: {body}");
    // Newest-first: sum-2 (FAILED) before sum-1 (COMPLETED).
    assert_eq!(items[0]["workflow_id"], json!("sum-2"));
    assert_eq!(items[0]["summarized"], json!(true));
    assert_eq!(items[0]["history_available"], json!(false));
    assert_eq!(items[0]["state"], json!("FAILED"));
    assert_eq!(items[1]["workflow_id"], json!("sum-1"));
    assert_eq!(items[1]["result"], json!({"ok": true}));
    assert!(body["next_cursor"].is_null(), "no further page: {body}");
}

// AC4: filters by workflow_name / state / completed range / search_attr.
#[tokio::test]
async fn list_applies_filters() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let now = Utc::now();
    seed_summary(
        &mut conn,
        "billing",
        "b-1",
        "COMPLETED",
        now - Duration::hours(3),
        Some(json!({"tenant": "acme"})),
        None,
    )
    .await;
    seed_summary(
        &mut conn,
        "billing",
        "b-2",
        "FAILED",
        now - Duration::minutes(30),
        Some(json!({"tenant": "acme"})),
        None,
    )
    .await;
    seed_summary(
        &mut conn,
        "onboarding",
        "o-1",
        "COMPLETED",
        now - Duration::minutes(10),
        Some(json!({"tenant": "globex"})),
        None,
    )
    .await;

    // Filter by workflow_name.
    let (status, body) = get_json(&app, "/workflows/summaries?workflow_name=billing", true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["summaries"].as_array().unwrap().len(), 2, "{body}");

    // Filter by state.
    let (_, body) = get_json(&app, "/workflows/summaries?state=FAILED", true).await;
    let items = body["summaries"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["workflow_id"], json!("b-2"));

    // Filter by search_attr containment.
    let (_, body) = get_json(&app, "/workflows/summaries?search_attr=tenant:globex", true).await;
    let items = body["summaries"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["workflow_id"], json!("o-1"));

    // Filter by completed_after (only the two most recent). Use the Z-suffixed
    // RFC 3339 form so the timezone offset carries no `+` (axum's query parser
    // decodes a raw `+` as a space).
    let cutoff = (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (status, body) = get_json(
        &app,
        &format!("/workflows/summaries?completed_after={cutoff}"),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["summaries"].as_array().unwrap().len(), 2, "{body}");
}

// AC4: keyset pagination — limit + next_cursor round-trip yields stable,
// gap-free, dup-free ordering.
#[tokio::test]
async fn list_paginates_with_keyset_cursor() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let now = Utc::now();
    let mut expected_ids = Vec::new();
    for i in 0..5 {
        let id = seed_summary(
            &mut conn,
            "page_wf",
            &format!("p-{i}"),
            "COMPLETED",
            now - Duration::minutes(i64::from(i)),
            None,
            None,
        )
        .await;
        expected_ids.push(id);
    }
    // Newest-first order = p-0, p-1, p-2, p-3, p-4 (p-0 has the latest completed_at).

    // Page 1: limit 2.
    let (status, body) = get_json(&app, "/workflows/summaries?limit=2", true).await;
    assert_eq!(status, StatusCode::OK);
    let page1 = body["summaries"].as_array().unwrap();
    assert_eq!(page1.len(), 2);
    let cursor = body["next_cursor"].as_str().expect("next_cursor present");
    let mut seen: Vec<String> = page1
        .iter()
        .map(|r| r["execution_id"].as_str().unwrap().to_string())
        .collect();

    // Page 2.
    let (_, body) = get_json(
        &app,
        &format!("/workflows/summaries?limit=2&cursor={cursor}"),
        true,
    )
    .await;
    let page2 = body["summaries"].as_array().unwrap();
    assert_eq!(page2.len(), 2);
    let cursor2 = body["next_cursor"].as_str().expect("second next_cursor");
    seen.extend(
        page2
            .iter()
            .map(|r| r["execution_id"].as_str().unwrap().to_string()),
    );

    // Page 3 (final).
    let (_, body) = get_json(
        &app,
        &format!("/workflows/summaries?limit=2&cursor={cursor2}"),
        true,
    )
    .await;
    let page3 = body["summaries"].as_array().unwrap();
    assert_eq!(page3.len(), 1);
    assert!(body["next_cursor"].is_null(), "last page has no cursor");
    seen.extend(
        page3
            .iter()
            .map(|r| r["execution_id"].as_str().unwrap().to_string()),
    );

    // No dupes, no gaps: exactly the 5 seeded ids.
    assert_eq!(seen.len(), 5, "5 rows across 3 pages");
    let unique: std::collections::HashSet<_> = seen.iter().collect();
    assert_eq!(unique.len(), 5, "no duplicate rows across pages");
    for id in &expected_ids {
        assert!(seen.contains(id), "row {id} appears exactly once");
    }
}

// AC4: the route is admin-guarded — no admin session returns 401.
#[tokio::test]
async fn list_requires_admin() {
    let app = build_unauth_app();
    let (status, _body) = get_json(&app, "/workflows/summaries", false).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// Malformed params return 400 (never 500, never a silent empty match).
#[tokio::test]
async fn list_rejects_malformed_params() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, _) = get_json(&app, "/workflows/summaries?state=BOGUS", true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&app, "/workflows/summaries?completed_after=nope", true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&app, "/workflows/summaries?cursor=garbage", true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// AC6: erasing a summary-only execution (whose full execution row was already
// retention-deleted) returns 200 and tombstones the summary result/search_attrs.
#[tokio::test]
async fn erase_scrubs_summary_only_execution() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let now = Utc::now();
    // No harvest_workflow_executions row — only the summary remains.
    let exec_id = seed_summary(
        &mut conn,
        "erase_wf",
        "e-1",
        "COMPLETED",
        now - Duration::minutes(5),
        Some(json!({"pii": "secret-attr"})),
        Some(json!({"pii": "secret-result"})),
    )
    .await;

    let (status, body) = post_json_admin(
        &app,
        &format!("/workflows/{exec_id}/erase-payloads"),
        json!({}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "summary-only erase succeeds: {body}"
    );
    assert_eq!(body["summary_scrubbed"], json!(true), "{body}");

    // The summary's result/search_attrs are tombstoned; no original PII survives.
    let (_, list) = get_json(&app, "/workflows/summaries?workflow_name=erase_wf", true).await;
    let items = list["summaries"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let row = &items[0];
    assert_eq!(row["result"], json!({"_harvest_erased": true}), "{row}");
    assert_eq!(
        row["search_attrs"],
        json!({"_harvest_erased": true}),
        "{row}"
    );
    let raw = serde_json::to_string(&list).unwrap();
    assert!(
        !raw.contains("secret-attr") && !raw.contains("secret-result"),
        "no original PII survives: {raw}"
    );
}
