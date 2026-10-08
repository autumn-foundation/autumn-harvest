//! HTTP integration tests for fairness key weights (issue #1976).
//!
//! Drives the three management routes end to end against a real Postgres:
//!
//!   * `GET    /admin/queues/{queue_name}/fairness`
//!   * `POST   /admin/queues/{queue_name}/fairness/{fairness_key}`
//!   * `DELETE /admin/queues/{queue_name}/fairness/{fairness_key}`
//!
//! Coverage:
//!   (a) Set returns the stored override and the read shows it.
//!   (b) Clear removes it, and a second clear is a no-op.
//!   (c) Both mutations write an audit row with the actor.
//!   (d) A bad weight, a bad key or a bad body is a 400 with a failed audit row.
//!   (e) The read shows the claim state of each key.
//!   (f) Without the admin boundary, all three routes are a 401.

#![allow(clippy::too_many_lines)]
#![allow(clippy::doc_markdown)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

/// Actor identity sent as `x-harvest-actor`.
const TEST_ACTOR: &str = "alice@ops";

const SET_ROUTE: &str = "POST /admin/queues/{queue_name}/fairness/{fairness_key}";
const CLEAR_ROUTE: &str = "DELETE /admin/queues/{queue_name}/fairness/{fairness_key}";

/// Swap the database name of a Postgres URL and keep any query string.
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

/// Make a new, migrated database for one test.
///
/// With `HARVEST_TEST_DATABASE_URL` set, the test makes a new database on that
/// server. Otherwise it starts a Postgres container, which needs Docker.
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

    let fresh_db = format!("harvest_fairw_test_{}", uuid::Uuid::new_v4().simple());
    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {fresh_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("create fresh test database");

    let db_url = replace_db_name(&admin_url, &fresh_db);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&db_url)
        .await
        .expect("connect to fresh test database");
    diesel_async::SimpleAsyncConnection::batch_execute(&mut conn, &autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations to fresh test database");

    (db_url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool, admin_boundary: bool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    if admin_boundary {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("fairness-weights-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state)
}

async fn send(
    app: &HarvestApiApp,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-harvest-admin", "true")
        .header("x-harvest-actor", TEST_ACTOR);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let body = body.map_or_else(Body::empty, |value| Body::from(value.to_string()));
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .expect("request");
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

#[derive(diesel::QueryableByName, Debug)]
struct AuditRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    actor: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_type: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    route_or_command: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error_summary: Option<String>,
}

async fn audit_rows(pool: &DbPool, operation: &str) -> Vec<AuditRow> {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(
        "SELECT actor, target_type, target_id, status, route_or_command, error_summary \
         FROM harvest_audit_log WHERE operation = $1 ORDER BY occurred_at",
    )
    .bind::<diesel::sql_types::Text, _>(operation)
    .load::<AuditRow>(&mut conn)
    .await
    .expect("load audit rows")
}

// ── (a) + (b) + (c): set, read, clear, with audit ─────────────────────────────

#[tokio::test]
async fn set_show_clear_round_trip_with_audit() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, true);

    // ── set ──────────────────────────────────────────────────────────────
    let (status, body) = send(
        &app,
        "POST",
        "/admin/queues/email-workers/fairness/tenant-a",
        Some(json!({ "weight": 4.0 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set body: {body}");
    assert_eq!(body["queue_name"], "email-workers");
    assert_eq!(body["fairness_key"], "tenant-a");
    assert_eq!(body["weight"], 4.0);
    assert_eq!(body["updated_by"], TEST_ACTOR, "the actor is recorded");
    assert!(body["updated_at"].is_string(), "updated_at: {body}");
    assert_eq!(body["ok"], true);
    assert_eq!(body["status"], "complete");
    assert!(body.get("partial_failures").is_none(), "body: {body}");

    // ── a change to the same key replaces the weight ─────────────────────
    let (status, body) = send(
        &app,
        "POST",
        "/admin/queues/email-workers/fairness/tenant-a",
        Some(json!({ "weight": 2.5 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second set body: {body}");
    assert_eq!(body["weight"], 2.5);

    // ── show ─────────────────────────────────────────────────────────────
    let (status, body) = send(&app, "GET", "/admin/queues/email-workers/fairness", None).await;
    assert_eq!(status, StatusCode::OK, "show body: {body}");
    assert_eq!(body["queue_name"], "email-workers");
    assert_eq!(body["status"], "complete");
    assert_eq!(body["weights_uniform"], true);
    let weights = body["weights"].as_array().expect("weights array");
    assert_eq!(weights.len(), 1, "one override: {body}");
    assert_eq!(weights[0]["fairness_key"], "tenant-a");
    assert_eq!(weights[0]["weight"], 2.5);
    assert_eq!(weights[0]["updated_by"], TEST_ACTOR);
    assert!(
        body["state"].as_array().expect("state array").is_empty(),
        "no claim ran, so no state: {body}"
    );

    // ── another queue is not affected ────────────────────────────────────
    let (status, body) = send(&app, "GET", "/admin/queues/other/fairness", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["weights"], json!([]), "body: {body}");

    // ── clear ────────────────────────────────────────────────────────────
    let (status, body) = send(
        &app,
        "DELETE",
        "/admin/queues/email-workers/fairness/tenant-a",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "clear body: {body}");
    assert_eq!(body["queue_name"], "email-workers");
    assert_eq!(body["fairness_key"], "tenant-a");
    assert_eq!(body["cleared"], true);
    assert_eq!(body["ok"], true);
    assert_eq!(body["status"], "complete");

    let (_, body) = send(&app, "GET", "/admin/queues/email-workers/fairness", None).await;
    assert!(
        body["weights"].as_array().expect("weights").is_empty(),
        "the override is gone: {body}"
    );

    // ── a second clear is a no-op ────────────────────────────────────────
    let (status, body) = send(
        &app,
        "DELETE",
        "/admin/queues/email-workers/fairness/tenant-a",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "repeat clear body: {body}");
    assert_eq!(body["cleared"], false);

    // ── audit ────────────────────────────────────────────────────────────
    let sets = audit_rows(&pool, "fairness.weight.set").await;
    assert_eq!(sets.len(), 2, "each set is audited");
    for row in &sets {
        assert_eq!(row.actor, TEST_ACTOR);
        assert_eq!(row.target_type, "queue");
        assert_eq!(row.target_id, "email-workers");
        assert_eq!(row.status, "succeeded");
        assert_eq!(row.route_or_command, SET_ROUTE);
    }
    let context = sets[0].error_summary.as_deref().unwrap_or_default();
    assert!(
        context.contains("tenant-a") && context.contains("weight: 4"),
        "the audit row names the key and the weight: {context}"
    );

    let clears = audit_rows(&pool, "fairness.weight.clear").await;
    assert_eq!(clears.len(), 2, "each clear is audited, a no-op included");
    for row in &clears {
        assert_eq!(row.actor, TEST_ACTOR);
        assert_eq!(row.target_type, "queue");
        assert_eq!(row.target_id, "email-workers");
        assert_eq!(row.status, "succeeded");
        assert_eq!(row.route_or_command, CLEAR_ROUTE);
    }
    assert!(
        clears[0]
            .error_summary
            .as_deref()
            .unwrap_or_default()
            .contains("tenant-a"),
        "the audit row names the key"
    );
}

// ── (d): rejected input ───────────────────────────────────────────────────────

#[tokio::test]
async fn invalid_weight_key_or_body_is_rejected_400_and_audited() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, true);

    let cases = [
        ("tenant-a", json!({ "weight": 0.0 }), "weight 0"),
        ("tenant-a", json!({ "weight": -1.0 }), "negative weight"),
        (
            "tenant-a",
            json!({ "weight": 1001.0 }),
            "weight above the cap",
        ),
        (
            "%20tenant-a",
            json!({ "weight": 2.0 }),
            "key with outer space",
        ),
        ("%20", json!({ "weight": 2.0 }), "blank key"),
        (
            "tenant%01a",
            json!({ "weight": 2.0 }),
            "key with a control character",
        ),
        (
            "tenant-a",
            json!({ "weight": 2.0, "wieght": 3.0 }),
            "unknown field",
        ),
        ("tenant-a", json!({}), "missing weight"),
    ];
    for (key, body, label) in &cases {
        let (status, response) = send(
            &app,
            "POST",
            &format!("/admin/queues/email-workers/fairness/{key}"),
            Some(body.clone()),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{label} must be a 400: {response}"
        );
    }

    let (status, response) = send(
        &app,
        "DELETE",
        "/admin/queues/email-workers/fairness/%20",
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a blank key on clear must be a 400: {response}"
    );

    let (status, response) = send(&app, "GET", "/admin/queues/%20/fairness", None).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a blank queue name on the read must be a 400: {response}"
    );

    let (_, body) = send(&app, "GET", "/admin/queues/email-workers/fairness", None).await;
    assert!(
        body["weights"].as_array().expect("weights").is_empty(),
        "no rejected request stored a weight: {body}"
    );

    let sets = audit_rows(&pool, "fairness.weight.set").await;
    assert_eq!(sets.len(), cases.len(), "each rejected set is audited");
    for row in &sets {
        assert_eq!(row.status, "failed");
        assert_eq!(row.actor, TEST_ACTOR);
        assert_eq!(row.route_or_command, SET_ROUTE);
        assert!(row.error_summary.is_some(), "the reason is recorded");
    }
    let clears = audit_rows(&pool, "fairness.weight.clear").await;
    assert_eq!(clears.len(), 1, "the rejected clear is audited");
    assert_eq!(clears[0].status, "failed");
}

// ── (e): claim state ──────────────────────────────────────────────────────────

#[tokio::test]
async fn show_reports_the_claim_state_of_each_key() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, true);

    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "INSERT INTO harvest_fairness_state (queue_name, fairness_key, pass, last_start) \
             VALUES ('email-workers', 'tenant-a', 10.0, 9.0), \
                    ('email-workers', 'tenant-b', 9.5, 8.0), \
                    ('other', 'tenant-a', 1.0, 1.0)",
        )
        .execute(&mut conn)
        .await
        .expect("seed fairness state");
    }

    let (status, body) = send(&app, "GET", "/admin/queues/email-workers/fairness", None).await;
    assert_eq!(status, StatusCode::OK, "show body: {body}");
    let state = body["state"].as_array().expect("state array");
    assert_eq!(state.len(), 2, "only the rows of this queue: {body}");
    // The clock V is 9. tenant-b has lag 0.5 and tenant-a has lag 1.
    assert_eq!(state[0]["fairness_key"], "tenant-b", "smallest lag first");
    assert_eq!(state[0]["lag"], 0.5);
    assert_eq!(state[1]["fairness_key"], "tenant-a");
    assert_eq!(state[1]["lag"], 1.0);
    for row in state {
        assert_eq!(row["shard_id"], 0, "each row names its shard: {row}");
        assert_eq!(row["queue_name"], "email-workers");
        assert!(row["pass"].is_number() && row["last_start"].is_number());
    }
}

// ── (f): admin gate ───────────────────────────────────────────────────────────

#[tokio::test]
async fn all_three_routes_require_admin() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, false);

    for (method, body) in [
        ("GET", None),
        ("POST", Some(json!({ "weight": 2.0 }))),
        ("DELETE", None),
    ] {
        let uri = if method == "GET" {
            "/admin/queues/email-workers/fairness"
        } else {
            "/admin/queues/email-workers/fairness/tenant-a"
        };
        // No admin header: the built-in guard must refuse the request.
        let mut builder = Request::builder().method(method).uri(uri);
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let body = body.map_or_else(Body::empty, |value| Body::from(value.to_string()));
        let response = app
            .clone()
            .oneshot(builder.body(body).unwrap())
            .await
            .expect("request");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri} must be admin-gated"
        );
    }
}
