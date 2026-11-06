//! HTTP tests for the activity default override.
//!
//! `GET` and `PATCH /admin/activity-types/{activity_name}/defaults` read and
//! change the operator override of the five activity defaults. No worker
//! runs. The DB tests in
//! `autumn-harvest/tests/integration/activity_default_override_db_tests.rs`
//! drive the schedules.
//!
//! Dual-mode DB: set `HARVEST_TEST_DATABASE_URL` to a Postgres admin URL to
//! run each test in a fresh database on that cluster. Otherwise each test
//! starts a testcontainers Postgres 16.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::RetentionConfig;
use autumn_harvest::info::ActivityInfo;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_audit_log;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

/// A remote activity that declares all five defaults.
const CHARGE: &str = "charge_card";
/// A remote activity that declares none.
const PLAIN: &str = "send_receipt";
/// A local activity with a retry policy and a `start_to_close`.
const LOCAL: &str = "checksum";

/// Keeps a test database alive: its container, if the test started one.
struct DbGuard {
    _container: Option<Box<ContainerAsync<Postgres>>>,
}

fn with_db(base: &str, db: &str) -> String {
    let (before, _after) = base.rsplit_once('/').expect("url has a database segment");
    format!("{before}/{db}")
}

async fn setup_database() -> (String, DbGuard) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_act_defaults_{}", Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        diesel::sql_query(format!("CREATE DATABASE \"{db_name}\""))
            .execute(&mut admin)
            .await
            .expect("create test database");
        let url = with_db(&admin_url, &db_name);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect test db");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("run migrations");
        return (url, DbGuard { _container: None });
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let guard = DbGuard {
        _container: Some(Box::new(container)),
    };
    (url, guard)
}

fn build_pool(url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn activity(name: &'static str, is_local: bool, full: bool) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_defaults_integration",
        default_retry_policy: full.then(code_policy),
        default_start_to_close: full.then_some(Duration::from_secs(30)),
        default_heartbeat_timeout: (full && !is_local).then_some(Duration::from_millis(1_500)),
        default_schedule_to_start: (full && !is_local).then_some(Duration::from_secs(60)),
        default_schedule_to_close: (full && !is_local).then_some(Duration::from_secs(600)),
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        is_local,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        input_schema: None,
        output_schema: None,
    }
}

fn code_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 5,
        initial_interval: Duration::from_millis(250),
        backoff_coefficient: 2.0,
        max_interval: Duration::from_secs(10),
        non_retryable_errors: vec!["Fatal".to_string()],
        jitter: JitterPolicy::None,
    }
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![],
        vec![
            activity(CHARGE, false, true),
            activity(PLAIN, false, false),
            activity(LOCAL, true, true),
        ],
    ))
}

/// The app, and the registry whose cache the route writes through.
fn build_app(pool: &DbPool) -> (axum::Router, Arc<HandlerRegistry>) {
    let registry = registry();
    let api_state = HarvestApiState::new();
    // This test exercises the handler, not auth.
    api_state.set_allow_unauthenticated_mutations(true);
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("api-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    ));
    (harvest_api_router(api_state), registry)
}

async fn request_json(
    app: &axum::Router,
    method: &str,
    uri: &str,
    payload: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if payload.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let body = payload.map_or_else(Body::empty, |payload| Body::from(payload.to_string()));
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

fn route(name: &str) -> String {
    format!("/admin/activity-types/{name}/defaults")
}

async fn patch(app: &axum::Router, name: &str, body: Value) -> (StatusCode, Value) {
    request_json(app, "PATCH", &route(name), Some(body)).await
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

/// The status and the summary of each `activity_type.defaults` audit row,
/// oldest first.
async fn audit_rows(url: &str) -> Vec<(String, Option<String>, Option<String>)> {
    use harvest_audit_log::dsl;
    let mut conn = connect(url).await;
    harvest_audit_log::table
        .filter(dsl::operation.eq("activity_type.defaults"))
        .order(dsl::occurred_at.asc())
        .select((dsl::status, dsl::target_id, dsl::error_summary))
        .load(&mut conn)
        .await
        .expect("audit rows")
}

#[tokio::test]
async fn get_reports_the_code_override_and_effective_values() {
    let (url, _guard) = setup_database().await;
    let (app, _registry) = build_app(&build_pool(&url));
    let (status, body) = request_json(&app, "GET", &route(CHARGE), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["activity_name"], CHARGE);
    assert_eq!(body["local"], false);
    assert_eq!(body["code"]["retry_max_attempts"], 5);
    assert_eq!(body["code"]["retry_initial_interval_ms"], 250);
    assert_eq!(body["code"]["retry_max_interval_ms"], 10_000);
    assert_eq!(body["code"]["start_to_close_ms"], 30_000);
    assert_eq!(body["code"]["heartbeat_timeout_ms"], 1_500);
    assert_eq!(body["code"]["schedule_to_start_ms"], 60_000);
    assert_eq!(body["code"]["schedule_to_close_ms"], 600_000);
    assert_eq!(body["override"], Value::Null);
    assert_eq!(body["effective"], body["code"]);

    let (status, body) = request_json(&app, "GET", &route("no_such_activity"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

/// AC1 in this process, and AC4: a change applies at once, a clear restores
/// the code values, and each change writes one audit row.
#[tokio::test]
async fn a_patch_applies_at_once_and_a_clear_restores_the_code_values() {
    let (url, _guard) = setup_database().await;
    let (app, registry) = build_app(&build_pool(&url));
    let (status, body) = patch(
        &app,
        CHARGE,
        json!({
            "retry_max_attempts": 1,
            "start_to_close_ms": 90_000,
            "heartbeat_timeout_ms": 4_000,
            "reason": "INC-77 downstream is slow",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"]["retry_max_attempts"], 1);
    assert_eq!(body["override"]["start_to_close_ms"], 90_000);
    assert_eq!(body["override"]["schedule_to_start_ms"], Value::Null);
    assert_eq!(body["override"]["reason"], "INC-77 downstream is slow");
    assert!(body["override"]["updated_by"].is_string(), "{body}");
    assert_eq!(body["effective"]["retry_max_attempts"], 1);
    assert_eq!(body["effective"]["retry_initial_interval_ms"], 250);
    assert_eq!(body["effective"]["start_to_close_ms"], 90_000);
    assert_eq!(body["effective"]["heartbeat_timeout_ms"], 4_000);
    assert_eq!(body["effective"]["schedule_to_start_ms"], 60_000);
    assert_eq!(body["code"]["start_to_close_ms"], 30_000);

    let cached = registry
        .run_defaults()
        .activity(CHARGE)
        .expect("the write-through reaches the cache");
    assert_eq!(cached.start_to_close, Some(Duration::from_secs(90)));
    assert_eq!(cached.retry.max_attempts, Some(1));

    let (status, body) = patch(&app, CHARGE, json!({ "start_to_close_ms": null })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"]["start_to_close_ms"], Value::Null);
    assert_eq!(
        body["override"]["retry_max_attempts"], 1,
        "an unnamed field stays"
    );
    assert_eq!(body["effective"]["start_to_close_ms"], 30_000);

    let (status, body) = patch(&app, CHARGE, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"], Value::Null);
    assert_eq!(body["effective"], body["code"]);
    assert_eq!(registry.run_defaults().activity(CHARGE), None);

    let (status, body) = request_json(&app, "GET", &route(CHARGE), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"], Value::Null);

    let rows = audit_rows(&url).await;
    assert_eq!(rows.len(), 3, "one audit row per change: {rows:?}");
    for (status, target, summary) in rows {
        assert_eq!(status, "succeeded");
        assert_eq!(target.as_deref(), Some(CHARGE));
        assert_eq!(summary, None);
    }
}

/// Each refused change writes a failed audit row and stores nothing.
#[tokio::test]
async fn each_refused_change_is_audited_and_stores_nothing() {
    let (url, _guard) = setup_database().await;
    let (app, registry) = build_app(&build_pool(&url));
    let cases = [
        (PLAIN, json!({ "retry_max_attempts": 2 }), "retry_policy"),
        (
            PLAIN,
            json!({ "heartbeat_timeout_ms": 1_000 }),
            "heartbeat_timeout",
        ),
        (LOCAL, json!({ "schedule_to_start_ms": 1_000 }), "local"),
        (LOCAL, json!({ "schedule_to_close_ms": 1_000 }), "local"),
        (LOCAL, json!({ "heartbeat_timeout_ms": 1_000 }), "local"),
        (CHARGE, json!({ "start_to_close_ms": 0 }), "at least"),
        (
            CHARGE,
            json!({ "retry_initial_interval_ms": 5_000, "retry_max_interval_ms": 1_000 }),
            "retry_max_interval_ms",
        ),
        (CHARGE, json!({}), "empty patch"),
        (CHARGE, json!({ "bogus": 1 }), "invalid request body"),
        (
            "__harvest_session_acquire",
            json!({ "start_to_close_ms": 1_000 }),
            "reserved",
        ),
    ];
    for (name, body, needle) in &cases {
        let (status, response) = patch(&app, name, body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name} {body}: {response}");
        let text = response.to_string();
        assert!(
            text.contains(needle),
            "{name} {body}: {text} lacks {needle}"
        );
    }
    let (status, _) = patch(
        &app,
        "no_such_activity",
        json!({ "start_to_close_ms": 1_000 }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let rows = audit_rows(&url).await;
    assert_eq!(rows.len(), cases.len() + 1, "{rows:?}");
    assert!(
        rows.iter()
            .all(|(status, _, summary)| status == "failed" && summary.is_some())
    );
    for name in [CHARGE, PLAIN, LOCAL] {
        assert_eq!(registry.run_defaults().activity(name), None, "{name}");
        let (_, body) = request_json(&app, "GET", &route(name), None).await;
        assert_eq!(body["override"], Value::Null, "{name}");
    }
}

/// A local activity takes a retry and a `start_to_close` override.
#[tokio::test]
async fn a_local_activity_takes_its_two_supported_fields() {
    let (url, _guard) = setup_database().await;
    let (app, _registry) = build_app(&build_pool(&url));
    let (status, body) = patch(
        &app,
        LOCAL,
        json!({ "retry_max_attempts": 2, "start_to_close_ms": 5_000 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["local"], true);
    assert_eq!(body["effective"]["retry_max_attempts"], 2);
    assert_eq!(body["effective"]["start_to_close_ms"], 5_000);
}

/// `GET /admin/overrides` lists the activity overrides, and
/// `GET /admin/config` counts them.
#[tokio::test]
async fn the_override_list_names_each_activity_override() {
    let (url, _guard) = setup_database().await;
    let (app, _registry) = build_app(&build_pool(&url));
    let (status, body) = request_json(&app, "GET", "/admin/overrides", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["activity_default_overrides"], json!([]));

    let (status, body) = patch(
        &app,
        CHARGE,
        json!({ "schedule_to_close_ms": 1_200_000, "reason": "longer" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = request_json(&app, "GET", "/admin/overrides", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let entry = &body["activity_default_overrides"][0];
    assert_eq!(entry["activity_name"], CHARGE);
    assert_eq!(entry["schedule_to_close_ms"], 1_200_000);
    assert_eq!(entry["reason"], "longer");
    assert!(entry["updated_at"].is_string(), "{entry}");

    let (status, body) = request_json(&app, "GET", "/admin/config", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["overrides"]["activity_default_overrides"], json!(1));
}
