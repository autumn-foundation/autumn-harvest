//! HTTP tests for the `max_concurrent` override of an activity type.
//!
//! `PATCH /admin/activity-types/{activity_name}/defaults` takes
//! `max_concurrent`. A change rewrites the stored cap of the live task rows
//! of the activity. No worker runs. The DB tests in
//! `autumn-harvest/tests/integration/activity_cap_override_db_tests.rs`
//! drive the claims and a worker.
//!
//! Dual-mode DB: set `HARVEST_TEST_DATABASE_URL` to a Postgres admin URL to
//! run each test in a fresh database on that cluster. Otherwise each test
//! starts a testcontainers Postgres 16.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::RetentionConfig;
use autumn_harvest::info::ActivityInfo;
use autumn_harvest::models::TaskQueueItem;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{harvest_audit_log, harvest_task_queue};
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

/// A remote activity with a code cap of 3 under its own key.
const CAPPED: &str = "call_partner";
/// A remote activity with no code cap.
const UNCAPPED: &str = "send_mail";
/// Two remote activities that share one key.
const SHARED_A: &str = "sync_orders";
const SHARED_B: &str = "sync_refunds";
const SHARED_KEY: &str = "partner_api";
/// A local activity with a code cap.
const LOCAL: &str = "hash_inline";

const CODE_CAP: u32 = 3;

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
        let db_name = format!("harvest_act_cap_{}", Uuid::new_v4().simple());
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

fn activity(
    name: &'static str,
    cap: Option<u32>,
    key: Option<&'static str>,
    is_local: bool,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_cap_override_integration",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(30)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: cap,
        concurrency_key: key,
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

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![],
        vec![
            activity(CAPPED, Some(CODE_CAP), None, false),
            activity(UNCAPPED, None, None, false),
            activity(SHARED_A, Some(2), Some(SHARED_KEY), false),
            activity(SHARED_B, Some(2), Some(SHARED_KEY), false),
            activity(LOCAL, Some(2), None, true),
        ],
    ))
}

/// The app, its state and the registry whose cache the route writes through.
fn build_app(pool: &DbPool) -> (axum::Router, HarvestApiState, Arc<HandlerRegistry>) {
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
    (harvest_api_router(api_state.clone()), api_state, registry)
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

/// Enqueue one task of [`CAPPED`] under the code cap, as the worker does.
async fn enqueue(conn: &mut AsyncPgConnection) -> Uuid {
    let exec_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'aco_owner', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    let mut params = EnqueueParams::new("default", TaskType::Activity, json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some(CAPPED.to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.concurrency_key = Some(CAPPED.to_string());
    params.max_concurrent = Some(CODE_CAP);
    queue::enqueue(conn, &params).await.expect("enqueue")
}

/// The state and the stored cap of each row, in the order of `ids`.
async fn rows(url: &str, ids: &[Uuid]) -> Vec<(String, Option<i32>)> {
    let mut conn = connect(url).await;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let row: TaskQueueItem = harvest_task_queue::table
            .find(*id)
            .select(TaskQueueItem::as_select())
            .first(&mut conn)
            .await
            .expect("row");
        out.push((row.state, row.concurrency_cap));
    }
    out
}

#[tokio::test]
async fn get_reports_the_code_override_and_effective_cap() {
    let (url, _guard) = setup_database().await;
    let (app, _state, _registry) = build_app(&build_pool(&url));
    let (status, body) = request_json(&app, "GET", &route(CAPPED), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["max_concurrent"], CODE_CAP);
    assert_eq!(body["effective"]["max_concurrent"], CODE_CAP);
    assert_eq!(body["override"], Value::Null);
    let (_, body) = request_json(&app, "GET", &route(UNCAPPED), None).await;
    assert_eq!(body["code"]["max_concurrent"], Value::Null);
}

/// AC1, AC2 and AC3 at the route.
///
/// A lower cap rewrites the pending row and the running row, and the running
/// row stays `RUNNING`. A clear restores the code cap on both. Each change
/// writes one audit row.
#[tokio::test]
async fn a_lower_cap_rewrites_the_live_rows_and_a_clear_restores_the_code_cap() {
    let (url, _guard) = setup_database().await;
    let (app, _state, registry) = build_app(&build_pool(&url));
    let mut conn = connect(&url).await;
    let running = enqueue(&mut conn).await;
    queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "w1",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("one task runs");
    let pending = enqueue(&mut conn).await;

    let (status, body) = patch(
        &app,
        CAPPED,
        json!({ "max_concurrent": 1, "reason": "INC-91 partner throttles" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"]["max_concurrent"], 1);
    assert_eq!(body["effective"]["max_concurrent"], 1);
    assert_eq!(body["code"]["max_concurrent"], CODE_CAP);
    assert_eq!(body["cap_rewrite"]["rewritten_tasks"], 2, "{body}");
    assert_eq!(body["cap_rewrite"]["unresolved_tasks"], 0, "{body}");
    assert_eq!(body["cap_rewrite"]["unavailable_shards"], json!([]));
    assert_eq!(body["cap_rewrite"]["superseded"], false);
    assert_eq!(
        rows(&url, &[running, pending]).await,
        vec![
            ("RUNNING".to_string(), Some(1)),
            ("PENDING".to_string(), Some(1)),
        ],
        "AC1 and AC2: each live row takes the cap, and the task in flight runs on"
    );
    assert_eq!(
        registry
            .run_defaults()
            .activity(CAPPED)
            .and_then(|fields| fields.max_concurrent),
        Some(1),
        "the write-through reaches the cache"
    );

    let (status, body) = patch(&app, CAPPED, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"], Value::Null);
    assert_eq!(body["effective"]["max_concurrent"], CODE_CAP);
    assert_eq!(body["cap_rewrite"]["rewritten_tasks"], 2, "{body}");
    assert_eq!(
        rows(&url, &[running, pending]).await,
        vec![
            ("RUNNING".to_string(), Some(3)),
            ("PENDING".to_string(), Some(3)),
        ],
        "AC3: a clear restores the code cap"
    );
    assert_eq!(registry.run_defaults().activity(CAPPED), None);

    let audit = audit_rows(&url).await;
    assert_eq!(audit.len(), 2, "one audit row per change: {audit:?}");
    for (status, target, summary) in audit {
        assert_eq!(status, "succeeded");
        assert_eq!(target.as_deref(), Some(CAPPED));
        assert_eq!(summary, None);
    }
}

/// A `null` cap returns to the code cap and rewrites the rows. A change that
/// names no cap leaves the rows as they are.
#[tokio::test]
async fn only_a_change_that_names_the_cap_rewrites_rows() {
    let (url, _guard) = setup_database().await;
    let (app, _state, _registry) = build_app(&build_pool(&url));
    let mut conn = connect(&url).await;
    let id = enqueue(&mut conn).await;

    let (status, body) = patch(&app, CAPPED, json!({ "start_to_close_ms": 90_000 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["cap_rewrite"], Value::Null, "{body}");

    let (status, body) = patch(&app, CAPPED, json!({ "max_concurrent": 2 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"]["start_to_close_ms"], 90_000);
    assert_eq!(body["cap_rewrite"]["rewritten_tasks"], 1, "{body}");
    assert_eq!(rows(&url, &[id]).await[0].1, Some(2));

    let (status, body) = patch(&app, CAPPED, json!({ "max_concurrent": null })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"]["max_concurrent"], Value::Null);
    assert_eq!(
        body["override"]["start_to_close_ms"], 90_000,
        "an unnamed field stays"
    );
    assert_eq!(body["cap_rewrite"]["rewritten_tasks"], 1, "{body}");
    assert_eq!(rows(&url, &[id]).await[0].1, Some(3));

    // A clear when no cap is stored still runs the pass, which finds
    // nothing to change.
    let (status, body) = patch(&app, CAPPED, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["cap_rewrite"]["rewritten_tasks"], 0, "{body}");
    assert_eq!(rows(&url, &[id]).await[0].1, Some(3));
}

/// A settle pass rewrites a row that a process with a stale cache wrote
/// after the change.
#[tokio::test]
async fn a_settle_pass_rewrites_a_row_stored_under_the_old_cap() {
    let (url, _guard) = setup_database().await;
    let (app, api_state, _registry) = build_app(&build_pool(&url));
    api_state.set_concurrency_raise_settle_delay(Duration::from_secs(2));
    let (status, body) = patch(&app, CAPPED, json!({ "max_concurrent": 1 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["cap_rewrite"]["rewritten_tasks"], 0);

    let mut conn = connect(&url).await;
    let stale = enqueue(&mut conn).await;
    assert_eq!(rows(&url, &[stale]).await[0].1, Some(3));
    tokio::time::timeout(Duration::from_secs(10), async {
        while rows(&url, &[stale]).await[0].1 != Some(1) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a settle pass rewrites the stale row");
}

/// Each refused cap writes a failed audit row and stores nothing.
#[tokio::test]
async fn each_refused_cap_is_audited_and_stores_nothing() {
    let (url, _guard) = setup_database().await;
    let (app, _state, registry) = build_app(&build_pool(&url));
    let cases = [
        (
            UNCAPPED,
            json!({ "max_concurrent": 1 }),
            "declares no max_concurrent",
        ),
        (LOCAL, json!({ "max_concurrent": 1 }), "local"),
        (SHARED_A, json!({ "max_concurrent": 1 }), SHARED_B),
        (CAPPED, json!({ "max_concurrent": 0 }), "max_concurrent"),
        (
            CAPPED,
            json!({ "max_concurrent": 2_147_483_648_u64 }),
            "max_concurrent",
        ),
        (
            CAPPED,
            json!({ "max_concurrent": -1 }),
            "invalid request body",
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
    let audit = audit_rows(&url).await;
    assert_eq!(audit.len(), cases.len(), "{audit:?}");
    assert!(
        audit
            .iter()
            .all(|(status, _, summary)| status == "failed" && summary.is_some())
    );
    for name in [CAPPED, UNCAPPED, SHARED_A, LOCAL] {
        assert_eq!(registry.run_defaults().activity(name), None, "{name}");
        let (_, body) = request_json(&app, "GET", &route(name), None).await;
        assert_eq!(body["override"], Value::Null, "{name}");
    }
}

/// `GET /admin/overrides` shows the cap of an activity override.
#[tokio::test]
async fn the_override_list_shows_the_cap() {
    let (url, _guard) = setup_database().await;
    let (app, _state, _registry) = build_app(&build_pool(&url));
    let (status, body) = patch(&app, CAPPED, json!({ "max_concurrent": 2 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = request_json(&app, "GET", "/admin/overrides", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let entry = &body["activity_default_overrides"][0];
    assert_eq!(entry["activity_name"], CAPPED);
    assert_eq!(entry["max_concurrent"], 2);
}

/// The route frees its connection before the pass. The pass then takes the
/// one connection of a pool of one, so the rewrite does not wait on it.
#[tokio::test]
async fn a_pool_of_one_connection_runs_the_rewrite() {
    let (url, _guard) = setup_database().await;
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            &url,
        );
    let pool: DbPool = deadpool::managed::Pool::builder(manager)
        .max_size(1)
        .build()
        .expect("pool");
    let (app, _state, _registry) = build_app(&pool);
    let id = {
        let mut conn = connect(&url).await;
        enqueue(&mut conn).await
    };
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(20),
        patch(&app, CAPPED, json!({ "max_concurrent": 1 })),
    )
    .await
    .expect("the PATCH does not wait on its own connection");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["cap_rewrite"]["rewritten_tasks"], 1, "{body}");
    assert_eq!(rows(&url, &[id]).await[0].1, Some(1));
}
