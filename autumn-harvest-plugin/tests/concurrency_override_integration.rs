//! HTTP integration tests for the concurrency limit override (issue #2407).
//!
//! `PATCH /admin/workflow-types/{workflow_name}/run-defaults` sets the
//! operator override of the `limit` of `WorkflowInfo::concurrency`. Most tests
//! map to one acceptance criterion of #2407. No worker runs. A test claims a
//! task through the queue to act as one.
//!
//! Dual-mode DB: set `HARVEST_TEST_DATABASE_URL` to a Postgres admin URL to
//! run each test in a fresh database on that cluster. Otherwise each test
//! starts a testcontainers Postgres 16.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::RetentionConfig;
use autumn_harvest::WorkflowContext;
use autumn_harvest::concurrency::{ConcurrencyOnConflict, ConcurrencyPolicy};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::queue;
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
use diesel::sql_types::{Integer, Nullable, Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

/// A deferring type: `concurrency(key = "input.tenant", limit = 2)`.
const DEFER_WF: &str = "tenant_sync";
/// A latest-wins type: `concurrency(key = "input.doc", limit = 3,
/// on_conflict = cancel_running)`.
const LATEST_WF: &str = "doc_render";
/// A type with no concurrency policy.
const PLAIN_WF: &str = "plain_report";
/// The worker build id of the API runtime, so a claim matches its starts.
const BUILD: &str = "api-only";

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
        let db_name = format!("harvest_conc_ovr_{}", Uuid::new_v4().simple());
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

fn noop_handler<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({})) })
}

fn info(name: &'static str, concurrency: Option<ConcurrencyPolicy>) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "concurrency_override_integration",
        handler: noop_handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency,
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

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            info(DEFER_WF, Some(ConcurrencyPolicy::new("input.tenant", 2))),
            info(
                LATEST_WF,
                Some(
                    ConcurrencyPolicy::new("input.doc", 3)
                        .with_on_conflict(ConcurrencyOnConflict::CancelRunning),
                ),
            ),
            info(PLAIN_WF, None),
        ],
        vec![],
    ))
}

fn build_app(pool: &DbPool) -> axum::Router {
    // A settle pass after 10 minutes never runs inside a test.
    build_app_with_settle(pool, Duration::from_secs(600))
}

fn build_app_with_settle(pool: &DbPool, settle: Duration) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_concurrency_raise_settle_delay(settle);
    // Issue #1802: set the opt-out. This test exercises the handler, not auth.
    api_state.set_allow_unauthenticated_mutations(true);
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some(BUILD.to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    ));
    harvest_api_router(api_state)
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
    format!("/admin/workflow-types/{name}/run-defaults")
}

async fn patch(app: &axum::Router, name: &str, body: Value) -> (StatusCode, Value) {
    request_json(app, "PATCH", &route(name), Some(body)).await
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

async fn start(app: &axum::Router, name: &str, input: Value) -> Uuid {
    let (status, body) = request_json(
        app,
        "POST",
        &format!("/workflows/{name}/start"),
        Some(json!({ "input": input })),
    )
    .await;
    assert!(status.is_success(), "start must succeed: {status} {body}");
    body["execution_id"]
        .as_str()
        .expect("execution_id")
        .parse()
        .expect("uuid")
}

#[derive(QueryableByName)]
struct TaskRow {
    #[diesel(sql_type = Nullable<Integer>)]
    concurrency_cap: Option<i32>,
    #[diesel(sql_type = Text)]
    state: String,
}

/// The stored cap and state of the workflow task row of a run.
async fn task(url: &str, exec_id: Uuid) -> (Option<i32>, String) {
    let mut conn = connect(url).await;
    let row: TaskRow = diesel::sql_query(
        "SELECT concurrency_cap, state FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<SqlUuid, _>(exec_id)
    .get_result(&mut conn)
    .await
    .expect("read task row");
    (row.concurrency_cap, row.state)
}

#[derive(QueryableByName)]
struct StateRow {
    #[diesel(sql_type = Text)]
    state: String,
}

async fn run_state(url: &str, exec_id: Uuid) -> String {
    let mut conn = connect(url).await;
    let row: StateRow =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<SqlUuid, _>(exec_id)
            .get_result(&mut conn)
            .await
            .expect("read run state");
    row.state
}

/// Claim one task from the `default` queue, as a worker does.
async fn claim(url: &str) -> Option<Uuid> {
    let mut conn = connect(url).await;
    queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "worker-1",
        BUILD,
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .map(|task| task.id)
}

async fn audit_statuses(url: &str) -> Vec<String> {
    use harvest_audit_log::dsl;
    let mut conn = connect(url).await;
    harvest_audit_log::table
        .filter(dsl::operation.eq("workflow_type.run_defaults"))
        .order(dsl::occurred_at.asc())
        .select(dsl::status)
        .load(&mut conn)
        .await
        .expect("audit rows")
}

/// The entry of `workflow_name` in the `workflow` group of `key` on
/// `GET /admin/concurrency`.
async fn concurrency_entry(app: &axum::Router, key: &str, workflow_name: &str) -> Value {
    let (status, body) = request_json(app, "GET", "/admin/concurrency", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let group = body
        .as_array()
        .expect("array")
        .iter()
        .find(|g| g["key"] == json!(key) && g["task_type"] == json!("workflow"))
        .unwrap_or_else(|| panic!("no workflow group for {key}: {body}"))
        .clone();
    group["workflows"]
        .as_array()
        .expect("workflows")
        .iter()
        .find(|w| w["workflow_name"] == json!(workflow_name))
        .unwrap_or_else(|| panic!("no entry for {workflow_name}: {group}"))
        .clone()
}

/// AC1: a lower limit applies to the next start, and it changes no run in
/// flight.
#[tokio::test]
async fn a_lower_limit_applies_to_the_next_start_only() {
    let (url, _db) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let in_flight = start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    assert!(claim(&url).await.is_some(), "the first run is admitted");
    assert_eq!(task(&url, in_flight).await, (Some(2), "RUNNING".into()));

    let (status, body) = patch(
        &app,
        DEFER_WF,
        json!({ "concurrency_limit": 1, "reason": "INC-7" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["concurrency_limit"], json!(2));
    assert_eq!(body["override"]["concurrency_limit"], json!(1));
    assert_eq!(body["effective"]["concurrency_limit"], json!(1));

    let next = start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    assert_eq!(
        task(&url, next).await,
        (Some(1), "PENDING".into()),
        "the next start takes the lower limit"
    );
    assert_eq!(
        task(&url, in_flight).await,
        (Some(2), "RUNNING".into()),
        "the run in flight keeps its task and its cap"
    );
    assert_eq!(run_state(&url, in_flight).await, "RUNNING");
    assert!(
        claim(&url).await.is_none(),
        "the lower limit makes the next start wait"
    );
}

/// AC1: a latest-wins type refuses every override. A latest-wins start
/// cancels runs down to its limit, so any later drop of an override, a clear
/// included, would cancel runs in flight.
#[tokio::test]
async fn a_latest_wins_type_refuses_an_override() {
    let (url, _db) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(start(&app, LATEST_WF, json!({ "doc": "d1" })).await);
    }

    for limit in [1, 3, 5] {
        let (status, body) = patch(&app, LATEST_WF, json!({ "concurrency_limit": limit })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{limit}: {body}");
    }
    assert_eq!(audit_statuses(&url).await, ["failed", "failed", "failed"]);
    let (_, view) = request_json(&app, "GET", &route(LATEST_WF), None).await;
    assert_eq!(view["override"], Value::Null, "{view}");
    assert_eq!(view["effective"]["concurrency_limit"], json!(3));

    // The code limit of 3 still holds: a fourth start cancels the oldest run
    // only, and the other runs stay in flight.
    let fourth = start(&app, LATEST_WF, json!({ "doc": "d1" })).await;
    assert_eq!(run_state(&url, runs[0]).await, "CANCELLED");
    for run in runs[1..].iter().chain([&fourth]) {
        assert_eq!(run_state(&url, *run).await, "RUNNING");
    }
    assert_eq!(task(&url, fourth).await.0, Some(3));
}

/// AC2: a higher limit admits the waiting starts at the next admission, and
/// the next start takes it.
#[tokio::test]
async fn a_higher_limit_admits_waiting_starts() {
    let (url, _db) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(start(&app, DEFER_WF, json!({ "tenant": "acme" })).await);
    }
    assert!(claim(&url).await.is_some());
    assert!(claim(&url).await.is_some());
    assert!(
        claim(&url).await.is_none(),
        "the code limit of 2 holds the third start"
    );

    let (status, body) = patch(&app, DEFER_WF, json!({ "concurrency_limit": 4 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["concurrency_raise"]["raised_tasks"],
        json!(3),
        "the waiting start and the two claimed runs: {body}"
    );
    assert_eq!(task(&url, runs[0]).await, (Some(4), "RUNNING".into()));
    assert_eq!(body["concurrency_raise"]["superseded"], json!(false));
    assert_eq!(
        body["concurrency_raise"]["unavailable_shards"],
        json!([]),
        "{body}"
    );

    assert!(
        claim(&url).await.is_some(),
        "the next admission takes the waiting start"
    );
    let next = start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    assert_eq!(task(&url, next).await, (Some(4), "PENDING".into()));
    assert!(
        claim(&url).await.is_some(),
        "the next start is admitted up to the new limit"
    );
    // Four runs now hold the key. A fifth start waits at the new limit.
    let fifth = start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    assert_eq!(task(&url, fifth).await, (Some(4), "PENDING".into()));
    assert!(claim(&url).await.is_none(), "the new limit still holds");
}

/// AC3: both read routes report the code limit, the override and the
/// effective limit.
#[tokio::test]
async fn the_reads_report_code_override_and_effective_limits() {
    let (url, _db) = setup_database().await;
    let app = build_app(&build_pool(&url));
    start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;

    let before = concurrency_entry(&app, "acme", DEFER_WF).await;
    assert_eq!(before["code_limit"], json!(2), "{before}");
    assert_eq!(before["override_limit"], Value::Null, "{before}");
    assert_eq!(before["effective_limit"], json!(2), "{before}");

    let (status, body) = patch(&app, DEFER_WF, json!({ "concurrency_limit": 6 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let after = concurrency_entry(&app, "acme", DEFER_WF).await;
    assert_eq!(after["code_limit"], json!(2), "{after}");
    assert_eq!(after["override_limit"], json!(6), "{after}");
    assert_eq!(after["effective_limit"], json!(6), "{after}");
    assert_eq!(after["on_conflict"], json!("defer"));

    let (status, view) = request_json(&app, "GET", &route(DEFER_WF), None).await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["code"]["concurrency_limit"], json!(2));
    assert_eq!(view["override"]["concurrency_limit"], json!(6));
    assert_eq!(view["effective"]["concurrency_limit"], json!(6));

    let (_, plain) = request_json(&app, "GET", &route(PLAIN_WF), None).await;
    assert_eq!(plain["code"]["concurrency_limit"], Value::Null, "{plain}");
    assert_eq!(plain["effective"]["concurrency_limit"], Value::Null);
}

/// AC4: clearing the override restores the code value, and every change is
/// audited.
#[tokio::test]
async fn clearing_restores_the_code_limit_and_each_change_is_audited() {
    let (url, _db) = setup_database().await;
    let app = build_app(&build_pool(&url));

    let (status, _) = patch(
        &app,
        DEFER_WF,
        json!({ "concurrency_limit": 5, "sla_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = patch(&app, DEFER_WF, json!({ "concurrency_limit": null })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"]["concurrency_limit"], Value::Null);
    assert_eq!(body["override"]["sla_secs"], json!(60), "other fields stay");
    assert_eq!(body["effective"]["concurrency_limit"], json!(2));
    let run = start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    assert_eq!(task(&url, run).await.0, Some(2), "a new start takes code");

    let (status, _) = patch(&app, DEFER_WF, json!({ "concurrency_limit": 7 })).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = patch(&app, DEFER_WF, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"], Value::Null);
    assert_eq!(body["effective"]["concurrency_limit"], json!(2));
    let run = start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    assert_eq!(task(&url, run).await.0, Some(2));

    let (status, _) = patch(&app, DEFER_WF, json!({ "concurrency_limit": 0 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        audit_statuses(&url).await,
        ["succeeded", "succeeded", "succeeded", "succeeded", "failed"],
        "each PATCH writes one audit row, a refused one included"
    );
}

/// The route refuses a limit that cannot apply, and audits each refusal.
#[tokio::test]
async fn the_route_refuses_a_limit_that_cannot_apply() {
    let (url, _db) = setup_database().await;
    let app = build_app(&build_pool(&url));

    let (status, body) = patch(&app, PLAIN_WF, json!({ "concurrency_limit": 3 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "no policy: {body}");
    let (status, body) = patch(
        &app,
        DEFER_WF,
        json!({ "concurrency_limit": 2_147_483_648_u64 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "past the column: {body}");
    let (status, body) = patch(&app, DEFER_WF, json!({ "concurrency_limit": -1 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "negative: {body}");
    let (status, body) = patch(&app, PLAIN_WF, json!({ "concurrency_limit": null })).await;
    assert_eq!(status, StatusCode::OK, "a null sets nothing: {body}");
    assert_eq!(
        audit_statuses(&url).await,
        ["failed", "failed", "failed", "succeeded"]
    );
}

/// AC2: the settle passes raise a start that a process stored with the old
/// cap after the first raise, for example before its cache reloaded. A later
/// pass raises a row that an earlier pass missed.
#[tokio::test]
async fn the_settle_passes_raise_a_late_start() {
    let (url, _db) = setup_database().await;
    // The passes run 0.1 s, 0.6 s and 3 s after the change.
    let app = build_app_with_settle(&build_pool(&url), Duration::from_millis(100));
    let (status, body) = patch(&app, DEFER_WF, json!({ "concurrency_limit": 5 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A start from a process with a stale cache stores the code limit.
    let late = start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    store_cap(&url, late, 2).await;
    wait_for_cap(&url, late, 5, "the first settle pass").await;

    // A row that the earlier passes missed is raised by a later one.
    store_cap(&url, late, 2).await;
    wait_for_cap(&url, late, 5, "a later settle pass").await;
}

async fn store_cap(url: &str, exec_id: Uuid, cap: i32) {
    let mut conn = connect(url).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET concurrency_cap = $2 WHERE workflow_exec_id = $1",
    )
    .bind::<SqlUuid, _>(exec_id)
    .bind::<Integer, _>(cap)
    .execute(&mut conn)
    .await
    .expect("store the old cap");
}

async fn wait_for_cap(url: &str, exec_id: Uuid, cap: i32, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while task(url, exec_id).await.0 != Some(cap) {
        assert!(
            std::time::Instant::now() < deadline,
            "{what} must raise the late start"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// AC2 and AC4: clearing a lower override raises the waiting starts back to
/// the code limit, so the next claim admits them.
#[tokio::test]
async fn clearing_a_lower_limit_admits_waiting_starts() {
    let (url, _db) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, _) = patch(&app, DEFER_WF, json!({ "concurrency_limit": 1 })).await;
    assert_eq!(status, StatusCode::OK);
    start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    let waiting = start(&app, DEFER_WF, json!({ "tenant": "acme" })).await;
    assert!(claim(&url).await.is_some());
    assert!(
        claim(&url).await.is_none(),
        "the limit of 1 holds the second start"
    );

    let (status, body) = patch(&app, DEFER_WF, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["effective"]["concurrency_limit"], json!(2));
    assert_eq!(
        body["concurrency_raise"]["raised_tasks"],
        json!(2),
        "the waiting start and the claimed run: {body}"
    );
    assert_eq!(task(&url, waiting).await, (Some(2), "PENDING".into()));
    assert!(
        claim(&url).await.is_some(),
        "the code limit admits the waiting start"
    );
}
