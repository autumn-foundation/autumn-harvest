//! HTTP integration tests for the quota limit override (issue #2408).
//!
//! `PATCH /admin/workflow-types/{workflow_name}/run-defaults` sets the
//! operator override of the three admission caps of `WorkflowInfo::quota`.
//! Most tests map to one acceptance criterion of #2408. No worker runs, so a
//! started run stays in flight.
//!
//! Dual-mode DB: set `HARVEST_TEST_DATABASE_URL` to a Postgres admin URL to
//! run each test in a fresh database on that cluster. Otherwise each test
//! starts a testcontainers Postgres 16.

use std::sync::Arc;

use autumn_harvest::RetentionConfig;
use autumn_harvest::WorkflowContext;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::quota::QuotaPolicy;
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
use diesel::sql_types::{Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

/// A type with `quota(key = "input.tenant", max_active_executions = 2)`.
const QUOTA_WF: &str = "tenant_import";
/// A type with a quota key and no admission cap.
const CAPLESS_WF: &str = "tenant_audit";
/// A type with no quota policy.
const PLAIN_WF: &str = "plain_report";
/// The worker build id of the API runtime.
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
        let db_name = format!("harvest_quota_ovr_{}", Uuid::new_v4().simple());
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

fn info(name: &'static str, quota: Option<QuotaPolicy>) -> WorkflowInfo {
    WorkflowInfo {
        quota,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "quota_override_integration",
        handler: noop_handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
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
            info(
                QUOTA_WF,
                Some(QuotaPolicy::new("input.tenant").with_max_active_executions(2)),
            ),
            info(CAPLESS_WF, Some(QuotaPolicy::new("input.tenant"))),
            info(PLAIN_WF, None),
        ],
        vec![],
    ))
}

/// A DAG. It declares no quota policy.
const DAG: &str = "tenant_rollup";

fn dag_catalog() -> DagCatalog {
    autumn_harvest::compile_dag_catalog(vec![autumn_harvest::info::DagInfo {
        name: DAG,
        module: "quota_override_integration",
        schedule: Some(autumn_harvest::policy::Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: None,
        builder: |_dag| {},
        workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
        jitter: std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
        execution_timeout: None,
        sla: None,
    }])
    .expect("dag compiles")
}

fn build_app(pool: &DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    // Issue #1802: set the opt-out. This test exercises the handler, not auth.
    api_state.set_allow_unauthenticated_mutations(true);
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry(),
        Arc::new(dag_catalog()),
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

/// Serializes the tests. A start reads the quota policy and the override
/// through process globals, which each `HandlerRegistry::new` replaces.
static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_SERIAL.lock().await
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

async fn try_start(app: &axum::Router, name: &str, tenant: &str) -> (StatusCode, Value) {
    request_json(
        app,
        "POST",
        &format!("/workflows/{name}/start"),
        Some(json!({ "input": { "tenant": tenant } })),
    )
    .await
}

async fn start(app: &axum::Router, name: &str, tenant: &str) -> Uuid {
    let (status, body) = try_start(app, name, tenant).await;
    assert!(status.is_success(), "start must succeed: {status} {body}");
    body["execution_id"]
        .as_str()
        .expect("execution_id")
        .parse()
        .expect("uuid")
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

async fn task_states(url: &str, exec_id: Uuid) -> Vec<String> {
    let mut conn = connect(url).await;
    let rows: Vec<StateRow> =
        diesel::sql_query("SELECT state FROM harvest_task_queue WHERE workflow_exec_id = $1")
            .bind::<SqlUuid, _>(exec_id)
            .load(&mut conn)
            .await
            .expect("read task rows");
    rows.into_iter().map(|row| row.state).collect()
}

/// The `error_summary` of each failed run-defaults audit row, oldest first.
async fn audit_failures(url: &str) -> Vec<String> {
    use harvest_audit_log::dsl;
    let mut conn = connect(url).await;
    harvest_audit_log::table
        .filter(dsl::operation.eq("workflow_type.run_defaults"))
        .filter(dsl::status.eq("failed"))
        .order(dsl::occurred_at.asc())
        .select(dsl::error_summary)
        .load::<Option<String>>(&mut conn)
        .await
        .expect("audit rows")
        .into_iter()
        .map(Option::unwrap_or_default)
        .collect()
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

async fn quotas(app: &axum::Router) -> Value {
    let (status, body) = request_json(app, "GET", "/admin/quotas", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

/// The usage entry of one `(workflow_name, quota_key)` pair.
fn usage_entry<'a>(body: &'a Value, workflow_name: &str, key: &str) -> &'a Value {
    body["quotas"]
        .as_array()
        .expect("quotas array")
        .iter()
        .find(|entry| entry["workflow_name"] == workflow_name && entry["quota_key"] == key)
        .unwrap_or_else(|| panic!("no entry for {workflow_name}/{key}: {body}"))
}

/// The `workflow_types` entry of one type.
fn type_entry<'a>(body: &'a Value, workflow_name: &str) -> &'a Value {
    body["workflow_types"]
        .as_array()
        .expect("workflow_types array")
        .iter()
        .find(|entry| entry["workflow_name"] == workflow_name)
        .unwrap_or_else(|| panic!("no workflow_types entry for {workflow_name}: {body}"))
}

/// AC1: a lower limit refuses new work for a tenant over it, and never ends
/// a run in flight.
#[tokio::test]
async fn a_lower_limit_refuses_new_work_and_keeps_the_runs_in_flight() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let first = start(&app, QUOTA_WF, "acme").await;
    let second = start(&app, QUOTA_WF, "acme").await;

    let (status, body) = patch(
        &app,
        QUOTA_WF,
        json!({ "quota_max_active_executions": 1, "reason": "INC-7: acme floods" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["quota_max_active_executions"], 2);
    assert_eq!(body["override"]["quota_max_active_executions"], 1);
    assert_eq!(body["effective"]["quota_max_active_executions"], 1);

    let (status, body) = try_start(&app, QUOTA_WF, "acme").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["resource"], "active_executions");
    assert_eq!(body["limit"], 1, "the lower limit refuses");
    assert_eq!(body["current"], 2);

    for exec_id in [first, second] {
        assert_eq!(run_state(&url, exec_id).await, "RUNNING", "no run ends");
        assert_eq!(
            task_states(&url, exec_id).await,
            ["PENDING"],
            "no task ends"
        );
    }
    // A tenant under the lower limit still starts.
    start(&app, QUOTA_WF, "globex").await;
}

/// A higher limit admits a start that the code limit refuses.
#[tokio::test]
async fn a_higher_limit_admits_new_work() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    start(&app, QUOTA_WF, "acme").await;
    start(&app, QUOTA_WF, "acme").await;
    let (status, _) = try_start(&app, QUOTA_WF, "acme").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

    let (status, body) = patch(&app, QUOTA_WF, json!({ "quota_max_active_executions": 3 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    start(&app, QUOTA_WF, "acme").await;
}

/// AC2: `GET /admin/quotas` reports the effective limit and names the
/// override.
#[tokio::test]
async fn admin_quotas_reports_the_effective_limit_and_names_the_override() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));

    // Before any usage, the type entry shows the code values.
    let body = quotas(&app).await;
    let entry = type_entry(&body, QUOTA_WF);
    assert_eq!(entry["limits"]["max_active_executions"], 2);
    assert_eq!(entry["code_limits"]["max_active_executions"], 2);
    assert_eq!(entry["override"], Value::Null);
    let listed: Vec<&str> = body["workflow_types"]
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|entry| entry["workflow_name"].as_str())
        .collect();
    assert_eq!(listed, [QUOTA_WF], "only a type with an admission cap");

    start(&app, QUOTA_WF, "acme").await;
    let (status, body) = patch(
        &app,
        QUOTA_WF,
        json!({
            "quota_max_active_executions": 1,
            "quota_max_dead_letters": 4,
            "reason": "INC-7",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let body = quotas(&app).await;
    for entry in [
        usage_entry(&body, QUOTA_WF, "acme"),
        type_entry(&body, QUOTA_WF),
    ] {
        assert_eq!(entry["limits"]["max_active_executions"], 1, "{entry}");
        assert_eq!(entry["limits"]["max_dead_letters"], 4);
        assert_eq!(entry["limits"]["max_history_bytes"], Value::Null);
        assert_eq!(entry["code_limits"]["max_active_executions"], 2);
        assert_eq!(entry["code_limits"]["max_dead_letters"], Value::Null);
        let named = &entry["override"];
        assert_eq!(named["max_active_executions"], 1);
        assert_eq!(named["max_dead_letters"], 4);
        assert_eq!(named["max_history_bytes"], Value::Null);
        assert_eq!(named["reason"], "INC-7");
        assert_eq!(named["applies"], true);
        assert!(named["updated_by"].is_string());
        assert!(named["updated_at"].is_string());
    }
    let usage = usage_entry(&body, QUOTA_WF, "acme");
    assert_eq!(usage["usage"]["active_executions"], 1);
    assert_eq!(body["override_source"], "table", "read from the stored row");
}

/// AC3: clearing the override restores the code value. Every change is
/// audited.
#[tokio::test]
async fn clearing_restores_the_code_value_and_each_change_is_audited() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    start(&app, QUOTA_WF, "acme").await;

    let (status, _) = patch(&app, QUOTA_WF, json!({ "quota_max_active_executions": 1 })).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = try_start(&app, QUOTA_WF, "acme").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

    let (status, _) = patch(&app, PLAIN_WF, json!({ "quota_max_active_executions": 1 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, body) = patch(&app, QUOTA_WF, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"], Value::Null);
    assert_eq!(body["effective"]["quota_max_active_executions"], 2);
    start(&app, QUOTA_WF, "acme").await;

    let body = quotas(&app).await;
    let entry = usage_entry(&body, QUOTA_WF, "acme");
    assert_eq!(entry["limits"]["max_active_executions"], 2);
    assert_eq!(entry["override"], Value::Null);

    assert_eq!(
        audit_statuses(&url).await,
        ["succeeded", "failed", "succeeded"],
        "each PATCH writes one audit row, a refused one included"
    );
}

/// A `null` returns one cap to code and keeps the others.
#[tokio::test]
async fn a_null_returns_one_cap_to_code() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, _) = patch(
        &app,
        QUOTA_WF,
        json!({ "quota_max_active_executions": 5, "quota_max_history_bytes": 4096 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = patch(
        &app,
        QUOTA_WF,
        json!({ "quota_max_active_executions": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["effective"]["quota_max_active_executions"], 2);
    assert_eq!(body["effective"]["quota_max_history_bytes"], 4096);

    let (status, body) = request_json(&app, "GET", &route(QUOTA_WF), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["override"]["quota_max_history_bytes"], 4096);
    assert_eq!(body["code"]["quota_max_history_bytes"], Value::Null);

    let (status, body) = request_json(&app, "GET", "/admin/overrides", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let listed = body["run_default_overrides"]
        .as_array()
        .expect("array")
        .iter()
        .find(|entry| entry["workflow_name"] == QUOTA_WF)
        .expect("the override is listed");
    assert_eq!(listed["quota_max_history_bytes"], 4096);
    assert_eq!(listed["quota_max_active_executions"], Value::Null);
}

/// The route refuses a cap that cannot apply, and audits each refusal with
/// its reason.
#[tokio::test]
async fn a_cap_that_cannot_apply_is_refused_and_audited() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let no_cap = "declares no quota admission cap";
    let refused = [
        (PLAIN_WF, json!({ "quota_max_dead_letters": 1 }), no_cap),
        (
            CAPLESS_WF,
            json!({ "quota_max_active_executions": 1 }),
            no_cap,
        ),
        (DAG, json!({ "quota_max_history_bytes": 1 }), no_cap),
        (
            QUOTA_WF,
            json!({ "quota_max_active_executions": 0 }),
            "quota_max_active_executions must be at least 1",
        ),
        (
            QUOTA_WF,
            json!({ "quota_max_history_bytes": 0 }),
            "quota_max_history_bytes must be at least 1",
        ),
        (
            QUOTA_WF,
            json!({ "quota_max_history_bytes": 9_223_372_036_854_775_808_u64 }),
            "quota_max_history_bytes must be at most 9223372036854775807",
        ),
        (
            QUOTA_WF,
            json!({ "quota_max_dead_letters": 4_294_967_296_u64 }),
            "invalid request body",
        ),
        (
            QUOTA_WF,
            json!({ "quota_max_active_executions": -1 }),
            "invalid request body",
        ),
    ];
    for (name, body, _) in &refused {
        let (status, reply) = patch(&app, name, body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name} {body}: {reply}");
    }
    let failures = audit_failures(&url).await;
    assert_eq!(failures.len(), refused.len(), "one audit row per refusal");
    for ((name, body, reason), summary) in refused.iter().zip(&failures) {
        assert!(
            summary.contains(reason),
            "{name} {body}: {summary:?} names {reason:?}"
        );
    }
    assert!(
        audit_statuses(&url)
            .await
            .iter()
            .all(|status| status == "failed")
    );
    // A null sets no cap, so it is not refused, even on a type with no cap.
    let (status, body) = patch(&app, PLAIN_WF, json!({ "quota_max_dead_letters": null })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
