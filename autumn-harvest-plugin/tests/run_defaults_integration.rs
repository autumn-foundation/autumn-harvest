//! HTTP integration tests for the run-default overrides (issues #2387 and
//! #2406).
//!
//! `GET` and `PATCH /admin/workflow-types/{workflow_name}/run-defaults` set
//! the operator override of the `sla`, the `execution_timeout` and the
//! `chain_execution_timeout` of one type. Each test maps to one acceptance
//! criterion of #2387 or #2406. No worker runs, so a
//! started run stays `RUNNING` and its deadlines stay as the start wrote them.
//!
//! `GET /admin/overrides` and the `overrides` count on `GET /admin/config`
//! (issue #2388) are tested here too, because they read these overrides.

use autumn_harvest::schema::harvest_workflow_executions_ext;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::RetentionConfig;
use autumn_harvest::WorkflowContext;
use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::effective_config::{EffectiveConfigView, PayloadCapsView, PoolConfigView};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{harvest_audit_log, harvest_workflow_executions};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

const WF: &str = "nightly_etl";
const OTHER_WF: &str = "hourly_report";
const THREE_HOURS: Duration = Duration::from_hours(3);
/// A type that declares `chain_execution_timeout = 1h` in code (issue #2406).
const CHAIN_WF: &str = "looping_poller";
const HOUR: Duration = Duration::from_hours(1);

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            database_url,
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

fn info(name: &'static str, sla: Option<Duration>) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "run_defaults_integration",
        handler: noop_handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla,
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

/// `WF` and `OTHER_WF` declare `sla = 3h` in code. `CHAIN_WF` declares a
/// one-hour chain cap.
fn registry() -> Arc<HandlerRegistry> {
    let mut chain = info(CHAIN_WF, None);
    chain.chain_execution_timeout = Some(HOUR);
    Arc::new(HandlerRegistry::new(
        vec![
            info(WF, Some(THREE_HOURS)),
            info(OTHER_WF, Some(THREE_HOURS)),
            chain,
        ],
        vec![],
    ))
}

fn build_app(pool: &DbPool) -> axum::Router {
    build_app_with_admin(pool, true)
}

fn build_app_with_admin(pool: &DbPool, admin: bool) -> axum::Router {
    build_app_with(pool, admin, None)
}

fn build_app_with(pool: &DbPool, admin: bool, chain_ceiling: Option<Duration>) -> axum::Router {
    build_app_from(pool, admin, chain_ceiling, DagCatalog::default())
}

/// A DAG named `DAG` with no declared durations.
const DAG: &str = "nightly_dag";

fn dag_catalog() -> DagCatalog {
    autumn_harvest::compile_dag_catalog(vec![autumn_harvest::info::DagInfo {
        name: DAG,
        module: "run_defaults_integration",
        schedule: Some(autumn_harvest::policy::Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: None,
        builder: |_dag| {},
        workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
        jitter: Duration::ZERO,
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

fn build_app_from(
    pool: &DbPool,
    admin: bool,
    chain_ceiling: Option<Duration>,
    dags: DagCatalog,
) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_max_workflow_chain_timeout(chain_ceiling);
    // Issue #1802: set the opt-out. This test exercises the handler, not auth.
    api_state.set_allow_unauthenticated_mutations(true);
    api_state.set_admin_auth_boundary(admin);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(
        HarvestApiRuntime::new(
            registry(),
            Arc::new(dags),
            Arc::new(Vec::new()),
            Some("api-only".to_string()),
            vec!["default".to_string()],
            SchedulerMonitor::offline(),
            HarvestRetentionRuntime::disabled(RetentionConfig::default()),
            ShardRouter::single(),
        )
        .with_effective_config(effective_config()),
    );
    harvest_api_router(api_state)
}

/// A snapshot for `GET /admin/config`. Its values are not under test.
fn effective_config() -> EffectiveConfigView {
    EffectiveConfigView::capture(
        &WorkerConfig::default(),
        PayloadCapsView::new(
            1024,
            1024,
            1024,
            1024,
            1024,
            None,
            None,
            None,
            Duration::from_secs(3600),
            100,
            false,
            1024,
        ),
        &ShardRouter::single(),
        PoolConfigView {
            worker_pool_max_connections: 4,
            shard_pool_count: 1,
        },
        Duration::from_millis(500),
        None,
        None,
    )
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
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn start(app: &axum::Router, name: &str) -> Uuid {
    let (status, body) = request_json(
        app,
        "POST",
        &format!("/workflows/{name}/start"),
        Some(json!({ "input": {} })),
    )
    .await;
    assert!(status.is_success(), "start must succeed: {status} {body}");
    body["execution_id"]
        .as_str()
        .expect("execution_id")
        .parse()
        .expect("uuid")
}

struct Deadlines {
    started_at: DateTime<Utc>,
    sla_deadline_at: Option<DateTime<Utc>>,
    deadline_at: Option<DateTime<Utc>>,
    sla_breached: bool,
}

/// The span from `started_at` to `at`, to the nearest second.
///
/// The start path takes its clock reading a few milliseconds before the row
/// default `started_at`, so an exact comparison would be off by that gap.
fn whole_secs(started_at: DateTime<Utc>, at: DateTime<Utc>) -> chrono::Duration {
    chrono::Duration::seconds(((at - started_at).num_milliseconds() + 500).div_euclid(1000))
}

impl Deadlines {
    fn sla(&self) -> Option<chrono::Duration> {
        self.sla_deadline_at
            .map(|at| whole_secs(self.started_at, at))
    }

    fn timeout(&self) -> Option<chrono::Duration> {
        self.deadline_at.map(|at| whole_secs(self.started_at, at))
    }
}

async fn deadlines(url: &str, id: Uuid) -> Deadlines {
    use harvest_workflow_executions::dsl;
    let mut conn = connect(url).await;
    let (started_at, sla_deadline_at, deadline_at, sla_breached) =
        harvest_workflow_executions::table
            .find(id)
            .select((
                dsl::started_at,
                harvest_workflow_executions_ext::sla_deadline_at,
                dsl::deadline_at,
                harvest_workflow_executions_ext::sla_breached,
            ))
            .first::<(
                DateTime<Utc>,
                Option<DateTime<Utc>>,
                Option<DateTime<Utc>>,
                bool,
            )>(&mut conn)
            .await
            .expect("load execution");
    Deadlines {
        started_at,
        sla_deadline_at,
        deadline_at,
        sla_breached,
    }
}

const fn secs(n: i64) -> Option<chrono::Duration> {
    chrono::Duration::try_seconds(n)
}

async fn audit_statuses(conn: &mut AsyncPgConnection) -> Vec<String> {
    use harvest_audit_log::dsl;
    harvest_audit_log::table
        .filter(dsl::operation.eq("workflow_type.run_defaults"))
        .select(dsl::status)
        .load(conn)
        .await
        .expect("audit rows")
}

/// AC1: an override `sla = 5m` on a type declared `sla = 3h` gives a new run
/// `sla_deadline_at = started_at + 5m`.
#[tokio::test]
async fn an_override_sla_sets_the_deadline_of_a_new_start() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    let before = start(&app, WF).await;
    assert_eq!(deadlines(&url, before).await.sla(), secs(3 * 3600));

    let (status, body) = patch(&app, WF, json!({ "sla_secs": 300, "reason": "tighter" })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["sla_secs"], json!(3 * 3600));
    assert_eq!(body["override"]["sla_secs"], json!(300));
    assert_eq!(body["override"]["reason"], json!("tighter"));
    assert_eq!(body["effective"]["sla_secs"], json!(300));

    let after = start(&app, WF).await;
    assert_eq!(deadlines(&url, after).await.sla(), secs(300));

    // The override is per type. The other type keeps its code value.
    let other = start(&app, OTHER_WF).await;
    assert_eq!(deadlines(&url, other).await.sla(), secs(3 * 3600));

    let (status, body) = request_json(&app, "GET", &route(WF), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["effective"]["sla_secs"], json!(300));
}

/// AC2 and AC3: a run in flight keeps its deadline. The opt-in recompute
/// moves only the non-terminal runs of the type, and the breach scanner then
/// fires on the new deadline.
#[tokio::test]
async fn the_recompute_moves_in_flight_runs_and_the_scanner_fires() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    let running = start(&app, WF).await;
    let completed = start(&app, WF).await;
    let other = start(&app, OTHER_WF).await;
    {
        use harvest_workflow_executions::dsl;
        let mut conn = connect(&url).await;
        diesel::update(harvest_workflow_executions::table.find(completed))
            .set((dsl::state.eq("COMPLETED"), dsl::completed_at.eq(Utc::now())))
            .execute(&mut conn)
            .await
            .expect("complete one run");
    }

    // AC2: a change without the opt-in moves nothing.
    let (status, body) = patch(&app, WF, json!({ "sla_secs": 1 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("recompute").is_none(), "{body}");
    assert_eq!(deadlines(&url, running).await.sla(), secs(3 * 3600));

    // AC3: the opt-in moves the RUNNING run of the type only.
    let (status, body) = patch(&app, WF, json!({ "recompute_in_flight": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["recompute"]["moved_runs"], json!(1), "{body}");
    assert_eq!(body["recompute"]["unavailable_shards"], json!([]));
    assert_eq!(deadlines(&url, running).await.sla(), secs(1));
    assert_eq!(deadlines(&url, completed).await.sla(), secs(3 * 3600));
    assert_eq!(deadlines(&url, other).await.sla(), secs(3 * 3600));

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let mut conn = connect(&url).await;
    let breached = autumn_harvest::timeout::enforce_workflow_sla_breaches(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("scan");
    assert_eq!(breached, 1, "only the moved run is past its SLA");
    assert!(deadlines(&url, running).await.sla_breached);
    assert!(!deadlines(&url, other).await.sla_breached);

    // A breached run never moves again, so the scanner does not see it twice.
    let (_, body) = patch(
        &app,
        WF,
        json!({ "sla_secs": 7200, "recompute_in_flight": true }),
    )
    .await;
    assert_eq!(body["recompute"]["moved_runs"], json!(0), "{body}");
}

/// AC4: clearing the override restores the code value for new starts.
#[tokio::test]
async fn clearing_the_override_restores_the_code_value() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    let (status, _) = patch(&app, WF, json!({ "sla_secs": 300 })).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = patch(&app, WF, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"], Value::Null, "{body}");
    assert_eq!(body["effective"]["sla_secs"], json!(3 * 3600));

    let id = start(&app, WF).await;
    assert_eq!(deadlines(&url, id).await.sla(), secs(3 * 3600));

    // A null field also returns to code. The last field deletes the row.
    patch(&app, WF, json!({ "sla_secs": 300 })).await;
    let (_, body) = patch(&app, WF, json!({ "sla_secs": null })).await;
    assert_eq!(body["override"], Value::Null, "{body}");
}

/// AC5: an override `execution_timeout` below the effective `sla` clamps the
/// SLA, as at start.
#[tokio::test]
async fn an_override_timeout_below_the_sla_clamps_the_sla() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    let (status, body) = patch(&app, WF, json!({ "execution_timeout_secs": 600 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["effective"]["execution_timeout_secs"], json!(600));

    let id = start(&app, WF).await;
    let run = deadlines(&url, id).await;
    assert_eq!(run.timeout(), secs(600));
    assert_eq!(run.sla(), secs(600), "the 3h SLA clamps to the 10m timeout");
}

/// The route refuses a bad request, and audits every change.
#[tokio::test]
async fn bad_requests_are_refused_and_changes_are_audited() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    let (status, _) = request_json(&app, "GET", &route("no_such_type"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = patch(&app, "no_such_type", json!({ "sla_secs": 60 })).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = patch(&app, WF, json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "an empty patch is a 400");
    let (status, _) = patch(&app, WF, json!({ "sla_secs": 0 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a zero SLA is a 400");
    let (status, _) = patch(&app, WF, json!({ "sla": 60 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "an unknown field is a 400");

    let (status, _) = patch(&app, WF, json!({ "sla_secs": 60 })).await;
    assert_eq!(status, StatusCode::OK);

    let mut conn = connect(&url).await;
    let mut statuses = audit_statuses(&mut conn).await;
    statuses.sort();
    assert_eq!(
        statuses,
        ["failed", "failed", "failed", "failed", "succeeded"],
        "each PATCH writes one audit row"
    );
}

/// Both routes sit behind the admin guard.
#[tokio::test]
async fn the_routes_require_admin_auth() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app_with_admin(&build_test_pool(&url), false);
    let (status, _) = request_json(&app, "GET", &route(WF), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = patch(&app, WF, json!({ "sla_secs": 60 })).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// Issue #2388 AC3: `GET /admin/overrides` lists every override with its
/// actor, reason and time, and `GET /admin/config` counts them.
#[tokio::test]
async fn the_override_list_names_actor_reason_and_time() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    let (status, body) = request_json(&app, "GET", "/admin/overrides", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["schedule_overrides"], json!([]));
    assert_eq!(body["run_default_overrides"], json!([]));
    let (status, body) = request_json(&app, "GET", "/admin/config", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["overrides"]["schedule_overrides"], json!(0));
    assert_eq!(body["overrides"]["run_default_overrides"], json!(0));

    let (status, body) = patch(&app, WF, json!({ "sla_secs": 300, "reason": "tighter" })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // A DAG override, written as the PATCH /dags route stores it.
    let mut conn = connect(&url).await;
    diesel::sql_query(
        "INSERT INTO harvest_schedule_overrides \
             (kind, name, max_active_runs, reason, updated_by) \
         VALUES ('dag', 'nightly_dag', 4, 'backfill week', 'oncall@example.com')",
    )
    .execute(&mut conn)
    .await
    .expect("insert a DAG override");

    let (status, body) = request_json(&app, "GET", "/admin/overrides", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let dag = &body["schedule_overrides"][0];
    assert_eq!(dag["kind"], json!("dag"));
    assert_eq!(dag["name"], json!("nightly_dag"));
    assert_eq!(dag["fields"], json!({ "max_active_runs": 4 }));
    assert_eq!(dag["reason"], json!("backfill week"));
    assert_eq!(dag["updated_by"], json!("oncall@example.com"));
    assert!(dag["updated_at"].is_string(), "{dag}");
    assert_eq!(dag["shard_ids"], json!([0]));
    let run_default = &body["run_default_overrides"][0];
    assert_eq!(run_default["workflow_name"], json!(WF));
    assert_eq!(run_default["sla_secs"], json!(300));
    assert_eq!(run_default["reason"], json!("tighter"));
    assert!(run_default["updated_by"].is_string(), "{run_default}");
    assert!(run_default["updated_at"].is_string(), "{run_default}");
    assert_eq!(body["unavailable_shards"], json!([]));

    let (status, body) = request_json(&app, "GET", "/admin/config", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["overrides"],
        json!({ "schedule_overrides": 1, "run_default_overrides": 1, "unavailable_shards": 0 })
    );
}

/// The override list sits behind the admin guard.
#[tokio::test]
async fn the_override_list_requires_admin_auth() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app_with_admin(&build_test_pool(&url), false);
    let (status, _) = request_json(&app, "GET", "/admin/overrides", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// The chain cap of one run, from `started_at` to `chain_deadline_at`.
async fn chain_cap(url: &str, id: Uuid) -> (Option<chrono::Duration>, Option<chrono::Duration>) {
    use harvest_workflow_executions::dsl;
    let mut conn = connect(url).await;
    let (started_at, timeout, deadline) = harvest_workflow_executions::table
        .find(id)
        .select((
            dsl::started_at,
            harvest_workflow_executions_ext::chain_execution_timeout,
            harvest_workflow_executions_ext::chain_deadline_at,
        ))
        .first::<(
            DateTime<Utc>,
            Option<chrono::Duration>,
            Option<DateTime<Utc>>,
        )>(&mut conn)
        .await
        .expect("load execution");
    (timeout, deadline.map(|at| whole_secs(started_at, at)))
}

/// Issue #2406 AC1: an override of the chain cap applies to the next chain
/// that starts. A run that started before it keeps its stored deadline.
#[tokio::test]
async fn a_chain_override_applies_to_the_next_chain() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    let before = start(&app, CHAIN_WF).await;
    let stored_before = chain_cap(&url, before).await;
    assert_eq!(stored_before, (secs(3600), secs(3600)));

    let (status, body) = patch(
        &app,
        CHAIN_WF,
        json!({ "chain_execution_timeout_secs": 7200, "reason": "longer backfill" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["chain_execution_timeout_secs"], json!(3600));
    assert_eq!(
        body["override"]["chain_execution_timeout_secs"],
        json!(7200)
    );
    assert_eq!(
        body["effective"]["chain_execution_timeout_secs"],
        json!(7200)
    );
    assert_eq!(body["override"]["sla_secs"], Value::Null, "{body}");

    let after = start(&app, CHAIN_WF).await;
    assert_eq!(chain_cap(&url, after).await, (secs(7200), secs(7200)));
    assert_eq!(
        chain_cap(&url, before).await,
        stored_before,
        "a chain in flight keeps its stored deadline"
    );

    // The override is per type.
    let other = start(&app, WF).await;
    assert_eq!(chain_cap(&url, other).await, (None, None));

    let (status, body) = request_json(&app, "GET", &route(CHAIN_WF), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["effective"]["chain_execution_timeout_secs"],
        json!(7200)
    );

    // `recompute_in_flight` moves the SLA only. The chain deadline stays.
    let (status, body) = patch(&app, CHAIN_WF, json!({ "recompute_in_flight": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(chain_cap(&url, before).await, stored_before);
}

/// Issue #2406 AC1: a type that declares no chain cap takes one from the
/// override.
#[tokio::test]
async fn a_chain_override_sets_a_cap_that_code_leaves_unset() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    let (status, body) = patch(&app, WF, json!({ "chain_execution_timeout_secs": 600 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["chain_execution_timeout_secs"], Value::Null);
    let id = start(&app, WF).await;
    assert_eq!(chain_cap(&url, id).await, (secs(600), secs(600)));
    // The SLA keeps its code value.
    assert_eq!(deadlines(&url, id).await.sla(), secs(3 * 3600));
}

/// Issue #2406 AC2: the fleet chain ceiling still caps the override.
#[tokio::test]
async fn the_fleet_chain_ceiling_caps_the_override() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app_with(&build_test_pool(&url), true, Some(2 * HOUR));

    let (status, body) = patch(
        &app,
        CHAIN_WF,
        json!({ "chain_execution_timeout_secs": 10 * 3600 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let capped = start(&app, CHAIN_WF).await;
    assert_eq!(chain_cap(&url, capped).await, (secs(7200), secs(7200)));

    // An override below the ceiling is kept.
    let (status, body) = patch(
        &app,
        CHAIN_WF,
        json!({ "chain_execution_timeout_secs": 1800 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let below = start(&app, CHAIN_WF).await;
    assert_eq!(chain_cap(&url, below).await, (secs(1800), secs(1800)));
}

/// Issue #2406 AC3: clearing the override restores the code value.
#[tokio::test]
async fn clearing_the_chain_override_restores_the_code_value() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));

    patch(
        &app,
        CHAIN_WF,
        json!({ "chain_execution_timeout_secs": 7200, "sla_secs": 60 }),
    )
    .await;

    // A null returns the chain cap to code and keeps the other field.
    let (status, body) = patch(
        &app,
        CHAIN_WF,
        json!({ "chain_execution_timeout_secs": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["override"]["chain_execution_timeout_secs"],
        Value::Null,
        "{body}"
    );
    assert_eq!(body["override"]["sla_secs"], json!(60), "{body}");
    assert_eq!(
        body["effective"]["chain_execution_timeout_secs"],
        json!(3600)
    );
    let id = start(&app, CHAIN_WF).await;
    assert_eq!(chain_cap(&url, id).await, (secs(3600), secs(3600)));

    // `clear_override` deletes the whole row.
    patch(
        &app,
        CHAIN_WF,
        json!({ "chain_execution_timeout_secs": 7200 }),
    )
    .await;
    let (status, body) = patch(&app, CHAIN_WF, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"], Value::Null, "{body}");
    let id = start(&app, CHAIN_WF).await;
    assert_eq!(chain_cap(&url, id).await, (secs(3600), secs(3600)));
}

/// Issue #2406: the chain cap takes the bound of the other fields.
#[tokio::test]
async fn a_chain_cap_out_of_bounds_is_a_400() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app(&build_test_pool(&url));
    for value in [json!(0), json!(3_153_600_001_u64), json!(-1)] {
        let (status, body) = patch(
            &app,
            CHAIN_WF,
            json!({ "chain_execution_timeout_secs": value }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}: {body}");
    }
}

/// Issue #2406: a DAG declares no chain cap, so the route refuses one. Its
/// other fields still take an override.
#[tokio::test]
async fn a_dag_refuses_a_chain_cap() {
    let (url, _container) = setup_test_database_url().await;
    let app = build_app_from(&build_test_pool(&url), true, None, dag_catalog());

    let (status, body) = patch(&app, DAG, json!({ "chain_execution_timeout_secs": 600 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = patch(&app, DAG, json!({ "sla_secs": 600 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["chain_execution_timeout_secs"], Value::Null);
    let (status, body) = patch(&app, DAG, json!({ "chain_execution_timeout_secs": null })).await;
    assert_eq!(status, StatusCode::OK, "a null sets no chain cap: {body}");

    let mut conn = connect(&url).await;
    let mut statuses = audit_statuses(&mut conn).await;
    statuses.sort();
    assert_eq!(statuses, ["failed", "succeeded", "succeeded"]);
}
