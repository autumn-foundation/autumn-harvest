//! HTTP integration tests for the workflow retry policy override (issue
//! #2409).
//!
//! `PATCH /admin/workflow-types/{workflow_name}/run-defaults` sets the
//! operator override of three fields of `WorkflowInfo::retry_policy`. No
//! worker runs. The failure path reads the override through
//! `GLOBAL_RUN_DEFAULTS`, so the tests read that global to check that a
//! change applies in this process at once. The DB tests in
//! `autumn-harvest/tests/integration/retry_override_db_tests.rs` drive the
//! failures.
//!
//! Dual-mode DB: set `HARVEST_TEST_DATABASE_URL` to a Postgres admin URL to
//! run each test in a fresh database on that cluster. Otherwise each test
//! starts a testcontainers Postgres 16.

use std::sync::Arc;

use autumn_harvest::RetentionConfig;
use autumn_harvest::WorkflowContext;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_audit_log;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::workflow_retry::RetryPolicySource;
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

/// A type with a retry policy: 5 attempts, 250 ms, capped at 10 s.
const RETRY_WF: &str = "billing_sync";
/// A type with no retry policy.
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
        let db_name = format!("harvest_retry_ovr_{}", Uuid::new_v4().simple());
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

fn info(name: &'static str, retry_policy: Option<RetryPolicy>) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "retry_override_integration",
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
        retry_policy,
    }
}

fn code_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 5,
        initial_interval: std::time::Duration::from_millis(250),
        backoff_coefficient: 2.0,
        max_interval: std::time::Duration::from_secs(10),
        non_retryable_errors: vec!["Fatal".to_string()],
        jitter: JitterPolicy::None,
    }
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![info(RETRY_WF, Some(code_policy())), info(PLAIN_WF, None)],
        vec![],
    ))
}

/// A DAG. It declares no retry policy.
const DAG: &str = "billing_rollup";

fn dag_catalog() -> DagCatalog {
    autumn_harvest::compile_dag_catalog(vec![autumn_harvest::info::DagInfo {
        name: DAG,
        module: "retry_override_integration",
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
    build_app_with_ceiling(pool, None)
}

/// An app whose fleet ceiling on `max_attempts` is `ceiling`.
fn build_app_with_ceiling(pool: &DbPool, ceiling: Option<u32>) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_max_workflow_attempts(ceiling);
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

/// Serializes the tests. The failure path reads the override through a
/// process global, which each `HandlerRegistry::new` replaces.
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

/// The policy that the failure path of this process uses for a failed run
/// of `RETRY_WF` that stores the code policy.
fn failure_policy(source: RetryPolicySource) -> RetryPolicy {
    autumn_harvest::run_defaults::current_retry_policy(RETRY_WF, code_policy(), source)
}

/// The route reports the code, override and effective retry fields.
#[tokio::test]
async fn get_reports_the_code_and_the_effective_retry_fields() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, body) = request_json(&app, "GET", &route(RETRY_WF), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["retry_max_attempts"], 5);
    assert_eq!(body["code"]["retry_initial_interval_ms"], 250);
    assert_eq!(body["code"]["retry_max_interval_ms"], 10_000);
    assert_eq!(body["effective"], body["code"]);
    assert_eq!(body["override"], Value::Null);

    let (status, body) = request_json(&app, "GET", &route(PLAIN_WF), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"]["retry_max_attempts"], Value::Null);
}

/// AC 1: a change applies to the next failure in this process at once.
#[tokio::test]
async fn a_patch_stops_the_retries_at_the_next_failure() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, body) = patch(
        &app,
        RETRY_WF,
        json!({ "retry_max_attempts": 1, "reason": "INC-2409" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"]["retry_max_attempts"], 1);
    assert_eq!(body["override"]["reason"], "INC-2409");
    assert_eq!(body["effective"]["retry_max_attempts"], 1);
    assert_eq!(body["effective"]["retry_initial_interval_ms"], 250);
    assert_eq!(body["code"]["retry_max_attempts"], 5);

    let policy = failure_policy(RetryPolicySource::Type);
    assert_eq!(
        policy.max_attempts, 1,
        "the write-through reaches the failure path"
    );
    assert_eq!(policy.non_retryable_errors, vec!["Fatal".to_string()]);
    // AC 4: a schedule policy wins over the override.
    assert_eq!(failure_policy(RetryPolicySource::Schedule).max_attempts, 5);
}

/// AC 1: a longer first delay alone raises the cap to it.
#[tokio::test]
async fn a_patch_slows_the_retries() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, body) = patch(
        &app,
        RETRY_WF,
        json!({ "retry_initial_interval_ms": 60_000 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["effective"]["retry_initial_interval_ms"], 60_000);
    assert_eq!(body["effective"]["retry_max_interval_ms"], 60_000);
    let policy = failure_policy(RetryPolicySource::Type);
    assert_eq!(policy.initial_interval, std::time::Duration::from_secs(60));
    assert_eq!(policy.max_interval, std::time::Duration::from_secs(60));
}

/// A cap alone below the first delay of the policy rises to that delay.
#[tokio::test]
async fn a_cap_below_the_first_delay_rises_to_it() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, body) = patch(&app, RETRY_WF, json!({ "retry_max_interval_ms": 100 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"]["retry_max_interval_ms"], 100);
    assert_eq!(body["effective"]["retry_initial_interval_ms"], 250);
    assert_eq!(body["effective"]["retry_max_interval_ms"], 250);
    let policy = failure_policy(RetryPolicySource::Type);
    assert_eq!(policy.max_interval, std::time::Duration::from_millis(250));
}

/// AC 3: a clear restores the code value, and each change is audited.
#[tokio::test]
async fn clearing_restores_the_code_value_and_each_change_is_audited() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, _) = patch(&app, RETRY_WF, json!({ "retry_max_attempts": 1 })).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = patch(&app, PLAIN_WF, json!({ "retry_max_attempts": 1 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, body) = patch(&app, RETRY_WF, json!({ "clear_override": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["override"], Value::Null);
    assert_eq!(body["effective"]["retry_max_attempts"], 5);
    assert_eq!(failure_policy(RetryPolicySource::Type).max_attempts, 5);
    assert_eq!(
        audit_statuses(&url).await,
        ["succeeded", "failed", "succeeded"],
        "each PATCH writes one audit row, a refused one included"
    );
}

/// A `null` returns one retry field to code and keeps the others.
#[tokio::test]
async fn a_null_returns_one_retry_field_to_code() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, _) = patch(
        &app,
        RETRY_WF,
        json!({ "retry_max_attempts": 2, "retry_max_interval_ms": 30_000 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = patch(&app, RETRY_WF, json!({ "retry_max_attempts": null })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["effective"]["retry_max_attempts"], 5);
    assert_eq!(body["effective"]["retry_max_interval_ms"], 30_000);
    assert_eq!(body["override"]["retry_max_attempts"], Value::Null);

    let (status, body) = request_json(&app, "GET", "/admin/overrides", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let listed = body["run_default_overrides"]
        .as_array()
        .expect("array")
        .iter()
        .find(|entry| entry["workflow_name"] == RETRY_WF)
        .expect("the override is listed");
    assert_eq!(listed["retry_max_interval_ms"], 30_000);
}

/// The route refuses a retry field that cannot apply, and audits each
/// refusal with its reason.
#[tokio::test]
async fn a_retry_field_that_cannot_apply_is_refused_and_audited() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app_with_ceiling(&build_pool(&url), Some(8));
    let no_policy = "declares no retry_policy";
    let refused = [
        (PLAIN_WF, json!({ "retry_max_attempts": 1 }), no_policy),
        (
            DAG,
            json!({ "retry_initial_interval_ms": 1_000 }),
            no_policy,
        ),
        (
            RETRY_WF,
            json!({ "retry_max_attempts": 0 }),
            "retry_max_attempts must be at least 1",
        ),
        (
            RETRY_WF,
            json!({ "retry_max_attempts": 9 }),
            "retry_max_attempts must be at most the fleet ceiling (8)",
        ),
        (
            RETRY_WF,
            json!({ "retry_initial_interval_ms": 0 }),
            "retry_initial_interval_ms must be at least 1",
        ),
        (
            RETRY_WF,
            json!({ "retry_initial_interval_ms": 60_000, "retry_max_interval_ms": 1_000 }),
            "retry_max_interval_ms must be at least retry_initial_interval_ms",
        ),
        (
            RETRY_WF,
            json!({ "retry_max_interval_ms": 31_536_000_001_u64 }),
            "retry_max_interval_ms must be at most 31536000000 (365 days)",
        ),
        (
            RETRY_WF,
            json!({ "retry_max_attempts": -1 }),
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
    // A value at the ceiling is accepted.
    let (status, body) = patch(&app, RETRY_WF, json!({ "retry_max_attempts": 8 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // A null sets no field, so it is not refused, even on a type with no
    // retry policy.
    let (status, body) = patch(&app, PLAIN_WF, json!({ "retry_max_attempts": null })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// An empty patch names the retry fields among the fields to set.
#[tokio::test]
async fn an_empty_patch_names_the_retry_fields() {
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let (status, body) = patch(&app, RETRY_WF, json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let summary = audit_failures(&url).await.join(" ");
    for field in [
        "retry_max_attempts",
        "retry_initial_interval_ms",
        "retry_max_interval_ms",
    ] {
        assert!(summary.contains(field), "{summary}");
    }
}

/// Insert a due workflow schedule of `RETRY_WF` with `retry_policy`.
async fn insert_schedule(url: &str, retry_policy: Option<&RetryPolicy>) -> Uuid {
    use autumn_harvest::schema::{harvest_schedules, harvest_schedules_ext};
    use autumn_harvest::table_split::Ext;
    use harvest_schedules::dsl;
    let mut conn = connect(url).await;
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(RETRY_WF),
            dsl::schedule_expr.eq("interval:3600"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(chrono::Utc::now() + chrono::Duration::hours(1)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(json!([])),
            dsl::buffer_all_max.eq(0),
            dsl::skip_policy.eq("skip"),
            Ext(harvest_schedules_ext::workflow_input.eq(json!({}))),
            Ext(harvest_schedules_ext::retry_policy
                .eq(retry_policy.map(|p| serde_json::to_value(p).expect("json")))),
        ))
        .execute(&mut conn)
        .await
        .expect("insert schedule");
    id
}

/// The stored retry policy of each run that `schedule_id` started.
async fn schedule_run_policies(url: &str, schedule_id: Uuid) -> Vec<Option<Value>> {
    use autumn_harvest::schema::{harvest_workflow_executions, harvest_workflow_executions_ext};
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::schedule_id.eq(Some(schedule_id)))
        .select(harvest_workflow_executions_ext::workflow_retry_policy)
        .load(&mut conn)
        .await
        .expect("schedule runs")
}

/// AC 4: trigger-now and backfill mark a schedule's own policy, and only it.
#[tokio::test]
async fn trigger_now_and_backfill_mark_only_a_schedule_policy() {
    use autumn_harvest::workflow_retry::{SCHEDULE_SOURCE, SOURCE_KEY};
    let _serial = serial().await;
    let (url, _guard) = setup_database().await;
    let app = build_app(&build_pool(&url));
    let own = RetryPolicy::fixed(2, std::time::Duration::from_secs(1));
    // One hour-aligned slot of the `interval:3600` schedule, two hours ago.
    let hours = chrono::Utc::now().timestamp() / 3_600 - 2;
    let slot = chrono::DateTime::from_timestamp(hours * 3_600, 0).expect("a slot");
    let window_start = slot - chrono::Duration::seconds(1);
    let window_end = slot + chrono::Duration::seconds(1);
    // A type holds one schedule, so the two cases run one after the other.
    for (policy, mark, attempts) in [(Some(&own), Some(SCHEDULE_SOURCE), 2), (None, None, 5)] {
        let schedule = insert_schedule(&url, policy).await;
        let (status, body) = request_json(
            &app,
            "POST",
            &format!("/admin/schedules/{schedule}/trigger"),
            Some(json!({})),
        )
        .await;
        assert!(status.is_success(), "trigger: {status} {body}");
        let (status, body) = request_json(
            &app,
            "POST",
            &format!("/admin/schedules/{schedule}/backfill"),
            Some(json!({ "from": window_start, "to": window_end, "max_count": 1 })),
        )
        .await;
        assert!(status.is_success(), "backfill: {status} {body}");
        let stored_runs = schedule_run_policies(&url, schedule).await;
        assert_eq!(stored_runs.len(), 2, "one trigger and one backfill run");
        for stored in &stored_runs {
            let stored = stored.as_ref().expect("a stored policy");
            assert_eq!(
                stored.get(SOURCE_KEY).and_then(Value::as_str),
                mark,
                "{stored}"
            );
            assert_eq!(stored["max_attempts"], attempts, "{stored}");
        }
        let mut conn = connect(&url).await;
        diesel::sql_query("DELETE FROM harvest_schedules WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(schedule)
            .execute(&mut conn)
            .await
            .expect("delete the schedule");
    }
}
