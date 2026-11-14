//! Integration tests for `GET /admin/queue-coverage` (issue #774).
//!
//! Checks the core coverage predicate end to end against a real Postgres:
//!
//! - A queue with pending work and zero live pollers is uncovered.
//! - `Draining` workers still cover (AC1). `Stopped` and stale workers do not.
//! - A paused queue is excluded from the uncovered list.
//! - The `?queue_name=` filter narrows the report.
//! - An unreachable shard degrades `status` to `partial`/`unavailable` and
//!   keeps the reachable shard's data (AC6).
//! - Sample ids are capped at 5 (AC3).
//! - The top-level `uncovered`/`total_uncovered_queues` fields answer a CI
//!   gate alone (AC4).
//! - The route is admin-gated.

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowContext;
use autumn_harvest::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::queue_pause;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_task_queue;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::workers::{WorkerStatus, register_worker, transition_status};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Duration as ChronoDuration, Utc};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

type HarvestApiApp = axum::Router;

// ── Boilerplate shared with the sibling reachability/shard-health suites ──

/// A trivial workflow handler — only its registration name matters here.
fn noop_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

fn workflow_info_named(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: noop_workflow,
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

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn setup_database_url_with_migrations() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    (url, container)
}

/// Env-URL (local Postgres, no Docker) when `HARVEST_TEST_DATABASE_URL` is
/// set, otherwise a fresh testcontainer. Mirrors the sibling
/// `workflow_reachability_integration::setup_db_env_or_container` precedent,
/// except migrations are (idempotently) applied on *both* paths so a bare,
/// un-migrated local Postgres instance still works for local verification.
async fn setup_db_env_or_container() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
            .expect("failed to run Harvest migrations");
        return (url, None);
    }
    let (url, container) = setup_database_url_with_migrations().await;
    (url, Some(container))
}

fn single_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    )
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

fn build_api_state(
    pool: HarvestDbPool,
    router: ShardRouter,
    registered: Vec<&'static str>,
) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(pool);
    let workflows = registered.into_iter().map(workflow_info_named).collect();
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(workflows, vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::<WorkflowSchedule>::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    api_state
}

fn build_api_app(pool: HarvestDbPool, router: ShardRouter) -> HarvestApiApp {
    harvest_api_router(build_api_state(pool, router, vec![]))
}

async fn read_json_response(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    serde_json::from_slice(&body).expect("response must be JSON")
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn connect(database_url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database")
}

async fn insert_execution(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = connect(database_url).await;
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: shard.as_i32(),
        input: json!({}).into(),
        parent_id: None,
        queue_name: "default",
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
        start_source: None,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution");

    let events = vec![WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("failed to append start event");
    exec_id
}

/// Insert a `PENDING` task-queue row on `queue_name`, owned by a fresh
/// `RUNNING` execution of `workflow_name` (the FK the schema requires).
/// Returns `(task_id, execution_id)`.
async fn insert_pending_task(
    database_url: &str,
    shard: ShardId,
    queue_name: &str,
    workflow_name: &str,
) -> (Uuid, ExecutionId) {
    let exec_id = insert_execution(
        database_url,
        shard,
        workflow_name,
        &format!("wf-{}", Uuid::new_v4()),
    )
    .await;
    let mut conn = connect(database_url).await;
    let task_id = Uuid::new_v4();
    diesel::insert_into(harvest_task_queue::table)
        .values((
            harvest_task_queue::id.eq(task_id),
            harvest_task_queue::queue_name.eq(queue_name),
            harvest_task_queue::task_type.eq("activity"),
            harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())),
            harvest_task_queue::input.eq(json!({})),
        ))
        .execute(&mut conn)
        .await
        .expect("failed to insert pending task");
    (task_id, exec_id)
}

/// Register a worker via the real `register_worker` primitive, then
/// optionally transition it off `Active` (its post-registration default).
async fn insert_worker(
    database_url: &str,
    worker_id: &str,
    queues: &[&str],
    shard_assignments: &[i32],
    status: WorkerStatus,
) {
    let mut conn = connect(database_url).await;
    let queues: Vec<String> = queues.iter().map(ToString::to_string).collect();
    register_worker(
        &mut conn,
        worker_id,
        &queues,
        shard_assignments,
        10,
        "test-host",
        None,
        "",
        None,
        &HashMap::<String, String>::new(),
        0,
        &[],
    )
    .await
    .expect("failed to register worker");
    if status != WorkerStatus::Active {
        transition_status(&mut conn, worker_id, status)
            .await
            .expect("failed to transition worker status");
    }
}

/// Backdate a worker's `last_heartbeat_at` so it reads as `Stale` against
/// the default 10s threshold (`HarvestApiState::worker_stale_threshold`).
async fn backdate_worker_heartbeat(database_url: &str, worker_id: &str, seconds_ago: i64) {
    use autumn_harvest::schema::harvest_workers;

    let mut conn = connect(database_url).await;
    diesel::update(harvest_workers::table.find(worker_id))
        .set(
            harvest_workers::last_heartbeat_at
                .eq(Utc::now() - ChronoDuration::seconds(seconds_ago)),
        )
        .execute(&mut conn)
        .await
        .expect("failed to backdate heartbeat");
}

async fn pause_queue(database_url: &str, queue_name: &str) {
    let mut conn = connect(database_url).await;
    queue_pause::pause_queue(&mut conn, queue_name, "test pause", "test-actor", None)
        .await
        .expect("failed to pause queue");
}

/// Fully saturate a worker (`in_flight_count == max_concurrency`) for the
/// AC2 "coverage != capacity" test — coverage must ignore utilization.
async fn saturate_worker(database_url: &str, worker_id: &str) {
    use autumn_harvest::schema::harvest_workers;

    let mut conn = connect(database_url).await;
    diesel::update(harvest_workers::table.find(worker_id))
        .set(harvest_workers::in_flight_count.eq(harvest_workers::max_concurrency))
        .execute(&mut conn)
        .await
        .expect("failed to saturate worker");
}

/// Set a worker's `build_id` for the AC5 "coverage is orthogonal to build
/// compatibility" test -- `worker_covers_queue` must never consult it.
async fn set_worker_build_id(database_url: &str, worker_id: &str, build_id: &str) {
    use autumn_harvest::schema::harvest_workers;

    let mut conn = connect(database_url).await;
    diesel::update(harvest_workers::table.find(worker_id))
        .set(harvest_workers::build_id.eq(build_id))
        .execute(&mut conn)
        .await
        .expect("failed to set worker build_id");
}

/// Scrubs the tables this suite writes, so a shared migrated DB stays
/// isolated per test.
///
/// Deleting `harvest_workflow_executions` cascades to `harvest_task_queue`
/// and every other execution-scoped table. `harvest_workers` and
/// `harvest_queue_pauses` have no FK to an execution, so they are scrubbed
/// directly. With `HARVEST_TEST_DATABASE_URL`, every test in this binary
/// shares one database. Mirrors the `scrub` idiom of `retention_overrides_tests.rs`.
async fn scrub(database_url: &str) {
    let mut conn = connect(database_url).await;
    for stmt in [
        "DELETE FROM harvest_workflow_executions",
        "DELETE FROM harvest_workers",
        "DELETE FROM harvest_queue_pauses",
    ] {
        diesel::sql_query(stmt)
            .execute(&mut conn)
            .await
            .expect(stmt);
    }
}

fn item<'a>(report: &'a Value, queue_name: &str) -> &'a Value {
    report
        .get("items")
        .and_then(Value::as_array)
        .expect("items array")
        .iter()
        .find(|item| item.get("queue_name").and_then(Value::as_str) == Some(queue_name))
        .unwrap_or_else(|| panic!("expected item for {queue_name}"))
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn fully_covered_queue_reports_no_uncovered_items() {
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;
    insert_worker(
        &database_url,
        "worker-1",
        &["email-workers"],
        &[0],
        WorkerStatus::Active,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("status").and_then(Value::as_str),
        Some("complete")
    );
    assert_eq!(report.get("uncovered"), Some(&json!(false)));
    assert_eq!(report.get("total_uncovered_queues"), Some(&json!(0)));
    assert_eq!(
        report.get("items").and_then(Value::as_array).map(Vec::len),
        Some(0)
    );
}

/// Issue #2479: a declared queue with no live poller is named, with or
/// without pending work. It does not change `uncovered`. A `queue_name`
/// filter narrows the list.
#[tokio::test]
async fn a_declared_queue_with_no_poller_is_named_unpolled() {
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_worker(
        &database_url,
        "priority-tier",
        &["priority"],
        &[0],
        WorkerStatus::Active,
    )
    .await;
    insert_worker(
        &database_url,
        "stopped-tier",
        &["bulk"],
        &[0],
        WorkerStatus::Stopped,
    )
    .await;

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::single(build_test_pool(&database_url)));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(Vec::new(), vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::<WorkflowSchedule>::new()),
        None,
        vec![
            "default".to_string(),
            "priority".to_string(),
            "bulk".to_string(),
        ],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        single_shard_router(),
    ));
    let app = harvest_api_router(api_state);

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(report.get("uncovered"), Some(&json!(false)), "{report}");
    assert_eq!(
        report.get("unpolled_declared_queues"),
        Some(&json!(["bulk", "default"])),
        "{report}"
    );

    let (_, filtered) = get_json(&app, "/admin/queue-coverage?queue_name=priority").await;
    assert_eq!(
        filtered.get("unpolled_declared_queues"),
        Some(&json!([])),
        "{filtered}"
    );
    let (_, filtered) = get_json(&app, "/admin/queue-coverage?queue_name=bulk").await;
    assert_eq!(
        filtered.get("unpolled_declared_queues"),
        Some(&json!(["bulk"])),
        "{filtered}"
    );
}

#[tokio::test]
async fn queue_with_no_workers_is_reported_uncovered() {
    // AC1/AC4: a queue with pending work and zero live workers is uncovered.
    // The top-level `uncovered`/`total_uncovered_queues` fields alone answer
    // an "is my fleet fully covered" CI gate.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    let (task_id, exec_id) =
        insert_pending_task(&database_url, ShardId::new(0), "typo_queue", "onboarding").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(report.get("uncovered"), Some(&json!(true)));
    assert_eq!(report.get("total_uncovered_queues"), Some(&json!(1)));

    let entry = item(&report, "typo_queue");
    assert_eq!(entry.get("pending_count"), Some(&json!(1)));

    let task_samples = entry
        .get("sample_task_ids")
        .and_then(Value::as_array)
        .expect("sample_task_ids");
    assert_eq!(task_samples.len(), 1);
    assert_eq!(
        task_samples[0]
            .as_str()
            .and_then(|s| s.parse::<Uuid>().ok()),
        Some(task_id)
    );

    let exec_samples = entry
        .get("sample_execution_ids")
        .and_then(Value::as_array)
        .expect("sample_execution_ids");
    assert_eq!(exec_samples.len(), 1);
    assert_eq!(
        exec_samples[0]
            .as_str()
            .and_then(|s| s.parse::<Uuid>().ok()),
        Some(exec_id.as_uuid())
    );

    let breakdown = entry
        .get("shard_breakdown")
        .and_then(Value::as_array)
        .expect("shard_breakdown");
    assert_eq!(breakdown.len(), 1);
    assert_eq!(breakdown[0].get("shard_id"), Some(&json!(0)));
    assert_eq!(breakdown[0].get("pending_count"), Some(&json!(1)));
}

#[tokio::test]
async fn draining_worker_still_covers_the_queue() {
    // AC1: `Draining` is still finishing in-flight work and must count as
    // covering — only `Stopped` (and stale/absent) workers do not.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;
    insert_worker(
        &database_url,
        "worker-1",
        &["email-workers"],
        &[0],
        WorkerStatus::Draining,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("uncovered"),
        Some(&json!(false)),
        "a Draining worker must still count as covering"
    );
}

#[tokio::test]
async fn stopped_worker_does_not_cover_the_queue() {
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;
    insert_worker(
        &database_url,
        "worker-1",
        &["email-workers"],
        &[0],
        WorkerStatus::Stopped,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(report.get("uncovered"), Some(&json!(true)));
    assert!(
        item(&report, "email-workers")
            .get("pending_count")
            .is_some()
    );
}

#[tokio::test]
async fn many_historical_stopped_workers_do_not_prevent_correct_coverage_detection() {
    // Issue #774: the coverage check filters `Stopped` workers server-side
    // (`status IN (Active, Draining)`). It must not load every worker a shard
    // ever registered and filter client-side. Restarts with random UUID
    // worker IDs leave a long history of departed workers. This seeds 30
    // `Stopped` rows on the queue name that a live worker also polls. A
    // regression that drops or mis-scopes the status-filtered query then
    // shows up as a false "uncovered" report.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;
    for i in 0..30 {
        insert_worker(
            &database_url,
            &format!("departed-worker-{i}"),
            &["email-workers"],
            &[0],
            WorkerStatus::Stopped,
        )
        .await;
    }
    insert_worker(
        &database_url,
        "worker-live",
        &["email-workers"],
        &[0],
        WorkerStatus::Active,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("uncovered"),
        Some(&json!(false)),
        "a genuinely live Active worker must still be found covering the \
         queue even with 30 historical Stopped rows in the registry: {report:?}"
    );
    assert_eq!(report.get("total_uncovered_queues"), Some(&json!(0)));
}

#[tokio::test]
async fn stale_workers_exceeding_the_cap_do_not_crowd_out_a_healthy_poller() {
    use autumn_harvest::schema::harvest_workers;

    // Issue #774: the `MAX_LIMIT` (500) truncation of
    // `fetch_potentially_live_workers` runs inside `list_workers` via
    // `apply_worker_filters`. That doc says the limit applies "after the
    // in-process retain passes". This holds only when the caller supplies
    // the retain criteria it needs. A crashed worker's `status` can stay
    // `Active` forever. Nothing calls `transition_status` on a dead process,
    // so only its heartbeat goes stale. More than `MAX_LIMIT` such stale
    // `Active` rows must not truncate away the one healthy poller before the
    // health check sees it. Unlike
    // `many_historical_stopped_workers_do_not_prevent_correct_coverage_detection`,
    // every row here is `status = 'Active'`. So only a health-before-truncate
    // order, not the status filter, can save the healthy poller.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;

    // Bulk-register MAX_LIMIT + 1 (501) `Active` workers on ONE reused
    // connection (avoiding 501 separate connection-establish round trips),
    // all polling "email-workers".
    let mut conn = connect(&database_url).await;
    let mut stale_worker_ids = Vec::with_capacity(501);
    for i in 0..501 {
        let worker_id = format!("stale-active-{i}");
        register_worker(
            &mut conn,
            &worker_id,
            &["email-workers".to_string()],
            &[0],
            10,
            "test-host",
            None,
            "",
            None,
            &HashMap::<String, String>::new(),
            0,
            &[],
        )
        .await
        .expect("failed to register stale worker");
        stale_worker_ids.push(worker_id);
    }
    // Backdate all 501 in one query so they classify `Stale` against the
    // default 10s threshold, instead of 501 individual round trips.
    diesel::update(
        harvest_workers::table.filter(harvest_workers::worker_id.eq_any(&stale_worker_ids)),
    )
    .set(harvest_workers::last_heartbeat_at.eq(Utc::now() - ChronoDuration::seconds(60)))
    .execute(&mut conn)
    .await
    .expect("failed to bulk-backdate stale workers");

    // One genuinely healthy poller for the same queue.
    insert_worker(
        &database_url,
        "worker-live",
        &["email-workers"],
        &[0],
        WorkerStatus::Active,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("uncovered"),
        Some(&json!(false)),
        "501 stale-but-Active rows must not truncate away the one healthy \
         poller before the health check runs: {report:?}"
    );
    assert_eq!(report.get("total_uncovered_queues"), Some(&json!(0)));
}

#[tokio::test]
async fn healthy_workers_exceeding_the_cap_do_not_crowd_out_a_covering_poller() {
    // Issue #774: even after pre-filtering to `WorkerHealth::Healthy`,
    // `fetch_potentially_live_workers` still truncated each status's result
    // set at `WorkerFilters::MAX_LIMIT` (500). The truncation is unordered.
    // With more than 500 healthy `Active` workers, the one worker that covers
    // a pending queue could fall outside the cap. The result was a false
    // "uncovered" verdict with a live poller present. The SQL of
    // `list_workers` has no `LIMIT`, so the cap saved no query cost. This
    // internal existence check no longer has the cap.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;

    // Bulk-register 500 (== MAX_LIMIT) genuinely healthy `Active` workers on
    // one reused connection, all polling an UNRELATED queue.
    let mut conn = connect(&database_url).await;
    for i in 0..500 {
        let worker_id = format!("healthy-noise-{i}");
        register_worker(
            &mut conn,
            &worker_id,
            &["unrelated-queue".to_string()],
            &[0],
            10,
            "test-host",
            None,
            "",
            None,
            &HashMap::<String, String>::new(),
            0,
            &[],
        )
        .await
        .expect("failed to register noise worker");
    }

    // The one worker that covers "email-workers" registers LAST. Under an
    // insertion-order sequential scan, it lands past the removed 500-row cap,
    // where the old `truncate(500)` dropped it.
    insert_worker(
        &database_url,
        "worker-live",
        &["email-workers"],
        &[0],
        WorkerStatus::Active,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("uncovered"),
        Some(&json!(false)),
        "500 genuinely healthy, non-covering Active rows must not truncate \
         away the one worker actually covering the pending queue: {report:?}"
    );
    assert_eq!(report.get("total_uncovered_queues"), Some(&json!(0)));
}

#[tokio::test]
async fn stale_worker_does_not_cover_the_queue() {
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;
    insert_worker(
        &database_url,
        "worker-1",
        &["email-workers"],
        &[0],
        WorkerStatus::Active,
    )
    .await;
    // Default worker_stale_threshold is 10s (HarvestApiState::new()).
    backdate_worker_heartbeat(&database_url, "worker-1", 300).await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("uncovered"),
        Some(&json!(true)),
        "a stale heartbeat must not count as a live poller"
    );
}

#[tokio::test]
async fn paused_queue_is_excluded_from_the_uncovered_list() {
    // A queue paused by an operator is deliberately not being drained --
    // that intent is already surfaced by `harvest_queue_paused_too_long`.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "seasonal-batch",
        "onboarding",
    )
    .await;
    pause_queue(&database_url, "seasonal-batch").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("uncovered"),
        Some(&json!(false)),
        "a paused queue's pending work must not be reported uncovered"
    );
    assert_eq!(
        report.get("items").and_then(Value::as_array).map(Vec::len),
        Some(0)
    );
    // The pause exclusion is not invisible. A paused, pollerless queue that
    // has real pending work must still be named. Otherwise it silently
    // outlasts the grace window of `harvest_queue_paused_too_long`.
    assert_eq!(
        report.get("excluded_paused_queues"),
        Some(&json!(["seasonal-batch"])),
        "the paused, pollerless queue must be surfaced in excluded_paused_queues"
    );
}

#[tokio::test]
async fn paused_and_covered_queue_is_absent_from_excluded_paused_queues() {
    // A paused queue that already has a live poller is not uncovered even if
    // unpaused, so it must not clutter excluded_paused_queues either.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "seasonal-batch",
        "onboarding",
    )
    .await;
    insert_worker(
        &database_url,
        "worker-1",
        &["seasonal-batch"],
        &[0],
        WorkerStatus::Active,
    )
    .await;
    pause_queue(&database_url, "seasonal-batch").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(report.get("uncovered"), Some(&json!(false)));
    assert_eq!(
        report.get("excluded_paused_queues"),
        Some(&json!([])),
        "a paused-but-covered queue must not appear in excluded_paused_queues"
    );
}

#[tokio::test]
async fn saturated_worker_still_covers_the_queue() {
    // AC2: coverage means a poller exists, not that capacity is free. A
    // worker fully saturated at max_concurrency must still count as
    // covering -- utilization is #531/#742's job, out of scope here.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;
    insert_worker(
        &database_url,
        "worker-1",
        &["email-workers"],
        &[0],
        WorkerStatus::Active,
    )
    .await;
    saturate_worker(&database_url, "worker-1").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("uncovered"),
        Some(&json!(false)),
        "a fully saturated worker must still count as a live poller"
    );
}

#[tokio::test]
async fn build_incompatible_worker_still_covers_the_queue() {
    // AC5: this endpoint answers "does any live worker poll this queue". It
    // never checks build compatibility with the task's required_build_id,
    // which is the build_reachability of #171. Coverage never depends on
    // `build_id`.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(
        &database_url,
        ShardId::new(0),
        "email-workers",
        "onboarding",
    )
    .await;
    insert_worker(
        &database_url,
        "worker-1",
        &["email-workers"],
        &[0],
        WorkerStatus::Active,
    )
    .await;
    set_worker_build_id(&database_url, "worker-1", "some-arbitrary-build-sha").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("uncovered"),
        Some(&json!(false)),
        "worker build_id must never affect coverage"
    );
}

#[tokio::test]
async fn queue_name_filter_narrows_the_report_to_one_queue() {
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(&database_url, ShardId::new(0), "typo_queue_a", "onboarding").await;
    insert_pending_task(&database_url, ShardId::new(0), "typo_queue_b", "onboarding").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage?queue_name=typo_queue_a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("filter").and_then(Value::as_str),
        Some("typo_queue_a")
    );
    assert_eq!(report.get("total_uncovered_queues"), Some(&json!(1)));
    let names: Vec<&str> = report
        .get("items")
        .and_then(Value::as_array)
        .expect("items")
        .iter()
        .filter_map(|item| item.get("queue_name").and_then(Value::as_str))
        .collect();
    assert_eq!(names, vec!["typo_queue_a"]);
}

#[tokio::test]
async fn queue_name_filter_matching_nothing_is_fully_covered() {
    // An unrecognized/never-scheduled queue name yields uncovered: false --
    // there is nothing pending on it, never an error (module doc contract).
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(&database_url, ShardId::new(0), "typo_queue_a", "onboarding").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage?queue_name=never_scheduled").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("filter").and_then(Value::as_str),
        Some("never_scheduled")
    );
    assert_eq!(report.get("uncovered"), Some(&json!(false)));
    assert_eq!(report.get("total_uncovered_queues"), Some(&json!(0)));
    assert_eq!(
        report.get("items").and_then(Value::as_array).map(Vec::len),
        Some(0)
    );
}

#[tokio::test]
async fn duplicate_and_unknown_query_params_do_not_400() {
    // AC8: invalid params return 400 with a JSON error body. A duplicate or
    // unknown query key is not an invalid value (queue_name has no invalid
    // value). By the `from_query_pairs` convention it resolves, never
    // rejects. This proves it over the real axum route. The old derive-based
    // `Query<QueueCoverageQuery>` extractor returned a `text/plain` 400 for a
    // duplicate key.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(&database_url, ShardId::new(0), "typo_queue_b", "onboarding").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(
        &app,
        "/admin/queue-coverage?queue_name=typo_queue_a&queue_name=typo_queue_b&unknown_param=1",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a duplicate/unknown query key must never 400"
    );
    assert_eq!(
        report.get("filter").and_then(Value::as_str),
        Some("typo_queue_b"),
        "a duplicate key must resolve last-value-wins"
    );
    assert_eq!(report.get("total_uncovered_queues"), Some(&json!(1)));
}

#[tokio::test]
async fn malformed_percent_encoded_query_param_returns_400() {
    // Issue #774: axum's `Query<Vec<(String, String)>>` extractor uses
    // `serde_urlencoded`/`form_urlencoded`. It always succeeds and puts
    // U+FFFD in place of a malformed percent-encoded byte sequence. A
    // corrupted `queue_name` then resolves to a different, nonexistent queue.
    // The report is a false-clean `uncovered: false`, which defeats a scoped
    // CI/CD deploy gate. `%FF` alone is not valid UTF-8. This proves the fix
    // over the real axum route, beyond the `parse_raw_query_pairs_strict` unit
    // tests in `queue_coverage.rs`. The handler must return a JSON 400 body
    // (via `get_json`'s `.expect("response must be JSON")`). A lossy 200 or a
    // bare `text/plain` 400 fails.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(&database_url, ShardId::new(0), "typo_queue_a", "onboarding").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, body) = get_json(&app, "/admin/queue-coverage?queue_name=%FF").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a malformed percent-encoded byte sequence must 400, not silently \
         decode to U+FFFD and report a false-clean result: {body:?}"
    );
    assert_eq!(
        body.get("error").and_then(Value::as_str),
        Some("malformed query string: invalid percent-encoded UTF-8")
    );
}

#[tokio::test]
async fn malformed_percent_encoding_in_an_unknown_query_key_also_returns_400() {
    // Decode-before-parse rejects a malformed byte sequence in any key or
    // value up front. This includes a key that `QueueCoverageQuery` does not
    // recognize. A valid-UTF-8 unknown key is dropped instead (see
    // `duplicate_and_unknown_query_params_do_not_400`).
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, body) = get_json(&app, "/admin/queue-coverage?%FF=1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(
        body.get("error").and_then(Value::as_str),
        Some("malformed query string: invalid percent-encoded UTF-8")
    );
}

#[tokio::test]
async fn syntactically_invalid_percent_escape_also_returns_400() {
    // Issue #774: `%FF` above is a well-formed escape that decodes to invalid
    // UTF-8. `%GG` is a different malformed shape, because `G` is not a hex
    // digit. `percent_encoding::percent_decode_str` leaves `%GG` as literal
    // bytes and does not fail. Those bytes are valid ASCII, so the UTF-8 check
    // passes. Without the hex-escape check, the route queries the literal
    // queue name "orders%GG". It then reports a false-clean
    // `uncovered: false` instead of rejecting the request.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(&database_url, ShardId::new(0), "typo_queue_b", "onboarding").await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, body) = get_json(&app, "/admin/queue-coverage?queue_name=orders%GG").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a syntactically invalid percent escape must 400, not silently query \
         the literal mangled string and report a false-clean result: {body:?}"
    );
    assert_eq!(
        body.get("error").and_then(Value::as_str),
        Some("malformed query string: invalid percent-encoded UTF-8")
    );
}

#[tokio::test]
async fn unavailable_shard_makes_report_partial_and_is_named() {
    // AC6: an unreachable shard is named, never silently dropped, and the
    // reachable shard's uncovered queue is still reported.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    insert_pending_task(&database_url, ShardId::new(0), "typo_queue", "onboarding").await;

    let mut shard_pools = std::collections::BTreeMap::new();
    shard_pools.insert(ShardId::new(0), build_test_pool(&database_url));
    shard_pools.insert(
        ShardId::new(1),
        build_test_pool("postgres://postgres:postgres@127.0.0.1:1/nonexistent"),
    );
    let pool = HarvestDbPool::sharded(ShardedDbPool::from_map(shard_pools, ShardId::new(0)));
    let app = build_api_app(pool, two_shard_router());

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("status").and_then(Value::as_str),
        Some("partial"),
        "an unreachable shard must never be silently dropped"
    );

    let shards = report
        .get("shards")
        .and_then(Value::as_array)
        .expect("shards");
    let unreachable = shards
        .iter()
        .find(|entry| entry.get("shard_id").and_then(Value::as_i64) == Some(1))
        .expect("shard 1 must be named");
    assert_eq!(
        unreachable.get("status").and_then(Value::as_str),
        Some("unavailable")
    );
    assert!(unreachable.get("error").and_then(Value::as_str).is_some());

    // The reachable shard's uncovered queue is still surfaced.
    assert_eq!(report.get("uncovered"), Some(&json!(true)));
    let entry = item(&report, "typo_queue");
    assert_eq!(entry.get("pending_count"), Some(&json!(1)));
}

#[tokio::test]
async fn sample_ids_are_capped_at_five_and_reference_real_rows() {
    // AC3: bounded representative sample ids so an operator can chain
    // directly into `GET /workflows/{id}` or the per-task eligibility
    // explainer without a second unbounded query.
    let (database_url, _container) = setup_db_env_or_container().await;
    scrub(&database_url).await;
    let mut seeded_tasks = BTreeSet::new();
    let mut seeded_execs = BTreeSet::new();
    for _ in 0..7 {
        let (task_id, exec_id) =
            insert_pending_task(&database_url, ShardId::new(0), "typo_queue", "onboarding").await;
        seeded_tasks.insert(task_id);
        seeded_execs.insert(exec_id.as_uuid());
    }

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
    );

    let (status, report) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::OK);
    let entry = item(&report, "typo_queue");
    assert_eq!(entry.get("pending_count"), Some(&json!(7)));

    let task_samples = entry
        .get("sample_task_ids")
        .and_then(Value::as_array)
        .expect("sample_task_ids");
    assert_eq!(task_samples.len(), 5, "task samples must be capped at 5");
    for sample in task_samples {
        let id: Uuid = sample
            .as_str()
            .expect("sample id is a string")
            .parse()
            .expect("sample id is a uuid");
        assert!(
            seeded_tasks.contains(&id),
            "every task sample must be one of the seeded pending tasks"
        );
    }

    let exec_samples = entry
        .get("sample_execution_ids")
        .and_then(Value::as_array)
        .expect("sample_execution_ids");
    assert_eq!(
        exec_samples.len(),
        5,
        "execution samples must be capped at 5"
    );
    for sample in exec_samples {
        let id: Uuid = sample
            .as_str()
            .expect("sample id is a string")
            .parse()
            .expect("sample id is a uuid");
        assert!(
            seeded_execs.contains(&id),
            "every execution sample must be one of the seeded executions"
        );
    }
}

#[tokio::test]
async fn endpoint_requires_admin_auth() {
    // No admin boundary set -> the shared `/admin/*` guard must reject.
    let app = harvest_api_router(HarvestApiState::new());
    let (status, _) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
