//! Integration tests for `GET /admin/queue-coverage` (issue #774).
//!
//! Verifies the core coverage predicate end to end against a real Postgres
//! database: a queue with pending work and zero live pollers is reported
//! uncovered; `Draining` workers still cover (AC1) while `Stopped`/stale
//! workers do not; a paused queue is excluded from the uncovered list; the
//! `?queue_name=` filter narrows the report; an unreachable shard degrades
//! `status` to `partial`/`unavailable` without dropping the reachable
//! shard's data (AC6); representative sample ids are capped at 5 (AC3); the
//! top-level `uncovered`/`total_uncovered_queues` fields answer a CI gate
//! with a single field (AC4); and the route is admin-gated.

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
        .with_state(autumn_web::AppState::for_test())
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
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: shard.as_i32(),
        input: json!({}),
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

/// Scrubs `harvest_workflow_executions` (cascades to `harvest_task_queue`
/// and every other execution-scoped table via `ON DELETE CASCADE`) plus the
/// two standalone tables this suite writes with no FK back to an execution
/// (`harvest_workers`, `harvest_queue_pauses`), so a shared migrated DB
/// (the `HARVEST_TEST_DATABASE_URL` path, which points every test in this
/// binary at the SAME physical database) stays isolated per test. Mirrors
/// `retention_overrides_tests.rs`'s `scrub` idiom.
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

#[tokio::test]
async fn queue_with_no_workers_is_reported_uncovered() {
    // AC1/AC4: a queue with pending work and zero live workers is uncovered,
    // and the top-level `uncovered`/`total_uncovered_queues` fields alone
    // answer a "is my fleet fully covered" CI gate.
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
    let app =
        harvest_api_router(HarvestApiState::new()).with_state(autumn_web::AppState::for_test());
    let (status, _) = get_json(&app, "/admin/queue-coverage").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
