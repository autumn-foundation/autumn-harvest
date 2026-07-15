#![cfg(feature = "db")]
//! Builder-level default activity retry + start_to_close floor — issue #620.
//!
//! RED PHASE (TDD): these tests reference the not-yet-existing
//! `HandlerRegistry::with_activity_defaults(retry, start_to_close)` builder, so
//! the file fails to COMPILE against the missing symbol until the green phase
//! adds it (the #543/#593 "red = compile error" precedent). They pin the
//! schedule-time precedence the feature must implement:
//!
//!   call-site override  →  activity `#[activity(retry=…/start_to_close=…)]`
//!   default  →  builder default  →  implicit fallback
//!
//! Harness (DB setup, pool, worker, seed, load helpers) is copied verbatim from
//! `activity_interceptor_tests.rs` so this suite EXECUTES against a migrated
//! Postgres when `HARVEST_TEST_DATABASE_URL` is set, otherwise boots a fresh
//! testcontainers Postgres 16. Each test uses a unique task queue + unique
//! `ExecutionId` so a shared cluster run stays isolated.
//!
//! Assertions read the *enqueued* activity task's `max_attempts` /
//! `start_to_close` / `retry_policy` columns (regular path) and the recorded
//! `LocalActivityFailed` event count (local path).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::telemetry::TelemetryConfig;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup — env-var-redirectable, otherwise testcontainers.
// ---------------------------------------------------------------------------

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Regular activity: echoes its input.
fn echo_activity(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

/// Local activity that always fails (so retry counting is observable).
fn failing_local(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Err("failing_local always errors".to_string()) })
}

/// Workflow: schedule one regular activity via the plain raw path (no per-call
/// overrides) onto the workflow's own queue, then suspend on it.
fn wf_one_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("echo", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Workflow: schedule one regular activity WITH a call-site retry override
/// (`max_attempts = 5`). Mirrors the DAG unified handler's opts path.
fn wf_one_activity_call_override(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw_with_opts(
            "echo",
            input,
            &queue,
            Some(RetryPolicy::fixed(5, Duration::from_millis(10))),
            None,
        )
        .await
        .map_err(|e| e.to_string())
    })
}

/// Workflow: run a failing local activity with NO call-site retry/STC override,
/// so the builder default is what must apply. Propagates the terminal Err.
fn wf_failing_local(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("failing_local", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Workflow: run a slow local activity with NO call-site STC override, so the
/// builder-default STC (clamped by `max_local_activity_start_to_close`) governs
/// the per-attempt timeout. Propagates the terminal Err (timeout → FAILED).
fn wf_slow_local(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("slow_local", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Local activity that sleeps ~500ms then echoes.
fn slow_local(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(input)
    })
}

// ---------------------------------------------------------------------------
// Registry / worker construction.
// ---------------------------------------------------------------------------

/// Build a registry, optionally installing builder-level activity defaults.
///
/// RED PHASE: `HandlerRegistry::with_activity_defaults` does not exist yet —
/// this is the not-yet-existing symbol these tests fail to compile against.
fn build_registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    default_retry: Option<RetryPolicy>,
    default_stc: Option<Duration>,
) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(TelemetryConfig::default());
    Arc::new(
        HandlerRegistry::with_state_and_telemetry(
            workflows,
            activities,
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        )
        .with_activity_defaults(default_retry, default_stc),
    )
}

fn build_worker(
    worker_id: &str,
    queue: &str,
    registry: Arc<HandlerRegistry>,
    max_local_stc: Duration,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: worker_id.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: max_local_stc,
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

// ---------------------------------------------------------------------------
// Seeding + read helpers.
// ---------------------------------------------------------------------------

async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
    queue: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
        parent_id: None,
        queue_name: queue,
        execution_timeout: None,
        deadline_at: None,
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
        continued_from_exec_id: None,
        first_exec_id: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");

    exec_id
}

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload workflow execution")
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

/// Load the single enqueued *activity* task row for `activity_name`.
async fn load_activity_task(
    url: &str,
    exec_id: ExecutionId,
    activity_name: &str,
) -> TaskQueueItem {
    let mut conn = connect(url).await;
    let rows: Vec<TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::activity_name.eq(Some(activity_name.to_string())))
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("reload task rows");
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one enqueued activity task for '{activity_name}'"
    );
    rows.into_iter().next().unwrap()
}

async fn wait_for_state(
    url: &str,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    tokio::time::timeout(timeout, async {
        loop {
            let e = load_execution(url, exec_id).await;
            if e.state == want {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution did not reach state {want} within {timeout:?}"))
}

/// Drive a worker until `exec_id` reaches `want`, then shut it down cleanly.
async fn run_to_state(
    url: &str,
    pool: &DbPool,
    worker: Arc<Worker>,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    let execution = wait_for_state(url, exec_id, want, timeout).await;
    worker.shutdown();
    handle.await.expect("worker task joins cleanly");
    execution
}

/// Drive the worker until the activity task for `activity_name` has been
/// enqueued (the workflow suspended on it), then shut down. Returns the row.
async fn run_until_activity_enqueued(
    url: &str,
    pool: &DbPool,
    worker: Arc<Worker>,
    exec_id: ExecutionId,
    activity_name: &str,
    timeout: Duration,
) -> TaskQueueItem {
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    let row = tokio::time::timeout(timeout, async {
        loop {
            let mut conn = connect(url).await;
            let rows: Vec<TaskQueueItem> = harvest_task_queue::table
                .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
                .filter(harvest_task_queue::activity_name.eq(Some(activity_name.to_string())))
                .select(TaskQueueItem::as_select())
                .load(&mut conn)
                .await
                .expect("reload task rows");
            if let Some(r) = rows.into_iter().next() {
                break r;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("activity '{activity_name}' not enqueued within {timeout:?}"));
    worker.shutdown();
    handle.await.expect("worker task joins cleanly");
    row
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "activity_default_floor_tests",
        handler,
        execution_timeout: None,
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

fn act_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    is_local: bool,
    retry: Option<RetryPolicy>,
    start_to_close: Option<Duration>,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_default_floor_tests",
        default_retry_policy: retry,
        default_start_to_close: start_to_close,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        is_local,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

fn count_local_failed(history: &[WorkflowEvent]) -> usize {
    history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::LocalActivityFailed { .. }))
        .count()
}

// ---------------------------------------------------------------------------
// AC3 — a no-`retry` activity under a builder default gets max_attempts = N.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_retry_activity_under_builder_default_gets_max_attempts_n() {
    let (url, _container) = setup_db().await;
    let queue = "q620-builder-default";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({"v": 1}), queue).await;

    // Builder default max_attempts = 4; activity declares no retry.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None, None)],
        Some(RetryPolicy::fixed(4, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker("w620-default", queue, Arc::clone(&registry), Duration::from_secs(60));
    let pool = build_pool(&url);

    let task = run_until_activity_enqueued(
        &url,
        &pool,
        worker,
        exec_id,
        "echo",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        task.max_attempts, 4,
        "builder default retry (max_attempts=4) must apply to a no-retry activity"
    );
    assert!(
        task.retry_policy.is_some(),
        "the enqueued task must carry the resolved retry policy JSON"
    );
}

// ---------------------------------------------------------------------------
// AC3 — the raw dispatch path (used by DAGs) honours the builder default.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_dispatched_activity_honours_builder_default() {
    let (url, _container) = setup_db().await;
    let queue = "q620-raw-default";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({"v": 1}), queue).await;

    // `wf_one_activity` uses `ctx.execute_activity_raw` — the DAG/raw path.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None, None)],
        Some(RetryPolicy::fixed(4, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker("w620-raw", queue, Arc::clone(&registry), Duration::from_secs(60));
    let pool = build_pool(&url);

    let task = run_until_activity_enqueued(
        &url,
        &pool,
        worker,
        exec_id,
        "echo",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        task.max_attempts, 4,
        "the raw dispatch path must resolve the builder default retry floor"
    );
}

// ---------------------------------------------------------------------------
// AC4 — a declared-retry activity is unaffected by the builder default.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_retry_activity_unaffected_by_builder_default() {
    let (url, _container) = setup_db().await;
    let queue = "q620-declared-wins";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({"v": 1}), queue).await;

    // Activity declares its own retry (max_attempts=2); builder default is 9.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info(
            "echo",
            echo_activity,
            false,
            Some(RetryPolicy::fixed(2, Duration::from_millis(10))),
            None,
        )],
        Some(RetryPolicy::fixed(9, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker("w620-declared", queue, Arc::clone(&registry), Duration::from_secs(60));
    let pool = build_pool(&url);

    let task = run_until_activity_enqueued(
        &url,
        &pool,
        worker,
        exec_id,
        "echo",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        task.max_attempts, 2,
        "the activity's own declared retry must win over the builder default"
    );
}

// ---------------------------------------------------------------------------
// AC5 — a call-site override wins over the builder default.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_site_override_wins_over_builder_default() {
    let (url, _container) = setup_db().await;
    let queue = "q620-call-wins";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity_call_override",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Call-site override max_attempts=5; builder default 9; activity default none.
    let registry = build_registry(
        vec![wf_info(
            "wf_one_activity_call_override",
            wf_one_activity_call_override,
        )],
        vec![act_info("echo", echo_activity, false, None, None)],
        Some(RetryPolicy::fixed(9, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker("w620-call", queue, Arc::clone(&registry), Duration::from_secs(60));
    let pool = build_pool(&url);

    let task = run_until_activity_enqueued(
        &url,
        &pool,
        worker,
        exec_id,
        "echo",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        task.max_attempts, 5,
        "the call-site retry override must win over the builder default"
    );
}

// ---------------------------------------------------------------------------
// AC6 — no defaults set anywhere is byte-for-byte today's behaviour.
//
// NOTE (RED-phase deviation, flagged to the coordinator): the task brief said
// to assert `max_attempts == 1`, but the CURRENT engine enqueues a no-retry
// activity with `max_attempts == 3` (the `EnqueueParams::new` default) and a
// NULL `retry_policy` — and `worker::next_retry_delay` actively retries up to
// `max_attempts` when `retry_policy` is None. So today a no-`retry` activity
// gets 3 attempts, NOT a single attempt. Since AC6 is explicitly "byte-for-byte
// today's behaviour", this test asserts the real current values (3 + NULL). The
// green phase MUST NOT change this no-default path.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_defaults_set_is_byte_for_byte_current_behaviour() {
    let (url, _container) = setup_db().await;
    let queue = "q620-no-defaults";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({"v": 1}), queue).await;

    // No builder default, no activity default, no call-site override.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None, None)],
        None,
        None,
    );
    let worker = build_worker("w620-nodefault", queue, Arc::clone(&registry), Duration::from_secs(60));
    let pool = build_pool(&url);

    let task = run_until_activity_enqueued(
        &url,
        &pool,
        worker,
        exec_id,
        "echo",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        task.max_attempts, 3,
        "with no defaults the enqueued max_attempts must remain the current \
         EnqueueParams default (3) — byte-for-byte"
    );
    assert!(
        task.retry_policy.is_none(),
        "with no defaults the enqueued task must carry a NULL retry_policy"
    );
    assert!(
        task.start_to_close.is_none(),
        "with no defaults the enqueued task must carry a NULL start_to_close"
    );
}

// ---------------------------------------------------------------------------
// AC7 — a local activity honours the builder-default retry.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_honours_builder_default_retry() {
    let (url, _container) = setup_db().await;
    let queue = "q620-local-retry";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_failing_local", serde_json::json!({"v": 1}), queue).await;

    // Builder default retry max_attempts=3; local activity always fails; the
    // workflow supplies NO call-site retry override, so the builder default is
    // what governs the attempt count.
    let registry = build_registry(
        vec![wf_info("wf_failing_local", wf_failing_local)],
        vec![act_info("failing_local", failing_local, true, None, None)],
        Some(RetryPolicy::fixed(3, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker("w620-local-retry", queue, Arc::clone(&registry), Duration::from_secs(60));
    let pool = build_pool(&url);

    run_to_state(&url, &pool, worker, exec_id, "FAILED", Duration::from_secs(20)).await;

    let history = load_history(&url, exec_id).await;
    assert_eq!(
        count_local_failed(&history),
        3,
        "the builder-default retry (max_attempts=3) must drive 3 local-activity attempts"
    );
}

// ---------------------------------------------------------------------------
// AC7 — a local activity's builder-default STC is clamped by the worker max.
//
// NOTE (RED-phase limitation, flagged to the coordinator): asserting the
// builder-default STC end-to-end is awkward. The local path's per-attempt
// timeout is `run.start_to_close_secs.map_or(max, from_secs).min(max)`, so a
// no-call-site-STC local activity ALREADY falls back to `max` today —
// meaning a builder default LARGER than `max` (300s vs 60s) yields the same
// clamped value (60s) with or without the feature. This integration test can
// therefore only assert the weaker AC "the builder default does not grant the
// full 300s": a 500ms-sleeping local activity under a small `max` (200ms) times
// out (FAILED) rather than completing. The precedence itself is pinned by the
// pure `resolve_effective_start_to_close_precedence` unit test in policy.rs.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_builder_default_stc_clamped_by_max() {
    let (url, _container) = setup_db().await;
    let queue = "q620-local-stc";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, "wf_slow_local", serde_json::json!({"v": 1}), queue).await;

    // Builder default STC = 300s, worker max_local_activity_start_to_close =
    // 200ms. A 500ms-sleeping local activity must NOT be granted the full 300s;
    // it is clamped and times out → FAILED.
    let registry = build_registry(
        vec![wf_info("wf_slow_local", wf_slow_local)],
        vec![act_info("slow_local", slow_local, true, None, None)],
        None,
        Some(Duration::from_secs(300)),
    );
    let worker = build_worker(
        "w620-local-stc",
        queue,
        Arc::clone(&registry),
        Duration::from_millis(200),
    );
    let pool = build_pool(&url);

    let execution =
        run_to_state(&url, &pool, worker, exec_id, "FAILED", Duration::from_secs(20)).await;

    assert_eq!(
        execution.state, "FAILED",
        "a 500ms local activity under a 200ms clamp must time out — the \
         builder-default 300s STC must not grant the full window"
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityFailed { .. })),
        "the clamped local activity must record a LocalActivityFailed (timeout)"
    );
}
