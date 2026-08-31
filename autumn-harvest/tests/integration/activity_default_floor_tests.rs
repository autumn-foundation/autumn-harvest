#![cfg(feature = "db")]
//! Builder-level default activity retry + `start_to_close` floor — issue #620.
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

/// Local activity that sleeps ~2s then echoes — long enough that a 1s
/// builder-default STC kills it while the 60s worker cap alone would not.
fn slower_local(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(input)
    })
}

/// Workflow: run the ~2s local activity with NO call-site STC override.
fn wf_slower_local(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("slower_local", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Local activity that sleeps ~50ms then echoes — well under a 500ms subsecond
/// builder-default STC, so it MUST complete. Without the full-`Duration` fix
/// (issue #620, Codex P2) the 500ms builder default truncates to
/// `Duration::from_secs(0)` and even this 50ms activity is instantly timed out.
fn fast_local(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(input)
    })
}

/// Workflow: run the ~50ms local activity with NO call-site STC override, so the
/// subsecond builder-default STC governs.
fn wf_fast_local(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("fast_local", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Workflow: schedule one regular activity WITH a call-site `start_to_close`
/// override (5s), no retry override. Used to prove the call-site STC wins over
/// the builder-default STC on the regular dispatch path.
fn wf_one_activity_stc_override(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw_with_opts(
            "echo",
            input,
            &queue,
            None,
            Some(Duration::from_secs(5)),
        )
        .await
        .map_err(|e| e.to_string())
    })
}

/// Workflow: schedule the reserved session-acquire internal activity via the
/// raw path (mirroring how `create_session` dispatches it). Used to prove the
/// builder-level floor does NOT leak onto engine-internal session machinery.
/// The worker is shut down at enqueue time, so the reserved dispatch handler
/// never runs.
fn wf_session_acquire(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        // Reserved engine-internal session-acquire activity name (issue #606);
        // the public `is_reserved_session_activity_name` helper classifies it,
        // and the worker auto-registers an `ActivityInfo` stub for it so the
        // enqueue-time lookup succeeds.
        ctx.execute_activity_raw(RESERVED_SESSION_ACQUIRE, input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// The reserved worker-session acquire activity name (issue #606). Kept in sync
/// with `autumn_harvest::context`'s `pub(crate)` constant via the public
/// `is_reserved_session_activity_name` classifier (asserted in the test below).
const RESERVED_SESSION_ACQUIRE: &str = "__harvest_session_acquire";

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
                codec_rotation_batch_size: 0,
                dr_fencing: false,
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
                capability_miss_max_redeliveries: 5,
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
        quota_key: None,
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
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

/// Seed a RUNNING execution whose local activity is MID-RETRY: its history
/// already carries `LocalActivityScheduled` (frozen with `recorded_resolved` /
/// `recorded_retry` / `recorded_stc_nanos`) and one
/// `LocalActivityFailed(attempt=1)`, and a workflow task is enqueued so a
/// worker resumes it. This reproduces the crash-recovery window for AC8: the
/// original retry budget/timeout is frozen in history and must survive a later
/// change to the builder-level default.
///
/// `recorded_resolved` is the #620 disambiguation marker (Codex P2): `true`
/// models a #620+ event whose frozen values are authoritative (even `None`);
/// `false` models a pre-#620 legacy event that falls back to live re-derivation.
#[allow(clippy::too_many_arguments)]
async fn seed_local_activity_in_progress(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    activity_name: &str,
    input: serde_json::Value,
    queue: &str,
    recorded_resolved: bool,
    recorded_retry: Option<RetryPolicy>,
    recorded_stc_nanos: Option<u64>,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");

    let activity_id = autumn_harvest::types::ActivityExecId::new();
    store::append_events(
        conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: activity_name.to_string(),
                input: input.clone(),
                resolved: recorded_resolved,
                retry_policy: recorded_retry,
                start_to_close_nanos: recorded_stc_nanos,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id,
                error: "attempt 1 failed (pre-crash)".to_string(),
                attempt: 1,
            },
        ],
        0,
    )
    .await
    .expect("append partial local-activity history");

    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue resume workflow task");

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
async fn load_activity_task(url: &str, exec_id: ExecutionId, activity_name: &str) -> TaskQueueItem {
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
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "activity_default_floor_tests",
        handler,
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
        rate_limit_key_expr: None,
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

/// The `start_to_close_nanos` the worker FROZE onto the recorded
/// `LocalActivityScheduled` — i.e. the fully-resolved effective per-attempt
/// timeout, in nanoseconds (issue #620, Codex P2). This is the pre-clamp
/// resolved `Duration` value, so asserting it proves the resolution carried the
/// configured `Duration` with no truncation at any unit — deterministically,
/// with no wall-clock timing dependence.
fn local_scheduled_stc_nanos(history: &[WorkflowEvent]) -> Option<u64> {
    history.iter().find_map(|e| match e {
        WorkflowEvent::LocalActivityScheduled {
            start_to_close_nanos,
            ..
        } => Some(*start_to_close_nanos),
        _ => None,
    })?
}

// ---------------------------------------------------------------------------
// AC3 — a no-`retry` activity under a builder default gets max_attempts = N.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_retry_activity_under_builder_default_gets_max_attempts_n() {
    let (url, _container) = setup_db().await;
    let queue = "q620-builder-default";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Builder default max_attempts = 4; activity declares no retry.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None, None)],
        Some(RetryPolicy::fixed(4, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker(
        "w620-default",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // `wf_one_activity` uses `ctx.execute_activity_raw` — the DAG/raw path.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None, None)],
        Some(RetryPolicy::fixed(4, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker(
        "w620-raw",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

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
    let worker = build_worker(
        "w620-declared",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
    let worker = build_worker(
        "w620-call",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // No builder default, no activity default, no call-site override.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None, None)],
        None,
        None,
    );
    let worker = build_worker(
        "w620-nodefault",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
    let exec_id = seed_workflow(
        &mut conn,
        "wf_failing_local",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Builder default retry max_attempts=3; local activity always fails; the
    // workflow supplies NO call-site retry override, so the builder default is
    // what governs the attempt count.
    let registry = build_registry(
        vec![wf_info("wf_failing_local", wf_failing_local)],
        vec![act_info("failing_local", failing_local, true, None, None)],
        Some(RetryPolicy::fixed(3, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker(
        "w620-local-retry",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    assert_eq!(
        count_local_failed(&history),
        3,
        "the builder-default retry (max_attempts=3) must drive 3 local-activity attempts"
    );
}

// ---------------------------------------------------------------------------
// AC7 (discriminating) — a local activity's builder-default STC BELOW the
// worker cap is enforced at its EXACT value.
//
// This is the discriminating rework of the original clamp test: builder-default
// STC = 1s (well below the 60s worker cap), local activity sleeps ~2s. The
// local path's per-attempt timeout is
// `run.start_to_close.unwrap_or(max).min(max)` — WITH the feature the resolved
// `Duration` is `Some(1s)`, so the timeout is `min(1s, 60s) = 1s` and the 2s
// activity FAILS. WITHOUT the feature it is `None`, so the timeout is the 60s
// cap and the 2s activity would COMPLETE. The outcome therefore depends on the
// feature's exact value, not merely the cap. (The local path carries the full
// `Duration`, so subsecond builder STCs are meaningful — see the
// `local_activity_subsecond_*` tests below.)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_builder_default_stc_below_cap_is_enforced() {
    let (url, _container) = setup_db().await;
    let queue = "q620-local-stc-enforced";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_slower_local",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Builder default STC = 1s, worker cap = 60s. No retry (so exactly one
    // attempt), no activity-level STC, no call-site STC — the builder default
    // is the sole source. The ~2s activity must be killed by the 1s builder STC.
    let registry = build_registry(
        vec![wf_info("wf_slower_local", wf_slower_local)],
        vec![act_info("slower_local", slower_local, true, None, None)],
        None,
        Some(Duration::from_secs(1)),
    );
    let worker = build_worker(
        "w620-local-stc-enforced",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    let execution = run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        execution.state, "FAILED",
        "the builder-default 1s STC (below the 60s cap) must kill the ~2s local \
         activity — without the feature the 60s cap alone would let it complete"
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityFailed { .. })),
        "the STC-killed local activity must record a LocalActivityFailed (timeout)"
    );
}

// ---------------------------------------------------------------------------
// AC7 (clamp-proof) — a builder-default STC LARGER than the worker cap is still
// clamped by the cap. Cheap companion to the discriminating test above: it
// pins that the builder default can never GRANT more than the worker cap.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_builder_default_stc_larger_than_cap_is_clamped() {
    let (url, _container) = setup_db().await;
    let queue = "q620-local-stc-clamp";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_slow_local",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Builder default STC = 300s, worker cap = 200ms. A 500ms-sleeping local
    // activity must NOT be granted the full 300s; it is clamped → times out.
    let registry = build_registry(
        vec![wf_info("wf_slow_local", wf_slow_local)],
        vec![act_info("slow_local", slow_local, true, None, None)],
        None,
        Some(Duration::from_secs(300)),
    );
    let worker = build_worker(
        "w620-local-stc-clamp",
        queue,
        Arc::clone(&registry),
        Duration::from_millis(200),
    );
    let pool = build_pool(&url);

    let execution = run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        execution.state, "FAILED",
        "a 500ms local activity under a 200ms clamp must time out — the \
         builder-default 300s STC must never grant more than the worker cap"
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityFailed { .. })),
        "the clamped local activity must record a LocalActivityFailed (timeout)"
    );
}

// ---------------------------------------------------------------------------
// FIX 1 (issue #606 interaction) — the builder-level floor must NOT leak onto
// the reserved worker-session internal activities. They are engine machinery
// bounded by schedule_to_start / acquisition-timeout, not a user retry/timeout
// floor; inheriting a builder default would govern session acquire/release.
// The enqueued reserved task must therefore keep the PRE-FEATURE resolution:
// the EnqueueParams default (max_attempts = 3) and a NULL start_to_close.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserved_session_activity_does_not_inherit_builder_default() {
    // Sanity: the literal used by the workflow handler is the classified
    // reserved name, so this test cannot silently drift from the engine const.
    assert!(
        autumn_harvest::context::is_reserved_session_activity_name(RESERVED_SESSION_ACQUIRE),
        "the test's reserved-name literal must match the engine classifier"
    );

    let (url, _container) = setup_db().await;
    let queue = "q620-reserved-session";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_session_acquire",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // A deliberately conspicuous builder default (max_attempts = 9, STC = 30s).
    // The reserved session-acquire activity declares neither its own retry nor
    // start_to_close, so WITHOUT the guard it would inherit 9 / 30s; WITH the
    // guard it must stay at the pre-feature values (3 / NULL).
    let registry = build_registry(
        vec![wf_info("wf_session_acquire", wf_session_acquire)],
        // No user activity registration needed — the worker auto-registers the
        // reserved session-acquire ActivityInfo stub for the enqueue lookup.
        vec![],
        Some(RetryPolicy::fixed(9, Duration::from_millis(10))),
        Some(Duration::from_secs(30)),
    );
    let worker = build_worker(
        "w620-reserved",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    let task = run_until_activity_enqueued(
        &url,
        &pool,
        worker,
        exec_id,
        RESERVED_SESSION_ACQUIRE,
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        task.max_attempts, 3,
        "a reserved session-internal activity must NOT inherit the builder \
         default retry (9); it keeps the EnqueueParams default (3)"
    );
    assert!(
        task.retry_policy.is_none(),
        "a reserved session-internal activity must carry a NULL retry_policy — \
         the builder default must not be resolved for it"
    );
    assert!(
        task.start_to_close.is_none(),
        "a reserved session-internal activity must carry a NULL start_to_close — \
         the builder default (30s) must not be resolved for it"
    );
}

// ---------------------------------------------------------------------------
// FIX 2 (AC8) — a local activity mid-retry must keep its ORIGINAL retry budget
// across a crash even if the builder-level default is changed in the recovery
// window. The budget is frozen into the `LocalActivityScheduled` event at first
// schedule; the recovery path reads it back rather than re-deriving from the
// (now-changed) builder default.
//
// Scenario: original builder default fixed(2). The activity ran attempt 1 and
// crashed (history: LocalActivityScheduled{retry=fixed(2)} + Failed(1)). The
// operator then LOWERS the builder default to fixed(1) and the worker resumes.
// With the fix, the recorded fixed(2) governs → attempt 2 runs → 2 failures.
// WITHOUT the fix, the re-derived fixed(1) would exhaust immediately (only the
// 1 pre-crash failure), never running attempt 2.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_mid_retry_keeps_original_budget_across_default_change() {
    let (url, _container) = setup_db().await;
    let queue = "q620-ac8-recovery";
    let mut conn = connect(&url).await;

    // Freeze the ORIGINAL builder default (fixed(2)) into the scheduled event,
    // exactly as the worker would have at first schedule. A #620+ event
    // (resolved = true) → the frozen fixed(2) is authoritative.
    let exec_id = seed_local_activity_in_progress(
        &mut conn,
        "wf_failing_local",
        "failing_local",
        serde_json::json!({"v": 1}),
        queue,
        true,
        Some(RetryPolicy::fixed(2, Duration::from_millis(10))),
        None,
    )
    .await;

    // The operator has since LOWERED the builder default to fixed(1). If the
    // recovery path re-derived from this, the activity would exhaust at once.
    let registry = build_registry(
        vec![wf_info("wf_failing_local", wf_failing_local)],
        vec![act_info("failing_local", failing_local, true, None, None)],
        Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker(
        "w620-ac8",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    assert_eq!(
        count_local_failed(&history),
        2,
        "the ORIGINAL frozen retry budget (fixed(2)) must survive the builder \
         default change to fixed(1): attempt 2 must still run, yielding 2 \
         LocalActivityFailed events — not 1 (which a re-derived fixed(1) would \
         produce by exhausting immediately)"
    );
}

// ---------------------------------------------------------------------------
// FIX 2 (backward compat) — a PRE-#620 partial history (no recorded
// `retry_policy` on `LocalActivityScheduled`, i.e. `None`) falls back to the
// current re-derived builder default on recovery, exactly as today. Here the
// resumed builder default is fixed(3): with no frozen budget the recovery path
// re-derives fixed(3) and runs to 3 failures.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_mid_retry_without_recorded_budget_uses_current_default() {
    let (url, _container) = setup_db().await;
    let queue = "q620-ac8-legacy";
    let mut conn = connect(&url).await;

    // Pre-#620 legacy event: the resolution marker is absent (resolved =
    // false) and no frozen retry_policy → recovery re-derives the live default.
    let exec_id = seed_local_activity_in_progress(
        &mut conn,
        "wf_failing_local",
        "failing_local",
        serde_json::json!({"v": 1}),
        queue,
        false,
        None,
        None,
    )
    .await;

    let registry = build_registry(
        vec![wf_info("wf_failing_local", wf_failing_local)],
        vec![act_info("failing_local", failing_local, true, None, None)],
        Some(RetryPolicy::fixed(3, Duration::from_millis(10))),
        None,
    );
    let worker = build_worker(
        "w620-ac8-legacy",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    assert_eq!(
        count_local_failed(&history),
        3,
        "with NO frozen budget (pre-#620 event), recovery re-derives the current \
         builder default (fixed(3)) — 1 pre-crash failure + 2 more = 3 total"
    );
}

// ---------------------------------------------------------------------------
// FIX 2 (AC8 — Codex P2, the resolution-marker refinement). A #620+ event that
// resolved to NO floor (retry = None) is serialized identically to a genuine
// pre-#620 legacy event (field absent → None). The `resolved` marker
// disambiguates them: a #620+ event with `resolved = true` must keep its FROZEN
// "no floor" (implicit 1 attempt) on recovery, NOT fall through to a builder
// default that was ADDED FROM NOTHING after the activity was scheduled.
//
// Scenario: original schedule had NO retry floor of any kind (resolved = true,
// retry = None → implicit 1 attempt). History: LocalActivityScheduled{resolved,
// retry=None} + Failed(1). The operator then ADDS a builder default fixed(3) and
// the worker resumes. With the marker, the frozen implicit 1 attempt governs →
// `failed_attempts(1) >= max_attempts(1)` → exhausts immediately, NEVER running
// attempt 2 → exactly 1 LocalActivityFailed. WITHOUT the marker, the added
// fixed(3) would wrongly apply and run 3 total (the AC8-violating bug).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_resolved_to_no_floor_ignores_a_later_added_default() {
    let (url, _container) = setup_db().await;
    let queue = "q620-ac8-nofloor";
    let mut conn = connect(&url).await;

    // A #620+ event that resolved to NO retry AND NO STC floor: resolved = true,
    // retry = None, stc = None — exactly what the worker writes when there is no
    // call-site / activity-level / builder default at first schedule.
    let exec_id = seed_local_activity_in_progress(
        &mut conn,
        "wf_failing_local",
        "failing_local",
        serde_json::json!({"v": 1}),
        queue,
        true,
        None,
        None,
    )
    .await;

    // The operator has since ADDED a builder default (retry fixed(3) AND a 30s
    // STC) that did NOT exist when the activity was scheduled. The frozen "no
    // floor" must win — the added default must NOT leak onto this in-flight run.
    let registry = build_registry(
        vec![wf_info("wf_failing_local", wf_failing_local)],
        vec![act_info("failing_local", failing_local, true, None, None)],
        Some(RetryPolicy::fixed(3, Duration::from_millis(10))),
        Some(Duration::from_secs(30)),
    );
    let worker = build_worker(
        "w620-ac8-nofloor",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    assert_eq!(
        count_local_failed(&history),
        1,
        "a #620+ event that resolved to NO floor (resolved = true, retry = None) \
         must keep its FROZEN implicit 1 attempt on recovery — the builder \
         default fixed(3) ADDED after scheduling must NOT apply. Exactly 1 \
         LocalActivityFailed (the pre-crash attempt); attempt 2 must never run. \
         A count of 3 would mean the marker was ignored and the added default \
         wrongly re-derived (the AC8-violating bug)."
    );
}

// ---------------------------------------------------------------------------
// FIX 3 (Codex P2, value proof) — the EFFECTIVE resolved local start_to_close
// must EQUAL the configured Duration with NO loss at any unit. The command and
// the frozen `LocalActivityScheduled.start_to_close_nanos` carry the full
// Duration (nanoseconds), so:
//   * a `from_millis(1500)` builder default resolves to EXACTLY 1.5s
//     (1_500_000_000 ns) — not 1s (a seconds field would truncate it);
//   * a `from_micros(500)` sub-millisecond value is preserved as 500_000 ns —
//     NON-ZERO (a millis field would zero it → instant timeout).
// This asserts the resolved Duration VALUE from recorded history — deterministic,
// no wall-clock timing race — and subsumes the "subsecond not zeroed" outcome
// assertion below (which additionally proves the worker HONORS the value
// end-to-end at a robust 10x margin).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_resolved_stc_equals_configured_duration_no_truncation() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);

    // Scenario 1: from_millis(1500) → exactly 1.5s frozen, and (1.5s > 50ms
    // activity, 60s cap) the run COMPLETES.
    {
        let queue = "q620-stc-value-1500ms";
        let mut conn = connect(&url).await;
        let exec_id = seed_workflow(
            &mut conn,
            "wf_fast_local",
            serde_json::json!({"v": 1}),
            queue,
        )
        .await;
        let registry = build_registry(
            vec![wf_info("wf_fast_local", wf_fast_local)],
            vec![act_info("fast_local", fast_local, true, None, None)],
            None,
            Some(Duration::from_millis(1500)),
        );
        let worker = build_worker(
            "w620-stc-value-1500ms",
            queue,
            Arc::clone(&registry),
            Duration::from_secs(60),
        );
        run_to_state(
            &url,
            &pool,
            worker,
            exec_id,
            "COMPLETED",
            Duration::from_secs(20),
        )
        .await;
        let history = load_history(&url, exec_id).await;
        assert_eq!(
            local_scheduled_stc_nanos(&history),
            Some(1_500_000_000),
            "a from_millis(1500) builder default must freeze EXACTLY 1.5s \
             (1_500_000_000 ns) — a seconds field would have truncated it to 1s"
        );
    }

    // Scenario 2: from_micros(500) → 500_000 ns frozen (NON-ZERO), proving
    // sub-millisecond precision survives. The 500µs per-attempt timeout kills
    // the 50ms activity (→ FAILED), but the FROZEN value is recorded at schedule
    // time regardless, so the value assertion is outcome-independent.
    {
        let queue = "q620-stc-value-500us";
        let mut conn = connect(&url).await;
        let exec_id = seed_workflow(
            &mut conn,
            "wf_fast_local",
            serde_json::json!({"v": 2}),
            queue,
        )
        .await;
        let registry = build_registry(
            vec![wf_info("wf_fast_local", wf_fast_local)],
            vec![act_info("fast_local", fast_local, true, None, None)],
            None,
            Some(Duration::from_micros(500)),
        );
        let worker = build_worker(
            "w620-stc-value-500us",
            queue,
            Arc::clone(&registry),
            Duration::from_secs(60),
        );
        run_to_state(
            &url,
            &pool,
            worker,
            exec_id,
            "FAILED",
            Duration::from_secs(20),
        )
        .await;
        let history = load_history(&url, exec_id).await;
        assert_eq!(
            local_scheduled_stc_nanos(&history),
            Some(500_000),
            "a from_micros(500) sub-millisecond builder default must freeze \
             500_000 ns (NON-ZERO) — a millis field would have zeroed it, \
             instantly timing out every attempt"
        );
    }
}

// ---------------------------------------------------------------------------
// FIX 3 (Codex P2) — a SUBSECOND builder-default start_to_close must NOT
// truncate to 0 and instantly time out every local activity. The command now
// carries millis (not seconds), so `Duration::from_millis(500)` is honored
// rather than `Duration::from_secs(0)`.
//
// Discriminating pair under a 500ms subsecond builder default (worker cap 60s):
//   * a ~50ms activity COMPLETES — WITHOUT the fix, 500ms.as_secs() = 0 →
//     from_secs(0) → even 50ms is instantly timed out → the workflow FAILS;
//   * a ~2s activity TIMES OUT — proving the 500ms STC is actually enforced
//     (not silently ignored / defaulting to the 60s cap).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_subsecond_builder_default_stc_is_not_truncated_to_zero() {
    let (url, _container) = setup_db().await;
    let queue = "q620-subsecond-fast";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_fast_local",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Builder default STC = 500ms (SUBSECOND — the bug trigger), worker cap 60s.
    // No retry, no activity-level STC, no call-site STC — the subsecond builder
    // default is the sole source. The ~50ms activity must COMPLETE.
    let registry = build_registry(
        vec![wf_info("wf_fast_local", wf_fast_local)],
        vec![act_info("fast_local", fast_local, true, None, None)],
        None,
        Some(Duration::from_millis(500)),
    );
    let worker = build_worker(
        "w620-subsecond-fast",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    let execution = run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "COMPLETED",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        execution.state, "COMPLETED",
        "a ~50ms local activity under a 500ms SUBSECOND builder-default STC must \
         COMPLETE. Without the millis fix, 500ms truncates to Duration::from_secs(0) \
         and even a 50ms activity is instantly timed out → FAILED."
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityFailed { .. })),
        "a completed subsecond-STC local activity must record NO LocalActivityFailed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_subsecond_builder_default_stc_still_enforces_the_deadline() {
    let (url, _container) = setup_db().await;
    let queue = "q620-subsecond-slow";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_slower_local",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Builder default STC = 500ms (subsecond), worker cap 60s. The ~2s activity
    // must TIME OUT — the 500ms floor is actually enforced, not silently ignored.
    let registry = build_registry(
        vec![wf_info("wf_slower_local", wf_slower_local)],
        vec![act_info("slower_local", slower_local, true, None, None)],
        None,
        Some(Duration::from_millis(500)),
    );
    let worker = build_worker(
        "w620-subsecond-slow",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    let execution = run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(
        execution.state, "FAILED",
        "a ~2s local activity under a 500ms subsecond builder-default STC must \
         TIME OUT — proving the subsecond floor is enforced, not defaulting to \
         the 60s worker cap"
    );
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityFailed { .. })),
        "the STC-killed subsecond local activity must record a LocalActivityFailed"
    );
}

// ---------------------------------------------------------------------------
// Item 3 (P1) — the builder-default start_to_close reaches a REGULAR activity's
// enqueued task.start_to_close (the positive STC path — the retry equivalent is
// already covered by no_retry_activity_under_builder_default_gets_max_attempts_n).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regular_activity_under_builder_default_gets_start_to_close() {
    let (url, _container) = setup_db().await;
    let queue = "q620-stc-positive";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Builder default STC = 30s; activity declares no STC; no call override.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None, None)],
        None,
        Some(Duration::from_secs(30)),
    );
    let worker = build_worker(
        "w620-stc-positive",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
        task.start_to_close,
        Some(chrono::Duration::seconds(30)),
        "the builder-default start_to_close (30s) must reach the enqueued \
         regular activity task's start_to_close column"
    );
}

// ---------------------------------------------------------------------------
// Item 6 (P2) — STC precedence on the regular path: a call-site START_TO_CLOSE
// override wins over the builder-default STC (the STC mirror of AC5 for retry).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_site_stc_override_wins_over_builder_default_stc() {
    let (url, _container) = setup_db().await;
    let queue = "q620-stc-call-wins";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity_stc_override",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Call-site STC override = 5s; builder default STC = 30s; activity none.
    let registry = build_registry(
        vec![wf_info(
            "wf_one_activity_stc_override",
            wf_one_activity_stc_override,
        )],
        vec![act_info("echo", echo_activity, false, None, None)],
        None,
        Some(Duration::from_secs(30)),
    );
    let worker = build_worker(
        "w620-stc-call",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
        task.start_to_close,
        Some(chrono::Duration::seconds(5)),
        "the call-site start_to_close override (5s) must win over the \
         builder-default STC (30s)"
    );
}

// ---------------------------------------------------------------------------
// Item 6 (P2) — STC precedence on the regular path: an activity-declared
// #[activity(start_to_close = …)] default wins over the builder-default STC.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_activity_stc_wins_over_builder_default_stc() {
    let (url, _container) = setup_db().await;
    let queue = "q620-stc-declared-wins";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // Activity declares its own STC = 7s; builder default STC = 30s.
    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info(
            "echo",
            echo_activity,
            false,
            None,
            Some(Duration::from_secs(7)),
        )],
        None,
        Some(Duration::from_secs(30)),
    );
    let worker = build_worker(
        "w620-stc-declared",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
        task.start_to_close,
        Some(chrono::Duration::seconds(7)),
        "the activity's own declared start_to_close (7s) must win over the \
         builder-default STC (30s)"
    );
}

// ---------------------------------------------------------------------------
// Item 7 (P3) — the FULL builder-default retry policy (not just max_attempts)
// round-trips into the enqueued task.retry_policy JSON: an EXPONENTIAL default
// with a distinct initial_interval + backoff must be observable after decode.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_default_exponential_retry_policy_round_trips_into_task() {
    let (url, _container) = setup_db().await;
    let queue = "q620-retry-roundtrip";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_one_activity",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // A distinctive exponential policy: 4 attempts, 2500ms initial, ×2 backoff.
    let builder_policy = RetryPolicy::exponential(4, Duration::from_millis(2500));
    assert!(
        (builder_policy.backoff_coefficient - 2.0).abs() < f64::EPSILON,
        "sanity: exponential backoff coefficient is 2.0"
    );

    let registry = build_registry(
        vec![wf_info("wf_one_activity", wf_one_activity)],
        vec![act_info("echo", echo_activity, false, None, None)],
        Some(builder_policy),
        None,
    );
    let worker = build_worker(
        "w620-roundtrip",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
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
        "the exponential builder default max_attempts (4) must reach the task"
    );
    let policy_json = task
        .retry_policy
        .expect("the enqueued task must carry the resolved retry policy JSON");
    let decoded: RetryPolicy = serde_json::from_value(policy_json)
        .expect("task.retry_policy must decode into RetryPolicy");
    assert_eq!(
        decoded.max_attempts, 4,
        "decoded max_attempts must round-trip"
    );
    assert_eq!(
        decoded.initial_interval,
        Duration::from_millis(2500),
        "decoded initial_interval must round-trip (guards against dropping the \
         backoff curve while keeping max_attempts)"
    );
    assert!(
        (decoded.backoff_coefficient - 2.0).abs() < f64::EPSILON,
        "decoded backoff_coefficient (2.0 = exponential) must round-trip"
    );
}

// ---------------------------------------------------------------------------
// Item 8 (P3) — with builder defaults UNSET, a failing LOCAL activity records
// exactly ONE LocalActivityFailed (the local fallback is 1 attempt, vs 3 for a
// regular activity). Guards the local no-op path byte-for-byte.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_no_defaults_records_single_failure() {
    let (url, _container) = setup_db().await;
    let queue = "q620-local-noop";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(
        &mut conn,
        "wf_failing_local",
        serde_json::json!({"v": 1}),
        queue,
    )
    .await;

    // No builder default, no activity default, no call-site override → the
    // local fallback of a single attempt governs.
    let registry = build_registry(
        vec![wf_info("wf_failing_local", wf_failing_local)],
        vec![act_info("failing_local", failing_local, true, None, None)],
        None,
        None,
    );
    let worker = build_worker(
        "w620-local-noop",
        queue,
        Arc::clone(&registry),
        Duration::from_secs(60),
    );
    let pool = build_pool(&url);

    run_to_state(
        &url,
        &pool,
        worker,
        exec_id,
        "FAILED",
        Duration::from_secs(20),
    )
    .await;

    let history = load_history(&url, exec_id).await;
    assert_eq!(
        count_local_failed(&history),
        1,
        "with no defaults a failing local activity runs exactly once — a single \
         LocalActivityFailed (the local fallback of 1 attempt), byte-for-byte"
    );
}
