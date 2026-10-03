#![cfg(feature = "db")]
//! Shipped activity `start_to_close` default (issue #1808).
//!
//! A regular activity with no timeout and no heartbeat used to run forever on
//! a live worker. `WorkerConfig::default()` now bounds it at
//! `DEFAULT_ACTIVITY_START_TO_CLOSE` (10 minutes).
//!
//! Each test builds its registry through `HarvestBuilder`, so the shipped
//! default reaches the worker on the same path as in production. A test does
//! not wait 10 minutes. It moves `started_at` back in time and runs the
//! timeout scanner, so it checks both sides of the boundary.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::builder::{DEFAULT_ACTIVITY_START_TO_CLOSE, HarvestBuilder, WorkerConfig};
use autumn_harvest::error::TimeoutType;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::timeout::{self, TimeoutReason, find_timed_out_tasks};
use autumn_harvest::types::{ExecutionId, ShardId};
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
// DB setup.
// ---------------------------------------------------------------------------

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
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

/// An activity that never returns and never heartbeats.
fn hang(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        std::future::pending::<()>().await;
        Ok(serde_json::Value::Null)
    })
}

/// A workflow that calls `hang` on its own queue with no call-site timeout.
fn wf_calls_hang(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("hang", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

fn wf_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "wf_calls_hang",
        module: "activity_default_timeout_tests",
        handler: wf_calls_hang,
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

/// `hang` declares no timeout and no heartbeat. One attempt makes the first
/// timeout terminal, so the workflow fails and the test can end.
fn hang_info() -> ActivityInfo {
    ActivityInfo {
        name: "hang",
        module: "activity_default_timeout_tests",
        default_retry_policy: Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: hang,
    }
}

// ---------------------------------------------------------------------------
// Registry and worker.
// ---------------------------------------------------------------------------

/// Build the registry through `HarvestBuilder`, as production does.
fn registry_from_builder(config: WorkerConfig) -> Arc<HandlerRegistry> {
    let (registry, _dags, _schedules, _config) = HarvestBuilder::new()
        .workflows(vec![wf_info()])
        .activities(vec![hang_info()])
        .worker(config)
        .build()
        .into_worker_parts();
    Arc::new(registry)
}

fn build_worker(worker_id: &str, queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 2,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::ZERO,
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
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
// Seed and read helpers.
// ---------------------------------------------------------------------------

async fn seed_workflow(conn: &mut AsyncPgConnection, queue: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = serde_json::json!({"issue": 1808});
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "wf_calls_hang",
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone().into(),
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

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload workflow execution")
}

/// Wait until a worker runs the `hang` task, then return its row.
async fn wait_for_running_hang(url: &str, exec_id: ExecutionId) -> TaskQueueItem {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut conn = connect(url).await;
            let rows: Vec<TaskQueueItem> = harvest_task_queue::table
                .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
                .filter(harvest_task_queue::activity_name.eq(Some("hang".to_string())))
                .filter(harvest_task_queue::state.eq("RUNNING"))
                .select(TaskQueueItem::as_select())
                .load(&mut conn)
                .await
                .expect("reload task rows");
            if let Some(row) = rows.into_iter().next() {
                break row;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the worker must claim the hang task within 30s")
}

/// Move `started_at` back so the attempt looks `secs` old.
async fn backdate_start(conn: &mut AsyncPgConnection, task_id: Uuid, secs: i64) {
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET started_at = NOW() - ($2 * INTERVAL '1 second') \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::BigInt, _>(secs)
    .execute(conn)
    .await
    .expect("backdate started_at");
}

/// The scanner verdict for one task, or `None` if the task is in bounds.
async fn scanner_reason(conn: &mut AsyncPgConnection, task_id: Uuid) -> Option<TimeoutReason> {
    find_timed_out_tasks(conn)
        .await
        .expect("timeout scan")
        .into_iter()
        .find(|(t, _)| t.id == task_id)
        .map(|(_, reason)| reason)
}

fn default_secs() -> i64 {
    i64::try_from(DEFAULT_ACTIVITY_START_TO_CLOSE.as_secs()).expect("fits in i64")
}

/// Stop the worker. The `hang` future never yields, so do not wait forever.
async fn stop(worker: &Worker, handle: tokio::task::JoinHandle<()>) {
    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// AC1: a hung activity with no timeout times out at the shipped default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hung_activity_without_a_timeout_times_out_at_the_default() {
    let (url, _container) = setup_db().await;
    let queue = "q1808-default";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, queue).await;

    let registry = registry_from_builder(WorkerConfig::default());
    let worker = build_worker("w1808-default", queue, registry);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&run_pool).await;
    });

    let task = wait_for_running_hang(&url, exec_id).await;

    // Ten seconds inside the window: the scanner leaves the task alone.
    backdate_start(&mut conn, task.id, default_secs() - 10).await;
    assert_eq!(scanner_reason(&mut conn, task.id).await, None);

    // One second past the window: the scanner reclaims it.
    backdate_start(&mut conn, task.id, default_secs() + 1).await;
    assert_eq!(
        scanner_reason(&mut conn, task.id).await,
        Some(TimeoutReason::StartToClose),
        "a hung activity must time out at the default, not run forever"
    );
    assert_eq!(
        task.start_to_close,
        Some(chrono::Duration::from_std(DEFAULT_ACTIVITY_START_TO_CLOSE).unwrap()),
        "the task row must carry the shipped default"
    );
    timeout::enforce_timeouts_once(
        &mut conn,
        &NoOpMetrics,
        Duration::from_secs(60),
        &None,
        &[ShardId::new(0)],
        None,
        None,
        60,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    )
    .await
    .expect("timeout sweep");

    // One attempt only, so the timeout fails the workflow.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while load_execution(&url, exec_id).await.state != "FAILED" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the workflow must fail after the timeout"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    stop(&worker, handle).await;

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load history")
        .events;
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::StartToClose,
                ..
            }
        )),
        "history must record a start_to_close timeout; history={history:?}"
    );
}

/// The opt-out restores the old behaviour: no timeout on the task row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opted_out_default_leaves_a_hung_activity_unbounded() {
    let (url, _container) = setup_db().await;
    let queue = "q1808-opt-out";
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, queue).await;

    let config = WorkerConfig::default().without_default_activity_start_to_close();
    let registry = registry_from_builder(config);
    let worker = build_worker("w1808-opt-out", queue, registry);
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&run_pool).await;
    });

    let task = wait_for_running_hang(&url, exec_id).await;
    assert_eq!(task.start_to_close, None);

    // A day old and still in bounds: nothing bounds this attempt.
    backdate_start(&mut conn, task.id, 24 * 3600).await;
    assert_eq!(scanner_reason(&mut conn, task.id).await, None);

    stop(&worker, handle).await;
}
