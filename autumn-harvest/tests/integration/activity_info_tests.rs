#![cfg(feature = "db")]
//! `ActivityContext::info()` / `deadline()` / `time_remaining()` behavioural
//! tests — issue #783.
//!
//! These prove the *worker threading*: the pure helpers and the accessor
//! surface are unit-tested in `src/context.rs`, but only a real dispatch can
//! show that the values an author reads actually describe the run and agree
//! with the mechanism that kills the attempt.
//!
//! * TEST A (`regular_dispatch_reports_the_owning_run_and_a_usable_task_id`):
//!   AC1 + AC3. A queue-dispatched activity captures `ctx.info()`; every field
//!   matches the seeded run, `execution_id` round-trips through its string form
//!   back to the seeded `ExecutionId` (the management-API path segment), and
//!   `task_id` is the real `harvest_task_queue` PK — the id the operator
//!   `retry-now` / `fail-now` routes address.
//!
//! * TEST B (`reported_deadline_agrees_with_the_start_to_close_scanner`): AC2,
//!   in two composed halves. Part 1 asserts the reported deadline *is* the
//!   `started_at + start_to_close` arithmetic read off the very row the scanner
//!   reads — exact `DateTime` equality, so a `scheduled_at` anchor fails. Part 2
//!   drives the real `timeout::find_timed_out_tasks` against two probe rows
//!   positioned by shifting `started_at` around the scanner's own `NOW()`: one
//!   just short of that same arithmetic (spared) and one just past it
//!   (reclaimed). Together they pin `reported == f(row)` and `scanner enforces
//!   f(row)`. (The scanner takes no injectable clock, hence the probe rows
//!   rather than a probe *time*.)
//!
//! * TEST C (`schedule_to_close_shortens_the_reported_deadline`): the
//!   min-of-two-clocks rule. With a `schedule_to_close` closer than the
//!   per-attempt `start_to_close`, the reported deadline is the *earlier*
//!   one — over-reporting here would let an activity work right up to a kill
//!   that discards everything.
//!
//! * TEST D (`identity_is_stable_across_a_real_retry`): AC4. A real
//!   worker-driven retry: identity fields are byte-identical across attempts 1
//!   and 2 while `attempt` advances and the deadline moves forward.
//!
//! * TEST E (`local_dispatch_reports_is_local_and_the_configured_cap`): AC5.
//!   A local activity reports `is_local == true`, `task_id == None`, a bounded
//!   deadline drawn from `WorkerConfig::max_local_activity_start_to_close` when
//!   `start_to_close` is unset, and the *cap* (not the larger declared value)
//!   when the declared budget exceeds it.
//!
//! * TEST F (`checkpointing_activity_loses_zero_completed_work`): the success
//!   metric. An activity processes items one at a time, heartbeating a
//!   checkpoint after each, and returns early once
//!   `ctx.is_expiring_within(reserve)` trips. The retry resumes from
//!   `heartbeat_details` and processes only the remainder: **0%** of
//!   already-completed work is re-executed. A negative control in the same test
//!   shows the same activity WITHOUT the deadline guard re-executing work.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted
//! with the full migration bundle.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::timeout::{TimeoutReason, find_timed_out_tasks};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{ActivityExecutionInfo, WorkflowContext, store};

use chrono::{DateTime, Utc};
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
    autumn_harvest::test_init_sql().as_bytes().to_vec()
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
// Worker + registry construction.
// ---------------------------------------------------------------------------

/// Builds a registry whose shared state carries a fresh observation sink,
/// returning both so the test can read what its handlers observed.
fn build_registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
) -> (Arc<HandlerRegistry>, Arc<Observations>) {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::new(NoOpMetrics) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let observations = Arc::new(Observations::default());
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Observations>>(),
        Box::new(Arc::clone(&observations)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        Arc::new(state),
        telemetry,
    ));
    (registry, observations)
}

fn worker_config(worker_id: &str, local_cap: Duration) -> WorkerRuntimeConfig {
    WorkerRuntimeConfig {
        dr_fencing: false,
        worker_id: worker_id.to_string(),
        // Two queues so `info().queue_name` is falsifiable: with a single
        // "default" queue, the workflow queue, the activity queue and the
        // asserted value are all the same string, so the assertion could not
        // distinguish "the activity's queue" from a hardcoded constant.
        queues: vec!["default".to_string(), "activity-q".to_string()],
        notification_database_url: None,
        max_concurrent_workflows: 2,
        max_concurrent_activities: 2,
        poll_interval: Duration::from_millis(25),
        shutdown_timeout: Duration::from_secs(1),
        cancellation_grace_period: Duration::from_secs(1),
        sticky_timeout: Duration::from_secs(5),
        max_local_activity_start_to_close: local_cap,
        shard_assignments: vec![ShardId::new(0)],
        worker_heartbeat_interval: Duration::from_secs(5),
        build_id: String::new(),
        deployment_name: None,
        workflow_cache_size: 1000,
        priority_aging_secs: None,
        unknown_target_grace_window: Duration::from_secs(5),
        poison_pill_threshold: 3,
        capability_miss_max_redeliveries: 5,
        workflow_task_timeout: Duration::from_secs(30),
        workflow_panic_max_attempts: 3,
        labels: HashMap::new(),
        queue_weights: HashMap::new(),
        max_workflow_pause_duration: Duration::from_secs(24 * 3600),
        max_workflow_history_events: None,
        shard_notification_database_urls: Vec::new(),
        sharded_pool: None,
        slot_tuner: None,
        max_concurrent_sessions: 0,
    }
}

fn build_worker(worker_id: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    build_worker_with_local_cap(worker_id, registry, Duration::from_secs(60))
}

fn build_worker_with_local_cap(
    worker_id: &str,
    registry: Arc<HandlerRegistry>,
    local_cap: Duration,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(worker_config(worker_id, local_cap), registry).expect("worker should build"),
    )
}

fn workflow_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "activity_info_tests",
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

/// `ActivityInfo` with only the knobs these tests vary.
fn activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    start_to_close: Option<Duration>,
    schedule_to_close: Option<Duration>,
    retry: Option<RetryPolicy>,
    is_local: bool,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_info_tests",
        default_retry_policy: retry,
        default_start_to_close: start_to_close,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: schedule_to_close,
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

/// Like [`activity_info`] but with a heartbeat timeout, for the checkpointing
/// activities (a heartbeat is rejected without one).
fn heartbeating_activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    start_to_close: Duration,
    retry: RetryPolicy,
) -> ActivityInfo {
    ActivityInfo {
        default_heartbeat_timeout: Some(Duration::from_secs(30)),
        ..activity_info(
            name,
            handler,
            Some(start_to_close),
            None,
            Some(retry),
            false,
        )
    }
}

// ---------------------------------------------------------------------------
// Seeding + read helpers.
// ---------------------------------------------------------------------------

/// Seeds a RUNNING execution plus its workflow task. Returns
/// `(exec_id, workflow_id)` so a test can assert `info().workflow_id`.
async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
) -> (ExecutionId, String) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_id = format!("wf-{}", exec_id.as_uuid());
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
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

    let mut params = EnqueueParams::new("default", TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");

    (exec_id, workflow_id)
}

/// Seeds a standalone RUNNING activity task row with a chosen
/// `(started_at, start_to_close)` pair so the real start-to-close scanner can
/// be driven against a deadline positioned relative to its own `NOW()`.
/// Returns the task id.
async fn seed_running_activity_probe(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    start_to_close: chrono::Duration,
    started_at: DateTime<Utc>,
) -> Uuid {
    let (exec_id, _) = seed_workflow(conn, workflow_name, serde_json::json!({})).await;
    let mut params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("observe_activity".to_string());
    params.start_to_close = Some(start_to_close);
    let task_id = queue::enqueue(conn, &params)
        .await
        .expect("enqueue probe activity");
    diesel::update(harvest_task_queue::table.find(task_id))
        .set((
            harvest_task_queue::state.eq("RUNNING"),
            harvest_task_queue::worker_id.eq("probe"),
            harvest_task_queue::started_at.eq(started_at),
        ))
        .execute(conn)
        .await
        .expect("mark probe RUNNING at the chosen started_at");

    // `seed_workflow` also enqueues a WORKFLOW task for this probe run, which
    // no test ever drives. Left PENDING it is claimable, so under a shared
    // `HARVEST_TEST_DATABASE_URL` database a later test's worker would pick up
    // a handler-less task. Cancel it: the probe only needs its activity row.
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(harvest_task_queue::task_type.eq("workflow")),
    )
    .set(harvest_task_queue::state.eq("CANCELLED"))
    .execute(conn)
    .await
    .expect("retire the probe run's workflow task");

    task_id
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
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution did not reach state {want} within {timeout:?}"))
}

// ---------------------------------------------------------------------------
// Observation plumbing.
//
// An activity handler is a plain `fn`, so it cannot capture per-test state.
// Rather than a process-wide static (which would make these tests order- and
// parallelism-dependent within the shared integration binary), each test
// registers its own `Arc<Observations>` in the registry's shared state and the
// handler reads it back with `ctx.state::<Arc<Observations>>()` — the same
// pattern `integration_e2e.rs` uses for its retry observations.
// ---------------------------------------------------------------------------

/// Everything a handler observed, in attempt order.
#[derive(Default)]
struct Observations {
    infos: Mutex<Vec<ActivityExecutionInfo>>,
    deadlines: Mutex<Vec<Option<DateTime<Utc>>>>,
    /// Items an attempt actually processed (the 0%-rework ledger).
    processed: Mutex<Vec<u32>>,
}

impl Observations {
    fn record(&self, ctx: &autumn_harvest::ActivityContext) {
        self.infos.lock().unwrap().push(ctx.info());
        self.deadlines.lock().unwrap().push(ctx.deadline());
    }

    fn infos(&self) -> Vec<ActivityExecutionInfo> {
        self.infos.lock().unwrap().clone()
    }

    fn deadlines(&self) -> Vec<Option<DateTime<Utc>>> {
        self.deadlines.lock().unwrap().clone()
    }

    fn processed(&self) -> Vec<u32> {
        self.processed.lock().unwrap().clone()
    }
}

/// Reads the per-test observation sink out of the registry's shared state.
fn observed(ctx: &autumn_harvest::ActivityContext) -> Arc<Observations> {
    Arc::clone(
        ctx.state::<Arc<Observations>>()
            .expect("Observations must be registered in shared state"),
    )
}

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

/// Workflow calling the observed regular activity.
fn wf_observe(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("observe_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// TEST A's workflow: the bounded activity on the default queue, then an
/// unbounded one on a *different* queue. Sequential, so `observed.infos()` is
/// index-stable — [0] bounded/"default", [1] unbounded/"activity-q".
fn wf_observe_pair(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("observe_activity", input.clone(), "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("unbounded_activity", input, "activity-q")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Regular activity that snapshots `ctx.info()` / `ctx.deadline()` and returns.
fn observe_activity(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        observed(ctx).record(ctx);
        Ok(serde_json::json!({"ok": true}))
    })
}

/// Same, but declared with NO `start_to_close` and NO `schedule_to_close`, and
/// dispatched on a non-default queue — so a single dispatch pins both AC2's
/// "`None` when unbounded" clause through the real worker (a mutant that
/// substituted a fabricated budget would be caught) and the `queue_name` field
/// against a value that differs from the workflow's own queue.
fn unbounded_activity(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        observed(ctx).record(ctx);
        Ok(serde_json::json!({"ok": true}))
    })
}

/// Workflow calling the observed LOCAL activity.
fn wf_observe_local(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("observe_local_activity", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

fn observe_local_activity(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        observed(ctx).record(ctx);
        Ok(serde_json::json!({"ok": true}))
    })
}

/// Workflow calling the LOCAL activity whose declared budget exceeds the cap.
fn wf_observe_local_over_cap(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("observe_local_over_cap", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

fn observe_local_over_cap(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        observed(ctx).record(ctx);
        Ok(serde_json::json!({"ok": true}))
    })
}

/// Regular activity that snapshots identity, then FAILS on attempt 1 so the
/// worker retries it and a second snapshot is taken (AC4).
fn retry_observe_activity(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        observed(ctx).record(ctx);
        if ctx.attempt() == 1 {
            return Err("transient".to_string());
        }
        Ok(serde_json::json!({"ok": true}))
    })
}

fn wf_retry_observe(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("retry_observe_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

// --- Success metric: checkpointing activity ---------------------------------

/// Total items the checkpointing activity must process across all attempts.
/// Twelve items x 700ms is ~8.4s of work against an 8s per-attempt budget, so a
/// single attempt provably cannot finish them all — the retry is real, not
/// incidental, which is what makes the 0%-rework claim meaningful.
const TOTAL_ITEMS: u32 = 12;
/// Simulated per-item cost.
const ITEM_COST: Duration = Duration::from_millis(700);
/// Per-attempt `start_to_close` budget.
const ATTEMPT_BUDGET: Duration = Duration::from_secs(8);
/// Reserve: return early while there is still time to flush the checkpoint.
///
/// The guard is checked once per item, so remaining-at-trip lands in
/// `(RESERVE - ITEM_COST, RESERVE]` — 3.8s..4.5s. Subtracting `FLUSH_WAIT`
/// leaves 1.8s..2.5s of clearance before the kill, and that margin is
/// independent of dispatch overhead because the trip is evaluated against the
/// real deadline rather than an item count. A kill mid-flush would lose the
/// last checkpoint and the test would (correctly) fail.
const RESERVE: Duration = Duration::from_millis(4_500);
/// The batched heartbeat flusher ticks once a second and does NOT drain on
/// cancel, so a checkpoint sent immediately before returning can be lost. Wait
/// out one full interval plus a full second of slack — the slack has to cover
/// `pool.get()` and the UPDATE being *issued*, so it is load-bearing, not
/// cosmetic.
const FLUSH_WAIT: Duration = Duration::from_secs(2);

/// Reads `{"next": n}` from the heartbeat checkpoint, defaulting to 0.
///
/// A read *failure* is fatal rather than silently resuming from 0: that would
/// re-execute completed work, which the 0%-rework assertion would then report
/// as a duplicate — a correct failure with a misleading cause.
fn checkpoint_start(ctx: &autumn_harvest::ActivityContext) -> u32 {
    ctx.heartbeat_details::<serde_json::Value>()
        .expect("reading the resume checkpoint must not fail")
        .and_then(|v| v.get("next").and_then(serde_json::Value::as_u64))
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0)
}

/// The success-metric activity: processes items one at a time from the
/// checkpoint, heartbeating after each, and returns early (retryable) once the
/// attempt deadline is within `RESERVE`. The retry resumes from the checkpoint.
fn checkpointing_activity(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        let obs = observed(ctx);
        let mut next = checkpoint_start(ctx);
        while next < TOTAL_ITEMS {
            if ctx.is_expiring_within(RESERVE) {
                // Give the batched flusher time to persist the last checkpoint
                // before this attempt's row is requeued.
                tokio::time::sleep(FLUSH_WAIT).await;
                return Err(format!("out of time at item {next}"));
            }
            tokio::time::sleep(ITEM_COST).await;
            obs.processed.lock().unwrap().push(next);
            next += 1;
            ctx.heartbeat(serde_json::json!({"next": next}))
                .await
                .map_err(|e| e.to_string())?;
        }
        tokio::time::sleep(FLUSH_WAIT).await;
        Ok(serde_json::json!({"processed": TOTAL_ITEMS}))
    })
}

fn wf_checkpointing(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("checkpointing_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Negative control: identical, but WITHOUT the deadline guard. It is killed
/// mid-item by the `start_to_close` scanner — a terminal, non-retryable
/// timeout — so the remaining items are never processed at all and the run
/// fails. This is the failure mode `ctx.time_remaining()` exists to avoid.
fn unguarded_activity(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        let obs = observed(ctx);
        let mut next = checkpoint_start(ctx);
        while next < TOTAL_ITEMS {
            tokio::time::sleep(ITEM_COST).await;
            obs.processed.lock().unwrap().push(next);
            next += 1;
            ctx.heartbeat(serde_json::json!({"next": next}))
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(serde_json::json!({"processed": TOTAL_ITEMS}))
    })
}

fn wf_unguarded(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("unguarded_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

// ---------------------------------------------------------------------------
// TEST A — AC1 + AC3.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regular_dispatch_reports_the_owning_run_and_a_usable_task_id() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let (exec_id, workflow_id) =
        seed_workflow(&mut conn, "info_wf", serde_json::json!({"n": 1})).await;

    let (registry, observed) = build_registry(
        vec![workflow_info("info_wf", wf_observe_pair)],
        vec![
            activity_info(
                "observe_activity",
                observe_activity,
                Some(Duration::from_secs(30)),
                None,
                None,
                false,
            ),
            // No start_to_close, no schedule_to_close: genuinely unbounded.
            activity_info(
                "unbounded_activity",
                unbounded_activity,
                None,
                None,
                None,
                false,
            ),
        ],
    );
    let worker = build_worker("w-info-a", registry);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });

    wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await;
    worker.shutdown();
    let _ = handle.await;

    let infos = observed.infos();
    assert_eq!(
        infos.len(),
        2,
        "both activities should have run exactly once"
    );
    let info = &infos[0];

    // AC1 — every required field describes the real run.
    assert_eq!(info.execution_id, exec_id);
    assert_eq!(info.workflow_id, workflow_id);
    assert_eq!(info.workflow_type, "info_wf");
    assert_eq!(info.activity_type, "observe_activity");
    assert_eq!(info.attempt, 1);
    assert!(!info.is_local, "a queue dispatch is not local");
    assert_eq!(info.queue_name, "default");

    // `max_attempts` must mirror the queue row, not a hand-written constant —
    // the engine applies its own default when no retry policy is declared.
    let row_max_attempts: i32 = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .filter(harvest_task_queue::activity_name.eq("observe_activity"))
        .select(harvest_task_queue::max_attempts)
        .first(&mut conn)
        .await
        .expect("load max_attempts");
    assert_eq!(
        i64::from(info.max_attempts),
        i64::from(row_max_attempts),
        "info().max_attempts must mirror the task row"
    );

    // AC1 — `activity_id` is the id recorded in history, not a fresh UUID.
    let history = {
        let mut conn = connect(&url).await;
        store::load_history(&mut conn, exec_id)
            .await
            .expect("load_history")
            .events
    };
    let scheduled_id = history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if name == "observe_activity" => Some(*activity_id),
            _ => None,
        })
        .expect("ActivityScheduled must be recorded");
    assert_eq!(
        info.activity_id, scheduled_id,
        "info().activity_id must be the recorded scheduled id"
    );

    assert_execution_id_is_the_api_path_segment(&url, info, exec_id).await;

    // `task_id` is the real queue-row PK the operator retry-now/fail-now routes
    // address — a `None` or a random UUID here would 404 an operator.
    let task_id = info.task_id.expect("a queue dispatch has a task row");
    let matching: i64 = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq(task_id))
        .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count task row");
    assert_eq!(
        matching, 1,
        "task_id must address this execution's task row"
    );

    assert_unbounded_sibling(&infos[1], info, observed.deadlines()[1]);
}

/// AC3: the reported `execution_id` IS the management-API path segment — it
/// round-trips through its rendered string form back to the same id, and that
/// id selects exactly the seeded row.
async fn assert_execution_id_is_the_api_path_segment(
    url: &str,
    info: &ActivityExecutionInfo,
    exec_id: ExecutionId,
) {
    let rendered = info.execution_id.to_string();
    assert!(
        !rendered.contains("ExecutionId"),
        "Display must render the bare id, got {rendered}"
    );
    let parsed: ExecutionId = rendered.parse().expect("rendered id must parse back");
    assert_eq!(parsed, exec_id);
    let looked_up = load_execution(url, parsed).await;
    assert_eq!(looked_up.id, exec_id.as_uuid());
}

/// The second half of TEST A's single run, on the sibling activity dispatched
/// with no budget and on a different queue.
///
/// Kept as a helper so both halves observe the *same* run — the shared
/// `execution_id` next to the differing `queue_name` is what makes the queue
/// assertion attributable to the dispatch rather than to the workflow.
fn assert_unbounded_sibling(
    unbounded: &ActivityExecutionInfo,
    bounded: &ActivityExecutionInfo,
    unbounded_deadline: Option<DateTime<Utc>>,
) {
    // AC2 — "None when unbounded", proven through the real worker rather than
    // only through the pure helper. A mutant that fabricated a budget here
    // (`.or(Some(now + 1h))`) would make `is_expiring_within` fire spuriously
    // on every unbounded activity in the fleet.
    assert_eq!(unbounded.activity_type, "unbounded_activity");
    assert_eq!(
        unbounded_deadline, None,
        "an activity with neither start_to_close nor schedule_to_close is unbounded"
    );

    // `queue_name` is the activity's OWN dispatch queue, not the workflow's.
    // Asserted against a value that differs from the workflow queue, so a
    // hardcoded "default" would fail here.
    assert_eq!(
        unbounded.queue_name, "activity-q",
        "info().queue_name must be the activity's dispatch queue"
    );
    assert_eq!(
        bounded.queue_name, "default",
        "and the sibling on the default queue still reports its own"
    );
    // Same run, so identity's run-level half is shared; the activity-level half
    // differs. This is what makes `queue_name` attributable to the dispatch.
    assert_eq!(unbounded.execution_id, bounded.execution_id);
    assert_ne!(unbounded.activity_id, bounded.activity_id);
}

// ---------------------------------------------------------------------------
// TEST B — AC2: the deadline agrees with the scanner that enforces it.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reported_deadline_agrees_with_the_start_to_close_scanner() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let (exec_id, _) = seed_workflow(&mut conn, "stc_wf", serde_json::json!({})).await;

    let start_to_close = Duration::from_secs(30);
    let (registry, observed) = build_registry(
        vec![workflow_info("stc_wf", wf_observe)],
        vec![activity_info(
            "observe_activity",
            observe_activity,
            Some(start_to_close),
            None,
            None,
            false,
        )],
    );
    let worker = build_worker("w-info-b", registry);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });
    wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await;
    worker.shutdown();
    let _ = handle.await;

    let deadline = observed.deadlines()[0].expect("a start_to_close activity has a deadline");

    // ── Part 1: the reported deadline is exactly the arithmetic the scanner
    // performs — `started_at + start_to_close` — read straight off the row the
    // scanner would have compared against. A "plausible-looking" deadline
    // computed from any other anchor (e.g. `scheduled_at`, or a local
    // `Instant`) fails here.
    let (row_started_at, row_stc): (Option<DateTime<Utc>>, Option<chrono::Duration>) =
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(harvest_task_queue::task_type.eq("activity"))
            .select((
                harvest_task_queue::started_at,
                harvest_task_queue::start_to_close,
            ))
            .first(&mut conn)
            .await
            .expect("load the activity task row");
    let row_started_at = row_started_at.expect("a dispatched activity has started_at");
    let row_stc = row_stc.expect("start_to_close was configured");
    assert_eq!(
        deadline,
        row_started_at + row_stc,
        "the reported deadline must be `started_at + start_to_close` from the \
         very row the start-to-close scanner reads"
    );

    // ── Part 2: prove that arithmetic really is what the scanner enforces, by
    // driving the real `find_timed_out_tasks` against two probe rows that
    // straddle its `NOW()`. The scanner has no injectable clock, so the probes
    // are positioned by shifting `started_at` instead: same `start_to_close`,
    // one deadline 5s in the future and one 5s in the past.
    let not_yet = seed_running_activity_probe(
        &mut conn,
        "stc_probe_future",
        row_stc,
        // started_at such that started_at + stc == now + 5s
        Utc::now() - row_stc + chrono::Duration::seconds(5),
    )
    .await;
    let overdue = seed_running_activity_probe(
        &mut conn,
        "stc_probe_past",
        row_stc,
        // started_at such that started_at + stc == now - 5s
        Utc::now() - row_stc - chrono::Duration::seconds(5),
    )
    .await;

    let timed_out = find_timed_out_tasks(&mut conn)
        .await
        .expect("run the real start-to-close scanner");
    let start_to_close_ids: Vec<uuid::Uuid> = timed_out
        .iter()
        .filter(|(_, reason)| *reason == TimeoutReason::StartToClose)
        .map(|(task, _)| task.id)
        .collect();

    assert!(
        !start_to_close_ids.contains(&not_yet),
        "a row whose `started_at + start_to_close` is still in the future must \
         NOT be reclaimed — so a deadline reported EARLIER than this would be \
         pessimistic"
    );
    assert!(
        start_to_close_ids.contains(&overdue),
        "a row whose `started_at + start_to_close` has passed MUST be reclaimed \
         — so a deadline reported LATER than this would over-report the time \
         available"
    );
}

// ---------------------------------------------------------------------------
// TEST C — min-of-two-clocks.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_to_close_shortens_the_reported_deadline() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let (exec_id, _) = seed_workflow(&mut conn, "stcc_wf", serde_json::json!({})).await;

    // schedule_to_close (5s from schedule time) is much closer than the
    // per-attempt start_to_close (10 minutes).
    let (registry, observed) = build_registry(
        vec![workflow_info("stcc_wf", wf_observe)],
        vec![activity_info(
            "observe_activity",
            observe_activity,
            Some(Duration::from_secs(600)),
            Some(Duration::from_secs(5)),
            None,
            false,
        )],
    );
    let worker = build_worker("w-info-c", registry);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });
    wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await;
    worker.shutdown();
    let _ = handle.await;

    let deadline = observed.deadlines()[0].expect("a bounded activity has a deadline");
    let stc_at: Option<DateTime<Utc>> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(harvest_task_queue::schedule_to_close_at)
        .first(&mut conn)
        .await
        .expect("load schedule_to_close_at");
    let stc_at = stc_at.expect("schedule_to_close_at must be persisted");

    assert_eq!(
        deadline, stc_at,
        "the reported deadline must be the EARLIER schedule_to_close clock, \
         not the far-away start_to_close budget"
    );
    // The min really did bite: the per-attempt `start_to_close` clock was the
    // later of the two, so an un-shortened deadline would have been
    // `started_at + 600s`. Compare against that exact value, not against a
    // `Utc::now()`-relative bound — `now > started_at` makes the latter true
    // even when the shortening never happened.
    let row_started_at: Option<DateTime<Utc>> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(harvest_task_queue::started_at)
        .first(&mut conn)
        .await
        .expect("load started_at");
    let unshortened =
        row_started_at.expect("a claimed row has started_at") + chrono::Duration::seconds(600);
    assert!(
        deadline < unshortened,
        "min-of-two-clocks did not shorten the deadline: reported {deadline}, \
         un-shortened start_to_close clock would be {unshortened}"
    );
}

// ---------------------------------------------------------------------------
// TEST D — AC4: identity stability across a real retry.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_is_stable_across_a_real_retry() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let (exec_id, workflow_id) = seed_workflow(&mut conn, "retry_wf", serde_json::json!({})).await;

    let (registry, observed) = build_registry(
        vec![workflow_info("retry_wf", wf_retry_observe)],
        vec![activity_info(
            "retry_observe_activity",
            retry_observe_activity,
            Some(Duration::from_secs(30)),
            None,
            Some(RetryPolicy::fixed(2, Duration::from_millis(50))),
            false,
        )],
    );
    let worker = build_worker("w-info-d", registry);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });
    wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(40)).await;
    worker.shutdown();
    let _ = handle.await;

    let infos = observed.infos();
    assert_eq!(
        infos.len(),
        2,
        "expected exactly two attempts, got {infos:?}"
    );
    let (first, second) = (&infos[0], &infos[1]);

    // AC4 — the whole identity is byte-identical. Comparing the *struct*
    // rather than a hand-written field list keeps this honest as fields are
    // added: a new identity field automatically joins the assertion.
    assert_eq!(
        first.identity(),
        second.identity(),
        "identity must be stable across attempts"
    );
    assert_eq!(first.execution_id, exec_id);
    assert_eq!(first.workflow_id, workflow_id);
    assert_eq!(first.workflow_type, "retry_wf");
    assert_eq!(first.activity_type, "retry_observe_activity");

    // ...while the per-attempt fields advance.
    assert_eq!(first.attempt, 1);
    assert_eq!(second.attempt, 2);
    assert_eq!(first.max_attempts, 2);
    assert_eq!(second.max_attempts, 2);

    // AC4 is "only `attempt` and the deadline advance" — so the per-dispatch
    // fields outside `identity()` must hold too. A retry reuses the same queue
    // row, so `task_id` is stable as well.
    assert_eq!(first.is_local, second.is_local);
    assert_eq!(first.queue_name, second.queue_name);
    assert_eq!(
        first.task_id, second.task_id,
        "a retry reuses the same harvest_task_queue row"
    );

    let deadlines = observed.deadlines();
    let (d1, d2) = (
        deadlines[0].expect("attempt 1 deadline"),
        deadlines[1].expect("attempt 2 deadline"),
    );
    assert!(
        d2 > d1,
        "the retry gets a fresh per-attempt budget: {d2} must be after {d1}"
    );
}

// ---------------------------------------------------------------------------
// TEST E — AC5: local dispatch.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_dispatch_reports_is_local_and_the_configured_cap() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let local_cap = Duration::from_secs(7);

    // --- (1) start_to_close UNSET: the deadline must come from the cap. ---
    let (exec_id, workflow_id) = seed_workflow(&mut conn, "local_wf", serde_json::json!({})).await;
    let (registry, observed) = build_registry(
        vec![workflow_info("local_wf", wf_observe_local)],
        vec![activity_info(
            "observe_local_activity",
            observe_local_activity,
            None,
            None,
            None,
            true,
        )],
    );
    let worker = build_worker_with_local_cap("w-info-e1", registry, local_cap);
    let before = Utc::now();
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });
    wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await;
    worker.shutdown();
    let _ = handle.await;

    let info = observed.infos().remove(0);
    assert!(info.is_local, "a local activity must report is_local");
    assert_eq!(
        info.task_id, None,
        "a local activity has no harvest_task_queue row"
    );
    assert_eq!(info.execution_id, exec_id);
    assert_eq!(info.workflow_id, workflow_id);
    assert_eq!(info.workflow_type, "local_wf");
    assert_eq!(info.activity_type, "observe_local_activity");
    assert_eq!(
        info.queue_name, "default",
        "a local activity reports the owning workflow's queue"
    );

    let after = Utc::now();
    let deadline = observed.deadlines()[0].expect("a local activity is always capped");
    let cap_budget = chrono::Duration::from_std(local_cap).unwrap();
    // A two-sided sandwich, not `>= before`: the deadline is `dispatch + cap`
    // and `before <= dispatch <= after`, so it must be at least a FULL cap past
    // `before`. A one-sided `>= before` would pass for any budget at all —
    // e.g. a mutant that hardcoded `now + 1s` and ignored the cap entirely.
    assert!(
        deadline >= before + cap_budget,
        "an unset start_to_close must report the FULL local cap ({local_cap:?}) \
         measured from dispatch; got {deadline}, which is under {before} + cap"
    );
    assert!(
        deadline <= after + cap_budget,
        "the deadline must not exceed one cap past the run, got {deadline}"
    );

    // --- (2) start_to_close ABOVE the cap: the cap must win. ---
    let (exec2, _) = seed_workflow(&mut conn, "local_cap_wf", serde_json::json!({})).await;
    let (registry2, observed2) = build_registry(
        vec![workflow_info("local_cap_wf", wf_observe_local_over_cap)],
        // Declared far above the 7s worker cap.
        vec![activity_info(
            "observe_local_over_cap",
            observe_local_over_cap,
            Some(Duration::from_secs(600)),
            None,
            None,
            true,
        )],
    );
    let worker2 = build_worker_with_local_cap("w-info-e2", registry2, local_cap);
    let before2 = Utc::now();
    let handle2 = tokio::spawn({
        let worker = Arc::clone(&worker2);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });
    wait_for_state(&url, exec2, "COMPLETED", Duration::from_secs(30)).await;
    worker2.shutdown();
    let _ = handle2.await;

    let after2 = Utc::now();
    let capped = observed2.deadlines()[0].expect("a local activity is always capped");
    // Same two-sided sandwich. The upper bound is what proves the clamp (the
    // declared 600s budget would blow past it); the lower bound proves the
    // clamp landed *at* the cap rather than somewhere arbitrarily below it.
    assert!(
        capped <= after2 + cap_budget,
        "an above-cap start_to_close must be clamped to the cap ({local_cap:?}), \
         got {capped}"
    );
    assert!(
        capped >= before2 + cap_budget,
        "the clamped deadline must be the FULL cap from dispatch, got {capped}"
    );
}

// ---------------------------------------------------------------------------
// TEST F — success metric: zero mid-timeout progress loss.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpointing_activity_loses_zero_completed_work() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    // --- Guarded: return early on `is_expiring_within`, resume from checkpoint. ---
    let (exec_id, _) = seed_workflow(&mut conn, "ckpt_wf", serde_json::json!({})).await;
    let (registry, observed) = build_registry(
        vec![workflow_info("ckpt_wf", wf_checkpointing)],
        vec![heartbeating_activity_info(
            "checkpointing_activity",
            checkpointing_activity,
            ATTEMPT_BUDGET,
            RetryPolicy::fixed(5, Duration::from_millis(50)),
        )],
    );
    let worker = build_worker("w-info-f", registry);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });
    wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(90)).await;
    worker.shutdown();
    let _ = handle.await;

    let processed = observed.processed();

    // The metric: every item was processed EXACTLY once across all attempts.
    // Duplicates here are precisely "re-executed already-completed work".
    let mut sorted = processed.clone();
    sorted.sort_unstable();
    let expected: Vec<u32> = (0..TOTAL_ITEMS).collect();
    assert_eq!(
        sorted, expected,
        "0% of completed work may be re-executed; ledger was {processed:?}"
    );

    // The test is only meaningful if a retry actually happened — otherwise a
    // single attempt would trivially satisfy "no duplicates".
    let attempts: i32 = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(harvest_task_queue::attempt)
        .first(&mut conn)
        .await
        .expect("load attempt count");
    assert!(
        attempts > 1,
        "the activity must have been retried for this metric to mean anything"
    );

    // --- Negative control: same work, no deadline guard. ---
    //
    // Without the guard the attempt is killed by the `start_to_close` scanner,
    // which is terminal (non-retryable), so the run fails with strictly fewer
    // than TOTAL_ITEMS processed. This is what makes the guarded result above
    // attributable to `ctx.is_expiring_within` rather than to luck.
    let (exec2, _) = seed_workflow(&mut conn, "unguarded_wf", serde_json::json!({})).await;
    let (registry2, observed2) = build_registry(
        vec![workflow_info("unguarded_wf", wf_unguarded)],
        vec![heartbeating_activity_info(
            "unguarded_activity",
            unguarded_activity,
            ATTEMPT_BUDGET,
            RetryPolicy::fixed(5, Duration::from_millis(50)),
        )],
    );
    let worker2 = build_worker("w-info-f2", registry2);
    let handle2 = tokio::spawn({
        let worker = Arc::clone(&worker2);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });
    let control_state = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let e = load_execution(&url, exec2).await;
            if e.state != "RUNNING" && e.state != "SUSPENDED" {
                break e.state;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the unguarded control must reach a terminal state");
    worker2.shutdown();
    let _ = handle2.await;

    assert_control_work_was_discarded(&url, &mut conn, exec2, &control_state, &observed2).await;
}

/// The contrast that makes the guard load-bearing: the guarded run reached
/// COMPLETED with every item durably accounted for exactly once, while the
/// unguarded one — doing the same work at the same cost under the same budget —
/// had its work DISCARDED when the deadline killed it.
async fn assert_control_work_was_discarded(
    url: &str,
    conn: &mut AsyncPgConnection,
    exec2: ExecutionId,
    control_state: &str,
    observed2: &Arc<Observations>,
) {
    let control = observed2.processed();
    assert_eq!(
        control_state, "FAILED",
        "an unguarded activity must be killed by the start_to_close scanner; \
         in-process ledger was {control:?}"
    );

    // Pin the control to the *specific* failure mode, so a future change that
    // fails it for an unrelated reason does not quietly keep this test green.
    let control_error: Option<String> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(exec2.as_uuid()))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(harvest_task_queue::error)
        .first(conn)
        .await
        .expect("load control activity error");
    let control_error = control_error.unwrap_or_default();
    assert!(
        control_error.contains("StartToClose"),
        "the control must fail specifically on the start-to-close deadline, \
         got: {control_error}"
    );

    // Assert both halves of "discarded", not merely that it died. First: it
    // genuinely did work, otherwise there was nothing to lose and the contrast
    // is empty.
    assert!(
        !control.is_empty(),
        "the control must actually do work before being killed, or the \
         'work was discarded' contrast is vacuous"
    );

    // Second: none of that work became durable. Note the in-process ledger is
    // NOT the measure here — the scanner reclaims the task ROW while the
    // in-flight future keeps running in memory, so the control's ledger can
    // well show every item even though the run failed. What was discarded is
    // the *durable* record: no `ActivityCompleted` was ever appended, so from
    // the engine's point of view the activity produced nothing at all.
    let control_history = {
        let mut conn = connect(url).await;
        store::load_history(&mut conn, exec2)
            .await
            .expect("load control history")
            .events
    };
    assert!(
        !control_history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "the control's work must not be durable — it processed {} items in \
         memory and recorded none of them: {control_history:?}",
        control.len()
    );
}
