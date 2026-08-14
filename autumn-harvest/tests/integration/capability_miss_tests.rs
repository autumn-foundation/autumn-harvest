#![cfg(feature = "db")]
//! Capability-miss release-for-a-capable-peer integration tests — issue #804.
//!
//! A worker that claims a task whose handler it does not register used to
//! **fail the execution terminally**. During a rolling deploy that is a
//! self-inflicted outage: the old pods are perfectly healthy, they simply have
//! not been given the new handler yet, and `SKIP LOCKED` hands them the new
//! build's work anyway (the claim query has no capability predicate, and — by
//! construction — cannot have one: you cannot enumerate the handlers you have
//! *not* registered).
//!
//! These tests drive the **real worker poll loop** against a real Postgres and
//! prove the release-then-escalate contract end to end:
//!
//! - **AC1** — a workflow task whose `workflow_name` is unregistered is released
//!   back to `PENDING` (no `WorkflowFailed` appended, no `FAILED` transition),
//!   and a capable peer then drives the very same execution to `COMPLETED`.
//! - **AC2** — the same for an activity task whose `activity_name` is
//!   unregistered, using a realistic split-queue fleet.
//! - **AC3** — with **zero** capable workers the release is bounded: after the
//!   configured redelivery budget the task escalates through the ordinary
//!   terminal-failure path with the greppable `no_capable_worker:` reason.
//! - **AC4** — a capability miss is distinguishable from a poison-pill crash
//!   (#367) and a hung body (#494): `crash_strikes` is never incremented, the
//!   retry budget (`attempt`) is never drained, and no `PoisonPill` dead-letter
//!   row is written.
//! - **AC5** — `harvest.task.capability_miss` is emitted on every release and
//!   once with the distinct `escalated` outcome at the bound.
//! - **AC7** — no new `WorkflowEvent` variant: a release appends **nothing** to
//!   `harvest_events`, so replay determinism is untouched.
//!
//! Plus the session hard-pin carve-out: a session-pinned task (#606) can only
//! ever return to the *same* host, so "release for a capable peer" is false by
//! construction and it escalates immediately regardless of the budget.
//!
//! **Determinism.** Every test is *phased*: the incapable worker runs alone
//! until the release is observed, is shut down, and only then is the capable
//! worker started. Running both concurrently would make it a coin flip whether
//! the incapable worker ever sees the task at all.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted
//! with the full migration bundle.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_task_queue, harvest_workflow_executions,
};
use autumn_harvest::telemetry::{
    CAPABILITY_MISS_OUTCOME_ESCALATED, CAPABILITY_MISS_OUTCOME_RELEASED, MetricsRecorder,
    TelemetryConfig,
};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{
    DbPool, HandlerRegistry, NO_CAPABLE_WORKER_PREFIX, SESSION_PINNED_ESCALATION_MARKER, Worker,
    WorkerRuntimeConfig,
};
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
// Capturing metrics recorder for `harvest.task.capability_miss` (AC5).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CapabilityMetrics {
    /// `(queue, task_type, outcome)` per emission, in order.
    misses: Mutex<Vec<(String, String, String)>>,
    /// Poison-pill quarantine emissions — must stay empty (AC4).
    quarantined: Mutex<Vec<(String, String)>>,
}

impl CapabilityMetrics {
    fn samples(&self) -> Vec<(String, String, String)> {
        self.misses.lock().unwrap().clone()
    }

    fn count_with_outcome(&self, outcome: &str) -> usize {
        self.misses
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, o)| o == outcome)
            .count()
    }

    fn quarantine_count(&self) -> usize {
        self.quarantined.lock().unwrap().len()
    }
}

impl MetricsRecorder for CapabilityMetrics {
    fn record_task_capability_miss(&self, queue: &str, task_type: &str, outcome: &str) {
        self.misses.lock().unwrap().push((
            queue.to_owned(),
            task_type.to_owned(),
            outcome.to_owned(),
        ));
    }

    fn record_task_quarantined(&self, queue: &str, reason: &str) {
        self.quarantined
            .lock()
            .unwrap()
            .push((queue.to_owned(), reason.to_owned()));
    }
}

// ---------------------------------------------------------------------------
// Worker + registry construction.
// ---------------------------------------------------------------------------

fn build_registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    metrics: Arc<CapabilityMetrics>,
) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

/// Registry for the AC2 split-queue fleet: the workflow plus the activity it
/// dispatches. Shared by the workflow-queue worker (which enqueues the activity
/// but never polls its queue) and the capable `acts` worker, so the two can
/// never drift into disagreeing about what the "capable" handler set is.
fn activity_peer_registry(metrics: Arc<CapabilityMetrics>) -> Arc<HandlerRegistry> {
    build_registry(
        vec![workflow_info(
            "activity_peer_wf",
            workflow_calls_peer_activity,
        )],
        vec![activity_info(
            "peer_only_activity",
            peer_only_activity,
            Q_ACT_ACTS,
        )],
        metrics,
    )
}

fn build_worker(
    worker_id: &str,
    queues: &[&str],
    registry: Arc<HandlerRegistry>,
    capability_miss_max_redeliveries: u32,
) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: worker_id.to_string(),
                queues: queues.iter().map(|q| (*q).to_string()).collect(),
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries,
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
            },
            registry,
        )
        .expect("worker should build"),
    )
}

/// Run `worker` against `pool` until `until` resolves, then shut it down.
///
/// Every test phases its workers this way so the "incapable worker sees the
/// task first" precondition is deterministic rather than a race.
async fn with_worker_running<F, T>(worker: &Arc<Worker>, pool: &DbPool, until: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let runner = Arc::clone(worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    let out = until.await;
    worker.shutdown();
    handle.await.expect("worker task joins cleanly");
    out
}

// ---------------------------------------------------------------------------
// Seeding + read helpers.
// ---------------------------------------------------------------------------

async fn seed_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
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
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    exec_id
}

/// Seed an execution and enqueue its initial workflow task on `queue`.
async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
    queue_name: &str,
) -> ExecutionId {
    let exec_id = seed_execution(conn, workflow_name, input.clone()).await;
    let mut params = EnqueueParams::new(queue_name, TaskType::Workflow, input);
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

async fn load_tasks(url: &str, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .order(harvest_task_queue::scheduled_at.asc())
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("reload task rows")
}

async fn load_task(url: &str, task_id: Uuid) -> TaskQueueItem {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("reload task row")
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

async fn dead_letter_errors(url: &str, exec_id: ExecutionId) -> Vec<String> {
    let mut conn = connect(url).await;
    harvest_dead_letters::table
        .filter(harvest_dead_letters::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .select(harvest_dead_letters::error)
        .load(&mut conn)
        .await
        .expect("load dead letters")
}

/// Poll until at least one task row for `exec_id` reports `capability_misses >= want`.
async fn wait_for_capability_misses(
    url: &str,
    exec_id: ExecutionId,
    want: i32,
    timeout: Duration,
) -> TaskQueueItem {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(task) = load_tasks(url, exec_id)
                .await
                .into_iter()
                .find(|t| t.capability_misses >= want)
            {
                break task;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no task reached capability_misses >= {want} within {timeout:?}"))
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

/// Wait for ANY terminal state and return the row.
///
/// The success-metric test must distinguish "still running" from "went FAILED":
/// waiting for `COMPLETED` specifically would report a spurious escalation as a
/// bare timeout, hiding the `no_capable_worker:` reason that explains it.
async fn wait_for_terminal(
    url: &str,
    exec_id: ExecutionId,
    timeout: Duration,
) -> WorkflowExecution {
    tokio::time::timeout(timeout, async {
        loop {
            let e = load_execution(url, exec_id).await;
            if autumn_harvest::erase::is_terminal_state(&e.state) {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution {exec_id} reached no terminal state within {timeout:?}"))
}

// ---------------------------------------------------------------------------
// Shared assertions.
//
// The release contract is identical for workflow and activity tasks, so both
// AC1 and AC2 assert it through one helper — a divergence between them would
// then be a compile-visible edit, not a silently-drifting copy.
// ---------------------------------------------------------------------------

/// Assert the invariants every capability-miss release must satisfy, whatever
/// the task type: the claim is dropped for a peer, none of the *crash*
/// accounting is touched (AC4), and the row is not marked as having failed.
fn assert_released_for_a_peer(task: &TaskQueueItem) {
    assert_eq!(
        task.state, "PENDING",
        "a capability miss must release the claim, not hold or fail it"
    );
    assert!(
        task.worker_id.is_none(),
        "the releasing worker must drop ownership so a peer can claim"
    );
    assert!(
        task.started_at.is_none(),
        "started_at must be cleared so the row is not mistaken for in-flight work"
    );
    assert!(
        task.sticky_worker_id.is_none(),
        "sticky affinity must be cleared or the incapable worker keeps first refusal"
    );

    // AC4 — distinguishable from a poison-pill crash (#367) and a hung body (#494).
    assert_eq!(
        task.crash_strikes, 0,
        "a clean capability miss must never increment crash strikes (AC4)"
    );
    assert_eq!(
        task.attempt, 0,
        "the release must restore the attempt `claim_task` consumed, or the retry \
         budget silently drains (AC4)"
    );

    // The `error` column is reserved for genuine task failures and must stay
    // untouched. `/stack` (issue #773) renders any non-null `error` on a pending
    // activity as a `last_failure` — and because the release also restores
    // `attempt` to 0 (asserted above), a breadcrumb here would surface as a
    // failure at an otherwise-unreachable attempt 0 for work that never ran,
    // violating #773 AC3. The operator breadcrumb reaches them through the
    // release's `tracing::info!` and the `harvest.task.capability_miss` counter
    // instead; `release_never_writes_the_error_column` covers both directions.
    assert!(
        task.error.is_none(),
        "a capability miss is not a task failure and must not write `error`; got {:?}",
        task.error
    );
}

/// Assert the capability-miss counter recorded a release for this task shape,
/// and that nothing was escalated or quarantined (AC4 + AC5).
fn assert_released_sample(metrics: &CapabilityMetrics, queue: &str, task_type: &str) {
    let samples = metrics.samples();
    assert!(
        samples
            .iter()
            .any(|(q, t, o)| q == queue && t == task_type && o == CAPABILITY_MISS_OUTCOME_RELEASED),
        "expected a released capability-miss sample for {queue}/{task_type}, got {samples:?}"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "must not escalate while the budget is nowhere near exhausted, got {samples:?}"
    );
    assert_eq!(
        metrics.quarantine_count(),
        0,
        "a capability miss is not a poison pill (AC4)"
    );
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Per-test queue names.
//
// The whole module shares one database (and, under `--test-threads=1`, runs
// back to back), so every test gets its own queue. Without this a worker left
// polling `default` could claim a *sibling* test's seeded task and skew its
// capability-miss counts.
// ---------------------------------------------------------------------------

const Q_WF_MISS: &str = "q804-wf-miss";
const Q_ACT_WF: &str = "q804-act-wf";
const Q_ACT_ACTS: &str = "q804-act-acts";
const Q_ESCALATE: &str = "q804-escalate";
const Q_SESSION: &str = "q804-session";
const Q_MIXED: &str = "q804-mixed";

/// Budget for the success-metric test. Deliberately far above the shipped
/// default: see the rationale at its use site.
const MIXED_FLEET_BUDGET: u32 = 50;
const Q_NOOP: &str = "q804-noop";
const Q_PARK: &str = "q804-park";

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Trivial workflow used as the "new build only" handler.
fn peer_only_workflow(_ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("done")) })
}

/// Decoy so an "incapable" worker still has a non-empty registry — a capability
/// miss must be about *this* handler being absent, not about the worker having
/// no handlers at all.
fn decoy_workflow(_ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("decoy")) })
}

/// Workflow that dispatches one activity onto the dedicated activity queue and
/// returns its output (AC2's realistic split-queue fleet).
fn workflow_calls_peer_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("peer_only_activity", input, Q_ACT_ACTS)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Workflow that parks on a signal that never arrives — the cleanest
/// `park_workflow_task` shape, because unlike an activity dispatch it needs no
/// second handler lookup (which would itself be a capability miss).
fn workflow_waits_for_signal(ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.wait_for_signal("never_arrives")
            .await
            .map_err(|e| e.to_string())
    })
}

fn peer_only_activity(
    _ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("activity done")) })
}

fn decoy_activity(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("decoy")) })
}

fn workflow_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "capability_miss_tests",
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

fn activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    default_queue: &'static str,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "capability_miss_tests",
        default_retry_policy: Some(RetryPolicy::fixed(3, Duration::from_millis(50))),
        default_start_to_close: Some(Duration::from_secs(5)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(default_queue),
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
        handler,
    }
}

// ---------------------------------------------------------------------------
// AC1 — a workflow-task capability miss is released for a capable peer.
// ---------------------------------------------------------------------------

/// The headline #804 claim: a task for an unregistered workflow handler is
/// **released**, not failed, and the very same execution then reaches a normal
/// terminal outcome once a capable peer claims it. Zero spurious `FAILED`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_capability_miss_is_released_for_a_capable_peer() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "peer_only_wf",
        serde_json::json!({"n": 1}),
        Q_WF_MISS,
    )
    .await;

    // --- Phase 1: only the INCAPABLE worker is live. ------------------------
    let old_build_metrics = Arc::new(CapabilityMetrics::default());
    let old_build = build_worker(
        "worker-old-build",
        &[Q_WF_MISS],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&old_build_metrics),
        ),
        // Generous budget so phase 1 provably cannot escalate while we watch.
        50,
    );

    let released = with_worker_running(&old_build, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(20)).await
    })
    .await;

    // AC1 — released back to PENDING for a peer, not failed.
    assert_released_for_a_peer(&released);
    assert_eq!(released.task_type, "workflow");
    assert_eq!(
        released.capability_misses, 1,
        "exactly one capability miss should have been recorded"
    );
    assert_eq!(
        released.max_attempts, 3,
        "the retry budget itself is untouched"
    );

    // The execution itself is untouched: still RUNNING, no terminal event.
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a capability miss must not transition the execution (AC1)"
    );

    // AC7 — no new event variant, and in fact no event at all.
    let history = load_history(&url, exec_id).await;
    assert_eq!(
        history.len(),
        1,
        "a release must append nothing to harvest_events; got {history:?}"
    );
    assert!(
        matches!(history[0], WorkflowEvent::WorkflowStarted { .. }),
        "only the seeded WorkflowStarted should be present, got {:?}",
        history[0]
    );

    // AC5 — the release is observable.
    assert_released_sample(&old_build_metrics, Q_WF_MISS, "workflow");

    // --- Phase 2: the CAPABLE peer picks up the very same task. -------------
    let new_build_metrics = Arc::new(CapabilityMetrics::default());
    let new_build = build_worker(
        "worker-new-build",
        &[Q_WF_MISS],
        build_registry(
            vec![workflow_info("peer_only_wf", peer_only_workflow)],
            vec![],
            Arc::clone(&new_build_metrics),
        ),
        50,
    );

    let completed = with_worker_running(&new_build, &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(completed.state, "COMPLETED");
    assert_eq!(
        completed.output,
        Some(serde_json::json!("done")),
        "the capable peer ran the real handler"
    );
    assert_eq!(
        new_build_metrics.samples().len(),
        0,
        "the capable worker had nothing to release"
    );
    assert!(
        dead_letter_errors(&url, exec_id).await.is_empty(),
        "no dead-letter row for a run that completed normally"
    );
}

// ---------------------------------------------------------------------------
// AC2 — an activity-task capability miss is released for a capable peer.
// ---------------------------------------------------------------------------

/// Realistic split-queue fleet: the workflow runs on `default` and dispatches
/// its activity to `acts`. An old-build worker polling `acts` claims the
/// activity task it cannot run and releases it; a capable `acts` worker then
/// completes it and the workflow finishes normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activity_capability_miss_is_released_for_a_capable_peer() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "activity_peer_wf",
        serde_json::json!({"n": 2}),
        Q_ACT_WF,
    )
    .await;

    // --- Phase 1: a workflow-only worker enqueues the activity onto `acts`,
    //     and an incapable `acts` worker claims and releases it. -------------
    //
    // The workflow worker must itself register the activity (the enqueue path
    // resolves the activity's declared defaults from the registry) but never
    // polls `acts`, so it can never run it.
    let wf_worker = build_worker(
        "worker-workflow-queue",
        &[Q_ACT_WF],
        activity_peer_registry(Arc::new(CapabilityMetrics::default())),
        50,
    );

    let old_acts_metrics = Arc::new(CapabilityMetrics::default());
    let old_acts_worker = build_worker(
        "worker-acts-old-build",
        &[Q_ACT_ACTS],
        build_registry(
            vec![],
            vec![activity_info(
                "some_other_activity",
                decoy_activity,
                Q_ACT_ACTS,
            )],
            Arc::clone(&old_acts_metrics),
        ),
        50,
    );

    let released = with_worker_running(&wf_worker, &pool, async {
        with_worker_running(
            &old_acts_worker,
            &pool,
            Box::pin(async {
                wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
            }),
        )
        .await
    })
    .await;

    // AC2 — the ACTIVITY task is the one released.
    assert_eq!(released.task_type, "activity");
    assert_eq!(released.queue_name, Q_ACT_ACTS);
    assert_released_for_a_peer(&released);
    assert_eq!(
        released.activity_name.as_deref(),
        Some("peer_only_activity"),
        "the release must preserve activity_name on an activity row (unlike a \
         workflow row, where it can carry a stale suspension sentinel)"
    );
    assert_released_sample(&old_acts_metrics, Q_ACT_ACTS, "activity");

    // The execution is still RUNNING and carries no terminal event.
    assert_eq!(load_execution(&url, exec_id).await.state, "RUNNING");
    let history = load_history(&url, exec_id).await;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a released activity must not fail the owning workflow, got {history:?}"
    );

    // --- Phase 2: a capable `acts` worker finishes the job. -----------------
    let full_fleet = build_worker(
        "worker-full-fleet",
        &[Q_ACT_WF, Q_ACT_ACTS],
        activity_peer_registry(Arc::new(CapabilityMetrics::default())),
        50,
    );

    let completed = with_worker_running(&full_fleet, &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("activity done")),
        "the workflow returned the capable peer's activity output"
    );
    assert!(dead_letter_errors(&url, exec_id).await.is_empty());
}

// ---------------------------------------------------------------------------
// AC3 + AC5 — bounded release: escalate when no capable worker ever appears.
// ---------------------------------------------------------------------------

/// With **zero** capable workers the release must not loop forever. After the
/// configured budget the task escalates through the ordinary terminal-failure
/// path, tagged with the greppable `no_capable_worker:` prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_miss_escalates_after_the_budget_with_no_capable_worker() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "never_registered_wf",
        serde_json::json!({}),
        Q_ESCALATE,
    )
    .await;

    // Budget 3 → releases at misses 1 and 2 (backoff 1s then 2s), escalation on
    // the third claim. Bounded well inside the test timeout.
    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "worker-no-capable-peer",
        &[Q_ESCALATE],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        3,
    );

    let failed = with_worker_running(&worker, &pool, async {
        wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(60)).await
    })
    .await;

    // AC3 — the terminal reason is unambiguous and greppable.
    let error = failed.error.unwrap_or_default();
    assert!(
        error.starts_with(NO_CAPABLE_WORKER_PREFIX),
        "escalation must carry the stable `no_capable_worker:` prefix, got {error:?}"
    );
    assert!(
        error.contains("never_registered_wf"),
        "the reason must name the missing handler, got {error:?}"
    );
    assert!(
        error.contains("after 3 capability-miss redeliveries"),
        "the reason must state the exact budget that was exhausted \
         (a single-char check cannot catch an off-by-one), got {error:?}"
    );

    // AC3 — it routes through the EXISTING terminal path, not a new one.
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "escalation appends the ordinary WorkflowFailed event, got {history:?}"
    );

    // AC5 — released exactly N times, escalated exactly once, with the distinct
    // outcome. The knob is named `capability_miss_max_redeliveries`, so a budget
    // of N grants N releases and escalates on the N+1th claim.
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        1,
        "exactly one escalation, got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        3,
        "budget 3 must grant exactly three redeliveries before escalating, got {:?}",
        metrics.samples()
    );

    // AC4 — never mistaken for a poison pill, even at escalation.
    assert_eq!(metrics.quarantine_count(), 0);
    // Escalation routes through the ordinary terminal-failure path
    // (`fail_task_and_execution` -> `persist_workflow_failure`), which writes
    // NO dead-letter row. So the whole DLQ must stay empty -- in particular
    // there is no `PoisonPill` quarantine (#367).
    let dlq = dead_letter_errors(&url, exec_id).await;
    assert!(
        dlq.is_empty(),
        "escalation must not dead-letter; the reason lives on the execution row, got {dlq:?}"
    );
    let tasks = load_tasks(&url, exec_id).await;
    assert!(
        tasks.iter().all(|t| t.crash_strikes == 0),
        "crash strikes must stay at zero across the whole release cycle (AC4)"
    );
}

// ---------------------------------------------------------------------------
// Session hard-pin carve-out (#606) — release is impossible, so escalate now.
// ---------------------------------------------------------------------------

/// A session-pinned activity row can only ever be claimed by its acquiring
/// host, so releasing it "for a capable peer" would strand it forever. Such a
/// task escalates on the FIRST miss regardless of the budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_pinned_capability_miss_escalates_immediately() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_execution(&mut conn, "session_host_wf", serde_json::json!({})).await;

    // Hard-pin an activity task to the (incapable) worker, exactly as the
    // session dispatch path does.
    let worker_id = "worker-session-host";
    let mut params = EnqueueParams::new(Q_SESSION, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("session_only_activity".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.session_id = Some(Uuid::new_v4());
    params.sticky_worker_id = Some(worker_id.to_string());
    params.sticky_timeout = Some(Duration::from_secs(300));
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue session-pinned activity task");

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        worker_id,
        &[Q_SESSION],
        build_registry(
            vec![],
            vec![activity_info(
                "some_other_activity",
                decoy_activity,
                Q_SESSION,
            )],
            Arc::clone(&metrics),
        ),
        // A generous budget that must be IGNORED: the pin makes release unsound.
        50,
    );

    let failed = with_worker_running(&worker, &pool, async {
        wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(30)).await
    })
    .await;

    let error = failed.error.unwrap_or_default();
    assert!(error.starts_with(NO_CAPABLE_WORKER_PREFIX), "got {error:?}");

    // The reason must describe THIS branch, not the budget-exhausted one. The
    // budget above is 50 and zero releases happened, so the shared wording would
    // have claimed "after 50 capability-miss redeliveries" — a release count
    // that never occurred — and asserted a fleet-wide conclusion that may be
    // false, since a capable peer can exist and simply be ineligible for a
    // pinned row. An operator following the runbook would then check
    // `reachability`, get `in_use`, and be stranded.
    assert!(
        !error.contains("after 50"),
        "the pinned task was released ZERO times; the reason must not name the \
         configured budget: {error}"
    );
    assert!(
        !error.contains("no live worker on this queue has the handler"),
        "a capable peer may exist and merely be ineligible for a pinned row: {error}"
    );
    assert!(
        error.contains(SESSION_PINNED_ESCALATION_MARKER),
        "the pin is the actual cause and must be named: {error}"
    );

    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        1,
        "the pinned task escalates on the first miss"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        0,
        "a session-pinned task must never be released: no peer can claim it"
    );
}

// ---------------------------------------------------------------------------
// Success metric — zero spurious FAILED while a capable worker is live.
// ---------------------------------------------------------------------------

/// Issue #804's success metric, measured: with a mixed fleet (one incapable
/// worker, one capable), **100%** of executions reach a normal terminal outcome
/// and **zero** are spuriously failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_fleet_completes_every_execution_with_zero_spurious_failures() {
    const RUNS: usize = 8;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let mut exec_ids = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        exec_ids.push(
            seed_workflow(
                &mut conn,
                "peer_only_wf",
                serde_json::json!({"n": 1}),
                Q_MIXED,
            )
            .await,
        );
    }

    let old_metrics = Arc::new(CapabilityMetrics::default());
    let old_build = build_worker(
        "mixed-old-build",
        &[Q_MIXED],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&old_metrics),
        ),
        // Generous budget, matching the AC1/AC2 convention in this file. Both
        // workers poll concurrently here (the realistic mid-deploy fleet), so
        // the claim race is genuinely stochastic: at the shipped default of 5
        // the incapable worker can win six consecutive races for one task and
        // escalate it, making this test flaky at a few percent per run. This
        // test measures "does EVERY execution reach a normal terminal outcome
        // while a capable worker is live" — the budget bound is measured by the
        // AC3 test, deterministically, with no capable worker at all.
        MIXED_FLEET_BUDGET,
    );
    let new_metrics = Arc::new(CapabilityMetrics::default());
    let new_build = build_worker(
        "mixed-new-build",
        &[Q_MIXED],
        build_registry(
            vec![workflow_info("peer_only_wf", peer_only_workflow)],
            vec![],
            Arc::clone(&new_metrics),
        ),
        MIXED_FLEET_BUDGET,
    );

    // Both live simultaneously — the realistic mid-deploy fleet. The inner
    // future is boxed: nesting two `with_worker_running` scopes around an
    // 8-execution wait otherwise builds a single very large stack future.
    with_worker_running(&old_build, &pool, async {
        with_worker_running(
            &new_build,
            &pool,
            Box::pin(async {
                for exec_id in &exec_ids {
                    wait_for_terminal(&url, *exec_id, Duration::from_secs(60)).await;
                }
            }),
        )
        .await;
    })
    .await;

    for exec_id in &exec_ids {
        let e = load_execution(&url, *exec_id).await;
        assert_eq!(
            e.state, "COMPLETED",
            "every execution must reach a normal terminal outcome while a capable \
             worker is live; {exec_id} ended {} ({:?})",
            e.state, e.error
        );
    }
    assert_eq!(
        old_metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED)
            + new_metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "no execution may escalate while a capable worker is live"
    );
}

/// A capable worker that PARKS the task must reset the capability-miss budget.
///
/// This is the counter's whole contract: it measures *consecutive* misses. A
/// workflow task row is long-lived — parked and re-pended in place for the
/// execution's entire life — and parking is the dominant suspension path
/// (activity, signal, child workflow, mutex). If parking did not reset, the
/// counter would be cumulative and a long-lived execution would accumulate one
/// miss per deploy until it terminally failed with `no_capable_worker:` while a
/// capable worker was demonstrably live — the exact outage #804 exists to
/// prevent, inverted.
///
/// Reaching a park PROVES capability: `process_workflow_task` resolves the
/// workflow handler and runs a decision cycle before any park can happen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capable_worker_that_parks_resets_the_capability_miss_budget() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    // A workflow that suspends on a signal wait — a `park_workflow_task` shape.
    let exec_id = seed_workflow(
        &mut conn,
        "signal_park_wf",
        serde_json::json!({"n": 1}),
        Q_PARK,
    )
    .await;

    // Misses already absorbed during an earlier deploy window.
    let task_id = load_tasks(&url, exec_id).await[0].id;
    diesel::sql_query("UPDATE harvest_task_queue SET capability_misses = 3 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("seed prior misses");
    assert_eq!(load_task(&url, task_id).await.capability_misses, 3);

    // A worker that HAS the workflow handler claims it, runs a decision cycle,
    // and parks waiting for a signal that never arrives.
    let metrics = Arc::new(CapabilityMetrics::default());
    let capable = build_worker(
        "park-capable",
        &[Q_PARK],
        build_registry(
            vec![workflow_info("signal_park_wf", workflow_waits_for_signal)],
            vec![],
            Arc::clone(&metrics),
        ),
        5,
    );

    with_worker_running(&capable, &pool, async {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if load_task(&url, task_id).await.capability_misses == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("parking must reset capability_misses to 0 within the window");
    })
    .await;

    let task = load_task(&url, task_id).await;
    assert_eq!(
        task.capability_misses, 0,
        "a park proves a capable worker handled this row, so the consecutive-miss \
         budget must start clean"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "the capable worker must not escalate anything"
    );
    // The run is genuinely parked mid-flight, not terminal.
    assert_eq!(load_execution(&url, exec_id).await.state, "RUNNING");
}

/// The **workflow-task-timeout** re-pend (#494) must NOT touch the budget.
///
/// It is tempting to reset here — a timeout usually means the handler was found
/// and ran long, which would be proof of capability. That premise does not hold
/// on this path. The #494 budget is armed around the whole of `process_task`,
/// and `pool.get()` plus the full history load both sit inside it, strictly
/// *before* the registry lookup that defines a capability miss. Under pool
/// starvation or a slow shard the timeout fires with the lookup never having
/// run, so reaching here proves nothing about the claiming worker.
///
/// Resetting on that false premise is the worse of the two errors: a genuinely
/// unregistered type under load would have its consecutive-miss streak zeroed
/// indefinitely and never escalate, so the run never reaches the
/// `no_capable_worker:` terminal AC3 promises and the operator sees only the
/// ticket-severity sustained-release rule instead of the page. It also erases
/// an in-flight miss when the release UPDATE itself is what timed out.
///
/// The accepted cost runs the other way: a capable-but-slow worker's timeout
/// leaves a stale streak, so a later genuine miss can escalate before spending
/// the full budget. That direction is fail-safe — a loud, actionable terminal
/// failure rather than a silently un-escalating task.
///
/// Driven against the real `pub` entry point, so the assertion cannot drift
/// from the statement the way a SQL-shape test could.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_workflow_task_reset_preserves_the_capability_miss_budget() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "timeout_reset_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;

    // The exact state the #494 timeout handler acts on: claimed by *this*
    // worker, with misses already absorbed during an earlier deploy window.
    diesel::sql_query(
        "UPDATE harvest_task_queue \
            SET state = 'RUNNING', worker_id = 'slow-but-capable', started_at = NOW(), \
                capability_misses = 4 \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("seed a claimed, previously-missed task");
    assert_eq!(load_task(&url, task_id).await.capability_misses, 4);

    autumn_harvest::worker::reset_timed_out_workflow_task(&pool, task_id, "slow-but-capable").await;

    let task = load_task(&url, task_id).await;
    assert_eq!(
        task.state, "PENDING",
        "the timed-out task must be re-pended for any worker to re-claim"
    );
    assert_eq!(
        task.capability_misses, 4,
        "a timeout can fire before the handler lookup ever runs (pool starvation, \
         slow shard), so it is NOT proof of capability; zeroing here would let an \
         unregistered type reset its streak indefinitely and never escalate"
    );
    assert!(
        task.worker_id.is_none(),
        "ownership must be released so any worker can re-claim: {:?}",
        task.worker_id
    );
    assert_eq!(
        load_execution(&url, exec_id).await.state,
        "RUNNING",
        "a timeout re-pend must not terminate the execution"
    );
}

/// A release must leave the task row's `error` column **untouched** so the
/// issue #773 `/stack` surface does not fabricate a failure that never happened.
///
/// `/stack` renders any non-null `error` on a pending activity as a
/// `last_failure`, with `error_type: "Error"` and the row's raw `attempt`.
/// Because a release also restores `attempt` to `0`, writing the diagnostic
/// there would report a failure at an otherwise-unreachable `attempt: 0` for an
/// activity that never executed — violating #773 AC3 ("a never-failed pending
/// activity omits `last_failure`") and making a blameless deploy skew look like
/// an application bug on the one surface whose runbook question is "why is this
/// activity retrying?".
///
/// A real prior failure must survive the release untouched too: the next
/// attempt reads it through `ActivityContext::previous_failure()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_never_writes_the_error_column() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    // (a) A fresh, never-failed row must stay never-failed.
    let exec_id = seed_workflow(&mut conn, "noop_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'incapable', \
         started_at = NOW(), attempt = 1 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("simulate a claim");

    let released = queue::release_task_for_capability_miss(
        &mut conn,
        task_id,
        "incapable",
        Duration::from_secs(1),
    )
    .await
    .expect("release query runs");
    assert!(released);

    let task = load_task(&url, task_id).await;
    assert!(
        task.error.is_none(),
        "a capability miss is not a task failure; /stack would render this as a \
         last_failure at attempt 0 for work that never ran (#773 AC3): {:?}",
        task.error
    );
    assert_eq!(
        task.attempt, 0,
        "the claim's increment is undone, which is exactly why an `error` here \
         would surface at an impossible attempt 0"
    );

    // (b) A genuine prior failure must survive untouched.
    let exec2 = seed_workflow(&mut conn, "noop_wf", serde_json::json!({ "n": 2 }), Q_NOOP).await;
    let task2 = load_tasks(&url, exec2).await[0].id;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'incapable', \
         started_at = NOW(), attempt = 2, error = 'downstream 503' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task2)
    .execute(&mut conn)
    .await
    .expect("simulate a claim over a real prior failure");

    let released2 = queue::release_task_for_capability_miss(
        &mut conn,
        task2,
        "incapable",
        Duration::from_secs(1),
    )
    .await
    .expect("release query runs");
    assert!(released2);
    assert_eq!(
        load_task(&url, task2).await.error.as_deref(),
        Some("downstream 503"),
        "the real failure the next attempt branches on via previous_failure() \
         must not be clobbered by an infrastructure string"
    );
}

// ---------------------------------------------------------------------------
// Release is idempotent against a stolen claim.
// ---------------------------------------------------------------------------

/// The release UPDATE is ownership-guarded (`state = 'RUNNING' AND worker_id =
/// $2`). If a concurrent poison-pill reclaim or operator action already took
/// the row, the release matches nothing, counts nothing, and is a clean no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_is_a_noop_when_the_claim_was_already_taken() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "noop_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;

    // Simulate a foreign owner: the row is RUNNING under someone else.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'someone-else', \
         started_at = NOW() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("simulate foreign claim");

    let released = queue::release_task_for_capability_miss(
        &mut conn,
        task_id,
        "worker-we-are",
        Duration::from_secs(1),
    )
    .await
    .expect("release query runs");

    assert!(
        !released,
        "release must report no-op when the row is owned by another worker"
    );
    let task = load_task(&url, task_id).await;
    assert_eq!(
        task.state, "RUNNING",
        "the foreign owner's claim must be untouched"
    );
    assert_eq!(
        task.worker_id.as_deref(),
        Some("someone-else"),
        "ownership must not be stolen back"
    );
    assert_eq!(
        task.capability_misses, 0,
        "a no-op release must not inflate the miss counter"
    );
}
