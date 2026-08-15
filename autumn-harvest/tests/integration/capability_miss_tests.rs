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

use autumn_harvest::error::CapabilityMissPhase;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_rate_limit_buckets, harvest_task_queue,
    harvest_workflow_executions,
};
use autumn_harvest::telemetry::{
    CAPABILITY_MISS_OUTCOME_ESCALATED, CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED,
    CAPABILITY_MISS_OUTCOME_RELEASED, MetricsRecorder, TelemetryConfig,
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
    /// `harvest.workflow.started` emissions as `(workflow, queue)`. A release
    /// must never let this fire twice for one execution (Codex round-17 P2).
    started: Mutex<Vec<(String, String)>>,
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

    fn started_samples(&self) -> Vec<(String, String)> {
        self.started.lock().unwrap().clone()
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

    fn record_workflow_started(&self, workflow: &str, queue: &str) {
        self.started
            .lock()
            .unwrap()
            .push((workflow.to_owned(), queue.to_owned()));
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

/// Seed a task row's distinct-miss set as if `workers` had each already claimed
/// and released it (issue #804 round 6).
///
/// The redelivery budget is consumed per DISTINCT worker, so an all-incapable
/// *fleet* is what exhausts it — a single worker bouncing never can. Seeding the
/// prior set models the fleet without standing up N worker runtimes, and drives
/// the real decision path: `resolve_capability_miss` reads this column back off
/// the claimed row.
async fn seed_prior_missing_workers(url: &str, exec_id: ExecutionId, workers: &[&str]) {
    let mut conn = connect(url).await;
    let owned: Vec<String> = workers.iter().map(|w| (*w).to_string()).collect();
    let misses = i32::try_from(owned.len()).expect("small");
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid()))),
    )
    .set((
        harvest_task_queue::capability_miss_workers.eq(owned),
        harvest_task_queue::capability_misses.eq(misses),
    ))
    .execute(&mut conn)
    .await
    .expect("seed prior missing workers");
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

/// Wait until `worker_id` has recorded a miss on the task, or the execution
/// went terminal trying.
///
/// The frontier-scoping test cannot wait on `capability_misses` reaching a
/// count: the whole point of the fix is that the counter RESETS on durable
/// inline progress, so the target value is identical before and after it. It
/// also cannot wait on the release alone — under the bug the miss escalates and
/// the row never returns to `PENDING`, which would surface a crisp accounting
/// regression as a bare timeout. Waiting on "this worker's miss is resolved,
/// either way" terminates promptly under both outcomes and lets the assertions
/// name the difference.
async fn wait_for_miss_by(
    url: &str,
    exec_id: ExecutionId,
    worker_id: &str,
    timeout: Duration,
) -> Option<TaskQueueItem> {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(task) = load_tasks(url, exec_id)
                .await
                .into_iter()
                .find(|t| t.capability_miss_workers.iter().any(|w| w == worker_id))
            {
                break Some(task);
            }
            if autumn_harvest::erase::is_terminal_state(&load_execution(url, exec_id).await.state) {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("worker {worker_id} neither recorded a miss nor sealed the run within {timeout:?}")
    })
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
/// the task type *or phase*: the claim is dropped for a peer, none of the
/// *crash* accounting is touched (AC4), and the row is not marked as having
/// failed.
///
/// `attempt` is deliberately **not** asserted here, because it is the one
/// clause that legitimately differs by phase (Codex round-17 P2): a
/// pre-handler release restores it, a post-handler release must not. Use
/// [`assert_released_before_the_handler`] for the pre-handler case, which is
/// where restoring it is the AC4 retry-budget guarantee.
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
    // The `error` column is reserved for genuine task failures and must stay
    // untouched. `/stack` (issue #773) renders any non-null `error` on a pending
    // activity as a `last_failure` — and because a pre-handler release also
    // restores `attempt` to 0, a breadcrumb here would surface as a failure at
    // an otherwise-unreachable attempt 0 for work that never ran, violating
    // #773 AC3. The operator breadcrumb reaches them through the release's
    // `tracing::info!` and the `harvest.task.capability_miss` counter instead;
    // `release_never_writes_the_error_column` covers both directions.
    assert!(
        task.error.is_none(),
        "a capability miss is not a task failure and must not write `error`; got {:?}",
        task.error
    );
}

/// [`assert_released_for_a_peer`] plus the clause that is specific to a
/// **pre-handler** miss: the claim-time `attempt` increment is undone.
///
/// This is the AC4 retry-budget guarantee, and it belongs to exactly the phase
/// where nothing ran — which is every activity-task miss, and a workflow task
/// whose *type* was unregistered. A post-handler release deliberately leaves
/// `attempt` alone; see `CapabilityMissPhase::restores_dispatch_attempt`.
fn assert_released_before_the_handler(task: &TaskQueueItem) {
    assert_released_for_a_peer(task);
    assert_eq!(
        task.attempt, 0,
        "a pre-handler release must restore the attempt `claim_task` consumed, \
         or the retry budget silently drains (AC4)"
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
const Q_LIVE_PEER: &str = "q804-live-peer";
const Q_STARTED_ONCE: &str = "q804-started-once";
const Q_STARTED_ONCE_ACTS: &str = "q804-started-once-acts";
const Q_LOCAL_MISS: &str = "q804-local-miss";
/// The breadcrumb whose presence proves the inline arm's commit block ran.
const LOCAL_MISS_BREADCRUMB: &str = "about to run the local activity";

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

/// Sibling of [`workflow_calls_peer_activity`] on its own dedicated activity
/// queue, so the round-17 start-metric test cannot claim (or be blocked by) an
/// adjacent test's activity row in the shared database.
fn workflow_calls_started_once_activity(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("started_once_activity", input, Q_STARTED_ONCE_ACTS)
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
    assert_released_before_the_handler(&released);
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
    assert_released_before_the_handler(&released);
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
// A post-handler release must not let `harvest.workflow.started` fire twice.
// ---------------------------------------------------------------------------

/// `record_workflow_started` fires exactly once per execution, gated on
/// `task.attempt == 1 && no scheduling events in history`. A **post-handler**
/// capability miss (the persist-time pre-pass finding an unregistered activity)
/// happens *after* that gate has already fired, and its persistence transaction
/// rolls back — so nothing lands in history either. If the release also rolled
/// `attempt` back to `0`, the next capable claim would see `attempt == 1` with
/// no scheduling events and emit the counter a **second** time, inflating
/// workflow-start counts and every SLO derived from them (Codex round-17 P2).
///
/// The fix is phase-gated: only a `BeforeHandler` release restores `attempt`
/// (nothing ran, and the metric was never emitted, so the next dispatch *must*
/// still look like the first). `During`/`AfterHandler` releases leave it alone.
/// That is safe because `task.attempt` is a retry budget only for **activity**
/// tasks — and every activity-task miss is `BeforeHandler` — while on a
/// workflow row it is read solely by this metric gate and the informational DLQ
/// `attempts` field. The #523 workflow-level retry loop counts
/// `executions.workflow_attempt`, a different column entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_post_handler_release_does_not_re_emit_workflow_started() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "started_once_wf",
        serde_json::json!({"n": 17}),
        Q_STARTED_ONCE,
    )
    .await;

    // ONE recorder across both phases: the assertion is about the total number
    // of `harvest.workflow.started` emissions for this execution, however many
    // workers observed it.
    let metrics = Arc::new(CapabilityMetrics::default());

    // --- Phase 1: a worker that CAN run the body but NOT the activity. ------
    //
    // The body runs to a conclusion (so the start metric fires), then the
    // persist-time capability pre-pass finds `started_once_activity`
    // unregistered and releases the workflow task — an `AfterHandler` miss.
    // It polls the activity queue too, so a later inability to *complete* the
    // run can only be the capability miss, never a routing gap.
    let half_capable = build_worker(
        "worker-body-only",
        &[Q_STARTED_ONCE, Q_STARTED_ONCE_ACTS],
        build_registry(
            vec![workflow_info(
                "started_once_wf",
                workflow_calls_started_once_activity,
            )],
            vec![activity_info(
                "some_other_activity",
                decoy_activity,
                Q_STARTED_ONCE_ACTS,
            )],
            Arc::clone(&metrics),
        ),
        50,
    );

    let released = with_worker_running(&half_capable, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        released.task_type, "workflow",
        "the persist-time pre-pass releases the WORKFLOW task, not an activity \
         row -- the activity was never enqueued"
    );
    assert_released_for_a_peer(&released);
    assert_eq!(
        metrics.started_samples().len(),
        1,
        "the body ran, so the start metric fired exactly once in phase 1: {:?}",
        metrics.started_samples()
    );
    assert_eq!(
        released.attempt, 1,
        "a post-handler release must LEAVE `attempt` at its claimed value: \
         rolling it back to 0 is precisely what makes the next capable claim \
         look like a first dispatch and re-emit the start metric"
    );
    // The rolled-back persist means history is still just the seeded start
    // event -- which is the other half of the metric gate's condition, and why
    // `attempt` alone has to carry the "already dispatched" signal here.
    assert_eq!(
        load_history(&url, exec_id).await.len(),
        1,
        "the persist transaction rolled back, so no scheduling event landed"
    );

    // --- Phase 2: the fully capable worker finishes the job. ----------------
    let capable = build_worker(
        "worker-full",
        &[Q_STARTED_ONCE, Q_STARTED_ONCE_ACTS],
        build_registry(
            vec![workflow_info(
                "started_once_wf",
                workflow_calls_started_once_activity,
            )],
            vec![activity_info(
                "started_once_activity",
                peer_only_activity,
                Q_STARTED_ONCE_ACTS,
            )],
            Arc::clone(&metrics),
        ),
        50,
    );

    let completed = with_worker_running(&capable, &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("activity done")),
        "the capable worker ran the real activity"
    );
    assert_eq!(
        metrics.started_samples(),
        vec![("started_once_wf".to_string(), Q_STARTED_ONCE.to_string())],
        "`harvest.workflow.started` must be emitted EXACTLY ONCE for this \
         execution across the whole incapable-then-capable dispatch sequence"
    );
    assert!(dead_letter_errors(&url, exec_id).await.is_empty());
}

// ---------------------------------------------------------------------------
// Codex round-19 P2 — a local-activity miss releases BEFORE its batch's
// sibling commands are committed.
// ---------------------------------------------------------------------------

/// A workflow that publishes an operator breadcrumb and then runs a **local**
/// activity, so both commands arrive in one suspension batch — the shape whose
/// commits used to run ahead of the handler lookup.
fn workflow_details_then_local_activity(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.set_current_details(LOCAL_MISS_BREADCRUMB);
        ctx.execute_local_activity_raw("local_only_activity", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// A local activity — `is_local: true` is what routes it through the inline arm
/// rather than the task queue.
fn local_activity_info(name: &'static str) -> ActivityInfo {
    ActivityInfo {
        is_local: true,
        default_queue: None,
        ..activity_info(name, peer_only_activity, Q_LOCAL_MISS)
    }
}

/// The inline local-activity arm commits search-attribute patches, the
/// `current_details` breadcrumb, durable logs (#790), progress frames (#791),
/// and any early mutex release (#691) *before* `run_local_activity_inline`
/// resolves the handler. Once a miss became releasable, an incapable worker
/// paid for all five and handed the task on, and the next worker redid them.
///
/// `current_details` is the cheapest durable, directly-queryable witness for the
/// whole block: it is `NULL` until that commit runs. Phase 1 asserts the
/// incapable worker left it `NULL`; phase 2 asserts the capable worker *does*
/// write it, so the phase-1 assertion cannot be passing merely because the
/// workflow never sets it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_activity_miss_releases_before_committing_its_batch() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "local_miss_wf",
        serde_json::json!({"n": 19}),
        Q_LOCAL_MISS,
    )
    .await;

    let metrics = Arc::new(CapabilityMetrics::default());

    // --- Phase 1: registers the workflow, NOT the local activity. -----------
    let incapable = build_worker(
        "worker-no-local",
        &[Q_LOCAL_MISS],
        build_registry(
            vec![workflow_info(
                "local_miss_wf",
                workflow_details_then_local_activity,
            )],
            vec![local_activity_info("some_other_local_activity")],
            Arc::clone(&metrics),
        ),
        50,
    );

    let released = with_worker_running(&incapable, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        released.task_type, "workflow",
        "a local activity runs inline on the WORKFLOW task, so that is the row \
         released -- no activity row is ever enqueued"
    );
    assert_released_for_a_peer(&released);
    assert_eq!(
        load_execution(&url, exec_id).await.current_details,
        None,
        "the incapable worker must release BEFORE the batch's sibling commands \
         are committed -- a written breadcrumb means it patched attributes, \
         appended logs, and fired progress frames on its way to discovering it \
         could not run the local activity at all"
    );
    // The body began and did not conclude, so `attempt` stays at its claimed
    // value: this is a `DuringHandler` miss, not a `BeforeHandler` one.
    assert_eq!(
        released.attempt, 1,
        "a mid-handler release must not roll `attempt` back -- the body ran"
    );

    // --- Phase 2: a worker that registers the local activity finishes. ------
    let capable = build_worker(
        "worker-with-local",
        &[Q_LOCAL_MISS],
        build_registry(
            vec![workflow_info(
                "local_miss_wf",
                workflow_details_then_local_activity,
            )],
            vec![local_activity_info("local_only_activity")],
            Arc::clone(&metrics),
        ),
        50,
    );

    let completed = with_worker_running(&capable, &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("activity done")),
        "the capable worker ran the local activity inline"
    );
    assert_eq!(
        completed.current_details.as_deref(),
        Some(LOCAL_MISS_BREADCRUMB),
        "the capable worker DOES commit the breadcrumb -- without this the \
         phase-1 assertion would hold for the wrong reason"
    );
    assert!(dead_letter_errors(&url, exec_id).await.is_empty());
}

// ---------------------------------------------------------------------------
// Frontier-scoped miss accounting (issue #804, Codex round-20 P1).
// ---------------------------------------------------------------------------

const Q_TWO_FRONTIER: &str = "q804-two-frontier";

fn second_frontier_activity(
    _ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!("second frontier done")) })
}

/// Two local-activity frontiers in one workflow. The inline drive loop resolves
/// them in the SAME dispatch without parking, so a worker can clear the first
/// and still miss the second.
fn workflow_two_local_frontiers(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("first_local", input.clone(), None, None)
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_local_activity_raw("second_local", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

fn local_activity_info_on(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    queue: &'static str,
) -> ActivityInfo {
    ActivityInfo {
        is_local: true,
        default_queue: None,
        ..activity_info(name, handler, queue)
    }
}

/// One member of the deliberately non-nested two-frontier fleet: registers the
/// shared workflow and exactly ONE of the two local activities.
///
/// Budget of 1 throughout, so the distinct-worker bound escalates on the SECOND
/// distinct misser — conflating the two frontiers therefore fails the run at the
/// earliest possible point rather than somewhere downstream.
fn two_frontier_worker(
    worker_id: &str,
    local: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    metrics: &Arc<CapabilityMetrics>,
) -> Arc<Worker> {
    build_worker(
        worker_id,
        &[Q_TWO_FRONTIER],
        build_registry(
            vec![workflow_info(
                "two_frontier_wf",
                workflow_two_local_frontiers,
            )],
            vec![local_activity_info_on(local, handler, Q_TWO_FRONTIER)],
            Arc::clone(metrics),
        ),
        1,
    )
}

/// `capability_misses` / `capability_miss_workers` describe ONE frontier — the
/// position the workflow is currently stuck at. Durable inline progress moves
/// that position, so every miss recorded before it was recorded against a
/// *different* handler and must not count against the new one.
///
/// The fleet here is deliberately non-nested, which is what makes the run
/// completable only by successive claims: worker A has `second_local` but not
/// `first_local`; worker B has `first_local` but not `second_local`. Neither can
/// finish the run alone, but B can clear frontier 1 and A can then clear
/// frontier 2 — so a correct engine completes it.
///
/// Without frontier scoping, B's miss on `second_local` is added to A's miss on
/// `first_local`, the registry reports every live worker as having missed "the
/// task", and the run is terminally failed at a budget of 1 while A — which
/// registers the handler the run is actually waiting on — is live and polling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_inline_progress_starts_the_next_frontier_with_a_clean_budget() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "two_frontier_wf",
        serde_json::json!({"n": 20}),
        Q_TWO_FRONTIER,
    )
    .await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let has_second = || {
        two_frontier_worker(
            "worker-has-second",
            "second_local",
            second_frontier_activity,
            &metrics,
        )
    };

    // --- Phase 1: A misses frontier 1 (`first_local`). ----------------------
    let released_a = with_worker_running(&has_second(), &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await
    })
    .await;
    assert_released_for_a_peer(&released_a);
    assert_eq!(
        released_a.capability_miss_workers,
        vec!["worker-has-second".to_string()],
        "phase 1 records A as the misser of frontier 1"
    );

    // --- Phase 2: B clears frontier 1 durably, then misses frontier 2. ------
    let has_first = two_frontier_worker(
        "worker-has-first",
        "first_local",
        peer_only_activity,
        &metrics,
    );

    let released_b = with_worker_running(&has_first, &pool, async {
        wait_for_miss_by(&url, exec_id, "worker-has-first", Duration::from_secs(30)).await
    })
    .await;

    // The premise: B really did make durable progress before missing.
    let history = load_history(&url, exec_id).await;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityCompleted { .. })),
        "phase 2's premise is that B durably completed frontier 1 before \
         missing frontier 2; without that event this test proves nothing: {history:?}"
    );
    assert_eq!(
        load_execution(&url, exec_id).await.state,
        "RUNNING",
        "B missed a frontier NO worker has yet been asked about -- A has never \
         been shown `second_local`. Failing here asserts the exact opposite of \
         the truth while a capable worker is live"
    );
    let released_b = released_b.expect(
        "B must record a miss and release; a sealed run here IS the bug -- the \
         miss was charged to the frontier A already missed",
    );
    assert_released_for_a_peer(&released_b);
    assert_eq!(
        released_b.capability_miss_workers,
        vec!["worker-has-first".to_string()],
        "durable inline progress starts a NEW frontier, so the set must hold \
         only the workers that missed THIS one -- A's miss was at frontier 1"
    );
    assert_eq!(
        released_b.capability_misses, 1,
        "the count is scoped to the current frontier too, or the configured-total \
         bound inherits the prior frontier's spend"
    );

    // --- Phase 3: A replays past frontier 1 and clears frontier 2. ----------
    let completed = with_worker_running(&has_second(), &pool, async {
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await
    })
    .await;

    assert_eq!(
        completed.output,
        Some(serde_json::json!("second frontier done")),
        "A replays past the durably-completed frontier 1 and runs frontier 2"
    );
    assert!(
        dead_letter_errors(&url, exec_id).await.is_empty(),
        "a run the fleet could finish must never reach the DLQ"
    );
}

// ---------------------------------------------------------------------------
// AC3 + AC5 — bounded release: escalate when no capable worker ever appears.
// ---------------------------------------------------------------------------

/// With **zero** capable workers the release must not loop forever. Once the
/// budget's worth of DISTINCT workers have each missed the task, the next one
/// escalates through the ordinary terminal-failure path, tagged with the
/// greppable `no_capable_worker:` prefix.
///
/// The budget is consumed per distinct worker (issue #804 round 6), so it is an
/// all-incapable *fleet* that exhausts it. Three prior workers are seeded onto
/// the row and this test's worker is the fourth — one past a budget of 3.
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
    // A budget of 3 that three distinct workers have already spent.
    seed_prior_missing_workers(
        &url,
        exec_id,
        &["pod-a-incapable", "pod-b-incapable", "pod-c-incapable"],
    )
    .await;

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

    // AC5 — escalated exactly once, with the fleet-exhaustion outcome. The knob
    // is named `capability_miss_max_redeliveries`, so a budget of N grants N
    // DISTINCT-worker releases and escalates on the N+1th distinct worker. This
    // worker is the fourth against a budget of 3, so it never releases at all.
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        1,
        "exactly one escalation, got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        0,
        "the budget was already spent by three distinct peers, so the fourth \
         must escalate on its first claim rather than release again, got {:?}",
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

/// **Issue #804 round-6 P1.** One incapable worker repeatedly winning the claim
/// race must never exhaust the redelivery budget.
///
/// The release backoff makes the task eligible to *every* worker again — the
/// one that just released it included — and the claim query has no capability
/// filter. Under plain total-miss accounting, `budget + 1` consecutive wins by
/// a single incapable pod terminally failed the run, which during a rolling
/// deploy is exactly the outage #804 exists to prevent: a capable peer is live
/// and idle the whole time, it just keeps losing a coin flip.
///
/// The budget is therefore consumed per DISTINCT worker. This test runs a
/// single incapable worker against a budget of 2 and lets it bounce the task
/// far past that budget: the distinct set stays at one entry, so it releases
/// every time and never escalates.
///
/// A live peer that never claims is seeded so the fleet evidence stays
/// `CapablePeerMayExist` — which is exactly the state this property is about.
/// The concern round 6 fixed is "a capable peer may be live and merely losing
/// the claim race", so the test has to be run in a fleet where that is true.
/// With the registry instead CONFIRMING that the lone worker is the whole live
/// fleet there is provably no peer to protect, and the configured budget bounds
/// total releases too (round 15) — see
/// `single_worker_fleet_escalates_at_the_configured_budget` below and the pure
/// `configured_total_bound_requires_confirmed_fleet_coverage`.
///
/// Falsifiable — verified by reverting `capability_miss_decision` to the
/// total-miss bound and re-running: with a budget of 2 the task escalates on
/// the third claim, so it never accrues a fourth miss and the row is gone.
/// `wait_for_capability_misses` times out rather than the `RUNNING` assertion
/// firing, but the cause is the same terminal failure this test exists to
/// forbid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_incapable_worker_cannot_exhaust_the_shared_redelivery_budget() {
    // Budget 2. Backoff is 1s, 2s, 4s, 8s..., so waiting for 4 total misses
    // takes ~7s and puts the task two claims past a budget that a single
    // worker would previously have exhausted.
    const BUDGET: u32 = 2;
    const MISSES: i32 = 4;

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

    // A live worker on this queue that never claims, so the registry can never
    // conclude the fleet has been swept. Without it the lone incapable pod IS
    // the confirmed fleet, and the round-15 total bound would (correctly)
    // escalate at BUDGET + 1 -- a different property, covered separately.
    seed_live_worker(&url, "pod-peer-live-never-claims", Q_ESCALATE).await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "the-one-incapable-pod",
        &[Q_ESCALATE],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        BUDGET,
    );

    let task = with_worker_running(&worker, &pool, async {
        wait_for_capability_misses(&url, exec_id, MISSES, Duration::from_secs(90)).await
    })
    .await;

    // The distinct set is the budget's unit of account, and one worker only
    // ever contributes one entry however many times it bounces the task.
    assert_eq!(
        task.capability_miss_workers,
        vec!["the-one-incapable-pod".to_string()],
        "a repeat miss by the same worker must not grow the distinct set"
    );
    assert!(
        task.capability_misses >= MISSES,
        "the total counter still advances on every bounce (it drives the \
         backoff and the absolute ceiling), got {}",
        task.capability_misses
    );

    // The headline: the run is untouched. Under the old accounting it would
    // have been terminally failed after `BUDGET + 1` claims.
    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a single incapable worker must never terminally fail a run that a \
         capable peer could still pick up; error was {:?}",
        execution.error
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "no escalation may be recorded, got {:?}",
        metrics.samples()
    );
    assert!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED)
            >= usize::try_from(MISSES).expect("small"),
        "every bounce releases, got {:?}",
        metrics.samples()
    );
    // AC4 stays true throughout: a capability miss is never a crash.
    assert_eq!(metrics.quarantine_count(), 0);
}

/// A dedicated queue so the *only* live worker on it is this test's. The
/// database is shared across the suite when `HARVEST_TEST_DATABASE_URL` is set,
/// and `live_workers_on_queue` filters by queue name — reusing `Q_ESCALATE`
/// would let another test's registered pod (or its deliberately-seeded
/// never-claiming peer) sit in this test's live set and flip the evidence to
/// `CapablePeerMayExist`, which is the state this test is the contrast for.
const Q_SOLO_FLEET: &str = "q804-solo-fleet";

/// **Issue #804 round-15 P1.** `capability_miss_max_redeliveries` must remain a
/// *maximum* in a fleet smaller than the budget.
///
/// The budget's unit of account is DISTINCT workers (round 6), so a fleet of
/// one can never satisfy `distinct_after > budget` however many times it
/// bounces the task. Before round 15 the only remaining bound was the absolute
/// `10x` release ceiling: with the default budget of 5 that is 51 claims and,
/// under the capped backoff, roughly 23 minutes before anyone is paged — for a
/// knob documented as five releases and ~31 seconds.
///
/// Round 15 applies the configured budget to *total* releases as well, but only
/// once the worker registry CONFIRMS every live worker on the queue has missed
/// the task (`AllLiveWorkersMissed`). That gate is what keeps round 6 intact:
/// on unconfirmed evidence the same total could have been run up by one pod
/// while a capable peer merely kept losing the claim race.
///
/// This is the exact contrast to
/// `one_incapable_worker_cannot_exhaust_the_shared_redelivery_budget`: same
/// single incapable pod, same repeated self-claim — the only difference is that
/// there a live peer is seeded so the fleet is never confirmed swept, and here
/// the lone pod IS the confirmed fleet, so the budget is free to bound it.
///
/// Falsifiable — verified by reverting the round-15 branch in
/// `capability_miss_decision`: the run stays `RUNNING` past the budget and
/// `wait_for_state(.., "FAILED", ..)` times out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_worker_fleet_escalates_at_the_configured_budget() {
    // Budget 2: releases at 1s and 2s of backoff, escalating on the third
    // claim ~3s in. The `10x` ceiling would need 21 claims and ~2 minutes, so
    // the assertion below distinguishes the two bounds by wall clock as well as
    // by wording.
    const BUDGET: u32 = 2;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "never_registered_wf",
        serde_json::json!({}),
        Q_SOLO_FLEET,
    )
    .await;

    // Deliberately NO seeded peer. The worker registers itself in
    // `harvest_workers` before its poll loop starts, so from its very first
    // miss the live set is exactly `[the-only-pod]` and the evidence is
    // `AllLiveWorkersMissed`.
    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "the-only-pod",
        &[Q_SOLO_FLEET],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        BUDGET,
    );

    let failed = with_worker_running(&worker, &pool, async {
        wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(60)).await
    })
    .await;

    let error = failed.error.unwrap_or_default();
    assert!(
        error.starts_with(NO_CAPABLE_WORKER_PREFIX),
        "escalation must carry the stable prefix, got {error:?}"
    );
    // The headline: the reason names the CONFIGURED budget, not the ceiling.
    assert!(
        error.contains("escalated after 2 capability-miss redeliveries"),
        "the reason must name the budget the operator configured, got {error:?}"
    );
    assert!(
        !error.contains("absolute release ceiling"),
        "a confirmed-fleet budget exhaustion must not inherit the ceiling's \
         wording -- that would send the operator to a knob 10x away from the \
         one that actually bounded this, got {error:?}"
    );
    assert!(
        error.contains("every worker with a live heartbeat here has now missed it"),
        "confirmed fleet coverage is what licenses applying the budget to \
         total releases, so the reason must say so, got {error:?}"
    );

    // Exactly BUDGET releases, then one escalation. `capability_misses` counts
    // the escalating claim too, so it lands at BUDGET + 1 while the released
    // count is BUDGET.
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        usize::try_from(BUDGET).expect("small"),
        "a budget of {BUDGET} must grant exactly {BUDGET} releases, got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        1,
        "exactly one escalation, and the fleet-confirmed one (never \
         `escalated_never_offered`, which is a ticket rather than a page), got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED),
        0,
        "the task WAS offered to a peer {BUDGET} times, so the never-offered \
         outcome would misroute the alert, got {:?}",
        metrics.samples()
    );

    // AC4 holds at escalation too: a capability miss is never a crash.
    assert_eq!(metrics.quarantine_count(), 0);
    assert!(
        dead_letter_errors(&url, exec_id).await.is_empty(),
        "escalation routes through the ordinary terminal-failure path, which \
         writes no dead-letter row"
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
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED),
        1,
        "the pinned task escalates on the first miss"
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED),
        0,
        "a session-pinned task must never be released: no peer can claim it"
    );

    // The metric must agree with the reason string, or the page and the error
    // column tell an operator two different stories. `harvest_no_capable_worker`
    // is severity `page` and selects `outcome="escalated"` exactly; its whole
    // narrative is "no live worker on this queue registers the handler", which
    // the reason above explicitly refuses to claim for a pinned task. Recording
    // the paging value here would page on-call and then send them to
    // `GET /admin/workflow-types/reachability`, which reports `in_use` and
    // contradicts the page they are holding.
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "a session-pinned escalation must NOT record the fleet-exhaustion value \
         that pages harvest_no_capable_worker: the task was never offered to a \
         peer, so it is not evidence the fleet lacks the handler"
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
        // A workflow-type lookup miss is `BeforeHandler`: the crash-strike
        // counter is preserved (Codex round-12 P1) and `attempt` is restored
        // (Codex round-17 P2).
        CapabilityMissPhase::BeforeHandler,
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
        // A workflow-type lookup miss is `BeforeHandler`: the crash-strike
        // counter is preserved (Codex round-12 P1) and `attempt` is restored
        // (Codex round-17 P2).
        CapabilityMissPhase::BeforeHandler,
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
        CapabilityMissPhase::BeforeHandler,
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

/// Register a worker row in `harvest_workers` advertising `queue` (issue #804).
///
/// The capability-miss fleet gate asks the registry which workers *could* claim
/// this task; a row inserted here is indistinguishable from a real worker that
/// has never happened to win a claim race for it.
async fn seed_live_worker(url: &str, worker_id: &str, queue: &str) {
    let mut conn = connect(url).await;
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, started_at, last_heartbeat_at, queues, shard_assignments, \
          max_concurrency, in_flight_count, host, status, build_id, labels) \
         VALUES ($1, NOW(), NOW(), $2::jsonb, '[]'::jsonb, 4, 0, 'test-host', 'Active', '', '{}'::jsonb) \
         ON CONFLICT (worker_id) DO UPDATE SET last_heartbeat_at = NOW(), queues = EXCLUDED.queues",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Text, _>(format!("[\"{queue}\"]"))
    .execute(&mut conn)
    .await
    .expect("seed live worker row");
}

/// Codex round-8 P1 / the success metric's load-bearing half: **zero spurious
/// FAILED executions as long as >= 1 capable worker is live.**
///
/// A fixed redelivery budget has no relationship to the live fleet, so a
/// rollout with `budget + 1` incapable pods could hand `budget + 1` DISTINCT
/// worker ids to the decision and terminally fail the run — while the capable
/// pod was live and polling the whole time, and while the error column asserted
/// the exact opposite.
///
/// The contrast case is
/// `capability_miss_escalates_after_the_budget_with_no_capable_worker`: same
/// shape, no live capable peer registered, and it escalates as AC3 requires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_never_escalates_while_a_capable_worker_is_live() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_workflow(
        &mut conn,
        "never_registered_wf",
        serde_json::json!({}),
        Q_LIVE_PEER,
    )
    .await;
    // Budget of 1, already spent by one distinct incapable worker: the next
    // distinct worker crosses the raw bound.
    seed_prior_missing_workers(&url, exec_id, &["pod-a-incapable"]).await;
    // ... but a capable worker is live on this queue and has never missed it.
    seed_live_worker(&url, "pod-capable-live", Q_LIVE_PEER).await;

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "worker-incapable-second",
        &[Q_LIVE_PEER],
        build_registry(
            vec![workflow_info("some_other_wf", decoy_workflow)],
            vec![],
            Arc::clone(&metrics),
        ),
        1,
    );

    with_worker_running(&worker, &pool, async {
        // Two releases PAST the point the ungated budget would have escalated.
        wait_for_capability_misses(&url, exec_id, 3, Duration::from_secs(60)).await;
    })
    .await;

    let execution = load_execution(&url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "a live capable peer must keep the task in play, got {:?} / {:?}",
        execution.state, execution.error
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED),
        0,
        "the distinct-worker budget must be withheld while a live worker has \
         never missed this task, got {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_ESCALATED_NEVER_OFFERED),
        0
    );
}

// ---------------------------------------------------------------------------
// Rate-limit token refund on release (#804 x #332, Codex round-11 P2)
// ---------------------------------------------------------------------------

const Q_RATE: &str = "q804-rate";

/// Read a rate-limit bucket's stored token count.
///
/// The stored value is only accurate as of `last_refilled_at` in general, but
/// the bucket this helper is used against is seeded with `refill_rate = 0.0`,
/// so no time-based refill can occur and the stored value *is* the effective
/// one. That is deliberate: a bucket that refills would replace the token on
/// its own and mask a missing refund entirely.
async fn stored_tokens(url: &str, key: &str) -> f64 {
    let mut conn = connect(url).await;
    harvest_rate_limit_buckets::table
        .find(key)
        .select(harvest_rate_limit_buckets::tokens)
        .first(&mut conn)
        .await
        .expect("read rate-limit bucket")
}

/// Releasing a rate-limited activity for a capable peer must give its token
/// back (issue #804, Codex round-11 P2).
///
/// `claim_task` debits a token for any rate-limited activity that is not
/// circuit-breaker-tracked, and an incapable worker's claim is no exception —
/// it is not tracked *because* the worker does not register the activity at
/// all. The activity never runs, so keeping the token charges real capacity to
/// a dispatch that did nothing. With a slow refill that is not cosmetic: the
/// capable peer waits a full refill interval it should not have to, and the
/// incapable worker (whose release backoff starts at 1 s) is positioned to take
/// each newly minted token again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_a_rate_limited_activity_refunds_its_token() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);

    let exec_id = seed_execution(&mut conn, "rate_limited_wf", serde_json::json!({})).await;

    // burst 1 / refill 0: exactly one token, ever. Any refill would mask a
    // missing refund by replacing the token on its own.
    //
    // The key is unique per run. A shared key would let a PENDING task left
    // behind by an earlier run race for the single token, so a later run could
    // fail for having lost that race rather than for the property under test.
    let key = format!("q804-rate-bucket-{exec_id}");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, 0.0, 1.0, 1.0, NOW())",
    )
    .bind::<diesel::sql_types::Text, _>(key.as_str())
    .execute(&mut conn)
    .await
    .expect("seed rate-limit bucket");

    let mut params = EnqueueParams::new(Q_RATE, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("rate_limited_activity".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(key.clone());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue rate-limited activity task");

    let metrics = Arc::new(CapabilityMetrics::default());
    let worker = build_worker(
        "worker-rate-incapable",
        &[Q_RATE],
        build_registry(
            vec![],
            // Registers a DIFFERENT activity, so `rate_limited_activity` is a
            // capability miss -- and, being unregistered, is necessarily absent
            // from this worker's circuit-breaker tracked set, so its claim took
            // the debiting branch.
            vec![activity_info("some_other_activity", decoy_activity, Q_RATE)],
            Arc::clone(&metrics),
        ),
        50,
    );

    with_worker_running(&worker, &pool, async {
        wait_for_capability_misses(&url, exec_id, 1, Duration::from_secs(30)).await;
    })
    .await;

    assert!(
        metrics.count_with_outcome(CAPABILITY_MISS_OUTCOME_RELEASED) >= 1,
        "the task must have been released, got {:?}",
        metrics.samples()
    );

    // The refund is the whole point: with refill 0, a bucket that is not
    // refunded stays at 0.0 forever and the capable peer can never claim.
    let tokens = stored_tokens(&url, &key).await;
    assert!(
        tokens >= 0.99,
        "releasing an activity that never ran must refund its claim-time token, \
         leaving the bucket full; got {tokens}"
    );
}

// ---------------------------------------------------------------------------
// A release must not launder a poison task's crash history (issue #804 x #367).
// ---------------------------------------------------------------------------

/// A pre-handler capability miss must **preserve** `crash_strikes` (Codex
/// round-12 P1).
///
/// `crash_strikes` is issue #367's evidence that a task keeps killing the worker
/// process that runs it: `reclaim_orphaned_tasks` increments it each time the
/// row is found orphaned under a worker that stopped heartbeating, and
/// quarantines the task once it reaches `poison_pill_threshold`.
///
/// A capability miss ran nothing, so it is evidence about *neither* direction.
/// Zeroing the column on it is not merely generous — in a heterogeneous fleet it
/// is a silent defeat of the quarantine: an incapable claim landing between two
/// capable crashes resets the streak to zero every time, so the threshold is
/// never reached and the poison task goes on crashing replacement workers
/// indefinitely.
///
/// AC4 requires only that a clean miss never *increments* a crash counter, and
/// preserving satisfies that strictly more conservatively than zeroing did.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_a_pre_handler_miss_preserves_the_poison_pill_crash_strikes() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "noop_wf", serde_json::json!({}), Q_NOOP).await;
    let task_id = load_tasks(&url, exec_id).await[0].id;

    // The exact interleaving the finding describes: two capable workers have
    // already died on this row (so `reclaim_orphaned_tasks` banked two strikes),
    // and an incapable worker now wins the claim race.
    diesel::sql_query(
        "UPDATE harvest_task_queue \
            SET state = 'RUNNING', worker_id = 'incapable', started_at = NOW(), \
                crash_strikes = 2 \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("seed a claimed row that already crashed two workers");
    assert_eq!(load_task(&url, task_id).await.crash_strikes, 2);

    let released = queue::release_task_for_capability_miss(
        &mut conn,
        task_id,
        "incapable",
        Duration::from_secs(1),
        CapabilityMissPhase::BeforeHandler,
    )
    .await
    .expect("release query runs");
    assert!(released);

    let task = load_task(&url, task_id).await;
    assert_eq!(
        task.crash_strikes, 2,
        "the incapable worker ran nothing, so it must not erase the two worker \
         deaths this task has already caused; zeroing here means the next \
         capable worker's crash restarts the streak at 1 and \
         `poison_pill_threshold` is never reached"
    );
    assert_eq!(
        task.state, "PENDING",
        "the release itself must still work exactly as before"
    );
    assert_eq!(
        task.capability_misses, 1,
        "the redelivery counter is the ONLY counter a capability miss advances"
    );

    // The complement: a post-handler miss DID watch the body run to a
    // conclusion without killing this worker, so the streak is genuinely broken.
    let exec2 = seed_workflow(&mut conn, "noop_wf", serde_json::json!({ "n": 2 }), Q_NOOP).await;
    let task2 = load_tasks(&url, exec2).await[0].id;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
            SET state = 'RUNNING', worker_id = 'incapable', started_at = NOW(), \
                crash_strikes = 2, attempt = 1 \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task2)
    .execute(&mut conn)
    .await
    .expect("seed a second claimed row");

    let released2 = queue::release_task_for_capability_miss(
        &mut conn,
        task2,
        "incapable",
        Duration::from_secs(1),
        CapabilityMissPhase::AfterHandler,
    )
    .await
    .expect("release query runs");
    assert!(released2);
    let after = load_task(&url, task2).await;
    assert_eq!(
        after.crash_strikes, 0,
        "the body returned its commands on this very dispatch, so this worker \
         demonstrably survived running it"
    );
    assert_eq!(
        after.attempt, 1,
        "the SAME phase that clears the crash strikes must PRESERVE `attempt`: \
         the body ran, so `record_workflow_started` already fired and rolling \
         back to 0 would let the next capable claim re-emit it (round-17 P2)"
    );
}
