#![cfg(feature = "db")]
//! Logical-handle routing across the workflow-level retry chain — issue #843.
//!
//! Workflow-level retry (#523) starts each attempt as a **fresh execution** with
//! a new `exec_id`, linked to its predecessor via `retry_of_exec_id`. The result
//! wait already follows that chain (#842), but the *interactive and mutating*
//! operations did not: a caller still holding the original handle would cancel a
//! sealed `FAILED` predecessor while the retry kept running, drop a signal onto
//! it, or be told the run is not running by a query/update.
//!
//! These tests pin the "logical handle" contract: the original `exec_id` names
//! the logical run, and every mutating/interactive operation is routed to the
//! **live attempt** — the deepest execution reachable by following
//! `retry_of_exec_id` successors from `FAILED` rows.

use std::any::TypeId;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::schema::{
    harvest_signals, harvest_task_queue, harvest_timers, harvest_workflow_executions,
};
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, Priority, ShardId, StartWorkflowParams, WorkflowContext,
    start_or_load_workflow_execution,
};

use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── Harness ────────────────────────────────────────────────────────────────

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

/// Dual-mode setup: prefer an operator-supplied `HARVEST_TEST_DATABASE_URL`
/// (creating a fresh per-test database off it so tests in this binary never
/// share state), else fall back to testcontainers.
async fn setup() -> (String, Option<ContainerAsync<Postgres>>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_843_{}", uuid::Uuid::new_v4().simple());
        let mut admin = connect(&admin_url).await;
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin)
            .await
            .expect("create per-test database");
        drop(admin);
        let url = replace_database(&admin_url, &db_name);
        let mut conn = connect(&url).await;
        SimpleAsyncConnection::batch_execute(&mut conn, autumn_harvest::full_migrations_sql())
            .await
            .expect("apply migrations");
        drop(conn);
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

/// Swap the database component of a `postgres://…/db` URL.
fn replace_database(url: &str, db_name: &str) -> String {
    let (prefix, _) = url
        .rsplit_once('/')
        .expect("url must carry a database path");
    format!("{prefix}/{db_name}")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect failed")
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

#[derive(Debug, Default)]
struct NoMetrics;
impl MetricsRecorder for NoMetrics {}

#[derive(Debug, Default)]
struct CallCounter {
    count: AtomicUsize,
}

impl CallCounter {
    fn increment(&self) -> usize {
        self.count.fetch_add(1, Ordering::SeqCst) + 1
    }
    fn get(&self) -> usize {
        AtomicUsize::load(&self.count, Ordering::SeqCst)
    }
}

/// Records which attempt numbers actually started executing, so a test can
/// prove a cancelled chain never spawned a further attempt.
#[derive(Debug, Default)]
struct AttemptLog {
    seen: Mutex<Vec<usize>>,
}

fn make_shared_state(
    counter: Arc<CallCounter>,
    log: Arc<AttemptLog>,
) -> autumn_harvest::context::SharedState {
    let mut map: HashMap<TypeId, Box<dyn std::any::Any + Send + Sync>> = HashMap::new();
    map.insert(TypeId::of::<Arc<CallCounter>>(), Box::new(counter));
    map.insert(TypeId::of::<Arc<AttemptLog>>(), Box::new(log));
    Arc::new(map)
}

/// Attempt 1 fails; every later attempt parks on a long durable timer so the
/// live retry stays observably `RUNNING` for the whole test.
fn fail_then_park_handler(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    let counter = ctx
        .state::<Arc<CallCounter>>()
        .expect("CallCounter in shared state")
        .clone();
    let log = ctx
        .state::<Arc<AttemptLog>>()
        .expect("AttemptLog in shared state")
        .clone();
    Box::pin(async move {
        // Only count the first entry per attempt: the body is re-driven on
        // every replay cycle, so gate on whether this execution already parked.
        if ctx.history_event_count() <= 1 {
            let n = counter.increment();
            log.seen.lock().unwrap().push(n);
            if n == 1 {
                return Err("transient error".to_string());
            }
        }
        ctx.timer("park", 3600).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!("done"))
    })
}

fn wf_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
    retry_policy: Option<RetryPolicy>,
) -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "retry_chain_routing_tests",
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
        retry_policy,
    }
}

fn make_worker(
    workflows: Vec<WorkflowInfo>,
    shared_state: autumn_harvest::context::SharedState,
) -> Worker {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::new(NoMetrics) as Arc<dyn MetricsRecorder + Send + Sync>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        vec![],
        shared_state,
        telemetry,
    ));
    Worker::new(
        WorkerRuntimeConfig {
            worker_id: uuid::Uuid::new_v4().to_string(),
            queues: vec!["default".to_string()],
            queue_weights: std::collections::HashMap::new(),
            notification_database_url: None,
            shard_notification_database_urls: Vec::new(),
            max_concurrent_workflows: 10,
            max_concurrent_activities: 20,
            poll_interval: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(2),
            cancellation_grace_period: Duration::from_secs(2),
            sticky_timeout: Duration::ZERO,
            max_local_activity_start_to_close: Duration::from_secs(60),
            shard_assignments: vec![ShardId::new(0)],
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            workflow_cache_size: 100,
            priority_aging_secs: None,
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            capability_miss_max_redeliveries: 5,
            workflow_task_timeout: Duration::from_secs(30),
            workflow_panic_max_attempts: 3,
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            labels: std::collections::HashMap::new(),
            sharded_pool: None,
            max_workflow_history_events: None,
            slot_tuner: None,
            max_concurrent_sessions: 0,
        },
        registry,
    )
    .expect("worker should build")
}

const fn fast_retry_policy(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    }
}

async fn start_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    workflow_id: &str,
    retry_policy: Option<RetryPolicy>,
) -> ExecutionId {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: Value::Null,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: retry_policy,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("workflow start should succeed")
    .exec_id
}

async fn get_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .expect("execution must exist")
}

/// Wait until the retry successor of `original` exists and is durably parked on
/// its timer — the unambiguous "attempt 2 is live" signal.
async fn wait_for_parked_retry(conn: &mut AsyncPgConnection, original: ExecutionId) -> ExecutionId {
    for _ in 0..600 {
        let rows: Vec<uuid::Uuid> = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::retry_of_exec_id.eq(Some(original.as_uuid())))
            .select(harvest_workflow_executions::id)
            .load(conn)
            .await
            .expect("load retry executions");
        for id in rows {
            let timers: i64 = harvest_timers::table
                .filter(harvest_timers::workflow_exec_id.eq(id))
                .count()
                .get_result(conn)
                .await
                .expect("count timers");
            if timers > 0 {
                return ExecutionId::from_uuid(id);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("retry successor of {original} never parked on its timer");
}

/// Drive a worker until attempt 1 has failed and attempt 2 is parked.
/// Returns `(url, original_exec_id, retry_exec_id, worker, worker_handle, log)`.
struct ParkedChain {
    url: String,
    original: ExecutionId,
    retry: ExecutionId,
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
    log: Arc<AttemptLog>,
    counter: Arc<CallCounter>,
    container: Option<ContainerAsync<Postgres>>,
}

async fn parked_chain(workflow_id: &str) -> ParkedChain {
    let (url, container) = setup().await;
    let mut conn = connect(&url).await;
    let policy = fast_retry_policy(3);
    let original = start_workflow(&mut conn, "routing_wf", workflow_id, Some(policy.clone())).await;
    drop(conn);

    let counter = Arc::new(CallCounter::default());
    let log = Arc::new(AttemptLog::default());
    let pool = build_pool(&url);
    let worker = Arc::new(make_worker(
        vec![wf_info("routing_wf", fail_then_park_handler, Some(policy))],
        make_shared_state(counter.clone(), log.clone()),
    ));
    let worker_ref = worker.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(60), worker_ref.run(&pool)).await;
    });

    let mut check = connect(&url).await;
    let retry = wait_for_parked_retry(&mut check, original).await;
    assert_eq!(get_state(&mut check, original).await, "FAILED");
    assert_eq!(get_state(&mut check, retry).await, "RUNNING");
    drop(check);

    ParkedChain {
        url,
        original,
        retry,
        worker,
        handle,
        log,
        counter,
        container,
    }
}

impl ParkedChain {
    async fn stop(self) -> (String, Option<ContainerAsync<Postgres>>) {
        self.worker.shutdown();
        let _ = self.handle.await;
        (self.url, self.container)
    }
}

// ── AC2: cancel routes to the live attempt ────────────────────────────────

/// A `cancel` issued against the ORIGINAL execution id must stop the live retry
/// attempt, not no-op against the sealed `FAILED` predecessor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_routes_to_the_live_retry_attempt() {
    let chain = parked_chain("route-cancel-001").await;
    let url = chain.url.clone();
    let (original, retry) = (chain.original, chain.retry);
    let mut conn = connect(&url).await;

    let result = autumn_harvest::execution::cancel_live_attempt(
        &mut conn,
        original,
        "operator asked to stop",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel must succeed against the live attempt");

    assert_eq!(
        result.exec_id, retry,
        "cancel must report the live attempt it actually cancelled"
    );
    assert!(
        result.newly_cancelled,
        "the live attempt must be newly cancelled"
    );
    assert_eq!(get_state(&mut conn, retry).await, "CANCELLED");
    // The sealed predecessor is untouched.
    assert_eq!(get_state(&mut conn, original).await, "FAILED");
    drop(conn);

    chain.stop().await;
}

/// A cancelled chain must not spawn a further attempt: the live attempt is
/// sealed `CANCELLED` and no attempt 3 ever runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_chain_never_spawns_a_further_attempt() {
    let chain = parked_chain("route-cancel-002").await;
    let url = chain.url.clone();
    let (original, retry) = (chain.original, chain.retry);
    let log = chain.log.clone();
    let mut conn = connect(&url).await;

    autumn_harvest::execution::cancel_live_attempt(
        &mut conn,
        original,
        "stop the chain",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel must succeed");

    // Give the worker a chance to (incorrectly) start a further attempt.
    tokio::time::sleep(Duration::from_millis(750)).await;

    let successors: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::retry_of_exec_id.eq(Some(retry.as_uuid())))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count successors");
    assert_eq!(successors, 0, "a cancelled attempt must not spawn a retry");
    assert_eq!(get_state(&mut conn, retry).await, "CANCELLED");
    assert_eq!(
        log.seen.lock().unwrap().as_slice(),
        &[1usize, 2usize],
        "exactly two attempts must have started"
    );
    drop(conn);

    chain.stop().await;
}

// ── AC2: terminate routes to the live attempt ─────────────────────────────

/// A `terminate` issued against the ORIGINAL execution id must seal the live
/// retry attempt `TERMINATED` — not silently idempotent-no-op against the
/// already-terminal `FAILED` predecessor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_routes_to_the_live_retry_attempt() {
    let chain = parked_chain("route-terminate-001").await;
    let url = chain.url.clone();
    let (original, retry) = (chain.original, chain.retry);
    let mut conn = connect(&url).await;

    let result = autumn_harvest::execution::terminate_live_attempt(
        &mut conn,
        original,
        "operator force-kill",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("terminate must succeed against the live attempt");

    assert_eq!(result.exec_id, retry);
    assert!(
        result.newly_cancelled,
        "the live attempt must be newly terminated, not an idempotent no-op"
    );
    assert_eq!(get_state(&mut conn, retry).await, "TERMINATED");
    assert_eq!(get_state(&mut conn, original).await, "FAILED");

    // Terminate fails every open task row, so the retry cannot be re-claimed.
    let open: i64 = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(retry.as_uuid())))
        .filter(harvest_task_queue::state.eq_any(["PENDING", "RUNNING"]))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count open tasks");
    assert_eq!(open, 0, "terminate must fail the live attempt's open tasks");
    drop(conn);

    chain.stop().await;
}

// ── AC1: signals route to the live attempt ────────────────────────────────

/// A signal sent against the ORIGINAL execution id must land on the live retry
/// attempt, not be dropped onto the sealed `FAILED` predecessor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_routes_to_the_live_retry_attempt() {
    let chain = parked_chain("route-signal-001").await;
    let url = chain.url.clone();
    let (original, retry) = (chain.original, chain.retry);
    let mut conn = connect(&url).await;

    let delivery = autumn_harvest::signal::send_signal_to_live_attempt(
        &mut conn,
        original,
        "approve",
        serde_json::json!({"by": "alice"}),
        None,
    )
    .await
    .expect("signal must be delivered to the live attempt");

    assert!(delivery.delivered, "a fresh signal must be queued");
    assert_eq!(
        delivery.target, retry,
        "the signal must be routed to the live attempt"
    );

    let on_retry: i64 = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(retry.as_uuid()))
        .filter(harvest_signals::signal_name.eq("approve"))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count signals on retry");
    assert_eq!(
        on_retry, 1,
        "the signal row must belong to the live attempt"
    );

    let on_original: i64 = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(original.as_uuid()))
        .filter(harvest_signals::signal_name.eq("approve"))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count signals on original");
    assert_eq!(
        on_original, 0,
        "no signal may be dropped onto the sealed predecessor"
    );
    drop(conn);

    chain.stop().await;
}

/// #521 idempotency keys keep working through routing: two keyed deliveries
/// against the original id land exactly one signal row on the live attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_signal_dedupes_on_the_routed_live_attempt() {
    let chain = parked_chain("route-signal-002").await;
    let url = chain.url.clone();
    let (original, retry) = (chain.original, chain.retry);
    let mut conn = connect(&url).await;

    let first = autumn_harvest::signal::send_signal_to_live_attempt(
        &mut conn,
        original,
        "approve",
        serde_json::json!(1),
        Some("delivery-abc"),
    )
    .await
    .expect("first keyed delivery");
    let second = autumn_harvest::signal::send_signal_to_live_attempt(
        &mut conn,
        original,
        "approve",
        serde_json::json!(1),
        Some("delivery-abc"),
    )
    .await
    .expect("second keyed delivery");

    assert!(
        first.delivered,
        "first keyed delivery must queue the signal"
    );
    assert!(!second.delivered, "second keyed delivery must dedupe");
    assert_eq!(first.target, retry);
    assert_eq!(second.target, retry);

    let rows: i64 = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(retry.as_uuid()))
        .filter(harvest_signals::signal_name.eq("approve"))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count signals");
    assert_eq!(rows, 1, "exactly one signal row must exist");
    drop(conn);

    chain.stop().await;
}

// ── AC1: unconsumed signals are forwarded across the retry boundary ───────

/// Signals staged on a failing attempt but never ingested into its history are
/// reassigned to the retry successor — mirroring the continue-as-new behavior
/// the engine already has for the same fresh-history transition. Consumed
/// signals stay with the attempt that consumed them, for audit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unconsumed_signals_are_forwarded_to_the_retry_successor() {
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;

    let predecessor = start_workflow(&mut conn, "fwd_wf", "fwd-001", None).await;
    let successor = start_workflow(&mut conn, "fwd_wf", "fwd-002", None).await;

    for (name, consumed) in [("fresh", false), ("already-seen", true)] {
        diesel::insert_into(harvest_signals::table)
            .values((
                harvest_signals::workflow_exec_id.eq(predecessor.as_uuid()),
                harvest_signals::signal_name.eq(name),
                harvest_signals::payload.eq(serde_json::json!({})),
                harvest_signals::consumed.eq(consumed),
            ))
            .execute(&mut conn)
            .await
            .expect("seed signal");
    }

    let moved =
        autumn_harvest::signal::forward_unconsumed_signals(&mut conn, predecessor, successor)
            .await
            .expect("forward unconsumed signals");
    assert_eq!(moved, 1, "exactly the unconsumed signal must move");

    let on_successor: Vec<String> = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(successor.as_uuid()))
        .select(harvest_signals::signal_name)
        .load(&mut conn)
        .await
        .expect("load successor signals");
    assert_eq!(on_successor, vec!["fresh".to_string()]);

    let on_predecessor: Vec<String> = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(predecessor.as_uuid()))
        .select(harvest_signals::signal_name)
        .load(&mut conn)
        .await
        .expect("load predecessor signals");
    assert_eq!(
        on_predecessor,
        vec!["already-seen".to_string()],
        "a consumed signal must stay with the attempt that consumed it"
    );
}

// ── AC3: queries/updates route to the live attempt ────────────────────────

/// An update admitted against the ORIGINAL execution id must be admitted on the
/// live retry attempt rather than rejected as not-running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_routes_to_the_live_retry_attempt() {
    let chain = parked_chain("route-update-001").await;
    let url = chain.url.clone();
    let (original, retry) = (chain.original, chain.retry);
    let mut conn = connect(&url).await;

    let update_id = autumn_harvest::UpdateId::new();
    let target = autumn_harvest::execution::resolve_live_attempt_id(&mut conn, original)
        .await
        .expect("resolve live attempt");
    assert_eq!(target, retry);

    autumn_harvest::store::admit_update_event(
        &mut conn,
        target,
        update_id,
        "adjust".to_string(),
        serde_json::json!({"delta": 1}),
        None,
    )
    .await
    .expect("update must be admitted on the live attempt");

    let history = autumn_harvest::store::load_history(&mut conn, retry)
        .await
        .expect("load retry history");
    assert!(
        history.events.iter().any(|e| matches!(
            e,
            autumn_harvest::event::WorkflowEvent::UpdateAdmitted { .. }
        )),
        "the live attempt's history must carry the admitted update"
    );
    drop(conn);

    chain.stop().await;
}

// ── AC4: reads ────────────────────────────────────────────────────────────

/// `resolve_live_attempt_id` walks the chain to the deepest live attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_live_attempt_walks_the_chain() {
    let chain = parked_chain("route-resolve-001").await;
    let url = chain.url.clone();
    let (original, retry) = (chain.original, chain.retry);
    let mut conn = connect(&url).await;

    assert_eq!(
        autumn_harvest::execution::resolve_live_attempt_id(&mut conn, original)
            .await
            .expect("resolve"),
        retry
    );
    // Resolving from the deepest attempt is a fixed point.
    assert_eq!(
        autumn_harvest::execution::resolve_live_attempt_id(&mut conn, retry)
            .await
            .expect("resolve"),
        retry
    );
    drop(conn);

    chain.stop().await;
}

/// AC6 regression guard: with no retry chain, resolution is a strict no-op —
/// including for a genuinely-failed run whose failure is the final outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_live_attempt_is_a_noop_without_a_retry_chain() {
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;

    let running = start_workflow(&mut conn, "plain_wf", "plain-001", None).await;
    assert_eq!(
        autumn_harvest::execution::resolve_live_attempt_id(&mut conn, running)
            .await
            .expect("resolve running"),
        running
    );

    // A terminal FAILED run with no successor resolves to itself, so a
    // post-mortem read/operation still targets it.
    diesel::update(harvest_workflow_executions::table.find(running.as_uuid()))
        .set(harvest_workflow_executions::state.eq("FAILED"))
        .execute(&mut conn)
        .await
        .expect("seal failed");
    assert_eq!(
        autumn_harvest::execution::resolve_live_attempt_id(&mut conn, running)
            .await
            .expect("resolve failed"),
        running
    );
}

/// The counter helper is used by the chain fixture; assert it is wired so a
/// silently-broken fixture cannot make the routing tests vacuous.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixture_runs_exactly_two_attempts() {
    let chain = parked_chain("route-fixture-001").await;
    assert_eq!(chain.counter.get(), 2, "attempt 1 failed, attempt 2 parked");
    chain.stop().await;
}
