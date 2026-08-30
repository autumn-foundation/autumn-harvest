#![cfg(feature = "db")]
//! Synthetic liveness canary end-to-end emission tests — issue #796.
//!
//! Exercises the *actual* recorder branch (not just the `is_canary_workflow`
//! predicate) end to end against a real worker + timeout scanner:
//!
//! - AC5/AC8: a completed canary probe emits `harvest.canary.success` + `harvest.canary.roundtrip` and none of `harvest.workflow.terminal`/`started`/`duration`.
//! - AC6/AC8: a canary past its per-probe execution timeout emits `harvest.canary.failure` and none of `harvest.workflow.terminal`/`timeout`.
//! - A NORMAL workflow still increments `harvest.workflow.terminal` (non-canary behavior is byte-identical).
//!
//! The probe workflow here is a faithful, hand-built copy of the plugin's
//! built-in canary (which lives in `autumn-harvest-plugin` and so cannot be
//! imported from the core crate): its `name` carries the reserved
//! `__harvest_canary_probe` prefix so `is_canary_workflow` matches, and its body
//! dispatches one non-local activity then waits on a short durable timer, exactly
//! like the real probe.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::canary::CANARY_ACTIVITY_NAME;
use autumn_harvest::context::empty_shared_state;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig, WorkflowStatus};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, Priority, ShardId, StartWorkflowParams, WorkflowContext,
    start_or_load_workflow_execution, timeout,
};

use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const CANARY_WF: &str = "__harvest_canary_probe__default";
const NORMAL_WF: &str = "normal_wf";

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

// ── Recording metrics ──────────────────────────────────────────────────────

// Mutex-based (not atomics): `diesel_async::RunQueryDsl`'s blanket `load` method
// shadows `AtomicUsize::load` in this file's scope.
#[derive(Debug, Default)]
struct RecordingMetrics {
    canary_success: Mutex<Vec<(String, u16)>>,
    canary_failure: Mutex<Vec<(String, u16)>>,
    canary_roundtrip: Mutex<Vec<(String, u16, f64)>>,
    /// (`workflow_name`, queue, outcome)
    terminal: Mutex<Vec<(String, String, String)>>,
    /// `workflow_name` of each `record_workflow_started`
    started: Mutex<Vec<String>>,
    /// `workflow_name` of each `record_workflow_completed` (`harvest.workflow.duration`)
    completed_duration: Mutex<Vec<String>>,
    /// (`workflow_name`, queue) of each `record_workflow_timeout`
    workflow_timeout: Mutex<Vec<(String, String)>>,
}

impl RecordingMetrics {
    fn canary_success_count(&self) -> usize {
        self.canary_success.lock().unwrap().len()
    }
    fn canary_failure_count(&self) -> usize {
        self.canary_failure.lock().unwrap().len()
    }
    fn canary_roundtrip_count(&self) -> usize {
        self.canary_roundtrip.lock().unwrap().len()
    }
    fn terminal_count_for(&self, wf: &str) -> usize {
        self.terminal
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _, _)| name == wf)
            .count()
    }
    fn started_count_for(&self, wf: &str) -> usize {
        self.started
            .lock()
            .unwrap()
            .iter()
            .filter(|n| *n == wf)
            .count()
    }
    fn duration_count_for(&self, wf: &str) -> usize {
        self.completed_duration
            .lock()
            .unwrap()
            .iter()
            .filter(|n| *n == wf)
            .count()
    }
    fn workflow_timeout_count(&self) -> usize {
        self.workflow_timeout.lock().unwrap().len()
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_canary_success(&self, queue: &str, shard: u16) {
        self.canary_success
            .lock()
            .unwrap()
            .push((queue.to_string(), shard));
    }
    fn record_canary_failure(&self, queue: &str, shard: u16) {
        self.canary_failure
            .lock()
            .unwrap()
            .push((queue.to_string(), shard));
    }
    fn record_canary_roundtrip(&self, queue: &str, shard: u16, duration_secs: f64) {
        self.canary_roundtrip
            .lock()
            .unwrap()
            .push((queue.to_string(), shard, duration_secs));
    }
    fn record_workflow_terminal(&self, workflow_name: &str, queue: &str, outcome: WorkflowStatus) {
        self.terminal.lock().unwrap().push((
            workflow_name.to_string(),
            queue.to_string(),
            outcome.as_str().to_string(),
        ));
    }
    fn record_workflow_started(&self, workflow_name: &str, _queue: &str) {
        self.started.lock().unwrap().push(workflow_name.to_string());
    }
    fn record_workflow_completed(
        &self,
        workflow_name: &str,
        _queue: &str,
        _duration_secs: f64,
        _status: WorkflowStatus,
    ) {
        self.completed_duration
            .lock()
            .unwrap()
            .push(workflow_name.to_string());
    }
    fn record_workflow_timeout(&self, workflow_name: &str, queue: &str) {
        self.workflow_timeout
            .lock()
            .unwrap()
            .push((workflow_name.to_string(), queue.to_string()));
    }
}

// ── Handlers (fn pointers, not closures) ────────────────────────────────────

/// Faithful copy of the built-in canary probe: dispatch one non-local activity
/// on the probe queue, wait on a short durable timer, complete.
fn canary_probe_handler(
    ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        ctx.execute_activity_raw(CANARY_ACTIVITY_NAME, serde_json::json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("probe", 1).await.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// The canary activity: trivial, just proves the dispatch path works.
fn canary_activity_handler(
    _ctx: &autumn_harvest::context::ActivityContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move { Ok(serde_json::json!({ "ok": true })) })
}

/// A normal (non-canary) workflow that completes immediately.
fn normal_handler(
    _ctx: &WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move { Ok(serde_json::json!("done")) })
}

// ── Test helpers ────────────────────────────────────────────────────────────

async fn setup() -> (String, Option<ContainerAsync<Postgres>>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    // Local-PG runs: provision a fresh, uniquely-named, migrated database off the
    // base URL per call (mirroring the testcontainers per-test isolation), so the
    // suite is re-runnable against a persistent Postgres. These tests reuse fixed
    // workflow_ids, so a shared database would let a prior run's terminal
    // execution short-circuit a fresh start and starve the worker of the probe.
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (provision_ephemeral_db(&base_url).await, None);
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

/// Provision a fresh, uniquely-named, migrated database off `base_url`, giving
/// each test the same per-test isolation the testcontainers path provides. The
/// base role must be able to `CREATE DATABASE`.
async fn provision_ephemeral_db(base_url: &str) -> String {
    use diesel_async::SimpleAsyncConnection;

    let db_name = format!("harvest_canary_core_{}", uuid::Uuid::new_v4().simple());
    let mut admin = AsyncPgConnection::establish(base_url)
        .await
        .expect("connect to base database");
    diesel::sql_query(format!("CREATE DATABASE \"{db_name}\""))
        .execute(&mut admin)
        .await
        .expect("create ephemeral database");

    let (prefix, _old_db) = base_url
        .rsplit_once('/')
        .expect("base url must carry a /<database> path segment");
    let new_url = format!("{prefix}/{db_name}");

    let mut conn = AsyncPgConnection::establish(&new_url)
        .await
        .expect("connect to ephemeral database");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("apply migration bundle to ephemeral database");
    new_url
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

fn canary_wf_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        name: CANARY_WF,
        module: "canary_tests",
        handler: canary_probe_handler,
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
        mcp: false,
    }
}

fn normal_wf_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        name: NORMAL_WF,
        module: "canary_tests",
        handler: normal_handler,
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
        mcp: false,
    }
}

fn canary_activity_info() -> ActivityInfo {
    ActivityInfo {
        name: CANARY_ACTIVITY_NAME,
        module: "canary_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(10)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        default_schedule_to_close: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: canary_activity_handler,
    }
}

fn make_worker(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    metrics: Arc<dyn MetricsRecorder + Send + Sync>,
) -> Worker {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        empty_shared_state(),
        telemetry,
    ));
    Worker::new(
        WorkerRuntimeConfig {
            dr_fencing: false,
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
            build_id: "canary-test".to_string(),
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

fn spawn_worker(worker: Worker, pool: DbPool) -> (Arc<Worker>, tokio::task::JoinHandle<()>) {
    let worker = Arc::new(worker);
    let worker_ref = worker.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(90), worker_ref.run(&pool)).await;
    });
    (worker, handle)
}

async fn start_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    workflow_id: &str,
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
            workflow_retry_policy: None,
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

async fn wait_for_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, want: &str) {
    for _ in 0..1200 {
        if get_state(conn, exec_id).await == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
    panic!(
        "execution {exec_id} never reached {want}; last state = {}",
        get_state(conn, exec_id).await
    );
}

/// Backdate `deadline_at` into the past so the execution-timeout scanner fires
/// it on the next sweep (skips waiting for the real deadline).
async fn expire_deadline_now(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    let n = diesel::update(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid())),
    )
    .set(
        harvest_workflow_executions::deadline_at
            .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(5))),
    )
    .execute(conn)
    .await
    .expect("backdate deadline_at");
    assert_eq!(n, 1, "deadline_at must be updated for exactly one row");
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// AC5/AC8: a canary probe driven to COMPLETION emits ONLY `harvest.canary.*`
/// (success + roundtrip) and NO `harvest.workflow.*` business counter.
#[tokio::test]
async fn canary_probe_completion_emits_only_canary_metrics() {
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);
    let recording = Arc::new(RecordingMetrics::default());

    let worker = make_worker(
        vec![canary_wf_info()],
        vec![canary_activity_info()],
        recording.clone(),
    );
    let (worker, handle) = spawn_worker(worker, pool);

    let exec_id = start_workflow(&mut conn, CANARY_WF, "canary-success-1").await;
    wait_for_state(&mut conn, exec_id, "COMPLETED").await;

    worker.shutdown();
    let _ = handle.await;

    // AC5: the probe emitted its own liveness signal.
    assert!(
        recording.canary_success_count() >= 1,
        "a completed probe must emit harvest.canary.success"
    );
    assert!(
        recording.canary_roundtrip_count() >= 1,
        "a completed probe must emit harvest.canary.roundtrip"
    );
    // AC8: the probe emitted NONE of the harvest.workflow.* business counters.
    assert_eq!(
        recording.terminal_count_for(CANARY_WF),
        0,
        "a canary must never increment harvest.workflow.terminal"
    );
    assert_eq!(
        recording.started_count_for(CANARY_WF),
        0,
        "a canary must never increment harvest.workflow.started"
    );
    assert_eq!(
        recording.duration_count_for(CANARY_WF),
        0,
        "a canary must never increment harvest.workflow.duration"
    );
}

/// AC6/AC8: a canary that exceeds its per-probe execution timeout emits
/// `harvest.canary.failure` and NO `harvest.workflow.terminal`/timeout.
#[tokio::test]
async fn canary_execution_timeout_emits_failure_not_terminal() {
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let recording = Arc::new(RecordingMetrics::default());

    // Start the probe and give it a past deadline (no worker runs it, so it
    // stays RUNNING); run the execution-timeout scanner directly for a
    // deterministic timeout — mirroring a wedged probe that never completes.
    let exec_id = start_workflow(&mut conn, CANARY_WF, "canary-timeout-1").await;
    expire_deadline_now(&mut conn, exec_id).await;

    let timed_out = timeout::enforce_workflow_execution_timeouts(&mut conn, recording.as_ref())
        .await
        .expect("timeout scanner runs");
    assert!(
        timed_out >= 1,
        "the backdated deadline must time out the probe"
    );
    assert_eq!(get_state(&mut conn, exec_id).await, "TIMED_OUT");

    // AC6: the probe's failure is reported on the canary channel.
    assert!(
        recording.canary_failure_count() >= 1,
        "a timed-out probe must emit harvest.canary.failure"
    );
    // AC8: NOT on the business terminal/timeout counters.
    assert_eq!(
        recording.terminal_count_for(CANARY_WF),
        0,
        "a timed-out canary must never increment harvest.workflow.terminal"
    );
    assert_eq!(
        recording.workflow_timeout_count(),
        0,
        "a timed-out canary must never increment harvest.workflow.timeout"
    );
}

/// A NORMAL (non-canary) workflow still increments `harvest.workflow.terminal`
/// (non-canary behavior is byte-identical).
#[tokio::test]
async fn normal_workflow_still_records_terminal() {
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);
    let recording = Arc::new(RecordingMetrics::default());

    let worker = make_worker(vec![normal_wf_info()], vec![], recording.clone());
    let (worker, handle) = spawn_worker(worker, pool);

    let exec_id = start_workflow(&mut conn, NORMAL_WF, "normal-1").await;
    wait_for_state(&mut conn, exec_id, "COMPLETED").await;

    worker.shutdown();
    let _ = handle.await;

    assert_eq!(
        recording.terminal_count_for(NORMAL_WF),
        1,
        "a normal workflow must increment harvest.workflow.terminal exactly once"
    );
    assert!(
        recording.canary_success_count() == 0 && recording.canary_failure_count() == 0,
        "a normal workflow must never touch the harvest.canary.* metrics"
    );
}
