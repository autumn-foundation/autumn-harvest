//! Metric-gated automatic build-ramp abort (issue #1814).
//!
//! Each test runs two real workers against Postgres. Worker A runs the base
//! build and completes every run. Worker B runs the ramp target. The tests
//! pick execution ids by ramp bucket, so the split between the builds is
//! exact and the tests are deterministic.
//!
//! A test uses `HARVEST_TEST_DATABASE_URL` when it is set and makes its own
//! database from it. Otherwise it starts a Postgres container.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::build_routing::{
    get_build_policy, ramp_bucket, set_build_policy, set_build_ramp,
};
use autumn_harvest::context::empty_shared_state;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::ramp_guard::{
    RampAbortReason, RampGuardConfig, abort_ramp, guard_once, run_ramp_guard,
};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::{
    BUILD_ID_LABEL_OTHER, MetricsRecorder, TelemetryConfig, WorkflowStatus,
};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, Priority, ShardId, StartWorkflowParams, WorkflowContext,
    start_or_load_workflow_execution,
};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;

const WF: &str = "ramp_guard_wf";
const QUEUE: &str = "default";
const BUILD_A: &str = "ramp-a";
const BUILD_B: &str = "ramp-b";
const RAMP_PERCENT: i32 = 10;
const RUNS_A: usize = 30;
const RUNS_B: usize = 10;

// ── Handlers ────────────────────────────────────────────────────────────────

type HandlerFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

fn ok_handler(_ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move { Ok(serde_json::json!("done")) })
}

fn failing_handler(_ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move { Err("build B is broken".to_owned()) })
}

fn wf_info(handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        name: WF,
        module: "ramp_guard_tests",
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
        mcp: false,
    }
}

// ── Metrics capture ─────────────────────────────────────────────────────────

/// Records the build-labelled terminal counter and the ramp-abort counter.
#[derive(Default)]
struct RecordingMetrics {
    terminal: Mutex<Vec<(String, WorkflowStatus)>>,
    aborted: Mutex<Vec<(String, String)>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_terminal_for_build(
        &self,
        _workflow_name: &str,
        _queue: &str,
        build_id: &str,
        outcome: WorkflowStatus,
    ) {
        self.terminal
            .lock()
            .unwrap()
            .push((build_id.to_owned(), outcome));
    }

    fn record_build_ramp_aborted(&self, queue: &str, reason: &str) {
        self.aborted
            .lock()
            .unwrap()
            .push((queue.to_owned(), reason.to_owned()));
    }
}

// ── Database helpers ────────────────────────────────────────────────────────

async fn setup() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (provision_ephemeral_db(&base_url).await, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// Make a new, migrated database for one test, so that tests do not share
/// build policies or executions.
async fn provision_ephemeral_db(base_url: &str) -> String {
    use diesel_async::SimpleAsyncConnection;

    let db_name = format!("harvest_ramp_guard_{}", uuid::Uuid::new_v4().simple());
    let mut admin = AsyncPgConnection::establish(base_url)
        .await
        .expect("connect to base database");
    diesel::sql_query(format!("CREATE DATABASE \"{db_name}\""))
        .execute(&mut admin)
        .await
        .expect("create ephemeral database");
    let (prefix, _) = base_url
        .rsplit_once('/')
        .expect("base url must end with /<database>");
    let url = format!("{prefix}/{db_name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to ephemeral database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations");
    url
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn auto_abort_audit_rows(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_audit_log \
         WHERE operation = 'build_routing.ramp.auto_abort' \
           AND target_type = 'build_routing' AND target_id = $1 \
           AND actor = 'system' AND route_or_command = 'background.ramp_guard'",
    )
    .bind::<Text, _>(QUEUE)
    .get_result::<CountRow>(conn)
    .await
    .expect("count audit rows")
    .n
}

async fn terminal_count(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND state IN ('COMPLETED', 'FAILED')",
    )
    .bind::<Text, _>(WF)
    .get_result::<CountRow>(conn)
    .await
    .expect("count terminal runs")
    .n
}

async fn wait_for_terminal(conn: &mut AsyncPgConnection, want: usize) {
    let want = i64::try_from(want).expect("small count");
    for _ in 0..1200 {
        if terminal_count(conn).await >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
    panic!("only {} of {want} runs ended", terminal_count(conn).await);
}

// ── Worker helpers ──────────────────────────────────────────────────────────

fn make_worker(build_id: &str, info: WorkflowInfo, metrics: Arc<RecordingMetrics>) -> Worker {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![info],
        vec![],
        empty_shared_state(),
        telemetry,
    ));
    Worker::new(
        WorkerRuntimeConfig {
            codec_rotation_batch_size: 0,
            dr: autumn_harvest::replication::DrConfig::default(),
            worker_id: uuid::Uuid::new_v4().to_string(),
            queues: vec![QUEUE.to_string()],
            queue_weights: std::collections::HashMap::new(),
            notification_database_url: None,
            shard_notification_database_urls: Vec::new(),
            max_concurrent_workflows: 10,
            max_concurrent_activities: 10,
            poll_interval: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(2),
            cancellation_grace_period: Duration::from_secs(2),
            sticky_timeout: Duration::ZERO,
            max_local_activity_start_to_close: Duration::from_secs(60),
            shard_assignments: vec![ShardId::new(0)],
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: build_id.to_string(),
            deployment_name: None,
            workflow_cache_size: 100,
            resident_workflows: true,
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

struct RunningWorkers {
    workers: Vec<Arc<Worker>>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl RunningWorkers {
    fn spawn(workers: Vec<Worker>, pool: &DbPool) -> Self {
        let mut running = Self {
            workers: Vec::new(),
            handles: Vec::new(),
        };
        for worker in workers {
            let worker = Arc::new(worker);
            let worker_ref = Arc::clone(&worker);
            let pool = pool.clone();
            running.handles.push(tokio::spawn(async move {
                let _ = tokio::time::timeout(Duration::from_secs(120), worker_ref.run(&pool)).await;
            }));
            running.workers.push(worker);
        }
        running
    }

    async fn stop(self) {
        for worker in &self.workers {
            worker.shutdown();
        }
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

/// Return an execution id whose ramp bucket sends it to the target build
/// when `to_target` is true, and to the base build otherwise.
fn exec_id_for(to_target: bool) -> ExecutionId {
    let percent = u8::try_from(RAMP_PERCENT).expect("small percent");
    loop {
        let id = ExecutionId::new_for_shard(ShardId::new(0));
        if (ramp_bucket(id) < percent) == to_target {
            return id;
        }
    }
}

async fn start_run(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    let workflow_id = format!("ramp-guard-{}", exec_id.as_uuid());
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: WF,
            workflow_id: &workflow_id,
            exec_id,
            input: Value::Null.into(),
            parent_id: None,
            queue_name: QUEUE,
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
    .expect("workflow start should succeed");
}

/// Set the base policy, ramp the target to 10 %, then start the runs.
async fn ramp_and_start(conn: &mut AsyncPgConnection) {
    set_build_policy(conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    set_build_ramp(conn, QUEUE, BUILD_B, RAMP_PERCENT)
        .await
        .expect("set ramp");
    for _ in 0..RUNS_A {
        start_run(conn, exec_id_for(false)).await;
    }
    for _ in 0..RUNS_B {
        start_run(conn, exec_id_for(true)).await;
    }
}

async fn assigned_builds(conn: &mut AsyncPgConnection) -> (i64, i64) {
    let rows: Vec<Option<String>> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(WF))
        .select(harvest_workflow_executions::assigned_build_id)
        .load(conn)
        .await
        .expect("load assigned builds");
    let a = rows
        .iter()
        .filter(|b| b.as_deref() == Some(BUILD_A))
        .count();
    let b = rows
        .iter()
        .filter(|b| b.as_deref() == Some(BUILD_B))
        .count();
    (
        i64::try_from(a).expect("small"),
        i64::try_from(b).expect("small"),
    )
}

fn guard_config() -> RampGuardConfig {
    RampGuardConfig::new()
        .with_interval(Duration::from_secs(1))
        .with_min_samples(5)
}

// ── Tests ───────────────────────────────────────────────────────────────────

/// RED test of issue #1814. Build B takes 10 % of new starts and fails every
/// run. The guard loop must abort the ramp with no operator action, write one
/// audit row, and count the abort.
#[tokio::test]
async fn ramp_aborts_automatically_when_target_build_fails_every_run() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let metrics = Arc::new(RecordingMetrics::default());

    ramp_and_start(&mut conn).await;
    assert_eq!(
        assigned_builds(&mut conn).await,
        (30, 10),
        "the ramp must send exactly the chosen runs to build B"
    );

    let workers = RunningWorkers::spawn(
        vec![
            make_worker(BUILD_A, wf_info(ok_handler), Arc::clone(&metrics)),
            make_worker(BUILD_B, wf_info(failing_handler), Arc::clone(&metrics)),
        ],
        &pool,
    );
    wait_for_terminal(&mut conn, RUNS_A + RUNS_B).await;
    workers.stop().await;

    // The guard runs as a background loop, as it does in a deployment.
    let cancel = CancellationToken::new();
    let guard = tokio::spawn(run_ramp_guard(
        vec![pool.clone()],
        pool.clone(),
        guard_config(),
        Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
        cancel.child_token(),
    ));
    let mut aborted = false;
    for _ in 0..100 {
        let policy = get_build_policy(&mut conn, QUEUE)
            .await
            .expect("read policy")
            .expect("policy exists");
        if policy.target_build_id.is_none() {
            aborted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancel.cancel();
    let _ = guard.await;
    assert!(aborted, "the guard must abort the ramp automatically");

    let policy = get_build_policy(&mut conn, QUEUE)
        .await
        .expect("read policy")
        .expect("policy exists");
    assert_eq!(policy.build_id, BUILD_A, "the base build stays");
    assert_eq!(policy.target_build_id, None);
    assert_eq!(policy.ramp_percent, None);
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 1);
    assert_eq!(
        metrics.aborted.lock().unwrap().as_slice(),
        &[(
            QUEUE.to_owned(),
            RampAbortReason::FailureRate.as_str().to_owned()
        )]
    );

    // The worker metrics carry the build of the worker that ran the task.
    // The worker metrics carry the build of the worker that ran the task.
    // The label cap is process-wide. A full local run of every suite in one
    // process can fill it first, so `__other__` is also accepted.
    let terminal = metrics.terminal.lock().unwrap().clone();
    let is = |label: &str, build: &str| label == build || label == BUILD_ID_LABEL_OTHER;
    let failed_b = terminal
        .iter()
        .filter(|(b, s)| is(b, BUILD_B) && *s == WorkflowStatus::Failed)
        .count();
    let completed_a = terminal
        .iter()
        .filter(|(b, s)| is(b, BUILD_A) && *s == WorkflowStatus::Completed)
        .count();
    assert_eq!((completed_a, failed_b), (RUNS_A, RUNS_B));

    // After the abort, a run that the ramp sent to B now goes to A.
    let before = assigned_builds(&mut conn).await;
    for _ in 0..3 {
        start_run(&mut conn, exec_id_for(true)).await;
    }
    assert_eq!(
        assigned_builds(&mut conn).await,
        (before.0 + 3, before.1),
        "the base build takes every new start"
    );

    // A second pass finds no ramp and writes nothing.
    let again = guard_once(&[pool.clone()], &pool, &guard_config(), None).await;
    assert!(again.is_empty(), "no ramp is left to abort: {again:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 1);
}

/// A healthy target build keeps its ramp.
#[tokio::test]
async fn healthy_ramp_is_not_aborted() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let metrics = Arc::new(RecordingMetrics::default());

    ramp_and_start(&mut conn).await;
    let workers = RunningWorkers::spawn(
        vec![
            make_worker(BUILD_A, wf_info(ok_handler), Arc::clone(&metrics)),
            make_worker(BUILD_B, wf_info(ok_handler), Arc::clone(&metrics)),
        ],
        &pool,
    );
    wait_for_terminal(&mut conn, RUNS_A + RUNS_B).await;
    workers.stop().await;

    let aborts = guard_once(&[pool.clone()], &pool, &guard_config(), None).await;
    assert!(aborts.is_empty(), "a healthy ramp must stay: {aborts:?}");
    let policy = get_build_policy(&mut conn, QUEUE)
        .await
        .expect("read policy")
        .expect("policy exists");
    assert_eq!(policy.target_build_id.as_deref(), Some(BUILD_B));
    assert_eq!(policy.ramp_percent, Some(RAMP_PERCENT));
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 0);
}

/// Too few target runs gives no verdict, so the ramp stays.
#[tokio::test]
async fn ramp_with_too_few_target_samples_is_not_aborted() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let metrics = Arc::new(RecordingMetrics::default());

    ramp_and_start(&mut conn).await;
    let workers = RunningWorkers::spawn(
        vec![
            make_worker(BUILD_A, wf_info(ok_handler), Arc::clone(&metrics)),
            make_worker(BUILD_B, wf_info(failing_handler), Arc::clone(&metrics)),
        ],
        &pool,
    );
    wait_for_terminal(&mut conn, RUNS_A + RUNS_B).await;
    workers.stop().await;

    let strict = guard_config().with_min_samples(1_000);
    let aborts = guard_once(&[pool.clone()], &pool, &strict, None).await;
    assert!(
        aborts.is_empty(),
        "no verdict below min_samples: {aborts:?}"
    );
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 0);
}

/// The abort is a compare-and-swap. When an operator moves the ramp to a new
/// target, a verdict about the old target does not clear it.
#[tokio::test]
async fn abort_does_not_clear_a_ramp_that_moved_to_another_target() {
    let (url, _container) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    set_build_policy(&mut conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    let ramp = set_build_ramp(&mut conn, QUEUE, "ramp-c", 5)
        .await
        .expect("set ramp");

    let cleared = abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, ramp.updated_at)
        .await
        .expect("abort_ramp");
    assert!(!cleared, "a ramp to another target must stay");
    let policy = get_build_policy(&mut conn, QUEUE)
        .await
        .expect("read policy")
        .expect("policy exists");
    assert_eq!(policy.target_build_id.as_deref(), Some("ramp-c"));
}

/// A verdict about an old step does not clear a new step of the same ramp.
#[tokio::test]
async fn abort_does_not_clear_a_new_step_of_the_same_ramp() {
    let (url, _container) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    set_build_policy(&mut conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    let old_step = set_build_ramp(&mut conn, QUEUE, BUILD_B, 10)
        .await
        .expect("set ramp")
        .updated_at;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let new_step = set_build_ramp(&mut conn, QUEUE, BUILD_B, 1)
        .await
        .expect("re-ramp")
        .updated_at;
    assert_ne!(old_step, new_step);

    assert!(
        !abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, old_step)
            .await
            .expect("abort_ramp"),
        "the old step must not clear the new one"
    );
    assert!(
        abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, new_step)
            .await
            .expect("abort_ramp"),
        "the current step clears"
    );
}

// ── Seeded tests: query semantics without workers ───────────────────────────

/// Start one run on the ramp and set its outcome columns directly.
async fn seed(
    conn: &mut AsyncPgConnection,
    to_target: bool,
    state: &str,
    nd_blocked: bool,
) -> ExecutionId {
    let exec_id = exec_id_for(to_target);
    start_run(conn, exec_id).await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
         SET state = $2, \
             nd_blocked_at = CASE WHEN $3 THEN NOW() ELSE NULL END \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(state)
    .bind::<diesel::sql_types::Bool, _>(nd_blocked)
    .execute(conn)
    .await
    .expect("seed outcome");
    exec_id
}

async fn set_ramp(conn: &mut AsyncPgConnection) {
    set_build_policy(conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    set_build_ramp(conn, QUEUE, BUILD_B, RAMP_PERCENT)
        .await
        .expect("set ramp");
}

async fn seed_healthy_base(conn: &mut AsyncPgConnection, n: usize) {
    for _ in 0..n {
        seed(conn, false, "COMPLETED", false).await;
    }
}

async fn ramp_is_active(conn: &mut AsyncPgConnection) -> bool {
    get_build_policy(conn, QUEUE)
        .await
        .expect("read policy")
        .expect("policy exists")
        .target_build_id
        .is_some()
}

/// Blocked runs abort the ramp on the ND-block rate. A blocked run that an
/// operator paused still counts.
#[tokio::test]
async fn nd_blocked_target_runs_abort_on_nd_block_rate() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    set_ramp(&mut conn).await;
    seed_healthy_base(&mut conn, 10).await;
    for _ in 0..3 {
        seed(&mut conn, true, "RUNNING", true).await;
        seed(&mut conn, true, "PAUSED", true).await;
    }

    let aborts = guard_once(&[pool.clone()], &pool, &guard_config(), None).await;
    assert_eq!(aborts.len(), 1, "{aborts:?}");
    assert_eq!(aborts[0].reason, RampAbortReason::NdBlockRate);
    assert_eq!(
        aborts[0].target.nd_blocked, 6,
        "RUNNING and PAUSED blocks count"
    );
    assert!(!ramp_is_active(&mut conn).await);

    let summary: Vec<Option<String>> = autumn_harvest::schema::harvest_audit_log::table
        .filter(
            autumn_harvest::schema::harvest_audit_log::operation
                .eq("build_routing.ramp.auto_abort"),
        )
        .select(autumn_harvest::schema::harvest_audit_log::error_summary)
        .load(&mut conn)
        .await
        .expect("load audit summary");
    let summary = summary[0].clone().expect("summary set");
    assert!(summary.contains("reason=nd_block_rate"), "{summary}");
    assert!(summary.contains("target_build=ramp-b"), "{summary}");
    assert!(summary.contains("target_started=6"), "{summary}");
}

/// Only runs of the current step count. Canary probes never count. A timed
/// out run is a failure.
#[tokio::test]
async fn only_current_step_non_canary_runs_count_and_timeouts_fail() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    set_ramp(&mut conn).await;
    seed_healthy_base(&mut conn, 10).await;

    // Five failed target runs from before the step.
    let mut old = Vec::new();
    for _ in 0..5 {
        old.push(seed(&mut conn, true, "FAILED", false).await);
    }
    for id in old {
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
             SET created_at = created_at - INTERVAL '1 day' WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("backdate");
    }
    // Five failed canary probes on the target build.
    for _ in 0..5 {
        let id = seed(&mut conn, true, "FAILED", false).await;
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
             SET workflow_name = '__harvest_canary_probe__default' WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("rename to canary");
    }
    assert!(
        guard_once(&[pool.clone()], &pool, &guard_config(), None)
            .await
            .is_empty(),
        "old-step and canary runs give no samples"
    );
    assert!(ramp_is_active(&mut conn).await);

    // Five timed-out runs in the step are failures.
    for _ in 0..5 {
        seed(&mut conn, true, "TIMED_OUT", false).await;
    }
    let aborts = guard_once(&[pool.clone()], &pool, &guard_config(), None).await;
    assert_eq!(aborts.len(), 1, "{aborts:?}");
    assert_eq!(aborts[0].reason, RampAbortReason::FailureRate);
    assert_eq!(
        aborts[0].target.failed, 5,
        "only the step's timed-out runs count"
    );
}

/// Two pools that both hold the ramp: the guard merges their counts, clears
/// both and audits once.
#[tokio::test]
async fn guard_merges_pools_clears_each_and_audits_once() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    for conn in [&mut conn_1, &mut conn_2] {
        set_ramp(conn).await;
        seed_healthy_base(conn, 5).await;
        // Three failed target runs per pool: below min_samples on each pool
        // alone, above it once merged.
        for _ in 0..3 {
            seed(conn, true, "FAILED", false).await;
        }
    }
    let config = guard_config().with_min_samples(6);
    let pools = [pool_1.clone(), pool_2.clone()];

    let aborts = guard_once(&pools, &pool_1, &config, None).await;
    assert_eq!(aborts.len(), 1, "{aborts:?}");
    assert_eq!(aborts[0].target.failed, 6, "the counts merge over pools");
    assert!(!aborts[0].incomplete);
    assert!(!ramp_is_active(&mut conn_1).await);
    assert!(!ramp_is_active(&mut conn_2).await);
    assert_eq!(auto_abort_audit_rows(&mut conn_1).await, 1);
    assert_eq!(auto_abort_audit_rows(&mut conn_2).await, 0);
}

/// With the base build promoted to the target, the ramp is not a ramp.
#[tokio::test]
async fn a_ramp_to_its_own_base_build_is_skipped() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    set_build_policy(&mut conn, QUEUE, BUILD_B, None)
        .await
        .expect("set base policy");
    set_build_ramp(&mut conn, QUEUE, BUILD_B, 100)
        .await
        .expect("set ramp");
    for _ in 0..10 {
        seed(&mut conn, true, "FAILED", false).await;
    }
    assert!(
        guard_once(&[pool.clone()], &pool, &guard_config(), None)
            .await
            .is_empty()
    );
    assert!(ramp_is_active(&mut conn).await);
}
