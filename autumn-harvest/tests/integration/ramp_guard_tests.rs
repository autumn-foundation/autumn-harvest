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
    clear_build_ramp, get_build_policy, ramp_bucket, ramp_generation_id, set_build_policy,
    set_build_policy_with_ramp_id, set_build_ramp, set_build_ramp_with_id,
};
use autumn_harvest::context::empty_shared_state;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::ramp_guard::{
    RampAbortReason, RampGuardConfig, abort_ramp, claim_unreported_abort, guard_once,
    mark_abort_reported, ramp_aborted_by_guard, record_abort_report, run_ramp_guard,
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
const BUILD_C: &str = "ramp-c";
const RAMP_PERCENT: i32 = 10;
const RUNS_A: usize = 30;
const RUNS_B: usize = 10;
const CLEAR_BOUND: Duration = Duration::from_secs(5);

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
    let again = guard_once(std::slice::from_ref(&pool), &pool, &guard_config(), None).await;
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

    let aborts = guard_once(std::slice::from_ref(&pool), &pool, &guard_config(), None).await;
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
    let aborts = guard_once(std::slice::from_ref(&pool), &pool, &strict, None).await;
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

    let cleared = abort_ramp(
        &mut conn,
        QUEUE,
        BUILD_A,
        BUILD_B,
        ramp.updated_at,
        CLEAR_BOUND,
    )
    .await
    .expect("abort_ramp");
    assert!(cleared.is_none(), "a ramp to another target must stay");
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
        abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, old_step, CLEAR_BOUND)
            .await
            .expect("abort_ramp")
            .is_none(),
        "the old step must not clear the new one"
    );
    assert!(
        abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, new_step, CLEAR_BOUND)
            .await
            .expect("abort_ramp")
            .is_some(),
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

    let aborts = guard_once(std::slice::from_ref(&pool), &pool, &guard_config(), None).await;
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
        guard_once(std::slice::from_ref(&pool), &pool, &guard_config(), None)
            .await
            .is_empty(),
        "old-step and canary runs give no samples"
    );
    assert!(ramp_is_active(&mut conn).await);

    // Five timed-out runs in the step are failures.
    for _ in 0..5 {
        seed(&mut conn, true, "TIMED_OUT", false).await;
    }
    let aborts = guard_once(std::slice::from_ref(&pool), &pool, &guard_config(), None).await;
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
    // The API fan-out writes one `ramp_id` to every pool.
    let ramp_id = uuid::Uuid::new_v4();
    for conn in [&mut conn_1, &mut conn_2] {
        set_ramp_with_id(conn, ramp_id).await;
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

/// Two pools hold ramps with the same builds but different `ramp_id`s. The
/// guard judges each ramp on its own counts, so failures of one ramp cannot
/// abort the other.
#[tokio::test]
async fn ramps_with_different_ids_are_judged_apart() {
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
        set_ramp_with_id(conn, uuid::Uuid::new_v4()).await;
        seed_healthy_base(conn, 5).await;
        // Three failed target runs per ramp: below min_samples for each one.
        for _ in 0..3 {
            seed(conn, true, "FAILED", false).await;
        }
    }
    let config = guard_config().with_min_samples(6);
    let pools = [pool_1.clone(), pool_2.clone()];

    let aborts = guard_once(&pools, &pool_1, &config, None).await;
    assert!(aborts.is_empty(), "the counts do not merge: {aborts:?}");
    assert!(ramp_is_active(&mut conn_1).await);
    assert!(ramp_is_active(&mut conn_2).await);
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
    let aborts = guard_once(std::slice::from_ref(&pool), &pool, &guard_config(), None).await;
    assert!(aborts.is_empty(), "a promotion is not judged: {aborts:?}");
    assert!(ramp_is_active(&mut conn).await);
}

/// The server bounds a clear. A clear that waits on a row lock fails on the
/// server and rolls back, so it cannot commit after the guard gave up.
#[tokio::test]
async fn a_blocked_clear_fails_on_the_server_and_changes_nothing() {
    use diesel_async::AsyncConnection as _;

    let (url, _container) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let mut locker = AsyncPgConnection::establish(&url)
        .await
        .expect("connect locker");
    set_build_policy(&mut conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    let step = set_build_ramp(&mut conn, QUEUE, BUILD_B, RAMP_PERCENT)
        .await
        .expect("set ramp")
        .updated_at;

    // Hold the policy row lock for longer than the clear bound.
    diesel::sql_query("BEGIN")
        .execute(&mut locker)
        .await
        .expect("begin");
    diesel::sql_query("SELECT 1 FROM harvest_build_policies WHERE queue_name = $1 FOR UPDATE")
        .bind::<Text, _>(QUEUE)
        .execute(&mut locker)
        .await
        .expect("lock policy row");

    let started = std::time::Instant::now();
    let result = abort_ramp(
        &mut conn,
        QUEUE,
        BUILD_A,
        BUILD_B,
        step,
        Duration::from_millis(300),
    )
    .await;
    assert!(
        result.is_err(),
        "the server stops a blocked clear: {result:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the server bound applies, not a client wait"
    );

    diesel::sql_query("ROLLBACK")
        .execute(&mut locker)
        .await
        .expect("rollback");
    assert!(
        ramp_is_active(&mut conn).await,
        "the failed clear rolled back"
    );
}

/// Set the test ramp on one pool with a given `ramp_id`, as the API fan-out
/// does on every shard.
/// The `ramp_id` that [`set_ramp_with_id`] stores for a caller id.
fn stored(ramp_id: uuid::Uuid) -> uuid::Uuid {
    ramp_generation_id(ramp_id, BUILD_A, BUILD_B)
}

async fn set_ramp_with_id(conn: &mut AsyncPgConnection, ramp_id: uuid::Uuid) {
    set_build_policy(conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    set_build_ramp_with_id(conn, QUEUE, BUILD_B, RAMP_PERCENT, ramp_id)
        .await
        .expect("set ramp");
}

async fn policy_step(conn: &mut AsyncPgConnection) -> chrono::DateTime<chrono::Utc> {
    get_build_policy(conn, QUEUE)
        .await
        .expect("read policy")
        .expect("policy exists")
        .updated_at
}

/// After a restart, a ramp that one pool still holds is cleared when another
/// pool holds the abort marker for the same `ramp_id`. The marker commits
/// with the clear, so this works with no audit row at all.
#[tokio::test]
async fn a_restarted_guard_finishes_a_marked_partial_abort() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, ramp_id).await;
    set_ramp_with_id(&mut conn_2, ramp_id).await;

    // The old guard cleared pool 1, then stopped before its audit write.
    let step_1 = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step_1, CLEAR_BOUND)
            .await
            .expect("clear pool 1")
            .is_some()
    );
    assert_eq!(auto_abort_audit_rows(&mut conn_1).await, 0, "no audit row");

    // Pool 2 alone has no runs, so a verdict is impossible. A new guard
    // still finishes the clear from the marker.
    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(
        aborts.is_empty(),
        "a finished clear reports nothing: {aborts:?}"
    );
    assert!(!ramp_is_active(&mut conn_2).await, "pool 2 is cleared");
    assert_eq!(
        auto_abort_audit_rows(&mut conn_1).await,
        0,
        "no new audit row"
    );
}

/// An operator ramp set after a guard abort has a new `ramp_id`, so the old
/// marker does not clear it. The match uses ids, so no clock is involved.
#[tokio::test]
async fn an_abort_marker_for_another_ramp_id_does_not_clear_a_ramp() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    set_ramp_with_id(&mut conn_1, uuid::Uuid::new_v4()).await;
    let step_1 = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step_1, CLEAR_BOUND)
            .await
            .expect("clear pool 1")
            .is_some()
    );
    // The operator ramps the same target again, but only pool 2 takes it.
    set_ramp_with_id(&mut conn_2, uuid::Uuid::new_v4()).await;

    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        ramp_is_active(&mut conn_2).await,
        "the newer operator ramp stays"
    );
}

/// A pool can hold the marker of an old ramp and a newer operator ramp with
/// the same builds. The guard finishes the old ramp on the other pool. It
/// keeps the newer ramp, because its `ramp_id` matches no marker.
#[tokio::test]
async fn a_marker_beside_a_newer_ramp_finishes_only_the_marked_ramp() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let old_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, old_id).await;
    set_ramp_with_id(&mut conn_2, old_id).await;

    // The old guard cleared pool 1 only. Pool 1 now holds the marker.
    let step_1 = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step_1, CLEAR_BOUND)
            .await
            .expect("clear pool 1")
            .is_some()
    );
    // A new operator ramp with the same builds reaches pool 1 only.
    let new_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, new_id).await;

    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        !ramp_is_active(&mut conn_2).await,
        "the marked old ramp on pool 2 is cleared"
    );
    assert!(
        ramp_is_active(&mut conn_1).await,
        "the newer ramp on pool 1 stays"
    );
    assert_eq!(auto_abort_audit_rows(&mut conn_1).await, 0, "no audit row");
}

/// A base-build change keeps the `ramp_id`, but it starts a new step. An old
/// marker for the old base therefore does not finish the ramp.
#[tokio::test]
async fn a_marker_does_not_finish_a_ramp_whose_base_changed() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, ramp_id).await;
    set_ramp_with_id(&mut conn_2, ramp_id).await;
    let step_1 = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step_1, CLEAR_BOUND)
            .await
            .expect("clear pool 1")
            .is_some()
    );
    // The operator moves pool 2 to a new base build. The ramp stays.
    set_build_policy(&mut conn_2, QUEUE, BUILD_C, None)
        .await
        .expect("set new base");
    assert!(ramp_is_active(&mut conn_2).await);

    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        ramp_is_active(&mut conn_2).await,
        "the ramp from the new base stays"
    );
}

/// A lost clear tells a guard clear from an operator change by the abort
/// marker. Only a guard clear sets the marker of the ramp's `ramp_id`.
#[tokio::test]
async fn the_marker_tells_a_guard_clear_from_an_operator_clear() {
    let (url, _c) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    // An operator clears the ramp: no guard marker.
    let operator_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn, operator_id).await;
    clear_build_ramp(&mut conn, QUEUE).await.expect("clear");
    assert!(
        !ramp_aborted_by_guard(&mut conn, QUEUE, stored(operator_id))
            .await
            .expect("read marker")
    );

    // A guard clears the next ramp: the marker holds its id.
    let guard_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn, guard_id).await;
    let step = policy_step(&mut conn).await;
    assert!(
        abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("guard clear")
            .is_some()
    );
    assert!(
        ramp_aborted_by_guard(&mut conn, QUEUE, stored(guard_id))
            .await
            .expect("read marker")
    );
    assert!(
        !ramp_aborted_by_guard(&mut conn, QUEUE, stored(operator_id))
            .await
            .expect("read marker")
    );
}

/// A guard abort on a pool keeps the older markers of that pool. An old
/// partial abort can therefore still finish after a newer abort there.
#[tokio::test]
async fn a_newer_abort_keeps_an_older_marker() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let old_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, old_id).await;
    set_ramp_with_id(&mut conn_2, old_id).await;
    // The old ramp clears on pool 1 only.
    let step = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear old ramp")
            .is_some()
    );
    // A newer ramp on pool 1 is aborted there too.
    set_ramp_with_id(&mut conn_1, uuid::Uuid::new_v4()).await;
    let step = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear new ramp")
            .is_some()
    );
    assert!(
        ramp_aborted_by_guard(&mut conn_1, QUEUE, stored(old_id))
            .await
            .expect("read marker"),
        "the old marker stays"
    );

    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        !ramp_is_active(&mut conn_2).await,
        "the old ramp on pool 2 is finished"
    );
}

/// The number of abort markers that one pool holds for the test queue.
async fn abort_marker_count(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query(
        "SELECT jsonb_array_length(ramp_aborted)::bigint AS n \
         FROM harvest_build_policies WHERE queue_name = $1",
    )
    .bind::<Text, _>(QUEUE)
    .get_result::<Row>(conn)
    .await
    .expect("count markers")
    .n
}

/// A marker stays while any pool holds its ramp, however many newer aborts
/// the pool takes. Once no pool holds the ramp, a pass removes the marker.
#[tokio::test]
async fn a_marker_stays_until_its_abort_finishes_and_is_then_pruned() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let old_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, old_id).await;
    set_ramp_with_id(&mut conn_2, old_id).await;
    let step = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear old ramp")
            .is_some()
    );
    // The guard that cleared each ramp also reported it.
    mark_abort_reported(&mut conn_1, QUEUE, stored(old_id), CLEAR_BOUND)
        .await
        .expect("mark old ramp");
    // Many newer ramps on pool 1 are aborted there too.
    for _ in 0..12 {
        let new_id = uuid::Uuid::new_v4();
        set_ramp_with_id(&mut conn_1, new_id).await;
        let step = policy_step(&mut conn_1).await;
        assert!(
            abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
                .await
                .expect("clear newer ramp")
                .is_some()
        );
        mark_abort_reported(&mut conn_1, QUEUE, stored(new_id), CLEAR_BOUND)
            .await
            .expect("mark newer ramp");
    }
    assert_eq!(
        abort_marker_count(&mut conn_1).await,
        13,
        "no marker is evicted"
    );

    // The pass finishes the old ramp on pool 2.
    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        !ramp_is_active(&mut conn_2).await,
        "the old ramp on pool 2 is finished"
    );

    // The finishing clear wrote an unreported marker on pool 2. The abort was
    // reported on pool 1, so the next pass only marks pool 2.
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert_eq!(
        marker_reported(&mut conn_2, stored(old_id)).await,
        Some(true)
    );
    assert_eq!(auto_abort_audit_rows(&mut conn_1).await, 0, "no new report");

    // Every marker is reported and no pool holds a marked ramp. After the
    // report grace, here zero, the next pass prunes every marker.
    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(&pools, &pool_1, &config, None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert_eq!(
        abort_marker_count(&mut conn_1).await,
        0,
        "finished markers go"
    );
    assert_eq!(abort_marker_count(&mut conn_2).await, 0);
}

/// A guard can stop after its clear commits and before its report. A later
/// pass reports that abort once, from the marker, after the report grace.
#[tokio::test]
async fn an_unreported_abort_is_reported_once_from_its_marker() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    set_ramp_with_id(&mut conn, uuid::Uuid::new_v4()).await;
    // The old guard cleared the ramp, then stopped before its report.
    let step = policy_step(&mut conn).await;
    assert!(
        abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear")
            .is_some()
    );
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 0);

    // Within the default grace, a pass reports nothing and keeps the marker.
    let pools = [pool.clone()];
    let aborts = guard_once(&pools, &pool, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert_eq!(abort_marker_count(&mut conn).await, 1);

    // After the grace, a pass reports the abort from the marker.
    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(&pools, &pool, &config, None).await;
    assert_eq!(aborts.len(), 1, "{aborts:?}");
    assert_eq!(aborts[0].reason, RampAbortReason::Unreported);
    assert_eq!(aborts[0].target_build_id, BUILD_B);
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 1);

    // The next pass reports nothing more and prunes the marker.
    let aborts = guard_once(&pools, &pool, &config, None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 1, "reported once");
    assert_eq!(abort_marker_count(&mut conn).await, 0);
}

/// The `reported` flag of the abort marker of `ramp_id` on one pool.
async fn marker_reported(conn: &mut AsyncPgConnection, ramp_id: uuid::Uuid) -> Option<bool> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        reported: bool,
    }
    diesel::sql_query(
        "SELECT (entry->>'reported')::boolean AS reported \
         FROM harvest_build_policies, jsonb_array_elements(ramp_aborted) AS m(entry) \
         WHERE queue_name = $1 AND entry->>'id' = $2",
    )
    .bind::<Text, _>(QUEUE)
    .bind::<Text, _>(ramp_id.to_string())
    .get_result::<Row>(conn)
    .await
    .optional()
    .expect("read marker")
    .map(|row| row.reported)
}

/// The `ramp_id` of the test queue on one pool.
async fn policy_ramp_id(conn: &mut AsyncPgConnection) -> Option<uuid::Uuid> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
        ramp_id: Option<uuid::Uuid>,
    }
    diesel::sql_query("SELECT ramp_id FROM harvest_build_policies WHERE queue_name = $1")
        .bind::<Text, _>(QUEUE)
        .get_result::<Row>(conn)
        .await
        .expect("read ramp_id")
        .ramp_id
}

/// Build ids are free text, so a `/` in one cannot shift the split between
/// base and target. Two pairs with the same joined text get different ids.
#[test]
fn a_slash_in_a_build_id_cannot_collide_two_generations() {
    let id = uuid::Uuid::new_v4();
    assert_ne!(
        ramp_generation_id(id, "a/b", "c"),
        ramp_generation_id(id, "a", "b/c")
    );
}

/// The SQL writers store the same id as [`ramp_generation_id`], also for
/// build ids with a `/` or multi-byte text.
#[tokio::test]
async fn the_stored_ramp_id_matches_the_rust_derivation() {
    let (url, _c) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let (base, target) = ("base/ü", "target/ç");
    let ramp_id = uuid::Uuid::new_v4();
    set_build_policy(&mut conn, QUEUE, base, None)
        .await
        .expect("set base policy");
    set_build_ramp_with_id(&mut conn, QUEUE, target, RAMP_PERCENT, ramp_id)
        .await
        .expect("set ramp");
    assert_eq!(
        policy_ramp_id(&mut conn).await,
        Some(ramp_generation_id(ramp_id, base, target)),
        "ramp write"
    );

    let new_base = "base/ä";
    set_build_policy_with_ramp_id(&mut conn, QUEUE, new_base, None, ramp_id)
        .await
        .expect("set policy");
    assert_eq!(
        policy_ramp_id(&mut conn).await,
        Some(ramp_generation_id(ramp_id, new_base, target)),
        "policy write"
    );
}

/// A writer from before the `ramp_id` column changes a ramp but keeps the
/// old `ramp_id`. The database clears that id, so an old abort marker cannot
/// match the new ramp.
#[tokio::test]
async fn a_ramp_change_without_a_new_ramp_id_drops_the_id() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let old_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, old_id).await;
    set_ramp_with_id(&mut conn_2, old_id).await;
    let step = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear pool 1")
            .is_some()
    );
    // An old API replica sets a new ramp on pool 2 with its old UPDATE.
    diesel::sql_query(
        "UPDATE harvest_build_policies \
         SET target_build_id = $2, ramp_percent = $3, updated_at = NOW() \
         WHERE queue_name = $1",
    )
    .bind::<Text, _>(QUEUE)
    .bind::<Text, _>(BUILD_B)
    .bind::<diesel::sql_types::Integer, _>(RAMP_PERCENT + 10)
    .execute(&mut conn_2)
    .await
    .expect("old writer ramp");
    assert_eq!(policy_ramp_id(&mut conn_2).await, None, "the old id goes");

    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        ramp_is_active(&mut conn_2).await,
        "the old marker does not clear the new ramp"
    );
}

/// The guard marks an abort as reported only after its audit row commits.
/// A failed audit write keeps the marker unreported, so a later pass can
/// report the abort.
#[tokio::test]
async fn a_failed_audit_write_keeps_the_marker_unreported() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn, ramp_id).await;
    seed_healthy_base(&mut conn, 5).await;
    for _ in 0..5 {
        seed(&mut conn, true, "FAILED", false).await;
    }
    // No server listens on port 1, so every audit write fails.
    let audit_pool = build_pool("postgres://postgres:postgres@127.0.0.1:1/none");

    let aborts = guard_once(
        std::slice::from_ref(&pool),
        &audit_pool,
        &guard_config(),
        None,
    )
    .await;
    assert!(
        aborts.is_empty(),
        "a failed report is not returned: {aborts:?}"
    );
    assert!(!ramp_is_active(&mut conn).await, "the clear still happens");
    assert_eq!(
        marker_reported(&mut conn, stored(ramp_id)).await,
        Some(false),
        "no audit row, so the marker stays unreported"
    );
}

/// A recovery claim is a lease. A guard can claim an unreported abort and
/// then stop. After the lease, another pass claims it again and reports it
/// once.
#[tokio::test]
async fn a_recovery_claim_that_did_not_report_is_retried_after_its_lease() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn, ramp_id).await;
    let step = policy_step(&mut conn).await;
    assert!(
        abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear")
            .is_some()
    );
    let grace = Duration::from_secs(1);
    tokio::time::sleep(grace + Duration::from_millis(200)).await;
    // A guard claims the recovery, then stops before its report.
    assert!(
        claim_unreported_abort(&mut conn, QUEUE, stored(ramp_id), grace, CLEAR_BOUND)
            .await
            .expect("claim")
    );
    assert_eq!(
        marker_reported(&mut conn, stored(ramp_id)).await,
        Some(false)
    );

    // The lease is fresh, so a pass does not report.
    let config = guard_config().with_report_grace(grace);
    let pools = [pool.clone()];
    let aborts = guard_once(&pools, &pool, &config, None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 0);

    // After the lease, a pass claims it again and reports it once. With one
    // pool and a bound of 1 s, the lease is 5 s.
    tokio::time::sleep(Duration::from_secs(5) + Duration::from_millis(300)).await;
    let aborts = guard_once(&pools, &pool, &config, None).await;
    assert_eq!(aborts.len(), 1, "{aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 1);
    assert_eq!(
        marker_reported(&mut conn, stored(ramp_id)).await,
        Some(true)
    );
}

/// A base-build change keeps an active ramp. The fan-out gives the ramp one
/// new `ramp_id` on every pool. A repeated write on one pool, from a retry
/// or from two shards on one pool, keeps that id. So the ramp keeps one
/// identity across pools, and no old marker matches it.
#[tokio::test]
async fn a_base_change_fan_out_gives_the_ramp_one_new_id() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let old_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, old_id).await;
    set_ramp_with_id(&mut conn_2, old_id).await;
    let new_id = uuid::Uuid::new_v4();
    // Pool 1 takes the write twice, as from two shards on one pool.
    for conn in [&mut conn_1, &mut conn_2] {
        set_build_policy_with_ramp_id(conn, QUEUE, BUILD_C, None, new_id)
            .await
            .expect("set new base");
        assert!(ramp_is_active(conn).await, "the ramp stays");
    }
    set_build_policy_with_ramp_id(&mut conn_1, QUEUE, BUILD_C, None, new_id)
        .await
        .expect("repeat on pool 1");
    let want = ramp_generation_id(new_id, BUILD_C, BUILD_B);
    assert_eq!(policy_ramp_id(&mut conn_1).await, Some(want));
    assert_eq!(policy_ramp_id(&mut conn_2).await, Some(want));
    assert_ne!(want, old_id);
}

/// A partial fan-out can leave two pools with different targets. A later
/// policy update keeps both ramps. Each target must keep its own `ramp_id`,
/// so an abort of one target cannot finish the other.
#[tokio::test]
async fn a_policy_update_keeps_diverged_targets_apart() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    // Pool 1 ramps to build B. Pool 2 ramps to build C.
    set_ramp_with_id(&mut conn_1, uuid::Uuid::new_v4()).await;
    set_build_policy(&mut conn_2, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    set_build_ramp_with_id(
        &mut conn_2,
        QUEUE,
        BUILD_C,
        RAMP_PERCENT,
        uuid::Uuid::new_v4(),
    )
    .await
    .expect("set ramp");

    // One policy update reaches both pools with one caller id.
    let new_id = uuid::Uuid::new_v4();
    for conn in [&mut conn_1, &mut conn_2] {
        set_build_policy_with_ramp_id(conn, QUEUE, BUILD_A, Some("deploy"), new_id)
            .await
            .expect("policy update");
    }
    let id_1 = policy_ramp_id(&mut conn_1).await;
    let id_2 = policy_ramp_id(&mut conn_2).await;
    assert_eq!(id_1, Some(ramp_generation_id(new_id, BUILD_A, BUILD_B)));
    assert_eq!(id_2, Some(ramp_generation_id(new_id, BUILD_A, BUILD_C)));
    assert_ne!(id_1, id_2, "each target keeps its own id");

    // The guard aborts the ramp to B on pool 1. The ramp to C stays.
    let step_1 = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step_1, CLEAR_BOUND)
            .await
            .expect("clear pool 1")
            .is_some()
    );
    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        ramp_is_active(&mut conn_2).await,
        "the ramp to C is not finished by the marker of B"
    );
}

/// A partial policy fan-out can leave pools with different base builds. A
/// later ramp fan-out then reaches both pools with one caller id. The guard
/// judges the two ramps as two generations, so each abort must report on
/// its own.
#[tokio::test]
async fn a_ramp_fan_out_keeps_diverged_bases_apart() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    // Pool 1 has base build A. Pool 2 has base build C.
    set_build_policy(&mut conn_1, QUEUE, BUILD_A, None)
        .await
        .expect("set base A");
    set_build_policy(&mut conn_2, QUEUE, BUILD_C, None)
        .await
        .expect("set base C");
    let caller_id = uuid::Uuid::new_v4();
    for conn in [&mut conn_1, &mut conn_2] {
        set_build_ramp_with_id(conn, QUEUE, BUILD_B, RAMP_PERCENT, caller_id)
            .await
            .expect("ramp fan-out");
    }
    assert_ne!(
        policy_ramp_id(&mut conn_1).await,
        policy_ramp_id(&mut conn_2).await,
        "each base keeps its own id"
    );

    // Both ramps are cleared with no report, as by a guard that stopped.
    for (conn, base) in [(&mut conn_1, BUILD_A), (&mut conn_2, BUILD_C)] {
        let step = policy_step(conn).await;
        assert!(
            abort_ramp(conn, QUEUE, base, BUILD_B, step, CLEAR_BOUND)
                .await
                .expect("clear")
                .is_some()
        );
    }
    let pools = [pool_1.clone(), pool_2.clone()];
    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(&pools, &pool_1, &config, None).await;
    assert_eq!(aborts.len(), 2, "each abort reports on its own: {aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn_1).await, 2);
}

/// `abort_ramp` returns the marker id of its clear. A ramp with no
/// `ramp_id` gets a fresh report id, so the caller can only report and mark
/// that abort with the returned id.
#[tokio::test]
async fn abort_ramp_returns_the_marker_id_of_an_id_less_ramp() {
    let (url, _c) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    set_build_policy(&mut conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    // An old writer sets the ramp with no id.
    diesel::sql_query(
        "UPDATE harvest_build_policies \
         SET target_build_id = $2, ramp_percent = $3, updated_at = NOW() \
         WHERE queue_name = $1",
    )
    .bind::<Text, _>(QUEUE)
    .bind::<Text, _>(BUILD_B)
    .bind::<diesel::sql_types::Integer, _>(RAMP_PERCENT)
    .execute(&mut conn)
    .await
    .expect("old writer ramp");
    assert_eq!(policy_ramp_id(&mut conn).await, None);

    let step = policy_step(&mut conn).await;
    let id = abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
        .await
        .expect("clear")
        .expect("the clear returns its marker id");
    assert_eq!(marker_reported(&mut conn, id).await, Some(false));
    assert!(
        mark_abort_reported(&mut conn, QUEUE, id, CLEAR_BOUND)
            .await
            .expect("mark")
    );
    assert_eq!(marker_reported(&mut conn, id).await, Some(true));
}

/// A recovery claim stays leased while its guard reports, even with a zero
/// report grace. Another pass therefore does not report the abort again.
#[tokio::test]
async fn a_zero_grace_still_leases_a_recovery_claim() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn, ramp_id).await;
    let step = policy_step(&mut conn).await;
    assert!(
        abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear")
            .is_some()
    );
    // Another guard has just claimed the recovery and is still reporting.
    assert!(
        claim_unreported_abort(
            &mut conn,
            QUEUE,
            stored(ramp_id),
            Duration::ZERO,
            CLEAR_BOUND
        )
        .await
        .expect("claim")
    );

    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(std::slice::from_ref(&pool), &pool, &config, None).await;
    assert!(aborts.is_empty(), "the fresh claim holds: {aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 0);
}

/// A repeated ramp write with the same `ramp_id`, from a retry or from two
/// shards on one pool, keeps the id. It does not start a new step either.
#[tokio::test]
async fn a_repeated_ramp_write_keeps_its_ramp_id() {
    let (url, _c) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn, ramp_id).await;
    let step = policy_step(&mut conn).await;
    set_build_ramp_with_id(&mut conn, QUEUE, BUILD_B, RAMP_PERCENT, ramp_id)
        .await
        .expect("repeat the ramp write");
    assert_eq!(policy_ramp_id(&mut conn).await, Some(stored(ramp_id)));
    assert_eq!(policy_step(&mut conn).await, step, "the step stays");
}

/// A policy update gives an id-less ramp the caller's `ramp_id`. Such a ramp
/// comes from a writer from before the migration. With an id, a later guard
/// can finish a partial abort of it after a restart.
#[tokio::test]
async fn a_policy_update_gives_an_id_less_ramp_an_id() {
    let (url, _c) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    set_build_policy(&mut conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    // An old writer sets the ramp with no id.
    diesel::sql_query(
        "UPDATE harvest_build_policies \
         SET target_build_id = $2, ramp_percent = $3, updated_at = NOW() \
         WHERE queue_name = $1",
    )
    .bind::<Text, _>(QUEUE)
    .bind::<Text, _>(BUILD_B)
    .bind::<diesel::sql_types::Integer, _>(RAMP_PERCENT)
    .execute(&mut conn)
    .await
    .expect("old writer ramp");
    assert_eq!(policy_ramp_id(&mut conn).await, None);

    let ramp_id = uuid::Uuid::new_v4();
    set_build_policy_with_ramp_id(&mut conn, QUEUE, BUILD_C, None, ramp_id)
        .await
        .expect("set new base");
    assert!(ramp_is_active(&mut conn).await);
    assert_eq!(
        policy_ramp_id(&mut conn).await,
        Some(ramp_generation_id(ramp_id, BUILD_C, BUILD_B))
    );
}

/// Clear the test ramp on both pools with unreported markers for one id, as
/// a guard does that stopped before its report.
async fn clear_on_both_pools_unreported(
    conn_1: &mut AsyncPgConnection,
    conn_2: &mut AsyncPgConnection,
) -> uuid::Uuid {
    let ramp_id = uuid::Uuid::new_v4();
    for conn in [conn_1, conn_2] {
        set_ramp_with_id(conn, ramp_id).await;
        let step = policy_step(conn).await;
        assert!(
            abort_ramp(conn, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
                .await
                .expect("clear")
                .is_some()
        );
    }
    stored(ramp_id)
}

/// A fresh recovery claim on one pool stops recovery of that abort on every
/// pool. Another guard therefore cannot report it through the second pool.
#[tokio::test]
async fn a_fresh_claim_on_one_pool_stops_recovery_on_every_pool() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let ramp_id = clear_on_both_pools_unreported(&mut conn_1, &mut conn_2).await;
    // Another guard has claimed the recovery on pool 1 and still reports.
    assert!(
        claim_unreported_abort(&mut conn_1, QUEUE, ramp_id, Duration::ZERO, CLEAR_BOUND)
            .await
            .expect("claim")
    );

    let pools = [pool_1.clone(), pool_2.clone()];
    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(&pools, &pool_1, &config, None).await;
    assert!(aborts.is_empty(), "the claim holds fleet-wide: {aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn_1).await, 0);
    assert_eq!(marker_reported(&mut conn_2, ramp_id).await, Some(false));
}

/// A recovery claim that fails on one pool moves to the next marker pool.
/// Pool 1 stays readable, but a row lock blocks the claim there. The abort is
/// still reported once, through pool 2.
#[tokio::test]
async fn a_failed_recovery_claim_tries_the_next_marker_pool() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let mut locker = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect locker");
    let ramp_id = clear_on_both_pools_unreported(&mut conn_1, &mut conn_2).await;
    diesel::sql_query("BEGIN")
        .execute(&mut locker)
        .await
        .expect("begin");
    diesel::sql_query("SELECT 1 FROM harvest_build_policies WHERE queue_name = $1 FOR UPDATE")
        .bind::<Text, _>(QUEUE)
        .execute(&mut locker)
        .await
        .expect("lock pool 1 policy row");

    let pools = [pool_1.clone(), pool_2.clone()];
    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(&pools, &pool_2, &config, None).await;
    diesel::sql_query("ROLLBACK")
        .execute(&mut locker)
        .await
        .expect("rollback");
    assert_eq!(aborts.len(), 1, "pool 2 takes the claim: {aborts:?}");
    assert_eq!(aborts[0].reason, RampAbortReason::Unreported);
    assert_eq!(auto_abort_audit_rows(&mut conn_2).await, 1);
    assert_eq!(marker_reported(&mut conn_2, ramp_id).await, Some(true));
}

/// A reported marker on one pool means that the abort was reported. A pass
/// marks the rest as reported and does not report the abort again.
#[tokio::test]
async fn a_partly_marked_abort_is_not_reported_again() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let ramp_id = clear_on_both_pools_unreported(&mut conn_1, &mut conn_2).await;
    // The reporting guard marked pool 1, then stopped before pool 2.
    assert!(
        mark_abort_reported(&mut conn_1, QUEUE, ramp_id, CLEAR_BOUND)
            .await
            .expect("mark pool 1")
    );

    let pools = [pool_1.clone(), pool_2.clone()];
    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(&pools, &pool_1, &config, None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn_1).await, 0);
    assert_eq!(marker_reported(&mut conn_2, ramp_id).await, Some(true));
}

/// The report ledger records each abort once. A second record of the same
/// `ramp_id` changes nothing.
#[tokio::test]
async fn the_report_ledger_records_an_abort_once() {
    let (url, _c) = setup().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let ramp_id = uuid::Uuid::new_v4();
    assert!(
        record_abort_report(&mut conn, QUEUE, ramp_id)
            .await
            .expect("first record")
    );
    assert!(
        !record_abort_report(&mut conn, QUEUE, ramp_id)
            .await
            .expect("second record")
    );
}

/// A guard that wants to report an abort that the ledger already holds
/// writes no audit row. It only marks the markers as reported. So two
/// replicas that both think they won an abort report it once.
#[tokio::test]
async fn an_abort_in_the_report_ledger_is_not_reported_again() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn, ramp_id).await;
    let step = policy_step(&mut conn).await;
    assert!(
        abort_ramp(&mut conn, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear")
            .is_some()
    );
    // Another replica reported the abort, then stopped before its mark.
    assert!(
        record_abort_report(&mut conn, QUEUE, stored(ramp_id))
            .await
            .expect("record")
    );

    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(std::slice::from_ref(&pool), &pool, &config, None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 0);
    assert_eq!(
        marker_reported(&mut conn, stored(ramp_id)).await,
        Some(true)
    );
}

/// A guard that finishes a partial abort keeps the abort unreported, so a
/// later pass still reports an abort whose first guard stopped early.
#[tokio::test]
async fn finishing_an_unreported_abort_keeps_it_unreported() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, ramp_id).await;
    set_ramp_with_id(&mut conn_2, ramp_id).await;
    // The first guard cleared pool 1 and stopped before its report.
    let step = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear pool 1")
            .is_some()
    );

    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(!ramp_is_active(&mut conn_2).await, "pool 2 is finished");
    assert_eq!(
        marker_reported(&mut conn_2, stored(ramp_id)).await,
        Some(false)
    );

    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(&pools, &pool_1, &config, None).await;
    assert_eq!(aborts.len(), 1, "the abort is still reported: {aborts:?}");
    assert_eq!(auto_abort_audit_rows(&mut conn_1).await, 1);
}

/// A reported marker stays for the report grace before a pass prunes it. In
/// that window, a fan-out still in flight can write the same `ramp_id` to a
/// later pool. The marker then still finishes that ramp.
#[tokio::test]
async fn a_reported_marker_outlives_a_late_fan_out_write() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    set_build_policy(&mut conn_2, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy 2");
    // The fan-out reached pool 1, and the guard aborted and reported it.
    let ramp_id = uuid::Uuid::new_v4();
    set_ramp_with_id(&mut conn_1, ramp_id).await;
    let step = policy_step(&mut conn_1).await;
    assert!(
        abort_ramp(&mut conn_1, QUEUE, BUILD_A, BUILD_B, step, CLEAR_BOUND)
            .await
            .expect("clear pool 1")
            .is_some()
    );
    mark_abort_reported(&mut conn_1, QUEUE, stored(ramp_id), CLEAR_BOUND)
        .await
        .expect("mark pool 1");

    // Within the grace, a pass keeps the reported marker.
    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert_eq!(abort_marker_count(&mut conn_1).await, 1, "the marker stays");

    // The late fan-out write reaches pool 2, and the marker finishes it.
    set_build_ramp_with_id(&mut conn_2, QUEUE, BUILD_B, RAMP_PERCENT, ramp_id)
        .await
        .expect("late fan-out write");
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        !ramp_is_active(&mut conn_2).await,
        "the late ramp is cleared"
    );
}

/// The guard keeps a failed report of a ramp with no `ramp_id` recoverable.
/// The clear writes a marker under a report id of its own, so a later pass
/// still reports the abort.
#[tokio::test]
async fn a_failed_report_of_an_id_less_ramp_is_recovered() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    set_build_policy(&mut conn, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy");
    // An old writer sets the ramp with no id.
    diesel::sql_query(
        "UPDATE harvest_build_policies \
         SET target_build_id = $2, ramp_percent = $3, updated_at = NOW() \
         WHERE queue_name = $1",
    )
    .bind::<Text, _>(QUEUE)
    .bind::<Text, _>(BUILD_B)
    .bind::<diesel::sql_types::Integer, _>(RAMP_PERCENT)
    .execute(&mut conn)
    .await
    .expect("old writer ramp");
    seed_healthy_base(&mut conn, 5).await;
    for _ in 0..5 {
        seed(&mut conn, true, "FAILED", false).await;
    }
    // The first report fails: no server listens on port 1.
    let audit_down = build_pool("postgres://postgres:postgres@127.0.0.1:1/none");
    let pools = [pool.clone()];
    let aborts = guard_once(&pools, &audit_down, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(!ramp_is_active(&mut conn).await, "the clear happens");
    assert_eq!(abort_marker_count(&mut conn).await, 1, "a marker remains");

    // A later pass reports the abort from that marker.
    let config = guard_config().with_report_grace(Duration::ZERO);
    let aborts = guard_once(&pools, &pool, &config, None).await;
    assert_eq!(aborts.len(), 1, "{aborts:?}");
    assert_eq!(aborts[0].reason, RampAbortReason::Unreported);
    assert_eq!(auto_abort_audit_rows(&mut conn).await, 1);
}

/// A split ramp with no abort marker is not cleared.
#[tokio::test]
async fn a_split_ramp_without_an_abort_marker_stays() {
    let (url_1, _c1) = setup().await;
    let (url_2, _c2) = setup().await;
    let (pool_1, pool_2) = (build_pool(&url_1), build_pool(&url_2));
    let mut conn_1 = AsyncPgConnection::establish(&url_1)
        .await
        .expect("connect 1");
    let mut conn_2 = AsyncPgConnection::establish(&url_2)
        .await
        .expect("connect 2");
    set_build_policy(&mut conn_1, QUEUE, BUILD_A, None)
        .await
        .expect("set base policy 1");
    set_ramp(&mut conn_2).await;

    let pools = [pool_1.clone(), pool_2.clone()];
    let aborts = guard_once(&pools, &pool_1, &guard_config(), None).await;
    assert!(aborts.is_empty(), "{aborts:?}");
    assert!(
        ramp_is_active(&mut conn_2).await,
        "no marker, so the ramp stays"
    );
}
