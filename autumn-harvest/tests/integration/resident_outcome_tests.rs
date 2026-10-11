#![cfg(feature = "db")]
//! The resident outcome of each decision on a real worker (issues #2007
//! and #2008).
//!
//! The worker records `harvest.workflow.resident` once per decision. These
//! tests read the outcome sequence of one run:
//!
//! 1. A sequential run is cold once, then hits on every decision.
//! 2. A join of an activity and a signal wait is a `multi_await` miss.
//! 3. A join of activities stays resident. Each tool result is a hit.
//! 4. An agent loop with parallel tool calls hits on every decision after
//!    the first. The test prints the hit rate, which the design records.
//!
//! Each test uses its own queue and worker id, so the tests can share one
//! database.

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::telemetry::{
    MetricsRecorder, NoOpPropagator, ResidentOutcome, TelemetryConfig,
};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, HarvestBuilder, StartWorkflowParams, WorkerConfig, WorkflowContext,
    start_or_load_workflow_execution,
};
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::{Value, json};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, wait_for_execution_state_with_timeout,
};

type HandlerFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

const ECHO: &str = "resident_outcome_echo";
const SEQUENTIAL: &str = "resident_outcome_sequential_wf";
const MIXED_JOIN: &str = "resident_outcome_mixed_join_wf";
const ACTIVITY_JOIN: &str = "resident_outcome_activity_join_wf";
const TOOL_LOOP: &str = "resident_outcome_tool_loop_wf";

/// The outcome of each decision, by workflow, in order.
#[derive(Debug, Default)]
struct OutcomeLog {
    outcomes: Mutex<Vec<(String, ResidentOutcome)>>,
}

impl OutcomeLog {
    fn of(&self, workflow: &str) -> Vec<ResidentOutcome> {
        self.outcomes
            .lock()
            .expect("outcome log lock")
            .iter()
            .filter(|(name, _)| name == workflow)
            .map(|(_, outcome)| *outcome)
            .collect()
    }
}

impl MetricsRecorder for OutcomeLog {
    fn record_workflow_resident(
        &self,
        workflow_name: &str,
        _queue: &str,
        outcome: ResidentOutcome,
    ) {
        self.outcomes
            .lock()
            .expect("outcome log lock")
            .push((workflow_name.to_string(), outcome));
    }
}

/// Body starts of `activity_join_workflow`. Only one test runs it.
static JOIN_BODY_STARTS: AtomicU64 = AtomicU64::new(0);

/// Body starts of `tool_loop_workflow`. Only one test runs it.
static TOOL_LOOP_BODY_STARTS: AtomicU64 = AtomicU64::new(0);

fn echo_activity<'a>(_ctx: &'a ActivityContext, input: Value) -> HandlerFuture<'a> {
    Box::pin(async move { Ok(input) })
}

/// One activity, then one signal. Each await ends one decision.
fn sequential_workflow<'a>(ctx: &'a WorkflowContext, input: Value) -> HandlerFuture<'a> {
    Box::pin(async move {
        let queue = input["queue"].as_str().ok_or("missing queue")?;
        let echo = ctx
            .execute_activity_raw(ECHO, json!({ "step": 1 }), queue)
            .await
            .map_err(|e| e.to_string())?;
        let go: Value = ctx.receive_signal("go").await.map_err(|e| e.to_string())?;
        Ok(json!([echo, go]))
    })
}

/// Joins an activity with a signal wait. The path does not cover the mix.
fn mixed_join_workflow<'a>(ctx: &'a WorkflowContext, input: Value) -> HandlerFuture<'a> {
    Box::pin(async move {
        let queue = input["queue"].as_str().ok_or("missing queue")?;
        let (echo, go) = futures::join!(
            ctx.execute_activity_raw(ECHO, json!({ "step": 1 }), queue),
            ctx.receive_signal::<Value>("go"),
        );
        Ok(json!([
            echo.map_err(|e| e.to_string())?,
            go.map_err(|e| e.to_string())?
        ]))
    })
}

/// Joins three activities.
fn activity_join_workflow<'a>(ctx: &'a WorkflowContext, input: Value) -> HandlerFuture<'a> {
    JOIN_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
    Box::pin(async move {
        let queue = input["queue"].as_str().ok_or("missing queue")?;
        let calls = (0..3).map(|i| ctx.execute_activity_raw(ECHO, json!({ "call": i }), queue));
        let results = futures::future::try_join_all(calls)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(results))
    })
}

/// An agent loop: a model call, then parallel tool calls, per round.
fn tool_loop_workflow<'a>(ctx: &'a WorkflowContext, input: Value) -> HandlerFuture<'a> {
    TOOL_LOOP_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
    Box::pin(async move {
        let queue = input["queue"].as_str().ok_or("missing queue")?;
        let mut transcript = Vec::new();
        for round in 0..3 {
            let plan = ctx
                .execute_activity_raw(ECHO, json!({ "model": round }), queue)
                .await
                .map_err(|e| e.to_string())?;
            let calls = (0..4).map(|tool| {
                ctx.execute_activity_raw(ECHO, json!({ "plan": plan, "tool": tool }), queue)
            });
            let results = futures::future::try_join_all(calls)
                .await
                .map_err(|e| e.to_string())?;
            transcript.push(json!(results));
        }
        Ok(json!(transcript))
    })
}

fn echo_info() -> ActivityInfo {
    ActivityInfo {
        name: ECHO,
        module: "resident_outcome_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(30)),
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
        handler: echo_activity,
    }
}

fn info_for(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "resident_outcome_tests",
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

/// Builds a worker from `WorkerConfig::default()`. It polls only `queue`.
fn build_worker(queue: &str, log: Arc<OutcomeLog>) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(vec![
            info_for(SEQUENTIAL, sequential_workflow),
            info_for(MIXED_JOIN, mixed_join_workflow),
            info_for(ACTIVITY_JOIN, activity_join_workflow),
            info_for(TOOL_LOOP, tool_loop_workflow),
        ])
        .activities(vec![echo_info()])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("resident_outcome_tests"),
            propagator: Arc::new(NoOpPropagator),
            metrics: log as Arc<dyn MetricsRecorder>,
        })
        .worker(WorkerConfig::default().with_queues([queue]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = unique_id("resident-outcome-w");
    runtime_config.poll_interval = Duration::from_millis(50);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

fn spawn(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    let runner = Arc::clone(worker);
    let pool = pool.clone();
    tokio::spawn(async move { runner.run(&pool).await })
}

/// Returns `prefix` with a short random suffix. A NOTIFY channel name holds
/// the queue name and has a cap of 63 bytes.
fn unique_id(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

fn start_params<'a>(
    workflow_name: &'a str,
    exec_id: ExecutionId,
    workflow_id: &'a str,
    queue: &'a str,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        start_source: autumn_harvest::StartSource::Api,
        ..StartWorkflowParams::new(
            workflow_name,
            workflow_id,
            exec_id,
            json!({ "queue": queue }),
            queue,
        )
    }
}

/// Starts `workflow_name` on a fresh queue and worker. Returns the run, the
/// log, the worker and its task.
async fn start_run(
    url: &str,
    workflow_name: &'static str,
) -> (
    ExecutionId,
    AsyncPgConnection,
    Arc<OutcomeLog>,
    Arc<Worker>,
    tokio::task::JoinHandle<()>,
) {
    let pool = build_test_pool(url);
    let mut conn = AsyncPgConnection::establish(url)
        .await
        .expect("connect to test DB");
    let queue = unique_id("resident-outcome-q");
    let log = Arc::new(OutcomeLog::default());
    let worker = build_worker(&queue, Arc::clone(&log));
    let handle = spawn(&worker, &pool);
    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("resident-outcome-wf");
    start_or_load_workflow_execution(
        &mut conn,
        start_params(workflow_name, exec_id, &workflow_id, &queue),
        None,
    )
    .await
    .expect("start workflow");
    (exec_id, conn, log, worker, handle)
}

/// Waits until `log` holds `decisions` outcomes of `workflow`.
async fn wait_decisions(log: &OutcomeLog, workflow: &str, decisions: usize) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while log.of(workflow).len() < decisions {
        assert!(
            Instant::now() < deadline,
            "{workflow} did not reach {decisions} decision(s): {:?}",
            log.of(workflow)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn stop(worker: &Arc<Worker>, handle: tokio::task::JoinHandle<()>) {
    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

/// AC (issue #2007): the worker records one outcome per decision.
#[tokio::test]
async fn a_sequential_run_is_cold_once_then_hits() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let (exec_id, mut conn, log, worker, handle) = start_run(&url, SEQUENTIAL).await;

    wait_decisions(&log, SEQUENTIAL, 2).await;
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "go", json!("go"))
        .await
        .expect("send signal");
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    assert_eq!(
        log.of(SEQUENTIAL),
        [
            ResidentOutcome::Cold,
            ResidentOutcome::Hit,
            ResidentOutcome::Hit
        ],
        "one outcome per decision"
    );
    stop(&worker, handle).await;
}

/// AC (issue #2007): a miss names its reason.
#[tokio::test]
async fn a_join_of_an_activity_and_a_signal_is_a_multi_await_miss() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let (exec_id, mut conn, log, worker, handle) = start_run(&url, MIXED_JOIN).await;

    // Decision 2 runs when the activity completes.
    wait_decisions(&log, MIXED_JOIN, 2).await;
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "go", json!("go"))
        .await
        .expect("send signal");
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    let outcomes = log.of(MIXED_JOIN);
    assert_eq!(
        outcomes.first(),
        Some(&ResidentOutcome::Cold),
        "{outcomes:?}"
    );
    assert_eq!(
        outcomes.get(1),
        Some(&ResidentOutcome::MultiAwait),
        "the decision after a mixed join replays: {outcomes:?}"
    );
    stop(&worker, handle).await;
}

/// AC (issue #2008): a join of activities stays resident. Every decision
/// after the first resumes it, and the body runs once.
#[tokio::test]
async fn a_join_of_activities_hits_on_every_tool_result() {
    let (url, _container) = setup_test_database_url_or_env().await;
    JOIN_BODY_STARTS.store(0, Ordering::SeqCst);
    let (exec_id, _conn, log, worker, handle) = start_run(&url, ACTIVITY_JOIN).await;

    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    let outcomes = log.of(ACTIVITY_JOIN);
    assert_eq!(
        outcomes.first(),
        Some(&ResidentOutcome::Cold),
        "{outcomes:?}"
    );
    assert!(
        outcomes.len() >= 2 && outcomes[1..].iter().all(|o| *o == ResidentOutcome::Hit),
        "each tool result must resume the join: {outcomes:?}"
    );
    assert_eq!(JOIN_BODY_STARTS.load(Ordering::SeqCst), 1, "{outcomes:?}");
    stop(&worker, handle).await;
}

/// AC (issue #2008): the hit rate of an agent loop with parallel tool calls.
///
/// Three rounds of one model call and four tool calls. Every decision after
/// the first must hit. The printed line is the measurement in
/// `DESIGN-2008.md`.
#[tokio::test]
async fn an_agent_loop_with_parallel_tool_calls_hits_after_the_first_decision() {
    let (url, _container) = setup_test_database_url_or_env().await;
    TOOL_LOOP_BODY_STARTS.store(0, Ordering::SeqCst);
    let (exec_id, _conn, log, worker, handle) = start_run(&url, TOOL_LOOP).await;

    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(60))
        .await;

    let outcomes = log.of(TOOL_LOOP);
    let hits = outcomes
        .iter()
        .filter(|o| **o == ResidentOutcome::Hit)
        .count();
    println!(
        "agent loop: {hits} hits of {} decisions; outcomes {:?}",
        outcomes.len(),
        outcomes
    );
    assert_eq!(
        outcomes.first(),
        Some(&ResidentOutcome::Cold),
        "{outcomes:?}"
    );
    assert_eq!(
        hits,
        outcomes.len() - 1,
        "every decision after the first must hit: {outcomes:?}"
    );
    assert_eq!(
        TOOL_LOOP_BODY_STARTS.load(Ordering::SeqCst),
        1,
        "{outcomes:?}"
    );
    stop(&worker, handle).await;
}
