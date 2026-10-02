#![cfg(feature = "db")]
//! Sticky routing is on by default (issue #1798).
//!
//! These tests use `WorkerConfig::default()` and set no sticky option. They
//! prove three things:
//!
//! 1. The next decision of a suspended execution runs on the same worker.
//!    That worker reports a cache hit.
//! 2. When that worker dies, a peer runs the next decision after the
//!    sticky window closes.
//! 3. A graceful shutdown releases the pins of the worker at once.
//!
//! A queue-level test also proves which rows the shutdown release touches.

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpPropagator, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    HarvestBuilder, StartWorkflowParams, WorkerConfig, WorkflowContext,
    start_or_load_workflow_execution,
};
use diesel::sql_types::{Nullable, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, wait_for_execution_state_with_timeout,
};

const WORKFLOW: &str = "sticky_default_wf";

/// Counts cache hits and misses for one worker.
#[derive(Debug, Default)]
struct CacheCounts {
    hits: AtomicU64,
    misses: AtomicU64,
}

impl CacheCounts {
    fn hits(&self) -> u64 {
        AtomicU64::load(&self.hits, Ordering::SeqCst)
    }

    fn misses(&self) -> u64 {
        AtomicU64::load(&self.misses, Ordering::SeqCst)
    }

    fn decisions(&self) -> u64 {
        self.hits() + self.misses()
    }
}

impl MetricsRecorder for CacheCounts {
    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.hits.fetch_add(1, Ordering::SeqCst);
    }

    fn record_workflow_cache_miss(&self, _workflow_name: &str, _queue: &str) {
        self.misses.fetch_add(1, Ordering::SeqCst);
    }
}

/// Waits for two signals, then completes.
fn two_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let first: serde_json::Value = ctx
            .receive_signal("first")
            .await
            .map_err(|e| e.to_string())?;
        let second: serde_json::Value = ctx
            .receive_signal("second")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!([first, second]))
    })
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: WORKFLOW,
        module: "sticky_default_tests",
        handler: two_signal_workflow,
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

/// Builds a worker from `WorkerConfig::default()` with no sticky override.
fn build_default_worker(worker_id: &str, metrics: Arc<CacheCounts>) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(vec![workflow_info()])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("sticky_default_tests"),
            propagator: Arc::new(NoOpPropagator),
            metrics: metrics as Arc<dyn MetricsRecorder>,
        })
        .worker(WorkerConfig::default())
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

fn spawn(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    let runner = Arc::clone(worker);
    let pool = pool.clone();
    tokio::spawn(async move { runner.run(&pool).await })
}

fn start_params(exec_id: ExecutionId, workflow_id: &str) -> StartWorkflowParams<'_> {
    StartWorkflowParams {
        workflow_name: WORKFLOW,
        workflow_id,
        exec_id,
        input: serde_json::json!({}),
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
        priority: autumn_harvest::types::Priority::default(),
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
    }
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test DB")
}

#[derive(diesel::QueryableByName, Debug)]
struct PinRow {
    #[diesel(sql_type = Nullable<Text>)]
    sticky_worker_id: Option<String>,
}

/// Returns the sticky owner of the parked workflow task, or `None` while
/// the task is not parked.
async fn parked_pin(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<PinRow> {
    diesel::sql_query(
        "SELECT sticky_worker_id FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow' \
           AND state = 'RUNNING' AND worker_id IS NULL",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .ok()
}

/// Waits until `counts` reports `decisions` decisions and the task is parked.
async fn wait_parked_after(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    counts: &CacheCounts,
    decisions: u64,
) -> PinRow {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if counts.decisions() >= decisions {
            if let Some(row) = parked_pin(conn, exec_id).await {
                return row;
            }
        }
        assert!(
            Instant::now() < deadline,
            "workflow did not park after {decisions} decision(s)"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn signal(conn: &mut AsyncPgConnection, exec_id: ExecutionId, name: &str) {
    autumn_harvest::signal::send_signal(conn, exec_id, name, serde_json::json!(name))
        .await
        .expect("send signal");
}

/// AC: the second decision lands on the same worker. When that worker
/// dies, the next decision falls back to a peer after the sticky window.
#[tokio::test]
async fn default_worker_keeps_its_execution_and_peer_takes_over_after_a_crash() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;

    let a_counts = Arc::new(CacheCounts::default());
    let b_counts = Arc::new(CacheCounts::default());
    let worker_a = build_default_worker("sticky-default-a", Arc::clone(&a_counts));
    let worker_b = build_default_worker("sticky-default-b", Arc::clone(&b_counts));

    // Only worker A runs, so A runs decision 1 and parks the task.
    let handle_a = spawn(&worker_a, &pool);
    let exec_id = ExecutionId::new();
    let workflow_id = format!("sticky-default-crash-{}", exec_id.as_uuid());
    start_or_load_workflow_execution(&mut conn, start_params(exec_id, &workflow_id), None)
        .await
        .expect("start workflow");
    let pin = wait_parked_after(&mut conn, exec_id, &a_counts, 1).await;
    assert_eq!(
        pin.sticky_worker_id.as_deref(),
        Some("sticky-default-a"),
        "a default worker must pin the parked task to itself"
    );
    assert_eq!(a_counts.misses(), 1, "decision 1 is a cold load");

    // Worker B joins. The pin keeps decision 2 on worker A.
    let handle_b = spawn(&worker_b, &pool);
    signal(&mut conn, exec_id, "first").await;
    wait_parked_after(&mut conn, exec_id, &a_counts, 2).await;
    assert_eq!(a_counts.hits(), 1, "decision 2 must be a cache hit on A");
    assert_eq!(b_counts.decisions(), 0, "B must not run a pinned decision");

    // Worker A dies without a shutdown. The pin stays until it expires.
    handle_a.abort();
    let _ = handle_a.await;
    let signalled_at = Instant::now();
    signal(&mut conn, exec_id, "second").await;
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    assert_eq!(b_counts.misses(), 1, "B runs decision 3 from a cold load");
    assert!(
        signalled_at.elapsed() >= Duration::from_secs(4),
        "B must wait for the sticky window to close; waited {:?}",
        signalled_at.elapsed()
    );

    worker_b.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_b).await;
}

/// A graceful shutdown releases the pins of the worker, so a peer does not
/// wait for the sticky window.
#[tokio::test]
async fn graceful_shutdown_releases_the_sticky_pins_of_the_worker() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;

    let a_counts = Arc::new(CacheCounts::default());
    let b_counts = Arc::new(CacheCounts::default());
    let worker_a = build_default_worker("sticky-release-a", Arc::clone(&a_counts));

    let handle_a = spawn(&worker_a, &pool);
    let exec_id = ExecutionId::new();
    let workflow_id = format!("sticky-default-release-{}", exec_id.as_uuid());
    start_or_load_workflow_execution(&mut conn, start_params(exec_id, &workflow_id), None)
        .await
        .expect("start workflow");
    let pin = wait_parked_after(&mut conn, exec_id, &a_counts, 1).await;
    assert_eq!(pin.sticky_worker_id.as_deref(), Some("sticky-release-a"));

    worker_a.shutdown();
    tokio::time::timeout(Duration::from_secs(10), handle_a)
        .await
        .expect("worker A stops")
        .expect("worker A task joins");

    let pin = parked_pin(&mut conn, exec_id)
        .await
        .expect("task is still parked");
    assert_eq!(
        pin.sticky_worker_id, None,
        "a graceful shutdown must release the pin"
    );

    // A peer now runs the rest of the workflow.
    let worker_b = build_default_worker("sticky-release-b", Arc::clone(&b_counts));
    let handle_b = spawn(&worker_b, &pool);
    signal(&mut conn, exec_id, "first").await;
    signal(&mut conn, exec_id, "second").await;
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;
    assert!(
        b_counts.decisions() >= 1,
        "B must run the follow-up decision"
    );

    worker_b.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_b).await;
}

#[derive(diesel::QueryableByName, Debug)]
struct IdPin {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = Nullable<Text>)]
    sticky_worker_id: Option<String>,
}

async fn enqueue_pinned(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    session_id: Option<uuid::Uuid>,
) -> uuid::Uuid {
    let exec_id = ExecutionId::new();
    let workflow_id = format!("sticky-release-row-{}", exec_id.as_uuid());
    start_or_load_workflow_execution(conn, start_params(exec_id, &workflow_id), None)
        .await
        .expect("start workflow");
    // Drop the start task so each test row is the only task of its execution.
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(conn)
        .await
        .expect("drop start task");
    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    let mut params = params.with_sticky(worker_id, Duration::from_secs(60));
    if let Some(session_id) = session_id {
        params = params.with_session_id(session_id);
    }
    queue::enqueue(conn, &params).await.expect("enqueue")
}

async fn set_state(
    conn: &mut AsyncPgConnection,
    id: uuid::Uuid,
    state: &str,
    worker: Option<&str>,
) {
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET state = $2, worker_id = $3, \
             started_at = CASE WHEN $3 IS NULL THEN NULL ELSE NOW() END \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<Text, _>(state)
    .bind::<Nullable<Text>, _>(worker)
    .execute(conn)
    .await
    .expect("set task state");
}

async fn pin_of(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> Option<String> {
    let row: IdPin =
        diesel::sql_query("SELECT id, sticky_worker_id FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(id)
            .get_result(conn)
            .await
            .expect("read pin");
    row.sticky_worker_id
}

/// The release clears pending and parked pins of one worker. It keeps
/// session pins, pins of other workers and rows the worker still runs.
#[tokio::test]
async fn release_worker_sticky_pins_clears_only_idle_unsessioned_rows_of_the_worker() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let me = format!("release-me-{}", uuid::Uuid::new_v4());
    let peer = format!("release-peer-{}", uuid::Uuid::new_v4());

    let pending = enqueue_pinned(&mut conn, &me, None).await;
    let parked = enqueue_pinned(&mut conn, &me, None).await;
    set_state(&mut conn, parked, "RUNNING", None).await;
    let running = enqueue_pinned(&mut conn, &me, None).await;
    set_state(&mut conn, running, "RUNNING", Some(&me)).await;
    let session = enqueue_pinned(&mut conn, &me, Some(uuid::Uuid::new_v4())).await;
    let other = enqueue_pinned(&mut conn, &peer, None).await;

    let released = queue::release_worker_sticky_pins(&mut conn, &me)
        .await
        .expect("release pins");

    assert_eq!(released, 2, "only the pending and parked rows are released");
    assert_eq!(pin_of(&mut conn, pending).await, None);
    assert_eq!(pin_of(&mut conn, parked).await, None);
    assert_eq!(
        pin_of(&mut conn, running).await.as_deref(),
        Some(me.as_str())
    );
    assert_eq!(
        pin_of(&mut conn, session).await.as_deref(),
        Some(me.as_str())
    );
    assert_eq!(
        pin_of(&mut conn, other).await.as_deref(),
        Some(peer.as_str())
    );
}
