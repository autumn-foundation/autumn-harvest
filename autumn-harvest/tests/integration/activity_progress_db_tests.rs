#![cfg(feature = "db")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

//! Live progress from inside an activity on Postgres (issue #2268).
//!
//! An activity sends chunks with `ActivityContext::publish_progress`. Each
//! chunk goes out with `pg_notify` on the progress channel of the run while
//! the activity runs. Nothing is stored: no event, no `harvest_stream_chunks`
//! row. Each chunk names its attempt, so a reader can drop the partial output
//! of a failed attempt.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use autumn_harvest::activity_progress::{
    ACTIVITY_PROGRESS_KEY, ActivityProgress, ActivityProgressSink, NotifyProgressSink,
};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::notify::{ProgressWaitOutcome, WorkflowProgressListener};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::telemetry::TelemetryConfig;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, ActivityExecId, ExecutionId, ShardId, StartWorkflowParams, WorkflowContext,
};

use crate::integration_e2e::setup_test_database_url_or_env;
use diesel::sql_types::{BigInt, Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use uuid::Uuid;

type BoxFut<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// A text that only a live chunk holds.
const LIVE_MARKER: &str = "live-only-chunk-7f3a";

// ── harness ──────────────────────────────────────────────────────────────────

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect failed")
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "activity_progress_db_tests",
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
    queue: &'static str,
    is_local: bool,
    retry: Option<RetryPolicy>,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_progress_db_tests",
        default_retry_policy: retry,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(queue),
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
        input_schema: None,
        output_schema: None,
        handler,
    }
}

fn registry(workflows: Vec<WorkflowInfo>, activities: Vec<ActivityInfo>) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        autumn_harvest::context::empty_shared_state(),
        Arc::new(TelemetryConfig::default()),
    ))
}

fn worker(queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: Uuid::new_v4().to_string(),
                queues: vec![queue.to_string()],
                queue_weights: HashMap::new(),
                notification_database_url: None,
                shard_notification_database_urls: Vec::new(),
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                max_concurrent_claims: autumn_harvest::worker::DEFAULT_MAX_CONCURRENT_CLAIMS,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::ZERO,
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 100,
                resident_workflows: true,
                priority_aging_secs: None,
                fairness_keys: false,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                labels: HashMap::new(),
                sharded_pool: None,
                max_workflow_history_events: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

fn unique_queue(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}-{}", Uuid::new_v4().simple()).into_boxed_str())
}

async fn start(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    queue: &str,
) -> ExecutionId {
    let workflow_id = format!("live-{}", Uuid::new_v4().simple());
    let params = StartWorkflowParams::new(
        workflow_name,
        &workflow_id,
        ExecutionId::new_for_shard(ShardId::new(0)),
        json!({}),
        queue,
    );
    autumn_harvest::execution::start_or_load_workflow_execution_with_codecs(
        conn,
        params,
        None,
        &PayloadCodecs::default(),
    )
    .await
    .expect("start")
    .exec_id
}

async fn count(conn: &mut AsyncPgConnection, sql: &str, exec_id: ExecutionId) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(sql)
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result(conn)
        .await
        .expect("count");
    row.n
}

async fn state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = Text)]
        state: String,
    }
    let row: StateRow =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .expect("load state");
    row.state
}

/// Wait for the next live chunk of the run, up to ten seconds. The chunk
/// must carry the activity flag, so the route never gives it a workflow `seq`.
async fn next_chunk(listener: &mut WorkflowProgressListener) -> Value {
    match listener
        .wait_for_progress_timeout(Duration::from_secs(10))
        .await
    {
        Ok(ProgressWaitOutcome::Chunk(payload)) => {
            assert!(
                payload.activity,
                "an activity chunk is flagged: {payload:?}"
            );
            payload.chunk
        }
        other => panic!("no live chunk arrived: {other:?}"),
    }
}

// ── workflows and activities ─────────────────────────────────────────────────

static RELEASE: AtomicBool = AtomicBool::new(false);

fn wf_live(ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("live_step", json!({}), &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Sends two chunks, then waits until the test has read them.
fn live_step(ctx: &ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.publish_progress(json!({"text": LIVE_MARKER, "n": 0}))
            .map_err(|e| e.to_string())?;
        ctx.publish_progress(json!({"text": LIVE_MARKER, "n": 1}))
            .map_err(|e| e.to_string())?;
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !AtomicBool::load(&RELEASE, Ordering::SeqCst) && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(json!("done"))
    })
}

fn wf_flaky(ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("flaky_live_step", json!({}), &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Sends a chunk, and fails its first attempt.
fn flaky_live_step(ctx: &ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let attempt = ctx.info().attempt;
        ctx.publish_progress(json!({"attempt_seen": attempt}))
            .map_err(|e| e.to_string())?;
        // The reader needs time to read the chunk before the retry.
        tokio::time::sleep(Duration::from_millis(200)).await;
        if attempt == 1 {
            return Err("a transient fault".to_owned());
        }
        Ok(json!("done"))
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_chunk_arrives_before_the_activity_completes_and_is_never_stored() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique_queue("live");
    let registry = registry(
        vec![wf_info("activity_progress_live", wf_live)],
        vec![activity_info("live_step", queue, false, None, live_step)],
    );
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "activity_progress_live", queue).await;
    let mut listener = WorkflowProgressListener::connect(&url, exec_id.as_uuid())
        .await
        .expect("listen");
    let pool = build_pool(&url);
    let worker = worker(queue, registry);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });

    let first = next_chunk(&mut listener).await;
    let second = next_chunk(&mut listener).await;
    // The activity still waits, so it has not completed.
    let completed = count(
        &mut conn,
        "SELECT count(*) AS n FROM harvest_events \
          WHERE workflow_exec_id = $1 AND event_type = 'ActivityCompleted'",
        exec_id,
    )
    .await;
    assert_eq!(completed, 0, "the chunk came while the activity ran");
    assert_eq!(first[ACTIVITY_PROGRESS_KEY]["attempt"], 1);
    assert_eq!(first[ACTIVITY_PROGRESS_KEY]["index"], 0);
    assert_eq!(second[ACTIVITY_PROGRESS_KEY]["index"], 1);
    assert_eq!(first["data"]["text"], LIVE_MARKER);

    RELEASE.store(true, Ordering::SeqCst);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while state(&mut conn, exec_id).await != "COMPLETED" {
        assert!(
            std::time::Instant::now() < deadline,
            "the run did not complete"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle.abort();

    let chunks = count(
        &mut conn,
        "SELECT count(*) AS n FROM harvest_stream_chunks WHERE workflow_exec_id = $1",
        exec_id,
    )
    .await;
    assert_eq!(chunks, 0, "a live chunk is never stored");
    let stored = count(
        &mut conn,
        &format!(
            "SELECT count(*) AS n FROM harvest_events \
              WHERE workflow_exec_id = $1 AND event_data::text LIKE '%{LIVE_MARKER}%'"
        ),
        exec_id,
    )
    .await;
    assert_eq!(stored, 0, "no event holds the live text");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_attempt_tags_its_chunks_with_the_new_attempt() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique_queue("live-retry");
    let retry = RetryPolicy::exponential(2, Duration::from_millis(100));
    let registry = registry(
        vec![wf_info("activity_progress_retry", wf_flaky)],
        vec![activity_info(
            "flaky_live_step",
            queue,
            false,
            Some(retry),
            flaky_live_step,
        )],
    );
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, "activity_progress_retry", queue).await;
    let mut listener = WorkflowProgressListener::connect(&url, exec_id.as_uuid())
        .await
        .expect("listen");
    let pool = build_pool(&url);
    let worker = worker(queue, registry);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });

    let first = next_chunk(&mut listener).await;
    let second = next_chunk(&mut listener).await;
    handle.abort();
    assert_eq!(first[ACTIVITY_PROGRESS_KEY]["attempt"], 1);
    assert_eq!(second[ACTIVITY_PROGRESS_KEY]["attempt"], 2);
    assert_eq!(
        second[ACTIVITY_PROGRESS_KEY]["index"], 0,
        "a retry counts from 0"
    );
    assert_eq!(second["data"]["attempt_seen"], 2);
}

// ── the Postgres sink ────────────────────────────────────────────────────────

/// A pool of one connection. `wait` bounds each acquire.
fn one_connection_pool(url: &str, wait: Duration) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(1)
        .wait_timeout(Some(wait))
        .runtime(deadpool::Runtime::Tokio1)
        .build()
        .expect("pool build failed")
}

fn progress(exec_id: ExecutionId, activity_id: ActivityExecId, index: u64) -> ActivityProgress {
    ActivityProgress {
        execution_id: exec_id,
        activity_id,
        activity_type: "agent_model_turn".to_owned(),
        attempt: 1,
        index,
        chunk: json!({"text": format!("t{index}")}),
    }
}

/// A burst goes out whole and in order. The sink then gives its one
/// connection back while it still lives, so a long model turn holds none.
#[tokio::test(flavor = "multi_thread")]
async fn the_sink_sends_a_burst_in_order_and_gives_its_connection_back() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let exec_id = ExecutionId::new();
    let activity_id = ActivityExecId::new();
    let mut listener = WorkflowProgressListener::connect(&url, exec_id.as_uuid())
        .await
        .expect("listen");
    let pool = one_connection_pool(&url, Duration::from_secs(5));
    let sink = NotifyProgressSink::spawn(pool.clone());
    for index in 0..50 {
        sink.publish(progress(exec_id, activity_id, index));
    }
    for index in 0..50 {
        let chunk = next_chunk(&mut listener).await;
        assert_eq!(chunk[ACTIVITY_PROGRESS_KEY]["index"], index, "in order");
        assert_eq!(chunk["data"]["text"], format!("t{index}"));
    }
    let free = tokio::time::timeout(Duration::from_secs(2), pool.get()).await;
    assert!(
        matches!(free, Ok(Ok(_))),
        "the sink holds no connection while its queue is empty"
    );
    drop(sink);
}

/// A failed acquire drops every chunk that waits. Each queued chunk does not
/// wait out an acquire of its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_acquire_drops_the_waiting_chunks_at_once() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let exec_id = ExecutionId::new();
    let activity_id = ActivityExecId::new();
    let mut listener = WorkflowProgressListener::connect(&url, exec_id.as_uuid())
        .await
        .expect("listen");
    let pool = one_connection_pool(&url, Duration::from_millis(300));
    let held = pool.get().await.expect("take the only connection");
    let sink = NotifyProgressSink::spawn(pool.clone());
    for index in 0..3 {
        sink.publish(progress(exec_id, activity_id, index));
    }
    // The first acquire fails after 300 ms. With one acquire per chunk, the
    // third chunk would still wait when the connection frees at 700 ms.
    tokio::time::sleep(Duration::from_millis(700)).await;
    drop(held);
    sink.publish(progress(exec_id, activity_id, 3));
    let first = next_chunk(&mut listener).await;
    assert_eq!(
        first[ACTIVITY_PROGRESS_KEY]["index"], 3,
        "the chunks that waited on the failed acquire drop"
    );
}
