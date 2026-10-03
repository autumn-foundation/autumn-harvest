#![cfg(feature = "db")]
//! DB-pool, query-latency, poller and outlier signals (issue #1815).
//!
//! The pure detection rules have unit tests in `worker_outlier.rs`. This
//! suite checks the parts that need Postgres:
//!
//! - the task-stats table, the live-peer filter and the outlier tick;
//! - a running worker that emits every new metric and publishes its stats;
//! - the timeout scanner pass that records the `scan` op.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::telemetry::{DbOp, MetricsRecorder, NoOpPropagator, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::worker_outlier::{
    OutlierConfig, OutlierDimension, TaskOutcomeWindow, WorkerTaskStats,
};
use autumn_harvest::workers::{self, OutlierProbe};
use autumn_harvest::{
    ActivityContext, HarvestBuilder, RetryPolicy, ShardedDbPool, StartWorkflowParams, WorkerConfig,
    WorkflowContext, start_or_load_workflow_execution, timeout,
};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, wait_for_execution_state_with_timeout,
};

// ---------------------------------------------------------------------------
// Recording metrics.
// ---------------------------------------------------------------------------

/// One recorded sample of a new #1815 metric.
#[derive(Debug, Clone, PartialEq)]
enum Sample {
    Pool {
        shard: u16,
    },
    PoolWait {
        shard: u16,
    },
    Query(&'static str),
    Pollers {
        queue: String,
        pollers: u64,
    },
    Outlier {
        dimension: OutlierDimension,
        flagged: bool,
    },
}

#[derive(Debug, Default)]
struct Recording {
    samples: Mutex<Vec<Sample>>,
}

impl Recording {
    fn push(&self, sample: Sample) {
        self.samples.lock().unwrap().push(sample);
    }

    fn samples(&self) -> Vec<Sample> {
        self.samples.lock().unwrap().clone()
    }

    fn has(&self, wanted: &Sample) -> bool {
        self.samples.lock().unwrap().contains(wanted)
    }
}

impl MetricsRecorder for Recording {
    fn is_enabled(&self) -> bool {
        true
    }

    fn record_db_pool(&self, shard: u16, _in_use: u64, _idle: u64) {
        self.push(Sample::Pool { shard });
    }

    fn record_db_pool_wait(&self, shard: u16, _seconds: f64) {
        self.push(Sample::PoolWait { shard });
    }

    fn record_db_query_duration(&self, op: DbOp, _seconds: f64) {
        self.push(Sample::Query(op.as_str()));
    }

    fn record_worker_pollers(&self, queue: &str, pollers: u64) {
        self.push(Sample::Pollers {
            queue: queue.to_owned(),
            pollers,
        });
    }

    fn record_worker_outlier(&self, dimension: OutlierDimension, flagged: bool) {
        self.push(Sample::Outlier { dimension, flagged });
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Returns `prefix` with a random suffix. Twelve hex digits keep a queue name
/// inside the 63-byte NOTIFY channel cap.
fn unique_id(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

async fn register(conn: &mut AsyncPgConnection, worker_id: &str, queue: &str) {
    workers::register_worker(
        conn,
        worker_id,
        &[queue.to_owned()],
        &[0],
        4,
        "test-host",
        None,
        "",
        None,
        &HashMap::<String, String>::new(),
        0,
        &[],
    )
    .await
    .expect("register worker");
}

/// A window with `tasks` outcomes, every `fail_every`-th one failed.
fn window(tasks: u32, fail_every: Option<u32>) -> Arc<TaskOutcomeWindow> {
    let window = Arc::new(TaskOutcomeWindow::default());
    for i in 0..tasks {
        let failed = fail_every.is_some_and(|n| i % n == 0);
        window.record(failed, Duration::from_millis(20));
    }
    window
}

fn probe(window: Arc<TaskOutcomeWindow>, metrics: Arc<Recording>) -> OutlierProbe {
    OutlierProbe {
        window,
        metrics,
        config: OutlierConfig::default(),
        fleet_stale_secs: 60,
    }
}

// ---------------------------------------------------------------------------
// Task-stats table and outlier tick.
// ---------------------------------------------------------------------------

/// The issue #1815 RED test against the real database: one worker fails 50%
/// of its tasks while three peers fail none. Its own heartbeat tick flags it
/// and sets the gauge to 1. Each peer tick reads 0.
#[tokio::test]
async fn outlier_tick_flags_the_worker_failing_half_its_tasks() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("outlier-q");
    let sick = unique_id("w-sick");
    let peers: Vec<String> = (0..3).map(|i| unique_id(&format!("w-ok{i}"))).collect();
    // Other tests share this database. Clear stats left by earlier runs so
    // only this test's fleet takes part.
    diesel::sql_query("DELETE FROM harvest_worker_task_stats")
        .execute(&mut conn)
        .await
        .expect("clear stats");

    register(&mut conn, &sick, &queue).await;
    for peer in &peers {
        register(&mut conn, peer, &queue).await;
    }

    // Peers publish first, so the sick worker sees a full fleet.
    let peer_metrics = Arc::new(Recording::default());
    for peer in &peers {
        let flagged = workers::run_outlier_tick(
            &mut conn,
            peer,
            &probe(window(100, None), Arc::clone(&peer_metrics)),
        )
        .await
        .expect("peer tick");
        assert!(flagged.is_empty(), "a healthy peer is not flagged");
    }

    let sick_metrics = Arc::new(Recording::default());
    let flagged = workers::run_outlier_tick(
        &mut conn,
        &sick,
        &probe(window(100, Some(2)), Arc::clone(&sick_metrics)),
    )
    .await
    .expect("sick tick");
    assert_eq!(flagged, vec![OutlierDimension::FailureRatio]);
    assert_eq!(
        sick_metrics.samples(),
        vec![
            Sample::Outlier {
                dimension: OutlierDimension::FailureRatio,
                flagged: true
            },
            Sample::Outlier {
                dimension: OutlierDimension::LatencyP99,
                flagged: false
            },
        ],
        "the tick sets every dimension, flagged or not"
    );

    // A peer that ticks again, now with the sick worker in the fleet, stays 0.
    let again = Arc::new(Recording::default());
    let flagged = workers::run_outlier_tick(
        &mut conn,
        &peers[0],
        &probe(window(100, None), Arc::clone(&again)),
    )
    .await
    .expect("peer tick");
    assert_eq!(flagged, Vec::<OutlierDimension>::new());
    assert!(again.has(&Sample::Outlier {
        dimension: OutlierDimension::FailureRatio,
        flagged: false
    }));

    // The stored row is the snapshot the sick worker published.
    let stats = workers::load_live_worker_task_stats(&mut conn, 60)
        .await
        .expect("load stats");
    let own = stats
        .iter()
        .find(|(id, _)| id == &sick)
        .map(|(_, s)| *s)
        .expect("sick row");
    assert_eq!(
        own,
        WorkerTaskStats {
            tasks: 100,
            failures: 50,
            p99_latency_ms: Some(20)
        }
    );
}

/// A draining worker is not a live peer, and deleting a worker row drops its
/// stats through the foreign key.
#[tokio::test]
async fn live_stats_skip_draining_workers_and_follow_worker_deletes() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("live-q");
    let active = unique_id("w-active");
    let draining = unique_id("w-draining");
    register(&mut conn, &active, &queue).await;
    register(&mut conn, &draining, &queue).await;
    let stats = WorkerTaskStats {
        tasks: 30,
        failures: 3,
        p99_latency_ms: Some(12),
    };
    workers::upsert_worker_task_stats(&mut conn, &active, &stats)
        .await
        .expect("upsert active");
    workers::upsert_worker_task_stats(&mut conn, &draining, &stats)
        .await
        .expect("upsert draining");
    diesel::sql_query("UPDATE harvest_workers SET status = 'Draining' WHERE worker_id = $1")
        .bind::<diesel::sql_types::Text, _>(&draining)
        .execute(&mut conn)
        .await
        .expect("drain");

    let ids: Vec<String> = workers::load_live_worker_task_stats(&mut conn, 60)
        .await
        .expect("load")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(ids.contains(&active));
    assert!(!ids.contains(&draining), "a draining worker is not a peer");

    // A second upsert replaces the row.
    let newer = WorkerTaskStats { tasks: 40, ..stats };
    workers::upsert_worker_task_stats(&mut conn, &active, &newer)
        .await
        .expect("upsert again");
    let row = workers::load_live_worker_task_stats(&mut conn, 60)
        .await
        .expect("load")
        .into_iter()
        .find(|(id, _)| id == &active)
        .map(|(_, s)| s);
    assert_eq!(row, Some(newer));

    diesel::sql_query("DELETE FROM harvest_workers WHERE worker_id = $1")
        .bind::<diesel::sql_types::Text, _>(&active)
        .execute(&mut conn)
        .await
        .expect("delete worker");
    let left: Count = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_worker_task_stats WHERE worker_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&active)
    .get_result(&mut conn)
    .await
    .expect("count");
    assert_eq!(left.n, 0, "the FK cascade drops the stats row");
}

/// An upsert for a worker with no row fails on the foreign key. The heartbeat
/// logs it and retries on the next tick.
#[tokio::test]
async fn stats_upsert_for_an_unknown_worker_fails() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let result = workers::upsert_worker_task_stats(
        &mut conn,
        &unique_id("w-ghost"),
        &WorkerTaskStats::default(),
    )
    .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// A running worker emits every new metric.
// ---------------------------------------------------------------------------

const WORKFLOW: &str = "saturation_wf";
const ACTIVITY: &str = "saturation_always_fails";
static ACTIVITY_CALLS: AtomicU32 = AtomicU32::new(0);

fn failing_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ACTIVITY_CALLS.fetch_add(1, Ordering::SeqCst);
        Err("boom".to_owned())
    })
}

fn calls_failing_activity<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw(ACTIVITY, input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: WORKFLOW,
        module: "worker_saturation_metrics_tests",
        handler: calls_failing_activity,
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

fn activity_info() -> ActivityInfo {
    ActivityInfo {
        name: ACTIVITY,
        module: "worker_saturation_metrics_tests",
        default_retry_policy: Some(RetryPolicy::fixed(2, Duration::from_millis(10))),
        default_start_to_close: Some(Duration::from_secs(30)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
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
        handler: failing_activity,
    }
}

fn build_worker(queue: &str, worker_id: &str, metrics: Arc<Recording>) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(vec![workflow_info()])
        .activities(vec![activity_info()])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("worker_saturation_metrics_tests"),
            propagator: Arc::new(NoOpPropagator),
            metrics: metrics as Arc<dyn MetricsRecorder>,
        })
        .worker(WorkerConfig::default().with_queues([queue]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.worker_heartbeat_interval = Duration::from_millis(100);
    runtime_config.shutdown_timeout = Duration::from_secs(5);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

fn start_params<'a>(
    exec_id: ExecutionId,
    workflow_id: &'a str,
    queue_name: &'a str,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name: WORKFLOW,
        workflow_id,
        exec_id,
        input: serde_json::json!({}).into(),
        parent_id: None,
        queue_name,
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

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct StatsRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    window_tasks: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    window_failures: i32,
}

/// A running worker emits the pool, wait, query, poller and outlier metrics,
/// and its heartbeat publishes a task-stats row that counts the failed
/// activity attempts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_worker_emits_saturation_metrics_and_publishes_task_stats() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool: DbPool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let queue = unique_id("sat-q");
    let worker_id = unique_id("sat-w");
    let metrics = Arc::new(Recording::default());
    let worker = build_worker(&queue, &worker_id, Arc::clone(&metrics));
    let calls_before = AtomicU32::load(&ACTIVITY_CALLS, Ordering::SeqCst);

    let exec_id = ExecutionId::new();
    let workflow_id = unique_id("sat-wf");
    start_or_load_workflow_execution(&mut conn, start_params(exec_id, &workflow_id, &queue), None)
        .await
        .expect("start workflow");

    let runner = Arc::clone(&worker);
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&run_pool).await });

    wait_for_execution_state_with_timeout(&url, exec_id, "FAILED", Duration::from_secs(30)).await;
    assert_eq!(
        AtomicU32::load(&ACTIVITY_CALLS, Ordering::SeqCst) - calls_before,
        2,
        "the activity runs once per allowed attempt"
    );

    // The heartbeat publishes on its own cadence. Wait for a snapshot that
    // holds both failed attempts.
    let deadline = Instant::now() + Duration::from_secs(15);
    let row = loop {
        let row: Option<StatsRow> = diesel::sql_query(
            "SELECT window_tasks, window_failures FROM harvest_worker_task_stats \
             WHERE worker_id = $1",
        )
        .bind::<diesel::sql_types::Text, _>(&worker_id)
        .get_result(&mut conn)
        .await
        .ok();
        if let Some(row) = row
            && row.window_failures >= 2
        {
            break row;
        }
        assert!(
            Instant::now() < deadline,
            "no task-stats row with 2 failures"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(row.window_failures, 2, "each failed attempt counts once");
    assert!(
        row.window_tasks > row.window_failures,
        "workflow tasks count as successes too"
    );

    // Wait for one more sampler pass so the gauges have a value.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let shard = 0u16;
    for wanted in [
        Sample::Pool { shard },
        Sample::PoolWait { shard },
        Sample::Query(DbOp::Claim.as_str()),
        Sample::Query(DbOp::Persist.as_str()),
        Sample::Query(DbOp::Heartbeat.as_str()),
        Sample::Pollers {
            queue: queue.clone(),
            pollers: 1,
        },
        Sample::Outlier {
            dimension: OutlierDimension::FailureRatio,
            flagged: false,
        },
        Sample::Outlier {
            dimension: OutlierDimension::LatencyP99,
            flagged: false,
        },
    ] {
        assert!(metrics.has(&wanted), "missing {wanted:?}");
    }

    worker.shutdown();
    handle.await.expect("worker joins");
    // A drained worker reports no pollers, so a stale `1` cannot mask it.
    let last_pollers = metrics
        .samples()
        .into_iter()
        .filter_map(|s| match s {
            Sample::Pollers { queue: q, pollers } if q == queue => Some(pollers),
            _ => None,
        })
        .next_back();
    assert_eq!(last_pollers, Some(0), "pollers read 0 after the drain");
}

/// One timeout-scanner pass records the `scan` op.
#[tokio::test]
async fn timeout_scanner_pass_records_the_scan_op() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = pool.get().await.expect("connection");
    let recorder = Recording::default();
    timeout::enforce_timeouts_once(
        &mut conn,
        &recorder,
        Duration::from_secs(5),
        &Option::<ShardedDbPool>::None,
        &[ShardId::new(0)],
        None,
        None,
        60,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    )
    .await
    .expect("scan pass");
    assert!(recorder.has(&Sample::Query(DbOp::Scan.as_str())));
}
