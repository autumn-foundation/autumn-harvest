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
use autumn_harvest::workers::{self, LiveWorkerTaskStats, OutlierProbe};
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
        in_use: u64,
        idle: u64,
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

    fn record_db_pool(&self, shard: u16, in_use: u64, idle: u64) {
        self.push(Sample::Pool {
            shard,
            in_use,
            idle,
        });
    }

    fn record_db_pool_wait(&self, shard: u16, _seconds: f64) {
        self.push(Sample::PoolWait { shard });
    }

    fn record_db_query_duration(&self, op: DbOp, _shard: u16, _seconds: f64) {
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

/// The cohort key of a worker that polls `queue` alone, with no weights.
fn cohort(queue: &str) -> String {
    workers::worker_cohort(&workers::CohortPolicy {
        queues: &[queue.to_owned()],
        queue_weights: &HashMap::new(),
        build_id: "",
        labels: &HashMap::new(),
        slots: workers::SlotPolicy::of(1, 1, None),
        session_slots: 0,
        priority_aging_secs: None,
        ineligible_activities: &[],
        shard_assignments: &[],
        registered_workflows: &[],
        registered_activities: &[],
        circuit_breakers: &autumn_harvest::circuit_breaker::CircuitBreakerRegistry::empty(),
        dispatch_channel: false,
        retry_budgets: &autumn_harvest::retry_budget::RetryBudgetConfig::default(),
        outcome_window: std::time::Duration::from_secs(300),
        peer_stale_secs: 120,
        execution: workers::ExecutionPolicy::default(),
        payload: workers::PayloadPolicy::default(),
    })
}

fn probe(queue: &str, window: Arc<TaskOutcomeWindow>, metrics: Arc<Recording>) -> OutlierProbe {
    OutlierProbe {
        window,
        metrics,
        config: OutlierConfig::default(),
        fleet_stale_secs: 60,
        cohort: cohort(queue),
        codecs: None,
        compare: true,
        slot: 0,
        // Fresh boards keep each probe apart from other tests in the process.
        shard_peers: Arc::default(),
        process_flags: Arc::default(),
    }
}

/// One comparing, non-draining outlier tick.
async fn tick(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    probe: &OutlierProbe,
) -> Vec<OutlierDimension> {
    workers::run_outlier_tick(conn, worker_id, probe, false)
        .await
        .expect("outlier tick")
}

fn find(rows: &[LiveWorkerTaskStats], worker_id: &str) -> Option<WorkerTaskStats> {
    rows.iter()
        .find(|row| row.worker_id == worker_id)
        .map(|row| row.stats)
}

/// Registers `n` healthy peers on `queue` and publishes their stats.
async fn healthy_peers(conn: &mut AsyncPgConnection, queue: &str, n: usize) -> Vec<String> {
    let metrics = Arc::new(Recording::default());
    let mut peers = Vec::with_capacity(n);
    for i in 0..n {
        let peer = unique_id(&format!("w-ok{i}"));
        register(conn, &peer, queue).await;
        let flagged = tick(
            conn,
            &peer,
            &probe(queue, window(100, None), Arc::clone(&metrics)),
        )
        .await;
        assert!(flagged.is_empty(), "a healthy peer is not flagged");
        peers.push(peer);
    }
    peers
}

async fn count_stats_rows(conn: &mut AsyncPgConnection, worker_id: &str) -> i64 {
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_worker_task_stats WHERE worker_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .get_result(conn)
    .await
    .expect("count");
    row.n
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
    // The unique queue makes a cohort of this test's workers only. Other
    // tests that share the database take no part in the comparison.
    let queue = unique_id("outlier-q");
    let sick = unique_id("w-sick");
    register(&mut conn, &sick, &queue).await;
    // Peers publish first, so the sick worker sees a full cohort.
    let peers = healthy_peers(&mut conn, &queue, 3).await;

    let sick_metrics = Arc::new(Recording::default());
    let flagged = tick(
        &mut conn,
        &sick,
        &probe(&queue, window(100, Some(2)), Arc::clone(&sick_metrics)),
    )
    .await;
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

    // A peer that ticks again, now with the sick worker in the cohort, stays 0.
    let again = Arc::new(Recording::default());
    let flagged = tick(
        &mut conn,
        &peers[0],
        &probe(&queue, window(100, None), Arc::clone(&again)),
    )
    .await;
    assert_eq!(flagged, Vec::<OutlierDimension>::new());
    assert!(again.has(&Sample::Outlier {
        dimension: OutlierDimension::FailureRatio,
        flagged: false
    }));

    // The stored row is the snapshot the sick worker published.
    let rows = workers::load_live_worker_task_stats(&mut conn, 60, None)
        .await
        .expect("load stats");
    assert_eq!(
        find(&rows, &sick),
        Some(WorkerTaskStats {
            tasks: 100,
            failures: 50,
            p99_latency_ms: Some(20)
        })
    );
    let stored = rows
        .iter()
        .find(|row| row.worker_id == sick)
        .map(|row| row.cohort.clone());
    assert_eq!(
        stored,
        Some(cohort(&queue)),
        "the cohort is the worker's own key"
    );
}

/// A sick worker whose peers all poll another queue has no peers in its
/// cohort, so it is not flagged.
#[tokio::test]
async fn outlier_tick_compares_only_within_the_queue_cohort() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let sick = unique_id("w-sick");
    let slow = unique_id("cohort-slow");
    register(&mut conn, &sick, &slow).await;
    healthy_peers(&mut conn, &unique_id("cohort-fast"), 3).await;

    let metrics = Arc::new(Recording::default());
    let flagged = tick(
        &mut conn,
        &sick,
        &probe(&slow, window(100, Some(2)), metrics),
    )
    .await;
    assert_eq!(flagged, Vec::<OutlierDimension>::new());
}

/// Two workers in one process share the gauge. A healthy worker's tick keeps
/// the sick worker's flag, because the gauge reports the OR of both verdicts.
#[tokio::test]
async fn a_healthy_local_worker_does_not_clear_a_sick_workers_flag() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("process-q");
    let sick = unique_id("w-sick");
    register(&mut conn, &sick, &queue).await;
    let peers = healthy_peers(&mut conn, &queue, 3).await;

    let shared = Arc::new(workers::ProcessOutlierFlags::default());
    let metrics = Arc::new(Recording::default());
    let mut sick_probe = probe(&queue, window(100, Some(2)), Arc::clone(&metrics));
    sick_probe.process_flags = Arc::clone(&shared);
    let mut peer_probe = probe(&queue, window(100, None), Arc::clone(&metrics));
    peer_probe.process_flags = Arc::clone(&shared);

    assert_eq!(
        tick(&mut conn, &sick, &sick_probe).await,
        vec![OutlierDimension::FailureRatio]
    );
    assert_eq!(
        tick(&mut conn, &peers[0], &peer_probe).await,
        Vec::<OutlierDimension>::new()
    );
    let last_failure_ratio = metrics
        .samples()
        .into_iter()
        .filter_map(|s| match s {
            Sample::Outlier {
                dimension: OutlierDimension::FailureRatio,
                flagged,
            } => Some(flagged),
            _ => None,
        })
        .next_back();
    assert_eq!(last_failure_ratio, Some(true), "the OR keeps the sick flag");

    sick_probe.clear_gauge(&sick);
    assert!(metrics.samples().ends_with(&[
        Sample::Outlier {
            dimension: OutlierDimension::FailureRatio,
            flagged: false
        },
        Sample::Outlier {
            dimension: OutlierDimension::LatencyP99,
            flagged: false
        },
    ]));
}

/// A draining worker reads 0. A heartbeat that does not compare publishes its
/// stats and leaves the gauge alone.
#[tokio::test]
async fn draining_or_non_comparing_ticks_never_flag() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("drain-q");
    let sick = unique_id("w-sick");
    register(&mut conn, &sick, &queue).await;
    healthy_peers(&mut conn, &queue, 3).await;

    let draining = Arc::new(Recording::default());
    let flagged = workers::run_outlier_tick(
        &mut conn,
        &sick,
        &probe(&queue, window(100, Some(2)), Arc::clone(&draining)),
        true,
    )
    .await
    .expect("draining tick");
    assert_eq!(flagged, Vec::<OutlierDimension>::new());
    assert!(draining.has(&Sample::Outlier {
        dimension: OutlierDimension::FailureRatio,
        flagged: false
    }));

    let quiet = Arc::new(Recording::default());
    let mut silent = probe(&queue, window(100, Some(2)), Arc::clone(&quiet));
    silent.compare = false;
    let flagged = tick(&mut conn, &sick, &silent).await;
    assert_eq!(flagged, Vec::<OutlierDimension>::new());
    assert_eq!(quiet.samples(), Vec::new(), "no comparison, no gauge write");
    let rows = workers::load_live_worker_task_stats(&mut conn, 60, None)
        .await
        .expect("load");
    assert_eq!(
        find(&rows, &sick).map(|s| s.failures),
        Some(50),
        "the stats are still published"
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
    workers::upsert_worker_task_stats(&mut conn, &active, &cohort(&queue), &stats)
        .await
        .expect("upsert active");
    workers::upsert_worker_task_stats(&mut conn, &draining, &cohort(&queue), &stats)
        .await
        .expect("upsert draining");
    diesel::sql_query("UPDATE harvest_workers SET status = 'Draining' WHERE worker_id = $1")
        .bind::<diesel::sql_types::Text, _>(&draining)
        .execute(&mut conn)
        .await
        .expect("drain");

    let rows = workers::load_live_worker_task_stats(&mut conn, 60, None)
        .await
        .expect("load");
    assert!(find(&rows, &active).is_some());
    assert!(
        find(&rows, &draining).is_none(),
        "a draining worker is not a peer"
    );

    // A second upsert replaces the row.
    let newer = WorkerTaskStats { tasks: 40, ..stats };
    workers::upsert_worker_task_stats(&mut conn, &active, &cohort(&queue), &newer)
        .await
        .expect("upsert again");
    let rows = workers::load_live_worker_task_stats(&mut conn, 60, None)
        .await
        .expect("load");
    assert_eq!(find(&rows, &active), Some(newer));

    diesel::sql_query("DELETE FROM harvest_workers WHERE worker_id = $1")
        .bind::<diesel::sql_types::Text, _>(&active)
        .execute(&mut conn)
        .await
        .expect("delete worker");
    assert_eq!(
        count_stats_rows(&mut conn, &active).await,
        0,
        "the FK cascade drops the stats row"
    );
}

/// A heartbeat reads only the rows of its own cohort. Without a cohort, the
/// read returns every cohort, as `GET /admin/status` needs.
#[tokio::test]
async fn the_heartbeat_reads_only_its_own_cohort() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let mine = unique_id("mine-q");
    let other = unique_id("other-q");
    let stats = WorkerTaskStats {
        tasks: 30,
        failures: 0,
        p99_latency_ms: Some(5),
    };
    let mut ids = Vec::new();
    for queue in [&mine, &mine, &other] {
        let id = unique_id("w-cohort");
        register(&mut conn, &id, queue).await;
        workers::upsert_worker_task_stats(&mut conn, &id, &cohort(queue), &stats)
            .await
            .expect("upsert");
        ids.push(id);
    }

    let own = workers::load_live_worker_task_stats(&mut conn, 60, Some(&cohort(&mine)))
        .await
        .expect("load own cohort");
    let own_ids: Vec<&str> = own.iter().map(|r| r.worker_id.as_str()).collect();
    assert!(own_ids.contains(&ids[0].as_str()) && own_ids.contains(&ids[1].as_str()));
    assert!(
        own.iter().all(|r| r.cohort == cohort(&mine)),
        "only the own cohort is read: {own_ids:?}"
    );

    let all = workers::load_live_worker_task_stats(&mut conn, 60, None)
        .await
        .expect("load every cohort");
    assert!(
        find(&all, &ids[2]).is_some(),
        "the full read keeps other cohorts"
    );
}

/// Two heartbeats of one worker can write one row, as colocated shards that
/// share a database do. A snapshot captured earlier but written later does
/// not replace the newer one.
#[tokio::test]
async fn a_late_write_of_an_older_snapshot_is_dropped() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("late-q");
    let id = unique_id("w-late");
    register(&mut conn, &id, &queue).await;
    let (older, older_seq) = workers::capture_task_stats(&window(40, None));
    let (newer, newer_seq) = workers::capture_task_stats(&window(60, Some(2)));
    assert!(
        newer_seq > older_seq,
        "a later capture has a higher sequence"
    );

    let first =
        workers::write_task_stats_snapshot(&mut conn, &id, &cohort(&queue), &newer, newer_seq)
            .await
            .expect("write the newer snapshot");
    assert_eq!(first, workers::SnapshotWrite::Stored);
    let late =
        workers::write_task_stats_snapshot(&mut conn, &id, &cohort(&queue), &older, older_seq)
            .await
            .expect("write the older snapshot");
    assert_eq!(late, workers::SnapshotWrite::Superseded);

    let rows = workers::load_live_worker_task_stats(&mut conn, 60, None)
        .await
        .expect("load");
    let row = rows
        .iter()
        .find(|r| r.worker_id == id)
        .expect("the row is live");
    assert_eq!(row.stats, newer, "the newer snapshot stays");
    assert_eq!(row.snapshot_seq, newer_seq);
}

/// A heartbeat judges the worker on its newest snapshot. Another shard
/// heartbeat of the worker can hold a newer self row than this tick wrote,
/// for example when this tick's write lost to it. The tick then compares that
/// row, not its own older capture.
#[tokio::test]
async fn a_tick_judges_the_newest_self_row() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("fresh-q");
    let me = unique_id("w-fresh");
    register(&mut conn, &me, &queue).await;
    healthy_peers(&mut conn, &queue, 3).await;

    // Another shard heartbeat of this worker holds a newer, sick self row.
    let metrics = Arc::new(Recording::default());
    let late = probe(&queue, window(100, None), Arc::clone(&metrics));
    let sick = WorkerTaskStats {
        tasks: 100,
        failures: 50,
        p99_latency_ms: Some(20),
    };
    late.shard_peers.store(
        1,
        vec![LiveWorkerTaskStats {
            worker_id: me.clone(),
            cohort: cohort(&queue),
            stats: sick,
            snapshot_seq: i64::MAX,
        }],
    );

    // This tick captured a healthy window, but the newest self row is sick.
    let flagged = tick(&mut conn, &me, &late).await;
    assert_eq!(flagged, vec![OutlierDimension::FailureRatio]);
}

/// A cohort key can be long, for a worker with many queues or labels. The
/// stats write still succeeds, because the index does not hold the key.
#[tokio::test]
async fn a_long_cohort_key_still_stores() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("long-q");
    let id = unique_id("w-long");
    register(&mut conn, &id, &queue).await;
    let queues: Vec<String> = (0..400).map(|_| uuid::Uuid::new_v4().to_string()).collect();
    let key = workers::worker_cohort(&workers::CohortPolicy {
        queues: &queues,
        queue_weights: &HashMap::new(),
        build_id: "",
        labels: &HashMap::new(),
        slots: workers::SlotPolicy::of(1, 1, None),
        session_slots: 0,
        priority_aging_secs: None,
        ineligible_activities: &[],
        shard_assignments: &[],
        registered_workflows: &[],
        registered_activities: &[],
        circuit_breakers: &autumn_harvest::circuit_breaker::CircuitBreakerRegistry::empty(),
        dispatch_channel: false,
        retry_budgets: &autumn_harvest::retry_budget::RetryBudgetConfig::default(),
        outcome_window: std::time::Duration::from_secs(300),
        peer_stale_secs: 120,
        execution: workers::ExecutionPolicy::default(),
        payload: workers::PayloadPolicy::default(),
    });
    assert!(key.len() > 10_000, "the key is long: {}", key.len());
    workers::upsert_worker_task_stats(&mut conn, &id, &key, &WorkerTaskStats::default())
        .await
        .expect("a long cohort key stores");
    let rows = workers::load_live_worker_task_stats(&mut conn, 60, Some(&key))
        .await
        .expect("load by the long key");
    assert!(rows.iter().any(|r| r.worker_id == id));
}

/// A frozen stats row leaves the live set. The prune deletes a row once it
/// outlives the retention.
#[tokio::test]
async fn frozen_stats_leave_the_live_set_and_old_rows_are_pruned() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("frozen-q");
    let frozen = unique_id("w-frozen");
    let ancient = unique_id("w-ancient");
    let stats = WorkerTaskStats {
        tasks: 30,
        failures: 15,
        p99_latency_ms: Some(10),
    };
    for (id, age_secs) in [(&frozen, 120_i64), (&ancient, 7_200)] {
        register(&mut conn, id, &queue).await;
        workers::upsert_worker_task_stats(&mut conn, id, &cohort(&queue), &stats)
            .await
            .expect("upsert");
        diesel::sql_query(
            "UPDATE harvest_worker_task_stats \
             SET updated_at = NOW() - ($2::bigint * INTERVAL '1 second') WHERE worker_id = $1",
        )
        .bind::<diesel::sql_types::Text, _>(id)
        .bind::<diesel::sql_types::BigInt, _>(age_secs)
        .execute(&mut conn)
        .await
        .expect("age the row");
    }

    let rows = workers::load_live_worker_task_stats(&mut conn, 60, None)
        .await
        .expect("load");
    assert_eq!(find(&rows, &frozen), None, "a frozen row is not live");

    // A fleet with a slow heartbeat counts a row as live for longer than the
    // default retention, so the prune keeps it.
    workers::prune_worker_task_stats(&mut conn, 3 * 3_600)
        .await
        .expect("prune with a slow fleet");
    assert_eq!(
        count_stats_rows(&mut conn, &ancient).await,
        1,
        "inside the slow fleet's freshness window"
    );

    workers::prune_worker_task_stats(&mut conn, 60)
        .await
        .expect("prune");
    assert_eq!(
        count_stats_rows(&mut conn, &frozen).await,
        1,
        "inside the retention"
    );
    assert_eq!(
        count_stats_rows(&mut conn, &ancient).await,
        0,
        "past the retention"
    );
}

/// A restarted worker keeps its id. Its new process can start with a lower
/// sequence, for example on a host with a slower clock. The upsert still
/// stores a sequence above the old process's row, and the process counter
/// continues above it.
#[tokio::test]
async fn a_restarted_worker_outranks_the_rows_of_its_previous_process() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("restart-q");
    let id = unique_id("w-restart");
    register(&mut conn, &id, &queue).await;
    let stats = WorkerTaskStats {
        tasks: 30,
        failures: 0,
        p99_latency_ms: Some(5),
    };
    workers::upsert_worker_task_stats(&mut conn, &id, &cohort(&queue), &stats)
        .await
        .expect("first upsert");
    // The previous process ran on a host whose clock was far ahead.
    let previous = workers::next_snapshot_seq() + 1_000_000_000_000;
    diesel::sql_query(
        "UPDATE harvest_worker_task_stats SET snapshot_seq = $2 WHERE worker_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&id)
    .bind::<diesel::sql_types::BigInt, _>(previous)
    .execute(&mut conn)
    .await
    .expect("seed the previous process's row");

    workers::upsert_worker_task_stats(&mut conn, &id, &cohort(&queue), &stats)
        .await
        .expect("upsert after the restart");
    let rows = workers::load_live_worker_task_stats(&mut conn, 60, None)
        .await
        .expect("load");
    let stored = rows
        .iter()
        .find(|r| r.worker_id == id)
        .expect("the row is live")
        .snapshot_seq;
    assert!(stored > previous, "{stored} must exceed {previous}");
    assert!(
        workers::next_snapshot_seq() > stored,
        "the process counter continues above the stored sequence"
    );
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
        &cohort("ghost-q"),
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
    ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ACTIVITY_CALLS.fetch_add(1, Ordering::SeqCst);
        // One heartbeat, then a wait past the one-second flush interval, so
        // the flusher records the `heartbeat` op and its pool wait.
        ctx.heartbeat(serde_json::json!({ "step": 1 }))
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(Duration::from_millis(1_300)).await;
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
    assert!(
        metrics.samples().iter().any(|s| matches!(
            s,
            Sample::Pool { shard: 0, in_use, idle } if in_use + idle > 0
        )),
        "the pool sampler reports the open connections"
    );
    for wanted in [
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
    // A single-pool worker tags every wait with its own pool. The
    // process-global sharded pool does not change the label.
    assert!(
        metrics
            .samples()
            .iter()
            .all(|s| !matches!(s, Sample::PoolWait { shard } if *shard != 0)),
        "every pool wait carries the label of the worker pool"
    );

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

/// Records the shard label of each pool wait and each scan op.
#[derive(Default)]
struct ScanShards {
    waits: Mutex<Vec<u16>>,
    scans: Mutex<Vec<u16>>,
}

impl MetricsRecorder for ScanShards {
    fn record_db_pool_wait(&self, shard: u16, _seconds: f64) {
        self.waits.lock().expect("lock").push(shard);
    }

    fn record_db_query_duration(&self, op: DbOp, shard: u16, _seconds: f64) {
        if op == DbOp::Scan {
            self.scans.lock().expect("lock").push(shard);
        }
    }
}

/// Issue #1815: a checker for a nonzero shard has no pool shard of its own.
/// It records its pool wait and its scan under the same shard, so the
/// per-shard alert and panels see one shard.
#[tokio::test]
async fn a_shard_checker_records_its_scan_under_its_shard() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let recorder = Arc::new(ScanShards::default());
    let telemetry = Arc::new(TelemetryConfig {
        metrics: Arc::clone(&recorder) as Arc<dyn MetricsRecorder>,
        ..Default::default()
    });
    let cancel = tokio_util::sync::CancellationToken::new();
    let handle = timeout::spawn_timeout_checker_for_shard(
        pool,
        cancel.clone(),
        // Long enough to open a connection: a slower acquire skips the pass.
        Duration::from_millis(500),
        telemetry,
        Duration::from_secs(5),
        None,
        vec![ShardId::new(3)],
        Arc::new(autumn_harvest::circuit_breaker::CircuitBreakerRegistry::default()),
        None,
        60,
        Some(ShardId::new(3)),
        autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    );

    let deadline = Instant::now() + Duration::from_secs(20);
    while recorder.scans.lock().expect("lock").is_empty() {
        assert!(
            Instant::now() < deadline,
            "no scan pass recorded; pool waits: {:?}",
            recorder.waits.lock().expect("lock")
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cancel.cancel();
    let _ = handle.await;

    let waits = recorder.waits.lock().expect("lock").clone();
    let scans = recorder.scans.lock().expect("lock").clone();
    assert!(
        waits.iter().all(|shard| *shard == 3),
        "pool waits: {waits:?}"
    );
    assert!(scans.iter().all(|shard| *shard == 3), "scans: {scans:?}");
}

/// Issue #1815: `GET /admin/status` keeps each cohort's rows for as long as
/// that cohort's own heartbeat does. A fast API runtime must not drop a slow
/// cohort's rows between its heartbeats.
#[tokio::test]
async fn status_keeps_each_cohort_for_its_own_freshness_window() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let queue = unique_id("slow-q");
    let id = unique_id("w-slow");
    register(&mut conn, &id, &queue).await;
    let stats = WorkerTaskStats {
        tasks: 30,
        failures: 0,
        p99_latency_ms: Some(10),
    };
    // `cohort` records a peer freshness limit of 120 seconds.
    workers::upsert_worker_task_stats(&mut conn, &id, &cohort(&queue), &stats)
        .await
        .expect("upsert");
    for table in ["harvest_worker_task_stats", "harvest_workers"] {
        let column = if table == "harvest_workers" {
            "last_heartbeat_at"
        } else {
            "updated_at"
        };
        diesel::sql_query(format!(
            "UPDATE {table} SET {column} = NOW() - INTERVAL '60 seconds' WHERE worker_id = $1"
        ))
        .bind::<diesel::sql_types::Text, _>(&id)
        .execute(&mut conn)
        .await
        .expect("age the row");
    }

    let api_threshold_secs = 10;
    let fixed = workers::load_live_worker_task_stats(&mut conn, api_threshold_secs, None)
        .await
        .expect("load with one threshold");
    assert_eq!(find(&fixed, &id), None, "one fast threshold drops the row");

    let per_cohort = workers::load_live_worker_task_stats_per_cohort(&mut conn, api_threshold_secs)
        .await
        .expect("load per cohort");
    assert_eq!(
        find(&per_cohort, &id),
        Some(stats),
        "the cohort's own 120 second limit keeps the row"
    );
}
