//! One timeout scanner per shard, bounded scans, and failover (issue #1795).
//!
//! Before this change, every replica ran the timeout pass on every tick. Scan
//! load grew with fleet size. These tests prove three properties against a
//! real database:
//!
//! 1. Three checkers on one shard run about one checker's worth of passes.
//! 2. A task-timeout scan returns at most one batch per reason per pass. It
//!    still reaches every expired row and drains a backlog.
//! 3. When the lease holder dies, a standby takes over within the lease TTL.
//!    A graceful stop hands over at once.
#![cfg(feature = "db")]

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::scanner_lease::ScannerCoordination;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::timeout::{self, TimeoutReason, TimeoutScanCursor};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use diesel::QueryableByName;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;

/// Prefer an operator-supplied database and fall back to testcontainers.
async fn setup_test_db_url() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        use diesel_async::{AsyncConnection, SimpleAsyncConnection};
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        let already_migrated = conn
            .batch_execute("SELECT 1 FROM harvest_scanner_leases LIMIT 0")
            .await
            .is_ok();
        if !already_migrated {
            let mut fresh = AsyncPgConnection::establish(&url)
                .await
                .expect("HARVEST_TEST_DATABASE_URL must be reachable");
            fresh
                .batch_execute(&autumn_harvest::test_init_sql())
                .await
                .expect("migrations should apply");
        }
        return (url, None);
    }

    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    DbPool::builder(manager)
        .max_size(8)
        .build()
        .expect("failed to build pool")
}

/// Counts `harvest.scanner.pass` samples by role, and loop ticks.
#[derive(Default)]
struct PassRecorder {
    roles: Mutex<BTreeMap<String, usize>>,
    ticks: Mutex<usize>,
}

impl PassRecorder {
    fn role(&self, role: &str) -> usize {
        self.roles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(role)
            .copied()
            .unwrap_or(0)
    }

    /// Passes that ran the enforcement work, whatever the reason.
    fn ran(&self) -> usize {
        self.role("leader") + self.role("unelected") + self.role("fail_open")
    }

    fn ticks(&self) -> usize {
        *self
            .ticks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl MetricsRecorder for PassRecorder {
    fn record_scanner_pass(&self, scanner: &str, shard: &str, role: &str) {
        let _ = shard;
        if scanner == "timeout" {
            *self
                .roles
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(role.to_owned())
                .or_default() += 1;
        }
    }

    fn record_scanner_tick(&self, scanner: &str, shard: &str) {
        let _ = shard;
        if scanner == "timeout" {
            *self
                .ticks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        }
    }
}

/// One spawned checker: its holder id, metrics, and task handle.
struct Checker {
    holder: String,
    metrics: Arc<PassRecorder>,
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

fn spawn_checker(
    pool: &DbPool,
    shard: ShardId,
    holder: &str,
    interval: Duration,
    lease_ttl: Duration,
) -> Checker {
    let metrics = Arc::new(PassRecorder::default());
    let telemetry = Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: metrics.clone(),
        ..Default::default()
    });
    let cancel = CancellationToken::new();
    let handle = timeout::spawn_coordinated_timeout_checker_for_shard(
        pool.clone(),
        cancel.clone(),
        interval,
        telemetry,
        Duration::from_secs(5),
        None,
        vec![shard],
        Arc::new(autumn_harvest::circuit_breaker::CircuitBreakerRegistry::default()),
        None,
        60,
        Some(shard),
        autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
        ScannerCoordination {
            holder: Some(holder.to_owned()),
            lease_ttl,
            jitter: 0.2,
        },
        timeout::DEFAULT_TIMEOUT_SCAN_BATCH_SIZE,
    );
    Checker {
        holder: holder.to_owned(),
        metrics,
        cancel,
        handle,
    }
}

#[derive(QueryableByName)]
struct LeaseRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    holder: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    epoch: i64,
}

/// The live holder of the `timeout` lease on `shard`, if any.
async fn live_holder(pool: &DbPool, shard: ShardId) -> Option<(String, i64)> {
    let mut conn = pool.get().await.expect("connection");
    let rows: Vec<LeaseRow> = diesel::sql_query(
        "SELECT holder, epoch FROM harvest_scanner_leases \
         WHERE shard_id = $1 AND scanner = 'timeout' AND lease_until > NOW()",
    )
    .bind::<diesel::sql_types::Integer, _>(shard.as_i32())
    .load(&mut conn)
    .await
    .expect("lease query");
    rows.into_iter().next().map(|r| (r.holder, r.epoch))
}

async fn wait_for<F: Fn() -> bool>(what: &str, limit: Duration, done: F) {
    let deadline = Instant::now() + limit;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// AC1: three checkers on one shard run about one pass per tick, not three.
#[tokio::test]
async fn three_checkers_on_one_shard_run_about_one_pass_per_tick() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let shard = ShardId::new(17_951);
    let interval = Duration::from_millis(100);

    let checkers: Vec<Checker> = ["w1", "w2", "w3"]
        .into_iter()
        .map(|h| spawn_checker(&pool, shard, h, interval, Duration::from_secs(5)))
        .collect();

    wait_for("20 ticks on every checker", Duration::from_secs(30), || {
        checkers.iter().all(|c| c.metrics.ticks() >= 20)
    })
    .await;

    let ran: usize = checkers.iter().map(|c| c.metrics.ran()).sum();
    let standby: usize = checkers.iter().map(|c| c.metrics.role("standby")).sum();
    let most_ticks = checkers.iter().map(|c| c.metrics.ticks()).max().unwrap();

    for c in &checkers {
        c.cancel.cancel();
    }
    for c in checkers {
        let _ = c.handle.await;
    }

    // Unelected, the three checkers run about 3 × `most_ticks` passes.
    // Elected, one checker runs them. The bound leaves room for the
    // checkers that tick a few more times before the window closes.
    assert!(
        ran * 2 <= most_ticks * 3,
        "expected about one checker's worth of passes, got {ran} passes over \
         {most_ticks} ticks"
    );
    assert!(ran > 0, "the leader must still run passes");
    assert!(standby > 0, "the other checkers must stand by");
}

/// Inserts `n` RUNNING activity rows whose start-to-close budget has run out.
async fn insert_expired_running_tasks(conn: &mut AsyncPgConnection, n: usize) -> Vec<uuid::Uuid> {
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let id = uuid::Uuid::new_v4();
        diesel::sql_query(
            "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, input, state, attempt, max_attempts, \
              started_at, start_to_close) \
             VALUES ($1, 'scanner-lease-batch', 'activity', '{}'::jsonb, 'RUNNING', \
                     1, 1, NOW() - INTERVAL '1 minute', INTERVAL '1 second')",
        )
        .bind::<diesel::sql_types::Uuid, _>(id)
        .execute(conn)
        .await
        .expect("insert expired task");
        ids.push(id);
    }
    ids
}

/// AC2: a backlog larger than the batch is read one bounded batch per pass.
/// The keyset cursor reaches every row once per cycle, and failing each
/// batch drains the backlog.
#[tokio::test]
async fn timeout_scan_is_bounded_per_pass_and_converges() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("connection");
    let ours: HashSet<uuid::Uuid> = insert_expired_running_tasks(&mut conn, 7)
        .await
        .into_iter()
        .collect();
    let limit = 3;

    // Rows that stay expired: three passes must visit each row exactly once.
    let mut cursor = TimeoutScanCursor::default();
    let mut seen = Vec::new();
    for _ in 0..3 {
        let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, limit)
            .await
            .expect("batch scan");
        let start_to_close: Vec<_> = page
            .iter()
            .filter(|(_, r)| *r == TimeoutReason::StartToClose)
            .collect();
        assert!(
            start_to_close.len() <= usize::try_from(limit).unwrap(),
            "a pass must read at most one batch per reason, got {}",
            start_to_close.len()
        );
        seen.extend(
            start_to_close
                .iter()
                .map(|(t, _)| t.id)
                .filter(|id| ours.contains(id)),
        );
    }
    let distinct: HashSet<_> = seen.iter().copied().collect();
    assert_eq!(
        seen.len(),
        distinct.len(),
        "no row is read twice in a cycle"
    );
    assert_eq!(distinct, ours, "one cycle must reach every expired row");

    // Rows that leave the predicate: the backlog drains in a bounded number
    // of passes.
    let mut cursor = TimeoutScanCursor::default();
    let mut passes = 0;
    loop {
        let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, limit)
            .await
            .expect("batch scan");
        let mine: Vec<_> = page.iter().filter(|(t, _)| ours.contains(&t.id)).collect();
        if mine.is_empty() && passes > 0 {
            break;
        }
        for (task, _) in mine {
            autumn_harvest::queue::fail_task(&mut conn, task.id, "timed out")
                .await
                .expect("fail task");
        }
        passes += 1;
        assert!(
            passes <= 4,
            "the backlog must drain within ceil(7 / 3) + 1 passes"
        );
    }
}

/// AC3: kill the lease holder. A standby takes over within the lease TTL.
/// Then stop the new holder gracefully. A standby takes over at once.
#[tokio::test]
async fn standby_takes_over_within_lease_ttl() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let shard = ShardId::new(17_953);
    let interval = Duration::from_millis(100);
    let ttl = Duration::from_secs(2);

    let mut checkers: Vec<Checker> = ["k1", "k2", "k3"]
        .into_iter()
        .map(|h| spawn_checker(&pool, shard, h, interval, ttl))
        .collect();

    let deadline = Instant::now() + Duration::from_secs(20);
    let (first, first_epoch) = loop {
        if let Some(held) = live_holder(&pool, shard).await {
            break held;
        }
        assert!(Instant::now() < deadline, "no checker took the lease");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // Kill, not stop: an abort skips the graceful release.
    let idx = checkers.iter().position(|c| c.holder == first).unwrap();
    let killed = checkers.remove(idx);
    killed.handle.abort();
    let _ = killed.handle.await;
    let killed_at = Instant::now();

    let (second, second_epoch) = loop {
        if let Some((holder, epoch)) = live_holder(&pool, shard).await
            && holder != first
        {
            break (holder, epoch);
        }
        assert!(
            killed_at.elapsed() < ttl + Duration::from_secs(2),
            "no standby took over within the lease TTL"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(second_epoch > first_epoch, "a takeover must bump the epoch");

    let leader = checkers.iter().find(|c| c.holder == second).unwrap();
    let before = leader.metrics.role("leader");
    wait_for("passes on the new leader", ttl, || {
        leader.metrics.role("leader") > before
    })
    .await;

    // Graceful stop: the holder releases the lease, so the last standby leads
    // well inside the TTL.
    let idx = checkers.iter().position(|c| c.holder == second).unwrap();
    let stopped = checkers.remove(idx);
    stopped.cancel.cancel();
    let _ = stopped.handle.await;
    let stopped_at = Instant::now();
    let last = &checkers[0];
    loop {
        if let Some((holder, _)) = live_holder(&pool, shard).await
            && holder == last.holder
        {
            break;
        }
        assert!(
            stopped_at.elapsed() < ttl,
            "a graceful stop must hand over before the TTL runs out"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    for c in checkers {
        c.cancel.cancel();
        let _ = c.handle.await;
    }
}
