//! One timeout scanner per shard, bounded scans, and failover (issue #1795).
//!
//! Before issue #1795, every replica ran the timeout pass on every tick. Scan
//! load grew with fleet size. These tests prove the fix against a real
//! database:
//!
//! 1. Three checkers on one shard run about one checker's worth of passes.
//! 2. A task-timeout scan returns at most one batch per reason per pass. It
//!    still reaches every expired row and drains a backlog. The spawned
//!    checker enforces one batch per pass.
//! 3. When the lease holder dies, a standby takes over within the lease TTL.
//!    A graceful stop hands over at once.
//! 4. A failed lease query fails open, and a standby still refreshes its
//!    codec key.
#![cfg(feature = "db")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::payload_codec::{CodecError, PayloadCodec, PayloadCodecs};
use autumn_harvest::scanner_lease::{
    ScannerCoordination, effective_lease_ttl, max_jittered_interval,
};
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

/// The jitter every checker in this file uses.
const JITTER: f64 = 0.2;

/// Prefer an operator-supplied database and fall back to testcontainers.
async fn setup_test_db_url() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        use diesel_async::{AsyncConnection, SimpleAsyncConnection};
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        // Several suites share one operator-supplied database, so apply the
        // bundle only once. A failed probe poisons the connection, so the
        // apply runs on a fresh one.
        let migrated = conn
            .batch_execute("SELECT 1 FROM harvest_workflow_executions LIMIT 0")
            .await
            .is_ok();
        if !migrated {
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

/// How to spawn one test checker.
struct Spec<'a> {
    shard: ShardId,
    holder: &'a str,
    interval: Duration,
    lease_ttl: Duration,
    batch: u32,
    codecs: PayloadCodecs,
    /// The shards this checker scans. Defaults to `[shard]`.
    scope: Vec<ShardId>,
}

impl<'a> Spec<'a> {
    fn new(shard: ShardId, holder: &'a str, interval: Duration, lease_ttl: Duration) -> Self {
        Self {
            shard,
            holder,
            interval,
            lease_ttl,
            batch: timeout::DEFAULT_TIMEOUT_SCAN_BATCH_SIZE,
            codecs: PayloadCodecs::default(),
            scope: vec![shard],
        }
    }
}

fn spawn_checker(pool: &DbPool, spec: Spec<'_>) -> Checker {
    let metrics = Arc::new(PassRecorder::default());
    let telemetry = Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: metrics.clone(),
        ..Default::default()
    });
    let cancel = CancellationToken::new();
    let handle = timeout::spawn_coordinated_timeout_checker_for_shard(
        pool.clone(),
        cancel.clone(),
        spec.interval,
        telemetry,
        Duration::from_secs(5),
        None,
        spec.scope,
        Arc::new(autumn_harvest::circuit_breaker::CircuitBreakerRegistry::default()),
        None,
        60,
        Some(spec.shard),
        spec.codecs,
        0,
        ScannerCoordination {
            holder: Some(spec.holder.to_owned()),
            lease_ttl: spec.lease_ttl,
            jitter: JITTER,
        },
        spec.batch,
    );
    Checker {
        holder: spec.holder.to_owned(),
        metrics,
        cancel,
        handle,
    }
}

async fn stop_all(checkers: Vec<Checker>) {
    for c in &checkers {
        c.cancel.cancel();
    }
    for c in checkers {
        let _ = c.handle.await;
    }
}

#[derive(QueryableByName)]
struct LeaseRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    holder: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    epoch: i64,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    live: bool,
}

/// The `timeout` lease row on `shard`, live or not.
async fn lease_row(pool: &DbPool, shard: ShardId) -> Option<LeaseRow> {
    let mut conn = pool.get().await.expect("connection");
    let rows: Vec<LeaseRow> = diesel::sql_query(
        "SELECT holder, epoch, lease_until > NOW() AS live FROM harvest_scanner_leases \
         WHERE shard_id = $1 AND scanner = 'timeout'",
    )
    .bind::<diesel::sql_types::Integer, _>(shard.as_i32())
    .load(&mut conn)
    .await
    .expect("lease query");
    rows.into_iter().next()
}

/// The live holder of the `timeout` lease on `shard`, if any.
async fn live_holder(pool: &DbPool, shard: ShardId) -> Option<(String, i64)> {
    lease_row(pool, shard)
        .await
        .filter(|r| r.live)
        .map(|r| (r.holder, r.epoch))
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
        .map(|h| spawn_checker(&pool, Spec::new(shard, h, interval, Duration::from_secs(5))))
        .collect();

    wait_for("20 ticks on every checker", Duration::from_secs(30), || {
        checkers.iter().all(|c| c.metrics.ticks() >= 20)
    })
    .await;

    let ran: usize = checkers.iter().map(|c| c.metrics.ran()).sum();
    let standby: usize = checkers.iter().map(|c| c.metrics.role("standby")).sum();
    let most_ticks = checkers.iter().map(|c| c.metrics.ticks()).max().unwrap();
    let leader = checkers
        .iter()
        .max_by_key(|c| c.metrics.role("leader"))
        .unwrap();
    let leader_standby = leader.metrics.role("standby");
    stop_all(checkers).await;

    // Unelected, the three checkers run about 3 × `most_ticks` passes.
    // Elected, one checker runs them. The bound leaves room for the
    // checkers that tick a few more times before the window closes.
    assert!(
        ran * 2 <= most_ticks * 3,
        "expected about one checker's worth of passes, got {ran} passes over \
         {most_ticks} ticks"
    );
    assert!(standby > 0, "the other checkers must stand by");
    // The first winner keeps the lease for the whole window, which is
    // shorter than the TTL. A tick with no connection records no role.
    assert_eq!(leader_standby, 0, "the leader must never lose the lease");
}

/// Inserts `n` RUNNING activity rows on `queue` whose start-to-close budget
/// has run out. Removes earlier rows on `queue` first. Returns the ids in
/// creation order, which is the order a sweep reads them.
async fn insert_expired_running_tasks(
    conn: &mut AsyncPgConnection,
    queue: &str,
    n: usize,
) -> Vec<uuid::Uuid> {
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(conn)
        .await
        .expect("clear queue");
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let id = uuid::Uuid::new_v4();
        diesel::sql_query(
            "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, input, state, attempt, max_attempts, \
              started_at, start_to_close) \
             VALUES ($1, $2, 'activity', '{}'::jsonb, 'RUNNING', \
                     1, 1, NOW() - INTERVAL '1 minute', INTERVAL '1 second')",
        )
        .bind::<diesel::sql_types::Uuid, _>(id)
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(conn)
        .await
        .expect("insert expired task");
        ids.push(id);
    }
    ids
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// Expired start-to-close rows outside `queue`. The bound tests need none.
async fn foreign_expired_rows(conn: &mut AsyncPgConnection, queue: &str) -> i64 {
    let rows: Vec<Count> = diesel::sql_query(format!(
        "SELECT COUNT(*) AS n FROM ({}) q WHERE q.queue_name <> $1",
        timeout::start_to_close_timeout_query()
    ))
    .bind::<diesel::sql_types::Text, _>(queue)
    .load(conn)
    .await
    .expect("count");
    rows[0].n
}

/// Rows among `ids` still RUNNING.
async fn still_running(conn: &mut AsyncPgConnection, ids: &[uuid::Uuid]) -> i64 {
    let rows: Vec<Count> = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue WHERE id = ANY($1) AND state = 'RUNNING'",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(ids)
    .load(conn)
    .await
    .expect("count");
    rows[0].n
}

fn start_to_close_ids(
    page: &[(autumn_harvest::models::TaskQueueItem, TimeoutReason)],
) -> Vec<uuid::Uuid> {
    page.iter()
        .filter(|(_, r)| *r == TimeoutReason::StartToClose)
        .map(|(t, _)| t.id)
        .collect()
}

/// AC2: a backlog larger than the batch is read one bounded batch per pass.
/// The keyset cursor reaches every row once per sweep, wraps after a short
/// page, and failing each batch drains the backlog.
#[tokio::test]
async fn timeout_scan_is_bounded_per_pass_and_converges() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("connection");
    let queue = "scanner-lease-batch";
    let ours = insert_expired_running_tasks(&mut conn, queue, 7).await;
    assert_eq!(
        foreign_expired_rows(&mut conn, queue).await,
        0,
        "precondition: no other expired rows in this database"
    );
    let limit = 3;

    // Rows that stay expired: three passes visit each row once. The third
    // page is short, so the fourth pass wraps to the lowest ids.
    let mut cursor = TimeoutScanCursor::default();
    let mut pages = Vec::new();
    for _ in 0..4 {
        let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, limit)
            .await
            .expect("batch scan");
        let ids = start_to_close_ids(&page);
        assert!(
            ids.len() <= 3,
            "a pass must read at most one batch per reason, got {}",
            ids.len()
        );
        pages.push(ids);
    }
    // A batch loads in id order, so compare each page as a set.
    let sorted = |ids: &[uuid::Uuid]| {
        let mut ids = ids.to_vec();
        ids.sort();
        ids
    };
    for (page, rows) in pages[..3].iter().zip(ours.chunks(3)) {
        assert_eq!(
            sorted(page),
            sorted(rows),
            "one sweep reads every row once, in creation order"
        );
    }
    assert_eq!(
        sorted(&pages[3]),
        sorted(&ours[..3]),
        "a short page wraps the cursor"
    );

    // A limit below 1 counts as 1.
    let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut TimeoutScanCursor::default(), 0)
        .await
        .expect("batch scan");
    assert_eq!(start_to_close_ids(&page), ours[..1]);

    // Rows that leave the predicate: the backlog drains in ceil(7 / 3) passes.
    let mut cursor = TimeoutScanCursor::default();
    for _ in 0..3 {
        let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, limit)
            .await
            .expect("batch scan");
        for id in start_to_close_ids(&page) {
            autumn_harvest::queue::fail_task(&mut conn, id, "timed out")
                .await
                .expect("fail task");
        }
    }
    assert_eq!(
        still_running(&mut conn, &ours).await,
        0,
        "the backlog must drain"
    );
}

/// A sweep reads the rows created before it started, and stops there. A row
/// created during the sweep waits for the next one, even with the highest
/// id. So new rows cannot stretch the sweep.
#[tokio::test]
async fn a_row_created_during_a_sweep_waits_for_the_next() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("connection");
    let queue = "scanner-lease-high-water";
    // More rows than one refill holds at a batch of 1 (64 ids), so the
    // sweep takes a full refill and then a short one.
    let ours = insert_expired_running_tasks(&mut conn, queue, 70).await;
    assert_eq!(
        foreign_expired_rows(&mut conn, queue).await,
        0,
        "precondition: no other expired rows in this database"
    );

    let mut cursor = TimeoutScanCursor::default();
    let mut seen = Vec::new();
    let first = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, 1)
        .await
        .expect("batch scan");
    seen.extend(start_to_close_ids(&first));

    // An arrival above every id in the backlog, after the sweep started.
    let late = uuid::Uuid::from_u128(u128::MAX);
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, input, state, attempt, max_attempts, \
          started_at, start_to_close) \
         VALUES ($1, $2, 'activity', '{}'::jsonb, 'RUNNING', \
                 1, 1, NOW() - INTERVAL '1 minute', INTERVAL '1 second')",
    )
    .bind::<diesel::sql_types::Uuid, _>(late)
    .bind::<diesel::sql_types::Text, _>(queue)
    .execute(&mut conn)
    .await
    .expect("insert late task");

    for _ in 1..ours.len() {
        let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, 1)
            .await
            .expect("batch scan");
        seen.extend(start_to_close_ids(&page));
    }
    assert_eq!(
        seen, ours,
        "the sweep reads the backlog it started with, in creation order"
    );

    // The late row comes in the next sweep.
    for id in &ours {
        autumn_harvest::queue::fail_task(&mut conn, *id, "timed out")
            .await
            .expect("fail task");
    }
    let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, 1)
        .await
        .expect("batch scan");
    let next = start_to_close_ids(&page);

    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(&mut conn)
        .await
        .expect("clear queue");
    assert_eq!(next, [late], "the next sweep reaches the late row");
}

/// A sweep reads only the rows that were live at its start. Rows created
/// later wait for the next sweep. So they cannot stretch the sweep, even
/// when they are already expired.
#[tokio::test]
async fn arrivals_cannot_stretch_a_sweep() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("connection");
    let queue = "scanner-lease-budget";
    // At a batch of 1 a page holds 64 rows, so 70 rows give two pages:
    // passes 1-64 and 65-70. Pass 71 starts the next sweep.
    let ours = insert_expired_running_tasks(&mut conn, queue, 70).await;
    assert_eq!(
        foreign_expired_rows(&mut conn, queue).await,
        0,
        "precondition: no other expired rows in this database"
    );

    let mut cursor = TimeoutScanCursor::default();
    let first = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, 1)
        .await
        .expect("batch scan");
    assert_eq!(start_to_close_ids(&first), ours[..1]);

    // 100 arrivals that are already expired. Read in this sweep, they would
    // add two pages.
    let arrivals: Vec<uuid::Uuid> = (0..100).map(|_| uuid::Uuid::new_v4()).collect();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, input, state, attempt, max_attempts, \
          started_at, start_to_close) \
         SELECT u, $2, 'activity', '{}'::jsonb, 'RUNNING', \
                1, 1, NOW() - INTERVAL '1 minute', INTERVAL '1 second' \
         FROM unnest($1) AS u",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&arrivals)
    .bind::<diesel::sql_types::Text, _>(queue)
    .execute(&mut conn)
    .await
    .expect("insert arrivals");

    // The sweep wraps when the lowest id comes back.
    let mut wrapped_at = None;
    for pass in 2..=200 {
        let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, 1)
            .await
            .expect("batch scan");
        if start_to_close_ids(&page) == ours[..1] {
            wrapped_at = Some(pass);
            break;
        }
    }

    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(&mut conn)
        .await
        .expect("clear queue");
    assert_eq!(
        wrapped_at,
        Some(71),
        "the sweep must end at the rows live at its start, not read the arrivals"
    );
}

/// A sweep reads the rows that were expired when it started. Rows that
/// expire later cannot take their place. Otherwise a steady stream of
/// arrivals can push an old row out of every sweep.
#[tokio::test]
async fn later_expiries_cannot_displace_the_rows_a_sweep_counted() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("connection");
    let queue = "scanner-lease-snapshot";
    // At a batch of 1 a refill holds 64 ids, so 70 rows give two refills.
    let ours = insert_expired_running_tasks(&mut conn, queue, 70).await;
    assert_eq!(
        foreign_expired_rows(&mut conn, queue).await,
        0,
        "precondition: no other expired rows in this database"
    );

    let mut cursor = TimeoutScanCursor::default();
    let mut seen = start_to_close_ids(
        &timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, 1)
            .await
            .expect("batch scan"),
    );

    // 100 rows that expire after the sweep started.
    let arrivals: Vec<uuid::Uuid> = (0..100).map(|_| uuid::Uuid::new_v4()).collect();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, input, state, attempt, max_attempts, \
          started_at, start_to_close) \
         SELECT u, $2, 'activity', '{}'::jsonb, 'RUNNING', \
                1, 1, NOW(), INTERVAL '1 millisecond' \
         FROM unnest($1) AS u",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&arrivals)
    .bind::<diesel::sql_types::Text, _>(queue)
    .execute(&mut conn)
    .await
    .expect("insert arrivals");
    tokio::time::sleep(Duration::from_millis(50)).await;

    for _ in 1..ours.len() {
        let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, 1)
            .await
            .expect("batch scan");
        seen.extend(start_to_close_ids(&page));
    }

    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(&mut conn)
        .await
        .expect("clear queue");
    assert_eq!(
        seen, ours,
        "the sweep must read every row it counted, and only those"
    );
}

/// A refill reads one bounded page of live rows, in creation order. It does
/// not scan every live row to find the expired ones. So a refill costs the same
/// at any backlog size, and one sweep reads each live row once.
#[tokio::test]
async fn a_refill_reads_one_bounded_page_of_live_rows() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("connection");
    let queue = "scanner-lease-page";
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(&mut conn)
        .await
        .expect("clear queue");
    // 200 live rows that are not expired. At a batch of 1, a page holds 64.
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, input, state, attempt, max_attempts, \
          started_at, start_to_close) \
         SELECT gen_random_uuid(), $1, 'activity', '{}'::jsonb, 'RUNNING', \
                1, 1, NOW(), INTERVAL '1 hour' \
         FROM generate_series(1, 200)",
    )
    .bind::<diesel::sql_types::Text, _>(queue)
    .execute(&mut conn)
    .await
    .expect("insert live tasks");
    // One expired row, created after them all.
    let target = uuid::Uuid::from_u128(u128::MAX - 1);
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, input, state, attempt, max_attempts, \
          started_at, start_to_close) \
         VALUES ($1, $2, 'activity', '{}'::jsonb, 'RUNNING', \
                 1, 1, NOW() - INTERVAL '1 minute', INTERVAL '1 second')",
    )
    .bind::<diesel::sql_types::Uuid, _>(target)
    .bind::<diesel::sql_types::Text, _>(queue)
    .execute(&mut conn)
    .await
    .expect("insert expired task");

    let mut cursor = TimeoutScanCursor::default();
    let mut found_at = None;
    for pass in 1..=100 {
        let page = timeout::find_timed_out_tasks_batch(&mut conn, &mut cursor, 1)
            .await
            .expect("batch scan");
        if start_to_close_ids(&page).contains(&target) {
            found_at = Some(pass);
            break;
        }
    }

    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(&mut conn)
        .await
        .expect("clear queue");
    // At least 200 live rows were created before the target, so it is on the
    // fourth page or later. Other suites' live rows can only push it further.
    let found_at = found_at.expect("the sweep must reach the expired row");
    assert!(
        found_at >= 4,
        "a refill must read one page of live rows, not all of them: found on pass {found_at}"
    );
}

/// AC2, end to end: the spawned checker enforces one batch per pass.
#[tokio::test]
async fn spawned_checker_enforces_one_batch_per_pass() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let queue = "scanner-lease-spawned";
    let ours = {
        let mut conn = pool.get().await.expect("connection");
        let ours = insert_expired_running_tasks(&mut conn, queue, 7).await;
        assert_eq!(
            foreign_expired_rows(&mut conn, queue).await,
            0,
            "precondition: no other expired rows in this database"
        );
        ours
    };

    // A long interval, so the test can stop the loop after its first pass.
    let mut spec = Spec::new(
        ShardId::new(17_952),
        "batch-leader",
        Duration::from_secs(2),
        Duration::from_secs(10),
    );
    spec.batch = 3;
    let checker = spawn_checker(&pool, spec);
    wait_for("the first pass", Duration::from_secs(20), || {
        checker.metrics.role("leader") >= 1
    })
    .await;
    stop_all(vec![checker]).await;

    let mut conn = pool.get().await.expect("connection");
    let left = still_running(&mut conn, &ours).await;
    // Leave no expired rows for the next test in a shared database.
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(&mut conn)
        .await
        .expect("clear queue");
    assert_eq!(left, 4, "one pass must enforce exactly one batch of 3");
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
    let longest_sleep = max_jittered_interval(interval, JITTER);

    let mut checkers: Vec<Checker> = ["k1", "k2", "k3"]
        .into_iter()
        .map(|h| spawn_checker(&pool, Spec::new(shard, h, interval, ttl)))
        .collect();

    let deadline = Instant::now() + Duration::from_secs(20);
    let (first, first_epoch) = loop {
        if let Some(held) = live_holder(&pool, shard).await {
            break held;
        }
        assert!(Instant::now() < deadline, "no checker took the lease");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // Renewal by the same holder keeps the epoch.
    tokio::time::sleep(longest_sleep * 3).await;
    assert_eq!(
        live_holder(&pool, shard).await,
        Some((first.clone(), first_epoch)),
        "a renewal must keep the holder and the epoch"
    );

    // Kill, not stop: an abort skips the graceful release.
    let idx = checkers.iter().position(|c| c.holder == first).unwrap();
    let killed = checkers.remove(idx);
    killed.handle.abort();
    let _ = killed.handle.await;
    let killed_at = Instant::now();

    // A standby leads on its first tick after the lease ends.
    let bound =
        effective_lease_ttl(ttl, interval, JITTER) + longest_sleep + Duration::from_millis(500);
    let (second, second_epoch) = loop {
        if let Some((holder, epoch)) = live_holder(&pool, shard).await
            && holder != first
        {
            break (holder, epoch);
        }
        assert!(
            killed_at.elapsed() < bound,
            "no standby took over within the lease TTL"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        killed_at.elapsed() >= ttl / 2,
        "a killed holder must keep its lease until the TTL runs out"
    );
    assert!(second_epoch > first_epoch, "a takeover must bump the epoch");

    let leader = checkers.iter().find(|c| c.holder == second).unwrap();
    let before = leader.metrics.role("leader");
    wait_for("passes on the new leader", ttl, || {
        leader.metrics.role("leader") > before
    })
    .await;

    // Graceful stop: the holder expires its lease on exit.
    let idx = checkers.iter().position(|c| c.holder == second).unwrap();
    let stopped = checkers.remove(idx);
    stopped.cancel.cancel();
    let _ = stopped.handle.await;
    let stopped_at = Instant::now();
    let row = lease_row(&pool, shard).await;
    assert!(
        row.is_some_and(|r| r.holder != second || !r.live),
        "a graceful stop must expire the lease"
    );

    // The last standby leads on its next tick, well inside the TTL.
    let last = &checkers[0];
    loop {
        if let Some((holder, _)) = live_holder(&pool, shard).await
            && holder == last.holder
        {
            break;
        }
        assert!(
            stopped_at.elapsed() < longest_sleep * 3 + Duration::from_millis(500),
            "a graceful stop must hand over on the next standby tick"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    stop_all(checkers).await;
}

/// Checkers on one pool that scan different shards must not share a lease.
/// A shared lease would leave the standby's shards unscanned, because each
/// pass scans only its own checker's shards.
#[tokio::test]
async fn checkers_with_different_scopes_each_lead() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let shard = ShardId::new(17_956);
    let interval = Duration::from_millis(100);

    let checkers: Vec<Checker> = [("own", vec![shard]), ("other", vec![ShardId::new(17_957)])]
        .into_iter()
        .map(|(holder, scope)| {
            let mut spec = Spec::new(shard, holder, interval, Duration::from_secs(5));
            spec.scope = scope;
            spawn_checker(&pool, spec)
        })
        .collect();
    wait_for("10 ticks on every checker", Duration::from_secs(30), || {
        checkers.iter().all(|c| c.metrics.ticks() >= 10)
    })
    .await;
    let standby: Vec<usize> = checkers.iter().map(|c| c.metrics.role("standby")).collect();
    let leader: Vec<usize> = checkers.iter().map(|c| c.metrics.role("leader")).collect();
    stop_all(checkers).await;

    assert_eq!(
        standby,
        [0, 0],
        "neither checker may stand by for the other"
    );
    assert!(
        leader.iter().all(|n| *n > 0),
        "each scope must have its own leader, got {leader:?}"
    );
}

/// A failed lease query does not stop enforcement: the checker runs the
/// pass anyway. A holder id with a NUL byte makes Postgres reject the query
/// without touching the schema.
#[tokio::test]
async fn a_failed_lease_query_fails_open() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let checker = spawn_checker(
        &pool,
        Spec::new(
            ShardId::new(17_954),
            "bad\0holder",
            Duration::from_millis(100),
            Duration::from_secs(2),
        ),
    );
    wait_for("fail-open passes", Duration::from_secs(20), || {
        checker.metrics.role("fail_open") >= 3
    })
    .await;
    let (leader, standby) = (
        checker.metrics.role("leader"),
        checker.metrics.role("standby"),
    );
    stop_all(vec![checker]).await;
    assert_eq!(
        (leader, standby),
        (0, 0),
        "no tick may claim a lease it cannot take"
    );
}

#[derive(Debug)]
struct XorCodec(u8);

impl PayloadCodec for XorCodec {
    fn codec_id(&self) -> &'static str {
        "xor"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().map(|b| b ^ self.0).collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.iter().map(|b| b ^ self.0).collect())
    }
}

fn two_key_registry() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    codecs
        .register_key("k1", Arc::new(XorCodec(0x11)))
        .expect("register k1");
    codecs
        .register_key("k2", Arc::new(XorCodec(0x22)))
        .expect("register k2");
    codecs.set_active_key("k1").expect("activate k1");
    codecs
}

/// A standby skips the pass but still refreshes its active codec key.
/// Codec key retirement counts on every live process to do that each tick.
#[tokio::test]
async fn a_standby_still_refreshes_its_codec_key() {
    let (url, _container) = setup_test_db_url().await;
    let pool = build_pool(&url);
    let shard = ShardId::new(17_955);
    let interval = Duration::from_millis(100);

    let views: Vec<PayloadCodecs> = (0..2).map(|_| two_key_registry()).collect();
    let checkers: Vec<Checker> = ["c1", "c2"]
        .into_iter()
        .zip(&views)
        .map(|(h, codecs)| {
            let mut spec = Spec::new(shard, h, interval, Duration::from_secs(5));
            spec.codecs = codecs.clone();
            spawn_checker(&pool, spec)
        })
        .collect();
    wait_for("a standby tick", Duration::from_secs(20), || {
        checkers.iter().any(|c| c.metrics.role("standby") > 0)
    })
    .await;

    let operator = two_key_registry();
    let sharded = autumn_harvest::ShardedDbPool::single(pool.clone());
    autumn_harvest::codec_rotation::activate_codec_key(
        &sharded,
        &[ShardId::new(0)],
        &operator,
        "k2",
        60,
    )
    .await
    .expect("activate k2");

    wait_for("both checkers on k2", Duration::from_secs(20), || {
        views.iter().all(|c| c.active_key_id() == "k2")
    })
    .await;
    stop_all(checkers).await;
}
