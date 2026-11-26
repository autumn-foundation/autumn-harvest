//! One elected gauge sampler per shard, against a real database.
//!
//! 1. A leader reads a shard and writes its read. A standby uses that row
//!    and does not read the shard.
//! 2. Three workers read each shard once per round.
//! 3. A graceful stop hands over on the next tick. A dead leader hands over
//!    within the lease TTL. No tick drops or doubles a value.
//! 4. A missing, old or undecodable row, and a failed lease query, fail open.
//! 5. A worker-driven harness counts the reads of a fleet. Each worker emits
//!    the values that an unelected worker emits.
#![cfg(feature = "db")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::gauge_election::{
    GaugeElection, GaugeSampler, GaugeSlot, fresh_gauge_sample, publish_gauge_sample,
};
use autumn_harvest::scanner_lease::{ScannerConfig, ScannerLease, ScannerRole};
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::throwaway_db::ThrowawayDb;

/// A database of this test alone. The guards drop it after the test.
struct Db {
    url: String,
    _throwaway: Option<ThrowawayDb>,
    _container: Option<ContainerAsync<Postgres>>,
}

/// Create a migrated database for one test.
async fn database(prefix: &str) -> Db {
    if let Some(throwaway) = ThrowawayDb::create(prefix).await {
        return Db {
            url: throwaway.url(),
            _throwaway: Some(throwaway),
            _container: None,
        };
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("start the Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    Db {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        _throwaway: None,
        _container: Some(container),
    }
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to the test database")
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    DbPool::builder(manager)
        .max_size(16)
        .build()
        .expect("build the pool")
}

/// Counts `harvest.scanner.pass` by scanner, shard and role. Keeps the last
/// value of each gauge, and every DLQ emission.
#[derive(Default)]
struct Recorder {
    passes: Mutex<BTreeMap<(String, String, String), usize>>,
    dlq: Mutex<Vec<(u16, u64)>>,
    gauges: Mutex<BTreeMap<String, String>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Recorder {
    /// Passes of `scanner` in `role`, on every shard.
    fn passes(&self, scanner: &str, role: &str) -> usize {
        lock(&self.passes)
            .iter()
            .filter(|((s, _, r), _)| s == scanner && r == role)
            .map(|(_, n)| n)
            .sum()
    }

    /// Passes of `scanner` on `shard` that read the shard, whatever the
    /// reason.
    fn shard_reads(&self, scanner: &str, shard: &str) -> usize {
        lock(&self.passes)
            .iter()
            .filter(|((s, sh, r), _)| s == scanner && sh == shard && r != "standby")
            .map(|(_, n)| n)
            .sum()
    }

    /// Passes of `scanner` on `shard`, in any role.
    fn shard_passes(&self, scanner: &str, shard: &str) -> usize {
        lock(&self.passes)
            .iter()
            .filter(|((s, sh, _), _)| s == scanner && sh == shard)
            .map(|(_, n)| n)
            .sum()
    }

    fn set(&self, name: String, value: String) {
        lock(&self.gauges).insert(name, value);
    }

    /// The gauge values this worker last emitted, as one comparable map.
    fn snapshot(&self) -> BTreeMap<String, String> {
        lock(&self.gauges).clone()
    }
}

impl MetricsRecorder for Recorder {
    fn record_scanner_pass(&self, scanner: &str, shard: &str, role: &str) {
        *lock(&self.passes)
            .entry((scanner.to_owned(), shard.to_owned(), role.to_owned()))
            .or_insert(0) += 1;
    }

    fn record_dlq_entries(&self, shard_id: u16, count: u64) {
        lock(&self.dlq).push((shard_id, count));
        self.set(format!("dlq/{shard_id}"), count.to_string());
    }

    fn record_workflow_active(&self, workflow: &str, state: &str, count: u64) {
        self.set(format!("active/{workflow}/{state}"), count.to_string());
    }

    fn record_queue_paused(&self, queue: &str, paused: bool) {
        self.set(format!("paused/{queue}"), paused.to_string());
    }

    fn record_queue_depth(&self, queue: &str, depth: u64) {
        self.set(format!("depth/{queue}"), depth.to_string());
    }

    fn record_shard_stranded_pending(&self, shard_id: u16, count: u64) {
        self.set(format!("stranded/{shard_id}"), count.to_string());
    }

    fn record_rate_limit_tokens_available(&self, key: &str, tokens: f64) {
        self.set(format!("tokens/{key}"), tokens.to_string());
    }

    fn record_rate_limit_refill_rate(&self, key: &str, refill_rate: f64) {
        self.set(format!("refill/{key}"), refill_rate.to_string());
    }

    fn record_workflow_history_oversized(&self, workflow: &str, count: u64) {
        self.set(format!("oversized/{workflow}"), count.to_string());
    }
}

/// A key of this test alone, so tests can share one server.
fn test_key(name: &str) -> String {
    format!("gauge:test:{name}:{}", uuid::Uuid::new_v4().simple())
}

/// Election with a short interval, so a TTL passes in a test.
fn election(holder: &str, interval: Duration) -> GaugeElection {
    GaugeElection::new(&ScannerConfig::default(), holder, interval)
}

fn slot(holder: &str, key: &str, shard: i32, interval: Duration) -> GaugeSlot {
    GaugeSlot::new(
        &election(holder, interval),
        GaugeSampler::Dlq,
        key,
        ShardId::new(shard),
        None,
    )
}

/// One sample through `slot`. The read returns `value` and counts itself.
async fn sample(
    slot: &mut GaugeSlot,
    conn: &mut AsyncPgConnection,
    reads: &AtomicUsize,
    value: u64,
) -> (u64, ScannerRole) {
    let metrics = Recorder::default();
    slot.sample(conn, &metrics, async |_conn| {
        reads.fetch_add(1, Ordering::SeqCst);
        Ok::<_, autumn_harvest::error::HarvestError>(value)
    })
    .await
    .expect("sample succeeds")
}

/// The reads that `reads` counted.
fn count(reads: &AtomicUsize) -> usize {
    AtomicUsize::load(reads, Ordering::SeqCst)
}

const LONG: Duration = Duration::from_secs(30);

/// Rounds of the slot-level round test.
const ROUNDS: usize = 10;

/// Gauge intervals that the fleet harness runs.
const TICKS: u32 = 12;

/// Takes the lease of `key` on `shard` for `holder`, then writes `sample`.
async fn publish_as(
    conn: &mut AsyncPgConnection,
    holder: &str,
    shard: i32,
    key: &str,
    sample: &serde_json::Value,
) {
    let lease = ScannerLease::new(ShardId::new(shard), key, holder, LONG);
    assert!(
        lease.try_acquire(conn).await.expect("lease").is_some(),
        "{holder} must hold the lease"
    );
    let written = publish_gauge_sample(conn, ShardId::new(shard), key, holder, sample)
        .await
        .expect("publish");
    assert!(written, "a lease holder writes its row");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_without_the_lease_writes_no_row() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("guard");
    let shard = ShardId::new(0);
    // No lease at all.
    let written = publish_gauge_sample(&mut conn, shard, &key, "x", &serde_json::json!(1))
        .await
        .expect("publish");
    assert!(!written);
    // Another holder has the lease. An outdated leader cannot overwrite.
    publish_as(&mut conn, "new", 0, &key, &serde_json::json!(2)).await;
    let written = publish_gauge_sample(&mut conn, shard, &key, "old", &serde_json::json!(1))
        .await
        .expect("publish");
    assert!(!written);
    let row = fresh_gauge_sample(&mut conn, shard, &key, LONG)
        .await
        .expect("read");
    assert_eq!(row, Some(serde_json::json!(2)));
}

/// A handover in flight decides the write. The old holder waits for it, and
/// then writes nothing over the new holder's lease.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_waits_for_a_handover_and_then_yields() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("handover");
    publish_as(&mut conn, "old", 0, &key, &serde_json::json!(1)).await;

    // A new holder takes the lease in a transaction that is still open.
    let mut taker = connect(&db.url).await;
    taker.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query(
        "UPDATE harvest_scanner_leases SET holder = 'new', epoch = epoch + 1 \
         WHERE shard_id = 0 AND scanner = $1",
    )
    .bind::<Text, _>(&key)
    .execute(&mut taker)
    .await
    .expect("take the lease");

    let url = db.url.clone();
    let write_key = key.clone();
    let write = tokio::spawn(async move {
        let mut conn = connect(&url).await;
        publish_gauge_sample(
            &mut conn,
            ShardId::new(0),
            &write_key,
            "old",
            &serde_json::json!(9),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    taker.batch_execute("COMMIT").await.expect("commit");

    let written = write
        .await
        .expect("join")
        .expect("the write ends without error");
    assert!(!written, "the old holder must not write after the handover");
    let row = fresh_gauge_sample(&mut conn, ShardId::new(0), &key, LONG)
        .await
        .expect("read");
    assert_eq!(row, Some(serde_json::json!(1)));
}

/// A transaction that holds the row lock cannot stall the leader. The write
/// gives up within its bound, and the leader still emits its own read.
#[tokio::test(flavor = "multi_thread")]
async fn a_locked_row_cannot_stall_the_leader() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("locked");
    publish_as(&mut conn, "a", 0, &key, &serde_json::json!(1)).await;

    // Another session holds the row lock until the test ends.
    let mut blocker = connect(&db.url).await;
    blocker.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query("SELECT 1 FROM harvest_gauge_samples WHERE sampler = $1 FOR UPDATE")
        .bind::<Text, _>(&key)
        .execute(&mut blocker)
        .await
        .expect("lock the row");

    let started = std::time::Instant::now();
    let written = tokio::time::timeout(
        Duration::from_secs(10),
        publish_gauge_sample(&mut conn, ShardId::new(0), &key, "a", &serde_json::json!(2)),
    )
    .await
    .expect("the write must not wait for the lock");
    assert!(written.is_err(), "a lock timeout is a failed write");
    assert!(started.elapsed() < Duration::from_secs(5));

    // The slot path: the leader keeps its own read.
    let mut a = slot("a", &key, 0, LONG);
    let reads = AtomicUsize::new(0);
    let sampled = tokio::time::timeout(
        Duration::from_secs(10),
        sample(&mut a, &mut conn, &reads, 3),
    )
    .await
    .expect("the sample must not wait for the lock");
    assert_eq!(sampled, (3, ScannerRole::Leader));
    blocker.batch_execute("ROLLBACK").await.expect("rollback");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leader_whose_reads_fail_stands_by_for_one_ttl() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("failing");
    let mut a = slot("a", &key, 0, LONG);
    let metrics = Recorder::default();
    for _ in 0..3 {
        let failed = a
            .sample(&mut conn, &metrics, async |_conn| {
                Err::<u64, _>(autumn_harvest::error::HarvestError::Database(
                    "read failed".into(),
                ))
            })
            .await;
        assert!(failed.is_err());
    }
    assert_eq!(metrics.passes("gauge_dlq", "leader"), 3);
    // The lease is free, but `a` stays off it for one TTL. No leader wrote
    // a row, so `a` reads the shard itself.
    let reads = AtomicUsize::new(0);
    assert_eq!(
        sample(&mut a, &mut conn, &reads, 4).await,
        (4, ScannerRole::FailOpen)
    );
    let mut b = slot("b", &key, 0, LONG);
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 5).await.1,
        ScannerRole::Leader
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_that_does_not_survive_json_is_not_shared() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("nan");
    let mut a = slot("a", &key, 0, LONG);
    let metrics = Recorder::default();
    let (value, role) = a
        .sample(&mut conn, &metrics, async |_conn| {
            Ok::<_, autumn_harvest::error::HarvestError>(f64::NAN)
        })
        .await
        .expect("the leader keeps its own read");
    assert!(value.is_nan());
    assert_eq!(role, ScannerRole::Leader);
    let row = fresh_gauge_sample(&mut conn, ShardId::new(0), &key, LONG)
        .await
        .expect("read");
    assert_eq!(row, None, "a NaN must not reach the shared row");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leader_reads_and_a_standby_uses_its_row() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("leader");
    let mut a = slot("a", &key, 0, LONG);
    let mut b = slot("b", &key, 0, LONG);
    let reads = AtomicUsize::new(0);

    assert_eq!(
        sample(&mut a, &mut conn, &reads, 7).await,
        (7, ScannerRole::Leader)
    );
    // The standby gets the leader's value. Its own read never runs.
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 99).await,
        (7, ScannerRole::Standby)
    );
    assert_eq!(count(&reads), 1);
    assert!(a.has_led());
    assert!(!b.has_led());
}

#[tokio::test(flavor = "multi_thread")]
async fn three_workers_read_each_shard_once_per_round() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("rounds");
    let shards = [0, 1];
    let mut slots: Vec<GaugeSlot> = ["a", "b", "c"]
        .iter()
        .flat_map(|holder| shards.iter().map(|s| slot(holder, &key, *s, LONG)))
        .collect();
    let reads: Vec<AtomicUsize> = shards.iter().map(|_| AtomicUsize::new(0)).collect();
    for round in 0..ROUNDS {
        for slot in &mut slots {
            let shard = usize::try_from(slot.shard().as_i32()).expect("shard");
            let (value, _) = sample(slot, &mut conn, &reads[shard], round as u64).await;
            // Each worker sees this round's value. Nothing doubles it.
            assert_eq!(value, round as u64);
        }
    }
    for (shard, shard_reads) in reads.iter().enumerate() {
        assert_eq!(
            count(shard_reads),
            ROUNDS,
            "shard {shard} must be read once per round, not once per worker"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_graceful_stop_hands_over_on_the_next_tick() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("release");
    let mut a = slot("a", &key, 0, LONG);
    let mut b = slot("b", &key, 0, LONG);
    let reads = AtomicUsize::new(0);

    assert_eq!(
        sample(&mut a, &mut conn, &reads, 1).await.1,
        ScannerRole::Leader
    );
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 1).await.1,
        ScannerRole::Standby
    );
    a.release(&mut conn).await.expect("release");
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 2).await,
        (2, ScannerRole::Leader)
    );
    // A slot that never led releases nothing. The lease of `b` stays, so a
    // new slot stands by and gets the row of `b`.
    let mut c = slot("c", &key, 0, LONG);
    c.release(&mut conn).await.expect("release");
    let mut d = slot("d", &key, 0, LONG);
    assert_eq!(
        sample(&mut d, &mut conn, &reads, 3).await,
        (2, ScannerRole::Standby)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_leader_hands_over_within_the_lease_ttl() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("dead");
    let interval = Duration::from_millis(100);
    let ttl = election("x", interval).lease_ttl;
    let max_age = election("x", interval).max_age;
    assert!(max_age < ttl, "the row must age out before the lease ends");
    let mut a = slot("a", &key, 0, interval);
    let mut b = slot("b", &key, 0, interval);
    let reads = AtomicUsize::new(0);

    assert_eq!(
        sample(&mut a, &mut conn, &reads, 5).await.1,
        ScannerRole::Leader
    );
    // The leader dies without a release. The standby uses its row.
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 6).await,
        (5, ScannerRole::Standby)
    );
    // The row ages out while the dead lease still holds. The standby reads
    // the shard itself, so its gauge never goes stale past the row age.
    tokio::time::sleep(max_age + interval).await;
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 6).await,
        (6, ScannerRole::FailOpen)
    );
    let started = std::time::Instant::now();
    loop {
        let (value, role) = sample(&mut b, &mut conn, &reads, 6).await;
        // No tick drops the gauge or doubles it.
        assert_eq!(value, 6);
        if role == ScannerRole::Leader {
            break;
        }
        assert_eq!(role, ScannerRole::FailOpen);
        assert!(
            started.elapsed() < ttl + interval * 3,
            "the standby must take over within the lease TTL of {ttl:?}"
        );
        tokio::time::sleep(interval).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_old_row_makes_a_standby_read_the_shard() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("old");
    let mut a = slot("a", &key, 0, LONG);
    let mut b = slot("b", &key, 0, LONG);
    let reads = AtomicUsize::new(0);
    assert_eq!(
        sample(&mut a, &mut conn, &reads, 1).await.1,
        ScannerRole::Leader
    );
    diesel::sql_query(
        "UPDATE harvest_gauge_samples SET sampled_at = sampled_at - INTERVAL '1 hour' \
         WHERE sampler = $1",
    )
    .bind::<Text, _>(&key)
    .execute(&mut conn)
    .await
    .expect("age the row");
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 2).await,
        (2, ScannerRole::FailOpen)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_or_undecodable_row_makes_a_standby_read_the_shard() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("garbled");
    let mut a = slot("a", &key, 0, LONG);
    let mut b = slot("b", &key, 0, LONG);
    let reads = AtomicUsize::new(0);
    // The leader holds the lease, then its row is replaced with text.
    assert_eq!(
        sample(&mut a, &mut conn, &reads, 1).await.1,
        ScannerRole::Leader
    );
    publish_as(&mut conn, "a", 0, &key, &serde_json::json!("x")).await;
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 2).await,
        (2, ScannerRole::FailOpen)
    );
    // With no row at all, the standby reads the shard too.
    diesel::sql_query("DELETE FROM harvest_gauge_samples WHERE sampler = $1")
        .bind::<Text, _>(&key)
        .execute(&mut conn)
        .await
        .expect("delete the row");
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 3).await,
        (3, ScannerRole::FailOpen)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_lease_query_fails_open() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    conn.batch_execute("SET default_transaction_read_only = on")
        .await
        .expect("read only");
    let key = test_key("readonly");
    let mut a = slot("a", &key, 0, LONG);
    let reads = AtomicUsize::new(0);
    assert_eq!(
        sample(&mut a, &mut conn, &reads, 4).await,
        (4, ScannerRole::FailOpen)
    );
    assert_eq!(count(&reads), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_holder_each_worker_reads_and_writes_nothing() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("unelected");
    let mut a = GaugeSlot::new(
        &GaugeElection::unelected(),
        GaugeSampler::Dlq,
        &key,
        ShardId::new(0),
        None,
    );
    let reads = AtomicUsize::new(0);
    for value in 0..3 {
        assert_eq!(
            sample(&mut a, &mut conn, &reads, value).await,
            (value, ScannerRole::Unelected)
        );
    }
    assert_eq!(count(&reads), 3);
    let row = fresh_gauge_sample(&mut conn, ShardId::new(0), &key, LONG)
        .await
        .expect("read");
    assert_eq!(row, None, "an unelected worker writes no row");
}

/// A value whose serialization fails, so the leader cannot write its row.
struct Unwritable;

impl serde::Serialize for Unwritable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("cannot write"))
    }
}

impl<'de> serde::Deserialize<'de> for Unwritable {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Ok(Self)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leader_that_cannot_write_its_row_gives_up_the_lease() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("unwritable");
    let mut a = slot("a", &key, 0, LONG);
    let mut b = slot("b", &key, 0, LONG);
    let metrics = Recorder::default();
    for _ in 0..3 {
        let (_, role) = a
            .sample(&mut conn, &metrics, async |_conn| {
                Ok::<_, autumn_harvest::error::HarvestError>(Unwritable)
            })
            .await
            .expect("the leader still gets its own read");
        assert_eq!(role, ScannerRole::Leader);
    }
    // Three failed writes in a row give up the lease. Another worker leads.
    let reads = AtomicUsize::new(0);
    assert_eq!(
        sample(&mut b, &mut conn, &reads, 8).await.1,
        ScannerRole::Leader
    );
}

#[derive(QueryableByName)]
struct KeyRow {
    #[diesel(sql_type = Text)]
    sampler: String,
    #[diesel(sql_type = Integer)]
    shard_id: i32,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_deletes_rows_of_its_shard_that_no_leader_wrote_for_a_day() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let (old, other_shard, fresh) = (test_key("old"), test_key("other"), test_key("fresh"));
    for (key, shard) in [(&old, 0), (&other_shard, 1)] {
        publish_as(&mut conn, "t", shard, key, &serde_json::json!(1)).await;
    }
    conn.batch_execute(
        "UPDATE harvest_gauge_samples SET sampled_at = sampled_at - INTERVAL '2 days'",
    )
    .await
    .expect("age every row");
    publish_as(&mut conn, "t", 0, &fresh, &serde_json::json!(2)).await;
    let rows: Vec<KeyRow> =
        diesel::sql_query("SELECT sampler, shard_id FROM harvest_gauge_samples ORDER BY sampler")
            .load(&mut conn)
            .await
            .expect("rows");
    let keys: Vec<(String, i32)> = rows.into_iter().map(|r| (r.sampler, r.shard_id)).collect();
    assert!(
        !keys.contains(&(old.clone(), 0)),
        "the old row of shard 0 goes"
    );
    assert!(keys.contains(&(fresh.clone(), 0)));
    assert!(
        keys.contains(&(other_shard.clone(), 1)),
        "a write on shard 0 leaves shard 1 alone"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_row_is_fresh_only_within_its_age() {
    let db = database("gauge_elect_t").await;
    let mut conn = connect(&db.url).await;
    let key = test_key("age");
    publish_as(&mut conn, "t", 0, &key, &serde_json::json!({"n": 3})).await;
    let fresh = fresh_gauge_sample(&mut conn, ShardId::new(0), &key, LONG)
        .await
        .expect("read");
    assert_eq!(fresh, Some(serde_json::json!({"n": 3})));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let old = fresh_gauge_sample(&mut conn, ShardId::new(0), &key, Duration::from_millis(10))
        .await
        .expect("read");
    assert_eq!(old, None);
    // Another shard has its own row.
    let other = fresh_gauge_sample(&mut conn, ShardId::new(1), &key, LONG)
        .await
        .expect("read");
    assert_eq!(other, None);
}

// ── Worker-driven harness ────────────────────────────────────────────────

/// The gauge interval of the harness workers.
const GAUGE_INTERVAL: Duration = Duration::from_millis(250);

/// Workers in the harness fleet.
const FLEET: usize = 3;

/// The dead letters that the harness seeds on each shard.
const DEAD_LETTERS: [i64; 2] = [37, 11];

/// The queue that harness workers poll unless a test names another.
const QUEUE: &str = "gauge-harness";

fn worker_config(
    sharded: &autumn_harvest::shard::ShardedDbPool,
    worker_id: String,
    elect: bool,
    queue: &str,
) -> WorkerRuntimeConfig {
    WorkerRuntimeConfig {
        gauge_sample_interval: GAUGE_INTERVAL,
        codec_rotation_batch_size: 0,
        scanner: ScannerConfig {
            elect,
            ..ScannerConfig::default()
        },
        dr: autumn_harvest::replication::DrConfig::default(),
        worker_id,
        queues: vec![queue.to_string()],
        queue_weights: std::collections::HashMap::new(),
        notification_database_url: None,
        shard_notification_database_urls: Vec::new(),
        max_concurrent_workflows: 4,
        max_concurrent_activities: 4,
        max_concurrent_claims: autumn_harvest::worker::DEFAULT_MAX_CONCURRENT_CLAIMS,
        claim_slots_per_shard: None,
        claim_batch_max: None,
        claim_lean: true,
        poll_interval: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(2),
        cancellation_grace_period: Duration::from_secs(1),
        sticky_timeout: Duration::ZERO,
        max_local_activity_start_to_close: Duration::from_secs(60),
        shard_assignments: sharded.iter_shards().map(|(shard, _)| shard).collect(),
        worker_heartbeat_interval: Duration::from_secs(1),
        build_id: "gauge-harness".to_string(),
        deployment_name: None,
        workflow_cache_size: 10,
        resident_workflows: true,
        priority_aging_secs: None,
        unknown_target_grace_window: Duration::from_secs(5),
        poison_pill_threshold: 3,
        capability_miss_max_redeliveries: 5,
        workflow_task_timeout: Duration::from_secs(30),
        workflow_panic_max_attempts: 3,
        max_workflow_pause_duration: Duration::from_hours(24),
        labels: std::collections::HashMap::new(),
        sharded_pool: Some(sharded.clone()),
        max_workflow_history_events: None,
        slot_tuner: None,
        max_concurrent_sessions: 0,
        fairness_keys: false,
    }
}

/// One harness worker and its recorder.
struct FleetWorker {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
    metrics: Arc<Recorder>,
}

fn start_worker(
    sharded: &autumn_harvest::shard::ShardedDbPool,
    elect: bool,
    queue: &str,
) -> FleetWorker {
    let metrics = Arc::new(Recorder::default());
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics.clone()).build());
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        Vec::new(),
        Vec::new(),
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));
    let worker = Arc::new(
        Worker::new(
            worker_config(sharded, uuid::Uuid::new_v4().to_string(), elect, queue),
            registry,
        )
        .expect("worker builds"),
    );
    let run = worker.clone();
    let run_pool = sharded.pool_for(ShardId::new(0)).clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(120), run.run(&run_pool)).await;
    });
    FleetWorker {
        worker,
        handle,
        metrics,
    }
}

/// Stops `workers` together, so none samples alone while another stops.
async fn stop_all(workers: Vec<FleetWorker>) {
    for worker in &workers {
        worker.worker.shutdown();
    }
    let handles = workers.into_iter().map(|w| w.handle);
    let _ = tokio::time::timeout(Duration::from_secs(10), futures::future::join_all(handles)).await;
}

/// Seeds one shard with rows that no harness worker changes.
///
/// The rows are dead letters, active runs and a held queue with pending
/// rows. There are also pending rows on a queue that no worker polls, and a
/// full rate-limit bucket.
async fn seed(conn: &mut AsyncPgConnection, dead_letters: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_dead_letters \
             (original_task_id, queue_name, task_type, input, error, attempts) \
         SELECT gen_random_uuid(), '{QUEUE}', 'activity', '{{}}'::jsonb, 'x', 1 \
         FROM generate_series(1, {dead_letters}); \
         INSERT INTO harvest_workflow_executions \
             (workflow_name, workflow_id, shard_id, state, input, started_at) \
         SELECT 'gauge-wf', 'gauge-wf-' || g, 0, \
                CASE WHEN g % 3 = 0 THEN 'PAUSED' ELSE 'RUNNING' END, '{{}}'::jsonb, NOW() \
         FROM generate_series(1, 11) g; \
         INSERT INTO harvest_queue_pauses (queue_name, reason) \
         VALUES ('{QUEUE}', 'gauge harness'); \
         INSERT INTO harvest_task_queue (queue_name, task_type, input, state) \
         SELECT q, 'activity', '{{}}'::jsonb, 'PENDING' \
         FROM unnest(ARRAY['{QUEUE}', 'gauge-orphan']) q, generate_series(1, 5); \
         INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ('gauge-bucket', 2.0, 9.0, 9.0, NOW());"
    ))
    .await
    .expect("seed the harness fixture");
}

/// A fleet database for each harness shard, seeded, and its pool.
struct Shards {
    sharded: autumn_harvest::shard::ShardedDbPool,
    conns: Vec<AsyncPgConnection>,
    _dbs: Vec<Db>,
}

async fn shards(count: usize) -> Shards {
    let mut dbs = Vec::new();
    let mut conns = Vec::new();
    let mut pools = BTreeMap::new();
    for (i, dead_letters) in DEAD_LETTERS.iter().take(count).enumerate() {
        let db = database("gauge_fleet_t").await;
        let mut conn = connect(&db.url).await;
        seed(&mut conn, *dead_letters).await;
        let shard = ShardId::new(i32::try_from(i).expect("shard"));
        pools.insert(shard, build_pool(&db.url));
        conns.push(conn);
        dbs.push(db);
    }
    Shards {
        sharded: autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0)),
        conns,
        _dbs: dbs,
    }
}

/// Runs a fleet for `ticks` gauge intervals and stops it.
async fn run_fleet(
    sharded: &autumn_harvest::shard::ShardedDbPool,
    elect: bool,
    ticks: u32,
) -> Vec<Arc<Recorder>> {
    let workers: Vec<FleetWorker> = (0..FLEET)
        .map(|_| start_worker(sharded, elect, QUEUE))
        .collect();
    tokio::time::sleep(GAUGE_INTERVAL * ticks).await;
    let recorders: Vec<Arc<Recorder>> = workers.iter().map(|w| w.metrics.clone()).collect();
    stop_all(workers).await;
    recorders
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn gauge_leases(conn: &mut AsyncPgConnection, pattern: &str) -> i64 {
    let rows: Vec<Count> =
        diesel::sql_query("SELECT count(*) AS n FROM harvest_scanner_leases WHERE scanner LIKE $1")
            .bind::<Text, _>(pattern)
            .load(conn)
            .await
            .expect("count leases");
    rows[0].n
}

/// Each gauge sampler of an elected fleet reads each shard about once per
/// interval. An unelected fleet reads it once per worker. Each elected
/// worker emits the values of an unelected worker.
#[tokio::test(flavor = "multi_thread")]
async fn a_fleet_reads_each_shard_once_per_interval_and_emits_unchanged_values() {
    let mut fleet = shards(2).await;

    let unelected = run_fleet(&fleet.sharded, false, TICKS).await;
    let elected = run_fleet(&fleet.sharded, true, TICKS).await;

    for sampler in GaugeSampler::ALL {
        let scanner = sampler.scanner_label();
        for shard in ["0", "1"] {
            let reads: usize = elected.iter().map(|r| r.shard_reads(scanner, shard)).sum();
            // The longest-running worker sets the number of intervals.
            let ticks = elected
                .iter()
                .map(|r| r.shard_passes(scanner, shard))
                .max()
                .unwrap_or(0);
            let baseline: usize = unelected
                .iter()
                .map(|r| r.shard_reads(scanner, shard))
                .sum();
            let baseline_ticks = unelected
                .iter()
                .map(|r| r.shard_passes(scanner, shard))
                .max()
                .unwrap_or(0);
            eprintln!(
                "{scanner} shard {shard}: elected reads={reads} over {ticks} ticks; \
                 unelected reads={baseline} over {baseline_ticks} ticks"
            );
            assert!(ticks > 0, "{scanner} must run on shard {shard}");
            assert!(
                baseline_ticks > 0,
                "{scanner} must run unelected on shard {shard}"
            );
            // One read per tick, plus a first-tick read on each standby.
            assert!(
                reads <= ticks + FLEET,
                "{scanner} shard {shard}: the fleet must read the shard about once \
                 per interval, saw {reads} reads in {ticks} ticks"
            );
            // The two runs get different tick counts on a busy runner. So the
            // check compares reads per tick, not totals. Three workers read
            // three times per tick without election, and about once with it.
            // The check asks for half that gap, so runner noise cannot fail it.
            let rate = |reads: usize, ticks: usize| {
                f64::from(u32::try_from(reads).expect("fits"))
                    / f64::from(u32::try_from(ticks).expect("fits"))
            };
            let (elected_rate, baseline_rate) =
                (rate(reads, ticks), rate(baseline, baseline_ticks));
            assert!(
                baseline_rate >= elected_rate * 1.5,
                "{scanner} shard {shard}: election must cut the reads of {FLEET} workers, \
                 saw {elected_rate:.2} reads per tick against {baseline_rate:.2}"
            );
        }
        assert_eq!(
            unelected
                .iter()
                .map(|r| r.passes(scanner, "standby"))
                .sum::<usize>(),
            0,
            "{scanner}: an unelected fleet has no standby"
        );
    }

    // Every elected worker emits what an unelected worker emits.
    let reference = unelected[0].snapshot();
    let expect = [
        ("dlq/0", DEAD_LETTERS[0].to_string()),
        ("dlq/1", DEAD_LETTERS[1].to_string()),
        // Two shards each hold 8 running and 3 paused runs.
        ("active/gauge-wf/running", "16".to_string()),
        ("active/gauge-wf/paused", "6".to_string()),
        ("paused/gauge-harness", "true".to_string()),
        ("tokens/gauge-bucket", "18".to_string()),
        ("refill/gauge-bucket", "2".to_string()),
    ];
    for (name, value) in &expect {
        assert_eq!(reference.get(*name), Some(value), "{name} in {reference:?}");
    }
    for name in ["depth/gauge-harness", "stranded/0", "stranded/1"] {
        assert!(reference.contains_key(name), "{name} in {reference:?}");
    }
    for (i, recorder) in unelected.iter().chain(&elected).enumerate() {
        assert_eq!(
            recorder.snapshot(),
            reference,
            "worker {i} emits other values"
        );
    }

    // One lease per sampler and shard for the one input set. The DLQ
    // samplers of the assigned shards share the lease of their shard.
    for conn in &mut fleet.conns {
        let leases = gauge_leases(conn, "gauge:%").await;
        assert_eq!(
            leases,
            i64::try_from(GaugeSampler::ALL.len()).expect("fits"),
            "each sampler holds one lease per shard"
        );
    }
}

/// Workers with other inputs must not share a read. Each elects its own
/// leader, and each emits the depth of its own queue.
#[tokio::test(flavor = "multi_thread")]
async fn workers_with_other_queues_elect_their_own_reader() {
    let mut fleet = shards(1).await;
    let a = start_worker(&fleet.sharded, true, QUEUE);
    // A queue with no rows, so this worker claims nothing.
    let b = start_worker(&fleet.sharded, true, "gauge-other");
    tokio::time::sleep(GAUGE_INTERVAL * 8).await;
    let (ma, mb) = (a.metrics.clone(), b.metrics.clone());
    stop_all(vec![a, b]).await;

    let leases = gauge_leases(&mut fleet.conns[0], "gauge:v1:queue_depth:%").await;
    assert_eq!(leases, 2, "two queue sets need two queue-depth leases");
    for metrics in [&ma, &mb] {
        assert!(metrics.passes(GaugeSampler::QueueDepth.scanner_label(), "leader") > 0);
    }
    let (sa, sb) = (ma.snapshot(), mb.snapshot());
    assert!(sa.contains_key("depth/gauge-harness"));
    assert!(!sa.contains_key("depth/gauge-other"));
    assert_eq!(sb.get("depth/gauge-other").map(String::as_str), Some("0"));
    assert!(!sb.contains_key("depth/gauge-harness"));
}

/// A graceful stop of the leader hands over on the next tick. Each
/// survivor keeps emitting, follows a change, and never doubles a value.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_handover_keeps_every_gauge_on_every_worker() {
    let mut fleet = shards(1).await;
    let mut workers: Vec<FleetWorker> = (0..FLEET)
        .map(|_| start_worker(&fleet.sharded, true, QUEUE))
        .collect();
    tokio::time::sleep(GAUGE_INTERVAL * 6).await;
    let scanner = GaugeSampler::Dlq.scanner_label();
    let leader = workers
        .iter()
        .position(|w| w.metrics.passes(scanner, "leader") > 0)
        .expect("one worker leads the DLQ sampler");
    let leader = workers.remove(leader);
    stop_all(vec![leader]).await;

    // The dead letters grow after the handover. Each survivor must follow.
    let before: Vec<usize> = workers.iter().map(|w| lock(&w.metrics.dlq).len()).collect();
    let leaders_before: usize = workers
        .iter()
        .map(|w| w.metrics.passes(scanner, "leader"))
        .sum();
    let standbys_before: usize = workers
        .iter()
        .map(|w| w.metrics.passes(scanner, "standby"))
        .sum();
    fleet.conns[0]
        .batch_execute(&format!(
            "INSERT INTO harvest_dead_letters \
                 (original_task_id, queue_name, task_type, input, error, attempts) \
             SELECT gen_random_uuid(), '{QUEUE}', 'activity', '{{}}'::jsonb, 'x', 1 \
             FROM generate_series(1, 5)"
        ))
        .await
        .expect("add dead letters");
    let total = u64::try_from(DEAD_LETTERS[0] + 5).expect("fits");
    tokio::time::sleep(GAUGE_INTERVAL * 8).await;

    let leaders_after: usize = workers
        .iter()
        .map(|w| w.metrics.passes(scanner, "leader"))
        .sum();
    let standbys_after: usize = workers
        .iter()
        .map(|w| w.metrics.passes(scanner, "standby"))
        .sum();
    // The TTL is 10 s. Only the release of the stopped leader lets a
    // survivor lead within these 2 s.
    assert!(
        leaders_after > leaders_before,
        "a survivor must take the lease after a graceful stop"
    );
    assert!(
        standbys_after > standbys_before,
        "the other survivor must use the new leader's row"
    );
    for (i, worker) in workers.iter().enumerate() {
        let emitted = lock(&worker.metrics.dlq).clone();
        assert!(
            emitted.len() - before[i] >= 2,
            "survivor {i} must keep emitting through the handover"
        );
        for (shard, value) in &emitted {
            assert_eq!(*shard, 0);
            assert!(
                *value == u64::try_from(DEAD_LETTERS[0]).expect("fits") || *value == total,
                "survivor {i} must neither drop nor double the gauge, saw {value}"
            );
        }
        assert_eq!(
            emitted.last().map(|(_, v)| *v),
            Some(total),
            "survivor {i} must follow the new count"
        );
        assert_eq!(
            worker
                .metrics
                .snapshot()
                .get("active/gauge-wf/running")
                .map(String::as_str),
            Some("8"),
            "survivor {i} must not double the active runs"
        );
    }
    stop_all(workers).await;
}
