#![cfg(feature = "db")]
//! Idle rate-limit-bucket GC — issue #1127.
//!
//! `harvest_rate_limit_buckets` rows are auto-registered `ON CONFLICT (key) DO
//! NOTHING` and, before this issue, were never deleted. Two key families embed
//! caller/tenant input — `dyn-rate:{expr}:{resolved}` (#699) and
//! `start-throttle:{workflow}:{key}` (#607) — so the table grew without bound.
//!
//! These tests drive the real retention janitor (`RetentionRuntime::spawn` +
//! `run_now`) against Postgres and pin the sweep's whole safety contract:
//!
//! - AC1/AC8: an idle, full bucket in an unbounded family is collected.
//! - AC2: a bucket that never drained ("continuously full") is collected once
//!   it is also idle; one still being debited is not.
//! - AC3: the window is configurable.
//! - AC5: the sweep is shard-local.
//! - AC7: a collected bucket re-registers on next use and admits work again.
//! - AC12: `dry_run` collects nothing; the count is reported and metered.
//! - R1/R2/R3: a bucket with a live dependent — a non-terminal task, or a
//!   deferred throttled start — is retained (both consumers fail CLOSED on a
//!   missing row, so collecting one would strand work forever).
//! - R4: a partially drained bucket is retained (re-registration would reset
//!   `tokens = burst` and hand out free capacity).
//! - R5: a live TTL'd pacing override (#945) is never silently destroyed.
//! - R6: a bounded static activity bucket is never collected (it re-registers
//!   only at worker startup).
//! - The concurrency interlock: a bucket an *uncommitted* enqueue has touched
//!   is not swept out from under the task that transaction is about to commit.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (single-threaded, each test scrubs first); otherwise a
//! fresh testcontainers Postgres is booted with the full migration bundle.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::retention::{RateLimitBucketGcOutcome, RetentionConfig, RetentionRuntime};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Double, Nullable, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

/// Capturing recorder for `record_rate_limit_buckets_deleted` (issue #1127)
/// plus the end-of-iteration liveness tick used to await a whole tick.
#[derive(Default)]
struct CapturingMetrics {
    buckets_deleted: Mutex<Vec<(String, u64)>>,
    completed_ticks: Mutex<u64>,
}

impl CapturingMetrics {
    fn buckets_deleted(&self) -> Vec<(String, u64)> {
        self.buckets_deleted.lock().unwrap().clone()
    }

    fn deleted_by_family(&self) -> BTreeMap<String, u64> {
        let mut out: BTreeMap<String, u64> = BTreeMap::new();
        for (family, count) in self.buckets_deleted() {
            *out.entry(family).or_insert(0) += count;
        }
        out
    }

    fn completed_ticks(&self) -> u64 {
        *self.completed_ticks.lock().unwrap()
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_rate_limit_buckets_deleted(&self, family: &str, count: u64) {
        self.buckets_deleted
            .lock()
            .unwrap()
            .push((family.to_string(), count));
    }

    fn record_scanner_tick(&self, _scanner: &str, _shard: &str) {
        *self.completed_ticks.lock().unwrap() += 1;
    }
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

/// Swap the database component of a Postgres URL (shard-locality test).
fn with_database(url: &str, database: &str) -> String {
    let (base, _) = url.rsplit_once('/').expect("url must carry a database");
    format!("{base}/{database}")
}

/// A second migrated database, so the shard-locality assertion has two real
/// shard databases rather than two aliases for one.
async fn setup_second_shard(primary: &str) -> (String, String) {
    let name = format!("harvest_rlgc_{}", uuid::Uuid::new_v4().simple());
    let mut admin = connect(primary).await;
    diesel::sql_query(format!("CREATE DATABASE {name}"))
        .execute(&mut admin)
        .await
        .expect("create the second shard database");
    let url = with_database(primary, &name);
    let mut conn = connect(&url).await;
    diesel_async::SimpleAsyncConnection::batch_execute(&mut conn, &autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations to the second shard");
    (url, name)
}

async fn drop_database(primary: &str, name: &str) {
    if let Ok(mut admin) = AsyncPgConnection::establish(primary).await {
        let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
            .execute(&mut admin)
            .await;
    }
}

/// Scrub every table this suite touches so a shared database stays isolated.
async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_start_throttle",
        "DELETE FROM harvest_task_queue",
        "DELETE FROM harvest_rate_limit_buckets",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// One bucket row with fully explicit activity timestamps.
#[allow(clippy::too_many_arguments)]
async fn insert_bucket_at(
    conn: &mut AsyncPgConnection,
    key: &str,
    refill_rate: f64,
    burst: f64,
    tokens: f64,
    last_refilled_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
) {
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets
            (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind::<Text, _>(key)
    .bind::<Double, _>(refill_rate)
    .bind::<Double, _>(burst)
    .bind::<Double, _>(tokens)
    .bind::<Timestamptz, _>(last_refilled_at)
    .bind::<Timestamptz, _>(created_at)
    .bind::<Timestamptz, _>(updated_at)
    .execute(conn)
    .await
    .expect("insert bucket");
}

/// A full bucket whose every activity column is `age` old.
async fn insert_idle_bucket(conn: &mut AsyncPgConnection, key: &str, age: chrono::Duration) {
    let at = Utc::now() - age;
    insert_bucket_at(conn, key, 1.0, 10.0, 10.0, at, at, at).await;
}

async fn set_override(
    conn: &mut AsyncPgConnection,
    key: &str,
    expires_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
) {
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets
            SET override_refill_rate = 99.0, override_burst = 99.0,
                override_expires_at = $2, updated_at = $3
          WHERE key = $1",
    )
    .bind::<Text, _>(key)
    .bind::<Timestamptz, _>(expires_at)
    .bind::<Timestamptz, _>(updated_at)
    .execute(conn)
    .await
    .expect("set override");
}

/// A task row in `state` referencing `rate_limit_key`.
async fn insert_task(conn: &mut AsyncPgConnection, rate_limit_key: &str, state: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_task_queue
            (queue_name, task_type, activity_name, input, state, rate_limit_key)
         VALUES ('default', 'activity', 'send_email', '{}'::jsonb, $2, $1)",
    )
    .bind::<Text, _>(rate_limit_key)
    .bind::<Text, _>(state)
    .execute(conn)
    .await
    .expect("insert task");
}

/// A deferred throttled start pinned to `bucket_key`.
async fn insert_deferred_start(conn: &mut AsyncPgConnection, bucket_key: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id)
         VALUES ('onboarding', 'acme', $1, 'wf-1')",
    )
    .bind::<Text, _>(bucket_key)
    .execute(conn)
    .await
    .expect("insert deferred start");
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

#[derive(diesel::QueryableByName)]
struct KeyRow {
    #[diesel(sql_type = Text)]
    key: String,
}

#[derive(diesel::QueryableByName)]
struct TsRow {
    #[diesel(sql_type = Timestamptz)]
    updated_at: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct TokensRow {
    #[diesel(sql_type = Double)]
    tokens: f64,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn surviving_keys(conn: &mut AsyncPgConnection) -> Vec<String> {
    diesel::sql_query("SELECT key FROM harvest_rate_limit_buckets ORDER BY key")
        .load::<KeyRow>(conn)
        .await
        .expect("load buckets")
        .into_iter()
        .map(|r| r.key)
        .collect()
}

async fn bucket_exists(conn: &mut AsyncPgConnection, key: &str) -> bool {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<Text, _>(key)
        .get_result::<CountRow>(conn)
        .await
        .expect("count bucket")
        .n
        > 0
}

async fn bucket_updated_at(conn: &mut AsyncPgConnection, key: &str) -> DateTime<Utc> {
    diesel::sql_query("SELECT updated_at FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<Text, _>(key)
        .get_result::<TsRow>(conn)
        .await
        .expect("bucket must exist")
        .updated_at
}

async fn bucket_last_registered_at(
    conn: &mut AsyncPgConnection,
    key: &str,
) -> Option<DateTime<Utc>> {
    #[derive(diesel::QueryableByName)]
    struct MaybeTsRow {
        #[diesel(sql_type = Nullable<Timestamptz>)]
        last_registered_at: Option<DateTime<Utc>>,
    }
    diesel::sql_query("SELECT last_registered_at FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<Text, _>(key)
        .get_result::<MaybeTsRow>(conn)
        .await
        .expect("bucket must exist")
        .last_registered_at
}

async fn bucket_tokens(conn: &mut AsyncPgConnection, key: &str) -> f64 {
    diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<Text, _>(key)
        .get_result::<TokensRow>(conn)
        .await
        .expect("bucket must exist")
        .tokens
}

// ---------------------------------------------------------------------------
// Tick driver
// ---------------------------------------------------------------------------

/// Config with every retention pass except the bucket GC switched off, so the
/// tick's observable effect is exactly this sweep.
fn gc_only(window: Duration) -> RetentionConfig {
    RetentionConfig {
        max_age_secs: None,
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        ..RetentionConfig::default()
    }
    .with_rate_limit_bucket_retention(window)
}

async fn run_one_tick_on(
    pools: ShardedDbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
) -> autumn_harvest::retention::RetentionTickResult {
    let runtime = RetentionRuntime::spawn(
        pools,
        config,
        Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
        None,
        None,
    )
    .expect("retention runtime should spawn when the bucket GC is active");
    runtime.run_now();

    // The end-of-iteration liveness tick (#797) is unconditional and runs last,
    // so observing it is exactly "the whole iteration completed" — waiting on
    // `ran_at` would race the passes that run after the history phase.
    let baseline = metrics.completed_ticks();
    let mut result = None;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if metrics.completed_ticks() <= baseline {
            continue;
        }
        let snap = runtime.monitor().snapshot();
        // `.iter().find(...)`, never `.first()`: diesel's blanket `RunQueryDsl`
        // impl shadows `Vec::first` in a diesel-importing scope.
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0) {
            result = Some(r.clone());
        }
        break;
    }
    runtime.shutdown();
    result.expect("retention tick did not complete in time")
}

async fn run_one_tick(
    pool: DbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
) -> autumn_harvest::retention::RetentionTickResult {
    run_one_tick_on(ShardedDbPool::single(pool), config, metrics).await
}

/// The whole per-shard snapshot, for the multi-shard assertions.
async fn run_one_tick_snapshot(
    pools: ShardedDbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
) -> Vec<autumn_harvest::retention::RetentionTickResult> {
    let runtime = RetentionRuntime::spawn(
        pools,
        config,
        Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
        None,
        None,
    )
    .expect("retention runtime should spawn when the bucket GC is active");
    runtime.run_now();
    let baseline = metrics.completed_ticks();
    let mut snap = Vec::new();
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if metrics.completed_ticks() <= baseline {
            continue;
        }
        snap = runtime.monitor().snapshot().per_shard;
        break;
    }
    runtime.shutdown();
    assert!(!snap.is_empty(), "retention tick did not complete in time");
    snap
}

/// The GC outcome a tick reported, which must always be present when the pass
/// is enabled — `None` is reserved for "the GC is switched off".
const fn gc(result: &autumn_harvest::retention::RetentionTickResult) -> &RateLimitBucketGcOutcome {
    result
        .rate_limit_bucket_gc
        .as_ref()
        .expect("an enabled GC must report an outcome every tick")
}

const fn collected(result: &autumn_harvest::retention::RetentionTickResult) -> u64 {
    gc(result).collected
}

const WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

const fn day_old() -> chrono::Duration {
    chrono::Duration::hours(48)
}

// ---------------------------------------------------------------------------
// AC1 / AC2 / AC8 — inert buckets in the unbounded families are collected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn idle_full_buckets_in_both_unbounded_families_are_collected() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_idle_bucket(&mut conn, "dyn-rate:input.tenant_id:acme", day_old()).await;
    insert_idle_bucket(&mut conn, "start-throttle:onboarding:acme", day_old()).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, gc_only(WINDOW), Arc::clone(&metrics)).await;

    assert_eq!(
        surviving_keys(&mut conn).await,
        Vec::<String>::new(),
        "both unbounded families must be collected"
    );
    assert_eq!(collected(&result), 2);
    assert_eq!(
        metrics.deleted_by_family(),
        BTreeMap::from([
            ("dyn-rate".to_string(), 1),
            ("start-throttle".to_string(), 1)
        ]),
        "the counter is labelled by family — never by the unbounded key itself"
    );
}

#[tokio::test]
async fn a_bucket_that_never_drained_is_collected_once_it_is_idle() {
    // AC2 "or continuously full": a bucket sitting at burst with no debit
    // inside the window is inert, and re-registering it reproduces exactly the
    // row that was deleted.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let at = Utc::now() - day_old();
    insert_bucket_at(&mut conn, "dyn-rate:t:full", 5.0, 20.0, 20.0, at, at, at).await;

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;

    assert_eq!(collected(&result), 1);
    assert!(!bucket_exists(&mut conn, "dyn-rate:t:full").await);
}

#[tokio::test]
async fn a_recently_used_bucket_is_retained() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let recent = Utc::now() - chrono::Duration::minutes(5);
    let old = Utc::now() - day_old();
    // Idle by `updated_at`/`created_at`, but debited five minutes ago.
    insert_bucket_at(
        &mut conn,
        "dyn-rate:t:hot",
        1.0,
        10.0,
        10.0,
        recent,
        old,
        old,
    )
    .await;
    // Idle by `last_refilled_at`, but its override was written five minutes ago
    // — AC4's `updated_at` half of the idleness clock.
    insert_bucket_at(
        &mut conn,
        "dyn-rate:t:touched",
        1.0,
        10.0,
        10.0,
        old,
        recent,
        old,
    )
    .await;

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;

    assert_eq!(collected(&result), 0);
    assert_eq!(
        surviving_keys(&mut conn).await,
        vec!["dyn-rate:t:hot", "dyn-rate:t:touched"]
    );
}

// ---------------------------------------------------------------------------
// R4 / AC10 — a partially drained bucket is never collected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_partially_drained_bucket_is_retained() {
    // Deleting one hands out free capacity: re-registration resets
    // `tokens = burst`. With a zero refill rate the bucket can never refill to
    // full on its own, so this stays observably below burst.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let at = Utc::now() - day_old();
    insert_bucket_at(&mut conn, "dyn-rate:t:drained", 0.0, 10.0, 3.0, at, at, at).await;

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;

    assert_eq!(collected(&result), 0);
    assert!(bucket_exists(&mut conn, "dyn-rate:t:drained").await);
    assert!(
        (bucket_tokens(&mut conn, "dyn-rate:t:drained").await - 3.0).abs() < f64::EPSILON,
        "a retained bucket's pacing state must be left exactly as it was"
    );
}

// ---------------------------------------------------------------------------
// R1 / R2 / R3 — live dependents pin their bucket
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_bucket_with_a_non_terminal_task_is_retained() {
    // Both the claim-time gate and `try_consume_rate_limit_token` fail CLOSED
    // on a missing row, and nothing re-registers a bucket for an
    // already-enqueued task — so collecting one strands that task forever.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    for (key, state) in [
        ("dyn-rate:t:pending", "PENDING"),
        ("dyn-rate:t:running", "RUNNING"),
    ] {
        insert_idle_bucket(&mut conn, key, day_old()).await;
        insert_task(&mut conn, key, state).await;
    }
    // Terminal tasks pin nothing.
    for (key, state) in [
        ("dyn-rate:t:done", "COMPLETED"),
        ("dyn-rate:t:failed", "FAILED"),
        ("dyn-rate:t:cancelled", "CANCELLED"),
    ] {
        insert_idle_bucket(&mut conn, key, day_old()).await;
        insert_task(&mut conn, key, state).await;
    }

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;

    assert_eq!(collected(&result), 3);
    assert_eq!(
        surviving_keys(&mut conn).await,
        vec!["dyn-rate:t:pending", "dyn-rate:t:running"],
        "every non-terminal task pins its bucket; a terminal one does not"
    );
}

#[tokio::test]
async fn a_bucket_with_a_deferred_throttled_start_is_retained() {
    // A deferred start whose bucket vanished can never debit a token, so it
    // would sit deferred forever (it is not even expired unless it carries a
    // schedule_to_start deadline).
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_idle_bucket(&mut conn, "start-throttle:onboarding:acme", day_old()).await;
    insert_deferred_start(&mut conn, "start-throttle:onboarding:acme").await;
    insert_idle_bucket(&mut conn, "start-throttle:onboarding:idle", day_old()).await;

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;

    assert_eq!(collected(&result), 1);
    assert_eq!(
        surviving_keys(&mut conn).await,
        vec!["start-throttle:onboarding:acme"]
    );
}

// ---------------------------------------------------------------------------
// R5 / AC11 — operator overrides survive
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_live_override_pins_its_bucket_and_an_expired_one_does_not() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let old = Utc::now() - day_old();
    insert_idle_bucket(&mut conn, "dyn-rate:t:live", day_old()).await;
    set_override(
        &mut conn,
        "dyn-rate:t:live",
        Utc::now() + chrono::Duration::hours(2),
        old,
    )
    .await;

    insert_idle_bucket(&mut conn, "dyn-rate:t:lapsed", day_old()).await;
    set_override(
        &mut conn,
        "dyn-rate:t:lapsed",
        Utc::now() - chrono::Duration::hours(2),
        old,
    )
    .await;

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;

    assert_eq!(collected(&result), 1);
    assert_eq!(
        surviving_keys(&mut conn).await,
        vec!["dyn-rate:t:live"],
        "a live TTL'd override must never be silently destroyed by the GC"
    );
}

// ---------------------------------------------------------------------------
// R6 — bounded static keys are out of scope
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bounded_static_activity_buckets_are_never_collected() {
    // A static activity bucket is registered ONLY at worker startup, so
    // collecting it would stall the next enqueue behind the fail-closed gate
    // until a restart. It is also bounded (one per registered activity), so it
    // is not the growth this issue is about.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    for key in [
        "send_email",
        "billing.charge",
        "dyn-rate",
        "start-throttler:x",
    ] {
        insert_idle_bucket(&mut conn, key, chrono::Duration::days(400)).await;
    }

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;

    assert_eq!(collected(&result), 0);
    assert_eq!(surviving_keys(&mut conn).await.len(), 4);
}

// ---------------------------------------------------------------------------
// AC3 / AC12 — window, dry-run, batching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_configured_window_is_honoured() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_idle_bucket(&mut conn, "dyn-rate:t:a", chrono::Duration::hours(3)).await;

    // A 24h window retains a 3h-idle bucket.
    let result = run_one_tick(
        pool.clone(),
        gc_only(WINDOW),
        Arc::new(CapturingMetrics::default()),
    )
    .await;
    assert_eq!(collected(&result), 0);
    assert!(bucket_exists(&mut conn, "dyn-rate:t:a").await);

    // A 1h window collects it.
    let result = run_one_tick(
        pool,
        gc_only(Duration::from_secs(60 * 60)),
        Arc::new(CapturingMetrics::default()),
    )
    .await;
    assert_eq!(collected(&result), 1);
    assert!(!bucket_exists(&mut conn, "dyn-rate:t:a").await);
}

#[tokio::test]
async fn dry_run_previews_without_deleting_or_metering() {
    // A collector that is on by default must be previewable: `dry_run` reports
    // what a real pass WOULD collect, deletes nothing, and moves no counter an
    // operator reads as work done.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_idle_bucket(&mut conn, "dyn-rate:t:a", day_old()).await;
    insert_idle_bucket(&mut conn, "dyn-rate:t:b", day_old()).await;
    // ...and one the real pass would refuse, so the preview is proven to run
    // the sweep's own predicates rather than a looser count.
    insert_idle_bucket(&mut conn, "dyn-rate:t:pinned", day_old()).await;
    insert_task(&mut conn, "dyn-rate:t:pinned", "PENDING").await;

    let config = RetentionConfig {
        dry_run: true,
        ..gc_only(WINDOW)
    };
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    let outcome = gc(&result);
    assert!(outcome.dry_run, "the outcome must say it is a forecast");
    assert_eq!(outcome.collected, 2, "the two inert buckets are forecast");
    assert_eq!(surviving_keys(&mut conn).await.len(), 3, "nothing deleted");
    assert!(
        metrics.buckets_deleted().is_empty(),
        "a preview must not move the deletion counter"
    );
}

#[tokio::test]
async fn the_sweep_is_batched_and_converges_within_a_tick() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    for i in 0..5 {
        insert_idle_bucket(&mut conn, &format!("dyn-rate:t:{i}"), day_old()).await;
    }

    let config = RetentionConfig {
        batch_size: 2,
        ..gc_only(WINDOW)
    };
    let result = run_one_tick(pool, config, Arc::new(CapturingMetrics::default())).await;

    assert_eq!(
        collected(&result),
        5,
        "the drain loop must converge rather than leaving a permanent backlog"
    );
    assert!(surviving_keys(&mut conn).await.is_empty());
}

// ---------------------------------------------------------------------------
// AC7 — a collected bucket re-registers on next use
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_collected_bucket_re_registers_and_admits_work_again() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let key = "dyn-rate:input.tenant_id:acme";
    insert_idle_bucket(&mut conn, key, day_old()).await;

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;
    assert_eq!(collected(&result), 1);

    // Fail-closed while absent — this is exactly why every live dependent pins
    // its bucket.
    assert!(
        !autumn_harvest::queue::try_consume_rate_limit_token(&mut conn, key)
            .await
            .expect("consume"),
        "a missing bucket must not admit work"
    );

    // The next enqueue re-registers it in its own transaction, and dispatch
    // proceeds exactly as before the collection.
    autumn_harvest::queue::ensure_rate_limit_bucket(&mut conn, key, 1.0, 10.0)
        .await
        .expect("re-register");
    assert!(bucket_exists(&mut conn, key).await);
    assert!(
        autumn_harvest::queue::try_consume_rate_limit_token(&mut conn, key)
            .await
            .expect("consume"),
        "a re-registered bucket admits work again"
    );
}

#[tokio::test]
async fn ensuring_a_stale_bucket_touches_it_so_the_sweep_cannot_race_the_enqueue() {
    // `ON CONFLICT DO NOTHING` takes no lock on the existing row, so an enqueue
    // still uncommitted when the sweep took its snapshot could be stranded.
    // Touching a stale row makes it both non-idle and row-locked for the
    // duration of the enqueue transaction.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let key = "dyn-rate:t:stale";
    insert_idle_bucket(&mut conn, key, day_old()).await;
    // A pre-#1127 row has never been registered at all, which must still be
    // touchable — otherwise every bucket that predates the upgrade is
    // permanently unprotected against the race.
    assert_eq!(bucket_last_registered_at(&mut conn, key).await, None);
    let updated_before = bucket_updated_at(&mut conn, key).await;

    autumn_harvest::queue::ensure_rate_limit_bucket(&mut conn, key, 1.0, 10.0)
        .await
        .expect("ensure");
    let registered_after = bucket_last_registered_at(&mut conn, key)
        .await
        .expect("a stale bucket must be touched");
    assert!(registered_after > Utc::now() - chrono::Duration::minutes(1));
    assert_eq!(
        bucket_updated_at(&mut conn, key).await,
        updated_before,
        "the touch must not disturb `updated_at`, which still means 'an \
         operator or config write changed this bucket'"
    );

    // ...and the touch alone is enough to keep the sweep off it.
    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;
    assert_eq!(collected(&result), 0);
    assert!(bucket_exists(&mut conn, key).await);

    // A FRESH bucket is not written at all — the touch must never become a hot
    // path write (nor, therefore, a hot path row LOCK) on every enqueue.
    let touched = bucket_last_registered_at(&mut conn, key).await;
    autumn_harvest::queue::ensure_rate_limit_bucket(&mut conn, key, 1.0, 10.0)
        .await
        .expect("ensure");
    assert_eq!(
        bucket_last_registered_at(&mut conn, key).await,
        touched,
        "re-ensuring a fresh bucket must not write"
    );
}

#[tokio::test]
async fn an_uncommitted_enqueue_is_not_swept_out_from_under_its_own_task() {
    // The regression this whole interlock exists for. The sweep's dependent
    // anti-joins run against ONE snapshot, so an enqueue transaction that has
    // not committed yet is invisible to them: its task row cannot be seen, and
    // under the pre-#1127 `ON CONFLICT DO NOTHING` its `ensure` left the
    // bucket row unlocked as well. The sweep would then delete the bucket, the
    // enqueue would commit a task referencing nothing, and — because the claim
    // gate and `try_consume_rate_limit_token` both fail CLOSED, with nothing to
    // re-register the row — that task would never run again.
    //
    // Verified against a real concurrent transaction rather than reasoned
    // about: the ensure's `DO UPDATE` touch locks the row, and the sweep's
    // `FOR UPDATE SKIP LOCKED` skips it.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let key = "dyn-rate:input.tenant_id:racer";
    insert_idle_bucket(&mut conn, key, day_old()).await;

    // Raw BEGIN/COMMIT so the enqueue transaction stays open across the sweep.
    let mut enqueue = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut enqueue)
        .await
        .expect("begin");
    autumn_harvest::queue::ensure_rate_limit_bucket(&mut enqueue, key, 1.0, 10.0)
        .await
        .expect("ensure");
    insert_task(&mut enqueue, key, "PENDING").await;

    // ...and the sweep runs while it is still uncommitted.
    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;
    assert_eq!(
        collected(&result),
        0,
        "a bucket an in-flight enqueue has touched must not be collected"
    );

    diesel::sql_query("COMMIT")
        .execute(&mut enqueue)
        .await
        .expect("commit");

    assert!(
        bucket_exists(&mut conn, key).await,
        "the committed task must still have its bucket"
    );
    assert!(
        autumn_harvest::queue::try_consume_rate_limit_token(&mut conn, key)
            .await
            .expect("consume"),
        "and dispatch for it still admits work"
    );
}

#[tokio::test]
async fn the_per_tick_batch_budget_is_enforced_and_the_next_tick_drains_the_rest() {
    // The bounded budget stops the first sweep of a table that grew for months
    // from turning one janitor tick into an open-ended delete loop holding a
    // pooled connection. Without the cap this test's first tick would collect
    // all of them.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let cap = autumn_harvest::queue::MAX_RATE_LIMIT_SWEEP_BATCHES_PER_TICK;
    let total = cap + 5;
    for i in 0..total {
        insert_idle_bucket(&mut conn, &format!("dyn-rate:t:{i:04}"), day_old()).await;
    }

    // batch_size 1 => one bucket per batch => exactly `cap` collected per tick.
    let config = RetentionConfig {
        batch_size: 1,
        ..gc_only(WINDOW)
    };
    let result = run_one_tick(
        pool.clone(),
        config.clone(),
        Arc::new(CapturingMetrics::default()),
    )
    .await;
    assert_eq!(collected(&result), cap as u64, "one tick spends its budget");
    assert_eq!(surviving_keys(&mut conn).await.len(), total - cap);

    let result = run_one_tick(pool, config, Arc::new(CapturingMetrics::default())).await;
    assert_eq!(collected(&result), 5, "successive ticks converge");
    assert!(surviving_keys(&mut conn).await.is_empty());
}

#[tokio::test]
async fn a_bucket_with_an_operator_written_baseline_is_retained() {
    // `POST /admin/rate-limits/{key}` writes a PERMANENT baseline straight onto
    // the row and validates nothing against the registry, so an operator can
    // clamp a per-tenant key with it. Collecting such a row would silently
    // revert the clamp to the code-declared rate the next time that tenant
    // appeared — the same intent-destruction the live-override guard prevents.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_idle_bucket(&mut conn, "dyn-rate:t:clamped", day_old()).await;
    let old = Utc::now() - day_old();
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets
            SET refill_rate = 0.5, baseline_set_at = $2, updated_at = $2
          WHERE key = $1",
    )
    .bind::<Text, _>("dyn-rate:t:clamped")
    .bind::<Timestamptz, _>(old)
    .execute(&mut conn)
    .await
    .expect("operator baseline");
    insert_idle_bucket(&mut conn, "dyn-rate:t:untouched", day_old()).await;

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;

    assert_eq!(collected(&result), 1);
    assert_eq!(
        surviving_keys(&mut conn).await,
        vec!["dyn-rate:t:clamped"],
        "an operator's permanent baseline must outlive the GC"
    );
}

#[tokio::test]
async fn an_uncommitted_debit_is_not_swept_out_from_under_the_dispatch_that_made_it() {
    // The sweep must never take a bucket a concurrent claim/dispatch is
    // debiting: it would delete a row mid-transaction and leave that dispatch's
    // accounting pointing at nothing.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let key = "dyn-rate:t:debiting";
    insert_idle_bucket(&mut conn, key, day_old()).await;

    let mut dispatch = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut dispatch)
        .await
        .expect("begin");
    assert!(
        autumn_harvest::queue::try_consume_rate_limit_token(&mut dispatch, key)
            .await
            .expect("consume"),
        "the idle bucket is full, so the debit succeeds"
    );

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;
    assert_eq!(
        collected(&result),
        0,
        "a bucket an in-flight debit holds must not be collected"
    );

    diesel::sql_query("COMMIT")
        .execute(&mut dispatch)
        .await
        .expect("commit");
    assert!(bucket_exists(&mut conn, key).await);
}

#[tokio::test]
async fn a_slowly_refilling_bucket_below_burst_is_retained() {
    // The realistic shape of the partial-drain guard: a positive (production-
    // legal) refill rate so low that a bucket idle well past the window is
    // still below burst. Collecting it would reset `tokens = burst` and hand
    // the tenant a full bucket it had not earned.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let at = Utc::now() - day_old();
    // 0.001 tokens/s over 48h accrues ~172 of a 100_000 burst.
    insert_bucket_at(
        &mut conn,
        "dyn-rate:t:slow",
        0.001,
        100_000.0,
        10.0,
        at,
        at,
        at,
    )
    .await;

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;
    assert_eq!(collected(&result), 0);
    assert!(bucket_exists(&mut conn, "dyn-rate:t:slow").await);
}

#[tokio::test]
async fn re_ensuring_a_live_bucket_never_disturbs_its_pacing_state() {
    // The registration touch writes `last_registered_at` and nothing else: a
    // partially drained bucket keeps its tokens, its declared rate/burst, and
    // the operator-facing `updated_at` stamp that answers "when was this limit
    // last changed".
    let (url, _c) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let key = "dyn-rate:t:live";
    let at = Utc::now() - day_old();
    insert_bucket_at(&mut conn, key, 1.0, 10.0, 3.0, at, at, at).await;
    let updated_before = bucket_updated_at(&mut conn, key).await;

    // A different declared rate on re-registration must NOT reset a live bucket.
    autumn_harvest::queue::ensure_rate_limit_bucket(&mut conn, key, 99.0, 99.0)
        .await
        .expect("ensure");

    assert!(
        (bucket_tokens(&mut conn, key).await - 3.0).abs() < f64::EPSILON,
        "tokens untouched"
    );
    assert_eq!(
        bucket_updated_at(&mut conn, key).await,
        updated_before,
        "`updated_at` still means 'an operator or config write changed this'"
    );
}

#[tokio::test]
async fn the_throttle_path_registers_through_the_same_interlocked_statement() {
    // `throttle::ensure_throttle_bucket` delegates to
    // `queue::ensure_rate_limit_bucket`; a second copy of the statement there
    // would silently miss the touch, and a deferred start whose bucket was
    // collected mid-flight can never debit a token again.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let key = autumn_harvest::throttle::bucket_key("onboarding", "acme");
    insert_idle_bucket(&mut conn, &key, day_old()).await;

    // Admitting a start through the real throttle API ensures the bucket.
    autumn_harvest::throttle::reserve_or_defer(
        &mut conn,
        autumn_harvest::throttle::AdmitThrottleParams {
            workflow_name: "onboarding",
            throttle_key: "acme",
            workflow_id: "wf-1",
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: autumn_harvest::debounce::DebounceStartOptions::default(),
            refill_per_sec: 1.0,
            burst: 10.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("reserve");

    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;
    assert_eq!(
        collected(&result),
        0,
        "a bucket the throttle path just registered must not be collected"
    );
}

#[tokio::test]
async fn the_throttle_backlog_append_path_also_interlocks_the_bucket() {
    // Codex review round 1, P1. `reserve_or_defer`'s FIFO fast path used to
    // skip `ensure_throttle_bucket` whenever a backlog already existed, so the
    // append path touched the bucket not at all: an observed backlog row that
    // the scanner drops between the check and the insert leaves the sweep
    // seeing an idle, full, dependent-free bucket — and the pending row that
    // commits a moment later references a bucket that no longer exists.
    // `fire_claimed_throttle_row` fails closed on a missing bucket, so that
    // start sits deferred forever.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let key = autumn_harvest::throttle::bucket_key("onboarding", "acme");
    insert_idle_bucket(&mut conn, &key, day_old()).await;
    // A pre-existing backlog row puts the next admission on the append path...
    insert_deferred_start(&mut conn, &key).await;
    let registered_before = bucket_last_registered_at(&mut conn, &key).await;

    let mut admitting = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut admitting)
        .await
        .expect("begin");
    autumn_harvest::throttle::reserve_or_defer(
        &mut admitting,
        autumn_harvest::throttle::AdmitThrottleParams {
            workflow_name: "onboarding",
            throttle_key: "acme",
            workflow_id: "wf-appended",
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: autumn_harvest::debounce::DebounceStartOptions::default(),
            refill_per_sec: 1.0,
            burst: 10.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("append to the backlog");

    // ...and while that append is still uncommitted, a sweep that could not see
    // its pending row must still leave the bucket alone. (The pre-existing
    // backlog row is deleted first so the anti-join cannot be what saves it —
    // the interlock has to.)
    diesel::sql_query("DELETE FROM harvest_start_throttle WHERE workflow_id = 'wf-1'")
        .execute(&mut conn)
        .await
        .expect("drop the old backlog row");
    let result = run_one_tick(pool, gc_only(WINDOW), Arc::new(CapturingMetrics::default())).await;
    assert_eq!(
        collected(&result),
        0,
        "the append path must interlock the bucket it is about to depend on"
    );

    diesel::sql_query("COMMIT")
        .execute(&mut admitting)
        .await
        .expect("commit");
    assert!(
        bucket_exists(&mut conn, &key).await,
        "the appended start must still have a bucket to debit"
    );
    assert!(
        bucket_last_registered_at(&mut conn, &key).await > registered_before,
        "the append path registers the bucket like every other dependent"
    );
}

#[tokio::test]
async fn the_dry_run_preview_forecasts_the_whole_per_tick_budget() {
    // Codex review round 1, P2. A preview deletes nothing, so without a keyset
    // cursor it re-reads its first batch forever and can only ever forecast
    // `batch_size` — against a real pass that spends the whole per-tick budget.
    // An operator derisking a 50k-row tick by previewing 1k of it is being
    // misled by exactly the tool meant to prevent that.
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    for i in 0..5 {
        insert_idle_bucket(&mut conn, &format!("dyn-rate:t:{i:04}"), day_old()).await;
    }

    let config = RetentionConfig {
        dry_run: true,
        batch_size: 2,
        ..gc_only(WINDOW)
    };
    let result = run_one_tick(pool.clone(), config, Arc::new(CapturingMetrics::default())).await;
    assert_eq!(
        collected(&result),
        5,
        "the forecast must page past the first batch"
    );
    assert_eq!(
        surviving_keys(&mut conn).await.len(),
        5,
        "and delete nothing"
    );

    // ...and the forecast matches what the real pass then collects.
    let real = RetentionConfig {
        batch_size: 2,
        ..gc_only(WINDOW)
    };
    let result = run_one_tick(pool, real, Arc::new(CapturingMetrics::default())).await;
    assert_eq!(collected(&result), 5);
    assert!(surviving_keys(&mut conn).await.is_empty());
}

// ---------------------------------------------------------------------------
// AC5 — shard-local
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_sweep_is_shard_local() {
    let (url, _c) = setup_db().await;
    let (second_url, second_name) = setup_second_shard(&url).await;

    let mut a = connect(&url).await;
    let mut b = connect(&second_url).await;
    scrub(&mut a).await;
    scrub(&mut b).await;

    // Same key on both shards: an idle one on shard 0, a pinned one on shard 1.
    insert_idle_bucket(&mut a, "dyn-rate:t:acme", day_old()).await;
    insert_idle_bucket(&mut b, "dyn-rate:t:acme", day_old()).await;
    insert_task(&mut b, "dyn-rate:t:acme", "PENDING").await;

    let pools = ShardedDbPool::from_map(
        BTreeMap::from([
            (ShardId::new(0), build_pool(&url)),
            (ShardId::new(1), build_pool(&second_url)),
        ]),
        ShardId::new(0),
    );
    let snapshot = run_one_tick_snapshot(
        pools,
        gc_only(WINDOW),
        Arc::new(CapturingMetrics::default()),
    )
    .await;

    assert!(
        !bucket_exists(&mut a, "dyn-rate:t:acme").await,
        "shard 0's inert bucket is collected"
    );
    assert!(
        bucket_exists(&mut b, "dyn-rate:t:acme").await,
        "shard 1's bucket is judged against shard 1's own dependents"
    );
    // Each shard reports its OWN outcome — a fleet-wide total would hide which
    // shard is doing the collecting (or failing to).
    let by_shard: BTreeMap<u16, u64> = snapshot
        .iter()
        .map(|r| (r.shard, gc(r).collected))
        .collect();
    assert_eq!(by_shard, BTreeMap::from([(0, 1), (1, 0)]));

    drop(b);
    drop_database(&url, &second_name).await;
}

#[tokio::test]
async fn a_shard_whose_sweep_fails_reports_the_failure_instead_of_a_silent_zero() {
    // A permanently-failing shard must not read as an idle one: without the
    // reported error, `GET /admin/retention` would keep serving a zero while
    // that shard's table grew unbounded — the exact bug the GC exists to fix,
    // made invisible. (A missing DELETE grant on a least-privilege role is the
    // realistic cause; an unreachable database is the reproducible one.)
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    insert_idle_bucket(&mut conn, "dyn-rate:t:healthy", day_old()).await;

    let broken = build_pool("postgres://postgres:postgres@127.0.0.1:1/nonexistent");
    let pools = ShardedDbPool::from_map(
        BTreeMap::from([
            (ShardId::new(0), build_pool(&url)),
            (ShardId::new(1), broken),
        ]),
        ShardId::new(0),
    );
    let snapshot = run_one_tick_snapshot(
        pools,
        gc_only(WINDOW),
        Arc::new(CapturingMetrics::default()),
    )
    .await;

    let healthy = snapshot.iter().find(|r| r.shard == 0).expect("shard 0");
    assert_eq!(gc(healthy).collected, 1);
    assert!(gc(healthy).error.is_none());

    let failed = snapshot.iter().find(|r| r.shard == 1).expect("shard 1");
    assert!(
        gc(failed).error.is_some(),
        "an unreachable shard must report WHY it collected nothing"
    );
    assert_eq!(gc(failed).collected, 0);

    // ...and one shard's failure never stops another shard's collection.
    assert!(!bucket_exists(&mut conn, "dyn-rate:t:healthy").await);
}
