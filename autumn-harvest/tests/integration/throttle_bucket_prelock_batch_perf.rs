#![cfg(feature = "db")]
//! Ledger performance fix:
//! `throttle::pre_lock_rate_limit_buckets_for_claimed_batch`'s per-bucket-key
//! `FOR UPDATE` N+1 (issue #1230 Finding 2).
//!
//! The start-throttle scanner's claim-and-fire loop pre-locks every distinct
//! rate-limit-bucket row a claimed batch references, in sorted order, before
//! firing any row -- a deliberate deadlock-freedom measure (see the doc
//! comment on `pre_lock_rate_limit_buckets_for_claimed_batch`). The
//! pre-fix shape issued one `SELECT key FROM harvest_rate_limit_buckets
//! WHERE key = $1 FOR UPDATE` round trip PER distinct bucket key in the
//! batch, instead of a single `= ANY($1)` statement -- an N+1 that runs on
//! every scanner tick that finds ready work, not on a rare path.
//!
//! This harness drives that scanner tick end to end through the public
//! `throttle::fire_due_throttled_starts` entry point, against a
//! production-shaped fixture: 1,000 distinct tenant bucket keys (realistic
//! multi-tenant cardinality) with skewed backlog depth (200 "hot" tenants
//! carrying a 5-deep backlog, 800 "normal" tenants carrying 1 row each --
//! 1,800 pending rows total), all with fully-refilled buckets so a full
//! `THROTTLE_FIRE_BATCH_SIZE`-row batch is admissible and claims exactly
//! 100 distinct bucket keys (the worst case for this N+1: as many round
//! trips as the batch size allows). Evidence is `pg_stat_statements` call
//! counts, captured immediately before and after the measured tick, exactly
//! as `backup_verify_refs_batch_perf.rs` / `child_fanout_batch_perf.rs` do
//! it. Each run gets a fresh, uniquely-named database.

use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::throttle::{THROTTLE_FIRE_BATCH_SIZE, bucket_key, fire_due_throttled_starts};
use chrono::{Duration as ChronoDuration, Utc};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

// ── DB bootstrap (mirrors backup_verify_refs_batch_perf.rs) ─────────────────

type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    use testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        // Preload `pg_stat_statements` so this harness also works on the
        // pure-Docker fallback path, mirroring `backup_verify_refs_batch_perf.rs`.
        .with_cmd([
            "-c",
            "shared_preload_libraries=pg_stat_statements",
            "-c",
            "fsync=off",
        ])
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn create_fresh_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;

    let (prefix, _) = admin_url.rsplit_once('/').expect("url has a db segment");
    let url = format!("{prefix}/{name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migration bundle");
    drop(conn);
    url
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

// ── pg_stat_statements capture (mirrors backup_verify_refs_batch_perf.rs) ───

#[derive(diesel::QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
}

async fn ensure_pg_stat_statements(conn: &mut AsyncPgConnection) {
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(conn)
        .await;
}

async fn reset_stats_for_db(conn: &mut AsyncPgConnection, db_name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect(
        "pg_stat_statements_reset(...) failed -- the HARVEST_TEST_DATABASE_URL role must be \
         able to reset statistics (superuser, or granted EXECUTE on this function)",
    );
}

async fn snapshot_statements(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%'"
    ))
    .load(conn)
    .await
    .expect(
        "pg_stat_statements query failed -- it must be preloaded via shared_preload_libraries \
         for this capture to produce real evidence rather than fail outright",
    )
}

/// Total `calls` across every statement whose normalized text contains `needle`
/// (case-insensitive).
fn calls_containing(rows: &[StatRow], needle: &str) -> i64 {
    let needle = needle.to_ascii_lowercase();
    rows.iter()
        .filter(|r| r.query.to_ascii_lowercase().contains(&needle))
        .map(|r| r.calls)
        .sum()
}

async fn scalar_i64(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query(sql)
        .get_result::<N>(conn)
        .await
        .expect("scalar query")
        .n
}

// ── Fixture seeding ──────────────────────────────────────────────────────

/// A fully-refilled bucket: `tokens == burst`, so every claimed row is
/// admissible regardless of the exact accrual formula.
async fn seed_bucket(conn: &mut AsyncPgConnection, key: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets \
         (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, 10, 100, 100, NOW()) \
         ON CONFLICT (key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(conn)
    .await
    .expect("seed bucket");
}

async fn seed_throttle_row(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    throttle_key: &str,
    bkey: &str,
    workflow_id: &str,
    deferred_at: chrono::DateTime<Utc>,
) {
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle \
         (workflow_name, throttle_key, bucket_key, workflow_id, queue_name, input, \
          start_options, deferred_at, expires_at, shard_id) \
         VALUES ($1, $2, $3, $4, 'default', 'null'::jsonb, '{}'::jsonb, $5, NULL, 0)",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(throttle_key)
    .bind::<diesel::sql_types::Text, _>(bkey)
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .bind::<diesel::sql_types::Timestamptz, _>(deferred_at)
    .execute(conn)
    .await
    .expect("seed throttle row");
}

#[derive(Debug, Default)]
struct NoopMetrics;
impl MetricsRecorder for NoopMetrics {}

/// 1,000 distinct tenant bucket keys. 200 "hot" tenants carry a 5-deep
/// backlog, 800 "normal" tenants carry 1 row each -- 1,800 pending rows,
/// production-shaped cardinality skew for a busy multi-tenant deployment.
const N_KEYS: i64 = 1_000;
const HOT_KEYS: i64 = 200;
const HOT_DEPTH: i64 = 5;

#[tokio::test]
async fn claimed_batch_prelocks_bucket_keys_with_bounded_round_trips() {
    let (admin, _guard) = setup_server().await;
    let db = unique("throttle_prelock_perf");
    let url = create_fresh_db(&admin, &db).await;
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh database");
    ensure_pg_stat_statements(&mut conn).await;

    // Every key's FIRST (rn=1, earliest `deferred_at`) row lands strictly in
    // key-index order: key 0's is the oldest, key 999's is the youngest.
    // `selected` in the scanner's claim query orders by `(rn ASC, deferred_at
    // ASC)`, so the earliest THROTTLE_FIRE_BATCH_SIZE=100 rn=1 rows -- keys
    // 0..99 -- are exactly what the claimed batch takes: 100 distinct bucket
    // keys, the worst case for this N+1. Extra backlog rows (hot-tenant
    // depth 2..5) get `deferred_at` far in the future of every rn=1 row, so
    // they never displace a distinct key out of the claimed batch.
    let base = Utc::now() - ChronoDuration::hours(2);
    for i in 0..N_KEYS {
        let throttle_key = format!("tenant-{i}");
        let key = bucket_key("throttled_flow", &throttle_key);
        seed_bucket(&mut conn, &key).await;
        seed_throttle_row(
            &mut conn,
            "throttled_flow",
            &throttle_key,
            &key,
            &format!("wf-{i}-0"),
            base + ChronoDuration::seconds(i),
        )
        .await;
        if i < HOT_KEYS {
            for depth in 1..HOT_DEPTH {
                seed_throttle_row(
                    &mut conn,
                    "throttled_flow",
                    &throttle_key,
                    &key,
                    &format!("wf-{i}-{depth}"),
                    base + ChronoDuration::seconds(10 * N_KEYS + i * 10 + depth),
                )
                .await;
            }
        }
    }

    let backlog_before =
        scalar_i64(&mut conn, "SELECT COUNT(*) FROM harvest_start_throttle").await;
    assert_eq!(
        backlog_before,
        N_KEYS + HOT_KEYS * (HOT_DEPTH - 1),
        "fixture seeding sanity check"
    );

    reset_stats_for_db(&mut conn, &db).await;

    let metrics = NoopMetrics;
    let fired = fire_due_throttled_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("fire_due_throttled_starts");

    // Functional correctness first: the claimed batch hits the scanner's own
    // per-tick cap, and every fired row's own bucket had ample tokens, so
    // every claimed row actually starts (none is re-deferred for lack of a
    // token).
    assert_eq!(
        fired, THROTTLE_FIRE_BATCH_SIZE as usize,
        "a fully-admissible 1,000-key backlog should fill the claimed batch to the scanner's \
         own per-tick cap"
    );
    let backlog_after = scalar_i64(&mut conn, "SELECT COUNT(*) FROM harvest_start_throttle").await;
    assert_eq!(
        backlog_after,
        backlog_before - THROTTLE_FIRE_BATCH_SIZE,
        "exactly the claimed batch should be removed from the backlog"
    );

    let rows = snapshot_statements(&mut conn, &db).await;
    let per_key_lock_calls = calls_containing(
        &rows,
        "select key from harvest_rate_limit_buckets where key = $1 for update",
    );
    let batched_lock_calls = calls_containing(
        &rows,
        "select key from harvest_rate_limit_buckets where key = any($1) order by key for update",
    );

    // GREEN: the fixed shape. The whole 100-distinct-key claimed batch
    // pre-locks in one `= ANY($1)` round trip; the old per-key statement is
    // never issued. Measured on this machine: before the fix, `SELECT key
    // FROM harvest_rate_limit_buckets WHERE key = $1 FOR UPDATE` had calls:
    // 100 (one per distinct bucket key in the claimed batch). After the fix,
    // it has calls: 0.
    assert_eq!(
        per_key_lock_calls, 0,
        "the per-bucket-key `WHERE key = $1 FOR UPDATE` pre-lock must not be issued once the \
         batch fix is in place: {rows:#?}"
    );
    assert_eq!(
        batched_lock_calls, 1,
        "a 100-distinct-key claimed batch must pre-lock in exactly one `= ANY($1)` round trip: \
         {rows:#?}"
    );
}
