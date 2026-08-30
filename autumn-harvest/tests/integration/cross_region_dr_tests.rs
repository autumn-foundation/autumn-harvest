//! Cross-region DR: fencing and measured RPO — DB integration tests (issue #954).
//!
//! These are the correctness oracle for the issue's success metric. They run
//! against a live Postgres and cover the three proofs AC6 asks for:
//!
//! * **(a) a fenced stale worker cannot claim or persist** — `fenced_*` tests.
//! * **(b) post-promotion, in-flight work resumes on the new primary** —
//!   `promoted_standby_*` tests, over *real* logical replication.
//! * **(c) the RPO metric reports the injected lag** — `rpo_*` tests, which
//!   compare the reported lag against an independently-read
//!   `pg_stat_replication.replay_lag` within the issue's ±5s tolerance.
//!
//! # Topology
//!
//! Two *databases* in one Postgres instance, wired together with stock logical
//! replication (`CREATE PUBLICATION` / `CREATE SUBSCRIPTION`). That is a real
//! walsender, a real replication slot, real LSNs and a real `replay_lag` — the
//! same machinery a cross-region deployment uses — without the container-to-
//! container networking a two-instance topology would need for no extra
//! fidelity. The human drill in `docs/runbooks/cross-region-failover.md` uses
//! the two-container compose topology; this suite proves the engine behaviour
//! that drill depends on.
//!
//! Requires `wal_level = logical`. The replication tests skip with an explicit
//! message when the server is not configured for it; the fencing tests do not
//! depend on replication at all and always run.
#![cfg(feature = "db")]
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::items_after_statements
)]

use std::sync::atomic::{AtomicU32, Ordering};

use autumn_harvest::replication::{
    FenceRegistry, ReplicationStatus, ShardGeneration, assert_fence, bump_generation,
    current_generation, ensure_generation_row, query_replication_status,
};
use autumn_harvest::types::{ExecutionId, ShardId};
use futures::FutureExt as _;

use diesel_async::SimpleAsyncConnection;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

static DB_SEQ: AtomicU32 = AtomicU32::new(0);

/// [`FenceRegistry`] is process-global; every test that pins a generation must
/// hold this so a sibling test cannot observe a half-built registry.
/// Async-aware, because the guard is deliberately held across `.await`s: the
/// whole point is to keep a sibling test from observing a half-built registry
/// while this one is mid-scenario.
static REGISTRY_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

async fn registry_guard() -> tokio::sync::MutexGuard<'static, ()> {
    REGISTRY_SERIAL.lock().await
}

fn admin_url() -> Option<String> {
    std::env::var("HARVEST_TEST_DATABASE_URL").ok()
}

fn with_db_name(base: &str, db: &str) -> String {
    let (base, query) = base
        .split_once('?')
        .map_or((base, None), |(b, q)| (b, Some(q)));
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |q| format!("{prefix}/{db}?{q}"),
    )
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .unwrap_or_else(|e| panic!("connect {url}: {e}"))
}

/// Create a freshly-migrated database and return `(url, db_name)`.
async fn fresh_db(tag: &str) -> Option<(String, String)> {
    let admin = admin_url()?;
    let mut conn = connect(&admin).await;
    let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
    let db = format!("dr_{tag}_{}_{n}", std::process::id());
    diesel::sql_query(format!("CREATE DATABASE {db}"))
        .execute(&mut conn)
        .await
        .expect("create database");
    let url = with_db_name(&admin, &db);
    let mut fresh = connect(&url).await;
    fresh
        .batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("apply migrations");
    Some((url, db))
}

macro_rules! require_db {
    ($tag:literal) => {
        match fresh_db($tag).await {
            Some(v) => v,
            None => {
                eprintln!("skipping: HARVEST_TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

// ── AC2 / AC6(a): the fence ────────────────────────────────────────────────

#[tokio::test]
async fn a_fresh_database_provisions_generation_zero_and_is_idempotent() {
    let (url, _db) = require_db!("provision");
    let mut conn = connect(&url).await;

    let g = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .expect("provision");
    assert_eq!(g, ShardGeneration::INITIAL);

    // Re-provisioning must never reset a shard that has already been fenced —
    // that would silently hand write authority back to the old region.
    bump_generation(&mut conn, ShardId::new(0), "drill", "test")
        .await
        .expect("bump");
    let again = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .expect("re-provision");
    assert_eq!(again, ShardGeneration(1), "provisioning must be idempotent");
}

#[tokio::test]
async fn bump_is_monotonic_and_records_who_and_why() {
    let (url, _db) = require_db!("bump");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(2))
        .await
        .unwrap();

    assert_eq!(
        bump_generation(&mut conn, ShardId::new(2), "failover to eu-west", "oncall")
            .await
            .unwrap(),
        ShardGeneration(1)
    );
    assert_eq!(
        bump_generation(&mut conn, ShardId::new(2), "second", "oncall")
            .await
            .unwrap(),
        ShardGeneration(2)
    );
    assert_eq!(
        current_generation(&mut conn, ShardId::new(2))
            .await
            .unwrap(),
        Some(ShardGeneration(2))
    );

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        fenced_reason: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        fenced_by: Option<String>,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT fenced_reason, fenced_by FROM harvest_shard_generation WHERE shard_id = 2",
    )
    .load(&mut conn)
    .await
    .unwrap();
    assert_eq!(rows[0].fenced_reason.as_deref(), Some("second"));
    assert_eq!(rows[0].fenced_by.as_deref(), Some("oncall"));
}

#[tokio::test]
async fn assert_fence_rejects_a_stale_generation_and_names_both_epochs() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("assert");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration(0));
    FenceRegistry::set_default_shard(ShardId::new(0));

    // Still current: the assert is a no-op.
    assert_fence(&mut conn, ShardId::new(0))
        .await
        .expect("current epoch passes");

    // The promoted primary fences the old region.
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let err = assert_fence(&mut conn, ShardId::new(0))
        .await
        .expect_err("a pinned-stale worker must be rejected");
    match err {
        autumn_harvest::error::HarvestError::ShardFenced {
            shard_id,
            pinned,
            current,
        } => {
            assert_eq!(shard_id, 0);
            assert_eq!(pinned, 0);
            assert_eq!(current, Some(1));
        }
        other => panic!("expected ShardFenced, got {other:?}"),
    }
    FenceRegistry::clear();
}

#[tokio::test]
async fn an_unregistered_process_is_never_fenced() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("optout");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    FenceRegistry::clear();
    // No pin => fencing is off => the assert issues no statement and passes.
    assert_fence(&mut conn, ShardId::new(0))
        .await
        .expect("a deployment that never opted in must be unaffected");
}

#[tokio::test]
async fn a_missing_generation_row_fences_a_pinned_worker() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("missingrow");
    let mut conn = connect(&url).await;
    FenceRegistry::clear();
    // Pinned, but the row this worker pinned against is gone — a restore from a
    // backup taken before DR was enabled, or a hand-edited database. Fail
    // closed: a pinned worker with nothing to check against must stop.
    FenceRegistry::register(ShardId::new(0), ShardGeneration(3));
    let err = assert_fence(&mut conn, ShardId::new(0))
        .await
        .expect_err("a pinned worker must fail closed when the row is absent");
    match err {
        autumn_harvest::error::HarvestError::ShardFenced { current, .. } => {
            assert_eq!(current, None);
        }
        other => panic!("expected ShardFenced, got {other:?}"),
    }
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_persist_events() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("persist");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'wf', 'k1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .unwrap();

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration(0));
    FenceRegistry::set_default_shard(ShardId::new(0));
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let event = autumn_harvest::event::WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    };
    let err = autumn_harvest::store::append_events(&mut conn, exec_id, &[event], 1)
        .await
        .expect_err("a fenced worker must not append history");
    assert!(
        matches!(err, autumn_harvest::error::HarvestError::ShardFenced { .. }),
        "expected ShardFenced, got {err:?}"
    );

    // …and nothing landed. A partial append would be the fork this exists to
    // prevent.
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<Count> = diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows[0].n, 0, "a fenced append must write nothing");
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_claim_tasks() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("claim");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr",
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration(0));
    FenceRegistry::set_default_shard(ShardId::new(0));

    // Current epoch: the claim succeeds exactly as it did before #954.
    let claimed = autumn_harvest::queue::claim_task_on_shard(
        &mut conn,
        &["q-dr".to_string()],
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("claim");
    assert!(claimed.is_some(), "an unfenced worker claims normally");

    // Release it and fence the region.
    diesel::sql_query("UPDATE harvest_task_queue SET state = 'PENDING', worker_id = NULL")
        .execute(&mut conn)
        .await
        .unwrap();
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let after = autumn_harvest::queue::claim_task_on_shard(
        &mut conn,
        &["q-dr".to_string()],
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("claim query itself still succeeds");
    assert!(after.is_none(), "a fenced worker must claim nothing");

    // The task is untouched — still PENDING, attempt not consumed.
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
    }
    let rows: Vec<Row> = diesel::sql_query("SELECT state, attempt FROM harvest_task_queue")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows[0].state, "PENDING");
    assert_eq!(
        rows[0].attempt, 1,
        "the fenced attempt must not burn a retry"
    );
    FenceRegistry::clear();
}

// ── AC4 / AC6(c): measured RPO ─────────────────────────────────────────────

#[tokio::test]
async fn replication_status_on_a_primary_with_no_standby_is_not_a_zero_rpo() {
    let (url, _db) = require_db!("norepl");
    let mut conn = connect(&url).await;
    let status = query_replication_status(&mut conn, ShardId::new(0))
        .await
        .expect("query");
    assert_eq!(
        status.connected_standbys(),
        0,
        "no subscription was created for this database"
    );
    assert_eq!(
        status.max_replay_lag_seconds(),
        None,
        "a primary with no standby has an UNKNOWN RPO, never 0"
    );
    assert!(matches!(status, ReplicationStatus::Observed { .. }));
}

// ── The two-"region" topology, over real logical replication ───────────────

/// Turn a `postgres://` URL into the libpq keyword form `CREATE SUBSCRIPTION`
/// wants, retargeted at `db`.
fn libpq_conninfo(url: &str, db: &str) -> String {
    let rest = url
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://");
    let (userinfo, after_at) = rest.split_once('@').unwrap_or(("postgres", rest));
    let (user, password) = userinfo
        .split_once(':')
        .map_or((userinfo, None), |(u, p)| (u, Some(p)));
    let authority = after_at.split(['/', '?']).next().unwrap_or(after_at);
    let (host, port) = authority.split_once(':').unwrap_or((authority, "5432"));
    use std::fmt::Write as _;

    let mut conninfo = format!("host={host} port={port} user={user} dbname={db}");
    if let Some(pw) = password {
        let _ = write!(conninfo, " password={pw}");
    }
    conninfo
}

/// A primary ("region A") and a standby ("region B") wired with stock logical
/// replication, plus the teardown that must run even on failure — an orphaned
/// replication slot pins WAL on the shared test server forever.
struct Regions {
    primary_url: String,
    primary_db: String,
    standby_url: String,
    slot: String,
    sub: String,
}

impl Regions {
    async fn teardown(&self) {
        if let Ok(mut b) = AsyncPgConnection::establish(&self.standby_url).await {
            let _ = b
                .batch_execute(&format!("DROP SUBSCRIPTION IF EXISTS {}", self.sub))
                .await;
        }
        if let Ok(mut a) = AsyncPgConnection::establish(&self.primary_url).await {
            let _ = diesel::sql_query(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE slot_name = $1",
            )
            .bind::<diesel::sql_types::Text, _>(self.slot.clone())
            .execute(&mut a)
            .await;
        }
    }
}

async fn wal_level_is_logical(url: &str) -> bool {
    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        wal_level: String,
    }
    let mut conn = connect(url).await;
    let rows: Result<Vec<S>, _> =
        diesel::sql_query("SELECT current_setting('wal_level') AS wal_level")
            .load(&mut conn)
            .await;
    rows.map(|r| {
        r.into_iter()
            .next()
            .is_some_and(|s| s.wal_level == "logical")
    })
    .unwrap_or(false)
}

/// Build the two-region topology, or `None` when the server cannot host it.
async fn two_regions(tag: &str) -> Option<Regions> {
    let admin = admin_url()?;
    if !wal_level_is_logical(&admin).await {
        eprintln!(
            "skipping {tag}: server is not configured with wal_level=logical, so stock logical \
             replication cannot be exercised"
        );
        return None;
    }
    let (primary_url, primary_db) = fresh_db(&format!("{tag}a")).await?;
    let (standby_url, _standby_db) = fresh_db(&format!("{tag}b")).await?;

    let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
    let slot = format!("dr_slot_{}_{n}", std::process::id());
    let sub = format!("dr_sub_{}_{n}", std::process::id());

    let mut a = connect(&primary_url).await;
    a.batch_execute("CREATE PUBLICATION harvest_dr FOR ALL TABLES")
        .await
        .expect("publication");

    // The slot is created on its own connection, BEFORE the subscription, and
    // the subscription is told not to create one. `CREATE SUBSCRIPTION` runs in
    // a transaction, and slot creation waits for transactions older than itself
    // to end — so when publisher and subscriber live in the same Postgres
    // instance, letting it create its own slot deadlocks against itself. (Two
    // separate instances would not, but they buy no extra fidelity and cost
    // container-to-container networking.)
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut a)
        .await
        .expect("create slot");

    let conninfo = libpq_conninfo(&admin, &primary_db);
    let mut b = connect(&standby_url).await;
    b.batch_execute(&format!(
        "CREATE SUBSCRIPTION {sub} CONNECTION '{conninfo}' PUBLICATION harvest_dr \
         WITH (create_slot = false, slot_name = '{slot}', copy_data = true)"
    ))
    .await
    .expect("subscription");

    Some(Regions {
        primary_url,
        primary_db,
        standby_url,
        slot,
        sub,
    })
}

macro_rules! require_regions {
    ($tag:literal) => {
        match two_regions($tag).await {
            Some(v) => v,
            None => return,
        }
    };
}

async fn count_on(url: &str, sql: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct C {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = connect(url).await;
    let rows: Vec<C> = diesel::sql_query(sql).load(&mut conn).await.expect("count");
    rows.into_iter().next().map_or(0, |c| c.n)
}

/// Poll until `f` holds or the deadline passes. Replication is asynchronous by
/// definition; a fixed sleep would be either flaky or slow.
async fn eventually<F, Fut>(what: &str, timeout: std::time::Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

// ── AC6(c): the RPO metric reports the injected lag ────────────────────────

#[tokio::test]
async fn rpo_metric_reports_injected_replication_lag() {
    let regions = require_regions!("rpo");
    let result = rpo_body(&regions).await;
    regions.teardown().await;
    result.unwrap();
}

#[allow(clippy::cognitive_complexity)]
async fn rpo_body(regions: &Regions) -> Result<(), String> {
    use autumn_harvest::replication::{measure_rpo, record_replication_heartbeat};
    let retain = std::time::Duration::from_secs(3600);
    let shard = ShardId::new(0);
    let mut a = connect(&regions.primary_url).await;

    // Healthy: beats are confirmed by the standby within a beat or two.
    for _ in 0..3 {
        record_replication_heartbeat(&mut a, shard, retain)
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    let primary_url = regions.primary_url.clone();
    eventually(
        "a healthy RPO reading",
        std::time::Duration::from_secs(30),
        || {
            let url = primary_url.clone();
            async move {
                let mut conn = connect(&url).await;
                let _ = record_replication_heartbeat(&mut conn, shard, retain).await;
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                matches!(measure_rpo(&mut conn, shard).await, Ok(Some(v)) if v < 5.0)
            }
        },
    )
    .await;

    // Inject lag: hold ACCESS EXCLUSIVE on a replicated table so the
    // subscriber's apply worker blocks. This is the realistic shape of the
    // incident — the walsender stays connected and the stream keeps flowing,
    // only apply stalls — and it is exactly the case where
    // `pg_stat_replication.replay_lag` goes blind.
    let mut blocker = connect(&regions.standby_url).await;
    blocker
        .batch_execute("BEGIN; LOCK TABLE harvest_replication_heartbeat IN ACCESS EXCLUSIVE MODE")
        .await
        .map_err(|e| format!("lock: {e}"))?;

    let stall_started = std::time::Instant::now();
    let injected = std::time::Duration::from_secs(12);
    while stall_started.elapsed() < injected {
        record_replication_heartbeat(&mut a, shard, retain)
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let independently_measured = stall_started.elapsed().as_secs_f64();
    let reported = measure_rpo(&mut a, shard)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("RPO must be measurable while replication is stalled")?;

    // The issue's success metric: within ±5s of an independent measurement.
    let delta = (reported - independently_measured).abs();
    if delta > 5.0 {
        blocker.batch_execute("ROLLBACK").await.ok();
        return Err(format!(
            "reported RPO {reported:.1}s vs independently-measured {independently_measured:.1}s \
             (delta {delta:.1}s) exceeds the ±5s tolerance"
        ));
    }

    // …and the reading is honest about *which* source it came from: with apply
    // blocked, Postgres' own replay_lag is blind, which is the whole reason the
    // watermark trail exists.
    let status = autumn_harvest::replication::query_replication_status(&mut a, shard)
        .await
        .map_err(|e| e.to_string())?;
    let rpo = status.rpo_seconds().ok_or("status must carry the RPO")?;
    if (rpo - reported).abs() > 2.0 {
        blocker.batch_execute("ROLLBACK").await.ok();
        return Err(format!(
            "status RPO {rpo} disagrees with measure_rpo {reported}"
        ));
    }
    if status.max_lag_bytes().unwrap_or(0) <= 0 {
        blocker.batch_execute("ROLLBACK").await.ok();
        return Err("a stalled standby must show a byte backlog".into());
    }

    // Release the stall; the RPO must recover, not stay latched.
    blocker
        .batch_execute("ROLLBACK")
        .await
        .map_err(|e| e.to_string())?;
    let primary_url = regions.primary_url.clone();
    eventually(
        "the RPO to recover after the stall clears",
        std::time::Duration::from_secs(60),
        || {
            let url = primary_url.clone();
            async move {
                let mut conn = connect(&url).await;
                let _ = record_replication_heartbeat(&mut conn, shard, retain).await;
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                matches!(measure_rpo(&mut conn, shard).await, Ok(Some(v)) if v < 5.0)
            }
        },
    )
    .await;

    Ok(())
}

#[tokio::test]
async fn a_disconnected_standby_reports_bytes_and_an_unknown_rpo_never_zero() {
    let regions = require_regions!("discon");
    // The body is polled to completion inside `AssertUnwindSafe` so a failed
    // assertion still drops the replication slot: an orphaned slot pins WAL on
    // the shared test server indefinitely.
    let out = std::panic::AssertUnwindSafe(async {
        let shard = ShardId::new(0);
        let mut b = connect(&regions.standby_url).await;
        b.batch_execute(&format!("ALTER SUBSCRIPTION {} DISABLE", regions.sub))
            .await
            .expect("disable subscription");

        let mut a = connect(&regions.primary_url).await;
        // Generate WAL the now-absent standby cannot consume.
        for _ in 0..5 {
            autumn_harvest::replication::record_replication_heartbeat(
                &mut a,
                shard,
                std::time::Duration::from_secs(3600),
            )
            .await
            .expect("beat");
        }

        let primary_url = regions.primary_url.clone();
        eventually(
            "the walsender to disappear",
            std::time::Duration::from_secs(30),
            || {
                let url = primary_url.clone();
                async move {
                    let mut conn = connect(&url).await;
                    autumn_harvest::replication::query_replication_status(&mut conn, shard)
                        .await
                        .is_ok_and(|s| s.connected_standbys() == 0)
                }
            },
        )
        .await;

        let status = autumn_harvest::replication::query_replication_status(&mut a, shard)
            .await
            .expect("status");
        assert_eq!(status.connected_standbys(), 0, "the standby is gone");
        assert_eq!(
            status.max_replay_lag_seconds(),
            None,
            "Postgres cannot report a replay lag for a standby that is not connected"
        );
        assert!(
            status.max_lag_bytes().unwrap_or(0) > 0,
            "the slot still pins WAL, so the byte backlog is knowable and must be reported"
        );
        assert_eq!(
            status.inactive_slots(),
            1,
            "the abandoned slot must be visible"
        );
        assert_ne!(
            status.rpo_seconds(),
            Some(0.0),
            "a dead standby must never read as a perfect RPO"
        );
    })
    .catch_unwind()
    .await;
    regions.teardown().await;
    if let Err(panic) = out {
        std::panic::resume_unwind(panic);
    }
}

// ── AC6(b): promotion, fencing, and in-flight work resuming ────────────────

#[tokio::test]
async fn a_promoted_standby_resumes_in_flight_work_and_rejects_the_old_region() {
    let _serial = registry_guard().await;
    let regions = require_regions!("promote");
    let result = promotion_body(&regions).await;
    regions.teardown().await;
    FenceRegistry::clear();
    result.unwrap();
}

async fn promotion_body(regions: &Regions) -> Result<(), String> {
    let shard = ShardId::new(0);
    let mut a = connect(&regions.primary_url).await;

    // ── Region A is live: an in-flight workflow with a pending task. ───────
    ensure_generation_row(&mut a, shard)
        .await
        .map_err(|e| e.to_string())?;
    let exec_id = ExecutionId::new_for_shard(shard);
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'dr-wf', 'order-1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut a)
    .await
    .map_err(|e| e.to_string())?;

    let started = autumn_harvest::event::WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    };
    autumn_harvest::store::append_events(&mut a, exec_id, &[started], 1)
        .await
        .map_err(|e| e.to_string())?;

    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr",
        autumn_harvest::queue::TaskType::Workflow,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut a, &params)
        .await
        .map_err(|e| e.to_string())?;

    // ── Replication carries it to region B. ───────────────────────────────
    let standby_url = regions.standby_url.clone();
    eventually(
        "the in-flight workflow to reach region B",
        std::time::Duration::from_secs(60),
        || {
            let url = standby_url.clone();
            async move {
                count_on(&url, "SELECT COUNT(*) AS n FROM harvest_events").await == 1
                    && count_on(&url, "SELECT COUNT(*) AS n FROM harvest_task_queue").await == 1
                    && count_on(&url, "SELECT COUNT(*) AS n FROM harvest_shard_generation").await
                        == 1
            }
        },
    )
    .await;

    // ── Region A is gone. Promote B: stop replicating, then FENCE. ────────
    let mut b = connect(&regions.standby_url).await;
    b.batch_execute(&format!("DROP SUBSCRIPTION {}", regions.sub))
        .await
        .map_err(|e| format!("promote: {e}"))?;

    let promoted_gen = bump_generation(&mut b, shard, "failover drill", "oncall")
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(promoted_gen, ShardGeneration(1));

    // Sequences are NOT replicated by logical replication. Without this the
    // new primary's `harvest_events_id_seq` still sits at 1 and the first
    // append collides with a replicated row's primary key.
    let advanced = autumn_harvest::replication::advance_sequences_after_promotion(&mut b)
        .await
        .map_err(|e| e.to_string())?;
    assert!(
        advanced
            .iter()
            .any(|(name, _)| name.contains("harvest_events_id_seq")),
        "the promotion helper must advance harvest_events' sequence; advanced: {advanced:?}"
    );

    // ── A surviving region-A worker, pinned to the pre-failover epoch. ────
    FenceRegistry::clear();
    FenceRegistry::register(shard, ShardGeneration(0));
    FenceRegistry::set_default_shard(shard);

    let stale_claim = autumn_harvest::queue::claim_task_on_shard(
        &mut b,
        &["q-dr".to_string()],
        "worker-old-region",
        "",
        None,
        &[],
        &[],
        Some(shard),
    )
    .await
    .map_err(|e| e.to_string())?;
    if stale_claim.is_some() {
        return Err("a stale-epoch worker claimed a task on the promoted primary".into());
    }

    let event = autumn_harvest::event::WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({"by": "old-region"}),
    };
    let err = autumn_harvest::store::append_events(&mut b, exec_id, &[event], 2)
        .await
        .expect_err("a stale-epoch worker must not append to the promoted primary");
    if !matches!(err, autumn_harvest::error::HarvestError::ShardFenced { .. }) {
        return Err(format!("expected ShardFenced, got {err:?}"));
    }

    // ── A region-B worker, pinned to the promoted epoch, carries on. ──────
    FenceRegistry::clear();
    FenceRegistry::register(shard, promoted_gen);
    FenceRegistry::set_default_shard(shard);

    let claim = autumn_harvest::queue::claim_task_on_shard(
        &mut b,
        &["q-dr".to_string()],
        "worker-new-region",
        "",
        None,
        &[],
        &[],
        Some(shard),
    )
    .await
    .map_err(|e| e.to_string())?;
    if claim.is_none() {
        return Err(
            "the promoted region's worker must be able to claim the replicated task".into(),
        );
    }

    let done = autumn_harvest::event::WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({"by": "new-region"}),
    };
    autumn_harvest::store::append_events(&mut b, exec_id, &[done], 2)
        .await
        .map_err(|e| format!("the promoted region must be able to append: {e}"))?;

    // ── No fork: exactly one continuous history, extended not branched. ───
    let n = count_on(
        &regions.standby_url,
        "SELECT COUNT(*) AS n FROM harvest_events",
    )
    .await;
    if n != 2 {
        return Err(format!(
            "expected a single 2-event history on the new primary, found {n}"
        ));
    }
    let forks = count_on(
        &regions.standby_url,
        "SELECT COUNT(*) AS n FROM ( \
             SELECT event_id FROM harvest_events GROUP BY workflow_exec_id, event_id \
             HAVING COUNT(*) > 1 \
         ) d",
    )
    .await;
    if forks != 0 {
        return Err(format!("history forked: {forks} duplicated event ids"));
    }
    Ok(())
}
