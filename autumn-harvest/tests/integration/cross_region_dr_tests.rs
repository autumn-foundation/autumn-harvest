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
    AdminWrite, DrFencing, DrMarkers, FenceRegistry, ReplicationStatus, ShardGeneration,
    WatermarkReading, assert_admin_write_authority, assert_fence, bump_generation,
    current_generation, ensure_generation_row, pin_process_fence, pin_worker_fence,
    probe_dr_markers, query_replication_status, resolve_held,
};
use autumn_harvest::types::{ExecutionId, ShardId};
use futures::FutureExt as _;

use diesel_async::SimpleAsyncConnection;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

static DB_SEQ: AtomicU32 = AtomicU32::new(0);

/// Sampler cadence these tests beat at.
///
/// Near-zero on purpose: the beat is now rate-limited to one write per shard
/// per half-interval, and a test that wants several distinct watermarks in
/// quick succession must not be throttled by that gate.
const BEAT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);

/// The slot-name prefix the tests create their DR slots with — the same
/// `harvest_dr` the topology doc's setup SQL prescribes and the code defaults
/// to. Anything not carrying it is deliberately NOT counted as a DR standby.
const DR_PREFIX: &str = autumn_harvest::replication::DEFAULT_DR_SLOT_PREFIX;

/// [`FenceRegistry`] is process-global; every test that pins a generation must
/// hold this so a sibling test cannot observe a half-built registry.
/// Async-aware, because the guard is deliberately held across `.await`s: the
/// whole point is to keep a sibling test from observing a half-built registry
/// while this one is mid-scenario.
static REGISTRY_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// A codec that always succeeds and changes the bytes -- enough for the sweep
/// to have real work to do, without pulling a real cipher into a DR test.
struct DrXorCodec(u8);

impl autumn_harvest::payload_codec::PayloadCodec for DrXorCodec {
    fn codec_id(&self) -> &'static str {
        "dr-xor"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        Ok(raw.iter().map(|b| b ^ self.0).collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        Ok(encoded.iter().map(|b| b ^ self.0).collect())
    }
}

struct NoOpMetrics;
impl autumn_harvest::telemetry::MetricsRecorder for NoOpMetrics {}

/// Holds [`REGISTRY_SERIAL`]. On drop it clears the registry and the DR
/// config, so a test that panics leaves no pin behind (issue #1823).
pub struct RegistryGuard {
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        FenceRegistry::clear();
        autumn_harvest::replication::set_dr_config(autumn_harvest::replication::DrConfig::default());
    }
}

/// Serializes every test in this crate that pins the process-global
/// `FenceRegistry`. `shard_rebalance_db_tests` takes it too (issue #1839).
pub async fn registry_guard() -> RegistryGuard {
    RegistryGuard {
        _serial: REGISTRY_SERIAL.lock().await,
    }
}

/// A slot prefix no other test uses, for a "plain database" assertion.
///
/// A physical slot covers the whole cluster. A slot that another test leaks
/// with the default prefix would mark every database as DR. A unique prefix
/// keeps the plain-database tests independent of that.
fn unique_prefix(db: &str) -> String {
    format!("{DR_PREFIX}_{db}")
}

/// The shared Postgres these tests create their per-test databases on.
///
/// Prefers a caller-supplied `HARVEST_TEST_DATABASE_URL`; otherwise starts one
/// throwaway container for the whole suite (CI's path). The container is
/// started with **`wal_level = logical`**, which is not the image default and
/// without which the three replication tests below would silently skip — i.e.
/// AC6(b) and AC6(c) would be permanently unproven while CI stayed green. That
/// is the single most important line in this file.
///
/// One container for the suite, not one per test: `pg_replication_slots` and
/// `pg_stat_replication` are cluster-wide, and the module under test scopes
/// every query to `current_database()` precisely so several shard databases can
/// share a cluster. Sharing one here therefore exercises that scoping rather
/// than dodging it — but it is also why the suite must run `--test-threads=1`
/// (the manifest's `linux` osclass supplies that).
static SHARED_PG: tokio::sync::OnceCell<Option<SharedPg>> = tokio::sync::OnceCell::const_new();

struct SharedPg {
    admin_url: String,
    /// Kept alive for the process; dropping it stops the container.
    _container: Option<ContainerAsync<Postgres>>,
}

async fn shared_pg() -> Option<&'static SharedPg> {
    SHARED_PG
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
                return Some(SharedPg {
                    admin_url: url,
                    _container: None,
                });
            }
            let container = Postgres::default()
                .with_tag("16")
                // `wal_level=logical` is required for `CREATE PUBLICATION` to
                // produce anything. The image default is `replica`.
                .with_cmd(["postgres", "-c", "wal_level=logical"])
                .start()
                .await
                .ok()?;
            let host = container.get_host().await.ok()?;
            let port = container.get_host_port_ipv4(5432).await.ok()?;
            Some(SharedPg {
                admin_url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
                _container: Some(container),
            })
        })
        .await
        .as_ref()
}

async fn admin_url() -> Option<String> {
    shared_pg().await.map(|pg| pg.admin_url.clone())
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
    let admin = admin_url().await?;
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
        .batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations");
    Some((url, db))
}

macro_rules! require_db {
    ($tag:literal) => {
        match fresh_db($tag).await {
            Some(v) => v,
            None => {
                // Reached only when neither a caller-supplied
                // `HARVEST_TEST_DATABASE_URL` nor Docker is available. Loud on
                // purpose: a silently-skipping suite that claims to prove an
                // acceptance criterion is worse than no suite.
                eprintln!(
                    "SKIPPED {}: no HARVEST_TEST_DATABASE_URL and no usable Docker — this suite \
                     proved NOTHING",
                    $tag
                );
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
    assert_eq!(
        again,
        ShardGeneration::new(1),
        "provisioning must be idempotent"
    );
}

/// Concurrent first starts must never spuriously fail to provision (finding 8).
///
/// The old query read its `ON CONFLICT DO NOTHING` fallback in the SAME
/// statement as the `INSERT`, sharing that statement's snapshot. Under READ
/// COMMITTED, a losing `INSERT` blocks on the winner's commit and then
/// finds nothing to insert. Its fallback read, sharing the pre-commit
/// snapshot, could still see zero rows, though. So the whole statement
/// returned empty, and the losing worker refused to start. This is
/// inherently a timing-dependent race, so this test cannot force the old
/// bug to reproduce on every run. But with several connections racing the
/// same shard on every run of this suite, it is a live regression guard
/// rather than a coincidence.
#[tokio::test]
async fn concurrent_first_starts_always_agree_on_the_provisioned_generation() {
    let (url, _db) = require_db!("provisionrace");
    let shard = ShardId::new(0);

    let mut conns = Vec::new();
    for _ in 0..8 {
        conns.push(connect(&url).await);
    }

    let results = futures::future::join_all(
        conns
            .iter_mut()
            .map(|conn| ensure_generation_row(conn, shard)),
    )
    .await;

    for result in &results {
        assert_eq!(
            result.as_ref().ok().copied(),
            Some(ShardGeneration::INITIAL),
            "every racing first start must observe the provisioned row, never a spurious \
             failure: {results:?}"
        );
    }
}

/// A heartbeat written under a superseded generation must never be read back
/// as if it were part of the current WAL stream (finding 10).
///
/// `docs/cross-region-dr.md`'s setup SQL replicates
/// `harvest_replication_heartbeat` with `FOR ALL TABLES`. So a standby can
/// carry beats the OLD primary wrote — LSNs from a WAL stream the new
/// primary does not share. This plants exactly that: a stale, hour-old
/// beat tagged with generation 0 at a LARGER LSN. Beside it sits a fresh,
/// current beat tagged with generation 1 at a SMALLER one. Only the
/// generation filter — never `ORDER BY beat_lsn DESC` — can be why the
/// fresh one wins.
#[tokio::test]
async fn measure_rpo_ignores_heartbeats_from_a_superseded_generation() {
    let (url, _db) = require_db!("genscope");
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    ensure_generation_row(&mut conn, shard)
        .await
        .expect("provision generation 0");

    let slot = format!("{DR_PREFIX}_genscope_{}", std::process::id());
    diesel::sql_query("SELECT pg_create_physical_replication_slot($1, true)")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("create a reserved physical slot");

    // Stale: generation 0, an hour old, at a LARGE LSN.
    diesel::sql_query(
        "INSERT INTO harvest_replication_heartbeat \
             (shard_id, beat_lsn, beat_at, fence_generation) \
         VALUES ($1, pg_current_wal_lsn(), NOW() - INTERVAL '1 hour', 0)",
    )
    .bind::<diesel::sql_types::Integer, _>(shard.as_i32())
    .execute(&mut conn)
    .await
    .expect("seed a stale, wrong-generation beat with a LARGE LSN");

    bump_generation(&mut conn, shard, "drill", "test")
        .await
        .expect("bump to generation 1");

    // Fresh: generation 1, just now, at a SMALL LSN -- smaller than the stale
    // row above, so `ORDER BY beat_lsn DESC` alone would pick the stale one.
    diesel::sql_query(
        "INSERT INTO harvest_replication_heartbeat \
             (shard_id, beat_lsn, beat_at, fence_generation) \
         VALUES ($1, '0/1'::pg_lsn, NOW(), 1)",
    )
    .bind::<diesel::sql_types::Integer, _>(shard.as_i32())
    .execute(&mut conn)
    .await
    .expect("seed a fresh, current-generation beat with a SMALL LSN");

    // Advance the slot's position past both beats, so the position query's
    // `beat_lsn <= position` predicate admits both rows and only the
    // generation filter can decide between them.
    diesel::sql_query("SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("advance the slot's position past both beats");

    let reading = autumn_harvest::replication::measure_rpo(&mut conn, shard, DR_PREFIX)
        .await
        .expect("measure_rpo");
    match reading {
        WatermarkReading::Measured(seconds) => {
            assert!(
                seconds < 30.0,
                "must read the fresh current-generation beat, not the hour-old \
                 superseded-generation one that sorts ahead of it by LSN: got {seconds}s"
            );
        }
        other => panic!("expected a fresh Measured reading, got {other:?}"),
    }

    let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
        .bind::<diesel::sql_types::Text, _>(slot)
        .execute(&mut conn)
        .await;
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
        ShardGeneration::new(1)
    );
    assert_eq!(
        bump_generation(&mut conn, ShardId::new(2), "second", "oncall")
            .await
            .unwrap(),
        ShardGeneration::new(2)
    );
    assert_eq!(
        current_generation(&mut conn, ShardId::new(2))
            .await
            .unwrap(),
        Some(ShardGeneration::new(2))
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
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");

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
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(3))
        .expect("no conflicting pin in this test");
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
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
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
async fn a_fenced_worker_cannot_re_encrypt_history() {
    // The codec re-encryption sweep (issue #948) is sanctioned exception #3 to
    // the append-only invariant: it is the only path that UPDATEs
    // `harvest_events` in place. `store.rs` fences every INSERT, but this
    // UPDATE is a different statement in a different module, so it needs its
    // own assertion.
    //
    // The failure this prevents is the worst one the sweep has. A worker still
    // pinned to the old generation reconnects to the promoted primary; its
    // appends are refused, but an unfenced sweep would happily re-encode rows
    // the new region now owns -- under *its* active key, which the promoted
    // region may already have retired. That is silent, permanent, and destroys
    // payloads rather than merely forking history.
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("rotate");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'wf', 'rotate-1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .unwrap();

    // A history row encoded under `k1`, with `k2` now active: exactly what the
    // sweep exists to convert.
    let codecs = autumn_harvest::payload_codec::PayloadCodecs::default();
    codecs
        .register_key("k1", std::sync::Arc::new(DrXorCodec(0x5a)))
        .unwrap();
    codecs
        .register_key("k2", std::sync::Arc::new(DrXorCodec(0x33)))
        .unwrap();
    codecs.set_active_key("k1").unwrap();
    let encoded = codecs
        .encode_payload(&serde_json::json!({"secret": "value"}))
        .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         VALUES ($1, 0, 'WorkflowStarted', $2)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!({
        "type": "WorkflowStarted",
        "data": {"input": encoded, "timestamp": "2026-08-31T00:00:00Z"}
    }))
    .execute(&mut conn)
    .await
    .unwrap();
    codecs.set_active_key("k2").unwrap();

    // This worker is pinned to generation 0; the region has been promoted past
    // it.
    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let swept = autumn_harvest::codec_rotation::sweep_codec_reencryption_once(
        &mut conn,
        0,
        &codecs,
        100,
        &NoOpMetrics,
    )
    .await;

    // Whether the sweep surfaces the fence as an error or simply converts
    // nothing, the row must be untouched -- that is the guarantee.
    #[derive(diesel::QueryableByName)]
    struct Kid {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        kid: Option<String>,
    }
    let rows: Vec<Kid> =
        diesel::sql_query("SELECT event_data->'data'->'input'->>'kid' AS kid FROM harvest_events")
            .load(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        rows[0].kid.as_deref(),
        Some("k1"),
        "a fenced worker must not re-encrypt history the promoted region owns; \
         sweep returned {swept:?}"
    );
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_advance_the_rotation_cursor() {
    // Sibling to `a_fenced_worker_cannot_re_encrypt_history` (issue #1257).
    // A worker pinned to a superseded generation must not record any
    // rotation progress at all, not just leave `harvest_events` untouched.
    //
    // With a convertible row present, the existing per-row fence on
    // `compare_and_swap_event` (issue #954) already errors out of the
    // sweep before `write_cursor` is reached. So this scenario alone does
    // not discriminate the fix from before it. See
    // `a_fenced_sweep_that_converts_nothing_still_fails_closed` for the
    // batch that reaches `write_cursor` with nothing to convert. This test
    // stays as a direct check that no cursor row leaks in the common case
    // too.
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("rotate_cursor");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'wf', 'rotate-cursor-1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .unwrap();

    // A history row encoded under `k1`, with `k2` now active: exactly what
    // the sweep exists to convert.
    let codecs = autumn_harvest::payload_codec::PayloadCodecs::default();
    codecs
        .register_key("k1", std::sync::Arc::new(DrXorCodec(0x5a)))
        .unwrap();
    codecs
        .register_key("k2", std::sync::Arc::new(DrXorCodec(0x33)))
        .unwrap();
    codecs.set_active_key("k1").unwrap();
    let encoded = codecs
        .encode_payload(&serde_json::json!({"secret": "value"}))
        .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         VALUES ($1, 0, 'WorkflowStarted', $2)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!({
        "type": "WorkflowStarted",
        "data": {"input": encoded, "timestamp": "2026-08-31T00:00:00Z"}
    }))
    .execute(&mut conn)
    .await
    .unwrap();
    codecs.set_active_key("k2").unwrap();

    // This worker is pinned to generation 0; the region has been promoted
    // past it.
    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let _ = autumn_harvest::codec_rotation::sweep_codec_reencryption_once(
        &mut conn,
        0,
        &codecs,
        100,
        &NoOpMetrics,
    )
    .await;

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<Count> =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_codec_rotation_cursor")
            .load(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        rows[0].n, 0,
        "a fenced worker must not record rotation progress it was fenced away from"
    );
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_sweep_that_converts_nothing_still_fails_closed() {
    // Sibling to `a_fenced_worker_cannot_re_encrypt_history` (issue #1257,
    // acceptance criterion: a sweep batch that converts no rows must still
    // fail closed under a stale fence). A batch with nothing to convert
    // never calls the per-row fenced CAS at all. `write_cursor` and
    // `claim_completed_cursor_revalidation` are the only writes left on
    // this path, so they are the only guard against a stale worker
    // recording progress.
    //
    // The cursor here is already complete and due for revalidation. An
    // unfenced worker would also win `claim_completed_cursor_revalidation`
    // and bump `updated_at` even though it converted nothing.
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("rotate_cursor_empty");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    diesel::sql_query(
        "INSERT INTO harvest_codec_rotation_cursor \
             (shard_id, active_key_id, last_event_id, rows_reencrypted, \
              unresolved_rows, completed_at, updated_at) \
         VALUES (0, 'k2', 0, 0, 0, NOW() - interval '1 hour', \
                 NOW() - interval '1 hour')",
    )
    .execute(&mut conn)
    .await
    .unwrap();

    let codecs = autumn_harvest::payload_codec::PayloadCodecs::default();
    codecs
        .register_key("k2", std::sync::Arc::new(DrXorCodec(0x33)))
        .unwrap();
    codecs.set_active_key("k2").unwrap();

    // This worker is pinned to generation 0; the region has been promoted
    // past it.
    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let _ = autumn_harvest::codec_rotation::sweep_codec_reencryption_once(
        &mut conn,
        0,
        &codecs,
        100,
        &NoOpMetrics,
    )
    .await;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        last_event_id: i64,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: chrono::DateTime<chrono::Utc>,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT last_event_id, updated_at FROM harvest_codec_rotation_cursor \
         WHERE shard_id = 0",
    )
    .load(&mut conn)
    .await
    .unwrap();
    // `rows.len()` and `last_event_id` hold both before and after this
    // fix. The seeded row already starts at `last_event_id = 0`, so they
    // document the row is untouched rather than proving the fence fired.
    // `updated_at` is the assertion that actually distinguishes a fenced
    // `claim_completed_cursor_revalidation` from an unfenced one. An
    // unfenced worker would have won the claim and bumped it to `NOW()`.
    assert_eq!(
        rows.len(),
        1,
        "the pre-existing cursor row must survive untouched"
    );
    assert_eq!(
        rows[0].last_event_id, 0,
        "a fenced worker must not advance a cursor it examined nothing under"
    );
    assert!(
        rows[0].updated_at < chrono::Utc::now() - chrono::Duration::minutes(30),
        "a fenced worker must not win the revalidation claim and bump updated_at"
    );
    FenceRegistry::clear();
}

#[tokio::test]
async fn a_fenced_worker_cannot_backfill_quota_keys() {
    // The quota_key backfill reconciler (issue #1226, follow-up to #946)
    // is a second module that UPDATEs `harvest_workflow_executions`
    // outside the ordinary claim/dispatch path. It needs its own fence
    // assertion for the same reason the codec-rotation sweep above does.
    //
    // A fenced worker's registry view can be stale relative to the
    // promoted region. An unfenced backfill could therefore write a
    // `quota_key` the new region's own reconciler would have resolved
    // differently. It could even write one at all, for a row the new
    // region has already moved past.
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("quota_reconcile");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();

    let workflow_name = format!("wf_dr_fenced_{}", DB_SEQ.fetch_add(1, Ordering::SeqCst));
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, $2, 'rotate-1', 'RUNNING', '{\"tenant_id\": \"acme\"}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(&workflow_name)
    .execute(&mut conn)
    .await
    .unwrap();

    let policy =
        autumn_harvest::quota::QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let previous_metadata = {
        let mut lock = autumn_harvest::completion_trigger::GLOBAL_WORKFLOW_METADATA
            .write()
            .expect("metadata lock");
        let mut map = std::collections::HashMap::new();
        map.insert(
            workflow_name.clone(),
            autumn_harvest::completion_trigger::WorkflowMetadata {
                concurrency: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                input_schema: None,
                sla: None,
                retry_policy: None,
                quota: Some(policy),
            },
        );
        lock.replace(map)
    };

    // This worker is pinned to generation 0; the region has been promoted
    // past it.
    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let result = autumn_harvest::quota_reconcile::reconcile_quota_keys(
        &mut conn,
        100,
        Some(ShardId::new(0)),
    )
    .await;

    #[derive(diesel::QueryableByName)]
    struct QuotaKeyRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
    }
    let rows: Vec<QuotaKeyRow> =
        diesel::sql_query("SELECT quota_key FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .load(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        rows[0].quota_key, None,
        "a fenced worker must not backfill a quota_key onto a row the promoted \
         region now owns; sweep returned {result:?}"
    );

    {
        let mut lock = autumn_harvest::completion_trigger::GLOBAL_WORKFLOW_METADATA
            .write()
            .expect("metadata lock");
        *lock = previous_metadata;
    }
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
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");

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

/// The batched claim applies the same fence as the single-row claim (issue
/// #1823). Issue #1340 intends to make it the default claim path.
#[tokio::test]
async fn a_fenced_worker_cannot_claim_through_claim_task_batched() {
    use autumn_harvest::queue::{BatchedClaimConfig, claim_task_batched};

    let _serial = registry_guard().await;
    let (url, _db) = require_db!("claimbatched");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr-batched",
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");

    FenceRegistry::clear();
    FenceRegistry::publish(
        &[(ShardId::new(0), ShardGeneration::new(0))],
        ShardId::new(0),
    )
    .expect("no conflicting pin in this test");

    let queues = ["q-dr-batched".to_string()];
    let claimed = claim_task_batched(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        BatchedClaimConfig::default(),
    )
    .await
    .expect("claim");
    assert!(claimed.is_some(), "the current epoch claims normally");

    diesel::sql_query("UPDATE harvest_task_queue SET state = 'PENDING', worker_id = NULL")
        .execute(&mut conn)
        .await
        .unwrap();
    bump_generation(&mut conn, ShardId::new(0), "promote", "oncall")
        .await
        .unwrap();

    let after = claim_task_batched(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        BatchedClaimConfig::default(),
    )
    .await
    .expect("the claim query itself still succeeds");
    let after_on_shard = autumn_harvest::queue::claim_task_batched_on_shard(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        BatchedClaimConfig::default(),
        Some(ShardId::new(0)),
    )
    .await
    .expect("the claim query itself still succeeds");
    FenceRegistry::clear();
    assert!(
        after.is_none(),
        "a worker pinned to a superseded generation must claim nothing"
    );
    assert!(
        after_on_shard.is_none(),
        "the explicit-shard entry point is fenced too"
    );

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
    assert_eq!(rows[0].attempt, 1, "a fenced claim must not burn a retry");

    // Positive control: the row is claimable at the current epoch, so the
    // `None` above comes from the fence, not from leftover row state.
    FenceRegistry::publish(
        &[(ShardId::new(0), ShardGeneration::new(1))],
        ShardId::new(0),
    )
    .expect("pin the current epoch");
    let current = claim_task_batched(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        BatchedClaimConfig::default(),
    )
    .await
    .expect("claim");
    FenceRegistry::clear();
    assert!(current.is_some(), "the current epoch claims the row");
}

/// The fence bump is a **commit-order barrier**, not a racy read.
///
/// This is the property the whole mechanism rests on: a persist that passes the
/// fence check must be guaranteed to commit *before* the fence takes effect, so
/// there is never a "one last append" from a worker that has just lost write
/// authority. The barrier is `bump_generation`'s `ACCESS EXCLUSIVE` table lock
/// conflicting with the `ACCESS SHARE` that `assert_fence`'s plain read takes
/// implicitly — deliberately not a shared row lock, which on a
/// one-row-per-shard table would make every claim in the fleet a MultiXactId
/// producer.
#[tokio::test]
async fn a_fence_bump_cannot_commit_while_a_persist_holds_the_fence() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("barrier");
    let mut setup = connect(&url).await;
    ensure_generation_row(&mut setup, ShardId::new(0))
        .await
        .unwrap();

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");

    // Session A: open a transaction and pass the fence check. Its ACCESS SHARE
    // on harvest_shard_generation is now held until A commits.
    let mut a = connect(&url).await;
    a.batch_execute("BEGIN").await.expect("begin");
    assert_fence(&mut a, ShardId::new(0))
        .await
        .expect("the pinned epoch is current");

    // Session B: bump. It must BLOCK behind A rather than committing underneath
    // it. `bump_generation` carries a 5s lock_timeout, so a broken barrier
    // shows up as an immediate success and a working one as a timeout error.
    let mut b = connect(&url).await;
    let bump = bump_generation(&mut b, ShardId::new(0), "barrier probe", "test").await;
    let err = bump.expect_err("the bump must not commit while a persist holds the fence");
    assert!(
        err.to_string().contains("lock timeout") || err.to_string().contains("lock_timeout"),
        "expected the bump to block on the fence table lock, got: {err}"
    );

    // A commits; the bump now succeeds, and a persist starting afterwards is
    // fenced.
    a.batch_execute("COMMIT").await.expect("commit");
    let generation = bump_generation(&mut b, ShardId::new(0), "barrier probe", "test")
        .await
        .expect("the bump proceeds once the holder commits");
    assert_eq!(generation, ShardGeneration::new(1));
    let err = assert_fence(&mut setup, ShardId::new(0))
        .await
        .expect_err("a persist beginning after the bump must observe the new epoch");
    assert!(matches!(
        err,
        autumn_harvest::error::HarvestError::ShardFenced { .. }
    ));
    FenceRegistry::clear();
}

/// A [`PayloadStore`](autumn_harvest::payload_store::PayloadStore) whose
/// `put` blocks until released. A test can hold a batched append mid-
/// upload with it, and probe whether it holds the fence lock at that
/// moment.
struct BlockingStore {
    started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl autumn_harvest::payload_store::PayloadStore for BlockingStore {
    fn put(&self, bytes: &[u8]) -> autumn_harvest::payload_store::PayloadStoreFuture<'_, String> {
        let started = self.started.lock().unwrap().take();
        let release = self.release.lock().unwrap().take();
        let key = format!("blocked-{}", bytes.len());
        Box::pin(async move {
            if let Some(tx) = started {
                let _ = tx.send(());
            }
            if let Some(rx) = release {
                let _ = rx.await;
            }
            Ok(key)
        })
    }
    fn get(&self, _key: &str) -> autumn_harvest::payload_store::PayloadStoreFuture<'_, Vec<u8>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn delete(&self, _key: &str) -> autumn_harvest::payload_store::PayloadStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// A batched append must not hold the DR fence lock across its offload
/// upload (issue #1589 review).
///
/// `append_new_execution_started_events_batch` offloads before opening its
/// fenced transaction, per chunk. This mirrors the fix
/// `append_events_offloaded_with_codecs` already applies to the single-
/// execution append path. A concurrent `bump_generation` started while
/// the upload is in flight must therefore succeed immediately. It must
/// not block behind a fence read the pre-fix code would have held across
/// the whole upload. That is exactly the barrier the previous test
/// proves DOES block a persist that holds the fence.
#[tokio::test]
async fn a_batched_append_does_not_hold_the_fence_across_its_offload_upload() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("batchoffload");
    let mut setup = connect(&url).await;
    ensure_generation_row(&mut setup, ShardId::new(0))
        .await
        .unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, state, input, shard_id) \
         VALUES ($1, 'wf', 'k1', 'RUNNING', '{}'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut setup)
    .await
    .unwrap();

    FenceRegistry::clear();
    FenceRegistry::register(ShardId::new(0), ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(ShardId::new(0))
        .expect("no conflicting default shard in this test");

    let events = vec![(
        exec_id,
        autumn_harvest::event::WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
    )];

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let store = std::sync::Arc::new(BlockingStore {
        started: std::sync::Mutex::new(Some(started_tx)),
        release: std::sync::Mutex::new(Some(release_rx)),
    });
    // Threshold 0: even `input: {}` offloads, so `put` is guaranteed to run.
    let offloader = autumn_harvest::payload_store::PayloadOffloader::new(
        store,
        0,
        std::sync::Arc::new(NoOpMetrics),
    );

    let mut appender = connect(&url).await;
    let append_handle = tokio::spawn(async move {
        autumn_harvest::store::append_new_execution_started_events_batch(
            &mut appender,
            &events,
            Some(&offloader),
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
        )
        .await
    });

    started_rx
        .await
        .expect("the offload upload must start before the append can proceed");

    // The upload is in flight, blocked on `release_rx`. If the fence lock
    // were held across it (the pre-fix bug), this bump would block behind
    // it and time out. That is exactly what the previous test proves for
    // a persist that DOES hold the fence across an in-progress
    // transaction.
    let mut bumper = connect(&url).await;
    let generation = bump_generation(&mut bumper, ShardId::new(0), "concurrent bump", "test")
        .await
        .expect("a bump during the upload must not block on the fence lock");
    assert_eq!(generation, ShardGeneration::new(1));

    release_tx.send(()).expect("release the blocked upload");
    let result = append_handle.await.expect("append task must not panic");
    let err = result.expect_err("the fence must still catch the now-superseded generation");
    assert!(
        matches!(err, autumn_harvest::error::HarvestError::ShardFenced { .. }),
        "expected ShardFenced, got {err:?}"
    );
    FenceRegistry::clear();
}

/// A sequence owned by a **view** must never reach the promotion helper.
///
/// `ALTER SEQUENCE s OWNED BY <view>.<col>` is accepted by Postgres. Without a
/// `relkind` filter the helper would emit `... FROM <view>`, executing that
/// view's query — including any volatile function in it — on the operator's
/// high-privilege DR connection, during an incident, on a command whose output
/// nobody reads closely. Anyone with `CREATE` in the schema can plant it months
/// ahead, and the prescribed `FOR ALL TABLES` publication replicates it to the
/// standby too.
#[tokio::test]
async fn promotion_never_executes_a_view_that_owns_a_sequence() {
    let (url, _db) = require_db!("viewown");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE TABLE dr_probe_marker (hit int);
         CREATE FUNCTION dr_probe_fire() RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$
           BEGIN INSERT INTO dr_probe_marker VALUES (1); RETURN 1; END $$;
         CREATE VIEW dr_probe_view AS SELECT dr_probe_fire() AS id;
         CREATE SEQUENCE dr_probe_seq;
         ALTER SEQUENCE dr_probe_seq OWNED BY dr_probe_view.id;",
    )
    .await
    .expect("plant the view-owned sequence");

    let advanced = autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion must succeed, having simply skipped the view");
    assert!(
        !advanced
            .iter()
            .any(|(name, _)| name.contains("dr_probe_seq")),
        "a view-owned sequence must never be advanced: {advanced:?}"
    );

    #[derive(diesel::QueryableByName)]
    struct C {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<C> = diesel::sql_query("SELECT COUNT(*) AS n FROM dr_probe_marker")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        rows.into_iter().next().map_or(-1, |c| c.n),
        0,
        "the view's body must NOT have been executed"
    );
}

/// Promotion must never rewind a sequence that is already ahead of its table.
///
/// A sequence legitimately sits ahead of `MAX(col)` after cached values, a
/// rolled-back transaction, or deleted rows — and a *physical* replica
/// replicates sequences already, which is why the docs call this command a
/// harmless no-op there. Setting it to `MAX(col)` unconditionally would rewind
/// it and start re-issuing ids the database has already handed out: a
/// duplicate-key outage caused by the very command that exists to prevent one.
#[tokio::test]
async fn promotion_never_rewinds_a_sequence_that_is_ahead_of_its_table() {
    let (url, _db) = require_db!("seqahead");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE TABLE dr_ahead (id BIGSERIAL PRIMARY KEY);
         INSERT INTO dr_ahead DEFAULT VALUES;
         INSERT INTO dr_ahead DEFAULT VALUES;
         DELETE FROM dr_ahead WHERE id = 2;",
    )
    .await
    .expect("seed a sequence ahead of MAX(id)");

    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        v: i64,
    }
    async fn scalar(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
        let rows: Vec<N> = diesel::sql_query(sql).load(conn).await.expect("scalar");
        rows.into_iter().next().map_or(-1, |n| n.v)
    }

    // Precondition: MAX = 1 while the sequence has already issued 2.
    assert_eq!(
        scalar(&mut conn, "SELECT COALESCE(MAX(id), 0) AS v FROM dr_ahead").await,
        1
    );
    assert_eq!(
        scalar(
            &mut conn,
            "SELECT pg_sequence_last_value('dr_ahead_id_seq') AS v"
        )
        .await,
        2
    );

    autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion");

    assert_eq!(
        scalar(
            &mut conn,
            "SELECT pg_sequence_last_value('dr_ahead_id_seq') AS v"
        )
        .await,
        2,
        "the sequence must be left where it was, not rewound to MAX(id)"
    );
    assert_eq!(
        scalar(&mut conn, "SELECT nextval('dr_ahead_id_seq') AS v").await,
        3,
        "the next id must not collide with one already issued"
    );
}

/// Identifiers that are not plain lowercase must be advanced, not skipped.
///
/// An earlier revision screened identifiers instead of quoting them: an
/// embedder on a `PascalCase` ORM schema had **every** sequence silently skipped
/// while `harvest dr promote` reported success, and an ordinary table named
/// `user` or `order` passed the screen and then failed as a bare keyword.
#[tokio::test]
async fn promotion_advances_reserved_word_and_mixed_case_relations() {
    let (url, _db) = require_db!("quoting");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE TABLE \"user\" (id BIGSERIAL PRIMARY KEY);
         CREATE TABLE \"order\" (id BIGSERIAL PRIMARY KEY);
         CREATE TABLE \"MixedCase\" (\"Id\" BIGSERIAL PRIMARY KEY);
         INSERT INTO \"user\" DEFAULT VALUES;
         INSERT INTO \"user\" DEFAULT VALUES;
         INSERT INTO \"order\" DEFAULT VALUES;
         INSERT INTO \"MixedCase\" DEFAULT VALUES;",
    )
    .await
    .expect("seed awkwardly-named relations");

    // Rewind every sequence, exactly as an un-replicated logical standby would
    // have them.
    conn.batch_execute(
        "SELECT setval('\"user_id_seq\"', 1, false);
         SELECT setval('\"order_id_seq\"', 1, false);
         SELECT setval('\"MixedCase_Id_seq\"', 1, false);",
    )
    .await
    .expect("rewind sequences");

    let advanced = autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion must handle quoted identifiers");
    for expected in ["user_id_seq", "order_id_seq", "MixedCase_Id_seq"] {
        assert!(
            advanced.iter().any(|(name, _)| name.contains(expected)),
            "{expected} must have been advanced, got {advanced:?}"
        );
    }

    // And the advance is real: the next value must clear the existing rows.
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        v: i64,
    }
    let rows: Vec<N> = diesel::sql_query("SELECT nextval('\"user_id_seq\"') AS v")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        rows.into_iter().next().map_or(0, |n| n.v),
        3,
        "the next id must be max(id) + 1, not a duplicate"
    );
}

/// A sequence owned by a table in a non-public schema must be advanced,
/// using the OWNING TABLE's schema rather than the sequence's own
/// (finding 5).
///
/// The catalog query selected `tn.nspname` (the owning table's namespace)
/// but filtered on `sn.nspname = current_schema()` (the sequence's own).
/// Postgres always keeps an owned sequence in the same schema as its
/// table. The two names never diverge, so this distinction has no
/// effect on the current query. Filtering on the table's schema states
/// the intent plainly: promotion advances sequences for tables, not for
/// the sequences themselves. This test exercises the qualified-name path
/// with both objects outside `public`.
#[tokio::test]
async fn promotion_advances_a_sequence_owned_by_a_table_in_a_non_public_schema() {
    let (url, _db) = require_db!("crossschema");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE SCHEMA dr_seq_home;
         CREATE SEQUENCE dr_seq_home.dr_cross_seq;
         CREATE TABLE dr_seq_home.dr_cross (id BIGINT PRIMARY KEY DEFAULT nextval('dr_seq_home.dr_cross_seq'));
         ALTER SEQUENCE dr_seq_home.dr_cross_seq OWNED BY dr_seq_home.dr_cross.id;
         SET search_path TO dr_seq_home, public;
         INSERT INTO dr_cross DEFAULT VALUES;
         INSERT INTO dr_cross DEFAULT VALUES;
         SELECT setval('dr_seq_home.dr_cross_seq', 1, false);",
    )
    .await
    .expect("seed a table owning a sequence in a non-public schema");

    let advanced = autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion");
    assert!(
        advanced
            .iter()
            .any(|(name, _)| name.contains("dr_cross_seq")),
        "a sequence owned by a table in a non-public current_schema() must \
         be advanced; advanced: {advanced:?}"
    );

    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        v: i64,
    }
    let rows: Vec<N> = diesel::sql_query("SELECT nextval('dr_seq_home.dr_cross_seq') AS v")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        rows.into_iter().next().map_or(0, |n| n.v),
        3,
        "the next id must clear the two already-inserted rows, not collide with one"
    );
}

/// Promotion must not rewind a DESCENDING sequence (finding 11).
///
/// `GREATEST` assumes ascending issuance. For a descending sequence,
/// "furthest issued" is the MINIMUM, not the maximum. Using `GREATEST`
/// unconditionally reset a sequence that had issued 100 then 99 back to
/// 100. The next value handed out was then 99 again — a collision, from
/// the helper whose entire purpose is preventing one.
#[tokio::test]
async fn promotion_never_rewinds_a_descending_sequence() {
    let (url, _db) = require_db!("seqdesc");
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE SEQUENCE dr_desc_seq INCREMENT BY -1 MINVALUE -1000000 MAXVALUE -1 START -1;
         CREATE TABLE dr_desc (id BIGINT PRIMARY KEY DEFAULT nextval('dr_desc_seq'));
         ALTER SEQUENCE dr_desc_seq OWNED BY dr_desc.id;
         INSERT INTO dr_desc DEFAULT VALUES; -- issues -1
         INSERT INTO dr_desc DEFAULT VALUES; -- issues -2",
    )
    .await
    .expect("seed a descending owned sequence that has issued -1 then -2");

    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        v: i64,
    }
    async fn scalar(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
        let rows: Vec<N> = diesel::sql_query(sql).load(conn).await.expect("scalar");
        rows.into_iter().next().map_or(i64::MIN, |n| n.v)
    }

    assert_eq!(
        scalar(
            &mut conn,
            "SELECT pg_sequence_last_value('dr_desc_seq') AS v"
        )
        .await,
        -2
    );

    autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion");

    assert_eq!(
        scalar(
            &mut conn,
            "SELECT pg_sequence_last_value('dr_desc_seq') AS v"
        )
        .await,
        -2,
        "a descending sequence must not be reset back up to a table MAX"
    );
    assert_eq!(
        scalar(&mut conn, "SELECT nextval('dr_desc_seq') AS v").await,
        -3,
        "the next id must continue descending past what was already issued, not collide"
    );
}

/// The watermark beat must never leave an advisory lock behind.
///
/// The single-writer gate uses `pg_try_advisory_xact_lock`, not the
/// session-scoped variant. A session lock is released only by an explicit
/// unlock, so any error between acquire and release leaks it — and on a
/// **pooled** connection it leaks permanently: every other worker's sampler
/// then skips its beat forever, re-acquiring on the same session only bumps the
/// lock count, and the measured RPO goes stale during exactly the database
/// trouble it exists to measure. (Verified against live Postgres: an advisory
/// lock does survive a failed statement in the same session.)
#[tokio::test]
async fn the_watermark_beat_leaves_no_advisory_lock_behind() {
    let (url, _db) = require_db!("beatlock");
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    #[derive(diesel::QueryableByName)]
    struct C {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    async fn advisory_locks_held(conn: &mut AsyncPgConnection) -> i64 {
        let rows: Vec<C> = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM pg_locks \
             WHERE locktype = 'advisory' AND pid = pg_backend_pid()",
        )
        .load(conn)
        .await
        .expect("count advisory locks");
        rows.into_iter().next().map_or(-1, |c| c.n)
    }

    for _ in 0..3 {
        autumn_harvest::replication::record_replication_heartbeat(
            &mut conn,
            shard,
            std::time::Duration::from_secs(3600),
            BEAT_INTERVAL,
        )
        .await
        .expect("beat");
        assert_eq!(
            advisory_locks_held(&mut conn).await,
            0,
            "the beat must hold no advisory lock once it returns"
        );
    }

    // …and a beat on a *different* connection is never starved by a previous
    // one, which is what a leaked session lock would cause.
    let mut other = connect(&url).await;
    autumn_harvest::replication::record_replication_heartbeat(
        &mut other,
        shard,
        std::time::Duration::from_secs(3600),
        BEAT_INTERVAL,
    )
    .await
    .expect("a second connection must still be able to take the beat");
}

/// The promotion's statement timeout must actually be in force.
///
/// `SET LOCAL` outside a transaction block is **ignored** by Postgres — it
/// emits a `WARNING` and `statement_timeout` reads back as `0`. Verified
/// against live Postgres. The helper therefore has to run inside a transaction,
/// or the ceiling it documents on the RTO-critical path is silently absent.
#[tokio::test]
async fn the_promotion_runs_inside_a_transaction_so_its_timeout_applies() {
    let (url, _db) = require_db!("promotetx");
    let mut conn = connect(&url).await;

    // A table whose `MAX()` scan is slow enough to prove the timeout is armed
    // would make this test slow too. Instead, assert the property the timeout
    // depends on: the helper must not leave `statement_timeout` set at session
    // level (which would mean it used `SET`, leaking the ceiling onto every
    // later query on a pooled connection), and must succeed — which under
    // `SET LOCAL` is only possible inside a transaction.
    autumn_harvest::replication::advance_sequences_after_promotion(&mut conn)
        .await
        .expect("promotion succeeds");

    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        timeout: String,
    }
    let rows: Vec<S> = diesel::sql_query("SELECT current_setting('statement_timeout') AS timeout")
        .load(&mut conn)
        .await
        .expect("read statement_timeout");
    assert_eq!(
        rows.into_iter().next().map(|s| s.timeout).as_deref(),
        Some("0"),
        "the promotion's timeout must be transaction-local, not leaked onto the session"
    );
}

/// The subscription conninfo must address the server, not the test client.
///
/// Regression guard for the CI-only failure that the three two-region tests hit
/// the first time they ever ran under Docker: the conninfo was built from the
/// *client-side* URL, so it carried the host-mapped port (`localhost:33624`).
/// `CREATE SUBSCRIPTION` dials the publisher from inside the server, where that
/// port does not exist, and every two-region test died with "could not connect
/// to the publisher ... Connection refused".
///
/// It passed on a host-installed Postgres because there the client port and the
/// server port are the same number, which is exactly why no local run could
/// catch it. This test creates that divergence deliberately by passing a URL
/// whose port is wrong, and asserts the conninfo ignores it.
#[tokio::test]
async fn the_subscription_conninfo_uses_the_servers_own_port_not_the_clients() {
    let (url, _db) = require_db!("conninfo");
    let mut conn = connect(&url).await;

    // A port the server is certainly NOT listening on, standing in for the
    // host-mapped port a container publishes.
    let lying_url = "postgres://postgres@localhost:1/postgres";
    let conninfo = server_side_conninfo(&mut conn, lying_url, "somedb").await;

    assert!(
        !conninfo.contains("port=1 "),
        "the conninfo must not carry the client-side port; got {conninfo:?}"
    );
    assert!(
        conninfo.contains("host=127.0.0.1"),
        "the conninfo must dial loopback explicitly — `localhost` resolved to ::1 in CI, which \
         a server not listening on IPv6 refuses; got {conninfo:?}"
    );

    #[derive(diesel::QueryableByName)]
    struct Port {
        #[diesel(sql_type = diesel::sql_types::Text)]
        port: String,
    }
    let actual = diesel::sql_query("SELECT current_setting('port') AS port")
        .load::<Port>(&mut conn)
        .await
        .expect("read port")
        .into_iter()
        .next()
        .expect("one row")
        .port;
    assert!(
        conninfo.contains(&format!("port={actual}")),
        "the conninfo must carry the server's own port ({actual}); got {conninfo:?}"
    );
}

// ── AC4 / AC6(c): measured RPO ─────────────────────────────────────────────

#[tokio::test]
async fn replication_status_on_a_primary_with_no_standby_is_not_a_zero_rpo() {
    let (url, _db) = require_db!("norepl");
    let mut conn = connect(&url).await;
    let status = query_replication_status(&mut conn, ShardId::new(0), DR_PREFIX)
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

/// One abandoned DR slot beside one healthy one must be a PARTIAL reading,
/// never folded into "nothing measured" (finding 1).
///
/// A physical slot created with `immediately_reserve = false` has a NULL
/// `restart_lsn` until a standby connects — a never-connected or abandoned
/// DR target. Before the fix, `bool_or(position IS NULL)` made the WHOLE
/// reading `Unknown` whenever any one slot lacked a position. A healthy
/// slot's small lag then masked the abandoned one entirely.
/// `rpo_seconds()` fell back to `replay_lag`, which has no idea the
/// abandoned slot exists either.
#[tokio::test]
async fn one_unmeasurable_slot_beside_a_measurable_one_is_a_partial_reading() {
    let (url, _db) = require_db!("partialslot");
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);

    let reserved = format!("{DR_PREFIX}_reserved_{}", std::process::id());
    let unreserved = format!("{DR_PREFIX}_unreserved_{}", std::process::id());
    diesel::sql_query("SELECT pg_create_physical_replication_slot($1, true)")
        .bind::<diesel::sql_types::Text, _>(reserved.clone())
        .execute(&mut conn)
        .await
        .expect("create the reserved (measurable) physical slot");
    diesel::sql_query("SELECT pg_create_physical_replication_slot($1, false)")
        .bind::<diesel::sql_types::Text, _>(unreserved.clone())
        .execute(&mut conn)
        .await
        .expect("create the unreserved (never-connected) physical slot");

    let reading = autumn_harvest::replication::measure_rpo(&mut conn, shard, DR_PREFIX)
        .await
        .expect("measure_rpo");
    match reading {
        WatermarkReading::PartiallyMeasured {
            unmeasurable_slots, ..
        } => {
            assert_eq!(
                unmeasurable_slots, 1,
                "exactly the unreserved slot must be counted unmeasurable"
            );
        }
        other => panic!(
            "one measurable slot beside one unmeasurable one must be a partial reading, got \
             {other:?}"
        ),
    }
    let status = query_replication_status(&mut conn, shard, DR_PREFIX)
        .await
        .expect("status");
    assert_eq!(
        status.unmeasurable_slot_count(),
        1,
        "the count must also be visible from ReplicationStatus"
    );
    assert!(
        status.rpo_seconds().is_none() || status.max_replay_lag_seconds().is_none(),
        "a partial reading with nothing measured yet must never silently resolve via replay_lag"
    );

    for name in [reserved, unreserved] {
        let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
            .bind::<diesel::sql_types::Text, _>(name)
            .execute(&mut conn)
            .await;
    }
}

/// An unrelated logical-decoding consumer must not read as a DR standby.
///
/// A shard database can legitimately host a CDC pipeline's slot alongside its
/// DR subscription. Counting every walsender for the database meant that if the
/// real cross-region subscriber disconnected, `connected_standbys()` stayed
/// non-zero, `harvest dr status` reported the shard protected, and
/// `harvest_replication_down` never fired — the most dangerous false negative
/// this feature has, because it is silent and it is wrong in the safe-looking
/// direction.
#[tokio::test]
async fn a_non_dr_slot_is_not_counted_as_a_dr_standby() {
    let (url, _db) = require_db!("cdcslot");
    let mut conn = connect(&url).await;

    // A CDC-style logical slot with a name that is nothing to do with DR.
    let slot = format!("cdc_pipeline_{}", std::process::id());
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("create the non-DR slot");

    let status = query_replication_status(&mut conn, ShardId::new(0), DR_PREFIX)
        .await
        .expect("status");
    assert_eq!(
        status.connected_standbys(),
        0,
        "a CDC slot must not be counted as a DR standby"
    );
    assert_eq!(
        status.inactive_slots(),
        0,
        "and it must not appear in the DR slot inventory either: {status:?}"
    );
    assert_eq!(
        status.max_lag_bytes(),
        None,
        "its WAL backlog is not this shard's DR backlog"
    );

    // Sanity: the same query DOES see a slot carrying the DR prefix, so the
    // assertions above are about the filter and not about an empty database.
    let dr_slot = format!("{DR_PREFIX}_cdcprobe_{}", std::process::id());
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(dr_slot.clone())
        .execute(&mut conn)
        .await
        .expect("create the DR slot");
    let status = query_replication_status(&mut conn, ShardId::new(0), DR_PREFIX)
        .await
        .expect("status");
    assert_eq!(
        status.inactive_slots(),
        1,
        "the DR-prefixed slot must be visible: {status:?}"
    );

    for name in [slot, dr_slot] {
        let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
            .bind::<diesel::sql_types::Text, _>(name)
            .execute(&mut conn)
            .await;
    }
}

/// A slot name that would satisfy `LIKE <prefix> || '%'` but does NOT start
/// with the prefix must never be counted as a DR standby (finding 7).
///
/// `LIKE` treats `_` as "any single character", and `DR_PREFIX`
/// (`harvest_dr`, the shipped default) contains one. Reproduced against live
/// Postgres in the finding: `'harvestXdr_shard0' LIKE 'harvest_dr' || '%'` is
/// `true`. Suppose the real DR sender then disconnected while this unrelated
/// slot remained. `connected_standbys()` would stay non-zero, and
/// `harvest_replication_down` would never fire — on the DEFAULT
/// configuration, not only a custom prefix.
#[tokio::test]
async fn a_slot_matching_the_prefix_only_under_like_wildcards_is_not_counted() {
    let (url, _db) = require_db!("likewild");
    let mut conn = connect(&url).await;

    // `x` stands in for the prefix's own underscore: differs at that one
    // position, so `starts_with` correctly rejects it while `LIKE` would not.
    // Lowercase only -- Postgres replication slot names allow no other case.
    let slot = format!("harvestxdr_wildprobe_{}", std::process::id());
    assert_ne!(&slot[..10], DR_PREFIX, "the probe must not literally match");
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("create the wildcard-matching slot");

    let status = query_replication_status(&mut conn, ShardId::new(0), DR_PREFIX)
        .await
        .expect("status");
    assert_eq!(
        status.connected_standbys(),
        0,
        "a slot that only matches under LIKE's wildcard semantics must not count"
    );
    assert_eq!(
        status.inactive_slots(),
        0,
        "and must not appear in the DR slot inventory either: {status:?}"
    );

    let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
        .bind::<diesel::sql_types::Text, _>(slot)
        .execute(&mut conn)
        .await;
}

// ── The worker lifecycle: pin at startup, self-fence, stop ─────────────────

/// A DR-enabled worker pins at startup, and a fence stops it.
///
/// The claim gate and the persist assert are proven above at the SQL layer;
/// this covers the half that only exists at runtime — `pin_dr_generations`
/// (which must run *before* fleet registration and the first poll),
/// `spawn_replication_sampler`'s self-fence check, the `harvest.shard.fenced`
/// counter, and the worker actually shutting down rather than idling forever
/// claiming nothing.
///
/// Deliberately a **single-pool** worker with no `ShardedDbPool`: that is the
/// deployment shape the topology doc documents (`.with_dr_fencing(true)` and
/// nothing else), and an earlier revision gated the sampler on
/// `sharded_pool.is_some()` — so exactly this configuration got the claim gate
/// but never beat a watermark, never measured an RPO, and never stopped when
/// fenced.
#[tokio::test]
async fn a_dr_enabled_worker_pins_at_startup_and_stops_when_fenced() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("workerfence");

    // A non-zero shard, so a regression that fabricates shard 0 fails here.
    let shard = ShardId::new(3);
    FenceRegistry::clear();
    autumn_harvest::replication::set_dr_config(autumn_harvest::replication::DrConfig {
        fencing: DrFencing::Enabled,
        sample_interval: std::time::Duration::from_millis(300),
        watermark_retain: std::time::Duration::from_secs(3600),
        slot_prefix: DR_PREFIX.to_string(),
    });

    let fenced_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let telemetry = std::sync::Arc::new(
        autumn_harvest::telemetry::TelemetryConfig::builder()
            .metrics(std::sync::Arc::new(FenceCounter {
                fenced: std::sync::Arc::clone(&fenced_count),
            }))
            .build(),
    );
    let registry = std::sync::Arc::new(
        autumn_harvest::worker::HandlerRegistry::with_state_and_telemetry(
            Vec::new(),
            Vec::new(),
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        ),
    );

    let mut config = dr_worker_config();
    config.shard_assignments = vec![shard];
    let worker = autumn_harvest::worker::Worker::new(config, registry).expect("worker builds");

    let pool = dr_pool(&url);
    let run = tokio::spawn(async move { worker.run(&pool).await });

    // Pinning happens before registration and the first poll, so both the row
    // and the published registry appear within moments of startup.
    let pin_url = url.clone();
    eventually(
        "the worker to pin its generation",
        std::time::Duration::from_secs(30),
        || {
            let url = pin_url.clone();
            async move {
                let mut conn = connect(&url).await;
                matches!(current_generation(&mut conn, shard).await, Ok(Some(_)))
                    && FenceRegistry::expected(shard).is_some()
            }
        },
    )
    .await;

    // Fence it, as the promoted primary would.
    {
        let mut conn = connect(&url).await;
        bump_generation(&mut conn, shard, "worker lifecycle drill", "test")
            .await
            .expect("bump");
    }

    // `run()` returning is the observable proof that the worker stopped.
    // Idling forever while claiming nothing is the failure mode this asserts
    // against, so a timeout here is a real failure, not flake.
    tokio::time::timeout(std::time::Duration::from_secs(60), run)
        .await
        .expect("a fenced worker must shut down, not idle claiming nothing")
        .expect("worker task must not panic");

    // UFCS: diesel's blanket `RunQueryDsl::load` shadows `AtomicU64::load`
    // through the `Arc` deref in this module.
    assert!(
        std::sync::atomic::AtomicU64::load(&fenced_count, std::sync::atomic::Ordering::SeqCst) > 0,
        "the worker must record harvest.shard.fenced before stopping — an operator's only \\
         signal that a fleet is pinned to a superseded epoch"
    );

    FenceRegistry::clear();
    autumn_harvest::replication::set_dr_config(autumn_harvest::replication::DrConfig::default());
}

/// A worker's shutdown writes check the live fence (issue #1823). The
/// sampler stops with the worker, so a bump during shutdown never sets the
/// fenced-out flag. The fleet row must still not change after the bump.
#[tokio::test]
async fn a_shutdown_after_a_bump_leaves_the_fleet_row_alone() {
    #[derive(diesel::QueryableByName)]
    struct Status {
        #[diesel(sql_type = diesel::sql_types::Text)]
        status: String,
    }
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("shutdownfence");
    let shard = ShardId::new(3);
    let hour = std::time::Duration::from_secs(3600);
    FenceRegistry::clear();
    autumn_harvest::replication::set_dr_config(autumn_harvest::replication::DrConfig {
        fencing: DrFencing::Enabled,
        sample_interval: hour,
        watermark_retain: hour,
        slot_prefix: DR_PREFIX.to_string(),
    });
    // Neither the sampler nor the heartbeat may see the bump first.
    let mut config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default()
            .with_dr_fencing(true)
            .with_replication_sample_interval(hour),
    );
    config.shard_assignments = vec![shard];
    config.worker_heartbeat_interval = hour;
    let worker_id = config.worker_id.clone();
    let registry = std::sync::Arc::new(autumn_harvest::worker::HandlerRegistry::new(
        Vec::new(),
        Vec::new(),
    ));
    let worker = std::sync::Arc::new(
        autumn_harvest::worker::Worker::new(config, registry).expect("worker builds"),
    );
    let pool = dr_pool(&url);
    let runner = std::sync::Arc::clone(&worker);
    let run = tokio::spawn(async move { runner.run(&pool).await });
    let status = |url: String, worker_id: String| async move {
        let mut conn = connect(&url).await;
        let rows: Vec<Status> =
            diesel::sql_query("SELECT status FROM harvest_workers WHERE worker_id = $1")
                .bind::<diesel::sql_types::Text, _>(worker_id)
                .load(&mut conn)
                .await
                .unwrap_or_default();
        <[Status]>::first(&rows).map(|row| row.status.clone())
    };
    eventually(
        "the worker to register",
        std::time::Duration::from_secs(30),
        || {
            let (url, worker_id) = (url.clone(), worker_id.clone());
            async move { status(url, worker_id).await.as_deref() == Some("Active") }
        },
    )
    .await;

    {
        let mut conn = connect(&url).await;
        bump_generation(&mut conn, shard, "failover", "test")
            .await
            .expect("bump");
    }
    worker.shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(60), run)
        .await
        .expect("the worker stops")
        .expect("the worker task must not panic");

    assert_eq!(
        status(url.clone(), worker_id.clone()).await.as_deref(),
        Some("Active"),
        "a worker that lost write authority must not write its fleet status"
    );
}

/// A detached completion-trigger relay holds its own fence (issue #1823).
/// It runs after the caller returns, so the caller's fence no longer covers
/// it. A source shard whose pin is superseded keeps its outbox row.
#[tokio::test]
async fn a_detached_trigger_relay_writes_nothing_on_a_fenced_source() {
    #[derive(diesel::QueryableByName)]
    struct Rows {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        rows: i64,
    }
    let _serial = registry_guard().await;
    let (source_url, _source_db) = require_db!("relaysrc");
    let (target_url, _target_db) = require_db!("relaytgt");
    let (source, target) = (ShardId::new(0), ShardId::new(1));
    let source_pool = dr_pool(&source_url);
    let pools: std::collections::BTreeMap<_, _> = [
        (source, source_pool.clone()),
        (target, dr_pool(&target_url)),
    ]
    .into_iter()
    .collect();
    let sharded = autumn_harvest::shard::ShardedDbPool::from_map(pools, source);

    let outbox_id = uuid::Uuid::new_v4();
    let source_exec_id = ExecutionId::new_for_shard(source).as_uuid();
    let mut conn = connect(&source_url).await;
    diesel::sql_query(
        "INSERT INTO harvest_completion_trigger_outbox \
            (id, source_exec_id, trigger_id, target_shard, target_workflow_name, \
             target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes) \
         VALUES ($1, $2, $3, 1, 'dr_relay_target', 'dr-relay', '{}'::jsonb, \
                 'default', '0'::jsonb, 1048576)",
    )
    .bind::<diesel::sql_types::Uuid, _>(outbox_id)
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id)
    .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::new_v4())
    .execute(&mut conn)
    .await
    .expect("insert the outbox row");
    // The source pin is superseded: another region owns the source shard.
    let pinned = ensure_generation_row(&mut conn, source).await.unwrap();
    FenceRegistry::publish(&[(source, pinned)], source).expect("pin");
    bump_generation(&mut conn, source, "failover", "test")
        .await
        .expect("bump");

    autumn_harvest::completion_trigger::DeferredTriggerStart {
        outbox_id,
        source_exec_id,
        trigger_id: uuid::Uuid::new_v4(),
        source_shard: source,
        target_shard: target,
        target_workflow_name: "dr_relay_target".to_string(),
        target_workflow_id: "dr-relay".to_string(),
        target_input: serde_json::json!({}),
        queue_name: Some("default".to_string()),
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 1_048_576,
        trigger_name: "dr_relay".to_string(),
        owner: None,
        runbook_url: None,
        severity: None,
        sla: None,
        retry_policy: None,
        max_workflow_attempts_ceiling: None,
        codecs: autumn_harvest::payload_codec::PayloadCodecs::default(),
    }
    .spawn();
    // The relay is fire-and-forget. Give it time to run.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let remaining: Vec<Rows> =
        diesel::sql_query("SELECT count(*) AS rows FROM harvest_completion_trigger_outbox")
            .load(&mut conn)
            .await
            .expect("count the outbox");
    let _ = autumn_harvest::shard::ShardedDbPool::single(source_pool);
    drop(sharded);

    assert_eq!(
        <[Rows]>::first(&remaining).map(|row| row.rows),
        Some(1),
        "a relay must not write a source shard whose pin is superseded"
    );
}

/// An audit write checks the fence in its own transaction (issue #1823). A
/// read route such as the event stream writes audit rows but takes no fence
/// barrier. A process whose pin is superseded must write none.
#[tokio::test]
async fn a_superseded_pin_writes_no_audit_row() {
    #[derive(diesel::QueryableByName)]
    struct Rows {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        rows: i64,
    }
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("auditfence");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    FenceRegistry::publish(&[(ShardId::new(0), pinned)], ShardId::new(0)).expect("pin");
    bump_generation(&mut conn, ShardId::new(0), "failover", "test")
        .await
        .expect("bump");
    let record = autumn_harvest::models::NewAuditRecord {
        actor: "reader",
        operation: "execution.stream.open",
        target_type: "execution",
        target_id: None,
        route_or_command: "GET /executions/{exec_id}/events/stream",
        request_id: None,
        idempotency_key: None,
        status: autumn_harvest::audit::STATUS_SUCCEEDED,
        error_summary: None,
        shard_id: Some(0),
        source: "api",
    };

    let single = autumn_harvest::audit::insert_audit(&mut conn, &record).await;
    let batch =
        autumn_harvest::audit::insert_audit_batch(&mut conn, std::slice::from_ref(&record)).await;
    let rows: Vec<Rows> = diesel::sql_query("SELECT count(*) AS rows FROM harvest_audit_log")
        .load(&mut conn)
        .await
        .expect("count the audit rows");

    assert!(
        matches!(
            single,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "a superseded pin must not write an audit row: {single:?}"
    );
    assert!(
        matches!(
            batch,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "a superseded pin must not write an audit batch: {batch:?}"
    );
    assert_eq!(<[Rows]>::first(&rows).map(|row| row.rows), Some(0));
}

// ── Fence on by default where DR is configured (issue #1823) ───────────────

#[tokio::test]
async fn the_probe_finds_no_dr_marker_on_a_plain_database() {
    let (url, db) = require_db!("probeplain");
    let mut conn = connect(&url).await;
    let markers = probe_dr_markers(&mut conn, &unique_prefix(&db))
        .await
        .expect("probe");
    assert_eq!(markers, DrMarkers::default(), "{markers:?}");
    assert!(!markers.is_dr());
}

#[tokio::test]
async fn the_probe_finds_a_generation_row() {
    let (url, _db) = require_db!("proberow");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(5))
        .await
        .expect("provision");
    let markers = probe_dr_markers(&mut conn, DR_PREFIX).await.expect("probe");
    assert_eq!(markers.generation_shards, vec![ShardId::new(5)]);
    assert!(markers.is_dr());
}

/// A DR slot on THIS database marks it. A slot with another prefix does not.
#[tokio::test]
async fn the_probe_finds_a_logical_dr_slot_on_this_database_only() {
    let (url, db) = require_db!("probeslot");
    if !wal_level_is_logical(&url).await {
        eprintln!("SKIPPED probeslot: wal_level is not logical");
        return;
    }
    let mut conn = connect(&url).await;
    let dr_slot = format!("{DR_PREFIX}_{db}");
    let cdc_slot = format!("cdc_{db}");
    for slot in [&dr_slot, &cdc_slot] {
        diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
            .bind::<diesel::sql_types::Text, _>(slot.clone())
            .execute(&mut conn)
            .await
            .expect("create slot");
    }
    let markers = probe_dr_markers(&mut conn, DR_PREFIX).await;
    let other = probe_dr_markers(&mut conn, "no_such_prefix").await;
    // The same slot, seen from another database, must not count there.
    let elsewhere = match fresh_db("probeslotother").await {
        Some((other_url, _)) => {
            let mut other_conn = connect(&other_url).await;
            Some(probe_dr_markers(&mut other_conn, &dr_slot).await)
        }
        None => None,
    };
    for slot in [&dr_slot, &cdc_slot] {
        let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
            .bind::<diesel::sql_types::Text, _>(slot.clone())
            .execute(&mut conn)
            .await;
    }
    let markers = markers.expect("probe");
    assert_eq!(markers.dr_slots, 1, "{markers:?}");
    assert!(markers.is_dr());
    assert!(!other.expect("probe").is_dr(), "only the DR prefix counts");
    if let Some(elsewhere) = elsewhere {
        assert_eq!(
            elsewhere.expect("probe").dr_slots,
            0,
            "a logical slot marks its own database only"
        );
    }
}

#[tokio::test]
async fn auto_mode_leaves_a_plain_database_unfenced() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("autoplain");
    let pool = dr_pool(&url);
    FenceRegistry::clear();
    let targets = Some((vec![(ShardId::new(2), pool.clone())], ShardId::new(2)));
    let resolved = pin_process_fence(DrFencing::Auto, &unique_prefix(&db), targets, &pool)
        .await
        .expect("a plain database starts");
    let enabled = FenceRegistry::is_enabled();
    let mut conn = connect(&url).await;
    let row = current_generation(&mut conn, ShardId::new(2))
        .await
        .unwrap();
    FenceRegistry::clear();
    assert!(resolved.is_none(), "no marker means no fence");
    assert!(!enabled, "nothing is pinned");
    assert_eq!(row, None, "Auto never provisions a plain database");
}

#[tokio::test]
async fn auto_mode_pins_a_dr_database() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("autodr");
    let pool = dr_pool(&url);
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(2))
        .await
        .unwrap();
    bump_generation(&mut conn, ShardId::new(2), "earlier failover", "test")
        .await
        .unwrap();
    FenceRegistry::clear();
    let targets = Some((vec![(ShardId::new(2), pool.clone())], ShardId::new(2)));
    let resolved = pin_process_fence(DrFencing::Auto, DR_PREFIX, targets, &pool)
        .await
        .expect("a DR database starts fenced");
    let pinned = FenceRegistry::expected(ShardId::new(2));
    FenceRegistry::clear();
    assert_eq!(
        resolved.map(|targets| targets.len()),
        Some(1),
        "the fence is on"
    );
    assert_eq!(
        pinned,
        Some(ShardGeneration::new(1)),
        "pins the current epoch"
    );
}

/// With no shard identity, Auto reads the shard from the single row.
#[tokio::test]
async fn auto_mode_reads_the_shard_from_a_single_generation_row() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("autoinfer");
    let pool = dr_pool(&url);
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(7))
        .await
        .unwrap();
    FenceRegistry::clear();
    let resolved = pin_process_fence(DrFencing::Auto, DR_PREFIX, None, &pool)
        .await
        .expect("one row names the shard");
    let pinned = FenceRegistry::expected(ShardId::new(7));
    let unencoded = FenceRegistry::expected(ShardId::UNENCODED);
    FenceRegistry::clear();
    assert_eq!(
        resolved.map(|targets| targets.iter().map(|(s, _)| *s).collect::<Vec<_>>()),
        Some(vec![ShardId::new(7)])
    );
    assert_eq!(pinned, Some(ShardGeneration::INITIAL));
    assert_eq!(
        unencoded,
        Some(ShardGeneration::INITIAL),
        "default shard is 7"
    );
}

/// A DR database with no row and no shard identity cannot be fenced. The
/// process refuses to start rather than invent shard 0.
#[tokio::test]
async fn auto_mode_refuses_a_dr_database_it_cannot_name() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("autononame");
    let pool = dr_pool(&url);
    let mut conn = connect(&url).await;
    for shard in [1, 2] {
        ensure_generation_row(&mut conn, ShardId::new(shard))
            .await
            .unwrap();
    }
    FenceRegistry::clear();
    let refused = pin_process_fence(DrFencing::Auto, DR_PREFIX, None, &pool).await;
    let enabled = FenceRegistry::is_enabled();
    FenceRegistry::clear();
    let Err(error) = refused else {
        panic!("two rows name no single shard");
    };
    assert!(error.to_string().contains("shard"), "{error}");
    assert!(!enabled, "a refused start pins nothing");
}

#[tokio::test]
async fn disabled_mode_refuses_a_dr_database() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("disableddr");
    let pool = dr_pool(&url);
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(2))
        .await
        .unwrap();
    FenceRegistry::clear();
    let targets = Some((vec![(ShardId::new(2), pool.clone())], ShardId::new(2)));
    let refused = pin_process_fence(DrFencing::Disabled, DR_PREFIX, targets.clone(), &pool).await;
    let enabled = FenceRegistry::is_enabled();
    FenceRegistry::clear();
    let Err(error) = refused else {
        panic!("Disabled on a DR database must refuse to start");
    };
    assert!(
        matches!(error, autumn_harvest::error::HarvestError::Config(_)),
        "{error:?}"
    );
    assert!(!enabled);

    // A plain database runs unfenced under Disabled, as before.
    let (plain_url, plain_db) = require_db!("disabledplain");
    let plain = dr_pool(&plain_url);
    let plain_targets = Some((vec![(ShardId::new(2), plain.clone())], ShardId::new(2)));
    let prefix = unique_prefix(&plain_db);
    let resolved = pin_process_fence(DrFencing::Disabled, &prefix, plain_targets, &plain)
        .await
        .expect("a plain database starts");
    assert!(resolved.is_none());
}

/// A worker configured `Disabled` does not start on a DR database: it never
/// registers in the fleet and never claims.
#[tokio::test]
async fn a_worker_configured_disabled_refuses_to_start_on_a_dr_database() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("workerdisabled");
    let shard = ShardId::new(3);
    {
        let mut conn = connect(&url).await;
        ensure_generation_row(&mut conn, shard).await.unwrap();
    }
    FenceRegistry::clear();
    let mut config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_dr_fencing(false),
    );
    config.shard_assignments = vec![shard];
    let registry = std::sync::Arc::new(autumn_harvest::worker::HandlerRegistry::new(
        Vec::new(),
        Vec::new(),
    ));
    let worker = autumn_harvest::worker::Worker::new(config, registry).expect("worker builds");
    let pool = dr_pool(&url);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), worker.run(&pool)).await;
    let enabled = FenceRegistry::is_enabled();
    FenceRegistry::clear();
    autumn_harvest::replication::set_dr_config(autumn_harvest::replication::DrConfig::default());
    outcome.expect("a disagreeing worker must stop at once, not run");
    assert!(!enabled, "a refused worker pins nothing");
    assert_eq!(
        count_on(&url, "SELECT COUNT(*) AS n FROM harvest_workers").await,
        0,
        "a refused worker never registers in the fleet"
    );
}

/// The default worker config fences a DR database with no extra setting.
#[tokio::test]
async fn a_default_worker_fences_a_dr_database() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("workerauto");
    let shard = ShardId::new(4);
    {
        let mut conn = connect(&url).await;
        ensure_generation_row(&mut conn, shard).await.unwrap();
    }
    FenceRegistry::clear();
    let config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default()
            .with_replication_sample_interval(std::time::Duration::from_millis(300)),
    );
    let registry = std::sync::Arc::new(autumn_harvest::worker::HandlerRegistry::new(
        Vec::new(),
        Vec::new(),
    ));
    let worker = autumn_harvest::worker::Worker::new(config, registry).expect("worker builds");
    let pool = dr_pool(&url);
    let run = tokio::spawn(async move { worker.run(&pool).await });
    eventually(
        "the default worker to pin its generation",
        std::time::Duration::from_secs(30),
        || async move { FenceRegistry::expected(shard).is_some() },
    )
    .await;
    {
        let mut conn = connect(&url).await;
        bump_generation(&mut conn, shard, "failover", "test")
            .await
            .unwrap();
    }
    let stopped = tokio::time::timeout(std::time::Duration::from_secs(60), run).await;
    FenceRegistry::clear();
    autumn_harvest::replication::set_dr_config(autumn_harvest::replication::DrConfig::default());
    stopped
        .expect("a fenced default worker must stop")
        .expect("worker task must not panic");
}

/// A DR subscription marks a logical standby. No process may start there,
/// and nothing is provisioned: a local row would collide with the row that
/// replication later delivers.
#[tokio::test]
async fn a_logical_standby_refuses_to_start_and_provisions_nothing() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("standbysub");
    let mut conn = connect(&url).await;
    let sub = format!("{DR_PREFIX}_sub_{db}");
    diesel::sql_query(format!(
        "CREATE SUBSCRIPTION {sub} CONNECTION 'dbname=unused' PUBLICATION harvest_dr \
         WITH (connect = false)"
    ))
    .execute(&mut conn)
    .await
    .expect("create a disconnected subscription");

    let markers = probe_dr_markers(&mut conn, DR_PREFIX).await;
    let pool = dr_pool(&url);
    let targets = Some((vec![(ShardId::new(0), pool.clone())], ShardId::new(0)));
    let refused = pin_process_fence(DrFencing::Auto, DR_PREFIX, targets, &pool).await;
    let row = current_generation(&mut conn, ShardId::new(0)).await;
    let _ = diesel::sql_query(format!("ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)"))
        .execute(&mut conn)
        .await;
    let _ = diesel::sql_query(format!("DROP SUBSCRIPTION {sub}"))
        .execute(&mut conn)
        .await;

    let markers = markers.expect("probe");
    assert_eq!(markers.dr_subscriptions, 1, "{markers:?}");
    assert!(markers.is_dr() && markers.is_standby());
    let Err(error) = refused else {
        panic!("a standby must refuse to start");
    };
    assert!(error.to_string().contains("standby"), "{error}");
    assert_eq!(
        row.expect("read"),
        None,
        "nothing is provisioned on a standby"
    );
    assert!(!FenceRegistry::is_enabled());
}

/// A subscription may have any local name. Its slot name carries the DR
/// prefix, so the slot name marks the standby too.
#[tokio::test]
async fn a_standby_whose_subscription_slot_has_the_dr_prefix_refuses_to_start() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("standbyslotname");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .expect("the replicated row");
    let sub = format!("regional_replica_{db}");
    diesel::sql_query(format!(
        "CREATE SUBSCRIPTION {sub} CONNECTION 'dbname=unused' PUBLICATION harvest_dr \
         WITH (connect = false, slot_name = '{DR_PREFIX}_{db}')"
    ))
    .execute(&mut conn)
    .await
    .expect("create a disconnected subscription");

    let pool = dr_pool(&url);
    let targets = Some((vec![(ShardId::new(0), pool.clone())], ShardId::new(0)));
    let refused = pin_process_fence(DrFencing::Auto, DR_PREFIX, targets, &pool).await;
    let enabled = FenceRegistry::is_enabled();
    let _ = diesel::sql_query(format!("ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)"))
        .execute(&mut conn)
        .await;
    let _ = diesel::sql_query(format!("DROP SUBSCRIPTION {sub}"))
        .execute(&mut conn)
        .await;

    let Err(error) = refused else {
        panic!("a standby must refuse to start, whatever its subscription is named");
    };
    assert!(error.to_string().contains("standby"), "{error}");
    assert!(!enabled, "a refused start pins nothing");
}

/// A worker that cannot probe a shard at boot holds it instead of refusing
/// to start (issues #961, #1823). A held shard claims nothing until the
/// resolver releases it.
#[tokio::test]
async fn a_worker_holds_a_shard_it_cannot_probe_and_claims_nothing_there() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("holdshard");
    let unreachable = dr_pool("postgres://postgres:postgres@127.0.0.1:1/unreachable");
    let targets = Some((
        vec![(ShardId::new(0), unreachable.clone())],
        ShardId::new(0),
    ));
    let Ok((fenced, mut held)) =
        pin_worker_fence(DrFencing::Auto, DR_PREFIX, targets, &unreachable, &[]).await
    else {
        panic!("an unreachable shard is held, not refused");
    };
    assert!(fenced.is_none());
    assert_eq!(held.len(), 1);
    assert!(FenceRegistry::is_held(ShardId::new(0)));

    // The held shard claims nothing, even on a database that has work.
    let mut conn = connect(&url).await;
    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-held",
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");
    async fn claim(
        conn: &mut AsyncPgConnection,
    ) -> autumn_harvest::error::HarvestResult<Option<autumn_harvest::models::TaskQueueItem>> {
        autumn_harvest::queue::claim_task_on_shard(
            conn,
            &["q-held".to_string()],
            "w-held",
            "",
            None,
            &[],
            &[],
            Some(ShardId::new(0)),
        )
        .await
    }
    let while_held = claim(&mut conn).await.expect("claim query runs");

    // The shard comes back with no DR marker: the resolver releases it.
    let plain = dr_pool(&url);
    held[0].1 = plain;
    resolve_held(&mut held, &unique_prefix(&db))
        .await
        .expect("a plain shard is released");
    let after_release = claim(&mut conn).await.expect("claim query runs");

    assert!(while_held.is_none(), "a held shard must claim nothing");
    assert!(held.is_empty(), "the released shard leaves the held list");
    assert!(!FenceRegistry::is_held(ShardId::new(0)));
    assert!(after_release.is_some(), "a released shard claims normally");
}

/// A shard this process already pinned keeps that pin when a later worker
/// cannot probe it (issue #1823). The runner pins before its worker starts.
/// A brief outage at that moment must not hold, refuse or stop the worker.
/// The worker must probe through the pool that took the pin. Another pool
/// can reach another database, such as a logical standby.
#[tokio::test]
async fn an_unprobeable_shard_reuses_the_process_pin() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("pinreuse");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    drop(conn);
    let pool = dr_pool(&url);
    let runner_targets = Some((vec![(ShardId::new(0), pool.clone())], ShardId::new(0)));
    pin_process_fence(DrFencing::Auto, DR_PREFIX, runner_targets, &pool)
        .await
        .expect("the runner pins first");
    // The outage: the pinned database goes away.
    let admin = admin_url().await.expect("admin url");
    let mut admin_conn = connect(&admin).await;
    diesel::sql_query(format!("DROP DATABASE \"{db}\" WITH (FORCE)"))
        .execute(&mut admin_conn)
        .await
        .expect("drop the pinned database");

    let targets = Some((vec![(ShardId::new(0), pool.clone())], ShardId::new(0)));
    let Ok((fenced, held)) =
        pin_worker_fence(DrFencing::Auto, DR_PREFIX, targets, &pool, &[]).await
    else {
        panic!("a pinned shard needs no probe to start");
    };
    let fenced: Vec<ShardId> = fenced
        .expect("the pinned shard stays fenced")
        .into_iter()
        .map(|(shard, _)| shard)
        .collect();
    assert_eq!(fenced, vec![ShardId::new(0)]);
    assert!(held.is_empty(), "a pinned shard is never held");
    assert_eq!(
        FenceRegistry::expected(ShardId::new(0)),
        Some(pinned),
        "the process pin is kept"
    );

    // A worker with no shard identity resolves through the default shard.
    let Ok((fenced, held)) = pin_worker_fence(DrFencing::Auto, DR_PREFIX, None, &pool, &[]).await
    else {
        panic!("the default shard pin covers a worker with no shard identity");
    };
    assert!(fenced.is_some() && held.is_empty());

    // Another pool proves nothing about the pinned database.
    let other = dr_pool("postgres://postgres:postgres@127.0.0.1:1/unreachable");
    let targets = Some((vec![(ShardId::new(0), other.clone())], ShardId::new(0)));
    let refused = pin_worker_fence(DrFencing::Auto, DR_PREFIX, targets, &other, &[]).await;
    assert!(
        refused.is_err(),
        "an unprobed pool that did not take the pin must not reuse it"
    );
}

/// A peer row that a pin discovered reuses that pin too (issue #1823). The
/// pin was taken through this pool, so a later worker that targets the peer
/// and cannot probe it starts on the existing pin.
#[tokio::test]
async fn an_unprobeable_peer_reuses_the_discovered_pin() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("peerreuse");
    let (own, peer) = (ShardId::new(0), ShardId::new(5));
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, own).await.unwrap();
    let peer_generation = ensure_generation_row(&mut conn, peer).await.unwrap();
    drop(conn);
    let pool = dr_pool(&url);
    let runner_targets = Some((vec![(own, pool.clone())], own));
    pin_process_fence(DrFencing::Auto, DR_PREFIX, runner_targets, &pool)
        .await
        .expect("the runner pins its shard and discovers the peer");
    assert_eq!(FenceRegistry::expected(peer), Some(peer_generation));
    // The outage: the database goes away.
    let admin = admin_url().await.expect("admin url");
    let mut admin_conn = connect(&admin).await;
    diesel::sql_query(format!("DROP DATABASE \"{db}\" WITH (FORCE)"))
        .execute(&mut admin_conn)
        .await
        .expect("drop the database");

    // The process keeps its default shard. Only the target is the peer.
    let targets = Some((vec![(peer, pool.clone())], own));
    let started = pin_worker_fence(DrFencing::Auto, DR_PREFIX, targets, &pool, &[]).await;

    let fenced: Vec<ShardId> = started
        .expect("the peer pin was taken through this pool")
        .0
        .expect("the peer stays fenced")
        .into_iter()
        .map(|(shard, _)| shard)
        .collect();
    assert_eq!(fenced, vec![peer]);
}

/// A loop that skips a held shard still proves it is alive (issue #1823). A
/// held shard is a supported state, so its scanners must not read as stale.
#[tokio::test]
async fn a_held_shard_keeps_its_scanners_live() {
    #[derive(Default)]
    struct Ticks(std::sync::Mutex<Vec<String>>);
    impl autumn_harvest::telemetry::MetricsRecorder for Ticks {
        fn record_scanner_tick(&self, scanner: &str, _shard: &str) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(scanner.to_owned());
        }
    }
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("heldlive");
    let shard = ShardId::new(0);
    FenceRegistry::hold(&[shard], shard).expect("hold");
    let ticks = std::sync::Arc::new(Ticks::default());
    let telemetry = std::sync::Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: ticks.clone(),
        ..Default::default()
    });
    let cancel = tokio_util::sync::CancellationToken::new();
    let interval = std::time::Duration::from_millis(50);
    let poison = autumn_harvest::poison_pill::spawn_poison_pill_reclaimer_for_shard(
        dr_pool(&url),
        cancel.clone(),
        interval,
        3,
        60,
        None,
        std::sync::Arc::clone(&telemetry),
        Some(shard),
        autumn_harvest::payload_codec::PayloadCodecs::default(),
    );
    let export = autumn_harvest::audit_export::spawn_audit_export_checker_for_shard(
        dr_pool(&url),
        cancel.clone(),
        interval,
        telemetry,
        Some(shard),
        None,
        None,
    );

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    cancel.cancel();
    let _ = poison.await;
    let _ = export.await;

    let seen = ticks
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    for scanner in ["poison_pill", "audit_export"] {
        assert!(
            seen.iter().filter(|tick| *tick == scanner).count() >= 2,
            "{scanner} must keep ticking while its shard is held; saw {seen:?}"
        );
    }
}

/// A retention lease release checks the fence (issue #1823). A dropped tick
/// releases its leases from a detached task, after the tick's own fence is
/// gone. A process whose pin is superseded must leave the leases alone.
#[tokio::test]
async fn a_superseded_pin_releases_no_retention_lease() {
    #[derive(diesel::QueryableByName)]
    struct Lease {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        sticky_worker_id: Option<String>,
    }
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("leasefence");
    let shard = ShardId::new(0);
    let mut conn = connect(&url).await;
    let exec_id = ExecutionId::new_for_shard(shard).as_uuid();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
            (id, workflow_name, workflow_id, shard_id, state, input, sticky_worker_id) \
         VALUES ($1, 'wf', 'lease-fence', 0, 'COMPLETED', '{}'::jsonb, 'retention-lease-x')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(&mut conn)
    .await
    .expect("insert a leased execution");
    let pinned = ensure_generation_row(&mut conn, shard).await.unwrap();
    FenceRegistry::publish(&[(shard, pinned)], shard).expect("pin");
    bump_generation(&mut conn, shard, "failover", "test")
        .await
        .expect("bump");

    autumn_harvest::retention::release_retention_leases(
        dr_pool(&url),
        shard,
        "retention-lease-x".to_string(),
        vec![exec_id],
    )
    .await;

    let rows: Vec<Lease> =
        diesel::sql_query("SELECT sticky_worker_id FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id)
            .load(&mut conn)
            .await
            .expect("read the lease");
    assert_eq!(
        <[Lease]>::first(&rows).and_then(|row| row.sticky_worker_id.clone()),
        Some("retention-lease-x".to_string()),
        "a process that lost write authority must not release a lease"
    );
}

/// A loop that waits for a pooled connection holds no fence barrier (issue
/// #1823). An exhausted pool must not block a bump.
#[tokio::test]
async fn a_loop_waiting_for_a_connection_does_not_block_a_bump() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("poolwait");
    let shard = ShardId::new(0);
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, shard).await.unwrap();
    FenceRegistry::publish(&[(shard, pinned)], shard).expect("pin");

    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            &url,
        );
    let pool = deadpool::managed::Pool::builder(manager)
        .max_size(1)
        .build()
        .expect("pool build");
    // The only connection stays checked out, so the loop waits for it.
    let busy = pool.get().await.expect("check out the only connection");
    let cancel = tokio_util::sync::CancellationToken::new();
    let reclaimer = autumn_harvest::poison_pill::spawn_poison_pill_reclaimer_for_shard(
        pool.clone(),
        cancel.clone(),
        std::time::Duration::from_millis(50),
        3,
        60,
        None,
        std::sync::Arc::new(autumn_harvest::telemetry::TelemetryConfig::default()),
        Some(shard),
        autumn_harvest::payload_codec::PayloadCodecs::default(),
    );
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let bumped = bump_generation(&mut conn, shard, "failover", "test").await;
    cancel.cancel();
    drop(busy);
    let _ = reclaimer.await;
    assert!(
        bumped.is_ok(),
        "a loop parked on the pool must not hold the bump off: {bumped:?}"
    );
}

/// An activity heartbeat asserts the fence in its own transaction (issue
/// #1823). A process whose pin is superseded must not refresh a claim, even
/// before the sampler cancels the activity.
#[tokio::test]
async fn a_superseded_pin_writes_no_activity_heartbeat() {
    #[derive(diesel::QueryableByName)]
    struct Beat {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
        heartbeat_details: Option<serde_json::Value>,
    }
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("beatfence");
    let shard = ShardId::new(0);
    let mut conn = connect(&url).await;
    let task_id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
            (id, queue_name, task_type, input, state, worker_id, attempt) \
         VALUES ($1, 'q', 'activity', '{}'::jsonb, 'RUNNING', 'w-1', 1)",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("insert a claimed task");
    let pinned = ensure_generation_row(&mut conn, shard).await.unwrap();
    FenceRegistry::publish(&[(shard, pinned)], shard).expect("pin");
    bump_generation(&mut conn, shard, "failover", "test")
        .await
        .expect("bump");

    let claim = autumn_harvest::queue::TaskClaim::new(task_id, "w-1", 1);
    let written = autumn_harvest::queue::record_heartbeat(
        &mut conn,
        &claim,
        serde_json::json!({"progress": 1}),
    )
    .await;

    let rows: Vec<Beat> =
        diesel::sql_query("SELECT heartbeat_details FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .load(&mut conn)
            .await
            .expect("read the beat");
    assert_eq!(
        <[Beat]>::first(&rows).and_then(|row| row.heartbeat_details.clone()),
        None,
        "a process that lost write authority must not write a heartbeat: {written:?}"
    );
    assert!(
        matches!(
            written,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "the beat must fail as fenced: {written:?}"
    );
}

/// A batch pass that cannot check out a connection drops its fence barriers
/// (issue #1823). An exhausted pool must not block a bump.
#[tokio::test]
async fn a_batch_pass_waiting_for_a_connection_does_not_block_a_bump() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("batchwait");
    let shard = ShardId::new(0);
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, shard).await.unwrap();
    FenceRegistry::publish(&[(shard, pinned)], shard).expect("pin");

    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            &url,
        );
    let pool = deadpool::managed::Pool::builder(manager)
        .max_size(1)
        .build()
        .expect("pool build");
    // The only connection stays checked out, so the pass waits for it.
    let busy = pool.get().await.expect("check out the only connection");
    let pools = autumn_harvest::shard::ShardedDbPool::single(pool.clone());
    let pass = tokio::spawn(async move {
        autumn_harvest::batch::run_executor_once(
            &pools,
            &autumn_harvest::batch::BatchExecutorConfig::default(),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let bumped = bump_generation(&mut conn, shard, "failover", "test").await;
    drop(busy);
    pass.abort();
    let _ = pass.await;
    assert!(
        bumped.is_ok(),
        "a batch pass parked on the pool must not hold the bump off: {bumped:?}"
    );
}

/// A worker writes nothing to a held shard (issue #1823). The shard may be
/// an unpromoted logical standby. Fleet rows and rate-limit buckets wait for
/// the release, and the heartbeat then registers the worker.
#[tokio::test]
async fn a_worker_defers_its_startup_writes_on_a_held_shard() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("holdwrites");
    FenceRegistry::hold(&[ShardId::new(0)], ShardId::new(0)).expect("hold");

    let mut config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_shard_assignments([ShardId::new(0)]),
    );
    config.worker_heartbeat_interval = std::time::Duration::from_millis(200);
    let worker_id = config.worker_id.clone();
    let registry = std::sync::Arc::new(autumn_harvest::worker::HandlerRegistry::new(
        Vec::new(),
        Vec::new(),
    ));
    let worker = std::sync::Arc::new(
        autumn_harvest::worker::Worker::new(config, registry).expect("worker builds"),
    );
    let runner = std::sync::Arc::clone(&worker);
    let pool = dr_pool(&url);
    let run = tokio::spawn(async move { runner.run(&pool).await });

    let registered = |url: String, worker_id: String| async move {
        #[derive(diesel::QueryableByName)]
        struct Count {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        let mut conn = connect(&url).await;
        diesel::sql_query("SELECT count(*) AS n FROM harvest_workers WHERE worker_id = $1")
            .bind::<diesel::sql_types::Text, _>(worker_id)
            .get_result::<Count>(&mut conn)
            .await
            .expect("count workers")
            .n
            == 1
    };
    // Several heartbeat intervals pass while the shard is held.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let while_held = registered(url.clone(), worker_id.clone()).await;

    FenceRegistry::release_held(ShardId::new(0));
    eventually(
        "the worker to register after the release",
        std::time::Duration::from_secs(20),
        || registered(url.clone(), worker_id.clone()),
    )
    .await;
    worker.shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(30), run)
        .await
        .expect("worker stops")
        .expect("worker task joins");

    assert!(!while_held, "a held shard must not get a fleet row");
}

/// A fenced worker holds an unassigned shard it cannot probe (issue #1823).
/// It serves its assigned shard, and cross-shard writes to the held shard
/// fail closed. Only an assigned shard it cannot probe refuses the start.
#[tokio::test]
async fn a_fenced_worker_holds_an_unassigned_shard_it_cannot_probe() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("holdunassigned");
    {
        let mut conn = connect(&url).await;
        ensure_generation_row(&mut conn, ShardId::new(0))
            .await
            .unwrap();
    }
    let healthy = dr_pool(&url);
    let unreachable = dr_pool("postgres://postgres:postgres@127.0.0.1:1/unreachable");
    let targets = || {
        Some((
            vec![
                (ShardId::new(0), healthy.clone()),
                (ShardId::new(1), unreachable.clone()),
            ],
            ShardId::new(0),
        ))
    };

    let refused = pin_worker_fence(
        DrFencing::Auto,
        DR_PREFIX,
        targets(),
        &healthy,
        &[ShardId::new(1)],
    )
    .await;
    assert!(
        refused.is_err(),
        "an assigned shard that cannot be pinned refuses"
    );
    FenceRegistry::clear();

    let Ok((fenced, held)) = pin_worker_fence(
        DrFencing::Auto,
        DR_PREFIX,
        targets(),
        &healthy,
        &[ShardId::new(0)],
    )
    .await
    else {
        panic!("an unassigned shard must not refuse the worker");
    };
    let fenced: Vec<ShardId> = fenced
        .expect("the assigned shard is fenced")
        .into_iter()
        .map(|(shard, _)| shard)
        .collect();
    let held: Vec<ShardId> = held.into_iter().map(|(shard, _)| shard).collect();
    assert_eq!(fenced, vec![ShardId::new(0)]);
    assert_eq!(held, vec![ShardId::new(1)]);
    assert!(FenceRegistry::is_held(ShardId::new(1)));
    assert_eq!(
        FenceRegistry::expected(ShardId::new(0)),
        Some(ShardGeneration::INITIAL)
    );
}

/// A worker issues no write statement on a held shard (issue #1823). A
/// statement trigger on every Harvest table records each write attempt.
#[tokio::test]
async fn a_held_shard_gets_no_worker_write_statement() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("holdnowrite");
    {
        let mut conn = connect(&url).await;
        conn.batch_execute(
            "CREATE TABLE test_write_log (tbl text NOT NULL, op text NOT NULL);
             CREATE FUNCTION test_note_write() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
               INSERT INTO test_write_log VALUES (TG_TABLE_NAME, TG_OP);
               RETURN NULL;
             END $$;
             DO $$
             DECLARE t text;
             BEGIN
               FOR t IN
                 SELECT c.relname FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
                   AND c.relname LIKE 'harvest\\_%' AND NOT c.relispartition
               LOOP
                 EXECUTE format(
                   'CREATE TRIGGER test_note_write AFTER INSERT OR UPDATE OR DELETE ON %I \
                    FOR EACH STATEMENT EXECUTE FUNCTION test_note_write()', t);
               END LOOP;
             END $$;",
        )
        .await
        .expect("install the write log");
    }
    FenceRegistry::hold(&[ShardId::new(0)], ShardId::new(0)).expect("hold");

    let mut config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_shard_assignments([ShardId::new(0)]),
    );
    config.worker_heartbeat_interval = std::time::Duration::from_millis(200);
    config.poll_interval = std::time::Duration::from_millis(100);
    let registry = std::sync::Arc::new(autumn_harvest::worker::HandlerRegistry::new(
        Vec::new(),
        Vec::new(),
    ));
    let worker = std::sync::Arc::new(
        autumn_harvest::worker::Worker::new(config, registry).expect("worker builds"),
    );
    let runner = std::sync::Arc::clone(&worker);
    let pool = dr_pool(&url);
    let run = tokio::spawn(async move { runner.run(&pool).await });

    let writes = |url: String| async move {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = diesel::sql_types::Text)]
            tbl: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            op: String,
        }
        let mut conn = connect(&url).await;
        diesel::sql_query("SELECT tbl, op FROM test_write_log ORDER BY tbl, op")
            .load::<Row>(&mut conn)
            .await
            .expect("read the write log")
            .into_iter()
            .map(|row| format!("{} {}", row.op, row.tbl))
            .collect::<Vec<_>>()
    };
    // Many poll, heartbeat and monitor ticks pass while the shard is held.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let while_held = writes(url.clone()).await;

    FenceRegistry::release_held(ShardId::new(0));
    eventually(
        "a write after the release",
        std::time::Duration::from_secs(20),
        || {
            let url = url.clone();
            async move { !writes(url).await.is_empty() }
        },
    )
    .await;
    worker.shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(30), run)
        .await
        .expect("worker stops")
        .expect("worker task joins");

    assert!(
        while_held.is_empty(),
        "a held shard must get no write statement: {while_held:?}"
    );
}

/// A worker whose logical shards share one database pins every one of them
/// (issue #1823). Fencing shard 1 must stop its shard-1 claims too.
#[tokio::test]
async fn a_worker_pins_every_colocated_shard() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("colocated");
    {
        let mut conn = connect(&url).await;
        ensure_generation_row(&mut conn, ShardId::new(0))
            .await
            .unwrap();
    }
    let assigned = [ShardId::new(0), ShardId::new(1)];
    let config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_shard_assignments(assigned),
    );
    let pool = dr_pool(&url);
    let targets = autumn_harvest::worker::dr_fence_targets(&config, &pool);
    let Ok((fenced, held)) =
        pin_worker_fence(DrFencing::Auto, DR_PREFIX, targets, &pool, &assigned).await
    else {
        panic!("colocated shards on one database must start");
    };
    assert!(held.is_empty());
    assert_eq!(fenced.map(|targets| targets.len()), Some(2));
    assert_eq!(
        FenceRegistry::expected(ShardId::new(0)),
        Some(ShardGeneration::INITIAL)
    );
    assert_eq!(
        FenceRegistry::expected(ShardId::new(1)),
        Some(ShardGeneration::INITIAL),
        "every colocated shard is pinned"
    );
}

/// A fenced scheduler writes nothing (issue #1823). Its schedule-table writes
/// do not pass the persist assert, so each shard pass checks the fence first.
#[tokio::test]
async fn a_fenced_scheduler_writes_nothing() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("fencedsched");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    FenceRegistry::publish(&[(ShardId::new(0), pinned)], ShardId::new(0)).expect("pin");
    bump_generation(&mut conn, ShardId::new(0), "failover", "test")
        .await
        .unwrap();
    conn.batch_execute(
        "INSERT INTO harvest_schedules (id, workflow_name, schedule_expr, timezone, catchup, \
           max_active_runs, is_paused, next_run_at, jitter_secs, overlap_policy, \
           buffered_runs, buffer_all_max, skip_policy) \
         VALUES (gen_random_uuid(), 'dr_fenced_wf', 'interval:60', 'UTC', false, 10, false, \
           now() - interval '5 seconds', 0, 'skip', '[]', 100, 'skip');
         CREATE TABLE test_write_log (tbl text NOT NULL, op text NOT NULL);
         CREATE FUNCTION test_note_write() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO test_write_log VALUES (TG_TABLE_NAME, TG_OP);
           RETURN NULL;
         END $$;
         CREATE TRIGGER test_note_write AFTER INSERT OR UPDATE OR DELETE ON harvest_schedules
           FOR EACH STATEMENT EXECUTE FUNCTION test_note_write();
         CREATE TRIGGER test_note_write AFTER INSERT OR UPDATE OR DELETE
           ON harvest_workflow_executions
           FOR EACH STATEMENT EXECUTE FUNCTION test_note_write();",
    )
    .await
    .expect("seed a due schedule and the write log");

    let registry = std::sync::Arc::new(autumn_harvest::worker::HandlerRegistry::new(
        Vec::new(),
        Vec::new(),
    ));
    let _ = autumn_harvest::tick_once(
        dr_pool(&url),
        registry,
        std::sync::Arc::new(autumn_harvest::DagCatalog::default()),
        std::sync::Arc::new(Vec::new()),
        autumn_harvest::SchedulerMonitor::offline(),
    )
    .await;

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let writes = diesel::sql_query("SELECT count(*) AS n FROM test_write_log")
        .get_result::<Count>(&mut conn)
        .await
        .expect("read the write log")
        .n;
    assert_eq!(writes, 0, "a fenced scheduler must not write");
}

/// Workers may split the logical shards of one database (issue #1823). A
/// second worker finds the first worker's row there and still starts.
#[tokio::test]
async fn split_workers_on_one_database_both_start() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("splitworkers");
    {
        let mut conn = connect(&url).await;
        ensure_generation_row(&mut conn, ShardId::new(0))
            .await
            .unwrap();
    }
    let assigned = [ShardId::new(1)];
    let config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_shard_assignments(assigned),
    );
    let pool = dr_pool(&url);
    let targets = autumn_harvest::worker::dr_fence_targets(&config, &pool);
    let started = pin_worker_fence(DrFencing::Auto, DR_PREFIX, targets, &pool, &assigned).await;
    let Ok((fenced, held)) = started else {
        panic!("a worker for another logical shard on this database must start");
    };
    assert!(held.is_empty());
    assert!(fenced.is_some());
    assert_eq!(
        FenceRegistry::expected(ShardId::new(1)),
        Some(ShardGeneration::INITIAL)
    );
}

/// A fenced pass outlives an idle-in-transaction timeout (issue #1823). The
/// guard runs no query while the pass works. A server timeout must not end
/// its transaction and free the lock mid-pass.
#[tokio::test]
async fn a_fenced_pass_outlives_an_idle_transaction_timeout() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("passidle");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    diesel::sql_query(format!(
        "ALTER DATABASE \"{db}\" SET idle_in_transaction_session_timeout = '300ms'"
    ))
    .execute(&mut conn)
    .await
    .expect("set the idle timeout");
    FenceRegistry::publish(&[(ShardId::new(0), pinned)], ShardId::new(0)).expect("pin");
    let pool = dr_pool(&url);
    let guard = autumn_harvest::replication::begin_fenced_pass(&pool, ShardId::new(0))
        .await
        .expect("open the pass")
        .expect("a pinned shard gets a guard");
    // Well past the timeout, as a long pass would be.
    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;

    let bump_url = url.clone();
    let bump = tokio::spawn(async move {
        let mut conn = connect(&bump_url).await;
        bump_generation(&mut conn, ShardId::new(0), "failover", "test").await
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let waited = !bump.is_finished();
    drop(guard);
    let _ = bump.await;

    assert!(waited, "the idle timeout must not free the pass lock");
}

/// A runner given one plain pool wraps it as shard 0 (issue #1823). Its
/// configured logical shard must still be the one it pins.
#[tokio::test]
async fn a_single_pool_wrapper_pins_the_configured_shard() {
    let (url, _db) = require_db!("singlewrap");
    let pool = dr_pool(&url);
    let mut config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_shard_assignments([ShardId::new(1)]),
    );
    config.sharded_pool = Some(autumn_harvest::shard::ShardedDbPool::single(pool.clone()));
    let (targets, default_shard) = autumn_harvest::worker::dr_fence_targets(&config, &pool)
        .expect("a configured shard gives targets");
    let shards: Vec<ShardId> = targets.into_iter().map(|(shard, _)| shard).collect();
    assert_eq!(shards, vec![ShardId::new(1)]);
    assert_eq!(default_shard, ShardId::new(1));
}

/// A fence guard notices when its session ends (issue #1823). The server
/// then frees the pass lock, so the pass must stop writing.
#[tokio::test]
async fn a_fence_guard_reports_a_lost_session() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("guardlost");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    FenceRegistry::publish(&[(ShardId::new(0), pinned)], ShardId::new(0)).expect("pin");
    let pool = dr_pool(&url);
    let guard = autumn_harvest::replication::begin_fenced_pass(&pool, ShardId::new(0))
        .await
        .expect("open the pass")
        .expect("a pinned shard gets a guard");
    assert!(!guard.is_lost(), "a fresh guard holds its lock");

    diesel::sql_query(format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = '{db}' AND application_name = 'harvest_dr_fence_pass'"
    ))
    .execute(&mut conn)
    .await
    .expect("terminate the guard backend");
    eventually(
        "the guard to report its lost session",
        std::time::Duration::from_secs(10),
        || async { guard.is_lost() },
    )
    .await;
}

/// A pass stops when its fence guard loses its session (issue #1823). The
/// server then frees the pass lock, so a bump can commit mid-pass.
#[tokio::test]
async fn a_pass_stops_when_its_fence_guard_is_lost() {
    let (url, db) = require_db!("passlost");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    let pool = dr_pool(&url);
    let guard = autumn_harvest::replication::begin_fenced_pass_at(&pool, ShardId::new(0), pinned)
        .await
        .expect("open the pass");
    let pass = autumn_harvest::replication::run_fenced_pass(
        Some(&guard),
        tokio::time::sleep(std::time::Duration::from_secs(60)),
    );
    let terminate = async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        diesel::sql_query(format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = '{db}' AND application_name = 'harvest_dr_fence_pass'"
        ))
        .execute(&mut conn)
        .await
        .expect("terminate the guard backend");
    };
    let (stopped, ()) = tokio::join!(
        tokio::time::timeout(std::time::Duration::from_secs(10), pass),
        terminate
    );

    let Ok(outcome) = stopped else {
        panic!("a pass must stop when its guard is lost");
    };
    assert!(outcome.is_err(), "a stopped pass reports an error");
}

/// A bump waits out a write that a lost pass already sent (issue #1823).
/// The pass sees the loss within one keepalive interval and stops. A short
/// statement it sent before then must commit before the bump.
#[tokio::test]
async fn a_bump_waits_out_a_write_a_lost_pass_already_sent() {
    let (url, db) = require_db!("passstraggler");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    diesel::sql_query("CREATE TABLE dr_straggler (id int PRIMARY KEY, written bool NOT NULL)")
        .execute(&mut conn)
        .await
        .expect("create the probe table");
    diesel::sql_query("INSERT INTO dr_straggler VALUES (1, false)")
        .execute(&mut conn)
        .await
        .expect("seed the probe row");
    #[derive(diesel::QueryableByName)]
    struct Probe {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        written: bool,
    }
    let pool = dr_pool(&url);
    let guard = autumn_harvest::replication::begin_fenced_pass_at(&pool, ShardId::new(0), pinned)
        .await
        .expect("open the pass");
    // The pass sends a write. The server still runs it when the guard ends.
    let writer_url = url.clone();
    let write = tokio::spawn(async move {
        let mut writer = connect(&writer_url).await;
        diesel::sql_query("UPDATE dr_straggler SET written = true WHERE pg_sleep(0.8) IS NOT NULL")
            .execute(&mut writer)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    diesel::sql_query(format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = '{db}' AND application_name = 'harvest_dr_fence_pass'"
    ))
    .execute(&mut conn)
    .await
    .expect("terminate the guard backend");

    bump_generation(&mut conn, ShardId::new(0), "failover", "test")
        .await
        .expect("bump");
    let written: Vec<Probe> = diesel::sql_query("SELECT written FROM dr_straggler")
        .load(&mut conn)
        .await
        .expect("read the probe row");
    write.await.expect("join").expect("the write commits");
    drop(guard);

    assert!(
        <[Probe]>::first(&written).is_some_and(|row| row.written),
        "a write sent before the loss must commit before the bump"
    );
}

/// A fence stops an activity heartbeat flusher (issue #1823). The flusher
/// outlives a drain, so the worker token does not reach it. Without this,
/// it keeps writing `last_heartbeat_at` after another region owns the row.
#[tokio::test]
async fn a_fence_stops_an_activity_heartbeat_flusher() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("hbfence");
    FenceRegistry::publish(
        &[(ShardId::new(0), ShardGeneration::INITIAL)],
        ShardId::new(0),
    )
    .expect("pin");
    let stop = tokio_util::sync::CancellationToken::new();
    let _slot = autumn_harvest::heartbeat::spawn_heartbeat_flusher_with(
        autumn_harvest::queue::TaskClaim::new(uuid::Uuid::new_v4(), "w-1", 1),
        dr_pool(&url),
        stop.clone(),
        autumn_harvest::heartbeat::HeartbeatFlushOptions {
            acquire_timeout: std::time::Duration::from_secs(1),
            metrics: std::sync::Arc::new(autumn_harvest::telemetry::NoOpMetrics),
        },
    );

    FenceRegistry::mark_fenced_out();

    assert!(
        stop.is_cancelled(),
        "a fence must stop the heartbeat flusher"
    );
}

/// A replication heartbeat checks the fence in its own transaction (issue
/// #1823). A process whose pin is superseded writes no beat, also when the
/// bump lands after the sampler's own fence check.
#[tokio::test]
async fn a_superseded_pin_writes_no_replication_heartbeat() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("beatfence");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    FenceRegistry::publish(&[(ShardId::new(0), pinned)], ShardId::new(0)).expect("pin");
    bump_generation(&mut conn, ShardId::new(0), "failover", "test")
        .await
        .expect("bump");

    let beat = autumn_harvest::replication::record_replication_heartbeat(
        &mut conn,
        ShardId::new(0),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(10),
    )
    .await;
    #[derive(diesel::QueryableByName)]
    struct Beats {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        beats: i64,
    }
    let rows: Vec<Beats> =
        diesel::sql_query("SELECT count(*) AS beats FROM harvest_replication_heartbeat")
            .load(&mut conn)
            .await
            .expect("count the beats");

    assert!(
        matches!(
            beat,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "a superseded pin must not write a beat: {beat:?}"
    );
    assert_eq!(<[Beats]>::first(&rows).map(|row| row.beats), Some(0));
}

/// A pass on one shard does not block a bump of another shard on the same
/// database (issue #1823). Generations are per shard, so the barrier is too.
#[tokio::test]
async fn a_fenced_pass_does_not_block_another_shards_bump() {
    let (url, _db) = require_db!("passscope");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(1))
        .await
        .unwrap();
    let other = ensure_generation_row(&mut conn, ShardId::new(2))
        .await
        .unwrap();
    let pool = dr_pool(&url);
    let guard = autumn_harvest::replication::begin_fenced_pass_at(&pool, ShardId::new(2), other)
        .await
        .expect("open the pass on shard 2");

    let bump = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        bump_generation(&mut conn, ShardId::new(1), "failover", "test"),
    )
    .await;
    drop(guard);

    assert!(
        matches!(bump, Ok(Ok(_))),
        "a pass on shard 2 must not block a bump of shard 1: {bump:?}"
    );
}

/// A background tick holds a barrier on each pinned shard, and a fenced
/// process gets no tick (issue #1823). Retention and the batch executor use
/// this.
#[tokio::test]
async fn a_background_tick_holds_the_fence_and_refuses_when_fenced() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("tickfence");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    let pools = autumn_harvest::shard::ShardedDbPool::single(dr_pool(&url));
    let unpinned = autumn_harvest::replication::begin_fenced_tick(&pools)
        .await
        .expect("an unpinned process ticks");
    assert!(unpinned.is_empty(), "no pin, no barrier");

    FenceRegistry::publish(&[(ShardId::new(0), pinned)], ShardId::new(0)).expect("pin");
    let guards = autumn_harvest::replication::begin_fenced_tick(&pools)
        .await
        .expect("a pinned process ticks");
    assert_eq!(guards.len(), 1, "one barrier per pinned shard");
    let bump_url = url.clone();
    let bump = tokio::spawn(async move {
        let mut conn = connect(&bump_url).await;
        bump_generation(&mut conn, ShardId::new(0), "failover", "test").await
    });
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    let waited = !bump.is_finished();
    drop(guards);
    bump.await
        .expect("bump task")
        .expect("the bump commits after the tick");
    assert!(waited, "a bump waits for the tick");

    let refused = autumn_harvest::replication::begin_fenced_tick(&pools).await;
    assert!(
        matches!(
            refused,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "a fenced process gets no tick"
    );
}

/// A worker on a plain database refuses to start in a process that already
/// pins DR shards (issue #1823). The registry is process-wide, so the worker
/// would check its writes against another database's pins.
#[tokio::test]
async fn an_unfenced_worker_refuses_to_join_a_pinned_process() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("mixedworker");
    FenceRegistry::publish(
        &[(ShardId::new(5), ShardGeneration::new(2))],
        ShardId::new(5),
    )
    .expect("another worker pins first");
    let pool = dr_pool(&url);
    let targets = Some((vec![(ShardId::new(0), pool.clone())], ShardId::new(0)));
    let refused = pin_worker_fence(DrFencing::Auto, DR_PREFIX, targets, &pool, &[]).await;
    assert!(
        refused.is_err(),
        "an unfenced worker must not share a pinned process"
    );
}

/// A claim on a database shared by several logical shards checks every one
/// of their pins (issue #1823). The claim scan is not filtered by shard, so
/// a bump of any colocated shard must stop it.
#[tokio::test]
async fn a_colocated_claim_stops_when_any_colocated_shard_is_bumped() {
    use autumn_harvest::queue::claim_task_on_shard;

    let _serial = registry_guard().await;
    let (url, _db) = require_db!("colocclaim");
    let pool = dr_pool(&url);
    let config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default()
            .with_shard_assignments([ShardId::new(0), ShardId::new(1)]),
    );
    let targets = autumn_harvest::worker::dr_fence_targets(&config, &pool);
    let assigned = [ShardId::new(0), ShardId::new(1)];
    pin_worker_fence(DrFencing::Enabled, DR_PREFIX, targets, &pool, &assigned)
        .await
        .expect("both colocated shards pin");
    let mut conn = connect(&url).await;
    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr-coloc",
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");

    bump_generation(&mut conn, ShardId::new(1), "promote", "oncall")
        .await
        .unwrap();
    let queues = ["q-dr-coloc".to_string()];
    let claimed = claim_task_on_shard(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("the claim query itself still succeeds");
    assert!(
        claimed.is_none(),
        "a bump of a colocated shard must stop the claim"
    );
}

/// A failed publish releases the holds that the same startup added (issue
/// #1823). Otherwise the sentinel stays in the process-wide registry, and
/// other workers treat the shard as unwritable forever.
#[tokio::test]
async fn a_failed_publish_releases_its_holds() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("holdleak");
    let mut conn = connect(&url).await;
    let current = ensure_generation_row(&mut conn, ShardId::new(1))
        .await
        .unwrap();
    FenceRegistry::publish(
        &[(ShardId::new(1), ShardGeneration::new(current.as_i64() + 5))],
        ShardId::new(1),
    )
    .expect("an older worker pinned shard 1 at another generation");
    let unreachable = dr_pool("postgres://postgres:postgres@127.0.0.1:1/unreachable");
    let reachable = dr_pool(&url);
    let targets = Some((
        vec![
            (ShardId::new(0), unreachable),
            (ShardId::new(1), reachable.clone()),
        ],
        ShardId::new(1),
    ));
    let refused = pin_worker_fence(
        DrFencing::Auto,
        DR_PREFIX,
        targets,
        &reachable,
        &[ShardId::new(1)],
    )
    .await;

    assert!(refused.is_err(), "a pin conflict refuses the worker");
    assert!(
        !FenceRegistry::is_held(ShardId::new(0)),
        "a refused startup must not leave its hold behind"
    );
}

/// A background tick on a database that several logical shards share holds
/// a barrier for each of them (issue #1823). The tick's work is not filtered
/// by shard, so a bump of any of them must wait for it.
#[tokio::test]
async fn a_background_tick_guards_every_colocated_shard() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("tickcoloc");
    let pool = dr_pool(&url);
    let config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default()
            .with_shard_assignments([ShardId::new(0), ShardId::new(1)]),
    );
    let targets = autumn_harvest::worker::dr_fence_targets(&config, &pool);
    let assigned = [ShardId::new(0), ShardId::new(1)];
    pin_worker_fence(DrFencing::Enabled, DR_PREFIX, targets, &pool, &assigned)
        .await
        .expect("both colocated shards pin");

    let pools = autumn_harvest::shard::ShardedDbPool::single(pool.clone());
    let guards = autumn_harvest::replication::begin_fenced_tick(&pools)
        .await
        .expect("a pinned process ticks");
    let bump_url = url.clone();
    let bump = tokio::spawn(async move {
        let mut conn = connect(&bump_url).await;
        bump_generation(&mut conn, ShardId::new(1), "failover", "test").await
    });
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    let waited = !bump.is_finished();
    let count = guards.len();
    drop(guards);
    let _ = bump.await;

    assert_eq!(count, 1, "one guard holds both colocated shards");
    assert!(waited, "a bump of a colocated shard waits for the tick");

    // A scheduler pass on that database takes the same group.
    let group = autumn_harvest::replication::begin_fenced_group(&pool, ShardId::UNENCODED).await;
    assert!(
        matches!(
            group,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "the bumped colocated shard fences the scheduler pass too"
    );
}

/// A process that shares its database with another process's logical shard
/// stops claiming when that shard is fenced (issue #1823). Its claim scan
/// is not filtered by shard, so it reaches the other shard's rows too.
#[tokio::test]
async fn a_split_database_claim_stops_when_the_peer_shard_is_bumped() {
    use autumn_harvest::queue::claim_task_on_shard;

    let _serial = registry_guard().await;
    let (url, _db) = require_db!("splitpeer");
    let mut conn = connect(&url).await;
    // Another process serves logical shard 1 on this database.
    ensure_generation_row(&mut conn, ShardId::new(1))
        .await
        .unwrap();
    let pool = dr_pool(&url);
    let config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_shard_assignments([ShardId::new(0)]),
    );
    let targets = autumn_harvest::worker::dr_fence_targets(&config, &pool);
    pin_worker_fence(
        DrFencing::Auto,
        DR_PREFIX,
        targets,
        &pool,
        &[ShardId::new(0)],
    )
    .await
    .expect("a split database is supported");
    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr-split",
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");

    bump_generation(&mut conn, ShardId::new(1), "promote", "oncall")
        .await
        .unwrap();
    let queues = ["q-dr-split".to_string()];
    let claimed = claim_task_on_shard(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("the claim query itself still succeeds");
    assert!(
        claimed.is_none(),
        "a bump of the peer shard on this database must stop the claim"
    );
}

/// A generation row that appears after startup stops this process's claims
/// and background ticks (issue #1823). The process did not pin it, so it
/// cannot tell whether that shard was fenced. A restart pins it.
#[tokio::test]
async fn a_late_peer_row_fails_claims_and_ticks_closed() {
    use autumn_harvest::queue::claim_task_on_shard;

    let _serial = registry_guard().await;
    let (url, _db) = require_db!("latepeer");
    let pool = dr_pool(&url);
    let config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_shard_assignments([ShardId::new(0)]),
    );
    let targets = autumn_harvest::worker::dr_fence_targets(&config, &pool);
    pin_worker_fence(
        DrFencing::Enabled,
        DR_PREFIX,
        targets,
        &pool,
        &[ShardId::new(0)],
    )
    .await
    .expect("shard 0 pins");
    let mut conn = connect(&url).await;
    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr-late",
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");

    // Another process for shard 1 starts later on the same database.
    ensure_generation_row(&mut conn, ShardId::new(1))
        .await
        .unwrap();
    let queues = ["q-dr-late".to_string()];
    let claimed = claim_task_on_shard(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("the claim query itself still succeeds");
    let tick = autumn_harvest::replication::begin_fenced_group(&pool, ShardId::new(0)).await;

    assert!(claimed.is_none(), "an unpinned row must stop the claim");
    assert!(tick.is_err(), "an unpinned row must stop a background tick");
}

/// A fenced worker refuses to join a process that already runs an unfenced
/// worker (issue #1823). Its pins are process-wide, so the unfenced worker
/// would check its writes against another database's generation.
#[tokio::test]
async fn a_fenced_worker_refuses_to_join_an_unfenced_process() {
    let _serial = registry_guard().await;
    let (plain_url, _plain) = require_db!("unfencedfirst");
    let plain = dr_pool(&plain_url);
    let plain_targets = Some((vec![(ShardId::new(0), plain.clone())], ShardId::new(0)));
    let (fenced, _) = pin_worker_fence(DrFencing::Auto, DR_PREFIX, plain_targets, &plain, &[])
        .await
        .expect("a plain database runs unfenced");
    assert!(fenced.is_none());

    let (dr_url, _dr) = require_db!("fencedsecond");
    let dr = dr_pool(&dr_url);
    let dr_targets = Some((vec![(ShardId::new(0), dr.clone())], ShardId::new(0)));
    let refused = pin_worker_fence(DrFencing::Enabled, DR_PREFIX, dr_targets, &dr, &[]).await;
    assert!(
        refused.is_err(),
        "a fenced worker must not pin shards an unfenced worker already uses"
    );
    assert!(!FenceRegistry::is_enabled(), "nothing is published");
}

/// Two DSN aliases of one database are one database to the fence (issue
/// #1823). `from_dsns` builds a pool per alias, but groups them as one
/// physical pool. The worker must start twice, and a bump of either shard
/// must stop a claim on the other.
#[tokio::test]
async fn dsn_aliases_of_one_database_are_colocated() {
    use autumn_harvest::queue::claim_task_on_shard;

    let _serial = registry_guard().await;
    let (url, _db) = require_db!("dsnalias");
    let sharded = autumn_harvest::shard::ShardedDbPool::from_dsns(
        [
            (ShardId::new(0), url.clone()),
            (ShardId::new(1), url.clone()),
        ],
        ShardId::new(0),
        4,
    )
    .expect("two aliases of one database");
    let mut config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default(),
    );
    config.sharded_pool = Some(sharded);
    let fallback = dr_pool(&url);
    for start in 0..2 {
        FenceRegistry::clear();
        let targets = autumn_harvest::worker::dr_fence_targets(&config, &fallback);
        let pinned = pin_worker_fence(DrFencing::Enabled, DR_PREFIX, targets, &fallback, &[]).await;
        assert!(pinned.is_ok(), "start {start} must pin: {:?}", pinned.err());
    }

    let mut conn = connect(&url).await;
    let params = autumn_harvest::queue::EnqueueParams::new(
        "q-dr-alias",
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");
    let queues = ["q-dr-alias".to_string()];
    let before = claim_task_on_shard(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("claim");
    assert!(before.is_some(), "the current epoch claims normally");
    diesel::sql_query("UPDATE harvest_task_queue SET state = 'PENDING', worker_id = NULL")
        .execute(&mut conn)
        .await
        .unwrap();
    bump_generation(&mut conn, ShardId::new(1), "promote", "oncall")
        .await
        .unwrap();
    let claimed = claim_task_on_shard(
        &mut conn,
        &queues,
        "w-dr",
        "",
        None,
        &[],
        &[],
        Some(ShardId::new(0)),
    )
    .await
    .expect("the claim query itself still succeeds");
    assert!(
        claimed.is_none(),
        "a bump of an aliased shard must stop the claim"
    );
}

/// A direct-database command freezes the set of generation rows while it
/// runs (issue #1823). A shard provisioned mid-command would otherwise have
/// no barrier.
#[tokio::test]
async fn a_row_freeze_blocks_new_rows_and_refuses_unguarded_ones() {
    use autumn_harvest::replication::freeze_generation_rows_on;

    let (url, _db) = require_db!("rowfreeze");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    // An empty table is frozen too: the first row must wait.
    let (empty_url, _empty) = require_db!("rowfreezeempty");
    let mut empty_conn = connect(&empty_url).await;
    let freeze = freeze_generation_rows_on(connect(&empty_url).await, &[])
        .await
        .expect("an empty table freezes");
    let first = tokio::time::timeout(
        std::time::Duration::from_millis(700),
        ensure_generation_row(&mut empty_conn, ShardId::new(0)),
    )
    .await;
    assert!(first.is_err(), "the first row waits for the freeze");
    drop(freeze);

    let freeze = freeze_generation_rows_on(connect(&url).await, &[ShardId::new(0)])
        .await
        .expect("every row is guarded");
    let provision = tokio::time::timeout(
        std::time::Duration::from_millis(700),
        ensure_generation_row(&mut conn, ShardId::new(1)),
    )
    .await;
    assert!(provision.is_err(), "a new row waits for the freeze");
    drop(freeze);
    drop(conn);

    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(1))
        .await
        .expect("the row lands once the freeze drops");
    let refused = freeze_generation_rows_on(connect(&url).await, &[ShardId::new(0)]).await;
    assert!(
        matches!(refused, Err(autumn_harvest::error::HarvestError::Config(_))),
        "an unguarded row is refused"
    );
}

/// A new shard row waits for every fenced pass already running on its
/// database (issue #1823). The running pass guards only the rows it saw, so
/// a row provisioned mid-pass would have no barrier.
#[tokio::test]
async fn provisioning_a_row_waits_for_a_running_pass() {
    let (url, _db) = require_db!("provisionwait");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    let pool = dr_pool(&url);
    let guard = autumn_harvest::replication::begin_fenced_pass_at(&pool, ShardId::new(0), pinned)
        .await
        .expect("open the pass");
    let provision = tokio::time::timeout(
        std::time::Duration::from_millis(700),
        ensure_generation_row(&mut conn, ShardId::new(1)),
    )
    .await;
    assert!(provision.is_err(), "a new row waits for the running pass");
    drop(guard);
    drop(conn);

    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(1))
        .await
        .expect("the row lands once the pass ends");
    // An existing row never waits: a restart re-provisions every time.
    let guard = autumn_harvest::replication::begin_fenced_pass_at(&pool, ShardId::new(0), pinned)
        .await
        .expect("open the pass");
    let again = tokio::time::timeout(
        std::time::Duration::from_millis(700),
        ensure_generation_row(&mut conn, ShardId::new(1)),
    )
    .await;
    drop(guard);
    assert!(matches!(again, Ok(Ok(_))), "an existing row does not wait");
}

/// A group larger than the guard cap still gets a guard (issue #1823). One
/// connection holds the barrier of every shard in the group, so the group
/// takes one slot, not one per shard.
#[tokio::test]
async fn a_group_larger_than_the_guard_cap_gets_a_guard() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("biggroup");
    let pool = dr_pool(&url);
    let assigned: Vec<ShardId> = (0..70).map(ShardId::new).collect();
    let config = autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default().with_shard_assignments(assigned.clone()),
    );
    let targets = autumn_harvest::worker::dr_fence_targets(&config, &pool);
    pin_worker_fence(DrFencing::Enabled, DR_PREFIX, targets, &pool, &assigned)
        .await
        .expect("70 colocated shards pin");

    let group = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        autumn_harvest::replication::begin_fenced_group(&pool, ShardId::UNENCODED),
    )
    .await
    .expect("the group must not wait for 70 slots")
    .expect("the group gets a guard");
    assert_eq!(group.len(), 1, "one guard for the whole group");
    let pools = autumn_harvest::shard::ShardedDbPool::single(pool.clone());
    let tick = autumn_harvest::replication::begin_fenced_tick(&pools)
        .await
        .expect("a tick over 70 shards gets a guard");
    assert_eq!(tick.len(), 1);
}

/// A tick takes one guard slot per database, all at once (issue #1823). A
/// guard opens its own connection, so a slot per guard bounds connections.
/// Taking them all at once means no tick holds some slots while it waits.
#[tokio::test]
async fn a_tick_takes_a_guard_slot_per_database_at_once() {
    let _serial = registry_guard().await;
    let (first_url, _first) = require_db!("slotone");
    let (second_url, _second) = require_db!("slottwo");
    let (first, second) = (ShardId::new(0), ShardId::new(1));
    let first_pool = dr_pool(&first_url);
    let second_pool = dr_pool(&second_url);
    let mut pins = Vec::new();
    for (shard, url) in [(first, &first_url), (second, &second_url)] {
        let mut conn = connect(url).await;
        pins.push((
            shard,
            ensure_generation_row(&mut conn, shard).await.unwrap(),
        ));
    }
    FenceRegistry::publish(&pins, first).expect("pin");
    // Every slot but one is busy.
    let mut busy = Vec::new();
    for _ in 1..autumn_harvest::replication::FENCE_GUARD_LIMIT {
        busy.push(
            autumn_harvest::replication::begin_fenced_group(&first_pool, first)
                .await
                .expect("hold a slot"),
        );
    }
    let pools = autumn_harvest::shard::ShardedDbPool::from_map(
        [(first, first_pool.clone()), (second, second_pool.clone())]
            .into_iter()
            .collect(),
        first,
    );
    let groups = [(first_pool.clone(), first), (second_pool.clone(), second)];

    let tick = tokio::spawn(async move {
        guard_count(
            tokio::time::timeout(
                std::time::Duration::from_secs(8),
                autumn_harvest::replication::begin_fenced_tick(&pools),
            )
            .await,
        )
    });
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let waited = !tick.is_finished();
    // One more free slot lets the tick take both of its slots.
    busy.pop();
    let tick = tick.await.expect("the tick task");
    // A cross-shard relay fences its source and its target the same way.
    let relay = guard_count(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            autumn_harvest::replication::begin_fenced_groups(&[
                (&groups[0].0, groups[0].1),
                (&groups[1].0, groups[1].1),
            ]),
        )
        .await,
    );
    drop(busy);

    assert!(
        waited,
        "a tick over two databases must wait for two free slots"
    );
    assert_eq!(tick, Ok(2), "the tick gets a guard on each database");
    assert_eq!(relay, Ok(2), "a relay gets a guard on each database");
}

/// An operation that guards more databases than the cap takes every slot
/// and runs alone (issue #1823). It must not wait for slots that cannot
/// exist.
#[tokio::test]
async fn a_tick_over_more_databases_than_the_cap_runs_alone() {
    let _serial = registry_guard().await;
    let (base_url, base_db) = require_db!("slotwide");
    let count = autumn_harvest::replication::FENCE_GUARD_LIMIT + 1;
    // One database per shard, cloned from a migrated one.
    let admin = admin_url().await.expect("admin url");
    let mut admin_conn = connect(&admin).await;
    let mut urls = vec![base_url];
    for index in 1..count {
        let db = format!("{base_db}_{index}");
        diesel::sql_query(format!("CREATE DATABASE {db} TEMPLATE {base_db}"))
            .execute(&mut admin_conn)
            .await
            .expect("clone the database");
        urls.push(with_db_name(&admin, &db));
    }
    drop(admin_conn);
    let mut pins = Vec::new();
    let mut pools = std::collections::BTreeMap::new();
    for (index, url) in urls.iter().enumerate() {
        let shard = ShardId::new(i32::try_from(index).unwrap());
        let mut conn = connect(url).await;
        pins.push((
            shard,
            ensure_generation_row(&mut conn, shard).await.unwrap(),
        ));
        pools.insert(shard, dr_pool(url));
    }
    FenceRegistry::publish(&pins, ShardId::new(0)).expect("pin");
    let pools = autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0));

    let tick = guard_count(
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            autumn_harvest::replication::begin_fenced_tick(&pools),
        )
        .await,
    );

    assert_eq!(tick, Ok(count), "the tick takes every slot and runs");
}

/// How many guards an attempt opened, or why it opened none. The guards drop
/// here, so the next attempt can take their slot.
fn guard_count(
    attempt: Result<
        autumn_harvest::error::HarvestResult<Vec<autumn_harvest::replication::FencePassGuard>>,
        tokio::time::error::Elapsed,
    >,
) -> Result<usize, String> {
    match attempt {
        Ok(Ok(guards)) => Ok(guards.len()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("timed out waiting for a guard slot".to_string()),
    }
}

/// A held shard that turns out to carry a DR marker stops the worker. A pin
/// is fixed for the life of a process, so it restarts and pins at startup.
#[tokio::test]
async fn a_held_shard_with_a_dr_marker_stops_the_worker() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("holddr");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    FenceRegistry::hold(&[ShardId::new(0)], ShardId::new(0)).expect("hold");
    let mut held = vec![(ShardId::new(0), dr_pool(&url))];
    let Err(error) = resolve_held(&mut held, DR_PREFIX).await else {
        panic!("a DR shard found after startup must stop the worker");
    };
    assert!(error.to_string().contains("Restart"), "{error}");
    assert!(
        FenceRegistry::is_held(ShardId::new(0)),
        "the shard stays held until the process stops"
    );
}

/// A database without the fence table probes as plain. Before issue #1823 an
/// unfenced process issued no DR query, so it must not fail now.
#[tokio::test]
async fn the_probe_tolerates_a_database_without_the_fence_table() {
    let (url, db) = require_db!("probenotable");
    let mut conn = connect(&url).await;
    diesel::sql_query("DROP TABLE harvest_shard_generation CASCADE")
        .execute(&mut conn)
        .await
        .expect("drop the fence table");
    let markers = probe_dr_markers(&mut conn, &unique_prefix(&db))
        .await
        .expect("a missing table is not an error");
    assert_eq!(markers.generation_shards, Vec::<ShardId>::new());
    assert!(!markers.is_dr());
}

/// A DR slot with no row yet still turns the fence on, and Auto provisions
/// the row for the shard the process serves.
#[tokio::test]
async fn auto_mode_provisions_the_row_on_a_slot_only_dr_database() {
    let _serial = registry_guard().await;
    let (url, db) = require_db!("autoslot");
    if !wal_level_is_logical(&url).await {
        eprintln!("SKIPPED autoslot: wal_level is not logical");
        return;
    }
    let prefix = unique_prefix(&db);
    let slot = format!("{prefix}_slot");
    let mut conn = connect(&url).await;
    diesel::sql_query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind::<diesel::sql_types::Text, _>(slot.clone())
        .execute(&mut conn)
        .await
        .expect("create slot");
    let pool = dr_pool(&url);
    let targets = Some((vec![(ShardId::new(6), pool.clone())], ShardId::new(6)));
    let resolved = pin_process_fence(DrFencing::Auto, &prefix, targets, &pool).await;
    let pinned = FenceRegistry::expected(ShardId::new(6));
    let row = current_generation(&mut conn, ShardId::new(6)).await;
    let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
        .bind::<diesel::sql_types::Text, _>(slot)
        .execute(&mut conn)
        .await;
    let Ok(resolved) = resolved else {
        panic!("a slot-only DR database starts fenced");
    };
    assert!(resolved.is_some(), "the slot turns the fence on");
    assert_eq!(pinned, Some(ShardGeneration::INITIAL));
    assert_eq!(row.expect("read"), Some(ShardGeneration::INITIAL));
}

/// In a sharded pool, a database whose row names another shard is
/// misconfigured: two DSNs are swapped. Pinning would add a second row that
/// `harvest dr fence` never bumps.
#[tokio::test]
async fn a_process_that_names_the_wrong_shard_refuses_to_start() {
    let _serial = registry_guard().await;
    let (url_a, _db_a) = require_db!("wrongshard_a");
    let (url_b, _db_b) = require_db!("wrongshard_b");
    let mut conn_b = connect(&url_b).await;
    ensure_generation_row(&mut conn_b, ShardId::new(7))
        .await
        .unwrap();
    let pool_a = dr_pool(&url_a);
    let pool_b = dr_pool(&url_b);
    let targets = Some((
        vec![(ShardId::new(0), pool_a.clone()), (ShardId::new(1), pool_b)],
        ShardId::new(0),
    ));
    let refused = pin_process_fence(DrFencing::Auto, DR_PREFIX, targets, &pool_a).await;
    let row = current_generation(&mut conn_b, ShardId::new(1)).await;
    let Err(error) = refused else {
        panic!("a shard mismatch must refuse to start");
    };
    assert!(error.to_string().contains("names shard"), "{error}");
    assert_eq!(row.expect("read"), None, "no second row is provisioned");
    assert!(!FenceRegistry::is_enabled());
}

/// Runners may split the logical shards of one database (issue #1823). A
/// second runner finds the first runner's row there and still pins.
#[tokio::test]
async fn split_runners_on_one_database_both_pin() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("splitrunners");
    {
        let mut conn = connect(&url).await;
        ensure_generation_row(&mut conn, ShardId::new(0))
            .await
            .unwrap();
    }
    let pool = dr_pool(&url);
    let targets = Some((vec![(ShardId::new(1), pool.clone())], ShardId::new(1)));
    let pinned = pin_process_fence(DrFencing::Auto, DR_PREFIX, targets, &pool).await;
    if let Err(error) = pinned {
        panic!("a runner for another logical shard on this database must pin: {error}");
    }
    assert_eq!(
        FenceRegistry::expected(ShardId::new(1)),
        Some(ShardGeneration::INITIAL)
    );
}

/// A fenced pass is a commit-order barrier (issue #1823). A bump waits for
/// the open pass, and a pass that starts after the bump is refused.
#[tokio::test]
async fn a_bump_waits_for_an_open_fenced_pass() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("passbarrier");
    let mut conn = connect(&url).await;
    let pinned = ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    FenceRegistry::publish(&[(ShardId::new(0), pinned)], ShardId::new(0)).expect("pin");
    let pool = dr_pool(&url);
    let guard = autumn_harvest::replication::begin_fenced_pass(&pool, ShardId::new(0))
        .await
        .expect("an unfenced shard opens a pass")
        .expect("a pinned shard gets a guard");

    let bump_url = url.clone();
    let bump = tokio::spawn(async move {
        let mut conn = connect(&bump_url).await;
        bump_generation(&mut conn, ShardId::new(0), "failover", "test").await
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let waited = !bump.is_finished();
    // The pass keeps writing while the bump waits. Its own fence checks
    // must not queue behind the bump, or the pass and the bump deadlock.
    let mut pass_conn = connect(&url).await;
    let pass_check = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        autumn_harvest::replication::assert_fence(&mut pass_conn, ShardId::new(0)),
    )
    .await;
    drop(guard);
    let bumped = bump.await.expect("bump task").expect("bump commits");
    let after = autumn_harvest::replication::begin_fenced_pass(&pool, ShardId::new(0)).await;

    assert!(waited, "a bump must wait for the open pass");
    assert!(
        matches!(pass_check, Ok(Ok(()))),
        "the pass's own fence check must not wait behind the bump"
    );
    assert!(bumped > pinned);
    assert!(
        matches!(
            after,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "a pass after the bump is refused"
    );
}

/// `Enabled` with no shard identity reads the shard from a single row, and
/// refuses when the database names none.
#[tokio::test]
async fn enabled_mode_without_shard_identity_needs_exactly_one_row() {
    let _serial = registry_guard().await;
    let (url, _db) = require_db!("enablednoid");
    let pool = dr_pool(&url);
    let refused = pin_process_fence(DrFencing::Enabled, DR_PREFIX, None, &pool).await;
    assert!(refused.is_err(), "no row and no identity: nothing to pin");
    assert!(!FenceRegistry::is_enabled());

    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(9))
        .await
        .unwrap();
    let Ok(resolved) = pin_process_fence(DrFencing::Enabled, DR_PREFIX, None, &pool).await else {
        panic!("one row names the shard");
    };
    assert!(resolved.is_some());
    assert_eq!(
        FenceRegistry::expected(ShardId::new(9)),
        Some(ShardGeneration::INITIAL)
    );
}

// ── Direct-database admin writes (issue #1823) ─────────────────────────────

/// An admin write against a demoted primary is rejected. The operator states
/// the epoch that holds authority; the old primary still has the older one.
#[tokio::test]
async fn an_admin_write_against_a_demoted_shard_is_rejected() {
    let (url, _db) = require_db!("admindemoted");
    let mut conn = connect(&url).await;
    let shard = ShardId::new(0);
    ensure_generation_row(&mut conn, shard).await.unwrap();

    // The promoted primary is at generation 1. This database never saw it.
    let error = assert_admin_write_authority(
        &mut conn,
        shard,
        Some(ShardGeneration::new(1)),
        DR_PREFIX,
        AdminWrite::Data,
    )
    .await
    .expect_err("a demoted shard must refuse the write");
    match error {
        autumn_harvest::error::HarvestError::ShardFenced {
            shard_id,
            pinned,
            current,
        } => {
            assert_eq!((shard_id, pinned, current), (0, 1, Some(0)));
        }
        other => panic!("expected ShardFenced, got {other:?}"),
    }

    assert_admin_write_authority(
        &mut conn,
        shard,
        Some(ShardGeneration::INITIAL),
        DR_PREFIX,
        AdminWrite::Data,
    )
    .await
    .expect("the stated epoch matches, so the write may run");
}

/// On a DR database, an admin write with no stated epoch is refused.
#[tokio::test]
async fn an_admin_write_on_a_dr_database_must_state_the_epoch() {
    let (url, db) = require_db!("adminepoch");
    let mut conn = connect(&url).await;
    assert_admin_write_authority(
        &mut conn,
        ShardId::new(0),
        None,
        &unique_prefix(&db),
        AdminWrite::Data,
    )
    .await
    .expect("a plain database needs no epoch");

    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    let error = assert_admin_write_authority(
        &mut conn,
        ShardId::new(0),
        None,
        DR_PREFIX,
        AdminWrite::Data,
    )
    .await
    .expect_err("a DR database needs a stated epoch");
    assert!(
        matches!(error, autumn_harvest::error::HarvestError::Config(_)),
        "{error:?}"
    );
}

/// A data write on a logical standby is refused even when the stated epoch
/// matches: the standby carries the replicated row at the same generation.
/// The subscription's name does not matter.
/// A schema-only write (partition DDL) is allowed there, because logical
/// replication carries no DDL and the docs require it on both sides.
#[tokio::test]
async fn an_admin_data_write_on_a_logical_standby_is_refused() {
    let (url, db) = require_db!("adminstandby");
    let mut conn = connect(&url).await;
    ensure_generation_row(&mut conn, ShardId::new(0))
        .await
        .unwrap();
    // A custom name: deployments may set their own slot prefix, and the CLI
    // cannot know it. The standby check must not depend on the name.
    let sub = format!("custom_sub_{db}");
    diesel::sql_query(format!(
        "CREATE SUBSCRIPTION {sub} CONNECTION 'dbname=unused' PUBLICATION harvest_dr \
         WITH (connect = false)"
    ))
    .execute(&mut conn)
    .await
    .expect("create a disconnected subscription");

    let data = assert_admin_write_authority(
        &mut conn,
        ShardId::new(0),
        Some(ShardGeneration::INITIAL),
        DR_PREFIX,
        AdminWrite::Data,
    )
    .await;
    let schema = assert_admin_write_authority(
        &mut conn,
        ShardId::new(0),
        Some(ShardGeneration::INITIAL),
        DR_PREFIX,
        AdminWrite::SchemaOnly,
    )
    .await;
    let _ = diesel::sql_query(format!("ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)"))
        .execute(&mut conn)
        .await;
    let _ = diesel::sql_query(format!("DROP SUBSCRIPTION {sub}"))
        .execute(&mut conn)
        .await;

    let error = data.expect_err("a data write on a standby must be refused");
    assert!(error.to_string().contains("standby"), "{error}");
    schema.expect("partition DDL runs on both sides of logical replication");
}

/// Counts `harvest.shard.fenced`; every other metric is the default no-op.
#[derive(Debug)]
struct FenceCounter {
    fenced: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl autumn_harvest::telemetry::MetricsRecorder for FenceCounter {
    fn record_shard_fenced(&self, _shard: u16) {
        self.fenced
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn dr_pool(url: &str) -> autumn_harvest::worker::DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool build")
}

fn dr_worker_config() -> autumn_harvest::worker::WorkerRuntimeConfig {
    autumn_harvest::worker::WorkerRuntimeConfig::from(
        autumn_harvest::builder::WorkerConfig::default()
            .with_dr_fencing(true)
            .with_replication_sample_interval(std::time::Duration::from_millis(300)),
    )
}

// ── The two-"region" topology, over real logical replication ───────────────
/// Build a libpq conninfo string that **the server itself** can use to reach
/// the publisher database.
///
/// The host and port are deliberately NOT taken from `url`. `url` is the
/// *client-side* address, and under testcontainers that is a host-mapped port
/// (`localhost:33624`) which exists only on the Docker host. `CREATE
/// SUBSCRIPTION` makes Postgres dial the publisher from inside its own network
/// namespace, where that port is closed — the subscription then fails with
/// "could not connect to the publisher ... Connection refused". Both "regions"
/// are databases in one instance, so the address the server needs is its own:
/// loopback on the port it is actually listening on, which it can be asked for.
///
/// `127.0.0.1` rather than `localhost` on purpose: `localhost` resolved to
/// `::1` in CI, and a server not listening on IPv6 refuses that even when the
/// port is right.
async fn server_side_conninfo(conn: &mut AsyncPgConnection, url: &str, db: &str) -> String {
    #[derive(diesel::QueryableByName)]
    struct Port {
        #[diesel(sql_type = diesel::sql_types::Text)]
        port: String,
    }

    let port = diesel::sql_query("SELECT current_setting('port') AS port")
        .load::<Port>(conn)
        .await
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .map_or_else(|| "5432".to_string(), |r| r.port);

    let rest = url
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://");
    // Only the credentials are taken from `url`; the address comes from the
    // server itself, above.
    let userinfo = rest.split_once('@').map_or("postgres", |(u, _)| u);
    let (user, password) = userinfo
        .split_once(':')
        .map_or((userinfo, None), |(u, p)| (u, Some(p)));
    use std::fmt::Write as _;

    let mut conninfo = format!("host=127.0.0.1 port={port} user={user} dbname={db}");
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
        // `DROP SUBSCRIPTION` can deadlock against a sync worker that is still
        // creating its slot. Each step is therefore bounded. A step that times
        // out is logged and skipped, so cleanup never pins a CI shard.
        let bound = std::time::Duration::from_secs(30);
        let drop_subscription = async {
            if let Ok(mut b) = AsyncPgConnection::establish(&self.standby_url).await {
                let _ = b
                    .batch_execute(&format!("DROP SUBSCRIPTION IF EXISTS {}", self.sub))
                    .await;
            }
        };
        if tokio::time::timeout(bound, drop_subscription)
            .await
            .is_err()
        {
            eprintln!(
                "teardown: DROP SUBSCRIPTION {} did not finish within 30s, skipped",
                self.sub
            );
        }
        let drop_slot = async {
            if let Ok(mut a) = AsyncPgConnection::establish(&self.primary_url).await {
                let _ = diesel::sql_query(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                     WHERE slot_name = $1",
                )
                .bind::<diesel::sql_types::Text, _>(self.slot.clone())
                .execute(&mut a)
                .await;
            }
        };
        if tokio::time::timeout(bound, drop_slot).await.is_err() {
            eprintln!(
                "teardown: dropping slot {} did not finish within 30s, skipped",
                self.slot
            );
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
    rows.is_ok_and(|r| {
        r.into_iter()
            .next()
            .is_some_and(|s| s.wal_level == "logical")
    })
}

/// Build the two-region topology, or `None` when the server cannot host it.
async fn two_regions(tag: &str) -> Option<Regions> {
    let admin = admin_url().await?;
    if !wal_level_is_logical(&admin).await {
        eprintln!(
            "SKIPPED {tag}: server is not configured with wal_level=logical, so stock logical \
             replication cannot be exercised and this test proved NOTHING"
        );
        return None;
    }
    let (primary_url, primary_db) = fresh_db(&format!("{tag}a")).await?;
    let (standby_url, _standby_db) = fresh_db(&format!("{tag}b")).await?;

    let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
    let slot = format!("{DR_PREFIX}_slot_{}_{n}", std::process::id());
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

    let conninfo = server_side_conninfo(&mut a, &admin, &primary_db).await;
    let mut b = connect(&standby_url).await;
    // The migrations seed some tables, such as `harvest_calendars`, in both
    // databases. The initial copy of such a table then fails on a duplicate
    // key and retries forever. A real standby starts empty, so the standby
    // here is emptied first. The copy then brings the primary's rows.
    b.batch_execute(
        "DO $$ DECLARE tables text; BEGIN \
           SELECT string_agg(format('%I.%I', schemaname, tablename), ', ') INTO tables \
             FROM pg_tables \
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
              AND tablename <> '__diesel_schema_migrations'; \
           IF tables IS NOT NULL THEN EXECUTE 'TRUNCATE ' || tables || ' CASCADE'; END IF; \
         END $$;",
    )
    .await
    .expect("empty the standby before the initial copy");
    let create_subscription = format!(
        "CREATE SUBSCRIPTION {sub} CONNECTION '{conninfo}' PUBLICATION harvest_dr \
         WITH (create_slot = false, slot_name = '{slot}', copy_data = true)"
    );
    // `copy_data = true` blocks until the STANDBY's Postgres *server* process
    // reaches the primary at `conninfo`. This test client does not make that
    // connection. It depends on the runner's container networking, not on
    // this test's logic, and Postgres places no timeout on it. A bad or
    // momentarily-unreachable address here therefore hangs this `.await`
    // forever rather than erroring, which is exactly what
    // pinned `Test DB (linux, shard 1)` for a full 6-hour CI job on a run whose
    // diff never touched this file (see the PR discussion this comment was
    // added from). Bounding it turns that into a fast, clear skip — consistent
    // with this function's existing "server cannot host it" contract
    // (`wal_level_is_logical` above already skips for the same class of
    // reason), not a special case.
    match tokio::time::timeout(
        std::time::Duration::from_secs(60),
        b.batch_execute(&create_subscription),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("subscription: {e}"),
        Err(_) => {
            eprintln!(
                "SKIPPED {tag}: CREATE SUBSCRIPTION did not complete within 60s — the standby's \
                 Postgres process could not reach the primary at the server-side conninfo in \
                 this environment. Dropping the orphaned replication slot; this test proved \
                 NOTHING."
            );
            // Best-effort cleanup: an orphaned slot pins WAL on the shared test
            // server indefinitely (the same concern the disconnected-standby
            // test below already guards against for its own slot).
            let _ = diesel::sql_query("SELECT pg_drop_replication_slot($1)")
                .bind::<diesel::sql_types::Text, _>(slot.clone())
                .execute(&mut a)
                .await;
            return None;
        }
    }

    let regions = Regions {
        primary_url,
        primary_db,
        standby_url,
        slot,
        sub,
    };
    // `CREATE SUBSCRIPTION` returns before the initial copy ends. Sync workers
    // copy each table on their own, so one table can lag behind another. A
    // test that drops the subscription too early loses the rows that are not
    // copied yet. One such loss was a `harvest_workflow_executions` row: the
    // promoted region then failed an append on `harvest_events_workflow_exec_id_fkey`.
    // So the topology is ready only when every table is in state `r` (ready).
    // From then on one apply worker applies changes in commit order.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        let syncing = count_on(
            &regions.standby_url,
            "SELECT COUNT(*) AS n FROM pg_subscription_rel WHERE srsubstate <> 'r'",
        )
        .await;
        if syncing == 0 {
            return Some(regions);
        }
        if std::time::Instant::now() >= deadline {
            regions.teardown().await;
            panic!("{tag}: {syncing} table(s) did not finish the initial sync within 120s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

macro_rules! require_regions {
    ($tag:literal) => {
        match two_regions($tag).await {
            Some(v) => v,
            None => {
                eprintln!(
                    "SKIPPED {}: no two-region topology available — this test proved NOTHING",
                    $tag
                );
                return;
            }
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
        record_replication_heartbeat(&mut a, shard, retain, BEAT_INTERVAL)
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
                let _ = record_replication_heartbeat(&mut conn, shard, retain, BEAT_INTERVAL).await;
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                matches!(
                    measure_rpo(&mut conn, shard, DR_PREFIX).await,
                    Ok(WatermarkReading::Measured(v)) if v < 5.0
                )
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
        record_replication_heartbeat(&mut a, shard, retain, BEAT_INTERVAL)
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let independently_measured = stall_started.elapsed().as_secs_f64();
    let reported = match measure_rpo(&mut a, shard, DR_PREFIX)
        .await
        .map_err(|e| e.to_string())?
    {
        WatermarkReading::Measured(v) => v,
        // A stall shorter than the retained trail must stay a MEASUREMENT: the
        // floor is for a standby that has fallen off the end of the trail
        // entirely, which a 12s stall against an hour of retention is not.
        other => {
            blocker.batch_execute("ROLLBACK").await.ok();
            return Err(format!(
                "a stall well inside the retention window must be measured exactly, got {other:?}"
            ));
        }
    };

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
    let status = autumn_harvest::replication::query_replication_status(&mut a, shard, DR_PREFIX)
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
                let _ = record_replication_heartbeat(&mut conn, shard, retain, BEAT_INTERVAL).await;
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                matches!(
                    measure_rpo(&mut conn, shard, DR_PREFIX).await,
                    Ok(WatermarkReading::Measured(v)) if v < 5.0
                )
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
                BEAT_INTERVAL,
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
                    autumn_harvest::replication::query_replication_status(
                        &mut conn, shard, DR_PREFIX,
                    )
                    .await
                    .is_ok_and(|s| s.connected_standbys() == 0)
                }
            },
        )
        .await;

        let status =
            autumn_harvest::replication::query_replication_status(&mut a, shard, DR_PREFIX)
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
    assert_eq!(promoted_gen, ShardGeneration::new(1));

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
    FenceRegistry::register(shard, ShardGeneration::new(0))
        .expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(shard).expect("no conflicting default shard in this test");

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
    FenceRegistry::register(shard, promoted_gen).expect("no conflicting pin in this test");
    FenceRegistry::set_default_shard(shard).expect("no conflicting default shard in this test");

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
