//! Ledger performance investigation (issue #1620): the workflow-start
//! outbox relay's per-row delivery-mark round trip.
//!
//! `drain_workflow_start_outbox_batch` (`autumn-harvest-plugin/src/outbox.rs`)
//! claims one batch of due `harvest_workflow_outbox` rows -- default
//! `batch_size` 32 -- and calls `dispatch_workflow_start_request` for each
//! one. `dispatch_workflow_start_request` genuinely differs per row: it
//! starts a distinct workflow execution and cannot be batched. The mark
//! that records each row's outcome is a single fixed-shape `UPDATE ...
//! WHERE id = $1 AND claimed_by = $2`, differing only in its bound values.
//! Before this investigation's fix, that mark ran once per row. At a
//! 50-row batch drain, it was 90.9% of the drain's own statement calls
//! and 46.0% of its buffers. The fix batches it into one `UPDATE ... FROM
//! UNNEST(...)` call per outcome (delivered, failed) per drain.
//!
//! This file captures the `pg_stat_statements` profile of a full drain
//! (`flush_workflow_start_outbox`), against whatever mark shape the
//! checked-out code currently uses. Evidence is `pg_stat_statements` call
//! and buffer counts, never wall-clock. Wall-clock is not admissible on a
//! shared-vCPU machine. This harness follows the same shape as
//! `completion_trigger_outbox_queue_perf.rs`. Each measurement point gets
//! a fresh, uniquely-named, fully-migrated pair of databases (app and
//! harvest), mirroring the relay's real split-database deployment.
//! `pg_stat_statements` is reset immediately before the measured call and
//! snapshotted immediately after it. See
//! `docs/perf-artifacts/outbox-start-relay/` for the before/after capture
//! this file produced and `docs/performance-outbox-start-relay.md` for the
//! full writeup.

#![allow(clippy::too_many_lines)]

use autumn_harvest_plugin::{
    HarvestDbPool, WorkflowStartRequest, enqueue_workflow_start_outbox, flush_workflow_start_outbox,
};
use autumn_web::AppState;
use autumn_web::config::DatabaseConfig;
use diesel_async::pooled_connection::deadpool;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use uuid::Uuid;

const OUTBOX_INIT_SQL: &str =
    include_str!("../migrations/app/20260409010000_harvest_workflow_outbox/up.sql");

// ── DB bootstrap (mirrors completion_trigger_outbox_queue_perf.rs, adapted
//    for the outbox relay's real two-database split) ───────────────────────

fn admin_url() -> String {
    std::env::var("HARVEST_TEST_DATABASE_URL")
        .expect("HARVEST_TEST_DATABASE_URL must point at a reachable Postgres admin role")
}

async fn create_fresh_app_db(admin: &str, name: &str) -> (String, AsyncPgConnection) {
    let mut admin_conn = AsyncPgConnection::establish(admin)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin_conn)
        .await;
    let (prefix, _) = admin.rsplit_once('/').expect("url has a db segment");
    let url = format!("{prefix}/{name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh app database");
    conn.batch_execute(OUTBOX_INIT_SQL)
        .await
        .expect("apply app outbox migration");
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await;
    (url, conn)
}

async fn create_fresh_harvest_db(admin: &str, name: &str) -> String {
    let mut admin_conn = AsyncPgConnection::establish(admin)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin_conn)
        .await;
    let (prefix, _) = admin.rsplit_once('/').expect("url has a db segment");
    let url = format!("{prefix}/{name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh harvest database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply harvest migration bundle");
    drop(conn);
    url
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

fn build_pool(database_url: &str, pool_size: usize) -> deadpool::Pool<AsyncPgConnection> {
    autumn_web::db::create_pool(&DatabaseConfig {
        url: Some(database_url.to_owned()),
        pool_size,
        ..DatabaseConfig::default()
    })
    .expect("failed to build pool config")
    .expect("database url should create a pool")
}

fn build_test_state(app_url: &str, harvest_url: &str) -> AppState {
    let state = AppState::for_test().with_pool(build_pool(app_url, 8));
    state.insert_extension(HarvestDbPool::from(build_pool(harvest_url, 8)));
    state
}

// ── pg_stat_statements capture (mirrors completion_trigger_outbox_queue_perf.rs) ──

#[allow(dead_code)] // Row mirrors the full pg_stat_statements snapshot; not every column feeds a computed total.
#[derive(diesel::QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    total_buffers: i64,
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
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
                (shared_blks_hit + shared_blks_read) AS total_buffers \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY total_buffers DESC"
    ))
    .load(conn)
    .await
    .expect(
        "pg_stat_statements query failed -- it must be preloaded via shared_preload_libraries \
         for this capture to produce real evidence rather than fail outright",
    )
}

/// Whether `row` is the delivery-mark `UPDATE` this investigation targets.
/// Matches both the delivered-mark (sets `delivered_execution_id`) and the
/// failed-mark (sets `next_attempt_at`) shapes. Both set
/// `delivery_attempts`, which the claim statement never touches. So this
/// predicate cannot also match the claim statement. Both statements set
/// `claimed_by`/`claimed_at`, so matching on those alone double-counts.
fn is_mark_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("update") && q.contains("harvest_workflow_outbox") && q.contains("delivery_attempts")
}

fn is_claim_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("update")
        && q.contains("harvest_workflow_outbox")
        && q.contains("claimed_at")
        && !q.contains("delivery_attempts")
}

#[allow(clippy::cast_precision_loss)]
fn fmt_row(r: &StatRow, total_calls: i64, total_buffers: i64) -> String {
    let query: String = r.query.split_whitespace().collect::<Vec<_>>().join(" ");
    let query = if query.len() > 100 {
        format!("{}...", &query[..100])
    } else {
        query
    };
    format!(
        "calls={:>4} ({:>5.1}%)  buffers={:>5} ({:>5.1}%)  {query}",
        r.calls,
        100.0 * r.calls as f64 / total_calls.max(1) as f64,
        r.total_buffers,
        100.0 * r.total_buffers as f64 / total_buffers.max(1) as f64,
    )
}

// ── Direct measurement: flush_workflow_start_outbox ────────────────────────

struct SizePoint {
    n: i64,
    delivered: usize,
    mark_calls: i64,
    mark_buffers: i64,
    claim_calls: i64,
    total_calls: i64,
    total_buffers: i64,
    profile_by_buffers: Vec<String>,
    profile_by_calls: Vec<String>,
}

async fn measure_one_batch(admin: &str, n: usize) -> SizePoint {
    let db_id = unique("obsr");
    let app_name = format!("{db_id}_app");
    let harvest_name = format!("{db_id}_hv");
    let (app_url, mut app_conn) = create_fresh_app_db(admin, &app_name).await;
    let harvest_url = create_fresh_harvest_db(admin, &harvest_name).await;

    for i in 0..n {
        enqueue_workflow_start_outbox(
            &mut app_conn,
            &WorkflowStartRequest {
                workflow_name: "outbox_relay_perf_wf".to_string(),
                workflow_id: format!("outbox-relay-perf-{db_id}-{i}"),
                queue_name: "default".to_string(),
                input: serde_json::json!({ "index": i }),
                memo: None,
                search_attrs: None,
            },
        )
        .await
        .expect("seed outbox row");
    }

    let state = build_test_state(&app_url, &harvest_url);

    let mut stats_conn = AsyncPgConnection::establish(&app_url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &app_name).await;

    // The real public entry point under test: the outbox relay's drain
    // loop, exactly as `spawn_workflow_start_outbox_relay`'s periodic tick
    // calls it.
    let delivered = flush_workflow_start_outbox(&state)
        .await
        .expect("flush_workflow_start_outbox should succeed");

    let all_rows = snapshot_statements(&mut stats_conn, &app_name).await;
    let mark_rows: Vec<&StatRow> = all_rows.iter().filter(|r| is_mark_statement(r)).collect();
    let claim_rows: Vec<&StatRow> = all_rows.iter().filter(|r| is_claim_statement(r)).collect();
    let mark_calls: i64 = mark_rows.iter().map(|r| r.calls).sum();
    let mark_buffers: i64 = mark_rows.iter().map(|r| r.total_buffers).sum();
    let claim_calls: i64 = claim_rows.iter().map(|r| r.calls).sum();
    let total_calls: i64 = all_rows.iter().map(|r| r.calls).sum();
    let total_buffers: i64 = all_rows.iter().map(|r| r.total_buffers).sum();

    let profile_by_buffers: Vec<String> = all_rows
        .iter()
        .take(10)
        .map(|r| fmt_row(r, total_calls, total_buffers))
        .collect();
    let mut by_calls: Vec<&StatRow> = all_rows.iter().collect();
    by_calls.sort_by_key(|r| std::cmp::Reverse(r.calls));
    let profile_by_calls: Vec<String> = by_calls
        .iter()
        .take(10)
        .map(|r| fmt_row(r, total_calls, total_buffers))
        .collect();

    SizePoint {
        n: i64::try_from(n).unwrap(),
        delivered,
        mark_calls,
        mark_buffers,
        claim_calls,
        total_calls,
        total_buffers,
        profile_by_buffers,
        profile_by_calls,
    }
}

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- run manually against \
            HARVEST_TEST_DATABASE_URL"]
#[allow(clippy::cast_precision_loss)]
async fn zz_capture_outbox_start_relay_perf_evidence() {
    let admin = admin_url();

    for n in [5_usize, 20, 50] {
        let point = measure_one_batch(&admin, n).await;
        eprintln!(
            "n={} delivered={} claim_calls={} mark_calls={} mark_buffers={} total_calls={} \
             total_buffers={} mark_call_share={:.1}% mark_buffer_share={:.1}%",
            point.n,
            point.delivered,
            point.claim_calls,
            point.mark_calls,
            point.mark_buffers,
            point.total_calls,
            point.total_buffers,
            100.0 * point.mark_calls as f64 / point.total_calls.max(1) as f64,
            100.0 * point.mark_buffers as f64 / point.total_buffers.max(1) as f64,
        );
        if point.n == 50 {
            eprintln!("-- top statements by buffers (n=50) --");
            for line in &point.profile_by_buffers {
                eprintln!("{line}");
            }
            eprintln!("-- top statements by calls (n=50) --");
            for line in &point.profile_by_calls {
                eprintln!("{line}");
            }
        }
    }
}
