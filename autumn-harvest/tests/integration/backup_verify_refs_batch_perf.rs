#![cfg(all(feature = "db", feature = "testing"))]
//! Ledger performance fix: `backup_verify::probes::adjudicate_refs`'s
//! per-reference execution-state lookup N+1.
//!
//! `resolve_refs` groups every cross-shard `PendingRef` a shard's history
//! scan discovered by the shard that OWNS the reference target, then hands
//! each owner-shard group to `adjudicate_refs`. That function issued one
//! `SELECT state FROM harvest_workflow_executions WHERE id = $1` per
//! reference — `n` round trips for `n` references pointing at one target
//! shard.
//!
//! This is the exact class of query the sibling completion-trigger-fire
//! path (`adjudicate_trigger_fires`, issue #1401) was already rewritten to
//! avoid, and for the documented reason: "One query per fire ... would make
//! a routine restore drill issue up to two round trips per row." That
//! reasoning applies unchanged to `adjudicate_refs` — it was simply never
//! carried over when #1401 landed.
//!
//! The fix batches the state lookup with the same `= ANY($1)` shape and the
//! same `WORKFLOW_KEY_LOOKUP_CHUNK` (1,000-row) bound `adjudicate_refs`'s
//! sibling already uses, so a whole-shard batch cannot hold an unbounded
//! request or result set in memory either.
//!
//! Evidence here is `pg_stat_statements` call counts, driven end-to-end
//! through the public `verify_restore` entry point against two real,
//! freshly-migrated shard databases — not a direct call to the private
//! `adjudicate_refs` helper. This harness follows the same shape as
//! `child_fanout_batch_perf.rs`: a fresh, uniquely-named database per
//! measurement, `pg_stat_statements` reset immediately before the measured
//! call and snapshotted immediately after it.

use autumn_harvest::backup_verify::{ShardTarget, VerifyOptions, verify_restore};
use autumn_harvest::testing::WorkflowReplayer;
use autumn_harvest::types::{ExecutionId, ShardId};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

// ── DB bootstrap (mirrors child_fanout_batch_perf.rs) ───────────────────────

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
        // pure-Docker fallback path, mirroring `child_fanout_batch_perf.rs`
        // and `claim_bench_support.rs`.
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

// ── pg_stat_statements capture (mirrors child_fanout_batch_perf.rs) ─────────

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

// ── Fixture seeding (mirrors backup_verify_tests.rs's helpers) ─────────────

async fn seed_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    shard_id: i32,
) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, state, input, started_at, shard_id, queue_name) \
         VALUES ($1, $2, $3, $4, '{}'::jsonb, NOW(), $5, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .execute(conn)
    .await
    .expect("seed execution");
}

async fn append_event(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    event_id: i32,
    event_type: &str,
    data: serde_json::Value,
) {
    let payload = json!({ "type": event_type, "data": data });
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         VALUES ($1, $2, $3, $4, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(event_id)
    .bind::<diesel::sql_types::Text, _>(event_type)
    .bind::<diesel::sql_types::Jsonb, _>(payload)
    .execute(conn)
    .await
    .expect("append event");
}

/// How many awaited-child references the fixture seeds. Comfortably inside
/// `VerifyOptions::default().probe_limit` (1,000) and `WORKFLOW_KEY_LOOKUP_CHUNK`
/// (1,000), so the whole fixture lands in one owner-shard batch either way —
/// this measures the per-reference round-trip count, not chunking behavior.
const REF_COUNT: i64 = 500;

/// End-to-end: `N` parents on shard 0 each await one distinct, live child on
/// shard 1. Every child exists and is `RUNNING`, so every reference takes the
/// clean `(AwaitedChild, Some(_))` branch in `adjudicate_refs` — no
/// `retention_summary_exists` or `effect_verdict` calls to muddy the count.
/// This isolates exactly the state lookup the fix targets.
#[tokio::test]
async fn awaited_child_batch_resolves_with_bounded_state_lookup_calls() {
    let (admin, _guard) = setup_server().await;
    let db_a = unique("bkverify_refs_perf_a");
    let db_b = unique("bkverify_refs_perf_b");
    let url_a = create_fresh_db(&admin, &db_a).await;
    let url_b = create_fresh_db(&admin, &db_b).await;

    let mut a = AsyncPgConnection::establish(&url_a)
        .await
        .expect("connect shard 0");
    let mut b = AsyncPgConnection::establish(&url_b)
        .await
        .expect("connect shard 1");
    ensure_pg_stat_statements(&mut b).await;

    for i in 0..REF_COUNT {
        let parent = ExecutionId::new_for_shard(ShardId::new(0));
        let child = ExecutionId::new_for_shard(ShardId::new(1));
        seed_execution(
            &mut a,
            parent,
            "parent_flow",
            &format!("pf-{i}"),
            "RUNNING",
            0,
        )
        .await;
        append_event(&mut a, parent, 1, "WorkflowStarted", json!({ "input": {} })).await;
        append_event(
            &mut a,
            parent,
            2,
            "ChildWorkflowStarted",
            json!({ "child_id": child.to_string(), "workflow_name": "child_flow", "input": {} }),
        )
        .await;
        seed_execution(
            &mut b,
            child,
            "child_flow",
            &format!("cf-{i}"),
            "RUNNING",
            1,
        )
        .await;
    }

    let mut stats_conn = AsyncPgConnection::establish(&url_b)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_b).await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let opts = VerifyOptions::default().with_scratch_ack(true);
    let report = verify_restore(&targets, &opts, &WorkflowReplayer::new()).await;

    // Functional correctness first: a healthy, fully-live fixture must not
    // report any of these references as missing, rolled back, or unproven.
    assert!(
        !report.detected(autumn_harvest::backup_verify::FindingClass::ChildExecutionMissing),
        "every child is live and RUNNING: {report:#?}"
    );
    assert!(
        !report.detected(autumn_harvest::backup_verify::FindingClass::ChildTerminalRolledBack),
        "{report:#?}"
    );
    assert!(
        !report.detected(autumn_harvest::backup_verify::FindingClass::RetentionUnproven),
        "{report:#?}"
    );

    let rows = snapshot_statements(&mut stats_conn, &db_b).await;
    let per_row_calls = calls_containing(
        &rows,
        "select state from harvest_workflow_executions where id = $1",
    );
    let batched_calls = calls_containing(
        &rows,
        "select id, state from harvest_workflow_executions where id = any($1)",
    );

    // RED: characterizes the current, unfixed behavior. Measured on this
    // machine: `SELECT state FROM harvest_workflow_executions WHERE id = $1`
    // -- calls: 500, exactly `REF_COUNT`. That is one round trip per
    // reference. No batched `= ANY($1)` statement exists yet.
    assert_eq!(
        per_row_calls, REF_COUNT,
        "adjudicate_refs currently issues one `WHERE id = $1` round trip per reference: {rows:#?}"
    );
    assert_eq!(
        batched_calls, 0,
        "no batched `= ANY($1)` state lookup exists yet: {rows:#?}"
    );
}
