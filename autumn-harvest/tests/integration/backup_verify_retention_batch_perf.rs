#![cfg(all(feature = "db", feature = "testing"))]
//! Ledger performance fix: `backup_verify::probes::adjudicate_refs`'s
//! per-reference retention-summary lookup N+1 (issue #1704 follow-up).
//!
//! `backup_verify_refs_batch_perf.rs` (issue #1704) already batched the
//! execution-STATE lookup of this function: one `= ANY($1)` per chunk, not
//! one `WHERE id = $1` round trip per reference. Its fixture isolates the
//! state lookup. Every reference there resolves against a live, `RUNNING`
//! target, so none reaches the retention-summary branch.
//!
//! This file covers that branch. Take a restore drill against a backup old
//! enough that retention collected a completed child. That is the ordinary
//! case for a `ChildTerminalRecorded` reference. The parent recorded the
//! terminal of the child, and the execution row of the child is gone.
//! `harvest_execution_summaries` proves that retention, not data loss,
//! removed it. Before this fix, `adjudicate_refs` sent one `SELECT EXISTS
//! (... WHERE execution_id = $1)` round trip per such reference. The fix
//! batches it as the state lookup is batched: `matching_retention_summaries`
//! makes one `= ANY($1)` call per chunk.
//!
//! The evidence is `pg_stat_statements` call counts, through the public
//! `verify_restore` entry point. The harness has the shape of
//! `backup_verify_refs_batch_perf.rs`.

use autumn_harvest::backup_verify::{ShardTarget, VerifyOptions, verify_restore};
use autumn_harvest::testing::WorkflowReplayer;
use autumn_harvest::types::{ExecutionId, ShardId};
use chrono::Utc;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::json;
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
    .expect("snapshot pg_stat_statements")
}

fn calls_containing(rows: &[StatRow], needle: &str) -> i64 {
    rows.iter()
        .filter(|r| r.query.to_lowercase().contains(needle))
        .map(|r| r.calls)
        .sum()
}

// ── Fixture seeding ──────────────────────────────────────────────────────────

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

async fn seed_retention_summary(conn: &mut AsyncPgConnection, exec_id: ExecutionId, shard_id: i32) {
    let now = Utc::now();
    diesel::sql_query(
        "INSERT INTO harvest_execution_summaries \
         (execution_id, workflow_name, workflow_id, state, started_at, completed_at, shard_id) \
         VALUES ($1, 'child_flow', $2, 'COMPLETED', $3, $3, $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Timestamptz, _>(now)
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .execute(conn)
    .await
    .expect("seed retention summary");
}

/// The `ChildTerminalRecorded` references that the fixture seeds, each
/// against a target row that does not exist.
///
/// The count stays under `VerifyOptions::default().probe_limit` (1,000) and
/// `WORKFLOW_KEY_LOOKUP_CHUNK` (1,000). The fixture lands in one chunk, so
/// the test measures round trips per reference, not chunking.
const REF_COUNT: i64 = 500;

/// End-to-end: `N` parents on shard 0 each recorded the terminal of a
/// distinct child with `ChildWorkflowCompleted`.
///
/// No child row exists on shard 1. Each child has a
/// `harvest_execution_summaries` row instead, as after retention. So each
/// reference takes the `(ChildTerminalRecorded, None)` branch with retention
/// proven. No `RetentionUnproven` finding fires, but the query under test
/// runs.
#[tokio::test]
async fn terminal_child_batch_resolves_with_bounded_retention_lookup_calls() {
    let (admin, _guard) = setup_server().await;
    let db_a = unique("bkverify_retention_perf_a");
    let db_b = unique("bkverify_retention_perf_b");
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
        // The scan adjudicates a CHILD reference only from a NON-TERMINAL
        // owner: the `e.state IN ('RUNNING', 'PAUSED', 'SUSPENDED')` filter
        // of `scan_reference_events`. For a terminal parent, a recorded
        // child terminal is history. A parent stays `RUNNING` after one
        // child completes when it awaits further steps.
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
        append_event(
            &mut a,
            parent,
            3,
            "ChildWorkflowCompleted",
            json!({ "child_id": child.to_string(), "output": {} }),
        )
        .await;
        // Only the retention summary of the child exists on shard 1. So each
        // reference takes the retention-summary branch of `adjudicate_refs`.
        seed_retention_summary(&mut b, child, 1).await;
    }

    let mut stats_conn = AsyncPgConnection::establish(&url_b)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_b).await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let opts = VerifyOptions::default().with_scratch_ack(true);
    let report = verify_restore(&targets, &opts, &WorkflowReplayer::new()).await;

    // Functional correctness first: every reference has provable retention,
    // so none of these findings may fire.
    assert!(
        !report.detected(autumn_harvest::backup_verify::FindingClass::RetentionUnproven),
        "every target has a retention summary: {report:#?}"
    );
    assert!(
        !report.detected(autumn_harvest::backup_verify::FindingClass::ChildTerminalRolledBack),
        "{report:#?}"
    );
    assert!(
        !report.detected(autumn_harvest::backup_verify::FindingClass::ChildExecutionMissing),
        "{report:#?}"
    );

    let rows = snapshot_statements(&mut stats_conn, &db_b).await;
    let per_row_calls = calls_containing(
        &rows,
        "select exists ( select $2 from harvest_execution_summaries where execution_id = $1 ) as present",
    );
    let batched_calls = calls_containing(
        &rows,
        "select execution_id from harvest_execution_summaries where execution_id = any($1)",
    );
    eprintln!(
        "backup_verify_retention_batch_perf: per_row_calls={per_row_calls} \
         batched_calls={batched_calls} all_rows={rows:#?}"
    );

    // GREEN: the whole batch resolves in one `= ANY($1)` round trip, and
    // the old per-row `EXISTS` never runs. Measured here, `SELECT EXISTS
    // (... WHERE execution_id = $1)` had 500 calls before the fix (one per
    // reference) and 0 after it.
    assert_eq!(
        per_row_calls, 0,
        "the per-reference retention-summary `EXISTS` lookup must not be issued once the batch \
         fix is in place: {rows:#?}"
    );
    assert_eq!(
        batched_calls, 1,
        "a {REF_COUNT}-reference batch, under the 1,000-row chunk bound, must resolve its \
         retention-summary lookup in exactly one `= ANY($1)` round trip: {rows:#?}"
    );
}
