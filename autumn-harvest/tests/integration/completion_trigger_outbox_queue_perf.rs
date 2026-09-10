#![cfg(feature = "db")]
//! Ledger performance investigation: the completion-trigger outbox relay's
//! per-row `harvest_schedules` lookup.
//!
//! `enforce_completion_triggers_outbox` (`autumn-harvest/src/completion_trigger.rs`)
//! is the scanner that relays a batch of up to `OUTBOX_CLAIM_BATCH_LIMIT`
//! (50) cross-shard completion-trigger targets on every poll tick. For
//! each task whose `queue_name` column is `NULL`, it calls
//! `resolve_target_queue`. That is one `SELECT queue_name FROM
//! harvest_schedules WHERE workflow_name = $1` against the default shard,
//! on that task's own turn through the loop.
//!
//! `queue_name` is `NULL` on the common path: `CompletionTrigger::new`
//! defaults it to `None`, and a caller must explicitly call
//! `.with_queue_name(...)` to set it. A fan-in deployment sends many
//! source executions' completions through the same downstream trigger.
//! That fills a claim batch with rows that share a `target_shard`.
//! Commonly, they also share a `target_workflow_name`. This is exactly
//! the shape this persona's own charter names: "Workflow/activity
//! bookkeeping queries (Harvest) that are individually trivial and
//! collectively dominant. These will never show up in a buffer ranking;
//! find them by `calls`."
//!
//! The fix adds `resolve_target_queues_batch`: one round trip resolving
//! every distinct `target_workflow_name` needing a lookup in the batch,
//! via `workflow_name = ANY($1)`. `harvest_schedules_workflow_name_unique`
//! (issue #91's migration) means the batched query cannot return more
//! than one row per name. So the per-name answer is unchanged -- only the
//! round-trip count moves, from one per row needing a lookup to exactly
//! one per scan tick. It falls back to the original per-row path only if
//! the batch attempt itself could not run, e.g. no default-shard pool.
//!
//! Evidence here is `pg_stat_statements` call and buffer counts, never
//! wall-clock. Wall-clock is not admissible on a shared-vCPU machine.
//! This harness follows the same shape as `activity_enqueue_batch_perf.rs`.
//! It uses a fresh, uniquely-named, fully-migrated database per
//! measurement point. `pg_stat_statements` is reset immediately before
//! the measured call, and snapshotted immediately after it.

#![allow(clippy::too_many_lines)]

use std::collections::HashMap;

use autumn_harvest::completion_trigger::enforce_completion_triggers_outbox;
use autumn_harvest::models::NewCompletionTriggerOutboxDb;
use autumn_harvest::schema::{harvest_completion_trigger_outbox, harvest_schedules};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── DB bootstrap (mirrors activity_enqueue_batch_perf.rs) ──────────────────

type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
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

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("failed to build test pool")
}

/// Seeds `count` unrelated DAG-kind `harvest_schedules` rows. This gives
/// the table the batched query filters against realistic cardinality,
/// not a toy table sized to exactly what the test needs.
async fn seed_unrelated_dag_schedules(conn: &mut AsyncPgConnection, count: usize) {
    for i in 0..count {
        diesel::sql_query(
            "INSERT INTO harvest_schedules (dag_name, schedule_expr) VALUES ($1, '0 * * * *')",
        )
        .bind::<diesel::sql_types::Text, _>(format!("unrelated_dag_{i}"))
        .execute(conn)
        .await
        .expect("seed unrelated dag schedule");
    }
}

/// Seeds a workflow-kind schedule row (the row `resolve_target_queue`'s
/// lookup is written to find) for `workflow_name`, carrying `queue_name`.
async fn seed_workflow_schedule(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    queue_name: &str,
) {
    diesel::insert_into(harvest_schedules::table)
        .values((
            harvest_schedules::workflow_name.eq(Some(workflow_name.to_string())),
            harvest_schedules::queue_name.eq(Some(queue_name.to_string())),
            harvest_schedules::schedule_expr.eq(Some("0 * * * *".to_string())),
        ))
        .execute(conn)
        .await
        .expect("seed workflow schedule");
}

/// Builds `n` production-shaped outbox rows for one scan batch. Every row
/// has `queue_name = NULL` (the common, un-overridden case) and
/// `next_attempt_at = NULL` (fresh tier). Every row also has
/// `target_shard = 1`: cross-shard is the only case the outbox exists
/// for. Rows cycle through `distinct_names` target workflow names -- the
/// fan-in skew a real completion-trigger deployment produces when many
/// source executions share one downstream trigger.
fn build_outbox_rows(n: usize, distinct_names: &[String]) -> Vec<NewCompletionTriggerOutboxDb> {
    (0..n)
        .map(|i| NewCompletionTriggerOutboxDb {
            source_exec_id: Uuid::new_v4(),
            trigger_id: Uuid::new_v4(),
            target_shard: 1,
            target_workflow_name: distinct_names[i % distinct_names.len()].clone(),
            target_workflow_id: format!("outbox_perf_target_{i}"),
            target_input: json!({"outbox_perf_index": i}),
            queue_name: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: json!("Normal"),
            max_workflow_input_bytes: 1_048_576,
        })
        .collect()
}

// ── pg_stat_statements capture (mirrors activity_enqueue_batch_perf.rs) ────

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

/// Whether `row` is the `harvest_schedules` lookup this investigation
/// targets -- the per-row `resolve_target_queue` SELECT before the fix, the
/// batched `resolve_target_queues_batch` SELECT after it. Both shapes filter
/// `workflow_name` against `harvest_schedules`, so one predicate covers both
/// sides of the before/after comparison.
fn is_schedule_lookup_statement(row: &StatRow) -> bool {
    // Diesel quotes identifiers (`FROM "harvest_schedules"`), so match
    // loosely rather than assume unquoted `FROM harvest_schedules`.
    let q = row.query.to_ascii_lowercase();
    q.contains("harvest_schedules") && q.contains("workflow_name")
}

/// One human-readable profile line: `calls`/`buffers` plus each one's share
/// of the point's totals, and a whitespace-collapsed, truncated query text.
/// Precision loss from `i64 as f64` is immaterial here. These are
/// percentages over call/buffer counts in the hundreds, nowhere near
/// f64's 52-bit mantissa limit. The result is a debug/artifact string,
/// not a value anything computes from.
#[allow(clippy::cast_precision_loss)]
fn fmt_row(r: &StatRow, total_calls: i64, total_buffers: i64) -> String {
    let query: String = r.query.split_whitespace().collect::<Vec<_>>().join(" ");
    let query = if query.len() > 90 {
        format!("{}...", &query[..90])
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

async fn wal_bytes(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct WalRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        bytes: i64,
    }
    let row: WalRow =
        diesel::sql_query("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint AS bytes")
            .get_result(conn)
            .await
            .expect("read WAL insert position");
    row.bytes
}

// ── Direct measurement: enforce_completion_triggers_outbox ─────────────────

struct SizePoint {
    n: i64,
    lookup_calls: i64,
    lookup_buffers: i64,
    total_calls: i64,
    total_buffers: i64,
    wal_bytes: i64,
    processed: usize,
    /// Every statement `enforce_completion_triggers_outbox` issued for
    /// this point, ranked by buffers then by calls. This is the profile
    /// the charter's "profile before hypothesis" step asks for, captured
    /// alongside the direct measurement rather than as a separate run.
    profile_by_buffers: Vec<String>,
    profile_by_calls: Vec<String>,
}

/// One measurement point: `n` outbox rows across `DISTINCT_NAMES` target
/// workflow names. Three of the names carry a matching `harvest_schedules`
/// row, exercising the match branch. Two do not, exercising the
/// `default_workflow_queue()` fallback branch. Both branches behave
/// identically before and after the fix.
const DISTINCT_NAMES: usize = 5;
const NAMES_WITH_SCHEDULE: usize = 3;

async fn measure_one_batch(admin: &str, _label: &str, n: usize) -> SizePoint {
    // Postgres identifiers truncate silently at 63 bytes (`NAMEDATALEN`).
    // A `label`+`n`-qualified prefix here made the "_shard0"/"_shard1"
    // suffixes collide after truncation, so the second `CREATE DATABASE`
    // silently targeted the first database again. Keep the physical name
    // short, and let `unique()`'s UUID carry all the uniqueness a test
    // database needs; `label`/`n` still tag the point in-memory below.
    let db_id = unique("otbq");
    let default_db = format!("{db_id}_s0");
    let target_db = format!("{db_id}_s1");
    let default_url = create_fresh_db(admin, &default_db).await;
    let target_url = create_fresh_db(admin, &target_db).await;

    let mut seed_conn = AsyncPgConnection::establish(&default_url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;

    let names: Vec<String> = (0..DISTINCT_NAMES)
        .map(|i| unique(&format!("outbox_perf_wf_{i}")))
        .collect();
    seed_unrelated_dag_schedules(&mut seed_conn, 300).await;
    for name in names.iter().take(NAMES_WITH_SCHEDULE) {
        seed_workflow_schedule(&mut seed_conn, name, "outbox-perf-priority-queue").await;
    }

    let rows = build_outbox_rows(n, &names);
    diesel::insert_into(harvest_completion_trigger_outbox::table)
        .values(&rows)
        .execute(&mut seed_conn)
        .await
        .expect("seed outbox rows");

    let default_pool = build_test_pool(&default_url);
    let target_pool = build_test_pool(&target_url);
    let mut pools = std::collections::BTreeMap::new();
    pools.insert(ShardId::new(0), default_pool);
    pools.insert(ShardId::new(1), target_pool);
    let sharded_pool = Some(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let mut op_conn = AsyncPgConnection::establish(&default_url)
        .await
        .expect("op connection");
    let mut stats_conn = AsyncPgConnection::establish(&default_url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &default_db).await;

    // The real public entry point under test: the outbox scanner, exactly
    // as `timeout.rs`'s periodic sweep calls it.
    let wal_before = wal_bytes(&mut stats_conn).await;
    let processed = enforce_completion_triggers_outbox(
        &mut op_conn,
        &NoOpMetrics,
        &sharded_pool,
        &[ShardId::new(1)],
    )
    .await
    .expect("enforce_completion_triggers_outbox should succeed");
    let wal_after = wal_bytes(&mut stats_conn).await;

    let all_rows = snapshot_statements(&mut stats_conn, &default_db).await;
    let lookup_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_schedule_lookup_statement(r))
        .collect();
    let lookup_calls: i64 = lookup_rows.iter().map(|r| r.calls).sum();
    let lookup_buffers: i64 = lookup_rows.iter().map(|r| r.total_buffers).sum();
    let total_calls: i64 = all_rows.iter().map(|r| r.calls).sum();
    let total_buffers: i64 = all_rows.iter().map(|r| r.total_buffers).sum();

    // `snapshot_statements` already orders by total_buffers DESC.
    let profile_by_buffers: Vec<String> = all_rows
        .iter()
        .take(10)
        .map(|r| fmt_row(r, total_calls, total_buffers))
        .collect();
    let mut by_calls: Vec<&StatRow> = all_rows.iter().collect();
    by_calls.sort_by(|a, b| b.calls.cmp(&a.calls));
    let profile_by_calls: Vec<String> = by_calls
        .iter()
        .take(10)
        .map(|r| fmt_row(r, total_calls, total_buffers))
        .collect();

    SizePoint {
        n: i64::try_from(n).unwrap(),
        lookup_calls,
        lookup_buffers,
        total_calls,
        total_buffers,
        wal_bytes: wal_after - wal_before,
        processed,
        profile_by_buffers,
        profile_by_calls,
    }
}

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-completion-trigger-outbox-queue.md"]
async fn zz_capture_completion_trigger_outbox_queue_perf_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("completion-trigger-outbox-queue");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut lines = vec![format!(
        "-- {label}: outbox relay queue-name resolution, pg_stat_statements sweep --\n\
         n\tprocessed\tlookup_calls\tlookup_buffers\ttotal_calls\ttotal_buffers\twal_bytes"
    )];
    let mut headline_profile: Option<(Vec<String>, Vec<String>)> = None;
    for n in [5_usize, 20, 50] {
        let point = measure_one_batch(&admin, &label, n).await;
        eprintln!(
            "label={label} n={} processed={} lookup_calls={} lookup_buffers={} total_calls={} \
             total_buffers={} wal_bytes={}",
            point.n,
            point.processed,
            point.lookup_calls,
            point.lookup_buffers,
            point.total_calls,
            point.total_buffers,
            point.wal_bytes
        );
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            point.n,
            point.processed,
            point.lookup_calls,
            point.lookup_buffers,
            point.total_calls,
            point.total_buffers,
            point.wal_bytes
        ));
        if n == 50 {
            headline_profile = Some((point.profile_by_buffers, point.profile_by_calls));
        }
    }
    if let Some((by_buffers, by_calls)) = headline_profile {
        let mut profile_lines = vec![format!(
            "-- {label}: headline scenario (n=50), top statements by buffers --"
        )];
        profile_lines.extend(by_buffers);
        profile_lines.push(String::new());
        profile_lines.push(format!(
            "-- {label}: headline scenario (n=50), top statements by calls --"
        ));
        profile_lines.extend(by_calls);
        std::fs::write(
            out_dir.join(format!("{label}-profile-n50.txt")),
            profile_lines.join("\n") + "\n",
        )
        .expect("write profile artifact");
    }
    std::fs::write(
        out_dir.join(format!("{label}-sweep.txt")),
        lines.join("\n") + "\n",
    )
    .expect("write sweep artifact");
    eprintln!("evidence capture complete: label={label}");
}

// ── Equivalence: batched resolution matches the per-row resolve exactly ────

/// Proves the fix resolves the identical `queue_name` for every task the
/// per-row `resolve_target_queue` path would have. It covers both the
/// schedule-match branch and the `default_workflow_queue()` fallback
/// branch, by reading back what each task actually started with.
#[tokio::test]
async fn outbox_scan_resolves_the_same_queue_with_or_without_batching() {
    let (admin, _guard) = setup_server().await;
    let db_id = unique("outbox_queue_equiv");
    let default_url = create_fresh_db(&admin, &format!("{db_id}_shard0")).await;
    let target_url = create_fresh_db(&admin, &format!("{db_id}_shard1")).await;

    let mut seed_conn = AsyncPgConnection::establish(&default_url)
        .await
        .expect("seed connection");
    let matched_name = unique("outbox_equiv_matched_wf");
    let unmatched_name = unique("outbox_equiv_unmatched_wf");
    seed_workflow_schedule(&mut seed_conn, &matched_name, "equiv-priority-queue").await;

    let rows = vec![
        NewCompletionTriggerOutboxDb {
            source_exec_id: Uuid::new_v4(),
            trigger_id: Uuid::new_v4(),
            target_shard: 1,
            target_workflow_name: matched_name.clone(),
            target_workflow_id: "equiv_target_matched".to_string(),
            target_input: Value::Null,
            queue_name: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: json!("Normal"),
            max_workflow_input_bytes: 1_048_576,
        },
        NewCompletionTriggerOutboxDb {
            source_exec_id: Uuid::new_v4(),
            trigger_id: Uuid::new_v4(),
            target_shard: 1,
            target_workflow_name: unmatched_name.clone(),
            target_workflow_id: "equiv_target_unmatched".to_string(),
            target_input: Value::Null,
            queue_name: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: json!("Normal"),
            max_workflow_input_bytes: 1_048_576,
        },
    ];
    diesel::insert_into(harvest_completion_trigger_outbox::table)
        .values(&rows)
        .execute(&mut seed_conn)
        .await
        .expect("seed outbox rows");

    let default_pool = build_test_pool(&default_url);
    let target_pool = build_test_pool(&target_url);
    let mut pools = std::collections::BTreeMap::new();
    pools.insert(ShardId::new(0), default_pool);
    pools.insert(ShardId::new(1), target_pool);
    let sharded_pool = Some(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let mut op_conn = AsyncPgConnection::establish(&default_url)
        .await
        .expect("op connection");
    let processed = enforce_completion_triggers_outbox(
        &mut op_conn,
        &NoOpMetrics,
        &sharded_pool,
        &[ShardId::new(1)],
    )
    .await
    .expect("enforce_completion_triggers_outbox should succeed");
    assert_eq!(processed, 2, "both outbox rows should relay successfully");

    let mut target_conn = AsyncPgConnection::establish(&target_url)
        .await
        .expect("target connection");
    let queues = started_queue_names(&mut target_conn).await;
    assert_eq!(
        queues.get("equiv_target_matched").map(String::as_str),
        Some("equiv-priority-queue"),
        "a row whose target workflow has a matching schedule must resolve that schedule's queue"
    );
    assert_eq!(
        queues.get("equiv_target_unmatched").map(String::as_str),
        Some("default"),
        "a row whose target workflow has no schedule must fall back to the default queue"
    );
}

async fn started_queue_names(conn: &mut AsyncPgConnection) -> HashMap<String, String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
    }
    let rows: Vec<Row> =
        diesel::sql_query("SELECT workflow_id, queue_name FROM harvest_workflow_executions")
            .load(conn)
            .await
            .expect("load started executions");
    rows.into_iter()
        .map(|r| (r.workflow_id, r.queue_name))
        .collect()
}

#[tokio::test]
async fn outbox_scan_on_no_pending_rows_is_a_no_op() {
    let (admin, _guard) = setup_server().await;
    let default_url = create_fresh_db(&admin, &unique("outbox_queue_empty")).await;
    let mut conn = AsyncPgConnection::establish(&default_url)
        .await
        .expect("connect");
    let sharded_pool = Some(ShardedDbPool::single(build_test_pool(&default_url)));
    let processed = enforce_completion_triggers_outbox(
        &mut conn,
        &NoOpMetrics,
        &sharded_pool,
        &[ShardId::new(0)],
    )
    .await
    .expect("enforce_completion_triggers_outbox should succeed on an empty outbox");
    assert_eq!(processed, 0);
}
