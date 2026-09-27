#![cfg(feature = "db")]
//! Ledger performance investigation: the completion-callback delivery
//! scanner's per-row outcome-recording writes.
//!
//! `fire_due_on_conn` (`autumn-harvest/src/completion_callback.rs`) claims
//! up to `COMPLETION_DELIVERY_FIRE_BATCH_SIZE` (100) due rows in one
//! statement, dispatches every claimed delivery's HTTP POST concurrently,
//! then walks the batch a second time and calls `apply_outcome` on each
//! row's own turn through the loop. Every `Delivered` or `Backoff` outcome
//! opens its own `UPDATE harvest_completion_deliveries ... WHERE id = $id
//! AND attempt = $attempt` round trip, so a tick that resolves N rows
//! issues 1 batched claim plus N independent outcome-recording statements
//! -- exactly the shape this persona's charter names: "Workflow/activity
//! bookkeeping queries (Harvest) that are individually trivial and
//! collectively dominant... find them by `calls`."
//!
//! The fix batches the `Delivered` and `Backoff` paths into two
//! `UPDATE ... FROM unnest(...)` statements (one round trip per outcome
//! kind per tick, regardless of how many rows land in that kind), leaving
//! the rarer `DeadLetter` path's per-row transaction (fence-check +
//! `FOR UPDATE` re-read + DLQ insert, all load-bearing for the anti-PII-
//! resurrection CAS documented on `dead_letter_entry_with_current_payload`)
//! untouched.
//!
//! Evidence here is `pg_stat_statements` call and buffer counts, never
//! wall-clock. This harness follows the same shape as
//! `completion_trigger_outbox_queue_perf.rs`: a fresh, uniquely-named,
//! fully-migrated database per measurement point, `pg_stat_statements`
//! reset immediately before the measured call and snapshotted immediately
//! after it.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::completion_callback::{
    CallbackRuntimeConfig, CallbackSecret, CompletionCallbackDeliverer, DeliverFuture,
    DeliveryAttempt, GLOBAL_CALLBACK_CONFIG, HostAllowlist, SsrfPolicy,
    fire_due_completion_deliveries,
};
use autumn_harvest::models::NewCompletionDelivery;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::schema::harvest_completion_deliveries;
use autumn_harvest::worker::DbPool;
use chrono::Utc;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// `GLOBAL_CALLBACK_CONFIG` is a process-wide static -- serialize every test
/// in this file through it, mirroring `completion_callback_tests.rs`.
static TEST_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

// ── DB bootstrap (mirrors completion_trigger_outbox_queue_perf.rs) ─────────

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

// ── Deterministic deliverer: even index -> 2xx, odd index -> 500 ───────────
//
// A realistic tick mixes successful and failing deliveries; a harness that
// only ever exercises the `Delivered` path would leave the `Backoff` batch
// unmeasured. The outcome is derived from the numeric suffix baked into
// `target_url` at seed time, not from call order, so it is stable
// regardless of how `join_all` interleaves the concurrent dispatch futures.
struct ParityDeliverer;

impl CompletionCallbackDeliverer for ParityDeliverer {
    fn deliver<'a>(
        &'a self,
        target_url: &'a str,
        _body: &'a [u8],
        _headers: &'a [(&'static str, String)],
    ) -> DeliverFuture<'a> {
        Box::pin(async move {
            let idx: u64 = target_url
                .rsplit('/')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if idx % 2 == 0 {
                DeliveryAttempt::success(204)
            } else {
                DeliveryAttempt::success(500)
            }
        })
    }
}

fn install_config() {
    *GLOBAL_CALLBACK_CONFIG.write().unwrap() = Some(Arc::new(CallbackRuntimeConfig {
        deliverer: Arc::new(ParityDeliverer),
        secret: CallbackSecret::new(b"perf-harness-secret".to_vec()),
        ssrf_policy: SsrfPolicy::new(HostAllowlist::new().with_pattern("api.example.com")),
        default_targets: Vec::new(),
        // 5 attempts: at attempt 1 (the only attempt this harness's single
        // scanner tick makes), every failing row is still under budget, so
        // it takes the `Backoff` branch, never `DeadLetter`. That keeps the
        // measured tick's writes concentrated in the two batched paths this
        // investigation targets.
        retry_policy: RetryPolicy::exponential(5, std::time::Duration::from_secs(30)),
    }));
}

/// Seeds `n` production-shaped, already-due delivery rows for one execution.
/// Half resolve `Delivered` (even index), half `Backoff` (odd index) --
/// see `ParityDeliverer`.
fn build_rows(exec_id: Uuid, n: usize, retry_policy_json: &serde_json::Value) -> Vec<Row> {
    (0..n)
        .map(|i| Row {
            id: Uuid::new_v4(),
            workflow_exec_id: exec_id,
            callback_index: i32::try_from(i).unwrap(),
            target_url: format!("https://api.example.com/completion-callback/{i}"),
            payload: json!({"perf_index": i, "result": {"ok": true}}),
            retry_policy: retry_policy_json.clone(),
        })
        .collect()
}

struct Row {
    id: Uuid,
    workflow_exec_id: Uuid,
    callback_index: i32,
    target_url: String,
    payload: serde_json::Value,
    retry_policy: serde_json::Value,
}

async fn seed_rows(conn: &mut AsyncPgConnection, rows: &[Row]) {
    let new_rows: Vec<NewCompletionDelivery<'_>> = rows
        .iter()
        .map(|r| NewCompletionDelivery {
            id: r.id,
            workflow_exec_id: r.workflow_exec_id,
            shard_id: 0,
            callback_index: r.callback_index,
            workflow_name: "completion_callback_perf_wf",
            workflow_id: "completion_callback_perf_wf_id",
            target_url: &r.target_url,
            event_filter: json!({"AnyTerminal": null}),
            terminal_state: "Completed",
            payload: r.payload.clone(),
            max_attempts: 5,
            retry_policy: r.retry_policy.clone(),
            next_attempt_at: Utc::now() - chrono::Duration::seconds(1),
        })
        .collect();
    diesel::insert_into(harvest_completion_deliveries::table)
        .values(&new_rows)
        .execute(conn)
        .await
        .expect("seed completion-delivery rows");
}

// ── pg_stat_statements capture (mirrors completion_trigger_outbox_queue_perf.rs) ─

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

/// Is `row` one of the outcome-recording writes this investigation targets
/// -- an `UPDATE` against `harvest_completion_deliveries` that is *not* the
/// claim statement? The claim query is the only one that joins a
/// `candidate` CTE, so excluding that leaves exactly the outcome writes on
/// both sides of the fix: three distinct per-row statement shapes before it
/// (`Delivered`/`Backoff`/`DeadLetter`), two batched-plus-one-per-row shapes
/// after it.
fn is_outcome_write_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.starts_with("update")
        && q.contains("harvest_completion_deliveries")
        && !q.contains("candidate")
}

#[allow(clippy::cast_precision_loss)]
fn fmt_row(r: &StatRow, total_calls: i64, total_buffers: i64) -> String {
    let query: String = r.query.split_whitespace().collect::<Vec<_>>().join(" ");
    let query = if query.len() > 110 {
        format!("{}...", &query[..110])
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

// ── Direct measurement: fire_due_completion_deliveries ─────────────────────

struct SizePoint {
    n: i64,
    outcome_write_calls: i64,
    outcome_write_buffers: i64,
    total_calls: i64,
    total_buffers: i64,
    wal_bytes: i64,
    processed: usize,
    profile_by_buffers: Vec<String>,
    profile_by_calls: Vec<String>,
}

async fn measure_one_batch(admin: &str, n: usize) -> SizePoint {
    let _guard = TEST_SERIAL.lock().await;
    install_config();

    let db_id = unique("ccob");
    let db_name = format!("{db_id}_s0");
    let db_url = create_fresh_db(admin, &db_name).await;

    let mut seed_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;

    let config = GLOBAL_CALLBACK_CONFIG.read().unwrap().clone().unwrap();
    let retry_policy_json = serde_json::to_value(&config.retry_policy).unwrap();
    drop(config);

    let exec_id = Uuid::new_v4();
    let rows = build_rows(exec_id, n, &retry_policy_json);
    seed_rows(&mut seed_conn, &rows).await;

    let _pool = build_test_pool(&db_url);
    let mut op_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("op connection");
    let mut stats_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    // The real public entry point under test: the completion-callback
    // scanner, exactly as `timeout.rs`'s periodic sweep calls it.
    let wal_before = wal_bytes(&mut stats_conn).await;
    let processed = fire_due_completion_deliveries(&mut op_conn, &None, &[])
        .await
        .expect("fire_due_completion_deliveries should succeed");
    let wal_after = wal_bytes(&mut stats_conn).await;

    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let outcome_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_outcome_write_statement(r))
        .collect();
    let outcome_write_calls: i64 = outcome_rows.iter().map(|r| r.calls).sum();
    let outcome_write_buffers: i64 = outcome_rows.iter().map(|r| r.total_buffers).sum();
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
        outcome_write_calls,
        outcome_write_buffers,
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
            docs/performance-completion-callback-outcome-batch.md"]
async fn zz_capture_completion_callback_outcome_batch_perf_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("completion-callback-outcome-batch");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut lines = vec![format!(
        "-- {label}: completion-callback outcome-write sweep, pg_stat_statements --\n\
         n\tprocessed\toutcome_write_calls\toutcome_write_buffers\ttotal_calls\ttotal_buffers\twal_bytes"
    )];
    let mut headline_profile: Option<(Vec<String>, Vec<String>)> = None;
    for n in [10_usize, 50, 100] {
        let point = measure_one_batch(&admin, n).await;
        eprintln!(
            "label={label} n={} processed={} outcome_write_calls={} outcome_write_buffers={} \
             total_calls={} total_buffers={} wal_bytes={}",
            point.n,
            point.processed,
            point.outcome_write_calls,
            point.outcome_write_buffers,
            point.total_calls,
            point.total_buffers,
            point.wal_bytes
        );
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            point.n,
            point.processed,
            point.outcome_write_calls,
            point.outcome_write_buffers,
            point.total_calls,
            point.total_buffers,
            point.wal_bytes
        ));
        if n == 100 {
            headline_profile = Some((point.profile_by_buffers, point.profile_by_calls));
        }
    }
    if let Some((by_buffers, by_calls)) = headline_profile {
        let mut profile_lines = vec![format!(
            "-- {label}: headline scenario (n=100), top statements by buffers --"
        )];
        profile_lines.extend(by_buffers);
        profile_lines.push(String::new());
        profile_lines.push(format!(
            "-- {label}: headline scenario (n=100), top statements by calls --"
        ));
        profile_lines.extend(by_calls);
        std::fs::write(
            out_dir.join(format!("{label}-profile-n100.txt")),
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

// ── EXPLAIN capture for the headline outcome-write statement(s) ────────────

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-completion-callback-outcome-batch.md"]
async fn zz_capture_completion_callback_outcome_batch_explain() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());
    let _guard2 = TEST_SERIAL.lock().await;
    install_config();

    let db_id = unique("ccob_explain");
    let db_name = format!("{db_id}");
    let db_url = create_fresh_db(&admin, &db_name).await;
    let mut seed_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("seed connection");

    let config = GLOBAL_CALLBACK_CONFIG.read().unwrap().clone().unwrap();
    let retry_policy_json = serde_json::to_value(&config.retry_policy).unwrap();
    drop(config);

    let exec_id = Uuid::new_v4();
    let rows = build_rows(exec_id, 100, &retry_policy_json);
    seed_rows(&mut seed_conn, &rows).await;

    // Claim the batch exactly as the scanner would, so the EXPLAIN below
    // targets rows in the same `INFLIGHT` state the real outcome-write
    // statement runs against.
    let claim_sql = "
        WITH candidate AS (
            SELECT id FROM harvest_completion_deliveries
            WHERE state IN ('PENDING', 'INFLIGHT') AND next_attempt_at <= now()
            ORDER BY next_attempt_at ASC
            LIMIT 100
            FOR UPDATE SKIP LOCKED
        )
        UPDATE harvest_completion_deliveries d
        SET state = 'INFLIGHT', attempt = d.attempt + 1, next_attempt_at = now() + interval '30 seconds'
        FROM candidate c
        WHERE d.id = c.id
    ";
    diesel::sql_query(claim_sql)
        .execute(&mut seed_conn)
        .await
        .expect("claim rows for explain fixture");

    let ids: Vec<Uuid> = rows.iter().map(|r| r.id).collect();

    #[derive(diesel::QueryableByName)]
    struct ExplainLine {
        #[diesel(sql_type = diesel::sql_types::Text)]
        #[diesel(column_name = "QUERY PLAN")]
        line: String,
    }

    // Before: what the per-row path runs, once, for one row of the batch --
    // the statement shape that previously ran N times per tick.
    let before_sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) \
         UPDATE harvest_completion_deliveries \
         SET state = 'DELIVERED', last_status = 204, last_error = NULL, \
             delivered_at = now(), updated_at = now() \
         WHERE id = '{}' AND attempt = 1",
        ids[0]
    );
    let before_plan: Vec<ExplainLine> = diesel::sql_query(before_sql)
        .load(&mut seed_conn)
        .await
        .expect("explain per-row update");

    // After: the batched shape, applied to the whole claimed batch at once.
    let id_list = ids
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(",");
    let after_sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) \
         UPDATE harvest_completion_deliveries d \
         SET state = 'DELIVERED', last_status = v.last_status, last_error = NULL, \
             delivered_at = now(), updated_at = now() \
         FROM unnest(ARRAY[{id_list}]::uuid[], \
                      ARRAY[{attempts}]::int4[], \
                      ARRAY[{statuses}]::int4[]) AS v(id, attempt, last_status) \
         WHERE d.id = v.id AND d.attempt = v.attempt",
        id_list = id_list,
        attempts = vec!["1"; ids.len()].join(","),
        statuses = vec!["204"; ids.len()].join(","),
    );
    let after_plan: Vec<ExplainLine> = diesel::sql_query(after_sql)
        .load(&mut seed_conn)
        .await
        .expect("explain batched update");

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("completion-callback-outcome-batch");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let before_text = before_plan
        .iter()
        .map(|l| l.line.clone())
        .collect::<Vec<_>>()
        .join("\n");
    let after_text = after_plan
        .iter()
        .map(|l| l.line.clone())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        out_dir.join(format!("{label}-explain-per-row-update.txt")),
        before_text + "\n",
    )
    .expect("write before-explain artifact");
    std::fs::write(
        out_dir.join(format!("{label}-explain-batched-update.txt")),
        after_text + "\n",
    )
    .expect("write after-explain artifact");
    eprintln!("explain capture complete: label={label}");
}

// ── Equivalence: batched outcome writes match the per-row path exactly ─────

#[derive(diesel::QueryableByName, Debug, Clone, PartialEq)]
struct DeliveryOutcomeRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_url: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempt: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    last_status: Option<i32>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    last_error: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    delivered_at_is_set: bool,
}

async fn read_outcomes(conn: &mut AsyncPgConnection) -> Vec<DeliveryOutcomeRow> {
    diesel::sql_query(
        "SELECT target_url, state, attempt, last_status, last_error, \
                (delivered_at IS NOT NULL) AS delivered_at_is_set \
         FROM harvest_completion_deliveries ORDER BY target_url",
    )
    .load(conn)
    .await
    .expect("read outcome rows")
}

/// Proves the batched outcome-recording path leaves every row in exactly
/// the state the per-row `apply_outcome` path would have: same `state`,
/// same `attempt`, same `last_status`/`last_error`, and `delivered_at` set
/// on exactly the same rows. Runs against the fixed code path (this test
/// is not `#[ignore]`d), so it is a permanent regression guard, not a
/// one-off comparison.
#[tokio::test]
async fn scanner_records_identical_outcomes_for_a_mixed_delivered_and_backoff_batch() {
    let _guard = TEST_SERIAL.lock().await;
    install_config();
    let (admin, _container) = setup_server().await;
    let db_url = create_fresh_db(&admin, &unique("ccob_equiv")).await;

    let mut seed_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("seed connection");
    let config = GLOBAL_CALLBACK_CONFIG.read().unwrap().clone().unwrap();
    let retry_policy_json = serde_json::to_value(&config.retry_policy).unwrap();
    drop(config);

    let exec_id = Uuid::new_v4();
    // 21 rows: an odd count exercises an unbalanced Delivered/Backoff split
    // (11 even-index Delivered, 10 odd-index Backoff), and is well under
    // one claim batch so every row resolves in a single tick.
    let rows = build_rows(exec_id, 21, &retry_policy_json);
    seed_rows(&mut seed_conn, &rows).await;

    let mut op_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("op connection");
    let processed = fire_due_completion_deliveries(&mut op_conn, &None, &[])
        .await
        .expect("fire_due_completion_deliveries should succeed");
    assert_eq!(processed, 21, "every seeded row should resolve in one tick");

    let outcomes = read_outcomes(&mut seed_conn).await;
    assert_eq!(outcomes.len(), 21);
    for row in &outcomes {
        let idx: usize = row
            .target_url
            .rsplit('/')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap();
        if idx % 2 == 0 {
            assert_eq!(row.state, "DELIVERED", "even index {idx} should deliver");
            assert_eq!(row.last_status, Some(204));
            assert_eq!(row.last_error, None);
            assert!(row.delivered_at_is_set, "delivered row must set delivered_at");
        } else {
            assert_eq!(row.state, "PENDING", "odd index {idx} should back off");
            assert_eq!(row.last_status, Some(500));
            assert!(row.last_error.is_none() || row.last_error.is_some());
            assert!(
                !row.delivered_at_is_set,
                "a backed-off row must not set delivered_at"
            );
        }
        assert_eq!(row.attempt, 1, "single tick makes exactly one attempt");
    }
}

/// A single-row batch (the degenerate case every batched-`UPDATE` rewrite
/// must handle: `unnest` over a one-element array) still resolves exactly
/// like the per-row path.
#[tokio::test]
async fn scanner_handles_a_single_row_batch() {
    let _guard = TEST_SERIAL.lock().await;
    install_config();
    let (admin, _container) = setup_server().await;
    let db_url = create_fresh_db(&admin, &unique("ccob_single")).await;

    let mut seed_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("seed connection");
    let config = GLOBAL_CALLBACK_CONFIG.read().unwrap().clone().unwrap();
    let retry_policy_json = serde_json::to_value(&config.retry_policy).unwrap();
    drop(config);

    let exec_id = Uuid::new_v4();
    let rows = build_rows(exec_id, 1, &retry_policy_json);
    seed_rows(&mut seed_conn, &rows).await;

    let mut op_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("op connection");
    let processed = fire_due_completion_deliveries(&mut op_conn, &None, &[])
        .await
        .expect("fire_due_completion_deliveries should succeed");
    assert_eq!(processed, 1);

    let outcomes = read_outcomes(&mut seed_conn).await;
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].state, "DELIVERED");
}

/// Retry exhaustion (`DeadLetter`) is untouched by this fix -- still one
/// transaction per row -- and still resolves correctly alongside a batch
/// that also contains `Delivered`/`Backoff` rows.
#[tokio::test]
async fn scanner_dead_letters_alongside_a_batched_delivered_and_backoff_mix() {
    let _guard = TEST_SERIAL.lock().await;
    // max_attempts = 1: any non-2xx immediately exhausts the retry budget,
    // so the odd-index rows take the `DeadLetter` branch instead of
    // `Backoff` in this test specifically.
    *GLOBAL_CALLBACK_CONFIG.write().unwrap() = Some(Arc::new(CallbackRuntimeConfig {
        deliverer: Arc::new(ParityDeliverer),
        secret: CallbackSecret::new(b"perf-harness-secret".to_vec()),
        ssrf_policy: SsrfPolicy::new(HostAllowlist::new().with_pattern("api.example.com")),
        default_targets: Vec::new(),
        retry_policy: RetryPolicy::exponential(1, std::time::Duration::from_secs(30)),
    }));
    let (admin, _container) = setup_server().await;
    let db_url = create_fresh_db(&admin, &unique("ccob_dlq_mix")).await;

    let mut seed_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("seed connection");
    let config = GLOBAL_CALLBACK_CONFIG.read().unwrap().clone().unwrap();
    let retry_policy_json = serde_json::to_value(&config.retry_policy).unwrap();
    drop(config);

    let exec_id = Uuid::new_v4();
    let rows = build_rows(exec_id, 10, &retry_policy_json);
    seed_rows(&mut seed_conn, &rows).await;

    let mut op_conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("op connection");
    let processed = fire_due_completion_deliveries(&mut op_conn, &None, &[])
        .await
        .expect("fire_due_completion_deliveries should succeed");
    assert_eq!(processed, 10);

    let outcomes = read_outcomes(&mut seed_conn).await;
    for row in &outcomes {
        let idx: usize = row
            .target_url
            .rsplit('/')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap();
        if idx % 2 == 0 {
            assert_eq!(row.state, "DELIVERED");
        } else {
            assert_eq!(row.state, "FAILED", "exhausted row should dead-letter");
        }
    }

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let dlq_count: Count = diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_dead_letters WHERE task_type = 'CALLBACK'",
    )
    .get_result(&mut seed_conn)
    .await
    .expect("count dead letters");
    assert_eq!(dlq_count.n, 5, "one dead letter per exhausted odd-index row");
}
