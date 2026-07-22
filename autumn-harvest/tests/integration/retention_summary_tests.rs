#![cfg(feature = "db")]
//! Tiered / summary retention behavioral tests — issue #752.
//!
//! Drives the real retention janitor end-to-end (`RetentionRuntime::spawn` +
//! `run_now`) against a Postgres instance and asserts the summary-demotion,
//! GC, and #495-erase interactions:
//!
//! - AC1: summary disabled -> execution hard-deleted, no summary row (today's
//!   behavior).
//! - AC2/AC3: a summary row is written in the SAME transaction as the delete,
//!   carrying identity/timing/shard/search-attrs; `summarized_count` reported.
//! - AC3 (opt-in payload): captured `result` stored verbatim when small, a
//!   typed `too_large` marker when over cap, and NULL when capture is off.
//! - AC3 (offload): an offloaded `output` becomes an `offloaded` marker, never
//!   the blob reference (#524).
//! - AC5: the summary GC pass deletes summaries past the summary horizon and
//!   emits `record_summary_deleted`; `Unbounded` keeps them forever.
//! - AC6: #495 PII erase scrubs a matching summary — both when the erase runs
//!   before GC (the summary captures the already-tombstoned output) and after
//!   GC (a summary-only erase succeeds and tombstones the row).
//! - AC7: a legal-held execution is neither deleted nor summarized until the
//!   hold is released.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (single-threaded, each test scrubs first); otherwise a
//! fresh testcontainers Postgres is booted with the full migration bundle
//! (`autumn_harvest::full_migrations_sql()`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::erase::erase_workflow_payloads;
use autumn_harvest::retention::{RetentionConfig, RetentionRuntime, SummaryPolicy};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Jsonb, Nullable, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

/// Capturing metrics recorder — records `(workflow, count)` from
/// `record_summary_deleted` (issue #752, AC5) alongside
/// `record_retention_deleted`. All other methods no-op via trait defaults.
#[derive(Default)]
struct CapturingMetrics {
    deleted: Mutex<Vec<(String, u64)>>,
    summary_deleted: Mutex<Vec<(String, u64)>>,
}

impl CapturingMetrics {
    fn summary_deleted(&self) -> Vec<(String, u64)> {
        self.summary_deleted.lock().unwrap().clone()
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_retention_deleted(&self, workflow: &str, count: u64) {
        self.deleted
            .lock()
            .unwrap()
            .push((workflow.to_string(), count));
    }

    fn record_summary_deleted(&self, workflow: &str, count: u64) {
        self.summary_deleted
            .lock()
            .unwrap()
            .push((workflow.to_string(), count));
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

/// Scrubs all executions (cascades events/tasks) plus summaries and the tables
/// retention touches, so a shared migrated DB stays isolated per test.
async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_execution_summaries",
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

/// Inserts a terminal execution (default COMPLETED) whose `started_at` is 5s
/// before `completed_at` (so `duration_ms == 5000`), with the given optional
/// output / error / `search_attrs`. Also inserts one trivial event.
#[allow(clippy::too_many_arguments)]
async fn insert_completed_full(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    completed_at: DateTime<Utc>,
    output: Option<serde_json::Value>,
    error: Option<&str>,
    search_attrs: Option<serde_json::Value>,
) -> uuid::Uuid {
    let started_at = completed_at - chrono::Duration::seconds(5);
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, output, error,
             search_attrs, started_at, completed_at)
         VALUES ($1, $2, 0, $3, '{\"secret\":\"pii\"}'::jsonb, $4, $5, $6, $7, $8)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .bind::<Nullable<Jsonb>, _>(output)
    .bind::<Nullable<Text>, _>(error)
    .bind::<Nullable<Jsonb>, _>(search_attrs)
    .bind::<Timestamptz, _>(started_at)
    .bind::<Timestamptz, _>(completed_at)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{\"secret\":\"pii\"}}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert event");
    id
}

/// Simple COMPLETED execution, no captured payload columns.
async fn insert_completed(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    completed_at: DateTime<Utc>,
) -> uuid::Uuid {
    insert_completed_full(
        conn,
        workflow_name,
        workflow_id,
        "COMPLETED",
        completed_at,
        None,
        None,
        None,
    )
    .await
}

/// Point `child`'s `parent_id` at `parent`.
async fn set_parent(conn: &mut AsyncPgConnection, child: uuid::Uuid, parent: uuid::Uuid) {
    diesel::sql_query("UPDATE harvest_workflow_executions SET parent_id = $2 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(child)
        .bind::<diesel::sql_types::Uuid, _>(parent)
        .execute(conn)
        .await
        .expect("set parent_id");
}

/// Read a summary row's `parent_id` (issue #752 erase-cascade link).
async fn summary_parent(conn: &mut AsyncPgConnection, exec_id: uuid::Uuid) -> Option<uuid::Uuid> {
    #[derive(diesel::QueryableByName)]
    struct ParentRow {
        #[diesel(sql_type = Nullable<diesel::sql_types::Uuid>)]
        parent_id: Option<uuid::Uuid>,
    }
    diesel::sql_query("SELECT parent_id FROM harvest_execution_summaries WHERE execution_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .get_result::<ParentRow>(conn)
        .await
        .expect("summary row must exist")
        .parent_id
}

/// Seed a summary row directly (for GC tests) with the given `completed_at`.
async fn insert_summary(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    completed_at: DateTime<Utc>,
) -> uuid::Uuid {
    let started_at = completed_at - chrono::Duration::seconds(5);
    diesel::sql_query(
        "INSERT INTO harvest_execution_summaries
            (execution_id, workflow_name, workflow_id, state, started_at,
             completed_at, duration_ms, shard_id, result)
         VALUES (gen_random_uuid(), $1, $2, 'COMPLETED', $3, $4, 5000, 0,
                 '{\"result\":\"secret\"}'::jsonb)
         RETURNING execution_id AS id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Timestamptz, _>(started_at)
    .bind::<Timestamptz, _>(completed_at)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert summary")
    .id
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn count_executions(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_workflow_executions")
        .get_result::<CountRow>(conn)
        .await
        .expect("count executions")
        .n
}

async fn count_summaries(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_execution_summaries")
        .get_result::<CountRow>(conn)
        .await
        .expect("count summaries")
        .n
}

#[derive(diesel::QueryableByName)]
struct SummaryRow {
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    workflow_id: String,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    duration_ms: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    shard_id: i32,
    #[diesel(sql_type = Nullable<Jsonb>)]
    search_attrs: Option<serde_json::Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    result: Option<serde_json::Value>,
    #[diesel(sql_type = Nullable<Text>)]
    error: Option<String>,
}

async fn load_summary(conn: &mut AsyncPgConnection, exec_id: uuid::Uuid) -> Option<SummaryRow> {
    diesel::sql_query(
        "SELECT workflow_name, workflow_id, state, duration_ms, shard_id,
                search_attrs, result, error
         FROM harvest_execution_summaries WHERE execution_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result::<SummaryRow>(conn)
    .await
    .ok()
}

/// Drives a single retention tick to completion and returns the shard-0 result.
async fn run_one_tick(
    pool: DbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
) -> autumn_harvest::retention::RetentionTickResult {
    let pools = ShardedDbPool::single(pool);
    let runtime = RetentionRuntime::spawn(pools, config, metrics, None, None)
        .expect("retention runtime should spawn when enabled");
    runtime.run_now();

    let mut result = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.ran_at.is_some()
        {
            result = Some(r.clone());
            break;
        }
    }
    runtime.shutdown();
    result.expect("retention tick did not report a result in time")
}

/// History-only base config (audit/schedule purges off).
fn history_only(max_age: Option<Duration>) -> RetentionConfig {
    RetentionConfig {
        max_age_secs: max_age.map(|d| d.as_secs()),
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        ..RetentionConfig::default()
    }
}

// AC1: summary disabled -> execution hard-deleted, no summary row (identical to
// today).
#[tokio::test]
async fn summary_disabled_deletes_and_writes_no_summary() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    insert_completed(&mut conn, "wf", "w1", old).await;

    let config = history_only(Some(Duration::from_secs(86_400))); // summary: None
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    assert_eq!(result.deleted_count, 1);
    assert_eq!(
        result.summarized_count, 0,
        "no summary created when disabled"
    );
    assert_eq!(count_executions(&mut conn).await, 0, "execution deleted");
    assert_eq!(count_summaries(&mut conn).await, 0, "no summary row");
}

// AC2 + AC3: a summary row is written in the same delete transaction, carrying
// identity/timing/shard/search-attrs. summarized_count reported.
#[tokio::test]
async fn delete_demotes_into_a_summary_row() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    let exec_id = insert_completed_full(
        &mut conn,
        "orders",
        "o1",
        "FAILED",
        old,
        Some(serde_json::json!({"total": 42})),
        Some("boom"),
        Some(serde_json::json!({"tenant": "acme"})),
    )
    .await;

    // capture_payload OFF by default here: identity/timing only, no PII.
    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::for_days(30));
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    assert_eq!(result.deleted_count, 1);
    assert_eq!(result.summarized_count, 1, "one summary demoted");
    assert_eq!(count_executions(&mut conn).await, 0, "execution deleted");
    assert_eq!(count_summaries(&mut conn).await, 1, "summary written");

    let s = load_summary(&mut conn, exec_id).await.expect("summary row");
    assert_eq!(s.workflow_name, "orders");
    assert_eq!(s.workflow_id, "o1");
    assert_eq!(s.state, "FAILED");
    assert_eq!(
        s.duration_ms,
        Some(5000),
        "started_at 5s before completed_at"
    );
    assert_eq!(s.shard_id, 0);
    assert_eq!(s.search_attrs, Some(serde_json::json!({"tenant": "acme"})));
    // Payload capture OFF -> result/error NULL.
    assert!(s.result.is_none(), "no result captured when capture off");
    assert!(s.error.is_none(), "no error captured when capture off");
}

// AC3 (opt-in payload): capture on with a small payload -> verbatim result;
// over-cap -> a typed too_large marker; capture off -> NULL.
#[tokio::test]
async fn payload_capture_stores_verbatim_and_marks_oversized() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    let small_output = serde_json::json!({"ok": true, "n": 7});
    let small_id = insert_completed_full(
        &mut conn,
        "cap_small",
        "s1",
        "COMPLETED",
        old,
        Some(small_output.clone()),
        Some("short err"),
        None,
    )
    .await;
    let big_output = serde_json::json!({"blob": "x".repeat(500)});
    let big_id = insert_completed_full(
        &mut conn,
        "cap_big",
        "b1",
        "COMPLETED",
        old,
        Some(big_output),
        None,
        None,
    )
    .await;

    let config = history_only(Some(Duration::from_secs(86_400))).with_summary_retention(
        SummaryPolicy::for_days(30)
            .with_payload_capture()
            .with_max_payload_bytes(64),
    );
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;
    assert_eq!(result.summarized_count, 2);

    let small = load_summary(&mut conn, small_id)
        .await
        .expect("small summary");
    assert_eq!(
        small.result,
        Some(small_output),
        "small payload stored verbatim"
    );
    assert_eq!(small.error.as_deref(), Some("short err"));

    let big = load_summary(&mut conn, big_id).await.expect("big summary");
    let r = big.result.expect("result marker");
    assert_eq!(r["_harvest_omitted"], true);
    assert_eq!(r["reason"], "too_large");
    assert!(
        r["bytes"].as_u64().unwrap() > 64,
        "byte count of the omitted payload is reported"
    );
}

// AC3 (offload, #524): an offloaded `output` becomes an `offloaded` marker,
// NEVER the blob reference envelope.
#[tokio::test]
async fn offloaded_payload_is_marked_not_stored() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    let envelope = serde_json::json!({
        "_harvest_offload_envelope": 1,
        "store_id": "s3",
        "key": "blob/abc123",
        "len": 2_000_000,
        "checksum": "deadbeef",
    });
    let id = insert_completed_full(
        &mut conn,
        "offload_wf",
        "of1",
        "COMPLETED",
        old,
        Some(envelope),
        None,
        None,
    )
    .await;

    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::for_days(30).with_payload_capture());
    let metrics = Arc::new(CapturingMetrics::default());
    run_one_tick(pool, config, Arc::clone(&metrics)).await;

    let s = load_summary(&mut conn, id).await.expect("summary");
    let r = s.result.expect("result marker");
    assert_eq!(r["_harvest_omitted"], true);
    assert_eq!(r["reason"], "offloaded");
    assert!(
        r.get("key").is_none(),
        "the blob reference must never leak into the summary"
    );
}

// AC5: the summary GC pass deletes summaries past the horizon and emits the
// per-workflow metric; a young summary survives.
#[tokio::test]
async fn summary_gc_deletes_expired_and_emits_metric() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    // two old summaries of the same workflow, one young summary.
    insert_summary(&mut conn, "gc_wf", "g1", now - chrono::Duration::days(2)).await;
    insert_summary(&mut conn, "gc_wf", "g2", now - chrono::Duration::days(3)).await;
    let young = insert_summary(&mut conn, "gc_wf", "g3", now - chrono::Duration::minutes(5)).await;

    // History horizon present (so the janitor spawns) + a 1-hour summary
    // horizon. No executions exist, so history phase deletes nothing.
    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::for_duration(Duration::from_secs(3_600)));
    let metrics = Arc::new(CapturingMetrics::default());
    run_one_tick(pool, config, Arc::clone(&metrics)).await;

    assert_eq!(
        count_summaries(&mut conn).await,
        1,
        "only the young summary survives"
    );
    assert!(
        load_summary(&mut conn, young).await.is_some(),
        "young summary survives GC"
    );
    assert_eq!(
        metrics.summary_deleted(),
        vec![("gc_wf".to_string(), 2)],
        "summary GC emits the per-workflow deletion count"
    );
}

// AC5: Unbounded summary retention never GCs, even very old summaries.
#[tokio::test]
async fn unbounded_summary_retention_never_gcs() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    insert_summary(&mut conn, "keep", "k1", now - chrono::Duration::days(3650)).await;
    insert_summary(&mut conn, "keep", "k2", now - chrono::Duration::days(1000)).await;

    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::unbounded());
    let metrics = Arc::new(CapturingMetrics::default());
    run_one_tick(pool, config, Arc::clone(&metrics)).await;

    assert_eq!(
        count_summaries(&mut conn).await,
        2,
        "unbounded retention keeps summaries forever"
    );
    assert!(
        metrics.summary_deleted().is_empty(),
        "no summary GC deletions under Unbounded"
    );
}

// AC6 (a): an erase BEFORE the retention tick tombstones the execution output;
// the subsequent summary captures the already-tombstoned value (never re-exposes
// PII).
#[tokio::test]
async fn erase_before_gc_summary_captures_tombstone() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    let exec_id = insert_completed_full(
        &mut conn,
        "erase_wf",
        "e1",
        "COMPLETED",
        old,
        Some(serde_json::json!({"card": "4111-1111-1111-1111"})),
        None,
        Some(serde_json::json!({"pii": "yes"})),
    )
    .await;

    // Erase first — tombstones the execution row's output + search_attrs.
    erase_workflow_payloads(&mut conn, ExecutionId::from_uuid(exec_id), "test")
        .await
        .expect("erase should succeed on a terminal execution");

    // Then retention demotes it, capturing the (now tombstoned) output.
    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::for_days(30).with_payload_capture());
    let metrics = Arc::new(CapturingMetrics::default());
    run_one_tick(pool, config, Arc::clone(&metrics)).await;

    let s = load_summary(&mut conn, exec_id).await.expect("summary");
    assert_eq!(
        s.result,
        Some(serde_json::json!({"_harvest_erased": true})),
        "summary captures the tombstoned output, not the PII"
    );
    // The execution-row erase also tombstones search_attrs; the summary copies
    // that verbatim.
    assert_eq!(
        s.search_attrs,
        Some(serde_json::json!({"_harvest_erased": true}))
    );
}

// AC6 (b): a summary-only erase (execution row already GC'd) succeeds and
// tombstones the summary's result + search_attrs, leaving error intact.
#[tokio::test]
async fn erase_after_gc_scrubs_the_summary() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    let exec_id = insert_completed_full(
        &mut conn,
        "erase_after",
        "ea1",
        "FAILED",
        old,
        Some(serde_json::json!({"ssn": "123-45-6789"})),
        Some("operational error text"),
        Some(serde_json::json!({"pii": "yes"})),
    )
    .await;

    // Retention demotes it: the execution row is gone, only the summary (with
    // real PII) remains.
    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::for_days(30).with_payload_capture());
    let metrics = Arc::new(CapturingMetrics::default());
    run_one_tick(pool.clone(), config, Arc::clone(&metrics)).await;
    assert_eq!(count_executions(&mut conn).await, 0, "execution GC'd");
    let before = load_summary(&mut conn, exec_id)
        .await
        .expect("summary exists");
    assert_eq!(
        before.result,
        Some(serde_json::json!({"ssn": "123-45-6789"}))
    );

    // Now erase by execution id — the execution row is gone, so this is a
    // summary-only erase and must still succeed.
    let outcome = erase_workflow_payloads(&mut conn, ExecutionId::from_uuid(exec_id), "test")
        .await
        .expect("summary-only erase should succeed even after history GC");
    assert!(
        !outcome.execution_row_scrubbed,
        "execution row already gone"
    );
    assert!(outcome.summary_scrubbed, "the summary row was scrubbed");

    let after = load_summary(&mut conn, exec_id)
        .await
        .expect("summary still exists");
    assert_eq!(
        after.result,
        Some(serde_json::json!({"_harvest_erased": true})),
        "result tombstoned"
    );
    assert_eq!(
        after.search_attrs,
        Some(serde_json::json!({"_harvest_erased": true})),
        "search_attrs tombstoned"
    );
    assert_eq!(
        after.error.as_deref(),
        Some("operational error text"),
        "error deliberately kept (consistent with #495)"
    );
}

// Seed a lingering completion-delivery row whose frozen `payload` envelope
// carries the workflow's own output (PII), keyed to `exec_id`. Retention leaves
// non-DELIVERED deliveries behind, so this survives even after the execution
// row is summarized away.
async fn insert_lingering_delivery(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
    payload: serde_json::Value,
) {
    diesel::sql_query(
        "INSERT INTO harvest_completion_deliveries
            (workflow_exec_id, callback_index, workflow_name, workflow_id,
             target_url, event_filter, terminal_state, payload, state,
             max_attempts, retry_policy)
         VALUES ($1, 0, 'cb_wf', 'cb1', 'https://example.test/hook',
                 '{}'::jsonb, 'COMPLETED', $2, 'PENDING', 10, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .bind::<Jsonb, _>(payload)
    .execute(conn)
    .await
    .expect("insert lingering delivery");
}

// Seed a CALLBACK dead-letter row whose `input` carries the frozen envelope
// (PII), keyed to `exec_id`. Retention leaves CALLBACK DLQ rows behind.
async fn insert_callback_dlq(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
    input: serde_json::Value,
) {
    diesel::sql_query(
        "INSERT INTO harvest_dead_letters
            (original_task_id, queue_name, task_type, workflow_exec_id, input,
             error, attempts)
         VALUES (gen_random_uuid(), 'default', 'CALLBACK', $1, $2,
                 'delivery exhausted', 10)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .bind::<Jsonb, _>(input)
    .execute(conn)
    .await
    .expect("insert callback dlq");
}

#[derive(diesel::QueryableByName)]
struct PayloadRow {
    #[diesel(sql_type = Jsonb)]
    payload: serde_json::Value,
}

async fn load_delivery_payload(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
) -> serde_json::Value {
    diesel::sql_query(
        "SELECT payload FROM harvest_completion_deliveries WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result::<PayloadRow>(conn)
    .await
    .expect("delivery row must exist")
    .payload
}

#[derive(diesel::QueryableByName)]
struct DlqInputRow {
    #[diesel(sql_type = Jsonb)]
    input: serde_json::Value,
}

async fn load_callback_dlq_input(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
) -> serde_json::Value {
    diesel::sql_query(
        "SELECT input FROM harvest_dead_letters
         WHERE workflow_exec_id = $1 AND task_type = 'CALLBACK'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result::<DlqInputRow>(conn)
    .await
    .expect("callback dlq row must exist")
    .input
}

// AC6 (Codex P1): a summary-ONLY erase (execution row already GC'd) must ALSO
// scrub the lingering completion-callback PII — the frozen `payload` envelope
// in `harvest_completion_deliveries` and the CALLBACK `harvest_dead_letters
// .input` copy — both of which retention deliberately leaves behind and both of
// which are still retryable/redrivable. Before the fix the summary-only path
// hardcoded these counters to 0 and left the PII in place while returning 200.
#[tokio::test]
async fn erase_summary_only_scrubs_lingering_completion_callback_pii() {
    let (url, _c) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    // A summarized execution: the summary row exists, the execution row does
    // not. Seed the summary directly.
    let exec_id = insert_summary(&mut conn, "cb_wf", "cb1", Utc::now()).await;

    // The two lingering PII-bearing artifacts retention leaves behind, both
    // keyed to the (now-gone) execution id.
    let envelope = serde_json::json!({
        "execution_id": exec_id.to_string(),
        "state": "completed",
        "result": {"ssn": "123-45-6789", "email": "victim@example.com"},
    });
    insert_lingering_delivery(&mut conn, exec_id, envelope.clone()).await;
    insert_callback_dlq(&mut conn, exec_id, envelope.clone()).await;

    // Sanity: the PII is present before the erase.
    assert_eq!(load_delivery_payload(&mut conn, exec_id).await, envelope);
    assert_eq!(load_callback_dlq_input(&mut conn, exec_id).await, envelope);

    // Summary-only erase — the execution row is gone.
    let outcome = erase_workflow_payloads(&mut conn, ExecutionId::from_uuid(exec_id), "test")
        .await
        .expect("summary-only erase should succeed even after history GC");
    assert!(
        !outcome.execution_row_scrubbed,
        "execution row already gone"
    );
    assert!(outcome.summary_scrubbed, "the summary row was scrubbed");
    assert!(
        outcome.completion_deliveries_scrubbed > 0,
        "the lingering completion delivery must be scrubbed (was hardcoded 0)"
    );
    assert!(
        outcome.dead_letters_scrubbed > 0,
        "the lingering CALLBACK DLQ input must be scrubbed (was hardcoded 0)"
    );

    // The PII is gone from both tables, replaced by the erase tombstone.
    let tombstone = serde_json::json!({"_harvest_erased": true});
    assert_eq!(
        load_delivery_payload(&mut conn, exec_id).await,
        tombstone,
        "completion delivery payload tombstoned"
    );
    assert_eq!(
        load_callback_dlq_input(&mut conn, exec_id).await,
        tombstone,
        "CALLBACK dead-letter input tombstoned"
    );

    // The summary itself is tombstoned too.
    let after = load_summary(&mut conn, exec_id)
        .await
        .expect("summary still exists");
    assert_eq!(after.result, Some(tombstone));
}

// Regression guard for the shared-helper extraction (issue #752 Codex P1): the
// execution-EXISTS path must still scrub both callback-PII tables exactly as
// before.
#[tokio::test]
async fn erase_execution_exists_still_scrubs_callback_pii() {
    let (url, _c) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let exec_id = insert_completed_full(
        &mut conn,
        "cb_wf",
        "cb1",
        "COMPLETED",
        Utc::now(),
        Some(serde_json::json!({"ssn": "123-45-6789"})),
        None,
        None,
    )
    .await;

    let envelope = serde_json::json!({"result": {"ssn": "123-45-6789"}});
    insert_lingering_delivery(&mut conn, exec_id, envelope.clone()).await;
    insert_callback_dlq(&mut conn, exec_id, envelope).await;

    let outcome = erase_workflow_payloads(&mut conn, ExecutionId::from_uuid(exec_id), "test")
        .await
        .expect("erase should succeed on a terminal execution");
    assert!(outcome.execution_row_scrubbed);
    assert!(outcome.completion_deliveries_scrubbed > 0);
    assert!(outcome.dead_letters_scrubbed > 0);

    let tombstone = serde_json::json!({"_harvest_erased": true});
    assert_eq!(load_delivery_payload(&mut conn, exec_id).await, tombstone);
    assert_eq!(load_callback_dlq_input(&mut conn, exec_id).await, tombstone);
}

// AC7: a legal-held execution is neither deleted nor summarized; releasing the
// hold lets the next tick delete + summarize it.
#[tokio::test]
async fn legal_held_execution_is_not_summarized_until_released() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    let exec_id = insert_completed(&mut conn, "held_wf", "h1", old).await;
    // Place an indefinite legal hold.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions
         SET legal_hold_set_at = NOW(), legal_hold_reason = 'subpoena'
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(&mut conn)
    .await
    .expect("set hold");

    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::for_days(30));
    let metrics = Arc::new(CapturingMetrics::default());
    run_one_tick(pool.clone(), config.clone(), Arc::clone(&metrics)).await;

    assert_eq!(count_executions(&mut conn).await, 1, "held row not deleted");
    assert_eq!(
        count_summaries(&mut conn).await,
        0,
        "held row not summarized"
    );

    // Release the hold, tick again.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions
         SET legal_hold_set_at = NULL, legal_hold_reason = NULL WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(&mut conn)
    .await
    .expect("release hold");

    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;
    assert_eq!(result.deleted_count, 1);
    assert_eq!(result.summarized_count, 1);
    assert_eq!(count_executions(&mut conn).await, 0, "now deleted");
    assert_eq!(count_summaries(&mut conn).await, 1, "now summarized");
}

// AC6 (child-summary erase cascade, issue #752 review): a TERMINAL child is
// independently retention-eligible, so it is demoted into a summary (with its
// PII captured) and its execution row deleted BEFORE the parent is erased.
// Erasing the parent must reach the child summary via `parent_id`, or the
// child's captured PII survives undiscoverably. Also proves the `parent_id`
// link is preserved through deletion regardless of retention processing order.
#[tokio::test]
async fn erase_parent_scrubs_a_demoted_child_summary() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    let parent = insert_completed_full(
        &mut conn,
        "parent_wf",
        "p1",
        "COMPLETED",
        old,
        Some(serde_json::json!({"parent_card": "4111-1111-1111-1111"})),
        None,
        Some(serde_json::json!({"parent_pii": "yes"})),
    )
    .await;
    let child = insert_completed_full(
        &mut conn,
        "child_wf",
        "c1",
        "COMPLETED",
        old,
        Some(serde_json::json!({"child_ssn": "123-45-6789"})),
        Some("child operational error"),
        Some(serde_json::json!({"child_pii": "yes"})),
    )
    .await;
    set_parent(&mut conn, child, parent).await;

    // Demote BOTH into summaries with payload capture (child captures its PII).
    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::for_days(30).with_payload_capture());
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;
    assert_eq!(result.summarized_count, 2, "parent and child both demoted");
    assert_eq!(count_executions(&mut conn).await, 0, "both rows deleted");

    // The child→parent link survives deletion (order-independent).
    assert_eq!(
        summary_parent(&mut conn, child).await,
        Some(parent),
        "child summary retains parent_id after deletion"
    );

    // Sanity: the child summary still holds the real PII before the erase.
    let before = load_summary(&mut conn, child).await.expect("child summary");
    assert_eq!(
        before.result,
        Some(serde_json::json!({"child_ssn": "123-45-6789"}))
    );

    // Erase the PARENT — the child execution row is already gone, so the cascade
    // must reach the child SUMMARY via parent_id.
    let outcome = erase_workflow_payloads(&mut conn, ExecutionId::from_uuid(parent), "test")
        .await
        .expect("summary-only parent erase should succeed");
    assert!(outcome.summary_scrubbed, "parent summary scrubbed");
    assert_eq!(outcome.children.len(), 1, "reached the child summary");
    assert!(
        outcome.children[0].summary_scrubbed,
        "child summary scrubbed"
    );

    // The child summary's PII is now tombstoned; error deliberately kept (#495).
    let after = load_summary(&mut conn, child).await.expect("child summary");
    assert_eq!(
        after.result,
        Some(serde_json::json!({"_harvest_erased": true})),
        "child summary result tombstoned by the parent erase"
    );
    assert_eq!(
        after.search_attrs,
        Some(serde_json::json!({"_harvest_erased": true})),
        "child summary search_attrs tombstoned"
    );
    assert_eq!(
        after.error.as_deref(),
        Some("child operational error"),
        "child summary error kept (consistent with #495)"
    );

    // The parent summary itself is also tombstoned.
    let parent_after = load_summary(&mut conn, parent)
        .await
        .expect("parent summary");
    assert_eq!(
        parent_after.result,
        Some(serde_json::json!({"_harvest_erased": true}))
    );
}

// AC6 recursion: a grandchild demoted into a summary is reached when the
// top-level parent is erased (parent → child → grandchild summary links).
#[tokio::test]
async fn erase_parent_recurses_into_grandchild_summary() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    let parent = insert_completed_full(
        &mut conn,
        "gp_wf",
        "gp1",
        "COMPLETED",
        old,
        Some(serde_json::json!({"p": "x"})),
        None,
        None,
    )
    .await;
    let child = insert_completed_full(
        &mut conn,
        "gc_child_wf",
        "gc_c1",
        "COMPLETED",
        old,
        Some(serde_json::json!({"c": "x"})),
        None,
        None,
    )
    .await;
    let grandchild = insert_completed_full(
        &mut conn,
        "grandchild_wf",
        "g1",
        "COMPLETED",
        old,
        Some(serde_json::json!({"grandchild_secret": "top-secret"})),
        None,
        Some(serde_json::json!({"gc_pii": "yes"})),
    )
    .await;
    set_parent(&mut conn, child, parent).await;
    set_parent(&mut conn, grandchild, child).await;

    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_summary_retention(SummaryPolicy::for_days(30).with_payload_capture());
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;
    assert_eq!(result.summarized_count, 3, "all three demoted");
    assert_eq!(count_executions(&mut conn).await, 0);
    assert_eq!(summary_parent(&mut conn, grandchild).await, Some(child));

    erase_workflow_payloads(&mut conn, ExecutionId::from_uuid(parent), "test")
        .await
        .expect("erase parent");

    let gc = load_summary(&mut conn, grandchild)
        .await
        .expect("grandchild summary");
    assert_eq!(
        gc.result,
        Some(serde_json::json!({"_harvest_erased": true})),
        "grandchild summary reached recursively and tombstoned"
    );
    assert_eq!(
        gc.search_attrs,
        Some(serde_json::json!({"_harvest_erased": true}))
    );
}
