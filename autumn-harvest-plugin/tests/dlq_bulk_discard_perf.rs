//! Ledger performance investigation: `POST /dead-letters/discard`.
//!
//! `bulk_discard_dead_letters_for_selector` (in
//! `autumn-harvest-plugin/src/api.rs`) batches its own row selection
//! (`query_dead_letters_for_api_bulk`) up to `dlq::MAX_BULK_LIMIT` (1,000)
//! rows. It then discards the matches **one row at a time**. A
//! `for row in rows` loop issues one
//! `DELETE FROM harvest_dead_letters WHERE id = $1` per row. The library's
//! own embedder-facing `dlq::bulk_discard_dead_letters` had the identical
//! loop. Purging every dead letter that shares one broken activity's name
//! is the exact scenario `MAX_BULK_LIMIT` is sized for. That purge issued
//! up to 1,000 single-row `DELETE` statements in one HTTP request.
//!
//! The fix, `dlq::discard_dead_letters_batch`, replaces the loop at both
//! call sites with one `DELETE ... WHERE id = ANY($1) RETURNING id`. Unlike
//! `audit::insert_audit_batch` (a multi-row `INSERT` binding one column per
//! row), `id = ANY($1)` binds the whole id list as a single array
//! parameter. So there is no bound-parameter ceiling to chunk against even
//! at the 1,000-row cap.
//!
//! This file is the harness + evidence generator for that investigation.
//! See `docs/performance-dlq-bulk-discard.md`.

#![allow(clippy::too_many_lines)]

use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// ── DB bootstrap (same convention as schedule_bulk_audit_perf.rs) ──────────

type HarvestApiApp = axum::Router;
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

/// Creates a fresh, uniquely-named, fully-migrated database off `admin_url`.
/// This harness's fixture and `pg_stat_statements` capture cannot collide
/// with, or be polluted by, any other test or run on the same server.
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

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: HarvestDbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(pool);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn read_json_response(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    serde_json::from_slice(&body).expect("response must be JSON")
}

async fn post_json(app: &HarvestApiApp, uri: &str, payload: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .expect("valid JSON request"),
        )
        .await
        .expect("POST request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

// ── Fixture generation ──────────────────────────────────────────────────────

/// Seeds `matching` dead letters whose `activity_name` is
/// `dlq_bulk_perf_target`. The bulk-discard filter then selects exactly
/// this set -- an operator purging every dead letter left by one broken
/// activity. That is exactly the shape `dlq::MAX_BULK_LIMIT` is sized for
/// (issue #1421).
///
/// Also seeds `noise` unrelated dead letters spanning 25 other activity
/// names and 4 queues. `attempts` is skewed and `failed_at` spreads over 90
/// days. NULL density on `owner`/`severity` matches production rows -- both
/// columns are optional annotations most dead letters never get. Pure
/// set-based SQL, not a per-row Rust loop.
async fn seed_fixture(conn: &mut AsyncPgConnection, matching: i64, noise: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_dead_letters (
             id, original_task_id, queue_name, task_type, workflow_exec_id,
             activity_name, input, error, attempts, failed_at, owner, severity,
             workflow_name, quota_key
         )
         SELECT
             gen_random_uuid(), gen_random_uuid(), 'default', 'ACTIVITY', NULL,
             'dlq_bulk_perf_target',
             '{{\"test\": true}}'::jsonb,
             'activity dlq_bulk_perf_target exhausted retries',
             1 + (gs % 5),
             NOW() - (random() * interval '30 days'),
             CASE WHEN gs % 3 = 0 THEN 'team-payments' ELSE NULL END,
             CASE WHEN gs % 4 = 0 THEN 'high' ELSE NULL END,
             NULL, NULL
         FROM generate_series(1, {matching}) AS gs;

         INSERT INTO harvest_dead_letters (
             id, original_task_id, queue_name, task_type, workflow_exec_id,
             activity_name, input, error, attempts, failed_at, owner, severity,
             workflow_name, quota_key
         )
         SELECT
             gen_random_uuid(), gen_random_uuid(),
             (ARRAY['default', 'emails', 'billing', 'webhooks'])[1 + (gs % 4)],
             'ACTIVITY', NULL,
             'dlq_bulk_noise_' || (gs % 25),
             '{{\"test\": true}}'::jsonb,
             'noise failure ' || gs,
             1 + (gs % 8),
             NOW() - (random() * interval '90 days'),
             CASE WHEN gs % 5 = 0 THEN 'team-platform' ELSE NULL END,
             CASE WHEN gs % 6 = 0 THEN 'low' ELSE NULL END,
             NULL, NULL
         FROM generate_series(1, {noise}) AS gs;

         ANALYZE harvest_dead_letters;"
    ))
    .await
    .expect("seed dlq-bulk-discard perf fixture");
}

// ── pg_stat_statements capture (identical convention to
//    schedule_bulk_audit_perf.rs -- see that file's doc comments for why the
//    single-snapshot-query discipline matters) ──────────────────────────────

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

/// Whether `row` is the discard-delete statement shape this investigation
/// targets: a `DELETE` against `harvest_dead_letters`.
fn is_dead_letter_delete_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("delete from") && q.contains("harvest_dead_letters")
}

#[derive(diesel::QueryableByName, Debug)]
struct CountValue {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

// ── Evidence capture (not a CI assertion) ───────────────────────────────────

async fn capture_dlq_bulk_discard_evidence() {
    // MATCHING is the endpoint's own `dlq::MAX_BULK_LIMIT` -- the real
    // production ceiling on how many rows one bulk-discard request can act
    // on. It is not an arbitrary round number. NOISE models a lived-in DLQ
    // with 25 other activities' failures still sitting in it.
    const MATCHING: i64 = 1000;
    const NOISE: i64 = 4000;

    let (admin, _guard) = setup_server().await;
    let db_name = unique("dlq_bulk_discard_perf");
    let url = create_fresh_db(&admin, &db_name).await;
    let pool = build_pool(&url);
    let app = build_app(HarvestDbPool::from(pool));

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;
    seed_fixture(&mut seed_conn, MATCHING, NOISE).await;

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-plugin/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("dlq-bulk-discard");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());
    eprintln!("== capturing label={label} ==");

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    // The one, real, public entry point: the exact request an operator's
    // "purge every dead letter for this broken activity" action issues. It
    // runs at the endpoint's own row cap.
    let (status, body) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "activity_name": "dlq_bulk_perf_target", "limit": MATCHING }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "response body: {body}");
    assert_eq!(body["matched"], MATCHING);
    assert_eq!(
        body["acted_on"], MATCHING,
        "every matching row must be discarded"
    );
    assert_eq!(body["skipped"], 0);

    // ONE snapshot query -- see `snapshot_statements`'s sibling in
    // schedule_bulk_audit_perf.rs for why a second query against
    // pg_stat_statements here would pollute the total.
    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;

    let remaining: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS value FROM harvest_dead_letters \
         WHERE activity_name = 'dlq_bulk_perf_target'",
    )
    .get_result::<CountValue>(&mut stats_conn)
    .await
    .expect("count remaining matching rows")
    .value;
    assert_eq!(remaining, 0, "every matching dead letter must be gone");

    let noise_remaining: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS value FROM harvest_dead_letters \
         WHERE activity_name LIKE 'dlq_bulk_noise_%'",
    )
    .get_result::<CountValue>(&mut stats_conn)
    .await
    .expect("count remaining noise rows")
    .value;
    assert_eq!(noise_remaining, NOISE, "noise rows must be untouched");

    let stats_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_dead_letter_delete_statement(r))
        .collect();
    assert!(
        !stats_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the discard-delete shape after one \
         real bulk-discard request -- check pg_stat_statements.track and \
         shared_preload_libraries",
    );
    let total_calls: i64 = stats_rows.iter().map(|r| r.calls).sum();
    let total_buffers: i64 = stats_rows.iter().map(|r| r.total_buffers).sum();

    let stats_text = stats_rows
        .iter()
        .map(|r| {
            format!(
                "calls={} shared_blks_hit={} shared_blks_read={} total_buffers={}\nquery={}\n",
                r.calls, r.shared_blks_hit, r.shared_blks_read, r.total_buffers, r.query,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        out_dir.join(format!("{label}-pg_stat_statements.txt")),
        format!(
            "-- {label}: pg_stat_statements @ one real POST /dead-letters/discard request, \
             {MATCHING} matching + {NOISE} noise dead letters --\n\
             TOTAL across matching statement shapes: calls={total_calls} buffers={total_buffers}\n\n\
             {stats_text}\n"
        ),
    )
    .expect("write pg_stat_statements artifact");
    eprintln!("total_calls={total_calls} total_buffers={total_buffers}");

    let request_total_calls: i64 = all_rows.iter().map(|r| r.calls).sum();
    let request_total_buffers: i64 = all_rows.iter().map(|r| r.total_buffers).sum();
    let all_text = all_rows
        .iter()
        .map(|r| {
            format!(
                "calls={} shared_blks_hit={} shared_blks_read={} total_buffers={}\nquery={}\n",
                r.calls, r.shared_blks_hit, r.shared_blks_read, r.total_buffers, r.query,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    #[allow(clippy::cast_precision_loss)]
    let (calls_pct, buffers_pct) = (
        100.0 * total_calls as f64 / request_total_calls.max(1) as f64,
        100.0 * total_buffers as f64 / request_total_buffers.max(1) as f64,
    );
    std::fs::write(
        out_dir.join(format!("{label}-all-statements.txt")),
        format!(
            "-- {label}: every statement pg_stat_statements recorded for this database during \
             one real POST /dead-letters/discard request, {MATCHING} matching dead letters --\n\
             REQUEST TOTAL: calls={request_total_calls} buffers={request_total_buffers}\n\
             discard-delete shape's share: calls={total_calls}/{request_total_calls} \
             ({calls_pct:.1}%), buffers={total_buffers}/{request_total_buffers} \
             ({buffers_pct:.1}%)\n\n\
             {all_text}\n"
        ),
    )
    .expect("write all-statements artifact");
    eprintln!(
        "request_total_calls={request_total_calls} request_total_buffers={request_total_buffers}"
    );

    std::fs::write(
        out_dir.join(format!("{label}-fixture-summary.txt")),
        format!(
            "label={label}\n\
             fixture: {MATCHING} matching dead letters (activity_name = 'dlq_bulk_perf_target'), \
             {NOISE} non-matching noise dead letters across 25 other activities\n\
             request: POST /dead-letters/discard \
             {{\"activity_name\": \"dlq_bulk_perf_target\", \"limit\": {MATCHING}}}\n\
             acted_on={MATCHING}\n\
             discard_statement_shapes={}\n\
             total_calls={total_calls}\n\
             total_buffers={total_buffers}\n\
             request_total_calls={request_total_calls}\n\
             request_total_buffers={request_total_buffers}\n",
            stats_rows.len(),
        ),
    )
    .expect("write fixture summary");

    eprintln!("evidence capture complete: label={label}");
}

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-dlq-bulk-discard.md"]
async fn zz_capture_dlq_bulk_discard_perf_evidence() {
    capture_dlq_bulk_discard_evidence().await;
}

// ── Equivalence: batched delete vs. the original per-row loop ──────────────

async fn insert_dead_letter_row(conn: &mut AsyncPgConnection, activity: &str) -> uuid::Uuid {
    autumn_harvest::dlq::dead_letter(
        conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: None,
            activity_name: Some(activity.to_string()),
            input: serde_json::json!({ "test": true }),
            error: format!("{activity} failed"),
            attempts: 3,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead-letter insert should succeed")
}

/// Proves `dlq::discard_dead_letters_batch` deletes exactly the rows the
/// original per-row `diesel::delete(...find(id))` loop would have deleted.
/// It also returns exactly their ids. Covers both a set of ids that all
/// exist and a set where some ids are already gone. That mirrors the
/// `deleted == 0` skip path both the old loop and the new batch must
/// handle identically.
#[tokio::test]
async fn discard_dead_letters_batch_matches_per_row_delete_loop() {
    use autumn_harvest::dlq::discard_dead_letters_batch;
    use autumn_harvest::schema::harvest_dead_letters;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("dlq_discard_batch_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    // Group 1: every id exists. The original loop would have deleted all of
    // them, one DELETE apiece. The batch must delete all of them in one
    // statement and return every id.
    let mut existing_ids = Vec::new();
    for i in 0..25 {
        existing_ids.push(insert_dead_letter_row(&mut conn, &format!("equiv_all_exist_{i}")).await);
    }
    let mut deleted = discard_dead_letters_batch(&mut conn, &existing_ids)
        .await
        .expect("batch delete should succeed");
    deleted.sort();
    let mut expected = existing_ids.clone();
    expected.sort();
    assert_eq!(
        deleted, expected,
        "batch must delete and return every existing id"
    );
    let remaining: i64 = harvest_dead_letters::table
        .filter(harvest_dead_letters::id.eq_any(&existing_ids))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count remaining");
    assert_eq!(remaining, 0, "every row must actually be gone");

    // Group 2: half the ids exist, half never did. The original loop's
    // `Ok(0) => skipped += 1` branch silently dropped a missing id from
    // `acted_ids`. The batch must do the same -- absent from the returned
    // Vec, no error.
    let mut mixed_ids = Vec::new();
    for i in 0..10 {
        mixed_ids.push(insert_dead_letter_row(&mut conn, &format!("equiv_mixed_{i}")).await);
    }
    let missing_ids: Vec<uuid::Uuid> = (0..10).map(|_| uuid::Uuid::new_v4()).collect();
    let mut probe_ids = mixed_ids.clone();
    probe_ids.extend(missing_ids.iter().copied());

    let mut deleted_mixed = discard_dead_letters_batch(&mut conn, &probe_ids)
        .await
        .expect("batch delete over a mixed id set should succeed");
    deleted_mixed.sort();
    let mut expected_mixed = mixed_ids.clone();
    expected_mixed.sort();
    assert_eq!(
        deleted_mixed, expected_mixed,
        "only the ids that actually existed must come back"
    );
}

/// An empty slice must not touch the connection. It must return
/// `Ok(vec![])` rather than erroring. An empty `id = ANY(...)` array is
/// well-formed SQL. But sending it at all would be one more wasted round
/// trip on the common "filter matched zero rows" path.
#[tokio::test]
async fn discard_dead_letters_batch_on_empty_slice_is_a_no_op() {
    use autumn_harvest::dlq::discard_dead_letters_batch;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("dlq_discard_batch_empty")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let deleted = discard_dead_letters_batch(&mut conn, &[])
        .await
        .expect("empty batch must succeed");
    assert!(deleted.is_empty());
}

/// A batch well past the old per-chunk cap other batched writers in this
/// crate need. `audit::insert_audit_batch` chunks at 4,999 rows, because a
/// multi-row `INSERT` binds one parameter per column per row. A `DELETE
/// ... WHERE id = ANY($1)` binds the whole list as a single array
/// parameter, so this must succeed unchunked well beyond that boundary.
#[tokio::test]
async fn discard_dead_letters_batch_handles_a_large_id_list_unchunked() {
    use autumn_harvest::dlq::discard_dead_letters_batch;

    const N: usize = 10_001;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("dlq_discard_batch_large")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let mut ids = Vec::with_capacity(N);
    for i in 0..N {
        ids.push(
            autumn_harvest::dlq::dead_letter(
                &mut conn,
                &autumn_harvest::dlq::NewDeadLetterEntry {
                    original_task_id: uuid::Uuid::new_v4(),
                    queue_name: "default".to_string(),
                    task_type: "ACTIVITY".to_string(),
                    workflow_exec_id: None,
                    activity_name: Some(format!("large_batch_{i}")),
                    input: serde_json::json!({ "test": true }),
                    error: "boom".to_string(),
                    attempts: 1,
                    owner: None,
                    severity: None,
                },
            )
            .await
            .expect("seed dead-letter row"),
        );
    }

    let deleted = discard_dead_letters_batch(&mut conn, &ids)
        .await
        .expect("a large id list must not hit a bind-parameter limit");
    assert_eq!(deleted.len(), N, "every seeded row must have been deleted");
}
