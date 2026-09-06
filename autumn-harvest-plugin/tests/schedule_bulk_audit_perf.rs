//! Ledger performance investigation: `POST /ui/schedules/bulk-pause` and
//! `POST /ui/schedules/bulk-resume`.
//!
//! `schedule_bulk_pause_ui` and `schedule_bulk_resume_ui` (in
//! `autumn-harvest-plugin/src/ui.rs`) each batch their own row selection and
//! `UPDATE ... RETURNING id` per shard. Then each audits the outcome **one
//! row at a time**: a `for id in &updated_ids` loop calls `insert_audit`
//! once per updated schedule. Bulk-pausing or bulk-resuming N schedules on
//! one shard issues 1 read query, 1 batched update, and N single-row audit
//! inserts. This is exactly the class of bug this repo's own performance
//! playbook calls out: "workflow/activity bookkeeping queries that are
//! individually trivial but collectively dominant... they will never show
//! up in a buffer ranking, only in a `calls` ranking."
//!
//! The fix collects the per-row `NewAuditRecord`s into a `Vec` and issues
//! one multi-row insert per shard via the new `audit::insert_audit_batch`.
//! Every record in the batch shares the same actor, operation, route,
//! status, and shard. Only `target_id` varies. So the batched statement is
//! a straightforward `INSERT ... VALUES (...), (...), ...` with the
//! identical column values `insert_audit` would have written one row at a
//! time.
//!
//! This file is the harness + evidence generator for that investigation.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::SelectableHelper;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// ── DB bootstrap (same convention as schedule_overdue_aux_perf.rs) ─────────

type HarvestUiApp = axum::Router;
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
/// This harness's fixture and `pg_stat_statements` capture then cannot
/// collide with, or be polluted by, any other test or run on the same
/// server.
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

fn build_app(pool: HarvestDbPool) -> HarvestUiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(pool);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("schedule-bulk-audit-perf-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0)],
            vec![ShardId::new(0)],
            ShardId::new(0),
        ),
    ));
    harvest_ui_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn post_form(app: &HarvestUiApp, uri: &str, body: &str) -> StatusCode {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_string()))
                .expect("valid form request"),
        )
        .await
        .expect("POST form request failed");
    response.status()
}

// ── Fixture generation ──────────────────────────────────────────────────────

/// Seeds `matching` schedules whose `workflow_name` contains
/// `bulk_perf_target`, so the bulk action's `target=bulk_perf_target`
/// filter selects exactly this set. Also seeds `noise` unrelated,
/// non-matching schedules on the same shard. This models a realistic
/// "pause every schedule for this workflow family" operator action. The
/// fleet also has other schedules on it, not just the rows under test.
/// Pure set-based SQL, not a per-row Rust loop.
async fn seed_fixture(conn: &mut AsyncPgConnection, matching: i64, noise: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_schedules (
             id, dag_name, schedule_expr, timezone, catchup, max_active_runs, is_paused,
             next_run_at, created_at, updated_at, workflow_name, queue_name, jitter_secs,
             overlap_policy, buffered_runs, buffer_all_max, calendar_name, skip_policy
         )
         SELECT
             gen_random_uuid(), NULL, 'interval:3600', 'UTC', false,
             (1 + (gs % 5))::int4, false,
             NOW() - (random() * interval '2 hours'),
             NOW(), NOW(),
             'bulk_perf_target_' || gs,
             'default', 0, 'skip', '[]'::jsonb, 100, NULL, 'skip'
         FROM generate_series(1, {matching}) AS gs;

         INSERT INTO harvest_schedules (
             id, dag_name, schedule_expr, timezone, catchup, max_active_runs, is_paused,
             next_run_at, created_at, updated_at, workflow_name, queue_name, jitter_secs,
             overlap_policy, buffered_runs, buffer_all_max, calendar_name, skip_policy
         )
         SELECT
             gen_random_uuid(), NULL, 'interval:3600', 'UTC', false,
             (1 + (gs % 5))::int4, false,
             NOW() - (random() * interval '2 hours'),
             NOW(), NOW(),
             'bulk_perf_noise_' || gs,
             'default', 0, 'skip', '[]'::jsonb, 100, NULL, 'skip'
         FROM generate_series(1, {noise}) AS gs;

         ANALYZE harvest_schedules;"
    ))
    .await
    .expect("seed schedule-bulk-audit perf fixture");
}

// ── pg_stat_statements capture ──────────────────────────────────────────────

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

/// Every statement recorded for this database since the last reset, in ONE
/// query. See `schedule_overdue_aux_perf.rs`'s identical helper for why a
/// second query against `pg_stat_statements` here would self-pollute
/// whatever total this later computes from it (Codex review, PR #1314).
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

/// Whether `row` is the audit-insert statement shape this investigation
/// targets: an `INSERT` into `harvest_audit_log`.
fn is_audit_insert_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("insert into") && q.contains("harvest_audit_log")
}

// ── Evidence capture (not a CI assertion) ───────────────────────────────────

async fn capture_bulk_action_evidence(action: &str, label_prefix: &str) {
    const MATCHING: i64 = 300;
    const NOISE: i64 = 200;

    let (admin, _guard) = setup_server().await;
    let db_name = unique(&format!("{label_prefix}_perf"));
    let url = create_fresh_db(&admin, &db_name).await;
    let pool = build_pool(&url);
    let app = build_app(HarvestDbPool::from(pool));

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;

    seed_fixture(&mut seed_conn, MATCHING, NOISE).await;
    if action == "bulk-resume" {
        // Seed already-paused rows so bulk-resume has something to flip.
        diesel::sql_query(
            "UPDATE harvest_schedules SET is_paused = true, paused_at = NOW() \
             WHERE workflow_name LIKE 'bulk_perf_target_%'",
        )
        .execute(&mut seed_conn)
        .await
        .expect("pre-pause matching rows for the bulk-resume scenario");
    }

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-plugin/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("schedule-bulk-audit");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());
    eprintln!("== capturing label={label} action={action} ==");

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    // The one, real, public entry point: the exact request Vantage's
    // schedules management page issues for a filtered bulk pause/resume.
    let status = post_form(
        &app,
        &format!("/schedules/{action}"),
        "target=bulk_perf_target",
    )
    .await;
    assert!(
        status.is_redirection(),
        "bulk action must redirect (got {status})"
    );

    let acted_on: i64 = diesel::sql_query(if action == "bulk-resume" {
        "SELECT COUNT(*) AS value FROM harvest_schedules \
         WHERE workflow_name LIKE 'bulk_perf_target_%' AND is_paused = false"
    } else {
        "SELECT COUNT(*) AS value FROM harvest_schedules \
         WHERE workflow_name LIKE 'bulk_perf_target_%' AND is_paused = true"
    })
    .get_result::<CountValue>(&mut stats_conn)
    .await
    .expect("count acted-on rows")
    .value;
    assert_eq!(
        acted_on, MATCHING,
        "every matching schedule must have been acted on"
    );

    // ONE snapshot query for both views below -- see `snapshot_statements`'s
    // doc comment for why a second query here would pollute the total.
    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let stats_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_audit_insert_statement(r))
        .collect();
    assert!(
        !stats_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the audit-insert shape after one real \
         bulk action -- check pg_stat_statements.track (must be 'all' or 'top') and \
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
        out_dir.join(format!("{label}-{action}-pg_stat_statements.txt")),
        format!(
            "-- {label} {action}: pg_stat_statements @ one real POST /ui/schedules/{action} \
             request, {MATCHING} matching + {NOISE} noise schedules --\n\
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
        out_dir.join(format!("{label}-{action}-all-statements.txt")),
        format!(
            "-- {label} {action}: every statement pg_stat_statements recorded for this database \
             during one real POST /ui/schedules/{action} request, {MATCHING} matching schedules --\n\
             REQUEST TOTAL: calls={request_total_calls} buffers={request_total_buffers}\n\
             audit-insert shape's share: calls={total_calls}/{request_total_calls} \
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
        out_dir.join(format!("{label}-{action}-fixture-summary.txt")),
        format!(
            "label={label}\naction={action}\n\
             fixture: {MATCHING} matching schedules (workflow_name LIKE 'bulk_perf_target_%'), \
             {NOISE} non-matching noise schedules on the same shard\n\
             request: POST /ui/schedules/{action} target=bulk_perf_target\n\
             acted_on={acted_on}\n\
             audit_statement_shapes={}\n\
             total_calls={total_calls}\n\
             total_buffers={total_buffers}\n\
             request_total_calls={request_total_calls}\n\
             request_total_buffers={request_total_buffers}\n",
            stats_rows.len(),
        ),
    )
    .expect("write fixture summary");

    eprintln!("evidence capture complete: label={label} action={action}");
}

#[derive(diesel::QueryableByName, Debug)]
struct CountValue {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-schedule-bulk-audit.md"]
async fn zz_capture_schedule_bulk_pause_audit_perf_evidence() {
    capture_bulk_action_evidence("bulk-pause", "schedule_bulk_pause_audit").await;
}

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-schedule-bulk-audit.md"]
async fn zz_capture_schedule_bulk_resume_audit_perf_evidence() {
    capture_bulk_action_evidence("bulk-resume", "schedule_bulk_resume_audit").await;
}

// ── Equivalence: batched audit insert vs. the original per-row loop ────────

/// Proves `audit::insert_audit_batch` writes exactly the rows the original
/// per-row `insert_audit` loop would have written. Same columns, same
/// values, for every id, just in one statement instead of N. Uses a fresh,
/// disjoint set of target ids per call, so the two code paths' rows never
/// collide in the same table.
#[tokio::test]
async fn insert_audit_batch_matches_per_row_insert_audit_loop() {
    use autumn_harvest::audit::{insert_audit, insert_audit_batch};
    use autumn_harvest::models::{AuditRecord, NewAuditRecord};
    use autumn_harvest::schema::harvest_audit_log;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("audit_batch_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let looped_ids: Vec<String> = (0..25).map(|i| format!("looped-{i}")).collect();
    for id in &looped_ids {
        let ar = NewAuditRecord {
            actor: "ui",
            operation: "schedule.pause",
            target_type: "schedule",
            target_id: Some(id.as_str()),
            route_or_command: "POST /ui/schedules/bulk-pause",
            request_id: None,
            idempotency_key: None,
            status: "succeeded",
            error_summary: None,
            shard_id: Some(0),
            source: "ui",
        };
        insert_audit(&mut conn, &ar).await.expect("looped insert");
    }

    let batched_ids: Vec<String> = (0..25).map(|i| format!("batched-{i}")).collect();
    let records: Vec<NewAuditRecord<'_>> = batched_ids
        .iter()
        .map(|id| NewAuditRecord {
            actor: "ui",
            operation: "schedule.pause",
            target_type: "schedule",
            target_id: Some(id.as_str()),
            route_or_command: "POST /ui/schedules/bulk-pause",
            request_id: None,
            idempotency_key: None,
            status: "succeeded",
            error_summary: None,
            shard_id: Some(0),
            source: "ui",
        })
        .collect();
    let returned_ids = insert_audit_batch(&mut conn, &records)
        .await
        .expect("batched insert");
    assert_eq!(
        returned_ids.len(),
        batched_ids.len(),
        "insert_audit_batch must return one generated id per input record"
    );

    let mut looped_rows: Vec<AuditRecord> = harvest_audit_log::table
        .filter(harvest_audit_log::target_id.eq_any(&looped_ids))
        .select(AuditRecord::as_select())
        .load(&mut conn)
        .await
        .expect("load looped rows");
    let mut batched_rows: Vec<AuditRecord> = harvest_audit_log::table
        .filter(harvest_audit_log::target_id.eq_any(&batched_ids))
        .select(AuditRecord::as_select())
        .load(&mut conn)
        .await
        .expect("load batched rows");
    assert_eq!(
        looped_rows.len(),
        25,
        "every looped insert must have landed"
    );
    assert_eq!(
        batched_rows.len(),
        25,
        "every batched insert must have landed"
    );

    // Compare content field-by-field, excluding `id` and `occurred_at`.
    // Those two legitimately differ. `id` is a fresh UUID per row either
    // way. A single-statement batch insert shares one `NOW()` across its
    // rows, where N separate statements each get their own. That is a
    // disclosed, intentional side effect of batching, not a correctness
    // gap. Sort by target_id first, so row order (arbitrary either way,
    // since neither path assumes an order) does not cause a spurious
    // mismatch.
    looped_rows.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    batched_rows.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    for (looped, batched) in looped_rows.iter().zip(batched_rows.iter()) {
        assert_eq!(looped.actor, batched.actor);
        assert_eq!(looped.operation, batched.operation);
        assert_eq!(looped.target_type, batched.target_type);
        assert_eq!(looped.route_or_command, batched.route_or_command);
        assert_eq!(looped.request_id, batched.request_id);
        assert_eq!(looped.idempotency_key, batched.idempotency_key);
        assert_eq!(looped.status, batched.status);
        assert_eq!(looped.error_summary, batched.error_summary);
        assert_eq!(looped.shard_id, batched.shard_id);
        assert_eq!(looped.source, batched.source);
    }

    // Every batched row shares exactly one `occurred_at`: single-statement
    // insert, single transaction, single `NOW()`. This proves the batch
    // really landed as one statement, not N autocommitted ones.
    let distinct_batched_times: std::collections::BTreeSet<_> =
        batched_rows.iter().map(|r| r.occurred_at).collect();
    assert_eq!(
        distinct_batched_times.len(),
        1,
        "a single multi-row INSERT must give every row the same occurred_at"
    );
}

/// `insert_audit_batch` on an empty slice must not touch the connection.
/// An empty `VALUES` list has no `Insertable` representation. It must
/// return an empty `Vec` rather than erroring.
#[tokio::test]
async fn insert_audit_batch_on_empty_slice_is_a_no_op() {
    use autumn_harvest::audit::insert_audit_batch;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("audit_batch_empty")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let returned = insert_audit_batch(&mut conn, &[])
        .await
        .expect("empty batch must succeed");
    assert!(returned.is_empty());
}
