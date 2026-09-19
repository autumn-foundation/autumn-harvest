//! Performance investigation: `GET /admin/status`'s `stalled_workflows`
//! anti-join (issue #1643).
//!
//! `status_summary::count_stalled_candidates` counted stalled executions with
//! a `NOT EXISTS` correlated on a `timestamp` range, evaluated once per
//! `RUNNING`/`SUSPENDED` row (a `Nested Loop Anti Join`, not hashable). Issue
//! #1643 measured this at 86.9% of a request's buffers on a 3,000-active-
//! execution fixture. The fix reads a `MATERIALIZED` CTE of "execution ids
//! with a recent event", built once, then anti-joins by equality.
//!
//! The issue explicitly declined to propose this fix without first
//! characterizing it against **two** workload shapes. The new query scans
//! `harvest_events` by timestamp fleet-wide instead of once per active
//! execution:
//!
//! * **execution-heavy** — many active executions, few recent events per one
//!   (the issue's own worst-case fixture: 3,000 active, ~1 recent event
//!   each).
//! * **event-write-heavy** — the same active-execution count, but each
//!   healthy execution is far chattier (many recent events each). The
//!   window `harvest_events` must scan is much larger relative to the
//!   active-execution count.
//!
//! Both regimes share the same 50,000-row terminal-execution population.
//! Each terminal execution carries old, out-of-window events, as dead weight
//! the state filter and the new index must both skip. Both regimes also
//! share the same 30 true-positive stalled executions, so only the
//! write-volume variable changes between them.
//!
//! This file is the evidence generator (`#[ignore]`d) plus a
//! permanent correctness regression pinning the true-positive/healthy split
//! on the execution-heavy fixture.

#![allow(clippy::too_many_lines)]

use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// ── DB bootstrap (matches schedule_overdue_aux_perf.rs's convention) ────────

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
    harvest_api_router(api_state)
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

// ── Fixture generation ──────────────────────────────────────────────────────

const TERMINAL_COUNT: i64 = 50_000;
const HEALTHY_COUNT: i64 = 2_970;
const STALLED_COUNT: i64 = 30;
const WINDOW_MINUTES: i64 = 60;

/// Seeds the shared fixture skeleton, in three populations.
/// `TERMINAL_COUNT` completed executions each carry 3 old events, well
/// outside the window — a mature deployment's dead-weight history.
/// `HEALTHY_COUNT` actively-progressing RUNNING executions each carry a
/// pending task-queue row, so they pass the OR-block, plus
/// `events_per_healthy` recent events. `STALLED_COUNT` true positives each
/// carry a single stale event, no other pending work, and no timer at all.
/// This is the worst case for the bounded LIMIT, matching issue #1643's own
/// fixture.
///
/// `events_per_healthy` is the one variable that distinguishes the
/// execution-heavy regime (1) from the event-write-heavy regime (100).
/// Everything else — active-execution count, OR-block shape, true-positive
/// count — is held constant. Only write volume changes between runs.
/// `terminal_age` is a Postgres `INTERVAL` literal (e.g. `"30 days"` or
/// `"5 minutes"`) for the terminal-execution population's event age. A
/// recent value recreates a high-churn deployment: many workflows finish
/// inside the no-progress window (issue #1643, code review). The default
/// regimes instead use a well-outside-the-window history.
async fn seed_fixture_with_terminal_age(
    conn: &mut AsyncPgConnection,
    events_per_healthy: i64,
    terminal_age: &str,
) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at
         )
         SELECT
             gen_random_uuid(), 'stalled_perf_terminal', 'stalled_perf_terminal_' || gs,
             gen_random_uuid(), 0, 'COMPLETED', '{{}}'::jsonb, 'default', NOW(), NOW()
         FROM generate_series(1, {TERMINAL_COUNT}) AS gs;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT e.id, s, 'WorkflowStarted', '{{}}'::jsonb, NOW() - INTERVAL '{terminal_age}'
         FROM harvest_workflow_executions e
         CROSS JOIN generate_series(0, 2) AS s
         WHERE e.workflow_name = 'stalled_perf_terminal';

         INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at
         )
         SELECT
             gen_random_uuid(), 'stalled_perf_healthy', 'stalled_perf_healthy_' || gs,
             gen_random_uuid(), 0, 'RUNNING', '{{}}'::jsonb, 'default', NOW(), NOW()
         FROM generate_series(1, {HEALTHY_COUNT}) AS gs;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT e.id, s, 'ActivityCompleted', '{{}}'::jsonb, NOW() - (s || ' seconds')::interval
         FROM harvest_workflow_executions e
         CROSS JOIN generate_series(0, {events_per_healthy} - 1) AS s
         WHERE e.workflow_name = 'stalled_perf_healthy';

         INSERT INTO harvest_task_queue (
             id, queue_name, task_type, workflow_exec_id, input, state, priority,
             max_attempts, scheduled_at
         )
         SELECT gen_random_uuid(), 'default', 'activity', e.id, '{{}}'::jsonb, 'PENDING', 0, 1, NOW()
         FROM harvest_workflow_executions e
         WHERE e.workflow_name = 'stalled_perf_healthy';

         INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at
         )
         SELECT
             gen_random_uuid(), 'stalled_perf_stalled', 'stalled_perf_stalled_' || gs,
             gen_random_uuid(), 0, 'RUNNING', '{{}}'::jsonb, 'default', NOW(), NOW()
         FROM generate_series(1, {STALLED_COUNT}) AS gs;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT e.id, 0, 'WorkflowStarted', '{{}}'::jsonb, NOW() - INTERVAL '2 hours'
         FROM harvest_workflow_executions e
         WHERE e.workflow_name = 'stalled_perf_stalled';

         ANALYZE harvest_workflow_executions;
         ANALYZE harvest_events;
         ANALYZE harvest_task_queue;"
    ))
    .await
    .expect("seed stalled-perf fixture");
}

// ── pg_stat_statements capture ──────────────────────────────────────────────

#[derive(diesel::QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = diesel::sql_types::Text, column_name = "QUERY PLAN")]
    query_plan: String,
}

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

/// Identifies the `count_stalled_candidates` statement by a substring
/// present in both the pre-#1643 and post-#1643 query text. The OR-block is
/// untouched by the rewrite, so the same filter works for before/after
/// comparisons.
fn is_stalled_candidates_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("harvest_signals") && q.contains("fires_at")
}

/// `count_stalled_candidates_query()`'s `$1`/`$2` placeholders, replaced
/// with literal values, for `EXPLAIN` only. Reads the same `pub const fn`
/// `status_summary::count_stalled_candidates` itself calls. This cannot
/// drift from the real query, unlike a hand-copied second source of truth
/// (issue #1643, code review). Each placeholder appears exactly once in the
/// source text, so a plain string replace is unambiguous.
fn literal_count_stalled_candidates_sql(minutes: i64, cap: i64) -> String {
    autumn_harvest_plugin::status_summary::count_stalled_candidates_query()
        .replacen("$1", &minutes.to_string(), 1)
        .replacen("$2", &cap.to_string(), 1)
}

async fn capture_regime(
    admin_url: &str,
    label: &str,
    events_per_healthy: i64,
    terminal_age: &str,
    out_dir: &std::path::Path,
) {
    let db_name = unique("status_summary_stalled_perf");
    let url = create_fresh_db(admin_url, &db_name).await;
    let pool = build_pool(&url);
    let app = build_app(HarvestDbPool::from(pool));

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;
    seed_fixture_with_terminal_age(&mut seed_conn, events_per_healthy, terminal_age).await;

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK, "admin/status request must succeed");
    let stalled_count = body["subsystems"]
        .as_array()
        .expect("subsystems array")
        .iter()
        .find(|s| s["name"] == "stalled_workflows")
        .and_then(|s| s["count"].as_i64())
        .expect("stalled_workflows count present");
    assert_eq!(
        stalled_count, STALLED_COUNT,
        "the {STALLED_COUNT} seeded true positives must all be counted, \
         and none of the {HEALTHY_COUNT} healthy executions miscounted, \
         for this evidence run to mean anything"
    );

    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let target_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_stalled_candidates_statement(r))
        .collect();
    assert!(
        !target_rows.is_empty(),
        "pg_stat_statements returned zero rows matching count_stalled_candidates's shape"
    );
    let total_calls: i64 = target_rows.iter().map(|r| r.calls).sum();
    let total_buffers: i64 = target_rows.iter().map(|r| r.total_buffers).sum();
    let request_total_buffers: i64 = all_rows.iter().map(|r| r.total_buffers).sum();
    #[allow(clippy::cast_precision_loss)]
    let buffers_pct = 100.0 * total_buffers as f64 / request_total_buffers.max(1) as f64;

    let target_text = target_rows
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
            "-- {label}: count_stalled_candidates statement(s) after one real \
             GET /admin/status request --\n\
             events_per_healthy_execution={events_per_healthy}\n\
             stalled_count_returned={stalled_count}\n\
             total_calls={total_calls}\n\
             total_buffers={total_buffers}\n\
             request_total_buffers={request_total_buffers}\n\
             buffers_pct_of_request={buffers_pct:.1}\n\n\
             {target_text}\n"
        ),
    )
    .expect("write pg_stat_statements artifact");

    // A representative EXPLAIN (ANALYZE, BUFFERS) run, using the same values
    // `count_stalled_candidates` binds: a 60-minute window, cap =
    // stalled_critical_count + 1 = 51. These values are literals, not
    // placeholders. `pg_stat_statements` jumbles every constant in
    // `target_rows[0].query`, not just the two real bind parameters, into
    // renumbered `$N` placeholders. That text cannot be replayed directly.
    let explainable = literal_count_stalled_candidates_sql(WINDOW_MINUTES, 51);
    let explain_rows: Vec<ExplainRow> =
        diesel::sql_query(format!("EXPLAIN (ANALYZE, BUFFERS) {explainable}"))
            .load(&mut stats_conn)
            .await
            .expect("EXPLAIN of count_stalled_candidates's query shape");
    let explain_text = explain_rows
        .iter()
        .map(|r| r.query_plan.clone())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        out_dir.join(format!("{label}-explain.txt")),
        format!("-- {label} --\n{explainable}\n\n{explain_text}\n"),
    )
    .expect("write explain artifact");

    eprintln!(
        "label={label} events_per_healthy={events_per_healthy} total_calls={total_calls} \
         total_buffers={total_buffers} buffers_pct={buffers_pct:.1}"
    );
}

/// Evidence generator: captures `count_stalled_candidates`'s buffer cost
/// under both workload regimes, against whatever query the working tree
/// currently has. Run once before the #1643 rewrite and once after, per
/// `docs/performance-status-summary-stalled.md`'s reproduction steps.
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-status-summary-stalled.md"]
async fn zz_capture_status_summary_stalled_perf_evidence() {
    let (admin, _guard) = setup_server().await;

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-plugin/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("status-summary-stalled-cte");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let variant = std::env::var("PERF_VARIANT").unwrap_or_else(|_| "after".to_string());
    capture_regime(
        &admin,
        &format!("{variant}-execution-heavy"),
        1,
        "30 days",
        &out_dir,
    )
    .await;
    capture_regime(
        &admin,
        &format!("{variant}-event-write-heavy"),
        100,
        "30 days",
        &out_dir,
    )
    .await;
    // (issue #1643, code review): a high-churn deployment where many
    // workflows finish inside the no-progress window, instead of the other
    // two regimes' well-outside-the-window terminal history. Same active
    // population (3,000) and per-healthy-execution event rate (1) as the
    // execution-heavy regime; only the terminal population's event age
    // changes.
    capture_regime(
        &admin,
        &format!("{variant}-terminal-churn"),
        1,
        "5 minutes",
        &out_dir,
    )
    .await;

    eprintln!("evidence capture complete: variant={variant}");
}

/// Permanent regression: the execution-heavy fixture's true-positive/healthy
/// split must come back exactly right. Small-N so it stays in the default
/// suite; the 50k/3k evidence fixture above is `#[ignore]`d for cost.
#[tokio::test]
async fn count_stalled_candidates_matches_seeded_true_positives_small_fixture() {
    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("stalled_candidates_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    // A tenth of the full fixture's scale, same shape.
    conn.batch_execute(
        "INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at
         )
         SELECT gen_random_uuid(), 'small_healthy', 'small_healthy_' || gs,
                gen_random_uuid(), 0, 'RUNNING', '{}'::jsonb, 'default', NOW(), NOW()
         FROM generate_series(1, 20) AS gs;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT id, 0, 'ActivityCompleted', '{}'::jsonb, NOW()
         FROM harvest_workflow_executions WHERE workflow_name = 'small_healthy';

         INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at
         )
         SELECT gen_random_uuid(), 'small_stalled', 'small_stalled_' || gs,
                gen_random_uuid(), 0, 'RUNNING', '{}'::jsonb, 'default', NOW(), NOW()
         FROM generate_series(1, 3) AS gs;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT id, 0, 'WorkflowStarted', '{}'::jsonb, NOW() - INTERVAL '2 hours'
         FROM harvest_workflow_executions WHERE workflow_name = 'small_stalled';",
    )
    .await
    .expect("seed small fixture");

    let pool = build_pool(&url);
    let app = build_app(HarvestDbPool::from(pool));
    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK);
    let stalled_count = body["subsystems"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "stalled_workflows")
        .and_then(|s| s["count"].as_i64())
        .expect("stalled_workflows count present");
    assert_eq!(
        stalled_count, 3,
        "exactly the 3 seeded true positives must be counted, none of the 20 healthy rows"
    );
}
