//! Ledger performance investigation: `GET /admin/status`'s `stalled_workflows`
//! subsystem.
//!
//! `status_summary::count_stalled_candidates` runs a single bounded SQL
//! statement. It mirrors `api::load_stalled_workflows`'s candidate
//! predicate: an active-state (`RUNNING`/`SUSPENDED`) execution with no
//! recent `harvest_events` row. The execution must also not be "correctly
//! sleeping" on a lone future timer. This file profiles that statement
//! against a production-shaped fixture through the real `GET /admin/status`
//! HTTP entry point. The fixture holds a large terminal-execution
//! population, a moderate active population, and a rare genuinely-stalled
//! subset.
//!
//! This is a **measurement-only** investigation: it captures evidence
//! (`pg_stat_statements`, `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)`)
//! and does not ship a query-shape change. See the tracking issue this
//! commit's history links for the write-up and the reasoning for why this
//! stayed a findings report rather than a PR.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
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

// ── DB bootstrap (mirrors schedule_overdue_aux_perf.rs's own convention) ────

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
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("status-summary-stalled-perf-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0)],
            vec![ShardId::new(0)],
            ShardId::new(0),
        ),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// `build_app` sets `set_admin_auth_boundary(true)`. That declares the
/// boundary already enforced by an embedder, so it short-circuits
/// `has_harvest_admin_access` to `true` for every caller. No header is
/// needed, mirroring `status_summary_localpg.rs`'s own authenticated
/// `build_app`.
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
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

// ── Fixture generation ──────────────────────────────────────────────────────

/// Production-shaped fixture. Pure set-based SQL.
///
/// It holds a large terminal population — the bulk of a mature deployment's
/// history table. It holds a moderate active population that is mostly
/// healthy and progressing. And it holds a rare, genuinely-stalled subset
/// within that active population.
///
/// * `terminal` completed executions — dead weight the `state` filter must
///   skip without a full-table scan (served by `idx_harvest_we_state`).
/// * `active` `RUNNING` executions, of which:
///   - `active - stalled` are **healthy**: a fresh event, so the anti-join
///     probe excludes them. They also carry a real pending
///     `harvest_task_queue` row, so the "not purely sleeping" `OR`-block
///     passes deterministically, independent of the timer table. This is
///     the realistic majority shape — most active work progresses normally.
///   - `stalled` (the last `stalled` by workflow id) are the true
///     positives. Each carries a stale (2h old) most-recent event, no
///     pending task-queue row, no non-terminal child, no unconsumed
///     signal, and no timer of any kind. The `OR`-block passes via "no
///     future timer to sleep on", so the row survives the anti-join.
///
///   Both groups pass the `OR`-block, so **both** reach the
///   `harvest_events` anti-join probe. Only the events check itself tells
///   healthy and stalled apart. That is the worst case for a
///   bounded-`LIMIT` scan: true positives (`stalled`) are far fewer than
///   the cap, so the scan cannot stop early. It must probe the *entire*
///   active population.
/// * A `harvest_timers` population, ~15% of terminal executions with fired
///   status mixed, so the `OR`-block's timer subplans touch a non-trivial
///   table. This population is independent of the healthy/stalled
///   classification above, which `task_queue` decides, not timers.
async fn seed_fixture(conn: &mut AsyncPgConnection, terminal: i64, active: i64, stalled: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at
         )
         SELECT gen_random_uuid(), 'ledger_done_' || (gs % 50), 'ledger_done_' || gs,
                gen_random_uuid(), 0, 'COMPLETED', '{{}}'::jsonb, 'default',
                NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days'
         FROM generate_series(1, {terminal}) AS gs;

         INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at
         )
         SELECT gen_random_uuid(), 'ledger_active_' || (gs % 50), 'ledger_active_' || gs,
                gen_random_uuid(), 0, 'RUNNING', '{{}}'::jsonb, 'default',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours'
         FROM generate_series(1, {active}) AS gs;

         -- Every terminal row gets an old event (irrelevant -- excluded by state).
         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT id, 0, 'WorkflowStarted', '{{}}'::jsonb, NOW() - INTERVAL '30 days'
         FROM harvest_workflow_executions WHERE state = 'COMPLETED';

         -- Healthy active rows: a fresh event.
         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT id, 0, 'WorkflowStarted', '{{}}'::jsonb, NOW()
         FROM harvest_workflow_executions
         WHERE state = 'RUNNING'
           AND CAST(split_part(workflow_id, '_', 3) AS BIGINT) <= ({active} - {stalled});

         -- Stalled active rows: a stale event only.
         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT id, 0, 'WorkflowStarted', '{{}}'::jsonb, NOW() - INTERVAL '2 hours'
         FROM harvest_workflow_executions
         WHERE state = 'RUNNING'
           AND CAST(split_part(workflow_id, '_', 3) AS BIGINT) > ({active} - {stalled});

         -- Healthy active rows get real pending task-queue work.
         INSERT INTO harvest_task_queue (
             id, task_type, queue_name, state, priority, scheduled_at, input,
             workflow_exec_id, created_at
         )
         SELECT gen_random_uuid(), 'activity', 'default', 'PENDING', 0, NOW(), '{{}}'::jsonb, id, NOW()
         FROM harvest_workflow_executions
         WHERE state = 'RUNNING'
           AND CAST(split_part(workflow_id, '_', 3) AS BIGINT) <= ({active} - {stalled});

         -- A timer population (not-fired and already-fired mixed) drawn from
         -- terminal executions only, so it does not change the
         -- healthy/stalled classification above -- exists purely to give the
         -- OR-block's timer subplans a non-trivial table to scan.
         INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired)
         SELECT gen_random_uuid(), id, 'sleep',
                CASE WHEN random() < 0.5 THEN NOW() + INTERVAL '1 hour'
                     ELSE NOW() - INTERVAL '1 hour' END,
                random() < 0.3
         FROM harvest_workflow_executions
         WHERE state = 'COMPLETED' AND random() < 0.15;

         ANALYZE;"
    ))
    .await
    .expect("seed status-summary stalled-perf fixture");
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
/// query. See `schedule_overdue_aux_perf.rs`'s identical helper: a second
/// `pg_stat_statements` query here would pollute its own total.
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

/// Whether `row` is the `count_stalled_candidates` statement shape. This is
/// the bounded stalled-candidate count from `status_summary.rs`. It is
/// identified by a distinctive table/column substring combination, not
/// shared with any other statement `GET /admin/status` issues.
fn is_stalled_count_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("harvest_workflow_executions")
        && q.contains("harvest_events")
        && q.contains("harvest_timers")
        && q.contains("harvest_signals")
        && q.contains("count(*)")
}

#[derive(diesel::QueryableByName, Debug)]
struct ExplainLine {
    #[diesel(sql_type = diesel::sql_types::Text)]
    #[diesel(column_name = "QUERY PLAN")]
    line: String,
}

// ── Evidence capture (not a CI assertion) ───────────────────────────────────

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-status-summary-stalled.md"]
async fn zz_capture_status_summary_stalled_perf_evidence() {
    const TERMINAL: i64 = 50_000;
    const ACTIVE: i64 = 3_000;
    const STALLED: i64 = 30;

    let (admin, _guard) = setup_server().await;
    let db_name = unique("status_summary_stalled_perf");
    let url = create_fresh_db(&admin, &db_name).await;
    let pool = build_pool(&url);
    let app = build_app(HarvestDbPool::from(pool));

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;

    seed_fixture(&mut seed_conn, TERMINAL, ACTIVE, STALLED).await;

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-plugin/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("status-summary-stalled-anti-join");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    // The one, real, public entry point -- the exact request Vantage's (or
    // any operator's) one-call triage page makes.
    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK, "status request must succeed");
    let stalled_count = body["subsystems"]
        .as_array()
        .expect("subsystems array")
        .iter()
        .find(|s| s["name"] == "stalled_workflows")
        .expect("stalled_workflows subsystem present")["count"]
        .as_i64()
        .expect("count is an integer");
    eprintln!("reported stalled_count={stalled_count}");

    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let target_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_stalled_count_statement(r))
        .collect();
    assert!(
        !target_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the stalled-count statement shape after \
         one real GET /admin/status request -- check pg_stat_statements.track and \
         shared_preload_libraries",
    );
    let target_calls: i64 = target_rows.iter().map(|r| r.calls).sum();
    let target_buffers: i64 = target_rows.iter().map(|r| r.total_buffers).sum();

    let request_total_calls: i64 = all_rows.iter().map(|r| r.calls).sum();
    let request_total_buffers: i64 = all_rows.iter().map(|r| r.total_buffers).sum();

    #[allow(clippy::cast_precision_loss)]
    let (calls_pct, buffers_pct) = (
        100.0 * target_calls as f64 / request_total_calls.max(1) as f64,
        100.0 * target_buffers as f64 / request_total_buffers.max(1) as f64,
    );

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
    std::fs::write(
        out_dir.join("pg_stat_statements.txt"),
        format!(
            "-- pg_stat_statements @ one real GET /admin/status request, \
             {TERMINAL} terminal + {ACTIVE} active executions ({STALLED} genuinely stalled) --\n\
             REQUEST TOTAL: calls={request_total_calls} buffers={request_total_buffers}\n\
             stalled-count statement share: calls={target_calls}/{request_total_calls} \
             ({calls_pct:.1}%), buffers={target_buffers}/{request_total_buffers} \
             ({buffers_pct:.1}%)\n\n\
             {all_text}\n"
        ),
    )
    .expect("write pg_stat_statements artifact");
    eprintln!(
        "target_calls={target_calls} target_buffers={target_buffers} \
         request_total_calls={request_total_calls} request_total_buffers={request_total_buffers} \
         calls_pct={calls_pct:.1}% buffers_pct={buffers_pct:.1}%"
    );

    // Full EXPLAIN of the exact statement `count_stalled_candidates` issues.
    // Same connection, same fixture, immediately after the real request
    // above, so the plan reflects a warm cache matching operational
    // conditions.
    let explain_sql = "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) \
         SELECT COUNT(*)::BIGINT AS cnt FROM ( \
             SELECT 1 FROM harvest_workflow_executions e \
             WHERE e.state IN ('RUNNING', 'SUSPENDED') \
             AND NOT EXISTS ( \
                 SELECT 1 FROM harvest_events ev \
                 WHERE ev.workflow_exec_id = e.id \
                 AND ev.timestamp >= NOW() - ($1 * INTERVAL '1 minute') \
             ) \
             AND ( \
                 EXISTS ( \
                     SELECT 1 FROM harvest_task_queue \
                     WHERE workflow_exec_id = e.id \
                     AND state IN ('PENDING','CLAIMED','RUNNING','BACKOFF') \
                 ) \
              OR EXISTS ( \
                     SELECT 1 FROM harvest_workflow_executions c \
                     WHERE c.parent_id = e.id \
                     AND c.state NOT IN ( \
                         'COMPLETED','FAILED','CANCELLED', \
                         'TIMED_OUT','CONTINUED_AS_NEW','TERMINATED' \
                     ) \
                 ) \
              OR EXISTS ( \
                     SELECT 1 FROM harvest_signals \
                     WHERE workflow_exec_id = e.id AND consumed = false \
                 ) \
              OR NOT EXISTS ( \
                     SELECT 1 FROM harvest_timers \
                     WHERE workflow_exec_id = e.id \
                     AND fired = false AND fires_at > NOW() \
                 ) \
              OR EXISTS ( \
                     SELECT 1 FROM harvest_timers \
                     WHERE workflow_exec_id = e.id \
                     AND fired = false AND fires_at <= NOW() \
                 ) \
             ) \
             LIMIT $2 \
         ) t";

    let plan_lines: Vec<ExplainLine> = diesel::sql_query(explain_sql)
        .bind::<diesel::sql_types::BigInt, _>(60_i64)
        .bind::<diesel::sql_types::BigInt, _>(51_i64)
        .load(&mut stats_conn)
        .await
        .expect("EXPLAIN query");
    let plan_text = plan_lines
        .iter()
        .map(|l| l.line.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        out_dir.join("explain.txt"),
        format!(
            "-- EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) of \
             status_summary::count_stalled_candidates's exact SQL, minutes=60 cap=51, \
             {TERMINAL} terminal + {ACTIVE} active executions ({STALLED} genuinely stalled) --\n\n\
             {plan_text}\n"
        ),
    )
    .expect("write explain artifact");
    eprintln!("{plan_text}");

    std::fs::write(
        out_dir.join("fixture-summary.txt"),
        format!(
            "fixture: {TERMINAL} terminal (COMPLETED) executions, {ACTIVE} active (RUNNING) \
             executions, {STALLED} of the active set genuinely stalled (a stale 2h-old \
             most-recent event, no pending task-queue row, no non-terminal child, no unconsumed \
             signal, no timer at all); every other active row is healthy (a fresh event plus a \
             real pending task-queue row). Both groups pass the OR-block's 'not purely \
             sleeping' test -- the healthy group via its task-queue row, the stalled group via \
             having no timer to sleep on -- so both reach the harvest_events anti-join probe; \
             only that probe tells them apart\n\
             request: GET /admin/status (admin-authed)\n\
             reported stalled_count={stalled_count}\n\
             target_statement_shapes={}\n\
             target_calls={target_calls}\n\
             target_buffers={target_buffers}\n\
             request_total_calls={request_total_calls}\n\
             request_total_buffers={request_total_buffers}\n\
             calls_pct={calls_pct:.1}%\n\
             buffers_pct={buffers_pct:.1}%\n",
            target_rows.len(),
        ),
    )
    .expect("write fixture summary");

    eprintln!("evidence capture complete");
}
