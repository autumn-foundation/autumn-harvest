//! Ledger performance investigation: `GET /admin/schedules`.
//!
//! `load_schedule_overdue_aux_by_shard` (in `autumn-harvest-plugin/src/api.rs`)
//! computes each schedule's overdue-suppression basis and calendar-adjusted
//! fire time **one schedule row at a time**: for every schedule on the shard
//! it called `scheduler::schedule_running_basis` -- a `COUNT(*)` on
//! `harvest_workflow_executions` plus `throttle::pending_throttle_count_for_workflow`
//! (itself a `to_regclass` existence check *and* a second count query, always,
//! on every call) -- and, for calendar-bearing schedules,
//! `scheduler::resolve_effective_fire_at`, which re-queries
//! `calendar::load_exclusions_for_calendar` from scratch even when several
//! schedules share the same calendar. Up to four round trips per schedule
//! row, every one of them on every single `GET /admin/schedules` request --
//! exactly the class of bug Harvest's own performance-agent playbook calls
//! out: "workflow/activity bookkeeping queries that are individually trivial
//! but collectively dominant... they will never show up in a buffer ranking,
//! only in a `calls` ranking."
//!
//! The fix batches all three lookups per shard: one grouped `RUNNING`/`PAUSED`
//! count query (`scheduler::schedule_running_basis_batch`), one grouped
//! pending-throttle count query
//! (`throttle::pending_throttle_counts_for_workflows`), and one grouped
//! calendar-exclusions query keyed by *distinct* calendar name
//! (`calendar::load_exclusions_for_calendars`) -- each covering every
//! schedule row on the shard at once. The per-schedule decision itself
//! (`scheduler::resolve_effective_fire_at_pure`) is unchanged, pure, DB-free
//! logic; only how its inputs are fetched changed.
//!
//! This file is the harness + evidence generator for that investigation, plus
//! two permanent equivalence regression tests comparing the batched
//! functions' output against the original (unmodified, still-`pub`)
//! per-schedule functions across the same fixture.

#![allow(clippy::too_many_lines, clippy::similar_names)]

use std::collections::HashMap;

use autumn_harvest::models::HarvestSchedule;
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
use diesel::SelectableHelper;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::Value;
use std::sync::Arc;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// ── DB bootstrap ────────────────────────────────────────────────────────────

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

/// Creates a fresh, uniquely-named, fully-migrated database off `admin_url`
/// (treated as an admin connection string, matching `claim_bench_support`'s
/// and `workflow_children_traversal_perf.rs`'s own convention) so this
/// harness's ~500-row fixture and `pg_stat_statements` capture cannot collide
/// with -- or be polluted by -- any other test/run sharing the same server.
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
    api_state.install_storage_pool(pool);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("schedule-overdue-aux-perf-test".to_string()),
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

/// The fixed slot every calendar-bearing schedule's `next_run_at` is pinned
/// to; every calendar's exclusion set includes this exact date, so
/// `resolve_effective_fire_at`/`resolve_effective_fire_at_pure` actually
/// rebase it (not a vacuous "calendar present but never excluded" no-op).
const PINNED_CALENDAR_SLOT: &str = "2026-06-15T12:00:00+00:00";
const CALENDAR_COUNT: i64 = 3;

/// Seeds `n` schedules (each its own `workflow_name`), a shared pool of
/// `CALENDAR_COUNT` calendars (every 10th schedule references one, cycling
/// through them -- so several schedules share a calendar, which is exactly
/// the case the batched calendar-exclusions query collapses from
/// O(schedules-with-a-calendar) to O(distinct calendars)), `RUNNING`/`PAUSED`
/// executions for every 4th schedule's workflow (a mix that trips
/// `at_capacity` for some `max_active_runs` values and not others), and
/// pending-throttle rows for every 7th schedule's workflow. Pure set-based
/// SQL, not a per-row Rust loop.
async fn seed_fixture(conn: &mut AsyncPgConnection, n: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_calendars (id, name, built_in, created_at, updated_at)
         SELECT gen_random_uuid(), 'sched_perf_cal_' || g, false, NOW(), NOW()
         FROM generate_series(1, {CALENDAR_COUNT}) AS g;

         INSERT INTO harvest_calendar_exclusions (id, calendar_name, excluded_date, created_at)
         SELECT gen_random_uuid(), 'sched_perf_cal_' || g, d::date, NOW()
         FROM generate_series(1, {CALENDAR_COUNT}) AS g
         CROSS JOIN (VALUES ('{PINNED_CALENDAR_SLOT}'::date), ('2026-01-05'::date),
                             ('2026-02-11'::date), ('2026-03-22'::date)) AS d(d);

         INSERT INTO harvest_schedules (
             id, dag_name, schedule_expr, timezone, catchup, max_active_runs, is_paused,
             next_run_at, created_at, updated_at, workflow_name, queue_name, jitter_secs,
             overlap_policy, buffered_runs, buffer_all_max, calendar_name, skip_policy
         )
         SELECT
             gen_random_uuid(),
             NULL,
             'interval:3600',
             'UTC',
             false,
             (1 + (gs % 5))::int4,
             false,
             CASE WHEN gs % 10 = 0 THEN '{PINNED_CALENDAR_SLOT}'::timestamptz
                  ELSE NOW() - (random() * interval '2 hours') END,
             NOW(), NOW(),
             'sched_perf_wf_' || gs,
             'default',
             0,
             'skip',
             '[]'::jsonb,
             100,
             CASE WHEN gs % 10 = 0 THEN 'sched_perf_cal_' || (1 + gs % {CALENDAR_COUNT}) ELSE NULL END,
             CASE WHEN gs % 10 = 0 THEN 'run_next_business_day' ELSE 'skip' END
         FROM generate_series(1, {n}) AS gs;

         INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name,
             started_at, created_at
         )
         SELECT
             gen_random_uuid(),
             'sched_perf_wf_' || gs,
             'sched_perf_wf_' || gs || '_exec_' || e,
             gen_random_uuid(),
             0,
             CASE WHEN e % 3 = 0 THEN 'PAUSED' ELSE 'RUNNING' END,
             '{{}}'::jsonb,
             'default',
             NOW(),
             NOW()
         FROM generate_series(1, {n}) AS gs
         CROSS JOIN generate_series(1, 3) AS e
         WHERE gs % 4 = 0;

         INSERT INTO harvest_start_throttle (
             id, workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at, shard_id, created_at
         )
         SELECT
             gen_random_uuid(),
             'sched_perf_wf_' || gs,
             '',
             'sched_perf_bucket_' || gs,
             'sched_perf_throttle_' || gs,
             'default',
             '{{}}'::jsonb,
             '{{}}'::jsonb,
             NOW(),
             0,
             NOW()
         FROM generate_series(1, {n}) AS gs
         WHERE gs % 7 = 0;

         ANALYZE harvest_schedules;
         ANALYZE harvest_workflow_executions;
         ANALYZE harvest_start_throttle;
         ANALYZE harvest_calendar_exclusions;"
    ))
    .await
    .expect("seed schedule-overdue-aux perf fixture");
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
/// query -- deliberately not split into a filtered "aux" query followed by a
/// separate unfiltered "everything" query. `pg_stat_statements` tracks
/// queries *against itself* like any other statement, so a first snapshot
/// query would become a new row a second snapshot query could then pick up,
/// inflating whatever total is computed from the later one (Codex review, PR
/// #1314). Querying exactly once per label and deriving both the aux-lookup
/// subset and the whole-request total from that single result set in Rust
/// (see [`is_aux_lookup_statement`]) avoids that self-pollution entirely.
/// `pg_stat_statements` itself is excluded from the snapshot for the same
/// reason -- a row for this very `SELECT` would otherwise appear in its own
/// results.
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

/// Whether `row` is one of the statement shapes this investigation targets:
/// the running-basis count on `harvest_workflow_executions`, the throttle
/// existence-check (`to_regclass`) and count on `harvest_start_throttle`, and
/// the calendar-exclusions lookup on `harvest_calendar_exclusions` -- each
/// identified by a distinctive table/function + column substring so this
/// can't accidentally match an unrelated statement shape (e.g. the
/// schedule-list's own `SELECT * FROM harvest_schedules`).
///
/// `to_regclass` is matched on its own: `pg_stat_statements` normalizes the
/// literal `'harvest_start_throttle'` argument in
/// `pending_throttle_count_for_workflow`'s existence check into a `$N`
/// placeholder in the stored query text (constant-jumbling), so the table
/// name itself is not present in that row's `query` column even though the
/// row belongs to this investigation.
fn is_aux_lookup_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    (q.contains("harvest_workflow_executions") && q.contains("workflow_name"))
        || q.contains("harvest_start_throttle")
        || q.contains("harvest_calendar_exclusions")
        || q.contains("to_regclass")
}

// ── Evidence capture (not a CI assertion) ───────────────────────────────────

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-schedule-overdue-aux.md"]
async fn zz_capture_schedule_overdue_aux_perf_evidence() {
    const N: i64 = 500;

    let (admin, _guard) = setup_server().await;
    let db_name = unique("schedule_overdue_aux_perf");
    let url = create_fresh_db(&admin, &db_name).await;
    let pool = build_pool(&url);
    let app = build_app(HarvestDbPool::from(pool));

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;

    seed_fixture(&mut seed_conn, N).await;

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-plugin/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("schedule-overdue-aux");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());
    eprintln!("== capturing label={label} ==");

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    // The one, real, public entry point -- the exact request Vantage's
    // schedules management page (issue #951) makes on load.
    let (status, body) = get_json(&app, "/admin/schedules").await;
    assert_eq!(status, StatusCode::OK, "schedule list request must succeed");
    let returned = body.as_array().expect("schedules list is an array").len();
    assert_eq!(
        i64::try_from(returned).unwrap(),
        N,
        "every seeded schedule must appear in the list"
    );

    // ONE snapshot query for both views below -- see `snapshot_statements`'s
    // doc comment for why a second query against `pg_stat_statements` here
    // would pollute whatever total is computed from it.
    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let stats_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_aux_lookup_statement(r))
        .collect();
    assert!(
        !stats_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the aux-lookup shapes after one real \
         GET /admin/schedules request -- check pg_stat_statements.track (must be 'all' or \
         'top') and shared_preload_libraries",
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
            "-- {label}: pg_stat_statements @ one real GET /admin/schedules request, \
             {N} schedules, {CALENDAR_COUNT} shared calendars --\n\
             TOTAL across matching statement shapes: calls={total_calls} buffers={total_buffers}\n\n\
             {stats_text}\n"
        ),
    )
    .expect("write pg_stat_statements artifact");
    eprintln!("total_calls={total_calls} total_buffers={total_buffers}");

    // The same snapshot's unfiltered total, so the PR write-up can state the
    // aux-lookup shapes' share of the whole request (profiling step 2: "state
    // the percentage. If under 5% of buffers *and* under 5% of calls, stop").
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
             one real GET /admin/schedules request, {N} schedules --\n\
             REQUEST TOTAL: calls={request_total_calls} buffers={request_total_buffers}\n\
             aux-lookup shapes' share: calls={total_calls}/{request_total_calls} \
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
             fixture: {N} schedules, {CALENDAR_COUNT} shared calendars (every 10th schedule \
             references one), RUNNING/PAUSED executions seeded for every 4th schedule's \
             workflow, pending-throttle rows seeded for every 7th\n\
             request: GET /admin/schedules\n\
             returned_rows={returned}\n\
             aux_statement_shapes={}\n\
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

// ── Equivalence: batched functions vs. the original per-item functions ─────

/// Loads every schedule row directly (bypassing the HTTP layer) so the
/// equivalence tests below can compute both the "before" (per-item, looped)
/// and "after" (batched) aux values against the identical fixture in the
/// identical run.
async fn load_all_schedules(conn: &mut AsyncPgConnection) -> Vec<HarvestSchedule> {
    use autumn_harvest::schema::harvest_schedules;
    harvest_schedules::table
        .select(HarvestSchedule::as_select())
        .load(conn)
        .await
        .expect("load schedules")
}

/// Proves `schedule_running_basis_batch` is exactly equivalent to calling the
/// original, unmodified `schedule_running_basis` once per schedule: same
/// fixture, same connection, same run. A moderate-scale fixture (60
/// schedules -- enough distinct names, running populations and throttle rows
/// to be non-trivial) so this runs fast enough to stay in the default suite,
/// unlike the 500-row evidence capture above.
///
/// Keyed by schedule id rather than name (issue #1160): `schedule_running_basis`
/// gained a `schedule_id`-scoped disjunct so a cross-type continue-as-new
/// successor counts toward its schedule's `max_active_runs` too, so the batch
/// form's result is no longer just a function of `name` alone.
#[tokio::test]
async fn schedule_running_basis_batch_matches_per_schedule_loop() {
    const N: i64 = 60;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("schedule_basis_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    seed_fixture(&mut conn, N).await;

    let schedules = load_all_schedules(&mut conn).await;
    assert_eq!(schedules.len(), usize::try_from(N).unwrap());

    let owned_pairs: Vec<(uuid::Uuid, String)> = schedules
        .iter()
        .map(|s| {
            (
                s.id,
                s.dag_name
                    .as_deref()
                    .or(s.workflow_name.as_deref())
                    .unwrap_or("")
                    .to_string(),
            )
        })
        .collect();
    let pairs: Vec<(uuid::Uuid, &str)> = owned_pairs
        .iter()
        .map(|(id, name)| (*id, name.as_str()))
        .collect();

    let batched = autumn_harvest::scheduler::schedule_running_basis_batch(&mut conn, &pairs)
        .await
        .expect("batched basis query");

    let mut per_schedule: HashMap<uuid::Uuid, i64> = HashMap::new();
    for (id, name) in &pairs {
        let basis = autumn_harvest::scheduler::schedule_running_basis(&mut conn, name, *id)
            .await
            .expect("per-schedule basis query");
        per_schedule.insert(*id, basis);
    }

    for (id, name) in &pairs {
        let expected = per_schedule.get(id).copied().unwrap_or(0);
        let actual = batched.get(id).copied().unwrap_or(0);
        assert_eq!(
            actual, expected,
            "batched running-basis for schedule {id} ({name:?}) must equal the per-schedule loop's result"
        );
    }
}

/// (issue #1160) A cross-type continue-as-new successor -- a RUNNING execution
/// carrying schedule A's `schedule_id` but schedule B's `workflow_name` -- must
/// count toward BOTH the batched and per-schedule running basis for schedule
/// A, additively on top of A's own same-type count (which still includes a
/// manual-trigger run of A's own type, proving the disjunct doesn't regress
/// existing same-type counting).
#[tokio::test]
async fn schedule_running_basis_counts_a_cross_type_successor_additively() {
    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("schedule_basis_cross_type")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let schedule_a = "cross_type_basis_schedule_a";
    let schedule_b = "cross_type_basis_schedule_b";
    let schedule_a_id = uuid::Uuid::new_v4();
    let schedule_b_id = uuid::Uuid::new_v4();

    conn.batch_execute(&format!(
        "INSERT INTO harvest_schedules (
             id, dag_name, schedule_expr, timezone, catchup, max_active_runs, is_paused,
             next_run_at, created_at, updated_at, workflow_name, queue_name, jitter_secs,
             overlap_policy, buffered_runs, buffer_all_max, calendar_name, skip_policy
         ) VALUES
         ('{schedule_a_id}', NULL, '*/30 * * * * *', 'UTC', false, 1, false,
          NOW() + interval '1 hour', NOW(), NOW(), '{schedule_a}', 'default', 0,
          'skip', '[]'::jsonb, 1, NULL, 'skip'),
         ('{schedule_b_id}', NULL, '*/30 * * * * *', 'UTC', false, 1, false,
          NOW() + interval '1 hour', NOW(), NOW(), '{schedule_b}', 'default', 0,
          'skip', '[]'::jsonb, 1, NULL, 'skip');

         -- A manual-trigger run of schedule A's OWN type: schedule_id NULL,
         -- origin = manual_trigger. Must still count toward A (regression guard).
         INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at, schedule_id, origin
         ) VALUES (
             gen_random_uuid(), '{schedule_a}', 'manual-a', gen_random_uuid(), 0, 'RUNNING',
             'null'::jsonb, 'default', NOW(), NOW(), NULL, 'manual_trigger'
         );

         -- The cross-type successor: schedule A's schedule_id, schedule B's
         -- workflow_name -- exactly what ctx.continue_as_new_as(...) (#803)
         -- leaves behind mid-chain.
         INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input,
             queue_name, started_at, created_at, schedule_id, origin
         ) VALUES (
             gen_random_uuid(), '{schedule_b}', 'sched:{schedule_a_id}:successor', gen_random_uuid(), 0,
             'RUNNING', 'null'::jsonb, 'default', NOW(), NOW(), '{schedule_a_id}', 'scheduled'
         );"
    ))
    .await
    .expect("seed cross-type fixture");

    let basis_a = autumn_harvest::scheduler::schedule_running_basis(&mut conn, schedule_a, schedule_a_id)
        .await
        .expect("schedule A basis query");
    assert_eq!(
        basis_a, 2,
        "schedule A's basis must be 2: its own manual-trigger run PLUS the cross-type successor"
    );

    let basis_b = autumn_harvest::scheduler::schedule_running_basis(&mut conn, schedule_b, schedule_b_id)
        .await
        .expect("schedule B basis query");
    assert_eq!(
        basis_b, 1,
        "schedule B's basis counts the same execution once (it IS schedule B's own type), \
         not twice just because it also happens to carry schedule A's schedule_id"
    );

    let pairs = [(schedule_a_id, schedule_a), (schedule_b_id, schedule_b)];
    let batched = autumn_harvest::scheduler::schedule_running_basis_batch(&mut conn, &pairs)
        .await
        .expect("batched basis query");
    assert_eq!(
        batched.get(&schedule_a_id).copied().unwrap_or(0),
        2,
        "batched form must agree with the per-schedule form for A"
    );
    assert_eq!(
        batched.get(&schedule_b_id).copied().unwrap_or(0),
        1,
        "batched form must agree with the per-schedule form for B"
    );
}

/// Proves `resolve_effective_fire_at_pure`, fed the batched
/// `load_exclusions_for_calendars` result, is exactly equivalent to calling
/// the original, unmodified `resolve_effective_fire_at` once per schedule --
/// including the schedules with no calendar at all (both must agree on
/// `None` without ever touching the exclusions map).
#[tokio::test]
async fn resolve_effective_fire_at_pure_matches_resolve_effective_fire_at() {
    const N: i64 = 60;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("schedule_calendar_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    seed_fixture(&mut conn, N).await;

    let schedules = load_all_schedules(&mut conn).await;
    let calendar_bearing = schedules
        .iter()
        .filter(|s| s.calendar_name.is_some())
        .count();
    assert!(
        calendar_bearing >= 5,
        "fixture must seed a non-trivial number of calendar-bearing schedules \
         (got {calendar_bearing}) for this equivalence check to mean anything"
    );

    let calendar_names: Vec<&str> = schedules
        .iter()
        .filter_map(|s| s.calendar_name.as_deref())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let exclusions =
        autumn_harvest::calendar::load_exclusions_for_calendars(&mut conn, &calendar_names)
            .await
            .expect("batched exclusions query");

    let mut rebased_count = 0;
    for s in &schedules {
        let expected = autumn_harvest::scheduler::resolve_effective_fire_at(
            &mut conn,
            s.calendar_name.as_deref(),
            &s.skip_policy,
            s.schedule_expr.as_deref(),
            s.next_run_at,
        )
        .await
        .expect("per-schedule resolve_effective_fire_at");

        let empty: Vec<chrono::NaiveDate> = Vec::new();
        let actual = s.calendar_name.as_deref().and_then(|cal_name| {
            let excluded = exclusions.get(cal_name).unwrap_or(&empty);
            let exclude_weekends = autumn_harvest::calendar::calendar_excludes_weekends(cal_name);
            autumn_harvest::scheduler::resolve_effective_fire_at_pure(
                excluded,
                exclude_weekends,
                &s.skip_policy,
                s.schedule_expr.as_deref(),
                s.next_run_at,
            )
        });

        assert_eq!(
            actual, expected,
            "resolve_effective_fire_at_pure must match resolve_effective_fire_at exactly \
             for schedule {} (calendar={:?})",
            s.id, s.calendar_name
        );
        if expected.is_some() {
            rebased_count += 1;
        }
    }
    assert!(
        rebased_count >= 5,
        "fixture must exercise real calendar rebasing (got {rebased_count} rebased schedules) \
         -- otherwise this equivalence check never exercises the non-trivial branch"
    );
}
