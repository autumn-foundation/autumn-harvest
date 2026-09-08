#![cfg(feature = "db")]
//! Ledger performance investigation: `scheduler::overdue_schedule_pass`.
//!
//! `docs/performance-schedule-overdue-aux.md`'s "Known limitations" named
//! this function as carrying the identical per-schedule-loop N+1 shape its
//! own fix batched away from `GET /admin/schedules`'s
//! `load_schedule_overdue_aux_by_shard`, left as a follow-up because it is a
//! periodic background pass, not a per-HTTP-request path (different
//! workload, different profile). This file is that follow-up.
//!
//! `overdue_schedule_pass` runs on every worker's adaptive-interval sampler
//! tick (`worker.rs`'s `overdue_schedule_pass` call site), once per shard,
//! over every schedule row on that shard: for each one it called
//! `scheduler::schedule_running_basis` (a `COUNT(*)` on
//! `harvest_workflow_executions` plus
//! `throttle::pending_throttle_count_for_workflow` -- itself a `to_regclass`
//! existence check *and* a second count query, unconditionally, on every
//! call) and, for calendar-bearing schedules,
//! `scheduler::resolve_effective_fire_at`, which re-queries
//! `calendar::load_exclusions_for_calendar` from scratch even when several
//! schedules share the same calendar. Up to four round trips per schedule
//! row, on every sampler tick, forever, for the lifetime of the process --
//! the same class of bug the aux-lookup fix documents: "workflow/activity
//! bookkeeping queries that are individually trivial but collectively
//! dominant... they will never show up in a buffer ranking, only in a
//! `calls` ranking."
//!
//! The fix reuses the exact batched functions the aux-lookup fix already
//! built and tested (`schedule_running_basis_batch`,
//! `resolve_effective_fire_at_pure` fed by
//! `calendar::load_exclusions_for_calendars`) inside `overdue_schedule_pass`
//! itself: one grouped running-basis query and one grouped
//! calendar-exclusions query, covering every schedule on the shard, instead
//! of up to three round trips per row.
//!
//! Evidence is `pg_stat_statements` call/buffer counts (not wall-clock, not
//! admissible on a shared-vCPU machine) -- the same tool and the same
//! statement-shape filter `schedule_overdue_aux_perf.rs` uses for the sibling
//! fix, over three schedule-population sizes to also demonstrate the O(n) ->
//! O(1) call-count shape directly (not just one point on the curve).

#![allow(clippy::too_many_lines)]

use autumn_harvest::scheduler::overdue_schedule_pass;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── DB bootstrap ────────────────────────────────────────────────────────────

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

/// Creates a fresh, uniquely-named, fully-migrated database off `admin_url`,
/// mirroring `schedule_overdue_aux_perf.rs`'s own convention, so this
/// harness's fixture and `pg_stat_statements` capture cannot collide with --
/// or be polluted by -- any other test/run sharing the same server.
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

// ── Fixture generation ──────────────────────────────────────────────────────

/// The fixed slot every calendar-bearing schedule's `next_run_at` is pinned
/// to; every calendar's exclusion set includes this exact date, so
/// `resolve_effective_fire_at`/`resolve_effective_fire_at_pure` actually
/// rebase it (not a vacuous "calendar present but never excluded" no-op).
/// Identical fixture shape to `schedule_overdue_aux_perf.rs` -- same
/// investigation family, same class of bug -- so the two measurements are
/// directly comparable.
const PINNED_CALENDAR_SLOT: &str = "2026-06-15T12:00:00+00:00";
const CALENDAR_COUNT: i64 = 3;

/// Seeds `n` schedules (each its own `workflow_name`), a shared pool of
/// `CALENDAR_COUNT` calendars (every 10th schedule references one, cycling
/// through them), `RUNNING`/`PAUSED` executions for every 4th schedule's
/// workflow, and pending-throttle rows for every 7th schedule's workflow.
/// Pure set-based SQL, not a per-row Rust loop -- identical to
/// `schedule_overdue_aux_perf.rs::seed_fixture`.
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
             'sched_pass_perf_wf_' || gs,
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
             'sched_pass_perf_wf_' || gs,
             'sched_pass_perf_wf_' || gs || '_exec_' || e,
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
             'sched_pass_perf_wf_' || gs,
             '',
             'sched_pass_perf_bucket_' || gs,
             'sched_pass_perf_throttle_' || gs,
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
    .expect("seed scheduler overdue-pass perf fixture");
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
/// query -- see `schedule_overdue_aux_perf.rs::snapshot_statements`'s doc
/// comment for why a second `pg_stat_statements` query here would pollute
/// whatever total is computed from it (a real bug caught in that sibling
/// investigation's own review).
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
/// the calendar-exclusions lookup on `harvest_calendar_exclusions`. Mirrors
/// `schedule_overdue_aux_perf.rs::is_aux_lookup_statement` exactly (same
/// underlying lookups, same statement shapes).
fn is_aux_lookup_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    (q.contains("harvest_workflow_executions") && q.contains("workflow_name"))
        || q.contains("harvest_start_throttle")
        || q.contains("harvest_calendar_exclusions")
        || q.contains("to_regclass")
}

/// One (schedule count, calls, buffers) measurement, written as one line of
/// the artifact this harness produces.
struct SizePoint {
    n: i64,
    aux_calls: i64,
    aux_buffers: i64,
    schedules_list_calls: i64,
}

async fn measure_one_pass(admin: &str, label: &str, n: i64) -> SizePoint {
    let db_name = unique(&format!("sched_pass_perf_{label}_{n}"));
    let url = create_fresh_db(admin, &db_name).await;

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;
    seed_fixture(&mut seed_conn, n).await;

    // The one real public entry point: the exact function every worker's
    // adaptive-interval sampler tick calls, once per shard
    // (`worker.rs`'s `overdue_schedule_pass` call site).
    let mut pass_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("pass connection");

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    let result = overdue_schedule_pass(&mut pass_conn, Utc::now())
        .await
        .expect("overdue_schedule_pass should succeed");
    assert_eq!(
        i64::try_from(result.samples.len()).unwrap(),
        n,
        "every seeded schedule must appear in the pass"
    );

    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let aux_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_aux_lookup_statement(r))
        .collect();
    assert!(
        !aux_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the aux-lookup shapes after one \
         overdue_schedule_pass call -- check pg_stat_statements.track and \
         shared_preload_libraries",
    );
    let aux_calls: i64 = aux_rows.iter().map(|r| r.calls).sum();
    let aux_buffers: i64 = aux_rows.iter().map(|r| r.total_buffers).sum();
    let schedules_list_calls: i64 = all_rows
        .iter()
        .filter(|r| r.query.to_ascii_lowercase().contains("harvest_schedules"))
        .map(|r| r.calls)
        .sum();

    SizePoint {
        n,
        aux_calls,
        aux_buffers,
        schedules_list_calls,
    }
}

// ── Evidence capture (not a CI assertion) ───────────────────────────────────

/// Sweeps three schedule-population sizes so the artifact demonstrates the
/// O(n) (before) / O(1) (after) call-count shape directly, not just one
/// point on the curve -- run once against the pre-fix code (checked out at
/// the commit before this investigation's change) and once against the
/// post-fix code, with `PERF_LABEL` distinguishing the two artifact sets.
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-schedule-overdue-pass.md"]
async fn zz_capture_overdue_schedule_pass_perf_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("schedule-overdue-pass");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut lines = vec![format!(
        "-- {label}: overdue_schedule_pass, pg_stat_statements aux-lookup call/buffer sweep --\n\
         n\taux_calls\taux_buffers\tschedules_list_calls"
    )];
    for n in [50_i64, 200, 500] {
        let point = measure_one_pass(&admin, &label, n).await;
        eprintln!(
            "label={label} n={} aux_calls={} aux_buffers={} schedules_list_calls={}",
            point.n, point.aux_calls, point.aux_buffers, point.schedules_list_calls
        );
        lines.push(format!(
            "{}\t{}\t{}\t{}",
            point.n, point.aux_calls, point.aux_buffers, point.schedules_list_calls
        ));
    }
    std::fs::write(
        out_dir.join(format!("{label}-sweep.txt")),
        lines.join("\n") + "\n",
    )
    .expect("write sweep artifact");
    eprintln!("evidence capture complete: label={label}");
}

// ── Equivalence: the batched pass vs. the original per-schedule loop ───────

/// Proves `overdue_schedule_pass`'s batched running-basis and calendar
/// lookups agree exactly with the original per-schedule loop (the pre-fix
/// implementation, reconstructed inline here from the still-`pub`,
/// unmodified single-item functions `schedule_running_basis` and
/// `resolve_effective_fire_at`) across the same fixture and connection --
/// mirroring `schedule_overdue_aux_perf.rs`'s equivalence-test pattern for
/// the sibling fix.
#[tokio::test]
async fn overdue_schedule_pass_matches_the_original_per_schedule_loop() {
    use autumn_harvest::models::HarvestSchedule;
    use autumn_harvest::schema::harvest_schedules;
    use diesel::SelectableHelper;

    const N: i64 = 60;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("sched_pass_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    seed_fixture(&mut conn, N).await;

    let schedules: Vec<HarvestSchedule> = harvest_schedules::table
        .select(HarvestSchedule::as_select())
        .load(&mut conn)
        .await
        .expect("load schedules");
    assert_eq!(schedules.len(), usize::try_from(N).unwrap());

    // "Before": the original per-schedule loop, called directly against the
    // still-`pub`, unmodified single-item functions.
    let mut expected_at_capacity = std::collections::HashMap::new();
    let mut expected_effective_fire_at = std::collections::HashMap::new();
    for s in &schedules {
        let name = s
            .dag_name
            .as_deref()
            .or(s.workflow_name.as_deref())
            .unwrap_or("");
        let basis = autumn_harvest::scheduler::schedule_running_basis(&mut conn, name, s.id)
            .await
            .expect("per-schedule basis query");
        expected_at_capacity.insert(s.id, basis >= i64::from(s.max_active_runs));

        let effective_fire_at = autumn_harvest::scheduler::resolve_effective_fire_at(
            &mut conn,
            s.calendar_name.as_deref(),
            &s.skip_policy,
            s.schedule_expr.as_deref(),
            s.next_run_at,
        )
        .await
        .expect("per-schedule resolve_effective_fire_at");
        expected_effective_fire_at.insert(s.id, effective_fire_at);
    }
    let calendar_bearing = schedules
        .iter()
        .filter(|s| s.calendar_name.is_some())
        .count();
    assert!(
        calendar_bearing >= 5,
        "fixture must seed a non-trivial number of calendar-bearing schedules \
         (got {calendar_bearing}) for this equivalence check to mean anything"
    );

    // "After": the batched pass, via the pure predicate's own inputs. We
    // cannot read `overdue_schedule_pass`'s internal `at_capacity` /
    // `effective_fire_at` directly (private to the loop), so this test
    // instead calls the same batched building blocks
    // `overdue_schedule_pass` now uses, the identical way it uses them --
    // proving the *inputs* to the overdue predicate match, which is exactly
    // what the "before" loop above computed.
    let schedule_names: Vec<(uuid::Uuid, &str)> = schedules
        .iter()
        .map(|s| {
            (
                s.id,
                s.dag_name
                    .as_deref()
                    .or(s.workflow_name.as_deref())
                    .unwrap_or(""),
            )
        })
        .collect();
    let basis =
        autumn_harvest::scheduler::schedule_running_basis_batch(&mut conn, &schedule_names)
            .await
            .expect("batched basis query");

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
        let actual_at_capacity =
            basis.get(&s.id).copied().unwrap_or(0) >= i64::from(s.max_active_runs);
        assert_eq!(
            actual_at_capacity, expected_at_capacity[&s.id],
            "batched at_capacity must equal the per-schedule loop's result for schedule {}",
            s.id
        );

        let empty: Vec<chrono::NaiveDate> = Vec::new();
        let actual_effective_fire_at = s.calendar_name.as_deref().and_then(|cal_name| {
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
            actual_effective_fire_at, expected_effective_fire_at[&s.id],
            "batched effective_fire_at must equal the per-schedule loop's result for schedule {} \
             (calendar={:?})",
            s.id, s.calendar_name
        );
        if actual_effective_fire_at.is_some() {
            rebased_count += 1;
        }
    }
    assert!(
        rebased_count >= 5,
        "fixture must exercise real calendar rebasing (got {rebased_count} rebased schedules) \
         -- otherwise this equivalence check never exercises the non-trivial branch"
    );
}
