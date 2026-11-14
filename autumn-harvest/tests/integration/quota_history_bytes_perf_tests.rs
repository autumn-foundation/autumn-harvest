//! Ledger perf pass: `quota::load_quota_usage`'s `history_bytes` counter
//! (issue #946 AC7 — "cheap by construction... never a full-table scan per
//! admission").
//!
//! # Workload
//!
//! [`crate::quota::load_quota_usage`] runs once per admission attempt. That
//! covers *every* fresh start and every spawned child of a workflow type with
//! a declared [`crate::quota::QuotaPolicy`]. It runs in
//! `execution::enforce_quota_admission`, inside the transaction that holds
//! the per-key advisory lock. Its `history_bytes` counter is an exact
//! `SUM(pg_column_size(event_data))` over every event of the resolved key's
//! active (`RUNNING`/`PAUSED`) executions.
//!
//! # Profile
//!
//! This is opt-in. A deployment with no `QuotaPolicy` pays nothing (AC9).
//! The `enforce_quota_admission` function also skips this query when the
//! policy has no cap or the input resolves no quota key. Within that scope,
//! the trigger is `QuotaPolicy::has_any_cap()`, not `max_history_bytes`
//! alone. By design, `load_quota_usage` computes all three counters in one
//! round trip. So a type that declares only `max_active_executions` or only
//! `max_dead_letters` pays this `history_bytes` cost on each key-resolved
//! admission too. This pass measures that cost as the target tenant's
//! active-execution footprint grows. It also measures it as the shared
//! `harvest_events` table grows around that tenant.
//!
//! # Fixture
//!
//! Seeded, deterministic (no `random()`) and production-shaped. One target
//! tenant (`workflow_name = 'order_saga'`, `quota_key = 'acme'`) has 1,000
//! active executions. The execution index skews each event-history length:
//! most workflows are short, and a tail runs long. So a realistic minority
//! of large histories dominates the aggregate, not a uniform average:
//!
//! | share of 1,000 | events per execution | mechanism |
//! |---:|---:|---|
//! | 5% (i % 20 == 0) | 2,001-2,481 | long-running saga tail |
//! | 20% (i % 20 in 1..=4) | 202-285 | medium workflow |
//! | 75% | 16-30 | typical short workflow |
//!
//! This totals 178,000 events / ~80.2 MB of history for the target tenant.
//! The seeded fixture reproduces byte-for-byte on every run. The
//! `EXPLAIN`/`pg_stat_statements` output does not. Wall-clock timing,
//! cache-dependent hit/read splits and ANALYZE row estimates vary run to
//! run. Only buffer totals are stable.
//!
//! [`NOISE_SWEEP`] scales *background* tenants that share the same tables.
//! Each is another `quota_key` with a light uniform history. They grow
//! independently of the target's own footprint. That matches production:
//! `harvest_events` grows by more tenants, not by one unbounded history.
//! Total table rows: 205k / 313k / 1.08M.
//!
//! The evidence-capture test below is ignored by default: a one-shot tool,
//! not a repeatable CI assertion. Run via
//! `cargo xtask perf quota-history-bytes`. Full writeup:
//! `docs/performance-quota-history-bytes.md`.

#![cfg(feature = "db")]

use diesel::QueryableByName;
use diesel::sql_types::Text;
use diesel_async::RunQueryDsl;

use super::claim_bench_support::db;
use autumn_harvest::quota::{load_quota_usage, quota_usage_query};

/// Background-tenant multiplier at each measured point.
///
/// Total `harvest_events` rows land at roughly 205k / 313k / 1.08M. The
/// target tenant's 1,000-execution / 178,000-event footprint stays fixed.
const NOISE_SWEEP: [i64; 3] = [3, 15, 100];

const TARGET_ACTIVE: i64 = 1_000;
const TARGET_WORKFLOW: &str = "order_saga";
const TARGET_KEY: &str = "acme";

async fn bench_db_or_skip() -> Option<db::BenchDb> {
    match db::setup_bench_db().await {
        Ok(bench) => Some(bench),
        Err(reason) => {
            assert!(
                std::env::var("CI").is_err(),
                "quota history-bytes evidence capture could not reach a database \
                 under CI: {}. Set HARVEST_TEST_DATABASE_URL or start Docker.",
                reason.0,
            );
            eprintln!(
                "SKIP: quota history-bytes evidence capture needs Postgres ({}). \
                 Set HARVEST_TEST_DATABASE_URL or start Docker.",
                reason.0,
            );
            None
        }
    }
}

/// Deterministically (re)seed the target tenant plus `noise_mult * 1000`
/// background executions sharing the same tables, then `ANALYZE`.
///
/// Idempotent: `TRUNCATE`s the three quota-relevant tables first, so calling
/// this repeatedly across [`NOISE_SWEEP`] never accumulates state from a
/// previous size.
#[allow(clippy::too_many_lines)] // one-shot fixture seed, not control flow
async fn seed_fixture(conn: &mut diesel_async::AsyncPgConnection, noise_mult: i64) {
    diesel::sql_query(
        "TRUNCATE harvest_events, harvest_dead_letters, harvest_workflow_executions CASCADE",
    )
    .execute(conn)
    .await
    .expect("truncate quota fixture tables");

    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT gen_random_uuid(), '{TARGET_WORKFLOW}', 'order-' || i, 0, \
                CASE WHEN i % 11 = 0 THEN 'PAUSED' ELSE 'RUNNING' END, \
                jsonb_build_object('tenant_id', '{TARGET_KEY}', 'order_id', i), \
                '{TARGET_KEY}' \
         FROM generate_series(1, {TARGET_ACTIVE}) AS i"
    ))
    .execute(conn)
    .await
    .expect("seed target tenant executions");

    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT gen_random_uuid(), '{TARGET_WORKFLOW}', 'noise-' || i, 0, \
                CASE WHEN i % 7 = 0 THEN 'COMPLETED' ELSE 'RUNNING' END, \
                jsonb_build_object('tenant_id', 'other_' || (i % 200), 'order_id', i), \
                'other_' || (i % 200) \
         FROM generate_series(1, {TARGET_ACTIVE} * {noise_mult}) AS i"
    ))
    .execute(conn)
    .await
    .expect("seed background-tenant executions");

    diesel::sql_query("ANALYZE harvest_workflow_executions")
        .execute(conn)
        .await
        .expect("analyze harvest_workflow_executions");

    // Deterministic skewed event-history length, keyed off the execution
    // index encoded in workflow_id -- see the module doc's table. No
    // random(): the same noise_mult always produces the same event counts
    // and the same history_bytes total, so a captured artifact reproduces
    // exactly on re-run.
    diesel::sql_query(format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         SELECT e.id, gen.event_id, 'ActivityCompleted', \
                jsonb_build_object( \
                  'activity', 'charge_card', \
                  'result', repeat('x', 150 + (gen.event_id % 350)), \
                  'attempt', gen.event_id, \
                  'meta', jsonb_build_object('trace', md5(e.id::text || gen.event_id::text), 'shard', 0) \
                ) \
         FROM harvest_workflow_executions e \
         JOIN LATERAL ( \
             SELECT generate_series( \
                 0, \
                 CASE \
                     WHEN (split_part(e.workflow_id, '-', 2))::int % 20 = 0 \
                         THEN 2000 + (split_part(e.workflow_id, '-', 2))::int % 500 \
                     WHEN (split_part(e.workflow_id, '-', 2))::int % 20 BETWEEN 1 AND 4 \
                         THEN 200 + (split_part(e.workflow_id, '-', 2))::int % 100 \
                     ELSE 10 + (split_part(e.workflow_id, '-', 2))::int % 20 \
                 END \
             ) AS event_id \
         ) gen ON TRUE \
         WHERE e.quota_key = '{TARGET_KEY}'"
    ))
    .execute(conn)
    .await
    .expect("seed target tenant event history");

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         SELECT e.id, gen.event_id, 'ActivityCompleted', \
                jsonb_build_object('activity', 'noise', 'result', repeat('y', 100)) \
         FROM harvest_workflow_executions e \
         JOIN LATERAL (SELECT generate_series(0, 8) AS event_id) gen ON TRUE \
         WHERE e.quota_key != 'acme'",
    )
    .execute(conn)
    .await
    .expect("seed background-tenant event history");

    diesel::sql_query("ANALYZE harvest_events")
        .execute(conn)
        .await
        .expect("analyze harvest_events");

    diesel::sql_query(format!(
        "INSERT INTO harvest_dead_letters \
           (original_task_id, queue_name, task_type, workflow_exec_id, input, error, \
            attempts, workflow_name, quota_key) \
         SELECT gen_random_uuid(), 'default', 'workflow', NULL, '{{}}'::jsonb, 'boom', 1, \
                '{TARGET_WORKFLOW}', '{TARGET_KEY}' \
         FROM generate_series(1, 50)"
    ))
    .execute(conn)
    .await
    .expect("seed target tenant dead letters");

    diesel::sql_query(format!(
        "INSERT INTO harvest_dead_letters \
           (original_task_id, queue_name, task_type, workflow_exec_id, input, error, \
            attempts, workflow_name, quota_key) \
         SELECT gen_random_uuid(), 'default', 'workflow', NULL, '{{}}'::jsonb, 'boom', 1, \
                '{TARGET_WORKFLOW}', 'other_' || (i % 200) \
         FROM generate_series(1, {TARGET_ACTIVE} * {noise_mult}) AS i"
    ))
    .execute(conn)
    .await
    .expect("seed background-tenant dead letters");

    diesel::sql_query("ANALYZE harvest_dead_letters")
        .execute(conn)
        .await
        .expect("analyze harvest_dead_letters");
}

#[derive(QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    query_plan: String,
}

/// Real `load_quota_usage()` calls driven for the `pg_stat_statements`
/// snapshot at the end of the capture.
const STAT_SNAPSHOT_ITERS: usize = 20;

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    total_buffers: i64,
    #[diesel(sql_type = diesel::sql_types::Double)]
    mean_exec_time: f64,
}

async fn explain_quota_usage_query(conn: &mut diesel_async::AsyncPgConnection) -> String {
    let sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) {}",
        quota_usage_query()
    );
    let rows: Vec<ExplainRow> = diesel::sql_query(sql)
        .bind::<Text, _>(TARGET_WORKFLOW)
        .bind::<Text, _>(TARGET_KEY)
        .load(conn)
        .await
        .expect("EXPLAIN quota_usage_query()");
    rows.into_iter()
        .map(|r| r.query_plan)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The negative-result variant, tested and rejected.
///
/// A `LATERAL` per-active-row correlated aggregate forces the intended
/// per-tenant index-bounded plan structurally. It does not rely on the
/// planner's cost-based choice between that shape and a full `Seq Scan`.
/// It measured worse at every scale tried. See
/// `docs/performance-quota-history-bytes.md`.
async fn explain_lateral_variant(conn: &mut diesel_async::AsyncPgConnection) -> String {
    let sql = "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) \
        WITH active AS ( \
            SELECT id FROM harvest_workflow_executions \
            WHERE workflow_name = $1 AND quota_key = $2 AND state IN ('RUNNING', 'PAUSED') \
        ) \
        SELECT \
            (SELECT COUNT(*) FROM active)::BIGINT AS active_executions, \
            COALESCE( \
                (SELECT SUM(ev.bytes) \
                 FROM active a \
                 CROSS JOIN LATERAL ( \
                     SELECT COALESCE(SUM(pg_column_size(e.event_data)), 0) AS bytes \
                     FROM harvest_events e \
                     WHERE e.workflow_exec_id = a.id \
                 ) ev), \
                0 \
            )::BIGINT AS history_bytes, \
            (SELECT COUNT(*) FROM harvest_dead_letters \
             WHERE workflow_name = $1 AND quota_key = $2)::BIGINT AS dead_letters";
    let rows: Vec<ExplainRow> = diesel::sql_query(sql)
        .bind::<Text, _>(TARGET_WORKFLOW)
        .bind::<Text, _>(TARGET_KEY)
        .load(conn)
        .await
        .expect("EXPLAIN LATERAL variant");
    rows.into_iter()
        .map(|r| r.query_plan)
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "evidence generator, not a CI assertion -- run via \
            cargo xtask perf quota-history-bytes"]
#[allow(clippy::too_many_lines)] // one-shot evidence capture, not control flow
async fn zz_capture_quota_history_bytes_evidence() {
    let Some(bench) = bench_db_or_skip().await else {
        eprintln!("no database reachable; nothing captured");
        return;
    };

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("quota-history-bytes-admission");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut summary_lines: Vec<String> = Vec::new();
    let mut conn = db::connect(&bench.url).await;

    for noise_mult in NOISE_SWEEP {
        seed_fixture(&mut conn, noise_mult).await;

        // Correctness sanity check: the fixture's target-tenant footprint is
        // constant across every sweep point regardless of noise_mult, and
        // must match what the real production function returns.
        let usage = load_quota_usage(&mut conn, TARGET_WORKFLOW, TARGET_KEY)
            .await
            .expect("load_quota_usage");
        assert_eq!(
            usage.active_executions, TARGET_ACTIVE,
            "noise_mult={noise_mult}"
        );
        assert_eq!(usage.history_bytes, 80_192_528, "noise_mult={noise_mult}");
        assert_eq!(usage.dead_letters, 50, "noise_mult={noise_mult}");

        let plan_text = explain_quota_usage_query(&mut conn).await;
        let file_name = format!("noise_mult-{noise_mult}.explain.txt");
        std::fs::write(
            out_dir.join(&file_name),
            format!(
                "-- quota_usage_query() @ target=1000 active/178000 events, \
                 noise_mult={noise_mult} --\n{plan_text}\n"
            ),
        )
        .expect("write explain artifact");
        eprintln!("wrote {file_name}");

        if noise_mult == NOISE_SWEEP[0] {
            // Capture the negative result at the smallest size only. There
            // the unmodified query picks the Seq Scan. So a forced plan
            // shape would matter most there, if it helped.
            let lateral_text = explain_lateral_variant(&mut conn).await;
            std::fs::write(
                out_dir.join("lateral-variant-negative-result.explain.txt"),
                format!(
                    "-- REJECTED rewrite: LATERAL per-active-row aggregate @ \
                     noise_mult={noise_mult} -- measured worse than the \
                     unmodified query's own plan choice; see \
                     docs/performance-quota-history-bytes.md --\n{lateral_text}\n"
                ),
            )
            .expect("write lateral-variant artifact");
        }

        summary_lines.push(format!(
            "noise_mult={noise_mult} active_executions={} history_bytes={} dead_letters={}",
            usage.active_executions, usage.history_bytes, usage.dead_letters,
        ));
    }

    // A `pg_stat_statements` snapshot of the REAL `load_quota_usage()`
    // calls, not of the EXPLAIN text above. It runs at the largest fixture,
    // where the query is on its natural Nested-Loop-plus-index plan.
    //
    // Two independent causes can make it unusable on an external
    // `HARVEST_TEST_DATABASE_URL` server. Each skips only this secondary
    // snapshot and does not panic. The EXPLAIN artifacts above are the
    // primary evidence, and they are already written either way:
    // 1. `shared_preload_libraries = 'pg_stat_statements'` was never set.
    //    `CREATE EXTENSION` alone cannot enable it retroactively. The
    //    availability probe below catches this.
    // 2. The role can read `pg_stat_statements` but cannot call
    //    `pg_stat_statements_reset()`. That function defaults to
    //    superuser-only (or an explicit `GRANT EXECUTE`). This realistic
    //    external-admin setup differs from (1), so it needs its own check.
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await;
    let available: Result<i64, _> =
        diesel::sql_query("SELECT count(*)::BIGINT AS count FROM pg_stat_statements WHERE FALSE")
            .get_result::<CountRow>(&mut conn)
            .await
            .map(|r| r.count);

    let reset_result = if available.is_ok() {
        Some(
            diesel::sql_query(
                "SELECT pg_stat_statements_reset(0, \
                        (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
            )
            .execute(&mut conn)
            .await,
        )
    } else {
        None
    };

    let skip_reason = match (&available, &reset_result) {
        (Err(_), _) => Some(
            "pg_stat_statements is not usable on this server (needs \
             shared_preload_libraries = 'pg_stat_statements', which CREATE \
             EXTENSION cannot enable retroactively)",
        ),
        (Ok(_), Some(Err(_))) => Some(
            "pg_stat_statements_reset() failed -- the connected role can \
             likely read pg_stat_statements but lacks permission to reset it \
             (reset defaults to superuser-only)",
        ),
        _ => None,
    };

    if let Some(reason) = skip_reason {
        eprintln!(
            "SKIP: {reason} -- skipping the pg_stat_statements snapshot. The \
             EXPLAIN artifacts above are the primary evidence and are \
             unaffected."
        );
        std::fs::write(
            out_dir.join("pg_stat_statements.txt"),
            format!(
                "-- SKIPPED: {reason}. See the EXPLAIN artifacts for the primary evidence. --\n"
            ),
        )
        .expect("write pg_stat_statements skip notice");
    } else {
        for _ in 0..STAT_SNAPSHOT_ITERS {
            load_quota_usage(&mut conn, TARGET_WORKFLOW, TARGET_KEY)
                .await
                .expect("load_quota_usage");
        }

        // Scope to THIS database's oid on both ends. The reset above already
        // was scoped, but an earlier revision's follow-up SELECT was not. On
        // a shared cluster it then returned other databases' rows for the
        // same query text. Those included stale, already-dropped ephemeral
        // benchmark databases. That revision's committed artifact showed five
        // distinct "calls=20" rows for what this run drives as one. Every
        // statement between the reset and this SELECT ran on this same
        // connection and database. So the current-dbid scope alone makes the
        // match exact. The exact `calls` check below asserts it.
        //
        // A fourth way this can fail is distinct from the other three. PG12
        // (this crate's stated minimum, see README's "Postgres 12+") names
        // this column `mean_time`. Only PG13 renames it to `mean_exec_time`.
        // Queried as `Result` rather than a version probe, so it folds into
        // the same skip path as the others. A version probe would add a second
        // way to detect "cannot get this snapshot here".
        let stats_result: Result<Vec<StatRow>, _> = diesel::sql_query(
            "SELECT calls, shared_blks_hit, shared_blks_read, \
                    (shared_blks_hit + shared_blks_read) AS total_buffers, mean_exec_time \
             FROM pg_stat_statements \
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
               AND query LIKE '%pg_column_size%' \
             ORDER BY total_buffers DESC",
        )
        .load(&mut conn)
        .await;

        if matches!(&stats_result, Ok(s) if s.is_empty()) {
            // A third way the secondary snapshot can be unavailable, distinct
            // from "unpreloaded" and "unauthorized to reset". An external
            // server with `pg_stat_statements.track = 'none'` lets the probe
            // and the reset succeed. It then records no row for the 20 real
            // calls just driven. Like the other two, this skips only this
            // snapshot and does not panic. A **multiple**-row result still
            // asserts below. That result means the dbid scope failed to
            // isolate the production statement, so the evidence is not
            // trusted silently.
            let reason = "pg_stat_statements recorded no row for the driven calls \
                           (the server likely has pg_stat_statements.track = 'none')";
            eprintln!(
                "SKIP: {reason} -- skipping the pg_stat_statements snapshot. The \
                 EXPLAIN artifacts above are the primary evidence and are \
                 unaffected."
            );
            std::fs::write(
                out_dir.join("pg_stat_statements.txt"),
                format!(
                    "-- SKIPPED: {reason}. See the EXPLAIN artifacts for the primary evidence. --\n"
                ),
            )
            .expect("write pg_stat_statements skip notice");
        } else if let Err(e) = &stats_result {
            let reason = format!(
                "querying pg_stat_statements failed ({e}) -- likely a pre-PG13 server, \
                 where this view names the column `mean_time` rather than `mean_exec_time`"
            );
            eprintln!(
                "SKIP: {reason} -- skipping the pg_stat_statements snapshot. The \
                 EXPLAIN artifacts above are the primary evidence and are \
                 unaffected."
            );
            std::fs::write(
                out_dir.join("pg_stat_statements.txt"),
                format!(
                    "-- SKIPPED: {reason}. See the EXPLAIN artifacts for the primary evidence. --\n"
                ),
            )
            .expect("write pg_stat_statements skip notice");
        } else {
            let stats = stats_result.expect("checked Ok and non-empty above");
            assert_eq!(
                stats.len(),
                1,
                "expected exactly one dbid-scoped pg_stat_statements row for \
                 load_quota_usage()'s query text after a scoped reset; got {}: \
                 {stats:?}",
                stats.len()
            );
            assert_eq!(
                stats[0].calls,
                i64::try_from(STAT_SNAPSHOT_ITERS).expect("STAT_SNAPSHOT_ITERS fits in i64"),
                "the single matched row's call count must equal exactly the \
                 number of load_quota_usage() calls just driven"
            );

            let stats_text = stats
                .iter()
                .map(|r| {
                    format!(
                        "calls={} shared_blks_hit={} shared_blks_read={} total_buffers={} mean_exec_time_ms={:.3}",
                        r.calls, r.shared_blks_hit, r.shared_blks_read, r.total_buffers, r.mean_exec_time
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            std::fs::write(
                out_dir.join("pg_stat_statements.txt"),
                format!(
                    "-- pg_stat_statements after {STAT_SNAPSHOT_ITERS} real load_quota_usage() calls \
                     @ noise_mult={} (largest fixture), scoped to this database's dbid --\n{stats_text}\n",
                    NOISE_SWEEP[NOISE_SWEEP.len() - 1]
                ),
            )
            .expect("write pg_stat_statements artifact");
        }
    }

    std::fs::write(
        out_dir.join("fixture-summary.txt"),
        summary_lines.join("\n") + "\n",
    )
    .expect("write fixture summary");

    eprintln!("== capture complete: artifacts in {} ==", out_dir.display());
}

/// Event count `seed_fixture`'s SQL assigns to execution index `i` (1-indexed,
/// matching `generate_series(1, TARGET_ACTIVE)`).
///
/// A pure Rust mirror of the `CASE` expression and `generate_series(0, upper)`
/// event-count formula in the SQL of `seed_fixture`. It makes the module
/// doc's per-bucket ranges a checked regression. A hand re-derivation can
/// silently drift from the SQL, as the doc page itself did (PR #1276). If
/// the `CASE` in `seed_fixture` changes, change this too, or
/// [`event_count_ranges_match_documentation`] fails.
#[allow(dead_code)] // exercised only by the test below
const fn event_count_for_index(i: i64) -> i64 {
    let r = i % 20;
    let upper = if r == 0 {
        2000 + (i % 500)
    } else if r >= 1 && r <= 4 {
        200 + (i % 100)
    } else {
        10 + (i % 20)
    };
    upper + 1 // generate_series(0, upper) is inclusive
}

/// Locks in the per-bucket event-count ranges that the docs publish.
///
/// The module doc and `docs/performance-quota-history-bytes.md` publish
/// 2,001-2,481 / 202-285 / 16-30. An edit to the skew formula that changes
/// those bounds then fails a fast, no-database test. A slow manual review of
/// a committed artifact no longer has to catch it.
///
/// No live Postgres is required. This is pure arithmetic over
/// [`event_count_for_index`]. It lives in this `#[cfg(feature = "db")]`
/// module because it guards the DB-backed fixture that this file seeds. A
/// separate always-on file would split the guard from the code it guards.
#[test]
fn event_count_ranges_match_documentation() {
    let (mut long, mut medium, mut short) = (
        Vec::new() as Vec<i64>,
        Vec::new() as Vec<i64>,
        Vec::new() as Vec<i64>,
    );
    let mut total = 0i64;
    for i in 1..=TARGET_ACTIVE {
        let count = event_count_for_index(i);
        total += count;
        match i % 20 {
            0 => long.push(count),
            1..=4 => medium.push(count),
            _ => short.push(count),
        }
    }

    assert_eq!(long.len(), 50, "long bucket must be 5% of 1,000");
    assert_eq!(medium.len(), 200, "medium bucket must be 20% of 1,000");
    assert_eq!(short.len(), 750, "short bucket must be 75% of 1,000");

    assert_eq!(
        (long.iter().min(), long.iter().max()),
        (Some(&2001), Some(&2481)),
        "long-bucket event-count range drifted from the documented 2,001-2,481"
    );
    assert_eq!(
        (medium.iter().min(), medium.iter().max()),
        (Some(&202), Some(&285)),
        "medium-bucket event-count range drifted from the documented 202-285"
    );
    assert_eq!(
        (short.iter().min(), short.iter().max()),
        (Some(&16), Some(&30)),
        "short-bucket event-count range drifted from the documented 16-30"
    );

    assert_eq!(
        total, 178_000,
        "total seeded event count drifted from the documented 178,000"
    );
}
