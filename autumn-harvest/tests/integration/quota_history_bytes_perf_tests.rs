//! Ledger perf pass: `quota::load_quota_usage`'s `history_bytes` counter
//! (issue #946 AC7 — "cheap by construction... never a full-table scan per
//! admission").
//!
//! # Workload
//!
//! [`crate::quota::load_quota_usage`] runs once per admission attempt for
//! *every* fresh start and every spawned child of a workflow type with a
//! declared [`crate::quota::QuotaPolicy`] (`execution::enforce_quota_admission`,
//! inside the same transaction as the per-key advisory lock). Its
//! `history_bytes` counter is an exact `SUM(pg_column_size(event_data))`
//! over every event belonging to the resolved key's currently-active
//! (`RUNNING`/`PAUSED`) executions.
//!
//! # Profile
//!
//! This is opt-in: a deployment with no `QuotaPolicy` pays nothing (AC9). But
//! the trigger is `QuotaPolicy::has_any_cap()` — any declared cap — not
//! specifically `max_history_bytes`: `load_quota_usage` computes all three
//! counters in one round trip by design, so a workflow type declaring only
//! `max_active_executions` or only `max_dead_letters` pays this same
//! `history_bytes` cost on every admission too. This pass measures that cost
//! as the target tenant's own accumulated active-execution footprint grows,
//! and as the total `harvest_events` table (shared with every other tenant)
//! grows around it.
//!
//! # Fixture
//!
//! Seeded, deterministic (no `random()`), production-shaped: one target
//! tenant (`workflow_name = 'order_saga'`, `quota_key = 'acme'`) with 1,000
//! active executions whose event-history length is skewed by execution
//! index rather than uniform — most workflows short, a tail long-running —
//! so the aggregate is dominated by a realistic minority of large
//! histories, not by a uniform average:
//!
//! | share of 1,000 | events per execution | mechanism |
//! |---:|---:|---|
//! | 5% (i % 20 == 0) | 2,000-2,499 | long-running saga tail |
//! | 20% (i % 20 in 1..=4) | 200-299 | medium workflow |
//! | 75% | 10-29 | typical short workflow |
//!
//! This totals 178,000 events / ~80.2 MB of history for the target tenant,
//! reproducing byte-for-byte on every run. [`NOISE_SWEEP`] scales
//! *background* tenants sharing the same tables — other `quota_key`s, each
//! with a light uniform history — independently of the target's own
//! footprint, matching how `harvest_events` actually grows in production
//! (more tenants over time, not one tenant's history growing without
//! bound): 205k / 313k / 1.08M total table rows.
//!
//! `#[ignore]`d: a one-shot evidence-capture tool, not a repeatable CI
//! assertion. Run via
//! `autumn-harvest/scripts/quota_history_bytes_perf_repro.sh`. Full writeup:
//! `docs/performance-quota-history-bytes.md`.

#![cfg(feature = "db")]

use diesel::QueryableByName;
use diesel::sql_types::Text;
use diesel_async::RunQueryDsl;

use super::claim_bench_support::db;
use autumn_harvest::quota::{load_quota_usage, quota_usage_query};

/// Background-tenant multiplier at each measured point: total `harvest_events`
/// rows in the fixture land at roughly 205k / 313k / 1.08M while the target
/// tenant's own 1,000-execution / 178,000-event footprint stays fixed.
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

/// The negative-result variant tested and rejected: forces the intended
/// per-tenant index-bounded plan structurally via a `LATERAL` per-active-row
/// correlated aggregate, instead of relying on the planner's own cost-based
/// choice between that shape and a full `Seq Scan`. Measured worse at every
/// scale tried -- see `docs/performance-quota-history-bytes.md`.
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
            autumn-harvest/scripts/quota_history_bytes_perf_repro.sh"]
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
            // Negative-result capture: only at the smallest size, which is
            // where the unmodified query currently picks the Seq Scan and
            // therefore where a forced-plan-shape fix would matter most, if
            // it helped.
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

    // `pg_stat_statements` snapshot from the REAL `load_quota_usage()` calls
    // (not the literal EXPLAIN text above), at the largest fixture, where
    // the query is on its natural Nested-Loop-plus-index plan.
    //
    // Two independent ways this can be unusable on an external
    // `HARVEST_TEST_DATABASE_URL` server, both degrading to the same skip of
    // just this secondary snapshot rather than a panic -- the EXPLAIN
    // artifacts above, which are this page's primary evidence, are already
    // written either way:
    // 1. `shared_preload_libraries = 'pg_stat_statements'` was never set --
    //    `CREATE EXTENSION` alone cannot enable it retroactively. Caught by
    //    the availability probe below.
    // 2. The connected role can read `pg_stat_statements` but lacks
    //    permission to call `pg_stat_statements_reset()`, which defaults to
    //    superuser-only (or an explicit `GRANT EXECUTE`) -- a realistic
    //    external-admin configuration distinct from (1), so it needs its own
    //    check rather than assuming the probe above already covered it.
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

        // Scoped to THIS database's oid on both ends: the reset above already
        // was, but an earlier revision's follow-up SELECT was not, so on a
        // shared cluster it also returned other databases' (including stale,
        // already-dropped ephemeral benchmark databases') rows for the same
        // query text -- confirmed by that revision's committed artifact,
        // which showed five distinct "calls=20" rows for what this run
        // drives as one. Every statement between the reset and this SELECT
        // ran on this same connection/database, so the current-dbid scope
        // alone now makes the match exact -- asserted, not just hoped, via
        // the exact `calls` check below.
        let stats: Vec<StatRow> = diesel::sql_query(
            "SELECT calls, shared_blks_hit, shared_blks_read, \
                    (shared_blks_hit + shared_blks_read) AS total_buffers, mean_exec_time \
             FROM pg_stat_statements \
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
               AND query LIKE '%pg_column_size%' \
             ORDER BY total_buffers DESC",
        )
        .load(&mut conn)
        .await
        .expect("query pg_stat_statements");

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

    std::fs::write(
        out_dir.join("fixture-summary.txt"),
        summary_lines.join("\n") + "\n",
    )
    .expect("write fixture summary");

    eprintln!("== capture complete: artifacts in {} ==", out_dir.display());
}
