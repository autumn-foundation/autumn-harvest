//! Ledger perf pass: profiling `timeout::enforce_timeouts_once`'s scanner
//! queries against a production-shaped fixture (issue #786's own "known
//! limitations" note: "The scheduler tick and the timeout scanner are not
//! benchmarked here").
//!
//! This is a **negative-result** capture, not a fix. The hypothesis under
//! test: `schedule_to_start_timeout_query()` carries two correlated
//! `NOT EXISTS` anti-joins (`harvest_queue_pauses`, `harvest_activity_pauses`)
//! in the *same shape* the claim query's queue-pause anti-join had before it
//! was rewritten into a `MATERIALIZED` CTE prefilter (see
//! `docs/performance.md#the-queue-pause-anti-join-fix`) — so the same
//! `loops=N` correlated-subquery cost might reappear here, unfixed. Measured
//! against a fixture with a realistic cardinality skew (a large bulk of
//! terminal `harvest_task_queue` rows dwarfing a small live population, which
//! is what a busy, unretired production table actually looks like — see
//! `docs/performance-timeout-scanner.md`), it does not reproduce: Postgres
//! already resolves both anti-joins via a one-time `Materialize` node (the
//! pause tables are tiny, PK-indexed, low-cardinality) rather than a
//! per-candidate-row probe, and the dominant base-table scan is already
//! served by an existing partial index. Every scanner query this file
//! exercises is already cheap at realistic scale.
//!
//! `#[ignore]`d: this is a one-shot evidence-capture tool, not a repeatable CI
//! assertion. Run via
//! `autumn-harvest/scripts/timeout_scanner_perf_repro.sh`, which writes its
//! output into `docs/perf-artifacts/timeout-scanner-queries/`.
#![cfg(feature = "db")]

use std::time::Duration;

use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ShardId;
use diesel::QueryableByName;
use diesel_async::RunQueryDsl;

use super::claim_bench_support::db;

#[derive(QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = diesel::sql_types::Text, column_name = "QUERY PLAN")]
    query_plan: String,
}

#[derive(QueryableByName, Debug)]
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

/// Seed a production-shaped fixture directly on `conn`:
///
/// * `harvest_task_queue`: 300 000 terminal (`COMPLETED`) rows -- the bulk a
///   busy, not-yet-retention-swept table accumulates from long-lived
///   workflows' completed activities -- plus a small `RUNNING` population
///   (2 000 rows, a plausible worker-fleet concurrency ceiling) and a small
///   `PENDING` population (5 000 rows, this repo's own published claim-bench
///   headline backlog). Only 1-in-20/1-in-30/1-in-10 of the live rows carry
///   `heartbeat_timeout`/`start_to_close`/`schedule_to_start` at all --
///   realistic, since most callers do not set these optional timeouts -- and
///   all seeded values are already expired so every scanner's `WHERE` clause
///   has genuine matching rows to find, not an empty result set.
/// * One queue held by `harvest_queue_pauses`, one activity name held by
///   `harvest_activity_pauses` -- both tiny, PK-indexed tables, exercising
///   the two anti-joins `schedule_to_start_timeout_query()` evaluates.
/// * `harvest_workflow_executions`: 100 000 `RUNNING` rows for
///   `workflow_execution_timeout_query()`/the SLA-breach scan, a sparse
///   `deadline_at`/`chain_deadline_at`/`sla_deadline_at` population (roughly
///   1-in-500/1-in-700/1-in-300), and a handful of genuinely `PAUSED` rows
///   referenced by `PENDING` task rows' `schedule_to_close_at`, so
///   `schedule_to_start_timeout_query()`'s frozen-row carve-out subplan has
///   real rows to match too.
#[allow(clippy::too_many_lines)] // one-shot fixture seeding, not production code
async fn seed_production_shaped_fixture(conn: &mut diesel_async::AsyncPgConnection) {
    diesel::sql_query(
        "INSERT INTO harvest_task_queue (
             id, task_type, queue_name, state, priority, scheduled_at, activity_name,
             workflow_exec_id, input, retry_policy, completed_at
         )
         SELECT gen_random_uuid(), 'activity', 'queue_' || (i % 4), 'COMPLETED', 0,
                NOW() - interval '1 day', 'act_' || (i % 20), NULL, '{}'::jsonb, '{}'::jsonb, NOW()
         FROM generate_series(1, 300000) AS i",
    )
    .execute(conn)
    .await
    .expect("seed terminal task_queue bulk");

    diesel::sql_query(
        "INSERT INTO harvest_task_queue (
             id, task_type, queue_name, state, priority, scheduled_at, activity_name,
             workflow_exec_id, input, retry_policy, started_at, start_to_close,
             heartbeat_timeout, last_heartbeat_at
         )
         SELECT gen_random_uuid(), 'activity', 'queue_' || (i % 4), 'RUNNING', 0,
                NOW() - interval '1 hour', 'act_' || (i % 20), NULL, '{}'::jsonb, '{}'::jsonb,
                NOW() - interval '1 hour',
                CASE WHEN i % 30 = 0 THEN interval '1 second' ELSE NULL END,
                CASE WHEN i % 20 = 0 THEN interval '1 second' ELSE NULL END,
                NOW() - interval '1 hour'
         FROM generate_series(1, 2000) AS i",
    )
    .execute(conn)
    .await
    .expect("seed RUNNING task_queue rows");

    // A handful of genuinely PAUSED executions for the frozen-row carve-out
    // subplan inside schedule_to_start_timeout_query().
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (
             id, workflow_id, workflow_name, queue_name, state, started_at, input, shard_id
         )
         SELECT gen_random_uuid(), 'paused-wf-' || i, 'paused_wf', 'queue_0', 'PAUSED',
                NOW() - interval '2 hours', '{}'::jsonb, 0
         FROM generate_series(1, 25) AS i",
    )
    .execute(conn)
    .await
    .expect("seed a handful of PAUSED executions");

    diesel::sql_query(
        "INSERT INTO harvest_task_queue (
             id, task_type, queue_name, state, priority, scheduled_at, activity_name,
             workflow_exec_id, schedule_to_start, schedule_to_close_at, input, retry_policy
         )
         SELECT gen_random_uuid(), 'activity', 'queue_' || (i % 4), 'PENDING', 0,
                NOW() - interval '10 minutes', 'act_' || (i % 20),
                CASE WHEN i % 200 = 0
                     THEN (SELECT id FROM harvest_workflow_executions
                           WHERE workflow_name = 'paused_wf'
                           OFFSET (i % 25) LIMIT 1)
                     ELSE NULL END,
                CASE WHEN i % 10 = 0 THEN interval '30 seconds' ELSE NULL END,
                CASE WHEN i % 200 = 0 THEN NOW() - interval '1 minute' ELSE NULL END,
                '{}'::jsonb, '{}'::jsonb
         FROM generate_series(1, 5000) AS i",
    )
    .execute(conn)
    .await
    .expect("seed PENDING task_queue rows");

    diesel::sql_query(
        "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ('queue_0', 'ledger-evidence-capture')",
    )
    .execute(conn)
    .await
    .expect("seed one active queue pause");
    diesel::sql_query(
        "INSERT INTO harvest_activity_pauses (activity_name, reason) VALUES ('act_5', 'ledger-evidence-capture')",
    )
    .execute(conn)
    .await
    .expect("seed one active activity pause");

    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (
             id, workflow_id, workflow_name, queue_name, state, started_at, input, shard_id,
             deadline_at, chain_deadline_at, sla_deadline_at
         )
         SELECT gen_random_uuid(), 'wf-' || i, 'wf_' || (i % 50), 'queue_' || (i % 4), 'RUNNING',
                NOW() - interval '1 hour', '{}'::jsonb, 0,
                CASE WHEN i % 500 = 0 THEN NOW() - interval '1 minute' ELSE NULL END,
                CASE WHEN i % 700 = 0 THEN NOW() - interval '1 minute' ELSE NULL END,
                CASE WHEN i % 300 = 0 THEN NOW() - interval '1 minute' ELSE NULL END
         FROM generate_series(1, 100000) AS i",
    )
    .execute(conn)
    .await
    .expect("seed RUNNING workflow_executions");

    diesel::sql_query("ANALYZE harvest_task_queue")
        .execute(conn)
        .await
        .expect("analyze task_queue");
    diesel::sql_query("ANALYZE harvest_workflow_executions")
        .execute(conn)
        .await
        .expect("analyze workflow_executions");
    diesel::sql_query("ANALYZE harvest_queue_pauses")
        .execute(conn)
        .await
        .expect("analyze queue_pauses");
    diesel::sql_query("ANALYZE harvest_activity_pauses")
        .execute(conn)
        .await
        .expect("analyze activity_pauses");
}

async fn explain(conn: &mut diesel_async::AsyncPgConnection, label: &str, sql: &str) -> String {
    diesel::sql_query("BEGIN")
        .execute(conn)
        .await
        .expect("begin");
    let loaded: Result<Vec<ExplainRow>, _> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) {sql}"
    ))
    .load(conn)
    .await;
    diesel::sql_query("ROLLBACK")
        .execute(conn)
        .await
        .expect("rollback");
    let plan_text = loaded
        .unwrap_or_else(|e| panic!("EXPLAIN failed for {label}: {e}"))
        .into_iter()
        .map(|r| r.query_plan)
        .collect::<Vec<_>>()
        .join("\n");
    format!("-- {label} --\n{plan_text}\n")
}

/// Generates the evidence behind `docs/performance-timeout-scanner.md`: a
/// negative result on whether `timeout.rs`'s scanner queries need the same
/// correlated-anti-join fix already applied to `queue::claim_task_query()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "evidence generator, not a CI assertion -- run via \
            autumn-harvest/scripts/timeout_scanner_perf_repro.sh"]
#[allow(clippy::too_many_lines)] // one-shot evidence capture, not a CI assertion
async fn zz_capture_timeout_scanner_evidence() {
    let Ok(bench) = db::setup_bench_db().await else {
        eprintln!("no database reachable; nothing captured");
        return;
    };

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("timeout-scanner-queries");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut conn = db::connect(&bench.url).await;
    seed_production_shaped_fixture(&mut conn).await;

    // EXPLAIN capture: the five scanner query builders driven verbatim from
    // `autumn_harvest::timeout`'s own `pub const fn`s, so this can never drift
    // out of sync with the compiled query text the way a hand-copied SQL
    // string could.
    let queries: [(&str, &str); 5] = [
        (
            "heartbeat_timeout_query",
            autumn_harvest::timeout::heartbeat_timeout_query(),
        ),
        (
            "start_to_close_timeout_query",
            autumn_harvest::timeout::start_to_close_timeout_query(),
        ),
        (
            "schedule_to_start_timeout_query",
            autumn_harvest::timeout::schedule_to_start_timeout_query(),
        ),
        (
            "schedule_to_close_timeout_query",
            autumn_harvest::timeout::schedule_to_close_timeout_query(),
        ),
        (
            "workflow_execution_timeout_query",
            autumn_harvest::timeout::workflow_execution_timeout_query(),
        ),
    ];

    let mut explain_bundle = String::new();
    for (name, sql) in queries {
        eprintln!("== EXPLAIN {name} ==");
        explain_bundle.push_str(&explain(&mut conn, name, sql).await);
        explain_bundle.push('\n');
    }
    std::fs::write(out_dir.join("scanner-queries.explain.txt"), &explain_bundle)
        .expect("write explain bundle");
    eprintln!("wrote scanner-queries.explain.txt");

    // Real end-to-end corroboration: reset pg_stat_statements, drive the
    // actual `enforce_timeouts_once` production entry point once against the
    // same fixture (not a literal-substituted string), then snapshot the
    // statements it issued against the two tables under test.
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await;
    let reset_ok = diesel::sql_query("SELECT pg_stat_statements_reset()")
        .execute(&mut conn)
        .await
        .is_ok();
    if !reset_ok {
        eprintln!(
            "pg_stat_statements not available on this server; skipping the \
             real-call snapshot (EXPLAIN evidence above still written)"
        );
        return;
    }

    let enforced = autumn_harvest::timeout::enforce_timeouts_once(
        &mut conn,
        &NoOpMetrics,
        Duration::from_secs(5),
        &None,
        &[ShardId::new(0)],
        None,
        None,
        60,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
        0,
    )
    .await
    .expect("enforce_timeouts_once should succeed against a real fixture");
    eprintln!("enforce_timeouts_once enforced {enforced} timed-out/breaching rows");

    let rows: Vec<StatRow> = diesel::sql_query(
        "SELECT query, calls, shared_blks_hit, shared_blks_read,
                (shared_blks_hit + shared_blks_read) AS total_buffers
         FROM pg_stat_statements
         WHERE (query ILIKE '%harvest_task_queue%'
                OR query ILIKE '%harvest_workflow_executions%')
           AND query NOT ILIKE '%pg_stat_statements%'
         ORDER BY total_buffers DESC
         LIMIT 20",
    )
    .load(&mut conn)
    .await
    .expect("pg_stat_statements snapshot");

    let mut stat_lines = vec![format!(
        "-- pg_stat_statements snapshot after one real enforce_timeouts_once() \
         call (enforced={enforced}) against the production-shaped fixture --"
    )];
    for row in &rows {
        stat_lines.push(format!(
            "calls={:<6} shared_blks_hit={:<10} shared_blks_read={:<8} total_buffers={:<10} query={}",
            row.calls, row.shared_blks_hit, row.shared_blks_read, row.total_buffers, row.query,
        ));
    }
    std::fs::write(
        out_dir.join("pg_stat_statements-after-one-tick.txt"),
        stat_lines.join("\n") + "\n",
    )
    .expect("write pg_stat_statements snapshot");
    eprintln!("wrote pg_stat_statements-after-one-tick.txt");

    std::fs::write(
        out_dir.join("fixture-summary.txt"),
        "harvest_task_queue: 300000 COMPLETED, 2000 RUNNING (1-in-30 heartbeat_timeout, \
         1-in-20 start_to_close expired), 5000 PENDING (1-in-10 schedule_to_start expired, \
         1-in-200 schedule_to_close_at expired+owned by a PAUSED execution)\n\
         harvest_queue_pauses: 1 row (queue_0)\n\
         harvest_activity_pauses: 1 row (act_5)\n\
         harvest_workflow_executions: 100025 RUNNING+PAUSED (25 PAUSED referenced by the \
         frozen-row carve-out above; of the 100000 RUNNING rows, 1-in-500 deadline_at, \
         1-in-700 chain_deadline_at, 1-in-300 sla_deadline_at expired)\n",
    )
    .expect("write fixture summary");

    eprintln!("== capture complete ==");
}
