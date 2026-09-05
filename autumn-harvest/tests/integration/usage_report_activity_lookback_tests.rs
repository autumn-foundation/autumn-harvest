#![cfg(feature = "db")]
//! Buffer-cost evidence for the `activity_metrics` CTE inside
//! `usage::usage_sql()` (issue #596, `GET /admin/usage`) -- specifically the
//! `LEFT JOIN LATERAL` that resolves each activity terminal event's owning
//! `ActivityStarted` attempt:
//!
//! ```sql
//! LEFT JOIN LATERAL (
//!     SELECT MAX(e2.timestamp) AS last_started_at
//!     FROM harvest_events e2
//!     WHERE e2.workflow_exec_id = ae.workflow_exec_id
//!       AND e2.event_type = 'ActivityStarted'
//!       AND e2.event_data #>> '{data,activity_id}' = ae.activity_id
//!       AND e2.timestamp <= ae.timestamp
//! ) s ON true
//! ```
//!
//! `docs/performance-usage-report-indexes` (migration
//! `20260702000000_harvest_usage_report_indexes`) already indexed every other
//! CTE in this query (`execution_starts`, `terminal_counts`,
//! `reset_terminated_execs`, and the outer `activity_events` scan that feeds
//! this CTE). It did not index the LATERAL subquery above: the only index
//! that names `workflow_exec_id` at all is the initial migration's
//! `idx_harvest_events_exec (workflow_exec_id, event_id)`, which cannot serve
//! an `event_type` + JSON-path equality lookup -- so for every activity
//! terminal event in the report window, this subquery re-scans (and
//! re-evaluates the JSON extraction against) every OTHER event belonging to
//! that same execution, not just its `ActivityStarted` siblings. A workflow
//! with a heavy activity fan-out (a batch/DAG run -- exactly the "real
//! cardinality skew" this fixture seeds a 1% tail of) pays for this on every
//! one of its activities.
//!
//! Two tests:
//! - [`usage_report_activity_lookback_index_does_not_change_the_result_set`]
//!   -- fast, always-run correctness check: the query returns the same
//!   grouped counters with and without the new index, on a small hand-built
//!   fixture that exercises retries (multiple `ActivityStarted` attempts per
//!   `activity_id`) and an activity with no `ActivityStarted` at all
//!   (external-activity `ActivityTimedOut`, which must NOT count as
//!   `activity_executions_failed` -- see `usage.rs`'s module doc).
//! - [`zz_capture_usage_report_activity_lookback_evidence`] -- `#[ignore]`d.
//!   Seeds a production-shaped fixture into a throwaway database, captures
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` and a `pg_stat_statements`
//!   snapshot for the identical query text before and after adding the
//!   candidate index, and asserts the two produce byte-identical result sets.

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Double, Integer, Nullable, Text, Timestamptz};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::usage::{UsageGroupBy, UsageQuery, load_usage_grouped, usage_sql};

use crate::integration_e2e::setup_test_database_url_or_env;

const SHARD_ID: i32 = 0;

/// The candidate index this evidence file is deciding on. Partial (only
/// `ActivityStarted` rows -- the only event type the LATERAL subquery ever
/// looks for) and keyed exactly to the subquery's own predicates: equality on
/// `workflow_exec_id` and the JSON-extracted `activity_id`, then `timestamp`
/// so `MAX(timestamp) WHERE timestamp <= $ae.timestamp` is answerable by a
/// backward index scan instead of a per-candidate-row sort.
const CANDIDATE_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS idx_harvest_events_activity_started_lookup \
     ON harvest_events (workflow_exec_id, (event_data #>> '{data,activity_id}'), timestamp) \
     WHERE event_type = 'ActivityStarted'";

#[derive(QueryableByName, Debug, Clone, PartialEq)]
struct UsageRow {
    #[diesel(sql_type = Text)]
    grp: String,
    #[diesel(sql_type = BigInt)]
    workflow_starts: i64,
    #[diesel(sql_type = BigInt)]
    completed: i64,
    #[diesel(sql_type = BigInt)]
    failed: i64,
    #[diesel(sql_type = BigInt)]
    cancelled: i64,
    #[diesel(sql_type = BigInt)]
    timed_out: i64,
    #[diesel(sql_type = BigInt)]
    activity_executions: i64,
    #[diesel(sql_type = BigInt)]
    activity_executions_failed: i64,
    #[diesel(sql_type = Double)]
    activity_compute_seconds: f64,
}

async fn run_usage_query(conn: &mut AsyncPgConnection, row_limit: i64) -> Vec<UsageRow> {
    let mut rows: Vec<UsageRow> = diesel::sql_query(usage_sql())
        .bind::<Integer, _>(SHARD_ID)
        .bind::<Nullable<Text>, _>(None::<String>)
        .bind::<Timestamptz, _>(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH)
        .bind::<Timestamptz, _>(chrono::Utc::now() + chrono::Duration::days(1))
        .bind::<BigInt, _>(row_limit)
        .load(conn)
        .await
        .expect("usage_sql() query failed");
    rows.sort_by(|a, b| a.grp.cmp(&b.grp));
    rows
}

// ---------------------------------------------------------------------------
// Fast, always-run correctness check
// ---------------------------------------------------------------------------

async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    started_at: chrono::DateTime<chrono::Utc>,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at) \
         VALUES ('{id}', '{workflow_name}', 'wf-{id}', gen_random_uuid(), {SHARD_ID}, \
                 'COMPLETED', '{{}}'::jsonb, 'default', '{ts}', '{ts}')",
        ts = started_at.to_rfc3339(),
    ))
    .await
    .expect("seed workflow execution");
    id
}

async fn seed_event(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
    event_id: i32,
    event_type: &str,
    activity_id: Option<uuid::Uuid>,
    ts: chrono::DateTime<chrono::Utc>,
) {
    let data = activity_id.map_or_else(
        || "{}".to_string(),
        |aid| format!(r#"{{"activity_id":"{aid}"}}"#),
    );
    conn.batch_execute(&format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         VALUES ('{exec_id}', {event_id}, '{event_type}', \
                 '{{\"type\":\"{event_type}\",\"data\":{data}}}'::jsonb, '{ts}')",
        ts = ts.to_rfc3339(),
    ))
    .await
    .expect("seed event");
}

/// Exercises the real `load_usage_grouped` against a small, hand-built
/// fixture covering the cases the LATERAL subquery must get right:
///
/// - `wf_retry`: two `ActivityStarted` attempts sharing one `activity_id`,
///   terminal event after the SECOND attempt -- `activity_compute_seconds`
///   must be measured from the second (later) start, not the first, and
///   `MAX(timestamp) WHERE timestamp <= terminal.timestamp` is exactly the
///   predicate that picks it out.
/// - `wf_external`: an `ActivityTimedOut` with NO matching `ActivityStarted`
///   at all (external-activity timeout) -- must NOT count toward
///   `activity_executions_failed` (module doc's documented exclusion).
///
/// Run twice against the same seeded rows -- once before, once after adding
/// [`CANDIDATE_INDEX_SQL`] -- and asserts identical results, so a correctness
/// regression in the index (or in Postgres choosing a different plan) is
/// caught independently of the buffer-cost evidence capture below.
#[tokio::test]
async fn usage_report_activity_lookback_index_does_not_change_the_result_set() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let base = chrono::Utc::now() - chrono::Duration::hours(1);

    // Unique per invocation: `usage_sql()` groups by workflow_name over a
    // window wide enough to span this whole test, so a fixed name would
    // double-count against leftover rows from an earlier run of this same
    // test against a shared, uncleaned `HARVEST_TEST_DATABASE_URL` database
    // (caught by a real, reproducible failure in this environment: repeated
    // runs within the same hour accumulated retry_row.activity_executions
    // beyond the expected 2).
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let retry_wf_name = format!("usage_lookback_retry_wf_{run_id}");
    let external_wf_name = format!("usage_lookback_external_wf_{run_id}");

    // wf_retry: ActivityStarted(t0), ActivityStarted(t0+10s, retry), ActivityCompleted(t0+20s).
    let retry_wf = seed_workflow(&mut conn, &retry_wf_name, base).await;
    let retry_activity = uuid::Uuid::new_v4();
    seed_event(
        &mut conn,
        retry_wf,
        0,
        "ActivityStarted",
        Some(retry_activity),
        base,
    )
    .await;
    seed_event(
        &mut conn,
        retry_wf,
        1,
        "ActivityStarted",
        Some(retry_activity),
        base + chrono::Duration::seconds(10),
    )
    .await;
    seed_event(
        &mut conn,
        retry_wf,
        2,
        "ActivityCompleted",
        Some(retry_activity),
        base + chrono::Duration::seconds(20),
    )
    .await;

    // wf_external: ActivityTimedOut with no matching ActivityStarted.
    let external_wf = seed_workflow(&mut conn, &external_wf_name, base).await;
    seed_event(
        &mut conn,
        external_wf,
        0,
        "ActivityTimedOut",
        Some(uuid::Uuid::new_v4()),
        base + chrono::Duration::seconds(5),
    )
    .await;

    conn.batch_execute("ANALYZE harvest_workflow_executions; ANALYZE harvest_events;")
        .await
        .expect("analyze");

    let query = UsageQuery {
        group_by: UsageGroupBy::WorkflowName,
        from: base - chrono::Duration::minutes(5),
        to: chrono::Utc::now(),
    };

    let before = load_usage_grouped(&mut conn, SHARD_ID, &query, 100)
        .await
        .expect("query before index");

    conn.batch_execute(CANDIDATE_INDEX_SQL)
        .await
        .expect("create candidate index");

    let after = load_usage_grouped(&mut conn, SHARD_ID, &query, 100)
        .await
        .expect("query after index");

    assert_eq!(
        before, after,
        "adding the lookback index must not change any reported counter"
    );

    let retry_row = before
        .iter()
        .find(|r| r.group == retry_wf_name)
        .expect("retry workflow group present");
    assert_eq!(retry_row.activity_executions, 2, "both attempts counted");
    assert!(
        (retry_row.activity_compute_seconds - 10.0).abs() < 0.01,
        "compute time must be measured from the SECOND (retry) start at t0+10s to \
         completion at t0+20s, i.e. 10s -- not from the first start at t0 (20s): got {}",
        retry_row.activity_compute_seconds
    );

    let external_row = before
        .iter()
        .find(|r| r.group == external_wf_name)
        .expect("external workflow group present");
    assert_eq!(
        external_row.activity_executions_failed, 0,
        "an ActivityTimedOut with no matching ActivityStarted (external activity) \
         must not count as a failed execution"
    );
}

// ---------------------------------------------------------------------------
// Evidence capture: EXPLAIN + pg_stat_statements baseline
// ---------------------------------------------------------------------------

/// 40,000 workflow executions across 30 workflow names, spread over the last
/// 80 days. Activity fan-out per execution is deliberately skewed: 85% get a
/// small 1-5 count (the overwhelming majority of real workflows), 14% get a
/// medium 5-25 count, and 1% get a heavy 50-300 count -- the batch/DAG-run
/// tail this LATERAL subquery's cost scales with, since it re-pays its cost
/// once per activity in that workflow. 10% of activities get a second
/// (retry) `ActivityStarted` attempt, exercising the same "which attempt
/// owns this terminal event" resolution the correctness test above checks
/// directly, at production scale.
const FIXTURE_EXECUTIONS: i64 = 40_000;

async fn seed_production_shaped_fixture(conn: &mut AsyncPgConnection) {
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE tmp_usage_exec (id UUID, workflow_name TEXT, started_at TIMESTAMPTZ, n_activities INT);

         -- One random draw per execution (`r`), reused for both thresholds
         -- below -- `WHEN random() < 0.85 ... WHEN random() < 0.99` would
         -- each call `random()` independently, so an execution that misses
         -- the first 85% test would only reach the heavy bucket when a
         -- SECOND draw lands above 0.99: 15% * 1% = 0.15% heavy, not the
         -- intended 1% (Codex review, PR #1381). `r` must come from the SAME
         -- flat SELECT list as the set-returning FROM item, not a separate
         -- `CROSS JOIN LATERAL (SELECT random() ...)`: an uncorrelated
         -- LATERAL subquery like that is not tied to the outer row and
         -- Postgres hoists it, evaluating `random()` exactly ONCE for the
         -- whole statement instead of once per row (verified: every row
         -- landed in the same bucket) -- a second review-round regression in
         -- the first attempt at this same fix.
         INSERT INTO tmp_usage_exec (id, workflow_name, started_at, n_activities)
         SELECT
             gen_random_uuid(),
             'usage_bench_wf_' || (1 + floor(random()*30))::text,
             NOW() - (random() * interval '80 days'),
             CASE
                 WHEN r < 0.85 THEN 1 + floor(random()*5)::int
                 WHEN r < 0.99 THEN 5 + floor(random()*20)::int
                 ELSE 50 + floor(random()*251)::int
             END
         FROM (SELECT random() AS r FROM generate_series(1, {FIXTURE_EXECUTIONS})) AS fanout;

         INSERT INTO harvest_workflow_executions
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name,
              started_at, created_at, completed_at)
         SELECT
             id, workflow_name, 'usage_bench_' || id::text, gen_random_uuid(), {SHARD_ID},
             'COMPLETED', '{{}}'::jsonb, 'default', started_at, started_at,
             started_at + interval '5 minutes'
         FROM tmp_usage_exec;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT id, 999999, 'WorkflowCompleted',
                jsonb_build_object('type','WorkflowCompleted','data', jsonb_build_object()),
                started_at + interval '5 minutes'
         FROM tmp_usage_exec;

         -- Same one-draw-per-row requirement as the fan-out CASE above, but
         -- `r` here must come from the SAME SELECT list as the CORRELATED
         -- `generate_series(1, e.n_activities)` lateral join (referencing
         -- `e.n_activities` is what keeps that one un-hoistable and genuinely
         -- per-outer-row); a separate, uncorrelated
         -- `CROSS JOIN LATERAL (SELECT random() AS r)` alongside it would
         -- still collapse to one draw for the entire statement.
         CREATE TEMP TABLE tmp_usage_activity AS
         SELECT
             exec_id,
             started_at,
             activity_seq,
             gen_random_uuid() AS activity_id,
             CASE
                 WHEN r < 0.90 THEN 'ActivityCompleted'
                 WHEN r < 0.98 THEN 'ActivityFailed'
                 ELSE 'ActivityTimedOut'
             END AS terminal_type,
             (random() < 0.10) AS has_retry
         FROM (
             SELECT e.id AS exec_id, e.started_at, gs.n AS activity_seq, random() AS r
             FROM tmp_usage_exec e
             CROSS JOIN LATERAL generate_series(1, e.n_activities) AS gs(n)
         ) AS activity_rows;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT exec_id, activity_seq * 4, 'ActivityStarted',
                jsonb_build_object('type','ActivityStarted','data',
                                    jsonb_build_object('activity_id', activity_id::text)),
                started_at + (activity_seq * 4) * interval '1 second'
         FROM tmp_usage_activity;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT exec_id, activity_seq * 4 + 1, 'ActivityStarted',
                jsonb_build_object('type','ActivityStarted','data',
                                    jsonb_build_object('activity_id', activity_id::text)),
                started_at + (activity_seq * 4 + 1) * interval '1 second'
         FROM tmp_usage_activity
         WHERE has_retry;

         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         SELECT exec_id, activity_seq * 4 + 2, terminal_type,
                jsonb_build_object('type', terminal_type, 'data',
                                    jsonb_build_object('activity_id', activity_id::text)),
                started_at + (activity_seq * 4 + 2) * interval '1 second'
         FROM tmp_usage_activity;

         ANALYZE harvest_workflow_executions;
         ANALYZE harvest_events;"
    ))
    .await
    .expect("seed production-shaped usage-report fixture");
}

#[derive(QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    query_plan: String,
}

#[derive(QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = Text)]
    query: String,
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = BigInt)]
    total_buffers: i64,
    #[diesel(sql_type = BigInt)]
    temp_blks_written: i64,
}

async fn explain_and_result_set(
    conn: &mut AsyncPgConnection,
    row_limit: i64,
) -> (String, Vec<UsageRow>) {
    let explained: Vec<ExplainRow> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) {}",
        usage_sql()
    ))
    .bind::<Integer, _>(SHARD_ID)
    .bind::<Nullable<Text>, _>(None::<String>)
    .bind::<Timestamptz, _>(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH)
    .bind::<Timestamptz, _>(chrono::Utc::now() + chrono::Duration::days(1))
    .bind::<BigInt, _>(row_limit)
    .load(conn)
    .await
    .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}"));
    let plan_text = explained
        .into_iter()
        .map(|r| r.query_plan)
        .collect::<Vec<_>>()
        .join("\n");

    let rows = run_usage_query(conn, row_limit).await;
    (plan_text, rows)
}

async fn capture(
    conn: &mut AsyncPgConnection,
    out_dir: &std::path::Path,
    label: &str,
) -> Vec<UsageRow> {
    // Unlike CREATE EXTENSION above (best-effort: a genuinely absent
    // extension just means no evidence, handled by the stats query's own
    // unwrap_or_else below), a reset failure here must not be swallowed: the
    // extension already exists by this point, so a failure means something
    // else is wrong (e.g. the connecting role lacks EXECUTE on
    // pg_stat_statements_reset()) and both forms' normalized query text is
    // identical, so silently proceeding would let the "before" counts leak
    // into the "after" capture and Postgres would aggregate the two,
    // publishing a contaminated comparison that still reports success
    // (Codex review, PR #1381).
    diesel::sql_query("SELECT pg_stat_statements_reset()")
        .execute(conn)
        .await
        .expect(
            "pg_stat_statements_reset() failed -- proceeding would let stale counts from \
             the other capture leak into this one and silently contaminate the comparison",
        );

    let (plan_text, rows) = explain_and_result_set(conn, 100).await;

    std::fs::write(
        out_dir.join(format!("{label}.explain.txt")),
        format!("-- {label}: usage_sql() @ {FIXTURE_EXECUTIONS} executions --\n{plan_text}\n"),
    )
    .expect("write explain artifact");

    let stats: Vec<StatRow> = diesel::sql_query(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
         (shared_blks_hit + shared_blks_read) AS total_buffers, temp_blks_written \
         FROM pg_stat_statements \
         WHERE query LIKE '%harvest_workflow_executions%harvest_events%' \
         ORDER BY total_buffers DESC LIMIT 5",
    )
    .load(conn)
    .await
    .unwrap_or_else(|e| {
        eprintln!("pg_stat_statements query failed: {e}");
        Vec::new()
    });
    std::fs::write(
        out_dir.join(format!("{label}.pg_stat_statements.txt")),
        format!("{stats:#?}\n"),
    )
    .expect("write pg_stat_statements artifact");

    std::fs::write(
        out_dir.join(format!("{label}.result-rows.txt")),
        format!(
            "rows={}\n{}\n",
            rows.len(),
            rows.iter()
                .map(|r| format!("{r:?}"))
                .collect::<Vec<_>>()
                .join("\n")
        ),
    )
    .expect("write result-set artifact");

    eprintln!("captured '{label}': {} groups", rows.len());
    rows
}

/// Regenerates `docs/perf-artifacts/usage-report-activity-lookback/`:
/// before/after `EXPLAIN`, a `pg_stat_statements` snapshot for each, and a
/// result-set equivalence check -- same query text both times, only the
/// schema changes (the candidate index is created between captures).
/// `#[ignore]`d -- seeds a quarter million+ event rows and takes well over a
/// minute.
///
/// Needs `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a
/// reachable Docker daemon for `claim_bench_support::db::setup_bench_db`'s
/// testcontainer fallback.
#[tokio::test]
#[ignore = "seeds a production-shaped usage-report fixture; run explicitly"]
async fn zz_capture_usage_report_activity_lookback_evidence() {
    use super::claim_bench_support::db;

    let bench = match db::setup_bench_db().await {
        Ok(b) => b,
        Err(reason) => {
            eprintln!("no database reachable; nothing captured: {}", reason.0);
            return;
        }
    };

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("usage-report-activity-lookback");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut conn = db::connect(&bench.url).await;
    // `setup_bench_db` provisions a fresh throwaway database; `pg_stat_statements`
    // (preloaded cluster-wide) still needs its view created IN this database
    // before it will report anything for queries run against it.
    diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await
        .ok();
    seed_production_shaped_fixture(&mut conn).await;

    // `setup_bench_db` runs every migration, including
    // `20260905181020_harvest_usage_activity_lookback_index` once it ships --
    // so on a checkout at or after that migration, the candidate index
    // already exists by the time we get here. Drop it unconditionally before
    // the "before" capture so this test keeps reproducing the pre-fix
    // baseline regardless of which side of the migration HEAD sits on.
    conn.batch_execute("DROP INDEX IF EXISTS idx_harvest_events_activity_started_lookup")
        .await
        .expect("drop candidate index for a clean before-capture");

    let before_rows = capture(&mut conn, &out_dir, "before").await;

    conn.batch_execute(CANDIDATE_INDEX_SQL)
        .await
        .expect("create candidate index");
    conn.batch_execute("ANALYZE harvest_events;")
        .await
        .expect("analyze after index build");

    let after_rows = capture(&mut conn, &out_dir, "after").await;

    assert_eq!(
        before_rows, after_rows,
        "before/after must return byte-identical grouped counters -- only the \
         schema changed, not the query"
    );
    eprintln!(
        "equivalence confirmed: before and after agree on all {} groups",
        before_rows.len()
    );

    eprintln!("== done. Artifacts in {} ==", out_dir.display());
}
