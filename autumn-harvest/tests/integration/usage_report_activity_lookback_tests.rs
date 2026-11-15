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
//! this CTE). It did not index the LATERAL subquery above. The only index
//! that names `workflow_exec_id` at all is the initial migration's
//! `idx_harvest_events_exec (workflow_exec_id, event_id)`. That index cannot
//! serve an `event_type` + JSON-path equality lookup. So for every activity
//! terminal event in the report window, this subquery re-scans every OTHER
//! event belonging to that same execution. It also re-evaluates the JSON
//! extraction against each of those events, not just its `ActivityStarted`
//! siblings. A workflow with a heavy activity fan-out pays for this on every
//! one of its activities. That fan-out is a batch/DAG run -- exactly the
//! "real cardinality skew" this fixture seeds a 1% tail of.
//!
//! Two tests:
//! - [`usage_report_activity_lookback_index_does_not_change_the_result_set`]
//!   -- fast, always-run correctness check. It runs the query once, against a
//!   small hand-built fixture. That fixture exercises retries -- multiple
//!   `ActivityStarted` attempts per `activity_id` -- and an activity with no
//!   `ActivityStarted` at all. That second case is an external-activity
//!   `ActivityTimedOut`, which must NOT count as `activity_executions_failed`
//!   -- see `usage.rs`'s module doc. These checks hold regardless of the
//!   index, so this test does not toggle it.
//! - [`zz_capture_usage_report_activity_lookback_evidence`] -- `#[ignore]`d.
//!   Seeds a production-shaped fixture into a throwaway database. It captures
//!   `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` and a `pg_stat_statements`
//!   snapshot for the identical query text, before and after adding the
//!   candidate index. It then asserts the two produce byte-identical result
//!   sets.

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Double, Integer, Nullable, Text, Timestamptz};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::usage::{UsageGroupBy, UsageQuery, load_usage_grouped, usage_sql};

use crate::integration_e2e::setup_test_database_url_or_env;

const SHARD_ID: i32 = 0;

/// The candidate index this evidence file is deciding on. Partial: only
/// `ActivityStarted` rows, the only event type the LATERAL subquery ever
/// looks for. Keyed exactly to the subquery's own predicates: equality on
/// `workflow_exec_id` and the JSON-extracted `activity_id`, then `timestamp`.
/// That ordering makes `MAX(timestamp) WHERE timestamp <= $ae.timestamp`
/// answerable by a backward index scan, instead of a per-candidate-row sort.
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
/// - `wf_retry`: two `ActivityStarted` attempts share one `activity_id`, with
///   the terminal event after the SECOND attempt. `activity_compute_seconds`
///   must be measured from the second, later start, not the first.
///   `MAX(timestamp) WHERE timestamp <= terminal.timestamp` is exactly the
///   predicate that picks it out.
/// - `wf_external`: an `ActivityTimedOut` with NO matching `ActivityStarted`
///   at all (external-activity timeout) -- must NOT count toward
///   `activity_executions_failed` (module doc's documented exclusion).
///
/// These are business-logic assertions on the query's RESULT VALUES. They
/// must hold regardless of which plan Postgres picks. So this test runs the
/// query exactly once, against whatever index state the ambient database
/// already has, rather than toggling [`CANDIDATE_INDEX_SQL`] itself.
///
/// The test does not drop and rebuild the index to compare plans.
/// `HARVEST_TEST_DATABASE_URL` can point at a large, shared database, not a
/// throwaway one. A rebuild there would drop a real schema index from under
/// concurrent tests. Its non-concurrent `CREATE INDEX` would also hold a
/// table-wide lock while the shared corpus indexes. The evidence-capture test
/// below proves the before/after equivalence in its own throwaway database.
#[tokio::test]
async fn usage_report_activity_lookback_index_does_not_change_the_result_set() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let base = chrono::Utc::now() - chrono::Duration::hours(1);

    // Unique per invocation. `usage_sql()` groups by workflow_name over a
    // window that spans this whole test. A fixed name would also count the
    // leftover rows of an earlier run on a shared `HARVEST_TEST_DATABASE_URL`
    // database. Repeated runs within one hour then pushed
    // `retry_row.activity_executions` above the expected 2.
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

    // A high row_limit, not the caller-facing default. `usage_sql()` applies
    // `ORDER BY 1 LIMIT $5`. A shared `HARVEST_TEST_DATABASE_URL` database
    // can hold many OTHER workflow names from the last hour, sorted before
    // this test's names. A small limit could then cut this test's groups
    // from the result. The `.expect(...)` lookups below would fail for a
    // reason unrelated to the query's correctness.
    let rows = load_usage_grouped(&mut conn, SHARD_ID, &query, 1_000_000)
        .await
        .expect("query usage report");

    let retry_row = rows
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

    let external_row = rows
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
/// 80 days. Activity fan-out per execution is deliberately skewed. 85% get a
/// small 1-5 count, the overwhelming majority of real workflows. 14% get a
/// medium 5-25 count, and 1% get a heavy 50-300 count. That heavy tail is the
/// batch/DAG-run population this LATERAL subquery's cost scales with. It
/// re-pays its cost once per activity in that workflow.
///
/// 10% of activities get a second, retry, `ActivityStarted` attempt. That
/// exercises the same "which attempt owns this terminal event" resolution
/// the correctness test above checks directly, at production scale.
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
    // Neither this reset nor the stats query below may fail silently.
    // CREATE EXTENSION above stays best-effort. An absent extension fails
    // here first, because pg_stat_statements_reset() does not exist without
    // it. A later reset failure means something else is wrong, such as a
    // role without EXECUTE on pg_stat_statements_reset(). Both forms share
    // one normalized query text. A silent failure would let the "before"
    // counts leak into the "after" capture. The comparison would then be
    // contaminated and still report success.
    //
    // Scoped to THIS database's dbid (`pg_stat_statements_reset(0, dbid, 0)`),
    // not the bare zero-argument form. `setup_bench_db` provisions its own
    // throwaway database. But `pg_stat_statements` is a per-CLUSTER view. The
    // zero-argument reset wipes statistics for every database, user and query
    // on the whole server. On a `HARVEST_TEST_DATABASE_URL` Postgres shared
    // with other tenants' monitoring, each capture would destroy their data.
    diesel::sql_query(
        "SELECT pg_stat_statements_reset(0, \
         (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
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

    // `pg_stat_statements` is a per-CLUSTER view. On a
    // `HARVEST_TEST_DATABASE_URL` server shared with other databases, an
    // unscoped SELECT here ranks THIS database's freshly-reset entries
    // alongside every OTHER database's un-reset entries. Those foreign
    // entries can match the same LIKE pattern and have larger buffer counts.
    // A check of the reset fix saw 3 rows for this dbid and 5 unscoped. The
    // extra two came from other databases on the same server. The scope to
    // `current_database()`'s dbid keeps the "top 5" ranking to this run's own
    // statements.
    let stats: Vec<StatRow> = diesel::sql_query(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
         (shared_blks_hit + shared_blks_read) AS total_buffers, temp_blks_written \
         FROM pg_stat_statements \
         WHERE query LIKE '%harvest_workflow_executions%harvest_events%' \
           AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         ORDER BY total_buffers DESC LIMIT 5",
    )
    .load(conn)
    .await
    .expect(
        "pg_stat_statements query failed -- degrading to an empty snapshot \
         here would let this evidence-capture test report \"equivalence \
         confirmed\" while publishing no buffer evidence at all",
    );
    // The reproduction script checks only the equivalence marker below, not
    // this file's content. An empty match set here is not a query error, so
    // the expect() above cannot catch it. Assert it explicitly, or a silent
    // zero-row snapshot would report success with no buffer evidence.
    assert!(
        !stats.is_empty(),
        "pg_stat_statements returned no rows matching this query for \
         label {label}"
    );
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

/// Regenerates `docs/perf-artifacts/usage-report-activity-lookback/`. It
/// captures before/after `EXPLAIN`, a `pg_stat_statements` snapshot for each,
/// and a result-set equivalence check. The query text is the same both
/// times; only the schema changes, since the candidate index is created
/// between captures. `#[ignore]`d -- seeds a quarter million+ event rows and
/// takes well over a minute.
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
    // `setup_bench_db` provisions a fresh throwaway database.
    // `pg_stat_statements` is preloaded cluster-wide, but it still needs its
    // view created IN this database. Only then will it report anything for
    // queries run against it.
    diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await
        .ok();
    seed_production_shaped_fixture(&mut conn).await;

    // `setup_bench_db` runs every migration, including
    // `20260905181020_harvest_usage_activity_lookback_index` once it ships.
    // So on a checkout at or after that migration, the candidate index
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
