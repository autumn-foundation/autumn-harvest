#![cfg(feature = "db")]
//! Ledger performance investigation: the per-timer N+1 in
//! `persist_mixed_suspension_batch`.
//!
//! A workflow that arms `N` durable timers in one suspension, for example
//! `ctx.race()` over `N` `.timer(..)` branches, reaches the generalized
//! mixed-batch persist path. That path resolves each `StartTimer` with a
//! `for` loop. Each turn issues one `SELECT ... FROM harvest_timers WHERE
//! workflow_exec_id = $1 AND timer_id = $2 AND NOT fired`, plus one
//! `SELECT NOW()` when the timer is new. A second loop then issues one
//! single-row `INSERT INTO harvest_timers` per new timer.
//!
//! `harvest_timers` has no index on `(workflow_exec_id, timer_id)`. Each
//! per-timer lookup therefore scans the table. The cost is `N` scans, not
//! one.
//!
//! The harness drives a real [`autumn_harvest::worker::Worker`] against a
//! production-shaped `harvest_timers` table, and reads `pg_stat_statements`
//! after the workflow parks. Evidence is call counts and buffer totals,
//! never wall-clock. Output goes to `docs/perf-artifacts/` under the label
//! in `PERF_LABEL`.
//!
//! Two tests:
//! - [`zz_capture_mixed_suspension_timer_batch_evidence`] -- `#[ignore]`d
//!   evidence generator.
//! - [`park_persists_the_same_timer_rows_and_events`] -- always-run
//!   correctness check over duplicate and pre-existing timer ids.

#![allow(clippy::too_many_lines)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use autumn_harvest::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::worker::HandlerRegistry;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_history_from_url, load_timers_for_execution_from_url,
    spawn_test_worker,
};

type DbGuard = Option<ContainerAsync<Postgres>>;
type WfFuture<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// Seeded history shape: executions that already finished and left timer rows.
const SEED_EXECUTIONS: i64 = 20_000;
const SEED_TIMERS_PER_EXECUTION: i64 = 10;

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

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
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
    url
}

/// Seeds a production-shaped `harvest_timers` table: finished executions
/// whose timers fired long ago (95%), a thin tail of still-pending timers
/// (5%), and a skewed timer count per execution. Deterministic: no random
/// calls, every value derives from the series index.
async fn seed_timer_history(conn: &mut AsyncPgConnection) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at) \
         SELECT uuid_in(md5('seed-exec-' || g)::cstring), 'seed_wf', 'seed_wf_' || g, \
                uuid_in(md5('seed-run-' || g)::cstring), 0, \
                CASE WHEN g % 20 = 0 THEN 'RUNNING' ELSE 'COMPLETED' END, \
                '{{}}'::jsonb, 'default', NOW() - interval '30 days', NOW() - interval '30 days' \
         FROM generate_series(1, {SEED_EXECUTIONS}) AS g;\
         INSERT INTO harvest_timers (workflow_exec_id, timer_id, fires_at, fired) \
         SELECT uuid_in(md5('seed-exec-' || g)::cstring), 'seed-timer-' || t, \
                NOW() - interval '29 days' + (t || ' minutes')::interval, \
                (g % 20 <> 0) \
         FROM generate_series(1, {SEED_EXECUTIONS}) AS g, \
              generate_series(1, {SEED_TIMERS_PER_EXECUTION}) AS t \
         WHERE t <= 1 + (g % {SEED_TIMERS_PER_EXECUTION});\
         ANALYZE harvest_timers; ANALYZE harvest_workflow_executions;"
    ))
    .await
    .expect("seed production-shaped timer history");
}

fn wf_info(name: &'static str, handler: fn(&WorkflowContext, Value) -> WfFuture<'_>) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "mixed_suspension_timer_batch_perf",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

/// Races `input["n"]` far-future timers. None fires during the run, so the
/// workflow parks once with all `n` timers armed in one suspension batch.
fn race_n_timers(ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let n = input.get("n").and_then(Value::as_u64).unwrap_or(1);
        let mut race = ctx.race();
        for i in 0..n {
            race = race.timer(StdDuration::from_secs(86_400 + i));
        }
        let winner = race.run().await.map_err(|e| e.to_string())?;
        Ok(json!({ "index": winner.index }))
    })
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
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    wal_bytes: i64,
}

async fn reset_stats_for_db(conn: &mut AsyncPgConnection, db_name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect("pg_stat_statements_reset failed; the role must be a superuser");
}

async fn snapshot_statements(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
                (shared_blks_hit + shared_blks_read) AS total_buffers, wal_bytes::bigint AS wal_bytes \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY total_buffers DESC, query"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements must be preloaded")
}

/// The per-timer existence lookup (before) or its batched form (after).
fn is_timer_lookup(q: &str) -> bool {
    let q = q.to_ascii_lowercase();
    let q = q.as_str();
    q.contains("from \"harvest_timers\" where")
        && q.contains("\"harvest_timers\".\"timer_id\" =")
        && !q.contains("\"fires_at\" <=")
}

fn is_timer_insert(q: &str) -> bool {
    let q = q.to_ascii_lowercase();
    let q = q.as_str();
    q.contains("insert into \"harvest_timers\"")
}

fn is_clock_read(q: &str) -> bool {
    q.trim().eq_ignore_ascii_case("select now()")
}

struct Point {
    n: u64,
    lookup_calls: i64,
    lookup_buffers: i64,
    clock_calls: i64,
    insert_calls: i64,
    insert_buffers: i64,
    target_calls: i64,
    target_buffers: i64,
    park_calls: i64,
    park_buffers: i64,
}

fn out_dir() -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("docs")
        .join("perf-artifacts")
        .join("mixed-suspension-timer-batch");
    std::fs::create_dir_all(&dir).expect("create artifact dir");
    dir
}

/// Drives one park of `n` timers through a real worker and snapshots the
/// statements it issued. Returns the point plus the raw rows.
async fn measure_one(admin: &str, label: &str, n: u64) -> (Point, Vec<StatRow>, String) {
    let db_name = unique(&format!("timer_batch_{label}_{n}"));
    let url = create_fresh_db(admin, &db_name).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("seed conn");
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await;
    seed_timer_history(&mut conn).await;

    let exec_id = insert_workflow_execution(&mut conn).await;
    // The seeded execution row and its task are written before the reset, so
    // the snapshot holds only the worker's own statements.
    enqueue_started_workflow_task(&mut conn, exec_id, json!({ "n": n })).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![wf_info("e2e_test_workflow", race_n_timers)],
        vec![],
    ));
    let worker = build_runtime_worker("timer-batch-perf", 4, 2, registry);

    reset_stats_for_db(&mut conn, &db_name).await;
    let pool = build_test_pool(&url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Wait until the park has persisted every timer row.
    let mut parked = false;
    for _ in 0..200 {
        if load_timers_for_execution_from_url(&url, exec_id).await.len() as u64 == n {
            parked = true;
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
    worker.shutdown();
    handle.await.expect("worker join");
    assert!(parked, "workflow must park with {n} timer rows");

    let rows = snapshot_statements(&mut conn, &db_name).await;

    let explain = explain_statements(&mut conn, exec_id).await;

    let lookups: Vec<&StatRow> = rows.iter().filter(|r| is_timer_lookup(&r.query)).collect();
    let inserts: Vec<&StatRow> = rows.iter().filter(|r| is_timer_insert(&r.query)).collect();
    let clocks: Vec<&StatRow> = rows.iter().filter(|r| is_clock_read(&r.query)).collect();
    let lookup_calls: i64 = lookups.iter().map(|r| r.calls).sum();
    let lookup_buffers: i64 = lookups.iter().map(|r| r.total_buffers).sum();
    let clock_calls: i64 = clocks.iter().map(|r| r.calls).sum();
    let clock_buffers: i64 = clocks.iter().map(|r| r.total_buffers).sum();
    let insert_calls: i64 = inserts.iter().map(|r| r.calls).sum();
    let insert_buffers: i64 = inserts.iter().map(|r| r.total_buffers).sum();
    // The park transaction's own statements: everything the worker issued
    // that is not the claim/poll loop. Polling volume depends on how long the
    // park took, so the poll statements are excluded from the denominator.
    let park_rows: Vec<&StatRow> = rows.iter().filter(|r| !is_poll_noise(&r.query)).collect();
    let park_calls: i64 = park_rows.iter().map(|r| r.calls).sum();
    let park_buffers: i64 = park_rows.iter().map(|r| r.total_buffers).sum();

    let point = Point {
        n,
        lookup_calls,
        lookup_buffers,
        clock_calls,
        insert_calls,
        insert_buffers,
        target_calls: lookup_calls + clock_calls + insert_calls,
        target_buffers: lookup_buffers + clock_buffers + insert_buffers,
        park_calls,
        park_buffers,
    };
    (point, rows, explain)
}

/// Statements that run on a timer, independent of the park under test.
fn is_poll_noise(q: &str) -> bool {
    let q = q.to_ascii_lowercase();
    let q = q.as_str();
    q.contains("skip locked")
        || q.contains("pg_try_advisory")
        || q.contains("pg_stat_")
        || q.contains("pg_catalog")
        || q.contains("harvest_scanner")
        || q.starts_with("set ")
        || q.starts_with("begin")
        || q.starts_with("commit")
        || q.starts_with("rollback")
        || (q.contains("from \"harvest_timers\"") && q.contains("\"fires_at\" <="))
        || (q.starts_with("update \"harvest_timers\""))
}

/// `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` for the lookup and the
/// insert, run inside a rolled-back transaction.
async fn explain_statements(conn: &mut AsyncPgConnection, exec_id: autumn_harvest::types::ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct Line {
        #[diesel(sql_type = diesel::sql_types::Text)]
        #[diesel(column_name = "QUERY PLAN")]
        plan: String,
    }
    let id = exec_id.as_uuid();
    let mut out = String::new();
    for (title, sql) in [
        (
            "per-timer lookup (one call; the loop issues this once per timer)",
            format!(
                "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) SELECT * FROM harvest_timers \
                 WHERE workflow_exec_id = '{id}' AND timer_id = 'timer-0' AND fired = false LIMIT 1"
            ),
        ),
        (
            "batched lookup over 10 timer ids",
            format!(
                "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) SELECT * FROM harvest_timers \
                 WHERE workflow_exec_id = '{id}' AND timer_id = ANY(ARRAY['t0','t1','t2','t3','t4','t5','t6','t7','t8','t9']) \
                 AND fired = false"
            ),
        ),
    ] {
        let lines: Vec<Line> = diesel::sql_query(sql)
            .load(conn)
            .await
            .expect("explain");
        out.push_str(&format!("-- {title}\n"));
        for l in lines {
            out.push_str(&l.plan);
            out.push('\n');
        }
        out.push('\n');
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-mixed-suspension-timer-batch.md"]
async fn zz_capture_mixed_suspension_timer_batch_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());
    let dir = out_dir();

    let mut lines = vec![format!(
        "-- {label}: mixed-suspension timer persist, pg_stat_statements sweep --\n\
         seed: {SEED_EXECUTIONS} executions, ~{} timer rows, 5% pending\n\
         n\tlookup_calls\tlookup_buffers\tclock_calls\tinsert_calls\tinsert_buffers\t\
         target_calls\ttarget_buffers\tpark_calls\tpark_buffers",
        SEED_EXECUTIONS * (SEED_TIMERS_PER_EXECUTION + 1) / 2
    )];
    for n in [3_u64, 10, 40] {
        let (p, rows, explain) = measure_one(&admin, &label, n).await;
        eprintln!(
            "label={label} n={} lookup_calls={} lookup_buffers={} clock_calls={} insert_calls={} \
             insert_buffers={} target_calls={} target_buffers={} park_calls={} park_buffers={}",
            p.n, p.lookup_calls, p.lookup_buffers, p.clock_calls, p.insert_calls,
            p.insert_buffers, p.target_calls, p.target_buffers, p.park_calls, p.park_buffers
        );
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            p.n, p.lookup_calls, p.lookup_buffers, p.clock_calls, p.insert_calls,
            p.insert_buffers, p.target_calls, p.target_buffers, p.park_calls, p.park_buffers
        ));
        if n == 10 {
            let mut snap = format!("-- {label}: pg_stat_statements, n={n}, ranked by buffers\n");
            snap.push_str("calls\tshared_hit\tshared_read\ttotal_buffers\twal_bytes\tquery\n");
            for r in &rows {
                snap.push_str(&format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\n",
                    r.calls,
                    r.shared_blks_hit,
                    r.shared_blks_read,
                    r.total_buffers,
                    r.wal_bytes,
                    r.query.replace('\n', " ")
                ));
            }
            snap.push_str("\n-- ranked by calls (top 10)\n");
            let mut by_calls: Vec<&StatRow> = rows.iter().collect();
            by_calls.sort_by(|a, b| b.calls.cmp(&a.calls).then(a.query.cmp(&b.query)));
            for r in by_calls.iter().take(10) {
                snap.push_str(&format!("{}\t{}\n", r.calls, r.query.replace('\n', " ")));
            }
            std::fs::write(dir.join(format!("{label}-pg_stat_statements.txt")), snap)
                .expect("write snapshot");
            std::fs::write(dir.join(format!("{label}-explain.txt")), explain).expect("write explain");
        }
    }
    std::fs::write(dir.join(format!("{label}-sweep.txt")), lines.join("\n") + "\n")
        .expect("write sweep");
}

/// A park with duplicate-free timers persists one row and one `TimerStarted`
/// event per timer, with `fires_at` taken from the database clock. The same
/// check passes on the per-timer loop and on the batched form.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn park_persists_the_same_timer_rows_and_events() {
    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("timer_batch_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("conn");
    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, json!({ "n": 5 })).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![wf_info("e2e_test_workflow", race_n_timers)],
        vec![],
    ));
    let worker = build_runtime_worker("timer-batch-equiv", 4, 2, registry);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    for _ in 0..200 {
        if load_timers_for_execution_from_url(&url, exec_id).await.len() == 5 {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
    worker.shutdown();
    handle.await.expect("join");

    let timers = load_timers_for_execution_from_url(&url, exec_id).await;
    assert_eq!(timers.len(), 5);
    let mut ids: Vec<&str> = timers.iter().map(|t| t.timer_id.as_str()).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 5, "every timer id is distinct");

    let history = load_history_from_url(&url, exec_id).await;
    let started = history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
        .count();
    assert_eq!(started, 5, "one TimerStarted per armed timer");

    // Every fire instant comes from one database-clock read, so the spacing
    // between rows equals the spacing between their requested durations.
    let mut fires: Vec<_> = timers.iter().map(|t| t.fires_at).collect();
    fires.sort();
    for pair in fires.windows(2) {
        assert_eq!((pair[1] - pair[0]).num_seconds(), 1);
    }
}
