#![cfg(feature = "db")]
//! The per-decision timer lookups on a run with many fired timers.
//!
//! Each workflow decision reads the open timers of its run from
//! `harvest_timers` by `workflow_exec_id` and `fired`. A fired timer row stays
//! until retention deletes its run. So the lookup cost can grow with every
//! timer that the run has fired. `docs/performance-timer-pending-exec-lookup.md`
//! records the measurement.
//!
//! The capture seeds `docs/perf-artifacts/timer-pending-exec-lookup/seed.sql`.
//! That fixture holds 1.13 million timer rows on 20,000 other runs. It then
//! starts one `timer_loop` run through the public start path. The run fires
//! `n` zero-second timers in a row, one decision each. The worker drives it to
//! completion. The capture reads `pg_stat_statements` for the run and explains
//! the three lookups on the finished run.
//!
//! The structural test needs a plain database. The ignored capture needs a
//! superuser and a preloaded `pg_stat_statements`. `HARVEST_TEST_DATABASE_URL`
//! is an admin URL. Without it the harness starts a Postgres container.

#![allow(clippy::too_many_lines, clippy::cast_precision_loss)]

use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::info::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::types::{
    ExecutionId, Priority, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::{HandlerRegistry, Worker};
use autumn_harvest::{StartSource, StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

use super::integration_e2e::{build_test_pool, runtime_config, spawn_test_worker};

const QUEUE: &str = "timer-lookup";
const WF: &str = "timer_loop";
const ARTIFACT_DIR: &str = "docs/perf-artifacts/timer-pending-exec-lookup";
const SEED: &str = include_str!("../../../docs/perf-artifacts/timer-pending-exec-lookup/seed.sql");

/// The terminal states of a run.
const TERMINAL: [&str; 5] = [
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TERMINATED",
    "TIMED_OUT",
];

/// Opens every harness query, so the statement capture can drop them.
const HARNESS: &str = "/* harness */ ";

/// The run sizes of the capture. Each size gets a fresh database.
const SIZES: [u64; 3] = [250, 1_000, 2_000];

/// The run size of the structural test. Small, because it runs in CI.
const STRUCTURAL_TIMERS: u64 = 5;

type BoxFut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// Fires `n` zero-second timers in a row. Each timer is one decision.
fn timer_loop(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let n = input["n"].as_u64().ok_or("input needs n")?;
        for i in 0..n {
            ctx.timer(&format!("t-{i}"), 0)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(json!({ "fired": n }))
    })
}

fn workflow_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "timer_pending_exec_lookup_perf",
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

fn worker(worker_id: &str) -> Arc<Worker> {
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info(WF, timer_loop)],
        vec![],
    ));
    let mut config = runtime_config(worker_id, 1, 1, Duration::from_secs(30));
    config.queues = vec![QUEUE.to_string()];
    // Scanner election and the worker heartbeat write rows on a timer. The
    // seeded runs have no task rows, so no scanner pass belongs to the run.
    config.scanner.elect = false;
    config.worker_heartbeat_interval = Duration::from_secs(600);
    Arc::new(Worker::new(config, registry).expect("the worker builds"))
}

async fn setup_server() -> (String, Option<ContainerAsync<Postgres>>) {
    use testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .with_cmd([
            "-c",
            "shared_preload_libraries=pg_stat_statements",
            "-c",
            "fsync=off",
        ])
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to the database")
}

/// The admin URL with its database name replaced. A query string stays.
fn db_url(admin_url: &str, name: &str) -> String {
    let (base, query) = admin_url
        .split_once('?')
        .map_or((admin_url, None), |(b, q)| (b, Some(q)));
    let (prefix, _) = base.rsplit_once('/').expect("the URL has a db segment");
    query.map_or_else(
        || format!("{prefix}/{name}"),
        |q| format!("{prefix}/{name}?{q}"),
    )
}

/// A fresh, migrated database. Dropped by [`drop_db`].
async fn create_db(admin_url: &str, stats: bool) -> (String, String) {
    let name = format!("timer_lookup_{}", uuid::Uuid::new_v4().simple());
    connect(admin_url)
        .await
        .batch_execute(&format!("CREATE DATABASE \"{name}\""))
        .await
        .expect("create the database");
    let url = db_url(admin_url, &name);
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply the migration bundle");
    if stats {
        conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .await
            .expect("the capture needs a superuser and a preloaded pg_stat_statements");
    }
    (name, url)
}

async fn drop_db(admin_url: &str, name: &str) {
    connect(admin_url)
        .await
        .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
        .await
        .expect("drop the database");
}

/// Runs `seed.sql` one statement at a time, because `VACUUM` cannot run in
/// the implicit transaction of a multi-statement query. Each psql
/// meta-command line is skipped.
async fn seed(conn: &mut AsyncPgConnection) {
    let body: String = SEED
        .lines()
        .filter(|line| !line.starts_with('\\') && !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for statement in body.split(";\n").map(str::trim).filter(|s| !s.is_empty()) {
        conn.batch_execute(statement)
            .await
            .unwrap_or_else(|e| panic!("seed statement failed: {e}\n{statement}"));
    }
}

#[derive(diesel::QueryableByName)]
struct Int {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    v: i64,
}

async fn int(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(format!("{HARNESS}{sql}"))
        .get_result::<Int>(conn)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        .v
}

#[derive(diesel::QueryableByName)]
struct Text {
    #[diesel(sql_type = diesel::sql_types::Text)]
    v: String,
}

async fn text(conn: &mut AsyncPgConnection, sql: &str) -> String {
    diesel::sql_query(format!("{HARNESS}{sql}"))
        .get_result::<Text>(conn)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        .v
}

async fn reset_statements(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "SELECT pg_stat_statements_reset(0, \
             (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
    .await
    .expect("reset pg_stat_statements");
}

#[derive(diesel::QueryableByName, Debug)]
struct Statement {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    rows: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    buffers: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    temp_written: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    wal_bytes: i64,
}

impl Statement {
    /// A per-decision read of the open timers of one run.
    fn is_timer_lookup(&self) -> bool {
        self.query.starts_with("SELECT")
            && self.query.contains("FROM \"harvest_timers\"")
            && self
                .query
                .contains("\"harvest_timers\".\"workflow_exec_id\" = $")
    }
}

async fn statements(conn: &mut AsyncPgConnection) -> Vec<Statement> {
    diesel::sql_query(
        "SELECT query, calls, rows, \
                (shared_blks_hit + shared_blks_read)::bigint AS buffers, \
                temp_blks_written::bigint AS temp_written, \
                wal_bytes::bigint AS wal_bytes \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND query NOT ILIKE '%pg_stat%' \
           AND query NOT LIKE '%/* harness */%' \
         ORDER BY buffers DESC, calls DESC, query",
    )
    .load(conn)
    .await
    .expect("read pg_stat_statements")
}

async fn start_run(conn: &mut AsyncPgConnection, n: u64) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let workflow_id = format!("{WF}-{n}");
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: WF,
            workflow_id: &workflow_id,
            exec_id,
            input: json!({ "n": n }).into(),
            parent_id: None,
            queue_name: QUEUE,
            execution_timeout: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            workflow_retry_source: autumn_harvest::workflow_retry::RetryPolicySource::Type,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: StartSource::Api,
            start_source_ref: None,
            started_by: None,
            fairness_key: None,
            tenant: None,
        },
        None,
    )
    .await
    .expect("start the timer loop");
    exec_id
}

/// What one run left behind, read after the worker stops.
struct RunShape {
    state: String,
    timers: i64,
    unfired: i64,
    timer_fired_events: i64,
    /// md5 over the ordered timer rows of the run, without the random ids.
    digest: String,
}

async fn run_shape(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> RunShape {
    let id = exec_id.as_uuid();
    RunShape {
        state: text(
            conn,
            &format!("SELECT state::text AS v FROM harvest_workflow_executions WHERE id = '{id}'"),
        )
        .await,
        timers: int(
            conn,
            &format!("SELECT COUNT(*) AS v FROM harvest_timers WHERE workflow_exec_id = '{id}'"),
        )
        .await,
        unfired: int(
            conn,
            &format!(
                "SELECT COUNT(*) AS v FROM harvest_timers \
                 WHERE workflow_exec_id = '{id}' AND NOT fired"
            ),
        )
        .await,
        timer_fired_events: int(
            conn,
            &format!(
                "SELECT COUNT(*) AS v FROM harvest_events \
                 WHERE workflow_exec_id = '{id}' AND event_type = 'TimerFired'"
            ),
        )
        .await,
        digest: text(
            conn,
            &format!(
                "SELECT md5(COALESCE(string_agg(timer_id || ':' || fired::text, ',' \
                     ORDER BY timer_id), '')) AS v \
                 FROM harvest_timers WHERE workflow_exec_id = '{id}'"
            ),
        )
        .await,
    }
}

/// Drives one run of `n` timers to completion and stops the worker.
async fn drive(url: &str, n: u64, stats: bool) -> (ExecutionId, Duration) {
    let mut conn = connect(url).await;
    if stats {
        reset_statements(&mut conn).await;
    }
    let exec_id = start_run(&mut conn, n).await;
    let pool = build_test_pool(url);
    let worker = worker(&format!("timer-lookup-{n}"));
    let started = Instant::now();
    let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
    let deadline = started + Duration::from_mins(30);
    loop {
        let run_state = text(
            &mut conn,
            &format!(
                "SELECT state::text AS v FROM harvest_workflow_executions WHERE id = '{}'",
                exec_id.as_uuid()
            ),
        )
        .await;
        if TERMINAL.contains(&run_state.as_str()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the timer loop must finish in 30 minutes"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let window = started.elapsed();
    worker.shutdown();
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("the worker stops within 30 s")
        .expect("the worker task ends cleanly");
    drop(pool);
    (exec_id, window)
}

/// The three lookups of the worker, explained on the finished run.
///
/// The run is complete, so no timer is open. The lookups show the scan that
/// a decision of the run paid while it read its fired timers.
async fn explain_lookups(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    let id = exec_id.as_uuid();
    let lookups = [
        (
            "ingest",
            "PREPARE ingest(uuid, boolean) AS SELECT \"harvest_timers\".\"id\", \"harvest_timers\".\"workflow_exec_id\", \"harvest_timers\".\"timer_id\", \"harvest_timers\".\"fires_at\", \"harvest_timers\".\"fired\" FROM \"harvest_timers\" WHERE (((\"harvest_timers\".\"workflow_exec_id\" = $1) AND (\"harvest_timers\".\"fired\" = $2)) AND (\"harvest_timers\".\"fires_at\" <= NOW())) ORDER BY \"harvest_timers\".\"fires_at\" ASC, \"harvest_timers\".\"timer_id\" ASC",
            format!("EXECUTE ingest('{id}', false)"),
        ),
        (
            "by_id",
            "PREPARE by_id(uuid, text, boolean, bigint) AS SELECT \"harvest_timers\".\"id\", \"harvest_timers\".\"workflow_exec_id\", \"harvest_timers\".\"timer_id\", \"harvest_timers\".\"fires_at\", \"harvest_timers\".\"fired\" FROM \"harvest_timers\" WHERE (((\"harvest_timers\".\"workflow_exec_id\" = $1) AND (\"harvest_timers\".\"timer_id\" = $2)) AND (\"harvest_timers\".\"fired\" = $3)) LIMIT $4",
            format!("EXECUTE by_id('{id}', 't-next', false, 1)"),
        ),
        (
            "by_ids",
            "PREPARE by_ids(uuid, text[], boolean) AS SELECT \"harvest_timers\".\"id\", \"harvest_timers\".\"workflow_exec_id\", \"harvest_timers\".\"timer_id\", \"harvest_timers\".\"fires_at\", \"harvest_timers\".\"fired\" FROM \"harvest_timers\" WHERE (((\"harvest_timers\".\"workflow_exec_id\" = $1) AND (\"harvest_timers\".\"timer_id\" = ANY($2))) AND (\"harvest_timers\".\"fired\" = $3)) ORDER BY \"harvest_timers\".\"fires_at\" ASC, \"harvest_timers\".\"id\" ASC",
            format!("EXECUTE by_ids('{id}', ARRAY['t-next'], false)"),
        ),
    ];
    let mut out = String::new();
    for (name, prepare, _) in &lookups {
        conn.batch_execute(prepare)
            .await
            .unwrap_or_else(|e| panic!("prepare {name}: {e}"));
    }
    for mode in ["force_generic_plan", "force_custom_plan"] {
        conn.batch_execute(&format!("SET plan_cache_mode = {mode}"))
            .await
            .expect("set plan_cache_mode");
        for (name, _, execute) in &lookups {
            #[derive(diesel::QueryableByName)]
            struct Line {
                #[diesel(sql_type = diesel::sql_types::Text, column_name = "QUERY PLAN")]
                line: String,
            }
            let plan: Vec<Line> = diesel::sql_query(format!(
                "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) {execute}"
            ))
            .load(conn)
            .await
            .unwrap_or_else(|e| panic!("explain {name}: {e}"));
            let _ = writeln!(out, "### {name}, plan_cache_mode = {mode}\n");
            for line in plan {
                let _ = writeln!(out, "    {}", line.line);
            }
            let _ = writeln!(out);
        }
    }
    conn.batch_execute("RESET plan_cache_mode; DEALLOCATE ALL")
        .await
        .expect("reset the session");
    out
}

fn short(query: &str) -> String {
    let flat = query.split_whitespace().collect::<Vec<_>>().join(" ");
    let cut: String = flat.chars().take(130).collect();
    cut.replace('|', "\\|")
}

/// One capture of one run size.
fn report(n: u64, window: Duration, shape: &RunShape, rows: &[Statement]) -> String {
    let mut out = String::new();
    let buffers: i64 = rows.iter().map(|s| s.buffers).sum();
    let calls: i64 = rows.iter().map(|s| s.calls).sum();
    let wal: i64 = rows.iter().map(|s| s.wal_bytes).sum();
    let temp: i64 = rows.iter().map(|s| s.temp_written).sum();
    let timer_buffers: i64 = rows
        .iter()
        .filter(|s| s.is_timer_lookup())
        .map(|s| s.buffers)
        .sum();
    let timer_calls: i64 = rows
        .iter()
        .filter(|s| s.is_timer_lookup())
        .map(|s| s.calls)
        .sum();
    let pct = |part: i64, whole: i64| 100.0 * part as f64 / whole.max(1) as f64;
    let _ = writeln!(out, "## Run of {n} timers\n");
    let _ = writeln!(
        out,
        "state {}, timer rows {}, unfired {}, TimerFired events {}, timer digest `{}`, window {} ms\n",
        shape.state,
        shape.timers,
        shape.unfired,
        shape.timer_fired_events,
        shape.digest,
        window.as_millis()
    );
    let _ = writeln!(
        out,
        "| total buffers | total calls | WAL bytes | temp blocks written |"
    );
    let _ = writeln!(out, "|--:|--:|--:|--:|");
    let _ = writeln!(out, "| {buffers} | {calls} | {wal} | {temp} |\n");
    let _ = writeln!(
        out,
        "Timer lookups: {timer_buffers} buffers ({:.1}% of buffers), {timer_calls} calls \
         ({:.1}% of calls), {:.1} buffers per call, {:.1} buffers per timer.\n",
        pct(timer_buffers, buffers),
        pct(timer_calls, calls),
        timer_buffers as f64 / timer_calls.max(1) as f64,
        timer_buffers as f64 / n as f64,
    );
    let _ = writeln!(out, "### Top 10 by buffers\n");
    let _ = writeln!(out, "| buffers | % | calls | rows | statement |");
    let _ = writeln!(out, "|--:|--:|--:|--:|---|");
    for s in rows.iter().take(10) {
        let _ = writeln!(
            out,
            "| {} | {:.1} | {} | {} | `{}` |",
            s.buffers,
            pct(s.buffers, buffers),
            s.calls,
            s.rows,
            short(&s.query)
        );
    }
    let mut by_calls: Vec<&Statement> = rows.iter().collect();
    by_calls.sort_by(|a, b| b.calls.cmp(&a.calls).then_with(|| a.query.cmp(&b.query)));
    let _ = writeln!(out, "\n### Top 10 by calls\n");
    let _ = writeln!(out, "| calls | % | buffers | statement |");
    let _ = writeln!(out, "|--:|--:|--:|---|");
    for s in by_calls.iter().take(10) {
        let _ = writeln!(
            out,
            "| {} | {:.1} | {} | `{}` |",
            s.calls,
            pct(s.calls, calls),
            s.buffers,
            short(&s.query)
        );
    }
    let mut by_wal: Vec<&Statement> = rows.iter().filter(|s| s.wal_bytes > 0).collect();
    by_wal.sort_by(|a, b| {
        b.wal_bytes
            .cmp(&a.wal_bytes)
            .then_with(|| a.query.cmp(&b.query))
    });
    let _ = writeln!(out, "\n### Top 10 by WAL bytes\n");
    let _ = writeln!(out, "| WAL bytes | calls | statement |");
    let _ = writeln!(out, "|--:|--:|---|");
    for s in by_wal.iter().take(10) {
        let _ = writeln!(
            out,
            "| {} | {} | `{}` |",
            s.wal_bytes,
            s.calls,
            short(&s.query)
        );
    }
    let _ = writeln!(out, "\n### Timer lookups\n");
    for s in rows.iter().filter(|s| s.is_timer_lookup()) {
        let _ = writeln!(
            out,
            "* {} buffers, {} calls, {} rows:\n  `{}`",
            s.buffers,
            s.calls,
            s.rows,
            s.query.split_whitespace().collect::<Vec<_>>().join(" ")
        );
    }
    let _ = writeln!(out);
    out
}

/// A small run on an empty fixture: one fired timer row and one
/// `TimerFired` event per timer, and no open timer at the end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timer_loop_leaves_one_fired_row_per_timer() {
    let (admin, _guard) = setup_server().await;
    let (name, url) = create_db(&admin, false).await;
    let (exec_id, _) = drive(&url, STRUCTURAL_TIMERS, false).await;
    let mut conn = connect(&url).await;
    let shape = run_shape(&mut conn, exec_id).await;
    drop(conn);
    drop_db(&admin, &name).await;
    assert_eq!(shape.state, "COMPLETED");
    assert_eq!(shape.timers, i64::try_from(STRUCTURAL_TIMERS).unwrap());
    assert_eq!(shape.unfired, 0);
    assert_eq!(shape.timer_fired_events, shape.timers);
}

/// Measures every run size and writes `<PERF_LABEL>-capture.txt` under
/// [`ARTIFACT_DIR`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "evidence capture: run by hand, see docs/performance-timer-pending-exec-lookup.md"]
async fn zz_capture_timer_pending_exec_lookup_evidence() {
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "capture".to_string());
    let (admin, _guard) = setup_server().await;
    let mut out = String::new();
    let _ = writeln!(out, "# Timer lookup capture: {label}\n");
    let mut plans = String::new();
    for n in SIZES {
        let (name, url) = create_db(&admin, true).await;
        let mut conn = connect(&url).await;
        seed(&mut conn).await;
        let indexes = text(
            &mut conn,
            "SELECT string_agg(indexrelname || ' ' || pg_relation_size(indexrelid), ', ' \
                 ORDER BY indexrelname) AS v \
             FROM pg_stat_user_indexes WHERE relname = 'harvest_timers'",
        )
        .await;
        let (exec_id, window) = drive(&url, n, true).await;
        let rows = statements(&mut conn).await;
        let shape = run_shape(&mut conn, exec_id).await;
        assert_eq!(
            shape.state, "COMPLETED",
            "the run of {n} timers must complete"
        );
        assert_eq!(shape.timer_fired_events, i64::try_from(n).unwrap());
        out.push_str(&report(n, window, &shape, &rows));
        let _ = writeln!(
            out,
            "harvest_timers indexes at seed time (bytes): {indexes}\n"
        );
        let _ = writeln!(plans, "## Run of {n} timers\n");
        plans.push_str(&explain_lookups(&mut conn, exec_id).await);
        drop(conn);
        drop_db(&admin, &name).await;
    }
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate has a parent")
        .join(ARTIFACT_DIR);
    std::fs::create_dir_all(&dir).expect("create the artifact directory");
    std::fs::write(dir.join(format!("{label}-capture.txt")), &out).expect("write the capture");
    std::fs::write(dir.join(format!("{label}-explain.txt")), &plans).expect("write the plans");
    println!("{out}");
}
