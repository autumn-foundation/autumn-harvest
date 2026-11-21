#![cfg(feature = "db")]
//! The terminal-sweep key read of `harvest_mutex_waiters` by waiter.
//!
//! Each workflow terminal transition calls
//! `mutex::sweep_terminal_holder_and_wake`. Its first statement reads the
//! keys that the run holds and the keys that it waits on. The waiter half
//! filters on `waiter_exec_id` alone. The unique key of the table leads with
//! `lock_key`, so no index serves that filter. A run that never took a mutex
//! then pays one scan of the whole waiter heap at its end.
//! `docs/performance-mutex-waiter-holder-scan.md` records the measurement.
//!
//! The capture seeds `docs/perf-artifacts/mutex-waiter-holder-scan/fixture.sql`
//! at three sizes. The fixture stands for other runs that hold or wait on a
//! mutex. Then the capture starts [`RUNS`] canonical three-activity runs
//! through the public start path. Half of them take one of four contended
//! mutex keys around their second step. A worker drives every run to its end.
//! The capture reads `pg_stat_statements` for the database, then explains the
//! key read on the fixture.
//!
//! Each size runs twice. The `without` arm drops the waiter index before the
//! seed. The `with` arm keeps it. A database without that index runs the
//! `without` arm only.
//!
//! The ignored capture needs a superuser and a preloaded
//! `pg_stat_statements`. `HARVEST_TEST_DATABASE_URL` is an admin URL. Without
//! it the harness starts a Postgres container.

#![allow(clippy::too_many_lines, clippy::cast_precision_loss)]

use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::info::{ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
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

const QUEUE: &str = "mutex-waiter-scan";
const WF: &str = "mutex_waiter_scan_wf";
const STEP: &str = "mutex_waiter_scan_step";
const ARTIFACT_DIR: &str = "docs/perf-artifacts/mutex-waiter-holder-scan";
const FIXTURE: &str =
    include_str!("../../../docs/perf-artifacts/mutex-waiter-holder-scan/fixture.sql");

/// The index that this page measures.
const WAITER_INDEX: &str = "idx_harvest_mutex_waiters_waiter";

/// Opens every harness query, so the statement capture can drop them.
const HARNESS: &str = "/* harness */ ";

/// The waiter rows of the fixture at each capture size.
const SIZES: [u64; 3] = [1_000, 10_000, 100_000];

/// The runs of one capture. Every second run takes a mutex.
const RUNS: usize = 200;

/// The contended keys of the runs that take a mutex.
const RUN_KEYS: usize = 4;

/// The terminal states of a run.
const TERMINAL: [&str; 5] = [
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TERMINATED",
    "TIMED_OUT",
];

type BoxFut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// Three steps. With a `key`, the second step runs under that mutex.
fn scan_wf(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        for step in 0..3 {
            let step_input = json!({ "step": step });
            match input["key"].as_str() {
                Some(key) if step == 1 => {
                    let guard = ctx
                        .mutex(key.to_string())
                        .acquire()
                        .await
                        .map_err(|e| e.to_string())?;
                    ctx.execute_activity_raw(STEP, step_input, QUEUE)
                        .await
                        .map_err(|e| e.to_string())?;
                    guard.release();
                }
                _ => {
                    ctx.execute_activity_raw(STEP, step_input, QUEUE)
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(json!({ "done": true }))
    })
}

fn scan_step(_ctx: &ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

fn workflow_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "mutex_waiter_holder_scan_perf",
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

fn step_info() -> ActivityInfo {
    ActivityInfo {
        name: STEP,
        module: "mutex_waiter_holder_scan_perf",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(60)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(QUEUE),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: scan_step,
        input_schema: None,
        output_schema: None,
    }
}

fn worker(worker_id: &str) -> Arc<Worker> {
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info(WF, scan_wf)],
        vec![step_info()],
    ));
    let mut config = runtime_config(worker_id, 16, 16, Duration::from_secs(30));
    config.queues = vec![QUEUE.to_string()];
    // Scanner election and the worker heartbeat write rows on a timer. They
    // do not belong to the runs, so the capture turns them off.
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
async fn create_db(admin_url: &str) -> (String, String) {
    let name = format!("mutex_waiter_scan_{}", uuid::Uuid::new_v4().simple());
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
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .expect("the capture needs a superuser and a preloaded pg_stat_statements");
    (name, url)
}

async fn drop_db(admin_url: &str, name: &str) {
    connect(admin_url)
        .await
        .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
        .await
        .expect("drop the database");
}

/// Runs `fixture.sql` at `waiters` rows, one statement at a time.
///
/// Each psql meta-command line and each comment line is skipped.
async fn seed(conn: &mut AsyncPgConnection, waiters: u64) {
    let body: String = FIXTURE
        .lines()
        .filter(|line| !line.starts_with('\\') && !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
        .replace(":waiters", &waiters.to_string());
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
    /// The terminal-sweep key read, or the older waiter-only key read.
    fn is_waiter_key_read(&self) -> bool {
        self.query.starts_with("SELECT")
            && self
                .query
                .contains("FROM harvest_mutex_waiters WHERE waiter_exec_id = $1")
    }

    /// A statement that writes a mutex table.
    fn writes_mutex_table(&self) -> bool {
        let q = self.query.as_str();
        (q.contains("INSERT INTO harvest_mutex_")
            || q.contains("DELETE FROM harvest_mutex_")
            || q.contains("UPDATE harvest_mutex_"))
            && self.wal_bytes > 0
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

async fn start_run(conn: &mut AsyncPgConnection, i: usize) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let workflow_id = format!("{WF}-{i}");
    let input = if i.is_multiple_of(2) {
        json!({ "key": format!("run-key-{}", (i / 2) % RUN_KEYS) })
    } else {
        json!({})
    };
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: WF,
            workflow_id: &workflow_id,
            exec_id,
            input: input.into(),
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
    .expect("start the run");
    exec_id
}

/// Starts [`RUNS`] runs, drives each one to its end and stops the worker.
async fn drive(url: &str, label: &str) -> (Vec<ExecutionId>, Duration) {
    let mut conn = connect(url).await;
    reset_statements(&mut conn).await;
    let mut ids = Vec::with_capacity(RUNS);
    for i in 0..RUNS {
        ids.push(start_run(&mut conn, i).await);
    }
    let pool = build_test_pool(url);
    let worker = worker(&format!("mutex-waiter-scan-{label}"));
    let started = Instant::now();
    let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
    let deadline = started + Duration::from_mins(20);
    let open = format!(
        "SELECT COUNT(*) AS v FROM harvest_workflow_executions \
         WHERE workflow_name = '{WF}' AND state NOT IN ({})",
        TERMINAL.map(|s| format!("'{s}'")).join(", ")
    );
    while int(&mut conn, &open).await > 0 {
        assert!(Instant::now() < deadline, "the runs must end in 20 minutes");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let window = started.elapsed();
    worker.shutdown();
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("the worker stops within 30 s")
        .expect("the worker task ends cleanly");
    drop(pool);
    (ids, window)
}

/// What the runs and the fixture left behind.
struct Outcome {
    completed: i64,
    /// Mutex rows that still name one of the runs. A correct sweep leaves 0.
    run_rows_left: i64,
    /// md5 over the fixture rows after the workload and an explicit sweep.
    digest: String,
}

/// Sweeps a fixed sample of fixture holders and waiters, then digests both
/// tables.
///
/// The sample holds one-row key holders, waiters at the head of a hot key
/// and waiters deep in a hot key. The digest covers the fixture keys only,
/// because the runs mint lock sequence values in a racy order.
async fn outcome(conn: &mut AsyncPgConnection, waiters: u64) -> Outcome {
    let completed = int(
        conn,
        &format!(
            "SELECT COUNT(*) AS v FROM harvest_workflow_executions \
             WHERE workflow_name = '{WF}' AND state = 'COMPLETED'"
        ),
    )
    .await;
    let run_rows_left = int(
        conn,
        &format!(
            "SELECT (SELECT COUNT(*) FROM harvest_mutex_waiters w \
                     JOIN harvest_workflow_executions e ON e.id = w.waiter_exec_id \
                     WHERE e.workflow_name = '{WF}') \
                  + (SELECT COUNT(*) FROM harvest_mutex_locks l \
                     JOIN harvest_workflow_executions e ON e.id = l.holder_exec_id \
                     WHERE e.workflow_name = '{WF}') AS v"
        ),
    )
    .await;
    let hot = (waiters * 8) / 10;
    let mut sample: Vec<String> = Vec::new();
    for g in (1..=50).map(|k| k * 7) {
        sample.push(format!("md5('order-{g}')::uuid"));
    }
    for g in (1..=50).map(|k| k * (hot / 51).max(1)) {
        sample.push(format!("md5('hot-{g}')::uuid"));
    }
    let ids: Vec<String> = diesel::sql_query(format!(
        "{HARNESS}SELECT x::text AS v FROM unnest(ARRAY[{}]) AS x",
        sample.join(", ")
    ))
    .load::<Text>(conn)
    .await
    .expect("resolve the sample ids")
    .into_iter()
    .map(|t| t.v)
    .collect();
    for id in ids {
        let exec: ExecutionId = id.parse().expect("a uuid");
        autumn_harvest::mutex::sweep_terminal_holder_and_wake(conn, exec)
            .await
            .expect("sweep a fixture execution");
    }
    let digest = text(
        conn,
        "SELECT md5( \
             COALESCE((SELECT string_agg(lock_key || '/' || holder_exec_id::text || '/' || lock_seq::text, ',' \
                        ORDER BY lock_key) \
                       FROM harvest_mutex_locks \
                       WHERE lock_key LIKE 'account:%' OR lock_key LIKE 'order:%'), '') \
             || '|' || \
             COALESCE((SELECT string_agg(lock_key || '/' || waiter_exec_id::text, ',' \
                        ORDER BY lock_key, id) \
                       FROM harvest_mutex_waiters \
                       WHERE lock_key LIKE 'account:%' OR lock_key LIKE 'order:%'), '') \
         ) AS v",
    )
    .await;
    Outcome {
        completed,
        run_rows_left,
        digest,
    }
}

/// The key read, explained with a generic plan as the prepared statement
/// cache runs it.
async fn explain(conn: &mut AsyncPgConnection) -> String {
    #[derive(diesel::QueryableByName)]
    struct Line {
        #[diesel(sql_type = diesel::sql_types::Text, column_name = "QUERY PLAN")]
        line: String,
    }
    conn.batch_execute(&format!(
        "PREPARE terminal_keys(uuid) AS {}; SET plan_cache_mode = force_generic_plan",
        autumn_harvest::mutex::terminal_keys_for_holder_stmt()
    ))
    .await
    .expect("prepare the key read");
    let cases = [
        (
            "no-mutex execution",
            "'00000000-0000-0000-0000-000000000001'::uuid",
        ),
        ("hot-key waiter", "md5('hot-7')::uuid"),
        ("one-row key holder", "md5('order-3')::uuid"),
    ];
    let mut out = String::new();
    for (name, arg) in cases {
        let plan: Vec<Line> = diesel::sql_query(format!(
            "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) EXECUTE terminal_keys({arg})"
        ))
        .load(conn)
        .await
        .unwrap_or_else(|e| panic!("explain {name}: {e}"));
        let _ = writeln!(out, "### {name}\n");
        for line in plan {
            let _ = writeln!(out, "    {}", line.line);
        }
        let _ = writeln!(out);
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

/// One capture of one size and one arm.
fn report(
    waiters: u64,
    arm: &str,
    window: Duration,
    shape: &str,
    outcome: &Outcome,
    rows: &[Statement],
) -> String {
    let mut out = String::new();
    let buffers: i64 = rows.iter().map(|s| s.buffers).sum();
    let calls: i64 = rows.iter().map(|s| s.calls).sum();
    let wal: i64 = rows.iter().map(|s| s.wal_bytes).sum();
    let temp: i64 = rows.iter().map(|s| s.temp_written).sum();
    let key_reads: Vec<&Statement> = rows.iter().filter(|s| s.is_waiter_key_read()).collect();
    let key_buffers: i64 = key_reads.iter().map(|s| s.buffers).sum();
    let key_calls: i64 = key_reads.iter().map(|s| s.calls).sum();
    let mutex_wal: i64 = rows
        .iter()
        .filter(|s| s.writes_mutex_table())
        .map(|s| s.wal_bytes)
        .sum();
    let pct = |part: i64, whole: i64| 100.0 * part as f64 / whole.max(1) as f64;
    let _ = writeln!(out, "## {waiters} fixture waiters, `{arm}` arm\n");
    let _ = writeln!(out, "Fixture: {shape}\n");
    let _ = writeln!(
        out,
        "Runs completed {} of {RUNS}, mutex rows left by the runs {}, fixture digest after the sample sweep `{}`, window {} ms\n",
        outcome.completed,
        outcome.run_rows_left,
        outcome.digest,
        window.as_millis()
    );
    let _ = writeln!(
        out,
        "| total buffers | total calls | WAL bytes | temp blocks written | mutex write WAL bytes |"
    );
    let _ = writeln!(out, "|--:|--:|--:|--:|--:|");
    let _ = writeln!(
        out,
        "| {buffers} | {calls} | {wal} | {temp} | {mutex_wal} |\n"
    );
    let _ = writeln!(
        out,
        "Waiter key read: {key_buffers} buffers ({:.1}% of buffers), {key_calls} calls \
         ({:.1}% of calls), {:.1} buffers per call, {:.1} buffers per run.\n",
        pct(key_buffers, buffers),
        pct(key_calls, calls),
        key_buffers as f64 / key_calls.max(1) as f64,
        key_buffers as f64 / RUNS as f64,
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
    let _ = writeln!(out, "\n### Mutex statements\n");
    let _ = writeln!(out, "| buffers | calls | rows | WAL bytes | statement |");
    let _ = writeln!(out, "|--:|--:|--:|--:|---|");
    for s in rows.iter().filter(|s| s.query.contains("harvest_mutex_")) {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | `{}` |",
            s.buffers,
            s.calls,
            s.rows,
            s.wal_bytes,
            short(&s.query)
        );
    }
    let temp_rows = rows.iter().filter(|s| s.temp_written > 0).count();
    let _ = writeln!(out, "\nStatements with temp blocks written: {temp_rows}\n");
    out
}

/// The key read probes the waiter index on a 10,000-waiter fixture.
///
/// A seq scan of the waiter heap here means that the index is gone, or that
/// the key read no longer filters on `waiter_exec_id` alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_key_read_probes_the_waiter_index() {
    let (admin, _guard) = setup_server().await;
    let name = format!("mutex_waiter_plan_{}", uuid::Uuid::new_v4().simple());
    connect(&admin)
        .await
        .batch_execute(&format!("CREATE DATABASE \"{name}\""))
        .await
        .expect("create the database");
    let url = db_url(&admin, &name);
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply the migration bundle");
    seed(&mut conn, 10_000).await;
    let plans = explain(&mut conn).await;
    drop(conn);
    drop_db(&admin, &name).await;
    assert!(
        !plans.contains("Seq Scan on public.harvest_mutex_waiters"),
        "the key read must not scan the waiter heap:\n{plans}"
    );
    assert_eq!(
        plans
            .matches(&format!("Index Scan using {WAITER_INDEX}"))
            .count(),
        3,
        "each explained key read must probe the waiter index:\n{plans}"
    );
}

/// The verdict line over both arms of every size.
///
/// Each size needs both arms, and the two digests of a size must match.
fn equivalence(digests: &[(u64, &str, String)]) -> String {
    let mut lines = Vec::new();
    for waiters in SIZES {
        let of = |arm: &str| {
            digests
                .iter()
                .find(|(w, a, _)| *w == waiters && *a == arm)
                .map(|(_, _, d)| d.clone())
        };
        match (of("without"), of("with")) {
            (Some(without), Some(with)) => {
                assert_eq!(
                    without, with,
                    "the two arms of {waiters} waiters must leave the same fixture rows"
                );
                lines.push(format!("{waiters} waiters `{with}`"));
            }
            _ => return "baseline only: the waiter index is absent".to_string(),
        }
    }
    format!("equivalence confirmed: {}", lines.join(", "))
}

/// Measures every size and arm, and writes `<PERF_LABEL>-capture.txt` and
/// `<PERF_LABEL>-explain.txt` under [`ARTIFACT_DIR`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "evidence capture: run by hand, see docs/performance-mutex-waiter-holder-scan.md"]
async fn zz_capture_mutex_waiter_holder_scan_evidence() {
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "capture".to_string());
    let (admin, _guard) = setup_server().await;
    let mut out = String::new();
    let _ = writeln!(out, "# Mutex waiter key read capture: {label}\n");
    let mut plans = String::new();
    let mut digests: Vec<(u64, &str, String)> = Vec::new();
    for waiters in SIZES {
        for arm in ["without", "with"] {
            let (name, url) = create_db(&admin).await;
            let mut conn = connect(&url).await;
            let present = int(
                &mut conn,
                &format!("SELECT COUNT(*) AS v FROM pg_class WHERE relname = '{WAITER_INDEX}'"),
            )
            .await;
            if arm == "with" && present == 0 {
                drop(conn);
                drop_db(&admin, &name).await;
                continue;
            }
            if arm == "without" {
                conn.batch_execute(&format!("DROP INDEX IF EXISTS {WAITER_INDEX}"))
                    .await
                    .expect("drop the waiter index");
            }
            seed(&mut conn, waiters).await;
            let shape = text(
                &mut conn,
                "SELECT format('%s live waiter rows, %s lock rows, %s heap pages, %s hot-key waiters, deepest key %s rows, indexes: %s', \
                     (SELECT COUNT(*) FROM harvest_mutex_waiters), \
                     (SELECT COUNT(*) FROM harvest_mutex_locks), \
                     pg_relation_size('harvest_mutex_waiters') / 8192, \
                     (SELECT COUNT(*) FROM harvest_mutex_waiters WHERE lock_key LIKE 'account:%'), \
                     (SELECT MAX(c) FROM (SELECT COUNT(*) c FROM harvest_mutex_waiters GROUP BY lock_key) d), \
                     (SELECT string_agg(indexrelname || ' ' || pg_relation_size(indexrelid), ', ' ORDER BY indexrelname) \
                      FROM pg_stat_user_indexes WHERE relname = 'harvest_mutex_waiters')) AS v",
            )
            .await;
            let (_ids, window) = drive(&url, &format!("{waiters}-{arm}")).await;
            // Each backend flushes its index counters when it ends.
            tokio::time::sleep(Duration::from_secs(2)).await;
            let rows = statements(&mut conn).await;
            let idx_scans = if arm == "with" {
                int(
                    &mut conn,
                    &format!(
                        "SELECT COALESCE(SUM(idx_scan), 0)::bigint AS v FROM pg_stat_user_indexes \
                         WHERE indexrelname = '{WAITER_INDEX}'"
                    ),
                )
                .await
            } else {
                0
            };
            let _ = writeln!(plans, "## {waiters} fixture waiters, `{arm}` arm\n");
            plans.push_str(&explain(&mut conn).await);
            let done = outcome(&mut conn, waiters).await;
            assert_eq!(
                usize::try_from(done.completed).unwrap(),
                RUNS,
                "every run must complete"
            );
            assert_eq!(done.run_rows_left, 0, "the sweep must free every run row");
            out.push_str(&report(waiters, arm, window, &shape, &done, &rows));
            digests.push((waiters, arm, done.digest.clone()));
            let _ = writeln!(
                out,
                "`{WAITER_INDEX}` idx_scan over the workload: {idx_scans}\n"
            );
            drop(conn);
            drop_db(&admin, &name).await;
        }
    }
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate has a parent")
        .join(ARTIFACT_DIR);
    std::fs::create_dir_all(&dir).expect("create the artifact directory");
    let verdict = equivalence(&digests);
    let _ = writeln!(out, "{verdict}");
    std::fs::write(dir.join(format!("{label}-capture.txt")), &out).expect("write the capture");
    std::fs::write(dir.join(format!("{label}-explain.txt")), &plans).expect("write the plans");
    println!("{out}");
}
