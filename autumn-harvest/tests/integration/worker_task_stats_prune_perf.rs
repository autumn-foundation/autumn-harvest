#![cfg(feature = "db")]
//! The task-stats prune of each worker heartbeat (issue #1815).
//!
//! Each heartbeat of each worker runs `prune_worker_task_stats` on its shard.
//! The prune keeps a row for the longer of the retention and its cohort's
//! `peer_stale_secs`. That threshold comes from the stored cohort key, so the
//! cost of the prune can grow with the whole table on every heartbeat.
//! `docs/performance-worker-task-stats-prune.md` records the measurement.
//!
//! The capture starts four real workers at the production cadence: a 5 s
//! heartbeat and a 500 ms poll. Each worker registers 30 workflow types, so
//! its cohort key has a production width. The capture then seeds
//! `docs/perf-artifacts/worker-task-stats-prune/seed.sql` beside them. That
//! fixture is a fleet of live workers, orphan rows of restarted pods, rows
//! past the retention and dead tuples. The capture reads `pg_stat_statements`
//! for a 60 s window and explains the prune on the final state.
//!
//! The structural test needs a plain database. The ignored capture needs a
//! superuser and a preloaded `pg_stat_statements`. `HARVEST_TEST_DATABASE_URL`
//! is an admin URL. Without it the harness starts a Postgres container.

#![allow(clippy::too_many_lines, clippy::cast_precision_loss)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::info::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::worker::{HandlerRegistry, Worker};
use autumn_harvest::workers;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

use super::integration_e2e::{build_test_pool, runtime_config, spawn_test_worker};

const QUEUE: &str = "task-stats-prune";
const ARTIFACT_DIR: &str = "docs/perf-artifacts/worker-task-stats-prune";
const SEED: &str = include_str!("../../../docs/perf-artifacts/worker-task-stats-prune/seed.sql");
const EDGES: &str =
    include_str!("../../../docs/perf-artifacts/worker-task-stats-prune/equivalence.sql");

/// Opens every harness query, so the statement capture can drop them.
const HARNESS: &str = "/* harness */ ";

/// The live fleet sizes of the capture. Each size gets a fresh database.
const FLEETS: [u32; 3] = [500, 2_000, 8_000];

/// The measured workers of each capture.
const MEASURED: usize = 4;

/// The registered workflow types of each measured worker.
const WORKFLOW_TYPES: usize = 30;

/// The measured window. A fleet row stays live for 120 s after the seed, so
/// the whole window sees a live fleet.
const WINDOW: Duration = Duration::from_secs(60);

/// The fallback that a worker with a 5 s heartbeat passes to the prune.
const FAST_FALLBACK: i64 = 120;

// ── workers ──────────────────────────────────────────────────────────────────

type BoxFut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

fn idle(_ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(Value::Null) })
}

fn workflow_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "worker_task_stats_prune_perf",
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

/// The workflow types of a production service. Each worker gets the same
/// set, so the measured workers share one cohort.
fn workflow_types() -> Vec<WorkflowInfo> {
    const DOMAINS: [&str; 6] = [
        "billing",
        "orders",
        "fulfilment",
        "identity",
        "ledger",
        "search",
    ];
    const ACTIONS: [&str; 5] = [
        "reconcile_batch",
        "settle_account",
        "rebuild_projection",
        "notify_subscribers",
        "expire_reservations",
    ];
    (0..WORKFLOW_TYPES)
        .map(|i| {
            let name = format!(
                "{}.{}_v{}",
                DOMAINS[i % DOMAINS.len()],
                ACTIONS[i / DOMAINS.len() % ACTIONS.len()],
                i
            );
            workflow_info(Box::leak(name.into_boxed_str()), idle)
        })
        .collect()
}

fn worker(worker_id: &str) -> Arc<Worker> {
    let registry = Arc::new(HandlerRegistry::new(workflow_types(), vec![]));
    let mut config = runtime_config(worker_id, 20, 50, Duration::from_secs(30));
    config.queues = vec![QUEUE.to_string()];
    config.poll_interval = autumn_harvest::worker::DEFAULT_WORKER_POLL_INTERVAL;
    config.worker_heartbeat_interval = Duration::from_secs(5);
    // Scanner election runs on one worker of a shard. The measured workers
    // stand for the rest of the fleet, which runs no scanner pass.
    config.scanner.elect = false;
    Arc::new(Worker::new(config, registry).expect("the worker builds"))
}

// ── database ─────────────────────────────────────────────────────────────────

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
    let name = format!("stats_prune_{}", uuid::Uuid::new_v4().simple());
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

/// Runs a SQL file one statement at a time. A statement with `$1` binds
/// `key`. Comment lines are skipped, and `__FLEET__` becomes `fleet`.
async fn run_sql(conn: &mut AsyncPgConnection, sql: &str, fleet: u32, key: &str) {
    let body: String = sql
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
        .replace("__FLEET__", &fleet.to_string());
    for statement in body.split(";\n").map(str::trim).filter(|s| !s.is_empty()) {
        let statement = format!("{HARNESS}{statement}");
        let result = if statement.contains("$1") {
            diesel::sql_query(&statement)
                .bind::<diesel::sql_types::Text, _>(key)
                .execute(conn)
                .await
                .map(|_| ())
        } else {
            conn.batch_execute(&statement).await
        };
        result.unwrap_or_else(|e| panic!("seed statement failed: {e}\n{statement}"));
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
    hit: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    read: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    temp_written: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    wal_bytes: i64,
}

impl Statement {
    const fn buffers(&self) -> i64 {
        self.hit + self.read
    }

    /// The heartbeat prune of `prune_worker_task_stats`.
    fn is_prune(&self) -> bool {
        self.query.contains("DELETE FROM harvest_worker_task_stats")
    }
}

async fn statements(conn: &mut AsyncPgConnection) -> Vec<Statement> {
    diesel::sql_query(
        "SELECT query, calls, rows, \
                shared_blks_hit::bigint AS hit, shared_blks_read::bigint AS read, \
                temp_blks_written::bigint AS temp_written, \
                wal_bytes::bigint AS wal_bytes \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND query NOT ILIKE '%pg_stat%' \
           AND query NOT LIKE '%/* harness */%' \
         ORDER BY shared_blks_hit + shared_blks_read DESC, calls DESC, query",
    )
    .load(conn)
    .await
    .expect("read pg_stat_statements")
}

/// The scan, block and tuple counters of the stats table.
#[derive(diesel::QueryableByName, Debug, Clone, Copy, Default)]
struct TableCounters {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    seq_scan: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    seq_tup_read: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    updated_at_idx_scan: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    heap_blks: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    idx_blks: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    toast_blks: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n_live_tup: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n_dead_tup: i64,
}

impl TableCounters {
    const fn minus(self, before: Self) -> Self {
        Self {
            seq_scan: self.seq_scan - before.seq_scan,
            seq_tup_read: self.seq_tup_read - before.seq_tup_read,
            updated_at_idx_scan: self.updated_at_idx_scan - before.updated_at_idx_scan,
            heap_blks: self.heap_blks - before.heap_blks,
            idx_blks: self.idx_blks - before.idx_blks,
            toast_blks: self.toast_blks - before.toast_blks,
            n_live_tup: self.n_live_tup,
            n_dead_tup: self.n_dead_tup,
        }
    }
}

async fn table_counters(conn: &mut AsyncPgConnection) -> TableCounters {
    // The statistics views are a snapshot per transaction. Clear it first.
    conn.batch_execute("SELECT pg_stat_clear_snapshot()")
        .await
        .expect("clear the stats snapshot");
    diesel::sql_query(format!(
        "{HARNESS}SELECT t.seq_scan, t.seq_tup_read, \
                COALESCE((SELECT idx_scan FROM pg_stat_user_indexes \
                          WHERE indexrelname = 'harvest_worker_task_stats_updated_at_idx'), 0) \
                    AS updated_at_idx_scan, \
                (io.heap_blks_hit + io.heap_blks_read) AS heap_blks, \
                COALESCE(io.idx_blks_hit + io.idx_blks_read, 0) AS idx_blks, \
                COALESCE(io.toast_blks_hit + io.toast_blks_read, 0) AS toast_blks, \
                t.n_live_tup, t.n_dead_tup \
         FROM pg_stat_user_tables t \
         JOIN pg_statio_user_tables io USING (relid) \
         WHERE t.relname = 'harvest_worker_task_stats'"
    ))
    .get_result(conn)
    .await
    .expect("read the table counters")
}

#[derive(diesel::QueryableByName)]
struct Wait {
    #[diesel(sql_type = diesel::sql_types::Text)]
    wait: String,
}

/// One sample of the wait events of the other backends of this database.
async fn sample_waits(conn: &mut AsyncPgConnection, tally: &mut BTreeMap<String, u64>) {
    let rows: Vec<Wait> = diesel::sql_query(format!(
        "{HARNESS}SELECT COALESCE(wait_event_type || ':' || wait_event, 'CPU or none') \
                    || ' (' || state || ')' AS wait \
         FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() \
           AND backend_type = 'client backend' AND state IS NOT NULL"
    ))
    .load(conn)
    .await
    .expect("sample pg_stat_activity");
    for row in rows {
        *tally.entry(row.wait).or_default() += 1;
    }
}

/// The cohort key that a measured worker wrote, once its first heartbeat
/// has run.
async fn measured_key(conn: &mut AsyncPgConnection) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let present = int(
            conn,
            "SELECT COUNT(*) AS v FROM harvest_worker_task_stats WHERE worker_id = 'measured-0'",
        )
        .await;
        if present == 1 {
            return text(
                conn,
                "SELECT cohort AS v FROM harvest_worker_task_stats WHERE worker_id = 'measured-0'",
            )
            .await;
        }
        assert!(
            Instant::now() < deadline,
            "the first heartbeat writes a stats row within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The ids left in the stats table, in order, and an md5 over them.
async fn remaining(conn: &mut AsyncPgConnection, filter: &str) -> (i64, String) {
    let count = int(
        conn,
        &format!("SELECT COUNT(*) AS v FROM harvest_worker_task_stats WHERE {filter}"),
    )
    .await;
    let digest = text(
        conn,
        &format!(
            "SELECT md5(COALESCE(string_agg(worker_id, ',' ORDER BY worker_id), '')) AS v \
             FROM harvest_worker_task_stats WHERE {filter}"
        ),
    )
    .await;
    (count, digest)
}

/// The prune of the build under test, explained on the final state.
///
/// The capture runs the public prune once on its own connection, in a
/// transaction that rolls back. A Diesel instrumentation hook records the
/// SQL text, so the capture explains the same statement that the workers
/// sent. Each run rolls back.
async fn explain_prune(conn: &mut AsyncPgConnection) -> String {
    #[derive(diesel::QueryableByName)]
    struct Line {
        #[diesel(sql_type = diesel::sql_types::Text, column_name = "QUERY PLAN")]
        line: String,
    }
    let seen = Arc::new(std::sync::Mutex::new(None::<String>));
    let sink = Arc::clone(&seen);
    conn.set_instrumentation(move |event: diesel::connection::InstrumentationEvent<'_>| {
        if let diesel::connection::InstrumentationEvent::StartQuery { query, .. } = event {
            let sql = query.to_string();
            if sql.starts_with("DELETE FROM harvest_worker_task_stats") {
                let sql = sql
                    .split(" -- binds:")
                    .next()
                    .unwrap_or_default()
                    .to_string();
                *sink.lock().expect("the sink lock") = Some(sql);
            }
        }
    });
    conn.batch_execute("BEGIN").await.expect("begin");
    workers::prune_worker_task_stats(conn, FAST_FALLBACK)
        .await
        .expect("prune");
    conn.batch_execute("ROLLBACK").await.expect("rollback");
    conn.set_instrumentation(|_: diesel::connection::InstrumentationEvent<'_>| {});
    let prune = seen
        .lock()
        .expect("the sink lock")
        .take()
        .expect("the hook saw the prune");
    conn.batch_execute(&format!("PREPARE prune(bigint, bigint) AS {prune}"))
        .await
        .expect("prepare the prune");
    let retention = workers::WORKER_TASK_STATS_RETENTION.as_secs();
    let mut out = String::new();
    for mode in ["force_custom_plan", "force_generic_plan"] {
        conn.batch_execute(&format!("BEGIN; SET LOCAL plan_cache_mode = {mode}"))
            .await
            .expect("begin");
        let plan: Vec<Line> = diesel::sql_query(format!(
            "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) \
             EXECUTE prune({retention}, {FAST_FALLBACK})"
        ))
        .load(conn)
        .await
        .expect("explain the prune");
        conn.batch_execute("ROLLBACK").await.expect("rollback");
        let _ = writeln!(out, "### plan_cache_mode = {mode}\n");
        for line in plan {
            let _ = writeln!(out, "    {}", line.line);
        }
        let _ = writeln!(out);
    }
    conn.batch_execute("DEALLOCATE prune")
        .await
        .expect("deallocate");
    let _ = writeln!(out, "Statement:\n\n    {prune}\n");
    out
}

fn short(query: &str) -> String {
    let flat = query.split_whitespace().collect::<Vec<_>>().join(" ");
    let cut: String = flat.chars().take(130).collect();
    cut.replace('|', "\\|")
}

/// What one capture saw.
struct Capture {
    fleet: u32,
    rows: Vec<Statement>,
    table: TableCounters,
    waits: BTreeMap<String, u64>,
    seeded: i64,
    after_first_prune: (i64, String),
    heap_bytes: i64,
    toast_bytes: i64,
    avg_key_bytes: i64,
}

fn report(c: &Capture) -> String {
    let mut out = String::new();
    let rows = &c.rows;
    let buffers: i64 = rows.iter().map(Statement::buffers).sum();
    let calls: i64 = rows.iter().map(|s| s.calls).sum();
    let wal: i64 = rows.iter().map(|s| s.wal_bytes).sum();
    let temp: i64 = rows.iter().map(|s| s.temp_written).sum();
    let prune: Vec<&Statement> = rows.iter().filter(|s| s.is_prune()).collect();
    let prune_buffers: i64 = prune.iter().map(|s| s.buffers()).sum();
    let prune_calls: i64 = prune.iter().map(|s| s.calls).sum();
    let prune_rows: i64 = prune.iter().map(|s| s.rows).sum();
    let pct = |part: i64, whole: i64| 100.0 * part as f64 / whole.max(1) as f64;
    let _ = writeln!(out, "## Live fleet of {}\n", c.fleet);
    let _ = writeln!(
        out,
        "Stats rows at seed {}, after the first prune {} (md5 `{}`). Heap {} bytes, \
         TOAST {} bytes, average cohort key {} bytes.\n",
        c.seeded,
        c.after_first_prune.0,
        c.after_first_prune.1,
        c.heap_bytes,
        c.toast_bytes,
        c.avg_key_bytes
    );
    let _ = writeln!(
        out,
        "| total buffers | hit | read | total calls | WAL bytes | temp blocks written |"
    );
    let _ = writeln!(out, "|--:|--:|--:|--:|--:|--:|");
    let _ = writeln!(
        out,
        "| {buffers} | {} | {} | {calls} | {wal} | {temp} |\n",
        rows.iter().map(|s| s.hit).sum::<i64>(),
        rows.iter().map(|s| s.read).sum::<i64>(),
    );
    let _ = writeln!(
        out,
        "Prune: {prune_buffers} buffers ({:.1}% of buffers), {prune_calls} calls \
         ({:.1}% of calls), {prune_rows} rows deleted, {:.1} buffers per call.\n",
        pct(prune_buffers, buffers),
        pct(prune_calls, calls),
        prune_buffers as f64 / prune_calls.max(1) as f64,
    );
    let t = c.table;
    let _ = writeln!(
        out,
        "harvest_worker_task_stats over the window: seq_scan {}, seq_tup_read {}, \
         idx_scan on harvest_worker_task_stats_updated_at_idx {}, heap blocks {}, \
         index blocks {}, TOAST blocks {}. At the end n_live_tup {}, n_dead_tup {}.\n",
        t.seq_scan,
        t.seq_tup_read,
        t.updated_at_idx_scan,
        t.heap_blks,
        t.idx_blks,
        t.toast_blks,
        t.n_live_tup,
        t.n_dead_tup
    );
    let _ = writeln!(out, "### Top 10 by buffers\n");
    let _ = writeln!(out, "| buffers | % | calls | rows | statement |");
    let _ = writeln!(out, "|--:|--:|--:|--:|---|");
    for s in rows.iter().take(10) {
        let _ = writeln!(
            out,
            "| {} | {:.1} | {} | {} | `{}` |",
            s.buffers(),
            pct(s.buffers(), buffers),
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
            s.buffers(),
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
    let spills = rows.iter().filter(|s| s.temp_written > 0).count();
    let _ = writeln!(out, "\nStatements with temp_blks_written > 0: {spills}.\n");
    let _ = writeln!(out, "### Wait-event samples (1 per second)\n");
    for (wait, n) in &c.waits {
        let _ = writeln!(out, "* {wait}: {n}");
    }
    let _ = writeln!(out, "\n### Prune statement\n");
    for s in &prune {
        let _ = writeln!(
            out,
            "* {} buffers, {} calls, {} rows:\n  `{}`",
            s.buffers(),
            s.calls,
            s.rows,
            s.query.split_whitespace().collect::<Vec<_>>().join(" ")
        );
    }
    let _ = writeln!(out);
    out
}

/// Seeds a fleet beside the measured workers and measures one window.
async fn capture(admin: &str, fleet: u32) -> (Capture, String) {
    let (name, url) = create_db(admin, true).await;
    let mut conn = connect(&url).await;
    let pool = build_test_pool(&url);
    let fleet_workers: Vec<Arc<Worker>> = (0..MEASURED)
        .map(|i| worker(&format!("measured-{i}")))
        .collect();
    let handles: Vec<_> = fleet_workers
        .iter()
        .map(|w| spawn_test_worker(Arc::clone(w), pool.clone()))
        .collect();
    let key = measured_key(&mut conn).await;
    run_sql(&mut conn, SEED, fleet, &key).await;
    let seeded = int(
        &mut conn,
        "SELECT COUNT(*) AS v FROM harvest_worker_task_stats",
    )
    .await;
    let heap_bytes = int(
        &mut conn,
        "SELECT pg_relation_size('harvest_worker_task_stats') AS v",
    )
    .await;
    let toast_bytes = int(
        &mut conn,
        "SELECT COALESCE(pg_relation_size(reltoastrelid), 0) AS v FROM pg_class \
         WHERE relname = 'harvest_worker_task_stats'",
    )
    .await;
    let avg_key_bytes = int(
        &mut conn,
        "SELECT AVG(octet_length(cohort))::bigint AS v FROM harvest_worker_task_stats",
    )
    .await;
    reset_statements(&mut conn).await;
    let before = table_counters(&mut conn).await;
    let started = Instant::now();
    let mut waits = BTreeMap::new();
    let mut first_prune = None;
    while started.elapsed() < WINDOW {
        tokio::time::sleep(Duration::from_secs(1)).await;
        sample_waits(&mut conn, &mut waits).await;
        if first_prune.is_none() {
            let pruned = int(
                &mut conn,
                "SELECT COALESCE(SUM(calls), 0)::bigint AS v FROM pg_stat_statements \
                 WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
                   AND query LIKE 'DELETE FROM harvest_worker_task_stats%'",
            )
            .await;
            if pruned > 0 {
                first_prune = Some(remaining(&mut conn, "worker_id NOT LIKE 'measured-%'").await);
            }
        }
    }
    for w in &fleet_workers {
        w.shutdown();
    }
    for handle in handles {
        tokio::time::timeout(Duration::from_secs(30), handle)
            .await
            .expect("the worker stops within 30 s")
            .expect("the worker task ends cleanly");
    }
    drop(pool);
    let table = table_counters(&mut conn).await.minus(before);
    let rows = statements(&mut conn).await;
    assert!(
        rows.iter().any(Statement::is_prune),
        "the window ran the prune"
    );
    let plans = explain_prune(&mut conn).await;
    drop(conn);
    drop_db(admin, &name).await;
    let capture = Capture {
        fleet,
        rows,
        table,
        waits,
        seeded,
        after_first_prune: first_prune.expect("a prune ran in the window"),
        heap_bytes,
        toast_bytes,
        avg_key_bytes,
    };
    (capture, plans)
}

/// A cohort key for the edge rows. Only its `peer_stale_secs` matters.
const EDGE_KEY: &str = r#"{"build_id":"edge","peer_stale_secs":120}"#;

/// One prune of the edge rows: the fallback, the rows deleted and the ids
/// kept, in byte order.
type EdgeRun = (i64, usize, Vec<String>);

/// The prune on the edge rows of `equivalence.sql`, once per fallback.
async fn edge_equivalence(admin: &str) -> Vec<EdgeRun> {
    let mut runs = Vec::new();
    for fallback in [FAST_FALLBACK, 7_200] {
        let (name, url) = create_db(admin, false).await;
        let mut conn = connect(&url).await;
        run_sql(&mut conn, EDGES, 0, EDGE_KEY).await;
        let deleted = workers::prune_worker_task_stats(&mut conn, fallback)
            .await
            .expect("prune");
        let ids: Vec<Text> = diesel::sql_query(format!(
            "{HARNESS}SELECT worker_id AS v FROM harvest_worker_task_stats \
             ORDER BY worker_id COLLATE \"C\""
        ))
        .load(&mut conn)
        .await
        .expect("read the remaining rows");
        runs.push((fallback, deleted, ids.into_iter().map(|t| t.v).collect()));
        drop(conn);
        drop_db(admin, &name).await;
    }
    runs
}

/// The prune keeps each row for the longer of the retention and its cohort's
/// freshness window, with the clamps and the fallback.
#[tokio::test]
async fn the_prune_keeps_each_row_for_its_own_window() {
    let (admin, _guard) = setup_server().await;
    let runs = edge_equivalence(&admin).await;
    let kept = |ids: &[&str]| ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    assert_eq!(
        runs,
        vec![
            (
                FAST_FALLBACK,
                12,
                kept(&[
                    "fast-0",
                    "fast-3598",
                    "huge-31535998",
                    "huge-3602",
                    "negative-3598",
                    "slow-10798",
                    "slow-3602",
                ])
            ),
            (
                7_200,
                8,
                kept(&[
                    "empty-3602",
                    "fast-0",
                    "fast-3598",
                    "huge-31535998",
                    "huge-3602",
                    "negative-3598",
                    "nofield-3602",
                    "nofield-7198",
                    "null-3602",
                    "slow-10798",
                    "slow-3602",
                ])
            ),
        ]
    );
}

/// Measures every fleet size and writes `<PERF_LABEL>-capture.txt` and
/// `<PERF_LABEL>-explain.txt` under [`ARTIFACT_DIR`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "evidence capture: run by hand, see docs/performance-worker-task-stats-prune.md"]
async fn zz_capture_worker_task_stats_prune_evidence() {
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "capture".to_string());
    let (admin, _guard) = setup_server().await;
    let mut out = String::new();
    let _ = writeln!(out, "# Task-stats prune capture: {label}\n");
    let _ = writeln!(
        out,
        "{MEASURED} measured workers, 5 s heartbeat, 500 ms poll, {} s window, \
         pg_stat_statements per database.\n",
        WINDOW.as_secs()
    );
    let mut plans = String::new();
    for fleet in FLEETS {
        let (capture, plan) = capture(&admin, fleet).await;
        out.push_str(&report(&capture));
        let _ = writeln!(plans, "## Live fleet of {fleet}\n");
        plans.push_str(&plan);
    }
    let _ = writeln!(out, "## Edge rows\n");
    for (fallback, deleted, kept) in edge_equivalence(&admin).await {
        let _ = writeln!(
            out,
            "fallback {fallback}: deleted {deleted}, kept {}: {}",
            kept.len(),
            kept.join(", ")
        );
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
