#![cfg(feature = "db")]
//! The overhead of a one-step workflow against a bare activity (issue #1987).
//!
//! Harvest has no standalone-activity start path. A user wraps one activity
//! in a one-step workflow. This harness measures what that wrapper costs.
//! `DESIGN-1987.md` fixes the arms and the decision rule.
//!
//! * Arm A: a one-step workflow that runs a regular activity.
//! * Arm B: a one-step workflow that runs a local activity.
//! * Arm C: the bare floor. It inserts one task row with no workflow,
//!   claims it and completes it through the `queue` API. No worker path
//!   runs such a row.
//! * Arm D: the realistic floor (`DESIGN-1987.md` §0.6). It adds a job
//!   record and the handler-start marker to arm C, and writes no events.
//!
//! The structural tests assert events, task rows and claims per job. Those
//! counts are exact, and the tests need only a plain database. The ignored
//! capture also measures the two deciders: rows written and WAL bytes. It
//! needs a superuser and a preloaded `pg_stat_statements`.
//!
//! The harness turns off scanner election and sets a 10-minute worker
//! heartbeat, because both write rows on a timer. It stops the worker and
//! waits for its connections to close before it reads any counter. It
//! counts only WAL records on the arm's own tables. An idle-worker control
//! then writes no row and no WAL. Statement calls are context only, because
//! idle polls add calls. The capture removes the idle call rate from them.
//!
//! Each arm gets a fresh, migrated database, dropped after the arm.
//! `HARVEST_TEST_DATABASE_URL` is an admin URL. Without it the harness
//! starts a Postgres container.

#![allow(clippy::too_many_lines, clippy::cast_precision_loss)]

use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::info::{ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::queue::{self, EnqueueParams, TaskClaim, TaskType};
use autumn_harvest::types::{
    ExecutionId, Priority, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{StartSource, StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

use super::integration_e2e::{build_test_pool, runtime_config, spawn_test_worker};
use super::standalone_activity_support::{
    ARM_A, ARM_B, ARM_C, ARM_D, ARTIFACT_DIR, Arm, BUILD_LINE, JOBS_PER_ARM, VERDICT_BUILD,
    VERDICT_DOCUMENT,
};

const QUEUE: &str = "standalone-overhead";
const JOB: &str = "standalone_job";
const JOB_LOCAL: &str = "standalone_job_local";
const WF_REGULAR: &str = "one_step_regular";
const WF_LOCAL: &str = "one_step_local";
const RECORD_NAME: &str = "standalone_record";
const BARE_WORKER: &str = "standalone-bare";

/// Opens every harness query, so the statement capture can drop them.
/// `pg_stat_statements` keeps the comment in the stored query text. Each
/// tagged query has a shape that no engine query shares.
const HARNESS: &str = "/* harness */ ";

/// Jobs per arm in the structural tests. Small, because the counts are exact.
const STRUCTURAL_JOBS: usize = 3;

/// The same input for every arm.
fn job_input(n: usize) -> Value {
    json!({ "job": n, "payload": "standalone-activity-overhead" })
}

/// The same body for every arm: return the input.
const fn job_body(input: Value) -> Value {
    input
}

// ── handlers ─────────────────────────────────────────────────────────────────

type BoxFut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

fn job_handler(_ctx: &ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(job_body(input)) })
}

fn regular_workflow(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw(JOB, input, QUEUE)
            .await
            .map_err(|e| e.to_string())
    })
}

fn local_workflow(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw(JOB_LOCAL, input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

fn workflow_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "standalone_activity_overhead_perf",
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

fn activity_info(name: &'static str, is_local: bool) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "standalone_activity_overhead_perf",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: if is_local { None } else { Some(QUEUE) },
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: job_handler,
    }
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            workflow_info(WF_REGULAR, regular_workflow),
            workflow_info(WF_LOCAL, local_workflow),
        ],
        vec![activity_info(JOB, false), activity_info(JOB_LOCAL, true)],
    ))
}

// ── database ─────────────────────────────────────────────────────────────────

type DbGuard = Option<ContainerAsync<Postgres>>;

/// Whether an arm also captures WAL and statement statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stats {
    Off,
    On,
}

async fn setup_server() -> (String, DbGuard) {
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
fn arm_url(admin_url: &str, name: &str) -> String {
    let (base, query) = admin_url
        .split_once('?')
        .map_or((admin_url, None), |(b, q)| (b, Some(q)));
    let (prefix, _) = base.rsplit_once('/').expect("the URL has a db segment");
    query.map_or_else(
        || format!("{prefix}/{name}"),
        |q| format!("{prefix}/{name}?{q}"),
    )
}

/// A uniquely-named, migrated database for one arm.
struct ArmDb {
    name: String,
    url: String,
}

async fn create_fresh_db(admin_url: &str, prefix: &str, stats: Stats) -> ArmDb {
    let name = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let mut admin = connect(admin_url).await;
    admin
        .batch_execute(&format!("CREATE DATABASE \"{name}\""))
        .await
        .expect("create the arm database");
    let url = arm_url(admin_url, &name);
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply the migration bundle");
    if stats == Stats::On {
        conn.batch_execute(
            "CREATE EXTENSION IF NOT EXISTS pg_stat_statements; \
             CREATE EXTENSION IF NOT EXISTS pg_walinspect;",
        )
        .await
        .expect("the capture needs a superuser and a preloaded pg_stat_statements");
    }
    ArmDb { name, url }
}

async fn drop_db(admin_url: &str, name: &str) {
    connect(admin_url)
        .await
        .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
        .await
        .expect("drop the arm database");
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

#[derive(diesel::QueryableByName, Debug, Clone, PartialEq, Eq)]
struct TableWrites {
    #[diesel(sql_type = diesel::sql_types::Text)]
    relname: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    ins: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    upd: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    del: i64,
}

async fn table_writes(conn: &mut AsyncPgConnection) -> Vec<TableWrites> {
    diesel::sql_query(
        "SELECT relname::text AS relname, n_tup_ins AS ins, n_tup_upd AS upd, n_tup_del AS del \
         FROM pg_stat_user_tables ORDER BY relname",
    )
    .load(conn)
    .await
    .expect("read pg_stat_user_tables")
}

/// Row writes per table between two snapshots. Tables with no change drop out.
fn writes_delta(before: &[TableWrites], after: &[TableWrites]) -> Vec<TableWrites> {
    after
        .iter()
        .map(|a| {
            let b = before.iter().find(|b| b.relname == a.relname);
            TableWrites {
                relname: a.relname.clone(),
                ins: a.ins - b.map_or(0, |b| b.ins),
                upd: a.upd - b.map_or(0, |b| b.upd),
                del: a.del - b.map_or(0, |b| b.del),
            }
        })
        .filter(|d| d.ins + d.upd + d.del != 0)
        .collect()
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

#[derive(diesel::QueryableByName, Default)]
struct Wal {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    data: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    fpi: i64,
}

/// WAL since `start` that touches this database's `public` relations.
///
/// The cluster WAL also holds autovacuum, catalog upkeep and other
/// databases. A block reference names `tablespace/database/filenode`, so
/// the filter keeps only records on this arm's own tables and indexes.
/// Records with no block reference, such as a commit, drop out. Full-page
/// images depend on checkpoint timing, so the harness counts them
/// separately.
async fn wal_since(conn: &mut AsyncPgConnection, start: &str) -> Wal {
    let end = text(conn, "SELECT pg_current_wal_flush_lsn()::text AS v").await;
    // `pg_get_wal_records_info` fails on a range with no record in it.
    let span = diesel::sql_query(format!(
        "{HARNESS}SELECT ($1::pg_lsn - $2::pg_lsn)::bigint AS v"
    ))
    .bind::<diesel::sql_types::Text, _>(&end)
    .bind::<diesel::sql_types::Text, _>(start)
    .get_result::<Int>(conn)
    .await
    .expect("compare WAL positions")
    .v;
    if span <= 0 {
        return Wal::default();
    }
    diesel::sql_query(format!(
        "{HARNESS}WITH rels AS ( \
             SELECT '/' || (SELECT oid FROM pg_database WHERE datname = current_database()) \
                 || '/' || pg_relation_filenode(c.oid) || '([^0-9]|$)' AS pattern \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND pg_relation_filenode(c.oid) IS NOT NULL) \
         SELECT COALESCE(SUM(r.record_length - r.fpi_length), 0)::bigint AS data, \
                COALESCE(SUM(r.fpi_length), 0)::bigint AS fpi \
         FROM pg_get_wal_records_info($1::pg_lsn, $2::pg_lsn) r \
         WHERE EXISTS (SELECT 1 FROM rels WHERE r.block_ref ~ rels.pattern)"
    ))
    .bind::<diesel::sql_types::Text, _>(start)
    .bind::<diesel::sql_types::Text, _>(&end)
    .get_result(conn)
    .await
    .expect("read the arm's WAL records")
}

async fn reset_statements(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "SELECT pg_stat_statements_reset(0, \
             (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
    .await
    .expect(
        "pg_stat_statements_reset failed: preload pg_stat_statements and use a role that may \
         reset it",
    );
}

#[derive(diesel::QueryableByName, Debug)]
struct Statement {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    rows: i64,
}

async fn statements(conn: &mut AsyncPgConnection) -> Vec<Statement> {
    diesel::sql_query(
        "SELECT query, calls, rows FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND query NOT ILIKE '%pg_stat%' \
           AND query NOT LIKE '%/* harness */%' \
         ORDER BY calls DESC, query",
    )
    .load(conn)
    .await
    .expect("read pg_stat_statements")
}

// ── measurement ──────────────────────────────────────────────────────────────

/// The counters an arm reads when its window closes.
struct Figures {
    writes: Vec<TableWrites>,
    wal: Wal,
    statements: Vec<Statement>,
    window: Duration,
}

/// What one arm costs, summed over all its jobs.
struct Measurement {
    jobs: usize,
    /// Per-job events, task rows and claims, one entry per job.
    events: Vec<i64>,
    task_rows: Vec<i64>,
    claims: Vec<i64>,
    figures: Figures,
}

impl Measurement {
    /// Joins the per-job rows to the figures. Every requested job must
    /// have a row.
    fn new(jobs: usize, figures: Figures, rows: &[JobRow]) -> Self {
        assert_eq!(rows.len(), jobs, "every requested job must leave a row");
        Self {
            jobs,
            events: rows.iter().map(|j| j.events).collect(),
            task_rows: rows.iter().map(|j| j.task_rows).collect(),
            claims: rows.iter().map(|j| j.claims).collect(),
            figures,
        }
    }

    fn rows_written(&self) -> i64 {
        self.figures
            .writes
            .iter()
            .map(|w| w.ins + w.upd + w.del)
            .sum()
    }

    const fn wal_bytes(&self) -> i64 {
        self.figures.wal.data
    }

    fn calls(&self) -> i64 {
        self.figures.statements.iter().map(|s| s.calls).sum()
    }

    fn per_job(&self, total: i64) -> f64 {
        total as f64 / self.jobs as f64
    }

    /// Rows written and WAL bytes per job, without the worker's own writes.
    ///
    /// A stopping worker updates its `harvest_workers` row. That cost is
    /// fixed per stop, not per job. The idle control pays exactly the same
    /// writes, so the arms with a worker subtract them.
    fn deciders(&self, idle: Option<&Self>) -> (f64, f64) {
        let (rows, wal) = idle.map_or((0, 0), |i| (i.rows_written(), i.wal_bytes()));
        (
            self.per_job(self.rows_written() - rows),
            self.per_job(self.wal_bytes() - wal),
        )
    }

    /// Asserts that every job equals the counts that `arm` states.
    fn assert_structure(&self, arm: Arm) {
        for (what, got, want) in [
            ("events", &self.events, arm.events),
            ("task rows", &self.task_rows, arm.task_rows),
            ("claims", &self.claims, arm.claims),
        ] {
            assert!(
                got.iter().all(|&g| g == want),
                "arm {}: every job must have {want} {what}, got {got:?}",
                arm.label
            );
        }
    }
}

#[derive(diesel::QueryableByName)]
struct JobRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    events: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    task_rows: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    claims: i64,
}

/// Per-job structure for the arms with an execution row.
async fn execution_jobs(conn: &mut AsyncPgConnection) -> Vec<JobRow> {
    diesel::sql_query(format!(
        "{HARNESS}SELECT \
             (SELECT COUNT(*) FROM harvest_events ev WHERE ev.workflow_exec_id = e.id) AS events, \
             (SELECT COUNT(*) FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id) \
                 AS task_rows, \
             (SELECT COALESCE(SUM(t.attempt), 0)::bigint FROM harvest_task_queue t \
              WHERE t.workflow_exec_id = e.id) AS claims \
         FROM harvest_workflow_executions e ORDER BY e.created_at"
    ))
    .load(conn)
    .await
    .expect("read the per-job structure")
}

/// Per-job structure for the bare arm. A job is one `activity_id`. Its
/// event count is every event in the database, which must be zero.
async fn bare_jobs(conn: &mut AsyncPgConnection) -> Vec<JobRow> {
    diesel::sql_query(format!(
        "{HARNESS}SELECT (SELECT COUNT(*) FROM harvest_events) AS events, \
             COUNT(*) AS task_rows, SUM(attempt)::bigint AS claims \
         FROM harvest_task_queue GROUP BY activity_id ORDER BY MIN(created_at)"
    ))
    .load(conn)
    .await
    .expect("read the per-task structure")
}

async fn start_one(conn: &mut AsyncPgConnection, workflow_name: &'static str, n: usize) {
    let workflow_id = format!("{workflow_name}-{n}");
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id: &workflow_id,
            exec_id: ExecutionId::new(),
            input: job_input(n).into(),
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
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start the one-step workflow");
}

fn worker(worker_id: &str) -> Arc<Worker> {
    let mut config = runtime_config(worker_id, 4, 4, Duration::from_secs(30));
    config.queues = vec![QUEUE.to_string()];
    // Scanner election and the worker heartbeat write rows on a timer.
    // Those writes are fleet upkeep, not job cost, so the harness keeps
    // them out of the measured window.
    config.scanner.elect = false;
    config.worker_heartbeat_interval = Duration::from_secs(600);
    Arc::new(Worker::new(config, registry()).expect("the worker builds"))
}

/// Stops the worker and waits until its connections close.
///
/// A backend publishes its table counters when it exits. The capture reads
/// the counters only after every worker backend is gone.
async fn stop_worker(
    worker: &Worker,
    handle: tokio::task::JoinHandle<()>,
    pool: DbPool,
    conn: &mut AsyncPgConnection,
) {
    worker.shutdown();
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("the worker stops within 30 s")
        .expect("the worker task ends cleanly");
    drop(pool);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let others = int(
            conn,
            "SELECT COUNT(*) AS v FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid()",
        )
        .await;
        if others == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{others} worker connections stay open after shutdown"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Where a measured window starts.
struct Window {
    writes: Vec<TableWrites>,
    wal_start: Option<String>,
    started: Instant,
}

async fn open_window(conn: &mut AsyncPgConnection, stats: Stats) -> Window {
    let wal_start = if stats == Stats::On {
        reset_statements(conn).await;
        Some(text(conn, "SELECT pg_current_wal_insert_lsn()::text AS v").await)
    } else {
        None
    };
    Window {
        writes: table_writes(conn).await,
        wal_start,
        started: Instant::now(),
    }
}

/// Reads the counters. `window` is the time the jobs took.
async fn close_window(conn: &mut AsyncPgConnection, opened: Window, window: Duration) -> Figures {
    // This backend wrote the bare arms' rows. Publish its own counters now.
    conn.batch_execute("SELECT pg_stat_force_next_flush()")
        .await
        .expect("flush this backend's counters");
    let stats = opened.wal_start.is_some();
    let statements = if stats {
        statements(conn).await
    } else {
        Vec::new()
    };
    let writes = writes_delta(&opened.writes, &table_writes(conn).await);
    let wal = match &opened.wal_start {
        Some(start) => wal_since(conn, start).await,
        None => Wal::default(),
    };
    Figures {
        writes,
        wal,
        statements,
        window,
    }
}

/// Arms A and B: start `jobs` one-step workflows and let a worker drain them.
async fn measure_workflow_arm(
    admin_url: &str,
    workflow_name: &'static str,
    jobs: usize,
    stats: Stats,
) -> Measurement {
    let db = create_fresh_db(admin_url, workflow_name, stats).await;
    let mut conn = connect(&db.url).await;
    let pool = build_test_pool(&db.url);
    let worker = worker(&format!("{workflow_name}-worker"));
    let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
    // Let the worker finish its start-up writes before the window opens.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let opened = open_window(&mut conn, stats).await;
    for n in 0..jobs {
        start_one(&mut conn, workflow_name, n).await;
    }
    let want = i64::try_from(jobs).expect("the job count fits in i64");
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let done = int(
            &mut conn,
            "SELECT COUNT(*) AS v FROM harvest_workflow_executions WHERE state = 'COMPLETED'",
        )
        .await;
        if done == want {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{workflow_name}: only {done} of {jobs} jobs completed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let window = opened.started.elapsed();
    stop_worker(&worker, handle, pool, &mut conn).await;
    let figures = close_window(&mut conn, opened, window).await;
    let rows = execution_jobs(&mut conn).await;
    drop(conn);
    drop_db(admin_url, &db.name).await;
    Measurement::new(jobs, figures, &rows)
}

/// Arm C: enqueue `jobs` bare task rows, then claim, run and complete each.
async fn measure_bare_arm(admin_url: &str, jobs: usize, stats: Stats) -> Measurement {
    let db = create_fresh_db(admin_url, "bare_floor", stats).await;
    let mut conn = connect(&db.url).await;
    let queues = vec![QUEUE.to_string()];

    let opened = open_window(&mut conn, stats).await;
    for n in 0..jobs {
        let mut params = EnqueueParams::new(QUEUE, TaskType::Activity, job_input(n));
        params.activity_name = Some(JOB.to_string());
        params.activity_id = Some(uuid::Uuid::new_v4());
        queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue a bare task");
    }
    for _ in 0..jobs {
        let item = queue::claim_task(&mut conn, &queues, BARE_WORKER, "", None, &[], &[])
            .await
            .expect("claim a bare task")
            .expect("a bare task is pending");
        let output = job_body(item.input.clone());
        let claim = TaskClaim::new(item.id, BARE_WORKER, item.attempt);
        let write = queue::complete_claimed_task(&mut conn, &claim, output)
            .await
            .expect("complete the bare task");
        assert_eq!(
            write,
            queue::ClaimWrite::Applied,
            "the bare claim is current"
        );
    }
    let window = opened.started.elapsed();
    let figures = close_window(&mut conn, opened, window).await;
    let rows = bare_jobs(&mut conn).await;
    drop(conn);
    drop_db(admin_url, &db.name).await;
    Measurement::new(jobs, figures, &rows)
}

/// Arm D, start: the job record and its task row, in one transaction.
async fn start_record_job(conn: &mut AsyncPgConnection, n: usize) {
    conn.transaction::<_, diesel::result::Error, _>(async |conn| {
        #[derive(diesel::QueryableByName)]
        struct Id {
            #[diesel(sql_type = diesel::sql_types::Uuid)]
            id: uuid::Uuid,
        }
        let record: Id = diesel::sql_query(
            "INSERT INTO harvest_workflow_executions \
                     (id, workflow_name, workflow_id, run_id, shard_id, state, input, \
                      queue_name, started_at, created_at) \
                 VALUES (gen_random_uuid(), $1, $2, gen_random_uuid(), 0, 'RUNNING', $3, $4, \
                         NOW(), NOW()) \
                 RETURNING id",
        )
        .bind::<diesel::sql_types::Text, _>(RECORD_NAME)
        .bind::<diesel::sql_types::Text, _>(format!("{RECORD_NAME}-{n}"))
        .bind::<diesel::sql_types::Jsonb, _>(job_input(n))
        .bind::<diesel::sql_types::Text, _>(QUEUE)
        .get_result(conn)
        .await?;
        let mut params = EnqueueParams::new(QUEUE, TaskType::Activity, job_input(n));
        params.workflow_exec_id = Some(record.id);
        params.activity_name = Some(JOB.to_string());
        params.activity_id = Some(uuid::Uuid::new_v4());
        queue::enqueue(conn, &params)
            .await
            .expect("enqueue the record job's task");
        Ok(())
    })
    .await
    .expect("start a record job");
}

/// Arm D, run: claim, mark the handler start, then complete the task and
/// the record in one transaction.
async fn run_record_job(conn: &mut AsyncPgConnection, queues: &[String]) {
    let item = queue::claim_task(conn, queues, BARE_WORKER, "", None, &[], &[])
        .await
        .expect("claim a record task")
        .expect("a record task is pending");
    // The statement that `queue::mark_claim_handler_started` issues.
    let marked = diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET handler_started_attempt = $3, handler_started_at = clock_timestamp() \
         WHERE id = $1 AND worker_id = $2 AND attempt = $3 AND state = 'RUNNING'",
    )
    .bind::<diesel::sql_types::Uuid, _>(item.id)
    .bind::<diesel::sql_types::Text, _>(BARE_WORKER)
    .bind::<diesel::sql_types::Integer, _>(item.attempt)
    .execute(conn)
    .await
    .expect("mark the handler start");
    assert_eq!(marked, 1, "the record claim is current");
    let output = job_body(item.input.clone());
    let record = item.workflow_exec_id.expect("a record task has a record");
    let claim = TaskClaim::new(item.id, BARE_WORKER, item.attempt);
    conn.transaction::<_, diesel::result::Error, _>(async |conn| {
        let write = queue::complete_claimed_task(conn, &claim, output.clone())
            .await
            .expect("complete the record task");
        assert_eq!(
            write,
            queue::ClaimWrite::Applied,
            "the record claim is current"
        );
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
                 SET state = 'COMPLETED', output = $2, completed_at = NOW() WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(record)
        .bind::<diesel::sql_types::Jsonb, _>(output)
        .execute(conn)
        .await?;
        Ok(())
    })
    .await
    .expect("complete a record job");
}

/// Arm D: start `jobs` record jobs, then claim, run and complete each.
async fn measure_record_arm(admin_url: &str, jobs: usize, stats: Stats) -> Measurement {
    let db = create_fresh_db(admin_url, "record_floor", stats).await;
    let mut conn = connect(&db.url).await;
    let queues = vec![QUEUE.to_string()];

    let opened = open_window(&mut conn, stats).await;
    for n in 0..jobs {
        start_record_job(&mut conn, n).await;
    }
    for _ in 0..jobs {
        run_record_job(&mut conn, &queues).await;
    }
    let window = opened.started.elapsed();
    let figures = close_window(&mut conn, opened, window).await;
    let rows = execution_jobs(&mut conn).await;
    drop(conn);
    drop_db(admin_url, &db.name).await;
    Measurement::new(jobs, figures, &rows)
}

/// The control: a worker with no work, open for `window`.
async fn measure_idle_control(admin_url: &str, window: Duration) -> Measurement {
    let db = create_fresh_db(admin_url, "idle_control", Stats::On).await;
    let mut conn = connect(&db.url).await;
    let pool = build_test_pool(&db.url);
    let worker = worker("idle-control-worker");
    let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
    tokio::time::sleep(Duration::from_secs(2)).await;

    let opened = open_window(&mut conn, Stats::On).await;
    tokio::time::sleep(window).await;
    let window = opened.started.elapsed();
    stop_worker(&worker, handle, pool, &mut conn).await;
    let figures = close_window(&mut conn, opened, window).await;
    drop(conn);
    drop_db(admin_url, &db.name).await;
    Measurement::new(0, figures, &[])
}

// ── structural tests ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arm_a_structural_counts() {
    let (admin, _guard) = setup_server().await;
    measure_workflow_arm(&admin, WF_REGULAR, STRUCTURAL_JOBS, Stats::Off)
        .await
        .assert_structure(ARM_A);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arm_b_structural_counts() {
    let (admin, _guard) = setup_server().await;
    measure_workflow_arm(&admin, WF_LOCAL, STRUCTURAL_JOBS, Stats::Off)
        .await
        .assert_structure(ARM_B);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arm_c_structural_counts() {
    let (admin, _guard) = setup_server().await;
    measure_bare_arm(&admin, STRUCTURAL_JOBS, Stats::Off)
        .await
        .assert_structure(ARM_C);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arm_d_structural_counts() {
    let (admin, _guard) = setup_server().await;
    measure_record_arm(&admin, STRUCTURAL_JOBS, Stats::Off)
        .await
        .assert_structure(ARM_D);
}

// ── evidence ─────────────────────────────────────────────────────────────────

/// Calls per job after the idle worker's call rate over the same window.
fn net_calls_per_job(m: &Measurement, idle: &Measurement) -> f64 {
    let idle_rate = idle.calls() as f64 / idle.figures.window.as_secs_f64();
    let net = idle_rate.mul_add(-m.figures.window.as_secs_f64(), m.calls() as f64);
    net / m.jobs as f64
}

fn cost_row(label: &str, m: &Measurement, idle: Option<&Measurement>) -> String {
    let (rows, wal) = m.deciders(idle);
    format!(
        "| {label} | {rows:.2} | {wal:.0} | {:.2} | {:.2} |",
        m.per_job(m.calls()),
        idle.map_or_else(|| m.per_job(m.calls()), |idle| net_calls_per_job(m, idle)),
    )
}

/// The verdict at [`BUILD_LINE`] for the two ratios against a floor.
fn verdict(rows: f64, wal: f64) -> &'static str {
    if rows <= BUILD_LINE && wal <= BUILD_LINE {
        VERDICT_DOCUMENT
    } else {
        VERDICT_BUILD
    }
}

fn detail(out: &mut String, label: &str, m: &Measurement) {
    let f = &m.figures;
    let _ = writeln!(
        out,
        "\n## Arm {label}: {} jobs, window {} ms\n",
        m.jobs,
        f.window.as_millis()
    );
    let _ = writeln!(
        out,
        "rows written {}, WAL bytes {}, full-page image bytes {}, calls {}",
        m.rows_written(),
        f.wal.data,
        f.wal.fpi,
        m.calls()
    );
    let _ = writeln!(out, "\n| table | ins | upd | del |\n|---|--:|--:|--:|");
    for w in &f.writes {
        let _ = writeln!(out, "| {} | {} | {} | {} |", w.relname, w.ins, w.upd, w.del);
    }
    let _ = writeln!(out, "\n| calls | rows | statement |\n|--:|--:|---|");
    for s in &f.statements {
        let query: String = s.query.split_whitespace().collect::<Vec<_>>().join(" ");
        let query: String = query.chars().take(140).collect();
        let _ = writeln!(out, "| {} | {} | `{query}` |", s.calls, s.rows);
    }
}

/// Measures every arm and writes `capture.txt` under [`ARTIFACT_DIR`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "evidence capture: run by hand, see docs/performance-standalone-activity-overhead.md"]
async fn zz_capture_standalone_activity_overhead_evidence() {
    let (admin, _guard) = setup_server().await;
    let a = measure_workflow_arm(&admin, WF_REGULAR, JOBS_PER_ARM, Stats::On).await;
    let b = measure_workflow_arm(&admin, WF_LOCAL, JOBS_PER_ARM, Stats::On).await;
    let c = measure_bare_arm(&admin, JOBS_PER_ARM, Stats::On).await;
    let d = measure_record_arm(&admin, JOBS_PER_ARM, Stats::On).await;
    a.assert_structure(ARM_A);
    b.assert_structure(ARM_B);
    c.assert_structure(ARM_C);
    d.assert_structure(ARM_D);
    let idle = measure_idle_control(&admin, a.figures.window.max(b.figures.window)).await;

    // Arms C and D run no worker, so they have no worker writes to remove.
    let ratio = |x: &Measurement, floor: &Measurement| {
        let (rows, wal) = x.deciders(Some(&idle));
        let (floor_rows, floor_wal) = floor.deciders(None);
        (rows / floor_rows, wal / floor_wal)
    };

    let mut out = String::new();
    let _ = writeln!(
        out,
        "# Standalone-activity overhead capture (issue #1987)\n"
    );
    let _ = writeln!(
        out,
        "| Arm | Rows written | WAL bytes | Statement calls | Calls net of idle |"
    );
    let _ = writeln!(out, "|---|--:|--:|--:|--:|");
    for (label, m, idle) in [
        ("A", &a, Some(&idle)),
        ("B", &b, Some(&idle)),
        ("C", &c, None),
        ("D", &d, None),
    ] {
        let _ = writeln!(out, "{}", cost_row(label, m, idle));
    }
    let _ = writeln!(out, "\n| Ratio | Rows written | WAL bytes |\n|---|--:|--:|");
    for (label, x, floor) in [("A / C", &a, &c), ("B / C", &b, &c), ("B / D", &b, &d)] {
        let (rows, wal) = ratio(x, floor);
        let _ = writeln!(out, "| {label} | {rows:.2}x | {wal:.2}x |");
    }
    let _ = writeln!(
        out,
        "\nIdle control over {} ms: rows written {}, WAL bytes {}, calls {}",
        idle.figures.window.as_millis(),
        idle.rows_written(),
        idle.wal_bytes(),
        idle.calls()
    );
    let against_c = ratio(&b, &c);
    let against_d = ratio(&b, &d);
    let _ = writeln!(
        out,
        "\nVerdict at the {BUILD_LINE:.1}x line against arm C (§0.4): {}",
        verdict(against_c.0, against_c.1)
    );
    let _ = writeln!(
        out,
        "Verdict at the {BUILD_LINE:.1}x line against arm D (§0.6, decides): {}",
        verdict(against_d.0, against_d.1)
    );
    for (label, m) in [("A", &a), ("B", &b), ("C", &c), ("D", &d), ("idle", &idle)] {
        detail(&mut out, label, m);
    }

    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate has a parent")
        .join(ARTIFACT_DIR);
    std::fs::create_dir_all(&dir).expect("create the artifact directory");
    std::fs::write(dir.join("capture.txt"), &out).expect("write the capture");
    println!("{out}");
}
