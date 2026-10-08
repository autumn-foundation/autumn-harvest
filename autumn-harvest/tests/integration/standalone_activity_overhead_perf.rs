#![cfg(feature = "db")]
//! The overhead of a one-step workflow against a bare activity (issue #1987).
//!
//! Harvest has no standalone-activity start path. A user wraps one activity
//! in a one-step workflow. This harness measures what that wrapper costs.
//! `DESIGN-1987.md` fixes the arms and the decision rule.
//!
//! * Arm A: a one-step workflow that runs a regular activity.
//! * Arm B: a one-step workflow that runs a local activity.
//! * Arm C: the floor. It inserts one task row with no workflow, claims it
//!   and completes it through the `queue` API. No worker path runs such a
//!   row, so arm C is the least that a durable job can cost here.
//!
//! The structural tests assert events, task rows and claims per job. Those
//! counts are exact. The ignored capture measures rows written and WAL
//! bytes, which are the deciders. An idle claim poll changes no row and
//! writes no WAL, so the deciders do not depend on poll timing. Statement
//! calls and latency are context only. An idle-worker control measures the
//! noise in them.
//!
//! Each arm gets a fresh, migrated database. `HARVEST_TEST_DATABASE_URL`
//! is an admin URL. Without it the harness starts a Postgres container.

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
use autumn_harvest::worker::{HandlerRegistry, Worker};
use autumn_harvest::{StartSource, StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

use super::integration_e2e::{build_test_pool, runtime_config, spawn_test_worker};
use super::standalone_activity_support::{
    ARM_A, ARM_B, ARM_C, ARTIFACT_DIR, Arm, BUILD_LINE, JOBS_PER_ARM, VERDICT_BUILD,
    VERDICT_DOCUMENT,
};

const QUEUE: &str = "standalone-overhead";
const JOB: &str = "standalone_job";
const JOB_LOCAL: &str = "standalone_job_local";
const WF_REGULAR: &str = "one_step_regular";
const WF_LOCAL: &str = "one_step_local";
const BARE_WORKER: &str = "standalone-bare";

/// Jobs per arm in the structural tests. Small, because the counts are exact.
const STRUCTURAL_JOBS: usize = 3;

/// The same input for every arm.
fn job_input(n: usize) -> Value {
    json!({ "job": n, "payload": "standalone-activity-overhead" })
}

/// The same body for every arm: return the input.
fn job_body(input: Value) -> Result<Value, String> {
    Ok(input)
}

// ── handlers ─────────────────────────────────────────────────────────────────

type BoxFut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

fn job_handler(_ctx: &ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move { job_body(input) })
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

/// Creates and migrates a uniquely-named database. Returns its name and URL.
async fn create_fresh_db(admin_url: &str, prefix: &str) -> (String, String) {
    let name = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let mut admin = connect(admin_url).await;
    admin
        .batch_execute(&format!("CREATE DATABASE \"{name}\""))
        .await
        .expect("create the arm database");
    let (base, _) = admin_url.rsplit_once('/').expect("the URL has a db segment");
    let url = format!("{base}/{name}");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply the migration bundle");
    let _ = conn
        .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await;
    (name, url)
}

#[derive(diesel::QueryableByName)]
struct Int {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    v: i64,
}

async fn int(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
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

async fn wal_lsn(conn: &mut AsyncPgConnection) -> i64 {
    int(
        conn,
        "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint AS v",
    )
    .await
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
           AND query NOT ILIKE '%pg_current_wal_lsn%' \
         ORDER BY calls DESC, query",
    )
    .load(conn)
    .await
    .expect("read pg_stat_statements")
}

// ── measurement ──────────────────────────────────────────────────────────────

/// What one arm costs, summed over all its jobs.
#[derive(Debug)]
struct Measurement {
    jobs: usize,
    /// Per-job events, task rows and claims. Each job must match the first.
    events: Vec<i64>,
    task_rows: Vec<i64>,
    claims: Vec<i64>,
    writes: Vec<TableWrites>,
    wal_bytes: i64,
    statements: Vec<Statement>,
    latency_ms: Vec<f64>,
    window: Duration,
}

impl Measurement {
    fn rows_written(&self) -> i64 {
        self.writes.iter().map(|w| w.ins + w.upd + w.del).sum()
    }

    fn calls(&self) -> i64 {
        self.statements.iter().map(|s| s.calls).sum()
    }

    fn per_job(&self, total: i64) -> f64 {
        total as f64 / self.jobs as f64
    }

    fn p50_latency_ms(&self) -> f64 {
        let mut v = self.latency_ms.clone();
        v.sort_by(f64::total_cmp);
        v.get(v.len() / 2).copied().unwrap_or(0.0)
    }

    /// Asserts that every job has the structure `arm` states.
    fn assert_structure(&self, arm: Arm) {
        assert_eq!(self.events.len(), self.jobs, "arm {}: one row per job", arm.label);
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
    #[diesel(sql_type = diesel::sql_types::Double)]
    latency_ms: f64,
}

/// Per-job structure for the workflow arms, one row per execution.
async fn workflow_jobs(conn: &mut AsyncPgConnection) -> Vec<JobRow> {
    diesel::sql_query(
        "SELECT \
             (SELECT COUNT(*) FROM harvest_events ev WHERE ev.workflow_exec_id = e.id) AS events, \
             (SELECT COUNT(*) FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id) \
                 AS task_rows, \
             (SELECT COALESCE(SUM(t.attempt), 0)::bigint FROM harvest_task_queue t \
              WHERE t.workflow_exec_id = e.id) AS claims, \
             (EXTRACT(EPOCH FROM (e.completed_at - e.created_at)) * 1000)::float8 AS latency_ms \
         FROM harvest_workflow_executions e ORDER BY e.created_at",
    )
    .load(conn)
    .await
    .expect("read the per-job structure")
}

/// Per-job structure for the bare arm, one row per task.
async fn bare_jobs(conn: &mut AsyncPgConnection) -> Vec<JobRow> {
    diesel::sql_query(
        "SELECT 0::bigint AS events, 1::bigint AS task_rows, attempt::bigint AS claims, \
             (EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000)::float8 AS latency_ms \
         FROM harvest_task_queue WHERE state = 'COMPLETED' ORDER BY created_at",
    )
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
    Arc::new(Worker::new(config, registry()).expect("the worker builds"))
}

/// Gives backends time to publish their table counters after they exit.
async fn settle_stats() {
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

/// Opens the measured window on a fresh database.
async fn open_window(conn: &mut AsyncPgConnection) -> (Vec<TableWrites>, i64, Instant) {
    reset_statements(conn).await;
    (table_writes(conn).await, wal_lsn(conn).await, Instant::now())
}

/// Closes the measured window and collects the figures.
async fn close_window(
    conn: &mut AsyncPgConnection,
    opened: (Vec<TableWrites>, i64, Instant),
    jobs: Vec<JobRow>,
) -> Measurement {
    let window = opened.2.elapsed();
    settle_stats().await;
    let wal_bytes = wal_lsn(conn).await - opened.1;
    let writes = writes_delta(&opened.0, &table_writes(conn).await);
    let statements = statements(conn).await;
    Measurement {
        jobs: jobs.len(),
        events: jobs.iter().map(|j| j.events).collect(),
        task_rows: jobs.iter().map(|j| j.task_rows).collect(),
        claims: jobs.iter().map(|j| j.claims).collect(),
        writes,
        wal_bytes,
        statements,
        latency_ms: jobs.iter().map(|j| j.latency_ms).collect(),
        window,
    }
}

/// Arms A and B: start `jobs` one-step workflows and let a worker drain them.
async fn measure_workflow_arm(
    admin_url: &str,
    workflow_name: &'static str,
    jobs: usize,
) -> Measurement {
    let (_, url) = create_fresh_db(admin_url, workflow_name).await;
    let mut conn = connect(&url).await;
    let pool = build_test_pool(&url);
    let handle = spawn_test_worker(worker(&format!("{workflow_name}-worker")), pool.clone());
    // Let the worker finish its start-up writes before the window opens.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let opened = open_window(&mut conn).await;
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
    handle.abort();
    let _ = handle.await;
    drop(pool);
    let rows = workflow_jobs(&mut conn).await;
    close_window(&mut conn, opened, rows).await
}

/// Arm C: enqueue `jobs` bare task rows, then claim, run and complete each.
async fn measure_bare_arm(admin_url: &str, jobs: usize) -> Measurement {
    let (_, url) = create_fresh_db(admin_url, "bare_floor").await;
    let mut conn = connect(&url).await;
    let queues = vec![QUEUE.to_string()];

    let opened = open_window(&mut conn).await;
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
        let output = job_body(item.input.clone()).expect("the job body succeeds");
        let claim = TaskClaim::new(item.id, BARE_WORKER, item.attempt);
        queue::complete_claimed_task(&mut conn, &claim, output)
            .await
            .expect("complete the bare task");
    }
    let rows = bare_jobs(&mut conn).await;
    close_window(&mut conn, opened, rows).await
}

/// The control: a worker with no work, open for `window`.
async fn measure_idle_control(admin_url: &str, window: Duration) -> Measurement {
    let (_, url) = create_fresh_db(admin_url, "idle_control").await;
    let mut conn = connect(&url).await;
    let pool = build_test_pool(&url);
    let handle = spawn_test_worker(worker("idle-control-worker"), pool.clone());
    tokio::time::sleep(Duration::from_secs(2)).await;

    let opened = open_window(&mut conn).await;
    tokio::time::sleep(window).await;
    handle.abort();
    let _ = handle.await;
    drop(pool);
    close_window(&mut conn, opened, Vec::new()).await
}

// ── structural tests ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arm_a_structural_counts() {
    let (admin, _guard) = setup_server().await;
    measure_workflow_arm(&admin, WF_REGULAR, STRUCTURAL_JOBS)
        .await
        .assert_structure(ARM_A);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arm_b_structural_counts() {
    let (admin, _guard) = setup_server().await;
    measure_workflow_arm(&admin, WF_LOCAL, STRUCTURAL_JOBS)
        .await
        .assert_structure(ARM_B);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arm_c_structural_counts() {
    let (admin, _guard) = setup_server().await;
    measure_bare_arm(&admin, STRUCTURAL_JOBS)
        .await
        .assert_structure(ARM_C);
}

// ── evidence ─────────────────────────────────────────────────────────────────

fn cost_row(label: &str, m: &Measurement) -> String {
    format!(
        "| {label} | {:.2} | {:.0} | {:.2} | {:.1} |",
        m.per_job(m.rows_written()),
        m.per_job(m.wal_bytes),
        m.per_job(m.calls()),
        m.p50_latency_ms(),
    )
}

fn detail(out: &mut String, label: &str, m: &Measurement) {
    let _ = writeln!(
        out,
        "\n## Arm {label}: {} jobs, window {} ms\n",
        m.jobs,
        m.window.as_millis()
    );
    let _ = writeln!(
        out,
        "rows written {}, WAL bytes {}, calls {}",
        m.rows_written(),
        m.wal_bytes,
        m.calls()
    );
    let _ = writeln!(out, "\n| table | ins | upd | del |\n|---|--:|--:|--:|");
    for w in &m.writes {
        let _ = writeln!(out, "| {} | {} | {} | {} |", w.relname, w.ins, w.upd, w.del);
    }
    let _ = writeln!(out, "\n| calls | rows | statement |\n|--:|--:|---|");
    for s in &m.statements {
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
    let a = measure_workflow_arm(&admin, WF_REGULAR, JOBS_PER_ARM).await;
    let b = measure_workflow_arm(&admin, WF_LOCAL, JOBS_PER_ARM).await;
    let c = measure_bare_arm(&admin, JOBS_PER_ARM).await;
    a.assert_structure(ARM_A);
    b.assert_structure(ARM_B);
    c.assert_structure(ARM_C);
    let idle = measure_idle_control(&admin, a.window.max(b.window)).await;

    let ratio = |x: &Measurement, total: fn(&Measurement) -> i64| {
        x.per_job(total(x)) / c.per_job(total(&c))
    };
    let rows = |m: &Measurement| m.rows_written();
    let wal = |m: &Measurement| m.wal_bytes;
    let verdict = if ratio(&b, rows) <= BUILD_LINE && ratio(&b, wal) <= BUILD_LINE {
        VERDICT_DOCUMENT
    } else {
        VERDICT_BUILD
    };

    let mut out = String::new();
    let _ = writeln!(out, "# Standalone-activity overhead capture (issue #1987)\n");
    let _ = writeln!(
        out,
        "| Arm | Rows written | WAL bytes | Statement calls | p50 latency (ms) |"
    );
    let _ = writeln!(out, "|---|--:|--:|--:|--:|");
    for (label, m) in [("A", &a), ("B", &b), ("C", &c)] {
        let _ = writeln!(out, "{}", cost_row(label, m));
    }
    let _ = writeln!(out, "\n| Ratio | Rows written | WAL bytes |\n|---|--:|--:|");
    for (label, m) in [("A / C", &a), ("B / C", &b)] {
        let _ = writeln!(
            out,
            "| {label} | {:.2}x | {:.2}x |",
            ratio(m, rows),
            ratio(m, wal)
        );
    }
    let _ = writeln!(
        out,
        "\nIdle control over {} ms: rows written {}, WAL bytes {}, calls {}",
        idle.window.as_millis(),
        idle.rows_written(),
        idle.wal_bytes,
        idle.calls()
    );
    let _ = writeln!(out, "\nVerdict at the {BUILD_LINE:.1}x line: {verdict}");
    for (label, m) in [("A", &a), ("B", &b), ("C", &c), ("idle", &idle)] {
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
