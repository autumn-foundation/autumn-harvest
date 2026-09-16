// NON-PRODUCTION THROWAWAY APPARATUS. Never build against this. It calls the
// public API of `autumn-harvest`, `autumn-harvest-redis` and
// `autumn-harvest-sqlite` only. It modifies no workspace crate.
//
// It answers assay ledger #10. Three persistence and dispatch modes drain the
// same backlog of the same canonical 3-activity workflow, on one box, from one
// source tree, in one sitting.
//
// The lines, the shape and the repetition plan come from
// `docs/rnd/2026-09-16-cross-mode-throughput-preregistration.md`.
//
// The pool construction and the Redis probe follow
// `docs/assays/apparatus/0008-redis-dispatch-integrated/src/main.rs`. The
// workload constants are ported by value from
// `autumn-harvest/tests/integration/e2e_bench_support.rs`. Ledger #2 failed
// twice on an apparatus that only resembled its control. A port by value is
// what stops that failure here.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::dispatch::DispatchSettings;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{Priority, StartSource, StartWorkflowParams, WorkflowContext};
use autumn_harvest_redis::{RedisDispatch, RedisDispatchConfig};
use autumn_harvest_sqlite::SqliteRuntime;

use diesel::prelude::QueryableByName;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use redis::AsyncCommands;

// ---------------------------------------------------------------------------
// Fixed shape. Ported by value from `e2e_bench_support.rs`.
// ---------------------------------------------------------------------------

/// The one queue every worker serves. `BENCH_QUEUE` in the published harness.
const QUEUE: &str = "default";
/// The one registered workflow type.
const WORKFLOW: &str = "harvest_e2e_bench_wf";

/// The three activities of the canonical workflow.
///
/// `BENCH_ACTIVITIES` in the published harness, copied name for name. The
/// count drives `DISPATCHES_PER_WORKFLOW`, so a fourth entry would silently
/// change the published multiplier this assay reports against.
const ACTIVITIES: [&str; 3] = [
    "harvest_e2e_bench_step_1",
    "harvest_e2e_bench_step_2",
    "harvest_e2e_bench_step_3",
];

/// Task dispatches per completed run. `2 * ACTIVITIES.len() + 1`.
///
/// Four workflow-task claims plus one claim per activity. The published
/// harness derives the same number the same way.
const DISPATCHES_PER_WORKFLOW: usize = 2 * ACTIVITIES.len() + 1;

/// `WORKERS_PER_SHARD` in the published harness.
const WORKERS: usize = 1;
/// `MAX_CONCURRENT_WORKFLOWS` in the published harness.
const WORKFLOW_SLOTS: usize = 8;
/// `MAX_CONCURRENT_ACTIVITIES` in the published harness.
const ACTIVITY_SLOTS: usize = 16;
/// `POOL_SIZE_PER_SHARD` in the published harness.
const POOL_SIZE: usize = 32;
/// `POLL_INTERVAL_MS` in the published harness.
const WORKER_POLL_MS: u64 = 25;

/// Consumer group name, matching the shared default.
const CONSUMER_GROUP: &str = "harvest_workers";
/// Blocking read wait for the dispatch channel, on the Redis arm.
const DISPATCH_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// Reconcile sweep interval, on the Redis arm.
const DISPATCH_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
/// Visibility timeout of a delivered reference.
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(60);
/// Lifetime of a dedupe marker.
const DEDUPE_TTL: Duration = Duration::from_secs(600);

/// A ~40-byte workflow input. The payload is inert.
const INPUT_PAYLOAD: &str = "0123456789abcdef0123456789abcdef";

// ---------------------------------------------------------------------------
// Environment knobs. Every default is the pre-registered value.
// ---------------------------------------------------------------------------

fn env_string(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_string())
}

fn env_usize(key: &str, fallback: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(fallback)
}

struct Settings {
    database_url: String,
    admin_url: String,
    database_name: String,
    redis_url: String,
    sqlite_dir: String,
    workflows: usize,
    reps: usize,
    seeders: usize,
    cap_secs: u64,
    arms: Vec<Arm>,
}

impl Settings {
    fn from_env() -> Self {
        let arms = env_string("ASSAY10_ARMS", "sqlite,postgres,redis_pg")
            .split(',')
            .filter_map(Arm::parse)
            .collect::<Vec<_>>();
        Self {
            database_url: env_string(
                "ASSAY10_DATABASE_URL",
                "postgres://postgres@127.0.0.1:5432/assay10",
            ),
            admin_url: env_string(
                "ASSAY10_ADMIN_URL",
                "postgres://postgres@127.0.0.1:5432/postgres",
            ),
            database_name: env_string("ASSAY10_DB_NAME", "assay10"),
            redis_url: env_string("ASSAY10_REDIS_URL", "redis://127.0.0.1:6379"),
            sqlite_dir: env_string("ASSAY10_SQLITE_DIR", "/tmp/assay10-sqlite"),
            workflows: env_usize("ASSAY10_WORKFLOWS", 2_000),
            reps: env_usize("ASSAY10_REPS", 3),
            seeders: env_usize("ASSAY10_SEEDERS", 16),
            cap_secs: env_usize("ASSAY10_CAP_SECS", 900) as u64,
            arms,
        }
    }
}

// ---------------------------------------------------------------------------
// Arms.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    Sqlite,
    Postgres,
    RedisPg,
}

impl Arm {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "sqlite" => Some(Self::Sqlite),
            "postgres" => Some(Self::Postgres),
            "redis_pg" => Some(Self::RedisPg),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::RedisPg => "redis_pg",
        }
    }

    const fn uses_postgres(self) -> bool {
        matches!(self, Self::Postgres | Self::RedisPg)
    }
}

// ---------------------------------------------------------------------------
// Handler scope.
// ---------------------------------------------------------------------------

/// Counts activity body executions across every arm.
///
/// The pre-registration words the correctness precondition as a side-effect
/// row count. This counter replaces the row, on every arm equally. The reason
/// is parity, and the report records it. The embedded backend runs activity
/// bodies inline on the thread that holds its single writer. A body that wrote
/// a row would therefore contend with the runtime's own writer. Charging the
/// Postgres arms for a write the embedded arm cannot perform would bias the
/// comparison the assay exists to make. The intent of the precondition is
/// unchanged. An exact count still proves no activity ran twice and none was
/// dropped.
static ACTIVITY_RUNS: AtomicU64 = AtomicU64::new(0);

fn counter(value: &AtomicU64) -> u64 {
    AtomicU64::load(value, Ordering::Relaxed)
}

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// The canonical workflow. Three activities, in sequence.
///
/// This one function is the workflow handler on every arm. The replay engine
/// is backend-neutral, so the embedded backend and the Postgres core run
/// identical workflow code. Only persistence and dispatch differ between
/// arms.
fn wf_three_activities(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let mut last = serde_json::Value::Null;
        for activity in ACTIVITIES {
            last = ctx
                .execute_activity_raw(activity, input.clone(), QUEUE)
                .await
                .map_err(|err| err.to_string())?;
        }
        Ok(last)
    })
}

/// An inert activity body for the Postgres arms.
///
/// It performs no input or output. The counter is the correctness ledger.
fn act_inert(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ACTIVITY_RUNS.fetch_add(1, Ordering::Relaxed);
        Ok(serde_json::Value::Null)
    })
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: WORKFLOW,
        module: "assay10",
        handler: wf_three_activities,
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

fn activity_info(name: &'static str) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "assay10",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(30)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(QUEUE),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: act_inert,
    }
}

fn registry() -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::new(NoOpMetrics) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![workflow_info()],
        ACTIVITIES.iter().map(|name| activity_info(name)).collect(),
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

// ---------------------------------------------------------------------------
// Postgres helpers.
// ---------------------------------------------------------------------------

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

/// Read one live Postgres setting.
///
/// The embedded backend hard-codes `PRAGMA synchronous = FULL`, so it fsyncs
/// on every commit and no caller can turn that off. The Postgres arms fsync
/// only when the server says so. The two arms are therefore comparable on
/// durability only when these settings say they are. Recording them per run
/// stops a later reader from attributing a durability gap to an engine.
async fn pg_setting(conn: &mut AsyncPgConnection, name: &str) -> String {
    diesel::sql_query(format!("SELECT current_setting('{name}') AS value"))
        .get_result::<TextRow>(conn)
        .await
        .map(|row| row.value)
        .unwrap_or_else(|_| "unknown".to_string())
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("postgres should accept a connection")
}

fn build_pool(url: &str, size: usize) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(size)
        .build()
        .expect("pool should build")
}

/// Drop and recreate the assay database, then apply the engine schema.
///
/// Every run starts on an empty database. A carried-over backlog or carried
/// over planner statistics would make one run's number depend on the run
/// before it.
async fn reset_database(settings: &Settings) {
    let mut admin = connect(&settings.admin_url).await;
    let name = &settings.database_name;
    admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .await
        .expect("the assay database should drop");
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .expect("the assay database should be created");
    drop(admin);

    let mut conn = connect(&settings.database_url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("the harvest schema should apply");
}

async fn scalar(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .get_result::<CountRow>(conn)
        .await
        .expect("count query")
        .value
}

async fn completed_executions(conn: &mut AsyncPgConnection) -> i64 {
    scalar(
        conn,
        "SELECT count(*)::bigint AS value FROM harvest_workflow_executions \
         WHERE state = 'COMPLETED'",
    )
    .await
}

// ---------------------------------------------------------------------------
// Redis probe.
// ---------------------------------------------------------------------------

/// What the dispatch key space still holds after a drain.
///
/// All three counts must be zero for the pre-registered Redis precondition.
#[derive(Clone, Copy)]
struct Residue {
    entries: i64,
    pending: i64,
    markers: i64,
}

impl Residue {
    const fn is_drained(self) -> bool {
        self.entries == 0 && self.pending == 0 && self.markers == 0
    }
}

/// Reads the dispatch key space so a run can prove the channel drained.
struct RedisProbe {
    client: redis::Client,
    prefix: String,
}

impl RedisProbe {
    async fn connect(url: &str, prefix: &str) -> Self {
        Self {
            client: redis::Client::open(url).expect("redis client should open"),
            prefix: prefix.to_string(),
        }
    }

    /// Delete only the keys under this run's own prefix. Never `FLUSHALL`.
    async fn clear(&self) {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .expect("redis connection");
        let keys: Vec<String> = conn
            .keys(format!("{}*", self.prefix))
            .await
            .unwrap_or_default();
        for key in keys {
            let _: i64 = conn.del(&key).await.unwrap_or(0);
        }
    }

    /// Count stream entries, pending entries and dedupe markers after a drain.
    ///
    /// The pre-registration requires an empty stream, an empty PEL **and** an
    /// empty marker set. An earlier version counted only stream keys, so a
    /// leaked marker whose stream entry was acknowledged would have graded as
    /// correct. Found by review on PR #1617.
    async fn residue(&self) -> Residue {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .expect("redis connection");
        let keys: Vec<String> = conn
            .keys(format!("{}*", self.prefix))
            .await
            .unwrap_or_default();
        let mut entries = 0_i64;
        let mut pending = 0_i64;
        let mut markers = 0_i64;
        let marker_prefix = format!("{}:dispatch:marker:", self.prefix);
        for key in keys {
            if key.starts_with(&marker_prefix) {
                markers += 1;
                continue;
            }
            let kind: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            if kind != "stream" {
                continue;
            }
            entries += redis::cmd("XLEN")
                .arg(&key)
                .query_async::<i64>(&mut conn)
                .await
                .unwrap_or(0);
            let summary: redis::Value = redis::cmd("XPENDING")
                .arg(&key)
                .arg(CONSUMER_GROUP)
                .query_async(&mut conn)
                .await
                .unwrap_or(redis::Value::Nil);
            // Match on a slice pattern rather than calling `first`. Diesel's
            // `RunQueryDsl` is in scope here and its own `first` shadows the
            // slice method on this `Vec`. Same hazard the sibling apparatus
            // documents for `load`.
            if let redis::Value::Array(items) = summary
                && let [redis::Value::Int(count), ..] = items.as_slice()
            {
                pending += *count;
            }
        }
        Residue { entries, pending, markers }
    }
}

// ---------------------------------------------------------------------------
// Worker pool.
// ---------------------------------------------------------------------------

fn runtime_config(worker_id: &str) -> WorkerRuntimeConfig {
    let mut config: WorkerRuntimeConfig = WorkerConfig::default().with_queues([QUEUE]).into();
    config.worker_id = worker_id.to_string();
    config.poll_interval = Duration::from_millis(WORKER_POLL_MS);
    config.shutdown_timeout = Duration::from_secs(10);
    config.worker_heartbeat_interval = Duration::from_secs(5);
    config.shard_assignments = vec![ShardId::new(0)];
    config.max_concurrent_workflows = WORKFLOW_SLOTS;
    config.max_concurrent_activities = ACTIVITY_SLOTS;
    config
}

struct Pool {
    workers: Vec<Arc<Worker>>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Pool {
    fn start(pool: &DbPool, run: &str) -> Self {
        let registry = registry();
        let mut workers = Vec::with_capacity(WORKERS);
        let mut handles = Vec::with_capacity(WORKERS);
        for index in 0..WORKERS {
            let worker = Arc::new(
                Worker::new(
                    runtime_config(&format!("{run}-w{index}")),
                    Arc::clone(&registry),
                )
                .expect("worker should build"),
            );
            let pool = pool.clone();
            let running = Arc::clone(&worker);
            handles.push(tokio::spawn(async move { running.run(&pool).await }));
            workers.push(worker);
        }
        Self { workers, handles }
    }

    async fn stop(self) {
        for worker in &self.workers {
            worker.shutdown();
        }
        for handle in self.handles {
            let _ = tokio::time::timeout(Duration::from_secs(20), handle).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Workflow start.
// ---------------------------------------------------------------------------

fn workflow_input() -> serde_json::Value {
    serde_json::json!({ "p": INPUT_PAYLOAD })
}

async fn start_one(conn: &mut AsyncPgConnection, workflow_id: &str) -> bool {
    autumn_harvest::start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: WORKFLOW,
            workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: workflow_input(),
            parent_id: None,
            queue_name: QUEUE,
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
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
    .is_ok()
}

/// Seed `count` workflow starts through the public start API.
///
/// The starts run on `seeders` connections in parallel. The seed phase is not
/// part of any measured window.
async fn seed(url: &str, run: &str, count: usize, seeders: usize) -> usize {
    let mut set = tokio::task::JoinSet::new();
    let lanes = seeders.max(1);
    for lane in 0..lanes {
        let url = url.to_string();
        let run = run.to_string();
        set.spawn(async move {
            let mut conn = connect(&url).await;
            let mut started_here = 0_usize;
            let mut index = lane;
            while index < count {
                if start_one(&mut conn, &format!("{run}-{index}")).await {
                    started_here += 1;
                }
                index += lanes;
            }
            started_here
        });
    }
    let mut total = 0;
    while let Some(result) = set.join_next().await {
        total += result.expect("seed lane");
    }
    total
}

// ---------------------------------------------------------------------------
// One repetition.
// ---------------------------------------------------------------------------

struct RepOutcome {
    workflows_per_sec: f64,
    elapsed_secs: f64,
    completed: i64,
    activity_runs: u64,
    residue: Option<Residue>,
    truncated: bool,
}

impl RepOutcome {
    /// Grade the pre-registered correctness precondition for this repetition.
    fn correct(&self, expected: usize) -> bool {
        let expected_activities = expected as u64 * ACTIVITIES.len() as u64;
        !self.truncated
            && self.completed == expected as i64
            && self.activity_runs == expected_activities
            && self.residue.is_none_or(Residue::is_drained)
    }
}

/// Drain one backlog on a Postgres-backed arm.
async fn run_postgres_arm(settings: &Settings, arm: Arm, rep: usize) -> RepOutcome {
    let run = format!("a10-{}-{rep}", arm.as_str());
    reset_database(settings).await;
    ACTIVITY_RUNS.store(0, Ordering::Relaxed);

    // Install the dispatch channel BEFORE seeding, never after.
    //
    // `queue::enqueue` publishes a hint only when a channel is installed
    // (`queue.rs`, guarded on `dispatch::is_installed`). A backlog seeded
    // first therefore carries no hint at all, and the worker can discover it
    // only through the reconcile sweep. That sweep is capped at
    // `DEFAULT_DISPATCH_RECONCILE_BATCH` rows per `reconcile_interval`, which
    // is 1000 rows per second at this configuration. The Redis arm would then
    // measure a forced recovery path bounded by that cap, not the dispatch
    // path L2 asks about. Found by review on PR #1617.
    autumn_harvest::dispatch::uninstall();
    let probe = if arm == Arm::RedisPg {
        let prefix = format!("assay10:{run}");
        let dispatch = RedisDispatch::connect(
            &settings.redis_url,
            RedisDispatchConfig {
                key_prefix: prefix.clone(),
                consumer_group: CONSUMER_GROUP.to_string(),
                visibility_timeout: VISIBILITY_TIMEOUT,
                dedupe_ttl: DEDUPE_TTL,
            },
        )
        .await
        .expect("redis dispatch should connect");
        let probe = RedisProbe::connect(&settings.redis_url, &prefix).await;
        probe.clear().await;
        autumn_harvest::dispatch::install(
            Arc::new(dispatch),
            DispatchSettings {
                poll_interval: DISPATCH_POLL_INTERVAL,
                reconcile_interval: DISPATCH_RECONCILE_INTERVAL,
                ..Default::default()
            },
        );
        Some(probe)
    } else {
        None
    };

    let seeded = seed(
        &settings.database_url,
        &run,
        settings.workflows,
        settings.seeders,
    )
    .await;
    assert_eq!(seeded, settings.workflows, "every start should be accepted");

    let pool = build_pool(&settings.database_url, POOL_SIZE);
    let mut conn = connect(&settings.database_url).await;

    let started = Instant::now();
    let workers = Pool::start(&pool, &run);

    let mut truncated = false;
    loop {
        if completed_executions(&mut conn).await >= settings.workflows as i64 {
            break;
        }
        if started.elapsed().as_secs() >= settings.cap_secs {
            truncated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = started.elapsed().as_secs_f64();

    workers.stop().await;
    autumn_harvest::dispatch::uninstall();

    let completed = completed_executions(&mut conn).await;
    let residue = match &probe {
        Some(probe) => {
            let residue = probe.residue().await;
            probe.clear().await;
            Some(residue)
        }
        None => None,
    };

    RepOutcome {
        workflows_per_sec: if elapsed > 0.0 {
            completed as f64 / elapsed
        } else {
            0.0
        },
        elapsed_secs: elapsed,
        completed,
        activity_runs: counter(&ACTIVITY_RUNS),
        residue,
        truncated,
    }
}

/// Drain one backlog on the embedded arm.
///
/// The embedded backend has no worker pool and no claim loop. The caller
/// drives it. `run_until_idle` advances every execution until the fleet
/// quiesces, which is the drive model `docs/sqlite-backend.md` documents.
async fn run_sqlite_arm(settings: &Settings, rep: usize) -> RepOutcome {
    let dir = std::path::Path::new(&settings.sqlite_dir);
    std::fs::create_dir_all(dir).expect("the sqlite directory should be created");
    let path = dir.join(format!("assay10-rep{rep}.db"));
    let _ = std::fs::remove_file(&path);

    ACTIVITY_RUNS.store(0, Ordering::Relaxed);
    let mut runtime = SqliteRuntime::open(&path).expect("the sqlite runtime should open");
    runtime.register_workflow(&workflow_info());
    for name in ACTIVITIES {
        runtime.register_activity(&activity_info(name), |_input| {
            ACTIVITY_RUNS.fetch_add(1, Ordering::Relaxed);
            Ok(serde_json::Value::Null)
        });
    }

    let mut execs = Vec::with_capacity(settings.workflows);
    for _ in 0..settings.workflows {
        execs.push(
            runtime
                .start_workflow(WORKFLOW, workflow_input())
                .expect("the start should be accepted"),
        );
    }

    let started = Instant::now();
    let mut truncated = false;
    loop {
        runtime
            .run_until_idle()
            .await
            .expect("the fleet should drive without error");
        let done = execs
            .iter()
            .filter(|exec| {
                matches!(
                    runtime.outcome(**exec),
                    Ok(autumn_harvest_sqlite::ExecutionOutcome::Completed(_))
                )
            })
            .count();
        if done >= settings.workflows {
            break;
        }
        if started.elapsed().as_secs() >= settings.cap_secs {
            truncated = true;
            break;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();

    let completed = execs
        .iter()
        .filter(|exec| {
            matches!(
                runtime.outcome(**exec),
                Ok(autumn_harvest_sqlite::ExecutionOutcome::Completed(_))
            )
        })
        .count() as i64;

    RepOutcome {
        workflows_per_sec: if elapsed > 0.0 {
            completed as f64 / elapsed
        } else {
            0.0
        },
        elapsed_secs: elapsed,
        completed,
        activity_runs: counter(&ACTIVITY_RUNS),
        residue: None,
        truncated,
    }
}

// ---------------------------------------------------------------------------
// Reporting.
// ---------------------------------------------------------------------------

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

#[tokio::main]
async fn main() {
    let settings = Settings::from_env();

    println!("# Assay #10 — cross-mode throughput\n");
    println!(
        "Workflow `{WORKFLOW}`, {} activities, {DISPATCHES_PER_WORKFLOW} dispatches per run.",
        ACTIVITIES.len()
    );
    println!(
        "Backlog {} workflows, {} reps per arm, cap {} s.\n",
        settings.workflows, settings.reps, settings.cap_secs
    );
    println!(
        "Pool: {WORKERS} worker, {WORKFLOW_SLOTS} workflow slots, {ACTIVITY_SLOTS} activity \
         slots, {POOL_SIZE} connections, {WORKER_POLL_MS} ms poll.\n"
    );

    {
        let mut conn = connect(&settings.admin_url).await;
        println!(
            "Postgres durability this run: `fsync = {}`, `synchronous_commit = {}`.",
            pg_setting(&mut conn, "fsync").await,
            pg_setting(&mut conn, "synchronous_commit").await
        );
        println!(
            "Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`, \
             which fsyncs on every commit and cannot be turned off by a caller.\n"
        );
    }

    let mut summary: Vec<(Arm, Vec<f64>, bool)> = Vec::new();

    for arm in settings.arms.clone() {
        println!("## arm `{}`\n", arm.as_str());
        let mut rates = Vec::new();
        let mut all_correct = true;
        for rep in 0..settings.reps {
            let outcome = if arm.uses_postgres() {
                run_postgres_arm(&settings, arm, rep).await
            } else {
                run_sqlite_arm(&settings, rep).await
            };
            let correct = outcome.correct(settings.workflows);
            all_correct &= correct;
            println!(
                "rep {rep}: {:.2} workflows/sec ({} completed in {:.2} s, {} activity runs, \
                 correctness {}{}{})",
                outcome.workflows_per_sec,
                outcome.completed,
                outcome.elapsed_secs,
                outcome.activity_runs,
                if correct { "PASS" } else { "FAIL" },
                if outcome.truncated { ", TRUNCATED" } else { "" },
                match outcome.residue {
                    Some(r) => format!(
                        ", residue entries={} pending={} markers={}",
                        r.entries, r.pending, r.markers
                    ),
                    None => String::new(),
                }
            );
            if correct {
                rates.push(outcome.workflows_per_sec);
            }
        }
        let arm_mean = mean(&rates);
        println!(
            "\n**mean {:.2} workflows/sec** over {} valid rep(s); \
             per-dispatch rate {:.2}/sec.\n",
            arm_mean,
            rates.len(),
            arm_mean * DISPATCHES_PER_WORKFLOW as f64
        );
        summary.push((arm, rates, all_correct));
    }

    println!("## summary\n");
    println!("| arm | mean workflows/sec | valid reps | correctness |");
    println!("|:--|--:|--:|:--|");
    for (arm, rates, correct) in &summary {
        println!(
            "| `{}` | {:.2} | {} | {} |",
            arm.as_str(),
            mean(rates),
            rates.len(),
            if *correct { "PASS" } else { "FAIL" }
        );
    }

    let find = |want: Arm| {
        summary
            .iter()
            .find(|(arm, _, _)| *arm == want)
            .map(|(_, rates, _)| mean(rates))
    };
    println!("\n## pre-registered lines\n");
    if let Some(pg) = find(Arm::Postgres) {
        let inside = (7.91..=71.19).contains(&pg);
        println!(
            "* **L1** postgres arm {pg:.2} workflows/sec against the [7.91, 71.19] band \
             around the published 23.73: **{}**",
            if inside { "PASS" } else { "KILL" }
        );
        if let Some(redis) = find(Arm::RedisPg) {
            let ratio = if pg > 0.0 { redis / pg } else { 0.0 };
            println!(
                "* **L2** redis_pg / postgres = {ratio:.2}x against a 2.0x line: **{}**",
                if ratio >= 2.0 { "PASS" } else { "KILL" }
            );
        }
        if let Some(sqlite) = find(Arm::Sqlite) {
            println!(
                "* **L3** sqlite {sqlite:.2} vs postgres {pg:.2} workflows/sec: **{}**",
                if sqlite >= pg { "PASS" } else { "KILL" }
            );
        }
    }
}
