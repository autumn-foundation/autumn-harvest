// NON-PRODUCTION THROWAWAY APPARATUS. Never build against this. It calls the
// public API of `autumn-harvest` and `autumn-harvest-redis` only. It modifies
// no workspace crate.
//
// It measures the deployment-shaped question of assay ledger #8. The
// integrated path is a real `Worker` pool, real Postgres claim and completion
// transactions, and Postgres as the source of truth. Does that path clear the
// founding ">10,000 tasks/sec" line? By what multiplier does it beat the same
// pool on the Postgres claim path?
//
// The pool shape, the workload, the two shapes and the repetition plan come
// from `docs/rnd/2026-09-07-redis-dispatch-integrated-throughput-preregistration.md`.
// The worker construction, the handler registry, the side-effect table and
// the Redis drain probe follow
// `autumn-harvest-redis/tests/worker_dispatch_e2e.rs`.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::dispatch::DispatchSettings;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{Priority, StartSource, StartWorkflowParams, WorkflowContext};
use autumn_harvest_redis::{RedisDispatch, RedisDispatchConfig};

use diesel::prelude::QueryableByName;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use redis::AsyncCommands;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixed shape, from the pre-registration.
// ---------------------------------------------------------------------------

/// The one queue every worker serves.
const QUEUE: &str = "assay6";
/// The one registered workflow type.
const WORKFLOW: &str = "assay6_wf";
/// The one registered activity type.
const ACTIVITY: &str = "assay6_noop";
/// Consumer group name, matching the shared default.
const CONSUMER_GROUP: &str = "harvest_workers";
/// In-process `Worker` instances in the pool.
const WORKERS: usize = 4;
/// Workflow slots per worker.
const WORKFLOW_SLOTS: usize = 8;
/// Activity slots per worker.
const ACTIVITY_SLOTS: usize = 16;
/// Connections in the one pool the whole worker pool shares.
const POOL_SIZE: usize = 64;
/// Poll interval of the worker loop, on both arms. The pre-registered value.
///
/// `Worker::run` also drives its metrics samplers on this interval. Several
/// of those samplers issue a scan of the in-flight population on every tick,
/// so the value governs far more than the claim cadence. `ASSAY6_WORKER_POLL_MS`
/// raises it for the diagnostic variant that separates the two costs.
const WORKER_POLL_MS: u64 = 25;
/// Blocking read wait for the dispatch channel, on the Redis arm.
const DISPATCH_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// Reconcile sweep interval, on the Redis arm.
const DISPATCH_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
/// Visibility timeout of a delivered reference.
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(60);
/// Lifetime of a dedupe marker.
const DEDUPE_TTL: Duration = Duration::from_secs(600);

/// Completed `harvest_task_queue` rows one workflow of this shape produces.
///
/// The pre-registration expects three: a workflow task that schedules the
/// activity, the activity task, and a workflow task that completes the run.
/// The engine produces two. The workflow task row is parked and re-pended for
/// the second turn, so one row serves both turns. The measured numbers count
/// rows, which is the definition the pre-registration gives.
const TASKS_PER_WORKFLOW: f64 = 2.0;

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
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_u64(key: &str, fallback: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

struct Settings {
    admin_url: String,
    database_url: String,
    database_name: String,
    redis_url: String,
    workflows: usize,
    reps: usize,
    paced_secs: u64,
    drain_cap_secs: u64,
    control_cap_secs: u64,
    seeders: usize,
    shapes: String,
    arms: String,
    worker_poll: Duration,
}

impl Settings {
    fn from_env() -> Self {
        let database_name = env_string("ASSAY6_DB_NAME", "assay6");
        let admin_url = env_string(
            "ASSAY6_ADMIN_URL",
            "postgres://postgres@127.0.0.1:5432/postgres",
        );
        let database_url = env_string(
            "ASSAY6_DATABASE_URL",
            &format!("postgres://postgres@127.0.0.1:5432/{database_name}"),
        );
        Self {
            admin_url,
            database_url,
            database_name,
            redis_url: env_string("ASSAY6_REDIS_URL", "redis://127.0.0.1:6379"),
            workflows: env_usize("ASSAY6_WORKFLOWS", 10_000),
            reps: env_usize("ASSAY6_REPS", 3),
            paced_secs: env_u64("ASSAY6_PACED_SECS", 30),
            drain_cap_secs: env_u64("ASSAY6_DRAIN_CAP_SECS", 600),
            control_cap_secs: env_u64(
                "ASSAY6_CONTROL_CAP_SECS",
                env_u64("ASSAY6_DRAIN_CAP_SECS", 600),
            ),
            seeders: env_usize("ASSAY6_SEEDERS", 16),
            shapes: env_string("ASSAY6_SHAPES", "drain,paced"),
            arms: env_string("ASSAY6_ARMS", "redis,control"),
            worker_poll: Duration::from_millis(env_u64("ASSAY6_WORKER_POLL_MS", WORKER_POLL_MS)),
        }
    }

    fn runs_shape(&self, shape: Shape) -> bool {
        self.shapes
            .split(',')
            .any(|part| part.trim() == shape.as_str())
    }

    fn runs_arm(&self, arm: Arm) -> bool {
        self.arms.split(',').any(|part| part.trim() == arm.as_str())
    }

    /// Wall-clock cap for one run of `arm`.
    ///
    /// The control arm gets its own cap. A control drain at this backlog
    /// depth does not finish, so its cap decides the length of the report,
    /// not the result.
    const fn cap(&self, arm: Arm) -> Duration {
        match arm {
            Arm::Redis => Duration::from_secs(self.drain_cap_secs),
            Arm::Control => Duration::from_secs(self.control_cap_secs),
        }
    }
}

// ---------------------------------------------------------------------------
// Arms and shapes.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    Redis,
    Control,
}

impl Arm {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Control => "control",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Drain,
    Paced,
}

impl Shape {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::Paced => "paced",
        }
    }
}

// ---------------------------------------------------------------------------
// Handler scope.
// ---------------------------------------------------------------------------

/// Pool the activity handler writes its side-effect row through.
///
/// The handlers are plain function pointers with no state of their own, so
/// the coordinates live in process globals. The pool is separate from the
/// worker pool. A handler that competed for a worker connection would charge
/// the measurement for a queue this deployment shape does not have.
static SIDE_EFFECT_POOL: OnceLock<RwLock<Option<DbPool>>> = OnceLock::new();

fn side_effect_pool_slot() -> &'static RwLock<Option<DbPool>> {
    SIDE_EFFECT_POOL.get_or_init(|| RwLock::new(None))
}

fn set_side_effect_pool(pool: Option<DbPool>) {
    *side_effect_pool_slot()
        .write()
        .expect("side effect pool lock") = pool;
}

fn side_effect_pool() -> DbPool {
    side_effect_pool_slot()
        .read()
        .expect("side effect pool lock")
        .clone()
        .expect("the side effect pool must be installed before a run")
}

/// Counts activity handler bodies that could not write their row.
static SIDE_EFFECT_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Read one counter.
///
/// A free function because `RunQueryDsl` is in scope here. Its `load` method
/// shadows `AtomicU64::load` on the counter and on an `Arc` around one.
fn counter(value: &AtomicU64) -> u64 {
    AtomicU64::load(value, Ordering::Relaxed)
}

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// One workflow with one activity. This is the whole workload.
fn wf_one_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw(ACTIVITY, input, QUEUE)
            .await
            .map_err(|err| err.to_string())
    })
}

/// A no-op activity that records one side-effect row per run.
///
/// The row is the correctness precondition's counter. The insert is charged
/// to both arms equally, so it does not bias the comparison.
fn act_noop(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let pool = side_effect_pool();
        match pool.get().await {
            Ok(mut conn) => {
                if diesel::sql_query("INSERT INTO assay6_side_effects DEFAULT VALUES")
                    .execute(&mut conn)
                    .await
                    .is_err()
                {
                    SIDE_EFFECT_ERRORS.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                SIDE_EFFECT_ERRORS.fetch_add(1, Ordering::Relaxed);
            }
        }
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
        module: "assay6",
        handler: wf_one_activity,
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

fn activity_info() -> ActivityInfo {
    ActivityInfo {
        name: ACTIVITY,
        module: "assay6",
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
        handler: act_noop,
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
        vec![activity_info()],
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
struct WindowRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    tasks: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    window_secs: Option<f64>,
}

#[derive(QueryableByName)]
struct LatencyRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    samples: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    p50_ms: Option<f64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    p99_ms: Option<f64>,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    negative: i64,
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
    conn.batch_execute(
        "CREATE UNLOGGED TABLE assay6_side_effects (
             id bigserial PRIMARY KEY,
             noted_at timestamptz NOT NULL DEFAULT now()
         )",
    )
    .await
    .expect("the side effect table should be created");
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

async fn side_effect_rows(conn: &mut AsyncPgConnection) -> i64 {
    scalar(
        conn,
        "SELECT count(*)::bigint AS value FROM assay6_side_effects",
    )
    .await
}

/// Completed task rows and the window they completed in.
///
/// The window is the first claim to the last completion, as the
/// pre-registration defines it. Both ends come from Postgres, never from a
/// clock in this process.
async fn drain_window(conn: &mut AsyncPgConnection) -> (i64, f64) {
    let row = diesel::sql_query(
        "SELECT count(*)::bigint AS tasks, \
                EXTRACT(EPOCH FROM (max(completed_at) - min(started_at)))::double precision \
                  AS window_secs \
         FROM harvest_task_queue WHERE state = 'COMPLETED'",
    )
    .get_result::<WindowRow>(conn)
    .await
    .expect("window query");
    (row.tasks, row.window_secs.unwrap_or(0.0))
}

/// Dispatch latency percentiles over one task-type population.
///
/// The sample is `started_at - GREATEST(created_at, scheduled_at)`. The
/// pre-registration writes the line as `created_at` to `started_at`. A row
/// parked until a future `scheduled_at` would charge that park time to the
/// channel. The later of the two timestamps is therefore the point from which
/// the row is claimable. `created_at` is nullable for pre-upgrade rows, so it
/// falls back to `scheduled_at`.
async fn latency(conn: &mut AsyncPgConnection, task_type: Option<&str>) -> LatencyRow {
    let filter = match task_type {
        Some(kind) => format!("AND task_type = '{kind}'"),
        None => String::new(),
    };
    diesel::sql_query(format!(
        "SELECT count(*)::bigint AS samples, \
                percentile_cont(0.5) WITHIN GROUP (ORDER BY ms)::double precision AS p50_ms, \
                percentile_cont(0.99) WITHIN GROUP (ORDER BY ms)::double precision AS p99_ms, \
                count(*) FILTER (WHERE ms < 0)::bigint AS negative \
         FROM ( \
           SELECT EXTRACT(EPOCH FROM ( \
                    started_at - GREATEST(COALESCE(created_at, scheduled_at), scheduled_at) \
                  )) * 1000.0 AS ms \
           FROM harvest_task_queue \
           WHERE started_at IS NOT NULL {filter} \
         ) s"
    ))
    .get_result::<LatencyRow>(conn)
    .await
    .expect("latency query")
}

// ---------------------------------------------------------------------------
// Redis probe.
// ---------------------------------------------------------------------------

/// Reads the dispatch key space so a run can prove the channel drained.
///
/// The commands are the `redis-cli` equivalents `XLEN`, `XPENDING` and
/// `SCAN`. Deletion is scoped to this run's own prefix. `FLUSHALL` is never
/// used.
struct RedisProbe {
    conn: redis::aio::ConnectionManager,
    prefix: String,
}

impl RedisProbe {
    async fn connect(url: &str, prefix: &str) -> Self {
        let client = redis::Client::open(url).expect("redis url should parse");
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .expect("redis should accept a connection");
        Self {
            conn,
            prefix: prefix.to_string(),
        }
    }

    fn stream_key(&self) -> String {
        format!("{}:dispatch:{QUEUE}", self.prefix)
    }

    async fn stream_len(&self) -> i64 {
        let mut conn = self.conn.clone();
        conn.xlen(self.stream_key()).await.unwrap_or(0)
    }

    async fn pending_count(&self) -> usize {
        let mut conn = self.conn.clone();
        let reply: redis::RedisResult<redis::streams::StreamPendingReply> =
            conn.xpending(self.stream_key(), CONSUMER_GROUP).await;
        match reply {
            Ok(redis::streams::StreamPendingReply::Data(data)) => data.count,
            _ => 0,
        }
    }

    async fn keys(&self, pattern: &str) -> Vec<String> {
        let mut conn = self.conn.clone();
        let mut cursor: u64 = 0;
        let mut found = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut conn)
                .await
                .expect("scan");
            found.extend(batch);
            cursor = next;
            if cursor == 0 {
                return found;
            }
        }
    }

    async fn marker_count(&self) -> usize {
        self.keys(&format!("{}:dispatch:marker:*", self.prefix))
            .await
            .len()
    }

    /// Stream length, pending entries and marker keys, all zero on a clean end.
    async fn residue(&self) -> (i64, usize, usize) {
        (
            self.stream_len().await,
            self.pending_count().await,
            self.marker_count().await,
        )
    }

    /// Wait for the channel to hold nothing, or give up.
    ///
    /// A reference recovered on the visibility timeout can land after the last
    /// execution reaches its terminal state. Call this while a worker still
    /// runs, so the check measures convergence.
    async fn await_drained(&self, deadline: Duration) -> (i64, usize, usize) {
        let until = Instant::now() + deadline;
        loop {
            let residue = self.residue().await;
            if residue == (0, 0, 0) || Instant::now() >= until {
                return residue;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Delete every key under this run's prefix, and nothing else.
    async fn wipe(&self) {
        let keys = self.keys(&format!("{}:*", self.prefix)).await;
        if keys.is_empty() {
            return;
        }
        let mut conn = self.conn.clone();
        let _: i64 = conn.del(keys).await.expect("del");
    }
}

// ---------------------------------------------------------------------------
// Worker pool.
// ---------------------------------------------------------------------------

fn runtime_config(worker_id: &str, poll_interval: Duration) -> WorkerRuntimeConfig {
    let mut config: WorkerRuntimeConfig = WorkerConfig::default().with_queues([QUEUE]).into();
    config.worker_id = worker_id.to_string();
    config.poll_interval = poll_interval;
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
    fn start(pool: &DbPool, run: &str, poll_interval: Duration) -> Self {
        let registry = registry();
        let mut workers = Vec::with_capacity(WORKERS);
        let mut handles = Vec::with_capacity(WORKERS);
        for index in 0..WORKERS {
            let worker = Arc::new(
                Worker::new(
                    runtime_config(&format!("{run}-w{index}"), poll_interval),
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
/// The starts run on `seeders` connections in parallel. One connection cannot
/// seed ten thousand rows quickly enough. The seed phase is not part of any
/// measured window.
async fn seed(url: &str, run: &str, count: usize, seeders: usize) -> (usize, Duration) {
    let started = Instant::now();
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
    (total, started.elapsed())
}

// ---------------------------------------------------------------------------
// Arm installation.
// ---------------------------------------------------------------------------

/// Install the arm's dispatch configuration.
///
/// The Redis arm installs a real `RedisDispatch` under a per-run key prefix.
/// The control arm removes any channel, so the workers use the Postgres claim
/// path.
async fn install_arm(arm: Arm, redis_url: &str, prefix: &str) -> Option<RedisProbe> {
    autumn_harvest::dispatch::uninstall();
    match arm {
        Arm::Control => None,
        Arm::Redis => {
            let dispatch = RedisDispatch::connect(
                redis_url,
                RedisDispatchConfig {
                    key_prefix: prefix.to_string(),
                    consumer_group: CONSUMER_GROUP.to_string(),
                    visibility_timeout: VISIBILITY_TIMEOUT,
                    dedupe_ttl: DEDUPE_TTL,
                },
            )
            .await
            .expect("redis dispatch should connect");
            autumn_harvest::dispatch::install(
                Arc::new(dispatch),
                DispatchSettings {
                    poll_interval: DISPATCH_POLL_INTERVAL,
                    reconcile_interval: DISPATCH_RECONCILE_INTERVAL,
                    ..Default::default()
                },
            );
            Some(RedisProbe::connect(redis_url, prefix).await)
        }
    }
}

// ---------------------------------------------------------------------------
// Results.
// ---------------------------------------------------------------------------

struct DrainRun {
    arm: Arm,
    rep: usize,
    seeded: usize,
    seed_secs: f64,
    completed_execs: i64,
    completed_tasks: i64,
    side_effects: i64,
    window_secs: f64,
    tasks_per_sec: f64,
    workflows_per_sec: f64,
    truncated: bool,
    residue: Option<(i64, usize, usize)>,
    dropped_hints: u64,
    side_effect_errors: u64,
}

impl DrainRun {
    fn correct(&self, target: usize) -> bool {
        let counted = target as i64;
        let redis_clean = self.residue.is_none_or(|residue| residue == (0, 0, 0));
        self.completed_execs == counted
            && self.side_effects == counted
            && self.side_effect_errors == 0
            && redis_clean
            && !self.truncated
    }

    fn print(&self) {
        let (stream, pending, markers) = self.residue.unwrap_or((0, 0, 0));
        println!(
            "ASSAY6 shape=drain arm={} rep={} seeded={} seed_s={:.2} completed_execs={} \
             completed_tasks={} side_effects={} window_s={:.3} tasks_per_s={:.2} \
             workflows_per_s={:.2} truncated={} stream={} pending={} markers={} \
             dropped_hints={} side_effect_errors={}",
            self.arm.as_str(),
            self.rep,
            self.seeded,
            self.seed_secs,
            self.completed_execs,
            self.completed_tasks,
            self.side_effects,
            self.window_secs,
            self.tasks_per_sec,
            self.workflows_per_sec,
            self.truncated,
            stream,
            pending,
            markers,
            self.dropped_hints,
            self.side_effect_errors,
        );
    }
}

struct PacedRun {
    arm: Arm,
    rep: usize,
    target_rate: f64,
    started: usize,
    achieved_rate: f64,
    completed_execs: i64,
    side_effects: i64,
    activity_samples: i64,
    activity_p50: Option<f64>,
    activity_p99: Option<f64>,
    all_p50: Option<f64>,
    all_p99: Option<f64>,
    negative: i64,
    residue: Option<(i64, usize, usize)>,
    complete: bool,
}

fn render(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |ms| format!("{ms:.3}"))
}

impl PacedRun {
    fn print(&self) {
        let (stream, pending, markers) = self.residue.unwrap_or((0, 0, 0));
        println!(
            "ASSAY6 shape=paced arm={} rep={} target_wf_per_s={:.2} started={} \
             achieved_wf_per_s={:.2} completed_execs={} side_effects={} n={} \
             activity_p50_ms={} activity_p99_ms={} all_p50_ms={} all_p99_ms={} \
             negative={} stream={} pending={} markers={} complete={}",
            self.arm.as_str(),
            self.rep,
            self.target_rate,
            self.started,
            self.achieved_rate,
            self.completed_execs,
            self.side_effects,
            self.activity_samples,
            render(self.activity_p50),
            render(self.activity_p99),
            render(self.all_p50),
            render(self.all_p99),
            self.negative,
            stream,
            pending,
            markers,
            self.complete,
        );
    }
}

// ---------------------------------------------------------------------------
// The two shapes.
// ---------------------------------------------------------------------------

async fn run_drain(settings: &Settings, arm: Arm, rep: usize) -> DrainRun {
    let run = format!("a6_{}", Uuid::new_v4().simple());
    reset_database(settings).await;

    let worker_pool = build_pool(&settings.database_url, POOL_SIZE);
    let effects_pool = build_pool(&settings.database_url, 16);
    set_side_effect_pool(Some(effects_pool));
    SIDE_EFFECT_ERRORS.store(0, Ordering::Relaxed);

    let probe = install_arm(arm, &settings.redis_url, &run).await;

    // Seed with the workers stopped, so the whole backlog exists before the
    // measured window opens.
    let (seeded, seed_elapsed) = seed(
        &settings.database_url,
        &run,
        settings.workflows,
        settings.seeders,
    )
    .await;

    let pool = Pool::start(&worker_pool, &run, settings.worker_poll);
    let deadline = Instant::now() + settings.cap(arm);
    let mut conn = connect(&settings.database_url).await;
    let mut truncated = true;
    while Instant::now() < deadline {
        if completed_executions(&mut conn).await >= seeded as i64 {
            truncated = false;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Check the channel while a worker still runs: a reference recovered on
    // the visibility timeout needs a live consumer to ack it.
    let residue = match probe.as_ref() {
        Some(probe) if !truncated => Some(probe.await_drained(Duration::from_secs(30)).await),
        Some(probe) => Some(probe.residue().await),
        None => None,
    };

    pool.stop().await;

    let completed_execs = completed_executions(&mut conn).await;
    let side_effects = side_effect_rows(&mut conn).await;
    let (completed_tasks, window_secs) = drain_window(&mut conn).await;
    let tasks_per_sec = if window_secs > 0.0 {
        completed_tasks as f64 / window_secs
    } else {
        0.0
    };
    let workflows_per_sec = if window_secs > 0.0 {
        completed_execs as f64 / window_secs
    } else {
        0.0
    };

    if let Some(probe) = probe.as_ref() {
        probe.wipe().await;
    }
    autumn_harvest::dispatch::uninstall();
    set_side_effect_pool(None);

    DrainRun {
        arm,
        rep,
        seeded,
        seed_secs: seed_elapsed.as_secs_f64(),
        completed_execs,
        completed_tasks,
        side_effects,
        window_secs,
        tasks_per_sec,
        workflows_per_sec,
        truncated,
        residue,
        dropped_hints: autumn_harvest::dispatch::dropped_hints(),
        side_effect_errors: counter(&SIDE_EFFECT_ERRORS),
    }
}

/// Start workflows at `rate` per second for `secs`, with the workers running.
///
/// The loop ticks every 10 ms and starts the batch the tick owes. Each batch
/// runs on its own connection from a starter pool, so a slow start does not
/// delay the next tick.
async fn paced_starts(url: &str, run: &str, rate: f64, secs: u64) -> (usize, f64) {
    let tick = Duration::from_millis(10);
    let ticks = (secs * 1000) / 10;
    // Starts the tick owes, which is fractional below one hundred per second.
    // The `owed` accumulator carries the fraction to the next tick, so a rate
    // under one hundred per second is held exactly rather than rounded up.
    let per_tick = rate / 100.0;
    let starter_pool = build_pool(url, 32);
    let started = Arc::new(AtomicU64::new(0));
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut owed = 0.0_f64;
    let mut index = 0_usize;
    let mut set = tokio::task::JoinSet::new();
    let clock = Instant::now();
    for _ in 0..ticks {
        interval.tick().await;
        owed += per_tick;
        let batch = owed.floor() as usize;
        owed -= batch as f64;
        for _ in 0..batch {
            let pool = starter_pool.clone();
            let id = format!("{run}-p{index}");
            let started = Arc::clone(&started);
            index += 1;
            set.spawn(async move {
                if let Ok(mut conn) = pool.get().await
                    && start_one(&mut conn, &id).await
                {
                    started.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        while set.len() > 512 {
            let _ = set.join_next().await;
        }
    }
    while set.join_next().await.is_some() {}
    let elapsed = clock.elapsed().as_secs_f64();
    let total = counter(&started) as usize;
    // The first tick fires immediately, so n starts span n-1 periods. The
    // e2e benchmark divides by n-1 for the same reason.
    let achieved = if elapsed > 0.0 {
        total.saturating_sub(1) as f64 / elapsed
    } else {
        0.0
    };
    (total, achieved)
}

async fn run_paced(settings: &Settings, arm: Arm, rep: usize, rate: f64) -> PacedRun {
    let run = format!("a6_{}", Uuid::new_v4().simple());
    reset_database(settings).await;

    let worker_pool = build_pool(&settings.database_url, POOL_SIZE);
    let effects_pool = build_pool(&settings.database_url, 16);
    set_side_effect_pool(Some(effects_pool));
    SIDE_EFFECT_ERRORS.store(0, Ordering::Relaxed);

    let probe = install_arm(arm, &settings.redis_url, &run).await;
    let pool = Pool::start(&worker_pool, &run, settings.worker_poll);

    let (started, achieved_rate) =
        paced_starts(&settings.database_url, &run, rate, settings.paced_secs).await;

    // Let the tail of the paced window finish, so the completed population is
    // not truncated at an arbitrary point.
    let mut conn = connect(&settings.database_url).await;
    let deadline = Instant::now() + settings.cap(arm);
    let mut complete = false;
    while Instant::now() < deadline {
        if completed_executions(&mut conn).await >= started as i64 {
            complete = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let residue = match probe.as_ref() {
        Some(probe) if complete => Some(probe.await_drained(Duration::from_secs(30)).await),
        Some(probe) => Some(probe.residue().await),
        None => None,
    };

    pool.stop().await;

    let completed_execs = completed_executions(&mut conn).await;
    let side_effects = side_effect_rows(&mut conn).await;
    let activity = latency(&mut conn, Some("activity")).await;
    let all = latency(&mut conn, None).await;

    if let Some(probe) = probe.as_ref() {
        probe.wipe().await;
    }
    autumn_harvest::dispatch::uninstall();
    set_side_effect_pool(None);

    PacedRun {
        arm,
        rep,
        target_rate: rate,
        started,
        achieved_rate,
        completed_execs,
        side_effects,
        activity_samples: activity.samples,
        activity_p50: activity.p50_ms,
        activity_p99: activity.p99_ms,
        all_p50: all.p50_ms,
        all_p99: all.p99_ms,
        negative: activity.negative,
        residue,
        complete,
    }
}

// ---------------------------------------------------------------------------
// Driver.
// ---------------------------------------------------------------------------

/// The paced-start rate one arm's drain runs support, in workflows per second.
fn paced_rate(drains: &[DrainRun], arm: Arm) -> f64 {
    let rates: Vec<f64> = drains
        .iter()
        .filter(|run| run.arm == arm)
        .map(|run| run.tasks_per_sec / TASKS_PER_WORKFLOW)
        .collect();
    mean(&rates)
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build the assay runtime");
    runtime.block_on(run());
}

async fn run() {
    let settings = Settings::from_env();
    let wall = Instant::now();

    println!("# assay ledger #8: integrated Redis dispatch throughput");
    println!(
        "# workflows={} reps={} paced_secs={} drain_cap_secs={} pool={} workers={} \
         wf_slots={} act_slots={} worker_poll_ms={}",
        settings.workflows,
        settings.reps,
        settings.paced_secs,
        settings.drain_cap_secs,
        POOL_SIZE,
        WORKERS,
        WORKFLOW_SLOTS,
        ACTIVITY_SLOTS,
        settings.worker_poll.as_millis(),
    );
    println!("# input_bytes={}", workflow_input().to_string().len());

    let mut drains: Vec<DrainRun> = Vec::new();
    if settings.runs_shape(Shape::Drain) {
        for rep in 1..=settings.reps {
            for arm in [Arm::Redis, Arm::Control] {
                if !settings.runs_arm(arm) {
                    continue;
                }
                let result = run_drain(&settings, arm, rep).await;
                result.print();
                drains.push(result);
            }
        }
    }

    // The paced rate is the drain arm's sustained rate in workflow
    // equivalents. It comes from completed task rows, not from completed
    // executions. A truncated control drain completes task rows without
    // completing one execution. A zero there would leave the paced control
    // arm with no rate to hold.
    let redis_drain_rate = paced_rate(&drains, Arm::Redis);
    let control_drain_rate = paced_rate(&drains, Arm::Control);
    #[allow(clippy::cast_precision_loss, reason = "a paced rate is a small number")]
    let paced_override = env_u64("ASSAY6_PACED_RATE_MILLI", 0) as f64 / 1000.0;

    let mut paced: Vec<PacedRun> = Vec::new();
    if settings.runs_shape(Shape::Paced) {
        for rep in 1..=settings.reps {
            for arm in [Arm::Redis, Arm::Control] {
                if !settings.runs_arm(arm) {
                    continue;
                }
                let rate = match arm {
                    Arm::Redis => redis_drain_rate,
                    Arm::Control => control_drain_rate,
                };
                // A control drain that completes no task row has no rate of
                // its own. It then holds the Redis arm's rate, so both arms
                // face the same offered load. `ASSAY6_PACED_RATE` overrides
                // both, which is how the paced shape is re-run on its own.
                let rate = if paced_override > 0.0 {
                    paced_override
                } else if rate > 0.0 {
                    rate
                } else {
                    redis_drain_rate
                };
                let result = run_paced(&settings, arm, rep, rate).await;
                result.print();
                paced.push(result);
            }
        }
    }

    println!("\n## drain shape");
    println!(
        "| arm | rep | seeded | completed execs | completed tasks | side effects | window s | \
         tasks/s | workflows/s | correct |"
    );
    println!("|:--|--:|--:|--:|--:|--:|--:|--:|--:|:--|");
    for run in &drains {
        println!(
            "| {} | {} | {} | {} | {} | {} | {:.3} | {:.2} | {:.2} | {} |",
            run.arm.as_str(),
            run.rep,
            run.seeded,
            run.completed_execs,
            run.completed_tasks,
            run.side_effects,
            run.window_secs,
            run.tasks_per_sec,
            run.workflows_per_sec,
            if run.correct(settings.workflows) {
                "yes"
            } else if run.truncated {
                "truncated"
            } else {
                "NO"
            },
        );
    }

    let redis_tasks = mean(
        &drains
            .iter()
            .filter(|run| run.arm == Arm::Redis)
            .map(|run| run.tasks_per_sec)
            .collect::<Vec<_>>(),
    );
    let control_tasks = mean(
        &drains
            .iter()
            .filter(|run| run.arm == Arm::Control)
            .map(|run| run.tasks_per_sec)
            .collect::<Vec<_>>(),
    );
    println!(
        "\nmean tasks/s: redis={redis_tasks:.2} control={control_tasks:.2} multiplier={:.2}x",
        if control_tasks > 0.0 {
            redis_tasks / control_tasks
        } else {
            f64::INFINITY
        }
    );

    println!("\n## paced shape");
    println!(
        "| arm | rep | target wf/s | started | achieved wf/s | n | activity p50 ms | \
         activity p99 ms | all p50 ms | all p99 ms |"
    );
    println!("|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|");
    for run in &paced {
        println!(
            "| {} | {} | {:.2} | {} | {:.2} | {} | {} | {} | {} | {} |",
            run.arm.as_str(),
            run.rep,
            run.target_rate,
            run.started,
            run.achieved_rate,
            run.activity_samples,
            render(run.activity_p50),
            render(run.activity_p99),
            render(run.all_p50),
            render(run.all_p99),
        );
    }

    println!("\n# wall_secs={:.1}", wall.elapsed().as_secs_f64());
}
