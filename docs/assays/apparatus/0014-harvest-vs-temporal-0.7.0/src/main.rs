// NON-PRODUCTION APPARATUS. It calls the public API of `autumn-harvest` and
// `autumn-harvest-redis` only. It changes no workspace crate.
//
// It answers assay ledger #14. The lines, the shape and the run order come
// from `docs/rnd/2026-10-08-harvest-vs-temporal-0.7.0-depth-sweep-preregistration.md`.
//
// The workload is assay #10's harness, ported by value from
// `docs/assays/apparatus/0010-cross-mode-throughput/src/main.rs`. Two things
// change. One process sweeps every registered depth. A capture recorder keeps
// the per-event #1815 signals.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::dispatch::DispatchSettings;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::telemetry::{DbOp, MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{Priority, StartSource, StartWorkflowParams, WorkflowContext};
use autumn_harvest_redis::{RedisDispatch, RedisDispatchConfig};

use diesel::prelude::QueryableByName;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use redis::AsyncCommands;

// ---------------------------------------------------------------------------
// Fixed shape, as assay #10 and assay #11 ran it.
// ---------------------------------------------------------------------------

const QUEUE: &str = "default";
const WORKFLOW: &str = "harvest_e2e_bench_wf";
const ACTIVITIES: [&str; 3] = [
    "harvest_e2e_bench_step_1",
    "harvest_e2e_bench_step_2",
    "harvest_e2e_bench_step_3",
];

const WORKERS: usize = 1;
const WORKFLOW_SLOTS: usize = 8;
const ACTIVITY_SLOTS: usize = 16;
const POOL_SIZE: usize = 32;
const WORKER_POLL_MS: u64 = 25;

const CONSUMER_GROUP: &str = "harvest_workers";
const DISPATCH_POLL_INTERVAL: Duration = Duration::from_millis(20);
const DISPATCH_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(60);
const DEDUPE_TTL: Duration = Duration::from_secs(600);

/// Assay #11's registered workflow input, about 40 bytes.
const INPUT_JSON: &str = r#"{"p":"0123456789abcdef0123456789abcdef"}"#;

/// How often the drain loop reads completion and pool occupancy.
const SAMPLE_EVERY: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Environment knobs. Every default is the registered value.
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
    tree: String,
    round: usize,
    database_url: String,
    admin_url: String,
    database_name: String,
    redis_url: String,
    input_json: String,
    depths: Vec<usize>,
    seeders: usize,
    cap_secs: u64,
    arms: Vec<Arm>,
}

impl Settings {
    fn from_env() -> Self {
        let arms = env_string("ASSAY14_ARMS", "postgres,redis_pg")
            .split(',')
            .map(|raw| Arm::parse(raw).unwrap_or_else(|| panic!("unknown arm `{raw}`")))
            .collect();
        let depths = env_string("ASSAY14_DEPTHS", "250,500,1000,2000")
            .split(',')
            .map(|raw| raw.trim().parse().expect("a depth is a whole number"))
            .collect();
        Self {
            tree: env_string("ASSAY14_TREE", "unknown"),
            round: env_usize("ASSAY14_ROUND", 0),
            database_url: env_string(
                "ASSAY14_DATABASE_URL",
                "postgres://postgres@127.0.0.1:5432/assay14",
            ),
            admin_url: env_string(
                "ASSAY14_ADMIN_URL",
                "postgres://postgres@127.0.0.1:5432/postgres",
            ),
            database_name: env_string("ASSAY14_DB_NAME", "assay14"),
            redis_url: env_string("ASSAY14_REDIS_URL", "redis://127.0.0.1:6379"),
            input_json: env_string("ASSAY14_INPUT_JSON", INPUT_JSON),
            depths,
            seeders: env_usize("ASSAY14_SEEDERS", 16),
            cap_secs: env_usize("ASSAY14_CAP_SECS", 900) as u64,
            arms,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    Postgres,
    RedisPg,
}

impl Arm {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "postgres" => Some(Self::Postgres),
            "redis_pg" => Some(Self::RedisPg),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::RedisPg => "redis_pg",
        }
    }
}

// ---------------------------------------------------------------------------
// The #1815 capture recorder.
// ---------------------------------------------------------------------------

/// Per-event samples of one run, in seconds.
struct Samples {
    claim: Vec<f64>,
    persist: Vec<f64>,
    scan: Vec<f64>,
    heartbeat: Vec<f64>,
    pool_wait: Vec<f64>,
}

static SAMPLES: Mutex<Samples> = Mutex::new(Samples {
    claim: Vec::new(),
    persist: Vec::new(),
    scan: Vec::new(),
    heartbeat: Vec::new(),
    pool_wait: Vec::new(),
});

/// Keeps the per-event #1815 signals and turns off every sampler.
///
/// `is_enabled` returns false, as `NoOpMetrics` does. The worker then starts
/// no sampler and issues no sampler SQL, so the workload stays assay #10's.
/// The claim, persist, scan, heartbeat and pool-wait timings do not check
/// `is_enabled`, so they still arrive here.
struct Capture;

impl MetricsRecorder for Capture {
    fn is_enabled(&self) -> bool {
        false
    }

    fn record_db_query_duration(&self, op: DbOp, _shard: u16, seconds: f64) {
        let mut samples = SAMPLES.lock().expect("samples lock");
        match op {
            DbOp::Claim => samples.claim.push(seconds),
            DbOp::Persist => samples.persist.push(seconds),
            DbOp::Scan => samples.scan.push(seconds),
            DbOp::Heartbeat => samples.heartbeat.push(seconds),
        }
    }

    fn record_db_pool_wait(&self, _shard: u16, seconds: f64) {
        SAMPLES
            .lock()
            .expect("samples lock")
            .pool_wait
            .push(seconds);
    }
}

fn reset_samples() {
    let mut samples = SAMPLES.lock().expect("samples lock");
    samples.claim.clear();
    samples.persist.clear();
    samples.scan.clear();
    samples.heartbeat.clear();
    samples.pool_wait.clear();
}

/// The nearest-rank percentile of `values`, in milliseconds.
fn percentile_ms(values: &[f64], pct: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = ((pct / 100.0) * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1] * 1_000.0)
}

fn mean_ms(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64 * 1_000.0)
}

/// Render one signal as `name_n=… name_mean_ms=… name_p99_ms=…`.
///
/// A signal with no sample prints its count only. The grader then skips it.
fn signal_fields(name: &str, values: &[f64]) -> String {
    let mut out = format!("{name}_n={}", values.len());
    if let (Some(mean), Some(p99)) = (mean_ms(values), percentile_ms(values, 99.0)) {
        out.push_str(&format!(" {name}_mean_ms={mean:.3} {name}_p99_ms={p99:.3}"));
    }
    out
}

/// Pool occupancy, read from the pool's own counters.
#[derive(Default)]
struct Occupancy {
    reads: u64,
    in_use_sum: u64,
    in_use_max: u64,
}

impl Occupancy {
    fn read(&mut self, pool: &DbPool) {
        let status = pool.status();
        let in_use = status.size.saturating_sub(status.available) as u64;
        self.reads += 1;
        self.in_use_sum += in_use;
        self.in_use_max = self.in_use_max.max(in_use);
    }

    fn mean(&self) -> f64 {
        if self.reads == 0 {
            0.0
        } else {
            self.in_use_sum as f64 / self.reads as f64
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

/// Activity body runs, the correctness ledger.
static ACTIVITY_RUNS: AtomicU64 = AtomicU64::new(0);

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// The canonical workflow: three activities in sequence.
///
/// It ignores its input and sends JSON null to each activity, as
/// `bench_workflow` does in the published harness.
fn wf_three_activities(ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        for name in ACTIVITIES {
            ctx.execute_activity_raw(name, serde_json::Value::Null, QUEUE)
                .await
                .map_err(|err| err.to_string())?;
        }
        Ok(serde_json::json!({ "ok": true }))
    })
}

/// An inert activity body. It returns what `bench_activity` returns.
fn act_inert(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ACTIVITY_RUNS.fetch_add(1, Ordering::Relaxed);
        Ok(serde_json::json!({ "ok": true }))
    })
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: WORKFLOW,
        module: "assay14",
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
        module: "assay14",
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
            .metrics(Arc::new(Capture) as Arc<dyn MetricsRecorder>)
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

async fn pg_text(conn: &mut AsyncPgConnection, sql: &str) -> String {
    diesel::sql_query(sql)
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

async fn completed_executions(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT count(*)::bigint AS value FROM harvest_workflow_executions \
         WHERE state = 'COMPLETED'",
    )
    .get_result::<CountRow>(conn)
    .await
    .expect("count query")
    .value
}

/// The one-minute load average, so a contaminated run shows in the record.
fn load_one() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|raw| raw.split_whitespace().next().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

// ---------------------------------------------------------------------------
// Redis probe.
// ---------------------------------------------------------------------------

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

/// Reads the dispatch key space, so a run can prove that the channel drained.
struct RedisProbe {
    client: redis::Client,
    prefix: String,
}

impl RedisProbe {
    fn new(url: &str, prefix: &str) -> Self {
        Self {
            client: redis::Client::open(url).expect("redis client should open"),
            prefix: prefix.to_string(),
        }
    }

    /// Every key under this run's prefix.
    ///
    /// `RedisDispatch` wraps its keys in a hash tag, as in
    /// `{prefix:dispatch:queue}`. A plain `prefix*` glob misses them, so this
    /// reads both families. Returns `None` on a probe error.
    async fn keys(&self, conn: &mut redis::aio::MultiplexedConnection) -> Option<Vec<String>> {
        let mut keys: Vec<String> = conn.keys(format!("{}*", self.prefix)).await.ok()?;
        let tagged: Vec<String> = conn.keys(format!("{{{}:*", self.prefix)).await.ok()?;
        keys.extend(tagged);
        Some(keys)
    }

    /// Delete the keys under this run's prefix only. Never `FLUSHALL`.
    ///
    /// Returns false when the cleanup fails. A run must not start on a
    /// prefix that still holds entries.
    async fn clear(&self) -> bool {
        let Ok(mut conn) = self.client.get_multiplexed_async_connection().await else {
            return false;
        };
        let Some(keys) = self.keys(&mut conn).await else {
            return false;
        };
        for key in keys {
            if conn.del::<_, i64>(&key).await.is_err() {
                return false;
            }
        }
        true
    }

    /// Count stream entries, pending entries and dedupe markers.
    ///
    /// A probe error returns `None`. An unverified drain then makes the run
    /// invalid, so it cannot read as a clean drain.
    async fn residue(&self) -> Option<Residue> {
        let mut conn = self.client.get_multiplexed_async_connection().await.ok()?;
        let keys = self.keys(&mut conn).await?;
        let mut residue = Residue {
            entries: 0,
            pending: 0,
            markers: 0,
        };
        for key in keys {
            if key.contains("}:marker:") {
                residue.markers += 1;
                continue;
            }
            let kind: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .ok()?;
            if kind != "stream" {
                continue;
            }
            residue.entries += redis::cmd("XLEN")
                .arg(&key)
                .query_async::<i64>(&mut conn)
                .await
                .ok()?;
            let summary: redis::Value = redis::cmd("XPENDING")
                .arg(&key)
                .arg(CONSUMER_GROUP)
                .query_async(&mut conn)
                .await
                .ok()?;
            // A slice pattern, because Diesel's `first` shadows the slice method.
            if let redis::Value::Array(items) = summary
                && let [redis::Value::Int(count), ..] = items.as_slice()
            {
                residue.pending += *count;
            }
        }
        Some(residue)
    }
}

// ---------------------------------------------------------------------------
// Worker pool.
// ---------------------------------------------------------------------------

/// The worker configuration, with LISTEN/NOTIFY wired as in the published
/// harness.
fn runtime_config(worker_id: &str, database_url: &str) -> WorkerRuntimeConfig {
    let mut config: WorkerRuntimeConfig = WorkerConfig::default()
        .with_queues([QUEUE])
        .with_shard_assignments([ShardId::new(0)])
        .with_shard_notification_database_urls([(ShardId::new(0), database_url.to_string())])
        .into();
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
    fn start(pool: &DbPool, run: &str, database_url: &str) -> Self {
        let registry = registry();
        let mut workers = Vec::with_capacity(WORKERS);
        let mut handles = Vec::with_capacity(WORKERS);
        for index in 0..WORKERS {
            let worker = Arc::new(
                Worker::new(
                    runtime_config(&format!("{run}-w{index}"), database_url),
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

    /// Stop every worker. Abort one that overruns the shutdown timeout.
    ///
    /// A detached worker would keep polling while the next run resets the
    /// database, and could add to the activity counter.
    async fn stop(self) {
        for worker in &self.workers {
            worker.shutdown();
        }
        for handle in self.handles {
            let abort = handle.abort_handle();
            if tokio::time::timeout(Duration::from_secs(20), handle)
                .await
                .is_err()
            {
                abort.abort();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Seeding.
// ---------------------------------------------------------------------------

fn workflow_input(settings: &Settings) -> serde_json::Value {
    serde_json::from_str(&settings.input_json).expect("the seeded input should parse")
}

async fn start_one(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    input: serde_json::Value,
) -> bool {
    autumn_harvest::start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: WORKFLOW,
            workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: input.into(),
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

/// Seed `count` starts through the public start API, on parallel lanes.
///
/// Seeding is outside the measured window.
async fn seed(
    url: &str,
    run: &str,
    count: usize,
    seeders: usize,
    input: serde_json::Value,
) -> usize {
    let mut set = tokio::task::JoinSet::new();
    let lanes = seeders.max(1);
    for lane in 0..lanes {
        let url = url.to_string();
        let run = run.to_string();
        let input = input.clone();
        set.spawn(async move {
            let mut conn = connect(&url).await;
            let mut started_here = 0_usize;
            let mut index = lane;
            while index < count {
                if start_one(&mut conn, &format!("{run}-{index}"), input.clone()).await {
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
// One run.
// ---------------------------------------------------------------------------

struct RunOutcome {
    workflows_per_sec: f64,
    elapsed_secs: f64,
    completed: i64,
    activity_runs: u64,
    residue: Option<Residue>,
    probe_failed: bool,
    truncated: bool,
    occupancy: Occupancy,
    load_at_start: String,
}

impl RunOutcome {
    /// Grade the registered validity and correctness rules for this run.
    fn valid(&self, depth: usize) -> bool {
        let expected_activities = depth as u64 * ACTIVITIES.len() as u64;
        !self.truncated
            && !self.probe_failed
            && self.completed == depth as i64
            && self.activity_runs == expected_activities
            && self.residue.is_none_or(Residue::is_drained)
    }
}

/// Drain one backlog of `depth` workflows on one arm.
async fn run_once(settings: &Settings, arm: Arm, depth: usize) -> RunOutcome {
    let run = format!("a14-{}-{}-d{depth}-r{}", settings.tree, arm.as_str(), settings.round);
    reset_database(settings).await;
    ACTIVITY_RUNS.store(0, Ordering::Relaxed);

    // Install the dispatch channel before the seed. A start publishes a hint
    // only when a channel is installed. A backlog seeded first would reach the
    // worker only through the reconcile sweep.
    autumn_harvest::dispatch::uninstall();
    let probe = if arm == Arm::RedisPg {
        let prefix = format!("assay14:{run}");
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
        let probe = RedisProbe::new(&settings.redis_url, &prefix);
        assert!(probe.clear().await, "the redis prefix must be empty before the seed");
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
        depth,
        settings.seeders,
        workflow_input(settings),
    )
    .await;
    assert_eq!(seeded, depth, "every start should be accepted");

    let pool = build_pool(&settings.database_url, POOL_SIZE);
    let mut conn = connect(&settings.database_url).await;
    let mut occupancy = Occupancy::default();

    reset_samples();
    let load_at_start = load_one();
    let started = Instant::now();
    let workers = Pool::start(&pool, &run, &settings.database_url);

    // The cap is checked before completion, so a run that crosses the cap is
    // never kept.
    let mut truncated = false;
    loop {
        if started.elapsed().as_secs() >= settings.cap_secs {
            truncated = true;
            break;
        }
        if completed_executions(&mut conn).await >= depth as i64 {
            break;
        }
        occupancy.read(&pool);
        tokio::time::sleep(SAMPLE_EVERY).await;
    }
    let elapsed = started.elapsed().as_secs_f64();

    workers.stop().await;
    autumn_harvest::dispatch::uninstall();

    let completed = completed_executions(&mut conn).await;
    let (residue, probe_failed) = match &probe {
        Some(probe) => {
            let residue = probe.residue().await;
            let _ = probe.clear().await;
            (residue, residue.is_none())
        }
        None => (None, false),
    };

    RunOutcome {
        workflows_per_sec: if elapsed > 0.0 {
            completed as f64 / elapsed
        } else {
            0.0
        },
        elapsed_secs: elapsed,
        completed,
        // Qualified, because Diesel's `load` shadows the atomic method.
        activity_runs: AtomicU64::load(&ACTIVITY_RUNS, Ordering::Relaxed),
        residue,
        probe_failed,
        truncated,
        occupancy,
        load_at_start,
    }
}

/// Print one run as a `cell key=value ...` line, which `grade.py` reads.
fn print_cell(settings: &Settings, arm: Arm, depth: usize, outcome: &RunOutcome) {
    let samples = SAMPLES.lock().expect("samples lock");
    let wait_p99 = percentile_ms(&samples.pool_wait, 99.0).unwrap_or(0.0);
    let wait_max = percentile_ms(&samples.pool_wait, 100.0).unwrap_or(0.0);
    let residue = outcome.residue.map_or_else(
        || "none".to_string(),
        |r| format!("{}/{}/{}", r.entries, r.pending, r.markers),
    );
    println!(
        "cell tree={} arm={} depth={depth} rep={} wfps={:.6} elapsed={:.3} completed={} \
         activities={} valid={} truncated={} residue={residue} load1={} {} {} {} {} \
         wait_n={} wait_p99_ms={wait_p99:.3} wait_max_ms={wait_max:.3} \
         in_use_mean={:.3} in_use_max={}",
        settings.tree,
        arm.as_str(),
        settings.round,
        outcome.workflows_per_sec,
        outcome.elapsed_secs,
        outcome.completed,
        outcome.activity_runs,
        if outcome.valid(depth) { "PASS" } else { "FAIL" },
        outcome.truncated,
        outcome.load_at_start,
        signal_fields("claim", &samples.claim),
        signal_fields("persist", &samples.persist),
        signal_fields("scan", &samples.scan),
        signal_fields("heartbeat", &samples.heartbeat),
        samples.pool_wait.len(),
        outcome.occupancy.mean(),
        outcome.occupancy.in_use_max,
    );
}

#[tokio::main]
async fn main() {
    let settings = Settings::from_env();
    {
        let mut conn = connect(&settings.admin_url).await;
        println!("# Assay #14 — harvest arms, tree `{}`, round {}\n", settings.tree, settings.round);
        println!("Postgres: {}", pg_text(&mut conn, "SELECT version() AS value").await);
        println!(
            "Durability: fsync = {}, synchronous_commit = {}.",
            pg_text(&mut conn, "SELECT current_setting('fsync') AS value").await,
            pg_text(&mut conn, "SELECT current_setting('synchronous_commit') AS value").await,
        );
        println!(
            "Pool: {WORKERS} worker, {WORKFLOW_SLOTS} workflow slots, {ACTIVITY_SLOTS} activity \
             slots, {POOL_SIZE} connections, {WORKER_POLL_MS} ms poll. Input `{}`. Cap {} s.\n",
            settings.input_json, settings.cap_secs
        );
    }
    for arm in settings.arms.clone() {
        for depth in settings.depths.clone() {
            let outcome = run_once(&settings, arm, depth).await;
            print_cell(&settings, arm, depth, &outcome);
        }
    }
}
