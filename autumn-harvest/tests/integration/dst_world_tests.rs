#![cfg(feature = "db")]
//! The world simulation on Postgres (issue #2002).
//!
//! `PgWorld` implements `dst::world::World` with real `Worker`s. Each poll
//! runs one iteration of the `worker.rs` poll loop to completion. The seed
//! picks every action, so a failing seed replays exactly.
//!
//! The scope is the claim, the decision, the resident path, timers,
//! signals, the scheduler fire claim, the orphan reclaimer and the timeout
//! sweeper. Faults are worker stalls and crashes.
//!
//! Each run gets a fresh database, cloned from a migrated template. The
//! clock moves by a shift of every stored instant. See
//! `docs/testing/simulation.md`.
//!
//! ```bash
//! HARVEST_DST_SEEDS=50 cargo test -p autumn-harvest --test integration \
//!   dst_world_tests:: -- --test-threads=1
//! ```

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::SharedStateMap;
use autumn_harvest::dst::SeedPlan;
use autumn_harvest::dst::world::{
    self, Decision, Effect, ExecFacts, Fact, Plant, Snapshot, TICK_SECS, World, WorldAction,
    WorldConfig, WorldInvariant, WorldReport,
};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::HarvestSchedule;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::schema::{harvest_schedules, harvest_workflow_executions};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{ActivityContext, StartWorkflowParams, WorkflowContext};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};

use crate::integration_e2e::{build_test_pool, runtime_config, setup_test_database_url_or_env};

/// The sweep size of a normal test run. The nightly job sets
/// `HARVEST_DST_SEEDS` higher.
const DEFAULT_SEEDS: u64 = 4;

/// The planted defect fails a seed within this range.
const PLANT_SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 24,
};

/// Set this variable to keep each world database for a look after the run.
const KEEP_VAR: &str = "HARVEST_DST_WORLD_KEEP_DB";

const QUEUE: &str = "dst";
const CHAIN: &str = "dst_chain";
const TICKER: &str = "dst_tick";
const ADD: &str = "dst_add";

/// One advance shifts every instant back by one tick plus 1 ms.
///
/// The engine builds ids from instants, such as the workflow id of a
/// schedule slot. With a whole tick, slot 6 seen at tick 6 has the stored
/// instant of slot 2 seen at tick 2. The two slots then share one id. The
/// extra millisecond keeps the stored instants of two slots apart. A deadline of
/// whole ticks still comes due after the same number of advances.
const WARP_MS: i64 = 3_600_001;

/// The tables whose instants the clock shifts. `harvest_events` is
/// append-only, and no decision reads its timestamps.
const SKIPPED_TABLES: &str = "harvest_events%";

// ── Workload ────────────────────────────────────────────────────────────────

/// The state of one simulated worker process, shared with its handlers.
#[derive(Debug)]
struct WorkerTag {
    /// The worker index, without the incarnation.
    index: usize,
    /// Whether the workflow reads this tag. See `Plant::ForeignState`.
    plant: bool,
    /// Workflow bodies that started from the top on this worker.
    body_starts: AtomicU64,
}

/// `{"x": n}` becomes `n + 1`.
fn add_activity<'a>(
    _ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let x = input["x"].as_i64().ok_or("missing x")?;
        Ok(json!(x + 1))
    })
}

/// An activity, a timer, a signal and a second activity.
///
/// Each step awaits one command, so every decision can stay resident. With
/// the plant, the timer id depends on the worker. A cold replay on a worker
/// of the other parity then sees a different command.
fn chain_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let tag = ctx.state::<Arc<WorkerTag>>().ok_or("missing worker tag")?;
        tag.body_starts.fetch_add(1, Ordering::SeqCst);
        let i = input["i"].as_i64().ok_or("missing i")?;
        let a = ctx
            .execute_activity_raw(ADD, json!({ "x": i }), QUEUE)
            .await
            .map_err(|e| e.to_string())?;
        let nap = if tag.plant && tag.index % 2 == 1 {
            "nap-odd"
        } else {
            "nap"
        };
        ctx.timer(nap, TICK_SECS).await.map_err(|e| e.to_string())?;
        let go: Value = ctx.receive_signal("go").await.map_err(|e| e.to_string())?;
        let x = a.as_i64().ok_or("bad a")? + go["v"].as_i64().ok_or("bad v")?;
        let b = ctx
            .execute_activity_raw(ADD, json!({ "x": x }), QUEUE)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "result": b }))
    })
}

/// One activity. The schedule starts this workflow.
fn tick_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if let Some(tag) = ctx.state::<Arc<WorkerTag>>() {
            tag.body_starts.fetch_add(1, Ordering::SeqCst);
        }
        let b = ctx
            .execute_activity_raw(ADD, json!({ "x": input["x"] }), QUEUE)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "result": b }))
    })
}

/// The output of chain `i`: `a = i + 1`, `v = 10 i`, `b = a + v + 1`.
fn chain_output(i: usize) -> Value {
    let i = i64::try_from(i).unwrap_or(0);
    json!({ "result": 11 * i + 2 })
}

fn workflow_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        name,
        module: "dst_world_tests",
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
        mcp: false,
    }
}

fn add_info() -> ActivityInfo {
    ActivityInfo {
        name: ADD,
        module: "dst_world_tests",
        default_retry_policy: None,
        // Whole ticks, so the clock decides every deadline.
        default_start_to_close: Some(Duration::from_secs(TICK_SECS)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        default_schedule_to_close: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: add_activity,
    }
}

/// Counts the cache hits and misses of one worker.
#[derive(Debug, Default)]
struct CacheCounts {
    hits: AtomicU64,
    misses: AtomicU64,
}

impl MetricsRecorder for CacheCounts {
    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.hits.fetch_add(1, Ordering::SeqCst);
    }

    fn record_workflow_cache_miss(&self, _workflow_name: &str, _queue: &str) {
        self.misses.fetch_add(1, Ordering::SeqCst);
    }
}

// ── Databases ───────────────────────────────────────────────────────────────

/// `url` with its database name replaced by `name`.
fn with_database(url: &str, name: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let root = base.rsplit_once('/').map_or(base, |(root, _)| root);
    query.map_or_else(
        || format!("{root}/{name}"),
        |query| format!("{root}/{name}?{query}"),
    )
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .unwrap_or_else(|error| panic!("connect to {url}: {error}"))
}

/// Creates one fresh database per world run, from a migrated template.
struct Databases {
    admin_url: String,
    template: String,
    created: Mutex<Vec<String>>,
}

impl Databases {
    /// Build the template on the server of `url`.
    ///
    /// The template name holds a hash of the migrations, so a template from
    /// an older schema is never used.
    async fn new(url: &str) -> Self {
        let init = autumn_harvest::test_init_sql();
        let hash = init.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        });
        let template = format!("dst_world_tmpl_{hash:016x}");
        let admin_url = with_database(url, "postgres");
        let mut admin = connect(&admin_url).await;
        // The lock stops two test processes from building one template.
        admin
            .batch_execute("SELECT pg_advisory_lock(2002)")
            .await
            .expect("lock");
        if !database_exists(&mut admin, &template).await {
            let building = format!("{template}_build");
            admin
                .batch_execute(&format!("DROP DATABASE IF EXISTS {building} WITH (FORCE)"))
                .await
                .expect("drop a half-built template");
            admin
                .batch_execute(&format!("CREATE DATABASE {building}"))
                .await
                .expect("create the template");
            let mut conn = connect(&with_database(url, &building)).await;
            conn.batch_execute(&init)
                .await
                .expect("migrate the template");
            drop(conn);
            admin
                .batch_execute(&format!("ALTER DATABASE {building} RENAME TO {template}"))
                .await
                .expect("publish the template");
        }
        admin
            .batch_execute("SELECT pg_advisory_unlock(2002)")
            .await
            .expect("unlock");
        Self {
            admin_url,
            template,
            created: Mutex::new(Vec::new()),
        }
    }

    /// A fresh database URL. It drops the databases of earlier runs first.
    async fn fresh(&self, url: &str) -> String {
        self.drop_created().await;
        let name = format!("dst_world_{}", uuid::Uuid::new_v4().simple());
        let mut admin = connect(&self.admin_url).await;
        admin
            .batch_execute(&format!(
                "CREATE DATABASE {name} TEMPLATE {}",
                self.template
            ))
            .await
            .expect("clone the template");
        self.created.lock().expect("lock").push(name.clone());
        with_database(url, &name)
    }

    /// Drop every database that this value created.
    async fn drop_created(&self) {
        let names: Vec<String> = std::mem::take(&mut *self.created.lock().expect("lock"));
        if names.is_empty() || std::env::var(KEEP_VAR).is_ok() {
            return;
        }
        let mut admin = connect(&self.admin_url).await;
        for name in names {
            admin
                .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                .await
                .expect("drop a world database");
        }
    }
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn database_exists(admin: &mut AsyncPgConnection, name: &str) -> bool {
    let row: Count = diesel::sql_query("SELECT count(*) AS n FROM pg_database WHERE datname = $1")
        .bind::<Text, _>(name)
        .get_result(admin)
        .await
        .expect("read pg_database");
    row.n > 0
}

#[derive(QueryableByName)]
struct InstantColumn {
    #[diesel(sql_type = Text)]
    table_name: String,
    #[diesel(sql_type = Text)]
    column_name: String,
}

/// One `UPDATE` per table that moves every instant back by one tick.
async fn warp_statements(conn: &mut AsyncPgConnection) -> Vec<String> {
    let columns: Vec<InstantColumn> = diesel::sql_query(
        "SELECT c.table_name::text AS table_name, c.column_name::text AS column_name \
         FROM information_schema.columns c \
         JOIN information_schema.tables t \
           ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
         WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE' \
           AND c.data_type = 'timestamp with time zone' \
           AND c.table_name LIKE 'harvest\\_%' AND c.table_name NOT LIKE $1 \
         ORDER BY c.table_name, c.column_name",
    )
    .bind::<Text, _>(SKIPPED_TABLES)
    .load(conn)
    .await
    .expect("list the instant columns");
    let mut tables: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for column in columns {
        tables
            .entry(column.table_name)
            .or_default()
            .push(column.column_name);
    }
    tables
        .into_iter()
        .map(|(table, columns)| {
            let sets: Vec<String> = columns
                .iter()
                .map(|c| format!("\"{c}\" = \"{c}\" - INTERVAL '{WARP_MS} milliseconds'"))
                .collect();
            format!("UPDATE {table} SET {}", sets.join(", "))
        })
        .collect()
}

// ── The world ───────────────────────────────────────────────────────────────

/// One live worker process.
struct SimWorker {
    worker: Worker,
    pool: DbPool,
    counts: Arc<CacheCounts>,
    tag: Arc<WorkerTag>,
}

/// A world of real workers on one fresh database.
struct PgWorld {
    url: String,
    plant: bool,
    conn: AsyncPgConnection,
    warp: Vec<String>,
    /// The real time at which tick 0 began.
    t0: DateTime<Utc>,
    /// The ticks that the clock has moved.
    ticks: u64,
    workers: Vec<Option<SimWorker>>,
    incarnation: Vec<u32>,
    chains: Vec<ExecutionId>,
    labels: BTreeMap<uuid::Uuid, String>,
    held: BTreeMap<usize, HarvestSchedule>,
}

impl PgWorld {
    async fn new(databases: &Databases, base_url: &str, config: &WorldConfig) -> Self {
        let url = databases.fresh(base_url).await;
        let mut conn = connect(&url).await;
        let warp = warp_statements(&mut conn).await;
        let t0 = Utc::now();
        let mut chains = Vec::new();
        let mut labels = BTreeMap::new();
        for i in 0..config.workflows {
            let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
            let workflow_id = format!("dst-chain-{i}");
            autumn_harvest::start_or_load_workflow_execution(
                &mut conn,
                start_params(CHAIN, exec_id, &workflow_id, json!({ "i": i })),
                None,
            )
            .await
            .expect("start a chain");
            labels.insert(exec_id.as_uuid(), format!("c{i}"));
            chains.push(exec_id);
        }
        let schedule = WorkflowSchedule::new(
            TICKER,
            Schedule::Interval(Duration::from_secs(2 * TICK_SECS)),
        )
        .with_input(json!({ "x": 100 }))
        .with_queue_name(QUEUE)
        .with_max_active_runs(10)
        .with_max_runs(3);
        autumn_harvest::scheduler::register_workflow_schedules(&mut conn, &[schedule])
            .await
            .expect("register the schedule");
        let mut world = Self {
            url,
            plant: config.plant == Plant::ForeignState,
            conn,
            warp,
            t0,
            ticks: 0,
            workers: Vec::new(),
            incarnation: vec![1; config.workers],
            chains,
            labels,
            held: BTreeMap::new(),
        };
        for w in 0..config.workers {
            let worker = world.start_worker(w).await;
            world.workers.push(Some(worker));
        }
        world
    }

    async fn start_worker(&self, w: usize) -> SimWorker {
        let id = format!("w{}.{}", w + 1, self.incarnation[w]);
        let counts = Arc::new(CacheCounts::default());
        let tag = Arc::new(WorkerTag {
            index: w,
            plant: self.plant,
            body_starts: AtomicU64::new(0),
        });
        let mut state = SharedStateMap::new();
        state.insert(
            std::any::TypeId::of::<Arc<WorkerTag>>(),
            Box::new(Arc::clone(&tag)) as Box<dyn std::any::Any + Send + Sync>,
        );
        let telemetry = Arc::new(
            TelemetryConfig::builder()
                .metrics(Arc::clone(&counts) as Arc<dyn MetricsRecorder>)
                .build(),
        );
        let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
            vec![
                workflow_info(CHAIN, chain_workflow),
                workflow_info(TICKER, tick_workflow),
            ],
            vec![add_info()],
            Arc::new(state),
            telemetry,
        ));
        let mut config = runtime_config(&id, 1, 1, Duration::from_secs(TICK_SECS));
        config.queues = vec![QUEUE.to_string()];
        config.sticky_timeout = Duration::from_secs(TICK_SECS);
        config.workflow_cache_size = 16;
        config.worker_heartbeat_interval = Duration::from_secs(TICK_SECS);
        let worker = Worker::new(config, registry).expect("build a worker");
        let pool = build_test_pool(&self.url);
        assert!(worker.dst_register(&pool).await, "register {id}");
        SimWorker {
            worker,
            pool,
            counts,
            tag,
        }
    }

    async fn poll(&mut self, w: usize) -> Effect {
        let Some(sim) = self.workers[w].as_ref() else {
            return Effect::Idle;
        };
        let hits = AtomicU64::load(&sim.counts.hits, Ordering::SeqCst);
        let misses = AtomicU64::load(&sim.counts.misses, Ordering::SeqCst);
        let starts = AtomicU64::load(&sim.tag.body_starts, Ordering::SeqCst);
        if !sim.worker.dst_poll_once(&sim.pool).await {
            return Effect::Idle;
        }
        let hit = AtomicU64::load(&sim.counts.hits, Ordering::SeqCst) > hits;
        let miss = AtomicU64::load(&sim.counts.misses, Ordering::SeqCst) > misses;
        let restarted = AtomicU64::load(&sim.tag.body_starts, Ordering::SeqCst) > starts;
        let decision = match (hit, miss, restarted) {
            (_, true, _) => Some(Decision::Cold),
            (true, false, false) => Some(Decision::Warm),
            (true, false, true) => Some(Decision::Declined),
            (false, false, _) => None,
        };
        Effect::Polled(decision)
    }

    async fn beat(&mut self, w: usize) -> Effect {
        let id = format!("w{}.{}", w + 1, self.incarnation[w]);
        autumn_harvest::workers::heartbeat_worker(&mut self.conn, &id, 0, &json!({}), 0, &[])
            .await
            .expect("beat");
        Effect::Done
    }

    async fn advance(&mut self) -> Effect {
        self.ticks += 1;
        let script = self.warp.join(";\n");
        self.conn
            .batch_execute(&format!("BEGIN;\n{script};\nCOMMIT;"))
            .await
            .expect("shift every instant back");
        Effect::Done
    }

    /// The tick of a real instant that the clock has shifted.
    fn tick_of(&self, at: DateTime<Utc>) -> u64 {
        let tick = i64::try_from(TICK_SECS * 1_000).unwrap_or(i64::MAX);
        let ticks = i64::try_from(self.ticks).unwrap_or(0);
        let shifted = (at - self.t0).num_milliseconds() + WARP_MS * ticks;
        u64::try_from((shifted + tick / 2).div_euclid(tick)).unwrap_or(0)
    }

    /// The due schedules, by the filter of `tick_workflow_schedules`.
    async fn scan(&mut self, s: usize) -> Effect {
        use harvest_schedules::dsl;
        let due: Vec<HarvestSchedule> = dsl::harvest_schedules
            .filter(dsl::workflow_name.is_not_null())
            .filter(dsl::is_paused.eq(false))
            .filter(dsl::auto_paused_at.is_null())
            .filter(dsl::exhausted_at.is_null())
            .filter(dsl::next_run_at.is_not_null())
            .filter(dsl::next_run_at.le(Utc::now()))
            .order(dsl::next_run_at.asc())
            .select(HarvestSchedule::as_select())
            .load(&mut self.conn)
            .await
            .expect("scan the schedules");
        let count = due.len();
        if let Some(first) = due.into_iter().next() {
            self.held.insert(s, first);
        } else {
            self.held.remove(&s);
        }
        Effect::Scanned(count)
    }

    /// Run the production fire claim on the snapshot of the last scan.
    async fn fire(&mut self, s: usize) -> Effect {
        let Some(snapshot) = self.held.remove(&s) else {
            return Effect::Fired(None);
        };
        let slot = snapshot.next_run_at.map(|at| self.tick_of(at));
        let before = runs_started(&mut self.conn, snapshot.id).await;
        let registry = HandlerRegistry::new(
            vec![
                workflow_info(CHAIN, chain_workflow),
                workflow_info(TICKER, tick_workflow),
            ],
            vec![add_info()],
        );
        let metrics: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        autumn_harvest::scheduler::claim_and_fire_workflow_schedule(
            &mut self.conn,
            &snapshot,
            Utc::now(),
            ShardId::new(0),
            &autumn_harvest::scheduler::DagCatalog::new(),
            &registry,
            &metrics,
            &[],
        )
        .await
        .expect("fire claim");
        let after = runs_started(&mut self.conn, snapshot.id).await;
        Effect::Fired(if after > before { slot } else { None })
    }

    async fn reclaim(&mut self, stale_ticks: u64) -> Effect {
        let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
            &mut self.conn,
            3,
            i64::try_from(stale_ticks * TICK_SECS).unwrap_or(i64::MAX),
            None,
            &NoOpMetrics,
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
        )
        .await
        .expect("reclaim");
        Effect::Reclaimed(summary.requeued)
    }

    async fn sweep(&mut self, stale_ticks: u64) -> Effect {
        let rows = autumn_harvest::timeout::enforce_timeouts_once(
            &mut self.conn,
            &NoOpMetrics,
            Duration::from_secs(TICK_SECS),
            &None,
            &[ShardId::new(0)],
            None,
            None,
            i64::try_from(stale_ticks * TICK_SECS).unwrap_or(i64::MAX),
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
            0,
        )
        .await
        .expect("sweep");
        Effect::Swept(rows)
    }

    async fn signal(&mut self, i: usize) -> Effect {
        let v = i64::try_from(i).unwrap_or(0) * 10;
        autumn_harvest::signal::send_signal(
            &mut self.conn,
            self.chains[i],
            "go",
            json!({ "v": v }),
        )
        .await
        .expect("send a signal");
        Effect::Done
    }

    async fn read_snapshot(&mut self) -> Snapshot {
        use harvest_workflow_executions::dsl;
        let rows: Vec<(
            uuid::Uuid,
            String,
            Option<DateTime<Utc>>,
            Option<uuid::Uuid>,
        )> = dsl::harvest_workflow_executions
            .order((dsl::started_at.asc(), dsl::id.asc()))
            .select((dsl::id, dsl::state, dsl::nd_blocked_at, dsl::schedule_id))
            .load(&mut self.conn)
            .await
            .expect("read executions");
        let mut executions = Vec::new();
        for (id, status, blocked, schedule_id) in rows {
            let next = self
                .labels
                .values()
                .filter(|label| label.starts_with('s'))
                .count();
            let label = self
                .labels
                .entry(id)
                .or_insert_with(|| format!("s{next}"))
                .clone();
            let expected = match label.strip_prefix('c') {
                Some(i) => chain_output(i.parse().unwrap_or(0)),
                None => json!({ "result": 101 }),
            };
            debug_assert!(label.starts_with('c') || schedule_id.is_some());
            let history =
                autumn_harvest::store::load_history(&mut self.conn, ExecutionId::from_uuid(id))
                    .await
                    .expect("load a history");
            executions.push(ExecFacts {
                label,
                status,
                blocked: blocked.is_some(),
                expected: Some(expected),
                events: Fact::from_events(&history.events),
            });
        }
        executions.sort_by(|a, b| a.label.cmp(&b.label));
        let schedules_done = harvest_schedules::table
            .filter(harvest_schedules::exhausted_at.is_null())
            .count()
            .get_result::<i64>(&mut self.conn)
            .await
            .expect("read the schedules")
            == 0;
        Snapshot {
            executions,
            schedules_done,
        }
    }
}

async fn runs_started(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> i32 {
    harvest_schedules::table
        .find(id)
        .select(harvest_schedules::runs_started)
        .first(conn)
        .await
        .expect("read runs_started")
}

/// A world plus the config values that its actions read.
struct Bound {
    world: PgWorld,
    stale_ticks: u64,
}

impl World for Bound {
    async fn apply(&mut self, action: WorldAction, _now_tick: u64) -> Effect {
        let world = &mut self.world;
        match action {
            WorldAction::Advance => world.advance().await,
            WorldAction::Beat(w) => world.beat(w).await,
            WorldAction::Poll(w) => world.poll(w).await,
            WorldAction::Stall(_) => Effect::Done,
            WorldAction::Crash(w) => {
                if let Some(sim) = world.workers[w].take() {
                    sim.worker.shutdown();
                }
                Effect::Done
            }
            WorldAction::Restart(w) => {
                world.incarnation[w] += 1;
                let sim = world.start_worker(w).await;
                world.workers[w] = Some(sim);
                Effect::Done
            }
            WorldAction::Signal(i) => world.signal(i).await,
            WorldAction::ScheduleScan(s) => world.scan(s).await,
            WorldAction::ScheduleFire(s) => world.fire(s).await,
            WorldAction::Reclaim => world.reclaim(self.stale_ticks).await,
            WorldAction::Sweep => world.sweep(self.stale_ticks).await,
        }
    }

    async fn snapshot(&mut self) -> Snapshot {
        self.world.read_snapshot().await
    }
}

fn start_params<'a>(
    workflow_name: &'a str,
    exec_id: ExecutionId,
    workflow_id: &'a str,
    input: Value,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
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
        priority: autumn_harvest::types::Priority::default(),
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    }
}

/// A server, a template, and a way to make one world per run.
struct Harness {
    url: String,
    databases: Databases,
    _container: Option<testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>>,
}

impl Harness {
    async fn new() -> Self {
        let (url, container) = setup_test_database_url_or_env().await;
        let databases = Databases::new(&url).await;
        Self {
            url,
            databases,
            _container: container,
        }
    }

    async fn world(&self, config: &WorldConfig) -> Bound {
        Bound {
            world: PgWorld::new(&self.databases, &self.url, config).await,
            stale_ticks: config.stale_ticks,
        }
    }

    async fn run_twice(&self, config: &WorldConfig) -> WorldReport {
        let report = world::run_twice(config, async || self.world(config).await)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        self.databases.drop_created().await;
        report
    }

    async fn sweep(
        &self,
        plan: &SeedPlan,
        config: impl Fn(u64) -> WorldConfig,
    ) -> Result<world::WorldSweepSummary, Box<world::WorldSweepFailure>> {
        let result = world::sweep(plan, config, async |config: &WorldConfig| {
            self.world(config).await
        })
        .await;
        self.databases.drop_created().await;
        result
    }
}

/// The first seed of `PLANT_SEEDS` that the plant fails.
async fn first_planted_failure(harness: &Harness) -> Box<world::WorldSweepFailure> {
    harness
        .sweep(&PLANT_SEEDS, |seed| {
            WorldConfig::new(seed).with_plant(Plant::ForeignState)
        })
        .await
        .expect_err("the plant fails a seed within PLANT_SEEDS")
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn with_database_swaps_only_the_name() {
    assert_eq!(
        with_database("postgres://u:p@h:5432/postgres", "x"),
        "postgres://u:p@h:5432/x"
    );
    assert_eq!(
        with_database("postgres://u:p@h/db?sslmode=disable", "x"),
        "postgres://u:p@h/x?sslmode=disable"
    );
}

/// The hook runs one claim and the whole task body before it returns.
#[tokio::test]
async fn dst_poll_once_runs_the_claimed_task_to_completion() {
    let harness = Harness::new().await;
    let config = WorldConfig {
        workflows: 1,
        workers: 1,
        ..WorldConfig::new(0)
    };
    let mut bound = harness.world(&config).await;
    let world = &mut bound.world;

    assert_eq!(world.poll(0).await, Effect::Polled(Some(Decision::Cold)));
    let after_decision = world.read_snapshot().await;
    let chain = &after_decision.executions[0];
    assert_eq!(
        chain.events.last(),
        Some(&Fact::ActivityScheduled {
            activity: 0,
            name: ADD.to_string()
        }),
        "the decision committed before the hook returned: {:?}",
        chain.events
    );

    assert_eq!(world.poll(0).await, Effect::Polled(None), "the activity");
    let after_activity = world.read_snapshot().await;
    assert!(
        after_activity.executions[0]
            .events
            .contains(&Fact::ActivityCompleted { activity: 0 }),
        "{:?}",
        after_activity.executions[0].events
    );

    // The next decision resumes the parked workflow on the same worker.
    assert_eq!(world.poll(0).await, Effect::Polled(Some(Decision::Warm)));
    let timer = world.read_snapshot().await.executions[0].events.clone();
    assert!(
        timer.contains(&Fact::TimerStarted {
            timer: "nap".to_string(),
            secs: TICK_SECS
        }),
        "{timer:?}"
    );

    // The timer is not due before the clock moves one tick.
    assert_eq!(world.poll(0).await, Effect::Idle);
    world.advance().await;
    assert_eq!(world.poll(0).await, Effect::Polled(Some(Decision::Warm)));
    let fired = world.read_snapshot().await.executions[0].events.clone();
    assert!(
        fired.contains(&Fact::TimerFired {
            timer: "nap".to_string()
        }),
        "{fired:?}"
    );
    drop(bound);
    harness.databases.drop_created().await;
}

/// Two runs of one seed, each on a fresh database, give equal reports.
#[tokio::test]
async fn a_world_seed_runs_twice_with_equal_traces() {
    let harness = Harness::new().await;
    let report = harness.run_twice(&WorldConfig::new(0)).await;
    assert_eq!(report.violation, None, "{}", report.trace_tail(40));
    assert!(report.converged, "{}", report.trace_tail(40));
}

/// The scope of issue #2002 runs: every action and every resident outcome.
#[tokio::test]
async fn a_world_sweep_covers_the_scope() {
    let harness = Harness::new().await;
    let plan = SeedPlan { first: 0, count: 6 };
    let summary = harness
        .sweep(&plan, WorldConfig::new)
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    let stats = summary.stats;
    assert_eq!(summary.seeds, 6);
    assert!(
        stats.cold > 0 && stats.warm > 0 && stats.declined > 0,
        "{stats:?}"
    );
    assert!(stats.activities > 0 && stats.idle_polls > 0, "{stats:?}");
    assert!(stats.timers_fired > 0 && stats.signals > 0, "{stats:?}");
    assert!(stats.fires > 0 && stats.lost_fires > 0, "{stats:?}");
    assert!(stats.stalls > 0 && stats.crashes > 0, "{stats:?}");
}

/// The plant fails a seed. The seed alone replays the same failure.
#[tokio::test]
async fn a_planted_failure_replays_from_its_seed_alone() {
    let harness = Harness::new().await;
    let failure = first_planted_failure(&harness).await;
    assert!(
        failure.reason.starts_with("Deterministic"),
        "the plant breaks determinism: {failure}"
    );
    let seed = failure.config.seed;
    assert_eq!(
        seed, 0,
        "docs/testing/simulation.md and the changelog fragment name seed 0; update both"
    );

    // The replay command names every variable that the config reads.
    let command = world::repro_command(&failure.config);
    assert!(
        command.contains(&format!("HARVEST_DST_SEED={seed} ")),
        "{command}"
    );
    assert!(
        command.contains("HARVEST_DST_WORLD_PLANT=foreign-state"),
        "{command}"
    );

    // A config from those variables alone gives the same violation.
    let checks: Vec<&str> = failure.config.checks.iter().map(|c| c.name()).collect();
    let replay = world::config_from_vars(seed, Some("foreign-state"), Some(&checks.join(",")))
        .expect("valid variables");
    assert_eq!(replay, failure.config);
    let report = harness.run_twice(&replay).await;
    let violation = report.violation.clone().expect("the replay fails too");
    assert_eq!(violation.invariant, WorldInvariant::Deterministic);
    assert_eq!(failure.reason, violation.to_string());
    assert_eq!(
        failure.trace_tail,
        report.trace_tail(autumn_harvest::dst::TAIL_LINES)
    );

    // The same seed with no plant passes.
    let clean = harness.run_twice(&WorldConfig::new(seed)).await;
    assert_eq!(clean.violation, None, "{}", clean.trace_tail(40));
}

/// The sweep that CI and the nightly job run.
///
/// `HARVEST_DST_SEEDS` and `HARVEST_DST_SEED_BASE` pick the seeds.
/// `HARVEST_DST_SEED` runs one seed. `HARVEST_DST_WORLD_PLANT` and
/// `HARVEST_DST_WORLD_CHECKS` change the config.
#[tokio::test]
async fn world_seed_sweep() {
    let plan = SeedPlan::from_env(DEFAULT_SEEDS).unwrap_or_else(|error| panic!("{error}"));
    let template = world::config_from_env(0).unwrap_or_else(|error| panic!("{error}"));
    let harness = Harness::new().await;
    let summary = harness
        .sweep(&plan, |seed| WorldConfig {
            seed,
            ..template.clone()
        })
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(summary.seeds, plan.count, "every planned seed ran");
    println!(
        "dst world: {} seeds from {} passed, plant {}: {:?}",
        summary.seeds,
        plan.first,
        template.plant.as_str(),
        summary.stats
    );
}

/// Print the full trace of the seed in `HARVEST_DST_SEED`.
///
/// The seed runs twice, as in a sweep. Without that variable, the test does
/// nothing.
#[tokio::test]
async fn replay_one_world_seed() {
    let Ok(text) = std::env::var(autumn_harvest::dst::SEED_VAR) else {
        return;
    };
    let plan = SeedPlan::parse(Some(&text), None, None, 1).unwrap_or_else(|e| panic!("{e}"));
    let config = world::config_from_env(plan.first).unwrap_or_else(|error| panic!("{error}"));
    let harness = Harness::new().await;
    let report = harness.run_twice(&config).await;
    for line in &report.trace {
        println!("{line}");
    }
    if let Some(violation) = report.violation {
        panic!("seed {}: {violation}", plan.first);
    }
}
