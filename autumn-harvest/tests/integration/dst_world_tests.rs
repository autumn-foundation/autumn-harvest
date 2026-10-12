#![cfg(feature = "db")]
//! The world simulation on Postgres (issue #2002).
//!
//! `PgWorld` implements `dst::world::World` with real `Worker`s. Each poll
//! runs one iteration of the `worker.rs` poll loop to completion. The seed
//! picks every action, so a failing seed replays exactly.
//!
//! The scope is the claim, the decision, the resident path, timers,
//! signals, the scheduler fire claim, the orphan reclaimer and the timeout
//! sweeper. Faults are worker stalls, crashes and abandoned claims.
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
use std::fmt::Write as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::context::SharedStateMap;
use autumn_harvest::dst::world::{
    self, Effect, ExecFacts, Fact, Plant, Ran, Snapshot, TICK_SECS, World, WorldAction,
    WorldConfig, WorldInvariant, WorldReport,
};
use autumn_harvest::dst::{SeedPlan, TAIL_LINES};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::HarvestSchedule;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::schema::{harvest_schedules, harvest_workflow_executions};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{ActivityContext, StartSource, StartWorkflowParams, WorkflowContext};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};

use crate::integration_e2e::{build_test_pool, runtime_config, setup_test_database_url_or_env};
use crate::throwaway_db::{ThrowawayDb, with_database};

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

/// One tick of the world clock, in milliseconds.
const TICK_MS: i64 = 86_400_000;

/// Each step moves the clock one minute.
///
/// Two writes in two steps are then at least one minute apart. A shorter
/// engine duration thus compares by the seed, not by the speed of the host.
/// An example is the claim handicap of a new start.
const STEP_MS: i64 = 60_000;

/// One advance moves the clock one tick plus 1 ms.
///
/// The engine builds ids from instants, such as the workflow id of a
/// schedule slot. The extra millisecond keeps the stored instants of two
/// slots apart. A deadline of whole ticks still comes due after the same
/// number of advances.
const WARP_MS: i64 = TICK_MS + 1;

/// The steps of one run must move the clock less than one tick.
const MAX_STEPS: usize = 1_000;

/// A step that takes longer than this in real time voids the clock
/// argument. The run then stops as a harness error. A step runs from one
/// action to the next, so it includes the snapshot between them.
const MAX_STEP_REAL: Duration = Duration::from_secs(20);

/// The orphan reclaimer quarantines a task at this many strikes. The
/// workload does not cover quarantine, so the value is out of reach.
const QUARANTINE_STRIKES: i32 = 1_000;

/// The tables whose instants the clock does not shift.
///
/// `harvest_events` is append-only. The sweeper reads its timestamps only
/// for external signals and awaits, which this workload does not use.
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
    /// Activity bodies that ran on this worker.
    activities: AtomicU64,
}

/// `{"x": n}` becomes `n + 1`.
fn add_activity<'a>(
    ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if let Some(tag) = ctx.state::<Arc<WorkerTag>>() {
            tag.activities.fetch_add(1, Ordering::SeqCst);
        }
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

/// Two activities at once. The schedule starts this workflow.
///
/// The join stays resident (issue #2008). A decision that reads one of the
/// two results resumes it and parks the other.
fn tick_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if let Some(tag) = ctx.state::<Arc<WorkerTag>>() {
            tag.body_starts.fetch_add(1, Ordering::SeqCst);
        }
        let x = input["x"].as_i64().ok_or("missing x")?;
        let (b, c) = futures::join!(
            ctx.execute_activity_raw(ADD, json!({ "x": x }), QUEUE),
            ctx.execute_activity_raw(ADD, json!({ "x": x + 10 }), QUEUE),
        );
        let b = b.map_err(|e| e.to_string())?.as_i64().ok_or("bad b")?;
        let c = c.map_err(|e| e.to_string())?.as_i64().ok_or("bad c")?;
        Ok(json!({ "result": b + c }))
    })
}

/// The output of a scheduled run: `(100 + 1) + (110 + 1)`.
const TICK_OUTPUT: i64 = 212;

/// The output of chain `i`: `a = i + 1`, `v = 10 i`, `b = a + v + 1`.
fn chain_output(i: usize) -> Value {
    let i = i64::try_from(i).expect("a small chain index");
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

fn workflows() -> Vec<WorkflowInfo> {
    vec![
        workflow_info(CHAIN, chain_workflow),
        workflow_info(TICKER, tick_workflow),
    ]
}

fn add_info() -> ActivityInfo {
    let tick = Duration::from_secs(TICK_SECS);
    ActivityInfo {
        name: ADD,
        module: "dst_world_tests",
        // A retry waits one whole tick, with no jitter. A jittered delay
        // comes from a random task id, so it would differ between runs.
        default_retry_policy: Some(RetryPolicy {
            max_attempts: 100,
            initial_interval: tick,
            backoff_coefficient: 1.0,
            max_interval: tick,
            non_retryable_errors: Vec::new(),
            jitter: JitterPolicy::None,
        }),
        // Whole ticks, so the clock decides every deadline.
        default_start_to_close: Some(tick),
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

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .unwrap_or_else(|error| panic!("connect to {url}: {error}"))
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// Turn off autovacuum and autoanalyze on every table.
///
/// They run at real-time moments that the seed does not pick. A vacuum moves
/// rows in the heap, and an analyze can change a plan. A query with no
/// `ORDER BY`, such as the orphan scan, then returns rows in another order.
/// The claim order then differs between two runs of one seed.
const NO_AUTOVACUUM: &str = "DO $$ DECLARE r record; BEGIN \
     FOR r IN SELECT c.oid::regclass AS t FROM pg_class c \
       JOIN pg_namespace n ON n.oid = c.relnamespace \
       WHERE n.nspname = 'public' AND c.relkind = 'r' LOOP \
       EXECUTE format('ALTER TABLE %s SET (autovacuum_enabled = false, \
         toast.autovacuum_enabled = false)', r.t); \
     END LOOP; END $$";

/// A migrated template database on the server of `admin_url`. Each world
/// run clones it.
///
/// The name holds a hash of the migrations and of [`NO_AUTOVACUUM`], so a
/// template from an older schema is never used. An advisory lock stops two
/// test processes from building one template.
async fn template_database(admin_url: &str) -> String {
    let init = format!("{};\n{NO_AUTOVACUUM}", autumn_harvest::test_init_sql());
    let hash = init.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    });
    let template = format!("dst_world_tmpl_{hash:016x}");
    let mut admin = connect(admin_url).await;
    admin
        .batch_execute("SELECT pg_advisory_lock(2002)")
        .await
        .expect("lock");
    let exists: Count =
        diesel::sql_query("SELECT count(*) AS n FROM pg_database WHERE datname = $1")
            .bind::<Text, _>(&template)
            .get_result(&mut admin)
            .await
            .expect("read pg_database");
    if exists.n == 0 {
        let building = format!("{template}_build");
        for sql in [
            format!("DROP DATABASE IF EXISTS {building}"),
            format!("CREATE DATABASE {building}"),
        ] {
            admin
                .batch_execute(&sql)
                .await
                .expect("prepare the template");
        }
        let mut conn = connect(&with_database(admin_url, &building)).await;
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
    template
}

#[derive(QueryableByName)]
struct InstantColumn {
    #[diesel(sql_type = Text)]
    table_name: String,
    #[diesel(sql_type = Text)]
    column_name: String,
}

/// Every `timestamptz` column of each `harvest_*` table, by table.
async fn instant_columns(conn: &mut AsyncPgConnection) -> Vec<(String, Vec<String>)> {
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
    tables.into_iter().collect()
}

// ── The world ───────────────────────────────────────────────────────────────

/// One live worker process.
struct SimWorker {
    worker: Worker,
    pool: DbPool,
    counts: Arc<CacheCounts>,
    tag: Arc<WorkerTag>,
}

/// A schedule row that a scan read, with what the scan knew.
struct Held {
    schedule: HarvestSchedule,
    /// The tick of the slot.
    slot: u64,
    /// The clock shift at the scan.
    shift_ms: i64,
}

/// A world of real workers on one fresh database.
struct PgWorld {
    db: Option<ThrowawayDb>,
    url: String,
    plant: bool,
    stale_secs: i64,
    conn: AsyncPgConnection,
    columns: Vec<(String, Vec<String>)>,
    /// The real time at which the clock began.
    t0: DateTime<Utc>,
    /// The total clock shift so far.
    shift_ms: i64,
    workers: Vec<Option<SimWorker>>,
    incarnation: Vec<u32>,
    chains: Vec<ExecutionId>,
    /// The label and the expected output of each execution.
    labels: BTreeMap<uuid::Uuid, (String, Value)>,
    scheduled: usize,
    held: BTreeMap<usize, Held>,
    registry: HandlerRegistry,
    /// The real time at which the last action began.
    step_started: Option<Instant>,
}

impl PgWorld {
    async fn new(admin_url: &str, template: &str, config: &WorldConfig) -> Self {
        assert!(
            config.fault_steps + config.drain_steps <= MAX_STEPS,
            "the steps of one run must move the clock less than one tick"
        );
        let db = ThrowawayDb::clone_on(admin_url, "dst_world", template).await;
        let url = db.url();
        let mut conn = connect(&url).await;
        let columns = instant_columns(&mut conn).await;
        let t0 = Utc::now();
        let mut chains = Vec::new();
        let mut labels = BTreeMap::new();
        for i in 0..config.workflows {
            let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
            let workflow_id = format!("dst-chain-{i}");
            let params = StartWorkflowParams {
                start_source: StartSource::Api,
                ..StartWorkflowParams::new(CHAIN, &workflow_id, exec_id, json!({ "i": i }), QUEUE)
            };
            autumn_harvest::start_or_load_workflow_execution(&mut conn, params, None)
                .await
                .expect("start a chain");
            labels.insert(exec_id.as_uuid(), (format!("c{i}"), chain_output(i)));
            chains.push(exec_id);
        }
        let every = Duration::from_secs(2 * TICK_SECS);
        let schedule = WorkflowSchedule::new(TICKER, Schedule::Interval(every))
            .with_input(json!({ "x": 100 }))
            .with_queue_name(QUEUE)
            .with_max_active_runs(10)
            .with_max_runs(3);
        autumn_harvest::scheduler::register_workflow_schedules(&mut conn, &[schedule])
            .await
            .expect("register the schedule");
        let mut world = Self {
            db: Some(db),
            url,
            plant: config.plant == Plant::ForeignState,
            stale_secs: i64::try_from(config.stale_ticks * TICK_SECS).expect("a small stale time"),
            conn,
            columns,
            t0,
            shift_ms: 0,
            workers: Vec::new(),
            incarnation: vec![1; config.workers],
            chains,
            labels,
            scheduled: 0,
            held: BTreeMap::new(),
            registry: HandlerRegistry::new(workflows(), vec![add_info()]),
            step_started: None,
        };
        for w in 0..config.workers {
            let worker = world.start_worker(w).await;
            world.workers.push(Some(worker));
        }
        world
    }

    fn worker_id(&self, w: usize) -> String {
        format!("w{}.{}", w + 1, self.incarnation[w])
    }

    async fn start_worker(&self, w: usize) -> SimWorker {
        let id = self.worker_id(w);
        let counts = Arc::new(CacheCounts::default());
        let tag = Arc::new(WorkerTag {
            index: w,
            plant: self.plant,
            body_starts: AtomicU64::new(0),
            activities: AtomicU64::new(0),
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
            workflows(),
            vec![add_info()],
            Arc::new(state),
            telemetry,
        ));
        let tick = Duration::from_secs(TICK_SECS);
        let mut config = runtime_config(&id, 1, 1, tick);
        config.queues = vec![QUEUE.to_string()];
        config.sticky_timeout = tick;
        config.workflow_cache_size = 16;
        config.worker_heartbeat_interval = tick;
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

    /// Move every stored instant back by `ms`.
    async fn shift(&mut self, ms: i64) {
        let mut script = String::from("BEGIN;\n");
        for (table, columns) in &self.columns {
            let sets: Vec<String> = columns
                .iter()
                .map(|c| format!("\"{c}\" = \"{c}\" - INTERVAL '{ms} milliseconds'"))
                .collect();
            writeln!(script, "UPDATE {table} SET {};", sets.join(", ")).expect("a string write");
        }
        script.push_str("COMMIT;");
        self.conn
            .batch_execute(&script)
            .await
            .expect("shift every instant back");
        self.shift_ms += ms;
    }

    /// The tick that holds a stored instant.
    fn tick_of(&self, at: DateTime<Utc>) -> u64 {
        let virtual_ms = (at - self.t0).num_milliseconds() + self.shift_ms;
        u64::try_from(virtual_ms.div_euclid(TICK_MS)).expect("an instant after the start")
    }

    async fn poll(&self, w: usize) -> Effect {
        let Some(sim) = self.workers[w].as_ref() else {
            return Effect::Idle;
        };
        let read = |counter: &AtomicU64| counter.load(Ordering::SeqCst);
        let before = [
            read(&sim.counts.hits),
            read(&sim.counts.misses),
            read(&sim.tag.body_starts),
            read(&sim.tag.activities),
        ];
        if !sim.worker.dst_poll_once(&sim.pool).await {
            return Effect::Idle;
        }
        let after = [
            read(&sim.counts.hits),
            read(&sim.counts.misses),
            read(&sim.tag.body_starts),
            read(&sim.tag.activities),
        ];
        let grew = |i: usize| after[i] > before[i];
        let ran = match (grew(0), grew(1), grew(2), grew(3)) {
            (_, _, _, true) => Ran::Activity,
            (_, true, _, false) => Ran::Cold,
            (true, false, false, false) => Ran::Warm,
            (true, false, true, false) => Ran::Declined,
            (false, false, _, false) => Ran::Other,
        };
        Effect::Polled(ran)
    }

    async fn beat(&mut self, w: usize) -> Effect {
        let id = self.worker_id(w);
        autumn_harvest::workers::heartbeat_worker(&mut self.conn, &id, 0, &json!({}), 0, &[])
            .await
            .expect("beat");
        Effect::Done
    }

    fn crash(&mut self, w: usize) {
        if let Some(sim) = self.workers[w].take() {
            sim.worker.shutdown();
        }
    }

    /// Claim one task as worker `w`, start it if it is an activity, and
    /// crash before the body runs.
    async fn abandon(&mut self, w: usize) -> Effect {
        if self.workers[w].is_none() {
            return Effect::Idle;
        }
        let id = self.worker_id(w);
        let claimed = autumn_harvest::queue::claim_task(
            &mut self.conn,
            &[QUEUE.to_string()],
            &id,
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim a task");
        self.crash(w);
        let Some(task) = claimed else {
            return Effect::Idle;
        };
        if task.task_type == "activity" {
            let exec_id = ExecutionId::from_uuid(task.workflow_exec_id.expect("an execution"));
            let name = task.activity_name.clone().expect("an activity name");
            autumn_harvest::worker::append_activity_started_for_test(
                &mut self.conn,
                &task,
                exec_id,
                &name,
                &id,
                &autumn_harvest::payload_codec::PayloadCodecs::default(),
            )
            .await
            .expect("start the activity");
        }
        Effect::Done
    }

    /// The due schedules, by the query of the scheduler tick.
    async fn scan(&mut self, s: usize) -> Effect {
        let due = autumn_harvest::scheduler::due_workflow_schedules(&mut self.conn, Utc::now())
            .await
            .expect("scan the schedules");
        let count = due.len();
        match due.into_iter().next() {
            Some(schedule) => {
                let at = schedule.next_run_at.expect("a due row has a next run");
                let held = Held {
                    slot: self.tick_of(at),
                    shift_ms: self.shift_ms,
                    schedule,
                };
                self.held.insert(s, held);
            }
            None => {
                self.held.remove(&s);
            }
        }
        Effect::Scanned(count)
    }

    /// Run the production fire claim on the snapshot of the last scan.
    async fn fire(&mut self, s: usize) -> Effect {
        let Some(Held {
            mut schedule,
            slot,
            shift_ms,
        }) = self.held.remove(&s)
        else {
            return Effect::Fired(None);
        };
        // The clock moved the stored row since the scan, so the snapshot
        // moves with it. The claim compares `next_run_at` exactly.
        let moved = chrono::Duration::milliseconds(self.shift_ms - shift_ms);
        schedule.next_run_at = schedule.next_run_at.map(|at| at - moved);
        let before = runs_started(&mut self.conn, schedule.id).await;
        let metrics: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
        autumn_harvest::scheduler::claim_and_fire_workflow_schedule(
            &mut self.conn,
            &schedule,
            Utc::now(),
            ShardId::new(0),
            &autumn_harvest::scheduler::DagCatalog::new(),
            &self.registry,
            &metrics,
            &[],
        )
        .await
        .expect("fire claim");
        let after = runs_started(&mut self.conn, schedule.id).await;
        Effect::Fired((after > before).then_some(slot))
    }

    async fn reclaim(&mut self) -> Effect {
        let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
            &mut self.conn,
            QUARANTINE_STRIKES,
            self.stale_secs,
            None,
            &NoOpMetrics,
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
        )
        .await
        .expect("reclaim");
        Effect::Reclaimed(u64::try_from(summary.total()).expect("a row count"))
    }

    async fn sweep(&mut self) -> Effect {
        let rows = autumn_harvest::timeout::enforce_timeouts_once(
            &mut self.conn,
            &NoOpMetrics,
            Duration::from_secs(TICK_SECS),
            &None,
            &[ShardId::new(0)],
            None,
            None,
            self.stale_secs,
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
            0,
        )
        .await
        .expect("sweep");
        Effect::Swept(u64::try_from(rows).expect("a row count"))
    }

    async fn signal(&mut self, i: usize) -> Effect {
        let v = i64::try_from(i).expect("a small chain index") * 10;
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
        let rows: Vec<(uuid::Uuid, String, Option<DateTime<Utc>>)> =
            dsl::harvest_workflow_executions
                .order((dsl::started_at.asc(), dsl::id.asc()))
                .select((dsl::id, dsl::state, dsl::nd_blocked_at))
                .load(&mut self.conn)
                .await
                .expect("read executions");
        let mut executions = Vec::new();
        for (id, status, blocked) in rows {
            // Only the schedule starts executions after the start.
            let (label, expected) = self
                .labels
                .entry(id)
                .or_insert_with(|| {
                    self.scheduled += 1;
                    (
                        format!("s{}", self.scheduled - 1),
                        json!({ "result": TICK_OUTPUT }),
                    )
                })
                .clone();
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

impl Drop for PgWorld {
    fn drop(&mut self) {
        if std::env::var(KEEP_VAR).is_ok()
            && let Some(db) = self.db.take()
        {
            println!("kept the world database {}", db.keep());
        }
    }
}

/// Stop the run when one step took `took` of real time, too much for the
/// clock argument.
fn assert_step_time(action: WorldAction, took: Duration) {
    assert!(
        took < MAX_STEP_REAL,
        "a step before {action:?} took {took:?}: a step must take less than \
         {MAX_STEP_REAL:?}, or real time can change a claim order"
    );
}

async fn runs_started(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> i32 {
    harvest_schedules::table
        .find(id)
        .select(harvest_schedules::runs_started)
        .first(conn)
        .await
        .expect("read runs_started")
}

impl World for PgWorld {
    async fn apply(&mut self, action: WorldAction, _now_tick: u64) -> Effect {
        // The last step ran from its action to this one, snapshot included.
        let started = Instant::now();
        if let Some(last) = self.step_started.replace(started) {
            assert_step_time(action, started - last);
        }
        self.shift(STEP_MS).await;
        let effect = match action {
            WorldAction::Advance => {
                self.shift(WARP_MS).await;
                Effect::Done
            }
            WorldAction::Beat(w) => self.beat(w).await,
            WorldAction::Poll(w) => self.poll(w).await,
            WorldAction::Stall(_) => Effect::Done,
            WorldAction::Crash(w) => {
                self.crash(w);
                Effect::Done
            }
            WorldAction::Abandon(w) => self.abandon(w).await,
            WorldAction::Restart(w) => {
                self.incarnation[w] += 1;
                let sim = self.start_worker(w).await;
                self.workers[w] = Some(sim);
                Effect::Done
            }
            WorldAction::Signal(i) => self.signal(i).await,
            WorldAction::ScheduleScan(s) => self.scan(s).await,
            WorldAction::ScheduleFire(s) => self.fire(s).await,
            WorldAction::Reclaim => self.reclaim().await,
            WorldAction::Sweep => self.sweep().await,
        };
        assert_step_time(action, started.elapsed());
        effect
    }

    async fn snapshot(&mut self) -> Snapshot {
        self.read_snapshot().await
    }
}

/// A server and a template, to make one world per run.
struct Harness {
    admin_url: String,
    template: String,
    _container: Option<testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>>,
}

impl Harness {
    async fn new() -> Self {
        let (url, container) = setup_test_database_url_or_env().await;
        let admin_url = with_database(&url, "postgres");
        let template = template_database(&admin_url).await;
        Self {
            admin_url,
            template,
            _container: container,
        }
    }

    async fn world(&self, config: &WorldConfig) -> PgWorld {
        PgWorld::new(&self.admin_url, &self.template, config).await
    }

    async fn run_twice(&self, config: &WorldConfig) -> WorldReport {
        world::run_twice(config, async || self.world(config).await)
            .await
            .unwrap_or_else(|error| panic!("{error}"))
    }

    async fn sweep(
        &self,
        plan: &SeedPlan,
        config: impl Fn(u64) -> WorldConfig,
    ) -> Result<world::WorldSweepSummary, Box<world::WorldSweepFailure>> {
        world::sweep(plan, config, async |config: &WorldConfig| {
            self.world(config).await
        })
        .await
    }

    /// Run `config` twice, as the replay command does, and print the trace.
    async fn replay(&self, config: &WorldConfig) -> WorldReport {
        let report = self.run_twice(config).await;
        for line in &report.trace {
            println!("{line}");
        }
        report
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

/// The hook runs one claim and the whole task body before it returns.
#[tokio::test]
async fn dst_poll_once_runs_the_claimed_task_to_completion() {
    let harness = Harness::new().await;
    let config = WorldConfig {
        workflows: 1,
        workers: 1,
        ..WorldConfig::new(0)
    };
    let mut world = harness.world(&config).await;

    assert_eq!(world.poll(0).await, Effect::Polled(Ran::Cold));
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

    assert_eq!(world.poll(0).await, Effect::Polled(Ran::Activity));
    let after_activity = world.read_snapshot().await;
    assert!(
        after_activity.executions[0]
            .events
            .contains(&Fact::ActivityCompleted { activity: 0 }),
        "{:?}",
        after_activity.executions[0].events
    );

    // The next decision resumes the parked workflow on the same worker.
    assert_eq!(world.poll(0).await, Effect::Polled(Ran::Warm));
    let timer = world.read_snapshot().await.executions[0].events.clone();
    let started = Fact::TimerStarted {
        timer: "nap".to_string(),
        secs: TICK_SECS,
    };
    assert!(timer.contains(&started), "{timer:?}");

    // The timer is not due before the clock moves one tick, however many
    // steps pass.
    for _ in 0..30 {
        assert_eq!(world.apply(WorldAction::Poll(0), 0).await, Effect::Idle);
    }
    world.apply(WorldAction::Advance, 1).await;
    assert_eq!(world.poll(0).await, Effect::Polled(Ran::Warm));
    let fired = world.read_snapshot().await.executions[0].events.clone();
    let fire = Fact::TimerFired {
        timer: "nap".to_string(),
    };
    assert!(fired.contains(&fire), "{fired:?}");
}

/// A join resumes warm through the real worker (issue #2008).
///
/// The scheduled workflow joins two activities. Each decision after the
/// first resumes the resident workflow, also the one that reads a partial
/// result.
#[tokio::test]
async fn a_join_resumes_warm_through_the_worker() {
    let harness = Harness::new().await;
    let config = WorldConfig {
        workflows: 0,
        workers: 1,
        schedulers: 1,
        ..WorldConfig::new(0)
    };
    let mut world = harness.world(&config).await;

    // Move the clock until the schedule fires its first run.
    let mut tick = 0;
    loop {
        if let Effect::Scanned(due) = world.apply(WorldAction::ScheduleScan(0), tick).await
            && due > 0
        {
            let fired = world.apply(WorldAction::ScheduleFire(0), tick).await;
            assert!(matches!(fired, Effect::Fired(Some(_))), "{fired}");
            break;
        }
        tick += 1;
        assert!(tick < 8, "the schedule never came due");
        world.apply(WorldAction::Advance, tick).await;
    }

    // Poll until the run completes. Keep the effect of each decision.
    let mut decisions = Vec::new();
    for _ in 0..12 {
        match world.poll(0).await {
            Effect::Polled(Ran::Activity) | Effect::Idle => {}
            Effect::Polled(ran) => decisions.push(ran),
            other => panic!("unexpected effect {other}"),
        }
    }
    let snapshot = world.read_snapshot().await;
    let run = &snapshot.executions[0];
    assert_eq!(run.status, "COMPLETED", "{:?}", run.events);
    // Decision 2 reads one result and parks the other activity.
    assert_eq!(
        decisions,
        [Ran::Cold, Ran::Warm, Ran::Warm],
        "each decision after the first must resume the join"
    );
}

/// An abandoned claim comes back through the reclaimer or the sweeper.
#[tokio::test]
async fn an_abandoned_claim_is_recovered() {
    let harness = Harness::new().await;
    let config = WorldConfig {
        workflows: 1,
        workers: 2,
        ..WorldConfig::new(0)
    };
    let mut world = harness.world(&config).await;

    // Worker 1 dies holding the first workflow task.
    assert_eq!(world.apply(WorldAction::Abandon(0), 0).await, Effect::Done);
    assert_eq!(world.apply(WorldAction::Poll(1), 0).await, Effect::Idle);
    assert_eq!(
        world.apply(WorldAction::Reclaim, 0).await,
        Effect::Reclaimed(0),
        "the dead worker is not stale yet"
    );
    for tick in 1..=config.stale_ticks {
        world.apply(WorldAction::Advance, tick).await;
    }
    assert_eq!(
        world.apply(WorldAction::Reclaim, 2).await,
        Effect::Reclaimed(1)
    );
    assert_eq!(
        world.apply(WorldAction::Poll(1), 2).await,
        Effect::Polled(Ran::Cold)
    );

    // Worker 2 dies holding the started activity. Its deadline is one tick.
    assert_eq!(world.apply(WorldAction::Abandon(1), 2).await, Effect::Done);
    world.incarnation[1] += 1;
    let restarted = world.start_worker(1).await;
    world.workers[1] = Some(restarted);
    world.apply(WorldAction::Advance, 3).await;
    let swept = world.apply(WorldAction::Sweep, 3).await;
    assert_eq!(swept, Effect::Swept(1), "start-to-close timed out");

    // The retry waits one whole tick. Then a live worker runs it.
    assert_eq!(world.apply(WorldAction::Poll(1), 3).await, Effect::Idle);
    world.apply(WorldAction::Advance, 4).await;
    assert_eq!(
        world.apply(WorldAction::Poll(1), 4).await,
        Effect::Polled(Ran::Activity)
    );
    let events = world.read_snapshot().await.executions[0].events.clone();
    let done = Fact::ActivityCompleted { activity: 0 };
    assert!(events.contains(&done), "{events:?}");
}

/// Two runs of one seed, each on a fresh database, give equal reports.
#[tokio::test]
async fn a_world_seed_runs_twice_with_equal_traces() {
    let harness = Harness::new().await;
    let report = harness.run_twice(&WorldConfig::new(0)).await;
    assert_eq!(report.violation, None, "{}", report.trace_tail(TAIL_LINES));
    assert!(report.converged, "{}", report.trace_tail(TAIL_LINES));
}

/// The scope of issue #2002 runs: every action and every resident outcome.
#[tokio::test]
async fn a_world_sweep_covers_the_scope() {
    let harness = Harness::new().await;
    let plan = SeedPlan {
        first: 10,
        count: 6,
    };
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
    assert!(stats.abandons > 0, "{stats:?}");
    assert!(stats.reclaimed > 0 && stats.swept > 0, "{stats:?}");
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

    // A config from those variables alone gives the same violation, through
    // the replay path of `replay_one_world_seed`.
    let checks = world::checks_arg(&failure.config.checks);
    let replay = world::config_from_vars(seed, Some("foreign-state"), Some(&checks))
        .expect("valid variables");
    assert_eq!(replay, failure.config);
    let report = harness.replay(&replay).await;
    let violation = report.violation.clone().expect("the replay fails too");
    assert_eq!(violation.invariant, WorldInvariant::Deterministic);
    assert_eq!(failure.reason, violation.to_string());
    assert_eq!(failure.trace_tail, report.trace_tail(TAIL_LINES));

    // The same seed with no plant passes.
    let clean = harness.run_twice(&WorldConfig::new(seed)).await;
    assert_eq!(clean.violation, None, "{}", clean.trace_tail(TAIL_LINES));
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
    let report = Harness::new().await.replay(&config).await;
    if let Some(violation) = report.violation {
        panic!("seed {}: {violation}", plan.first);
    }
}
