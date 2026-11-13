#![cfg(feature = "db")]
//! Claim slots: a fleet-wide cap on the claims that run at once per table.
//!
//! See `docs/operations/claim-slots.md`. A test trigger makes each claim of one
//! queue sleep and logs its interval. The largest overlap of those intervals
//! is the number of claim statements that ran at once.
//!
//! Each test owns its database. The slots belong to the task-queue table, so
//! two tests on one table would share them. The stall test also locks the
//! whole table. Set `HARVEST_TEST_DATABASE_URL` to a Postgres server, and
//! each test creates and migrates its own database there. Otherwise each test
//! starts a testcontainers Postgres 16.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::queue::{self, ClaimSlots, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::{ClaimKind, MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup.
// ---------------------------------------------------------------------------

/// A migrated database that one test owns.
///
/// A test that panics leaks its database. The `harvest_cs_` prefix names
/// such a database.
struct TestDb {
    url: String,
    /// The URL that creates and drops the databases of this test.
    admin: String,
    /// The databases that this test created.
    owned: Vec<String>,
    _container: Option<ContainerAsync<Postgres>>,
}

impl TestDb {
    /// A database on `HARVEST_TEST_DATABASE_URL`, or a new container.
    async fn new() -> Self {
        if let Ok(admin) = std::env::var("HARVEST_TEST_DATABASE_URL") {
            let (url, name) = create_database(&admin).await;
            return Self {
                url,
                admin,
                owned: vec![name],
                _container: None,
            };
        }
        let container = Postgres::default()
            .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        Self {
            admin: url.clone(),
            url,
            owned: Vec::new(),
            _container: Some(container),
        }
    }

    /// A second migrated database on the same server, owned by this test.
    async fn sibling(&mut self) -> String {
        let (url, name) = create_database(&self.admin).await;
        self.owned.push(name);
        url
    }

    /// Drop each database of this test. Close its pools first.
    async fn drop(self) {
        let mut admin = connect(&self.admin).await;
        for name in &self.owned {
            exec(
                &mut admin,
                &format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
            )
            .await;
        }
    }
}

/// `url` with its database name set to `database`.
fn with_database(url: &str, database: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let (prefix, _) = base.rsplit_once('/').expect("a database URL has a path");
    query.map_or_else(
        || format!("{prefix}/{database}"),
        |query| format!("{prefix}/{database}?{query}"),
    )
}

/// Create and migrate a database on the server of `admin`.
async fn create_database(admin: &str) -> (String, String) {
    let name = format!("harvest_cs_{}", Uuid::new_v4().simple());
    let mut conn = connect(admin).await;
    exec(&mut conn, &format!("CREATE DATABASE {name}")).await;
    let url = with_database(admin, &name);
    let mut fresh = connect(&url).await;
    diesel_async::SimpleAsyncConnection::batch_execute(
        &mut fresh,
        &autumn_harvest::test_init_sql(),
    )
    .await
    .expect("apply migrations");
    (url, name)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

async fn exec(conn: &mut AsyncPgConnection, sql: &str) {
    diesel::sql_query(sql)
        .execute(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// One `bigint` value named `v`.
async fn scalar(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        v: i64,
    }
    diesel::sql_query(sql)
        .get_result::<Row>(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .v
}

/// `n` as a database count.
fn as_count(n: usize) -> i64 {
    i64::try_from(n).expect("a test count fits in i64")
}

const fn slots(n: u16) -> NonZeroU16 {
    NonZeroU16::new(n).expect("a test slot count is above 0")
}

// ---------------------------------------------------------------------------
// The slow-claim probe.
// ---------------------------------------------------------------------------

/// Serializes probe DDL. Parallel tests on one database otherwise change
/// triggers on `harvest_task_queue` at the same time.
const PROBE_DDL_LOCK: &str = "SELECT pg_advisory_lock(hashtext('claim_slot_tests'))";
const PROBE_DDL_UNLOCK: &str = "SELECT pg_advisory_unlock(hashtext('claim_slot_tests'))";

/// A trigger that makes each claim on one queue sleep, and logs its interval.
struct ClaimProbe {
    url: String,
    suffix: String,
}

impl ClaimProbe {
    /// Install the probe on `queue`. Each claim sleeps `sleep_ms`.
    async fn install(url: &str, queue: &str, sleep_ms: u32) -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        let mut conn = connect(url).await;
        exec(&mut conn, PROBE_DDL_LOCK).await;
        exec(
            &mut conn,
            &format!(
                "CREATE TABLE slot_probe_{suffix} (
                     started timestamptz NOT NULL,
                     finished timestamptz NOT NULL)"
            ),
        )
        .await;
        exec(
            &mut conn,
            &format!(
                "CREATE FUNCTION slot_probe_fn_{suffix}() RETURNS trigger AS $$
                 DECLARE t0 timestamptz := clock_timestamp();
                 BEGIN
                     PERFORM pg_sleep({sleep_ms} / 1000.0);
                     INSERT INTO slot_probe_{suffix} VALUES (t0, clock_timestamp());
                     RETURN NEW;
                 END $$ LANGUAGE plpgsql"
            ),
        )
        .await;
        exec(
            &mut conn,
            &format!(
                "CREATE TRIGGER slot_probe_trg_{suffix}
                 BEFORE UPDATE ON harvest_task_queue
                 FOR EACH ROW
                 WHEN (OLD.state = 'PENDING' AND NEW.state = 'RUNNING'
                       AND NEW.queue_name = '{queue}')
                 EXECUTE FUNCTION slot_probe_fn_{suffix}()"
            ),
        )
        .await;
        exec(&mut conn, PROBE_DDL_UNLOCK).await;
        Self {
            url: url.to_owned(),
            suffix,
        }
    }

    /// The most claims that ran at one time.
    ///
    /// The peak is at the start of some claim. So count, at each start, the
    /// claims that started by then and had not finished.
    async fn max_overlap(&self) -> i64 {
        let s = &self.suffix;
        let mut conn = connect(&self.url).await;
        scalar(
            &mut conn,
            &format!(
                "SELECT coalesce(max(n), 0) AS v FROM (
                     SELECT count(*) AS n
                       FROM slot_probe_{s} p
                       JOIN slot_probe_{s} q
                         ON q.started <= p.started AND q.finished > p.started
                      GROUP BY p.ctid) o"
            ),
        )
        .await
    }

    /// Drop the trigger, its function and the probe table.
    async fn remove(self) {
        let s = &self.suffix;
        let mut conn = connect(&self.url).await;
        exec(&mut conn, PROBE_DDL_LOCK).await;
        exec(
            &mut conn,
            &format!("DROP TRIGGER slot_probe_trg_{s} ON harvest_task_queue"),
        )
        .await;
        exec(&mut conn, &format!("DROP FUNCTION slot_probe_fn_{s}()")).await;
        exec(&mut conn, &format!("DROP TABLE slot_probe_{s}")).await;
        exec(&mut conn, PROBE_DDL_UNLOCK).await;
    }
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Activity runs, by execution id.
static ACTIVITY_RUNS: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Workflow: run `cs_activity` when the input asks for it, else return.
fn cs_workflow(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        if input.get("activity").and_then(serde_json::Value::as_bool) != Some(true) {
            return Ok(serde_json::json!("done"));
        }
        let queue = ctx.queue_name().to_string();
        let id = serde_json::json!(ctx.execution_id().as_uuid().to_string());
        ctx.execute_activity_raw("cs_activity", id, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Activity: count the run.
fn cs_activity(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let key = input.as_str().expect("activity input is an id").to_owned();
        *ACTIVITY_RUNS
            .lock()
            .expect("runs map")
            .entry(key)
            .or_insert(0) += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(input)
    })
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "cs_workflow",
        module: "claim_slot_tests",
        handler: cs_workflow,
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
        name: "cs_activity",
        module: "claim_slot_tests",
        default_retry_policy: Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
        default_start_to_close: Some(Duration::from_secs(60)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
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
        input_schema: None,
        output_schema: None,
        requires: None,
        handler: cs_activity,
    }
}

fn build_registry(metrics: Arc<dyn MetricsRecorder>) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![workflow_info()],
        vec![activity_info()],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

/// A worker config with `claims` claim loops, 16 slots of each kind and the
/// cap `cap` on one queue.
fn runtime_config(
    worker_id: &str,
    queue: &str,
    claims: usize,
    cap: Option<NonZeroU16>,
) -> WorkerRuntimeConfig {
    let mut config: WorkerRuntimeConfig = autumn_harvest::builder::WorkerConfig::default().into();
    worker_id.clone_into(&mut config.worker_id);
    config.queues = vec![queue.to_owned()];
    config.max_concurrent_workflows = 16;
    config.max_concurrent_activities = 16;
    config.max_concurrent_claims = claims;
    config.claim_slots_per_shard = cap;
    config.poll_interval = Duration::from_millis(25);
    config.shutdown_timeout = Duration::from_secs(5);
    config.shard_assignments = vec![ShardId::new(0)];
    config
}

fn build_worker(config: WorkerRuntimeConfig, metrics: Arc<dyn MetricsRecorder>) -> Arc<Worker> {
    Arc::new(Worker::new(config, build_registry(metrics)).expect("worker should build"))
}

/// Run `worker` on `pool` in a task.
fn spawn_worker(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    let runner = Arc::clone(worker);
    let pool = pool.clone();
    tokio::spawn(async move { runner.run(&pool).await })
}

/// Stop every worker, then join each.
async fn stop(workers: Vec<(Arc<Worker>, tokio::task::JoinHandle<()>)>) {
    for (worker, _) in &workers {
        worker.shutdown();
    }
    for (_, handle) in workers {
        handle.await.expect("worker joins");
    }
}

/// One claim sample or slot miss, in the order that workers record them.
#[derive(Debug, Clone, Copy)]
enum ClaimEvent {
    /// A claim on `shard` that started at `at`. `task` is true when it
    /// returned a task.
    Claim { shard: u16, at: Instant, task: bool },
    /// A slot miss on `shard`, recorded at `at`.
    Miss { shard: u16, at: Instant },
}

/// Counts the claims and the slot misses that workers record, by shard.
#[derive(Default)]
struct ClaimCounter {
    /// Each claim and miss, in order.
    events: Mutex<Vec<ClaimEvent>>,
    /// Claims that returned a task.
    tasks: AtomicUsize,
    /// Every claim sample, by shard label.
    claims_by_shard: Mutex<BTreeMap<u16, usize>>,
    /// Slot misses, by shard label.
    misses_by_shard: Mutex<BTreeMap<u16, usize>>,
}

impl ClaimCounter {
    fn claims_on(&self, shard: u16) -> usize {
        self.claims_by_shard
            .lock()
            .expect("claims map")
            .get(&shard)
            .copied()
            .unwrap_or(0)
    }

    fn misses_on(&self, shard: u16) -> usize {
        self.misses_by_shard
            .lock()
            .expect("misses map")
            .get(&shard)
            .copied()
            .unwrap_or(0)
    }

    fn misses(&self) -> usize {
        self.misses_by_shard
            .lock()
            .expect("misses map")
            .values()
            .sum()
    }
}

impl MetricsRecorder for ClaimCounter {
    fn record_claim_duration(&self, kind: ClaimKind, shard: u16, seconds: f64) {
        let at = Instant::now()
            .checked_sub(Duration::from_secs_f64(seconds))
            .unwrap_or_else(Instant::now);
        self.events.lock().expect("events").push(ClaimEvent::Claim {
            shard,
            at,
            task: matches!(kind, ClaimKind::Workflow | ClaimKind::Activity),
        });
        if matches!(kind, ClaimKind::Workflow | ClaimKind::Activity) {
            self.tasks.fetch_add(1, Ordering::Relaxed);
        }
        *self
            .claims_by_shard
            .lock()
            .expect("claims map")
            .entry(shard)
            .or_insert(0) += 1;
    }

    fn record_db_claim_slot_miss(&self, shard: u16) {
        self.events.lock().expect("events").push(ClaimEvent::Miss {
            shard,
            at: Instant::now(),
        });
        *self
            .misses_by_shard
            .lock()
            .expect("misses map")
            .entry(shard)
            .or_insert(0) += 1;
    }
}

// ---------------------------------------------------------------------------
// Seeding and read helpers.
// ---------------------------------------------------------------------------

async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    queue: &str,
    input: serde_json::Value,
    shard: ShardId,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "cs_workflow",
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: shard.as_i32(),
        input: input.clone().into(),
        parent_id: None,
        queue_name: queue,
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

/// Seed `n` workflows of `shard` on `queue` over 8 connections.
async fn seed(
    url: &str,
    queue: &str,
    n: usize,
    with_activity: bool,
    shard: ShardId,
) -> Vec<ExecutionId> {
    let input = serde_json::json!({ "activity": with_activity });
    let lanes = 8.min(n.max(1));
    let mut joins = Vec::with_capacity(lanes);
    for lane in 0..lanes {
        let url = url.to_owned();
        let queue = queue.to_owned();
        let input = input.clone();
        let count = n / lanes + usize::from(lane < n % lanes);
        joins.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(seed_workflow(&mut conn, &queue, input.clone(), shard).await);
            }
            ids
        }));
    }
    let mut ids = Vec::with_capacity(n);
    for join in joins {
        ids.extend(join.await.expect("seed lane joins"));
    }
    ids
}

/// One `count(*)` with one text bind.
async fn count(url: &str, sql: &str, bind: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        v: i64,
    }
    let mut conn = connect(url).await;
    diesel::sql_query(sql)
        .bind::<Text, _>(bind)
        .get_result::<Row>(&mut conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .v
}

/// How many workflows on `queue` completed.
async fn completed(url: &str, queue: &str) -> i64 {
    count(
        url,
        "SELECT count(*) AS v FROM harvest_workflow_executions
          WHERE queue_name = $1 AND state = 'COMPLETED'",
        queue,
    )
    .await
}

/// Wait until `n` workflows on `queue` complete, or `limit` elapses.
async fn wait_completed(url: &str, queue: &str, n: i64, limit: Duration) -> bool {
    tokio::time::timeout(limit, async {
        while completed(url, queue).await < n {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

/// Start `workers` workers of `claims` loops each on `queue`, each with its
/// own pool.
fn start_fleet(
    url: &str,
    queue: &str,
    workers: usize,
    claims: usize,
    cap: Option<NonZeroU16>,
    metrics: &Arc<dyn MetricsRecorder>,
) -> Vec<(Arc<Worker>, tokio::task::JoinHandle<()>)> {
    (0..workers)
        .map(|i| {
            let config = runtime_config(&format!("{queue}-{i}"), queue, claims, cap);
            let worker = build_worker(config, Arc::clone(metrics));
            let handle = spawn_worker(&worker, &build_pool(url));
            (worker, handle)
        })
        .collect()
}

/// Drain `n` no-activity workflows through a slow claim with 3 workers of 4
/// claim loops each. Return the largest claim overlap and the slot misses.
async fn fleet_overlap(cap: Option<NonZeroU16>, n: usize) -> (i64, usize) {
    let db = TestDb::new().await;
    let queue = format!("cs-overlap-{}", Uuid::new_v4());
    let probe = ClaimProbe::install(&db.url, &queue, 150).await;
    seed(&db.url, &queue, n, false, ShardId::new(0)).await;
    let counter = Arc::new(ClaimCounter::default());
    let metrics: Arc<dyn MetricsRecorder> = Arc::clone(&counter) as Arc<dyn MetricsRecorder>;
    let fleet = start_fleet(&db.url, &queue, 3, 4, cap, &metrics);

    let drained = wait_completed(&db.url, &queue, as_count(n), Duration::from_secs(90)).await;
    stop(fleet).await;
    let overlap = probe.max_overlap().await;
    probe.remove().await;
    db.drop().await;
    assert!(drained, "every workflow completes");
    (overlap, counter.misses())
}

// ---------------------------------------------------------------------------
// AC1: the cap bounds the claims that run at once on one table.
// ---------------------------------------------------------------------------

/// Twelve claim loops share two slots. At most two claims run at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_slots_bound_the_claims_that_run_at_once() {
    let (overlap, misses) = fleet_overlap(Some(slots(2)), 24).await;
    assert!(
        overlap <= 2,
        "no more than 2 claims run at once on the table; largest overlap {overlap}"
    );
    assert_eq!(overlap, 2, "both slots are in use at the peak");
    assert!(misses > 0, "some claims miss a slot, so the cap is tested");
}

/// The control: with no cap, the same fleet runs more than two claims at
/// once. So the test above can fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_claim_slots_the_fleet_overlaps_past_two() {
    let (overlap, misses) = fleet_overlap(None, 24).await;
    assert!(
        overlap > 2,
        "12 claim loops run more than 2 claims at once; largest overlap {overlap}"
    );
    assert_eq!(misses, 0, "a worker with no cap takes no slot");
}

// ---------------------------------------------------------------------------
// AC2: a miss moves the claim to the next shard in the same poll.
// ---------------------------------------------------------------------------

/// Take every slot of the table at `url`. Each slot stays held until its
/// connection ends its transaction.
async fn hold_every_slot(url: &str, cap: NonZeroU16) -> Vec<AsyncPgConnection> {
    let mut holders = Vec::new();
    for _ in 0..cap.get() {
        let mut conn = connect(url).await;
        exec(&mut conn, "BEGIN").await;
        let taken = queue::take_claim_slot(&mut conn, ClaimSlots::new(cap))
            .await
            .expect("the slot statement runs");
        assert!(taken.is_some(), "a free slot is taken");
        holders.push(conn);
    }
    holders
}

/// How long each decision of the next-shard test takes.
const SLOW_DECISION: Duration = Duration::from_secs(3);
/// The window in which the next-shard test must claim every task.
const BURST: Duration = Duration::from_millis(1500);

/// Shard 0 and shard 1 are two databases. Every slot of shard 0 is held. The
/// worker claims each task on shard 1.
///
/// The worker runs one claim loop, no listener and a 30 s poll interval. A
/// trigger makes each decision commit take 3 s, so no decision wakes the
/// loop for 3 s. The loop claims all 8 tasks inside 1.5 s, each after a miss
/// on shard 0. A loop that waited after a miss could claim only one task in
/// that time. So each miss moved the claim to shard 1 in the same pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slot_miss_moves_the_claim_to_the_next_shard() {
    let mut db = TestDb::new().await;
    let shard_one = db.sibling().await;
    let queue = format!("cs-next-{}", Uuid::new_v4());
    let n = 8;
    seed(&shard_one, &queue, n, false, ShardId::new(1)).await;
    let mut slow = connect(&shard_one).await;
    exec(
        &mut slow,
        &format!(
            "CREATE FUNCTION cs_slow_fn() RETURNS trigger AS $$
             BEGIN
                 PERFORM pg_sleep({});
                 RETURN NEW;
             END $$ LANGUAGE plpgsql",
            SLOW_DECISION.as_secs()
        ),
    )
    .await;
    exec(
        &mut slow,
        "CREATE TRIGGER cs_slow_trg BEFORE UPDATE ON harvest_workflow_executions
         FOR EACH ROW
         WHEN (OLD.state <> 'COMPLETED' AND NEW.state = 'COMPLETED')
         EXECUTE FUNCTION cs_slow_fn()",
    )
    .await;
    drop(slow);
    let cap = slots(2);
    let holders = hold_every_slot(&db.url, cap).await;

    let pool = build_pool(&db.url);
    let pools = BTreeMap::from([
        (ShardId::new(0), pool.clone()),
        (ShardId::new(1), build_pool(&shard_one)),
    ]);
    let mut config = runtime_config(&queue, &queue, 1, Some(cap));
    config.poll_interval = Duration::from_secs(30);
    config.shard_assignments = vec![ShardId::new(0), ShardId::new(1)];
    config.sharded_pool = Some(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let counter = Arc::new(ClaimCounter::default());
    let worker = build_worker(config, Arc::clone(&counter) as Arc<dyn MetricsRecorder>);
    let handle = spawn_worker(&worker, &pool);

    let drained = wait_completed(&shard_one, &queue, as_count(n), Duration::from_secs(30)).await;
    stop(vec![(worker, handle)]).await;
    drop(holders);
    db.drop().await;

    assert!(drained, "every workflow completes on shard 1");
    assert_eq!(
        counter.claims_on(0),
        0,
        "no claim statement runs on the shard with no free slot"
    );
    let events = counter.events.lock().expect("events").clone();
    let mut task_claims = Vec::new();
    for (i, event) in events.iter().enumerate() {
        let ClaimEvent::Claim { shard, at, task } = *event else {
            continue;
        };
        assert_eq!(shard, 1, "every claim runs on shard 1");
        if task {
            assert!(
                i > 0 && matches!(events[i - 1], ClaimEvent::Miss { shard: 0, .. }),
                "each task claim follows a miss on shard 0: {:?}",
                &events[..=i]
            );
            task_claims.push(at);
        }
    }
    assert_eq!(task_claims.len(), n, "one claim per task");
    let first = task_claims.iter().min().expect("a claim");
    let last = task_claims.iter().max().expect("a claim");
    let burst = last.duration_since(*first);
    assert!(
        burst < BURST,
        "the loop claims all {n} tasks in {burst:?}, inside one decision of {SLOW_DECISION:?}"
    );
}

// ---------------------------------------------------------------------------
// AC3: exactly once with the cap on.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_task_runs_once_with_claim_slots() {
    let db = TestDb::new().await;
    let queue = format!("cs-once-{}", Uuid::new_v4());
    let probe = ClaimProbe::install(&db.url, &queue, 30).await;
    let n = 24;
    let ids = seed(&db.url, &queue, n, true, ShardId::new(0)).await;
    let counter = Arc::new(ClaimCounter::default());
    let metrics: Arc<dyn MetricsRecorder> = Arc::clone(&counter) as Arc<dyn MetricsRecorder>;
    // Two workers, so claims race inside one worker and across workers.
    let fleet = start_fleet(&db.url, &queue, 2, 4, Some(slots(2)), &metrics);

    let drained = wait_completed(&db.url, &queue, as_count(n), Duration::from_secs(90)).await;
    stop(fleet).await;
    let overlap = probe.max_overlap().await;
    probe.remove().await;
    let left = count(
        &db.url,
        "SELECT count(*) AS v FROM harvest_task_queue
          WHERE queue_name = $1 AND state <> 'COMPLETED'",
        &queue,
    )
    .await;
    db.drop().await;
    assert!(drained, "every workflow completes");
    assert!(overlap <= 2, "the cap holds; largest overlap {overlap}");
    assert!(counter.misses() > 0, "some claims miss a slot");

    let runs = ACTIVITY_RUNS.lock().expect("runs map").clone();
    for id in &ids {
        let key = id.as_uuid().to_string();
        assert_eq!(
            runs.get(&key),
            Some(&1),
            "activity of {key} runs exactly once"
        );
    }
    assert_eq!(left, 0, "no task row is left behind");
}

// ---------------------------------------------------------------------------
// AC4: claim throughput recovers after a stall.
// ---------------------------------------------------------------------------

/// What one stall run measured.
#[derive(Debug)]
struct Recovery {
    /// Claims per second before the stall: the median of three windows.
    before: f64,
    /// Claims per second in the window that met the bar.
    after: f64,
    /// Time from the end of the stall to the window that met the bar.
    recovered_in: Duration,
    /// Slot misses over the whole run.
    misses: usize,
}

/// The claims that a sample window must reach, as a share of the rate
/// before the stall.
const RECOVERY_BAR: f64 = 0.9;
/// How long a recovery may take.
const RECOVERY_LIMIT: Duration = Duration::from_secs(10);
/// How long the stall holds the table.
const STALL: Duration = Duration::from_secs(3);
/// The width of one rate window.
const WINDOW: Duration = Duration::from_secs(1);
/// The backlog of each run.
const STALL_BACKLOG: usize = 6000;

/// Claims per second between two samples.
fn rate(from: (Instant, usize), to: (Instant, usize)) -> f64 {
    let secs = to.0.duration_since(from.0).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let claims = to.1.saturating_sub(from.1) as f64;
    claims / secs
}

/// Stall the claims of a 3-worker fleet with an `ACCESS EXCLUSIVE` lock on
/// `harvest_task_queue`, lift it, and measure the recovery.
///
/// The run owns its database, so the stall blocks no other test. The rate
/// before the stall is the median of three 1 s windows.
async fn stall_and_recover(cap: Option<NonZeroU16>) -> Recovery {
    let db = TestDb::new().await;
    let queue = format!("cs-stall-{}", Uuid::new_v4());
    seed(&db.url, &queue, STALL_BACKLOG, false, ShardId::new(0)).await;
    let counter = Arc::new(ClaimCounter::default());
    let metrics: Arc<dyn MetricsRecorder> = Arc::clone(&counter) as Arc<dyn MetricsRecorder>;
    let fleet = start_fleet(&db.url, &queue, 3, 4, cap, &metrics);
    // Fully qualified: diesel's `RunQueryDsl::load` shadows it.
    let sample = || {
        (
            Instant::now(),
            AtomicUsize::load(&counter.tasks, Ordering::Relaxed),
        )
    };

    // Warm up, then take the rate before the stall.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut windows = Vec::with_capacity(3);
    let mut last = sample();
    for _ in 0..3 {
        tokio::time::sleep(WINDOW).await;
        let now = sample();
        windows.push(rate(last, now));
        last = now;
    }
    windows.sort_by(f64::total_cmp);
    let before = windows[1];

    // The backlog must outlast the stall and the longest recovery.
    let left = STALL_BACKLOG.saturating_sub(last.1);
    #[allow(clippy::cast_precision_loss)]
    let needed = before * (RECOVERY_LIMIT + Duration::from_secs(2)).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let enough = left as f64 >= needed;
    assert!(
        enough,
        "{left} tasks left at {before:.0}/s cannot outlast the recovery; raise STALL_BACKLOG"
    );

    // The stall: one transaction holds the table for `STALL`.
    let mut staller = connect(&db.url).await;
    exec(&mut staller, "BEGIN").await;
    exec(
        &mut staller,
        "LOCK TABLE harvest_task_queue IN ACCESS EXCLUSIVE MODE",
    )
    .await;
    tokio::time::sleep(STALL).await;
    exec(&mut staller, "COMMIT").await;
    let lifted = Instant::now();

    // Slide a 1 s window in 100 ms steps until it meets the bar.
    let mut samples = vec![sample()];
    let mut met = None;
    let mut best = 0.0_f64;
    while lifted.elapsed() < RECOVERY_LIMIT {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = sample();
        samples.push(now);
        if let Some(start) = samples
            .iter()
            .rev()
            .find(|s| now.0.duration_since(s.0) >= WINDOW)
        {
            let r = rate(*start, now);
            best = best.max(r);
            if r >= RECOVERY_BAR * before {
                met = Some((now.0.duration_since(lifted), r));
                break;
            }
        }
    }
    stop(fleet).await;
    drop(staller);
    db.drop().await;

    let (recovered_in, after) = met.unwrap_or_else(|| {
        panic!(
            "claim throughput did not reach {RECOVERY_BAR} of {before:.0}/s within \
             {RECOVERY_LIMIT:?} after the stall (cap {cap:?}). The windows before: \
             {windows:?}. The best window after: {best:.0}/s"
        )
    });
    let recovery = Recovery {
        before,
        after,
        recovered_in,
        misses: counter.misses(),
    };
    eprintln!("claim slot stall recovery, cap {cap:?}: {recovery:?}");
    recovery
}

/// The cap off, then the cap on, one after the other. Each run must recover
/// to 90% of its rate before the stall within 10 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_throughput_recovers_after_a_stall() {
    let off = stall_and_recover(None).await;
    assert_eq!(off.misses, 0, "a worker with no cap takes no slot");
    let on = stall_and_recover(Some(slots(4))).await;
    assert!(on.misses > 0, "12 claim loops miss some of 4 slots");
}

// ---------------------------------------------------------------------------
// The slot statement.
// ---------------------------------------------------------------------------

/// The claim slot locks that this session holds, as `(table tag, slot)`.
async fn slot_locks_held(conn: &mut AsyncPgConnection) -> Vec<(i64, i64)> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        table_tag: i64,
        #[diesel(sql_type = BigInt)]
        slot: i64,
    }
    let sql = format!(
        "SELECT objid::bigint >> 16 AS table_tag, objid::bigint & 65535 AS slot
           FROM pg_locks
          WHERE locktype = 'advisory' AND objsubid = 1
            AND classid::bigint = {}
            AND pid = pg_backend_pid()",
        queue::CLAIM_SLOT_LOCK_CLASS
    );
    diesel::sql_query(&sql)
        .load::<Row>(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (row.table_tag, row.slot))
        .collect()
}

/// The table tag of `harvest_task_queue` on `conn`.
async fn table_tag(conn: &mut AsyncPgConnection) -> i64 {
    scalar(
        conn,
        "SELECT to_regclass('harvest_task_queue')::oid::int8 & 65535 AS v",
    )
    .await
}

/// Each transaction takes one free slot and holds it until it ends. A
/// transaction that finds every slot held takes none. The key in `pg_locks`
/// is `claim_slot_lock_key` of the table tag and the slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_slot_statement_takes_one_slot_until_commit() {
    let db = TestDb::new().await;
    let cap = slots(3);
    let full = ClaimSlots::new(cap);

    let mut holders = Vec::new();
    let mut taken = Vec::new();
    for _ in 0..cap.get() {
        let mut conn = connect(&db.url).await;
        exec(&mut conn, "BEGIN").await;
        let slot = queue::take_claim_slot(&mut conn, full)
            .await
            .expect("the slot statement runs")
            .expect("a free slot is taken");
        assert!(slot < cap.get(), "slot {slot} is in range");
        let tag = table_tag(&mut conn).await;
        assert_eq!(
            slot_locks_held(&mut conn).await,
            vec![(tag, i64::from(slot))],
            "one slot per claim, keyed by the table tag and the slot"
        );
        taken.push(slot);
        holders.push(conn);
    }
    let first_slot = taken[0];
    taken.sort_unstable();
    assert_eq!(taken, vec![0, 1, 2], "each claim gets its own slot");

    let mut late = connect(&db.url).await;
    exec(&mut late, "BEGIN").await;
    let missed = queue::take_claim_slot(&mut late, full)
        .await
        .expect("the slot statement runs");
    assert_eq!(missed, None, "every slot is held");
    assert!(
        slot_locks_held(&mut late).await.is_empty(),
        "a miss holds no slot"
    );
    exec(&mut late, "COMMIT").await;

    // A commit frees the slot of its claim.
    let mut first = holders.remove(0);
    exec(&mut first, "COMMIT").await;
    assert!(
        slot_locks_held(&mut first).await.is_empty(),
        "commit frees the slot"
    );
    exec(&mut late, "BEGIN").await;
    let freed = queue::take_claim_slot(&mut late, full)
        .await
        .expect("the slot statement runs");
    assert_eq!(freed, Some(first_slot), "the freed slot is taken again");
    exec(&mut late, "ROLLBACK").await;
    assert!(
        slot_locks_held(&mut late).await.is_empty(),
        "rollback frees the slot"
    );
    for mut conn in holders {
        exec(&mut conn, "ROLLBACK").await;
    }
    drop((first, late));
    db.drop().await;
}

/// Two databases have separate slots.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_database_leaves_the_slots_of_another_free() {
    let mut db = TestDb::new().await;
    let other = db.sibling().await;
    let cap = slots(1);
    let holders = hold_every_slot(&db.url, cap).await;

    let mut conn = connect(&db.url).await;
    exec(&mut conn, "BEGIN").await;
    let missed = queue::take_claim_slot(&mut conn, ClaimSlots::new(cap))
        .await
        .expect("the slot statement runs");
    assert_eq!(missed, None, "the full database has no slot");
    exec(&mut conn, "ROLLBACK").await;

    let mut elsewhere = connect(&other).await;
    exec(&mut elsewhere, "BEGIN").await;
    let free = queue::take_claim_slot(&mut elsewhere, ClaimSlots::new(cap))
        .await
        .expect("the slot statement runs");
    assert_eq!(free, Some(0), "the other database has its own slot");
    exec(&mut elsewhere, "ROLLBACK").await;
    drop((holders, conn, elsewhere));
    db.drop().await;
}
