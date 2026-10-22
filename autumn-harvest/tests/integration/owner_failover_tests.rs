#![cfg(all(feature = "db", unix))]
#![allow(clippy::unused_async)]
//! Owner failover against real Postgres (issue #2254).
//!
//! The owner runs as a child process of this test binary. The test can then
//! stop it with `SIGSTOP`, wake it with `SIGCONT` and kill it with `SIGKILL`.
//! A standby worker runs in this process.
//!
//! - **Frozen owner.** The owner stops during a decision. Its lease ends, and
//!   the standby takes its partitions and completes the run. Then the owner
//!   wakes. Its commit must fail, and no event is lost or duplicated. This is
//!   the test of golemcloud/golem#3845.
//! - **kill -9.** The next decision on the partitions of the killed owner
//!   must run within the lease TTL plus 1 s.
//! - **Scale-out.** The fleet grows from 4 to 8 workers under load. It must
//!   recover within two lease TTLs, with no fenced commit after it settles.
//!
//! Owner mode is a per-database switch, so each test runs on its own
//! database. `docs/performance-owner-failover.md` records the results.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::decision_owner::{DecisionPartition, OwnerLeaseTtl};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Double, Integer, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::Value;

use crate::decision_owner_claim_tests::OwnDb;
use crate::integration_e2e::spawn_test_worker;

/// The child reads its database URL from this variable.
const CHILD_DB_URL_VAR: &str = "HARVEST_FAILOVER_CHILD_DB_URL";
/// The child polls this queue.
const CHILD_QUEUE_VAR: &str = "HARVEST_FAILOVER_CHILD_QUEUE";
/// The child uses this worker id.
const CHILD_ID_VAR: &str = "HARVEST_FAILOVER_CHILD_ID";
/// The child writes its markers and its fence log to this directory.
const CHILD_DIR_VAR: &str = "HARVEST_FAILOVER_CHILD_DIR";

/// The libtest name of the child entry point.
const CHILD_ENTRY: &str = "owner_failover_tests::failover_child_entry";

/// The worker heartbeat. The reclaimer then sees a stopped worker as dead
/// after 2 s.
const HEARTBEAT: Duration = Duration::from_secs(1);

/// The bound on each wait for a state that the design says must come.
const WAIT: Duration = Duration::from_secs(90);

/// The lease TTL of every worker. Workers use the default.
const TTL: Duration = OwnerLeaseTtl::DEFAULT.get();

/// The decision events of the workflows here. A foreign writer writes none
/// of them.
const DECISION_EVENTS: [&str; 3] = ["ActivityScheduled", "TimerStarted", "WorkflowCompleted"];

// ── Workflows ───────────────────────────────────────────────────────────────

#[activity(start_to_close = "30s")]
async fn failover_step(_ctx: &ActivityContext, n: u64) -> Result<u64, String> {
    Ok(n)
}

/// Waits for the signal `go`, then runs one activity.
#[workflow]
async fn failover_signal_flow(ctx: &WorkflowContext, queue: String) -> Result<String, String> {
    let _ = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    ctx.execute_activity_raw("failover_step", serde_json::json!(1), &queue)
        .await
        .map_err(|e| e.to_string())?;
    Ok("done".to_string())
}

/// [`failover_signal_flow`] with a hold after the signal. In the child, the
/// decision blocks there once, until the test releases it.
#[workflow]
async fn failover_hold_flow(ctx: &WorkflowContext, queue: String) -> Result<String, String> {
    let _ = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    hold_in_child();
    ctx.execute_activity_raw("failover_step", serde_json::json!(1), &queue)
        .await
        .map_err(|e| e.to_string())?;
    Ok("done".to_string())
}

/// `n` one-second timers in sequence. Each timer makes one decision, and no
/// foreign writer takes part.
#[workflow]
async fn failover_timers(ctx: &WorkflowContext, n: u32) -> Result<String, String> {
    for i in 0..n {
        ctx.timer(&format!("t{i}"), 1)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok("done".to_string())
}

/// `n` activities in sequence, as the e2e throughput bench runs.
#[workflow]
async fn failover_steps(ctx: &WorkflowContext, input: (String, u64)) -> Result<String, String> {
    let (queue, n) = input;
    for i in 0..n {
        ctx.execute_activity_raw("failover_step", serde_json::json!(i), &queue)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok("done".to_string())
}

/// Block the decision once in the child process.
///
/// It writes the marker `held`, then waits for the marker `release`. It does
/// nothing outside the child, and nothing after the first hold. So a replay
/// on the standby sends the same commands.
fn hold_in_child() {
    let Some(dir) = std::env::var_os(CHILD_DIR_VAR) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let held = dir.join("held");
    if held.exists() {
        return;
    }
    std::fs::write(&held, b"held").expect("write the hold marker");
    let deadline = Instant::now() + WAIT;
    while !dir.join("release").exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ── Workers ─────────────────────────────────────────────────────────────────

/// Records the time of each fenced commit. With a log path, it also appends
/// one line per fence, so the parent can count the fences of a child.
#[derive(Clone, Default)]
struct Fences {
    at: Arc<Mutex<Vec<Instant>>>,
    log: Option<PathBuf>,
}

impl Fences {
    fn times(&self) -> Vec<Instant> {
        self.at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl MetricsRecorder for Fences {
    fn record_decision_owner_fenced(&self, _shard: &str) {
        self.at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Instant::now());
        if let Some(path) = &self.log {
            use std::io::Write as _;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("open the fence log");
            file.write_all(b"fenced\n")
                .expect("append to the fence log");
        }
    }
}

/// A worker in owner mode on `queue`.
///
/// A decision of the child can hold for many seconds. So the workflow-task
/// budget is longer than any hold.
fn build_worker(worker_id: &str, queue: &str, fences: &Fences) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(workflows![
            failover_signal_flow,
            failover_hold_flow,
            failover_timers,
            failover_steps
        ])
        .activities(activities![failover_step])
        .worker(WorkerConfig::default().with_queues([queue]))
        .telemetry(
            TelemetryConfig::builder()
                .metrics(Arc::new(fences.clone()))
                .build(),
        )
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime: WorkerRuntimeConfig = worker_config.into();
    runtime.worker_id = worker_id.to_string();
    runtime.poll_interval = Duration::from_millis(25);
    runtime.worker_heartbeat_interval = HEARTBEAT;
    runtime.max_concurrent_workflows = 4;
    runtime.max_concurrent_activities = 4;
    runtime.max_concurrent_claims = 1;
    runtime.workflow_task_timeout = WAIT;
    runtime.max_local_activity_start_to_close = WAIT;
    Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"))
}

/// A pool of at most `size` connections. Eight workers share one server.
fn pool(url: &str, size: usize) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    diesel_async::pooled_connection::deadpool::Pool::builder(manager)
        .max_size(size)
        .build()
        .expect("pool")
}

/// A worker in this process, and its fences.
struct Node {
    id: String,
    worker: Arc<Worker>,
    fences: Fences,
    task: tokio::task::JoinHandle<()>,
}

impl Node {
    fn start(url: &str, prefix: &str, queue: &str) -> Self {
        let id = unique(prefix);
        let fences = Fences::default();
        let worker = build_worker(&id, queue, &fences);
        let task = spawn_test_worker(Arc::clone(&worker), pool(url, 10));
        Self {
            id,
            worker,
            fences,
            task,
        }
    }

    async fn stop(self) {
        self.worker.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(15), self.task).await;
    }
}

/// The owner as a child process. Drop kills it, so a failed test leaves no
/// process.
struct Child {
    process: std::process::Child,
    id: String,
    dir: tempfile::TempDir,
}

impl Child {
    fn start(url: &str, queue: &str) -> Self {
        let id = unique("failover-child");
        let dir = tempfile::tempdir().expect("marker directory");
        let process = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", CHILD_ENTRY, "--ignored", "--nocapture"])
            .env(CHILD_DB_URL_VAR, url)
            .env(CHILD_QUEUE_VAR, queue)
            .env(CHILD_ID_VAR, &id)
            .env(CHILD_DIR_VAR, dir.path())
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("start the child");
        Self { process, id, dir }
    }

    fn signal(&self, name: &str) {
        let status = std::process::Command::new("kill")
            .args([format!("-{name}"), self.process.id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -{name} failed: {status:?}");
    }

    fn marker(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn fences(&self) -> usize {
        std::fs::read_to_string(self.marker("fenced")).map_or(0, |s| s.lines().count())
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if !matches!(self.process.try_wait(), Ok(None)) {
            return;
        }
        // Wake a stopped process first, so that it dies at once.
        let _ = std::process::Command::new("kill")
            .args(["-CONT", &self.process.id().to_string()])
            .stderr(std::process::Stdio::null())
            .status();
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// The child worker process. It runs only when the parent test sets
/// [`CHILD_DB_URL_VAR`]. Otherwise it returns at once.
#[test]
#[ignore = "started as a child process by the owner failover tests"]
fn failover_child_entry() {
    let Ok(url) = std::env::var(CHILD_DB_URL_VAR) else {
        return;
    };
    let queue = std::env::var(CHILD_QUEUE_VAR).expect("queue");
    let id = std::env::var(CHILD_ID_VAR).expect("worker id");
    let dir = PathBuf::from(std::env::var_os(CHILD_DIR_VAR).expect("marker directory"));
    // A panic becomes an abort, so the parent never sees a clean exit.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("failover child: panic: {info}");
        std::process::abort();
    }));
    // The child must not outlive a parent that failed to kill it.
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(300));
        std::process::exit(3);
    });
    let fences = Fences {
        at: Arc::default(),
        log: Some(dir.join("fenced")),
    };
    let runtime = tokio::runtime::Runtime::new().expect("child runtime");
    runtime.block_on(async move {
        let worker = build_worker(&id, &queue, &fences);
        worker.run(&pool(&url, 10)).await;
    });
}

// ── Probes ──────────────────────────────────────────────────────────────────

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

#[derive(QueryableByName)]
struct HeldRow {
    #[diesel(sql_type = Text)]
    holder: String,
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// The live partitions of each worker id.
async fn held_by_worker(conn: &mut AsyncPgConnection) -> BTreeMap<String, i64> {
    let rows: Vec<HeldRow> = diesel::sql_query(
        "SELECT holder, count(*)::bigint AS n FROM harvest_decision_owner \
         WHERE holder IS NOT NULL AND lease_until > clock_timestamp() GROUP BY holder",
    )
    .load(conn)
    .await
    .expect("read the holders");
    let mut held = BTreeMap::new();
    for row in rows {
        *held.entry(worker_of(&row.holder)).or_default() += row.n;
    }
    held
}

/// The worker id of a holder token `worker/nonce`.
fn worker_of(holder: &str) -> String {
    holder
        .rsplit_once('/')
        .map_or_else(|| holder.to_string(), |(w, _)| w.to_string())
}

#[derive(QueryableByName)]
struct PartitionRow {
    #[diesel(sql_type = Integer)]
    partition: i32,
}

/// The partitions that `worker` holds with a live lease.
async fn partitions_of(conn: &mut AsyncPgConnection, worker: &str) -> BTreeSet<u16> {
    let rows: Vec<PartitionRow> = diesel::sql_query(
        "SELECT partition::int4 AS partition FROM harvest_decision_owner \
         WHERE split_part(holder, '/', 1) = $1 AND lease_until > clock_timestamp()",
    )
    .bind::<Text, _>(worker)
    .load(conn)
    .await
    .expect("read the partitions");
    rows.into_iter()
        .map(|r| u16::try_from(r.partition).expect("partition"))
        .collect()
}

#[derive(QueryableByName)]
struct OwnerRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    holder: Option<String>,
    #[diesel(sql_type = BigInt)]
    epoch: i64,
}

async fn owner_of(conn: &mut AsyncPgConnection, p: DecisionPartition) -> (Option<String>, i64) {
    let row: OwnerRow =
        diesel::sql_query("SELECT holder, epoch FROM harvest_decision_owner WHERE partition = $1")
            .bind::<Integer, _>(i32::from(p.get()))
            .get_result(conn)
            .await
            .expect("read the owner row");
    (row.holder.as_deref().map(worker_of), row.epoch)
}

/// Wait until `check` holds, up to [`WAIT`].
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait until each of `workers` holds partitions and together they hold all
/// 512, `low` to `high` each.
async fn wait_for_spread(conn: &mut AsyncPgConnection, workers: &[&str], low: i64, high: i64) {
    let what = format!(
        "{} workers hold {low} to {high} partitions each",
        workers.len()
    );
    eventually(&what, async || {
        let held = held_by_worker(conn).await;
        spread_holds(&held, workers, low, high)
    })
    .await;
}

fn spread_holds(held: &BTreeMap<String, i64>, workers: &[&str], low: i64, high: i64) -> bool {
    held.values().sum::<i64>() == 512
        && held.len() == workers.len()
        && workers
            .iter()
            .all(|w| held.get(*w).is_some_and(|n| (low..=high).contains(n)))
}

async fn enable_owner_mode(conn: &mut AsyncPgConnection) {
    autumn_harvest::decision_owner::enable_owner_mode(
        conn,
        &autumn_harvest::decision_owner::EnableOwnerMode {
            force: true,
            ..Default::default()
        },
    )
    .await
    .expect("enable owner mode");
}

#[derive(QueryableByName)]
struct EventRow {
    #[diesel(sql_type = Integer)]
    event_id: i32,
    #[diesel(sql_type = Text)]
    t: String,
}

/// The event ids and types of a run, in history order.
async fn history(conn: &mut AsyncPgConnection, exec: ExecutionId) -> Vec<(i32, String)> {
    let rows: Vec<EventRow> = diesel::sql_query(
        "SELECT event_id, event_type AS t FROM harvest_events \
         WHERE workflow_exec_id = $1 ORDER BY event_id",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .load(conn)
    .await
    .expect("read the history");
    rows.into_iter().map(|r| (r.event_id, r.t)).collect()
}

/// Assert that a history has gapless event ids and exactly the event types
/// of `expected`, in order. A lost or duplicated event fails it.
async fn assert_history(conn: &mut AsyncPgConnection, exec: ExecutionId, expected: &[String]) {
    let events = history(conn, exec).await;
    let ids: Vec<i32> = events.iter().map(|(id, _)| *id).collect();
    let first = ids.iter().next().copied().unwrap_or(1);
    let gapless: Vec<i32> = (first..).take(ids.len()).collect();
    assert_eq!(ids, gapless, "event ids of {exec} have a gap: {events:?}");
    let types: Vec<String> = events.into_iter().map(|(_, t)| t).collect();
    assert_eq!(types, expected, "history of {exec}");
}

async fn exec_state(conn: &mut AsyncPgConnection, exec: ExecutionId) -> String {
    #[derive(QueryableByName)]
    struct State {
        #[diesel(sql_type = Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
        .get_result::<State>(conn)
        .await
        .expect("read the state")
        .state
}

async fn wait_completed(conn: &mut AsyncPgConnection, exec: ExecutionId) {
    eventually(&format!("{exec} completes"), async || {
        exec_state(conn, exec).await == "COMPLETED"
    })
    .await;
}

fn opts(queue: &str) -> TypedStartOptions {
    TypedStartOptions {
        queue_name: Some(queue.to_string()),
        ..Default::default()
    }
}

/// A workflow id whose partition is in `partitions`.
fn workflow_id_in(prefix: &str, partitions: &BTreeSet<u16>) -> String {
    loop {
        let id = unique(prefix);
        if partitions.contains(&DecisionPartition::for_workflow_id(&id).get()) {
            return id;
        }
    }
}

/// The database clock, as seconds since the epoch.
async fn db_now(conn: &mut AsyncPgConnection) -> f64 {
    #[derive(QueryableByName)]
    struct Now {
        #[diesel(sql_type = Double)]
        t: f64,
    }
    diesel::sql_query("SELECT extract(epoch FROM clock_timestamp())::float8 AS t")
        .get_result::<Now>(conn)
        .await
        .expect("read the clock")
        .t
}

// ── Frozen owner ────────────────────────────────────────────────────────────

/// The parts of a frozen-owner test: a child that owns every partition and
/// one run in the child's hands.
struct Frozen {
    db: OwnDb,
    conn: AsyncPgConnection,
    queue: String,
    child: Child,
    exec: ExecutionId,
    partition: DecisionPartition,
}

impl Frozen {
    /// Start the child, wait until it owns every partition, and start one
    /// run of `workflow`.
    async fn start(workflow: &str) -> Self {
        let db = OwnDb::new().await;
        let mut conn = db.conn().await;
        enable_owner_mode(&mut conn).await;
        let queue = unique("failover-q");
        let child = Child::start(&db.url, &queue);
        let child_id = child.id.clone();
        wait_for_spread(&mut conn, &[child_id.as_str()], 512, 512).await;
        let workflow_id = unique("failover-frozen");
        let partition = DecisionPartition::for_workflow_id(&workflow_id);
        let client = WorkflowHandleClient::single(pool(&db.url, 2), db.url.clone());
        let exec = match workflow {
            "hold" => FailoverHoldFlowStub::start_with_options(
                &mut conn,
                &client,
                &workflow_id,
                queue.clone(),
                opts(&queue),
            )
            .await
            .expect("start")
            .exec_id(),
            _ => FailoverSignalFlowStub::start_with_options(
                &mut conn,
                &client,
                &workflow_id,
                queue.clone(),
                opts(&queue),
            )
            .await
            .expect("start")
            .exec_id(),
        };
        Self {
            db,
            conn,
            queue,
            child,
            exec,
            partition,
        }
    }

    async fn send_go(&mut self) {
        autumn_harvest::signal::send_signal(&mut self.conn, self.exec, "go", Value::Null)
            .await
            .expect("signal");
    }

    /// Stop the child, start a standby and wait until the standby holds the
    /// partition of the run at the next epoch. Returns the standby and the
    /// time from the stop to that takeover.
    async fn stop_and_take_over(&mut self) -> (Node, Duration) {
        let (holder, epoch) = owner_of(&mut self.conn, self.partition).await;
        assert_eq!(holder.as_deref(), Some(self.child.id.as_str()));
        self.child.signal("STOP");
        let stopped = Instant::now();
        let standby = Node::start(&self.db.url, "failover-standby", &self.queue);
        let partition = self.partition;
        let conn = &mut self.conn;
        let standby_id = standby.id.clone();
        eventually("the standby takes the partition", async || {
            owner_of(conn, partition).await == (Some(standby_id.clone()), epoch + 1)
        })
        .await;
        (standby, stopped.elapsed())
    }

    /// The history of the run on one worker.
    fn expected_history() -> Vec<String> {
        [
            "WorkflowStarted",
            "SignalReceived",
            "ActivityScheduled",
            "ActivityStarted",
            "ActivityCompleted",
            "WorkflowCompleted",
        ]
        .map(String::from)
        .to_vec()
    }
}

/// The owner stops inside a decision, after it pinned its tenure and before
/// its commit. The standby takes the partition and completes the run. Then
/// the owner wakes and commits. The epoch fence must refuse that commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_frozen_owner_is_fenced_after_a_takeover() {
    let mut t = Frozen::start("hold").await;
    t.send_go().await;
    let held = t.child.marker("held");
    eventually("the child holds the decision", async || held.exists()).await;

    let (standby, takeover) = t.stop_and_take_over().await;
    // The child still holds its claim. The reclaimer re-pends it once the
    // child misses its heartbeats. The standby then completes the run.
    wait_completed(&mut t.conn, t.exec).await;
    let before = history(&mut t.conn, t.exec).await;

    std::fs::write(t.child.marker("release"), b"go").expect("release the hold");
    t.child.signal("CONT");
    let child = &t.child;
    eventually("the woken owner is fenced", async || child.fences() >= 1).await;

    assert_eq!(
        history(&mut t.conn, t.exec).await,
        before,
        "the fenced commit wrote nothing"
    );
    assert_history(&mut t.conn, t.exec, &Frozen::expected_history()).await;
    assert!(
        standby.fences.times().is_empty(),
        "the standby is never fenced"
    );
    eprintln!("frozen owner: takeover {takeover:?} after SIGSTOP");
    standby.stop().await;
    drop(t.child);
    t.db.drop_db().await;
}

/// The owner stops inside its open commit transaction, while it holds the
/// owner row `FOR KEY SHARE`. The idle timeout that the fence sets must end
/// that transaction, so the standby takes the partition while the owner is
/// still stopped. The woken owner then writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_frozen_commit_transaction_does_not_block_the_takeover() {
    let mut t = Frozen::start("signal").await;
    // The fence sets `idle_in_transaction_session_timeout`. So a transaction
    // with another value than this session has passed the fence. The first
    // event insert of such a transaction for the run sleeps for 2 s. The
    // sequence is not transactional, so the sleep happens only once.
    let idle: String = {
        #[derive(QueryableByName)]
        struct Setting {
            #[diesel(sql_type = Text)]
            v: String,
        }
        diesel::sql_query("SELECT current_setting('idle_in_transaction_session_timeout') AS v")
            .get_result::<Setting>(&mut t.conn)
            .await
            .expect("read the setting")
            .v
    };
    t.conn
        .batch_execute(&format!(
            "CREATE SEQUENCE failover_hold_once; \
             CREATE FUNCTION failover_hold() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
                 IF NEW.workflow_exec_id = '{exec}'::uuid \
                    AND current_setting('idle_in_transaction_session_timeout') <> '{idle}' THEN \
                     IF nextval('failover_hold_once') = 1 THEN \
                         PERFORM pg_sleep(2); \
                     END IF; \
                 END IF; \
                 RETURN NEW; \
             END $$; \
             CREATE TRIGGER failover_hold_trg BEFORE INSERT ON harvest_events \
                 FOR EACH ROW EXECUTE FUNCTION failover_hold();",
            exec = t.exec.as_uuid(),
        ))
        .await
        .expect("install the hold trigger");
    t.send_go().await;

    #[derive(QueryableByName)]
    struct Pid {
        #[diesel(sql_type = Integer)]
        pid: i32,
    }
    let conn = &mut t.conn;
    let mut sleeper = None;
    eventually("the owner's commit sleeps in the trigger", async || {
        let rows: Vec<Pid> = diesel::sql_query(
            "SELECT pid FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event = 'PgSleep'",
        )
        .load(conn)
        .await
        .expect("read the backends");
        sleeper = rows.iter().next().map(|r| r.pid);
        sleeper.is_some()
    })
    .await;
    let sleeper = sleeper.expect("a sleeping backend");

    let (standby, takeover) = t.stop_and_take_over().await;
    let conn = &mut t.conn;
    let gone: bool = {
        #[derive(QueryableByName)]
        struct Alive {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        diesel::sql_query("SELECT count(*)::bigint AS n FROM pg_stat_activity WHERE pid = $1")
            .bind::<Integer, _>(sleeper)
            .get_result::<Alive>(conn)
            .await
            .expect("read the backend")
            .n
            == 0
    };
    assert!(gone, "the idle timeout ended the frozen transaction");
    wait_completed(&mut t.conn, t.exec).await;
    let before = history(&mut t.conn, t.exec).await;

    t.child.signal("CONT");
    // The woken owner renews nothing and drops its tenures. Its candidate
    // row then moves again, so its runtime has run after the wake.
    let child_id = t.child.id.clone();
    let woke = db_now(&mut t.conn).await;
    let conn = &mut t.conn;
    eventually("the woken owner runs again", async || {
        #[derive(QueryableByName)]
        struct Seen {
            #[diesel(sql_type = Double)]
            t: f64,
        }
        let seen: Vec<Seen> = diesel::sql_query(
            "SELECT extract(epoch FROM seen_at)::float8 AS t \
             FROM harvest_decision_owner_candidates WHERE split_part(holder, '/', 1) = $1",
        )
        .bind::<Text, _>(&child_id)
        .load(conn)
        .await
        .expect("read the candidates");
        seen.iter().any(|s| s.t > woke)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_eq!(
        history(&mut t.conn, t.exec).await,
        before,
        "the frozen commit wrote nothing"
    );
    assert_history(&mut t.conn, t.exec, &Frozen::expected_history()).await;
    eprintln!("frozen commit: takeover {takeover:?} after SIGSTOP");
    standby.stop().await;
    drop(t.child);
    t.db.drop_db().await;
}

// ── kill -9 ─────────────────────────────────────────────────────────────────

#[derive(QueryableByName)]
struct LeaseEnd {
    #[diesel(sql_type = diesel::sql_types::Nullable<Double>)]
    t: Option<f64>,
}

/// The latest lease end of `worker`, as seconds since the epoch.
async fn lease_end_of(conn: &mut AsyncPgConnection, worker: &str) -> Option<f64> {
    diesel::sql_query(
        "SELECT extract(epoch FROM max(lease_until))::float8 AS t \
         FROM harvest_decision_owner WHERE split_part(holder, '/', 1) = $1",
    )
    .bind::<Text, _>(worker)
    .get_result::<LeaseEnd>(conn)
    .await
    .expect("read the lease end")
    .t
}

/// The earliest lease end of `partitions`, as seconds since the epoch.
async fn first_acquisition(conn: &mut AsyncPgConnection, partitions: &BTreeSet<u16>) -> f64 {
    let list: Vec<i32> = partitions.iter().map(|p| i32::from(*p)).collect();
    diesel::sql_query(
        "SELECT extract(epoch FROM min(lease_until))::float8 AS t \
         FROM harvest_decision_owner WHERE partition = ANY($1)",
    )
    .bind::<diesel::sql_types::Array<Integer>, _>(&list)
    .get_result::<LeaseEnd>(conn)
    .await
    .expect("read the lease ends")
    .t
    .expect("the partitions exist")
}

/// The first decision commit of `execs` that started after `after`.
async fn first_decision_after(
    conn: &mut AsyncPgConnection,
    execs: &[ExecutionId],
    after: f64,
) -> Option<f64> {
    let ids: Vec<uuid::Uuid> = execs.iter().map(ExecutionId::as_uuid).collect();
    diesel::sql_query(
        "SELECT extract(epoch FROM min(timestamp))::float8 AS t FROM harvest_events \
         WHERE workflow_exec_id = ANY($1) AND event_type = ANY($2) \
           AND timestamp > to_timestamp($3)",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
    .bind::<diesel::sql_types::Array<Text>, _>(DECISION_EVENTS.to_vec())
    .bind::<Double, _>(after)
    .get_result::<LeaseEnd>(conn)
    .await
    .expect("read the first decision")
    .t
}

/// Kill the owner with `SIGKILL` just after it renews, so its lease has a
/// full TTL left. The next decision on its partitions must run within
/// TTL + 1 s of the kill.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_kill_9_the_next_decision_runs_within_ttl_plus_one_second() {
    use std::os::unix::process::ExitStatusExt;

    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable_owner_mode(&mut conn).await;
    let queue = unique("failover-kill-q");
    let mut child = Child::start(&db.url, &queue);
    let child_id = child.id.clone();
    wait_for_spread(&mut conn, &[child_id.as_str()], 512, 512).await;
    let standby = Node::start(&db.url, "failover-survivor", &queue);
    wait_for_spread(
        &mut conn,
        &[child_id.as_str(), standby.id.as_str()],
        256,
        256,
    )
    .await;

    // Half of the runs in each owner's partitions.
    let theirs = partitions_of(&mut conn, &child_id).await;
    let ours = partitions_of(&mut conn, &standby.id).await;
    let client = WorkflowHandleClient::single(pool(&db.url, 2), db.url.clone());
    let mut doomed = Vec::new();
    let mut kept = Vec::new();
    for i in 0..24 {
        let (set, list) = if i % 2 == 0 {
            (&theirs, &mut doomed)
        } else {
            (&ours, &mut kept)
        };
        let id = workflow_id_in("failover-kill", set);
        let exec =
            FailoverTimersStub::start_with_options(&mut conn, &client, &id, TIMERS, opts(&queue))
                .await
                .expect("start")
                .exec_id();
        list.push(exec);
    }
    // Let the runs reach their timer loops on both owners.
    let started = db_now(&mut conn).await;
    eventually("the child decides", async || {
        first_decision_after(&mut conn, &doomed, started)
            .await
            .is_some()
    })
    .await;

    // Kill just after a renewal moves the lease end.
    let renewed = lease_end_of(&mut conn, &child_id).await;
    eventually("the child renews", async || {
        lease_end_of(&mut conn, &child_id).await > renewed
    })
    .await;
    let killed_at = db_now(&mut conn).await;
    child.process.kill().expect("SIGKILL the child");
    let status = child.process.wait().expect("reap the child");
    assert_eq!(status.signal(), Some(9), "death by SIGKILL: {status:?}");
    let dead_at = db_now(&mut conn).await;
    let lease_end = lease_end_of(&mut conn, &child_id)
        .await
        .expect("the child held partitions");

    let survivor = standby.id.clone();
    wait_for_spread(&mut conn, &[survivor.as_str()], 512, 512).await;
    // The survivor has not renewed yet, so each new lease end is its
    // acquisition time + TTL.
    let taken = first_acquisition(&mut conn, &theirs).await - TTL.as_secs_f64() - killed_at;
    let mut next = None;
    eventually("a decision runs on the child's partitions", async || {
        next = first_decision_after(&mut conn, &doomed, dead_at).await;
        next.is_some()
    })
    .await;
    let next = next.expect("a decision") - killed_at;
    let residual = lease_end - killed_at;
    eprintln!(
        "kill -9: residual lease {residual:.3} s, takeover {taken:.3} s, \
         next decision {next:.3} s after the kill"
    );
    assert!(
        residual > TTL.as_secs_f64() - 1.0,
        "the kill landed just after a renewal: residual {residual:.3} s"
    );
    let bound = TTL.as_secs_f64() + 1.0;
    assert!(
        next <= bound,
        "the next decision ran {next:.3} s after the kill, over TTL + 1 s"
    );

    // Every run completes once, with no lost or duplicated event.
    let expected = timer_history(TIMERS);
    for exec in doomed.iter().chain(&kept) {
        wait_completed(&mut conn, *exec).await;
        assert_history(&mut conn, *exec, &expected).await;
    }
    standby.stop().await;
    drop(child);
    db.drop_db().await;
}

/// The timers of each run in the kill test. The run outlives the takeover.
const TIMERS: u32 = 25;

/// The history of a [`failover_timers`] run on one worker.
fn timer_history(n: u32) -> Vec<String> {
    let mut expected = vec!["WorkflowStarted".to_string()];
    for _ in 0..n {
        expected.push("TimerStarted".to_string());
        expected.push("TimerFired".to_string());
    }
    expected.push("WorkflowCompleted".to_string());
    expected
}

// ── Scale-out ───────────────────────────────────────────────────────────────

/// Runs in flight during the scale-out test.
const INFLIGHT: usize = 32;

/// Keep [`INFLIGHT`] runs of [`failover_steps`] in flight until `stop`.
/// Returns every run that it started.
async fn drive_load(url: String, queue: String, stop: Arc<AtomicBool>) -> Vec<ExecutionId> {
    let client = WorkflowHandleClient::single(pool(&url, 2), url.clone());
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let mut all = Vec::new();
    let mut open: VecDeque<ExecutionId> = VecDeque::new();
    // Fully qualified: `RunQueryDsl` puts a `load` method on every type.
    while !AtomicBool::load(&stop, Ordering::SeqCst) {
        while open.len() < INFLIGHT {
            let id = unique("failover-load");
            let exec = FailoverStepsStub::start_with_options(
                &mut conn,
                &client,
                &id,
                (queue.clone(), 3),
                opts(&queue),
            )
            .await
            .expect("start")
            .exec_id();
            open.push_back(exec);
            all.push(exec);
        }
        let ids: Vec<uuid::Uuid> = open.iter().map(ExecutionId::as_uuid).collect();
        #[derive(QueryableByName)]
        struct Done {
            #[diesel(sql_type = diesel::sql_types::Uuid)]
            id: uuid::Uuid,
        }
        let done: BTreeSet<uuid::Uuid> = diesel::sql_query(
            "SELECT id FROM harvest_workflow_executions \
             WHERE id = ANY($1) AND state = 'COMPLETED'",
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
        .load::<Done>(&mut conn)
        .await
        .expect("read the states")
        .into_iter()
        .map(|d| d.id)
        .collect();
        open.retain(|e| !done.contains(&e.as_uuid()));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    all
}

/// Decision commits per second of `execs`, by second since `origin`.
async fn decisions_per_second(
    conn: &mut AsyncPgConnection,
    execs: &[ExecutionId],
    origin: f64,
) -> BTreeMap<i32, i64> {
    #[derive(QueryableByName)]
    struct Bucket {
        #[diesel(sql_type = Integer)]
        b: i32,
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let ids: Vec<uuid::Uuid> = execs.iter().map(ExecutionId::as_uuid).collect();
    diesel::sql_query(
        "SELECT floor(extract(epoch FROM timestamp) - $3)::int4 AS b, count(*)::bigint AS n \
         FROM harvest_events WHERE workflow_exec_id = ANY($1) AND event_type = ANY($2) \
         GROUP BY 1 ORDER BY 1",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
    .bind::<diesel::sql_types::Array<Text>, _>(DECISION_EVENTS.to_vec())
    .bind::<Double, _>(origin)
    .load::<Bucket>(conn)
    .await
    .expect("read the throughput")
    .into_iter()
    .map(|r| (r.b, r.n))
    .collect()
}

/// What the scale-out test measures.
#[derive(Debug)]
struct ScaleOut {
    /// Decision commits per second before the scale-out.
    baseline: f64,
    /// Decision commits per second over the last 10 s of the window.
    steady: f64,
    /// The slowest second after the scale-out, over the baseline.
    dip: f64,
    /// The time until each worker holds its target.
    settled: Duration,
    /// The later of the settle and the throughput recovery.
    recovered: Duration,
    fences_before: usize,
    fences_after: usize,
}

/// The first second from which each 3 s window holds at least 80% of
/// `level`, up to second `end`.
fn recovery_second(series: &BTreeMap<i32, i64>, level: f64, end: i32) -> Option<i32> {
    let rate = |b: i32| series.get(&b).copied().unwrap_or(0);
    // Precision loss is fine: these are small counts.
    #[allow(clippy::cast_precision_loss)]
    let window = |b: i32| (rate(b) + rate(b + 1) + rate(b + 2)) as f64 / 3.0;
    (0..end - 2).find(|start| (*start..end - 2).all(|b| window(b) >= 0.8 * level))
}

/// The mean rate of the seconds `from..to`.
fn mean_rate(series: &BTreeMap<i32, i64>, from: i32, to: i32) -> f64 {
    let sum: i64 = (from..to)
        .map(|b| series.get(&b).copied().unwrap_or(0))
        .sum();
    // Precision loss is fine: these are small counts.
    #[allow(clippy::cast_precision_loss)]
    let mean = sum as f64 / f64::from((to - from).max(1));
    mean
}

/// Grow the fleet from 4 to 8 workers under load, and measure it.
///
/// With `owner_mode` off, it is the control: no owner rows, so it reports
/// no settle time and no fence.
async fn run_scale_out(owner_mode: bool) -> ScaleOut {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    if owner_mode {
        enable_owner_mode(&mut conn).await;
    }
    let queue = unique("failover-scale-q");
    let mut nodes: Vec<Node> = (0..4)
        .map(|_| Node::start(&db.url, "failover-scale", &queue))
        .collect();
    let first: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
    let first_ids: Vec<&str> = first.iter().map(String::as_str).collect();
    if owner_mode {
        wait_for_spread(&mut conn, &first_ids, 128, 128).await;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let load = tokio::spawn(drive_load(db.url.clone(), queue.clone(), Arc::clone(&stop)));
    tokio::time::sleep(BASELINE + WARM_UP).await;

    let origin = db_now(&mut conn).await;
    let scaled = Instant::now();
    nodes.extend((0..4).map(|_| Node::start(&db.url, "failover-scale", &queue)));
    let all: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
    let all_ids: Vec<&str> = all.iter().map(String::as_str).collect();
    if owner_mode {
        // Each worker holds its target of 64. A worker may keep one more
        // until its next scan.
        wait_for_spread(&mut conn, &all_ids, 63, 65).await;
    }
    let settled = scaled.elapsed();
    let settled_at = scaled + settled;
    tokio::time::sleep(AFTER.saturating_sub(settled)).await;
    stop.store(true, Ordering::SeqCst);
    let execs = load.await.expect("the load driver");
    for exec in &execs {
        wait_completed(&mut conn, *exec).await;
    }

    let series = decisions_per_second(&mut conn, &execs, origin).await;
    // The last second is partial, because the load stops in it.
    let end = i32::try_from(AFTER.as_secs()).expect("fits") - 1;
    let baseline = mean_rate(&series, -baseline_secs(), 0);
    // Eight workers in one process can settle at another rate than four.
    // So recovery is to the lower of the two rates.
    let steady = mean_rate(&series, end - 10, end);
    let back = recovery_second(&series, baseline.min(steady), end).unwrap_or(end);
    // Precision loss is fine: these are small counts.
    #[allow(clippy::cast_precision_loss)]
    let dip = (0..end)
        .map(|b| series.get(&b).copied().unwrap_or(0))
        .min()
        .unwrap_or(0) as f64
        / baseline;
    let recovered = settled.max(Duration::from_secs(u64::try_from(back).expect("fits")));
    let fences: Vec<Instant> = nodes.iter().flat_map(|n| n.fences.times()).collect();
    let report = ScaleOut {
        baseline,
        steady,
        dip,
        settled,
        recovered,
        fences_before: fences.iter().filter(|t| **t <= settled_at).count(),
        fences_after: fences.iter().filter(|t| **t > settled_at).count(),
    };
    eprintln!("scale-out (owner mode {owner_mode}): {report:?}\nseries: {series:?}");

    let expected: Vec<String> = [
        "WorkflowStarted",
        "ActivityScheduled",
        "ActivityStarted",
        "ActivityCompleted",
        "ActivityScheduled",
        "ActivityStarted",
        "ActivityCompleted",
        "ActivityScheduled",
        "ActivityStarted",
        "ActivityCompleted",
        "WorkflowCompleted",
    ]
    .map(String::from)
    .to_vec();
    for exec in &execs {
        assert_history(&mut conn, *exec, &expected).await;
    }
    for node in nodes {
        node.stop().await;
    }
    db.drop_db().await;
    report
}

/// The fleet grows from 4 to 8 workers under load. It must settle and
/// recover within two lease TTLs. No commit is fenced after it settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scale_out_from_4_to_8_workers_recovers_within_two_ttls() {
    let report = run_scale_out(true).await;
    assert!(
        report.recovered <= TTL * 2,
        "recovery took over two TTLs: {report:?}"
    );
    assert_eq!(
        report.fences_after, 0,
        "a commit was fenced after the settle"
    );
}

/// The control of the test above, with owner mode off. It prints the
/// rates and asserts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, not a gate"]
async fn scale_out_without_owner_mode_measurement() {
    let report = run_scale_out(false).await;
    eprintln!("legacy scale-out: {report:?}");
}

/// The load runs this long before the baseline window.
const WARM_UP: Duration = Duration::from_secs(5);
/// The baseline window, just before the scale-out.
const BASELINE: Duration = Duration::from_secs(10);
/// The load runs this long after the scale-out.
const AFTER: Duration = Duration::from_secs(30);

fn baseline_secs() -> i32 {
    i32::try_from(BASELINE.as_secs()).expect("fits")
}

#[test]
fn recovery_second_finds_the_first_sustained_window() {
    let series: BTreeMap<i32, i64> = [(0, 10), (1, 2), (2, 2), (3, 9), (4, 10), (5, 10), (6, 10)]
        .into_iter()
        .collect();
    assert_eq!(recovery_second(&series, 10.0, 7), Some(3));
    assert_eq!(recovery_second(&series, 100.0, 7), None);
    assert!((mean_rate(&series, 4, 7) - 10.0).abs() < f64::EPSILON);
    assert!(
        mean_rate(&series, 9, 9).abs() < f64::EPSILON,
        "an empty range"
    );
}

#[test]
fn spread_holds_needs_every_partition_and_each_worker_in_range() {
    let held: BTreeMap<String, i64> = [("a".to_string(), 256), ("b".to_string(), 256)]
        .into_iter()
        .collect();
    assert!(spread_holds(&held, &["a", "b"], 256, 256));
    assert!(!spread_holds(&held, &["a"], 256, 256));
    assert!(!spread_holds(&held, &["a", "b"], 300, 512));
    let short: BTreeMap<String, i64> = [("a".to_string(), 256)].into_iter().collect();
    assert!(!spread_holds(&short, &["a"], 0, 512));
}

#[test]
fn hold_in_child_does_nothing_outside_the_child() {
    // The parent never sets the marker directory.
    assert!(std::env::var_os(CHILD_DIR_VAR).is_none());
    hold_in_child();
}
