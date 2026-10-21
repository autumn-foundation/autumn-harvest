#![cfg(feature = "db")]
#![allow(clippy::unused_async)]
//! Foreign writers wake the partition owner and evict its cache (issue
//! #2253).
//!
//! Each test runs two workers on one queue, with owner mode on in both.
//! Worker A holds the tenure of the run's partition. Worker B holds none, so
//! a decision of B fails closed and re-pends. Each writer class must wake A,
//! and B must prepare no decision.
//!
//! A pause, a reset or a terminate from another connection must evict the
//! entry of A. The next decision then replays cold.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, OwnedPartitions, OwnerHolder, OwnerKey, OwnerLeaseConfig,
    OwnerTenure,
};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use diesel::QueryableByName;
use diesel::sql_types::Integer;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::Value;

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

/// This suite owns partitions 300 to 399. Each test takes its own slot.
const BASE: u16 = 300;

/// The partitions of the other owner suites. A child must not land there.
const OTHER_SUITES: std::ops::RangeInclusive<u16> = 300..=499;

const WAIT: Duration = Duration::from_secs(30);

#[workflow]
async fn owner_wake_signal_flow(ctx: &WorkflowContext, input: String) -> Result<String, String> {
    let _ = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(format!("done:{input}"))
}

#[workflow]
async fn owner_wake_activity_flow(ctx: &WorkflowContext, queue: String) -> Result<String, String> {
    let key = ctx.info().workflow_id.clone();
    let out = ctx
        .execute_activity_raw("owner_wake_echo", Value::String(key), &queue)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("echo:{}", out.as_str().unwrap_or_default()))
}

#[workflow]
async fn owner_wake_parent_flow(ctx: &WorkflowContext, input: String) -> Result<String, String> {
    let out = ctx
        .spawn_child_workflow_raw("owner_wake_signal_flow", Value::String(input))
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("parent:{}", out.as_str().unwrap_or_default()))
}

#[update(workflow = "owner_wake_signal_flow")]
async fn owner_wake_bump(_ctx: &WorkflowContext, by: i64) -> Result<i64, String> {
    Ok(by + 1)
}

/// The activity keys that a test released. A held activity waits, so the
/// test can see its result as a separate foreign write.
static RELEASED: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn release(key: &str) {
    RELEASED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(key.to_string());
}

#[activity]
async fn owner_wake_echo(_ctx: &ActivityContext, key: Value) -> Result<Value, String> {
    let key = key.as_str().unwrap_or_default().to_string();
    for _ in 0..1200 {
        let released = RELEASED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&key);
        if released {
            return Ok(Value::String("x".into()));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err("never released".into())
}

/// Counts decisions, fences and owner wakes of one worker.
#[derive(Default, Clone)]
struct Counter {
    decisions: Arc<AtomicUsize>,
    hits: Arc<AtomicUsize>,
    misses: Arc<AtomicUsize>,
    woken: Arc<Mutex<Vec<String>>>,
}

impl Counter {
    fn decisions(&self) -> usize {
        AtomicUsize::load(&self.decisions, Ordering::SeqCst)
    }

    fn misses(&self) -> usize {
        AtomicUsize::load(&self.misses, Ordering::SeqCst)
    }

    fn hits(&self) -> usize {
        AtomicUsize::load(&self.hits, Ordering::SeqCst)
    }

    fn woken(&self, kind: &str) -> usize {
        self.woken
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|k| k.as_str() == kind)
            .count()
    }
}

impl MetricsRecorder for Counter {
    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.decisions.fetch_add(1, Ordering::SeqCst);
        self.hits.fetch_add(1, Ordering::SeqCst);
    }

    fn record_workflow_cache_miss(&self, _workflow_name: &str, _queue: &str) {
        self.decisions.fetch_add(1, Ordering::SeqCst);
        self.misses.fetch_add(1, Ordering::SeqCst);
    }

    fn record_decision_owner_woken(&self, _shard: &str, kind: &str) {
        self.woken
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(kind.to_string());
    }
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

/// A workflow id in partition `BASE + slot`.
fn workflow_id_in(slot: u16) -> (String, DecisionPartition) {
    let target = DecisionPartition::new(BASE + slot).expect("in range");
    loop {
        let id = unique("owner-wake");
        if DecisionPartition::for_workflow_id(&id) == target {
            return (id, target);
        }
    }
}

/// The shard key of a single-pool worker.
fn shard() -> ShardId {
    OwnerKey::pool_shard(None)
}

/// A worker, its metrics and its tenures.
struct Node {
    id: String,
    worker: Arc<Worker>,
    owned: Arc<OwnedPartitions>,
    metrics: Counter,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    async fn stop(mut self) {
        self.worker.shutdown();
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
        }
    }
}

struct Fixture {
    url: String,
    pool: DbPool,
    conn: AsyncPgConnection,
    queue: String,
}

impl Fixture {
    async fn new() -> (Self, Option<impl Sized>) {
        let (url, container) = setup_test_database_url_or_env().await;
        let pool: DbPool = build_test_pool(&url);
        let conn = AsyncPgConnection::establish(&url).await.expect("connect");
        (
            Self {
                url,
                pool,
                conn,
                queue: unique("owner-wake-q"),
            },
            container,
        )
    }

    fn client(&self) -> WorkflowHandleClient {
        WorkflowHandleClient::single(self.pool.clone(), self.url.clone())
    }

    fn opts(&self) -> TypedStartOptions {
        TypedStartOptions {
            queue_name: Some(self.queue.clone()),
            ..Default::default()
        }
    }

    /// A worker in owner mode. With `listen`, it gets the notify URL, so it
    /// listens for queue and owner wakes.
    fn node(&self, prefix: &str, listen: bool) -> Node {
        let metrics = Counter::default();
        let built = HarvestBuilder::new()
            .workflows(workflows![
                owner_wake_signal_flow,
                owner_wake_activity_flow,
                owner_wake_parent_flow
            ])
            .activities(activities![owner_wake_echo])
            .updates(updates![owner_wake_bump])
            .worker(WorkerConfig::default().with_queues([self.queue.as_str()]))
            .telemetry(
                TelemetryConfig::builder()
                    .metrics(Arc::new(metrics.clone()))
                    .build(),
            )
            .build();
        let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
        let owned = registry.decision_owner();
        owned.set_owner_mode(shard(), true);
        let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
        let id = unique(prefix);
        runtime_config.worker_id.clone_from(&id);
        runtime_config.poll_interval = Duration::from_millis(100);
        runtime_config.notification_database_url = listen.then(|| self.url.clone());
        let worker = Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker"));
        Node {
            id,
            worker,
            owned,
            metrics,
            task: None,
        }
    }

    fn run(&self, node: &mut Node) {
        node.task = Some(spawn_test_worker(
            Arc::clone(&node.worker),
            self.pool.clone(),
        ));
    }

    /// Free `partition` and acquire it for `node`.
    async fn take(&mut self, node: &Node, partition: DecisionPartition) -> OwnerTenure {
        diesel::sql_query(
            "UPDATE harvest_decision_owner SET holder = NULL, lease_until = '-infinity' \
             WHERE partition = $1",
        )
        .bind::<Integer, _>(i32::from(partition.get()))
        .execute(&mut self.conn)
        .await
        .expect("free the row");
        // A long TTL: no renewal loop runs until issue #2252.
        let ttl = decision_owner::OwnerLeaseTtl::new(Duration::from_secs(300)).expect("ttl");
        let config = OwnerLeaseConfig::default().with_ttl(ttl);
        let holder = OwnerHolder::for_worker(&node.id);
        let Acquisition::Acquired(tenure) =
            decision_owner::acquire(&mut self.conn, shard(), &holder, partition, &config)
                .await
                .expect("acquire")
        else {
            panic!("the free row is acquired");
        };
        node.owned.adopt(tenure.clone());
        tenure
    }

    /// The owner A of `slot` and the non-owner B, both running.
    async fn pair(&mut self, slot: u16) -> (String, Node, Node) {
        let (workflow_id, partition) = workflow_id_in(slot);
        let mut a = self.node("owner-a", true);
        let mut b = self.node("owner-b", true);
        self.take(&a, partition).await;
        self.run(&mut a);
        self.run(&mut b);
        listening(&a, partition).await;
        (workflow_id, a, b)
    }

    async fn conn(&self) -> AsyncPgConnection {
        AsyncPgConnection::establish(&self.url)
            .await
            .expect("connect")
    }
}

/// Wait until the owner wake task of `node` listens on `partition`. A wake
/// before that would be lost.
async fn listening(node: &Node, partition: DecisionPartition) {
    let key = OwnerKey {
        shard: shard(),
        partition,
    };
    eventually("the owner listens on its partition", async || {
        node.worker.owner_wake_listening(key)
    })
    .await;
}

/// Wait until `check` holds, up to [`WAIT`].
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while tokio::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out: {what}");
}

/// The owner decided, it woke for a foreign write, and B decided nothing.
fn assert_only_the_owner_decided(a: &Node, b: &Node) {
    assert!(a.metrics.decisions() > 0, "the owner decides");
    assert!(
        a.metrics.woken("wake") + a.metrics.woken("evict") > 0,
        "a foreign write woke the owner"
    );
    assert_eq!(b.metrics.decisions(), 0, "no non-owner decides");
}

/// Whether the run has a recorded non-determinism block.
async fn nd_blocked(conn: &mut AsyncPgConnection, exec: ExecutionId) -> bool {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        blocked: bool,
    }
    diesel::sql_query(
        "SELECT nd_blocked_at IS NOT NULL AS blocked FROM harvest_workflow_executions \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .get_result::<Row>(conn)
    .await
    .expect("read the run")
    .blocked
}

async fn start_signal_flow(fx: &mut Fixture, workflow_id: &str) -> ExecutionId {
    let (client, opts) = (fx.client(), fx.opts());
    OwnerWakeSignalFlowStub::start_with_options(
        &mut fx.conn,
        &client,
        workflow_id,
        "x".to_string(),
        opts,
    )
    .await
    .expect("start")
    .exec_id()
}

/// Start the signal flow and wait until A caches its suspension.
async fn start_cached(fx: &mut Fixture, workflow_id: &str, a: &Node) -> ExecutionId {
    let exec = start_signal_flow(fx, workflow_id).await;
    eventually("the owner caches the suspension", async || {
        a.worker.has_cached_execution(exec).await
    })
    .await;
    exec
}

async fn signal(fx: &Fixture, exec: ExecutionId) {
    let mut conn = fx.conn().await;
    autumn_harvest::signal::send_signal(&mut conn, exec, "go", Value::Null)
        .await
        .expect("signal");
}

// ---------------------------------------------------------------------------
// One test per writer class
// ---------------------------------------------------------------------------

/// An activity result is a foreign write. It wakes the owner.
#[tokio::test]
async fn an_activity_result_wakes_the_owner_and_no_non_owner_decides() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, a, b) = fx.pair(0).await;
    let (client, opts) = (fx.client(), fx.opts());
    let exec = OwnerWakeActivityFlowStub::start_with_options(
        &mut fx.conn,
        &client,
        &workflow_id,
        fx.queue.clone(),
        opts,
    )
    .await
    .expect("start")
    .exec_id();
    // The run waits for the held activity. Its result is the write under test.
    eventually("the owner caches the activity wait", async || {
        a.worker.has_cached_execution(exec).await
    })
    .await;
    let before = a.metrics.woken("wake");
    release(&workflow_id);

    let done = wait_for_execution_state_with_timeout(&fx.url, exec, "COMPLETED", WAIT).await;
    assert_eq!(
        done.output.as_ref().map(ToString::to_string),
        Some(r#""echo:x""#.to_string())
    );
    assert!(
        a.metrics.woken("wake") > before,
        "the activity result woke the owner"
    );
    assert_only_the_owner_decided(&a, &b);
    a.stop().await;
    b.stop().await;
}

/// A signal stages a row and wakes the parked task. It wakes the owner.
#[tokio::test]
async fn a_signal_wakes_the_owner_and_no_non_owner_decides() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, a, b) = fx.pair(1).await;
    let exec = start_cached(&mut fx, &workflow_id, &a).await;
    let before = a.metrics.woken("wake");
    signal(&fx, exec).await;

    wait_for_execution_state_with_timeout(&fx.url, exec, "COMPLETED", WAIT).await;
    assert!(
        a.metrics.woken("wake") > before,
        "the signal woke the owner"
    );
    assert_only_the_owner_decided(&a, &b);
    a.stop().await;
    b.stop().await;
}

/// The timeout scanner writes an execution timeout. It wakes and evicts.
///
/// A durable timer is no foreign write: the decision that starts it also
/// schedules its task. The scanner of an execution deadline is one.
#[tokio::test]
async fn a_timeout_scanner_write_evicts_the_owner_entry() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, a, b) = fx.pair(2).await;
    let (client, mut opts) = (fx.client(), fx.opts());
    opts.execution_timeout = Some(Duration::from_secs(15));
    let exec = OwnerWakeSignalFlowStub::start_with_options(
        &mut fx.conn,
        &client,
        &workflow_id,
        "x".to_string(),
        opts,
    )
    .await
    .expect("start")
    .exec_id();
    eventually("the owner caches the suspension", async || {
        a.worker.has_cached_execution(exec).await
    })
    .await;

    wait_for_execution_state_with_timeout(&fx.url, exec, "TIMED_OUT", WAIT).await;
    eventually("the timeout evicts the owner's entry", async || {
        !a.worker.has_cached_execution(exec).await
    })
    .await;
    assert!(a.metrics.woken("evict") > 0, "an evict wake reached A");
    assert_only_the_owner_decided(&a, &b);
    a.stop().await;
    b.stop().await;
}

/// An operator cancel writes the terminal from another connection. It wakes
/// the owner and evicts its entry.
#[tokio::test]
async fn a_cancel_wakes_the_owner_and_no_non_owner_decides() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, a, b) = fx.pair(3).await;
    let exec = start_cached(&mut fx, &workflow_id, &a).await;
    let mut conn = fx.conn().await;
    autumn_harvest::execution::cancel_workflow_execution(&mut conn, exec, "stop", &NoOpMetrics)
        .await
        .expect("cancel");

    wait_for_execution_state_with_timeout(&fx.url, exec, "CANCELLED", WAIT).await;
    eventually("the cancel evicts the owner's entry", async || {
        !a.worker.has_cached_execution(exec).await
    })
    .await;
    assert_only_the_owner_decided(&a, &b);
    a.stop().await;
    b.stop().await;
}

/// An update admission appends `UpdateAdmitted`, and its caller re-pends
/// the task. It wakes the owner. The admission takes the edge path of each
/// caller, as `declarative_update_tests` does.
#[tokio::test]
async fn an_update_admission_wakes_the_owner_and_no_non_owner_decides() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, a, b) = fx.pair(9).await;
    let exec = start_cached(&mut fx, &workflow_id, &a).await;
    let before = a.metrics.woken("wake");
    let mut conn = fx.conn().await;
    let update_id = autumn_harvest::types::UpdateId::new();
    autumn_harvest::store::admit_update_event(
        &mut conn,
        exec,
        update_id,
        "owner_wake_bump".to_string(),
        serde_json::json!(41),
        None,
    )
    .await
    .expect("admit");
    autumn_harvest::queue::wake_workflow_task(&mut conn, exec)
        .await
        .expect("wake");

    eventually("the owner runs the update", async || {
        crate::integration_e2e::load_history_from_url(&fx.url, exec)
            .await
            .events
            .iter()
            .any(|event| {
                matches!(
                    event,
                    autumn_harvest::event::WorkflowEvent::UpdateCompleted { update_id: id, .. }
                        if *id == update_id
                )
            })
    })
    .await;
    assert!(
        a.metrics.woken("wake") > before,
        "the admission woke the owner"
    );
    signal(&fx, exec).await;
    wait_for_execution_state_with_timeout(&fx.url, exec, "COMPLETED", WAIT).await;
    assert_only_the_owner_decided(&a, &b);
    a.stop().await;
    b.stop().await;
}

// ---------------------------------------------------------------------------
// State changes from another process evict the owner's entry
// ---------------------------------------------------------------------------

/// A pause evicts the entry. After the resume, the decision replays cold and
/// raises no non-determinism error.
#[tokio::test]
async fn a_pause_from_another_process_evicts_and_the_next_decision_replays_cold() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, a, b) = fx.pair(4).await;
    let exec = start_cached(&mut fx, &workflow_id, &a).await;
    let mut conn = fx.conn().await;
    autumn_harvest::execution::pause_workflow_execution(
        &mut conn,
        exec,
        Some("hold"),
        "oncall",
        &NoOpMetrics,
    )
    .await
    .expect("pause");

    eventually("the pause evicts the owner's entry", async || {
        !a.worker.has_cached_execution(exec).await
    })
    .await;
    assert!(a.metrics.woken("evict") > 0, "an evict wake reached A");

    // Signal while paused. The resume then wakes one decision, which reads
    // the signal and completes.
    let (hits, misses) = (a.metrics.hits(), a.metrics.misses());
    signal(&fx, exec).await;
    autumn_harvest::execution::resume_workflow_execution(&mut conn, exec, "oncall", &NoOpMetrics)
        .await
        .expect("resume");
    wait_for_execution_state_with_timeout(&fx.url, exec, "COMPLETED", WAIT).await;
    assert_eq!(a.metrics.hits(), hits, "no decision used the old entry");
    assert!(
        a.metrics.misses() > misses,
        "the next decision replays cold"
    );
    assert!(!nd_blocked(&mut fx.conn, exec).await, "no non-determinism");
    assert_eq!(b.metrics.decisions(), 0, "no non-owner decides");
    a.stop().await;
    b.stop().await;
}

/// A reset seals the source and forks it. The source entry is evicted, and
/// the fork decides cold.
#[tokio::test]
async fn a_reset_from_another_process_evicts_and_the_fork_replays_cold() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, a, b) = fx.pair(5).await;
    let exec = start_cached(&mut fx, &workflow_id, &a).await;
    let mut conn = fx.conn().await;
    let misses = a.metrics.misses();
    let reset = autumn_harvest::reset::reset_workflow_execution(
        &mut conn,
        exec,
        autumn_harvest::reset::WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "operator reset".to_string(),
            operator_id: "oncall".to_string(),
            signal_reapply: autumn_harvest::reset::ResetSignalReapplyPolicy::default(),
            allow_terminal_source: false,
        },
        None,
    )
    .await
    .expect("reset");

    eventually("the reset evicts the source entry", async || {
        !a.worker.has_cached_execution(exec).await
    })
    .await;
    assert!(a.metrics.woken("evict") > 0, "an evict wake reached A");

    // The fork keeps the workflow id, so A owns it too.
    let fork = reset.new_exec_id;
    eventually("the fork suspends", async || {
        a.worker.has_cached_execution(fork).await
    })
    .await;
    assert!(a.metrics.misses() > misses, "the fork decides cold");
    signal(&fx, fork).await;
    wait_for_execution_state_with_timeout(&fx.url, fork, "COMPLETED", WAIT).await;
    assert!(!nd_blocked(&mut fx.conn, fork).await, "no non-determinism");
    assert_eq!(b.metrics.decisions(), 0, "no non-owner decides");
    a.stop().await;
    b.stop().await;
}

/// A terminate evicts the entry. No later decision reads it.
#[tokio::test]
async fn a_terminate_from_another_process_evicts_the_entry() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, a, b) = fx.pair(6).await;
    let exec = start_cached(&mut fx, &workflow_id, &a).await;
    let mut conn = fx.conn().await;
    let hits = a.metrics.hits();
    autumn_harvest::execution::terminate_workflow_execution(&mut conn, exec, "stop", &NoOpMetrics)
        .await
        .expect("terminate");

    eventually("the terminate evicts the owner's entry", async || {
        !a.worker.has_cached_execution(exec).await
    })
    .await;
    assert!(a.metrics.woken("evict") > 0, "an evict wake reached A");
    // A late signal must not resume the terminated run from the old entry.
    let _ = autumn_harvest::signal::send_signal(&mut conn, exec, "go", Value::Null).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(a.metrics.hits(), hits, "no decision used the old entry");
    let run = crate::integration_e2e::load_execution_from_url(&fx.url, exec).await;
    assert_eq!(run.state, "TERMINATED");
    assert!(!nd_blocked(&mut fx.conn, exec).await, "no non-determinism");
    assert_eq!(b.metrics.decisions(), 0, "no non-owner decides");
    a.stop().await;
    b.stop().await;
}

/// With no owner listener, a lost evict wake still cannot cause a stale
/// decision. The hit sees the pause events in its delta and replays cold.
#[tokio::test]
async fn a_lost_evict_wake_still_replays_cold() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, partition) = workflow_id_in(7);
    // No notify URL: no queue listener and no owner listener.
    let mut a = fx.node("owner-a", false);
    fx.take(&a, partition).await;
    fx.run(&mut a);
    let exec = start_cached(&mut fx, &workflow_id, &a).await;
    let mut conn = fx.conn().await;
    autumn_harvest::execution::pause_workflow_execution(
        &mut conn,
        exec,
        None,
        "oncall",
        &NoOpMetrics,
    )
    .await
    .expect("pause");
    // A paused run claims no task, so the entry stays in place.
    assert!(
        a.worker.has_cached_execution(exec).await,
        "no wake evicted the entry"
    );
    let (hits, misses) = (a.metrics.hits(), a.metrics.misses());
    signal(&fx, exec).await;
    autumn_harvest::execution::resume_workflow_execution(&mut conn, exec, "oncall", &NoOpMetrics)
        .await
        .expect("resume");
    wait_for_execution_state_with_timeout(&fx.url, exec, "COMPLETED", WAIT).await;
    assert_eq!(a.metrics.hits(), hits, "the stale entry is not used");
    assert!(a.metrics.misses() > misses, "the decision replays cold");
    assert!(!nd_blocked(&mut fx.conn, exec).await, "no non-determinism");
    a.stop().await;
}

// ---------------------------------------------------------------------------
// A child terminal written into a parent that another worker owns
// ---------------------------------------------------------------------------

/// The child id of `parent`, once the parent spawned it.
async fn child_of(fx: &Fixture, parent: ExecutionId) -> ExecutionId {
    let mut found = None;
    eventually("the parent spawns its child", async || {
        let children =
            crate::integration_e2e::load_child_executions_from_url(&fx.url, parent).await;
        found = children
            .into_iter()
            .next()
            .map(|c| ExecutionId::from_uuid(c.id));
        found.is_some()
    })
    .await;
    found.expect("a child")
}

/// A child decided by A writes its terminal into a parent that B owns. The
/// write wakes B, and the next decision of B reads the child terminal.
#[tokio::test]
async fn a_child_terminal_wakes_and_invalidates_the_parent_owner_elsewhere() {
    let (mut fx, _container) = Fixture::new().await;
    // B owns the parent. A owns the child, once its partition is known.
    let mut a = fx.node("child-owner", true);
    let mut b = fx.node("parent-owner", true);
    fx.run(&mut a);
    fx.run(&mut b);

    let (parent, child) = loop {
        let (workflow_id, partition) = workflow_id_in(8);
        fx.take(&b, partition).await;
        listening(&b, partition).await;
        let (client, opts) = (fx.client(), fx.opts());
        let parent = OwnerWakeParentFlowStub::start_with_options(
            &mut fx.conn,
            &client,
            &workflow_id,
            "x".to_string(),
            opts,
        )
        .await
        .expect("start")
        .exec_id();
        let child = child_of(&fx, parent).await;
        let execution = crate::integration_e2e::load_execution_from_url(&fx.url, child).await;
        let child_partition = DecisionPartition::for_workflow_id(&execution.workflow_id);
        if child_partition != partition && !OTHER_SUITES.contains(&child_partition.get()) {
            fx.take(&a, child_partition).await;
            listening(&a, child_partition).await;
            break (parent, child);
        }
        // The child landed in a partition another suite uses. Try again.
        let mut conn = fx.conn().await;
        let _ = autumn_harvest::execution::terminate_workflow_execution(
            &mut conn,
            parent,
            "retry",
            &NoOpMetrics,
        )
        .await;
    };

    eventually("the parent owner caches the parent", async || {
        b.worker.has_cached_execution(parent).await
    })
    .await;
    eventually("the child owner caches the child", async || {
        a.worker.has_cached_execution(child).await
    })
    .await;
    let woken = b.metrics.woken("wake");
    signal(&fx, child).await;

    let done = wait_for_execution_state_with_timeout(&fx.url, parent, "COMPLETED", WAIT).await;
    assert_eq!(
        done.output.as_ref().map(ToString::to_string),
        Some(r#""parent:done:x""#.to_string()),
        "the parent decision read the child terminal"
    );
    assert!(
        b.metrics.woken("wake") > woken,
        "the child terminal woke the parent owner"
    );
    assert!(
        !b.worker.has_cached_execution(parent).await,
        "the parent entry is gone once the parent ends"
    );
    a.stop().await;
    b.stop().await;
}
