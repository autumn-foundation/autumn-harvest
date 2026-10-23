#![cfg(all(feature = "db", feature = "owner-disk-cache"))]
#![allow(clippy::unused_async)]
//! The owner-local disk cache in a real worker (issue #2258).
//!
//! Each test runs one worker in owner mode with a disk cache. The worker
//! holds the tenure of the run's partition. The tests prove these rules:
//!
//! - A memory miss reads the disk and the Postgres delta.
//! - A terminal run leaves no local bytes, also after an erasure.
//! - A foreign terminal write purges the entry, and so does the reconcile
//!   pass when the wake is lost.
//! - A damaged file replays cold, with no non-determinism block.
//! - A lost tenure and a shutdown delete the files.
//!
//! The module tests of `owner_disk_cache.rs` cover the kill test and the
//! codec rotation test.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, OwnedPartitions, OwnerHolder, OwnerKey, OwnerLeaseConfig,
};
use autumn_harvest::owner_disk_cache;
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

/// This suite owns partitions 500 to 511. Each test takes its own slot.
const BASE: u16 = 500;

const WAIT: Duration = Duration::from_secs(30);

/// A payload marker. A test searches the raw cache files for it.
const MARKER: &str = "disk-cache-pii-4be1d0";

#[workflow]
async fn disk_cache_flow(ctx: &WorkflowContext, input: String) -> Result<String, String> {
    let _ = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(format!("done:{input}"))
}

#[workflow]
async fn disk_cache_activity_flow(ctx: &WorkflowContext, queue: String) -> Result<String, String> {
    let secret = Value::String(format!("secret:{MARKER}"));
    let echoed = ctx
        .execute_activity_raw("disk_cache_echo", secret, &queue)
        .await
        .map_err(|e| e.to_string())?;
    let _ = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(format!("done:{}", echoed.as_str().unwrap_or_default()))
}

#[activity]
async fn disk_cache_echo(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    Ok(input)
}

/// Records each disk cache outcome of one worker.
#[derive(Default, Clone)]
struct Outcomes(Arc<Mutex<Vec<String>>>);

impl Outcomes {
    fn count(&self, outcome: &str) -> usize {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|o| o.as_str() == outcome)
            .count()
    }
}

impl MetricsRecorder for Outcomes {
    fn record_workflow_disk_cache(&self, _workflow_name: &str, _queue: &str, outcome: &str) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(outcome.to_string());
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
        let id = unique("disk-cache");
        if DecisionPartition::for_workflow_id(&id) == target {
            return (id, target);
        }
    }
}

fn shard() -> ShardId {
    OwnerKey::pool_shard(None)
}

fn key(partition: DecisionPartition) -> OwnerKey {
    OwnerKey {
        shard: shard(),
        partition,
    }
}

struct Node {
    id: String,
    worker: Arc<Worker>,
    owned: Arc<OwnedPartitions>,
    outcomes: Outcomes,
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
    dir: tempfile::TempDir,
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
                queue: unique("disk-cache-q"),
                dir: tempfile::tempdir().expect("cache dir"),
            },
            container,
        )
    }

    fn dir(&self) -> &Path {
        self.dir.path()
    }

    fn file(&self, partition: DecisionPartition) -> PathBuf {
        self.dir().join(owner_disk_cache::file_name(key(partition)))
    }

    /// A worker in owner mode with a disk cache. With `listen`, it gets the
    /// notify URL, so it hears owner wakes.
    fn node(&self, listen: bool) -> Node {
        let outcomes = Outcomes::default();
        let built = HarvestBuilder::new()
            .workflows(workflows![disk_cache_flow, disk_cache_activity_flow])
            .activities(activities![disk_cache_echo])
            .worker(
                WorkerConfig::default()
                    .with_queues([self.queue.as_str()])
                    .with_owner_disk_cache(self.dir()),
            )
            .telemetry(
                TelemetryConfig::builder()
                    .metrics(Arc::new(outcomes.clone()))
                    .build(),
            )
            .build();
        let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
        let owned = registry.decision_owner();
        owned.set_owner_mode(shard(), true);
        let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
        let id = unique("disk-owner");
        runtime_config.worker_id.clone_from(&id);
        runtime_config.poll_interval = Duration::from_millis(100);
        runtime_config.notification_database_url = listen.then(|| self.url.clone());
        let worker = Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker"));
        Node {
            id,
            worker,
            owned,
            outcomes,
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
    async fn take(&mut self, node: &Node, partition: DecisionPartition) {
        free_row(&mut self.conn, partition).await;
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
        node.owned.adopt(tenure);
    }

    /// An owner of `slot`, running.
    async fn owner(&mut self, slot: u16, listen: bool) -> (String, DecisionPartition, Node) {
        let (workflow_id, partition) = workflow_id_in(slot);
        let mut node = self.node(listen);
        self.take(&node, partition).await;
        self.run(&mut node);
        if listen {
            eventually("the owner listens on its partition", async || {
                node.worker.owner_wake_listening(key(partition))
            })
            .await;
        }
        (workflow_id, partition, node)
    }

    async fn start(&mut self, workflow_id: &str) -> ExecutionId {
        let client = WorkflowHandleClient::single(self.pool.clone(), self.url.clone());
        let opts = TypedStartOptions {
            queue_name: Some(self.queue.clone()),
            ..Default::default()
        };
        DiskCacheFlowStub::start_with_options(
            &mut self.conn,
            &client,
            workflow_id,
            format!("secret:{MARKER}"),
            opts,
        )
        .await
        .expect("start")
        .exec_id()
    }

    /// Start a run and wait until the owner has it on disk and parks it.
    async fn start_on_disk(&mut self, workflow_id: &str, node: &Node) -> ExecutionId {
        let exec = self.start(workflow_id).await;
        // The worker writes the disk first, then the memory cache. Wait for
        // both, so a later forget is not undone.
        eventually(
            "the owner caches the run on disk and in memory",
            async || {
                node.worker.has_disk_cached_execution(exec).await
                    && node.worker.has_cached_execution(exec).await
            },
        )
        .await;
        eventually("the owner parks the task", async || {
            parked(&self.url, exec).await
        })
        .await;
        exec
    }

    async fn signal(&self, exec: ExecutionId) {
        let mut conn = AsyncPgConnection::establish(&self.url)
            .await
            .expect("connect");
        autumn_harvest::signal::send_signal(&mut conn, exec, "go", Value::Null)
            .await
            .expect("signal");
    }

    async fn completed(&self, exec: ExecutionId) {
        let execution =
            wait_for_execution_state_with_timeout(&self.url, exec, "COMPLETED", WAIT).await;
        assert_eq!(execution.state, "COMPLETED", "the run completes");
    }
}

async fn free_row(conn: &mut AsyncPgConnection, partition: DecisionPartition) {
    diesel::sql_query(
        "UPDATE harvest_decision_owner SET holder = NULL, lease_until = '-infinity' \
         WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(partition.get()))
    .execute(conn)
    .await
    .expect("free the row");
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

/// Whether the workflow task of `exec` is parked: `RUNNING` with no worker.
async fn parked(url: &str, exec: ExecutionId) -> bool {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        parked: bool,
    }
    let Ok(mut conn) = AsyncPgConnection::establish(url).await else {
        return false;
    };
    diesel::sql_query(
        "SELECT bool_or(state = 'RUNNING' AND worker_id IS NULL) AS parked \
         FROM harvest_task_queue WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .get_result::<Row>(&mut conn)
    .await
    .is_ok_and(|row| row.parked)
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

/// Every sealed row of `exec` in `file`, read through a second connection.
fn sealed_rows(file: &Path, exec: ExecutionId) -> Vec<Vec<u8>> {
    let conn = rusqlite::Connection::open(file).expect("open the cache file");
    let mut stmt = conn
        .prepare(
            "SELECT event_json FROM harvest_events WHERE exec_id = ?1 \
             UNION ALL SELECT sealed FROM harvest_cache_runs WHERE exec_id = ?1",
        )
        .expect("prepare");
    stmt.query_map([exec.as_uuid().to_string()], |row| row.get::<_, Vec<u8>>(0))
        .expect("query")
        .map(|row| row.expect("row"))
        .collect()
}

/// The bytes of every file in `dir`, the lock file aside.
fn dir_bytes(dir: &Path) -> Vec<u8> {
    let mut all = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.file_name().and_then(|n| n.to_str()) != Some(owner_disk_cache::LOCK_FILE_NAME) {
            all.extend(std::fs::read(&path).unwrap_or_default());
        }
    }
    all
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

/// No sealed row of `rows`, no marker and no run id remain in `dir`.
fn assert_no_local_bytes(dir: &Path, exec: ExecutionId, rows: &[Vec<u8>]) {
    let raw = dir_bytes(dir);
    assert!(!contains(&raw, MARKER.as_bytes()), "plaintext on disk");
    assert!(
        !contains(&raw, exec.as_uuid().to_string().as_bytes()),
        "the run id is still on disk"
    );
    for row in rows {
        assert!(
            !contains(&raw, row),
            "a sealed row of the run is still on disk"
        );
    }
}

/// Flip one byte inside the first sealed event row of `exec`.
fn corrupt(file: &Path, exec: ExecutionId) {
    let row = sealed_rows(file, exec).remove(0);
    let mut raw = std::fs::read(file).expect("read");
    let at = raw
        .windows(row.len())
        .position(|w| w == row.as_slice())
        .expect("the sealed row is in the file");
    raw[at + row.len() / 2] ^= 0x5a;
    // Bump the file change counter of the SQLite header, bytes 24 to 27.
    // The open connection of the worker then drops its page cache.
    let mut counter = [0_u8; 4];
    counter.copy_from_slice(&raw[24..28]);
    let next = u32::from_be_bytes(counter).wrapping_add(1);
    raw[24..28].copy_from_slice(&next.to_be_bytes());
    std::fs::write(file, raw).expect("write");
}

#[tokio::test]
async fn a_memory_miss_reads_the_disk() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, _partition, node) = fx.owner(0, false).await;
    let exec = fx.start_on_disk(&workflow_id, &node).await;
    assert_ne!(
        dir_bytes(fx.dir()).len(),
        0,
        "the owner writes a cache file"
    );

    node.worker.forget_cached_execution(exec).await;
    fx.signal(exec).await;
    fx.completed(exec).await;

    assert_eq!(node.outcomes.count("hit"), 1, "the decision reads the disk");
    assert_eq!(node.outcomes.count("corrupt"), 0);
    assert!(!node.worker.has_disk_cached_execution(exec).await);
    node.stop().await;
}

/// A disk entry with events that the decision built in memory passes the
/// anchor check, and a later suspension appends to it.
#[tokio::test]
async fn a_decided_history_reads_back_from_the_disk() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, partition, node) = fx.owner(8, false).await;
    let client = WorkflowHandleClient::single(fx.pool.clone(), fx.url.clone());
    let opts = TypedStartOptions {
        queue_name: Some(fx.queue.clone()),
        ..Default::default()
    };
    let exec = DiskCacheActivityFlowStub::start_with_options(
        &mut fx.conn,
        &client,
        &workflow_id,
        fx.queue.clone(),
        opts,
    )
    .await
    .expect("start")
    .exec_id();
    // The activity result needs a second decision, which appends to disk.
    eventually("the run waits for its signal on disk", async || {
        node.worker.has_disk_cached_execution(exec).await
            && node.worker.has_cached_execution(exec).await
            && parked(&fx.url, exec).await
            && sealed_rows(&fx.file(partition), exec).len() >= 4
    })
    .await;
    assert!(!contains(&dir_bytes(fx.dir()), MARKER.as_bytes()));

    node.worker.forget_cached_execution(exec).await;
    fx.signal(exec).await;
    fx.completed(exec).await;

    assert_eq!(node.outcomes.count("hit"), 1, "the anchor check passes");
    assert_eq!(
        node.outcomes.count("stale") + node.outcomes.count("corrupt"),
        0
    );
    assert!(!nd_blocked(&mut fx.conn, exec).await, "no ND block");
    node.stop().await;
}

/// AC 1: after a run is erased, no local bytes remain.
#[tokio::test]
async fn an_erased_run_leaves_no_local_bytes() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, partition, node) = fx.owner(1, false).await;
    let exec = fx.start_on_disk(&workflow_id, &node).await;
    let rows = sealed_rows(&fx.file(partition), exec);
    // `WorkflowStarted` and the run stamp, at least.
    assert!(rows.len() >= 2, "the run has sealed rows on disk");
    assert!(!contains(&dir_bytes(fx.dir()), MARKER.as_bytes()));

    fx.signal(exec).await;
    fx.completed(exec).await;
    autumn_harvest::erase::erase_workflow_payloads(&mut fx.conn, exec, "gdpr")
        .await
        .expect("erase");

    assert!(!node.worker.has_disk_cached_execution(exec).await);
    assert_no_local_bytes(fx.dir(), exec, &rows);
    node.stop().await;
}

/// AC 1 for a foreign terminal write. The evict wake purges the entry.
#[tokio::test]
async fn a_foreign_terminal_purges_the_disk_entry() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, partition, node) = fx.owner(2, true).await;
    let exec = fx.start_on_disk(&workflow_id, &node).await;
    let rows = sealed_rows(&fx.file(partition), exec);

    autumn_harvest::execution::terminate_workflow_execution(
        &mut fx.conn,
        exec,
        "operator",
        &NoOpMetrics,
    )
    .await
    .expect("terminate");
    eventually("the evict wake purges the disk entry", async || {
        !node.worker.has_disk_cached_execution(exec).await
    })
    .await;
    autumn_harvest::erase::erase_workflow_payloads(&mut fx.conn, exec, "gdpr")
        .await
        .expect("erase");

    assert_no_local_bytes(fx.dir(), exec, &rows);
    node.stop().await;
}

/// A lost evict wake: the reconcile pass purges the entry.
#[tokio::test]
async fn the_reconcile_pass_purges_a_run_that_is_not_running() {
    let (mut fx, _container) = Fixture::new().await;
    // No notify URL, so no owner wake reaches the node.
    let (workflow_id, partition, node) = fx.owner(3, false).await;
    let exec = fx.start_on_disk(&workflow_id, &node).await;
    let rows = sealed_rows(&fx.file(partition), exec);

    autumn_harvest::execution::terminate_workflow_execution(
        &mut fx.conn,
        exec,
        "operator",
        &NoOpMetrics,
    )
    .await
    .expect("terminate");
    assert!(
        node.worker.has_disk_cached_execution(exec).await,
        "no wake came"
    );

    let purged = node
        .worker
        .reconcile_owner_disk_cache(&fx.pool)
        .await
        .expect("reconcile");
    assert_eq!(purged, 1);
    assert!(!node.worker.has_disk_cached_execution(exec).await);
    assert_no_local_bytes(fx.dir(), exec, &rows);
    node.stop().await;
}

/// AC 4: a corrupted cache file triggers a cold replay, not a
/// non-determinism error.
#[tokio::test]
async fn a_corrupt_file_replays_cold() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, partition, node) = fx.owner(4, false).await;
    let exec = fx.start_on_disk(&workflow_id, &node).await;

    node.worker.forget_cached_execution(exec).await;
    corrupt(&fx.file(partition), exec);
    fx.signal(exec).await;
    fx.completed(exec).await;

    assert_eq!(node.outcomes.count("corrupt"), 1, "the seal check fails");
    assert_eq!(node.outcomes.count("hit"), 0);
    assert!(!nd_blocked(&mut fx.conn, exec).await, "no ND block");
    node.stop().await;
}

/// A changed last event fails the anchor check. The decision replays cold.
#[tokio::test]
async fn a_changed_history_tail_replays_cold() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, _partition, node) = fx.owner(5, false).await;
    let exec = fx.start_on_disk(&workflow_id, &node).await;

    node.worker.forget_cached_execution(exec).await;
    // A restore from another backup gives another tail. The parked run has
    // one event, `WorkflowStarted`, so its input is the tail.
    autumn_harvest::append_only::with_guard_off(&mut fx.conn, async |conn| {
        diesel::sql_query(
            "UPDATE harvest_events SET event_data = jsonb_set(event_data, \
             '{data,input}', '\"rewritten\"', false) \
             WHERE workflow_exec_id = $1 AND event_id = \
             (SELECT max(event_id) FROM harvest_events WHERE workflow_exec_id = $1)",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
        .execute(conn)
        .await
        .map(|_| ())
    })
    .await
    .expect("rewrite the tail");
    fx.signal(exec).await;

    eventually("the decision reads the stale entry", async || {
        node.outcomes.count("stale") == 1
    })
    .await;
    assert_eq!(node.outcomes.count("hit"), 0);
    // The cold replay reads the new tail from Postgres.
    fx.completed(exec).await;
    assert!(!nd_blocked(&mut fx.conn, exec).await, "no ND block");
    node.stop().await;
}

/// A fenced tenure deletes the partition file before any later decision.
#[tokio::test]
async fn a_lost_tenure_deletes_the_partition_file() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, partition, node) = fx.owner(6, false).await;
    let exec = fx.start_on_disk(&workflow_id, &node).await;
    assert!(fx.file(partition).exists());

    // A peer takes the row over. The next decision of the node is fenced.
    diesel::sql_query(
        "UPDATE harvest_decision_owner SET lease_until = clock_timestamp() \
         WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(partition.get()))
    .execute(&mut fx.conn)
    .await
    .expect("lapse the lease");
    let peer = OwnerHolder::for_worker("disk-cache-peer");
    let Acquisition::Acquired(_peer) = decision_owner::acquire(
        &mut fx.conn,
        shard(),
        &peer,
        partition,
        &OwnerLeaseConfig::default(),
    )
    .await
    .expect("take over") else {
        panic!("the lapsed row is taken over");
    };
    fx.signal(exec).await;

    eventually("the fence deletes the partition file", async || {
        !fx.file(partition).exists()
    })
    .await;
    assert!(!node.worker.has_disk_cached_execution(exec).await);
    node.stop().await;
    free_row(&mut fx.conn, partition).await;
}

/// A shutdown deletes every cache file.
#[tokio::test]
async fn a_shutdown_wipes_the_cache_directory() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, partition, node) = fx.owner(7, false).await;
    let _exec = fx.start_on_disk(&workflow_id, &node).await;
    assert!(fx.file(partition).exists());
    node.stop().await;
    assert_eq!(dir_bytes(fx.dir()).len(), 0, "the shutdown wipes the files");
}
