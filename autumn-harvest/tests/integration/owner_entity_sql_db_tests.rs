#![cfg(all(feature = "db", feature = "owner-disk-cache"))]
//! The owner-local entity projection in a real worker (issue #2260).
//!
//! Each test runs one owner with a disk cache and the entity projection on.
//! The owner holds the tenure of the entity's partition. The tests prove
//! these rules:
//!
//! - The owner writes the row after each commit. A query reads it.
//! - A deleted file rebuilds from history on the next query, with identical
//!   results. This is the red test of the issue.
//! - The erasure and key-rotation tests of the disk cache (issue #2258) pass
//!   for the projection rows. So do its purge, tenure-end and shutdown
//!   tests.
//! - A worker with no tenure, or with the switch off, reads history and
//!   writes no row.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::TypedSignalWithStartOptions;
use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, OwnedPartitions, OwnerHolder, OwnerKey, OwnerLeaseConfig,
};
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY, Entity, EntityMessage};
use autumn_harvest::entity_sql::{EntitySqlError, EntitySqlRows, EntitySqlSource};
use autumn_harvest::entity_state;
use autumn_harvest::owner_disk_cache;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use diesel::QueryableByName;
use diesel::sql_types::Integer;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

/// This suite owns partitions 486 to 495. Each test takes its own slot.
const BASE: u16 = 486;

const WAIT: Duration = Duration::from_secs(30);

/// A payload marker. A test searches the raw cache files for it.
const MARKER: &str = "entity-sql-pii-91c7e2";

/// The SQL of most tests: each item, in order, and the item count.
const ITEMS_SQL: &str = "SELECT item.key AS position, item.value AS item, \
                         json_array_length(entity.state) AS count, entity.event_id \
                         FROM entity, json_each(entity.state) AS item ORDER BY item.key";

/// A cart entity. Its state is the list of added items.
#[workflow]
async fn entity_sql_cart(ctx: &WorkflowContext, input: Value) -> Result<Vec<String>, String> {
    Entity::<Vec<String>>::from_input(ctx, input)
        .run(|mut items: Vec<String>, item: String| async move {
            items.push(item);
            Ok(items)
        })
        .await
        .map_err(|e| e.to_string())
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

/// An entity key in partition `BASE + slot`.
fn key_in(slot: u16) -> (String, DecisionPartition) {
    let target = DecisionPartition::new(BASE + slot).expect("in range");
    loop {
        let id = unique("entity-sql");
        if DecisionPartition::for_workflow_id(&id) == target {
            return (id, target);
        }
    }
}

fn shard() -> ShardId {
    OwnerKey::pool_shard(None)
}

fn owner_key(partition: DecisionPartition) -> OwnerKey {
    OwnerKey {
        shard: shard(),
        partition,
    }
}

const fn workflow_name() -> &'static str {
    "entity_sql_cart"
}

/// Counts the evict wakes of one worker.
#[derive(Default, Clone)]
struct EvictWakes(Arc<AtomicUsize>);

impl EvictWakes {
    fn count(&self) -> usize {
        AtomicUsize::load(&self.0, Ordering::SeqCst)
    }
}

impl MetricsRecorder for EvictWakes {
    fn record_decision_owner_woken(&self, _shard: &str, kind: &str) {
        if kind == "evict" {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct Node {
    id: String,
    worker: Arc<Worker>,
    owned: Arc<OwnedPartitions>,
    codecs: PayloadCodecs,
    evicts: EvictWakes,
    dir: tempfile::TempDir,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    fn file(&self, partition: DecisionPartition) -> PathBuf {
        self.dir
            .path()
            .join(owner_disk_cache::file_name(owner_key(partition)))
    }

    /// Run `sql` once on this node.
    async fn query(&self, pool: &DbPool, key: &str) -> Result<EntitySqlRows, EntitySqlError> {
        self.worker
            .query_entity_sql(pool, workflow_name(), key, ITEMS_SQL, &[])
            .await
    }

    /// Stop the worker. The cache directory outlives the stop.
    async fn stop(mut self) -> tempfile::TempDir {
        self.worker.shutdown();
        if let Some(task) = self.task.take() {
            tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .expect("the worker stops in time")
                .expect("the worker task ends cleanly");
        }
        self.dir
    }
}

/// How a test node is built.
#[derive(Default)]
struct NodeOptions {
    /// Hear owner wakes.
    listen: bool,
    /// The active payload codec key.
    codec: Option<AeadCodec>,
    /// Leave the entity projection off.
    switch_off: bool,
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
                queue: unique("entity-sql-q"),
            },
            container,
        )
    }

    /// A worker in owner mode with a disk cache in its own directory.
    fn node(&self, options: NodeOptions) -> Node {
        let dir = private_dir();
        let evicts = EvictWakes::default();
        let mut builder = HarvestBuilder::new();
        let key_id = options.codec.as_ref().map(|c| c.key_id().to_string());
        if let Some(codec) = options.codec {
            builder = builder.aead_payload_codec_key(codec);
        }
        let built = builder
            .workflows(workflows![entity_sql_cart])
            .worker(
                WorkerConfig::default()
                    .with_queues([self.queue.as_str()])
                    .with_owner_disk_cache(dir.path())
                    .with_owner_entity_sql(!options.switch_off),
            )
            .telemetry(
                TelemetryConfig::builder()
                    .metrics(Arc::new(evicts.clone()))
                    .build(),
            )
            .build();
        let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
        if let Some(key_id) = key_id {
            registry
                .payload_codecs()
                .set_active_key(&key_id)
                .expect("activate the codec key");
        }
        let codecs = registry.payload_codecs().clone();
        let owned = registry.decision_owner();
        owned.set_owner_mode(shard(), true);
        let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
        let id = unique("entity-sql-owner");
        runtime_config.worker_id.clone_from(&id);
        runtime_config.poll_interval = Duration::from_millis(100);
        runtime_config.notification_database_url = options.listen.then(|| self.url.clone());
        let worker = Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker"));
        Node {
            id,
            worker,
            owned,
            codecs,
            evicts,
            dir,
            task: None,
        }
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

    /// A running owner of a key in `slot`.
    async fn owner(
        &mut self,
        slot: u16,
        options: NodeOptions,
    ) -> (String, DecisionPartition, Node) {
        let (key, partition) = key_in(slot);
        let listen = options.listen;
        let mut node = self.node(options);
        self.take(&node, partition).await;
        node.task = Some(spawn_test_worker(
            Arc::clone(&node.worker),
            self.pool.clone(),
        ));
        if listen {
            eventually("the owner listens on its partition", async || {
                node.worker.owner_wake_listening(owner_key(partition))
            })
            .await;
        }
        (key, partition, node)
    }

    /// Send one op to the entity `key`.
    async fn send(&mut self, key: &str, message: EntityMessage<String>) -> ExecutionId {
        let client = WorkflowHandleClient::single(self.pool.clone(), self.url.clone());
        let opts = TypedSignalWithStartOptions {
            queue_name: Some(self.queue.clone()),
            idempotency_key: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        };
        EntitySqlCartStub::signal_with_start(
            &mut self.conn,
            &client,
            key,
            json!({}),
            ENTITY_OP_SIGNAL,
            message,
            opts,
        )
        .await
        .expect("signal with start")
        .exec_id()
    }

    async fn add(&mut self, key: &str, item: &str) -> ExecutionId {
        self.send(key, EntityMessage::op(item.to_string())).await
    }

    /// Wait until the run of `key` holds `count` items and is parked.
    ///
    /// Returns the event id of its last snapshot. The wait never queries
    /// the worker, so it writes no row.
    async fn settled(&mut self, node: &Node, key: &str, exec: ExecutionId, count: usize) -> i32 {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let row = entity_state::read(&mut self.conn, &node.codecs, workflow_name(), key)
                .await
                .expect("read the Postgres projection");
            let items = row
                .as_ref()
                .and_then(|row| row.snapshot.query_answer(ENTITY_STATE_QUERY))
                .and_then(|state| state.as_array().map(Vec::len));
            if let Some(row) = row
                && row.execution_id == exec
                && items == Some(count)
                && parked(&self.url, exec).await
            {
                return row.event_id;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out: {count} items for {key}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// [`Self::settled`], then wait until the owner applied that snapshot
    /// after its commit. The wait never queries the worker.
    async fn applied(&mut self, node: &Node, key: &str, exec: ExecutionId, count: usize) {
        let event_id = self.settled(node, key, exec, count).await;
        eventually(
            "the owner applies the snapshot after its commit",
            async || node.worker.entity_projection_event(exec).await == Some(event_id),
        )
        .await;
    }

    async fn completed(&self, exec: ExecutionId) {
        let execution =
            wait_for_execution_state_with_timeout(&self.url, exec, "COMPLETED", WAIT).await;
        assert_eq!(execution.state, "COMPLETED", "the run completes");
    }
}

/// A temporary directory with the mode `0700`, as the cache requires.
fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("cache dir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("chmod");
    }
    dir
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

/// Wait until `check` holds, up to `limit`.
async fn eventually_within(limit: Duration, what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    while tokio::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out: {what}");
}

/// Wait until `check` holds, up to [`WAIT`].
async fn eventually(what: &str, check: impl AsyncFnMut() -> bool) {
    eventually_within(WAIT, what, check).await;
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

/// Every sealed projection row of `exec` in `file`.
fn sealed_rows(file: &Path, exec: ExecutionId) -> Vec<Vec<u8>> {
    let conn = rusqlite::Connection::open(file).expect("open the cache file");
    let mut stmt = conn
        .prepare("SELECT sealed FROM harvest_entity_projection WHERE exec_id = ?1")
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

/// No sealed row of `rows`, no marker, no entity key and no run id remain.
fn assert_no_local_bytes(dir: &Path, exec: ExecutionId, key: &str, rows: &[Vec<u8>]) {
    let raw = dir_bytes(dir);
    assert!(!contains(&raw, MARKER.as_bytes()), "plaintext on disk");
    assert!(!contains(&raw, key.as_bytes()), "the entity key on disk");
    assert!(
        !contains(&raw, exec.as_uuid().to_string().as_bytes()),
        "the run id is still on disk"
    );
    for row in rows {
        assert!(!contains(&raw, row), "a sealed row is still on disk");
    }
}

/// The stored snapshot cell of the entity `key`, as Postgres holds it.
async fn stored_snapshot(conn: &mut AsyncPgConnection, key: &str) -> Value {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        snapshot: Value,
    }
    diesel::sql_query("SELECT snapshot FROM harvest_entity_state WHERE workflow_id = $1")
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<Row>(conn)
        .await
        .expect("read the stored snapshot")
        .snapshot
}

/// An owner with one entity run that holds the marker, applied to disk.
async fn marked_entity(
    fx: &mut Fixture,
    slot: u16,
    options: NodeOptions,
) -> (String, DecisionPartition, Node, ExecutionId, Vec<Vec<u8>>) {
    let (key, partition, node) = fx.owner(slot, options).await;
    let exec = fx.add(&key, &format!("secret:{MARKER}")).await;
    fx.applied(&node, &key, exec, 1).await;
    let rows = sealed_rows(&node.file(partition), exec);
    assert_eq!(rows.len(), 1, "the projection row is on disk");
    assert!(!contains(&dir_bytes(node.dir.path()), MARKER.as_bytes()));
    (key, partition, node, exec, rows)
}

/// The red test of the issue: delete the file mid-run, and the next query
/// rebuilds it from history with identical results.
#[tokio::test]
async fn deleting_the_file_mid_run_rebuilds_from_history() {
    let (mut fx, _container) = Fixture::new().await;
    let (key, partition, node) = fx.owner(0, NodeOptions::default()).await;
    let exec = fx.add(&key, "apple").await;
    fx.applied(&node, &key, exec, 1).await;
    let _ = fx.add(&key, "pear").await;
    fx.applied(&node, &key, exec, 2).await;

    // The owner wrote the row after its commit. No query wrote it.
    let file = node.file(partition);
    assert!(file.exists(), "the owner wrote the projection file");
    let before = node.query(&fx.pool, &key).await.expect("the query");
    assert_eq!(before.source, EntitySqlSource::File, "the applied row");
    assert_eq!(before.columns, ["position", "item", "count", "event_id"]);
    assert_eq!(before.rows.len(), 2);
    assert_eq!(before.rows[0][1], json!("apple"));
    assert_eq!(before.rows[1][1], json!("pear"));
    assert_eq!(before.rows[0][2], json!(2));

    // Delete the file while the run waits for its next op.
    std::fs::remove_file(&file).expect("delete the projection file");

    let rebuilt = node
        .query(&fx.pool, &key)
        .await
        .expect("the query rebuilds");
    assert_eq!(rebuilt.source, EntitySqlSource::History, "a rebuild");
    assert_eq!(rebuilt.columns, before.columns);
    assert_eq!(rebuilt.rows, before.rows, "identical results");
    assert!(file.exists(), "the rebuild writes a new file");
    assert!(node.worker.has_entity_projection(exec).await);

    let again = node.query(&fx.pool, &key).await.expect("the new file");
    assert_eq!(again.source, EntitySqlSource::File);
    assert_eq!(again.rows, before.rows);

    // The run goes on, and the next commit replaces the row.
    let _ = fx.add(&key, "plum").await;
    fx.applied(&node, &key, exec, 3).await;
    let after = node.query(&fx.pool, &key).await.expect("the query");
    assert_eq!(after.source, EntitySqlSource::File);
    assert_eq!(after.rows.len(), 3);
    assert_eq!(after.rows[2][1], json!("plum"));

    // A parameter binds to `?1`.
    let pear = node
        .worker
        .query_entity_sql(
            &fx.pool,
            workflow_name(),
            &key,
            "SELECT count(*) FROM entity, json_each(entity.state) WHERE value = ?1",
            &[json!("pear")],
        )
        .await
        .expect("a bound query");
    assert_eq!(pear.rows, vec![vec![json!(1)]]);

    // A write is refused, and an unknown key has no snapshot.
    assert!(matches!(
        node.worker
            .query_entity_sql(&fx.pool, workflow_name(), &key, "DELETE FROM entity", &[])
            .await,
        Err(EntitySqlError::Refused(_))
    ));
    assert!(matches!(
        node.query(&fx.pool, "no-such-key").await,
        Err(EntitySqlError::NotFound { .. })
    ));
    let _ = node.stop().await;
}

/// The erasure test of issue #2258, for the projection: after the entity
/// ends and its run is erased, no local bytes remain.
#[tokio::test]
async fn an_erased_entity_leaves_no_local_bytes() {
    let (mut fx, _container) = Fixture::new().await;
    let (key, _partition, node, exec, rows) =
        marked_entity(&mut fx, 1, NodeOptions::default()).await;

    let _ = fx.send(&key, EntityMessage::delete()).await;
    fx.completed(exec).await;
    eventually(
        "the terminal commit purges the projection row",
        async || !node.worker.has_entity_projection(exec).await,
    )
    .await;
    autumn_harvest::erase::erase_workflow_payloads(&mut fx.conn, exec, "gdpr")
        .await
        .expect("erase");

    assert_no_local_bytes(node.dir.path(), exec, &key, &rows);
    assert!(matches!(
        node.query(&fx.pool, &key).await,
        Err(EntitySqlError::Erased)
    ));
    assert_no_local_bytes(node.dir.path(), exec, &key, &rows);
    let _ = node.stop().await;
}

/// The missed-purge erasure test of issue #2258, for the projection. The
/// run ends with no wake, so its row stays. The erasure wakes the owner,
/// which purges it.
#[tokio::test]
async fn an_erasure_purges_an_entity_that_a_missed_purge_left() {
    let (mut fx, _container) = Fixture::new().await;
    let options = NodeOptions {
        listen: true,
        ..NodeOptions::default()
    };
    let (key, _partition, node, exec, rows) = marked_entity(&mut fx, 2, options).await;

    // A terminal write that wakes no one, as after a lost `NOTIFY`.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'TERMINATED', \
         completed_at = now() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .execute(&mut fx.conn)
    .await
    .expect("end the run");
    assert!(
        node.worker.has_entity_projection(exec).await,
        "no purge ran"
    );
    let wakes = node.evicts.count();

    autumn_harvest::erase::erase_workflow_payloads(&mut fx.conn, exec, "gdpr")
        .await
        .expect("erase");
    // The wake comes in well under the 30 s reconcile interval.
    eventually_within(
        Duration::from_secs(10),
        "the erasure wake purges the projection row",
        async || node.evicts.count() > wakes && !node.worker.has_entity_projection(exec).await,
    )
    .await;
    assert_no_local_bytes(node.dir.path(), exec, &key, &rows);
    let _ = node.stop().await;
}

/// The foreign-terminal test of issue #2258, for the projection. The evict
/// wake purges the row before the erasure.
#[tokio::test]
async fn a_foreign_terminal_purges_the_projection() {
    let (mut fx, _container) = Fixture::new().await;
    let options = NodeOptions {
        listen: true,
        ..NodeOptions::default()
    };
    let (key, _partition, node, exec, rows) = marked_entity(&mut fx, 7, options).await;

    autumn_harvest::execution::terminate_workflow_execution(
        &mut fx.conn,
        exec,
        "operator",
        &NoOpMetrics,
    )
    .await
    .expect("terminate");
    eventually("the evict wake purges the projection row", async || {
        !node.worker.has_entity_projection(exec).await
    })
    .await;
    autumn_harvest::erase::erase_workflow_payloads(&mut fx.conn, exec, "gdpr")
        .await
        .expect("erase");
    assert_no_local_bytes(node.dir.path(), exec, &key, &rows);
    let _ = node.stop().await;
}

/// The reconcile test of issue #2258, for the projection. With no wake,
/// the reconcile pass purges a row whose run is not running.
#[tokio::test]
async fn the_reconcile_pass_purges_an_entity_that_is_not_running() {
    let (mut fx, _container) = Fixture::new().await;
    let (key, _partition, node, exec, rows) =
        marked_entity(&mut fx, 6, NodeOptions::default()).await;

    autumn_harvest::execution::terminate_workflow_execution(
        &mut fx.conn,
        exec,
        "operator",
        &NoOpMetrics,
    )
    .await
    .expect("terminate");
    assert!(
        node.worker.has_entity_projection(exec).await,
        "no wake came"
    );

    // The owner scan can run a pass first, so the count is 0 or 1.
    let purged = node
        .worker
        .reconcile_owner_disk_cache(&fx.pool)
        .await
        .expect("reconcile");
    assert!(purged <= 1, "one run to purge, got {purged}");
    assert!(!node.worker.has_entity_projection(exec).await);
    assert_no_local_bytes(node.dir.path(), exec, &key, &rows);
    let _ = node.stop().await;
}

/// The key-rotation test of issue #2258, for the projection. The disk holds
/// no codec ciphertext. A key change and a reconcile pass delete the file.
#[tokio::test]
async fn a_rotated_codec_key_leaves_no_entity_ciphertext_on_disk() {
    let (mut fx, _container) = Fixture::new().await;
    let k1 = AeadCodec::new("entity-sql-k1", &DataKey::generate()).expect("codec");
    let options = NodeOptions {
        codec: Some(k1),
        ..NodeOptions::default()
    };
    let (key, partition, node, exec, _rows) = marked_entity(&mut fx, 3, options).await;

    let envelope = stored_snapshot(&mut fx.conn, &key).await;
    assert_eq!(
        autumn_harvest::payload_codec::codec_envelope_key_id(&envelope),
        Some("entity-sql-k1"),
        "Postgres holds k1 ciphertext"
    );
    let raw = dir_bytes(node.dir.path());
    let mut checked = 0;
    for part in envelope
        .to_string()
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '+' && c != '/')
    {
        if part.len() >= 16 {
            checked += 1;
            assert!(!contains(&raw, part.as_bytes()), "codec ciphertext on disk");
        }
    }
    assert!(checked > 0, "the envelope holds a ciphertext segment");

    // Rotate onto k2 and retire k1 in this process.
    AeadCodec::new("entity-sql-k2", &DataKey::generate())
        .expect("codec")
        .register_with(&node.codecs)
        .expect("register k2");
    node.codecs
        .set_active_key("entity-sql-k2")
        .expect("activate k2");
    eventually("k1 retires once no sweep batch pins it", async || {
        node.codecs.retire_key_local("entity-sql-k1").is_ok()
    })
    .await;
    node.worker
        .reconcile_owner_disk_cache(&fx.pool)
        .await
        .expect("reconcile");

    assert!(
        !node.file(partition).exists(),
        "the old generation is deleted"
    );
    assert!(!node.worker.has_entity_projection(exec).await);
    let _ = node.stop().await;
}

/// The lost-tenure test of issue #2258, for the projection. A fence deletes
/// the file. A later query on the old owner reads history and writes none.
#[tokio::test]
async fn a_lost_tenure_deletes_the_projection_file() {
    let (mut fx, _container) = Fixture::new().await;
    let (key, partition, node, exec, _rows) =
        marked_entity(&mut fx, 8, NodeOptions::default()).await;
    assert!(node.file(partition).exists());

    // A peer takes the row over. The next decision of the node is fenced.
    diesel::sql_query(
        "UPDATE harvest_decision_owner SET lease_until = clock_timestamp() \
         WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(partition.get()))
    .execute(&mut fx.conn)
    .await
    .expect("lapse the lease");
    let peer = OwnerHolder::for_worker("entity-sql-peer");
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
    let _ = fx.add(&key, "late").await;

    eventually("the fence deletes the projection file", async || {
        !node.file(partition).exists()
    })
    .await;
    assert!(!node.worker.has_entity_projection(exec).await);
    let rows = node.query(&fx.pool, &key).await.expect("history");
    assert_eq!(rows.source, EntitySqlSource::History);
    assert!(!node.file(partition).exists(), "no tenure, no file");
    let _ = node.stop().await;
    free_row(&mut fx.conn, partition).await;
}

/// The shutdown test of issue #2258, for the projection.
#[tokio::test]
async fn a_shutdown_wipes_the_projection() {
    let (mut fx, _container) = Fixture::new().await;
    let (_key, partition, node, _exec, _rows) =
        marked_entity(&mut fx, 9, NodeOptions::default()).await;
    assert!(node.file(partition).exists());
    let dir = node.stop().await;
    assert_eq!(
        dir_bytes(dir.path()).len(),
        0,
        "the shutdown wipes the files"
    );
}

/// A worker with no tenure answers from history and writes no file.
#[tokio::test]
async fn a_query_off_the_owner_reads_history() {
    let (mut fx, _container) = Fixture::new().await;
    let (key, partition, owner) = fx.owner(4, NodeOptions::default()).await;
    let exec = fx.add(&key, "fig").await;
    fx.applied(&owner, &key, exec, 1).await;
    let on_owner = owner.query(&fx.pool, &key).await.expect("the owner");
    assert_eq!(on_owner.source, EntitySqlSource::File);

    // Another worker with its own directory, no tenure and no loop.
    let peer = fx.node(NodeOptions::default());
    let on_peer = peer.query(&fx.pool, &key).await.expect("the peer");
    assert_eq!(on_peer.source, EntitySqlSource::History);
    assert_eq!(on_peer.rows, on_owner.rows);
    assert!(!peer.worker.has_entity_projection(exec).await);
    assert!(!peer.file(partition).exists(), "the peer writes no file");
    assert_eq!(dir_bytes(peer.dir.path()).len(), 0);
    let _ = owner.stop().await;
}

/// With the switch off, the owner keeps no projection and reads history.
#[tokio::test]
async fn the_switch_off_keeps_no_projection() {
    let (mut fx, _container) = Fixture::new().await;
    let options = NodeOptions {
        switch_off: true,
        ..NodeOptions::default()
    };
    let (key, _partition, node) = fx.owner(5, options).await;
    let exec = fx.add(&key, "kiwi").await;
    let _ = fx.settled(&node, &key, exec, 1).await;
    let rows = node.query(&fx.pool, &key).await.expect("history");
    assert_eq!(rows.source, EntitySqlSource::History);
    assert_eq!(rows.rows[0][1], json!("kiwi"));
    assert!(!node.worker.has_entity_projection(exec).await);
    // The run is under the stage 2 event bound, so no file holds a byte.
    assert_eq!(dir_bytes(node.dir.path()).len(), 0);
    let _ = node.stop().await;
}
