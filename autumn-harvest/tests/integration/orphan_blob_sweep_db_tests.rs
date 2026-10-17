#![cfg(feature = "db")]
//! Orphan-blob sweep (issue #2123).
//!
//! A write uploads its blobs before it opens its transaction. A rollback leaves
//! blobs that no `harvest_payload_refs` row names. These tests drive the real
//! retention runtime against Postgres and an in-memory store. They pin this
//! contract:
//!
//! - A blob that a rolled-back append uploaded is deleted after the grace period.
//! - A blob that any shard references is never deleted.
//! - A blob younger than the grace period is never deleted.
//! - A failed reference check on any shard deletes nothing.
//! - A store without listing disables the sweep. The runtime asks once.
//! - `dry_run` counts orphans and deletes nothing.
//! - A failed delete is counted and retried on the next pass.
//! - One tick pages through a listing larger than `batch_size`.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise the suite starts a testcontainers Postgres.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_store::{
    BlobPage, PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture, StoredBlob,
};
use autumn_harvest::retention::{RetentionConfig, RetentionRuntime};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A store with a creation time on each blob and switches for each failure.
struct MemStore {
    blobs: Mutex<BTreeMap<String, (Vec<u8>, DateTime<Utc>)>>,
    can_list: bool,
    fail_delete: AtomicBool,
    list_calls: AtomicUsize,
}

impl MemStore {
    fn new(can_list: bool) -> Arc<Self> {
        Arc::new(Self {
            blobs: Mutex::new(BTreeMap::new()),
            can_list,
            fail_delete: AtomicBool::new(false),
            list_calls: AtomicUsize::new(0),
        })
    }

    fn keys(&self) -> BTreeSet<String> {
        self.blobs.lock().unwrap().keys().cloned().collect()
    }

    /// Move every blob's creation time `hours` into the past.
    fn age_all(&self, hours: i64) {
        for (_, created) in self.blobs.lock().unwrap().values_mut() {
            *created = Utc::now() - chrono::Duration::hours(hours);
        }
    }

    /// Add a blob that no run references, `hours` old.
    fn plant_orphan(&self, key: &str, hours: i64) {
        self.blobs.lock().unwrap().insert(
            key.to_string(),
            (
                b"orphan".to_vec(),
                Utc::now() - chrono::Duration::hours(hours),
            ),
        );
    }
}

impl PayloadStore for MemStore {
    fn store_id(&self) -> &'static str {
        "memdb"
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        // One key per upload, so a re-upload never shares a key.
        let key = format!("blob-{}", Uuid::new_v4().simple());
        self.blobs
            .lock()
            .unwrap()
            .insert(key.clone(), (bytes.to_vec(), Utc::now()));
        Box::pin(async move { Ok(key) })
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let found = self.blobs.lock().unwrap().get(key).map(|b| b.0.clone());
        let key = key.to_string();
        Box::pin(async move { found.ok_or_else(|| PayloadStoreError(format!("missing {key}"))) })
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        let fail = self.fail_delete.load(Ordering::SeqCst);
        if !fail {
            self.blobs.lock().unwrap().remove(key);
        }
        Box::pin(async move {
            if fail {
                Err(PayloadStoreError("delete refused".to_string()))
            } else {
                Ok(())
            }
        })
    }

    fn list(
        &self,
        after: Option<&str>,
        created_before: DateTime<Utc>,
        limit: usize,
    ) -> PayloadStoreFuture<'_, Option<BlobPage>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        if !self.can_list {
            return Box::pin(async { Ok(None) });
        }
        let mut matching: Vec<StoredBlob> = self
            .blobs
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, (_, created))| {
                *created < created_before && after.is_none_or(|a| key.as_str() > a)
            })
            .map(|(key, (_, created))| StoredBlob {
                key: key.clone(),
                created_at: *created,
            })
            .collect();
        let more = matching.len() > limit;
        matching.truncate(limit);
        let next = more
            .then(|| matching.last().map(|b| b.key.clone()))
            .flatten();
        Box::pin(async move {
            Ok(Some(BlobPage {
                blobs: matching,
                next,
            }))
        })
    }
}

#[derive(Default)]
struct CapturingMetrics {
    found: AtomicU64,
    deleted: AtomicU64,
    failures: AtomicU64,
}

impl MetricsRecorder for CapturingMetrics {
    fn record_orphan_blob_sweep(&self, found: u64, deleted: u64, delete_failures: u64) {
        self.found.fetch_add(found, Ordering::SeqCst);
        self.deleted.fetch_add(deleted, Ordering::SeqCst);
        self.failures.fetch_add(delete_failures, Ordering::SeqCst);
    }

    fn record_scanner_tick(&self, _scanner: &str, _shard: &str) {}
}

impl CapturingMetrics {
    fn counts(&self) -> (u64, u64, u64) {
        (
            self.found.load(Ordering::SeqCst),
            self.deleted.load(Ordering::SeqCst),
            self.failures.load(Ordering::SeqCst),
        )
    }
}

struct Dbs {
    url_a: String,
    url_b: String,
    _container: Option<ContainerAsync<Postgres>>,
}

fn with_db_name(url: &str, name: &str) -> String {
    let (base, _) = url.rsplit_once('/').expect("a database URL has a path");
    format!("{base}/{name}")
}

/// Two migrated databases, one for each shard.
async fn setup_dbs() -> Dbs {
    let (url_a, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let name_b = format!("orphan_sweep_b_{}", Uuid::new_v4().simple());
    let mut admin = connect(&url_a).await;
    admin
        .batch_execute(&format!("CREATE DATABASE {name_b}"))
        .await
        .expect("create the second database");
    let url_b = with_db_name(&url_a, &name_b);
    let mut conn_b = connect(&url_b).await;
    conn_b
        .batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrate the second database");
    let mut conn_a = connect(&url_a).await;
    conn_a
        .batch_execute("DELETE FROM harvest_workflow_executions;")
        .await
        .expect("scrub");
    Dbs {
        url_a,
        url_b,
        _container: container,
    }
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    // Scoped: the trait's `load` would shadow the atomics' `load` elsewhere.
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "orphan_sweep_wf",
        workflow_id: &Uuid::new_v4().to_string(),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({}).into(),
        parent_id: None,
        queue_name: "default",
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
        start_source: None,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");
    exec_id
}

fn big_events() -> Vec<WorkflowEvent> {
    vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({ "blob": "Q".repeat(4096) }),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }]
}

fn offloader(store: &Arc<MemStore>) -> Arc<PayloadOffloader> {
    Arc::new(PayloadOffloader::new(
        Arc::clone(store) as Arc<dyn PayloadStore>,
        256,
        Arc::new(NoOpMetrics),
    ))
}

fn sweep_config() -> RetentionConfig {
    RetentionConfig::default().with_orphan_blob_grace(Duration::from_secs(60 * 60))
}

/// Run `ticks` janitor iterations and return when the last one is done.
async fn run_ticks(
    pools: ShardedDbPool,
    config: RetentionConfig,
    metrics: &Arc<CapturingMetrics>,
    offloader: Arc<PayloadOffloader>,
    ticks: u64,
) {
    let runtime = RetentionRuntime::spawn(
        pools,
        config,
        Arc::clone(metrics) as Arc<dyn MetricsRecorder>,
        None,
        Some(offloader),
    )
    .expect("the runtime spawns");
    for done in 1..=ticks {
        runtime.run_now();
        let mut finished = false;
        for _ in 0..400 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if runtime.monitor().iterations_completed() >= done {
                finished = true;
                break;
            }
        }
        assert!(finished, "retention tick {done} did not complete in time");
    }
    runtime.shutdown();
}

fn one_shard(dbs: &Dbs) -> ShardedDbPool {
    ShardedDbPool::single(build_pool(&dbs.url_a))
}

fn two_shards(dbs: &Dbs) -> ShardedDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&dbs.url_a));
    pools.insert(ShardId::new(1), build_pool(&dbs.url_b));
    ShardedDbPool::from_map(pools, ShardId::new(0))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_rolled_back_append_blob_is_swept_after_the_grace_period() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    let off = offloader(&store);
    let mut conn = connect(&dbs.url_a).await;

    // No execution row exists, so the events INSERT fails and the append
    // rolls back after its upload.
    let result =
        store::append_events_offloaded(&mut conn, ExecutionId::new(), &big_events(), 0, Some(&off))
            .await;
    assert!(result.is_err(), "the append must roll back");
    assert_eq!(store.keys().len(), 1, "the rolled-back write left a blob");

    store.age_all(2);
    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(one_shard(&dbs), sweep_config(), &metrics, off, 1).await;

    assert!(store.keys().is_empty(), "the sweep deleted the orphan");
    assert_eq!(metrics.counts(), (1, 1, 0));
}

#[tokio::test]
async fn a_blob_that_any_shard_references_is_never_deleted() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    let off = offloader(&store);

    // The reference lives on shard 1 only. The sweep must still keep the blob.
    let mut conn_b = connect(&dbs.url_b).await;
    let exec = insert_execution(&mut conn_b).await;
    store::append_events_offloaded(&mut conn_b, exec, &big_events(), 0, Some(&off))
        .await
        .expect("append");
    let referenced = store.keys();
    assert_eq!(referenced.len(), 1);
    store.age_all(2);
    store.plant_orphan("orphan-key", 48);

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(two_shards(&dbs), sweep_config(), &metrics, off, 1).await;

    let remaining = store.keys();
    assert!(
        referenced.is_subset(&remaining),
        "a blob that shard 1 references must stay"
    );
    assert!(!remaining.contains("orphan-key"), "the orphan goes");
    assert_eq!(metrics.counts(), (1, 1, 0));
}

#[tokio::test]
async fn a_blob_younger_than_the_grace_period_is_never_deleted() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    store.plant_orphan("young", 0);
    store.plant_orphan("old", 48);
    let off = offloader(&store);

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(one_shard(&dbs), sweep_config(), &metrics, off, 1).await;

    assert_eq!(store.keys(), BTreeSet::from(["young".to_string()]));
    assert_eq!(metrics.counts(), (1, 1, 0));
}

#[tokio::test]
async fn a_failed_reference_check_deletes_nothing() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    store.plant_orphan("old", 48);
    let off = offloader(&store);

    // Shard 1 cannot be reached, so its references are unknown.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&dbs.url_a));
    pools.insert(
        ShardId::new(1),
        build_pool("postgres://postgres:postgres@127.0.0.1:1/none"),
    );
    let sharded = ShardedDbPool::from_map(pools, ShardId::new(0));

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(sharded, sweep_config(), &metrics, off, 1).await;

    assert_eq!(store.keys(), BTreeSet::from(["old".to_string()]));
    assert_eq!(metrics.counts(), (0, 0, 0));
}

#[tokio::test]
async fn a_store_without_listing_disables_the_sweep() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(false);
    store.plant_orphan("old", 48);
    let off = offloader(&store);

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(one_shard(&dbs), sweep_config(), &metrics, off, 2).await;

    assert_eq!(store.keys(), BTreeSet::from(["old".to_string()]));
    assert_eq!(
        store.list_calls.load(Ordering::SeqCst),
        1,
        "the runtime asks once, then stops"
    );
    assert_eq!(metrics.counts(), (0, 0, 0));
}

#[tokio::test]
async fn a_dry_run_counts_orphans_and_deletes_nothing() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    store.plant_orphan("old", 48);
    let off = offloader(&store);
    let mut config = sweep_config();
    config.dry_run = true;

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(one_shard(&dbs), config, &metrics, off, 1).await;

    assert_eq!(store.keys(), BTreeSet::from(["old".to_string()]));
    assert_eq!(metrics.counts(), (1, 0, 0));
}

#[tokio::test]
async fn a_disabled_sweep_does_not_list() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    store.plant_orphan("old", 48);
    let off = offloader(&store);
    let config = sweep_config().without_orphan_blob_sweep();

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(one_shard(&dbs), config, &metrics, off, 1).await;

    assert_eq!(store.list_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.keys().len(), 1);
}

#[tokio::test]
async fn a_failed_delete_is_counted_and_retried() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    store.plant_orphan("old", 48);
    store.fail_delete.store(true, Ordering::SeqCst);
    let off = offloader(&store);

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(
        one_shard(&dbs),
        sweep_config(),
        &metrics,
        Arc::clone(&off),
        1,
    )
    .await;
    assert_eq!(store.keys().len(), 1, "the failed delete leaves the blob");
    assert_eq!(metrics.counts(), (1, 0, 1));

    store.fail_delete.store(false, Ordering::SeqCst);
    run_ticks(one_shard(&dbs), sweep_config(), &metrics, off, 1).await;
    assert!(store.keys().is_empty(), "the next pass retries the delete");
    assert_eq!(metrics.counts(), (2, 1, 1));
}

#[tokio::test]
async fn one_tick_pages_through_a_listing_larger_than_the_batch() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    for n in 0..5 {
        store.plant_orphan(&format!("old-{n}"), 48);
    }
    let off = offloader(&store);
    let mut config = sweep_config();
    config.batch_size = 2;

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(one_shard(&dbs), config, &metrics, off, 1).await;

    assert!(store.keys().is_empty(), "three pages cover five blobs");
    assert_eq!(metrics.counts(), (5, 5, 0));
}

#[tokio::test]
async fn a_grace_below_the_floor_skips_the_pass() {
    let dbs = setup_dbs().await;
    let store = MemStore::new(true);
    store.plant_orphan("young", 0);
    let off = offloader(&store);
    // `spawn` does not call `validate`. The pass must refuse a one-minute grace.
    let mut config = sweep_config();
    config.orphan_blob_grace_secs = Some(60);

    let metrics = Arc::new(CapturingMetrics::default());
    run_ticks(one_shard(&dbs), config, &metrics, off, 1).await;

    assert_eq!(store.list_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.keys().len(), 1);
}
