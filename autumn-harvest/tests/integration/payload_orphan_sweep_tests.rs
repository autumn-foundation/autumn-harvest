#![cfg(feature = "db")]
//! Orphaned payload blob sweep (issue #2123).
//!
//! A write uploads its blobs before it opens its transaction. A rollback then
//! leaves blobs with no `harvest_payload_refs` row. These tests drive the
//! sweep against Postgres and pin this contract:
//!
//! - A blob that a rolled-back append uploaded is deleted after the grace
//!   period.
//! - A blob that any shard references is never deleted.
//! - A blob younger than the grace period is never deleted, even when the
//!   store ignores the cutoff.
//! - A store without listing turns the sweep off.
//! - A page budget bounds one call, and the cursor resumes it.
//! - A dry run deletes nothing.
//! - The retention runtime runs the sweep and reports it.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a Postgres that has the migrations
//! applied, with `--test-threads=1`. Otherwise the suite starts a
//! testcontainers Postgres.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_orphans::{
    OrphanSweep, OrphanSweepOptions, OrphanSweepReport, sweep_orphaned_blobs,
};
use autumn_harvest::payload_store::{
    BlobPage, OffloadedRef, PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture,
    StoredBlob,
};
use autumn_harvest::retention::{
    PartitionMaintenanceConfig, RetentionConfig, RetentionHooks, RetentionRuntime,
};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::store;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const PAGE_SIZE: usize = 2;
const GRACE: Duration = Duration::from_secs(60 * 60);

type Blobs = HashMap<String, (Vec<u8>, DateTime<Utc>)>;

/// A store that lists. Keys are content-addressed, and `put` refreshes the
/// creation time, as the `list` contract requires.
struct ListingStore {
    blobs: Mutex<Blobs>,
    can_list: bool,
    /// Ignore `created_before`, like a faulty backend.
    ignore_cutoff: bool,
    deletes: Mutex<Vec<String>>,
    /// Keys whose `delete` fails and keeps the blob.
    fail_deletes: Mutex<HashSet<String>>,
    list_calls: AtomicUsize,
    /// Offer an atomic `delete_if_older`.
    conditional: AtomicBool,
    /// A key that a writer re-uploads right after the next `list`.
    reput_after_list: Mutex<Option<String>>,
    /// Never answer a `list`, as a hung backend.
    hang_list: AtomicBool,
    /// Never answer a `delete` or `delete_if_older`, as a hung backend.
    hang_delete: AtomicBool,
}

impl ListingStore {
    fn new(can_list: bool, ignore_cutoff: bool) -> Arc<Self> {
        Arc::new(Self {
            blobs: Mutex::default(),
            can_list,
            ignore_cutoff,
            deletes: Mutex::default(),
            fail_deletes: Mutex::default(),
            list_calls: AtomicUsize::new(0),
            conditional: AtomicBool::new(true),
            reput_after_list: Mutex::default(),
            hang_list: AtomicBool::new(false),
            hang_delete: AtomicBool::new(false),
        })
    }

    fn listing() -> Arc<Self> {
        Self::new(true, false)
    }

    /// Add a blob that is `age` old.
    fn add_aged(&self, key: &str, age: Duration) {
        let created = Utc::now() - chrono::Duration::from_std(age).unwrap();
        self.blobs
            .lock()
            .unwrap()
            .insert(key.to_owned(), (key.as_bytes().to_vec(), created));
    }

    /// Move every blob back in time.
    fn age_all(&self, age: Duration) {
        let created = Utc::now() - chrono::Duration::from_std(age).unwrap();
        for entry in self.blobs.lock().unwrap().values_mut() {
            entry.1 = created;
        }
    }

    fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.blobs.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }

    fn delete_calls(&self) -> usize {
        self.deletes.lock().unwrap().len()
    }

    fn list_calls(&self) -> usize {
        AtomicUsize::load(&self.list_calls, Ordering::SeqCst)
    }

    fn set_conditional_delete(&self, on: bool) {
        self.conditional.store(on, Ordering::SeqCst);
    }

    fn reput_after_next_list(&self, key: &str) {
        *self.reput_after_list.lock().unwrap() = Some(key.to_owned());
    }

    fn fail_delete_of(&self, key: &str) {
        self.fail_deletes.lock().unwrap().insert(key.to_owned());
    }
}

impl PayloadStore for ListingStore {
    fn store_id(&self) -> &'static str {
        "orphans"
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key = format!("k{}", bytes.len());
        self.blobs
            .lock()
            .unwrap()
            .insert(key.clone(), (bytes.to_vec(), Utc::now()));
        Box::pin(async move { Ok(key) })
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let found = self.blobs.lock().unwrap().get(key).map(|e| e.0.clone());
        let key = key.to_owned();
        Box::pin(async move { found.ok_or_else(|| PayloadStoreError(format!("missing {key}"))) })
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        self.deletes.lock().unwrap().push(key.to_owned());
        if AtomicBool::load(&self.hang_delete, Ordering::SeqCst) {
            return Box::pin(std::future::pending());
        }
        if self.fail_deletes.lock().unwrap().contains(key) {
            return Box::pin(async { Err(PayloadStoreError("delete refused".to_owned())) });
        }
        self.blobs.lock().unwrap().remove(key);
        Box::pin(async move { Ok(()) })
    }

    fn delete_if_older(
        &self,
        key: &str,
        created_before: DateTime<Utc>,
    ) -> Option<PayloadStoreFuture<'_, bool>> {
        if !AtomicBool::load(&self.conditional, Ordering::SeqCst) {
            return None;
        }
        self.deletes.lock().unwrap().push(key.to_owned());
        if AtomicBool::load(&self.hang_delete, Ordering::SeqCst) {
            return Some(Box::pin(std::future::pending()));
        }
        if self.fail_deletes.lock().unwrap().contains(key) {
            return Some(Box::pin(async {
                Err(PayloadStoreError("delete refused".to_owned()))
            }));
        }
        let deleted = {
            let mut blobs = self.blobs.lock().unwrap();
            let old = blobs.get(key).is_some_and(|(_, at)| *at < created_before);
            if old {
                blobs.remove(key);
            }
            old
        };
        Some(Box::pin(async move { Ok(deleted) }))
    }

    fn list(
        &self,
        prefix: &str,
        created_before: DateTime<Utc>,
        cursor: Option<&str>,
    ) -> Option<PayloadStoreFuture<'_, BlobPage>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        if !self.can_list {
            return None;
        }
        if AtomicBool::load(&self.hang_list, Ordering::SeqCst) {
            return Some(Box::pin(std::future::pending()));
        }
        let mut found: Vec<StoredBlob> = self
            .blobs
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, (_, at))| {
                key.starts_with(prefix) && (self.ignore_cutoff || *at < created_before)
            })
            .map(|(key, (_, at))| StoredBlob {
                key: key.clone(),
                created_at: *at,
            })
            .collect();
        found.sort_by(|a, b| a.key.cmp(&b.key));
        found.retain(|blob| cursor.is_none_or(|after| blob.key.as_str() > after));
        // A writer re-uploads the key after the listing.
        if let Some(key) = self.reput_after_list.lock().unwrap().take()
            && let Some(entry) = self.blobs.lock().unwrap().get_mut(&key)
        {
            entry.1 = Utc::now();
        }
        let more = found.len() > PAGE_SIZE;
        found.truncate(PAGE_SIZE);
        let next = more
            .then(|| found.last().map(|blob| blob.key.clone()))
            .flatten();
        Some(Box::pin(async move { Ok(BlobPage { blobs: found, next }) }))
    }
}

/// Captures `record_payload_orphans` calls.
#[derive(Default)]
struct CapturingMetrics {
    orphans: Mutex<Vec<(String, u64)>>,
}

impl CapturingMetrics {
    fn total(&self, outcome: &str) -> u64 {
        self.orphans
            .lock()
            .unwrap()
            .iter()
            .filter(|(o, _)| o == outcome)
            .map(|(_, n)| n)
            .sum()
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_payload_orphans(&self, _store_id: &str, outcome: &str, count: u64) {
        self.orphans
            .lock()
            .unwrap()
            .push((outcome.to_owned(), count));
    }

    fn record_scanner_tick(&self, _scanner: &str, _shard: &str) {}
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
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
    (url, Some(container))
}

/// A fresh migrated database next to the one at `url`: the second shard.
async fn second_database(url: &str) -> String {
    let (base, _) = url.rsplit_once('/').expect("database url");
    let name = format!("shard_b_{}", Uuid::new_v4().simple());
    let mut admin = connect(url).await;
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .expect("create database");
    let other = format!("{base}/{name}");
    let mut conn = connect(&other).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrate second database");
    other
}

/// Drop the second shard database. `FORCE` ends the pool connections.
async fn drop_database(admin_url: &str, other_url: &str) {
    let (_, name) = other_url.rsplit_once('/').expect("database url");
    connect(admin_url)
        .await
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .await
        .expect("drop database");
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

async fn scrub(conn: &mut AsyncPgConnection) {
    conn.batch_execute("DELETE FROM harvest_workflow_executions;")
        .await
        .expect("scrub");
}

const fn options() -> OrphanSweepOptions {
    OrphanSweepOptions {
        grace: GRACE,
        page_budget: 100,
        dry_run: false,
        cursor: None,
        call_timeout: Duration::from_secs(30),
    }
}

async fn sweep(
    store: &ListingStore,
    pools: &ShardedDbPool,
    options: &OrphanSweepOptions,
) -> OrphanSweep {
    sweep_orphaned_blobs(store, pools, options, &CancellationToken::new()).await
}

fn report(outcome: OrphanSweep) -> OrphanSweepReport {
    match outcome {
        OrphanSweep::Swept(report) => report,
        OrphanSweep::Unsupported => panic!("the store lists, so the sweep must run"),
    }
}

fn single(url: &str) -> ShardedDbPool {
    ShardedDbPool::single(build_pool(url))
}

async fn reference(conn: &mut AsyncPgConnection, key: &str) {
    let exec = insert_execution(conn, "orphan_wf").await;
    store::insert_payload_refs(
        conn,
        exec,
        &[OffloadedRef {
            blob_key: key.to_owned(),
            store_id: "orphans".into(),
            byte_len: 1,
        }],
    )
    .await
    .expect("insert ref");
}

async fn insert_execution(conn: &mut AsyncPgConnection, name: &str) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: name,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn blob_of_a_rolled_back_append_is_swept_after_the_grace_period() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    let offloader =
        PayloadOffloader::new(store.clone(), 1024, Arc::new(CapturingMetrics::default()));

    // No execution row exists, so the event insert violates its foreign key
    // after the upload. The transaction rolls back and leaves the blob.
    let events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({ "blob": "Q".repeat(4096) }),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let result =
        store::append_events_offloaded(&mut conn, ExecutionId::new(), &events, 0, Some(&offloader))
            .await;
    assert!(result.is_err(), "the append must roll back");
    assert_eq!(store.keys().len(), 1, "the upload left a blob behind");

    let pools = single(&url);
    let young = report(sweep(&store, &pools, &options()).await);
    assert_eq!(
        (young.found, young.deleted),
        (0, 0),
        "inside the grace period"
    );
    assert_eq!(store.keys().len(), 1);

    store.age_all(GRACE * 2);
    let swept = report(sweep(&store, &pools, &options()).await);
    assert_eq!((swept.found, swept.deleted, swept.failed), (1, 1, 0));
    assert_eq!(store.keys().len(), 0);
}

#[tokio::test]
async fn blob_that_any_shard_references_is_never_deleted() {
    let (url_a, _c) = setup_db().await;
    let url_b = second_database(&url_a).await;
    let mut conn_a = connect(&url_a).await;
    let mut conn_b = connect(&url_b).await;
    scrub(&mut conn_a).await;
    reference(&mut conn_b, "only-on-b").await;
    reference(&mut conn_a, "only-on-a").await;

    let store = ListingStore::listing();
    for key in ["only-on-a", "only-on-b", "orphan"] {
        store.add_aged(key, GRACE * 3);
    }
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url_a));
    pools.insert(ShardId::new(1), build_pool(&url_b));
    let pools = ShardedDbPool::from_map(pools, ShardId::new(0));

    let swept = report(sweep(&store, &pools, &options()).await);
    assert_eq!((swept.found, swept.deleted), (1, 1));
    assert_eq!(store.keys(), ["only-on-a", "only-on-b"]);
    drop(pools);
    drop_database(&url_a, &url_b).await;
}

#[tokio::test]
async fn blob_younger_than_the_grace_period_is_never_deleted() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    // The faulty store lists young blobs too. The sweep checks the age itself.
    let store = ListingStore::new(true, true);
    store.add_aged("young", GRACE / 2);
    store.add_aged("old", GRACE * 2);

    let swept = report(sweep(&store, &single(&url), &options()).await);
    assert_eq!((swept.found, swept.deleted), (1, 1));
    assert_eq!(store.keys(), ["young"]);
}

#[tokio::test]
async fn store_without_listing_turns_the_sweep_off() {
    let (url, _c) = setup_db().await;
    let store = ListingStore::new(false, false);
    store.add_aged("old", GRACE * 2);

    let outcome = sweep(&store, &single(&url), &options()).await;
    assert_eq!(outcome, OrphanSweep::Unsupported);
    assert_eq!(store.delete_calls(), 0);
}

#[tokio::test]
async fn page_budget_bounds_a_call_and_the_cursor_resumes_it() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    for key in ["a", "b", "c", "d", "e"] {
        store.add_aged(key, GRACE * 2);
    }
    let pools = single(&url);
    let mut opts = OrphanSweepOptions {
        page_budget: 1,
        ..options()
    };

    let mut deleted = 0;
    let mut calls = 0;
    loop {
        let swept = report(sweep(&store, &pools, &opts).await);
        assert!(swept.deleted <= PAGE_SIZE as u64, "one page per call");
        deleted += swept.deleted;
        calls += 1;
        assert!(calls < 10, "the cursor must advance");
        if swept.next_cursor.is_none() {
            break;
        }
        opts.cursor = swept.next_cursor;
    }
    assert_eq!(deleted, 5);
    assert_eq!(store.keys().len(), 0);
}

#[tokio::test]
async fn dry_run_counts_orphans_and_deletes_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.add_aged("old", GRACE * 2);

    let opts = OrphanSweepOptions {
        dry_run: true,
        ..options()
    };
    let swept = report(sweep(&store, &single(&url), &opts).await);
    assert_eq!((swept.found, swept.deleted), (1, 0));
    assert_eq!(store.keys(), ["old"]);
}

fn sweep_only(grace: Duration) -> RetentionConfig {
    RetentionConfig {
        max_age_secs: None,
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        tick_interval_secs: 1,
        partitions: PartitionMaintenanceConfig {
            enabled: false,
            ..PartitionMaintenanceConfig::default()
        },
        ..RetentionConfig::default()
    }
    .without_rate_limit_bucket_gc()
    .without_terminal_task_gc()
    .with_payload_orphan_grace(grace)
}

async fn run_one_tick(
    url: &str,
    config: RetentionConfig,
    store: &Arc<ListingStore>,
    metrics: &Arc<CapturingMetrics>,
    wait_for: impl Fn() -> bool,
) {
    let offloader = Arc::new(PayloadOffloader::new(store.clone(), 1024, metrics.clone()));
    let runtime = RetentionRuntime::spawn_with_hooks(
        single(url),
        config,
        metrics.clone(),
        RetentionHooks::default().with_offloader(Some(offloader)),
    )
    .expect("the sweep alone spawns the runtime");
    runtime.run_now();
    for _ in 0..100 {
        if wait_for() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    runtime.shutdown();
    runtime.join().await.expect("join");
}

#[tokio::test]
async fn retention_runtime_sweeps_and_reports_metrics() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    reference(&mut conn, "kept").await;
    let store = ListingStore::listing();
    store.add_aged("kept", GRACE * 2);
    store.add_aged("orphan", GRACE * 2);
    store.add_aged("young", GRACE / 2);
    let metrics = Arc::new(CapturingMetrics::default());

    run_one_tick(&url, sweep_only(GRACE), &store, &metrics, || {
        store.keys().len() == 2 && metrics.total("deleted") == 1
    })
    .await;

    assert_eq!(store.keys(), ["kept", "young"]);
    assert_eq!(metrics.total("found"), 1);
    assert_eq!(metrics.total("deleted"), 1);
    assert_eq!(metrics.total("failed"), 0);
}

#[tokio::test]
async fn retention_runtime_leaves_a_store_without_listing_alone() {
    let (url, _c) = setup_db().await;
    let store = ListingStore::new(false, false);
    store.add_aged("old", GRACE * 2);
    let metrics = Arc::new(CapturingMetrics::default());

    run_one_tick(&url, sweep_only(GRACE), &store, &metrics, || {
        store.list_calls() >= 1
    })
    .await;

    assert_eq!(store.keys(), ["old"]);
    assert_eq!(store.delete_calls(), 0);
    assert_eq!(metrics.total("found"), 0);
}

#[tokio::test]
async fn retention_runtime_reports_failed_deletes() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.add_aged("stuck", GRACE * 2);
    store.fail_delete_of("stuck");
    let metrics = Arc::new(CapturingMetrics::default());

    run_one_tick(&url, sweep_only(GRACE), &store, &metrics, || {
        metrics.total("failed") == 1
    })
    .await;

    assert_eq!(store.keys(), ["stuck"], "a failed delete keeps the blob");
    assert_eq!(metrics.total("found"), 1);
    assert_eq!(metrics.total("deleted"), 0);
    assert_eq!(metrics.total("failed"), 1);
}

#[tokio::test]
async fn retention_runtime_dry_run_deletes_nothing_and_records_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.add_aged("old", GRACE * 2);
    let metrics = Arc::new(CapturingMetrics::default());
    let config = RetentionConfig {
        dry_run: true,
        ..sweep_only(GRACE)
    };

    run_one_tick(&url, config, &store, &metrics, || store.list_calls() >= 1).await;

    assert_eq!(store.keys(), ["old"]);
    assert_eq!(metrics.total("found"), 0);
    assert_eq!(store.delete_calls(), 0);
}

#[tokio::test]
async fn delete_failure_is_counted_and_the_sweep_goes_on() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.add_aged("a", GRACE * 2);
    store.add_aged("b", GRACE * 2);
    store.fail_delete_of("a");

    let swept = report(sweep(&store, &single(&url), &options()).await);
    assert_eq!((swept.found, swept.deleted, swept.failed), (2, 1, 1));
    assert_eq!(store.keys(), ["a"]);
}

#[tokio::test]
async fn failed_reference_check_deletes_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.add_aged("old", GRACE * 2);
    // Shard 1 refuses every dial, so its reference check fails.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url));
    pools.insert(
        ShardId::new(1),
        build_pool("postgres://unused:unused@127.0.0.1:1/unused"),
    );
    let pools = ShardedDbPool::from_map(pools, ShardId::new(0));
    let opts = OrphanSweepOptions {
        cursor: Some("a".to_owned()),
        ..options()
    };

    let swept = report(sweep(&store, &pools, &opts).await);
    assert!(
        swept.error.is_some(),
        "the failed check must stop the sweep"
    );
    assert_eq!((swept.found, swept.deleted), (0, 0));
    assert_eq!(swept.next_cursor.as_deref(), Some("a"));
    assert_eq!(store.keys(), ["old"]);
}

#[tokio::test]
async fn cursor_is_consumed_across_calls() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    for key in ["a", "b", "c", "d", "e"] {
        store.add_aged(key, GRACE * 2);
    }
    let pools = single(&url);
    let mut opts = OrphanSweepOptions {
        page_budget: 1,
        dry_run: true,
        ..options()
    };

    // A dry run deletes nothing, so only the cursor moves the listing on.
    let mut found = 0;
    for _ in 0..10 {
        let swept = report(sweep(&store, &pools, &opts).await);
        found += swept.found;
        if swept.next_cursor.is_none() {
            break;
        }
        opts.cursor = swept.next_cursor;
    }
    assert_eq!(found, 5);
    assert_eq!(store.keys().len(), 5);
}

#[tokio::test]
async fn pre_cancelled_shutdown_deletes_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.add_aged("old", GRACE * 2);
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let outcome = sweep_orphaned_blobs(&*store, &single(&url), &options(), &shutdown).await;
    assert_eq!(report(outcome), OrphanSweepReport::default());
    assert_eq!(store.keys(), ["old"]);
}

#[tokio::test]
async fn out_of_range_grace_is_an_error_and_keeps_the_cursor() {
    let (url, _c) = setup_db().await;
    let store = ListingStore::listing();
    store.add_aged("old", GRACE * 2);
    let opts = OrphanSweepOptions {
        grace: Duration::MAX,
        cursor: Some("a".to_owned()),
        ..options()
    };

    let swept = report(sweep(&store, &single(&url), &opts).await);
    assert!(swept.error.is_some());
    assert_eq!(swept.next_cursor.as_deref(), Some("a"));
    assert_eq!(store.keys(), ["old"]);
}

#[tokio::test]
async fn blob_re_uploaded_after_the_listing_is_not_deleted() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.add_aged("reused", GRACE * 2);
    store.reput_after_next_list("reused");

    let swept = report(sweep(&store, &single(&url), &options()).await);
    assert_eq!(
        swept.deleted, 0,
        "the conditional delete must see the new write"
    );
    assert_eq!(store.keys(), ["reused"]);
}

#[tokio::test]
async fn store_without_a_conditional_delete_falls_back_to_a_plain_delete() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.set_conditional_delete(false);
    store.add_aged("old", GRACE * 2);

    let swept = report(sweep(&store, &single(&url), &options()).await);
    assert_eq!((swept.found, swept.deleted), (1, 1));
    assert_eq!(store.keys().len(), 0);
}

/// A sweep that must end soon, or the test fails instead of hanging.
async fn sweep_within_five_seconds(
    store: &ListingStore,
    pools: &ShardedDbPool,
    options: &OrphanSweepOptions,
    shutdown: &CancellationToken,
) -> OrphanSweepReport {
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        sweep_orphaned_blobs(store, pools, options, shutdown),
    )
    .await
    .expect("a hung store must not hold the sweep");
    report(outcome)
}

#[tokio::test]
async fn hung_delete_costs_one_timeout_and_ends_the_sweep() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    for conditional in [true, false] {
        let store = ListingStore::listing();
        store.set_conditional_delete(conditional);
        for key in ["a", "b", "c"] {
            store.add_aged(key, GRACE * 2);
        }
        store.hang_delete.store(true, Ordering::SeqCst);
        let opts = OrphanSweepOptions {
            call_timeout: Duration::from_millis(200),
            ..options()
        };

        let swept =
            sweep_within_five_seconds(&store, &single(&url), &opts, &CancellationToken::new())
                .await;
        assert!(swept.error.is_some(), "the timeout is reported");
        assert_eq!(store.delete_calls(), 1, "the other keys are not tried");
        assert_eq!((swept.deleted, swept.failed), (0, 1));
        assert_eq!(store.keys(), ["a", "b", "c"]);
    }
}

#[tokio::test]
async fn hung_listing_times_out_and_keeps_the_cursor() {
    let (url, _c) = setup_db().await;
    let store = ListingStore::listing();
    store.add_aged("old", GRACE * 2);
    store.hang_list.store(true, Ordering::SeqCst);
    let opts = OrphanSweepOptions {
        call_timeout: Duration::from_millis(200),
        cursor: Some("a".to_owned()),
        ..options()
    };

    let swept =
        sweep_within_five_seconds(&store, &single(&url), &opts, &CancellationToken::new()).await;
    assert!(swept.error.is_some());
    assert_eq!(swept.next_cursor.as_deref(), Some("a"));
    assert_eq!(store.delete_calls(), 0);
}

#[tokio::test]
async fn shutdown_ends_a_hung_delete_before_the_timeout() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    let store = ListingStore::listing();
    store.add_aged("old", GRACE * 2);
    store.hang_delete.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let stopper = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        stopper.cancel();
    });

    // The default 30 s timeout is far above the 5 s bound of the helper.
    let swept = sweep_within_five_seconds(&store, &single(&url), &options(), &shutdown).await;
    assert_eq!(swept.deleted, 0);
    assert_eq!(store.keys(), ["old"]);
}
