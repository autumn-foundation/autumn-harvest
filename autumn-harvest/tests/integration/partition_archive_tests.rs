#![cfg(feature = "db")]
//! Partition export before the drop (issue #2009).
//!
//! These tests prove the done-when line of the issue: an aged partition is
//! exported, verified, then dropped, and it can be read back. They also
//! prove that each failure keeps the partition.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise a testcontainers Postgres starts with the full migration bundle.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::partition::{self, EnableOptions, SweepOptions};
use autumn_harvest::partition_archive::{
    self, ArchiveIo, DirectoryPartitionArchiver, PartitionArchiver, PartitionExport,
};
use autumn_harvest::retention::{RetentionConfig, RetentionHooks, RetentionRuntime};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, TimeZone, Utc};
use diesel::sql_types::{BigInt, Bool, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── Harness ────────────────────────────────────────────────────────────────

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

/// Start each test from an empty, unpartitioned shard.
async fn reset(conn: &mut AsyncPgConnection) {
    partition::disable_partitioning(conn)
        .await
        .expect("revert to unpartitioned layout");
    for stmt in [
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_execution_summaries",
        "DELETE FROM harvest_workflow_executions",
        "DELETE FROM harvest_events",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

async fn reset_partitioned(conn: &mut AsyncPgConnection) {
    reset(conn).await;
    partition::enable_partitioning(conn, &EnableOptions::default())
        .await
        .expect("enable");
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    v: String,
}

#[derive(diesel::QueryableByName)]
struct BoolRow {
    #[diesel(sql_type = Bool)]
    v: bool,
}

async fn exists(conn: &mut AsyncPgConnection, table: &str) -> bool {
    diesel::sql_query(format!(
        "SELECT to_regclass('{table}') IS NOT NULL AS v"
    ))
    .get_result::<BoolRow>(conn)
    .await
    .expect("to_regclass")
    .v
}

async fn row_count(conn: &mut AsyncPgConnection, table: &str) -> i64 {
    diesel::sql_query(format!("SELECT COUNT(*)::bigint AS n FROM {table}"))
        .get_result::<CountRow>(conn)
        .await
        .expect("count")
        .n
}

/// Each row of `table` as `to_jsonb(row)`, in `id` order.
async fn rows_as_json(conn: &mut AsyncPgConnection, table: &str) -> Vec<serde_json::Value> {
    diesel::sql_query(format!(
        "SELECT to_jsonb(e)::text AS v FROM {table} e ORDER BY e.id"
    ))
    .load::<TextRow>(conn)
    .await
    .expect("rows")
    .into_iter()
    .map(|r| serde_json::from_str(&r.v).expect("row json"))
    .collect()
}

async fn insert_execution(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    at: DateTime<Utc>,
) -> uuid::Uuid {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, created_at, started_at, completed_at)
         VALUES ('archive_wf', $1, 0, 'COMPLETED', '{}'::jsonb, $2, $2, $2)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_id)
    .bind::<Timestamptz, _>(at)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id
}

/// Move a run's rows into the cohort of `at`, as if appended then.
async fn backdate_events(conn: &mut AsyncPgConnection, exec: uuid::Uuid, at: DateTime<Utc>) {
    partition::ensure_cohort(conn, at)
        .await
        .expect("materialize the destination cohort");
    autumn_harvest::append_only::with_guard_off(conn, async |c| {
        diesel::sql_query(
            "UPDATE harvest_events
                SET cohort = harvest_event_cohort($1), timestamp = $1
              WHERE workflow_exec_id = $2",
        )
        .bind::<Timestamptz, _>(at)
        .bind::<diesel::sql_types::Uuid, _>(exec)
        .execute(c)
        .await
    })
    .await
    .expect("backdate events");
}

fn sample_events() -> Vec<WorkflowEvent> {
    vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"customer": "acme", "amount": 42}),
            timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "checkpoint".into(),
            details: serde_json::json!({"step": 3}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"status": "ok"}),
        },
    ]
}

fn as_json(events: &[WorkflowEvent]) -> Vec<serde_json::Value> {
    events
        .iter()
        .map(|e| serde_json::to_value(e).expect("event serializes"))
        .collect()
}

/// Seed `n` terminal runs whose history sits in the cohort of `at`.
async fn seed_runs(
    conn: &mut AsyncPgConnection,
    at: DateTime<Utc>,
    n: usize,
) -> Vec<uuid::Uuid> {
    let mut ids = Vec::new();
    for i in 0..n {
        let exec = insert_execution(conn, &format!("pa-{i}-{}", uuid::Uuid::new_v4()), at).await;
        autumn_harvest::store::append_events(
            conn,
            ExecutionId::from_uuid(exec),
            &sample_events(),
            0,
        )
        .await
        .expect("seed history");
        backdate_events(conn, exec, at).await;
        ids.push(exec);
    }
    ids
}

/// Delete the run rows, as retention does. Their events become orphans.
async fn delete_runs(conn: &mut AsyncPgConnection, ids: &[uuid::Uuid]) {
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = ANY($1)")
        .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(ids.to_vec())
        .execute(conn)
        .await
        .expect("delete runs");
}

/// The partition, its bounds, and the orphan runs it holds.
struct Aged {
    name: String,
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
    runs: Vec<uuid::Uuid>,
}

async fn seed_aged_partition(conn: &mut AsyncPgConnection, n: usize) -> Aged {
    let at = Utc::now() - chrono::Duration::days(30);
    let runs = seed_runs(conn, at, n).await;
    delete_runs(conn, &runs).await;
    let lower = partition::cohort_start(at, partition::DEFAULT_COHORT_WIDTH_SECS);
    let upper = lower + chrono::Duration::seconds(partition::DEFAULT_COHORT_WIDTH_SECS);
    Aged {
        name: partition::partition_name(lower),
        lower,
        upper,
        runs,
    }
}

/// A backend with switchable faults. It logs each call in order.
#[derive(Default)]
struct TestArchiver {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    log: Mutex<Vec<String>>,
    fail_put: bool,
    lose_on_get: bool,
    corrupt_on_get: bool,
    /// When set, the manifest upload changes one row of this partition.
    change_row: Mutex<Option<(String, String)>>,
}

impl PartitionArchiver for TestArchiver {
    fn put<'a>(&'a self, key: &'a str, bytes: Vec<u8>) -> ArchiveIo<'a, ()> {
        self.log.lock().unwrap().push(format!("put {key}"));
        Box::pin(async move {
            if self.fail_put {
                return Err("bucket unavailable".into());
            }
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
            let change = if key.ends_with("manifest.json") {
                self.change_row.lock().unwrap().take()
            } else {
                None
            };
            if let Some((url, table)) = change {
                let mut conn = connect(&url).await;
                autumn_harvest::append_only::with_guard_off(&mut conn, async |c| {
                    diesel::sql_query(format!(
                        "UPDATE {table} SET event_data = event_data || '{{\"rotated\": true}}'::jsonb
                          WHERE id = (SELECT min(id) FROM {table})"
                    ))
                    .execute(c)
                    .await
                })
                .await
                .expect("change one row after the export");
            }
            Ok(())
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> ArchiveIo<'a, Option<Vec<u8>>> {
        self.log.lock().unwrap().push(format!("get {key}"));
        let mut got = self.objects.lock().unwrap().get(key).cloned();
        if self.lose_on_get {
            got = None;
        }
        if self.corrupt_on_get
            && let Some(bytes) = got.as_mut()
            && let Some(first) = bytes.first_mut()
        {
            *first ^= 0x01;
        }
        Box::pin(async move { Ok(got) })
    }
}

fn export_to(archiver: Arc<dyn PartitionArchiver>) -> PartitionExport {
    PartitionExport::new(archiver, 0).with_io_timeout(Duration::from_secs(10))
}

async fn sweep_with(
    conn: &mut AsyncPgConnection,
    archiver: Arc<dyn PartitionArchiver>,
) -> partition::SweepOutcome {
    partition::sweep_exporting(
        conn,
        Utc::now(),
        &SweepOptions::default(),
        None,
        &export_to(archiver),
    )
    .await
    .expect("sweep")
}

fn expected_manifest_key(name: &str, lower: Option<DateTime<Utc>>, upper: DateTime<Utc>) -> String {
    partition_archive::manifest_key(&partition_archive::archive_prefix(0, name, lower, upper))
}

// ── Done when: exported, verified, then dropped, and read back ────────────

#[tokio::test]
async fn an_aged_partition_is_exported_verified_then_dropped_and_reads_back() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_partitioned(&mut conn).await;
    let aged = seed_aged_partition(&mut conn, 3).await;
    let before = rows_as_json(&mut conn, &aged.name).await;
    assert_eq!(before.len(), 9, "precondition: three runs of three events");

    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(TestArchiver::default());
    let outcome = sweep_with(&mut conn, backend.clone()).await;

    let key = expected_manifest_key(&aged.name, Some(aged.lower), aged.upper);
    assert_eq!(outcome.dropped, vec![aged.name.clone()], "{outcome:?}");
    assert_eq!(outcome.exported, vec![key.clone()], "{outcome:?}");
    assert!(!exists(&mut conn, &aged.name).await, "the partition is dropped");

    // Order: every segment, then the manifest, then the read-back.
    let log = backend.log.lock().unwrap().clone();
    let last_put = log.iter().rposition(|l| l.starts_with("put ")).unwrap();
    let first_get = log.iter().position(|l| l.starts_with("get ")).unwrap();
    assert_eq!(log[last_put], format!("put {key}"), "the manifest goes last: {log:?}");
    assert!(first_get > last_put, "verify reads back after the upload: {log:?}");
    assert!(log.contains(&format!("get {key}")), "the manifest is read back: {log:?}");

    // Copy the objects into a directory backend to read back through it.
    let disk = DirectoryPartitionArchiver::new(dir.path());
    for (k, v) in backend.objects.lock().unwrap().clone() {
        disk.put(&k, v).await.unwrap();
    }
    let archived = partition_archive::read_back(&disk, &key)
        .await
        .expect("read back");
    assert_eq!(archived.manifest.partition, aged.name);
    assert_eq!(archived.manifest.shard_id, 0);
    assert_eq!(archived.manifest.lower, Some(aged.lower));
    assert_eq!(archived.manifest.upper, aged.upper);
    assert_eq!(archived.manifest.row_count, 9);
    let after: Vec<serde_json::Value> = archived
        .rows
        .iter()
        .map(|r| serde_json::to_value(r).unwrap())
        .collect();
    assert_eq!(after, before, "the archive holds every row, byte for byte");
    for run in &aged.runs {
        let history = archived.history(ExecutionId::from_uuid(*run)).unwrap();
        assert_eq!(as_json(&history), as_json(&sample_events()));
    }
}

#[tokio::test]
async fn the_legacy_partition_exports_under_a_min_lower_bound() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let runs = seed_runs(&mut conn, Utc::now() - chrono::Duration::days(3), 2).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable on a populated table");
    delete_runs(&mut conn, &runs).await;
    let legacy = partition::list_partitions(&mut conn)
        .await
        .unwrap()
        .into_iter()
        .find(|p| p.name == partition::LEGACY_PARTITION)
        .expect("the legacy partition");

    let backend = Arc::new(TestArchiver::default());
    let outcome = sweep_with(&mut conn, backend.clone()).await;

    let key = expected_manifest_key(partition::LEGACY_PARTITION, None, legacy.upper.unwrap());
    assert!(key.contains("/min_"), "{key}");
    assert!(outcome.exported.contains(&key), "{outcome:?}");
    assert!(!exists(&mut conn, partition::LEGACY_PARTITION).await);
    let archived = partition_archive::read_back(backend.as_ref(), &key)
        .await
        .expect("read back");
    assert_eq!(archived.manifest.lower, None);
    assert_eq!(archived.rows.len(), 6);
    assert!(
        archived
            .rows
            .iter()
            .all(|r| r.extra.get("cohort") == Some(&serde_json::json!("-infinity"))),
        "legacy rows keep the -infinity cohort"
    );
}

// ── Each failure keeps the partition ──────────────────────────────────────

async fn assert_kept(conn: &mut AsyncPgConnection, aged: &Aged, outcome: &partition::SweepOutcome, why: &str) {
    assert!(outcome.dropped.is_empty(), "{why}: {outcome:?}");
    assert!(outcome.exported.is_empty(), "{why}: {outcome:?}");
    assert!(
        outcome
            .blocked
            .iter()
            .any(|b| b.starts_with(&aged.name) && b.contains(why)),
        "{why}: the reason is reported: {outcome:?}"
    );
    assert!(exists(conn, &aged.name).await, "{why}: the partition stays");
    assert_eq!(row_count(conn, &aged.name).await, 9, "{why}: every row stays");
}

#[tokio::test]
async fn a_failed_upload_keeps_the_partition() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_partitioned(&mut conn).await;
    let aged = seed_aged_partition(&mut conn, 3).await;
    let backend = Arc::new(TestArchiver {
        fail_put: true,
        ..TestArchiver::default()
    });
    let outcome = sweep_with(&mut conn, backend).await;
    assert_kept(&mut conn, &aged, &outcome, "export failed").await;
}

#[tokio::test]
async fn a_lost_object_keeps_the_partition() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_partitioned(&mut conn).await;
    let aged = seed_aged_partition(&mut conn, 3).await;
    let backend = Arc::new(TestArchiver {
        lose_on_get: true,
        ..TestArchiver::default()
    });
    let outcome = sweep_with(&mut conn, backend).await;
    assert_kept(&mut conn, &aged, &outcome, "export failed").await;
}

#[tokio::test]
async fn changed_bytes_from_the_backend_keep_the_partition() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_partitioned(&mut conn).await;
    let aged = seed_aged_partition(&mut conn, 3).await;
    let backend = Arc::new(TestArchiver {
        corrupt_on_get: true,
        ..TestArchiver::default()
    });
    let outcome = sweep_with(&mut conn, backend).await;
    assert_kept(&mut conn, &aged, &outcome, "export failed").await;
}

#[tokio::test]
async fn a_row_changed_after_the_export_keeps_the_partition_until_a_new_export() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_partitioned(&mut conn).await;
    let aged = seed_aged_partition(&mut conn, 3).await;
    let backend = Arc::new(TestArchiver::default());
    *backend.change_row.lock().unwrap() = Some((url.clone(), aged.name.clone()));

    let first = sweep_with(&mut conn, backend.clone()).await;
    assert_kept(&mut conn, &aged, &first, "changed since export").await;

    let current = rows_as_json(&mut conn, &aged.name).await;
    let second = sweep_with(&mut conn, backend.clone()).await;
    assert_eq!(second.dropped, vec![aged.name.clone()], "{second:?}");
    let key = expected_manifest_key(&aged.name, Some(aged.lower), aged.upper);
    let archived = partition_archive::read_back(backend.as_ref(), &key)
        .await
        .expect("read back");
    let rows: Vec<serde_json::Value> = archived
        .rows
        .iter()
        .map(|r| serde_json::to_value(r).unwrap())
        .collect();
    assert_eq!(rows, current, "the new export holds the changed row");
    assert_eq!(rows[0]["event_data"]["rotated"], serde_json::json!(true));
}

#[tokio::test]
async fn a_partition_a_live_run_owns_is_not_exported() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_partitioned(&mut conn).await;
    let at = Utc::now() - chrono::Duration::days(30);
    seed_runs(&mut conn, at, 1).await;
    let backend = Arc::new(TestArchiver::default());
    let outcome = sweep_with(&mut conn, backend.clone()).await;
    assert!(outcome.dropped.is_empty(), "{outcome:?}");
    assert!(
        backend.log.lock().unwrap().is_empty(),
        "the ownership gate runs before any upload"
    );
}

// ── The retention runtime hands the partition to the archiver ────────────

#[derive(Default)]
struct NoopMetrics;
impl MetricsRecorder for NoopMetrics {}

#[tokio::test]
async fn the_retention_runtime_exports_before_it_drops() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_partitioned(&mut conn).await;
    let at = Utc::now() - chrono::Duration::days(30);
    let runs = seed_runs(&mut conn, at, 2).await;
    let lower = partition::cohort_start(at, partition::DEFAULT_COHORT_WIDTH_SECS);
    let upper = lower + chrono::Duration::seconds(partition::DEFAULT_COHORT_WIDTH_SECS);
    let name = partition::partition_name(lower);

    let dir = tempfile::tempdir().unwrap();
    let disk: Arc<dyn PartitionArchiver> = Arc::new(DirectoryPartitionArchiver::new(dir.path()));
    let started = Utc::now();
    let runtime = RetentionRuntime::spawn_with_hooks(
        ShardedDbPool::single(build_pool(&url)),
        RetentionConfig::with_max_age(Duration::from_secs(86_400)),
        Arc::new(NoopMetrics),
        RetentionHooks {
            partition_archiver: Some(disk.clone()),
            ..RetentionHooks::default()
        },
    )
    .expect("retention runtime should spawn when enabled");
    runtime.run_now();
    let mut result = None;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.partition_maintenance
                .as_ref()
                .and_then(|m| m.at)
                .is_some_and(|t| t >= started)
        {
            result = Some(r.clone());
            break;
        }
    }
    runtime.shutdown();
    let result = result.expect("partition maintenance ran");
    assert_eq!(result.deleted_count, 2, "retention collected both runs");
    let sweep = &result.partition_maintenance.as_ref().unwrap().sweep;
    let key = expected_manifest_key(&name, Some(lower), upper);
    assert_eq!(sweep.exported, vec![key.clone()], "{sweep:?}");
    assert!(!exists(&mut conn, &name).await, "the partition is dropped");
    let archived = partition_archive::read_back(disk.as_ref(), &key)
        .await
        .expect("read back");
    for run in runs {
        let history = archived.history(ExecutionId::from_uuid(run)).unwrap();
        assert_eq!(as_json(&history), as_json(&sample_events()));
    }
}
