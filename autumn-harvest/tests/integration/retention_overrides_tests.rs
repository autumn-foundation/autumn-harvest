#![cfg(feature = "db")]
//! Per-workflow-type history retention override behavioral tests — issue #737.
//!
//! Drives the real retention janitor end-to-end (`RetentionRuntime::spawn` +
//! `run_now`) against a Postgres instance and asserts:
//!
//! - AC3: a type whose override is *longer* than the global default is NOT
//!   deleted at the global age — it survives until its own age.
//! - AC4: a type whose override is *shorter* than the global default is
//!   deleted at its own (earlier) age.
//! - AC7: `dry_run` reports per-type would-delete counts and deletes nothing.
//! - AC8: the `record_retention_deleted` metric carries a per-workflow label
//!   for real deletes only.
//! - AC9: with no overrides, behavior is identical to the single-`max_age`
//!   deployment.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (single-threaded, each test scrubs the executions table
//! first); otherwise a fresh testcontainers Postgres is booted with the full
//! migration bundle (`autumn_harvest::full_migrations_sql()`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::history_export::HistoryExportDocument;
use autumn_harvest::retention::{
    ArchiverFuture, HistoryArchiver, RetentionConfig, RetentionRuntime,
};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel::sql_types::{Nullable, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

/// Capturing metrics recorder — records `(workflow, count)` from
/// `record_retention_deleted` (issue #737, AC8). All other methods no-op via
/// trait defaults.
#[derive(Default)]
struct CapturingMetrics {
    deleted: Mutex<Vec<(String, u64)>>,
}

impl CapturingMetrics {
    fn deleted(&self) -> Vec<(String, u64)> {
        self.deleted.lock().unwrap().clone()
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_retention_deleted(&self, workflow: &str, count: u64) {
        self.deleted
            .lock()
            .unwrap()
            .push((workflow.to_string(), count));
    }
}

/// Returns a live URL to a migrated Postgres, keeping the container (if any)
/// alive for the test's duration.
async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
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

/// Scrubs all workflow executions (cascades to events/tasks/etc.) plus the
/// tables retention touches, so a shared migrated DB stays isolated per test.
async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

/// Inserts a terminal (COMPLETED) execution with a distinct name/id and the
/// given completion timestamp, plus one trivial event so history exists.
async fn insert_completed(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    completed_at: DateTime<Utc>,
) -> uuid::Uuid {
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at)
         VALUES ($1, $2, 0, 'COMPLETED', '{}'::jsonb, $3, $3)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Timestamptz, _>(completed_at)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert event");
    id
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

#[derive(diesel::QueryableByName)]
struct ExistsRow {
    #[diesel(sql_type = Nullable<Text>)]
    workflow_name: Option<String>,
}

/// Returns the sorted set of surviving execution `workflow_name`s.
async fn surviving_names(conn: &mut AsyncPgConnection) -> Vec<String> {
    let rows = diesel::sql_query(
        "SELECT workflow_name FROM harvest_workflow_executions ORDER BY workflow_name",
    )
    .load::<ExistsRow>(conn)
    .await
    .expect("load survivors");
    rows.into_iter().filter_map(|r| r.workflow_name).collect()
}

/// Drives a single retention tick to completion and returns the shard-0 result.
async fn run_one_tick(
    pool: DbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
) -> autumn_harvest::retention::RetentionTickResult {
    let pools = ShardedDbPool::single(pool);
    let runtime = RetentionRuntime::spawn(pools, config, metrics, None, None)
        .expect("retention runtime should spawn when enabled");
    runtime.run_now();

    // Poll until the shard-0 tick reports a run (ran_at set), then shut down.
    let mut result = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.ran_at.is_some()
        {
            result = Some(r.clone());
            break;
        }
    }
    runtime.shutdown();
    result.expect("retention tick did not report a result in time")
}

/// A config with only workflow-history retention active (audit/schedule purges
/// off) so the tick's observable effect is deterministic.
fn history_only(max_age: Option<Duration>) -> RetentionConfig {
    RetentionConfig {
        max_age_secs: max_age.map(|d| d.as_secs()),
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        ..RetentionConfig::default()
    }
}

// AC3 + AC4 + AC8: a longer override survives the global age; a shorter
// override is deleted at its own earlier age; the metric labels the deleted
// type.
//
// The distinguishing AC4 case (a shorter override deleting a row that the
// global would have KEPT) is exercised by the `short_wf`/`control_wf` rows
// completed ~2 HOURS ago: 2h > the 1-hour override but < the 1-day global.
// This makes the test falsifiable against a `max(override, global)` mutation
// of `effective_max_age` — under that mutation `short_wf@2h` would resolve to
// the 1-day global and survive, so the survivor assertion would fail.
// Comfortable 2h-vs-1h/1day margins avoid `Utc::now()` boundary flakiness.
#[tokio::test]
async fn override_longer_survives_shorter_deleted() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);
    let two_hours_ago = now - chrono::Duration::hours(2);
    // "long_wf": override 30 days (longer than the 1-day global) -> survives.
    insert_completed(&mut conn, "long_wf", "l1", two_days_ago).await;
    // "short_wf": override 1 hour (shorter than the 1-day global). Both the
    // 2-days-ago and the 2-hours-ago rows exceed the 1h override -> deleted.
    insert_completed(&mut conn, "short_wf", "s1", two_days_ago).await;
    // AC4 crux: 2h exceeds the 1h override but is well under the 1-day global,
    // so this row is deleted ONLY because the shorter override fires early.
    insert_completed(&mut conn, "short_wf", "s2", two_hours_ago).await;
    // "default_wf": no override, uses the 1-day global -> deleted (2d > 1d).
    insert_completed(&mut conn, "default_wf", "d1", two_days_ago).await;
    // Same-age no-override control: 2h < 1-day global -> SURVIVES. Proves that
    // without the override, a 2-hour-old row is not old enough to delete.
    insert_completed(&mut conn, "control_wf", "c1", two_hours_ago).await;

    let config = history_only(Some(Duration::from_secs(86_400))) // global 1 day
        .with_workflow_override("long_wf", Duration::from_secs(30 * 86_400))
        .with_workflow_override("short_wf", Duration::from_secs(3_600));

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    let survivors = surviving_names(&mut conn).await;
    assert_eq!(
        survivors,
        vec!["control_wf".to_string(), "long_wf".to_string()],
        "long_wf (30-day override) and control_wf (2h < 1-day global) survive; \
         short_wf@2h is deleted only because its 1h override fires before the global"
    );
    assert_eq!(
        result.deleted_count, 3,
        "short_wf@2d + short_wf@2h + default_wf@2d deleted"
    );

    // AC8: metric labeled per workflow, real deletes only.
    let mut deleted = metrics.deleted();
    deleted.sort();
    assert_eq!(
        deleted,
        vec![("default_wf".to_string(), 1), ("short_wf".to_string(), 2)],
        "per-type deletion metric must name exactly the deleted types with counts"
    );
    // Per-type reporting on the tick result (AC7 reporting surface).
    assert_eq!(result.deleted_by_workflow.get("short_wf"), Some(&2));
    assert_eq!(result.deleted_by_workflow.get("default_wf"), Some(&1));
    assert!(!result.deleted_by_workflow.contains_key("long_wf"));
    assert!(!result.deleted_by_workflow.contains_key("control_wf"));
}

// AC7: dry_run reports per-type would-delete counts and deletes nothing.
#[tokio::test]
async fn dry_run_reports_per_type_counts_without_deleting() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    insert_completed(&mut conn, "dry_a", "a1", old).await;
    insert_completed(&mut conn, "dry_a", "a2", old).await;
    insert_completed(&mut conn, "dry_b", "b1", old).await;

    let mut config = history_only(Some(Duration::from_secs(86_400)));
    config.dry_run = true;

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    // Nothing actually deleted.
    let mut survivors = surviving_names(&mut conn).await;
    survivors.sort();
    assert_eq!(
        survivors,
        vec![
            "dry_a".to_string(),
            "dry_a".to_string(),
            "dry_b".to_string()
        ],
        "dry_run must not delete any rows"
    );
    // But per-type would-delete counts are reported.
    assert_eq!(result.deleted_by_workflow.get("dry_a"), Some(&2));
    assert_eq!(result.deleted_by_workflow.get("dry_b"), Some(&1));
    // AC8: metric is NOT emitted for dry-run would-deletes.
    assert!(
        metrics.deleted().is_empty(),
        "dry_run must not emit the real-delete metric"
    );
}

// AC9: with no overrides, behavior is identical to a single-max_age deployment
// (everything past the global age is deleted; nothing younger is).
#[tokio::test]
async fn no_overrides_behaves_like_global_only() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    insert_completed(
        &mut conn,
        "compat_old",
        "o1",
        now - chrono::Duration::days(2),
    )
    .await;
    insert_completed(
        &mut conn,
        "compat_young",
        "y1",
        now - chrono::Duration::minutes(5),
    )
    .await;

    // Global 1-day, no overrides.
    let config = history_only(Some(Duration::from_secs(86_400)));
    assert!(config.workflow_overrides().is_empty());

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    let survivors = surviving_names(&mut conn).await;
    assert_eq!(
        survivors,
        vec!["compat_young".to_string()],
        "only the old row is deleted under the global age"
    );
    assert_eq!(result.deleted_count, 1);
    assert_eq!(metrics.deleted(), vec![("compat_old".to_string(), 1)]);
}

// AC2: overrides-only config (no global max_age) deletes exactly the overridden
// types past their own age and never touches types without an override.
#[tokio::test]
async fn overrides_only_never_deletes_types_without_override() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let old = Utc::now() - chrono::Duration::days(2);
    insert_completed(&mut conn, "keep_forever", "k1", old).await;
    insert_completed(&mut conn, "purge_me", "p1", old).await;

    // No global max_age; only "purge_me" has an override (1 hour).
    let config = history_only(None).with_workflow_override("purge_me", Duration::from_secs(3_600));
    assert!(config.enabled(), "overrides-only config must be enabled");

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    let survivors = surviving_names(&mut conn).await;
    assert_eq!(
        survivors,
        vec!["keep_forever".to_string()],
        "a type with no override and no global age is never deleted"
    );
    assert_eq!(result.deleted_count, 1);
    assert_eq!(metrics.deleted(), vec![("purge_me".to_string(), 1)]);
    // Scalability optimization (issue #737): in overrides-only mode the SQL
    // pre-filter is bounded to the overridden type(s), so the never-delete
    // "keep_forever" row is never even SELECTed as a candidate. Only "purge_me"
    // is scanned. Before the `workflow_name = ANY(...)` filter, candidate_count
    // would be 2 (both rows loaded, one skipped).
    assert_eq!(
        result.candidate_count, 1,
        "overrides-only pre-filter must not scan never-delete types"
    );
}

// Mixed-policy fairness (issue #737, PR #990 review): a global default plus a
// LONGER per-workflow override — the canonical compliance config (short default,
// long financial-record retention). The long-retained type's rows are OLDER
// (smaller `completed_at`), so under the old single-loose-cutoff SELECT
// (`ORDER BY completed_at ASC`) they were returned FIRST, claimed into the
// batch, and skipped as not-yet-eligible — consuming the batch budget and
// starving/delaying deletion of newer already-expired rows of the shorter
// policy. The fix pushes each row's exact per-type cutoff into the predicate, so
// only genuinely-eligible rows are ever selected.
//
// Falsifiable against the old behavior: with `batch_size = 2` and a 3-row
// not-yet-eligible `archive_wf` backlog that is OLDER than the one eligible
// `global_wf` row, the old code would load 2 archive_wf rows (oldest first),
// skip both, exhaust `remaining`, and never reach `global_wf` — deleting nothing
// and reporting `candidate_count = 2`. The fix selects only the eligible row.
#[tokio::test]
async fn not_yet_eligible_backlog_does_not_starve_eligible_deletion() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    // A backlog of `archive_wf` rows, all OLDER than the eligible global_wf row
    // but NONE eligible under the 365-day override. These would sort FIRST under
    // `ORDER BY completed_at ASC` and consume the batch under the old behavior.
    insert_completed(
        &mut conn,
        "archive_wf",
        "a60",
        now - chrono::Duration::days(60),
    )
    .await;
    insert_completed(
        &mut conn,
        "archive_wf",
        "a45",
        now - chrono::Duration::days(45),
    )
    .await;
    insert_completed(
        &mut conn,
        "archive_wf",
        "a30",
        now - chrono::Duration::days(30),
    )
    .await;
    // One eligible short-policy row: 2 days > the 1-day global default.
    insert_completed(
        &mut conn,
        "global_wf",
        "g1",
        now - chrono::Duration::days(2),
    )
    .await;

    // Global 1 day; archive_wf retained 365 days. Small batch so the backlog
    // would exhaust it before reaching global_wf under the old loose cutoff.
    let mut config = history_only(Some(Duration::from_secs(86_400)))
        .with_workflow_override("archive_wf", Duration::from_secs(365 * 86_400));
    config.batch_size = 2;

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    // The eligible short-policy row is deleted; the entire not-yet-eligible
    // backlog survives — it never starved the eligible deletion.
    let mut survivors = surviving_names(&mut conn).await;
    survivors.sort();
    assert_eq!(
        survivors,
        vec![
            "archive_wf".to_string(),
            "archive_wf".to_string(),
            "archive_wf".to_string(),
        ],
        "every not-yet-eligible archive_wf row survives; only the eligible \
         global_wf row is deleted despite the small batch"
    );
    assert_eq!(
        result.deleted_count, 1,
        "exactly the eligible global_wf row"
    );

    // Only the eligible row is ever SELECTed — the not-yet-eligible backlog is
    // filtered out in SQL and never consumes candidate/batch budget. Under the
    // old loose-cutoff behavior this would have been 2 (both oldest archive_wf
    // rows loaded and skipped).
    assert_eq!(
        result.candidate_count, 1,
        "per-type SQL cutoff must not select not-yet-eligible rows"
    );

    // Per-type reporting and metric name exactly the deleted type.
    assert_eq!(result.deleted_by_workflow.get("global_wf"), Some(&1));
    assert!(!result.deleted_by_workflow.contains_key("archive_wf"));
    assert_eq!(metrics.deleted(), vec![("global_wf".to_string(), 1)]);
}

// Issue #772 (Codex Finding A): the retention archive writes the LAST cold-storage
// copy before an execution row is permanently deleted. A completed / continued
// run can carry a recorded `SideEffectRecorded{Now}` deadline probe (issue #772)
// before its next command or terminal event, so the archived
// `HistoryExportDocument` MUST carry `execution_timeout`/`deadline_at` — replaying
// the archive later with no deadline would leave that probe unconsumed and
// false-report non-determinism, and after deletion there is no row left from
// which an operator could recover the missing values.
//
// Falsifiable: before the fix the retention `HistoryExportRequest` hardcoded
// `execution_timeout: None, deadline_at: None`, so the captured archive doc's
// deadline metadata was absent even though the deleted row carried it.
#[tokio::test]
async fn retention_archive_carries_deadline_metadata() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);
    // The live effective deadline the timeout scanner enforced for this run.
    // Whole-second so it round-trips through Postgres `timestamptz` (microsecond
    // precision) with an exact equality check against the archived document.
    let deadline_at =
        DateTime::from_timestamp(two_days_ago.timestamp() + 30, 0).expect("valid deadline");
    let exec_id = insert_completed_with_deadline(
        &mut conn,
        "deadline_wf",
        "dw1",
        two_days_ago,
        30,
        deadline_at,
    )
    .await;

    // Global 1-day age -> the 2-day-old row is eligible for deletion+archival.
    let config = history_only(Some(Duration::from_secs(86_400)));
    let metrics = Arc::new(CapturingMetrics::default());
    let archiver = Arc::new(CapturingArchiver::default());

    let result = run_one_tick_with_archiver(
        pool,
        config,
        Arc::clone(&metrics),
        Arc::clone(&archiver) as Arc<dyn HistoryArchiver>,
    )
    .await;

    assert_eq!(
        result.deleted_count, 1,
        "the eligible row is archived+deleted"
    );
    assert!(
        surviving_names(&mut conn).await.is_empty(),
        "the archived row is deleted from the primary store"
    );

    let docs = archiver.docs();
    assert_eq!(docs.len(), 1, "exactly one execution was archived");
    let doc = &docs[0];
    assert_eq!(doc.execution_id, exec_id);
    // The archive — the last surviving copy — must carry the deadline budget.
    assert_eq!(
        doc.execution_timeout.map(|d| d.num_seconds()),
        Some(30),
        "archived doc must carry execution_timeout (issue #772 Finding A)"
    );
    assert_eq!(
        doc.deadline_at,
        Some(deadline_at),
        "archived doc must carry the live deadline_at (issue #772 Finding A)"
    );
    // The archived history genuinely contains the deadline probe the missing
    // metadata would strand unconsumed on a later replay.
    assert_eq!(
        doc.events.len(),
        2,
        "WorkflowStarted + deadline probe archived"
    );
    assert_eq!(doc.events[1]["type"], "SideEffectRecorded");
    assert_eq!(
        doc.events[1]["data"]["name"],
        autumn_harvest::DEADLINE_PROBE_SIDE_EFFECT_NAME
    );
}

/// Capturing archiver — records the `HistoryExportDocument` handed to the
/// retention janitor so a test can assert what the last cold-storage copy
/// carried (issue #772 Finding A).
#[derive(Default)]
struct CapturingArchiver {
    docs: Mutex<Vec<HistoryExportDocument>>,
}

impl CapturingArchiver {
    fn docs(&self) -> Vec<HistoryExportDocument> {
        self.docs.lock().unwrap().clone()
    }
}

impl HistoryArchiver for CapturingArchiver {
    fn archive(&self, doc: &HistoryExportDocument) -> ArchiverFuture<'_> {
        self.docs.lock().unwrap().push(doc.clone());
        Box::pin(async { Ok(()) })
    }
}

/// Drives a single retention tick with a registered archiver and returns the
/// shard-0 result.
async fn run_one_tick_with_archiver(
    pool: DbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
    archiver: Arc<dyn HistoryArchiver>,
) -> autumn_harvest::retention::RetentionTickResult {
    let pools = ShardedDbPool::single(pool);
    let runtime = RetentionRuntime::spawn(pools, config, metrics, Some(archiver), None)
        .expect("retention runtime should spawn when enabled");
    runtime.run_now();

    let mut result = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.ran_at.is_some()
        {
            result = Some(r.clone());
            break;
        }
    }
    runtime.shutdown();
    result.expect("retention tick did not report a result in time")
}

/// Inserts a terminal (COMPLETED) execution carrying an `execution_timeout` +
/// live `deadline_at` and a deadline-probe-bearing history (issue #772).
async fn insert_completed_with_deadline(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    completed_at: DateTime<Utc>,
    execution_timeout_secs: i32,
    deadline_at: DateTime<Utc>,
) -> ExecutionId {
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
             execution_timeout, deadline_at)
         VALUES ($1, $2, 0, 'COMPLETED', '{}'::jsonb, $3, $3,
             make_interval(secs => $4), $5)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Timestamptz, _>(completed_at)
    .bind::<diesel::sql_types::Double, _>(f64::from(execution_timeout_secs))
    .bind::<Timestamptz, _>(deadline_at)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;

    let exec_id = ExecutionId::from_uuid(id);
    let recorded_now = completed_at;
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: completed_at,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Now,
            name: Some(autumn_harvest::DEADLINE_PROBE_SIDE_EFFECT_NAME.to_string()),
            value: serde_json::json!(recorded_now.timestamp_millis()),
        },
    ];
    autumn_harvest::store::append_events(conn, exec_id, &events, 0)
        .await
        .expect("append probe history");
    exec_id
}
