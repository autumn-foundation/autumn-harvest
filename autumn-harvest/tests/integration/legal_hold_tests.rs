#![cfg(feature = "db")]
//! Per-execution legal hold behavioral tests — issue #747.
//!
//! Drives the real retention janitor + erase + set/release core against a
//! Postgres instance and asserts:
//!
//! - A held execution SURVIVES past its retention age, and
//!   `record_retention_deleted` reports ZERO for the held id.
//! - An expired hold becomes eligible again and is deleted on the next tick.
//! - A hold trumps both the global and a per-type override.
//! - `erase_workflow_payloads` rejects a held execution with a
//!   `HarvestError::Config` (→ 409) naming the hold, tombstoning nothing.
//! - `set_legal_hold` / `release_legal_hold` are idempotent (round-trip).
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (single-threaded; each test scrubs first); otherwise a
//! fresh testcontainers Postgres is booted with `INIT_SQL`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::retention::{RetentionConfig, RetentionRuntime};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260709000001_harvest_legal_hold/up.sql"),
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    "\n",
    include_str!("../../migrations/20260710000002_harvest_workflow_continue_chain/up.sql"),
);

/// Capturing metrics recorder — records `(workflow, count)` from
/// `record_retention_deleted` (issue #737 AC8). All other methods no-op.
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

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
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

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// Inserts a terminal (COMPLETED) execution with a distinct name/id and the
/// given completion timestamp, plus one trivial event so history exists.
/// Optionally applies a legal hold via the four columns directly.
async fn insert_completed(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    completed_at: DateTime<Utc>,
    hold_set_at: Option<DateTime<Utc>>,
    hold_until: Option<DateTime<Utc>>,
) -> uuid::Uuid {
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
             legal_hold_set_at, legal_hold_until, legal_hold_reason, legal_hold_actor)
         VALUES ($1, $2, 0, 'COMPLETED', '{}'::jsonb, $3, $3, $4, $5,
                 CASE WHEN $4 IS NULL THEN NULL ELSE 'seed-hold' END,
                 CASE WHEN $4 IS NULL THEN NULL ELSE 'seed-actor' END)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Timestamptz, _>(completed_at)
    .bind::<Nullable<Timestamptz>, _>(hold_set_at)
    .bind::<Nullable<Timestamptz>, _>(hold_until)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{\"pii\":\"x\"},\"timestamp\":\"2026-01-01T00:00:00Z\"}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert event");
    id
}

async fn surviving_names(conn: &mut AsyncPgConnection) -> Vec<String> {
    let rows = diesel::sql_query(
        "SELECT workflow_name FROM harvest_workflow_executions ORDER BY workflow_name",
    )
    .load::<ExistsRow>(conn)
    .await
    .expect("load survivors");
    rows.into_iter().filter_map(|r| r.workflow_name).collect()
}

async fn tombstoned_event_count(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events
         WHERE workflow_exec_id = $1
           AND event_data -> 'data' -> 'input' ? '_harvest_erased'",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result::<CountRow>(conn)
    .await
    .expect("count tombstoned")
    .n
}

async fn run_one_tick(
    pool: DbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
) -> autumn_harvest::retention::RetentionTickResult {
    let pools = ShardedDbPool::single(pool);
    let runtime = RetentionRuntime::spawn(pools, config, metrics, None, None)
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

async fn run_one_tick_with_archiver(
    pool: DbPool,
    config: RetentionConfig,
    metrics: Arc<CapturingMetrics>,
    archiver: Arc<dyn autumn_harvest::HistoryArchiver>,
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

/// A `HistoryArchiver` that records every archived `exec_id` and, when
/// `place_hold_on` is set, places a legal hold on that execution FROM INSIDE
/// `archive()` — deterministically injecting the "hold committed after
/// candidate selection but before delete" race that the delete-tx `FOR UPDATE`
/// re-check must honor (issue #747 BLOCKER 1).
struct RaceArchiver {
    url: String,
    archived: Mutex<Vec<uuid::Uuid>>,
    place_hold_on: Option<uuid::Uuid>,
}

impl RaceArchiver {
    fn archived_ids(&self) -> Vec<uuid::Uuid> {
        self.archived.lock().unwrap().clone()
    }
}

impl autumn_harvest::HistoryArchiver for RaceArchiver {
    fn archive(
        &self,
        doc: &autumn_harvest::history_export::HistoryExportDocument,
    ) -> autumn_harvest::ArchiverFuture<'_> {
        let exec_id = doc.execution_id;
        let exec_uuid = exec_id.as_uuid();
        Box::pin(async move {
            // Drop the guard before any await (keeps the future `Send`).
            self.archived.lock().unwrap().push(exec_uuid);

            if self.place_hold_on == Some(exec_uuid) {
                let mut c = AsyncPgConnection::establish(&self.url)
                    .await
                    .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?;
                autumn_harvest::set_legal_hold(
                    &mut c,
                    exec_id,
                    "mid-tick hold",
                    None,
                    "racer",
                    Utc::now(),
                )
                .await
                .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))?;
            }
            Ok(())
        })
    }
}

async fn hold_columns(
    conn: &mut AsyncPgConnection,
    id: uuid::Uuid,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>, Option<String>) {
    #[derive(diesel::QueryableByName)]
    #[allow(clippy::struct_field_names)]
    struct HoldRow {
        #[diesel(sql_type = Nullable<Timestamptz>)]
        legal_hold_set_at: Option<DateTime<Utc>>,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        legal_hold_until: Option<DateTime<Utc>>,
        #[diesel(sql_type = Nullable<Text>)]
        legal_hold_reason: Option<String>,
    }
    let row = diesel::sql_query(
        "SELECT legal_hold_set_at, legal_hold_until, legal_hold_reason
         FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result::<HoldRow>(conn)
    .await
    .expect("load hold columns");
    (
        row.legal_hold_set_at,
        row.legal_hold_until,
        row.legal_hold_reason,
    )
}

fn history_only(max_age: Option<Duration>) -> RetentionConfig {
    RetentionConfig {
        max_age_secs: max_age.map(|d| d.as_secs()),
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        ..RetentionConfig::default()
    }
}

// A held execution survives past its retention age, and the deletion metric
// reports ZERO for the held id.
#[tokio::test]
async fn held_execution_survives_and_metric_is_zero_for_it() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);

    // Held (indefinite): completed 2 days ago, older than the 1-day global.
    insert_completed(&mut conn, "held_wf", "h1", two_days_ago, Some(now), None).await;
    // Unheld control at the same age -> deleted.
    insert_completed(&mut conn, "plain_wf", "p1", two_days_ago, None, None).await;

    let config = history_only(Some(Duration::from_secs(86_400))); // global 1 day
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    assert_eq!(
        surviving_names(&mut conn).await,
        vec!["held_wf".to_string()],
        "the held execution survives; the unheld one is deleted"
    );
    assert_eq!(result.deleted_count, 1, "only the unheld execution deleted");

    let deleted = metrics.deleted();
    assert!(
        !deleted.iter().any(|(name, _)| name == "held_wf"),
        "harvest.retention.deleted must NEVER count the held id; got {deleted:?}"
    );
    assert_eq!(deleted, vec![("plain_wf".to_string(), 1)]);
    assert!(!result.deleted_by_workflow.contains_key("held_wf"));
}

// An expired hold becomes eligible again and is deleted on the next tick.
#[tokio::test]
async fn expired_hold_becomes_eligible_and_is_deleted() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);
    let one_hour_ago = now - chrono::Duration::hours(1);

    // Hold placed but auto-expired an hour ago -> inactive -> eligible.
    insert_completed(
        &mut conn,
        "expired_wf",
        "e1",
        two_days_ago,
        Some(two_days_ago),
        Some(one_hour_ago),
    )
    .await;

    let config = history_only(Some(Duration::from_secs(86_400)));
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    assert!(
        surviving_names(&mut conn).await.is_empty(),
        "an execution whose hold has expired is no longer exempt and is deleted"
    );
    assert_eq!(result.deleted_count, 1);
    assert_eq!(metrics.deleted(), vec![("expired_wf".to_string(), 1)]);
}

// A hold trumps both the global age and a per-type override.
#[tokio::test]
async fn hold_trumps_global_and_override() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);

    // Held row for a type carrying a SHORT (1-hour) override — the override
    // alone would delete it at 2 days old, but the hold wins.
    insert_completed(&mut conn, "short_wf", "s1", two_days_ago, Some(now), None).await;
    // Unheld sibling of the same type -> deleted by the short override.
    insert_completed(&mut conn, "short_wf", "s2", two_days_ago, None, None).await;

    let config = history_only(Some(Duration::from_secs(86_400)))
        .with_workflow_override("short_wf", Duration::from_secs(3_600));
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    assert_eq!(
        surviving_names(&mut conn).await,
        vec!["short_wf".to_string()],
        "the held short_wf row survives despite its 1-hour override"
    );
    assert_eq!(result.deleted_count, 1, "only the unheld sibling deleted");
    assert_eq!(metrics.deleted(), vec![("short_wf".to_string(), 1)]);
}

// erase_workflow_payloads rejects a held execution with a 409-mapped
// HarvestError::Config naming the hold, and tombstones nothing.
#[tokio::test]
async fn erase_rejected_while_held() {
    let (url, _container) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let id = insert_completed(
        &mut conn,
        "erase_wf",
        "x1",
        now - chrono::Duration::days(1),
        Some(now),
        None,
    )
    .await;
    let exec_id = ExecutionId::from_uuid(id);

    let err = autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "gdpr")
        .await
        .expect_err("erase must be rejected while held");
    let msg = err.to_string();
    assert!(
        msg.contains("legal hold"),
        "error must name the legal hold; got: {msg}"
    );
    assert_eq!(
        tombstoned_event_count(&mut conn, id).await,
        0,
        "no events may be tombstoned when erase is rejected"
    );

    // After releasing, erase succeeds and DOES tombstone.
    autumn_harvest::release_legal_hold(&mut conn, exec_id, Utc::now())
        .await
        .expect("release");
    autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "gdpr")
        .await
        .expect("erase succeeds after release");
    assert_eq!(
        tombstoned_event_count(&mut conn, id).await,
        1,
        "the input field is tombstoned once the hold is gone"
    );
}

// set_legal_hold / release_legal_hold idempotency round-trip.
#[tokio::test]
async fn set_release_idempotency_round_trip() {
    let (url, _container) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let id = insert_completed(
        &mut conn,
        "idem_wf",
        "i1",
        now - chrono::Duration::days(1),
        None,
        None,
    )
    .await;
    let exec_id = ExecutionId::from_uuid(id);

    // First set places the hold.
    let first = autumn_harvest::set_legal_hold(&mut conn, exec_id, "case-1", None, "alice", now)
        .await
        .expect("set");
    assert!(first.held && first.newly_held);
    assert_eq!(first.legal_hold_reason.as_deref(), Some("case-1"));
    assert_eq!(first.legal_hold_actor.as_deref(), Some("alice"));

    // Re-set is a no-op: provenance preserved, newly_held=false.
    let again = autumn_harvest::set_legal_hold(&mut conn, exec_id, "case-2", None, "bob", now)
        .await
        .expect("re-set");
    assert!(again.held && !again.newly_held);
    assert_eq!(
        again.legal_hold_reason.as_deref(),
        Some("case-1"),
        "re-hold must NOT overwrite the original reason"
    );
    assert_eq!(again.legal_hold_actor.as_deref(), Some("alice"));

    // Release clears it.
    let rel = autumn_harvest::release_legal_hold(&mut conn, exec_id, now)
        .await
        .expect("release");
    assert!(!rel.held && rel.released);

    // Re-release is a 200 no-op.
    let rel2 = autumn_harvest::release_legal_hold(&mut conn, exec_id, now)
        .await
        .expect("re-release");
    assert!(!rel2.held && !rel2.released);

    // Unknown execution -> NotFound (404).
    let missing = ExecutionId::new();
    assert!(
        autumn_harvest::set_legal_hold(&mut conn, missing, "r", None, "a", now)
            .await
            .is_err(),
        "set on an unknown execution must error"
    );
}

// ── MINOR 2b: held child is SKIPPED (not failed) during cascade ───────────────

// During cascade erasure, a child under an active legal hold is reported as a
// SKIPPED child (with a legal-hold reason), NOT a failure; its events are left
// intact while the parent's are tombstoned.
#[tokio::test]
async fn held_child_is_skipped_not_failed_during_cascade() {
    let (url, _container) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let ago = now - chrono::Duration::days(1);

    // Parent (terminal, unheld) with a PII event.
    let parent_id = insert_completed(&mut conn, "parent_wf", "pw-child", ago, None, None).await;

    // Child (terminal) UNDER a legal hold, parented to the above, with its own
    // PII event.
    let child_id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
             parent_id, legal_hold_set_at, legal_hold_until, legal_hold_reason, legal_hold_actor)
         VALUES ('child_wf', 'cw-held', 0, 'COMPLETED', '{}'::jsonb, $1, $1,
                 $2, $3, NULL, 'child subpoena', 'legal')
         RETURNING id",
    )
    .bind::<Timestamptz, _>(ago)
    .bind::<diesel::sql_types::Uuid, _>(parent_id)
    .bind::<Timestamptz, _>(now)
    .get_result::<IdRow>(&mut conn)
    .await
    .expect("insert child")
    .id;
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{\"pii\":\"childsecret\"}}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(child_id)
    .execute(&mut conn)
    .await
    .expect("insert child event");

    let parent_exec = ExecutionId::from_uuid(parent_id);
    let outcome = autumn_harvest::erase::erase_workflow_payloads(&mut conn, parent_exec, "gdpr")
        .await
        .expect("parent erase succeeds even with a held child");

    // The held child is a SKIP, not a failure.
    assert!(
        outcome.failures.is_empty(),
        "a held child must not be a failure; got {:?}",
        outcome.failures
    );
    assert_eq!(
        outcome.skipped_children.len(),
        1,
        "the held child is reported as skipped"
    );
    let skipped = &outcome.skipped_children[0];
    assert_eq!(
        skipped.execution_id,
        ExecutionId::from_uuid(child_id).to_string()
    );
    let reason = skipped.reason.as_deref().unwrap_or_default();
    assert!(
        reason.contains("legal hold") && reason.contains("child subpoena"),
        "skip reason names the legal hold; got: {reason:?}"
    );

    // The parent's events ARE tombstoned; the held child's are NOT.
    assert_eq!(
        tombstoned_event_count(&mut conn, parent_id).await,
        1,
        "the parent's input is tombstoned"
    );
    assert_eq!(
        tombstoned_event_count(&mut conn, child_id).await,
        0,
        "the held child's events are left intact"
    );
}

// ── BLOCKER 1: delete-time hold TOCTOU ────────────────────────────────────────

// A hold placed AFTER candidate selection but before the delete transaction
// (injected here from inside archive()) must be honored by the delete-tx
// FOR UPDATE re-check: the row SURVIVES and the deletion metric reads zero.
#[tokio::test]
async fn hold_placed_mid_tick_is_not_deleted() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);

    // An UNHELD candidate at selection time — the hold is placed mid-tick.
    let mid_id = insert_completed(&mut conn, "mid_wf", "m1", two_days_ago, None, None).await;

    let archiver = Arc::new(RaceArchiver {
        url: url.clone(),
        archived: Mutex::new(Vec::new()),
        place_hold_on: Some(mid_id),
    });
    let config = history_only(Some(Duration::from_secs(86_400))); // global 1 day
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick_with_archiver(
        pool,
        config,
        Arc::clone(&metrics),
        archiver.clone() as Arc<dyn autumn_harvest::HistoryArchiver>,
    )
    .await;

    // The row survives: the delete-tx FOR UPDATE re-check saw the mid-tick hold.
    assert_eq!(
        surviving_names(&mut conn).await,
        vec!["mid_wf".to_string()],
        "a hold placed mid-tick must prevent deletion (delete-tx re-check)"
    );
    assert_eq!(
        result.deleted_count, 0,
        "deleted_count must EXCLUDE the mid-tick-held execution"
    );
    // The metric must read zero for the held id.
    assert!(
        metrics.deleted().is_empty(),
        "harvest.retention.deleted must be zero for a mid-tick-held id; got {:?}",
        metrics.deleted()
    );
    assert!(!result.deleted_by_workflow.contains_key("mid_wf"));
    // The row genuinely carries the hold now.
    let (set_at, _until, reason) = hold_columns(&mut conn, mid_id).await;
    assert!(set_at.is_some(), "the mid-tick hold is durably present");
    assert_eq!(reason.as_deref(), Some("mid-tick hold"));
    // The archiver did run for this candidate (proving the hold landed AFTER
    // selection/archival, exercising the delete-tx re-check, not the earlier
    // gate).
    assert_eq!(archiver.archived_ids(), vec![mid_id]);
}

// AC3 archival half: a held row must NOT have the #345 archival hook fired.
#[tokio::test]
async fn held_row_is_not_archived() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);

    // Held (indefinite) old row + unheld old sibling.
    let held_id =
        insert_completed(&mut conn, "held_arch", "ha1", two_days_ago, Some(now), None).await;
    let plain_id = insert_completed(&mut conn, "plain_arch", "pa1", two_days_ago, None, None).await;

    let archiver = Arc::new(RaceArchiver {
        url: url.clone(),
        archived: Mutex::new(Vec::new()),
        place_hold_on: None,
    });
    let config = history_only(Some(Duration::from_secs(86_400)));
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick_with_archiver(
        pool,
        config,
        Arc::clone(&metrics),
        archiver.clone() as Arc<dyn autumn_harvest::HistoryArchiver>,
    )
    .await;

    // Only the unheld sibling was archived; the held id was never shipped to
    // cold storage.
    let shipped_ids = archiver.archived_ids();
    assert!(
        shipped_ids.contains(&plain_id),
        "the unheld sibling must be archived"
    );
    assert!(
        !shipped_ids.contains(&held_id),
        "the held execution must NEVER be archived; got {shipped_ids:?}"
    );

    assert_eq!(
        surviving_names(&mut conn).await,
        vec!["held_arch".to_string()],
        "the held row survives; the unheld one is archived then deleted"
    );
    assert_eq!(result.deleted_count, 1);
    assert!(!result.deleted_by_workflow.contains_key("held_arch"));
    assert!(!metrics.deleted().iter().any(|(n, _)| n == "held_arch"));
}

// ── Core: past hold_until, overwrite, running, release, second-tick ───────────

// MAJOR (core layer): a hold_until already in the past writes the columns but
// reports held:false (no false protection for a direct core/CLI caller).
#[tokio::test]
async fn set_legal_hold_with_past_until_reports_inactive() {
    let (url, _container) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let id = insert_completed(
        &mut conn,
        "past_wf",
        "pw1",
        now - chrono::Duration::days(1),
        None,
        None,
    )
    .await;
    let exec_id = ExecutionId::from_uuid(id);

    let past = now - chrono::Duration::hours(1);
    let out = autumn_harvest::set_legal_hold(&mut conn, exec_id, "case-x", Some(past), "op", now)
        .await
        .expect("set");
    assert!(
        !out.held,
        "a past hold_until must report held:false (no false protection)"
    );
    assert!(!out.newly_held, "no active hold was placed");
}

// Expired-hold overwrite: set past hold_until, then re-set future hold_until —
// provenance is REPLACED and newly_held == true.
#[tokio::test]
async fn expired_hold_is_overwritten_by_a_fresh_set() {
    let (url, _container) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let id = insert_completed(
        &mut conn,
        "ovr_wf",
        "ow1",
        now - chrono::Duration::days(1),
        None,
        None,
    )
    .await;
    let exec_id = ExecutionId::from_uuid(id);

    // First: an already-expired hold.
    let past = now - chrono::Duration::hours(1);
    let first =
        autumn_harvest::set_legal_hold(&mut conn, exec_id, "old-reason", Some(past), "alice", now)
            .await
            .expect("first set");
    assert!(!first.held, "the expired hold is inactive");

    // Re-set with a FUTURE until: provenance replaced (expired hold does not
    // block the overwrite), newly_held == true.
    let future = now + chrono::Duration::days(3);
    let second =
        autumn_harvest::set_legal_hold(&mut conn, exec_id, "new-reason", Some(future), "bob", now)
            .await
            .expect("second set");
    assert!(second.held && second.newly_held, "fresh active hold placed");
    assert_eq!(second.legal_hold_reason.as_deref(), Some("new-reason"));
    assert_eq!(second.legal_hold_actor.as_deref(), Some("bob"));

    let (_set_at, until, reason) = hold_columns(&mut conn, id).await;
    assert_eq!(reason.as_deref(), Some("new-reason"), "reason replaced");
    assert!(until.is_some(), "future until persisted");
}

// Running-hold: a hold on a RUNNING (non-terminal) execution persists and is
// untouched by a retention tick (RUNNING is not a retention candidate).
#[tokio::test]
async fn hold_on_running_execution_persists_and_is_untouched_by_tick() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    // Insert a RUNNING execution (no completed_at).
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at)
         VALUES ('run_wf', 'rw1', 0, 'RUNNING', '{}'::jsonb, $1)
         RETURNING id",
    )
    .bind::<Timestamptz, _>(now - chrono::Duration::days(2))
    .get_result::<IdRow>(&mut conn)
    .await
    .expect("insert running")
    .id;
    let exec_id = ExecutionId::from_uuid(id);

    let out = autumn_harvest::set_legal_hold(&mut conn, exec_id, "running-hold", None, "op", now)
        .await
        .expect("set");
    assert!(out.held && out.newly_held);

    // A tick with a tiny global age never selects a RUNNING row.
    let config = history_only(Some(Duration::from_secs(1)));
    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;
    assert_eq!(result.deleted_count, 0);

    assert_eq!(
        surviving_names(&mut conn).await,
        vec!["run_wf".to_string()],
        "the RUNNING held execution is untouched"
    );
    let (set_at, _until, reason) = hold_columns(&mut conn, id).await;
    assert!(set_at.is_some());
    assert_eq!(reason.as_deref(), Some("running-hold"));
}

// Release-of-expired: releasing an expired-but-not-cleared hold NULLs the
// columns and reports released == true.
#[tokio::test]
async fn release_of_expired_hold_clears_columns() {
    let (url, _container) = setup_db().await;
    let _pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let one_hour_ago = now - chrono::Duration::hours(1);
    // Seed an execution whose hold has already expired (set_at + past until).
    let id = insert_completed(
        &mut conn,
        "relexp_wf",
        "re1",
        now - chrono::Duration::days(1),
        Some(now - chrono::Duration::hours(2)),
        Some(one_hour_ago),
    )
    .await;
    let exec_id = ExecutionId::from_uuid(id);

    let out = autumn_harvest::release_legal_hold(&mut conn, exec_id, now)
        .await
        .expect("release");
    assert!(
        out.released,
        "releasing an expired-but-not-cleared hold reports released:true"
    );
    assert!(!out.held);

    let (set_at, until, reason) = hold_columns(&mut conn, id).await;
    assert!(
        set_at.is_none() && until.is_none() && reason.is_none(),
        "all legal_hold columns are NULLed on release"
    );
}

// Second-tick metric-zero: an indefinitely-held old row survives TWO ticks and
// the deletion metric reads zero across both (the success metric's ">= 1 full
// cycle beyond max_age" spirit).
#[tokio::test]
async fn indefinitely_held_row_survives_two_ticks_metric_zero() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);
    insert_completed(
        &mut conn,
        "twotick_wf",
        "tt1",
        two_days_ago,
        Some(now),
        None,
    )
    .await;

    let metrics = Arc::new(CapturingMetrics::default());
    for pass in 1..=2 {
        let config = history_only(Some(Duration::from_secs(86_400)));
        let result = run_one_tick(pool.clone(), config, Arc::clone(&metrics)).await;
        assert_eq!(result.deleted_count, 0, "held row must survive tick {pass}");
        assert_eq!(
            surviving_names(&mut conn).await,
            vec!["twotick_wf".to_string()],
            "held row still present after tick {pass}"
        );
    }
    assert!(
        metrics.deleted().is_empty(),
        "harvest.retention.deleted must be zero across BOTH ticks; got {:?}",
        metrics.deleted()
    );
}
