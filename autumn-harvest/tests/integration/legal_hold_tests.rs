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
    include_str!("../../migrations/20260709000000_harvest_legal_hold/up.sql"),
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
         VALUES ($1, 0, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{\"pii\":\"x\"}}}'::jsonb)",
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
