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
//! first); otherwise a fresh testcontainers Postgres is booted with `INIT_SQL`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::retention::{RetentionConfig, RetentionRuntime};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, Utc};
use diesel::sql_types::{Nullable, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../../migrations/20260504000000_harvest_workflow_parent_children/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
);

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
    let mut cfg = RetentionConfig {
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        ..RetentionConfig::default()
    };
    cfg.max_age_secs = max_age.map(|d| d.as_secs());
    cfg
}

// AC3 + AC4 + AC8: a longer override survives the global age; a shorter
// override is deleted at its own earlier age; the metric labels the deleted
// type.
#[tokio::test]
async fn override_longer_survives_shorter_deleted() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let now = Utc::now();
    // Both completed 2 days ago.
    let two_days_ago = now - chrono::Duration::days(2);
    // "long_wf": override 30 days (longer than the 1-day global) -> survives.
    insert_completed(&mut conn, "long_wf", "l1", two_days_ago).await;
    // "short_wf": override 1 hour (shorter than the 1-day global) -> deleted.
    insert_completed(&mut conn, "short_wf", "s1", two_days_ago).await;
    // "default_wf": no override, uses the 1-day global -> deleted.
    insert_completed(&mut conn, "default_wf", "d1", two_days_ago).await;

    let config = history_only(Some(Duration::from_secs(86_400))) // global 1 day
        .with_workflow_override("long_wf", Duration::from_secs(30 * 86_400))
        .with_workflow_override("short_wf", Duration::from_secs(3_600));

    let metrics = Arc::new(CapturingMetrics::default());
    let result = run_one_tick(pool, config, Arc::clone(&metrics)).await;

    let survivors = surviving_names(&mut conn).await;
    assert_eq!(
        survivors,
        vec!["long_wf".to_string()],
        "only long_wf (30-day override) should survive the 1-day global cutoff"
    );
    assert_eq!(result.deleted_count, 2, "short_wf + default_wf deleted");

    // AC8: metric labeled per workflow, real deletes only.
    let mut deleted = metrics.deleted();
    deleted.sort();
    assert_eq!(
        deleted,
        vec![("default_wf".to_string(), 1), ("short_wf".to_string(), 1)],
        "per-type deletion metric must name exactly the deleted types"
    );
    // Per-type reporting on the tick result (AC7 reporting surface).
    assert_eq!(result.deleted_by_workflow.get("short_wf"), Some(&1));
    assert_eq!(result.deleted_by_workflow.get("default_wf"), Some(&1));
    assert!(result.deleted_by_workflow.get("long_wf").is_none());
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
    insert_completed(&mut conn, "compat_old", "o1", now - chrono::Duration::days(2)).await;
    insert_completed(&mut conn, "compat_young", "y1", now - chrono::Duration::minutes(5)).await;

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
}
