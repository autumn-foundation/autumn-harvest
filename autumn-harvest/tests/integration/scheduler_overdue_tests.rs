#![cfg(feature = "db")]
//! Overdue-schedule detection tests — issue #696.
//!
//! Server-side detection that an *active* schedule is overdue to fire relative
//! to its own cadence (the scheduler loop stalled, `next_run_at` wedged in the
//! past, an HA claim that never released, or all scheduler replicas down).
//!
//! These tests drive the per-shard sampler function
//! (`scheduler::sample_overdue_schedules`) against a real Postgres container
//! and assert the `harvest.schedule.overdue` gauge is emitted correctly. AC3
//! (intentionally-not-firing states) and the at-capacity false-positive guard
//! are exercised in the DB path here; the pure predicate (`schedule_overdue`)
//! is unit-tested without a database in `scheduler.rs`.

use std::sync::Mutex;

use autumn_harvest::schema::harvest_schedules;
use autumn_harvest::scheduler::{overdue_schedule_samples, sample_overdue_schedules};
use autumn_harvest::telemetry::{METRIC_SCHEDULE_OVERDUE, MetricsRecorder};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── Recording metrics ───────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct RecordingMetrics {
    /// (kind, name, overdue) tuples in emission order.
    overdue: Mutex<Vec<(String, String, bool)>>,
}

impl RecordingMetrics {
    fn samples(&self) -> Vec<(String, String, bool)> {
        self.overdue.lock().unwrap().clone()
    }

    /// The recorded `overdue` flag for a `(kind, name)`, or `None` if not emitted.
    fn overdue_for(&self, kind: &str, name: &str) -> Option<bool> {
        self.overdue
            .lock()
            .unwrap()
            .iter()
            .find(|(k, n, _)| k == kind && n == name)
            .map(|(_, _, o)| *o)
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_schedule_overdue(&self, kind: &str, name: &str, overdue: bool) {
        self.overdue
            .lock()
            .unwrap()
            .push((kind.to_owned(), name.to_owned(), overdue));
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

async fn setup_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("migration");
    (conn, container)
}

/// Insert a workflow schedule with an explicit `next_run_at` and AC3 flags.
#[allow(clippy::too_many_arguments)]
async fn insert_schedule(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    schedule_expr: &str,
    next_run_at: DateTime<Utc>,
    is_paused: bool,
    exhausted_at: Option<DateTime<Utc>>,
    max_active_runs: i32,
) -> Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq(schedule_expr),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(max_active_runs),
            dsl::is_paused.eq(is_paused),
            dsl::next_run_at.eq(next_run_at),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
            dsl::exhausted_at.eq(exhausted_at),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

/// Insert one RUNNING execution row for `wf_name` (drives the at-capacity guard).
async fn insert_running_execution(conn: &mut AsyncPgConnection, wf_name: &str) {
    let sql = format!(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at) \
         VALUES (gen_random_uuid(), '{wf_name}', '{wf_name}-run', gen_random_uuid(), 0, 'RUNNING', '{{}}'::jsonb, 'default', now(), now())"
    );
    conn.batch_execute(&sql).await.expect("insert execution");
}

// ── Unit markers ─────────────────────────────────────────────────────────────

#[test]
fn metric_constant_schedule_overdue_is_defined() {
    assert_eq!(METRIC_SCHEDULE_OVERDUE, "harvest.schedule.overdue");
}

// ── Integration tests ────────────────────────────────────────────────────────

/// AC6 / success metric: a schedule whose `next_run_at` is held in the past
/// (no scheduler progress) is reported overdue.
#[tokio::test]
async fn wedged_schedule_is_reported_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // interval:60 => grace = 60 + 0 + 1 = 61s. 200s past its slot => overdue.
    insert_schedule(
        &mut conn,
        "wedged_wf",
        "interval:60",
        now - chrono::Duration::seconds(200),
        false,
        None,
        10,
    )
    .await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "wedged_wf"),
        Some(true),
        "a wedged schedule must be reported overdue; samples: {:?}",
        metrics.samples()
    );
}

/// AC3: intentionally-not-firing states are never reported overdue, and the
/// gauge is still emitted (0) for them so it stays fresh.
#[tokio::test]
async fn healthy_paused_exhausted_are_not_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // Healthy: just fired, well within grace.
    insert_schedule(
        &mut conn,
        "healthy_wf",
        "interval:60",
        now - chrono::Duration::seconds(10),
        false,
        None,
        10,
    )
    .await;
    // Paused but with a stale next_run_at (would be overdue if active).
    insert_schedule(
        &mut conn,
        "paused_wf",
        "interval:60",
        now - chrono::Duration::seconds(5000),
        true,
        None,
        10,
    )
    .await;
    // Exhausted (#478/#543): intentionally done, never overdue.
    insert_schedule(
        &mut conn,
        "exhausted_wf",
        "interval:60",
        now - chrono::Duration::seconds(5000),
        false,
        Some(now),
        10,
    )
    .await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(metrics.overdue_for("workflow", "healthy_wf"), Some(false));
    assert_eq!(metrics.overdue_for("workflow", "paused_wf"), Some(false));
    assert_eq!(metrics.overdue_for("workflow", "exhausted_wf"), Some(false));
}

/// §2 false-positive guard: a schedule the tick deliberately holds in the past
/// because it is at `max_active_runs` (a run is in flight) is NOT overdue.
#[tokio::test]
async fn at_capacity_schedule_is_not_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // Stale next_run_at that WOULD be overdue if the schedule were free.
    insert_schedule(
        &mut conn,
        "busy_wf",
        "interval:60",
        now - chrono::Duration::seconds(5000),
        false,
        None,
        1, // max_active_runs = 1
    )
    .await;
    // One RUNNING execution => running (1) >= max_active_runs (1) => at capacity.
    insert_running_execution(&mut conn, "busy_wf").await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "busy_wf"),
        Some(false),
        "an at-capacity schedule holds next_run_at deliberately; not a wedge"
    );
}

/// The returning helper surfaces `overdue_by_secs` = now - next_run_at.
#[tokio::test]
async fn overdue_schedule_samples_reports_lag() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    insert_schedule(
        &mut conn,
        "lag_wf",
        "interval:60",
        now - chrono::Duration::seconds(300),
        false,
        None,
        10,
    )
    .await;

    let samples = overdue_schedule_samples(&mut conn, now)
        .await
        .expect("samples");
    let s = samples
        .iter()
        .find(|s| s.name == "lag_wf")
        .expect("lag_wf sample present");
    assert!(s.overdue, "lag_wf must be overdue");
    // now - next_run_at == 300 (allowing a small clock-read slack).
    let by = s.overdue_by_secs.expect("overdue_by_secs set");
    assert!(
        (299..=301).contains(&by),
        "overdue_by_secs should be ~300 (now - next_run_at), got {by}"
    );
}
