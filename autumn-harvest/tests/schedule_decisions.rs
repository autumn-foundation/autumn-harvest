#![cfg(feature = "db")]

use autumn_harvest::models::ScheduleDecision;
use autumn_harvest::schedule_decision::record_decision_graceful;
use autumn_harvest::schema::harvest_schedule_decisions;
use autumn_harvest::telemetry::MetricsRecorder;

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use std::sync::{Arc, Mutex};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260514010000_unified_dag_schedule_kind/up.sql"),
    "\n",
    include_str!("../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql")
);

#[derive(Debug, Default, Clone)]
struct TestMetrics {
    write_failed_count: Arc<Mutex<u64>>,
}

impl MetricsRecorder for TestMetrics {
    fn record_schedule_decision_write_failed(&self) {
        let mut count = self.write_failed_count.lock().unwrap();
        *count += 1;
    }
}

async fn make_conn() -> (
    diesel_async::AsyncPgConnection,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default().start().await.expect("postgres start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let mut conn = diesel_async::AsyncPgConnection::establish(&url)
        .await
        .expect("connect");
    conn.batch_execute(INIT_SQL).await.expect("migration");
    (conn, container)
}

#[tokio::test]
async fn test_record_decision_graceful_success() {
    let (mut conn, _c) = make_conn().await;
    let metrics = TestMetrics::default();

    let schedule_id = None;
    let next_fire_at = Utc::now() + chrono::Duration::hours(1);
    let occurred_at = Utc::now();

    record_decision_graceful(
        &mut conn,
        Some(&metrics),
        schedule_id,
        "test-schedule-success",
        "workflow",
        "fired",
        "fired_ok",
        Some(serde_json::json!({ "run_id": "exec-123" })),
        occurred_at,
        next_fire_at,
        0,
    )
    .await;

    // Verify metric did not increment
    assert_eq!(*metrics.write_failed_count.lock().unwrap(), 0);

    // Read the database and verify the entry was written
    let decisions = harvest_schedule_decisions::table
        .load::<ScheduleDecision>(&mut conn)
        .await
        .expect("load decisions");

    assert_eq!(decisions.len(), 1);
    let dec = &decisions[0];
    assert_eq!(dec.schedule_id, schedule_id);
    assert_eq!(dec.schedule_name, "test-schedule-success");
    assert_eq!(dec.target_kind, "workflow");
    assert_eq!(dec.decision, "fired");
    assert_eq!(dec.reason_code, "fired_ok");
    assert_eq!(
        dec.detail.as_ref().unwrap(),
        &serde_json::json!({ "run_id": "exec-123" })
    );
    assert_eq!(dec.shard_id, 0);
}

#[tokio::test]
async fn test_record_decision_graceful_failure() {
    let (mut conn, container) = make_conn().await;

    // Stop the database container to ensure database write fails
    drop(container);

    let metrics = TestMetrics::default();
    let next_fire_at = Utc::now() + chrono::Duration::hours(1);
    let occurred_at = Utc::now();

    // Call record_decision_graceful. It should catch the DB error, log it,
    // and record the metrics without crashing or returning an error.
    record_decision_graceful(
        &mut conn,
        Some(&metrics),
        None,
        "test-schedule-failure",
        "dag",
        "skipped",
        "dag_not_registered",
        None,
        occurred_at,
        next_fire_at,
        1,
    )
    .await;

    // Verify metric incremented exactly once
    assert_eq!(*metrics.write_failed_count.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_purge_old_schedule_decisions() {
    let (mut conn, _c) = make_conn().await;
    let metrics = TestMetrics::default();

    // 1. Record an old decision (e.g. 10 days ago)
    let ten_days_ago = Utc::now() - chrono::Duration::days(10);
    record_decision_graceful(
        &mut conn,
        Some(&metrics),
        None,
        "old-schedule",
        "workflow",
        "skipped",
        "calendar",
        None,
        ten_days_ago,
        ten_days_ago,
        0,
    )
    .await;

    // 2. Record a new decision (e.g. now)
    let now = Utc::now();
    record_decision_graceful(
        &mut conn,
        Some(&metrics),
        None,
        "new-schedule",
        "workflow",
        "fired",
        "fired_ok",
        None,
        now,
        now,
        0,
    )
    .await;

    // Verify both are present
    let count: i64 = harvest_schedule_decisions::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(count, 2);

    // 3. Purge older than 7 days
    let purged = autumn_harvest::schedule_decision::purge_old_schedule_decisions(&mut conn, 7)
        .await
        .expect("purge");
    assert_eq!(purged, 1);

    // 4. Verify only the new decision remains
    let remaining = harvest_schedule_decisions::table
        .load::<ScheduleDecision>(&mut conn)
        .await
        .expect("load remaining");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].schedule_name, "new-schedule");
}
