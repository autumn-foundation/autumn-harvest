#![cfg(feature = "db")]
#![allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::doc_markdown,
    clippy::cast_possible_wrap
)]
//! Per-schedule run-history read helpers + origin attribution — issue #534.
//!
//! Verifies that `start_or_load_workflow_execution` persists the dispatch
//! `origin`, that `list_schedule_runs` returns a schedule's runs newest-first
//! with state/origin filtering and keyset pagination, and that
//! `schedule_run_state_summary` counts only `scheduled`-origin runs (so a
//! backfill or manual fire never inflates the cadence failure ratio).

use autumn_harvest::execution::{
    ORIGIN_BACKFILL, ORIGIN_MANUAL_TRIGGER, ORIGIN_SCHEDULED, ScheduleRunQuery, list_schedule_runs,
    schedule_run_state_summary,
};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::{
    StartWorkflowParams, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
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
    "\n",
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    "\n",
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    "\n",
    include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../migrations/20260611000001_harvest_stalled_workflow_index/up.sql"),
    "\n",
    include_str!("../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../migrations/20260704000000_harvest_build_policy_ramp/up.sql"),
    include_str!("../migrations/20260704000000_harvest_workflow_nd_block/up.sql")
);

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
    conn.batch_execute(INIT_SQL).await.expect("migrate");
    (conn, container)
}

/// Start one run attributed to `schedule_id` with the given origin/slot, then
/// force it into `state` at `started_at` for deterministic ordering and summary.
#[allow(clippy::too_many_arguments)]
async fn seed_run(
    conn: &mut AsyncPgConnection,
    schedule_id: Uuid,
    workflow_id: &str,
    origin: Option<&str>,
    scheduled_for: Option<DateTime<Utc>>,
    state: &str,
    started_at: DateTime<Utc>,
) -> Uuid {
    use harvest_workflow_executions::dsl;

    let exec_id = ExecutionId::new();
    let started = start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "nightly_etl",
            workflow_id,
            exec_id,
            input: json!(null),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: autumn_harvest::types::Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: Some(schedule_id),
            scheduled_for,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin,
        },
    )
    .await
    .expect("start");

    let id = started.exec_id.as_uuid();
    let completed_at = if state == "RUNNING" {
        None
    } else {
        Some(started_at + Duration::seconds(5))
    };
    diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(id)))
        .set((
            dsl::state.eq(state),
            dsl::started_at.eq(started_at),
            dsl::completed_at.eq(completed_at),
        ))
        .execute(conn)
        .await
        .expect("force state");
    id
}

#[tokio::test]
async fn origin_is_persisted_for_each_dispatch_source() {
    let (mut conn, _c) = setup_db().await;
    let sid = Uuid::new_v4();
    let slot = Utc::now() - Duration::hours(1);

    seed_run(
        &mut conn,
        sid,
        "sched-1",
        Some(ORIGIN_SCHEDULED),
        Some(slot),
        "COMPLETED",
        slot,
    )
    .await;
    seed_run(
        &mut conn,
        sid,
        "backfill-1",
        Some(ORIGIN_BACKFILL),
        Some(slot - Duration::hours(2)),
        "COMPLETED",
        slot - Duration::minutes(30),
    )
    .await;
    seed_run(
        &mut conn,
        sid,
        "manual-1",
        Some(ORIGIN_MANUAL_TRIGGER),
        None,
        "FAILED",
        slot - Duration::minutes(10),
    )
    .await;

    let runs = list_schedule_runs(
        &mut conn,
        sid,
        0,
        &ScheduleRunQuery {
            limit: 100,
            ..Default::default()
        },
    )
    .await
    .expect("list");

    assert_eq!(runs.len(), 3, "all three attributed runs are returned");
    let origins: std::collections::HashSet<_> =
        runs.iter().filter_map(|r| r.origin.clone()).collect();
    assert!(origins.contains(ORIGIN_SCHEDULED));
    assert!(origins.contains(ORIGIN_BACKFILL));
    assert!(origins.contains(ORIGIN_MANUAL_TRIGGER));

    // The manual_trigger run is attributed but carries no nominal slot.
    let manual = runs
        .iter()
        .find(|r| r.origin.as_deref() == Some(ORIGIN_MANUAL_TRIGGER))
        .unwrap();
    assert!(
        manual.nominal_fire_time.is_none(),
        "manual fire has no slot"
    );
}

#[tokio::test]
async fn list_orders_newest_first_and_filters_by_state() {
    let (mut conn, _c) = setup_db().await;
    let sid = Uuid::new_v4();
    let base = Utc::now() - Duration::hours(5);

    seed_run(
        &mut conn,
        sid,
        "r1",
        Some(ORIGIN_SCHEDULED),
        Some(base),
        "COMPLETED",
        base,
    )
    .await;
    seed_run(
        &mut conn,
        sid,
        "r2",
        Some(ORIGIN_SCHEDULED),
        Some(base + Duration::hours(1)),
        "FAILED",
        base + Duration::hours(1),
    )
    .await;
    seed_run(
        &mut conn,
        sid,
        "r3",
        Some(ORIGIN_SCHEDULED),
        Some(base + Duration::hours(2)),
        "COMPLETED",
        base + Duration::hours(2),
    )
    .await;

    let all = list_schedule_runs(
        &mut conn,
        sid,
        0,
        &ScheduleRunQuery {
            limit: 100,
            ..Default::default()
        },
    )
    .await
    .expect("list");
    let order: Vec<_> = all.iter().map(|r| r.started_at).collect();
    assert!(order.windows(2).all(|w| w[0] >= w[1]), "newest-first");

    let failed = list_schedule_runs(
        &mut conn,
        sid,
        0,
        &ScheduleRunQuery {
            states: vec!["FAILED".to_string()],
            limit: 100,
            ..Default::default()
        },
    )
    .await
    .expect("list failed");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].state, "FAILED");
}

#[tokio::test]
async fn keyset_cursor_paginates_without_overlap() {
    let (mut conn, _c) = setup_db().await;
    let sid = Uuid::new_v4();
    let base = Utc::now() - Duration::hours(10);

    for i in 0..5 {
        seed_run(
            &mut conn,
            sid,
            &format!("p{i}"),
            Some(ORIGIN_SCHEDULED),
            Some(base + Duration::minutes(i)),
            "COMPLETED",
            base + Duration::minutes(i),
        )
        .await;
    }

    // Page 1: limit 2 (+1 to detect more).
    let page1 = list_schedule_runs(
        &mut conn,
        sid,
        0,
        &ScheduleRunQuery {
            limit: 2,
            ..Default::default()
        },
    )
    .await
    .expect("page1");
    assert_eq!(page1.len(), 3, "fetched limit+1");
    let last_keep = &page1[1]; // operator-facing limit is 2
    let cursor = Some((last_keep.started_at, last_keep.execution_id));

    let page2 = list_schedule_runs(
        &mut conn,
        sid,
        0,
        &ScheduleRunQuery {
            cursor,
            limit: 100,
            ..Default::default()
        },
    )
    .await
    .expect("page2");
    // Every page-2 row is strictly older than the cursor row — no overlap.
    for row in &page2 {
        assert!(
            (row.started_at, row.execution_id) < (last_keep.started_at, last_keep.execution_id)
        );
    }
}

#[tokio::test]
async fn summary_counts_scheduled_origin_only() {
    let (mut conn, _c) = setup_db().await;
    let sid = Uuid::new_v4();
    let base = Utc::now() - Duration::hours(3);

    // 2 scheduled COMPLETED, 1 scheduled FAILED.
    seed_run(
        &mut conn,
        sid,
        "s1",
        Some(ORIGIN_SCHEDULED),
        Some(base),
        "COMPLETED",
        base,
    )
    .await;
    seed_run(
        &mut conn,
        sid,
        "s2",
        Some(ORIGIN_SCHEDULED),
        Some(base + Duration::minutes(1)),
        "COMPLETED",
        base + Duration::minutes(1),
    )
    .await;
    seed_run(
        &mut conn,
        sid,
        "s3",
        Some(ORIGIN_SCHEDULED),
        Some(base + Duration::minutes(2)),
        "FAILED",
        base + Duration::minutes(2),
    )
    .await;
    // A backfill FAILED and a manual FAILED must NOT count toward cadence.
    seed_run(
        &mut conn,
        sid,
        "b1",
        Some(ORIGIN_BACKFILL),
        Some(base - Duration::hours(1)),
        "FAILED",
        base + Duration::minutes(3),
    )
    .await;
    seed_run(
        &mut conn,
        sid,
        "m1",
        Some(ORIGIN_MANUAL_TRIGGER),
        None,
        "FAILED",
        base + Duration::minutes(4),
    )
    .await;

    let summary = schedule_run_state_summary(&mut conn, sid, 0, None, None)
        .await
        .expect("summary");
    let by_state: std::collections::HashMap<_, _> =
        summary.into_iter().map(|c| (c.state, c.count)).collect();

    assert_eq!(by_state.get("COMPLETED").copied().unwrap_or(0), 2);
    assert_eq!(
        by_state.get("FAILED").copied().unwrap_or(0),
        1,
        "only the scheduled FAILED counts; backfill+manual excluded"
    );

    // And the run *list* still includes all five attributed runs.
    let all = list_schedule_runs(
        &mut conn,
        sid,
        0,
        &ScheduleRunQuery {
            limit: 100,
            ..Default::default()
        },
    )
    .await
    .expect("list");
    assert_eq!(
        all.len(),
        5,
        "list shows every origin; summary filters cadence"
    );
}
