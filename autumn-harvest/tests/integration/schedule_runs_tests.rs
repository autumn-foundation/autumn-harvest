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
        .expect("migrate");
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
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
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
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
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

/// Force an `error` value onto an already-seeded execution row.
async fn set_error(conn: &mut AsyncPgConnection, id: Uuid, error: &str) {
    use harvest_workflow_executions::dsl;
    diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(id)))
        .set(dsl::error.eq(error))
        .execute(conn)
        .await
        .expect("set error");
}

#[tokio::test]
async fn list_orders_by_logical_slot_and_returns_error() {
    // issue #762: order by COALESCE(scheduled_for, started_at) DESC, not started_at,
    // and surface the raw `error` column (the plugin gates it to terminal-failed).
    let (mut conn, _c) = setup_db().await;
    let sid = Uuid::new_v4();
    let base = Utc::now() - Duration::hours(6);

    // A: scheduled COMPLETED, slot = base+3h, started = base+3h.
    let a = seed_run(
        &mut conn,
        sid,
        "a",
        Some(ORIGIN_SCHEDULED),
        Some(base + Duration::hours(3)),
        "COMPLETED",
        base + Duration::hours(3),
    )
    .await;
    // B: scheduled FAILED, slot = base+2h but started LATER (base+4h) than A — proves
    // ordering keys off the slot, not the start time.
    let b = seed_run(
        &mut conn,
        sid,
        "b",
        Some(ORIGIN_SCHEDULED),
        Some(base + Duration::hours(2)),
        "FAILED",
        base + Duration::hours(4),
    )
    .await;
    set_error(&mut conn, b, "billing failed: card declined\nstack line 2").await;
    // C: manual FAILED, no slot, started base+2h30m → slot key = started_at.
    let c = seed_run(
        &mut conn,
        sid,
        "c",
        Some(ORIGIN_MANUAL_TRIGGER),
        None,
        "FAILED",
        base + Duration::minutes(150),
    )
    .await;
    set_error(&mut conn, c, "manual boom").await;

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

    // Slot keys: A=base+3h, C=base+2h30m (its started_at), B=base+2h → A, C, B.
    let order: Vec<Uuid> = runs.iter().map(|r| r.execution_id).collect();
    assert_eq!(
        order,
        vec![a, c, b],
        "ordered by logical slot, not started_at"
    );

    let b_row = runs.iter().find(|r| r.execution_id == b).unwrap();
    assert_eq!(
        b_row.error.as_deref(),
        Some("billing failed: card declined\nstack line 2"),
        "core returns the raw error verbatim"
    );
    let a_row = runs.iter().find(|r| r.execution_id == a).unwrap();
    assert!(a_row.error.is_none(), "completed run has no error");

    // A manual (slot-less) run's sort key falls back to its own started_at.
    // Compare the DB-sourced row's started_at to the expected timestamp using
    // microsecond precision to avoid spurious nanosecond-level mismatches.
    let c_row = runs.iter().find(|r| r.execution_id == c).unwrap();
    assert_eq!(
        c_row.sort_key(),
        c_row.started_at,
        "manual run's sort key falls back to started_at"
    );
    assert_eq!(
        c_row.started_at.timestamp_micros(),
        (base + Duration::minutes(150)).timestamp_micros(),
        "manual run's started_at matches the expected seeded value"
    );
}

#[tokio::test]
async fn keyset_cursor_uses_slot_key() {
    // issue #762: pagination cursor keys off the logical slot, so a later page never
    // overlaps even when start times disagree with slot order.
    let (mut conn, _c) = setup_db().await;
    let sid = Uuid::new_v4();
    let base = Utc::now() - Duration::hours(12);

    // Five runs whose slots strictly decrease but whose start times are shuffled.
    let starts = [7, 3, 9, 1, 5];
    for (i, s) in starts.iter().enumerate() {
        let i = i64::try_from(i).unwrap();
        seed_run(
            &mut conn,
            sid,
            &format!("k{i}"),
            Some(ORIGIN_SCHEDULED),
            Some(base + Duration::hours(10 - i)), // slot decreasing
            "COMPLETED",
            base + Duration::hours(*s), // start shuffled
        )
        .await;
    }

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
    let last_keep = &page1[1];
    let cursor = Some((last_keep.sort_key(), last_keep.execution_id));

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
    for row in &page2 {
        assert!(
            (row.sort_key(), row.execution_id) < (last_keep.sort_key(), last_keep.execution_id),
            "page2 row {:?} not strictly before cursor",
            row.execution_id
        );
    }
    // Union of page1[..2] and page2 covers all 5 with no overlap.
    assert_eq!(page1.len() - 1 + page2.len(), 5);
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
