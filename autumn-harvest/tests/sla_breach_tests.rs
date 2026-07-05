#![cfg(feature = "db")]
//! Integration tests for the soft workflow SLA breach signal — issue #487.
//!
//! Exercises the observation-only scanner `timeout::enforce_workflow_sla_breaches`:
//! - AC#3/#4: an execution past its `sla_deadline_at` is flipped to
//!   `sla_breached = true` and the metric is recorded **exactly once**; a second
//!   scan is a no-op (idempotent across repeated scans / restarts / replicas).
//! - AC#5: a breached run is **never** terminated — it can still reach COMPLETED.
//! - AC#7: the scan leaves **zero footprint** in `harvest_events`.
//! - The scanner compares `sla_deadline_at` against `COALESCE(completed_at, NOW())`,
//!   so RUNNING rows are judged against now and already-terminal rows against
//!   their actual completion instant (a run that finished before its deadline is
//!   never a breach; one that crossed it just before going terminal still is).
//!   PAUSED rows are excluded (pause suspends the SLA clock); NULL-sla is never
//!   selected.

use std::sync::Mutex;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::timeout;
use autumn_harvest::types::ExecutionId;
use chrono::{Duration as ChronoDuration, Utc};
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

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
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
    include_str!("../migrations/20260613000000_harvest_workflow_sla/up.sql"),
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
    include_str!("../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
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
    conn.batch_execute(INIT_SQL).await.expect("migration");
    (conn, container)
}

/// Counts every `record_workflow_sla_breach` call so we can assert exactly-once.
#[derive(Default)]
struct SpyRecorder {
    breaches: Mutex<Vec<(String, String)>>,
}
impl MetricsRecorder for SpyRecorder {
    fn record_workflow_sla_breach(&self, workflow_name: &str, queue: &str) {
        self.breaches
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }
}

/// Insert one execution row in the given state with an optional SLA deadline.
async fn insert_execution(
    conn: &mut AsyncPgConnection,
    state: &str,
    sla_deadline_at: Option<chrono::DateTime<Utc>>,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let sla = sla_deadline_at.map(|_| ChronoDuration::hours(2));
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "slow_workflow",
            workflow_id: &format!("wf-sla-{}", Uuid::new_v4()),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: serde_json::json!({}),
            memo: None,
            search_attrs: None,
            queue_name: "priority-queue",
            parent_id: None,
            parent_close_policy: None,
            assigned_build_id: None,
            execution_timeout: None,
            deadline_at: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla,
            sla_deadline_at,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            origin: None,
            completion_callbacks: None,
        })
        .execute(conn)
        .await
        .expect("insert workflow execution");

    // New rows default to RUNNING; move to the requested state when different.
    if state != "RUNNING" {
        diesel::update(
            harvest_workflow_executions::table
                .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid())),
        )
        .set(harvest_workflow_executions::state.eq(state))
        .execute(conn)
        .await
        .expect("set state");
    }
    exec_id
}

/// Insert an execution already in a terminal state with a recorded
/// `completed_at`, so the scanner's terminal-row path (which compares against
/// `completed_at`) can be exercised.
async fn insert_terminal_execution(
    conn: &mut AsyncPgConnection,
    state: &str,
    sla_deadline_at: Option<chrono::DateTime<Utc>>,
    completed_at: chrono::DateTime<Utc>,
) -> ExecutionId {
    let exec_id = insert_execution(conn, state, sla_deadline_at).await;
    diesel::update(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid())),
    )
    .set(harvest_workflow_executions::completed_at.eq(Some(completed_at)))
    .execute(conn)
    .await
    .expect("set completed_at");
    exec_id
}

async fn load_breach_flags(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> (bool, Option<chrono::DateTime<Utc>>, String) {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select((
            harvest_workflow_executions::sla_breached,
            harvest_workflow_executions::sla_breached_at,
            harvest_workflow_executions::state,
        ))
        .first(conn)
        .await
        .expect("load breach flags")
}

// ── AC#3 / AC#4: flip exactly once, idempotent across repeated scans ─────────

#[tokio::test]
async fn scanner_flips_breach_once_and_is_idempotent() {
    let (mut conn, _c) = setup_db().await;
    let past = Utc::now() - ChronoDuration::minutes(5);
    let exec_id = insert_execution(&mut conn, "RUNNING", Some(past)).await;

    let spy = SpyRecorder::default();

    // First scan: flips the flag and records the metric once.
    let n1 = timeout::enforce_workflow_sla_breaches(&mut conn, &spy)
        .await
        .expect("first scan");
    assert_eq!(n1, 1, "exactly one row breaches on the first scan");

    let (breached, breached_at, state) = load_breach_flags(&mut conn, exec_id).await;
    assert!(breached, "sla_breached must be set");
    assert!(breached_at.is_some(), "sla_breached_at must be stamped");
    assert_eq!(state, "RUNNING", "scanner never changes lifecycle state");

    // Second scan: no row is eligible (guard `sla_breached = false`), no metric.
    let n2 = timeout::enforce_workflow_sla_breaches(&mut conn, &spy)
        .await
        .expect("second scan");
    assert_eq!(n2, 0, "idempotent: already-breached row is not re-selected");

    let calls = spy.breaches.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        1,
        "metric emitted exactly once across both scans"
    );
    assert_eq!(
        calls[0],
        ("slow_workflow".to_owned(), "priority-queue".to_owned())
    );
}

// ── AC#5: a breached run is never terminated — it can still COMPLETE ──────────

#[tokio::test]
async fn breached_run_is_not_terminated_and_can_complete() {
    let (mut conn, _c) = setup_db().await;
    let past = Utc::now() - ChronoDuration::minutes(5);
    let exec_id = insert_execution(&mut conn, "RUNNING", Some(past)).await;

    timeout::enforce_workflow_sla_breaches(&mut conn, &autumn_harvest::telemetry::NoOpMetrics)
        .await
        .expect("scan");

    let (breached, _, state) = load_breach_flags(&mut conn, exec_id).await;
    assert!(breached);
    assert_eq!(state, "RUNNING", "still running after breach");

    // The owning worker is free to complete the run normally afterwards.
    diesel::update(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid())),
    )
    .set((
        harvest_workflow_executions::state.eq("COMPLETED"),
        harvest_workflow_executions::output.eq(Some(serde_json::json!({"ok": true}))),
        harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
    ))
    .execute(&mut conn)
    .await
    .expect("complete");

    let (breached_after, _, state_after) = load_breach_flags(&mut conn, exec_id).await;
    assert_eq!(state_after, "COMPLETED");
    assert!(
        breached_after,
        "breach observation survives into the terminal record"
    );
}

// ── AC#7: zero `harvest_events` footprint ────────────────────────────────────

#[tokio::test]
async fn breach_scan_leaves_harvest_events_untouched() {
    let (mut conn, _c) = setup_db().await;
    let past = Utc::now() - ChronoDuration::minutes(5);
    let exec_id = insert_execution(&mut conn, "RUNNING", Some(past)).await;

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("seed event");

    let count_before: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count before");

    timeout::enforce_workflow_sla_breaches(&mut conn, &autumn_harvest::telemetry::NoOpMetrics)
        .await
        .expect("scan");

    let count_after: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count after");

    assert_eq!(
        count_before, count_after,
        "the SLA scan must append zero events (replay-neutral)"
    );
}

// ── Eligibility across state + completion (terminal-inclusive scanner) ────────
//
// The scanner compares `sla_deadline_at` against `COALESCE(completed_at, NOW())`.
// RUNNING rows are judged vs now; terminal rows vs their actual completion
// instant. PAUSED is excluded (pause suspends the SLA clock); NULL-sla is never
// selected. SUSPENDED is not a persisted state (the CHECK constraint forbids it).

#[tokio::test]
async fn scanner_eligibility_by_state_and_completion() {
    let (mut conn, _c) = setup_db().await;
    let past = Utc::now() - ChronoDuration::minutes(10);
    let recent_past = Utc::now() - ChronoDuration::minutes(1);
    let future = Utc::now() + ChronoDuration::hours(1);

    // RUNNING past its deadline → breaches.
    let running = insert_execution(&mut conn, "RUNNING", Some(past)).await;
    // PAUSED past its deadline → excluded.
    let paused = insert_execution(&mut conn, "PAUSED", Some(past)).await;
    // COMPLETED *after* its deadline (deadline `past`, finished `recent_past`) → breaches.
    let completed_late =
        insert_terminal_execution(&mut conn, "COMPLETED", Some(past), recent_past).await;
    // COMPLETED *before* its deadline (deadline `future`, finished `recent_past`) → no breach.
    let completed_ontime =
        insert_terminal_execution(&mut conn, "COMPLETED", Some(future), recent_past).await;
    // No SLA → never selected.
    let no_sla = insert_execution(&mut conn, "RUNNING", None).await;
    // RUNNING but deadline still ahead → not yet.
    let not_yet = insert_execution(&mut conn, "RUNNING", Some(future)).await;

    let n =
        timeout::enforce_workflow_sla_breaches(&mut conn, &autumn_harvest::telemetry::NoOpMetrics)
            .await
            .expect("scan");
    assert_eq!(n, 2, "RUNNING-past and COMPLETED-late breach; nothing else");

    assert!(load_breach_flags(&mut conn, running).await.0);
    assert!(
        load_breach_flags(&mut conn, completed_late).await.0,
        "a run that finished after its deadline is caught post-terminal"
    );
    assert!(
        !load_breach_flags(&mut conn, paused).await.0,
        "PAUSED excluded (pause suspends the SLA clock)"
    );
    assert!(
        !load_breach_flags(&mut conn, completed_ontime).await.0,
        "a run that finished before its deadline never breaches"
    );
    assert!(
        !load_breach_flags(&mut conn, no_sla).await.0,
        "NULL sla never selected"
    );
    assert!(
        !load_breach_flags(&mut conn, not_yet).await.0,
        "future deadline not yet breached"
    );
}

// ── Terminal rows are marked exactly once (closes the scan-interval window) ───

#[tokio::test]
async fn terminal_row_past_deadline_breaches_once() {
    let (mut conn, _c) = setup_db().await;
    let deadline = Utc::now() - ChronoDuration::minutes(10);
    let finished = Utc::now() - ChronoDuration::minutes(1); // after the deadline

    // A run that crossed its SLA deadline and then COMPLETED before any scan ran.
    let exec = insert_terminal_execution(&mut conn, "COMPLETED", Some(deadline), finished).await;

    let spy = SpyRecorder::default();
    let n1 = timeout::enforce_workflow_sla_breaches(&mut conn, &spy)
        .await
        .expect("first scan");
    assert_eq!(n1, 1, "the late-finishing terminal row breaches once");

    let (breached, breached_at, state) = load_breach_flags(&mut conn, exec).await;
    assert!(breached);
    assert!(breached_at.is_some());
    assert_eq!(state, "COMPLETED", "scanner never changes lifecycle state");

    // Re-scan: the `sla_breached = false` guard makes it exactly-once.
    let n2 = timeout::enforce_workflow_sla_breaches(&mut conn, &spy)
        .await
        .expect("second scan");
    assert_eq!(n2, 0, "idempotent across re-scans");
    assert_eq!(
        spy.breaches.lock().unwrap().len(),
        1,
        "counted exactly once"
    );
}
