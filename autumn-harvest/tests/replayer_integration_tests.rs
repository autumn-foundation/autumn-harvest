#![cfg(all(feature = "db", feature = "testing"))]

//! Real-Postgres integration tests for [`WorkflowReplayer::replay_from_db`].
//!
//! Spins up a throwaway Postgres container per test (via testcontainers),
//! inserts a workflow execution row and its event history directly, then
//! replays using `replay_from_db` and asserts the outcome.
//!
//! Run with:
//! ```text
//!   cargo test -p autumn-harvest --features testing --test replayer_integration_tests
//! ```

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
use chrono::Utc;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Migration SQL (same set as integration_e2e.rs)
// ---------------------------------------------------------------------------

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
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
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
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
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
    include_str!("../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn setup_test_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    (conn, container)
}

/// Insert a minimal workflow execution row so `replay_from_db` can look up the name.
async fn insert_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId, name: &str) {
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: name,
        workflow_id: "test-wf",
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: Value::Null,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,

        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert workflow execution");
}

// ---------------------------------------------------------------------------
// Workflow handler functions
// ---------------------------------------------------------------------------

fn canonical_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn reordered_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Activities in reverse order — diverges from canonical history.
        ctx.execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// ---------------------------------------------------------------------------
// Build a canonical event history and persist it to the DB.
// ---------------------------------------------------------------------------

async fn persist_canonical_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Vec<WorkflowEvent> {
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let timer_id = TimerId::new("cooldown");

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id2,
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id2,
            output: Value::Null,
        },
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 60,
        },
        WorkflowEvent::TimerFired { timer_id },
    ];

    store::append_events(conn, exec_id, &events, 0)
        .await
        .expect("failed to append events");

    events
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// (a) Unchanged workflow replays its own real DB history → `ReplaySucceeded`.
#[tokio::test]
async fn replay_from_db_unchanged_workflow_succeeds() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    insert_execution(&mut conn, exec_id, "canonical_workflow").await;
    persist_canonical_history(&mut conn, exec_id).await;

    let replayer = WorkflowReplayer::new().register_fn("canonical_workflow", canonical_workflow);

    let report = replayer
        .replay_from_db(&mut conn, exec_id)
        .await
        .expect("replay_from_db must not return a DB error");

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "unchanged workflow against real DB history must succeed: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be > 0 when DB history exists"
    );
}

/// (b) Reordered activities against real DB history → `NonDeterminismDetected`.
#[tokio::test]
async fn replay_from_db_detects_non_determinism() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    insert_execution(&mut conn, exec_id, "reordered_workflow").await;
    persist_canonical_history(&mut conn, exec_id).await;

    // Register the reordered handler under the name stored in the DB row.
    let replayer = WorkflowReplayer::new().register_fn("reordered_workflow", reordered_workflow);

    let report = replayer
        .replay_from_db(&mut conn, exec_id)
        .await
        .expect("replay_from_db must not return a DB error");

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "reordered workflow against real DB history must detect non-determinism: {report}"
    );
}

/// (c) `replay_from_db` fails gracefully when the execution ID doesn't exist.
#[tokio::test]
async fn replay_from_db_unknown_exec_id_returns_error() {
    let (mut conn, _container) = setup_test_db().await;
    let unknown_exec_id = ExecutionId::new();

    let replayer = WorkflowReplayer::new().register_fn("canonical_workflow", canonical_workflow);

    let result = replayer.replay_from_db(&mut conn, unknown_exec_id).await;

    assert!(
        result.is_err(),
        "replay_from_db on a nonexistent exec_id must return Err"
    );
}

/// (d) Correct handler registered but with mismatched DB workflow name → `WorkflowFailed`.
#[tokio::test]
async fn replay_from_db_unregistered_handler_surfaces_as_workflow_failed() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    // DB row says "other_workflow" but the replayer only knows "canonical_workflow".
    insert_execution(&mut conn, exec_id, "other_workflow").await;
    persist_canonical_history(&mut conn, exec_id).await;

    let replayer = WorkflowReplayer::new().register_fn("canonical_workflow", canonical_workflow);

    let report = replayer
        .replay_from_db(&mut conn, exec_id)
        .await
        .expect("replay_from_db must not return a DB error on lookup failure");

    assert!(
        matches!(report.status, ReplayStatus::WorkflowFailed { .. }),
        "unregistered handler name must surface as WorkflowFailed: {report}"
    );
}
