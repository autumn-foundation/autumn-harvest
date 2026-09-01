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

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn setup_test_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
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
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
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
        chain_execution_timeout: None,
        chain_deadline_at: None,
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
        start_source: None,
        start_source_ref: None,
        started_by: None,
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

// ---------------------------------------------------------------------------
// Issue #772 Finding A — replay_from_db threads the row's own
// execution_timeout / deadline_at (deadline-aware continue-as-new).
// ---------------------------------------------------------------------------

/// Insert a workflow execution row carrying an `execution_timeout` and a live
/// (pause/resume/redrive-shifted) `deadline_at`, so `replay_from_db` can thread
/// the per-row deadline-aware CAN inputs (issue #772).
#[allow(clippy::too_many_arguments)]
async fn insert_execution_with_deadline(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    name: &str,
    execution_timeout: chrono::Duration,
    deadline_at: chrono::DateTime<Utc>,
) {
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: name,
        workflow_id: "test-wf",
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: Value::Null,
        parent_id: None,
        queue_name: "default",
        execution_timeout: Some(execution_timeout),
        deadline_at: Some(deadline_at),
        chain_execution_timeout: None,
        chain_deadline_at: None,
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
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert workflow execution");
}

/// A workflow that consults the deadline-aware `should_continue_as_new()` before
/// doing one durable step. When a deadline is present the check records/matches a
/// `SideEffectRecorded{Now}` at this exact call site; the subsequent activity then
/// matches the recorded activity events.
fn deadline_can_probe_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // The deadline branch consults the recorded clock here (issue #772).
        if ctx.should_continue_as_new() {
            // In this fixture the deadline is far in the future, so this branch
            // is never taken; it exists so the check is a genuine call site.
            return Ok(Value::Null);
        }
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Persist a history whose second event is the deadline branch's recorded
/// `SideEffectRecorded{Now}`, followed by one activity's scheduled/completed pair.
async fn persist_deadline_probe_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    started_at: chrono::DateTime<Utc>,
    now_capture: chrono::DateTime<Utc>,
) -> Vec<WorkflowEvent> {
    let id1 = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: started_at,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        // The recorded clock read from should_continue_as_new()'s deadline branch
        // — recorded under the reserved probe name (issue #772).
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::event::SideEffectKind::Now,
            name: Some(autumn_harvest::DEADLINE_PROBE_SIDE_EFFECT_NAME.to_string()),
            value: serde_json::json!(now_capture.timestamp_millis()),
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
    ];
    store::append_events(conn, exec_id, &events, 0)
        .await
        .expect("failed to append events");
    events
}

/// Issue #772 Finding A: `replay_from_db` must thread the execution row's own
/// `execution_timeout`/`deadline_at`, NOT the replayer-global option. A row that
/// recorded a `SideEffectRecorded{Now}` from the deadline branch replays cleanly
/// (`ReplaySucceeded`) — the branch is enabled by the per-row timeout so the
/// recorded `Now` matches. Replaying the *same* events with the deadline branch
/// disabled (no `execution_timeout` threaded) leaves the `Now` unmatched and
/// diverges — proving the per-row threading is load-bearing.
#[tokio::test]
async fn replay_from_db_threads_per_row_execution_timeout_for_deadline_branch() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    let started_at = Utc::now();
    let timeout = chrono::Duration::minutes(30);
    // A resume-shifted deadline far in the future, so the branch never trips.
    let deadline_at = started_at + timeout + chrono::Duration::hours(2);
    // The clock read is captured near start, well within budget.
    let now_capture = started_at + chrono::Duration::seconds(5);

    insert_execution_with_deadline(
        &mut conn,
        exec_id,
        "deadline_can_probe_workflow",
        timeout,
        deadline_at,
    )
    .await;
    let events = persist_deadline_probe_history(&mut conn, exec_id, started_at, now_capture).await;

    let replayer = WorkflowReplayer::new()
        .register_fn("deadline_can_probe_workflow", deadline_can_probe_workflow);

    // replay_from_db threads the row's own execution_timeout/deadline_at, so the
    // deadline branch is enabled and consumes the recorded `Now`.
    let report = replayer
        .replay_from_db(&mut conn, exec_id)
        .await
        .expect("replay_from_db must not return a DB error");
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay_from_db must thread the row's execution_timeout so the recorded \
         SideEffectRecorded{{Now}} matches: {report}"
    );

    // Control: the SAME events replayed with no execution_timeout threaded
    // (the pre-Finding-A global-option behavior) leave the `Now` unmatched and
    // diverge — this must NOT report ReplaySucceeded.
    let global_report = replayer.replay_from_events(events).await;
    assert!(
        !matches!(global_report.status, ReplayStatus::ReplaySucceeded),
        "control: with the deadline branch disabled (no per-row timeout) the \
         recorded Now is unmatched and replay must diverge: {global_report}"
    );
}
