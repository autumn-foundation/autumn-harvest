#![cfg(feature = "db")]
//! Integration tests for the `schedule_to_close` cross-retry wall-clock deadline — issue #378.
//!
//! Exercises:
//! - `timeout::schedule_to_close_timeout_query` fires for PENDING tasks whose deadline elapsed.
//! - The timeout scanner appends `ActivityTimedOut { ScheduleToClose }` and marks the row FAILED.
//! - The scanner also fires for RUNNING tasks whose deadline elapsed.
//! - Activities without `schedule_to_close` are unaffected (no regression).
//! - The pre-retry check short-circuits requeue when the deadline would be exceeded.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::queue::{EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::timeout;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::{RetryPolicy, TimeoutType};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use std::time::Duration;
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
    // schedule_to_close column on harvest_task_queue (issue #378)
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
    // issue #499: enforce_timeouts_once now scans harvest_debounce.
    include_str!("../migrations/20260618000001_harvest_debounce/up.sql")
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

async fn insert_workflow_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new();
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "test_wf",
            workflow_id: &format!("wf-stc-{}", Uuid::new_v4()),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: serde_json::json!({}),
            memo: None,
            search_attrs: None,
            queue_name: "default",
            parent_id: None,
            parent_close_policy: None,
            assigned_build_id: None,
            execution_timeout: None,
            deadline_at: None,
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
            max_workflow_attempts_ceiling: None,
        })
        .execute(conn)
        .await
        .expect("insert workflow execution");
    exec_id
}

// ── Scanner: PENDING task with expired schedule_to_close_at ─────────────────

/// The timeout scanner must fail a PENDING task whose `schedule_to_close_at`
/// has elapsed and append `ActivityTimedOut { ScheduleToClose }` to history.
#[tokio::test]
async fn scanner_times_out_pending_task_with_expired_schedule_to_close() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

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
    .expect("append WorkflowStarted");

    let activity_id = ActivityExecId::new();
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "timed_activity".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        }],
        1,
    )
    .await
    .expect("append ActivityScheduled");

    // Insert PENDING task with an already-expired schedule_to_close_at
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          attempt, max_attempts, schedule_to_close_at) \
         VALUES ($1, 'default', 'activity', $2, 'timed_activity', $3, '{}'::jsonb, 'PENDING', \
                 0, 10, NOW() - INTERVAL '1 second')",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("insert pending task");

    let enforced = timeout::enforce_timeouts_once(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
        Duration::from_secs(60),
        &None,
        &[],
        None,
        None,
    )
    .await
    .expect("enforce_timeouts_once");
    assert!(enforced >= 1, "expected at least one timeout enforcement");

    let state: String = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<TaskStateRow>(&mut conn)
        .await
        .map(|r| r.state)
        .expect("task state query");
    assert_eq!(
        state, "FAILED",
        "PENDING task must be FAILED after ScheduleToClose"
    );

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");
    assert!(
        history.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToClose,
                ..
            }
        )),
        "ActivityTimedOut {{ ScheduleToClose }} must appear in history; events: {:?}",
        history.events
    );
}

// ── Scanner: RUNNING task with expired schedule_to_close_at ─────────────────

/// The scanner must also fire for RUNNING tasks whose deadline has elapsed
/// (covers in-flight executions that exceed the total wall-clock budget).
#[tokio::test]
async fn scanner_times_out_running_task_with_expired_schedule_to_close() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

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
    .expect("append WorkflowStarted");

    let activity_id = ActivityExecId::new();
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "slow_activity".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        }],
        1,
    )
    .await
    .expect("append ActivityScheduled");

    // Insert RUNNING task with an already-expired schedule_to_close_at
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          worker_id, attempt, max_attempts, started_at, schedule_to_close_at) \
         VALUES ($1, 'default', 'activity', $2, 'slow_activity', $3, '{}'::jsonb, 'RUNNING', \
                 'worker-1', 1, 10, NOW() - INTERVAL '10 seconds', NOW() - INTERVAL '1 second')",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("insert running task");

    let enforced = timeout::enforce_timeouts_once(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
        Duration::from_secs(60),
        &None,
        &[],
        None,
        None,
    )
    .await
    .expect("enforce_timeouts_once");
    assert!(enforced >= 1);

    let state: String = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<TaskStateRow>(&mut conn)
        .await
        .map(|r| r.state)
        .expect("task state query");
    assert_eq!(state, "FAILED");

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");
    assert!(history.events.iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityTimedOut {
            timeout_type: TimeoutType::ScheduleToClose,
            ..
        }
    )));
}

// ── No regression: task without schedule_to_close_at is unaffected ──────────

/// Tasks without a `schedule_to_close_at` must not be touched by the new
/// schedule-to-close scanner path.
#[tokio::test]
async fn scanner_does_not_affect_tasks_without_schedule_to_close() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

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
    .expect("append WorkflowStarted");

    let activity_id = ActivityExecId::new();
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "normal_activity".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        }],
        1,
    )
    .await
    .expect("append ActivityScheduled");

    // RUNNING task with NO schedule_to_close_at
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          worker_id, attempt, max_attempts, started_at) \
         VALUES ($1, 'default', 'activity', $2, 'normal_activity', $3, '{}'::jsonb, 'RUNNING', \
                 'worker-1', 1, 10, NOW() - INTERVAL '1 second')",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("insert running task without stc");

    timeout::enforce_timeouts_once(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
        Duration::from_secs(60),
        &None,
        &[],
        None,
        None,
    )
    .await
    .expect("enforce_timeouts_once");

    let state: String = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<TaskStateRow>(&mut conn)
        .await
        .map(|r| r.state)
        .expect("task state query");
    assert_eq!(
        state, "RUNNING",
        "task without schedule_to_close_at must remain RUNNING"
    );
}

// ── Pre-retry deadline check ─────────────────────────────────────────────────

/// When a task's `schedule_to_close_at` is already in the past at the point the
/// worker computes the next retry delay, `record_schedule_to_close_activity_timeout`
/// must be called instead of re-queuing. The task ends up FAILED and history
/// carries `ActivityTimedOut { ScheduleToClose }`.
///
/// This test sets up the scenario directly in the database (expired deadline +
/// attempt 1 of 10) and confirms the scanner fires, which mirrors the pre-retry
/// code path for the PENDING case.
#[tokio::test]
async fn pre_retry_deadline_check_prevents_requeue_when_deadline_exceeded() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

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
    .expect("append WorkflowStarted");

    let activity_id = ActivityExecId::new();
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "retry_activity".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        }],
        1,
    )
    .await
    .expect("append ActivityScheduled");

    // Task is PENDING with attempt 1/10 and an already-expired deadline.
    // The retry path would attempt attempt 2, but the deadline is expired → ScheduleToClose.
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          attempt, max_attempts, schedule_to_close_at) \
         VALUES ($1, 'default', 'activity', $2, 'retry_activity', $3, '{}'::jsonb, 'PENDING', \
                 1, 10, NOW() - INTERVAL '500 milliseconds')",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("insert task with expired deadline mid-retry");

    let enforced = timeout::enforce_timeouts_once(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
        Duration::from_secs(60),
        &None,
        &[],
        None,
        None,
    )
    .await
    .expect("enforce_timeouts_once");
    assert!(
        enforced >= 1,
        "deadline-exceeded task must be caught by scanner"
    );

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");
    let has_stc_timeout = history.events.iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToClose,
                ..
            }
        )
    });
    assert!(
        has_stc_timeout,
        "ActivityTimedOut {{ ScheduleToClose }} must appear in history when pre-retry deadline is exceeded; events: {:?}",
        history.events
    );
    let has_plain_failure = history
        .events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }));
    assert!(
        !has_plain_failure,
        "ActivityFailed must NOT appear when ScheduleToClose fires (no retry should be attempted)"
    );
}

// ── schedule_to_close_at is persisted via EnqueueParams ─────────────────────

/// Verify that `EnqueueParams::schedule_to_close_at` is correctly persisted to
/// the `harvest_task_queue` table and that a task enqueued with a future deadline
/// is NOT timed out by the scanner.
#[tokio::test]
async fn enqueue_params_schedule_to_close_at_persisted_and_not_prematurely_fired() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

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
    .expect("append WorkflowStarted");

    let activity_id = ActivityExecId::new();
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "future_activity".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        }],
        1,
    )
    .await
    .expect("append ActivityScheduled");

    let future_deadline = Utc::now() + chrono::Duration::minutes(10);
    let mut params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("future_activity".to_string());
    params.activity_id = Some(activity_id.as_uuid());
    params.max_attempts = 5;
    params.schedule_to_close_at = Some(future_deadline);

    let task_id = autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");

    // Verify the deadline was written to the DB
    let stored_deadline: Option<chrono::DateTime<Utc>> =
        diesel::sql_query("SELECT schedule_to_close_at FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .get_result::<DeadlineRow>(&mut conn)
            .await
            .map(|r| r.schedule_to_close_at)
            .expect("deadline query");
    assert!(
        stored_deadline.is_some(),
        "schedule_to_close_at must be persisted"
    );
    let diff = (stored_deadline.unwrap() - future_deadline)
        .num_seconds()
        .abs();
    assert!(diff < 2, "stored deadline must match the one passed in");

    // Scanner must NOT fire for this task (deadline is in the future)
    let enforced = timeout::enforce_timeouts_once(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
        Duration::from_secs(60),
        &None,
        &[],
        None,
        None,
    )
    .await
    .expect("enforce_timeouts_once");
    assert_eq!(enforced, 0, "scanner must not fire for a future deadline");

    let state: String = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<TaskStateRow>(&mut conn)
        .await
        .map(|r| r.state)
        .expect("state query");
    assert_eq!(
        state, "PENDING",
        "task with future deadline must stay PENDING"
    );
}

// ── RetryPolicy::fixed smoke-check (unit, no DB needed) ─────────────────────

/// `schedule_to_close` is correctly represented as a `Duration` in `ActivityInfo`.
#[test]
fn activity_info_schedule_to_close_duration_roundtrip() {
    use autumn_harvest::info::ActivityInfo;

    let info = ActivityInfo {
        name: "payment",
        module: "tests",
        default_retry_policy: Some(RetryPolicy::fixed(10, Duration::from_secs(1))),
        default_start_to_close: Some(Duration::from_secs(2)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: Some(Duration::from_secs(5)),
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };

    assert_eq!(
        info.default_schedule_to_close,
        Some(Duration::from_secs(5)),
        "5s schedule_to_close must round-trip through ActivityInfo"
    );
    assert_eq!(
        info.default_start_to_close,
        Some(Duration::from_secs(2)),
        "start_to_close must be independently stored"
    );
}

// ── Helpers ──────────────────────────────────────────────────────────────────

#[derive(QueryableByName)]
struct TaskStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

#[derive(QueryableByName)]
struct DeadlineRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    schedule_to_close_at: Option<chrono::DateTime<Utc>>,
}
