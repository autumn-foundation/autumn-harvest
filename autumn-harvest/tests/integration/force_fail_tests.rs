#![cfg(feature = "db")]
//! Integration tests for `timeout::force_fail_activity` — issue #765.
//!
//! Operator force-fail for a single hung in-flight (RUNNING) activity task:
//! - The task row transitions to FAILED with the typed `OperatorForceFailed`
//!   envelope, an `ActivityFailed` event is appended (no new event variant),
//!   and the parked workflow task is woken.
//! - Remaining retry attempts are skipped regardless of retry policy.
//! - Idempotent: a second call on an already-forced task is a no-op success.
//! - 404 (`NotFound`) for unknown tasks / wrong workflow; 409 (`Config`) for
//!   non-RUNNING states, genuine (non-forced) failures, and workflow tasks.
//! - Late worker results (completion or retryable error) cannot resurrect or
//!   double-terminal the forced row.
//! - Pause does not block the force-fail (in-flight enforcement is
//!   pause-blind, mirroring Heartbeat/StartToClose).

use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{
    ActivityFailure, ERROR_TYPE_OPERATOR_FORCE_FAILED, IntoActivityErrorString,
    parse_error_payload_full,
};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::queue;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::timeout::force_fail_activity;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
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
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (conn, container)
}

async fn insert_workflow_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new();
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&NewWorkflowExecution {
            quota_key: None,
            continued_from_exec_id: None,
            first_exec_id: None,
            id: exec_id.as_uuid(),
            workflow_name: "test_wf",
            workflow_id: &format!("wf-ff-{}", Uuid::new_v4()),
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
            chain_execution_timeout: None,
            chain_deadline_at: None,
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
        })
        .execute(conn)
        .await
        .expect("insert workflow execution");
    exec_id
}

/// Seed history: `WorkflowStarted` + `ActivityScheduled` for `activity_id`.
async fn seed_scheduled_activity_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
) {
    store::append_events(
        conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "hung_activity".to_string(),
                input: serde_json::json!({}),
                queue: "default".to_string(),
            },
        ],
        0,
    )
    .await
    .expect("append seed history");
}

/// Insert a claimed, in-flight (RUNNING) activity task row.
async fn insert_running_activity_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    attempt: i32,
    max_attempts: i32,
) -> Uuid {
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          attempt, max_attempts, worker_id, started_at) \
         VALUES ($1, 'default', 'activity', $2, 'hung_activity', $3, '{}'::jsonb, 'RUNNING', \
                 $4, $5, 'hung-worker', NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(attempt)
    .bind::<diesel::sql_types::Integer, _>(max_attempts)
    .execute(conn)
    .await
    .expect("insert running activity task");
    task_id
}

/// Insert a claimed, in-flight (RUNNING) activity task row with a NULL
/// `activity_id` — the legacy pre-#516 row shape whose pending event is
/// resolved by the name-based fallback in `pending_activity_id_for_task`.
async fn insert_legacy_running_activity_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Uuid {
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, input, state, \
          attempt, max_attempts, worker_id, started_at) \
         VALUES ($1, 'default', 'activity', $2, 'hung_activity', '{}'::jsonb, 'RUNNING', \
                 1, 5, 'hung-worker', NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("insert legacy running activity task");
    task_id
}

/// Insert a parked workflow task (state RUNNING, `worker_id` NULL,
/// `started_at` NULL — the shape `wake_workflow_task`'s primary re-pend
/// targets).
async fn insert_parked_workflow_task(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Uuid {
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, attempt, max_attempts) \
         VALUES ($1, 'default', 'workflow', $2, '{}'::jsonb, 'RUNNING', 0, 1)",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("insert parked workflow task");
    task_id
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

#[derive(QueryableByName)]
struct TaskRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error: Option<String>,
}

async fn load_task_row(conn: &mut AsyncPgConnection, task_id: Uuid) -> TaskRow {
    diesel::sql_query("SELECT state, error FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<TaskRow>(conn)
        .await
        .expect("task row query")
}

#[derive(QueryableByName)]
struct ExecStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<ExecStateRow>(conn)
        .await
        .expect("execution state query")
        .state
}

/// Append a terminal `ActivityCompleted` event for `activity_id` on top of
/// the two seed events (`WorkflowStarted` + `ActivityScheduled`).
async fn append_activity_completed(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
) {
    let history = store::load_history(conn, exec_id).await.expect("history");
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!("done"),
        }],
        history.next_event_id,
    )
    .await
    .expect("append terminal event");
}

fn count_activity_failed_events(events: &[WorkflowEvent], activity_id: ActivityExecId) -> usize {
    events
        .iter()
        .filter(|e| {
            matches!(
                e,
                WorkflowEvent::ActivityFailed { activity_id: id, .. } if *id == activity_id
            )
        })
        .count()
}

// ── happy path ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn running_task_is_force_failed() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;

    let events_before = store::load_history(&mut conn, exec_id)
        .await
        .expect("history")
        .events
        .len();

    let outcome = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        Some("INC-42"),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("force_fail_activity should succeed for a RUNNING activity task");

    assert!(outcome.forced, "this call performed the force-fail");
    assert!(!outcome.already_forced);
    assert_eq!(outcome.activity_name, "hung_activity");
    assert_eq!(outcome.queue_name, "default");

    // Row is FAILED with the typed OperatorForceFailed envelope.
    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED");
    let stored = row.error.expect("forced row stores the failure envelope");
    let full = parse_error_payload_full(&stored);
    assert_eq!(full.error_type, ERROR_TYPE_OPERATOR_FORCE_FAILED);
    assert!(full.non_retryable);
    assert!(full.message.contains("INC-42"));

    // Exactly one ActivityFailed event appended, carrying the typed metadata.
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert_eq!(history.events.len(), events_before + 1);
    match history.events.last().expect("appended event") {
        WorkflowEvent::ActivityFailed {
            activity_id: id,
            error,
            error_type,
            non_retryable,
            details,
            ..
        } => {
            assert_eq!(*id, activity_id);
            assert_eq!(error_type, ERROR_TYPE_OPERATOR_FORCE_FAILED);
            assert!(*non_retryable);
            assert!(error.contains("force-failed by operator"));
            assert_eq!(
                details.as_ref().expect("details survive")["reason"],
                "INC-42"
            );
        }
        other => panic!("expected ActivityFailed, got {other:?}"),
    }

    // The owning workflow is NOT terminated: it advances to its own
    // failure/compensation path, so the execution row stays RUNNING.
    assert_eq!(
        execution_state(&mut conn, exec_id).await,
        "RUNNING",
        "force-fail must never terminate the owning execution"
    );
}

#[tokio::test]
async fn workflow_task_is_woken() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;
    let wf_task_id = insert_parked_workflow_task(&mut conn, exec_id).await;

    force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("force fail");

    // The parked workflow task was re-pended (PENDING, immediately claimable)
    // so the workflow advances to its own failure path within one poll cycle.
    let row = load_task_row(&mut conn, wf_task_id).await;
    assert_eq!(
        row.state, "PENDING",
        "parked workflow task must be woken (re-pended) by the force-fail"
    );
}

#[tokio::test]
async fn remaining_retries_are_skipped() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    // attempt 1 of 5: four retry attempts remain — all must be skipped.
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;

    force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("force fail");

    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED", "no PENDING requeue may exist");

    // No other open task rows exist for this execution's activity.
    let open: CountRow = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'activity' \
           AND state IN ('PENDING', 'RUNNING')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("count");
    assert_eq!(
        open.n, 0,
        "no retry attempt may be scheduled after force-fail"
    );
}

// ── idempotency ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn second_call_is_idempotent_no_op() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;

    let first = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        Some("first"),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("first call succeeds");
    assert!(first.forced);

    let second = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        Some("second"),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("second call is an idempotent no-op success");
    assert!(!second.forced);
    assert!(second.already_forced);

    // Exactly ONE ActivityFailed event in history.
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert_eq!(
        count_activity_failed_events(&history.events, activity_id),
        1,
        "re-issuing the call must not append a second terminal event"
    );

    // Zero-writes idempotency: the stored task-row envelope still carries the
    // FIRST call's reason — the second call's "second" reason was never
    // persisted anywhere.
    let row = load_task_row(&mut conn, task_id).await;
    let stored = row.error.expect("forced row stores the failure envelope");
    let full = parse_error_payload_full(&stored);
    assert!(
        full.message.contains("first"),
        "stored envelope must keep the first reason, got: {}",
        full.message
    );
    assert!(
        !full.message.contains("second"),
        "the idempotent no-op must not overwrite the stored reason, got: {}",
        full.message
    );
}

#[tokio::test]
async fn retry_after_forced_run_sealed_is_idempotent_no_op() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;

    let first = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        Some("first"),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("first call succeeds");
    assert!(first.forced);

    // The woken workflow consumes the forced ActivityFailed and seals its own
    // run — the common lifecycle within one poll cycle. Simulate the seal.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'FAILED', error = 'wf boom' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("seal execution FAILED");

    let events_before = store::load_history(&mut conn, exec_id)
        .await
        .expect("history")
        .events
        .len();

    // A retried fail-now (lost response / script retry) must stay the
    // documented idempotent no-op success — NOT flip to the terminal 409
    // (PR #974 Codex review): the already-forced short-circuit wins over the
    // terminal-execution guard.
    let second = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        Some("second"),
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("retry after the run sealed is an idempotent no-op success");
    assert!(!second.forced);
    assert!(second.already_forced);

    // Zero writes: no event appended, exactly one terminal event for the
    // activity, the stored envelope keeps the FIRST reason, and neither the
    // task row nor the execution row was touched.
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert_eq!(
        history.events.len(),
        events_before,
        "no event may be appended by the idempotent retry"
    );
    assert_eq!(
        count_activity_failed_events(&history.events, activity_id),
        1,
        "history must still carry exactly one forced ActivityFailed"
    );
    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED", "task row must be untouched");
    let stored = row.error.expect("forced row stores the failure envelope");
    assert!(
        parse_error_payload_full(&stored).message.contains("first"),
        "the idempotent no-op must not overwrite the stored reason"
    );
    assert_eq!(
        execution_state(&mut conn, exec_id).await,
        "FAILED",
        "execution row must be untouched"
    );
}

// ── conflicts (409) ──────────────────────────────────────────────────────────

#[tokio::test]
async fn pending_task_returns_conflict() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          attempt, max_attempts, scheduled_at) \
         VALUES ($1, 'default', 'activity', $2, 'hung_activity', $3, '{}'::jsonb, 'PENDING', \
                 1, 5, NOW() + INTERVAL '5 minutes')",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("insert pending task");

    let result = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "PENDING (backing-off) task must conflict, got: {result:?}"
    );
}

#[tokio::test]
async fn completed_task_returns_conflict() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;
    queue::complete_task(&mut conn, task_id, serde_json::json!("done"))
        .await
        .expect("complete");

    let result = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "COMPLETED task must conflict, got: {result:?}"
    );
}

#[tokio::test]
async fn genuinely_failed_task_returns_conflict() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;
    queue::fail_task(&mut conn, task_id, "connection refused")
        .await
        .expect("fail with genuine error");

    let result = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "FAILED-with-genuine-error task must conflict (NOT idempotent success), got: {result:?}"
    );
}

#[tokio::test]
async fn terminal_execution_returns_conflict() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    // Stray RUNNING activity row on a FAILED execution — reachable because a
    // plain workflow failure does NOT fail open activity rows.
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'FAILED', error = 'wf boom' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("seal execution FAILED");

    let events_before = store::load_history(&mut conn, exec_id)
        .await
        .expect("history")
        .events
        .len();

    let result = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "terminal execution must conflict (409) — a sealed run's history must \
         never grow another ActivityFailed, got: {result:?}"
    );

    // Zero writes: task row untouched, no event appended.
    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "RUNNING", "task row must be untouched");
    let events_after = store::load_history(&mut conn, exec_id)
        .await
        .expect("history")
        .events
        .len();
    assert_eq!(events_after, events_before, "no event may be appended");
}

#[tokio::test]
async fn terminal_history_with_running_row_returns_conflict() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    // History already carries a terminal event for the activity while the
    // task row is (anomalously) still RUNNING.
    append_activity_completed(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;

    let result = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "terminal-in-history must conflict (409) — a second terminal event \
         for the same activity_id is never appended, got: {result:?}"
    );

    // Task row untouched; history still carries exactly ONE terminal event
    // for the activity (the seeded ActivityCompleted).
    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "RUNNING", "task row must be untouched");
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    let terminal = history
        .events
        .iter()
        .filter(|e| {
            matches!(
                e,
                WorkflowEvent::ActivityCompleted { activity_id: id, .. }
                | WorkflowEvent::ActivityFailed { activity_id: id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id: id, .. }
                    if *id == activity_id
            )
        })
        .count();
    assert_eq!(
        terminal, 1,
        "history must still carry exactly one terminal event for the activity"
    );
}

#[tokio::test]
async fn legacy_task_terminal_history_returns_conflict_not_404() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    append_activity_completed(&mut conn, exec_id, activity_id).await;
    // Legacy row shape: activity_id NULL — resolution falls back to the
    // name-based lookup, whose NotFound must NOT leak through as a 404 for
    // the terminal-in-history case (it is the same documented 409 as the
    // id-resolved branch).
    let task_id = insert_legacy_running_activity_task(&mut conn, exec_id).await;

    let result = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "legacy terminal-in-history must be a 409 Config, not a 404 NotFound, got: {result:?}"
    );
    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "RUNNING", "task row must be untouched");
}

#[tokio::test]
async fn workflow_task_type_returns_conflict() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let wf_task_id = insert_parked_workflow_task(&mut conn, exec_id).await;

    let result = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        wf_task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "a workflow task is not force-failable → 409 Config, got: {result:?}"
    );
}

// ── not found (404) ──────────────────────────────────────────────────────────

#[tokio::test]
async fn unknown_task_returns_not_found() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let result = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        Uuid::new_v4(),
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::NotFound(_))),
        "unknown task id must be NotFound, got: {result:?}"
    );
}

#[tokio::test]
async fn task_for_different_workflow_returns_not_found() {
    let (mut conn, _container) = setup_db().await;
    let exec_a = insert_workflow_execution(&mut conn).await;
    let exec_b = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_a, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_a, activity_id, 1, 5).await;

    let result = force_fail_activity(
        &mut conn,
        exec_b.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::NotFound(_))),
        "task belonging to a different workflow must be NotFound, got: {result:?}"
    );
}

// ── late worker results are ignored ─────────────────────────────────────────

#[tokio::test]
async fn late_completion_after_force_fail_is_ignored() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;

    force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("force fail");

    // The still-running worker attempt finally returns Ok — the queue-layer
    // transition rejects it because the row is no longer RUNNING.
    let late = queue::complete_task(&mut conn, task_id, serde_json::json!("late result")).await;
    assert!(
        matches!(late, Err(HarvestError::NotFound(_))),
        "late completion must be rejected, got: {late:?}"
    );

    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED", "row must stay FAILED");
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert_eq!(
        count_activity_failed_events(&history.events, activity_id),
        1,
        "history must still carry exactly the forced ActivityFailed"
    );
    assert!(
        !history.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityCompleted { activity_id: id, .. } if *id == activity_id
        )),
        "no ActivityCompleted may be recorded after the forced failure"
    );
}

#[tokio::test]
async fn late_retryable_error_cannot_resurrect_failed_row() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;

    force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("force fail");

    // The still-running worker attempt returns a retryable Err and the worker
    // tries to requeue it for retry. `requeue_for_retry`'s UPDATE filters
    // `state = 'RUNNING'`, so the FAILED row cannot be flipped back to PENDING
    // (this pins the "remaining retries are skipped" AC against resurrects).
    let late = queue::requeue_for_retry(
        &mut conn,
        task_id,
        chrono::Duration::seconds(1),
        "late transient error",
    )
    .await;
    assert!(
        matches!(late, Err(HarvestError::NotFound(_))),
        "late retry requeue must be rejected, got: {late:?}"
    );

    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED", "forced row must stay FAILED");
    let stored = row.error.expect("stored error");
    assert_eq!(
        parse_error_payload_full(&stored).error_type,
        ERROR_TYPE_OPERATOR_FORCE_FAILED,
        "stored envelope must remain the forced failure"
    );
}

// ── pause interaction ────────────────────────────────────────────────────────

#[tokio::test]
async fn force_fail_on_paused_execution_succeeds() {
    let (mut conn, _container) = setup_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;
    let activity_id = ActivityExecId::new();
    seed_scheduled_activity_history(&mut conn, exec_id, activity_id).await;
    let task_id = insert_running_activity_task(&mut conn, exec_id, activity_id, 1, 5).await;
    let wf_task_id = insert_parked_workflow_task(&mut conn, exec_id).await;

    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'PAUSED', paused_at = NOW() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("pause execution");

    // In-flight enforcement is pause-blind (mirrors Heartbeat/StartToClose):
    // the force-fail succeeds while paused.
    let outcome = force_fail_activity(
        &mut conn,
        exec_id.as_uuid(),
        task_id,
        None,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("force-fail must succeed on a paused execution");
    assert!(outcome.forced);

    let row = load_task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED");

    // The wake is recorded durably; dispatch is deferred by the PAUSED claim
    // gate until resume (the woken row is PENDING, claim_task skips it).
    let wf_row = load_task_row(&mut conn, wf_task_id).await;
    assert_eq!(
        wf_row.state, "PENDING",
        "wake must be durably recorded even while paused"
    );
}

// ── constructor sanity (exercised through the DB path above; this keeps the
//    envelope contract pinned in the same file for triage convenience) ───────

#[test]
fn forced_envelope_is_recognized_by_the_full_parser() {
    let envelope = ActivityFailure::operator_force_failed(Some("ops")).into_error_payload();
    let full = parse_error_payload_full(&envelope);
    assert_eq!(full.error_type, ERROR_TYPE_OPERATOR_FORCE_FAILED);
    assert!(full.non_retryable);
}
