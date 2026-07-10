#![cfg(feature = "db")]

//! Integration tests for `queue::force_retry_activity_now` (issue #516).
//!
//! Spins up a real Postgres container with the full harvest schema and verifies
//! the backing-off-activity force-retry semantics end to end.

use autumn_harvest::queue::{self, EnqueueParams, TaskType, force_retry_activity_now};
use autumn_harvest::schema::harvest_task_queue;
use chrono::{Duration, Utc};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
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
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
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
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
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
    // issue #523: workflow-level retry policy columns.
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!("../../migrations/20260710000000_harvest_workflow_continue_chain/up.sql"),
);

async fn setup_test_db() -> (AsyncPgConnection, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container.get_host().await.expect("failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    (conn, container)
}

async fn seed_workflow_execution(
    conn: &mut AsyncPgConnection,
    id: Uuid,
    workflow_name: &str,
    workflow_id: &str,
) {
    diesel::sql_query("INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) VALUES ($1, $2, $3, $4, $5)")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .bind::<diesel::sql_types::Text, _>(workflow_name)
        .bind::<diesel::sql_types::Text, _>(workflow_id)
        .bind::<diesel::sql_types::Integer, _>(0)
        .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!({}))
        .execute(conn)
        .await
        .expect("seeding workflow execution should succeed");
}

/// Enqueue an activity task in a backing-off state: PENDING with a future `scheduled_at`.
async fn enqueue_backing_off_activity(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Uuid,
    backoff_delay_secs: i64,
) -> Uuid {
    seed_workflow_execution(
        conn,
        workflow_exec_id,
        "test_workflow",
        &workflow_exec_id.to_string(),
    )
    .await;

    let mut params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(workflow_exec_id);
    params.activity_name = Some("test_activity".to_string());
    // Future scheduled_at simulates a backing-off retry.
    params.scheduled_at = Utc::now() + Duration::seconds(backoff_delay_secs);
    params.max_attempts = 5;

    queue::enqueue(conn, &params)
        .await
        .expect("enqueue should succeed")
}

/// Enqueue an activity task that is immediately eligible (no backoff).
async fn enqueue_eligible_activity(conn: &mut AsyncPgConnection, workflow_exec_id: Uuid) -> Uuid {
    seed_workflow_execution(
        conn,
        workflow_exec_id,
        "test_workflow",
        &workflow_exec_id.to_string(),
    )
    .await;

    let mut params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(workflow_exec_id);
    params.activity_name = Some("test_activity".to_string());
    // Past scheduled_at = immediately eligible (matches EnqueueParams::new default).

    queue::enqueue(conn, &params)
        .await
        .expect("enqueue should succeed")
}

#[tokio::test]
async fn backing_off_task_is_advanced_to_now() {
    let (mut conn, _container) = setup_test_db().await;
    let workflow_exec_id = Uuid::new_v4();

    let task_id = enqueue_backing_off_activity(&mut conn, workflow_exec_id, 300).await;

    let before = Utc::now();
    let outcome = force_retry_activity_now(&mut conn, workflow_exec_id, task_id)
        .await
        .expect("force_retry_activity_now should succeed for a backing-off task");
    let after = Utc::now();

    assert!(outcome.advanced, "backing-off task should be advanced");
    // scheduled_at should have been moved to approximately now.
    assert!(
        outcome.scheduled_at >= before - Duration::seconds(15)
            && outcome.scheduled_at <= after + Duration::seconds(5),
        "scheduled_at should be near now, got {:?}",
        outcome.scheduled_at
    );
    assert_eq!(outcome.task_id, task_id);
}

#[tokio::test]
async fn attempt_counter_is_not_changed() {
    let (mut conn, _container) = setup_test_db().await;
    let workflow_exec_id = Uuid::new_v4();

    let task_id = enqueue_backing_off_activity(&mut conn, workflow_exec_id, 300).await;

    // Record the attempt count before.
    let row_before = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq(task_id))
        .select((
            harvest_task_queue::attempt,
            harvest_task_queue::max_attempts,
        ))
        .first::<(i32, i32)>(&mut conn)
        .await
        .expect("row should exist");

    force_retry_activity_now(&mut conn, workflow_exec_id, task_id)
        .await
        .expect("should succeed");

    let row_after = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq(task_id))
        .select((
            harvest_task_queue::attempt,
            harvest_task_queue::max_attempts,
        ))
        .first::<(i32, i32)>(&mut conn)
        .await
        .expect("row should still exist");

    assert_eq!(
        row_before, row_after,
        "attempt and max_attempts must not change after force-retry"
    );
}

#[tokio::test]
async fn already_eligible_task_is_idempotent_no_op() {
    let (mut conn, _container) = setup_test_db().await;
    let workflow_exec_id = Uuid::new_v4();

    let task_id = enqueue_eligible_activity(&mut conn, workflow_exec_id).await;

    let outcome = force_retry_activity_now(&mut conn, workflow_exec_id, task_id)
        .await
        .expect("force_retry_activity_now should succeed for an already-eligible task");

    assert!(
        !outcome.advanced,
        "already-eligible task should return advanced=false (no-op)"
    );
    assert!(
        outcome.already_eligible,
        "already-eligible task should report already_eligible=true"
    );
}

#[tokio::test]
async fn idempotent_second_call_also_succeeds() {
    let (mut conn, _container) = setup_test_db().await;
    let workflow_exec_id = Uuid::new_v4();

    let task_id = enqueue_backing_off_activity(&mut conn, workflow_exec_id, 300).await;

    // First call advances.
    let first = force_retry_activity_now(&mut conn, workflow_exec_id, task_id)
        .await
        .expect("first call should succeed");
    assert!(first.advanced);

    // Second call is a no-op (now eligible).
    let second = force_retry_activity_now(&mut conn, workflow_exec_id, task_id)
        .await
        .expect("second call should also succeed");
    assert!(
        !second.advanced,
        "second call on already-eligible task should be no-op"
    );
    assert!(
        second.already_eligible,
        "second call should report already_eligible=true"
    );
}

#[tokio::test]
async fn running_task_returns_conflict_error() {
    use autumn_harvest::error::HarvestError;

    let (mut conn, _container) = setup_test_db().await;
    let workflow_exec_id = Uuid::new_v4();

    // Enqueue and then claim the task to put it in RUNNING state.
    let task_id = enqueue_eligible_activity(&mut conn, workflow_exec_id).await;
    queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "test-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim should succeed");

    let result = force_retry_activity_now(&mut conn, workflow_exec_id, task_id).await;

    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "RUNNING task should return Config (conflict) error, got: {result:?}",
    );
}

#[tokio::test]
async fn unknown_task_id_returns_not_found() {
    use autumn_harvest::error::HarvestError;

    let (mut conn, _container) = setup_test_db().await;
    let workflow_exec_id = Uuid::new_v4();
    let nonexistent_id = Uuid::new_v4();

    let result = force_retry_activity_now(&mut conn, workflow_exec_id, nonexistent_id).await;

    assert!(
        matches!(result, Err(HarvestError::NotFound(_))),
        "unknown task_id should return NotFound, got: {result:?}",
    );
}

#[tokio::test]
async fn task_for_different_workflow_returns_not_found() {
    use autumn_harvest::error::HarvestError;

    let (mut conn, _container) = setup_test_db().await;
    let workflow_a = Uuid::new_v4();
    let workflow_b = Uuid::new_v4();

    let task_id = enqueue_backing_off_activity(&mut conn, workflow_a, 300).await;

    // Attempt to retry it as if it belongs to workflow_b.
    let result = force_retry_activity_now(&mut conn, workflow_b, task_id).await;

    assert!(
        matches!(result, Err(HarvestError::NotFound(_))),
        "task belonging to different workflow should return NotFound, got: {result:?}",
    );
}
