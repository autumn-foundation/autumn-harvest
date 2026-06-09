//! `WorkflowHandle` request/response embedding tests.

use std::time::Duration;

use autumn_harvest::error::{HarvestError, TimeoutType};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{
    ExecutionId, Priority, StartWorkflowParams, WorkflowHandleClient, WorkflowIdReusePolicy,
    WorkflowResult, WorkflowResultState, start_or_load_workflow_execution,
};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use scoped_futures::ScopedFutureExt;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
    "\n",
    include_str!("../migrations/20260424000000_harvest_sticky_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260428000000_harvest_retention_scan_index/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!("../migrations/20260501020000_harvest_batch_processed_ids/up.sql"),
    "\n",
    include_str!("../migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../migrations/20260504000000_harvest_workflow_parent_children/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
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
    // Auto-pause columns (issue #360)
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
    include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql")
);

async fn setup_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container postgres port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, container)
}

fn build_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("test pool should build")
}

async fn start_running_workflow(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> autumn_harvest::StartedWorkflowExecution {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "echo",
            workflow_id: "echo-1",
            exec_id,
            input: serde_json::json!({"name": "Mina"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::RejectDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,

            owner: None,
            runbook_url: None,
            severity: None,
        },
    )
    .await
    .expect("workflow should start")
}

async fn mark_completed(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    use autumn_harvest::WorkflowEvent;
    use autumn_harvest::error::HarvestError;
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let output = serde_json::json!({"ok": true});
    conn.transaction::<(), HarvestError, _>(|conn| {
        let output = output.clone();
        async move {
            autumn_harvest::store::append_events(
                conn,
                exec_id,
                &[WorkflowEvent::WorkflowCompleted {
                    output: output.clone(),
                }],
                1,
            )
            .await?;

            diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
                .set((
                    dsl::state.eq("COMPLETED"),
                    dsl::output.eq(Some(output)),
                    dsl::completed_at.eq(Some(chrono::Utc::now())),
                ))
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;
            Ok(())
        }
        .scope_boxed()
    })
    .await
    .expect("workflow row should complete");
}

#[tokio::test]
async fn workflow_event_listener_receives_append_notification() {
    let (database_url, _container) = setup_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let _started = start_running_workflow(&mut conn, exec_id).await;
    let mut listener = autumn_harvest::notify::WorkflowEventListener::connect(&database_url)
        .await
        .expect("listener should connect");

    mark_completed(&mut conn, exec_id).await;

    let outcome = tokio::time::timeout(Duration::from_secs(2), listener.wait_for_notification())
        .await
        .expect("listener should receive notification")
        .expect("notification payload should parse");

    assert!(matches!(
        outcome,
        autumn_harvest::notify::WorkflowEventWaitOutcome::Notification(payload)
            if payload.workflow_exec_id == exec_id.as_uuid()
                && payload.last_event_type == "WorkflowCompleted"
    ));
}

#[tokio::test]
async fn result_raw_with_timeout_returns_timeout_for_running_workflow() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::single(pool, database_url);

    let error = client
        .handle(started.exec_id)
        .result_raw_with_timeout(Duration::from_millis(50))
        .await
        .expect_err("running workflow should time out");

    assert!(matches!(
        error,
        HarvestError::Timeout {
            timeout_type: TimeoutType::ScheduleToClose,
            task_name
        } if task_name == "echo"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_raw_wakes_on_harvest_events_notification() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::new(
        ShardedDbPool::single(pool),
        ShardRouter::single(),
        [(autumn_harvest::ShardId::new(0), database_url.clone())],
    );
    let handle = client.handle(started.exec_id);

    let waiter = tokio::spawn(async move { handle.result_raw().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    mark_completed(&mut conn, exec_id).await;

    let result = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("handle should wake after completion notification")
        .expect("waiter task should not panic")
        .expect("result should be successful");

    assert_eq!(result, serde_json::json!({"ok": true}));
}

#[tokio::test]
async fn result_snapshot_with_wait_returns_none_for_running_workflow() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::single(pool, database_url);

    let result = client
        .handle(started.exec_id)
        .result_snapshot_with_wait(Duration::from_millis(50))
        .await
        .expect("wait should not fail");

    assert!(
        result.is_none(),
        "running workflow should produce 204-style miss"
    );
}

#[test]
fn workflow_result_from_terminal_execution_is_compact() {
    let response = WorkflowResult::completed(
        WorkflowResultState::Completed,
        serde_json::json!({"value": 42}),
        Some("2026-05-09T12:00:00Z".parse().expect("timestamp")),
    );
    let json = serde_json::to_value(response).expect("serializable result response");

    assert_eq!(json["state"], "completed");
    assert_eq!(json["output"], serde_json::json!({"value": 42}));
    assert!(
        json.get("history").is_none(),
        "result response must stay compact"
    );
}
