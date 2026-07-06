#![cfg(feature = "db")]

use chrono::Utc;
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{CanaryVerdict, ReplayCanaryOptions, WorkflowReplayer};
use autumn_harvest::types::ExecutionId;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── test helpers ─────────────────────────────────────────────────────────────

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
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
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!("../../migrations/20260501020000_harvest_batch_processed_ids/up.sql"),
    "\n",
    include_str!("../../migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../../migrations/20260504000000_harvest_workflow_parent_children/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
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
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
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
    // issue #523: workflow-level retry policy columns.
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
);

async fn make_pool() -> (
    autumn_harvest::shard::ShardedDbPool,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let mut conn = diesel_async::AsyncPgConnection::establish(&url)
        .await
        .expect("connect");
    conn.batch_execute(INIT_SQL).await.expect("migration");

    let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
        diesel_async::AsyncPgConnection,
    >::new(url);
    let pool = deadpool::managed::Pool::builder(manager).build().unwrap();

    let sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool);
    (sharded_pool, container)
}

fn hello_workflow<'a>(
    _ctx: &'a autumn_harvest::context::WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_replay_canary_simple_pass() {
    use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};

    let (pool, _c) = make_pool().await;

    // Insert a running execution and its history event into the DB.
    let mut conn = pool
        .pool_for(pool.default_shard())
        .get()
        .await
        .expect("connection");

    let exec_id = ExecutionId::new();
    let run_id = uuid::Uuid::new_v4();

    diesel::insert_into(harvest_workflow_executions::table)
        .values((
            harvest_workflow_executions::id.eq(exec_id.as_uuid()),
            harvest_workflow_executions::workflow_name.eq("hello".to_string()),
            harvest_workflow_executions::workflow_id.eq("wf-1".to_string()),
            harvest_workflow_executions::run_id.eq(run_id),
            harvest_workflow_executions::shard_id.eq(0),
            harvest_workflow_executions::state.eq("RUNNING".to_string()),
            harvest_workflow_executions::input.eq(json!("hi")),
            harvest_workflow_executions::queue_name.eq("default".to_string()),
            harvest_workflow_executions::started_at.eq(Utc::now()),
            harvest_workflow_executions::created_at.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    let event = WorkflowEvent::WorkflowStarted {
        input: json!("hi"),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    };
    let event_json = serde_json::to_value(&event).unwrap();

    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(1),
            harvest_events::event_type.eq("WorkflowStarted".to_string()),
            harvest_events::event_data.eq(event_json),
            harvest_events::timestamp.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Run the canary.
    let replayer = WorkflowReplayer::new().register_fn("hello", hello_workflow);
    let options = ReplayCanaryOptions {
        sample_size: 10,
        ..Default::default()
    };
    let report = replayer
        .run_canary(&pool, options)
        .await
        .expect("canary run");

    assert_eq!(report.verdict, CanaryVerdict::Pass);
    assert_eq!(report.sampled, 1);
    assert_eq!(report.replay_succeeded, 1);
    assert_eq!(report.replay_failed, 0);
    assert!(report.details.is_empty());
}

fn activity_workflow<'a>(
    ctx: &'a autumn_harvest::context::WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _ = ctx
            .execute_activity_raw("my_activity", input, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

#[tokio::test]
async fn test_replay_canary_suspended_workflow() {
    use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
    use autumn_harvest::types::ActivityExecId;

    let (pool, _c) = make_pool().await;

    let mut conn = pool
        .pool_for(pool.default_shard())
        .get()
        .await
        .expect("connection");

    let exec_id = ExecutionId::new();
    let run_id = uuid::Uuid::new_v4();

    diesel::insert_into(harvest_workflow_executions::table)
        .values((
            harvest_workflow_executions::id.eq(exec_id.as_uuid()),
            harvest_workflow_executions::workflow_name.eq("activity_wf".to_string()),
            harvest_workflow_executions::workflow_id.eq("wf-2".to_string()),
            harvest_workflow_executions::run_id.eq(run_id),
            harvest_workflow_executions::shard_id.eq(0),
            harvest_workflow_executions::state.eq("RUNNING".to_string()),
            harvest_workflow_executions::input.eq(json!("hi")),
            harvest_workflow_executions::queue_name.eq("default".to_string()),
            harvest_workflow_executions::started_at.eq(Utc::now()),
            harvest_workflow_executions::created_at.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Event 1: WorkflowStarted
    let start_event = WorkflowEvent::WorkflowStarted {
        input: json!("hi"),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    };
    let start_json = serde_json::to_value(&start_event).unwrap();

    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(1),
            harvest_events::event_type.eq("WorkflowStarted".to_string()),
            harvest_events::event_data.eq(start_json),
            harvest_events::timestamp.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Event 2: ActivityScheduled
    let act_event = WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "my_activity".to_string(),
        input: json!("hi"),
        queue: "default".to_string(),
    };
    let act_json = serde_json::to_value(&act_event).unwrap();

    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(2),
            harvest_events::event_type.eq("ActivityScheduled".to_string()),
            harvest_events::event_data.eq(act_json),
            harvest_events::timestamp.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Run the canary.
    let replayer = WorkflowReplayer::new().register_fn("activity_wf", activity_workflow);
    let options = ReplayCanaryOptions {
        sample_size: 10,
        ..Default::default()
    };
    let report = replayer
        .run_canary(&pool, options)
        .await
        .expect("canary run");

    // This should PASS because suspending at the end of history for a RUNNING workflow is expected.
    assert_eq!(report.verdict, CanaryVerdict::Pass);
    assert_eq!(report.sampled, 1);
    assert_eq!(report.replay_succeeded, 1);
    assert_eq!(report.replay_failed, 0);
    assert!(report.details.is_empty());
}

#[tokio::test]
async fn test_replay_canary_divergent_workflow() {
    use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
    use autumn_harvest::types::ActivityExecId;

    let (pool, _c) = make_pool().await;

    let mut conn = pool
        .pool_for(pool.default_shard())
        .get()
        .await
        .expect("connection");

    let exec_id = ExecutionId::new();
    let run_id = uuid::Uuid::new_v4();

    diesel::insert_into(harvest_workflow_executions::table)
        .values((
            harvest_workflow_executions::id.eq(exec_id.as_uuid()),
            harvest_workflow_executions::workflow_name.eq("activity_wf".to_string()),
            harvest_workflow_executions::workflow_id.eq("wf-3".to_string()),
            harvest_workflow_executions::run_id.eq(run_id),
            harvest_workflow_executions::shard_id.eq(0),
            harvest_workflow_executions::state.eq("RUNNING".to_string()),
            harvest_workflow_executions::input.eq(json!("hi")),
            harvest_workflow_executions::queue_name.eq("default".to_string()),
            harvest_workflow_executions::started_at.eq(Utc::now()),
            harvest_workflow_executions::created_at.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Event 1: WorkflowStarted
    let start_event = WorkflowEvent::WorkflowStarted {
        input: json!("hi"),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    };
    let start_json = serde_json::to_value(&start_event).unwrap();

    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(1),
            harvest_events::event_type.eq("WorkflowStarted".to_string()),
            harvest_events::event_data.eq(start_json),
            harvest_events::timestamp.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Event 2: ActivityScheduled with a mismatching activity name "different_activity"
    let act_event = WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "different_activity".to_string(),
        input: json!("hi"),
        queue: "default".to_string(),
    };
    let act_json = serde_json::to_value(&act_event).unwrap();

    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(2),
            harvest_events::event_type.eq("ActivityScheduled".to_string()),
            harvest_events::event_data.eq(act_json),
            harvest_events::timestamp.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Run the canary.
    let replayer = WorkflowReplayer::new().register_fn("activity_wf", activity_workflow);
    let options = ReplayCanaryOptions {
        sample_size: 10,
        ..Default::default()
    };
    let report = replayer
        .run_canary(&pool, options)
        .await
        .expect("canary run");

    // This should FAIL because code scheduled "my_activity" but history expected "different_activity".
    assert_eq!(report.verdict, CanaryVerdict::Fail);
    assert_eq!(report.sampled, 1);
    assert_eq!(report.replay_succeeded, 0);
    assert_eq!(report.replay_failed, 1);
    assert_eq!(report.details.len(), 1);
    assert_eq!(
        report.details[0].kind,
        Some(autumn_harvest::testing::NonDeterminismKind::ActivityScheduleMismatch)
    );
    assert!(
        report.details[0].error.contains("expected") && report.details[0].error.contains("got")
    );
}
