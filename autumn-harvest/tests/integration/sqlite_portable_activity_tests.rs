#![cfg(feature = "db")]
//! The SQLite portable `#[activity]` fixture runs unchanged on Postgres (issue #2024).
//!
//! `autumn-harvest-sqlite` runs the same file on SQLite. Both suites compare the
//! run's output with `expected_report`. A drift in `ActivityContext` values
//! between the two backends fails one of them.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly. Otherwise a fresh testcontainers Postgres starts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

#[path = "../../../autumn-harvest-sqlite/tests/integration/portable_fixture.rs"]
mod portable_fixture;

use portable_fixture::{
    PORTABLE_WORKFLOW, expected_report, portable_flow_info, portable_probe_info,
};

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

fn worker_config() -> WorkerRuntimeConfig {
    WorkerRuntimeConfig {
        codec_rotation_batch_size: 0,
        scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
        dr: autumn_harvest::replication::DrConfig::default(),
        worker_id: "w-portable".to_string(),
        queues: vec!["default".to_string()],
        notification_database_url: None,
        max_concurrent_workflows: 2,
        max_concurrent_activities: 2,
        max_concurrent_claims: autumn_harvest::worker::DEFAULT_MAX_CONCURRENT_CLAIMS,
        poll_interval: Duration::from_millis(25),
        shutdown_timeout: Duration::from_secs(1),
        cancellation_grace_period: Duration::from_secs(1),
        sticky_timeout: Duration::from_secs(5),
        max_local_activity_start_to_close: Duration::from_secs(60),
        shard_assignments: vec![ShardId::new(0)],
        worker_heartbeat_interval: Duration::from_secs(5),
        build_id: String::new(),
        deployment_name: None,
        workflow_cache_size: 1000,
        resident_workflows: true,
        priority_aging_secs: None,
        unknown_target_grace_window: Duration::from_secs(5),
        poison_pill_threshold: 3,
        capability_miss_max_redeliveries: 5,
        workflow_task_timeout: Duration::from_secs(30),
        workflow_panic_max_attempts: 3,
        labels: HashMap::new(),
        queue_weights: HashMap::new(),
        max_workflow_pause_duration: Duration::from_secs(24 * 3600),
        max_workflow_history_events: None,
        shard_notification_database_urls: Vec::new(),
        sharded_pool: None,
        slot_tuner: None,
        max_concurrent_sessions: 0,
    }
}

/// Seeds a RUNNING execution of the portable workflow and its workflow task.
async fn seed_portable_run(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    label: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = serde_json::json!(label);
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: PORTABLE_WORKFLOW,
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone().into(),
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");
    let mut params = EnqueueParams::new("default", TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

async fn wait_for_completion(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut conn = connect(url).await;
            let e: WorkflowExecution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .select(WorkflowExecution::as_select())
                .first(&mut conn)
                .await
                .expect("reload workflow execution");
            if e.state == "COMPLETED" || e.state == "FAILED" {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the portable run did not finish within 30s")
}

#[tokio::test]
async fn the_portable_activity_runs_unchanged_on_postgres() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let exec_id = seed_portable_run(&mut conn, &workflow_id, "postgres").await;

    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::new(NoOpMetrics) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![portable_flow_info()],
        vec![portable_probe_info()],
        Arc::new(HashMap::new()),
        telemetry,
    ));
    let worker = Arc::new(Worker::new(worker_config(), registry).expect("worker should build"));
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });

    let execution = wait_for_completion(&url, exec_id).await;
    worker.shutdown();
    let _ = handle.await;

    assert_eq!(execution.state, "COMPLETED", "{execution:?}");
    assert_eq!(
        execution.output,
        Some(expected_report("postgres", exec_id, &workflow_id))
    );
}
