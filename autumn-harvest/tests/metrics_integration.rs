//! DB-backed integration tests for metric emission.
//!
//! Verifies that the `MetricsRecorder` hooks wired into the worker runtime
//! fire in production code paths (not just through the trait surface directly).
//! Requires the `db` feature (testcontainers Postgres).

#![cfg(feature = "db")]

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use autumn_harvest::dlq::{self, NewDeadLetterEntry};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::queue::{self as queue_mod, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::telemetry::{
    ActivityStatus, METRIC_ACTIVITY_DURATION, METRIC_DLQ_ENTRIES, METRIC_QUEUE_DEPTH,
    METRIC_WORKFLOW_DURATION, METRIC_WORKFLOW_STARTED, MetricsRecorder, TelemetryConfig,
    WorkflowStatus,
};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{ActivityContext, WorkflowContext};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Shared INIT_SQL (same set as integration_e2e.rs)
// ---------------------------------------------------------------------------

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
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
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
);

// ---------------------------------------------------------------------------
// Recording MetricsRecorder for assertions
// ---------------------------------------------------------------------------

/// A captured metric emission: name + an opaque label string for quick
/// equality checks without depending on internal label ordering.
#[derive(Debug, Clone)]
struct MetricEmission {
    name: &'static str,
    /// Concatenation of `key=value` pairs (comma-separated, sorted).
    labels_debug: String,
}

#[derive(Debug, Default)]
struct RecordingMetrics {
    emissions: Mutex<Vec<MetricEmission>>,
}

impl RecordingMetrics {
    fn drain(&self) -> Vec<MetricEmission> {
        self.emissions.lock().unwrap().drain(..).collect()
    }

    fn names(&self) -> Vec<&'static str> {
        self.emissions
            .lock()
            .unwrap()
            .iter()
            .map(|e| e.name)
            .collect()
    }

    fn push(&self, name: &'static str, mut labels: Vec<(&'static str, String)>) {
        labels.sort_by_key(|(k, _)| *k);
        let labels_debug = labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        self.emissions
            .lock()
            .unwrap()
            .push(MetricEmission { name, labels_debug });
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_started(&self, workflow_name: &str, queue: &str) {
        self.push(
            METRIC_WORKFLOW_STARTED,
            vec![
                ("queue", queue.to_owned()),
                ("workflow", workflow_name.to_owned()),
            ],
        );
    }

    fn record_workflow_completed(
        &self,
        workflow_name: &str,
        queue: &str,
        _duration_secs: f64,
        status: WorkflowStatus,
    ) {
        self.push(
            METRIC_WORKFLOW_DURATION,
            vec![
                ("queue", queue.to_owned()),
                ("status", status.as_str().to_owned()),
                ("workflow", workflow_name.to_owned()),
            ],
        );
    }

    fn record_activity_completed(
        &self,
        activity_name: &str,
        queue: &str,
        _duration_secs: f64,
        status: ActivityStatus,
    ) {
        self.push(
            METRIC_ACTIVITY_DURATION,
            vec![
                ("activity", activity_name.to_owned()),
                ("queue", queue.to_owned()),
                ("status", status.as_str().to_owned()),
            ],
        );
    }

    fn record_queue_depth(&self, queue_name: &str, depth: u64) {
        self.push(
            METRIC_QUEUE_DEPTH,
            vec![
                ("depth", depth.to_string()),
                ("queue", queue_name.to_owned()),
            ],
        );
    }

    fn record_dlq_entries(&self, shard: u16, depth: u64) {
        self.push(
            METRIC_DLQ_ENTRIES,
            vec![("depth", depth.to_string()), ("shard", shard.to_string())],
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn load_execution(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for execution reload");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("failed to load execution")
}

async fn wait_for_completed(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let ex = load_execution(database_url, exec_id).await;
            if ex.state == "COMPLETED" {
                break ex;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow did not reach COMPLETED within timeout")
}

fn build_worker(worker_id: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: worker_id.to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 2,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(30),
            },
            registry,
        )
        .expect("worker should build"),
    )
}

// ---------------------------------------------------------------------------
// Workflow and activity handlers
// ---------------------------------------------------------------------------

fn metrics_test_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("metrics_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

fn metrics_activity<'a>(
    _ctx: &'a ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

// ---------------------------------------------------------------------------
// Test 1 — workflow.started / workflow.completed / activity.completed
// ---------------------------------------------------------------------------

/// Runs a real workflow (with one activity) end-to-end through the worker and
/// verifies that `harvest.workflow.started`, `harvest.workflow.duration`, and
/// `harvest.activity.duration` metrics are emitted with the correct labels.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_and_activity_metrics_are_recorded() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    // Enqueue a workflow task manually.
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({"msg": "hello metrics"});

    let exec_row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "metrics_test_workflow",
        workflow_id: &format!("metrics-test-{}", Uuid::new_v4()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: workflow_input.clone(),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&exec_row)
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution row");

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    let mut enqueue_params =
        EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    enqueue_params.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    queue_mod::enqueue(&mut conn, &enqueue_params)
        .await
        .expect("enqueue failed");

    // Wire up RecordingMetrics.
    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );

    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![WorkflowInfo {
            name: "metrics_test_workflow",
            module: "metrics_integration",
            handler: metrics_test_workflow,
        }],
        vec![ActivityInfo {
            name: "metrics_activity",
            module: "metrics_integration",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            handler: metrics_activity,
        }],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));

    let worker = build_worker("metrics-worker-1", registry);
    let pool = build_test_pool(&database_url);

    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    wait_for_completed(&database_url, exec_id).await;

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    // --- assertions ---

    let names = recording.names();

    assert!(
        names.contains(&METRIC_WORKFLOW_STARTED),
        "harvest.workflow.started must be emitted; got: {names:?}"
    );

    // Must be emitted exactly once (not once per resume cycle).
    let started_count = names
        .iter()
        .filter(|&&n| n == METRIC_WORKFLOW_STARTED)
        .count();
    assert_eq!(
        started_count, 1,
        "harvest.workflow.started must be emitted exactly once, got {started_count}"
    );

    assert!(
        names.contains(&METRIC_WORKFLOW_DURATION),
        "harvest.workflow.duration must be emitted; got: {names:?}"
    );

    assert!(
        names.contains(&METRIC_ACTIVITY_DURATION),
        "harvest.activity.duration must be emitted; got: {names:?}"
    );

    // Verify workflow label is present in the started emission.
    let emissions = recording.drain();
    let started_emission = emissions
        .iter()
        .find(|e| e.name == METRIC_WORKFLOW_STARTED)
        .expect("workflow.started emission must exist");
    assert!(
        started_emission
            .labels_debug
            .contains("workflow=metrics_test_workflow"),
        "workflow.started label must include workflow name; got: {}",
        started_emission.labels_debug
    );

    // Verify activity status label is "completed".
    let activity_emission = emissions
        .iter()
        .find(|e| e.name == METRIC_ACTIVITY_DURATION)
        .expect("activity.duration emission must exist");
    assert!(
        activity_emission.labels_debug.contains("status=completed"),
        "activity.duration label must have status=completed; got: {}",
        activity_emission.labels_debug
    );

    // Verify workflow completion status label is "completed".
    let wf_duration_emission = emissions
        .iter()
        .find(|e| e.name == METRIC_WORKFLOW_DURATION && e.labels_debug.contains("status=completed"))
        .expect("workflow.duration completed emission must exist");
    assert!(
        wf_duration_emission
            .labels_debug
            .contains("status=completed"),
        "workflow.duration label must have status=completed; got: {}",
        wf_duration_emission.labels_debug
    );
}

// ---------------------------------------------------------------------------
// Test 2 — dlq.entries gauge is sampled by the background ticker
// ---------------------------------------------------------------------------

/// Inserts a dead-letter entry, starts the worker (which spawns the DLQ
/// depth sampler), waits for at least one sampler tick, and asserts that
/// `harvest.dlq.entries` was recorded with depth >= 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dlq_depth_sampler_emits_dlq_entries_metric() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    // Insert one dead-letter entry so the sampler sees depth = 1.
    let dlq_entry = NewDeadLetterEntry {
        original_task_id: Uuid::new_v4(),
        queue_name: "default".to_string(),
        task_type: "ACTIVITY".to_string(),
        workflow_exec_id: None,
        activity_name: Some("some_activity".to_string()),
        input: serde_json::json!({}),
        error: "intentional test failure".to_string(),
        attempts: 3,
    };
    dlq::dead_letter(&mut conn, &dlq_entry)
        .await
        .expect("failed to insert dead-letter entry");

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );

    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));

    let worker = build_worker("metrics-worker-dlq", registry);
    let pool = build_test_pool(&database_url);

    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // poll_interval is 25ms, but Docker-backed worker startup on Windows can
    // spend a few seconds opening the first pooled connections.
    let sampled = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if recording
                .names()
                .into_iter()
                .any(|n| n == METRIC_DLQ_ENTRIES)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    sampled.expect("harvest.dlq.entries must be sampled within 15 s");

    // Verify the depth is >= 1 (we inserted one entry above).
    let emissions = recording.drain();
    let dlq_emission = emissions
        .iter()
        .find(|e| e.name == METRIC_DLQ_ENTRIES)
        .expect("dlq.entries emission must exist after worker ran");

    assert!(
        dlq_emission.labels_debug.contains("shard=0"),
        "dlq.entries label must include shard=0; got: {}",
        dlq_emission.labels_debug
    );
    // depth >= 1 — the label value is the stringified count.
    let depth: u64 = dlq_emission
        .labels_debug
        .split(',')
        .find(|s| s.starts_with("depth="))
        .and_then(|s| s.strip_prefix("depth="))
        .and_then(|v| v.parse().ok())
        .expect("depth label must be a valid u64");
    assert!(depth >= 1, "dlq.entries depth must be >= 1, got {depth}");
}
