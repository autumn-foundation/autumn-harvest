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
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use autumn_harvest::dlq::{self, NewDeadLetterEntry};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::queue::{self as queue_mod, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_task_queue::dsl as queue_dsl;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::telemetry::{
    ActivityStatus, METRIC_ACTIVITY_DURATION, METRIC_DLQ_ENTRIES, METRIC_QUEUE_DEPTH,
    METRIC_QUEUE_OLDEST_PENDING_AGE, METRIC_QUEUE_SCHEDULE_TO_START,
    METRIC_WORKFLOW_CONTINUE_AS_NEW, METRIC_WORKFLOW_DURATION, METRIC_WORKFLOW_HISTORY_SIZE,
    METRIC_WORKFLOW_NON_DETERMINISM, METRIC_WORKFLOW_STARTED, MetricsRecorder, TelemetryConfig,
    WorkflowStatus,
};
use autumn_harvest::types::{ActivityExecId, ExecutionId, ParentClosePolicy, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{ActivityContext, RetryPolicy, WorkflowContext, WorkflowHistoryPolicy};
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
    include_str!("../migrations/20260615000001_harvest_context_headers/up.sql")
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

    fn record_workflow_history_size(&self, workflow_name: &str, event_count: u64) {
        self.push(
            METRIC_WORKFLOW_HISTORY_SIZE,
            vec![
                ("count", event_count.to_string()),
                ("workflow.type", workflow_name.to_owned()),
            ],
        );
    }

    fn record_workflow_continue_as_new(&self, workflow_name: &str) {
        self.push(
            METRIC_WORKFLOW_CONTINUE_AS_NEW,
            vec![("workflow.type", workflow_name.to_owned())],
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

    fn record_schedule_to_start(&self, queue_name: &str, wait_secs: f64) {
        self.push(
            METRIC_QUEUE_SCHEDULE_TO_START,
            vec![
                ("queue", queue_name.to_owned()),
                ("wait_secs", format!("{wait_secs:.3}")),
            ],
        );
    }

    fn record_queue_oldest_pending_age(&self, queue_name: &str, age_secs: f64) {
        self.push(
            METRIC_QUEUE_OLDEST_PENDING_AGE,
            vec![
                ("queue", queue_name.to_owned()),
                ("age_secs", format!("{age_secs:.3}")),
            ],
        );
    }

    fn record_dlq_entries(&self, shard: u16, depth: u64) {
        self.push(
            METRIC_DLQ_ENTRIES,
            vec![("depth", depth.to_string()), ("shard", shard.to_string())],
        );
    }

    fn record_workflow_non_determinism(&self, workflow_name: &str, build_id: &str) {
        self.push(
            METRIC_WORKFLOW_NON_DETERMINISM,
            vec![
                ("workflow", workflow_name.to_owned()),
                ("build_id", build_id.to_owned()),
            ],
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
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

async fn load_history(database_url: &str, exec_id: ExecutionId) -> store::EventHistory {
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for history reload");
    store::load_history(&mut conn, exec_id)
        .await
        .expect("failed to load history")
}

async fn load_child_executions(
    database_url: &str,
    parent_exec_id: ExecutionId,
) -> Vec<WorkflowExecution> {
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for child reload");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .order(harvest_workflow_executions::started_at.asc())
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("failed to load child executions")
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

async fn wait_for_state(
    database_url: &str,
    exec_id: ExecutionId,
    expected: &str,
) -> WorkflowExecution {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let ex = load_execution(database_url, exec_id).await;
            if ex.state == expected {
                break ex;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("workflow did not reach {expected} within timeout"))
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
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,

                workflow_task_timeout: std::time::Duration::from_secs(10),
                labels: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                sharded_pool: None,
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

fn continue_metric_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if input
            .get("rotated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(serde_json::json!({"done": true}));
        }

        ctx.continue_as_new(serde_json::json!({"rotated": true}))
            .await
            .map_err(|error| error.to_string())?;
        Ok(serde_json::Value::Null)
    })
}

fn history_cap_violator<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        assert_eq!(ctx.history_event_count(), 2);
        assert!(!ctx.should_continue_as_new());
        Ok(serde_json::json!({"ignored": true}))
    })
}

// History-cap regression workflow handlers.
fn suspended_command_reaches_history_cap<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _: serde_json::Value = ctx
            .side_effect("near-cap", || serde_json::json!({"counted": true}))
            .map_err(|error| error.to_string())?;

        match input.get("kind").and_then(serde_json::Value::as_str) {
            Some("timer") => {
                ctx.timer("history-cap-long-timer", 3_600)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Some("activity") => {
                ctx.execute_activity_raw(
                    "history_cap_never_polled_activity",
                    serde_json::json!({"blocked": true}),
                    "unpolled",
                )
                .await
                .map_err(|error| error.to_string())?;
            }
            Some("child") => {
                ctx.spawn_child_workflow_raw(
                    "history_cap_never_finishing_child",
                    serde_json::json!({"blocked": true}),
                )
                .await
                .map_err(|error| error.to_string())?;
            }
            other => return Err(format!("unknown suspended-command kind: {other:?}")),
        }

        Ok(serde_json::json!({"unreachable": true}))
    })
}

fn history_cap_never_finishing_child<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("child-history-cap-long-timer", 3_600)
            .await
            .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({"unreachable": true}))
    })
}

fn local_activity_retry_reaches_history_cap<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _: serde_json::Value = ctx
            .side_effect(
                "near-cap-local-retry",
                || serde_json::json!({"counted": true}),
            )
            .map_err(|error| error.to_string())?;

        ctx.execute_local_activity_raw(
            "history_cap_always_failing_local",
            serde_json::json!({"blocked": true}),
            Some(RetryPolicy::fixed(5, Duration::ZERO)),
            Some(5),
        )
        .await
        .map_err(|error| error.to_string())?;

        Ok(serde_json::json!({"unreachable": true}))
    })
}

fn history_cap_always_failing_local<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Err("intentional local failure".into()) })
}

fn parent_with_history_capped_child<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("child_breaches_history_cap_inline", input)
            .await
            .map_err(|error| error.to_string())
    })
}

fn detached_cascade_reaches_history_cap<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let first = ctx
            .spawn_child_workflow_detached_raw(
                "history_cap_never_finishing_child",
                serde_json::json!({"child": 1}),
                ParentClosePolicy::RequestCancel,
            )
            .map_err(|error| error.to_string())?;
        let second = ctx
            .spawn_child_workflow_detached_raw(
                "history_cap_never_finishing_child",
                serde_json::json!({"child": 2}),
                ParentClosePolicy::Terminate,
            )
            .map_err(|error| error.to_string())?;

        Ok(serde_json::json!({
            "first": first.to_string(),
            "second": second.to_string(),
        }))
    })
}

fn child_breaches_history_cap_inline<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _: serde_json::Value = ctx
            .side_effect("history-cap-marker", || serde_json::json!({"seen": true}))
            .map_err(|error| error.to_string())?;
        ctx.execute_local_activity_raw(
            "history_cap_local_step",
            serde_json::json!({"step": 1}),
            None,
            Some(5),
        )
        .await
        .map_err(|error| error.to_string())?;
        ctx.execute_local_activity_raw(
            "history_cap_local_step",
            serde_json::json!({"step": 2}),
            None,
            Some(5),
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({"unreachable": true}))
    })
}

fn history_cap_local_step<'a>(
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
            last_completion_result: None,
            last_error: None,
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
            execution_timeout: None,
            sla: None,
            concurrency: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![ActivityInfo {
            name: "metrics_activity",
            module: "metrics_integration",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
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
        names.contains(&METRIC_WORKFLOW_HISTORY_SIZE),
        "harvest.workflow.history_size must be emitted; got: {names:?}"
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

    let history_size_emission = emissions
        .iter()
        .find(|e| e.name == METRIC_WORKFLOW_HISTORY_SIZE)
        .expect("workflow.history_size emission must exist");
    assert!(
        history_size_emission
            .labels_debug
            .contains("workflow.type=metrics_test_workflow"),
        "workflow.history_size label must include workflow.type; got: {}",
        history_size_emission.labels_debug
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn continue_as_new_records_history_size_and_rotation_metrics() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({"rotated": false});
    let workflow_id = format!("continue-metrics-{}", Uuid::new_v4());

    diesel::insert_into(harvest_workflow_executions::table)
        .values(NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "continue_metric_workflow",
            workflow_id: &workflow_id,
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: workflow_input.clone(),
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
        })
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution row");

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
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

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![WorkflowInfo {
            name: "continue_metric_workflow",
            module: "metrics_integration",
            handler: continue_metric_workflow,
            execution_timeout: None,
            sla: None,
            concurrency: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));

    let worker = build_worker("metrics-worker-can", registry);
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    wait_for_state(&database_url, exec_id, "CONTINUED_AS_NEW").await;

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    let emissions = recording.drain();
    assert!(
        emissions
            .iter()
            .any(|e| e.name == METRIC_WORKFLOW_CONTINUE_AS_NEW
                && e.labels_debug
                    .contains("workflow.type=continue_metric_workflow")),
        "continue_as_new metric must be emitted once with workflow.type label; got: {emissions:?}"
    );
    assert!(
        emissions
            .iter()
            .any(|e| e.name == METRIC_WORKFLOW_HISTORY_SIZE
                && e.labels_debug
                    .contains("workflow.type=continue_metric_workflow")),
        "history_size metric must be emitted for continued-as-new execution; got: {emissions:?}"
    );
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_hard_cap_moves_offender_to_dlq() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({});
    let workflow_id = format!("hard-cap-{}", Uuid::new_v4());

    diesel::insert_into(harvest_workflow_executions::table)
        .values(NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "history_cap_violator",
            workflow_id: &workflow_id,
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: workflow_input.clone(),
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
        })
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution row");

    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: workflow_input.clone(),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "already-large".into(),
                details: serde_json::json!({}),
            },
        ],
        0,
    )
    .await
    .expect("append initial history failed");

    let mut enqueue_params =
        EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    enqueue_params.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    queue_mod::enqueue(&mut conn, &enqueue_params)
        .await
        .expect("enqueue failed");

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let policy = WorkflowHistoryPolicy::default().with_event_hard_cap(2);
    let registry = Arc::new(HandlerRegistry::with_state_telemetry_and_history_policy(
        vec![WorkflowInfo {
            name: "history_cap_violator",
            module: "metrics_integration",
            handler: history_cap_violator,
            execution_timeout: None,
            sla: None,
            concurrency: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
        policy,
    ));

    let worker = build_worker("metrics-worker-hard-cap", registry);
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let execution = wait_for_state(&database_url, exec_id, "FAILED").await;

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    let error = execution.error.expect("hard cap should fail execution");
    assert!(
        error.contains("HistoryCapExceeded"),
        "execution error should identify hard cap reason, got: {error}"
    );

    let dead_letters = dlq::list_dead_letters(&mut conn, 10, None)
        .await
        .expect("failed to list DLQ rows");
    let dlq_row = dead_letters
        .iter()
        .find(|row| row.workflow_exec_id == Some(exec_id.as_uuid()))
        .expect("hard-cap offender must be moved to DLQ");
    assert!(
        dlq_row.error.contains("HistoryCapExceeded"),
        "DLQ reason should identify hard cap, got: {}",
        dlq_row.error
    );
    assert_eq!(
        dlq_row.attempts, 1,
        "DLQ attempts must match the terminal task attempt"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn workflow_hard_cap_dlq_preserves_terminal_attempt_count() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({"case":"attempt-regression"});
    let workflow_id = format!("hard-cap-attempt-{}", Uuid::new_v4());

    diesel::insert_into(harvest_workflow_executions::table)
        .values(NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "history_cap_violator",
            workflow_id: &workflow_id,
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: workflow_input.clone(),
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
        })
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution row");

    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: workflow_input.clone(),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "already-large".into(),
                details: serde_json::json!({}),
            },
        ],
        0,
    )
    .await
    .expect("append initial history failed");

    let mut enqueue_params =
        EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    enqueue_params.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    let task_id = queue_mod::enqueue(&mut conn, &enqueue_params)
        .await
        .expect("enqueue failed");

    diesel::update(queue_dsl::harvest_task_queue.find(task_id))
        .set(queue_dsl::attempt.eq(2))
        .execute(&mut conn)
        .await
        .expect("failed to force attempt");

    let policy = WorkflowHistoryPolicy::default().with_event_hard_cap(2);
    let registry = Arc::new(
        HandlerRegistry::new(
            vec![WorkflowInfo {
                name: "history_cap_violator",
                module: "metrics_integration",
                handler: history_cap_violator,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            }],
            vec![],
        )
        .with_history_policy(policy),
    );

    let worker = build_worker("metrics-worker-hard-cap-attempt", registry);
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let _execution = wait_for_state(&database_url, exec_id, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    let dead_letters = dlq::list_dead_letters(&mut conn, 10, None)
        .await
        .expect("failed to list DLQ rows");
    let dlq_row = dead_letters
        .iter()
        .find(|row| row.workflow_exec_id == Some(exec_id.as_uuid()))
        .expect("hard-cap offender must be moved to DLQ");
    assert_eq!(
        dlq_row.attempts, 3,
        "DLQ attempts must preserve terminal task attempt count"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — dlq.entries gauge is sampled by the background ticker
// ---------------------------------------------------------------------------

/// Inserts a dead-letter entry, starts the worker (which spawns the DLQ
/// depth sampler), waits for at least one sampler tick, and asserts that
/// `harvest.dlq.entries` was recorded with depth >= 1.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suspended_commands_that_reach_hard_cap_move_to_dlq_immediately() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let cases = ["timer", "activity", "child"];
    let mut exec_ids = Vec::new();

    for kind in cases {
        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        let workflow_input = serde_json::json!({"kind": kind});
        let workflow_id = format!("suspended-cap-{kind}-{}", Uuid::new_v4());

        diesel::insert_into(harvest_workflow_executions::table)
            .values(NewWorkflowExecution {
                id: exec_id.as_uuid(),
                workflow_name: "suspended_command_reaches_history_cap",
                workflow_id: &workflow_id,
                run_id: Uuid::new_v4(),
                shard_id: 0,
                input: workflow_input.clone(),
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
            })
            .execute(&mut conn)
            .await
            .expect("failed to insert workflow execution row");

        store::append_events(
            &mut conn,
            exec_id,
            &[
                WorkflowEvent::WorkflowStarted {
                    input: workflow_input.clone(),
                    timestamp: Utc::now(),
                    last_completion_result: None,
                    last_error: None,
                },
                WorkflowEvent::MarkerRecorded {
                    name: "side_effect:near-cap".into(),
                    details: serde_json::json!({"counted": true}),
                },
            ],
            0,
        )
        .await
        .expect("append initial history failed");

        let mut enqueue_params =
            EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
        enqueue_params.workflow_exec_id = Some(exec_id.as_uuid());
        enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        queue_mod::enqueue(&mut conn, &enqueue_params)
            .await
            .expect("enqueue failed");

        exec_ids.push((kind, exec_id));
    }

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let policy = WorkflowHistoryPolicy::default().with_event_hard_cap(3);
    let registry = Arc::new(HandlerRegistry::with_state_telemetry_and_history_policy(
        vec![
            WorkflowInfo {
                name: "suspended_command_reaches_history_cap",
                module: "metrics_integration",
                handler: suspended_command_reaches_history_cap,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            },
            WorkflowInfo {
                name: "history_cap_never_finishing_child",
                module: "metrics_integration",
                handler: history_cap_never_finishing_child,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            },
        ],
        vec![ActivityInfo {
            name: "history_cap_never_polled_activity",
            module: "metrics_integration",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("unpolled"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: metrics_activity,
        }],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
        policy,
    ));

    let worker = build_worker("metrics-worker-suspended-cap", registry);
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    for (_, exec_id) in &exec_ids {
        wait_for_state(&database_url, *exec_id, "FAILED").await;
    }

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    let dead_letters = dlq::list_dead_letters(&mut conn, 10, None)
        .await
        .expect("failed to list DLQ rows");

    for (kind, exec_id) in exec_ids {
        let execution = load_execution(&database_url, exec_id).await;
        let error = execution.error.expect("hard cap should fail execution");
        assert!(
            error.contains("HistoryCapExceeded"),
            "{kind} execution error should identify hard cap, got: {error}"
        );

        let history = load_history(&database_url, exec_id).await;
        assert!(
            history
                .events
                .iter()
                .any(|event| matches!(event, WorkflowEvent::WorkflowFailed { .. })),
            "{kind} execution should record terminal failure; got: {:?}",
            history.events
        );

        let appended_suspension_event = history.events.iter().any(|event| match kind {
            "timer" => matches!(event, WorkflowEvent::TimerStarted { .. }),
            "activity" => matches!(event, WorkflowEvent::ActivityScheduled { .. }),
            "child" => matches!(event, WorkflowEvent::ChildWorkflowStarted { .. }),
            _ => false,
        });
        assert!(
            !appended_suspension_event,
            "{kind} suspension event should not be persisted after the cap is reached; got: {:?}",
            history.events
        );

        assert!(
            dead_letters.iter().any(|row| {
                row.workflow_exec_id == Some(exec_id.as_uuid())
                    && row.error.contains("HistoryCapExceeded")
            }),
            "{kind} execution should have a typed DLQ row; got: {dead_letters:?}"
        );

        if kind == "child" {
            let children = load_child_executions(&database_url, exec_id).await;
            assert!(
                children.is_empty(),
                "child-start command should not create child executions after cap breach; got: {children:?}"
            );
        }
    }
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_retries_stop_when_hard_cap_is_reached() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({});
    let workflow_id = format!("local-retry-cap-{}", Uuid::new_v4());

    diesel::insert_into(harvest_workflow_executions::table)
        .values(NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "local_activity_retry_reaches_history_cap",
            workflow_id: &workflow_id,
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: workflow_input.clone(),
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
        })
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution row");

    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: workflow_input.clone(),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "side_effect:near-cap-local-retry".into(),
                details: serde_json::json!({"counted": true}),
            },
        ],
        0,
    )
    .await
    .expect("append initial history failed");

    let mut enqueue_params =
        EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    enqueue_params.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    queue_mod::enqueue(&mut conn, &enqueue_params)
        .await
        .expect("enqueue failed");

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let policy = WorkflowHistoryPolicy::default().with_event_hard_cap(4);
    let registry = Arc::new(HandlerRegistry::with_state_telemetry_and_history_policy(
        vec![WorkflowInfo {
            name: "local_activity_retry_reaches_history_cap",
            module: "metrics_integration",
            handler: local_activity_retry_reaches_history_cap,
            execution_timeout: None,
            sla: None,
            concurrency: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![ActivityInfo {
            name: "history_cap_always_failing_local",
            module: "metrics_integration",
            default_retry_policy: None,
            default_start_to_close: Some(Duration::from_secs(5)),
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            circuit_breaker: None,
            is_local: true,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: history_cap_always_failing_local,
        }],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
        policy,
    ));

    let worker = build_worker("metrics-worker-local-retry-cap", registry);
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let execution = wait_for_state(&database_url, exec_id, "FAILED").await;

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    let error = execution.error.expect("hard cap should fail execution");
    assert!(
        error.contains("HistoryCapExceeded"),
        "execution error should identify hard cap reason, got: {error}"
    );

    let history = load_history(&database_url, exec_id).await;
    let failed_local_attempts = history
        .events
        .iter()
        .filter(|event| matches!(event, WorkflowEvent::LocalActivityFailed { .. }))
        .count();
    assert_eq!(
        failed_local_attempts, 1,
        "hard cap should stop before running local activity retry attempts; got {:?}",
        history.events
    );
    assert!(
        !history
            .events
            .iter()
            .any(|event| matches!(event, WorkflowEvent::LocalActivityExhausted { .. })),
        "hard cap should preempt local activity retry exhaustion; got {:?}",
        history.events
    );

    let dead_letters = dlq::list_dead_letters(&mut conn, 10, None)
        .await
        .expect("failed to list DLQ rows");
    assert!(
        dead_letters.iter().any(|row| {
            row.workflow_exec_id == Some(exec_id.as_uuid())
                && row.error.contains("HistoryCapExceeded")
        }),
        "workflow should have a typed DLQ row; got: {dead_letters:?}"
    );
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_parent_close_cascade_counts_against_history_cap() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({});
    let workflow_id = format!("detached-cascade-cap-{}", Uuid::new_v4());

    diesel::insert_into(harvest_workflow_executions::table)
        .values(NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "detached_cascade_reaches_history_cap",
            workflow_id: &workflow_id,
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: workflow_input.clone(),
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
        })
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution row");

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
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

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let policy = WorkflowHistoryPolicy::default().with_event_hard_cap(5);
    let registry = Arc::new(HandlerRegistry::with_state_telemetry_and_history_policy(
        vec![
            WorkflowInfo {
                name: "detached_cascade_reaches_history_cap",
                module: "metrics_integration",
                handler: detached_cascade_reaches_history_cap,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            },
            WorkflowInfo {
                name: "history_cap_never_finishing_child",
                module: "metrics_integration",
                handler: history_cap_never_finishing_child,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            },
        ],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
        policy,
    ));

    let worker = build_worker("metrics-worker-detached-cascade-cap", registry);
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let parent_execution = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let execution = load_execution(&database_url, exec_id).await;
            if execution.state != "RUNNING" {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow did not reach a terminal state");

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    assert_eq!(parent_execution.state, "FAILED");
    let error = parent_execution
        .error
        .expect("hard cap should fail execution");
    assert!(
        error.contains("HistoryCapExceeded"),
        "execution error should identify hard cap reason, got: {error}"
    );

    let parent_history = load_history(&database_url, exec_id).await;
    assert!(
        !parent_history.events.iter().any(|event| matches!(
            event,
            WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::ChildWorkflowCascadeApplied { .. }
                | WorkflowEvent::WorkflowCompleted { .. }
        )),
        "detached spawn and cascade events should not persist after cap breach: {:?}",
        parent_history.events
    );

    let children = load_child_executions(&database_url, exec_id).await;
    assert!(
        children.is_empty(),
        "detached child rows should not be created after cap breach: {children:?}"
    );

    let dead_letters = dlq::list_dead_letters(&mut conn, 10, None)
        .await
        .expect("failed to list DLQ rows");
    assert!(
        dead_letters.iter().any(|row| {
            row.workflow_exec_id == Some(exec_id.as_uuid())
                && row.error.contains("HistoryCapExceeded")
        }),
        "workflow should have a typed DLQ row; got: {dead_letters:?}"
    );
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_hard_cap_dlq_notifies_parent_and_stops_inline_growth() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let parent_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({});
    let workflow_id = format!("parent-hard-cap-{}", Uuid::new_v4());

    diesel::insert_into(harvest_workflow_executions::table)
        .values(NewWorkflowExecution {
            id: parent_exec_id.as_uuid(),
            workflow_name: "parent_with_history_capped_child",
            workflow_id: &workflow_id,
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: workflow_input.clone(),
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
        })
        .execute(&mut conn)
        .await
        .expect("failed to insert parent workflow execution row");

    store::append_events(
        &mut conn,
        parent_exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        }],
        0,
    )
    .await
    .expect("append parent WorkflowStarted failed");

    let mut enqueue_params =
        EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    enqueue_params.workflow_exec_id = Some(parent_exec_id.as_uuid());
    enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    queue_mod::enqueue(&mut conn, &enqueue_params)
        .await
        .expect("enqueue parent failed");

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let policy = WorkflowHistoryPolicy::default().with_event_hard_cap(4);
    let registry = Arc::new(HandlerRegistry::with_state_telemetry_and_history_policy(
        vec![
            WorkflowInfo {
                name: "parent_with_history_capped_child",
                module: "metrics_integration",
                handler: parent_with_history_capped_child,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            },
            WorkflowInfo {
                name: "child_breaches_history_cap_inline",
                module: "metrics_integration",
                handler: child_breaches_history_cap_inline,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            },
        ],
        vec![ActivityInfo {
            name: "history_cap_local_step",
            module: "metrics_integration",
            default_retry_policy: None,
            default_start_to_close: Some(Duration::from_secs(5)),
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            circuit_breaker: None,
            is_local: true,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: history_cap_local_step,
        }],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
        policy,
    ));

    let worker = build_worker("metrics-worker-child-hard-cap", registry);
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let parent_execution = wait_for_state(&database_url, parent_exec_id, "FAILED").await;

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    let parent_error = parent_execution
        .error
        .expect("parent should fail with child hard-cap reason");
    assert!(
        parent_error.contains("HistoryCapExceeded"),
        "parent error should include child hard-cap reason, got: {parent_error}"
    );
    assert!(
        parent_error.contains("child_breaches_history_cap_inline"),
        "parent should fail from the child failure event, not its own hard cap; got: {parent_error}"
    );

    let parent_history = load_history(&database_url, parent_exec_id).await;
    assert!(
        parent_history
            .events
            .iter()
            .any(|event| matches!(event, WorkflowEvent::ChildWorkflowFailed { .. })),
        "parent history should include ChildWorkflowFailed; got: {:?}",
        parent_history.events
    );

    let child_executions = load_child_executions(&database_url, parent_exec_id).await;
    assert_eq!(child_executions.len(), 1);
    let child_exec_id = ExecutionId::from_uuid(child_executions[0].id);
    assert_eq!(child_executions[0].state, "FAILED");
    assert!(
        child_executions[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("HistoryCapExceeded")),
        "child error should identify hard-cap reason: {:?}",
        child_executions[0].error
    );

    let child_history = load_history(&database_url, child_exec_id).await;
    let completed_local_activities = child_history
        .events
        .iter()
        .filter(|event| matches!(event, WorkflowEvent::LocalActivityCompleted { .. }))
        .count();
    assert_eq!(
        completed_local_activities, 1,
        "hard cap should stop inline local-activity growth after the first local activity; got {:?}",
        child_history.events
    );

    let dead_letters = dlq::list_dead_letters(&mut conn, 10, None)
        .await
        .expect("failed to list DLQ rows");
    assert!(
        dead_letters.iter().any(|row| {
            row.workflow_exec_id == Some(child_exec_id.as_uuid())
                && row.error.contains("HistoryCapExceeded")
        }),
        "capped child should have a typed DLQ row; got: {dead_letters:?}"
    );
}

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

        owner: None,
        severity: None,
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

fn non_deterministic_test_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("wrong_name", serde_json::Value::Null, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn workflow_non_determinism_metric_and_search_attrs_are_recorded() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({"msg": "non-det"});

    let exec_row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "non_deterministic_test_workflow",
        workflow_id: &format!("non-det-{}", Uuid::new_v4()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: workflow_input.clone(),
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&exec_row)
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution row");

    // Replay history expects "step_1" activity scheduled.
    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: workflow_input.clone(),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "step_1".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
            },
        ],
        0,
    )
    .await
    .expect("append events failed");

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
            name: "non_deterministic_test_workflow",
            module: "metrics_integration",
            handler: non_deterministic_test_workflow,
            execution_timeout: None,
            sla: None,
            concurrency: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));

    // Worker with a build_id
    let config = WorkerRuntimeConfig {
        worker_id: "nd-worker-1".to_string(),
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
        build_id: "test-build-v999".to_string(),
        deployment_name: None,
        workflow_cache_size: 1000,
        priority_aging_secs: None,
        unknown_target_grace_window: Duration::from_secs(5),
        poison_pill_threshold: 3,

        workflow_task_timeout: std::time::Duration::from_secs(10),
        labels: std::collections::HashMap::new(),
        max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
        max_workflow_history_events: None,
        sharded_pool: None,
    };
    let worker = Arc::new(Worker::new(config, registry).expect("worker should build"));
    let pool = build_test_pool(&database_url);

    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Wait for the workflow execution to fail.
    let ex = wait_for_state(&database_url, exec_id, "FAILED").await;

    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    // --- assertions ---

    // 1. Metric is recorded.
    let names = recording.names();
    assert!(
        names.contains(&METRIC_WORKFLOW_NON_DETERMINISM),
        "METRIC_WORKFLOW_NON_DETERMINISM must be emitted; got: {names:?}"
    );

    let emissions = recording.drain();
    let nd_emission = emissions
        .iter()
        .find(|e| e.name == METRIC_WORKFLOW_NON_DETERMINISM)
        .expect("non_determinism emission must exist");
    assert!(
        nd_emission
            .labels_debug
            .contains("workflow=non_deterministic_test_workflow"),
        "non-determinism label must include workflow name; got: {}",
        nd_emission.labels_debug
    );
    assert!(
        nd_emission
            .labels_debug
            .contains("build_id=test-build-v999"),
        "non-determinism label must include build_id; got: {}",
        nd_emission.labels_debug
    );

    // 2. search_attrs in DB contains the details.
    let search_attrs = ex.search_attrs.expect("search_attrs should be populated");
    assert_eq!(search_attrs["failure_cause"], "non_determinism");
    assert_eq!(search_attrs["event_index"], 1); // Divergence at position 1 (the scheduled activity).
    assert_eq!(search_attrs["expected"], "ActivityScheduled(wrong_name)");
    assert_eq!(search_attrs["actual"], "ActivityScheduled(step_1)");
    assert_eq!(
        search_attrs["workflow_type"],
        "non_deterministic_test_workflow"
    );
    assert_eq!(search_attrs["build_id"], "test-build-v999");
}

// ---------------------------------------------------------------------------
// Issue #501: schedule-to-start latency + oldest-pending-age metrics
// ---------------------------------------------------------------------------

fn sts_test_workflow<'a>(
    _ctx: &'a autumn_harvest::WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::Value::Null) })
}

/// Verifies that `harvest.queue.schedule_to_start` is emitted when a worker
/// picks up and runs a task, labeled by `queue`, with a wait value that reflects
/// real queue time net of the immediate-enqueue skew allowance (issue #501).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn schedule_to_start_histogram_emitted_at_dispatch() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({"msg": "sts test"});

    let exec_row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "sts_test_workflow",
        workflow_id: &format!("sts-{}", Uuid::new_v4()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: workflow_input.clone(),
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&exec_row)
        .execute(&mut conn)
        .await
        .expect("insert workflow execution failed");

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    // Enqueue with scheduled_at well past the 5s skew allowance so the recorded
    // wait is measurably positive *after* skew correction (issue #501): a 13s
    // backdated schedule yields a >= 8s net wait.
    let mut enqueue_params =
        EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    enqueue_params.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(13);
    queue_mod::enqueue(&mut conn, &enqueue_params)
        .await
        .expect("enqueue failed");

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );

    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![WorkflowInfo {
            name: "sts_test_workflow",
            module: "metrics_integration",
            handler: sts_test_workflow,
            execution_timeout: None,
            sla: None,
            concurrency: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));

    let worker = build_worker("sts-worker-1", registry);
    let pool = build_test_pool(&database_url);

    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    wait_for_completed(&database_url, exec_id).await;
    worker.shutdown();
    handle.await.expect("worker task should join cleanly");

    let emissions = recording.drain();
    let sts = emissions
        .iter()
        .find(|e| e.name == METRIC_QUEUE_SCHEDULE_TO_START)
        .expect("harvest.queue.schedule_to_start must be emitted");
    assert!(
        sts.labels_debug.contains("queue=default"),
        "emission must be labeled by queue; got: {}",
        sts.labels_debug
    );
    // wait_secs is recorded as a formatted f64 string. With a 13s backdated
    // schedule and the 5s skew allowance discounted, the net wait must be a
    // robustly positive value (>= 8s, growing with claim/dispatch delay) — this
    // proves skew correction did not zero out a real wait.
    let wait_secs: f64 = sts
        .labels_debug
        .split(',')
        .find(|s| s.starts_with("wait_secs="))
        .and_then(|s| s.strip_prefix("wait_secs="))
        .and_then(|v| v.parse().ok())
        .expect("wait_secs label must be present and parseable");
    assert!(
        wait_secs >= 5.0,
        "wait_secs must reflect real wait net of skew allowance; got {wait_secs}"
    );
}

/// Verifies that `oldest_pending_ages` returns a positive age when there are
/// eligible pending tasks, and returns an empty slice when the queue is drained
/// (which is what causes the sampler to reset the gauge to 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn oldest_pending_age_query_positive_then_zero_after_drain() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({"msg": "age test"});

    let exec_row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "age_test_workflow",
        workflow_id: &format!("age-{}", Uuid::new_v4()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: workflow_input.clone(),
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&exec_row)
        .execute(&mut conn)
        .await
        .expect("insert workflow execution failed");

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    // Enqueue with scheduled_at well past the 5s skew allowance so the reported
    // age is positive after skew correction (issue #501): 13s backdated → >= 8s.
    let mut enqueue_params =
        EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    enqueue_params.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(13);
    queue_mod::enqueue(&mut conn, &enqueue_params)
        .await
        .expect("enqueue failed");

    let queues = vec!["default".to_string()];

    // Phase 1: there is an eligible task — age must be > 0.
    let ages = autumn_harvest::queue::oldest_pending_ages(&mut conn, &queues)
        .await
        .expect("oldest_pending_ages query must succeed");
    assert_eq!(ages.len(), 1, "expected one queue with an eligible task");
    let (queue_name, age_secs) = &ages[0];
    assert_eq!(queue_name, "default");
    assert!(*age_secs > 0.0, "age_secs must be positive; got {age_secs}");

    // Phase 2: mark the task FAILED (simulating drain) and re-query.
    diesel::update(
        queue_dsl::harvest_task_queue
            .filter(queue_dsl::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(queue_dsl::state.eq("PENDING")),
    )
    .set(queue_dsl::state.eq("FAILED"))
    .execute(&mut conn)
    .await
    .expect("update task to FAILED failed");

    let ages_after_drain = autumn_harvest::queue::oldest_pending_ages(&mut conn, &queues)
        .await
        .expect("oldest_pending_ages query must succeed after drain");
    assert!(
        ages_after_drain.is_empty(),
        "no eligible tasks remain — result must be empty (sampler resets gauge to 0)"
    );
}

/// Verifies that `oldest_pending_ages` excludes a pending workflow task whose
/// execution is PAUSED, mirroring `claim_task`'s pause gating (issue #383 / #501
/// review): a paused execution is intentionally not claimable, so counting its
/// age would inflate the saturation signal and fire false alerts.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn oldest_pending_age_excludes_paused_executions() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("failed to connect");

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_input = serde_json::json!({"msg": "paused age test"});

    let exec_row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "paused_age_test_workflow",
        workflow_id: &format!("paused-age-{}", Uuid::new_v4()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: workflow_input.clone(),
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&exec_row)
        .execute(&mut conn)
        .await
        .expect("insert workflow execution failed");

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: workflow_input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted failed");

    // Enqueue an eligible workflow task aged well past the skew allowance.
    let mut enqueue_params =
        EnqueueParams::new("default", TaskType::Workflow, workflow_input.clone());
    enqueue_params.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue_params.scheduled_at = Utc::now() - chrono::Duration::seconds(13);
    queue_mod::enqueue(&mut conn, &enqueue_params)
        .await
        .expect("enqueue failed");

    let queues = vec!["default".to_string()];

    // While the execution is RUNNING the task is counted.
    let ages = autumn_harvest::queue::oldest_pending_ages(&mut conn, &queues)
        .await
        .expect("oldest_pending_ages query must succeed");
    assert_eq!(ages.len(), 1, "running execution's task must be counted");

    // Pause the execution — the parked task must now be excluded.
    diesel::update(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid())),
    )
    .set(harvest_workflow_executions::state.eq("PAUSED"))
    .execute(&mut conn)
    .await
    .expect("pause execution failed");

    let ages_paused = autumn_harvest::queue::oldest_pending_ages(&mut conn, &queues)
        .await
        .expect("oldest_pending_ages query must succeed while paused");
    assert!(
        ages_paused.is_empty(),
        "paused execution's task must be excluded; got {ages_paused:?}"
    );
}
