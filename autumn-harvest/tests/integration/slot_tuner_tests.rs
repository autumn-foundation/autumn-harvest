#![cfg(feature = "db")]

//! Integration tests for the adaptive worker slot tuner (issue #548).
//!
//! Exercises the tuner end-to-end against a real Postgres container: the
//! tuner spawn wiring in `Worker::run`, the dispatch-path permit-wait
//! accumulator, and the `harvest.worker.slot_target` telemetry — none of
//! which the pure/`#[tokio::test]` unit tests in `slot_tuner.rs` can reach
//! on their own, since those never construct a real `Worker`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::slot_tuner::SlotTunerConfig;
use autumn_harvest::telemetry::{MetricsRecorder, SlotType, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, StartWorkflowParams, WorkflowContext, WorkflowIdReusePolicy,
    start_or_load_workflow_execution,
};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// -------------------------------------------------------------------------
// Schema migrations (mirrors tests/telemetry_span_tests.rs)
// -------------------------------------------------------------------------

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
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
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
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
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
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
    // issue #499: enforce_timeouts_once now scans harvest_debounce.
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
);

async fn setup_test_db() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

// -------------------------------------------------------------------------
// Metrics capture
// -------------------------------------------------------------------------

/// Records every `record_worker_slot_target` sample, keyed by slot type.
#[derive(Debug, Default)]
struct RecordingMetrics {
    workflow_targets: Mutex<Vec<u64>>,
    activity_targets: Mutex<Vec<u64>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_worker_slot_target(&self, slot_type: SlotType, target: u64) {
        match slot_type {
            SlotType::Workflow => self.workflow_targets.lock().unwrap().push(target),
            SlotType::Activity => self.activity_targets.lock().unwrap().push(target),
        }
    }
}

// -------------------------------------------------------------------------
// Workflow / activity handlers: a workflow that runs one slow activity.
// -------------------------------------------------------------------------

fn slow_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("slow_activity", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn slow_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        Ok(Value::Null)
    })
}

fn build_registry(telemetry: Arc<TelemetryConfig>) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![WorkflowInfo {
            mcp: false,
            name: "slot_tuner_slow_workflow",
            module: "slot_tuner_tests",
            handler: slow_activity_workflow,
            execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![ActivityInfo {
            name: "slow_activity",
            module: "slot_tuner_tests",
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
            handler: slow_activity,
        }],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

fn runtime_config(worker_id: &str, slot_tuner: Option<SlotTunerConfig>) -> WorkerRuntimeConfig {
    WorkerRuntimeConfig {
        worker_id: worker_id.to_string(),
        queues: vec!["default".to_string()],
        notification_database_url: None,
        max_concurrent_workflows: 10,
        // Deliberately small so a burst of concurrent activities saturates
        // the initial target and drives the permit-wait signal above the
        // default 50ms grow threshold.
        max_concurrent_activities: 4,
        poll_interval: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(5),
        cancellation_grace_period: Duration::from_secs(1),
        sticky_timeout: Duration::from_secs(5),
        max_local_activity_start_to_close: Duration::from_secs(60),
        shard_assignments: vec![ShardId::new(0)],
        worker_heartbeat_interval: Duration::from_secs(5),
        build_id: String::new(),
        deployment_name: None,
        workflow_cache_size: 1000,
        priority_aging_secs: None,
        unknown_target_grace_window: Duration::from_secs(5),
        poison_pill_threshold: 3,
        workflow_task_timeout: Duration::from_secs(10),
        workflow_panic_max_attempts: 3,
        labels: std::collections::HashMap::new(),
        queue_weights: std::collections::HashMap::new(),
        max_workflow_pause_duration: Duration::from_secs(24 * 3600),
        max_workflow_history_events: None,
        shard_notification_database_urls: Vec::new(),
        sharded_pool: None,
        slot_tuner,
        max_concurrent_sessions: 0,
    }
}

async fn start_workflow(database_url: &str, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "slot_tuner_slow_workflow",
            workflow_id,
            exec_id,
            input: Value::Null,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
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
            context_headers: None,

            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
        },
    )
    .await
    .expect("start workflow");
    exec_id
}

async fn wait_for_state(database_url: &str, exec_id: ExecutionId, state: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut conn = AsyncPgConnection::establish(database_url)
                .await
                .expect("connect");
            let wf_state: String = autumn_harvest::schema::harvest_workflow_executions::table
                .select(autumn_harvest::schema::harvest_workflow_executions::state)
                .filter(
                    autumn_harvest::schema::harvest_workflow_executions::id.eq(exec_id.as_uuid()),
                )
                .first(&mut conn)
                .await
                .unwrap_or_else(|_| "UNKNOWN".to_string());
            if wf_state == state {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("workflow {exec_id:?} did not reach {state} within timeout"));
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

/// Under a burst of slow activities that saturates the initial dispatch
/// target, the tuner grows the activity semaphore above its starting point
/// (AC: default controller reacts to slot utilization + permit-wait), and a
/// graceful shutdown still drains every in-flight activity to completion
/// (AC: a shrink/tuned worker never cancels in-flight work).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tuner_grows_under_load_and_drains_cleanly() {
    let (db_url, _container) = setup_test_db().await;

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = build_registry(telemetry);
    let pool = build_pool(&db_url);

    // Band [2, 40]: initial target clamps to max_concurrent_activities (4),
    // well inside the band, so there is room to grow.
    let cfg = runtime_config("slot-tuner-grow-worker", Some(SlotTunerConfig::new(2, 40)));
    let worker = Arc::new(Worker::new(cfg, registry).expect("worker build"));

    let worker_ref = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let worker_task = tokio::spawn(async move {
        worker_ref.run(&pool_for_run).await;
    });

    // Enqueue a burst of workflows, each with one 400ms activity, well beyond
    // the initial 4-slot activity capacity, to sustain permit-wait pressure.
    let mut exec_ids = Vec::new();
    for i in 0..24 {
        exec_ids.push(start_workflow(&db_url, &format!("slot-tuner-grow-{i}")).await);
    }

    // Poll the recorded activity slot_target samples until we observe growth
    // above the initial target of 4, or time out.
    let grew = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let max_seen = recording
                .activity_targets
                .lock()
                .unwrap()
                .iter()
                .copied()
                .max()
                .unwrap_or(0);
            if max_seen > 4 {
                return max_seen;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        grew.is_ok(),
        "expected the tuner to grow the activity slot target above 4 under sustained load"
    );

    // All workflows must still reach COMPLETED — no work is lost while the
    // tuner is actively resizing.
    for exec_id in &exec_ids {
        wait_for_state(&db_url, *exec_id, "COMPLETED").await;
    }

    // Graceful shutdown: drain must still succeed cleanly with a tuner
    // installed (a shrink never cancels in-flight work, so nothing here
    // should be left half-finished).
    worker.shutdown();
    worker_task
        .await
        .expect("worker task must shut down cleanly");
}

/// With no tuner configured, the worker never emits `slot_target` samples
/// and dispatches normally — the opt-in default is inert (AC: `None` is
/// byte-for-byte identical to today's fixed-concurrency behaviour).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tuner_unset_is_inert() {
    let (db_url, _container) = setup_test_db().await;

    let recording = Arc::new(RecordingMetrics::default());
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::clone(&recording) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = build_registry(telemetry);
    let pool = build_pool(&db_url);

    let cfg = runtime_config("slot-tuner-inert-worker", None);
    let worker = Arc::new(Worker::new(cfg, registry).expect("worker build"));

    let worker_ref = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let worker_task = tokio::spawn(async move {
        worker_ref.run(&pool_for_run).await;
    });

    let exec_id = start_workflow(&db_url, "slot-tuner-inert-1").await;
    wait_for_state(&db_url, exec_id, "COMPLETED").await;

    // Give any (unexpected) tuner loop a few sampler intervals to have fired
    // if it were wrongly spawned.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        recording.workflow_targets.lock().unwrap().is_empty(),
        "no slot_target samples must be emitted when no tuner is configured"
    );
    assert!(
        recording.activity_targets.lock().unwrap().is_empty(),
        "no slot_target samples must be emitted when no tuner is configured"
    );

    worker.shutdown();
    worker_task
        .await
        .expect("worker task must shut down cleanly");
}
