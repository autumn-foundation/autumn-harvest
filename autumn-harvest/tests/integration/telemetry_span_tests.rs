#![cfg(feature = "db")]

//! Integration tests for ADR-0001 OpenTelemetry span emission (issue #136).
//!
//! Verifies that all 8 named span kinds are emitted when a workflow with
//! two activities, one signal, one timer, and one child workflow runs to
//! completion.  Uses a real Postgres container via testcontainers so that
//! the worker-side spans (activity.schedule, signal.deliver, timer.fire,
//! `child_workflow.start`) are also exercised.
//!
//! Because the Worker spawns tasks with `tokio::spawn`, we intentionally use
//! a `current_thread` Tokio runtime so that all spawned tasks execute on the
//! same thread.  This lets `tracing::subscriber::with_default` propagate the
//! test subscriber to every spawned task without a global subscriber install.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
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
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

// -------------------------------------------------------------------------
// Schema migrations
// -------------------------------------------------------------------------

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

// -------------------------------------------------------------------------
// Span-capturing tracing layer
// -------------------------------------------------------------------------

/// Records span names in insertion order.  Used to assert that every ADR-0001
/// span kind is emitted at least once during a workflow run.
struct SpanNameLayer(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> Layer<S> for SpanNameLayer {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0
            .lock()
            .unwrap()
            .push(attrs.metadata().name().to_string());
    }
}

/// Install a `SpanNameLayer` as the thread-local default subscriber.
/// Returns `(names_vec, guard)` — both must stay alive for the duration
/// of the test scope.
fn install_span_capture() -> (Arc<Mutex<Vec<String>>>, DefaultGuard) {
    let names = Arc::new(Mutex::new(Vec::<String>::new()));
    let layer = SpanNameLayer(Arc::clone(&names));
    let sub = tracing_subscriber::registry().with(layer);
    let guard = tracing::subscriber::set_default(sub);
    (names, guard)
}

// -------------------------------------------------------------------------
// DB helpers
// -------------------------------------------------------------------------

async fn setup_test_db() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
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
        .max_size(4)
        .build()
        .expect("pool build failed")
}

// -------------------------------------------------------------------------
// Workflow and activity handlers
// -------------------------------------------------------------------------

/// Master workflow: runs 2 activities, waits for a signal, fires a 1-second
/// timer, and spawns a child workflow.  This exercises all 7 core span kinds
/// that are emitted without an HTTP handler.
fn telemetry_master_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("telem_act_a", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("telem_act_b", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.wait_for_signal("telem_proceed")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("telem_timer", 1)
            .await
            .map_err(|e| e.to_string())?;
        ctx.spawn_child_workflow_raw("telem_child_wf", Value::Null)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn telem_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

fn telem_child_wf<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

fn build_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "telemetry_master_workflow",
                module: "telemetry_span_tests",
                handler: telemetry_master_workflow,
                execution_timeout: None,
                chain_execution_timeout: None,
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
            },
            WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "telem_child_wf",
                module: "telemetry_span_tests",
                handler: telem_child_wf,
                execution_timeout: None,
                chain_execution_timeout: None,
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
            },
        ],
        vec![
            ActivityInfo {
                name: "telem_act_a",
                module: "telemetry_span_tests",
                default_retry_policy: None,
                default_start_to_close: None,
                default_heartbeat_timeout: None,
                default_schedule_to_start: None,
                default_schedule_to_close: None,
                default_queue: Some("default"),
                max_concurrent: None,
                concurrency_key: None,
                is_local: false,
                max_input_bytes: None,
                max_result_bytes: None,
                rate_limit_rps: None,
                rate_limit_burst: None,
                rate_limit_key: None,
                rate_limit_key_expr: None,
                circuit_breaker: None,
                requires: None,
                handler: telem_activity,
            },
            ActivityInfo {
                name: "telem_act_b",
                module: "telemetry_span_tests",
                default_retry_policy: None,
                default_start_to_close: None,
                default_heartbeat_timeout: None,
                default_schedule_to_start: None,
                default_schedule_to_close: None,
                default_queue: Some("default"),
                max_concurrent: None,
                concurrency_key: None,
                is_local: false,
                max_input_bytes: None,
                max_result_bytes: None,
                rate_limit_rps: None,
                rate_limit_burst: None,
                rate_limit_key: None,
                rate_limit_key_expr: None,
                circuit_breaker: None,
                requires: None,
                handler: telem_activity,
            },
        ],
    ))
}

async fn wait_for_state(database_url: &str, exec_id: ExecutionId, state: &str) {
    tokio::time::timeout(Duration::from_secs(20), async {
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
    .expect("workflow should reach expected state within timeout");
}

// -------------------------------------------------------------------------
// Integration test
// -------------------------------------------------------------------------

/// Run the full span test inside a `current_thread` runtime so that
/// `with_default` propagates to every spawned task.
///
/// This is a plain `#[test]` (not `#[tokio::test]`) so we control the runtime
/// and can wrap the entire async block in `with_default`.
#[allow(clippy::too_many_lines)]
#[test]
fn all_adr_0001_span_kinds_are_emitted() {
    let (names, _guard) = install_span_capture();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let (db_url, _container) = setup_test_db().await;

            let exec_id = ExecutionId::new_for_shard(ShardId::new(0));

            // Start workflow execution.
            let mut conn = AsyncPgConnection::establish(&db_url)
                .await
                .expect("connect");
            start_or_load_workflow_execution(
                &mut conn,
                StartWorkflowParams {
                    workflow_name: "telemetry_master_workflow",
                    workflow_id: "telem-master-001",
                    exec_id,
                    input: Value::Null,
                    parent_id: None,
                    queue_name: "default",
                    execution_timeout: None,
                    memo: None,
                    search_attrs: None,
                    reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                    conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
                    trace_context: None,
                    max_execution_timeout_ceiling: None,
                    chain_execution_timeout: None,
                    max_workflow_chain_timeout_ceiling: None,
                    inherited_chain_deadline_at: None,
                    concurrency_key: None,
                    concurrency_limit: None,
                    concurrency_on_conflict:
                        autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
                    start_source: autumn_harvest::StartSource::Api,
                    start_source_ref: None,
                    started_by: None,
                },
                None,
            )
            .await
            .expect("start workflow");
            drop(conn);

            // Spawn worker.
            let registry = build_registry();
            let pool = build_pool(&db_url);
            let worker = Arc::new(
                Worker::new(
                    WorkerRuntimeConfig {
                        codec_rotation_batch_size: 0,
                        dr_fencing: false,
                        worker_id: "telem-test-worker".to_string(),
                        queues: vec!["default".to_string()],
                        notification_database_url: None,
                        max_concurrent_workflows: 2,
                        max_concurrent_activities: 4,
                        poll_interval: Duration::from_millis(25),
                        shutdown_timeout: Duration::from_secs(2),
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
                        capability_miss_max_redeliveries: 5,

                        workflow_task_timeout: std::time::Duration::from_secs(10),
                        workflow_panic_max_attempts: 3,
                        labels: std::collections::HashMap::new(),
                        queue_weights: std::collections::HashMap::new(),
                        max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                        max_workflow_history_events: None,
                        shard_notification_database_urls: Vec::new(),
                        sharded_pool: None,
                        slot_tuner: None,
                        max_concurrent_sessions: 0,
                    },
                    registry,
                )
                .expect("worker build"),
            );
            let worker_ref = Arc::clone(&worker);
            let pool_for_run = pool.clone();
            let worker_task = tokio::spawn(async move {
                worker_ref.run(&pool_for_run).await;
            });

            // Let the worker reach the signal-wait suspension.
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Send the signal to unblock the workflow.
            let mut conn = AsyncPgConnection::establish(&db_url)
                .await
                .expect("connect for signal");
            autumn_harvest::signal::send_signal(&mut conn, exec_id, "telem_proceed", Value::Null)
                .await
                .expect("send_signal");
            drop(conn);

            // Wait for the workflow to reach COMPLETED state (the timer adds ~1s).
            wait_for_state(&db_url, exec_id, "COMPLETED").await;

            worker.shutdown();
            worker_task.await.expect("worker task join");
        });

    // Verify every ADR-0001 span kind (except harvest.workflow.schedule which
    // lives in the plugin HTTP layer) was emitted at least once.
    let captured = names.lock().unwrap();

    let assert_span = |span_name: &str| {
        assert!(
            captured.iter().any(|n| n == span_name),
            "expected span '{span_name}' but got: {captured:?}",
        );
    };

    assert_span("harvest.workflow.execute");
    assert_span("harvest.activity.schedule");
    assert_span("harvest.activity.execute");
    assert_span("harvest.signal.send");
    assert_span("harvest.signal.deliver");
    assert_span("harvest.timer.fire");
    assert_span("harvest.child_workflow.start");

    // harvest.workflow.execute must appear at least twice: once for the
    // initial live cycle and at least once for a replay/resume cycle.
    let execute_count = captured
        .iter()
        .filter(|n| n.as_str() == "harvest.workflow.execute")
        .count();
    assert!(
        execute_count >= 2,
        "expected at least 2 harvest.workflow.execute spans (got {execute_count})"
    );
    drop(captured);
}

/// During a replay cycle the executor emits `harvest.workflow.execute` with
/// `harvest.replay = true`.  Activity spans are NOT emitted during replay
/// because the workflow function does not re-execute activity side-effects.
/// This is a pure no-DB unit test (mirrors the executor.rs unit tests).
#[test]
#[allow(clippy::too_many_lines)]
fn replay_span_has_replay_true_and_no_activity_execute_span() {
    use autumn_harvest::context::SharedState;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::executor::run_workflow_strict;
    use autumn_harvest::types::ActivityExecId;
    use chrono::Utc;
    use std::collections::HashMap;

    #[allow(clippy::type_complexity)]
    struct SpanFieldCapture {
        records: Arc<Mutex<Vec<(String, Vec<(String, String)>)>>>,
    }

    impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> Layer<S>
        for SpanFieldCapture
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Visitor(Vec<(String, String)>);
            impl tracing::field::Visit for Visitor {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0
                        .push((field.name().to_string(), format!("{value:?}")));
                }
                fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                    self.0.push((field.name().to_string(), value.to_string()));
                }
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.0.push((field.name().to_string(), value.to_string()));
                }
            }
            let mut v = Visitor(Vec::new());
            attrs.record(&mut v);
            self.records
                .lock()
                .unwrap()
                .push((attrs.metadata().name().to_string(), v.0));
        }
    }

    let records = Arc::new(Mutex::new(Vec::<(String, Vec<(String, String)>)>::new()));
    let layer = SpanFieldCapture {
        records: Arc::clone(&records),
    };
    let sub = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(sub, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let exec_id = ExecutionId::new();
                let act_id = ActivityExecId::new();

                // History: workflow started + one completed activity.
                let history = vec![
                    WorkflowEvent::WorkflowStarted {
                        input: Value::Null,
                        timestamp: Utc::now(),
                        last_completion_result: None,
                        last_error: None,
                        scheduled_time: None,
                    },
                    WorkflowEvent::ActivityScheduled {
                        activity_id: act_id,
                        name: "telem_act_a".to_string(),
                        input: Value::Null,
                        queue: "default".into(),
                    },
                    WorkflowEvent::ActivityCompleted {
                        activity_id: act_id,
                        output: Value::Null,
                    },
                    WorkflowEvent::WorkflowCompleted {
                        output: Value::Null,
                    },
                ];

                let state: SharedState = Arc::new(HashMap::new());
                run_workflow_strict(
                    exec_id,
                    history,
                    telemetry_master_workflow,
                    Value::Null,
                    state,
                    std::collections::HashMap::new(),
                    std::sync::Arc::new(autumn_harvest::telemetry::NoOpMetrics),
                    None,
                    // Issue #772: no per-execution execution_timeout / live deadline_at.
                    None,
                    // Issue #698: no spawning parent (top-level run).
                    None,
                    // Issue #698: workflow type name / business workflow_id.
                    "telemetry_master_workflow".to_string(),
                    None,
                    // Issue #614: default history policy for this span test.
                    // Issue #798: no task queue on this fixture.
                    None,
                    None, // issue #798: candidate build id (unset in this span test)
                    autumn_harvest::context::WorkflowHistoryPolicy::default(),
                    // Issue #798: library-default payload limits for this span test.
                    autumn_harvest::executor::ReplayPayloadLimits::default(),
                    autumn_harvest::executor::ReplayDeclarativeHandlers::default(),
                )
                .await;
            });
    });

    let captured = records.lock().unwrap();

    // harvest.workflow.execute must appear.
    let execute_spans: Vec<_> = captured
        .iter()
        .filter(|(name, _)| name == "harvest.workflow.execute")
        .collect();
    assert!(
        !execute_spans.is_empty(),
        "expected harvest.workflow.execute span"
    );

    // The replay span must carry harvest.replay = true.
    let has_replay_true = execute_spans.iter().any(|(_, fields)| {
        fields
            .iter()
            .any(|(k, v)| k == "harvest.replay" && v == "true")
    });
    assert!(
        has_replay_true,
        "expected harvest.workflow.execute with harvest.replay=true, got: {execute_spans:?}"
    );

    // No harvest.activity.execute should appear during replay (activities are
    // not re-dispatched; their results are replayed from history).
    let activity_execute_count = captured
        .iter()
        .filter(|(name, _)| name == "harvest.activity.execute")
        .count();
    assert_eq!(
        activity_execute_count, 0,
        "harvest.activity.execute must not be emitted during replay"
    );
    drop(captured);
}
