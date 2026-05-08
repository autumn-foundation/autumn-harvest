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
use autumn_harvest::types::{ExecutionId, ShardId};
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
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

// -------------------------------------------------------------------------
// Schema migrations
// -------------------------------------------------------------------------

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
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
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!("../migrations/20260501020000_harvest_batch_processed_ids/up.sql"),
);

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
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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
                name: "telemetry_master_workflow",
                module: "telemetry_span_tests",
                handler: telemetry_master_workflow,
            },
            WorkflowInfo {
                name: "telem_child_wf",
                module: "telemetry_span_tests",
                handler: telem_child_wf,
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
                default_queue: Some("default"),
                max_concurrent: None,
                concurrency_key: None,
                is_local: false,
                handler: telem_activity,
            },
            ActivityInfo {
                name: "telem_act_b",
                module: "telemetry_span_tests",
                default_retry_policy: None,
                default_start_to_close: None,
                default_heartbeat_timeout: None,
                default_schedule_to_start: None,
                default_queue: Some("default"),
                max_concurrent: None,
                concurrency_key: None,
                is_local: false,
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
                    trace_context: None,
                },
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
