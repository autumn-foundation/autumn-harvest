//! Default history caps (issue #1804).
//!
//! These tests run a real worker against Postgres with the default
//! `WorkflowHistoryPolicy`. They prove three things:
//!
//! - A run that reaches 50,000 events fails with the typed
//!   `HistoryCapExceeded` reason.
//! - The early warning fires at 10,000 events.
//! - A run whose stored history reaches the byte cap fails with the typed
//!   `HistoryBytesCapExceeded` reason, on the warm cache path too.

#![cfg(feature = "db")]

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Jsonb, Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use autumn_harvest::dlq::{self, DeadLetterReason};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::queue::{self as queue_mod, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::telemetry::{METRIC_WORKFLOW_HISTORY_BLOAT, MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{WorkflowContext, WorkflowHistoryPolicy};
use chrono::Utc;

const WORKFLOW_NAME: &str = "history_default_caps_grower";

/// Time budget for a decision over a 50,000-event history.
const LARGE_HISTORY_WAIT: Duration = Duration::from_secs(90);

// ---------------------------------------------------------------------------
// Database and worker setup
// ---------------------------------------------------------------------------

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
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn build_worker(worker_id: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 2,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                // A long sticky window keeps follow-up tasks on the warm cache.
                sticky_timeout: Duration::from_secs(30),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(30),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(60),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct RecordingMetrics {
    history_bloat: Mutex<Vec<String>>,
    cache_hits: Mutex<u64>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_history_bloat(&self, workflow_name: &str) {
        self.history_bloat
            .lock()
            .unwrap()
            .push(workflow_name.to_owned());
    }

    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        *self.cache_hits.lock().unwrap() += 1;
    }
}

// ---------------------------------------------------------------------------
// Workflow handler
// ---------------------------------------------------------------------------

/// A runaway signal loop. Each `grow` signal adds history and nothing ends it.
fn grower<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        loop {
            ctx.wait_for_signal("grow")
                .await
                .map_err(|error| error.to_string())?;
        }
    })
}

fn registry(policy: WorkflowHistoryPolicy, metrics: Arc<RecordingMetrics>) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_telemetry_and_history_policy(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: WORKFLOW_NAME,
            module: "history_default_caps_tests",
            handler: grower,
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
        }],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
        policy,
    ))
}

// ---------------------------------------------------------------------------
// Seeding helpers
// ---------------------------------------------------------------------------

/// Insert a RUNNING execution with a `WorkflowStarted` event and a due task.
async fn seed_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = serde_json::json!({});
    let workflow_id = format!("history-default-caps-{}", Uuid::new_v4());

    diesel::insert_into(harvest_workflow_executions::table)
        .values(NewWorkflowExecution {
            quota_key: None,
            continued_from_exec_id: None,
            first_exec_id: None,
            id: exec_id.as_uuid(),
            workflow_name: WORKFLOW_NAME,
            workflow_id: &workflow_id,
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
            start_source: None,
            start_source_ref: None,
            started_by: None,
        })
        .execute(conn)
        .await
        .expect("insert execution row");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");
    exec_id
}

/// Append `count` inert `SignalReceived` events after `WorkflowStarted`.
///
/// One `INSERT ... SELECT` keeps a 50,000-row seed fast.
async fn pad_history(conn: &mut AsyncPgConnection, exec_id: ExecutionId, count: i32) {
    let padding = WorkflowEvent::SignalReceived {
        signal_name: "pad".into(),
        payload: serde_json::json!({}),
    };
    let event_data = serde_json::to_value(&padding).expect("serialize padding");
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data) \
         SELECT $1, g, $2, $3 FROM generate_series(1, $4) AS g",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(padding.type_name())
    .bind::<Jsonb, _>(event_data)
    .bind::<Integer, _>(count)
    .execute(conn)
    .await
    .expect("pad history");
}

async fn enqueue_workflow_task(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    queue_mod::enqueue(conn, &params).await.expect("enqueue");
}

/// A payload that Postgres cannot compress much: random hex text.
fn incompressible_payload(approx_bytes: usize) -> serde_json::Value {
    let mut text = String::with_capacity(approx_bytes);
    while text.len() < approx_bytes {
        text.push_str(&Uuid::new_v4().simple().to_string());
    }
    serde_json::json!({ "blob": text })
}

#[derive(QueryableByName)]
struct Bytes {
    #[diesel(sql_type = BigInt)]
    bytes: i64,
}

async fn stored_history_bytes(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> u64 {
    let row: Bytes = diesel::sql_query(
        "SELECT COALESCE(SUM(pg_column_size(event_data)), 0)::bigint AS bytes \
         FROM harvest_events WHERE workflow_exec_id = $1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("sum stored history bytes");
    u64::try_from(row.bytes).expect("non-negative byte sum")
}

// ---------------------------------------------------------------------------
// Polling helpers
// ---------------------------------------------------------------------------

async fn load_execution(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect for reload");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load execution")
}

async fn wait_until<F>(
    database_url: &str,
    exec_id: ExecutionId,
    what: &str,
    done: F,
) -> WorkflowExecution
where
    F: Fn(&WorkflowExecution) -> bool + Send + Sync,
{
    let polled = tokio::time::timeout(LARGE_HISTORY_WAIT, async {
        loop {
            let execution = load_execution(database_url, exec_id).await;
            if done(&execution) {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    if let Ok(execution) = polled {
        return execution;
    }
    let execution = load_execution(database_url, exec_id).await;
    panic!(
        "timed out waiting for {what}; state={} error={:?}",
        execution.state, execution.error
    );
}

async fn signal_received_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> usize {
    store::load_history(conn, exec_id)
        .await
        .expect("load history")
        .events
        .iter()
        .filter(|event| {
            matches!(event, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "grow")
        })
        .count()
}

/// The typed reason on the run's DLQ row.
async fn dead_letter_reason(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> DeadLetterReason {
    let rows = dlq::list_dead_letters(conn, 50, None)
        .await
        .expect("list DLQ rows");
    let row = rows
        .iter()
        .find(|row| row.workflow_exec_id == Some(exec_id.as_uuid()))
        .expect("the capped run must have a DLQ row");
    serde_json::from_str(&row.error)
        .unwrap_or_else(|error| panic!("DLQ error is not a typed reason ({error}): {}", row.error))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// AC1 (RED before #1804): with no cap configured, a run at 50,000 events
/// fails with the typed `HistoryCapExceeded` reason. Before the default
/// cap, the run stayed RUNNING and kept growing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_event_cap_fails_a_run_at_fifty_thousand_events() {
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn).await;
    // WorkflowStarted + 49,999 padding = 50,000 durable events.
    pad_history(&mut conn, exec_id, 49_999).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let worker = build_worker(
        "history-default-event-cap",
        registry(WorkflowHistoryPolicy::default(), Arc::clone(&metrics)),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });

    let execution = wait_until(&database_url, exec_id, "the default event cap", |ex| {
        ex.state != "RUNNING"
    })
    .await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(
        execution.state, "FAILED",
        "the default cap must fail the run"
    );
    match dead_letter_reason(&mut conn, exec_id).await {
        DeadLetterReason::HistoryCapExceeded {
            count,
            cap,
            workflow_type,
        } => {
            assert_eq!(cap, 50_000, "the default event cap is 50,000");
            assert!(count >= cap, "count {count} must reach the cap");
            assert_eq!(workflow_type, WORKFLOW_NAME);
        }
        other => panic!("expected HistoryCapExceeded, got {other:?}"),
    }
}

/// AC2 (RED before #1804): with no cap configured, the early-warning metric
/// fires once a still-running history reaches 10,000 events. Before the
/// default cap, nothing fired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_warning_fires_at_ten_thousand_events() {
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn).await;
    // WorkflowStarted + 9,999 padding = 10,000 durable events.
    pad_history(&mut conn, exec_id, 9_999).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let worker = build_worker(
        "history-default-warning",
        registry(WorkflowHistoryPolicy::default(), Arc::clone(&metrics)),
    );
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });

    let execution = wait_until(&database_url, exec_id, "the default warning", |ex| {
        ex.history_bloat_warned_at.is_some() || ex.state != "RUNNING"
    })
    .await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(execution.state, "RUNNING", "a warning never ends the run");
    assert!(execution.history_bloat_warned_at.is_some());
    let warned = metrics.history_bloat.lock().unwrap().clone();
    assert_eq!(
        warned,
        vec![WORKFLOW_NAME.to_owned()],
        "{METRIC_WORKFLOW_HISTORY_BLOAT} must fire once"
    );
}

/// A run whose stored history reaches the byte cap fails with the typed
/// `HistoryBytesCapExceeded` reason. The signals arrive one at a time, so
/// later decisions take the warm cache path and add only the new bytes.
///
/// The small signals between the two large ones guard against a double
/// count: with one, 40 KiB counts twice and the run fails too early.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_cap_fails_a_run_on_the_warm_cache_path() {
    const CAP: u64 = 64 * 1024;
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let policy = WorkflowHistoryPolicy::default().with_byte_hard_cap(CAP);
    let worker = build_worker("history-byte-cap", registry(policy, Arc::clone(&metrics)));
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });

    let send = async |conn: &mut AsyncPgConnection, payload: serde_json::Value, expect: usize| {
        autumn_harvest::signal::send_signal(conn, exec_id, "grow", payload)
            .await
            .expect("send grow signal");
        tokio::time::timeout(LARGE_HISTORY_WAIT, async {
            while signal_received_count(conn, exec_id).await < expect {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("signal was never ingested");
    };

    // About 40 KiB stored: below the cap.
    send(&mut conn, incompressible_payload(40 * 1024), 1).await;
    send(&mut conn, serde_json::json!({}), 2).await;
    send(&mut conn, serde_json::json!({}), 3).await;
    // Let the last decision finish before the state check.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let below = stored_history_bytes(&mut conn, exec_id).await;
    assert!(below < CAP, "setup: {below} bytes must stay below {CAP}");
    let execution = load_execution(&database_url, exec_id).await;
    assert_eq!(
        execution.state, "RUNNING",
        "{below} stored bytes are below the cap; error={:?}",
        execution.error
    );

    // The second large signal crosses the cap.
    send(&mut conn, incompressible_payload(40 * 1024), 4).await;
    let execution = wait_until(&database_url, exec_id, "the byte cap", |ex| {
        ex.state != "RUNNING"
    })
    .await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(execution.state, "FAILED");
    assert!(
        *metrics.cache_hits.lock().unwrap() >= 1,
        "the follow-up decisions must take the warm cache path"
    );
    match dead_letter_reason(&mut conn, exec_id).await {
        DeadLetterReason::HistoryBytesCapExceeded {
            bytes,
            cap,
            workflow_type,
        } => {
            assert_eq!(cap, CAP);
            assert!(bytes >= cap, "bytes {bytes} must reach the cap");
            assert_eq!(workflow_type, WORKFLOW_NAME);
        }
        other => panic!("expected HistoryBytesCapExceeded, got {other:?}"),
    }
}

/// An explicit "unlimited" keeps the pre-#1804 behaviour: a run past the
/// default event cap keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlimited_caps_let_a_run_grow_past_the_defaults() {
    let (database_url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = seed_execution(&mut conn).await;
    pad_history(&mut conn, exec_id, 49_999).await;
    enqueue_workflow_task(&mut conn, exec_id).await;

    let metrics = Arc::new(RecordingMetrics::default());
    let policy = WorkflowHistoryPolicy::default()
        .without_event_hard_cap()
        .without_byte_hard_cap();
    let worker = build_worker("history-unlimited", registry(policy, Arc::clone(&metrics)));
    let pool = build_test_pool(&database_url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });

    // A signal past the default cap: the run ingests it and stays RUNNING.
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "grow", serde_json::json!({}))
        .await
        .expect("send grow signal");
    tokio::time::timeout(LARGE_HISTORY_WAIT, async {
        while signal_received_count(&mut conn, exec_id).await < 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("signal was never ingested");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let execution = load_execution(&database_url, exec_id).await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(
        execution.state, "RUNNING",
        "unlimited caps must not fail the run; error={:?}",
        execution.error
    );
    assert!(metrics.history_bloat.lock().unwrap().is_empty());
}
