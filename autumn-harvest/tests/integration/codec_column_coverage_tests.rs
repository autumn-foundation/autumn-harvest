#![cfg(feature = "db")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

//! End-to-end proof that the codec covers the execution, signal and
//! dead-letter columns (issue #1979).
//!
//! Each test runs a real worker against Postgres with an AES-256-GCM codec.
//! It checks two facts. The stored column holds ciphertext, not the secret.
//! The engine still reads plaintext, so the workflow sees its real input and
//! signal, and the caller gets the real result.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::dlq::{self, NewDeadLetterEntry};
use autumn_harvest::handle::WorkflowHandleClient;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::payload_codec::{PayloadCodecs, is_codec_envelope};
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::telemetry::TelemetryConfig;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{ExecutionId, ShardId, StartWorkflowParams, WorkflowContext};

use crate::integration_e2e::setup_test_database_url_or_env;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use uuid::Uuid;

const INPUT_SECRET: &str = "cc-input-ssn-123-45-6789";
const MEMO_SECRET: &str = "cc-memo-account-0042";
const SIGNAL_SECRET: &str = "cc-signal-pin-4321";
const DLQ_SECRET: &str = "cc-dlq-email-alice";

type BoxFut<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

// ── harness ──────────────────────────────────────────────────────────────────

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect failed")
}

/// An AES-256-GCM registry. `columns` turns column encoding on.
fn aead_codecs(columns: bool) -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let key = DataKey::from_bytes(&[0x5a; 32]).expect("data key");
    AeadCodec::new("cc-k1", &key)
        .expect("aead codec")
        .register_with(&codecs)
        .expect("register");
    codecs.set_column_encoding(columns);
    codecs
}

fn wf_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
    retry_policy: Option<RetryPolicy>,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "codec_column_coverage_tests",
        handler,
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
        retry_policy,
    }
}

fn activity_info(
    name: &'static str,
    queue: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "codec_column_coverage_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(queue),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

fn registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    codecs: &PayloadCodecs,
) -> Arc<HandlerRegistry> {
    Arc::new(
        HandlerRegistry::with_state_and_telemetry(
            workflows,
            activities,
            autumn_harvest::context::empty_shared_state(),
            Arc::new(TelemetryConfig::default()),
        )
        .with_payload_codecs(codecs.clone()),
    )
}

fn worker(queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: Uuid::new_v4().to_string(),
                queues: vec![queue.to_string()],
                queue_weights: HashMap::new(),
                notification_database_url: None,
                shard_notification_database_urls: Vec::new(),
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::ZERO,
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 100,
                resident_workflows: true,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                labels: HashMap::new(),
                sharded_pool: None,
                max_workflow_history_events: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

async fn start(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    workflow_name: &'static str,
    queue: &str,
    retry_policy: Option<RetryPolicy>,
) -> ExecutionId {
    let workflow_id = Uuid::new_v4().to_string();
    let params = StartWorkflowParams {
        memo: Some(json!({"account": MEMO_SECRET})),
        workflow_retry_policy: retry_policy,
        ..StartWorkflowParams::new(
            workflow_name,
            &workflow_id,
            ExecutionId::new_for_shard(ShardId::new(0)),
            json!({"ssn": INPUT_SECRET}),
            queue,
        )
    };
    autumn_harvest::execution::start_or_load_workflow_execution_with_codecs(
        conn, params, None, codecs,
    )
    .await
    .expect("start")
    .exec_id
}

async fn load_execution(conn: &mut AsyncPgConnection, exec_id: Uuid) -> WorkflowExecution {
    use autumn_harvest::schema::harvest_workflow_executions;
    harvest_workflow_executions::table
        .find(exec_id)
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
}

async fn signal_payloads(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Vec<Value> {
    use autumn_harvest::schema::harvest_signals;
    harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(exec_id))
        .select(harvest_signals::payload)
        .load(conn)
        .await
        .expect("load signals")
}

/// Assert that `stored` is an envelope that does not hold `secret`.
fn assert_ciphertext(stored: &Value, secret: &str, what: &str) {
    assert!(
        is_codec_envelope(stored),
        "{what} must be an envelope: {stored}"
    );
    assert!(
        !stored.to_string().contains(secret),
        "{what} must not hold the secret in clear"
    );
}

// ── handlers ─────────────────────────────────────────────────────────────────

fn echo_activity(_ctx: &autumn_harvest::ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

/// Waits for `go`, echoes its input through an activity, and returns all three.
fn wf_signal_echo(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let signal: Value = ctx.receive_signal("go").await.map_err(|e| e.to_string())?;
        let queue = ctx.queue_name().to_string();
        let echoed = ctx
            .execute_activity_raw("cc_echo", input.clone(), &queue)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"input": input, "signal": signal, "echoed": echoed}))
    })
}

static RETRY_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Fails on its first call. On the retry it returns its input only when the
/// input is the real plaintext.
fn wf_retry_once(_ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        if RETRY_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err("transient".to_string());
        }
        if input != json!({"ssn": INPUT_SECRET}) {
            return Err(format!("the retry saw a wrong input: {input}"));
        }
        Ok(input)
    })
}

// ── tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn covered_columns_hold_ciphertext_and_the_engine_reads_plaintext() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let queue: &'static str = Box::leak(queue.into_boxed_str());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, "cc_signal_echo", queue, None).await;

    let execution = load_execution(&mut conn, exec_id.as_uuid()).await;
    assert_ciphertext(&execution.input, INPUT_SECRET, "executions.input");
    assert_ciphertext(
        execution.memo.as_ref().expect("memo"),
        MEMO_SECRET,
        "executions.memo",
    );

    autumn_harvest::signal::send_signal_with_codecs(
        &mut conn,
        exec_id,
        "go",
        json!({"pin": SIGNAL_SECRET}),
        &codecs,
    )
    .await
    .expect("signal");
    for payload in signal_payloads(&mut conn, exec_id.as_uuid()).await {
        assert_ciphertext(&payload, SIGNAL_SECRET, "signals.payload");
    }

    let pool = build_pool(&url);
    let worker = worker(
        queue,
        registry(
            vec![wf_info("cc_signal_echo", wf_signal_echo, None)],
            vec![activity_info("cc_echo", queue, echo_activity)],
            &codecs,
        ),
    );
    let run = worker.clone();
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), run.run(&run_pool)).await;
    });

    let client = WorkflowHandleClient::single(pool, url.clone()).with_codecs(codecs.clone());
    let result = client
        .handle(exec_id)
        .result_raw_with_timeout(Duration::from_secs(25))
        .await
        .expect("the workflow completes");
    worker.shutdown();
    let _ = handle.await;

    assert_eq!(
        result,
        json!({
            "input": {"ssn": INPUT_SECRET},
            "signal": {"pin": SIGNAL_SECRET},
            "echoed": {"ssn": INPUT_SECRET},
        }),
        "the workflow saw plaintext input and signal, and the caller got plaintext"
    );

    let mut execution = load_execution(&mut conn, exec_id.as_uuid()).await;
    assert_ciphertext(
        execution.output.as_ref().expect("output"),
        INPUT_SECRET,
        "executions.output",
    );
    execution.decode_columns(&codecs).expect("decode columns");
    assert_eq!(execution.input, json!({"ssn": INPUT_SECRET}));
    assert_eq!(execution.memo, Some(json!({"account": MEMO_SECRET})));
    assert_eq!(execution.output, Some(result));
}

#[tokio::test]
async fn columns_stay_in_clear_while_column_encoding_is_off() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let codecs = aead_codecs(false);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, "cc_signal_echo", &queue, None).await;
    autumn_harvest::signal::send_signal_with_codecs(
        &mut conn,
        exec_id,
        "go",
        json!({"pin": SIGNAL_SECRET}),
        &codecs,
    )
    .await
    .expect("signal");

    let execution = load_execution(&mut conn, exec_id.as_uuid()).await;
    assert_eq!(execution.input, json!({"ssn": INPUT_SECRET}));
    assert_eq!(execution.memo, Some(json!({"account": MEMO_SECRET})));
    assert_eq!(
        signal_payloads(&mut conn, exec_id.as_uuid()).await,
        vec![json!({"pin": SIGNAL_SECRET})]
    );
}

#[tokio::test]
async fn a_dead_letter_input_is_ciphertext_and_replay_requeues_plaintext() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let entry = NewDeadLetterEntry {
        original_task_id: Uuid::new_v4(),
        queue_name: format!("cc-{}", Uuid::new_v4().simple()),
        task_type: "ACTIVITY".to_string(),
        workflow_exec_id: None,
        activity_name: Some("cc_echo".to_string()),
        input: json!({"email": DLQ_SECRET}),
        error: "boom".to_string(),
        attempts: 1,
        owner: None,
        severity: None,
    };
    let dead_letter_id = dlq::dead_letter_with_codecs(&mut conn, &entry, &codecs)
        .await
        .expect("dead letter");
    let stored: Value = {
        use autumn_harvest::schema::harvest_dead_letters;
        harvest_dead_letters::table
            .find(dead_letter_id)
            .select(harvest_dead_letters::input)
            .first(&mut conn)
            .await
            .expect("load dead letter")
    };
    assert_ciphertext(&stored, DLQ_SECRET, "dead_letters.input");

    let registry = registry(Vec::new(), Vec::new(), &codecs);
    let task_id = dlq::replay_dead_letter(&mut conn, dead_letter_id, Some(&registry))
        .await
        .expect("replay");
    let requeued: Value = {
        use autumn_harvest::schema::harvest_task_queue;
        harvest_task_queue::table
            .find(task_id)
            .select(harvest_task_queue::input)
            .first(&mut conn)
            .await
            .expect("load task")
    };
    assert_eq!(
        requeued,
        json!({"email": DLQ_SECRET}),
        "the task queue holds the plaintext the worker runs on"
    );
}

#[tokio::test]
async fn a_workflow_retry_runs_on_the_plaintext_input() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let policy = RetryPolicy {
        max_attempts: 2,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(10),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    };
    let exec_id = start(
        &mut conn,
        &codecs,
        "cc_retry_once",
        &queue,
        Some(policy.clone()),
    )
    .await;

    let pool = build_pool(&url);
    let worker = worker(
        &queue,
        registry(
            vec![wf_info("cc_retry_once", wf_retry_once, Some(policy))],
            Vec::new(),
            &codecs,
        ),
    );
    let run = worker.clone();
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), run.run(&run_pool)).await;
    });

    let client = WorkflowHandleClient::single(pool, url.clone()).with_codecs(codecs.clone());
    let result = client
        .handle(exec_id)
        .result_raw_with_timeout(Duration::from_secs(25))
        .await
        .expect("the retry completes");
    worker.shutdown();
    let _ = handle.await;
    assert_eq!(result, json!({"ssn": INPUT_SECRET}));

    let retry_input: Value = {
        use autumn_harvest::schema::harvest_workflow_executions;
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::retry_of_exec_id.eq(Some(exec_id.as_uuid())))
            .select(harvest_workflow_executions::input)
            .first(&mut conn)
            .await
            .expect("load the retry row")
    };
    assert_ciphertext(&retry_input, INPUT_SECRET, "the retry row input");
}
