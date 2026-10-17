#![cfg(feature = "db")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::default_trait_access
)]

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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::dlq::{self, NewDeadLetterEntry};
use autumn_harvest::handle::WorkflowHandleClient;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::payload_codec::{
    CodecError, PayloadCodec, PayloadCodecs, codec_envelope_key_id, is_codec_envelope,
};
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
const HEARTBEAT_SECRET: &str = "cc-heartbeat-cursor-77";

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
        input_schema: None,
        output_schema: None,
    }
}

fn registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    codecs: &PayloadCodecs,
) -> Arc<HandlerRegistry> {
    registry_with_telemetry(
        workflows,
        activities,
        codecs,
        Arc::new(TelemetryConfig::default()),
    )
}

fn registry_with_telemetry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    codecs: &PayloadCodecs,
    telemetry: Arc<TelemetryConfig>,
) -> Arc<HandlerRegistry> {
    Arc::new(
        HandlerRegistry::with_state_and_telemetry(
            workflows,
            activities,
            autumn_harvest::context::empty_shared_state(),
            telemetry,
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
                max_concurrent_claims: autumn_harvest::worker::DEFAULT_MAX_CONCURRENT_CLAIMS,
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
                fairness_keys: false,
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

/// Every task row of one execution: `(task_type, input, output)`.
async fn task_rows(
    conn: &mut AsyncPgConnection,
    exec_id: Uuid,
) -> Vec<(String, Value, Option<Value>)> {
    use autumn_harvest::schema::harvest_task_queue;
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id)))
        .select((
            harvest_task_queue::task_type,
            harvest_task_queue::input,
            harvest_task_queue::output,
        ))
        .load(conn)
        .await
        .expect("load task rows")
}

/// Assert that `stored` is an envelope under the test key that does not hold
/// `secret`. An identity envelope would pass a bare envelope check.
fn assert_ciphertext(stored: &Value, secret: &str, what: &str) {
    assert!(
        is_codec_envelope(stored),
        "{what} must be an envelope: {stored}"
    );
    assert_eq!(
        codec_envelope_key_id(stored),
        Some("cc-k1"),
        "{what} must be under the test key"
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

/// Runs one child workflow and returns its result.
fn wf_parent(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("cc_child", input)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Returns its input only when the input is the real plaintext.
fn wf_child(_ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        if input != json!({"ssn": INPUT_SECRET}) {
            return Err(format!("the child saw a wrong input: {input}"));
        }
        Ok(json!({"child": input}))
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
    let payloads = signal_payloads(&mut conn, exec_id.as_uuid()).await;
    assert_eq!(payloads.len(), 1, "exactly one signal row");
    assert_ciphertext(&payloads[0], SIGNAL_SECRET, "signals.payload");

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

    // Every task row holds envelopes, activity rows included (issue #2043).
    let rows = task_rows(&mut conn, exec_id.as_uuid()).await;
    assert!(
        rows.iter().any(|(task_type, _, _)| task_type == "workflow"),
        "the run has a workflow task row"
    );
    assert!(
        rows.iter().any(|(task_type, _, _)| task_type == "activity"),
        "the run has an activity task row"
    );
    for (task_type, input, output) in rows {
        assert_ciphertext(&input, INPUT_SECRET, &format!("{task_type} task input"));
        assert_ciphertext(
            output.as_ref().expect("task output"),
            INPUT_SECRET,
            &format!("{task_type} task output"),
        );
    }
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

/// A durable promise settlement is a signal row, so it holds ciphertext too.
/// The dedupe key stays in clear, so a second settlement is still refused.
#[tokio::test]
async fn a_promise_settlement_with_codecs_is_ciphertext() {
    use autumn_harvest::durable_promise::{self, PromiseId};
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, "cc_signal_echo", &queue, None).await;
    let approve = PromiseId::new(exec_id, "approve").expect("promise id");
    let deny = PromiseId::new(exec_id, "deny").expect("promise id");

    let value = json!({"pin": SIGNAL_SECRET});
    assert!(
        durable_promise::resolve_with_codecs(&mut conn, &approve, value.clone(), &codecs)
            .await
            .expect("resolve")
    );
    assert!(
        durable_promise::reject_with_codecs(&mut conn, &deny, SIGNAL_SECRET, &codecs)
            .await
            .expect("reject")
    );
    assert!(
        !durable_promise::resolve_with_codecs(&mut conn, &approve, value, &codecs)
            .await
            .expect("second resolve"),
        "the first settlement wins"
    );

    let payloads = signal_payloads(&mut conn, exec_id.as_uuid()).await;
    assert_eq!(payloads.len(), 2, "one row per promise");
    for payload in &payloads {
        assert_ciphertext(payload, SIGNAL_SECRET, "promise settlement");
    }
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
    assert_ciphertext(&requeued, DLQ_SECRET, "the requeued activity input");
    assert_eq!(
        codecs.decode_column(&requeued).expect("decode"),
        json!({"email": DLQ_SECRET}),
        "the requeued row decodes to the plaintext the worker runs on"
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
    let retry_memo: Option<Value> = {
        use autumn_harvest::schema::harvest_workflow_executions;
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::retry_of_exec_id.eq(Some(exec_id.as_uuid())))
            .select(harvest_workflow_executions::memo)
            .first(&mut conn)
            .await
            .expect("load the retry memo")
    };
    let retry_memo = retry_memo.expect("the retry keeps the memo");
    assert_ciphertext(&retry_memo, MEMO_SECRET, "the retry row memo");
    assert_eq!(
        codecs.decode_column(&retry_memo).expect("decode memo"),
        json!({"account": MEMO_SECRET}),
        "the retry memo decodes once to plaintext, not to another envelope"
    );
}

#[tokio::test]
async fn a_child_workflow_runs_on_plaintext_and_its_rows_hold_ciphertext() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, "cc_parent", &queue, None).await;

    let pool = build_pool(&url);
    let worker = worker(
        &queue,
        registry(
            vec![
                wf_info("cc_parent", wf_parent, None),
                wf_info("cc_child", wf_child, None),
            ],
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
        .expect("the parent completes");
    worker.shutdown();
    let _ = handle.await;
    assert_eq!(
        result,
        json!({"child": {"ssn": INPUT_SECRET}}),
        "the child saw plaintext and the parent got the plaintext child result"
    );

    let child: WorkflowExecution = {
        use autumn_harvest::schema::harvest_workflow_executions;
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::parent_id.eq(Some(exec_id.as_uuid())))
            .select(WorkflowExecution::as_select())
            .first(&mut conn)
            .await
            .expect("load the child row")
    };
    assert_ciphertext(&child.input, INPUT_SECRET, "child executions.input");
    assert_ciphertext(
        child.output.as_ref().expect("child output"),
        INPUT_SECRET,
        "child executions.output",
    );
    for (task_type, input, _) in task_rows(&mut conn, child.id).await {
        assert_eq!(task_type, "workflow");
        assert_ciphertext(&input, INPUT_SECRET, "child workflow task input");
    }
}

#[tokio::test]
async fn an_envelope_shaped_activity_input_survives_a_dead_letter_round_trip() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    // An activity argument that only looks like an identity envelope. The task
    // row stores it as the enqueue writes it, and the dead letter takes that
    // stored form.
    let shaped = json!({
        autumn_harvest::payload_codec::CODEC_ENVELOPE_KEY: 1,
        "codec_id": "identity",
        "data": "e30=",
    });
    let stored = codecs.encode_column(&shaped).expect("encode");
    let entry = NewDeadLetterEntry {
        original_task_id: Uuid::new_v4(),
        queue_name: format!("cc-{}", Uuid::new_v4().simple()),
        task_type: "ACTIVITY".to_string(),
        workflow_exec_id: None,
        activity_name: Some("cc_echo".to_string()),
        input: stored,
        error: "boom".to_string(),
        attempts: 1,
        owner: None,
        severity: None,
    };
    let dead_letter_id = dlq::dead_letter_with_codecs(&mut conn, &entry, &codecs)
        .await
        .expect("dead letter");
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
        codecs.decode_column(&requeued).expect("decode"),
        shaped,
        "the activity argument is replayed exactly as it was, never unwrapped"
    );
}

// ── activity heartbeat checkpoint (issue #2043) ──────────────────────────────

static HEARTBEAT_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static HEARTBEAT_STORED: AtomicBool = AtomicBool::new(false);

/// The first attempt sends a checkpoint, waits until the test has read the
/// stored checkpoint, and fails. The retry returns the checkpoint it reads.
fn heartbeat_activity(ctx: &autumn_harvest::ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        if HEARTBEAT_ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            ctx.heartbeat(json!({"cursor": HEARTBEAT_SECRET}))
                .await
                .map_err(|e| e.to_string())?;
            for _ in 0..1200 {
                if AtomicBool::load(&HEARTBEAT_STORED, Ordering::SeqCst) {
                    return Err("retry after a checkpoint".to_string());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            return Err("the test never read the stored checkpoint".to_string());
        }
        let seen: Option<Value> = ctx.heartbeat_details().map_err(|e| e.to_string())?;
        Ok(json!({"seen": seen}))
    })
}

fn wf_heartbeat(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_interval: Duration::from_millis(10),
            backoff_coefficient: 1.0,
            max_interval: Duration::from_millis(10),
            non_retryable_errors: vec![],
            jitter: JitterPolicy::None,
        };
        ctx.execute_activity_raw_with_opts("cc_heartbeat", input, &queue, Some(policy), None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// The stored checkpoint of the run's activity task row now, if any.
async fn peek_heartbeat(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Option<Value> {
    use autumn_harvest::schema::harvest_task_queue;
    let stored: Option<Option<Value>> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id)))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(harvest_task_queue::heartbeat_details)
        .first(conn)
        .await
        .optional()
        .expect("load heartbeat");
    stored.flatten().filter(|value| !value.is_null())
}

/// The stored checkpoint of the run's activity task row, once the flusher
/// has written one.
async fn stored_heartbeat(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Option<Value> {
    for _ in 0..600 {
        if let Some(value) = peek_heartbeat(conn, exec_id).await {
            return Some(value);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

#[tokio::test]
async fn a_heartbeat_checkpoint_is_ciphertext_and_the_retry_reads_plaintext() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let queue: &'static str = Box::leak(queue.into_boxed_str());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, "cc_heartbeat_wf", queue, None).await;

    let pool = build_pool(&url);
    let worker = worker(
        queue,
        registry(
            vec![wf_info("cc_heartbeat_wf", wf_heartbeat, None)],
            vec![activity_info("cc_heartbeat", queue, heartbeat_activity)],
            &codecs,
        ),
    );
    let run = worker.clone();
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(40), run.run(&run_pool)).await;
    });

    let stored = stored_heartbeat(&mut conn, exec_id.as_uuid())
        .await
        .expect("the flusher writes the checkpoint");
    assert_ciphertext(&stored, HEARTBEAT_SECRET, "task_queue.heartbeat_details");
    HEARTBEAT_STORED.store(true, Ordering::SeqCst);

    let client = WorkflowHandleClient::single(pool, url.clone()).with_codecs(codecs.clone());
    let result = client
        .handle(exec_id)
        .result_raw_with_timeout(Duration::from_secs(30))
        .await
        .expect("the retry completes");
    worker.shutdown();
    let _ = handle.await;
    assert_eq!(
        result,
        json!({"seen": {"cursor": HEARTBEAT_SECRET}}),
        "the retry reads the plaintext checkpoint"
    );
}

// ── a checkpoint that fails to encode (issue #2043) ──────────────────────────

static FLAKY_FAILING: AtomicBool = AtomicBool::new(false);
static FLAKY_FAILURES: AtomicUsize = AtomicUsize::new(0);
static FLAKY_DONE: AtomicBool = AtomicBool::new(false);

/// An AES-256-GCM codec that fails to encode while `failing` is set. Each
/// failure adds one to `failures`.
struct FlakyAead {
    inner: AeadCodec,
    failing: &'static AtomicBool,
    failures: &'static AtomicUsize,
}

impl PayloadCodec for FlakyAead {
    fn codec_id(&self) -> &'static str {
        self.inner.codec_id()
    }

    fn bound_key_id(&self) -> Option<&str> {
        self.inner.bound_key_id()
    }

    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        if AtomicBool::load(self.failing, Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(CodecError("simulated encode failure".to_string()));
        }
        self.inner.encode(raw)
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        self.inner.decode(encoded)
    }
}

/// A registry whose codec fails to encode while `failing` is set. Column
/// encoding is on.
fn flaky_codecs(failing: &'static AtomicBool, failures: &'static AtomicUsize) -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let key = DataKey::from_bytes(&[0x5a; 32]).expect("data key");
    codecs
        .register_key(
            "cc-k1",
            Arc::new(FlakyAead {
                inner: AeadCodec::new("cc-k1", &key).expect("aead codec"),
                failing,
                failures,
            }),
        )
        .expect("register");
    codecs.set_column_encoding(true);
    codecs
}

/// Sends one checkpoint while the codec fails, then waits for the test.
fn flaky_heartbeat_activity(ctx: &autumn_harvest::ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        AtomicBool::store(&FLAKY_FAILING, true, Ordering::SeqCst);
        ctx.heartbeat(json!({"cursor": HEARTBEAT_SECRET}))
            .await
            .map_err(|e| e.to_string())?;
        for _ in 0..1200 {
            if AtomicBool::load(&FLAKY_DONE, Ordering::SeqCst) {
                return Ok(json!({"done": true}));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Err("the test never read the stored checkpoint".to_string())
    })
}

fn wf_flaky_heartbeat(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("cc_flaky_heartbeat", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

#[tokio::test]
async fn a_checkpoint_that_fails_to_encode_is_retried_on_the_next_flush() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue: &'static str = Box::leak(format!("cc-{}", Uuid::new_v4().simple()).into_boxed_str());
    let codecs = flaky_codecs(&FLAKY_FAILING, &FLAKY_FAILURES);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, "cc_flaky_heartbeat_wf", queue, None).await;

    let pool = build_pool(&url);
    let worker = worker(
        queue,
        registry(
            vec![wf_info("cc_flaky_heartbeat_wf", wf_flaky_heartbeat, None)],
            vec![activity_info(
                "cc_flaky_heartbeat",
                queue,
                flaky_heartbeat_activity,
            )],
            &codecs,
        ),
    );
    let run = worker.clone();
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(60), run.run(&run_pool)).await;
    });

    for _ in 0..600 {
        if AtomicUsize::load(&FLAKY_FAILURES, Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        AtomicUsize::load(&FLAKY_FAILURES, Ordering::SeqCst) > 0,
        "the flusher must try to encode the checkpoint"
    );
    assert!(
        peek_heartbeat(&mut conn, exec_id.as_uuid()).await.is_none(),
        "a checkpoint that does not encode is not stored in clear"
    );

    // The activity sends no later heartbeat. Only a retry can store it.
    AtomicBool::store(&FLAKY_FAILING, false, Ordering::SeqCst);
    let stored = stored_heartbeat(&mut conn, exec_id.as_uuid())
        .await
        .expect("the flusher retries the checkpoint once the codec works");
    assert_ciphertext(&stored, HEARTBEAT_SECRET, "task_queue.heartbeat_details");
    assert_eq!(
        codecs.decode_column(&stored).expect("decode"),
        json!({"cursor": HEARTBEAT_SECRET}),
        "the retry stores the checkpoint the activity sent"
    );

    AtomicBool::store(&FLAKY_DONE, true, Ordering::SeqCst);
    let client = WorkflowHandleClient::single(pool, url.clone()).with_codecs(codecs.clone());
    client
        .handle(exec_id)
        .result_raw_with_timeout(Duration::from_secs(30))
        .await
        .expect("the workflow completes");
    worker.shutdown();
    let _ = handle.await;
}

// ── helpers for the short-lived copies (issue #2043) ─────────────────────────

#[derive(diesel::QueryableByName)]
struct JsonCell {
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    value: Value,
}

/// The execution a start created for `workflow_name` and `workflow_id`.
async fn started_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> WorkflowExecution {
    use autumn_harvest::schema::harvest_workflow_executions;
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("the start created an execution")
}

/// Assert that a started run holds its input as one envelope layer.
fn assert_one_layer(codecs: &PayloadCodecs, stored: &Value, expected: &Value, what: &str) {
    assert_ciphertext(stored, INPUT_SECRET, what);
    assert_eq!(
        &codecs.decode_column(stored).expect("decode"),
        expected,
        "{what} must decode once to the plaintext"
    );
}

// ── completion callbacks (issue #2043) ───────────────────────────────────────

const CALLBACK_SECRET: &str = "cc-callback-refund-9911";
const CALLBACK_HMAC_KEY: &[u8] = b"cc-callback-hmac-key";

type CapturedPost = (Vec<u8>, Vec<(&'static str, String)>);

/// A deliverer that records each POST and answers with a settable status.
struct CapturingDeliverer {
    status: std::sync::atomic::AtomicU16,
    posts: std::sync::Mutex<Vec<CapturedPost>>,
}

impl CapturingDeliverer {
    fn new(status: u16) -> Arc<Self> {
        Arc::new(Self {
            status: std::sync::atomic::AtomicU16::new(status),
            posts: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn answer_with(&self, status: u16) {
        self.status.store(status, Ordering::SeqCst);
    }

    fn posts(&self) -> Vec<CapturedPost> {
        self.posts.lock().unwrap().clone()
    }
}

impl autumn_harvest::completion_callback::CompletionCallbackDeliverer for CapturingDeliverer {
    fn deliver<'a>(
        &'a self,
        _target_url: &'a str,
        body: &'a [u8],
        headers: &'a [(&'static str, String)],
    ) -> autumn_harvest::completion_callback::DeliverFuture<'a> {
        Box::pin(async move {
            self.posts
                .lock()
                .unwrap()
                .push((body.to_vec(), headers.to_vec()));
            autumn_harvest::completion_callback::DeliveryAttempt {
                status: Some(std::sync::atomic::AtomicU16::load(
                    &self.status,
                    Ordering::SeqCst,
                )),
                transport_error: None,
                retry_after: None,
            }
        })
    }
}

/// Clears the process-wide callback config when a test ends.
struct CallbackConfigGuard;

impl Drop for CallbackConfigGuard {
    fn drop(&mut self) {
        *autumn_harvest::completion_callback::GLOBAL_CALLBACK_CONFIG
            .write()
            .unwrap() = None;
    }
}

fn install_callbacks(deliverer: Arc<CapturingDeliverer>, max_attempts: u32) -> CallbackConfigGuard {
    install_callbacks_with_backoff(deliverer, max_attempts, Duration::from_millis(10))
}

/// As [`install_callbacks`], with the first retry delay set to `initial`.
fn install_callbacks_with_backoff(
    deliverer: Arc<CapturingDeliverer>,
    max_attempts: u32,
    initial: Duration,
) -> CallbackConfigGuard {
    use autumn_harvest::completion_callback::{
        CallbackRuntimeConfig, CallbackSecret, GLOBAL_CALLBACK_CONFIG, HostAllowlist, SsrfPolicy,
    };
    *GLOBAL_CALLBACK_CONFIG.write().unwrap() = Some(Arc::new(CallbackRuntimeConfig {
        deliverer,
        secret: CallbackSecret::new(CALLBACK_HMAC_KEY.to_vec()),
        ssrf_policy: SsrfPolicy::new(HostAllowlist::new().with_pattern("api.example.com")),
        default_targets: Vec::new(),
        retry_policy: RetryPolicy::exponential(max_attempts, initial),
    }));
    CallbackConfigGuard
}

/// A completed run with one callback target. Its stored result is an
/// envelope, as the engine writes it.
async fn completed_run_with_callback(conn: &mut AsyncPgConnection, codecs: &PayloadCodecs) -> Uuid {
    diesel::sql_query("DELETE FROM harvest_dead_letters WHERE task_type = 'CALLBACK'")
        .execute(conn)
        .await
        .expect("clear callback dead letters");
    diesel::sql_query("DELETE FROM harvest_completion_deliveries")
        .execute(conn)
        .await
        .expect("clear deliveries");
    let exec_id = Uuid::new_v4();
    let output = codecs
        .encode_column(&json!({"refund": CALLBACK_SECRET}))
        .expect("encode output");
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, state, output, completed_at, \
          completion_callbacks) \
         VALUES ($1, 'cc_callback_wf', $2, 0, 'null'::jsonb, 'COMPLETED', $3, now(), $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .bind::<diesel::sql_types::Text, _>(Uuid::new_v4().to_string())
    .bind::<diesel::sql_types::Jsonb, _>(&output)
    .bind::<diesel::sql_types::Jsonb, _>(
        &json!([{"url": "https://api.example.com/hook", "filter": {"type": "AnyTerminal"}}]),
    )
    .execute(conn)
    .await
    .expect("insert the completed run");
    exec_id
}

async fn delivery_cells(conn: &mut AsyncPgConnection, exec_id: Uuid) -> (Uuid, Value) {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        payload: Value,
    }
    let row: Row = diesel::sql_query(
        "SELECT id, payload FROM harvest_completion_deliveries WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result(conn)
    .await
    .expect("the run has one delivery row");
    (row.id, row.payload)
}

async fn callback_dead_letter_input(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Value {
    let cell: JsonCell = diesel::sql_query(
        "SELECT input AS value FROM harvest_dead_letters \
         WHERE task_type = 'CALLBACK' AND workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result(conn)
    .await
    .expect("the run has one callback dead letter");
    cell.value
}

async fn evaluate_callbacks(conn: &mut AsyncPgConnection, exec_id: Uuid, codecs: &PayloadCodecs) {
    autumn_harvest::completion_trigger::evaluate_triggers_for_execution_with_codecs(
        conn,
        ExecutionId::from_uuid(exec_id),
        autumn_harvest::completion_trigger::TerminalState::Completed,
        None,
        codecs,
    )
    .await
    .expect("evaluate triggers");
}

#[tokio::test]
async fn a_callback_payload_is_ciphertext_and_the_post_body_is_plaintext() {
    use autumn_harvest::completion_callback::{
        CallbackSecret, SIGNATURE_HEADER, fire_due_completion_deliveries_with_codecs, sign,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let deliverer = CapturingDeliverer::new(200);
    let _config = install_callbacks(deliverer.clone(), 3);
    let mut conn = connect(&url).await;
    let exec_id = completed_run_with_callback(&mut conn, &codecs).await;
    evaluate_callbacks(&mut conn, exec_id, &codecs).await;

    let (_, stored) = delivery_cells(&mut conn, exec_id).await;
    assert_ciphertext(&stored, CALLBACK_SECRET, "completion_deliveries.payload");

    let fired = fire_due_completion_deliveries_with_codecs(&mut conn, &None, &[], &codecs)
        .await
        .expect("fire");
    assert_eq!(fired, 1, "one delivery was due");
    let posts = deliverer.posts();
    assert_eq!(posts.len(), 1, "one POST");
    let (body, headers) = &posts[0];
    let sent: Value = serde_json::from_slice(body).expect("the body is JSON");
    assert!(
        !is_codec_envelope(&sent) && sent.to_string().contains(CALLBACK_SECRET),
        "the receiver gets the plaintext body: {sent}"
    );
    let signature = headers
        .iter()
        .find(|(name, _)| *name == SIGNATURE_HEADER)
        .map(|(_, value)| value.clone())
        .expect("a signature header");
    assert_eq!(
        signature,
        sign(&CallbackSecret::new(CALLBACK_HMAC_KEY.to_vec()), body),
        "the signature covers the bytes that were sent"
    );
}

#[tokio::test]
async fn a_callback_dead_letter_is_ciphertext_and_a_redrive_posts_plaintext() {
    use autumn_harvest::completion_callback::{
        fire_due_completion_deliveries_with_codecs, redrive_delivery,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let deliverer = CapturingDeliverer::new(500);
    let _config = install_callbacks(deliverer.clone(), 1);
    let mut conn = connect(&url).await;
    let exec_id = completed_run_with_callback(&mut conn, &codecs).await;
    evaluate_callbacks(&mut conn, exec_id, &codecs).await;
    fire_due_completion_deliveries_with_codecs(&mut conn, &None, &[], &codecs)
        .await
        .expect("fire");

    let stored = callback_dead_letter_input(&mut conn, exec_id).await;
    assert_ciphertext(&stored, CALLBACK_SECRET, "dead_letters.input (CALLBACK)");
    let decoded = codecs.decode_column(&stored).expect("decode once");
    assert!(
        !is_codec_envelope(&decoded) && decoded.to_string().contains(CALLBACK_SECRET),
        "the dead letter holds one envelope layer: {decoded}"
    );

    let (delivery_id, _) = delivery_cells(&mut conn, exec_id).await;
    let posts_before = deliverer.posts().len();
    redrive_delivery(&mut conn, ExecutionId::from_uuid(exec_id), delivery_id)
        .await
        .expect("redrive");
    deliverer.answer_with(200);
    fire_due_completion_deliveries_with_codecs(&mut conn, &None, &[], &codecs)
        .await
        .expect("fire again");
    let posts = deliverer.posts();
    assert_eq!(posts.len(), posts_before + 1, "the redrive posts once");
    let (body, _) = posts.last().expect("a POST after the redrive");
    let sent: Value = serde_json::from_slice(body).expect("the body is JSON");
    assert!(
        !is_codec_envelope(&sent) && sent.to_string().contains(CALLBACK_SECRET),
        "the redrive posts the plaintext body: {sent}"
    );
}

// ── start buffers and the trigger outbox (issue #2043) ───────────────────────

#[tokio::test]
async fn a_debounced_input_is_ciphertext_and_the_fire_starts_on_plaintext() {
    use autumn_harvest::debounce::{
        AdmitDebounceParams, admit_debounced_start_with_codecs,
        fire_due_debounced_starts_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let workflow_id = format!("cc-debounce-{}", Uuid::new_v4().simple());
    let plain = json!({"ssn": INPUT_SECRET});
    admit_debounced_start_with_codecs(
        &mut conn,
        AdmitDebounceParams {
            workflow_name: "cc_debounce_wf",
            debounce_key: &key,
            workflow_id: &workflow_id,
            queue_name: "default",
            last_input: plain.clone(),
            start_options: Default::default(),
            window: Duration::from_millis(1),
            max_wait: Duration::from_secs(60),
            shard_id: 0,
        },
        false,
        &codecs,
    )
    .await
    .expect("admit")
    .expect("the gate is off, so the request is admitted");

    let stored: JsonCell = diesel::sql_query(
        "SELECT last_input AS value FROM harvest_debounce \
         WHERE workflow_name = 'cc_debounce_wf' AND debounce_key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&key)
    .get_result(&mut conn)
    .await
    .expect("load the debounce row");
    assert_ciphertext(&stored.value, INPUT_SECRET, "debounce.last_input");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let fired = fire_due_debounced_starts_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &codecs,
    )
    .await
    .expect("fire");
    assert!(fired >= 1, "the due row fired");
    let started = started_execution(&mut conn, "cc_debounce_wf", &workflow_id).await;
    assert_one_layer(&codecs, &started.input, &plain, "the debounced run input");
}

#[tokio::test]
async fn a_throttled_input_is_ciphertext_and_the_fire_starts_on_plaintext() {
    use autumn_harvest::throttle::{
        AdmitThrottleParams, ThrottleAdmission, fire_due_throttled_starts_with_codecs,
        reserve_or_defer_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let plain = json!({"ssn": INPUT_SECRET});
    let mut defer = async |workflow_id: &str| {
        reserve_or_defer_with_codecs(
            &mut conn,
            AdmitThrottleParams {
                workflow_name: "cc_throttle_wf",
                throttle_key: &key,
                workflow_id,
                queue_name: "default",
                input: plain.clone(),
                start_options: Default::default(),
                refill_per_sec: 0.001,
                burst: 1.0,
                schedule_to_start: None,
                shard_id: 0,
            },
            &codecs,
        )
        .await
        .expect("admit")
    };
    // The first request takes the only token. The second is deferred.
    assert!(matches!(
        defer("cc-throttle-first").await,
        ThrottleAdmission::Reserved { .. }
    ));
    let workflow_id = format!("cc-throttle-{}", Uuid::new_v4().simple());
    assert!(matches!(
        defer(&workflow_id).await,
        ThrottleAdmission::Deferred(_)
    ));

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        input: Value,
        #[diesel(sql_type = diesel::sql_types::Text)]
        bucket_key: String,
    }
    let row: Row = diesel::sql_query(
        "SELECT input, bucket_key FROM harvest_start_throttle WHERE workflow_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&workflow_id)
    .get_result(&mut conn)
    .await
    .expect("load the throttle row");
    assert_ciphertext(&row.input, INPUT_SECRET, "start_throttle.input");

    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens = 5, last_refilled_at = NOW() WHERE key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&row.bucket_key)
    .execute(&mut conn)
    .await
    .expect("refill the bucket");
    autumn_harvest::admission_gate::set_global_admission_gate_cache(None);
    let fired = fire_due_throttled_starts_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &codecs,
    )
    .await
    .expect("fire");
    assert!(fired >= 1, "the deferred row fired");
    let started = started_execution(&mut conn, "cc_throttle_wf", &workflow_id).await;
    assert_one_layer(&codecs, &started.input, &plain, "the throttled run input");
}

#[tokio::test]
async fn an_outbox_target_input_is_read_as_plaintext_when_the_relay_starts_the_run() {
    use autumn_harvest::models::NewCompletionTriggerOutboxDb;
    use autumn_harvest::schema::harvest_completion_trigger_outbox;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let plain = json!({"ssn": INPUT_SECRET});
    let workflow_id = format!("cc-outbox-{}", Uuid::new_v4().simple());
    diesel::insert_into(harvest_completion_trigger_outbox::table)
        .values(&NewCompletionTriggerOutboxDb {
            source_exec_id: Uuid::new_v4(),
            trigger_id: Uuid::new_v4(),
            target_shard: 0,
            target_workflow_name: "cc_outbox_wf".to_string(),
            target_workflow_id: workflow_id.clone(),
            target_input: codecs.encode_column(&plain).expect("encode"),
            queue_name: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: serde_json::to_value(autumn_harvest::types::Priority::default()).unwrap(),
            max_workflow_input_bytes: 1_000_000,
            tenant: None,
        })
        .execute(&mut conn)
        .await
        .expect("insert the outbox row");

    // The relay needs the pool of the target shard.
    let sharded = Some(autumn_harvest::shard::ShardedDbPool::single(build_pool(
        &url,
    )));
    autumn_harvest::completion_trigger::enforce_completion_triggers_outbox_with_codecs(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
        &sharded,
        &[ShardId::new(0)],
        &codecs,
    )
    .await
    .expect("relay");
    let started = started_execution(&mut conn, "cc_outbox_wf", &workflow_id).await;
    assert_one_layer(&codecs, &started.input, &plain, "the relayed run input");
}

static GATE_FAILING: AtomicBool = AtomicBool::new(false);
static GATE_FAILURES: AtomicUsize = AtomicUsize::new(0);

#[tokio::test]
async fn a_gated_debounce_admission_returns_the_gate_result_while_the_encoder_fails() {
    use autumn_harvest::debounce::{AdmitDebounceParams, admit_debounced_start_with_codecs};
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = flaky_codecs(&GATE_FAILING, &GATE_FAILURES);
    AtomicBool::store(&GATE_FAILING, true, Ordering::SeqCst);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let workflow_id = format!("cc-gated-{}", Uuid::new_v4().simple());
    let params = || AdmitDebounceParams {
        workflow_name: "cc_gated_debounce_wf",
        debounce_key: &key,
        workflow_id: &workflow_id,
        queue_name: "default",
        last_input: json!({"ssn": INPUT_SECRET}),
        start_options: Default::default(),
        window: Duration::from_millis(1),
        max_wait: Duration::from_secs(60),
        shard_id: 0,
    };

    let gated = admit_debounced_start_with_codecs(&mut conn, params(), true, &codecs)
        .await
        .expect("a gated admission gives the gate result, not a codec error");
    assert!(gated.is_none(), "the gate rejects the request");
    assert_eq!(
        AtomicUsize::load(&GATE_FAILURES, Ordering::SeqCst),
        0,
        "a rolled-back row needs no ciphertext"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_debounce \
             WHERE workflow_name = 'cc_gated_debounce_wf' AND debounce_key = $1",
            &key,
        )
        .await,
        0,
        "a gated admission leaves no row"
    );

    admit_debounced_start_with_codecs(&mut conn, params(), false, &codecs)
        .await
        .expect_err("an admitted request still needs the encoder");
}

// ── batch jobs (issue #2043) ─────────────────────────────────────────────────

#[tokio::test]
async fn a_batch_signal_payload_is_ciphertext_and_each_delivery_is_plaintext() {
    use autumn_harvest::batch::{
        BatchAction, BatchExecutorConfig, BatchFilter, BatchSubmission, run_executor_once,
        submit_batch_job_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let workflow_name: &'static str =
        Box::leak(format!("cc_batch_{}", Uuid::new_v4().simple()).into_boxed_str());
    let exec_id = start(&mut conn, &codecs, workflow_name, &queue, None).await;
    let plain = json!({"pin": SIGNAL_SECRET});
    let job_id = submit_batch_job_with_codecs(
        &mut conn,
        BatchSubmission {
            action: BatchAction::Signal,
            filter: BatchFilter {
                states: vec!["RUNNING".to_string()],
                workflow_name: Some(workflow_name.to_string()),
                search_attrs: Vec::new(),
            },
            signal_name: Some("go".to_string()),
            signal_payload: Some(plain.clone()),
            idempotency_key: None,
            created_by: None,
        },
        &codecs,
    )
    .await
    .expect("submit");

    let stored: JsonCell =
        diesel::sql_query("SELECT signal_payload AS value FROM harvest_batch_jobs WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(job_id)
            .get_result(&mut conn)
            .await
            .expect("load the batch job");
    assert_ciphertext(&stored.value, SIGNAL_SECRET, "batch_jobs.signal_payload");

    let pool = autumn_harvest::shard::ShardedDbPool::single(build_pool(&url));
    run_executor_once(
        &pool,
        &BatchExecutorConfig {
            payload_codecs: codecs.clone(),
            ..BatchExecutorConfig::default()
        },
    )
    .await
    .expect("run the executor");
    let payloads = signal_payloads(&mut conn, exec_id.as_uuid()).await;
    assert_eq!(payloads.len(), 1, "one signal row");
    assert_ciphertext(&payloads[0], SIGNAL_SECRET, "signals.payload");
    assert_eq!(
        codecs.decode_column(&payloads[0]).expect("decode"),
        plain,
        "the signal holds one envelope layer, and it decodes to the batch payload"
    );
}

// ── a value that does not decode never starts work (issue #2043) ─────────────

/// A registry that holds no codec key, so a stored envelope does not decode.
fn reader_without_the_key() -> PayloadCodecs {
    PayloadCodecs::default()
}

async fn count_rows(conn: &mut AsyncPgConnection, sql: &str, bind: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(sql)
        .bind::<diesel::sql_types::Text, _>(bind)
        .get_result(conn)
        .await
        .expect("count rows");
    row.n
}

static GUARDED_RUNS: AtomicUsize = AtomicUsize::new(0);

fn guarded_activity(_ctx: &autumn_harvest::ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        GUARDED_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    })
}

/// Runs the activity on a queue that another worker owns.
fn wf_guarded(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = format!("{}-act", ctx.queue_name());
        ctx.execute_activity_raw("cc_guarded", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Runs the activity on a queue that another worker owns, with a policy that
/// skips `PayloadDecodeFailed`.
fn wf_guarded_non_retryable(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = format!("{}-act", ctx.queue_name());
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_interval: Duration::from_millis(10),
            backoff_coefficient: 1.0,
            max_interval: Duration::from_millis(10),
            non_retryable_errors: vec!["PayloadDecodeFailed".to_string()],
            jitter: JitterPolicy::None,
        };
        ctx.execute_activity_raw_with_opts("cc_guarded", input, &queue, Some(policy), None)
            .await
            .map_err(|e| e.to_string())
    })
}

/// The `(error_type, non_retryable)` labels of the failed activity attempts.
#[derive(Default)]
struct FailureLabels(std::sync::Mutex<Vec<(String, bool)>>);

impl autumn_harvest::telemetry::MetricsRecorder for FailureLabels {
    fn record_activity_failed(
        &self,
        _activity_name: &str,
        _workflow_type: &str,
        error_type: &str,
        non_retryable: bool,
    ) {
        self.0
            .lock()
            .unwrap()
            .push((error_type.to_string(), non_retryable));
    }
}

/// Corrupts the activity task row of a run, then lets a second worker claim
/// it. Returns the failure labels that worker recorded.
async fn fail_a_corrupt_activity_row(
    workflow_name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
    corrupt_column: &'static str,
) -> Vec<(String, bool)> {
    let (url, _c) = setup_test_database_url_or_env().await;
    let wf_queue: &'static str =
        Box::leak(format!("cc-{}", Uuid::new_v4().simple()).into_boxed_str());
    let act_queue: &'static str = Box::leak(format!("{wf_queue}-act").into_boxed_str());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, workflow_name, wf_queue, None).await;
    let exec = exec_id.as_uuid().to_string();

    // The first worker owns the workflow queue. It schedules the activity on a
    // queue that no worker polls yet.
    let pool = build_pool(&url);
    let owner = worker(
        wf_queue,
        registry(
            vec![wf_info(workflow_name, handler, None)],
            vec![activity_info("cc_guarded", act_queue, guarded_activity)],
            &codecs,
        ),
    );
    let run_owner = owner.clone();
    let owner_pool = pool.clone();
    let owner_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(40), run_owner.run(&owner_pool)).await;
    });
    let sql = "SELECT COUNT(*)::BIGINT AS n FROM harvest_task_queue \
               WHERE task_type = 'activity' AND workflow_exec_id::TEXT = $1";
    let mut scheduled = 0;
    for _ in 0..600 {
        scheduled = count_rows(&mut conn, sql, &exec).await;
        if scheduled > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(scheduled, 1, "the workflow scheduled the activity");

    // History still decodes. Only the task row names a key that no registry
    // holds, as a corrupt cell would.
    diesel::sql_query(format!(
        "UPDATE harvest_task_queue SET {corrupt_column} = $2 \
         WHERE task_type = 'activity' AND workflow_exec_id::TEXT = $1"
    ))
    .bind::<diesel::sql_types::Text, _>(&exec)
    .bind::<diesel::sql_types::Jsonb, _>(&json!({
        autumn_harvest::payload_codec::CODEC_ENVELOPE_KEY:
            {"codec_id": "aes-256-gcm", "kid": "cc-gone", "data": "AAAA"}
    }))
    .execute(&mut conn)
    .await
    .expect("corrupt the task row");

    let labels = Arc::new(FailureLabels::default());
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(labels.clone()).build());
    let activity_worker = worker(
        act_queue,
        registry_with_telemetry(
            Vec::new(),
            vec![activity_info("cc_guarded", act_queue, guarded_activity)],
            &codecs,
            telemetry,
        ),
    );
    let run_activity = activity_worker.clone();
    let activity_pool = pool.clone();
    let activity_handle = tokio::spawn(async move {
        let _ =
            tokio::time::timeout(Duration::from_secs(40), run_activity.run(&activity_pool)).await;
    });

    let mut failed = 0;
    for _ in 0..800 {
        failed = count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_task_queue \
             WHERE task_type = 'activity' AND workflow_exec_id::TEXT = $1 \
               AND error LIKE '%PayloadDecodeFailed%'",
            &exec,
        )
        .await;
        if failed > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    owner.shutdown();
    activity_worker.shutdown();
    let _ = owner_handle.await;
    let _ = activity_handle.await;
    assert!(failed > 0, "the attempt fails with PayloadDecodeFailed");
    assert_eq!(
        AtomicUsize::load(&GUARDED_RUNS, Ordering::SeqCst),
        0,
        "the handler never runs on ciphertext"
    );
    labels.0.lock().unwrap().clone()
}

#[tokio::test]
async fn an_activity_that_cannot_decode_its_input_fails_the_attempt_and_never_runs() {
    let labels = fail_a_corrupt_activity_row("cc_guarded_wf", wf_guarded, "input").await;
    assert!(!labels.is_empty(), "the failed attempt is counted");
    assert!(
        labels.iter().all(
            |(error_type, non_retryable)| error_type == "PayloadDecodeFailed" && !non_retryable
        ),
        "the default policy retries a decode failure: {labels:?}"
    );
}

#[tokio::test]
async fn a_decode_failure_the_policy_skips_is_counted_as_non_retryable() {
    let labels =
        fail_a_corrupt_activity_row("cc_guarded_nr_wf", wf_guarded_non_retryable, "input").await;
    assert!(!labels.is_empty(), "the failed attempt is counted");
    assert!(
        labels.iter().all(
            |(error_type, non_retryable)| error_type == "PayloadDecodeFailed" && *non_retryable
        ),
        "the policy lists PayloadDecodeFailed, so the metric says non-retryable: {labels:?}"
    );
}

#[tokio::test]
async fn a_callback_body_that_cannot_decode_is_not_posted_and_backs_off() {
    use autumn_harvest::completion_callback::fire_due_completion_deliveries_with_codecs;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let deliverer = CapturingDeliverer::new(200);
    // A long first delay makes the `delayed` check below exact. A short one
    // can expire before the check runs.
    let _config = install_callbacks_with_backoff(deliverer.clone(), 3, Duration::from_secs(600));
    let mut conn = connect(&url).await;
    let exec_id = completed_run_with_callback(&mut conn, &codecs).await;
    evaluate_callbacks(&mut conn, exec_id, &codecs).await;

    let fired = fire_due_completion_deliveries_with_codecs(
        &mut conn,
        &None,
        &[],
        &reader_without_the_key(),
    )
    .await
    .expect("fire");
    assert_eq!(fired, 1, "the delivery was due");
    assert!(deliverer.posts().is_empty(), "ciphertext is never posted");

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        delayed: bool,
    }
    let row: Row = diesel::sql_query(
        "SELECT state, attempt, next_attempt_at > now() AS delayed \
         FROM harvest_completion_deliveries WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result(&mut conn)
    .await
    .expect("load the delivery");
    assert_eq!(row.attempt, 1, "the failed decode counts as an attempt");
    assert_eq!(row.state, "PENDING");
    assert!(row.delayed, "the delivery backs off");
}

/// Defer one throttled start. The first call on a key takes its only token.
async fn defer_throttled(
    conn: &mut AsyncPgConnection,
    key: &str,
    workflow_id: &str,
    schedule_to_start: Option<Duration>,
    codecs: &PayloadCodecs,
) {
    use autumn_harvest::throttle::{AdmitThrottleParams, ThrottleAdmission};
    let params = |id: &str| AdmitThrottleParams {
        workflow_name: "cc_throttle_stuck_wf",
        throttle_key: key,
        workflow_id: id.to_string().leak(),
        queue_name: "default",
        input: json!({"ssn": INPUT_SECRET}),
        start_options: Default::default(),
        refill_per_sec: 0.001,
        burst: 1.0,
        schedule_to_start,
        shard_id: 0,
    };
    let first = autumn_harvest::throttle::reserve_or_defer_with_codecs(
        conn,
        params("cc-stuck-first"),
        codecs,
    )
    .await
    .expect("admit");
    assert!(matches!(first, ThrottleAdmission::Reserved { .. }));
    let second =
        autumn_harvest::throttle::reserve_or_defer_with_codecs(conn, params(workflow_id), codecs)
            .await
            .expect("admit");
    assert!(matches!(second, ThrottleAdmission::Deferred(_)));
}

#[tokio::test]
async fn an_undecodable_buffer_row_moves_back_and_never_starts_a_run() {
    use autumn_harvest::debounce::{
        AdmitDebounceParams, admit_debounced_start_with_codecs,
        fire_due_debounced_starts_with_codecs,
    };
    use autumn_harvest::throttle::fire_due_throttled_starts_with_codecs;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let reader = reader_without_the_key();
    let mut conn = connect(&url).await;
    let metrics = autumn_harvest::telemetry::NoOpMetrics;

    // A debounce row.
    let debounce_key = format!("k-{}", Uuid::new_v4().simple());
    admit_debounced_start_with_codecs(
        &mut conn,
        AdmitDebounceParams {
            workflow_name: "cc_debounce_stuck_wf",
            debounce_key: &debounce_key,
            workflow_id: &format!("cc-stuck-{}", Uuid::new_v4().simple()),
            queue_name: "default",
            last_input: json!({"ssn": INPUT_SECRET}),
            start_options: Default::default(),
            window: Duration::from_millis(1),
            max_wait: Duration::from_secs(60),
            shard_id: 0,
        },
        false,
        &codecs,
    )
    .await
    .expect("admit")
    .expect("admitted");

    // A live throttle row, and one past its deadline.
    let live_key = format!("k-{}", Uuid::new_v4().simple());
    let live_id = format!("cc-live-{}", Uuid::new_v4().simple());
    defer_throttled(&mut conn, &live_key, &live_id, None, &codecs).await;
    let expired_key = format!("k-{}", Uuid::new_v4().simple());
    let expired_id = format!("cc-expired-{}", Uuid::new_v4().simple());
    defer_throttled(
        &mut conn,
        &expired_key,
        &expired_id,
        Some(Duration::from_millis(1)),
        &codecs,
    )
    .await;
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens = 5, last_refilled_at = NOW() \
         WHERE key IN (SELECT bucket_key FROM harvest_start_throttle \
                       WHERE workflow_id IN ($1, $2))",
    )
    .bind::<diesel::sql_types::Text, _>(&live_id)
    .bind::<diesel::sql_types::Text, _>(&expired_id)
    .execute(&mut conn)
    .await
    .expect("refill the buckets");
    tokio::time::sleep(Duration::from_millis(100)).await;

    fire_due_debounced_starts_with_codecs(&mut conn, &None, &[], &metrics, &reader)
        .await
        .expect("fire debounce");
    autumn_harvest::admission_gate::set_global_admission_gate_cache(None);
    fire_due_throttled_starts_with_codecs(&mut conn, &None, &[], &metrics, &reader)
        .await
        .expect("fire throttle");

    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_debounce \
             WHERE debounce_key = $1 AND effective_fire_at > now() + interval '10 seconds'",
            &debounce_key,
        )
        .await,
        1,
        "the debounce row moves back and stays"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_start_throttle \
             WHERE workflow_id = $1 AND deferred_at > now() + interval '10 seconds'",
            &live_id,
        )
        .await,
        1,
        "the live throttle row moves back and stays"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_start_throttle WHERE workflow_id = $1",
            &expired_id,
        )
        .await,
        0,
        "the row past its deadline is still dropped"
    );
    for name in ["cc_debounce_stuck_wf", "cc_throttle_stuck_wf"] {
        assert_eq!(
            count_rows(
                &mut conn,
                "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
                 WHERE workflow_name = $1",
                name,
            )
            .await,
            0,
            "{name} must not start on ciphertext"
        );
    }
}

/// The throttle claim has no `deferred_at` floor, so a backed-off row is still
/// a candidate on the next tick. The scan must not write or warn again inside
/// the window.
#[tokio::test]
async fn an_undecodable_throttle_row_is_written_once_per_backoff_window() {
    use autumn_harvest::throttle::fire_due_throttled_starts_with_codecs;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let reader = reader_without_the_key();
    let mut conn = connect(&url).await;
    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let id = format!("cc-window-{}", Uuid::new_v4().simple());
    defer_throttled(&mut conn, &key, &id, None, &codecs).await;
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens = 5, last_refilled_at = NOW() \
         WHERE key IN (SELECT bucket_key FROM harvest_start_throttle WHERE workflow_id = $1)",
    )
    .bind::<diesel::sql_types::Text, _>(&id)
    .execute(&mut conn)
    .await
    .expect("refill the bucket");
    tokio::time::sleep(Duration::from_millis(100)).await;

    async fn deferred_at(conn: &mut AsyncPgConnection, id: &str) -> String {
        let cell: JsonCell = diesel::sql_query(
            "SELECT to_jsonb(deferred_at::text) AS value FROM harvest_start_throttle \
             WHERE workflow_id = $1",
        )
        .bind::<diesel::sql_types::Text, _>(id)
        .get_result(conn)
        .await
        .expect("load deferred_at");
        cell.value.as_str().expect("a text value").to_string()
    }

    autumn_harvest::admission_gate::set_global_admission_gate_cache(None);
    fire_due_throttled_starts_with_codecs(&mut conn, &None, &[], &metrics, &reader)
        .await
        .expect("first scan");
    let after_first = deferred_at(&mut conn, &id).await;
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_start_throttle \
             WHERE workflow_id = $1 AND deferred_at > now() + interval '10 seconds'",
            &id,
        )
        .await,
        1,
        "the first scan moves the row back"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    autumn_harvest::admission_gate::set_global_admission_gate_cache(None);
    fire_due_throttled_starts_with_codecs(&mut conn, &None, &[], &metrics, &reader)
        .await
        .expect("second scan");
    assert_eq!(
        deferred_at(&mut conn, &id).await,
        after_first,
        "a row inside its backoff window is not written again"
    );
}

#[tokio::test]
async fn an_undecodable_outbox_row_backs_off_and_never_starts_a_run() {
    use autumn_harvest::models::NewCompletionTriggerOutboxDb;
    use autumn_harvest::schema::harvest_completion_trigger_outbox;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let workflow_id = format!("cc-stuck-outbox-{}", Uuid::new_v4().simple());
    diesel::insert_into(harvest_completion_trigger_outbox::table)
        .values(&NewCompletionTriggerOutboxDb {
            source_exec_id: Uuid::new_v4(),
            trigger_id: Uuid::new_v4(),
            target_shard: 0,
            target_workflow_name: "cc_outbox_stuck_wf".to_string(),
            target_workflow_id: workflow_id.clone(),
            target_input: codecs
                .encode_column(&json!({"ssn": INPUT_SECRET}))
                .expect("encode"),
            queue_name: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: serde_json::to_value(autumn_harvest::types::Priority::default()).unwrap(),
            max_workflow_input_bytes: 1_000_000,
            tenant: None,
        })
        .execute(&mut conn)
        .await
        .expect("insert the outbox row");

    let sharded = Some(autumn_harvest::shard::ShardedDbPool::single(build_pool(
        &url,
    )));
    autumn_harvest::completion_trigger::enforce_completion_triggers_outbox_with_codecs(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
        &sharded,
        &[ShardId::new(0)],
        &reader_without_the_key(),
    )
    .await
    .expect("relay");

    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_completion_trigger_outbox \
             WHERE target_workflow_id = $1 AND next_attempt_at IS NOT NULL",
            &workflow_id,
        )
        .await,
        1,
        "the row stays and backs off"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_id = $1",
            &workflow_id,
        )
        .await,
        0,
        "the target must not start on ciphertext"
    );
}

#[tokio::test]
async fn an_undecodable_batch_job_stays_pending_and_sends_no_signal() {
    use autumn_harvest::batch::{
        BatchAction, BatchExecutorConfig, BatchFilter, BatchSubmission, run_executor_once,
        submit_batch_job_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let workflow_name: &'static str =
        Box::leak(format!("cc_batch_{}", Uuid::new_v4().simple()).into_boxed_str());
    let exec_id = start(&mut conn, &codecs, workflow_name, &queue, None).await;
    let job_id = submit_batch_job_with_codecs(
        &mut conn,
        BatchSubmission {
            action: BatchAction::Signal,
            filter: BatchFilter {
                states: vec!["RUNNING".to_string()],
                workflow_name: Some(workflow_name.to_string()),
                search_attrs: Vec::new(),
            },
            signal_name: Some("go".to_string()),
            signal_payload: Some(json!({"pin": SIGNAL_SECRET})),
            idempotency_key: None,
            created_by: None,
        },
        &codecs,
    )
    .await
    .expect("submit");

    let pool = autumn_harvest::shard::ShardedDbPool::single(build_pool(&url));
    run_executor_once(
        &pool,
        &BatchExecutorConfig {
            payload_codecs: reader_without_the_key(),
            ..BatchExecutorConfig::default()
        },
    )
    .await
    .expect("run the executor");

    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_batch_jobs \
             WHERE id::TEXT = $1 AND status = 'Pending'",
            &job_id.to_string(),
        )
        .await,
        1,
        "the job stays pending"
    );
    assert!(
        signal_payloads(&mut conn, exec_id.as_uuid())
            .await
            .is_empty(),
        "no signal is sent"
    );
}

/// A cancel job that an older writer stored with a payload. The executor
/// ignores the payload for a cancel, so a missing key must not block the job.
#[tokio::test]
async fn a_cancel_batch_job_runs_without_the_key_for_its_unused_payload() {
    use autumn_harvest::batch::{
        BatchAction, BatchExecutorConfig, BatchFilter, BatchJobStatus, run_executor_once,
    };
    use autumn_harvest::models::NewBatchJob;
    use autumn_harvest::schema::harvest_batch_jobs;
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue = format!("cc-{}", Uuid::new_v4().simple());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let workflow_name: &'static str =
        Box::leak(format!("cc_batch_{}", Uuid::new_v4().simple()).into_boxed_str());
    start(&mut conn, &codecs, workflow_name, &queue, None).await;
    let job_id = Uuid::new_v4();
    let filter = BatchFilter {
        states: vec!["RUNNING".to_string()],
        workflow_name: Some(workflow_name.to_string()),
        search_attrs: Vec::new(),
    };
    diesel::insert_into(harvest_batch_jobs::table)
        .values(&NewBatchJob {
            id: job_id,
            action: BatchAction::Cancel.as_str(),
            filter: serde_json::to_value(&filter).expect("filter"),
            signal_name: None,
            signal_payload: Some(
                codecs
                    .encode_column(&json!({"pin": SIGNAL_SECRET}))
                    .expect("encode"),
            ),
            status: BatchJobStatus::Pending.as_str(),
            idempotency_key: None,
            created_by: None,
        })
        .execute(&mut conn)
        .await
        .expect("insert the job");

    let pool = autumn_harvest::shard::ShardedDbPool::single(build_pool(&url));
    run_executor_once(
        &pool,
        &BatchExecutorConfig {
            payload_codecs: reader_without_the_key(),
            ..BatchExecutorConfig::default()
        },
    )
    .await
    .expect("run the executor");

    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_batch_jobs \
             WHERE id::TEXT = $1 AND status = 'Completed'",
            &job_id.to_string(),
        )
        .await,
        1,
        "a cancel job never reads the payload, so a missing key must not block it"
    );
}

static CANCEL_FAILING: AtomicBool = AtomicBool::new(false);
static CANCEL_FAILURES: AtomicUsize = AtomicUsize::new(0);

#[tokio::test]
async fn a_cancel_batch_submission_stores_no_payload_and_needs_no_encoder() {
    use autumn_harvest::batch::{
        BatchAction, BatchFilter, BatchSubmission, get_batch_job, submit_batch_job_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = flaky_codecs(&CANCEL_FAILING, &CANCEL_FAILURES);
    AtomicBool::store(&CANCEL_FAILING, true, Ordering::SeqCst);
    let mut conn = connect(&url).await;
    let job_id = submit_batch_job_with_codecs(
        &mut conn,
        BatchSubmission {
            action: BatchAction::Cancel,
            filter: BatchFilter {
                states: vec!["RUNNING".to_string()],
                workflow_name: Some(format!("cc_batch_{}", Uuid::new_v4().simple())),
                search_attrs: Vec::new(),
            },
            signal_name: None,
            signal_payload: Some(json!({"pin": SIGNAL_SECRET})),
            idempotency_key: None,
            created_by: None,
        },
        &codecs,
    )
    .await
    .expect("a cancel does not depend on the encoder");

    let job = get_batch_job(&mut conn, job_id)
        .await
        .expect("load the job")
        .expect("the job exists");
    assert_eq!(
        job.signal_payload, None,
        "a cancel never reads the payload, so the row stores none"
    );
    assert_eq!(
        AtomicUsize::load(&CANCEL_FAILURES, Ordering::SeqCst),
        0,
        "the encoder is not called for a cancel"
    );
}

static BATCH_FAILING: AtomicBool = AtomicBool::new(false);
static BATCH_FAILURES: AtomicUsize = AtomicUsize::new(0);

#[tokio::test]
async fn a_retry_of_an_accepted_batch_submission_resolves_while_the_encoder_fails() {
    use autumn_harvest::batch::{
        BatchAction, BatchFilter, BatchSubmission, submit_batch_job_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = flaky_codecs(&BATCH_FAILING, &BATCH_FAILURES);
    let mut conn = connect(&url).await;
    let workflow_name = format!("cc_batch_{}", Uuid::new_v4().simple());
    let submission = BatchSubmission {
        action: BatchAction::Signal,
        filter: BatchFilter {
            states: vec!["RUNNING".to_string()],
            workflow_name: Some(workflow_name),
            search_attrs: Vec::new(),
        },
        signal_name: Some("go".to_string()),
        signal_payload: Some(json!({"pin": SIGNAL_SECRET})),
        idempotency_key: Some(format!("cc-idem-{}", Uuid::new_v4().simple())),
        created_by: None,
    };
    let accepted = submit_batch_job_with_codecs(&mut conn, submission.clone(), &codecs)
        .await
        .expect("the first submission is accepted");

    AtomicBool::store(&BATCH_FAILING, true, Ordering::SeqCst);
    let retry = submit_batch_job_with_codecs(&mut conn, submission, &codecs)
        .await
        .expect("a retry of an accepted submission resolves to its job");

    assert_eq!(retry, accepted, "the retry returns the existing job id");
    assert_eq!(
        AtomicUsize::load(&BATCH_FAILURES, Ordering::SeqCst),
        0,
        "an existing job needs no new ciphertext"
    );
}

// ── the switch off (issue #2043) ─────────────────────────────────────────────

/// Runs one activity and returns its result.
fn wf_activity_echo(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("cc_echo", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

#[tokio::test]
async fn activity_rows_stay_clear_while_column_encoding_is_off() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue: &'static str = Box::leak(format!("cc-{}", Uuid::new_v4().simple()).into_boxed_str());
    let codecs = aead_codecs(false);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, "cc_activity_echo_wf", queue, None).await;

    let pool = build_pool(&url);
    let worker = worker(
        queue,
        registry(
            vec![wf_info("cc_activity_echo_wf", wf_activity_echo, None)],
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
    assert_eq!(result, json!({"ssn": INPUT_SECRET}));

    let activity_rows: Vec<_> = task_rows(&mut conn, exec_id.as_uuid())
        .await
        .into_iter()
        .filter(|(task_type, _, _)| task_type == "activity")
        .collect();
    assert_eq!(activity_rows.len(), 1, "one activity task row");
    let (_, input, output) = &activity_rows[0];
    assert_eq!(
        input,
        &json!({"ssn": INPUT_SECRET}),
        "the input stays clear"
    );
    assert_eq!(
        output.as_ref(),
        Some(&json!({"ssn": INPUT_SECRET})),
        "the result stays clear"
    );
}

// ── event batch buffers (issue #2043) ────────────────────────────────────────

/// The batch scanner claims every due row in the database. Two tests that
/// fire with different registries would take each other's rows, so they run
/// one at a time.
static BATCH_FIRE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const BATCH_SECRET_A: &str = "cc-batch-a-ssn-111";
const BATCH_SECRET_B: &str = "cc-batch-b-ssn-222";

fn batch_params(
    name: &str,
    key: &str,
    workflow_id: &str,
    payload: Value,
    max_size: usize,
) -> autumn_harvest::event_batch::AdmitBatchParams {
    autumn_harvest::event_batch::AdmitBatchParams {
        workflow_name: name.to_string(),
        batch_key: key.to_string(),
        workflow_id: workflow_id.to_string(),
        queue_name: "default".to_string(),
        payload,
        start_options: Default::default(),
        max_wait: Duration::from_millis(1),
        max_size,
        shard_id: 0,
    }
}

async fn batch_cells(conn: &mut AsyncPgConnection, key: &str) -> Vec<Value> {
    let cell: JsonCell = diesel::sql_query(
        "SELECT buffered_payloads AS value FROM harvest_event_batches WHERE batch_key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .get_result(conn)
    .await
    .expect("load the batch row");
    cell.value.as_array().expect("an array").clone()
}

#[tokio::test]
async fn a_buffered_batch_payload_is_ciphertext_and_the_fire_starts_on_plaintext() {
    let _serial = BATCH_FIRE_LOCK.lock().await;
    use autumn_harvest::event_batch::{
        admit_batched_start_with_codecs, fire_due_event_batches_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let workflow_id = format!("cc-batch-{}", Uuid::new_v4().simple());
    let (a, b) = (
        json!({"ssn": BATCH_SECRET_A}),
        json!({"ssn": BATCH_SECRET_B}),
    );
    for payload in [&a, &b] {
        admit_batched_start_with_codecs(
            &mut conn,
            batch_params("cc_batch_wf", &key, &workflow_id, payload.clone(), 10),
            None,
            &codecs,
        )
        .await
        .expect("admit")
        .expect("admitted");
    }

    let cells = batch_cells(&mut conn, &key).await;
    assert_eq!(cells.len(), 2, "one array element per admission");
    assert_ciphertext(&cells[0], BATCH_SECRET_A, "the first buffered payload");
    assert_ciphertext(&cells[1], BATCH_SECRET_B, "the second buffered payload");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let fired = fire_due_event_batches_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &codecs,
    )
    .await
    .expect("fire");
    assert!(fired >= 1, "the due batch fired");
    let started = started_execution(&mut conn, "cc_batch_wf", &workflow_id).await;
    assert_ciphertext(&started.input, BATCH_SECRET_A, "the batched run input");
    assert_eq!(
        codecs.decode_column(&started.input).expect("decode"),
        json!([a, b]),
        "the run input is the plaintext array, one layer deep"
    );
}

#[tokio::test]
async fn an_immediate_batch_flush_starts_on_plaintext() {
    use autumn_harvest::event_batch::admit_batched_start_with_codecs;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let workflow_id = format!("cc-flush-{}", Uuid::new_v4().simple());
    let (a, b) = (
        json!({"ssn": BATCH_SECRET_A}),
        json!({"ssn": BATCH_SECRET_B}),
    );
    for payload in [&a, &b] {
        admit_batched_start_with_codecs(
            &mut conn,
            batch_params("cc_flush_wf", &key, &workflow_id, payload.clone(), 2),
            None,
            &codecs,
        )
        .await
        .expect("admit")
        .expect("admitted");
    }
    let started = started_execution(&mut conn, "cc_flush_wf", &workflow_id).await;
    assert_ciphertext(&started.input, BATCH_SECRET_A, "the flushed run input");
    assert_eq!(
        codecs.decode_column(&started.input).expect("decode"),
        json!([a, b]),
        "the flush decodes each element once"
    );
}

#[tokio::test]
async fn batch_payloads_stay_in_clear_while_column_encoding_is_off() {
    use autumn_harvest::event_batch::admit_batched_start_with_codecs;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(false);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let payload = json!({"ssn": BATCH_SECRET_A});
    admit_batched_start_with_codecs(
        &mut conn,
        batch_params(
            "cc_batch_off_wf",
            &key,
            &format!("cc-off-{key}"),
            payload.clone(),
            10,
        ),
        None,
        &codecs,
    )
    .await
    .expect("admit")
    .expect("admitted");
    assert_eq!(batch_cells(&mut conn, &key).await, vec![payload]);
}

#[tokio::test]
async fn an_undecodable_batch_row_moves_back_and_is_never_dropped_or_started() {
    let _serial = BATCH_FIRE_LOCK.lock().await;
    use autumn_harvest::event_batch::{
        admit_batched_start_with_codecs, fire_due_event_batches_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let reader = reader_without_the_key();
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let workflow_id = format!("cc-stuck-batch-{}", Uuid::new_v4().simple());
    let name = format!("cc_batch_stuck_{}", Uuid::new_v4().simple());
    admit_batched_start_with_codecs(
        &mut conn,
        batch_params(
            &name,
            &key,
            &workflow_id,
            json!({"ssn": BATCH_SECRET_A}),
            10,
        ),
        None,
        &codecs,
    )
    .await
    .expect("admit")
    .expect("admitted");
    tokio::time::sleep(Duration::from_millis(100)).await;

    fire_due_event_batches_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &reader,
    )
    .await
    .expect("a bad row never fails the scan");

    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_event_batches \
             WHERE batch_key = $1 AND fire_at > now() + interval '10 seconds'",
            &key,
        )
        .await,
        1,
        "the row stays and moves back"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1",
            &name,
        )
        .await,
        0,
        "the batch must not start on ciphertext"
    );
}

// ── schedule input (issue #2043) ─────────────────────────────────────────────

const SCHEDULE_SECRET: &str = "cc-schedule-input-ssn-777";

fn wf_noop(_ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

fn secret_schedule(name: &str) -> autumn_harvest::WorkflowSchedule {
    let mut ws = autumn_harvest::WorkflowSchedule::new(
        name,
        autumn_harvest::Schedule::Interval(Duration::from_secs(3600)),
    );
    ws.input = json!({"ssn": SCHEDULE_SECRET});
    ws
}

async fn schedule_cell(
    conn: &mut AsyncPgConnection,
    name: &str,
) -> (Value, chrono::DateTime<chrono::Utc>) {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        value: Value,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: chrono::DateTime<chrono::Utc>,
    }
    let row: Row = diesel::sql_query(
        "SELECT workflow_input AS value, updated_at FROM harvest_schedules \
         WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(name)
    .get_result(conn)
    .await
    .expect("load the schedule row");
    (row.value, row.updated_at)
}

#[tokio::test]
async fn a_schedule_input_is_ciphertext_and_a_second_registration_writes_nothing() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let name = format!("cc_sched_{}", Uuid::new_v4().simple());
    let ws = secret_schedule(&name);
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        std::slice::from_ref(&ws),
        &codecs,
    )
    .await
    .expect("register");
    let (first, first_updated) = schedule_cell(&mut conn, &name).await;
    assert_ciphertext(&first, SCHEDULE_SECRET, "schedules.workflow_input");

    autumn_harvest::register_workflow_schedules_with_codecs(&mut conn, &[ws], &codecs)
        .await
        .expect("register again");
    let (second, second_updated) = schedule_cell(&mut conn, &name).await;
    assert_eq!(first, second, "a converged row keeps its envelope");
    assert_eq!(
        first_updated, second_updated,
        "a converged row is not written"
    );
}

#[tokio::test]
async fn a_due_schedule_starts_its_run_on_the_plaintext_input() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let name = format!("cc_sched_fire_{}", Uuid::new_v4().simple());
    // Leak the name: a workflow info holds a `&'static str`.
    let static_name: &'static str = Box::leak(name.clone().into_boxed_str());
    let reg = registry(vec![wf_info(static_name, wf_noop, None)], vec![], &codecs);
    let ws = secret_schedule(&name);
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        std::slice::from_ref(&ws),
        &codecs,
    )
    .await
    .expect("register");
    diesel::sql_query(
        "UPDATE harvest_schedules SET next_run_at = now() - interval '1 minute' \
         WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&name)
    .execute(&mut conn)
    .await
    .expect("make it due");

    autumn_harvest::scheduler::tick_once(
        build_pool(&url),
        reg,
        Arc::new(autumn_harvest::DagCatalog::new()),
        Arc::new(vec![ws]),
        autumn_harvest::SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");

    use autumn_harvest::schema::harvest_workflow_executions;
    let input: Value = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(&name))
        .select(harvest_workflow_executions::input)
        .first(&mut conn)
        .await
        .expect("the tick started a run");
    assert_ciphertext(&input, SCHEDULE_SECRET, "the scheduled run input");
    assert_eq!(
        codecs.decode_column(&input).expect("decode one layer"),
        json!({"ssn": SCHEDULE_SECRET}),
        "the run starts on the plaintext input"
    );
    let (cell, _) = schedule_cell(&mut conn, &name).await;
    assert_ciphertext(&cell, SCHEDULE_SECRET, "the tick keeps the stored envelope");
}

#[tokio::test]
async fn a_schedule_input_that_cannot_decode_never_starts_a_run() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let name = format!("cc_sched_stuck_{}", Uuid::new_v4().simple());
    let static_name: &'static str = Box::leak(name.clone().into_boxed_str());
    let keyless = reader_without_the_key();
    let reg = registry(vec![wf_info(static_name, wf_noop, None)], vec![], &keyless);
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        &[secret_schedule(&name)],
        &codecs,
    )
    .await
    .expect("register");
    diesel::sql_query(
        "UPDATE harvest_schedules SET next_run_at = now() - interval '1 minute' \
         WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&name)
    .execute(&mut conn)
    .await
    .expect("make it due");

    // No in-memory schedules: the tick fires the stored row as it is.
    autumn_harvest::scheduler::tick_once(
        build_pool(&url),
        reg,
        Arc::new(autumn_harvest::DagCatalog::new()),
        Arc::new(vec![]),
        autumn_harvest::SchedulerMonitor::offline(),
    )
    .await
    .expect("a bad row never fails the tick");

    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1",
            &name,
        )
        .await,
        0,
        "no run starts on ciphertext"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_schedules \
             WHERE workflow_name = $1 AND next_run_at <= now()",
            &name,
        )
        .await,
        1,
        "the slot stays due, so a restored key fires it"
    );

    // Positive control: with the key restored, the same row fires.
    autumn_harvest::scheduler::tick_once(
        build_pool(&url),
        registry(vec![wf_info(static_name, wf_noop, None)], vec![], &codecs),
        Arc::new(autumn_harvest::DagCatalog::new()),
        Arc::new(vec![]),
        autumn_harvest::SchedulerMonitor::offline(),
    )
    .await
    .expect("tick with the key");
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1",
            &name,
        )
        .await,
        1,
        "a restored key starts the run"
    );
}

#[tokio::test]
async fn a_schedule_input_stored_in_clear_becomes_ciphertext_at_the_next_registration() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = format!("cc_sched_legacy_{}", Uuid::new_v4().simple());
    let ws = secret_schedule(&name);
    // A row from before the upgrade: the identity registry stores it in clear.
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        std::slice::from_ref(&ws),
        &aead_codecs(false),
    )
    .await
    .expect("register in clear");
    let (clear, _) = schedule_cell(&mut conn, &name).await;
    assert_eq!(
        clear,
        json!({"ssn": SCHEDULE_SECRET}),
        "the old row is clear"
    );

    // Column encoding is on now. The registration pass converts the row.
    let codecs = aead_codecs(true);
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        std::slice::from_ref(&ws),
        &codecs,
    )
    .await
    .expect("register encoded");
    let (stored, _) = schedule_cell(&mut conn, &name).await;
    assert_ciphertext(&stored, SCHEDULE_SECRET, "the converted schedule input");
    assert_eq!(
        codecs.decode_column(&stored).expect("decode one layer"),
        json!({"ssn": SCHEDULE_SECRET})
    );
}

#[tokio::test]
async fn a_schedule_patch_encodes_a_new_input_and_keeps_a_stored_one() {
    use autumn_harvest::scheduler::{
        ScheduleUpdateOutcome, WorkflowSchedulePatch, update_workflow_schedule_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let name = format!("cc_sched_patch_{}", Uuid::new_v4().simple());
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        &[secret_schedule(&name)],
        &codecs,
    )
    .await
    .expect("register");
    #[derive(diesel::QueryableByName)]
    struct Id {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    let id: Id = diesel::sql_query("SELECT id FROM harvest_schedules WHERE workflow_name = $1")
        .bind::<diesel::sql_types::Text, _>(&name)
        .get_result(&mut conn)
        .await
        .expect("schedule id");
    let (before, _) = schedule_cell(&mut conn, &name).await;

    // A patch without an input keeps the stored bytes.
    let outcome = update_workflow_schedule_with_codecs(
        &mut conn,
        id.id,
        &WorkflowSchedulePatch {
            max_active_runs: Some(3),
            ..Default::default()
        },
        &codecs,
    )
    .await
    .expect("patch without input");
    assert!(matches!(outcome, ScheduleUpdateOutcome::Updated(_)));
    let (kept, _) = schedule_cell(&mut conn, &name).await;
    assert_eq!(before, kept, "the stored envelope is not re-encoded");

    // A patch with an input stores a new envelope.
    let new_secret = "cc-schedule-patched-ssn-888";
    let outcome = update_workflow_schedule_with_codecs(
        &mut conn,
        id.id,
        &WorkflowSchedulePatch {
            input: Some(json!({"ssn": new_secret})),
            ..Default::default()
        },
        &codecs,
    )
    .await
    .expect("patch with input");
    assert!(matches!(outcome, ScheduleUpdateOutcome::Updated(_)));
    let (patched, _) = schedule_cell(&mut conn, &name).await;
    assert_ciphertext(&patched, new_secret, "the patched schedule input");
    assert_eq!(
        codecs.decode_column(&patched).expect("decode one layer"),
        json!({"ssn": new_secret})
    );
}

// ── context headers (issue #2043) ────────────────────────────────────────────

const HEADER_SECRET: &str = "cc-header-tenant-acme-9";

fn secret_headers() -> HashMap<String, String> {
    HashMap::from([("x-tenant".to_string(), HEADER_SECRET.to_string())])
}

/// Start a run that carries `secret_headers`.
async fn start_with_headers(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    workflow_name: &'static str,
    queue: &str,
    retry_policy: Option<RetryPolicy>,
) -> ExecutionId {
    let workflow_id = Uuid::new_v4().to_string();
    let params = StartWorkflowParams {
        context_headers: Some(secret_headers()),
        workflow_retry_policy: retry_policy,
        ..StartWorkflowParams::new(
            workflow_name,
            &workflow_id,
            ExecutionId::new_for_shard(ShardId::new(0)),
            json!({}),
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

/// Run one worker until `exec_id` completes, and return its result.
async fn run_to_result(
    url: &str,
    queue: &str,
    reg: Arc<HandlerRegistry>,
    codecs: &PayloadCodecs,
    exec_id: ExecutionId,
) -> Value {
    let pool = build_pool(url);
    let worker = worker(queue, reg);
    let run = worker.clone();
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), run.run(&run_pool)).await;
    });
    let client = WorkflowHandleClient::single(pool, url.to_string()).with_codecs(codecs.clone());
    let result = client
        .handle(exec_id)
        .result_raw_with_timeout(Duration::from_secs(25))
        .await
        .expect("the workflow completes");
    worker.shutdown();
    let _ = handle.await;
    result
}

fn wf_headers(ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let seen = ctx.header("x-tenant").map(str::to_string);
        let queue = ctx.queue_name().to_string();
        let act = ctx
            .execute_activity_raw("cc_headers_act", json!({}), &queue)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"wf": seen, "act": act}))
    })
}

fn headers_activity(ctx: &autumn_harvest::ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(json!(ctx.header("x-tenant"))) })
}

fn wf_headers_parent(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("cc_headers_child", input)
            .await
            .map_err(|e| e.to_string())
    })
}

fn wf_headers_child(ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(json!(ctx.header("x-tenant"))) })
}

static HEADER_RETRY_CALLS: AtomicUsize = AtomicUsize::new(0);

fn wf_headers_retry(ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        if HEADER_RETRY_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err("transient".to_string());
        }
        Ok(json!(ctx.header("x-tenant")))
    })
}

static HEADER_GUARDED_RUNS: AtomicUsize = AtomicUsize::new(0);

fn wf_headers_guarded(_ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        HEADER_GUARDED_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    })
}

#[tokio::test]
async fn context_headers_are_ciphertext_and_handlers_read_plaintext() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue: &'static str = Box::leak(format!("cc-{}", Uuid::new_v4().simple()).into_boxed_str());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id = start_with_headers(&mut conn, &codecs, "cc_headers_wf", queue, None).await;
    let execution = load_execution(&mut conn, exec_id.as_uuid()).await;
    assert_ciphertext(
        execution.context_headers.as_ref().expect("headers"),
        HEADER_SECRET,
        "executions.context_headers",
    );

    let result = run_to_result(
        &url,
        queue,
        registry(
            vec![wf_info("cc_headers_wf", wf_headers, None)],
            vec![activity_info("cc_headers_act", queue, headers_activity)],
            &codecs,
        ),
        &codecs,
        exec_id,
    )
    .await;
    assert_eq!(
        result,
        json!({"wf": HEADER_SECRET, "act": HEADER_SECRET}),
        "the workflow and the activity both saw the plaintext header"
    );

    #[derive(diesel::QueryableByName)]
    struct Cell {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        value: Value,
    }
    let rows: Vec<Cell> = diesel::sql_query(
        "SELECT context_headers AS value FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'activity' \
           AND context_headers IS NOT NULL",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("activity rows");
    assert!(!rows.is_empty(), "the activity row carries the headers");
    for row in rows {
        assert_ciphertext(&row.value, HEADER_SECRET, "task_queue.context_headers");
    }
}

#[tokio::test]
async fn headers_stay_in_clear_while_column_encoding_is_off() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(false);
    let mut conn = connect(&url).await;
    let exec_id =
        start_with_headers(&mut conn, &codecs, "cc_headers_off_wf", "default", None).await;
    let execution = load_execution(&mut conn, exec_id.as_uuid()).await;
    assert_eq!(
        execution.context_headers,
        Some(json!({"x-tenant": HEADER_SECRET}))
    );
}

#[tokio::test]
async fn a_child_workflow_inherits_the_encoded_headers_one_layer_deep() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue: &'static str = Box::leak(format!("cc-{}", Uuid::new_v4().simple()).into_boxed_str());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id = start_with_headers(&mut conn, &codecs, "cc_headers_parent", queue, None).await;
    let result = run_to_result(
        &url,
        queue,
        registry(
            vec![
                wf_info("cc_headers_parent", wf_headers_parent, None),
                wf_info("cc_headers_child", wf_headers_child, None),
            ],
            Vec::new(),
            &codecs,
        ),
        &codecs,
        exec_id,
    )
    .await;
    assert_eq!(result, json!(HEADER_SECRET), "the child saw the header");
    let child: WorkflowExecution = {
        use autumn_harvest::schema::harvest_workflow_executions;
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::parent_id.eq(Some(exec_id.as_uuid())))
            .select(WorkflowExecution::as_select())
            .first(&mut conn)
            .await
            .expect("load the child row")
    };
    let cell = child.context_headers.expect("the child keeps the headers");
    assert_ciphertext(&cell, HEADER_SECRET, "child context_headers");
    assert_eq!(
        codecs.decode_column(&cell).expect("decode one layer"),
        json!({"x-tenant": HEADER_SECRET}),
        "the child cell decodes once, with no second layer"
    );
}

#[tokio::test]
async fn a_workflow_retry_keeps_the_headers_one_layer_deep() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue: &'static str = Box::leak(format!("cc-{}", Uuid::new_v4().simple()).into_boxed_str());
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
    let exec_id = start_with_headers(
        &mut conn,
        &codecs,
        "cc_headers_retry",
        queue,
        Some(policy.clone()),
    )
    .await;
    let result = run_to_result(
        &url,
        queue,
        registry(
            vec![wf_info("cc_headers_retry", wf_headers_retry, Some(policy))],
            Vec::new(),
            &codecs,
        ),
        &codecs,
        exec_id,
    )
    .await;
    assert_eq!(result, json!(HEADER_SECRET), "the retry saw the header");
    let cell: Option<Value> = {
        use autumn_harvest::schema::harvest_workflow_executions;
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::retry_of_exec_id.eq(Some(exec_id.as_uuid())))
            .select(harvest_workflow_executions::context_headers)
            .first(&mut conn)
            .await
            .expect("load the retry row")
    };
    let cell = cell.expect("the retry keeps the headers");
    assert_ciphertext(&cell, HEADER_SECRET, "retry context_headers");
    assert_eq!(
        codecs.decode_column(&cell).expect("decode one layer"),
        json!({"x-tenant": HEADER_SECRET})
    );
}

#[tokio::test]
async fn an_activity_that_cannot_decode_its_headers_fails_the_attempt_and_never_runs() {
    let labels =
        fail_a_corrupt_activity_row("cc_guarded_hdr_wf", wf_guarded, "context_headers").await;
    assert!(!labels.is_empty(), "the failed attempt is counted");
    assert!(
        labels.iter().all(
            |(error_type, non_retryable)| error_type == "PayloadDecodeFailed" && !non_retryable
        ),
        "the default policy retries a decode failure: {labels:?}"
    );
}

/// Start a run whose stored headers name a key that no registry holds. Return
/// its final state and the number of retry runs.
async fn fail_a_run_with_corrupt_headers(
    workflow_name: &'static str,
    policy: Option<RetryPolicy>,
) -> (String, i64) {
    let (url, _c) = setup_test_database_url_or_env().await;
    let queue: &'static str = Box::leak(format!("cc-{}", Uuid::new_v4().simple()).into_boxed_str());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id =
        start_with_headers(&mut conn, &codecs, workflow_name, queue, policy.clone()).await;
    // History still decodes. Only the headers name a key that no registry holds.
    diesel::sql_query("UPDATE harvest_workflow_executions SET context_headers = $2 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Jsonb, _>(&json!({
            autumn_harvest::payload_codec::CODEC_ENVELOPE_KEY:
                {"codec_id": "aes-256-gcm", "kid": "cc-gone", "data": "AAAA"}
        }))
        .execute(&mut conn)
        .await
        .expect("corrupt the headers");

    let pool = build_pool(&url);
    let worker = worker(
        queue,
        registry(
            vec![wf_info(workflow_name, wf_headers_guarded, policy)],
            Vec::new(),
            &codecs,
        ),
    );
    let run = worker.clone();
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), run.run(&run_pool)).await;
    });
    let mut state = String::new();
    for _ in 0..800 {
        state = load_execution(&mut conn, exec_id.as_uuid()).await.state;
        if state == "FAILED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    worker.shutdown();
    let _ = handle.await;
    assert_eq!(
        state, "FAILED",
        "the run fails instead of losing its headers"
    );
    assert_eq!(
        AtomicUsize::load(&HEADER_GUARDED_RUNS, Ordering::SeqCst),
        0,
        "the handler never runs without its headers"
    );
    let retries = count_rows(
        &mut conn,
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
         WHERE retry_of_exec_id::TEXT = $1",
        &exec_id.as_uuid().to_string(),
    )
    .await;
    (state, retries)
}

#[tokio::test]
async fn a_workflow_whose_headers_cannot_decode_fails_and_never_runs() {
    let (state, _) = fail_a_run_with_corrupt_headers("cc_headers_guarded", None).await;
    assert_eq!(state, "FAILED");
}

#[tokio::test]
async fn a_workflow_retry_is_skipped_when_the_headers_cannot_decode() {
    let policy = RetryPolicy {
        max_attempts: 3,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(10),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    };
    let (state, retries) =
        fail_a_run_with_corrupt_headers("cc_headers_guarded_retry", Some(policy)).await;
    assert_eq!(state, "FAILED");
    assert_eq!(retries, 0, "no retry starts without the headers");
}

fn header_start_options() -> autumn_harvest::debounce::DebounceStartOptions {
    autumn_harvest::debounce::DebounceStartOptions {
        context_headers: Some(secret_headers()),
        memo: Some(json!({"account": MEMO_SECRET})),
        ..Default::default()
    }
}

async fn stored_start_options_headers(
    conn: &mut AsyncPgConnection,
    table: &str,
    where_sql: &str,
    bind: &str,
) -> Value {
    stored_start_option(conn, table, where_sql, bind, "context_headers").await
}

/// One key of a stored `start_options` object. The table, the filter and the
/// key are constants of the calling test.
async fn stored_start_option(
    conn: &mut AsyncPgConnection,
    table: &str,
    where_sql: &str,
    bind: &str,
    key: &str,
) -> Value {
    #[derive(diesel::QueryableByName)]
    struct Cell {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        value: Value,
    }
    let cell: Cell = diesel::sql_query(format!(
        "SELECT start_options -> '{key}' AS value FROM {table} WHERE {where_sql}"
    ))
    .bind::<diesel::sql_types::Text, _>(bind)
    .get_result(conn)
    .await
    .expect("load start options");
    cell.value
}

#[tokio::test]
async fn buffered_start_headers_are_ciphertext_and_the_fire_starts_with_plaintext() {
    use autumn_harvest::debounce::{
        AdmitDebounceParams, admit_debounced_start_with_codecs,
        fire_due_debounced_starts_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let workflow_id = format!("cc-hdr-debounce-{}", Uuid::new_v4().simple());
    admit_debounced_start_with_codecs(
        &mut conn,
        AdmitDebounceParams {
            workflow_name: "cc_hdr_debounce_wf",
            debounce_key: &key,
            workflow_id: &workflow_id,
            queue_name: "default",
            last_input: json!({}),
            start_options: header_start_options(),
            window: Duration::from_millis(1),
            max_wait: Duration::from_secs(60),
            shard_id: 0,
        },
        false,
        &codecs,
    )
    .await
    .expect("admit")
    .expect("admitted");
    let stored = stored_start_options_headers(
        &mut conn,
        "harvest_debounce",
        "workflow_name = 'cc_hdr_debounce_wf' AND debounce_key = $1",
        &key,
    )
    .await;
    assert_ciphertext(&stored, HEADER_SECRET, "debounce start_options headers");
    let memo = stored_start_option(
        &mut conn,
        "harvest_debounce",
        "workflow_name = 'cc_hdr_debounce_wf' AND debounce_key = $1",
        &key,
        "memo",
    )
    .await;
    assert_ciphertext(&memo, MEMO_SECRET, "debounce start_options memo");

    tokio::time::sleep(Duration::from_millis(100)).await;
    fire_due_debounced_starts_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &codecs,
    )
    .await
    .expect("fire");
    let started = started_execution(&mut conn, "cc_hdr_debounce_wf", &workflow_id).await;
    let cell = started.context_headers.expect("the run keeps the headers");
    assert_ciphertext(&cell, HEADER_SECRET, "the started run headers");
    assert_eq!(
        codecs.decode_column(&cell).expect("decode one layer"),
        json!({"x-tenant": HEADER_SECRET})
    );
    let run_memo = started.memo.expect("the run keeps the memo");
    assert_ciphertext(&run_memo, MEMO_SECRET, "the started run memo");
    assert_eq!(
        codecs.decode_column(&run_memo).expect("decode one layer"),
        json!({"account": MEMO_SECRET}),
        "the memo decodes once, with no second layer"
    );
}

#[tokio::test]
async fn throttled_start_headers_are_ciphertext_and_the_fire_starts_with_plaintext() {
    use autumn_harvest::throttle::{
        AdmitThrottleParams, ThrottleAdmission, fire_due_throttled_starts_with_codecs,
        reserve_or_defer_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let mut defer = async |workflow_id: &str| {
        reserve_or_defer_with_codecs(
            &mut conn,
            AdmitThrottleParams {
                workflow_name: "cc_hdr_throttle_wf",
                throttle_key: &key,
                workflow_id,
                queue_name: "default",
                input: json!({}),
                start_options: header_start_options(),
                refill_per_sec: 0.001,
                burst: 1.0,
                schedule_to_start: None,
                shard_id: 0,
            },
            &codecs,
        )
        .await
        .expect("admit")
    };
    assert!(matches!(
        defer("cc-hdr-throttle-first").await,
        ThrottleAdmission::Reserved { .. }
    ));
    let workflow_id = format!("cc-hdr-throttle-{}", Uuid::new_v4().simple());
    assert!(matches!(
        defer(&workflow_id).await,
        ThrottleAdmission::Deferred(_)
    ));
    let stored = stored_start_options_headers(
        &mut conn,
        "harvest_start_throttle",
        "workflow_id = $1",
        &workflow_id,
    )
    .await;
    assert_ciphertext(&stored, HEADER_SECRET, "throttle start_options headers");

    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens = 5, last_refilled_at = NOW() \
         WHERE key IN (SELECT bucket_key FROM harvest_start_throttle WHERE workflow_id = $1)",
    )
    .bind::<diesel::sql_types::Text, _>(&workflow_id)
    .execute(&mut conn)
    .await
    .expect("refill the bucket");
    autumn_harvest::admission_gate::set_global_admission_gate_cache(None);
    let fired = fire_due_throttled_starts_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &codecs,
    )
    .await
    .expect("fire");
    assert!(fired >= 1, "the deferred row fired");
    let started = started_execution(&mut conn, "cc_hdr_throttle_wf", &workflow_id).await;
    let cell = started.context_headers.expect("the run keeps the headers");
    assert_eq!(
        codecs.decode_column(&cell).expect("decode one layer"),
        json!({"x-tenant": HEADER_SECRET})
    );
}

#[tokio::test]
async fn batched_start_headers_are_ciphertext_and_both_start_paths_decode_them() {
    use autumn_harvest::event_batch::{
        admit_batched_start_with_codecs, fire_due_event_batches_with_codecs,
    };
    let _serial = BATCH_FIRE_LOCK.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;

    // The timed fire.
    let key = format!("k-{}", Uuid::new_v4().simple());
    let timed_id = format!("cc-hdr-batch-{}", Uuid::new_v4().simple());
    let mut params = batch_params("cc_hdr_batch_wf", &key, &timed_id, json!({}), 10);
    params.start_options = header_start_options();
    admit_batched_start_with_codecs(&mut conn, params, None, &codecs)
        .await
        .expect("admit")
        .expect("admitted");
    let stored =
        stored_start_options_headers(&mut conn, "harvest_event_batches", "batch_key = $1", &key)
            .await;
    assert_ciphertext(&stored, HEADER_SECRET, "batch start_options headers");
    tokio::time::sleep(Duration::from_millis(100)).await;
    fire_due_event_batches_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &codecs,
    )
    .await
    .expect("fire");
    let started = started_execution(&mut conn, "cc_hdr_batch_wf", &timed_id).await;
    let cell = started
        .context_headers
        .expect("the timed run keeps the headers");
    assert_eq!(
        codecs.decode_column(&cell).expect("decode one layer"),
        json!({"x-tenant": HEADER_SECRET})
    );

    // The immediate flush.
    let key = format!("k-{}", Uuid::new_v4().simple());
    let flush_id = format!("cc-hdr-flush-{}", Uuid::new_v4().simple());
    let mut params = batch_params("cc_hdr_flush_wf", &key, &flush_id, json!({}), 1);
    params.start_options = header_start_options();
    admit_batched_start_with_codecs(&mut conn, params, None, &codecs)
        .await
        .expect("admit")
        .expect("admitted");
    let started = started_execution(&mut conn, "cc_hdr_flush_wf", &flush_id).await;
    let cell = started
        .context_headers
        .expect("the flushed run keeps the headers");
    assert_eq!(
        codecs.decode_column(&cell).expect("decode one layer"),
        json!({"x-tenant": HEADER_SECRET})
    );
}

#[tokio::test]
async fn a_debounce_row_whose_headers_cannot_decode_moves_back_and_never_starts() {
    use autumn_harvest::debounce::{
        AdmitDebounceParams, admit_debounced_start_with_codecs,
        fire_due_debounced_starts_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let name = format!("cc_hdr_stuck_{}", Uuid::new_v4().simple());
    admit_debounced_start_with_codecs(
        &mut conn,
        AdmitDebounceParams {
            workflow_name: &name,
            debounce_key: &key,
            workflow_id: &format!("cc-hdr-stuck-{key}"),
            queue_name: "default",
            last_input: json!({}),
            start_options: header_start_options(),
            window: Duration::from_millis(1),
            max_wait: Duration::from_secs(60),
            shard_id: 0,
        },
        false,
        &codecs,
    )
    .await
    .expect("admit")
    .expect("admitted");
    // Admission encodes the input too. Put it back in clear, so only the
    // headers can fail the decode.
    diesel::sql_query(
        "UPDATE harvest_debounce SET last_input = '{}'::jsonb WHERE debounce_key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&key)
    .execute(&mut conn)
    .await
    .expect("clear the input");
    tokio::time::sleep(Duration::from_millis(100)).await;
    fire_due_debounced_starts_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &reader_without_the_key(),
    )
    .await
    .expect("a bad row never fails the scan");
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_debounce \
             WHERE debounce_key = $1 AND effective_fire_at > now() + interval '10 seconds'",
            &key,
        )
        .await,
        1,
        "the row moves back and stays"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1",
            &name,
        )
        .await,
        0,
        "no run starts without its headers"
    );
}

/// A stored header envelope under a key that no registry holds.
fn gone_key_envelope() -> Value {
    json!({
        autumn_harvest::payload_codec::CODEC_ENVELOPE_KEY:
            {"codec_id": "aes-256-gcm", "kid": "cc-gone", "data": "AAAA"}
    })
}

#[tokio::test]
async fn a_batch_append_needs_no_key_for_the_headers() {
    use autumn_harvest::event_batch::admit_batched_start_with_codecs;
    let _serial = BATCH_FIRE_LOCK.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let name = format!("cc_hdr_append_{}", Uuid::new_v4().simple());

    let mut first = batch_params(&name, &key, "cc-hdr-append-1", json!({"n": 1}), 10);
    first.max_wait = Duration::from_secs(3600);
    first.start_options = header_start_options();
    admit_batched_start_with_codecs(&mut conn, first, None, &codecs)
        .await
        .expect("admit")
        .expect("admitted");
    diesel::sql_query(
        "UPDATE harvest_event_batches \
         SET start_options = jsonb_set(start_options, '{context_headers}', $2) \
         WHERE batch_key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&key)
    .bind::<diesel::sql_types::Jsonb, _>(&gone_key_envelope())
    .execute(&mut conn)
    .await
    .expect("corrupt the headers");

    // An admission that only appends never reads the headers.
    let mut second = batch_params(&name, &key, "cc-hdr-append-2", json!({"n": 2}), 10);
    second.max_wait = Duration::from_secs(3600);
    second.start_options = header_start_options();
    admit_batched_start_with_codecs(&mut conn, second, None, &codecs)
        .await
        .expect("a later admission still appends")
        .expect("admitted");
    assert_eq!(batch_cells(&mut conn, &key).await.len(), 2);
}

#[tokio::test]
async fn a_flush_that_cannot_decode_gets_a_short_deadline_and_keeps_its_elements() {
    use autumn_harvest::event_batch::admit_batched_start_with_codecs;
    let _serial = BATCH_FIRE_LOCK.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let name = format!("cc_hdr_flush_late_{}", Uuid::new_v4().simple());

    let mut first = batch_params(&name, &key, "cc-hdr-late-1", json!({"n": 1}), 2);
    first.max_wait = Duration::from_secs(3600);
    first.start_options = header_start_options();
    admit_batched_start_with_codecs(&mut conn, first, None, &codecs)
        .await
        .expect("admit")
        .expect("admitted");
    diesel::sql_query(
        "UPDATE harvest_event_batches \
         SET start_options = jsonb_set(start_options, '{context_headers}', $2) \
         WHERE batch_key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&key)
    .bind::<diesel::sql_types::Jsonb, _>(&gone_key_envelope())
    .execute(&mut conn)
    .await
    .expect("corrupt the headers");

    // The second admission reaches max_size. The flush cannot decode.
    let mut second = batch_params(&name, &key, "cc-hdr-late-2", json!({"n": 2}), 2);
    second.max_wait = Duration::from_secs(3600);
    second.start_options = header_start_options();
    admit_batched_start_with_codecs(&mut conn, second, None, &codecs)
        .await
        .expect("an admission never fails on a flush it cannot decode")
        .expect("admitted");

    assert_eq!(batch_cells(&mut conn, &key).await.len(), 2);
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_event_batches \
             WHERE batch_key = $1 AND fire_at < now() + interval '120 seconds'",
            &key,
        )
        .await,
        1,
        "the deadline moves in from one hour, so a restored key retries soon"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1",
            &name,
        )
        .await,
        0,
        "no run starts without its headers"
    );
}

#[tokio::test]
async fn a_batch_row_that_cannot_decode_does_not_hold_up_the_next_due_row() {
    use autumn_harvest::event_batch::{
        admit_batched_start_with_codecs, fire_due_event_batches_with_codecs,
    };
    let _serial = BATCH_FIRE_LOCK.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let bad_name = format!("cc_hdr_bad_{}", Uuid::new_v4().simple());
    let good_name = format!("cc_hdr_good_{}", Uuid::new_v4().simple());
    let bad_key = format!("k-{}", Uuid::new_v4().simple());
    let good_key = format!("k-{}", Uuid::new_v4().simple());
    for (name, key, workflow_id) in [
        (&bad_name, &bad_key, "cc-hdr-bad"),
        (&good_name, &good_key, "cc-hdr-good"),
    ] {
        let mut params = batch_params(name, key, workflow_id, json!({}), 10);
        params.start_options = header_start_options();
        admit_batched_start_with_codecs(&mut conn, params, None, &codecs)
            .await
            .expect("admit")
            .expect("admitted");
    }
    // The bad row is due first. Its headers name a key that no registry holds.
    diesel::sql_query(
        "UPDATE harvest_event_batches \
         SET start_options = jsonb_set(start_options, '{context_headers}', $2), \
             fire_at = now() - interval '2 minutes' \
         WHERE batch_key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&bad_key)
    .bind::<diesel::sql_types::Jsonb, _>(&gone_key_envelope())
    .execute(&mut conn)
    .await
    .expect("corrupt the first row");
    diesel::sql_query(
        "UPDATE harvest_event_batches SET fire_at = now() - interval '1 minute' \
         WHERE batch_key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&good_key)
    .execute(&mut conn)
    .await
    .expect("make the second row due");

    fire_due_event_batches_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &codecs,
    )
    .await
    .expect("a bad row never fails the scan");

    started_execution(&mut conn, &good_name, "cc-hdr-good").await;
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1",
            &bad_name,
        )
        .await,
        0,
        "the bad row starts nothing"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_event_batches \
             WHERE batch_key = $1 AND fire_at > now() + interval '10 seconds'",
            &bad_key,
        )
        .await,
        1,
        "the bad row moves back and stays"
    );
}

#[tokio::test]
async fn a_throttle_row_whose_headers_cannot_decode_moves_back_and_never_starts() {
    use autumn_harvest::throttle::{
        AdmitThrottleParams, ThrottleAdmission, fire_due_throttled_starts_with_codecs,
        reserve_or_defer_with_codecs,
    };
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let key = format!("k-{}", Uuid::new_v4().simple());
    let name = format!("cc_hdr_tstuck_{}", Uuid::new_v4().simple());
    let workflow_id = format!("cc-hdr-tstuck-{key}");
    let mut defer = async |id: &str| {
        reserve_or_defer_with_codecs(
            &mut conn,
            AdmitThrottleParams {
                workflow_name: &name,
                throttle_key: &key,
                workflow_id: id,
                queue_name: "default",
                input: json!({}),
                start_options: header_start_options(),
                refill_per_sec: 0.001,
                burst: 1.0,
                schedule_to_start: None,
                shard_id: 0,
            },
            &codecs,
        )
        .await
        .expect("admit")
    };
    assert!(matches!(
        defer("cc-hdr-tstuck-first").await,
        ThrottleAdmission::Reserved { .. }
    ));
    assert!(matches!(
        defer(&workflow_id).await,
        ThrottleAdmission::Deferred(_)
    ));
    // Admission encodes the input too. Put it back in clear, so only the
    // headers and the memo can fail the decode.
    diesel::sql_query(
        "UPDATE harvest_start_throttle SET input = '{}'::jsonb WHERE workflow_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&workflow_id)
    .execute(&mut conn)
    .await
    .expect("clear the input");
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens = 5, last_refilled_at = NOW() \
         WHERE key IN (SELECT bucket_key FROM harvest_start_throttle WHERE workflow_id = $1)",
    )
    .bind::<diesel::sql_types::Text, _>(&workflow_id)
    .execute(&mut conn)
    .await
    .expect("refill the bucket");
    autumn_harvest::admission_gate::set_global_admission_gate_cache(None);

    fire_due_throttled_starts_with_codecs(
        &mut conn,
        &None,
        &[],
        &autumn_harvest::telemetry::NoOpMetrics,
        &reader_without_the_key(),
    )
    .await
    .expect("a bad row never fails the scan");
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_start_throttle \
             WHERE workflow_id = $1 AND deferred_at > now() + interval '10 seconds'",
            &workflow_id,
        )
        .await,
        1,
        "the row moves back and stays"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1",
            &name,
        )
        .await,
        0,
        "no run starts without its headers"
    );
}

#[tokio::test]
async fn a_rerun_keeps_the_headers_one_layer_deep_and_fails_on_a_cell_it_cannot_read() {
    use autumn_harvest::execution::{RerunRequest, rerun_workflow_execution_with_codecs};
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let source = start_with_headers(&mut conn, &codecs, "cc_headers_rerun", "default", None).await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'FAILED', completed_at = now() \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source.as_uuid())
    .execute(&mut conn)
    .await
    .expect("fail the source");
    let request = || RerunRequest {
        input_override: None,
        workflow_id_override: None,
        started_by: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        max_workflow_input_bytes: 0,
        max_execution_timeout_ceiling: None,
        max_workflow_chain_timeout_ceiling: None,
        max_workflow_attempts_ceiling: None,
        trace_context: None,
    };

    // A registry without the key cannot clone the headers. The rerun fails.
    let refused = rerun_workflow_execution_with_codecs(
        &mut conn,
        source,
        request(),
        None,
        &reader_without_the_key(),
    )
    .await;
    assert!(refused.is_err(), "a rerun never drops the headers");

    let outcome = rerun_workflow_execution_with_codecs(&mut conn, source, request(), None, &codecs)
        .await
        .expect("rerun");
    let cell = load_execution(&mut conn, outcome.exec_id.as_uuid())
        .await
        .context_headers
        .expect("the rerun keeps the headers");
    assert_ciphertext(&cell, HEADER_SECRET, "the rerun headers");
    assert_eq!(
        codecs.decode_column(&cell).expect("decode one layer"),
        json!({"x-tenant": HEADER_SECRET}),
        "the rerun cell decodes once, with no second layer"
    );
}

// ── a down encoder and the overlap policies (issue #2043) ────────────────────

/// A codec whose encode always fails, as a key service that is down would.
struct DownEncoder;

impl PayloadCodec for DownEncoder {
    fn codec_id(&self) -> &'static str {
        "down-encoder"
    }

    fn encode(&self, _raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Err(CodecError("the key service is down".to_string()))
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.to_vec())
    }
}

/// The test key still decodes. The active key cannot encode.
fn encoder_down() -> PayloadCodecs {
    let codecs = aead_codecs(true);
    codecs
        .register_key("down", Arc::new(DownEncoder))
        .expect("register");
    codecs.set_active_key("down").expect("activate");
    codecs
}

#[tokio::test]
async fn a_converged_schedule_registers_again_while_the_encoder_is_down() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = format!("cc_sched_down_{}", Uuid::new_v4().simple());
    let ws = secret_schedule(&name);
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        std::slice::from_ref(&ws),
        &aead_codecs(true),
    )
    .await
    .expect("register");
    let (before, updated_before) = schedule_cell(&mut conn, &name).await;

    // An idempotent request needs no write, so it needs no encode.
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        std::slice::from_ref(&ws),
        &encoder_down(),
    )
    .await
    .expect("a converged schedule needs no encoder");
    let (after, updated_after) = schedule_cell(&mut conn, &name).await;
    assert_eq!(before, after);
    assert_eq!(updated_before, updated_after, "nothing is written");

    // A change still needs the encoder, so it fails instead of storing clear.
    let mut changed = ws;
    changed.input = json!({"ssn": "cc-schedule-new-input-999"});
    let refused = autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        std::slice::from_ref(&changed),
        &encoder_down(),
    )
    .await;
    assert!(refused.is_err(), "a changed input is never stored in clear");
    let (kept, _) = schedule_cell(&mut conn, &name).await;
    assert_eq!(kept, before, "the stored input is unchanged");
}

#[tokio::test]
async fn an_overlap_policy_disturbs_no_run_when_the_schedule_input_cannot_decode() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let name = format!("cc_sched_overlap_{}", Uuid::new_v4().simple());
    let static_name: &'static str = Box::leak(name.clone().into_boxed_str());
    let mut ws = secret_schedule(&name);
    ws.overlap_policy = autumn_harvest::OverlapPolicy::TerminateOther;
    ws.max_active_runs = 1;
    autumn_harvest::register_workflow_schedules_with_codecs(
        &mut conn,
        std::slice::from_ref(&ws),
        &codecs,
    )
    .await
    .expect("register");

    #[derive(diesel::QueryableByName)]
    struct Id {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    let schedule: Id =
        diesel::sql_query("SELECT id FROM harvest_schedules WHERE workflow_name = $1")
            .bind::<diesel::sql_types::Text, _>(&name)
            .get_result(&mut conn)
            .await
            .expect("schedule id");

    // One scheduled run is in flight and fills the only slot.
    let in_flight = autumn_harvest::execution::start_or_load_workflow_execution_with_codecs(
        &mut conn,
        StartWorkflowParams {
            schedule_id: Some(schedule.id),
            scheduled_for: Some(chrono::Utc::now() - chrono::Duration::hours(1)),
            ..StartWorkflowParams::new(
                static_name,
                "cc-sched-in-flight",
                ExecutionId::new_for_shard(ShardId::new(0)),
                json!({}),
                "default",
            )
        },
        None,
        &codecs,
    )
    .await
    .expect("start the in-flight run")
    .exec_id;
    diesel::sql_query(
        "UPDATE harvest_schedules SET next_run_at = now() - interval '1 minute' \
         WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&name)
    .execute(&mut conn)
    .await
    .expect("make it due");

    // No key: the input cannot decode. The overlap step must not run first.
    autumn_harvest::scheduler::tick_once(
        build_pool(&url),
        registry(
            vec![wf_info(static_name, wf_noop, None)],
            vec![],
            &reader_without_the_key(),
        ),
        Arc::new(autumn_harvest::DagCatalog::new()),
        Arc::new(vec![]),
        autumn_harvest::SchedulerMonitor::offline(),
    )
    .await
    .expect("a bad row never fails the tick");
    assert_eq!(
        load_execution(&mut conn, in_flight.as_uuid()).await.state,
        "RUNNING",
        "the in-flight run is left alone"
    );

    // Positive control: with the key, the policy terminates it and starts the new run.
    autumn_harvest::scheduler::tick_once(
        build_pool(&url),
        registry(vec![wf_info(static_name, wf_noop, None)], vec![], &codecs),
        Arc::new(autumn_harvest::DagCatalog::new()),
        Arc::new(vec![]),
        autumn_harvest::SchedulerMonitor::offline(),
    )
    .await
    .expect("tick with the key");
    assert_ne!(
        load_execution(&mut conn, in_flight.as_uuid()).await.state,
        "RUNNING",
        "the policy replaces the in-flight run"
    );
    assert_eq!(
        count_rows(
            &mut conn,
            "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1",
            &name,
        )
        .await,
        2,
        "the replacement run started"
    );
}
