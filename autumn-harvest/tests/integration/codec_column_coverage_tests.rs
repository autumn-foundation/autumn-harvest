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

/// An AES-256-GCM codec that fails to encode while `FLAKY_FAILING` is set.
struct FlakyAead(AeadCodec);

impl PayloadCodec for FlakyAead {
    fn codec_id(&self) -> &'static str {
        self.0.codec_id()
    }

    fn bound_key_id(&self) -> Option<&str> {
        self.0.bound_key_id()
    }

    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        if AtomicBool::load(&FLAKY_FAILING, Ordering::SeqCst) {
            FLAKY_FAILURES.fetch_add(1, Ordering::SeqCst);
            return Err(CodecError("simulated encode failure".to_string()));
        }
        self.0.encode(raw)
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        self.0.decode(encoded)
    }
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
    let codecs = PayloadCodecs::default();
    let key = DataKey::from_bytes(&[0x5a; 32]).expect("data key");
    codecs
        .register_key(
            "cc-k1",
            Arc::new(FlakyAead(
                AeadCodec::new("cc-k1", &key).expect("aead codec"),
            )),
        )
        .expect("register");
    codecs.set_column_encoding(true);
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
    use autumn_harvest::completion_callback::{
        CallbackRuntimeConfig, CallbackSecret, GLOBAL_CALLBACK_CONFIG, HostAllowlist, SsrfPolicy,
    };
    *GLOBAL_CALLBACK_CONFIG.write().unwrap() = Some(Arc::new(CallbackRuntimeConfig {
        deliverer,
        secret: CallbackSecret::new(CALLBACK_HMAC_KEY.to_vec()),
        ssrf_policy: SsrfPolicy::new(HostAllowlist::new().with_pattern("api.example.com")),
        default_targets: Vec::new(),
        retry_policy: RetryPolicy::exponential(max_attempts, Duration::from_millis(10)),
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

#[tokio::test]
async fn an_activity_that_cannot_decode_its_input_fails_the_attempt_and_never_runs() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let wf_queue: &'static str =
        Box::leak(format!("cc-{}", Uuid::new_v4().simple()).into_boxed_str());
    let act_queue: &'static str = Box::leak(format!("{wf_queue}-act").into_boxed_str());
    let codecs = aead_codecs(true);
    let mut conn = connect(&url).await;
    let exec_id = start(&mut conn, &codecs, "cc_guarded_wf", wf_queue, None).await;
    let exec = exec_id.as_uuid().to_string();

    // The first worker owns the workflow queue. It schedules the activity on a
    // queue that no worker polls yet.
    let pool = build_pool(&url);
    let owner = worker(
        wf_queue,
        registry(
            vec![wf_info("cc_guarded_wf", wf_guarded, None)],
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
    diesel::sql_query(
        "UPDATE harvest_task_queue SET input = $2 \
         WHERE task_type = 'activity' AND workflow_exec_id::TEXT = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&exec)
    .bind::<diesel::sql_types::Jsonb, _>(&json!({
        autumn_harvest::payload_codec::CODEC_ENVELOPE_KEY:
            {"codec_id": "aes-256-gcm", "kid": "cc-gone", "data": "AAAA"}
    }))
    .execute(&mut conn)
    .await
    .expect("corrupt the task row");

    let activity_worker = worker(
        act_queue,
        registry(
            Vec::new(),
            vec![activity_info("cc_guarded", act_queue, guarded_activity)],
            &codecs,
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
}

#[tokio::test]
async fn a_callback_body_that_cannot_decode_is_not_posted_and_backs_off() {
    use autumn_harvest::completion_callback::fire_due_completion_deliveries_with_codecs;
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs(true);
    let deliverer = CapturingDeliverer::new(200);
    let _config = install_callbacks(deliverer.clone(), 3);
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

#[tokio::test]
async fn a_cancel_batch_job_runs_without_the_key_for_its_unused_payload() {
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
    start(&mut conn, &codecs, workflow_name, &queue, None).await;
    let job_id = submit_batch_job_with_codecs(
        &mut conn,
        BatchSubmission {
            action: BatchAction::Cancel,
            filter: BatchFilter {
                states: vec!["RUNNING".to_string()],
                workflow_name: Some(workflow_name.to_string()),
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
             WHERE id::TEXT = $1 AND status = 'Completed'",
            &job_id.to_string(),
        )
        .await,
        1,
        "a cancel job never reads the payload, so a missing key must not block it"
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
