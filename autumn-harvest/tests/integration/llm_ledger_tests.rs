#![cfg(feature = "db")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

//! End-to-end tests of the agent cost ledger (issue #1996).
//!
//! Each worker test runs a real worker against Postgres with an AES-256-GCM
//! codec. It checks that each completion path writes one ledger row per call,
//! keyed by the completion event. It also checks that the event output stays
//! ciphertext and holds no ledger field.
//!
//! # AC coverage map
//!
//! - **AC1** (model, tokens, cost and latency outside the encrypted payload)
//!   — [`the_worker_path_writes_one_row_per_call_outside_the_ciphertext`],
//!   [`a_local_activity_writes_its_calls_with_its_completion_event`],
//!   [`a_transactional_activity_writes_its_calls_in_its_commit`] and
//!   [`a_failed_attempt_writes_no_row`].
//! - **AC2** (usage rolls up per workflow type and per tenant) —
//!   [`usage_rolls_the_ledger_up_per_workflow_type_and_per_tenant`].
//! - **AC3** (replay is unchanged) —
//!   [`replay_of_a_history_with_llm_steps_is_unchanged`].
//! - Retention — [`retention_deletes_the_ledger_rows_with_the_run`].

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::llm_ledger::LlmCall;
use autumn_harvest::payload_codec::{PayloadCodecs, is_codec_envelope};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::telemetry::TelemetryConfig;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::usage::{UsageGroupBy, UsageQuery, UsageShardRow, load_usage_grouped};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{ActivityContext, ExecutionId, ShardId, StartWorkflowParams, WorkflowContext};

use crate::integration_e2e::{insert_workflow_execution, setup_test_database_url_or_env};
use chrono::Utc;
use diesel::sql_types::{BigInt, Integer, Nullable, Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use uuid::Uuid;

const OUTPUT_SECRET: &str = "llm-ledger-answer-ssn-987-65-4321";

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

fn aead_codecs() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let key = DataKey::from_bytes(&[0x3c; 32]).expect("data key");
    AeadCodec::new("llm-k1", &key)
        .expect("aead codec")
        .register_with(&codecs)
        .expect("register");
    codecs
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "llm_ledger_tests",
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
        retry_policy: None,
    }
}

fn activity_info(
    name: &'static str,
    queue: &'static str,
    is_local: bool,
    retry: Option<RetryPolicy>,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "llm_ledger_tests",
        default_retry_policy: retry,
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
        is_local,
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

fn unique_queue(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}-{}", Uuid::new_v4().simple()).into_boxed_str())
}

async fn start(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    workflow_name: &'static str,
    workflow_id: &str,
    queue: &str,
) -> ExecutionId {
    let params = StartWorkflowParams::new(
        workflow_name,
        workflow_id,
        ExecutionId::new_for_shard(ShardId::new(0)),
        json!({"prompt": "summarise"}),
        queue,
    );
    autumn_harvest::execution::start_or_load_workflow_execution_with_codecs(
        conn, params, None, codecs,
    )
    .await
    .expect("start")
    .exec_id
}

async fn wait_for_completed(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = Text)]
        state: String,
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let row: StateRow =
            diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
                .bind::<SqlUuid, _>(exec_id.as_uuid())
                .get_result(conn)
                .await
                .expect("load state");
        if row.state == "COMPLETED" {
            return;
        }
        assert!(
            !matches!(row.state.as_str(), "FAILED" | "CANCELLED" | "TERMINATED"),
            "the run ended in {}",
            row.state
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the run did not complete; state {}",
            row.state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Run every workflow in `workflow_ids` to completion on one worker.
async fn run_to_completion(
    url: &str,
    codecs: &PayloadCodecs,
    queue: &'static str,
    registry: Arc<HandlerRegistry>,
    workflow_name: &'static str,
    workflow_ids: &[&str],
) -> Vec<ExecutionId> {
    let pool = build_pool(url);
    let mut conn = connect(url).await;
    let mut exec_ids = Vec::new();
    for workflow_id in workflow_ids {
        exec_ids.push(start(&mut conn, codecs, workflow_name, workflow_id, queue).await);
    }
    let worker = worker(queue, registry);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool.clone();
        async move { worker.run(&pool).await }
    });
    for exec_id in &exec_ids {
        wait_for_completed(&mut conn, *exec_id).await;
    }
    handle.abort();
    exec_ids
}

#[derive(Debug, PartialEq, Eq, diesel::QueryableByName)]
struct LedgerRow {
    #[diesel(sql_type = Integer)]
    event_id: i32,
    #[diesel(sql_type = Integer)]
    call_index: i32,
    #[diesel(sql_type = Text)]
    activity_name: String,
    #[diesel(sql_type = Text)]
    model: String,
    #[diesel(sql_type = BigInt)]
    input_tokens: i64,
    #[diesel(sql_type = BigInt)]
    output_tokens: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    cost_usd_micros: Option<i64>,
    #[diesel(sql_type = BigInt)]
    latency_ms: i64,
}

async fn ledger_rows(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<LedgerRow> {
    diesel::sql_query(
        "SELECT event_id, call_index, activity_name, model, input_tokens, output_tokens, \
                cost_usd_micros, latency_ms \
           FROM harvest_llm_ledger WHERE workflow_exec_id = $1 \
          ORDER BY event_id, call_index",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .load(conn)
    .await
    .expect("load ledger rows")
}

#[derive(diesel::QueryableByName)]
struct EventRow {
    #[diesel(sql_type = Integer)]
    event_id: i32,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    event_data: Value,
}

/// The stored rows of one event type, in order.
async fn stored_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    event_type: &str,
) -> Vec<EventRow> {
    diesel::sql_query(
        "SELECT event_id, event_data FROM harvest_events \
          WHERE workflow_exec_id = $1 AND event_type = $2 ORDER BY event_id",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(event_type)
    .load(conn)
    .await
    .expect("load events")
}

/// The stored event holds ciphertext and no ledger field.
fn assert_output_is_sealed(event: &EventRow, model: &str) {
    let output = &event.event_data["data"]["output"];
    assert!(
        is_codec_envelope(output),
        "the output is an envelope: {output}"
    );
    let text = event.event_data.to_string();
    assert!(!text.contains(OUTPUT_SECRET), "the output secret is sealed");
    assert!(
        !text.contains(model),
        "the event holds no ledger field: {text}"
    );
    for field in [
        "input_tokens",
        "output_tokens",
        "cost_usd_micros",
        "latency",
    ] {
        assert!(
            !text.contains(field),
            "the event holds no `{field}`: {text}"
        );
    }
}

// ── handlers ─────────────────────────────────────────────────────────────────

fn wf_remote(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("llm_remote_step", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Records two calls, then answers with a secret.
fn llm_remote_step(ctx: &ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.record_llm_call(
            LlmCall::new("model-remote-a", 1_200, 340)
                .with_cost_usd_micros(8_700)
                .with_latency(Duration::from_millis(950)),
        )?;
        ctx.record_llm_call(LlmCall::new("model-remote-b", 50, 7))?;
        Ok(json!({"answer": OUTPUT_SECRET}))
    })
}

/// The same answer with no ledger call, for the replay comparison.
fn silent_remote_step(_ctx: &ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(json!({"answer": OUTPUT_SECRET})) })
}

fn wf_local(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_local_activity_raw("llm_local_step", input, None, None)
            .await
            .map_err(|e| e.to_string())
    })
}

fn llm_local_step(ctx: &ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.record_llm_call(
            LlmCall::new("model-local", 30, 3)
                .with_cost_usd_micros(12)
                .with_latency(Duration::from_millis(40)),
        )?;
        Ok(json!({"answer": OUTPUT_SECRET}))
    })
}

fn wf_txn(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("llm_txn_step", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Records a call, then completes through `run_transactional`.
fn llm_txn_step(ctx: &ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.record_llm_call(
            LlmCall::new("model-txn", 400, 90)
                .with_cost_usd_micros(1_500)
                .with_latency(Duration::from_millis(300)),
        )?;
        ctx.run_transactional(|_conn| Box::pin(async move { Ok(json!({"answer": OUTPUT_SECRET})) }))
            .await
    })
}

fn wf_flaky(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw("llm_flaky_step", input, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

static FLAKY_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Records a call on each attempt. The first attempt fails.
fn llm_flaky_step(ctx: &ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let attempt = FLAKY_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        ctx.record_llm_call(
            LlmCall::new(format!("model-attempt-{attempt}"), 10, 1)
                .with_latency(Duration::from_millis(5)),
        )?;
        if attempt == 1 {
            return Err("transient provider fault".to_string());
        }
        Ok(json!({"answer": OUTPUT_SECRET}))
    })
}

// ── AC1: the three completion paths ──────────────────────────────────────────

#[tokio::test]
async fn the_worker_path_writes_one_row_per_call_outside_the_ciphertext() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs();
    let queue = unique_queue("llm-remote");
    let registry = registry(
        vec![wf_info("llm_remote_wf", wf_remote)],
        vec![activity_info(
            "llm_remote_step",
            queue,
            false,
            None,
            llm_remote_step,
        )],
        &codecs,
    );
    let exec_ids = run_to_completion(
        &url,
        &codecs,
        queue,
        registry,
        "llm_remote_wf",
        &[&Uuid::new_v4().to_string()],
    )
    .await;
    let exec_id = exec_ids[0];

    let mut conn = connect(&url).await;
    let completed = stored_events(&mut conn, exec_id, "ActivityCompleted").await;
    assert_eq!(completed.len(), 1);
    assert_output_is_sealed(&completed[0], "model-remote-a");

    let rows = ledger_rows(&mut conn, exec_id).await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        rows[0],
        LedgerRow {
            event_id: completed[0].event_id,
            call_index: 0,
            activity_name: "llm_remote_step".to_string(),
            model: "model-remote-a".to_string(),
            input_tokens: 1_200,
            output_tokens: 340,
            cost_usd_micros: Some(8_700),
            latency_ms: 950,
        }
    );
    assert_eq!(rows[1].event_id, completed[0].event_id);
    assert_eq!(rows[1].call_index, 1);
    assert_eq!(rows[1].model, "model-remote-b");
    assert_eq!(rows[1].cost_usd_micros, None, "an unpriced call");
    assert!(rows[1].latency_ms >= 0);
}

#[tokio::test]
async fn a_local_activity_writes_its_calls_with_its_completion_event() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs();
    let queue = unique_queue("llm-local");
    let registry = registry(
        vec![wf_info("llm_local_wf", wf_local)],
        vec![activity_info(
            "llm_local_step",
            queue,
            true,
            None,
            llm_local_step,
        )],
        &codecs,
    );
    let exec_ids = run_to_completion(
        &url,
        &codecs,
        queue,
        registry,
        "llm_local_wf",
        &[&Uuid::new_v4().to_string()],
    )
    .await;

    let mut conn = connect(&url).await;
    let completed = stored_events(&mut conn, exec_ids[0], "LocalActivityCompleted").await;
    assert_eq!(completed.len(), 1);
    assert_output_is_sealed(&completed[0], "model-local");
    assert_eq!(
        ledger_rows(&mut conn, exec_ids[0]).await,
        vec![LedgerRow {
            event_id: completed[0].event_id,
            call_index: 0,
            activity_name: "llm_local_step".to_string(),
            model: "model-local".to_string(),
            input_tokens: 30,
            output_tokens: 3,
            cost_usd_micros: Some(12),
            latency_ms: 40,
        }]
    );
}

#[tokio::test]
async fn a_transactional_activity_writes_its_calls_in_its_commit() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs();
    let queue = unique_queue("llm-txn");
    let registry = registry(
        vec![wf_info("llm_txn_wf", wf_txn)],
        vec![activity_info(
            "llm_txn_step",
            queue,
            false,
            None,
            llm_txn_step,
        )],
        &codecs,
    );
    let exec_ids = run_to_completion(
        &url,
        &codecs,
        queue,
        registry,
        "llm_txn_wf",
        &[&Uuid::new_v4().to_string()],
    )
    .await;

    let mut conn = connect(&url).await;
    let completed = stored_events(&mut conn, exec_ids[0], "ActivityCompleted").await;
    assert_eq!(completed.len(), 1);
    assert_output_is_sealed(&completed[0], "model-txn");
    assert_eq!(
        ledger_rows(&mut conn, exec_ids[0]).await,
        vec![LedgerRow {
            event_id: completed[0].event_id,
            call_index: 0,
            activity_name: "llm_txn_step".to_string(),
            model: "model-txn".to_string(),
            input_tokens: 400,
            output_tokens: 90,
            cost_usd_micros: Some(1_500),
            latency_ms: 300,
        }]
    );
}

#[tokio::test]
async fn a_failed_attempt_writes_no_row() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs();
    let queue = unique_queue("llm-flaky");
    FLAKY_CALLS.store(0, Ordering::SeqCst);
    let registry = registry(
        vec![wf_info("llm_flaky_wf", wf_flaky)],
        vec![activity_info(
            "llm_flaky_step",
            queue,
            false,
            Some(RetryPolicy::fixed(2, Duration::from_millis(10))),
            llm_flaky_step,
        )],
        &codecs,
    );
    let exec_ids = run_to_completion(
        &url,
        &codecs,
        queue,
        registry,
        "llm_flaky_wf",
        &[&Uuid::new_v4().to_string()],
    )
    .await;

    let mut conn = connect(&url).await;
    let rows = ledger_rows(&mut conn, exec_ids[0]).await;
    assert_eq!(
        AtomicUsize::load(&FLAKY_CALLS, Ordering::SeqCst),
        2,
        "two attempts ran"
    );
    assert_eq!(rows.len(), 1, "only the completed attempt writes: {rows:?}");
    assert_eq!(rows[0].model, "model-attempt-2");
}

// ── AC3: replay is unchanged ─────────────────────────────────────────────────

/// Drop the keys that differ between two runs of the same workflow.
fn normalized(events: &[autumn_harvest::event::WorkflowEvent]) -> Value {
    fn strip(value: &mut Value) {
        match value {
            Value::Object(map) => {
                for key in ["timestamp", "activity_id", "worker_id", "queue"] {
                    map.remove(key);
                }
                map.values_mut().for_each(strip);
            }
            Value::Array(items) => items.iter_mut().for_each(strip),
            _ => {}
        }
    }
    let mut value = serde_json::to_value(events).expect("serialize history");
    strip(&mut value);
    value
}

#[tokio::test]
async fn replay_of_a_history_with_llm_steps_is_unchanged() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let codecs = aead_codecs();

    let ledger_queue = unique_queue("llm-replay-a");
    let ledger_registry = registry(
        vec![wf_info("llm_remote_wf", wf_remote)],
        vec![activity_info(
            "llm_remote_step",
            ledger_queue,
            false,
            None,
            llm_remote_step,
        )],
        &codecs,
    );
    let with_ledger = run_to_completion(
        &url,
        &codecs,
        ledger_queue,
        ledger_registry,
        "llm_remote_wf",
        &[&Uuid::new_v4().to_string()],
    )
    .await[0];

    let silent_queue = unique_queue("llm-replay-b");
    let silent_registry = registry(
        vec![wf_info("llm_remote_wf", wf_remote)],
        vec![activity_info(
            "llm_remote_step",
            silent_queue,
            false,
            None,
            silent_remote_step,
        )],
        &codecs,
    );
    let without_ledger = run_to_completion(
        &url,
        &codecs,
        silent_queue,
        silent_registry,
        "llm_remote_wf",
        &[&Uuid::new_v4().to_string()],
    )
    .await[0];

    let mut conn = connect(&url).await;
    assert_eq!(ledger_rows(&mut conn, with_ledger).await.len(), 2);
    assert!(ledger_rows(&mut conn, without_ledger).await.is_empty());

    let ledger_history =
        autumn_harvest::store::load_history_with_codecs(&mut conn, with_ledger, &codecs)
            .await
            .expect("history with ledger");
    let silent_history =
        autumn_harvest::store::load_history_with_codecs(&mut conn, without_ledger, &codecs)
            .await
            .expect("history without ledger");
    assert_eq!(
        normalized(&ledger_history.events),
        normalized(&silent_history.events),
        "the ledger leaves the decoded history as it is"
    );

    for (label, events) in [
        ("with the ledger", ledger_history.events),
        ("without the ledger", silent_history.events),
    ] {
        let report = WorkflowReplayer::new()
            .register_fn("llm_remote_wf", wf_remote)
            .replay_from_events(events)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay {label} must succeed:\n{report}"
        );
    }
}

// ── AC2: the usage rollup ────────────────────────────────────────────────────

async fn seed_run(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    tenant: Option<&str>,
) -> ExecutionId {
    let exec_id = insert_workflow_execution(conn).await;
    let attrs = tenant.map(|t| json!({"tenant_id": t}));
    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
            SET workflow_name = $2, search_attrs = $3 WHERE id = $1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(workflow_name)
    .bind::<Nullable<diesel::sql_types::Jsonb>, _>(attrs)
    .execute(conn)
    .await
    .expect("label the run");
    exec_id
}

#[allow(clippy::too_many_arguments)]
async fn seed_ledger_row(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    event_id: i32,
    input_tokens: i64,
    output_tokens: i64,
    cost: Option<i64>,
    latency_ms: i64,
    hours_ago: i64,
) {
    diesel::sql_query(
        "INSERT INTO harvest_llm_ledger \
             (workflow_exec_id, event_id, call_index, activity_name, model, \
              input_tokens, output_tokens, cost_usd_micros, latency_ms, recorded_at) \
         VALUES ($1, $2, 0, 'llm_step', 'model-x', $3, $4, $5, $6, $7)",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Integer, _>(event_id)
    .bind::<BigInt, _>(input_tokens)
    .bind::<BigInt, _>(output_tokens)
    .bind::<Nullable<BigInt>, _>(cost)
    .bind::<BigInt, _>(latency_ms)
    .bind::<diesel::sql_types::Timestamptz, _>(Utc::now() - chrono::Duration::hours(hours_ago))
    .execute(conn)
    .await
    .expect("seed ledger row");
}

fn group<'a>(rows: &'a [UsageShardRow], name: &str) -> &'a UsageShardRow {
    rows.iter()
        .find(|row| row.group == name)
        .unwrap_or_else(|| panic!("no usage group {name}: {rows:?}"))
}

#[tokio::test]
async fn usage_rolls_the_ledger_up_per_workflow_type_and_per_tenant() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string();
    let summarise = format!("summarise-{suffix}");
    let classify = format!("classify-{suffix}");
    let acme = format!("acme-{suffix}");
    let globex = format!("globex-{suffix}");

    let a1 = seed_run(&mut conn, &summarise, Some(&acme)).await;
    let a2 = seed_run(&mut conn, &classify, Some(&acme)).await;
    let g1 = seed_run(&mut conn, &summarise, Some(&globex)).await;
    seed_ledger_row(&mut conn, a1, 3, 1_000, 200, Some(5_000), 900, 0).await;
    seed_ledger_row(&mut conn, a1, 7, 500, 100, Some(2_500), 400, 0).await;
    seed_ledger_row(&mut conn, a2, 3, 80, 4, None, 60, 0).await;
    seed_ledger_row(&mut conn, g1, 3, 2_000, 300, Some(9_000), 1_100, 0).await;
    // Outside the window: three days old.
    seed_ledger_row(&mut conn, g1, 9, 99_999, 99_999, Some(99_999), 99_999, 72).await;

    let window = |group_by| UsageQuery {
        group_by,
        from: Utc::now() - chrono::Duration::hours(1),
        to: Utc::now() + chrono::Duration::hours(1),
    };

    let by_type = load_usage_grouped(&mut conn, 0, &window(UsageGroupBy::WorkflowName), 10_000)
        .await
        .expect("usage by workflow type");
    let s = group(&by_type, &summarise);
    assert_eq!(s.llm_calls, 3);
    assert_eq!(s.llm_input_tokens, 3_500);
    assert_eq!(s.llm_output_tokens, 600);
    assert_eq!(s.llm_cost_usd_micros, 16_500);
    assert_eq!(s.llm_unpriced_calls, 0);
    assert_eq!(s.llm_latency_ms, 2_400);
    let c = group(&by_type, &classify);
    assert_eq!(c.llm_calls, 1);
    assert_eq!(c.llm_input_tokens, 80);
    assert_eq!(c.llm_output_tokens, 4);
    assert_eq!(c.llm_cost_usd_micros, 0);
    assert_eq!(c.llm_unpriced_calls, 1);
    assert_eq!(c.llm_latency_ms, 60);

    let by_tenant = load_usage_grouped(
        &mut conn,
        0,
        &window(UsageGroupBy::SearchAttr("tenant_id".to_string())),
        10_000,
    )
    .await
    .expect("usage by tenant");
    let a = group(&by_tenant, &acme);
    assert_eq!(a.llm_calls, 3);
    assert_eq!(a.llm_input_tokens, 1_580);
    assert_eq!(a.llm_output_tokens, 304);
    assert_eq!(a.llm_cost_usd_micros, 7_500);
    assert_eq!(a.llm_unpriced_calls, 1);
    assert_eq!(a.llm_latency_ms, 1_360);
    let g = group(&by_tenant, &globex);
    assert_eq!(g.llm_calls, 1, "the old row is outside the window");
    assert_eq!(g.llm_input_tokens, 2_000);
    assert_eq!(g.llm_cost_usd_micros, 9_000);
    assert_eq!(g.workflow_starts, 1, "the other metrics are unchanged");
}

// ── retention ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn retention_deletes_the_ledger_rows_with_the_run() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let exec_id = seed_run(&mut conn, "llm_retention_wf", None).await;
    seed_ledger_row(&mut conn, exec_id, 3, 10, 1, Some(1), 5, 0).await;
    assert_eq!(ledger_rows(&mut conn, exec_id).await.len(), 1);

    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete the run as retention does");
    assert!(
        ledger_rows(&mut conn, exec_id).await.is_empty(),
        "the rows cascade with the run"
    );
}
