//! Local-Postgres end-to-end evidence for the admission-gate producer contract
//! (issue #618): `GET /admin/gates` producer block (AC5), the outbox exempt-by-
//! design bypass counter (AC3/AC4), and the success metric — with a `Fleet`
//! gate raised, every in-process producer either BLOCKS and counts the block
//! (API, completion trigger), or bypasses and counts the bypass (outbox, the
//! cross-shard completion-trigger relay, and the debounce / throttle /
//! event-batch deferred-fire scanners). Zero un-counted admissions.
//!
//! The primary coverage lives in the core `admission_gate_authoritative_tests`
//! (testcontainers) plus the pure unit tests. This file drives the identical
//! app router + real outbox relay against an operator-supplied
//! `HARVEST_TEST_DATABASE_URL` so it runs in a sandbox that has Postgres but no
//! Docker daemon (the #679/#597 local-PG precedent). No-op unless the env var
//! is set. Serialised: the admission gate cache and shard router are
//! process-global.
#![allow(clippy::await_holding_lock, clippy::too_many_lines)]

use std::sync::{Arc, Mutex};

use autumn_harvest::admission_gate::{GateScope, set_global_admission_gate_cache};
use autumn_harvest::completion_trigger::{TerminalState, evaluate_triggers_for_execution};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{RetentionConfig, StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_harvest_plugin::{
    HarvestDbPool, HarvestOutboxConfig, WorkflowStartRequest, enqueue_workflow_start_outbox,
    flush_workflow_start_outbox,
};
use autumn_web::AppState;
use autumn_web::config::DatabaseConfig;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::sql_types::BigInt;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

static TEST_SERIAL: Mutex<()> = Mutex::new(());

/// Shared capturing recorder: both `record_admission_blocked` (gated producers)
/// and `record_admission_bypassed` (exempt producers) land here, so a single
/// instance proves "zero un-counted admissions".
#[derive(Default)]
struct CapturingMetrics {
    blocked: Mutex<Vec<(String, String)>>,
    bypassed: Mutex<Vec<String>>,
}

impl CapturingMetrics {
    fn blocked(&self) -> Vec<(String, String)> {
        self.blocked.lock().unwrap().clone()
    }
    fn bypassed(&self) -> Vec<String> {
        self.bypassed.lock().unwrap().clone()
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_admission_blocked(&self, scope_kind: &str, reason_hash: &str) {
        self.blocked
            .lock()
            .unwrap()
            .push((scope_kind.to_string(), reason_hash.to_string()));
    }
    fn record_admission_bypassed(&self, producer: &str) {
        self.bypassed.lock().unwrap().push(producer.to_string());
    }
}

fn db_url() -> Option<String> {
    std::env::var("HARVEST_TEST_DATABASE_URL").ok()
}

fn build_diesel_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(6)
        .build()
        .expect("pool build failed")
}

fn build_web_pool(url: &str) -> diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection> {
    autumn_web::db::create_pool(&DatabaseConfig {
        url: Some(url.to_owned()),
        pool_size: 6,
        ..DatabaseConfig::default()
    })
    .expect("pool config")
    .expect("pool")
}

fn wf_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "tests",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

fn build_registry(metrics: Arc<CapturingMetrics>) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![wf_info("ag_source_wf"), wf_info("ag_target_wf")],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

fn build_api_state(storage: &DbPool, registry: Arc<HandlerRegistry>) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(storage.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("gate-authoritative-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_completion_trigger_fires",
        "DELETE FROM harvest_completion_trigger_outbox",
        "DELETE FROM harvest_completion_triggers",
        "DELETE FROM harvest_admission_gates",
        "DELETE FROM harvest_workflow_outbox",
        "DELETE FROM harvest_debounce",
        "DELETE FROM harvest_start_throttle",
        "DELETE FROM harvest_event_batches",
        "DELETE FROM harvest_rate_limit_buckets",
        "DELETE FROM harvest_events",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn target_exec_count(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = 'ag_target_wf'",
    )
    .get_result::<CountRow>(conn)
    .await
    .unwrap()
    .n
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn post_json(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// AC5: `GET /admin/gates` surfaces the discoverable producer contract so the
/// gated-vs-exempt-by-design classification is discoverable without source.
#[tokio::test]
async fn admin_gates_exposes_producer_contract() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_diesel_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let registry = build_registry(metrics);
    let api_state = build_api_state(&pool, registry);
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    let (status, body) = get_json(&app, "/admin/gates").await;
    assert_eq!(status, StatusCode::OK);
    let producers = body
        .get("producers")
        .and_then(Value::as_array)
        .expect("response must carry a `producers` block (AC5)");
    assert!(!producers.is_empty());

    let by_name = |name: &str| -> Value {
        producers
            .iter()
            .find(|p| p.get("producer").and_then(Value::as_str) == Some(name))
            .cloned()
            .unwrap_or(Value::Null)
    };
    // Outbox is exempt-by-design with a stated rationale.
    let outbox = by_name("outbox");
    assert_eq!(
        outbox.get("status").and_then(Value::as_str),
        Some("exempt_by_design")
    );
    assert!(
        outbox
            .get("rationale")
            .and_then(Value::as_str)
            .is_some_and(|r| !r.is_empty())
    );
    // Completion triggers and the API are gated.
    assert_eq!(
        by_name("completion_trigger")
            .get("status")
            .and_then(Value::as_str),
        Some("gated")
    );
    assert_eq!(
        by_name("api").get("status").and_then(Value::as_str),
        Some("gated")
    );
    // Deferred-fire producers are split: gated at HTTP admission, scanner fire
    // exempt-with-bypass-counter (issue #618, F2).
    for split in ["debounce", "throttle", "event_batch"] {
        assert_eq!(
            by_name(split).get("status").and_then(Value::as_str),
            Some("gated_at_admission"),
            "{split} must be gated_at_admission"
        );
    }
    // The cross-shard completion-trigger relay is exempt-by-design.
    assert_eq!(
        by_name("completion_trigger_outbox")
            .get("status")
            .and_then(Value::as_str),
        Some("exempt_by_design")
    );
}

/// AC3/AC4: the outbox relay is exempt-by-design but every relayed start
/// increments `harvest.admission.bypassed{producer="outbox"}` — even (and
/// especially) under an active Fleet gate.
#[tokio::test]
async fn outbox_relay_is_exempt_and_counts_the_bypass() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_diesel_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let registry = build_registry(Arc::clone(&metrics));
    // Installing the runtime also installs the global shard router.
    let _api_state = build_api_state(&pool, Arc::clone(&registry));

    // Raise a Fleet gate directly and publish the same cache globally.
    autumn_harvest::admission_gate::db::create_gate(
        &mut conn,
        &GateScope::Fleet,
        "outbox-incident",
        None,
        "test",
        None,
    )
    .await
    .unwrap();
    let cache = Arc::new(autumn_harvest::AdmissionGateCache::new());
    let gates = autumn_harvest::admission_gate::db::load_active_gates(&mut conn)
        .await
        .unwrap();
    cache.refresh(gates);
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    // Build an AppState for the outbox relay pointing at the same DB, wired
    // with the shared-metrics registry + router the relay reads for telemetry.
    let state = AppState::for_test().with_pool(build_web_pool(&url));
    state.insert_extension(HarvestDbPool::from(pool.clone()));
    state.insert_extension(ShardRouter::default());
    state.insert_extension(registry);
    state.insert_extension(HarvestOutboxConfig {
        enabled: true,
        ..HarvestOutboxConfig::default()
    });

    enqueue_workflow_start_outbox(
        &mut conn,
        &WorkflowStartRequest {
            workflow_name: "ag_target_wf".to_string(),
            workflow_id: "outbox-1".to_string(),
            queue_name: "default".to_string(),
            input: json!({"from": "outbox"}),
            memo: None,
            search_attrs: None,
        },
    )
    .await
    .unwrap();

    let delivered = flush_workflow_start_outbox(&state).await.unwrap();

    set_global_admission_gate_cache(None);

    assert_eq!(
        delivered, 1,
        "outbox relay starts the workflow despite the gate"
    );
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "outbox is exempt-by-design and starts the target"
    );
    let bypassed = metrics.bypassed();
    assert_eq!(
        bypassed,
        vec!["outbox".to_string()],
        "the outbox bypass must be counted (observable exemption)"
    );
}

/// AC2: a scoped gate blocks a start that matches its scope and lets a
/// non-matching start proceed. The inbound webhook receiver (issue #344)
/// delegates `Starts` / `SignalsWithStart` straight to `api::start_workflow` /
/// `signal_with_start_workflow` (see `webhook_receiver::handle_webhook`), so the
/// webhook-delegate producer is gated by exactly this start-route check.
#[tokio::test]
async fn scoped_gate_blocks_matching_start_and_passes_non_matching() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_diesel_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let registry = build_registry(metrics);
    let api_state = build_api_state(&pool, registry);
    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    // A WorkflowName gate scoped to a DIFFERENT workflow than the one started.
    autumn_harvest::admission_gate::db::create_gate(
        &mut conn,
        &GateScope::WorkflowName("some_other_wf".to_string()),
        "scoped-incident",
        None,
        "test",
        None,
    )
    .await
    .unwrap();
    let gates = autumn_harvest::admission_gate::db::load_active_gates(&mut conn)
        .await
        .unwrap();
    api_state.gate_cache().refresh(gates);
    set_global_admission_gate_cache(Some(api_state.gate_cache()));

    // Non-matching scope → the start proceeds (NOT 503).
    let (status, _b) = post_json(&app, "/workflows/ag_target_wf/start", json!({})).await;
    set_global_admission_gate_cache(None);
    assert_ne!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a non-matching scoped gate must not block the start"
    );

    // Now raise a matching WorkflowName gate → the start is blocked (503).
    scrub(&mut conn).await;
    autumn_harvest::admission_gate::db::create_gate(
        &mut conn,
        &GateScope::WorkflowName("ag_target_wf".to_string()),
        "scoped-incident-match",
        None,
        "test",
        None,
    )
    .await
    .unwrap();
    let gates = autumn_harvest::admission_gate::db::load_active_gates(&mut conn)
        .await
        .unwrap();
    api_state.gate_cache().refresh(gates);
    set_global_admission_gate_cache(Some(api_state.gate_cache()));
    let (status, _b) = post_json(&app, "/workflows/ag_target_wf/start", json!({})).await;
    set_global_admission_gate_cache(None);
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a matching scoped gate must block the start (webhook-delegate parity)"
    );
}

/// Success metric: with a `Fleet` gate active, every in-process producer this
/// PR touches (API start, completion trigger, outbox) is EITHER blocked and
/// counted, OR bypassed and counted — zero un-counted admissions.
#[tokio::test]
async fn fleet_gate_leaves_zero_uncounted_admissions() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_diesel_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let registry = build_registry(Arc::clone(&metrics));
    let api_state = build_api_state(&pool, Arc::clone(&registry));
    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    // Raise a Fleet gate; publish the SAME cache the API uses globally so the
    // core completion-trigger path honours it too.
    autumn_harvest::admission_gate::db::create_gate(
        &mut conn,
        &GateScope::Fleet,
        "success-metric-incident",
        None,
        "test",
        None,
    )
    .await
    .unwrap();
    let gates = autumn_harvest::admission_gate::db::load_active_gates(&mut conn)
        .await
        .unwrap();
    api_state.gate_cache().refresh(gates);
    set_global_admission_gate_cache(Some(api_state.gate_cache()));

    // ── Producer 1: the HTTP API start route — BLOCKED (503) + counted. ──
    let (status, body) = post_json(&app, "/workflows/ag_target_wf/start", json!({})).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "API start must be blocked by the Fleet gate: {body:?}"
    );

    // ── Producer 2: completion trigger — BLOCKED + counted, no target start. ──
    let trigger_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_completion_triggers
            (id, source_workflow_name, terminal_states, target_workflow_name, input_mapping)
         VALUES ($1, 'ag_source_wf', '[\"Completed\"]'::jsonb, 'ag_target_wf',
                 '{\"type\":\"Passthrough\"}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(trigger_id)
    .execute(&mut conn)
    .await
    .unwrap();
    let source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "ag_source_wf",
            workflow_id: "ag-src-success",
            exec_id: source_exec_id,
            input: json!({}),
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
            priority: autumn_harvest::types::Priority::default(),
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
    .unwrap();
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state='COMPLETED',
         output='{}'::jsonb, completed_at=NOW() WHERE id=$1",
    )
    .bind::<diesel::sql_types::Uuid, _>(source_exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .unwrap();
    let metrics_ref: &(dyn MetricsRecorder + Send + Sync) = registry.telemetry().metrics.as_ref();
    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(metrics_ref),
    )
    .await
    .unwrap();

    // ── Producer 3: outbox relay — BYPASSED + counted, target starts. ──
    let state = AppState::for_test().with_pool(build_web_pool(&url));
    state.insert_extension(HarvestDbPool::from(pool.clone()));
    state.insert_extension(ShardRouter::default());
    state.insert_extension(Arc::clone(&registry));
    state.insert_extension(HarvestOutboxConfig {
        enabled: true,
        ..HarvestOutboxConfig::default()
    });
    enqueue_workflow_start_outbox(
        &mut conn,
        &WorkflowStartRequest {
            workflow_name: "ag_target_wf".to_string(),
            workflow_id: "outbox-success".to_string(),
            queue_name: "default".to_string(),
            input: json!({}),
            memo: None,
            search_attrs: None,
        },
    )
    .await
    .unwrap();
    let delivered = flush_workflow_start_outbox(&state).await.unwrap();
    assert_eq!(delivered, 1);

    // ── Producer 4: completion-trigger CROSS-SHARD relay — EXEMPT + counted. ──
    // Seed a persisted outbox row (the gate was checked at evaluate time before
    // this row was written); the scanner relays it despite the now-active gate
    // and counts the bypass. Direct analog of the workflow-start outbox.
    let sharded = ShardedDbPool::single(pool.clone());
    diesel::sql_query(
        "INSERT INTO harvest_completion_trigger_outbox
            (source_exec_id, trigger_id, target_shard, target_workflow_name,
             target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes)
         VALUES ($1, $2, 0, 'ag_target_wf', 'ct-outbox-success', '{}'::jsonb, 'default',
                 '0'::jsonb, 1048576)",
    )
    .bind::<diesel::sql_types::Uuid, _>(ExecutionId::new_for_shard(ShardId::new(0)).as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(trigger_id)
    .execute(&mut conn)
    .await
    .unwrap();
    let relayed = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut conn,
        metrics_ref,
        &Some(sharded),
        &[ShardId::new(0)],
    )
    .await
    .unwrap();
    assert_eq!(relayed, 1, "CT cross-shard relay fires despite the gate");

    // ── Producer 5: debounce scanner — EXEMPT + counted. ──
    // A row admitted before the gate was raised, fired after (the leak F1 closes).
    diesel::sql_query(
        "INSERT INTO harvest_debounce
            (workflow_name, debounce_key, workflow_id, queue_name, last_input,
             start_options, effective_fire_at, max_fire_at)
         VALUES ('ag_target_wf', 'k-deb', 'deb-success', 'default', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second', NOW() + INTERVAL '1 hour')",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let deb =
        autumn_harvest::debounce::fire_due_debounced_starts(&mut conn, &None, &[], metrics_ref)
            .await
            .unwrap();
    assert_eq!(deb, 1, "debounce scanner fires despite the gate");

    // ── Producer 6: throttle scanner — EXEMPT + counted. ──
    // Seed a token so the scanner debits + fires (else it re-defers).
    let bucket = autumn_harvest::throttle::bucket_key("ag_target_wf", "k-thr");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets
            (key, refill_rate, burst, tokens, last_refilled_at)
         VALUES ($1, 0.0, 10.0, 10.0, NOW())",
    )
    .bind::<diesel::sql_types::Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle
            (workflow_name, throttle_key, bucket_key, workflow_id, queue_name,
             input, start_options, deferred_at)
         VALUES ('ag_target_wf', 'k-thr', $1, 'thr-success', 'default', '{}'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second')",
    )
    .bind::<diesel::sql_types::Text, _>(&bucket)
    .execute(&mut conn)
    .await
    .unwrap();
    let thr =
        autumn_harvest::throttle::fire_due_throttled_starts(&mut conn, &None, &[], metrics_ref)
            .await
            .unwrap();
    assert_eq!(thr, 1, "throttle scanner fires despite the gate");

    // ── Producer 7: event-batch scanner — EXEMPT + counted. ──
    diesel::sql_query(
        "INSERT INTO harvest_event_batches
            (workflow_name, batch_key, workflow_id, queue_name, buffered_payloads,
             start_options, fire_at, max_size)
         VALUES ('ag_target_wf', 'k-batch', 'batch-success', 'default', '[{}]'::jsonb,
                 '{}'::jsonb, NOW() - INTERVAL '1 second', 100)",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let batch =
        autumn_harvest::event_batch::fire_due_event_batches(&mut conn, &None, &[], metrics_ref)
            .await
            .unwrap();
    assert_eq!(batch, 1, "event-batch scanner fires despite the gate");

    set_global_admission_gate_cache(None);

    // ── Assertions: zero un-counted admissions across EVERY producer. ──
    let blocked = metrics.blocked();
    let mut bypassed = metrics.bypassed();
    bypassed.sort();
    // Both the API start and the completion trigger were blocked-and-counted.
    assert!(
        blocked.len() >= 2,
        "API start + completion trigger must both be counted as blocks: {blocked:?}"
    );
    assert!(blocked.iter().all(|(scope, _)| scope == "fleet"));
    // Every exempt / deferred-scanner producer that fired under the gate counted
    // its bypass — zero un-counted admissions.
    let mut expected = vec![
        "completion_trigger_outbox".to_string(),
        "debounce".to_string(),
        "event_batch".to_string(),
        "outbox".to_string(),
        "throttle".to_string(),
    ];
    expected.sort();
    assert_eq!(
        bypassed, expected,
        "every producer that started a target under an active gate must count a bypass"
    );
    // Five exempt producers each started their own target; neither the API start
    // nor the completion trigger (both blocked) slipped through.
    assert_eq!(
        target_exec_count(&mut conn).await,
        5,
        "only the exempt producers started targets; nothing gated slipped the gate"
    );
}
