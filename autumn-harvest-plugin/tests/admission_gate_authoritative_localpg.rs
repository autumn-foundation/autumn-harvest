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
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        chain_execution_timeout: None,
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

/// A workflow carrying a start throttle (issue #607) so a fresh start reserves a
/// token and falls through to the gated plain start path (issue #618, PR #1014).
fn wf_info_throttled(name: &'static str) -> WorkflowInfo {
    let mut info = wf_info(name);
    info.throttle = Some(autumn_harvest::throttle::ThrottlePolicy {
        refill_per_sec: 0.0,
        burst: 10.0,
        key_expr: None,
        schedule_to_start: None,
    });
    info
}

fn build_registry(metrics: Arc<CapturingMetrics>) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![
            wf_info("ag_source_wf"),
            wf_info("ag_target_wf"),
            wf_info_throttled("ag_throttled_wf"),
        ],
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

#[derive(diesel::QueryableByName)]
struct TokenRow {
    #[diesel(sql_type = diesel::sql_types::Double)]
    t: f64,
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

async fn target_exec_count_named(conn: &mut AsyncPgConnection, name: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(name)
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
    // Deferred-fire producers gated at HTTP admission, scanner fire
    // exempt-with-bypass-counter (issue #618, F2). Throttle is NO LONGER here —
    // it flipped to gated_at_relay (fire-time gate, re-defer) in issue #1053;
    // debounce + event_batch remain gated_at_admission (#1053 scopes only
    // throttle).
    for split in ["debounce", "event_batch"] {
        assert_eq!(
            by_name(split).get("status").and_then(Value::as_str),
            Some("gated_at_admission"),
            "{split} must be gated_at_admission"
        );
    }
    // The cross-shard completion-trigger relay is gated authoritatively at relay
    // time on the target shard (issue #618, F-round7).
    assert_eq!(
        by_name("completion_trigger_outbox")
            .get("status")
            .and_then(Value::as_str),
        Some("gated_at_relay")
    );
    // Throttle is gated authoritatively at fire time (issue #1053): the deferred
    // scanner fire re-checks the gate on the workflow's real queue and, when a
    // gate matches, BLOCKS + RE-DEFERS the row (nothing dropped).
    assert_eq!(
        by_name("throttle").get("status").and_then(Value::as_str),
        Some("gated_at_relay"),
        "throttle must be gated_at_relay (issue #1053)"
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

/// F-round8 (issue #618): the workflow-start outbox bypass is counted EXACTLY
/// ONCE per committed start, gated on the app outbox row being durably marked
/// delivered — not on the Harvest start succeeding. This test drives the
/// observable half of that guarantee: a single relay counts once and marks the
/// row delivered, and a SECOND flush (the delivered row is now ineligible) never
/// re-counts. The start-ok/mark-Err retry path — where the OLD code counted at
/// start time and would count AGAIN on the retry (`start_or_load` returning the
/// same existing execution) — is closed by moving the count strictly AFTER the
/// `?`-guarded `mark_outbox_row_delivered`; a genuine mark-DB-error cannot be
/// injected through the public flush path, so that specific window is covered by
/// the count's placement (verified by reading) rather than a fault-injection test.
#[tokio::test]
async fn outbox_bypass_counted_exactly_once_across_reflush() {
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
            workflow_id: "outbox-once".to_string(),
            queue_name: "default".to_string(),
            input: json!({"from": "outbox"}),
            memo: None,
            search_attrs: None,
        },
    )
    .await
    .unwrap();

    // First flush: relays the row, marks it delivered, and counts the bypass ONCE.
    let delivered = flush_workflow_start_outbox(&state).await.unwrap();
    assert_eq!(delivered, 1, "the relay delivers the one queued row");
    assert_eq!(
        metrics.bypassed(),
        vec!["outbox".to_string()],
        "one committed outbox start counts exactly one bypass"
    );
    // The count is observably tied to the durable mark: the row is delivered.
    let undelivered: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_outbox WHERE delivered_at IS NULL",
    )
    .get_result::<CountRow>(&mut conn)
    .await
    .unwrap()
    .n;
    assert_eq!(
        undelivered, 0,
        "the relayed row is durably marked delivered"
    );

    // Second flush: the delivered row is no longer eligible, so nothing is
    // re-dispatched and the bypass is NOT counted again (exactly-once holds).
    let delivered_again = flush_workflow_start_outbox(&state).await.unwrap();
    assert_eq!(delivered_again, 0, "a delivered row is never re-relayed");
    assert_eq!(
        metrics.bypassed(),
        vec!["outbox".to_string()],
        "a delivered row is never re-counted — exactly one bypass per committed start"
    );
    // Exactly one execution started (start_or_load would return the same one on a
    // retry, so this also guards the re-dispatch double-count from the exec side).
    assert_eq!(target_exec_count(&mut conn).await, 1);
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
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
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
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
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

    // ── Producer 4: completion-trigger CROSS-SHARD relay — BLOCKED + counted. ──
    // The relay materializes a *new* pre-committed start on the target shard, so
    // (Finding B, F-round7) it is gated AUTHORITATIVELY at relay time: under this
    // Fleet gate the scanner drops the row + records admission_blocked instead of
    // starting the target. It still "processes" the row (returns 1), but as a BLOCK.
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
    assert_eq!(
        relayed, 1,
        "CT cross-shard relay processes the row (blocked + dropped) under the gate"
    );

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

    // ── Producer 6: throttle scanner — GatedAtRelay, BLOCKED + counted. ──
    // Since issue #1053 throttle honors the gate at fire time: even with a token
    // available, under the Fleet gate the deferred fire BLOCKS + RE-DEFERS the
    // row (nothing dropped, token refunded) and counts a block, not a bypass.
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
    assert_eq!(thr, 0, "throttle scanner blocks + re-defers under the gate");

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
    // Four producers were blocked-and-counted: the API start, the completion
    // trigger (evaluate-time), the completion-trigger cross-shard relay
    // (relay-time, Finding B), and — since issue #1053 — the throttle scanner
    // (fire-time, GatedAtRelay; re-defers its row). Block-vs-bypass never
    // double-counts: each of these was counted as a block exactly once and never
    // bypassed.
    assert!(
        blocked.len() >= 4,
        "API start + completion trigger + CT cross-shard relay + throttle (fire-time) must all be counted as blocks: {blocked:?}"
    );
    assert!(blocked.iter().all(|(scope, _)| scope == "fleet"));
    // Every GatedAtAdmission / exempt deferred-scanner producer that fired under
    // the gate counted its bypass — zero un-counted admissions. Throttle and the
    // CT cross-shard relay are NOT here: both are GatedAtRelay and blocked.
    let mut expected = vec![
        "debounce".to_string(),
        "event_batch".to_string(),
        "outbox".to_string(),
    ];
    expected.sort();
    assert_eq!(
        bypassed, expected,
        "every exempt producer that started a target under an active gate must count a bypass"
    );
    // Three exempt producers each started their own target; the API start, the
    // completion trigger, the CT cross-shard relay, and the throttle scanner
    // (issue #1053) were all blocked.
    assert_eq!(
        target_exec_count(&mut conn).await,
        3,
        "throttle target no longer starts under the gate; only the exempt producers started targets"
    );
}

/// Seed a prior `ag_target_wf` execution with an explicit `workflow_id` and a
/// terminal-or-live `state`, so the HTTP start route's gate-skip pre-check has a
/// concrete prior to reason about. Returns the seeded prior's execution id.
async fn seed_target_prior(conn: &mut AsyncPgConnection, workflow_id: &str, state: &str) -> Uuid {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "ag_target_wf",
            workflow_id,
            exec_id,
            input: json!({}),
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
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .unwrap();
    if state != "RUNNING" {
        let sql = if state == "COMPLETED" {
            "UPDATE harvest_workflow_executions SET state=$2, output='{}'::jsonb, \
             completed_at=NOW() WHERE id=$1"
        } else {
            "UPDATE harvest_workflow_executions SET state=$2, error='seed', \
             completed_at=NOW() WHERE id=$1"
        };
        diesel::sql_query(sql)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .bind::<diesel::sql_types::Text, _>(state)
            .execute(conn)
            .await
            .unwrap();
    }
    exec_id.as_uuid()
}

/// Raise a `Fleet` gate and publish the cache the API + core paths share.
async fn raise_fleet_gate(conn: &mut AsyncPgConnection, api_state: &HarvestApiState, reason: &str) {
    autumn_harvest::admission_gate::db::create_gate(
        conn,
        &GateScope::Fleet,
        reason,
        None,
        "test",
        None,
    )
    .await
    .unwrap();
    let gates = autumn_harvest::admission_gate::db::load_active_gates(conn)
        .await
        .unwrap();
    api_state.gate_cache().refresh(gates);
    set_global_admission_gate_cache(Some(api_state.gate_cache()));
}

/// Round 20 (issue #618): the HTTP start route's gate-skip pre-check must be
/// reuse-policy aware. `TerminateIfRunning` over a *live* prior ALWAYS creates a
/// replacement, so it is a genuine admission and MUST be blocked by an active
/// gate — the old policy-blind "non-sealed row exists → skip gate" wrongly let
/// it slip.
#[tokio::test]
async fn gate_blocks_terminate_if_running_start_over_a_live_prior() {
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
    let api_state = build_api_state(&pool, registry);
    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    seed_target_prior(&mut conn, "tir-live-prior", "RUNNING").await;
    raise_fleet_gate(&mut conn, &api_state, "tir-incident").await;

    let (status, body) = post_json(
        &app,
        "/workflows/ag_target_wf/start",
        json!({ "workflow_id": "tir-live-prior", "reuse_policy": "terminate_if_running" }),
    )
    .await;
    set_global_admission_gate_cache(None);

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "TerminateIfRunning over a live prior creates a replacement and must be gated: {body:?}"
    );
    let blocked = metrics.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "the block must be counted exactly once: {blocked:?}"
    );
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "only the seeded prior exists; no replacement slipped the gate"
    );
}

/// Round 20 (issue #618): `AllowDuplicateFailedOnly` over a prior that is
/// FAILED/CANCELLED at read time creates a replacement, so it is a genuine
/// admission and MUST be blocked by an active gate.
#[tokio::test]
async fn gate_blocks_allow_duplicate_failed_only_start_over_a_failed_prior() {
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
    let api_state = build_api_state(&pool, registry);
    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    seed_target_prior(&mut conn, "adfo-failed-prior", "FAILED").await;
    raise_fleet_gate(&mut conn, &api_state, "adfo-incident").await;

    let (status, body) = post_json(
        &app,
        "/workflows/ag_target_wf/start",
        json!({ "workflow_id": "adfo-failed-prior", "reuse_policy": "allow_duplicate_failed_only" }),
    )
    .await;
    set_global_admission_gate_cache(None);

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "ADFO over a FAILED prior creates a replacement and must be gated: {body:?}"
    );
    let blocked = metrics.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "the block must be counted exactly once: {blocked:?}"
    );
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "only the seeded FAILED prior exists; no replacement slipped the gate"
    );
}

/// Round 20 (issue #618): the fix must NOT regress the idempotent-attach bypass
/// (#808). `AllowDuplicate` attaches to the prior (live OR terminal) instead of
/// creating anything, so an active gate must be SKIPPED — the retry returns the
/// existing execution (200) and counts no block, with no new execution.
#[tokio::test]
async fn gate_skips_allow_duplicate_attach_regardless_of_prior_state() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_diesel_pool(&url);
    let mut conn = pool.get().await.unwrap();

    for prior_state in ["RUNNING", "COMPLETED"] {
        scrub(&mut conn).await;
        let metrics = Arc::new(CapturingMetrics::default());
        let registry = build_registry(Arc::clone(&metrics));
        let api_state = build_api_state(&pool, registry);
        let app = harvest_api_router(api_state.clone())
            .with_state(AppState::for_test().with_profile("test"));

        seed_target_prior(&mut conn, "ad-prior", prior_state).await;
        raise_fleet_gate(&mut conn, &api_state, "ad-incident").await;

        let (status, body) = post_json(
            &app,
            "/workflows/ag_target_wf/start",
            json!({ "workflow_id": "ad-prior", "reuse_policy": "allow_duplicate" }),
        )
        .await;
        set_global_admission_gate_cache(None);

        assert_eq!(
            status,
            StatusCode::OK,
            "AllowDuplicate attaches ({prior_state} prior); the gate must be skipped: {body:?}"
        );
        assert!(
            metrics.blocked().is_empty(),
            "an attach is not an admission — no block may be counted ({prior_state} prior)"
        );
        assert_eq!(
            target_exec_count(&mut conn).await,
            1,
            "attach created no new execution ({prior_state} prior)"
        );
    }
}

/// Round 21 (issue #618): the HTTP start route's pure-plain explicit-`workflow_id`
/// gate decision is now made under the SAME `FOR UPDATE` lock as the
/// create-vs-attach decision (via `gate_checked_start_or_load`), closing the
/// round-20 residual TOCTOU. Deterministic race repro with the lock-holding
/// harness: a holder locks the RUNNING prior and seals it to FAILED (uncommitted),
/// then we spawn the HTTP start (`AllowDuplicateFailedOnly`). Under the fix the
/// start queues on the prior's `FOR UPDATE` lock inside the locked primitive and,
/// after the holder commits, observes FAILED — so the ADFO replacement is a fresh
/// admission and is BLOCKED + counted, with no replacement created. Pre-fix
/// (round 20) the unlocked pre-read (a plain SELECT) observed the *committed*
/// RUNNING state and skipped the gate, letting `start_or_load` replace the
/// freshly-sealed prior past the active gate uncounted (201, no block, 2 rows).
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn http_start_blocks_a_prior_that_seals_between_read_and_start() {
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
    let api_state = build_api_state(&pool, registry);
    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    let prior = seed_target_prior(&mut conn, "seal-race", "RUNNING").await;
    raise_fleet_gate(&mut conn, &api_state, "seal-race-incident").await;

    // Holder: lock the RUNNING prior FOR UPDATE and seal it to FAILED, uncommitted.
    let mut holder = pool.get().await.unwrap();
    {
        use diesel_async::SimpleAsyncConnection;
        holder.batch_execute("BEGIN").await.unwrap();
        diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
            .bind::<diesel::sql_types::Uuid, _>(prior)
            .execute(&mut holder)
            .await
            .unwrap();
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET state='FAILED', error='sealed', \
             completed_at=NOW() WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(prior)
        .execute(&mut holder)
        .await
        .unwrap();
    }

    // Spawn the HTTP start (ADFO). Under the fix it queues on the prior's FOR
    // UPDATE lock inside `gate_checked_start_or_load`; pre-fix its unlocked
    // pre-read reads the committed RUNNING state and skips the gate.
    let app_clone = app.clone();
    let start_task = tokio::spawn(async move {
        let body =
            json!({ "workflow_id": "seal-race", "reuse_policy": "allow_duplicate_failed_only" });
        let resp = app_clone
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/ag_target_wf/start")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.status()
    });

    // Let the start reach (and, under the fix, block on) its FOR UPDATE lock load.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Holder commits the seal, releasing the lock.
    {
        use diesel_async::SimpleAsyncConnection;
        holder.batch_execute("COMMIT").await.unwrap();
    }

    let status = tokio::time::timeout(std::time::Duration::from_secs(8), start_task)
        .await
        .expect("HTTP start did not complete within 8s")
        .expect("join start task");
    set_global_admission_gate_cache(None);

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a prior that sealed to FAILED before the start must BLOCK the ADFO replacement"
    );
    let blocked = metrics.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "the block must be counted exactly once: {blocked:?}"
    );
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "no fresh replacement slipped the gate (only the sealed prior remains)"
    );
}

/// issue #618 (PR #1014): a fresh REQUEST-SCOPED-IDEMPOTENCY (#808) start under an
/// active gate must be BLOCKED (503) and counted — the gate is enforced inside the
/// `_idempotent` reservation transaction (`Some(GateMode::Check)`), rolling the
/// reservation back so a retry can start fresh. Pre-fix (gate arg `None`): the
/// keyed start slipped the gate uncounted (201/created).
#[tokio::test]
async fn keyed_start_blocked_by_gate_is_counted_and_rolls_back_reservation() {
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
    diesel::sql_query("DELETE FROM harvest_start_idempotency")
        .execute(&mut conn)
        .await
        .ok();

    let metrics = Arc::new(CapturingMetrics::default());
    let registry = build_registry(Arc::clone(&metrics));
    let api_state = build_api_state(&pool, registry);
    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    raise_fleet_gate(&mut conn, &api_state, "keyed-incident").await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/ag_target_wf/start")
                .header("content-type", "application/json")
                .header("Idempotency-Key", "keyed-under-gate")
                .body(Body::from(json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    set_global_admission_gate_cache(None);

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a fresh keyed start under an active gate must be blocked (503)"
    );
    let blocked = metrics.blocked();
    assert_eq!(blocked.len(), 1, "block counted exactly once: {blocked:?}");
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(
        target_exec_count(&mut conn).await,
        0,
        "no execution created for the blocked keyed start"
    );
    // The reservation rolled back — no idempotency claim persisted.
    let claims: i64 = diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_start_idempotency")
        .get_result::<CountRow>(&mut conn)
        .await
        .unwrap()
        .n;
    assert_eq!(claims, 0, "blocked keyed start rolled back its reservation");
}

/// issue #618 (PR #1014): a fresh THROTTLED (#607) start under an active gate — the
/// throttle RESERVES a token, then falls through to the gated plain start path,
/// which BLOCKS it (503), counts the block once, and REFUNDS the reserved token.
/// Pre-fix (gate arg `None`): the throttle-reserved start slipped the gate (201).
#[tokio::test]
async fn throttle_reserved_start_blocked_by_gate_refunds_token_and_counts() {
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
    let api_state = build_api_state(&pool, registry);
    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    raise_fleet_gate(&mut conn, &api_state, "throttle-incident").await;

    // Fresh start: `reserve_or_defer` auto-creates the bucket with `burst` tokens
    // and reserves one → Reserved → the plain gated start path runs and blocks.
    let (status, body) = post_json(&app, "/workflows/ag_throttled_wf/start", json!({})).await;
    set_global_admission_gate_cache(None);

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a throttle-reserved fresh start under an active gate must be blocked: {body:?}"
    );
    let blocked = metrics.blocked();
    assert_eq!(blocked.len(), 1, "block counted exactly once: {blocked:?}");
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(
        target_exec_count_named(&mut conn, "ag_throttled_wf").await,
        0,
        "no execution created for the blocked throttled start"
    );
    // The reserved token was refunded: the bucket is back to full `burst` (10).
    let bucket = autumn_harvest::throttle::bucket_key("ag_throttled_wf", "");
    let tokens: f64 =
        diesel::sql_query("SELECT tokens AS t FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket)
            .get_result::<TokenRow>(&mut conn)
            .await
            .map_or(10.0, |r| r.t);
    assert!(
        (tokens - 10.0).abs() < 1e-6,
        "the reserved token was refunded on the block (tokens = {tokens})"
    );
}

/// issue #618 (PR #1014) — THE BATCH TOCTOU FINDING: a batch item whose prior
/// SEALS (to TERMINATED) between Phase 1's unlocked, policy-blind gate pre-check
/// (which sees the still-RUNNING committed prior and skips the gate as an
/// "idempotent retry") and Phase 2's start. Under the fix Phase 2 passes
/// `Some(GateMode::Check)`, so `_collect` takes the `FOR UPDATE` lock, observes
/// TERMINATED, creates a fresh replacement, and BLOCKS it — counted once, no
/// replacement. Pre-fix (Phase 2 gate arg `None`): the fresh replacement slipped
/// the gate uncounted (item Started, 2 rows).
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn batch_item_blocks_a_prior_that_seals_between_phase1_and_phase2() {
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
    let api_state = build_api_state(&pool, registry);
    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    let prior = seed_target_prior(&mut conn, "batch-seal-race", "RUNNING").await;
    raise_fleet_gate(&mut conn, &api_state, "batch-seal-incident").await;

    // Holder: lock the RUNNING prior FOR UPDATE and seal it to TERMINATED,
    // uncommitted. Phase 1's plain SELECT sees the committed RUNNING state and
    // skips the gate; Phase 2's FOR UPDATE lock queues behind the holder.
    let mut holder = pool.get().await.unwrap();
    {
        use diesel_async::SimpleAsyncConnection;
        holder.batch_execute("BEGIN").await.unwrap();
        diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
            .bind::<diesel::sql_types::Uuid, _>(prior)
            .execute(&mut holder)
            .await
            .unwrap();
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET state='TERMINATED', completed_at=NOW() \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(prior)
        .execute(&mut holder)
        .await
        .unwrap();
    }

    let app_clone = app.clone();
    let start_task = tokio::spawn(async move {
        let body = json!({
            "atomic": false,
            "items": [{ "workflow_name": "ag_target_wf", "workflow_id": "batch-seal-race" }]
        });
        let resp = app_clone
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/batch_start")
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
            serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
        )
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    {
        use diesel_async::SimpleAsyncConnection;
        holder.batch_execute("COMMIT").await.unwrap();
    }

    let (status, body) = tokio::time::timeout(std::time::Duration::from_secs(8), start_task)
        .await
        .expect("batch start did not complete within 8s")
        .expect("join batch task");
    set_global_admission_gate_cache(None);

    // The batch response is a 200/OK envelope; the item itself must be rejected
    // (not Started) and the block counted once, with no fresh replacement.
    assert_ne!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "batch must not 500: {body:?}"
    );
    let blocked = metrics.blocked();
    assert_eq!(
        blocked.len(),
        1,
        "the batch item block must be counted exactly once: {blocked:?} (body {body:?})"
    );
    assert_eq!(blocked[0].0, "fleet");
    assert_eq!(
        target_exec_count(&mut conn).await,
        1,
        "no fresh replacement slipped the gate (only the sealed prior remains): {body:?}"
    );
}
