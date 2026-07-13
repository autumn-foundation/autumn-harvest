//! Integration tests for operator read-path decoding of codec-encrypted
//! payloads (issue #608).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise
//! the management-API read surfaces end-to-end: `/result`, history export
//! (single + batch), describe, `/history`, `/stack`, `/dead-letters`, and the
//! SSE event stream.
//!
//! **Seeding note:** every engine write path persists via identity codecs
//! (real codecs are consumed only by the client handle path), so these tests
//! synthesize envelope-bearing rows directly — an envelope is just a JSON
//! object, and identity-codec persistence stores it verbatim. That simulates
//! the future/foreign write path the read-path decoder must serve.
//!
//! **Sandbox note:** no Docker is available in the authoring sandbox, so this
//! file is compile-checked only (`cargo test --no-run`), matching the
//! #543/#544/#601 precedent.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::payload_codec::{
    CodecError, PayloadCodec, PayloadCodecs, UNDECODABLE_MARKER_KEY,
    UNDECODABLE_REASON_UNKNOWN_CODEC,
};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{
    harvest_audit_log, harvest_events, harvest_task_queue, harvest_workflow_executions,
};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    StartWorkflowParams, WorkflowEvent, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260616000001_harvest_workflow_schedule_id/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260430000000_harvest_workflow_schedules/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508000000_harvest_external_task_updated_at/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000000_harvest_signal_idempotency/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260519000000_harvest_calendar_awareness/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260522000000_harvest_schedule_decisions/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260526000001_harvest_parent_close_policy/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000001_harvest_poison_pill_strikes/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000002_harvest_ownership_metadata/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260603000000_harvest_completion_triggers/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260708000001_harvest_completion_trigger_condition/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000001_harvest_task_required_capabilities/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260609000001_harvest_workflow_current_details/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260613000001_harvest_schedule_catchup_window/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../autumn-harvest/migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260705000000_harvest_completion_deliveries/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260710000002_harvest_workflow_continue_chain/up.sql"
    ),
);

type HarvestApiApp = axum::Router;

// ── Test codec + envelope fixtures ───────────────────────────────────────────

/// Byte-reversal "encryption" — mirrors `payload_codec.rs`'s own test codec.
#[derive(Debug)]
struct ReverseCodec;

impl PayloadCodec for ReverseCodec {
    fn codec_id(&self) -> &'static str {
        "reverse"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut v = raw.to_vec();
        v.reverse();
        Ok(v)
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut v = encoded.to_vec();
        v.reverse();
        Ok(v)
    }
}

fn reverse_codecs() -> PayloadCodecs {
    let mut codecs = PayloadCodecs::default();
    codecs.set_default(Arc::new(ReverseCodec));
    codecs
}

/// Builds a well-formed `reverse` codec envelope for `plain` by round-tripping
/// through the public `encode_event` (no base64 dependency needed here).
fn envelope_for(plain: &Value) -> Value {
    let event = WorkflowEvent::WorkflowCompleted {
        output: plain.clone(),
    };
    let encoded = reverse_codecs().encode_event(&event).expect("encode event");
    let envelope = encoded["data"]["output"].clone();
    assert_eq!(
        envelope["_harvest_codec_envelope"], 1,
        "fixture sanity: envelope_for must produce a codec envelope"
    );
    envelope
}

/// A serialized envelope for TEXT `error` columns.
fn stringified_envelope(plain: &Value) -> String {
    serde_json::to_string(&envelope_for(plain)).expect("serialize envelope")
}

/// An envelope naming a codec that is not registered — the graceful-degrade
/// marker case.
fn unknown_codec_envelope() -> Value {
    json!({
        "_harvest_codec_envelope": 1,
        "codec_id": "kms-rotated-away",
        "data": "e30=",
    })
}

// ── Harness ──────────────────────────────────────────────────────────────────

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

struct AppConfig<'a> {
    /// `true` ⇒ every request is treated as harvest-admin
    /// (`set_admin_auth_boundary(true)`); `false` ⇒ requests carry no session
    /// and are non-admin.
    admin: bool,
    /// Register the `ReverseCodec` registry on the api state.
    codecs: bool,
    /// Enable the issue-#608 opt-in.
    decode_on_read: bool,
    /// Notification URL for the SSE stream (usually `None`).
    notification_url: Option<&'a str>,
}

fn build_app_with(pool: &DbPool, config: &AppConfig<'_>) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    if config.admin {
        api_state.set_admin_auth_boundary(true);
    }
    if config.codecs {
        // Mirror the codec registry into the api state, the way plugin.rs
        // mirrors the SSRF policy (issue #608).
        api_state.set_payload_codecs(reverse_codecs());
    }
    if config.decode_on_read {
        // The deployment-level opt-in flag (issue #608).
        api_state.set_decode_payloads_on_read(true);
    }
    if let Some(url) = config.notification_url {
        api_state.set_workflow_result_notification_database_url(url);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("decode-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// The full decode-enabled admin app.
fn build_decode_app(pool: &DbPool) -> HarvestApiApp {
    build_app_with(
        pool,
        &AppConfig {
            admin: true,
            codecs: true,
            decode_on_read: true,
            notification_url: None,
        },
    )
}

/// Baseline: exactly today's app — the decode API surface is never invoked.
fn build_plain_app(pool: &DbPool) -> HarvestApiApp {
    build_app_with(
        pool,
        &AppConfig {
            admin: true,
            codecs: false,
            decode_on_read: false,
            notification_url: None,
        },
    )
}

/// Codecs registered but the opt-in never set — must behave byte-identically
/// to the plain app.
fn build_flag_off_app(pool: &DbPool) -> HarvestApiApp {
    build_app_with(
        pool,
        &AppConfig {
            admin: true,
            codecs: true,
            decode_on_read: false,
            notification_url: None,
        },
    )
}

/// Flag on + codecs registered, but requests are NOT admin.
fn build_non_admin_decode_app(pool: &DbPool) -> HarvestApiApp {
    build_app_with(
        pool,
        &AppConfig {
            admin: false,
            codecs: true,
            decode_on_read: true,
            notification_url: None,
        },
    )
}

async fn get_raw(app: &HarvestApiApp, uri: &str) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let (status, bytes) = get_raw(app, uri).await;
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

// ── Seeding helpers ──────────────────────────────────────────────────────────

async fn seed_running(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    input: Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "decode-wf",
            workflow_id,
            exec_id,
            input,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::default(),
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
        None,
    )
    .await
    .expect("seed workflow");
    exec_id
}

async fn mark_completed(conn: &mut AsyncPgConnection, exec_id: ExecutionId, output: Value) {
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            harvest_workflow_executions::output.eq(Some(output)),
        ))
        .execute(conn)
        .await
        .unwrap();
}

async fn mark_failed(conn: &mut AsyncPgConnection, exec_id: ExecutionId, error: &str) {
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("FAILED"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            harvest_workflow_executions::error.eq(Some(error.to_string())),
        ))
        .execute(conn)
        .await
        .unwrap();
}

async fn append_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
) {
    // Raw load: the seeded history may already carry non-identity envelopes,
    // which the strict identity-only `load_history` would refuse to load.
    let history = store::load_history_undecoded(conn, exec_id).await.unwrap();
    store::append_events(conn, exec_id, events, history.next_event_id)
        .await
        .expect("append events");
}

/// Seeds a pending activity task-queue row (the row backing the `/stack`
/// pending-activities panel) for `exec_id`, optionally with a stored
/// heartbeat checkpoint payload. Returns the recorded activity id.
async fn seed_pending_activity(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    input: Value,
    heartbeat_details: Option<Value>,
) -> ActivityExecId {
    let activity_id = ActivityExecId::new();
    let mut params = autumn_harvest::queue::EnqueueParams::new(
        "default",
        autumn_harvest::queue::TaskType::Activity,
        input,
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("charge_card".to_string());
    params.activity_id = Some(activity_id.as_uuid());
    autumn_harvest::queue::enqueue(conn, &params)
        .await
        .expect("seed pending activity task");
    if let Some(checkpoint) = heartbeat_details {
        diesel::update(
            harvest_task_queue::table
                .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
                .filter(harvest_task_queue::activity_id.eq(Some(activity_id.as_uuid()))),
        )
        .set((
            harvest_task_queue::heartbeat_details.eq(Some(checkpoint)),
            harvest_task_queue::last_heartbeat_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .expect("seed heartbeat checkpoint");
    }
    activity_id
}

/// Seed a pending activity in the real BACKING-OFF state (issue #773): a
/// `PENDING` task-queue row whose `error` column carries the last retryable
/// failure message, a bumped `attempt`, and a future `scheduled_at`. This is
/// what `queue::requeue_for_retry` actually writes — a retryable regular
/// failure appends NO `ActivityFailed` event, so the stack handler must source
/// `last_failure` from this column. `error` is written verbatim (pass an
/// envelope to exercise the #608 decode path).
async fn seed_backing_off_activity(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    input: Value,
    error: &str,
    attempt: i32,
) -> ActivityExecId {
    let activity_id = seed_pending_activity(conn, exec_id, input, None).await;
    diesel::update(
        harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(harvest_task_queue::activity_id.eq(Some(activity_id.as_uuid()))),
    )
    .set((
        harvest_task_queue::state.eq("PENDING"),
        harvest_task_queue::error.eq(Some(error.to_string())),
        harvest_task_queue::attempt.eq(attempt),
        harvest_task_queue::scheduled_at.eq(Utc::now() + chrono::Duration::seconds(30)),
    ))
    .execute(conn)
    .await
    .expect("seed backing-off task error/attempt/scheduled_at");
    activity_id
}

/// Audit rows for `OP_PAYLOAD_DECODE_READ`, as `(target_id, route_or_command,
/// error_summary)` triples, oldest first.
async fn decode_audit_rows(
    conn: &mut AsyncPgConnection,
) -> Vec<(Option<String>, String, Option<String>)> {
    harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq(autumn_harvest::audit::OP_PAYLOAD_DECODE_READ))
        .order(harvest_audit_log::occurred_at.asc())
        .select((
            harvest_audit_log::target_id,
            harvest_audit_log::route_or_command,
            harvest_audit_log::error_summary,
        ))
        .load(conn)
        .await
        .expect("query audit rows")
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// AC1: with the flag off, responses are byte-for-byte identical to today —
/// even when a codec registry is mirrored onto the api state — and the stored
/// ciphertext is what callers see.
#[tokio::test]
async fn responses_byte_identical_when_flag_off() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "flag-off", json!({"plain": true})).await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: envelope_for(&json!({"pii": "pii-alpha"})),
        }],
    )
    .await;
    mark_completed(
        &mut conn,
        exec_id,
        envelope_for(&json!({"pii": "pii-alpha"})),
    )
    .await;

    let baseline = build_plain_app(&pool);
    let flag_off = build_flag_off_app(&pool);

    for uri in [
        format!("/workflows/{exec_id}"),
        format!("/workflows/{exec_id}/result"),
        format!("/workflows/{exec_id}/history"),
    ] {
        let (base_status, base_body) = get_raw(&baseline, &uri).await;
        let (off_status, off_body) = get_raw(&flag_off, &uri).await;
        assert_eq!(base_status, off_status, "status must match for {uri}");
        assert_eq!(
            base_body, off_body,
            "flag-off body must be byte-identical to baseline for {uri}"
        );
        let body = String::from_utf8_lossy(&base_body);
        assert!(
            body.contains("_harvest_codec_envelope"),
            "ciphertext must be visible with the flag off for {uri}: {body}"
        );
        assert!(
            !body.contains("pii-alpha"),
            "plaintext must NOT appear with the flag off for {uri}: {body}"
        );
    }

    // /stack is the one surface whose response schema changed (the new
    // optional PendingActivity.input field): with the flag off the field must
    // be omitted entirely — no `input` key, no ciphertext, no plaintext — so
    // the JSON stays byte-identical to a pre-#608 build.
    let stack_exec = seed_running(&mut conn, "flag-off-stack", json!({})).await;
    seed_pending_activity(
        &mut conn,
        stack_exec,
        envelope_for(&json!({"card": "pii-alpha"})),
        None,
    )
    .await;
    let stack_uri = format!("/workflows/{stack_exec}/stack");
    let (base_status, base_body) = get_raw(&baseline, &stack_uri).await;
    let (off_status, off_body) = get_raw(&flag_off, &stack_uri).await;
    assert_eq!(base_status, StatusCode::OK);
    assert_eq!(base_status, off_status, "status must match for {stack_uri}");
    assert_eq!(
        base_body, off_body,
        "flag-off stack body must be byte-identical to baseline"
    );
    let stack_body = String::from_utf8_lossy(&base_body);
    assert!(
        stack_body.contains("pending_activities"),
        "fixture sanity: the seeded pending activity must appear: {stack_body}"
    );
    assert!(
        stack_body.contains("charge_card"),
        "fixture sanity: the seeded pending activity row must render: {stack_body}"
    );
    assert!(
        !stack_body.contains("\"input\""),
        "flag-off stack JSON must omit the input key entirely: {stack_body}"
    );
    assert!(
        !stack_body.contains("pii-alpha") && !stack_body.contains("_harvest_codec_envelope"),
        "flag-off stack must surface neither plaintext nor ciphertext input: {stack_body}"
    );
}

/// AC2: `GET /workflows/{id}/result` returns the decoded output and error
/// when decoding is enabled (zero-wait snapshot path).
#[tokio::test]
async fn result_endpoint_returns_decoded_output_and_error_when_enabled() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let completed = seed_running(&mut conn, "decoded-result-ok", json!({})).await;
    mark_completed(
        &mut conn,
        completed,
        envelope_for(&json!({"answer": 42, "pii": "pii-alpha"})),
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{completed}/result")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["state"], "completed");
    assert_eq!(
        body["output"]["answer"], 42,
        "output must be decoded: {body}"
    );
    assert!(
        !serde_json::to_string(&body)
            .unwrap()
            .contains("_harvest_codec_envelope"),
        "no envelope may leak: {body}"
    );

    // TEXT error column: a stringified envelope decodes to the plain error.
    let failed = seed_running(&mut conn, "decoded-result-err", json!({})).await;
    mark_failed(&mut conn, failed, &stringified_envelope(&json!("boom"))).await;

    let (status, body) = get_json(&app, &format!("/workflows/{failed}/result")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["state"], "failed");
    assert_eq!(
        body["error"], "boom",
        "TEXT error column must be decoded via decode_error_string_lossy: {body}"
    );
}

/// AC2 (long-poll): `GET /workflows/{id}/result?wait=…` routes through the
/// same decode + audit helper as the zero-wait snapshot. An already-terminal
/// execution returns immediately, so no real waiting happens.
#[tokio::test]
async fn result_long_poll_wait_returns_decoded_output_and_audits_once() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // The long-poll path requires the LISTEN/NOTIFY handle client.
    let app = build_app_with(
        &pool,
        &AppConfig {
            admin: true,
            codecs: true,
            decode_on_read: true,
            notification_url: Some(&url),
        },
    );
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "decoded-result-wait", json!({})).await;
    mark_completed(
        &mut conn,
        exec_id,
        envelope_for(&json!({"answer": 42, "pii": "pii-alpha"})),
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/result?wait=5")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["state"], "completed");
    assert_eq!(
        body["output"]["pii"], "pii-alpha",
        "long-poll output must be decoded: {body}"
    );
    assert!(
        !serde_json::to_string(&body)
            .unwrap()
            .contains("_harvest_codec_envelope"),
        "no envelope may leak on the long-poll path: {body}"
    );

    let audit = decode_audit_rows(&mut conn).await;
    assert_eq!(
        audit.len(),
        1,
        "exactly one payload.decode_read row for the long-poll read: {audit:?}"
    );
    assert_eq!(audit[0].0.as_deref(), Some(exec_id.to_string().as_str()));
    assert_eq!(audit[0].1, "GET /workflows/{id}/result");
}

/// AC3: the single-execution history export decodes each event payload under
/// `payload_policy=full`.
#[tokio::test]
async fn history_export_decodes_each_event_under_full_policy() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "export-full", json!({})).await;
    append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "charge_card".to_string(),
                input: envelope_for(&json!({"card": "pii-alpha"})),
                queue: "default".to_string(),
            },
            WorkflowEvent::WorkflowCompleted {
                output: envelope_for(&json!({"receipt": "pii-beta"})),
            },
        ],
    )
    .await;
    mark_completed(&mut conn, exec_id, json!(null)).await;

    let (status, bytes) = get_raw(
        &app,
        &format!("/workflows/{exec_id}/history/export?payload_policy=full"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("pii-alpha") && body.contains("pii-beta"),
        "every event payload must be decoded in the export: {body}"
    );
    assert!(
        !body.contains("_harvest_codec_envelope"),
        "no envelope may remain in a full export: {body}"
    );
}

/// Redacted exports never decode: decoding only to redact would be pointless
/// plaintext exposure — the export must carry neither plaintext nor decode
/// side effects.
#[tokio::test]
async fn history_export_skips_decode_under_redacted_policy() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "export-redacted", json!({})).await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: envelope_for(&json!({"pii": "pii-alpha"})),
        }],
    )
    .await;
    mark_completed(&mut conn, exec_id, json!(null)).await;

    let (status, bytes) = get_raw(
        &app,
        &format!("/workflows/{exec_id}/history/export?payload_policy=redacted"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        !body.contains("pii-alpha"),
        "redacted export must never contain decoded plaintext: {body}"
    );

    // No decode happened ⇒ no plaintext-read audit row either.
    let audit = decode_audit_rows(&mut conn).await;
    assert!(
        audit.is_empty(),
        "a redacted export must not write a payload.decode_read audit row: {audit:?}"
    );
}

/// PR #936 review: export size enforcement runs on the *decoded* document.
/// An export whose encoded envelope pushes it over `max_bytes` but whose
/// decoded form fits must succeed when decoding is enabled, and the reported
/// `size_limit.actual_bytes` must be the serialized size of the exact
/// response body (one source of truth). Flag-off keeps the pre-existing
/// behavior byte-identical: the stored (encoded) document is measured and
/// rejected with 413.
#[tokio::test]
async fn history_export_size_limit_is_enforced_on_the_decoded_document() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "export-size", json!({})).await;
    // Decoded payload ~3 KB; its base64 envelope is ~4/3 larger, so a 4000
    // byte limit sits between the decoded (~3.4 KB) and encoded (~4.5 KB)
    // document sizes.
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: envelope_for(&json!({"blob": "a".repeat(3000)})),
        }],
    )
    .await;
    mark_completed(&mut conn, exec_id, json!(null)).await;

    let uri = format!("/workflows/{exec_id}/history/export?payload_policy=full&max_bytes=4000");

    // Flag off: the stored (encoded) document is over the limit — rejected,
    // exactly as before this feature.
    let flag_off = build_flag_off_app(&pool);
    let (status, _bytes) = get_raw(&flag_off, &uri).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "flag-off measures the stored (encoded) document, which exceeds max_bytes"
    );

    // Decode enabled: the decoded document fits, so the export succeeds and
    // its size accounting reflects the decoded bytes actually returned.
    let app = build_decode_app(&pool);
    let (status, bytes) = get_raw(&app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let document: Value = serde_json::from_slice(&bytes).expect("export document JSON");
    let actual_bytes = usize::try_from(
        document["size_limit"]["actual_bytes"]
            .as_u64()
            .expect("actual_bytes"),
    )
    .expect("actual_bytes fits usize");
    assert_eq!(
        actual_bytes,
        bytes.len(),
        "actual_bytes must be the serialized size of the exact returned document"
    );
    assert!(
        actual_bytes <= 4000,
        "the decoded document must fit under max_bytes: {actual_bytes}"
    );
    assert!(
        String::from_utf8_lossy(&bytes).contains(&"a".repeat(3000)),
        "the export must carry the decoded payload"
    );
}

/// AC3 (batch): `GET /admin/history/exports?payload_policy=full` decodes each
/// export entry and writes exactly one audit row for the whole request.
#[tokio::test]
async fn admin_history_exports_batch_decodes_and_audits_once() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    for i in 0..2 {
        let exec_id = seed_running(&mut conn, &format!("batch-export-{i}"), json!({})).await;
        append_events(
            &mut conn,
            exec_id,
            &[WorkflowEvent::WorkflowCompleted {
                output: envelope_for(&json!({"pii": format!("pii-batch-{i}")})),
            }],
        )
        .await;
        mark_completed(&mut conn, exec_id, json!(null)).await;
    }

    let (status, bytes) = get_raw(&app, "/admin/history/exports?payload_policy=full").await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("pii-batch-0") && body.contains("pii-batch-1"),
        "batch export entries must be decoded: {body}"
    );
    assert!(!body.contains("_harvest_codec_envelope"), "{body}");

    let audit = decode_audit_rows(&mut conn).await;
    assert_eq!(
        audit.len(),
        1,
        "exactly one payload.decode_read row per request, not per entry: {audit:?}"
    );
}

/// AC4 (part 1): the describe surface and the paginated history page emit
/// decoded payloads.
#[tokio::test]
async fn describe_and_history_pages_emit_decoded_payloads() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // The workflow input itself is an envelope (identity persistence stores
    // it verbatim), plus an envelope-bearing activity event.
    let exec_id = seed_running(
        &mut conn,
        "describe-decoded",
        envelope_for(&json!({"user": "pii-alpha"})),
    )
    .await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "charge_card".to_string(),
            input: envelope_for(&json!({"card": "pii-beta"})),
            queue: "default".to_string(),
        }],
    )
    .await;

    // Describe embeds the first history page + the execution row.
    let (status, describe) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {describe}");
    let describe_str = serde_json::to_string(&describe).unwrap();
    assert!(
        describe_str.contains("pii-alpha") && describe_str.contains("pii-beta"),
        "describe must decode execution input + history payloads: {describe_str}"
    );
    assert!(
        !describe_str.contains("_harvest_codec_envelope"),
        "describe must not leak envelopes: {describe_str}"
    );

    // The dedicated paginated history endpoint too.
    let (status, history) = get_json(&app, &format!("/workflows/{exec_id}/history")).await;
    assert_eq!(status, StatusCode::OK, "body: {history}");
    let history_str = serde_json::to_string(&history).unwrap();
    assert!(
        history_str.contains("pii-beta"),
        "history page must decode event payloads: {history_str}"
    );
    assert!(
        !history_str.contains("_harvest_codec_envelope"),
        "{history_str}"
    );
}

/// AC4 (part 2): the stack/describe pending-activity input and heartbeat
/// checkpoint are decoded.
#[tokio::test]
async fn stack_pending_activity_input_is_decoded() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "stack-decoded", json!({})).await;
    let activity_input = envelope_for(&json!({"card": "pii-alpha"}));
    // Seed the pending task-queue row backing the stack panel, with a stored
    // heartbeat checkpoint that is itself an envelope (decoded before the
    // #503 cap projection).
    let activity_id = seed_pending_activity(
        &mut conn,
        exec_id,
        activity_input.clone(),
        Some(envelope_for(&json!({"progress": "pii-checkpoint"}))),
    )
    .await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "charge_card".to_string(),
            input: activity_input,
            queue: "default".to_string(),
        }],
    )
    .await;

    let (status, stack) = get_json(&app, &format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK, "body: {stack}");
    let stack_str = serde_json::to_string(&stack).unwrap();
    assert!(
        stack_str.contains("pii-alpha"),
        "pending-activity input must be decoded on the stack surface: {stack_str}"
    );
    assert!(
        stack_str.contains("pii-checkpoint"),
        "the heartbeat checkpoint must be decoded on the stack surface: {stack_str}"
    );
    assert!(
        !stack_str.contains("_harvest_codec_envelope"),
        "stack must not leak envelopes: {stack_str}"
    );
    // A decode-active response carries the input-budget signaling fields
    // (PR #936 review): the small seeded input is within its per-activity cap
    // and the cumulative per-response budget, so it passes through unclipped.
    let pending = &stack["pending_activities"][0];
    assert_eq!(
        pending["input_truncated"],
        json!(false),
        "small input must not be truncated per-field: {stack_str}"
    );
    assert_eq!(
        pending["input_omitted_for_budget"],
        json!(false),
        "small input must not be withheld for budget: {stack_str}"
    );
    assert!(
        pending["input_bytes"].as_u64().is_some_and(|b| b > 0),
        "the decoded input's observed size must be reported: {stack_str}"
    );
    assert_eq!(
        stack["inputs_truncated_for_budget"],
        json!(false),
        "the response-level input-budget flag must be present (and false) \
         when decoding is active: {stack_str}"
    );
}

/// #773 (P1): a retrying (backing-off) REGULAR pending activity surfaces
/// `last_failure` sourced from the task-row `harvest_task_queue.error` column —
/// NOT from an `ActivityFailed` event, which a retryable failure never appends.
/// The regular-path shape is: `error`/`attempt` from the task row,
/// `error_type="Error"`, `non_retryable=false`, and `failed_at`/`details`
/// omitted. A never-failed pending activity omits the field entirely
/// (AC3/AC7). No decoding needed — plaintext here — so this runs on the plain
/// app.
#[tokio::test]
async fn stack_pending_activity_surfaces_last_failure() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_plain_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // Scenario 1: a backing-off regular activity — the REAL engine state after
    // a retryable failure: PENDING task, error column set, attempt bumped,
    // scheduled_at in the future, and NO ActivityFailed event in history.
    let exec_id = seed_running(&mut conn, "stack-last-failure", json!({})).await;
    seed_backing_off_activity(
        &mut conn,
        exec_id,
        json!({"card": "1234"}),
        "downstream 503",
        3,
    )
    .await;

    let (status, stack) = get_json(&app, &format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK, "body: {stack}");
    let pending = &stack["pending_activities"][0];
    let last_failure = &pending["last_failure"];
    assert!(
        last_failure.is_object(),
        "a backing-off pending activity must carry last_failure: {stack}"
    );
    assert_eq!(last_failure["error"], json!("downstream 503"));
    assert_eq!(
        last_failure["attempt"],
        json!(3),
        "attempt from the task row: {stack}"
    );
    // Regular (task-row) path defaults.
    assert_eq!(last_failure["error_type"], json!("Error"));
    assert_eq!(last_failure["non_retryable"], json!(false));
    // The task row has no failure timestamp and no structured detail — both keys
    // are omitted.
    assert!(
        last_failure.get("failed_at").is_none(),
        "regular task-row path must omit failed_at: {stack}"
    );
    assert!(
        last_failure.get("details").is_none(),
        "regular task-row path must omit details: {stack}"
    );

    // Scenario 2: a never-failed pending activity omits last_failure entirely.
    let clean_exec = seed_running(&mut conn, "stack-no-failure", json!({})).await;
    seed_pending_activity(&mut conn, clean_exec, json!({"card": "5678"}), None).await;
    let (status, clean_stack) = get_json(&app, &format!("/workflows/{clean_exec}/stack")).await;
    assert_eq!(status, StatusCode::OK, "body: {clean_stack}");
    let clean_str = serde_json::to_string(&clean_stack).unwrap();
    assert!(
        clean_str.contains("pending_activities"),
        "fixture sanity: pending activity must render: {clean_str}"
    );
    assert!(
        !clean_str.contains("last_failure"),
        "a never-failed pending activity must omit last_failure entirely: {clean_str}"
    );
}

/// #773: a pending LOCAL activity surfaces `last_failure` from the most recent
/// `LocalActivityFailed` event (local activities ARE event-sourced per attempt,
/// unlike backing-off regular activities). The event/timestamp path populates
/// `failed_at`; `error_type` defaults to `"Error"` and `non_retryable` to
/// `false`. Plaintext here, so this runs on the plain app.
#[tokio::test]
async fn stack_pending_local_activity_surfaces_last_failure() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_plain_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "stack-local-last-failure", json!({})).await;
    let activity_id = ActivityExecId::new();
    // LocalActivityScheduled (still pending — no LocalActivityCompleted) plus a
    // LocalActivityFailed for the same activity_id, so the local panel surfaces
    // it as a pending local activity with its recorded failure (AC4).
    append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "compute_checksum".to_string(),
                input: json!({"blob": "abc"}),
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id,
                error: "checksum mismatch".to_string(),
                attempt: 2,
            },
        ],
    )
    .await;

    let (status, stack) = get_json(&app, &format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK, "body: {stack}");
    let pending = &stack["pending_local_activities"][0];
    let last_failure = &pending["last_failure"];
    assert!(
        last_failure.is_object(),
        "a failed pending local activity must carry last_failure: {stack}"
    );
    assert_eq!(last_failure["error"], json!("checksum mismatch"));
    assert_eq!(last_failure["attempt"], json!(2));
    assert_eq!(last_failure["error_type"], json!("Error"));
    assert_eq!(last_failure["non_retryable"], json!(false));
    // The event-sourced path populates failed_at from the event timestamp.
    assert!(
        last_failure["failed_at"].is_string(),
        "local (event) path must carry failed_at: {stack}"
    );
    assert!(
        last_failure.get("details").is_none(),
        "LocalActivityFailed carries no details: {stack}"
    );
}

/// A local activity whose retry budget is exhausted closes with a terminal
/// `LocalActivityExhausted` event. If the workflow catches that failure and
/// keeps running, `/stack` must NOT report the exhausted activity as pending
/// (Codex review, PR #1022; issue #773). Before the fix the fold never
/// consumed `LocalActivityExhausted`, so the entry stayed pending forever.
#[tokio::test]
async fn stack_exhausted_local_activity_is_not_pending() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_plain_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "stack-local-exhausted", json!({})).await;
    let activity_id = ActivityExecId::new();
    // Catch-and-continue: the local activity retried, exhausted its budget
    // (terminal `LocalActivityExhausted`), and the workflow is still RUNNING.
    append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "compute_checksum".to_string(),
                input: json!({"blob": "abc"}),
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id,
                error: "checksum mismatch".to_string(),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityExhausted {
                activity_id,
                error: "checksum mismatch".to_string(),
                attempt: 2,
            },
        ],
    )
    .await;

    let (status, stack) = get_json(&app, &format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK, "body: {stack}");
    let pending = stack["pending_local_activities"]
        .as_array()
        .expect("pending_local_activities must be an array");
    assert!(
        pending.is_empty(),
        "an exhausted (terminally failed) local activity must not be reported \
         as pending: {stack}"
    );
}

/// #608 audit fidelity (Codex P2, PR #1022): a local activity that failed with
/// a stringified codec envelope and then EXHAUSTED (terminal) is removed from
/// the pending panel. Its `last_failure` must therefore NOT be decoded — and
/// so must never touch `decode_outcome` and never write a spurious
/// `payload.decode_read` audit row for data that never left the handler. Before
/// the fix the fold decoded `last_failure` inside the `LocalActivityScheduled`
/// arm, before the terminal `LocalActivityExhausted` arm removed the entry, so
/// the removed entry's envelope was decoded and audited.
#[tokio::test]
async fn stack_removed_local_activity_failure_is_not_decoded_or_audited() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // Decode-enabled admin app: the read-path decoder is active for this request.
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "stack-local-removed-no-audit", json!({})).await;
    let activity_id = ActivityExecId::new();
    // The failure error is a codec envelope that WOULD decode if surfaced, but
    // the following terminal `LocalActivityExhausted` removes the pending entry
    // (catch-and-continue; the workflow is still RUNNING).
    append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "compute_checksum".to_string(),
                input: json!({"blob": "abc"}),
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id,
                error: stringified_envelope(&json!({"secret": "pii-alpha"})),
                attempt: 1,
            },
            WorkflowEvent::LocalActivityExhausted {
                activity_id,
                error: stringified_envelope(&json!({"secret": "pii-alpha"})),
                attempt: 2,
            },
        ],
    )
    .await;

    let (status, stack) = get_json(&app, &format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK, "body: {stack}");
    // (a) The exhausted local activity is not surfaced.
    let pending = stack["pending_local_activities"]
        .as_array()
        .expect("pending_local_activities must be an array");
    assert!(
        pending.is_empty(),
        "an exhausted (terminally failed) local activity must not be reported \
         as pending: {stack}"
    );
    // (b) No decode of the removed entry ⇒ no plaintext leak on the surface …
    let stack_str = serde_json::to_string(&stack).unwrap();
    assert!(
        !stack_str.contains("pii-alpha") && !stack_str.contains("_harvest_codec_envelope"),
        "the removed entry's failure must neither be surfaced nor decoded: {stack_str}"
    );
    // … and, crucially, NO payload.decode_read audit row for a request that
    // surfaced nothing decodable (audit_decoded_read no-ops on an untouched
    // outcome, so a row means the removed entry's failure was decoded).
    let audit = decode_audit_rows(&mut conn).await;
    assert!(
        audit.is_empty(),
        "a stack read whose only envelope belongs to a REMOVED local activity \
         must not write a payload.decode_read audit row: {audit:?}"
    );
}

/// AC5: `GET /dead-letters` decodes the failed task's input (JSONB) and
/// error (TEXT).
#[tokio::test]
async fn dead_letters_list_decodes_input_and_error() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "dlq-decoded", json!({})).await;
    autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: Some(exec_id.as_uuid()),
            activity_name: Some("charge_card".to_string()),
            input: envelope_for(&json!({"card": "pii-alpha"})),
            error: stringified_envelope(&json!("card declined: pii-beta")),
            attempts: 3,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("seed dead letter");

    let (status, body) = get_json(&app, "/dead-letters").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let body_str = serde_json::to_string(&body).unwrap();
    assert!(
        body_str.contains("pii-alpha"),
        "DLQ input must be decoded: {body_str}"
    );
    assert!(
        body_str.contains("pii-beta"),
        "DLQ TEXT error must be decoded: {body_str}"
    );
    assert!(
        !body_str.contains("_harvest_codec_envelope"),
        "DLQ list must not leak envelopes: {body_str}"
    );
}

/// AC4 (SSE): the live event stream emits decoded frames, and one
/// `payload.decode_read` audit row is written at stream open when decode mode
/// is active (mirrors the `OP_EXECUTION_STREAM_OPEN` one-row-per-stream shape).
#[tokio::test]
async fn sse_stream_emits_decoded_frames_and_writes_decode_audit_at_open() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app_with(
        &pool,
        &AppConfig {
            admin: true,
            codecs: true,
            decode_on_read: true,
            notification_url: Some(&url),
        },
    );
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // A terminal execution: the stream backfills the recorded events and then
    // closes with stream-end, so the whole body can be read to completion.
    let exec_id = seed_running(&mut conn, "sse-decoded", json!({})).await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: envelope_for(&json!({"pii": "pii-alpha"})),
        }],
    )
    .await;
    mark_completed(&mut conn, exec_id, json!(null)).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/executions/{exec_id}/events/stream"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("open SSE stream");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("terminal execution's stream must close after backfill")
    .expect("read SSE body");
    let stream_text = String::from_utf8_lossy(&bytes);

    assert!(
        stream_text.contains("pii-alpha"),
        "SSE frames must be decoded (backfill + live paths): {stream_text}"
    );
    assert!(
        !stream_text.contains("_harvest_codec_envelope"),
        "SSE frames must not leak envelopes: {stream_text}"
    );

    let audit = decode_audit_rows(&mut conn).await;
    assert_eq!(
        audit.len(),
        1,
        "one payload.decode_read row at stream open when decode mode is active: {audit:?}"
    );
    assert_eq!(audit[0].0.as_deref(), Some(exec_id.to_string().as_str()));
}

/// AC6: decode-only-when-admin. On an ungated route, a non-admin caller
/// (flag on, no session) receives today's bytes — the stored ciphertext.
#[tokio::test]
async fn non_admin_request_on_ungated_route_receives_encoded_payloads() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "non-admin", json!({})).await;
    mark_completed(
        &mut conn,
        exec_id,
        envelope_for(&json!({"pii": "pii-alpha"})),
    )
    .await;

    // Non-admin: /result and describe are NOT admin-gated, so they answer —
    // but with ciphertext, exactly as today.
    let non_admin = build_non_admin_decode_app(&pool);
    for uri in [
        format!("/workflows/{exec_id}/result"),
        format!("/workflows/{exec_id}"),
    ] {
        let (status, bytes) = get_raw(&non_admin, &uri).await;
        assert_eq!(status, StatusCode::OK, "ungated route must still answer");
        let body = String::from_utf8_lossy(&bytes);
        assert!(
            body.contains("_harvest_codec_envelope"),
            "non-admin caller must see ciphertext on {uri}: {body}"
        );
        assert!(
            !body.contains("pii-alpha"),
            "non-admin caller must never see plaintext on {uri}: {body}"
        );
    }

    // No decode happened ⇒ no plaintext-read audit row for the non-admin calls.
    let audit = decode_audit_rows(&mut conn).await;
    assert!(
        audit.is_empty(),
        "non-admin reads must not write decode audit rows: {audit:?}"
    );

    // The same request with admin access decodes.
    let admin = build_decode_app(&pool);
    let (status, bytes) = get_raw(&admin, &format!("/workflows/{exec_id}/result")).await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("pii-alpha") && !body.contains("_harvest_codec_envelope"),
        "admin caller must see plaintext: {body}"
    );
}

/// AC8: exactly one audit row per request that decoded ≥1 envelope, and the
/// row records the access — never the payload content.
#[tokio::test]
async fn audit_row_written_once_per_request_and_contains_no_payload_content() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let ciphertext_b64 = envelope_for(&json!({"pii": "pii-alpha"}))["data"]
        .as_str()
        .expect("envelope data is base64 text")
        .to_string();

    let exec_id = seed_running(
        &mut conn,
        "audit-once",
        envelope_for(&json!({"pii": "pii-alpha"})),
    )
    .await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: envelope_for(&json!({"pii": "pii-alpha"})),
        }],
    )
    .await;

    // One describe request touching two envelopes ⇒ exactly one row.
    let (status, _body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK);

    let audit = decode_audit_rows(&mut conn).await;
    assert_eq!(
        audit.len(),
        1,
        "one row per request, regardless of how many fields decoded: {audit:?}"
    );
    let (target_id, route, error_summary) = &audit[0];
    assert_eq!(
        target_id.as_deref(),
        Some(exec_id.to_string().as_str()),
        "the audit row must name the execution whose plaintext was read"
    );
    for field in [
        route.as_str(),
        target_id.as_deref().unwrap_or_default(),
        error_summary.as_deref().unwrap_or_default(),
    ] {
        assert!(
            !field.contains("pii-alpha") && !field.contains(&ciphertext_b64),
            "audit rows must never record payload content (plain or cipher): {audit:?}"
        );
    }
}

/// D2 predicate: a request that decodes zero envelopes writes zero audit rows
/// — flipping the flag on a non-encrypted deployment is audit-silent.
#[tokio::test]
async fn no_audit_row_when_no_envelopes_present() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "no-envelopes", json!({"plain": "data"})).await;
    mark_completed(&mut conn, exec_id, json!({"plain": "output"})).await;

    let (status, _body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _body) = get_json(&app, &format!("/workflows/{exec_id}/result")).await;
    assert_eq!(status, StatusCode::OK);

    let audit = decode_audit_rows(&mut conn).await;
    assert!(
        audit.is_empty(),
        "no envelopes touched ⇒ no payload.decode_read rows: {audit:?}"
    );
}

/// AC7: a decode failure degrades per-field — the response is still 200, the
/// good sibling decodes, and the bad field carries the typed marker.
#[tokio::test]
async fn bad_envelope_degrades_per_field_response_still_200_with_marker() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "degrade", json!({})).await;
    append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "good".to_string(),
                input: envelope_for(&json!({"ok": "pii-alpha"})),
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "bad".to_string(),
                input: unknown_codec_envelope(),
                queue: "default".to_string(),
            },
        ],
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/history")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "one bad envelope must never 500 the history: {body}"
    );
    let body_str = serde_json::to_string(&body).unwrap();
    assert!(
        body_str.contains("pii-alpha"),
        "the good sibling still decodes: {body_str}"
    );
    assert!(
        body_str.contains(UNDECODABLE_MARKER_KEY),
        "the bad field degrades to the typed marker: {body_str}"
    );
    assert!(
        body_str.contains(UNDECODABLE_REASON_UNKNOWN_CODEC)
            && body_str.contains("kms-rotated-away"),
        "the marker names the codec and a bounded reason: {body_str}"
    );
}

/// The append-only contract: decoding happens on the response copy only —
/// stored rows keep their ciphertext byte-for-byte.
#[tokio::test]
async fn read_with_decode_leaves_stored_rows_untouched() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let output_envelope = envelope_for(&json!({"pii": "pii-alpha"}));
    let exec_id = seed_running(&mut conn, "stored-untouched", json!({})).await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: output_envelope.clone(),
        }],
    )
    .await;
    mark_completed(&mut conn, exec_id, output_envelope.clone()).await;

    // Decoded reads across the surfaces.
    for uri in [
        format!("/workflows/{exec_id}"),
        format!("/workflows/{exec_id}/result"),
        format!("/workflows/{exec_id}/history"),
        format!("/workflows/{exec_id}/history/export?payload_policy=full"),
    ] {
        let (status, _bytes) = get_raw(&app, &uri).await;
        assert_eq!(status, StatusCode::OK, "read {uri}");
    }

    // The stored execution row still carries the envelope…
    let stored_output: Option<Value> = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::output)
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        stored_output.as_ref(),
        Some(&output_envelope),
        "stored execution output must remain ciphertext"
    );

    // …and so does the stored event row.
    let stored_events: Vec<Value> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select(harvest_events::event_data)
        .load(&mut conn)
        .await
        .unwrap();
    let completed = stored_events
        .iter()
        .find(|e| e["type"] == "WorkflowCompleted")
        .expect("stored WorkflowCompleted event");
    assert_eq!(
        completed["data"]["output"], output_envelope,
        "stored event payload must remain ciphertext byte-for-byte"
    );
}

/// PR #936 review (P1): the export path must load history *raw* so an
/// encrypted deployment can still export with the flag off — the stored
/// envelopes ride along verbatim (the same ciphertext describe/history/SSE
/// already surface) instead of the whole export failing `UnknownPayloadCodec`
/// before any policy runs. No decode happened, so no decode-audit row either.
#[tokio::test]
async fn history_export_flag_off_returns_stored_envelopes_verbatim() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_flag_off_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "export-flag-off", json!({})).await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: envelope_for(&json!({"pii": "pii-alpha"})),
        }],
    )
    .await;
    mark_completed(&mut conn, exec_id, json!(null)).await;

    // Full: envelopes pass through byte-identical (ciphertext, no plaintext).
    let (status, bytes) = get_raw(
        &app,
        &format!("/workflows/{exec_id}/history/export?payload_policy=full"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an encrypted history must still export with the flag off"
    );
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("_harvest_codec_envelope"),
        "flag-off full export must carry the stored envelope verbatim: {body}"
    );
    assert!(
        !body.contains("pii-alpha"),
        "flag-off full export must never contain plaintext: {body}"
    );

    // Redacted: the payload field is replaced wholesale — no envelope, no
    // plaintext — without ever needing a codec.
    let (status, bytes) = get_raw(
        &app,
        &format!("/workflows/{exec_id}/history/export?payload_policy=redacted"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        !body.contains("_harvest_codec_envelope") && !body.contains("pii-alpha"),
        "flag-off redacted export must carry neither envelope nor plaintext: {body}"
    );

    let audit = decode_audit_rows(&mut conn).await;
    assert!(
        audit.is_empty(),
        "flag-off exports must not write payload.decode_read audit rows: {audit:?}"
    );
}

/// PR #936 review (P2): the decode audit row is written through the caller's
/// already-held connection. With a pool sized to exactly one connection, a
/// decoded read that acquired a *second* connection for the audit would stall
/// until the pool timeout and silently skip the row — so this pins both the
/// prompt 200 and the audit row's presence under a size-1 pool.
#[tokio::test]
async fn decode_audit_reuses_callers_connection_under_single_connection_pool() {
    let (url, _container) = setup_database().await;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
    let pool: DbPool = deadpool::managed::Pool::builder(manager)
        .max_size(1)
        .build()
        .expect("pool should build");
    let app = build_decode_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "single-conn-pool", json!({})).await;
    append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: envelope_for(&json!({"pii": "pii-alpha"})),
        }],
    )
    .await;
    mark_completed(
        &mut conn,
        exec_id,
        envelope_for(&json!({"pii": "pii-alpha"})),
    )
    .await;

    // Exercise every surface whose handler holds its read connection across
    // the decode-audit await (describe, /history, /history/export, /stack).
    let deadline = std::time::Duration::from_secs(10);
    for uri in [
        format!("/workflows/{exec_id}"),
        format!("/workflows/{exec_id}/history"),
        format!("/workflows/{exec_id}/history/export?payload_policy=full"),
        format!("/workflows/{exec_id}/stack"),
    ] {
        let (status, bytes) = tokio::time::timeout(deadline, get_raw(&app, &uri))
            .await
            .unwrap_or_else(|_| {
                panic!("decoded read of {uri} stalled — audit must reuse the caller's connection")
            });
        assert_eq!(status, StatusCode::OK, "read {uri}");
        let body = String::from_utf8_lossy(&bytes);
        assert!(
            !body.contains("_harvest_codec_envelope"),
            "decoded read of {uri} must not leak the envelope: {body}"
        );
    }

    // The audit rows landed (they were not silently skipped on a pool stall):
    // describe, /history, and the export each decoded ≥1 envelope. (/stack
    // found none for this fixture — its predicate keeps it audit-silent.)
    let audit = decode_audit_rows(&mut conn).await;
    assert_eq!(
        audit.len(),
        3,
        "one decode-audit row per decoding read must land: {audit:?}"
    );
}
