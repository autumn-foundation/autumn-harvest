//! HTTP integration tests for operator re-run of a terminal workflow (issue #777).
//!
//! Exercises `POST /workflows/{id}/rerun` end-to-end: a brand-new execution is
//! started from a terminal source run's recorded start parameters, the source
//! is sealed to `CONTINUED_AS_NEW` when the business key is reused (with its
//! `completed_at` / `output` / `error` preserved), and provenance is stamped
//! via the issue #740 columns.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (run this file `--test-threads=1`, since one test
//! mutates the process-global admission-gate cache); otherwise a fresh
//! testcontainers Postgres is booted with the full migration set (Docker).

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
// `rerun_respects_admission_gate` holds `TEST_SERIAL` across `.await` points to
// serialize its process-global gate-cache mutation — same as the admission-gate
// suites (`start_idempotency_integration.rs`, `admission_gate_authoritative_localpg.rs`).
#![allow(clippy::await_holding_lock)]

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowInfo, context::WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Utc};
use diesel::sql_types::{Jsonb, Nullable, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// Serializes the one test that mutates the process-global admission-gate
/// cache against any future gate-manipulating sibling in this binary.
static TEST_SERIAL: Mutex<()> = Mutex::new(());

const TEST_ACTOR: &str = "rerun-operator";

type HarvestApiApp = axum::Router;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn plain_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: dummy_workflow,
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

/// A stored workflow-level retry policy (issue #523), in the exact serde shape
/// `RetryPolicy` round-trips through, so a faithful clone compares equal.
fn retry_policy_json() -> Value {
    serde_json::to_value(autumn_harvest::RetryPolicy::exponential(
        3,
        std::time::Duration::from_secs(1),
    ))
    .expect("RetryPolicy serialises")
}

/// The published input schema used by the issue #373 override-validation tests:
/// an object requiring an integer `order`.
fn order_input_schema() -> Value {
    json!({
        "type": "object",
        "required": ["order"],
        "properties": { "order": { "type": "integer" } }
    })
}

/// A workflow whose `WorkflowInfo` publishes [`order_input_schema`].
fn schema_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        input_schema: Some(order_input_schema),
        ..plain_info(name)
    }
}

/// A workflow carrying a per-key concurrency policy (issue #247).
fn concurrency_info(name: &'static str, key_expr: &'static str, limit: u32) -> WorkflowInfo {
    WorkflowInfo {
        concurrency: Some(autumn_harvest::concurrency::ConcurrencyPolicy::new(
            key_expr, limit,
        )),
        ..plain_info(name)
    }
}

fn api_state_with(
    infos: Vec<WorkflowInfo>,
    pool: &DbPool,
    admin_boundary: bool,
) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(admin_boundary);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("rerun-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

fn build_app(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    harvest_api_router(api_state_with(infos, pool, true))
        .with_state(AppState::for_test().with_profile("test"))
}

/// Extra knobs a few tests need beyond `build_app`'s defaults.
#[derive(Default)]
struct AppOpts {
    /// Lower the effective workflow-input byte cap (issue #252) so an oversized
    /// payload can be exercised without building a multi-megabyte JSON string.
    max_workflow_input_bytes: Option<u64>,
    /// Names to register as unified DAGs. `HarvestApiRuntime::new` folds a
    /// schedule's `dag_name` into `registered_dag_names`, which is exactly what
    /// `is_registered_dag` consults.
    dag_names: Vec<&'static str>,
}

fn build_app_with(pool: &DbPool, infos: Vec<WorkflowInfo>, opts: &AppOpts) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let mut registry = HandlerRegistry::new(infos, vec![]);
    if let Some(cap) = opts.max_workflow_input_bytes {
        registry = registry.with_payload_caps(cap, cap, cap, cap);
    }
    let schedules: Vec<autumn_harvest::WorkflowSchedule> = opts
        .dag_names
        .iter()
        .map(|name| autumn_harvest::WorkflowSchedule {
            dag_name: Some((*name).to_string()),
            ..autumn_harvest::WorkflowSchedule::new(
                (*name).to_string(),
                autumn_harvest::Schedule::Manual,
            )
        })
        .collect();
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(schedules),
        Some("rerun-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// An app whose admin-auth boundary is NOT installed, so the `require_admin`
/// route layer rejects an unauthenticated caller (mirrors `security.rs`).
fn build_app_no_admin(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    harvest_api_router(api_state_with(infos, pool, false))
        .with_state(AppState::for_test().with_profile("test"))
}

/// An app whose completion-callback SSRF policy allowlists `hook.example`
/// (issue #605), for tests whose seeded `completion_callbacks` must survive
/// re-run validation intact rather than exercising `build_app`'s default
/// (empty-allowlist, always-reject) posture — see R-64.
fn build_app_allowing_hook_example(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    let api_state = api_state_with(infos, pool, true);
    api_state.set_completion_callback_ssrf_policy(
        autumn_harvest::completion_callback::SsrfPolicy::new(
            autumn_harvest::completion_callback::HostAllowlist::new().with_pattern("hook.example"),
        ),
    );
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
    read_response(response).await
}

async fn post_json_unauth(app: &HarvestApiApp, uri: &str, body: Value) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request")
        .status()
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    read_response(response).await
}

async fn read_response(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let jsonv = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, jsonv)
}

// ── Seeding ──────────────────────────────────────────────────────────────────

/// Column values a seeded terminal source row may carry beyond the defaults.
#[derive(Default)]
struct Seed {
    input: Option<Value>,
    output: Option<Value>,
    error: Option<String>,
    queue_name: Option<&'static str>,
    memo: Option<Value>,
    search_attrs: Option<Value>,
    execution_timeout_secs: Option<i64>,
    sla_secs: Option<i64>,
    owner: Option<&'static str>,
    runbook_url: Option<&'static str>,
    severity: Option<&'static str>,
    completion_callbacks: Option<Value>,
    context_headers: Option<Value>,
    workflow_retry_policy: Option<Value>,
    chain_execution_timeout_secs: Option<i64>,
    chain_deadline_at: Option<DateTime<Utc>>,
    schedule_id: Option<uuid::Uuid>,
    scheduled_for: Option<DateTime<Utc>>,
    origin: Option<&'static str>,
    completed_at: Option<DateTime<Utc>>,
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

/// Seed a terminal (or non-terminal) execution on shard 0 with one
/// `WorkflowStarted` event, returning its wire-form execution id.
async fn seed_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    seed: Seed,
) -> String {
    let now = Utc::now();
    let completed_at = if matches!(state, "RUNNING" | "PAUSED") {
        None
    } else {
        Some(seed.completed_at.unwrap_or(now))
    };
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
             output, error, queue_name, memo, search_attrs,
             execution_timeout, sla, owner, runbook_url, severity,
             completion_callbacks, context_headers, workflow_retry_policy,
             schedule_id, scheduled_for, origin,
             chain_execution_timeout, chain_deadline_at)
         VALUES ($1, $2, 0, $3, $4, $5, $6,
                 $7, $8, COALESCE($9, 'default'), $10, $11,
                 make_interval(secs => $12::double precision),
                 make_interval(secs => $13::double precision),
                 $14, $15, $16,
                 $17, $18, $19,
                 $20, $21, $22,
                 make_interval(secs => $23::double precision), $24)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .bind::<Jsonb, _>(seed.input.unwrap_or_else(|| json!({})))
    .bind::<Timestamptz, _>(now)
    .bind::<Nullable<Timestamptz>, _>(completed_at)
    .bind::<Nullable<Jsonb>, _>(seed.output)
    .bind::<Nullable<Text>, _>(seed.error)
    .bind::<Nullable<Text>, _>(seed.queue_name)
    .bind::<Nullable<Jsonb>, _>(seed.memo)
    .bind::<Nullable<Jsonb>, _>(seed.search_attrs)
    .bind::<Nullable<diesel::sql_types::BigInt>, _>(seed.execution_timeout_secs)
    .bind::<Nullable<diesel::sql_types::BigInt>, _>(seed.sla_secs)
    .bind::<Nullable<Text>, _>(seed.owner)
    .bind::<Nullable<Text>, _>(seed.runbook_url)
    .bind::<Nullable<Text>, _>(seed.severity)
    .bind::<Nullable<Jsonb>, _>(seed.completion_callbacks)
    .bind::<Nullable<Jsonb>, _>(seed.context_headers)
    .bind::<Nullable<Jsonb>, _>(seed.workflow_retry_policy)
    .bind::<Nullable<diesel::sql_types::Uuid>, _>(seed.schedule_id)
    .bind::<Nullable<Timestamptz>, _>(seed.scheduled_for)
    .bind::<Nullable<Text>, _>(seed.origin)
    .bind::<Nullable<diesel::sql_types::BigInt>, _>(seed.chain_execution_timeout_secs)
    .bind::<Nullable<Timestamptz>, _>(seed.chain_deadline_at)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted',
                 '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{},\"timestamp\":\"2026-01-01T00:00:00Z\"}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert event");

    autumn_harvest::types::ExecutionId::from_uuid(id).to_string()
}

/// Seed a plain terminal source with default columns.
async fn seed_terminal(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
) -> String {
    seed_execution(conn, workflow_name, workflow_id, state, Seed::default()).await
}

/// Seed a minimal execution row whose `retry_of_exec_id` points at
/// `predecessor` (issue #523's workflow-level retry chain), so a re-run of
/// `predecessor` can be shown to detect an in-flight or already-run automatic
/// retry successor (Codex review, issue #777 PR #1152). `state` need not be
/// terminal — the retry-chain gate under test disqualifies on EXISTENCE of a
/// successor row, not on its state.
async fn seed_retry_successor(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    predecessor: &str,
    state: &str,
) -> String {
    let now = Utc::now();
    let completed_at = if matches!(state, "RUNNING" | "PAUSED") {
        None
    } else {
        Some(now)
    };
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
             queue_name, retry_of_exec_id, workflow_attempt)
         VALUES ($1, $2, 0, $3, '{}'::jsonb, $4, $5, 'default', $6::uuid, 2)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .bind::<Timestamptz, _>(now)
    .bind::<Nullable<Timestamptz>, _>(completed_at)
    .bind::<Text, _>(predecessor)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert retry successor")
    .id;

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted',
                 '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{},\"timestamp\":\"2026-01-01T00:00:00Z\"}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert event");

    autumn_harvest::types::ExecutionId::from_uuid(id).to_string()
}

// ── Row readers ──────────────────────────────────────────────────────────────

#[derive(diesel::QueryableByName, Debug)]
struct ExecRow {
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Text)]
    workflow_id: String,
    #[diesel(sql_type = Text)]
    queue_name: String,
    #[diesel(sql_type = Jsonb)]
    input: Value,
    #[diesel(sql_type = Nullable<Jsonb>)]
    output: Option<Value>,
    #[diesel(sql_type = Nullable<Text>)]
    error: Option<String>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    completed_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    memo: Option<Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    search_attrs: Option<Value>,
    #[diesel(sql_type = Nullable<diesel::sql_types::BigInt>)]
    execution_timeout_secs: Option<i64>,
    #[diesel(sql_type = Nullable<diesel::sql_types::BigInt>)]
    sla_secs: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    owner: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    runbook_url: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    severity: Option<String>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    completion_callbacks: Option<Value>,
    #[diesel(sql_type = Nullable<Text>)]
    start_source: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    start_source_ref: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    started_by: Option<String>,
    #[diesel(sql_type = Nullable<diesel::sql_types::Uuid>)]
    schedule_id: Option<uuid::Uuid>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    scheduled_for: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Text>)]
    origin: Option<String>,
    #[diesel(sql_type = Nullable<diesel::sql_types::Uuid>)]
    continued_from_exec_id: Option<uuid::Uuid>,
    #[diesel(sql_type = Nullable<diesel::sql_types::Uuid>)]
    first_exec_id: Option<uuid::Uuid>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    workflow_attempt: i32,
    #[diesel(sql_type = Nullable<Jsonb>)]
    context_headers: Option<Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    workflow_retry_policy: Option<Value>,
    #[diesel(sql_type = Nullable<diesel::sql_types::BigInt>)]
    chain_execution_timeout_secs: Option<i64>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    chain_deadline_at: Option<DateTime<Utc>>,
}

const EXEC_SELECT: &str = "SELECT state, workflow_id, queue_name, input, output, error,
        completed_at, memo, search_attrs,
        EXTRACT(EPOCH FROM execution_timeout)::bigint AS execution_timeout_secs,
        EXTRACT(EPOCH FROM sla)::bigint AS sla_secs,
        owner, runbook_url, severity, completion_callbacks,
        start_source, start_source_ref, started_by,
        schedule_id, scheduled_for, origin,
        continued_from_exec_id, first_exec_id, workflow_attempt,
        context_headers, workflow_retry_policy,
        EXTRACT(EPOCH FROM chain_execution_timeout)::bigint AS chain_execution_timeout_secs,
        chain_deadline_at
     FROM harvest_workflow_executions ";

async fn load_exec(conn: &mut AsyncPgConnection, exec_id: &str) -> ExecRow {
    diesel::sql_query(format!("{EXEC_SELECT} WHERE id = $1::uuid"))
        .bind::<Text, _>(exec_id)
        .get_result(conn)
        .await
        .expect("load execution")
}

/// Load every execution under one `(workflow_name, workflow_id)` business key.
///
/// Scoped to the KEY, never just the workflow name: `HARVEST_TEST_DATABASE_URL`
/// points at a shared, persistent database, so a name-scoped count would
/// accumulate rows from previous runs of this suite and make the "no new run
/// was created" assertions drift. Every test mints a `uuid`-suffixed
/// `workflow_id`, so the key is unique per test invocation.
async fn load_execs_for_key(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> Vec<ExecRow> {
    diesel::sql_query(format!(
        "{EXEC_SELECT} WHERE workflow_name = $1 AND workflow_id = $2 ORDER BY started_at"
    ))
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .get_results(conn)
    .await
    .expect("load executions")
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn event_count(conn: &mut AsyncPgConnection, exec_id: &str) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events WHERE workflow_exec_id = $1::uuid")
        .bind::<Text, _>(exec_id)
        .get_result::<CountRow>(conn)
        .await
        .unwrap()
        .n
}

#[derive(diesel::QueryableByName)]
struct EventRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    event_id: i32,
    #[diesel(sql_type = Text)]
    event_type: String,
}

async fn events_of(conn: &mut AsyncPgConnection, exec_id: &str) -> Vec<EventRow> {
    diesel::sql_query(
        "SELECT event_id, event_type FROM harvest_events
         WHERE workflow_exec_id = $1::uuid ORDER BY event_id",
    )
    .bind::<Text, _>(exec_id)
    .get_results(conn)
    .await
    .unwrap()
}

/// Task-queue rows for one execution, so a re-run can be shown to be actually
/// DISPATCHABLE (a new execution row with no queued task would never run).
#[derive(diesel::QueryableByName, Debug)]
struct TaskRow {
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Text)]
    task_type: String,
    /// Per-key concurrency group key (issue #247) — resolved at start time from
    /// the EFFECTIVE input and stamped on the task row, not the execution row.
    #[diesel(sql_type = Nullable<Text>)]
    concurrency_key: Option<String>,
}

async fn tasks_of(conn: &mut AsyncPgConnection, exec_id: &str) -> Vec<TaskRow> {
    diesel::sql_query(
        "SELECT state, task_type, concurrency_key FROM harvest_task_queue
         WHERE workflow_exec_id = $1::uuid ORDER BY created_at",
    )
    .bind::<Text, _>(exec_id)
    .get_results(conn)
    .await
    .expect("load tasks")
}

#[derive(diesel::QueryableByName, Debug)]
struct AuditRow {
    #[diesel(sql_type = Text)]
    actor: String,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Nullable<Text>)]
    error_summary: Option<String>,
}

async fn audit_rows(conn: &mut AsyncPgConnection, operation: &str, target: &str) -> Vec<AuditRow> {
    diesel::sql_query(
        "SELECT actor, status, error_summary FROM harvest_audit_log
         WHERE operation = $1 AND target_id = $2 ORDER BY occurred_at",
    )
    .bind::<Text, _>(operation)
    .bind::<Text, _>(target)
    .get_results(conn)
    .await
    .unwrap()
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

fn rerun_uri(exec_id: &str) -> String {
    format!("/workflows/{exec_id}/rerun")
}

// ── Multi-shard harness (Codex review, PR #1152) ────────────────────────────
//
// A second, fully-migrated shard database on the SAME Postgres server as
// `admin_url`, mirroring `lineage_tree_integration.rs`'s multi-shard harness.
// Every other test in this file is single-shard; these helpers are additive
// and touch nothing the rest of the suite depends on.

/// Admin URL of a Postgres that can `CREATE DATABASE`, plus a keep-alive guard.
async fn setup_shard_server() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// Create a fresh, fully-migrated database off `admin_url` and return its URL.
async fn create_shard_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;
    let (prefix, _) = admin_url
        .rsplit_once('/')
        .expect("url has a database segment");
    let url = format!("{prefix}/{name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh shard database");
    let bundle = String::from_utf8(init_sql()).expect("migration bundle is utf-8");
    conn.batch_execute(&bundle)
        .await
        .expect("apply migration bundle");
    url
}

fn two_shard_router() -> ShardRouter {
    let shards = vec![
        autumn_harvest::types::ShardId::new(0),
        autumn_harvest::types::ShardId::new(1),
    ];
    ShardRouter::new(
        shards.clone(),
        shards,
        autumn_harvest::types::ShardId::new(0),
    )
}

fn build_multi_shard_app(
    pool0: &DbPool,
    pool1: &DbPool,
    infos: Vec<WorkflowInfo>,
) -> HarvestApiApp {
    let mut pools = std::collections::BTreeMap::new();
    pools.insert(autumn_harvest::types::ShardId::new(0), pool0.clone());
    pools.insert(autumn_harvest::types::ShardId::new(1), pool1.clone());
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(
        autumn_harvest::shard::ShardedDbPool::from_map(
            pools,
            autumn_harvest::types::ShardId::new(0),
        ),
    ));
    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("rerun-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        two_shard_router(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Variant of [`build_multi_shard_app`] whose SHARDED-POOL default is shard 1
/// (independent of the router's own default, which only affects id-less
/// `pick_for_new_workflow` routing — irrelevant here). For tests that need
/// `HarvestDbPool::default_pool()` specifically to resolve to a given shard,
/// while an execution explicitly encoded onto a DIFFERENT shard still routes
/// correctly via `pool_for_execution`.
fn build_multi_shard_app_default_shard1(
    pool0: &DbPool,
    pool1: &DbPool,
    infos: Vec<WorkflowInfo>,
) -> HarvestApiApp {
    let mut pools = std::collections::BTreeMap::new();
    pools.insert(autumn_harvest::types::ShardId::new(0), pool0.clone());
    pools.insert(autumn_harvest::types::ShardId::new(1), pool1.clone());
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(
        autumn_harvest::shard::ShardedDbPool::from_map(
            pools,
            autumn_harvest::types::ShardId::new(1),
        ),
    ));
    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("rerun-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        two_shard_router(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Seed a terminal (COMPLETED) source directly on shard 0's connection, with
/// an explicitly shard-0-encoded exec id (`ExecutionId::new_for_shard`)
/// rather than relying on the table's default UUID generator, whose shard
/// bits would be effectively random.
async fn seed_terminal_on_shard0(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> String {
    let exec_id =
        autumn_harvest::types::ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
    let now = Utc::now();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (id, workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
             queue_name)
         VALUES ($1, $2, $3, 0, 'COMPLETED', $4, $5, $5, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Jsonb, _>(json!({}))
    .bind::<Timestamptz, _>(now)
    .execute(conn)
    .await
    .expect("insert execution");

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted',
                 '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{},\"timestamp\":\"2026-01-01T00:00:00Z\"}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("insert event");

    exec_id.to_string()
}

/// Codex review finding (PR #1152, P1): `rerun_workflow_execution` always
/// minted the new execution's id on `source.shard_id`, ignoring where
/// `ShardRouter::pick_for_new_workflow` — the SAME function every ordinary
/// explicit-`workflow_id` start uses (`api.rs:13162`) — would route the
/// override. In a multi-shard deployment this could silently create the
/// override's execution on the WRONG shard: invisible to the override's own
/// `RejectDuplicate` uniqueness check (which only queries the source's shard
/// connection) and to by-id addressing (issue #751), which resolves a
/// `WorkflowId` target's shard via the identical hash. Fixed by rejecting a
/// `workflow_id` override that hashes to a different shard than the source.
#[tokio::test]
async fn rerun_workflow_id_override_that_hashes_to_a_different_shard_is_rejected() {
    let (admin, _guard) = setup_shard_server().await;
    let url0 = create_shard_db(&admin, &unique("rerun_x0").replace('-', "_")).await;
    let url1 = create_shard_db(&admin, &unique("rerun_x1").replace('-', "_")).await;
    let pool0 = build_pool(&url0);
    let pool1 = build_pool(&url1);
    let mut conn0 = AsyncPgConnection::establish(&url0)
        .await
        .expect("connect shard 0");
    let mut conn1 = AsyncPgConnection::establish(&url1)
        .await
        .expect("connect shard 1");

    let router = two_shard_router();
    let wf = "rr_xshard_wf";
    let source_wf_id = unique("rr-xshard-src");

    // Find an override workflow_id the router hashes to a DIFFERENT shard
    // than the source's (shard 0) — computed via the SAME hashing function
    // every ordinary start uses, not hand-picked.
    let override_id = (0..1000)
        .map(|i| format!("rr-xshard-override-{i}"))
        .find(|candidate| {
            router.pick_for_new_workflow(wf, candidate) != autumn_harvest::types::ShardId::new(0)
        })
        .expect("a candidate hashing to a different shard exists within 1000 tries");

    let source = seed_terminal_on_shard0(&mut conn0, wf, &source_wf_id).await;

    let app = build_multi_shard_app(&pool0, &pool1, vec![plain_info(wf)]);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(rerun_uri(&source))
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::from(
                    json!({ "workflow_id": override_id }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = read_response(response).await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    // `conflict_from` (autumn-web `AutumnError::bad_request_msg`) surfaces
    // the core's `Config` message under RFC 7807's "detail" key, not
    // "error" (which is reserved for a handful of hand-built JSON bodies
    // elsewhere in this file, e.g. the admission-blocked/validation-failure
    // responses).
    let msg = body["detail"].as_str().unwrap_or_default();
    assert!(
        msg.to_lowercase().contains("shard"),
        "error should name the shard mismatch: {msg}"
    );

    // No execution was created for the override key on EITHER physical
    // database — the whole point of rejecting rather than silently
    // corrupting the routing invariant.
    let on_shard0 = load_execs_for_key(&mut conn0, wf, &override_id).await;
    let on_shard1 = load_execs_for_key(&mut conn1, wf, &override_id).await;
    assert!(
        on_shard0.is_empty() && on_shard1.is_empty(),
        "no execution should exist for the override key on either shard: \
         shard0={on_shard0:?} shard1={on_shard1:?}"
    );

    // The source is untouched (state, still COMPLETED — never sealed).
    let source_row = load_exec(&mut conn0, &source).await;
    assert_eq!(source_row.state, "COMPLETED");

    // A failed audit row was written.
    let rows = audit_rows(&mut conn0, "workflow.rerun", &source).await;
    assert!(
        rows.iter().any(|r| r.status == "failed"),
        "expected a failed audit row: {rows:?}"
    );
}

/// Regression guard for the fix above: an override that hashes to the SAME
/// shard as the source must still succeed (the guard must not over-fire on
/// same-shard overrides, which is the common case even in a multi-shard
/// deployment).
#[tokio::test]
async fn rerun_workflow_id_override_that_hashes_to_the_same_shard_still_succeeds() {
    let (admin, _guard) = setup_shard_server().await;
    let url0 = create_shard_db(&admin, &unique("rerun_s0").replace('-', "_")).await;
    let url1 = create_shard_db(&admin, &unique("rerun_s1").replace('-', "_")).await;
    let pool0 = build_pool(&url0);
    let pool1 = build_pool(&url1);
    let mut conn0 = AsyncPgConnection::establish(&url0)
        .await
        .expect("connect shard 0");

    let router = two_shard_router();
    let wf = "rr_sameshard_wf";
    let source_wf_id = unique("rr-sameshard-src");

    let override_id = (0..1000)
        .map(|i| format!("rr-sameshard-override-{i}"))
        .find(|candidate| {
            router.pick_for_new_workflow(wf, candidate) == autumn_harvest::types::ShardId::new(0)
        })
        .expect("a candidate hashing to shard 0 exists within 1000 tries");

    let source = seed_terminal_on_shard0(&mut conn0, wf, &source_wf_id).await;

    let app = build_multi_shard_app(&pool0, &pool1, vec![plain_info(wf)]);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(rerun_uri(&source))
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::from(
                    json!({ "workflow_id": override_id }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = read_response(response).await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    let new_exec = body["execution_id"].as_str().unwrap().to_string();

    let rows = load_execs_for_key(&mut conn0, wf, &override_id).await;
    assert_eq!(
        rows.len(),
        1,
        "exactly one execution under the override key"
    );
    assert_eq!(
        autumn_harvest::types::ExecutionId::from_uuid(uuid::Uuid::parse_str(&new_exec).unwrap())
            .shard(),
        autumn_harvest::types::ShardId::new(0),
        "the new execution must land on the shard the router actually picked"
    );
}

/// Codex review finding (PR #1152, P2): the malformed-JSON-body audit ran
/// BEFORE the execution id was parsed and always wrote through
/// `pool.default_pool()`. When the caller's execution id IS well-formed, its
/// shard is already known — so the audit should route through THAT shard's
/// connection instead, the same way every post-id-parse failure in this
/// handler already does. Proven end to end: two shards, with the
/// SHARDED-POOL default deliberately pointed at an unreachable address while
/// the source lives on a DIFFERENT, healthy shard — the pre-fix code would
/// have silently dropped this audit entirely (`acquire_conn` on the broken
/// default pool fails and the whole `audit_rerun_failure_via_pool` call is a
/// silent no-op by design).
#[tokio::test]
async fn rerun_malformed_body_audits_via_source_shard_when_default_shard_is_unavailable() {
    let (admin, _guard) = setup_shard_server().await;
    let url0 = create_shard_db(&admin, &unique("rerun_mb0").replace('-', "_")).await;
    let pool0 = build_pool(&url0);
    // Deliberately unreachable (loopback, refused instantly) — proves the
    // audit does NOT route via the default-shard pool, which the pre-fix code
    // always used for this case regardless of whether the id was well-formed.
    let broken_pool1 = build_pool("postgres://postgres:postgres@127.0.0.1:1/nonexistent");
    let mut conn0 = AsyncPgConnection::establish(&url0)
        .await
        .expect("connect shard 0");

    let wf = "rr_mb_default_down_wf";
    let source_wf_id = unique("rr-mb-default-down");
    let source = seed_terminal_on_shard0(&mut conn0, wf, &source_wf_id).await;

    let app = build_multi_shard_app_default_shard1(&pool0, &broken_pool1, vec![plain_info(wf)]);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(rerun_uri(&source))
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::from("{\"input\": }"))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = read_response(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    assert_eq!(
        load_execs_for_key(&mut conn0, wf, &source_wf_id)
            .await
            .len(),
        1,
        "a malformed body must start nothing"
    );

    // The failed audit row landed on the SOURCE's own (healthy) shard even
    // though the sharded-pool default (shard 1) is unreachable — this would
    // have been silently dropped entirely before the fix.
    let rows = audit_rows(&mut conn0, "workflow.rerun", &source).await;
    assert_eq!(
        rows.len(),
        1,
        "a malformed body must still be audited via the source's own shard \
         even when the default shard is unavailable"
    );
    assert_eq!(rows[0].status, "failed");
}

/// A router matching [`two_shard_router`]'s shard SET but with shard 0
/// removed from `writable_shards` — a deployment mid-drain of shard 0.
fn two_shard_router_shard0_drained() -> ShardRouter {
    ShardRouter::new(
        vec![
            autumn_harvest::types::ShardId::new(0),
            autumn_harvest::types::ShardId::new(1),
        ],
        vec![autumn_harvest::types::ShardId::new(1)],
        autumn_harvest::types::ShardId::new(0),
    )
}

/// The mirror image: shard 1 drained, shard 0 (where every test source in
/// this section lives) stays writable — the negative control proving the
/// gate below checks the SOURCE's own shard, not "is any shard in the
/// deployment drained".
fn two_shard_router_shard1_drained() -> ShardRouter {
    ShardRouter::new(
        vec![
            autumn_harvest::types::ShardId::new(0),
            autumn_harvest::types::ShardId::new(1),
        ],
        vec![autumn_harvest::types::ShardId::new(0)],
        autumn_harvest::types::ShardId::new(0),
    )
}

/// Variant of [`build_multi_shard_app`] taking an explicit router, for tests
/// that need a non-default writable-shard configuration (a drained shard).
fn build_multi_shard_app_with_router(
    pool0: &DbPool,
    pool1: &DbPool,
    infos: Vec<WorkflowInfo>,
    router: ShardRouter,
) -> HarvestApiApp {
    let mut pools = std::collections::BTreeMap::new();
    pools.insert(autumn_harvest::types::ShardId::new(0), pool0.clone());
    pools.insert(autumn_harvest::types::ShardId::new(1), pool1.clone());
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(
        autumn_harvest::shard::ShardedDbPool::from_map(
            pools,
            autumn_harvest::types::ShardId::new(0),
        ),
    ));
    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("rerun-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Codex review finding (PR #1152, P1): the DEFAULT (no `workflow_id`
/// override) rerun path constructs the new execution's id unconditionally
/// via `ExecutionId::new_for_shard(source.shard_id)`, with NO check that
/// `source.shard_id` is still in `writable_shards`. Unlike the override
/// path — guarded above by the shard-consistency check, which can only ever
/// resolve to a WRITABLE shard via `ShardRouter::pick_for_new_workflow` —
/// the default path had no protection at all: an operator draining shard 0
/// (removing it from `writable_shards` so it stops accepting new
/// admissions) would still see re-runs of shard-0 work silently land
/// brand-new executions there. A re-run is a fresh admission — exactly what
/// `writable_shards` exists to gate (`docs/sharding.md`: "placing new work
/// on a shard the operator is draining contradicts the drain"). Fixed by
/// rejecting a re-run whose source lives on a non-writable shard.
#[tokio::test]
async fn rerun_is_rejected_when_the_source_shard_has_been_drained() {
    let (admin, _guard) = setup_shard_server().await;
    let url0 = create_shard_db(&admin, &unique("rerun_drain0").replace('-', "_")).await;
    let url1 = create_shard_db(&admin, &unique("rerun_drain1").replace('-', "_")).await;
    let pool0 = build_pool(&url0);
    let pool1 = build_pool(&url1);
    let mut conn0 = AsyncPgConnection::establish(&url0)
        .await
        .expect("connect shard 0");
    let mut conn1 = AsyncPgConnection::establish(&url1)
        .await
        .expect("connect shard 1");

    let wf = "rr_drained_wf";
    let source_wf_id = unique("rr-drained-src");
    let source = seed_terminal_on_shard0(&mut conn0, wf, &source_wf_id).await;

    // Shard 0 (where the source lives) is readable but NOT writable —
    // deliberately drained mid-transition.
    let app = build_multi_shard_app_with_router(
        &pool0,
        &pool1,
        vec![plain_info(wf)],
        two_shard_router_shard0_drained(),
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(rerun_uri(&source))
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::from(json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = read_response(response).await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    let msg = body["detail"].as_str().unwrap_or_default();
    assert!(
        msg.to_lowercase().contains("shard") && msg.to_lowercase().contains("drain"),
        "error should name the drained shard: {msg}"
    );

    // No new execution was created anywhere — neither shard.
    let on_shard0 = load_execs_for_key(&mut conn0, wf, &source_wf_id).await;
    let on_shard1 = load_execs_for_key(&mut conn1, wf, &source_wf_id).await;
    assert_eq!(
        on_shard0.len(),
        1,
        "only the original source row should exist on shard 0: {on_shard0:?}"
    );
    assert!(
        on_shard1.is_empty(),
        "nothing should have landed on shard 1: {on_shard1:?}"
    );

    // The source is untouched (never sealed).
    let source_row = load_exec(&mut conn0, &source).await;
    assert_eq!(source_row.state, "COMPLETED");

    let rows = audit_rows(&mut conn0, "workflow.rerun", &source).await;
    assert!(
        rows.iter().any(|r| r.status == "failed"),
        "expected a failed audit row: {rows:?}"
    );
}

/// Negative control for the fix above: draining a DIFFERENT shard (shard 1)
/// while the source's own shard (0) stays writable must not affect the
/// re-run — the gate must check the SOURCE's specific shard, not "is any
/// shard in the deployment drained".
#[tokio::test]
async fn rerun_still_succeeds_when_a_different_shard_is_drained() {
    let (admin, _guard) = setup_shard_server().await;
    let url0 = create_shard_db(&admin, &unique("rerun_okdrain0").replace('-', "_")).await;
    let url1 = create_shard_db(&admin, &unique("rerun_okdrain1").replace('-', "_")).await;
    let pool0 = build_pool(&url0);
    let pool1 = build_pool(&url1);
    let mut conn0 = AsyncPgConnection::establish(&url0)
        .await
        .expect("connect shard 0");

    let wf = "rr_okdrain_wf";
    let source_wf_id = unique("rr-okdrain-src");
    let source = seed_terminal_on_shard0(&mut conn0, wf, &source_wf_id).await;

    let app = build_multi_shard_app_with_router(
        &pool0,
        &pool1,
        vec![plain_info(wf)],
        two_shard_router_shard1_drained(),
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(rerun_uri(&source))
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::from(json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = read_response(response).await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");

    let rows = load_execs_for_key(&mut conn0, wf, &source_wf_id).await;
    assert_eq!(rows.len(), 2, "the source plus the fresh re-run: {rows:?}");
}

// ── R-12 .. R-41 ─────────────────────────────────────────────────────────────

/// R-12: a re-run of a COMPLETED source creates a brand-new execution with a
/// fresh, single-event history.
#[tokio::test]
async fn rerun_completed_source_creates_new_execution() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_basic_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-basic");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let new_id = body["execution_id"]
        .as_str()
        .expect("execution_id")
        .to_string();
    assert_ne!(new_id, source, "a re-run must create a NEW execution");
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["reran_from"].as_str(), Some(source.as_str()));
    assert_eq!(body["source_prior_state"], json!("COMPLETED"));
    assert_eq!(body["workflow_name"], json!(wf));
    assert_eq!(body["workflow_id"], json!(wf_id));

    let new_row = load_exec(&mut conn, &new_id).await;
    assert_eq!(new_row.state, "RUNNING", "the new run must be live");
    assert_eq!(body["state"], json!("RUNNING"));

    let evs = events_of(&mut conn, &new_id).await;
    assert_eq!(evs.len(), 1, "a fresh run has exactly one event");
    assert_eq!(evs[0].event_id, 0);
    assert_eq!(evs[0].event_type, "WorkflowStarted");

    // The new run must be DISPATCHABLE, not merely present: an execution row
    // with no queued workflow task would sit RUNNING forever and never execute.
    let tasks = tasks_of(&mut conn, &new_id).await;
    assert_eq!(tasks.len(), 1, "exactly one workflow task is enqueued");
    assert_eq!(tasks[0].state, "PENDING", "and it is claimable");
    assert_eq!(tasks[0].task_type, "workflow");
}

/// R-13: the re-run clones the source's recorded start parameters verbatim.
#[tokio::test]
async fn rerun_clones_start_params_verbatim() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_clone_wf";
    // Issue #605 review (R-64): the re-run route now re-validates a cloned
    // `completion_callbacks` against the live SSRF policy, so this app must
    // allowlist the seeded target — `build_app`'s default empty allowlist
    // would otherwise reject it and this test would exercise R-64's
    // rejection path instead of the verbatim-clone path it's named for.
    let app = build_app_allowing_hook_example(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-clone");
    // A schema-complete `CallbackTarget` (with `filter`) — the shape the
    // production start route and delivery-time re-validation both expect;
    // a bare `{"url": ...}` object fails deserialization under the new
    // re-run validation and is not representative of a real stored row.
    let callback_targets = vec![autumn_harvest::completion_callback::CallbackTarget::new(
        "https://hook.example/done",
        autumn_harvest::completion_callback::EventFilter::AnyTerminal,
    )];
    let callback_targets_json = serde_json::to_value(&callback_targets).unwrap();
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            input: Some(json!({"order": 42, "nested": {"k": "v"}})),
            queue_name: Some("billing"),
            memo: Some(json!({"note": "important"})),
            search_attrs: Some(json!({"tenant": "acme"})),
            execution_timeout_secs: Some(1800),
            sla_secs: Some(600),
            owner: Some("payments-team"),
            runbook_url: Some("https://runbook.example/orders"),
            severity: Some("high"),
            completion_callbacks: Some(callback_targets_json.clone()),
            context_headers: Some(json!({"traceparent": "00-abc-def-01"})),
            workflow_retry_policy: Some(retry_policy_json()),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    let row = load_exec(&mut conn, &new_id).await;
    assert_eq!(row.input, json!({"order": 42, "nested": {"k": "v"}}));
    assert_eq!(row.queue_name, "billing");
    assert_eq!(row.memo, Some(json!({"note": "important"})));
    assert_eq!(row.search_attrs, Some(json!({"tenant": "acme"})));
    assert_eq!(row.execution_timeout_secs, Some(1800));
    assert_eq!(row.sla_secs, Some(600));
    assert_eq!(row.owner.as_deref(), Some("payments-team"));
    assert_eq!(
        row.runbook_url.as_deref(),
        Some("https://runbook.example/orders")
    );
    assert_eq!(row.severity.as_deref(), Some("high"));
    assert_eq!(
        row.completion_callbacks,
        Some(callback_targets_json),
        "completion_callbacks must be cloned byte-for-byte"
    );
    // Both stored-JSON start parameters must round-trip byte-for-byte: a parse
    // failure is fail-loud, never a silently dropped field.
    assert_eq!(
        row.context_headers,
        Some(json!({"traceparent": "00-abc-def-01"})),
        "context_headers must be cloned faithfully"
    );
    assert_eq!(
        row.workflow_retry_policy,
        Some(retry_policy_json()),
        "workflow_retry_policy must be cloned faithfully"
    );
}

/// R-14: the new run stamps issue #740 provenance columns.
#[tokio::test]
async fn rerun_stamps_provenance() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_prov_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-prov");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    let row = load_exec(&mut conn, &new_id).await;
    assert_eq!(row.start_source.as_deref(), Some("rerun"));
    assert_eq!(
        row.start_source_ref.as_deref(),
        Some(source.as_str()),
        "start_source_ref must correlate back to the source run"
    );
    assert_eq!(
        row.started_by.as_deref(),
        Some(TEST_ACTOR),
        "re-run is the FIRST writer of started_by"
    );
}

/// R-15: a successful re-run writes exactly one succeeded audit row against
/// the SOURCE execution id.
#[tokio::test]
async fn rerun_writes_audit_row() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_audit_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-audit");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1, "exactly one audit row for the re-run");
    assert_eq!(rows[0].status, "succeeded");
    assert_eq!(rows[0].actor, TEST_ACTOR);
}

/// R-16: an explicit `input` override replaces the clone and never mutates the
/// source row's own input.
#[tokio::test]
async fn rerun_input_override_replaces_clone() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_override_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-override");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            input: Some(json!({"original": true})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) =
        post_json(&app, &rerun_uri(&source), json!({"input": {"replaced": 7}})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    assert_eq!(
        load_exec(&mut conn, &new_id).await.input,
        json!({"replaced": 7})
    );
    assert_eq!(
        load_exec(&mut conn, &source).await.input,
        json!({"original": true}),
        "the source row's input must never be mutated"
    );
}

/// R-17: an explicit JSON `null` input IS a valid override (not "absent").
#[tokio::test]
async fn rerun_input_override_accepts_json_null() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_null_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-null");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            input: Some(json!({"clone_me": true})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({"input": null})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();
    assert_eq!(
        load_exec(&mut conn, &new_id).await.input,
        Value::Null,
        "an explicit null input must override the clone, not be ignored"
    );
}

/// R-18: with no override, the new run reuses the source's business key.
#[tokio::test]
async fn rerun_reuses_source_workflow_id_by_default() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_reuse_id_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-reuse");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();
    assert_eq!(load_exec(&mut conn, &new_id).await.workflow_id, wf_id);
}

/// R-19: a `workflow_id` override starts under the new key and leaves the
/// source untouched in its original terminal state (no sealing needed).
#[tokio::test]
async fn rerun_workflow_id_override_starts_under_new_key() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_wfid_override_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-wfid-src");
    let new_key = unique("rr-wfid-dst");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({"workflow_id": new_key.clone()}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    assert_eq!(load_exec(&mut conn, &new_id).await.workflow_id, new_key);
    assert_eq!(body["workflow_id"], json!(new_key));
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "COMPLETED",
        "an override re-run must NOT seal the source (its key is not reused)"
    );
}

/// R-20: reusing the business key seals the source to CONTINUED_AS_NEW but
/// PRESERVES its original `completed_at` (the C4 repair).
#[tokio::test]
async fn rerun_seals_source_and_preserves_completed_at() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_seal_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-seal");
    let fixed = DateTime::parse_from_rfc3339("2026-02-03T04:05:06Z")
        .unwrap()
        .with_timezone(&Utc);
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            completed_at: Some(fixed),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let src = load_exec(&mut conn, &source).await;
    assert_eq!(src.state, "CONTINUED_AS_NEW", "source must be sealed");
    assert_eq!(
        src.completed_at,
        Some(fixed),
        "sealing must NOT overwrite the source's original completed_at"
    );
    assert_eq!(
        body["source_prior_state"],
        json!("COMPLETED"),
        "the pre-seal state must still be reported"
    );
}

/// R-21: sealing preserves the source's recorded output and error.
#[tokio::test]
async fn rerun_preserves_source_output_and_error() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_preserve_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-preserve");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            output: Some(json!({"partial": 3})),
            error: Some("boom".to_string()),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let src = load_exec(&mut conn, &source).await;
    assert_eq!(src.state, "CONTINUED_AS_NEW");
    assert_eq!(src.output, Some(json!({"partial": 3})));
    assert_eq!(src.error.as_deref(), Some("boom"));
    assert_eq!(body["source_prior_state"], json!("FAILED"));
}

/// R-22: a TERMINATED source is already released from the active-uniqueness
/// index, so the re-run creates a new run WITHOUT sealing anything.
#[tokio::test]
async fn rerun_of_terminated_source_does_not_seal_anything() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_terminated_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-terminated");
    let source = seed_terminal(&mut conn, wf, &wf_id, "TERMINATED").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "TERMINATED",
        "a sealed TERMINATED source stays TERMINATED"
    );
    assert_eq!(load_exec(&mut conn, &new_id).await.state, "RUNNING");
    assert_eq!(body["source_prior_state"], json!("TERMINATED"));
}

/// R-23: all five re-runnable terminal states are accepted.
#[tokio::test]
async fn rerun_accepts_all_five_terminal_states() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_all_states_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    for state in [
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMED_OUT",
        "TERMINATED",
    ] {
        let wf_id = unique(&format!("rr-state-{}", state.to_lowercase()));
        let source = seed_terminal(&mut conn, wf, &wf_id, state).await;
        let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
        assert_eq!(status, StatusCode::CREATED, "state {state}: {body}");
        assert_eq!(body["source_prior_state"], json!(state));
    }
}

/// R-24: a RUNNING source is rejected (409) — re-run is for finished work.
#[tokio::test]
async fn rerun_rejects_running_source() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_running_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-running");
    let source = seed_terminal(&mut conn, wf, &wf_id, "RUNNING").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "no new run"
    );
}

/// R-25: a PAUSED source is rejected (409).
#[tokio::test]
async fn rerun_rejects_paused_source() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_paused_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-paused");
    let source = seed_terminal(&mut conn, wf, &wf_id, "PAUSED").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "no new run"
    );
}

/// R-26: a CONTINUED_AS_NEW source is rejected (409) with a message pointing
/// at the chain's latest run.
#[tokio::test]
async fn rerun_rejects_continued_as_new_source() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_can_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-can");
    let source = seed_terminal(&mut conn, wf, &wf_id, "CONTINUED_AS_NEW").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let msg = body.to_string().to_lowercase();
    assert!(
        msg.contains("continued") && (msg.contains("chain") || msg.contains("latest")),
        "the 409 must point at the chain's latest run: {body}"
    );
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "no new run"
    );
}

/// R-27: a re-run is NOT idempotent — the second identical call is rejected
/// because the first sealed the source out of a re-runnable state.
#[tokio::test]
async fn rerun_twice_is_rejected_the_second_time() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_twice_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-twice");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let (s1, b1) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(s1, StatusCode::CREATED, "{b1}");

    let (s2, b2) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(s2, StatusCode::CONFLICT, "a re-run is not idempotent: {b2}");
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        2,
        "the rejected second call must create nothing"
    );
}

/// R-28: when the business key is held by a DIFFERENT live execution, the
/// re-run is rejected (409) naming the occupant.
#[tokio::test]
async fn rerun_rejects_when_business_key_held_by_another_run() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_held_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-held");
    // Source is sealed TERMINATED, so it is out of the active-uniqueness index...
    let source = seed_terminal(&mut conn, wf, &wf_id, "TERMINATED").await;
    // ...but a DIFFERENT execution now holds the same business key, live.
    let occupant = seed_terminal(&mut conn, wf, &wf_id, "RUNNING").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body.to_string().contains(&occupant),
        "the 409 must name the occupying execution: {body}"
    );
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        2,
        "no third run may be created"
    );

    // The occupant-409 audit row rides the CALLER'S connection (issue #777
    // review); before that fix a second pool checkout could silently lose it.
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1, "the occupant rejection must be audited");
    assert_eq!(rows[0].status, "failed");
    assert_eq!(rows[0].actor, TEST_ACTOR);
}

/// R-28b: a `workflow_id` OVERRIDE onto a key already held by a live run is
/// rejected via the distinct `AlreadyExists` path (a different response body
/// from the same-key R-28 conflict), and starts nothing.
#[tokio::test]
async fn rerun_workflow_id_override_onto_occupied_key_is_rejected() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_override_occupied_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-ovr-src");
    let taken = unique("rr-ovr-taken");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;
    let occupant = seed_terminal(&mut conn, wf, &taken, "RUNNING").await;

    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({"workflow_id": taken.clone()}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["existing_execution_id"].as_str(),
        Some(occupant.as_str()),
        "the AlreadyExists body must name the occupying run: {body}"
    );
    // Nothing started under either key, and the source is NOT sealed (an
    // override never seals).
    assert_eq!(load_execs_for_key(&mut conn, wf, &taken).await.len(), 1);
    assert_eq!(load_execs_for_key(&mut conn, wf, &wf_id).await.len(), 1);
    assert_eq!(load_exec(&mut conn, &source).await.state, "COMPLETED");
}

/// R-29: an erased source input (issue #495) is rejected without an override.
#[tokio::test]
async fn rerun_rejects_erased_source_input() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_erased_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-erased");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            input: Some(json!({"_harvest_erased": true})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body.to_string().to_lowercase().contains("eras"),
        "the 409 must explain the erasure: {body}"
    );
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "no new run"
    );
}

/// R-30: an erased source IS re-runnable when the operator supplies an input.
#[tokio::test]
async fn rerun_erased_source_allowed_with_explicit_input() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_erased_ok_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-erased-ok");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            input: Some(json!({"_harvest_erased": true})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({"input": {"restored": "by operator"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();
    assert_eq!(
        load_exec(&mut conn, &new_id).await.input,
        json!({"restored": "by operator"})
    );
}

/// R-31: a raised admission gate blocks the re-run (503) — nothing is started
/// and the source is NOT sealed.
#[tokio::test]
async fn rerun_respects_admission_gate() {
    use autumn_harvest::admission_gate::set_global_admission_gate_cache;
    use autumn_harvest::{AdmissionGate, AdmissionGateId, GateScope};

    // Serialize against any other test in this binary that mutates the
    // process-global gate cache.
    let _serial = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    // RAII teardown: clear the process-global cache on scope exit, incl. panic.
    struct GateGuard;
    impl Drop for GateGuard {
        fn drop(&mut self) {
            autumn_harvest::admission_gate::set_global_admission_gate_cache(None);
        }
    }
    let _gate_guard = GateGuard;

    // A workflow name no sibling test starts: the gate scope is WorkflowName,
    // so cross-talk is structurally impossible.
    let wf = "rr_gated_wf";

    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let api_state = api_state_with(vec![plain_info(wf)], &pool, true);
    let gate_id = uuid::Uuid::new_v4();
    api_state.initialize_gate_cache(vec![AdmissionGate {
        id: AdmissionGateId(gate_id),
        scope: GateScope::WorkflowName(wf.to_string()),
        reason: "incident".to_string(),
        message: None,
        created_by: "op".to_string(),
        created_at: Utc::now(),
        expires_at: None,
    }]);
    set_global_admission_gate_cache(Some(api_state.gate_cache()));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    let mut conn = pool.get().await.unwrap();
    let wf_id = unique("rr-gated");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"], json!("admission blocked"));
    assert_eq!(
        body["gate_id"].as_str(),
        Some(gate_id.to_string().as_str()),
        "the body names the gate that actually blocked it: {body}"
    );
    assert_eq!(body["reason"], json!("incident"));

    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "a gated re-run must start nothing"
    );
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "COMPLETED",
        "a gated re-run must not seal the source"
    );

    // The failed audit row rides the CALLER'S connection (issue #777 review):
    // before that fix it was written through a second pool checkout and could
    // silently vanish under connection pressure.
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1, "a gated re-run must still be audited");
    assert_eq!(rows[0].status, "failed");
    assert_eq!(rows[0].actor, TEST_ACTOR);
    assert!(
        rows[0]
            .error_summary
            .as_deref()
            .is_some_and(|s| s.contains("admission blocked")),
        "the audit row records why: {:?}",
        rows[0].error_summary
    );
}

/// R-32: the route is admin-only.
#[tokio::test]
async fn rerun_requires_admin() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_admin_wf";
    let app = build_app_no_admin(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-admin");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let status = post_json_unauth(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "an unauthenticated re-run must not mutate the database"
    );
    assert_eq!(load_exec(&mut conn, &source).await.state, "COMPLETED");
}

/// R-33: an unknown execution id returns 404 FROM THE HANDLER.
///
/// A bare `assert_eq!(status, NOT_FOUND)` would pass vacuously: axum answers an
/// unmatched route with 404 too, so a typo in `rerun_uri` or a dropped route
/// registration would keep this test green while the handler never ran. The
/// body message and the failed audit row are what prove it reached the handler.
#[tokio::test]
async fn rerun_unknown_execution_returns_404() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("rr_unknown_wf")]);
    let mut conn = pool.get().await.unwrap();

    let missing = autumn_harvest::types::ExecutionId::new().to_string();
    let (status, body) = post_json(&app, &rerun_uri(&missing), json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(
        body.to_string().contains(&missing),
        "the handler's own not-found message names the execution, so this is not \
         axum's unmatched-route 404: {body}"
    );

    let rows = audit_rows(&mut conn, "workflow.rerun", &missing).await;
    assert_eq!(
        rows.len(),
        1,
        "the handler ran and audited the rejection (an unmatched route would not)"
    );
    assert_eq!(rows[0].status, "failed");
}

/// R-34: a malformed execution id returns 400 and writes a failed audit row.
#[tokio::test]
async fn rerun_malformed_id_returns_400_and_audits_failure() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("rr_malformed_wf")]);
    let mut conn = pool.get().await.unwrap();

    // Unique per run: the audit target_id is the raw id string, and the
    // shared persistent DB would otherwise accumulate rows across runs.
    let bad = format!("not-a-uuid-{}", uuid::Uuid::new_v4());
    let (status, body) = post_json(&app, &rerun_uri(&bad), json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let rows = audit_rows(&mut conn, "workflow.rerun", &bad).await;
    assert_eq!(rows.len(), 1, "a malformed id must still be audited");
    assert_eq!(rows[0].status, "failed");
}

/// R-35: a debounced workflow cannot be re-run (its start is deferred, so no
/// execution id could be returned).
#[tokio::test]
async fn rerun_rejects_debounced_workflow() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_debounced_wf";
    let mut info = plain_info(wf);
    info.debounce = Some(autumn_harvest::debounce::DebouncePolicy {
        key_expr: "tenant",
        window: std::time::Duration::from_secs(30),
        max_wait: None,
    });
    let app = build_app(&pool, vec![info]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-debounced");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            input: Some(json!({"tenant": "acme"})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "no new run"
    );
}

/// R-36: a throttled workflow cannot be re-run (same deferred-start reason).
#[tokio::test]
async fn rerun_rejects_throttled_workflow() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_throttled_wf";
    let mut info = plain_info(wf);
    info.throttle = Some(autumn_harvest::throttle::ThrottlePolicy {
        refill_per_sec: 1.0,
        burst: 1.0,
        key_expr: None,
        schedule_to_start: None,
    });
    let app = build_app(&pool, vec![info]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-throttled");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "no new run"
    );
}

/// R-37: the six replay-non-determinism diagnostic keys (issue #603) are
/// stripped from the cloned search attributes; user keys survive.
#[tokio::test]
async fn rerun_strips_nd_diagnostic_search_attrs() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_nd_attrs_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-nd-attrs");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            search_attrs: Some(json!({
                "failure_cause": "non_determinism",
                "expected": "ActivityScheduled(a)",
                "actual": "TimerStarted(t)",
                "event_index": 3,
                "workflow_type": wf,
                "build_id": "v1",
                "tenant": "acme",
            })),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    let attrs = load_exec(&mut conn, &new_id)
        .await
        .search_attrs
        .expect("cloned search attrs");
    assert_eq!(
        attrs,
        json!({"tenant": "acme"}),
        "the fresh run must not display a phantom ND diagnostic"
    );
}

/// R-37b (Codex review, issue #777): `search_attrs` is an unvalidated JSON
/// value at the model layer -- the start API and core accept a scalar or
/// array, not only an object. The ND-diagnostic strip above must not
/// silently discard such a value into `{}`: none of the six diagnostic keys
/// could ever exist inside a non-object value, so stripping them is a no-op
/// by definition, and the "clone verbatim" contract demands it survive.
#[tokio::test]
async fn rerun_preserves_non_object_search_attrs_verbatim() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_scalar_attrs_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    // A scalar (string) source.
    let wf_id_str = unique("rr-scalar-attrs");
    let source_str = seed_execution(
        &mut conn,
        wf,
        &wf_id_str,
        "COMPLETED",
        Seed {
            search_attrs: Some(json!("legacy-caller-sent-a-bare-string")),
            ..Seed::default()
        },
    )
    .await;
    let (status, body) = post_json(&app, &rerun_uri(&source_str), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id_str = body["execution_id"].as_str().unwrap().to_string();
    let attrs_str = load_exec(&mut conn, &new_id_str)
        .await
        .search_attrs
        .expect("cloned search attrs");
    assert_eq!(
        attrs_str,
        json!("legacy-caller-sent-a-bare-string"),
        "a non-object string search_attrs must survive the rerun verbatim, not collapse to {{}}"
    );

    // An array source.
    let wf_id_arr = unique("rr-array-attrs");
    let source_arr = seed_execution(
        &mut conn,
        wf,
        &wf_id_arr,
        "COMPLETED",
        Seed {
            search_attrs: Some(json!(["tag-a", "tag-b"])),
            ..Seed::default()
        },
    )
    .await;
    let (status, body) = post_json(&app, &rerun_uri(&source_arr), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id_arr = body["execution_id"].as_str().unwrap().to_string();
    let attrs_arr = load_exec(&mut conn, &new_id_arr)
        .await
        .search_attrs
        .expect("cloned search attrs");
    assert_eq!(
        attrs_arr,
        json!(["tag-a", "tag-b"]),
        "a non-object array search_attrs must survive the rerun verbatim, not collapse to {{}}"
    );
}

/// R-38: schedule provenance (issue #488/#534) is NOT carried onto the re-run,
/// matching the reset-fork precedent.
///
/// Uses the `workflow_id`-OVERRIDE path: a default-key re-run of a
/// schedule-attributed source is rejected outright (see
/// `rerun_of_schedule_attributed_source_is_rejected`), because sealing it would
/// break the schedule's carryover lineage. The override path is the supported
/// way to re-run scheduled work, and it is where this assertion belongs.
#[tokio::test]
async fn rerun_does_not_carry_schedule_provenance() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_sched_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-sched");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            schedule_id: Some(uuid::Uuid::new_v4()),
            scheduled_for: Some(Utc::now()),
            origin: Some("scheduled"),
            ..Seed::default()
        },
    )
    .await;

    let fresh_key = unique("rr-sched-fresh");
    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({ "workflow_id": fresh_key }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    let row = load_exec(&mut conn, &new_id).await;
    assert_eq!(row.schedule_id, None, "schedule_id must not carry over");
    assert_eq!(row.scheduled_for, None, "scheduled_for must not carry over");
    assert_eq!(row.origin, None, "origin must not carry over");
}

/// R-39: the response never echoes the (possibly sensitive) input payload.
#[tokio::test]
async fn rerun_response_never_echoes_input() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_no_echo_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-no-echo");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            input: Some(json!({"ssn": "123-45-6789"})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert!(
        body.get("input").is_none(),
        "the re-run response must never carry an input field: {body}"
    );
    assert!(
        !body.to_string().contains("123-45-6789"),
        "the response must not leak the input payload: {body}"
    );
}

/// R-40: PINS the documented R1 consequence — after a re-run seals the source,
/// `GET /workflows/{source}/result` observes it as CONTINUED_AS_NEW and follows
/// the chain (issue #527's chain-following) rather than reporting the original
/// terminal outcome.
#[tokio::test]
async fn rerun_source_result_route_reports_continued_as_new() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_result_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-result");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            error: Some("original failure".to_string()),
            ..Seed::default()
        },
    )
    .await;

    // Before the re-run, /result reports the original FAILED outcome.
    let (s_before, b_before) = get_json(&app, &format!("/workflows/{source}/result")).await;
    assert_eq!(s_before, StatusCode::OK, "{b_before}");
    assert_eq!(b_before["state"], json!("failed"));

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // AFTER the re-run the source is sealed CONTINUED_AS_NEW. This test pins
    // whatever the /result route actually does with that (the deliberate R1
    // consequence of reusing the business key) so a future change is a
    // conscious decision, not a silent regression.
    let (s_after, b_after) = get_json(&app, &format!("/workflows/{source}/result")).await;
    // Observed exactly: `200 {"completed_at": "...", "state": "continued_as_new"}`
    // — `error` and `output` are omitted entirely (skip_serializing_if), which
    // indexes as JSON null below.
    assert_eq!(s_after, StatusCode::OK, "{b_after}");
    assert_eq!(
        b_after["state"],
        json!("continued_as_new"),
        "the sealed source now reports as a chain predecessor: {b_after}"
    );
    // The KNOWN CONSEQUENCE, pinned exactly: the original failure reason is
    // dropped from THIS surface. It survives in `harvest_events` (the terminal
    // event) and on the describe surface (`execution.error`), and the pre-seal
    // state is returned by the re-run response and persisted in the succeeded
    // `workflow.rerun` audit row — so nothing is lost overall, but a caller
    // polling /result for the SOURCE id can no longer see why it failed.
    assert_eq!(
        b_after["error"],
        Value::Null,
        "the original failure reason is dropped from /result: {b_after}"
    );

    // ...and it IS still recoverable from the describe surface.
    let (s_desc, b_desc) = get_json(&app, &format!("/workflows/{source}")).await;
    assert_eq!(s_desc, StatusCode::OK, "{b_desc}");
    assert_eq!(
        b_desc["execution"]["error"],
        json!("original failure"),
        "describe still carries the original error: {b_desc}"
    );
}

/// R-41: the re-run is a fresh chain origin and a fresh retry chain.
#[tokio::test]
async fn rerun_sets_no_continue_as_new_backlinks() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_backlinks_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-backlinks");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    let row = load_exec(&mut conn, &new_id).await;
    assert_eq!(
        row.continued_from_exec_id, None,
        "a re-run is NOT a continue-as-new successor"
    );
    assert_eq!(row.first_exec_id, None, "a re-run begins a fresh chain");
    assert_eq!(
        row.workflow_attempt, 1,
        "a re-run begins a fresh retry chain"
    );
    assert_eq!(event_count(&mut conn, &new_id).await, 1);
}

/// R-42 (issue #373): an explicitly supplied `input` OVERRIDE is validated
/// against the workflow's published input schema, and a violation is rejected
/// at the edge with the standard `{error, violations}` body — nothing is
/// started and the source is not sealed.
#[tokio::test]
async fn rerun_input_override_violating_schema_is_rejected() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_schema_override_wf";
    let app = build_app(&pool, vec![schema_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-schema-override");
    // The SOURCE input satisfies the schema; only the override violates it.
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            input: Some(json!({"order": 42})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({"input": {"order": "not-an-integer"}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], json!("input validation failed"));
    let violations = body["violations"].as_array().expect("violations array");
    assert!(!violations.is_empty(), "at least one violation: {body}");
    assert!(
        violations[0].get("message").is_some() && violations[0].get("field_path").is_some(),
        "issue #373 violation shape {{message, field_path}}: {body}"
    );

    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "a schema-rejected re-run must start nothing"
    );
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "FAILED",
        "a schema-rejected re-run must not seal the source"
    );

    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1, "the rejection must be audited");
    assert_eq!(rows[0].status, "failed");
    assert_eq!(rows[0].actor, TEST_ACTOR);
}

/// R-43 (issue #373): the VERBATIM CLONE is deliberately NOT validated. A run
/// started under an older or looser schema must stay re-runnable even after the
/// schema is tightened — otherwise tightening a schema would silently strand
/// every historical run of that workflow type.
#[tokio::test]
async fn rerun_does_not_validate_the_cloned_input() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_schema_clone_wf";
    let app = build_app(&pool, vec![schema_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-schema-clone");
    // Violates the CURRENT schema (no `order`) — as if it were started before
    // the schema was published/tightened.
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            input: Some(json!({"legacy": true})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a clone is never re-validated: {body}"
    );
    let new_id = body["execution_id"].as_str().unwrap().to_string();
    assert_eq!(
        load_exec(&mut conn, &new_id).await.input,
        json!({"legacy": true}),
        "the schema-violating input is cloned byte-for-byte"
    );
}

/// R-44 (issue #373): an override that SATISFIES the schema is accepted, so the
/// validation gate cannot be passing vacuously.
#[tokio::test]
async fn rerun_input_override_satisfying_schema_is_accepted() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_schema_ok_wf";
    let app = build_app(&pool, vec![schema_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-schema-ok");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            input: Some(json!({"order": 1})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({"input": {"order": 7}})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();
    assert_eq!(
        load_exec(&mut conn, &new_id).await.input,
        json!({"order": 7})
    );
}

/// R-45 (issue #488 / #534): a default-key re-run of a SCHEDULE-ATTRIBUTED
/// source is rejected. Sealing it to CONTINUED_AS_NEW would put the slot in a
/// state `resolve_carryover` recognises in neither of its sets, rolling the
/// next fire's incremental cursor backward and deflating the schedule's
/// success ratio. The source row must be left byte-untouched.
#[tokio::test]
async fn rerun_of_schedule_attributed_source_is_rejected() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_sched_seal_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-sched-seal");
    let schedule_id = uuid::Uuid::new_v4();
    let slot = Utc::now();
    let completed = Utc::now();
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            schedule_id: Some(schedule_id),
            scheduled_for: Some(slot),
            origin: Some("scheduled"),
            completed_at: Some(completed),
            ..Seed::default()
        },
    )
    .await;
    let before = load_exec(&mut conn, &source).await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body.to_string().contains("schedule-attributed"),
        "the 409 explains why and points at the override: {body}"
    );

    // The source is byte-untouched: still COMPLETED, same completed_at, still
    // attributed to its schedule slot.
    let after = load_exec(&mut conn, &source).await;
    assert_eq!(after.state, "COMPLETED");
    assert_eq!(after.completed_at, before.completed_at);
    assert_eq!(after.schedule_id, Some(schedule_id));
    assert_eq!(after.scheduled_for, before.scheduled_for);
    assert_eq!(after.origin.as_deref(), Some("scheduled"));

    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "nothing may be started"
    );
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1, "the rejection must be audited");
    assert_eq!(rows[0].status, "failed");
}

/// R-46: the supported way to re-run scheduled work — an explicit `workflow_id`
/// override never seals, so the schedule's lineage is untouched and the new run
/// is deliberately NOT schedule-attributed.
#[tokio::test]
async fn rerun_of_schedule_attributed_source_with_override_is_allowed() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_sched_override_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-sched-ovr");
    let schedule_id = uuid::Uuid::new_v4();
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            schedule_id: Some(schedule_id),
            scheduled_for: Some(Utc::now()),
            origin: Some("scheduled"),
            ..Seed::default()
        },
    )
    .await;
    let before = load_exec(&mut conn, &source).await;

    let fresh_key = unique("rr-sched-ovr-new");
    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({ "workflow_id": fresh_key }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    // Source untouched — no seal, lineage intact.
    let after = load_exec(&mut conn, &source).await;
    assert_eq!(after.state, "FAILED");
    assert_eq!(after.completed_at, before.completed_at);
    assert_eq!(after.schedule_id, Some(schedule_id));

    let row = load_exec(&mut conn, &new_id).await;
    assert_eq!(row.workflow_id, fresh_key);
    assert_eq!(
        row.schedule_id, None,
        "an operator re-run is excluded from scheduled carryover"
    );
    assert_eq!(row.start_source.as_deref(), Some("rerun"));
    assert_eq!(row.start_source_ref.as_deref(), Some(source.as_str()));
}

/// R-47 (issue #495): a re-run-with-override of an ERASED source must not clone
/// the erasure tombstones out of `memo` / `search_attrs` onto the fresh,
/// never-erased run — that would pollute `?search_attr=` filtering and mislead
/// compliance tooling into believing the new run had been erased.
#[tokio::test]
async fn rerun_of_erased_source_does_not_propagate_tombstones() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_erased_tombstone_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-erased-tomb");
    let tombstone = json!({ "_harvest_erased": true });
    // Shaped exactly like an `erase_workflow_payloads` row scrub: input, memo
    // and search_attrs tombstoned; context_headers NULLed.
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            input: Some(tombstone.clone()),
            memo: Some(tombstone.clone()),
            search_attrs: Some(tombstone.clone()),
            context_headers: None,
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({"input": {"reconstructed": true}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    let row = load_exec(&mut conn, &new_id).await;
    assert_eq!(row.input, json!({"reconstructed": true}));
    assert_eq!(row.memo, None, "an erased memo must not be cloned");
    assert_eq!(
        row.search_attrs, None,
        "erased search attributes must not be cloned"
    );
    assert_eq!(row.context_headers, None);
    assert!(
        !serde_json::to_string(&row.memo)
            .unwrap()
            .contains("_harvest_erased")
            && !serde_json::to_string(&row.search_attrs)
                .unwrap()
                .contains("_harvest_erased"),
        "no tombstone key may survive onto the new run"
    );
}

/// R-48 (issue #617): the chain-scoped lifetime cap is CLONED, but its absolute
/// deadline is RE-ANCHORED — a re-run is a fresh chain origin, so it must not
/// inherit the source's already-elapsed `chain_deadline_at`.
#[tokio::test]
async fn rerun_clones_chain_timeout_and_reanchors_its_deadline() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_chain_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-chain");
    // A deadline far in the PAST: if it were inherited, the new run would be
    // born already over its chain cap.
    let stale_deadline = Utc::now() - chrono::Duration::hours(24);
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            chain_execution_timeout_secs: Some(7200),
            chain_deadline_at: Some(stale_deadline),
            ..Seed::default()
        },
    )
    .await;

    let before = Utc::now();
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();
    let after = Utc::now();

    let row = load_exec(&mut conn, &new_id).await;
    assert_eq!(
        row.chain_execution_timeout_secs,
        Some(7200),
        "the declared chain cap is cloned"
    );
    let deadline = row
        .chain_deadline_at
        .expect("a cloned chain cap anchors a fresh deadline");
    assert!(
        deadline > stale_deadline,
        "the stale deadline must NOT be inherited: {deadline} vs {stale_deadline}"
    );
    // Fresh anchor = (start instant) + 7200s, bracketed by the request window.
    assert!(
        deadline >= before + chrono::Duration::seconds(7200)
            && deadline <= after + chrono::Duration::seconds(7200),
        "the deadline is re-anchored at now + chain cap: {deadline} not in \
         [{before} + 2h, {after} + 2h]"
    );
}

/// R-49: a body-less POST (zero bytes, no `Content-Type` JSON payload) is
/// accepted and clones the source input verbatim — the whole reason the handler
/// takes raw `Bytes` rather than `Option<Json<…>>`.
#[tokio::test]
async fn rerun_with_empty_body_clones_input() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_empty_body_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-empty-body");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            input: Some(json!({"kept": "verbatim"})),
            ..Seed::default()
        },
    )
    .await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(rerun_uri(&source))
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("POST request");
    let (status, body) = read_response(response).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let new_id = body["execution_id"].as_str().unwrap().to_string();
    assert_eq!(
        load_exec(&mut conn, &new_id).await.input,
        json!({"kept": "verbatim"})
    );
}

/// R-50 (issue #247): the per-key concurrency group key is resolved from the
/// EFFECTIVE input — the override when one is supplied, the clone otherwise.
#[tokio::test]
async fn rerun_resolves_concurrency_key_from_effective_input() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_conc_wf";
    let app = build_app(&pool, vec![concurrency_info(wf, "input.tenant_id", 4)]);
    let mut conn = pool.get().await.unwrap();

    // (a) An override re-keys the new run onto the OVERRIDE's tenant.
    let wf_id = unique("rr-conc-override");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            input: Some(json!({"tenant_id": "acme"})),
            ..Seed::default()
        },
    )
    .await;
    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({"input": {"tenant_id": "zeta"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();
    let tasks = tasks_of(&mut conn, &new_id).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0].concurrency_key.as_deref(),
        Some("zeta"),
        "the key resolves from the OVERRIDE, not the source input"
    );

    // (b) With no override it resolves from the cloned source input.
    let wf_id2 = unique("rr-conc-clone");
    let source2 = seed_execution(
        &mut conn,
        wf,
        &wf_id2,
        "FAILED",
        Seed {
            input: Some(json!({"tenant_id": "acme"})),
            ..Seed::default()
        },
    )
    .await;
    let (status2, body2) = post_json(&app, &rerun_uri(&source2), json!({})).await;
    assert_eq!(status2, StatusCode::CREATED, "{body2}");
    let new_id2 = body2["execution_id"].as_str().unwrap().to_string();
    let tasks2 = tasks_of(&mut conn, &new_id2).await;
    assert_eq!(tasks2.len(), 1);
    assert_eq!(tasks2[0].concurrency_key.as_deref(), Some("acme"));
}

/// R-51: a source whose workflow type is no longer registered on this node is
/// rejected 404 — the fresh run would otherwise wedge immediately in
/// HandlerNotFound replay failure.
#[tokio::test]
async fn rerun_unregistered_workflow_type_returns_404() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    // The app registers a DIFFERENT workflow than the source's type.
    let app = build_app(&pool, vec![plain_info("rr_registered_wf")]);
    let mut conn = pool.get().await.unwrap();

    let wf = "rr_retired_wf";
    let wf_id = unique("rr-retired");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(
        body.to_string().contains("not registered"),
        "the 404 explains the type is retired: {body}"
    );

    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "nothing may be started"
    );
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1, "the rejection must be audited");
    assert_eq!(rows[0].status, "failed");
}

/// R-52: a batched workflow (issue #518) cannot be re-run — its start is
/// deferred, so no execution id could be returned.
#[tokio::test]
async fn rerun_rejects_batched_workflow() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_batched_wf";
    let mut info = plain_info(wf);
    info.batch = Some(autumn_harvest::event_batch::BatchPolicy {
        key_expr: "input.tenant_id".to_string(),
        max_size: 10,
        max_wait: std::time::Duration::from_secs(30),
    });
    let app = build_app(&pool, vec![info]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-batched");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            input: Some(json!({"tenant_id": "acme"})),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.to_string().contains("batch"),
        "the 400 names the offending policy: {body}"
    );
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "nothing may be started"
    );
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "failed");
}

/// R-53: a registered unified DAG is rejected 400 — DAGs have their own
/// trigger/retry routes.
#[tokio::test]
async fn rerun_rejects_registered_dag() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let dag = "rr_dag_wf";
    let app = build_app_with(
        &pool,
        vec![plain_info(dag)],
        &AppOpts {
            dag_names: vec![dag],
            ..AppOpts::default()
        },
    );
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-dag");
    let source = seed_terminal(&mut conn, dag, &wf_id, "FAILED").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.to_string().contains("DAG"),
        "the 400 points at the DAG routes: {body}"
    );
    assert_eq!(
        load_execs_for_key(&mut conn, dag, &wf_id).await.len(),
        1,
        "nothing may be started"
    );
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "failed");
}

/// R-54: a malformed JSON body returns 400 and writes a failed audit row.
#[tokio::test]
async fn rerun_malformed_body_returns_400_and_audits_failure() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_bad_body_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-bad-body");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(rerun_uri(&source))
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::from("{\"input\": }"))
                .unwrap(),
        )
        .await
        .expect("POST request");
    let (status, body) = read_response(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "a malformed body must start nothing"
    );
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1, "a malformed body must still be audited");
    assert_eq!(rows[0].status, "failed");
}

/// R-55: the describe surface carries the issue #740 provenance of the new run,
/// so an operator can see WHERE it came from without reading the audit log.
#[tokio::test]
async fn rerun_describe_surface_shows_provenance() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_describe_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-describe");
    let source = seed_terminal(&mut conn, wf, &wf_id, "FAILED").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    let (s, b) = get_json(&app, &format!("/workflows/{new_id}")).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["execution"]["start_source"], json!("rerun"));
    assert_eq!(b["execution"]["start_source_ref"], json!(source));
    assert_eq!(b["execution"]["started_by"], json!(TEST_ACTOR));
}

/// R-56: re-running a re-run works, and provenance is ONE HOP — the second
/// run's `start_source_ref` points at the run it was re-run from, never
/// transitively back at the original source.
#[tokio::test]
async fn rerun_of_a_rerun_records_one_hop_provenance() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_rerun_of_rerun_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-rerun2");
    let first = seed_terminal(&mut conn, wf, &wf_id, "FAILED").await;

    let (s1, b1) = post_json(&app, &rerun_uri(&first), json!({})).await;
    assert_eq!(s1, StatusCode::CREATED, "{b1}");
    let second = b1["execution_id"].as_str().unwrap().to_string();

    // Seal the second run terminally so it becomes re-runnable in turn.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions
         SET state = 'FAILED', completed_at = NOW(), error = 'still broken'
         WHERE id = $1::uuid",
    )
    .bind::<Text, _>(second.as_str())
    .execute(&mut conn)
    .await
    .expect("seal the second run");

    let (s2, b2) = post_json(&app, &rerun_uri(&second), json!({})).await;
    assert_eq!(s2, StatusCode::CREATED, "{b2}");
    let third = b2["execution_id"].as_str().unwrap().to_string();
    assert_eq!(b2["source_prior_state"], json!("FAILED"));

    let row = load_exec(&mut conn, &third).await;
    assert_eq!(row.start_source.as_deref(), Some("rerun"));
    assert_eq!(
        row.start_source_ref.as_deref(),
        Some(second.as_str()),
        "provenance is one hop: the SECOND run, not the original source"
    );
    assert_ne!(row.start_source_ref.as_deref(), Some(first.as_str()));
}

/// R-57 (issue #252): an oversized `input` override is rejected 413 rather than
/// being persisted. Uses a deliberately tiny cap so the test stays fast.
#[tokio::test]
async fn rerun_oversized_input_override_returns_413() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_oversized_wf";
    let app = build_app_with(
        &pool,
        vec![plain_info(wf)],
        &AppOpts {
            max_workflow_input_bytes: Some(64),
            ..AppOpts::default()
        },
    );
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-oversized");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let big = "x".repeat(4096);
    let (status, body) =
        post_json(&app, &rerun_uri(&source), json!({"input": {"blob": big}})).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");

    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "an oversized re-run must start nothing"
    );
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "COMPLETED",
        "and must not seal the source"
    );
}

/// R-58: a `workflow_id` override colliding with a COMPLETED (terminal but
/// still key-holding) run is rejected 409 — RejectDuplicate surfaces
/// AlreadyExists, and the occupant is never disturbed.
#[tokio::test]
async fn rerun_override_onto_completed_occupant_is_rejected() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_completed_occupant_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let source_key = unique("rr-occ-source");
    let source = seed_terminal(&mut conn, wf, &source_key, "FAILED").await;

    // A COMPLETED run still sits inside the partial active-uniqueness index.
    let occupied_key = unique("rr-occ-target");
    let occupant = seed_terminal(&mut conn, wf, &occupied_key, "COMPLETED").await;

    let (status, body) = post_json(
        &app,
        &rerun_uri(&source),
        json!({ "workflow_id": occupied_key }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["existing_execution_id"].as_str(),
        Some(occupant.as_str()),
        "the 409 names the occupant: {body}"
    );
    assert_eq!(body["existing_state"], json!("COMPLETED"));

    assert_eq!(
        load_execs_for_key(&mut conn, wf, &occupied_key).await.len(),
        1,
        "the occupied key gains no run"
    );
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "FAILED",
        "the source must not be sealed"
    );
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "failed");
}

/// R-59: the succeeded audit row persists the source's pre-seal state, so the
/// operator trail is self-contained once the row itself reads CONTINUED_AS_NEW.
#[tokio::test]
async fn rerun_success_audit_records_source_prior_state() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_audit_state_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-audit-state");
    let source = seed_terminal(&mut conn, wf, &wf_id, "TIMED_OUT").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["source_prior_state"], json!("TIMED_OUT"));

    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "succeeded");
    assert_eq!(
        rows[0].error_summary.as_deref(),
        Some("reran from state TIMED_OUT"),
        "the pre-seal state survives in the audit trail"
    );
    // ...and the row itself now reads CONTINUED_AS_NEW, which is exactly why
    // the audit row has to carry it.
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "CONTINUED_AS_NEW"
    );
}

/// R-60 (issue #701): a re-run does NOT join the source's continue-as-new run
/// chain. Provenance is `start_source`/`start_source_ref`, not CAN back-links —
/// co-opting the CAN linkage would misrepresent an operator restart as a
/// continue-as-new. This PINS what `/run-chain` reports for both ids so the
/// separation is a conscious contract, and asserts the endpoint never 500s.
#[tokio::test]
async fn rerun_run_chain_endpoint_reports_separate_chains() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_chainview_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-chainview");
    let source = seed_terminal(&mut conn, wf, &wf_id, "FAILED").await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let new_id = body["execution_id"].as_str().unwrap().to_string();

    // The NEW run: a standalone chain of one. It carries no back-link columns,
    // so #701's legacy forward-walk finds no predecessor.
    let (s_new, b_new) = get_json(&app, &format!("/workflows/{new_id}/run-chain")).await;
    assert_eq!(s_new, StatusCode::OK, "run-chain must never 500: {b_new}");
    let runs_new = b_new["runs"].as_array().expect("runs array");
    assert_eq!(
        runs_new.len(),
        1,
        "the re-run is its own chain head, not a CAN successor: {b_new}"
    );
    assert_eq!(runs_new[0]["exec_id"].as_str(), Some(new_id.as_str()));
    assert_eq!(runs_new[0]["sequence"], json!(0));
    assert_eq!(
        runs_new[0]["continued_to_exec_id"],
        Value::Null,
        "tail of its own chain"
    );

    // The SOURCE: sealed CONTINUED_AS_NEW by the key reuse, but with no
    // `WorkflowContinuedAsNew` event and no back-link, so #701 cannot resolve a
    // successor for it either.
    let (s_src, b_src) = get_json(&app, &format!("/workflows/{source}/run-chain")).await;
    assert_eq!(s_src, StatusCode::OK, "run-chain must never 500: {b_src}");
    let runs_src = b_src["runs"].as_array().expect("runs array");
    assert!(
        runs_src
            .iter()
            .any(|r| r["exec_id"].as_str() == Some(source.as_str())),
        "the source is present in its own chain view: {b_src}"
    );
    assert!(
        !runs_src
            .iter()
            .any(|r| r["exec_id"].as_str() == Some(new_id.as_str())),
        "the re-run must NOT appear in the source's chain: {b_src}"
    );
}

/// R-61: two concurrent re-runs of the SAME source serialize on the source
/// row's `FOR UPDATE` lock — exactly one creates a run, the loser observes the
/// sealed CONTINUED_AS_NEW state and is rejected. A double-start here would
/// duplicate real work under one business key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reruns_of_one_source_start_exactly_one_run() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_concurrent_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-concurrent");
    let source = seed_terminal(&mut conn, wf, &wf_id, "FAILED").await;

    let a = {
        let app = app.clone();
        let uri = rerun_uri(&source);
        tokio::spawn(async move { post_json(&app, &uri, json!({})).await })
    };
    let b = {
        let app = app.clone();
        let uri = rerun_uri(&source);
        tokio::spawn(async move { post_json(&app, &uri, json!({})).await })
    };
    let (ra, rb) = (a.await.unwrap(), b.await.unwrap());

    let statuses = [ra.0, rb.0];
    let created = statuses
        .iter()
        .filter(|s| **s == StatusCode::CREATED)
        .count();
    let conflicts = statuses
        .iter()
        .filter(|s| **s == StatusCode::CONFLICT)
        .count();
    assert_eq!(created, 1, "exactly one re-run may create: {ra:?} / {rb:?}");
    assert_eq!(
        conflicts, 1,
        "the loser must be rejected 409, never silently succeed: {ra:?} / {rb:?}"
    );

    // The source plus exactly ONE new run — never two.
    let rows = load_execs_for_key(&mut conn, wf, &wf_id).await;
    assert_eq!(
        rows.len(),
        2,
        "source + one new run only, found {}",
        rows.len()
    );
    let running = rows.iter().filter(|r| r.state == "RUNNING").count();
    assert_eq!(running, 1, "exactly one live run under the business key");
    assert_eq!(
        rows.iter()
            .filter(|r| r.state == "CONTINUED_AS_NEW")
            .count(),
        1,
        "the source is sealed exactly once"
    );
}

/// R-62 (issue #777 review, the P2-1 fix): every failure audit raised AFTER the
/// caller's connection is checked out must ride THAT connection.
///
/// Pinned with a SIZE-1 pool, which is what makes the assertion load-bearing:
/// the previous pool-acquiring form asked for a second connection while this
/// handler still held the only one, so the checkout blocked until the pool
/// timeout and the audit was silently dropped (`let _ =`). Mirrors the #608
/// round-2 `decode_audit_reuses_callers_connection_under_single_connection_pool`.
#[tokio::test]
async fn failure_audits_ride_the_callers_connection_under_a_size_one_pool() {
    let (url, _c) = setup_database().await;
    // Seed and verify on a normal pool; drive the HTTP request on a size-1 one.
    let seed_pool = build_pool(&url);
    let mut conn = seed_pool.get().await.unwrap();

    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.as_str());
    let tiny: DbPool = deadpool::managed::Pool::builder(manager)
        .max_size(1)
        .build()
        .expect("size-1 pool");
    let wf = "rr_size1_wf";
    let app = build_app(&tiny, vec![plain_info(wf)]);

    // A RUNNING source: rejected 409 by the state gate, i.e. a failure raised
    // well after `db_conn_for_execution` handed out the pool's only connection.
    let wf_id = unique("rr-size1");
    let source = seed_terminal(&mut conn, wf, &wf_id, "RUNNING").await;

    let (status, body) = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        post_json(&app, &rerun_uri(&source), json!({})),
    )
    .await
    .expect("the handler must not stall waiting on a second connection");
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert_eq!(
        rows.len(),
        1,
        "the failure audit must be written on the caller's own connection"
    );
    assert_eq!(rows[0].status, "failed");
    assert_eq!(rows[0].actor, TEST_ACTOR);
}

/// R-63 (issue #777 review, the P2 clone-fidelity fix): an unparseable stored
/// `workflow_retry_policy` / `context_headers` is FAIL-LOUD, never a silently
/// dropped field. Silently dropping would start the new run without the retry
/// policy the operator believes it inherited.
#[tokio::test]
async fn rerun_rejects_unparseable_stored_start_parameters() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_corrupt_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    // (a) A retry policy that is valid JSON but not a `RetryPolicy`.
    let wf_id = unique("rr-corrupt-retry");
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "FAILED",
        Seed {
            workflow_retry_policy: Some(json!({"garbage": true})),
            ..Seed::default()
        },
    )
    .await;
    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body.to_string().contains("workflow_retry_policy"),
        "the rejection names the offending field: {body}"
    );
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "nothing may be started"
    );
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "FAILED",
        "and the source must not be sealed"
    );

    // (b) Context headers that are not a string map.
    let wf_id2 = unique("rr-corrupt-headers");
    let source2 = seed_execution(
        &mut conn,
        wf,
        &wf_id2,
        "FAILED",
        Seed {
            context_headers: Some(json!({"trace": {"nested": "not-a-string"}})),
            ..Seed::default()
        },
    )
    .await;
    let (status2, body2) = post_json(&app, &rerun_uri(&source2), json!({})).await;
    assert_eq!(status2, StatusCode::CONFLICT, "{body2}");
    assert!(
        body2.to_string().contains("context_headers"),
        "the rejection names the offending field: {body2}"
    );
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id2).await.len(),
        1,
        "nothing may be started"
    );
}

/// R-64 (issue #605 review, Codex, PR #1152): a re-run clones the source's
/// `completion_callbacks` verbatim, so a target that has since been dropped
/// from the SSRF allowlist must be re-validated BEFORE the re-run is
/// created — otherwise it silently succeeds and the delivery is later
/// dropped without error at `enqueue_completion_deliveries`'s own
/// live-policy re-validation. `build_app`'s `HarvestApiState::new()`
/// installs `SsrfPolicy::default()` (empty allowlist), so any bare
/// `https://…` target is already "not (or no longer) allowlisted" without
/// any extra test setup — exactly the scenario the finding describes.
#[tokio::test]
async fn rerun_rejects_a_cloned_completion_callback_no_longer_allowlisted() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_callback_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-callback-stale");
    let stale_targets = vec![autumn_harvest::completion_callback::CallbackTarget::new(
        "https://stale-receiver.example.com/hook",
        autumn_harvest::completion_callback::EventFilter::AnyTerminal,
    )];
    let source = seed_execution(
        &mut conn,
        wf,
        &wf_id,
        "COMPLETED",
        Seed {
            completion_callbacks: Some(serde_json::to_value(&stale_targets).unwrap()),
            ..Seed::default()
        },
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"], json!("completion callback target rejected"));
    assert_eq!(
        body["url"].as_str(),
        Some("https://stale-receiver.example.com/hook")
    );

    // Nothing was started, and the source was never sealed.
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "no execution should be created for a rejected callback target"
    );
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "COMPLETED",
        "the source must not be sealed when the re-run is rejected"
    );
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert!(
        rows.iter().any(|r| r.status == "failed"),
        "expected a failed audit row: {rows:?}"
    );
}

/// Regression guard for R-64: a source with NO `completion_callbacks` at all
/// re-runs exactly as before (the new check is a no-op on the common case).
#[tokio::test]
async fn rerun_with_no_completion_callbacks_is_unaffected() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_no_callback_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-no-callback");
    let source = seed_terminal(&mut conn, wf, &wf_id, "COMPLETED").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

/// R-65 (Codex review, issue #777 PR #1152, P1): a FAILED source that already
/// has an automatic workflow-level retry successor (issue #523,
/// `retry_of_exec_id`) is rejected. `persist_workflow_failure` atomically
/// starts that successor in the SAME transaction that seals the predecessor
/// FAILED whenever attempts remain, so without this gate an operator could
/// re-run the FAILED predecessor while its successor is RUNNING — a THIRD
/// execution racing the engine's own retry. Existence alone disqualifies,
/// regardless of the successor's own state: this test uses RUNNING (the
/// literal race the finding names).
#[tokio::test]
async fn rerun_rejects_failed_source_with_a_running_retry_successor() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_retry_running_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-retry-running");
    let source = seed_terminal(&mut conn, wf, &wf_id, "FAILED").await;
    let _successor = seed_retry_successor(
        &mut conn,
        wf,
        &unique("rr-retry-running-succ"),
        &source,
        "RUNNING",
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let msg = body.to_string().to_lowercase();
    assert!(
        msg.contains("retry") && (msg.contains("chain") || msg.contains("latest")),
        "the 409 must point at the chain's latest attempt: {body}"
    );

    // Nothing new was started under the source's own key, and the source was
    // never sealed — a rejected re-run must be a pure no-op.
    assert_eq!(
        load_execs_for_key(&mut conn, wf, &wf_id).await.len(),
        1,
        "no new run under the source's key"
    );
    assert_eq!(
        load_exec(&mut conn, &source).await.state,
        "FAILED",
        "the source must not be sealed when the re-run is rejected"
    );
    let rows = audit_rows(&mut conn, "workflow.rerun", &source).await;
    assert!(
        rows.iter().any(|r| r.status == "failed"),
        "expected a failed audit row: {rows:?}"
    );
}

/// R-65b: existence of a retry successor is disqualifying regardless of ITS
/// state — a successor that has SINCE completed still means this
/// predecessor's failure was already superseded by the chain moving on, so
/// re-running the stale predecessor would still duplicate completed work.
#[tokio::test]
async fn rerun_rejects_failed_source_with_a_completed_retry_successor() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_retry_completed_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-retry-completed");
    let source = seed_terminal(&mut conn, wf, &wf_id, "FAILED").await;
    let _successor = seed_retry_successor(
        &mut conn,
        wf,
        &unique("rr-retry-completed-succ"),
        &source,
        "COMPLETED",
    )
    .await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
}

/// R-65c: regression guard — a plain FAILED source with NO retry successor
/// (the overwhelmingly common case: a manual failure with no configured
/// workflow-level retry policy, or one whose attempts are simply not yet
/// exhausted-and-superseded) re-runs exactly as before.
#[tokio::test]
async fn rerun_of_failed_source_with_no_retry_successor_still_works() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_retry_none_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-retry-none");
    let source = seed_terminal(&mut conn, wf, &wf_id, "FAILED").await;

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}
