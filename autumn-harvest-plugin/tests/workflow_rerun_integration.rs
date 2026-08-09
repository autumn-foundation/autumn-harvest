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
use diesel_async::{AsyncPgConnection, RunQueryDsl};
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

/// An app whose admin-auth boundary is NOT installed, so the `require_admin`
/// route layer rejects an unauthenticated caller (mirrors `security.rs`).
fn build_app_no_admin(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    harvest_api_router(api_state_with(infos, pool, false))
        .with_state(AppState::for_test().with_profile("test"))
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
             schedule_id, scheduled_for, origin)
         VALUES ($1, $2, 0, $3, $4, $5, $6,
                 $7, $8, COALESCE($9, 'default'), $10, $11,
                 make_interval(secs => $12::double precision),
                 make_interval(secs => $13::double precision),
                 $14, $15, $16,
                 $17, $18, $19,
                 $20, $21, $22)
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
}

const EXEC_SELECT: &str = "SELECT state, workflow_id, queue_name, input, output, error,
        completed_at, memo, search_attrs,
        EXTRACT(EPOCH FROM execution_timeout)::bigint AS execution_timeout_secs,
        EXTRACT(EPOCH FROM sla)::bigint AS sla_secs,
        owner, runbook_url, severity, completion_callbacks,
        start_source, start_source_ref, started_by,
        schedule_id, scheduled_for, origin,
        continued_from_exec_id, first_exec_id, workflow_attempt
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

#[derive(diesel::QueryableByName)]
struct AuditRow {
    #[diesel(sql_type = Text)]
    actor: String,
    #[diesel(sql_type = Text)]
    status: String,
}

async fn audit_rows(conn: &mut AsyncPgConnection, operation: &str, target: &str) -> Vec<AuditRow> {
    diesel::sql_query(
        "SELECT actor, status FROM harvest_audit_log
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
}

/// R-13: the re-run clones the source's recorded start parameters verbatim.
#[tokio::test]
async fn rerun_clones_start_params_verbatim() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let wf = "rr_clone_wf";
    let app = build_app(&pool, vec![plain_info(wf)]);
    let mut conn = pool.get().await.unwrap();

    let wf_id = unique("rr-clone");
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
            completion_callbacks: Some(json!([{"url": "https://hook.example/done"}])),
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
        Some(json!([{"url": "https://hook.example/done"}]))
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
    api_state.initialize_gate_cache(vec![AdmissionGate {
        id: AdmissionGateId(uuid::Uuid::new_v4()),
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
    assert!(body.get("gate_id").is_some(), "gate_id in body: {body}");
    assert!(body.get("reason").is_some(), "reason in body: {body}");

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

/// R-33: an unknown execution id returns 404.
#[tokio::test]
async fn rerun_unknown_execution_returns_404() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("rr_unknown_wf")]);

    let missing = autumn_harvest::types::ExecutionId::new().to_string();
    let (status, body) = post_json(&app, &rerun_uri(&missing), json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
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

/// R-38: schedule provenance (issue #488/#534) is NOT carried onto the re-run,
/// matching the reset-fork precedent.
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

    let (status, body) = post_json(&app, &rerun_uri(&source), json!({})).await;
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
    assert_ne!(
        b_after["state"],
        json!("failed"),
        "sealing the source changes what /result reports for it (documented R1 \
         consequence): {s_after} {b_after}"
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
