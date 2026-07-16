//! Integration tests for the workflow interaction-schema surface (issue #610).
//!
//! Exercises, end-to-end against a real Postgres instance, the two behaviours
//! PART 2 adds on top of the #373 schema machinery:
//!
//!   AC3 — `GET /workflows/registered/{name}/interface` returns the workflow
//!         type's published signal/query/update handlers, each with an optional
//!         `arg_schema`/`response_schema`/`description`, sorted by name and
//!         deterministic across calls. Unknown names → 404. A workflow with no
//!         published schemas omits the schema fields.
//!   AC4 — a signal or update payload is validated against its handler's
//!         published `arg_schema` *before* durable enqueue at the three HTTP
//!         boundaries (`.../signal/{name}`, `.../signal-with-start`,
//!         `.../update/{name}`), returning `400` with
//!         `{ "error": "...", "violations": [{ "message", "field_path" }] }`
//!         (RFC 6901 pointer). A handler with no published schema is not
//!         validated (today's behaviour).
//!
//! Runs against a real Postgres: uses `HARVEST_TEST_DATABASE_URL` (creating a
//! fresh per-test database via `psql`) when set, otherwise a testcontainers
//! Postgres 16 container. Docker-backed in CI (per the #543/#544/#601
//! precedent).

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{
    QueryHandlerInfo, SignalHandlerInfo, UpdateHandlerInfo, WorkflowInfo,
};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// The full migration schema, mirroring every other plugin integration
/// suite (e.g. `query_integration.rs`) — uses `full_migrations_sql()` so the
/// test schema always tracks trunk and never hand-rolls a bundle (which the
/// `migration_hygiene` guard forbids).
fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

// ── Published schemas ──────────────────────────────────────────────────────

fn approve_arg_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "reason": { "type": "string" } },
        "required": ["reason"]
    })
}

fn priority_arg_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "priority": { "type": "integer" } },
        "required": ["priority"]
    })
}

fn priority_response_schema() -> Value {
    json!({ "type": "object", "properties": { "ok": { "type": "boolean" } } })
}

fn progress_arg_schema() -> Value {
    json!({ "type": "object", "properties": { "include_summary": { "type": "boolean" } } })
}

fn progress_response_schema() -> Value {
    json!({ "type": "object", "properties": { "processed": { "type": "integer" } } })
}

// ── Test workflows & handler infos ─────────────────────────────────────────

fn iface_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

fn iface_info() -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name: "iface_wf",
        module: "tests",
        handler: iface_workflow,
        execution_timeout: None,
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

fn plain_info() -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name: "plain_wf",
        module: "tests",
        handler: iface_workflow,
        execution_timeout: None,
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

fn query_handlers() -> Vec<QueryHandlerInfo> {
    vec![
        // Deliberately declared second so the /interface sort is observable.
        QueryHandlerInfo {
            name: "status",
            workflow: "iface_wf",
            module: "tests",
            input_type_hint: "()",
            output_type_hint: "String",
            handler: |_ctx, _args| Ok(json!("running")),
            description: Some("Current coarse status."),
            arg_schema: None,
            response_schema: None,
        },
        QueryHandlerInfo {
            name: "progress",
            workflow: "iface_wf",
            module: "tests",
            input_type_hint: "ProgressReq",
            output_type_hint: "ProgressResp",
            handler: |_ctx, _args| Ok(json!({ "processed": 0 })),
            description: Some("Progress counter."),
            arg_schema: Some(progress_arg_schema),
            response_schema: Some(progress_response_schema),
        },
    ]
}

fn update_handlers() -> Vec<UpdateHandlerInfo> {
    vec![
        UpdateHandlerInfo {
            name: "set_priority",
            workflow: "iface_wf",
            module: "tests",
            input_type_hint: "SetPriority",
            output_type_hint: "Ack",
            has_validator: false,
            handler: |_ctx, _args| Box::pin(async move { Ok(json!({ "ok": true })) }),
            validator: None,
            mcp: false,
            description: Some("Set the run priority."),
            arg_schema: Some(priority_arg_schema),
            response_schema: Some(priority_response_schema),
        },
        // On the schema-less workflow: must NOT be validated at the edge.
        UpdateHandlerInfo {
            name: "plain_upd",
            workflow: "plain_wf",
            module: "tests",
            input_type_hint: "Value",
            output_type_hint: "Value",
            has_validator: false,
            handler: |_ctx, _args| Box::pin(async move { Ok(Value::Null) }),
            validator: None,
            mcp: false,
            description: None,
            arg_schema: None,
            response_schema: None,
        },
    ]
}

fn signal_handlers() -> Vec<SignalHandlerInfo> {
    vec![
        SignalHandlerInfo {
            name: "approve",
            workflow: "iface_wf",
            module: "tests",
            arg_type_hint: "ApproveRequest",
            description: Some("Approve the pending request."),
            arg_schema: Some(approve_arg_schema),
        },
        // On the schema-less workflow: must NOT be validated at the edge.
        SignalHandlerInfo {
            name: "plain_sig",
            workflow: "plain_wf",
            module: "tests",
            arg_type_hint: "Value",
            description: None,
            arg_schema: None,
        },
    ]
}

// ── Harness ────────────────────────────────────────────────────────────────

type HarvestApiApp = axum::Router;

static DB_SEQ: AtomicU64 = AtomicU64::new(0);

/// Provision a database, returning its URL plus (in the testcontainers path) a
/// container guard that must be kept alive for the test's duration.
///
/// - `HARVEST_TEST_DATABASE_URL` set → create a fresh, uniquely-named database
///   via `psql` against that server and apply `INIT_SQL` into it (lets the
///   suite run against a locally-installed Postgres without Docker).
/// - unset → start a fresh testcontainers Postgres 16 (the CI path).
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let (server, _base_db) = base_url.rsplit_once('/').expect("db url has a database segment");
        let seq = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dbname = format!("harvest_iface_{}_{seq}_{nanos}", std::process::id());
        run_psql(&base_url, &format!("CREATE DATABASE {dbname};"));
        let db_url = format!("{server}/{dbname}");
        let mut tmp = std::env::temp_dir();
        tmp.push(format!("harvest_iface_init_{dbname}.sql"));
        std::fs::write(&tmp, init_sql()).expect("write init sql");
        let out = std::process::Command::new("psql")
            .arg(&db_url)
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-q")
            .arg("-f")
            .arg(&tmp)
            .env("PGPASSWORD", "postgres")
            .output()
            .expect("psql -f INIT_SQL");
        assert!(
            out.status.success(),
            "applying INIT_SQL failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let _ = std::fs::remove_file(&tmp);
        return (db_url, None);
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

fn run_psql(conn_url: &str, sql: &str) {
    let out = std::process::Command::new("psql")
        .arg(conn_url)
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-c")
        .arg(sql)
        .env("PGPASSWORD", "postgres")
        .output()
        .expect("psql -c");
    assert!(
        out.status.success(),
        "psql `{sql}` failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let registry = HandlerRegistry::new(vec![iface_info(), plain_info()], vec![])
        .with_handler_infos(query_handlers(), update_handlers(), signal_handlers());
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("iface-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn get(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    read_response(response).await
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
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
    read_response(response).await
}

/// Read a response, returning the raw bytes too so callers can byte-compare
/// deterministic bodies without JSON-key reordering.
async fn read_response_raw(response: axum::response::Response) -> (StatusCode, Vec<u8>) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn read_response(response: axum::response::Response) -> (StatusCode, Value) {
    let (status, bytes) = read_response_raw(response).await;
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

async fn get_raw(app: &HarvestApiApp, uri: &str) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    read_response_raw(response).await
}

/// Seed a minimal `RUNNING` execution row + `WorkflowStarted` event for the
/// given workflow type so `load_execution`/`admit_update`/`send_signal`
/// succeed. Returns the execution id.
async fn seed_running_execution(pool: &DbPool, workflow_name: &str) -> ExecutionId {
    let mut conn = pool.get().await.expect("pooled conn");
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, $2, $3, 0, $4, 'default', 'RUNNING')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Jsonb, _>(json!({}))
    .execute(&mut conn)
    .await
    .expect("seed execution");

    let events = vec![WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("seed history");
    exec_id
}

fn names(arr: &Value) -> Vec<String> {
    arr.as_array()
        .expect("array")
        .iter()
        .map(|e| e["name"].as_str().expect("name").to_string())
        .collect()
}

// ── AC3: /interface discovery ──────────────────────────────────────────────

#[tokio::test]
async fn interface_lists_sorted_handlers_with_schemas() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = get(&app, "/workflows/registered/iface_wf/interface").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    // Signals: one entry `approve`, with an arg_schema + description, no response_schema.
    assert_eq!(names(&body["signals"]), vec!["approve"]);
    assert_eq!(body["signals"][0]["description"], json!("Approve the pending request."));
    assert_eq!(body["signals"][0]["arg_schema"], approve_arg_schema());
    assert!(
        body["signals"][0].get("response_schema").is_none(),
        "signals never carry a response_schema"
    );

    // Queries: sorted by name → progress, status.
    assert_eq!(names(&body["queries"]), vec!["progress", "status"]);
    assert_eq!(body["queries"][0]["arg_schema"], progress_arg_schema());
    assert_eq!(body["queries"][0]["response_schema"], progress_response_schema());
    // `status` has a description but no schemas → schema fields omitted.
    assert_eq!(body["queries"][1]["description"], json!("Current coarse status."));
    assert!(body["queries"][1].get("arg_schema").is_none());
    assert!(body["queries"][1].get("response_schema").is_none());

    // Updates: one entry `set_priority` with both schemas + description.
    assert_eq!(names(&body["updates"]), vec!["set_priority"]);
    assert_eq!(body["updates"][0]["arg_schema"], priority_arg_schema());
    assert_eq!(body["updates"][0]["response_schema"], priority_response_schema());
    assert_eq!(body["updates"][0]["description"], json!("Set the run priority."));
}

#[tokio::test]
async fn interface_is_deterministic_across_calls() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (s1, b1) = get_raw(&app, "/workflows/registered/iface_wf/interface").await;
    let (s2, b2) = get_raw(&app, "/workflows/registered/iface_wf/interface").await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b1, b2, "interface response must be byte-identical across calls");
}

#[tokio::test]
async fn interface_unknown_workflow_returns_404() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, _body) = get(&app, "/workflows/registered/does_not_exist/interface").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn interface_omits_schema_fields_for_schema_less_workflow() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = get(&app, "/workflows/registered/plain_wf/interface").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    assert_eq!(names(&body["signals"]), vec!["plain_sig"]);
    assert!(body["signals"][0].get("arg_schema").is_none());
    assert!(body["signals"][0].get("description").is_none());
    assert!(body["queries"].as_array().unwrap().is_empty());
    assert_eq!(names(&body["updates"]), vec!["plain_upd"]);
    assert!(body["updates"][0].get("arg_schema").is_none());
    assert!(body["updates"][0].get("response_schema").is_none());
}

// ── AC4: boundary validation ───────────────────────────────────────────────

fn assert_field_violation(body: &Value, error_contains: &str, expected_pointer: &str) {
    assert_eq!(
        body["error"].as_str().unwrap_or_default().contains(error_contains),
        true,
        "error should mention `{error_contains}`, got: {body}"
    );
    let violations = body["violations"].as_array().expect("violations array");
    assert!(!violations.is_empty(), "expected at least one violation: {body}");
    assert!(
        violations
            .iter()
            .any(|v| v["field_path"].as_str() == Some(expected_pointer)),
        "expected a violation with field_path `{expected_pointer}`, got: {body}"
    );
    // Each violation must carry a human-readable message.
    assert!(violations.iter().all(|v| v["message"].is_string()));
}

#[tokio::test]
async fn signal_route_rejects_malformed_payload_with_400() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_running_execution(&pool, "iface_wf").await;

    // `approve` requires a string `reason`; send an empty object.
    let (status, body) =
        post_json(&app, &format!("/workflows/{exec_id}/signal/approve"), json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_field_violation(&body, "signal payload validation failed", "/reason");
}

#[tokio::test]
async fn signal_route_accepts_valid_payload() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_running_execution(&pool, "iface_wf").await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/signal/approve"),
        json!({ "reason": "looks good" }),
    )
    .await;
    assert_ne!(status, StatusCode::BAD_REQUEST, "valid payload must not 400: {body}");
    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
}

#[tokio::test]
async fn signal_with_start_rejects_malformed_payload_with_400() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Validation runs before any durable start, so no seeded execution is needed.
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/signal-with-start",
        json!({
            "workflow_id": "sws-iface-1",
            "signal_name": "approve",
            "signal_payload": {}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_field_violation(&body, "signal payload validation failed", "/reason");
}

#[tokio::test]
async fn update_route_rejects_malformed_payload_with_400() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_running_execution(&pool, "iface_wf").await;

    // `set_priority` requires an integer `priority`; send a string.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/update/set_priority?wait=admitted"),
        json!({ "input": { "priority": "high" } }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_field_violation(&body, "update payload validation failed", "/priority");
}

#[tokio::test]
async fn update_route_accepts_valid_payload() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_running_execution(&pool, "iface_wf").await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/update/set_priority?wait=admitted"),
        json!({ "input": { "priority": 3 } }),
    )
    .await;
    assert_ne!(status, StatusCode::BAD_REQUEST, "valid payload must not 400: {body}");
    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
}

#[tokio::test]
async fn schema_less_workflow_signal_and_update_are_not_validated() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let sig_exec = seed_running_execution(&pool, "plain_wf").await;
    let upd_exec = seed_running_execution(&pool, "plain_wf").await;

    // A signal to a handler with no published schema is accepted with any payload.
    let (sig_status, sig_body) = post_json(
        &app,
        &format!("/workflows/{sig_exec}/signal/plain_sig"),
        json!({ "anything": 1 }),
    )
    .await;
    assert_ne!(sig_status, StatusCode::BAD_REQUEST, "no-schema signal must not 400: {sig_body}");
    assert_eq!(sig_status, StatusCode::ACCEPTED, "body: {sig_body}");

    // Likewise an update with no published schema.
    let (upd_status, upd_body) = post_json(
        &app,
        &format!("/workflows/{upd_exec}/update/plain_upd?wait=admitted"),
        json!({ "input": { "anything": "goes" } }),
    )
    .await;
    assert_ne!(upd_status, StatusCode::BAD_REQUEST, "no-schema update must not 400: {upd_body}");
    assert_eq!(upd_status, StatusCode::ACCEPTED, "body: {upd_body}");
}
