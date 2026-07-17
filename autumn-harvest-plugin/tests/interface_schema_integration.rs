//! Integration tests for the workflow interaction-schema surface (issue #610).
//!
//! Exercises, end-to-end against a real Postgres instance, the two behaviours
//! PART 2 adds on top of the #373 schema machinery:
//!
//!   AC3 — `GET /workflows/registered/{name}/interface` returns the workflow
//!         type's published signal/query/update handlers, each with an optional
//!         `arg_schema`/`response_schema`/`description`, sorted by name and
//!         deterministic across calls. Unknown names → 404. A workflow with no
//!         published schemas omits the schema fields, and a workflow with *no
//!         handlers at all* returns three empty arrays.
//!   AC4 — a signal or update payload is validated against its handler's
//!         published `arg_schema` *before* durable enqueue at every HTTP
//!         boundary (`.../signal/{name}`, `.../signal-with-start`,
//!         `.../update/{name}`, `.../update-with-start`), returning `400` with
//!         `{ "error": "...", "violations": [{ "message", "field_path" }] }`
//!         (RFC 6901 pointer). A handler with no published schema is not
//!         validated (today's behaviour). When a handler carries BOTH an
//!         `arg_schema` and a semantic validator, the structural schema gate
//!         (400) runs *before* the validator (422).
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
use autumn_harvest::info::{QueryHandlerInfo, SignalHandlerInfo, UpdateHandlerInfo, WorkflowInfo};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, UpdateId};
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

/// A workflow with ZERO query/update/signal handlers — its `/interface`
/// document must have all three arrays empty (FIX 4 / AC3).
fn empty_info() -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name: "empty_wf",
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

/// Semantic validator for `set_priority_gated`: rejects `priority > 10`. A
/// well-typed payload (integer `priority`) that violates this business rule
/// must reach the validator (422), *after* the #610 schema gate (400) has
/// already accepted its shape.
fn reject_priority_over_ten(args: &Value) -> Result<(), String> {
    match args.get("priority").and_then(Value::as_i64) {
        Some(p) if p > 10 => Err(format!("priority {p} exceeds the maximum of 10")),
        _ => Ok(()),
    }
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
        // Carries BOTH a published arg_schema AND a semantic validator, so the
        // schema-400-before-validator-422 ordering is observable (FIX 2 / AC4).
        UpdateHandlerInfo {
            name: "set_priority_gated",
            workflow: "iface_wf",
            module: "tests",
            input_type_hint: "SetPriority",
            output_type_hint: "Ack",
            has_validator: true,
            handler: |_ctx, _args| Box::pin(async move { Ok(json!({ "ok": true })) }),
            validator: Some(reject_priority_over_ten),
            mcp: false,
            description: Some("Set the run priority (bounded to 10)."),
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
        let (server, _base_db) = base_url
            .rsplit_once('/')
            .expect("db url has a database segment");
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
    let registry = HandlerRegistry::new(vec![iface_info(), plain_info(), empty_info()], vec![])
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
    seed_execution_in_state(pool, workflow_name, "RUNNING").await
}

/// State-parameterized variant of [`seed_running_execution`]. The multi-shard
/// `found_shard` scan in signal-/update-with-start excludes only
/// `CONTINUED_AS_NEW`/`TERMINATED`, so `COMPLETED`/`PAUSED` rows are found and
/// route correctly — and the read-only committed-replay probes join
/// `harvest_signals`/`harvest_events` with no state filter at all, so they
/// resolve a dedup hit against a terminal prior too.
async fn seed_execution_in_state(
    pool: &DbPool,
    workflow_name: &str,
    state: &str,
) -> ExecutionId {
    let mut conn = pool.get().await.expect("pooled conn");
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, $2, $3, 0, $4, 'default', $5)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Jsonb, _>(json!({}))
    .bind::<diesel::sql_types::Text, _>(state)
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

/// Reproduce the deterministic `update_id` the `update-with-start` handler
/// derives from an idempotency key (UUIDv5 over the OID namespace), so a test
/// can seed a matching `UpdateAdmitted` event that the committed-replay probe
/// will find.
fn derive_uws_update_id(key: &str) -> UpdateId {
    let namespace = uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        .expect("static namespace UUID is valid");
    UpdateId::from_uuid(uuid::Uuid::new_v5(&namespace, key.as_bytes()))
}

/// Seed an `UpdateAdmitted` event for `(exec_id, update_id)` directly, simulating
/// an update that was admitted on a prior committed delivery. `lookup_idempotent_
/// update_dedupe` matches on `event_data->'data'->>'update_id'`, so the seeded
/// event's `update_id` must serialize to the same UUID string the probe binds.
async fn seed_update_admitted_event(
    pool: &DbPool,
    exec_id: ExecutionId,
    update_id: UpdateId,
    update_name: &str,
) {
    let mut conn = pool.get().await.expect("pooled conn");
    let events = vec![WorkflowEvent::UpdateAdmitted {
        update_id,
        name: update_name.to_string(),
        input: json!({}),
        timestamp: Utc::now(),
    }];
    // WorkflowStarted was appended at event_id 0 by the seed helper; continue at 1.
    store::append_events(&mut conn, exec_id, &events, 1)
        .await
        .expect("seed UpdateAdmitted event");
}

/// Count `UpdateAdmitted` events for a given execution.
async fn count_update_admitted_events(pool: &DbPool, exec_id: ExecutionId) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = pool.get().await.expect("pooled conn");
    let row: Cnt = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = 'UpdateAdmitted'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("count UpdateAdmitted events");
    row.n
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
    assert_eq!(
        body["signals"][0]["description"],
        json!("Approve the pending request.")
    );
    assert_eq!(body["signals"][0]["arg_schema"], approve_arg_schema());
    assert!(
        body["signals"][0].get("response_schema").is_none(),
        "signals never carry a response_schema"
    );

    // Queries: sorted by name → progress, status.
    assert_eq!(names(&body["queries"]), vec!["progress", "status"]);
    assert_eq!(body["queries"][0]["arg_schema"], progress_arg_schema());
    assert_eq!(
        body["queries"][0]["response_schema"],
        progress_response_schema()
    );
    // `status` has a description but no schemas → schema fields omitted.
    assert_eq!(
        body["queries"][1]["description"],
        json!("Current coarse status.")
    );
    assert!(body["queries"][1].get("arg_schema").is_none());
    assert!(body["queries"][1].get("response_schema").is_none());

    // Updates: sorted by name → set_priority, set_priority_gated (the latter
    // carries both an arg_schema and a semantic validator; see FIX 2 tests).
    assert_eq!(
        names(&body["updates"]),
        vec!["set_priority", "set_priority_gated"]
    );
    assert_eq!(body["updates"][0]["arg_schema"], priority_arg_schema());
    assert_eq!(
        body["updates"][0]["response_schema"],
        priority_response_schema()
    );
    assert_eq!(
        body["updates"][0]["description"],
        json!("Set the run priority.")
    );
    // The gated update publishes the same argument schema and its own description.
    assert_eq!(body["updates"][1]["arg_schema"], priority_arg_schema());
    assert_eq!(
        body["updates"][1]["description"],
        json!("Set the run priority (bounded to 10).")
    );
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
    assert_eq!(
        b1, b2,
        "interface response must be byte-identical across calls"
    );
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
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains(error_contains),
        "error should mention `{error_contains}`, got: {body}"
    );
    let violations = body["violations"].as_array().expect("violations array");
    assert!(
        !violations.is_empty(),
        "expected at least one violation: {body}"
    );
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
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/signal/approve"),
        json!({}),
    )
    .await;
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
    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "valid payload must not 400: {body}"
    );
    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
}

/// Seed a raw `harvest_signals` row directly, simulating a signal that landed
/// *before* the `approve` handler's `arg_schema` was published or tightened.
/// The payload is inserted verbatim (no schema gate), so it may be
/// schema-invalid by the current rules.
async fn seed_signal_row(
    pool: &DbPool,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: Value,
    idempotency_key: &str,
) {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(
        "INSERT INTO harvest_signals \
         (workflow_exec_id, signal_name, payload, idempotency_key) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(signal_name)
    .bind::<diesel::sql_types::Jsonb, _>(payload)
    .bind::<diesel::sql_types::Text, _>(idempotency_key)
    .execute(&mut conn)
    .await
    .expect("seed signal row");
}

/// Count `harvest_signals` rows for a given `(exec_id, idempotency_key)`.
async fn count_signal_rows(pool: &DbPool, exec_id: ExecutionId, idempotency_key: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Cnt {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = pool.get().await.expect("pooled conn");
    let row: Cnt = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_signals \
         WHERE workflow_exec_id = $1 AND idempotency_key = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(idempotency_key)
    .get_result(&mut conn)
    .await
    .expect("count signal rows");
    row.n
}

/// Regression for the Codex P2 (issue #610): the #610 schema gate (and the
/// #252 cap) must NOT reject a *committed keyed replay* of a signal that was
/// accepted before the schema was published/tightened. A same-key retry
/// carrying a now-schema-invalid payload must return `202 signal_delivered:
/// false` and enqueue no second row — preserving #521's exactly-once contract.
/// A *fresh* keyed malformed delivery is still validated (400), and a fresh
/// valid keyed delivery still succeeds (`signal_delivered: true`).
#[tokio::test]
async fn signal_route_committed_keyed_replay_short_circuits_before_schema_gate() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_running_execution(&pool, "iface_wf").await;

    // A signal accepted *before* the `approve` schema existed: seed it directly
    // with a payload that is invalid under the now-published schema (`reason`
    // is required + must be a string).
    let committed_key = "k-committed";
    seed_signal_row(
        &pool,
        exec_id,
        "approve",
        json!({ "reason": 123 }),
        committed_key,
    )
    .await;
    assert_eq!(
        count_signal_rows(&pool, exec_id, committed_key).await,
        1,
        "precondition: exactly one committed row"
    );

    // Same-key retry with a now-schema-invalid payload → dedup no-op, NOT 400.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/signal/approve?idempotency_key={committed_key}"),
        json!({}), // invalid under `approve` schema (missing `reason`)
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "committed keyed replay must dedup to 202, not be rejected by the schema gate: {body}"
    );
    assert_eq!(
        body["signal_delivered"],
        json!(false),
        "committed keyed replay must report signal_delivered: false: {body}"
    );
    assert_eq!(
        count_signal_rows(&pool, exec_id, committed_key).await,
        1,
        "committed keyed replay must NOT enqueue a second row"
    );

    // A *fresh* keyed malformed delivery (key never seen) is still validated
    // at the edge → 400, and enqueues nothing.
    let fresh_bad_key = "k-fresh-bad";
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/signal/approve?idempotency_key={fresh_bad_key}"),
        json!({}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "fresh keyed malformed signal must still be validated before enqueue: {body}"
    );
    assert_field_violation(&body, "signal payload validation failed", "/reason");
    assert_eq!(
        count_signal_rows(&pool, exec_id, fresh_bad_key).await,
        0,
        "a rejected fresh keyed signal must enqueue nothing"
    );

    // A *fresh* valid keyed delivery still succeeds and enqueues a row.
    let fresh_good_key = "k-fresh-good";
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/signal/approve?idempotency_key={fresh_good_key}"),
        json!({ "reason": "looks good" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "fresh valid keyed signal: {body}"
    );
    assert_eq!(
        body["signal_delivered"],
        json!(true),
        "fresh valid keyed signal must report signal_delivered: true: {body}"
    );
    assert_eq!(
        count_signal_rows(&pool, exec_id, fresh_good_key).await,
        1,
        "fresh valid keyed signal must enqueue exactly one row"
    );
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
    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "valid payload must not 400: {body}"
    );
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
    assert_ne!(
        sig_status,
        StatusCode::BAD_REQUEST,
        "no-schema signal must not 400: {sig_body}"
    );
    assert_eq!(sig_status, StatusCode::ACCEPTED, "body: {sig_body}");

    // Likewise an update with no published schema.
    let (upd_status, upd_body) = post_json(
        &app,
        &format!("/workflows/{upd_exec}/update/plain_upd?wait=admitted"),
        json!({ "input": { "anything": "goes" } }),
    )
    .await;
    assert_ne!(
        upd_status,
        StatusCode::BAD_REQUEST,
        "no-schema update must not 400: {upd_body}"
    );
    assert_eq!(upd_status, StatusCode::ACCEPTED, "body: {upd_body}");
}

// ── FIX 2 / AC4: schema-400 precedes semantic-validator-422 ────────────────

#[tokio::test]
async fn update_schema_gate_400_precedes_validator_when_shape_is_wrong() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_running_execution(&pool, "iface_wf").await;

    // Wrong TYPE for `priority` (string, not integer). The #610 schema gate
    // must reject with a field-level 400 *before* the semantic validator runs,
    // so the response is the structural-violation body, not the validator body.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/update/set_priority_gated?wait=admitted"),
        json!({ "input": { "priority": "high" } }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_field_violation(&body, "update payload validation failed", "/priority");
    // It must NOT be the validator body.
    assert!(
        body.get("reason").is_none(),
        "schema failure must not surface the validator `reason`: {body}"
    );
}

#[tokio::test]
async fn update_validator_422_runs_after_schema_gate_accepts_shape() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_running_execution(&pool, "iface_wf").await;

    // Well-typed integer `priority` (passes the schema gate) but violates the
    // business rule (> 10): the pre-existing validator path must still fire and
    // return the 422 validator body, unbroken by the new #610 schema gate.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/update/set_priority_gated?wait=admitted"),
        json!({ "input": { "priority": 99 } }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert_eq!(
        body["error"], "update rejected by validator",
        "body: {body}"
    );
    assert!(
        body["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("exceeds the maximum"),
        "validator reason should be surfaced: {body}"
    );
    // It must NOT be the structural-violation body.
    assert!(
        body.get("violations").is_none(),
        "validator rejection must not carry schema `violations`: {body}"
    );
}

#[tokio::test]
async fn update_gated_accepts_a_valid_within_bounds_payload() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_running_execution(&pool, "iface_wf").await;

    // Well-typed AND within the validator bound → neither gate fires.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/update/set_priority_gated?wait=admitted"),
        json!({ "input": { "priority": 5 } }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
}

// ── FIX 3 / AC4: update-with-start boundary validation ─────────────────────

#[tokio::test]
async fn update_with_start_rejects_malformed_payload_with_400() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Validation runs before the durable start+admit, so no seeded run is needed.
    // `set_priority` requires an integer `priority`; send a string.
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/update-with-start",
        json!({
            "workflow_id": "uws-iface-malformed",
            "update_name": "set_priority",
            "update_args": { "priority": "high" },
            "wait_for_stage": "admitted"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_field_violation(&body, "update payload validation failed", "/priority");
}

#[tokio::test]
async fn update_with_start_accepts_valid_payload() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/update-with-start",
        json!({
            "workflow_id": "uws-iface-valid",
            "update_name": "set_priority",
            "update_args": { "priority": 3 },
            "wait_for_stage": "admitted"
        }),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "valid update-with-start payload must not 400: {body}"
    );
}

// ── FIX 4 / AC3: fully-empty interface + signal-with-start happy path ───────

#[tokio::test]
async fn interface_is_all_empty_for_workflow_with_no_handlers() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = get(&app, "/workflows/registered/empty_wf/interface").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(
        body["signals"].as_array().unwrap().is_empty(),
        "signals must be empty: {body}"
    );
    assert!(
        body["queries"].as_array().unwrap().is_empty(),
        "queries must be empty: {body}"
    );
    assert!(
        body["updates"].as_array().unwrap().is_empty(),
        "updates must be empty: {body}"
    );
}

#[tokio::test]
async fn signal_with_start_accepts_valid_payload() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // A schema-valid `approve` payload must pass the #610 gate and proceed to
    // the durable start-or-attach (mirrors signal_route_accepts_valid_payload).
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/signal-with-start",
        json!({
            "workflow_id": "sws-iface-valid",
            "signal_name": "approve",
            "signal_payload": { "reason": "looks good" }
        }),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "valid signal-with-start payload must not 400: {body}"
    );
    assert!(
        status.is_success(),
        "valid signal-with-start should proceed (2xx), got {status}: {body}"
    );
}

// ── with-start keyed committed-replay: validation ordering (this PR) ─────────
//
// A retry of an already-committed keyed signal-/update-with-start must replay to
// its documented no-op BEFORE any fresh-start-only validation runs (#610 schema
// gate, #373 start_input schema, #684 validator). Otherwise, if validation
// TIGHTENED between the original delivery and the retry (schema published/made
// stricter, payload cap lowered), the retry is wrongly rejected 400/422 instead
// of returning the cached outcome. This mirrors #808 (plain start route) and
// #1092 (plain signal route). A probe MISS falls through to the untouched
// authoritative in-lock path, which stays the source of truth.

/// AC1: a committed keyed signal-with-start that ATTACHED to a live (RUNNING)
/// run replays to `200 signal_delivered: false` even when the retry payload is
/// now schema-invalid — the committed-replay probe short-circuits before the
/// #610 signal-payload schema gate. No second signal row is enqueued.
#[tokio::test]
async fn sws_committed_keyed_replay_after_attach_short_circuits_before_schema_gate() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_execution_in_state(&pool, "iface_wf", "RUNNING").await;
    let wid = exec_id.to_string();

    // Simulate a signal-with-start that committed before the `approve` schema
    // was published: seed a matching signal row whose payload is now invalid.
    let committed_key = "k-sws-attach";
    seed_signal_row(
        &pool,
        exec_id,
        "approve",
        json!({ "reason": 123 }),
        committed_key,
    )
    .await;
    assert_eq!(
        count_signal_rows(&pool, exec_id, committed_key).await,
        1,
        "precondition: exactly one committed signal row"
    );

    // Same-key retry with a now-schema-invalid signal payload (`{}`, missing the
    // required string `reason`) → committed-replay no-op, NOT the schema 400.
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/signal-with-start",
        json!({
            "workflow_id": wid,
            "signal_name": "approve",
            "signal_payload": {},
            "idempotency_key": committed_key
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "committed keyed signal-with-start replay must dedup to 200, not be rejected by the \
         schema gate: {body}"
    );
    assert_eq!(
        body["signal_delivered"],
        json!(false),
        "committed replay must report signal_delivered: false: {body}"
    );
    assert_eq!(
        body["execution_id"],
        json!(exec_id.to_string()),
        "committed replay must return the original execution_id: {body}"
    );
    assert_eq!(
        count_signal_rows(&pool, exec_id, committed_key).await,
        1,
        "committed replay must NOT enqueue a second signal row"
    );
}

/// AC2: the same short-circuit holds when the committed prior is TERMINAL
/// (COMPLETED). The in-lock path would escalate a terminal prior to a fresh
/// start, but the idempotency-key dedup (and the probe mirroring it) recognizes
/// the committed key first and replays the no-op. Payload again now-invalid.
#[tokio::test]
async fn sws_committed_keyed_replay_after_terminal_fresh_start_short_circuits() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_execution_in_state(&pool, "iface_wf", "COMPLETED").await;
    let wid = exec_id.to_string();

    let committed_key = "k-sws-fresh";
    seed_signal_row(
        &pool,
        exec_id,
        "approve",
        json!({ "reason": 123 }),
        committed_key,
    )
    .await;

    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/signal-with-start",
        json!({
            "workflow_id": wid,
            "signal_name": "approve",
            "signal_payload": {},
            "idempotency_key": committed_key
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "committed keyed replay against a terminal prior must dedup to 200: {body}"
    );
    assert_eq!(
        body["signal_delivered"],
        json!(false),
        "committed replay must report signal_delivered: false: {body}"
    );
    assert_eq!(
        count_signal_rows(&pool, exec_id, committed_key).await,
        1,
        "committed replay must NOT enqueue a second signal row"
    );
}

/// AC3: a committed keyed update-with-start replay against a RUNNING run
/// short-circuits before the #610 update-arg schema gate (and the #373
/// start_input gate). Retry args are now schema-invalid; the response is the
/// cached admission (`200`, `started_fresh: false`), NOT a 400. No second
/// `UpdateAdmitted` event is appended.
#[tokio::test]
async fn uws_committed_keyed_replay_after_running_short_circuits_before_validation() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_execution_in_state(&pool, "iface_wf", "RUNNING").await;
    let wid = exec_id.to_string();

    let committed_key = "k-uws-run";
    let update_id = derive_uws_update_id(committed_key);
    seed_update_admitted_event(&pool, exec_id, update_id, "set_priority").await;
    assert_eq!(
        count_update_admitted_events(&pool, exec_id).await,
        1,
        "precondition: exactly one committed UpdateAdmitted event"
    );

    // Retry with a now-schema-invalid update arg (`priority` string, not int).
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/update-with-start",
        json!({
            "workflow_id": wid,
            "update_name": "set_priority",
            "update_args": { "priority": "high" },
            "idempotency_key": committed_key,
            "wait_for_stage": "admitted"
        }),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "committed keyed update-with-start replay must NOT be rejected by the schema gate: {body}"
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "committed keyed replay must return the cached admission (200): {body}"
    );
    assert_eq!(
        body["update_id"],
        json!(update_id.to_string()),
        "committed replay must return the derived update_id: {body}"
    );
    assert_eq!(
        body["started_fresh"],
        json!(false),
        "committed replay must report started_fresh: false: {body}"
    );
    assert_eq!(
        count_update_admitted_events(&pool, exec_id).await,
        1,
        "committed replay must NOT append a second UpdateAdmitted event"
    );
}

/// AC4: a committed keyed update-with-start replay does not re-run the #684
/// semantic validator. Seeded against a COMPLETED prior with `set_priority_gated`
/// (validator rejects `priority > 10`); the retry carries `priority: 99` which
/// would 422, but the committed-replay probe returns the cached outcome (200)
/// before the validator runs.
#[tokio::test]
async fn uws_committed_keyed_replay_validator_not_rerun() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_execution_in_state(&pool, "iface_wf", "COMPLETED").await;
    let wid = exec_id.to_string();

    let committed_key = "k-uws-gated";
    let update_id = derive_uws_update_id(committed_key);
    seed_update_admitted_event(&pool, exec_id, update_id, "set_priority_gated").await;

    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/update-with-start",
        json!({
            "workflow_id": wid,
            "update_name": "set_priority_gated",
            "update_args": { "priority": 99 },
            "idempotency_key": committed_key,
            "wait_for_stage": "admitted"
        }),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "committed keyed replay must NOT re-run the validator (422): {body}"
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "committed keyed replay must return the cached admission (200): {body}"
    );
    assert_eq!(
        body["update_id"],
        json!(update_id.to_string()),
        "committed replay must return the derived update_id: {body}"
    );
    assert_eq!(
        count_update_admitted_events(&pool, exec_id).await,
        1,
        "committed replay must NOT append a second UpdateAdmitted event"
    );
}

/// AC5a: a FRESH keyed signal-with-start with a malformed payload (key never
/// seen) is still validated at the edge → 400. The probe misses, so the #610
/// schema gate runs as before.
#[tokio::test]
async fn sws_fresh_keyed_malformed_still_400() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/signal-with-start",
        json!({
            "workflow_id": "sws-fresh-bad",
            "signal_name": "approve",
            "signal_payload": {},
            "idempotency_key": "k-sws-fresh-bad"
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a fresh keyed malformed signal-with-start must still be validated: {body}"
    );
    assert_field_violation(&body, "signal payload validation failed", "/reason");
}

/// AC5b: a FRESH keyed update-with-start with a malformed payload is still
/// validated at the edge → 400.
#[tokio::test]
async fn uws_fresh_keyed_malformed_still_400() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/update-with-start",
        json!({
            "workflow_id": "uws-fresh-bad",
            "update_name": "set_priority",
            "update_args": { "priority": "high" },
            "idempotency_key": "k-uws-fresh-bad",
            "wait_for_stage": "admitted"
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a fresh keyed malformed update-with-start must still be validated: {body}"
    );
    assert_field_violation(&body, "update payload validation failed", "/priority");
}

/// AC6: the UNKEYED path is byte-for-byte unchanged — a valid unkeyed
/// signal-/update-with-start still proceeds (2xx), and a malformed unkeyed one
/// is still rejected 400. The probe is keyed-only, so it never runs here.
#[tokio::test]
async fn sws_uws_unkeyed_behavior_unchanged() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Valid unkeyed signal-with-start → proceeds (2xx).
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/signal-with-start",
        json!({
            "workflow_id": "sws-unkeyed-ok",
            "signal_name": "approve",
            "signal_payload": { "reason": "ok" }
        }),
    )
    .await;
    assert!(
        status.is_success(),
        "valid unkeyed signal-with-start must still proceed (2xx): {status} {body}"
    );

    // Malformed unkeyed signal-with-start → still 400.
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/signal-with-start",
        json!({
            "workflow_id": "sws-unkeyed-bad",
            "signal_name": "approve",
            "signal_payload": {}
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "malformed unkeyed signal-with-start must still be validated: {body}"
    );

    // Malformed unkeyed update-with-start → still 400.
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/update-with-start",
        json!({
            "workflow_id": "uws-unkeyed-bad",
            "update_name": "set_priority",
            "update_args": { "priority": "high" },
            "wait_for_stage": "admitted"
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "malformed unkeyed update-with-start must still be validated: {body}"
    );
}

/// AC7: a genuine FRESH keyed update-with-start against a PAUSED prior still
/// returns 409 WorkflowPaused. This confirms the authoritative in-lock path is
/// preserved for non-committed requests: probe MISSES → validation runs (payload
/// valid) → authoritative call → WorkflowPaused.
#[tokio::test]
async fn uws_paused_fresh_keyed_still_409() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let exec_id = seed_execution_in_state(&pool, "iface_wf", "PAUSED").await;
    let wid = exec_id.to_string();

    // Fresh key (never committed) + a schema-valid update arg + AllowDuplicate
    // (default). Probe misses; validation passes; the authoritative resolver
    // rejects an update to a PAUSED prior with WorkflowPaused (409).
    let (status, body) = post_json(
        &app,
        "/workflows/iface_wf/update-with-start",
        json!({
            "workflow_id": wid,
            "update_name": "set_priority",
            "update_args": { "priority": 3 },
            "idempotency_key": "k-uws-paused-fresh",
            "wait_for_stage": "admitted"
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "a fresh keyed update-with-start to a PAUSED prior must still 409: {body}"
    );
    assert_eq!(
        body["error"], "workflow is paused",
        "409 body must carry the paused error: {body}"
    );
}
