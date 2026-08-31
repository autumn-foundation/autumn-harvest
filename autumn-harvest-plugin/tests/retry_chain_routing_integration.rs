//! HTTP routing of interactive/mutating operations across the workflow-level
//! retry chain — issue #843.
//!
//! Workflow-level retry (#523/#842) starts each attempt as a **fresh execution**
//! with a new `exec_id`, linked to its predecessor via `retry_of_exec_id`. A
//! caller holding the original id names the *logical* run, so every mutating
//! and interactive management route must be routed to the **live attempt** —
//! the deepest execution reachable by following `retry_of_exec_id` successors
//! from `FAILED` rows.
//!
//! Scenarios covered (all addressed by the ORIGINAL, sealed-`FAILED` id):
//!   - `POST /workflows/{id}/signal/{name}`   → lands on the live attempt, and
//!     reports where it landed via `routed_execution_id`.
//!   - `POST /workflows/{id}/cancel`          → cancels the live attempt.
//!   - `POST /workflows/{id}/terminate`       → terminates the live attempt.
//!   - `GET/POST /workflows/{id}/query/{name}` → serves the live attempt's state.
//!   - `POST /workflows/{id}/update/{name}`   → admits onto the live attempt.
//!   - `GET  /workflows/{id}`                 → **unchanged**: describe is a
//!     specific-`exec_id` read and still reports the addressed (sealed) row.
//!
//! The chain is seeded directly (a `FAILED` predecessor + a `RUNNING` successor
//! carrying `retry_of_exec_id`), which is exactly the durable shape the worker's
//! atomic retry transaction commits — so these tests exercise the HTTP routing
//! without needing a live worker.

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
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
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// ── Test workflow ──────────────────────────────────────────────────────────

/// Registers a `attempt_marker` query returning whatever the run's input says,
/// then parks on a signal. Replaying the successor's history therefore reports
/// the SUCCESSOR's input, which is what distinguishes a routed query from one
/// served against the sealed predecessor.
fn marker_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let marker = input
            .get("marker")
            .and_then(Value::as_str)
            .unwrap_or("none")
            .to_string();
        ctx.register_query_handler::<Value, String, _>("attempt_marker", move |_req: &Value| {
            Ok(marker.clone())
        });
        // Park forever on a signal that the fixture never delivers, so the
        // replay drive stops at a genuine suspension (the running-query path).
        let _ = ctx.wait_for_signal("never").await;
        Ok(Value::Null)
    })
}

fn marker_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "marker_wf",
        module: "tests",
        handler: marker_workflow,
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

// ── Harness ────────────────────────────────────────────────────────────────

type HarvestApiApp = axum::Router;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

/// Dual-mode setup: prefer an operator-supplied `HARVEST_TEST_DATABASE_URL`
/// (creating a fresh per-test database off it so tests in this binary never
/// share state), else fall back to testcontainers.
async fn setup() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_843_http_{}", uuid::Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect to admin database");
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin)
            .await
            .expect("create per-test database");
        drop(admin);
        let (prefix, _) = admin_url
            .rsplit_once('/')
            .expect("url must carry a database path");
        let url = format!("{prefix}/{db_name}");
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to per-test database");
        SimpleAsyncConnection::batch_execute(&mut conn, autumn_harvest::test_init_sql())
            .await
            .expect("apply migrations");
        drop(conn);
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

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.set_query_timeout(Duration::from_secs(5));
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![marker_info()], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("retry-chain-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn read_response(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

async fn get(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", "alice@ops")
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
                .header("x-harvest-actor", "alice@ops")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
    read_response(response).await
}

// ── Fixture ────────────────────────────────────────────────────────────────

/// Seed one execution row plus its `WorkflowStarted` event.
async fn seed_execution(
    pool: &DbPool,
    state: &str,
    marker: &str,
    retry_of: Option<ExecutionId>,
) -> ExecutionId {
    let mut conn = pool.get().await.expect("pooled conn");
    let exec_id = ExecutionId::new();
    let input = json!({ "marker": marker });
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state, retry_of_exec_id, \
          completed_at) \
         VALUES ($1, 'marker_wf', $2, 0, $3, 'default', $4, $5, \
                 CASE WHEN $4 IN ('FAILED','COMPLETED','CANCELLED') THEN NOW() ELSE NULL END)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Jsonb, _>(input.clone())
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Uuid>, _>(retry_of.map(|e| e.as_uuid()))
    .execute(&mut conn)
    .await
    .expect("seed execution row");

    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("seed WorkflowStarted");
    exec_id
}

/// Seed a two-attempt retry chain: attempt 1 sealed `FAILED`, attempt 2
/// `RUNNING` and linked back via `retry_of_exec_id` — the exact durable shape
/// the worker's atomic retry transaction commits.
async fn seed_chain(pool: &DbPool) -> (ExecutionId, ExecutionId) {
    let first = seed_execution(pool, "FAILED", "attempt-1", None).await;
    let second = seed_execution(pool, "RUNNING", "attempt-2", Some(first)).await;
    (first, second)
}

/// Insert a claimable workflow task row for the execution (so cancel/terminate
/// have something to act on).
async fn seed_workflow_task(pool: &DbPool, exec_id: ExecutionId) {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, attempt, max_attempts, \
          scheduled_at) \
         VALUES (gen_random_uuid(), 'default', 'workflow', $1, '{}'::jsonb, 'PENDING', 1, 1, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("seed workflow task");
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct StateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

async fn execution_state(pool: &DbPool, exec_id: ExecutionId) -> String {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<StateRow>(&mut conn)
        .await
        .expect("load state")
        .state
}

async fn signal_count(pool: &DbPool, exec_id: ExecutionId) -> i64 {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_signals WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<CountRow>(&mut conn)
        .await
        .expect("count signals")
        .n
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn signal_route_lands_on_the_live_retry_attempt() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let (first, second) = seed_chain(&pool).await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{first}/signal/approve"),
        json!({ "ok": true }),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "body={body}");
    assert_eq!(body["signal_delivered"], json!(true));
    assert_eq!(
        body["routed_execution_id"],
        json!(second.to_string()),
        "the ack must report the live attempt the signal landed on: {body}"
    );

    assert_eq!(
        signal_count(&pool, second).await,
        1,
        "the signal must land on the LIVE attempt"
    );
    assert_eq!(
        signal_count(&pool, first).await,
        0,
        "the sealed predecessor must receive nothing"
    );
}

#[tokio::test]
async fn signal_route_omits_routed_id_without_a_retry_chain() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let solo = seed_execution(&pool, "RUNNING", "solo", None).await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{solo}/signal/approve"),
        json!({ "ok": true }),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "body={body}");
    assert_eq!(body["signal_delivered"], json!(true));
    assert!(
        body.get("routed_execution_id").is_none(),
        "an unrouted signal must not carry routed_execution_id: {body}"
    );
    assert_eq!(signal_count(&pool, solo).await, 1);
}

#[tokio::test]
async fn cancel_route_cancels_the_live_retry_attempt() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let (first, second) = seed_chain(&pool).await;
    seed_workflow_task(&pool, second).await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{first}/cancel"),
        json!({ "reason": "operator abort" }),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "body={body}");
    assert_eq!(
        body["execution_id"],
        json!(second.to_string()),
        "cancel must report the live attempt it acted on: {body}"
    );
    assert_eq!(execution_state(&pool, second).await, "CANCELLED");
    assert_eq!(
        execution_state(&pool, first).await,
        "FAILED",
        "the sealed predecessor must be left untouched"
    );
}

#[tokio::test]
async fn terminate_route_terminates_the_live_retry_attempt() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let (first, second) = seed_chain(&pool).await;
    seed_workflow_task(&pool, second).await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{first}/terminate"),
        json!({ "reason": "operator kill" }),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "body={body}");
    assert_eq!(
        body["execution_id"],
        json!(second.to_string()),
        "terminate must report the live attempt it acted on: {body}"
    );
    assert_eq!(execution_state(&pool, second).await, "TERMINATED");
    assert_eq!(execution_state(&pool, first).await, "FAILED");
}

#[tokio::test]
async fn query_routes_read_the_live_retry_attempt() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let (first, _second) = seed_chain(&pool).await;

    // GET (raw value)
    let (status, body) = get(&app, &format!("/workflows/{first}/query/attempt_marker")).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(
        body,
        json!("attempt-2"),
        "the query must read the LIVE attempt's reconstructed state, not the sealed predecessor's"
    );

    // POST ({"result": …})
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{first}/query/attempt_marker"),
        json!({ "args": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["result"], json!("attempt-2"));
}

#[tokio::test]
async fn update_route_admits_onto_the_live_retry_attempt() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let (first, second) = seed_chain(&pool).await;

    // `wait=admitted` returns as soon as the UpdateAdmitted event is durable,
    // so no worker is needed to observe where it landed.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{first}/update/set_priority?wait=admitted"),
        json!({ "input": { "priority": "high" } }),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "body={body}");

    let mut conn = pool.get().await.expect("pooled conn");
    let live = store::load_history(&mut conn, second)
        .await
        .expect("load live history");
    assert!(
        live.events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::UpdateAdmitted { .. })),
        "the update must be admitted onto the LIVE attempt: {:?}",
        live.events
    );
    let sealed = store::load_history(&mut conn, first)
        .await
        .expect("load sealed history");
    assert!(
        !sealed
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::UpdateAdmitted { .. })),
        "the sealed predecessor must not gain an UpdateAdmitted event"
    );
}

#[tokio::test]
async fn pause_and_resume_routes_act_on_the_live_retry_attempt() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let (first, second) = seed_chain(&pool).await;
    seed_workflow_task(&pool, second).await;

    // Pause is the REVERSIBLE containment lever, and it accepts only a
    // RUNNING/PAUSED row — so unrouted it 409s against the sealed FAILED
    // predecessor, leaving an operator only the destructive levers.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{first}/pause"),
        json!({ "reason": "investigating" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(
        body["execution_id"],
        json!(second.to_string()),
        "pause must report the live attempt it paused: {body}"
    );
    assert_eq!(execution_state(&pool, second).await, "PAUSED");

    let (status, body) = post_json(&app, &format!("/workflows/{first}/resume"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(
        body["execution_id"],
        json!(second.to_string()),
        "resume must report the live attempt it resumed: {body}"
    );
    assert_eq!(
        body["newly_resumed"],
        json!(true),
        "an unrouted resume would be an idempotent success no-op while the live \
         attempt stayed parked: {body}"
    );
    assert_eq!(execution_state(&pool, second).await, "RUNNING");
    assert_eq!(
        execution_state(&pool, first).await,
        "FAILED",
        "the sealed predecessor must be left untouched"
    );
}

#[tokio::test]
async fn update_result_route_reads_the_live_retry_attempt() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let (first, second) = seed_chain(&pool).await;

    // Admit against the addressed (sealed) id — routing lands it on the live
    // attempt, and the 202 carries ONLY the update_id.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{first}/update/set_priority?wait=admitted"),
        json!({ "input": { "priority": "high" } }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "body={body}");
    let update_id = body["update_id"]
        .as_str()
        .expect("the admitted ack must carry an update_id")
        .to_string();

    // The paired READ must follow the same chain, or it 404s forever against
    // the addressed predecessor whose history never held the admission.
    let (status, body) = get(
        &app,
        &format!("/workflows/{first}/update/{update_id}/result"),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::NOT_FOUND,
        "the update-result read must route to the live attempt, not 404 against \
         the sealed predecessor: {body}"
    );

    // Sanity: the addressed id genuinely never held the admission, so the read
    // above could only have succeeded by routing.
    let mut conn = pool.get().await.expect("pooled conn");
    let sealed = store::load_history(&mut conn, first)
        .await
        .expect("load sealed history");
    assert!(
        !sealed
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::UpdateAdmitted { .. })),
    );
    let live = store::load_history(&mut conn, second)
        .await
        .expect("load live history");
    assert!(
        live.events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::UpdateAdmitted { .. })),
    );
}

/// Codex review (PR #1200): resolving the update-result read *straight to the
/// live attempt* is not enough. An `update_id` is minted per admission, so it
/// lives on exactly ONE attempt. When an update is admitted (and completed) on
/// attempt N and N then fails and schedules N+1, the durable
/// `UpdateAdmitted`/`UpdateCompleted` pair stays on N while N+1 replays an
/// empty history — so a live-attempt-only resolve would 404 forever and make a
/// durable, already-computed result permanently inaccessible.
#[tokio::test]
async fn update_result_route_finds_a_result_left_on_an_earlier_attempt() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Attempt 1 is live, so the admission lands on it directly (no chain yet).
    let first = seed_execution(&pool, "RUNNING", "attempt-1", None).await;
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{first}/update/set_priority?wait=admitted"),
        json!({ "input": { "priority": "high" } }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "body={body}");
    let update_id_str = body["update_id"]
        .as_str()
        .expect("the admitted ack must carry an update_id")
        .to_string();
    let update_id: autumn_harvest::types::UpdateId =
        update_id_str.parse().expect("parse update id");

    // The handler completes on attempt 1: the result is now durable there.
    {
        let mut conn = pool.get().await.expect("pooled conn");
        let history = store::load_history(&mut conn, first)
            .await
            .expect("load attempt-1 history");
        store::append_events(
            &mut conn,
            first,
            &[WorkflowEvent::UpdateCompleted {
                update_id,
                output: json!({ "applied": "high" }),
            }],
            history.next_event_id,
        )
        .await
        .expect("append UpdateCompleted");
    }

    // ...and only THEN does attempt 1 fail and schedule attempt 2.
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
             SET state = 'FAILED', completed_at = NOW() WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(first.as_uuid())
        .execute(&mut conn)
        .await
        .expect("seal attempt 1 FAILED");
    }
    let second = seed_execution(&pool, "RUNNING", "attempt-2", Some(first)).await;

    // The read must locate the attempt that actually carries the admission.
    let (status, body) = get(
        &app,
        &format!("/workflows/{first}/update/{update_id_str}/result"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an update completed on an earlier attempt must stay readable after the \
         chain advances, not 404 against the empty live attempt: {body}"
    );
    assert_eq!(body["output"], json!({ "applied": "high" }), "body={body}");

    // Sanity: the live attempt genuinely never held the admission, so the read
    // above could only have succeeded by searching the chain.
    let mut conn = pool.get().await.expect("pooled conn");
    let live = store::load_history(&mut conn, second)
        .await
        .expect("load live history");
    assert!(
        !live
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::UpdateAdmitted { .. })),
        "the live attempt must have an empty (retry-fresh) history"
    );
}

#[tokio::test]
async fn describe_route_still_reads_the_addressed_execution() {
    let (url, _c) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let (first, _second) = seed_chain(&pool).await;

    let (status, body) = get(&app, &format!("/workflows/{first}")).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(
        body["execution"]["id"],
        json!(first.to_string()),
        "describe is a specific-exec_id read and must NOT follow the retry chain: {body}"
    );
    assert_eq!(body["execution"]["state"], json!("FAILED"));
}
