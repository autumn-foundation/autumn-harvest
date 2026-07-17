//! Integration tests for the business-id ("latest run") management API
//! variants (issue #805): `/workflows/by-id/{workflow_name}/{workflow_id}/...`.
//!
//! These delegate to the existing `exec_id` handlers after resolving
//! `(workflow_name, workflow_id) → ExecutionId`, so they exercise the shared
//! resolver, the resolution rule (AC2 — active-first, else most-recent
//! terminal, else 404), the required-name rule, the always-present
//! `execution_id` (header + body, AC4), and the continue-as-new correctness
//! (the success metric — resolve to the live successor, not the sealed
//! predecessor).
//!
//! The pure ranking math is unit-tested in `autumn-harvest/src/execution.rs`
//! (`select_resolved_run`); these tests verify the HTTP wiring + the DB
//! resolution + the delegation.
//!
//! Docker/testcontainers is not available in every sandbox, so these tests are
//! compile-checked (`cargo test --no-run`) there and run against a real
//! Postgres in CI (and against a local Postgres 16 when available).

#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{harvest_signals, harvest_workflow_executions};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    StartWorkflowParams, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

// Paved-path migration bundle (issue #604): the whole `migrations/` directory,
// so the schema can never drift from the hand-rolled list.
fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

/// Dual-mode: use a pre-migrated Postgres from `HARVEST_TEST_DATABASE_URL` when
/// set (no Docker required — each test uses a distinct `(name, workflow_id)` so
/// rows never collide across a shared DB), else boot a fresh testcontainers
/// Postgres. Mirrors `legal_hold_integration.rs`.
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("failed to build test pool")
}

/// Build the router. `admin_granted = true` uses the admin-auth boundary so
/// admin-gated routes (cancel/pause/resume) pass; `false` leaves the default
/// (admin enforced, no session) so those routes 401.
fn build_app(pool: &DbPool, admin_granted: bool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    if admin_granted {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        // Register `progress_wf` (a query-handling workflow) so the by-id query
        // routes can resolve a registered handler against seeded history. The
        // other tests seed unregistered workflow names (describe/signal/cancel
        // never need registration), so this addition is harmless to them.
        Arc::new(HandlerRegistry::new(vec![progress_info()], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("by-id-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Seed one execution row for `(name, workflow_id)` in the requested state, at
/// the requested `started_at` minute offset. Returns the minted `ExecutionId`.
async fn seed(
    conn: &mut AsyncPgConnection,
    name: &str,
    workflow_id: &str,
    state: &str,
    started_min: i64,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: name,
            workflow_id,
            exec_id,
            input: json!({"n": 1}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
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
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("seed workflow");

    let started_at: DateTime<Utc> = Utc::now() + Duration::minutes(started_min);
    let completed_at = if autumn_harvest::erase::is_terminal_state(state) {
        Some(started_at + Duration::minutes(1))
    } else {
        None
    };
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq(state),
            harvest_workflow_executions::started_at.eq(started_at),
            harvest_workflow_executions::completed_at.eq(completed_at),
        ))
        .execute(conn)
        .await
        .expect("update seed row");
    exec_id
}

/// A workflow that processes each item from its input via an activity,
/// incrementing an internal counter, and registers a `progress` query reporting
/// that counter. Mirrors `query_integration.rs::progress_workflow`; the query
/// resolves the reconstructed count on replay against seeded history.
fn progress_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let processed = Arc::new(std::sync::Mutex::new(0u64));
        let state = processed.clone();
        ctx.register_query_handler::<Value, u64, _>("progress", move |_req: &Value| {
            Ok(*state.lock().expect("counter lock"))
        });

        let items = input
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for item in &items {
            ctx.execute_activity_raw("process_item", item.clone(), "default")
                .await
                .map_err(|e| e.to_string())?;
            *processed.lock().expect("counter lock") += 1;
        }
        Ok(json!({ "processed": *processed.lock().expect("counter lock") }))
    })
}

fn progress_info() -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name: "progress_wf",
        module: "tests",
        handler: progress_workflow,
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

/// Seed a queryable `progress_wf` run for `(name, workflow_id)`: a raw execution
/// row plus a `WorkflowStarted` + one scheduled/completed activity pair per
/// item, so the `progress` query reconstructs a count of `items.len()`. Uses a
/// raw insert (not `start_or_load_workflow_execution`) so the seeded history is
/// exactly what the query drive replays against.
async fn seed_query_run(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    state: &str,
    items: &[&str],
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = json!({ "items": items });
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, 'progress_wf', $2, 0, $3, 'default', $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .bind::<diesel::sql_types::Jsonb, _>(input.clone())
    .bind::<diesel::sql_types::Text, _>(state)
    .execute(conn)
    .await
    .expect("seed query execution");

    let mut events = vec![WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    for item in items {
        let id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "process_item".into(),
            input: json!(item),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        });
    }
    store::append_events(conn, exec_id, &events, 0)
        .await
        .expect("seed query history");
    exec_id
}

/// Raw-insert a *terminal* run for `(name, workflow_id)` at an explicit
/// `started_at` offset. Unlike [`seed`], this does not go through
/// `start_or_load_workflow_execution` — whose standalone semantics *load* an
/// existing terminal run rather than mint a fresh one — so it can create
/// multiple distinct terminal rows sharing one `(workflow_name, workflow_id)`
/// (the partial unique index only covers *active* runs), which is exactly the
/// "most-recent terminal wins" scenario the resolver's no-active branch handles.
async fn seed_terminal_raw(
    conn: &mut AsyncPgConnection,
    name: &str,
    workflow_id: &str,
    state: &str,
    started_min: i64,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let started_at: DateTime<Utc> = Utc::now() + Duration::minutes(started_min);
    let completed_at = started_at + Duration::minutes(1);
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state, started_at, completed_at) \
         VALUES ($1, $2, $3, 0, $4, 'default', $5, $6, $7)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(name)
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .bind::<diesel::sql_types::Jsonb, _>(json!({"n": 1}))
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Timestamptz, _>(started_at)
    .bind::<diesel::sql_types::Timestamptz, _>(completed_at)
    .execute(conn)
    .await
    .expect("seed terminal row");
    exec_id
}

async fn load_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .expect("load state")
}

async fn count_signals(conn: &mut AsyncPgConnection, exec_id: ExecutionId, name: &str) -> i64 {
    harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_signals::signal_name.eq(name))
        .count()
        .get_result::<i64>(conn)
        .await
        .expect("count signals")
}

struct Resp {
    status: StatusCode,
    exec_id_header: Option<String>,
    body: Value,
}

async fn send(app: &HarvestApiApp, req: Request<Body>) -> Resp {
    let response = app.clone().oneshot(req).await.expect("request");
    let status = response.status();
    let exec_id_header = response
        .headers()
        .get("x-harvest-execution-id")
        .and_then(|v| v.to_str().ok())
        .map(ToString::to_string);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    Resp {
        status,
        exec_id_header,
        body,
    }
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn post_json(uri: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

// ── AC1 + AC4: describe by business id returns the resolved execution_id ──────

#[tokio::test]
async fn describe_by_id_returns_resolved_execution_id() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    let exec_id = seed(&mut conn, "order_flow", "order-1", "RUNNING", 0).await;
    drop(conn);

    let app = build_app(&pool, true);
    let resp = send(&app, get("/workflows/by-id/order_flow/order-1")).await;

    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);
    // AC4: header carries the resolved exec id on every response.
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str())
    );
    // Body (WorkflowDetailsResponse) carries it nested under `execution.id`.
    assert_eq!(
        resp.body["execution"]["id"].as_str(),
        Some(exec_id.to_string().as_str())
    );
}

// ── AC5: unknown (name, id) → 404, never 500 ─────────────────────────────────

#[tokio::test]
async fn unknown_business_id_is_404_not_500() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, true);

    let resp = send(&app, get("/workflows/by-id/order_flow/does-not-exist")).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND, "body: {}", resp.body);
}

// ── Backward compat: malformed exec-id on the OLD route still 400s ───────────

#[tokio::test]
async fn malformed_exec_id_on_legacy_route_still_400s() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, true);

    let resp = send(&app, get("/workflows/not-a-uuid")).await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST, "body: {}", resp.body);
}

// ── exec-id routes unchanged (spot check) ────────────────────────────────────

#[tokio::test]
async fn legacy_exec_id_describe_still_works() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    let exec_id = seed(&mut conn, "order_flow", "order-9", "RUNNING", 0).await;
    drop(conn);

    let app = build_app(&pool, true);
    let resp = send(&app, get(&format!("/workflows/{exec_id}"))).await;
    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);
    assert_eq!(
        resp.body["execution"]["id"].as_str(),
        Some(exec_id.to_string().as_str())
    );
}

// ── signal by business id lands + surfaces execution_id ──────────────────────

#[tokio::test]
async fn signal_by_id_lands_and_surfaces_execution_id() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    let exec_id = seed(&mut conn, "order_flow", "order-sig", "RUNNING", 0).await;
    drop(conn);

    let app = build_app(&pool, true);
    let resp = send(
        &app,
        post_json(
            "/workflows/by-id/order_flow/order-sig/signal/approve",
            &json!({"approved_by": "alice"}),
        ),
    )
    .await;

    assert_eq!(resp.status, StatusCode::ACCEPTED, "body: {}", resp.body);
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str())
    );
    // AC4: signal body re-wrapped with execution_id.
    assert_eq!(
        resp.body["execution_id"].as_str(),
        Some(exec_id.to_string().as_str())
    );
    // The re-wrapped body carries both `ok` and `signal_delivered`.
    assert_eq!(resp.body["ok"], json!(true));
    assert_eq!(resp.body["signal_delivered"], json!(true));

    // The signal actually landed.
    let mut conn = pool.get().await.expect("conn");
    assert_eq!(count_signals(&mut conn, exec_id, "approve").await, 1);
}

// ── cancel by business id (admin) + 401 without auth ─────────────────────────

#[tokio::test]
async fn cancel_by_id_requires_admin_then_cancels() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    let exec_id = seed(&mut conn, "order_flow", "order-cancel", "RUNNING", 0).await;
    drop(conn);

    // Unauthenticated (admin enforced) → 401.
    let unauth_app = build_app(&pool, false);
    let resp = send(
        &unauth_app,
        post_json(
            "/workflows/by-id/order_flow/order-cancel/cancel",
            &json!({"reason": "dup"}),
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "body: {}", resp.body);

    // Admin granted → cancels.
    let app = build_app(&pool, true);
    let resp = send(
        &app,
        post_json(
            "/workflows/by-id/order_flow/order-cancel/cancel",
            &json!({"reason": "dup"}),
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::ACCEPTED, "body: {}", resp.body);
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str())
    );
    assert_eq!(
        resp.body["execution_id"].as_str(),
        Some(exec_id.to_string().as_str())
    );

    let mut conn = pool.get().await.expect("conn");
    assert_eq!(load_state(&mut conn, exec_id).await, "CANCELLED");
}

// ── Continue-as-new correctness (the success metric) ─────────────────────────
//
// A cached exec_id would target the sealed predecessor; business-id resolution
// must reach the RUNNING successor sharing the same (workflow_name,workflow_id).

#[tokio::test]
async fn resolves_to_running_successor_after_continue_as_new() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");

    // Predecessor: seeded RUNNING, then sealed CONTINUED_AS_NEW (older).
    let predecessor = seed(&mut conn, "order_flow", "order-can", "RUNNING", 0).await;
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("CONTINUED_AS_NEW"),
            harvest_workflow_executions::started_at.eq(Utc::now() - Duration::minutes(10)),
        ))
        .execute(&mut conn)
        .await
        .expect("seal predecessor");

    // Successor: same (name,id), starts fresh (predecessor is sealed), RUNNING, newer.
    let successor = seed(&mut conn, "order_flow", "order-can", "RUNNING", 5).await;
    assert_ne!(
        predecessor, successor,
        "successor must be a distinct exec_id"
    );
    drop(conn);

    let app = build_app(&pool, true);
    let resp = send(&app, get("/workflows/by-id/order_flow/order-can")).await;
    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(successor.to_string().as_str()),
        "must resolve the live successor, not the sealed predecessor"
    );
    assert_eq!(
        resp.body["execution"]["id"].as_str(),
        Some(successor.to_string().as_str())
    );
    assert_eq!(resp.body["execution"]["state"], json!("RUNNING"));
}

// ── All-terminal case: only a COMPLETED run → resolves to it (200) ───────────

#[tokio::test]
async fn resolves_to_most_recent_terminal_when_no_active() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    let exec_id = seed(&mut conn, "order_flow", "order-done", "COMPLETED", 0).await;
    drop(conn);

    let app = build_app(&pool, true);
    let resp = send(&app, get("/workflows/by-id/order_flow/order-done")).await;
    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str())
    );
    assert_eq!(resp.body["execution"]["state"], json!("COMPLETED"));
}

// ── FIX 5: query by business id end-to-end (GET + POST) ──────────────────────
//
// The one genuinely-bespoke AC4 body path: the by-id query handlers re-wrap the
// (opaque) exec-id query response as `{ "execution_id": …, "result": <value> }`.
// A `progress_wf` run seeded with two completed activities → the `progress`
// query reconstructs a count of 2 on replay.

#[tokio::test]
async fn query_by_id_get_and_post_rewrap_execution_id_and_result() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    let exec_id = seed_query_run(&mut conn, "order-query", "RUNNING", &["a", "b"]).await;
    drop(conn);

    let app = build_app(&pool, true);

    // GET → { execution_id, result } + header.
    let resp = send(
        &app,
        get("/workflows/by-id/progress_wf/order-query/query/progress"),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK, "GET query body: {}", resp.body);
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str())
    );
    assert_eq!(
        resp.body["execution_id"].as_str(),
        Some(exec_id.to_string().as_str())
    );
    assert_eq!(resp.body["result"], json!(2));

    // POST (typed args) → { execution_id, result } + header.
    let resp = send(
        &app,
        post_json(
            "/workflows/by-id/progress_wf/order-query/query/progress",
            &json!({ "args": null }),
        ),
    )
    .await;
    assert_eq!(
        resp.status,
        StatusCode::OK,
        "POST query body: {}",
        resp.body
    );
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str())
    );
    assert_eq!(
        resp.body["execution_id"].as_str(),
        Some(exec_id.to_string().as_str())
    );
    assert_eq!(resp.body["result"], json!(2));
}

// ── FIX 6: pause/resume admin parity + no existence oracle ───────────────────
//
// pause/resume mirror cancel's admin gate. An unauthenticated mutating by-id
// call against a NONEXISTENT (name, id) must 401 (admin runs at the route layer,
// BEFORE resolution) — never a 404 that would leak whether the run exists.

#[tokio::test]
async fn pause_resume_require_admin_and_do_not_leak_existence() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    let exec_id = seed(&mut conn, "order_flow", "order-pr", "RUNNING", 0).await;
    drop(conn);

    // Unauthenticated (admin enforced) against an EXISTING run → 401 for all three.
    let unauth = build_app(&pool, false);
    for (verb_uri, body) in [
        ("/workflows/by-id/order_flow/order-pr/pause", json!({})),
        ("/workflows/by-id/order_flow/order-pr/resume", json!({})),
        (
            "/workflows/by-id/order_flow/order-pr/cancel",
            json!({"reason": "x"}),
        ),
    ] {
        let resp = send(&unauth, post_json(verb_uri, &body)).await;
        assert_eq!(
            resp.status,
            StatusCode::UNAUTHORIZED,
            "{verb_uri} unauth (existing) must 401: {}",
            resp.body
        );
    }

    // Unauthenticated against a NONEXISTENT (name, id) → still 401, NOT 404 —
    // proving admin runs before resolution (no existence oracle).
    for (verb_uri, body) in [
        ("/workflows/by-id/order_flow/nope/pause", json!({})),
        ("/workflows/by-id/order_flow/nope/resume", json!({})),
        (
            "/workflows/by-id/order_flow/nope/cancel",
            json!({"reason": "x"}),
        ),
    ] {
        let resp = send(&unauth, post_json(verb_uri, &body)).await;
        assert_eq!(
            resp.status,
            StatusCode::UNAUTHORIZED,
            "{verb_uri} unauth (nonexistent) must 401 not 404: {}",
            resp.body
        );
    }

    // Admin granted → pause then resume the real run, header carries exec id.
    // (pause/resume both return 200 OK — unlike cancel's 202.)
    let app = build_app(&pool, true);
    let resp = send(
        &app,
        post_json("/workflows/by-id/order_flow/order-pr/pause", &json!({})),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK, "pause: {}", resp.body);
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str())
    );
    assert_eq!(resp.body["state"], json!("PAUSED"));

    let resp = send(
        &app,
        post_json("/workflows/by-id/order_flow/order-pr/resume", &json!({})),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK, "resume: {}", resp.body);
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str())
    );
}

// ── FIX 7: /result by-id carries the header even on the still-running (204) path
//
// A RUNNING run has no terminal output yet, so /result returns 204 (empty body).
// The X-Harvest-Execution-Id header is the raison d'être of the header design —
// it must still be present, carrying the resolved exec id.

#[tokio::test]
async fn result_by_id_running_returns_header_on_empty_body() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");
    let exec_id = seed(&mut conn, "order_flow", "order-result", "RUNNING", 0).await;
    drop(conn);

    let app = build_app(&pool, true);
    let resp = send(&app, get("/workflows/by-id/order_flow/order-result/result")).await;

    // Still running → 204 No Content (the delegate's still-running status).
    assert_eq!(
        resp.status,
        StatusCode::NO_CONTENT,
        "still-running /result must be 204: {}",
        resp.body
    );
    // The resolved exec id is carried by the header even on the empty body.
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(exec_id.to_string().as_str()),
        "header must carry the resolved exec id on the 204 path"
    );
}

// ── FIX 8: multi-terminal ordering end-to-end (newest terminal wins) ─────────
//
// Several coexisting TERMINAL runs for the same (name, id) with distinct
// started_at and no active run → resolve to the NEWEST terminal (the DB
// order(started_at.desc()) no-active branch, end-to-end). The realistic shape
// is a continue-as-new chain that finally completed: only CONTINUED_AS_NEW /
// TERMINATED rows are released from the active-run partial unique index, so a
// (name, id) with >1 terminal row is a CAN chain plus its final terminal. Here
// the newest by started_at is the COMPLETED final run.

#[tokio::test]
async fn resolves_to_newest_terminal_across_multiple_terminals() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.expect("conn");

    // Raw inserts (not seed/start_or_load, which loads an existing terminal
    // rather than minting a new one) so distinct rows coexist. Two sealed
    // CONTINUED_AS_NEW predecessors (released from the active index) + one
    // COMPLETED final run, inserted out of started_at order to prove the DB
    // orders by started_at (not insertion order).
    let _p1 = seed_terminal_raw(
        &mut conn,
        "order_flow",
        "order-multi",
        "CONTINUED_AS_NEW",
        5,
    )
    .await;
    let newest = seed_terminal_raw(&mut conn, "order_flow", "order-multi", "COMPLETED", 10).await;
    let _p0 = seed_terminal_raw(
        &mut conn,
        "order_flow",
        "order-multi",
        "CONTINUED_AS_NEW",
        0,
    )
    .await;
    drop(conn);

    let app = build_app(&pool, true);
    let resp = send(&app, get("/workflows/by-id/order_flow/order-multi")).await;
    assert_eq!(resp.status, StatusCode::OK, "body: {}", resp.body);
    assert_eq!(
        resp.exec_id_header.as_deref(),
        Some(newest.to_string().as_str()),
        "must resolve the most-recently-started terminal"
    );
    assert_eq!(resp.body["execution"]["state"], json!("COMPLETED"));
}

// ── FIX 9(a): a bare workflow_id (name-only, 3-segment path) is rejected 400
//
// The by-id routes require BOTH segments; a name-only `/workflows/by-id/{name}`
// path (no workflow_id) is an input-validation error and is rejected with a
// literal 400 (not a structural 404), matching AC3 verbatim ("a bare
// workflow_id without a name is rejected 400"). Guard routes are registered for
// both GET and POST so the rejection is uniform.

#[tokio::test]
async fn name_only_by_id_path_is_rejected_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, true);

    let resp = send(&app, get("/workflows/by-id/order_flow")).await;
    assert_eq!(
        resp.status,
        StatusCode::BAD_REQUEST,
        "name-only by-id GET must be rejected 400: {}",
        resp.body
    );
    assert_eq!(resp.body["error"], json!("workflow_id is required"));
    assert!(
        resp.body["detail"]
            .as_str()
            .is_some_and(|d| d.contains("/workflows/by-id/{workflow_name}/{workflow_id}")),
        "detail must point at the two-segment business-id form: {}",
        resp.body
    );

    let resp_post = send(&app, post_json("/workflows/by-id/order_flow", &json!({}))).await;
    assert_eq!(
        resp_post.status,
        StatusCode::BAD_REQUEST,
        "name-only by-id POST must be rejected 400: {}",
        resp_post.body
    );
    assert_eq!(resp_post.body["error"], json!("workflow_id is required"));
}
