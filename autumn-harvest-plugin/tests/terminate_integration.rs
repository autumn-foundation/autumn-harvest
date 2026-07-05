//! Integration tests for the single-execution force-terminate endpoint (issue #504).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise
//! `POST /workflows/{id}/terminate` end-to-end.
//!
//! Scenarios covered:
//!   (a) Terminating a RUNNING execution seals it `TERMINATED`, returns 202 with
//!       `newly_terminated: true`, records the reason, and surfaces as a terminal
//!       failure via `GET /workflows/{id}/result` (AC #1, #4).
//!   (b) Terminate affects ONLY the addressed id — a concurrent execution of the
//!       same workflow type/queue is untouched (AC #3).
//!   (c) Idempotent no-op against an already-terminal execution: `newly_terminated:
//!       false`, state unchanged, no duplicate terminal event appended (AC #7).
//!   (d) Unknown id → 404.
//!   (e) An audit row is written via the `OP_WORKFLOW_TERMINATE` pattern (AC #5).

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{harvest_audit_log, harvest_events, harvest_workflow_executions};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
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
    // issue #523: workflow-level retry policy columns.
    include_str!("../../autumn-harvest/migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../../autumn-harvest/migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../autumn-harvest/migrations/20260704000000_harvest_workflow_nd_block/up.sql")
);

type HarvestApiApp = axum::Router;

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

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    // The admin-gated `POST /workflows/{id}/terminate` route is guarded by
    // `require_harvest_admin`, which checks session state (not the request
    // header). Open the boundary so the integration suite can reach the handler.
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("terminate-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
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
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
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

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
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

/// Seed a RUNNING workflow execution.
async fn seed_running(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "wedged",
            workflow_id,
            exec_id,
            input: json!({ "n": 1 }),
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
    )
    .await
    .expect("seed workflow");
    exec_id
}

async fn state_of(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(conn)
        .await
        .unwrap()
}

async fn event_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .count()
        .get_result(conn)
        .await
        .unwrap()
}

/// (a) Terminating a RUNNING execution seals it TERMINATED and surfaces as a
/// terminal failure carrying the reason.
#[tokio::test]
async fn terminate_running_seals_terminated() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "wedged-1").await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/terminate"),
        json!({ "reason": "stuck on a signal that will never arrive" }),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
    assert_eq!(body["ok"], true);
    assert_eq!(body["state"], "TERMINATED");
    assert_eq!(body["newly_terminated"], true);
    assert_eq!(body["reason"], "stuck on a signal that will never arrive");

    assert_eq!(state_of(&mut conn, exec_id).await, "TERMINATED");

    // A result-awaiting caller observes a terminal failure carrying the reason,
    // distinguishable from a normal FAILED.
    let (rstatus, rbody) = get_json(&app, &format!("/workflows/{exec_id}/result")).await;
    assert_eq!(rstatus, StatusCode::OK, "result body: {rbody}");
    assert_eq!(rbody["state"], "terminated");
    assert!(
        rbody["error"]
            .as_str()
            .unwrap_or_default()
            .contains("never arrive"),
        "result must carry the termination reason: {rbody}"
    );
}

/// (b) Terminate addresses ONLY the named id — a concurrent execution of the
/// same workflow type/queue is untouched (AC #3, zero collateral terminations).
#[tokio::test]
async fn terminate_only_affects_target() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let target = seed_running(&mut conn, "wedged-target").await;
    let sibling = seed_running(&mut conn, "wedged-sibling").await;

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{target}/terminate"),
        json!({ "reason": "kill the wedged one" }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    assert_eq!(state_of(&mut conn, target).await, "TERMINATED");
    assert_eq!(
        state_of(&mut conn, sibling).await,
        "RUNNING",
        "the sibling execution must be untouched by a single-execution terminate"
    );
}

/// (c) Idempotent no-op against an already-terminal execution: no state change,
/// no duplicate terminal transition appended (AC #7).
#[tokio::test]
async fn terminate_idempotent_on_terminal() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "already-done").await;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    let events_before = event_count(&mut conn, exec_id).await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/terminate"),
        json!({ "reason": "too late" }),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
    assert_eq!(
        body["newly_terminated"], false,
        "terminating a terminal run must be a non-mutating no-op"
    );
    assert_eq!(body["state"], "COMPLETED", "state must be unchanged");

    assert_eq!(
        state_of(&mut conn, exec_id).await,
        "COMPLETED",
        "the completed run must remain COMPLETED"
    );
    assert_eq!(
        event_count(&mut conn, exec_id).await,
        events_before,
        "no duplicate terminal event may be appended"
    );
}

/// (d) Unknown execution id → 404.
#[tokio::test]
async fn terminate_unknown_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{unknown}/terminate"),
        json!({ "reason": "ghost" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// (e) An audit row is written via the `OP_WORKFLOW_TERMINATE` pattern (AC #5).
#[tokio::test]
async fn terminate_writes_audit_record() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "audit-wf").await;

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/terminate"),
        json!({ "reason": "operator kill" }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let audit_count: i64 = harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq("workflow.terminate"))
        .filter(harvest_audit_log::target_id.eq(exec_id.to_string()))
        .filter(harvest_audit_log::status.eq("succeeded"))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(audit_count, 1, "exactly one succeeded audit row must exist");
}

/// Seed a child execution. `detached = false` is an **awaited** child
/// (`parent_id` set, `parent_close_policy` NULL); `detached = true` sets a
/// `parent_close_policy` so the row is a detached child.
async fn seed_child(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    parent: ExecutionId,
    detached: bool,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "child-wf",
            workflow_id,
            exec_id,
            input: json!({ "n": 1 }),
            parent_id: Some(parent.as_uuid()),
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
    )
    .await
    .expect("seed child");
    if detached {
        diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
            .set(harvest_workflow_executions::parent_close_policy.eq(Some("abandon")))
            .execute(conn)
            .await
            .unwrap();
    }
    exec_id
}

/// Collect the `ChildWorkflowFailed` events recorded on an execution's history.
async fn child_failed_events(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<Value> {
    let rows: Vec<Value> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::event_data)
        .load(conn)
        .await
        .unwrap();
    rows.into_iter()
        .filter(|v| v.get("type").and_then(Value::as_str) == Some("ChildWorkflowFailed"))
        .collect()
}

async fn post_empty_json(app: &HarvestApiApp, uri: &str) -> StatusCode {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("POST request");
    response.status()
}

/// (P1, #787 review) Terminating an **awaited** child wakes the parent: a
/// `ChildWorkflowFailed` event carrying the termination reason is appended to
/// the parent's history, so a parent parked on the child's await is not stuck.
#[tokio::test]
async fn terminate_awaited_child_notifies_parent() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let parent = seed_running(&mut conn, "parent-await").await;
    let child = seed_child(&mut conn, "child-await", parent, false).await;

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{child}/terminate"),
        json!({ "reason": "stuck child" }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let parent_events = child_failed_events(&mut conn, parent).await;
    assert_eq!(
        parent_events.len(),
        1,
        "parent must receive exactly one ChildWorkflowFailed, got {parent_events:?}"
    );
    let data = &parent_events[0]["data"];
    assert_eq!(data["child_id"].as_str(), Some(child.to_string().as_str()));
    assert!(
        data["error"]
            .as_str()
            .unwrap_or_default()
            .contains("terminated"),
        "child-failed error must carry the terminate reason: {data:?}"
    );
}

/// (P1, #787 review) A **detached** child (`parent_close_policy` set) is the
/// parent-close cascade's responsibility, not this path — terminating it must
/// NOT append a `ChildWorkflowFailed` to the parent.
#[tokio::test]
async fn terminate_detached_child_does_not_notify_parent() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let parent = seed_running(&mut conn, "parent-detached").await;
    let child = seed_child(&mut conn, "child-detached", parent, true).await;

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{child}/terminate"),
        json!({ "reason": "kill detached" }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    assert!(
        child_failed_events(&mut conn, parent).await.is_empty(),
        "a detached child terminate must not notify the parent"
    );
}

/// (P2, #787 review) A body-less terminate with `Content-Type: application/json`
/// and zero bytes must still terminate (defaulted reason), not 422.
#[tokio::test]
async fn terminate_with_empty_json_body_returns_202() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "empty-body").await;

    let status = post_empty_json(&app, &format!("/workflows/{exec_id}/terminate")).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "empty JSON body must be accepted"
    );
    assert_eq!(state_of(&mut conn, exec_id).await, "TERMINATED");
}

/// (P2, #787 review) An awaited child can outlive its parent (e.g. the parent
/// hit its execution timeout). Terminating the child must NOT append a
/// command-consumable `ChildWorkflowFailed` to the already-terminal parent —
/// that would add replay-visible history past the parent's closure.
#[tokio::test]
async fn terminate_awaited_child_skips_wake_when_parent_terminal() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let parent = seed_running(&mut conn, "parent-timed-out").await;
    let child = seed_child(&mut conn, "child-outlives-parent", parent, false).await;

    // Parent reaches a terminal state while the awaited child is still running.
    diesel::update(harvest_workflow_executions::table.find(parent.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("TIMED_OUT"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();
    let parent_events_before = event_count(&mut conn, parent).await;

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{child}/terminate"),
        json!({ "reason": "kill orphaned child" }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // The child terminates, but the terminal parent is left untouched.
    assert_eq!(state_of(&mut conn, child).await, "TERMINATED");
    assert!(
        child_failed_events(&mut conn, parent).await.is_empty(),
        "a terminal parent must not receive a ChildWorkflowFailed wake"
    );
    assert_eq!(
        event_count(&mut conn, parent).await,
        parent_events_before,
        "no event may be appended to an already-terminal parent's history"
    );
}
