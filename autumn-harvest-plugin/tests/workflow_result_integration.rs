//! Integration tests for `GET /api/harvest/workflows/{id}/result` (issue #527).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise:
//!   (a) 404 for an unknown execution id.
//!   (b) 200 with correct terminal state for a COMPLETED execution.
//!   (c) 204 No Content + Retry-After for a RUNNING execution (no wait).
//!   (d) `ContinuedAsNew` → follows the successor chain to the final output (AC5).

#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
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
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("result-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
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
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .map(ToOwned::to_owned);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let mut json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    if let Some(ra) = retry_after {
        json["__retry_after"] = Value::String(ra);
    }
    (status, json)
}

async fn seed_running(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "result-wf",
            workflow_id,
            exec_id,
            input: json!({"n": 1}),
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

async fn mark_continued_as_new(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    new_id: ExecutionId,
) {
    // Append the ContinuedAsNew event to history.
    let history = store::load_history(conn, exec_id).await.unwrap();
    let events = vec![WorkflowEvent::WorkflowContinuedAsNew {
        new_exec_id: new_id,
        input: json!({"n": 2}),
    }];
    store::append_events(conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append ContinuedAsNew event");

    // Seal the predecessor row.
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("CONTINUED_AS_NEW"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .unwrap();

    // Verify the event was appended.
    let count: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .count()
        .get_result(conn)
        .await
        .unwrap();
    // >= 2: WorkflowStarted is always present; ContinuedAsNew must be the second row.
    assert!(count >= 2, "ContinuedAsNew event must have been appended");
}

/// (a) Unknown execution id → 404.
#[tokio::test]
async fn result_unknown_id_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, _body) = get_json(&app, &format!("/workflows/{unknown}/result")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// (b) A COMPLETED execution → 200 OK with state=completed and its output.
#[tokio::test]
async fn result_completed_returns_200_with_output() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "completed-wf").await;
    mark_completed(&mut conn, exec_id, json!({"answer": 42})).await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/result")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["state"], "completed");
    assert_eq!(body["output"]["answer"], 42);
}

/// (c) A RUNNING execution with no wait → 204 No Content + Retry-After.
#[tokio::test]
async fn result_running_no_wait_returns_204() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "still-running").await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/result")).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "body: {body}");
    assert_eq!(
        body["__retry_after"].as_str(),
        Some("1"),
        "Retry-After header must be present"
    );
}

/// (d) AC5: `ContinuedAsNew` predecessor → follows the chain to the successor's
/// COMPLETED output, not the `ContinuedAsNew` sentinel.
#[tokio::test]
async fn result_follows_continue_as_new_to_final_output() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // Seed the predecessor as CONTINUED_AS_NEW → successor COMPLETED.
    let predecessor_id = seed_running(&mut conn, "can-predecessor").await;
    let successor_id = seed_running(&mut conn, "can-successor").await;

    mark_continued_as_new(&mut conn, predecessor_id, successor_id).await;
    mark_completed(&mut conn, successor_id, json!({"final": "result"})).await;

    // Querying the predecessor must resolve through the chain to the successor's output.
    let (status, body) = get_json(&app, &format!("/workflows/{predecessor_id}/result")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        body["state"], "completed",
        "must return the successor's completed state, not continued_as_new"
    );
    assert_eq!(
        body["output"]["final"], "result",
        "must return the successor's output"
    );
}

#[tokio::test]
async fn test_get_update_result_orphaned() {
    use autumn_harvest::types::UpdateId;
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "orphaned-wf").await;
    let update_id = UpdateId::new();

    // 1. Append UpdateAdmitted to history
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    let events = vec![WorkflowEvent::UpdateAdmitted {
        update_id,
        name: "test_update".to_string(),
        input: Value::Null,
        timestamp: Utc::now(),
    }];
    store::append_events(&mut conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append admitted update event");

    // 2. Querying result while running should return 202 Accepted
    let (status, body) = get_json(
        &app,
        &format!("/workflows/{exec_id}/update/{update_id}/result"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["update_id"], update_id.to_string());

    // 3. Mark the workflow COMPLETED (unresolved update becomes orphaned)
    mark_completed(&mut conn, exec_id, Value::Null).await;
    // Append WorkflowCompleted event to history to make get_terminal_workflow_state find it
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    let completed_event = vec![WorkflowEvent::WorkflowCompleted {
        output: Value::Null,
    }];
    store::append_events(&mut conn, exec_id, &completed_event, history.next_event_id)
        .await
        .expect("append completed event");

    // 4. Querying result now should return 409 Conflict with update_orphaned
    let (status, body) = get_json(
        &app,
        &format!("/workflows/{exec_id}/update/{update_id}/result"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["update_id"], update_id.to_string());
    assert_eq!(body["error_type"], "update_orphaned");
    assert_eq!(body["workflow_state"], "COMPLETED");
}

#[tokio::test]
async fn test_poll_update_result_orphaned() {
    use autumn_harvest::types::UpdateId;
    use autumn_harvest_plugin::api::poll_update_result;
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let harvest_pool = HarvestDbPool::from(pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "orphaned-poll-wf").await;
    let update_id = UpdateId::new();

    // Append UpdateAdmitted to history
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    let events = vec![WorkflowEvent::UpdateAdmitted {
        update_id,
        name: "test_update".to_string(),
        input: Value::Null,
        timestamp: Utc::now(),
    }];
    store::append_events(&mut conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append admitted update event");

    // Mark completed + append WorkflowCompleted event
    mark_completed(&mut conn, exec_id, Value::Null).await;
    let history2 = store::load_history(&mut conn, exec_id).await.unwrap();
    let completed_event = vec![WorkflowEvent::WorkflowCompleted {
        output: Value::Null,
    }];
    store::append_events(&mut conn, exec_id, &completed_event, history2.next_event_id)
        .await
        .expect("append completed event");

    // Poll the result — it should immediately resolve with 409 Conflict
    let response = poll_update_result(&harvest_pool, exec_id, update_id, 1).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["update_id"], update_id.to_string());
    assert_eq!(body["error_type"], "update_orphaned");
    assert_eq!(body["workflow_state"], "COMPLETED");
}

#[tokio::test]
async fn test_get_update_result_orphaned_timed_out() {
    use autumn_harvest::types::UpdateId;
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_running(&mut conn, "orphaned-timeout-wf").await;
    let update_id = UpdateId::new();

    // 1. Append UpdateAdmitted to history
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    let events = vec![WorkflowEvent::UpdateAdmitted {
        update_id,
        name: "test_update".to_string(),
        input: Value::Null,
        timestamp: Utc::now(),
    }];
    store::append_events(&mut conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append admitted update event");

    // 2. Set DB execution state to TIMED_OUT and append WorkflowFailed to history (simulates enforce_workflow_timeout)
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("TIMED_OUT"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            harvest_workflow_executions::error.eq(Some(
                "timeout: workflow exceeded execution timeout".to_string(),
            )),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    let history2 = store::load_history(&mut conn, exec_id).await.unwrap();
    let fail_event = vec![WorkflowEvent::WorkflowFailed {
        error: "timeout: workflow exceeded execution timeout".to_string(),
    }];
    store::append_events(&mut conn, exec_id, &fail_event, history2.next_event_id)
        .await
        .expect("append WorkflowFailed event");

    // 3. Querying result should return 409 Conflict with workflow_state: "TIMED_OUT"
    let (status, body) = get_json(
        &app,
        &format!("/workflows/{exec_id}/update/{update_id}/result"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["update_id"], update_id.to_string());
    assert_eq!(body["error_type"], "update_orphaned");
    assert_eq!(body["workflow_state"], "TIMED_OUT");
}
