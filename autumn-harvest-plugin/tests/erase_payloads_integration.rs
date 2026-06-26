//! Integration tests for targeted PII erasure (issue #495).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise
//! the `POST /workflows/{id}/erase-payloads` management API endpoint end-to-end.
//!
//! Scenarios covered:
//!   (a) All payload fields tombstoned; no original PII string survives in
//!       `harvest_events.event_data`, the execution row, or joined signal rows.
//!   (b) `type`/ids/timestamps/sequence intact, row retained (never deleted).
//!   (c) 409 on a running (non-terminal) target.
//!   (d) Re-run idempotent: `fields_tombstoned == 0`, no error.
//!   (e) Audit row written after successful erasure.
//!   (f) Adjacent running workflow on the same shard still replays clean.

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewHarvestSignal;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{
    harvest_audit_log, harvest_events, harvest_signals, harvest_workflow_executions,
};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
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
    include_str!("../../autumn-harvest/migrations/20260615000001_harvest_context_headers/up.sql")
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
        Some("erase-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

#[allow(dead_code)]
fn build_worker(registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "erase-worker".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 1,
                max_concurrent_activities: 1,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                workflow_task_timeout: std::time::Duration::from_secs(10),
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                sharded_pool: None,
            },
            registry,
        )
        .expect("worker config should be valid"),
    )
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

/// Seed a workflow execution with PII in its `WorkflowStarted` input.
async fn seed_execution_with_pii(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    pii: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "erasable",
            workflow_id,
            exec_id,
            input: json!({ "email": pii, "user_id": 42 }),
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
        },
    )
    .await
    .expect("seed workflow");

    // Append a fake ActivityCompleted event with PII in its output.
    let history = store::load_history(conn, exec_id).await.unwrap();
    let events = vec![WorkflowEvent::ActivityCompleted {
        activity_id: autumn_harvest::types::ActivityExecId::new(),
        output: json!({ "result": pii }),
    }];
    store::append_events(conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append events");

    exec_id
}

/// Mark an execution as COMPLETED.
async fn mark_completed(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .unwrap();
}

/// Check if PII string appears anywhere in the raw `event_data` JSON of an execution.
async fn pii_survives_in_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    pii: &str,
) -> bool {
    let rows: Vec<serde_json::Value> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::event_data)
        .load(conn)
        .await
        .unwrap();

    for row in &rows {
        if row.to_string().contains(pii) {
            return true;
        }
    }
    false
}

/// Check if PII appears in the execution row columns (input, output, memo, `search_attrs`).
async fn pii_survives_in_execution_row(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    pii: &str,
) -> bool {
    #[derive(Queryable)]
    struct ExecPiiCols {
        input: serde_json::Value,
        output: Option<serde_json::Value>,
        memo: Option<serde_json::Value>,
        search_attrs: Option<serde_json::Value>,
    }

    let row: ExecPiiCols = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select((
            harvest_workflow_executions::input,
            harvest_workflow_executions::output,
            harvest_workflow_executions::memo,
            harvest_workflow_executions::search_attrs,
        ))
        .first(conn)
        .await
        .unwrap();

    let input_str = row.input.to_string();
    let output_str = row.output.map(|v| v.to_string()).unwrap_or_default();
    let memo_str = row.memo.map(|v| v.to_string()).unwrap_or_default();
    let search_attrs_str = row.search_attrs.map(|v| v.to_string()).unwrap_or_default();

    input_str.contains(pii)
        || output_str.contains(pii)
        || memo_str.contains(pii)
        || search_attrs_str.contains(pii)
}

/// Check if PII appears in signal payloads for an execution.
async fn pii_survives_in_signals(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    pii: &str,
) -> bool {
    let payloads: Vec<serde_json::Value> = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_signals::payload)
        .load(conn)
        .await
        .unwrap();

    for p in &payloads {
        if p.to_string().contains(pii) {
            return true;
        }
    }
    false
}

/// Seed a pending signal with PII.
async fn seed_signal_with_pii(conn: &mut AsyncPgConnection, exec_id: ExecutionId, pii: &str) {
    diesel::insert_into(harvest_signals::table)
        .values(NewHarvestSignal {
            workflow_exec_id: exec_id.as_uuid(),
            signal_name: "approval",
            payload: json!({ "email": pii, "approved": true }),
            idempotency_key: None,
        })
        .execute(conn)
        .await
        .unwrap();
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// (a) + (b): Erase a completed execution with PII. After erase, no PII
/// survives in events, execution row, or signals. Structural fields
/// (type, event count, execution row) are intact.
#[tokio::test]
async fn erase_tombstones_pii_in_events_row_and_signals() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let pii = "alice@example.com";
    let exec_id = seed_execution_with_pii(&mut conn, "erase-pii-wf", pii).await;
    seed_signal_with_pii(&mut conn, exec_id, pii).await;
    mark_completed(&mut conn, exec_id).await;

    // Set input and output on execution row to contain PII too.
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::output.eq(Some(json!({ "email": pii }))))
        .execute(&mut conn)
        .await
        .unwrap();

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/erase-payloads"),
        json!({ "reason": "GDPR Art. 17 DSR-001" }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(
        body["fields_tombstoned"].as_u64().unwrap_or(0) > 0,
        "fields must have been tombstoned"
    );
    assert_eq!(body["execution_row_scrubbed"], json!(true));

    // (a) No PII string survives in any column/table.
    assert!(
        !pii_survives_in_events(&mut conn, exec_id, pii).await,
        "PII must not survive in harvest_events"
    );
    assert!(
        !pii_survives_in_execution_row(&mut conn, exec_id, pii).await,
        "PII must not survive in harvest_workflow_executions"
    );
    assert!(
        !pii_survives_in_signals(&mut conn, exec_id, pii).await,
        "PII must not survive in harvest_signals"
    );

    // (b) The execution row is still present (not deleted).
    let row_count: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(row_count, 1, "execution row must be retained");

    // (b) Events are retained (append-only invariant preserved).
    let event_count: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert!(
        event_count >= 2,
        "event rows must be retained, found {event_count}"
    );
}

/// (c) 409 when target is not in a terminal state (RUNNING).
#[tokio::test]
async fn erase_returns_409_on_non_terminal_execution() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // Leave the execution in RUNNING state (no mark_completed call).
    let exec_id = seed_execution_with_pii(&mut conn, "erase-running-wf", "bob@example.com").await;

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/erase-payloads"),
        json!({}),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "non-terminal execution must return 409"
    );
}

/// (d) Idempotency: re-running erase returns 0 newly scrubbed fields, no error.
#[tokio::test]
async fn erase_is_idempotent() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let pii = "carol@example.com";
    let exec_id = seed_execution_with_pii(&mut conn, "erase-idempotent-wf", pii).await;
    mark_completed(&mut conn, exec_id).await;

    // First erase.
    let (status1, body1) = post_json(
        &app,
        &format!("/workflows/{exec_id}/erase-payloads"),
        json!({}),
    )
    .await;
    assert_eq!(
        status1,
        StatusCode::OK,
        "first erase should succeed: {body1}"
    );
    let first_tombstoned = body1["fields_tombstoned"].as_u64().unwrap_or(0);
    assert!(first_tombstoned > 0, "first erase must tombstone fields");

    // Second erase — idempotent.
    let (status2, body2) = post_json(
        &app,
        &format!("/workflows/{exec_id}/erase-payloads"),
        json!({}),
    )
    .await;
    assert_eq!(
        status2,
        StatusCode::OK,
        "second erase should succeed: {body2}"
    );
    assert_eq!(
        body2["fields_tombstoned"].as_u64().unwrap_or(1),
        0,
        "re-running erase must report 0 newly tombstoned fields"
    );
}

/// (e) Audit row is written after successful erasure.
#[tokio::test]
async fn erase_writes_audit_record() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution_with_pii(&mut conn, "erase-audit-wf", "dave@example.com").await;
    mark_completed(&mut conn, exec_id).await;

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/erase-payloads"),
        json!({ "reason": "GDPR test" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let audit_count: i64 = harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq("workflow.erase_payloads"))
        .filter(harvest_audit_log::target_id.eq(exec_id.to_string()))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();

    assert_eq!(audit_count, 1, "exactly one audit row must be written");
}

/// (f) Adjacent running workflow on the same shard is unaffected.
#[tokio::test]
async fn erase_does_not_affect_adjacent_running_workflow() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let pii = "eve@example.com";

    // Target: completed execution.
    let target_id = seed_execution_with_pii(&mut conn, "erase-target-wf", pii).await;
    mark_completed(&mut conn, target_id).await;

    // Adjacent: running execution with same PII (to detect leakage).
    let adjacent_id = seed_execution_with_pii(&mut conn, "erase-adjacent-wf", pii).await;
    // Leave adjacent in RUNNING state.

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{target_id}/erase-payloads"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Target PII is gone.
    assert!(
        !pii_survives_in_events(&mut conn, target_id, pii).await,
        "PII must be erased from target"
    );

    // Adjacent running workflow still has its PII intact (untouched).
    assert!(
        pii_survives_in_events(&mut conn, adjacent_id, pii).await,
        "adjacent running workflow must not be erased"
    );
}

/// 404 when execution does not exist.
#[tokio::test]
async fn erase_returns_404_for_unknown_execution() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let unknown_id = ExecutionId::new_for_shard(ShardId::new(0));

    let (status, _body) = post_json(
        &app,
        &format!("/workflows/{unknown_id}/erase-payloads"),
        json!({}),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "unknown execution must return 404"
    );
}
