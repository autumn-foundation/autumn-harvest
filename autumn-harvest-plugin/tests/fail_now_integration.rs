//! Integration tests for the single-activity force-fail endpoint (issue #765).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise
//! `POST /workflows/{id}/activities/{activity_exec_id}/fail-now` end-to-end.
//!
//! Scenarios covered:
//!   (a) Force-failing a RUNNING activity returns 202 with `forced: true` and
//!       the distinct `error_type: "OperatorForceFailed"`, marks the task row
//!       FAILED, appends the `ActivityFailed` event, and writes an audit row
//!       under `OP_ACTIVITY_FAIL_NOW`.
//!   (b) Idempotent repeat: 202 with `forced: false, already_forced: true`
//!       and exactly one `ActivityFailed` event in history.
//!   (c) A PENDING (backing-off) task → 409.
//!   (d) A workflow task at the id → 409.
//!   (e) Unknown task id → 404.

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
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
use uuid::Uuid;

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
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    // The admin-gated fail-now route is guarded by `require_harvest_admin`;
    // open the boundary so the integration suite can reach the handler.
    build_app_with_admin_boundary(pool, true)
}

/// Like [`build_app`] but WITHOUT the outer-auth-boundary declaration, so the
/// built-in `require_harvest_admin` guard applies — mirrors the
/// `redrive_requires_admin` precedent in `dlq_redrive_integration.rs`.
fn build_app_no_admin(pool: &DbPool) -> HarvestApiApp {
    build_app_with_admin_boundary(pool, false)
}

fn build_app_with_admin_boundary(pool: &DbPool, boundary: bool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    if boundary {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("fail-now-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Actor identity sent as `x-harvest-actor` on every request — the header the
/// default `HarvestApiState::extract_actor` reads into audit rows.
const TEST_ACTOR: &str = "alice@ops";

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

/// Seed an execution row + `WorkflowStarted`/`ActivityScheduled` history and
/// return `(exec_id, activity_id)`.
async fn seed_execution_with_scheduled_activity(pool: &DbPool) -> (ExecutionId, ActivityExecId) {
    let mut conn = pool.get().await.expect("pooled conn");
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name) \
         VALUES ($1, 'ff_wf', $2, 0, '{}'::jsonb, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .execute(&mut conn)
    .await
    .expect("seed execution");

    let activity_id = ActivityExecId::new();
    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: json!({}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "hung_activity".to_string(),
                input: json!({}),
                queue: "default".to_string(),
            },
        ],
        0,
    )
    .await
    .expect("seed history");
    (exec_id, activity_id)
}

/// Insert a task-queue row for the execution and return its PK.
async fn seed_task(
    pool: &DbPool,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    task_type: &str,
    state: &str,
) -> Uuid {
    let mut conn = pool.get().await.expect("pooled conn");
    let task_id = Uuid::new_v4();
    let worker_id = if state == "RUNNING" {
        Some("hung-worker")
    } else {
        None
    };
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, state, \
          attempt, max_attempts, worker_id, started_at, scheduled_at) \
         VALUES ($1, 'default', $2, $3, 'hung_activity', $4, '{}'::jsonb, $5, 1, 5, $6, \
                 CASE WHEN $5 = 'RUNNING' THEN NOW() ELSE NULL END, \
                 CASE WHEN $5 = 'PENDING' THEN NOW() + INTERVAL '5 minutes' ELSE NOW() END)",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Text, _>(task_type)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(worker_id)
    .execute(&mut conn)
    .await
    .expect("seed task row");
    task_id
}

#[derive(diesel::QueryableByName)]
struct TaskStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct ActorRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    actor: String,
}

#[tokio::test]
async fn force_fail_over_http_returns_202_and_writes_audit_row() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (exec_id, activity_id) = seed_execution_with_scheduled_activity(&pool).await;
    let task_id = seed_task(&pool, exec_id, activity_id, "activity", "RUNNING").await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/activities/{task_id}/fail-now"),
        json!({"reason": "hung on dead downstream, INC-42"}),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
    assert_eq!(body["ok"], true);
    assert_eq!(body["forced"], true);
    assert_eq!(body["already_forced"], false);
    assert_eq!(body["error_type"], "OperatorForceFailed");
    assert_eq!(body["activity_name"], "hung_activity");
    assert_eq!(body["queue"], "default");

    let mut conn = pool.get().await.expect("pooled conn");
    let row: TaskStateRow = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(&mut conn)
        .await
        .expect("task state");
    assert_eq!(row.state, "FAILED");

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert!(
        history.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityFailed { activity_id: id, error_type, non_retryable, .. }
                if *id == activity_id
                    && error_type == "OperatorForceFailed"
                    && *non_retryable
        )),
        "history must carry the forced ActivityFailed event"
    );

    let audit: CountRow = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_audit_log \
         WHERE operation = 'activity.fail_now' AND status = 'succeeded' AND target_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(task_id.to_string())
    .get_result(&mut conn)
    .await
    .expect("audit count");
    assert_eq!(audit.n, 1, "one succeeded audit row must exist");

    // The audit row attributes the call to the x-harvest-actor header value
    // (read by the default `extract_actor`), not "anonymous".
    let actor: ActorRow = diesel::sql_query(
        "SELECT actor FROM harvest_audit_log \
         WHERE operation = 'activity.fail_now' AND target_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(task_id.to_string())
    .get_result(&mut conn)
    .await
    .expect("audit actor");
    assert_eq!(
        actor.actor, TEST_ACTOR,
        "audit row must record the header actor"
    );
}

#[tokio::test]
async fn second_call_is_idempotent_over_http() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (exec_id, activity_id) = seed_execution_with_scheduled_activity(&pool).await;
    let task_id = seed_task(&pool, exec_id, activity_id, "activity", "RUNNING").await;
    let uri = format!("/workflows/{exec_id}/activities/{task_id}/fail-now");

    let (first_status, first) = post_json(&app, &uri, json!({})).await;
    assert_eq!(first_status, StatusCode::ACCEPTED, "body: {first}");
    assert_eq!(first["forced"], true);

    let (second_status, second) = post_json(&app, &uri, json!({})).await;
    assert_eq!(second_status, StatusCode::ACCEPTED, "body: {second}");
    assert_eq!(second["forced"], false);
    assert_eq!(second["already_forced"], true);

    // Exactly one ActivityFailed event in history.
    let mut conn = pool.get().await.expect("pooled conn");
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    let failed = history
        .events
        .iter()
        .filter(|e| {
            matches!(
                e,
                WorkflowEvent::ActivityFailed { activity_id: id, .. } if *id == activity_id
            )
        })
        .count();
    assert_eq!(failed, 1, "no second terminal event may be appended");
}

#[tokio::test]
async fn pending_task_conflicts_with_409() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (exec_id, activity_id) = seed_execution_with_scheduled_activity(&pool).await;
    let task_id = seed_task(&pool, exec_id, activity_id, "activity", "PENDING").await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/activities/{task_id}/fail-now"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
}

#[tokio::test]
async fn workflow_task_conflicts_with_409() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (exec_id, activity_id) = seed_execution_with_scheduled_activity(&pool).await;
    let task_id = seed_task(&pool, exec_id, activity_id, "workflow", "RUNNING").await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/activities/{task_id}/fail-now"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
}

#[tokio::test]
async fn unknown_task_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (exec_id, _activity_id) = seed_execution_with_scheduled_activity(&pool).await;
    let bogus = Uuid::new_v4();

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/activities/{bogus}/fail-now"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "body: {body}");
}

#[tokio::test]
async fn unknown_execution_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Well-formed execution id that routes to the default shard but has no
    // row — the execution lock reports NotFound before any task lookup.
    let exec_id = ExecutionId::new();
    let task_id = Uuid::new_v4();

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/activities/{task_id}/fail-now"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "body: {body}");
}

#[tokio::test]
async fn malformed_activity_id_returns_404_and_failed_audit_row() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (exec_id, _activity_id) = seed_execution_with_scheduled_activity(&pool).await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/activities/not-a-uuid/fail-now"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "body: {body}");

    // The rejected call is visible in the audit log as a failed
    // activity.fail_now operation targeting the malformed id.
    let mut conn = pool.get().await.expect("pooled conn");
    let audit: CountRow = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_audit_log \
         WHERE operation = 'activity.fail_now' AND status = 'failed' AND target_id = 'not-a-uuid'",
    )
    .get_result(&mut conn)
    .await
    .expect("audit count");
    assert_eq!(audit.n, 1, "one failed audit row must exist");
}

#[tokio::test]
async fn fail_now_requires_admin() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app_no_admin(&pool);

    let (exec_id, activity_id) = seed_execution_with_scheduled_activity(&pool).await;
    let task_id = seed_task(&pool, exec_id, activity_id, "activity", "RUNNING").await;

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/activities/{task_id}/fail-now"),
        json!({}),
    )
    .await;
    assert!(
        status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN,
        "fail-now must be admin-guarded, got {status} (body: {body})"
    );

    // The request was rejected before the handler ran: task untouched.
    let mut conn = pool.get().await.expect("pooled conn");
    let row: TaskStateRow = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(&mut conn)
        .await
        .expect("task state");
    assert_eq!(
        row.state, "RUNNING",
        "guarded request must not mutate the task"
    );
}
