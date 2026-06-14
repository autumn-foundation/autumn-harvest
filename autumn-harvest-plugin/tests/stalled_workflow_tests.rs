//! Integration tests for the stalled-workflow discovery filter on
//! `GET /workflows` (issue #486). Seeds workflows in specific states and
//! verifies that `no_progress_minutes` returns only the expected subset,
//! that sleeping-on-future-timer workflows are excluded by default (zero false
//! positives), and that `include_sleeping=true` opts them back in.

use autumn_harvest::schema::harvest_events;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Duration, Utc};
use diesel::ExpressionMethods;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
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
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"
    ),
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
    include_str!("../../autumn-harvest/migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000000_harvest_schedule_auto_pause/up.sql"
    ),
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
        "../../autumn-harvest/migrations/20260611000001_harvest_stalled_workflow_index/up.sql"
    )
);

type HarvestApiApp = axum::Router;

fn test_app_state() -> AppState {
    AppState::for_test().with_profile("test")
}

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (database_url, container)
}

fn build_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn build_app(database_url: &str) -> HarvestApiApp {
    let pool = build_pool(database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    harvest_api_router(api_state).with_state(test_app_state())
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read body");
    let json: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("response must be JSON")
    };
    (status, json)
}

/// Seed a RUNNING workflow and insert a single `WorkflowStarted` event backdated
/// by `hours_ago` hours so it appears as "stalled" to the filter.
async fn seed_stalled_workflow(
    database_url: &str,
    workflow_id: &str,
    hours_ago: i64,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "stall_test",
            workflow_id,
            exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
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
            schedule_id: None,
        },
    )
    .await
    .expect("seed workflow");

    // Backdate the WorkflowStarted event that was just appended.
    let backdated_ts = Utc::now() - Duration::hours(hours_ago);
    diesel::update(harvest_events::table)
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .set(harvest_events::timestamp.eq(backdated_ts))
        .execute(&mut conn)
        .await
        .expect("backdate event");

    exec_id
}

/// Add a pending ACTIVITY task queue row for the given execution.
async fn add_pending_activity(database_url: &str, exec_id: ExecutionId) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, priority, attempt, max_attempts, scheduled_at, crash_strikes) \
         VALUES ($1, 'default', 'activity', $2, '{}', 'PENDING', 0, 1, 3, NOW(), 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("insert pending activity");
}

/// Add a future-dated durable timer for the given execution (fires in 1 hour).
async fn add_future_timer(database_url: &str, exec_id: ExecutionId) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    diesel::sql_query(
        "INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired) \
         VALUES ($1, $2, 'timer-1', NOW() + INTERVAL '1 hour', false)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("insert future timer");
}

/// Mark any pending `workflow`-type task queue rows as COMPLETED, simulating a worker
/// having run the workflow function, scheduled a timer, and successfully suspended.
/// A correctly-sleeping workflow has no runnable tasks in the queue; only the
/// future-dated timer row keeps it alive.
async fn complete_workflow_tasks(database_url: &str, exec_id: ExecutionId) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET state = 'COMPLETED' \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("complete workflow tasks");
}

/// Add a workflow-started event with the current timestamp (simulates healthy progress).
async fn touch_workflow(database_url: &str, exec_id: ExecutionId) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    diesel::sql_query(
        "UPDATE harvest_events SET timestamp = NOW() \
         WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("touch event timestamp");
}

fn stall_reason_of(row: &Value) -> &str {
    row["stall_reason"]
        .as_str()
        .expect("stall_reason must be present and a string")
}

fn workflow_ids_of(arr: &Value) -> Vec<String> {
    arr.as_array()
        .expect("response must be an array")
        .iter()
        .map(|r| r["workflow_id"].as_str().expect("workflow_id").to_string())
        .collect()
}

// ─── RED-phase tests ────────────────────────────────────────────────────────
// These tests verify the stalled-workflow discovery filter (issue #486).
// They will FAIL (compile or runtime) until the feature is implemented.

/// AC: A RUNNING workflow with no event progress for > N minutes and zero
/// pending work is returned with `stall_reason=no_pending_work`.
#[tokio::test]
async fn test_no_pending_work_workflow_is_returned() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // Seed a workflow whose only event is 2 hours old with no pending work.
    // Mark the initial workflow task COMPLETED to simulate a workflow that was
    // processed by a worker and then wedged with no further pending items.
    let exec_id = seed_stalled_workflow(&database_url, "wf-no-pending", 2).await;
    complete_workflow_tasks(&database_url, exec_id).await;

    let (status, body) = get_json(&app, "/workflows?no_progress_minutes=30").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let rows = body.as_array().expect("response must be array");
    let ids = workflow_ids_of(&body);

    assert!(
        ids.contains(&"wf-no-pending".to_string()),
        "no-pending-work workflow must appear in stalled list; got {rows:?}"
    );

    let row = rows
        .iter()
        .find(|r| r["workflow_id"] == "wf-no-pending")
        .unwrap();

    // The three new discovery fields must be present.
    assert_eq!(stall_reason_of(row), "no_pending_work");
    assert!(
        row["last_event_at"].is_string(),
        "last_event_at must be present; row={row}"
    );
    assert!(
        row["last_event_age_seconds"].as_f64().unwrap_or(0.0) > 1800.0,
        "last_event_age_seconds must be > 1800 (30 min); row={row}"
    );
    let _ = exec_id;
}

/// AC: A RUNNING workflow sleeping on a future-dated durable timer is NOT
/// returned by default (zero false positives for the sleeping case).
#[tokio::test]
async fn test_sleeping_workflow_excluded_by_default() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let exec_id = seed_stalled_workflow(&database_url, "wf-sleeping", 2).await;
    // Mark the initial workflow task COMPLETED — simulates the worker having run the
    // workflow function, scheduled the timer, and suspended cleanly.  A correctly-sleeping
    // workflow has no runnable tasks in the queue.
    complete_workflow_tasks(&database_url, exec_id).await;
    add_future_timer(&database_url, exec_id).await;

    let (status, body) = get_json(&app, "/workflows?no_progress_minutes=30").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert!(
        !ids.contains(&"wf-sleeping".to_string()),
        "sleeping workflow must NOT appear in default stalled list; got {ids:?}"
    );
}

/// AC: With `include_sleeping=true` the sleeping workflow IS returned with
/// `stall_reason=sleeping_timer`.
#[tokio::test]
async fn test_sleeping_workflow_included_with_flag() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let exec_id = seed_stalled_workflow(&database_url, "wf-sleeping-inc", 2).await;
    complete_workflow_tasks(&database_url, exec_id).await;
    add_future_timer(&database_url, exec_id).await;

    let (status, body) = get_json(
        &app,
        "/workflows?no_progress_minutes=30&include_sleeping=true",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let rows = body.as_array().expect("array");
    let ids = workflow_ids_of(&body);

    assert!(
        ids.contains(&"wf-sleeping-inc".to_string()),
        "sleeping workflow must appear when include_sleeping=true; got {ids:?}"
    );

    let row = rows
        .iter()
        .find(|r| r["workflow_id"] == "wf-sleeping-inc")
        .unwrap();
    assert_eq!(stall_reason_of(row), "sleeping_timer");
}

/// AC: A RUNNING workflow whose most recent event is within the threshold is
/// NOT returned (no false positives for healthy progressing workflows).
#[tokio::test]
async fn test_healthy_workflow_excluded() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // Seed then immediately re-touch so its event timestamp is NOW.
    let exec_id = seed_stalled_workflow(&database_url, "wf-healthy", 0).await;
    touch_workflow(&database_url, exec_id).await;

    let (status, body) = get_json(&app, "/workflows?no_progress_minutes=30").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert!(
        !ids.contains(&"wf-healthy".to_string()),
        "healthy workflow must NOT appear in stalled list; got {ids:?}"
    );
}

/// AC: A RUNNING workflow with no event progress and an active pending activity
/// task is returned with `stall_reason=pending_activity`.
#[tokio::test]
async fn test_stuck_pending_activity_returned() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let exec_id = seed_stalled_workflow(&database_url, "wf-pending-act", 2).await;
    add_pending_activity(&database_url, exec_id).await;

    let (status, body) = get_json(&app, "/workflows?no_progress_minutes=30").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let rows = body.as_array().expect("array");
    let ids = workflow_ids_of(&body);

    assert!(
        ids.contains(&"wf-pending-act".to_string()),
        "stuck-activity workflow must appear in stalled list; got {ids:?}"
    );

    let row = rows
        .iter()
        .find(|r| r["workflow_id"] == "wf-pending-act")
        .unwrap();
    assert_eq!(stall_reason_of(row), "pending_activity");
}

/// Unit-style check: the `stall_reason` field serialises as `snake_case` strings.
#[test]
fn test_stall_reason_serializes_as_snake_case() {
    use autumn_harvest_plugin::api::StallReason;

    assert_eq!(
        serde_json::to_string(&StallReason::PendingActivity).unwrap(),
        "\"pending_activity\""
    );
    assert_eq!(
        serde_json::to_string(&StallReason::PendingChild).unwrap(),
        "\"pending_child\""
    );
    assert_eq!(
        serde_json::to_string(&StallReason::AwaitingSignal).unwrap(),
        "\"awaiting_signal\""
    );
    assert_eq!(
        serde_json::to_string(&StallReason::SleepingTimer).unwrap(),
        "\"sleeping_timer\""
    );
    assert_eq!(
        serde_json::to_string(&StallReason::NoPendingWork).unwrap(),
        "\"no_pending_work\""
    );
}

/// AC: `no_pending_work` is always surfaced regardless of `include_sleeping` — even
/// if we pass `include_sleeping=false`, a workflow with no pending work at all
/// must still be returned (since it cannot be a sleeping-timer case).
#[tokio::test]
async fn test_no_pending_work_always_returned_regardless_of_include_sleeping() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let exec_id = seed_stalled_workflow(&database_url, "wf-always-no-pending", 2).await;
    complete_workflow_tasks(&database_url, exec_id).await;

    // include_sleeping=false is the DEFAULT, but let's be explicit.
    let (status, body) = get_json(
        &app,
        "/workflows?no_progress_minutes=30&include_sleeping=false",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"wf-always-no-pending".to_string()),
        "no_pending_work workflow must always appear; got {ids:?}"
    );
}

/// AC: Caller gets a 400 for a non-positive `no_progress_minutes`.
#[tokio::test]
async fn test_invalid_no_progress_minutes_returns_400() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let (status, _body) = get_json(&app, "/workflows?no_progress_minutes=0").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "0 minutes must be rejected"
    );

    let (status, _body) = get_json(&app, "/workflows?no_progress_minutes=-5").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "negative minutes must be rejected"
    );
}
