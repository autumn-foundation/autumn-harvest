//! Integration tests for serving query handlers on terminal (closed) workflows
//! for post-mortem state inspection (issue #612).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise the
//! `GET/POST /workflows/{id}/query/{name}` and `GET /workflows/{id}/queries`
//! endpoints end-to-end against terminal executions.
//!
//! Scenarios covered:
//!   AC1 — a `COMPLETED` execution answers GET (raw value) and POST (`{result}`)
//!         with the workflow's final reconstructed state → 200.
//!   AC2 — `FAILED`, `CANCELLED`, and `TIMED_OUT` executions answer 200 (final
//!         internal state, never the error string).
//!   AC3 — an unregistered query name on a terminal run → 404.
//!   AC4 — serving a query on a terminal execution appends zero events and
//!         performs zero writes to the execution row.
//!   AC5 — a released/insufficient history → 410, an erased history → 410 (both
//!         distinct bodies); a fully-deleted (unknown) execution stays 404.
//!   AC6 — a workflow whose function spins on replay → 408 (bounded by the
//!         configured `query_timeout`).
//!
//! Docker is unavailable in the authoring sandbox, so this suite is
//! compile-checked here and executed Docker-backed in CI (per the #543/#544/#601
//! precedent).

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::erase;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
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
);

// ── Test workflows ─────────────────────────────────────────────────────────

/// Processes each item from its input via an activity, incrementing an internal
/// counter, and registers a `progress` query reporting that counter. Returns
/// `Err` after processing every item when `should_fail` is set — so the query
/// still reads the computed count, never the error string.
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

        if input
            .get("should_fail")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err("deliberate failure after processing all items".to_string());
        }
        Ok(json!({ "processed": *processed.lock().expect("counter lock") }))
    })
}

/// A workflow whose function never completes on replay (spins yielding),
/// exercising the `query_timeout` → 408 path.
fn spin_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.register_query("stuck", || json!("never"));
        loop {
            tokio::task::yield_now().await;
        }
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

fn spin_info() -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name: "spin_wf",
        module: "tests",
        handler: spin_workflow,
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

// ── Harness ────────────────────────────────────────────────────────────────

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
    build_app_with_query_timeout(pool, Duration::from_secs(5))
}

fn build_app_with_query_timeout(pool: &DbPool, query_timeout: Duration) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.set_query_timeout(query_timeout);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(
            vec![progress_info(), spin_info()],
            vec![],
        )),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("query-test".to_string()),
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

/// Seed an execution row (with an explicit terminal `state`) plus the given
/// history events. `input` is stored on both the execution row and the
/// `WorkflowStarted` event so the drive replays against it.
async fn seed_execution(
    pool: &DbPool,
    workflow_name: &str,
    state: &str,
    input: Value,
    mut events: Vec<WorkflowEvent>,
    erased: bool,
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
    .bind::<diesel::sql_types::Jsonb, _>(input)
    .bind::<diesel::sql_types::Text, _>(state)
    .execute(&mut conn)
    .await
    .expect("seed execution");

    if erased {
        for event in &mut events {
            let mut value = serde_json::to_value(&*event).expect("event serialises");
            let _ = erase::tombstone_payload_fields(&mut value);
            *event = serde_json::from_value(value).expect("event round-trips");
        }
    }

    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("seed history");
    exec_id
}

/// Full `progress_wf` history for `items`: `WorkflowStarted` + one
/// scheduled/completed activity pair per item.
fn progress_history(items: &[&str], should_fail: bool) -> (Value, Vec<WorkflowEvent>) {
    let input = json!({ "items": items, "should_fail": should_fail });
    let mut events = vec![WorkflowEvent::WorkflowStarted {
        input: input.clone(),
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
    (input, events)
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn event_count(pool: &DbPool, exec_id: ExecutionId) -> i64 {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<CountRow>(&mut conn)
        .await
        .expect("count events")
        .n
}

#[derive(diesel::QueryableByName)]
struct ExecStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    updated_at: chrono::DateTime<Utc>,
}

async fn exec_state(pool: &DbPool, exec_id: ExecutionId) -> (String, chrono::DateTime<Utc>) {
    let mut conn = pool.get().await.expect("pooled conn");
    let row = diesel::sql_query(
        "SELECT state, updated_at FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<ExecStateRow>(&mut conn)
    .await
    .expect("load execution state");
    (row.state, row.updated_at)
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn ac1_completed_answers_get_and_post_with_final_state() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, events) = progress_history(&["a", "b", "c"], false);
    let exec_id = seed_execution(&pool, "progress_wf", "COMPLETED", input, events, false).await;

    // GET → raw value.
    let (status, body) = get(&app, &format!("/workflows/{exec_id}/query/progress")).await;
    assert_eq!(status, StatusCode::OK, "GET terminal query: {body:?}");
    assert_eq!(body, json!(3));

    // POST → {"result": value}.
    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/query/progress"),
        json!({ "args": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "POST terminal query: {body:?}");
    assert_eq!(body, json!({ "result": 3 }));

    // The queries-list endpoint also works against a terminal run.
    let (status, body) = get(&app, &format!("/workflows/{exec_id}/queries")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!(["progress"]));
}

#[tokio::test]
async fn ac2_failed_cancelled_timed_out_answer_200_with_internal_state() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // FAILED: workflow returns Err after processing both items — query still
    // reports the internal count (2), never the error string.
    let (input, events) = progress_history(&["x", "y"], true);
    let failed = seed_execution(&pool, "progress_wf", "FAILED", input, events, false).await;
    let (status, body) = get(&app, &format!("/workflows/{failed}/query/progress")).await;
    assert_eq!(status, StatusCode::OK, "FAILED query: {body:?}");
    assert_eq!(body, json!(2));

    for state in ["CANCELLED", "TIMED_OUT"] {
        let (input, events) = progress_history(&["a", "b"], false);
        let exec_id = seed_execution(&pool, "progress_wf", state, input, events, false).await;
        let (status, body) = get(&app, &format!("/workflows/{exec_id}/query/progress")).await;
        assert_eq!(status, StatusCode::OK, "{state} query: {body:?}");
        assert_eq!(body, json!(2), "{state} reports final internal state");
    }
}

#[tokio::test]
async fn ac3_unregistered_name_on_terminal_is_404() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, events) = progress_history(&["a"], false);
    let exec_id = seed_execution(&pool, "progress_wf", "COMPLETED", input, events, false).await;

    let (status, _body) = get(&app, &format!("/workflows/{exec_id}/query/does_not_exist")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "unregistered query name on a terminal run → 404"
    );
}

#[tokio::test]
async fn ac4_query_on_terminal_performs_zero_writes() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, events) = progress_history(&["a", "b", "c"], false);
    let exec_id = seed_execution(&pool, "progress_wf", "COMPLETED", input, events, false).await;

    let events_before = event_count(&pool, exec_id).await;
    let (state_before, updated_before) = exec_state(&pool, exec_id).await;

    let (status, _) = get(&app, &format!("/workflows/{exec_id}/query/progress")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = post_json(
        &app,
        &format!("/workflows/{exec_id}/query/progress"),
        json!({ "args": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let events_after = event_count(&pool, exec_id).await;
    let (state_after, updated_after) = exec_state(&pool, exec_id).await;

    assert_eq!(events_before, events_after, "no events appended by a query");
    assert_eq!(state_before, state_after, "execution state unchanged");
    assert_eq!(updated_before, updated_after, "execution row not rewritten");
}

#[tokio::test]
async fn ac5_released_and_erased_histories_are_410_unknown_is_404() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Released / insufficient: a TERMINATED run whose history is too short to
    // reach the workflow's final state (drive suspends) → 410.
    let released_input = json!({ "items": ["a", "b"], "should_fail": false });
    let released_events = vec![WorkflowEvent::WorkflowStarted {
        input: released_input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let released = seed_execution(
        &pool,
        "progress_wf",
        "TERMINATED",
        released_input,
        released_events,
        false,
    )
    .await;
    let (status, body) = get(&app, &format!("/workflows/{released}/query/progress")).await;
    assert_eq!(status, StatusCode::GONE, "released history → 410: {body:?}");
    let text = body.to_string();
    assert!(
        text.contains("history unavailable"),
        "distinct 410 body: {text}"
    );

    // Erased (PII-tombstoned) history → 410 with a distinct reason.
    let (input, events) = progress_history(&["a", "b"], false);
    let erased = seed_execution(&pool, "progress_wf", "COMPLETED", input, events, true).await;
    let (status, body) = get(&app, &format!("/workflows/{erased}/query/progress")).await;
    assert_eq!(status, StatusCode::GONE, "erased history → 410: {body:?}");
    assert!(
        body.to_string().contains("erased"),
        "erased reason surfaced: {body:?}"
    );

    // A fully-deleted (never-existed) execution stays 404, not 410.
    let unknown = ExecutionId::new();
    let (status, _) = get(&app, &format!("/workflows/{unknown}/query/progress")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unknown execution → 404");
}

#[tokio::test]
async fn ac6_spinning_replay_returns_408() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    // A tiny query budget forces the spinning workflow's replay to time out.
    let app = build_app_with_query_timeout(&pool, Duration::from_millis(50));

    let input = json!({});
    let events = vec![WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let exec_id = seed_execution(&pool, "spin_wf", "COMPLETED", input, events, false).await;

    let (status, body) = get(&app, &format!("/workflows/{exec_id}/query/stuck")).await;
    assert_eq!(
        status,
        StatusCode::REQUEST_TIMEOUT,
        "a spinning replay → 408: {body:?}"
    );
}
