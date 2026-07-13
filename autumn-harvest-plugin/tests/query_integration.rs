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

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

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

/// Like [`progress_workflow`] but calls `ctx.continue_as_new(...)` after
/// processing every item — which parks the function forever. A `CONTINUED_AS_NEW`
/// run therefore replays to a `Suspended` outcome (not `Poll::Ready`), yet its
/// history carries the terminal seal, so its query must still serve 200.
fn continue_as_new_workflow<'a>(
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

        ctx.continue_as_new(json!({ "carry": *processed.lock().expect("counter lock") }))
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
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

fn continue_as_new_info() -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name: "continue_as_new_wf",
        module: "tests",
        handler: continue_as_new_workflow,
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

/// Issue #772 (round 6): a workflow with an `execution_timeout` that performs
/// the deadline-aware `should_continue_as_new()` checkpoint check (recording /
/// consuming the reserved `__harvest_deadline_probe` side-effect) BEFORE
/// processing each item. Its recorded history therefore carries the probe
/// event — a query replay context that fails to thread the execution's
/// `execution_timeout`/`deadline_at` leaves that probe unconsumed and diverges.
fn deadline_probe_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let processed = Arc::new(std::sync::Mutex::new(0u64));
        let state = processed.clone();
        ctx.register_query_handler::<Value, u64, _>("progress", move |_req: &Value| {
            Ok(*state.lock().expect("counter lock"))
        });

        // Deadline-aware checkpoint: records/consumes the reserved
        // __harvest_deadline_probe side-effect when the run has an
        // execution_timeout (#772). It must not trip in this fixture (the
        // deadline is 24h out and the history is tiny).
        if ctx.should_continue_as_new() {
            return Err("fixture must not trip continue-as-new".to_string());
        }

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

fn deadline_probe_info() -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name: "deadline_probe_wf",
        module: "tests",
        handler: deadline_probe_workflow,
        execution_timeout: Some(Duration::from_secs(24 * 3600)),
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
        .with_init_sql(init_sql())
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
            vec![
                progress_info(),
                continue_as_new_info(),
                spin_info(),
                deadline_probe_info(),
            ],
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

    // `erase_workflow_payloads` tombstones BOTH the event payloads AND the
    // execution row's own `input` column — the terminal-query erasure check is
    // now the O(1) row check, so a realistic erased fixture must tombstone the
    // row input too (issue #612 review).
    let row_input = if erased {
        erase::erasure_tombstone()
    } else {
        input.clone()
    };

    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, $2, $3, 0, $4, 'default', $5)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Jsonb, _>(row_input)
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

/// A history the engine sealed while a workflow was parked mid-activity: full
/// pairs for `completed`, then a bare `ActivityScheduled` for `pending` (no
/// completion), then the `seal` terminal event. Driving `progress_workflow` with
/// `items = completed ++ [pending]` reconstructs a partial count of
/// `completed.len()` before parking on the incomplete activity.
fn sealed_mid_activity_history(
    completed: &[&str],
    pending: &str,
    seal: WorkflowEvent,
) -> (Value, Vec<WorkflowEvent>) {
    let mut items: Vec<&str> = completed.to_vec();
    items.push(pending);
    let input = json!({ "items": items, "should_fail": false });
    let mut events = vec![WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    for item in completed {
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
    events.push(WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "process_item".into(),
        input: json!(pending),
        queue: "default".into(),
    });
    events.push(seal);
    (input, events)
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

    // FAILED (cooperative return): the workflow returns Err after processing both
    // items — a full history that drives to Poll::Ready. Query reports the
    // internal count (2), never the error string.
    let (input, events) = progress_history(&["x", "y"], true);
    let failed = seed_execution(&pool, "progress_wf", "FAILED", input, events, false).await;
    let (status, body) = get(&app, &format!("/workflows/{failed}/query/progress")).await;
    assert_eq!(status, StatusCode::OK, "FAILED query: {body:?}");
    assert_eq!(body, json!(2));

    // CANCELLED / TIMED_OUT: the engine seals these while the function is parked
    // on an in-flight activity, so the history ends with a trailing incomplete
    // ActivityScheduled + a terminal lifecycle event and the drive replays to a
    // *suspension*, not Poll::Ready. It must still serve 200 with the partial
    // internal state reconstructed at the recorded terminal point (count = 1,
    // one activity completed before the seal).
    for (state, seal) in [
        (
            "CANCELLED",
            WorkflowEvent::WorkflowCancelled {
                reason: "operator cancelled mid-flight".into(),
            },
        ),
        (
            "TIMED_OUT",
            WorkflowEvent::WorkflowExecutionTimedOut {
                deadline: Utc::now(),
                timed_out_at: Utc::now(),
            },
        ),
    ] {
        let (input, events) = sealed_mid_activity_history(&["a"], "b", seal);
        let exec_id = seed_execution(&pool, "progress_wf", state, input, events, false).await;
        let (status, body) = get(&app, &format!("/workflows/{exec_id}/query/progress")).await;
        assert_eq!(status, StatusCode::OK, "{state} query: {body:?}");
        assert_eq!(
            body,
            json!(1),
            "{state} (sealed while parked mid-activity) reports partial internal state"
        );
    }
}

#[tokio::test]
async fn ac2_continued_as_new_answers_200_with_internal_state() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // CONTINUED_AS_NEW: continue_as_new parks the function forever, so the run
    // replays to a *suspension*, not Poll::Ready — yet its history ends with the
    // WorkflowContinuedAsNew terminal seal, so the query must serve 200 with the
    // count reached at the continue_as_new point (2).
    let (input, mut events) = progress_history(&["a", "b"], false);
    events.push(WorkflowEvent::WorkflowContinuedAsNew {
        new_exec_id: ExecutionId::new(),
        input: json!({ "carry": 2 }),
    });
    let exec_id = seed_execution(
        &pool,
        "continue_as_new_wf",
        "CONTINUED_AS_NEW",
        input,
        events,
        false,
    )
    .await;

    let (status, body) = get(&app, &format!("/workflows/{exec_id}/query/progress")).await;
    assert_eq!(status, StatusCode::OK, "CONTINUED_AS_NEW query: {body:?}");
    assert_eq!(
        body,
        json!(2),
        "CONTINUED_AS_NEW reports the count reached at the continue_as_new point"
    );
}

#[tokio::test]
async fn running_partial_history_serves_query_with_partial_state() {
    // FIX #612 review: guard the running-workflow path within this feature's own
    // file. A RUNNING execution with a partial history (parks at a suspension, no
    // terminal seal) must serve 200 with the partial state — NEVER 410. The
    // terminal-seal classifier only applies to terminal executions; a RUNNING run
    // is served regardless of the drive outcome.
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Two of three items completed, third never scheduled → drive suspends.
    let input = json!({ "items": ["a", "b", "c"], "should_fail": false });
    let mut events = vec![WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    for item in ["a", "b"] {
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
    let exec_id = seed_execution(&pool, "progress_wf", "RUNNING", input, events, false).await;

    let (status, body) = get(&app, &format!("/workflows/{exec_id}/query/progress")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a RUNNING partial-history query must serve 200, not 410: {body:?}"
    );
    assert_eq!(body, json!(2), "running path serves current partial state");
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

// ── Issue #772 (round 6): deadline probe × query hydration ───────────────────
//
// A workflow with an `execution_timeout` records a `__harvest_deadline_probe`
// side-effect whenever it calls `should_continue_as_new()`. The query hydration
// paths (`hydrate_ctx_for_query` for the HTTP API here, and the in-process
// `execute_query_in_process` in `handle.rs`) must thread the execution row's
// `execution_timeout` + `deadline_at` into their replay context, or the probe is
// left unconsumed and the query diverges (500 during drive, or a spurious 410 on
// the terminal drift check). These DB-backed tests exercise the real HTTP path.

/// Seed an execution row carrying `execution_timeout` (INTERVAL) and
/// `deadline_at` (TIMESTAMPTZ) plus a history whose events include the deadline
/// probe — so `load_execution` in `hydrate_ctx_for_query` surfaces the budget the
/// fix threads into the replay context.
async fn seed_execution_with_deadline(
    pool: &DbPool,
    workflow_name: &str,
    state: &str,
    input: Value,
    events: Vec<WorkflowEvent>,
    timeout_secs: f64,
    deadline_at: chrono::DateTime<Utc>,
) -> ExecutionId {
    let mut conn = pool.get().await.expect("pooled conn");
    let exec_id = ExecutionId::new();

    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state, \
          execution_timeout, deadline_at) \
         VALUES ($1, $2, $3, 0, $4, 'default', $5, make_interval(secs => $6), $7)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Jsonb, _>(input)
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Double, _>(timeout_secs)
    .bind::<diesel::sql_types::Timestamptz, _>(deadline_at)
    .execute(&mut conn)
    .await
    .expect("seed execution with deadline");

    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("seed history");
    exec_id
}

/// `WorkflowStarted` + the reserved deadline probe + one scheduled/completed
/// pair per item. Returns the input, the events, and the 24h-out deadline the
/// run was recorded against.
fn deadline_probe_history(
    items: &[&str],
    completed: bool,
) -> (Value, Vec<WorkflowEvent>, chrono::DateTime<Utc>) {
    let t0 = chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
    let deadline = t0 + chrono::Duration::hours(24);
    let recorded_now = t0 + chrono::Duration::seconds(1);
    let input = json!({ "items": items });
    let mut events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: t0,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Now,
            name: Some(autumn_harvest::DEADLINE_PROBE_SIDE_EFFECT_NAME.to_string()),
            value: json!(recorded_now.timestamp_millis()),
        },
    ];
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
    if completed {
        events.push(WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        });
    }
    (input, events, deadline)
}

/// The finding (P2): querying a RUNNING workflow whose history contains the
/// deadline probe must succeed with the reconstructed count — NOT diverge —
/// once the hydration path threads the `execution_timeout`/`deadline_at`.
#[tokio::test]
async fn deadline_probe_running_query_serves_reconstructed_state() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, events, deadline) = deadline_probe_history(&["a", "b", "c"], false);
    let exec_id = seed_execution_with_deadline(
        &pool,
        "deadline_probe_wf",
        "RUNNING",
        input,
        events,
        24.0 * 3600.0,
        deadline,
    )
    .await;

    let (status, body) = get(&app, &format!("/workflows/{exec_id}/query/progress")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a RUNNING query over a deadline-probe history must serve 200, not diverge: {body:?}"
    );
    assert_eq!(
        body,
        json!(3),
        "the query reports the fully reconstructed count across the probe: {body:?}"
    );
}

/// The finding (P2, terminal drift): querying a TERMINAL (COMPLETED) workflow
/// whose history contains the deadline probe must serve 200 — NOT a spurious 410
/// from the drift check that mistakes the unconsumed probe for leftover history.
#[tokio::test]
async fn deadline_probe_terminal_query_serves_not_gone() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, events, deadline) = deadline_probe_history(&["x", "y"], true);
    let exec_id = seed_execution_with_deadline(
        &pool,
        "deadline_probe_wf",
        "COMPLETED",
        input,
        events,
        24.0 * 3600.0,
        deadline,
    )
    .await;

    let (status, body) = get(&app, &format!("/workflows/{exec_id}/query/progress")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a terminal query over a deadline-probe history must serve 200, not a spurious 410: {body:?}"
    );
    assert_eq!(
        body,
        json!(2),
        "the terminal query reports the final reconstructed count: {body:?}"
    );
}
