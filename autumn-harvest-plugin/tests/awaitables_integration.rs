//! Integration tests for the open-awaitables diagnostic (issue #615):
//! `GET /workflows/{id}/awaitables` plus the Vantage UI blocked-on panel
//! re-source, exercised end-to-end over the real routers against Postgres.
//!
//! Scenarios covered:
//!   - The issue's falsifiable delta: an execution parked on
//!     `ctx.wait_for_signal("approval")` with NO signal sent reports an
//!     awaitable naming "approval" (`wait_set` = "replayed").
//!   - A pending activity reports name + activity exec id + since, and is
//!     enriched with the task-row deadline (`schedule_to_close_at`).
//!   - A pending child workflow reports the child's workflow name + exec id.
//!   - A terminal execution returns HTTP 200 with an EMPTY awaitables list and
//!     the terminal state echoed (`wait_set` = "terminal").
//!   - An unregistered workflow type degrades to the history-only scan with
//!     `wait_set_reason = "handler_not_registered"` — still naming the timer.
//!   - Serving the report performs zero writes (no events appended, execution
//!     row untouched).
//!   - Unknown execution → 404; malformed id → 400; missing admin → 401.
//!   - The UI workflow-detail page renders the replay-derived awaitables
//!     section naming the awaited-but-unsent signal (the side tables alone
//!     cannot show it).
//!
//! Dual-mode: uses a running Postgres from `HARVEST_TEST_DATABASE_URL` (as an
//! admin URL onto which a fresh database is created and migrated) when set — no
//! Docker required — else boots a fresh testcontainers Postgres. Docker is not
//! available in every sandbox, so this suite is compile-checked there and run
//! Docker-backed in CI (matching the #543/#544/#601 precedent).

#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{TimeZone, Utc};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

// ── Test workflows ─────────────────────────────────────────────────────────

/// Parks on `wait_for_signal("approval")` — the issue's falsifiable delta.
fn signal_wait_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let payload = ctx
            .wait_for_signal("approval")
            .await
            .map_err(|e| e.to_string())?;
        Ok(payload)
    })
}

/// Executes one activity then completes — parked while the activity is open.
fn activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("charge_card", json!({"amount": 42}), "payments")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Spawns one child workflow then awaits it — parked while the child is open.
fn child_parent_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("fulfillment_flow", json!({"order": 7}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Completes immediately — a RUNNING row whose replay reaches terminal.
fn completes_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!("done")) })
}

/// Panics on first poll — the replay drive contains it (#782) and reports it.
fn panics_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { panic!("deliberate test panic") })
}

/// Calls `ctx.system_now()` (a deterministic primitive) then parks
/// command-lessly. Replayed over a history whose recorded side effect has the
/// WRONG kind, the primitive records a deferred non-determinism error — the
/// drive still suspends, and the endpoint must degrade rather than serve a
/// wait-set derived from diverged code.
fn diverged_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _now = ctx.system_now();
        ctx.await_condition(|| false)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "tests",
        handler,
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

/// With `HARVEST_TEST_DATABASE_URL` set, a fresh database is created and
/// migrated on that server (guard `None`); otherwise a fresh testcontainers
/// Postgres backs it (guard `Some`).
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    let (admin_url, guard) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };

    let db = format!("harvest_awaitables_{}", Uuid::new_v4().simple());
    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connect");
    diesel::sql_query(format!("CREATE DATABASE {db}"))
        .execute(&mut admin)
        .await
        .expect("create db");

    let url = fresh_db_url(&admin_url, &db);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("db connect");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("migrate db");
    (url, guard)
}

/// Swap the database-name path segment of `admin_url`, preserving any query
/// string (e.g. `?sslmode=require`).
fn fresh_db_url(admin_url: &str, db: &str) -> String {
    let (base, query) = admin_url
        .split_once('?')
        .map_or((admin_url, None), |(b, q)| (b, Some(q)));
    let trimmed = base.rsplit_once('/').map_or(base, |(prefix, _)| prefix);
    query.map_or_else(
        || format!("{trimmed}/{db}"),
        |q| format!("{trimmed}/{db}?{q}"),
    )
}

fn build_pool(url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

fn build_api_state(pool: &DbPool) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(
            vec![
                wf_info("signal_wait_wf", signal_wait_workflow),
                wf_info("activity_wf", activity_workflow),
                wf_info("child_wf", child_parent_workflow),
                wf_info("completes_wf", completes_workflow),
                wf_info("panics_wf", panics_workflow),
                wf_info("diverged_wf", diverged_workflow),
            ],
            vec![],
        )),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("awaitables-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

fn build_api_app(pool: &DbPool) -> axum::Router {
    harvest_api_router(build_api_state(pool)).with_state(AppState::for_test().with_profile("test"))
}

fn build_ui_app(pool: &DbPool) -> axum::Router {
    harvest_ui_router(build_api_state(pool)).with_state(AppState::for_test().with_profile("test"))
}

async fn get_json(app: &axum::Router, uri: &str, admin: bool) -> (StatusCode, Value) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if admin {
        builder = builder.header("x-harvest-admin", "true");
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
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

async fn fetch_html(app: &axum::Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// Seed an execution row plus history events, returning the id.
async fn seed_execution(
    pool: &DbPool,
    workflow_name: &str,
    state: &str,
    input: Value,
    events: Vec<WorkflowEvent>,
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
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("seed history");
    exec_id
}

fn started_event(input: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn kinds(report: &Value) -> Vec<String> {
    report["awaitables"]
        .as_array()
        .expect("awaitables array")
        .iter()
        .map(|a| a["kind"].as_str().expect("kind").to_string())
        .collect()
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// The issue's falsifiable delta: a workflow parked on
/// `ctx.receive_signal("approval")` with no signal sent must report an
/// awaitable naming "approval".
#[tokio::test]
async fn parked_signal_wait_names_the_awaited_signal() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(&pool);

    let exec_id = seed_execution(
        &pool,
        "signal_wait_wf",
        "RUNNING",
        Value::Null,
        vec![started_event(Value::Null)],
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["wait_set"], "replayed");
    assert_eq!(body["is_terminal"], false);
    assert_eq!(body["state"], "RUNNING");
    let awaitables = body["awaitables"].as_array().expect("awaitables");
    assert_eq!(awaitables.len(), 1, "body: {body}");
    assert_eq!(awaitables[0]["kind"], "signal");
    assert_eq!(awaitables[0]["name"], "approval");
    assert!(
        awaitables[0].get("since").is_some(),
        "signal awaitable should carry a since timestamp: {body}"
    );
}

/// A pending activity reports name + id + since, and the deadline is enriched
/// from the task row's `schedule_to_close_at`.
#[tokio::test]
async fn parked_activity_reports_name_id_since_and_task_row_deadline() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(&pool);

    let activity_id = ActivityExecId::new();
    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        Value::Null,
        vec![
            started_event(Value::Null),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: json!({"amount": 42}),
                queue: "payments".into(),
            },
        ],
    )
    .await;

    // Task row carrying the cross-retry deadline (#378).
    let deadline = Utc.with_ymd_and_hms(2026, 7, 1, 13, 0, 0).unwrap();
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, \
              input, state, priority, attempt, max_attempts, scheduled_at, schedule_to_close_at) \
             VALUES ($1, 'payments', 'activity', $2, 'charge_card', $3, \
                     '{}'::jsonb, 'PENDING', 0, 1, 3, NOW(), $4)",
        )
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
        .bind::<diesel::sql_types::Timestamptz, _>(deadline)
        .execute(&mut conn)
        .await
        .expect("seed task row");
    }

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["wait_set"], "replayed");
    let awaitables = body["awaitables"].as_array().expect("awaitables");
    assert_eq!(awaitables.len(), 1, "body: {body}");
    assert_eq!(awaitables[0]["kind"], "activity");
    assert_eq!(awaitables[0]["name"], "charge_card");
    assert_eq!(awaitables[0]["id"], activity_id.to_string());
    assert!(awaitables[0].get("since").is_some(), "body: {body}");
    let got_deadline = awaitables[0]["deadline"]
        .as_str()
        .expect("deadline enriched from task row");
    assert!(
        got_deadline.starts_with("2026-07-01T13:00:00"),
        "deadline should come from schedule_to_close_at: {got_deadline}"
    );

    // T12: when the current attempt's start-to-close deadline
    // (started_at + start_to_close) is EARLIER than the cross-retry
    // schedule_to_close_at, the enrichment picks the earliest of the two.
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "UPDATE harvest_task_queue \
             SET state = 'RUNNING', started_at = $2, start_to_close = interval '600 seconds' \
             WHERE activity_id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
        .bind::<diesel::sql_types::Timestamptz, _>(
            Utc.with_ymd_and_hms(2026, 7, 1, 12, 30, 0).unwrap(),
        )
        .execute(&mut conn)
        .await
        .expect("update task row");
    }
    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let awaitables = body["awaitables"].as_array().expect("awaitables");
    let got_deadline = awaitables[0]["deadline"]
        .as_str()
        .expect("deadline enriched from task row");
    assert!(
        got_deadline.starts_with("2026-07-01T12:40:00"),
        "the EARLIEST of start-to-close (12:40) and schedule-to-close (13:00) wins: {got_deadline}"
    );
}

/// T7: a RUNNING row whose replay reaches `Poll::Ready` degrades with
/// `replay_reached_terminal`; a panicking handler degrades with
/// `replay_panicked`; a deferred non-determinism divergence degrades with
/// `replay_diverged`. Each still serves 200 with the history-only scan.
#[tokio::test]
async fn replay_failure_modes_degrade_with_named_reasons() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(&pool);

    let reached = seed_execution(
        &pool,
        "completes_wf",
        "RUNNING",
        Value::Null,
        vec![started_event(Value::Null)],
    )
    .await;
    let (status, body) = get_json(&app, &format!("/workflows/{reached}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["wait_set"], "history_only");
    assert_eq!(body["wait_set_reason"], "replay_reached_terminal");

    let panicked = seed_execution(
        &pool,
        "panics_wf",
        "RUNNING",
        Value::Null,
        vec![started_event(Value::Null)],
    )
    .await;
    let (status, body) = get_json(&app, &format!("/workflows/{panicked}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["wait_set"], "history_only");
    assert_eq!(body["wait_set_reason"], "replay_panicked");

    let diverged = seed_execution(
        &pool,
        "diverged_wf",
        "RUNNING",
        Value::Null,
        vec![
            started_event(Value::Null),
            WorkflowEvent::SideEffectRecorded {
                kind: autumn_harvest::event::SideEffectKind::Random,
                name: None,
                value: json!(7),
            },
        ],
    )
    .await;
    let (status, body) = get_json(&app, &format!("/workflows/{diverged}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["wait_set"], "history_only", "body: {body}");
    assert_eq!(body["wait_set_reason"], "replay_diverged", "body: {body}");
}

/// A pending child workflow reports the child's workflow name and exec id.
#[tokio::test]
async fn parked_child_reports_child_name_and_exec_id() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(&pool);

    let child_id = ExecutionId::new();
    let exec_id = seed_execution(
        &pool,
        "child_wf",
        "RUNNING",
        Value::Null,
        vec![
            started_event(Value::Null),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "fulfillment_flow".into(),
                input: json!({"order": 7}),
            },
        ],
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["wait_set"], "replayed");
    let awaitables = body["awaitables"].as_array().expect("awaitables");
    assert_eq!(awaitables.len(), 1, "body: {body}");
    assert_eq!(awaitables[0]["kind"], "child_workflow");
    assert_eq!(awaitables[0]["name"], "fulfillment_flow");
    assert_eq!(awaitables[0]["id"], child_id.to_string());
}

/// A terminal execution returns HTTP 200 with an empty list and the state
/// echoed — never an error.
#[tokio::test]
async fn terminal_execution_returns_empty_list_with_state_echoed() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(&pool);

    let exec_id = seed_execution(
        &pool,
        "signal_wait_wf",
        "COMPLETED",
        Value::Null,
        vec![started_event(Value::Null)],
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["is_terminal"], true);
    assert_eq!(body["state"], "COMPLETED");
    assert_eq!(body["wait_set"], "terminal");
    assert_eq!(body["awaitables"], json!([]));
}

/// An unregistered workflow type degrades to the history-only scan — still
/// naming what the event log can see (an unfired timer here).
#[tokio::test]
async fn unregistered_handler_degrades_to_history_only_scan() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(&pool);

    let exec_id = seed_execution(
        &pool,
        "ghost_wf",
        "RUNNING",
        Value::Null,
        vec![
            started_event(Value::Null),
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("cooldown"),
                duration_secs: 300,
            },
        ],
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["wait_set"], "history_only");
    assert_eq!(body["wait_set_reason"], "handler_not_registered");
    assert_eq!(kinds(&body), vec!["timer"]);
    let awaitables = body["awaitables"].as_array().expect("awaitables");
    assert_eq!(awaitables[0]["id"], "cooldown");
    assert!(
        awaitables[0].get("deadline").is_some(),
        "timer awaitable should derive its fire deadline: {body}"
    );
}

/// Serving the report performs zero writes: no events appended, the execution
/// row byte-identical.
#[tokio::test]
async fn report_is_read_only_zero_writes() {
    #[derive(diesel::QueryableByName)]
    struct Snapshot {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        execution_row: Value,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        events: i64,
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        task_rows: Value,
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        timer_rows: Value,
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        signal_rows: Value,
    }

    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(&pool);

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        Value::Null,
        vec![
            started_event(Value::Null),
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "charge_card".into(),
                input: json!({"amount": 42}),
                queue: "payments".into(),
            },
        ],
    )
    .await;

    // Seed side-table rows too (test review T4): the deadline-enrichment query
    // reads harvest_task_queue, and a mutation of ANY side table would violate
    // the read-only contract even if the execution row survived.
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, workflow_exec_id, activity_name, \
              input, state, priority, attempt, max_attempts, scheduled_at) \
             VALUES ($1, 'payments', 'activity', $2, 'charge_card', \
                     '{}'::jsonb, 'PENDING', 0, 1, 3, NOW())",
        )
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("seed task row");
        diesel::sql_query(
            "INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired) \
             VALUES ($1, $2, 'cooldown', NOW() + interval '300 seconds', false)",
        )
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("seed timer row");
        diesel::sql_query(
            "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, received_at, consumed) \
             VALUES ($1, $2, 'approval', 'null'::jsonb, NOW(), false)",
        )
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("seed signal row");
    }

    let snapshot_sql = "SELECT to_jsonb(w.*) AS execution_row, \
         (SELECT COUNT(*) FROM harvest_events e WHERE e.workflow_exec_id = w.id) AS events, \
         (SELECT COALESCE(jsonb_agg(to_jsonb(t.*) ORDER BY t.id), '[]'::jsonb) \
            FROM harvest_task_queue t WHERE t.workflow_exec_id = w.id) AS task_rows, \
         (SELECT COALESCE(jsonb_agg(to_jsonb(ti.*) ORDER BY ti.id), '[]'::jsonb) \
            FROM harvest_timers ti WHERE ti.workflow_exec_id = w.id) AS timer_rows, \
         (SELECT COALESCE(jsonb_agg(to_jsonb(s.*) ORDER BY s.id), '[]'::jsonb) \
            FROM harvest_signals s WHERE s.workflow_exec_id = w.id) AS signal_rows \
         FROM harvest_workflow_executions w WHERE w.id = $1";
    let before: Snapshot = {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(snapshot_sql)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(&mut conn)
            .await
            .expect("snapshot before")
    };

    let (status, _body) = get_json(&app, &format!("/workflows/{exec_id}/awaitables"), true).await;
    assert_eq!(status, StatusCode::OK);

    let after: Snapshot = {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(snapshot_sql)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(&mut conn)
            .await
            .expect("snapshot after")
    };
    assert_eq!(
        before.execution_row, after.execution_row,
        "execution row must be untouched"
    );
    assert_eq!(before.events, after.events, "no events may be appended");
    assert_eq!(before.task_rows, after.task_rows, "task rows untouched");
    assert_eq!(before.timer_rows, after.timer_rows, "timer rows untouched");
    assert_eq!(
        before.signal_rows, after.signal_rows,
        "signal rows untouched"
    );
}

/// Unknown execution → 404; malformed id → 400. (The unauthenticated 401
/// posture is pinned by the no-DB `security.rs` test
/// `eris_unauthenticated_awaitables_is_blocked` — this harness runs behind an
/// embedder admin-auth boundary, which makes the per-route gate a no-op.)
#[tokio::test]
async fn error_statuses_are_stable() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(&pool);

    let unknown = ExecutionId::new();
    let (status, _body) = get_json(&app, &format!("/workflows/{unknown}/awaitables"), true).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _body) = get_json(&app, "/workflows/not-a-uuid/awaitables", true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The Vantage UI blocked-on panel is re-sourced from the same report: a
/// parked signal wait — invisible to every side table — is rendered with the
/// awaited signal's name.
#[tokio::test]
async fn ui_blocked_on_panel_shows_awaited_unsent_signal() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let ui_app = build_ui_app(&pool);

    let exec_id = seed_execution(
        &pool,
        "signal_wait_wf",
        "RUNNING",
        Value::Null,
        vec![started_event(Value::Null)],
    )
    .await;

    let (status, html) = fetch_html(&ui_app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Waiting on (replay-derived)"),
        "detail page should render the awaitables section"
    );
    assert!(
        html.contains("approval"),
        "the awaited-but-unsent signal name must be visible"
    );
}

/// T9: the UI panel renders a pending child workflow (name + child exec id) —
/// and a TERMINAL execution renders no awaitables section at all.
#[tokio::test]
async fn ui_blocked_on_panel_renders_children_and_skips_terminal_runs() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let ui_app = build_ui_app(&pool);

    let child_id = ExecutionId::new();
    let exec_id = seed_execution(
        &pool,
        "child_wf",
        "RUNNING",
        Value::Null,
        vec![
            started_event(Value::Null),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "fulfillment_flow".into(),
                input: json!({"order": 7}),
            },
        ],
    )
    .await;
    let (status, html) = fetch_html(&ui_app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Waiting on (replay-derived)"),
        "detail page should render the awaitables section"
    );
    assert!(
        html.contains("fulfillment_flow"),
        "the pending child's workflow name must be visible"
    );
    assert!(
        html.contains(&child_id.to_string()),
        "the pending child's exec id must be visible"
    );

    // Negative control: a terminal run renders NO awaitables section.
    let terminal = seed_execution(
        &pool,
        "signal_wait_wf",
        "COMPLETED",
        Value::Null,
        vec![started_event(Value::Null)],
    )
    .await;
    let (status, html) = fetch_html(&ui_app, &format!("/workflows/{terminal}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !html.contains("Waiting on (replay-derived)"),
        "a terminal run must not render the awaitables section"
    );
}
