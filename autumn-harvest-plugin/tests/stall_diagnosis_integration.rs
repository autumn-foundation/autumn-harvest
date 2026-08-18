//! Integration tests for per-execution stall diagnosis (issue #809):
//! `GET /workflows/{id}/diagnose`, exercised end-to-end over the real router
//! against Postgres.
//!
//! Each test below maps to one of the issue's acceptance criteria:
//!
//! - AC1: 200 + `health` + discriminated `blocked_on` + `last_event_age_seconds`.
//! - AC3: a pending activity on a queue with ZERO live worker heartbeats
//!   resolves to `activity_no_worker { queue }` with `health = stalled` — the
//!   issue's headline "100% of the time it holds" condition.
//! - AC4: a future durable timer resolves to `sleeping_timer { fires_at }` with
//!   `health = healthy` REGARDLESS of how old the last event is.
//! - AC5: an open circuit resolves to `activity_circuit_open`; a backing-off
//!   task resolves to `activity_retrying` carrying `attempt` / `last_error` /
//!   `next_attempt_at` (the task row's own `scheduled_at`).
//! - AC6: a RUNNING execution with no pending work of any kind resolves to
//!   `no_pending_work` with `health = stalled`.
//! - AC7: a terminal execution returns `health = terminal` + `terminal_outcome`
//!   and NO `blocked_on`; an unknown id returns 404; serving the report
//!   performs zero writes and never advances a circuit breaker's phase.
//! - AC2: the remaining `blocked_on` variants: `awaiting_signal`
//!   (replay-derived), `pending_child`, `paused`, plus the fan-out worst-of
//!   rule.
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
use std::time::Duration;

use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::policy::CircuitBreakerPolicy;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
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

/// Executes one activity — parked while the activity task row is open.
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

/// Sleeps on a durable timer.
fn timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("long_sleep", 86_400)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Parks on `wait_for_signal("approval")` — visible only to the replay drive.
fn signal_wait_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.wait_for_signal("approval")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Spawns and awaits one child workflow.
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

/// A workflow whose replay reaches terminal — used for the AC6 "no pending
/// work of any kind" shape (a lost task / executor-loss indicator).
fn completes_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!("done")) })
}

fn noop_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
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

/// `charge_card` carries a circuit-breaker policy so the registry tracks it and
/// the AC5 test can force it open.
fn breaker_activity() -> ActivityInfo {
    ActivityInfo {
        name: "charge_card",
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_queue: Some("payments"),
        max_concurrent: None,
        concurrency_key: None,
        default_schedule_to_close: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: Some(CircuitBreakerPolicy::new(
            3,
            Duration::from_secs(60),
            Duration::from_secs(30),
        )),
        requires: None,
        handler: noop_activity,
    }
}

/// A plain activity with NO circuit breaker. Required for the rate-limit tests:
/// a breaker-tracked activity enforces its rate limit at DISPATCH (issue #369),
/// so its bucket is deliberately never consulted at claim time.
fn plain_activity() -> ActivityInfo {
    ActivityInfo {
        name: "send_email",
        default_queue: Some("email"),
        circuit_breaker: None,
        ..breaker_activity()
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

    let db = format!("harvest_diagnose_{}", Uuid::new_v4().simple());
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

/// Build the API state, also handing back the registry so a test can reach the
/// SAME shared circuit-breaker registry the endpoint reads (`runtime()` is
/// crate-private, so the handle is threaded out here instead).
fn build_api_state_with_registry(
    pool: &DbPool,
    admin_boundary: bool,
) -> (HarvestApiState, Arc<HandlerRegistry>) {
    let registry = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("activity_wf", activity_workflow),
            wf_info("timer_wf", timer_workflow),
            wf_info("signal_wait_wf", signal_wait_workflow),
            wf_info("child_wf", child_parent_workflow),
            wf_info("completes_wf", completes_workflow),
        ],
        vec![breaker_activity(), plain_activity()],
    ));
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(admin_boundary);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("diagnose-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    (api_state, registry)
}

fn build_api_state(pool: &DbPool) -> HarvestApiState {
    build_api_state_with_registry(pool, true).0
}

fn build_api_app(state: HarvestApiState) -> axum::Router {
    harvest_api_router(state).with_state(AppState::for_test().with_profile("test"))
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

async fn diagnose(app: &axum::Router, exec_id: ExecutionId) -> Value {
    let (status, body) = get_json(app, &format!("/workflows/{exec_id}/diagnose"), true).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    body
}

/// Seed an execution row plus history events, returning the id.
async fn seed_execution(
    pool: &DbPool,
    workflow_name: &str,
    state: &str,
    events: Vec<WorkflowEvent>,
) -> ExecutionId {
    let mut conn = pool.get().await.expect("pooled conn");
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, $2, $3, 0, 'null'::jsonb, 'default', $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Text, _>(state)
    .execute(&mut conn)
    .await
    .expect("seed execution");
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("seed history");
    exec_id
}

/// Backdate every recorded event of `exec_id` by `interval` (raw SQL interval
/// literal). `harvest_events.timestamp` defaults to `NOW()` on insert, so the
/// `WorkflowEvent`'s own timestamp field alone cannot make a fixture look old.
async fn backdate_events(pool: &DbPool, exec_id: ExecutionId, interval: &str) {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(format!(
        "UPDATE harvest_events SET timestamp = NOW() - INTERVAL '{interval}' \
         WHERE workflow_exec_id = $1"
    ))
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("backdate events");
}

/// Seed a pending activity task row. `scheduled_at_sql` is raw SQL so a test can
/// place the row in the future (backoff) or the past (eligible).
#[allow(clippy::too_many_arguments)]
async fn seed_activity_task(
    pool: &DbPool,
    exec_id: ExecutionId,
    activity_name: &str,
    queue: &str,
    state: &str,
    attempt: i32,
    error: Option<&str>,
    scheduled_at_sql: &str,
) {
    let mut conn = pool.get().await.expect("pooled conn");
    let sql = format!(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, \
          input, state, priority, attempt, max_attempts, scheduled_at, error) \
         VALUES ($1, $2, 'activity', $3, $4, $5, '{{}}'::jsonb, $6, 0, $7, 3, {scheduled_at_sql}, $8)"
    );
    diesel::sql_query(sql)
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Text, _>(queue)
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Text, _>(activity_name)
        .bind::<diesel::sql_types::Uuid, _>(ActivityExecId::new().as_uuid())
        .bind::<diesel::sql_types::Text, _>(state)
        .bind::<diesel::sql_types::Integer, _>(attempt)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(error)
        .execute(&mut conn)
        .await
        .expect("seed task");
}

/// Seed the run's OWN workflow task row (`task_type = 'workflow'`).
///
/// `worker_id` non-`None` means a worker holds the claim (the handler is
/// executing); `RUNNING` with `None` is the *parked* shape.
async fn seed_workflow_task(
    pool: &DbPool,
    exec_id: ExecutionId,
    queue: &str,
    state: &str,
    worker_id: Option<&str>,
    scheduled_at_sql: &str,
) {
    let mut conn = pool.get().await.expect("pooled conn");
    let sql = format!(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, priority, attempt, \
          max_attempts, scheduled_at, worker_id) \
         VALUES ($1, $2, 'workflow', $3, '{{}}'::jsonb, $4, 0, 1, 3, {scheduled_at_sql}, $5)"
    );
    diesel::sql_query(sql)
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Text, _>(queue)
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Text, _>(state)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(worker_id)
        .execute(&mut conn)
        .await
        .expect("seed workflow task");
}

/// Register a live worker covering `queue` on shard 0.
async fn seed_live_worker(pool: &DbPool, worker_id: &str, queue: &str) {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, last_heartbeat_at, status, queues, shard_assignments, max_concurrency, host) \
         VALUES ($1, NOW(), 'Active', $2, '[0]'::jsonb, 10, 'test-host') \
         ON CONFLICT (worker_id) DO NOTHING",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Jsonb, _>(json!([queue]))
    .execute(&mut conn)
    .await
    .expect("seed worker");
}

/// Seed a pending activity task carrying a rate-limit and/or concurrency key.
#[allow(clippy::too_many_arguments)]
async fn seed_keyed_activity_task(
    pool: &DbPool,
    exec_id: ExecutionId,
    activity_name: &str,
    queue: &str,
    rate_limit_key: Option<&str>,
    concurrency_key: Option<&str>,
    concurrency_cap: Option<i32>,
) {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, \
          input, state, priority, attempt, max_attempts, scheduled_at, \
          rate_limit_key, concurrency_key, concurrency_cap) \
         VALUES ($1, $2, 'activity', $3, $4, $5, '{}'::jsonb, 'PENDING', 0, 1, 3, \
                 NOW() - INTERVAL '1 minute', $6, $7, $8)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Text, _>(queue)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(activity_name)
    .bind::<diesel::sql_types::Uuid, _>(ActivityExecId::new().as_uuid())
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(rate_limit_key)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(concurrency_key)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(concurrency_cap)
    .execute(&mut conn)
    .await
    .expect("seed keyed task");
}

/// Seed a RUNNING task holding `concurrency_key`, so the cap is saturated.
async fn seed_running_concurrency_holder(pool: &DbPool, queue: &str, concurrency_key: &str) {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, activity_name, activity_id, input, state, \
          priority, attempt, max_attempts, scheduled_at, worker_id, concurrency_key) \
         VALUES ($1, $2, 'activity', 'send_email', $3, '{}'::jsonb, 'RUNNING', 0, 1, 3, \
                 NOW() - INTERVAL '1 minute', 'holder-worker', $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Text, _>(queue)
    .bind::<diesel::sql_types::Uuid, _>(ActivityExecId::new().as_uuid())
    .bind::<diesel::sql_types::Text, _>(concurrency_key)
    .execute(&mut conn)
    .await
    .expect("seed concurrency holder");
}

/// Drain a rate-limit bucket to zero tokens so the claim gate would refuse it.
async fn seed_drained_rate_limit_bucket(pool: &DbPool, key: &str) {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets \
         (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, 0.001, 1, 0, NOW()) \
         ON CONFLICT (key) DO UPDATE SET tokens = 0, last_refilled_at = NOW()",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(&mut conn)
    .await
    .expect("seed bucket");
}

fn started_event() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        // Deliberately ancient: proves `health` is never derived from event age.
        timestamp: Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn scheduled_activity_event() -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "charge_card".into(),
        input: json!({"amount": 42}),
        queue: "payments".into(),
    }
}

/// The `blocked_on.type` discriminator.
fn kind(body: &Value) -> &str {
    body["blocked_on"]["type"]
        .as_str()
        .unwrap_or_else(|| panic!("blocked_on.type missing: {body}"))
}

// ── AC3: zero live workers on the queue ────────────────────────────────────

/// The issue's headline condition. A pending activity whose queue has NO live
/// worker heartbeat must surface `activity_no_worker` — never a bare
/// `pending_activity` bucket — and be classified `stalled`.
#[tokio::test]
async fn ac3_pending_activity_with_no_live_worker_is_stalled_and_names_the_queue() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    // Eligible right now, and deliberately NO harvest_workers row.
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        0,
        None,
        "NOW()",
    )
    .await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_no_worker", "body: {body}");
    assert_eq!(body["blocked_on"]["queue"], "payments");
    assert_eq!(body["health"], "stalled", "body: {body}");
    assert!(
        body["contributing_reason_codes"]
            .as_array()
            .expect("reason codes")
            .iter()
            .any(|r| r == "no_live_worker"),
        "no_live_worker must appear in contributing_reason_codes: {body}"
    );
}

/// Control for AC3: the SAME shape with a live worker polling that queue is no
/// longer `activity_no_worker` — so the verdict is driven by fleet coverage,
/// not by the mere presence of a pending row.
#[tokio::test]
async fn ac3_control_live_worker_removes_the_no_worker_verdict() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        0,
        None,
        "NOW()",
    )
    .await;
    seed_live_worker(&pool, "worker-payments", "payments").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "healthy_in_progress", "body: {body}");
    assert_eq!(body["health"], "healthy", "body: {body}");
}

// ── AC4: a long durable sleep is not a stall ───────────────────────────────

/// A future timer resolves to `sleeping_timer` with `health = healthy` even
/// though the last recorded event is years old.
#[tokio::test]
async fn ac4_future_timer_is_healthy_regardless_of_event_age() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "timer_wf",
        "RUNNING",
        vec![
            started_event(),
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("long_sleep"),
                duration_secs: 86_400,
            },
        ],
    )
    .await;
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired) \
             VALUES ($1, $2, 'long_sleep', NOW() + INTERVAL '1 day', false)",
        )
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("seed timer");
    }
    // Two years old: proves `health` is never derived from event age.
    backdate_events(&pool, exec_id, "730 days").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "sleeping_timer", "body: {body}");
    assert_eq!(body["health"], "healthy", "body: {body}");
    assert!(
        body["blocked_on"]["fires_at"].is_string(),
        "sleeping_timer must carry fires_at: {body}"
    );
    // The falsifiable half: the event age is enormous, and health is still healthy.
    let age = body["last_event_age_seconds"]
        .as_f64()
        .expect("last_event_age_seconds present");
    assert!(
        age > 86_400.0 * 365.0,
        "fixture must have an ancient last event to prove age does not drive health: {age}"
    );
}

// ── AC5: circuit open, and retry backoff ───────────────────────────────────

/// A FORCE-opened circuit resolves to `activity_circuit_open` and outranks the
/// retry verdict — and deliberately carries NO `cooldown_until`: `force_open`
/// sets `forced_open`, so `snapshot_of` reports no probe deadline because no
/// probe is admitted on any timer. Recovery needs an explicit `force-close`,
/// and the absent field is what tells an operator that.
#[tokio::test]
async fn ac5_forced_open_circuit_reports_circuit_open_without_a_cooldown() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let (api_state, registry) = build_api_state_with_registry(&pool, true);
    // Force the breaker open through the SAME shared registry the endpoint reads.
    registry
        .circuit_breakers()
        .force_open("charge_card", std::time::Instant::now());
    let app = build_api_app(api_state);

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        1,
        Some("gateway 503"),
        "NOW() + INTERVAL '10 minutes'",
    )
    .await;
    // A live worker, so `activity_no_worker` cannot be what we are observing.
    seed_live_worker(&pool, "worker-payments", "payments").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_circuit_open", "body: {body}");
    assert_eq!(body["blocked_on"]["activity_name"], "charge_card");
    assert_eq!(body["health"], "stalled", "body: {body}");
    assert!(
        body["contributing_reason_codes"]
            .as_array()
            .expect("reason codes")
            .iter()
            .any(|r| r == "circuit_open"),
        "circuit_open must appear in contributing_reason_codes: {body}"
    );
    assert!(
        body["blocked_on"].get("cooldown_until").is_none(),
        "a force-opened breaker admits no timed probe, so it must advertise no \
         cooldown: {body}"
    );
}

/// An ORGANICALLY tripped circuit (three failures inside the window, so
/// `forced_open` is false) resolves to `activity_circuit_open` WITH a derived
/// `cooldown_until` — the AC5 clause the forced-open fixture structurally cannot
/// produce, and the only end-to-end exercise of the
/// `time_until_probe_secs` -> `DateTime` derivation.
#[tokio::test]
async fn ac5_organically_tripped_circuit_reports_a_derived_cooldown_until() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let (api_state, registry) = build_api_state_with_registry(&pool, true);
    // `breaker_activity()` trips at 3 failures inside a 60s window, cooldown 30s.
    let now = std::time::Instant::now();
    for _ in 0..3 {
        registry
            .circuit_breakers()
            .on_external_failure("charge_card", now);
    }
    let app = build_api_app(api_state);

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        1,
        Some("gateway 503"),
        "NOW() - INTERVAL '1 minute'",
    )
    .await;
    seed_live_worker(&pool, "worker-payments", "payments").await;

    let before = Utc::now();
    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_circuit_open", "body: {body}");
    let cooldown = body["blocked_on"]["cooldown_until"]
        .as_str()
        .unwrap_or_else(|| panic!("an organically tripped breaker must carry a cooldown: {body}"));
    let parsed = chrono::DateTime::parse_from_rfc3339(cooldown)
        .unwrap_or_else(|e| panic!("cooldown_until must be RFC3339 ({e}): {cooldown}"))
        .with_timezone(&Utc);
    assert!(
        parsed > before && parsed <= before + chrono::Duration::seconds(31),
        "cooldown must land inside the policy's 30s window: {parsed} vs {before}"
    );
}

/// A backing-off task (future `scheduled_at`) on a covered queue with a closed
/// circuit resolves to `activity_retrying` carrying `attempt` / `last_error` /
/// `next_attempt_at`.
#[tokio::test]
async fn ac5_backing_off_task_reports_retrying_with_attempt_error_and_next_attempt() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        2,
        Some("connection reset"),
        "NOW() + INTERVAL '5 minutes'",
    )
    .await;
    seed_live_worker(&pool, "worker-payments", "payments").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_retrying", "body: {body}");
    assert_eq!(body["blocked_on"]["activity_name"], "charge_card");
    assert_eq!(body["blocked_on"]["attempt"], 2);
    assert_eq!(body["blocked_on"]["last_error"], "connection reset");
    assert!(
        body["blocked_on"]["next_attempt_at"].is_string(),
        "activity_retrying must carry next_attempt_at from the task row: {body}"
    );
    // Backing off is a healthy wait: the engine WILL retry it.
    assert_eq!(body["health"], "healthy", "body: {body}");
}

/// A not-yet-due task with NO recorded error is a dispatcher deferral, not a
/// retry. `queue::defer_rate_limited_task` pushes `scheduled_at` forward and
/// decrements the claim-time attempt back down without recording a failure
/// ("a rate-limit deferral is not an attempt"); the session-capacity (#606) and
/// capability-miss (#804) paths reuse the same clean continuation. Reporting
/// those as `activity_retrying` would send an operator hunting a failure that
/// never happened.
#[tokio::test]
async fn ac2_not_due_task_without_an_error_reports_deferred_not_retrying() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        // The deferral restored the attempt, and no error was ever recorded.
        1,
        None,
        "NOW() + INTERVAL '5 minutes'",
    )
    .await;
    seed_live_worker(&pool, "worker-payments", "payments").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_deferred", "body: {body}");
    assert_eq!(body["blocked_on"]["activity_name"], "charge_card");
    assert!(
        body["blocked_on"]["next_attempt_at"].is_string(),
        "activity_deferred must carry next_attempt_at from the task row: {body}"
    );
    // A clean deferral self-heals, so it is not a stall.
    assert_eq!(body["health"], "healthy", "body: {body}");
    // And it must NOT fabricate retry facts.
    assert!(
        body["blocked_on"]["last_error"].is_null(),
        "a deferral has no failure to report: {body}"
    );
}

// ── AC6: no pending work of any kind ───────────────────────────────────────

/// A RUNNING execution with no task rows, no timers, no children and no
/// handoffs is the executor-loss / lost-task indicator: `no_pending_work`,
/// `health = stalled`.
#[tokio::test]
async fn ac6_running_with_no_pending_work_is_stalled() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "completes_wf", "RUNNING", vec![started_event()]).await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "no_pending_work", "body: {body}");
    assert_eq!(body["health"], "stalled", "body: {body}");
    assert!(
        body["contributing_reason_codes"]
            .as_array()
            .expect("reason codes")
            .is_empty(),
        "a non-activity verdict has no task row to explain: {body}"
    );
}

// ── AC7: terminal, 404, and zero writes ────────────────────────────────────

/// A terminal execution reports `health = terminal` with its outcome and NO
/// `blocked_on` — there is nothing to diagnose.
#[tokio::test]
async fn ac7_terminal_execution_reports_terminal_outcome_and_no_blocked_on() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "completes_wf", "FAILED", vec![started_event()]).await;
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET error = 'boom', completed_at = NOW() \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("seal execution");
    }

    let body = diagnose(&app, exec_id).await;
    assert_eq!(body["health"], "terminal", "body: {body}");
    assert!(
        body.get("blocked_on").is_none(),
        "a terminal execution must carry no blocked_on: {body}"
    );
    assert_eq!(body["terminal_outcome"]["state"], "FAILED");
    assert_eq!(body["terminal_outcome"]["error"], "boom");
    assert!(body["terminal_outcome"]["completed_at"].is_string());
}

/// An unknown execution id returns 404; a malformed one returns 400.
#[tokio::test]
async fn ac7_unknown_id_returns_404_and_malformed_id_returns_400() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let missing = ExecutionId::new();
    let (status, _) = get_json(&app, &format!("/workflows/{missing}/diagnose"), true).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = get_json(&app, "/workflows/not-a-uuid/diagnose", true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Serving the report appends no events, mutates no task-queue row, and does
/// not advance the circuit breaker's phase — safe to call repeatedly.
#[tokio::test]
async fn ac7_diagnosis_performs_no_writes_and_never_advances_a_breaker() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let (api_state, registry) = build_api_state_with_registry(&pool, true);
    let breakers = registry.circuit_breakers();
    let app = build_api_app(api_state);

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        0,
        None,
        "NOW()",
    )
    .await;

    let before = snapshot_state(&pool, exec_id).await;
    let phase_before = breaker_phase(&breakers, "charge_card");

    // Call it three times — the endpoint advertises "safe to call repeatedly".
    let first = diagnose(&app, exec_id).await;
    let second = diagnose(&app, exec_id).await;
    let third = diagnose(&app, exec_id).await;

    let after = snapshot_state(&pool, exec_id).await;
    assert_eq!(before, after, "diagnosing must perform zero writes");
    assert_eq!(
        phase_before,
        breaker_phase(&breakers, "charge_card"),
        "consulting the registry must never advance a breaker's phase"
    );
    // The verdict itself is stable across repeated calls.
    assert_eq!(kind(&first), kind(&second));
    assert_eq!(kind(&second), kind(&third));
}

/// `(event count, execution row fingerprint, task-queue fingerprint)` — every
/// column a write would have to move.
async fn snapshot_state(pool: &DbPool, exec_id: ExecutionId) -> (i64, String, String) {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        events: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        execution: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        tasks: String,
    }
    let mut conn = pool.get().await.expect("pooled conn");
    let row: Row = diesel::sql_query(
        "SELECT \
           (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = $1) AS events, \
           (SELECT to_jsonb(w.*)::text FROM harvest_workflow_executions w WHERE w.id = $1) AS execution, \
           (SELECT COALESCE(string_agg(t.id::text || ':' || t.state || ':' || t.attempt::text || ':' \
                                       || t.scheduled_at::text, ',' ORDER BY t.id), '') \
              FROM harvest_task_queue t WHERE t.workflow_exec_id = $1) AS tasks",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("snapshot");
    (row.events, row.execution, row.tasks)
}

fn breaker_phase(
    breakers: &autumn_harvest::circuit_breaker::CircuitBreakerRegistry,
    activity: &str,
) -> String {
    breakers
        .list(std::time::Instant::now())
        .into_iter()
        .find(|s| s.activity_name == activity)
        .map_or_else(|| "absent".to_string(), |s| s.state.to_string())
}

// ── AC2: the remaining discriminated variants ──────────────────────────────

/// An awaited-but-UNSENT signal is invisible to every side table — only the
/// replay drive can see it. It must surface as `awaiting_signal` naming the
/// signal, with `health = blocked_external` and `wait_set = "replayed"`.
#[tokio::test]
async fn ac2_awaited_unsent_signal_is_named_via_the_replay_drive() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "signal_wait_wf", "RUNNING", vec![started_event()]).await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "awaiting_signal", "body: {body}");
    assert_eq!(body["blocked_on"]["signal_name"], "approval");
    assert_eq!(body["health"], "blocked_external", "body: {body}");
    assert_eq!(
        body["wait_set"], "replayed",
        "the replay drive must have been consulted: {body}"
    );
}

/// A pending child workflow surfaces as `pending_child` with the child's id and
/// state, and is a healthy wait.
#[tokio::test]
async fn ac2_pending_child_reports_child_exec_id_and_state() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let parent = seed_execution(&pool, "child_wf", "RUNNING", vec![started_event()]).await;
    let child = seed_execution(&pool, "completes_wf", "RUNNING", vec![started_event()]).await;
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query("UPDATE harvest_workflow_executions SET parent_id = $1 WHERE id = $2")
            .bind::<diesel::sql_types::Uuid, _>(parent.as_uuid())
            .bind::<diesel::sql_types::Uuid, _>(child.as_uuid())
            .execute(&mut conn)
            .await
            .expect("link child");
    }

    let body = diagnose(&app, parent).await;
    assert_eq!(kind(&body), "pending_child", "body: {body}");
    assert_eq!(body["blocked_on"]["child_exec_id"], child.to_string());
    assert_eq!(body["blocked_on"]["child_state"], "RUNNING");
    assert_eq!(body["health"], "healthy", "body: {body}");
    // A child outranks a signal, so the replay drive is deliberately skipped.
    assert_eq!(body["wait_set"], "not_consulted", "body: {body}");
}

/// An operator-paused execution reports `paused` with the actor and since —
/// deliberate operator intent, classified `blocked_external`, and it outranks
/// every pending-work category.
#[tokio::test]
async fn ac2_paused_execution_reports_paused_with_actor_and_since() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "PAUSED",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        0,
        None,
        "NOW()",
    )
    .await;
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
             SET paused_at = NOW(), pause_actor = 'ops@example.com', pause_reason = 'incident' \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("pause execution");
    }

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "paused", "body: {body}");
    assert_eq!(body["blocked_on"]["actor"], "ops@example.com");
    assert!(body["blocked_on"]["since"].is_string(), "body: {body}");
    assert_eq!(body["health"], "blocked_external", "body: {body}");
}

/// The fan-out worst-of rule: one wedged slot among many healthy ones must
/// still surface the wedged one. A wide fan-out where 3 of 4 activities sit on
/// a covered queue and 1 sits on an uncovered queue reports
/// `activity_no_worker` naming the uncovered queue.
#[tokio::test]
async fn ac2_fan_out_reports_the_worst_slot_not_the_healthy_majority() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    for _ in 0..3 {
        seed_activity_task(
            &pool,
            exec_id,
            "charge_card",
            "payments",
            "PENDING",
            0,
            None,
            "NOW()",
        )
        .await;
    }
    // The single wedged slot, on a queue nobody polls.
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "typo-queue",
        "PENDING",
        0,
        None,
        "NOW()",
    )
    .await;
    seed_live_worker(&pool, "worker-payments", "payments").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_no_worker", "body: {body}");
    assert_eq!(
        body["blocked_on"]["queue"], "typo-queue",
        "the WORST slot must win, not the healthy majority: {body}"
    );
    assert_eq!(body["health"], "stalled", "body: {body}");
}

/// Admin-gated: with no admin boundary and no session on the request, the
/// admin route layer rejects before any handler logic runs.
#[tokio::test]
async fn diagnose_requires_admin() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state_with_registry(&pool, false).0);

    let exec_id = seed_execution(&pool, "completes_wf", "RUNNING", vec![started_event()]).await;
    let (status, _) = get_json(&app, &format!("/workflows/{exec_id}/diagnose"), false).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ── AC2 wire coverage for the remaining variants (review round) ────────────

/// A drained rate-limit bucket on a NON-breaker activity resolves to
/// `activity_rate_limited`. Exercises `saturated_rate_limit_keys` end to end —
/// the derivation the pure classifier tests hand-feed as a bool.
#[tokio::test]
async fn ac2_saturated_rate_limit_bucket_reports_activity_rate_limited() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_drained_rate_limit_bucket(&pool, "tenant:acme").await;
    seed_keyed_activity_task(
        &pool,
        exec_id,
        "send_email",
        "email",
        Some("tenant:acme"),
        None,
        None,
    )
    .await;
    seed_live_worker(&pool, "worker-email", "email").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_rate_limited", "body: {body}");
    assert_eq!(body["blocked_on"]["key"], "tenant:acme");
    assert_eq!(body["health"], "healthy", "a bucket refills: {body}");
    assert!(
        body["contributing_reason_codes"]
            .as_array()
            .expect("reasons")
            .iter()
            .any(|r| r == "rate_limit_exhausted"),
        "body: {body}"
    );
}

/// A rate-limit key with NO bucket row at all must be treated as saturated:
/// `claim_task`'s gate is `EXISTS(bucket >= 1.0)`, so a missing row makes the
/// task permanently unclaimable. Reporting it healthy would be the exact
/// false-negative this endpoint exists to prevent.
#[tokio::test]
async fn ac2_missing_rate_limit_bucket_is_treated_as_saturated_not_healthy() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    // Deliberately NO `seed_drained_rate_limit_bucket` call.
    seed_keyed_activity_task(
        &pool,
        exec_id,
        "send_email",
        "email",
        Some("tenant:ghost"),
        None,
        None,
    )
    .await;
    seed_live_worker(&pool, "worker-email", "email").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(
        kind(&body),
        "activity_rate_limited",
        "a key with no bucket row can never be claimed: {body}"
    );
}

/// A saturated per-key concurrency cap resolves to
/// `activity_concurrency_deferred`. Exercises `running_counts_for_concurrency_keys`.
#[tokio::test]
async fn ac2_saturated_concurrency_key_reports_activity_concurrency_deferred() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_running_concurrency_holder(&pool, "email", "tenant:acme").await;
    seed_keyed_activity_task(
        &pool,
        exec_id,
        "send_email",
        "email",
        None,
        Some("tenant:acme"),
        Some(1),
    )
    .await;
    seed_live_worker(&pool, "worker-email", "email").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_concurrency_deferred", "body: {body}");
    assert_eq!(body["blocked_on"]["key"], "tenant:acme");
    assert_eq!(body["health"], "healthy", "siblings finish: {body}");
}

/// An operator-paused queue resolves to `activity_queue_paused` — the highest
/// rank in the ladder, and the variant that keeps a paused queue from reading
/// as `healthy_in_progress`.
#[tokio::test]
async fn ac2_paused_queue_reports_activity_queue_paused() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "send_email",
        "email",
        "PENDING",
        1,
        None,
        "NOW() - INTERVAL '1 minute'",
    )
    .await;
    seed_live_worker(&pool, "worker-email", "email").await;
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "INSERT INTO harvest_queue_pauses (queue_name, reason, paused_by) \
             VALUES ('email', 'incident-42', 'oncall')",
        )
        .execute(&mut conn)
        .await
        .expect("pause queue");
    }

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_queue_paused", "body: {body}");
    assert_eq!(body["blocked_on"]["queue"], "email");
    assert_eq!(body["health"], "blocked_external", "body: {body}");
}

/// A pending external handoff resolves to `awaiting_external_handoff{token}`.
#[tokio::test]
async fn ac2_pending_external_handoff_reports_the_token() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "activity_wf", "RUNNING", vec![started_event()]).await;
    let token = Uuid::new_v4();
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "INSERT INTO harvest_external_tasks \
             (id, token, workflow_exec_id, activity_id, name, queue, state, \
              schedule_to_close_at, schedule_to_close_secs) \
             VALUES ($1, $2, $3, $4, 'await_partner', 'default', 'PENDING', \
                     NOW() + INTERVAL '1 hour', 3600)",
        )
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Uuid, _>(token)
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Uuid, _>(ActivityExecId::new().as_uuid())
        .execute(&mut conn)
        .await
        .expect("seed handoff");
    }

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "awaiting_external_handoff", "body: {body}");
    assert_eq!(body["blocked_on"]["token"], token.to_string());
    assert_eq!(body["health"], "blocked_external", "body: {body}");
    assert_eq!(
        body["wait_set"], "not_consulted",
        "a handoff outranks signals, so no replay is paid for: {body}"
    );
}

/// A claimed (`RUNNING`) row whose queue has no live poller is a poison-pill
/// orphan (issue #367), NOT healthy work. The reclaimer heals it within a poll
/// interval except when the whole fleet is down — the flagship stall.
#[tokio::test]
async fn ac2_orphaned_running_task_is_not_reported_healthy() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "RUNNING",
        1,
        None,
        "NOW() - INTERVAL '1 minute'",
    )
    .await;
    // Deliberately NO live worker on `payments`.

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_no_worker", "body: {body}");
    assert_eq!(body["health"], "stalled", "body: {body}");
}

/// An unfired durable timer long past its deadline is a lost-task wedge, not a
/// healthy sleep: timers fire only when a worker claims the owning workflow
/// task, so a missed deadline means that never happened.
#[tokio::test]
async fn ac2_overdue_unfired_timer_reports_timer_overdue() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "timer_wf", "RUNNING", vec![started_event()]).await;
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired) \
             VALUES ($1, $2, 'long_sleep', NOW() - INTERVAL '2 hours', false)",
        )
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("seed overdue timer");
    }

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "timer_overdue", "body: {body}");
    assert_eq!(body["health"], "stalled", "body: {body}");
    assert!(
        body["blocked_on"]["overdue_by_seconds"]
            .as_i64()
            .is_some_and(|s| s >= 7_000),
        "body: {body}"
    );
}

/// The multi-reason list is a UNION across rows, not the winner's reasons: a
/// lower-ranked sibling's cause must not vanish behind a higher-ranked one.
#[tokio::test]
async fn ac2_contributing_reasons_union_across_a_fan_out() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    // Slot A: paused queue (rank 6, so it wins `blocked_on`), with a poller.
    seed_activity_task(
        &pool,
        exec_id,
        "send_email",
        "email",
        "PENDING",
        1,
        None,
        "NOW() - INTERVAL '1 minute'",
    )
    .await;
    seed_live_worker(&pool, "worker-email", "email").await;
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "INSERT INTO harvest_queue_pauses (queue_name, reason, paused_by) \
             VALUES ('email', 'incident-42', 'oncall')",
        )
        .execute(&mut conn)
        .await
        .expect("pause queue");
    }
    // Slot B: a queue with NO live poller (rank 5).
    seed_activity_task(
        &pool,
        exec_id,
        "charge_card",
        "payments",
        "PENDING",
        1,
        None,
        "NOW() - INTERVAL '1 minute'",
    )
    .await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_queue_paused", "body: {body}");
    let reasons = body["contributing_reason_codes"]
        .as_array()
        .expect("reasons");
    assert!(reasons.iter().any(|r| r == "queue_paused"), "body: {body}");
    assert!(
        reasons.iter().any(|r| r == "no_live_worker"),
        "the sibling slot's no-poller condition HOLDS and must be surfaced: {body}"
    );
}

/// The replay degrade path: an execution whose workflow type is not registered
/// cannot have its wait set replayed, and the endpoint must still serve 200
/// with the database-only verdict rather than failing the triage report.
///
/// `build_awaitables_report` has its OWN degrade path here and reports the
/// precise `handler_not_registered` reason, which is passed through verbatim —
/// strictly more actionable than this endpoint's own generic
/// `wait_set_unavailable` (which covers the case where the replay itself
/// errors) or `wait_set_timed_out` (where it exceeds the query timeout).
#[tokio::test]
async fn ac2_replay_failure_degrades_to_history_only_and_still_serves_200() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    // `ghost_wf` is deliberately absent from the registry.
    let exec_id = seed_execution(&pool, "ghost_wf", "RUNNING", vec![started_event()]).await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "no_pending_work", "body: {body}");
    assert_eq!(body["wait_set"], "history_only", "body: {body}");
    assert_eq!(
        body["wait_set_reason"], "handler_not_registered",
        "the upstream reason must be passed through, not flattened: {body}"
    );
}

// ── PR #1188 review round 2: the run's own workflow task row ───────────────

/// A workflow task held by a worker means the handler is executing RIGHT NOW.
/// Canonically a long local activity (issue #98), which schedules no queue row
/// at all — so before this the run fell through every category and reported the
/// alarming `no_pending_work`/`stalled`.
#[tokio::test]
async fn claimed_workflow_task_reports_healthy_not_a_lost_task() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "completes_wf", "RUNNING", vec![started_event()]).await;
    seed_workflow_task(
        &pool,
        exec_id,
        "default",
        "RUNNING",
        Some("w-live"),
        "NOW()",
    )
    .await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "healthy_in_progress", "body: {body}");
    assert_eq!(body["health"], "healthy", "body: {body}");
}

/// A PENDING workflow task on a queue nothing polls: the decision cycle can
/// never be claimed, so the run cannot advance even though nothing individual
/// is wedged. The workflow-task analogue of AC3's flagship condition.
#[tokio::test]
async fn pending_workflow_task_with_no_live_worker_reports_workflow_no_worker() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "completes_wf", "RUNNING", vec![started_event()]).await;
    seed_workflow_task(&pool, exec_id, "orphan-queue", "PENDING", None, "NOW()").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "workflow_no_worker", "body: {body}");
    assert_eq!(body["blocked_on"]["queue"], "orphan-queue", "body: {body}");
    assert_eq!(body["health"], "stalled", "body: {body}");
}

/// The control for the test above: register a live poller and the verdict
/// becomes ordinary dispatch latency, proving the stall verdict is driven by
/// coverage rather than by the mere presence of a PENDING row.
#[tokio::test]
async fn pending_workflow_task_with_a_live_worker_is_healthy() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "completes_wf", "RUNNING", vec![started_event()]).await;
    seed_workflow_task(&pool, exec_id, "covered-queue", "PENDING", None, "NOW()").await;
    seed_live_worker(&pool, "w-covers", "covered-queue").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "healthy_in_progress", "body: {body}");
    assert_eq!(body["health"], "healthy", "body: {body}");
}

/// A parked workflow task (`RUNNING` with a NULL worker) advances on a *wake*,
/// not a claim, so a live worker proves nothing: this is exactly the lost-task
/// shape and its verdict must be unchanged.
#[tokio::test]
async fn parked_workflow_task_is_still_the_lost_task_verdict() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(&pool, "completes_wf", "RUNNING", vec![started_event()]).await;
    seed_workflow_task(&pool, exec_id, "default", "RUNNING", None, "NOW()").await;
    seed_live_worker(&pool, "w-parked", "default").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "no_pending_work", "body: {body}");
    assert_eq!(body["health"], "stalled", "body: {body}");
}

/// The fallback must never flatten a specific verdict: a wedged activity AND a
/// dead workflow queue reports the ACTIVITY, which is the actionable cause.
#[tokio::test]
async fn workflow_task_fallback_never_masks_a_wedged_activity() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_api_app(build_api_state(&pool));

    let exec_id = seed_execution(
        &pool,
        "activity_wf",
        "RUNNING",
        vec![started_event(), scheduled_activity_event()],
    )
    .await;
    seed_activity_task(
        &pool,
        exec_id,
        "noop_activity",
        "dead-activity-queue",
        "PENDING",
        1,
        None,
        "NOW()",
    )
    .await;
    seed_workflow_task(&pool, exec_id, "dead-wf-queue", "PENDING", None, "NOW()").await;

    let body = diagnose(&app, exec_id).await;
    assert_eq!(kind(&body), "activity_no_worker", "body: {body}");
    assert_eq!(
        body["blocked_on"]["queue"], "dead-activity-queue",
        "the activity queue is the actionable one: {body}"
    );
}
