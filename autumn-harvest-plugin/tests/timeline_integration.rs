//! Integration tests for `GET /api/harvest/workflows/{id}/timeline` (issue #739).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise:
//!   (1) 404 for an unknown execution id.
//!   (2) 200 with reconstructed steps for a COMPLETED execution whose seeded
//!       history carries an activity (scheduled/started/completed) plus a timer;
//!       the activity's wait/exec split is present and `slowest_step` is non-null.
//!   (3) A RUNNING (non-terminal) execution → the open step reports outcome
//!       "pending" with a null `ended_at`.
//!   (4) 400 for a malformed execution id.
//!
//! The core duration math is unit-tested in `autumn-harvest/src/timeline.rs`;
//! these tests verify the HTTP wiring (routing, 404/400, shape).

#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority, ShardId, TimerId, WorkerId};
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
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// DELIBERATE PARTIAL-SCHEMA FIXTURE — do NOT convert to `full_migrations_sql()`.
// `timeline_classic_dag_run_returns_404` inserts a classic DAG run row directly
// into `harvest_dag_runs`, but migration 20260514000000_drop_harvest_dag_runs
// (included in the full bundle) drops that table. This hand-rolled subset
// intentionally applies the initial migration (which creates harvest_dag_runs)
// WITHOUT the later drop, so the table remains available for the classic-DAG
// timeline 404 assertion.
const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
    // issue #740: start_source provenance columns (read back by WorkflowExecution::as_select()).
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS start_source TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS start_source_ref TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS started_by TEXT NULL;\n",
    // issue #617: chain-scoped lifetime cap columns (read back by WorkflowExecution::as_select()).
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS chain_execution_timeout INTERVAL NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS chain_deadline_at TIMESTAMPTZ NULL;\n",
    // issue #704: history-bloat early-warning guard column (read back by
    // WorkflowExecution::as_select()).
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS history_bloat_warned_at TIMESTAMPTZ NULL;\n",
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
    // Unified DAG schedule rows carry both dag_name and workflow_name, so the
    // strict XOR kind_check from the migration above must be relaxed to an OR.
    include_str!("../../autumn-harvest/migrations/20260514010000_unified_dag_schedule_kind/up.sql"),
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
    include_str!(
        "../../autumn-harvest/migrations/20260708000001_harvest_completion_trigger_condition/up.sql"
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
    // issue #704: history_bloat_warned_at column on harvest_workflow_executions,
    // referenced by every WorkflowExecution::as_select() read-back in this suite.
    include_str!(
        "../../autumn-harvest/migrations/20260716000000_harvest_workflow_history_bloat_warn/up.sql"
    ),
    // issue #759: triage_note column on harvest_workflow_executions, referenced
    // by every WorkflowExecution::as_select() read-back in this suite.
    include_str!(
        "../../autumn-harvest/migrations/20260717000000_harvest_workflow_triage_note/up.sql"
    ),
    // issue #804: capability_misses column on harvest_task_queue, referenced by
    // every TaskQueueItem read (claim_task's `RETURNING harvest_task_queue.*`)
    // in this suite.
    include_str!(
        "../../autumn-harvest/migrations/20260720000000_harvest_task_capability_misses/up.sql"
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
        Some("timeline-test".to_string()),
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

async fn seed_running(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "timeline-wf",
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
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
    exec_id
}

/// Anchor the execution's `started_at` to a fixed instant so timeline durations
/// are deterministic relative to the events we seed below.
async fn set_started_at(conn: &mut AsyncPgConnection, exec_id: ExecutionId, at: DateTime<Utc>) {
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::started_at.eq(at))
        .execute(conn)
        .await
        .unwrap();
}

async fn mark_completed(conn: &mut AsyncPgConnection, exec_id: ExecutionId, at: DateTime<Utc>) {
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(at)),
            harvest_workflow_executions::output.eq(Some(json!({"ok": true}))),
        ))
        .execute(conn)
        .await
        .unwrap();
}

/// Insert one event row with an explicit `event_id` and `timestamp` so we control
/// the reconstructed step durations (the engine's own `append_events` stamps the
/// row with `NOW()`, which we deliberately bypass here).
async fn insert_event(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    event_id: i32,
    event: &WorkflowEvent,
    at: DateTime<Utc>,
) {
    let data = serde_json::to_value(event).expect("event serializes");
    let event_type = data
        .get("type")
        .and_then(Value::as_str)
        .expect("adjacently-tagged event carries a 'type'")
        .to_string();
    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(event_id),
            harvest_events::event_type.eq(event_type),
            harvest_events::event_data.eq(data),
            harvest_events::timestamp.eq(at),
        ))
        .execute(conn)
        .await
        .unwrap();
}

async fn max_event_id(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i32 {
    harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(diesel::dsl::max(harvest_events::event_id))
        .first::<Option<i32>>(conn)
        .await
        .unwrap()
        .unwrap_or(0)
}

fn find_step<'a>(steps: &'a [Value], kind: &str) -> &'a Value {
    steps
        .iter()
        .find(|s| s["step_kind"] == kind)
        .unwrap_or_else(|| panic!("no {kind} step in {steps:#?}"))
}

/// (1) Unknown execution id → 404.
#[tokio::test]
async fn timeline_unknown_id_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, _body) = get_json(&app, &format!("/workflows/{unknown}/timeline")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// (4) Malformed execution id → 400.
#[tokio::test]
async fn timeline_malformed_id_returns_400() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, _body) = get_json(&app, "/workflows/not-a-uuid/timeline").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// (2) A COMPLETED execution whose history carries an activity (with a start
/// event, so the wait/exec split is present) and a timer → 200 with the
/// reconstructed steps and a non-null `slowest_step`.
#[tokio::test]
async fn timeline_completed_reconstructs_steps_with_split_and_slowest() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let t0 = Utc::now() - Duration::seconds(60);
    let exec_id = seed_running(&mut conn, "completed-timeline").await;
    set_started_at(&mut conn, exec_id, t0).await;

    // Continue event ids after the WorkflowStarted row created by seed_running.
    let mut next = max_event_id(&mut conn, exec_id).await + 1;
    let a1 = ActivityExecId::new();
    let t1 = TimerId::new("cooldown");

    // Activity: scheduled @ +10s, started @ +12s, completed @ +20s
    // → total 10s, wait 2s, exec 8s, completed.
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::ActivityScheduled {
            activity_id: a1,
            name: "send_email".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        t0 + Duration::seconds(10),
    )
    .await;
    next += 1;
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::ActivityStarted {
            activity_id: a1,
            worker_id: WorkerId::new("w-1"),
        },
        t0 + Duration::seconds(12),
    )
    .await;
    next += 1;
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::ActivityCompleted {
            activity_id: a1,
            output: Value::Null,
        },
        t0 + Duration::seconds(20),
    )
    .await;
    next += 1;
    // Timer: started @ +15s, fired @ +45s → total 30s (the slowest step).
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::TimerStarted {
            timer_id: t1.clone(),
            duration_secs: 30,
        },
        t0 + Duration::seconds(15),
    )
    .await;
    next += 1;
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::TimerFired { timer_id: t1 },
        t0 + Duration::seconds(45),
    )
    .await;

    mark_completed(&mut conn, exec_id, t0 + Duration::seconds(50)).await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/timeline")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    // Header echoes.
    assert_eq!(body["exec_id"], exec_id.to_string());
    assert_eq!(body["workflow_name"], "timeline-wf");
    assert_eq!(body["state"], "COMPLETED");

    let steps = body["steps"].as_array().expect("steps is an array");
    assert_eq!(steps.len(), 2, "activity + timer steps: {steps:#?}");

    // Ordering by scheduled_at: activity (+10s) precedes timer (+15s).
    assert_eq!(steps[0]["step_kind"], "activity");
    assert_eq!(steps[1]["step_kind"], "timer");

    // Activity: wait/exec split present + correct math + completed.
    let act = find_step(steps, "activity");
    assert_eq!(act["name"], "send_email");
    assert_eq!(act["outcome"], "completed");
    assert_eq!(act["total_ms"], 10_000);
    assert_eq!(act["wait_ms"], 2_000);
    assert_eq!(act["exec_ms"], 8_000);
    assert_eq!(act["attempt"], 1);

    // Timer: fired, no split, 30s total.
    let tm = find_step(steps, "timer");
    assert_eq!(tm["outcome"], "fired");
    assert_eq!(tm["total_ms"], 30_000);
    assert!(tm["wait_ms"].is_null());
    assert!(tm["exec_ms"].is_null());

    // slowest_step is non-null and is the timer (30s > 10s).
    let slowest = &body["rollup"]["slowest_step"];
    assert!(!slowest.is_null(), "slowest_step must be present");
    assert_eq!(slowest["step_kind"], "timer");
    assert_eq!(slowest["total_ms"], 30_000);

    // Rollup: busy = activity exec (8s), wait = activity wait (2s) + timer (30s).
    assert_eq!(body["rollup"]["busy_ms"], 8_000);
    assert_eq!(body["rollup"]["wait_ms"], 32_000);
    assert_eq!(body["rollup"]["total_wall_clock_ms"], 50_000);
}

/// (3) A RUNNING (non-terminal) execution with an open activity → the step
/// reports outcome "pending" and a null `ended_at`, measured to "now".
#[tokio::test]
async fn timeline_running_open_step_is_pending() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let t0 = Utc::now() - Duration::seconds(30);
    let exec_id = seed_running(&mut conn, "running-timeline").await;
    set_started_at(&mut conn, exec_id, t0).await;

    let mut next = max_event_id(&mut conn, exec_id).await + 1;
    let a1 = ActivityExecId::new();
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::ActivityScheduled {
            activity_id: a1,
            name: "long_task".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        t0 + Duration::seconds(5),
    )
    .await;
    next += 1;
    // Started but never completed → still open.
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::ActivityStarted {
            activity_id: a1,
            worker_id: WorkerId::new("w-1"),
        },
        t0 + Duration::seconds(6),
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/timeline")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["state"], "RUNNING");

    let steps = body["steps"].as_array().expect("steps is an array");
    let act = find_step(steps, "activity");
    assert_eq!(act["outcome"], "pending", "open activity must be pending");
    assert!(act["ended_at"].is_null(), "open step has no ended_at");
    // total_ms is measured to "now" (>= 24s: from +5s scheduled to ~+30s now).
    assert!(
        act["total_ms"].as_i64().unwrap() >= 24_000,
        "pending total measured to now, got {}",
        act["total_ms"]
    );
}

/// (5) AC6: a **classic** DAG run lives only in `harvest_dag_runs` and has NO
/// matching `harvest_workflow_executions` row — classic DAGs are not on the
/// standard execution path — so its timeline returns `404` with the explanatory
/// message. This makes the AC6 promise falsifiable beyond a random unknown UUID.
#[tokio::test]
async fn timeline_classic_dag_run_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // Insert a classic DAG run row (raw SQL — `harvest_dag_runs` is not in the
    // Diesel schema module). Route the id to shard 0 so `db_conn_for_execution`
    // resolves deterministically; there is deliberately no execution row for it.
    let dag_run_id = ExecutionId::new_for_shard(ShardId::new(0));
    diesel::sql_query(
        "INSERT INTO harvest_dag_runs \
         (id, dag_name, logical_date, data_interval_start, data_interval_end) \
         VALUES ($1, 'nightly_etl', NOW(), NOW(), NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(dag_run_id.as_uuid())
    .execute(&mut conn)
    .await
    .unwrap();

    let (status, body) = get_json(&app, &format!("/workflows/{dag_run_id}/timeline")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body.to_string()
            .contains("classic DAG runs are not on the execution path"),
        "404 body must explain classic DAG runs are off-path: {body}"
    );
}

/// (6) AC6/L1: a **unified** DAG run lowers onto the standard execution path — it
/// IS a `harvest_workflow_executions` row with an ordinary
/// `ActivityScheduled`/`ActivityStarted`/`ActivityCompleted` history — so its
/// timeline reconstructs exactly like a plain workflow run and returns `200`.
/// This is behaviorally identical to a plain workflow execution, which is the
/// point: the route serves unified DAGs with no special-casing.
#[tokio::test]
async fn timeline_unified_dag_run_reconstructs_like_a_workflow() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let t0 = Utc::now() - Duration::seconds(30);
    // A unified DAG start creates a normal execution row (same as a workflow).
    let exec_id = seed_running(&mut conn, "unified-dag-run").await;
    set_started_at(&mut conn, exec_id, t0).await;

    let mut next = max_event_id(&mut conn, exec_id).await + 1;
    let a1 = ActivityExecId::new();
    // The unified-DAG handler dispatches each node through `execute_activity_raw`,
    // so a node renders as an ordinary activity step.
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::ActivityScheduled {
            activity_id: a1,
            name: "extract".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        t0 + Duration::seconds(2),
    )
    .await;
    next += 1;
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::ActivityStarted {
            activity_id: a1,
            worker_id: WorkerId::new("w-1"),
        },
        t0 + Duration::seconds(3),
    )
    .await;
    next += 1;
    insert_event(
        &mut conn,
        exec_id,
        next,
        &WorkflowEvent::ActivityCompleted {
            activity_id: a1,
            output: Value::Null,
        },
        t0 + Duration::seconds(9),
    )
    .await;

    mark_completed(&mut conn, exec_id, t0 + Duration::seconds(10)).await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/timeline")).await;
    assert_eq!(status, StatusCode::OK, "unified DAG is a workflow: {body}");
    assert_eq!(body["state"], "COMPLETED");

    let steps = body["steps"].as_array().expect("steps is an array");
    let act = find_step(steps, "activity");
    assert_eq!(act["name"], "extract");
    assert_eq!(act["outcome"], "completed");
    assert_eq!(act["total_ms"], 7_000); // +2s → +9s
    assert_eq!(act["wait_ms"], 1_000); // +2s → +3s
    assert_eq!(act["exec_ms"], 6_000); // +3s → +9s
}
