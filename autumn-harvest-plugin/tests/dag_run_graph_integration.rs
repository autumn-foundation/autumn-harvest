//! Integration tests for the DAG run graph view (issue #690).
//!
//! These exercise `GET /dags/{dag_name}/runs/{run_exec_id}` end to end against a
//! real Postgres (testcontainers). The endpoint is read-only: it reconstructs a
//! unified DAG run's node topology annotated with per-node status/timing purely
//! from the registered `DagDefinition` and recorded history. It adds no new
//! event variant and no migration.

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_const_for_fn)]

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::dag::DagBuilder;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::scheduler::{RegisteredDag, SchedulerMonitor, compile_dag_catalog};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    OverlapPolicy, StartWorkflowParams, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// ── DAG + activity definitions under test ───────────────────────────────────
//
// Node name == activity name. Distinct activities give distinct node names.

#[activity(queue = "default")]
async fn step_a(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn step_b(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn step_c(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn step_d(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

/// Linear: step_a -> step_b -> step_c
#[dag(default_queue = "default")]
fn graph_linear_dag(dag: &mut DagBuilder) {
    let a = dag.activity(step_a);
    let b = dag.activity(step_b).upstream(&a);
    let _c = dag.activity(step_c).upstream(&b);
}

/// Fan-out: step_a -> {step_b, step_c} -> step_d
#[dag(default_queue = "default")]
fn graph_fanout_dag(dag: &mut DagBuilder) {
    let a = dag.activity(step_a);
    let b = dag.activity(step_b).upstream(&a);
    let c = dag.activity(step_c).upstream(&a);
    let _d = dag.activity(step_d).upstream(&b).upstream(&c);
}

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

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        workflows![graph_linear_dag, graph_fanout_dag],
        activities![step_a, step_b, step_c, step_d],
    ))
}

/// Build the app with a fabricated catalog. When `classic` is provided, that
/// `(name, RegisteredDag)` pair is inserted alongside the unified DAGs so the
/// classic (non-unified) 400 path can be exercised.
fn build_app_with(
    pool: &DbPool,
    admin: bool,
    classic: Option<(String, RegisteredDag)>,
) -> HarvestApiApp {
    let mut catalog = compile_dag_catalog(dags![graph_linear_dag, graph_fanout_dag])
        .expect("dag catalog compiles");
    if let Some((name, dag)) = classic {
        catalog.insert(name, dag);
    }
    let api_state = HarvestApiState::new();
    if admin {
        // Bypass the admin guard for the happy-path / DB assertions.
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry(),
        Arc::new(catalog),
        Arc::new(Vec::new()),
        Some("dag-graph-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    build_app_with(pool, true, None)
}

/// A hand-built classic (non-unified) `RegisteredDag`. `compile_dag_catalog`
/// rejects classic DAGs, so the only way to register one is to construct it.
fn classic_registered_dag(name: &str) -> RegisteredDag {
    let mut builder = DagBuilder::new();
    let _a = builder.activity(step_a);
    let definition = builder.build().expect("classic dag builds");
    RegisteredDag {
        name: name.to_string(),
        module: "test".to_string(),
        schedule: None,
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default".to_string()),
        is_unified: false,
        definition,
        jitter: Duration::ZERO,
        overlap_policy: OverlapPolicy::default(),
        buffer_all_max: 0,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
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
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

fn sched(name: &str, id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: name.to_string(),
        input: json!({ "dag_task": name }),
        queue: "default".to_string(),
    }
}

fn completed(id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityCompleted {
        activity_id: id,
        output: Value::Null,
    }
}

fn failed(id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityFailed {
        activity_id: id,
        error: "transient S3 500\nstack trace line".to_string(),
        attempt: 1,
        error_type: "S3Error".to_string(),
        non_retryable: false,
        details: None,
    }
}

/// Seed a workflow execution for `dag_name`, append `events` after the initial
/// `WorkflowStarted`, then transition it to `state`.
async fn seed_run(
    conn: &mut AsyncPgConnection,
    dag_name: &'static str,
    workflow_id: &str,
    events: Vec<WorkflowEvent>,
    state: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: dag_name,
            workflow_id,
            exec_id,
            input: json!({}),
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

    let history = store::load_history(conn, exec_id).await.unwrap();
    store::append_events(conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append seed events");

    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq(state))
        .execute(conn)
        .await
        .expect("set state");

    exec_id
}

async fn establish(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

fn node<'a>(body: &'a Value, name: &str) -> &'a Value {
    body["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .find(|n| n["node_name"] == json!(name))
        .unwrap_or_else(|| panic!("node {name} present in body: {body}"))
}

// ── (a) annotated topology, end to end ──────────────────────────────────────

#[tokio::test]
async fn dag_run_graph_returns_annotated_topology() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // a succeeded; b failed; c never reached. Run is FAILED.
    let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        sched("step_b", ib),
        failed(ib),
        WorkflowEvent::WorkflowFailed {
            error: "one or more DAG tasks failed".to_string(),
        },
    ];
    let exec_id = seed_run(&mut conn, "graph_linear_dag", "graph-1", events, "FAILED").await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    // Run-level summary.
    assert_eq!(body["dag_name"], json!("graph_linear_dag"));
    assert_eq!(body["run_exec_id"], json!(exec_id.to_string()));
    assert_eq!(body["state"], json!("FAILED"));
    assert!(!body["started_at"].is_null(), "started_at present");

    // Node statuses.
    assert_eq!(node(&body, "step_a")["status"], json!("succeeded"));
    assert_eq!(node(&body, "step_b")["status"], json!("failed"));
    assert_eq!(node(&body, "step_c")["status"], json!("pending"));

    // depends_on topology (AC3).
    assert_eq!(node(&body, "step_a")["depends_on"], json!([]));
    assert_eq!(node(&body, "step_b")["depends_on"], json!(["step_a"]));
    assert_eq!(node(&body, "step_c")["depends_on"], json!(["step_b"]));

    // Timing + attempts for the attempted nodes.
    assert!(!node(&body, "step_a")["started_at"].is_null());
    assert!(!node(&body, "step_a")["finished_at"].is_null());
    assert_eq!(node(&body, "step_a")["attempts"], json!(1));

    // Failed node carries error_type + first-line-truncated error.
    let b_node = node(&body, "step_b");
    assert_eq!(b_node["error_type"], json!("S3Error"));
    assert_eq!(b_node["error"], json!("transient S3 500"));
    assert!(!b_node["finished_at"].is_null());

    // Never-reached node has no timing / error fields (skip_serializing_if).
    let c_node = node(&body, "step_c");
    assert!(c_node.get("started_at").is_none() || c_node["started_at"].is_null());
    assert!(c_node.get("error").is_none() || c_node["error"].is_null());
}

// ── (b) 404 for unknown DAG ─────────────────────────────────────────────────

#[tokio::test]
async fn dag_run_graph_unknown_dag_is_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-404",
        events,
        "RUNNING",
    )
    .await;

    let (status, _body) = get_json(&app, &format!("/dags/no_such_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── (c) 404 when the exec is not a run of that DAG ──────────────────────────

#[tokio::test]
async fn dag_run_graph_exec_not_a_run_of_dag_is_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // The run belongs to graph_linear_dag; ask for it under graph_fanout_dag.
    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-mismatch",
        events,
        "RUNNING",
    )
    .await;

    let (status, _body) = get_json(&app, &format!("/dags/graph_fanout_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── (d) 400 (JSON body) for a classic (non-unified) DAG ─────────────────────

#[tokio::test]
async fn dag_run_graph_classic_dag_is_bad_request_with_json_body() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let classic = classic_registered_dag("classic_dag");
    let app = build_app_with(&pool, true, Some(("classic_dag".to_string(), classic)));
    let mut conn = establish(&url).await;

    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-classic",
        events,
        "COMPLETED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/classic_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    // AC6: a JSON error body, never a 500 / empty body.
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| m.contains("classic")),
        "expected a JSON error body mentioning classic DAGs, got: {body}"
    );
}

// ── (e) admin-auth rejection ────────────────────────────────────────────────

#[tokio::test]
async fn dag_run_graph_requires_admin_auth() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // No admin boundary, non-dev profile, no session on the request → the
    // admin route layer rejects with 401 before any handler logic runs.
    let app = build_app_with(&pool, false, None);
    let mut conn = establish(&url).await;

    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-auth",
        events,
        "RUNNING",
    )
    .await;

    let (status, _body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ── (f) running vs cancelled node status depends on run state ────────────────

#[tokio::test]
async fn dag_run_graph_running_node_on_live_run() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // a completed; b scheduled but no terminal event; run still RUNNING.
    let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
    let events = vec![sched("step_a", ia), completed(ia), sched("step_b", ib)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-live",
        events,
        "RUNNING",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(node(&body, "step_a")["status"], json!("succeeded"));
    // Scheduled-no-terminal on a live run → running (not cancelled).
    assert_eq!(node(&body, "step_b")["status"], json!("running"));
    assert!(body["finished_at"].is_null(), "live run has no finished_at");
}
