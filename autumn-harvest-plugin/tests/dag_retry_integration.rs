//! Integration tests for DAG retry-from-failed-node (issue #366).
//!
//! These exercise the `POST /dags/{dag_name}/runs/{run_exec_id}/retry` endpoint
//! end to end against a real Postgres (testcontainers). The endpoint composes on
//! top of the #148 workflow-reset internals; it adds no new event variant and no
//! migration.
//!
//! Semantics under test (operator-selected "level-granular" mode): retrying a
//! failed node resets to the clean boundary before the earliest scheduling of
//! the failed node and its declared downstream. When the failed node shares a
//! parallel level with siblings scheduled before it, that boundary is rejected
//! by the #148 validator and the endpoint returns `409` with a remediation hint.

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_const_for_fn)]

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::scheduler::{SchedulerMonitor, compile_dag_catalog};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, StartWorkflowParams, WorkflowIdReusePolicy, start_or_load_workflow_execution,
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
#[activity(queue = "default")]
async fn step_e(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

/// Linear: step_a -> step_b -> step_c -> step_d
#[dag(default_queue = "default")]
fn linear_retry_dag(dag: &mut DagBuilder) {
    let a = dag.activity(step_a);
    let b = dag.activity(step_b).upstream(&a);
    let c = dag.activity(step_c).upstream(&b);
    let _d = dag.activity(step_d).upstream(&c);
}

/// Fan-out: step_a -> {step_b, step_c, step_d} -> step_e
#[dag(default_queue = "default")]
fn fanout_retry_dag(dag: &mut DagBuilder) {
    let a = dag.activity(step_a);
    let b = dag.activity(step_b).upstream(&a);
    let c = dag.activity(step_c).upstream(&a);
    let d = dag.activity(step_d).upstream(&a);
    let _e = dag.activity(step_e).upstream(&b).upstream(&c).upstream(&d);
}

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
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
);

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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
        workflows![linear_retry_dag, fanout_retry_dag],
        activities![step_a, step_b, step_c, step_d, step_e],
    ))
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let catalog = compile_dag_catalog(dags![linear_retry_dag, fanout_retry_dag])
        .expect("dag catalog compiles");
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry(),
        Arc::new(catalog),
        Arc::new(Vec::new()),
        Some("dag-retry-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn build_worker() -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "dag-retry-worker".to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
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
                sharded_pool: None,
            },
            registry(),
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
        serde_json::from_slice(&bytes).expect("response is JSON")
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
        error: "transient S3 500".to_string(),
        attempt: 1,
        error_type: "Error".to_string(),
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

async fn fork_events(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    store::load_history(conn, exec_id).await.unwrap().events
}

// Linear failed run: a,b succeeded; c failed; d never reached.
// 0 WorkflowStarted, 1 schedA, 2 compA, 3 schedB, 4 compB, 5 schedC, 6 failC, 7 WorkflowFailed
fn linear_failed_events() -> (Vec<WorkflowEvent>, ActivityExecId, ActivityExecId) {
    let (ia, ib, ic) = (
        ActivityExecId::new(),
        ActivityExecId::new(),
        ActivityExecId::new(),
    );
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        sched("step_b", ib),
        completed(ib),
        sched("step_c", ic),
        failed(ic),
        WorkflowEvent::WorkflowFailed {
            error: "one or more DAG tasks failed".to_string(),
        },
    ];
    (events, ia, ib)
}

async fn establish(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

// ── (a) linear DAG, end-to-end re-execution ─────────────────────────────────

#[tokio::test]
async fn retry_linear_dag_from_failed_node_carries_upstream_and_completes() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let (events, _ia, _ib) = linear_failed_events();
    let exec_id = seed_run(&mut conn, "linear_retry_dag", "lin-1", events, "FAILED").await;

    let (status, body) = post_json(
        &app,
        &format!("/dags/linear_retry_dag/runs/{exec_id}/retry"),
        json!({
            "from_nodes": ["step_c"],
            "reason": "S3 incident 2026-05-17",
            "operator_id": "oncall"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "retry response: {body}");
    assert_eq!(body["reset_to_event_id"], 4);
    assert_eq!(body["nodes_carried_over"], json!(["step_a", "step_b"]));
    assert_eq!(body["nodes_to_re_execute"], json!(["step_c", "step_d"]));
    assert_eq!(body["dry_run"], json!(false));

    let new_exec_id: ExecutionId = body["new_run_exec_id"]
        .as_str()
        .expect("new_run_exec_id")
        .parse()
        .expect("valid exec id");

    // (h) the WorkflowResetFork marker on the new run carries the dag_retry reason.
    let forked = fork_events(&mut conn, new_exec_id).await;
    let fork_marker = forked
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowResetFork { reason, .. } => Some(reason.clone()),
            _ => None,
        })
        .expect("fork marker present");
    assert!(
        fork_marker.contains("dag_retry: nodes=[step_c]"),
        "fork reason should annotate the dag retry: {fork_marker}"
    );
    // Carried-over history includes step_a and step_b completions.
    assert!(forked.iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityScheduled { name, .. } if name == "step_a"
    )));
    assert!(forked.iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityScheduled { name, .. } if name == "step_b"
    )));

    // Source is sealed to TERMINATED (not left FAILED) so a second retry of the
    // same source run is rejected — no duplicate forks.
    let source_state: String = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(source_state, "TERMINATED");

    let (dup_status, _dup_body) = post_json(
        &app,
        &format!("/dags/linear_retry_dag/runs/{exec_id}/retry"),
        json!({
            "from_nodes": ["step_c"],
            "reason": "second attempt",
            "operator_id": "oncall"
        }),
    )
    .await;
    assert_eq!(
        dup_status,
        StatusCode::CONFLICT,
        "a sealed (TERMINATED) source must not be retried again"
    );

    // Drive the fork to completion: c and d re-dispatch and succeed.
    let worker = build_worker();
    let worker_task = {
        let worker = worker.clone();
        let pool = pool.clone();
        tokio::spawn(async move { worker.run(&pool).await })
    };

    let completed_state = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let st: String = harvest_workflow_executions::table
                .find(new_exec_id.as_uuid())
                .select(harvest_workflow_executions::state)
                .first(&mut conn)
                .await
                .unwrap();
            if st == "COMPLETED" || st == "FAILED" {
                break st;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("fork should reach a terminal state");

    worker.shutdown();
    let _ = worker_task.await;

    assert_eq!(
        completed_state, "COMPLETED",
        "fork should complete after retry"
    );

    // The fork re-executed c and d (fresh ActivityCompleted for both).
    let forked = fork_events(&mut conn, new_exec_id).await;
    assert!(forked.iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityScheduled { name, .. } if name == "step_c"
    )));
    assert!(forked.iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityScheduled { name, .. } if name == "step_d"
    )));
}

// ── (b) fan-out DAG, level-granular retry ───────────────────────────────────

#[tokio::test]
async fn retry_fanout_dag_re_executes_failed_node_level() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // c is scheduled first in the parallel level, then b and d; b and d complete;
    // c fails. The cut therefore lands on the clean boundary before the whole
    // level (after step_a). step_a is carried over; b, c, d, e re-execute.
    // 0 WfStarted,1 schedA,2 compA,3 schedC,4 schedB,5 schedD,6 compB,7 compD,8 failC,9 WfFailed
    let (ia, ib, ic, id) = (
        ActivityExecId::new(),
        ActivityExecId::new(),
        ActivityExecId::new(),
        ActivityExecId::new(),
    );
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        sched("step_c", ic),
        sched("step_b", ib),
        sched("step_d", id),
        completed(ib),
        completed(id),
        failed(ic),
        WorkflowEvent::WorkflowFailed {
            error: "one or more DAG tasks failed".to_string(),
        },
    ];
    let exec_id = seed_run(&mut conn, "fanout_retry_dag", "fan-1", events, "FAILED").await;

    let (status, body) = post_json(
        &app,
        &format!("/dags/fanout_retry_dag/runs/{exec_id}/retry"),
        json!({
            "from_nodes": ["step_c"],
            "reason": "branch c retry",
            "operator_id": "oncall"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "retry response: {body}");
    assert_eq!(body["reset_to_event_id"], 2);
    assert_eq!(body["nodes_carried_over"], json!(["step_a"]));
    assert_eq!(
        body["nodes_to_re_execute"],
        json!(["step_b", "step_c", "step_d", "step_e"])
    );
}

// ── (c) dry-run writes nothing ──────────────────────────────────────────────

#[tokio::test]
async fn retry_dry_run_returns_plan_without_writing() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let (events, _ia, _ib) = linear_failed_events();
    let exec_id = seed_run(&mut conn, "linear_retry_dag", "lin-dry", events, "FAILED").await;

    let runs_before: i64 = harvest_workflow_executions::table
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();

    let (status, body) = post_json(
        &app,
        &format!("/dags/linear_retry_dag/runs/{exec_id}/retry"),
        json!({
            "from_nodes": ["step_c"],
            "reason": "dry run preview",
            "operator_id": "oncall",
            "dry_run": true
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "dry-run response: {body}");
    assert_eq!(body["dry_run"], json!(true));
    assert_eq!(body["reset_to_event_id"], 4);
    assert_eq!(body["nodes_carried_over"], json!(["step_a", "step_b"]));
    assert_eq!(body["nodes_to_re_execute"], json!(["step_c", "step_d"]));
    assert!(body["new_run_exec_id"].is_null());

    // No write: no new execution row, source still FAILED.
    let runs_after: i64 = harvest_workflow_executions::table
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(runs_before, runs_after, "dry-run must not create a fork");
    let source_state: String = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(source_state, "FAILED");
}

// ── (d) retry of a Succeeded run -> 409 ──────────────────────────────────────

#[tokio::test]
async fn retry_succeeded_run_is_conflict() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let ia = ActivityExecId::new();
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    let exec_id = seed_run(&mut conn, "linear_retry_dag", "lin-ok", events, "COMPLETED").await;

    let (status, body) = post_json(
        &app,
        &format!("/dags/linear_retry_dag/runs/{exec_id}/retry"),
        json!({ "from_nodes": ["step_a"], "reason": "x", "operator_id": "oncall" }),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("succeeded"),
        "message should point at fresh run: {body}"
    );
}

// ── (e) retry of a Running run -> 409 ────────────────────────────────────────

#[tokio::test]
async fn retry_running_run_is_conflict() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let (events, _ia, _ib) = linear_failed_events();
    // Re-use the failed-history shape but leave state RUNNING.
    let exec_id = seed_run(&mut conn, "linear_retry_dag", "lin-run", events, "RUNNING").await;

    let (status, body) = post_json(
        &app,
        &format!("/dags/linear_retry_dag/runs/{exec_id}/retry"),
        json!({ "from_nodes": ["step_c"], "reason": "x", "operator_id": "oncall" }),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("cancel"),
        "message should hint to cancel first: {body}"
    );
}

// ── (f) non-existent node -> 400 with declared list ─────────────────────────

#[tokio::test]
async fn retry_unknown_node_lists_declared() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let (events, _ia, _ib) = linear_failed_events();
    let exec_id = seed_run(&mut conn, "linear_retry_dag", "lin-bad", events, "FAILED").await;

    let (status, body) = post_json(
        &app,
        &format!("/dags/linear_retry_dag/runs/{exec_id}/retry"),
        json!({ "from_nodes": ["no_such_node"], "reason": "x", "operator_id": "oncall" }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_eq!(body["unknown_nodes"], json!(["no_such_node"]));
    assert_eq!(
        body["declared_nodes"],
        json!(["step_a", "step_b", "step_c", "step_d"])
    );
}

// ── (g) reset boundary inside an unresolved upstream side effect -> 409 ──────

#[tokio::test]
async fn retry_with_unresolved_upstream_side_effect_is_conflict() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // Under level-granular semantics the cut lands before the failed node's
    // whole level, so the only way to land inside an unresolved side effect is
    // an UPSTREAM one. Here step_a was scheduled but never settled (the run was
    // cancelled while it was in flight) yet step_b was recorded as failed.
    // Retrying step_b cuts before step_b's level, where step_a is still pending,
    // so the #148 validator rejects the boundary -> 409 with a remediation hint.
    // 0 WfStarted,1 schedA,2 schedB,3 failB,4 WorkflowCancelled
    let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
    let events = vec![
        sched("step_a", ia), // never completes -> unresolved upstream at the cut
        sched("step_b", ib),
        failed(ib),
        WorkflowEvent::WorkflowCancelled {
            reason: "operator cancelled".to_string(),
        },
    ];
    let exec_id = seed_run(
        &mut conn,
        "linear_retry_dag",
        "lin-mid",
        events,
        "CANCELLED",
    )
    .await;

    let (status, body) = post_json(
        &app,
        &format!("/dags/linear_retry_dag/runs/{exec_id}/retry"),
        json!({ "from_nodes": ["step_b"], "reason": "x", "operator_id": "oncall" }),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    assert!(
        body["remediation"]
            .as_str()
            .unwrap_or_default()
            .contains("upstream"),
        "409 should carry a remediation hint: {body}"
    );
    assert!(body["nearest_valid_before"].is_i64());
}

// ── classic (non-unified) DAG rejection ─────────────────────────────────────
//
// (Covered structurally: a classic DAG has `is_unified=false`. Our test DAGs are
// unified; the rejection path is asserted via a fabricated catalog in the unit
// layer. Here we assert the unregistered-DAG 404 path.)

#[tokio::test]
async fn retry_unregistered_dag_is_not_found() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let (events, _ia, _ib) = linear_failed_events();
    let exec_id = seed_run(&mut conn, "linear_retry_dag", "lin-404", events, "FAILED").await;

    let (status, _body) = post_json(
        &app,
        &format!("/dags/no_such_dag/runs/{exec_id}/retry"),
        json!({ "from_nodes": ["step_c"], "reason": "x", "operator_id": "oncall" }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── empty from_nodes -> 400 ─────────────────────────────────────────────────

#[tokio::test]
async fn retry_empty_from_nodes_is_bad_request() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let (events, _ia, _ib) = linear_failed_events();
    let exec_id = seed_run(&mut conn, "linear_retry_dag", "lin-empty", events, "FAILED").await;

    let (status, _body) = post_json(
        &app,
        &format!("/dags/linear_retry_dag/runs/{exec_id}/retry"),
        json!({ "from_nodes": [], "reason": "x", "operator_id": "oncall" }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}
