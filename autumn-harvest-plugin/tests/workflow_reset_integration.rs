//! Integration tests for workflow reset recovery (issue #148).

#![allow(clippy::similar_names)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{
    harvest_events, harvest_signals, harvest_task_queue, harvest_workflow_executions,
};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    StartWorkflowParams, WorkflowContext, WorkflowIdReusePolicy, start_or_load_workflow_execution,
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
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

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
    "
",
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
    )
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

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("reset-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn build_reset_worker(registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: "reset-worker".to_string(),
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
                labels: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                sharded_pool: None,
            },
            registry,
        )
        .expect("worker config should be valid"),
    )
}

fn spawn_reset_worker(worker: Arc<Worker>, pool: DbPool) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        worker.run(&pool).await;
    })
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

async fn seed_execution(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "resettable",
            workflow_id,
            exec_id,
            input: json!({"workflow_id": workflow_id}),
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
        },
    )
    .await
    .expect("seed workflow");

    (
        exec_id,
        store::load_history(conn, exec_id).await.unwrap().events,
    )
}

async fn append_marker_events(conn: &mut AsyncPgConnection, exec_id: ExecutionId, count: usize) {
    let history = store::load_history(conn, exec_id).await.unwrap();
    let events = (0..count)
        .map(|idx| WorkflowEvent::MarkerRecorded {
            name: format!("checkpoint-{idx}"),
            details: json!({ "checkpoint": idx }),
        })
        .collect::<Vec<_>>();
    store::append_events(conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append markers");
}

async fn append_side_effect_checkpoint_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    count: usize,
) {
    let history = store::load_history(conn, exec_id).await.unwrap();
    let events = (0..count)
        .map(|idx| WorkflowEvent::MarkerRecorded {
            name: format!("side_effect:checkpoint-{idx}"),
            details: json!(idx),
        })
        .collect::<Vec<_>>();
    store::append_events(conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append side-effect markers");
}

async fn load_execution(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for execution reload");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load workflow execution")
}

async fn wait_for_execution_state(
    database_url: &str,
    exec_id: ExecutionId,
    expected_state: &str,
) -> WorkflowExecution {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let execution = load_execution(database_url, exec_id).await;
            if execution.state == expected_state {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow should reach expected state")
}

fn replay_checkpoints_then_signal<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        for checkpoint in 0..100 {
            let observed = ctx
                .side_effect(&format!("checkpoint-{checkpoint}"), || checkpoint)
                .map_err(|error| error.to_string())?;
            if observed != checkpoint {
                return Err(format!("checkpoint {checkpoint} replayed as {observed}"));
            }
        }

        let approval = ctx
            .wait_for_signal("approved")
            .await
            .map_err(|error| error.to_string())?;

        Ok(json!({
            "checkpoints_replayed": 100,
            "approval": approval,
        }))
    })
}

#[tokio::test]
async fn reset_forks_200_event_execution_and_tears_down_source() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let (exec_id, _) = seed_execution(&mut conn, "wf-reset-success").await;
    append_marker_events(&mut conn, exec_id, 199).await;

    autumn_harvest::signal::send_signal(&mut conn, exec_id, "approved", json!({"approved": true}))
        .await
        .expect("signal source");

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/reset"),
        json!({
            "reset_to_event_id": 100,
            "reason": "bad deploy on day 26",
            "operator_id": "oncall",
            "signal_reapply": "buffer"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "reset response: {body}");
    assert_eq!(body["reset_from_exec_id"], exec_id.to_string());
    assert_eq!(body["reset_to_event_id"], 100);
    assert_eq!(body["events_carried_over"], 101);
    let new_exec_id: ExecutionId = body["new_exec_id"]
        .as_str()
        .expect("new_exec_id")
        .parse()
        .expect("valid new exec id");

    let source_state: String = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(source_state, "TERMINATED");

    let fork: WorkflowExecution = harvest_workflow_executions::table
        .find(new_exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(fork.state, "RUNNING");
    assert_eq!(fork.workflow_id, "wf-reset-success");
    assert_eq!(new_exec_id.shard(), exec_id.shard());

    let fork_events: Vec<(i32, String)> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(new_exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select((harvest_events::event_id, harvest_events::event_type))
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(fork_events.len(), 102);
    assert_eq!(fork_events[100].0, 100);
    assert_eq!(fork_events[101].1, "WorkflowResetFork");

    let source_task_states: Vec<String> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .select(harvest_task_queue::state)
        .load(&mut conn)
        .await
        .unwrap();
    assert!(
        source_task_states.iter().all(|state| state == "CANCELLED"),
        "source task rows should be cancelled: {source_task_states:?}"
    );

    let fork_signal_count: i64 = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(new_exec_id.as_uuid()))
        .filter(harvest_signals::consumed.eq(false))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(fork_signal_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_fork_completes_with_current_code_and_observes_buffered_signal() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let (exec_id, _) = seed_execution(&mut conn, "wf-reset-worker").await;
    append_side_effect_checkpoint_events(&mut conn, exec_id, 199).await;

    autumn_harvest::signal::send_signal(
        &mut conn,
        exec_id,
        "approved",
        json!({"approved": true, "operator": "oncall"}),
    )
    .await
    .expect("signal source");

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/reset"),
        json!({
            "reset_to_event_id": 100,
            "reason": "bad deploy recovery",
            "operator_id": "oncall",
            "signal_reapply": "buffer"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "reset response: {body}");
    let new_exec_id: ExecutionId = body["new_exec_id"]
        .as_str()
        .expect("new_exec_id")
        .parse()
        .expect("valid new exec id");

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "resettable",
            module: "workflow_reset_integration",
            handler: replay_checkpoints_then_signal,
            execution_timeout: None,
            concurrency: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![],
    ));
    let worker = build_reset_worker(registry);
    let handle = spawn_reset_worker(Arc::clone(&worker), pool);

    let completed = wait_for_execution_state(&url, new_exec_id, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker should join cleanly");

    assert_eq!(
        completed.output,
        Some(json!({
            "checkpoints_replayed": 100,
            "approval": {
                "approved": true,
                "operator": "oncall"
            }
        }))
    );

    let signal_events: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(new_exec_id.as_uuid()))
        .filter(harvest_events::event_type.eq("SignalReceived"))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(signal_events, 1);
}

#[tokio::test]
async fn reset_rejects_unresolved_side_effect_boundary_with_hint() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let (exec_id, _) = seed_execution(&mut conn, "wf-reset-invalid").await;
    let activity_id = ActivityExecId::new();
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "charge_card".into(),
            input: Value::Null,
            queue: "default".into(),
        }],
        history.next_event_id,
    )
    .await
    .unwrap();

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/reset"),
        json!({
            "reset_to_event_id": 1,
            "reason": "bad args",
            "operator_id": "oncall",
            "signal_reapply": "drop"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_eq!(body["nearest_valid_before"], 0);
    assert_eq!(body["nearest_valid_after"], Value::Null);
    assert_eq!(
        body["unresolved_side_effects"][0]["kind"],
        "ActivityScheduled"
    );
    assert_eq!(
        body["unresolved_side_effects"][0]["side_effect_id"],
        activity_id.to_string()
    );
}

#[tokio::test]
async fn reset_on_terminal_source_returns_conflict() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let (exec_id, _) = seed_execution(&mut conn, "wf-reset-terminal").await;

    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/reset"),
        json!({
            "reset_to_event_id": 0,
            "reason": "too late",
            "operator_id": "oncall",
            "signal_reapply": "drop"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    assert!(body["message"].as_str().unwrap().contains("terminal"));
}
