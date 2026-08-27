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
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

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
                shard_notification_database_urls: Vec::new(),
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
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: std::time::Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
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
    // Deliberately a NON-default shard (issue #697 AC4): the reset fork must
    // inherit the *source's* shard, so seeding on shard 0 -- which is this
    // fixture's default shard -- would let a "always use default_shard"
    // regression pass. Only the encoded shard changes; the row is still written
    // through the single test pool, which every shard id resolves to here.
    let exec_id = ExecutionId::new_for_shard(ShardId::new(4));
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
    assert_eq!(
        new_exec_id.shard(),
        exec_id.shard(),
        "a reset fork must inherit the source's shard (issue #697 AC4); the \
         source is seeded on a non-default shard so this also falsifies a \
         `default_shard` regression, not just an `ExecutionId::new()` one"
    );

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
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "resettable",
            module: "workflow_reset_integration",
            handler: replay_checkpoints_then_signal,
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

/// Regression test (code-review fix, issue #603): resetting a currently
/// ND-blocked execution (the documented escalation path) must strip the six
/// replay-non-determinism diagnostic keys from the fork's `search_attrs`
/// while preserving unrelated business attributes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_strips_stale_nd_diagnostic_search_attrs_from_fork() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let (exec_id, _) = seed_execution(&mut conn, "wf-reset-nd-blocked").await;
    append_marker_events(&mut conn, exec_id, 5).await;

    // Simulate the source being currently ND-blocked: RUNNING, with the block
    // columns and search_attrs diagnostic stamped, plus one unrelated
    // business attribute the fork must keep.
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::nd_blocked_at.eq(Some(Utc::now())),
            harvest_workflow_executions::nd_block_reason.eq(Some(
                "non-deterministic replay: activity mismatch".to_string(),
            )),
            harvest_workflow_executions::nd_block_count.eq(1),
            harvest_workflow_executions::search_attrs.eq(Some(json!({
                "failure_cause": "non_determinism",
                "event_index": 3,
                "expected": "ActivityScheduled",
                "actual": "TimerStarted",
                "workflow_type": "wf-reset-nd-blocked",
                "build_id": "v2.0.0",
                "tenant": "acme",
            }))),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{exec_id}/reset"),
        json!({
            "reset_to_event_id": 1,
            "reason": "escalate stuck ND block",
            "operator_id": "oncall",
            "signal_reapply": "drop"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "reset response: {body}");
    let new_exec_id: ExecutionId = body["new_exec_id"]
        .as_str()
        .expect("new_exec_id")
        .parse()
        .expect("valid new exec id");

    let fork: WorkflowExecution = harvest_workflow_executions::table
        .find(new_exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .unwrap();

    assert!(
        fork.nd_blocked_at.is_none(),
        "fork must not carry the source's nd_blocked_at column"
    );
    assert_eq!(fork.nd_block_count, 0);

    let attrs = fork.search_attrs.expect("fork must keep the business attr");
    assert_eq!(
        attrs.get("tenant"),
        Some(&json!("acme")),
        "unrelated business search attr must survive: {attrs}"
    );
    for key in [
        "failure_cause",
        "event_index",
        "expected",
        "actual",
        "workflow_type",
        "build_id",
    ] {
        assert!(
            attrs.get(key).is_none(),
            "fork must not inherit the stale ND diagnostic key '{key}': {attrs}"
        );
    }
}
