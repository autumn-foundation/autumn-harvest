//! Integration tests for workflow reset recovery (issue #148).

#![allow(clippy::similar_names)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::reset::{
    ResetSignalReapplyPolicy, WorkflowResetError, WorkflowResetRequest, preview_workflow_reset,
    reset_workflow_execution,
};
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
    autumn_harvest::test_init_sql().as_bytes().to_vec()
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
    // Issue #1802: set the opt-out. This test exercises the handler, not auth.
    api_state.set_allow_unauthenticated_mutations(true);
    // `POST /workflows/batch_reset` is admin-only. The host owns that boundary.
    api_state.set_admin_auth_boundary(true);
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
    harvest_api_router(api_state)
}

fn build_reset_worker(registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
                dr: autumn_harvest::replication::DrConfig::default(),
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
                resident_workflows: true,
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
            input: json!({"workflow_id": workflow_id}).into(),
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
            tenant: None,
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

// ── a fork never uses a PII-erased source (issue #1999) ─────────────────────

/// Seed a run in the terminal `state`.
async fn seed_terminal_execution(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    state: &str,
) -> ExecutionId {
    let (exec_id, _) = seed_execution(conn, workflow_id).await;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq(state),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .expect("mark terminal");
    exec_id
}

/// Erase the payloads of `exec_id`, as `POST /workflows/{id}/erase-payloads` does.
async fn erase(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    let outcome =
        autumn_harvest::erase::erase_workflow_payloads(conn, exec_id, "gdpr subject request")
            .await
            .expect("erase payloads");
    assert!(
        outcome.fields_tombstoned > 0,
        "the fixture must erase something: {outcome:?}"
    );
}

/// Seed a terminal run in `state` and erase its payloads.
async fn seed_erased_execution(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    state: &str,
) -> ExecutionId {
    let exec_id = seed_terminal_execution(conn, workflow_id, state).await;
    erase(conn, exec_id).await;
    exec_id
}

/// Find the batch item for `exec_id`.
fn batch_item(body: &Value, exec_id: ExecutionId) -> Value {
    body["items"]
        .as_array()
        .expect("items array")
        .iter()
        .find(|item| item["exec_id"] == json!(exec_id.to_string()))
        .cloned()
        .unwrap_or_else(|| panic!("no batch item for {exec_id}: {body}"))
}

/// Count the reset forks of `exec_id`. A fork names its source in
/// `start_source_ref` (issue #740).
async fn fork_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::start_source.eq(Some("reset")))
        .filter(harvest_workflow_executions::start_source_ref.eq(Some(exec_id.to_string())))
        .count()
        .get_result(conn)
        .await
        .expect("count forks")
}

fn batch_reset_body(preview: bool) -> Value {
    json!({
        "filter": {
            "workflow_name": "resettable",
            "states": ["FAILED", "CANCELLED", "TIMED_OUT"]
        },
        "reset_point": { "type": "event_id", "event_id": 0 },
        "reason": "replay the failed cohort",
        "operator_id": "oncall",
        "preview": preview
    })
}

fn terminal_reset_request() -> WorkflowResetRequest {
    WorkflowResetRequest {
        reset_to_event_id: Some(0),
        reset_point: None,
        reason: "fork a terminal run".to_string(),
        operator_id: "oncall".to_string(),
        signal_reapply: ResetSignalReapplyPolicy::default(),
        allow_terminal_source: true,
    }
}

/// The engine refuses an erased source with no opt-in. This is what keeps
/// every fork path safe, a future one included.
#[tokio::test]
async fn reset_refuses_an_erased_source_with_no_opt_in() {
    let (url, _container) = setup_database().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let exec_id = seed_erased_execution(&mut conn, "wf-erased-engine", "FAILED").await;

    let error = reset_workflow_execution(&mut conn, exec_id, terminal_reset_request(), None)
        .await
        .expect_err("a fork over an erased source must be refused");

    assert!(
        matches!(error, WorkflowResetError::ErasedSource { .. }),
        "expected ErasedSource, got: {error:?}"
    );
    assert_eq!(fork_count(&mut conn, exec_id).await, 0, "no fork exists");
}

/// A dry run must return the rejection the real reset returns. Otherwise a
/// preview approves a fork that the reset then refuses.
#[tokio::test]
async fn preview_refuses_an_erased_source() {
    let (url, _container) = setup_database().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let exec_id = seed_erased_execution(&mut conn, "wf-erased-preview", "FAILED").await;

    let error = preview_workflow_reset(&mut conn, exec_id, terminal_reset_request())
        .await
        .expect_err("a preview over an erased source must be refused");

    assert!(
        matches!(error, WorkflowResetError::ErasedSource { .. }),
        "expected ErasedSource, got: {error:?}"
    );
}

/// A batch reset must not fork a PII-erased run. The fork would resume on
/// tombstones. The erased run is skipped with a typed reason. The intact run
/// in the same cohort still resets.
#[tokio::test]
async fn batch_reset_skips_an_erased_source_and_resets_the_rest() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let erased = seed_erased_execution(&mut conn, "wf-batch-erased", "TIMED_OUT").await;
    let intact = seed_terminal_execution(&mut conn, "wf-batch-intact", "FAILED").await;

    let (status, body) = post_json(&app, "/workflows/batch_reset", batch_reset_body(false)).await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], json!(2), "body: {body}");
    assert_eq!(body["reset_count"], json!(1), "body: {body}");
    assert_eq!(body["skipped_count"], json!(1), "body: {body}");

    let skipped = batch_item(&body, erased);
    assert_eq!(skipped["outcome"], json!("skipped"), "item: {skipped}");
    assert_eq!(
        skipped["skip_reason"],
        json!({ "type": "erased_source" }),
        "the skip must name erasure, not an infrastructure error: {skipped}"
    );
    assert!(skipped.get("new_exec_id").is_none(), "item: {skipped}");
    assert_eq!(
        fork_count(&mut conn, erased).await,
        0,
        "no fork of the erased run"
    );

    let reset = batch_item(&body, intact);
    assert_eq!(reset["outcome"], json!("reset"), "item: {reset}");
    assert_eq!(
        fork_count(&mut conn, intact).await,
        1,
        "the intact run forks"
    );
}

/// The dry run must predict the real outcome. It reports the erased run as
/// skipped with the same typed reason.
#[tokio::test]
async fn batch_reset_preview_reports_an_erased_source_as_skipped() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let erased = seed_erased_execution(&mut conn, "wf-batch-preview-erased", "CANCELLED").await;
    let intact = seed_terminal_execution(&mut conn, "wf-batch-preview-intact", "FAILED").await;

    let (status, body) = post_json(&app, "/workflows/batch_reset", batch_reset_body(true)).await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], json!(2), "body: {body}");
    assert_eq!(body["reset_count"], json!(0), "body: {body}");
    assert_eq!(body["skipped_count"], json!(1), "body: {body}");
    let skipped = batch_item(&body, erased);
    assert_eq!(skipped["outcome"], json!("skipped"), "item: {skipped}");
    assert_eq!(skipped["skip_reason"], json!({ "type": "erased_source" }));
    assert_eq!(batch_item(&body, intact)["outcome"], json!("previewed"));
    assert_eq!(
        fork_count(&mut conn, erased).await,
        0,
        "a preview forks nothing"
    );
    assert_eq!(
        fork_count(&mut conn, intact).await,
        0,
        "a preview forks nothing"
    );
}

/// Count the backends that wait for a lock.
async fn lock_waiters(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Waiters {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT count(*) AS n FROM pg_locks WHERE NOT granted")
        .get_result::<Waiters>(conn)
        .await
        .expect("read pg_locks")
        .n
}

/// An erasure can commit after the batch resolve and before the fork lock.
/// The fork must refuse, and the item must keep the typed reason.
///
/// The erasure runs in an open transaction, so it holds the row lock. The
/// unlocked resolve reads the intact row and passes. The fork then waits on
/// the row lock. The erasure commits, and the fork reads the tombstone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_reset_refuses_an_erasure_that_lands_before_the_fork_lock() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    let exec_id = seed_terminal_execution(&mut conn, "wf-batch-race", "FAILED").await;

    let (erased_tx, erased_rx) = tokio::sync::oneshot::channel::<()>();
    let (commit_tx, commit_rx) = tokio::sync::oneshot::channel::<()>();
    let eraser = tokio::spawn({
        let url = url.clone();
        async move {
            let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
                .await
                .expect("connect eraser");
            conn.transaction::<(), diesel::result::Error, _>(async move |conn| {
                erase(conn, exec_id).await;
                erased_tx.send(()).expect("signal erased");
                commit_rx.await.expect("commit signal");
                Ok(())
            })
            .await
            .expect("commit erasure");
        }
    });
    erased_rx.await.expect("erasure is open");

    let batch = tokio::spawn({
        let app = app.clone();
        async move { post_json(&app, "/workflows/batch_reset", batch_reset_body(false)).await }
    });

    // Wait until the fork blocks on the row lock that the erasure holds.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while lock_waiters(&mut conn).await == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the fork never waited on the row lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    commit_tx.send(()).expect("release the erasure");
    eraser.await.expect("eraser task");
    let (status, body) = batch.await.expect("batch task");

    assert_eq!(status, StatusCode::OK, "body: {body}");
    let item = batch_item(&body, exec_id);
    assert_eq!(item["outcome"], json!("skipped"), "item: {item}");
    assert_eq!(
        item["skip_reason"],
        json!({ "type": "erased_source" }),
        "a refusal at the fork must stay typed: {item}"
    );
    // Only a fork-time skip carries the resolved id. This proves the fork ran.
    assert_eq!(item["resolved_event_id"], json!(0), "item: {item}");
    assert_eq!(fork_count(&mut conn, exec_id).await, 0, "no fork exists");
}
