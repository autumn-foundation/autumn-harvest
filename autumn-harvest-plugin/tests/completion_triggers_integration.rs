//! Integration tests for Declarative Completion Triggers (issue #517).

#![allow(
    clippy::similar_names,
    clippy::redundant_clone,
    clippy::await_holding_lock,
    clippy::uninlined_format_args,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::completion_trigger::{TerminalState, evaluate_triggers_for_execution};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    StartWorkflowParams, start_or_load_workflow_execution, terminate_workflow_execution,
};
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};

static TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
use autumn_harvest_plugin::HarvestDbPool;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
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
    include_str!(
        "../../autumn-harvest/migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"
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
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260501020000_harvest_batch_processed_ids/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
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
    include_str!("../../autumn-harvest/migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"
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
    include_str!("../../autumn-harvest/migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000000_harvest_schedule_auto_pause/up.sql"
    ),
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
    include_str!("../../autumn-harvest/migrations/20260615000001_harvest_context_headers/up.sql")
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

async fn setup_sharded_databases() -> ((String, String), ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let shard0_db = "shard_zero";
    let shard1_db = "shard_one";

    let mut admin_conn = AsyncPgConnection::establish(&admin_url).await.unwrap();
    diesel::sql_query(format!("CREATE DATABASE {shard0_db}"))
        .execute(&mut admin_conn)
        .await
        .unwrap();
    diesel::sql_query(format!("CREATE DATABASE {shard1_db}"))
        .execute(&mut admin_conn)
        .await
        .unwrap();

    let shard0_url = format!("postgres://postgres:postgres@{host}:{port}/{shard0_db}");
    let shard1_url = format!("postgres://postgres:postgres@{host}:{port}/{shard1_db}");

    for url in [&shard0_url, &shard1_url] {
        let mut conn = AsyncPgConnection::establish(url).await.unwrap();
        conn.batch_execute(INIT_SQL).await.unwrap();
    }

    ((shard0_url, shard1_url), container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn test_workflow<'a>(
    _ctx: &'a autumn_harvest::context::WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

fn target_schema_fn() -> Value {
    json!({
        "type": "object",
        "properties": {
            "email": { "type": "string" }
        },
        "required": ["email"]
    })
}

fn test_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                name: "source_wf",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
                owner: None,
                runbook_url: None,
                severity: None,
            },
            WorkflowInfo {
                name: "target_wf",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
                owner: None,
                runbook_url: None,
                severity: None,
            },
            WorkflowInfo {
                name: "target_with_schema_wf",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                max_input_bytes: None,
                description: None,
                input_schema: Some(target_schema_fn),
                output_schema: None,
                error_schema: None,
                retry_policy: None,
                owner: None,
                runbook_url: None,
                severity: None,
            },
            // Workflows exercised by the terminate-trigger test (issue #504):
            // a source whose force-terminate fires `Terminated` triggers, plus the
            // two distinct targets the test registers triggers against.
            WorkflowInfo {
                name: "term_source_wf",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
                owner: None,
                runbook_url: None,
                severity: None,
            },
            WorkflowInfo {
                name: "on_terminate_wf",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
                owner: None,
                runbook_url: None,
                severity: None,
            },
            WorkflowInfo {
                name: "on_cancel_wf",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
                owner: None,
                runbook_url: None,
                severity: None,
            },
        ],
        vec![],
    ))
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("completion-trigger-test".to_string()),
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
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response is JSON")
    };
    (status, json)
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
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(e) => {
                panic!(
                    "Failed to parse JSON response. Status: {}. Body: {}. Error: {}",
                    status,
                    String::from_utf8_lossy(&bytes),
                    e
                );
            }
        }
    };
    (status, json)
}

#[tokio::test]
async fn test_completion_triggers_crud_api() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // 1. GET lists empty triggers initially
    let (status, list) = get_json(&app, "/admin/completion-triggers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list, json!([]));

    // 2. POST creates a completion trigger
    let trigger_id = uuid::Uuid::new_v4();
    let (status, created) = post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["id"], trigger_id.to_string());
    assert_eq!(created["source_workflow_name"], "source_wf");
    assert_eq!(created["target_workflow_name"], "target_wf");
    assert_eq!(created["terminal_states"], json!(["Completed"]));
    assert_eq!(created["input_mapping"], json!({"type": "Passthrough"}));

    // 3. GET lists the created trigger
    let (status, list) = get_json(&app, "/admin/completion-triggers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["id"], trigger_id.to_string());
}

#[tokio::test]
async fn test_trigger_evaluations_same_shard() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = pool.get().await.unwrap();

    let trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // Start a source workflow
    let source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-1",
            exec_id: source_exec_id,
            input: json!({"hello": "world"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    // Verify it is in RUNNING state
    let exec: WorkflowExecution = harvest_workflow_executions::table
        .find(source_exec_id.as_uuid())
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(exec.state, "RUNNING");

    // Transition it to COMPLETED with output
    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!({"result": "done"}))),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Evaluate trigger
    evaluate_triggers_for_execution(&mut conn, source_exec_id, TerminalState::Completed, None)
        .await
        .unwrap();

    // Verify target workflow has been started idempotently on shard 0
    let target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
    let target_exec: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id))
        .first(&mut conn)
        .await
        .unwrap();

    assert_eq!(target_exec.workflow_name, "target_wf");
    assert_eq!(target_exec.input, json!({"result": "done"}));
    assert_eq!(target_exec.state, "RUNNING");
}

/// issue #504: a force-terminate fires `Terminated` completion triggers, NOT
/// `Cancelled` ones — terminate is distinct from a cooperative cancellation
/// downstream.
#[tokio::test]
async fn test_terminate_fires_terminated_trigger_not_cancelled() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = pool.get().await.unwrap();

    // A trigger that should fire on terminate, and one that should NOT.
    let terminated_trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": terminated_trigger_id,
            "source_workflow_name": "term_source_wf",
            "terminal_states": ["Terminated"],
            "target_workflow_name": "on_terminate_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;
    let cancelled_trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": cancelled_trigger_id,
            "source_workflow_name": "term_source_wf",
            "terminal_states": ["Cancelled"],
            "target_workflow_name": "on_cancel_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // Start a RUNNING source workflow.
    let source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "term_source_wf",
            workflow_id: "term-source-1",
            exec_id: source_exec_id,
            input: json!({"hello": "world"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    // Force-terminate it. Same-shard trigger targets are started inline within
    // the terminate transaction, so we can assert synchronously.
    terminate_workflow_execution(
        &mut conn,
        source_exec_id,
        "operator kill",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .unwrap();

    let source: WorkflowExecution = harvest_workflow_executions::table
        .find(source_exec_id.as_uuid())
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(source.state, "TERMINATED");

    // The `["Terminated"]` trigger fired: its target run exists.
    let terminated_target_id = format!(
        "completion-trigger-{}-{}",
        terminated_trigger_id, source_exec_id
    );
    let terminated_target: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&terminated_target_id))
        .first(&mut conn)
        .await
        .expect("Terminated trigger must fire on a force-terminate");
    assert_eq!(terminated_target.workflow_name, "on_terminate_wf");
    assert_eq!(terminated_target.state, "RUNNING");

    // The `["Cancelled"]` trigger did NOT fire: no target run exists.
    let cancelled_target_id = format!(
        "completion-trigger-{}-{}",
        cancelled_trigger_id, source_exec_id
    );
    let cancelled_target = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&cancelled_target_id))
        .first::<WorkflowExecution>(&mut conn)
        .await
        .optional()
        .unwrap();
    assert!(
        cancelled_target.is_none(),
        "a Cancelled trigger must NOT fire on a force-terminate"
    );
}

#[tokio::test]
async fn test_trigger_input_mapping_static_and_projection() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = pool.get().await.unwrap();

    // 1. Static input mapping
    let static_trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": static_trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Static", "data": {"fixed": "payload"}}
        }),
    )
    .await;

    let source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-static",
            exec_id: source_exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!({"ignored": true}))),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    evaluate_triggers_for_execution(&mut conn, source_exec_id, TerminalState::Completed, None)
        .await
        .unwrap();

    let target_wf_id_static = format!(
        "completion-trigger-{}-{}",
        static_trigger_id, source_exec_id
    );
    let target_exec_static: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_wf_id_static))
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(target_exec_static.input, json!({"fixed": "payload"}));

    // 2. Projection input mapping
    let projection_trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": projection_trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Projection", "data": "response.nested.val"}
        }),
    )
    .await;

    let source_exec_id_proj = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-proj",
            exec_id: source_exec_id_proj,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    diesel::update(harvest_workflow_executions::table.find(source_exec_id_proj.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!({
                "response": {
                    "nested": {
                        "val": 999
                    }
                }
            }))),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id_proj,
        TerminalState::Completed,
        None,
    )
    .await
    .unwrap();

    let target_wf_id_proj = format!(
        "completion-trigger-{}-{}",
        projection_trigger_id, source_exec_id_proj
    );
    let target_exec_proj: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_wf_id_proj))
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(target_exec_proj.input, json!(999));
}

#[tokio::test]
async fn test_trigger_state_matching_and_deduplication() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = pool.get().await.unwrap();

    let trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Failed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // Start source workflow
    let source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-matching",
            exec_id: source_exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    // 1. Transition to COMPLETED (not matched by trigger terminal_states: FAILED)
    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    evaluate_triggers_for_execution(&mut conn, source_exec_id, TerminalState::Completed, None)
        .await
        .unwrap();

    // Target should NOT be started
    let target_wf_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
    let target_exists = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_wf_id))
        .first::<WorkflowExecution>(&mut conn)
        .await
        .optional()
        .unwrap()
        .is_some();
    assert!(!target_exists);

    // 2. Transition to FAILED (matched by trigger)
    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("FAILED"),
            harvest_workflow_executions::error.eq(Some("failed trigger".to_string())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    evaluate_triggers_for_execution(&mut conn, source_exec_id, TerminalState::Failed, None)
        .await
        .unwrap();

    // Target SHOULD be started
    let target_exec = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_wf_id))
        .first::<WorkflowExecution>(&mut conn)
        .await
        .unwrap();
    assert_eq!(target_exec.state, "RUNNING");

    // 3. Deduplication: run evaluate again for FAILED
    evaluate_triggers_for_execution(&mut conn, source_exec_id, TerminalState::Failed, None)
        .await
        .unwrap();

    // Target count should still be exactly 1
    let target_count: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_wf_id))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(target_count, 1);
}

#[tokio::test]
async fn test_trigger_cross_shard() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let pool0 = build_pool(&shard0_url);
    let pool1 = build_pool(&shard1_url);

    // Setup sharded database pool and router
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool0.clone());
    pools.insert(ShardId::new(1), pool1.clone());
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));

    // Install router
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded_pool));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("completion-trigger-test-sharded".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router.clone(),
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    let mut trigger_id = uuid::Uuid::new_v4();
    let mut source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);

    // Find trigger_id and source_exec_id such that the target routes to shard 1
    for _ in 0..10000 {
        trigger_id = uuid::Uuid::new_v4();
        source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
        if router
            .pick_for_new_workflow("target_wf", &target_workflow_id)
            .as_i32()
            == 1
        {
            break;
        }
    }

    let target_shard = router.pick_for_new_workflow("target_wf", &target_workflow_id);
    assert_eq!(
        target_shard.as_i32(),
        1,
        "Verify routing is sharded properly in test environment"
    );

    let (status, _res) = post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CREATED);

    // Start a source workflow on Shard 0
    let mut conn0 = pool0.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn0,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-sharded",
            exec_id: source_exec_id,
            input: json!({"data": "sharded"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    // Complete source workflow on Shard 0
    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!({"status": "ok"}))),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn0)
        .await
        .unwrap();

    let deferred =
        evaluate_triggers_for_execution(&mut conn0, source_exec_id, TerminalState::Completed, None)
            .await
            .unwrap();
    for start in deferred {
        start.spawn();
    }

    // Since cross-shard triggers run asynchronously via tokio::spawn, wait briefly
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Verify target workflow has been started on Shard 1's database
    let mut conn1 = pool1.get().await.unwrap();
    let target_exec: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id))
        .first(&mut conn1)
        .await
        .unwrap();

    assert_eq!(target_exec.workflow_name, "target_wf");
    assert_eq!(target_exec.input, json!({"status": "ok"}));
    assert_eq!(target_exec.state, "RUNNING");
}

#[tokio::test]
async fn test_completion_trigger_via_worker_run() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = pool.get().await.unwrap();

    // Setup trigger
    let trigger_id = uuid::Uuid::new_v4();
    let (status, res) = post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::CREATED,
        "Response was {:?}",
        res
    );

    // Start source workflow execution
    let source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-worker",
            exec_id: source_exec_id,
            input: json!({"worker": "processed"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    // Build and run the worker to process the workflow task to completion
    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "test-completion-worker".to_string();
    runtime_config.queues = vec!["default".to_string()];
    runtime_config.poll_interval = Duration::from_millis(20);
    runtime_config.shard_assignments = vec![ShardId::new(0)];

    let worker = Arc::new(Worker::new(runtime_config, test_registry()).unwrap());
    let worker_handle = {
        let worker = worker.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            worker.run(&pool).await;
        })
    };

    // Wait until the source workflow execution transitions to COMPLETED
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut conn = pool.get().await.unwrap();
            let exec: WorkflowExecution = harvest_workflow_executions::table
                .find(source_exec_id.as_uuid())
                .first(&mut conn)
                .await
                .unwrap();
            if exec.state == "COMPLETED" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();

    // Worker should shut down
    worker.shutdown();
    let _ = worker_handle.await;

    // Verify the target workflow target_wf was automatically started on completion!
    let target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
    let target_exec: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id))
        .first(&mut conn)
        .await
        .unwrap();

    assert_eq!(target_exec.workflow_name, "target_wf");
    assert_eq!(target_exec.input, json!({"worker": "processed"}));
    assert!(
        target_exec.state == "RUNNING" || target_exec.state == "COMPLETED",
        "State was {}",
        target_exec.state
    );
}

#[tokio::test]
async fn test_trigger_with_custom_queue() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = pool.get().await.unwrap();

    let trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"},
            "queue_name": "custom-queue"
        }),
    )
    .await;

    // Start source workflow
    let source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-queue-test",
            exec_id: source_exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    // Transition source to COMPLETED
    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Evaluate trigger
    let deferred =
        evaluate_triggers_for_execution(&mut conn, source_exec_id, TerminalState::Completed, None)
            .await
            .unwrap();
    for start in deferred {
        start.spawn();
    }

    // Verify target workflow has been started on queue "custom-queue"
    let target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
    let target_exec: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id))
        .first(&mut conn)
        .await
        .unwrap();

    assert_eq!(target_exec.queue_name, "custom-queue");
}

#[tokio::test]
async fn test_static_trigger_sync_and_cleanup() {
    use autumn_harvest::schema::harvest_completion_triggers::dsl as triggers_dsl;
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();

    let trigger_id = uuid::Uuid::new_v4();
    let trigger = autumn_harvest::completion_trigger::CompletionTrigger {
        id: trigger_id,
        source_workflow_name: "source_wf".to_string(),
        terminal_states: vec![TerminalState::Completed],
        target_workflow_name: "target_wf".to_string(),
        input_mapping: autumn_harvest::completion_trigger::InputMapping::Passthrough,
        queue_name: None,
    };

    // 1. Sync one trigger
    autumn_harvest::completion_trigger::sync_completion_triggers(&mut conn, &[trigger])
        .await
        .unwrap();

    // Verify it is present and is_static = true
    let row: autumn_harvest::models::CompletionTriggerDb =
        triggers_dsl::harvest_completion_triggers
            .find(trigger_id)
            .first(&mut conn)
            .await
            .unwrap();
    assert!(row.is_static);

    // 2. Sync empty list
    autumn_harvest::completion_trigger::sync_completion_triggers(&mut conn, &[])
        .await
        .unwrap();

    // Verify it is gone
    let exists = triggers_dsl::harvest_completion_triggers
        .find(trigger_id)
        .first::<autumn_harvest::models::CompletionTriggerDb>(&mut conn)
        .await
        .optional()
        .unwrap()
        .is_some();
    assert!(!exists);
}

#[tokio::test]
async fn test_trigger_outbox_retry_and_sweep() {
    use autumn_harvest::schema::harvest_completion_trigger_outbox::dsl as outbox_dsl;
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let pool0 = build_pool(&shard0_url);
    let pool1 = build_pool(&shard1_url);

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool0.clone());
    pools.insert(ShardId::new(1), pool1.clone());
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));

    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded_pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("completion-trigger-test-outbox".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router.clone(),
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    let mut trigger_id = uuid::Uuid::new_v4();
    let mut source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);

    // Ensure routing routes target to shard 1
    for _ in 0..10000 {
        trigger_id = uuid::Uuid::new_v4();
        source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
        if router
            .pick_for_new_workflow("target_wf", &target_workflow_id)
            .as_i32()
            == 1
        {
            break;
        }
    }

    // Register trigger
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // Start source on Shard 0
    let mut conn0 = pool0.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn0,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-outbox",
            exec_id: source_exec_id,
            input: json!({"key": "val"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    // Complete source workflow
    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!({"out": "yes"}))),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn0)
        .await
        .unwrap();

    // Evaluate trigger - this inserts into outbox inside source transaction because target is cross-shard (Shard 1)
    let deferred =
        evaluate_triggers_for_execution(&mut conn0, source_exec_id, TerminalState::Completed, None)
            .await
            .unwrap();
    assert_eq!(deferred.len(), 1);

    // Verify outbox row exists in Shard 0
    let outbox_exists = outbox_dsl::harvest_completion_trigger_outbox
        .filter(outbox_dsl::source_exec_id.eq(source_exec_id.as_uuid()))
        .first::<autumn_harvest::models::CompletionTriggerOutboxDb>(&mut conn0)
        .await
        .optional()
        .unwrap()
        .is_some();
    assert!(outbox_exists);

    // 1. Run outbox sweep with an incomplete/failing sharded pool mapping (mocking transient connection failure to Shard 1)
    let mut bad_pools = BTreeMap::new();
    bad_pools.insert(ShardId::new(0), pool0.clone());
    // Shard 1 is missing, so sweep cannot start workflow on Shard 1
    let bad_sharded_pool = ShardedDbPool::from_map(bad_pools, ShardId::new(0));

    let sweep_res = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut conn0,
        &Some(bad_sharded_pool),
        &[ShardId::new(1)], // sweep targets Shard 1
    )
    .await
    .unwrap();
    assert_eq!(sweep_res, 0); // nothing completed successfully

    // Outbox row should still be there
    let outbox_still_exists = outbox_dsl::harvest_completion_trigger_outbox
        .filter(outbox_dsl::source_exec_id.eq(source_exec_id.as_uuid()))
        .first::<autumn_harvest::models::CompletionTriggerOutboxDb>(&mut conn0)
        .await
        .optional()
        .unwrap()
        .is_some();
    assert!(outbox_still_exists);

    // 2. Now run outbox sweep with the correct/working sharded pool
    let sweep_res_success = autumn_harvest::completion_trigger::enforce_completion_triggers_outbox(
        &mut conn0,
        &Some(sharded_pool),
        &[ShardId::new(1)],
    )
    .await
    .unwrap();
    assert_eq!(sweep_res_success, 1); // 1 task succeeded

    // Outbox row should be deleted
    let outbox_deleted = outbox_dsl::harvest_completion_trigger_outbox
        .filter(outbox_dsl::source_exec_id.eq(source_exec_id.as_uuid()))
        .first::<autumn_harvest::models::CompletionTriggerOutboxDb>(&mut conn0)
        .await
        .optional()
        .unwrap()
        .is_none();
    assert!(outbox_deleted);

    // Verify target workflow execution is started on Shard 1
    let mut conn1 = pool1.get().await.unwrap();
    let target_exec: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id))
        .first(&mut conn1)
        .await
        .unwrap();
    assert_eq!(target_exec.input, json!({"out": "yes"}));
}

#[tokio::test]
async fn test_trigger_cross_shard_queue_preservation() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let pool0 = build_pool(&shard0_url);
    let pool1 = build_pool(&shard1_url);

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool0.clone());
    pools.insert(ShardId::new(1), pool1.clone());
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));

    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded_pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("completion-trigger-test-queue".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router.clone(),
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    let mut trigger_id = uuid::Uuid::new_v4();
    let mut source_exec_id = ExecutionId::new_for_shard(ShardId::new(1)); // Source runs on Shard 1
    let mut target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);

    // Target routes to Shard 1 as well
    for _ in 0..10000 {
        trigger_id = uuid::Uuid::new_v4();
        source_exec_id = ExecutionId::new_for_shard(ShardId::new(1));
        target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
        if router
            .pick_for_new_workflow("target_wf", &target_workflow_id)
            .as_i32()
            == 1
        {
            break;
        }
    }

    // Register trigger without explicit queue
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // Create a target workflow schedule on Shard 0 (default shard) with a custom queue
    let mut conn0 = pool0.get().await.unwrap();
    let ws = autumn_harvest::policy::WorkflowSchedule {
        workflow_name: "target_wf".to_string(),
        dag_name: None,
        schedule: autumn_harvest::policy::Schedule::Interval(Duration::from_secs(3600)),
        input: json!({}),
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "target-custom-queue".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 10,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    autumn_harvest::register_workflow_schedules(&mut conn0, &[ws])
        .await
        .unwrap();

    // Start source on Shard 1
    let mut conn1 = pool1.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn1,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-queue-lookup",
            exec_id: source_exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    // Complete source workflow on Shard 1
    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn1)
        .await
        .unwrap();

    // Evaluate trigger on Shard 1 (local execution)
    let deferred =
        evaluate_triggers_for_execution(&mut conn1, source_exec_id, TerminalState::Completed, None)
            .await
            .unwrap();
    for start in deferred {
        start.spawn();
    }

    // Since starts asynchronously, wait briefly
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Verify target execution was started on Shard 1, preserving schedule's "target-custom-queue" queue resolved from Shard 0!
    let target_exec: WorkflowExecution = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id))
        .first(&mut conn1)
        .await
        .unwrap();
    assert_eq!(target_exec.queue_name, "target-custom-queue");
}

#[tokio::test]
async fn test_trigger_compensating_rollback() {
    use autumn_harvest::schema::harvest_completion_triggers::dsl as triggers_dsl;
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let ((shard0_url, _shard1_url), _container) = setup_sharded_databases().await;
    let pool0 = build_pool(&shard0_url);
    // shard 1 pool: we'll create a bad pool pointing to a non-existent port/host to trigger connection failure
    let bad_pool1 = build_pool("postgres://postgres:postgres@localhost:12345/non_existent");

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool0.clone());
    pools.insert(ShardId::new(1), bad_pool1.clone());
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));

    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded_pool));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("completion-trigger-test-rollback".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router.clone(),
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    let trigger_id = uuid::Uuid::new_v4();

    // Post to register trigger - shard 0 should succeed, but shard 1 should fail.
    // This should trigger compensating rollback, deleting the row from shard 0.
    let (status, _err_res) = post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // Verify it failed
    assert_ne!(status, StatusCode::CREATED);

    // Verify trigger is NOT in Shard 0 (rolled back successfully!)
    let mut conn0 = pool0.get().await.unwrap();
    let exists = triggers_dsl::harvest_completion_triggers
        .find(trigger_id)
        .first::<autumn_harvest::models::CompletionTriggerDb>(&mut conn0)
        .await
        .optional()
        .unwrap()
        .is_some();
    assert!(!exists);
}

#[tokio::test]
async fn test_trigger_compensating_rollback_restores_existing() {
    use autumn_harvest::schema::harvest_completion_triggers::dsl as triggers_dsl;
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let pool0 = build_pool(&shard0_url);
    let pool1 = build_pool(&shard1_url);

    let trigger_id = uuid::Uuid::new_v4();

    // 1. Pre-register trigger on both shard 0 and shard 1 with initial definition
    for pool in [&pool0, &pool1] {
        let mut conn = pool.get().await.unwrap();
        diesel::insert_into(autumn_harvest::schema::harvest_completion_triggers::table)
            .values(&autumn_harvest::models::NewCompletionTriggerDb {
                id: trigger_id,
                source_workflow_name: "initial_source_wf".to_string(),
                terminal_states: json!(["Completed"]),
                target_workflow_name: "initial_target_wf".to_string(),
                input_mapping: json!({"type": "Passthrough"}),
                queue_name: None,
                is_static: false,
            })
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // Now, setup bad pool1 for shard 1 to trigger connection/update failure on shard 1
    let bad_pool1 = build_pool("postgres://postgres:postgres@localhost:12345/non_existent");

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool0.clone());
    pools.insert(ShardId::new(1), bad_pool1.clone());
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));

    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded_pool));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("completion-trigger-test-rollback-restore".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router.clone(),
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    // 2. Post to /admin/completion-triggers with updated values.
    // Shard 0 will succeed to update, but Shard 1 will fail.
    // This should trigger compensating rollback, updating Shard 0 back to "initial_source_wf"!
    let (status, _err_res) = post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "updated_source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "updated_target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // Verify request failed
    assert_ne!(status, StatusCode::CREATED);

    // Verify Shard 0 trigger was restored to its pre-existing definition (initial_source_wf)
    let mut conn0 = pool0.get().await.unwrap();
    let trigger: autumn_harvest::models::CompletionTriggerDb =
        triggers_dsl::harvest_completion_triggers
            .find(trigger_id)
            .first::<autumn_harvest::models::CompletionTriggerDb>(&mut conn0)
            .await
            .unwrap();

    assert_eq!(trigger.source_workflow_name, "initial_source_wf");
    assert_eq!(trigger.target_workflow_name, "initial_target_wf");
}

#[tokio::test]
async fn test_exact_pool_routing_cross_shard() {
    use autumn_harvest::completion_trigger::enforce_completion_triggers_outbox;
    use autumn_harvest::completion_trigger::evaluate_triggers_for_execution;
    use autumn_harvest::schema::harvest_completion_trigger_outbox::dsl as outbox_dsl;
    use autumn_harvest::schema::harvest_workflow_executions::dsl as execs_dsl;

    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let pool0 = build_pool(&shard0_url);
    let pool1 = build_pool(&shard1_url);

    // Setup sharded database pool and router
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool0.clone());
    pools.insert(ShardId::new(1), pool1.clone());
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));

    // Install router
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    // Set globally
    if let Ok(mut lock) = autumn_harvest::shard::GLOBAL_SHARDED_POOL.write() {
        *lock = Some(sharded_pool.clone());
    }
    autumn_harvest::shard::install_global_router(router.clone());

    let mut trigger_id = uuid::Uuid::new_v4();
    let mut source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);

    // Find trigger_id and source_exec_id such that the target routes to shard 1
    for _ in 0..10000 {
        trigger_id = uuid::Uuid::new_v4();
        source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
        if router
            .pick_for_new_workflow("target_wf", &target_workflow_id)
            .as_i32()
            == 1
        {
            break;
        }
    }

    // Insert trigger into Shard 0
    let mut conn0 = pool0.get().await.unwrap();
    diesel::insert_into(autumn_harvest::schema::harvest_completion_triggers::table)
        .values(&autumn_harvest::models::NewCompletionTriggerDb {
            id: trigger_id,
            source_workflow_name: "source_wf".to_string(),
            terminal_states: json!(["Completed"]),
            target_workflow_name: "target_wf".to_string(),
            input_mapping: json!({"type": "Passthrough"}),
            queue_name: None,
            is_static: false,
        })
        .execute(&mut conn0)
        .await
        .unwrap();

    // Insert completed source execution
    diesel::insert_into(execs_dsl::harvest_workflow_executions)
        .values((
            execs_dsl::id.eq(source_exec_id.as_uuid()),
            execs_dsl::workflow_name.eq("source_wf"),
            execs_dsl::workflow_id.eq("source-exec-1"),
            execs_dsl::run_id.eq(uuid::Uuid::new_v4()),
            execs_dsl::shard_id.eq(0),
            execs_dsl::state.eq("COMPLETED"),
            execs_dsl::input.eq(json!({})),
            execs_dsl::queue_name.eq("default"),
        ))
        .execute(&mut conn0)
        .await
        .unwrap();

    // Evaluate trigger - this should insert a row into outbox table since target routes to Shard 1
    let deferred =
        evaluate_triggers_for_execution(&mut conn0, source_exec_id, TerminalState::Completed, None)
            .await
            .unwrap();
    assert_eq!(deferred.len(), 1);

    // Verify outbox row exists in Shard 0
    let outbox_rows = outbox_dsl::harvest_completion_trigger_outbox
        .load::<autumn_harvest::models::CompletionTriggerOutboxDb>(&mut conn0)
        .await
        .unwrap();
    assert_eq!(outbox_rows.len(), 1);
    assert_eq!(outbox_rows[0].target_shard, 1);

    // Now, run the outbox sweep, but we mock the sharded_pool to ONLY have Shard 0 (e.g. simulating a worker that does not configure Shard 1 pool)
    let mut incomplete_pools = BTreeMap::new();
    incomplete_pools.insert(ShardId::new(0), pool0.clone());
    let incomplete_sharded_pool = Some(ShardedDbPool::from_map(incomplete_pools, ShardId::new(0)));

    let sweep_count = enforce_completion_triggers_outbox(
        &mut conn0,
        &incomplete_sharded_pool,
        &[ShardId::new(0), ShardId::new(1)],
    )
    .await
    .unwrap();

    // Verify it processed the row (returned count 0 since task was skipped/not processed)
    assert_eq!(sweep_count, 0);

    // Verify outbox row STILL exists in Shard 0 (it was skipped because Shard 1 pool was unavailable!)
    let outbox_rows_after = outbox_dsl::harvest_completion_trigger_outbox
        .load::<autumn_harvest::models::CompletionTriggerOutboxDb>(&mut conn0)
        .await
        .unwrap();
    assert_eq!(outbox_rows_after.len(), 1);
}

#[tokio::test]
async fn test_runner_startup_fails_on_sync_failure() {
    use autumn_harvest_plugin::{
        HarvestMode, HarvestRunner, HarvestRunnerResources, HarvestRuntimeConfig,
    };
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    // Create a pool that will immediately fail when a connection is retrieved
    let bad_pool = build_pool("postgres://postgres:postgres@localhost:12345/non_existent");

    let built = autumn_harvest::HarvestBuilder::new()
        .workflows(vec![
            autumn_harvest::info::WorkflowInfo {
                name: "source",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                debounce: None,
                batch: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
                owner: None,
                runbook_url: None,
                severity: None,
            },
            autumn_harvest::info::WorkflowInfo {
                name: "target",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                sla: None,
                concurrency: None,
                debounce: None,
                batch: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
                owner: None,
                runbook_url: None,
                severity: None,
            },
        ])
        .completion_triggers(vec![
            autumn_harvest::completion_trigger::CompletionTrigger {
                id: uuid::Uuid::new_v4(),
                source_workflow_name: "source".to_string(),
                terminal_states: vec![TerminalState::Completed],
                target_workflow_name: "target".to_string(),
                input_mapping: autumn_harvest::completion_trigger::InputMapping::Passthrough,
                queue_name: None,
            },
        ])
        .build();

    let result = HarvestRunner::start(
        built,
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some("postgres://postgres:postgres@localhost:12345/non_existent".to_string()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(bad_pool),
    )
    .await;

    // Verify it failed to start
    assert!(result.is_err());
    let err_str = result.err().unwrap().to_string();
    assert!(
        err_str.contains("Failed to get DB connection")
            || err_str.contains("sync completion triggers")
    );
}

#[tokio::test]
async fn test_trigger_evaluations_schema_validation() {
    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = pool.get().await.unwrap();

    let trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_with_schema_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // 1. Valid Input Path: should start target workflow
    let source_exec_id_valid = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-valid",
            exec_id: source_exec_id_valid,
            input: Value::Null,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    diesel::update(harvest_workflow_executions::table.find(source_exec_id_valid.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!({"email": "test@example.com"}))),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id_valid,
        TerminalState::Completed,
        None,
    )
    .await
    .unwrap();

    let target_workflow_id_valid =
        format!("completion-trigger-{}-{}", trigger_id, source_exec_id_valid);
    let target_exec_valid: Option<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id_valid))
        .first(&mut conn)
        .await
        .optional()
        .unwrap();
    assert!(target_exec_valid.is_some());
    let target_exec_valid = target_exec_valid.unwrap();
    assert_eq!(
        target_exec_valid.input,
        json!({"email": "test@example.com"})
    );

    // 2. Invalid Input Path: should skip starting target workflow
    let source_exec_id_invalid = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-invalid",
            exec_id: source_exec_id_invalid,
            input: Value::Null,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    diesel::update(harvest_workflow_executions::table.find(source_exec_id_invalid.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!({"username": "not_an_email"}))),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id_invalid,
        TerminalState::Completed,
        None,
    )
    .await
    .unwrap();

    let target_workflow_id_invalid = format!(
        "completion-trigger-{}-{}",
        trigger_id, source_exec_id_invalid
    );
    let target_exec_invalid: Option<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id_invalid))
        .first(&mut conn)
        .await
        .optional()
        .unwrap();
    assert!(target_exec_invalid.is_none());
}

/// AC #6 (issue #517): a per-trigger fire counter/metric
/// (`harvest.completion_trigger.fires{trigger, outcome}`) is emitted so
/// operators can confirm wiring. This drives the completion path twice and
/// asserts the recorder observes `started` on the first evaluation and
/// `deduped` on the (idempotent) second evaluation, both tagged with the
/// trigger id.
#[tokio::test]
async fn test_trigger_emits_fire_metric_outcomes() {
    // Minimal capturing MetricsRecorder — records only the completion-trigger
    // fire outcomes (all other trait methods keep their no-op defaults).
    #[derive(Default)]
    struct CapturingMetrics {
        fires: std::sync::Mutex<Vec<(String, String)>>,
    }
    impl autumn_harvest::telemetry::MetricsRecorder for CapturingMetrics {
        fn record_completion_trigger_fired(&self, trigger_id: &str, outcome: &str) {
            self.fires
                .lock()
                .unwrap()
                .push((trigger_id.to_string(), outcome.to_string()));
        }
    }

    let _lock = TEST_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = pool.get().await.unwrap();

    let trigger_id = uuid::Uuid::new_v4();
    post_json(
        &app,
        "/admin/completion-triggers",
        json!({
            "id": trigger_id,
            "source_workflow_name": "source_wf",
            "terminal_states": ["Completed"],
            "target_workflow_name": "target_wf",
            "input_mapping": {"type": "Passthrough"}
        }),
    )
    .await;

    // Start + complete a source workflow.
    let source_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "source_wf",
            workflow_id: "source-metric",
            exec_id: source_exec_id,
            input: json!({"hello": "metric"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
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
        },
    )
    .await
    .unwrap();

    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!({"result": "done"}))),
            harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    let metrics = CapturingMetrics::default();

    // First evaluation: the target is started → outcome "started".
    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .unwrap();

    // Second evaluation of the same (source_exec_id, trigger_id): the dedupe
    // ledger short-circuits → outcome "deduped". No second target is started.
    evaluate_triggers_for_execution(
        &mut conn,
        source_exec_id,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .unwrap();

    let fires = metrics.fires.lock().unwrap().clone();
    assert_eq!(
        fires,
        vec![
            (trigger_id.to_string(), "started".to_string()),
            (trigger_id.to_string(), "deduped".to_string()),
        ],
        "expected one `started` then one `deduped` fire metric for the trigger, got {fires:?}"
    );

    // Belt-and-suspenders: exactly one target execution exists despite two evals.
    let target_workflow_id = format!("completion-trigger-{}-{}", trigger_id, source_exec_id);
    let target_count: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&target_workflow_id))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(target_count, 1);
}
