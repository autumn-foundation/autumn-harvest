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
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
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
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
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
        "../../autumn-harvest/migrations/20260603000000_harvest_completion_triggers/up.sql"
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

async fn setup_sharded_databases() -> ((String, String), ContainerAsync<Postgres>) {
    let container = Postgres::default()
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

fn test_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                name: "source_wf",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                concurrency: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            },
            WorkflowInfo {
                name: "target_wf",
                module: "tests",
                handler: test_workflow,
                execution_timeout: None,
                concurrency: None,
                max_input_bytes: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
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
    let _lock = TEST_MUTEX.lock().unwrap();
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
    let _lock = TEST_MUTEX.lock().unwrap();
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

#[tokio::test]
async fn test_trigger_input_mapping_static_and_projection() {
    let _lock = TEST_MUTEX.lock().unwrap();
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
    let _lock = TEST_MUTEX.lock().unwrap();
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
    let _lock = TEST_MUTEX.lock().unwrap();
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

    evaluate_triggers_for_execution(&mut conn0, source_exec_id, TerminalState::Completed, None)
        .await
        .unwrap();

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
    let _lock = TEST_MUTEX.lock().unwrap();
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
