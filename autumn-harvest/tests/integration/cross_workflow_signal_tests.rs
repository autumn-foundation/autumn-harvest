#![cfg(feature = "db")]

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    HarvestBuilder, StartWorkflowParams, WorkerConfig, WorkflowContext,
    start_or_load_workflow_execution,
};

use diesel::prelude::*;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

static TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!("../../migrations/20260710000002_harvest_workflow_continue_chain/up.sql"),
);

// Rewrite the database (path) component of a Postgres URL, preserving scheme,
// authority (user/host/port), and any query string. Test-only helper for the
// local-Postgres path below.
fn rewrite_pg_db(base: &str, db: &str) -> String {
    let after_scheme = base.find("://").map_or(0, |i| i + 3);
    let rest = &base[after_scheme..];
    let (authority, tail) = rest
        .find('/')
        .map_or((rest, ""), |i| (&rest[..i], &rest[i + 1..]));
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{}{}/{}{}", &base[..after_scheme], authority, db, query)
}

async fn setup_test_database_url() -> (String, Option<ContainerAsync<Postgres>>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // Local-Postgres path (no Docker): when HARVEST_TEST_DATABASE_URL points at
    // a live Postgres, create a fresh per-test database, apply the full
    // migration bundle, and hand back its URL. CI leaves the env var unset and
    // uses the testcontainers path below (which is the authoritative path).
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        use diesel_async::SimpleAsyncConnection;
        let db_name = format!("harvest678_{}", uuid::Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&base_url)
            .await
            .expect("failed to connect to HARVEST_TEST_DATABASE_URL base");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("failed to create per-test database");
        let new_url = rewrite_pg_db(&base_url, &db_name);
        let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&new_url)
            .await
            .expect("failed to connect to per-test database");
        conn.batch_execute(INIT_SQL)
            .await
            .expect("failed to apply INIT_SQL to per-test database");
        return (new_url, None);
    }

    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, Some(container))
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn load_execution_from_url(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for execution query");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload workflow execution")
}

fn caller_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );
        ctx.signal_external_workflow(target, "my_signal", serde_json::json!({"data": "hello"}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"status": "signaled"}))
    })
}

fn target_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let val: serde_json::Value = ctx
            .receive_signal("my_signal")
            .await
            .map_err(|e| e.to_string())?;
        Ok(val)
    })
}

fn mixed_suspension_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );

        let timer_fut = ctx.timer("long_timer", 3600);
        let signal_fut =
            ctx.signal_external_workflow(target, "my_signal", serde_json::json!({"data": "hello"}));

        tokio::select! {
            res = timer_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "timer_fired"}))
            }
            res = signal_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "signaled"}))
            }
        }
    })
}

// A target that stays RUNNING (parked on a signal that never arrives) so an
// external cancel resolves against a live execution.
fn cancel_target_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _: serde_json::Value = ctx
            .receive_signal("never_arrives")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"status": "signalled"}))
    })
}

// select! between a 1-hour timer and request_cancel_external_workflow. Mirrors
// mixed_suspension_workflow but on the external-cancel primitive (issue #492).
fn mixed_suspension_cancel_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );

        let timer_fut = ctx.timer("long_timer", 3600);
        let cancel_fut = ctx.request_cancel_external_workflow(target);

        tokio::select! {
            res = timer_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "timer_fired"}))
            }
            res = cancel_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "cancel_resolved"}))
            }
        }
    })
}

// Compact WorkflowInfo builder mirroring the exact field set used by the inline
// struct literals elsewhere in this file (kept in one place so field drift
// breaks every test together rather than silently diverging).
fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "cross_workflow_signal_tests",
        handler,
        execution_timeout: None,
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

// Compact StartWorkflowParams builder mirroring the exact field set used by the
// inline struct literals elsewhere in this file.
fn mk_start_params(
    exec_id: ExecutionId,
    workflow_name: &'static str,
    workflow_id: &'static str,
    input: serde_json::Value,
) -> StartWorkflowParams<'static> {
    StartWorkflowParams {
        exec_id,
        workflow_name,
        workflow_id,
        input,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
    }
}

#[tokio::test]
async fn test_config_validation() {
    let config = WorkerConfig::default();
    assert_eq!(config.unknown_target_grace_window, Duration::from_secs(5));

    let config = config.with_unknown_target_grace_window(Duration::from_secs(42));
    assert_eq!(config.unknown_target_grace_window, Duration::from_secs(42));

    let built = HarvestBuilder::new().worker(config).build();
    let (_, _, _, worker_config) = built.into_worker_parts();
    assert_eq!(
        worker_config.unknown_target_grace_window,
        Duration::from_secs(42)
    );

    let runtime: WorkerRuntimeConfig = worker_config.into();
    assert_eq!(runtime.unknown_target_grace_window, Duration::from_secs(42));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_same_shard_not_found_retry() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Target workflow ID and ExecutionId (same shard, let's say Shard 0)
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    // Caller workflow ExecutionId
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            WorkflowInfo {
                mcp: false,
                name: "caller_workflow",
                module: "cross_workflow_signal_tests",
                handler: caller_workflow,
                execution_timeout: None,
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
            },
            WorkflowInfo {
                mcp: false,
                name: "target_workflow",
                module: "cross_workflow_signal_tests",
                handler: target_workflow,
                execution_timeout: None,
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
            },
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-same-shard".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Start caller first. Target does not exist yet!
    let mut conn = pool.get().await.unwrap();
    let start_params = StartWorkflowParams {
        exec_id: caller_exec_id,
        workflow_name: "caller_workflow",
        workflow_id: "caller-1",
        input: serde_json::json!({"target": target_exec_id.to_string()}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
    };
    start_or_load_workflow_execution(&mut conn, start_params)
        .await
        .unwrap();

    // Give it a moment to run and suspend (park), waiting for outbox delivery.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify caller execution is running/suspended (not failed immediately).
    let caller_state = load_execution_from_url(&database_url, caller_exec_id).await;
    assert_eq!(caller_state.state, "RUNNING");

    // Now start the target workflow.
    let start_target_params = StartWorkflowParams {
        exec_id: target_exec_id,
        workflow_name: "target_workflow",
        workflow_id: "target-1",
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
    };
    start_or_load_workflow_execution(&mut conn, start_target_params)
        .await
        .unwrap();

    // Wait for both workflows to complete.
    let completed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED" && target.state == "COMPLETED" {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("workflows should complete within timeout");

    assert_eq!(completed.0.state, "COMPLETED");
    assert_eq!(completed.1.state, "COMPLETED");

    worker.shutdown();
    let _ = handle.await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_cross_shard_outbox_delivery() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // Setup sharded pool with Shard 0 and Shard 1.
    // Both point to the same database (logical sharding mock).
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool.clone());
    pools.insert(ShardId::new(1), pool.clone());
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0));

    // Caller is in Shard 0, target is in Shard 1
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(1));

    let built = HarvestBuilder::new()
        .workflows(vec![
            WorkflowInfo {
                mcp: false,
                name: "caller_workflow",
                module: "cross_workflow_signal_tests",
                handler: caller_workflow,
                execution_timeout: None,
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
            },
            WorkflowInfo {
                mcp: false,
                name: "target_workflow",
                module: "cross_workflow_signal_tests",
                handler: target_workflow,
                execution_timeout: None,
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
            },
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-cross-shard".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();

    // Start target first
    let start_target_params = StartWorkflowParams {
        exec_id: target_exec_id,
        workflow_name: "target_workflow",
        workflow_id: "target-2",
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
    };
    start_or_load_workflow_execution(&mut conn, start_target_params)
        .await
        .unwrap();

    // Start caller
    let start_params = StartWorkflowParams {
        exec_id: caller_exec_id,
        workflow_name: "caller_workflow",
        workflow_id: "caller-2",
        input: serde_json::json!({"target": target_exec_id.to_string()}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
    };
    start_or_load_workflow_execution(&mut conn, start_params)
        .await
        .unwrap();

    // Wait for both workflows to complete.
    let completed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED" && target.state == "COMPLETED" {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("workflows should complete within timeout");

    assert_eq!(completed.0.state, "COMPLETED");
    assert_eq!(completed.1.state, "COMPLETED");

    worker.shutdown();
    let _ = handle.await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_grace_window_expiration() {
    let _guard = TEST_MUTEX.lock().await;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Target workflow ID does not exist, and caller has a 1 second grace window.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![WorkflowInfo {
            mcp: false,
            name: "caller_workflow",
            module: "cross_workflow_signal_tests",
            handler: caller_workflow,
            execution_timeout: None,
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
        }])
        .worker(WorkerConfig::default().with_unknown_target_grace_window(Duration::from_secs(1)))
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-grace-window".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();

    // Start caller
    let start_params = StartWorkflowParams {
        exec_id: caller_exec_id,
        workflow_name: "caller_workflow",
        workflow_id: "caller-3",
        input: serde_json::json!({"target": target_exec_id.to_string()}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
    };
    start_or_load_workflow_execution(&mut conn, start_params)
        .await
        .unwrap();

    // Sleep past the 1 second grace window.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Caller should have completed in FAILED state (or the workflow returned Err).
    let completed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            if caller.state == "FAILED" {
                break caller;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("caller should fail after grace window expiration");
    assert_eq!(completed.state, "FAILED");

    // The caller fails because the signal failed. Let's make sure history contains the failure event.
    let history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
        .await
        .unwrap();
    let failed_event = history
        .events
        .iter()
        .find(|e| matches!(e, WorkflowEvent::ExternalSignalFailed { .. }));
    assert!(
        failed_event.is_some(),
        "should have written ExternalSignalFailed event"
    );
    if let Some(WorkflowEvent::ExternalSignalFailed { reason_code, .. }) = failed_event {
        assert_eq!(reason_code, "target_unknown");
    }

    worker.shutdown();
    let _ = handle.await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_mixed_timer_suspension_signal_wakes_timer() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Target workflow ID and ExecutionId
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    // Caller workflow ExecutionId
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            WorkflowInfo {
                mcp: false,
                name: "mixed_suspension_workflow",
                module: "cross_workflow_signal_tests",
                handler: mixed_suspension_workflow,
                execution_timeout: None,
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
            },
            WorkflowInfo {
                mcp: false,
                name: "target_workflow",
                module: "cross_workflow_signal_tests",
                handler: target_workflow,
                execution_timeout: None,
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
            },
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-mixed-suspension".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Start caller first. Target does not exist yet!
    let mut conn = pool.get().await.unwrap();
    let start_params = StartWorkflowParams {
        exec_id: caller_exec_id,
        workflow_name: "mixed_suspension_workflow",
        workflow_id: "caller-mixed-1",
        input: serde_json::json!({"target": target_exec_id.to_string()}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
    };
    start_or_load_workflow_execution(&mut conn, start_params)
        .await
        .unwrap();

    // Give it a moment to run and suspend (park), waiting for outbox delivery/timer.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify caller execution is running/suspended (not failed immediately).
    let caller_state = load_execution_from_url(&database_url, caller_exec_id).await;
    assert_eq!(caller_state.state, "RUNNING");

    // Now start the target workflow.
    let start_target_params = StartWorkflowParams {
        exec_id: target_exec_id,
        workflow_name: "target_workflow",
        workflow_id: "target-mixed-1",
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
    };
    start_or_load_workflow_execution(&mut conn, start_target_params)
        .await
        .unwrap();

    // Wait for both workflows to complete.
    let completed = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED" && target.state == "COMPLETED" {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    if completed.is_err() {
        let caller = load_execution_from_url(&database_url, caller_exec_id).await;
        let target = load_execution_from_url(&database_url, target_exec_id).await;
        let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
            .await
            .unwrap();
        let target_history = autumn_harvest::store::load_history(&mut conn, target_exec_id)
            .await
            .unwrap();
        println!("TIMEOUT DIAGNOSTICS:");
        println!(
            "Caller State: {}, output: {:?}",
            caller.state, caller.output
        );
        println!("Caller History: {:?}", caller_history.events);
        println!(
            "Target State: {}, output: {:?}",
            target.state, target.output
        );
        println!("Target History: {:?}", target_history.events);
        panic!("workflows should complete within timeout, showing that caller didn't wait 1 hour");
    }

    let completed = completed.unwrap();

    assert_eq!(completed.0.state, "COMPLETED");
    assert_eq!(
        completed.0.output.unwrap()["status"].as_str(),
        Some("signaled")
    );
    assert_eq!(completed.1.state, "COMPLETED");

    worker.shutdown();
    let _ = handle.await;
}

// ── Issue #678: inline-resolved mixed timer + external op wakes immediately ──
//
// A `select!{ timer(3600s), signal_external_workflow(target) }` where the
// target EXISTS on the same shard resolves `ExternalSignalDelivered` INLINE in
// the same decision cycle. The caller must wake immediately on that terminal
// rather than parking its timer until fires_at (up to 1 hour). This differs
// from `test_mixed_timer_suspension_signal_wakes_timer` (the passing control),
// which starts the caller FIRST so the external resolves via the NotFound ->
// outbox path. Here the target is started FIRST and confirmed RUNNING so the
// caller's op resolves inline. Without the fix the caller stays RUNNING for
// ~1h and this 30s bound trips -> RED.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_mixed_timer_suspension_signal_resolves_inline_wakes_immediately() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Both on the same shard (0) so the external signal resolves inline.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info("mixed_suspension_workflow", mixed_suspension_workflow),
            wf_info("target_workflow", target_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-mixed-inline-signal".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Start the TARGET FIRST and confirm it is RUNNING (parked on
    // receive_signal), so the caller's signal_external_workflow resolves inline.
    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            target_exec_id,
            "target_workflow",
            "target-inline-signal-1",
            serde_json::json!({}),
        ),
    )
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if target.state == "RUNNING" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("target should be RUNNING (parked on signal) before caller starts");

    // Now start the CALLER pointed at the already-running target.
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            caller_exec_id,
            "mixed_suspension_workflow",
            "caller-inline-signal-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
    )
    .await
    .unwrap();

    // Tight 30s bound: proves the caller woke immediately on the inline
    // ExternalSignalDelivered instead of waiting out the 3600s timer.
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            if caller.state == "COMPLETED" {
                break caller;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    if completed.is_err() {
        let caller = load_execution_from_url(&database_url, caller_exec_id).await;
        let target = load_execution_from_url(&database_url, target_exec_id).await;
        let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
            .await
            .unwrap();
        let target_history = autumn_harvest::store::load_history(&mut conn, target_exec_id)
            .await
            .unwrap();
        println!("TIMEOUT DIAGNOSTICS (inline signal):");
        println!(
            "Caller State: {}, output: {:?}",
            caller.state, caller.output
        );
        println!("Caller History: {:?}", caller_history.events);
        println!(
            "Target State: {}, output: {:?}",
            target.state, target.output
        );
        println!("Target History: {:?}", target_history.events);
        panic!(
            "caller should wake immediately on the inline ExternalSignalDelivered, \
             not park the timer for 1 hour"
        );
    }

    let caller = completed.unwrap();
    assert_eq!(caller.state, "COMPLETED");
    assert_eq!(
        caller.output.unwrap()["status"].as_str(),
        Some("signaled"),
        "caller must resolve on the signal branch, not the timer branch"
    );

    worker.shutdown();
    let _ = handle.await;
}

// Same latency bug for the external-CANCEL primitive (issue #492). A
// `select!{ timer(3600s), request_cancel_external_workflow(target) }` where the
// target EXISTS on the same shard resolves `ExternalCancelDelivered` INLINE.
// The caller must wake immediately; without the fix it parks the timer for ~1h
// and this 30s bound trips -> RED.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_mixed_timer_suspension_cancel_resolves_inline_wakes_immediately() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Both on the same shard (0) so the external cancel resolves inline.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info(
                "mixed_suspension_cancel_workflow",
                mixed_suspension_cancel_workflow,
            ),
            wf_info("cancel_target_workflow", cancel_target_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-mixed-inline-cancel".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Start the TARGET FIRST and confirm it is RUNNING (parked on a signal that
    // never arrives), so the caller's cancel resolves inline against a live run.
    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            target_exec_id,
            "cancel_target_workflow",
            "target-inline-cancel-1",
            serde_json::json!({}),
        ),
    )
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if target.state == "RUNNING" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("target should be RUNNING before caller starts");

    // Now start the CALLER pointed at the already-running target.
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            caller_exec_id,
            "mixed_suspension_cancel_workflow",
            "caller-inline-cancel-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
    )
    .await
    .unwrap();

    // Tight 30s bound: proves the caller woke immediately on the inline
    // ExternalCancelDelivered instead of waiting out the 3600s timer.
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            if caller.state == "COMPLETED" {
                break caller;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    if completed.is_err() {
        let caller = load_execution_from_url(&database_url, caller_exec_id).await;
        let target = load_execution_from_url(&database_url, target_exec_id).await;
        let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
            .await
            .unwrap();
        let target_history = autumn_harvest::store::load_history(&mut conn, target_exec_id)
            .await
            .unwrap();
        println!("TIMEOUT DIAGNOSTICS (inline cancel):");
        println!(
            "Caller State: {}, output: {:?}",
            caller.state, caller.output
        );
        println!("Caller History: {:?}", caller_history.events);
        println!(
            "Target State: {}, output: {:?}",
            target.state, target.output
        );
        println!("Target History: {:?}", target_history.events);
        panic!(
            "caller should wake immediately on the inline ExternalCancelDelivered, \
             not park the timer for 1 hour"
        );
    }

    let caller = completed.unwrap();
    assert_eq!(caller.state, "COMPLETED");
    assert_eq!(
        caller.output.unwrap()["status"].as_str(),
        Some("cancel_resolved"),
        "caller must resolve on the cancel branch, not the timer branch"
    );

    worker.shutdown();
    let _ = handle.await;
}
