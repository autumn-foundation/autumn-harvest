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

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

/// Rewrite the database segment of a `postgres://…/db?query` URL, preserving the
/// authority and any query string (mirrors the signal suite's helper).
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
    // migration bundle, and hand back its URL — fully isolated regardless of
    // suite concurrency (matching the signal suite). CI leaves the env var unset
    // and uses the testcontainers path below (the authoritative path).
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        use diesel_async::SimpleAsyncConnection;
        // The per-test `harvest757c_<uuid>` database is intentionally NOT dropped:
        // this local-dev-only path is env-gated, and CI leaves the env var unset.
        let db_name = format!("harvest757c_{}", uuid::Uuid::new_v4().simple());
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
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("failed to apply migrations to per-test database");
        return (new_url, None);
    }

    let container = Postgres::default()
        .with_init_sql(init_sql())
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

fn canceller_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );
        ctx.request_cancel_external_workflow(target)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"status": "cancelled"}))
    })
}

fn long_running_target_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Wait for a signal that will never arrive — the workflow must be cancelled externally.
        let _: serde_json::Value = ctx
            .receive_signal("never_arrives")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"status": "signalled"}))
    })
}

// Target workflow that completes immediately.
fn instant_complete_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!({"status": "done"})) })
}

// Canceller that surfaces the cancel outcome (delivered/failed) in its output.
fn canceller_expecting_failure<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );
        match ctx.request_cancel_external_workflow(target).await {
            Ok(()) => Ok(serde_json::json!({"result": "delivered"})),
            Err(e) => Ok(serde_json::json!({"result": "failed", "reason": e.to_string()})),
        }
    })
}

fn default_start_params(
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
        sla: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
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
    }
}

fn assert_has_event(events: &[WorkflowEvent], name: &str) {
    assert!(
        events.iter().any(|e| e.type_name() == name),
        "expected event {name} in history; got: {:?}",
        events
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>()
    );
}

// Same-shard live cancel: caller cancels a running target that is waiting for a signal.
// Expected: caller reaches COMPLETED with ExternalCancelDelivered; target reaches CANCELLED.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_same_shard_live_cancel() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let canceller_info = WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "canceller_workflow",
        module: "cross_workflow_cancel_tests",
        handler: canceller_workflow,
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
    };
    let target_info = WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "long_running_target_workflow",
        module: "cross_workflow_cancel_tests",
        handler: long_running_target_workflow,
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
    };

    let built = HarvestBuilder::new()
        .workflows(vec![canceller_info, target_info])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-cancel-same-shard".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();

    // Start target first — it will park waiting for "never_arrives" signal.
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "long_running_target_workflow",
            "cancel-target-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();

    // Give target time to park.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Start canceller, passing the target's execution ID.
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "canceller_workflow",
            "cancel-caller-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    // Wait for the caller to complete and the target to reach CANCELLED.
    let (caller_final, target_final) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED"
                && (target.state == "CANCELLED" || target.state == "FAILED")
            {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("workflows should reach terminal state within timeout");

    assert_eq!(caller_final.state, "COMPLETED");
    // Target must have been cancelled (or failed due to cancellation propagation).
    assert!(
        target_final.state == "CANCELLED" || target_final.state == "FAILED",
        "target should be CANCELLED or FAILED, got {}",
        target_final.state
    );

    // Verify ExternalCancelDelivered is in caller's history.
    let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
        .await
        .unwrap();
    assert_has_event(&caller_history.events, "ExternalCancelDelivered");

    worker.shutdown();
    let _ = handle.await;
}

// Cancel of an already-terminal target is a no-op success (ExternalCancelDelivered).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_already_terminal_target_is_no_op_success() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            WorkflowInfo {
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "canceller_workflow",
                module: "cross_workflow_cancel_tests",
                handler: canceller_workflow,
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
            },
            WorkflowInfo {
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "instant_complete_workflow",
                module: "cross_workflow_cancel_tests",
                handler: instant_complete_workflow,
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
            },
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-cancel-terminal".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();

    // Start target first and let it complete.
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "instant_complete_workflow",
            "cancel-terminal-target-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();

    // Wait for target to complete.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if target.state == "COMPLETED" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("target should complete quickly");

    // Now start the canceller — target is already COMPLETED.
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "canceller_workflow",
            "cancel-terminal-caller-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    // Canceller should complete successfully (no-op cancel of terminal target).
    let caller_final = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            if caller.state == "COMPLETED" || caller.state == "FAILED" {
                break caller;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("caller should reach terminal state within timeout");

    // Key AC: cancelling an already-terminal target is COMPLETED (no-op success), NOT failed.
    assert_eq!(
        caller_final.state, "COMPLETED",
        "cancel of already-terminal target should be COMPLETED (no-op success)"
    );

    // Verify ExternalCancelDelivered (not ExternalCancelFailed) is in caller's history.
    let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
        .await
        .unwrap();
    assert_has_event(&caller_history.events, "ExternalCancelDelivered");
    let has_failed = caller_history
        .events
        .iter()
        .any(|e| e.type_name() == "ExternalCancelFailed");
    assert!(
        !has_failed,
        "should not have ExternalCancelFailed for a terminal target"
    );

    worker.shutdown();
    let _ = handle.await;
}

// Grace-window expiry for an unknown target resolves as ExternalCancelFailed{target_unknown}.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_grace_window_expiry_unknown_target() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Target does not exist at all.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![WorkflowInfo {
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "canceller_expecting_failure",
            module: "cross_workflow_cancel_tests",
            handler: canceller_expecting_failure,
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
        }])
        .worker(WorkerConfig::default().with_unknown_target_grace_window(Duration::from_secs(1)))
        .build();

    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-cancel-grace".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "canceller_expecting_failure",
            "cancel-grace-caller-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    // After the grace window (1 s), the outbox scanner should resolve with target_unknown.
    let caller_final = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            if caller.state == "COMPLETED" || caller.state == "FAILED" {
                break caller;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("caller should reach terminal state after grace window expires");

    assert_eq!(
        caller_final.state, "COMPLETED",
        "caller handles ExternalCancelFailed gracefully and should complete"
    );

    // Verify ExternalCancelFailed{target_unknown} is in caller's history.
    let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
        .await
        .unwrap();
    assert_has_event(&caller_history.events, "ExternalCancelFailed");

    let has_target_unknown = caller_history.events.iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::ExternalCancelFailed { reason_code, .. }
                if reason_code == "target_unknown"
        )
    });
    assert!(
        has_target_unknown,
        "ExternalCancelFailed should have reason_code=target_unknown"
    );

    worker.shutdown();
    let _ = handle.await;
}

// Cross-shard cancel via outbox: caller on Shard 0, target on Shard 1.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_cross_shard_cancel_via_outbox() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // Both shards point to the same physical DB (logical sharding mock).
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool.clone());
    pools.insert(ShardId::new(1), pool.clone());
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0));

    // Caller is Shard 0, target is Shard 1.
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(1));

    let built = HarvestBuilder::new()
        .workflows(vec![
            WorkflowInfo {
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "canceller_workflow",
                module: "cross_workflow_cancel_tests",
                handler: canceller_workflow,
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
            },
            WorkflowInfo {
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "long_running_target_workflow",
                module: "cross_workflow_cancel_tests",
                handler: long_running_target_workflow,
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
            },
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-cancel-cross-shard".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();

    // Start target first (Shard 1 encoding, but same physical DB).
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "long_running_target_workflow",
            "cross-cancel-target-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Start canceller (Shard 0).
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "canceller_workflow",
            "cross-cancel-caller-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    // Wait for caller to complete and target to be cancelled.
    let (caller_final, target_final) = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED"
                && (target.state == "CANCELLED" || target.state == "FAILED")
            {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .expect("cross-shard cancel should complete within timeout");

    assert_eq!(caller_final.state, "COMPLETED");
    assert!(
        target_final.state == "CANCELLED" || target_final.state == "FAILED",
        "target should be CANCELLED or FAILED after cross-shard cancel, got {}",
        target_final.state
    );

    let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
        .await
        .unwrap();
    assert_has_event(&caller_history.events, "ExternalCancelDelivered");

    worker.shutdown();
    let _ = handle.await;
}
