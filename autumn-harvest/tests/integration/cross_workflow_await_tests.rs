#![cfg(feature = "db")]
//! Integration tests for `ctx.await_external_workflow` (issue #757).
//!
//! Mirrors `cross_workflow_cancel_tests.rs` but for the await primitive. Uses
//! the env-aware DB helper so it runs against a local `HARVEST_TEST_DATABASE_URL`
//! or a fresh testcontainer. Each test truncates the harvest tables up front so
//! a shared local database stays isolated across the serialized suite.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{IntoWorkflowErrorString as _, WorkflowFailure};
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

async fn setup_test_database_url() -> (String, Option<ContainerAsync<Postgres>>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
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

/// Truncate the harvest tables so a shared local database is isolated across the
/// serialized suite. A no-op-friendly best-effort clean-up.
async fn truncate_all(pool: &DbPool) {
    let mut conn = pool.get().await.unwrap();
    let _ = diesel::sql_query(
        "TRUNCATE harvest_events, harvest_task_queue, harvest_workflow_executions, \
         harvest_signals, harvest_timers, harvest_dead_letters, harvest_workers RESTART IDENTITY CASCADE",
    )
    .execute(&mut conn)
    .await;
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

// ── Workflow handlers ─────────────────────────────────────────────────────

/// Awaits a target and surfaces the outcome (Ok output OR the typed Err cause)
/// into its own output so a test can assert on it.
fn awaiter_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().ok_or("missing target")?;
        let target =
            ExecutionId::from_uuid(uuid::Uuid::parse_str(target_str).map_err(|e| e.to_string())?);
        match ctx.await_external_workflow_value(target).await {
            Ok(output) => Ok(serde_json::json!({ "status": "ok", "output": output })),
            Err(e) => Ok(serde_json::json!({
                "status": "err",
                "error_type": e.workflow_error_type(),
                "reason": e.to_string(),
            })),
        }
    })
}

/// A target that completes immediately with a known output.
fn instant_complete_target<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!({ "value": 99 })) })
}

/// A target that fails immediately with a typed `PaymentDeclined` cause.
fn failing_target<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        Err(WorkflowFailure::new("PaymentDeclined", "card declined")
            .with_details(serde_json::json!({ "code": 402 }))
            .non_retryable()
            .into_workflow_error_payload())
    })
}

/// A target that parks on a signal that never arrives.
fn long_running_target<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _: serde_json::Value = ctx.receive_signal("go").await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "value": 7 }))
    })
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "cross_workflow_await_tests",
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

fn make_worker(workflows: Vec<WorkflowInfo>, worker_id: &str, grace: Option<Duration>) -> Worker {
    let mut cfg = WorkerConfig::default();
    if let Some(g) = grace {
        cfg = cfg.with_unknown_target_grace_window(g);
    }
    let built = HarvestBuilder::new()
        .workflows(workflows)
        .worker(cfg)
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    Worker::new(runtime_config, Arc::new(registry)).expect("worker should build")
}

async fn wait_awaiter_completed(database_url: &str, awaiter: ExecutionId) -> WorkflowExecution {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let a = load_execution_from_url(database_url, awaiter).await;
            if a.state == "COMPLETED" || a.state == "FAILED" {
                break a;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("awaiter should reach terminal state within timeout")
}

// ── (a) Same-shard target already COMPLETED → Ok(output) inline ────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_await_completed_target_ok() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    truncate_all(&pool).await;

    let target = ExecutionId::new_for_shard(ShardId::new(0));
    let awaiter = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = Arc::new(make_worker(
        vec![
            wf_info("awaiter_workflow", awaiter_workflow),
            wf_info("instant_complete_target", instant_complete_target),
        ],
        "worker-await-completed",
        None,
    ));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    // Start + let the target COMPLETE first.
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target,
            "instant_complete_target",
            "await-target-a",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            awaiter,
            "awaiter_workflow",
            "await-caller-a",
            serde_json::json!({ "target": target.to_string() }),
        ),
        None,
    )
    .await
    .unwrap();

    let a = wait_awaiter_completed(&database_url, awaiter).await;
    assert_eq!(a.state, "COMPLETED");
    let out = a.output.expect("awaiter has output");
    assert_eq!(out["status"], "ok");
    assert_eq!(out["output"], serde_json::json!({ "value": 99 }));

    // The resolved output is frozen into the awaiter's own history.
    let hist = autumn_harvest::store::load_history(&mut conn, awaiter)
        .await
        .unwrap();
    assert!(
        hist.events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ExternalAwaitResolved { .. })),
        "expected ExternalAwaitResolved in awaiter history"
    );

    worker.shutdown();
    let _ = handle.await;
}

// ── (b) Same-shard target already FAILED → typed Err ───────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_await_failed_target_surfaces_typed_error() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    truncate_all(&pool).await;

    let target = ExecutionId::new_for_shard(ShardId::new(0));
    let awaiter = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = Arc::new(make_worker(
        vec![
            wf_info("awaiter_workflow", awaiter_workflow),
            wf_info("failing_target", failing_target),
        ],
        "worker-await-failed",
        None,
    ));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target,
            "failing_target",
            "await-target-b",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    // Wait for the target to reach FAILED.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let t = load_execution_from_url(&database_url, target).await;
            if t.state == "FAILED" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("target should FAIL");

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            awaiter,
            "awaiter_workflow",
            "await-caller-b",
            serde_json::json!({ "target": target.to_string() }),
        ),
        None,
    )
    .await
    .unwrap();

    let a = wait_awaiter_completed(&database_url, awaiter).await;
    assert_eq!(a.state, "COMPLETED");
    let out = a.output.expect("awaiter has output");
    assert_eq!(out["status"], "err");
    assert_eq!(
        out["error_type"], "PaymentDeclined",
        "the target's typed error_type must surface to the awaiter, got {out}"
    );

    worker.shutdown();
    let _ = handle.await;
}

// ── (c) Target RUNNING at call time, then completes → outbox resolves ──────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_await_running_target_resolves_via_outbox() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    truncate_all(&pool).await;

    let target = ExecutionId::new_for_shard(ShardId::new(0));
    let awaiter = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = Arc::new(make_worker(
        vec![
            wf_info("awaiter_workflow", awaiter_workflow),
            wf_info("long_running_target", long_running_target),
        ],
        "worker-await-running",
        None,
    ));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    // Target parks on a signal (RUNNING).
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target,
            "long_running_target",
            "await-target-c",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Awaiter starts while the target is still RUNNING → parks; the outbox
    // resolves it once the target completes.
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            awaiter,
            "awaiter_workflow",
            "await-caller-c",
            serde_json::json!({ "target": target.to_string() }),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The awaiter must still be RUNNING (parked) before the target completes.
    let mid = load_execution_from_url(&database_url, awaiter).await;
    assert_eq!(
        mid.state, "RUNNING",
        "awaiter must park while the target is still running, got {}",
        mid.state
    );

    // Deliver the signal so the target completes.
    autumn_harvest::signal::send_signal(&mut conn, target, "go", serde_json::json!({}))
        .await
        .unwrap();

    // The outbox resolves the awaiter within ~one poll interval.
    let a = wait_awaiter_completed(&database_url, awaiter).await;
    assert_eq!(a.state, "COMPLETED");
    let out = a.output.expect("awaiter output");
    assert_eq!(out["status"], "ok");
    assert_eq!(out["output"], serde_json::json!({ "value": 7 }));

    worker.shutdown();
    let _ = handle.await;
}

// ── (d) Cross-shard target via outbox (logical sharding on one physical DB) ─

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_await_cross_shard_via_outbox() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    truncate_all(&pool).await;

    // Both shards point to the same physical DB (logical sharding mock), mirroring
    // the cross-shard cancel test.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool.clone());
    pools.insert(ShardId::new(1), pool.clone());
    let _sharded = autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0));

    let awaiter = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));

    let worker = Arc::new(make_worker(
        vec![
            wf_info("awaiter_workflow", awaiter_workflow),
            wf_info("instant_complete_target", instant_complete_target),
        ],
        "worker-await-cross-shard",
        None,
    ));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    // Target (shard 1) completes.
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target,
            "instant_complete_target",
            "await-target-d",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Awaiter (shard 0) awaits a shard-1 target: cross-shard-encoded → the inline
    // path leaves the bare Requested and the outbox resolves it.
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            awaiter,
            "awaiter_workflow",
            "await-caller-d",
            serde_json::json!({ "target": target.to_string() }),
        ),
        None,
    )
    .await
    .unwrap();

    let a = wait_awaiter_completed(&database_url, awaiter).await;
    assert_eq!(a.state, "COMPLETED");
    let out = a.output.expect("awaiter output");
    assert_eq!(out["status"], "ok");
    assert_eq!(out["output"], serde_json::json!({ "value": 99 }));

    worker.shutdown();
    let _ = handle.await;
}

// ── (e) Self-await → immediate Err, no history ─────────────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_self_await_immediate_error_no_park() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    truncate_all(&pool).await;

    // The awaiter awaits ITS OWN execution id → immediate self_await error.
    let awaiter = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = Arc::new(make_worker(
        vec![wf_info("awaiter_workflow", awaiter_workflow)],
        "worker-await-self",
        None,
    ));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            awaiter,
            "awaiter_workflow",
            "await-self",
            serde_json::json!({ "target": awaiter.to_string() }),
        ),
        None,
    )
    .await
    .unwrap();

    let a = wait_awaiter_completed(&database_url, awaiter).await;
    assert_eq!(a.state, "COMPLETED");
    let out = a.output.expect("awaiter output");
    assert_eq!(out["status"], "err");
    assert!(
        out["reason"].as_str().unwrap().contains("self_await"),
        "self-await must surface reason_code=self_await, got {out}"
    );
    // Self-await records NO ExternalAwait* events.
    let hist = autumn_harvest::store::load_history(&mut conn, awaiter)
        .await
        .unwrap();
    assert!(
        !hist.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. }
        )),
        "self-await must record no ExternalAwait events"
    );

    worker.shutdown();
    let _ = handle.await;
}

// ── (f) Unknown target + grace window → target_unknown Err ─────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_await_unknown_target_grace_window() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    truncate_all(&pool).await;

    let awaiter = ExecutionId::new_for_shard(ShardId::new(0));
    // A target that is never started.
    let ghost = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = Arc::new(make_worker(
        vec![wf_info("awaiter_workflow", awaiter_workflow)],
        "worker-await-unknown",
        Some(Duration::from_secs(1)),
    ));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            awaiter,
            "awaiter_workflow",
            "await-unknown",
            serde_json::json!({ "target": ghost.to_string() }),
        ),
        None,
    )
    .await
    .unwrap();

    // After the 1s grace window the outbox resolves target_unknown.
    let a = wait_awaiter_completed(&database_url, awaiter).await;
    assert_eq!(a.state, "COMPLETED");
    let out = a.output.expect("awaiter output");
    assert_eq!(out["status"], "err");
    assert!(
        out["reason"].as_str().unwrap().contains("target_unknown"),
        "unknown target after grace must resolve as target_unknown, got {out}"
    );
    let hist = autumn_harvest::store::load_history(&mut conn, awaiter)
        .await
        .unwrap();
    assert!(
        hist.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ExternalAwaitFailed { reason_code, .. } if reason_code == "target_unknown"
        )),
        "expected ExternalAwaitFailed{{target_unknown}} in history"
    );

    worker.shutdown();
    let _ = handle.await;
}

// ── (g) Observe-only: awaiting does not affect the target's lifecycle ──────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_await_is_observe_only_target_unchanged() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    truncate_all(&pool).await;

    let target = ExecutionId::new_for_shard(ShardId::new(0));
    let awaiter = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = Arc::new(make_worker(
        vec![
            wf_info("awaiter_workflow", awaiter_workflow),
            wf_info("instant_complete_target", instant_complete_target),
        ],
        "worker-await-observe",
        None,
    ));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target,
            "instant_complete_target",
            "await-target-g",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Snapshot the target's state + event count BEFORE the await.
    let before = load_execution_from_url(&database_url, target).await;
    let before_hist = autumn_harvest::store::load_history(&mut conn, target)
        .await
        .unwrap();
    let before_count = before_hist.events.len();

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            awaiter,
            "awaiter_workflow",
            "await-caller-g",
            serde_json::json!({ "target": target.to_string() }),
        ),
        None,
    )
    .await
    .unwrap();
    wait_awaiter_completed(&database_url, awaiter).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The target's state and history are UNCHANGED — no linkage, no cancellation,
    // no cascade (observe-only).
    let after = load_execution_from_url(&database_url, target).await;
    let after_hist = autumn_harvest::store::load_history(&mut conn, target)
        .await
        .unwrap();
    assert_eq!(before.state, after.state, "target state must be unchanged");
    assert_eq!(
        before_count,
        after_hist.events.len(),
        "target history must be unchanged by being awaited (observe-only)"
    );
    // No ExternalCancel/Await events were written to the TARGET's history.
    assert!(
        !after_hist.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalCancelRequested { .. }
        )),
        "the target's history must carry no external-op events"
    );

    worker.shutdown();
    let _ = handle.await;
}
