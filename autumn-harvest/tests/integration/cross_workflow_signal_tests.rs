#![cfg(feature = "db")]

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, HarvestBuilder, StartWorkflowParams, WorkerConfig, WorkflowContext,
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
        // The per-test `harvest678_<uuid>` database is intentionally NOT dropped:
        // this local-dev-only path is env-gated, and CI leaves the env var unset
        // so it uses the testcontainers path (which self-cleans on container drop).
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

// Issue #678 correctness guard: resolve an external op INLINE (cycle 1, its
// terminal appended this cycle), THEN park a PURE `ctx.timer()` in a later cycle.
// The earlier, fully-observed external op must NOT false-wake the pure timer —
// `resolved_external_ids` is scoped to the ids resolved on the *current* cycle,
// which for the timer-park cycle is empty.
fn external_then_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );

        // Cycle 1: external signal resolves inline against the same-shard target.
        ctx.signal_external_workflow(target, "my_signal", serde_json::json!({"data": "hi"}))
            .await
            .map_err(|e| e.to_string())?;

        // Later cycle: a PURE timer park with no external op in the batch.
        ctx.timer("sleep", 3).await.map_err(|e| e.to_string())?;

        Ok(serde_json::json!({"status": "done"}))
    })
}

// ── Issue #1034: mixed NON-timer suspension + inline external op ────────────
//
// The #678 fix wired the inline-external self-wake only into the TIMER
// remaining-command shape (`persist_started_timer`). These three workflow
// bodies mirror the #678 `mixed_suspension_workflow` construction EXACTLY, but
// replace the timer branch with the two OTHER remaining-command shapes:
// a `receive_signal` wait (→ persist_signal_wait_park) and an `execute_activity`
// (→ persist_scheduled_activities). Neither receives `resolved_inline_external`,
// so the parent never self-wakes on the inline external terminal (issue #1034):
// the signal-wait shape HANGS; the activity shape wakes only when the (slow)
// activity finishes.

// select! { receive_signal (never sent) , signal_external_workflow(target) }.
// Batch = [WaitForSignal, SignalExternalWorkflow]. After inline resolution +
// split, remaining = [WaitForSignal] → persist_signal_wait_park (no
// resolved_inline_external) → the caller's own signal never arrives and the
// stripped external id is not in `commands`, so NO self-wake → HANGS.
fn mixed_signal_wait_external_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );

        let wait_fut = ctx.receive_signal::<serde_json::Value>("parent_never_arrives");
        let signal_fut =
            ctx.signal_external_workflow(target, "my_signal", serde_json::json!({"data": "hello"}));

        tokio::select! {
            res = wait_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "signal_received"}))
            }
            res = signal_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "external_signal_sent"}))
            }
        }
    })
}

// select! { receive_signal (never sent) , request_cancel_external_workflow(target) }.
// Same signal-wait remaining shape as above, on the external-CANCEL primitive.
fn mixed_signal_wait_external_cancel_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );

        let wait_fut = ctx.receive_signal::<serde_json::Value>("parent_never_arrives");
        let cancel_fut = ctx.request_cancel_external_workflow(target);

        tokio::select! {
            res = wait_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "signal_received"}))
            }
            res = cancel_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "external_cancel_sent"}))
            }
        }
    })
}

// A deliberately slow activity (sleeps 60s) with NO start_to_close, so the
// timeout scanner never fails it early. On the buggy code the parent's activity
// park (persist_scheduled_activities) is only woken when this activity
// completes, so the parent cannot finish before ~60s. The long sleep is orphaned
// work on the green path (the parent completes on the external branch in ~seconds
// and never awaits it; the worker's 1s shutdown drain abandons it), so it does
// NOT slow the passing test — it only widens the margin for slow CI.
fn slow_activity<'a>(
    _ctx: &'a ActivityContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(serde_json::json!({"status": "slow_done"}))
    })
}

// select! { execute_activity("slow_noop") , signal_external_workflow(target) }.
// Batch = [ScheduleActivity, SignalExternalWorkflow]. After inline resolution +
// split, remaining = [ScheduleActivity] → persist_scheduled_activities (no
// resolved_inline_external) → the inline external terminal is durable but
// nothing self-wakes → parent only wakes when the 20s activity finishes.
fn mixed_activity_external_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );

        let activity_fut = ctx.execute_activity_raw("slow_noop", serde_json::json!({}), "default");
        let signal_fut =
            ctx.signal_external_workflow(target, "my_signal", serde_json::json!({"data": "hello"}));

        tokio::select! {
            res = activity_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "activity_done"}))
            }
            res = signal_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "external_signal_sent"}))
            }
        }
    })
}

// A deliberately slow CHILD workflow that parks on a 60s durable timer. On the
// buggy code the parent's child-workflow park (persist_all_started_child_workflows)
// is only woken when this child COMPLETES (~60s), mirroring the slow-activity case
// one level up. The child is orphaned when the parent completes on the external
// branch (awaited child, no parent-close cascade; the worker's 1s shutdown drain
// abandons it), so it does NOT slow the passing test.
fn slow_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("child_sleep", 60)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"status": "child_done"}))
    })
}

// select! { spawn_child_workflow_raw("slow_child_workflow", {}) , signal_external_workflow(target) }.
// Batch = [StartChildWorkflow, SignalExternalWorkflow]. After inline resolution +
// split, remaining = [StartChildWorkflow] → persist_all_started_child_workflows
// (a RUNNING park). Without the arm-level wake (issue #1034) the inline external
// terminal is durable but nothing self-wakes → the parent only wakes when the 60s
// child completes.
fn mixed_child_workflow_external_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_uuid_str = input["target"].as_str().ok_or("missing target")?;
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str(target_uuid_str).map_err(|e| e.to_string())?,
        );

        let child_fut = ctx.spawn_child_workflow_raw("slow_child_workflow", serde_json::json!({}));
        let signal_fut =
            ctx.signal_external_workflow(target, "my_signal", serde_json::json!({"data": "hello"}));

        tokio::select! {
            res = child_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "child_done"}))
            }
            res = signal_fut => {
                res.map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"status": "external_signal_sent"}))
            }
        }
    })
}

// Compact ActivityInfo builder (all timeouts None so the slow activity is never
// timed out by the enforcement scanner spawned in Worker::run).
fn slow_act_info() -> ActivityInfo {
    ActivityInfo {
        name: "slow_noop",
        module: "cross_workflow_signal_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: slow_activity,
    }
}

// Compact WorkflowInfo builder mirroring the exact field set used by the inline
// struct literals elsewhere in this file (kept in one place so field drift
// breaks every test together rather than silently diverging).
fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "cross_workflow_signal_tests",
        handler,
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
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
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "caller_workflow",
                module: "cross_workflow_signal_tests",
                handler: caller_workflow,
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
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "target_workflow",
                module: "cross_workflow_signal_tests",
                handler: target_workflow,
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    start_or_load_workflow_execution(&mut conn, start_params, None)
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    start_or_load_workflow_execution(&mut conn, start_target_params, None)
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
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "caller_workflow",
                module: "cross_workflow_signal_tests",
                handler: caller_workflow,
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
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "target_workflow",
                module: "cross_workflow_signal_tests",
                handler: target_workflow,
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    start_or_load_workflow_execution(&mut conn, start_target_params, None)
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    start_or_load_workflow_execution(&mut conn, start_params, None)
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
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "caller_workflow",
            module: "cross_workflow_signal_tests",
            handler: caller_workflow,
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    start_or_load_workflow_execution(&mut conn, start_params, None)
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
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "mixed_suspension_workflow",
                module: "cross_workflow_signal_tests",
                handler: mixed_suspension_workflow,
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
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "target_workflow",
                module: "cross_workflow_signal_tests",
                handler: target_workflow,
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    start_or_load_workflow_execution(&mut conn, start_params, None)
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
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    };
    start_or_load_workflow_execution(&mut conn, start_target_params, None)
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
        None,
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
        None,
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
        None,
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
        None,
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

    // The inline cancel must actually have cancelled the target. It may settle
    // just after the caller wakes, so poll briefly for the terminal transition.
    let target = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if target.state == "CANCELLED" {
                break target;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cancel target should reach CANCELLED after the inline cancel");
    assert_eq!(target.state, "CANCELLED");

    worker.shutdown();
    let _ = handle.await;
}

// ── Issue #678 correctness guard: pure timer after inline external ──────────
//
// The inline self-wake must be scoped to the ids resolved THIS cycle. A
// workflow that resolves an external op inline (cycle 1) and THEN parks a PURE
// `ctx.timer()` (later cycle) must not be false-woken by the earlier op: the
// timer's park cycle appends no external terminal, so `resolved_external_ids`
// is empty and the row parks normally. This proves the fix does not degrade
// the pure-timer contract (the timer must actually sleep out its duration).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_pure_timer_after_inline_external_does_not_false_wake() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Both on the same shard (0) so the external signal resolves inline.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info("external_then_timer_workflow", external_then_timer_workflow),
            wf_info("target_workflow", target_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-pure-timer-after-external".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Start the TARGET FIRST and confirm it is RUNNING (parked on receive_signal),
    // so the caller's signal_external_workflow resolves inline in cycle 1.
    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            target_exec_id,
            "target_workflow",
            "target-pure-timer-guard-1",
            serde_json::json!({}),
        ),
        None,
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

    let start = std::time::Instant::now();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            caller_exec_id,
            "external_then_timer_workflow",
            "caller-pure-timer-guard-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    // At ~1.5s the caller must still be RUNNING: the earlier inline external must
    // NOT have false-woken the pure 3s timer park.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mid = load_execution_from_url(&database_url, caller_exec_id).await;
    assert_ne!(
        mid.state,
        "COMPLETED",
        "pure 3s timer must not be false-woken by the earlier inline external op \
         (elapsed {:?})",
        start.elapsed()
    );

    // The timer eventually fires (~3s) and the caller completes.
    let completed = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let caller = load_execution_from_url(&database_url, caller_exec_id).await;
            if caller.state == "COMPLETED" {
                break caller;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("caller should complete after its 3s timer fires");

    assert_eq!(completed.state, "COMPLETED");
    assert_eq!(
        completed.output.unwrap()["status"].as_str(),
        Some("done"),
        "caller must complete via the pure timer branch"
    );

    worker.shutdown();
    let _ = handle.await;
}

// ── Issue #1034: mixed NON-timer suspension + inline external op self-wake ───
//
// The #678 fix wired the inline-external self-wake ONLY into the timer
// remaining-command shape (`persist_started_timer`). The two other shapes —
// a signal-wait park and a scheduled-activities park — never receive
// `resolved_inline_external`, so a caller that races `receive_signal` /
// `execute_activity` against a same-shard `signal_external_workflow` /
// `request_cancel_external_workflow` resolves the external terminal INLINE but
// is never self-woken. These three tests mirror the #678 inline tests exactly
// (target started FIRST → inline resolution; select! in a raw handler) but on
// the non-timer shapes.

// AC1: signal-wait + same-shard external SIGNAL. Batch = [WaitForSignal,
// SignalExternalWorkflow] → remaining [WaitForSignal] → persist_signal_wait_park.
// The caller's own "parent_never_arrives" signal is NEVER sent, so the ONLY
// resolvable branch is the external signal. Without the fix the caller never
// self-wakes on the inline ExternalSignalDelivered → HANGS → the 15s bound
// trips → RED.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_mixed_signal_wait_suspension_external_signal_resolves_inline_wakes_immediately() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Both on the same shard (0) so the external signal resolves inline.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info(
                "mixed_signal_wait_external_signal_workflow",
                mixed_signal_wait_external_signal_workflow,
            ),
            wf_info("target_workflow", target_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-1034-signal-wait-signal".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Start the TARGET FIRST and confirm it is RUNNING (parked on receive_signal),
    // so the caller's signal_external_workflow resolves inline this cycle.
    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            target_exec_id,
            "target_workflow",
            "target-1034-signal-wait-1",
            serde_json::json!({}),
        ),
        None,
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
            "mixed_signal_wait_external_signal_workflow",
            "caller-1034-signal-wait-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    // Tight 15s bound: proves the caller woke on the inline ExternalSignalDelivered
    // rather than hanging forever on its own never-arriving signal.
    let completed = tokio::time::timeout(Duration::from_secs(15), async {
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
        println!("TIMEOUT DIAGNOSTICS (1034 signal-wait + external signal):");
        println!(
            "Caller State: {}, output: {:?}",
            caller.state, caller.output
        );
        println!("Caller History: {:?}", caller_history.events);
        println!(
            "Target State: {}, output: {:?}",
            target.state, target.output
        );
        panic!(
            "caller must wake immediately on the inline ExternalSignalDelivered; \
             a signal-wait remaining shape currently HANGS (issue #1034)"
        );
    }

    let caller = completed.unwrap();
    assert_eq!(caller.state, "COMPLETED");
    assert_eq!(
        caller.output.unwrap()["status"].as_str(),
        Some("external_signal_sent"),
        "caller must resolve on the external-signal branch (its own signal never arrives)"
    );

    // The target actually received the signal (it only COMPLETES if it did).
    let target = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if target.state == "COMPLETED" {
                break target;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("target should COMPLETE after receiving the inline signal");
    assert_eq!(target.state, "COMPLETED");
    assert_eq!(
        target.output.unwrap()["data"].as_str(),
        Some("hello"),
        "target must have received the signal payload"
    );

    worker.shutdown();
    let _ = handle.await;
}

// AC2 / AC4: signal-wait + same-shard external CANCEL. Same remaining shape
// ([WaitForSignal] → persist_signal_wait_park) but on the external-cancel
// primitive. Without the fix the caller HANGS → 15s bound trips → RED.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_mixed_signal_wait_suspension_external_cancel_resolves_inline_wakes_immediately() {
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
                "mixed_signal_wait_external_cancel_workflow",
                mixed_signal_wait_external_cancel_workflow,
            ),
            wf_info("cancel_target_workflow", cancel_target_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-1034-signal-wait-cancel".to_string();
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
            "target-1034-signal-wait-cancel-1",
            serde_json::json!({}),
        ),
        None,
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
            "mixed_signal_wait_external_cancel_workflow",
            "caller-1034-signal-wait-cancel-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    let completed = tokio::time::timeout(Duration::from_secs(15), async {
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
        println!("TIMEOUT DIAGNOSTICS (1034 signal-wait + external cancel):");
        println!(
            "Caller State: {}, output: {:?}",
            caller.state, caller.output
        );
        println!("Caller History: {:?}", caller_history.events);
        println!(
            "Target State: {}, output: {:?}",
            target.state, target.output
        );
        panic!(
            "caller must wake immediately on the inline ExternalCancelDelivered; \
             a signal-wait remaining shape currently HANGS (issue #1034)"
        );
    }

    let caller = completed.unwrap();
    assert_eq!(caller.state, "COMPLETED");
    assert_eq!(
        caller.output.unwrap()["status"].as_str(),
        Some("external_cancel_sent"),
        "caller must resolve on the external-cancel branch (its own signal never arrives)"
    );

    // The inline cancel must actually have cancelled the target.
    let target = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let target = load_execution_from_url(&database_url, target_exec_id).await;
            if target.state == "CANCELLED" {
                break target;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cancel target should reach CANCELLED after the inline cancel");
    assert_eq!(target.state, "CANCELLED");

    worker.shutdown();
    let _ = handle.await;
}

// AC3: activity + same-shard external SIGNAL. Batch = [ScheduleActivity,
// SignalExternalWorkflow] → remaining [ScheduleActivity] →
// persist_scheduled_activities (no resolved_inline_external). The inline
// ExternalSignalDelivered is durable, but nothing self-wakes the parked
// activity task — so on the buggy code the caller cannot complete until the
// deliberately-slow (20s) activity finishes. The 10s bound proves the fix wakes
// the caller on the external branch immediately; without it → RED (~20s > 10s).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_mixed_activity_suspension_external_signal_resolves_inline_wakes_immediately() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Both on the same shard (0) so the external signal resolves inline.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info(
                "mixed_activity_external_signal_workflow",
                mixed_activity_external_signal_workflow,
            ),
            wf_info("target_workflow", target_workflow),
        ])
        .activities(vec![slow_act_info()])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-1034-activity-signal".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Start the TARGET FIRST and confirm it is RUNNING (parked on receive_signal),
    // so the caller's signal_external_workflow resolves inline this cycle.
    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            target_exec_id,
            "target_workflow",
            "target-1034-activity-1",
            serde_json::json!({}),
        ),
        None,
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
    let start = std::time::Instant::now();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            caller_exec_id,
            "mixed_activity_external_signal_workflow",
            "caller-1034-activity-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    // 30s bound with a 60s slow activity: the ONLY way to complete within 30s is
    // by self-waking on the inline external terminal (the fixed path completes in
    // ~seconds, independent of the activity length). Without the fix the caller
    // cannot finish until ~60s → this bound trips → RED. The 30s ceiling stays
    // well below 60s so the buggy path still fails, while giving the fixed path a
    // large cushion on slow CI.
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
        let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
            .await
            .unwrap();
        println!(
            "TIMEOUT DIAGNOSTICS (1034 activity + external signal), elapsed {:?}:",
            start.elapsed()
        );
        println!(
            "Caller State: {}, output: {:?}",
            caller.state, caller.output
        );
        println!("Caller History: {:?}", caller_history.events);
        panic!(
            "caller must wake immediately on the inline ExternalSignalDelivered; \
             a scheduled-activity remaining shape currently waits out the slow \
             activity (~60s) before waking (issue #1034)"
        );
    }

    let caller = completed.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(caller.state, "COMPLETED");
    assert!(
        elapsed < Duration::from_secs(30),
        "caller must complete well before the 60s activity finishes (elapsed {elapsed:?})"
    );
    assert_eq!(
        caller.output.unwrap()["status"].as_str(),
        Some("external_signal_sent"),
        "caller must resolve on the external-signal branch (activity still sleeping)"
    );

    worker.shutdown();
    let _ = handle.await;
}

// Issue #1034: the CHILD-WORKFLOW remaining shape. Batch =
// [StartChildWorkflow, SignalExternalWorkflow]; after inline resolution + split,
// remaining = [StartChildWorkflow] → persist_all_started_child_workflows (a
// RUNNING park). This backs the changelog claim that the arm-level wake covers
// more than the three named shapes: without the arm-level self-wake the parent
// only wakes when the 60s child completes; with it the parent wakes immediately
// on the inline ExternalSignalDelivered.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_mixed_child_workflow_suspension_external_signal_resolves_inline_wakes_immediately() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // Both on the same shard (0) so the external signal resolves inline.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info(
                "mixed_child_workflow_external_signal_workflow",
                mixed_child_workflow_external_signal_workflow,
            ),
            wf_info("slow_child_workflow", slow_child_workflow),
            wf_info("target_workflow", target_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();

    let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-1034-child-signal".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(1);

    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // Start the TARGET FIRST and confirm it is RUNNING (parked on receive_signal),
    // so the caller's signal_external_workflow resolves inline this cycle.
    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            target_exec_id,
            "target_workflow",
            "target-1034-child-1",
            serde_json::json!({}),
        ),
        None,
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
    let start = std::time::Instant::now();
    start_or_load_workflow_execution(
        &mut conn,
        mk_start_params(
            caller_exec_id,
            "mixed_child_workflow_external_signal_workflow",
            "caller-1034-child-1",
            serde_json::json!({"target": target_exec_id.to_string()}),
        ),
        None,
    )
    .await
    .unwrap();

    // 30s bound with a 60s slow child: the ONLY way to complete within 30s is by
    // self-waking on the inline external terminal (the fixed path completes in
    // ~seconds, independent of the child length). Without the fix the caller
    // cannot finish until the 60s child completes → this bound trips → RED.
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
        let caller_history = autumn_harvest::store::load_history(&mut conn, caller_exec_id)
            .await
            .unwrap();
        println!(
            "TIMEOUT DIAGNOSTICS (1034 child workflow + external signal), elapsed {:?}:",
            start.elapsed()
        );
        println!(
            "Caller State: {}, output: {:?}",
            caller.state, caller.output
        );
        println!("Caller History: {:?}", caller_history.events);
        panic!(
            "caller must wake immediately on the inline ExternalSignalDelivered; \
             a child-workflow remaining shape currently waits out the slow child \
             (~60s) before waking (issue #1034)"
        );
    }

    let caller = completed.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(caller.state, "COMPLETED");
    assert!(
        elapsed < Duration::from_secs(30),
        "caller must complete well before the 60s child finishes (elapsed {elapsed:?})"
    );
    assert_eq!(
        caller.output.unwrap()["status"].as_str(),
        Some("external_signal_sent"),
        "caller must resolve on the external-signal branch (child still parked)"
    );

    worker.shutdown();
    let _ = handle.await;
}
