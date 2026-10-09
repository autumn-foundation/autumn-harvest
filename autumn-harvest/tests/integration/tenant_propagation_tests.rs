#![cfg(feature = "db")]
//! Tenant propagation tests (issue #1977).
//!
//! A run stamped with a tenant passes the tenant to every run derived from it.
//! That is awaited and detached children, continue-as-new successors,
//! retries, reset forks and re-runs. A run with no tenant passes none.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly. Otherwise a fresh testcontainers Postgres starts with
//! the full migration bundle.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use autumn_harvest::context::ActivityContext;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::prelude::activity;
use autumn_harvest::types::ParentClosePolicy;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, ShardId, StartWorkflowParams, WorkflowContext, WorkflowResetRequest,
    reset_workflow_execution, start_or_load_workflow_execution,
};
use diesel::sql_types::{Jsonb, Nullable, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

type HandlerFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// Spawns an awaited and a detached child, then continues as new once.
fn tp_parent(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("tp_child", json!({}))
            .await
            .map_err(|e| e.to_string())?;
        ctx.spawn_child_workflow_detached_raw("tp_child", json!({}), ParentClosePolicy::Abandon)
            .map_err(|e| e.to_string())?;
        if input["hop"] == 0 {
            ctx.continue_as_new(json!({ "hop": 1 }))
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(json!("parent done"))
    })
}

fn tp_child(_ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move { Ok(json!("child done")) })
}

/// Returns the verified tenant that the activity reads (issue #1998).
#[activity(start_to_close = "30s")]
async fn tp_read_tenant(ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    let _ = input;
    let tenant = ctx.run_tenant().await.map_err(|e| e.to_string())?;
    Ok(json!(tenant))
}

/// Runs `tp_read_tenant` and returns its result.
fn tp_tenant_reader(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ctx.execute_activity(&tp_read_tenant_info(), json!({}))
            .await
            .map_err(|e| e.to_string())
    })
}

static RETRY_FAILED_ONCE: AtomicBool = AtomicBool::new(false);

/// Fails on the first attempt, then succeeds.
fn tp_retry(_ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        if RETRY_FAILED_ONCE.swap(true, Ordering::SeqCst) {
            Ok(json!("retry done"))
        } else {
            Err("first attempt fails".to_string())
        }
    })
}

const fn retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_interval: Duration::from_millis(10),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_millis(50),
        non_retryable_errors: vec![],
        jitter: JitterPolicy::None,
    }
}

fn info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
    retry_policy: Option<RetryPolicy>,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tenant_propagation_tests",
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
        retry_policy,
    }
}

fn make_worker(registry: Arc<HandlerRegistry>) -> Worker {
    Worker::new(
        WorkerRuntimeConfig {
            codec_rotation_batch_size: 0,
            scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
            dr: autumn_harvest::replication::DrConfig::default(),
            worker_id: uuid::Uuid::new_v4().to_string(),
            queues: vec!["default".to_string()],
            notification_database_url: None,
            max_concurrent_workflows: 10,
            max_concurrent_activities: 20,
            poll_interval: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(2),
            cancellation_grace_period: Duration::from_secs(2),
            sticky_timeout: Duration::ZERO,
            max_local_activity_start_to_close: Duration::from_secs(60),
            shard_assignments: vec![ShardId::new(0)],
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            workflow_cache_size: 100,
            resident_workflows: true,
            priority_aging_secs: None,
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            capability_miss_max_redeliveries: 5,
            workflow_task_timeout: Duration::from_secs(10),
            workflow_panic_max_attempts: 3,
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            labels: std::collections::HashMap::new(),
            queue_weights: std::collections::HashMap::new(),
            max_workflow_history_events: None,
            shard_notification_database_urls: Vec::new(),
            sharded_pool: None,
            slot_tuner: None,
            max_concurrent_sessions: 0,
        },
        registry,
    )
    .expect("worker should build")
}

async fn start(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    input: Value,
    tenant: Option<&str>,
    retry: Option<RetryPolicy>,
) -> ExecutionId {
    let params = StartWorkflowParams {
        tenant,
        workflow_retry_policy: retry,
        ..StartWorkflowParams::new(
            workflow_name,
            workflow_id,
            ExecutionId::new_for_shard(ShardId::new(0)),
            input,
            "default",
        )
    };
    start_or_load_workflow_execution(conn, params, None)
        .await
        .expect("start")
        .exec_id
}

#[derive(diesel::QueryableByName, Debug, PartialEq, Eq)]
struct Row {
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Nullable<Text>)]
    tenant: Option<String>,
}

/// Every run of the given types, oldest first.
async fn rows(conn: &mut AsyncPgConnection, names: &[&str]) -> Vec<Row> {
    let names: Vec<String> = names.iter().map(ToString::to_string).collect();
    diesel::sql_query(
        "SELECT workflow_name, state, tenant FROM harvest_workflow_executions \
         WHERE workflow_name = ANY($1) ORDER BY created_at, id",
    )
    .bind::<diesel::sql_types::Array<Text>, _>(names)
    .load::<Row>(conn)
    .await
    .expect("load rows")
}

async fn scrub(conn: &mut AsyncPgConnection, names: &[&str]) {
    let names: Vec<String> = names.iter().map(ToString::to_string).collect();
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE workflow_name = ANY($1)")
        .bind::<diesel::sql_types::Array<Text>, _>(names)
        .execute(conn)
        .await
        .expect("scrub");
}

/// Children, continue-as-new successors and retries copy the tenant.
#[tokio::test]
async fn derived_runs_copy_the_tenant() {
    let (url, _container) = setup_db().await;
    let names = ["tp_parent", "tp_child", "tp_retry"];
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn, &names).await;

    let suffix = uuid::Uuid::new_v4();
    start(
        &mut conn,
        "tp_parent",
        &format!("parent-{suffix}"),
        json!({ "hop": 0 }),
        Some("acme"),
        None,
    )
    .await;
    start(
        &mut conn,
        "tp_retry",
        &format!("retry-{suffix}"),
        json!({}),
        Some("acme"),
        Some(retry_policy()),
    )
    .await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![
            info("tp_parent", tp_parent, None),
            info("tp_child", tp_child, None),
            info("tp_retry", tp_retry, Some(retry_policy())),
        ],
        vec![],
    ));
    let worker = Arc::new(make_worker(registry));
    let pool = build_pool(&url);
    let runner = worker.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(60), runner.run(&pool)).await;
    });

    // Two parent runs, four children and two retry attempts, all settled.
    let mut settled = Vec::new();
    for _ in 0..400 {
        settled = rows(&mut conn, &names).await;
        let done = settled.len() == 8
            && settled
                .iter()
                .all(|r| !matches!(r.state.as_str(), "RUNNING" | "PENDING"));
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    worker.shutdown();
    let _ = handle.await;

    assert_eq!(settled.len(), 8, "{settled:?}");
    let count = |name: &str| settled.iter().filter(|r| r.workflow_name == name).count();
    assert_eq!(count("tp_parent"), 2, "{settled:?}");
    assert_eq!(count("tp_child"), 4, "{settled:?}");
    assert_eq!(count("tp_retry"), 2, "{settled:?}");
    assert!(
        settled.iter().all(|r| r.tenant.as_deref() == Some("acme")),
        "every derived run must carry the tenant: {settled:?}"
    );
}

/// A reset fork and a re-run copy the tenant. A run with no tenant passes
/// none.
#[tokio::test]
async fn reset_and_rerun_copy_the_tenant() {
    let (url, _container) = setup_db().await;
    let names = ["tp_reset"];
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn, &names).await;

    for tenant in [Some("acme"), None] {
        let suffix = uuid::Uuid::new_v4();
        let source = start(
            &mut conn,
            "tp_reset",
            &format!("reset-{suffix}"),
            json!({}),
            tenant,
            None,
        )
        .await;
        let fork = reset_workflow_execution(
            &mut conn,
            source,
            WorkflowResetRequest {
                reset_to_event_id: Some(0),
                reset_point: None,
                reason: "tenant propagation".to_string(),
                operator_id: "test-operator".to_string(),
                signal_reapply: autumn_harvest::ResetSignalReapplyPolicy::default(),
                allow_terminal_source: false,
                refuse_erased_source: false,
            },
            None,
        )
        .await
        .expect("reset");
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
             SET state = 'FAILED', completed_at = now() WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(fork.new_exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("fail the fork");
        autumn_harvest::execution::rerun_workflow_execution(
            &mut conn,
            fork.new_exec_id,
            autumn_harvest::execution::RerunRequest {
                input_override: None,
                workflow_id_override: None,
                started_by: None,
                concurrency_key: None,
                concurrency_limit: None,
                concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
                max_workflow_input_bytes: 0,
                max_execution_timeout_ceiling: None,
                max_workflow_chain_timeout_ceiling: None,
                max_workflow_attempts_ceiling: None,
                trace_context: None,
            },
            None,
        )
        .await
        .expect("rerun");

        let all = rows(&mut conn, &names).await;
        let mine = all.iter().filter(|r| r.tenant.as_deref() == tenant).count();
        assert_eq!(
            mine, 3,
            "source, fork and re-run share tenant {tenant:?}: {all:?}"
        );
        scrub(&mut conn, &names).await;
    }
}

/// An invalid tenant fails the start, so no run can hold a key that no
/// credential could match.
#[tokio::test]
async fn an_invalid_tenant_fails_the_start() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for bad in ["", "a b", &"t".repeat(129)] {
        let params = StartWorkflowParams {
            tenant: Some(bad),
            ..StartWorkflowParams::new(
                "tp_invalid",
                "invalid-tenant",
                ExecutionId::new_for_shard(ShardId::new(0)),
                json!({}),
                "default",
            )
        };
        let result = start_or_load_workflow_execution(&mut conn, params, None).await;
        assert!(
            matches!(result, Err(autumn_harvest::HarvestError::Config(_))),
            "{bad:?}: {result:?}"
        );
    }
}

/// A reconciled `MIGRATED` seal of another tenant is a prior run too (issue
/// #1977). A tenant start must not attach to it, report it, or replace it.
#[tokio::test]
async fn a_tenant_start_refuses_a_reconciled_seal_of_another_tenant() {
    let (url, _container) = setup_db().await;
    let names = ["tp_seal"];
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn, &names).await;

    for (policy, seal_state) in [
        (
            autumn_harvest::WorkflowIdReusePolicy::RejectDuplicate,
            "COMPLETED",
        ),
        (
            autumn_harvest::WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
            "COMPLETED",
        ),
        (
            autumn_harvest::WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
            "FAILED",
        ),
    ] {
        let workflow_id = format!("seal-{}", uuid::Uuid::new_v4());
        let seal = start(
            &mut conn,
            "tp_seal",
            &workflow_id,
            json!({}),
            Some("globex"),
            None,
        )
        .await;
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET state = 'MIGRATED', \
             migrated_to_shard = 1, migrated_at = now(), \
             migrated_run_terminal_at = now(), migrated_run_terminal_state = $2 \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(seal.as_uuid())
        .bind::<Text, _>(seal_state)
        .execute(&mut conn)
        .await
        .expect("seal the run");

        let params = StartWorkflowParams {
            tenant: Some("acme"),
            reuse_policy: policy,
            ..StartWorkflowParams::new(
                "tp_seal",
                &workflow_id,
                ExecutionId::new_for_shard(ShardId::new(0)),
                json!({}),
                "default",
            )
        };
        let result = start_or_load_workflow_execution(&mut conn, params, None).await;
        assert!(
            matches!(
                result,
                Err(autumn_harvest::HarvestError::TenantConflict { .. })
            ),
            "{policy:?} over a {seal_state} seal: {result:?}"
        );
    }
    scrub(&mut conn, &names).await;
}

/// Idempotency keys are shared across tenants (issue #1977). A tenant start
/// whose key names a run of another tenant is a conflict. It must not return
/// that run as a duplicate.
#[tokio::test]
async fn a_tenant_start_refuses_an_idempotency_claim_of_another_tenant() {
    let (url, _container) = setup_db().await;
    let names = ["tp_idem"];
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn, &names).await;
    let key = format!("tp-idem-{}", uuid::Uuid::new_v4());
    let params = |tenant, workflow_id| StartWorkflowParams {
        tenant: Some(tenant),
        ..StartWorkflowParams::new(
            "tp_idem",
            workflow_id,
            ExecutionId::new_for_shard(ShardId::new(0)),
            json!({}),
            "default",
        )
    };

    let first = autumn_harvest::start_or_load_workflow_execution_idempotent(
        &mut conn,
        params("globex", "tp-idem-globex"),
        &key,
        3600.0,
        None,
        None,
    )
    .await
    .expect("start the globex run");
    assert!(matches!(
        first,
        autumn_harvest::IdempotentStartOutcome::Started(_)
    ));

    let same_tenant = autumn_harvest::start_or_load_workflow_execution_idempotent(
        &mut conn,
        params("globex", "tp-idem-globex"),
        &key,
        3600.0,
        None,
        None,
    )
    .await
    .expect("the owner replays its key");
    assert!(matches!(
        same_tenant,
        autumn_harvest::IdempotentStartOutcome::Deduplicated { .. }
    ));

    let other = autumn_harvest::start_or_load_workflow_execution_idempotent(
        &mut conn,
        params("acme", "tp-idem-acme"),
        &key,
        3600.0,
        None,
        None,
    )
    .await;
    assert!(
        matches!(
            other,
            Err(autumn_harvest::HarvestError::TenantConflict { .. })
        ),
        "{other:?}"
    );
    scrub(&mut conn, &names).await;
}

#[derive(diesel::QueryableByName, Debug)]
struct OutputRow {
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Nullable<Jsonb>)]
    output: Option<Value>,
}

/// An activity reads the verified tenant of its run, and `None` for a run
/// with no tenant (issue #1998).
#[tokio::test]
async fn an_activity_reads_the_verified_tenant() {
    let (url, _container) = setup_db().await;
    let names = ["tp_tenant_reader"];
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn, &names).await;

    let suffix = uuid::Uuid::new_v4();
    let tenanted = format!("reader-acme-{suffix}");
    let untenanted = format!("reader-none-{suffix}");
    let reader = "tp_tenant_reader";
    start(&mut conn, reader, &tenanted, json!({}), Some("acme"), None).await;
    start(&mut conn, reader, &untenanted, json!({}), None, None).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![info("tp_tenant_reader", tp_tenant_reader, None)],
        vec![tp_read_tenant_info()],
    ));
    let worker = Arc::new(make_worker(registry));
    let pool = build_pool(&url);
    let runner = worker.clone();
    let handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(60), runner.run(&pool)).await;
    });

    let output = |workflow_id: String| {
        diesel::sql_query(
            "SELECT state, output FROM harvest_workflow_executions WHERE workflow_id = $1",
        )
        .bind::<Text, _>(workflow_id)
    };
    let mut settled: Vec<OutputRow> = Vec::new();
    for _ in 0..400 {
        settled = Vec::new();
        for id in [&tenanted, &untenanted] {
            let mut rows: Vec<OutputRow> = output(id.clone())
                .load(&mut conn)
                .await
                .expect("load output");
            settled.append(&mut rows);
        }
        if settled.len() == 2 && settled.iter().all(|r| r.state == "COMPLETED") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    worker.shutdown();
    let _ = handle.await;

    assert_eq!(settled.len(), 2, "{settled:?}");
    assert!(
        settled.iter().all(|r| r.state == "COMPLETED"),
        "{settled:?}"
    );
    assert_eq!(settled[0].output, Some(json!("acme")), "{settled:?}");
    // The engine can store a JSON null output as SQL NULL.
    let none = settled[1].output.clone().unwrap_or(Value::Null);
    assert_eq!(none, Value::Null, "{settled:?}");
    scrub(&mut conn, &names).await;
}
