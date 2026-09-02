//! Issue #1128: the **standalone** `HarvestRunner::start` embedder path runs the
//! same boot-time orphaned-workflow-type reachability gate the plugin boot path
//! got in #700/PR #1109.
//!
//! Before this, a standalone deployment could boot with orphaned non-terminal
//! executions (a workflow type with in-flight runs but no registered handler)
//! entirely unflagged — the same latent leak the #700 P1 fix closed for the
//! plugin. These tests drive the REAL entry point (`HarvestRunner::start`), not
//! the decision helper, so they pin the wiring and not just the policy.
//!
//! What is pinned here:
//!
//! - `fail` + a seeded orphan → `start` returns `Err`, **and** the orphan's
//!   claimable task row is untouched (`PENDING`, unclaimed, no `WorkflowFailed`
//!   event) — the deterministic analogue of "the gate ran before any worker
//!   could claim it", asserted with `worker_enabled = true`.
//! - `warn` / `off` + the same orphan → `start` returns `Ok`.
//! - `fail` + a clean fleet → `start` returns `Ok`.
//! - a multi-shard standalone deployment finds an orphan on a **non-zero**
//!   shard (the plugin's single-shard gate could not).
//! - a caller that already ran the gate (the plugin path) is not gated twice.

use std::pin::Pin;

use autumn_harvest::WorkflowContext;
use autumn_harvest::WorkflowEvent;
use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::config::{
    HarvestRuntimeConfig, HarvestStartupConfig, OrphanStartupAction,
};
use autumn_harvest_plugin::runner::{HarvestRunner, HarvestRunnerResources};
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn noop_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

fn workflow_info_named(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: noop_workflow,
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

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn setup_database_url_with_migrations() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
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
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    (url, container)
}

/// A live, migrated Harvest database: the `HARVEST_TEST_DATABASE_URL` server
/// when one is exported, otherwise a throwaway container (CI, one per test).
async fn setup_db_env_or_container() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let (url, container) = setup_database_url_with_migrations().await;
    (url, Some(container))
}

/// Create a SECOND migrated database on the same server as `database_url` and
/// return its URL. Used for the multi-shard case, where shard 1 must be a
/// genuinely different database than shard 0.
async fn sibling_database_with_migrations(database_url: &str) -> String {
    let name = format!("harvest_gate_shard_{}", uuid::Uuid::new_v4().simple());
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
            .await
            .expect("connect to create sibling database");
        diesel::sql_query(format!("CREATE DATABASE {name}"))
            .execute(&mut conn)
            .await
            .expect("create sibling database");
    }
    let sibling = match database_url.rsplit_once('/') {
        Some((prefix, _)) => format!("{prefix}/{name}"),
        None => panic!("database url has no path component: {database_url}"),
    };
    autumn_web::migrate::run_pending(&sibling, autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations on the sibling database");
    sibling
}

/// Seed a non-terminal execution of `workflow_name` on `shard`.
async fn insert_execution(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: shard.as_i32(),
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution");

    let events = vec![WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("failed to append start event");
    exec_id
}

/// Seed a claimable `PENDING` workflow task for `exec_id`, immediately eligible.
async fn insert_claimable_task(database_url: &str, exec_id: ExecutionId) -> uuid::Uuid {
    use autumn_harvest::schema::harvest_task_queue;

    let task_id = uuid::Uuid::new_v4();
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect to seed task");
    diesel::insert_into(harvest_task_queue::table)
        .values((
            harvest_task_queue::id.eq(task_id),
            harvest_task_queue::queue_name.eq("default"),
            harvest_task_queue::task_type.eq("workflow"),
            harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())),
            harvest_task_queue::input.eq(json!({})),
            harvest_task_queue::max_attempts.eq(1),
            harvest_task_queue::scheduled_at.eq(Utc::now() - chrono::Duration::seconds(5)),
        ))
        .execute(&mut conn)
        .await
        .expect("seed pending workflow task");
    task_id
}

/// Every workflow type that currently has a non-terminal execution in this
/// database.
///
/// The gate inspects the WHOLE database (it has no `?workflow_type=` filter —
/// a boot gate must see every orphan), so a "clean fleet" test cannot simply
/// register its own type and hope the database holds nothing else: a shared
/// `HARVEST_TEST_DATABASE_URL` server carries other suites' rows. Registering
/// exactly the set of in-flight types makes "no orphans" true by construction,
/// in both the container and shared-server modes.
async fn non_terminal_workflow_types(database_url: &str) -> Vec<String> {
    use autumn_harvest::schema::harvest_workflow_executions as e;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect to list non-terminal types");
    // The exact complement of the terminal-state list in the engine's
    // `NON_TERMINAL_COUNTS_SQL`, so this can only ever be a SUPERSET of what the
    // gate counts — never a subset, which would leave an orphan unregistered
    // and make the "clean fleet" assertion a false failure.
    let names: Vec<String> = e::table
        .filter(e::state.ne_all(vec![
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ]))
        .select(e::workflow_name)
        .distinct()
        .load(&mut conn)
        .await
        .expect("load non-terminal workflow names");
    names
}

fn runtime_config(action: OrphanStartupAction, worker_enabled: bool) -> HarvestRuntimeConfig {
    HarvestRuntimeConfig {
        worker_enabled,
        scheduler_enabled: false,
        startup: HarvestStartupConfig {
            orphaned_workflows: action,
        },
        ..HarvestRuntimeConfig::default()
    }
}

fn single_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    )
}

// ---------------------------------------------------------------------------
// AC1 — the gate exists on the standalone path and refuses boot under `fail`
// ---------------------------------------------------------------------------

/// `fail` + a seeded orphan → `HarvestRunner::start` refuses to boot, naming the
/// orphaned type.
///
/// `worker_enabled` is deliberately TRUE: the gate must fire before the runner
/// spawns the worker poll loop, so the assertions below (the orphan's task row
/// is still claimable and its history carries no `WorkflowFailed`) are the
/// deterministic analogue of "no worker claimed or terminally failed the
/// orphaned run" — exactly the #700 P1 guarantee, now on the standalone path.
#[tokio::test]
async fn standalone_start_refuses_boot_on_orphan_under_fail() {
    use autumn_harvest::schema::{harvest_events, harvest_task_queue, harvest_workflow_executions};

    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = format!("runner_gate_fail_orphan_{}", uuid::Uuid::new_v4().simple());
    let exec_id = insert_execution(
        &database_url,
        ShardId::new(0),
        &orphan_type,
        &format!("rgf-{}", uuid::Uuid::new_v4()),
    )
    .await;
    let task_id = insert_claimable_task(&database_url, exec_id).await;

    // A build that registers a DIFFERENT workflow type: `orphan_type` has an
    // in-flight run but no handler here.
    let built = HarvestBuilder::new()
        .workflows(vec![workflow_info_named("runner_gate_registered")])
        .build();

    let result = HarvestRunner::start(
        built,
        &runtime_config(OrphanStartupAction::Fail, true),
        HarvestRunnerResources::new(build_test_pool(&database_url))
            .with_shard_router(single_shard_router()),
    )
    .await;

    let Err(error) = result else {
        panic!("standalone start must refuse to boot with an orphaned workflow type");
    };
    let message = error.to_string();
    assert!(
        message.contains(&orphan_type),
        "the refusal must name the orphaned workflow type; got: {message}"
    );

    // The gate ran BEFORE any worker could claim the orphan.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to verify");
    let (state, worker_id): (String, Option<String>) = harvest_task_queue::table
        .find(task_id)
        .select((harvest_task_queue::state, harvest_task_queue::worker_id))
        .first(&mut conn)
        .await
        .expect("task row still present");
    assert_eq!(state, "PENDING", "the orphan task must remain claimable");
    assert!(worker_id.is_none(), "the orphan task must be unclaimed");

    let exec_state: String = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(&mut conn)
        .await
        .expect("execution row still present");
    assert_eq!(
        exec_state, "RUNNING",
        "the orphan execution must stay RUNNING"
    );

    let failed_events: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_events::event_type.eq("WorkflowFailed"))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count events");
    assert_eq!(
        failed_events, 0,
        "an aborted boot must not have failed the orphaned run"
    );
}

// ---------------------------------------------------------------------------
// AC2 — the same off/warn/fail action drives it
// ---------------------------------------------------------------------------

/// `warn` (the default) never blocks boot, even with an orphan present.
#[tokio::test]
async fn standalone_start_boots_under_warn_with_orphan() {
    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = format!("runner_gate_warn_orphan_{}", uuid::Uuid::new_v4().simple());
    insert_execution(
        &database_url,
        ShardId::new(0),
        &orphan_type,
        &format!("rgw-{}", uuid::Uuid::new_v4()),
    )
    .await;

    let built = HarvestBuilder::new()
        .workflows(vec![workflow_info_named("runner_gate_registered")])
        .build();
    let runner = HarvestRunner::start(
        built,
        &runtime_config(OrphanStartupAction::Warn, false),
        HarvestRunnerResources::new(build_test_pool(&database_url))
            .with_shard_router(single_shard_router()),
    )
    .await
    .expect("warn must never refuse boot");
    runner.stop().await;
}

/// `off` skips the check entirely — an orphan present, boot unaffected.
#[tokio::test]
async fn standalone_start_boots_under_off_with_orphan() {
    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = format!("runner_gate_off_orphan_{}", uuid::Uuid::new_v4().simple());
    insert_execution(
        &database_url,
        ShardId::new(0),
        &orphan_type,
        &format!("rgo-{}", uuid::Uuid::new_v4()),
    )
    .await;

    let built = HarvestBuilder::new()
        .workflows(vec![workflow_info_named("runner_gate_registered")])
        .build();
    let runner = HarvestRunner::start(
        built,
        &runtime_config(OrphanStartupAction::Off, false),
        HarvestRunnerResources::new(build_test_pool(&database_url))
            .with_shard_router(single_shard_router()),
    )
    .await
    .expect("off must skip the gate entirely");
    runner.stop().await;
}

/// `fail` with a clean fleet boots normally — the strict action is not a
/// blanket refusal.
#[tokio::test]
async fn standalone_start_boots_under_fail_when_clean() {
    let (database_url, _container) = setup_db_env_or_container().await;
    let in_use_type = format!("runner_gate_in_use_{}", uuid::Uuid::new_v4().simple());
    insert_execution(
        &database_url,
        ShardId::new(0),
        &in_use_type,
        &format!("rgc-{}", uuid::Uuid::new_v4()),
    )
    .await;

    // Register every in-flight type (see `non_terminal_workflow_types`) so the
    // fleet is orphan-free by construction on a shared test server too.
    let registered: Vec<WorkflowInfo> = non_terminal_workflow_types(&database_url)
        .await
        .into_iter()
        .map(|name| workflow_info_named(Box::leak(name.into_boxed_str())))
        .collect();
    assert!(
        registered.iter().any(|info| info.name == in_use_type),
        "the seeded in-use type must be among the registered handlers"
    );

    let built = HarvestBuilder::new().workflows(registered).build();
    let runner = HarvestRunner::start(
        built,
        &runtime_config(OrphanStartupAction::Fail, false),
        HarvestRunnerResources::new(build_test_pool(&database_url))
            .with_shard_router(single_shard_router()),
    )
    .await
    .expect("a clean fleet must boot under fail");
    runner.stop().await;
}

// ---------------------------------------------------------------------------
// AC3 — the standalone path is the multi-shard path: every shard is inspected
// ---------------------------------------------------------------------------

/// The standalone runner supports genuinely multi-shard deployments (#522),
/// which the plugin rejects. An orphan on a NON-ZERO shard must still refuse
/// boot: a shard-0-only gate (the plugin's, which is sound only because the
/// plugin is single-shard) would miss it and report a `complete`, clean fleet.
#[tokio::test]
async fn standalone_gate_detects_an_orphan_on_a_non_zero_shard() {
    let (shard0_url, _container) = setup_db_env_or_container().await;
    let shard1_url = sibling_database_with_migrations(&shard0_url).await;

    let orphan_type = format!(
        "runner_gate_shard1_orphan_{}",
        uuid::Uuid::new_v4().simple()
    );
    insert_execution(
        &shard1_url,
        ShardId::new(1),
        &orphan_type,
        &format!("rgs1-{}", uuid::Uuid::new_v4()),
    )
    .await;

    let sharded = ShardedDbPool::from_map(
        [
            (ShardId::new(0), build_test_pool(&shard0_url)),
            (ShardId::new(1), build_test_pool(&shard1_url)),
        ]
        .into_iter()
        .collect(),
        ShardId::new(0),
    );
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let built = HarvestBuilder::new()
        .workflows(vec![workflow_info_named("runner_gate_registered")])
        .build();
    let result = HarvestRunner::start(
        built,
        &runtime_config(OrphanStartupAction::Fail, false),
        HarvestRunnerResources::new(build_test_pool(&shard0_url))
            .with_shard_router(router)
            .with_sharded_pool(sharded),
    )
    .await;

    let Err(error) = result else {
        panic!("an orphan on shard 1 must refuse boot just as one on shard 0 does");
    };
    assert!(
        error.to_string().contains(&orphan_type),
        "the refusal must name the shard-1 orphaned type; got: {error}"
    );
}

// ---------------------------------------------------------------------------
// AC4 — a caller that already ran the gate is not gated twice
// ---------------------------------------------------------------------------

/// The plugin boot path runs the gate itself, EARLIER than the runner could —
/// before it publishes any admission global — and marks its resources so
/// `HarvestRunner::start` does not repeat the scan (and its warnings). With the
/// marker set, an orphan that would otherwise abort under `fail` does not.
#[tokio::test]
async fn standalone_start_skips_the_gate_when_the_caller_already_ran_it() {
    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = format!("runner_gate_skip_orphan_{}", uuid::Uuid::new_v4().simple());
    insert_execution(
        &database_url,
        ShardId::new(0),
        &orphan_type,
        &format!("rgsk-{}", uuid::Uuid::new_v4()),
    )
    .await;

    let built = HarvestBuilder::new()
        .workflows(vec![workflow_info_named("runner_gate_registered")])
        .build();
    let runner = HarvestRunner::start(
        built,
        &runtime_config(OrphanStartupAction::Fail, false),
        HarvestRunnerResources::new(build_test_pool(&database_url))
            .with_shard_router(single_shard_router())
            .with_startup_orphan_gate_already_run(),
    )
    .await
    .expect("a caller that already ran the gate must not be gated twice");
    runner.stop().await;
}
