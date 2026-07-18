//! Integration tests for `GET /admin/workflow-types/reachability` (issue #520).
//!
//! Verifies the three verdicts (`safe_to_remove` / `in_use` / `orphaned`), the
//! `?workflow_type=` filter, cross-shard aggregation with an unreachable shard
//! reported as `partial`, and the admin auth boundary.

use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowContext;
use autumn_harvest::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::OrphanStartupAction;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_harvest_plugin::runner::resolve_runtime_storage_pool;
use autumn_harvest_plugin::workflow_reachability::{
    StartupOrphanDecision, WorkflowReachabilityQuery, build_reachability_report_single_shard,
    build_workflow_reachability_report, startup_orphan_decision,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

/// A trivial workflow handler — only its registration name matters here.
fn noop_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

fn workflow_info_named(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "tests",
        handler: noop_workflow,
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

fn single_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    )
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

fn build_api_state(
    pool: HarvestDbPool,
    router: ShardRouter,
    registered: Vec<&'static str>,
) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(pool);
    let workflows = registered.into_iter().map(workflow_info_named).collect();
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(workflows, vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::<WorkflowSchedule>::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    api_state
}

fn build_api_app(
    pool: HarvestDbPool,
    router: ShardRouter,
    registered: Vec<&'static str>,
) -> HarvestApiApp {
    harvest_api_router(build_api_state(pool, router, registered))
        .with_state(autumn_web::AppState::for_test())
}

async fn read_json_response(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    serde_json::from_slice(&body).expect("response must be JSON")
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn insert_execution(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");
    let row = NewWorkflowExecution {
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

    let completed_at = if state == "RUNNING" {
        None
    } else {
        Some(Utc::now())
    };
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table.find(exec_id.as_uuid()),
    )
    .set((
        autumn_harvest::schema::harvest_workflow_executions::state.eq(state),
        autumn_harvest::schema::harvest_workflow_executions::completed_at.eq(completed_at),
    ))
    .execute(&mut conn)
    .await
    .expect("failed to update workflow state");

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

fn item<'a>(report: &'a Value, workflow_type: &str) -> &'a Value {
    report
        .get("items")
        .and_then(Value::as_array)
        .expect("items array")
        .iter()
        .find(|item| item.get("workflow_type").and_then(Value::as_str) == Some(workflow_type))
        .unwrap_or_else(|| panic!("expected item for {workflow_type}"))
}

#[tokio::test]
async fn reachability_assigns_safe_in_use_and_orphaned_verdicts() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    // onboarding: registered, all terminal -> safe_to_remove.
    insert_execution(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "ob-1",
        "COMPLETED",
    )
    .await;
    // subscription: registered, one RUNNING -> in_use.
    insert_execution(
        &database_url,
        ShardId::new(0),
        "subscription",
        "sub-1",
        "RUNNING",
    )
    .await;
    insert_execution(
        &database_url,
        ShardId::new(0),
        "subscription",
        "sub-2",
        "COMPLETED",
    )
    .await;
    // legacy_export: NOT registered, one RUNNING -> orphaned.
    insert_execution(
        &database_url,
        ShardId::new(0),
        "legacy_export",
        "leg-1",
        "RUNNING",
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec!["onboarding", "subscription"],
    );

    let (status, report) = get_json(&app, "/admin/workflow-types/reachability").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("status").and_then(Value::as_str),
        Some("complete")
    );

    let onboarding = item(&report, "onboarding");
    assert_eq!(onboarding.get("registered"), Some(&json!(true)));
    assert_eq!(onboarding.get("non_terminal_count"), Some(&json!(0)));
    assert_eq!(
        onboarding.get("verdict").and_then(Value::as_str),
        Some("safe_to_remove")
    );

    let subscription = item(&report, "subscription");
    assert_eq!(subscription.get("registered"), Some(&json!(true)));
    assert_eq!(subscription.get("non_terminal_count"), Some(&json!(1)));
    assert_eq!(
        subscription.get("verdict").and_then(Value::as_str),
        Some("in_use")
    );
    assert!(
        subscription
            .get("oldest_non_terminal_age_secs")
            .and_then(Value::as_i64)
            .is_some()
    );

    let legacy = item(&report, "legacy_export");
    assert_eq!(legacy.get("registered"), Some(&json!(false)));
    assert_eq!(legacy.get("non_terminal_count"), Some(&json!(1)));
    assert_eq!(
        legacy.get("verdict").and_then(Value::as_str),
        Some("orphaned")
    );
}

#[tokio::test]
async fn reachability_filter_for_absent_type_returns_safe_zero_object() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec!["onboarding"],
    );

    let (status, report) = get_json(
        &app,
        "/admin/workflow-types/reachability?workflow_type=never_started",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = report
        .get("items")
        .and_then(Value::as_array)
        .expect("items");
    assert_eq!(items.len(), 1);
    let only = &items[0];
    assert_eq!(
        only.get("workflow_type").and_then(Value::as_str),
        Some("never_started")
    );
    assert_eq!(only.get("non_terminal_count"), Some(&json!(0)));
    assert_eq!(
        only.get("verdict").and_then(Value::as_str),
        Some("safe_to_remove")
    );
}

#[tokio::test]
async fn reachability_aggregates_across_shards_and_reports_unavailable_shard() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    insert_execution(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "ob-shard0",
        "RUNNING",
    )
    .await;

    // Shard 0 points at the live DB; shard 1 points at an unreachable URL.
    let mut shard_pools = std::collections::BTreeMap::new();
    shard_pools.insert(ShardId::new(0), build_test_pool(&database_url));
    shard_pools.insert(
        ShardId::new(1),
        build_test_pool("postgres://postgres:postgres@127.0.0.1:1/nonexistent"),
    );
    let pool = HarvestDbPool::sharded(ShardedDbPool::from_map(shard_pools, ShardId::new(0)));
    let app = build_api_app(pool, two_shard_router(), vec!["onboarding"]);

    let (status, report) = get_json(&app, "/admin/workflow-types/reachability").await;
    assert_eq!(status, StatusCode::OK);
    // A partial answer must never be mistaken for "safe to remove".
    assert_eq!(
        report.get("status").and_then(Value::as_str),
        Some("partial")
    );

    let inspections = report
        .get("shards")
        .and_then(Value::as_array)
        .expect("shards");
    let unreachable = inspections
        .iter()
        .find(|entry| entry.get("shard_id").and_then(Value::as_i64) == Some(1))
        .expect("shard 1 reported");
    assert_eq!(
        unreachable.get("status").and_then(Value::as_str),
        Some("unavailable")
    );
    assert!(unreachable.get("error").and_then(Value::as_str).is_some());

    let onboarding = item(&report, "onboarding");
    assert_eq!(onboarding.get("non_terminal_count"), Some(&json!(1)));
    assert_eq!(
        onboarding.get("verdict").and_then(Value::as_str),
        Some("in_use")
    );
}

#[tokio::test]
async fn orphaned_group_surfaces_bounded_execution_id_samples() {
    // AC2: an orphaned group carries a bounded set of representative
    // non-terminal execution ids (cap 5), all drawn from the live runs.
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let mut seeded = std::collections::BTreeSet::new();
    for n in 0..7 {
        let exec_id = insert_execution(
            &database_url,
            ShardId::new(0),
            "legacy_export",
            &format!("leg-{n}"),
            "RUNNING",
        )
        .await;
        seeded.insert(exec_id.as_uuid());
    }
    // A terminal run of the same type must NOT appear among the samples.
    insert_execution(
        &database_url,
        ShardId::new(0),
        "legacy_export",
        "leg-done",
        "COMPLETED",
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec![], // legacy_export is unregistered -> orphaned
    );

    let (status, report) = get_json(&app, "/admin/workflow-types/reachability").await;
    assert_eq!(status, StatusCode::OK);

    let legacy = item(&report, "legacy_export");
    assert_eq!(
        legacy.get("verdict").and_then(Value::as_str),
        Some("orphaned")
    );
    assert_eq!(legacy.get("non_terminal_count"), Some(&json!(7)));

    let samples = legacy
        .get("sample_execution_ids")
        .and_then(Value::as_array)
        .expect("sample_execution_ids array");
    assert_eq!(samples.len(), 5, "samples must be capped at 5");
    for sample in samples {
        let id: uuid::Uuid = sample
            .as_str()
            .expect("sample id is a string")
            .parse()
            .expect("sample id is a uuid");
        assert!(
            seeded.contains(&id),
            "every sample must be one of the seeded non-terminal runs"
        );
    }
}

#[tokio::test]
async fn endpoint_exposes_top_level_orphaned_and_total() {
    // AC3: a single top-level `orphaned` bool + `total_orphaned_executions`
    // count so a CI/CD gate asserts "zero orphans" without walking items[].
    let (database_url, _container) = setup_database_url_with_migrations().await;
    // subscription is registered with a live run -> in_use (NOT counted).
    insert_execution(
        &database_url,
        ShardId::new(0),
        "subscription",
        "sub-1",
        "RUNNING",
    )
    .await;
    // legacy_export is unregistered with two live runs -> orphaned (counted).
    for n in 0..2 {
        insert_execution(
            &database_url,
            ShardId::new(0),
            "legacy_export",
            &format!("leg-{n}"),
            "RUNNING",
        )
        .await;
    }

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec!["subscription"],
    );

    let (status, report) = get_json(&app, "/admin/workflow-types/reachability").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(report.get("orphaned"), Some(&json!(true)));
    // Only the orphaned executions are counted; the in_use subscription run is not.
    assert_eq!(report.get("total_orphaned_executions"), Some(&json!(2)));
}

#[tokio::test]
async fn reachability_requires_admin_auth() {
    // No admin boundary set -> the shared `/admin/*` guard must reject.
    let app =
        harvest_api_router(HarvestApiState::new()).with_state(autumn_web::AppState::for_test());
    let (status, _) = get_json(&app, "/admin/workflow-types/reachability").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// Boot-time orphaned-workflow reachability gate (issue #700 AC4).
// ---------------------------------------------------------------------------
//
// `start_harvest_runtime` is a private, config-file/migration/pool-coupled
// `async fn` that spawns background tasks — it is not integration-callable, and
// on the abort path it tears those tasks down and returns `Err` *before*
// autumn-web reacts to a failed `on_startup` hook. What that abort decision
// depends on is a two-step composition of PUBLIC building blocks:
//
//   1. `build_workflow_reachability_report(api_state, ..)` — the cross-shard DB
//      fan-out (the riskiest new code), and
//   2. `startup_orphan_decision(action, report.orphaned, report.status)` — the
//      pure gate rule.
//
// These tests drive that exact decision path against a seeded database, so they
// exercise the real report builder end-to-end and confirm the Abort / Warn /
// Continue verdict the boot gate would produce. They deliberately do NOT boot a
// full autumn-web app (the only way to invoke the private `start_harvest_runtime`
// shell), which is thin task-teardown glue over this decision.
//
// Each test filters the report to a UNIQUE workflow-type name, which narrows
// `report.orphaned` / `total_orphaned_executions` to just that type — isolating
// it from leftover or concurrently-seeded rows on a shared database with no
// global scrub (so the tests are re-runnable and race-free).

/// Env-URL (local Postgres, no Docker) when `HARVEST_TEST_DATABASE_URL` is set,
/// otherwise a fresh testcontainer. Mirrors the core-crate DB-test precedent
/// (e.g. `retention_summary_tests`): the env URL is assumed to point at an
/// already-migrated database, so no migrations are run on that path.
async fn setup_db_env_or_container() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let (url, container) = setup_database_url_with_migrations().await;
    (url, Some(container))
}

#[tokio::test]
async fn boot_gate_fail_action_aborts_on_seeded_orphan() {
    // `fail` + a seeded orphan (unregistered type with a live run) + a complete
    // report -> the gate aborts. This is exactly the decision that makes
    // `start_harvest_runtime` return `Err` and refuse boot.
    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = "boot_gate_fail_orphan";
    insert_execution(
        &database_url,
        ShardId::new(0),
        orphan_type,
        &format!("bg-fail-{}", uuid::Uuid::new_v4()),
        "RUNNING",
    )
    .await;

    let api_state = build_api_state(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec![], // orphan_type is unregistered -> orphaned
    );

    let report = build_workflow_reachability_report(
        &api_state,
        WorkflowReachabilityQuery {
            workflow_type: Some(orphan_type.to_string()),
        },
    )
    .await
    .expect("reachability report builds");

    assert!(report.orphaned, "seeded orphan must set report.orphaned");
    assert!(report.total_orphaned_executions >= 1);
    assert_eq!(
        startup_orphan_decision(OrphanStartupAction::Fail, report.orphaned, report.status),
        StartupOrphanDecision::Abort,
        "fail + orphan + complete report must abort boot",
    );
}

#[tokio::test]
async fn boot_gate_warn_action_allows_boot_with_orphan() {
    // Same seeded orphan, but `warn` -> the gate warns and boot continues (never
    // Abort), so `start_harvest_runtime` returns `Ok`.
    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = "boot_gate_warn_orphan";
    insert_execution(
        &database_url,
        ShardId::new(0),
        orphan_type,
        &format!("bg-warn-{}", uuid::Uuid::new_v4()),
        "RUNNING",
    )
    .await;

    let api_state = build_api_state(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec![], // unregistered -> orphaned
    );

    let report = build_workflow_reachability_report(
        &api_state,
        WorkflowReachabilityQuery {
            workflow_type: Some(orphan_type.to_string()),
        },
    )
    .await
    .expect("reachability report builds");

    assert!(report.orphaned);
    let decision =
        startup_orphan_decision(OrphanStartupAction::Warn, report.orphaned, report.status);
    assert_eq!(
        decision,
        StartupOrphanDecision::Warn,
        "warn + orphan must warn, not abort",
    );
    assert_ne!(
        decision,
        StartupOrphanDecision::Abort,
        "boot must not be refused under warn",
    );
}

#[tokio::test]
async fn boot_gate_fail_action_allows_clean_boot() {
    // `fail` with NO orphan (only a registered type with a live run -> in_use)
    // -> the gate continues, so a clean fleet boots even under the strict action.
    let (database_url, _container) = setup_db_env_or_container().await;
    let registered_type = "boot_gate_clean_registered";
    insert_execution(
        &database_url,
        ShardId::new(0),
        registered_type,
        &format!("bg-clean-{}", uuid::Uuid::new_v4()),
        "RUNNING",
    )
    .await;

    let api_state = build_api_state(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec![registered_type], // registered -> in_use, NOT orphaned
    );

    let report = build_workflow_reachability_report(
        &api_state,
        WorkflowReachabilityQuery {
            workflow_type: Some(registered_type.to_string()),
        },
    )
    .await
    .expect("reachability report builds");

    assert!(
        !report.orphaned,
        "a registered in-use type is not an orphan"
    );
    assert_eq!(report.total_orphaned_executions, 0);
    assert_eq!(
        startup_orphan_decision(OrphanStartupAction::Fail, report.orphaned, report.status),
        StartupOrphanDecision::Continue,
        "fail with zero orphans must allow boot",
    );
}

// -------------------------------------------------------------------------
// Issue #700 AC4 (P1 fix): the boot-time orphan gate runs BEFORE workers spawn.
//
// The production race (a worker claiming/failing an orphaned run in the boot
// window before an after-the-fact abort) cannot be booted from a test — the
// only entry point, the private `start_harvest_runtime`, is pool/migration/
// full-app coupled. These two tests prove the fix structurally and behaviorally:
// (1) the gate core needs NO started runtime (so it can precede the runner),
// and (2) running the core + decision is side-effect-free (a seeded orphan's
// claimable task row and execution are untouched, and no `WorkflowFailed` event
// is appended) — the deterministic analogue of "no worker touched the task."
// -------------------------------------------------------------------------

/// The boot-gate core resolves the abort decision from just a `registered` set
/// and a shard pool — with NO `HarvestApiState`/runtime constructed. That is
/// the compile-time + runtime evidence that the gate can run before
/// `HarvestRunner::start` spawns any worker (the whole point of the P1 fix).
#[tokio::test]
async fn gate_report_core_needs_no_runtime() {
    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = "gate_core_no_runtime_orphan";
    insert_execution(
        &database_url,
        ShardId::new(0),
        orphan_type,
        &format!("gcnr-{}", uuid::Uuid::new_v4()),
        "RUNNING",
    )
    .await;

    // Deliberately construct NO HarvestApiState: registered is derived directly
    // (as the plugin does from the owned `BuiltHarvest` before the runner starts).
    let registered: BTreeSet<String> = BTreeSet::new();
    let pool = build_test_pool(&database_url);

    let report =
        build_reachability_report_single_shard(&registered, &pool, Some(orphan_type.to_string()))
            .await;

    assert!(
        report.orphaned,
        "seeded unregistered orphan must set orphaned"
    );
    assert!(report.total_orphaned_executions >= 1);
    assert_eq!(
        startup_orphan_decision(OrphanStartupAction::Fail, report.orphaned, report.status),
        StartupOrphanDecision::Abort,
        "fail + orphan + complete single-shard report must abort boot",
    );
}

/// Running the pre-runner gate core + decision does NOT mutate the seeded
/// orphan: its claimable pending task-queue row stays `PENDING`, its execution
/// stays `RUNNING`, and no `WorkflowFailed` event is appended. This is the
/// deterministic analogue of the production guarantee "no worker claimed or
/// failed the orphaned run" — the gate is a pure read that precedes any worker.
#[tokio::test]
async fn gate_is_side_effect_free_orphan_task_untouched() {
    use autumn_harvest::schema::{harvest_events, harvest_task_queue};

    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = "gate_side_effect_free_orphan";
    let exec_id = insert_execution(
        &database_url,
        ShardId::new(0),
        orphan_type,
        &format!("gsef-{}", uuid::Uuid::new_v4()),
        "RUNNING",
    )
    .await;

    // Seed a claimable pending workflow task for the orphan (state defaults to
    // PENDING; scheduled_at in the past = immediately eligible).
    let task_id = uuid::Uuid::new_v4();
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
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
    }

    // Run the pre-runner gate core + decision (as `start_harvest_runtime` now
    // does before spawning any worker).
    let registered: BTreeSet<String> = BTreeSet::new();
    let pool = build_test_pool(&database_url);
    let report =
        build_reachability_report_single_shard(&registered, &pool, Some(orphan_type.to_string()))
            .await;
    assert_eq!(
        startup_orphan_decision(OrphanStartupAction::Fail, report.orphaned, report.status),
        StartupOrphanDecision::Abort,
        "the seeded orphan must resolve to Abort",
    );

    // Re-query: the gate touched nothing.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect to verify");

    let task_state: String = harvest_task_queue::table
        .find(task_id)
        .select(harvest_task_queue::state)
        .first(&mut conn)
        .await
        .expect("task row still present");
    assert_eq!(
        task_state, "PENDING",
        "the orphan task must remain claimable"
    );

    let task_worker: Option<String> = harvest_task_queue::table
        .find(task_id)
        .select(harvest_task_queue::worker_id)
        .first(&mut conn)
        .await
        .expect("task row still present");
    assert!(task_worker.is_none(), "the orphan task must be unclaimed");

    let exec_state: String = autumn_harvest::schema::harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(autumn_harvest::schema::harvest_workflow_executions::state)
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
    assert_eq!(failed_events, 0, "the gate must not append WorkflowFailed");
}

/// Issue #700 P2: the boot-gate must query the SAME storage pool the runner will
/// run the workers against. `PreparedHarvestRuntime::build` gives
/// `WorkerConfig::with_sharded_pool` PRECEDENCE over `harvest_pool`, so a
/// `HarvestBuilder::with_sharded_pool` deployment must have the gate query the
/// sharded pool's database — not `harvest_pool`. Both call the shared
/// `resolve_runtime_storage_pool`, so they agree by construction.
///
/// Here `harvest_pool` is a deliberately UNCONNECTABLE pool and the sharded pool
/// is the real env database (where the orphan is seeded). If the gate wrongly
/// queried `harvest_pool` the report would be `Unavailable` (connection failure)
/// → `Warn` (crash-loop safety), never seeing the orphan. Because the shared
/// resolver picks the sharded pool, the report is `Complete` with the orphan
/// present → `Abort`. The decision being `Abort` can ONLY happen if the resolver
/// read the sharded (env) DB, so it pins the fix behaviorally.
#[tokio::test]
async fn gate_resolves_sharded_pool_not_config_pool() {
    let (database_url, _container) = setup_db_env_or_container().await;
    let orphan_type = "gate_resolves_sharded_pool_orphan";
    insert_execution(
        &database_url,
        ShardId::new(0),
        orphan_type,
        &format!("grsp-{}", uuid::Uuid::new_v4()),
        "RUNNING",
    )
    .await;

    // harvest_pool -> an unconnectable database; the sharded pool -> the real
    // env DB. This mirrors a `HarvestBuilder::with_sharded_pool` deployment
    // whose sharded pool points at a different database than the plugin's
    // resolved `harvest_pool`.
    let harvest_pool = build_test_pool("postgres://unused@127.0.0.1:1/none");
    let sharded_env = ShardedDbPool::single(build_test_pool(&database_url));

    // Resolve exactly as the gate does for the with_sharded_pool case (the
    // runner-level override is None — the plugin never sets it).
    let resolved = resolve_runtime_storage_pool(None, Some(sharded_env), harvest_pool);
    let gate_pool = resolved.clone_inner();

    let registered: BTreeSet<String> = BTreeSet::new();
    let report = build_reachability_report_single_shard(
        &registered,
        &gate_pool,
        Some(orphan_type.to_string()),
    )
    .await;

    assert!(
        report.orphaned,
        "the gate must read the sharded pool's DB, where the orphan lives",
    );
    assert_eq!(
        startup_orphan_decision(OrphanStartupAction::Fail, report.orphaned, report.status),
        StartupOrphanDecision::Abort,
        "querying harvest_pool instead would be Unavailable -> Warn, not Abort",
    );
}
