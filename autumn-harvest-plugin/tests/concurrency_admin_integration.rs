//! `GET /admin/concurrency` workflow attribution — issue #811 AC7 (second half).
//!
//! The counter half of AC7 is covered by unit tests; this covers the **admin
//! read**, which had no test at all: the handler's cross-shard merge, the
//! `(key, task_type)` grouping, the per-key `workflows` attribution, its
//! dedup + sort, and the registry lookup whose `map_or_else` fallback must
//! report `defer` for a workflow that declares no policy (or is not
//! registered at all).
//!
//! Runs against a real Postgres 16 container (or `HARVEST_TEST_DATABASE_URL`).

#![allow(clippy::default_trait_access)]

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::concurrency::{ConcurrencyOnConflict, ConcurrencyPolicy};
use autumn_harvest::execution::start_or_load_workflow_execution;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{
    ExecutionId, ShardId, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{StartWorkflowParams, WorkflowInfo, context::WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn info(name: &'static str, policy: Option<ConcurrencyPolicy>) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: dummy_workflow,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: policy,
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

fn build_app(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("concurrency-admin-test".to_string()),
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
        .expect("request should not fail");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// Start a run through the real primitive so BOTH the execution row and the
/// task row (which is where the resolved concurrency key actually lives) are
/// created exactly as production creates them.
async fn seed_run(pool: &DbPool, wf: &str, wf_id: &str, key: &str, limit: u32) {
    let mut conn = pool.get().await.expect("pool conn");
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: wf,
            workflow_id: wf_id,
            exec_id,
            input: json!({ "tenant_id": key }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: Some(key.to_string()),
            concurrency_limit: Some(limit),
            concurrency_on_conflict: ConcurrencyOnConflict::Defer,
            priority: Default::default(),
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
        },
        None,
    )
    .await
    .expect("seed start should succeed");
}

/// Two registered workflow types share one concurrency key -- one declaring
/// `cancel_running`, one declaring no policy at all -- plus a third type that
/// is not in the registry. The admin read must group them under one key,
/// attribute each by name, sort the `workflows` array, and resolve `defer`
/// for both the policy-less and the unregistered type.
#[tokio::test]
async fn admin_concurrency_attributes_each_workflow_and_its_effective_strategy() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);

    let latest_wins = info(
        "doc_index",
        Some(
            ConcurrencyPolicy::new("input.tenant_id", 1)
                .with_on_conflict(ConcurrencyOnConflict::CancelRunning),
        ),
    );
    // Registered, but declares NO concurrency policy: the handler's
    // `map_or_else(default)` fallback must report `defer`, matching what the
    // start path would resolve for it.
    let no_policy = info("report_build", None);
    let app = build_app(&pool, vec![latest_wins, no_policy]);

    seed_run(&pool, "doc_index", "admin-a", "acme", 1).await;
    seed_run(&pool, "report_build", "admin-b", "acme", 1).await;
    // Never registered at all -- must also fall back to `defer`, never panic
    // and never be omitted from the attribution.
    seed_run(&pool, "ghost_wf", "admin-c", "acme", 1).await;

    let (status, body) = get_json(&app, "/admin/concurrency").await;
    assert_eq!(status, StatusCode::OK, "admin read: {body:?}");

    let rows = body.as_array().expect("array response");
    let row = rows
        .iter()
        .find(|r| r["key"] == json!("acme") && r["task_type"] == json!("workflow"))
        .unwrap_or_else(|| panic!("no (acme, workflow) row in {rows:?}"));

    assert_eq!(row["max_concurrent"], json!(1));
    assert_eq!(
        row["pending"],
        json!(3),
        "all three seeded workflow tasks are PENDING on the key"
    );

    let workflows = row["workflows"].as_array().expect("workflows array");
    assert_eq!(
        workflows.len(),
        3,
        "each workflow type on the key is attributed exactly once: {workflows:?}"
    );
    // Sorted by workflow_name -- deterministic output for an operator diff.
    let names: Vec<&str> = workflows
        .iter()
        .map(|w| w["workflow_name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, vec!["doc_index", "ghost_wf", "report_build"]);

    let strategy = |name: &str| -> String {
        workflows
            .iter()
            .find(|w| w["workflow_name"] == json!(name))
            .and_then(|w| w["on_conflict"].as_str())
            .unwrap_or_else(|| panic!("{name} missing on_conflict"))
            .to_string()
    };
    assert_eq!(
        strategy("doc_index"),
        "cancel_running",
        "the declared latest-wins strategy is surfaced (AC7)"
    );
    assert_eq!(
        strategy("report_build"),
        "defer",
        "a registered workflow with no policy reports the default"
    );
    assert_eq!(
        strategy("ghost_wf"),
        "defer",
        "an unregistered workflow reports the default rather than being dropped"
    );
}

/// A key with two runs of the SAME workflow type is attributed once, not
/// twice: the handler dedups by `workflow_name` within a `(key, task_type)`
/// group.
#[tokio::test]
async fn admin_concurrency_dedups_repeated_workflow_names_on_one_key() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(
        &pool,
        vec![info(
            "doc_index",
            Some(
                ConcurrencyPolicy::new("input.tenant_id", 4)
                    .with_on_conflict(ConcurrencyOnConflict::CancelRunning),
            ),
        )],
    );

    for i in 0..3 {
        seed_run(&pool, "doc_index", &format!("dedup-{i}"), "dedup-key", 4).await;
    }

    let (status, body) = get_json(&app, "/admin/concurrency").await;
    assert_eq!(status, StatusCode::OK, "admin read: {body:?}");
    let rows = body.as_array().expect("array response");
    let row = rows
        .iter()
        .find(|r| r["key"] == json!("dedup-key"))
        .unwrap_or_else(|| panic!("no dedup-key row in {rows:?}"));

    assert_eq!(row["pending"], json!(3), "counters still count every task");
    let workflows = row["workflows"].as_array().expect("workflows array");
    assert_eq!(
        workflows.len(),
        1,
        "one attribution entry per workflow TYPE, not per task: {workflows:?}"
    );
    assert_eq!(workflows[0]["workflow_name"], json!("doc_index"));
    assert_eq!(workflows[0]["on_conflict"], json!("cancel_running"));
}

/// An **activity** concurrency group always reports `defer`, even when its
/// owning workflow declares `cancel_running` (issue #811, Codex round 1).
///
/// Latest-wins is a workflow-*start* admission control: the supersede runs
/// inside the start primitive and sheds workflow *executions*. An activity task
/// that carries a concurrency key is gated purely at claim time (issue #247)
/// and always defers — nothing sheds an in-flight activity for a newcomer. So
/// resolving the owning workflow's declared strategy onto the
/// `task_type = "activity"` row would advertise behaviour the engine does not
/// enforce for that group.
#[tokio::test]
async fn admin_concurrency_reports_defer_for_activity_rows() {
    use diesel::sql_types::{Int4, Text, Uuid as SqlUuid};
    use diesel_async::RunQueryDsl;

    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);

    // The owning workflow DECLARES cancel_running.
    let latest_wins = info(
        "doc_index",
        Some(
            ConcurrencyPolicy::new("input.tenant_id", 1)
                .with_on_conflict(ConcurrencyOnConflict::CancelRunning),
        ),
    );
    let app = build_app(&pool, vec![latest_wins]);

    // A workflow task on the key (reports cancel_running), plus an ACTIVITY
    // task on the SAME key owned by the SAME cancel_running workflow.
    seed_run(&pool, "doc_index", "act-a", "acme-act", 1).await;

    let mut conn = pool.get().await.expect("pool conn");
    let owner: uuid::Uuid = diesel::sql_query(
        "SELECT id FROM harvest_workflow_executions WHERE workflow_name = 'doc_index' \
         AND workflow_id = 'act-a' LIMIT 1",
    )
    .get_result::<OwnerRow>(&mut conn)
    .await
    .expect("owner execution")
    .id;

    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, activity_name, input, state, \
          priority, attempt, max_attempts, scheduled_at, concurrency_key, concurrency_cap) \
         VALUES (gen_random_uuid(), 'default', 'activity', $1, 'do_index', '{}'::jsonb, \
                 'PENDING', 0, 0, 3, NOW(), $2, $3)",
    )
    .bind::<SqlUuid, _>(owner)
    .bind::<Text, _>("acme-act")
    .bind::<Int4, _>(1)
    .execute(&mut conn)
    .await
    .expect("seed activity task");
    drop(conn);

    let (status, body) = get_json(&app, "/admin/concurrency").await;
    assert_eq!(status, StatusCode::OK, "admin read: {body:?}");
    let rows = body.as_array().expect("array response");

    // The workflow row still reports the declared strategy.
    let wf_row = rows
        .iter()
        .find(|r| r["key"] == json!("acme-act") && r["task_type"] == json!("workflow"))
        .unwrap_or_else(|| panic!("no (acme-act, workflow) row in {rows:?}"));
    assert_eq!(
        wf_row["workflows"][0]["on_conflict"],
        json!("cancel_running"),
        "the workflow row must still report the declared strategy"
    );

    // The activity row must report defer -- that is what is enforced.
    let act_row = rows
        .iter()
        .find(|r| r["key"] == json!("acme-act") && r["task_type"] == json!("activity"))
        .unwrap_or_else(|| panic!("no (acme-act, activity) row in {rows:?}"));
    assert_eq!(
        act_row["workflows"][0]["workflow_name"],
        json!("doc_index"),
        "the activity row is still attributed to its owning workflow type"
    );
    assert_eq!(
        act_row["workflows"][0]["on_conflict"],
        json!("defer"),
        "an activity concurrency group cannot latest-wins, so the admin read must \
         not advertise the owning workflow's cancel_running for it"
    );
}

#[derive(diesel::QueryableByName)]
struct OwnerRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}
