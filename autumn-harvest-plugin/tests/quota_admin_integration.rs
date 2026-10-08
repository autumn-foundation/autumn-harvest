#![allow(
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::await_holding_lock,
    clippy::items_after_statements
)]
//! Integration tests for the per-tenant resource quota HTTP surfaces (issue
//! #946): `GET /admin/quotas` (AC5) and the `429` admission mapping (AC4) on
//! `POST /workflows/{name}/start`.
//!
//! Direct-primitive coverage of the enforcement decision itself (active
//! executions / history bytes / dead letters caps, per-key and
//! per-workflow-type isolation, no-policy no-op, continue-as-new key
//! propagation) already lives in
//! `autumn-harvest/tests/integration/quota_enforcement_tests.rs`. This file
//! covers what that suite explicitly does not: the HTTP route wiring, the
//! cross-shard merge shape (sum + per-shard breakdown), partial-shard
//! degradation, admin-gating, and the exact `429` JSON body + audit trail an
//! HTTP caller observes.
//!
//! Dual-mode: `HARVEST_TEST_DATABASE_URL` (a locally migrated cluster, no
//! Docker) or a fresh testcontainers Postgres, mirroring
//! `conflict_policy_integration.rs`/`dlq_aggregate_integration.rs`.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::quota::QuotaPolicy;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowInfo, context::WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// Serializes the tests below that rely on `GLOBAL_WORKFLOW_METADATA`
/// (populated as a side effect of `HandlerRegistry::new`, read internally by
/// `start_or_load_workflow_execution_collect` to resolve the declared
/// [`QuotaPolicy`] for admission) actually carrying THIS test's own
/// declaration when its HTTP call runs. CI runs `linux` integration suites
/// with `--test-threads=1` (`.github/ci/integration-suites.txt`), but this
/// guards a plain local `cargo test` too, mirroring
/// `quota_enforcement_tests.rs`'s `TEST_SERIAL` and
/// `admission_gate_authoritative_localpg.rs`'s identical pattern. The
/// `GET /admin/quotas` read-path tests do not need it -- they read directly
/// from already-committed rows and the per-app `HandlerRegistry` the route
/// itself installs, never `GLOBAL_WORKFLOW_METADATA`.
static TEST_SERIAL: Mutex<()> = Mutex::new(());

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

// ---------------------------------------------------------------------------
// DB / shard setup
// ---------------------------------------------------------------------------

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
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

/// Two genuinely separate shard databases -- the `dlq_aggregate_integration.rs`
/// precedent -- so the cross-shard merge tests exercise real independent
/// pools, not two logical shards on one DB. Dual-mode like [`setup_db`]: when
/// `HARVEST_TEST_DATABASE_URL` is set, its server hosts two freshly created
/// (never dropped -- matching every other local-Postgres fixture in this
/// repo, which likewise leaves its scratch rows/DBs behind for the operator's
/// own cluster) shard databases instead of spinning up a container.
async fn setup_sharded_db() -> ((String, String), Option<ContainerAsync<Postgres>>) {
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let prefix = base_url
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .expect("HARVEST_TEST_DATABASE_URL must be a postgres:// URL with a database name");
        let (shard0_url, shard1_url) = create_and_migrate_shard_pair(&base_url, &prefix).await;
        return ((shard0_url, shard1_url), None);
    }

    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let prefix = format!("postgres://postgres:postgres@{host}:{port}");
    let (shard0_url, shard1_url) = create_and_migrate_shard_pair(&admin_url, &prefix).await;
    ((shard0_url, shard1_url), Some(container))
}

/// Connects to `admin_url` to create two fresh, uniquely named
/// `harvest_shard_*` databases on the same server (`url_prefix` is everything
/// up to -- and not including -- the trailing `/dbname`), migrates each, and
/// returns their full connection URLs.
async fn create_and_migrate_shard_pair(admin_url: &str, url_prefix: &str) -> (String, String) {
    let shard0_db = format!("harvest_shard_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_shard_{}", uuid::Uuid::new_v4().simple());

    let mut admin_conn = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {shard0_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("create shard 0 database");
    diesel::sql_query(format!("CREATE DATABASE {shard1_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("create shard 1 database");

    let shard0_url = format!("{url_prefix}/{shard0_db}");
    let shard1_url = format!("{url_prefix}/{shard1_db}");
    for url in [&shard0_url, &shard1_url] {
        let mut conn = AsyncPgConnection::establish(url)
            .await
            .expect("connect to shard database");
        diesel_async::SimpleAsyncConnection::batch_execute(
            &mut conn,
            autumn_harvest::full_migrations_sql(),
        )
        .await
        .expect("apply migrations to shard database");
    }
    (shard0_url, shard1_url)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

// ---------------------------------------------------------------------------
// WorkflowInfo / app builders
// ---------------------------------------------------------------------------

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn plain_info(name: &'static str) -> WorkflowInfo {
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

fn quota_info(name: &'static str, policy: QuotaPolicy) -> WorkflowInfo {
    let mut info = plain_info(name);
    info.quota = Some(policy);
    info
}

fn build_app_inner(pool: HarvestDbPool, infos: Vec<WorkflowInfo>, admin: bool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    if admin {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(pool);
    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("quota-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn build_app(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    build_app_inner(HarvestDbPool::from(pool.clone()), infos, true)
}

fn build_app_no_admin(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    build_app_inner(HarvestDbPool::from(pool.clone()), infos, false)
}

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(shard0_url));
    pools.insert(ShardId::new(1), build_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn build_sharded_app(
    shard0_url: &str,
    shard1_url: &str,
    infos: Vec<WorkflowInfo>,
) -> HarvestApiApp {
    build_app_inner(build_two_shard_pool(shard0_url, shard1_url), infos, true)
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

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
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

async fn post_start(app: &HarvestApiApp, wf: &str, body: Value) -> (StatusCode, Value) {
    post_json(app, &format!("/workflows/{wf}/start"), body).await
}

/// Generic `POST {uri}` JSON helper -- used by [`post_start`] and by the
/// `signal-with-start`/`update-with-start` 429-body-shape tests below (issue
/// #946, the P2 test-coverage gap: both routes carry their own bespoke
/// `HarvestError::QuotaExceeded` arm in `api.rs`, byte-identical to the plain
/// start route's, and neither had HTTP-level coverage).
async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.expect("POST request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

// ---------------------------------------------------------------------------
// Direct row-insert seeding -- `GET /admin/quotas` is a pure report over
// already-committed state, so its tests do not need to drive the admission
// pipeline at all.
// ---------------------------------------------------------------------------

async fn insert_running_execution(
    database_url: &str,
    shard: i32,
    workflow_name: &str,
    quota_key: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(shard));
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect for execution insert");
    let workflow_id = format!("{workflow_name}-{}", uuid::Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: shard,
        input: json!({ "tenant_id": quota_key }),
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
        quota_key: Some(quota_key),
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert execution row (defaults to RUNNING)");
    exec_id
}

async fn insert_dead_letters(database_url: &str, exec_id: ExecutionId, count: usize) {
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect for dead-letter insert");
    for _ in 0..count {
        autumn_harvest::dlq::dead_letter(
            &mut conn,
            &autumn_harvest::dlq::NewDeadLetterEntry {
                original_task_id: uuid::Uuid::new_v4(),
                queue_name: "default".to_string(),
                task_type: "ACTIVITY".to_string(),
                workflow_exec_id: Some(exec_id.as_uuid()),
                activity_name: Some("charge_card".to_string()),
                input: json!({}),
                error: "boom".to_string(),
                attempts: 3,
                owner: None,
                severity: None,
            },
        )
        .await
        .expect("dead-letter insert");
    }
}

async fn quotas_array(app: &HarvestApiApp) -> Vec<Value> {
    let (status, body) = get_json(app, "/admin/quotas").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    body.get("quotas")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn find_entry<'a>(quotas: &'a [Value], workflow_name: &str, quota_key: &str) -> Option<&'a Value> {
    quotas.iter().find(|q| {
        q.get("workflow_name").and_then(Value::as_str) == Some(workflow_name)
            && q.get("quota_key").and_then(Value::as_str) == Some(quota_key)
    })
}

// ---------------------------------------------------------------------------
// Section A -- GET /admin/quotas (issue #946 AC5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn admin_quotas_reports_usage_vs_declared_limit() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_report_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    for _ in 0..3 {
        insert_running_execution(&url, 0, wf, "acme").await;
    }
    let app = build_app(&pool, vec![quota_info(wf, policy)]);

    let quotas = quotas_array(&app).await;
    let entry = find_entry(&quotas, wf, "acme").expect("entry for (wf, acme)");
    assert_eq!(entry["usage"]["active_executions"], json!(3));
    assert_eq!(entry["usage"]["history_bytes"], json!(0));
    assert_eq!(entry["usage"]["dead_letters"], json!(0));
    assert_eq!(entry["limits"]["max_active_executions"], json!(10));
    assert_eq!(entry["limits"]["max_history_bytes"], Value::Null);
    assert_eq!(entry["limits"]["max_dead_letters"], Value::Null);
    let shards = entry["shards"].as_array().expect("shards array");
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0]["shard_id"], json!(0));
    assert_eq!(shards[0]["active_executions"], json!(3));
}

#[tokio::test]
async fn admin_quotas_reports_dead_letters_count() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_dlq_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_dead_letters(50);
    let exec = insert_running_execution(&url, 0, wf, "acme").await;
    insert_dead_letters(&url, exec, 4).await;
    let app = build_app(&pool, vec![quota_info(wf, policy)]);

    let quotas = quotas_array(&app).await;
    let entry = find_entry(&quotas, wf, "acme").expect("entry for (wf, acme)");
    assert_eq!(entry["usage"]["dead_letters"], json!(4));
    assert_eq!(entry["limits"]["max_dead_letters"], json!(50));
}

#[tokio::test]
async fn admin_quotas_isolates_by_key_within_one_workflow_type() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_isolation_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    for _ in 0..2 {
        insert_running_execution(&url, 0, wf, "acme").await;
    }
    for _ in 0..5 {
        insert_running_execution(&url, 0, wf, "globex").await;
    }
    let app = build_app(&pool, vec![quota_info(wf, policy)]);

    let quotas = quotas_array(&app).await;
    let acme = find_entry(&quotas, wf, "acme").expect("entry for acme");
    let globex = find_entry(&quotas, wf, "globex").expect("entry for globex");
    assert_eq!(acme["usage"]["active_executions"], json!(2));
    assert_eq!(globex["usage"]["active_executions"], json!(5));
}

/// The AC5 money test: a `(workflow_name, quota_key)` pair with usage spread
/// across two genuinely independent shard databases must SUM for the
/// fleet-wide total while still listing each shard's own count -- quota
/// enforcement is shard-local (AC8), so collapsing to a single number without
/// the per-shard breakdown would hide which shard(s) a tenant is capped on.
#[tokio::test]
async fn admin_quotas_sums_across_shards_and_lists_per_shard_breakdown() {
    let ((shard0_url, shard1_url), _c) = setup_sharded_db().await;
    let wf = "quota_shard_sum_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(100);
    for _ in 0..4 {
        insert_running_execution(&shard0_url, 0, wf, "acme").await;
    }
    for _ in 0..7 {
        insert_running_execution(&shard1_url, 1, wf, "acme").await;
    }
    let app = build_sharded_app(&shard0_url, &shard1_url, vec![quota_info(wf, policy)]);

    let quotas = quotas_array(&app).await;
    let entry = find_entry(&quotas, wf, "acme").expect("entry for acme");
    assert_eq!(
        entry["usage"]["active_executions"],
        json!(11),
        "must SUM across shards, not report a single shard's count"
    );
    let shards = entry["shards"].as_array().expect("shards array");
    assert_eq!(
        shards.len(),
        2,
        "must list BOTH shards' breakdowns, not collapse to a single count"
    );
    let by_shard: BTreeMap<i64, i64> = shards
        .iter()
        .map(|s| {
            (
                s["shard_id"].as_i64().unwrap(),
                s["active_executions"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(by_shard.get(&0), Some(&4));
    assert_eq!(by_shard.get(&1), Some(&7));
}

/// An unreachable shard must degrade the response to `partial` (naming the
/// down shard) rather than failing the whole read with a `500`, mirroring the
/// `/admin/concurrency`, `/admin/debounce`, and `/admin/start-throttle`
/// precedent this route is built on.
#[tokio::test]
async fn admin_quotas_reports_partial_status_when_a_shard_is_unreachable() {
    let ((shard0_url, shard1_url), _c) = setup_sharded_db().await;
    let wf = "quota_partial_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(100);
    insert_running_execution(&shard0_url, 0, wf, "acme").await;
    insert_running_execution(&shard1_url, 1, wf, "acme").await;

    // Retarget shard 1's pool at a database that does not exist, so its
    // connection acquire fails deterministically at connect time -- the
    // `fanout_degradation_integration.rs` precedent, avoiding the flakiness
    // of tearing down a live container mid-test.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&shard0_url));
    let dead_shard1_url = shard1_url.replace("harvest_shard_", "missing_db_");
    pools.insert(ShardId::new(1), build_pool(&dead_shard1_url));
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let app = build_app_inner(storage, vec![quota_info(wf, policy)], true);

    let (status, body) = get_json(&app, "/admin/quotas").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an unreachable shard must never surface as a hard 500: {body}"
    );
    assert_eq!(body["status"], json!("partial"));
    let unavailable = body["unavailable_shards"]
        .as_array()
        .expect("unavailable_shards array");
    assert!(!unavailable.is_empty(), "must name the unreachable shard");
    // Shard 0's data must still be present despite shard 1 being down.
    let quotas = body["quotas"].as_array().expect("quotas array");
    let entry = find_entry(quotas, wf, "acme").expect("entry for acme still present");
    assert_eq!(entry["usage"]["active_executions"], json!(1));
}

#[tokio::test]
async fn admin_quotas_requires_admin_auth() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_no_admin_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
    let app = build_app_no_admin(&pool, vec![quota_info(wf, policy)]);

    let (status, _body) = get_json(&app, "/admin/quotas").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// Section B -- 429 admission mapping (issue #946 AC4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plain_start_returns_429_with_structured_body_when_quota_exceeded() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_429_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(2);
    // Seed exactly `limit` active rows already at the cap.
    insert_running_execution(&url, 0, wf, "acme").await;
    insert_running_execution(&url, 0, wf, "acme").await;
    let app = build_app(&pool, vec![quota_info(wf, policy)]);

    let (status, body) = post_start(
        &app,
        wf,
        json!({"workflow_id": "over-cap-1", "input": {"tenant_id": "acme"}}),
    )
    .await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "body: {body}");
    assert_eq!(body["error"], json!("quota exceeded"));
    assert_eq!(body["workflow_name"], json!(wf));
    assert_eq!(body["key"], json!("acme"));
    assert_eq!(body["resource"], json!("active_executions"));
    assert_eq!(body["limit"], json!(2));
    assert_eq!(
        body["current"],
        json!(2),
        "current must report usage BEFORE this admission -- exactly at the cap, not one over"
    );

    // No third row was admitted.
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND quota_key = $2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>("acme")
    .get_result(&mut conn)
    .await
    .expect("count rows");
    assert_eq!(
        row.n, 2,
        "a rejected admission must not create a phantom row"
    );
}

#[tokio::test]
async fn plain_start_writes_a_failed_audit_row_when_quota_exceeded() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_audit_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    insert_running_execution(&url, 0, wf, "acme").await;
    let app = build_app(&pool, vec![quota_info(wf, policy)]);

    let (status, _body) = post_start(
        &app,
        wf,
        json!({"workflow_id": "audit-1", "input": {"tenant_id": "acme"}}),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    #[derive(diesel::QueryableByName)]
    struct AuditRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        error_summary: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        status: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Int4>)]
        shard_id: Option<i32>,
    }
    let rows: Vec<AuditRow> = diesel::sql_query(
        "SELECT error_summary, status, shard_id FROM harvest_audit_log \
         WHERE target_id = $1 ORDER BY occurred_at DESC LIMIT 1",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .load(&mut conn)
    .await
    .expect("load audit rows");
    let row = rows
        .into_iter()
        .next()
        .expect("a quota-rejected start must be audited");
    assert_eq!(row.status, "failed");
    assert_eq!(row.error_summary, "quota exceeded");
    // Regression guard for the Codex round-5 finding on issue #946: routing
    // has already resolved the target shard by the time the quota check
    // rejects the start, so the audit row must record it (matching every
    // other failure arm on this route, and the signal-with-start /
    // update-with-start quota arms below) -- a quota-rejection audit row
    // with a NULL shard is otherwise indistinguishable by shard in the
    // audit trail precisely where enforcement is shard-local.
    assert_eq!(
        row.shard_id,
        Some(0),
        "quota-rejection audit row must record the resolved shard, not NULL"
    );
}

#[tokio::test]
async fn plain_start_succeeds_when_under_quota() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_under_cap_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(2);
    insert_running_execution(&url, 0, wf, "acme").await;
    let app = build_app(&pool, vec![quota_info(wf, policy)]);

    let (status, body) = post_start(
        &app,
        wf,
        json!({"workflow_id": "under-cap-1", "input": {"tenant_id": "acme"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
}

#[tokio::test]
async fn plain_start_is_unaffected_when_workflow_has_no_quota_policy() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_no_policy_wf";
    // AC9: a workflow that declares no QuotaPolicy at all is byte-for-byte
    // unaffected, no matter how much usage exists under the same tenant key.
    for _ in 0..50 {
        insert_running_execution(&url, 0, wf, "acme").await;
    }
    let app = build_app(&pool, vec![plain_info(wf)]);

    let (status, body) = post_start(
        &app,
        wf,
        json!({"workflow_id": "no-policy-1", "input": {"tenant_id": "acme"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
}

// ---------------------------------------------------------------------------
// Section C -- 429 admission mapping on the two other registry-aware fresh-
// start entry points (issue #946 AC3/AC4): `signal-with-start` (#244) and
// `update-with-start` (#479). Both carry their own bespoke
// `HarvestError::QuotaExceeded` arm in `api.rs` -- copy-pasted from, and
// byte-identical in shape to, the plain-start route's arm exercised in
// Section B above -- but neither had HTTP-level coverage of that arm (a P2
// test-coverage gap flagged by an earlier review agent). Neither route needs
// a registered signal/update handler for this: the quota check runs inside
// the shared core admission primitive before any signal/update-specific
// validation is even reached for a name with no matching handler.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn signal_with_start_returns_429_with_structured_body_when_quota_exceeded() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_sws_429_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    insert_running_execution(&url, 0, wf, "acme").await;
    let app = build_app(&pool, vec![quota_info(wf, policy)]);

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{wf}/signal-with-start"),
        json!({
            "workflow_id": "sws-over-cap-1",
            "start_input": {"tenant_id": "acme"},
            "signal_name": "approve",
        }),
    )
    .await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "body: {body}");
    assert_eq!(body["error"], json!("quota exceeded"));
    assert_eq!(body["workflow_name"], json!(wf));
    assert_eq!(body["key"], json!("acme"));
    assert_eq!(body["resource"], json!("active_executions"));
    assert_eq!(body["limit"], json!(1));
    assert_eq!(body["current"], json!(1));

    // No fresh execution was admitted, and no signal was staged for the
    // (never-created) target -- a rejected fresh admission must leave zero
    // trace, exactly like the plain-start route.
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND quota_key = $2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>("acme")
    .get_result(&mut conn)
    .await
    .expect("count rows");
    assert_eq!(
        row.n, 1,
        "a rejected fresh signal-with-start must not create a phantom execution row"
    );
}

#[tokio::test]
async fn update_with_start_returns_429_with_structured_body_when_quota_exceeded() {
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let wf = "quota_uws_429_wf";
    let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(1);
    insert_running_execution(&url, 0, wf, "acme").await;
    let app = build_app(&pool, vec![quota_info(wf, policy)]);

    let (status, body) = post_json(
        &app,
        &format!("/workflows/{wf}/update-with-start"),
        json!({
            "workflow_id": "uws-over-cap-1",
            "start_input": {"tenant_id": "acme"},
            "update_name": "bump_priority",
            "update_args": {},
        }),
    )
    .await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "body: {body}");
    assert_eq!(body["error"], json!("quota exceeded"));
    assert_eq!(body["workflow_name"], json!(wf));
    assert_eq!(body["key"], json!("acme"));
    assert_eq!(body["resource"], json!("active_executions"));
    assert_eq!(body["limit"], json!(1));
    assert_eq!(body["current"], json!(1));

    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND quota_key = $2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>("acme")
    .get_result(&mut conn)
    .await
    .expect("count rows");
    assert_eq!(
        row.n, 1,
        "a rejected fresh update-with-start must not create a phantom execution row"
    );
}
