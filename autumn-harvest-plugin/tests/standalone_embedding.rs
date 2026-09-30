//! `HarvestEmbedding` runs the whole standalone startup sequence (issue #1613).
//!
//! Each test names the responsibility from the issue table that it pins. The
//! embedder used to do each one by hand, or not at all.
//!
//! Dual-mode database. `HARVEST_TEST_DATABASE_URL` gives each test a fresh
//! database on that server. Otherwise each test starts a Postgres 16
//! container. The tests share process-global admission state, so they run
//! one at a time.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::WorkflowContext;
use autumn_harvest::WorkflowEvent;
use autumn_harvest::admission_gate::{
    GateScope, global_admission_gate_cache, global_admission_metrics,
};
use autumn_harvest::batch_start::BatchStartConfig;
use autumn_harvest::builder::{HarvestBuilder, WorkerConfig};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::api::StandaloneAdminAuth;
use autumn_harvest_plugin::config::{
    HarvestDatabaseConfig, HarvestMode, HarvestRuntimeConfig, HarvestStartupConfig,
    OrphanStartupAction,
};
use autumn_harvest_plugin::embedding::HarvestEmbedding;
use autumn_harvest_plugin::runner::HarvestRunnerResources;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn echo_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

fn workflow_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "standalone_embedding",
        handler: echo_workflow,
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

fn builder() -> HarvestBuilder {
    HarvestBuilder::new().workflows(vec![workflow_info("embed_echo")])
}

fn config(database_url: &str) -> HarvestRuntimeConfig {
    HarvestRuntimeConfig {
        mode: HarvestMode::External,
        worker_enabled: true,
        scheduler_enabled: false,
        database: HarvestDatabaseConfig {
            url: Some(database_url.to_owned()),
        },
        ..HarvestRuntimeConfig::default()
    }
}

fn pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(6)
        .build()
        .expect("pool should build")
}

/// Keeps a container alive for the test. `None` on the shared-server path.
type DbGuard = Option<ContainerAsync<Postgres>>;

async fn database() -> (String, DbGuard) {
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (fresh_database(&base_url).await, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("migrations should apply");
    (url, Some(container))
}

/// A fresh, migrated database on the server `base_url` names.
async fn fresh_database(base_url: &str) -> String {
    let name = format!("harvest_embed_{}", uuid::Uuid::new_v4().simple());
    let mut admin = AsyncPgConnection::establish(base_url)
        .await
        .expect("connect to base database");
    diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await
        .expect("create database");
    let (prefix, _) = base_url.rsplit_once('/').expect("url has a database");
    let url = format!("{prefix}/{name}");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("migrations should apply");
    url
}

/// Seed a non-terminal run of a workflow type that no handler registers.
async fn seed_orphan(database_url: &str, workflow_name: &str) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect to seed");
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: "orphan-1",
        run_id: uuid::Uuid::new_v4(),
        shard_id: 0,
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
        .expect("insert execution");
    let events = vec![WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("append start event");
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let request = match body {
        Some(json) => request
            .header("content-type", "application/json")
            .body(Body::from(json.to_string())),
        None => request.body(Body::empty()),
    }
    .expect("request should build");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("router should serve");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

// ---------------------------------------------------------------------------
// Admission-gate cache
// ---------------------------------------------------------------------------

/// A gate a previous process persisted is live before `start` returns. The
/// refresh loop first ticks after one second, so only the boot load can
/// explain the gate here.
#[tokio::test]
async fn boot_loads_gates_a_previous_process_persisted() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to seed a gate");
    autumn_harvest::admission_gate::db::create_gate(
        &mut conn,
        &GateScope::Fleet,
        "incident-42",
        None,
        "previous-process",
        None,
    )
    .await
    .expect("create gate");

    let runtime = HarvestEmbedding::new(builder().build(), config(&url), HarvestRunnerResources::new(pool(&url)))
        .with_admin_auth(StandaloneAdminAuth::new().with_deployment_profile("dev"))
        .start()
        .await
        .expect("embedding should start");

    assert_eq!(runtime.api_state().gate_cache().active_count(), 1);
    let published = global_admission_gate_cache().expect("gate cache must be published");
    assert!(Arc::ptr_eq(&published, &runtime.api_state().gate_cache()));

    let (status, _) = send(
        &runtime.router(),
        "POST",
        "/workflows/embed_echo/start",
        None,
        Some(json!({ "workflow_id": "gated-1", "input": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    runtime.stop().await;
}

// ---------------------------------------------------------------------------
// Orphaned-workflow startup gate
// ---------------------------------------------------------------------------

/// A code-set `fail` stays `fail`. Nothing in the operator config names the
/// setting, so the overlay must keep the code value. The abort also runs
/// before any admission global is published.
#[tokio::test]
async fn code_set_fail_refuses_boot_and_publishes_nothing() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    seed_orphan(&url, "embed_removed_type").await;
    let before = global_admission_gate_cache();

    let mut config = config(&url);
    config.startup = HarvestStartupConfig {
        orphaned_workflows: OrphanStartupAction::Fail,
    };
    let result = HarvestEmbedding::new(builder().build(), config, HarvestRunnerResources::new(pool(&url)))
        .start()
        .await;

    let Err(error) = result else {
        panic!("an orphan under `fail` must refuse boot");
    };
    assert!(error.to_string().contains("embed_removed_type"), "{error}");
    let after = global_admission_gate_cache();
    assert_eq!(
        before.map(|cache| Arc::as_ptr(&cache)),
        after.map(|cache| Arc::as_ptr(&cache)),
        "an aborted boot must not publish an admission gate cache"
    );
}

// ---------------------------------------------------------------------------
// Posture: deployment profile, auth boundary, credential layers
// ---------------------------------------------------------------------------

/// The `dev` profile opens the admin API, and Vantage is mounted.
#[tokio::test]
async fn dev_profile_opens_preflight_and_mounts_vantage() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    let runtime = HarvestEmbedding::new(builder().build(), config(&url), HarvestRunnerResources::new(pool(&url)))
        .with_admin_auth(StandaloneAdminAuth::new().with_deployment_profile("dev"))
        .start()
        .await
        .expect("embedding should start");

    let (status, _) = send(&runtime.router(), "GET", "/admin/preflight", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&runtime.router(), "GET", "/ui/", None, None).await;
    assert_eq!(status, StatusCode::OK);

    runtime.stop().await;
}

/// A non-dev profile with no credential stays fail-closed.
#[tokio::test]
async fn non_dev_profile_rejects_an_anonymous_admin_call() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    let runtime = HarvestEmbedding::new(builder().build(), config(&url), HarvestRunnerResources::new(pool(&url)))
        .with_admin_auth(StandaloneAdminAuth::new().with_deployment_profile("prod"))
        .start()
        .await
        .expect("embedding should start");

    let (status, _) = send(&runtime.router(), "GET", "/admin/preflight", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    runtime.stop().await;
}

/// The token layer is installed and reads the installed storage pool. A token
/// in the store reaches an admin route under a non-dev profile.
#[tokio::test]
async fn a_stored_api_token_reaches_an_admin_route() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    let secret = autumn_harvest::api_token::mint_secret();
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to seed a token");
    diesel::sql_query(
        "INSERT INTO harvest_api_tokens (id, name, token_hash, scope, created_by) \
         VALUES (gen_random_uuid(), 'embed', $1, 'read', 'test')",
    )
    .bind::<diesel::sql_types::Text, _>(autumn_harvest::api_token::hash_secret(&secret))
    .execute(&mut conn)
    .await
    .expect("seed token");

    let runtime = HarvestEmbedding::new(builder().build(), config(&url), HarvestRunnerResources::new(pool(&url)))
        .with_admin_auth(
            StandaloneAdminAuth::new()
                .with_api_tokens()
                .with_deployment_profile("prod"),
        )
        .start()
        .await
        .expect("embedding should start");

    let (status, _) = send(
        &runtime.router(),
        "GET",
        "/admin/preflight",
        Some(&secret),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    runtime.stop().await;
}

/// `without_ui` leaves Vantage unmounted.
#[tokio::test]
async fn without_ui_leaves_vantage_unmounted() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    let runtime = HarvestEmbedding::new(builder().build(), config(&url), HarvestRunnerResources::new(pool(&url)))
        .with_admin_auth(StandaloneAdminAuth::new().with_deployment_profile("dev"))
        .without_ui()
        .start()
        .await
        .expect("embedding should start");

    let (status, _) = send(&runtime.router(), "GET", "/ui/", None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    runtime.stop().await;
}

// ---------------------------------------------------------------------------
// Storage pool and API runtime
// ---------------------------------------------------------------------------

/// The returned router starts a workflow, the owned worker runs it, and the
/// long-poll result route answers. The result route needs the storage pool,
/// the runtime and the notification URL, so all three are installed.
#[tokio::test]
async fn the_router_starts_a_workflow_the_worker_completes() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    let runtime = HarvestEmbedding::new(builder().build(), config(&url), HarvestRunnerResources::new(pool(&url)))
        .with_admin_auth(StandaloneAdminAuth::new().with_deployment_profile("dev"))
        .start()
        .await
        .expect("embedding should start");

    let (status, started) = send(
        &runtime.router(),
        "POST",
        "/workflows/embed_echo/start",
        None,
        Some(json!({ "workflow_id": "echo-1", "input": { "n": 7 } })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let exec_id = started["execution_id"].as_str().expect("execution_id");

    let (status, result) = send(
        &runtime.router(),
        "GET",
        &format!("/workflows/{exec_id}/result?wait=20s"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["output"], json!({ "n": 7 }), "{result}");

    runtime.stop().await;
}

/// Builder limits reach the API state and the purge-window global. The
/// standalone path used to skip this mirror.
#[tokio::test]
async fn builder_limits_reach_the_api_state() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    let built = builder()
        .batch_start_config(BatchStartConfig {
            max_items_per_batch: 7,
            ..BatchStartConfig::default()
        })
        .start_idempotency_window(Duration::from_secs(4321))
        .worker(WorkerConfig::default())
        .build();

    let runtime = HarvestEmbedding::new(built, config(&url), HarvestRunnerResources::new(pool(&url)))
        .start()
        .await
        .expect("embedding should start");

    assert_eq!(runtime.api_state().batch_start_max_items(), 7);
    assert!(
        (autumn_harvest::start_idempotency::purge_window_secs() - 4321.0).abs() < f64::EPSILON
    );

    runtime.stop().await;
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

/// `stop` clears the admission globals this runtime published and empties the
/// API state, so the router answers 503 instead of reading a dead runtime.
#[tokio::test]
async fn stop_tears_down_globals_and_the_api_state() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = database().await;
    let runtime = HarvestEmbedding::new(builder().build(), config(&url), HarvestRunnerResources::new(pool(&url)))
        .with_admin_auth(StandaloneAdminAuth::new().with_deployment_profile("dev"))
        .start()
        .await
        .expect("embedding should start");
    let router = runtime.router();
    assert!(global_admission_gate_cache().is_some());
    assert!(global_admission_metrics().is_some());

    runtime.stop().await;

    assert!(global_admission_gate_cache().is_none());
    assert!(global_admission_metrics().is_none());
    let (status, _) = send(&router, "GET", "/workflows", None, None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

// ---------------------------------------------------------------------------
// Multi-shard
// ---------------------------------------------------------------------------

fn two_shard_resources(shard0: &str, shard1: &str) -> HarvestRunnerResources {
    let sharded = ShardedDbPool::from_map(
        [
            (ShardId::new(0), pool(shard0)),
            (ShardId::new(1), pool(shard1)),
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
    HarvestRunnerResources::new(pool(shard0))
        .with_shard_router(router)
        .with_sharded_pool(sharded)
}

fn api_only(database_url: &str) -> HarvestRuntimeConfig {
    HarvestRuntimeConfig {
        worker_enabled: false,
        ..config(database_url)
    }
}

/// A shard with no notification URL would break result waits for every run
/// placed on it. `start` refuses before it spawns anything.
#[tokio::test]
async fn multi_shard_needs_a_notification_url_per_shard() {
    let _serial = SERIAL.lock().await;
    let (shard0, _db) = database().await;
    let shard1 = fresh_database(&shard0).await;
    let before = global_admission_gate_cache();

    let result = HarvestEmbedding::new(
        builder().build(),
        api_only(&shard0),
        two_shard_resources(&shard0, &shard1),
    )
    .start()
    .await;

    let Err(error) = result else {
        panic!("a shard with no notification URL must refuse boot");
    };
    assert!(error.to_string().contains("shard 1"), "{error}");
    assert_eq!(
        before.map(|cache| Arc::as_ptr(&cache)),
        global_admission_gate_cache().map(|cache| Arc::as_ptr(&cache)),
    );
}

/// With one URL per shard, a multi-shard embedding starts and serves.
#[tokio::test]
async fn multi_shard_starts_with_a_notification_url_per_shard() {
    let _serial = SERIAL.lock().await;
    let (shard0, _db) = database().await;
    let shard1 = fresh_database(&shard0).await;

    let runtime = HarvestEmbedding::new(
        builder().build(),
        api_only(&shard0),
        two_shard_resources(&shard0, &shard1),
    )
    .with_notification_database_urls([
        (ShardId::new(0), shard0.clone()),
        (ShardId::new(1), shard1.clone()),
    ])
    .with_admin_auth(StandaloneAdminAuth::new().with_deployment_profile("dev"))
    .start()
    .await
    .expect("multi-shard embedding should start");

    assert_eq!(runtime.runner().storage_pool().iter_shards().count(), 2);
    let (status, _) = send(&runtime.router(), "GET", "/workflows", None, None).await;
    assert_eq!(status, StatusCode::OK);

    runtime.stop().await;
}

// ---------------------------------------------------------------------------
// Operator configuration from the environment
// ---------------------------------------------------------------------------

/// Sets environment variables and removes them on drop, also on panic.
struct EnvGuard(&'static [&'static str]);

impl EnvGuard {
    fn set(vars: &'static [(&'static str, &'static str)], names: &'static [&'static str]) -> Self {
        for (name, value) in vars {
            // SAFETY: `SERIAL` is held, so no other test in this binary reads
            // the environment while this one changes it.
            unsafe { std::env::set_var(name, value) };
        }
        Self(names)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for name in self.0 {
            // SAFETY: see `EnvGuard::set`.
            unsafe { std::env::remove_var(name) };
        }
    }
}

/// The operator's `AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS=fail` overrides
/// the code default of `warn`. `AUTUMN_PROFILE=dev` declares the profile when
/// the embedder declares none. The example used to thread both by hand.
#[tokio::test]
async fn operator_environment_sets_the_startup_action_and_the_profile() {
    let _serial = SERIAL.lock().await;
    let (orphaned, _db) = database().await;
    let clean = fresh_database(&orphaned).await;
    seed_orphan(&orphaned, "embed_env_removed_type").await;
    let _env = EnvGuard::set(
        &[
            ("AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS", "fail"),
            ("AUTUMN_PROFILE", "dev"),
        ],
        &["AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS", "AUTUMN_PROFILE"],
    );

    let refused = HarvestEmbedding::new(
        builder().build(),
        config(&orphaned),
        HarvestRunnerResources::new(pool(&orphaned)),
    )
    .start()
    .await;
    let Err(error) = refused else {
        panic!("the operator's `fail` must refuse boot over an orphan");
    };
    assert!(error.to_string().contains("embed_env_removed_type"), "{error}");

    let runtime = HarvestEmbedding::new(
        builder().build(),
        config(&clean),
        HarvestRunnerResources::new(pool(&clean)),
    )
    .start()
    .await
    .expect("a clean database should boot");
    let (status, _) = send(&runtime.router(), "GET", "/admin/preflight", None, None).await;
    assert_eq!(status, StatusCode::OK);

    runtime.stop().await;
}

