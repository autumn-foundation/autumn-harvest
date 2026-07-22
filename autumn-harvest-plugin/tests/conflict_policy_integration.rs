#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
//! HTTP integration tests for the active-prior conflict policy (issue #685).
//!
//! Exercises the plugin `POST /workflows/{name}/start` conflict-policy axis
//! against a real Postgres (a `HARVEST_TEST_DATABASE_URL`-migrated cluster, or a
//! fresh testcontainer when the env var is unset). `conflict_policy` governs a
//! collision with a currently-ACTIVE (RUNNING/PAUSED) prior; `reuse_policy`
//! (unchanged) governs terminal priors.
//!
//! - **use_existing attaches to a RUNNING prior** → `200`, same execution_id,
//!   `started_fresh: false`, prior stays RUNNING, exactly one WorkflowStarted.
//! - **AC-3 idempotent-starter corner** — `reuse=terminate_if_running` +
//!   `conflict=use_existing` over a COMPLETED prior → `201` fresh (terminal
//!   priors follow the reuse axis; the conflict axis is inert there).
//! - **fail over a RUNNING prior** → `409`.
//! - **terminate_existing over a RUNNING prior** → `201` fresh, prior cancelled.
//! - **omitting conflict_policy** preserves today's response (no `started_fresh`
//!   / `deduplicated` keys — AC-7 byte-identical).
//! - **unknown conflict_policy** → `400`.
//! - **conflict_policy on a throttled / debounced / batched workflow** → `400`
//!   (mutual exclusion — all three deferred-start legs of the guard covered).
//! - **terminate_existing without admin** → `401` (the admin gate is narrowed to
//!   the precise "can cancel a live run" capability = effective active behavior
//!   is `Terminate`).
//! - **terminate_if_running + use_existing without admin, RUNNING prior** →
//!   `200` attach (NOT admin-gated: effective is `Attach`, cannot cancel a live
//!   run) — the flagship non-admin idempotent-starter shape, doubling as the
//!   HTTP AC-3 RUNNING-prior corner (zero WorkflowCancelled for the prior).
//! - **terminate_if_running + use_existing without admin, COMPLETED prior** →
//!   `201` fresh (still non-admin; the terminal prior is replaced by the reuse
//!   axis).
//! - **terminate_if_running with omitted conflict without admin** → `401`
//!   (unchanged; native `Terminate` still requires admin — no regression).
//! - **concurrent use_existing** — N simultaneous same-workflow_id starts →
//!   exactly one execution, all 200/201, zero 409, prior stays RUNNING.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::debounce::DebouncePolicy;
use autumn_harvest::event_batch::BatchPolicy;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::throttle::ThrottlePolicy;
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
use diesel::sql_types::{Text, Uuid as SqlUuid};
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
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

/// `HARVEST_TEST_DATABASE_URL`-first (a locally migrated cluster, no Docker),
/// falling back to a fresh testcontainer for CI. Mirrors the core integration
/// suites (`legal_hold_tests`, `retention_summary_tests`). Each test uses a
/// distinct `workflow_name` so a shared local cluster keeps rows isolated.
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

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(16)
        .build()
        .expect("pool should build")
}

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn plain_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
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

fn throttled_info(name: &'static str) -> WorkflowInfo {
    let mut info = plain_info(name);
    info.throttle = Some(
        ThrottlePolicy::from_rate_str("100/m", Some(10.0), Some("input.tenant_id"), None)
            .expect("valid rate"),
    );
    info
}

fn debounced_info(name: &'static str) -> WorkflowInfo {
    let mut info = plain_info(name);
    info.debounce = Some(DebouncePolicy {
        key_expr: "input.tenant_id",
        window: std::time::Duration::from_secs(30),
        max_wait: None,
    });
    info
}

fn batched_info(name: &'static str) -> WorkflowInfo {
    let mut info = plain_info(name);
    info.batch = Some(BatchPolicy {
        key_expr: "input.tenant_id".to_string(),
        max_size: 10,
        max_wait: std::time::Duration::from_secs(10),
    });
    info
}

/// App that grants admin to every request (`admin_auth_boundary = true`); the
/// embedder's own auth layer is assumed to gate admin upstream. Used by every
/// test except the explicit 401 (no-admin) case.
fn build_app(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    build_app_inner(pool, infos, true)
}

/// App with NO admin (`admin_auth_boundary = false`, default) and no session —
/// so an admin-gated start (`terminate_existing` / `terminate_if_running`)
/// returns `401`.
fn build_app_no_admin(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    build_app_inner(pool, infos, false)
}

fn build_app_inner(pool: &DbPool, infos: Vec<WorkflowInfo>, admin: bool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    if admin {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("conflict-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// POST a start (body carries all fields, incl. `conflict_policy` when set).
async fn post_start(app: &HarvestApiApp, wf: &str, body: Value) -> (StatusCode, Value) {
    // `harvest_api_router` mounts routes at the root (`/workflows/...`); sibling
    // integration tests hit the root path for the same reason.
    let request = Request::builder()
        .method("POST")
        .uri(format!("/workflows/{wf}/start"))
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

async fn raw_connect(url: &str) -> diesel_async::AsyncPgConnection {
    diesel_async::AsyncPgConnection::establish(url)
        .await
        .expect("raw connection should establish")
}

async fn execution_count(conn: &mut diesel_async::AsyncPgConnection, wf: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_workflow_executions WHERE workflow_name = $1",
    )
    .bind::<Text, _>(wf)
    .get_result::<Count>(conn)
    .await
    .expect("count executions")
    .count
}

async fn execution_state(conn: &mut diesel_async::AsyncPgConnection, exec_id: &str) -> String {
    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = Text)]
        state: String,
    }
    let id = uuid::Uuid::parse_str(exec_id).expect("valid uuid");
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<SqlUuid, _>(id)
        .get_result::<StateRow>(conn)
        .await
        .expect("load execution state")
        .state
}

async fn event_type_count(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: &str,
    event_type: &str,
) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    let id = uuid::Uuid::parse_str(exec_id).expect("valid uuid");
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = $2",
    )
    .bind::<SqlUuid, _>(id)
    .bind::<Text, _>(event_type)
    .get_result::<Count>(conn)
    .await
    .expect("count events")
    .count
}

/// Force a prior run terminal (COMPLETED) so the reuse axis governs it. There is
/// no worker in these tests, so a started run stays RUNNING until we do this.
async fn mark_completed(conn: &mut diesel_async::AsyncPgConnection, wf: &str, wf_id: &str) {
    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
         SET state = 'COMPLETED', completed_at = NOW() \
         WHERE workflow_name = $1 AND workflow_id = $2",
    )
    .bind::<Text, _>(wf)
    .bind::<Text, _>(wf_id)
    .execute(conn)
    .await
    .expect("mark completed");
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// AC: `conflict_policy=use_existing` over a RUNNING prior attaches — `200`,
/// same execution_id, `started_fresh: false`, prior stays RUNNING, exactly one
/// WorkflowStarted event.
#[tokio::test]
async fn use_existing_attaches_to_running_prior_returns_200() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("ue_running")]);

    let body = json!({"workflow_id": "ue-1", "input": {"n": 1}});
    let (s1, b1) = post_start(&app, "ue_running", body.clone()).await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    let (s2, b2) = post_start(
        &app,
        "ue_running",
        json!({"workflow_id": "ue-1", "input": {"n": 2}, "conflict_policy": "use_existing"}),
    )
    .await;
    assert_eq!(s2, StatusCode::OK, "attach must return 200: {b2}");
    assert_eq!(b2["started_fresh"], json!(false), "attach: {b2}");
    assert_eq!(
        b2["execution_id"].as_str().unwrap(),
        exec1,
        "attach returns the existing execution_id"
    );

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "ue_running").await, 1);
    assert_eq!(
        execution_state(&mut conn, &exec1).await,
        "RUNNING",
        "the attached-to prior must stay RUNNING"
    );
    assert_eq!(
        event_type_count(&mut conn, &exec1, "WorkflowStarted").await,
        1,
        "attach must not append a second WorkflowStarted"
    );
}

/// AC-3 idempotent-starter corner: `reuse=terminate_if_running` +
/// `conflict=use_existing` over a COMPLETED (terminal) prior → `201` fresh. The
/// conflict axis is inert for a terminal prior; the reuse axis
/// (terminate_if_running) replaces it.
#[tokio::test]
async fn terminate_if_running_reuse_plus_use_existing_completed_returns_201() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("ac3_completed")]);

    let (s1, b1) = post_start(
        &app,
        "ac3_completed",
        json!({"workflow_id": "ac3-1", "input": {}}),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    // Drive the prior terminal.
    let mut conn = raw_connect(&url).await;
    mark_completed(&mut conn, "ac3_completed", "ac3-1").await;

    let (s2, b2) = post_start(
        &app,
        "ac3_completed",
        json!({
            "workflow_id": "ac3-1",
            "input": {},
            "reuse_policy": "terminate_if_running",
            "conflict_policy": "use_existing"
        }),
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::CREATED,
        "terminal prior + terminate_if_running → fresh 201: {b2}"
    );
    assert_eq!(b2["started_fresh"], json!(true), "fresh: {b2}");
    let exec2 = b2["execution_id"].as_str().unwrap().to_string();
    assert_ne!(exec2, exec1, "a fresh run gets a new execution_id");
}

/// AC: `conflict_policy=fail` over a RUNNING prior → `409 AlreadyExists`.
#[tokio::test]
async fn fail_conflict_running_returns_409() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("fail_running")]);

    let (s1, _b1) = post_start(
        &app,
        "fail_running",
        json!({"workflow_id": "f-1", "input": {}}),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED);

    let (s2, b2) = post_start(
        &app,
        "fail_running",
        json!({"workflow_id": "f-1", "input": {}, "conflict_policy": "fail"}),
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::CONFLICT,
        "fail over an active prior → 409: {b2}"
    );
}

/// AC: `conflict_policy=terminate_existing` over a RUNNING prior → `201` fresh,
/// prior cancelled (a WorkflowCancelled event is appended and the prior is no
/// longer RUNNING — `replace_execution` seals the cancelled row).
#[tokio::test]
async fn terminate_existing_running_returns_201_and_cancels_prior() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("term_running")]);

    let (s1, b1) = post_start(
        &app,
        "term_running",
        json!({"workflow_id": "t-1", "input": {}}),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    let prior = b1["execution_id"].as_str().unwrap().to_string();

    let (s2, b2) = post_start(
        &app,
        "term_running",
        json!({"workflow_id": "t-1", "input": {}, "conflict_policy": "terminate_existing"}),
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::CREATED,
        "terminate_existing starts fresh → 201: {b2}"
    );
    assert_eq!(b2["started_fresh"], json!(true), "fresh: {b2}");
    let fresh = b2["execution_id"].as_str().unwrap().to_string();
    assert_ne!(fresh, prior, "a fresh run gets a new execution_id");

    let mut conn = raw_connect(&url).await;
    assert_ne!(
        execution_state(&mut conn, &prior).await,
        "RUNNING",
        "the prior must be cancelled/sealed (no longer RUNNING)"
    );
    assert_eq!(
        event_type_count(&mut conn, &prior, "WorkflowCancelled").await,
        1,
        "terminate_existing must cancel the prior (WorkflowCancelled appended)"
    );
    assert_eq!(
        execution_state(&mut conn, &fresh).await,
        "RUNNING",
        "the fresh run is RUNNING"
    );
}

/// AC-7: omitting `conflict_policy` keeps the response byte-for-byte identical
/// to a pre-#685/#808 build — neither `started_fresh` nor `deduplicated` appears.
#[tokio::test]
async fn omitting_conflict_policy_preserves_today_response() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("omit_default")]);

    let (s, b) = post_start(
        &app,
        "omit_default",
        json!({"workflow_id": "o-1", "input": {}}),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let obj = b.as_object().expect("response is an object");
    assert!(
        !obj.contains_key("started_fresh"),
        "no conflict_policy sent → started_fresh must be omitted: {b}"
    );
    assert!(
        !obj.contains_key("deduplicated"),
        "no idempotency_key → deduplicated must be omitted: {b}"
    );
}

/// AC: an unknown `conflict_policy` value → `400`.
#[tokio::test]
async fn unknown_conflict_policy_returns_400() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("unknown_val")]);

    let (s, b) = post_start(
        &app,
        "unknown_val",
        json!({"workflow_id": "u-1", "input": {}, "conflict_policy": "bogus"}),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "unknown value → 400: {b}");
}

/// AC: a non-default `conflict_policy` on a throttled workflow → `400` (a
/// throttled start is deferred and has no active prior to resolve at request
/// time — mutual exclusion, mirroring the idempotency_key rule).
#[tokio::test]
async fn conflict_policy_on_throttled_workflow_returns_400() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![throttled_info("throttled_ue")]);

    let (s, b) = post_start(
        &app,
        "throttled_ue",
        json!({
            "workflow_id": "th-1",
            "input": {"tenant_id": "acme"},
            "conflict_policy": "use_existing"
        }),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "conflict_policy + throttle → 400: {b}"
    );
    // And omitting conflict_policy on the same throttled workflow is accepted
    // (defers with 202) — proving the 400 is caused by conflict_policy, not the
    // throttle itself.
    let (s_ok, _b_ok) = post_start(
        &app,
        "throttled_ue",
        json!({"workflow_id": "th-2", "input": {"tenant_id": "acme"}}),
    )
    .await;
    assert_ne!(
        s_ok,
        StatusCode::BAD_REQUEST,
        "omitting conflict_policy must not 400 on a throttled workflow"
    );
}

/// FIX 5: the debounce leg of the `conflict_policy != Unspecified &&
/// (throttle||debounce||batch) → 400` mutual-exclusion guard (the throttle leg
/// is covered above; this exercises the debounce leg).
#[tokio::test]
async fn conflict_policy_on_debounced_workflow_returns_400() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![debounced_info("debounced_ue")]);

    let (s, b) = post_start(
        &app,
        "debounced_ue",
        json!({
            "workflow_id": "db-1",
            "input": {"tenant_id": "acme"},
            "conflict_policy": "use_existing"
        }),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "conflict_policy + debounce → 400: {b}"
    );
    // Omitting conflict_policy on the same debounced workflow is accepted (defers)
    // — proving the 400 is caused by conflict_policy, not the debounce itself.
    let (s_ok, _b_ok) = post_start(
        &app,
        "debounced_ue",
        json!({"workflow_id": "db-2", "input": {"tenant_id": "acme"}}),
    )
    .await;
    assert_ne!(
        s_ok,
        StatusCode::BAD_REQUEST,
        "omitting conflict_policy must not 400 on a debounced workflow"
    );
}

/// FIX 5: the batch leg of the same mutual-exclusion guard.
#[tokio::test]
async fn conflict_policy_on_batched_workflow_returns_400() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![batched_info("batched_ue")]);

    let (s, b) = post_start(
        &app,
        "batched_ue",
        json!({
            "workflow_id": "ba-1",
            "input": {"tenant_id": "acme"},
            "conflict_policy": "use_existing"
        }),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "conflict_policy + batch → 400: {b}"
    );
    // Omitting conflict_policy on the same batched workflow is accepted (defers)
    // — proving the 400 is caused by conflict_policy, not the batch itself.
    let (s_ok, _b_ok) = post_start(
        &app,
        "batched_ue",
        json!({"workflow_id": "ba-2", "input": {"tenant_id": "acme"}}),
    )
    .await;
    assert_ne!(
        s_ok,
        StatusCode::BAD_REQUEST,
        "omitting conflict_policy must not 400 on a batched workflow"
    );
}

/// AC (security parity): `conflict_policy=terminate_existing` requires admin —
/// without admin it returns `401`, exactly like `reuse=terminate_if_running`.
#[tokio::test]
async fn terminate_existing_without_admin_returns_401() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app_no_admin(&pool, vec![plain_info("term_noadmin")]);

    let (s, b) = post_start(
        &app,
        "term_noadmin",
        json!({"workflow_id": "na-1", "input": {}, "conflict_policy": "terminate_existing"}),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::UNAUTHORIZED,
        "terminate_existing without admin → 401: {b}"
    );

    // Control: a non-destructive conflict policy (use_existing) needs no admin.
    let (s_ok, _b_ok) = post_start(
        &app,
        "term_noadmin",
        json!({"workflow_id": "na-2", "input": {}, "conflict_policy": "use_existing"}),
    )
    .await;
    assert_eq!(
        s_ok,
        StatusCode::CREATED,
        "use_existing without admin must be allowed"
    );
}

/// FIX 1 gate-narrowing + semantics-review L1 (HTTP AC-3 RUNNING-prior corner):
/// the flagship non-admin idempotent-starter shape `reuse=terminate_if_running`
/// + `conflict=use_existing` over a RUNNING prior resolves to `Attach` and
/// provably cannot cancel a live run, so it is NOT admin-gated — `200`,
/// `started_fresh:false`, same execution_id, prior stays RUNNING, ZERO
/// WorkflowCancelled events for the prior. This is the shape the narrowed
/// capability-precise gate exists to permit.
#[tokio::test]
async fn terminate_if_running_plus_use_existing_is_not_admin_gated() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app_no_admin(&pool, vec![plain_info("tir_ue_noadmin")]);

    // Seed a RUNNING prior with a plain (non-admin) start.
    let (s1, b1) = post_start(
        &app,
        "tir_ue_noadmin",
        json!({"workflow_id": "tir-ue-1", "input": {}}),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "seed running prior: {b1}");
    let prior = b1["execution_id"].as_str().unwrap().to_string();

    // The flagship non-admin shape: terminate_if_running + use_existing over the
    // RUNNING prior → attach, NOT 401.
    let (s2, b2) = post_start(
        &app,
        "tir_ue_noadmin",
        json!({
            "workflow_id": "tir-ue-1",
            "input": {},
            "reuse_policy": "terminate_if_running",
            "conflict_policy": "use_existing"
        }),
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "terminate_if_running + use_existing over a RUNNING prior must NOT be admin-gated (attach → 200): {b2}"
    );
    assert_eq!(b2["started_fresh"], json!(false), "attach: {b2}");
    assert_eq!(
        b2["execution_id"].as_str().unwrap(),
        prior,
        "attach returns the existing execution_id"
    );

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "tir_ue_noadmin").await, 1);
    assert_eq!(
        execution_state(&mut conn, &prior).await,
        "RUNNING",
        "the attached-to prior must stay RUNNING (zero terminations)"
    );
    assert_eq!(
        event_type_count(&mut conn, &prior, "WorkflowCancelled").await,
        0,
        "an Attach resolution must never cancel the prior"
    );
}

/// FIX 1: the AC-3 corner as a non-admin idempotent starter —
/// `reuse=terminate_if_running` + `conflict=use_existing` over a COMPLETED
/// (terminal) prior on a NO-admin app → `201` fresh. The gate is
/// state-independent (Attach resolution regardless of prior state), so it never
/// requires admin; the terminal prior is then replaced by the reuse axis.
#[tokio::test]
async fn terminate_if_running_plus_use_existing_over_completed_is_not_admin_gated() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app_no_admin(&pool, vec![plain_info("tir_ue_completed")]);

    let (s1, b1) = post_start(
        &app,
        "tir_ue_completed",
        json!({"workflow_id": "tir-uc-1", "input": {}}),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "seed prior: {b1}");
    let prior = b1["execution_id"].as_str().unwrap().to_string();

    let mut conn = raw_connect(&url).await;
    mark_completed(&mut conn, "tir_ue_completed", "tir-uc-1").await;

    let (s2, b2) = post_start(
        &app,
        "tir_ue_completed",
        json!({
            "workflow_id": "tir-uc-1",
            "input": {},
            "reuse_policy": "terminate_if_running",
            "conflict_policy": "use_existing"
        }),
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::CREATED,
        "non-admin terminate_if_running + use_existing over a COMPLETED prior → fresh 201: {b2}"
    );
    assert_eq!(b2["started_fresh"], json!(true), "fresh: {b2}");
    assert_ne!(
        b2["execution_id"].as_str().unwrap(),
        prior,
        "a fresh run gets a new execution_id"
    );
}

/// FIX 1 no-regression: `reuse=terminate_if_running` with the conflict axis
/// OMITTED (Unspecified → native Terminate) over a RUNNING prior STILL requires
/// admin — the narrowed gate must not weaken the stock trunk behavior.
#[tokio::test]
async fn terminate_if_running_without_conflict_still_requires_admin() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app_no_admin(&pool, vec![plain_info("tir_noconflict_noadmin")]);

    // Seed a RUNNING prior with a plain (non-admin) start.
    let (s1, b1) = post_start(
        &app,
        "tir_noconflict_noadmin",
        json!({"workflow_id": "tir-nc-1", "input": {}}),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED);
    let prior = b1["execution_id"].as_str().unwrap().to_string();

    let (s2, b2) = post_start(
        &app,
        "tir_noconflict_noadmin",
        json!({
            "workflow_id": "tir-nc-1",
            "input": {},
            "reuse_policy": "terminate_if_running"
        }),
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::UNAUTHORIZED,
        "terminate_if_running with omitted conflict (native Terminate) must still require admin: {b2}"
    );

    // The prior was never touched (still RUNNING, not cancelled).
    let mut conn = raw_connect(&url).await;
    assert_eq!(
        execution_state(&mut conn, &prior).await,
        "RUNNING",
        "a rejected (401) terminate must leave the prior RUNNING"
    );
    assert_eq!(
        event_type_count(&mut conn, &prior, "WorkflowCancelled").await,
        0,
        "a rejected (401) terminate must never cancel the prior"
    );
}

/// Success-metric proof: N concurrent same-workflow_id starts under
/// `conflict_policy=use_existing` converge on exactly one execution — all
/// 200/201, exactly one 201 (the creator), zero 409, prior stays RUNNING, zero
/// terminations. (100/100 is this same shape; N=20 here.)
#[tokio::test]
async fn concurrent_use_existing_converges() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("concurrent_ue")]);

    const N: usize = 20;
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            post_start(
                &app,
                "concurrent_ue",
                json!({
                    "workflow_id": "c-1",
                    "input": {"n": i},
                    "conflict_policy": "use_existing"
                }),
            )
            .await
        }));
    }

    let mut created = 0usize;
    let mut ok = 0usize;
    let mut conflicts = 0usize;
    let mut exec_ids = std::collections::HashSet::new();
    for h in handles {
        let (status, body) = h.await.expect("task join");
        match status {
            StatusCode::CREATED => created += 1,
            StatusCode::OK => ok += 1,
            StatusCode::CONFLICT => conflicts += 1,
            other => panic!("unexpected status {other}: {body}"),
        }
        if let Some(id) = body["execution_id"].as_str() {
            exec_ids.insert(id.to_string());
        }
    }

    assert_eq!(conflicts, 0, "use_existing must never 409");
    assert_eq!(created, 1, "exactly one request creates the execution");
    assert_eq!(ok, N - 1, "the rest attach with 200");
    assert_eq!(
        exec_ids.len(),
        1,
        "all requests converge on one execution_id"
    );

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "concurrent_ue").await, 1);
    let exec = exec_ids.into_iter().next().unwrap();
    assert_eq!(
        execution_state(&mut conn, &exec).await,
        "RUNNING",
        "the converged execution stays RUNNING (zero terminations)"
    );
}
