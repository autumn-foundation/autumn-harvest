//! Integration tests for tenant binding (issue #1977).
//!
//! Tests run against a real Postgres. Set `HARVEST_TEST_DATABASE_URL` to a
//! migrated database and run with `--test-threads=1`. Otherwise a fresh
//! testcontainers Postgres starts with the full migration set.
//!
//! Coverage:
//!   - a tenant-A token cannot read or cancel a tenant-B run by setting the
//!     tenant header (the RED acceptance criterion)
//!   - the authorizer sees the verified tenant, not the header
//!   - with no authorizer, a tenant token reaches only its own runs
//!   - a tenant token is refused off the tenant-scoped routes
//!   - a header that names another tenant is refused and audited
//!   - a start stamps the verified tenant, and a collision answers `409`
//!   - an embedder `VerifiedTenant` binds the same way as a token tenant
//!   - the mint route stores, lists and validates the tenant

#![allow(clippy::too_many_lines)]

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    StartWorkflowParams, WorkflowInfo, context::WorkflowContext, start_or_load_workflow_execution,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, StandaloneAdminAuth,
    harvest_api_router,
};
use autumn_harvest_plugin::authz::{AuthzDecision, AuthzRequest};
use autumn_harvest_plugin::harvest_ui_router;
use autumn_harvest_plugin::tenant::VerifiedTenant;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type App = axum::Router;

/// The `(tenant_key, tenant_verified)` pairs an authorizer saw.
type Seen = Arc<Mutex<Vec<(Option<String>, bool)>>>;

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
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
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn plain_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

const WORKFLOW: &str = "tenant_wf";

fn plain_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: plain_workflow,
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

fn api_state(pool: &DbPool) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![plain_info(WORKFLOW)], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("tenant-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

/// The management router with Vantage nested at `/ui`.
fn composed_router(state: &HarvestApiState) -> axum::Router {
    harvest_api_router(state.clone()).nest("/ui", harvest_ui_router(state.clone()))
}

/// The embedder boundary plus the token layer. The boundary mints tokens.
fn boundary_app(state: &HarvestApiState) -> App {
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_admin_auth_boundary()
        .mount(composed_router(state), state)
}

/// Tokens are the only auth. No embedder boundary and no authorizer.
fn standalone_app(state: &HarvestApiState) -> App {
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .mount(composed_router(state), state)
}

/// The boundary app. The embedder's own auth layer verifies `tenant`.
fn embedder_app(state: &HarvestApiState, tenant: &'static str) -> App {
    boundary_app(state).layer(axum::middleware::from_fn(
        move |mut req: axum::extract::Request, next: axum::middleware::Next| async move {
            req.extensions_mut()
                .insert(VerifiedTenant::new(tenant).expect("valid tenant"));
            next.run(req).await
        },
    ))
}

/// The boundary app with an authorizer installed.
fn authorized_app<F>(state: &HarvestApiState, authorizer: F) -> App
where
    F: Fn(&AuthzRequest<'_>) -> AuthzDecision + Send + Sync + 'static,
{
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_admin_auth_boundary()
        .with_authorizer(authorizer)
        .mount(composed_router(state), state)
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        // The runs of this suite have no worker. Remove them, so a later
        // suite on a shared database does not claim their tasks.
        "DELETE FROM harvest_workflow_executions WHERE workflow_name = 'tenant_wf'",
        "DELETE FROM harvest_api_tokens",
        "DELETE FROM harvest_audit_log",
        "DELETE FROM harvest_audit_export_cursor",
    ] {
        let _ = diesel::sql_query(stmt).execute(conn).await;
    }
}

struct Call<'a> {
    method: &'a str,
    uri: &'a str,
    body: Option<Value>,
    bearer: Option<&'a str>,
    tenant: Option<&'a str>,
    key: Option<&'a str>,
}

impl<'a> Call<'a> {
    const fn new(method: &'a str, uri: &'a str) -> Self {
        Self {
            method,
            uri,
            body: None,
            bearer: None,
            tenant: None,
            key: None,
        }
    }

    fn body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }

    const fn bearer(mut self, bearer: &'a str) -> Self {
        self.bearer = Some(bearer);
        self
    }

    const fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    const fn key(mut self, key: &'a str) -> Self {
        self.key = Some(key);
        self
    }
}

async fn send(app: &App, call: Call<'_>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(call.method).uri(call.uri);
    if call.body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if let Some(b) = call.bearer {
        builder = builder.header("authorization", format!("Bearer {b}"));
    }
    if let Some(t) = call.tenant {
        builder = builder.header("x-harvest-tenant", t);
    }
    if let Some(k) = call.key {
        builder = builder.header("idempotency-key", k);
    }
    let body = call
        .body
        .map_or_else(Body::empty, |json| Body::from(json.to_string()));
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .expect("request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

/// Mint a token with a tenant claim through the boundary app. Returns the
/// secret.
async fn mint_for_tenant(app: &App, scope: &str, tenant: &str) -> String {
    let (status, resp) = send(
        app,
        Call::new("POST", "/admin/tokens").body(json!({
            "name": format!("{tenant}-{scope}"),
            "scope": scope,
            "tenant": tenant,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "mint should 201: {resp:?}");
    resp["secret"].as_str().expect("secret").to_string()
}

/// Start one run in-process and return its id.
async fn start_run(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    start_run_for(conn, workflow_id, None).await
}

/// Start one run in-process, stamped with `tenant`, and return its id.
async fn start_run_for(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    tenant: Option<&str>,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let params = StartWorkflowParams {
        tenant,
        ..StartWorkflowParams::new(WORKFLOW, workflow_id, exec_id, json!({}), "default")
    };
    start_or_load_workflow_execution(conn, params, None)
        .await
        .expect("start")
        .exec_id
}

/// The stored tenant of one run.
async fn stored_tenant(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<String> {
    #[derive(diesel::QueryableByName)]
    struct T {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        tenant: Option<String>,
    }
    diesel::sql_query("SELECT tenant FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<T>(conn)
        .await
        .expect("load tenant")
        .tenant
}

/// The `error_summary` of every `authz.deny` row, oldest first.
async fn deny_summaries(conn: &mut AsyncPgConnection) -> Vec<String> {
    #[derive(diesel::QueryableByName)]
    struct D {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        error_summary: Option<String>,
    }
    diesel::sql_query(
        "SELECT error_summary FROM harvest_audit_log \
         WHERE operation = 'authz.deny' ORDER BY occurred_at",
    )
    .load::<D>(conn)
    .await
    .unwrap()
    .into_iter()
    .map(|d| d.error_summary.unwrap_or_default())
    .collect()
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

/// RED acceptance criterion (issue #1977).
///
/// The embedder keeps its own map of runs to tenants. Its authorizer allows a
/// request when the run belongs to `tenant_key`. That was the documented way to
/// confine a caller. A caller with an `acme` credential sets the header to
/// `globex` and reads, then cancels, a `globex` run.
#[tokio::test]
async fn tenant_a_credential_cannot_reach_tenant_b_by_setting_the_header() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);

    let globex_run = start_run_for(
        &mut conn,
        &format!("globex-{}", uuid::Uuid::new_v4()),
        Some("globex"),
    )
    .await;
    let owner = globex_run.to_string();
    let app = authorized_app(&state, move |req| {
        let target_is_globex = req.path.contains(&owner);
        match (target_is_globex, req.tenant_key) {
            (true, Some("globex")) | (false, _) => AuthzDecision::Allow,
            (true, _) => AuthzDecision::deny("run belongs to globex"),
        }
    });
    let acme = mint_for_tenant(&boundary_app(&state), "mutate", "acme").await;

    let (status, body) = send(
        &app,
        Call::new("GET", &format!("/workflows/{globex_run}"))
            .bearer(&acme)
            .tenant("globex"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "an acme token must not read a globex run: {body:?}"
    );

    let (status, body) = send(
        &app,
        Call::new("POST", &format!("/workflows/{globex_run}/cancel"))
            .bearer(&acme)
            .tenant("globex")
            .body(json!({ "reason": "cross-tenant" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "an acme token must not cancel a globex run: {body:?}"
    );

    // Without the header the row check refuses the run of another tenant.
    let (status, body) = send(
        &app,
        Call::new("GET", &format!("/workflows/{globex_run}")).bearer(&acme),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body:?}");

    let summaries = deny_summaries(&mut conn).await;
    assert_eq!(
        summaries.len(),
        3,
        "one audit row per refusal: {summaries:?}"
    );

    let (state_now,): (String,) = {
        #[derive(diesel::QueryableByName)]
        struct S {
            #[diesel(sql_type = diesel::sql_types::Text)]
            state: String,
        }
        let row: S =
            diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(globex_run.as_uuid())
                .get_result(&mut conn)
                .await
                .unwrap();
        (row.state,)
    };
    assert_eq!(state_now, "RUNNING", "the globex run must not be cancelled");
}

/// With no authorizer, a tenant token reaches its own runs and no others. A
/// run of another tenant, and a run with no tenant, answer `404`, the same as
/// an unknown id.
#[tokio::test]
async fn tenant_token_reaches_only_its_own_runs_without_an_authorizer() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = standalone_app(&state);
    let acme = mint_for_tenant(&boundary_app(&state), "mutate", "acme").await;

    let own = start_run_for(&mut conn, &unique("acme"), Some("acme")).await;
    let other = start_run_for(&mut conn, &unique("globex"), Some("globex")).await;
    let untenanted = start_run(&mut conn, &unique("none")).await;
    let unknown = ExecutionId::new();

    let (status, body) = send(
        &app,
        Call::new("GET", &format!("/workflows/{own}")).bearer(&acme),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["execution"]["tenant"], "acme", "{body:?}");

    for target in [other, untenanted, unknown] {
        for (method, path) in [
            ("GET", format!("/workflows/{target}")),
            ("GET", format!("/workflows/{target}/history")),
            ("POST", format!("/workflows/{target}/cancel")),
            ("POST", format!("/workflows/{target}/signal/go")),
        ] {
            let (status, body) =
                send(&app, Call::new(method, &path).bearer(&acme).body(json!({}))).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}: {body:?}");
            assert!(
                !body.to_string().contains("globex"),
                "the answer must not name the other tenant: {body:?}"
            );
        }
    }
    let (status, _) = send(
        &app,
        Call::new("POST", &format!("/workflows/{own}/cancel"))
            .bearer(&acme)
            .body(json!({ "reason": "mine" })),
    )
    .await;
    assert!(
        status.is_success(),
        "a tenant may cancel its own run: {status}"
    );
    assert_eq!(
        stored_tenant(&mut conn, other).await.as_deref(),
        Some("globex")
    );

    // An untenanted token is not bound, so it reaches every run.
    let operator = {
        let (status, resp) = send(
            &boundary_app(&state),
            Call::new("POST", "/admin/tokens").body(json!({ "name": "ops", "scope": "read" })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        resp["secret"].as_str().unwrap().to_string()
    };
    let (status, _) = send(
        &app,
        Call::new("GET", &format!("/workflows/{other}")).bearer(&operator),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

/// A tenant token is refused, with an audit row, off the tenant-scoped
/// routes. That covers list routes, token mint, Vantage and other start paths.
#[tokio::test]
async fn tenant_token_is_refused_off_the_tenant_scoped_routes() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = standalone_app(&state);
    let admin = mint_for_tenant(&boundary_app(&state), "admin", "acme").await;
    let own = start_run_for(&mut conn, &unique("acme"), Some("acme")).await;

    let refused = [
        ("GET", "/workflows".to_string(), None),
        (
            "POST",
            "/admin/tokens".to_string(),
            Some(json!({ "name": "escalate", "scope": "admin" })),
        ),
        ("GET", "/admin/tokens".to_string(), None),
        ("GET", "/dead-letters".to_string(), None),
        ("GET", format!("/ui/workflows/{own}"), None),
        (
            "POST",
            format!("/workflows/{WORKFLOW}/signal-with-start"),
            Some(json!({ "signal_name": "go" })),
        ),
        (
            "POST",
            format!("/workflows/{own}/legal-hold/release"),
            Some(json!({})),
        ),
        (
            "POST",
            format!("/workflows/{own}/erase-payloads"),
            Some(json!({})),
        ),
        ("GET", format!("/workflows/{own}/children"), None),
    ];
    for (method, path, body) in &refused {
        let mut call = Call::new(method, path).bearer(&admin);
        if let Some(b) = body {
            call = call.body(b.clone());
        }
        let (status, resp) = send(&app, call).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {resp:?}");
    }
    let tokens: i64 = {
        #[derive(diesel::QueryableByName)]
        struct C {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_api_tokens")
            .get_result::<C>(&mut conn)
            .await
            .unwrap()
            .n
    };
    assert_eq!(tokens, 1, "a tenant token must not mint another token");

    let summaries = deny_summaries(&mut conn).await;
    assert_eq!(summaries.len(), refused.len(), "{summaries:?}");
    assert!(
        summaries.iter().all(|s| s.contains("not tenant-scoped")),
        "{summaries:?}"
    );

    // A public route still answers.
    let (status, _) = send(&app, Call::new("GET", "/health").bearer(&admin)).await;
    assert_eq!(status, StatusCode::OK, "a public route still answers");
}

/// A header that names another tenant is refused and audited, on any route.
/// A header that names the verified tenant passes.
#[tokio::test]
async fn header_naming_another_tenant_is_refused_and_audited() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = standalone_app(&state);
    let acme = mint_for_tenant(&boundary_app(&state), "mutate", "acme").await;
    let own = start_run_for(&mut conn, &unique("acme"), Some("acme")).await;

    let (status, body) = send(
        &app,
        Call::new("GET", &format!("/workflows/{own}"))
            .bearer(&acme)
            .tenant("globex"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    assert!(!body.to_string().contains("acme"), "{body:?}");

    let (status, _) = send(
        &app,
        Call::new("GET", &format!("/workflows/{own}"))
            .bearer(&acme)
            .tenant("acme"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let summaries = deny_summaries(&mut conn).await;
    assert_eq!(summaries.len(), 1, "{summaries:?}");
    assert!(summaries[0].contains("tenant mismatch"), "{summaries:?}");
    assert!(
        summaries[0].contains(r#"declared="globex""#),
        "{summaries:?}"
    );
}

/// The authorizer sees the verified tenant and the `tenant_verified` flag. A
/// caller with no credential tenant still passes the header, unverified.
#[tokio::test]
async fn authorizer_sees_the_verified_tenant() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let seen: Seen = Arc::default();
    let sink = seen.clone();
    let app = authorized_app(&state, move |req| {
        sink.lock()
            .unwrap()
            .push((req.tenant_key.map(str::to_string), req.tenant_verified));
        AuthzDecision::Allow
    });
    let acme = mint_for_tenant(&boundary_app(&state), "read", "acme").await;
    let own = start_run_for(&mut conn, &unique("acme"), Some("acme")).await;

    let (status, _) = send(
        &app,
        Call::new("GET", &format!("/workflows/{own}")).bearer(&acme),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&app, Call::new("GET", "/workflows").tenant("initech")).await;
    assert_eq!(status, StatusCode::OK);
    // A public route also carries the verified tenant to the hook.
    let (status, _) = send(&app, Call::new("GET", "/health").bearer(&acme)).await;
    assert_eq!(status, StatusCode::OK);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![
            (Some("acme".to_string()), true),
            (Some("initech".to_string()), false),
            (Some("acme".to_string()), true),
        ]
    );
}

/// An HTTP start by a tenant token stamps the verified tenant. A start that
/// resolves to a run of another tenant answers `409` with no execution id. A
/// deferred start is refused, because it cannot carry the tenant.
#[tokio::test]
async fn start_stamps_the_verified_tenant() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = standalone_app(&state);
    let acme = mint_for_tenant(&boundary_app(&state), "mutate", "acme").await;

    let (status, body) = send(
        &app,
        Call::new("POST", &format!("/workflows/{WORKFLOW}/start"))
            .bearer(&acme)
            .body(json!({ "workflow_id": unique("acme"), "input": {} })),
    )
    .await;
    assert!(status.is_success(), "{status}: {body:?}");
    let started: ExecutionId = body["execution_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        stored_tenant(&mut conn, started).await.as_deref(),
        Some("acme")
    );
    let (status, _) = send(
        &app,
        Call::new("GET", &format!("/workflows/{started}/result")).bearer(&acme),
    )
    .await;
    assert!(
        status == StatusCode::NO_CONTENT || status.is_success(),
        "the run is the caller's own: {status}"
    );

    let shared_id = unique("shared");
    start_run_for(&mut conn, &shared_id, Some("globex")).await;
    let (status, body) = send(
        &app,
        Call::new("POST", &format!("/workflows/{WORKFLOW}/start"))
            .bearer(&acme)
            .body(json!({
                "workflow_id": shared_id,
                "input": {},
                "conflict_policy": "use_existing",
            })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body:?}");
    assert!(body.get("execution_id").is_none(), "{body:?}");

    let (status, body) = send(
        &app,
        Call::new("POST", &format!("/workflows/{WORKFLOW}/start"))
            .bearer(&acme)
            .body(json!({ "workflow_id": unique("batch"), "input": {}, "batch_key": "k" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");

    // A start with no verified tenant stamps nothing, even with a header.
    let (status, body) = send(
        &boundary_app(&state),
        Call::new("POST", &format!("/workflows/{WORKFLOW}/start"))
            .tenant("acme")
            .body(json!({ "workflow_id": unique("open"), "input": {} })),
    )
    .await;
    assert!(status.is_success(), "{status}: {body:?}");
    let open: ExecutionId = body["execution_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(stored_tenant(&mut conn, open).await, None);
}

/// The embedder's own auth layer binds a tenant with `VerifiedTenant`. It
/// binds the same way as a token tenant. A token of another tenant on the same
/// request is refused.
#[tokio::test]
async fn embedder_verified_tenant_binds_like_a_token_tenant() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = embedder_app(&state, "acme");
    let own = start_run_for(&mut conn, &unique("acme"), Some("acme")).await;
    let other = start_run_for(&mut conn, &unique("globex"), Some("globex")).await;

    let (status, _) = send(&app, Call::new("GET", &format!("/workflows/{own}"))).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&app, Call::new("GET", &format!("/workflows/{other}"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&app, Call::new("GET", "/workflows")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let globex = mint_for_tenant(&boundary_app(&state), "read", "globex").await;
    let (status, _) = send(
        &app,
        Call::new("GET", &format!("/workflows/{other}")).bearer(&globex),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "an embedder tenant and a token tenant that differ"
    );
    let summaries = deny_summaries(&mut conn).await;
    assert_eq!(summaries.len(), 3, "{summaries:?}");
    assert!(
        summaries
            .last()
            .is_some_and(|s| s.contains("token tenant and embedder tenant differ")),
        "{summaries:?}"
    );
}

/// The mint route stores the tenant and the list route shows it. A blank, a
/// spaced or a long tenant is `400`.
#[tokio::test]
async fn mint_stores_lists_and_validates_the_tenant() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let boundary = boundary_app(&state);

    let (status, minted) = send(
        &boundary,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "t", "scope": "read", "tenant": "acme" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{minted:?}");
    assert_eq!(minted["tenant"], "acme");

    let (status, untenanted) = send(
        &boundary,
        Call::new("POST", "/admin/tokens").body(json!({ "name": "u", "scope": "read" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(untenanted["tenant"].is_null(), "{untenanted:?}");

    let (status, listed) = send(&boundary, Call::new("GET", "/admin/tokens")).await;
    assert_eq!(status, StatusCode::OK);
    let tenants: Vec<Value> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["tenant"].clone())
        .collect();
    assert!(tenants.contains(&json!("acme")), "{listed:?}");
    assert!(tenants.contains(&Value::Null), "{listed:?}");

    for bad in [
        json!(""),
        json!("a b"),
        json!(" acme"),
        json!("t".repeat(129)),
    ] {
        let (status, body) = send(
            &boundary,
            Call::new("POST", "/admin/tokens")
                .body(json!({ "name": "bad", "scope": "read", "tenant": bad })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body:?}");
    }
}

/// Mark one run as finished.
async fn complete_run(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
         SET state = 'COMPLETED', completed_at = now() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("complete run");
}

/// The state of one run.
async fn run_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<S>(conn)
        .await
        .expect("load state")
        .state
}

/// A tenant-bound start that reuses the workflow id of another tenant must
/// not cancel, replace or seal that run. The embedder boundary admits the
/// terminate policies, so this is the strongest caller.
#[tokio::test]
async fn tenant_start_cannot_terminate_or_replace_another_tenants_run() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = embedder_app(&state, "acme");

    for policy in [
        json!({ "conflict_policy": "terminate_existing" }),
        json!({ "reuse_policy": "terminate_if_running" }),
    ] {
        let shared_id = unique("live");
        let globex = start_run_for(&mut conn, &shared_id, Some("globex")).await;
        let mut body = json!({ "workflow_id": shared_id, "input": {} });
        body.as_object_mut()
            .unwrap()
            .extend(policy.as_object().unwrap().clone());
        let (status, resp) = send(
            &app,
            Call::new("POST", &format!("/workflows/{WORKFLOW}/start")).body(body),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{policy}: {resp:?}");
        assert_eq!(
            run_state(&mut conn, globex).await,
            "RUNNING",
            "{policy}: the globex run must survive"
        );
    }

    // A finished run of another tenant must not be replaced either.
    let finished_id = unique("done");
    let globex = start_run_for(&mut conn, &finished_id, Some("globex")).await;
    complete_run(&mut conn, globex).await;
    let (status, resp) = send(
        &app,
        Call::new("POST", &format!("/workflows/{WORKFLOW}/start"))
            .body(json!({ "workflow_id": finished_id, "input": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{resp:?}");
    assert_eq!(run_state(&mut conn, globex).await, "COMPLETED");
}

/// Every `409` a tenant-bound start gets names no execution and no state,
/// and writes one audit row.
#[tokio::test]
async fn tenant_start_conflict_leaks_no_execution() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = standalone_app(&state);
    let acme = mint_for_tenant(&boundary_app(&state), "mutate", "acme").await;

    let mut expected_rows = 0;
    for policy in [
        json!({ "conflict_policy": "fail" }),
        json!({ "reuse_policy": "reject_duplicate" }),
        json!({}),
    ] {
        let shared_id = unique("leak");
        let globex = start_run_for(&mut conn, &shared_id, Some("globex")).await;
        let mut body = json!({ "workflow_id": shared_id, "input": {} });
        body.as_object_mut()
            .unwrap()
            .extend(policy.as_object().unwrap().clone());
        let (status, resp) = send(
            &app,
            Call::new("POST", &format!("/workflows/{WORKFLOW}/start"))
                .bearer(&acme)
                .body(body),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{policy}: {resp:?}");
        let text = resp.to_string();
        assert!(
            !text.contains(&globex.to_string()) && !text.contains("existing"),
            "{policy}: the 409 must not name the run: {resp:?}"
        );
        expected_rows += 1;
        assert_eq!(run_state(&mut conn, globex).await, "RUNNING");
    }
    assert_eq!(deny_summaries(&mut conn).await.len(), expected_rows);
}

/// A keyed start does not replay a key that another tenant committed. The
/// committed-replay probe answers before the engine runs, so the probe must
/// check the tenant itself. A replay it answers writes a `succeeded` start
/// audit row, so that row shows a leaked replay.
#[tokio::test]
async fn a_tenant_start_does_not_replay_another_tenants_idempotency_key() {
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = standalone_app(&state);
    let boundary = boundary_app(&state);
    let globex = mint_for_tenant(&boundary, "mutate", "globex").await;
    let acme = mint_for_tenant(&boundary, "mutate", "acme").await;
    let start = format!("/workflows/{WORKFLOW}/start");
    let key = unique("key");

    let (status, body) = send(
        &app,
        Call::new("POST", &start)
            .bearer(&globex)
            .key(&key)
            .body(json!({ "input": {} })),
    )
    .await;
    assert!(status.is_success(), "{status}: {body:?}");
    let owned = body["execution_id"].as_str().unwrap().to_string();

    // A valid body and a malformed body take different probe paths.
    for request in [json!({ "input": {} }), json!("not a start request")] {
        let (status, resp) = send(
            &app,
            Call::new("POST", &start)
                .bearer(&acme)
                .key(&key)
                .body(request.clone()),
        )
        .await;
        assert!(!status.is_success(), "{request}: {status}: {resp:?}");
        assert!(
            !resp.to_string().contains(&owned),
            "{request}: the answer must not name the run: {resp:?}"
        );
    }

    let replays = diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_audit_log \
         WHERE operation = 'workflow.start' AND status = 'succeeded' \
         AND target_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&owned)
    .get_result::<N>(&mut conn)
    .await
    .unwrap()
    .n;
    assert_eq!(replays, 1, "only the globex start may succeed on its run");
}

/// A bad tenant header from a bound caller is `400`, before any lookup.
#[tokio::test]
async fn bad_tenant_header_from_a_bound_caller_is_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = standalone_app(&state);
    let acme = mint_for_tenant(&boundary_app(&state), "read", "acme").await;
    let own = start_run_for(&mut conn, &unique("acme"), Some("acme")).await;
    let uri = format!("/workflows/{own}");

    let long = "t".repeat(129);
    for values in [vec!["acme", "acme"], vec![" "], vec![long.as_str()]] {
        let mut builder = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("authorization", format!("Bearer {acme}"));
        for v in &values {
            builder = builder.header("x-harvest-tenant", *v);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{values:?}");
    }
}

/// A stored token tenant that breaks the key rule fails closed with `403`.
/// The database check covers only the length, so raw SQL can write one.
#[tokio::test]
async fn an_invalid_stored_token_tenant_fails_closed() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let state = api_state(&pool);
    let app = standalone_app(&state);
    let secret = mint_for_tenant(&boundary_app(&state), "read", "acme").await;
    diesel::sql_query("UPDATE harvest_api_tokens SET tenant = 'ac me'")
        .execute(&mut conn)
        .await
        .unwrap();
    let own = start_run(&mut conn, &unique("acme")).await;

    let (status, body) = send(
        &app,
        Call::new("GET", &format!("/workflows/{own}")).bearer(&secret),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    let summaries = deny_summaries(&mut conn).await;
    assert!(
        summaries
            .iter()
            .any(|s| s.contains("not a valid tenant key")),
        "{summaries:?}"
    );
}
