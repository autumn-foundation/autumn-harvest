//! Integration tests for the admin token scope and the authorizer hook
//! (issue #1803).
//!
//! Tests run against a real Postgres. Set `HARVEST_TEST_DATABASE_URL` to a
//! migrated database and run with `--test-threads=1`. Otherwise a fresh
//! testcontainers Postgres starts with the full migration set.
//!
//! Coverage:
//!   - a `mutate` token gets 403 on token mint and revoke; an `admin` token
//!     gets through (the RED acceptance criterion)
//!   - a custom authorizer denies by tenant key and by shard
//!   - the shard comes from an execution id, a query parameter, or a start body
//!   - the authorizer cannot widen what a token scope allows
//!   - every deny writes an `authz.deny` audit row that the SIEM export claims

#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]

use std::sync::{Arc, Mutex};

use autumn_harvest::audit::RouteClass;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, StandaloneAdminAuth,
    harvest_api_router,
};
use autumn_harvest_plugin::api_token::TokenScope;
use autumn_harvest_plugin::authz::{AuthzDecision, AuthzPrincipal, AuthzRequest};
use autumn_harvest_plugin::harvest_ui_router;
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

fn api_state(pool: &DbPool) -> HarvestApiState {
    api_state_with_router(pool, ShardRouter::default())
}

fn api_state_with_router(pool: &DbPool, router: ShardRouter) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("authz-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    api_state
}

/// The management router with Vantage nested at `/ui`, as `HarvestPlugin`
/// composes it.
fn composed_router(state: &HarvestApiState) -> axum::Router {
    harvest_api_router(state.clone()).nest("/ui", harvest_ui_router(state.clone()))
}

/// The embedder admin boundary plus the token layer. The boundary mints tokens.
fn boundary_app(pool: &DbPool) -> App {
    let state = api_state(pool);
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_admin_auth_boundary()
        .mount(harvest_api_router(state.clone()), &state)
}

/// Tokens are the only auth. No embedder boundary exists.
fn standalone_app(pool: &DbPool) -> App {
    let state = api_state(pool);
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .mount(harvest_api_router(state.clone()), &state)
}

/// The boundary app with an authorizer installed.
fn authorized_app<F>(pool: &DbPool, authorizer: F) -> App
where
    F: Fn(&AuthzRequest<'_>) -> AuthzDecision + Send + Sync + 'static,
{
    authorized_app_with_state(&api_state(pool), authorizer)
}

fn authorized_app_with_state<F>(state: &HarvestApiState, authorizer: F) -> App
where
    F: Fn(&AuthzRequest<'_>) -> AuthzDecision + Send + Sync + 'static,
{
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_admin_auth_boundary()
        .with_authorizer(authorizer)
        .mount(composed_router(state), state)
}

/// A hook that denies shard 7 and allows everything else.
fn deny_shard_7(req: &AuthzRequest<'_>) -> AuthzDecision {
    if req.shard == Some(ShardId::new(7)) {
        AuthzDecision::deny("shard 7 is out of region")
    } else {
        AuthzDecision::Allow
    }
}

/// A hook that records the shard of every call and allows it.
fn recording_app(pool: &DbPool) -> (App, Arc<Mutex<Vec<Option<ShardId>>>>) {
    let seen: Arc<Mutex<Vec<Option<ShardId>>>> = Arc::default();
    let sink = seen.clone();
    let app = authorized_app(pool, move |req| {
        sink.lock().unwrap().push(req.shard);
        AuthzDecision::Allow
    });
    (app, seen)
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
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
    form: Option<&'a str>,
    bearer: Option<&'a str>,
    tenant: Option<&'a str>,
}

impl<'a> Call<'a> {
    const fn new(method: &'a str, uri: &'a str) -> Self {
        Self {
            method,
            uri,
            body: None,
            form: None,
            bearer: None,
            tenant: None,
        }
    }

    const fn form(mut self, form: &'a str) -> Self {
        self.form = Some(form);
        self
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
}

async fn send(app: &App, call: Call<'_>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(call.method).uri(call.uri);
    if call.body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if call.form.is_some() {
        builder = builder.header("content-type", "application/x-www-form-urlencoded");
    }
    if let Some(b) = call.bearer {
        builder = builder.header("authorization", format!("Bearer {b}"));
    }
    if let Some(t) = call.tenant {
        builder = builder.header("x-harvest-tenant", t);
    }
    let body = match (call.body, call.form) {
        (Some(json), _) => Body::from(json.to_string()),
        (None, Some(form)) => Body::from(form.to_string()),
        (None, None) => Body::empty(),
    };
    let req = builder.body(body).unwrap();
    let response = app.clone().oneshot(req).await.expect("request");
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

/// Mint a token through the boundary app. Returns `(id, secret)`.
async fn mint(app: &App, name: &str, scope: &str) -> (String, String) {
    let (status, resp) = send(
        app,
        Call::new("POST", "/admin/tokens").body(json!({ "name": name, "scope": scope })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "mint should 201: {resp:?}");
    (
        resp["id"].as_str().expect("id").to_string(),
        resp["secret"].as_str().expect("secret").to_string(),
    )
}

#[derive(diesel::QueryableByName, Debug)]
struct DenyRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    actor: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    route_or_command: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error_summary: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    shard_id: Option<i32>,
}

async fn count_tokens(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct C {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_api_tokens")
        .get_result::<C>(conn)
        .await
        .unwrap()
        .n
}

async fn deny_rows(conn: &mut AsyncPgConnection) -> Vec<DenyRow> {
    diesel::sql_query(
        "SELECT actor, status, route_or_command, error_summary, shard_id \
         FROM harvest_audit_log WHERE operation = 'authz.deny' ORDER BY occurred_at",
    )
    .load(conn)
    .await
    .unwrap()
}

// ── Admin scope ───────────────────────────────────────────────────────────────

/// RED acceptance criterion: a `mutate` token cannot mint or revoke tokens.
#[tokio::test]
async fn mutate_token_cannot_mint_or_revoke_tokens() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let boundary = boundary_app(&pool);
    let standalone = standalone_app(&pool);

    let (mutate_id, mutate) = mint(&boundary, "ci", "mutate").await;
    let (victim_id, victim) = mint(&boundary, "victim", "read").await;
    let tokens_before = count_tokens(&mut conn).await;

    let (status, body) = send(
        &standalone,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "child", "scope": "mutate" }))
            .bearer(&mutate),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "mutate mint: {body:?}");

    let (status, body) = send(
        &standalone,
        Call::new("DELETE", &format!("/admin/tokens/{victim_id}")).bearer(&mutate),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "mutate revoke: {body:?}");

    // A deny has no side effect: no child token, and the victim still works.
    assert_eq!(count_tokens(&mut conn).await, tokens_before);
    let (status, _) = send(&standalone, Call::new("GET", "/workflows").bearer(&victim)).await;
    assert_ne!(
        status,
        StatusCode::UNAUTHORIZED,
        "the victim token is still live"
    );

    // A `mutate` token still reaches an ordinary mutation.
    let (status, _) = send(
        &standalone,
        Call::new("POST", "/workflows/some-wf/cancel")
            .body(json!({}))
            .bearer(&mutate),
    )
    .await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
    assert_ne!(status, StatusCode::FORBIDDEN);

    // Each deny is audited against the token, never the secret.
    let rows = deny_rows(&mut conn).await;
    assert_eq!(rows.len(), 2, "two denies must be audited: {rows:?}");
    for row in &rows {
        assert_eq!(row.actor, format!("token:{mutate_id}"));
        assert_eq!(row.status, "failed");
        assert!(!row.actor.contains(&mutate));
    }
    assert!(
        rows.iter()
            .any(|r| r.route_or_command == "POST /admin/tokens")
    );
    assert!(
        rows.iter()
            .any(|r| r.route_or_command == format!("DELETE /admin/tokens/{victim_id}"))
    );
}

/// An `admin` token mints and revokes with no embedder boundary.
#[tokio::test]
async fn admin_token_mints_and_revokes_tokens() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let boundary = boundary_app(&pool);
    let standalone = standalone_app(&pool);

    let (_, admin) = mint(&boundary, "root", "admin").await;

    let (status, child) = send(
        &standalone,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "child", "scope": "admin" }))
            .bearer(&admin),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "admin mint: {child:?}");
    assert_eq!(child["scope"], "admin");

    let child_id = child["id"].as_str().unwrap();
    let (status, body) = send(
        &standalone,
        Call::new("DELETE", &format!("/admin/tokens/{child_id}")).bearer(&admin),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "admin revoke: {body:?}");
    assert_eq!(body["revoked"], true);
    let rows = deny_rows(&mut conn).await;
    assert_eq!(rows.len(), 0, "{rows:?}");
}

/// A `read` token denied a mutation is audited too.
#[tokio::test]
async fn read_token_deny_is_audited() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let boundary = boundary_app(&pool);

    let (read_id, read) = mint(&boundary, "dash", "read").await;
    let (status, _) = send(
        &boundary,
        Call::new("POST", "/workflows/some-wf/cancel")
            .body(json!({}))
            .bearer(&read),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let rows = deny_rows(&mut conn).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].actor, format!("token:{read_id}"));
}

// ── Authorizer hook ───────────────────────────────────────────────────────────

/// A custom authorizer denies by tenant key.
#[tokio::test]
async fn authorizer_denies_by_tenant_key() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = authorized_app(&pool, |req| {
        if req.tenant_key == Some("globex") {
            AuthzDecision::deny("tenant globex is closed")
        } else {
            AuthzDecision::Allow
        }
    });

    let (status, body) = send(&app, Call::new("GET", "/workflows").tenant("globex")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    // The caller never sees the policy reason.
    assert!(!body.to_string().contains("closed"), "{body:?}");

    let (status, _) = send(&app, Call::new("GET", "/workflows").tenant("acme")).await;
    assert_ne!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(&app, Call::new("GET", "/workflows")).await;
    assert_ne!(status, StatusCode::FORBIDDEN);

    let rows = deny_rows(&mut conn).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let summary = rows[0].error_summary.as_deref().unwrap_or_default();
    assert!(summary.contains("tenant globex is closed"), "{summary}");
    assert!(summary.contains(r#"tenant="globex""#), "{summary}");
    assert_eq!(rows[0].route_or_command, "GET /workflows");
}

/// A tenant header that is too long is rejected before the authorizer runs.
#[tokio::test]
async fn oversized_tenant_header_is_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = authorized_app(&pool, |_| AuthzDecision::Allow);
    let long = "t".repeat(129);
    let (status, _) = send(&app, Call::new("GET", "/workflows").tenant(&long)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Percent-encode the first character of an id, as a client may.
fn encode_first_char(id: &str) -> String {
    let mut chars = id.chars();
    let first = chars.next().expect("non-empty id");
    format!("%{:02X}{}", u32::from(first), chars.as_str())
}

/// A custom authorizer denies by shard. The shard comes from the execution id
/// in the path, a `shard_id` query parameter, or a start body.
#[tokio::test]
async fn authorizer_denies_by_shard() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = authorized_app(&pool, deny_shard_7);

    let on_7 = ExecutionId::new_for_shard(ShardId::new(7));
    let on_3 = ExecutionId::new_for_shard(ShardId::new(3));

    let (status, _) = send(&app, Call::new("GET", &format!("/workflows/{on_7}"))).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "exec id on shard 7");
    let (status, _) = send(&app, Call::new("GET", &format!("/workflows/{on_3}"))).await;
    assert_ne!(status, StatusCode::FORBIDDEN, "exec id on shard 3");

    // The handler decodes the path, so the hook must too.
    let encoded = encode_first_char(&on_7.to_string());
    let (status, _) = send(
        &app,
        Call::new("POST", &format!("/workflows/{encoded}/cancel")).body(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "percent-encoded exec id");

    // HEAD is served by the GET handler.
    let (status, _) = send(&app, Call::new("HEAD", &format!("/workflows/{on_7}"))).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "HEAD on shard 7");

    // A query shard on a route that scopes by it.
    let (status, _) = send(&app, Call::new("GET", "/admin/history/exports?shard_id=7")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "query shard_id=7");
    let (status, _) = send(&app, Call::new("GET", "/admin/history/exports?shard_id=3")).await;
    assert_ne!(status, StatusCode::FORBIDDEN, "query shard_id=3");
    let (status, _) = send(
        &app,
        Call::new("GET", "/admin/external-handoffs?shard=3&shard=7"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "every named shard is checked"
    );

    let (status, _) = send(
        &app,
        Call::new("POST", "/workflows/some-wf/start").body(json!({ "shard_id": 7 })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "start pinned to shard 7");
    // The handler gets the rebuilt body. A field of the wrong type proves it:
    // the handler's JSON extractor answers 422, where an empty body gives 400.
    let (status, body) = send(
        &app,
        Call::new("POST", "/workflows/some-wf/start")
            .body(json!({ "shard_id": 3, "reuse_policy": 5 })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "the handler must see the start body: {body:?}"
    );

    // Ids of other kinds never decode to a shard.
    let (status, _) = send(&app, Call::new("DELETE", &format!("/admin/tokens/{on_7}"))).await;
    assert_ne!(
        status,
        StatusCode::FORBIDDEN,
        "a token id is not an exec id"
    );

    let rows = deny_rows(&mut conn).await;
    assert_eq!(rows.len(), 6, "{rows:?}");
    assert!(rows.iter().all(|r| r.shard_id == Some(7)), "{rows:?}");
}

/// A query shard on a route that ignores it must not reach the hook. The
/// handler reads every shard, so the hook sees `None`.
#[tokio::test]
async fn query_shard_on_a_route_that_ignores_it_is_none() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let (app, seen) = recording_app(&pool);

    let (status, _) = send(&app, Call::new("GET", "/workflows?shard_id=7")).await;
    assert_ne!(status, StatusCode::FORBIDDEN);
    assert_eq!(seen.lock().unwrap().clone(), vec![None]);
}

/// The nested `/ui` routes carry the shard of their execution id.
#[tokio::test]
async fn ui_execution_routes_carry_the_shard() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = authorized_app(&pool, deny_shard_7);

    let on_7 = ExecutionId::new_for_shard(ShardId::new(7));
    let on_3 = ExecutionId::new_for_shard(ShardId::new(3));
    let (status, _) = send(
        &app,
        Call::new("POST", &format!("/ui/workflows/{on_7}/cancel")),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "UI cancel on shard 7");
    let (status, _) = send(&app, Call::new("GET", &format!("/ui/workflows/{on_7}"))).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "UI detail on shard 7");
    let (status, _) = send(
        &app,
        Call::new("POST", &format!("/ui/dags/nightly/runs/{on_7}/retry")),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "UI DAG retry on shard 7");
    let (status, _) = send(&app, Call::new("GET", &format!("/ui/workflows/{on_3}"))).await;
    assert_ne!(status, StatusCode::FORBIDDEN, "UI detail on shard 3");
    assert_eq!(deny_rows(&mut conn).await.len(), 3);
}

/// Mutations that name a shard in the body carry it to the hook.
#[tokio::test]
async fn body_shard_routes_carry_the_shard() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = authorized_app(&pool, deny_shard_7);

    for (uri, body) in [
        (
            "/admin/queues/q/pause",
            json!({ "shard_id": 7, "reason": "drill" }),
        ),
        (
            "/admin/queues/q/resume",
            json!({ "shard_id": 7, "reason": "drill" }),
        ),
        ("/dlq/redrive", json!({ "shard_id": 7 })),
        ("/dead-letters/replay", json!({ "shard_id": 7 })),
        ("/admin/audit-export/decommission", json!({ "shard": 7 })),
        ("/admin/audit-export/reactivate", json!({ "shard": 7 })),
        (
            "/admin/audit-export/redrive",
            json!({ "shard": 7, "to_seq": 0 }),
        ),
    ] {
        let (status, resp) = send(&app, Call::new("POST", uri).body(body)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {resp:?}");
    }
    // The bulk DLQ routes also take a form body.
    let (status, _) = send(
        &app,
        Call::new("POST", "/dead-letters/discard").form("shard_id=7&dry_run=true"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "form shard_id=7");

    // No shard in the body means every shard, so the hook sees `None`.
    let (status, _) = send(
        &app,
        Call::new("POST", "/admin/queues/q/pause").body(json!({ "reason": "drill" })),
    )
    .await;
    assert_ne!(status, StatusCode::FORBIDDEN, "fleet-wide pause");
    assert_eq!(deny_rows(&mut conn).await.len(), 8);
}

/// A start body `residency_key` resolves through the shard router.
#[tokio::test]
async fn residency_key_start_resolves_the_shard() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let shards = vec![ShardId::new(0), ShardId::new(7)];
    let router = ShardRouter::new(shards.clone(), shards, ShardId::new(0))
        .with_residency_map([("eu".to_string(), ShardId::new(7))]);
    let app = authorized_app_with_state(&api_state_with_router(&pool, router), deny_shard_7);

    let (status, _) = send(
        &app,
        Call::new("POST", "/workflows/some-wf/start").body(json!({ "residency_key": " eu " })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "eu resolves to shard 7");
    let (status, _) = send(
        &app,
        Call::new("POST", "/workflows/some-wf/start").body(json!({ "residency_key": "us" })),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::FORBIDDEN,
        "undeclared key: the handler decides"
    );
    let rows = deny_rows(&mut conn).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].shard_id, Some(7));
}

/// Repeated or malformed tenant headers are rejected. OPTIONS skips the hook.
#[tokio::test]
async fn tenant_header_edge_cases() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let (app, seen) = recording_app(&pool);

    let req = Request::builder()
        .method("GET")
        .uri("/workflows")
        .header("x-harvest-tenant", "acme")
        .header("x-harvest-tenant", "globex")
        .body(Body::empty())
        .unwrap();
    let status = app.clone().oneshot(req).await.unwrap().status();
    assert_eq!(status, StatusCode::BAD_REQUEST, "two tenant headers");

    let (status, _) = send(&app, Call::new("GET", "/workflows").tenant("   ")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "blank tenant");
    let exact = "t".repeat(128);
    let (status, _) = send(&app, Call::new("GET", "/workflows").tenant(&exact)).await;
    assert_ne!(status, StatusCode::BAD_REQUEST, "128 bytes is accepted");

    let calls_before = seen.lock().unwrap().len();
    let (status, _) = send(&app, Call::new("OPTIONS", "/workflows").tenant("   ")).await;
    assert_ne!(status, StatusCode::BAD_REQUEST, "OPTIONS skips the hook");
    assert_eq!(seen.lock().unwrap().len(), calls_before);
}

/// With no audit store, a deny is still a deny.
#[tokio::test]
async fn deny_without_a_storage_pool_is_still_403() {
    let state = HarvestApiState::new();
    let app = StandaloneAdminAuth::new()
        .with_admin_auth_boundary()
        .with_authorizer(|_: &AuthzRequest<'_>| AuthzDecision::deny("closed"))
        .mount(harvest_api_router(state.clone()), &state);
    let (status, body) = send(&app, Call::new("GET", "/workflows")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
}

/// A body over the request limit is refused before the hook runs.
#[tokio::test]
async fn oversized_start_body_is_413() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let (app, seen) = recording_app(&pool);
    let big = json!({ "input": "x".repeat(2 * 1024 * 1024 + 1) });
    let (status, _) = send(
        &app,
        Call::new("POST", "/workflows/some-wf/start").body(big),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(seen.lock().unwrap().len(), 0);
}

/// A `mutate` token is denied exactly the admin-only routes. An `admin` token
/// is denied nothing.
#[tokio::test]
async fn mutate_token_403_exactly_on_admin_scope_routes() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = boundary_app(&pool);
    let (_, mutate) = mint(&app, "ci", "mutate").await;
    let (_, admin) = mint(&app, "root", "admin").await;

    for (template, _) in autumn_harvest::audit::CLASSIFIED_ROUTES {
        let Some((method, tmpl)) = template.split_once(' ') else {
            continue;
        };
        let path: String = tmpl
            .split('/')
            .map(|seg| {
                if seg.starts_with('{') && seg.ends_with('}') {
                    "x"
                } else {
                    seg
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        let admin_only = autumn_harvest::audit::ADMIN_SCOPE_ROUTES.contains(template);
        let (status, _) = send(&app, Call::new(method, &path).bearer(&mutate)).await;
        assert_eq!(
            status == StatusCode::FORBIDDEN,
            admin_only,
            "mutate token on {template}: {status}"
        );
        let (status, _) = send(&app, Call::new(method, &path).bearer(&admin)).await;
        assert_ne!(status, StatusCode::FORBIDDEN, "admin token on {template}");
    }
}

/// The authorizer sees the verified token principal and the route class.
#[tokio::test]
async fn authorizer_sees_token_principal_and_route_class() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let boundary = boundary_app(&pool);
    let (read_id, read) = mint(&boundary, "dash", "read").await;

    let seen: Arc<Mutex<Vec<(AuthzPrincipal, RouteClass)>>> = Arc::default();
    let sink = seen.clone();
    let app = authorized_app(&pool, move |req| {
        sink.lock().unwrap().push((req.principal, req.route_class));
        AuthzDecision::Allow
    });

    let (status, _) = send(&app, Call::new("GET", "/workflows").bearer(&read)).await;
    assert_ne!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(&app, Call::new("GET", "/workflows")).await;
    assert_ne!(status, StatusCode::FORBIDDEN);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    match seen[0].0 {
        AuthzPrincipal::Token { id, scope } => {
            assert_eq!(id.to_string(), read_id);
            assert_eq!(scope, TokenScope::Read);
        }
        other => panic!("expected a token principal, got {other:?}"),
    }
    assert_eq!(seen[0].1, RouteClass::ReadOnly);
    assert!(matches!(seen[1].0, AuthzPrincipal::Embedder));
}

/// An allow-all authorizer cannot widen a token scope.
#[tokio::test]
async fn authorizer_cannot_widen_a_token_scope() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let boundary = boundary_app(&pool);
    let (_, read) = mint(&boundary, "dash", "read").await;
    let (_, mutate) = mint(&boundary, "ci", "mutate").await;

    let app = authorized_app(&pool, |_| AuthzDecision::Allow);
    let (status, _) = send(
        &app,
        Call::new("POST", "/workflows/some-wf/cancel")
            .body(json!({}))
            .bearer(&read),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(
        &app,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "x", "scope": "read" }))
            .bearer(&mutate),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Deny rows reach the SIEM export like any other audit row.
#[tokio::test]
async fn deny_rows_reach_the_siem_export() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = authorized_app(&pool, |req| {
        if req.tenant_key == Some("globex") {
            AuthzDecision::deny("tenant globex is closed")
        } else {
            AuthzDecision::Allow
        }
    });
    let (status, _) = send(&app, Call::new("GET", "/workflows").tenant("globex")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    autumn_harvest::audit_export::ensure_cursor_row(&mut conn, 0)
        .await
        .expect("cursor row");
    let claim = autumn_harvest::audit_export::claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(60),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("the deny row is exportable");
    let deny = claim
        .records
        .iter()
        .find(|r| r.operation == "authz.deny")
        .expect("the export batch carries the deny");
    assert_eq!(deny.status, "failed");
    let wire = autumn_harvest::audit_export::serialize_batch(&claim.records).expect("serialize");
    assert!(String::from_utf8_lossy(&wire).contains("authz.deny"));
}
