//! HTTP integration tests for the sampling policy routes (issue #2439).
//!
//! `GET`, `PUT` and `DELETE /admin/agents/{kind}/sampling-policy` read, set
//! and reset the sampling policy of one agent kind. The tests drive the real
//! router behind the role and token layers, as a standalone mount in the
//! `prod` profile.
//!
//! The acceptance line of #2439 is here: a set without the operator role
//! returns 403, and the stored policy is unchanged.
//!
//! With `HARVEST_TEST_DATABASE_URL` set, each test creates and migrates its
//! own database on that server. Otherwise a testcontainers Postgres starts.

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_agent::sampling::{
    PostgresSamplingPolicyStore, SamplingPolicy, SamplingPolicyStore,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, StandaloneAdminAuth,
    harvest_api_router,
};
use autumn_harvest_plugin::roles::{HarvestRoles, ROLE_OPERATOR, ROLE_VIEWER, RoleGrant};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

type App = axum::Router;

/// One caller of a refused write: its app, its headers and its name.
type Attempt<'a> = (&'a App, &'a [(&'a str, &'a str)], &'a str);

const KIND: &str = "researcher";
const ACTOR: &str = "ops-alice";
const OP_SET: &str = "agent.sampling_policy.set";
const OP_RESET: &str = "agent.sampling_policy.reset";

/// A migrated database. The container, if any, lives as long as the test.
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (fresh_database(&url).await, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// Create and migrate a new database next to the database at `url`.
async fn fresh_database(url: &str) -> String {
    let mut admin = AsyncPgConnection::establish(url).await.expect("connect");
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let prefix = base.rsplit_once('/').map_or(base, |(prefix, _)| prefix);
    let name = format!("harvest_sampling_{}", Uuid::new_v4().simple());
    diesel::sql_query(format!("CREATE DATABASE {name}"))
        .execute(&mut admin)
        .await
        .expect("create database");
    let fresh = query.map_or_else(
        || format!("{prefix}/{name}"),
        |q| format!("{prefix}/{name}?{q}"),
    );
    let mut conn = AsyncPgConnection::establish(&fresh)
        .await
        .expect("connect fresh database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrate fresh database");
    fresh
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn api_state(pool: &DbPool) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("sampling-policy-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

/// The router behind the role layer, in the `prod` profile with no auth
/// boundary. Host middleware grants `role` to each request.
fn role_app(state: &HarvestApiState, role: &'static str) -> App {
    let roles = HarvestRoles::builder()
        .builtin_roles()
        .build()
        .expect("valid roles");
    StandaloneAdminAuth::new()
        .with_deployment_profile("prod")
        .with_roles(roles)
        .mount(harvest_api_router(state.clone()), state)
        .layer(axum::middleware::from_fn(
            move |mut request: Request, next: Next| async move {
                request.extensions_mut().insert(RoleGrant::new([role]));
                next.run(request).await
            },
        ))
}

/// The embedder boundary plus the token layer. It mints tokens.
fn boundary_app(state: &HarvestApiState) -> App {
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_admin_auth_boundary()
        .mount(harvest_api_router(state.clone()), state)
}

/// Tokens are the only auth.
fn token_app(state: &HarvestApiState) -> App {
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .mount(harvest_api_router(state.clone()), state)
}

async fn send(
    app: &App,
    method: &str,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("x-harvest-actor", ACTOR);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, json)
}

fn route(kind: &str) -> String {
    format!("/admin/agents/{kind}/sampling-policy")
}

/// Mint a token through the boundary app. Returns the secret.
async fn mint(app: &App, name: &str, scope: &str) -> String {
    let (status, body) = send(
        app,
        "POST",
        "/admin/tokens",
        Some(json!({ "name": name, "scope": scope })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "mint: {body}");
    body["secret"].as_str().expect("secret").to_owned()
}

/// A policy with each setting away from its default.
fn wide_policy() -> Value {
    json!({
        "strategies": ["best_of_n", "vote"],
        "max_draws": 4,
        "max_concurrent_draws": 2,
        "draw_effects": ["read_only", "write"],
        "scorers": {"only": ["exact_match"]},
        "judge_models": ["small-judge"],
        "max_answer_retries": 3,
    })
}

fn default_policy() -> Value {
    serde_json::to_value(SamplingPolicy::default()).expect("encode the default")
}

#[derive(diesel::QueryableByName, Debug, PartialEq, Eq)]
struct AuditLogRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    operation: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    actor: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    route_or_command: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error_summary: Option<String>,
}

/// The `harvest_audit_log` rows of the sampling policy of `kind`, oldest
/// first.
async fn audit_log(conn: &mut AsyncPgConnection, kind: &str) -> Vec<AuditLogRow> {
    diesel::sql_query(
        "SELECT operation, status, actor, route_or_command, error_summary \
         FROM harvest_audit_log \
         WHERE target_type = 'agent_kind' AND target_id = $1 \
         ORDER BY occurred_at, id",
    )
    .bind::<diesel::sql_types::Text, _>(kind)
    .load(conn)
    .await
    .expect("audit log rows")
}

/// The number of `harvest_agent_policy_audit` rows of `kind`.
async fn policy_audit_count(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_agent_policy_audit WHERE kind = $1")
        .bind::<diesel::sql_types::Text, _>(KIND)
        .get_result::<Count>(conn)
        .await
        .expect("count policy audit rows")
        .n
}

/// The stored policy, read through the store as a strategy run reads it.
async fn stored(pool: &DbPool) -> (Value, u64) {
    let read = PostgresSamplingPolicyStore::new(pool.clone())
        .read(KIND)
        .await
        .expect("read the stored policy");
    (
        serde_json::to_value(read.policy).expect("encode"),
        read.revision,
    )
}

/// AC: an operator sets the policy. The store and `harvest_audit_log` both
/// record the change.
#[tokio::test]
async fn an_operator_sets_the_policy_and_each_audit_records_it() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let state = api_state(&pool);
    let operator = role_app(&state, ROLE_OPERATOR);

    let (status, body) = send(
        &operator,
        "PUT",
        &route(KIND),
        Some(json!({ "policy": wide_policy() })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], json!(KIND));
    assert_eq!(body["revision"], json!(1));
    assert_eq!(body["old"], Value::Null);
    assert_eq!(body["new"], wide_policy());

    let (status, body) = send(&operator, "GET", &route(KIND), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], json!(1));
    assert_eq!(body["policy"], wide_policy());
    let audit = body["audit"].as_array().expect("audit rows");
    assert_eq!(audit.len(), 1, "{body}");
    assert_eq!(audit[0]["revision"], json!(1));
    assert_eq!(audit[0]["action"], json!("set"));
    assert_eq!(audit[0]["actor"], json!(ACTOR));
    assert_eq!(audit[0]["old"], Value::Null);
    assert_eq!(audit[0]["new"], wide_policy());

    // A strategy run reads the same policy through the store.
    assert_eq!(stored(&pool).await, (wide_policy(), 1));

    let mut conn = pool.get().await.expect("connection");
    assert_eq!(
        audit_log(&mut conn, KIND).await,
        vec![AuditLogRow {
            operation: OP_SET.to_owned(),
            status: "succeeded".to_owned(),
            actor: ACTOR.to_owned(),
            route_or_command: "PUT /admin/agents/{kind}/sampling-policy".to_owned(),
            error_summary: Some("revision 1".to_owned()),
        }]
    );
}

/// AC (the acceptance line of #2439): a set or a reset without the operator
/// role returns 403, and the stored policy is unchanged.
#[tokio::test]
async fn a_viewer_or_a_read_token_cannot_set_or_reset_the_policy() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let state = api_state(&pool);
    let operator = role_app(&state, ROLE_OPERATOR);
    let viewer = role_app(&state, ROLE_VIEWER);
    let read = mint(&boundary_app(&state), "dash", "read").await;
    let tokens = token_app(&state);
    let bearer = format!("Bearer {read}");

    let (status, body) = send(
        &operator,
        "PUT",
        &route(KIND),
        Some(json!({ "policy": wide_policy() })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut conn = pool.get().await.expect("connection");
    let log_before = audit_log(&mut conn, KIND).await;

    let narrow = json!({ "policy": { "max_draws": 1 } });
    let attempts: [Attempt<'_>; 2] = [
        (&viewer, &[], "viewer role"),
        (&tokens, &[("authorization", bearer.as_str())], "read token"),
    ];
    for (app, headers, who) in attempts {
        let (status, body) = send(app, "PUT", &route(KIND), Some(narrow.clone()), headers).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{who} PUT: {body}");
        let (status, body) = send(app, "DELETE", &route(KIND), None, headers).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{who} DELETE: {body}");

        // A read is still allowed, and shows the policy of the operator.
        let (status, body) = send(app, "GET", &route(KIND), None, headers).await;
        assert_eq!(status, StatusCode::OK, "{who} GET: {body}");
        assert_eq!(body["revision"], json!(1), "{who}");
        assert_eq!(body["policy"], wide_policy(), "{who}");
    }

    // Nothing changed: the policy, its revision and both audit trails.
    assert_eq!(stored(&pool).await, (wide_policy(), 1));
    assert_eq!(policy_audit_count(&mut conn).await, 1);
    assert_eq!(audit_log(&mut conn, KIND).await, log_before);
}

/// AC: an operator reset restores every default, keeps the revision going,
/// and is audited.
#[tokio::test]
async fn an_operator_reset_restores_every_default_and_is_audited() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let state = api_state(&pool);
    let operator = role_app(&state, ROLE_OPERATOR);

    let (status, body) = send(
        &operator,
        "PUT",
        &route(KIND),
        Some(json!({ "policy": wide_policy() })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = send(&operator, "DELETE", &route(KIND), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], json!(KIND));
    assert_eq!(body["revision"], json!(2));
    assert_eq!(body["old"], wide_policy());
    assert_eq!(body["new"], Value::Null);

    let (status, body) = send(&operator, "GET", &route(KIND), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], json!(2));
    assert_eq!(body["policy"], default_policy());
    let audit = body["audit"].as_array().expect("audit rows");
    assert_eq!(audit.len(), 2, "{body}");
    assert_eq!(audit[0]["action"], json!("reset"));
    assert_eq!(audit[0]["old"], wide_policy());
    assert_eq!(audit[0]["new"], Value::Null);
    assert_eq!(audit[1]["action"], json!("set"));

    assert_eq!(stored(&pool).await, (default_policy(), 2));
    let mut conn = pool.get().await.expect("connection");
    let log = audit_log(&mut conn, KIND).await;
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!(log[1].operation, OP_RESET);
    assert_eq!(log[1].status, "succeeded");
    assert_eq!(
        log[1].route_or_command,
        "DELETE /admin/agents/{kind}/sampling-policy"
    );
    assert_eq!(log[1].error_summary.as_deref(), Some("revision 2"));
}

/// AC: a body that does not decode or validate is an audited 400, and
/// nothing is stored.
#[tokio::test]
async fn a_bad_body_is_an_audited_400_and_stores_nothing() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let state = api_state(&pool);
    let operator = role_app(&state, ROLE_OPERATOR);

    for body in [
        json!({ "policy": { "max_drawz": 2 } }),
        json!({ "policy": { "max_draws": 0 } }),
        json!({ "policy": { "max_concurrent_draws": 17 } }),
        json!({ "policy": { "strategies": ["guess"] } }),
        json!({ "max_draws": 2 }),
        json!({ "policy": {}, "reason": "x" }),
    ] {
        let (status, answer) = send(&operator, "PUT", &route(KIND), Some(body.clone()), &[]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {answer}");
    }

    let (status, body) = send(&operator, "GET", &route(KIND), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], json!(0));
    assert_eq!(body["policy"], default_policy());
    assert_eq!(body["audit"], json!([]));

    let mut conn = pool.get().await.expect("connection");
    assert_eq!(policy_audit_count(&mut conn).await, 0);
    let log = audit_log(&mut conn, KIND).await;
    assert_eq!(log.len(), 6, "{log:?}");
    for row in &log {
        assert_eq!(row.operation, OP_SET);
        assert_eq!(row.status, "failed");
        assert!(row.error_summary.is_some(), "{row:?}");
    }
}

/// A kind that is not a valid agent kind is a 400 on each route.
#[tokio::test]
async fn an_invalid_kind_is_a_400() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let state = api_state(&pool);
    let operator = role_app(&state, ROLE_OPERATOR);
    let bad = "Not%20A%20Kind";

    let (status, body) = send(&operator, "GET", &route(bad), None, &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = send(
        &operator,
        "PUT",
        &route(bad),
        Some(json!({ "policy": {} })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = send(&operator, "DELETE", &route(bad), None, &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let mut conn = pool.get().await.expect("connection");
    let log = audit_log(&mut conn, "Not A Kind").await;
    assert_eq!(log.len(), 2, "{log:?}");
    assert!(log.iter().all(|row| row.status == "failed"), "{log:?}");
}
