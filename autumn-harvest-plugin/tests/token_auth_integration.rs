//! Integration tests for scoped API tokens + rotation (issue #942).
//!
//! Tests run against a real Postgres (set `HARVEST_TEST_DATABASE_URL` to a
//! migrated database to run directly with `--test-threads=1`, otherwise a fresh
//! testcontainers Postgres is booted with the full migration set).
//!
//! End-to-end coverage of the management token layer:
//!   - create returns the secret exactly once; only the hash is stored (AC2)
//!   - `GET /admin/tokens` returns metadata only, never the hash/secret (AC2)
//!   - a minted `hvst_` bearer authenticates a read route (AC1)
//!   - revocation and expiry are effective on the next request (AC5)
//!   - a `read` token gets 403 on 100% of mutating routes and reaches read-only
//!     routes (AC3/AC4, the 100%/0% success-metric sweep)
//!   - a `mutate` token reaches an admin-gated mutation without an embedder
//!     boundary (AC7/D3, the `require_admin` composition)
//!   - a token-authed mutation writes an audit row whose actor is `token:{id}`,
//!     never the secret/hash, and a client `x-harvest-actor` cannot override it
//!     (AC6)
//!   - an unknown `hvst_` bearer is rejected 401 (AC5/AC7)

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

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
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn api_state(pool: &DbPool, admin_boundary: bool) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    if admin_boundary {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("token-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

/// App with the embedder admin boundary set (mints/verifies under boundary) AND
/// the token scope layer installed. This is the `api_with_auth` + tokens
/// composition.
fn build_app_boundary(pool: &DbPool) -> HarvestApiApp {
    let state = api_state(pool, true);
    harvest_api_router(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state,
            autumn_harvest_plugin::api_token::enforce_token_scope,
        ))
        .with_state(AppState::for_test().with_profile("test"))
}

/// App with NO embedder boundary and the token scope layer installed. This is
/// the standalone-token mode: a verified `mutate` token must satisfy
/// `require_admin` via the `TokenPrincipal` extension.
fn build_app_standalone(pool: &DbPool) -> HarvestApiApp {
    let state = api_state(pool, false);
    harvest_api_router(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state,
            autumn_harvest_plugin::api_token::enforce_token_scope,
        ))
        .with_state(AppState::for_test().with_profile("test"))
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_api_tokens",
        "DELETE FROM harvest_audit_log",
    ] {
        let _ = diesel::sql_query(stmt).execute(conn).await;
    }
}

/// Send a request. If `bearer` is set, it is sent as `Authorization: Bearer`;
/// if `admin` a `x-harvest-admin: true` header is added; `actor` (if set) is
/// sent as a (spoofable) `x-harvest-actor` header.
async fn send(
    app: &HarvestApiApp,
    method: &str,
    uri: &str,
    body: Option<Value>,
    bearer: Option<&str>,
    admin: bool,
    actor: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if admin {
        builder = builder.header("x-harvest-admin", "true");
    }
    if let Some(b) = bearer {
        builder = builder.header("authorization", format!("Bearer {b}"));
    }
    if let Some(a) = actor {
        builder = builder.header("x-harvest-actor", a);
    }
    let req = builder
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .unwrap();
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

/// Mint a token via the boundary app and return its plaintext secret.
async fn mint(app: &HarvestApiApp, name: &str, scope: &str, expires_at: Option<&str>) -> String {
    let mut body = json!({ "name": name, "scope": scope });
    if let Some(e) = expires_at {
        body["expires_at"] = json!(e);
    }
    let (status, resp) = send(app, "POST", "/admin/tokens", Some(body), None, true, None).await;
    assert_eq!(status, StatusCode::CREATED, "mint should 201: {resp:?}");
    resp["secret"]
        .as_str()
        .expect("mint response carries a secret")
        .to_string()
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

#[tokio::test]
async fn create_returns_secret_once_and_stores_only_hash() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    let (status, resp) = send(
        &app,
        "POST",
        "/admin/tokens",
        Some(json!({ "name": "ci-bot", "scope": "read" })),
        None,
        true,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "resp: {resp:?}");
    let secret = resp["secret"].as_str().expect("secret in create response");
    assert!(secret.starts_with("hvst_"), "secret must be hvst_-prefixed");
    assert_eq!(resp["scope"], "read");
    assert_eq!(resp["name"], "ci-bot");

    // Only the hash is stored; the plaintext secret is nowhere in the DB.
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        token_hash: String,
    }
    let rows: Vec<Row> = diesel::sql_query("SELECT token_hash FROM harvest_api_tokens")
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_ne!(rows[0].token_hash, secret, "hash must not equal the secret");
    assert!(
        !rows[0].token_hash.contains(secret),
        "stored hash must not contain the plaintext"
    );
}

#[tokio::test]
async fn list_returns_metadata_only() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    let secret = mint(&app, "dash", "read", None).await;

    let (status, resp) = send(&app, "GET", "/admin/tokens", None, None, true, None).await;
    assert_eq!(status, StatusCode::OK);
    let body = serde_json::to_string(&resp).unwrap();
    assert!(!body.contains("token_hash"), "list leaked token_hash");
    assert!(!body.contains(&secret), "list leaked the plaintext secret");
    assert!(body.contains("\"name\":\"dash\""));
    assert!(body.contains("\"scope\":\"read\""));
    // metadata fields present
    assert!(body.contains("created_at"));
    assert!(body.contains("last_used_at"));
}

#[tokio::test]
async fn verify_with_secret_authenticates_a_read_route() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    let secret = mint(&app, "ci", "read", None).await;
    // A read route with the minted bearer authenticates and is not denied.
    let (status, _resp) = send(&app, "GET", "/workflows", None, Some(&secret), false, None).await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
    assert_ne!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn unknown_hvst_bearer_is_401() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    let (status, _resp) = send(
        &app,
        "GET",
        "/workflows",
        None,
        Some("hvst_thisdoesnotexist"),
        false,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn revoke_denies_next_request() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    let secret = mint(&app, "rot", "read", None).await;
    // works before revoke
    let (before, _) = send(&app, "GET", "/workflows", None, Some(&secret), false, None).await;
    assert_ne!(before, StatusCode::UNAUTHORIZED);

    // find the id and revoke it
    let (_, list) = send(&app, "GET", "/admin/tokens", None, None, true, None).await;
    let id = list[0]["id"].as_str().unwrap().to_string();
    let (status, resp) = send(
        &app,
        "DELETE",
        &format!("/admin/tokens/{id}"),
        None,
        None,
        true,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "revoke resp: {resp:?}");
    assert_eq!(resp["revoked"], true);

    // next request with the revoked token is denied (no grant cache)
    let (after, _) = send(&app, "GET", "/workflows", None, Some(&secret), false, None).await;
    assert_eq!(after, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn expired_token_is_401() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    let secret = mint(&app, "short", "read", Some("2000-01-01T00:00:00Z")).await;
    let (status, _) = send(&app, "GET", "/workflows", None, Some(&secret), false, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// The AC3/AC4 success-metric sweep: a `read` token receives 403 on 100% of
/// mutating routes and is never denied a read-only route.
#[tokio::test]
async fn read_token_403_on_all_mutations_and_reaches_reads() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    let secret = mint(&app, "reader", "read", None).await;

    for (template, class) in CLASSIFIED_ROUTES {
        let Some((method, tmpl)) = template.split_once(' ') else {
            continue;
        };
        // Concrete path: substitute each {param} with a placeholder segment.
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
        let (status, _) = send(&app, method, &path, None, Some(&secret), false, None).await;
        match class {
            RouteClass::Mutating => assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "read token must be 403 on mutating route {template}"
            ),
            RouteClass::ReadOnly | RouteClass::PublicSafe => assert_ne!(
                status,
                StatusCode::FORBIDDEN,
                "read token must NOT be 403 on read route {template}"
            ),
        }
    }
}

/// AC7/D3: a `mutate` token reaches an admin-gated mutation with NO embedder
/// boundary, via the `require_admin` composition (`TokenPrincipal` extension).
#[tokio::test]
async fn mutate_token_reaches_admin_route_standalone() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;

    // Mint with the boundary app...
    let boundary = build_app_boundary(&pool);
    let secret = mint(&boundary, "deployer", "mutate", None).await;

    // ...then present it to the standalone (no-boundary) app.
    let standalone = build_app_standalone(&pool);
    // A mutating admin route: without the token, standalone 401s (no admin).
    let (unauth, _) = send(
        &standalone,
        "POST",
        "/workflows/some-wf/cancel",
        Some(json!({})),
        None,
        false,
        None,
    )
    .await;
    assert_eq!(unauth, StatusCode::UNAUTHORIZED, "no creds → 401");

    // With the mutate token, require_admin admits it (not 401/403 from auth).
    let (authed, _) = send(
        &standalone,
        "POST",
        "/workflows/some-wf/cancel",
        Some(json!({})),
        Some(&secret),
        false,
        None,
    )
    .await;
    assert_ne!(authed, StatusCode::UNAUTHORIZED, "mutate token must pass admin");
    assert_ne!(authed, StatusCode::FORBIDDEN, "mutate token is not read-scoped");
}

/// AC6: a token-authed mutation records `actor = token:{id}`, never the
/// secret/hash, and a spoofed `x-harvest-actor` header cannot override it.
#[tokio::test]
async fn token_authed_mutation_audit_actor_is_token_id() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    // Mint a second token WITH the mutate scope (creating a token is itself an
    // audited mutation — but we assert on a revoke below to get a clean id).
    let secret = mint(&app, "actor-test", "mutate", None).await;
    let (_, list) = send(&app, "GET", "/admin/tokens", None, None, true, None).await;
    let id = list[0]["id"].as_str().unwrap().to_string();

    scrub(&mut conn).await; // clear the mint audit rows for a clean assertion
    // Re-mint the token row is gone now; instead perform a mutation with the
    // token bearer: revoke a (freshly minted) throwaway token.
    let throwaway = mint(&app, "throwaway", "read", None).await;
    let _ = throwaway;
    let (_, list2) = send(&app, "GET", "/admin/tokens", None, None, true, None).await;
    let throwaway_id = list2
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "throwaway")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Revoke the throwaway, authenticating with the mutate token, and try to
    // spoof a different actor via the header — the server must ignore it.
    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/admin/tokens/{throwaway_id}"),
        None,
        Some(&secret),
        true,
        Some("attacker@evil"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The revoke audit row's actor is token:{id-of-the-mutate-token}.
    #[derive(diesel::QueryableByName)]
    struct A {
        #[diesel(sql_type = diesel::sql_types::Text)]
        actor: String,
    }
    let rows: Vec<A> = diesel::sql_query(
        "SELECT actor FROM harvest_audit_log WHERE operation = 'token.revoke'",
    )
    .load(&mut conn)
    .await
    .unwrap();
    assert!(!rows.is_empty(), "a revoke audit row must exist");
    for r in &rows {
        assert_eq!(r.actor, format!("token:{id}"), "actor must be token:{{id}}");
        assert_ne!(r.actor, "attacker@evil", "spoofed actor must be ignored");
        assert!(!r.actor.contains(&secret), "actor must not carry the secret");
    }
}

/// AC6 companion: the create/revoke operations themselves write an audit row
/// with a non-empty actor.
#[tokio::test]
async fn create_and_revoke_are_audited_with_actor() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let app = build_app_boundary(&pool);

    // Mint (embedder-authed, actor from header).
    let (status, _) = send(
        &app,
        "POST",
        "/admin/tokens",
        Some(json!({ "name": "audited", "scope": "read" })),
        None,
        true,
        Some("release-eng"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(count_tokens(&mut conn).await, 1);

    #[derive(diesel::QueryableByName)]
    struct A {
        #[diesel(sql_type = diesel::sql_types::Text)]
        actor: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        operation: String,
    }
    let rows: Vec<A> =
        diesel::sql_query("SELECT actor, operation FROM harvest_audit_log ORDER BY occurred_at")
            .load(&mut conn)
            .await
            .unwrap();
    assert!(
        rows.iter().any(|r| r.operation == "token.create"),
        "create must be audited"
    );
    for r in &rows {
        assert!(!r.actor.is_empty(), "audit actor must be non-empty");
    }
}
