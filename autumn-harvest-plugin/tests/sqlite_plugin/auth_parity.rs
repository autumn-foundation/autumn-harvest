//! API tokens, roles, OIDC and the audit log on a SQLite app (issue #2469).
//!
//! The token, role and OIDC tests run the shared checks of
//! `support/token_contract.rs`, `support/role_contract.rs` and
//! `support/oidc_contract.rs`. The Postgres suites run the same checks.
//! The other tests cover what only this backend does.

// The `#[workflow]` macro reads the unused `_input` through its expansion.
#![allow(clippy::used_underscore_binding)]
// A `TokenBackend` method is async for the Postgres harness. The SQLite
// store answers at once.
#![allow(clippy::unused_async_trait_impl)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::api_rate_limit::{ApiRateLimit, BucketRate};
use autumn_harvest_plugin::authz::{AuthzDecision, AuthzFuture, AuthzRequest, HarvestAuthorizer};
use autumn_harvest_plugin::roles::{RoleGrant, SESSION_ROLES_KEY};
use autumn_harvest_plugin::sqlite::HarvestSqlite;
use autumn_harvest_sqlite::{NewToken, SqliteAuthStore};
use autumn_web::config::AutumnConfig;
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use autumn_web::test::TestApp;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use serde_json::{Value, json};
use tempfile::TempDir;

use super::role_contract::{self, RoleBackend, Source};
use super::token_contract::{self, Call, TokenBackend, audit_actors, send};

/// Waits for a signal. The role and OIDC checks start it as `wf`.
#[workflow]
async fn wf(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

fn config(dir: &Path) -> AutumnConfig {
    let mut config = AutumnConfig::default();
    config.database.url = Some(format!("sqlite://{}", dir.join("app.db").display()));
    // A `prod` app refuses a request whose `Host` it does not trust.
    config.security.trusted_hosts.hosts = vec![token_contract::HOST.to_owned()];
    config
}

/// The plugin of every app here: one workflow, its own Harvest file.
fn plugin(harvest_file: &Path) -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(workflows![wf])
        .sqlite_path(harvest_file)
}

/// Boot `plugin` in `profile` on `dir`, and return the app router and its
/// Harvest handle.
fn boot(dir: &Path, profile: &str, plugin: HarvestPlugin) -> (axum::Router, HarvestSqlite) {
    let client = TestApp::new()
        .config(config(dir))
        .profile(profile)
        .plugin(plugin)
        .build();
    let handle = client
        .state()
        .extension::<HarvestSqlite>()
        .expect("the startup hook installs the handle")
        .as_ref()
        .clone();
    (client.into_router(), handle)
}

/// A host auth layer that admits every caller, as a declared boundary does.
async fn pass(request: Request, next: Next) -> axum::response::Response {
    next.run(request).await
}

// ── Tokens ───────────────────────────────────────────────────────────────────

const PREFIX: &str = "/harvest";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Boundary,
    Standalone,
}

/// One Harvest file. The boundary app and the standalone app take turns on
/// it, because only one runtime may hold its writer lock.
struct Sqlite {
    dir: TempDir,
    running: Option<(Kind, axum::Router, HarvestSqlite)>,
}

impl Sqlite {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
            running: None,
        }
    }

    fn harvest_file(&self) -> PathBuf {
        self.dir.path().join("app.harvest.db")
    }

    async fn app(&mut self, kind: Kind) -> axum::Router {
        if let Some((running, router, _)) = &self.running
            && *running == kind
        {
            return router.clone();
        }
        if let Some((_, _, handle)) = self.running.take() {
            // `TestApp` runs no shutdown hook. Release the writer lock.
            handle.shutdown().await.unwrap();
        }
        let plugin = plugin(&self.harvest_file()).enable_api_tokens();
        let plugin = match kind {
            Kind::Boundary => plugin.api_with_auth(PREFIX, axum::middleware::from_fn(pass)),
            Kind::Standalone => plugin.api(PREFIX),
        };
        let (router, handle) = boot(self.dir.path(), "prod", plugin);
        self.running = Some((kind, router.clone(), handle));
        router
    }

    fn store(&self) -> SqliteAuthStore {
        SqliteAuthStore::open(self.harvest_file()).unwrap()
    }
}

impl TokenBackend for Sqlite {
    fn prefix(&self) -> &'static str {
        PREFIX
    }

    async fn boundary(&mut self) -> axum::Router {
        self.app(Kind::Boundary).await
    }

    async fn standalone(&mut self) -> axum::Router {
        self.app(Kind::Standalone).await
    }

    async fn stored_hashes(&self) -> Vec<(String, String)> {
        self.store()
            .list_tokens()
            .unwrap()
            .into_iter()
            .map(|t| (t.name, t.token_hash))
            .collect()
    }

    async fn seed_admin_token(&self, name: &str, token_hash: &str) -> String {
        let id = uuid::Uuid::new_v4();
        self.store()
            .insert_token(&NewToken {
                id,
                name,
                token_hash,
                scope: "admin",
                created_at: chrono::Utc::now(),
                expires_at: None,
                created_by: "bootstrap",
                tenant: None,
            })
            .unwrap();
        id.to_string()
    }
}

macro_rules! token_checks {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                token_contract::$name(&mut Sqlite::new()).await;
            }
        )*
    };
}

token_checks!(
    create_returns_secret_once_and_stores_only_hash,
    list_returns_metadata_only,
    verify_with_secret_authenticates_a_read_route,
    mixed_case_bearer_scheme_authenticates_identically,
    unknown_hvst_bearer_is_401,
    revoke_denies_next_request,
    expired_token_is_401,
    read_token_403_on_all_mutations_and_reaches_reads,
    mutate_token_reaches_admin_route_standalone,
    token_authed_mutation_audit_actor_is_token_id,
    passthrough_cannot_forge_token_namespaced_actor,
    create_and_revoke_are_audited_with_actor,
    bootstrap_seeded_token_authenticates_identically_to_route_minted,
    mutate_token_is_denied_token_admin_and_audited,
);

// ── Roles ────────────────────────────────────────────────────────────────────

/// Each app gets its own directory. The directories live as long as the
/// backend, so a running app keeps its files.
#[derive(Default)]
struct Apps {
    dirs: Mutex<Vec<TempDir>>,
}

impl Apps {
    fn dir(&self) -> PathBuf {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        self.dirs.lock().unwrap().push(dir);
        path
    }
}

/// Put the roles of `source` on `request`, as host auth does.
fn give_roles(request: &mut Request, source: &Source) {
    let (session, grant) = match source {
        Source::None => (None, None),
        Source::Session(s) => (Some(*s), None),
        Source::Grant(g) => (None, Some(g.clone())),
        Source::Both { session, grant } => (Some(*session), Some(grant.clone())),
    };
    if let Some(value) = session {
        let mut data = HashMap::new();
        data.insert(SESSION_ROLES_KEY.to_string(), value.to_string());
        request
            .extensions_mut()
            .insert(Session::new_for_test("roles".to_string(), data));
    }
    if let Some(grant) = grant {
        request.extensions_mut().insert(RoleGrant::new(grant));
    }
}

impl RoleBackend for Apps {
    /// An app-level layer gives the roles, as host middleware does. The app
    /// declares no boundary, as in the Postgres harness.
    fn app(&self, source: Source) -> axum::Router {
        let dir = self.dir();
        let host_auth = axum::middleware::from_fn(move |mut request: Request, next: Next| {
            let source = source.clone();
            async move {
                give_roles(&mut request, &source);
                next.run(request).await
            }
        });
        let plugin = plugin(&dir.join("app.harvest.db"))
            .with_roles(role_contract::roles())
            .api(role_contract::MOUNT);
        let client = TestApp::new()
            .config(config(&dir))
            .profile("prod")
            .layer(host_auth)
            .plugin(plugin)
            .build();
        client.into_router()
    }

    /// Vantage is #2468.
    fn serves_vantage(&self) -> bool {
        false
    }

    fn bare(&self) -> axum::Router {
        let dir = self.dir();
        let plugin = plugin(&dir.join("app.harvest.db")).api(role_contract::MOUNT);
        boot(&dir, "prod", plugin).0
    }
}

macro_rules! role_checks {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                role_contract::$name(&Apps::default()).await;
            }
        )*
    };
}

role_checks!(
    viewer_reads_the_api_and_vantage,
    viewer_is_denied_mutations_with_a_role_403,
    a_custom_role_reaches_only_its_extra_route,
    a_role_admits_an_admin_gated_read,
    an_operator_passes_the_mutation_gate,
    no_roles_and_unknown_roles_are_denied,
    public_routes_need_no_role,
    a_header_never_grants_a_role,
    a_role_grant_extension_admits_and_wins_over_the_session,
    options_preflight_passes_without_a_role,
    a_mount_without_roles_is_unchanged,
);

// ── OIDC ─────────────────────────────────────────────────────────────────────

#[cfg(feature = "oidc")]
mod oidc {
    use autumn_harvest_plugin::oidc::OidcLogin;

    use super::super::oidc_contract::{self, MOUNT, OidcBackend};
    use super::{Apps, axum, boot, plugin};

    impl Apps {
        fn oidc_app(&self, login: OidcLogin, tokens: bool) -> axum::Router {
            let dir = self.dir();
            let mut plugin = plugin(&dir.join("app.harvest.db")).api_with_oidc(MOUNT, login);
            if tokens {
                plugin = plugin.enable_api_tokens();
            }
            boot(&dir, "prod", plugin).0
        }
    }

    impl OidcBackend for Apps {
        fn app(&self, login: OidcLogin) -> axum::Router {
            self.oidc_app(login, false)
        }

        fn app_with_tokens(&self, login: OidcLogin) -> axum::Router {
            self.oidc_app(login, true)
        }

        /// Vantage is #2468.
        fn serves_vantage(&self) -> bool {
            false
        }

        /// The Harvest file holds the tokens.
        fn has_token_store(&self) -> bool {
            true
        }
    }

    macro_rules! oidc_checks {
        ($($name:ident),* $(,)?) => {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $name() {
                    oidc_contract::$name(&Apps::default()).await;
                }
            )*
        };
    }

    oidc_checks!(
        round_trip_logs_in_maps_claims_and_enforces_roles,
        admin_and_custom_claims_map_to_their_roles,
        an_identity_with_no_role_cannot_log_in,
        the_default_role_admits_an_identity_with_no_matching_claim,
        a_tampered_state_is_refused,
        a_callback_without_a_login_is_refused,
        a_provider_error_is_refused,
        a_token_signed_by_another_key_is_refused,
        a_token_for_another_audience_is_refused,
        an_old_session_must_log_in_again,
        public_routes_need_no_login,
        a_custom_post_login_redirect_is_used,
        discovery_builds_the_provider_and_checks_the_issuer,
        a_replayed_callback_is_refused_and_keeps_the_session,
        a_token_with_a_wrong_nonce_is_refused,
        an_expired_id_token_is_refused,
        a_token_from_another_issuer_is_refused,
        login_rotates_the_session_cookie,
        a_cross_site_logout_is_refused,
        an_api_token_bearer_passes_the_boundary_only_with_tokens_on,
    );
}

// ── SQLite only ──────────────────────────────────────────────────────────────

/// Denies every request that names the tenant `blocked`.
struct BlockTenant;

impl HarvestAuthorizer for BlockTenant {
    fn authorize<'a>(&'a self, request: &'a AuthzRequest<'a>) -> AuthzFuture<'a> {
        let decision = if request.tenant_key == Some("blocked") {
            AuthzDecision::deny("tenant is blocked")
        } else {
            AuthzDecision::Allow
        };
        Box::pin(std::future::ready(decision))
    }
}

/// An authorizer deny is a `403`. It writes one `authz.deny` row to the
/// Harvest file, and `GET /admin/audit` lists it.
#[tokio::test(flavor = "multi_thread")]
async fn an_authorizer_deny_is_audited_in_the_harvest_file() {
    let dir = tempfile::tempdir().unwrap();
    let harvest_file = dir.path().join("app.harvest.db");
    let plugin = plugin(&harvest_file)
        .with_authorizer(BlockTenant)
        .api_with_auth(PREFIX, axum::middleware::from_fn(pass));
    let (app, _handle) = boot(dir.path(), "prod", plugin);

    let allowed = axum_get(&app, "/workflows", &[("x-harvest-tenant", "acme")]).await;
    assert_eq!(allowed, StatusCode::OK);
    let denied = axum_get(
        &app,
        "/workflows",
        &[("x-harvest-tenant", "blocked"), ("x-harvest-actor", "ops")],
    )
    .await;
    assert_eq!(denied, StatusCode::FORBIDDEN);

    let (status, rows) = send(
        &app,
        PREFIX,
        Call::new("GET", "/admin/audit?operation=authz.deny"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["actor"], "ops");
    assert_eq!(rows[0]["status"], "failed");
    assert_eq!(rows[0]["route_or_command"], "GET /workflows");
    let summary = rows[0]["error_summary"].as_str().unwrap();
    assert!(summary.contains("tenant is blocked"), "{summary}");

    // The row is in the Harvest file, not in the app database.
    let stored = SqliteAuthStore::open(&harvest_file)
        .unwrap()
        .list_audit(&autumn_harvest_sqlite::AuditFilter {
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].operation, "authz.deny");
}

/// A role deny writes one `authz.deny` row too.
#[tokio::test(flavor = "multi_thread")]
async fn a_role_deny_is_audited_in_the_harvest_file() {
    let apps = Apps::default();
    let app = apps.app(Source::Session(autumn_harvest_plugin::roles::ROLE_VIEWER));
    let (status, _) = send(
        &app,
        role_contract::MOUNT,
        Call::new("POST", "/workflows/wf/start").body(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let file = apps.dirs.lock().unwrap()[0].path().join("app.harvest.db");
    let stored = SqliteAuthStore::open(file)
        .unwrap()
        .list_audit(&autumn_harvest_sqlite::AuditFilter {
            operation: Some("authz.deny".into()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(stored.len(), 1, "{stored:?}");
    let summary = stored[0].error_summary.as_deref().unwrap_or_default();
    assert!(summary.contains("harvest-viewer"), "{summary}");

    // The viewer reads the same row through the API.
    let (status, rows) = send(
        &app,
        role_contract::MOUNT,
        Call::new("GET", "/admin/audit?operation=authz.deny"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows.as_array().map(Vec::len), Some(1), "{rows}");
    assert_eq!(rows[0]["id"], stored[0].id.to_string());
}

/// The read-only role of `api_with_role_auth` (issue #776) works on
/// SQLite. A read-only session reads and gets `403` on a mutation.
#[tokio::test(flavor = "multi_thread")]
async fn the_read_only_role_reads_and_cannot_mutate() {
    let dir = tempfile::tempdir().unwrap();
    let host_auth = axum::middleware::from_fn(|mut request: Request, next: Next| async move {
        let mut data = HashMap::new();
        data.insert("role".to_owned(), "harvest_readonly".to_owned());
        request
            .extensions_mut()
            .insert(Session::new_for_test("readonly".to_owned(), data));
        next.run(request).await
    });
    let plugin = plugin(&dir.path().join("app.harvest.db")).api_with_role_auth(PREFIX, host_auth);
    let (app, _handle) = boot(dir.path(), "prod", plugin);

    assert_eq!(axum_get(&app, "/workflows", &[]).await, StatusCode::OK);
    let (status, body) = send(
        &app,
        PREFIX,
        Call::new("POST", "/workflows/wf/start").body(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

/// The SQLite store keeps no run tenant, so a tenant-bound token cannot be
/// checked. The mint is a `400`. Both fail closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_bound_token_is_refused() {
    let mut backend = Sqlite::new();
    let app = backend.boundary().await;
    let (status, body) = send(
        &app,
        PREFIX,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "acme-ci", "scope": "mutate", "tenant": "acme" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.to_string().contains("#2466"), "{body}");
    assert_eq!(backend.stored_hashes().await.len(), 0, "nothing is minted");
}

/// A seeded tenant-bound token, or any verified tenant, gets `501` on every
/// non-public route. The store cannot prove which runs the tenant owns.
#[tokio::test(flavor = "multi_thread")]
async fn a_verified_tenant_is_refused_and_audited() {
    let mut backend = Sqlite::new();
    let app = backend.boundary().await;
    let secret = autumn_harvest::api_token::mint_secret();
    let hash = autumn_harvest::api_token::hash_secret(&secret);
    let id = uuid::Uuid::new_v4();
    backend
        .store()
        .insert_token(&NewToken {
            id,
            name: "acme",
            token_hash: &hash,
            scope: "mutate",
            created_at: chrono::Utc::now(),
            expires_at: None,
            created_by: "bootstrap",
            tenant: Some("acme"),
        })
        .unwrap();
    for (method, path) in [
        ("GET", "/workflows"),
        ("POST", "/workflows/wf/start"),
        ("GET", "/workflows/0190f3c2-0000-7000-8000-000000000000"),
    ] {
        let (status, body) = send(
            &app,
            PREFIX,
            Call::new(method, path).body(json!({})).bearer(&secret),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_IMPLEMENTED,
            "{method} {path}: {body}"
        );
        assert!(body.to_string().contains("2466"), "{body}");
    }
    let (status, _) = send(&app, PREFIX, Call::new("GET", "/health").bearer(&secret)).await;
    assert_eq!(status, StatusCode::OK, "a public route passes");
    let actors = audit_actors(&app, PREFIX, "authz.deny").await;
    assert_eq!(actors.len(), 3);
    assert!(actors.iter().all(|a| a == &format!("token:{id}")));
}

/// A token minted on SQLite starts and signals a run. A read token and an
/// anonymous caller cannot change it.
#[tokio::test(flavor = "multi_thread")]
async fn a_mutate_token_starts_and_signals_a_run() {
    let mut backend = Sqlite::new();
    let app = backend.standalone().await;
    let seeded = autumn_harvest::api_token::mint_secret();
    backend
        .seed_admin_token("root", &autumn_harvest::api_token::hash_secret(&seeded))
        .await;
    let secret = mint_with(&app, &seeded, "deployer", "mutate").await;

    let (status, started) = send(
        &app,
        PREFIX,
        Call::new("POST", "/workflows/wf/start")
            .body(json!({ "input": 1 }))
            .bearer(&secret),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let exec = started["execution_id"].as_str().unwrap().to_owned();
    let (status, body) = send(
        &app,
        PREFIX,
        Call::new("POST", &format!("/workflows/{exec}/signal/go"))
            .body(json!({ "payload": "ok" }))
            .bearer(&secret),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");

    // A read token cannot signal.
    let reader = mint_with(&app, &seeded, "reader", "read").await;
    let (status, _) = send(
        &app,
        PREFIX,
        Call::new("POST", &format!("/workflows/{exec}/signal/go"))
            .body(json!({}))
            .bearer(&reader),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // With no credential, prod refuses the mutation.
    let (status, _) = send(
        &app,
        PREFIX,
        Call::new("POST", &format!("/workflows/{exec}/cancel")).body(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// A rate limit applies on SQLite as on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn the_api_rate_limit_applies() {
    let dir = tempfile::tempdir().unwrap();
    let limit = ApiRateLimit::new(BucketRate::per_second(1), BucketRate::per_second(1));
    let plugin = plugin(&dir.path().join("app.harvest.db"))
        .with_api_rate_limit(limit)
        .api_with_auth(PREFIX, axum::middleware::from_fn(pass));
    let (app, _handle) = boot(dir.path(), "prod", plugin);
    let mut statuses = Vec::new();
    for _ in 0..6 {
        statuses.push(axum_get(&app, "/workflows", &[]).await);
    }
    assert_eq!(statuses[0], StatusCode::OK, "the first call passes");
    assert!(
        statuses.contains(&StatusCode::TOO_MANY_REQUESTS),
        "{statuses:?}"
    );
}

/// Mint a token with an admin `bearer`, and return its secret.
async fn mint_with(app: &axum::Router, bearer: &str, name: &str, scope: &str) -> String {
    let (status, body) = send(
        app,
        PREFIX,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": name, "scope": scope }))
            .bearer(bearer),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body["secret"].as_str().unwrap().to_owned()
}

async fn axum_get(app: &axum::Router, path: &str, headers: &[(&str, &str)]) -> StatusCode {
    use tower::ServiceExt as _;
    let mut builder = axum::http::Request::builder()
        .uri(format!("{PREFIX}{path}"))
        .header("host", token_contract::HOST);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let mut request = builder.body(axum::body::Body::empty()).unwrap();
    request
        .extensions_mut()
        .insert(axum::extract::ConnectInfo(std::net::SocketAddr::from((
            [127, 0, 0, 1],
            40_000,
        ))));
    app.clone().oneshot(request).await.unwrap().status()
}
