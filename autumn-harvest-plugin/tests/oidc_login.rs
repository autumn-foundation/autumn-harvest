//! OIDC login round trip against a local mock identity provider (issue #1978).
//!
//! Harvest runs behind a real autumn-web session layer, and each test
//! carries the session cookie like a browser does. No database is needed.
//!
//! The shared checks live in `support/oidc_contract.rs`. The SQLite suite
//! runs them on a SQLite app (issue #2469). The tests at the end of this
//! file check the standalone mount or the configuration only.
#![cfg(feature = "oidc")]

use autumn_harvest_plugin::api::{HarvestApiState, StandaloneAdminAuth, harvest_api_router};
use autumn_harvest_plugin::harvest_ui_router;
use autumn_harvest_plugin::oidc::{OidcConfigError, OidcLogin};
use autumn_harvest_plugin::roles::{ClaimRoleMap, ClaimRule, RoleConfigError};
use autumn_web::reexports::axum;
use autumn_web::session::{MemoryStore, SessionConfig, SessionLayer};
use axum::http::StatusCode;
use serde_json::json;

#[path = "support/oidc_contract.rs"]
mod oidc_contract;

use oidc_contract::{
    Browser, MOUNT, OidcBackend, admitted, claim_map, login, provider, roles, set_claims, start_idp,
};

/// The API and Vantage routers behind the OIDC login, in the `prod`
/// profile, under a memory session layer.
struct Standalone;

fn mount(login: OidcLogin, tokens: bool) -> axum::Router {
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    let mut auth = StandaloneAdminAuth::new().with_deployment_profile("prod");
    if tokens {
        auth = auth.with_api_tokens();
    }
    let mounted = auth.with_oidc(login).mount(router, &api_state);
    axum::Router::new()
        .nest(MOUNT, mounted)
        .layer(SessionLayer::new(MemoryStore::new(), session_config()))
}

fn session_config() -> SessionConfig {
    SessionConfig {
        secure: false,
        ..SessionConfig::default()
    }
}

impl OidcBackend for Standalone {
    fn app(&self, login: OidcLogin) -> axum::Router {
        mount(login, false)
    }

    fn app_with_tokens(&self, login: OidcLogin) -> axum::Router {
        mount(login, true)
    }

    fn serves_vantage(&self) -> bool {
        true
    }
}

// ── Shared checks ────────────────────────────────────────────────────────────

#[tokio::test]
async fn round_trip_logs_in_maps_claims_and_enforces_roles() {
    oidc_contract::round_trip_logs_in_maps_claims_and_enforces_roles(&Standalone).await;
}

#[tokio::test]
async fn admin_and_custom_claims_map_to_their_roles() {
    oidc_contract::admin_and_custom_claims_map_to_their_roles(&Standalone).await;
}

#[tokio::test]
async fn an_identity_with_no_role_cannot_log_in() {
    oidc_contract::an_identity_with_no_role_cannot_log_in(&Standalone).await;
}

#[tokio::test]
async fn the_default_role_admits_an_identity_with_no_matching_claim() {
    oidc_contract::the_default_role_admits_an_identity_with_no_matching_claim(&Standalone).await;
}

#[tokio::test]
async fn a_tampered_state_is_refused() {
    oidc_contract::a_tampered_state_is_refused(&Standalone).await;
}

#[tokio::test]
async fn a_callback_without_a_login_is_refused() {
    oidc_contract::a_callback_without_a_login_is_refused(&Standalone).await;
}

#[tokio::test]
async fn a_provider_error_is_refused() {
    oidc_contract::a_provider_error_is_refused(&Standalone).await;
}

#[tokio::test]
async fn a_token_signed_by_another_key_is_refused() {
    oidc_contract::a_token_signed_by_another_key_is_refused(&Standalone).await;
}

#[tokio::test]
async fn a_token_for_another_audience_is_refused() {
    oidc_contract::a_token_for_another_audience_is_refused(&Standalone).await;
}

#[tokio::test]
async fn an_old_session_must_log_in_again() {
    oidc_contract::an_old_session_must_log_in_again(&Standalone).await;
}

#[tokio::test]
async fn public_routes_need_no_login() {
    oidc_contract::public_routes_need_no_login(&Standalone).await;
}

#[tokio::test]
async fn a_custom_post_login_redirect_is_used() {
    oidc_contract::a_custom_post_login_redirect_is_used(&Standalone).await;
}

#[tokio::test]
async fn discovery_builds_the_provider_and_checks_the_issuer() {
    oidc_contract::discovery_builds_the_provider_and_checks_the_issuer(&Standalone).await;
}

#[tokio::test]
async fn a_replayed_callback_is_refused_and_keeps_the_session() {
    oidc_contract::a_replayed_callback_is_refused_and_keeps_the_session(&Standalone).await;
}

#[tokio::test]
async fn a_token_with_a_wrong_nonce_is_refused() {
    oidc_contract::a_token_with_a_wrong_nonce_is_refused(&Standalone).await;
}

#[tokio::test]
async fn an_expired_id_token_is_refused() {
    oidc_contract::an_expired_id_token_is_refused(&Standalone).await;
}

#[tokio::test]
async fn a_token_from_another_issuer_is_refused() {
    oidc_contract::a_token_from_another_issuer_is_refused(&Standalone).await;
}

#[tokio::test]
async fn login_rotates_the_session_cookie() {
    oidc_contract::login_rotates_the_session_cookie(&Standalone).await;
}

#[tokio::test]
async fn a_cross_site_logout_is_refused() {
    oidc_contract::a_cross_site_logout_is_refused(&Standalone).await;
}

#[tokio::test]
async fn an_api_token_bearer_passes_the_boundary_only_with_tokens_on() {
    oidc_contract::an_api_token_bearer_passes_the_boundary_only_with_tokens_on(&Standalone).await;
}

// ── Standalone and configuration only ───────────────────────────────────────

#[tokio::test]
async fn bad_configurations_are_refused() {
    let idp = start_idp().await;
    let mut insecure = provider(&idp);
    insecure.token_url = "http://idp.example.com/token".to_string();
    assert!(matches!(
        OidcLogin::new(insecure, roles(), claim_map()),
        Err(OidcConfigError::InsecureUrl {
            field: "token_url",
            ..
        })
    ));

    let mut no_openid = provider(&idp);
    no_openid.scope = "profile email".to_string();
    assert_eq!(
        OidcLogin::new(no_openid, roles(), claim_map()).err(),
        Some(OidcConfigError::MissingOpenidScope)
    );

    let mut no_jwks = provider(&idp);
    no_jwks.jwks_url = None;
    assert_eq!(
        OidcLogin::new(no_jwks, roles(), claim_map()).err(),
        Some(OidcConfigError::MissingField("jwks_url"))
    );

    let mut no_client = provider(&idp);
    no_client.client_id = String::new();
    assert_eq!(
        OidcLogin::new(no_client, roles(), claim_map()).err(),
        Some(OidcConfigError::MissingField("client_id"))
    );

    let ghost = ClaimRoleMap::new().rule(ClaimRule::new("groups", "x", "ghost"));
    assert_eq!(
        OidcLogin::new(provider(&idp), roles(), ghost).err(),
        Some(OidcConfigError::Roles(RoleConfigError::UndefinedRole(
            "ghost".to_string()
        )))
    );

    for bad in [
        "",
        "ui",
        "//evil.example.com",
        "https://evil.example.com",
        "/\\evil",
    ] {
        assert!(
            matches!(
                login(&idp).with_post_login_redirect(bad),
                Err(OidcConfigError::InvalidRedirect(_))
            ),
            "{bad:?}"
        );
    }
}

#[tokio::test]
async fn standalone_oidc_replaces_the_legacy_read_only_role() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    let mounted = StandaloneAdminAuth::new()
        .with_deployment_profile("prod")
        .with_read_only_role()
        .with_oidc(login(&idp))
        .mount(router, &api_state);
    // The host app shares the session and keeps a legacy read-only marker.
    let host = axum::Router::new().route(
        "/host/mark",
        axum::routing::get(|session: autumn_web::session::Session| async move {
            session.insert("role", "operator").await;
            "marked"
        }),
    );
    let app = axum::Router::new()
        .nest(MOUNT, mounted)
        .merge(host)
        .layer(SessionLayer::new(
            MemoryStore::new(),
            SessionConfig {
                secure: false,
                ..SessionConfig::default()
            },
        ));
    let mut browser = Browser::new(app);
    assert_eq!(browser.get("/host/mark").await.status, StatusCode::OK);
    assert_eq!(browser.login().await.status, StatusCode::SEE_OTHER);
    let start = browser.post(&format!("{MOUNT}/workflows/wf/start")).await;
    assert!(admitted(&start), "{} {}", start.status, start.body);
}

/// A login never writes the shared `harvest_roles` key. A plain role mount
/// that shares the session reads that key with no login check.
#[tokio::test]
async fn a_login_keeps_its_roles_out_of_the_shared_role_key() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    let mounted = StandaloneAdminAuth::new()
        .with_deployment_profile("prod")
        .with_oidc(login(&idp))
        .mount(router, &api_state);
    let host = axum::Router::new().route(
        "/host/roles",
        axum::routing::get(|session: autumn_web::session::Session| async move {
            session
                .get(autumn_harvest_plugin::roles::SESSION_ROLES_KEY)
                .await
                .unwrap_or_else(|| "-".to_string())
        }),
    );
    let app = axum::Router::new()
        .nest(MOUNT, mounted)
        .merge(host)
        .layer(SessionLayer::new(
            MemoryStore::new(),
            SessionConfig {
                secure: false,
                ..SessionConfig::default()
            },
        ));
    let mut browser = Browser::new(app);
    assert_eq!(browser.login().await.status, StatusCode::SEE_OTHER);
    // The login still works: the admin reaches a mutation.
    let start = browser.post(&format!("{MOUNT}/workflows/wf/start")).await;
    assert!(admitted(&start), "{} {}", start.status, start.body);
    let shared = browser.get("/host/roles").await;
    assert_eq!(shared.body, "-");
}
