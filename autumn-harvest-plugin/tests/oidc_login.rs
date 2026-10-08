//! OIDC login round trip against a local mock identity provider (issue #1978).
//!
//! The mock provider runs on a loopback port. It serves discovery, the
//! authorize endpoint, the token endpoint and the JWKS. It signs ID tokens
//! with Ed25519. Harvest runs behind a real autumn-web session layer, and
//! each test carries the session cookie like a browser does.
//!
//! The round trip covers the login redirect, PKCE, `state`, `nonce`, the
//! signature check, the claim-to-role map, role enforcement and logout. No
//! database is needed.
#![cfg(feature = "oidc")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest_plugin::api::{HarvestApiState, StandaloneAdminAuth, harvest_api_router};
use autumn_harvest_plugin::api_token::TokenScope;
use autumn_harvest_plugin::harvest_ui_router;
use autumn_harvest_plugin::oidc::{
    CALLBACK_PATH, LOGIN_PATH, LOGOUT_PATH, OAuth2ProviderConfig, OidcConfigError, OidcLogin,
    discover_provider,
};
use autumn_harvest_plugin::roles::{
    ClaimRoleMap, ClaimRule, HarvestRole, HarvestRoles, ROLE_ADMIN, ROLE_DENIED_ERROR, ROLE_VIEWER,
    RoleConfigError,
};
use autumn_web::reexports::axum;
use autumn_web::session::{MemoryStore, SessionConfig, SessionLayer};
use axum::body::Body;
use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};
use sha2::Digest as _;
use tower::ServiceExt;

const CLIENT_ID: &str = "harvest-vantage";
const CLIENT_SECRET: &str = "s3cret";
const MOUNT: &str = "/api/harvest";
const KID: &str = "mock-key-1";

// ── Mock identity provider ───────────────────────────────────────────────────

/// One issued authorization code.
#[derive(Clone)]
struct Grant {
    nonce: String,
    challenge: String,
}

#[derive(Default)]
struct IdpState {
    /// The claims the next code carries, besides the standard ones.
    claims: Value,
    /// Override the `aud` claim.
    audience: Option<String>,
    /// Sign with this key seed instead of the published key.
    rogue_seed: Option<[u8; 32]>,
    codes: HashMap<String, Grant>,
    /// Every `code_verifier` the token endpoint saw, with its PKCE result.
    pkce_ok: Vec<bool>,
}

#[derive(Clone)]
struct Idp {
    issuer: String,
    state: Arc<Mutex<IdpState>>,
}

const SEED: [u8; 32] = [7u8; 32];

/// An Ed25519 PKCS#8 v1 document for `seed`.
fn pkcs8(seed: &[u8; 32]) -> Vec<u8> {
    let mut der = vec![
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    der.extend_from_slice(seed);
    der
}

fn public_x(seed: &[u8; 32]) -> String {
    let key = ed25519_dalek::SigningKey::from_bytes(seed);
    URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes())
}

async fn discovery(State(idp): State<Idp>) -> axum::Json<Value> {
    axum::Json(json!({
        "issuer": idp.issuer,
        "authorization_endpoint": format!("{}/authorize", idp.issuer),
        "token_endpoint": format!("{}/token", idp.issuer),
        "jwks_uri": format!("{}/jwks", idp.issuer),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["EdDSA"],
    }))
}

async fn jwks() -> axum::Json<Value> {
    axum::Json(json!({ "keys": [{
        "kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig",
        "kid": KID, "x": public_x(&SEED),
    }]}))
}

async fn authorize(State(idp): State<Idp>, Query(q): Query<HashMap<String, String>>) -> Response {
    assert_eq!(q.get("response_type").map(String::as_str), Some("code"));
    assert_eq!(q.get("client_id").map(String::as_str), Some(CLIENT_ID));
    assert_eq!(
        q.get("code_challenge_method").map(String::as_str),
        Some("S256")
    );
    assert!(q.get("scope").is_some_and(|s| s.contains("openid")));
    let code = uuid::Uuid::new_v4().to_string();
    idp.state.lock().expect("idp").codes.insert(
        code.clone(),
        Grant {
            nonce: q["nonce"].clone(),
            challenge: q["code_challenge"].clone(),
        },
    );
    let location = format!(
        "{}?code={code}&state={}",
        q["redirect_uri"],
        urlencode(&q["state"])
    );
    Redirect::to(&location).into_response()
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// The parts of the provider state one token request reads.
struct TokenPlan {
    grant: Grant,
    claims: Value,
    audience: Option<String>,
    seed: [u8; 32],
}

/// Take the code's grant and record the PKCE result, under one short lock.
fn plan_token(idp: &Idp, form: &HashMap<String, String>) -> Result<TokenPlan, &'static str> {
    let mut state = idp.state.lock().expect("idp");
    let grant = state
        .codes
        .remove(form.get("code").map_or("", String::as_str))
        .ok_or("unknown code")?;
    let verifier = form.get("code_verifier").cloned().unwrap_or_default();
    let challenge = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()));
    let pkce_ok = challenge == grant.challenge;
    state.pkce_ok.push(pkce_ok);
    if !pkce_ok || form.get("client_secret").map(String::as_str) != Some(CLIENT_SECRET) {
        return Err("invalid_grant");
    }
    Ok(TokenPlan {
        grant,
        claims: state.claims.clone(),
        audience: state.audience.clone(),
        seed: state.rogue_seed.unwrap_or(SEED),
    })
}

async fn token(State(idp): State<Idp>, Form(form): Form<HashMap<String, String>>) -> Response {
    let plan = match plan_token(&idp, &form) {
        Ok(plan) => plan,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    let now = chrono::Utc::now().timestamp();
    let mut claims = json!({
        "iss": idp.issuer,
        "sub": "user-42",
        "aud": plan.audience.unwrap_or_else(|| CLIENT_ID.to_string()),
        "iat": now,
        "exp": now + 300,
        "nonce": plan.grant.nonce,
    });
    if let (Some(base), Some(extra)) = (claims.as_object_mut(), plan.claims.as_object()) {
        for (k, v) in extra {
            base.insert(k.clone(), v.clone());
        }
    }
    let mut jwt_header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
    jwt_header.kid = Some(KID.to_string());
    let id_token = jsonwebtoken::encode(
        &jwt_header,
        &claims,
        &jsonwebtoken::EncodingKey::from_ed_der(&pkcs8(&plan.seed)),
    )
    .expect("sign id_token");
    axum::Json(json!({
        "access_token": "opaque-access-token",
        "token_type": "Bearer",
        "id_token": id_token,
    }))
    .into_response()
}

async fn start_idp() -> Idp {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind idp");
    let issuer = format!("http://{}", listener.local_addr().expect("addr"));
    let idp = Idp {
        issuer,
        state: Arc::new(Mutex::new(IdpState::default())),
    };
    let app = axum::Router::new()
        .route(
            "/.well-known/openid-configuration",
            axum::routing::get(discovery),
        )
        .route("/authorize", axum::routing::get(authorize))
        .route("/token", axum::routing::post(token))
        .route("/jwks", axum::routing::get(jwks))
        .with_state(idp.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("idp serve");
    });
    idp
}

// ── Harvest app ──────────────────────────────────────────────────────────────

fn roles() -> HarvestRoles {
    HarvestRoles::builder()
        .builtin_roles()
        .role(
            HarvestRole::new("dlq-operator")
                .with_scope(TokenScope::Read)
                .allow_route("POST /dead-letters/replay"),
        )
        .build()
        .expect("roles")
}

fn claim_map() -> ClaimRoleMap {
    ClaimRoleMap::new()
        .rule(ClaimRule::new("groups", "harvest-admins", ROLE_ADMIN))
        .rule(ClaimRule::new("groups", "support", ROLE_VIEWER))
        .rule(ClaimRule::new("realm_access.roles", "dlq", "dlq-operator"))
}

fn provider(idp: &Idp) -> OAuth2ProviderConfig {
    OAuth2ProviderConfig {
        client_id: CLIENT_ID.to_string(),
        client_secret: CLIENT_SECRET.to_string(),
        authorize_url: format!("{}/authorize", idp.issuer),
        token_url: format!("{}/token", idp.issuer),
        userinfo_url: None,
        redirect_uri: format!("http://harvest.test{MOUNT}{CALLBACK_PATH}"),
        scope: "openid profile email".to_string(),
        issuer: Some(idp.issuer.clone()),
        jwks_url: Some(format!("{}/jwks", idp.issuer)),
        discovery_url: None,
    }
}

fn harvest_app(login: OidcLogin) -> axum::Router {
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    let mounted = StandaloneAdminAuth::new()
        .with_deployment_profile("prod")
        .with_oidc(login)
        .mount(router, &api_state);
    let session_config = SessionConfig {
        secure: false,
        ..SessionConfig::default()
    };
    axum::Router::new()
        .nest(MOUNT, mounted)
        .layer(SessionLayer::new(MemoryStore::new(), session_config))
}

/// A minimal browser: one cookie jar, no automatic redirects.
struct Browser {
    app: axum::Router,
    cookies: HashMap<String, String>,
}

struct Reply {
    status: StatusCode,
    location: Option<String>,
    body: String,
}

impl Browser {
    fn new(app: axum::Router) -> Self {
        Self {
            app,
            cookies: HashMap::new(),
        }
    }

    async fn send(&mut self, method: Method, uri: &str, extra: &[(&str, &str)]) -> Reply {
        let mut builder = axum::http::Request::builder().method(method).uri(uri);
        if !self.cookies.is_empty() {
            let cookie = self
                .cookies
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("; ");
            builder = builder.header(header::COOKIE, cookie);
        }
        for (name, value) in extra {
            builder = builder.header(*name, *value);
        }
        let response = self
            .app
            .clone()
            .oneshot(builder.body(Body::empty()).expect("request"))
            .await
            .expect("served");
        self.store_cookies(response.headers());
        let status = response.status();
        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .expect("body");
        Reply {
            status,
            location,
            body: String::from_utf8_lossy(&bytes).into_owned(),
        }
    }

    fn store_cookies(&mut self, headers: &HeaderMap) {
        for value in headers.get_all(header::SET_COOKIE) {
            let Ok(text) = value.to_str() else { continue };
            let pair = text.split(';').next().unwrap_or_default();
            if let Some((name, value)) = pair.split_once('=') {
                if value.is_empty() || text.to_ascii_lowercase().contains("max-age=0") {
                    self.cookies.remove(name.trim());
                } else {
                    self.cookies
                        .insert(name.trim().to_string(), value.trim().to_string());
                }
            }
        }
    }

    async fn get(&mut self, uri: &str) -> Reply {
        self.send(Method::GET, uri, &[]).await
    }

    async fn post(&mut self, uri: &str) -> Reply {
        self.send(Method::POST, uri, &[("content-type", "application/json")])
            .await
    }

    /// Start a login, follow the provider redirect, and return the callback
    /// path and query that the provider sends the browser to.
    async fn begin_login(&mut self) -> String {
        let start = self.get(&format!("{MOUNT}{LOGIN_PATH}")).await;
        assert_eq!(start.status, StatusCode::SEE_OTHER, "login: {}", start.body);
        let authorize = start.location.expect("login redirects to the provider");
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .expect("client");
        let reply = client.get(&authorize).send().await.expect("authorize");
        assert!(reply.status().is_redirection(), "{}", reply.status());
        let location = reply
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .expect("provider redirects back")
            .to_string();
        location
            .strip_prefix("http://harvest.test")
            .expect("callback on harvest")
            .to_string()
    }

    async fn login(&mut self) -> Reply {
        let callback = self.begin_login().await;
        self.get(&callback).await
    }
}

fn set_claims(idp: &Idp, claims: Value) {
    idp.state.lock().expect("idp").claims = claims;
}

fn login(idp: &Idp) -> OidcLogin {
    OidcLogin::new(provider(idp), roles(), claim_map()).expect("login config")
}

fn denied_by_role(reply: &Reply) -> bool {
    reply.status == StatusCode::FORBIDDEN && reply.body.contains(ROLE_DENIED_ERROR)
}

fn admitted(reply: &Reply) -> bool {
    !matches!(
        reply.status,
        StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::SEE_OTHER
            | StatusCode::FOUND
    )
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn round_trip_logs_in_maps_claims_and_enforces_roles() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let mut browser = Browser::new(harvest_app(login(&idp)));

    // Before login: Vantage redirects to the login route, the API answers 401.
    let page = browser.get(&format!("{MOUNT}/ui")).await;
    assert_eq!(page.status, StatusCode::SEE_OTHER);
    assert_eq!(
        page.location.as_deref(),
        Some("/api/harvest/auth/oidc/login")
    );
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);

    // The round trip.
    let done = browser.login().await;
    assert_eq!(done.status, StatusCode::SEE_OTHER, "{}", done.body);
    assert_eq!(done.location.as_deref(), Some("/api/harvest/ui"));
    assert_eq!(idp.state.lock().expect("idp").pkce_ok, vec![true]);

    // The `support` group maps to the viewer role. The Vantage index now
    // sends the browser to its own workflow list, not to the login route.
    let index = browser.get(&format!("{MOUNT}/ui")).await;
    assert_eq!(index.location.as_deref(), Some("workflows"));
    let page = browser.get(&format!("{MOUNT}/ui/workflows")).await;
    assert!(admitted(&page), "{} {}", page.status, page.body);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert!(admitted(&api), "{} {}", api.status, api.body);
    let start = browser.post(&format!("{MOUNT}/workflows/wf/start")).await;
    assert!(denied_by_role(&start), "{} {}", start.status, start.body);

    // Logout ends the session.
    let out = browser.post(&format!("{MOUNT}{LOGOUT_PATH}")).await;
    assert_eq!(out.status, StatusCode::OK, "{}", out.body);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_and_custom_claims_map_to_their_roles() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    let mut admin = Browser::new(harvest_app(login(&idp)));
    assert_eq!(admin.login().await.status, StatusCode::SEE_OTHER);
    let start = admin.post(&format!("{MOUNT}/workflows/wf/start")).await;
    assert!(admitted(&start), "{} {}", start.status, start.body);

    set_claims(&idp, json!({ "realm_access": { "roles": ["dlq"] } }));
    let mut dlq = Browser::new(harvest_app(login(&idp)));
    assert_eq!(dlq.login().await.status, StatusCode::SEE_OTHER);
    let replay = dlq.post(&format!("{MOUNT}/dead-letters/replay")).await;
    assert!(admitted(&replay), "{} {}", replay.status, replay.body);
    let discard = dlq.post(&format!("{MOUNT}/dead-letters/discard")).await;
    assert!(
        denied_by_role(&discard),
        "{} {}",
        discard.status,
        discard.body
    );
}

#[tokio::test]
async fn an_identity_with_no_role_cannot_log_in() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["marketing"] }));
    let mut browser = Browser::new(harvest_app(login(&idp)));
    let done = browser.login().await;
    assert_eq!(done.status, StatusCode::FORBIDDEN, "{}", done.body);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_default_role_admits_an_identity_with_no_matching_claim() {
    let idp = start_idp().await;
    set_claims(&idp, json!({}));
    let login = OidcLogin::new(
        provider(&idp),
        roles(),
        claim_map().default_role(ROLE_VIEWER),
    )
    .expect("login config");
    let mut browser = Browser::new(harvest_app(login));
    assert_eq!(browser.login().await.status, StatusCode::SEE_OTHER);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert!(admitted(&api), "{} {}", api.status, api.body);
}

#[tokio::test]
async fn a_tampered_state_is_refused() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    let mut browser = Browser::new(harvest_app(login(&idp)));
    let callback = browser.begin_login().await;
    let (path, query) = callback.split_once('?').expect("query");
    let forged: Vec<String> = query
        .split('&')
        .map(|kv| {
            if kv.starts_with("state=") {
                "state=forged".to_string()
            } else {
                kv.to_string()
            }
        })
        .collect();
    let reply = browser.get(&format!("{path}?{}", forged.join("&"))).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_callback_without_a_login_is_refused() {
    let idp = start_idp().await;
    let mut browser = Browser::new(harvest_app(login(&idp)));
    let reply = browser
        .get(&format!("{MOUNT}{CALLBACK_PATH}?code=abc&state=xyz"))
        .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

#[tokio::test]
async fn a_provider_error_is_refused() {
    let idp = start_idp().await;
    let mut browser = Browser::new(harvest_app(login(&idp)));
    let reply = browser
        .get(&format!(
            "{MOUNT}{CALLBACK_PATH}?error=access_denied&state=x"
        ))
        .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

#[tokio::test]
async fn a_token_signed_by_another_key_is_refused() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    idp.state.lock().expect("idp").rogue_seed = Some([9u8; 32]);
    let mut browser = Browser::new(harvest_app(login(&idp)));
    let reply = browser.login().await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_token_for_another_audience_is_refused() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    idp.state.lock().expect("idp").audience = Some("another-app".to_string());
    let mut browser = Browser::new(harvest_app(login(&idp)));
    let reply = browser.login().await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

#[tokio::test]
async fn an_old_session_must_log_in_again() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let login = login(&idp).with_max_session_age(Duration::ZERO);
    let mut browser = Browser::new(harvest_app(login));
    assert_eq!(browser.login().await.status, StatusCode::SEE_OTHER);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn public_routes_need_no_login() {
    let idp = start_idp().await;
    let mut browser = Browser::new(harvest_app(login(&idp)));
    let health = browser.get(&format!("{MOUNT}/health")).await;
    assert!(admitted(&health), "{}", health.status);
}

#[tokio::test]
async fn a_custom_post_login_redirect_is_used() {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let login = login(&idp)
        .with_post_login_redirect("/dashboards/harvest")
        .expect("local path");
    let mut browser = Browser::new(harvest_app(login));
    let done = browser.login().await;
    assert_eq!(done.location.as_deref(), Some("/dashboards/harvest"));
}

#[tokio::test]
async fn discovery_builds_the_provider_and_checks_the_issuer() {
    let idp = start_idp().await;
    let found = discover_provider(
        &idp.issuer,
        CLIENT_ID,
        CLIENT_SECRET,
        format!("http://harvest.test{MOUNT}{CALLBACK_PATH}"),
    )
    .await
    .expect("discovery");
    assert_eq!(found.issuer.as_deref(), Some(idp.issuer.as_str()));
    assert_eq!(found.authorize_url, format!("{}/authorize", idp.issuer));
    assert_eq!(found.token_url, format!("{}/token", idp.issuer));
    assert_eq!(found.jwks_url, Some(format!("{}/jwks", idp.issuer)));
    assert!(found.scope.split_whitespace().any(|s| s == "openid"));

    // A discovered provider runs the same round trip.
    set_claims(&idp, json!({ "groups": ["support"] }));
    let login = OidcLogin::new(found, roles(), claim_map()).expect("login config");
    let mut browser = Browser::new(harvest_app(login));
    assert_eq!(browser.login().await.status, StatusCode::SEE_OTHER);

    // The document names `idp.issuer`, so another issuer is refused.
    let other = format!("{}/", idp.issuer);
    let err = discover_provider(&other, CLIENT_ID, CLIENT_SECRET, "https://h/cb")
        .await
        .expect_err("mismatch");
    assert!(
        matches!(err, OidcConfigError::IssuerMismatch { .. }),
        "{err}"
    );
}

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
