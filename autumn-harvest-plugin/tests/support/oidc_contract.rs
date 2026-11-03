//! The OIDC login checks that both backends keep (issue #1978, #2469).
//!
//! A mock identity provider runs on a loopback port. It serves discovery,
//! the authorize endpoint, the token endpoint and the JWKS. It signs ID
//! tokens with Ed25519. Each check drives a Harvest app with a cookie jar,
//! as a browser does.
//!
//! `tests/oidc_login.rs` runs the checks on a Postgres-path mount. The
//! SQLite suite runs them on a SQLite app. Each harness builds its own app.
//!
//! The round trip covers the login redirect, PKCE, `state`, `nonce`, the
//! signature check, the claim-to-role map, role enforcement and logout.

// Each test binary uses a different subset of the checks.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest_plugin::api_token::TokenScope;
use autumn_harvest_plugin::oidc::{
    CALLBACK_PATH, LOGIN_PATH, LOGOUT_PATH, OAuth2ProviderConfig, OidcConfigError, OidcLogin,
    discover_provider,
};
use autumn_harvest_plugin::roles::{
    ClaimRoleMap, ClaimRule, HarvestRole, HarvestRoles, ROLE_ADMIN, ROLE_DENIED_ERROR, ROLE_VIEWER,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};
use sha2::Digest as _;
use tower::ServiceExt;

pub const CLIENT_ID: &str = "harvest-vantage";
pub const CLIENT_SECRET: &str = "s3cret";
pub const MOUNT: &str = "/api/harvest";
pub const KID: &str = "mock-key-1";

// ── Mock identity provider ───────────────────────────────────────────────────

/// One issued authorization code.
#[derive(Clone)]
struct Grant {
    nonce: String,
    challenge: String,
}

#[derive(Default)]
pub struct IdpState {
    /// The claims the next code carries, besides the standard ones.
    pub claims: Value,
    /// Override the `aud` claim.
    pub audience: Option<String>,
    /// Sign with this key seed instead of the published key.
    pub rogue_seed: Option<[u8; 32]>,
    codes: HashMap<String, Grant>,
    /// Every `code_verifier` the token endpoint saw, with its PKCE result.
    pub pkce_ok: Vec<bool>,
}

#[derive(Clone)]
pub struct Idp {
    pub issuer: String,
    pub state: Arc<Mutex<IdpState>>,
}

pub const SEED: [u8; 32] = [7u8; 32];

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

pub async fn start_idp() -> Idp {
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

pub fn roles() -> HarvestRoles {
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

pub fn claim_map() -> ClaimRoleMap {
    ClaimRoleMap::new()
        .rule(ClaimRule::new("groups", "harvest-admins", ROLE_ADMIN))
        .rule(ClaimRule::new("groups", "support", ROLE_VIEWER))
        .rule(ClaimRule::new("realm_access.roles", "dlq", "dlq-operator"))
}

pub fn provider(idp: &Idp) -> OAuth2ProviderConfig {
    OAuth2ProviderConfig {
        client_id: CLIENT_ID.to_string(),
        client_secret: CLIENT_SECRET.to_string(),
        authorize_url: format!("{}/authorize", idp.issuer),
        token_url: format!("{}/token", idp.issuer),
        userinfo_url: None,
        redirect_uri: format!("https://harvest.test{MOUNT}{CALLBACK_PATH}"),
        scope: "openid profile email".to_string(),
        issuer: Some(idp.issuer.clone()),
        jwks_url: Some(format!("{}/jwks", idp.issuer)),
        discovery_url: None,
    }
}

/// A minimal browser: one cookie jar, no automatic redirects.
pub struct Browser {
    app: axum::Router,
    pub cookies: HashMap<String, String>,
}

pub struct Reply {
    pub status: StatusCode,
    pub location: Option<String>,
    pub body: String,
}

impl Browser {
    pub fn new(app: axum::Router) -> Self {
        Self {
            app,
            cookies: HashMap::new(),
        }
    }

    pub async fn send(&mut self, method: Method, uri: &str, extra: &[(&str, &str)]) -> Reply {
        // An app in `prod` refuses a request with no `Host`.
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "harvest.test");
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

    pub async fn get(&mut self, uri: &str) -> Reply {
        self.send(Method::GET, uri, &[]).await
    }

    pub async fn post(&mut self, uri: &str) -> Reply {
        self.send(Method::POST, uri, &[("content-type", "application/json")])
            .await
    }

    /// Start a login, follow the provider redirect, and return the callback
    /// path and query that the provider sends the browser to.
    pub async fn begin_login(&mut self) -> String {
        let start = self.get(&format!("{MOUNT}{LOGIN_PATH}")).await;
        assert_eq!(start.status, StatusCode::SEE_OTHER, "login: {}", start.body);
        let authorize = start.location.expect("login redirects to the provider");
        let client = autumn_harvest_plugin::http::client_builder()
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
            .strip_prefix("https://harvest.test")
            .expect("callback on harvest")
            .to_string()
    }

    pub async fn login(&mut self) -> Reply {
        let callback = self.begin_login().await;
        self.get(&callback).await
    }
}

pub fn set_claims(idp: &Idp, claims: Value) {
    idp.state.lock().expect("idp").claims = claims;
}

pub fn login(idp: &Idp) -> OidcLogin {
    OidcLogin::new(provider(idp), roles(), claim_map()).expect("login config")
}

pub fn denied_by_role(reply: &Reply) -> bool {
    reply.status == StatusCode::FORBIDDEN && reply.body.contains(ROLE_DENIED_ERROR)
}

/// The error a handler gives when it runs with no storage pool.
pub const NO_STORE: &str = "storage pool is not configured";

/// Whether the request reached its handler.
///
/// With no store, a handler can answer `400` or `503`. A gate answers `401`,
/// `403` or a redirect. A wrong route answers `404` or `405`. A broken
/// extractor answers `500`. None of those count. A handler that ran and
/// found no store answers `500` too, and names the store, so it counts.
pub fn admitted(reply: &Reply) -> bool {
    reply.body.contains(NO_STORE)
        || !matches!(
            reply.status,
            StatusCode::UNAUTHORIZED
                | StatusCode::FORBIDDEN
                | StatusCode::SEE_OTHER
                | StatusCode::FOUND
                | StatusCode::NOT_FOUND
                | StatusCode::METHOD_NOT_ALLOWED
                | StatusCode::INTERNAL_SERVER_ERROR
        )
}

/// One backend that serves the OIDC checks.
///
/// Each app runs in the `prod` profile behind a session layer. The API is
/// under [`MOUNT`]. A workflow named `wf` exists, or the start route
/// answers for it with no store.
pub trait OidcBackend {
    /// An app with `login`.
    fn app(&self, login: OidcLogin) -> axum::Router;
    /// An app with `login` and API tokens on.
    fn app_with_tokens(&self, login: OidcLogin) -> axum::Router;
    /// Whether the app serves Vantage under `/ui`.
    fn serves_vantage(&self) -> bool;
}

// ── Checks ───────────────────────────────────────────────────────────────────

pub async fn round_trip_logs_in_maps_claims_and_enforces_roles(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let mut browser = Browser::new(backend.app(login(&idp)));

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
    if backend.serves_vantage() {
        let index = browser.get(&format!("{MOUNT}/ui")).await;
        assert_eq!(index.location.as_deref(), Some("workflows"));
        let page = browser.get(&format!("{MOUNT}/ui/workflows")).await;
        assert!(admitted(&page), "{} {}", page.status, page.body);
    }
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

pub async fn admin_and_custom_claims_map_to_their_roles(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    let mut admin = Browser::new(backend.app(login(&idp)));
    assert_eq!(admin.login().await.status, StatusCode::SEE_OTHER);
    let start = admin.post(&format!("{MOUNT}/workflows/wf/start")).await;
    assert!(admitted(&start), "{} {}", start.status, start.body);

    set_claims(&idp, json!({ "realm_access": { "roles": ["dlq"] } }));
    let mut dlq = Browser::new(backend.app(login(&idp)));
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

pub async fn an_identity_with_no_role_cannot_log_in(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["marketing"] }));
    let mut browser = Browser::new(backend.app(login(&idp)));
    let done = browser.login().await;
    assert_eq!(done.status, StatusCode::FORBIDDEN, "{}", done.body);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

pub async fn the_default_role_admits_an_identity_with_no_matching_claim(
    backend: &impl OidcBackend,
) {
    let idp = start_idp().await;
    set_claims(&idp, json!({}));
    let login = OidcLogin::new(
        provider(&idp),
        roles(),
        claim_map().default_role(ROLE_VIEWER),
    )
    .expect("login config");
    let mut browser = Browser::new(backend.app(login));
    assert_eq!(browser.login().await.status, StatusCode::SEE_OTHER);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert!(admitted(&api), "{} {}", api.status, api.body);
}

pub async fn a_tampered_state_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    let mut browser = Browser::new(backend.app(login(&idp)));
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

pub async fn a_callback_without_a_login_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    let mut browser = Browser::new(backend.app(login(&idp)));
    let reply = browser
        .get(&format!("{MOUNT}{CALLBACK_PATH}?code=abc&state=xyz"))
        .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

pub async fn a_provider_error_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    let mut browser = Browser::new(backend.app(login(&idp)));
    let reply = browser
        .get(&format!(
            "{MOUNT}{CALLBACK_PATH}?error=access_denied&state=x"
        ))
        .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

pub async fn a_token_signed_by_another_key_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    idp.state.lock().expect("idp").rogue_seed = Some([9u8; 32]);
    let mut browser = Browser::new(backend.app(login(&idp)));
    let reply = browser.login().await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

pub async fn a_token_for_another_audience_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["harvest-admins"] }));
    idp.state.lock().expect("idp").audience = Some("another-app".to_string());
    let mut browser = Browser::new(backend.app(login(&idp)));
    let reply = browser.login().await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

pub async fn an_old_session_must_log_in_again(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let login = login(&idp).with_max_session_age(Duration::from_secs(1));
    let mut browser = Browser::new(backend.app(login));
    assert_eq!(browser.login().await.status, StatusCode::SEE_OTHER);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert!(admitted(&api), "{} {}", api.status, api.body);
    // The login time has a one-second resolution. Wait past two ticks.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

pub async fn public_routes_need_no_login(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    let mut browser = Browser::new(backend.app(login(&idp)));
    let health = browser.get(&format!("{MOUNT}/health")).await;
    assert!(admitted(&health), "{}", health.status);
}

pub async fn a_custom_post_login_redirect_is_used(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let login = login(&idp)
        .with_post_login_redirect("/dashboards/harvest")
        .expect("local path");
    let mut browser = Browser::new(backend.app(login));
    let done = browser.login().await;
    assert_eq!(done.location.as_deref(), Some("/dashboards/harvest"));
}

pub async fn discovery_builds_the_provider_and_checks_the_issuer(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    let found = discover_provider(
        &idp.issuer,
        CLIENT_ID,
        CLIENT_SECRET,
        format!("https://harvest.test{MOUNT}{CALLBACK_PATH}"),
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
    let mut browser = Browser::new(backend.app(login));
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

pub async fn a_replayed_callback_is_refused_and_keeps_the_session(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let mut browser = Browser::new(backend.app(login(&idp)));
    let callback = browser.begin_login().await;
    assert_eq!(browser.get(&callback).await.status, StatusCode::SEE_OTHER);
    let replay = browser.get(&callback).await;
    assert_eq!(replay.status, StatusCode::UNAUTHORIZED, "{}", replay.body);
    // A stray callback does not log the user out.
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert!(admitted(&api), "{} {}", api.status, api.body);
}

pub async fn a_token_with_a_wrong_nonce_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(
        &idp,
        json!({ "groups": ["harvest-admins"], "nonce": "not-the-session-nonce" }),
    );
    let mut browser = Browser::new(backend.app(login(&idp)));
    let reply = browser.login().await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

pub async fn an_expired_id_token_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    let past = chrono::Utc::now().timestamp() - 600;
    set_claims(
        &idp,
        json!({ "groups": ["harvest-admins"], "exp": past, "iat": past - 300 }),
    );
    let mut browser = Browser::new(backend.app(login(&idp)));
    let reply = browser.login().await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

pub async fn a_token_from_another_issuer_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(
        &idp,
        json!({ "groups": ["harvest-admins"], "iss": "https://evil.example.com" }),
    );
    let mut browser = Browser::new(backend.app(login(&idp)));
    let reply = browser.login().await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

pub async fn login_rotates_the_session_cookie(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let app = backend.app(login(&idp));
    let mut browser = Browser::new(app.clone());
    let callback = browser.begin_login().await;
    let before = browser.cookies.clone();
    assert!(!before.is_empty(), "login sets a session cookie");
    assert_eq!(browser.get(&callback).await.status, StatusCode::SEE_OTHER);
    assert_ne!(browser.cookies, before, "the session id rotates at login");
    // The pre-login cookie does not carry the new principal.
    let mut stale = Browser::new(app);
    stale.cookies = before;
    let api = stale.get(&format!("{MOUNT}/workflows")).await;
    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
}

pub async fn a_cross_site_logout_is_refused(backend: &impl OidcBackend) {
    let idp = start_idp().await;
    set_claims(&idp, json!({ "groups": ["support"] }));
    let mut browser = Browser::new(backend.app(login(&idp)));
    assert_eq!(browser.login().await.status, StatusCode::SEE_OTHER);
    let out = browser
        .send(
            Method::POST,
            &format!("{MOUNT}{LOGOUT_PATH}"),
            &[("sec-fetch-site", "cross-site")],
        )
        .await;
    assert_eq!(out.status, StatusCode::FORBIDDEN, "{}", out.body);
    let api = browser.get(&format!("{MOUNT}/workflows")).await;
    assert!(admitted(&api), "{} {}", api.status, api.body);
}

/// With tokens on, an `hvst_` bearer passes the session boundary to the
/// token layer. Without tokens, it is not a credential.
pub async fn an_api_token_bearer_passes_the_boundary_only_with_tokens_on(
    backend: &impl OidcBackend,
) {
    let idp = start_idp().await;
    let bearer = [("authorization", "Bearer hvst_unverifiable")];
    let mut with_tokens = Browser::new(backend.app_with_tokens(login(&idp)));
    let reply = with_tokens
        .send(Method::GET, &format!("{MOUNT}/workflows"), &bearer)
        .await;
    // With no store the token layer refuses to trust the token (`503`).
    // With a store it finds no such token (`401`).
    let token_layer = (reply.status == StatusCode::SERVICE_UNAVAILABLE
        && reply.body.contains("token store unavailable"))
        || (reply.status == StatusCode::UNAUTHORIZED && reply.body.contains("invalid api token"));
    assert!(token_layer, "{} {}", reply.status, reply.body);

    let mut without = Browser::new(backend.app(login(&idp)));
    let reply = without
        .send(Method::GET, &format!("{MOUNT}/workflows"), &bearer)
        .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
    assert!(!reply.body.contains("api token"), "{}", reply.body);
}
