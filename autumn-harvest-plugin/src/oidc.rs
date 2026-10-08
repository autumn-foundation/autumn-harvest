//! OIDC login for Vantage and the management API (issue #1978).
//!
//! Harvest does the login routes, the claim-to-role map and the session
//! boundary. autumn-web does the protocol and the crypto: PKCE, `state`,
//! `nonce`, the JWKS signature check, and the `iss`, `aud` and `exp` checks.
//! See ADR 0006.
//!
//! # Routes
//!
//! The paths are relative to the management API mount. They sit outside the
//! session boundary and the role layer.
//!
//! | Route | Effect |
//! |---|---|
//! | `GET /auth/oidc/login` | Redirect to the identity provider. |
//! | `GET /auth/oidc/callback` | Finish the login, map claims to roles, then redirect to Vantage. |
//! | `POST /auth/oidc/logout` | Clear the Harvest session. |
//!
//! # The session boundary
//!
//! - A session principal reaches the role layer. Its audit actor is
//!   `oidc:{subject}`.
//! - A `PublicSafe` route needs no session.
//! - A verified `hvst_` token passes, when API tokens are on.
//! - Any other Vantage `GET` gets `302` to the login route.
//! - Any other request gets `401`.
//!
//! The roles are fixed at login. After `max_session_age` the user must log in
//! again. The default age is 12 hours.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::audit::{HEADER_ACTOR, RouteClass};
pub use autumn_web::auth::OAuth2ProviderConfig;
use autumn_web::auth::{OAuth2Callback, oauth2_authorize_url, oauth2_finish_login};
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::Router;
use axum::extract::{Extension, OriginalUri, Query, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Redirect, Response};

use crate::roles::{
    ClaimRoleMap, HarvestRoles, RoleConfigError, RolePrincipal, SESSION_ROLES_KEY, join_role_list,
    parse_role_list,
};

/// The login route, relative to the management API mount.
pub const LOGIN_PATH: &str = "/auth/oidc/login";

/// The callback route, relative to the management API mount.
pub const CALLBACK_PATH: &str = "/auth/oidc/callback";

/// The logout route, relative to the management API mount.
pub const LOGOUT_PATH: &str = "/auth/oidc/logout";

/// The session key that holds the OIDC subject of a session principal.
pub const SESSION_SUBJECT_KEY: &str = "harvest_oidc_subject";

/// The session key that holds the login time, in Unix seconds.
pub const SESSION_AUTH_AT_KEY: &str = "harvest_oidc_auth_at";

/// The audit-actor prefix of a session principal.
pub const OIDC_ACTOR_PREFIX: &str = "oidc:";

/// The default longest session age.
pub const DEFAULT_MAX_SESSION_AGE: Duration = Duration::from_secs(12 * 60 * 60);

/// The longest OIDC subject Harvest accepts, in bytes. OIDC Core sets the
/// same limit.
const MAX_SUBJECT_LEN: usize = 255;

/// The time limit of a discovery request.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// The provider name autumn-web uses for its session keys.
const PROVIDER_NAME: &str = "harvest";

/// A configuration error in an OIDC login.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OidcConfigError {
    /// A role or claim-map error.
    #[error(transparent)]
    Roles(#[from] RoleConfigError),
    /// A required provider field is empty.
    #[error("oidc provider field `{0}` is required")]
    MissingField(&'static str),
    /// A provider URL is not `https`, and its host is not loopback.
    #[error("oidc provider field `{field}` must use https: {url}")]
    InsecureUrl {
        /// The field.
        field: &'static str,
        /// The URL as given.
        url: String,
    },
    /// The scope does not include `openid`.
    #[error("oidc provider scope must include `openid`")]
    MissingOpenidScope,
    /// The post-login redirect is not a local absolute path.
    #[error("post-login redirect must be a local path that starts with one '/': {0:?}")]
    InvalidRedirect(String),
    /// The discovery document could not be read.
    #[error("oidc discovery failed: {0}")]
    Discovery(String),
    /// The discovery document names another issuer.
    #[error("oidc discovery issuer mismatch: expected {expected:?}, found {found:?}")]
    IssuerMismatch {
        /// The issuer asked for.
        expected: String,
        /// The issuer in the document.
        found: String,
    },
}

#[derive(Clone, Debug)]
struct OidcInner {
    provider: OAuth2ProviderConfig,
    roles: HarvestRoles,
    claim_map: ClaimRoleMap,
    max_session_age: Duration,
    post_login_redirect: Option<String>,
}

/// A validated OIDC login configuration.
///
/// It is cheap to clone.
#[derive(Clone, Debug)]
pub struct OidcLogin {
    inner: Arc<OidcInner>,
}

impl OidcLogin {
    /// Validate a login configuration.
    ///
    /// The provider needs `client_id`, `authorize_url`, `token_url`,
    /// `redirect_uri`, `issuer` and `jwks_url`. Each provider URL must use
    /// `https`, unless its host is loopback. The scope must include `openid`.
    ///
    /// # Errors
    ///
    /// Returns an [`OidcConfigError`] for the first problem found. A claim
    /// rule that names an undefined role is
    /// [`OidcConfigError::Roles`].
    pub fn new(
        provider: OAuth2ProviderConfig,
        roles: HarvestRoles,
        claim_map: ClaimRoleMap,
    ) -> Result<Self, OidcConfigError> {
        validate_provider(&provider)?;
        claim_map.validate(&roles)?;
        Ok(Self {
            inner: Arc::new(OidcInner {
                provider,
                roles,
                claim_map,
                max_session_age: DEFAULT_MAX_SESSION_AGE,
                post_login_redirect: None,
            }),
        })
    }

    /// Set the longest session age. After it, the user must log in again.
    #[must_use]
    pub fn with_max_session_age(self, age: Duration) -> Self {
        let mut inner = Arc::unwrap_or_clone(self.inner);
        inner.max_session_age = age;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Set the page the callback redirects to.
    ///
    /// The default is the Vantage root under the mount, such as
    /// `/api/harvest/ui`.
    ///
    /// # Errors
    ///
    /// Returns [`OidcConfigError::InvalidRedirect`] unless `path` starts with
    /// exactly one `/` and has no backslash and no control character.
    pub fn with_post_login_redirect(
        self,
        path: impl Into<String>,
    ) -> Result<Self, OidcConfigError> {
        let path = path.into();
        if !is_local_path(&path) {
            return Err(OidcConfigError::InvalidRedirect(path));
        }
        let mut inner = Arc::unwrap_or_clone(self.inner);
        inner.post_login_redirect = Some(path);
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// The role set.
    #[must_use]
    pub fn roles(&self) -> &HarvestRoles {
        &self.inner.roles
    }

    /// The claim map.
    #[must_use]
    pub fn claim_map(&self) -> &ClaimRoleMap {
        &self.inner.claim_map
    }

    /// The longest session age.
    #[must_use]
    pub fn max_session_age(&self) -> Duration {
        self.inner.max_session_age
    }
}

/// Check the required fields, the URL schemes and the scope.
fn validate_provider(provider: &OAuth2ProviderConfig) -> Result<(), OidcConfigError> {
    let required: [(&'static str, Option<&str>); 6] = [
        ("client_id", Some(provider.client_id.as_str())),
        ("authorize_url", Some(provider.authorize_url.as_str())),
        ("token_url", Some(provider.token_url.as_str())),
        ("redirect_uri", Some(provider.redirect_uri.as_str())),
        ("issuer", provider.issuer.as_deref()),
        ("jwks_url", provider.jwks_url.as_deref()),
    ];
    for (field, value) in required {
        if value.is_none_or(|v| v.trim().is_empty()) {
            return Err(OidcConfigError::MissingField(field));
        }
    }
    let urls: [(&'static str, Option<&str>); 5] = [
        ("authorize_url", Some(provider.authorize_url.as_str())),
        ("token_url", Some(provider.token_url.as_str())),
        ("issuer", provider.issuer.as_deref()),
        ("jwks_url", provider.jwks_url.as_deref()),
        ("userinfo_url", provider.userinfo_url.as_deref()),
    ];
    for (field, url) in urls {
        if let Some(url) = url
            && !is_secure_url(url)
        {
            return Err(OidcConfigError::InsecureUrl {
                field,
                url: url.to_string(),
            });
        }
    }
    if !provider.scope.split_whitespace().any(|s| s == "openid") {
        return Err(OidcConfigError::MissingOpenidScope);
    }
    Ok(())
}

/// Whether `url` uses `https`, or uses `http` on a loopback host.
fn is_secure_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    match parsed.scheme() {
        "https" => true,
        "http" => parsed.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        }),
        _ => false,
    }
}

/// Whether `path` is a local absolute path, not a protocol-relative URL.
fn is_local_path(path: &str) -> bool {
    path.starts_with('/')
        && !path.starts_with("//")
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
}

/// The OIDC discovery fields Harvest reads.
#[derive(serde::Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
    #[serde(default)]
    userinfo_endpoint: Option<String>,
}

/// Read the OIDC discovery document of `issuer` and build a provider.
///
/// The document is at `{issuer}/.well-known/openid-configuration`. Its
/// `issuer` must equal `issuer` exactly, as OIDC Discovery requires. The
/// scope is `openid profile email`. Pass the result to [`OidcLogin::new`],
/// which checks the URLs.
///
/// # Errors
///
/// Returns [`OidcConfigError::InsecureUrl`] for a plain-HTTP issuer on a
/// non-loopback host, [`OidcConfigError::Discovery`] when the document
/// cannot be read, and [`OidcConfigError::IssuerMismatch`] when it names
/// another issuer.
pub async fn discover_provider(
    issuer: &str,
    client_id: impl Into<String>,
    client_secret: impl Into<String>,
    redirect_uri: impl Into<String>,
) -> Result<OAuth2ProviderConfig, OidcConfigError> {
    if !is_secure_url(issuer) {
        return Err(OidcConfigError::InsecureUrl {
            field: "issuer",
            url: issuer.to_string(),
        });
    }
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let discovery = |e: reqwest::Error| OidcConfigError::Discovery(e.to_string());
    let body = reqwest::Client::builder()
        .timeout(DISCOVERY_TIMEOUT)
        .build()
        .map_err(discovery)?
        .get(&url)
        .send()
        .await
        .map_err(discovery)?
        .error_for_status()
        .map_err(discovery)?
        .bytes()
        .await
        .map_err(discovery)?;
    let document: DiscoveryDocument =
        serde_json::from_slice(&body).map_err(|e| OidcConfigError::Discovery(e.to_string()))?;
    if document.issuer != issuer {
        return Err(OidcConfigError::IssuerMismatch {
            expected: issuer.to_string(),
            found: document.issuer,
        });
    }
    Ok(OAuth2ProviderConfig {
        client_id: client_id.into(),
        client_secret: client_secret.into(),
        authorize_url: document.authorization_endpoint,
        token_url: document.token_endpoint,
        userinfo_url: document.userinfo_endpoint,
        redirect_uri: redirect_uri.into(),
        scope: "openid profile email".to_string(),
        issuer: Some(document.issuer),
        jwks_url: Some(document.jwks_uri),
        discovery_url: Some(issuer.to_string()),
    })
}

// ── Routes ────────────────────────────────────────────────────────────────────

/// The mount prefix of a nested request, such as `/api/harvest`.
///
/// It is the original path less the path the nested router sees.
fn mount_prefix(original: &Uri, nested: &Uri) -> String {
    original
        .path()
        .strip_suffix(nested.path())
        .unwrap_or_default()
        .to_string()
}

/// A JSON error with `status`.
fn error(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(serde_json::json!({ "error": message }))).into_response()
}

fn no_session_layer() -> Response {
    tracing::error!("harvest: oidc login needs an autumn-web session layer");
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "oidc login needs a session layer",
    )
}

/// `GET /auth/oidc/login`: redirect to the identity provider.
async fn login_handler(
    State(login): State<OidcLogin>,
    session: Option<Extension<Session>>,
) -> Response {
    let Some(Extension(session)) = session else {
        return no_session_layer();
    };
    match oauth2_authorize_url(&session, PROVIDER_NAME, &login.inner.provider).await {
        Ok(url) => Redirect::to(&url).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "harvest: oidc authorize url failed");
            error(StatusCode::INTERNAL_SERVER_ERROR, "oidc login failed")
        }
    }
}

/// Remove the Harvest principal keys from `session`.
async fn clear_principal(session: &Session) {
    session.remove(SESSION_SUBJECT_KEY).await;
    session.remove(SESSION_ROLES_KEY).await;
    session.remove(SESSION_AUTH_AT_KEY).await;
}

/// Whether `subject` is 1 to 255 visible ASCII characters.
fn valid_subject(subject: &str) -> bool {
    !subject.is_empty()
        && subject.len() <= MAX_SUBJECT_LEN
        && subject.bytes().all(|b| b.is_ascii_graphic())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// `GET /auth/oidc/callback`: finish the login and map claims to roles.
async fn callback_handler(
    State(login): State<OidcLogin>,
    session: Option<Extension<Session>>,
    original: OriginalUri,
    uri: Uri,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(Extension(session)) = session else {
        return no_session_layer();
    };
    // A new login replaces any earlier principal, even when it fails.
    clear_principal(&session).await;
    if let Some(provider_error) = query.get("error") {
        tracing::warn!(error = %provider_error, "harvest: oidc provider refused the login");
        return error(StatusCode::UNAUTHORIZED, "oidc login failed");
    }
    let (Some(code), Some(state)) = (query.get("code"), query.get("state")) else {
        return error(
            StatusCode::BAD_REQUEST,
            "oidc callback needs code and state",
        );
    };
    let callback = OAuth2Callback {
        code: code.clone(),
        state: state.clone(),
    };
    let provider = &login.inner.provider;
    let identity = match oauth2_finish_login(&session, PROVIDER_NAME, provider, &callback).await {
        Ok(identity) => identity,
        Err(e) => {
            tracing::warn!(error = %e, "harvest: oidc login failed");
            return error(StatusCode::UNAUTHORIZED, "oidc login failed");
        }
    };
    if !valid_subject(&identity.subject) {
        tracing::warn!("harvest: oidc subject is not 1 to 255 visible ASCII characters");
        return error(StatusCode::UNAUTHORIZED, "oidc login failed");
    }
    let roles = login.inner.claim_map.roles_for(&identity.raw_claims);
    if roles.is_empty() {
        tracing::warn!(
            subject = %identity.subject,
            "harvest: oidc identity maps to no Harvest role (403)"
        );
        return error(StatusCode::FORBIDDEN, "no harvest role for this identity");
    }
    session
        .insert(SESSION_SUBJECT_KEY, identity.subject.clone())
        .await;
    session
        .insert(SESSION_ROLES_KEY, join_role_list(&roles))
        .await;
    session
        .insert(SESSION_AUTH_AT_KEY, now_unix().to_string())
        .await;
    tracing::info!(
        subject = %identity.subject,
        roles = %join_role_list(&roles),
        "harvest: oidc login"
    );
    let target = login
        .inner
        .post_login_redirect
        .clone()
        .unwrap_or_else(|| format!("{}/ui", mount_prefix(&original.0, &uri)));
    Redirect::to(&target).into_response()
}

/// `POST /auth/oidc/logout`: end the Harvest session.
async fn logout_handler(
    session: Option<Extension<Session>>,
    original: OriginalUri,
    uri: Uri,
) -> Response {
    if let Some(Extension(session)) = session {
        clear_principal(&session).await;
        session.destroy().await;
    }
    let login = format!("{}{LOGIN_PATH}", mount_prefix(&original.0, &uri));
    let page = maud::html! {
        (maud::DOCTYPE)
        html lang="en" {
            head { meta charset="utf-8"; title { "Signed out" } }
            body {
                main {
                    h1 { "Signed out" }
                    p { "Your Harvest session has ended." }
                    p { a href=(login) { "Sign in again" } }
                }
            }
        }
    };
    Html(page.into_string()).into_response()
}

/// The login routes. Merge them outside the session boundary.
pub(crate) fn login_router(login: OidcLogin) -> Router<()> {
    Router::new()
        .route(LOGIN_PATH, axum::routing::get(login_handler))
        .route(CALLBACK_PATH, axum::routing::get(callback_handler))
        .route(
            LOGOUT_PATH,
            axum::routing::post(logout_handler).route_layer(axum::middleware::from_fn(
                crate::same_origin::require_same_origin,
            )),
        )
        .with_state(login)
}

/// Wrap `router` in the session boundary and merge the login routes.
///
/// `router` already carries the admin-auth stack, the role layer included.
pub(crate) fn apply_oidc(router: Router<()>, login: &OidcLogin, api_tokens: bool) -> Router<()> {
    router
        .layer(axum::middleware::from_fn_with_state(
            (login.clone(), api_tokens),
            require_oidc_session,
        ))
        .merge(login_router(login.clone()))
}

// ── Session boundary ──────────────────────────────────────────────────────────

/// The subject of a live session principal, or `None`.
///
/// A session older than the longest age loses its principal keys.
async fn session_subject(session: &Session, max_age: Duration) -> Option<String> {
    let subject = session.get(SESSION_SUBJECT_KEY).await?;
    let auth_at = session
        .get(SESSION_AUTH_AT_KEY)
        .await
        .and_then(|v| v.parse::<u64>().ok());
    let fresh = auth_at.is_some_and(|at| now_unix().saturating_sub(at) < max_age.as_secs());
    if fresh && valid_subject(&subject) {
        Some(subject)
    } else {
        clear_principal(session).await;
        None
    }
}

/// Remove an inbound `oidc:` actor, so no caller can claim a session subject.
fn strip_reserved_actor(request: &mut Request) {
    let reserved = request
        .headers()
        .get(HEADER_ACTOR)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with(OIDC_ACTOR_PREFIX));
    if reserved {
        request.headers_mut().remove(HEADER_ACTOR);
    }
}

/// Set the authoritative actor of a session principal.
fn set_actor(request: &mut Request, subject: &str) {
    request.headers_mut().remove(HEADER_ACTOR);
    if let Ok(value) = HeaderValue::from_str(&format!("{OIDC_ACTOR_PREFIX}{subject}")) {
        request.headers_mut().insert(HEADER_ACTOR, value);
    }
}

/// Whether `path` is under the Vantage mount (`/ui`).
fn is_vantage_path(path: &str) -> bool {
    path == "/ui" || path.starts_with("/ui/")
}

/// The session boundary (issue #1978).
///
/// It runs outside the admin-auth stack, in the place of host auth.
pub(crate) async fn require_oidc_session(
    State((login, api_tokens)): State<(OidcLogin, bool)>,
    mut request: Request,
    next: Next,
) -> Response {
    strip_reserved_actor(&mut request);
    if *request.method() == Method::OPTIONS {
        return next.run(request).await;
    }
    let session = request.extensions().get::<Session>().cloned();
    if let Some(session) = session
        && let Some(subject) = session_subject(&session, login.inner.max_session_age).await
    {
        set_actor(&mut request, &subject);
        return next.run(request).await;
    }
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    if crate::api::classified_route(&method, &path) == Some(RouteClass::PublicSafe) {
        return next.run(request).await;
    }
    if api_tokens && crate::api_token::harvest_bearer(request.headers()).is_some() {
        // The token layer verifies it, and refuses a bad one with 401.
        return next.run(request).await;
    }
    if matches!(method, Method::GET | Method::HEAD) && is_vantage_path(&path) {
        let original = request
            .extensions()
            .get::<OriginalUri>()
            .map_or_else(|| request.uri().clone(), |o| o.0.clone());
        let prefix = mount_prefix(&original, request.uri());
        return Redirect::to(&format!("{prefix}{LOGIN_PATH}")).into_response();
    }
    error(StatusCode::UNAUTHORIZED, "authentication required")
}

/// The gate on a generated MCP tool route under OIDC login (issue #1978).
///
/// A tool route has no route class. A `GET` tool needs a role with a `read`
/// scope or wider. Any other tool needs `mutate` or wider. A caller with no
/// live session principal gets `401`.
pub(crate) async fn gate_mcp_tool(
    State(login): State<OidcLogin>,
    mut request: Request,
    next: Next,
) -> Response {
    strip_reserved_actor(&mut request);
    if *request.method() == Method::OPTIONS {
        return next.run(request).await;
    }
    let Some(session) = request.extensions().get::<Session>().cloned() else {
        return error(StatusCode::UNAUTHORIZED, "authentication required");
    };
    let Some(subject) = session_subject(&session, login.inner.max_session_age).await else {
        return error(StatusCode::UNAUTHORIZED, "authentication required");
    };
    let roles = session
        .get(SESSION_ROLES_KEY)
        .await
        .map(|v| parse_role_list(&v))
        .unwrap_or_default();
    if !login
        .roles()
        .allows_by_method(roles.iter().map(String::as_str), request.method())
    {
        return error(StatusCode::FORBIDDEN, crate::roles::ROLE_DENIED_ERROR);
    }
    set_actor(&mut request, &subject);
    request.extensions_mut().insert(RolePrincipal::new(roles));
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roles::{ROLE_OPERATOR, ROLE_VIEWER};
    use axum::body::Body;
    use tower::ServiceExt;

    fn provider() -> OAuth2ProviderConfig {
        OAuth2ProviderConfig {
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            authorize_url: "https://idp.example.com/authorize".to_string(),
            token_url: "https://idp.example.com/token".to_string(),
            userinfo_url: None,
            redirect_uri: "https://harvest.example.com/api/harvest/auth/oidc/callback".to_string(),
            scope: "openid".to_string(),
            issuer: Some("https://idp.example.com".to_string()),
            jwks_url: Some("https://idp.example.com/jwks".to_string()),
            discovery_url: None,
        }
    }

    fn login() -> OidcLogin {
        OidcLogin::new(provider(), HarvestRoles::builtin(), ClaimRoleMap::new()).expect("valid")
    }

    #[test]
    fn secure_urls_are_https_or_loopback() {
        assert!(is_secure_url("https://idp.example.com"));
        assert!(is_secure_url("http://127.0.0.1:8080/x"));
        assert!(is_secure_url("http://[::1]:8080/x"));
        assert!(is_secure_url("http://LOCALHOST/x"));
        assert!(!is_secure_url("http://idp.example.com"));
        assert!(!is_secure_url("http://127.0.0.1.example.com"));
        assert!(!is_secure_url("ftp://idp.example.com"));
        assert!(!is_secure_url("not a url"));
    }

    #[test]
    fn local_paths_exclude_other_origins() {
        assert!(is_local_path("/ui"));
        assert!(is_local_path("/a/b?c=d"));
        for bad in ["", "ui", "//x", "/\\x", "https://x", "/a\nb"] {
            assert!(!is_local_path(bad), "{bad:?}");
        }
    }

    #[test]
    fn userinfo_url_must_be_secure_too() {
        let mut p = provider();
        p.userinfo_url = Some("http://idp.example.com/userinfo".to_string());
        assert_eq!(
            OidcLogin::new(p, HarvestRoles::builtin(), ClaimRoleMap::new()).err(),
            Some(OidcConfigError::InsecureUrl {
                field: "userinfo_url",
                url: "http://idp.example.com/userinfo".to_string(),
            })
        );
    }

    #[test]
    fn mount_prefix_strips_the_nested_path() {
        let original: Uri = "/api/harvest/ui/workflows".parse().expect("uri");
        let nested: Uri = "/ui/workflows".parse().expect("uri");
        assert_eq!(mount_prefix(&original, &nested), "/api/harvest");
        assert_eq!(mount_prefix(&nested, &nested), "");
    }

    #[test]
    fn subjects_are_visible_ascii() {
        assert!(valid_subject("user-42"));
        assert!(!valid_subject(""));
        assert!(!valid_subject("a b"));
        assert!(!valid_subject("é"));
        assert!(!valid_subject(&"x".repeat(MAX_SUBJECT_LEN + 1)));
    }

    #[test]
    fn builders_keep_their_values() {
        let l = login().with_max_session_age(Duration::from_secs(60));
        assert_eq!(l.max_session_age(), Duration::from_secs(60));
        assert_eq!(login().max_session_age(), DEFAULT_MAX_SESSION_AGE);
        assert_eq!(l.roles().names().count(), 3);
        assert_eq!(l.claim_map(), &ClaimRoleMap::new());
    }

    /// An inner handler that echoes the actor header it received.
    fn echo_actor() -> Router<()> {
        Router::new().route(
            "/workflows",
            axum::routing::get(|headers: axum::http::HeaderMap| async move {
                headers
                    .get(HEADER_ACTOR)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .to_string()
            }),
        )
    }

    fn session(subject: Option<&str>, roles: &str, auth_at: u64) -> Session {
        let mut data = HashMap::new();
        if let Some(subject) = subject {
            data.insert(SESSION_SUBJECT_KEY.to_string(), subject.to_string());
        }
        data.insert(SESSION_ROLES_KEY.to_string(), roles.to_string());
        data.insert(SESSION_AUTH_AT_KEY.to_string(), auth_at.to_string());
        Session::new_for_test("s".to_string(), data)
    }

    async fn call(
        router: Router<()>,
        method: Method,
        session: Option<Session>,
        actor: &str,
    ) -> (StatusCode, String) {
        let mut request = axum::http::Request::builder()
            .method(method)
            .uri("/workflows")
            .header(HEADER_ACTOR, actor)
            .body(Body::empty())
            .expect("request");
        if let Some(session) = session {
            request.extensions_mut().insert(session);
        }
        let response = router.oneshot(request).await.expect("served");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .expect("body");
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    fn bounded() -> Router<()> {
        echo_actor().layer(axum::middleware::from_fn_with_state(
            (login(), false),
            require_oidc_session,
        ))
    }

    #[tokio::test]
    async fn the_boundary_sets_the_actor_of_a_session_principal() {
        let s = session(Some("user-42"), ROLE_VIEWER, now_unix());
        let out = call(bounded(), Method::GET, Some(s), "oidc:someone-else").await;
        assert_eq!(out, (StatusCode::OK, "oidc:user-42".to_string()));
    }

    #[tokio::test]
    async fn the_boundary_refuses_a_caller_with_no_principal() {
        let out = call(bounded(), Method::GET, None, "oidc:admin").await;
        assert_eq!(out.0, StatusCode::UNAUTHORIZED);
        let stale = session(Some("user-42"), ROLE_VIEWER, 0);
        let out = call(bounded(), Method::GET, Some(stale.clone()), "x").await;
        assert_eq!(out.0, StatusCode::UNAUTHORIZED);
        // A stale session loses its principal keys.
        assert!(stale.get(SESSION_SUBJECT_KEY).await.is_none());
        assert!(stale.get(SESSION_ROLES_KEY).await.is_none());
    }

    #[tokio::test]
    async fn options_passes_but_loses_a_reserved_actor() {
        let out = call(bounded(), Method::OPTIONS, None, "oidc:admin").await;
        assert_ne!(out.0, StatusCode::UNAUTHORIZED);
    }

    fn mcp_gated() -> Router<()> {
        Router::new()
            .route(
                "/workflows",
                axum::routing::get(|| async { "read" }).post(|| async { "write" }),
            )
            .layer(axum::middleware::from_fn_with_state(login(), gate_mcp_tool))
    }

    #[tokio::test]
    async fn the_mcp_gate_needs_a_scope_for_the_method() {
        let viewer = || Some(session(Some("u"), ROLE_VIEWER, now_unix()));
        let operator = || Some(session(Some("u"), ROLE_OPERATOR, now_unix()));
        assert_eq!(
            call(mcp_gated(), Method::GET, viewer(), "x").await.0,
            StatusCode::OK
        );
        assert_eq!(
            call(mcp_gated(), Method::POST, viewer(), "x").await.0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            call(mcp_gated(), Method::POST, operator(), "x").await.0,
            StatusCode::OK
        );
        assert_eq!(
            call(mcp_gated(), Method::GET, None, "x").await.0,
            StatusCode::UNAUTHORIZED
        );
        let no_subject = Some(session(None, ROLE_OPERATOR, now_unix()));
        assert_eq!(
            call(mcp_gated(), Method::POST, no_subject, "x").await.0,
            StatusCode::UNAUTHORIZED
        );
    }
}
