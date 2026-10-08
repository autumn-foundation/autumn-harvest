//! OIDC login for Vantage and the management API (issue #1978).
//!
//! Harvest owns the login routes, the claim-to-role map and the session
//! boundary. autumn-web owns the protocol and the crypto. It checks PKCE,
//! `state`, `nonce`, the JWKS signature, `iss`, `aud`, `exp` and `nbf`.
//! See ADR 0006.
//!
//! Harvest requires a signed ID token. It refuses a provider with a
//! `userinfo_url`, because the autumn-web userinfo path checks no signature,
//! no audience and no nonce.
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
//! | `POST /auth/oidc/logout` | Remove the Harvest keys from the session. A cross-site post is refused. |
//!
//! # The session boundary
//!
//! - A session principal reaches the role layer. Its audit actor is
//!   `oidc:{subject}@{issuer}`.
//! - A request with a host `RoleGrant` reaches the role layer.
//! - A `PublicSafe` route needs no session.
//! - An `hvst_` bearer passes when API tokens are on. The token layer
//!   verifies it.
//! - Any other Vantage `GET` gets `303` to the login route.
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

use crate::roles::{ClaimRoleMap, HarvestRoles, RoleConfigError, join_role_list};

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
pub const OIDC_ACTOR_PREFIX: &str = crate::roles::OIDC_ACTOR_PREFIX;

/// The default longest session age.
pub const DEFAULT_MAX_SESSION_AGE: Duration = Duration::from_secs(12 * 60 * 60);

/// The shortest session age. The login time has a one-second resolution.
const MIN_SESSION_AGE: Duration = Duration::from_secs(1);

/// The longest OIDC subject Harvest accepts, in bytes. OIDC Core sets the
/// same limit.
const MAX_SUBJECT_LEN: usize = 255;

/// The largest discovery document Harvest reads.
const MAX_DISCOVERY_BYTES: usize = 1024 * 1024;

/// The time limit of a discovery request.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// The session key that holds the role names of an OIDC session principal.
///
/// It is not the shared [`crate::roles::SESSION_ROLES_KEY`]. A plain role
/// mount reads that key with no login check, so a login never writes it.
/// The boundary reads this key only after it checks the login binding.
pub const SESSION_OIDC_ROLES_KEY: &str = "harvest_oidc_roles";

/// The largest clock skew a login time may show, in seconds.
///
/// A login time further in the future is refused. Otherwise a fast clock at
/// login would make a session last longer than its age limit.
const MAX_CLOCK_SKEW_SECS: u64 = 60;

/// The session key that binds the principal to the login that made it.
///
/// Two mounts can share one autumn-web session. A principal from one login
/// is not a principal on a mount with another login.
pub const SESSION_LOGIN_KEY: &str = "harvest_oidc_login";

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
    /// The provider sets `userinfo_url`. Harvest needs a signed ID token.
    #[error("oidc provider must not set `userinfo_url`: Harvest requires a signed ID token")]
    UserinfoNotSupported,
    /// The issuer is a Microsoft multi-tenant alias. autumn-web then accepts
    /// the unverified issuer of any tenant.
    #[error("oidc issuer {0:?} is a multi-tenant alias: use the issuer of one tenant")]
    MultiTenantIssuer(String),
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

#[derive(Clone)]
struct OidcInner {
    /// The session binding of this login. See [`OidcLogin::fingerprint`].
    fingerprint: String,
    provider: OAuth2ProviderConfig,
    roles: HarvestRoles,
    claim_map: ClaimRoleMap,
    max_session_age: Duration,
    post_login_redirect: Option<String>,
}

impl std::fmt::Debug for OidcInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The autumn-web provider type prints its client secret. This does not.
        f.debug_struct("OidcInner")
            .field("client_id", &self.provider.client_id)
            .field("client_secret", &"<redacted>")
            .field("issuer", &self.provider.issuer)
            .field("redirect_uri", &self.provider.redirect_uri)
            .field("roles", &self.roles)
            .field("claim_map", &self.claim_map)
            .field("max_session_age", &self.max_session_age)
            .field("post_login_redirect", &self.post_login_redirect)
            .finish_non_exhaustive()
    }
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
    /// `redirect_uri`, `issuer` and `jwks_url`. It must not set
    /// `userinfo_url`. Each URL must use `https`, unless its host is loopback.
    /// The scope must include `openid`.
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
        let fingerprint = login_fingerprint(&provider, &roles, &claim_map);
        Ok(Self {
            inner: Arc::new(OidcInner {
                fingerprint,
                provider,
                roles,
                claim_map,
                max_session_age: DEFAULT_MAX_SESSION_AGE,
                post_login_redirect: None,
            }),
        })
    }

    /// Set the longest session age. After it, the user must log in again.
    ///
    /// The login time has a resolution of one second, so an age under one
    /// second becomes one second. A shorter age would expire each session at
    /// once and loop through the identity provider.
    #[must_use]
    pub fn with_max_session_age(self, age: Duration) -> Self {
        let mut inner = Arc::unwrap_or_clone(self.inner);
        inner.max_session_age = age.max(MIN_SESSION_AGE);
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

    /// The identity of this login: `{client_id}@{issuer}#{digest}`.
    ///
    /// The digest covers the redirect URI, the role set and the claim map.
    /// So two mounts that share a client, but not a policy, never accept each
    /// other's principal. The session stores it under [`SESSION_LOGIN_KEY`].
    fn fingerprint(&self) -> String {
        self.inner.fingerprint.clone()
    }

    /// The issuer, for the audit actor.
    fn issuer(&self) -> &str {
        self.inner.provider.issuer.as_deref().unwrap_or_default()
    }

    /// The provider name autumn-web puts in its session keys.
    ///
    /// It holds the fingerprint, so two logins in one session keep separate
    /// `state`, `nonce` and PKCE values.
    fn provider_name(&self) -> String {
        format!("harvest:{}", self.fingerprint())
    }
}

/// The session binding of a login. See [`OidcLogin::fingerprint`].
fn login_fingerprint(
    provider: &OAuth2ProviderConfig,
    roles: &HarvestRoles,
    claim_map: &ClaimRoleMap,
) -> String {
    let policy = format!(
        "{}:{}|{}|{}",
        provider.redirect_uri.len(),
        provider.redirect_uri,
        roles.policy_text(),
        claim_map.policy_text()
    );
    format!(
        "{}@{}#{:016x}",
        provider.client_id,
        provider.issuer.as_deref().unwrap_or_default(),
        fnv1a_64(policy.as_bytes())
    )
}

/// FNV-1a, 64 bit.
///
/// The value must be the same in every process, so a session stays valid
/// across instances. The standard hasher is seeded per process. The digest
/// is a namespace, not a secret: the operator writes every input.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
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
    // For these Microsoft aliases, autumn-web adds the token's own `iss` to
    // the accepted issuers. Any tenant could then log in.
    if let Some(issuer) = &provider.issuer
        && issuer.contains("login.microsoftonline.com")
        && ["/common/", "/organizations/", "/consumers/"]
            .iter()
            .any(|alias| issuer.contains(alias))
    {
        return Err(OidcConfigError::MultiTenantIssuer(issuer.clone()));
    }
    // The autumn-web userinfo path checks no signature, audience or nonce.
    if provider.userinfo_url.is_some() {
        return Err(OidcConfigError::UserinfoNotSupported);
    }
    let urls: [(&'static str, Option<&str>); 5] = [
        ("authorize_url", Some(provider.authorize_url.as_str())),
        ("token_url", Some(provider.token_url.as_str())),
        ("redirect_uri", Some(provider.redirect_uri.as_str())),
        ("issuer", provider.issuer.as_deref()),
        ("jwks_url", provider.jwks_url.as_deref()),
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
}

/// Read the OIDC discovery document of `issuer` and build a provider.
///
/// The document is at `{issuer}/.well-known/openid-configuration`. Harvest
/// follows no redirect and reads at most 1 MiB. Its
/// `issuer` must equal `issuer` exactly, as OIDC Discovery requires. The
/// scope is `openid profile email`. The result has no `userinfo_url`, so a
/// login needs a signed ID token. Pass it to [`OidcLogin::new`], which checks
/// the URLs.
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
    let mut response = reqwest::Client::builder()
        .timeout(DISCOVERY_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(discovery)?
        .get(&url)
        .send()
        .await
        .map_err(discovery)?
        .error_for_status()
        .map_err(discovery)?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(discovery)? {
        if body.len() + chunk.len() > MAX_DISCOVERY_BYTES {
            return Err(OidcConfigError::Discovery(format!(
                "document is larger than {MAX_DISCOVERY_BYTES} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
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
        userinfo_url: None,
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
    match oauth2_authorize_url(&session, &login.provider_name(), &login.inner.provider).await {
        Ok(url) => Redirect::to(&url).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "harvest: oidc authorize url failed");
            error(StatusCode::INTERNAL_SERVER_ERROR, "oidc login failed")
        }
    }
}

/// Remove the Harvest principal keys from `session`.
async fn clear_principal(session: &Session) {
    // autumn-web writes `auth_provider` at a verified login.
    session.remove("auth_provider").await;
    session.remove(SESSION_SUBJECT_KEY).await;
    session.remove(SESSION_OIDC_ROLES_KEY).await;
    session.remove(SESSION_AUTH_AT_KEY).await;
    session.remove(SESSION_LOGIN_KEY).await;
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
    let provider_name = login.provider_name();
    let identity = match oauth2_finish_login(&session, &provider_name, provider, &callback).await {
        Ok(identity) => identity,
        Err(e) => {
            tracing::warn!(error = %e, "harvest: oidc login failed");
            return error(StatusCode::UNAUTHORIZED, "oidc login failed");
        }
    };
    // A verified login replaces any earlier principal, even when it maps to no
    // role. A forged or stray callback never reaches this point, so it cannot
    // log the user out.
    clear_principal(&session).await;
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
        .insert(SESSION_OIDC_ROLES_KEY, join_role_list(&roles))
        .await;
    session
        .insert(SESSION_AUTH_AT_KEY, now_unix().to_string())
        .await;
    session.insert(SESSION_LOGIN_KEY, login.fingerprint()).await;
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

/// `POST /auth/oidc/logout`: remove the Harvest keys from the session.
///
/// The host app can share the session, so its own keys stay. The session id
/// rotates, so an old cookie cannot reach the next login.
async fn logout_handler(
    session: Option<Extension<Session>>,
    original: OriginalUri,
    uri: Uri,
) -> Response {
    if let Some(Extension(session)) = session {
        clear_principal(&session).await;
        session.rotate_id().await;
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

/// The subject of a live session principal of `login`, or `None`.
///
/// A principal that another login made is not one here. Its keys stay, so a
/// visit to this mount does not log the user out of the other one. A session
/// older than the longest age loses its principal keys.
async fn session_subject(session: &Session, login: &OidcLogin) -> Option<String> {
    let max_age = login.inner.max_session_age;
    let subject = session.get(SESSION_SUBJECT_KEY).await?;
    if session.get(SESSION_LOGIN_KEY).await != Some(login.fingerprint()) {
        return None;
    }
    let auth_at = session
        .get(SESSION_AUTH_AT_KEY)
        .await
        .and_then(|v| v.parse::<u64>().ok());
    let now = now_unix();
    let fresh = auth_at.is_some_and(|at| {
        at <= now.saturating_add(MAX_CLOCK_SKEW_SECS) && now.saturating_sub(at) < max_age.as_secs()
    });
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
///
/// The actor is `oidc:{subject}@{issuer}`, because `sub` is unique only
/// within one issuer.
fn set_actor(request: &mut Request, subject: &str, issuer: &str) {
    request.headers_mut().remove(HEADER_ACTOR);
    if let Ok(value) = HeaderValue::from_str(&format!("{OIDC_ACTOR_PREFIX}{subject}@{issuer}")) {
        request.headers_mut().insert(HEADER_ACTOR, value);
        // The role layer keeps an `oidc:` actor only with this marker.
        request.extensions_mut().insert(crate::roles::OidcActor);
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
    // Host middleware can give roles, for example from an mTLS certificate.
    // A host grant wins over a live session, as in the role layer. It then
    // decides both the roles and the actor, so the host actor stays.
    if request
        .extensions()
        .get::<crate::roles::RoleGrant>()
        .is_some()
    {
        return next.run(request).await;
    }
    if let Some(session) = session
        && let Some(subject) = session_subject(&session, &login).await
    {
        set_actor(&mut request, &subject, login.issuer());
        // The login roles reach the role layer as a grant, never through the
        // shared session key.
        let roles = session
            .get(SESSION_OIDC_ROLES_KEY)
            .await
            .map(|v| crate::roles::parse_role_list(&v))
            .unwrap_or_default();
        request
            .extensions_mut()
            .insert(crate::roles::RoleGrant::new(roles));
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
/// live session principal gets `401`. A role deny writes one `authz.deny`
/// row.
#[cfg(feature = "mcp")]
pub(crate) async fn gate_mcp_tool(
    State((api_state, login)): State<(crate::api::HarvestApiState, OidcLogin)>,
    mut request: Request,
    next: Next,
) -> Response {
    strip_reserved_actor(&mut request);
    if *request.method() == Method::OPTIONS {
        return next.run(request).await;
    }
    let grant = request
        .extensions()
        .get::<crate::roles::RoleGrant>()
        .cloned();
    let session = request.extensions().get::<Session>().cloned();
    // A host `RoleGrant` wins, as in the role layer. Else the session decides.
    let (subject, roles) = if let Some(grant) = grant {
        (None, grant.roles().to_vec())
    } else {
        let Some(session) = session else {
            return error(StatusCode::UNAUTHORIZED, "authentication required");
        };
        let Some(subject) = session_subject(&session, &login).await else {
            return error(StatusCode::UNAUTHORIZED, "authentication required");
        };
        let roles = session
            .get(SESSION_OIDC_ROLES_KEY)
            .await
            .map(|v| crate::roles::parse_role_list(&v))
            .unwrap_or_default();
        (Some(subject), roles)
    };
    if let Some(subject) = &subject {
        set_actor(&mut request, subject, login.issuer());
    }
    if !login
        .roles()
        .allows_by_method(roles.iter().map(String::as_str), request.method())
    {
        if !roles.is_empty() {
            let method = request.method().clone();
            let path = request.uri().path().to_string();
            crate::roles::audit_role_deny(&api_state, request.headers(), &method, &path, &roles)
                .await;
        }
        return error(StatusCode::FORBIDDEN, crate::roles::ROLE_DENIED_ERROR);
    }
    crate::roles::run_admitted(login.roles(), roles, request, next).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_token::TokenScope;
    #[cfg(feature = "mcp")]
    use crate::roles::ROLE_OPERATOR;
    use crate::roles::ROLE_VIEWER;
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
    fn a_multi_tenant_microsoft_issuer_is_refused() {
        for alias in ["common", "organizations", "consumers"] {
            let mut p = provider();
            let issuer = format!("https://login.microsoftonline.com/{alias}/v2.0");
            p.issuer = Some(issuer.clone());
            assert_eq!(
                OidcLogin::new(p, HarvestRoles::builtin(), ClaimRoleMap::new()).err(),
                Some(OidcConfigError::MultiTenantIssuer(issuer))
            );
        }
        let mut one_tenant = provider();
        one_tenant.issuer = Some("https://login.microsoftonline.com/0f1e2d3c/v2.0".to_string());
        assert!(OidcLogin::new(one_tenant, HarvestRoles::builtin(), ClaimRoleMap::new()).is_ok());
    }

    #[test]
    fn debug_never_prints_the_client_secret() {
        let mut p = provider();
        p.client_secret = "very-secret-value".to_string();
        let login = OidcLogin::new(p, HarvestRoles::builtin(), ClaimRoleMap::new()).expect("ok");
        let text = format!("{login:?}");
        assert!(!text.contains("very-secret-value"), "{text}");
        assert!(text.contains("<redacted>"), "{text}");
        let auth = crate::api::StandaloneAdminAuth::new().with_oidc(login);
        assert!(!format!("{auth:?}").contains("very-secret-value"));
    }

    #[test]
    fn a_session_age_under_one_second_is_raised_to_one() {
        let l = login().with_max_session_age(Duration::from_millis(500));
        assert_eq!(l.max_session_age(), Duration::from_secs(1));
        assert_eq!(
            login()
                .with_max_session_age(Duration::ZERO)
                .max_session_age(),
            Duration::from_secs(1)
        );
    }

    #[tokio::test]
    async fn a_session_expires_after_its_max_age() {
        let l = login().with_max_session_age(Duration::from_secs(60));
        let fresh = session(Some("u"), ROLE_VIEWER, now_unix() - 30);
        assert_eq!(session_subject(&fresh, &l).await.as_deref(), Some("u"));
        let old = session(Some("u"), ROLE_VIEWER, now_unix() - 120);
        assert_eq!(session_subject(&old, &l).await, None);
        // A login time a little ahead is clock skew. Far ahead is refused.
        let skewed = session(Some("u"), ROLE_VIEWER, now_unix() + 30);
        assert_eq!(session_subject(&skewed, &l).await.as_deref(), Some("u"));
        let future = session(Some("u"), ROLE_VIEWER, now_unix() + 3600);
        assert_eq!(session_subject(&future, &l).await, None);
        // A missing or bad login time is never fresh.
        let mut data = HashMap::new();
        data.insert(SESSION_SUBJECT_KEY.to_string(), "u".to_string());
        data.insert(SESSION_AUTH_AT_KEY.to_string(), "soon".to_string());
        data.insert(SESSION_LOGIN_KEY.to_string(), l.fingerprint());
        let bad = Session::new_for_test("b".to_string(), data);
        assert_eq!(session_subject(&bad, &l).await, None);
    }

    /// The binding covers the mount and the policy, not only the client.
    #[test]
    fn the_fingerprint_covers_the_mount_and_the_policy() {
        let base = login().fingerprint();
        assert_eq!(login().fingerprint(), base, "stable for one config");

        let mut other_mount = provider();
        other_mount.redirect_uri = "https://harvest.example.com/b/auth/oidc/callback".to_string();
        let other_mount =
            OidcLogin::new(other_mount, HarvestRoles::builtin(), ClaimRoleMap::new()).expect("ok");
        assert_ne!(other_mount.fingerprint(), base);

        // The same names with other meanings.
        let read_only_admin = HarvestRoles::builder()
            .role(crate::roles::HarvestRole::new("harvest-admin").with_scope(TokenScope::Read))
            .build()
            .expect("roles");
        let other_roles =
            OidcLogin::new(provider(), read_only_admin, ClaimRoleMap::new()).expect("ok");
        assert_ne!(other_roles.fingerprint(), base);

        let other_claims = OidcLogin::new(
            provider(),
            HarvestRoles::builtin(),
            ClaimRoleMap::new().default_role(ROLE_VIEWER),
        )
        .expect("ok");
        assert_ne!(other_claims.fingerprint(), base);
    }

    /// Two mounts can share one session. A login on one is not a login on
    /// the other, and a visit to the other does not log the user out.
    #[tokio::test]
    async fn a_session_from_another_login_has_no_principal_here() {
        let mut other = provider();
        other.client_id = "another-client".to_string();
        let other =
            OidcLogin::new(other, HarvestRoles::builtin(), ClaimRoleMap::new()).expect("ok");
        assert_ne!(other.fingerprint(), login().fingerprint());
        assert_ne!(other.provider_name(), login().provider_name());
        let s = session(Some("u"), ROLE_VIEWER, now_unix());
        assert_eq!(session_subject(&s, &other).await, None);
        assert_eq!(session_subject(&s, &login()).await.as_deref(), Some("u"));
        // A session with no binding names no principal either.
        s.remove(SESSION_LOGIN_KEY).await;
        assert_eq!(session_subject(&s, &login()).await, None);
    }

    #[tokio::test]
    async fn logout_keeps_the_host_keys_of_the_session() {
        let mut data = HashMap::new();
        data.insert("user_id".to_string(), "host-user".to_string());
        data.insert(SESSION_SUBJECT_KEY.to_string(), "u".to_string());
        data.insert(SESSION_OIDC_ROLES_KEY.to_string(), ROLE_VIEWER.to_string());
        data.insert(SESSION_AUTH_AT_KEY.to_string(), now_unix().to_string());
        data.insert("auth_provider".to_string(), login().provider_name());
        data.insert(SESSION_LOGIN_KEY.to_string(), login().fingerprint());
        let session = Session::new_for_test("s".to_string(), data);
        let original: Uri = "/api/harvest/auth/oidc/logout".parse().expect("uri");
        let nested: Uri = "/auth/oidc/logout".parse().expect("uri");
        let response = logout_handler(
            Some(Extension(session.clone())),
            OriginalUri(original),
            nested,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(session.get("user_id").await.as_deref(), Some("host-user"));
        for key in [
            SESSION_SUBJECT_KEY,
            SESSION_OIDC_ROLES_KEY,
            SESSION_AUTH_AT_KEY,
            SESSION_LOGIN_KEY,
            "auth_provider",
        ] {
            assert!(session.get(key).await.is_none(), "{key}");
        }
    }

    #[tokio::test]
    async fn the_boundary_admits_a_host_role_grant() {
        let granted = echo_actor()
            .layer(axum::middleware::from_fn_with_state(
                (login(), false),
                require_oidc_session,
            ))
            .layer(axum::middleware::from_fn(
                |mut request: Request, next: Next| async move {
                    request
                        .extensions_mut()
                        .insert(crate::roles::RoleGrant::new([ROLE_VIEWER]));
                    next.run(request).await
                },
            ));
        let out = call(granted, Method::GET, None, "oidc:admin").await;
        // The grant passes. The forged `oidc:` actor does not.
        assert_eq!(out, (StatusCode::OK, "-".to_string()));
    }

    /// A host grant wins over a live OIDC session. Its host actor stays.
    #[tokio::test]
    async fn a_host_grant_keeps_its_host_actor_over_a_live_session() {
        let granted = echo_actor()
            .layer(axum::middleware::from_fn_with_state(
                (login(), false),
                require_oidc_session,
            ))
            .layer(axum::middleware::from_fn(
                |mut request: Request, next: Next| async move {
                    request
                        .extensions_mut()
                        .insert(crate::roles::RoleGrant::new([ROLE_VIEWER]));
                    next.run(request).await
                },
            ));
        let live = session(Some("user-42"), ROLE_VIEWER, now_unix());
        let out = call(granted, Method::GET, Some(live), "cert:svc-orders").await;
        assert_eq!(out, (StatusCode::OK, "cert:svc-orders".to_string()));
    }

    #[tokio::test]
    async fn the_boundary_passes_a_bearer_only_with_tokens_on() {
        let app = |tokens: bool| {
            echo_actor().layer(axum::middleware::from_fn_with_state(
                (login(), tokens),
                require_oidc_session,
            ))
        };
        let send = |tokens: bool| {
            let app = app(tokens);
            async move {
                let request = axum::http::Request::builder()
                    .uri("/workflows")
                    .header("authorization", "Bearer hvst_x")
                    .body(Body::empty())
                    .expect("request");
                app.oneshot(request).await.expect("served").status()
            }
        };
        assert_eq!(send(true).await, StatusCode::OK);
        assert_eq!(send(false).await, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn a_userinfo_url_is_refused() {
        let mut p = provider();
        p.userinfo_url = Some("https://idp.example.com/userinfo".to_string());
        assert_eq!(
            OidcLogin::new(p, HarvestRoles::builtin(), ClaimRoleMap::new()).err(),
            Some(OidcConfigError::UserinfoNotSupported)
        );
    }

    #[test]
    fn the_redirect_uri_must_be_secure_too() {
        let mut p = provider();
        p.redirect_uri = "http://harvest.example.com/cb".to_string();
        assert_eq!(
            OidcLogin::new(p, HarvestRoles::builtin(), ClaimRoleMap::new()).err(),
            Some(OidcConfigError::InsecureUrl {
                field: "redirect_uri",
                url: "http://harvest.example.com/cb".to_string(),
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
        let echo = |headers: axum::http::HeaderMap| async move {
            headers
                .get(HEADER_ACTOR)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string()
        };
        Router::new().route("/workflows", axum::routing::get(echo).options(echo))
    }

    fn session(subject: Option<&str>, roles: &str, auth_at: u64) -> Session {
        let mut data = HashMap::new();
        if let Some(subject) = subject {
            data.insert(SESSION_SUBJECT_KEY.to_string(), subject.to_string());
        }
        data.insert(SESSION_OIDC_ROLES_KEY.to_string(), roles.to_string());
        data.insert(SESSION_AUTH_AT_KEY.to_string(), auth_at.to_string());
        data.insert(SESSION_LOGIN_KEY.to_string(), login().fingerprint());
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
        // `sub` is unique only within an issuer, so the actor names both.
        assert_eq!(
            out,
            (
                StatusCode::OK,
                "oidc:user-42@https://idp.example.com".to_string()
            )
        );
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
        assert!(stale.get(SESSION_OIDC_ROLES_KEY).await.is_none());
    }

    #[tokio::test]
    async fn options_passes_but_loses_a_reserved_actor() {
        let out = call(bounded(), Method::OPTIONS, None, "oidc:admin").await;
        assert_eq!(out, (StatusCode::OK, "-".to_string()));
    }

    #[cfg(feature = "mcp")]
    fn mcp_gated() -> Router<()> {
        Router::new()
            .route(
                "/workflows",
                axum::routing::get(|| async { "read" }).post(|| async { "write" }),
            )
            .layer(axum::middleware::from_fn_with_state(
                (crate::api::HarvestApiState::new(), login()),
                gate_mcp_tool,
            ))
    }

    #[cfg(feature = "mcp")]
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
