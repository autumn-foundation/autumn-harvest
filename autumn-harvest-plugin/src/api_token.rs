//! Scoped API tokens + rotation for the Harvest management API (issue #942).
//!
//! A small, Postgres-only bearer-credential layer. It does **not** build an
//! identity provider: it mints an opaque high-entropy secret, stores only its
//! SHA-256 hash, and derives verb-level scopes from the **already-test-enforced**
//! [`autumn_harvest::audit::CLASSIFIED_ROUTES`] classification — the exact
//! promotion-from-audit-to-authz mechanism the read-only operator role (#776)
//! introduced. It reuses that route-class enforcement rather than forking a
//! second route taxonomy.
//!
//! # Wire format
//!
//! A token secret is `hvst_<base64url(32 random bytes)>`. The `hvst_` prefix is
//! the credential-namespace discriminator: a bearer that does not start with it
//! is ignored entirely (it is the embedder's own credential), so token auth
//! **composes with** — never replaces — an `api_with_auth` middleware (AC7).
//!
//! # Secret hygiene (AC2)
//!
//! The plaintext secret is returned **exactly once** at creation ([`MintResult`])
//! and never persists at rest — only `token_hash = hex(SHA256(secret))` is
//! stored, UNIQUE-indexed. Verification hashes the presented secret and does one
//! indexed `SELECT`; there is **no in-Rust hash comparison** (a 256-bit random
//! hash is not an iterable timing oracle), so no constant-time-compare crate is
//! needed. [`TokenView`] is a metadata-only DTO that structurally cannot hold
//! the hash or secret (the #695 redaction-view pattern), and [`MintResult`] and
//! [`autumn_harvest::models::NewApiToken`] redact the secret/hash in `Debug`.
//!
//! # Scope enforcement (AC3/AC4)
//!
//! A `read` token reaches every [`RouteClass::ReadOnly`]/`PublicSafe` route and
//! is denied **403** on every mutating route; a `mutate` token reaches
//! everything. The decision is [`deny_readonly_mutation`] over
//! [`crate::api::classify_route`] — the single source of truth — which **fails
//! closed**: an unclassified path resolves to `Mutating` and is denied to a
//! `read` token.
//!
//! # Rotation (AC5)
//!
//! There is **no grant cache**: every request re-queries `harvest_api_tokens`,
//! so revocation and expiry take effect on the very next request (bounded TTL =
//! 0). Zero-downtime rotation is create-a-replacement → cut callers over →
//! revoke the old, over the existing create/revoke routes (the CLI's
//! `harvest token rotate` is a client-side convenience over the create route).

use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use base64::Engine as _;
use chrono::{DateTime, Utc};
use rand::RngCore as _;

use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use autumn_harvest::HarvestResult;
use autumn_harvest::audit::{HEADER_ACTOR, RouteClass, deny_readonly_mutation};
use autumn_harvest::models::{ApiToken, NewApiToken};
use autumn_harvest::schema::harvest_api_tokens;

use crate::api::{HarvestApiState, acquire_conn, classify_route, read_only_forbidden_response};

/// The credential-namespace discriminator for a harvest-native token.
pub(crate) const TOKEN_PREFIX: &str = "hvst_";

// ── Types ─────────────────────────────────────────────────────────────────────

/// Verb-level scope drawn from the route classification (issue #942, AC3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenScope {
    /// Reaches every `ReadOnly`/`PublicSafe` route; denied 403 on mutations.
    Read,
    /// Reaches every route.
    Mutate,
}

impl TokenScope {
    /// Parse the persisted `scope` column value.
    #[must_use]
    pub(crate) fn from_db(s: &str) -> Option<Self> {
        match s {
            "read" => Some(Self::Read),
            "mutate" => Some(Self::Mutate),
            _ => None,
        }
    }

    /// The persisted `scope` column value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Mutate => "mutate",
        }
    }
}

/// A verified token principal, set as a request extension by
/// [`enforce_token_scope`]. Its presence is what lets [`crate::api::require_harvest_admin`]
/// admit a token-authenticated request without a session/embedder boundary
/// (issue #942, AC6/AC7).
#[derive(Clone, Copy, Debug)]
pub(crate) struct TokenPrincipal {
    pub id: Uuid,
    #[allow(dead_code)] // consulted by tests + future MCP gating; not read on the admin path
    pub scope: TokenScope,
}

/// Metadata-only response DTO for `GET /admin/tokens` (issue #942, AC2).
///
/// **Structurally cannot hold the hash or secret** — [`from_row`](Self::from_row)
/// destructures [`ApiToken`] exhaustively (no `..`), the #695 redaction guard, so
/// adding a secret-bearing column to `ApiToken` is a compile error here until it
/// is deliberately omitted.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TokenView {
    pub id: Uuid,
    pub name: String,
    pub scope: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

impl TokenView {
    fn from_row(t: &ApiToken) -> Self {
        // Exhaustive destructure is the #695 redaction guard — do NOT add `..`.
        // `token_hash` and `created_by` are deliberately dropped: the hash must
        // never leave the process, and the response shape is metadata-only.
        let ApiToken {
            id,
            name,
            token_hash: _,
            scope,
            created_at,
            expires_at,
            last_used_at,
            revoked_at,
            created_by: _,
        } = t;
        Self {
            id: *id,
            name: name.clone(),
            scope: scope.clone(),
            created_at: *created_at,
            expires_at: *expires_at,
            last_used_at: *last_used_at,
            revoked_at: *revoked_at,
        }
    }
}

/// The one-time result of minting a token — carries the plaintext secret
/// **exactly once** (issue #942, AC2). `Debug` redacts the secret.
pub struct MintResult {
    pub view: TokenView,
    secret: String,
}

impl MintResult {
    /// The plaintext secret, surfaced exactly once in the create response.
    #[must_use]
    pub(crate) fn secret(&self) -> &str {
        &self.secret
    }
}

impl std::fmt::Debug for MintResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MintResult")
            .field("view", &self.view)
            .field("secret", &"<redacted>")
            .finish()
    }
}

// ── Crypto / discrimination helpers (pure) ────────────────────────────────────

/// Mint a fresh opaque secret `hvst_<base64url(32 random bytes)>`.
///
/// Uses the OS CSPRNG for 256 bits of entropy — this is a bearer credential,
/// so the randomness source must be cryptographically secure.
#[must_use]
pub(crate) fn mint_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let body = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    format!("{TOKEN_PREFIX}{body}")
}

/// Hex-encoded SHA-256 of the full presented secret — the indexed lookup key.
///
/// A plain SHA-256 of a 256-bit random secret is standard for API keys (the
/// GitHub-PAT / Hatchet model): no salt/argon needed because the secret is not
/// a low-entropy human password.
#[must_use]
pub(crate) fn hash_secret(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    // Lowercase hex, 64 chars.
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Whether a bearer credential claims to be a harvest-native token (AC7).
#[must_use]
pub(crate) fn looks_like_harvest_token(bearer: &str) -> bool {
    bearer.starts_with(TOKEN_PREFIX)
}

/// Whether a token with the given (optional) expiry is expired at `now` (AC5).
///
/// The boundary is inclusive: a token whose `expires_at == now` is expired.
#[must_use]
pub(crate) fn is_expired(expires_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    match expires_at {
        Some(at) => at <= now,
        None => false,
    }
}

/// The scope-gate decision (issue #942, AC3/AC4): reuses the #776
/// route-classification single source of truth, so it fails closed for an
/// unclassified route.
///
/// A `Mutate` token is never denied. A `Read` token is denied exactly when the
/// route resolves to a mutating class — and [`classify_route`] fails closed by
/// resolving an unmatched path to [`RouteClass::Mutating`], so an unclassified
/// route is denied to a `read` token.
#[must_use]
pub(crate) fn token_scope_denies(scope: TokenScope, method: &Method, path: &str) -> bool {
    let class = classify_route(method, path);
    deny_readonly_mutation(matches!(scope, TokenScope::Read), class)
}

// ── DB CRUD (default/control shard) ───────────────────────────────────────────

/// Mint and persist a new token; returns the one-time [`MintResult`].
pub(crate) async fn create_token(
    conn: &mut AsyncPgConnection,
    name: &str,
    scope: TokenScope,
    expires_at: Option<DateTime<Utc>>,
    created_by: &str,
) -> HarvestResult<MintResult> {
    let secret = mint_secret();
    let hash = hash_secret(&secret);
    let new = NewApiToken {
        name,
        token_hash: &hash,
        scope: scope.as_str(),
        expires_at,
        created_by,
    };
    let row: ApiToken = diesel::insert_into(harvest_api_tokens::table)
        .values(&new)
        .returning(ApiToken::as_returning())
        .get_result(conn)
        .await?;
    Ok(MintResult {
        view: TokenView::from_row(&row),
        secret,
    })
}

/// List all tokens, newest first, as metadata-only [`TokenView`]s.
pub(crate) async fn list_tokens(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<TokenView>> {
    use harvest_api_tokens::dsl;
    let rows: Vec<ApiToken> = dsl::harvest_api_tokens
        .select(ApiToken::as_select())
        .order(dsl::created_at.desc())
        .load(conn)
        .await?;
    Ok(rows.iter().map(TokenView::from_row).collect())
}

/// Revoke a token by id. Idempotent: returns `false` if it was unknown or
/// already revoked (no grant cache — revocation is effective next request).
pub(crate) async fn revoke_token(conn: &mut AsyncPgConnection, id: Uuid) -> HarvestResult<bool> {
    use harvest_api_tokens::dsl;
    let n = diesel::update(
        dsl::harvest_api_tokens
            .filter(dsl::id.eq(id))
            .filter(dsl::revoked_at.is_null()),
    )
    .set(dsl::revoked_at.eq(Utc::now()))
    .execute(conn)
    .await?;
    Ok(n > 0)
}

/// Look up the token row whose stored hash matches the presented secret.
///
/// `Ok(None)` = no such token (unknown/invalid); `Err` = DB failure (the caller
/// returns 503 rather than passing a claimed token through unverified).
pub(crate) async fn lookup_by_secret(
    conn: &mut AsyncPgConnection,
    secret: &str,
) -> HarvestResult<Option<ApiToken>> {
    let hash = hash_secret(secret);
    use harvest_api_tokens::dsl;
    match dsl::harvest_api_tokens
        .filter(dsl::token_hash.eq(hash))
        .select(ApiToken::as_select())
        .first(conn)
        .await
    {
        Ok(t) => Ok(Some(t)),
        Err(diesel::result::Error::NotFound) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Best-effort `last_used_at` bump. Called off the request critical path.
pub(crate) async fn touch_last_used(conn: &mut AsyncPgConnection, id: Uuid) -> HarvestResult<()> {
    use harvest_api_tokens::dsl;
    diesel::update(dsl::harvest_api_tokens.filter(dsl::id.eq(id)))
        .set(dsl::last_used_at.eq(Utc::now()))
        .execute(conn)
        .await?;
    Ok(())
}

// ── Verification middleware ───────────────────────────────────────────────────

fn unauthorized(msg: &'static str) -> Response {
    AutumnError::unauthorized_msg(msg).into_response()
}

fn service_unavailable(msg: &'static str) -> Response {
    AutumnError::service_unavailable_msg(msg).into_response()
}

/// Extract a harvest-prefixed bearer secret, or `None` to pass through (AC7).
fn harvest_bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?
        .trim();
    if looks_like_harvest_token(token) {
        Some(token.to_string())
    } else {
        None
    }
}

/// The token-verification + scope-gate layer (issue #942).
///
/// Installed on `harvest_api_router` only under
/// [`crate::HarvestPlugin::enable_api_tokens`]; the default path never sees it,
/// so behavior is byte-for-byte unchanged (AC7). Request order (outermost →
/// innermost): embedder auth mw → **this layer** → per-route `require_admin` →
/// handler.
///
/// On a valid token it (1) sets a [`TokenPrincipal`] extension so `require_admin`
/// admits the request, and (2) strips any inbound `x-harvest-actor` and injects
/// `token:{id}` so every audited mutation is attributed to the token, and no
/// caller can spoof a different actor (AC6). A `read` token attempting a
/// mutating route is denied 403 here, before any handler runs.
pub async fn enforce_token_scope(
    State(api_state): State<HarvestApiState>,
    mut request: Request,
    next: Next,
) -> Response {
    // OPTIONS is a preflight/metadata verb, never a mutation — pass through.
    if *request.method() == Method::OPTIONS {
        return next.run(request).await;
    }

    // AC7: absent or non-`hvst_` bearer → pass through untouched (it is the
    // embedder's credential, or an unauthenticated read).
    let Some(secret) = harvest_bearer(request.headers()) else {
        return next.run(request).await;
    };

    // A claimed `hvst_` token that we cannot verify (store unavailable) must
    // NOT pass through: under the admin boundary that would be an auth bypass.
    let Ok(pool) = api_state.storage_pool() else {
        return service_unavailable("harvest token store unavailable");
    };
    let mut conn = match acquire_conn(pool.default_pool()).await {
        Ok(c) => c,
        Err(_) => return service_unavailable("harvest token store unavailable"),
    };

    let token = match lookup_by_secret(&mut conn, &secret).await {
        Ok(Some(t)) => t,
        Ok(None) => return unauthorized("invalid api token"),
        Err(_) => return service_unavailable("harvest token store unavailable"),
    };

    // AC5: revocation + expiry are re-checked on every request (no grant cache).
    if token.revoked_at.is_some() {
        return unauthorized("api token revoked");
    }
    if is_expired(token.expires_at, Utc::now()) {
        return unauthorized("api token expired");
    }

    let scope = TokenScope::from_db(&token.scope).unwrap_or(TokenScope::Read);

    // AC3/AC4: deny a read token every mutating route, before `require_admin`.
    if token_scope_denies(scope, request.method(), request.uri().path()) {
        tracing::warn!(
            method = %request.method(),
            path = %request.uri().path(),
            "harvest: read-scoped api token denied mutation (403)"
        );
        return read_only_forbidden_response();
    }

    // AC6/D3: mark the verified principal so `require_admin` admits it.
    request
        .extensions_mut()
        .insert(TokenPrincipal { id: token.id, scope });

    // AC6: authoritative actor. Strip any spoofed inbound value, set token:{id}.
    if let Ok(v) = axum::http::HeaderValue::from_str(&format!("token:{}", token.id)) {
        request.headers_mut().remove(HEADER_ACTOR);
        request.headers_mut().insert(HEADER_ACTOR, v);
    }

    // Best-effort last-used update, OFF the request critical path (no
    // write-amplification DoS; mirrors #776's no-hot-write policy).
    let touch_pool = pool.default_pool().clone();
    let id = token.id;
    tokio::spawn(async move {
        if let Ok(mut c) = touch_pool.get().await {
            let _ = touch_last_used(&mut c, id).await;
        }
    });

    next.run(request).await
}

/// The class gate for a **known-mutating** generated MCP tool route under token
/// auth (issue #942, mirrors [`crate::api::enforce_read_only_mcp_mutation`]).
///
/// The generated MCP tool routes are registered app-level (outside the nested
/// management router that [`enforce_token_scope`] wraps), so a `read`-scoped
/// token would otherwise reach a mutating tool. This layer verifies the bearer
/// and hard-denies a valid `read` token `403`. A non-`hvst_` bearer / no bearer
/// passes through (the embedder or `require_admin` governs). Installed only on
/// mutating tools and only when `enable_api_tokens()` is set.
pub async fn enforce_token_scope_mcp_mutation(
    State(api_state): State<HarvestApiState>,
    request: Request,
    next: Next,
) -> Response {
    if *request.method() == Method::OPTIONS {
        return next.run(request).await;
    }
    let Some(secret) = harvest_bearer(request.headers()) else {
        return next.run(request).await;
    };
    let Ok(pool) = api_state.storage_pool() else {
        return service_unavailable("harvest token store unavailable");
    };
    let mut conn = match acquire_conn(pool.default_pool()).await {
        Ok(c) => c,
        Err(_) => return service_unavailable("harvest token store unavailable"),
    };
    let token = match lookup_by_secret(&mut conn, &secret).await {
        Ok(Some(t)) => t,
        Ok(None) => return unauthorized("invalid api token"),
        Err(_) => return service_unavailable("harvest token store unavailable"),
    };
    if token.revoked_at.is_some() {
        return unauthorized("api token revoked");
    }
    if is_expired(token.expires_at, Utc::now()) {
        return unauthorized("api token expired");
    }
    // Every generated route carrying this layer is a mutation, so a read token
    // is always denied here.
    if TokenScope::from_db(&token.scope) == Some(TokenScope::Read) {
        tracing::warn!(
            method = %request.method(),
            path = %request.uri().path(),
            "harvest: read-scoped api token denied MCP mutation tool (403)"
        );
        return read_only_forbidden_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic_and_hex() {
        let a = hash_secret("hvst_abc");
        let b = hash_secret("hvst_abc");
        assert_eq!(a, b, "hash must be deterministic");
        assert_eq!(a.len(), 64, "SHA-256 hex is 64 chars");
        assert!(
            a.chars().all(|c| c.is_ascii_hexdigit()),
            "hash must be lowercase hex"
        );
        assert_ne!(hash_secret("hvst_abc"), hash_secret("hvst_abd"));
    }

    #[test]
    fn mint_secret_is_prefixed_and_high_entropy() {
        let s = mint_secret();
        assert!(s.starts_with(TOKEN_PREFIX), "secret must carry the hvst_ prefix");
        let body = &s[TOKEN_PREFIX.len()..];
        // 32 bytes base64url-no-pad = 43 chars.
        assert!(body.len() >= 43, "secret body too short: {}", body.len());
        assert_ne!(mint_secret(), mint_secret(), "two mints must differ");
    }

    #[test]
    fn mint_result_debug_is_redacted() {
        let view = TokenView {
            id: Uuid::nil(),
            name: "ci".to_string(),
            scope: "read".to_string(),
            created_at: Utc::now(),
            expires_at: None,
            last_used_at: None,
            revoked_at: None,
        };
        let secret = "hvst_super_secret_value".to_string();
        let mr = MintResult {
            view,
            secret: secret.clone(),
        };
        let dbg = format!("{mr:?}");
        assert!(dbg.contains("<redacted>"), "Debug must redact: {dbg}");
        assert!(
            !dbg.contains(&secret),
            "Debug must not contain the plaintext secret: {dbg}"
        );
        // The accessor still returns the real secret exactly once.
        assert_eq!(mr.secret(), secret);
    }

    #[test]
    fn token_view_never_serializes_hash_or_secret() {
        let row = ApiToken {
            id: Uuid::nil(),
            name: "ci".to_string(),
            token_hash: "DEADBEEFHASHVALUE".to_string(),
            scope: "read".to_string(),
            created_at: Utc::now(),
            expires_at: None,
            last_used_at: None,
            revoked_at: None,
            created_by: "op".to_string(),
        };
        let view = TokenView::from_row(&row);
        let body = serde_json::to_string(&view).unwrap();
        assert!(!body.contains("token_hash"), "view leaked token_hash key");
        assert!(!body.contains("secret"), "view leaked secret key");
        assert!(
            !body.contains("DEADBEEFHASHVALUE"),
            "view leaked the hash value: {body}"
        );
        assert!(body.contains("\"scope\":\"read\""));
    }

    #[test]
    fn scope_from_route_class_read_denies_mutating() {
        // AC3: a read token is denied a mutating route.
        assert!(token_scope_denies(
            TokenScope::Read,
            &Method::POST,
            "/workflows/abc/cancel"
        ));
        // AC3: a mutate token reaches the same route.
        assert!(!token_scope_denies(
            TokenScope::Mutate,
            &Method::POST,
            "/workflows/abc/cancel"
        ));
    }

    #[test]
    fn scope_read_allows_readonly() {
        // AC3: a read token reaches read-only routes.
        assert!(!token_scope_denies(TokenScope::Read, &Method::GET, "/workflows"));
        assert!(!token_scope_denies(
            TokenScope::Read,
            &Method::GET,
            "/admin/tokens"
        ));
    }

    #[test]
    fn unclassified_route_fails_closed_for_read() {
        // AC4: an unclassified path resolves to Mutating → denied to a read token.
        assert!(token_scope_denies(
            TokenScope::Read,
            &Method::POST,
            "/nope/not-a-real-route"
        ));
    }

    #[test]
    fn looks_like_harvest_token_discriminates() {
        assert!(looks_like_harvest_token("hvst_anything"));
        assert!(!looks_like_harvest_token(
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ));
        assert!(!looks_like_harvest_token("abc123"));
        assert!(!looks_like_harvest_token(""));
    }

    #[test]
    fn expiry_predicate() {
        let now = Utc::now();
        assert!(is_expired(Some(now - chrono::Duration::hours(1)), now));
        assert!(!is_expired(Some(now + chrono::Duration::hours(1)), now));
        assert!(!is_expired(None, now));
        // Exactly-now counts as expired (boundary is inclusive).
        assert!(is_expired(Some(now), now));
    }

    #[test]
    fn scope_round_trips_db_string() {
        assert_eq!(TokenScope::from_db("read"), Some(TokenScope::Read));
        assert_eq!(TokenScope::from_db("mutate"), Some(TokenScope::Mutate));
        assert_eq!(TokenScope::from_db("admin"), None);
        assert_eq!(TokenScope::Read.as_str(), "read");
        assert_eq!(TokenScope::Mutate.as_str(), "mutate");
    }
}
