//! Pluggable authorizer hook for the management API (issue #1803).
//!
//! The built-in gates decide by verb: a token scope, or the read-only role.
//! They know nothing of tenants or shards. This hook lets an embedder add that
//! policy without forking routes.
//!
//! # Contract
//!
//! - Harvest calls [`HarvestAuthorizer::authorize`] once per request, per
//!   distinct shard the request names. With no shard, it calls it once with
//!   `shard: None`.
//! - The hook runs after the token layer and the read-only layer. It can only
//!   deny. It cannot grant what a token scope withholds.
//! - A deny returns `403` with a generic body. The deny reason goes only to the
//!   audit row ([`autumn_harvest::audit::OP_AUTHZ_DENY`]).
//! - The default is no hook. The router is then byte-for-byte unchanged.
//!
//! # Inputs
//!
//! - `principal`: the verified token, or [`AuthzPrincipal::Embedder`] for every
//!   other caller. Read the embedder's own claims from `extensions`.
//! - `route_class`: from [`autumn_harvest::audit::CLASSIFIED_ROUTES`].
//!   An unclassified path is `Mutating`.
//! - `tenant_key`: the [`autumn_harvest::audit::HEADER_TENANT`] header.
//!   The caller declares it. Harvest does not bind it to stored executions.
//! - `shard`: from an execution id in the path, a `shard_id` query parameter,
//!   or the `shard_id` / `residency_key` of a start body. `None` means the
//!   request names no single shard. A list route then reads every shard.
//!
//! # Audit volume
//!
//! Each deny writes one audit row. Return [`AuthzDecision::Allow`] for a
//! request with no principal you can name. `require_admin` then answers `401`
//! with no audit write.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use autumn_web::reexports::axum;
use axum::extract::{Request, State};
use axum::http::{Extensions, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use diesel_async::AsyncPgConnection;
use uuid::Uuid;

use autumn_harvest::audit::{
    self, HEADER_TENANT, OP_AUTHZ_DENY, RouteClass, STATUS_FAILED, TARGET_ROUTE,
};
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest::shard::ShardPlacement;
use autumn_harvest::types::ShardId;

use crate::api::{
    HarvestApiState, acquire_conn, audit_context, classify_route, execution_id_in_path,
};
use crate::api_token::{TokenPrincipal, TokenScope};

/// Longest accepted [`HEADER_TENANT`] value, in bytes.
pub const MAX_TENANT_KEY_LEN: usize = 128;

/// Largest start body the hook buffers to read its placement.
///
/// This is axum's default body limit, which the start route also applies.
const MAX_START_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Longest request path an audit row records.
const MAX_AUDITED_PATH_LEN: usize = 256;

/// Longest `error_summary` an audit row records.
const MAX_AUDITED_SUMMARY_LEN: usize = 512;

/// Who made the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthzPrincipal {
    /// A verified Harvest API token.
    Token {
        /// The token row id. Audit rows name it as `token:{id}`.
        id: Uuid,
        /// The token scope. The built-in gate has already applied it.
        scope: TokenScope,
    },
    /// Any caller without a verified Harvest token.
    ///
    /// The embedder's auth decides who this is, if anyone. Its claims are in
    /// [`AuthzRequest::extensions`].
    Embedder,
}

/// The authorizer's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthzDecision {
    /// Let the request continue to the next gate.
    Allow,
    /// Refuse with `403`. The reason goes to the audit row, not the caller.
    Deny(String),
}

impl AuthzDecision {
    /// A deny with `reason` for the audit row.
    #[must_use]
    pub fn deny(reason: impl Into<String>) -> Self {
        Self::Deny(reason.into())
    }
}

/// What the authorizer sees for one request.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct AuthzRequest<'a> {
    /// The caller.
    pub principal: AuthzPrincipal,
    /// The route class of `method` and `path`.
    pub route_class: RouteClass,
    /// The caller-declared tenant key, trimmed.
    pub tenant_key: Option<&'a str>,
    /// The shard this call is about, if the request names one.
    pub shard: Option<ShardId>,
    /// The HTTP method.
    pub method: &'a Method,
    /// The path, relative to the management API mount.
    pub path: &'a str,
    /// The request extensions, including the embedder's own auth claims.
    pub extensions: &'a Extensions,
}

impl<'a> AuthzRequest<'a> {
    /// A request with no tenant key and no shard. Use this to unit-test an
    /// authorizer.
    #[must_use]
    pub const fn new(
        principal: AuthzPrincipal,
        route_class: RouteClass,
        method: &'a Method,
        path: &'a str,
        extensions: &'a Extensions,
    ) -> Self {
        Self {
            principal,
            route_class,
            tenant_key: None,
            shard: None,
            method,
            path,
            extensions,
        }
    }

    /// Set the tenant key.
    #[must_use]
    pub const fn with_tenant_key(mut self, tenant_key: Option<&'a str>) -> Self {
        self.tenant_key = tenant_key;
        self
    }

    /// Set the shard.
    #[must_use]
    pub const fn with_shard(mut self, shard: Option<ShardId>) -> Self {
        self.shard = shard;
        self
    }
}

/// The future an authorizer returns.
pub type AuthzFuture<'a> = Pin<Box<dyn Future<Output = AuthzDecision> + Send + 'a>>;

/// A per-request authorization policy (issue #1803).
///
/// A plain `Fn(&AuthzRequest) -> AuthzDecision` closure implements this trait.
/// Implement it directly when the policy needs I/O.
///
/// A panic in `authorize` aborts the request. It never lets it through.
pub trait HarvestAuthorizer: Send + Sync + 'static {
    /// Decide one request.
    fn authorize<'a>(&'a self, request: &'a AuthzRequest<'a>) -> AuthzFuture<'a>;
}

impl<F> HarvestAuthorizer for F
where
    F: Fn(&AuthzRequest<'_>) -> AuthzDecision + Send + Sync + 'static,
{
    fn authorize<'a>(&'a self, request: &'a AuthzRequest<'a>) -> AuthzFuture<'a> {
        Box::pin(std::future::ready(self(request)))
    }
}

/// An authorizer that allows every request: today's behaviour.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAll;

impl HarvestAuthorizer for AllowAll {
    fn authorize<'a>(&'a self, _request: &'a AuthzRequest<'a>) -> AuthzFuture<'a> {
        Box::pin(std::future::ready(AuthzDecision::Allow))
    }
}

/// A cloneable handle to an installed authorizer.
#[derive(Clone)]
pub struct SharedAuthorizer(Arc<dyn HarvestAuthorizer>);

impl SharedAuthorizer {
    /// Wrap `authorizer`.
    #[must_use]
    pub fn new(authorizer: impl HarvestAuthorizer) -> Self {
        Self(Arc::new(authorizer))
    }
}

impl std::fmt::Debug for SharedAuthorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedAuthorizer(..)")
    }
}

// ── Deny audit ────────────────────────────────────────────────────────────────

/// The fields of one `authz.deny` audit row.
pub(crate) struct DenyAudit<'a> {
    pub actor: &'a str,
    pub method: &'a Method,
    pub path: &'a str,
    pub request_id: Option<&'a str>,
    pub source: &'a str,
    pub shard: Option<ShardId>,
    pub summary: &'a str,
}

/// Cut `s` to at most `max` bytes on a char boundary.
fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Write one `authz.deny` row to the control shard.
///
/// Best effort: a failed write is logged. The caller returns the deny anyway.
pub(crate) async fn audit_deny(conn: &mut AsyncPgConnection, deny: &DenyAudit<'_>) {
    let route = format!(
        "{} {}",
        deny.method,
        truncate(deny.path, MAX_AUDITED_PATH_LEN)
    );
    let record = NewAuditRecord {
        actor: deny.actor,
        operation: OP_AUTHZ_DENY,
        target_type: TARGET_ROUTE,
        target_id: None,
        route_or_command: &route,
        request_id: deny.request_id,
        idempotency_key: None,
        status: STATUS_FAILED,
        error_summary: Some(truncate(deny.summary, MAX_AUDITED_SUMMARY_LEN)),
        shard_id: deny.shard.map(ShardId::as_i32),
        source: deny.source,
    };
    if let Err(e) = audit::insert_audit(conn, &record).await {
        tracing::error!(error = %e, route = %route, "harvest: failed to audit authz deny");
    }
}

// ── Request inputs ────────────────────────────────────────────────────────────

/// Read the tenant header. `Err` means the header is present but unusable.
fn tenant_key(request: &Request) -> Result<Option<String>, ()> {
    let Some(raw) = request.headers().get(HEADER_TENANT) else {
        return Ok(None);
    };
    let value = raw.to_str().map_err(|_| ())?.trim();
    if value.is_empty() || value.len() > MAX_TENANT_KEY_LEN {
        return Err(());
    }
    Ok(Some(value.to_string()))
}

/// The shard an execution id in the path routes to.
///
/// A retired shard resolves to its successor. An id with no encoded shard
/// resolves to the default shard.
fn path_shard(api_state: &HarvestApiState, method: &Method, path: &str) -> Option<ShardId> {
    let exec_id = execution_id_in_path(method, path)?;
    api_state.storage_pool().map_or_else(
        |_| Some(exec_id.shard()).filter(|s| !s.is_unencoded()),
        |pool| Some(pool.sharded_pool().routed_shard_for_execution(exec_id)),
    )
}

/// The shards named by a `shard_id`, `shard-id` or `shard` query parameter.
///
/// Decoded as the handlers decode it. A value that does not parse is skipped,
/// because the handler rejects it.
fn query_shards(uri: &axum::http::Uri) -> Vec<ShardId> {
    let Ok(axum::extract::Query(pairs)) =
        axum::extract::Query::<Vec<(String, String)>>::try_from_uri(uri)
    else {
        return Vec::new();
    };
    pairs
        .into_iter()
        .filter(|(k, _)| matches!(k.as_str(), "shard_id" | "shard-id" | "shard"))
        .filter_map(|(_, v)| v.trim().parse::<i32>().ok())
        .filter(|n| *n >= 0)
        .map(ShardId::new)
        .collect()
}

/// The workflow name of a `POST /workflows/{workflow_name}/start` path.
fn start_route_workflow<'p>(method: &Method, path: &'p str) -> Option<&'p str> {
    if *method != Method::POST {
        return None;
    }
    let rest = path.strip_prefix("/workflows/")?;
    let name = rest.strip_suffix("/start")?;
    (!name.is_empty() && !name.contains('/')).then_some(name)
}

/// The shard a start body pins, if it pins one.
///
/// A `residency_key` resolves through the shard router. A body that does not
/// parse, or names no placement, pins nothing. The handler validates it.
fn start_body_shard(api_state: &HarvestApiState, workflow: &str, body: &[u8]) -> Option<ShardId> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    if let Some(raw) = value.get("shard_id").and_then(serde_json::Value::as_i64) {
        return i32::try_from(raw)
            .ok()
            .filter(|n| *n >= 0)
            .map(ShardId::new);
    }
    let key = value.get("residency_key")?.as_str()?.trim();
    if key.is_empty() {
        return None;
    }
    let runtime = api_state.runtime().ok()?;
    let placement = ShardPlacement::residency_key(key);
    let workflow_id = value
        .get("workflow_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    runtime
        .router()
        .resolve_placement_for_lookup(&placement, workflow, workflow_id)
        .ok()
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({ "error": "forbidden by authorization policy" })),
    )
        .into_response()
}

fn bad_tenant() -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(serde_json::json!({
            "error": format!(
                "invalid {HEADER_TENANT} header: expected 1 to {MAX_TENANT_KEY_LEN} visible characters"
            )
        })),
    )
        .into_response()
}

// ── Middleware ────────────────────────────────────────────────────────────────

/// The authorizer layer (issue #1803).
///
/// Installed only by `with_authorizer`. It runs after the token layer, so it
/// sees a verified [`TokenPrincipal`]. It runs before `require_admin`.
pub async fn enforce_authorizer(
    State((api_state, authorizer)): State<(HarvestApiState, SharedAuthorizer)>,
    request: Request,
    next: Next,
) -> Response {
    // OPTIONS is a preflight verb. It carries no credential and changes nothing.
    if *request.method() == Method::OPTIONS {
        return next.run(request).await;
    }
    let Ok(tenant) = tenant_key(&request) else {
        return bad_tenant();
    };

    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let mut shards = Vec::new();
    shards.extend(path_shard(&api_state, &method, &path));
    shards.extend(query_shards(request.uri()));

    // Buffer a start body to read its placement, then put it back.
    let request = if let Some(workflow) = start_route_workflow(&method, &path) {
        let (parts, body) = request.into_parts();
        let Ok(bytes) = axum::body::to_bytes(body, MAX_START_BODY_BYTES).await else {
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        };
        shards.extend(start_body_shard(&api_state, workflow, &bytes));
        Request::from_parts(parts, axum::body::Body::from(bytes))
    } else {
        request
    };
    shards.sort_unstable_by_key(|s| s.as_i32());
    shards.dedup();

    let principal =
        request
            .extensions()
            .get::<TokenPrincipal>()
            .map_or(AuthzPrincipal::Embedder, |t| AuthzPrincipal::Token {
                id: t.id,
                scope: t.scope,
            });
    let route_class = classify_route(&method, &path);
    let candidates: Vec<Option<ShardId>> = if shards.is_empty() {
        vec![None]
    } else {
        shards.into_iter().map(Some).collect()
    };

    for shard in candidates {
        let authz = AuthzRequest::new(principal, route_class, &method, &path, request.extensions())
            .with_tenant_key(tenant.as_deref())
            .with_shard(shard);
        let AuthzDecision::Deny(reason) = authorizer.0.authorize(&authz).await else {
            continue;
        };
        tracing::warn!(
            method = %method,
            path = %path,
            tenant = tenant.as_deref().unwrap_or("-"),
            shard = shard.map(ShardId::as_i32),
            "harvest: authorizer denied request (403)"
        );
        let (actor, source, request_id) = audit_context(request.headers(), &api_state);
        let summary = format!(
            "authorizer denied (tenant={}, shard={}): {reason}",
            tenant.as_deref().unwrap_or("-"),
            shard.map_or_else(|| "-".to_string(), |s| s.as_i32().to_string()),
        );
        if let Ok(pool) = api_state.storage_pool()
            && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
        {
            audit_deny(
                &mut conn,
                &DenyAudit {
                    actor: &actor,
                    method: &method,
                    path: &path,
                    request_id: request_id.as_deref(),
                    source: &source,
                    shard,
                    summary: &summary,
                },
            )
            .await;
        } else {
            tracing::error!(path = %path, "harvest: no audit store for authz deny");
        }
        return forbidden();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("abc", 5), "abc");
        assert_eq!(truncate("abcdef", 3), "abc");
        // "é" is two bytes. A cut inside it backs off to the boundary.
        assert_eq!(truncate("aé", 2), "a");
    }

    #[test]
    fn query_shards_reads_every_alias() {
        let shards = |q: &str| query_shards(&q.parse::<axum::http::Uri>().unwrap());
        assert_eq!(shards("/workflows"), Vec::<ShardId>::new());
        assert_eq!(shards("/workflows?shard_id=7"), vec![ShardId::new(7)]);
        assert_eq!(shards("/workflows?shard=2&limit=5"), vec![ShardId::new(2)]);
        assert_eq!(shards("/workflows?shard-id=3"), vec![ShardId::new(3)]);
        // Percent-encoding decodes as in the handlers.
        assert_eq!(shards("/workflows?shard%5Fid=4"), vec![ShardId::new(4)]);
        assert_eq!(
            shards("/workflows?shard_id=x&shard_id=-1"),
            Vec::<ShardId>::new()
        );
    }

    #[test]
    fn start_route_workflow_matches_only_the_start_route() {
        assert_eq!(
            start_route_workflow(&Method::POST, "/workflows/wf/start"),
            Some("wf")
        );
        assert_eq!(
            start_route_workflow(&Method::GET, "/workflows/wf/start"),
            None
        );
        assert_eq!(
            start_route_workflow(&Method::POST, "/workflows//start"),
            None
        );
        assert_eq!(
            start_route_workflow(&Method::POST, "/workflows/a/b/start"),
            None
        );
        assert_eq!(
            start_route_workflow(&Method::POST, "/workflows/wf/signal-with-start"),
            None
        );
    }

    #[test]
    fn start_body_shard_reads_an_explicit_shard() {
        let state = HarvestApiState::new();
        assert_eq!(
            start_body_shard(&state, "wf", br#"{"shard_id": 4}"#),
            Some(ShardId::new(4))
        );
        assert_eq!(start_body_shard(&state, "wf", br#"{"shard_id": -4}"#), None);
        assert_eq!(start_body_shard(&state, "wf", br"{}"), None);
        assert_eq!(start_body_shard(&state, "wf", b"not json"), None);
        // No runtime is installed, so a residency key cannot resolve.
        assert_eq!(
            start_body_shard(&state, "wf", br#"{"residency_key": "eu"}"#),
            None
        );
    }

    #[test]
    fn builders_accept_an_annotated_closure() {
        // The form the docs show must compile.
        let policy = |req: &AuthzRequest<'_>| {
            if req.shard == Some(ShardId::new(2)) {
                AuthzDecision::deny("shard 2")
            } else {
                AuthzDecision::Allow
            }
        };
        let _ = crate::api::StandaloneAdminAuth::new().with_authorizer(policy);
        let _ = crate::HarvestPlugin::new().with_authorizer(policy);
        let _ = crate::HarvestPlugin::new().with_authorizer(AllowAll);
    }

    #[tokio::test]
    async fn closures_and_allow_all_are_authorizers() {
        let ext = Extensions::new();
        let method = Method::GET;
        let req = AuthzRequest::new(
            AuthzPrincipal::Embedder,
            RouteClass::ReadOnly,
            &method,
            "/workflows",
            &ext,
        )
        .with_tenant_key(Some("acme"))
        .with_shard(Some(ShardId::new(1)));

        let deny_acme = |r: &AuthzRequest<'_>| {
            if r.tenant_key == Some("acme") {
                AuthzDecision::deny("no")
            } else {
                AuthzDecision::Allow
            }
        };
        assert_eq!(
            deny_acme.authorize(&req).await,
            AuthzDecision::Deny("no".to_string())
        );
        assert_eq!(AllowAll.authorize(&req).await, AuthzDecision::Allow);
        let shared = SharedAuthorizer::new(AllowAll);
        assert_eq!(format!("{shared:?}"), "SharedAuthorizer(..)");
    }
}
