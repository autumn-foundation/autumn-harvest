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
//! - `shard`: read only from a source the route's handler uses. One source is
//!   an execution id in the path, also under `/ui`. The others are a shard
//!   query parameter or body field on the routes in `SHARD_SOURCES`. `None`
//!   means Harvest cannot name the shard before the handler runs. A list
//!   route reads every shard. A by-id route or an unpinned start reaches one
//!   shard by hash. A lineage route (`/children`, `/tree`) gets its
//!   execution's shards and also `None`, because it reads every shard.
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
use axum::body::{Body, Bytes};
use axum::extract::{FromRequest, Request, State};
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
    HarvestApiState, RouteMatchers, acquire_conn, audit_context, build_route_matchers,
    classify_route, execution_id_in_path, is_form_urlencoded, match_route,
};
use crate::api_token::{TokenPrincipal, TokenScope};

/// Longest accepted [`HEADER_TENANT`] value, in bytes.
pub const MAX_TENANT_KEY_LEN: usize = 128;

/// Longest request path an audit row records.
const MAX_AUDITED_PATH_LEN: usize = 256;

/// Longest `error_summary` an audit row records.
const MAX_AUDITED_SUMMARY_LEN: usize = 512;

/// Longest caller-supplied actor or request id an audit row records.
///
/// Both come from headers. Before `require_admin` runs, nothing has checked
/// them, so the cap bounds what one denied request can write.
const MAX_AUDITED_HEADER_LEN: usize = 256;

/// Who made the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
/// Annotate the closure argument as `|req: &AuthzRequest<'_>|`, or the
/// compiler cannot infer it. Implement the trait directly when the policy
/// needs I/O.
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

/// An authorizer that allows every request. It acts the same as no hook.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAll;

impl HarvestAuthorizer for AllowAll {
    fn authorize<'a>(&'a self, _request: &'a AuthzRequest<'a>) -> AuthzFuture<'a> {
        Box::pin(std::future::ready(AuthzDecision::Allow))
    }
}

/// A cloneable handle to an installed authorizer.
#[derive(Clone)]
pub(crate) struct SharedAuthorizer(Arc<dyn HarvestAuthorizer>);

impl SharedAuthorizer {
    /// Wrap `authorizer`.
    #[must_use]
    pub(crate) fn new(authorizer: impl HarvestAuthorizer) -> Self {
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
        actor: truncate(deny.actor, MAX_AUDITED_HEADER_LEN),
        operation: OP_AUTHZ_DENY,
        target_type: TARGET_ROUTE,
        target_id: None,
        route_or_command: &route,
        request_id: deny
            .request_id
            .map(|id| truncate(id, MAX_AUDITED_HEADER_LEN)),
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

// ── Shard sources ─────────────────────────────────────────────────────────────

/// Where a route's handler reads the one shard it acts on.
///
/// The hook reads a shard only from the source the handler uses. A shard named
/// anywhere else is ignored, so a caller cannot show the hook one shard while
/// the handler acts on all of them.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ShardSource {
    /// Query parameters with these names. Every value counts.
    Query(&'static [&'static str]),
    /// A JSON body field with this name. `form` also reads a form body.
    Body { field: &'static str, form: bool },
    /// A start body: `shard_id`, or a `residency_key` the router resolves.
    StartBody,
    /// The handler reads its execution's shards, then every shard for
    /// descendants. The hook is also called with `None`.
    FanOut,
}

/// Routes whose handler reads its shard from the query or the body.
///
/// Execution ids in the path are found from [`execution_id_in_path`] instead.
/// Each template must be in [`autumn_harvest::audit::CLASSIFIED_ROUTES`].
pub(crate) const SHARD_SOURCES: &[(&str, ShardSource)] = &[
    ("GET /workflows/{id}/children", ShardSource::FanOut),
    ("GET /workflows/{id}/tree", ShardSource::FanOut),
    (
        "GET /admin/history/exports",
        ShardSource::Query(&["shard_id", "shard-id", "shard"]),
    ),
    (
        "GET /admin/history/export-sample",
        ShardSource::Query(&["shard_id", "shard-id", "shard"]),
    ),
    (
        "GET /admin/external-handoffs",
        ShardSource::Query(&["shard_id", "shard"]),
    ),
    (
        "POST /workflows/{workflow_name}/start",
        ShardSource::StartBody,
    ),
    (
        "POST /dead-letters/replay",
        ShardSource::Body {
            field: "shard_id",
            form: true,
        },
    ),
    (
        "POST /dead-letters/discard",
        ShardSource::Body {
            field: "shard_id",
            form: true,
        },
    ),
    (
        "POST /dlq/redrive",
        ShardSource::Body {
            field: "shard_id",
            form: false,
        },
    ),
    (
        "POST /admin/queues/{queue_name}/pause",
        ShardSource::Body {
            field: "shard_id",
            form: false,
        },
    ),
    (
        "POST /admin/queues/{queue_name}/resume",
        ShardSource::Body {
            field: "shard_id",
            form: false,
        },
    ),
    (
        "POST /admin/audit-export/redrive",
        ShardSource::Body {
            field: "shard",
            form: false,
        },
    ),
    (
        "POST /admin/audit-export/decommission",
        ShardSource::Body {
            field: "shard",
            form: false,
        },
    ),
    (
        "POST /admin/audit-export/reactivate",
        ShardSource::Body {
            field: "shard",
            form: false,
        },
    ),
];

/// The shard source of `method` and `path`, if the route has one.
fn shard_source(method: &Method, path: &str) -> Option<ShardSource> {
    static MATCHERS: std::sync::OnceLock<RouteMatchers<ShardSource>> = std::sync::OnceLock::new();
    let matchers = MATCHERS
        .get_or_init(|| build_route_matchers("SHARD_SOURCES", SHARD_SOURCES.iter().copied()));
    match_route(matchers, method, path).map(|m| *m.value)
}

/// A non-negative shard number, or `None`.
fn shard_number(raw: i64) -> Option<ShardId> {
    i32::try_from(raw)
        .ok()
        .filter(|n| *n >= 0)
        .map(ShardId::new)
}

/// The shards an execution id in the path can touch.
///
/// That is the id's entry shard, the shard it lives on now, and the shard of
/// every later attempt in its retry chain. Handlers follow a rebalance
/// forward, and many follow the retry chain to the live attempt. The walks
/// are the ones the handlers use. If a walk fails, the handler fails it too,
/// so the request gets `503`. An unknown id adds no attempts; the handler
/// answers `404`.
///
/// A retired shard resolves to its successor. With a storage pool, an id with
/// no encoded shard resolves to the default shard. With no pool, it has none.
async fn path_shards(
    api_state: &HarvestApiState,
    method: &Method,
    path: &str,
) -> Result<Vec<ShardId>, Response> {
    let Some(exec_id) = execution_id_in_path(method, path) else {
        return Ok(Vec::new());
    };
    let Ok(pool) = api_state.storage_pool() else {
        return Ok(Some(exec_id.shard())
            .filter(|s| !s.is_unencoded())
            .into_iter()
            .collect());
    };
    let pool = pool.sharded_pool();
    let unavailable = |e: &autumn_harvest::HarvestError| {
        tracing::warn!(error = %e, path = %path, "harvest: authz could not resolve shard");
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    };
    let (mut conn, live) =
        autumn_harvest::shard_rebalance::conn_for_execution_forwarded_with_shard(pool, exec_id)
            .await
            .map_err(|e| unavailable(&e))?;
    let mut shards = vec![pool.routed_shard_for_execution(exec_id), live];
    match autumn_harvest::execution::walk_retry_chain(&mut conn, pool, live, exec_id).await {
        Ok(chain) => shards.extend(chain.into_iter().map(|(_, shard)| shard)),
        Err(autumn_harvest::HarvestError::NotFound(_)) => {}
        Err(e) => return Err(unavailable(&e)),
    }
    Ok(shards)
}

/// The shards named by `keys` in the query, decoded as the handlers decode it.
///
/// A query that does not decode, or a value that does not parse, names no
/// shard. The handler rejects it.
fn query_shards(query: Option<&str>, keys: &[&str]) -> Vec<ShardId> {
    let Some(Ok(pairs)) = query.map(crate::strict_query::parse_raw_query_pairs_strict) else {
        return Vec::new();
    };
    pairs
        .into_iter()
        .filter(|(k, _)| keys.contains(&k.as_str()))
        .filter_map(|(_, v)| v.trim().parse::<i64>().ok())
        .filter_map(shard_number)
        .collect()
}

/// The start-body fields that pick a shard. Other fields are skipped.
#[derive(serde::Deserialize)]
struct StartPlacement {
    #[serde(default)]
    shard_id: Option<i64>,
    #[serde(default)]
    residency_key: Option<String>,
}

/// The shard a start body pins, if it pins one.
///
/// A `residency_key` resolves through the shard router. A body that does not
/// parse names no shard. The handler rejects it.
fn start_body_shard(api_state: &HarvestApiState, body: &[u8]) -> Option<ShardId> {
    let placement: StartPlacement = serde_json::from_slice(body).ok()?;
    if let Some(raw) = placement.shard_id {
        return shard_number(raw);
    }
    let key = placement.residency_key?;
    let key = key.trim();
    if key.is_empty() {
        return None;
    }
    // A residency key maps to one shard whatever the workflow name or id.
    api_state
        .runtime()
        .ok()?
        .router()
        .resolve_placement_for_lookup(&ShardPlacement::residency_key(key), "", "")
        .ok()
}

/// The shard one body `field` names, from a JSON or a form body.
fn body_field_shard(field: &str, form: bool, is_form: bool, body: &[u8]) -> Option<ShardId> {
    if form && is_form {
        let raw = std::str::from_utf8(body).ok()?;
        let pairs = crate::strict_query::parse_raw_query_pairs_strict(raw).ok()?;
        let value = pairs.into_iter().rev().find(|(k, _)| k == field)?.1;
        return value.trim().parse::<i64>().ok().and_then(shard_number);
    }
    let value: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(body).ok()?;
    value.get(field)?.as_i64().and_then(shard_number)
}

// ── Request inputs ────────────────────────────────────────────────────────────

/// Read the tenant header. `Err` means the header is present but unusable.
///
/// A repeated header is refused, because two layers could each read a
/// different value.
fn tenant_key(request: &Request) -> Result<Option<String>, ()> {
    let mut values = request.headers().get_all(HEADER_TENANT).iter();
    let Some(raw) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    let value = raw.to_str().map_err(|_| ())?.trim();
    if value.is_empty() || value.len() > MAX_TENANT_KEY_LEN {
        return Err(());
    }
    Ok(Some(value.to_string()))
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
                "invalid {HEADER_TENANT} header: expected one value of 1 to {MAX_TENANT_KEY_LEN} visible characters"
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
pub(crate) async fn enforce_authorizer(
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
    let mut shards = match path_shards(&api_state, &method, &path).await {
        Ok(shards) => shards,
        Err(response) => return response,
    };

    let source = shard_source(&method, &path);
    let request = match source {
        None | Some(ShardSource::FanOut) => request,
        Some(ShardSource::Query(keys)) => {
            shards.extend(query_shards(request.uri().query(), keys));
            request
        }
        Some(source) => {
            // Buffer the body under the app's own limit, read the shard, then
            // put the body back for the handler.
            let is_form = is_form_urlencoded(request.headers());
            let (parts, body) = request.into_parts();
            let bytes =
                match Bytes::from_request(Request::from_parts(parts.clone(), body), &()).await {
                    Ok(bytes) => bytes,
                    Err(rejection) => return rejection.into_response(),
                };
            shards.extend(match source {
                ShardSource::StartBody => start_body_shard(&api_state, &bytes),
                ShardSource::Body { field, form } => body_field_shard(field, form, is_form, &bytes),
                ShardSource::Query(_) | ShardSource::FanOut => None,
            });
            Request::from_parts(parts, Body::from(bytes))
        }
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
    let fan_out = matches!(source, Some(ShardSource::FanOut));
    let mut candidates: Vec<Option<ShardId>> = shards.into_iter().map(Some).collect();
    if candidates.is_empty() || fan_out {
        candidates.push(None);
    }

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
        // Debug-quote the tenant, so a crafted value cannot forge the text
        // that follows it.
        let summary = format!(
            "authorizer denied (tenant={:?}, shard={}): {reason}",
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
    fn shard_sources_are_classified_routes() {
        for (template, _) in SHARD_SOURCES {
            assert!(
                autumn_harvest::audit::CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, _)| r == template),
                "{template} must be a classified route"
            );
        }
    }

    #[test]
    fn shard_source_matches_only_listed_routes() {
        assert!(matches!(
            shard_source(&Method::POST, "/workflows/wf/start"),
            Some(ShardSource::StartBody)
        ));
        assert!(matches!(
            shard_source(&Method::GET, "/admin/history/exports"),
            Some(ShardSource::Query(_))
        ));
        assert!(shard_source(&Method::GET, "/workflows").is_none());
        assert!(shard_source(&Method::POST, "/workflows/wf/signal-with-start").is_none());
    }

    #[test]
    fn query_shards_reads_only_the_given_keys() {
        let keys = &["shard_id", "shard"];
        assert_eq!(query_shards(None, keys), Vec::<ShardId>::new());
        assert_eq!(
            query_shards(Some("shard_id=7"), keys),
            vec![ShardId::new(7)]
        );
        assert_eq!(
            query_shards(Some("shard=2&shard_id=3"), keys),
            vec![ShardId::new(2), ShardId::new(3)]
        );
        assert_eq!(
            query_shards(Some("shard-id=3"), keys),
            Vec::<ShardId>::new()
        );
        // Percent-encoding decodes as in the handlers.
        assert_eq!(
            query_shards(Some("shard%5Fid=4"), keys),
            vec![ShardId::new(4)]
        );
        assert_eq!(
            query_shards(Some("shard_id=x&shard_id=-1"), keys),
            Vec::<ShardId>::new()
        );
        // A query that does not decode names no shard.
        assert_eq!(
            query_shards(Some("shard_id=%FF"), keys),
            Vec::<ShardId>::new()
        );
    }

    #[test]
    fn start_body_shard_reads_an_explicit_shard() {
        let state = HarvestApiState::new();
        assert_eq!(
            start_body_shard(&state, br#"{"shard_id": 4, "input": {"shard_id": 9}}"#),
            Some(ShardId::new(4))
        );
        assert_eq!(start_body_shard(&state, br#"{"shard_id": -4}"#), None);
        assert_eq!(start_body_shard(&state, br"{}"), None);
        assert_eq!(start_body_shard(&state, b"not json"), None);
        // No runtime is installed, so a residency key cannot resolve.
        assert_eq!(
            start_body_shard(&state, br#"{"residency_key": "eu"}"#),
            None
        );
    }

    #[test]
    fn body_field_shard_reads_json_and_form() {
        assert_eq!(
            body_field_shard("shard", false, false, br#"{"shard": 5}"#),
            Some(ShardId::new(5))
        );
        assert_eq!(
            body_field_shard("shard_id", true, true, b"dry_run=true&shard_id=6"),
            Some(ShardId::new(6))
        );
        // A form body on a JSON-only route is read as JSON, and fails.
        assert_eq!(
            body_field_shard("shard_id", false, true, b"shard_id=6"),
            None
        );
        assert_eq!(
            body_field_shard("shard", false, false, br#"{"shard": "5"}"#),
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
