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
//! - An allow fences the handler to the shards the policy saw
//!   ([`autumn_harvest::shard_fence`]). A rebalance cutover after the check
//!   cannot lead the handler onto a shard the policy never saw. A checkout
//!   outside the fence gets `503` with a retry hint, and the retry is
//!   authorized on the run's new shard. A request the policy saw with
//!   `shard: None` gets no fence.
//! - The default is no hook. The router is then byte-for-byte unchanged.
//!
//! # Inputs
//!
//! - `principal`: the verified token, or [`AuthzPrincipal::Embedder`] for every
//!   other caller. Read the embedder's own claims from `extensions`.
//! - `route_class`: from [`autumn_harvest::audit::CLASSIFIED_ROUTES`].
//!   An unclassified path is `Mutating`.
//! - `tenant_key`: the verified tenant, when the credential carries one
//!   (issue #1977). `tenant_verified` is then `true`. See [`crate::tenant`].
//!   Otherwise it is the [`autumn_harvest::audit::HEADER_TENANT`] header,
//!   and `tenant_verified` is `false`. The caller declares that header, so
//!   do not grant access on an unverified tenant.
//! - `shard`: read only from a source the route's handler uses. One source is
//!   an execution id in the path, also under `/ui`. The others are a shard
//!   query parameter or body field on the routes in `SHARD_SOURCES`. `None`
//!   means Harvest cannot name the shard before the handler runs. A list
//!   route reads every shard. A by-id route or an unpinned start reaches one
//!   shard by hash. A lineage route (`/children`, `/tree`, `erase-payloads`)
//!   gets its execution's shards and also `None`, because it reads, or
//!   erases, every shard.
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
use autumn_harvest::models::{NewAuditRecord, WorkflowExecution};
use autumn_harvest::shard::{ShardPlacement, ShardedDbPool};
use autumn_harvest::shard_fence::ShardFence;
use autumn_harvest::types::ShardId;

use crate::api::{
    HarvestApiState, RouteMatchers, acquire_conn, audit_context, build_route_matchers,
    classify_route, execution_id_in_path, is_form_urlencoded, match_route,
};
use crate::api_token::{TokenPrincipal, TokenScope};

/// Longest accepted [`HEADER_TENANT`] value, in bytes.
pub const MAX_TENANT_KEY_LEN: usize = autumn_harvest::tenant::MAX_TENANT_LEN;

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
    /// The tenant key. It is the verified tenant when `tenant_verified` is
    /// `true`. Otherwise it is the caller-declared header, trimmed.
    pub tenant_key: Option<&'a str>,
    /// Whether a credential verified `tenant_key` (issue #1977).
    pub tenant_verified: bool,
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
            tenant_verified: false,
            shard: None,
            method,
            path,
            extensions,
        }
    }

    /// Set an unverified tenant key.
    #[must_use]
    pub const fn with_tenant_key(mut self, tenant_key: Option<&'a str>) -> Self {
        self.tenant_key = tenant_key;
        self.tenant_verified = false;
        self
    }

    /// Set a verified tenant key (issue #1977).
    #[must_use]
    pub const fn with_verified_tenant(mut self, tenant: &'a str) -> Self {
        self.tenant_key = Some(tenant);
        self.tenant_verified = true;
        self
    }

    /// Set the tenant key. `verified` applies only to a present key.
    const fn with_tenant(mut self, tenant: Option<&'a str>, verified: bool) -> Self {
        self.tenant_key = tenant;
        self.tenant_verified = verified && tenant.is_some();
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
    audit_route_event(conn, OP_AUTHZ_DENY, deny).await;
}

/// Write one failed-request row for `operation` to the control shard.
///
/// The API rate limiter (issue #1827) writes its sustained rows with this. The
/// fields are cut to the same limits as an `authz.deny` row.
pub(crate) async fn audit_route_event(
    conn: &mut AsyncPgConnection,
    operation: &str,
    deny: &DenyAudit<'_>,
) {
    let route = format!(
        "{} {}",
        deny.method,
        truncate(deny.path, MAX_AUDITED_PATH_LEN)
    );
    let record = NewAuditRecord {
        actor: truncate(deny.actor, MAX_AUDITED_HEADER_LEN),
        operation,
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
        tracing::error!(
            error = %e,
            route = %route,
            operation,
            "harvest: failed to audit route event"
        );
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
    /// The handler reads its execution's shards, then reads or erases
    /// descendants on every shard. The hook is also called with `None`.
    FanOut,
}

/// Routes whose handler reads its shard from the query or the body.
///
/// Execution ids in the path are found from [`execution_id_in_path`] instead.
/// Each template must be in [`autumn_harvest::audit::CLASSIFIED_ROUTES`].
pub(crate) const SHARD_SOURCES: &[(&str, ShardSource)] = &[
    ("GET /workflows/{id}/children", ShardSource::FanOut),
    ("GET /workflows/{id}/tree", ShardSource::FanOut),
    ("POST /workflows/{id}/erase-payloads", ShardSource::FanOut),
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
/// That is the id's entry shard and the shard it lives on now. On a route in
/// [`RETRY_CHAIN_ROUTES`], it is also the shard of every later attempt in the
/// retry chain. On a route in [`CONTINUED_AS_NEW_ROUTES`], it is also every
/// shard of each continued-as-new successor. The walks are the ones the
/// handlers use. If a walk fails, the handler fails it too, so the request
/// gets `503`. An unknown id adds no attempts; the handler answers `404`.
///
/// The walks run under [`autumn_harvest::shard_fence::record`]. Every shard
/// a walk names, such as a forwarding hop or an attempt's own entry shard,
/// is in the result. The handler runs the same walk under the fence, so it
/// names the same shards. A request whose run has not moved never trips its
/// own fence.
///
/// A retired shard resolves to its successor. With a storage pool, an id with
/// no encoded shard resolves to the default shard. With no pool, it has none.
async fn path_shards(
    api_state: &HarvestApiState,
    method: &Method,
    path: &str,
) -> Result<Vec<ShardId>, StatusCode> {
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
        StatusCode::SERVICE_UNAVAILABLE
    };
    let retry_chain = follows_retry_chain(method, path);
    let continued_as_new = follows_continued_as_new(method, path);
    let (resolved, named) = autumn_harvest::shard_fence::record(async {
        let (mut conn, live) =
            autumn_harvest::shard_rebalance::conn_for_execution_forwarded_with_shard(pool, exec_id)
                .await?;
        let mut shards = vec![pool.routed_shard_for_execution(exec_id), live];
        if !retry_chain {
            return Ok(shards);
        }
        let mut chain =
            match autumn_harvest::execution::walk_retry_chain(&mut conn, pool, live, exec_id).await
            {
                Ok(chain) => chain,
                Err(autumn_harvest::HarvestError::NotFound(_)) => return Ok(shards),
                Err(e) => return Err(e),
            };
        shards.extend(chain.iter().map(|(_, shard)| *shard));
        // The handler holds no connection across a successor hop. Release
        // this one, so a pool of size one cannot wait on itself.
        drop(conn);
        if continued_as_new && let Some((attempt, _)) = chain.pop() {
            continued_as_new_shards(pool, attempt, &mut shards).await?;
        }
        Ok::<_, autumn_harvest::HarvestError>(shards)
    })
    .await;
    let mut shards = resolved.map_err(|e| unavailable(&e))?;
    shards.extend(named);
    Ok(shards)
}

/// The most continued-as-new hops a walk follows. The same bound as the
/// `/result` handler.
const CONTINUED_AS_NEW_MAX_HOPS: usize = 128;

/// Add the shards of every continued-as-new successor of `attempt`.
///
/// This is the walk the `/result` handler runs. While the live attempt is
/// `CONTINUED_AS_NEW`, its history names a successor. The successor's routed
/// shard, its live shard and the shards of its retry chain are added. The
/// walk then goes on from the successor's live attempt. A successor with no
/// row ends the walk, because the handler then answers with the last row it
/// found. Any other failure is an error, as it is in the handler.
async fn continued_as_new_shards(
    pool: &ShardedDbPool,
    mut attempt: WorkflowExecution,
    shards: &mut Vec<ShardId>,
) -> autumn_harvest::HarvestResult<()> {
    use autumn_harvest::HarvestError;
    for _ in 0..CONTINUED_AS_NEW_MAX_HOPS {
        if attempt.state != "CONTINUED_AS_NEW" {
            return Ok(());
        }
        let effective = autumn_harvest::types::ExecutionId::from_uuid(attempt.id);
        let Some(next) = continued_as_new_successor(pool, effective).await? else {
            return Ok(());
        };
        shards.push(pool.routed_shard_for_execution(next));
        let (mut conn, live) =
            match autumn_harvest::shard_rebalance::conn_for_execution_forwarded_with_shard(
                pool, next,
            )
            .await
            {
                Ok(found) => found,
                Err(HarvestError::NotFound(_)) => return Ok(()),
                Err(e) => return Err(e),
            };
        shards.push(live);
        let mut chain =
            match autumn_harvest::execution::walk_retry_chain(&mut conn, pool, live, next).await {
                Ok(chain) => chain,
                Err(HarvestError::NotFound(_)) => return Ok(()),
                Err(e) => return Err(e),
            };
        shards.extend(chain.iter().map(|(_, shard)| *shard));
        let Some((last, _)) = chain.pop() else {
            return Ok(());
        };
        attempt = last;
    }
    Ok(())
}

/// The successor named by the `WorkflowContinuedAsNew` event of `exec_id`.
///
/// The history is read undecoded, as the handler reads it. Only the typed
/// successor id is used, so a codec envelope rides along untouched.
async fn continued_as_new_successor(
    pool: &ShardedDbPool,
    exec_id: autumn_harvest::types::ExecutionId,
) -> autumn_harvest::HarvestResult<Option<autumn_harvest::types::ExecutionId>> {
    let (mut conn, _) =
        autumn_harvest::shard_rebalance::conn_for_execution_forwarded_with_shard(pool, exec_id)
            .await?;
    let history = autumn_harvest::store::load_history_undecoded(&mut conn, exec_id).await?;
    Ok(history.events.into_iter().find_map(|event| match event {
        autumn_harvest::WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => {
            Some(new_exec_id)
        }
        _ => None,
    }))
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

/// Routes whose handler follows the retry chain to the live attempt.
///
/// Every other execution-id route acts on the attempt it names. Checking the
/// chain there would deny, or fail, a request that never touches a later
/// attempt. Each template must be in
/// [`autumn_harvest::audit::CLASSIFIED_ROUTES`].
pub(crate) const RETRY_CHAIN_ROUTES: &[&str] = &[
    "GET /workflows/{id}/result",
    "POST /workflows/{id}/cancel",
    "POST /workflows/{id}/terminate",
    "POST /workflows/{id}/pause",
    "POST /workflows/{id}/resume",
    "POST /workflows/{id}/signal/{signal_name}",
    "GET /workflows/{id}/query/{query_name}",
    "POST /workflows/{id}/query/{query_name}",
    "GET /workflows/{id}/queries",
    "POST /workflows/{id}/update/{update_name}",
    "GET /workflows/{id}/update/{update_id}/result",
];

/// Whether the handler of `method` and `path` follows the retry chain.
fn follows_retry_chain(method: &Method, path: &str) -> bool {
    static MATCHERS: std::sync::OnceLock<RouteMatchers<()>> = std::sync::OnceLock::new();
    let matchers = MATCHERS.get_or_init(|| {
        build_route_matchers(
            "RETRY_CHAIN_ROUTES",
            RETRY_CHAIN_ROUTES.iter().map(|route| (*route, ())),
        )
    });
    match_route(matchers, method, path).is_some()
}

/// Routes whose handler follows the continued-as-new chain to its end.
///
/// Each successor is read from the predecessor's history, so the successor
/// can live on any shard. Every template must also be in
/// [`RETRY_CHAIN_ROUTES`], because the walk starts at the live attempt. Each
/// template must be in [`autumn_harvest::audit::CLASSIFIED_ROUTES`].
pub(crate) const CONTINUED_AS_NEW_ROUTES: &[&str] = &["GET /workflows/{id}/result"];

/// Whether the handler of `method` and `path` follows continued-as-new.
fn follows_continued_as_new(method: &Method, path: &str) -> bool {
    static MATCHERS: std::sync::OnceLock<RouteMatchers<()>> = std::sync::OnceLock::new();
    let matchers = MATCHERS.get_or_init(|| {
        build_route_matchers(
            "CONTINUED_AS_NEW_ROUTES",
            CONTINUED_AS_NEW_ROUTES.iter().map(|route| (*route, ())),
        )
    });
    match_route(matchers, method, path).is_some()
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

/// The tenant the hook sees, and whether a credential verified it.
///
/// A verified tenant replaces the header (issue #1977). The tenant binding
/// layer has already refused a header that names another tenant. `Err`
/// means an unusable header.
fn request_tenant(request: &Request) -> Result<(Option<String>, bool), ()> {
    request
        .extensions()
        .get::<crate::tenant::VerifiedTenant>()
        .map_or_else(
            || tenant_key(request).map(|t| (t, false)),
            |t| Ok((Some(t.as_str().to_string()), true)),
        )
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({ "error": "forbidden by authorization policy" })),
    )
        .into_response()
}

pub(crate) fn bad_tenant() -> Response {
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
    let Ok((tenant, tenant_verified)) = request_tenant(&request) else {
        return bad_tenant();
    };

    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let mut shards = match path_shards(&api_state, &method, &path).await {
        Ok(shards) => shards,
        Err(status) => return status.into_response(),
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
    let mut candidates: Vec<Option<ShardId>> = shards.iter().copied().map(Some).collect();
    if candidates.is_empty() || fan_out {
        candidates.push(None);
    }
    // The policy sees `None` when the handler reads every shard by design.
    // Such a request gets no fence. Every other request is fenced to the
    // shards the policy saw. A cutover after this check then cannot lead the
    // handler onto a shard the policy never decided on.
    let fence = (!candidates.contains(&None)).then(|| ShardFence::new(shards));

    for shard in candidates {
        let authz = AuthzRequest::new(principal, route_class, &method, &path, request.extensions())
            .with_tenant(tenant.as_deref(), tenant_verified)
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
    match fence {
        Some(fence) => fence.scope(next.run(request)).await,
        None => next.run(request).await,
    }
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
    fn retry_chain_routes_are_classified_routes() {
        for template in RETRY_CHAIN_ROUTES {
            assert!(
                autumn_harvest::audit::CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, _)| r == template),
                "{template} must be a classified route"
            );
        }
        assert!(follows_retry_chain(&Method::POST, "/workflows/x/cancel"));
        assert!(follows_retry_chain(&Method::HEAD, "/workflows/x/result"));
        assert!(!follows_retry_chain(&Method::GET, "/workflows/x/history"));
        assert!(!follows_retry_chain(
            &Method::POST,
            "/ui/workflows/x/cancel"
        ));
    }

    #[test]
    fn continued_as_new_routes_are_retry_chain_routes() {
        for template in CONTINUED_AS_NEW_ROUTES {
            assert!(
                autumn_harvest::audit::CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, _)| r == template),
                "{template} must be a classified route"
            );
            assert!(
                RETRY_CHAIN_ROUTES.contains(template),
                "{template} must follow the retry chain first"
            );
        }
        assert!(follows_continued_as_new(
            &Method::GET,
            "/workflows/x/result"
        ));
        assert!(follows_continued_as_new(
            &Method::HEAD,
            "/workflows/x/result"
        ));
        assert!(!follows_continued_as_new(
            &Method::POST,
            "/workflows/x/cancel"
        ));
        assert!(!follows_continued_as_new(
            &Method::GET,
            "/workflows/x/history"
        ));
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
        for (method, path) in [
            (Method::GET, "/workflows/x/children"),
            (Method::GET, "/workflows/x/tree"),
            (Method::POST, "/workflows/x/erase-payloads"),
        ] {
            assert!(
                matches!(shard_source(&method, path), Some(ShardSource::FanOut)),
                "{method} {path} fans out"
            );
        }
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
