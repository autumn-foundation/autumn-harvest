//! Tenant binding for the management API (issue #1977).
//!
//! A credential can carry a tenant. A Harvest token carries it in its
//! `tenant` column. The embedder's own auth layer carries it as a
//! [`VerifiedTenant`] request extension. Harvest calls that tenant the
//! *verified tenant*. The caller cannot change it.
//!
//! # Contract
//!
//! With no verified tenant, a request passes this layer unchanged. With a
//! verified tenant `T`:
//!
//! 1. A token tenant and an embedder tenant that differ get `403`.
//! 2. An `x-harvest-tenant` header that is not `T` gets `403`.
//! 3. A route not in [`TENANT_SCOPED_ROUTES`] gets `403`. A public route
//!    passes. List routes, admin routes, token mint, Vantage and the other
//!    start paths are not tenant-scoped.
//! 4. On a route with an execution id, the live run must have tenant `T`.
//!    Otherwise the answer is `404`, the same as for an unknown id.
//! 5. A start that meets a run of another tenant gets `409`. The engine
//!    refuses it before it attaches, cancels or replaces anything (see
//!    [`autumn_harvest::HarvestError::TenantConflict`]). Every `409` answer
//!    of a bound start is replaced by one that names no run.
//! 6. The layer inserts `VerifiedTenant(T)`. The authorizer hook sees `T`,
//!    and the start handler stamps `T` on the new run.
//!
//! Each `403` and each refused run writes one `authz.deny` audit row.
//!
//! # Placement
//!
//! The layer runs on every mount. It runs after the token layer and the
//! rate limiter, and before the read-only layer and the authorizer. The
//! limiter so bounds the lookups and the audit writes of refusals.
//!
//! # Limits
//!
//! Harvest does not filter list routes by tenant. A tenant-bound caller
//! cannot use them. Generated MCP tool routes are outside the management
//! router, so this layer does not cover them. Do not expose them to a
//! tenant-bound caller.

use autumn_web::reexports::axum;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use autumn_harvest::audit::{HEADER_TENANT, RouteClass};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::tenant::{InvalidTenant, validate_tenant};
use autumn_harvest::types::ExecutionId;

use crate::api::{
    HarvestApiState, RouteMatchers, acquire_conn, audit_context, build_route_matchers,
    classify_route, execution_id_in_path, match_route,
};
use crate::api_token::TokenPrincipal;
use crate::authz::{DenyAudit, audit_deny};

/// A tenant that the embedder's own auth layer has verified (issue #1977).
///
/// Insert it as a request extension in the auth middleware that wraps the
/// Harvest router. Harvest then binds the request to this tenant. A
/// Harvest token tenant fills the same role for a token caller.
///
/// ```rust
/// use autumn_harvest_plugin::tenant::VerifiedTenant;
///
/// let tenant = VerifiedTenant::new("acme").expect("a valid tenant key");
/// assert_eq!(tenant.as_str(), "acme");
/// assert!(VerifiedTenant::new("not valid").is_err());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedTenant(String);

impl VerifiedTenant {
    /// A verified tenant.
    ///
    /// # Errors
    ///
    /// Returns the rule that `tenant` breaks. See
    /// [`autumn_harvest::tenant::validate_tenant`].
    pub fn new(tenant: impl Into<String>) -> Result<Self, InvalidTenant> {
        let tenant = tenant.into();
        validate_tenant(&tenant)?;
        Ok(Self(tenant))
    }

    /// The tenant key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The routes a tenant-bound caller may use (issue #1977).
///
/// Each route acts on one run that the caller names by id, or starts one
/// run. The binding layer checks the run's tenant. Every other route is
/// refused. Each template must be in
/// [`autumn_harvest::audit::CLASSIFIED_ROUTES`].
pub const TENANT_SCOPED_ROUTES: &[&str] = &[
    START_ROUTE,
    "GET /workflows/{id}",
    "GET /workflows/{id}/history",
    "GET /workflows/{id}/result",
    "POST /workflows/{id}/cancel",
    "POST /workflows/{id}/terminate",
    "POST /workflows/{id}/signal/{signal_name}",
    "GET /workflows/{id}/query/{query_name}",
    "POST /workflows/{id}/query/{query_name}",
    "GET /workflows/{id}/queries",
    "POST /workflows/{id}/update/{update_name}",
    "GET /workflows/{id}/update/{update_id}/result",
];

/// The one start route a tenant-bound caller may use.
const START_ROUTE: &str = "POST /workflows/{workflow_name}/start";

/// What the binding layer knows about one route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Start,
    Run,
}

fn route_scope(method: &Method, path: &str) -> Option<Scope> {
    static MATCHERS: std::sync::OnceLock<RouteMatchers<Scope>> = std::sync::OnceLock::new();
    let matchers = MATCHERS.get_or_init(|| {
        build_route_matchers(
            "TENANT_SCOPED_ROUTES",
            TENANT_SCOPED_ROUTES.iter().map(|route| {
                let scope = if *route == START_ROUTE {
                    Scope::Start
                } else {
                    Scope::Run
                };
                (*route, scope)
            }),
        )
    });
    match_route(matchers, method, path).map(|m| *m.value)
}

/// The `403` body. It names no tenant and no reason.
pub(crate) fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({ "error": "forbidden by tenant binding" })),
    )
        .into_response()
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({ "error": "workflow execution not found" })),
    )
        .into_response()
}

fn conflict() -> Response {
    (
        StatusCode::CONFLICT,
        axum::Json(serde_json::json!({ "error": "workflow id is in use" })),
    )
        .into_response()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({ "error": "harvest store unavailable" })),
    )
        .into_response()
}

/// The one verified tenant of `request`, if any.
///
/// `Err` means the request carries two verified tenants that differ, or a
/// stored token tenant that is not valid. Both fail closed.
fn verified_tenant(request: &Request) -> Result<Option<String>, &'static str> {
    let token = request
        .extensions()
        .get::<TokenPrincipal>()
        .and_then(|t| t.tenant.clone());
    let embedder = request
        .extensions()
        .get::<VerifiedTenant>()
        .map(|t| t.as_str().to_string());
    if let Some(t) = &token
        && validate_tenant(t).is_err()
    {
        return Err("token tenant is not a valid tenant key");
    }
    match (token, embedder) {
        (Some(t), Some(e)) if t != e => Err("token tenant and embedder tenant differ"),
        (Some(t), _) | (None, Some(t)) => Ok(Some(t)),
        (None, None) => Ok(None),
    }
}

/// The stored tenant of the live row of `exec_id`.
///
/// `Ok(None)` means no such run. `Ok(Some(None))` means a run with no
/// tenant. `Err` means the store is unavailable.
async fn stored_tenant(
    api_state: &HarvestApiState,
    exec_id: ExecutionId,
) -> Result<Option<Option<String>>, StoreUnavailable> {
    let Ok(pool) = api_state.storage_pool() else {
        return Err(StoreUnavailable);
    };
    let (mut conn, _) =
        match autumn_harvest::shard_rebalance::conn_for_execution_forwarded_with_shard(
            pool.sharded_pool(),
            exec_id,
        )
        .await
        {
            Ok(found) => found,
            Err(autumn_harvest::HarvestError::NotFound(_)) => return Ok(None),
            Err(e) => {
                tracing::warn!(error = %e, "harvest: tenant binding could not resolve run");
                return Err(StoreUnavailable);
            }
        };
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::tenant)
        .first::<Option<String>>(&mut conn)
        .await
        .optional()
        .map_err(|e| {
            tracing::warn!(error = %e, "harvest: tenant binding could not read run");
            StoreUnavailable
        })
}

/// The run store could not answer. The layer answers `503`.
#[derive(Clone, Copy, Debug)]
struct StoreUnavailable;

/// The audit fields of one request, read before any `await`.
///
/// A `Request` is not `Sync`, so the layer must not hold a reference to it
/// across an `await`.
struct AuditFields {
    actor: String,
    source: String,
    request_id: Option<String>,
    method: Method,
    path: String,
}

impl AuditFields {
    fn of(api_state: &HarvestApiState, request: &Request) -> Self {
        let (actor, source, request_id) = audit_context(request.headers(), api_state);
        Self {
            actor,
            source,
            request_id,
            method: request.method().clone(),
            path: request.uri().path().to_string(),
        }
    }
}

/// Write one `authz.deny` row for a tenant binding refusal.
async fn audit_refusal(api_state: &HarvestApiState, fields: &AuditFields, summary: &str) {
    tracing::warn!(
        method = %fields.method,
        path = %fields.path,
        summary,
        "harvest: tenant binding refused request"
    );
    let Ok(pool) = api_state.storage_pool() else {
        tracing::error!("harvest: no audit store for tenant binding refusal");
        return;
    };
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        tracing::error!("harvest: no audit connection for tenant binding refusal");
        return;
    };
    audit_deny(
        &mut conn,
        &DenyAudit {
            actor: &fields.actor,
            method: &fields.method,
            path: &fields.path,
            request_id: fields.request_id.as_deref(),
            source: &fields.source,
            shard: None,
            summary,
        },
    )
    .await;
}

/// The declared tenant header. `Err` means it is present but unusable.
fn declared_tenant(request: &Request) -> Result<Option<String>, ()> {
    let mut values = request.headers().get_all(HEADER_TENANT).iter();
    let Some(raw) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    let value = raw.to_str().map_err(|_| ())?.trim();
    if value.is_empty() || value.len() > autumn_harvest::tenant::MAX_TENANT_LEN {
        return Err(());
    }
    Ok(Some(value.to_string()))
}

/// The tenant binding layer (issue #1977). See the module docs.
pub(crate) async fn enforce_tenant_binding(
    State(api_state): State<HarvestApiState>,
    mut request: Request,
    next: Next,
) -> Response {
    if *request.method() == Method::OPTIONS {
        return next.run(request).await;
    }
    let verified = verified_tenant(&request);
    let declared = declared_tenant(&request);
    let fields = AuditFields::of(&api_state, &request);
    let tenant = match verified {
        Ok(Some(tenant)) => tenant,
        Ok(None) => return next.run(request).await,
        Err(reason) => {
            audit_refusal(&api_state, &fields, &format!("tenant mismatch: {reason}")).await;
            return forbidden();
        }
    };

    // Debug-quote each tenant, so a crafted value cannot forge the summary.
    match declared {
        Err(()) => return crate::authz::bad_tenant(),
        Ok(Some(declared)) if declared != tenant => {
            let summary = format!("tenant mismatch (declared={declared:?}, verified={tenant:?})");
            audit_refusal(&api_state, &fields, &summary).await;
            return forbidden();
        }
        Ok(_) => {}
    }

    let (method, path) = (fields.method.clone(), fields.path.clone());
    // Later layers, the authorizer included, see the verified tenant on
    // every route. `verified_tenant` checked the key above.
    if request.extensions().get::<VerifiedTenant>().is_none() {
        request
            .extensions_mut()
            .insert(VerifiedTenant(tenant.clone()));
    }
    if classify_route(&method, &path) == RouteClass::PublicSafe {
        return next.run(request).await;
    }
    let Some(scope) = route_scope(&method, &path) else {
        let summary = format!("route is not tenant-scoped (tenant={tenant:?})");
        audit_refusal(&api_state, &fields, &summary).await;
        return forbidden();
    };

    if scope == Scope::Run {
        let Some(exec_id) = execution_id_in_path(&method, &path) else {
            return not_found();
        };
        match stored_tenant(&api_state, exec_id).await {
            Err(StoreUnavailable) => return unavailable(),
            Ok(None) => return not_found(),
            Ok(Some(owner)) if owner.as_deref() != Some(tenant.as_str()) => {
                let summary =
                    format!("run belongs to another tenant (tenant={tenant:?}, owner={owner:?})");
                audit_refusal(&api_state, &fields, &summary).await;
                return not_found();
            }
            Ok(Some(_)) => {}
        }
    }

    if scope == Scope::Run {
        return next.run(request).await;
    }

    // A start can resolve to a run that already exists: an idempotent replay
    // or a `use_existing` conflict. That run must be the caller's own.
    let response = next.run(request).await;
    if response.status() == StatusCode::CONFLICT {
        // A conflict body can name the prior run. The engine refuses a run
        // of another tenant, so replace every conflict with one that names
        // nothing.
        let summary = format!("start conflicted (tenant={tenant:?})");
        audit_refusal(&api_state, &fields, &summary).await;
        return conflict();
    }
    if !response.status().is_success() {
        return response;
    }
    let (parts, body) = response.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, usize::MAX).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let started = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| v.get("execution_id")?.as_str()?.parse::<ExecutionId>().ok());
    let Some(exec_id) = started else {
        // Fail closed: a start answer that names no run cannot be checked.
        tracing::error!(path = %path, "harvest: tenant start answer names no run");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match stored_tenant(&api_state, exec_id).await {
        Ok(Some(Some(owner))) if owner == tenant => Response::from_parts(parts, Body::from(bytes)),
        // The start has committed, and the engine has checked and stamped the
        // tenant. A `503` here makes a retry start a second run.
        Err(StoreUnavailable) => {
            tracing::warn!(path = %path, "harvest: tenant start owner check skipped");
            Response::from_parts(parts, Body::from(bytes))
        }
        Ok(owner) => {
            let summary = format!(
                "start resolved to a run of another tenant (tenant={tenant:?}, owner={:?})",
                owner.flatten()
            );
            audit_refusal(&api_state, &fields, &summary).await;
            conflict()
        }
    }
}

#[cfg(feature = "mcp")]
/// Refuse a generated MCP tool call by a tenant-bound caller (issue #1977).
///
/// The tool routes sit outside the management router, so the binding layer
/// does not cover them. A tool starts or reads a run with no tenant check.
pub(crate) async fn refuse_tenant_bound_mcp_tool(request: Request, next: Next) -> Response {
    if request.extensions().get::<VerifiedTenant>().is_some() {
        tracing::warn!(
            path = %request.uri().path(),
            "harvest: tenant-bound caller denied MCP tool (403)"
        );
        return forbidden();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A committed start keeps its answer when the owner check cannot reach
    /// the store (issue #1977). Otherwise a retry starts a second run.
    #[tokio::test]
    async fn a_committed_start_survives_an_unavailable_owner_check() {
        use tower::ServiceExt as _;
        let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
        let body = serde_json::json!({ "execution_id": exec_id.to_string() }).to_string();
        let app = axum::Router::new()
            .route(
                "/workflows/{name}/start",
                axum::routing::post(move || {
                    let body = body.clone();
                    async move { (StatusCode::CREATED, body) }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                HarvestApiState::new(),
                enforce_tenant_binding,
            ))
            .layer(axum::middleware::from_fn(
                |mut request: Request, next: Next| async move {
                    let tenant = VerifiedTenant::new("acme").expect("valid tenant");
                    request.extensions_mut().insert(tenant);
                    next.run(request).await
                },
            ));
        let request = Request::post("/workflows/wf/start")
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(value["execution_id"], exec_id.to_string());
    }

    #[test]
    fn tenant_scoped_routes_are_classified_routes() {
        for template in TENANT_SCOPED_ROUTES {
            assert!(
                autumn_harvest::audit::CLASSIFIED_ROUTES
                    .iter()
                    .any(|(r, _)| r == template),
                "{template} must be a classified route"
            );
        }
    }

    #[test]
    fn route_scope_matches_only_the_allowlist() {
        assert_eq!(
            route_scope(&Method::POST, "/workflows/wf/start"),
            Some(Scope::Start)
        );
        assert_eq!(route_scope(&Method::GET, "/workflows/x"), Some(Scope::Run));
        assert_eq!(route_scope(&Method::HEAD, "/workflows/x"), Some(Scope::Run));
        assert_eq!(
            route_scope(&Method::POST, "/workflows/x/signal/go"),
            Some(Scope::Run)
        );
        for (method, path) in [
            (Method::GET, "/workflows"),
            (Method::POST, "/admin/tokens"),
            (Method::GET, "/ui/workflows/x"),
            (Method::POST, "/workflows/wf/signal-with-start"),
            (Method::POST, "/workflows/x/reset"),
            (Method::POST, "/workflows/x/legal-hold/release"),
            (Method::GET, "/workflows/x/children"),
            (Method::DELETE, "/workflows/x"),
        ] {
            assert_eq!(route_scope(&method, path), None, "{method} {path}");
        }
    }

    #[test]
    fn verified_tenant_rejects_an_invalid_key() {
        assert!(VerifiedTenant::new("acme").is_ok());
        assert_eq!(VerifiedTenant::new(""), Err(InvalidTenant::Empty));
        assert_eq!(VerifiedTenant::new("a b"), Err(InvalidTenant::BadCharacter));
    }

    fn request_with(token: Option<&str>, embedder: Option<&str>) -> Request {
        let mut request = Request::new(Body::empty());
        if let Some(t) = token {
            request.extensions_mut().insert(TokenPrincipal {
                id: uuid::Uuid::nil(),
                scope: crate::api_token::TokenScope::Read,
                tenant: Some(t.to_string()),
            });
        }
        if let Some(e) = embedder {
            request
                .extensions_mut()
                .insert(VerifiedTenant::new(e).unwrap());
        }
        request
    }

    #[test]
    fn verified_tenant_resolution() {
        assert_eq!(verified_tenant(&request_with(None, None)), Ok(None));
        assert_eq!(
            verified_tenant(&request_with(Some("acme"), None)),
            Ok(Some("acme".to_string()))
        );
        assert_eq!(
            verified_tenant(&request_with(None, Some("acme"))),
            Ok(Some("acme".to_string()))
        );
        assert_eq!(
            verified_tenant(&request_with(Some("acme"), Some("acme"))),
            Ok(Some("acme".to_string()))
        );
        assert!(verified_tenant(&request_with(Some("acme"), Some("globex"))).is_err());
        assert!(verified_tenant(&request_with(Some("a b"), None)).is_err());
    }

    #[cfg(feature = "mcp")]
    #[tokio::test]
    async fn mcp_tool_gate_refuses_only_a_tenant_bound_caller() {
        use tower::ServiceExt as _;
        let app = axum::Router::new()
            .route("/tool", axum::routing::post(|| async { "ran" }))
            .layer(axum::middleware::from_fn(refuse_tenant_bound_mcp_tool));
        let call = |tenant: Option<&str>| {
            let mut request = Request::builder()
                .method("POST")
                .uri("/tool")
                .body(Body::empty())
                .unwrap();
            if let Some(t) = tenant {
                request
                    .extensions_mut()
                    .insert(VerifiedTenant::new(t).unwrap());
            }
            app.clone().oneshot(request)
        };
        assert_eq!(call(None).await.unwrap().status(), StatusCode::OK);
        assert_eq!(
            call(Some("acme")).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn declared_tenant_edge_cases() {
        let with = |values: &[&str]| {
            let mut request = Request::new(Body::empty());
            for v in values {
                request
                    .headers_mut()
                    .append(HEADER_TENANT, v.parse().unwrap());
            }
            declared_tenant(&request)
        };
        assert_eq!(with(&[]), Ok(None));
        assert_eq!(with(&[" acme "]), Ok(Some("acme".to_string())));
        assert_eq!(with(&["a", "b"]), Err(()));
        assert_eq!(with(&[" "]), Err(()));
        assert_eq!(with(&[&"t".repeat(129)]), Err(()));
    }
}
