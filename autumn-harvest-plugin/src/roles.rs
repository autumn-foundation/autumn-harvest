//! Custom roles for the management API and Vantage (issue #1978).
//!
//! A role has a name, an optional base scope and a list of extra routes. The
//! base scope is a [`TokenScope`]: `read`, `mutate` or `admin`. An extra route
//! is one entry of [`autumn_harvest::audit::CLASSIFIED_ROUTES`], such as
//! `POST /dead-letters/replay`. So a `dlq-operator` role can read every route
//! and replay the DLQ, and do nothing else.
//!
//! # Where roles come from
//!
//! The role layer reads the role names of a request from one of two places:
//!
//! 1. A [`RoleGrant`] request extension. Host middleware sets it, for example
//!    from a verified mTLS client certificate.
//! 2. Else, the autumn-web session key [`SESSION_ROLES_KEY`]. The OIDC login
//!    sets it. Host middleware can set it too. The value is a comma-separated
//!    list of role names.
//!
//! A client cannot set either source. A header never grants a role.
//!
//! # The decision
//!
//! - A `PublicSafe` route is always allowed.
//! - A role allows a route when its scope allows the route, or when the role
//!   names the route.
//! - A Vantage path (`/ui/...`) has no route class. A `GET` or `HEAD` there is
//!   a read. Every other method is a mutation. The #1802 gate uses the same
//!   rule.
//! - An unclassified API path is a mutation, so a `read` role cannot reach it.
//! - An unknown role name grants nothing.
//!
//! A verified `hvst_` token skips the role check. Its own scope applies.
//!
//! [`ClaimRoleMap`] maps OIDC token claims to role names.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::api::{
    HarvestApiState, RouteMatchers, acquire_conn, audit_context, build_route_matchers,
    classify_route, match_route, requires_admin_scope,
};
use crate::api_token::TokenScope;

/// The session key that holds the role names of a session principal.
///
/// The value is a comma-separated list, such as `harvest-viewer,dlq-operator`.
pub const SESSION_ROLES_KEY: &str = "harvest_roles";

/// The longest role name, in bytes.
pub const MAX_ROLE_NAME_LEN: usize = 64;

/// The built-in read role. Its scope is `read`.
pub const ROLE_VIEWER: &str = "harvest-viewer";

/// The built-in operator role. Its scope is `mutate`.
pub const ROLE_OPERATOR: &str = "harvest-operator";

/// The built-in admin role. Its scope is `admin`.
pub const ROLE_ADMIN: &str = "harvest-admin";

/// A configuration error in a role set or a claim map.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RoleConfigError {
    /// A role name is empty, too long, or has a character other than
    /// `a-z`, `0-9`, `-` and `_`.
    #[error("invalid role name {0:?}: use 1 to 64 of a-z, 0-9, '-' and '_'")]
    InvalidName(String),
    /// Two roles have the same name.
    #[error("duplicate role {0:?}")]
    DuplicateRole(String),
    /// A role has no scope and no route.
    #[error("role {0:?} grants nothing: give it a scope or a route")]
    EmptyRole(String),
    /// A route is not an entry of `CLASSIFIED_ROUTES`.
    #[error("role {role:?} names unknown route {route:?}: use a CLASSIFIED_ROUTES entry")]
    UnknownRoute {
        /// The role.
        role: String,
        /// The route as given.
        route: String,
    },
    /// A claim rule names a role that the role set does not define.
    #[error("claim rule maps to undefined role {0:?}")]
    UndefinedRole(String),
    /// A claim rule has an empty claim path or an empty value.
    #[error("claim rule for role {0:?} has an empty claim path or value")]
    EmptyClaimRule(String),
}

/// One custom role, before validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarvestRole {
    name: String,
    scope: Option<TokenScope>,
    routes: Vec<String>,
}

impl HarvestRole {
    /// A role with no scope and no route. Add at least one of them.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            scope: None,
            routes: Vec::new(),
        }
    }

    /// Set the base scope.
    #[must_use]
    pub const fn with_scope(mut self, scope: TokenScope) -> Self {
        self.scope = Some(scope);
        self
    }

    /// Allow one more route, written as a `CLASSIFIED_ROUTES` entry, such as
    /// `POST /dead-letters/replay`.
    #[must_use]
    pub fn allow_route(mut self, route: impl Into<String>) -> Self {
        self.routes.push(route.into());
        self
    }

    /// The role name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// One validated role.
#[derive(Clone, Debug)]
struct CompiledRole {
    scope: Option<TokenScope>,
    /// The `CLASSIFIED_ROUTES` entries this role names.
    routes: BTreeSet<&'static str>,
}

/// A validated set of roles.
///
/// Build it with [`HarvestRoles::builder`]. It is cheap to clone.
#[derive(Clone, Debug)]
pub struct HarvestRoles {
    roles: Arc<BTreeMap<String, CompiledRole>>,
}

/// Builds a [`HarvestRoles`].
#[derive(Clone, Debug, Default)]
pub struct HarvestRolesBuilder {
    roles: Vec<HarvestRole>,
}

impl HarvestRolesBuilder {
    /// Add the three built-in roles: [`ROLE_VIEWER`], [`ROLE_OPERATOR`] and
    /// [`ROLE_ADMIN`].
    #[must_use]
    pub fn builtin_roles(self) -> Self {
        self.role(HarvestRole::new(ROLE_VIEWER).with_scope(TokenScope::Read))
            .role(HarvestRole::new(ROLE_OPERATOR).with_scope(TokenScope::Mutate))
            .role(HarvestRole::new(ROLE_ADMIN).with_scope(TokenScope::Admin))
    }

    /// Add one role.
    #[must_use]
    pub fn role(mut self, role: HarvestRole) -> Self {
        self.roles.push(role);
        self
    }

    /// Validate the roles.
    ///
    /// # Errors
    ///
    /// Returns the first [`RoleConfigError`] found.
    pub fn build(self) -> Result<HarvestRoles, RoleConfigError> {
        let mut roles = BTreeMap::new();
        for role in self.roles {
            if !valid_role_name(&role.name) {
                return Err(RoleConfigError::InvalidName(role.name));
            }
            if role.scope.is_none() && role.routes.is_empty() {
                return Err(RoleConfigError::EmptyRole(role.name));
            }
            let mut routes = BTreeSet::new();
            for route in &role.routes {
                let Some((entry, _)) = CLASSIFIED_ROUTES.iter().find(|(r, _)| r == route) else {
                    return Err(RoleConfigError::UnknownRoute {
                        role: role.name.clone(),
                        route: route.clone(),
                    });
                };
                routes.insert(*entry);
            }
            let compiled = CompiledRole {
                scope: role.scope,
                routes,
            };
            if roles.insert(role.name.clone(), compiled).is_some() {
                return Err(RoleConfigError::DuplicateRole(role.name));
            }
        }
        Ok(HarvestRoles {
            roles: Arc::new(roles),
        })
    }
}

/// Whether `name` is 1 to 64 of `a-z`, `0-9`, `-` and `_`.
///
/// The session stores a comma-separated list, so a name has no comma, no
/// space and no capital letter.
fn valid_role_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_ROLE_NAME_LEN
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

impl CompiledRole {
    /// Whether this role allows `method` on `path`, given its route class.
    fn allows(&self, method: &Method, path: &str, class: RouteClass) -> bool {
        let by_scope = self.scope.is_some_and(|scope| match scope {
            TokenScope::Admin => true,
            TokenScope::Mutate => !requires_admin_scope(method, path),
            TokenScope::Read => class != RouteClass::Mutating,
        });
        by_scope
            || classified_template(method, path).is_some_and(|entry| self.routes.contains(entry))
    }

    /// Whether this role's scope allows a call by `method` with no route table.
    fn allows_by_method(&self, method: &Method) -> bool {
        let read = matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS);
        self.scope.is_some_and(|scope| match scope {
            TokenScope::Admin | TokenScope::Mutate => true,
            TokenScope::Read => read,
        })
    }
}

impl HarvestRoles {
    /// A builder with no roles.
    #[must_use]
    pub fn builder() -> HarvestRolesBuilder {
        HarvestRolesBuilder::default()
    }

    /// The three built-in roles only.
    #[must_use]
    pub fn builtin() -> Self {
        let built = HarvestRolesBuilder::default().builtin_roles().build();
        debug_assert!(built.is_ok(), "the built-in roles are valid");
        built.unwrap_or_else(|_| Self {
            roles: Arc::new(BTreeMap::new()),
        })
    }

    /// Whether the set defines a role with this name.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.roles.contains_key(name)
    }

    /// The role names, sorted.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.roles.keys().map(String::as_str)
    }

    /// Whether any of `roles` allows `method` on `path`.
    ///
    /// `path` is relative to the management API mount. Unknown role names
    /// grant nothing.
    #[must_use]
    pub fn allows<'a>(
        &self,
        roles: impl IntoIterator<Item = &'a str>,
        method: &Method,
        path: &str,
    ) -> bool {
        let class = role_route_class(method, path);
        if class == RouteClass::PublicSafe {
            return true;
        }
        roles.into_iter().any(|name| {
            self.roles
                .get(name)
                .is_some_and(|role| role.allows(method, path, class))
        })
    }

    /// Whether any of `roles` has a scope that allows a call by `method` to a
    /// route with no route table, such as a generated MCP tool route.
    ///
    /// A `GET` or `HEAD` needs `read`. Any other method needs `mutate`. Extra
    /// routes do not apply.
    #[must_use]
    pub fn allows_by_method<'a>(
        &self,
        roles: impl IntoIterator<Item = &'a str>,
        method: &Method,
    ) -> bool {
        roles.into_iter().any(|name| {
            self.roles
                .get(name)
                .is_some_and(|role| role.allows_by_method(method))
        })
    }
}

/// The route class the role check uses.
///
/// A Vantage path is a read for `GET`, `HEAD` and `OPTIONS`, and a mutation
/// otherwise. Every other path uses [`classify_route`], which fails closed.
fn role_route_class(method: &Method, path: &str) -> RouteClass {
    if is_vantage_path(path) {
        return if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
            RouteClass::ReadOnly
        } else {
            RouteClass::Mutating
        };
    }
    classify_route(method, path)
}

/// Whether `path` is under the Vantage mount (`/ui`).
fn is_vantage_path(path: &str) -> bool {
    path == "/ui" || path.starts_with("/ui/")
}

/// The `CLASSIFIED_ROUTES` entry that `method` and `path` match, if any.
///
/// The match uses the whole table, so a static route beats a parameter route.
fn classified_template(method: &Method, path: &str) -> Option<&'static str> {
    static MATCHERS: OnceLock<RouteMatchers<&'static str>> = OnceLock::new();
    let matchers = MATCHERS.get_or_init(|| {
        build_route_matchers(
            "CLASSIFIED_ROUTES",
            CLASSIFIED_ROUTES.iter().map(|(route, _)| (*route, *route)),
        )
    });
    match_route(matchers, method, path).map(|m| *m.value)
}

/// Role names granted by host middleware.
///
/// Insert it as a request extension before the Harvest router runs. It takes
/// precedence over the session. A client cannot set a request extension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleGrant {
    roles: Vec<String>,
}

impl RoleGrant {
    /// A grant of `roles`.
    #[must_use]
    pub fn new<I, S>(roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            roles: roles.into_iter().map(Into::into).collect(),
        }
    }

    /// The granted role names.
    #[must_use]
    pub fn roles(&self) -> &[String] {
        &self.roles
    }
}

/// The roles that admitted a request.
///
/// The role layer sets it as a request extension when it allows a request.
/// The admin gate and the #1802 gate admit a request that carries it. An
/// authorizer hook can read it from `AuthzRequest::extensions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RolePrincipal {
    roles: Vec<String>,
}

impl RolePrincipal {
    /// A principal with `roles`.
    pub(crate) const fn new(roles: Vec<String>) -> Self {
        Self { roles }
    }

    /// The role names of the caller.
    #[must_use]
    pub fn roles(&self) -> &[String] {
        &self.roles
    }
}

/// Split a session value into role names.
///
/// Each name is trimmed. Empty names are dropped.
#[must_use]
pub fn parse_role_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

/// Join role names into the session value [`parse_role_list`] reads.
#[must_use]
pub fn join_role_list(roles: &[String]) -> String {
    roles.join(",")
}

/// The role names of a request: the [`RoleGrant`], else the session value.
///
/// It takes the extensions apart first. A `&Request` is not `Send`, so the
/// future must not hold one across an `.await`.
async fn request_roles(grant: Option<RoleGrant>, session: Option<Session>) -> Vec<String> {
    if let Some(grant) = grant {
        return grant.roles;
    }
    let Some(session) = session else {
        return Vec::new();
    };
    session
        .get(SESSION_ROLES_KEY)
        .await
        .map(|v| parse_role_list(&v))
        .unwrap_or_default()
}

/// The `403` body of a role deny.
pub const ROLE_DENIED_ERROR: &str = "role does not allow this route";

fn role_forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({ "error": ROLE_DENIED_ERROR })),
    )
        .into_response()
}

/// The custom-role layer (issue #1978).
///
/// It runs after the token layer and the read-only layer, and before the
/// authorizer hook. A deny gives `403` and writes one `authz.deny` row.
pub(crate) async fn enforce_custom_roles(
    State((api_state, roles)): State<(HarvestApiState, HarvestRoles)>,
    mut request: Request,
    next: Next,
) -> Response {
    // OPTIONS is a preflight verb. It carries no credential and changes nothing.
    if *request.method() == Method::OPTIONS
        || request
            .extensions()
            .get::<crate::api_token::TokenPrincipal>()
            .is_some()
    {
        return next.run(request).await;
    }
    let grant = request.extensions().get::<RoleGrant>().cloned();
    let session = request.extensions().get::<Session>().cloned();
    let names = request_roles(grant, session).await;
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    if roles.allows(names.iter().map(String::as_str), &method, &path) {
        request.extensions_mut().insert(RolePrincipal::new(names));
        return next.run(request).await;
    }
    tracing::warn!(
        method = %method,
        path = %path,
        roles = %join_role_list(&names),
        "harvest: role denied route (403)"
    );
    let (actor, source, request_id) = audit_context(request.headers(), &api_state);
    let summary = format!("roles {:?} do not allow this route", join_role_list(&names));
    if let Ok(pool) = api_state.storage_pool()
        && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
    {
        crate::authz::audit_deny(
            &mut conn,
            &crate::authz::DenyAudit {
                actor: &actor,
                method: &method,
                path: &path,
                request_id: request_id.as_deref(),
                source: &source,
                shard: None,
                summary: &summary,
            },
        )
        .await;
    }
    role_forbidden()
}

/// The role gate on a generated MCP tool route (issue #1978).
///
/// A tool route has no route class. A `GET` tool needs a role with a `read`
/// scope or wider. Any other tool needs `mutate` or wider. Host auth runs
/// outside this gate and sets the roles.
#[cfg(feature = "mcp")]
pub(crate) async fn enforce_mcp_tool_roles(
    State(roles): State<HarvestRoles>,
    mut request: Request,
    next: Next,
) -> Response {
    if *request.method() == Method::OPTIONS {
        return next.run(request).await;
    }
    let grant = request.extensions().get::<RoleGrant>().cloned();
    let session = request.extensions().get::<Session>().cloned();
    let names = request_roles(grant, session).await;
    if !roles.allows_by_method(names.iter().map(String::as_str), request.method()) {
        return role_forbidden();
    }
    request.extensions_mut().insert(RolePrincipal::new(names));
    next.run(request).await
}

// ── Claim-to-role map ─────────────────────────────────────────────────────────

/// One claim rule: when a claim holds a value, grant a role.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimRule {
    claim: String,
    value: String,
    role: String,
}

impl ClaimRule {
    /// Grant `role` when the claim at `claim` holds `value`.
    ///
    /// `claim` is a dot-separated path, such as `groups` or
    /// `realm_access.roles`. A string claim matches when it equals `value`.
    /// An array claim matches when one of its strings equals `value`.
    #[must_use]
    pub fn new(
        claim: impl Into<String>,
        value: impl Into<String>,
        role: impl Into<String>,
    ) -> Self {
        Self {
            claim: claim.into(),
            value: value.into(),
            role: role.into(),
        }
    }

    /// Whether `claims` match this rule.
    #[must_use]
    pub fn matches(&self, claims: &serde_json::Value) -> bool {
        let mut node = claims;
        for key in self.claim.split('.') {
            match node.get(key) {
                Some(next) => node = next,
                None => return false,
            }
        }
        match node {
            serde_json::Value::String(value) => *value == self.value,
            serde_json::Value::Array(items) => items
                .iter()
                .any(|item| item.as_str() == Some(self.value.as_str())),
            _ => false,
        }
    }

    /// The role this rule grants.
    #[must_use]
    pub fn role(&self) -> &str {
        &self.role
    }
}

/// Maps the claims of an identity to Harvest role names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClaimRoleMap {
    rules: Vec<ClaimRule>,
    default_role: Option<String>,
}

impl ClaimRoleMap {
    /// A map with no rules. It maps every identity to no role.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one rule.
    #[must_use]
    pub fn rule(mut self, rule: ClaimRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Grant `role` to every identity that logs in.
    ///
    /// Without it, an identity that matches no rule cannot log in.
    #[must_use]
    pub fn default_role(mut self, role: impl Into<String>) -> Self {
        self.default_role = Some(role.into());
        self
    }

    /// Check every rule against `roles`.
    ///
    /// # Errors
    ///
    /// Returns [`RoleConfigError::UndefinedRole`] for a rule or a default role
    /// that `roles` does not define, and [`RoleConfigError::EmptyClaimRule`]
    /// for a rule with an empty claim path or value.
    pub fn validate(&self, roles: &HarvestRoles) -> Result<(), RoleConfigError> {
        for rule in &self.rules {
            if rule.claim.split('.').any(str::is_empty) || rule.value.is_empty() {
                return Err(RoleConfigError::EmptyClaimRule(rule.role.clone()));
            }
            if !roles.contains(&rule.role) {
                return Err(RoleConfigError::UndefinedRole(rule.role.clone()));
            }
        }
        if let Some(role) = &self.default_role
            && !roles.contains(role)
        {
            return Err(RoleConfigError::UndefinedRole(role.clone()));
        }
        Ok(())
    }

    /// The role names that `claims` map to, sorted and without duplicates.
    #[must_use]
    pub fn roles_for(&self, claims: &serde_json::Value) -> Vec<String> {
        let mut roles: BTreeSet<&str> = self
            .rules
            .iter()
            .filter(|rule| rule.matches(claims))
            .map(|rule| rule.role.as_str())
            .collect();
        if let Some(role) = &self.default_role {
            roles.insert(role);
        }
        roles.into_iter().map(str::to_string).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;
    use tower::ServiceExt;

    fn roles() -> HarvestRoles {
        HarvestRoles::builder()
            .builtin_roles()
            .role(
                HarvestRole::new("dlq-operator")
                    .with_scope(TokenScope::Read)
                    .allow_route("POST /dead-letters/replay"),
            )
            .role(HarvestRole::new("starter").allow_route("POST /workflows/{workflow_name}/start"))
            .build()
            .expect("valid roles")
    }

    #[test]
    fn builtin_roles_are_defined() {
        let set = HarvestRoles::builtin();
        for name in [ROLE_VIEWER, ROLE_OPERATOR, ROLE_ADMIN] {
            assert!(set.contains(name), "{name}");
        }
        assert_eq!(set.names().count(), 3);
    }

    #[test]
    fn public_safe_routes_need_no_role() {
        let set = roles();
        assert!(set.allows([], &Method::GET, "/health"));
        assert!(set.allows(["unknown"], &Method::GET, "/openapi.json"));
    }

    #[test]
    fn viewer_reads_but_does_not_mutate() {
        let set = roles();
        assert!(set.allows([ROLE_VIEWER], &Method::GET, "/workflows"));
        assert!(set.allows([ROLE_VIEWER], &Method::HEAD, "/workflows"));
        assert!(!set.allows([ROLE_VIEWER], &Method::POST, "/workflows/wf/start"));
        assert!(!set.allows([ROLE_VIEWER], &Method::POST, "/dead-letters/replay"));
    }

    #[test]
    fn operator_mutates_but_does_not_mint_tokens() {
        let set = roles();
        assert!(set.allows([ROLE_OPERATOR], &Method::POST, "/workflows/wf/start"));
        assert!(!set.allows([ROLE_OPERATOR], &Method::POST, "/admin/tokens"));
        assert!(set.allows([ROLE_ADMIN], &Method::POST, "/admin/tokens"));
    }

    #[test]
    fn extra_routes_widen_only_the_named_route() {
        let set = roles();
        assert!(set.allows(["dlq-operator"], &Method::POST, "/dead-letters/replay"));
        assert!(set.allows(["dlq-operator"], &Method::GET, "/workflows"));
        assert!(!set.allows(["dlq-operator"], &Method::POST, "/dead-letters/discard"));
        assert!(!set.allows(["dlq-operator"], &Method::POST, "/workflows/wf/start"));
    }

    #[test]
    fn a_role_with_routes_only_reads_nothing_else() {
        let set = roles();
        assert!(set.allows(["starter"], &Method::POST, "/workflows/wf/start"));
        assert!(!set.allows(["starter"], &Method::GET, "/workflows"));
    }

    #[test]
    fn a_static_route_beats_a_granted_parameter_route() {
        let set = HarvestRoles::builder()
            .role(HarvestRole::new("one").allow_route("GET /workflows/{id}"))
            .build()
            .expect("valid");
        assert!(set.allows(["one"], &Method::GET, "/workflows/abc"));
        // `GET /workflows/count` is its own route, so the grant of
        // `GET /workflows/{id}` does not cover it.
        assert!(classified_template(&Method::GET, "/workflows/count").is_some());
        assert_ne!(
            classified_template(&Method::GET, "/workflows/count"),
            Some("GET /workflows/{id}")
        );
        assert!(!set.allows(["one"], &Method::GET, "/workflows/count"));
    }

    #[test]
    fn unclassified_paths_fail_closed() {
        let set = roles();
        assert!(!set.allows([ROLE_VIEWER], &Method::GET, "/no/such/route"));
        assert!(set.allows([ROLE_OPERATOR], &Method::GET, "/no/such/route"));
    }

    #[test]
    fn vantage_reads_and_posts_follow_the_method() {
        let set = roles();
        assert!(set.allows([ROLE_VIEWER], &Method::GET, "/ui"));
        assert!(set.allows([ROLE_VIEWER], &Method::GET, "/ui/workflows/abc"));
        assert!(!set.allows([ROLE_VIEWER], &Method::POST, "/ui/workflows/abc/cancel"));
        assert!(set.allows([ROLE_OPERATOR], &Method::POST, "/ui/workflows/abc/cancel"));
        assert!(!set.allows(["starter"], &Method::GET, "/ui"));
        assert!(!set.allows([ROLE_VIEWER], &Method::GET, "/uix"));
    }

    #[test]
    fn unknown_and_empty_role_lists_grant_nothing() {
        let set = roles();
        assert!(!set.allows([], &Method::GET, "/workflows"));
        assert!(!set.allows(["nobody"], &Method::GET, "/workflows"));
        assert!(!set.allows(["Harvest-Viewer"], &Method::GET, "/workflows"));
    }

    #[test]
    fn allows_by_method_uses_the_scope_only() {
        let set = roles();
        assert!(set.allows_by_method([ROLE_VIEWER], &Method::GET));
        assert!(!set.allows_by_method([ROLE_VIEWER], &Method::POST));
        assert!(set.allows_by_method([ROLE_OPERATOR], &Method::POST));
        assert!(!set.allows_by_method(["starter"], &Method::GET));
        assert!(!set.allows_by_method(["starter"], &Method::POST));
        assert!(!set.allows_by_method([], &Method::GET));
    }

    #[test]
    fn builder_refuses_bad_names() {
        for name in [
            "",
            "Admin",
            "a b",
            "a,b",
            &"x".repeat(MAX_ROLE_NAME_LEN + 1),
        ] {
            let err = HarvestRoles::builder()
                .role(HarvestRole::new(name).with_scope(TokenScope::Read))
                .build()
                .expect_err(name);
            assert_eq!(err, RoleConfigError::InvalidName(name.to_string()));
        }
        assert!(
            HarvestRoles::builder()
                .role(HarvestRole::new("x".repeat(MAX_ROLE_NAME_LEN)).with_scope(TokenScope::Read))
                .build()
                .is_ok()
        );
    }

    #[test]
    fn builder_refuses_duplicates_empty_roles_and_unknown_routes() {
        assert_eq!(
            HarvestRoles::builder()
                .builtin_roles()
                .role(HarvestRole::new(ROLE_VIEWER).with_scope(TokenScope::Read))
                .build()
                .expect_err("duplicate"),
            RoleConfigError::DuplicateRole(ROLE_VIEWER.to_string())
        );
        assert_eq!(
            HarvestRoles::builder()
                .role(HarvestRole::new("empty"))
                .build()
                .expect_err("empty"),
            RoleConfigError::EmptyRole("empty".to_string())
        );
        assert_eq!(
            HarvestRoles::builder()
                .role(HarvestRole::new("typo").allow_route("POST /dead-letter/replay"))
                .build()
                .expect_err("typo"),
            RoleConfigError::UnknownRoute {
                role: "typo".to_string(),
                route: "POST /dead-letter/replay".to_string(),
            }
        );
    }

    #[test]
    fn role_lists_round_trip_through_the_session_value() {
        assert_eq!(
            parse_role_list(" harvest-viewer , ,dlq-operator,"),
            vec!["harvest-viewer".to_string(), "dlq-operator".to_string()]
        );
        assert_eq!(parse_role_list(""), Vec::<String>::new());
        let roles = vec!["a".to_string(), "b".to_string()];
        assert_eq!(parse_role_list(&join_role_list(&roles)), roles);
    }

    #[test]
    fn claim_rules_match_strings_arrays_and_nested_paths() {
        let claims = json!({
            "email": "ops@example.com",
            "groups": ["eng", "harvest-admins"],
            "realm_access": { "roles": ["dlq"] },
            "level": 3,
        });
        assert!(ClaimRule::new("groups", "harvest-admins", ROLE_ADMIN).matches(&claims));
        assert!(!ClaimRule::new("groups", "harvest", ROLE_ADMIN).matches(&claims));
        assert!(ClaimRule::new("email", "ops@example.com", ROLE_VIEWER).matches(&claims));
        assert!(ClaimRule::new("realm_access.roles", "dlq", "dlq-operator").matches(&claims));
        assert!(!ClaimRule::new("realm_access", "dlq", "dlq-operator").matches(&claims));
        assert!(!ClaimRule::new("level", "3", ROLE_VIEWER).matches(&claims));
        assert!(!ClaimRule::new("missing.path", "x", ROLE_VIEWER).matches(&claims));
    }

    #[test]
    fn claim_map_returns_sorted_unique_roles_and_the_default() {
        let map = ClaimRoleMap::new()
            .rule(ClaimRule::new("groups", "eng", ROLE_VIEWER))
            .rule(ClaimRule::new("groups", "harvest-admins", ROLE_ADMIN))
            .rule(ClaimRule::new("groups", "eng", ROLE_VIEWER));
        let claims = json!({ "groups": ["eng", "harvest-admins"] });
        assert_eq!(
            map.roles_for(&claims),
            vec![ROLE_ADMIN.to_string(), ROLE_VIEWER.to_string()]
        );
        assert_eq!(
            map.roles_for(&json!({ "groups": [] })),
            Vec::<String>::new()
        );
        let with_default = map.default_role(ROLE_VIEWER);
        assert_eq!(
            with_default.roles_for(&json!({})),
            vec![ROLE_VIEWER.to_string()]
        );
    }

    #[test]
    fn claim_map_validation_names_the_bad_rule() {
        let set = roles();
        assert_eq!(
            ClaimRoleMap::new()
                .rule(ClaimRule::new("groups", "x", "ghost"))
                .validate(&set),
            Err(RoleConfigError::UndefinedRole("ghost".to_string()))
        );
        assert_eq!(
            ClaimRoleMap::new().default_role("ghost").validate(&set),
            Err(RoleConfigError::UndefinedRole("ghost".to_string()))
        );
        assert_eq!(
            ClaimRoleMap::new()
                .rule(ClaimRule::new("", "x", ROLE_VIEWER))
                .validate(&set),
            Err(RoleConfigError::EmptyClaimRule(ROLE_VIEWER.to_string()))
        );
        assert!(
            ClaimRoleMap::new()
                .rule(ClaimRule::new("groups", "x", "dlq-operator"))
                .validate(&set)
                .is_ok()
        );
    }

    /// The role layer over a route that answers `200`, with `outer` outside it.
    fn layered(outer: impl Fn(&mut Request) + Clone + Send + Sync + 'static) -> axum::Router {
        axum::Router::new()
            .route(
                "/workflows/{name}/start",
                axum::routing::post(|| async { "started" }),
            )
            .layer(axum::middleware::from_fn_with_state(
                (HarvestApiState::new(), roles()),
                enforce_custom_roles,
            ))
            .layer(axum::middleware::from_fn(
                move |mut request: Request, next: Next| {
                    let outer = outer.clone();
                    async move {
                        outer(&mut request);
                        next.run(request).await
                    }
                },
            ))
    }

    async fn post_start(app: axum::Router) -> StatusCode {
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/workflows/wf/start")
            .body(axum::body::Body::empty())
            .expect("request");
        app.oneshot(request).await.expect("served").status()
    }

    #[tokio::test]
    async fn a_verified_token_skips_the_role_check() {
        let token = |request: &mut Request| {
            request
                .extensions_mut()
                .insert(crate::api_token::TokenPrincipal {
                    id: uuid::Uuid::nil(),
                    scope: TokenScope::Mutate,
                });
        };
        assert_eq!(post_start(layered(token)).await, StatusCode::OK);
        // Without the token, the same caller has no role.
        assert_eq!(post_start(layered(|_| {})).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn an_allowed_request_carries_its_role_principal() {
        let app = axum::Router::new()
            .route(
                "/workflows/{name}/start",
                axum::routing::post(|request: Request| async move {
                    request
                        .extensions()
                        .get::<RolePrincipal>()
                        .map(|p| join_role_list(p.roles()))
                        .unwrap_or_default()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                (HarvestApiState::new(), roles()),
                enforce_custom_roles,
            ))
            .layer(axum::middleware::from_fn(
                |mut request: Request, next: Next| async move {
                    request
                        .extensions_mut()
                        .insert(RoleGrant::new([ROLE_OPERATOR]));
                    next.run(request).await
                },
            ));
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/workflows/wf/start")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("served");
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .expect("body");
        assert_eq!(&body[..], ROLE_OPERATOR.as_bytes());
    }

    fn arb_method() -> impl Strategy<Value = Method> {
        prop_oneof![
            Just(Method::GET),
            Just(Method::HEAD),
            Just(Method::POST),
            Just(Method::PUT),
            Just(Method::PATCH),
            Just(Method::DELETE),
        ]
    }

    fn arb_path() -> impl Strategy<Value = String> {
        let classified: Vec<String> = CLASSIFIED_ROUTES
            .iter()
            .map(|(route, _)| {
                route
                    .split_once(' ')
                    .map_or("/", |(_, p)| p)
                    .replace("{id}", "abc")
                    .replace('{', "x")
                    .replace('}', "")
            })
            .collect();
        prop_oneof![
            proptest::sample::select(classified),
            "/ui(/[a-z]{1,8}){0,3}",
            "/[a-z]{1,8}(/[a-z]{1,8}){0,3}",
        ]
    }

    proptest! {
        /// A role never allows more than its scope or its named routes.
        #[test]
        fn a_role_allows_only_its_scope_and_routes(method in arb_method(), path in arb_path()) {
            let set = roles();
            let class = role_route_class(&method, &path);
            let named = classified_template(&method, &path) == Some("POST /dead-letters/replay");
            let public = class == RouteClass::PublicSafe;
            let read = class != RouteClass::Mutating;
            prop_assert_eq!(set.allows(["dlq-operator"], &method, &path), public || read || named);
            prop_assert_eq!(set.allows([ROLE_VIEWER], &method, &path), public || read);
            prop_assert_eq!(
                set.allows([ROLE_OPERATOR], &method, &path),
                !requires_admin_scope(&method, &path)
            );
            prop_assert!(set.allows([ROLE_ADMIN], &method, &path));
            prop_assert_eq!(set.allows(["nobody"], &method, &path), public);
            // A union of roles allows what each one allows.
            prop_assert_eq!(
                set.allows(["starter", ROLE_VIEWER], &method, &path),
                set.allows(["starter"], &method, &path) || set.allows([ROLE_VIEWER], &method, &path)
            );
        }
    }
}
