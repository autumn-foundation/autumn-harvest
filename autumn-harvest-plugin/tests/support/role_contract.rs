//! The custom-role checks that both backends keep (issue #1978, #2469).
//!
//! `tests/custom_roles.rs` runs them on a Postgres-path mount. The SQLite
//! suite runs them on a SQLite app. Each harness builds its own app. These
//! checks send the requests and read the answers.

// Each test binary uses a different subset of the checks.
#![allow(dead_code)]

use autumn_harvest_plugin::api_token::TokenScope;
use autumn_harvest_plugin::roles::{
    HarvestRole, HarvestRoles, ROLE_DENIED_ERROR, ROLE_OPERATOR, ROLE_VIEWER,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Method, StatusCode};
use tower::ServiceExt;

/// The `Host` of each request. An app in `prod` refuses a request with none.
pub const HOST: &str = "harvest.test";

/// The mount path of the API in every role app.
pub const MOUNT: &str = "/api/harvest";

/// The roles of every role app: the built-in roles and one custom role.
pub fn roles() -> HarvestRoles {
    HarvestRoles::builder()
        .builtin_roles()
        .role(
            HarvestRole::new("dlq-operator")
                .with_scope(TokenScope::Read)
                .allow_route("POST /dead-letters/replay"),
        )
        .build()
        .expect("valid roles")
}

/// How host middleware hands roles to the stack.
#[derive(Clone)]
pub enum Source {
    /// No roles.
    None,
    /// The session key `harvest_roles`.
    Session(&'static str),
    /// A `RoleGrant` request extension.
    Grant(Vec<&'static str>),
    /// Both. The grant wins.
    Both {
        session: &'static str,
        grant: Vec<&'static str>,
    },
}

/// One backend that serves the role checks.
///
/// Each app runs in the `prod` profile. The API is under [`MOUNT`]. A
/// workflow named `wf` exists, or the start route answers for it with no
/// store.
pub trait RoleBackend {
    /// An app with [`roles`], whose host middleware gives `source`.
    fn app(&self, source: Source) -> axum::Router;
    /// An app with no roles and no auth boundary.
    fn bare(&self) -> axum::Router;
}

/// Send one request with a `{}` JSON body.
pub async fn call(
    app: axum::Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, String) {
    let mut builder = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("host", HOST)
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = app
        .oneshot(builder.body(Body::from("{}")).expect("request"))
        .await
        .expect("served");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Whether the role layer denied the request.
pub fn role_denied((status, body): &(StatusCode, String)) -> bool {
    *status == StatusCode::FORBIDDEN && body.contains(ROLE_DENIED_ERROR)
}

/// The error a Postgres-path handler gives when it runs with no store.
pub const NO_STORE: &str = "storage pool is not configured";

/// Whether the request reached its handler.
///
/// A gate answers `401` or `403`. A wrong route answers `404` or `405`. A
/// broken extractor answers `500`. None of those count. A handler that ran
/// and found no store answers `500` too, and names the store, so it counts.
/// A SQLite route that is not ported answers `501`, after every layer, so it
/// counts.
pub fn admitted((status, body): &(StatusCode, String)) -> bool {
    body.contains(NO_STORE)
        || !matches!(
            *status,
            StatusCode::FORBIDDEN
                | StatusCode::UNAUTHORIZED
                | StatusCode::NOT_FOUND
                | StatusCode::METHOD_NOT_ALLOWED
                | StatusCode::INTERNAL_SERVER_ERROR
        )
}

fn at(path: &str) -> String {
    format!("{MOUNT}{path}")
}

pub async fn viewer_reads_the_api_and_vantage(backend: &impl RoleBackend) {
    for path in ["/workflows", "/ui"] {
        let out = call(
            backend.app(Source::Session(ROLE_VIEWER)),
            Method::GET,
            &at(path),
            &[],
        )
        .await;
        assert!(admitted(&out), "{path}: {out:?}");
    }
}

pub async fn viewer_is_denied_mutations_with_a_role_403(backend: &impl RoleBackend) {
    for path in [
        "/workflows/wf/start",
        "/dead-letters/replay",
        "/ui/workflows/abc/cancel",
    ] {
        let out = call(
            backend.app(Source::Session(ROLE_VIEWER)),
            Method::POST,
            &at(path),
            &[],
        )
        .await;
        assert!(role_denied(&out), "{path}: {out:?}");
    }
}

pub async fn a_custom_role_reaches_only_its_extra_route(backend: &impl RoleBackend) {
    let replay = call(
        backend.app(Source::Session("dlq-operator")),
        Method::POST,
        &at("/dead-letters/replay"),
        &[],
    )
    .await;
    assert!(admitted(&replay), "{replay:?}");
    let discard = call(
        backend.app(Source::Session("dlq-operator")),
        Method::POST,
        &at("/dead-letters/discard"),
        &[],
    )
    .await;
    assert!(role_denied(&discard), "{discard:?}");
}

/// `GET /admin/tokens` carries `require_admin`. Only a credential admits it
/// in the `prod` profile. The role principal counts.
pub async fn a_role_admits_an_admin_gated_read(backend: &impl RoleBackend) {
    let out = call(
        backend.app(Source::Session(ROLE_VIEWER)),
        Method::GET,
        &at("/admin/tokens"),
        &[],
    )
    .await;
    assert!(admitted(&out), "{out:?}");
}

pub async fn an_operator_passes_the_mutation_gate(backend: &impl RoleBackend) {
    let out = call(
        backend.app(Source::Session(ROLE_OPERATOR)),
        Method::POST,
        &at("/workflows/wf/start"),
        &[],
    )
    .await;
    assert!(admitted(&out), "{out:?}");
}

pub async fn no_roles_and_unknown_roles_are_denied(backend: &impl RoleBackend) {
    for source in [
        Source::None,
        Source::Session(""),
        Source::Session("ghost"),
        Source::Grant(vec![]),
    ] {
        let out = call(backend.app(source), Method::GET, &at("/workflows"), &[]).await;
        assert!(role_denied(&out), "{out:?}");
    }
}

pub async fn public_routes_need_no_role(backend: &impl RoleBackend) {
    let out = call(backend.app(Source::None), Method::GET, &at("/health"), &[]).await;
    assert!(admitted(&out), "{out:?}");
}

pub async fn a_header_never_grants_a_role(backend: &impl RoleBackend) {
    let out = call(
        backend.app(Source::None),
        Method::GET,
        &at("/workflows"),
        &[
            ("x-harvest-roles", "harvest-admin"),
            ("x-harvest-actor", "oidc:admin"),
            ("cookie", "harvest_roles=harvest-admin"),
        ],
    )
    .await;
    assert!(role_denied(&out), "{out:?}");
}

pub async fn a_role_grant_extension_admits_and_wins_over_the_session(backend: &impl RoleBackend) {
    let granted = call(
        backend.app(Source::Grant(vec![ROLE_OPERATOR])),
        Method::POST,
        &at("/workflows/wf/start"),
        &[],
    )
    .await;
    assert!(admitted(&granted), "{granted:?}");
    // The grant names only a viewer. The session's operator role is ignored.
    let narrowed = call(
        backend.app(Source::Both {
            session: ROLE_OPERATOR,
            grant: vec![ROLE_VIEWER],
        }),
        Method::POST,
        &at("/workflows/wf/start"),
        &[],
    )
    .await;
    assert!(role_denied(&narrowed), "{narrowed:?}");
}

pub async fn options_preflight_passes_without_a_role(backend: &impl RoleBackend) {
    let out = call(
        backend.app(Source::None),
        Method::OPTIONS,
        &at("/workflows"),
        &[],
    )
    .await;
    assert!(!role_denied(&out), "{out:?}");
}

/// With no role layer, the admin gate answers `401`, as before #1978.
pub async fn a_mount_without_roles_is_unchanged(backend: &impl RoleBackend) {
    let out = call(backend.bare(), Method::GET, &at("/admin/tokens"), &[]).await;
    assert_eq!(out.0, StatusCode::UNAUTHORIZED, "{out:?}");
}
