//! Custom roles on a standalone mount (issue #1978).
//!
//! These tests drive `StandaloneAdminAuth::with_roles` over the real API and
//! Vantage routers. No database is needed. An allowed request reaches its
//! handler, which may answer `503` without a store. A denied request gets
//! `403` from the role layer before any handler runs.

use std::collections::HashMap;

use autumn_harvest_plugin::api::{HarvestApiState, StandaloneAdminAuth, harvest_api_router};
use autumn_harvest_plugin::api_token::TokenScope;
use autumn_harvest_plugin::harvest_ui_router;
use autumn_harvest_plugin::roles::{
    HarvestRole, HarvestRoles, ROLE_DENIED_ERROR, ROLE_OPERATOR, ROLE_VIEWER, RoleGrant,
    SESSION_ROLES_KEY,
};
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use tower::ServiceExt;

fn roles() -> HarvestRoles {
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

/// How the test middleware hands roles to the stack.
#[derive(Clone)]
enum Source {
    None,
    Session(&'static str),
    Grant(Vec<&'static str>),
    Both {
        session: &'static str,
        grant: Vec<&'static str>,
    },
}

/// The API and Vantage routers behind the role layer, in the `prod` profile
/// with no auth boundary. Host middleware outside the stack sets the roles.
fn app(source: Source) -> axum::Router {
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    let auth = StandaloneAdminAuth::new()
        .with_deployment_profile("prod")
        .with_roles(roles());
    let mounted = auth
        .mount(router, &api_state)
        .layer(axum::middleware::from_fn(
            move |mut request: Request, next: Next| {
                let source = source.clone();
                async move {
                    let (session, grant) = match source {
                        Source::None => (None, None),
                        Source::Session(s) => (Some(s), None),
                        Source::Grant(g) => (None, Some(g)),
                        Source::Both { session, grant } => (Some(session), Some(grant)),
                    };
                    if let Some(value) = session {
                        let mut data = HashMap::new();
                        data.insert(SESSION_ROLES_KEY.to_string(), value.to_string());
                        request
                            .extensions_mut()
                            .insert(Session::new_for_test("roles".to_string(), data));
                    }
                    if let Some(grant) = grant {
                        request.extensions_mut().insert(RoleGrant::new(grant));
                    }
                    next.run(request).await
                }
            },
        ));
    axum::Router::new().nest("/api/harvest", mounted)
}

async fn call(
    app: axum::Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, String) {
    let mut builder = axum::http::Request::builder()
        .method(method)
        .uri(uri)
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

fn role_denied((status, body): &(StatusCode, String)) -> bool {
    *status == StatusCode::FORBIDDEN && body.contains(ROLE_DENIED_ERROR)
}

fn admitted((status, _): &(StatusCode, String)) -> bool {
    *status != StatusCode::FORBIDDEN && *status != StatusCode::UNAUTHORIZED
}

#[tokio::test]
async fn viewer_reads_the_api_and_vantage() {
    for uri in ["/api/harvest/workflows", "/api/harvest/ui"] {
        let out = call(app(Source::Session(ROLE_VIEWER)), Method::GET, uri, &[]).await;
        assert!(admitted(&out), "{uri}: {out:?}");
    }
}

#[tokio::test]
async fn viewer_is_denied_mutations_with_a_role_403() {
    for uri in [
        "/api/harvest/workflows/wf/start",
        "/api/harvest/dead-letters/replay",
        "/api/harvest/ui/workflows/abc/cancel",
    ] {
        let out = call(app(Source::Session(ROLE_VIEWER)), Method::POST, uri, &[]).await;
        assert!(role_denied(&out), "{uri}: {out:?}");
    }
}

#[tokio::test]
async fn a_custom_role_reaches_only_its_extra_route() {
    let replay = call(
        app(Source::Session("dlq-operator")),
        Method::POST,
        "/api/harvest/dead-letters/replay",
        &[],
    )
    .await;
    assert!(admitted(&replay), "{replay:?}");
    let discard = call(
        app(Source::Session("dlq-operator")),
        Method::POST,
        "/api/harvest/dead-letters/discard",
        &[],
    )
    .await;
    assert!(role_denied(&discard), "{discard:?}");
}

#[tokio::test]
async fn a_role_admits_an_admin_gated_read_without_a_boundary() {
    // `GET /admin/tokens` carries `require_admin`. With no boundary in the
    // `prod` profile, only a credential admits it. The role principal counts.
    let out = call(
        app(Source::Session(ROLE_VIEWER)),
        Method::GET,
        "/api/harvest/admin/tokens",
        &[],
    )
    .await;
    assert!(admitted(&out), "{out:?}");
}

#[tokio::test]
async fn an_operator_passes_the_mutation_gate_without_a_boundary() {
    let out = call(
        app(Source::Session(ROLE_OPERATOR)),
        Method::POST,
        "/api/harvest/workflows/wf/start",
        &[],
    )
    .await;
    assert!(admitted(&out), "{out:?}");
}

#[tokio::test]
async fn no_roles_and_unknown_roles_are_denied() {
    for source in [
        Source::None,
        Source::Session(""),
        Source::Session("ghost"),
        Source::Grant(vec![]),
    ] {
        let out = call(app(source), Method::GET, "/api/harvest/workflows", &[]).await;
        assert!(role_denied(&out), "{out:?}");
    }
}

#[tokio::test]
async fn public_routes_need_no_role() {
    let out = call(app(Source::None), Method::GET, "/api/harvest/health", &[]).await;
    assert!(admitted(&out), "{out:?}");
}

#[tokio::test]
async fn a_header_never_grants_a_role() {
    let out = call(
        app(Source::None),
        Method::GET,
        "/api/harvest/workflows",
        &[
            ("x-harvest-roles", "harvest-admin"),
            ("x-harvest-actor", "oidc:admin"),
            ("cookie", "harvest_roles=harvest-admin"),
        ],
    )
    .await;
    assert!(role_denied(&out), "{out:?}");
}

#[tokio::test]
async fn a_role_grant_extension_admits_and_wins_over_the_session() {
    let granted = call(
        app(Source::Grant(vec![ROLE_OPERATOR])),
        Method::POST,
        "/api/harvest/workflows/wf/start",
        &[],
    )
    .await;
    assert!(admitted(&granted), "{granted:?}");
    // The grant names only a viewer. The session's operator role is ignored.
    let narrowed = call(
        app(Source::Both {
            session: ROLE_OPERATOR,
            grant: vec![ROLE_VIEWER],
        }),
        Method::POST,
        "/api/harvest/workflows/wf/start",
        &[],
    )
    .await;
    assert!(role_denied(&narrowed), "{narrowed:?}");
}

#[tokio::test]
async fn options_preflight_passes_without_a_role() {
    let out = call(
        app(Source::None),
        Method::OPTIONS,
        "/api/harvest/workflows",
        &[],
    )
    .await;
    assert!(!role_denied(&out), "{out:?}");
}

#[tokio::test]
async fn a_mount_without_roles_is_unchanged() {
    let api_state = HarvestApiState::new();
    let router = harvest_api_router(api_state.clone());
    let mounted = StandaloneAdminAuth::new()
        .with_deployment_profile("prod")
        .mount(router, &api_state);
    let app = axum::Router::new().nest("/api/harvest", mounted);
    // No role layer: the admin gate answers 401, as before issue #1978.
    let out = call(app, Method::GET, "/api/harvest/admin/tokens", &[]).await;
    assert_eq!(out.0, StatusCode::UNAUTHORIZED, "{out:?}");
}
