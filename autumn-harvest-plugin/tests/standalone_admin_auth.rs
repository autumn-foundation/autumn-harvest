//! A standalone mount can reach a working admin credential (issue #1608).
//!
//! `harvest_api_router` gates its admin routes with `require_harvest_admin`.
//! That guard admits a request on one of three grounds. The first is a
//! verified `TokenPrincipal` set by `enforce_token_scope` (issue #942). The
//! second is a declared embedder auth boundary. The third is an autumn-web
//! `Session`. The first two were installed only by `HarvestPlugin`, and the
//! third needs autumn-web. A standalone mount could reach none of them.
//!
//! These tests drive the exported `StandaloneAdminAuth` mount, not a
//! hand-assembled layer stack, because the layer ordering is the part an
//! embedder gets wrong.
//!
//! No database is required. The token layer refuses to pass a claimed `hvst_`
//! bearer that it cannot verify, so the layer is observable without a token
//! store. `token_auth_integration.rs` carries the database-backed proof that a
//! minted token reaches an admin route.

use std::collections::HashMap;

use autumn_harvest_plugin::api::{HarvestApiState, StandaloneAdminAuth, harvest_api_router};
use autumn_harvest_plugin::harvest_ui_router;
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use tower::ServiceExt;

/// The standalone composition under test: the API router with Vantage nested
/// inside it, wrapped in the declared admin-auth stack, on a bare Axum router.
///
/// Vantage is nested before the stack is applied, which is how `HarvestPlugin`
/// composes it, so the layers cover both routers.
fn standalone_app(auth: &StandaloneAdminAuth) -> axum::Router {
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    axum::Router::new().nest("/api/harvest", auth.mount(router, &api_state))
}

/// The same composition with an embedder auth middleware outside the stack,
/// tagging every request with a session `role`. This is the production order:
/// embedder auth sets the `Session`, then the stack reads it.
fn standalone_app_with_role(auth: &StandaloneAdminAuth, role: &'static str) -> axum::Router {
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    let mounted = auth
        .mount(router, &api_state)
        .layer(axum::middleware::from_fn(
            move |mut request: Request, next: Next| async move {
                let mut claims = HashMap::new();
                claims.insert("role".to_string(), role.to_string());
                request
                    .extensions_mut()
                    .insert(Session::new_for_test("standalone".to_string(), claims));
                next.run(request).await
            },
        ));
    axum::Router::new().nest("/api/harvest", mounted)
}

async fn get(app: axum::Router, uri: &str, bearer: Option<&str>) -> StatusCode {
    let mut builder = axum::http::Request::builder().uri(uri);
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    app.oneshot(builder.body(Body::empty()).expect("request should build"))
        .await
        .expect("router should serve the request")
        .status()
}

/// An undeclared mount is unchanged: no layers, no boundary, so the admin gate
/// fails closed.
#[tokio::test]
async fn an_undeclared_mount_still_fails_closed() {
    let auth = StandaloneAdminAuth::new();

    let status = get(standalone_app(&auth), "/api/harvest/admin/preflight", None).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// A declared embedder boundary is reachable standalone. `set_admin_auth_boundary`
/// was called from exactly one non-test place before this change, inside
/// `HarvestPlugin`.
#[tokio::test]
async fn a_declared_embedder_boundary_admits_an_admin_route() {
    let auth = StandaloneAdminAuth::new().with_admin_auth_boundary();

    let status = get(standalone_app(&auth), "/api/harvest/admin/preflight", None).await;

    assert_eq!(status, StatusCode::OK);
}

/// The token layer is installed. A claimed `hvst_` bearer that the layer cannot
/// verify is refused with 503 rather than passed through. A 401 here would mean
/// the layer never ran.
#[tokio::test]
async fn the_token_layer_is_installed_on_a_standalone_mount() {
    let auth = StandaloneAdminAuth::new().with_api_tokens();

    let status = get(
        standalone_app(&auth),
        "/api/harvest/admin/preflight",
        Some("hvst_not_a_real_token"),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

/// Without the opt-in the token layer is absent, so the same request falls
/// through to the admin gate and is rejected there. This is the state every
/// standalone mount was in.
#[tokio::test]
async fn the_token_layer_is_absent_without_the_opt_in() {
    let auth = StandaloneAdminAuth::new();

    let status = get(
        standalone_app(&auth),
        "/api/harvest/admin/preflight",
        Some("hvst_not_a_real_token"),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// The read-only-role layer is installed and covers the nested Vantage router.
/// Vantage paths carry no route class, so the layer fails closed and denies a
/// read-only principal.
#[tokio::test]
async fn the_read_only_role_layer_covers_the_nested_ui_router() {
    let auth = StandaloneAdminAuth::new()
        .with_admin_auth_boundary()
        .with_read_only_role();

    let status = get(
        standalone_app_with_role(&auth, "harvest_readonly"),
        "/api/harvest/ui/workflows",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Without the opt-in the same read-only principal is not denied, which is what
/// makes the assertion above non-vacuous.
#[tokio::test]
async fn the_read_only_role_layer_is_absent_without_the_opt_in() {
    let auth = StandaloneAdminAuth::new().with_admin_auth_boundary();

    let status = get(
        standalone_app_with_role(&auth, "harvest_readonly"),
        "/api/harvest/ui/workflows",
        None,
    )
    .await;

    assert_ne!(status, StatusCode::FORBIDDEN);
}

/// The settings that only `HarvestPlugin` set are declared in one call, at
/// router-build time. `preflight` reports both of the ones it can observe.
#[tokio::test]
async fn the_declared_settings_reach_harvest_api_state() {
    let api_state = HarvestApiState::new();
    let auth = StandaloneAdminAuth::new()
        .with_admin_auth_boundary()
        .with_deployment_profile("prod");

    let app = axum::Router::new().nest(
        "/api/harvest",
        auth.mount(harvest_api_router(api_state.clone()), &api_state),
    );

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/harvest/admin/preflight")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should serve the request");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let report: serde_json::Value =
        serde_json::from_slice(&body).expect("preflight report should be JSON");
    let boundary = report["checks"]
        .as_array()
        .expect("checks should be an array")
        .iter()
        .find(|check| check["name"] == "admin_auth_boundary")
        .expect("admin_auth_boundary must be part of the preflight report");

    assert_eq!(boundary["details"]["profile"], "prod");
    assert_eq!(boundary["details"]["auth_boundary_present"], true);
}
