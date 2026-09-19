//! Standalone-mount composition tests for the two exported routers
//! (issue #1606).
//!
//! `harvest_api_router` and `harvest_ui_router` return `Router<()>`, so an
//! embedder nests them into a plain `axum::Router` and never constructs an
//! `autumn_web::AppState`. This file is the compile-time guard for that
//! property: it builds the exact shape a non-autumn-web embedder builds, with
//! no `.with_state(...)` call anywhere. A regression to `Router<AppState>`
//! fails this file at compile time, before any assertion runs.
//!
//! No database is required. `HarvestApiState::new()` with nothing installed is
//! the state a router has before it receives traffic, and the three routes
//! asserted here tolerate it.

use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::harvest_ui_router;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

/// The standalone composition under test: the API router with the Vantage
/// router nested inside it, mounted on a bare Axum router.
///
/// `plugin.rs` nests the two routers the same way, so their state types must
/// match. Nesting them here keeps that pairing under test on the standalone
/// path as well.
fn standalone_app(api_state: HarvestApiState) -> axum::Router {
    axum::Router::new().nest(
        "/api/harvest",
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state)),
    )
}

async fn get(app: axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should serve the request");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    (status, body.to_vec())
}

#[tokio::test]
async fn health_route_serves_without_autumn_web_state() {
    let (status, _) = get(
        standalone_app(HarvestApiState::new()),
        "/api/harvest/health",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn openapi_document_serves_without_autumn_web_state() {
    let (status, _) = get(
        standalone_app(HarvestApiState::new()),
        "/api/harvest/openapi.json",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
}

/// The admin gate is unchanged by the state-type change. A standalone mount
/// gets the default `unknown` profile, declares no auth boundary and presents
/// no credential, so the fail-closed branch rejects the request.
#[tokio::test]
async fn admin_route_still_fails_closed_without_a_credential() {
    let (status, _) = get(
        standalone_app(HarvestApiState::new()),
        "/api/harvest/admin/preflight",
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// Pins the profile source. `preflight` used to read the profile from the
/// `autumn_web::AppState` the router carried. It now reads the profile
/// `HarvestApiState` already holds, which is the only value a standalone
/// embedder can set.
#[tokio::test]
async fn preflight_reports_the_profile_from_harvest_api_state() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    api_state.set_admin_auth_boundary(true);

    let (status, body) = get(standalone_app(api_state), "/api/harvest/admin/preflight").await;

    assert_eq!(status, StatusCode::OK);
    let report: serde_json::Value =
        serde_json::from_slice(&body).expect("preflight report should be JSON");
    let boundary = report["checks"]
        .as_array()
        .expect("checks should be an array")
        .iter()
        .find(|check| check["name"] == "admin_auth_boundary")
        .expect("admin_auth_boundary must be part of the preflight report");

    assert_eq!(boundary["details"]["profile"], "prod");
}
