/// TDD Red Phase — tests for the SSE execution event stream (issue #324).
///
/// These tests will FAIL until the SSE stream implementation is in place.
use autumn_harvest_plugin::api::{HarvestApiState, management_api_routes};

// ── Route registration ────────────────────────────────────────────────────────

#[test]
fn sse_stream_route_is_registered_in_management_api_routes() {
    let routes = management_api_routes();
    assert!(
        routes.contains(&("GET", "/executions/{exec_id}/events/stream")),
        "SSE stream route not found in management_api_routes(); \
         add it to the router and the canonical route list"
    );
}

// ── HarvestApiState SSE configuration ────────────────────────────────────────

#[test]
fn sse_keepalive_interval_default_is_15s() {
    let state = HarvestApiState::new();
    assert_eq!(
        state.sse_keepalive_interval(),
        std::time::Duration::from_secs(15),
        "default SSE keepalive interval must be 15 s per issue #324 AC"
    );
}

#[test]
fn sse_buffer_depth_default_is_1024() {
    let state = HarvestApiState::new();
    assert_eq!(
        state.sse_buffer_depth(),
        1024,
        "default SSE buffer depth must be 1024 events per issue #324 AC"
    );
}

#[test]
fn sse_keepalive_interval_is_configurable() {
    let state = HarvestApiState::new();
    state.set_sse_keepalive_interval(std::time::Duration::from_secs(30));
    assert_eq!(
        state.sse_keepalive_interval(),
        std::time::Duration::from_secs(30)
    );
}

#[test]
fn sse_buffer_depth_is_configurable() {
    let state = HarvestApiState::new();
    state.set_sse_buffer_depth(512);
    assert_eq!(state.sse_buffer_depth(), 512);
}

// ── Audit constants ───────────────────────────────────────────────────────────

#[test]
fn audit_op_execution_stream_open_constant_exists() {
    use autumn_harvest::audit::OP_EXECUTION_STREAM_OPEN;
    assert_eq!(OP_EXECUTION_STREAM_OPEN, "execution.stream.open");
}

#[test]
fn audit_op_execution_stream_close_constant_exists() {
    use autumn_harvest::audit::OP_EXECUTION_STREAM_CLOSE;
    assert_eq!(OP_EXECUTION_STREAM_CLOSE, "execution.stream.close");
}

// ── Route classification ──────────────────────────────────────────────────────

#[test]
fn sse_stream_route_is_classified_as_read_only() {
    use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};
    let route = "GET /executions/{exec_id}/events/stream";
    let classification = CLASSIFIED_ROUTES
        .iter()
        .find(|(r, _)| *r == route)
        .map(|(_, c)| c);
    assert_eq!(
        classification,
        Some(&RouteClass::ReadOnly),
        "SSE stream route '{route}' must be classified as ReadOnly in CLASSIFIED_ROUTES"
    );
}

#[test]
fn sse_stream_route_is_in_all_mutation_routes_as_excluded() {
    use autumn_harvest::audit::ALL_MUTATION_ROUTES;
    let route = "GET /executions/{exec_id}/events/stream";
    let found = ALL_MUTATION_ROUTES.iter().any(|(r, _)| *r == route);
    assert!(
        found,
        "SSE stream route '{route}' must be present in ALL_MUTATION_ROUTES (with None operation)"
    );
}

// ── SSE HTTP contract ─────────────────────────────────────────────────────────

#[tokio::test]
async fn sse_stream_returns_service_unavailable_when_not_configured() {
    use autumn_web::reexports::axum::body::Body;
    use autumn_web::reexports::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    let app = autumn_harvest_plugin::api::harvest_api_router(HarvestApiState::new())
        .with_state(autumn_web::AppState::for_test());

    let req = Request::builder()
        .method(Method::GET)
        .uri("/executions/00000000-0000-0000-0000-000000000001/events/stream")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    // When no notification URL is configured, the endpoint should return 503 or 404/401.
    // It must NOT return 200 with no stream configured.
    assert_ne!(
        res.status(),
        StatusCode::OK,
        "SSE endpoint should not return 200 when notification URL is not configured"
    );
}

// ── Progress stream (issue #791) ──────────────────────────────────────────────

#[test]
fn progress_stream_route_is_registered_in_management_api_routes() {
    let routes = management_api_routes();
    assert!(
        routes.contains(&("GET", "/workflows/{id}/stream")),
        "progress stream route not found in management_api_routes(); \
         add it to the router and the canonical route list"
    );
}

#[test]
fn progress_stream_route_is_classified_as_read_only() {
    use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};
    let route = "GET /workflows/{id}/stream";
    let classification = CLASSIFIED_ROUTES
        .iter()
        .find(|(r, _)| *r == route)
        .map(|(_, c)| c);
    assert_eq!(
        classification,
        Some(&RouteClass::ReadOnly),
        "progress stream route '{route}' must be classified as ReadOnly in CLASSIFIED_ROUTES"
    );
}

/// Progress chunks may carry sensitive output and each subscription consumes a
/// dedicated Postgres LISTEN connection, so the route must reject an
/// unauthenticated request before it reaches the stream handler.
#[tokio::test]
async fn progress_stream_requires_admin_access() {
    use autumn_web::reexports::axum::body::Body;
    use autumn_web::reexports::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    // Default state: admin boundary NOT open, no session, no notification URL.
    let app = autumn_harvest_plugin::api::harvest_api_router(HarvestApiState::new())
        .with_state(autumn_web::AppState::for_test());

    let uuid = "00000000-0000-0000-0000-000000000001";

    // The admin-gated events stream (#324) rejects an unauthenticated request.
    let admin_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/executions/{uuid}/events/stream"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        admin_resp.status(),
        StatusCode::UNAUTHORIZED,
        "control: the admin-gated events stream must reject an unauthenticated request"
    );

    // The progress stream must enforce the same admin boundary.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/workflows/{uuid}/stream"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "progress stream must reject unauthenticated requests before opening a LISTEN connection"
    );
}

/// FIX (#791 review): an unconfigured / unavailable LISTEN/NOTIFY URL is a
/// *server* misconfiguration, so the progress stream must return 503 (retriable),
/// not the 400 a `HarvestError::Config` would otherwise map to. With no
/// notification URL configured, the handler short-circuits to 503.
#[tokio::test]
async fn progress_stream_returns_service_unavailable_when_not_configured() {
    use autumn_web::reexports::axum::body::Body;
    use autumn_web::reexports::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    let state = HarvestApiState::new();
    // Model the embedder-provided auth boundary so the request may reach the
    // handler and exercise its configuration error path.
    state.set_admin_auth_boundary(true);
    let app = autumn_harvest_plugin::api::harvest_api_router(state)
        .with_state(autumn_web::AppState::for_test());

    let req = Request::builder()
        .method(Method::GET)
        .uri("/workflows/00000000-0000-0000-0000-000000000001/stream")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(
        res.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "progress stream must return 503 (not 400) when the LISTEN/NOTIFY URL is \
         not configured — it is a retriable server misconfiguration"
    );
}

#[tokio::test]
async fn sse_stream_route_exists_in_router() {
    use autumn_web::reexports::axum::body::Body;
    use autumn_web::reexports::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    let app = autumn_harvest_plugin::api::harvest_api_router(HarvestApiState::new())
        .with_state(autumn_web::AppState::for_test());

    let req = Request::builder()
        .method(Method::GET)
        .uri("/executions/00000000-0000-0000-0000-000000000001/events/stream")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    // The route must be registered — 404 means it's not
    assert_ne!(
        res.status(),
        StatusCode::NOT_FOUND,
        "SSE stream route not found in router (returns 404). Register the route."
    );
    assert_ne!(
        res.status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "SSE stream route registered with wrong HTTP method."
    );
}
