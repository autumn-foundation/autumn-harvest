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

    let app =
        autumn_harvest_plugin::api::harvest_api_router(HarvestApiState::new())
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

#[tokio::test]
async fn sse_stream_route_exists_in_router() {
    use autumn_web::reexports::axum::body::Body;
    use autumn_web::reexports::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    let app =
        autumn_harvest_plugin::api::harvest_api_router(HarvestApiState::new())
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
