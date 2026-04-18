use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::auth::RequireAuth;
use autumn_web::reexports::axum::body::Body;
use autumn_web::reexports::http::{Method, Request, StatusCode};
use tower::ServiceExt;

/// Build an unauthenticated test app (no middleware applied).
fn unauthenticated_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(HarvestApiState::new()).with_state(AppState::for_test())
}

/// Build a test app protected by `RequireAuth`.
fn authenticated_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(HarvestApiState::new())
        .route_layer(RequireAuth::new("admin_id"))
        .with_state(AppState::for_test())
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn post_json(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

fn patch_json(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(Method::PATCH)
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

// ── Without authentication middleware ────────────────────────────────────────
//
// When `harvest_api_router` is mounted without any auth layer the API is
// directly reachable. Responses will be errors (no DB), but crucially the
// requests are NOT rejected with 401/403 – authentication is entirely absent.

#[tokio::test]
async fn eris_unauthenticated_health_is_accessible() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/health")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_list_workflows_is_accessible() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/workflows")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_start_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/workflows/my-workflow/start", "{}"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_list_dags_is_accessible() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/dags")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_get_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(get("/workflows/00000000-0000-0000-0000-000000000001"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_signal_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/signal/approve",
            r#"{"approved": true}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_query_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(get(
            "/workflows/00000000-0000-0000-0000-000000000001/query/status",
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_list_dag_runs_is_accessible() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/dags/my-dag/runs")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_trigger_dag_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/dags/my-dag/trigger", "{}"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_patch_dag_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(patch_json("/dags/my-dag", r#"{"paused": true}"#))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// ── With RequireAuth middleware ───────────────────────────────────────────────
//
// When the router is wrapped with `RequireAuth`, every endpoint must reject
// unauthenticated requests with 401 before any handler logic runs.

#[tokio::test]
async fn eris_require_auth_blocks_health() {
    let app = authenticated_app();
    let res = app.oneshot(get("/health")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_list_workflows() {
    let app = authenticated_app();
    let res = app.oneshot(get("/workflows")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_get_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(get("/workflows/00000000-0000-0000-0000-000000000001"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_start_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/workflows/my-workflow/start", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_signal_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/signal/approve",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_query_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(get(
            "/workflows/00000000-0000-0000-0000-000000000001/query/status",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_list_dags() {
    let app = authenticated_app();
    let res = app.oneshot(get("/dags")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_list_dag_runs() {
    let app = authenticated_app();
    let res = app.oneshot(get("/dags/my-dag/runs")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_trigger_dag() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/dags/my-dag/trigger", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_patch_dag() {
    let app = authenticated_app();
    let res = app
        .oneshot(patch_json("/dags/my-dag", r#"{"paused": true}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
