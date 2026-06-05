use std::collections::HashMap;

use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::AppState;
use autumn_web::auth::RequireAuth;
use autumn_web::reexports::axum::body::Body;
use autumn_web::reexports::http::{Method, Request, StatusCode};
use autumn_web::session::Session;
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

fn app_with_api_state(
    api_state: HarvestApiState,
) -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(api_state).with_state(AppState::for_test())
}

fn unauthenticated_app_with_ui() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    harvest_api_router(api_state.clone())
        .nest("/ui", harvest_ui_router(api_state))
        .with_state(AppState::for_test())
}

/// Build a test app protected by `RequireAuth`.
fn authenticated_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(HarvestApiState::new())
        .route_layer(RequireAuth::new("user_id"))
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

fn post_json_with_session(uri: &str, body: &str, session_key: &str) -> Request<Body> {
    let mut request = post_json(uri, body);
    let mut data = HashMap::new();
    data.insert(session_key.to_string(), "operator-1".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));
    request
}

fn patch_json(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(Method::PATCH)
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

fn delete(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::DELETE)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

// ── Without authentication middleware ────────────────────────────────────────
//
// When `harvest_api_router` is mounted without any auth layer, ordinary routes
// remain directly reachable while high-impact management operations use
// Harvest's built-in guard.

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
async fn eris_unauthenticated_start_workflow_terminate_if_running_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"reuse_policy": "terminate_if_running"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_start_workflow_terminate_if_running_honors_configured_session_key() {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(post_json_with_session(
            "/workflows/my-workflow/start",
            r#"{"reuse_policy": "terminate_if_running"}"#,
            "operator_id",
        ))
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
async fn eris_unauthenticated_cancel_workflow_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/cancel",
            r#"{"reason": "operator request"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
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
async fn eris_require_auth_blocks_workflow_children() {
    let app = authenticated_app();
    let res = app
        .oneshot(get(
            "/workflows/00000000-0000-0000-0000-000000000001/children",
        ))
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
async fn eris_require_auth_blocks_cancel_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/cancel",
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

#[tokio::test]
async fn eris_unauthenticated_list_dead_letters_is_blocked() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/dead-letters")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_replay_dead_letter_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/dead-letters/00000000-0000-0000-0000-000000000001/replay",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_bulk_replay_dead_letters_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/dead-letters/replay", r#"{"ids": []}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_bulk_discard_dead_letters_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/dead-letters/discard", r#"{"ids": []}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_vantage_dead_letters_page_is_blocked() {
    let app = unauthenticated_app_with_ui();
    let res = app.oneshot(get("/ui/dead-letters")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_builtin_guard_honors_configured_session_key() {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(post_json_with_session(
            "/dead-letters/replay",
            r#"{"ids": []}"#,
            "operator_id",
        ))
        .await
        .unwrap();

    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_builtin_guard_does_not_accept_hard_coded_admin_id() {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(post_json_with_session(
            "/dead-letters/replay",
            r#"{"ids": []}"#,
            "admin_id",
        ))
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_declared_outer_auth_boundary_skips_inner_session_check() {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(post_json("/dead-letters/replay", r#"{"ids": []}"#))
        .await
        .unwrap();

    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_require_auth_blocks_list_dead_letters() {
    let app = authenticated_app();
    let res = app.oneshot(get("/dead-letters")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_replay_dead_letter() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/dead-letters/00000000-0000-0000-0000-000000000001/replay",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ── Additional unauthenticated-accessible checks ──────────────────────────────
//
// Documents remaining routes that are intentionally still open without an
// outer middleware or one of Harvest's high-impact built-in guards.

#[tokio::test]
async fn eris_unauthenticated_reset_workflow_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/reset",
            "{}",
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_retention_run_now_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/admin/retention/run-now", "{}"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_create_schedule_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/admin/schedules/workflow", "{}"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_unauthenticated_submit_batch_cancel_operation_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/batch-operations",
            r#"{"action": "Cancel", "filter": {"workflow_name": "billing"}}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_submit_batch_terminate_operation_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/batch-operations",
            r#"{"action": "Terminate", "filter": {"workflow_name": "billing"}}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_worker_drain_is_accessible() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workers/worker-abc/drain",
            r#"{"deadline_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// ── RequireAuth blocks all mutating routes (AC5) ──────────────────────────────
//
// Proves that every route in the "Mutating" security class returns 401 when
// the RequireAuth middleware is applied. Covers: workflow start/signal/cancel
// (existing), workflow reset, DLQ replay/discard (bulk + single, existing),
// schedule mutation, batch submission, retention run-now, external activity
// completion, and worker drain.

#[tokio::test]
async fn eris_require_auth_blocks_reset_workflow() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/reset",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_admit_update() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/update/approve",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_bulk_replay_dead_letters() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/dead-letters/replay", r#"{"ids": []}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_bulk_discard_dead_letters() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/dead-letters/discard", r#"{"ids": []}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_retention_run_now() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/admin/retention/run-now", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_create_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/admin/schedules/workflow", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_pause_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/schedules/00000000-0000-0000-0000-000000000001/pause",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_resume_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/schedules/00000000-0000-0000-0000-000000000001/resume",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_delete_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(delete(
            "/admin/schedules/00000000-0000-0000-0000-000000000001",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_complete_external_activity() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/activities/external/some-task-token/complete",
            r#"{"result": null}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_fail_external_activity() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/activities/external/some-task-token/fail",
            r#"{"error": "timeout"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_heartbeat_external_activity() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/activities/external/some-task-token/heartbeat",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_worker_drain() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/workers/worker-abc/drain",
            r#"{"deadline_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_submit_batch_operation() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json("/batch-operations", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_trigger_schedule() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/schedules/00000000-0000-0000-0000-000000000001/trigger",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ── Payload limit DoS tests ────────────────────────────────────────────────

#[tokio::test]
async fn warden_batch_start_workflows_enforces_payload_limit() {
    let api_state = HarvestApiState::new();
    let limit = api_state.batch_start_max_bytes();
    let app = app_with_api_state(api_state);

    let large_body = vec![b'a'; usize::try_from(limit).unwrap_or(usize::MAX - 20) + 10];
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/workflows/batch_start")
        .header("Content-Type", "application/json")
        .body(Body::from(large_body))
        .unwrap();
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "operator-1".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));

    let res = app.oneshot(request).await.unwrap();
    assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn warden_query_workflow_post_enforces_payload_limit() {
    let app = unauthenticated_app();

    let large_body = vec![b'a'; 2 * 1024 * 1024 + 10]; // Over 2MB
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/workflows/00000000-0000-0000-0000-000000000001/query/my_query")
        .header("Content-Type", "application/json")
        .body(Body::from(large_body))
        .unwrap();
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "operator-1".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));

    let res = app.oneshot(request).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&bytes);

    assert!(body_str.contains("payload too large"));
}

#[tokio::test]
async fn warden_bulk_replay_dead_letters_enforces_payload_limit() {
    let app = authenticated_app();

    let large_body = vec![b'a'; 2 * 1024 * 1024 + 10]; // Over 2MB
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/dead-letters/replay")
        .header("Content-Type", "application/json")
        .body(Body::from(large_body))
        .unwrap();
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "operator-1".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));

    let res = app.oneshot(request).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&bytes);

    assert!(body_str.contains("payload too large"));
}

#[tokio::test]
async fn warden_bulk_discard_dead_letters_enforces_payload_limit() {
    let app = authenticated_app();

    let large_body = vec![b'a'; 2 * 1024 * 1024 + 10]; // Over 2MB
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/dead-letters/discard")
        .header("Content-Type", "application/json")
        .body(Body::from(large_body))
        .unwrap();
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "operator-1".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));

    let res = app.oneshot(request).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&bytes);

    assert!(body_str.contains("payload too large"));
}
