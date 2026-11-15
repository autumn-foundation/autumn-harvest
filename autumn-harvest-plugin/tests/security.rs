use std::collections::HashMap;

use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::auth::RequireAuth;
use autumn_web::reexports::axum::body::Body;
use autumn_web::reexports::http::{Method, Request, StatusCode};
use autumn_web::session::Session;
use tower::ServiceExt;

/// Build an unauthenticated test app (no middleware applied).
///
/// It sets the issue #1802 opt-out, which restores the pre-#1802 posture.
/// The tests that use it probe the admin gate and the handlers' own gates,
/// so the outer mutation gate must not answer for them.
fn unauthenticated_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    api_state.set_allow_unauthenticated_mutations(true);
    harvest_api_router(api_state)
}

/// Build an app in the default posture: profile `unknown`, no boundary, no
/// opt-out. The issue #1802 gate is closed.
fn closed_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(HarvestApiState::new())
}

fn app_with_api_state(
    api_state: HarvestApiState,
) -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    harvest_api_router(api_state)
}

fn unauthenticated_app_with_ui() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state))
}

/// Build a test app protected by `RequireAuth`.
///
/// It sets the issue #1802 opt-out, so every 401 comes from `RequireAuth`.
fn authenticated_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    api_state.set_allow_unauthenticated_mutations(true);
    harvest_api_router(api_state).route_layer(RequireAuth::new("user_id"))
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
// When `harvest_api_router` is mounted without any auth layer, read routes
// remain directly reachable. Outside the `dev` profile, every mutating route
// answers 401 (issue #1802).

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
async fn eris_unauthenticated_start_workflow_is_blocked() {
    let app = closed_app();
    let res = app
        .oneshot(post_json("/workflows/my-workflow/start", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
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

// `conflict_policy = terminate_existing`, with ANY reuse policy, resolves to
// `Terminate`. It can cancel a live RUNNING/PAUSED prior. So it is admin-gated
// like `terminate_if_running` with the default conflict.
#[tokio::test]
async fn eris_unauthenticated_start_workflow_terminate_existing_conflict_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"conflict_policy": "terminate_existing"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// Narrowing proof (issue #685): the non-admin idempotent-starter shape
// `reuse = terminate_if_running` + `conflict = use_existing` resolves to
// `Attach`. It returns the existing handle and CANNOT cancel a live run. So it
// is NOT admin-gated. It passes the auth gate, then fails for an unrelated
// boot-window reason. Like each other "allowed through" case, the test asserts
// only "not blocked by auth".
#[tokio::test]
async fn eris_unauthenticated_start_workflow_terminate_if_running_plus_use_existing_is_allowed() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"reuse_policy": "terminate_if_running", "conflict_policy": "use_existing"}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// Narrowing proof: `reuse = terminate_if_running` + `conflict = fail` resolves to
// `Fail` (`Err(AlreadyExists)` over a live prior). It touches no live work, so
// it is NOT admin-gated either.
#[tokio::test]
async fn eris_unauthenticated_start_workflow_terminate_if_running_plus_fail_is_allowed() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"reuse_policy": "terminate_if_running", "conflict_policy": "fail"}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// Narrowing proof: a `conflict_policy = use_existing` (attach) start with the
// default reuse policy resolves to `Attach` — non-destructive — and is NOT
// admin-gated.
#[tokio::test]
async fn eris_unauthenticated_start_workflow_use_existing_conflict_is_allowed() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/my-workflow/start",
            r#"{"conflict_policy": "use_existing"}"#,
        ))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// Issue #685: the early cancel-capable admin gate must NOT pre-empt a request
// with an idempotency key. The #808 committed-replay claim is keyed on
// `(workflow_name, idempotency_key)` ALONE. It ignores the reuse/conflict
// policy and the caller's auth. So an ADMIN can commit a keyed
// `terminate_if_running` start. A later NON-admin retry with the same key must
// replay it as a `200`, not a `401`. So a keyed cancel-capable request defers
// past the early gate. In this no-DB harness, the downstream path fails closed
// at the runtime-not-started check, with a non-`401` status. The UNKEYED case
// above asserts `401`; this KEYED case must NOT. A fresh keyed `Terminate` by a
// non-admin still gets `401` from the authoritative gate downstream. That gate
// runs AFTER the committed-replay probe. The #808 integration tests cover it
// with a DB.
#[tokio::test]
async fn eris_keyed_terminate_if_running_defers_past_early_gate() {
    let app = unauthenticated_app();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/workflows/my-workflow/start")
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", "replay-key-1")
        .body(Body::from(
            r#"{"reuse_policy": "terminate_if_running"}"#.to_owned(),
        ))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// Companion narrowing proof for the other cancel-capable shape: a keyed
// `conflict_policy = terminate_existing` request likewise defers past the early
// gate (the UNKEYED variant above asserts `401`).
#[tokio::test]
async fn eris_keyed_terminate_existing_conflict_defers_past_early_gate() {
    let app = unauthenticated_app();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/workflows/my-workflow/start")
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", "replay-key-2")
        .body(Body::from(
            r#"{"conflict_policy": "terminate_existing"}"#.to_owned(),
        ))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn eris_start_workflow_terminate_if_running_honors_configured_session_key() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
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
async fn eris_unauthenticated_signal_workflow_is_blocked() {
    let app = closed_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/signal/approve",
            r#"{"approved": true}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
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
async fn eris_unauthenticated_fail_activity_now_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/activities/00000000-0000-0000-0000-000000000002/fail-now",
            r#"{"reason": "operator request"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Issue #619 AC1/AC6: pausing or resuming a task queue is an operator action
/// that must be admin-gated and audited — never reachable unauthenticated.
#[tokio::test]
async fn eris_unauthenticated_queue_pause_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/queues/email-workers/pause",
            r#"{"reason": "SMTP outage"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_queue_resume_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/admin/queues/email-workers/resume", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Issue #807: pausing dispatch for an activity TYPE is a fleet-wide kill
/// switch for that downstream. It is the most surgical hold control, so
/// operators reach for it most. It must be admin-gated and audited.
///
/// The body is deliberately optional, because containment must not wait on
/// paperwork. A bare POST is a valid hold, so this mutation is the easiest to
/// fire by accident. The gate matters more here. Both cases send a body-free
/// POST for that reason.
#[tokio::test]
async fn eris_unauthenticated_activity_pause_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/activities/charge_card/pause", ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_activity_resume_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/activities/charge_card/resume", ""))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Issue #807: the paused-queue list is an ungated read, but BOTH activity
/// reads are admin-gated. They show operator-authored hold reasons, free text
/// written mid-incident. They also show the registered activity catalogue, a map
/// of the fleet's downstream dependencies. The pin stops someone who copies the
/// queue sibling from dropping the gate.
#[tokio::test]
async fn eris_unauthenticated_activity_list_is_blocked() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/activities")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_activity_get_is_blocked() {
    let app = unauthenticated_app();
    let res = app.oneshot(get("/activities/charge_card")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_workflow_logs_is_blocked() {
    // Durable per-execution author log lines (issue #790) are an admin-only
    // read. A log message is free-form author text that often carries business
    // detail. So it takes the /awaitables posture, not the execution-row one.
    let app = unauthenticated_app();
    let res = app
        .oneshot(get("/workflows/00000000-0000-0000-0000-000000000001/logs"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_awaitables_is_blocked() {
    // Open-awaitables diagnostic (issue #615): admin-only read (the
    // eligibility endpoints' posture).
    let app = unauthenticated_app();
    let res = app
        .oneshot(get(
            "/workflows/00000000-0000-0000-0000-000000000001/awaitables",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_replay_diagnosis_is_blocked() {
    // Single-execution replay diagnosis (issue #614): admin-only read.
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/replay-diagnosis",
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_rerun_is_blocked() {
    // Operator re-run of a terminal workflow (issue #777): starts a brand-new
    // execution from a source's recorded start params — admin-only mutation.
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/rerun",
            r"{}",
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_legal_hold_set_is_blocked() {
    // Per-execution legal hold (issue #747): admin-only mutation.
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/legal-hold",
            r#"{"reason": "litigation hold"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_legal_hold_release_is_blocked() {
    // Per-execution legal hold release (issue #747): admin-only mutation.
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json(
            "/workflows/00000000-0000-0000-0000-000000000001/legal-hold/release",
            "{}",
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
async fn eris_unauthenticated_trigger_dag_is_blocked() {
    let app = closed_app();
    let res = app
        .oneshot(post_json("/dags/my-dag/trigger", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_patch_dag_is_blocked() {
    let app = closed_app();
    let res = app
        .oneshot(patch_json("/dags/my-dag", r#"{"paused": true}"#))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
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
async fn eris_unauthenticated_admin_status_is_blocked() {
    // Issue #679: the rolled-up health summary is admin-gated; the built-in
    // guard rejects before the handler runs, so no DB is needed here.
    let app = unauthenticated_app();
    let res = app.oneshot(get("/admin/status")).await.unwrap();
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
    api_state.set_deployment_profile("dev");
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
    api_state.set_deployment_profile("dev");
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

// ── Additional unauthenticated mutation checks ───────────────────────────────
//
// Outside the `dev` profile, these mutating routes fail closed with no outer
// middleware (issue #1802).

#[tokio::test]
async fn eris_unauthenticated_reset_workflow_is_blocked() {
    let app = closed_app();
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
async fn eris_unauthenticated_retention_run_now_is_blocked() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(post_json("/admin/retention/run-now", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_non_admin_session_is_blocked_in_non_dev_profile() {
    let api_state = HarvestApiState::new();
    // Issue #1802: open the mutation gate, so the admin gate decides.
    api_state.set_allow_unauthenticated_mutations(true);
    let app = app_with_api_state(api_state);

    let mut request = post_json("/admin/retention/run-now", "{}");
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "some-user".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));

    let res = app.oneshot(request).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_admin_session_is_allowed_in_non_dev_profile() {
    let api_state = HarvestApiState::new();
    // Issue #1802: open the mutation gate, so the admin gate decides.
    api_state.set_allow_unauthenticated_mutations(true);
    let app = app_with_api_state(api_state);

    let mut request = post_json("/admin/retention/run-now", "{}");
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "some-user".to_string());
    data.insert("role".to_string(), "admin".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));

    let res = app.oneshot(request).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_unauthenticated_create_schedule_is_blocked() {
    let app = closed_app();
    let res = app
        .oneshot(post_json("/admin/schedules/workflow", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
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
async fn eris_unauthenticated_worker_drain_is_blocked() {
    let app = closed_app();
    let res = app
        .oneshot(post_json(
            "/workers/worker-abc/drain",
            r#"{"deadline_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
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

// ── TTL'd runtime pacing overrides (issue #945) — admin-gated ────────────────
//
// All five pacing-override routes carry `require_admin` in `api.rs`'s
// `harvest_api_router`: 2 rate-limit, 2 start-throttle, and the read-only
// start-throttle-override lookup. An unauthenticated request never reaches that
// gate. `RequireAuth` wraps the whole router and rejects it with 401 first.
// These tests prove the routes are in the SAME auth chain as each other
// admin-gated route.

#[tokio::test]
async fn eris_require_auth_blocks_set_rate_limit_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/rate-limits/send_email/override",
            r#"{"refill_rate": 100.0, "ttl_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_clear_rate_limit_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(delete("/admin/rate-limits/send_email/override"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_set_start_throttle_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/start-throttle/onboard_user/override",
            r#"{"refill_per_sec": 100.0, "ttl_secs": 60}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_clear_start_throttle_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(delete("/admin/start-throttle/onboard_user/override"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_get_start_throttle_override() {
    let app = authenticated_app();
    let res = app
        .oneshot(get("/admin/start-throttle/onboard_user/override"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ── Read-only operator role (issue #776) ──────────────────────────────────────
//
// Headline success-metric test (AC1/AC2, no DB). A synthetic router mounts a
// 200-OK handler at EVERY (method, path) of
// `autumn_harvest::audit::CLASSIFIED_ROUTES`. The read-only class-enforcement
// layer wraps it, and a read-only Session drives it. Every Mutating route →
// 403; every ReadOnly/PublicSafe route → 200. The test asserts the metric "0 of
// N mutation routes reachable, N of N read routes reachable". The layer matches
// the URI path, not `MatchedPath`. So a top-level synthetic router is faithful
// to nested production.

use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};
use autumn_harvest_plugin::api::enforce_read_only_class;
use autumn_web::reexports::axum::Router;
use autumn_web::reexports::axum::middleware::from_fn;
use autumn_web::reexports::axum::routing::{MethodFilter, MethodRouter};

fn method_filter(m: &Method) -> MethodFilter {
    match *m {
        Method::GET => MethodFilter::GET,
        Method::POST => MethodFilter::POST,
        Method::PUT => MethodFilter::PUT,
        Method::PATCH => MethodFilter::PATCH,
        Method::DELETE => MethodFilter::DELETE,
        _ => panic!("unexpected method in CLASSIFIED_ROUTES: {m}"),
    }
}

/// Substitute each `{param}` segment with a non-static placeholder so the
/// concrete request path uniquely matches its own template (never a sibling
/// static route).
fn concrete_request_path(template: &str) -> String {
    let mut out = String::new();
    let mut k = 0u32;
    for seg in template.split('/') {
        if seg.is_empty() {
            continue;
        }
        out.push('/');
        if seg.starts_with('{') && seg.ends_with('}') {
            out.push_str("zzztestparam");
            out.push_str(&k.to_string());
            k += 1;
        } else {
            out.push_str(seg);
        }
    }
    if out.is_empty() {
        out.push('/');
    }
    out
}

/// Build the synthetic router: every `CLASSIFIED_ROUTES` (method, path) → 200,
/// wrapped in the read-only enforcement layer. Routes sharing a path are merged
/// into one `MethodRouter` (registering the same (method, path) twice would
/// panic; `CLASSIFIED_ROUTES` is dup-free).
fn synthetic_enforced_router() -> Router {
    let mut by_path: HashMap<&str, MethodRouter> = HashMap::new();
    for (template, _class) in CLASSIFIED_ROUTES {
        let (method_str, path) = template.split_once(' ').expect("well-formed template");
        let method: Method = method_str.parse().expect("known method");
        let filter = method_filter(&method);
        let entry = by_path.entry(path).or_default();
        let mr = std::mem::take(entry);
        *entry = mr.on(filter, || async { StatusCode::OK });
    }
    let mut router: Router = Router::new();
    for (path, mr) in by_path {
        router = router.route(path, mr);
    }
    router.layer(from_fn(enforce_read_only_class))
}

fn session_role(role: &str) -> Session {
    let mut data = HashMap::new();
    data.insert("role".to_string(), role.to_string());
    Session::new_for_test("harvest-role-test".to_string(), data)
}

async fn status_for(
    app: &Router,
    method: &Method,
    concrete_path: &str,
    session: Option<Session>,
) -> StatusCode {
    let mut req = Request::builder()
        .method(method.clone())
        .uri(concrete_path)
        .body(Body::empty())
        .unwrap();
    if let Some(session) = session {
        req.extensions_mut().insert(session);
    }
    app.clone().oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn eris_readonly_principal_reaches_all_reads_and_no_mutations() {
    let app = synthetic_enforced_router();

    let mut mutations_total = 0usize;
    let mut mutations_reachable_by_readonly = 0usize;
    let mut reads_total = 0usize;
    let mut reads_reachable_by_readonly = 0usize;

    for (template, class) in CLASSIFIED_ROUTES {
        let (method_str, path) = template.split_once(' ').unwrap();
        let method: Method = method_str.parse().unwrap();
        let concrete = concrete_request_path(path);

        // Read-only principal.
        let ro_status = status_for(
            &app,
            &method,
            &concrete,
            Some(session_role("harvest_readonly")),
        )
        .await;

        match class {
            RouteClass::Mutating => {
                mutations_total += 1;
                assert_eq!(
                    ro_status,
                    StatusCode::FORBIDDEN,
                    "read-only principal must be denied 403 on Mutating route {template}"
                );
                if ro_status != StatusCode::FORBIDDEN {
                    mutations_reachable_by_readonly += 1;
                }
            }
            RouteClass::ReadOnly | RouteClass::PublicSafe => {
                reads_total += 1;
                assert_eq!(
                    ro_status,
                    StatusCode::OK,
                    "read-only principal must reach (200) ReadOnly/PublicSafe route {template}"
                );
                if ro_status == StatusCode::OK {
                    reads_reachable_by_readonly += 1;
                }
            }
        }

        // Admin principal: never denied by the class layer, reaches everything.
        let admin_status = status_for(&app, &method, &concrete, Some(session_role("admin"))).await;
        assert_eq!(
            admin_status,
            StatusCode::OK,
            "admin principal must reach (200) every route {template}"
        );
    }

    // The 100% / 0% split — the issue's explicit success metric.
    assert_eq!(
        mutations_reachable_by_readonly, 0,
        "0 of {mutations_total} mutation routes must be reachable by a read-only principal"
    );
    assert_eq!(
        reads_reachable_by_readonly, reads_total,
        "all {reads_total} read routes must be reachable by a read-only principal"
    );
    assert!(
        mutations_total > 0 && reads_total > 0,
        "sanity: both sets non-empty"
    );
}

#[tokio::test]
async fn eris_readonly_post_for_body_read_routes_are_reachable() {
    // A read-only principal reaches (200) the two ReadOnly POST routes (query,
    // retire) and the POST preview. Enforcement uses the class table, never the
    // HTTP verb.
    let app = synthetic_enforced_router();
    for concrete in [
        "/workflows/zzztestparam0/query/zzztestparam1",
        "/admin/build-routing/retire",
        "/admin/schedules/preview",
        "/workflows/by-id/zzztestparam0",
    ] {
        let status = status_for(
            &app,
            &Method::POST,
            concrete,
            Some(session_role("harvest_readonly")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "read-only principal must reach ReadOnly POST route {concrete}"
        );
    }
}

#[tokio::test]
async fn eris_unmarked_and_admin_principals_are_not_verb_restricted() {
    // AC6-adjacent: the class layer never denies a principal with no read-only
    // marker, or an admin, even on Mutating routes. The read-only restriction is
    // opt-in per principal.
    let app = synthetic_enforced_router();
    let mutating = "/workflows/zzztestparam0/cancel";

    // No session at all → not read-only-restricted → reaches the handler.
    let anon = status_for(&app, &Method::POST, mutating, None).await;
    assert_eq!(
        anon,
        StatusCode::OK,
        "unauthenticated/unmarked reaches handler"
    );

    // Session with an unrelated marker → not read-only-restricted.
    let unmarked = status_for(
        &app,
        &Method::POST,
        mutating,
        Some({
            let mut d = HashMap::new();
            d.insert("user_id".to_string(), "u1".to_string());
            Session::new_for_test("s".to_string(), d)
        }),
    )
    .await;
    assert_eq!(
        unmarked,
        StatusCode::OK,
        "unmarked principal reaches handler"
    );

    // Admin marker → not read-only-restricted.
    let admin = status_for(&app, &Method::POST, mutating, Some(session_role("admin"))).await;
    assert_eq!(admin, StatusCode::OK, "admin reaches handler");
}

// Issue #776: the headline synthetic test proves the class layer's verb
// decision. It does not prove the *production wiring* of the feature:
//
// - the enforce layer sits INSIDE the `/api/harvest` nest, so it sees the
//   mount-stripped path;
// - `admin_auth_boundary=true` lets a read-only principal reach the ~15
//   `require_admin` ReadOnly routes and the SSE stream's in-handler check.
//
// This test reproduces that wiring with the REAL `harvest_api_router`. It drives
// full `/api/harvest/...` paths through the nest.

use autumn_web::reexports::axum::middleware::Next;

/// Reproduce the `api_with_role_auth` production mount. The real
/// `harvest_api_router` runs with `admin_auth_boundary=true` and, when
/// `with_enforce`, the class layer. An embedder auth middleware tags the Session
/// `role`. The whole router nests under `/api/harvest`.
fn role_auth_nested_app(role: &'static str, with_enforce: bool) -> Router {
    let api_state = HarvestApiState::new();
    // `api_with_role_auth` → `api_with_auth` sets `admin_auth_boundary=true`,
    // making per-route `require_admin` (and the SSE in-handler admin check) a
    // no-op for the read-only principal — the AC1 reconciliation.
    api_state.set_admin_auth_boundary(true);
    let mut router = harvest_api_router(api_state);
    if with_enforce {
        router = router.layer(from_fn(enforce_read_only_class));
    }
    // Embedder auth middleware: authenticate + tag the Session. Applied LAST, so
    // it runs first and sets the Session before the enforce layer reads it. That
    // is the production order.
    router = router.layer(from_fn(
        move |mut req: Request<Body>, next: Next| async move {
            let mut d = HashMap::new();
            d.insert("role".to_string(), role.to_string());
            req.extensions_mut()
                .insert(Session::new_for_test("nested-test".to_string(), d));
            next.run(req).await
        },
    ));
    Router::new().nest("/api/harvest", router)
}

async fn drive_nested(app: &Router, method: Method, uri: &str) -> (StatusCode, String) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = autumn_web::reexports::axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn eris_readonly_role_auth_reaches_admin_reads_and_403s_mutations_through_the_nest() {
    let ro = role_auth_nested_app("harvest_readonly", true);

    // A read-only principal REACHES an admin-gated ReadOnly route through the
    // nest, because `require_admin` is a no-op under the boundary. Not 401: auth
    // passed. Not 403: the class layer allowed the read. A 5xx/400 from the
    // handler (no runtime) proves the request reached it.
    for path in ["/api/harvest/admin/config", "/api/harvest/admin/status"] {
        let (status, _) = drive_nested(&ro, Method::GET, path).await;
        assert_ne!(
            status,
            StatusCode::UNAUTHORIZED,
            "{path}: read-only must pass require_admin under the boundary (got 401)"
        );
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "{path}: read-only must reach an admin-gated ReadOnly route (got 403)"
        );
    }

    // A read-only principal reaches the SSE event stream (ReadOnly, admin-gated,
    // with an in-handler admin check). A valid uuid passes the id parse. The
    // in-handler gate passes under the boundary. The stream then fails closed on
    // the missing runtime. The timeout stops a DB connect from hanging the test.
    // A timeout still proves auth passed, because a 401/403 returns at once.
    let sse = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        drive_nested(
            &ro,
            Method::GET,
            "/api/harvest/executions/00000000-0000-0000-0000-000000000001/events/stream",
        ),
    )
    .await;
    if let Ok((status, _)) = sse {
        assert_ne!(
            status,
            StatusCode::UNAUTHORIZED,
            "SSE: read-only must pass the in-handler admin gate (got 401)"
        );
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "SSE: read-only must reach the SSE handler (got 403)"
        );
    }

    // A read-only principal gets 403 on a real Mutating route THROUGH THE NEST.
    // So the layer sees the mount-stripped path and gives the read-only-denial
    // body.
    let (mstatus, mbody) =
        drive_nested(&ro, Method::POST, "/api/harvest/workflows/exec-1/cancel").await;
    assert_eq!(
        mstatus,
        StatusCode::FORBIDDEN,
        "read-only must be 403'd on POST cancel through the /api/harvest nest"
    );
    assert!(
        mbody.contains("read-only principal: mutation not permitted"),
        "nest 403 must carry the read-only-denial marker, got: {mbody}"
    );

    // An admin principal reaches the same Mutating route (not 403).
    let admin = role_auth_nested_app("admin", true);
    let (astatus, _) =
        drive_nested(&admin, Method::POST, "/api/harvest/workflows/exec-1/cancel").await;
    assert_ne!(
        astatus,
        StatusCode::FORBIDDEN,
        "admin must reach the Mutating route through the nest"
    );
}

#[tokio::test]
async fn eris_role_auth_off_leaves_the_router_unchanged_through_the_nest() {
    // AC6: with NO enforce layer (the `api_with_auth`/`api()` shape), a
    // read-only principal reaches the Mutating handler — byte-for-byte the
    // pre-#776 behavior. `admin_auth_boundary=true` makes require_admin a
    // no-op, so the request reaches the handler (which fails closed on the
    // missing runtime), and is NEVER the read-only-denial 403.
    let off = role_auth_nested_app("harvest_readonly", false);
    let (status, body) =
        drive_nested(&off, Method::POST, "/api/harvest/workflows/exec-1/cancel").await;
    assert_ne!(
        status,
        StatusCode::FORBIDDEN,
        "without the enforce layer, a read-only principal is not 403'd (got {status})"
    );
    assert!(
        !body.contains("read-only principal: mutation not permitted"),
        "the read-only-denial marker must never appear when the layer is off"
    );
}

#[tokio::test]
async fn eris_readonly_denial_body_is_distinguishable() {
    // AC7 observability: a denied mutation is a 403 with a distinct body marker.
    // So a probing or misconfigured client is diagnosable. The layer also emits
    // `tracing::warn!(method, path, "...denied mutation (403)")` on each denial
    // (see `enforce_read_only_class`). The test deliberately does NOT assert
    // that warn. `tracing` caches callsite interest globally. Sibling tests hit
    // the callsite with no subscriber and poison it. So the stable body marker
    // is the pinned signal.
    let app = synthetic_enforced_router();
    let mut req = Request::builder()
        .method(Method::POST)
        .uri("/workflows/zzztestparam0/cancel")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(session_role("harvest_readonly"));
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("read-only principal: mutation not permitted"),
        "403 body must carry the distinguishable read-only-denial marker, got: {body}"
    );
}

#[tokio::test]
async fn eris_readonly_denied_on_unclassified_ui_path_through_the_layer() {
    // L4 + the documented /ui limitation: /ui is unclassified, so under
    // fail-closed enforcement a read-only principal is 403'd on it while an
    // admin reaches it. Drives the actual `enforce_read_only_class` LAYER
    // (not just the `classify_route` unit fn) over an /ui route.
    use autumn_web::reexports::axum::routing::get as axum_get;
    let router: Router = Router::new()
        .route("/ui/workflows", axum_get(|| async { StatusCode::OK }))
        .layer(from_fn(enforce_read_only_class));

    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/ui/workflows")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(session_role("harvest_readonly"));
    let res = router.clone().oneshot(req).await.unwrap();
    assert_eq!(
        res.status(),
        StatusCode::FORBIDDEN,
        "read-only principal must be 403'd on unclassified /ui (fail closed)"
    );
    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&bytes).contains("read-only principal: mutation not permitted")
    );

    let admin = status_for(
        &router,
        &Method::GET,
        "/ui/workflows",
        Some(session_role("admin")),
    )
    .await;
    assert_eq!(admin, StatusCode::OK, "admin must reach /ui unchanged");
}

#[tokio::test]
async fn eris_readonly_options_preflight_is_not_denied() {
    // OPTIONS is a preflight/metadata verb, never a mutation. The layer must
    // NOT give 403 to a read-only OPTIONS request, even on a Mutating path. The
    // synthetic router has no OPTIONS handler, so it gives 405. That shows the
    // layer let the request THROUGH.
    let app = synthetic_enforced_router();
    let status = status_for(
        &app,
        &Method::OPTIONS,
        "/workflows/zzztestparam0/cancel",
        Some(session_role("harvest_readonly")),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::FORBIDDEN,
        "OPTIONS must pass the read-only layer (got 403)"
    );
}

// ── Scoped API token layer (issue #942) ──────────────────────────────────────
//
// No-DB tests of the token verification/scope layer wired via
// `from_fn_with_state`. Pass-through paths (absent/non-`hvst_` bearer, OPTIONS)
// never touch the pool, so they hold without a database. A claimed `hvst_`
// bearer with no store must NOT pass through (503). So an unverified token is
// never trusted.

use autumn_harvest_plugin::api_token::enforce_token_scope;
use autumn_web::reexports::axum::middleware::from_fn_with_state;

fn token_layer_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    harvest_api_router(api_state.clone()).layer(from_fn_with_state(api_state, enforce_token_scope))
}

fn get_bearer(uri: &str, bearer: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("authorization", format!("Bearer {bearer}"))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn token_layer_passes_through_absent_bearer() {
    // AC7: with no bearer, the token layer passes through and the
    // embedder/session governs. A read route stays reachable. The test uses
    // `/health`, not `/workflows`. With no pool, `/workflows` fails closed to
    // 503 downstream (issue #756). That would mask the pass-through.
    let app = token_layer_app();
    let res = app.oneshot(get("/health")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn token_layer_passes_through_non_hvst_bearer() {
    // AC7: a bearer that is not a harvest token is ignored — the embedder's own
    // JWT still governs. The token layer must not 401 it. `/health` is used for
    // the same rig reason as above (the no-pool `/workflows` 503 is downstream).
    let app = token_layer_app();
    let res = app
        .oneshot(get_bearer("/health", "eyJhbGciOiJIUzI1NiJ9.embedder.jwt"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn token_layer_passes_through_options_preflight() {
    let app = token_layer_app();
    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri("/workflows/abc/cancel")
        .header("authorization", "Bearer hvst_whatever")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    // OPTIONS is never a mutation; the layer passes it through (no 403/503).
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn token_layer_rejects_hvst_bearer_when_store_unavailable() {
    // A claimed `hvst_` token that cannot be verified (no pool) must NOT pass
    // through. Under the admin boundary, a pass-through is an auth bypass. It
    // gets 503.
    let app = token_layer_app();
    let res = app
        .oneshot(get_bearer("/workflows", "hvst_someclaimedtoken"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn embedder_only_router_has_no_token_layer() {
    // AC7: the default router (no `enable_api_tokens`) is byte-identical to
    // trunk. With no token layer, an `hvst_` bearer passes through untouched
    // to the handler. The test uses `/health`, not `/workflows`. With no pool,
    // `/workflows` fails closed to 503 downstream (issue #756).
    let app = unauthenticated_app();
    let res = app
        .oneshot(get_bearer("/health", "hvst_ignored_here"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// Issue #944: the connector's derived idempotency keys must be unclaimable
/// from outside.
///
/// A `SignalsWithStart` binding derives its key into the
/// `(workflow_exec_id, idempotency_key)` scope on `harvest_signals` that this
/// route writes. Derived keys are *predictable*: Kafka coordinates are
/// `{topic}:{partition}:{offset}`. A caller who lands one first turns the
/// broker's delivery into an idempotent replay. It is acknowledged but never
/// dispatched: silent loss.
///
/// The real router runs with no storage configured. That proves the guard runs
/// at the request boundary, not behind a database read. The test asserts the
/// body too, so an unrelated 400 cannot pass it.
#[tokio::test]
async fn eris_a_reserved_connector_idempotency_key_is_refused_on_the_signal_route() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/workflows/00000000-0000-0000-0000-000000000001/signal/order_event")
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", "conn:L6:orders:L0::L10:orders:0:7")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = autumn_web::reexports::axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .expect("body");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("reserved"),
        "the refusal must name the reservation, not some other 400: {text}"
    );
}

/// The guard must not over-reject. A key that only resembles the reserved
/// prefix cannot alias a derived one, so it gets past the boundary. The
/// uniqueness scope uses Postgres text comparison, where `CONN:` differs from
/// `conn:`. So refusing it would reject a legitimate caller key.
#[tokio::test]
async fn eris_a_near_miss_idempotency_key_is_not_refused_as_reserved() {
    for key in ["CONN:x", "conn", "connector-key", "xconn:y"] {
        let app = unauthenticated_app();
        let res = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/workflows/00000000-0000-0000-0000-000000000001/signal/order_event")
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", key)
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let body = autumn_web::reexports::axum::body::to_bytes(res.into_body(), 64 * 1024)
            .await
            .expect("body");
        let text = String::from_utf8_lossy(&body);
        assert!(
            !text.contains("reserved"),
            "{key} must not be refused as reserved (status {status}): {text}"
        );
    }
}

/// Posts a signal through the real router with no storage configured.
async fn post_signal_without_storage(
    signal_name: &str,
    key: Option<&str>,
    body: &'static str,
) -> (StatusCode, String) {
    let app = unauthenticated_app();
    let mut req = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/workflows/00000000-0000-0000-0000-000000000001/signal/{signal_name}"
        ))
        .header("Content-Type", "application/json");
    if let Some(key) = key {
        req = req.header("Idempotency-Key", key);
    }
    let res = app
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = autumn_web::reexports::axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .expect("body");
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Issue #1985: the signal route applies the promise settlement rules at the
/// request boundary.
///
/// The keyed dedupe fast path reads `harvest_signals` before the insert. A
/// mismatched key that an unrelated signal already used would match that row.
/// The route would then report success, and the promise would stay unsettled.
/// No storage is configured, so a 400 proves that the rules run before any
/// database read.
#[tokio::test]
async fn a_promise_settlement_breaking_the_rules_is_refused_before_the_dedupe_probe() {
    let resolved = r#"{"outcome":"resolved","value":1}"#;
    let cases = [
        (
            "harvest.promise:k",
            Some("unrelated-key"),
            resolved,
            "idempotency key",
        ),
        (
            "harvest.promise:k",
            None,
            r#"{"value":1}"#,
            "not a promise settlement",
        ),
        ("order_event", Some("harvest.promise:k"), "{}", "reserved"),
    ];
    for (signal_name, key, body, needle) in cases {
        let (status, text) = post_signal_without_storage(signal_name, key, body).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{signal_name} with key {key:?} must be refused: {text}"
        );
        assert!(
            text.contains(needle),
            "the refusal must name the promise rule ({needle}): {text}"
        );
    }
}

/// A valid settlement passes the boundary. Without storage, it then fails
/// later, for a reason that is not a promise rule.
#[tokio::test]
async fn a_valid_promise_settlement_passes_the_request_boundary() {
    for key in [None, Some("harvest.promise:k")] {
        let (status, text) = post_signal_without_storage(
            "harvest.promise:k",
            key,
            r#"{"outcome":"rejected","error":"no"}"#,
        )
        .await;
        assert!(
            !text.contains("promise settlement") && !text.contains("idempotency key"),
            "a valid settlement must pass the promise rules (status {status}): {text}"
        );
    }
}

// ── Audit export to a SIEM sink (issue #953) ─────────────────────────────────
//
// Both routes must sit in the SAME auth chain as each other admin-gated route.
// `GET /admin/audit-export` reports the completeness of the privileged-action
// log, which an attacker wants to read before acting. `POST .../redrive`
// mutates a compliance cursor. The assertion is on `401` specifically. A
// `404`/`405` means the route is not registered, and the test passes for the
// wrong reason.

#[tokio::test]
async fn eris_require_auth_blocks_audit_export_status() {
    let app = authenticated_app();
    let res = app.oneshot(get("/admin/audit-export")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_audit_export_redrive() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/audit-export/redrive",
            r#"{"shard": 0, "to_seq": 0}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// Issue #1273: decommission and reactivate mutate the same compliance
// cursor as redrive. One retires it, discarding the compliance guarantee
// over unshipped records; the other resumes it. Both must sit behind the
// same auth gate.

#[tokio::test]
async fn eris_require_auth_blocks_audit_export_decommission() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/audit-export/decommission",
            r#"{"shard": 0}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn eris_require_auth_blocks_audit_export_reactivate() {
    let app = authenticated_app();
    let res = app
        .oneshot(post_json(
            "/admin/audit-export/reactivate",
            r#"{"shard": 0}"#,
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ── Dev-profile unauthenticated management API (issue #1284) ─────────────────
//
// The quickstart's documented Step 3 —
//
//   cargo run -p autumn-harvest-cli -- --base-url http://localhost:3000/api/harvest preflight
//
// — is a bare `reqwest` GET with no cookie, no bearer token and no session. In
// the `dev` profile it must reach `GET /admin/preflight`, not 401.
// `set_deployment_profile` promises that ("`dev` allows an unauthenticated
// local management API").
//
// Two request shapes both have to work, and they are NOT the same shape:
//
//   1. NO `Session` extension at all. A standalone integration that mounts
//      `harvest_api_router` with no autumn-web session layer produces this.
//      These tests produce it by default.
//   2. A FRESH, NON-COOKIE-BACKED `Session`. The real quickstart app produces
//      this. autumn-web always installs its session layer. That layer mints an
//      empty session for a request with no valid cookie. So the quickstart's
//      CLI call arrives as `Some(session)`, not `None`. A fix for shape 1 only
//      passes here and still gives 401 in the quickstart.
//
// The widening is bounded on three sides, each pinned below: `dev` only,
// no-established-session only, and an embedder-declared boundary still wins.

fn get_with_cookieless_session(uri: &str) -> Request<Body> {
    let mut request = get(uri);
    request
        .extensions_mut()
        .insert(Session::new_for_test_without_cookie(
            "harvest-cookieless-session".to_string(),
            HashMap::new(),
        ));
    request
}

fn get_with_session(uri: &str, session_key: &str) -> Request<Body> {
    let mut request = get(uri);
    let mut data = HashMap::new();
    data.insert(session_key.to_string(), "operator-1".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-established-session".to_string(),
        data,
    ));
    request
}

fn dev_profile_app() -> impl tower::Service<
    Request<Body>,
    Response = autumn_web::reexports::axum::response::Response,
    Error = std::convert::Infallible,
    Future = impl std::future::Future,
> + Clone {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    app_with_api_state(api_state)
}

/// AC1, shape 1: the documented preflight command with no session extension.
#[tokio::test]
async fn hermes_dev_profile_admits_preflight_with_no_session_extension() {
    let res = dev_profile_app()
        .oneshot(get("/admin/preflight"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// AC1, shape 2: the quickstart's ACTUAL shape — autumn-web's session layer has
/// already minted an empty, non-cookie-backed session for the cookieless CLI
/// request. This is the assertion the issue's own stated mechanism
/// (`session = None`) would have missed.
#[tokio::test]
async fn hermes_dev_profile_admits_preflight_with_fresh_cookieless_session() {
    let res = dev_profile_app()
        .oneshot(get_with_cookieless_session("/admin/preflight"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// AC1: the fix is the shared admin gate, not a `/admin/preflight` special
/// case — a second admin-gated read route admits the same caller.
#[tokio::test]
async fn hermes_dev_profile_admits_second_admin_route_when_unauthenticated() {
    let res = dev_profile_app()
        .oneshot(get_with_cookieless_session("/admin/shards/health"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// AC3: in `dev`, the configured session key still judges an ESTABLISHED
/// (cookie-backed) session. The widening is for callers with no credential.
/// `set_admin_auth_session_key` keeps its meaning for an embedder's own auth
/// middleware.
#[tokio::test]
async fn hermes_dev_profile_still_requires_configured_key_for_established_session() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(get_with_session("/admin/preflight", "admin_id"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// AC3: and the established-session path still ADMITS the configured key.
#[tokio::test]
async fn hermes_dev_profile_admits_established_session_with_configured_key() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_session_key("operator_id");
    let app = app_with_api_state(api_state);

    let res = app
        .oneshot(get_with_session("/admin/preflight", "operator_id"))
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// AC2: the bypass does not escape `dev`. A named non-dev profile stays
/// fail-closed for both unauthenticated shapes.
#[tokio::test]
async fn hermes_non_dev_profile_rejects_unauthenticated_admin_route() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    let app = app_with_api_state(api_state);

    let res = app.clone().oneshot(get("/admin/preflight")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let res = app
        .oneshot(get_with_cookieless_session("/admin/preflight"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// AC2: the default (`unknown`) profile is non-dev and stays fail-closed. This
/// is the profile a standalone integration gets when it never calls
/// `set_deployment_profile`, so it is the one that must not drift open.
#[tokio::test]
async fn hermes_unknown_profile_rejects_unauthenticated_admin_route() {
    let app = unauthenticated_app();
    let res = app
        .oneshot(get_with_cookieless_session("/admin/preflight"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// AC4: an embedder-declared auth boundary still short-circuits ahead of the
/// profile branch — precedence is unchanged.
#[tokio::test]
async fn hermes_declared_auth_boundary_still_short_circuits_in_dev() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    api_state.set_admin_auth_boundary(true);
    let app = app_with_api_state(api_state);

    let res = app.oneshot(get("/admin/preflight")).await.unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

// ── Fail-closed data-plane mutations (issue #1802) ───────────────────────────
//
// Outside the `dev` profile, a mount with no declared auth boundary must
// refuse every mutating route with 401. The explicit opt-out restores the open
// data plane. Read routes stay open, so this changes verbs, not reads.

fn request_with_session(method: Method, uri: &str, data: &[(&str, &str)]) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let data = data
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect::<HashMap<_, _>>();
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-1802-session".to_string(),
        data,
    ));
    request
}

/// Mutating routes with no admin gate (issue #1802).
const DATA_PLANE_MUTATIONS: &[(&str, &str)] = &[
    ("POST", "/workflows/my-workflow/start"),
    ("POST", "/workflows/my-workflow/signal-with-start"),
    ("POST", "/workflows/my-workflow/update-with-start"),
    (
        "POST",
        "/workflows/00000000-0000-0000-0000-000000000001/signal/approve",
    ),
    (
        "POST",
        "/workflows/00000000-0000-0000-0000-000000000001/reset",
    ),
    (
        "POST",
        "/workflows/00000000-0000-0000-0000-000000000001/update/approve",
    ),
    (
        "POST",
        "/workflows/by-id/my-workflow/order-1/signal/approve",
    ),
    ("POST", "/dags/my-dag/trigger"),
    ("PATCH", "/dags/my-dag"),
    (
        "POST",
        "/dags/my-dag/runs/00000000-0000-0000-0000-000000000001/retry",
    ),
    ("POST", "/admin/schedules/workflow"),
    ("PATCH", "/admin/schedules/sched-1"),
    ("DELETE", "/admin/schedules/sched-1"),
    ("POST", "/admin/schedules/sched-1/pause"),
    ("POST", "/admin/schedules/sched-1/resume"),
    ("POST", "/admin/schedules/sched-1/backfill"),
    (
        "POST",
        "/activities/external/00000000-0000-0000-0000-000000000001/complete",
    ),
    (
        "POST",
        "/activities/external/00000000-0000-0000-0000-000000000001/fail",
    ),
    (
        "POST",
        "/activities/external/00000000-0000-0000-0000-000000000001/heartbeat",
    ),
    ("POST", "/workers/worker-abc/drain"),
];

async fn data_plane_status(api_state: &HarvestApiState, method: &str, uri: &str) -> StatusCode {
    let request = Request::builder()
        .method(method.parse::<Method>().unwrap())
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    harvest_api_router(api_state.clone())
        .oneshot(request)
        .await
        .unwrap()
        .status()
}

/// AC1: with no auth and a non-dev profile, start and signal fail closed. So
/// does every other mutation with no admin gate. Cancel has an admin gate, so
/// the sweep below covers it.
#[tokio::test]
async fn nemesis_unauthenticated_data_plane_mutations_fail_closed() {
    for profile in [None, Some("prod"), Some("staging")] {
        let api_state = HarvestApiState::new();
        if let Some(profile) = profile {
            api_state.set_deployment_profile(profile);
        }
        for (method, uri) in DATA_PLANE_MUTATIONS {
            assert_eq!(
                data_plane_status(&api_state, method, uri).await,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} must fail closed under profile {profile:?}"
            );
        }
    }
}

/// AC1, sweep: no `Mutating` route of the live router admits an anonymous
/// caller. A new route that forgets an admin gate still fails closed.
#[tokio::test]
async fn nemesis_every_classified_mutation_fails_closed_without_auth() {
    let api_state = HarvestApiState::new();
    for (template, class) in CLASSIFIED_ROUTES {
        if *class != RouteClass::Mutating {
            continue;
        }
        let (method, path) = template.split_once(' ').unwrap();
        let status = data_plane_status(&api_state, method, &concrete_request_path(path)).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{template} must fail closed with no auth"
        );
    }
}

/// AC2: the opt-out restores the open data plane.
#[tokio::test]
async fn nemesis_opt_out_restores_unauthenticated_data_plane_mutations() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    api_state.set_allow_unauthenticated_mutations(true);
    for (method, uri) in DATA_PLANE_MUTATIONS {
        let status = data_plane_status(&api_state, method, uri).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
        assert_ne!(status, StatusCode::FORBIDDEN, "{method} {uri}");
    }
}

/// `CLASSIFIED_ROUTES` templates of [`DATA_PLANE_MUTATIONS`], spelled as in
/// `autumn-harvest/src/audit.rs`.
const DATA_PLANE_TEMPLATES: &[&str] = &[
    "POST /workflows/{workflow_name}/start",
    "POST /workflows/{workflow_name}/signal-with-start",
    "POST /workflows/{workflow_name}/update-with-start",
    "POST /workflows/{id}/signal/{signal_name}",
    "POST /workflows/{id}/reset",
    "POST /workflows/{id}/update/{update_name}",
    "POST /workflows/by-id/{workflow_name}/{workflow_id}/signal/{signal_name}",
    "POST /dags/{dag_name}/trigger",
    "PATCH /dags/{dag_name}",
    "POST /dags/{dag_name}/runs/{run_exec_id}/retry",
    "POST /admin/schedules/workflow",
    "PATCH /admin/schedules/{id}",
    "DELETE /admin/schedules/{id}",
    "POST /admin/schedules/{id}/pause",
    "POST /admin/schedules/{id}/resume",
    "POST /admin/schedules/{id}/backfill",
    "POST /activities/external/{token}/complete",
    "POST /activities/external/{token}/fail",
    "POST /activities/external/{token}/heartbeat",
    "POST /workers/{worker_id}/drain",
];

/// The opt-out opens exactly the data plane. Every admin-gated route keeps
/// its gate. A new mutating route with no admin gate fails this test.
#[tokio::test]
async fn nemesis_opt_out_opens_exactly_the_data_plane() {
    assert_eq!(DATA_PLANE_TEMPLATES.len(), DATA_PLANE_MUTATIONS.len());
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    api_state.set_allow_unauthenticated_mutations(true);
    let mut opened = 0;
    for (template, class) in CLASSIFIED_ROUTES {
        if *class != RouteClass::Mutating {
            continue;
        }
        let (method, path) = template.split_once(' ').unwrap();
        let status = data_plane_status(&api_state, method, &concrete_request_path(path)).await;
        if DATA_PLANE_TEMPLATES.contains(template) {
            opened += 1;
            assert_ne!(status, StatusCode::UNAUTHORIZED, "{template} opens");
        } else {
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{template} keeps its admin gate"
            );
        }
    }
    assert_eq!(
        opened,
        DATA_PLANE_TEMPLATES.len(),
        "every data-plane template is a classified mutation"
    );
}

/// The opt-out keeps the handlers' own admin checks on a start that can
/// cancel a live run.
#[tokio::test]
async fn nemesis_opt_out_keeps_the_destructive_start_policy_gates() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    api_state.set_allow_unauthenticated_mutations(true);
    for (uri, body) in [
        (
            "/workflows/my-workflow/start",
            r#"{"reuse_policy":"terminate_if_running"}"#,
        ),
        (
            "/workflows/my-workflow/start",
            r#"{"conflict_policy":"terminate_existing"}"#,
        ),
        (
            "/workflows/my-workflow/signal-with-start",
            r#"{"workflow_id":"w-1","signal_name":"approve","id_reuse_policy":"terminate_if_running"}"#,
        ),
        (
            "/workflows/my-workflow/update-with-start",
            r#"{"workflow_id":"w-1","update_name":"approve","id_reuse_policy":"terminate_if_running"}"#,
        ),
    ] {
        let res = harvest_api_router(api_state.clone())
            .oneshot(post_json(uri, body))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "{uri} {body}");
    }
}

/// The `dev` profile leaves the mutating routes open.
#[tokio::test]
async fn nemesis_dev_profile_keeps_data_plane_mutations_open() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("dev");
    for (method, uri) in DATA_PLANE_MUTATIONS {
        let status = data_plane_status(&api_state, method, uri).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
    }
}

/// A declared auth boundary hands the decision to the embedder's middleware.
#[tokio::test]
async fn nemesis_declared_boundary_keeps_data_plane_mutations_open() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    api_state.set_admin_auth_boundary(true);
    for (method, uri) in DATA_PLANE_MUTATIONS {
        let status = data_plane_status(&api_state, method, uri).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
    }
}

/// An admin session passes the gate in a non-dev profile. A plain session
/// does not.
#[tokio::test]
async fn nemesis_admin_session_passes_and_plain_session_fails() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    let app = harvest_api_router(api_state);

    let admin = app
        .clone()
        .oneshot(request_with_session(
            Method::POST,
            "/workflows/my-workflow/start",
            &[("user_id", "u-1"), ("role", "admin")],
        ))
        .await
        .unwrap();
    assert_ne!(admin.status(), StatusCode::UNAUTHORIZED);

    let plain = app
        .oneshot(request_with_session(
            Method::POST,
            "/workflows/my-workflow/start",
            &[("user_id", "u-1")],
        ))
        .await
        .unwrap();
    assert_eq!(plain.status(), StatusCode::UNAUTHORIZED);
}

/// The gate runs only on a matched route. An unknown path stays 404.
#[tokio::test]
async fn nemesis_unknown_path_is_still_not_found() {
    let res = closed_app()
        .oneshot(post_json("/no-such-route", "{}"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

/// Vantage forms post to their own `/ui` routes. Those mutations fail closed
/// too. The pages stay readable.
#[tokio::test]
async fn nemesis_vantage_mutations_fail_closed_and_pages_stay_open() {
    let app = unauthenticated_app_with_ui();
    for uri in VANTAGE_DATA_PLANE_POSTS {
        let res = app.clone().oneshot(post_json(uri, "{}")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "POST {uri}");
    }
    let page = app.oneshot(get("/ui/workflows")).await.unwrap();
    assert_ne!(page.status(), StatusCode::UNAUTHORIZED);
}

/// Vantage form posts with no admin gate (`ui.rs`).
const VANTAGE_DATA_PLANE_POSTS: &[&str] = &[
    "/ui/workflows/00000000-0000-0000-0000-000000000001/cancel",
    "/ui/workflows/00000000-0000-0000-0000-000000000001/pause",
    "/ui/workflows/00000000-0000-0000-0000-000000000001/resume",
    "/ui/workflows/00000000-0000-0000-0000-000000000001/signal",
    "/ui/workflows/00000000-0000-0000-0000-000000000001/reset",
    "/ui/workflows/00000000-0000-0000-0000-000000000001/trigger-update",
    "/ui/schedules/bulk-pause",
    "/ui/schedules/bulk-resume",
    "/ui/schedules/sched-1/pause",
    "/ui/schedules/sched-1/resume",
    "/ui/schedules/sched-1/delete",
    "/ui/schedules/sched-1/trigger-now",
    "/ui/schedules/sched-1/backfill",
];

/// An admin session passes the Vantage gate outside `dev`. A plain session
/// does not.
#[tokio::test]
async fn nemesis_admin_session_passes_the_vantage_gate_outside_dev() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    let app = harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state));
    let uri = "/ui/workflows/00000000-0000-0000-0000-000000000001/cancel";
    let admin = app
        .clone()
        .oneshot(request_with_session(
            Method::POST,
            uri,
            &[("user_id", "u-1"), ("role", "admin")],
        ))
        .await
        .unwrap();
    assert_ne!(admin.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(admin.status(), StatusCode::FORBIDDEN);
    let plain = app
        .oneshot(request_with_session(
            Method::POST,
            uri,
            &[("user_id", "u-1")],
        ))
        .await
        .unwrap();
    assert_eq!(plain.status(), StatusCode::UNAUTHORIZED);
}

/// The opt-out also restores the Vantage mutations.
#[tokio::test]
async fn nemesis_opt_out_restores_vantage_mutations() {
    let api_state = HarvestApiState::new();
    api_state.set_allow_unauthenticated_mutations(true);
    let app = harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state));
    for uri in VANTAGE_DATA_PLANE_POSTS {
        let res = app.clone().oneshot(post_json(uri, "{}")).await.unwrap();
        assert_ne!(res.status(), StatusCode::UNAUTHORIZED, "POST {uri}");
    }
}

/// An `OPTIONS` preflight is not a mutation, so the gate never answers it.
#[tokio::test]
async fn nemesis_options_preflight_is_not_refused_by_the_gate() {
    for uri in [
        "/workflows/my-workflow/start",
        "/workflows/00000000-0000-0000-0000-000000000001/cancel",
    ] {
        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri(uri)
            .header("Origin", "https://app.example.com")
            .header("Access-Control-Request-Method", "POST")
            .body(Body::empty())
            .unwrap();
        let res = closed_app().oneshot(request).await.unwrap();
        assert_ne!(res.status(), StatusCode::UNAUTHORIZED, "OPTIONS {uri}");
    }
}
