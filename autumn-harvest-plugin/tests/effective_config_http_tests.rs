//! No-database HTTP tests for the effective-config introspection endpoint
//! (issue #695).
//!
//! `GET /admin/config` reads only the in-process snapshot carried by the
//! installed `HarvestApiRuntime` — no database — so these drive the real
//! `harvest_api_router` (including its `require_admin` route layer) against
//! `AppState::for_test()` via `tower`, mirroring the `security.rs` pattern.
//!
//! Since the snapshot lives *on* the runtime (attached by the shared
//! `HarvestRunner::start` bring-up seam that both the plugin web-app path and
//! the standalone runner funnel through), installing a runtime is the only step
//! needed to serve the endpoint — there is no separate `set_effective_config`
//! call. These tests deliberately install the runtime *without* any such call,
//! exercising the exact install shape the standalone runner uses
//! (`api_state.install(runner.api_runtime())`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::effective_config::{EffectiveConfigView, PayloadCapsView, PoolConfigView};
use autumn_harvest::retention::RetentionConfig;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum::body::Body;
use autumn_web::reexports::http::{Method, Request, StatusCode};
use autumn_web::session::Session;
use tower::ServiceExt;

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

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

/// A GET request carrying an admin session (role = admin) so it passes the
/// built-in `require_harvest_admin` guard in a non-dev profile.
fn get_as_admin(uri: &str) -> Request<Body> {
    let mut request = get(uri);
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "operator-1".to_string());
    data.insert("role".to_string(), "admin".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));
    request
}

/// Build a representative snapshot whose worker config carries a secret-bearing
/// notification URL, so a happy-path response can be asserted to redact it.
fn sample_view() -> EffectiveConfigView {
    let worker = WorkerConfig {
        notification_database_url: Some("postgres://user:hunter2@dbhost:5432/harvest".to_string()),
        ..Default::default()
    };
    let caps = PayloadCapsView::new(
        2 * 1024 * 1024,
        2 * 1024 * 1024,
        256 * 1024,
        2 * 1024 * 1024,
        1024,
        None,
        None,
        None,
        Duration::from_secs(90 * 24 * 3600),
        10_000,
        false,
        262_144,
    );
    EffectiveConfigView::capture(
        &worker,
        caps,
        &ShardRouter::single(),
        PoolConfigView {
            worker_pool_max_connections: 10,
            shard_pool_count: 1,
        },
        Duration::from_millis(500),
    )
}

/// Install a minimal, DB-free runtime carrying `view` — mirroring the standalone
/// runner's `api_state.install(runner.api_runtime())` shape, where the snapshot
/// rides on the runtime and no separate `set_effective_config` call is made
/// (issue #695). The runtime shape otherwise mirrors what the crate's internal
/// tests build.
fn install_runtime_with_view(api_state: &HarvestApiState, view: EffectiveConfigView) {
    api_state.install(
        HarvestApiRuntime::new(
            Arc::new(HandlerRegistry::new(vec![], vec![])),
            Arc::new(DagCatalog::default()),
            Arc::new(Vec::new()),
            None,
            Vec::new(),
            SchedulerMonitor::offline(),
            HarvestRetentionRuntime::disabled(RetentionConfig::default()),
            ShardRouter::single(),
        )
        .with_effective_config(view),
    );
}

#[tokio::test]
async fn eris_unauthenticated_effective_config_is_blocked() {
    let app = app_with_api_state(HarvestApiState::new());
    let res = app.oneshot(get("/admin/config")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn effective_config_fails_closed_when_runtime_not_installed() {
    // Admin-authorized, but the runtime was never installed: the endpoint gates
    // on `runtime()`, so a genuinely un-started deployment must fail closed with
    // the standard "runtime not started" 400 — never 200 (P2#1 readiness gate).
    let app = app_with_api_state(HarvestApiState::new());
    let res = app.oneshot(get_as_admin("/admin/config")).await.unwrap();
    assert_ne!(res.status(), StatusCode::OK);
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn effective_config_served_from_installed_runtime_without_a_set_call() {
    // Regression for the PR #987 review P2: the standalone-runner deployment
    // shape starts a `HarvestRunner` and installs its runtime
    // (`api_state.install(runner.api_runtime())`) — it never calls
    // `set_effective_config`. The snapshot now rides on the runtime, so this
    // install-only shape must serve a populated 200. (There is no
    // `set_effective_config` to call — the footgun is structurally gone.)
    let api_state = HarvestApiState::new();
    install_runtime_with_view(&api_state, sample_view());
    let app = app_with_api_state(api_state);

    let res = app.oneshot(get_as_admin("/admin/config")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // Populated body: the snapshot attached to the runtime (not a placeholder).
    assert_eq!(json["pool"]["worker_pool_max_connections"], 10);
    assert_eq!(json["worker"]["poll_interval_ms"], 500);
}

#[tokio::test]
async fn effective_config_happy_path_returns_secret_free_view() {
    let api_state = HarvestApiState::new();
    // The snapshot rides on the installed runtime; the endpoint gates on
    // `runtime()` being installed (P2#1), which this satisfies by installing a
    // runtime carrying the sample view.
    install_runtime_with_view(&api_state, sample_view());
    let app = app_with_api_state(api_state);

    let res = app.oneshot(get_as_admin("/admin/config")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();

    // No secret fragment can appear anywhere in the serialized body.
    assert!(!text.contains("hunter2"), "leaked password: {text}");
    assert!(!text.contains("dbhost"), "leaked host: {text}");
    assert!(!text.contains("postgres://"), "leaked URL scheme: {text}");

    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for key in [
        "worker",
        "payload_caps",
        "shard_topology",
        "features",
        "pool",
    ] {
        assert!(
            json.as_object().unwrap().contains_key(key),
            "missing top-level key: {key}"
        );
    }
    assert!(
        json["worker"]["notification_channel_configured"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(json["worker"]["poll_interval_ms"], 500);
    assert_eq!(json["pool"]["worker_pool_max_connections"], 10);
}
