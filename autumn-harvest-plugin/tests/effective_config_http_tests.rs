//! No-database HTTP tests for the effective-config introspection endpoint
//! (issue #695).
//!
//! `GET /admin/config` reads only the in-process snapshot on `HarvestApiState`
//! — no database — so these drive the real `harvest_api_router` (including its
//! `require_admin` route layer) against `AppState::for_test()` via `tower`,
//! mirroring the `security.rs` pattern. The full plugin-wired capture at
//! startup is exercised by the testcontainers integration test.

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

/// Install a minimal, DB-free runtime so `HarvestApiState::runtime()` succeeds —
/// the readiness gate the handler now requires before serving the snapshot
/// (issue #695 review, P2). Mirrors the runtime shape the crate's internal
/// tests build.
fn install_minimal_runtime(api_state: &HarvestApiState) {
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        None,
        Vec::new(),
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    ));
}

#[tokio::test]
async fn eris_unauthenticated_effective_config_is_blocked() {
    let app = app_with_api_state(HarvestApiState::new());
    let res = app.oneshot(get("/admin/config")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn effective_config_fails_closed_when_snapshot_not_installed() {
    // Admin-authorized, but the runtime never captured a snapshot: must NOT 200.
    let app = app_with_api_state(HarvestApiState::new());
    let res = app.oneshot(get_as_admin("/admin/config")).await.unwrap();
    assert_ne!(res.status(), StatusCode::OK);
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn effective_config_snapshot_present_but_runtime_not_installed_fails_closed() {
    // Regression for the issue #695 review P2: the snapshot is captured early in
    // plugin startup, before the runtime is installed. A present snapshot must
    // NOT, on its own, make the endpoint serve a 200 — otherwise a request in the
    // startup window (or after a startup step errored before `install`) would see
    // config for a fleet that is not running. The runtime-not-installed gate must
    // fail closed even though the snapshot is present.
    let api_state = HarvestApiState::new();
    api_state.set_effective_config(sample_view());
    // Deliberately do NOT install a runtime.
    let app = app_with_api_state(api_state);

    let res = app.oneshot(get_as_admin("/admin/config")).await.unwrap();
    assert_ne!(res.status(), StatusCode::OK);
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn effective_config_happy_path_returns_secret_free_view() {
    let api_state = HarvestApiState::new();
    api_state.set_effective_config(sample_view());
    // The handler gates on the runtime being installed (issue #695 review, P2),
    // so a happy-path 200 requires an installed runtime alongside the snapshot.
    install_minimal_runtime(&api_state);
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
