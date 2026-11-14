//! No-database HTTP tests for the effective-config introspection endpoint
//! (issue #695).
//!
//! `GET /admin/config` reads only the in-process snapshot on the installed
//! `HarvestApiRuntime`, with no database. These tests drive the real
//! `harvest_api_router`, `require_admin` layer included, against
//! `AppState::for_test()` via `tower`, like the `security.rs` tests.
//!
//! The snapshot lives *on* the runtime. The shared `HarvestRunner::start`
//! bring-up attaches it for the web-app path and the standalone runner. So an
//! installed runtime is enough to serve the endpoint. These tests install the
//! runtime with no `set_effective_config` call, as the standalone runner does
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
    harvest_api_router(api_state)
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
        Duration::from_hours(2160),
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
        None,
        None,
    )
}

/// Install a minimal, DB-free runtime carrying `view`. This mirrors the
/// standalone runner's `api_state.install(runner.api_runtime())` shape, with no
/// `set_effective_config` call (issue #695).
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
    // Admin-authorized, but the runtime was never installed. The endpoint gates
    // on `runtime()`, so an un-started deployment must fail closed with the
    // standard "runtime not started" 400, never 200.
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
async fn effective_config_fails_closed_when_runtime_has_no_captured_config() {
    // Regression for PR #987: `HarvestApiRuntime::new` is a *public*
    // constructor. A runtime installed through it, without the runner's
    // `with_effective_config` call, carries no snapshot. For an incident-triage
    // endpoint, fabricated defaults are worse than an honest error. So the
    // handler must fail closed (400), not serve a 200 placeholder.
    let api_state = HarvestApiState::new();
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
    let app = app_with_api_state(api_state);

    let res = app.oneshot(get_as_admin("/admin/config")).await.unwrap();
    // Admin-authorized, and a runtime *is* installed, so this is not the
    // "runtime not started" case. The response is still the fail-closed 400.
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // The body must NOT be a fabricated config: no config sections present.
    let bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&bytes)
        && let Some(obj) = json.as_object()
    {
        for key in [
            "worker",
            "payload_caps",
            "shard_topology",
            "features",
            "pool",
        ] {
            assert!(
                !obj.contains_key(key),
                "fail-closed response must not carry a fabricated config section: {key}"
            );
        }
    }
}

#[tokio::test]
async fn effective_config_happy_path_returns_secret_free_view() {
    let api_state = HarvestApiState::new();
    // The endpoint gates on an installed `runtime()`. This installs one that
    // carries the sample view.
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
