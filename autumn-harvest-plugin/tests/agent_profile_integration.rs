//! HTTP integration tests for the agent profile overlay routes (issue
//! #2374).
//!
//! Each test maps to one acceptance test of #2374. The routes run behind
//! the role layer in the `prod` profile, as an operator mounts them. A test
//! middleware gives each request the role that the test names. The audit
//! rows land in a Postgres container.

use std::collections::HashMap;
use std::sync::Arc;

use autumn_harvest::RetentionConfig;
use autumn_harvest::audit_export::{claim_shard, ensure_cursor_row, serialize_batch};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_agent::{
    InMemoryOverlayStore, OverlayStoreError, OverlayValidator, OverlayVersion, ProfileOverlay,
    ProfileOverlayStore, Provenance,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, StandaloneAdminAuth,
    harvest_api_router,
};
use autumn_harvest_plugin::roles::{
    HarvestRoles, ROLE_DENIED_ERROR, ROLE_OPERATOR, ROLE_VIEWER, SESSION_ROLES_KEY,
};
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const KIND: &str = "support";
const ROLE_HEADER: &str = "x-test-role";

/// A validator that knows the kind `support` and accepts each overlay.
struct AnyOverlay;

impl OverlayValidator for AnyOverlay {
    fn validate(&self, kind: &str, _overlay: &ProfileOverlay) -> Result<(), OverlayStoreError> {
        if kind == KIND {
            Ok(())
        } else {
            Err(OverlayStoreError::UnknownKind {
                kind: kind.to_owned(),
            })
        }
    }
}

struct Fixture {
    app: axum::Router,
    store: Arc<InMemoryOverlayStore>,
    url: String,
    _container: ContainerAsync<Postgres>,
}

async fn database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        container,
    )
}

fn pool(url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// The API behind the role layer. The header [`ROLE_HEADER`] picks the
/// session role of a request. `with_store` installs the overlay store.
async fn fixture(with_store: bool) -> Fixture {
    let (url, container) = database().await;
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool(&url)));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("api-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let store = Arc::new(InMemoryOverlayStore::new(Arc::new(AnyOverlay)));
    if with_store {
        api_state.set_agent_overlay_store(Some(store.clone()));
    }
    let roles = HarvestRoles::builder()
        .builtin_roles()
        .build()
        .expect("roles");
    let mounted = StandaloneAdminAuth::new()
        .with_deployment_profile("prod")
        .with_roles(roles)
        .mount(harvest_api_router(api_state.clone()), &api_state)
        .layer(axum::middleware::from_fn(
            |mut request: Request, next: Next| async move {
                let role = request
                    .headers()
                    .get(ROLE_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                if let Some(role) = role {
                    let data = HashMap::from([(SESSION_ROLES_KEY.to_string(), role)]);
                    request
                        .extensions_mut()
                        .insert(Session::new_for_test("roles".to_string(), data));
                }
                next.run(request).await
            },
        ));
    Fixture {
        app: mounted,
        store,
        url,
        _container: container,
    }
}

async fn call(
    app: &axum::Router,
    role: &str,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    call_as(app, "alice@example.com", role, method, uri, body).await
}

async fn call_as(
    app: &axum::Router,
    actor: &str,
    role: &str,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header(ROLE_HEADER, role)
        .header("x-harvest-actor", actor);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("served");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, json)
}

fn activate_body(version: u64, expected: Option<u64>) -> Value {
    json!({ "version": version, "expected_active": expected })
}

async fn activate(app: &axum::Router, version: u64, expected: Option<u64>) -> (StatusCode, Value) {
    call(
        app,
        ROLE_OPERATOR,
        "POST",
        "/agents/support/profile/activate",
        Some(activate_body(version, expected)),
    )
    .await
}

async fn rollback(app: &axum::Router) -> (StatusCode, Value) {
    call(
        app,
        ROLE_OPERATOR,
        "POST",
        "/agents/support/profile/rollback",
        Some(json!({})),
    )
    .await
}

fn prompt(system: &str) -> ProfileOverlay {
    ProfileOverlay {
        system: Some(system.to_owned()),
        max_steps: Some(4),
        ..ProfileOverlay::default()
    }
}

/// Put v1 and v2 with prompts that differ only in `system`.
async fn two_versions(store: &InMemoryOverlayStore) -> (OverlayVersion, OverlayVersion) {
    let ops = || Provenance::operator("ops");
    let v1 = store.put(KIND, &prompt("One."), ops()).await.expect("v1");
    let v2 = store.put(KIND, &prompt("Two."), ops()).await.expect("v2");
    (v1, v2)
}

async fn active(store: &InMemoryOverlayStore) -> Option<u64> {
    store
        .active(KIND)
        .await
        .expect("active")
        .map(|stored| stored.version().get())
}

fn error_text(body: &Value) -> String {
    body.to_string()
}

/// AC 1, the route half: `rollback` after v1 then v2 makes v1 active. The
/// agent suite `a_rollback_restores_v1` shows that the next op then sees
/// v1's prompt. A second rollback restores v2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_after_v1_then_v2_makes_v1_active() {
    let fx = fixture(true).await;
    two_versions(&fx.store).await;
    assert_eq!(activate(&fx.app, 1, None).await.0, StatusCode::OK);
    assert_eq!(activate(&fx.app, 2, Some(1)).await.0, StatusCode::OK);

    let (status, body) = rollback(&fx.app).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"kind": KIND, "from": 2, "to": 1, "actor": "alice@example.com"})
    );
    assert_eq!(active(&fx.store).await, Some(1));
    let last = fx.store.last_activation(KIND).await.expect("last");
    let last = last.expect("a move");
    assert_eq!(last.actor(), &Provenance::operator("alice@example.com"));

    let (status, body) = rollback(&fx.app).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(active(&fx.store).await, Some(2), "a second rollback undoes");
}

/// AC 2: `rollback` with no earlier activation is a `409` with a message,
/// and changes nothing. The audit row records the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_with_no_earlier_activation_changes_nothing() {
    let fx = fixture(true).await;
    two_versions(&fx.store).await;
    let (status, body) = rollback(&fx.app).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        error_text(&body).contains("no earlier activation"),
        "{body}"
    );
    assert_eq!(active(&fx.store).await, None);

    assert_eq!(activate(&fx.app, 1, None).await.0, StatusCode::OK);
    let before = fx.store.last_activation(KIND).await.expect("last");
    let (status, body) = rollback(&fx.app).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(active(&fx.store).await, Some(1));
    assert_eq!(fx.store.last_activation(KIND).await.expect("last"), before);
}

/// AC 3: a diff of two versions that differ only in `system` holds only
/// `system`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_of_a_system_change_holds_only_system() {
    let fx = fixture(true).await;
    two_versions(&fx.store).await;
    let (status, body) = call(
        &fx.app,
        ROLE_VIEWER,
        "GET",
        "/agents/support/profile/diff?a=1&b=v2",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "kind": KIND,
            "a": 1,
            "b": 2,
            "changes": [{"path": ["system"], "a": "One.", "b": "Two."}]
        })
    );
    let (status, _) = call(
        &fx.app,
        ROLE_VIEWER,
        "GET",
        "/agents/support/profile/diff?a=1&b=9",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(
        &fx.app,
        ROLE_VIEWER,
        "GET",
        "/agents/support/profile/diff?a=1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// AC 4: `activate` and `rollback` without the operator role are `403`,
/// and `active` is unchanged. The viewer still reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_viewer_cannot_activate_or_roll_back() {
    let fx = fixture(true).await;
    two_versions(&fx.store).await;
    for (uri, body) in [
        ("/agents/support/profile/activate", activate_body(1, None)),
        ("/agents/support/profile/rollback", json!({})),
    ] {
        let (status, body) = call(&fx.app, ROLE_VIEWER, "POST", uri, Some(body)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
        assert!(error_text(&body).contains(ROLE_DENIED_ERROR), "{body}");
    }
    assert_eq!(active(&fx.store).await, None);
    assert_eq!(fx.store.last_activation(KIND).await.expect("last"), None);

    let (status, body) = call(
        &fx.app,
        ROLE_VIEWER,
        "GET",
        "/agents/support/profile/versions",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// AC 5: two concurrent `activate` calls over the same pointer. One wins,
/// and the other is a `409` that names the actual active version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_activates_have_one_winner() {
    let fx = fixture(true).await;
    two_versions(&fx.store).await;
    fx.store
        .put(KIND, &prompt("Three."), Provenance::operator("ops"))
        .await
        .expect("v3");
    assert_eq!(activate(&fx.app, 1, None).await.0, StatusCode::OK);
    for _ in 0..4 {
        let expected = active(&fx.store).await;
        let targets: Vec<u64> = [1, 2, 3]
            .into_iter()
            .filter(|v| Some(*v) != expected)
            .collect();
        let (left, right) = tokio::join!(
            activate(&fx.app, targets[0], expected),
            activate(&fx.app, targets[1], expected),
        );
        let (winner, loser) = match (left.0, right.0) {
            (StatusCode::OK, StatusCode::CONFLICT) => (targets[0], right.1),
            (StatusCode::CONFLICT, StatusCode::OK) => (targets[1], left.1),
            other => panic!("want one winner: {other:?} {left:?} {right:?}"),
        };
        assert!(
            error_text(&loser).contains(&format!("the active overlay is v{winner}")),
            "{loser}"
        );
        assert_eq!(active(&fx.store).await, Some(winner));
    }
}

/// AC 6: each activation and rollback is in the audit export, with the
/// kind, both ends and the operator id. A refused one is a failed row, and
/// an activation of the active version records that nothing moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_activation_is_in_the_audit_export() {
    let fx = fixture(true).await;
    two_versions(&fx.store).await;
    assert_eq!(rollback(&fx.app).await.0, StatusCode::CONFLICT);
    assert_eq!(activate(&fx.app, 1, None).await.0, StatusCode::OK);
    assert_eq!(activate(&fx.app, 2, Some(1)).await.0, StatusCode::OK);
    assert_eq!(rollback(&fx.app).await.0, StatusCode::OK);
    assert_eq!(activate(&fx.app, 2, None).await.0, StatusCode::CONFLICT);
    assert_eq!(activate(&fx.app, 1, Some(1)).await.0, StatusCode::OK);

    let mut conn = AsyncPgConnection::establish(&fx.url)
        .await
        .expect("connect");
    ensure_cursor_row(&mut conn, 0).await.expect("cursor");
    let claim = claim_shard(
        &mut conn,
        0,
        100,
        std::time::Duration::from_secs(0),
        chrono::Utc::now(),
    )
    .await
    .expect("claim")
    .expect("rows to export");
    let body = serialize_batch(&claim.records).expect("serialize");
    let rows: Vec<Value> = String::from_utf8(body)
        .expect("utf8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("json line"))
        .filter(|row: &Value| {
            row["operation"]
                .as_str()
                .is_some_and(|op| op.starts_with("agent_profile."))
        })
        .collect();
    let seen: Vec<(&str, &str, &str, &str, &str)> = rows
        .iter()
        .map(|row| {
            (
                row["operation"].as_str().unwrap_or_default(),
                row["status"].as_str().unwrap_or_default(),
                row["target_id"].as_str().unwrap_or_default(),
                row["actor"].as_str().unwrap_or_default(),
                row["error_summary"].as_str().unwrap_or_default(),
            )
        })
        .collect();
    let alice = "alice@example.com";
    let (operation, status, target, actor, error) = seen[0];
    assert_eq!(
        (operation, status, target, actor),
        ("agent_profile.rollback", "failed", KIND, alice)
    );
    assert!(error.contains("no earlier activation"), "{error}");
    assert_eq!(
        seen[1..4],
        [
            (
                "agent_profile.activate",
                "succeeded",
                KIND,
                alice,
                "from unset to v1"
            ),
            (
                "agent_profile.activate",
                "succeeded",
                KIND,
                alice,
                "from v1 to v2"
            ),
            (
                "agent_profile.rollback",
                "succeeded",
                KIND,
                alice,
                "from v2 to v1"
            ),
        ]
    );
    assert_eq!(seen.len(), 6, "{seen:?}");
    assert_eq!(
        seen[5],
        (
            "agent_profile.activate",
            "succeeded",
            KIND,
            alice,
            "unchanged at v1"
        )
    );
    let (operation, status, target, actor, error) = seen[4];
    assert_eq!(
        (operation, status, target, actor),
        ("agent_profile.activate", "failed", KIND, alice)
    );
    assert!(error.contains("the active overlay is v1"), "{error}");
    for row in &rows {
        assert_eq!(row["target_type"], "agent_profile");
    }
}

/// `list` marks the active version, and `show` returns one overlay. A bad
/// or missing version is `400` or `404`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_marks_the_active_version_and_show_returns_the_overlay() {
    let fx = fixture(true).await;
    let (status, body) = call(&fx.app, ROLE_VIEWER, "GET", "/agents/support/profile", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "no active version: {body}");
    two_versions(&fx.store).await;
    assert_eq!(activate(&fx.app, 2, None).await.0, StatusCode::OK);

    let (status, body) = call(
        &fx.app,
        ROLE_VIEWER,
        "GET",
        "/agents/support/profile/versions",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], KIND);
    assert_eq!(body["active"], 2);
    let versions = body["versions"].as_array().expect("versions");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0]["version"], 1);
    assert_eq!(versions[0]["active"], false);
    assert_eq!(versions[1]["active"], true);
    assert_eq!(
        versions[0]["provenance"],
        json!({"author": "operator", "id": "ops"})
    );
    assert!(versions[0]["created_at"].is_string());

    let (status, body) = call(
        &fx.app,
        ROLE_VIEWER,
        "GET",
        "/agents/support/profile/versions/1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["overlay"], json!({"system": "One.", "max_steps": 4}));
    assert_eq!(body["active"], false);
    assert_eq!(body["digest"], prompt("One.").digest());

    let (status, body) = call(&fx.app, ROLE_VIEWER, "GET", "/agents/support/profile", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["version"], 2);
    assert_eq!(body["active"], true);

    for (uri, want) in [
        (
            "/agents/support/profile/versions/0",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/agents/support/profile/versions/x",
            StatusCode::BAD_REQUEST,
        ),
        ("/agents/support/profile/versions/9", StatusCode::NOT_FOUND),
    ] {
        let (status, body) = call(&fx.app, ROLE_VIEWER, "GET", uri, None).await;
        assert_eq!(status, want, "{uri}: {body}");
    }
}

/// `expected_active` is required, and a bad body changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_activate_must_state_the_expected_pointer() {
    let fx = fixture(true).await;
    two_versions(&fx.store).await;
    for body in [
        json!({"version": 1}),
        json!({"version": 0, "expected_active": null}),
        json!({"version": 1, "expected_active": null, "force": true}),
    ] {
        let (status, reply) = call(
            &fx.app,
            ROLE_OPERATOR,
            "POST",
            "/agents/support/profile/activate",
            Some(body.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {reply}");
    }
    let (status, _) = activate(&fx.app, 9, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "an unknown version");
    let (status, body) = call(
        &fx.app,
        ROLE_OPERATOR,
        "POST",
        "/agents/other/profile/activate",
        Some(activate_body(1, None)),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(active(&fx.store).await, None);
}

/// With no store, each route is a `404` that names the builder method.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_route_is_404_without_a_store() {
    let fx = fixture(false).await;
    for (method, uri, body) in [
        ("GET", "/agents/support/profile", None),
        ("GET", "/agents/support/profile/versions", None),
        ("GET", "/agents/support/profile/versions/1", None),
        ("GET", "/agents/support/profile/diff?a=1&b=2", None),
        (
            "POST",
            "/agents/support/profile/activate",
            Some(activate_body(1, None)),
        ),
        ("POST", "/agents/support/profile/rollback", Some(json!({}))),
    ] {
        let (status, reply) = call(&fx.app, ROLE_OPERATOR, method, uri, body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {reply}");
        assert!(
            error_text(&reply).contains("agent_overlay_store"),
            "{uri}: {reply}"
        );
    }
}

/// A padded operator id is a `400` with a failed audit row, and the
/// pointer does not move.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bad_operator_id_is_refused_and_audited() {
    let fx = fixture(true).await;
    two_versions(&fx.store).await;
    let (status, body) = call_as(
        &fx.app,
        " alice",
        ROLE_OPERATOR,
        "POST",
        "/agents/support/profile/activate",
        Some(activate_body(1, None)),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(error_text(&body).contains("operator id"), "{body}");
    assert_eq!(active(&fx.store).await, None);

    let mut conn = AsyncPgConnection::establish(&fx.url)
        .await
        .expect("connect");
    let rows = autumn_harvest::audit::list_audit(
        &mut conn,
        &autumn_harvest::audit::AuditFilters {
            actor: None,
            operation: Some("agent_profile.activate".to_owned()),
            target_type: None,
            target_id: None,
            status: None,
            since: None,
            before: None,
            before_id: None,
            limit: 50,
        },
    )
    .await
    .expect("audit rows");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].status, "failed");
}
