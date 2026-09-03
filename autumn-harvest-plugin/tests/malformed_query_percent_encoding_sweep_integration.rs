//! Sweep regression test for issue #1151: every management API route that
//! consumes a raw `(key, value)` query-string pair list must reject a
//! malformed percent-encoded byte sequence with a genuine `400` JSON error,
//! never axum's built-in `Query<Vec<(String, String)>>` lossy fallback
//! (silently substituting `U+FFFD` and returning `200` with a
//! legitimate-looking but wrong filter value — see issue #774's original
//! finding for `GET /admin/queue-coverage`, and issue #1151 for the sweep
//! across the other 17 call sites that had the same gap).
//!
//! Every route fixed here decodes the raw query string as the very first
//! statement in its handler body, *before* any path-parameter validation or
//! database access (issue #1151 review: reordered so behavior does not
//! depend on path validity) — so this suite needs no database, no
//! testcontainer, and no admin session: a router built with no storage pool
//! installed is enough to prove the malformed-query `400` fires before
//! anything downstream would need one. `?queue_name=%FF` (an invalid
//! standalone UTF-8 byte) is the exact repro from the issue #774 review
//! comment.
//!
//! Two of the 19 originally-vulnerable call sites are deliberately absent
//! from this table: `GET /workflows/by-id/{workflow_name}/{workflow_id}/result`
//! and `.../children` resolve the business id to an execution id (a real
//! database lookup) *before* forwarding the raw query string unchanged to
//! `get_workflow_result` / `list_workflow_children` — the two routes that
//! *are* in this table. Business-id resolution must happen first there since
//! the delegate needs a shard-routable execution id, so those two wrappers
//! cannot be proven with a poolless router; they inherit the fix
//! transitively (same delegate, same decode call) once the delegate itself
//! is proven here.

use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn build_app() -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    harvest_api_router(api_state).with_state(autumn_web::AppState::for_test())
}

/// Every route's `400` body carries [`crate::strict_query::MALFORMED_QUERY_MESSAGE`]
/// (asserted by substring match, not full-body equality), but not in the same
/// JSON shape: `GET /admin/queue-coverage` keeps its original, already-shipped
/// `{"error": "..."}` shape (issue #774), while every other route -- whose
/// *other* invalid-param `400`s are already `AutumnError`-shaped -- wraps the
/// same message in `AutumnError`'s RFC-7807-flavored `{"detail": "...", ...}`
/// body instead, so a route's malformed-query `400` never introduces a SECOND,
/// inconsistent error shape alongside its own other `400`s (issue #1151
/// review). See `strict_query.rs`'s `decode_or_bad_request` vs.
/// `decode_or_autumn_error_response` doc comments for the full rationale.
fn assert_malformed_query_body(name: &str, body: &Value) {
    let message = body
        .get("error")
        .or_else(|| body.get("detail"))
        .and_then(Value::as_str);
    assert_eq!(
        message,
        Some("malformed query string: invalid percent-encoded UTF-8"),
        "{name}: unexpected error body {body:?}"
    );
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    let json = serde_json::from_slice(&body).expect("response must be JSON, not text/plain");
    (status, json)
}

/// `(route template with a syntactically-arbitrary path segment already
/// substituted in, human-readable name)`. The path segment value never
/// matters for this suite — the malformed query 400 fires before any path
/// validation runs — but each template still needs *a* value to produce a
/// concrete, routable URI.
const MALFORMED_QUERY_ROUTES: &[(&str, &str)] = &[
    ("GET /workflows", "/workflows"),
    ("GET /workflows/summaries", "/workflows/summaries"),
    ("GET /workflows/count", "/workflows/count"),
    ("GET /admin/usage", "/admin/usage"),
    (
        "GET /workflows/{id}/history/export",
        "/workflows/not-a-real-id/history/export",
    ),
    ("GET /admin/history/exports", "/admin/history/exports"),
    (
        "GET /admin/history/export-sample",
        "/admin/history/export-sample",
    ),
    (
        "GET /workflows/{id}/history",
        "/workflows/not-a-real-id/history",
    ),
    (
        "GET /workflows/{id}/result",
        "/workflows/not-a-real-id/result",
    ),
    (
        "GET /workflows/{id}/children",
        "/workflows/not-a-real-id/children",
    ),
    ("GET /workflows/{id}/tree", "/workflows/not-a-real-id/tree"),
    (
        "GET /admin/external-handoffs",
        "/admin/external-handoffs",
    ),
    ("GET /workflows/{id}/logs", "/workflows/not-a-real-id/logs"),
    (
        "GET /admin/schedules/{id}/runs",
        "/admin/schedules/not-a-real-id/runs",
    ),
    (
        "GET /dead-letters/aggregate",
        "/dead-letters/aggregate",
    ),
    ("GET /workers", "/workers"),
    ("GET /workers/drain-preview", "/workers/drain-preview"),
    ("GET /admin/queue-coverage", "/admin/queue-coverage"),
];

#[tokio::test]
async fn every_raw_pairs_route_400s_on_a_malformed_percent_encoded_value() {
    let app = build_app();
    for (name, path) in MALFORMED_QUERY_ROUTES {
        let uri = format!("{path}?queue_name=%FF");
        let (status, body) = get_json(&app, &uri).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{name} must 400 on a malformed percent-encoded query value \
             (?queue_name=%FF), not silently decode to U+FFFD and proceed: \
             got {status} with body {body:?}"
        );
        assert_malformed_query_body(name, &body);
    }
}

#[tokio::test]
async fn every_raw_pairs_route_400s_on_a_malformed_percent_encoded_key() {
    // The decode-before-parse ordering means a malformed byte sequence in
    // *any* key -- including one a route's own filter type does not
    // recognize -- is rejected up front, mirroring
    // `queue_coverage_integration.rs`'s
    // `malformed_percent_encoding_in_an_unknown_query_key_also_returns_400`.
    let app = build_app();
    for (name, path) in MALFORMED_QUERY_ROUTES {
        let separator = if path.contains('?') { '&' } else { '?' };
        let uri = format!("{path}{separator}%FF=1");
        let (status, body) = get_json(&app, &uri).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{name} must 400 on a malformed percent-encoded query KEY \
             (?%FF=1): got {status} with body {body:?}"
        );
        assert_malformed_query_body(name, &body);
    }
}

#[tokio::test]
async fn every_raw_pairs_route_400s_on_a_syntactically_invalid_percent_escape() {
    // A distinct malformed-encoding shape from `%FF` (well-formed escape,
    // invalid UTF-8): `%GG` is not a well-formed hex escape at all. Mirrors
    // `queue_coverage_integration.rs`'s
    // `syntactically_invalid_percent_escape_also_returns_400`.
    let app = build_app();
    for (name, path) in MALFORMED_QUERY_ROUTES {
        let uri = format!("{path}?queue_name=orders%GG");
        let (status, body) = get_json(&app, &uri).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{name} must 400 on a syntactically invalid percent escape \
             (?queue_name=orders%GG): got {status} with body {body:?}"
        );
        assert_malformed_query_body(name, &body);
    }
}
