//! Sweep regression test for issue #1151.
//!
//! Every management API route that consumes a raw `(key, value)` query-string
//! pair list must reject a malformed percent-encoded byte sequence with a
//! genuine `400` JSON error. It must never use axum's lossy
//! `Query<Vec<(String, String)>>` fallback. That fallback substitutes `U+FFFD`
//! and returns `200` with a wrong filter value. Issue #774 found it on
//! `GET /admin/queue-coverage`. Issue #1151 swept the other 17 call sites.
//!
//! Every route here decodes the raw query string first in its handler body,
//! *before* any path-parameter validation or database access (issue #1151).
//! So behavior does not depend on path validity. This suite needs no database,
//! no testcontainer and no admin session. A router with no storage pool proves
//! that the malformed-query `400` fires first. `?queue_name=%FF` (an invalid
//! standalone UTF-8 byte) is the exact repro from issue #774.
//!
//! The two "by-id" wrapper routes (`GET
//! /workflows/by-id/{workflow_name}/{workflow_id}/result` and `.../children`)
//! resolve the business id with a database lookup. They then forward the raw
//! query string to `get_workflow_result` / `list_workflow_children`. With the
//! check only in the delegate, a malformed query and an unresolvable business
//! id surfaced a 404/503, not the documented `400` (PR #1334). It also paid
//! for a needless lookup. Both wrappers now re-validate the raw query before
//! `resolve_workflow_by_business_id` runs. So the table below includes them,
//! and they need no database either.

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
    harvest_api_router(api_state)
}

/// Asserts a malformed-query `400` body carries the documented message.
///
/// Every route's `400` body carries the same message text. The test asserts it
/// by field lookup, not full-body equality, because the JSON shape differs.
/// `GET /admin/queue-coverage` keeps its shipped `{"error": "..."}` shape
/// (issue #774). Every other route already
/// returns `AutumnError`-shaped `400`s. So it wraps the message in
/// `AutumnError`'s RFC-7807-flavored `{"detail": "...", ...}` body. A route
/// then never gets a SECOND error shape (issue #1151). See the
/// `decode_or_bad_request` and `decode_or_autumn_error_response` doc comments
/// in `strict_query.rs`.
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

/// `(route with an arbitrary path segment, human-readable name)`. The segment
/// value never matters, because the malformed-query 400 fires before path
/// validation. Each route still needs *a* value to form a routable URI.
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
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/result",
        "/workflows/by-id/not-a-real-workflow/not-a-real-id/result",
    ),
    (
        "GET /workflows/by-id/{workflow_name}/{workflow_id}/children",
        "/workflows/by-id/not-a-real-workflow/not-a-real-id/children",
    ),
    (
        "GET /workflows/{id}/children",
        "/workflows/not-a-real-id/children",
    ),
    ("GET /workflows/{id}/tree", "/workflows/not-a-real-id/tree"),
    ("GET /admin/external-handoffs", "/admin/external-handoffs"),
    ("GET /workflows/{id}/logs", "/workflows/not-a-real-id/logs"),
    (
        "GET /admin/schedules/{id}/runs",
        "/admin/schedules/not-a-real-id/runs",
    ),
    ("GET /dead-letters/aggregate", "/dead-letters/aggregate"),
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
    // Decode comes before parse. So a malformed byte sequence in *any* key is
    // rejected, even a key the route's filter type does not recognize. This
    // mirrors `queue_coverage_integration.rs`'s
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
