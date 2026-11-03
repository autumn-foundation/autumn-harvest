//! The management API wire contract that both backends keep (issue #2489).
//!
//! The Postgres integration tests and the SQLite suite call the same checks.
//! Each harness sends its own requests and seeds its own runs. These checks
//! read only the answer. A new difference between the backends then fails
//! CI, not a review.

// Each test binary uses a different subset of the checks.
#![allow(dead_code)]

use serde_json::Value;

/// One HTTP answer, as a harness reads it.
#[derive(Debug, Clone)]
pub struct Reply {
    /// The status code.
    pub status: u16,
    /// The `Retry-After` header, if any.
    pub retry_after: Option<String>,
    /// The `X-Harvest-Execution-Id` header, if any.
    pub execution_id: Option<String>,
    /// The JSON body. An empty body is `Value::Null`.
    pub body: Value,
}

/// The header that a by-id route sets to the run it resolved.
pub const EXECUTION_ID_HEADER: &str = "x-harvest-execution-id";

// ── GET /workflows/{id}/result ───────────────────────────────────────────────

/// A run that is not terminal answers `204` with `Retry-After: 1` and no body.
pub fn result_is_pending(reply: &Reply) {
    assert_eq!(reply.status, 204, "a running run is not a 200: {reply:?}");
    assert_eq!(reply.retry_after.as_deref(), Some("1"), "{reply:?}");
    assert_eq!(reply.body, Value::Null, "a 204 has no body: {reply:?}");
}

/// A terminal run answers `200` with the compact result. The state is lower
/// case. A completed run carries `output`. A failed run carries `error`.
pub fn result_is_terminal<'a>(reply: &'a Reply, state: &str) -> &'a Value {
    assert_eq!(reply.status, 200, "{reply:?}");
    let body = &reply.body;
    assert_eq!(body["state"], state, "the compact state: {body}");
    assert!(
        body["completed_at"].is_string(),
        "an RFC 3339 completion time: {body}"
    );
    let fields: Vec<&str> = body
        .as_object()
        .map(|fields| fields.keys().map(String::as_str).collect())
        .unwrap_or_default();
    for field in &fields {
        assert!(
            ["state", "output", "error", "completed_at"].contains(field),
            "the compact form has no field '{field}': {body}"
        );
    }
    match state {
        "completed" => assert!(body.get("error").is_none(), "{body}"),
        "failed" | "cancelled" | "timed_out" | "terminated" => {
            assert!(body.get("output").is_none(), "{body}");
            // A run with no stored error has no `error` field.
            assert!(body.get("error").is_none_or(Value::is_string), "{body}");
        }
        other => panic!("not a terminal state: {other}"),
    }
    body
}

/// An unknown run is a `404`, and a bad id is a `400`.
pub fn result_of_unknown_run(unknown: &Reply, bad_id: &Reply) {
    assert_eq!(unknown.status, 404, "{unknown:?}");
    assert_eq!(bad_id.status, 400, "{bad_id:?}");
}

/// A query key that the route does not read is a `400`, as is a bad `wait`.
pub fn result_refuses_a_bad_query(reply: &Reply) {
    assert_eq!(reply.status, 400, "{reply:?}");
}

// ── GET /workflows/{id}/history ──────────────────────────────────────────────

/// The page envelope and the fields of each event.
///
/// A SQLite event written before #2489 has no time, so call this on a run
/// that this build wrote.
pub fn history_page_shape(body: &Value) {
    assert!(body["events"].is_array(), "{body}");
    assert!(body["total_events"].is_u64(), "{body}");
    assert!(
        body["last_event_id"].is_i64(),
        "a number, also when empty: {body}"
    );
    assert!(
        body["next_cursor"].is_null() || body["next_cursor"].is_string(),
        "{body}"
    );
    for event in body["events"].as_array().into_iter().flatten() {
        assert!(event["id"].as_i64().is_some_and(|id| id > 0), "{event}");
        assert!(
            event["event_id"].as_i64().is_some_and(|id| id >= 0),
            "{event}"
        );
        assert!(event["timestamp"].is_string(), "{event}");
        assert!(event["type"].is_string(), "{event}");
        assert!(event["data"].is_object(), "{event}");
    }
}

/// The pages of one cursor walk hold every event once, in order. Return the
/// event types in order.
pub fn history_pages_cover(pages: &[Value], limit: usize, total: u64) -> Vec<String> {
    assert!(pages.len() > 1, "the walk needs more than one page");
    let mut ids = Vec::new();
    let mut types = Vec::new();
    for (index, page) in pages.iter().enumerate() {
        history_page_shape(page);
        let events = page["events"].as_array().unwrap();
        assert!(events.len() <= limit, "page {index}: {page}");
        assert_eq!(page["total_events"], total, "page {index}: {page}");
        let last = index + 1 == pages.len();
        assert_eq!(page["next_cursor"].is_null(), last, "page {index}: {page}");
        if let Some(cursor) = page["next_cursor"].as_str() {
            // The cursor points at the last event of its page.
            assert_eq!(
                Some(cursor.to_owned()),
                events.last().map(|event| event["id"].to_string()),
                "page {index}: {page}"
            );
        }
        for event in events {
            ids.push(event["id"].as_i64().unwrap());
            types.push(event["type"].as_str().unwrap().to_owned());
        }
    }
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "{ids:?}");
    assert_eq!(ids.len() as u64, total, "no event is dropped");
    let last_event_id = pages[0]["last_event_id"].as_i64().unwrap();
    assert_eq!(ids.last().copied(), Some(last_event_id));
    types
}

/// A filtered page holds only the asked types. Its totals stay unfiltered.
pub fn history_filter_holds_only(body: &Value, types: &[&str], expected: usize, total: u64) {
    history_page_shape(body);
    let events = body["events"].as_array().unwrap();
    assert_eq!(events.len(), expected, "{body}");
    for event in events {
        assert!(types.contains(&event["type"].as_str().unwrap()), "{event}");
    }
    assert_eq!(body["total_events"], total, "{body}");
}

/// A bad `limit` or `after` is a `400`. An unknown run is a `404`.
pub fn history_refuses(reply: &Reply, status: u16) {
    assert_eq!(reply.status, status, "{reply:?}");
}

// ── GET /workflows/by-id/{workflow_name}/{workflow_id} ───────────────────────

/// A by-id read names the run it resolved, in the header and in the body.
pub fn by_id_names_its_run(reply: &Reply, exec: &str) {
    assert_eq!(reply.status, 200, "{reply:?}");
    assert_eq!(reply.execution_id.as_deref(), Some(exec), "{reply:?}");
    assert_eq!(reply.body["execution"]["id"], exec, "{reply:?}");
}

// ── GET /workflows/{id} ──────────────────────────────────────────────────────

/// The describe body carries the Postgres top-level fields.
pub fn describe_shape(body: &Value) {
    for field in ["execution", "history", "external_handoffs"] {
        assert!(!body[field].is_null(), "describe has '{field}': {body}");
    }
    assert!(body.get("parent_id").is_some(), "{body}");
    assert!(body["history_truncated"].is_boolean(), "{body}");
    assert!(body["history_endpoint"].is_string(), "{body}");
    assert!(body["legal_hold"].is_boolean(), "{body}");
    assert!(body["history"].as_array().is_some_and(|h| h.len() <= 100));
}

// ── GET /workflows ───────────────────────────────────────────────────────────

/// A state that no run can have is a `400`, never an empty `200`.
pub fn list_refuses_an_unknown_state(reply: &Reply) {
    assert_eq!(reply.status, 400, "{reply:?}");
}

/// The paged envelope.
pub fn list_envelope_shape(body: &Value) {
    assert!(body["workflows"].is_array(), "{body}");
    assert!(
        body["next_cursor"].is_null() || body["next_cursor"].is_string(),
        "{body}"
    );
    assert_eq!(body["status"], "complete", "{body}");
    assert_eq!(body["unavailable_shards"], serde_json::json!([]), "{body}");
}

// ── GET /workflows/registered ────────────────────────────────────────────────

/// Each record carries the schema fields, `null` when absent.
pub fn registered_shape(body: &Value) {
    for record in body.as_array().expect("an array") {
        assert!(record["name"].is_string(), "{record}");
        for field in ["input_schema", "output_schema", "error_schema"] {
            assert!(record.get(field).is_some(), "'{field}': {record}");
        }
    }
}

// ── GET /health/live and /health/ready ───────────────────────────────────────

/// Liveness does no I/O and says whether the process drains.
pub fn live_shape(reply: &Reply) {
    assert_eq!(reply.status, 200, "{reply:?}");
    assert_eq!(reply.body["alive"], true, "{reply:?}");
    assert!(reply.body["draining"].is_boolean(), "{reply:?}");
}

/// A ready replica says so, with no reasons.
pub fn ready_shape(reply: &Reply) {
    assert_eq!(reply.status, 200, "{reply:?}");
    assert_eq!(reply.body["ready"], true, "{reply:?}");
    assert_eq!(reply.body["runtime_ready"], true, "{reply:?}");
    assert_eq!(reply.body["database_reachable"], true, "{reply:?}");
    assert_eq!(reply.body["reasons"], serde_json::json!([]), "{reply:?}");
}
