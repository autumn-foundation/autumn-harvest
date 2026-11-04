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

// ── Read routes of issue #2468 ───────────────────────────────────────────────

/// The keys of a JSON object.
fn keys(body: &Value) -> Vec<&str> {
    body.as_object()
        .map(|fields| fields.keys().map(String::as_str).collect())
        .unwrap_or_default()
}

/// `body` has each of `fields`.
fn has_fields(body: &Value, fields: &[&str]) {
    for field in fields {
        assert!(body.get(*field).is_some(), "'{field}' is missing: {body}");
    }
}

/// A complete single-backend read names no unavailable shard.
fn is_complete(body: &Value) {
    assert_eq!(body["status"], "complete", "{body}");
    assert_eq!(body["unavailable_shards"], serde_json::json!([]), "{body}");
}

/// `GET /workflows/count`: the groups sum to `total`.
pub fn count_shape(body: &Value) {
    has_fields(body, &["as_of", "groups", "total", "truncated"]);
    is_complete(body);
    assert!(body["as_of"].is_string(), "{body}");
    assert!(body["truncated"].is_boolean(), "{body}");
    let groups = body["groups"].as_array().expect("groups");
    let sum: i64 = groups
        .iter()
        .map(|group| group["count"].as_i64().expect("a count"))
        .sum();
    assert_eq!(Some(sum), body["total"].as_i64(), "{body}");
}

/// The count of one group, or zero. `state` and `name` select the group.
pub fn count_of(body: &Value, state: Option<&str>, name: Option<&str>) -> i64 {
    body["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|group| group["state"].as_str() == state && group["workflow_name"].as_str() == name)
        .and_then(|group| group["count"].as_i64())
        .unwrap_or(0)
}

/// `GET /workflows/summaries`: the page envelope and the summary flags.
pub fn summaries_shape(body: &Value) {
    assert!(body["summaries"].is_array(), "{body}");
    assert!(
        body["next_cursor"].is_null() || body["next_cursor"].is_string(),
        "{body}"
    );
    for item in body["summaries"].as_array().into_iter().flatten() {
        assert_eq!(item["summarized"], true, "{item}");
        assert_eq!(item["history_available"], false, "{item}");
    }
}

/// `GET /workflows/registered/{name}/schema`: the record of `name`.
pub fn registered_schema_shape(body: &Value, name: &str) {
    assert_eq!(body["name"], name, "{body}");
    has_fields(body, &["input_schema", "output_schema", "error_schema"]);
}

/// `GET /workflows/registered/{name}/interface`: three sorted lists. Return
/// the names of each list.
pub fn interface_shape(body: &Value) -> [Vec<String>; 3] {
    ["signals", "queries", "updates"].map(|list| {
        let names: Vec<String> = body[list]
            .as_array()
            .unwrap_or_else(|| panic!("'{list}' is a list: {body}"))
            .iter()
            .map(|handler| handler["name"].as_str().expect("a name").to_owned())
            .collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted, "'{list}' is sorted: {body}");
        names
    })
}

/// `GET /workflows/{id}/children`: the page and the fields of each child.
pub fn children_shape(body: &Value) {
    has_fields(body, &["items", "next_cursor"]);
    is_complete(body);
    for child in body["items"].as_array().expect("items") {
        has_fields(
            child,
            &[
                "exec_id",
                "workflow_name",
                "status",
                "started_at",
                "completed_at",
                "error_summary",
                "shard_id",
                "depth",
            ],
        );
        let status = child["status"].as_str().expect("a status");
        assert!(
            status.chars().next().is_some_and(char::is_uppercase) && !status.contains('_'),
            "the status is a label such as Running: {child}"
        );
    }
}

/// The nodes of a lineage subtree. Each node has the Postgres fields.
fn count_nodes(node: &Value) -> usize {
    has_fields(
        node,
        &[
            "execution_id",
            "workflow_name",
            "workflow_id",
            "state",
            "started_at",
            "parent_id",
            "depth",
            "await_mode",
            "children",
        ],
    );
    1 + node["children"]
        .as_array()
        .expect("children")
        .iter()
        .map(count_nodes)
        .sum::<usize>()
}

/// `GET /workflows/{id}/tree`: the report and each node.
pub fn tree_shape(body: &Value) {
    has_fields(
        body,
        &[
            "root",
            "node_count",
            "max_depth_reached",
            "truncated",
            "truncated_parent_ids",
            "limits",
            "retained_summary_parent_ids",
        ],
    );
    is_complete(body);
    let counted = count_nodes(&body["root"]);
    assert_eq!(Some(counted as u64), body["node_count"].as_u64(), "{body}");
}

/// `GET /workflows/{id}/run-chain`: the runs in order, linked forward.
pub fn run_chain_shape(body: &Value) {
    has_fields(body, &["head_unknown", "workflow_id", "runs"]);
    let runs = body["runs"].as_array().expect("runs");
    for (index, run) in runs.iter().enumerate() {
        has_fields(
            run,
            &[
                "exec_id",
                "run_id",
                "sequence",
                "state",
                "started_at",
                "completed_at",
                "outcome",
                "continued_to_exec_id",
            ],
        );
        assert_eq!(run["sequence"], index, "{body}");
        let next = runs.get(index + 1).map(|next| next["exec_id"].clone());
        assert_eq!(
            run["continued_to_exec_id"],
            next.unwrap_or(Value::Null),
            "{body}"
        );
    }
}

/// `GET /workflows/{id}/timeline`: the header, the steps and the rollup.
pub fn timeline_shape(body: &Value) {
    has_fields(
        body,
        &[
            "exec_id",
            "workflow_id",
            "workflow_name",
            "state",
            "steps",
            "rollup",
        ],
    );
    assert!(body["steps"].is_array(), "{body}");
    assert!(!keys(&body["rollup"]).is_empty(), "{body}");
}

/// `GET /workflows/{id}/logs`: the page, its lines and its totals.
pub fn logs_shape(body: &Value) {
    has_fields(
        body,
        &[
            "execution_id",
            "lines",
            "next_cursor",
            "total_lines",
            "truncated",
        ],
    );
    for line in body["lines"].as_array().expect("lines") {
        has_fields(line, &["seq", "level", "message", "occurred_at"]);
        let level = line["level"].as_str().expect("a level");
        assert!(["info", "warn", "error"].contains(&level), "{line}");
    }
}

/// `GET /workflows/{id}/awaitables`: the report header and its awaitables.
pub fn awaitables_shape(body: &Value) {
    has_fields(
        body,
        &[
            "execution_id",
            "workflow_id",
            "workflow_name",
            "state",
            "is_terminal",
            "wait_set",
            "awaitables",
            "truncated",
            "truncated_kinds",
            "category_cap",
        ],
    );
    for awaitable in body["awaitables"].as_array().expect("awaitables") {
        has_fields(awaitable, &["kind"]);
    }
}

/// `GET /workflows/{id}/stack`: every list, also when empty.
pub fn stack_shape(body: &Value) {
    has_fields(
        body,
        &[
            "exec_id",
            "workflow_id",
            "workflow_name",
            "state",
            "is_terminal",
            "pending_activities",
            "pending_external_handoffs",
            "pending_local_activities",
            "pending_timers",
            "pending_signals",
            "buffered_signals",
            "pending_child_workflows",
            "checkpoints_truncated_for_budget",
            "last_event_id",
        ],
    );
    for activity in body["pending_activities"].as_array().expect("activities") {
        has_fields(
            activity,
            &[
                "activity_exec_id",
                "activity_name",
                "queue",
                "scheduled_at",
                "attempt",
                "max_attempts",
                "task_status",
                "heartbeat_details_truncated",
            ],
        );
    }
}

/// `GET /workflows/{id}/history/export`: the document header and events.
pub fn history_export_shape(body: &Value, events: usize) {
    has_fields(
        body,
        &[
            "schema",
            "version",
            "workflow_name",
            "execution_id",
            "shard_id",
            "exported_at",
            "event_count",
            "status",
            "payload_policy",
            "size_limit",
            "events",
        ],
    );
    assert_eq!(body["event_count"], events, "{body}");
    assert_eq!(
        body["events"].as_array().map(Vec::len),
        Some(events),
        "{body}"
    );
}
