//! No-database tests for the MCP Tasks route (issue #2005).
//!
//! These tests drive the JSON-RPC route with no runtime. A call that needs the
//! database fails closed, so each test checks the protocol contract only. The
//! Docker suite `mcp_tasks_integration.rs` covers the full task lifecycle.

#![cfg(feature = "mcp")]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::collections::HashMap;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestApiState;
use autumn_harvest_plugin::mcp_tasks::{
    CLIENT_CAPABILITIES_META, MISSING_CLIENT_CAPABILITY, START_KEY_META, TASKS_EXTENSION,
    build_mcp_task_route,
};
use autumn_harvest_plugin::mcp_tools::collect_descriptors;
use autumn_web::AppState;
use autumn_web::reexports::axum::Router;
use autumn_web::reexports::axum::body::{Body, to_bytes};
use autumn_web::reexports::http::{Request, StatusCode};
use autumn_web::session::Session;
use serde_json::{Value, json};
use tower::ServiceExt as _;

const TASKS: &str = "/api/harvest/mcp/tasks";

#[workflow(mcp, description = "Reviews a document")]
async fn review_flow(_ctx: &WorkflowContext, _doc: String) -> Result<String, String> {
    Ok("done".into())
}

#[workflow]
async fn private_flow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow(mcp)]
async fn invoice_status(_ctx: &WorkflowContext, _id: String) -> Result<(), String> {
    Ok(())
}

#[workflow(mcp)]
async fn start_invoice(_ctx: &WorkflowContext, _id: String) -> Result<(), String> {
    Ok(())
}

fn review_input_schema() -> Value {
    json!({"type": "string", "description": "document id"})
}

fn open_state() -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.set_allow_unauthenticated_mutations(true);
    api_state
}

fn router_with(api_state: &HarvestApiState, role_auth_enabled: bool) -> Router {
    let workflows = vec![
        __autumn_workflow_info_review_flow().with_input_schema_fn(review_input_schema),
        __autumn_workflow_info_private_flow(),
    ];
    let descriptors = collect_descriptors(&workflows, &[], &[]);
    let route = build_mcp_task_route(TASKS, &descriptors, api_state, None, role_auth_enabled);
    Router::<AppState>::new()
        .route(route.path, route.handler)
        .with_state(AppState::for_test())
}

fn router() -> Router {
    router_with(&open_state(), false)
}

fn declared() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        CLIENT_CAPABILITIES_META: {"extensions": {TASKS_EXTENSION: {}}},
    })
}

/// The Streamable HTTP headers that a 2026-07-28 body needs. An older body
/// needs none.
fn mcp_headers(body: &Value) -> Vec<(&'static str, String)> {
    let params = &body["params"];
    let Some(version) = params
        .pointer("/_meta/io.modelcontextprotocol~1protocolVersion")
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let method = body["method"].as_str().unwrap_or_default();
    let mut headers = vec![
        ("mcp-protocol-version", version.to_string()),
        ("mcp-method", method.to_string()),
    ];
    let name = if method == "tools/call" {
        "name"
    } else {
        "taskId"
    };
    if let Some(name) = params.get(name).and_then(Value::as_str) {
        headers.push(("mcp-name", name.to_string()));
    }
    headers
}

async fn post_raw(
    app: &Router,
    content_type: &str,
    body: String,
    session: Option<Session>,
) -> (StatusCode, String) {
    let mut req = Request::builder()
        .method("POST")
        .uri(TASKS)
        .header("content-type", content_type)
        .body(Body::from(body))
        .unwrap();
    if let Some(s) = session {
        req.extensions_mut().insert(s);
    }
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Send `body` with the headers it needs. A 2026-07-28 error can carry an
/// HTTP error status, so only a success must be 200.
async fn rpc(app: &Router, body: Value) -> Value {
    let headers = mcp_headers(&body);
    let pairs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let (status, out) = post_with_headers(app, &body, &pairs).await;
    if out.get("error").is_none() {
        assert_eq!(status, StatusCode::OK, "{out}");
    }
    out
}

fn call(method: &str, params: &Value) -> Value {
    json!({"jsonrpc": "2.0", "id": 7, "method": method, "params": params})
}

#[tokio::test]
async fn initialize_advertises_the_tasks_extension() {
    let out = rpc(
        &router(),
        call("initialize", &json!({"protocolVersion": "2026-07-28"})),
    )
    .await;
    assert_eq!(out["id"], 7);
    let caps = &out["result"]["capabilities"];
    assert_eq!(caps["extensions"][TASKS_EXTENSION], json!({}));
    assert!(caps["tools"].is_object(), "{out}");
    assert_eq!(out["result"]["protocolVersion"], "2026-07-28");
    assert!(out["result"]["serverInfo"]["name"].is_string(), "{out}");
}

/// The Tasks extension is defined for 2026-07-28 only. A 2025 session does
/// not see it, and a 2025 request cannot use it.
#[tokio::test]
async fn a_2025_client_gets_no_tasks() {
    let out = rpc(
        &router(),
        call("initialize", &json!({"protocolVersion": "2025-06-18"})),
    )
    .await;
    assert_eq!(out["result"]["protocolVersion"], "2025-06-18");
    let caps = &out["result"]["capabilities"];
    assert!(caps.get("extensions").is_none(), "{out}");
    assert!(caps["tools"].is_object(), "{out}");

    let old = json!({
        "io.modelcontextprotocol/protocolVersion": "2025-11-25",
        CLIENT_CAPABILITIES_META: {"extensions": {TASKS_EXTENSION: {}}},
    });
    let out = rpc(
        &router(),
        call("tasks/get", &json!({"taskId": "x", "_meta": old})),
    )
    .await;
    assert_eq!(out["error"]["code"], -32021, "{out}");
}

#[tokio::test]
async fn server_discover_advertises_the_tasks_extension() {
    let out = rpc(&router(), call("server/discover", &json!({}))).await;
    let result = &out["result"];
    assert_eq!(
        result["capabilities"]["extensions"][TASKS_EXTENSION],
        json!({})
    );
    // The 2026-07-28 `DiscoverResult` shape: a `CacheableResult` with the
    // server identity under `_meta`.
    assert_eq!(result["resultType"], "complete");
    assert!(
        result["supportedVersions"]
            .as_array()
            .unwrap()
            .contains(&json!("2026-07-28"))
    );
    assert!(
        result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"].is_string(),
        "{result}"
    );
    assert!(result.get("serverInfo").is_none(), "{result}");
    assert!(result["ttlMs"].is_u64(), "{result}");
    assert_eq!(result["cacheScope"], "private");
}

#[tokio::test]
async fn tools_list_serves_one_start_tool_per_mcp_workflow() {
    let out = rpc(&router(), call("tools/list", &json!({}))).await;
    assert_eq!(out["result"]["resultType"], "complete");
    assert!(out["result"]["ttlMs"].is_u64(), "{out}");
    assert_eq!(out["result"]["cacheScope"], "private");
    let tools = out["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert_eq!(names, ["start_review_flow"]);
    let schema = &tools[0]["inputSchema"];
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["body"]));
    assert_eq!(
        schema["properties"]["body"]["$ref"],
        "#/$defs/HarvestMcpInput_review_flow"
    );
    assert_eq!(
        schema["$defs"]["HarvestMcpInput_review_flow"],
        review_input_schema()
    );
    assert!(
        tools[0]["description"]
            .as_str()
            .unwrap()
            .contains("Reviews a document")
    );
}

/// The schema requires HTTP 400 with `-32021` for a missing capability.
#[tokio::test]
async fn task_methods_need_the_client_capability() {
    let app = router();
    for method in ["tasks/get", "tasks/update", "tasks/cancel"] {
        let (status, text) = post_raw(
            &app,
            "application/json",
            call(method, &json!({"taskId": "x"})).to_string(),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{method}: {text}");
        let out: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(out["error"]["code"], MISSING_CLIENT_CAPABILITY, "{method}");
        assert_eq!(
            out["error"]["data"]["requiredCapabilities"]["extensions"][TASKS_EXTENSION],
            json!({}),
            "{method}"
        );
    }
}

#[tokio::test]
async fn a_malformed_task_id_is_invalid_params() {
    let app = router();
    for method in ["tasks/get", "tasks/update", "tasks/cancel"] {
        let out = rpc(
            &app,
            call(
                method,
                &json!({"taskId": "not-a-task", "inputResponses": {}, "_meta": declared()}),
            ),
        )
        .await;
        assert_eq!(out["error"]["code"], -32602, "{method}: {out}");
    }
}

#[tokio::test]
async fn a_missing_task_id_is_invalid_params() {
    let out = rpc(&router(), call("tasks/get", &json!({"_meta": declared()}))).await;
    assert_eq!(out["error"]["code"], -32602, "{out}");
}

#[tokio::test]
async fn an_unknown_tool_is_invalid_params() {
    let out = rpc(
        &router(),
        call(
            "tools/call",
            &json!({"name": "start_private_flow", "arguments": {"body": null}}),
        ),
    )
    .await;
    assert_eq!(out["error"]["code"], -32602, "{out}");
}

/// With no runtime the start fails closed. The failure is a tool error, not
/// a task, so a client never polls a task that does not exist.
#[tokio::test]
async fn a_start_that_fails_is_a_tool_error_not_a_task() {
    let out = rpc(
        &router(),
        call(
            "tools/call",
            &json!({"name": "start_review_flow", "arguments": {"body": "d1"}, "_meta": declared()}),
        ),
    )
    .await;
    assert_eq!(out["result"]["isError"], true, "{out}");
    assert_ne!(out["result"]["resultType"], "task", "{out}");
}

#[tokio::test]
async fn a_start_key_in_meta_must_be_a_string() {
    let out = rpc(
        &router(),
        call(
            "tools/call",
            &json!({
                "name": "start_review_flow",
                "arguments": {"body": "d1"},
                "_meta": {START_KEY_META: 7},
            }),
        ),
    )
    .await;
    assert_eq!(out["error"]["code"], -32602, "{out}");
}

#[tokio::test]
async fn tasks_update_needs_input_responses() {
    let out = rpc(
        &router(),
        call(
            "tasks/update",
            &json!({"taskId": uuid::Uuid::new_v4().to_string(), "_meta": declared()}),
        ),
    )
    .await;
    assert_eq!(out["error"]["code"], -32602, "{out}");
}

/// MCP forbids a null request id.
#[tokio::test]
async fn a_null_id_is_an_invalid_request() {
    let out = rpc(
        &router(),
        json!({"jsonrpc": "2.0", "id": null, "method": "ping"}),
    )
    .await;
    assert_eq!(out["error"]["code"], -32600, "{out}");
}

#[tokio::test]
async fn an_unknown_method_is_method_not_found() {
    let out = rpc(&router(), call("tasks/list", &json!({"_meta": declared()}))).await;
    assert_eq!(out["error"]["code"], -32601, "{out}");
}

#[tokio::test]
async fn ping_answers_an_empty_complete_result() {
    let out = rpc(&router(), call("ping", &json!({}))).await;
    assert_eq!(out["result"], json!({"resultType": "complete"}));
}

#[tokio::test]
async fn a_batch_is_an_invalid_request() {
    let out = rpc(&router(), json!([call("ping", &json!({}))])).await;
    assert_eq!(out["error"]["code"], -32600, "{out}");
}

#[tokio::test]
async fn a_message_without_jsonrpc_2_is_an_invalid_request() {
    let out = rpc(&router(), json!({"id": 1, "method": "ping"})).await;
    assert_eq!(out["error"]["code"], -32600, "{out}");
}

#[tokio::test]
async fn a_notification_gets_no_body() {
    let (status, body) = post_raw(
        &router(),
        "application/json",
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}).to_string(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.is_empty(), "{body}");
}

#[tokio::test]
async fn bad_json_is_a_parse_error() {
    let (status, body) = post_raw(&router(), "application/json", "{".into(), None).await;
    assert_eq!(status, StatusCode::OK);
    let out: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(out["error"]["code"], -32700, "{out}");
}

/// A browser sends `text/plain` cross-site with no preflight. The route
/// refuses it, so a forged form cannot reach a task method.
#[tokio::test]
async fn a_non_json_content_type_is_refused() {
    let (status, _) = post_raw(
        &router(),
        "text/plain",
        call("ping", &json!({})).to_string(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

fn session(role: &str) -> Session {
    let mut d = HashMap::new();
    d.insert("role".to_string(), role.to_string());
    Session::new_for_test(role.to_string(), d)
}

/// The route can start and cancel runs, so it is a mutation route. A
/// read-only principal gets `403` with the role gate on.
#[tokio::test]
async fn a_read_only_principal_is_refused() {
    let app = router_with(&open_state(), true);
    let body = call("ping", &json!({})).to_string();
    let (status, text) = post_raw(
        &app,
        "application/json",
        body.clone(),
        Some(session("harvest_readonly")),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{text}");
    let (status, _) = post_raw(&app, "application/json", body, Some(session("admin"))).await;
    assert_eq!(status, StatusCode::OK);
}

/// Outside `dev` an anonymous caller gets `401` (issue #1802).
#[tokio::test]
async fn an_anonymous_caller_is_refused_outside_dev() {
    let api_state = HarvestApiState::new();
    api_state.set_deployment_profile("prod");
    let app = router_with(&api_state, false);
    let body = call("ping", &json!({})).to_string();
    let (status, _) = post_raw(&app, "application/json", body.clone(), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = post_raw(&app, "application/json", body, Some(session("admin"))).await;
    assert_eq!(status, StatusCode::OK);
}

/// A tenant-bound caller never reaches the route (issue #1977).
#[tokio::test]
async fn a_tenant_bound_caller_is_refused() {
    let tenant = autumn_harvest_plugin::tenant::VerifiedTenant::new("acme").expect("tenant");
    let mut req = Request::builder()
        .method("POST")
        .uri(TASKS)
        .header("content-type", "application/json")
        .body(Body::from(call("ping", &json!({})).to_string()))
        .unwrap();
    req.extensions_mut().insert(tenant);
    let resp = router().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

async fn post_with_headers(
    app: &Router,
    body: &Value,
    pairs: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(TASKS)
        .header("content-type", "application/json");
    for (name, value) in pairs {
        builder = builder.header(*name, *value);
    }
    let resp = app
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// A gateway can authorize on `Mcp-Method`. A header that differs from the
/// body method is refused, so it cannot pass a `ping` policy and start a run.
#[tokio::test]
async fn a_mismatched_method_header_is_refused_with_400() {
    let body = call(
        "tools/call",
        &json!({"name": "start_review_flow", "arguments": {"body": "d1"}}),
    );
    let (status, out) = post_with_headers(&router(), &body, &[("mcp-method", "ping")]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(out["error"]["code"], -32020, "{out}");
}

#[tokio::test]
async fn an_unsupported_version_header_is_refused_with_400() {
    let (status, out) = post_with_headers(
        &router(),
        &call("ping", &json!({})),
        &[("mcp-protocol-version", "2099-01-01")],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(out["error"]["code"], -32022, "{out}");
    assert_eq!(out["error"]["data"]["requested"], "2099-01-01");
}

/// A 2026-07-28 request with matching headers is served. Its unknown
/// method gets 404, as the transport requires.
#[tokio::test]
async fn a_modern_request_with_matching_headers_is_served() {
    let meta = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        CLIENT_CAPABILITIES_META: {},
    });
    let modern = [
        ("mcp-protocol-version", "2026-07-28"),
        ("mcp-method", "ping"),
    ];
    let (status, out) =
        post_with_headers(&router(), &call("ping", &json!({"_meta": meta})), &modern).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let unknown = [
        ("mcp-protocol-version", "2026-07-28"),
        ("mcp-method", "nope/x"),
    ];
    let (status, out) = post_with_headers(
        &router(),
        &call("nope/x", &json!({"_meta": meta})),
        &unknown,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(out["error"]["code"], -32601, "{out}");
}

/// A 2026-07-28 request without its client capabilities in `_meta` is
/// malformed: HTTP 400 with `-32602`.
#[tokio::test]
async fn a_modern_request_without_meta_fields_is_refused() {
    let meta = json!({"io.modelcontextprotocol/protocolVersion": "2026-07-28"});
    let modern = [
        ("mcp-protocol-version", "2026-07-28"),
        ("mcp-method", "ping"),
    ];
    let (status, out) =
        post_with_headers(&router(), &call("ping", &json!({"_meta": meta})), &modern).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(out["error"]["code"], -32602, "{out}");
}

/// The task route exposes only `start_{wf}`. A collision on another `/mcp`
/// tool name must not drop a workflow from it.
#[tokio::test]
async fn a_tool_name_collision_on_mcp_does_not_drop_a_task_tool() {
    let workflows = vec![
        __autumn_workflow_info_invoice_status(),
        __autumn_workflow_info_start_invoice(),
    ];
    // On `/mcp`, `start_invoice_status` is both the start tool of
    // `invoice_status` and the status tool of `start_invoice`.
    assert_eq!(collect_descriptors(&workflows, &[], &[]).len(), 1);
    let descriptors =
        autumn_harvest_plugin::mcp_tools::collect_task_descriptors(&workflows, &[], &[]);
    let route = build_mcp_task_route(TASKS, &descriptors, &open_state(), None, false);
    let app = Router::<AppState>::new()
        .route(route.path, route.handler)
        .with_state(AppState::for_test());
    let out = rpc(&app, call("tools/list", &json!({}))).await;
    let names: Vec<&str> = out["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert_eq!(names, ["start_invoice_status", "start_start_invoice"]);
}

/// A browser `Origin` on an untrusted host gets 403, which stops DNS
/// rebinding. A trusted same-origin call and a call with no `Origin` pass.
#[tokio::test]
async fn an_untrusted_origin_is_refused_with_403() {
    let ping = call("ping", &json!({}));
    let (status, _) = post_with_headers(
        &router(),
        &ping,
        &[
            ("origin", "http://evil.test:8080"),
            ("host", "evil.test:8080"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, out) = post_with_headers(
        &router(),
        &ping,
        &[
            ("origin", "http://localhost:8080"),
            ("host", "localhost:8080"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
}

/// `tools/list` marks `body` as required, so a call without it starts no
/// run. An explicit `null` body still reaches the start.
#[tokio::test]
async fn a_tool_call_without_a_body_is_invalid_params() {
    for arguments in [json!({}), json!("d1"), Value::Null] {
        let out = rpc(
            &router(),
            call(
                "tools/call",
                &json!({"name": "start_review_flow", "arguments": arguments}),
            ),
        )
        .await;
        assert_eq!(out["error"]["code"], -32602, "{arguments}: {out}");
    }
    let out = rpc(
        &router(),
        call(
            "tools/call",
            &json!({"name": "start_review_flow", "arguments": {"body": null}}),
        ),
    )
    .await;
    assert!(out.get("error").is_none(), "{out}");
}

/// A malformed 2026-07-28 request gets HTTP 400, also when dispatch finds
/// the fault: here a `tasks/get` without its `taskId`.
#[tokio::test]
async fn a_modern_invalid_params_error_is_http_400() {
    let meta = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        CLIENT_CAPABILITIES_META: {"extensions": {TASKS_EXTENSION: {}}},
    });
    let modern = [
        ("mcp-protocol-version", "2026-07-28"),
        ("mcp-method", "tasks/get"),
    ];
    let (status, out) = post_with_headers(
        &router(),
        &call("tasks/get", &json!({"_meta": meta})),
        &modern,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert_eq!(out["error"]["code"], -32602, "{out}");
}

/// A malformed 2026-07-28 envelope gets HTTP 400 before dispatch, and an
/// older request with the same fault keeps HTTP 200.
#[tokio::test]
async fn a_modern_malformed_envelope_is_http_400() {
    let modern = [
        ("mcp-protocol-version", "2026-07-28"),
        ("mcp-method", "ping"),
    ];
    for body in [
        json!({"id": 1, "method": "ping"}),
        json!({"jsonrpc": "2.0", "id": 1}),
        json!({"jsonrpc": "2.0", "id": null, "method": "ping"}),
        json!([call("ping", &json!({}))]),
    ] {
        let (status, out) = post_with_headers(&router(), &body, &modern).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} -> {out}");
        assert_eq!(out["error"]["code"], -32600, "{out}");
        let (status, _) = post_with_headers(&router(), &body, &[]).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
}
