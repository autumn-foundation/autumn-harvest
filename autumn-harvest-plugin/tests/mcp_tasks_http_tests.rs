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
    CLIENT_CAPABILITIES_META, MISSING_CLIENT_CAPABILITY, TASKS_EXTENSION, build_mcp_task_route,
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
    json!({CLIENT_CAPABILITIES_META: {"extensions": {TASKS_EXTENSION: {}}}})
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

async fn rpc(app: &Router, body: Value) -> Value {
    let (status, text) = post_raw(app, "application/json", body.to_string(), None).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

fn call(method: &str, params: &Value) -> Value {
    json!({"jsonrpc": "2.0", "id": 7, "method": method, "params": params})
}

#[tokio::test]
async fn initialize_advertises_the_tasks_extension() {
    let out = rpc(
        &router(),
        call("initialize", &json!({"protocolVersion": "2025-06-18"})),
    )
    .await;
    assert_eq!(out["id"], 7);
    let caps = &out["result"]["capabilities"];
    assert_eq!(caps["extensions"][TASKS_EXTENSION], json!({}));
    assert!(caps["tools"].is_object(), "{out}");
    assert_eq!(out["result"]["protocolVersion"], "2025-06-18");
    assert!(out["result"]["serverInfo"]["name"].is_string(), "{out}");
}

#[tokio::test]
async fn server_discover_advertises_the_tasks_extension() {
    let out = rpc(&router(), call("server/discover", &json!({}))).await;
    assert_eq!(
        out["result"]["capabilities"]["extensions"][TASKS_EXTENSION],
        json!({})
    );
}

#[tokio::test]
async fn tools_list_serves_one_start_tool_per_mcp_workflow() {
    let out = rpc(&router(), call("tools/list", &json!({}))).await;
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

#[tokio::test]
async fn task_methods_need_the_client_capability() {
    let app = router();
    for method in ["tasks/get", "tasks/update", "tasks/cancel"] {
        let out = rpc(&app, call(method, &json!({"taskId": "x"}))).await;
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
async fn an_unknown_method_is_method_not_found() {
    let out = rpc(&router(), call("tasks/list", &json!({"_meta": declared()}))).await;
    assert_eq!(out["error"]["code"], -32601, "{out}");
}

#[tokio::test]
async fn ping_answers_an_empty_result() {
    let out = rpc(&router(), call("ping", &json!({}))).await;
    assert_eq!(out["result"], json!({}));
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
