//! No-database HTTP tests for the MCP tool surface (issue #597).
//!
//! Builds a real autumn-web app with the generated workflow tool routes and
//! `mount_mcp("/mcp")`, then drives the JSON-RPC endpoint. `tools/list` and
//! schema derivation need no database; `tools/call` paths assert the error
//! contract that proves dispatch flows through the real handler pipeline
//! (the runtime is not installed, so handle-taking calls fail closed).
//!
//! NOTE: `TestApp::plugin(HarvestPlugin)` cannot be used here — `TestApp`
//! replays plugin startup hooks and `start_harvest_runtime` requires a live
//! Postgres. The full plugin-wired flow is covered by the testcontainers
//! integration test (`mcp_tools_integration.rs`, compile-checked in this
//! sandbox per the #543/#544 precedent).

#![cfg(feature = "mcp")]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestApiState;
use autumn_harvest_plugin::mcp_tools::{
    build_mcp_tool_routes, collect_descriptors, record_schemas,
};
use autumn_web::test::{TestApp, TestClient};
use serde_json::{Value, json};

#[workflow(mcp, description = "Processes an order end to end")]
async fn order_flow(_ctx: &WorkflowContext, _order_id: String) -> Result<String, String> {
    Ok("done".into())
}

#[workflow]
async fn hidden_flow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[derive(serde::Deserialize, serde::Serialize)]
struct ApproveRequest {
    approved: bool,
}

#[update(workflow = "order_flow", mcp)]
async fn approve(_ctx: &WorkflowContext, req: ApproveRequest) -> Result<bool, String> {
    Ok(req.approved)
}

#[update(workflow = "order_flow")]
async fn internal_only(_ctx: &WorkflowContext, req: ApproveRequest) -> Result<bool, String> {
    Ok(req.approved)
}

fn order_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "amount": {"type": "integer"}
        },
        "required": ["order_id"]
    })
}

/// Assemble a no-DB test app: descriptor plan -> schema map -> routes ->
/// `mount_mcp`. Mirrors what `HarvestPlugin::mcp_tools()` does inside
/// `Plugin::build`, minus the runtime startup.
fn build_client() -> TestClient {
    let workflows = vec![
        __autumn_workflow_info_order_flow().with_input_schema_fn(order_input_schema),
        __autumn_workflow_info_hidden_flow(),
    ];
    let updates = vec![
        __autumn_update_handler_info_approve(),
        __autumn_update_handler_info_internal_only(),
    ];
    let descriptors = collect_descriptors(&workflows, &updates);
    record_schemas(&descriptors);
    let routes = build_mcp_tool_routes(
        "/api/harvest/mcp",
        &descriptors,
        &HarvestApiState::new(),
        None,
    );
    TestApp::new().routes(routes).mount_mcp("/mcp").build()
}

async fn rpc(client: &TestClient, body: Value) -> Value {
    let resp = client.post("/mcp").json(&body).send().await;
    resp.assert_ok();
    resp.json::<Value>()
}

async fn tools(client: &TestClient) -> Vec<Value> {
    let out = rpc(
        client,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    )
    .await;
    out["result"]["tools"]
        .as_array()
        .expect("tools array")
        .clone()
}

fn tool<'a>(tools: &'a [Value], name: &str) -> &'a Value {
    tools
        .iter()
        .find(|t| t["name"] == name)
        .unwrap_or_else(|| panic!("tool '{name}' not found"))
}

#[tokio::test]
async fn initialize_negotiates_tools_capability() {
    let client = build_client();
    let out = rpc(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {}}
        }),
    )
    .await;
    assert!(out["result"]["capabilities"]["tools"].is_object());
}

#[tokio::test]
async fn tools_list_exposes_exactly_the_mcp_workflow_tool_set() {
    let client = build_client();
    let tools = tools(&client).await;
    let mut names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    names.sort_unstable();

    assert_eq!(
        names,
        vec![
            "order_flow_status",
            "order_flow_update_approve",
            "order_flow_watch",
            "signal_order_flow",
            "start_order_flow",
        ],
        "exactly the start/status/signal/update/watch set for the mcp workflow"
    );
    assert!(
        !names.iter().any(|n| n.contains("hidden_flow")),
        "non-mcp workflows must never surface"
    );
    assert!(
        !names.iter().any(|n| n.contains("internal_only")),
        "non-mcp updates must never surface"
    );
}

#[tokio::test]
async fn start_tool_input_schema_is_derived_from_workflow_typed_input() {
    let client = build_client();
    let tools = tools(&client).await;
    let start = tool(&tools, "start_order_flow");

    let schema = &start["inputSchema"];
    assert_eq!(schema["type"], "object");
    assert_eq!(
        schema["required"],
        json!(["body"]),
        "workflow input rides in the required `body` argument"
    );
    assert_eq!(
        schema["properties"]["body"]["$ref"], "#/$defs/HarvestMcpInput_order_flow",
        "body must reference the workflow's typed component schema"
    );
    let defs = &schema["$defs"]["HarvestMcpInput_order_flow"];
    assert_eq!(
        defs["properties"]["order_id"],
        json!({"type": "string"}),
        "the workflow's published input_schema (issue #373) must be inlined, \
         not a placeholder object; got: {defs}"
    );
    assert_eq!(defs["required"], json!(["order_id"]));
}

#[tokio::test]
async fn handle_tools_take_required_string_path_params() {
    let client = build_client();
    let tools = tools(&client).await;

    let status = tool(&tools, "order_flow_status");
    assert_eq!(
        status["inputSchema"]["properties"]["handle"],
        json!({"type": "string"})
    );
    assert_eq!(status["inputSchema"]["required"], json!(["handle"]));

    let signal = tool(&tools, "signal_order_flow");
    let props = &signal["inputSchema"]["properties"];
    assert!(props["handle"].is_object());
    assert!(props["signal_name"].is_object());
    let required = signal["inputSchema"]["required"].as_array().unwrap();
    assert!(required.contains(&json!("handle")));
    assert!(required.contains(&json!("signal_name")));
    assert!(
        required.contains(&json!("body")),
        "signal payload body is required"
    );

    let update = tool(&tools, "order_flow_update_approve");
    assert_eq!(
        update["inputSchema"]["properties"]["handle"],
        json!({"type": "string"})
    );
    assert!(
        update["description"]
            .as_str()
            .unwrap()
            .contains("ApproveRequest"),
        "update tool description carries the input type hint"
    );
}

#[tokio::test]
async fn annotations_mark_mutating_tools_not_read_only() {
    let client = build_client();
    let tools = tools(&client).await;

    assert_eq!(
        tool(&tools, "order_flow_status")["annotations"]["readOnlyHint"],
        json!(true),
        "status is a GET read"
    );
    for effectful in [
        "start_order_flow",
        "signal_order_flow",
        "order_flow_update_approve",
    ] {
        assert_eq!(
            tool(&tools, effectful)["annotations"]["readOnlyHint"],
            json!(false),
            "{effectful} is effectful and must not be read-only"
        );
    }
}

#[tokio::test]
async fn watch_tool_is_streaming_and_listed_without_json_response_schema() {
    let client = build_client();
    let tools = tools(&client).await;
    let watch = tool(&tools, "order_flow_watch");
    assert_eq!(
        watch["inputSchema"]["properties"]["handle"],
        json!({"type": "string"})
    );
    assert!(
        watch["description"]
            .as_str()
            .unwrap()
            .contains("notifications/progress"),
        "watch describes its streaming contract"
    );
}

#[tokio::test]
async fn tool_routes_coexist_with_the_nested_management_router() {
    // The default prefix (/api/harvest/mcp) sits inside the management API's
    // nest subtree (/api/harvest). axum must route the static tool paths and
    // the nested catch-all side by side without a collision panic.
    let workflows =
        vec![__autumn_workflow_info_order_flow().with_input_schema_fn(order_input_schema)];
    let descriptors = collect_descriptors(&workflows, &[]);
    record_schemas(&descriptors);
    let api_state = HarvestApiState::new();
    let routes = build_mcp_tool_routes("/api/harvest/mcp", &descriptors, &api_state, None);

    let client = TestApp::new()
        .routes(routes)
        .nest(
            "/api/harvest",
            autumn_harvest_plugin::harvest_api_router(api_state),
        )
        .mount_mcp("/mcp")
        .build();

    // The nested management route family still answers (fails closed pre-DB,
    // but routes — no panic, no 404-shadowing).
    let resp = client.get("/api/harvest/health").send().await;
    assert_ne!(resp.status, 404, "nested management route must still route");

    // And the tool catalog is served.
    let tools = tools(&client).await;
    assert!(tools.iter().any(|t| t["name"] == "start_order_flow"));
}

#[tokio::test]
async fn tools_call_dispatches_through_the_real_pipeline_and_fails_closed() {
    let client = build_client();

    // Malformed handle -> the handler's own 400 comes back as a tool error.
    let out = rpc(
        &client,
        json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": {"name": "order_flow_status", "arguments": {"handle": "not-a-uuid"}}
        }),
    )
    .await;
    assert_eq!(out["result"]["isError"], json!(true));
    let text = out["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("400"),
        "malformed handle must surface the handler's 400, got: {text}"
    );

    // Well-formed handle but no storage/runtime installed -> fail closed with
    // harvest's own "storage pool is not configured" error, never a silent
    // success. Proves the call reaches harvest's own state checks.
    let out = rpc(
        &client,
        json!({
            "jsonrpc": "2.0", "id": 8, "method": "tools/call",
            "params": {
                "name": "order_flow_status",
                "arguments": {"handle": "00000000-0000-4000-8000-000000000000"}
            }
        }),
    )
    .await;
    assert_eq!(out["result"]["isError"], json!(true));
    let text = out["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("storage pool is not configured"),
        "pre-startup calls must fail closed on harvest's storage check, got: {text}"
    );

    // Start tool: dispatch reaches the real (delegated) start_workflow
    // handler and fails closed pre-startup, same as the other calls above.
    // NOTE: start_tool no longer duplicates issue #373 schema validation
    // itself (removed as a redundant, closure-captured-schema copy of the
    // check start_workflow already performs against the live registry --
    // see the code-review hardening notes in CLAUDE.md); that validation is
    // gated behind a successful `api_state.runtime()` lookup inside
    // start_workflow, which this no-DB harness can never reach, so a
    // schema-violation rejection cannot be exercised here. That guarantee is
    // instead covered end-to-end (with a real runtime) by
    // `mcp_start_tool_rejects_input_that_violates_the_published_schema` in
    // `tests/mcp_tools_integration.rs`.
    let out = rpc(
        &client,
        json!({
            "jsonrpc": "2.0", "id": 9, "method": "tools/call",
            "params": {"name": "start_order_flow", "arguments": {"body": {"amount": 3}}}
        }),
    )
    .await;
    assert_eq!(out["result"]["isError"], json!(true));
    let text = out["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("harvest runtime is not started"),
        "pre-startup calls must fail closed on harvest's runtime check, got: {text}"
    );
}
