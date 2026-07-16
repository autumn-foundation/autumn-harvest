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

#[activity]
async fn noop_task(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

/// A unified DAG opted into MCP exposure (issue #601 follow-up). Requires
/// `unified-dag-execution` (a default feature) to lower onto a shadow
/// `WorkflowInfo`.
#[cfg(feature = "unified-dag-execution")]
#[dag(schedule = "0 3 * * *", mcp)]
fn daily_etl_dag(dag: &mut DagBuilder) {
    let _ = dag.activity(noop_task);
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
    let descriptors = collect_descriptors(&workflows, &updates, &[]);
    record_schemas(&descriptors);
    let routes = build_mcp_tool_routes(
        "/api/harvest/mcp",
        &descriptors,
        &HarvestApiState::new(),
        None,
        false,
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
    let descriptors = collect_descriptors(&workflows, &[], &[]);
    record_schemas(&descriptors);
    let api_state = HarvestApiState::new();
    let routes = build_mcp_tool_routes("/api/harvest/mcp", &descriptors, &api_state, None, false);

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

/// A unified DAG's shadow `WorkflowInfo` (`daily_etl_dag`), assembled the
/// same way `HarvestPlugin::build` would: workflows + dags ->
/// `collect_descriptors` -> schemas -> routes -> `mount_mcp`.
#[cfg(feature = "unified-dag-execution")]
fn build_dag_client() -> TestClient {
    let dags = vec![__autumn_dag_info_daily_etl_dag()];
    let workflows = vec![__autumn_workflow_info_daily_etl_dag()];
    let descriptors = collect_descriptors(&workflows, &[], &dags);
    record_schemas(&descriptors);
    let routes = build_mcp_tool_routes(
        "/api/harvest/mcp",
        &descriptors,
        &HarvestApiState::new(),
        None,
        false,
    );
    TestApp::new().routes(routes).mount_mcp("/mcp").build()
}

#[cfg(feature = "unified-dag-execution")]
#[tokio::test]
async fn tools_list_exposes_start_status_watch_but_not_signal_for_a_dag() {
    let client = build_dag_client();
    let tools = tools(&client).await;
    let mut names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    names.sort_unstable();

    assert_eq!(
        names,
        vec![
            "daily_etl_dag_status",
            "daily_etl_dag_watch",
            "start_daily_etl_dag",
        ],
        "a DAG must surface exactly start/status/watch -- no signal, no updates"
    );
}

#[cfg(feature = "unified-dag-execution")]
#[tokio::test]
async fn dag_start_tool_call_dispatches_through_the_dag_trigger_pipeline() {
    let client = build_dag_client();

    // No storage/runtime installed -> the delegated trigger_dag_run handler
    // fails closed on harvest's own runtime check, proving the call reaches
    // the real DAG trigger pipeline (not the generic start_workflow path,
    // which would fail closed with a different, workflow-shaped error).
    let out = rpc(
        &client,
        json!({
            "jsonrpc": "2.0", "id": 10, "method": "tools/call",
            "params": {"name": "start_daily_etl_dag", "arguments": {"body": null}}
        }),
    )
    .await;
    assert_eq!(out["result"]["isError"], json!(true));
    let text = out["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("harvest runtime is not started"),
        "pre-startup DAG start calls must fail closed on harvest's runtime check, got: {text}"
    );
}

// ── Read-only operator role × MCP tool routes (issue #776, F1) ────────────────
//
// The generated MCP tool routes are registered app-level (outside the nested
// management router that carries the `enforce_read_only_class` layer), so
// without a per-route gate a read-only principal could START / SIGNAL / UPDATE
// / DAG-start a workflow through a tool. `build_mcp_tool_routes(role_auth=true)`
// installs `enforce_read_only_mcp_mutation` on every mutating tool. These
// tests drive each generated tool route directly (a read-only `Session`
// injected into request extensions, exactly as the embedder auth middleware
// would populate it) and assert the class boundary. A `403` is produced ONLY
// by the gate (the handlers themselves return 400/404/503 when the runtime is
// absent, never 403), so `status == 403` ⟺ the gate fired.

use autumn_harvest_plugin::mcp_tools::tool_route_specs;
use autumn_web::AppState;
use autumn_web::reexports::axum::Router;
use autumn_web::reexports::axum::body::{Body, to_bytes};
use autumn_web::reexports::http::{Method, Request, StatusCode};
use autumn_web::session::Session;
use std::collections::{HashMap, HashSet};
use tower::ServiceExt;

const MCP_PREFIX: &str = "/api/harvest/mcp";

fn readonly_session() -> Session {
    let mut d = HashMap::new();
    d.insert("role".to_string(), "harvest_readonly".to_string());
    Session::new_for_test("ro".to_string(), d)
}

fn admin_session() -> Session {
    let mut d = HashMap::new();
    d.insert("role".to_string(), "admin".to_string());
    Session::new_for_test("admin".to_string(), d)
}

/// Replace every `{param}` segment with a non-empty placeholder so the concrete
/// path routes to its own template. `bogus` is not a valid execution id, so
/// handle-taking read handlers fail fast (400) with no DB access.
fn concrete(path: &str) -> String {
    path.split('/')
        .map(|s| {
            if s.starts_with('{') && s.ends_with('}') {
                "bogus"
            } else {
                s
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// The full descriptor set exercised: a plain mcp workflow with an mcp update,
/// plus (under `unified-dag-execution`) an mcp DAG.
fn all_descriptors() -> Vec<autumn_harvest_plugin::mcp_tools::McpWorkflowDescriptor> {
    let mut workflows =
        vec![__autumn_workflow_info_order_flow().with_input_schema_fn(order_input_schema)];
    let updates = vec![__autumn_update_handler_info_approve()];
    #[cfg(feature = "unified-dag-execution")]
    let dags = vec![__autumn_dag_info_daily_etl_dag()];
    #[cfg(not(feature = "unified-dag-execution"))]
    let dags: Vec<DagInfo> = vec![];
    #[cfg(feature = "unified-dag-execution")]
    workflows.push(__autumn_workflow_info_daily_etl_dag());
    collect_descriptors(&workflows, &updates, &dags)
}

/// Set of route paths whose tool is a mutation ([`ToolKind::is_mutation`]).
fn mutation_paths(
    descriptors: &[autumn_harvest_plugin::mcp_tools::McpWorkflowDescriptor],
) -> HashSet<String> {
    let mut set = HashSet::new();
    for d in descriptors {
        for spec in tool_route_specs(MCP_PREFIX, d) {
            if spec.kind.is_mutation() {
                set.insert(spec.path.clone());
            }
        }
    }
    set
}

/// Build a raw axum router from the generated tool routes (exactly what
/// `AppBuilder::routes(routes)` does inside `Plugin::build`, minus the app
/// scaffolding), so a `Session` can be injected per request. Returns the app
/// plus `(Method, concrete_path, is_mutation)` for every route.
fn build_router(role_auth_enabled: bool) -> (Router, Vec<(Method, String)>, Vec<(Method, String)>) {
    let descriptors = all_descriptors();
    record_schemas(&descriptors);
    let muts = mutation_paths(&descriptors);
    let routes = build_mcp_tool_routes(
        MCP_PREFIX,
        &descriptors,
        &HarvestApiState::new(),
        None,
        role_auth_enabled,
    );

    let mut router: Router<AppState> = Router::new();
    let mut mutating = Vec::new();
    let mut reading = Vec::new();
    for route in routes {
        let entry = (route.method.clone(), concrete(route.path));
        if muts.contains(route.path) {
            mutating.push(entry);
        } else {
            reading.push(entry);
        }
        router = router.route(route.path, route.handler);
    }
    (router.with_state(AppState::for_test()), mutating, reading)
}

async fn drive(
    app: &Router,
    method: &Method,
    uri: &str,
    session: Option<Session>,
) -> (StatusCode, String) {
    let mut req = Request::builder()
        .method(method.clone())
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    if let Some(s) = session {
        req.extensions_mut().insert(s);
    }
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn readonly_principal_is_denied_mutating_mcp_tools_but_reaches_reads() {
    let (app, mutating, reading) = build_router(true);
    assert!(
        !mutating.is_empty() && !reading.is_empty(),
        "sanity: both mutating and read MCP tools present"
    );

    // A read-only principal → 403 on every mutating tool (start/signal/update/
    // dag-start), with the distinguishable denial body.
    for (method, path) in &mutating {
        let (status, body) = drive(&app, method, path, Some(readonly_session())).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "read-only principal must be 403'd on mutating MCP tool {method} {path}"
        );
        assert!(
            body.contains("read-only principal: mutation not permitted"),
            "gate 403 must carry the distinguishable marker, got: {body}"
        );
    }

    // A read-only principal → NOT 403 on read tools (status/watch): the gate is
    // only installed on mutations, so the handler runs (and fails fast on the
    // bogus handle without a DB).
    for (method, path) in &reading {
        let (status, _) = drive(&app, method, path, Some(readonly_session())).await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "read-only principal must reach read MCP tool {method} {path}"
        );
    }

    // Admin and unmarked principals are never gated (reach every tool).
    for (method, path) in mutating.iter().chain(reading.iter()) {
        let (admin, _) = drive(&app, method, path, Some(admin_session())).await;
        assert_ne!(
            admin,
            StatusCode::FORBIDDEN,
            "admin principal must reach MCP tool {method} {path}"
        );
        let (anon, _) = drive(&app, method, path, None).await;
        assert_ne!(
            anon,
            StatusCode::FORBIDDEN,
            "unmarked principal must reach MCP tool {method} {path}"
        );
    }
}

#[tokio::test]
async fn role_auth_off_leaves_mcp_tools_ungated() {
    // AC6: without `api_with_role_auth`, no gate is installed — even a read-only
    // principal reaches every MCP tool (the routes are byte-for-byte the pre-
    // #776 shape).
    let (app, mutating, reading) = build_router(false);
    for (method, path) in mutating.iter().chain(reading.iter()) {
        let (status, _) = drive(&app, method, path, Some(readonly_session())).await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "role-auth OFF: read-only principal must reach MCP tool {method} {path}"
        );
    }
}
