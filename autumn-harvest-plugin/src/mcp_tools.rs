//! MCP tool exposure for `#[workflow(mcp)]` workflows (issue #597).
//!
//! Maps each mcp-flagged workflow onto a correlated set of MCP tools served by
//! autumn-web's MCP layer (autumn#1117/#1118): `start_{wf}` (returns a durable
//! handle immediately), `{wf}_status` (reads state/progress by handle),
//! `signal_{wf}` (delivers a signal by handle), `{wf}_watch` (streaming
//! progress over MCP `notifications/progress`), plus one synchronous
//! `{wf}_update_{name}` tool per `#[update(workflow = "…", mcp)]` handler.
//!
//! Architecture: autumn-web derives MCP tools **only** from typed routes
//! registered via `AppBuilder::routes(...)` — the harvest management API
//! (mounted via `nest()`) is invisible to the tool catalog. This module
//! therefore generates dedicated per-workflow [`autumn_web::Route`] values at
//! `Plugin::build()` time, split into three layers:
//!
//! 1. a **pure descriptor layer** ([`collect_descriptors`],
//!    [`tool_route_specs`]) that is unit-testable without axum or a database;
//! 2. a **global schema map** feeding autumn-web's `ApiDoc.register_schemas`
//!    hook, so each `start_{wf}` tool's `inputSchema` is derived from the
//!    workflow's published `WorkflowInfo::input_schema` (issue #373) — no
//!    second, hand-maintained schema;
//! 3. a **route/handler layer** ([`build_mcp_tool_routes`]) whose thin axum
//!    handlers delegate to the same start/signal/update primitives the
//!    management API uses.
//!
//! Determinism: everything here runs at the HTTP edge. No `WorkflowEvent`
//! variant, no migration, no replay surface — the `mcp` flag is never
//! consulted by core execution.

use std::collections::HashMap;

use autumn_harvest::{UpdateHandlerInfo, WorkflowInfo};
use autumn_web::reexports::axum;
use axum::extract::{Path, Query};
use axum::{Extension, Json};

/// Default mount prefix for the generated tool routes when the plugin has no
/// management-API path configured.
pub const DEFAULT_MCP_TOOLS_PREFIX_BASE: &str = "/api/harvest";

/// Component-schema name for the shared start-tool response.
pub const START_RESULT_SCHEMA: &str = "HarvestMcpStartResult";
/// Component-schema name for the shared status-tool response.
pub const STATUS_RESULT_SCHEMA: &str = "HarvestMcpStatusResult";
/// Component-schema name for the permissive signal payload body.
pub const SIGNAL_PAYLOAD_SCHEMA: &str = "HarvestMcpSignalPayload";
/// Component-schema name for the shared signal-tool response.
pub const SIGNAL_ACK_SCHEMA: &str = "HarvestMcpSignalAck";
/// Component-schema name for the shared update-tool response.
pub const UPDATE_RESULT_SCHEMA: &str = "HarvestMcpUpdateResult";

/// One mcp-flagged update attached to an MCP-exposed workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpUpdateDescriptor {
    /// The update name (`UpdateHandlerInfo::name`).
    pub name: String,
    /// Best-effort Rust type name of the update input, surfaced in the tool
    /// description (updates have no published JSON schema yet).
    pub input_type_hint: String,
    /// Best-effort Rust type name of the update output.
    pub output_type_hint: String,
}

/// A workflow selected for MCP exposure, with everything the route layer
/// needs, materialised out of `WorkflowInfo` (schema fns already invoked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpWorkflowDescriptor {
    /// Workflow type name.
    pub name: String,
    /// Optional human-readable description (`#[workflow(description = "…")]`).
    pub description: Option<String>,
    /// Materialised input schema from `WorkflowInfo::input_schema` (issue
    /// #373). `None` = no published schema; the tool falls back to a
    /// permissive object schema.
    pub input_schema: Option<serde_json::Value>,
    /// The mcp-flagged updates belonging to this workflow, sorted by name.
    pub updates: Vec<McpUpdateDescriptor>,
}

/// Which member of the per-workflow tool set a [`ToolRouteSpec`] describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolKind {
    /// `start_{wf}` — POST, returns the durable handle immediately.
    Start,
    /// `{wf}_status` — GET, reads state/progress by handle.
    Status,
    /// `signal_{wf}` — POST, delivers a signal by handle.
    Signal,
    /// `{wf}_update_{name}` — POST, synchronous update by handle.
    Update,
    /// `{wf}_watch` — GET, streaming progress (MCP `notifications/progress`).
    Watch,
}

/// Transport-agnostic description of one generated tool route. Consumed by
/// the route layer and asserted directly in unit tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRouteSpec {
    /// Which tool this is.
    pub kind: ToolKind,
    /// HTTP method (uppercase, e.g. `"POST"`).
    pub method: &'static str,
    /// Route path with `{param}` placeholders, absolute (starts with the
    /// prefix passed to [`tool_route_specs`]).
    pub path: String,
    /// MCP tool name (= `ApiDoc::operation_id`).
    pub operation_id: String,
    /// One-line human summary (tool `annotations.title`).
    pub summary: String,
    /// Longer description surfaced as the tool `description`.
    pub description: String,
    /// Path parameter names, in path order.
    pub path_params: Vec<&'static str>,
    /// Component-schema name for the JSON request body, when the tool takes
    /// one.
    pub body_component: Option<String>,
    /// Component-schema name for the JSON success response. `None` only for
    /// the streaming watch tool (exempt from the JSON-out eligibility gate).
    pub response_component: Option<&'static str>,
    /// `true` for the SSE-backed streaming watch tool (`ApiDoc::mcp_stream`).
    pub stream: bool,
    /// Owning workflow name.
    pub workflow: String,
    /// Update name, for `ToolKind::Update` specs.
    pub update: Option<String>,
}

/// Component-schema name carrying the typed input of one workflow's start
/// tool.
#[must_use]
pub fn input_schema_component(workflow: &str) -> String {
    format!("HarvestMcpInput_{workflow}")
}

/// Component-schema name carrying the (hint-only) input of one update tool.
#[must_use]
pub fn update_input_schema_component(workflow: &str, update: &str) -> String {
    format!("HarvestMcpUpdateInput_{workflow}_{update}")
}

/// Select the mcp-flagged workflows and attach their mcp-flagged updates.
///
/// Pure: no I/O, no state. Workflows are returned sorted by name; duplicate
/// workflow registrations keep the first and log a warning; updates whose
/// `workflow` does not match any mcp-enabled workflow are ignored (they may
/// belong to a non-exposed workflow, which is not an error).
#[must_use]
pub fn collect_descriptors(
    workflows: &[WorkflowInfo],
    updates: &[UpdateHandlerInfo],
) -> Vec<McpWorkflowDescriptor> {
    let mut descriptors: Vec<McpWorkflowDescriptor> = Vec::new();
    for info in workflows.iter().filter(|w| w.mcp) {
        if descriptors.iter().any(|d| d.name == info.name) {
            tracing::warn!(
                workflow = info.name,
                "duplicate mcp workflow registration; keeping the first"
            );
            continue;
        }
        // A debounced or batched start defers admission (the scanner fires
        // it later) and returns 202 Accepted with no execution_id yet -- but
        // every generated tool (start_{wf} included) promises a durable
        // handle immediately, and status_{wf}/signal_{wf}/{wf}_watch/updates
        // all require one. Excluding the workflow here (rather than
        // surfacing a broken start_{wf} tool that sometimes has no handle to
        // give back) keeps the "the handle is the correlation token"
        // contract true for every exposed workflow.
        if info.debounce.is_some() || info.batch.is_some() {
            tracing::warn!(
                workflow = info.name,
                "workflow has a debounce or batch policy configured; excluding it from \
                 mcp_tools() exposure, since a deferred start cannot return the durable \
                 handle every generated tool promises immediately"
            );
            continue;
        }
        let mut wf_updates: Vec<McpUpdateDescriptor> = Vec::new();
        for u in updates.iter().filter(|u| u.mcp && u.workflow == info.name) {
            if wf_updates.iter().any(|existing| existing.name == u.name) {
                tracing::warn!(
                    workflow = info.name,
                    update = u.name,
                    "duplicate mcp update registration; keeping the first"
                );
                continue;
            }
            wf_updates.push(McpUpdateDescriptor {
                name: u.name.to_string(),
                input_type_hint: u.input_type_hint.to_string(),
                output_type_hint: u.output_type_hint.to_string(),
            });
        }
        wf_updates.sort_by(|a, b| a.name.cmp(&b.name));
        descriptors.push(McpWorkflowDescriptor {
            name: info.name.to_string(),
            description: info.description.map(ToString::to_string),
            input_schema: info.input_schema.map(|f| f()),
            updates: wf_updates,
        });
    }
    descriptors.sort_by(|a, b| a.name.cmp(&b.name));
    descriptors
}

/// Expand one workflow descriptor into its full tool-route spec set:
/// start, status, signal, one update spec per attached update, and watch.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn tool_route_specs(prefix: &str, descriptor: &McpWorkflowDescriptor) -> Vec<ToolRouteSpec> {
    let wf = &descriptor.name;
    let about = descriptor
        .description
        .as_deref()
        .map_or_else(String::new, |d| format!(" {d}"));

    let mut specs = vec![
        ToolRouteSpec {
            kind: ToolKind::Start,
            method: "POST",
            path: format!("{prefix}/workflows/{wf}/start"),
            operation_id: format!("start_{wf}"),
            summary: format!("Start the '{wf}' workflow"),
            description: format!(
                "Start a durable '{wf}' workflow execution.{about} Returns a workflow \
                 handle immediately without blocking to completion; the work survives \
                 daemon restarts and does not require the caller to stay connected. \
                 Correlate follow-up calls ({wf}_status, signal_{wf}, {wf}_watch) with \
                 the returned execution_id handle."
            ),
            path_params: vec![],
            body_component: Some(input_schema_component(wf)),
            response_component: Some(START_RESULT_SCHEMA),
            stream: false,
            workflow: wf.clone(),
            update: None,
        },
        ToolRouteSpec {
            kind: ToolKind::Status,
            method: "GET",
            path: format!("{prefix}/workflows/{wf}/{{handle}}/status"),
            operation_id: format!("{wf}_status"),
            summary: format!("Read the status of a '{wf}' workflow execution"),
            description: format!(
                "Read the durable state of a '{wf}' execution by handle: lifecycle \
                 state, human-readable progress (current_details), output or error \
                 when terminal."
            ),
            path_params: vec!["handle"],
            body_component: None,
            response_component: Some(STATUS_RESULT_SCHEMA),
            stream: false,
            workflow: wf.clone(),
            update: None,
        },
        ToolRouteSpec {
            kind: ToolKind::Signal,
            method: "POST",
            path: format!("{prefix}/workflows/{wf}/{{handle}}/signal/{{signal_name}}"),
            operation_id: format!("signal_{wf}"),
            summary: format!("Send a signal to a '{wf}' workflow execution"),
            description: format!(
                "Deliver an asynchronous signal to a running '{wf}' execution by \
                 handle, unblocking any wait_for_signal/receive_signal in the \
                 workflow body. The JSON body is the signal payload."
            ),
            path_params: vec!["handle", "signal_name"],
            body_component: Some(SIGNAL_PAYLOAD_SCHEMA.to_string()),
            response_component: Some(SIGNAL_ACK_SCHEMA),
            stream: false,
            workflow: wf.clone(),
            update: None,
        },
    ];

    for u in &descriptor.updates {
        specs.push(ToolRouteSpec {
            kind: ToolKind::Update,
            method: "POST",
            path: format!("{prefix}/workflows/{wf}/{{handle}}/update/{}", u.name),
            operation_id: format!("{wf}_update_{}", u.name),
            summary: format!("Execute the '{}' update on a '{wf}' execution", u.name),
            description: format!(
                "Synchronously execute the '{}' update on a running '{wf}' execution \
                 by handle and return its result (input type: {}, output type: {}). \
                 Unlike a signal, an update is request/response: it is validated, \
                 admitted to durable history, executed by the workflow, and its \
                 outcome returned in this call.",
                u.name, u.input_type_hint, u.output_type_hint
            ),
            path_params: vec!["handle"],
            body_component: Some(update_input_schema_component(wf, &u.name)),
            response_component: Some(UPDATE_RESULT_SCHEMA),
            stream: false,
            workflow: wf.clone(),
            update: Some(u.name.clone()),
        });
    }

    specs.push(ToolRouteSpec {
        kind: ToolKind::Watch,
        method: "GET",
        path: format!("{prefix}/workflows/{wf}/{{handle}}/watch"),
        operation_id: format!("{wf}_watch"),
        summary: format!("Stream live progress of a '{wf}' workflow execution"),
        description: format!(
            "Subscribe to live progress of a '{wf}' execution by handle over \
             streaming MCP (notifications/progress) instead of polling \
             {wf}_status. Emits a progress frame per durable workflow event \
             (message = current_details when set) and terminates with the final \
             state when the execution reaches a terminal state."
        ),
        path_params: vec!["handle"],
        body_component: None,
        response_component: None,
        stream: true,
        workflow: wf.clone(),
        update: None,
    });

    specs
}

/// Derive the route prefix for the generated tools.
///
/// `prefix_override` wins when set; otherwise `{api_path}/mcp`, falling back
/// to `/api/harvest/mcp` when the plugin has no management API mounted.
#[must_use]
pub fn tools_prefix(api_path: Option<&str>, prefix_override: Option<&str>) -> String {
    prefix_override.map_or_else(
        || {
            let base = api_path
                .unwrap_or(DEFAULT_MCP_TOOLS_PREFIX_BASE)
                .trim_end_matches('/');
            format!("{base}/mcp")
        },
        |p| p.trim_end_matches('/').to_string(),
    )
}

// ── Global schema map (layer 2) ──────────────────────────────────────────────
//
// autumn-web resolves `SchemaEntry::Ref` component names against the schemas
// registered during spec generation. `ApiDoc.register_schemas` is a plain fn
// pointer (no closures), so per-workflow schemas — only known at plugin build
// time — are staged in a process-global map that the static hook drains.
// Mirrors the `GLOBAL_WORKFLOW_METADATA` precedent in `autumn_harvest::worker`.

static MCP_SCHEMAS: std::sync::OnceLock<std::sync::Mutex<HashMap<String, serde_json::Value>>> =
    std::sync::OnceLock::new();

fn schema_map() -> &'static std::sync::Mutex<HashMap<String, serde_json::Value>> {
    MCP_SCHEMAS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// The static `ApiDoc::register_schemas` hook attached to every generated
/// tool route. Inserts everything staged by [`record_schemas`].
fn register_harvest_mcp_schemas(registry: &mut autumn_web::openapi::SchemaRegistry) {
    let map = schema_map()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (name, schema) in map.iter() {
        registry.insert(name.clone(), schema.clone());
    }
}

/// Permissive schema used when a workflow (or update) publishes no
/// `input_schema`. Deliberately carries **no** `"type"` constraint: per
/// CLAUDE.md's "Multi-param dispatch packs into JSON array" convention, an
/// unpublished workflow's actual expected input can be a JSON object
/// (single struct param), an array (multi-param, positional), or a bare
/// scalar (single non-object param) — asserting `"type": "object"` here
/// would make a validating MCP client reject perfectly valid array/scalar
/// input before it ever reaches `start_tool`'s own schema check. Mirrors
/// [`SIGNAL_PAYLOAD_SCHEMA`], the existing "arbitrary JSON" fallback.
fn permissive_object_schema(description: &str) -> serde_json::Value {
    serde_json::json!({ "description": description })
}

/// Record the descriptors' schemas into the process-global schema map.
///
/// Covers each workflow's input schema plus the fixed response/payload
/// schemas, consumed by [`register_harvest_mcp_schemas`]. Idempotent,
/// name-keyed: re-recording the same workflow overwrites with identical
/// content.
pub fn record_schemas(descriptors: &[McpWorkflowDescriptor]) {
    let mut map = schema_map()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    map.insert(
        START_RESULT_SCHEMA.to_string(),
        serde_json::json!({
            "type": "object",
            "description": "Durable workflow handle returned immediately by a start tool.",
            "properties": {
                "execution_id": {
                    "type": "string",
                    "description": "The workflow handle. Pass it to the status/signal/update/watch tools."
                },
                "workflow_name": { "type": "string" },
                "workflow_id": { "type": "string" },
                "state": { "type": "string" }
            },
            "required": ["execution_id", "workflow_name", "workflow_id", "state"]
        }),
    );
    map.insert(
        STATUS_RESULT_SCHEMA.to_string(),
        serde_json::json!({
            "type": "object",
            "description": "Durable state snapshot of one workflow execution.",
            "properties": {
                "execution_id": { "type": "string" },
                "workflow_name": { "type": "string" },
                "workflow_id": { "type": "string" },
                "state": { "type": "string" },
                "is_terminal": { "type": "boolean" },
                "current_details": {
                    "type": ["string", "null"],
                    "description": "Author-published progress breadcrumb (ctx.set_current_details)."
                },
                "output": { "description": "Workflow output when state = COMPLETED." },
                "error": { "type": ["string", "null"] },
                "started_at": { "type": "string", "format": "date-time" },
                "completed_at": { "type": ["string", "null"], "format": "date-time" }
            },
            "required": ["execution_id", "workflow_name", "workflow_id", "state", "is_terminal"]
        }),
    );
    map.insert(
        SIGNAL_PAYLOAD_SCHEMA.to_string(),
        serde_json::json!({
            "description": "Arbitrary JSON signal payload delivered to the workflow."
        }),
    );
    map.insert(
        SIGNAL_ACK_SCHEMA.to_string(),
        serde_json::json!({
            "type": "object",
            "properties": {
                "ok": { "type": "boolean" },
                "signal_delivered": {
                    "type": "boolean",
                    "description": "false when an idempotency key deduplicated the delivery."
                }
            },
            "required": ["ok"]
        }),
    );
    map.insert(
        UPDATE_RESULT_SCHEMA.to_string(),
        serde_json::json!({
            "type": "object",
            "description": "Synchronous update outcome.",
            "properties": {
                "update_id": { "type": "string" },
                "output": { "description": "Handler result when the update completed." },
                "error": { "type": ["string", "null"] }
            },
            "required": ["update_id"]
        }),
    );

    for d in descriptors {
        insert_schema_warn_on_divergence(
            &mut map,
            input_schema_component(&d.name),
            d.input_schema.clone().unwrap_or_else(|| {
                permissive_object_schema(&format!(
                    "Input for the '{}' workflow (no published schema; see issue #373 \
                     to publish one)",
                    d.name
                ))
            }),
        );
        for u in &d.updates {
            insert_schema_warn_on_divergence(
                &mut map,
                update_input_schema_component(&d.name, &u.name),
                permissive_object_schema(&format!(
                    "Input for the '{}' update (Rust type: {})",
                    u.name, u.input_type_hint
                )),
            );
        }
    }
    drop(map);
}

/// Insert into the process-global schema map, logging a loud warning first
/// when an existing entry with the same name carries genuinely different
/// content.
///
/// `MCP_SCHEMAS` has no per-`HarvestPlugin`-instance scoping — it can't,
/// since autumn-web's `ApiDoc::register_schemas` hook is a bare fn pointer
/// with no captured state (see the module doc above `MCP_SCHEMAS`). Two
/// `HarvestPlugin::mcp_tools()` builds in one process (parallel tests, or a
/// multi-tenant embedder) that register a same-named workflow with
/// different schemas will still have the later build's schema win — this
/// cannot be fixed without an upstream autumn-web change — but the collision
/// is no longer silent.
fn insert_schema_warn_on_divergence(
    map: &mut HashMap<String, serde_json::Value>,
    name: String,
    schema: serde_json::Value,
) {
    if let Some(existing) = map.get(&name)
        && *existing != schema
    {
        tracing::warn!(
            schema_name = %name,
            "mcp tool schema '{name}' was already registered with different content by \
             another HarvestPlugin build in this process; the two builds' advertised \
             inputSchema will clobber each other (process-global schema map, no \
             per-instance scoping — see MCP_SCHEMAS doc comment)"
        );
    }
    map.insert(name, schema);
}

// ── Route/handler layer (layer 3) ────────────────────────────────────────────

/// Leak a string into a `&'static str` for `ApiDoc`/`Route` fields. Bounded:
/// a handful of strings per mcp workflow, once per plugin build.
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// Build the typed autumn-web routes for every descriptor's tool set.
///
/// Each route carries an `ApiDoc` with `mcp_tool: true` (and `mcp_stream` for
/// watch) so autumn-web's `mount_mcp` projects it into the tool catalog; the
/// handlers delegate to the same primitives the management API uses, reading
/// runtime state from the given [`HarvestApiState`] (fail-closed 503 before
/// `on_startup` installs the runtime).
#[must_use]
pub fn build_mcp_tool_routes(
    prefix: &str,
    descriptors: &[McpWorkflowDescriptor],
    api_state: &crate::api::HarvestApiState,
    tool_middleware: Option<&crate::plugin::McpToolMiddlewareFn>,
) -> Vec<autumn_web::Route> {
    let mut routes = Vec::new();
    for descriptor in descriptors {
        for spec in tool_route_specs(prefix, descriptor) {
            routes.push(build_tool_route(&spec, api_state.clone(), tool_middleware));
        }
    }
    routes
}

fn build_tool_route(
    spec: &ToolRouteSpec,
    api_state: crate::api::HarvestApiState,
    tool_middleware: Option<&crate::plugin::McpToolMiddlewareFn>,
) -> autumn_web::Route {
    use axum::routing::MethodRouter;

    let path: &'static str = leak(spec.path.clone());
    let operation_id: &'static str = leak(spec.operation_id.clone());
    let workflow: &'static str = leak(spec.workflow.clone());

    let handler: MethodRouter<autumn_web::AppState> = match spec.kind {
        ToolKind::Start => axum::routing::post(
            move |headers: axum::http::HeaderMap, Json(body): Json<serde_json::Value>| {
                let api_state = api_state.clone();
                // Boxed: the delegated start handler's future is large
                // (clippy::large_futures) and this is a cold edge path.
                async move { Box::pin(start_tool(api_state, workflow, headers, body)).await }
            },
        ),
        ToolKind::Status => axum::routing::get(move |Path(handle): Path<String>| {
            let api_state = api_state.clone();
            async move { status_tool(api_state, workflow, &handle).await }
        }),
        ToolKind::Signal => axum::routing::post(
            move |Path((handle, signal_name)): Path<(String, String)>,
                  headers: axum::http::HeaderMap,
                  Json(payload): Json<serde_json::Value>| {
                let api_state = api_state.clone();
                async move {
                    signal_tool(api_state, workflow, handle, signal_name, headers, payload).await
                }
            },
        ),
        ToolKind::Update => {
            let update: &'static str = leak(
                spec.update
                    .clone()
                    .expect("Update spec always carries the update name"),
            );
            axum::routing::post(
                move |Path(handle): Path<String>, Json(body): Json<serde_json::Value>| {
                    let api_state = api_state.clone();
                    async move { update_tool(api_state, workflow, update, handle, body).await }
                },
            )
        }
        ToolKind::Watch => axum::routing::get(move |Path(handle): Path<String>| {
            let api_state = api_state.clone();
            async move { watch_tool(api_state, workflow, &handle).await }
        }),
    };
    // Issue #597 hardening: these routes are registered via
    // `AppBuilder::routes(...)`, not `nest()`, so they never pass through
    // the harvest management API's own auth layer (which only wraps the
    // nested router) or autumn-web's `secure_mcp` (which only gates the
    // `/mcp` envelope, not a typed route's own HTTP path). Apply the same
    // layer `HarvestPlugin::api_with_auth` configured directly to this
    // route's handler so it requires the same credential either way.
    let handler = match tool_middleware {
        Some(mw) => mw(handler),
        None => handler,
    };

    let api_doc = autumn_web::openapi::ApiDoc {
        method: spec.method,
        path,
        operation_id,
        summary: Some(leak(spec.summary.clone())),
        description: Some(leak(spec.description.clone())),
        path_params: Box::leak(spec.path_params.clone().into_boxed_slice()),
        request_body: spec
            .body_component
            .clone()
            .map(|name| autumn_web::openapi::SchemaEntry {
                name: leak(name),
                kind: autumn_web::openapi::SchemaKind::Ref,
            }),
        response: spec
            .response_component
            .map(|name| autumn_web::openapi::SchemaEntry {
                name,
                kind: autumn_web::openapi::SchemaKind::Ref,
            }),
        success_status: 200,
        register_schemas: Some(register_harvest_mcp_schemas),
        mcp_tool: true,
        mcp_stream: spec.stream,
        ..Default::default()
    };

    autumn_web::Route {
        method: method_for(spec.method),
        path,
        handler,
        name: operation_id,
        api_doc,
        api_version: None,
        sunset_opt_out: false,
        repository: None,
        idempotency: autumn_web::RouteIdempotency::Direct,
    }
}

fn method_for(method: &str) -> axum::http::Method {
    match method {
        "GET" => axum::http::Method::GET,
        "POST" => axum::http::Method::POST,
        other => unreachable!("unsupported MCP tool method {other}"),
    }
}

// ── Tool handlers ────────────────────────────────────────────────────────────

/// Parse the handle, route to the owning shard, load the execution row, and
/// verify it belongs to `workflow`. A mismatching handle returns the same 404
/// an unknown handle does, so one workflow's tools cannot be used as an
/// existence oracle for another's executions.
async fn load_owned_execution(
    api_state: &crate::api::HarvestApiState,
    workflow: &str,
    handle: &str,
) -> Result<autumn_harvest::models::WorkflowExecution, axum::response::Response> {
    use axum::response::IntoResponse;

    let exec_id = crate::api::parse_execution_id(handle).map_err(IntoResponse::into_response)?;
    let mut conn = crate::api::db_conn_for_execution(api_state, exec_id)
        .await
        .map_err(IntoResponse::into_response)?;
    let execution = crate::api::load_execution(&mut conn, exec_id)
        .await
        .map_err(|e| crate::api::map_error(e).into_response())?;
    if execution.workflow_name != workflow {
        return Err(autumn_web::error::AutumnError::not_found_msg(format!(
            "workflow execution {handle}"
        ))
        .into_response());
    }
    Ok(execution)
}

/// `start_{wf}` — delegate straight to the management API's start handler
/// with only the input set (server-generated `workflow_id`).
///
/// Input-schema validation is intentionally *not* duplicated here: the
/// delegated `start_workflow` already validates against the workflow's
/// published `WorkflowInfo::input_schema` (issue #373), sourced from the
/// live runtime registry rather than a schema snapshot closure-captured at
/// `Plugin::build()` time, and — unlike an earlier version of this
/// function — writes the audit record for a rejected request. Duplicating
/// the check here bought no earlier failure (the validation itself is
/// pure/in-memory either way) at the cost of two independent sources of
/// truth for the same schema and a rejected MCP-originated attempt leaving
/// no audit trail.
async fn start_tool(
    api_state: crate::api::HarvestApiState,
    workflow: &'static str,
    headers: axum::http::HeaderMap,
    body: serde_json::Value,
) -> axum::response::Response {
    Box::pin(crate::api::start_workflow(
        Extension(api_state),
        Path(workflow.to_string()),
        None,
        headers,
        Json(crate::api::StartWorkflowRequest::from_input(body)),
    ))
    .await
}

/// Response body of `{wf}_status` — a compact durable-state snapshot.
#[derive(Debug, serde::Serialize)]
struct McpStatusResponse {
    execution_id: String,
    workflow_name: String,
    workflow_id: String,
    state: String,
    is_terminal: bool,
    current_details: Option<String>,
    output: Option<serde_json::Value>,
    error: Option<String>,
    started_at: chrono::DateTime<chrono::Utc>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl McpStatusResponse {
    fn from_execution(execution: autumn_harvest::models::WorkflowExecution) -> Self {
        let is_terminal = crate::api::is_terminal_state(&execution.state);
        Self {
            execution_id: execution.id.to_string(),
            workflow_name: execution.workflow_name,
            workflow_id: execution.workflow_id,
            state: execution.state,
            is_terminal,
            current_details: execution.current_details,
            output: execution.output,
            error: execution.error,
            started_at: execution.started_at,
            completed_at: execution.completed_at,
        }
    }
}

/// `{wf}_status` — read the durable execution row.
///
/// A `FAILED` or `CONTINUED_AS_NEW` row is resolved through the same
/// retry/continue-as-new chain-following the `/result` HTTP endpoint uses
/// (issue #527), so a continued-as-new run's status reflects its eventual
/// successor's real state/output/error instead of a dead-end sentinel with
/// null output. Other states need no extra lookup.
async fn status_tool(
    api_state: crate::api::HarvestApiState,
    workflow: &'static str,
    handle: &str,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let execution = match load_owned_execution(&api_state, workflow, handle).await {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    let execution = match resolve_if_chained(&api_state, execution).await {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    Json(McpStatusResponse::from_execution(execution)).into_response()
}

/// Follow the retry/continue-as-new chain when `execution` is a dead-end
/// (`FAILED` or `CONTINUED_AS_NEW`) row; returns `execution` unchanged
/// otherwise, avoiding a redundant reload for the common terminal/running
/// case (`load_owned_execution` already loaded it once).
async fn resolve_if_chained(
    api_state: &crate::api::HarvestApiState,
    execution: autumn_harvest::models::WorkflowExecution,
) -> Result<autumn_harvest::models::WorkflowExecution, axum::response::Response> {
    if !matches!(execution.state.as_str(), "FAILED" | "CONTINUED_AS_NEW") {
        return Ok(execution);
    }
    let exec_id = autumn_harvest::ExecutionId::from_uuid(execution.id);
    crate::api::resolve_terminal_workflow_execution(api_state, exec_id)
        .await
        .map_err(autumn_web::prelude::IntoResponse::into_response)
}

/// `signal_{wf}` — ownership check, then delegate to the standalone signal
/// handler (inherits Idempotency-Key header support).
async fn signal_tool(
    api_state: crate::api::HarvestApiState,
    workflow: &'static str,
    handle: String,
    signal_name: String,
    headers: axum::http::HeaderMap,
    payload: serde_json::Value,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let execution = match load_owned_execution(&api_state, workflow, &handle).await {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    // Resolve to the live successor if the caller's durable handle names a
    // predecessor that has since continued-as-new: the underlying signal
    // path rejects a terminal execution, so signalling with the stale
    // predecessor id would fail even though status_/watch_ already follow
    // the chain and report the run as live.
    let execution = match resolve_if_chained(&api_state, execution).await {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    crate::api::signal_workflow(
        Extension(api_state),
        Path((execution.id.to_string(), signal_name)),
        Query(crate::api::SignalQuery::default()),
        headers,
        Json(payload),
    )
    .await
    .into_response()
}

/// `{wf}_update_{name}` — ownership check, then delegate to the update-admit
/// handler with its synchronous default (`wait=completed`, 30 s timeout).
async fn update_tool(
    api_state: crate::api::HarvestApiState,
    workflow: &'static str,
    update: &'static str,
    handle: String,
    body: serde_json::Value,
) -> axum::response::Response {
    let execution = match load_owned_execution(&api_state, workflow, &handle).await {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    // Same continue-as-new chain resolution as signal_tool: admitting an
    // update against a sealed predecessor id would fail even though the
    // workflow is still live under its successor.
    let execution = match resolve_if_chained(&api_state, execution).await {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    crate::api::admit_update(
        Extension(api_state),
        Path((execution.id.to_string(), update.to_string())),
        Query(crate::api::AdmitUpdateQuery::default()),
        Json(crate::api::AdmitUpdateRequest::new(body)),
    )
    .await
}

/// `{wf}_watch` — SSE stream of progress frames driven by the shard's
/// LISTEN/NOTIFY `harvest_events` channel (no polling). autumn-web's MCP layer
/// projects each frame as a `notifications/progress` message; the terminal
/// `event: result` frame becomes the final `tools/call` result.
///
/// Frame contract:
/// - progress frames: `{"progress": <n>, "message": <current_details|state>}`
///   (numeric `progress` opts into structured MCP progress forwarding);
/// - terminal frame: `event: result`, data `{"state", "output", "error"}`;
/// - lost-listener frame: `event: error`, data `{"error": <message>}`, sent
///   only if the stream's underlying LISTEN/NOTIFY connection is lost before
///   a terminal state is observed (see below).
///
/// Modeled on the management API's `stream_execution_events` (listener
/// connected *before* the snapshot read so no event is missed; bounded
/// channel; pooled connection released while idle). A `FAILED`-with-retry or
/// `CONTINUED_AS_NEW` row is resolved through the same chain-following
/// `{wf}_status` uses (issue #527), both at subscribe time and whenever a
/// continue-as-new happens mid-watch, so the terminal frame always reflects
/// the eventual successor's real outcome instead of a dead-end sentinel.
#[allow(clippy::too_many_lines)]
async fn watch_tool(
    api_state: crate::api::HarvestApiState,
    workflow: &'static str,
    handle: &str,
) -> axum::response::Response {
    use autumn_harvest::notify::{WorkflowEventListener, WorkflowEventWaitOutcome};
    use axum::response::IntoResponse as _;
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures::SinkExt as _;

    let exec_id = match crate::api::parse_execution_id(handle) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    // LISTEN before the snapshot read: an event committed between the two is
    // caught by the first notification instead of being lost. Continue-as-new
    // successors always live on the same shard, so one listener connection
    // covers the whole chain even if we later jump to a successor's exec_id.
    let notification_url = match api_state.sse_notification_url(exec_id.shard()) {
        Ok(url) => url,
        Err(e) => return crate::api::map_error(e).into_response(),
    };
    let listener = match WorkflowEventListener::connect(&notification_url).await {
        Ok(l) => l,
        Err(e) => return crate::api::map_error(e).into_response(),
    };

    let execution = match load_owned_execution(&api_state, workflow, handle).await {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    // Jump straight to the active successor if the handle already names a
    // dead-end row (matches `{wf}_status`'s chain-following).
    let execution = match resolve_if_chained(&api_state, execution).await {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    let exec_id = autumn_harvest::ExecutionId::from_uuid(execution.id);

    let keepalive_interval = api_state.sse_keepalive_interval();
    let buffer_depth = api_state.sse_buffer_depth();
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Event, std::convert::Infallible>>(
        buffer_depth.max(1),
    );

    let api_clone = api_state.clone();
    tokio::spawn(async move {
        let result_frame = |execution: &autumn_harvest::models::WorkflowExecution| {
            Event::default().event("result").data(
                serde_json::json!({
                    "state": execution.state,
                    "output": execution.output,
                    "error": execution.error,
                })
                .to_string(),
            )
        };
        let progress_frame = |n: u64, execution: &autumn_harvest::models::WorkflowExecution| {
            let message = execution
                .current_details
                .clone()
                .unwrap_or_else(|| execution.state.clone());
            Event::default()
                .data(serde_json::json!({ "progress": n, "message": message }).to_string())
        };

        // Already terminal: emit the result frame and close.
        if crate::api::is_terminal_state(&execution.state) {
            let _ = tx.send(Ok(result_frame(&execution))).await;
            return;
        }

        // Initial snapshot frame so the subscriber sees progress immediately.
        let mut progress: u64 = 1;
        if tx
            .send(Ok(progress_frame(progress, &execution)))
            .await
            .is_err()
        {
            return;
        }

        let mut listener = listener;
        let mut exec_id = exec_id;
        loop {
            // 2x keepalive so the KeepAlive wrapper pings between wakeups; a
            // TimedOut wakeup re-checks terminal state as a missed-NOTIFY
            // safety net without emitting a progress frame.
            let wait_timeout = keepalive_interval.saturating_mul(2);
            let emit_progress = match listener.wait_for_notification_timeout(wait_timeout).await {
                Ok(WorkflowEventWaitOutcome::Notification(payload)) => {
                    if payload.workflow_exec_id != exec_id.as_uuid() {
                        continue;
                    }
                    true
                }
                Ok(WorkflowEventWaitOutcome::TimedOut) => false,
                Ok(WorkflowEventWaitOutcome::ChannelClosed) | Err(_) => {
                    // The listener connection is gone: the caller would
                    // otherwise see the stream end with no terminal signal
                    // and no error, indistinguishable from "workflow still
                    // running, nothing to report yet". Say so explicitly.
                    let _ = tx
                        .send(Ok(Event::default().event("error").data(
                            serde_json::json!({
                                "error": "watch stream lost its event listener connection \
                                          before the execution reached a terminal state"
                            })
                            .to_string(),
                        )))
                        .await;
                    break;
                }
            };
            // A client that dropped the SSE connection during a quiet
            // (TimedOut, no notification) stretch would otherwise never be
            // detected here — `tx.send` is only reached below when there is
            // a frame to emit — leaking this task and its DB polling until
            // the watched execution eventually terminates.
            if tx.is_closed() {
                break;
            }

            let refreshed = match crate::api::db_conn_for_execution(&api_clone, exec_id).await {
                Ok(mut conn) => match crate::api::load_execution(&mut conn, exec_id).await {
                    Ok(execution) => execution,
                    Err(_) => continue,
                },
                Err(_) => continue,
            };
            // Chain resolution hiccup (e.g. a transient DB error): treat as
            // no update this tick and retry on the next wakeup rather than
            // tearing down the stream.
            let Ok(refreshed) = resolve_if_chained(&api_clone, refreshed).await else {
                continue;
            };
            if refreshed.id != exec_id.as_uuid() {
                // Continue-as-new happened mid-watch: track the successor
                // going forward so subsequent NOTIFY filtering and polling
                // target the execution that is actually still active.
                exec_id = autumn_harvest::ExecutionId::from_uuid(refreshed.id);
            }

            if crate::api::is_terminal_state(&refreshed.state) {
                let _ = tx.send(Ok(result_frame(&refreshed))).await;
                break;
            }
            if emit_progress {
                progress += 1;
                if tx
                    .send(Ok(progress_frame(progress, &refreshed)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });

    Sse::new(rx)
        .keep_alive(KeepAlive::new().interval(keepalive_interval))
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wf(name: &'static str, mcp: bool) -> WorkflowInfo {
        WorkflowInfo {
            name,
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            sla: None,
            concurrency: None,
            debounce: None,
            batch: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
            mcp,
        }
    }

    fn upd(name: &'static str, workflow: &'static str, mcp: bool) -> UpdateHandlerInfo {
        UpdateHandlerInfo {
            name,
            workflow,
            module: "tests",
            input_type_hint: "ApproveRequest",
            output_type_hint: "bool",
            has_validator: false,
            handler: |_ctx, _args| Box::pin(async move { Ok(serde_json::Value::Null) }),
            validator: None,
            mcp,
        }
    }

    fn order_input_schema() -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": { "order_id": {"type": "string"}, "amount": {"type": "integer"} },
            "required": ["order_id"]
        })
    }

    #[test]
    fn collect_descriptors_filters_non_mcp_workflows() {
        let descriptors = collect_descriptors(&[wf("plain", false), wf("exposed", true)], &[]);
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].name, "exposed");
        assert!(descriptors[0].updates.is_empty());
    }

    /// Code-review regression test (issue #597, PR #908): a debounced or
    /// batched start defers admission and can return 202 Accepted with no
    /// `execution_id`, breaking every generated tool's "durable handle
    /// immediately" contract. Such workflows must never be exposed.
    #[test]
    fn collect_descriptors_excludes_debounced_and_batched_workflows() {
        let debounced =
            wf("debounced_flow", true).with_debounce(autumn_harvest::debounce::DebouncePolicy {
                key_expr: "tenant_id",
                window: std::time::Duration::from_secs(30),
                max_wait: None,
            });
        let batched =
            wf("batched_flow", true).with_batch(autumn_harvest::event_batch::BatchPolicy {
                key_expr: "tenant_id".to_string(),
                max_size: 10,
                max_wait: std::time::Duration::from_secs(10),
            });
        let plain = wf("plain_flow", true);

        let descriptors = collect_descriptors(&[debounced, batched, plain], &[]);

        let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["plain_flow"],
            "debounced/batched workflows must be excluded, plain ones kept"
        );
    }

    #[test]
    fn collect_descriptors_materialises_schema_and_description() {
        let info = wf("order_flow", true)
            .with_description("Processes an order")
            .with_input_schema_fn(order_input_schema);
        let descriptors = collect_descriptors(&[info], &[]);
        assert_eq!(
            descriptors[0].description.as_deref(),
            Some("Processes an order")
        );
        assert_eq!(descriptors[0].input_schema, Some(order_input_schema()));
    }

    #[test]
    fn collect_descriptors_attaches_only_matching_mcp_updates() {
        let descriptors = collect_descriptors(
            &[wf("order_flow", true), wf("hidden_flow", false)],
            &[
                upd("approve", "order_flow", true),
                upd("reject", "order_flow", false), // not mcp-flagged
                upd("orphan", "hidden_flow", true), // workflow not exposed
                upd("other", "missing_flow", true), // workflow not registered
            ],
        );
        assert_eq!(descriptors.len(), 1);
        let updates: Vec<&str> = descriptors[0]
            .updates
            .iter()
            .map(|u| u.name.as_str())
            .collect();
        assert_eq!(updates, vec!["approve"]);
        assert_eq!(descriptors[0].updates[0].input_type_hint, "ApproveRequest");
    }

    #[test]
    fn collect_descriptors_dedupes_workflows_and_updates_and_sorts() {
        let descriptors = collect_descriptors(
            &[wf("b_flow", true), wf("a_flow", true), wf("b_flow", true)],
            &[
                upd("approve", "a_flow", true),
                upd("approve", "a_flow", true),
                upd("zz", "a_flow", true),
            ],
        );
        let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["a_flow", "b_flow"], "sorted, deduped");
        let updates: Vec<&str> = descriptors[0]
            .updates
            .iter()
            .map(|u| u.name.as_str())
            .collect();
        assert_eq!(updates, vec!["approve", "zz"], "sorted, deduped");
    }

    #[test]
    fn tool_route_specs_emit_the_full_tool_set() {
        let descriptor = McpWorkflowDescriptor {
            name: "order_flow".into(),
            description: Some("Processes an order".into()),
            input_schema: Some(order_input_schema()),
            updates: vec![McpUpdateDescriptor {
                name: "approve".into(),
                input_type_hint: "ApproveRequest".into(),
                output_type_hint: "bool".into(),
            }],
        };
        let specs = tool_route_specs("/api/harvest/mcp", &descriptor);

        let ids: Vec<&str> = specs.iter().map(|s| s.operation_id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "start_order_flow",
                "order_flow_status",
                "signal_order_flow",
                "order_flow_update_approve",
                "order_flow_watch",
            ]
        );

        let start = &specs[0];
        assert_eq!(start.kind, ToolKind::Start);
        assert_eq!(start.method, "POST");
        assert_eq!(start.path, "/api/harvest/mcp/workflows/order_flow/start");
        assert!(start.path_params.is_empty());
        assert_eq!(
            start.body_component.as_deref(),
            Some("HarvestMcpInput_order_flow")
        );
        assert_eq!(start.response_component, Some(START_RESULT_SCHEMA));
        assert!(!start.stream);

        let status = &specs[1];
        assert_eq!(status.method, "GET");
        assert_eq!(
            status.path,
            "/api/harvest/mcp/workflows/order_flow/{handle}/status"
        );
        assert_eq!(status.path_params, vec!["handle"]);
        assert!(status.body_component.is_none());
        assert_eq!(status.response_component, Some(STATUS_RESULT_SCHEMA));

        let signal = &specs[2];
        assert_eq!(signal.method, "POST");
        assert_eq!(
            signal.path,
            "/api/harvest/mcp/workflows/order_flow/{handle}/signal/{signal_name}"
        );
        assert_eq!(signal.path_params, vec!["handle", "signal_name"]);
        assert_eq!(
            signal.body_component.as_deref(),
            Some(SIGNAL_PAYLOAD_SCHEMA)
        );

        let update = &specs[3];
        assert_eq!(update.kind, ToolKind::Update);
        assert_eq!(update.method, "POST");
        assert_eq!(
            update.path,
            "/api/harvest/mcp/workflows/order_flow/{handle}/update/approve"
        );
        assert_eq!(update.path_params, vec!["handle"]);
        assert_eq!(
            update.body_component.as_deref(),
            Some("HarvestMcpUpdateInput_order_flow_approve")
        );
        assert_eq!(update.update.as_deref(), Some("approve"));
        assert!(
            update.description.contains("ApproveRequest"),
            "update tool description must carry the input type hint; got: {}",
            update.description
        );

        let watch = &specs[4];
        assert_eq!(watch.kind, ToolKind::Watch);
        assert_eq!(watch.method, "GET");
        assert_eq!(
            watch.path,
            "/api/harvest/mcp/workflows/order_flow/{handle}/watch"
        );
        assert!(watch.stream, "watch must be a streaming (mcp_stream) tool");
        assert!(watch.response_component.is_none());

        for spec in &specs {
            assert_eq!(spec.workflow, "order_flow");
        }
    }

    #[test]
    fn tool_route_specs_description_falls_back_when_absent() {
        let descriptor = McpWorkflowDescriptor {
            name: "bare".into(),
            description: None,
            input_schema: None,
            updates: vec![],
        };
        let specs = tool_route_specs("/x", &descriptor);
        assert_eq!(specs.len(), 4, "start/status/signal/watch — no updates");
        assert!(specs.iter().all(|s| !s.description.is_empty()));
    }

    #[test]
    fn tools_prefix_derivation() {
        assert_eq!(tools_prefix(None, None), "/api/harvest/mcp");
        assert_eq!(tools_prefix(Some("/api/harvest"), None), "/api/harvest/mcp");
        assert_eq!(tools_prefix(Some("/custom"), None), "/custom/mcp");
        assert_eq!(
            tools_prefix(Some("/custom/"), None),
            "/custom/mcp",
            "trailing slash trimmed"
        );
        assert_eq!(
            tools_prefix(Some("/x"), Some("/tools")),
            "/tools",
            "override wins"
        );
        assert_eq!(
            tools_prefix(None, Some("/tools/")),
            "/tools",
            "override trailing slash trimmed"
        );
    }

    /// Code-review regression test (issue #597): the fallback schema must
    /// not assert `"type": "object"` — a multi-param (array-input) or
    /// scalar-input workflow with no published schema would otherwise
    /// advertise an `inputSchema` that rejects its own valid input.
    #[test]
    fn permissive_object_schema_carries_no_type_constraint() {
        let schema = permissive_object_schema("some description");
        assert!(
            schema.get("type").is_none(),
            "fallback schema must not assert a type, got: {schema}"
        );
        assert_eq!(schema["description"], "some description");
    }

    /// Code-review regression test (issue #597): `record_schemas` is called
    /// repeatedly (once per `Plugin::build()`); re-recording the same
    /// workflow with identical content must stay a no-op (idempotent), and
    /// re-recording with *different* content must still resolve to the
    /// latest call's schema (documented last-write-wins, not a panic or a
    /// stuck stale entry).
    ///
    /// Uses a workflow name not shared with any other test in this module:
    /// `MCP_SCHEMAS` is a process-global map and `cargo test` runs tests in
    /// parallel by default, so asserting on a shared name's content (e.g.
    /// the file's common `"order_flow"` fixture, used by several other
    /// tests here) would be a genuine cross-test race.
    #[test]
    fn record_schemas_is_idempotent_and_last_write_wins_on_divergence() {
        let first = collect_descriptors(&[wf("review_fix_schema_probe", true)], &[]);
        record_schemas(&first);
        record_schemas(&first); // identical re-registration: must not panic

        let diverged = collect_descriptors(
            &[wf("review_fix_schema_probe", true).with_input_schema_fn(order_input_schema)],
            &[],
        );
        record_schemas(&diverged); // divergent re-registration: must not panic

        let recorded = schema_map()
            .lock()
            .unwrap()
            .get(&input_schema_component("review_fix_schema_probe"))
            .cloned();
        assert_eq!(
            recorded,
            Some(order_input_schema()),
            "the latest build's schema must win"
        );
    }

    /// Regression test for the auth-bypass finding from the #597 code
    /// review: MCP tool routes are registered via `AppBuilder::routes(...)`,
    /// not `nest()`, so without an explicit applier they never pass through
    /// `HarvestPlugin::api_with_auth`'s middleware. This proves
    /// `build_mcp_tool_routes`'s `tool_middleware` parameter, when supplied,
    /// actually wraps every generated route's handler.
    #[tokio::test]
    async fn mcp_tool_routes_apply_the_configured_middleware() {
        use axum::response::IntoResponse as _;

        async fn deny_all(
            _req: axum::extract::Request,
            _next: axum::middleware::Next,
        ) -> axum::response::Response {
            axum::http::StatusCode::UNAUTHORIZED.into_response()
        }

        let deny_layer = axum::middleware::from_fn(deny_all);
        let tool_middleware: crate::plugin::McpToolMiddlewareFn =
            std::sync::Arc::new(move |mr| mr.layer(deny_layer.clone()));

        let descriptors = collect_descriptors(&[wf("order_flow", true)], &[]);
        record_schemas(&descriptors);
        let routes = build_mcp_tool_routes(
            "/api/harvest/mcp",
            &descriptors,
            &crate::api::HarvestApiState::new(),
            Some(&tool_middleware),
        );

        let client = autumn_web::test::TestApp::new().routes(routes).build();

        let resp = client
            .post("/api/harvest/mcp/workflows/order_flow/start")
            .json(&serde_json::json!({}))
            .send()
            .await;
        resp.assert_status(401);

        let resp = client
            .get("/api/harvest/mcp/workflows/order_flow/some-handle/status")
            .send()
            .await;
        resp.assert_status(401);
    }

    /// Without a configured middleware (`None`, matching every existing
    /// test above), a request must still reach the real handler — i.e. this
    /// module's hardening does not accidentally start gating requests when
    /// the embedder never opted into `api_with_auth`.
    #[tokio::test]
    async fn mcp_tool_routes_are_unwrapped_when_no_middleware_is_configured() {
        let descriptors = collect_descriptors(&[wf("order_flow", true)], &[]);
        record_schemas(&descriptors);
        let routes = build_mcp_tool_routes(
            "/api/harvest/mcp",
            &descriptors,
            &crate::api::HarvestApiState::new(),
            None,
        );

        let client = autumn_web::test::TestApp::new().routes(routes).build();

        let resp = client
            .post("/api/harvest/mcp/workflows/order_flow/start")
            .json(&serde_json::json!({}))
            .send()
            .await;
        // No middleware to reject the request with 401; it reaches the real
        // handler, which fails closed with a "harvest runtime is not
        // started" 400 (no runtime is installed in this no-DB test) —
        // proving the request was NOT rejected at the (absent) auth layer.
        let resp = resp.assert_status(400);
        assert!(
            resp.text().contains("harvest runtime is not started"),
            "expected the real handler's fail-closed error, got: {}",
            resp.text()
        );
    }
}
