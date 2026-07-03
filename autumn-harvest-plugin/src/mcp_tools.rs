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

use autumn_harvest::{UpdateHandlerInfo, WorkflowInfo};

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
}
