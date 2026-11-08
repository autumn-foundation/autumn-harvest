//! The build-time warning for an `mcp` flag that no route serves (issue #2609).
//!
//! `#[workflow(mcp)]` marks a workflow. Only a mounted MCP route exposes it.
//! Without `mcp_tools()` or `mcp_tasks()`, or without the `mcp` feature, the
//! workflow runs and no tool appears. Both backends call this check once at
//! build, so one app logs one message.

use autumn_harvest::{UpdateHandlerInfo, WorkflowInfo};

/// Log one `WARN` when a registered `mcp` flag has no mounted MCP route.
///
/// `mounted` is true when the plugin mounts MCP tool or task routes.
pub fn warn_if_mcp_unserved(
    workflows: &[WorkflowInfo],
    updates: &[UpdateHandlerInfo],
    mounted: bool,
) {
    if mounted {
        return;
    }
    let flows: Vec<&str> = workflows
        .iter()
        .filter(|info| info.mcp)
        .map(|info| info.name)
        .collect();
    let upds: Vec<String> = updates
        .iter()
        .filter(|info| info.mcp)
        .map(|info| format!("{}::{}", info.workflow, info.name))
        .collect();
    if flows.is_empty() && upds.is_empty() {
        return;
    }
    tracing::warn!(
        workflows = %flows.join(","),
        updates = %upds.join(","),
        "these `mcp` workflows and updates expose no MCP tools, because no MCP route is mounted. \
         Enable the `mcp` feature of autumn-harvest-plugin. \
         Then call `HarvestPlugin::mcp_tools()` or `HarvestPlugin::mcp_tasks()`. \
         The workflows still run."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buf {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    fn warnings_during(f: impl FnOnce()) -> String {
        let buf = Buf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    fn workflow(name: &'static str, mcp: bool) -> WorkflowInfo {
        WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp,
            name,
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
            concurrency: None,
            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            sla: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }
    }

    fn update(name: &'static str, workflow: &'static str, mcp: bool) -> UpdateHandlerInfo {
        UpdateHandlerInfo {
            name,
            workflow,
            module: "tests",
            input_type_hint: "In",
            output_type_hint: "Out",
            has_validator: false,
            handler: |_ctx, _args| Box::pin(async move { Ok(serde_json::Value::Null) }),
            validator: None,
            mcp,
            description: None,
            arg_schema: None,
            response_schema: None,
        }
    }

    /// An `mcp` workflow with no mounted route logs one warning that names it.
    #[test]
    fn an_mcp_workflow_with_no_route_warns_once_and_names_it() {
        let flows = [workflow("review", true), workflow("audit", true)];
        let logged = warnings_during(|| warn_if_mcp_unserved(&flows, &[], false));
        assert_eq!(logged.matches("WARN").count(), 1, "{logged}");
        assert!(
            logged.contains("review") && logged.contains("audit"),
            "{logged}"
        );
        assert!(
            logged.contains("mcp_tools") && logged.contains("mcp_tasks"),
            "{logged}"
        );
        assert!(logged.contains("`mcp` feature"), "{logged}");
    }

    /// A mounted route needs no warning.
    #[test]
    fn a_mounted_route_does_not_warn() {
        let flows = [workflow("review", true)];
        let updates = [update("approve", "review", true)];
        let logged = warnings_during(|| warn_if_mcp_unserved(&flows, &updates, true));
        assert!(logged.is_empty(), "{logged}");
    }

    /// A workflow without the flag needs no warning, mounted or not.
    #[test]
    fn a_workflow_without_mcp_does_not_warn() {
        let flows = [workflow("plain", false)];
        let updates = [update("poke", "plain", false)];
        let logged = warnings_during(|| warn_if_mcp_unserved(&flows, &updates, false));
        assert!(logged.is_empty(), "{logged}");
    }

    /// An `mcp` update alone also warns, and the message names it.
    #[test]
    fn an_mcp_update_with_no_route_warns() {
        let flows = [workflow("plain", false)];
        let updates = [update("approve", "plain", true)];
        let logged = warnings_during(|| warn_if_mcp_unserved(&flows, &updates, false));
        assert_eq!(logged.matches("WARN").count(), 1, "{logged}");
        assert!(logged.contains("plain::approve"), "{logged}");
    }
}
