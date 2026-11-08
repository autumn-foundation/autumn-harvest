//! The build-time warning for an `mcp` flag that no route serves (issue #2609).
//!
//! `#[workflow(mcp)]` marks a workflow. Only a mounted MCP route exposes it.
//! Without `mcp_tools()` or `mcp_tasks()`, or without the `mcp` feature, the
//! workflow runs and no tool appears. Both backends call this check once at
//! build, so one app logs one message.

use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::{UpdateHandlerInfo, WorkflowInfo};

/// Run [`warn_if_mcp_unserved`] over the definitions of `builder`.
pub fn warn_for_builder(backend: &str, builder: &HarvestBuilder, mounted: bool) {
    warn_if_mcp_unserved(
        backend,
        builder.workflow_infos(),
        builder.update_handlers(),
        mounted,
    );
}

/// The most names the warning lists. A longer list shows a count only.
const MAX_NAMES: usize = 10;

/// Log one `WARN` when a registered `mcp` flag has no mounted MCP route.
///
/// `mounted` is true when the plugin mounts MCP tool or task routes.
/// An update counts only when its workflow sets `mcp`, as the tool
/// generator requires. `backend` names the store in the log line.
pub fn warn_if_mcp_unserved(
    backend: &str,
    workflows: &[WorkflowInfo],
    updates: &[UpdateHandlerInfo],
    mounted: bool,
) {
    if mounted {
        return;
    }
    let mut names: Vec<String> = workflows
        .iter()
        .filter(|info| info.mcp)
        .map(|info| info.name.to_owned())
        .collect();
    names.extend(
        updates
            .iter()
            .filter(|info| info.mcp && workflows.iter().any(|w| w.mcp && w.name == info.workflow))
            .map(|info| format!("{}::{}", info.workflow, info.name)),
    );
    if names.is_empty() {
        return;
    }
    let total = names.len();
    names.truncate(MAX_NAMES);
    tracing::warn!(
        backend,
        total,
        names = %names.join(","),
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
        let logged = warnings_during(|| warn_if_mcp_unserved("test", &flows, &[], false));
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
        let logged = warnings_during(|| warn_if_mcp_unserved("test", &flows, &updates, true));
        assert!(logged.is_empty(), "{logged}");
    }

    /// A workflow without the flag needs no warning, mounted or not.
    #[test]
    fn a_workflow_without_mcp_does_not_warn() {
        let flows = [workflow("plain", false)];
        let updates = [update("poke", "plain", false)];
        let logged = warnings_during(|| warn_if_mcp_unserved("test", &flows, &updates, false));
        assert!(logged.is_empty(), "{logged}");
    }

    /// An `mcp` update of an `mcp` workflow is named as `workflow::update`.
    #[test]
    fn an_mcp_update_of_an_mcp_workflow_is_named() {
        let flows = [workflow("review", true)];
        let updates = [update("approve", "review", true)];
        let logged = warnings_during(|| warn_if_mcp_unserved("test", &flows, &updates, false));
        assert_eq!(logged.matches("WARN").count(), 1, "{logged}");
        assert!(logged.contains("names=review,review::approve"), "{logged}");
        assert!(logged.contains("total=2"), "{logged}");
    }

    /// An `mcp` update of a plain workflow exposes nothing, so it is silent.
    #[test]
    fn an_mcp_update_of_a_plain_workflow_does_not_warn() {
        let flows = [workflow("plain", false)];
        let updates = [update("approve", "plain", true)];
        let logged = warnings_during(|| warn_if_mcp_unserved("test", &flows, &updates, false));
        assert!(logged.is_empty(), "{logged}");
    }

    /// A long list shows the first names and the full count.
    #[test]
    fn a_long_list_is_capped_with_the_full_count() {
        const NAMES: [&str; 12] = [
            "w00", "w01", "w02", "w03", "w04", "w05", "w06", "w07", "w08", "w09", "w10", "w11",
        ];
        let flows: Vec<_> = NAMES.iter().map(|name| workflow(name, true)).collect();
        let logged = warnings_during(|| warn_if_mcp_unserved("test", &flows, &[], false));
        assert!(logged.contains("total=12"), "{logged}");
        assert!(
            logged.contains("w09") && !logged.contains("w10"),
            "{logged}"
        );
    }
}
