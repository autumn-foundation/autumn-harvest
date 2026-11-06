## A bare SQLite runtime warns when a workflow sets `mcp` (issue #2488)

`#[workflow(mcp)]` has no effect on a bare `SqliteRuntime`. It has no HTTP
edge, so it exposes no MCP tools. The flag was silent there.

What shipped:

- `SqliteRuntime::register_workflow` logs a `WARN` for a workflow with
  `mcp = true`. The message names the workflow and gives the reason. The
  workflow still registers and runs.
- New `register_workflow_mcp_served` skips the warning. The SQLite plugin
  driver uses it when its `mcp` feature is on, because the plugin serves the
  tools. Without that feature, the driver still warns.
- The MCP tools guide now says that tool exposure needs the plugin.

No migration. No new query.

Tests: `an_mcp_workflow_warns_at_registration_and_still_runs`,
`an_mcp_workflow_does_not_warn_when_the_caller_serves_mcp` and
`a_workflow_without_mcp_does_not_warn` in `autumn-harvest-sqlite/src/runtime.rs`.
