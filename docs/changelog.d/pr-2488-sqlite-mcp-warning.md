## SQLite warns when a workflow sets `mcp` (issue #2488)

`#[workflow(mcp)]` has no effect on the SQLite runtime. That runtime has no
HTTP edge, so it exposes no MCP tools. The flag was silent there.

What shipped:

- `SqliteRuntime::register_workflow` logs one `WARN` for a workflow with
  `mcp = true`. The message names the workflow and gives the reason. The
  workflow still registers and runs.
- The MCP tools guide now says that tool exposure needs the Postgres plugin
  runtime.

No migration. No new query.

Tests: `an_mcp_workflow_warns_at_registration_and_still_runs` and
`a_workflow_without_mcp_does_not_warn` in `autumn-harvest-sqlite/src/runtime.rs`.
