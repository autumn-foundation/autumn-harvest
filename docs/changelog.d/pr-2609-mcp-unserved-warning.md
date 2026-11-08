## The plugin warns once when an `mcp` flag has no route (issue #2609)

`#[workflow(mcp)]` is inert unless the plugin has its `mcp` feature and the
app calls `.mcp_tools()` or `.mcp_tasks()`. The Postgres plugin was silent
in both cases.

What shipped:

- New `mcp_unserved::warn_if_mcp_unserved`. Both backends call it once at
  plugin build. It logs one `WARN` when a workflow or update sets `mcp` and
  no MCP route is mounted. The message names them and the fix.
- The SQLite driver no longer warns for each registration. It repeated for
  each tenant reopen. The build warning replaces it.
- The MCP tools guide and the SQLite plugin guide mention the warning.

No migration. No new query.

Tests: four unit tests in `autumn-harvest-plugin/src/mcp_unserved.rs`. They
cover a warning with no route, no warning with a route, no warning without
the flag, and an `mcp` update alone.
