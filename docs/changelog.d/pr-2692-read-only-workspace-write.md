## Phase 2692 — A read-only run refuses a workspace change (issue #2692)

A read-only run never moves the workspace head. Before this change, an
`Internal` tool that used the workspace could change it on such a run and
return success, and the harness dropped the change. Now `tool_call` checks
the workspace after the tool returns. If the run is read-only and the
workspace is dirty, the outcome is an error that the model reads:
"this run cannot change the workspace; the change was not saved". The
head and the event log do not change. A call that only reads the workspace
still succeeds. An attended run still saves its change.

Tests: `a_read_only_call_that_changes_the_workspace_is_an_error`,
`a_read_only_call_that_only_reads_the_workspace_succeeds` and
`an_attended_call_that_changes_the_workspace_still_saves` in
`autumn-harvest-agent/tests/agent_workspace.rs`.
