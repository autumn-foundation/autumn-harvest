## Phase X.Y — A failed fan-out child reports its usage (issue #2328)

A failed `agent_loop` run returned only an error string. A parent lost the
usage and the flow labels of that run, and of its whole subtree.

- `AgentTask::report_failure` makes a failed run end with a report. The report
  has `AgentStop::Failed` and `AgentReport::error`. It keeps the usage, steps,
  tool calls, delegations and flow labels of the run.
- `fan_out` sets the flag on its children. It merges a failed report like any
  other, then fails the batch with the first error in input order. A nested
  failure keeps the usage of the whole subtree.
- Top-level `agent_loop` callers still get `Err`.
- Patch gate: `agent_child_failure_report`. The flag is part of the recorded
  child input, so a history from before the gate replays unchanged. The parent
  control flow does not change, so no further gate is needed.
- Tests: `a_failed_fan_out_slot_keeps_its_own_usage` and
  `a_nested_failure_keeps_the_usage_of_the_whole_subtree` in
  `autumn-harvest-agent/tests/fan_out.rs`. Both failed before the fix.
