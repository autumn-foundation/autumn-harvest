## The sweep workflow tool (issue #2716)

A running agent can now start an `agent_sweep` with the `sweep` workflow
tool. An operator turns it on with `AgentTask::sweep` or
`AgentProfile::sweep`, and gives a required token ceiling. The model gives
an instruction and a list of `{label, source}`. The loop runs one sweep
child with a single draw for each item, so the tool timeout does not apply
and a crash resumes the child. The result holds the joined answers, one
row per label (`ok`, `status`, `tokens`) and the spend. The parent counts
the spend of the sweep tree, and takes the flow labels of the setting. A
draw and a delegated child drop the setting, so a sweep never nests. A new
deterministic scorer, `Answered`, accepts each completed run with a
non-empty answer.

No new `WorkflowEvent` variant and no migration. Only a task with the
setting offers the tool, so an old history replays as before. Tests:
`autumn-harvest-agent/tests/sweep_tool.rs`. Design:
`docs/design/DESIGN-2716.md`.
