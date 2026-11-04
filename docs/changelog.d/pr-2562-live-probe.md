## Agent: a live probe on a fork (issue #2378)

`eval::live_probe(host, harness, source, overlay, allowed)` forks a recorded
`agent_loop` run at event 0 with `ForkEffects::Live`. The fork input is the
source task under the overlay, with the source ceilings, and a new
`AgentTask::probe` mark: the allowed `ToolEffect`s, as a one-byte mask, so
`AgentTask` does not grow. Pass
`RefinePolicy::probe_effects` as `allowed`. Its default is `[read_only]`.

- `agent_tool_call` reads the new `ToolCallRequest::probe_effects`. A tool
  with another effect gets `{"error": "probe_effect_refused", "effect": ...}`
  (`PROBE_EFFECT_REFUSED`), and `Tool::execute` does not run. The check is
  in the activity body, so a permissive policy cannot bypass it.
- A probe run sends no report. Each delegated child, inline or fan-out, is a
  probe with the same set. A delegation to a durable peer is refused. A
  probe never moves the workspace head.
- The probe input has no follow-ups and no draw, so it ends, and its ledger
  rows carry its own run id.
- `LiveProbeHost` is the backend. `PostgresAgents` forks. `SqliteRuntime`
  returns the new `EvalError::Unsupported`. A failed read or fork is the new
  `EvalError::Fork`.
- Core gains `fork::fork_workflow_execution_with_codecs`, so a caller with
  no handler registry forks with the codecs of its workers. It sends no new
  query.

No migration, no new `WorkflowEvent` variant, and no write to
`harvest_events`. The new fields are optional, so a recorded task or
request without them decodes as before. Roll out the workers before the
first probe. An older worker drops the mark and runs each tool.

Test evidence: `autumn-harvest-agent/tests/live_probe_postgres.rs` runs five
tests on a Postgres worker. `autumn-harvest-agent/tests/live_probe.rs` runs
four on SQLite. `eval::probe` has six unit tests. The design is in
`docs/design/DESIGN-2378.md`.
