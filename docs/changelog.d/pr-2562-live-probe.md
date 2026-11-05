## Agent: a live probe on a fork (issue #2378)

`eval::live_probe(host, harness, source, overlay, allowed)` forks a recorded
`agent_loop` run at event 0. The fork uses the new
`ForkEffects::LiveQuiet`. Its input is the source task under the overlay,
with the source ceilings. A new `AgentTask::probe` mark holds the allowed
`ToolEffect`s as a one-byte mask, so `AgentTask` does not grow. Pass
`RefinePolicy::probe_effects` as `allowed`. Its default is `[read_only]`.

- `agent_tool_call` reads the new `ToolCallRequest::probe_effects`. A tool
  with another effect gets `{"error": "probe_effect_refused", "effect": ...}`
  (`PROBE_EFFECT_REFUSED`), and `Tool::execute` does not run. The check is
  in the activity body, so a permissive policy cannot bypass it. A probe
  never calls the `memory` tool and never moves the workspace head.
- A probe run sends no report. Each delegation runs inline, as a probe
  with the same set. A delegation to a durable peer is refused.
- The probe input has no follow-ups and no draw, `deliver` is off, and the
  approval wait is one second.
- `ForkEffects::LiveQuiet` runs each activity for real. Its end sends no
  completion callback and fires no completion trigger.
  `fork::skips_completion_effects` replaces `is_recorded_fork` in the
  terminal path.
- `LiveProbeHost` is the backend. `PostgresAgents` forks, with the new
  `with_payload_offloader`. `SqliteRuntime` returns the new
  `EvalError::Unsupported`. A failed read or fork is the new
  `EvalError::Fork`.
- Core gains `fork::fork_workflow_execution_with_codecs`. A caller with no
  handler registry forks with the codecs and the offloader of its workers.

No migration, and no write to `harvest_events`. `ForkEffects` has a new
value, `live_quiet`, in the `WorkflowForked` event. An older worker cannot
decode it, so it cannot run a probe. The new task fields are skipped when
empty, so a recorded task or request keeps its bytes. Start a probe only
after every worker runs this release.

Test evidence: `autumn-harvest-agent/tests/live_probe_postgres.rs` runs six
tests on a Postgres worker. `autumn-harvest-agent/tests/live_probe.rs` runs
five on SQLite. `eval::probe` has seven unit tests. `fork::tests` and
`fork_tests.rs` cover `LiveQuiet`. The design is in
`docs/design/DESIGN-2378.md`.
