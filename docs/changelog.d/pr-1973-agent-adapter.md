## Feature — A durable agent loop: `autumn-harvest-agent` (issue #1973)

**What shipped.** A new optional crate, `autumn-harvest-agent`. The agent
loop is the workflow `agent_loop`. Each model call is the activity
`agent_model_turn`, and each tool call is the activity `agent_tool_call`. A
call that the tool policy gates waits on a durable signal with a deadline.
The crate owns its agent primitives (`AgentModel`, `Tool`, `ToolPolicy`,
`Approval`), modelled on `autumn-plugin-agent` with no dependency on it.
ADR 0006 records why. `docs/agent-adapter.md` shows the pattern end to end.

**Design.**

- The policy runs inside the model-turn activity. Its decision is recorded
  with the reply, so replay never asks the policy again.
- Each approval signal name holds the step, the position and the call id.
  A late decision cannot release a later call.
- `RateLimited`, `Transport` and `Unavailable` model failures retry, and
  so does a model call over its time budget. A tool call
  runs once. A tool error, or a tool over its time budget, is a result that
  the model reads.
- A payload that is not an `Approval` denies the call. It never fails the
  run. The daemon uses the same wait, `approval::await_decision`, and keeps
  its own rule: an unreadable decision fails its session.
- The caps are configurable: `AgentTask::max_request_bytes` and
  `AgentHarness::max_result_bytes`.
- Step, token, output-cap and transcript bounds end a run under a named
  `AgentStop`. A tool result, or a tool error, is cut to fit the result cap
  (2 MiB by default). A stalled tool policy denies the call after
  `policy_timeout`.
- The crate depends on the core engine only, with no default features.
  `scripts/check-agent-adapter-deps.sh` fails CI if it depends on any Autumn
  crate outside the engine.
- The `sqlite` feature adds `sqlite::register`, `start` and `decide`.
- `examples/claude-agent-daemon` now takes its approval signal names, its
  durable approval wait and its payload-cap checks from the adapter.
- The core crate adds `ActivityContext::new_test_with_state` under the
  `testing` feature.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route
change.

**Tests.** Two cross-process tests abort a child inside a tool call and
inside a model call. The parent resumes the run. No completed model call or
tool call runs again. Other tests cover approve, edit, reject
and timeout, a late decision, each bound, retry classification, replay of
recorded policy decisions, and the engine-path activity handlers.
