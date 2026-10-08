## Feature — A durable agent loop: `autumn-harvest-agent` (issue #1973)

**What shipped.** A new optional crate, `autumn-harvest-agent`. It adapts
`autumn-plugin-agent` 0.3 to the engine. The agent loop is the workflow
`agent_loop`. Each model call is the activity `agent_model_turn`, and each
tool call is the activity `agent_tool_call`. A call that the tool policy
gates waits on a durable signal with a deadline. ADR 0006 names the target
framework and why. `docs/agent-adapter.md` shows the pattern end to end.

**Design.**

- The policy runs inside the model-turn activity. Its decision is recorded
  with the reply, so replay never asks the policy again.
- Each approval signal name holds the step, the position and the call id.
  A late decision cannot release a later call.
- Only `RateLimited` and `Transport` provider failures retry. A tool call
  runs once. A tool error is a result that the model reads.
- Step, token, output-cap and transcript bounds end a run under a named
  `AgentStop`. A tool result is cut to fit the 2 MiB result cap.
- The crate takes the core engine and plugin-agent with no default
  features. `scripts/check-agent-adapter-no-autumn-web.sh` fails CI if
  `autumn-web` reaches its graph.
- The `sqlite` feature adds `sqlite::register`, `start` and `decide`.
- `examples/claude-agent-daemon` now takes its approval signal names and its
  payload-cap checks from the adapter.
- The core crate adds `ActivityContext::new_test_with_state` under the
  `testing` feature.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route
change.

**Tests.** `crash_mid_loop_resumes_without_rerunning_completed_calls` aborts
a child process inside a tool call. The parent resumes the run. No completed
model call or tool call runs again. Other tests cover approve, edit, reject
and timeout, a late decision, each bound, retry classification, replay of
recorded policy decisions, and the engine-path activity handlers.

**Release order.** Publish `autumn-plugin-agent` 0.3.0 before an engine
release that includes this crate.
