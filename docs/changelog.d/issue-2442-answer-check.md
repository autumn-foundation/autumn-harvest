## Answer check in the agent loop (issue #2442)

`AgentTask::answer_check` scores each final answer in the same run. A
reject sends the reason back to the model, which answers again within a
cap.

- `AnswerCheck` names an agent kind, a `ScorerRef` and `retries` (0 by
  default). The `SamplingPolicy` of the kind caps `retries` with
  `max_answer_retries`. It denies unknown keys.
- The run reads the policy once, in `agent_sampling_policy`, before the
  first model call. `sampling::admit_answer_check` refuses too many
  retries (`too_many_answer_retries`), a scorer or a judge model outside
  the policy. The run then fails before any model call.
- Each answer that completes is scored in `agent_score`. A reject adds a
  user message with the reason, and the model answers again. After
  `retries + 1` rejects, the run ends under the new stop
  `AgentStop::AnswerRejected` (`answer_rejected`).
  `AgentReport::answer_rejections` holds each reason.
- A retry counts against `max_total_tokens` and the LLM budgets. A judge
  call that the budget refuses ends the run as `budget_exceeded`.
- `sampling::answer_retry_cap` and `sampling::retry_after_reject` hold the
  integer part. `formal/verus/src/agent_sampling.rs` proves them.

**Compatibility.** The field is `#[serde(default)]` and is not written
when `None`. A task recorded before the change decodes with no check and
records no new event. No `WorkflowEvent` variant, no migration.
`AgentReport` gains a field, so a struct literal of it needs
`answer_rejections`.

Tests: `autumn-harvest-agent/tests/answer_check.rs`, one test per
acceptance line. The design is in `docs/design/DESIGN-2442.md`.
