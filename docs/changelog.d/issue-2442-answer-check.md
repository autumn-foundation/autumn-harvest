## Answer check in the agent loop (issue #2442)

`AgentTask::answer_check` scores each final answer in the same run. On a
reject, the loop sends the reason to the model. The model then answers
again, within a cap.

- `AnswerCheck` names an agent kind, a `ScorerRef` and `retries` (0 by
  default). It denies unknown keys.
- The recorded activity `agent_answer_check` admits the check before the
  first model call. The kind needs a profile of the tenant of the run.
  The `SamplingPolicy` of the kind caps `retries` with
  `max_answer_retries`, and it must allow the scorer and the judge model.
  The harness must have the scorer. A refused check fails the run with a
  named `sampling::Refusal`, and no model call runs. The new codes are
  `too_many_answer_retries`, `unknown_scorer` and `other_tenant`.
- Each answer that completes is scored in `agent_score`. A reject adds a
  user message with the reason. After `retries + 1` rejects, the run ends
  under the new stop `AgentStop::AnswerRejected` (`answer_rejected`).
  `AgentReport::answer_rejections` holds each reason. The delivery sends
  only the notice of that stop.
- `ScoreRequest::judge` holds a `JudgeLimit`. `ScoreContext::chat` refuses
  a judge model outside it, and the score fails as `JudgeNotAllowed`.
- A retry counts against `max_total_tokens` and the LLM budgets. A judge
  call that the budget refuses ends the run as `budget_exceeded`.
- `sampling::answer_retry_cap` and `sampling::retry_after_reject` hold the
  integer part. `formal/verus/src/agent_sampling.rs` proves them.

**Compatibility.** The field is `#[serde(default)]`. The encoder skips it
when it is `None`. A task from before the change decodes with no check and
records no new event. This release adds no `WorkflowEvent` variant and no
migration. A worker from before this release ignores the field, so upgrade
every worker first. A decoder from before this release cannot read
`answer_rejected`. `AgentReport` gains a field, so a struct literal of it
needs `answer_rejections`.

Tests: `autumn-harvest-agent/tests/answer_check.rs`, one test per
acceptance line, and tests for each review finding. The design is in
`docs/design/DESIGN-2442.md`.
