# DESIGN-2442: the answer check in the agent loop

Issue #2442. Parent epic #2438. It uses the `Scorer` of #2441 and the
`max_answer_retries` cap of #2439.

## 1. Problem

The agent loop accepts each final answer. An app that needs valid JSON or
a citation checks the answer after the run. On a bad answer it starts a
new run. That run pays for the whole transcript again, and it has no link
to the first run.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the check is set | a harness hook · **an optional `AgentTask::answer_check`** · the profile of a kind |
| How the check names its scorer | a scorer id · **a `ScorerRef`, so a judge names its model** |
| Where the cap comes from | the task · **the `SamplingPolicy` of a kind that the check names** · a harness constant |
| What the task asks for | nothing, the cap is the policy value · **`retries`, at most the policy cap** |
| When the policy is read | at each answer · **once, in the recorded `agent_sampling_policy` activity, before the first model call** |
| Where the scorer runs | in workflow code · **in the recorded `agent_score` activity** |
| What the scorer reads | the answer only · **a `ScoreInput`: the task input, the answer and the tool results** |
| How the model learns the reason | a system note · **a user message after the rejected answer** |
| How the run ends at the cap | `completed` with a flag · **a new stop, `answer_rejected`** |
| Where the reasons go | the error field · **`AgentReport::answer_rejections`** |
| Which answers are checked | the first only · **each answer that ends a segment as `completed`** |
| How the cap counts | per segment · **per run, follow-ups included** |

## 3. Reverse brainstorming

"How could the check cost too much, lie, or break replay?" Each answer
gives a rule.

| Failure | Rule |
|---|---|
| A task puts `max_answer_retries` at its top level. | The decode refuses each name in `SAMPLING_POLICY_FIELDS`, as before. The ask is `answer_check.retries`. |
| A task asks for 100 retries. | `sampling::answer_retry_cap` refuses an ask over the policy cap, or over `MAX_ANSWER_RETRIES`. The run fails with `too_many_answer_retries` before any model call. |
| A task names a judge with an expensive model. | `sampling::admit_answer_check` checks the scorer and the judge model against the policy, as `admit` does. |
| A task names a kind with a wide policy. | Accepted. A retry is one more model call in the same run. The run already sets its own `max_steps`. `max_total_tokens` and the LLM budget still bound it. |
| A crash between the answer and the verdict calls the model again. | The model turn and the verdict are recorded activities. Replay reads both. |
| A policy change during a run moves its cap. | The run reads the policy once. Replay reads the record. |
| Retries pass the token limit. | A retry goes back to the top of the loop. The token check and the LLM budget check run before each model call. |
| A judge call runs past a spent budget. | `agent_score` checks the budget. A refusal ends the run as `budget_exceeded`. |
| The score request is too large to record. | The loop drops the tool results first. A request that is still too large ends the run as `transcript_full`. |
| A reject reason holds an instruction from untrusted text. | The loop puts the reason in a fixed frame and calls it data. `Verdict::reject` cuts it to `MAX_REJECT_REASON_BYTES`. |
| A delegated child checks its own answers. | A child task has no check. Only the root answer is checked. |
| A task recorded before the field fails to decode or replay. | The field is `#[serde(default)]` and is not written when `None`. With `None`, the loop records no new event. |
| An older worker drops the check from a new task. | `AnswerCheck` denies unknown keys, so a newer key fails closed. Upgrade every worker before you set the field. |

## 4. Six thinking hats

- **White (facts).** `agent_score` and `SamplingPolicy::max_answer_retries`
  exist. `AgentTask` refuses the top-level key `max_answer_retries`. A run
  with no kind has no policy. `AgentStop` is `non_exhaustive`.
- **Red (feelings).** Apps want "valid JSON or try again" with no second
  run. Operators fear a loop that never ends.
- **Black (risks).** The cap must hold on every path. A retry must not skip
  a bound. A reason sent back to the model is an injection path.
- **Yellow (benefits).** One run, one transcript and one cost ledger. The
  model sees its own answer and the reason, so a retry is cheap.
- **Green (ideas).** A durable agent profile can carry a check later, with
  its kind as the policy key.
- **Blue (process).** Red tests first, one per acceptance line. Then the
  kernel and its Verus proof, the loop, the docs and a review from several
  angles.

## 5. Decision

### The type

```rust
pub struct AnswerCheck {
    pub kind: String,       // the kind whose policy caps the check
    pub scorer: ScorerRef,  // the scorer, and the model of a judge
    pub retries: u32,       // 0 by default
}
```

`AgentTask::answer_check: Option<AnswerCheck>`. `None` by default, and not
written when `None`.

### The kernel

- `answer_retry_cap(policy_max, asked)` gives the cap, or `None` when
  `asked` is over `policy_max` or over `MAX_ANSWER_RETRIES`.
- `retry_after_reject(rejections, cap)` is `true` while `rejections` is at
  most `cap`.
- `admit_answer_check(policy, check)` applies `answer_retry_cap`, then the
  scorer check of `admit`.

`formal/verus/src/agent_sampling.rs` proves the two integer kernels. With
cap `c`, the loop asks the model for at most `c + 1` answers.

### The loop

1. Before the first model call, a task with a check reads the policy of
   its kind in `agent_sampling_policy`. A refusal fails the run. With
   `report_failure`, the run ends as `failed`.
2. A turn with no tool calls that ends as `completed` is scored in
   `agent_score`.
3. A score ends the segment as `completed`.
4. A reject adds its reason to `AgentReport::answer_rejections`. While
   `retry_after_reject` holds, the loop adds a user message with the
   reason and asks the model again.
5. Past the cap, the run ends as `answer_rejected`.

## 6. Acceptance criteria

The tests are in `autumn-harvest-agent/tests/answer_check.rs` unless named
otherwise.

1. A valid first answer: one model call and one scorer call.
2. An invalid answer, then a valid one: two model calls. The second request
   holds the reason.
3. A cap of 2 and three invalid answers: `answer_rejected` after three
   model calls.
4. A crash mid-retry replays from history. No model call and no scorer
   call runs again.
5. Retries count against the token limit, so the run ends as
   `tokens_exhausted`.
6. A task recorded before the change replays with no error.
7. A task that asks for more retries than the policy allows fails before
   any model call.
