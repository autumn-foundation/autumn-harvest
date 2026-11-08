# DESIGN-2603: a notice when a tool call parks on approval

Issue #2603. Related: #2262, #2263.

## 1. Problem

A gated tool call parks on the signal
`tool_approval:<round>:<position>:<call_id>` with a deadline. Nothing tells
the app that the call parked. `Delivery::deliver` gets only a segment
`Report`. The only sign of a park is the search attribute
`agent_status = waiting_approval`. A chat app must poll to show
"Approve this?". An always-on agent on Telegram or Slack must push the
prompt when the call parks.

## 2. Facts

- `gated_call` in `workflow.rs` builds the signal name, then waits. The
  durable `Agent` races the signal, the interrupt signal and a timer. An
  `agent_loop` run waits with `await_decision`.
- `agent_deliver` is a recorded activity. A failed send is recorded and
  never fails the run. The same pattern fits a notice.
- `WorkflowContext::now` returns the start time of the workflow. It is not
  the current time, so the workflow cannot compute the deadline itself.
- A run that parked before the upgrade has no notice in its history. Its
  replay must not schedule one, so the new activity needs a patch gate.
- The evaluation harness mocks each activity of the loop. An activity with
  no mock fails the candidate.
- A live probe waits one second on each approval. Nobody answers it.
- `sqlite::decide` and `postgres::decide` take the execution id and the
  signal name.

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where the app hooks in | a new trait and harness setter · **a default method `Delivery::notify_approval`** |
| What the default does | nothing · **log the names, never the arguments** |
| When the notice goes | in a race with the wait · **in a recorded activity before the wait** |
| Who computes the deadline | the workflow · **the activity, from its clock and the timeout** |
| What the notice names | run id and signal · **run id, execution id, agent id, session id, signal, tool, reason, arguments, timeout and deadline** |
| How big arguments get | as they are · **cut to the tool result cap, with a flag** |
| The dedupe key | the signal alone · `<run id>:<signal>` · **`<run id>:<execution id>:<signal>`** |
| A failed notice | fail the run · deny the call · **record the failure, then wait as before** |
| Old histories | always notify · **a patch gate, asked only on a gated call** |
| A probe | notify · **no notice, as it sends no report** |
| An evaluation | run the delivery · **a stub, so no notice leaves the harness** |

## 4. Reverse brainstorming

"How could the notice hurt a run or a person?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A replay after a crash sends the prompt again. | The activity result is recorded. A replay reads it and sends nothing. A test proves it. |
| A retry of the activity sends the prompt twice. | The notice has a key of run id, execution id and signal. A retry keeps the key. A test proves it. |
| A reset or a fork replays the run id, so an app drops the notice of the new execution as a repeat. | The key holds the execution id too. |
| A run that parked before the upgrade fails replay. | The patch gate makes its replay skip the notice. |
| A broken chat API fails the run. | A failed notice is recorded and ignored. A test proves it. |
| A failed notice lets the call run. | The wait runs after the notice in every case. Only a decision releases the call. A test proves it. |
| The log leaks the arguments. | The default logs only names and sizes. |
| Huge arguments or a huge reason break the activity input cap. | The arguments are cut to the tool result cap. The reason is cut to 256 bytes. |
| A worker cap below the task cap refuses the notice. A refused schedule records nothing, so the next replay diverges and wedges the run. | A recorded side effect holds the result of `WorkflowContext::activity_input_fits`. An oversized notice is skipped, and each replay takes the same branch. A Postgres test proves it. |
| The deadline in the notice is too late, so a person approves too late. | The activity reads its clock before the wait starts. The real deadline is at that time or later. |
| An evaluation pings a person. | The evaluation mocks the activity. A test proves it. |
| A probe pings a person. | A probe skips the notice. |
| A decision left over from an earlier wait hides the next notice. The patch gate reads a gate with only signals after it as an old position. | A signal probe of a name that no client sends runs first. It moves each waiting signal into the buffer, so the gate sees the real end of history. A test proves it. |
| An evaluation drops an approval that came during the notice, and reports a false divergence. | A scheduled notice opens its wait for the evaluation. A test proves it. |

## 5. Six thinking hats

- **White (facts).** Section 2.
- **Red (feelings).** An approval prompt that comes twice, or never, makes
  people distrust the agent. A prompt for a stale wait is worse.
- **Black (risks).** A slow notice delays the wait. The activity makes up
  to three attempts. The hook timeout bounds each one, so the delay is at
  most about 90 seconds with the defaults. The deadline comes from the
  worker clock, and the timer from the engine clock, so clock skew can move
  it.
- **Yellow (benefits).** A chat app pushes "Approve this?" at once. It
  needs no poll. The notice holds what `decide` needs.
- **Green (ideas).** A separate notifier trait. Not needed: the app that
  sends reports already has the channel. A default method changes no
  existing `Delivery`.
- **Blue (process).** Red tests on SQLite first, then the minimum code,
  the Postgres test, the docs and the fragment. No Verus kernel: the
  feature adds no integer kernel. The invariants are about effects and
  history, so DB tests and replay tests hold them.

## 6. Decision

- `delivery::ParkedCall` is the input of the activity
  `agent_approval_notice`. The workflow builds it before the wait.
- `delivery::ApprovalNotice` is what the app gets. It is the parked call
  and a deadline. `ApprovalNotice::key` is
  `<run id>:<execution id>:<signal>`.
- `Delivery::notify_approval` has a default that logs the run id, the
  agent id, the signal, the tool, the deadline and the size of the
  arguments.
- `AgentHarness::notify_approval` reads the clock, adds the timeout, and
  calls the delivery under the hook timeout.
- `gated_call` sends the notice before the wait, on both paths. It skips a
  probe. The patch `agent_approval_notice` gates it. A signal probe runs
  before the gate.
- A failed, timed-out or too-large notice is recorded. The wait goes on.
- The activity is in `workflow::activities()`, in the SQLite registration
  and in the evaluation mocks.

## 7. Engine fix: a race that a buffered signal settles

The notice opens a new window. An app can send the decision while the
notice activity still runs, so the signal lands before the approval race
opens. A durable agent waits with `ctx.race()`. That race then settles on
its first poll and arms no timer branch. History holds `race:<n>`, then
`race_winner:<n>`, with no `TimerStarted`. Replay found the winner marker
where the timer branch looked for its start event, and reported a
divergence. The next replay of the agent failed. The `agent_loop` wait
uses `receive_signal_timeout`, which was safe.

The fix is in `WorkflowContext::race`. A timer, activity or child branch
that meets the winner marker of its own race reads as `NoMatch`, as on the
first poll. Any other mismatch stays a divergence. The SQLite suite
`race_buffered_signal` holds three cases: a signal of the first branch, a
signal of a later branch, and an activity branch beside the signal. Each
failed before the fix. The Postgres test
`a_decision_sent_from_the_notice_releases_the_call_and_replays` sends the
decision from inside the notice.

## 8. Acceptance criteria

1. A gated call on a durable `Agent` sends exactly one notice. `decide`
   accepts its signal and its execution id.
2. A replay after a crash during the wait sends no second notice. A retried
   notice activity keeps the key of run id and signal. The key also holds
   the execution id, so a reset or a fork gets a new key.
3. A failed notice never fails the run and never releases the call.
4. It works on SQLite and Postgres. `docs/agent-adapter.md` sections 5 and
   15 describe it.
