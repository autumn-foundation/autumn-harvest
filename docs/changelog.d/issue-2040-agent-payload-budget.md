## Feature — Agent payload-budget follow-ups (issue #2040)

`autumn-harvest-agent` now keeps a run inside the engine payload caps in four
more cases. Items 4 and 6 of the issue shipped earlier, in the always-on PR.

**What shipped.**

- **Round budget (item 1).** A tool round measures the whole next request: the
  transcript, the assistant turn and an answer for each call. It builds that
  request once, with a placeholder for each answer. A result replaces its
  placeholder. The placeholder is the longest fixed answer, so any fixed answer
  fits in its place. The request also holds the labels and releases that this
  turn can add. When the placeholders exceed the cap, no call runs.
- **Model reply cap (item 2).** The model-turn activity fits its reply to the
  result cap. It cuts each decision reason to 256 bytes first. A reply that
  still exceeds the cap becomes a turn with `oversized` set. The loop ends the
  run under the new stop `reply_too_large`. The activity no longer fails with a
  non-retryable `PayloadTooLarge` after the paid call. It logs a warning with
  the sizes.
- **Zero cap (items 3 and 5).** A cap of zero is no cap, as in the engine.
  `exceeds_bytes`, `max_tool_result_bytes` and the error result follow the
  rule. `AgentTask::max_request_bytes(0)` no longer ends a run before its
  first model call.
- **Narration.** A round that ends the run as `transcript_full` no longer
  makes the model text the report answer. The text can claim work that did not
  run.

**Payload store.** A worker with a payload store offloads a value over the
store threshold, and the engine skips its cap for it. The adapter cannot see
the store. Set `AgentTask::max_request_bytes` and `AgentHarness::max_result_bytes`
to zero for such a worker. Before this change, a zero cap broke both limits.

**Limits.** The round budget is exact for the result text and for the flow
state that the turn records. A delegation adds the labels and usage of its
child, which the round cannot know in advance. The loop measures the next
request again before it sends it, and ends the run as `transcript_full` if it
exceeds the cap. A result that does not fit is still dropped after its call
ran.

**Replay.** The round budget changes which tool calls a round schedules near
the cap. The zero-cap rule changes whether a run sends its first request.
`AgentTask::whole_request_budget` gates both. `AgentTask::new` turns it on. A
task recorded before this change has no field. It keeps the results-only
measure, reads a zero cap as a limit of zero bytes, and replays as it ran. A
heartbeat tick builds its task at run time, so `HeartbeatTask` records the
same flag, and `HeartbeatTask::new` turns it on.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route change.
`ModelTurn::oversized` and `AgentTask::whole_request_budget` are left out at
their defaults, so a recorded value encodes as before. The text of the
fixed "did not run" and "dropped" answers changed. They reach only the report
of a run that has ended.

**Breaking changes.** `ModelTurn`, `AgentTask`, `HeartbeatTask` and
`eval::TurnDecision` gain a public field. A struct literal of one of them
needs the new field. `AgentStop` is `#[non_exhaustive]`, so
the new stop is not a break. A reader built before this change cannot decode
`reply_too_large` in a recorded report. `exceeds_bytes(_, 0)` now returns
`false`, and `max_tool_result_bytes(0)` returns `usize::MAX`.

**Eval.** `eval::evaluate` follows a worker. A turn that exceeds the result cap
is cut or dropped. A dropped reply ends the candidate run under
`reply_too_large`. The run no longer fails. `TurnDecision::dropped` marks the
turn, and a dropped reply reads as a `stop` divergence.

**Tests.** Each behaviour change has a test that failed first. The `types`
guards and one zero-cap budget test passed at red, because the stubs held the
shape. Later tests pin behaviour that already held, and a mutation run showed
that they fail when the code is wrong. The tests cover:

- a round that cannot fit, and a round charged against a large history;
- the round budget to the byte, with a follow-up tool in the request;
- a label that a call adds to the next request;
- a reply over the cap, a huge policy reason, and a child that drops its reply;
- a zero cap in the request check, the round and the tool results;
- a task and a heartbeat tick recorded before the change, with a zero cap;
- the eval cap, which measures the ids that the provider gave.
