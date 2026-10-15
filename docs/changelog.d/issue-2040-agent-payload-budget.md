## Feature — Agent payload-budget follow-ups (issue #2040)

`autumn-harvest-agent` now keeps a run inside the engine payload caps in four
more cases. Items 4 and 6 of the issue shipped earlier, in the always-on PR.

**What shipped.**

- **Round budget (item 1).** A tool round measures the whole next request: the
  transcript, the assistant turn and every answer. It builds that request once,
  with a placeholder for each answer. A result replaces its placeholder, so the
  sum stays exact. When even minimal answers cannot fit, no call runs.
- **Model reply cap (item 2).** The model-turn activity fits its reply to the
  result cap. It cuts each decision reason to 256 bytes first. A reply that
  still does not fit becomes a turn with `oversized` set. The loop ends it
  under the new stop `reply_too_large`. The run no longer fails with a
  `PayloadTooLarge` that is not retryable, after the model call was paid for.
- **Zero cap (items 3 and 5).** A cap of zero is no cap, as in the engine.
  `exceeds_bytes`, `max_tool_result_bytes` and the error result follow the
  rule. `AgentTask::max_request_bytes(0)` no longer ends a run before its
  first model call.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route change.
`ModelTurn::oversized` is left out at its default, so a recorded turn encodes
as before and strict replay still matches. `AgentStop` is `#[non_exhaustive]`,
so the new stop is not a break. `ModelTurn` gains a field. A struct literal of
it needs the new field.

**Eval.** `eval::evaluate` follows a worker. A turn over the result cap is cut
or dropped, and the candidate run ends under `reply_too_large`. It no longer
fails.

**Tests.** Each case has a test that failed first: `bounds`, `harness` and
`types` unit tests, `engine_activities` for the activity, and `sqlite_loop`
for the loop. They cover a round that cannot fit, a round charged against a
large history, a reply over the cap, a huge policy reason, and a zero cap in
the request check, the round and the tool results.
