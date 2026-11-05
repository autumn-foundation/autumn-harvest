## A tool round over the cap resets the context (issue #2461)

With `ContextReset` on, a tool round that passes the request cap no longer
ends the run while a reset is left. The design is in
`docs/design/DESIGN-2461.md`.

- The round drops the result that does not fit, as before. Then a reset is
  due, with a save turn first when one fits.
- The reset window keeps the round: the assistant turn and the answer of
  each call. A call that ran shows its result or `DROPPED_FULL`. A call that
  did not run shows `NOT_RUN_FULL`. So the model does not run a call with an
  effect again.
- The round resets only when its window, with the notes of that time,
  falls under the threshold. If not, or with no reset left, the run ends as
  `transcript_full`, with the same report as before.
- A full round that the loop guard or an interrupt also stopped never
  resets, so the stop holds. Such a save round now ends the run too.
- The patch marker `agent_reset_on_full_round` gates both rules. The loop
  asks it only where a rule changes the branch. A run recorded under the
  #2456 rules replays unchanged. The fixture `full_round_2456_history.json`
  proves it.
- The kernel `bounds::full_round_resets` has a Verus proof in
  `formal/verus/src/agent_reset.rs`. The window after the reset, with the
  notes of that time, fits the cap. No mix of resets passes `max_resets`.
- No new `WorkflowEvent` variant. No migration.
- Tests: sixteen in `autumn-harvest-agent/tests/context_reset.rs` and a
  crash child, two unit tests in `workflow.rs`, and a unit test and a
  property test in `bounds.rs`.
