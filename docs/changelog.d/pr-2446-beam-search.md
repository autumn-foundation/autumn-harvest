## Agents — step-level beam search (issue #2446)

`agent_sample` runs step-level beam search with
`SampleStrategy::Beam { width, expansion, max_rounds }`. The policy
strategy is `beam`.

- Each round expands each live beam `expansion` times. Each expansion is
  a child `agent_loop` run with the new `AgentTask::beam_step` flag. A
  step sends its beam as `history`, adds no user message, and ends after
  one model turn and its tool round, with the new stop
  `AgentStop::StepEnded`. A kept beam pays for its new turn only.
- `agent_sample_score` scores each step with the step scorer of the
  input. `beam_rank` keeps the best `width` less the complete beams. It
  calls `select` once per kept step, so a tie goes to the lowest draw id.
- The run stops when `width` beams are complete, when no beam is live, or
  after `max_rounds` with the new stop `SampleStop::RoundsExhausted`. The
  outcome is the best complete beam, or `no_accepted_draw`.
- `SampleDraw::beam` records the round, the parent and the fate of each
  step.
- The policy admits `beam_draws(width, expansion, max_rounds)`, the
  saturated product, before the first model call. Verus proves
  `beam_draws`, `beam_slots` and the bound of each schedule in
  `formal/verus/src/agent_sampling.rs`.

No new `WorkflowEvent` variant and no migration. `beam_step` is skipped
when false, so a recorded task decodes as before.

Tests: `tests/sample_beam.rs`, `tests/sample_beam_crash.rs`, and the unit
tests and proptests of `sampling::admit` and `sampling::sample`. The
design is in `docs/design/DESIGN-2446.md`.
