## A consolidation pass sets no output cap by default (issue #2695)

A pass no longer caps the reply of the model at 2048 tokens. A full pass
could need more, so the reply was cut and the pass failed with
`InvalidReply`. The next pass read the same episodes and failed again.

- `EpisodeSettings::max_pass_output_tokens(n)` sets a cap. The default is
  `None`, so the engine adds none. 0 also sets none.
- `PASS_OUTPUT_TOKENS` is gone. `pass_request` takes the cap as an argument.
  `ConsolidateTurn` gains a field and is now `#[non_exhaustive]`.
- `ConsolidateTurn::max_output_tokens` records the cap. A turn recorded
  before this change decodes with no cap. It adds no `WorkflowEvent` variant.
  Replay does not change.
- No cap means the provider default applies. Anthropic needs a wire value
  and gets `DEFAULT_MAX_TOKENS` (16000). An Anthropic app sets a cap to
  raise it. A cut-off reply that reads as
  `InvalidReply` is issue #2696.

**Tests.** Unit tests cover the default, the builder, the serde forms, the
request, the turn and the model call. A SQLite test drives real passes.
