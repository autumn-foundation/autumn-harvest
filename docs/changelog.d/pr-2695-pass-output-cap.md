## A consolidation pass sets no output cap by default (issue #2695)

A pass no longer caps the reply of the model at 2048 tokens. A full pass
could need more, so the reply was cut and the pass failed with
`InvalidReply`. The next pass read the same episodes and failed again.

- `EpisodeSettings::max_pass_output_tokens(n)` sets a cap. The default is
  `None`, so the engine adds none. 0 also sets none.
- `PASS_OUTPUT_TOKENS` is gone. `pass_request` takes the cap as an argument.
- `ConsolidateTurn::max_output_tokens` records the cap. A turn recorded
  before this change decodes with no cap. No `WorkflowEvent` variant, no
  replay impact.
- A provider that needs a cap on the wire, such as Anthropic, still sends
  its own adapter default.

**Tests.** Unit tests cover the default, the builder, the serde forms, the
request, the turn and the model call.
