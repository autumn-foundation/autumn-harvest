## Consolidation output cap stop (issue #2696)

A consolidation pass whose reply hits the output cap and does not parse now
ends as `ConsolidationStop::OutputCapped`. Before, it ended as
`InvalidReply`, the same as a malformed reply. A retry of a malformed reply
may pass. A retry of a capped reply fails the same way.

`ConsolidateFacts` has a new `capped` flag. It has `serde(default)` and is
skipped when false, so recorded turns replay unchanged. A capped reply that
still parses ends as `Completed`. A capped pass writes no fact and marks no
episode.

Tests: `run_turn` unit tests for the reply cases and the serde forms. Three
integration tests cover the report, the agent op and a capped reply that
parses.
