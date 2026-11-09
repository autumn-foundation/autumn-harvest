## One Anthropic `AgentModel` for every example (issue #2605, PR #2628)

The `anthropic` feature of `autumn-harvest-agent` adds the `anthropic`
module. It is off by default, so the crate without it gains no dependency.
The design is in `docs/design/DESIGN-2605.md`. The guide is
`docs/agent-adapter.md`, section "The Claude model".

- `anthropic::wire` holds the API version `2026-10-21`, the default model
  `claude-opus-5-5`, the settings of each model, the error table, the usage
  counters and the prices. It does no I/O.
- `anthropic::Client` sends one request body. It reads the body under a cap,
  with one byte past the cap, and refuses bytes that are not UTF-8. A
  failure after a `2xx` reports `accepted()`, so a caller does not pay for a
  turn twice.
- `anthropic::ClaudeModel` implements `AgentModel`, with `chat_stream` over
  Server-Sent Events. `model_id` passes the cost ledger check.
  `cost_usd_micros` prices the uncached input, the cache reads, the cache
  writes and the output of the Claude 5 models.
- The status table is the retry table of the Anthropic SDKs: `408`, `409`,
  `429` and `5xx` retry. `413` is `Budget`. `400` does not retry.

The examples move to it and delete their copies:

- `autoresearch` deletes `src/claude.rs`. Its raw turns stay in the same
  files under the same keys, through a `TurnDir` store.
- `agent-mesh` and `recall-eval` depend on the feature, not on
  `autoresearch`.
- `claude-agent-daemon` keeps its reply rules and its offline stub. It sends
  each turn through `anthropic::Client`. Each live turn now writes one cost
  ledger row. Its default model is now `claude-opus-5-5`, with effort
  `high`. A session recorded on `claude-opus-5` resumes with
  `--model claude-opus-5`. A `409` now retries.

Fixed drift: the autoresearch model id held `=`, so the ledger recorded it
as `unknown`. Neither copy priced a call. A key with an interior newline
failed every autoresearch request as a retryable error.

Tests: `cargo test -p autumn-harvest-agent --features anthropic`, which
runs the client against a local HTTP server. `cargo test -p autoresearch
-p claude-agent-daemon -p agent-mesh -p recall-eval`, including
`a_restart_resumes_the_session_without_repeating_model_calls` and
`a_live_turn_writes_one_priced_ledger_row`.
