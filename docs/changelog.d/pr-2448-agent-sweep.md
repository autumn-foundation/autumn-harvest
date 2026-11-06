## Dataset sweep: one strategy over a fixed input set (issue #2448)

`agent_sweep` runs one strategy over a fixed set of inputs, durably, and
returns one row for each item. The design is in
`docs/design/DESIGN-2448.md`. The guide is section 25 of
`docs/agent-adapter.md`.

- Each item runs as a child `agent_sample` run. History records the end of
  each child, so a crash runs only the items in flight again. A completed
  item never runs again.
- `SweepReport` holds one `SweepRow` for each item: the status, the
  verdict, whether it was accepted, the selected answer, the draws, the
  tokens and the cost. `SweepTotals` holds the mean score, the accept
  rate, the total cost and the cost per accepted answer.
- `sweep_plan` splits the `max_concurrent_draws` cap of the policy between
  the items and their draws, so `items × draws` never passes the cap.
  Verus proves it (`formal/verus/src/agent_sweep.rs`).
- The items share the token budget `max_total_tokens`. Each wave splits
  the rest with `sibling_cap`. A child that stops `budget_exceeded` stops
  the sweep, and each item that did not start is `not_run`. A child that a
  budget refused before its first model call is `not_run` too.
- `SweepItemStore` keeps a list over the activity cap in a payload store.
  The input then holds a `SweepItemsRef` with a SHA-256 digest. The
  `agent_sweep_items` activity reads the list in pages of at most 1 MiB,
  and checks the digest and the count on each read.
- `SampleStrategy::Single` runs one draw. The policy needs no strategy for
  it. `sqlite::start_sweep` starts a sweep. `workflows()` and
  `activities()` hold `agent_sweep` and `agent_sweep_items`.
- `SampleInput::expected` and `ScoreInput::expected` carry the expected
  answer of an item. `ExactMatch` compares with it when it is set.
  `ExactMatch::per_item()` has no text of its own.
- `examples/sweep_compare.rs` compares best-of-3 with a cheap model
  against one draw with a strong model, and prints both tables.
- **API change.** `SampleStrategy::policy_strategy` returns
  `Option<Strategy>`, and `None` for `single`. `Refusal` gains
  `no_items`, `too_many_items` and `item_too_large`.
- **Upgrade.** Upgrade each worker first. An older worker does not
  register `agent_sweep`, cannot decode `single`, and refuses a
  `SampleInput` with `expected`.
- No new `WorkflowEvent` variant and no migration.
- Tests: `tests/sweep.rs`, `tests/sweep_crash.rs`, the unit tests of
  `sampling/sweep.rs` and `sampling/admit.rs`, and the proptest
  `a_sweep_plan_never_passes_the_cap`.
