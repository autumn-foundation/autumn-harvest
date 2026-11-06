## Dataset sweep: one strategy over a fixed input set (issue #2448)

`agent_sweep` runs one strategy over a fixed set of inputs, durably, and
returns one row for each item. The design is in
`docs/design/DESIGN-2448.md`. The guide is section 25 of
`docs/agent-adapter.md`.

- Each item runs as a child `agent_sample` run. History records the end of
  each child, so a crash runs only the items in flight again. A completed
  item never runs again.
- `SweepReport` holds one `SweepRow` for each item: the status, the
  verdict, the accepted flag, the selected answer, the draws, the tokens
  and the cost. `SweepTotals` holds the mean score, the accept
  rate, the total cost and the cost per accepted answer.
- `sweep_plan` splits the `max_concurrent_draws` cap of the policy between
  the items and their draws, so `items × draws` never passes the cap.
  Verus proves it (`formal/verus/src/agent_sweep.rs`).
- The items share the token budget `max_total_tokens`. Each wave splits
  the rest with `sibling_cap`, and a failed child counts its whole share.
  An LLM budget refusal in a child stops the sweep, and each item that did
  not start is `not_run`. A spent share or profile cap stops its item
  only.
- Each child reads the profile and the policy again. A child that reads
  another policy revision or overlay version stops the sweep with
  `config_changed`. Each row holds the `revision` and `overlay_version`
  of its child.
- `SweepItemStore` keeps a list over the activity cap in a payload store.
  The input then holds a `SweepItemsRef` with a SHA-256 digest. A list
  holds at most 16 MiB. The `agent_sweep_items` activity reads the list in
  pages of at most 1 MiB, and checks the digest, the size and the count on
  each read.
- `SampleStrategy::Single` runs one draw. The policy needs no strategy for
  it. `sqlite::start_sweep` starts a sweep. `workflows()` and
  `activities()` hold `agent_sweep` and `agent_sweep_items`.
- `SampleInput::expected` and `ScoreInput::expected` carry the expected
  answer of an item. Only `ExactMatch::per_item()` reads it.
  `ExactMatch::new(text)` keeps its own text.
- `SampleInput::brief` drops the transcript of the selected draw. Each
  sweep child sets it. `SampleReport` adds `refused` and
  `overlay_version`.
- `examples/sweep_compare.rs` compares best-of-3 with a cheap model
  against one draw with a strong model, and prints both tables.
- **API change.** `SampleStrategy::policy_strategy` returns
  `Option<Strategy>`, and `None` for `single`. `Refusal` gains
  `no_items`, `too_many_items` and `item_too_large`. A `SampleInput`
  struct literal needs `expected` and `brief`.
- **Upgrade.** Upgrade each worker first. An older worker does not
  register `agent_sweep`, cannot decode `single`, and refuses a
  `SampleInput` with `expected`.
- No new `WorkflowEvent` variant and no migration.
- Tests: `tests/sweep.rs`, `tests/sweep_crash.rs`, the unit tests of
  `sampling/sweep.rs` and `sampling/admit.rs`, and the proptest
  `a_sweep_plan_never_passes_the_cap`.
