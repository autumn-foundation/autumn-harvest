## Vote strategy: self-consistency by answer key (issue #2444)

`agent_sample` runs a vote. `SampleStrategy::Vote { n, key }` keys each
final answer with an `AnswerKey` and selects the most common key. It
needs no scorer. The design is in `docs/design/DESIGN-2444.md`.

- `AnswerKey` is `trimmed`, `last_number`, `json_field` (a JSON pointer)
  or `custom`. A custom key is an `AnswerKeyFn` on the harness. The
  recorded activity `agent_vote_key` applies each key, so replay reads
  it.
- Only a draw that completed votes. A draw with no key casts no vote. A
  tie goes to the key whose first vote has the lowest draw id. With no
  vote, the outcome is `no_accepted_draw`.
- **Early stop.** After each wave, the run stops with `settled` when
  draws are left and `lead > second + left`.
  `formal/verus/src/agent_vote.rs` proves that the early-stop winner and
  its first draw are those of the full run. It also proves that the order
  in which draws end, and so the key numbering, does not change the
  winner.
- A failed key activity, or a worker with no function for an app key,
  stops the run with `score_failed`. A failed key wins over `settled`.
- `SampleReport::tally` and `SampleDraw::key` report the votes.
- New refusals: `no_scorer`, `scorer_not_used`, `invalid_answer_key` and
  `unknown_answer_key`. Each comes before any model call.
- **API change.** `SampleStrategy` is no longer `Copy`, and
  `policy_strategy` and `draws` take `&self`. `SampleInput::scorer` is an
  `Option`. A recorded input still decodes. Upgrade each worker before you
  start a vote: an older worker fails to decode it.
- No migration. No new `WorkflowEvent` variant.
- Tests: `tests/vote.rs`, `tests/vote_crash.rs`, the vote case of
  `tests/sample_postgres.rs`, and the proptests
  `an_early_stop_never_changes_the_winner` and
  `the_order_of_the_votes_does_not_change_the_tally`.
