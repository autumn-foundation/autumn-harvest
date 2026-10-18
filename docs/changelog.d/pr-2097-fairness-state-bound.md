## Phase 8.x — Fairness state bound, preview locks and key spelling (issue #2097)

Four follow-ups to the fairness keys of issue #1976.

- **Bounded state.** A queue that never drains kept one state row per
  one-shot key. Prune now deletes the old row of every key with no pending
  task, except the clock row. The clock row has the greatest
  `(last_start, key)` of its queue, so `V` never falls and concurrent pruners
  keep the same row. A pruned key loses at most `1/w` of debt, one claim at
  its weight, because `pass - V <= 1/w` after a charge. The idle-queue reset
  now deletes only the last row. `FairClock::prune` models the rule.
- **Preview without locks.** A preview prunes a temp copy of the state rows,
  so a fair claim never waits for it. The count is the same as a live call,
  because both run one SQL text.
- **Queue name check.** `queue_fairness::validate_fairness_queue_name` is the
  one check. `WorkerRuntimeConfig::validate` and both public fair claims use
  it. A fair claim on `" q "` or `".."` returns `HarvestError::Config` before
  any statement.
- **CLI keys.** `harvest queue fairness set|clear` rejects only the literal
  `.` and `..`. Keys such as `%2e` reach the route as `%252e`.

Tests: `prune_forgives_at_most_one_claim_of_debt`,
`one_shot_keys_leave_one_row_on_a_queue_that_never_drains`,
`prune_bounds_one_shot_keys_on_a_queue_that_never_drains`,
`a_preview_takes_no_lock_on_the_state_rows`,
`a_fair_claim_refuses_an_unaddressable_queue_name` and
`queue_fairness_accepts_percent_spelled_keys`.
