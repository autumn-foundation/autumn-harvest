## Phase X.Y — Durable route: no final 500 on a chain that still moves (issue #2491)

The durable route walks a continue-as-new chain to its last run, in at most
`max_chain_steps` steps. At the cap, it answered a final
`500 result-unavailable` when each run that it read had ended. On a chain
that moved during the walk, that answer was wrong: a retry found the result.
CI saw this in `same_type_chain_takes_one_step` and
`rerun_of_a_chain_end_keeps_the_original_outcome`.

- At the cap, the route now reads the chain tail. A chain with a live run
  answers pending.
- A chain that has ended gets a second walk. That walk reads only data that
  no longer changes. It answers the result, or a final 500 when it caps at
  the same run as the first walk. Any other second walk answers pending.
- `Successor.live` is gone. The tail read replaces it.
- `RunReads::chain_live` and `live_chain_run_exists` are new. The query
  uses the `id` key and `idx_harvest_wfx_first_exec_id`. Its Ledger profile
  is in `docs/ledger/autumn-harvest-plugin/durable_route.toml`.

No new `WorkflowEvent` variant. No migration. The design record is
`docs/design/DESIGN-2491.md`. Six unit tests on the fake chain run the race
with no database. `chain_step_cap_answers_pending_while_the_chain_runs`
proves it on Postgres.
