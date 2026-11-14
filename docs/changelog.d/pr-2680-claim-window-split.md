## Claim window split (issue #2680)

A hot path audit with a system-wide CPU profile found a cost that the
`pg_stat_statements` census cannot see. The census times `ExecutorRun`
only. `standard_ExecutorStart` was 14.9% of all CPU samples on the e2e
bench. Postgres initializes every CTE subplan at startup, also one that
never runs. The default claim held four candidate scans. Sampled plans gave
204 plan nodes per claim, 110 of them never run, and 79.8% of all plan nodes
that the hot path initializes.

A default claim now sends a window form first. It keeps the window scan,
`seek_held` and `seek_contended`, and returns the claimed task or a
`claim_fallback` flag. The full statement, unchanged, runs only when the
flag is set: 8 of 10,128 claims on the bench. Ageing and fair claims send
the full statement alone, as before. `queue::claim_window::window_claim_sql`
derives each window text from the final full text, so the DR fence, the
kind predicate and the owned gate carry over. No migration, no new event,
no change to which row a claim takes.

Evidence: the `throughput` cell moves from 92.2 to 104.3 workflows/s, the
median of six alternating pairs (faster in six of six, 1.04 to 1.22 times).
`standard_ExecutorStart` falls from 15.1% to 12.2% of samples.
`formal/verus/src/claim_window.rs` proves that the split claim takes the
row of the full statement for every input. `window_claim_sql_holds_for_every_variant`
checks all twelve texts. `claim_seek_tests`, `queue_pause_tests`,
`decision_owner_claim_tests`, `owner_decider_tests`, `claim_budget_tests`
and `activity_claim_epoch_tests` pass unchanged. See
`docs/performance-claim-window-split.md`.
