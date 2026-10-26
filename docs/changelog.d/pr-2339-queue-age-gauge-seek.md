## Phase — Queue age gauge seeks the oldest rows (Ledger, issue #2339)

`queue::oldest_pending_ages` no longer reads every eligible pending row. For
each queue it reads the oldest 32 rows through `idx_harvest_tq_coverage_sample`,
runs the claim gates over them, and keeps the result when it is provably exact.
A queue that cannot be settled from the window reads the whole queue, as before.
The result is the same as the old aggregate. There is no migration, no new index
and no change to the gate text, which one macro now shares.

Measured on a skewed backlog with `pg_stat_statements`: the age statement took
89.5% to 98.6% of all buffers in a poll tick. Its buffers fell by 88.8%, 98.6%
and 99.5% at 20,000, 100,000 and 300,000 pending rows. WAL bytes and statement
count did not change. The harness, baseline and plans are in
`docs/perf-artifacts/queue-age-gauge/`. The write-up is
`docs/performance-queue-age-gauge.md`. The test
`age_gauge_matches_the_reference_query` compares the new query with the old
query on edge cases.
