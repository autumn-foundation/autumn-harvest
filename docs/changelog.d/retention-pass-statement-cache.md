## Ledger — the retention pass keeps its per-candidate statements prepared

The retention candidate loop sent five statements unnamed, so Postgres
planned each of them on every call: the open-fork lineage CTE (twice per
candidate), the chain-link and active-child counts of
`should_skip_candidate`, and the legal-hold read `FOR UPDATE` and the
child lineage null-out of `delete_candidate_execution`. Each now carries
`cached_stmt::CachePreparedDsl::cache_prepared`. No statement text, bind or
migration changes.

On one pass of `retention_reclaim_bench` over 10,000 candidates, the loop
plans 88 times instead of 60,046. Its buffers fall from 3,781,048 to
916,459 (−75.8%), and its WAL is unchanged. The generic plan of each
statement matches its custom plan at 100k, 300k and 1M executions, and the
two give identical results over a 100k fixture.
`docs/performance-retention-pass-statement-cache.md` has the measurement.
