## Batch claim, off by default (issue #2693)

`WorkerConfig::with_claim_batch_max(n)` lets one claim statement take up to
`n` rows, in batches of 2, 4 or 8. The batch text derives from the window
form of the default claim. `seek_candidate` locks up to K rows. `candidate`
keeps them in claim order, up to the second row with a concurrency key or a
rate-limit key. So a key or a bucket meets at most one row of a batch. Each
row passes the post-claim rechecks of a single claim, in one pipeline. A
claim loop holds its share of the free permits of each kind across the
claim. Queue weights, fairness keys, priority ageing, owned workflow claims,
adaptive limits, cost holds and dispatch channels keep one row per claim.

- `WorkerConfig`, `WorkerRuntimeConfig` and the effective config view gain
  the public field `claim_batch_max`. Add `claim_batch_max: None` to a
  struct literal.
- `queue::claim_task_batch_with_slots`, `queue::ClaimBatch`,
  `queue::SlottedBatch` and `queue::batch_claim_query_for` are new.
- `formal/verus/src/claim_batch.rs` proves the batch size bounds and the key
  cut.

Evidence: on the `throughput` cell, the median of six alternating pairs
rises from 100.0 to 118.7 workflows/s, faster in six of six. Claim
statements fall 37% and their execution time 29%. See
`docs/performance-claim-batch.md`.
