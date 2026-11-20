## A keyed claim counts only the running rows of its own keys (issue #2744)

The claim gate compares the running count of each pending key with its cap.
`claim_running_counts_sql!` counted all pairs in one pass. Under the claim
plan settings, that pass read every `RUNNING` row of the shard through
`harvest_task_queue_running_worker_idx`. A keyed claim read about 1,090
buffers, against 97 for a claim with no key.

- The count now probes `harvest_task_queue_concurrency_key_running` once for
  each pending pair, with a `LATERAL` count. The macro builds
  `seek_running_counts`, `seek_deep_running_counts` and
  `concurrency_running_counts`, so all three change.
- `concurrency_live_index_perf` (PR #2746): a keyed claim falls from 1,094 to
  112 buffers per call. A plain claim stays at 97. With 10,000 running rows
  across the heap, the window claim falls from 9,774 to 138 buffers. See
  `docs/performance-keyed-claim-running-counts.md`.
- Gates: `keyed_claim_running_counts_tests` bounds the buffers of each count
  CTE under the claim settings, checks that more foreign running rows add no
  reads, and compares each count with a direct count. The buffer gate read
  978 buffers before the change and 9 after. A shape test pins the per-pair
  text and `SELECT DISTINCT` in each pending-keys CTE.
- No migration. No new index. No new `WorkflowEvent` variant. The claim
  result does not change. The recheck in `claimed` and
  `release_claim_if_over_cap` do not change.
