## A keyed claim counts only the running rows of its own keys (issue #2744, PR #2761)

The claim gate compares the running count of each pending key with its cap.
The window CTE `seek_running_counts` counted all keys in one pass. Under the
claim plan settings, that pass could read every `RUNNING` row of the shard
through `harvest_task_queue_running_worker_idx`. A keyed claim read about
1,090 buffers, against 97 for a claim with no key.

- The new macro `claim_window_running_counts_sql!` builds
  `seek_running_counts` and `seek_deep_running_counts`. It probes
  `harvest_task_queue_concurrency_key_running` once for each distinct
  pending key, with a `LATERAL` count grouped by task type.
- The full scan keeps `concurrency_running_counts` in its one-pass form. Its
  keys are the due keys of the whole backlog, so the planner chooses
  between one pass and a probe per key.
- `concurrency_live_index_perf` (PR #2746): a keyed claim falls from 1,094 to
  112 buffers per call. A plain claim stays at 97. With 10,000 running rows
  across the heap, the window claim falls from 9,749 to 138 buffers. See
  `docs/performance-keyed-claim-running-counts.md`.
- Gates: `keyed_claim_running_counts_tests` bounds the buffers of each window
  count CTE under the claim settings. It checks for one key probe per key. It
  checks that more foreign running rows add no reads. It compares each count
  of all three CTEs with a direct count. The buffer gate read 978 buffers
  before the change and 7 after. A shape test pins the text of each count
  CTE in every claim form.
- No migration. No new index. No new `WorkflowEvent` variant. The claim
  result does not change. The recheck in `claimed` and
  `release_claim_if_over_cap` do not change.
