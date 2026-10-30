## Engine — Held claims no longer send the claim to the full scan (issue #2420)

**Claim order does not change.** The claim picks the same row as before.
Only its cost changes under contention.

**The problem.** The seek window of issue #1971 reads 32 rows per queue
head with a plain read. A peer claim holds its row lock until it commits,
so the window still returns that row. With 32 or more claims in flight on
one head, every window row can be locked. The claim then ran the full scan
of the backlog. At 100K pending rows it read 106,467 buffers and 2,259 temp
blocks, against 263 with no held claim. A slow claim keeps more claims in
flight, so the cost fed itself.

**The fix.** `queue::claim_task_query` has a deep window between the window
and the full scan:

- It runs only when the window found no candidate and a guarded window row
  passes every claim gate. So peers hold that row. A head that a gate
  blocks never starts it.
- It reads up to `queue::CLAIM_DEEP_SEEK_WINDOW` (256) rows from each held
  queue head only.
- The window guard runs on the window plus the deep rows. Each head is
  still a prefix in claim order, so the guard stays exact.
- It counts the running rows for its own concurrency keys, so a capped key
  stays capped.
- The full scan runs only when a bound remains. A held head that the deep
  window reads to its end needs no full scan.

No migration. No new bind. No new `WorkflowEvent` variant. The fence, kind,
owner and fair splices reach the new scans.

**Evidence.** `held_claims_on_one_head_keep_the_claim_off_the_full_scan`
holds K claim transactions open on one queue. At 100K pending rows, K = 32
reads 1,727 buffers and K = 64 reads 1,772, with no full scan and no
spill. The cost is the same at 10K.
`a_claim_behind_held_rows_takes_the_next_row_in_order` checks the order
inside and past the deep window.
`the_deep_window_keeps_the_bound_of_a_full_head_it_does_not_read` and
`a_fully_held_short_head_skips_the_full_scan` cover the deep bounds.
Shape tests pin the new CTEs and the splice counts. With 256 or more held
claims on one head, the claim falls back to the full scan, as before. `docs/performance.md` § "Held claims" has the
table.
