## Engine — `queue::complete_task` requires a claim (issue #1992)

**Breaking API change.** `queue::complete_task` now takes a
`&queue::TaskClaim` in place of a task id:

```rust
// Before
queue::complete_task(conn, task.id, output).await?;
// After: build the claim from the claimed row ...
let claim = queue::TaskClaim::of(&task).ok_or(MyError::NotClaimed)?;
// ... or from the parts of the claim.
let claim = queue::TaskClaim::new(task_id, worker_id, attempt);
queue::complete_task(conn, &claim, output).await?;
```

See section 1.3 of `docs/upgrading/0.8.0.md`.

**The problem.** The old function filtered on `id` and
`state = 'RUNNING'` only. It skipped the claim-epoch fence of issue #1789.
A worker that lost its claim to a reclaim could complete the row of the next
claim through it. That claim then lost its row.

**Impact.** No shipped worker path completed a stale claim. Only a direct
caller of the public function could do it.

**The fix.**

- `complete_task` adds `claim_held(worker_id, attempt)` to its `UPDATE`. It
  returns `HarvestError::NotFound` when the row is not `RUNNING` under the
  claim. `NotFound` now also covers a row that a later claim holds.
- `complete_claimed_task` is the same write. It returns
  `ClaimWrite::LeaseLost` for a stale claim.
- No completion in `queue.rs` is unfenced. The shared inner write takes a
  `&TaskClaim`, not an `Option`.
- The three `worker.rs` callers already ran after the
  `claim_still_held_for_update` guard (issue #1184). They now also pass
  their claim, so the write checks it again.
- This supersedes the #1789 note that `complete_task` stays unfenced.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route
change.

**Tests.** `stale_owner_cannot_complete_a_reclaimed_row_through_complete_task`
in `activity_claim_epoch_tests.rs`. Worker A claims, the reclaimer
requeues, and worker B claims. A's `complete_task` returns `NotFound` and
leaves B's row unchanged. B then completes. The test failed before the fix:
A's call completed B's row. Existing callers pass the claim that they took.
`Fencing::StateOnly` in `dst_differential_tests.rs` runs the pre-#1789
`UPDATE` as raw SQL.
