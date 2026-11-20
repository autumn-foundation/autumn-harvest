## A failed removal of the restore target refuses the open (issue #2753)

`Replicas::prepare` removed the restore target with `remove_quietly` after
it applied the target. When the removal failed, the open went on and the
runtime wrote runs. The target stayed, so the next open applied it again
and lost those runs.

- **Fix.** `prepare` removes the target with `write_entry(path, None)?`, as
  its `Pending` branch writes. A failed removal now refuses the open. The
  error names the tenant and the target file. The order stays: the
  database record is written before the target goes.
- **Publish.** `publish` removed the sidecars of the old Harvest file
  quietly before the rename. An old WAL that stayed could replay onto the
  restored file. The sidecars now move to private names first. When one
  cannot move, or the rename fails, the moved ones come back and the
  publish refuses. So the old file keeps its WAL. The private copies go
  after the rename.
- **Retry.** A retry applies the same target again. The refused open wrote
  no run, so the Harvest file is still at the target, and the retry loses
  nothing.
- **Proof.** `formal/verus/src/restore_target.rs` adds the step
  `OpenFails`: an open that fails at any point changes no entry.
  `theorem_a_kept_target_must_refuse_the_open` shows the old fault. A kept
  target that applies breaks the invariant. A kept target that a later
  restore superseded makes each later open replace the Harvest file again.
- **Invariants.** No `WorkflowEvent` variant and no migration.
  `harvest_events` is not rewritten.
- **Tests.** `a_failed_target_removal_refuses_the_open`,
  `a_retry_after_a_failed_removal_keeps_the_harvest_file_at_the_target`,
  `a_sidecar_that_cannot_move_refuses_the_publish` and
  `a_publish_removes_the_old_sidecars` in `replica.rs`. A test
  destination puts a non-empty directory at the target path during the
  restore, so the removal fails on each OS. Production code has no fault
  seam.
