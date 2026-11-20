## A failed removal of the restore target refuses the open (issue #2753)

`Replicas::prepare` removed the restore target with `remove_quietly` after
it applied the target. When the removal failed, the open went on and the
runtime wrote runs. The target stayed, so the next open applied it again
and lost those runs.

- **Fix.** `prepare` removes the target with `write_entry(path, None)?`, as
  its `Pending` branch writes. A failed removal now refuses the open, and
  the error names the target file. The order stays: the database record is
  written before the target goes.
- **Retry.** A retry applies the same target again. The refused open wrote
  no run, so the Harvest file is still at the target, and the retry loses
  nothing.
- **Proof.** `formal/verus/src/restore_target.rs` adds the step
  `OpenFails`. `theorem_a_failed_open_applies_the_same_target` shows that a
  failed open keeps the entry and the next open applies the same target.
  `theorem_a_kept_target_must_refuse_the_open` shows that an open that
  keeps the target and does not refuse breaks the invariant.
- **Invariants.** No `WorkflowEvent` variant and no migration.
  `harvest_events` is not rewritten.
- **Tests.** `a_failed_target_removal_refuses_the_open` and
  `a_retry_after_a_failed_removal_keeps_the_harvest_file_at_the_target` in
  `replica.rs`. A test destination puts a non-empty directory at the target
  path during the restore, so the removal fails on each OS. Production code
  has no fault seam.
