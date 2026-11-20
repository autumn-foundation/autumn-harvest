## A failed sidecar move refuses the set-aside (issue #2759)

`set_aside` in `sqlite/replica.rs` ignored a failed sidecar move, and the
main file moved anyway. The next open then started an empty Harvest file
beside the old `-wal`, and SQLite replayed the old frames onto it.

- **Fix.** `move_aside` moves each sidecar first, and the main file last.
  A failed move, of a sidecar or of the main file, moves back each moved
  sidecar, last first. Then the set-aside refuses, and so does the open.
  When each move back succeeds, the main file keeps its WAL.
- **Double fault.** A failed move back leaves its sidecar aside. The main
  file stays at its path, so no new file starts beside an old sidecar. The
  error names the stranded sidecar. Move it back before the next open.
- **Aside name.** A rename replaces its target, and the aside name can
  repeat after a restart. `free_aside` now skips a name that any file of
  the set uses.
- **Stray sidecars.** An open of a missing Harvest file sets its stray
  sidecars aside first. So no open starts a new file beside an old WAL.
- **Absent sidecar.** A rename that gives `NotFound` shows an absent
  sidecar. The mover no longer checks `exists` first, so no race sits
  between the check and the move.
- **Proof.** `formal/verus/src/set_aside.rs` models the mover under each
  fault script. It has no mirror, because the mover does I/O. At each step,
  a sidecar at the path has its main file there. A failure keeps the main
  file, and each sidecar is back unless its own move back failed.
- **Invariants.** No `WorkflowEvent` variant and no migration.
  `harvest_events` is not rewritten.
- **Tests.** Unit tests in `replica.rs` cover a failed sidecar move, a
  failed main move, a failed move back, a used aside name and stray
  sidecars. The proptest `the_mover_keeps_the_files_together` checks the
  outcome of the model and the order of the moves back. On Windows,
  `a_sidecar_held_open_refuses_the_set_aside` holds a sidecar open with
  `share_mode(FILE_SHARE_READ)`. CI does not run it. See
  `docs/design/DESIGN-2759.md`.
