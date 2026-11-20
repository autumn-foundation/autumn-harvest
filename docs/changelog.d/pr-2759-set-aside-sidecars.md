## A failed sidecar move refuses the set-aside (issue #2759)

`set_aside` in `sqlite/replica.rs` ignored a failed sidecar move, and the
main file moved anyway. The next open then started an empty Harvest file
beside the old `-wal`, and SQLite replayed the old frames onto it.

- **Fix.** `move_aside` moves each sidecar first, and the main file last.
  A failed move, of a sidecar or of the main file, moves back each moved
  sidecar, last first. Then the set-aside refuses, and so does the open.
  The main file keeps its WAL.
- **Double fault.** A failed move back leaves its sidecar aside. The main
  file stays at its path, so no new file starts beside an old sidecar. The
  error names the stranded sidecar.
- **Absent sidecar.** A rename that gives `NotFound` shows an absent
  sidecar. The mover no longer checks `exists` first, so no race sits
  between the check and the move.
- **Proof.** `formal/verus/src/set_aside.rs` models the mover under each
  fault script. It has no mirror, because the mover does I/O. At each step,
  a sidecar at the path has its main file there. A success moves each file.
  A failure keeps the main file, and a clean failure puts each file back.
- **Invariants.** No `WorkflowEvent` variant and no migration.
  `harvest_events` is not rewritten.
- **Tests.** `a_sidecar_that_cannot_move_refuses_the_set_aside`,
  `a_main_file_that_cannot_move_keeps_its_sidecars`,
  `a_failed_undo_names_the_stranded_sidecar` and
  `a_set_aside_moves_each_sidecar_with_the_file` in `replica.rs`. The
  proptest `the_mover_keeps_the_files_together` runs the real mover under
  each fault script. On Windows, `a_sidecar_held_open_refuses_the_set_aside`
  holds a sidecar open with `share_mode(FILE_SHARE_READ)`. See
  `docs/design/DESIGN-2759.md`.
