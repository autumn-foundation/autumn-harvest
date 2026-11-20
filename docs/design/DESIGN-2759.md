# DESIGN-2759: a failed sidecar move in `set_aside`

## 1. Problem

`set_aside` in `autumn-harvest-plugin/src/sqlite/replica.rs` moves the
`-wal`, `-shm` and `-journal` sidecars, then the main file. It ignores a
failed sidecar move, and the main file moves anyway. The next open then
creates an empty file at `path`, beside the old `-wal`. SQLite replays the
old frames onto the new file.

A failed move of the main file has the opposite fault. The sidecars already
moved, so the main file at `path` loses its WAL.

## 2. Facts

- `set_aside` holds the publish lock and the Harvest writer lock. No
  Harvest runtime has the file open.
- A process outside Harvest can hold a sidecar open. On Windows a rename
  then fails with a sharing violation.
- `prepare` refuses the open when `set_aside` fails. It records the
  database identity only after `set_aside` succeeds. So the next open
  tries the set-aside again.
- The aside name is unique. Nothing opens it but an operator.
- On Unix a rename fails only on rare faults, such as `EIO`. A test needs a
  seam.

## 3. Brainstorming

The chosen options are in bold.

1. **Move each sidecar first, and the main file last. On a failed move,
   move back each moved sidecar in reverse order, then refuse.**
2. Check first that each sidecar can move. No portable check exists, and
   the result can change before the move.
3. Checkpoint and truncate the WAL before the move. This opens a file that
   does not belong to the tenant, and a hot `-journal` still needs a move.
4. Copy the files aside, then remove them. A copy doubles the disk use, and
   a failed remove has the same fault.
5. **Move with `rename`, and read `NotFound` as an absent sidecar.** This
   removes the race between `exists` and `rename`.
6. **Pass the rename function to the mover, so a test can make one move
   fail.**

## 4. Reverse brainstorming

How can the fix make things worse?

| Fault | Guard |
| --- | --- |
| A move back fails. A sidecar stays aside. | The main file never moved, so no new file starts beside a sidecar. The error names the stranded file. |
| The main file moves before a sidecar. | The main file moves last. The Verus model proves that the main file leaves only after each sidecar left. |
| A crash between two moves. | The main file is still at `path`, so the next open sets it aside again. |
| The undo moves a sidecar that was never moved. | The mover undoes only the moves that it made. |
| The error hides the first cause. | The error holds the failed move. A failed undo adds its own text. |

## 5. Six thinking hats

- **White (facts).** Section 2. One function has the fault. No other
  production code ignores a rename result.
- **Red (feeling).** An operator who reads "set aside" expects the old file
  whole, in one place.
- **Black (risk).** A double fault strands a sidecar. The fix accepts this,
  because the main file stays at `path` and the open refuses.
- **Yellow (value).** No old frame replays onto a new file. The old file
  keeps its WAL on each single fault.
- **Green (ideas).** Section 3. A Windows test holds a sidecar open with
  `share_mode(FILE_SHARE_READ)`, so CI covers a real sharing violation.
- **Blue (process).** Spec, then red tests, then the fix, then a review.

## 6. Spec

`formal/verus/src/set_aside.rs` models the mover. A file is absent, at
`from` or at `to`. A fault script tells which rename fails. The model
proves these properties for each script:

- **Safe.** At each step, a sidecar at `from` has its main file at `from`.
- **Moved.** A success puts each file at `to`.
- **Kept.** A failure keeps the main file at `from`.
- **Restored.** A failure with no failed undo puts each file back at
  `from`.

The model has no mirror, because the mover does I/O. A property test in
`replica.rs` runs the real mover under each fault script and checks the
same properties.

## 7. Tests

- `a_sidecar_that_cannot_move_refuses_the_set_aside`: the main file keeps
  its WAL.
- `a_main_file_that_cannot_move_keeps_its_sidecars`.
- `a_failed_undo_names_the_stranded_sidecar`.
- `the_mover_keeps_the_files_together` (proptest).
- `a_sidecar_held_open_refuses_the_set_aside` (Windows only).
