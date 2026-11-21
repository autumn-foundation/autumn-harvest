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
- The writer lock needs the main file, because it locks a file beside its
  canonical path.
- A process outside Harvest can hold a sidecar open. On Windows a rename
  then fails with a sharing violation.
- `prepare` refuses the open when `set_aside` fails. It records the
  database identity only after `set_aside` succeeds. So the next open
  tries the set-aside again.
- `std::fs::rename` replaces its target on each OS.
- The aside name is `<pid>-<counter>`. A restart can reuse the process id,
  and the counter starts at 0 again. So a name can repeat.
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
7. **Skip an aside name that a file already uses.** A rename never then
   replaces an earlier aside set.
8. **Set stray sidecars aside.** A sidecar with no main file never belongs
   to a new file.

## 4. Reverse brainstorming

How can the fix make things worse?

| Fault | Guard |
| --- | --- |
| A move back fails. A sidecar stays aside. | The main file never moved, so no new file starts beside a sidecar. The error names the stranded file and tells the operator to move it back before the next open. |
| The main file moves before a sidecar. | The main file moves last. The Verus model proves that no sidecar stays at `path` without its main file. |
| The undo stops at its first failed move back. | The Verus model proves that each sidecar is back, unless its own move back failed. The proptest checks the order of the moves back. |
| The new aside set lands on an old one. | `free_aside` skips a name that any file of the set uses. It runs under the publish lock. |
| A stray sidecar moves while a runtime creates the main file. | The mover sends no rename for an absent main file. Fleet ownership allows one opener of a tenant. |
| A crash between two moves. | The main file is still at `path`, so the next open sets it aside again. See section 8. |
| The error hides the first cause. | The error holds the failed move. A failed undo adds its own text. |

## 5. Six thinking hats

- **White (facts).** Section 2. One function has the fault. No other
  engine or plugin code ignores a rename result.
- **Red (feeling).** An operator who reads "set aside" expects the old file
  whole, in one place.
- **Black (risk).** A double fault strands a sidecar. The fix accepts this,
  because the main file stays at `path` and the open refuses.
- **Yellow (value).** No old frame replays onto a new file. The old file
  keeps its WAL on each single fault.
- **Green (ideas).** Section 3. A Windows test holds a sidecar open with
  `share_mode(FILE_SHARE_READ)`, so a real sharing violation runs.
- **Blue (process).** Spec, then red tests, then the fix, then a review.

## 6. Spec

`formal/verus/src/set_aside.rs` models the mover. A file is absent, at
`from` or at `to`. One fault script tells which forward rename fails. A
second script tells which sidecar cannot move back. The model proves these
properties for each pair of scripts:

- **Safe.** At each step, a sidecar at `from` has its main file at `from`.
- **Moved.** A success puts each present file at `to`. No sidecar stays at
  `from`, also when the main file was absent.
- **Kept.** A failure keeps the main file where it was. Each sidecar is
  back where it was, unless its own move back failed.
- **Restored.** A failure with no failed move back puts each file back.
- **Live.** With no failed forward rename, the mover succeeds.

The model has no mirror, because the mover does I/O. The proptest
`the_mover_keeps_the_files_together` runs the real mover under each pair of
scripts. It checks the outcome of the model, and the order of the moves
back.

## 7. Tests

- `a_set_aside_moves_each_sidecar_with_the_file`.
- `a_sidecar_that_cannot_move_refuses_the_set_aside`: the main file keeps
  its WAL.
- `a_main_file_that_cannot_move_keeps_its_sidecars`.
- `a_failed_undo_names_the_stranded_sidecar`.
- `an_aside_name_in_use_is_skipped`.
- `stray_sidecars_go_aside` and
  `an_open_never_starts_a_file_beside_a_stray_wal`.
- `the_mover_keeps_the_files_together` (proptest).
- `a_sidecar_held_open_refuses_the_set_aside` (Windows only). CI runs the
  plugin lib tests on Linux only, so CI does not run this test.

## 8. Limits

- A crash between two moves splits the set. The main file stays at `path`
  with no WAL, and the next open sets it aside under a new name. The WAL
  stays under the first name. The old code had the same window.
- A sharing violation on Windows can be brief, for example from a virus
  scanner. The open then refuses, and the next open tries again.
