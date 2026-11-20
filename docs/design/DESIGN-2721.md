# DESIGN-2721: a reused database identity hides a restore

## 1. Problem

`prepare` finds a new tenant database file in two ways. A restore target
can show it, or the identity record `<file>.tenant-db` can show it. On Unix
the identity was `<dev>:<ino>`.

A second restore can put the new file on the inode of the recorded file.
ext4 reuses a free inode at once. The sequence A, then B, then a new file
on the inode of A gives `recorded == current`.

`a_direct_restore_supersedes_a_committed_target` hit this in CI. The target
was superseded, but the record matched, so the open kept the old Harvest
file at epoch 1. The replica held epoch 6.

The rule of issue #2660 caused the miss: "a superseded entry forces a
restore only when the Harvest file has no database record". That rule
trusts the record over the target.

## 2. Facts

- `record_database` runs at each open, after `prepare`.
- `prepare` removes the target at each open, except the pending entry of an
  unreturned restore.
- Each release binds the target to the current file, or drops it.
- So a superseded target with a matching record comes only from a reused
  identity. No other step gives that state.
- ext4 reuses an inode at once. A test shows it: `rm` and create give the
  same inode, with birth times 4 ms apart.
- The birth time comes from a coarse clock, one tick of a few ms.
- `Metadata::created` gives the birth time through `statx` on Linux. A file
  system with no birth time gives an error.
- The mtime, the ctime and the length change at each write.
- Each `.tenant-db` record and each target entry on disk holds an older
  identity until the next open or release writes it again.

## 3. Brainstorming

The chosen options are in bold.

| Question | Options |
|---|---|
| What shows a new file | the record only · **a superseded target or a moved record** |
| What makes the identity unique | the mtime or ctime (each write changes it) · the length (a checkpoint changes it) · the inode generation through an `ioctl` (needs `unsafe`, few file systems) · a mark written into the database (the fleet owns the file) · **the birth time** |
| How an older identity compares | as text (each upgraded tenant then follows the replica) · **field by field, over the fields that both hold** |
| The decision rule | inline in `prepare` · **a pure kernel, `replaced`, with a Verus proof** |

## 4. Reverse brainstorming

"How could the fix make things worse?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| Each write looks like a new file, so each open restores | The identity holds no mtime, ctime or length. |
| Each tenant follows its replica after the upgrade | `same_file` compares only the fields that both identities hold. |
| An older committed target stops at the upgrade | `held` uses `same_file`, so the target still applies. |
| An older pending target applies with no publish | `held` uses `same_file`, so the file did not move. |
| A file system with no birth time fails each open | The identity falls back to `<dev>:<ino>`. |
| The identity breaks the target entry format | The identity holds no whitespace. |
| A test hides the bug with a retry | The tests force the reused identity. No test retries the open. |

## 5. Six thinking hats

- **White (facts).** One CI failure, `left: 1, right: 6`. The local loop of
  50 runs did not fail. The cause is inode reuse. Section 2 lists the rest.
- **Red (feeling).** A flaky test hides a real fault. An operator who
  restores twice gets an old Harvest file, with no error.
- **Black (risk).** The birth time is coarse, so two files born in one tick
  on one inode still match. Some file systems have no birth time. A
  downgrade reads a three-field identity as a new file.
- **Yellow (value).** The target rule removes the CI failure for every file
  system and every clock. The birth time closes most of the other gap,
  two direct restores with no target.
- **Green (ideas).** Prove in Verus that a superseded target shows a new
  file when the record shows no move. Test the reuse with a forced record,
  not with the file system alone.
- **Blue (process).** Spec and proof first. Then red tests, the fix, a
  refactor, the gates, and a review from several angles.

## 6. Decision

1. `replaced(resolution, moved)` is true when a target applies, when a
   later restore superseded the target, or when the record moved.
   `formal/verus/src/restore_target.rs` proves it. The theorem
   `theorem_a_superseded_target_shows_a_new_file` shows that the open
   after a direct restore that supersedes a target sees a new file. This
   holds when the record shows no move. The model has no record and no
   `follow_replaced`.
2. The Unix identity is `<dev>:<ino>:<birth ns>`, or `<dev>:<ino>` with no
   birth time.
3. `same_file` compares two identities field by field. `prepare` and
   `held` use it.

## 7. Limits

- Two direct restores with no target, on a file system with no birth time,
  can still give a reused identity. The record then shows no move.
- Two files on one inode, born in one clock tick, match.
- A record or a target entry from before the upgrade holds no birth time.
  Until the next open or release writes it again, a new file on its inode
  still matches it.
- A downgrade reads each new identity as a new file. At its first open,
  each tenant follows its replica. A committed target then does not
  apply. A pending target applies, as if its restore published. Let each
  `HarvestFleet::restore` return before a downgrade.

## 8. Tests

- `a_superseded_target_follows_a_reused_identity` replays the CI failure
  with a forced record.
- `a_new_file_on_a_reused_inode_has_a_new_identity` uses the file system.
- `the_unix_identity_holds_the_birth_time`,
  `same_file_compares_the_fields_that_both_hold` and
  `replaced_sees_a_superseded_target` test the pure functions.
- `an_older_record_keeps_its_harvest_file`,
  `an_older_committed_target_still_applies` and
  `an_older_pending_target_sees_an_unmoved_file` test the upgrade.
- `a_superseded_target_restores_the_harvest_file` replaces the test of the
  rule from issue #2660.
