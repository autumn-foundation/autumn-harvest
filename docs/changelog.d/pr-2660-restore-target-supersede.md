## A direct fleet restore supersedes a pending Harvest target (issue #2660)

`HarvestFleet::restore(key, Some(t1))` keeps the target `t1` for the next
open. A direct `DatabaseFleet::restore` before that open no longer leaves
`t1` live. The two files now open at the same point.

- **Target file.** Each entry names the database file that it belongs to:
  `committed <of> <target>` or `pending <from> <target> <previous>`. A
  committed target applies only while its file is current. A failed direct
  restore keeps the file, so the target still applies.
- **Release.** Each release before the fleet restore returns writes the
  pending entry again, from the entry before the request. The release of
  the drain comes last, so a stale-epoch open does no harm. Each later
  release binds the target that applies now to the current file, or drops
  it. So an old inode number never revives a target.
- **Open.** An open before the fleet restore returns reads the entry from
  before the request, and keeps the pending entry. When that entry moved,
  a publish ran, so the open refuses until the return.
- **Robustness.** `HarvestFleet::restore` runs in an owned task, so a
  dropped caller never skips the end. A release replaces an unreadable
  entry, and a write syncs the file. An entry in the older format applies
  once, as before.
- **Lock.** `<file>.restore-target.lock` guards each change. An open holds
  it, so a late `end_restore` never writes back a target that the open
  used. A release blocks in place, and the end runs on a blocking thread.
- **Proof.** `formal/verus/src/restore_target.rs` proves `resolve_target`
  and a model of every step. Each open applies the target of the last
  finished `HarvestFleet::restore`, and none after a successful direct
  restore.
- **Invariants.** No `WorkflowEvent` variant and no migration.
  `harvest_events` is not rewritten.
- **Tests.** `a_direct_restore_supersedes_a_waiting_target`,
  `a_failed_direct_restore_keeps_the_waiting_target` and
  `a_dropped_restore_call_still_restores_both_files` in
  `tests/sqlite_plugin_fleet_replica.rs`, and unit tests in `replica.rs`.
  See `docs/design/DESIGN-2660.md`.
