## A reused database identity no longer hides a restore (issue #2721)

Two restores of a tenant before its next open could put the new database
file on the inode of the recorded file. The open then kept the old Harvest
file. `a_direct_restore_supersedes_a_committed_target` failed in CI from
this.

- **Rule.** `replaced` in `sqlite/replica.rs` decides that the database is
  a new file. A superseded restore target now shows a new file, even when
  the identity record matches. This replaces the rule of issue #2660.
- **Identity.** The Unix identity is `<dev>:<ino>:<birth ns>`. A file
  system with no birth time gives `<dev>:<ino>`.
- **Upgrade.** `same_file` compares only the fields that both identities
  hold. A record or a target entry from an older release still matches its
  file. The next open writes the new identity.
- **Downgrade.** An older release reads each new identity as a new file.
  At its first open, each tenant follows its replica. A committed target
  then does not apply. A pending target applies, as if its restore
  published. Let each `HarvestFleet::restore` return before a downgrade.
- **Proof.** `formal/verus/src/restore_target.rs` proves `replaced`. The
  theorem `theorem_a_superseded_target_shows_a_new_file` shows that the
  open after a direct restore that supersedes a target sees a new file.
  This holds when the record shows no move.
- **Invariants.** No `WorkflowEvent` variant and no migration.
  `harvest_events` is not rewritten.
- **Tests.** `a_superseded_target_follows_a_reused_identity` replays the CI
  failure. Unit tests in `replica.rs` cover the identity, `same_file`,
  `replaced` and the upgrade. See `docs/design/DESIGN-2721.md`.
