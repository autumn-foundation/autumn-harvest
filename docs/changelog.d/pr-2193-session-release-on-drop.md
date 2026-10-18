## Phase — Release a Session when its owner drops it (issue #2193)

A `Session` now releases its host slot when its owner drops it. This covers
a cancelled `Future`, a race loser and a `select!` branch. Before, the slot
stayed `ACTIVE` until the 24-hour lease ended.

`Drop for Session` pushes a `ScheduleActivity` for the new reserved name
`__harvest_session_drop_release`. The command is pinned to the host and no
body awaits it. The host handler is the one that `complete()` uses, and it is
idempotent.

**No new `WorkflowEvent` variant. No new `WorkflowCommand` variant. No
migration.** Replay treats each record of the new name as transparent, so it
never shifts a positional match. The release is recorded once. `Drop` issues
nothing when history already holds a release for the session. A drop also
issues nothing in a suspending cycle, while a panic unwinds, and after
`complete()` has dispatched.

Limits: a run that ends first still relies on the broken-session scanner to
reap its session. Deploy the new build to every worker before a workflow
drops a session. An older build cannot run the new name. See the 0.8.0
upgrade guide, section 1.11.

Design record, with brainstorm, reverse brainstorm and six hats: `DESIGN-2193.md`.

Tests: `dropping_a_session_issues_one_pinned_release`,
`a_dropped_complete_future_releases_the_session`,
`a_session_dropped_before_a_park_is_released`,
`a_drop_release_record_is_transparent_to_the_matcher`,
`a_failed_cycle_records_nothing_for_a_drop_release`,
`a_drop_release_does_not_conflict_with_a_local_activity` and
`the_test_env_resolves_a_drop_release`.
