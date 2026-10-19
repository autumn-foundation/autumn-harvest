## Phase — A drop release waits for live members of its session (issue #2193)

A dropped `Session` releases its host slot at the next suspension. Its owner
can be dropped while a member activity of the session is still queued or
running. The release then freed the slot under that member, and a second
session could take the capacity while the first still used its local resource.

The host handler for `__harvest_session_drop_release` now checks for another
task of the session in `PENDING` or `RUNNING`. If one exists, the release goes
back to the queue for five seconds and the session keeps its slot. The
release that `complete()` awaits is unchanged, so only a drop release waits.

**No new `WorkflowEvent` variant. No new `WorkflowCommand` variant. No
migration.** The check keys on `workflow_exec_id`, which has an index. The
`session_id` index was dropped as unusable, so the check does not use it.

Tests: `a_drop_release_waits_for_a_live_member_of_its_session`,
`a_drop_release_proceeds_when_every_member_has_settled` and
`an_explicit_release_does_not_wait_for_a_live_member`. They need Postgres.
