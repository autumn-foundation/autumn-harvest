-- Legal-hold race during shard rebalancing (issue #1317, round 6 finding #5).
--
-- `stage_copy` snapshots the execution row with no lock. An administrator can
-- place or refresh a legal hold on the source between that snapshot and the
-- cutover; the cutover guard checked only quiescence and history, never hold
-- state, so it could seal a source that now correctly holds the run while the
-- target -- the live copy from here on -- keeps the pre-hold columns. The hold
-- API reports success; the data it was meant to protect is not, in fact, held.
--
-- Fixed the same way `verified_event_count`/`verified_max_event_id` already
-- guard against history drift: `verify_target_copy` re-reads the source's
-- current `legal_hold_set_at` and stamps it here, and the cutover's atomic
-- guard requires the live value to still match at seal time. A hold placed or
-- released in between makes the values differ, so the cutover declines and the
-- operator re-runs the migration -- which restages under the fresh hold state.
ALTER TABLE harvest_shard_migrations
    ADD COLUMN IF NOT EXISTS verified_legal_hold_set_at TIMESTAMPTZ NULL;

-- `verified_legal_hold_set_at` is NULL both for "verified, no hold" and for
-- "never verified by code that knows this column exists" -- a rolling
-- deployment can have an old worker run `verify_target_copy` against a
-- database that already carries this migration, leaving the column at its
-- default. A cutover guard comparing NULL to NULL would then authorize a
-- target staged under a hold that was released before an OLD worker verified
-- it, treating an unverified row as verified. `legal_hold_verified` marks
-- that the check actually ran, independent of what it found, so a legacy or
-- foreign-verified record fails the cutover guard closed rather than by
-- coincidence -- the same shape `verified_event_count IS NOT NULL` already
-- gives the history guard.
ALTER TABLE harvest_shard_migrations
    ADD COLUMN IF NOT EXISTS legal_hold_verified BOOLEAN NOT NULL DEFAULT FALSE;
