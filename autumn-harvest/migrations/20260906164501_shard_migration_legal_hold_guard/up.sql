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
